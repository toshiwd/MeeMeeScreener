from __future__ import annotations

import hashlib
import json
import os
import subprocess
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from app.backend.api.dependencies import get_stock_repo
from app.backend.infra.duckdb.stock_repo import StockRepository
from app.backend.services.tradex_experiment_store import (
    acquire_lock,
    baseline_lock_file,
    family_compare_file,
    family_dir,
    family_file,
    family_lock_path,
    list_family_ids,
    load_family,
    load_run,
    load_run_any,
    read_json,
    run_adopt_file,
    run_detail_file,
    run_file,
    tradex_families_root,
    write_json,
)
from external_analysis.contracts.analysis_input import AnalysisInputContract
from external_analysis.contracts.analysis_output import ANALYSIS_OUTPUT_SCHEMA_VERSION
from external_analysis.runtime.orchestrator import run_tradex_analysis

TRADEX_FAMILY_SCHEMA_VERSION = "tradex_experiment_family_v1"
TRADEX_RUN_SCHEMA_VERSION = "tradex_experiment_run_v1"
TRADEX_COMPARE_SCHEMA_VERSION = "tradex_experiment_compare_v1"
TRADEX_DETAIL_SCHEMA_VERSION = "tradex_experiment_detail_v1"
TRADEX_ADOPT_SCHEMA_VERSION = "tradex_experiment_adopt_v1"
TRADEX_ANALYSIS_ENGINE_VERSION = ANALYSIS_OUTPUT_SCHEMA_VERSION
DEFAULT_GATE_FILENAME = "adopt_gate.json"
REPO_ROOT = Path(__file__).resolve().parents[3]


def _text(value: Any, fallback: str = "") -> str:
    text = str(value or "").strip()
    return text or fallback


def _float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        out = float(value)
        return out if out == out else None
    if isinstance(value, str) and value.strip():
        try:
            out = float(value)
        except Exception:
            return None
        return out if out == out else None
    return None


def _int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str) and value.strip():
        try:
            return int(float(value))
        except Exception:
            return None
    return None


def _bool(value: Any, fallback: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        raw = value.strip().lower()
        if raw in {"1", "true", "yes", "on"}:
            return True
        if raw in {"0", "false", "no", "off"}:
            return False
    return fallback


def _json_ready(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    return value


def _git_commit() -> str:
    try:
        out = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True)
        return out.strip() or "unknown"
    except Exception:
        return "unknown"


def _iso_date(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"):
        try:
            return datetime.strptime(text[:10] if fmt != "%Y%m%d" else text[:8], fmt).date().isoformat()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date().isoformat()
    except ValueError:
        return None


def _date_from_iso(text: str) -> date:
    return date.fromisoformat(text)


def _epoch_from_date(text: str) -> int:
    d = _date_from_iso(text)
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())


def _safe_list(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for item in values:
        text = _text(item)
        if text and text not in seen:
            seen.add(text)
            out.append(text)
    return out


def _normalize_segments(values: Any) -> list[dict[str, str]]:
    if not isinstance(values, list):
        return []
    out: list[dict[str, str]] = []
    for item in values:
        if not isinstance(item, dict):
            continue
        start = _iso_date(item.get("start_date"))
        end = _iso_date(item.get("end_date"))
        if not start or not end or _date_from_iso(end) < _date_from_iso(start):
            continue
        payload = {"start_date": start, "end_date": end}
        label = _text(item.get("label"))
        if label:
            payload["label"] = label
        out.append(payload)
    return out


def _normalize_plan(plan: dict[str, Any], *, default_plan_id: str) -> dict[str, Any]:
    if not isinstance(plan, dict):
        raise ValueError("plan must be an object")
    plan_id = _text(plan.get("plan_id") or plan.get("id"), fallback=default_plan_id)
    if not plan_id:
        raise ValueError("plan_id is required")
    signal_bias = _text(plan.get("signal_bias"), fallback="balanced")
    if signal_bias not in {"buy", "sell", "balanced"}:
        signal_bias = "balanced"
    return {
        "plan_id": plan_id,
        "plan_version": _text(plan.get("plan_version"), fallback="v1"),
        "label": _text(plan.get("label"), fallback=plan_id),
        "minimum_confidence": _float(plan.get("minimum_confidence")),
        "minimum_ready_rate": _float(plan.get("minimum_ready_rate")),
        "signal_bias": signal_bias,
        "top_k": max(1, _int(plan.get("top_k")) or 3),
        "notes": _text(plan.get("notes")),
    }


def tradex_gate_config_file() -> Path:
    root = REPO_ROOT / "config" / "tradex"
    root.mkdir(parents=True, exist_ok=True)
    return root / DEFAULT_GATE_FILENAME


def _default_gate_config() -> dict[str, Any]:
    return {
        "schema_version": "tradex_adopt_gate_v1",
        "primary_metrics": ["signal_rate", "ready_rate", "mean_confidence"],
        "metric_direction": {"signal_rate": "higher", "ready_rate": "higher", "mean_confidence": "higher"},
        "minimum_effect_size": {"signal_rate": 0.01, "ready_rate": 0.01, "mean_confidence": 0.01},
        "by_period_deterioration_limit": 0.5,
        "symbol_concentration_limit": 0.35,
        "detail_reason_required": True,
        "confirmed_rerun_match_required": True,
    }


def _load_gate_config() -> dict[str, Any]:
    path = tradex_gate_config_file()
    payload = read_json(path, _default_gate_config())
    return payload if isinstance(payload, dict) else _default_gate_config()


def _feature_flags() -> dict[str, str]:
    keys = [key for key in os.environ if key.startswith("MEEMEE_ENABLE_TRADEX") or key.startswith("VITE_ENABLE_TRADEX")]
    out: dict[str, str] = {}
    for key in sorted(keys):
        value = _text(os.getenv(key))
        if value:
            out[key] = value
    return out


def _baseline_lock(family: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "tradex_baseline_lock_v1",
        "baseline_version": _text((family.get("baseline_plan") or {}).get("plan_version"), fallback="v1"),
        "input_dataset_version": _text(family.get("input_dataset_version"), fallback="unknown"),
        "code_revision": _text(family.get("code_revision"), fallback=_git_commit()),
        "analysis_engine_version": TRADEX_ANALYSIS_ENGINE_VERSION,
        "feature_flags": _feature_flags(),
        "confirmed_only": True,
        "timezone": _text(family.get("timezone"), fallback="Asia/Tokyo"),
        "price_source": _text(family.get("price_source"), fallback="daily_bars"),
        "data_cutoff_at": _text(family.get("data_cutoff_at")),
        "random_seed": _int(family.get("random_seed")) or 0,
        "metric_schema_version": "tradex_experiment_metrics_v1",
    }


def _family_status_summary(family: dict[str, Any]) -> dict[str, Any]:
    run_ids = [str(run_id) for run_id in family.get("run_ids") or [] if _text(run_id)]
    runs: list[dict[str, Any]] = []
    for run_id in run_ids:
        run = load_run_any(run_id)[1]
        if isinstance(run, dict):
            runs.append(run)
    counts: dict[str, int] = {}
    for run in runs:
        status = _text(run.get("status"), fallback="unknown")
        counts[status] = counts.get(status, 0) + 1
    return {
        "total_runs": len(runs),
        "baseline_runs": sum(1 for run in runs if _text(run.get("run_kind")) == "baseline"),
        "candidate_runs": sum(1 for run in runs if _text(run.get("run_kind")) == "candidate"),
        "status_counts": counts,
    }


def _family_payload(family_id: str) -> dict[str, Any] | None:
    family = load_family(family_id)
    if not family:
        return None
    payload = dict(family)
    payload["status_summary"] = _family_status_summary(payload)
    return payload


def _period_segments(family: dict[str, Any]) -> list[dict[str, str]]:
    period = family.get("period") if isinstance(family.get("period"), dict) else {}
    segments = period.get("segments") if isinstance(period, dict) else []
    return _normalize_segments(segments)


def _analysis_points(repo: StockRepository, code: str, start_date: str, end_date: str) -> list[dict[str, Any]]:
    timeline = repo.get_analysis_timeline(code, _epoch_from_date(end_date), limit=1000)
    out: list[dict[str, Any]] = []
    start_key = start_date.replace("-", "")
    end_key = end_date.replace("-", "")
    for row in timeline:
        if not isinstance(row, dict):
            continue
        dt_key = _int(row.get("dt"))
        if dt_key is None:
            continue
        text = str(dt_key)
        if len(text) != 8 or text < start_key or text > end_key:
            continue
        out.append(row)
    return out


def _analysis_input(code: str, dt_key: int, point: dict[str, Any]) -> AnalysisInputContract:
    ymd = str(dt_key)
    return AnalysisInputContract(
        symbol=code,
        asof=f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:8]}",
        analysis_p_up=point.get("pUp"),
        analysis_p_down=point.get("pDown"),
        analysis_p_turn_up=point.get("pTurnUp"),
        analysis_p_turn_down=point.get("pTurnDown"),
        analysis_ev_net=point.get("ev20Net"),
        sell_analysis={
            "dt": dt_key,
            "pDown": point.get("sellPDown"),
            "pTurnDown": point.get("sellPTurnDown"),
            "trendDown": point.get("trendDown"),
            "trendDownStrict": point.get("trendDownStrict"),
            "shortRet5": point.get("shortRet5"),
            "shortRet10": point.get("shortRet10"),
            "shortRet20": point.get("shortRet20"),
            "shortWin5": point.get("shortWin5"),
            "shortWin10": point.get("shortWin10"),
            "shortWin20": point.get("shortWin20"),
        },
    )


def _signal_flag(output: dict[str, Any], plan: dict[str, Any]) -> bool:
    readiness = output.get("publish_readiness") if isinstance(output.get("publish_readiness"), dict) else {}
    ratios = output.get("side_ratios") if isinstance(output.get("side_ratios"), dict) else {}
    confidence = _float(output.get("confidence")) or 0.0
    min_confidence = _float(plan.get("minimum_confidence")) or 0.0
    bias = _text(plan.get("signal_bias"), fallback="balanced")
    buy = _float(ratios.get("buy")) or 0.0
    sell = _float(ratios.get("sell")) or 0.0
    bias_ok = True
    if bias == "buy":
        bias_ok = buy >= sell
    elif bias == "sell":
        bias_ok = sell >= buy
    return bool(confidence >= min_confidence and _bool(readiness.get("ready"), False) and bias_ok)


def _sample_trace(code: str, dt_key: int, input_contract: AnalysisInputContract, output: dict[str, Any], plan: dict[str, Any]) -> dict[str, Any]:
    return {
        "code": code,
        "date": f"{str(dt_key)[:4]}-{str(dt_key)[4:6]}-{str(dt_key)[6:8]}",
        "signal": _signal_flag(output, plan),
        "confidence": _float(output.get("confidence")) or 0.0,
        "reasons": _safe_list(output.get("reasons")),
        "candidate_reasons": _safe_list(
            reason
            for comparison in output.get("candidate_comparisons") or []
            if isinstance(comparison, dict)
            for reason in (comparison.get("reasons") or [])
        ),
        "publish_ready": _bool((output.get("publish_readiness") or {}).get("ready"), False) if isinstance(output.get("publish_readiness"), dict) else False,
        "side_ratios": _json_ready(output.get("side_ratios") or {}),
        "input": _json_ready(input_contract.to_dict()),
        "output": _json_ready(output),
    }


def _aggregate(samples: list[dict[str, Any]]) -> dict[str, Any]:
    if not samples:
        return {
            "sample_count": 0,
            "signal_count": 0,
            "signal_rate": 0.0,
            "ready_rate": 0.0,
            "mean_confidence": 0.0,
            "top_symbol_share": 0.0,
            "signal_dates": [],
            "top_reasons": [],
            "winning_examples": [],
            "losing_examples": [],
        }
    reason_counts: dict[str, int] = {}
    symbol_counts: dict[str, int] = {}
    signal_dates: list[str] = []
    signal_count = 0
    ready_count = 0
    confidence_total = 0.0
    for item in samples:
        confidence_total += float(item.get("confidence") or 0.0)
        if item.get("publish_ready"):
            ready_count += 1
        if item.get("signal"):
            signal_count += 1
            signal_dates.append(_text(item.get("date")))
            code = _text(item.get("code"))
            symbol_counts[code] = symbol_counts.get(code, 0) + 1
        for reason in _safe_list(item.get("reasons")) + _safe_list(item.get("candidate_reasons")):
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
    top_symbol_share = (max(symbol_counts.values()) / signal_count) if signal_count else 0.0
    ordered_reasons = sorted(reason_counts.items(), key=lambda pair: (-pair[1], pair[0]))[:3]
    by_conf = sorted(samples, key=lambda item: (float(item.get("confidence") or 0.0), item.get("date") or ""), reverse=True)
    by_low_conf = list(reversed(by_conf))
    return {
        "sample_count": len(samples),
        "signal_count": signal_count,
        "signal_rate": signal_count / float(len(samples)),
        "ready_rate": ready_count / float(len(samples)),
        "mean_confidence": confidence_total / float(len(samples)),
        "top_symbol_share": top_symbol_share,
        "signal_dates": sorted(set(signal_dates)),
        "top_reasons": [{"reason": reason, "count": count} for reason, count in ordered_reasons],
        "winning_examples": by_conf[:3],
        "losing_examples": by_low_conf[:3],
    }


def _metrics_signature(metrics: dict[str, Any]) -> str:
    payload = json.dumps(_json_ready(metrics), ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _overall_score(primary: dict[str, Any]) -> float:
    if not isinstance(primary, dict) or not primary:
        return 0.0
    values = [_float(primary.get(key)) or 0.0 for key in ("signal_rate", "ready_rate", "mean_confidence")]
    return sum(values) / float(len(values)) if values else 0.0


def _compare_signature(compare: dict[str, Any]) -> str:
    payload = {
        "baseline_run_id": compare.get("baseline_run_id"),
        "candidate_results": compare.get("candidate_results"),
    }
    return hashlib.sha256(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _run_compare_signature(compare: dict[str, Any]) -> str:
    payload = {
        "primary_metric_deltas": compare.get("primary_metric_deltas"),
        "target_symbol_count_delta": compare.get("target_symbol_count_delta"),
        "signal_date_deltas": compare.get("signal_date_deltas"),
        "winning_examples": compare.get("winning_examples"),
        "losing_examples": compare.get("losing_examples"),
        "top_conditions": compare.get("top_conditions"),
        "symbol_summary": compare.get("symbol_summary"),
    }
    return hashlib.sha256(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _build_run_result(family: dict[str, Any], run: dict[str, Any]) -> dict[str, Any]:
    repo = get_stock_repo()
    plan = family["baseline_plan"] if _text(run.get("run_kind")) == "baseline" else next(
        (item for item in family.get("candidate_plans") or [] if isinstance(item, dict) and _text(item.get("plan_id")) == _text(run.get("plan_id"))),
        None,
    )
    if not isinstance(plan, dict):
        raise ValueError("plan not found")
    segments = _period_segments(family)
    by_code: dict[str, dict[str, Any]] = {}
    all_samples: list[dict[str, Any]] = []
    for code in family.get("universe") or []:
        normalized_code = _text(code)
        if not normalized_code:
            continue
        code_samples: list[dict[str, Any]] = []
        for segment in segments:
            points = _analysis_points(repo, normalized_code, segment["start_date"], segment["end_date"])
            for point in points:
                dt_key = _int(point.get("dt"))
                if dt_key is None:
                    continue
                sample_input = _analysis_input(normalized_code, dt_key, point)
                sample_output = run_tradex_analysis(sample_input).to_dict()
                sample_trace = _sample_trace(normalized_code, dt_key, sample_input, sample_output, plan)
                code_samples.append(sample_trace)
                all_samples.append(sample_trace)
        if code_samples:
            by_code[normalized_code] = {
                "code": normalized_code,
                **_aggregate(code_samples),
                "samples": code_samples,
            }
    overall = _aggregate(all_samples)
    by_period: list[dict[str, Any]] = []
    period_scores: list[float] = []
    for segment in segments:
        segment_samples = [item for item in all_samples if segment["start_date"] <= item["date"] <= segment["end_date"]]
        aggregate = _aggregate(segment_samples)
        period_score = _overall_score({
            "signal_rate": aggregate["signal_rate"],
            "ready_rate": aggregate["ready_rate"],
            "mean_confidence": aggregate["mean_confidence"],
        })
        period_scores.append(period_score)
        by_period.append(
            {
                "label": segment.get("label") or f"{segment['start_date']}..{segment['end_date']}",
                "start_date": segment["start_date"],
                "end_date": segment["end_date"],
                "overall_score": period_score,
                "metrics": aggregate,
            }
        )
    overall_score = _overall_score({
        "signal_rate": overall["signal_rate"],
        "ready_rate": overall["ready_rate"],
        "mean_confidence": overall["mean_confidence"],
    })
    if period_scores:
        period_stability = max(0.0, 1.0 - min(1.0, max(period_scores) - min(period_scores)))
    else:
        period_stability = 0.0
    metrics = {
        "overall": {
            **overall,
            "target_symbol_count": len(by_code),
            "overall_score": overall_score,
            "by_period_stability": period_stability,
            "primary": {
                "signal_rate": overall["signal_rate"],
                "ready_rate": overall["ready_rate"],
                "mean_confidence": overall["mean_confidence"],
            },
        },
        "by_period": by_period,
        "by_code": by_code,
        "samples": all_samples,
    }
    run["status"] = "succeeded"
    run["completed_at"] = datetime.now(timezone.utc).isoformat()
    run["error"] = None
    run["metrics"] = metrics
    run["summary"] = {
        "run_signature": _metrics_signature(metrics),
        "sample_count": overall["sample_count"],
        "signal_count": overall["signal_count"],
        "signal_dates": overall["signal_dates"],
        "top_reasons": overall["top_reasons"],
        "target_symbol_count": len(by_code),
        "top_symbol_share": overall["top_symbol_share"],
        "overall_score": overall_score,
        "by_period_stability": period_stability,
    }
    run["analysis"] = {"segments": segments, "by_code": by_code, "signature": run["summary"]["run_signature"]}
    return run


def _run_base(family: dict[str, Any], run_id: str, run_kind: str, plan: dict[str, Any], notes: str | None) -> dict[str, Any]:
    return {
        "schema_version": TRADEX_RUN_SCHEMA_VERSION,
        "family_id": family["family_id"],
        "run_id": run_id,
        "run_kind": run_kind,
        "plan_id": plan["plan_id"],
        "plan_version": plan["plan_version"],
        "baseline_version": family["baseline_plan"]["plan_version"],
        "status": "created",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "completed_at": None,
        "error": None,
        "universe": list(family.get("universe") or []),
        "period": family.get("period") or {},
        "confirmed_only": True,
        "input_dataset_version": _text(family.get("input_dataset_version")),
        "timezone": _text(family.get("timezone"), fallback="Asia/Tokyo"),
        "price_source": _text(family.get("price_source"), fallback="daily_bars"),
        "data_cutoff_at": _text(family.get("data_cutoff_at")),
        "random_seed": _int(family.get("random_seed")) or 0,
        "notes": _text(notes),
        "metrics": {},
        "summary": {},
        "analysis": {},
    }


def _update_family_file(family: dict[str, Any]) -> None:
    family["status_summary"] = _family_status_summary(family)
    write_json(family_file(family["family_id"]), family)


def create_family(body: dict[str, Any]) -> dict[str, Any]:
    family_id = _text(body.get("family_id")) or hashlib.sha1(
        json.dumps(body, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()[:12]
    if load_family(family_id):
        raise ValueError("family already exists")
    if body.get("confirmed_only") is not None and not _bool(body.get("confirmed_only"), True):
        raise ValueError("confirmed_only must be true")
    universe = _safe_list(body.get("universe"))
    if not 20 <= len(universe) <= 50:
        raise ValueError("universe must contain 20 to 50 symbols")
    segments = _normalize_segments(body.get("period", {}).get("segments") if isinstance(body.get("period"), dict) else None)
    if len(segments) < 2:
        raise ValueError("period must contain at least 2 segments")
    baseline_plan = _normalize_plan(body.get("baseline_plan") or {}, default_plan_id="baseline")
    candidate_plans_raw = body.get("candidate_plans") if isinstance(body.get("candidate_plans"), list) else []
    if len(candidate_plans_raw) > 3:
        raise ValueError("candidate_plans must be at most 3")
    candidate_plans = [_normalize_plan(plan, default_plan_id=f"candidate-{idx + 1}") for idx, plan in enumerate(candidate_plans_raw)]
    family = {
        "schema_version": TRADEX_FAMILY_SCHEMA_VERSION,
        "family_id": family_id,
        "family_name": _text(body.get("family_name"), fallback=f"family-{family_id}"),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "frozen": False,
        "frozen_at": None,
        "universe": universe,
        "period": {"segments": segments},
        "confirmed_only": True,
        "input_dataset_version": _text(body.get("input_dataset_version"), fallback=f"data_cutoff:{segments[-1]['end_date']}"),
        "code_revision": _text(body.get("code_revision"), fallback=_git_commit()),
        "timezone": _text(body.get("timezone"), fallback="Asia/Tokyo"),
        "price_source": _text(body.get("price_source"), fallback="daily_bars"),
        "data_cutoff_at": _text(body.get("data_cutoff_at"), fallback=segments[-1]["end_date"]),
        "random_seed": _int(body.get("random_seed")) or 0,
        "baseline_plan": baseline_plan,
        "candidate_plans": candidate_plans,
        "candidate_limit": 3,
        "run_ids": [],
        "baseline_run_id": None,
        "candidate_run_ids": [],
        "notes": _text(body.get("notes")),
        "status_summary": {"total_runs": 0, "baseline_runs": 0, "candidate_runs": 0, "status_counts": {}},
    }
    with acquire_lock(family_lock_path(family_id)):
        write_json(family_file(family_id), family)
        write_json(baseline_lock_file(family_id), _baseline_lock(family))
    return get_family(family_id) or family


def get_family(family_id: str) -> dict[str, Any] | None:
    return _family_payload(family_id)


def list_families() -> list[dict[str, Any]]:
    return [payload for family_id in list_family_ids() if (payload := _family_payload(family_id))]


def _run_plan(family: dict[str, Any], run_kind: str, plan_id: str | None) -> dict[str, Any]:
    if run_kind == "baseline":
        return family["baseline_plan"]
    for plan in family.get("candidate_plans") or []:
        if isinstance(plan, dict) and _text(plan.get("plan_id")) == _text(plan_id):
            return plan
    raise ValueError("candidate plan not found")


def create_run(*, family_id: str, run_kind: str, plan_id: str | None = None, notes: str | None = None) -> dict[str, Any]:
    with acquire_lock(family_lock_path(family_id)):
        family = load_family(family_id)
        if not family:
            raise ValueError("family not found")
        baseline_run_id = _text(family.get("baseline_run_id"))
        if run_kind not in {"baseline", "candidate"}:
            raise ValueError("run_kind must be baseline or candidate")
        if run_kind == "baseline":
            if baseline_run_id:
                raise ValueError("baseline run already exists")
            plan = _run_plan(family, run_kind, plan_id)
            run_id = f"{family_id}-baseline"
        else:
            if not baseline_run_id:
                raise ValueError("baseline run must succeed before candidate runs")
            baseline_run = load_run(family_id, baseline_run_id)
            if not baseline_run or _text(baseline_run.get("status")) != "succeeded":
                raise ValueError("baseline run must succeed before candidate runs")
            if len([item for item in (family.get("candidate_run_ids") or []) if _text(item)]) >= 3:
                raise ValueError("candidate run limit reached")
            plan = _run_plan(family, run_kind, plan_id)
            run_id = f"{family_id}-{plan['plan_id']}"
        if run_file(family_id, run_id).exists():
            raise ValueError("run already exists")
        run = _run_base(family, run_id, run_kind, plan, notes)
        write_json(run_file(family_id, run_id), run)
        run["status"] = "running"
        write_json(run_file(family_id, run_id), run)
        try:
            run = _build_run_result(family, run)
        except Exception as exc:
            run["status"] = "failed"
            run["error"] = str(exc)
            run["completed_at"] = datetime.now(timezone.utc).isoformat()
        write_json(run_file(family_id, run_id), run)
        family["run_ids"] = [*family.get("run_ids", []), run_id]
        if run_kind == "baseline":
            family["baseline_run_id"] = run_id
            family["frozen"] = True
            family["frozen_at"] = datetime.now(timezone.utc).isoformat()
        else:
            family["candidate_run_ids"] = [*family.get("candidate_run_ids", []), run_id]
        compare = _generate_compare(family)
        if run_kind == "candidate" and compare and _text(run.get("status")) == "succeeded":
            run["status"] = "compared"
            write_json(run_file(family_id, run_id), run)
        _update_family_file(family)
        return run


def get_run(family_id: str, run_id: str) -> dict[str, Any] | None:
    return load_run(family_id, run_id)


def _compare_payload(family: dict[str, Any], baseline: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    baseline_metrics = baseline.get("metrics") if isinstance(baseline.get("metrics"), dict) else {}
    candidate_metrics = candidate.get("metrics") if isinstance(candidate.get("metrics"), dict) else {}
    baseline_overall = baseline_metrics.get("overall") if isinstance(baseline_metrics.get("overall"), dict) else {}
    candidate_overall = candidate_metrics.get("overall") if isinstance(candidate_metrics.get("overall"), dict) else {}
    baseline_primary = baseline_overall.get("primary") if isinstance(baseline_overall.get("primary"), dict) else {}
    candidate_primary = candidate_overall.get("primary") if isinstance(candidate_overall.get("primary"), dict) else {}
    baseline_metrics_absolute = {
        "overall_score": _float(baseline_overall.get("overall_score")) or _overall_score(baseline_primary),
        "by_period_stability": _float(baseline_overall.get("by_period_stability")) or 0.0,
        "symbol_concentration": _float(baseline_overall.get("top_symbol_share")) or 0.0,
        "target_symbol_count": int(baseline_overall.get("target_symbol_count") or 0),
    }
    candidate_metrics_absolute = {
        "overall_score": _float(candidate_overall.get("overall_score")) or _overall_score(candidate_primary),
        "by_period_stability": _float(candidate_overall.get("by_period_stability")) or 0.0,
        "symbol_concentration": _float(candidate_overall.get("top_symbol_share")) or 0.0,
        "target_symbol_count": int(candidate_overall.get("target_symbol_count") or 0),
    }
    metric_directions = {
        "overall_score": "higher",
        "by_period_stability": "higher",
        "symbol_concentration": "lower",
    }
    absolute_metric_comparisons = [
        {
            "metric": "overall_score",
            "direction": metric_directions["overall_score"],
            "baseline": baseline_metrics_absolute["overall_score"],
            "candidate": candidate_metrics_absolute["overall_score"],
            "delta": candidate_metrics_absolute["overall_score"] - baseline_metrics_absolute["overall_score"],
            "pass": candidate_metrics_absolute["overall_score"] >= baseline_metrics_absolute["overall_score"],
        },
        {
            "metric": "by_period_stability",
            "direction": metric_directions["by_period_stability"],
            "baseline": baseline_metrics_absolute["by_period_stability"],
            "candidate": candidate_metrics_absolute["by_period_stability"],
            "delta": candidate_metrics_absolute["by_period_stability"] - baseline_metrics_absolute["by_period_stability"],
            "pass": candidate_metrics_absolute["by_period_stability"] >= baseline_metrics_absolute["by_period_stability"],
        },
        {
            "metric": "symbol_concentration",
            "direction": metric_directions["symbol_concentration"],
            "baseline": baseline_metrics_absolute["symbol_concentration"],
            "candidate": candidate_metrics_absolute["symbol_concentration"],
            "delta": candidate_metrics_absolute["symbol_concentration"] - baseline_metrics_absolute["symbol_concentration"],
            "pass": candidate_metrics_absolute["symbol_concentration"] <= baseline_metrics_absolute["symbol_concentration"],
        },
    ]
    return {
        "run_id": candidate.get("run_id"),
        "plan_id": candidate.get("plan_id"),
        "plan_version": candidate.get("plan_version"),
        "status": candidate.get("status"),
        "metric_directions": metric_directions,
        "baseline_absolute": baseline_metrics_absolute,
        "candidate_absolute": candidate_metrics_absolute,
        "absolute_metric_comparisons": absolute_metric_comparisons,
        "primary_metric_deltas": {
            key: float(candidate_primary.get(key) or 0.0) - float(baseline_primary.get(key) or 0.0)
            for key in sorted(set(baseline_primary) | set(candidate_primary))
        },
        "target_symbol_count_delta": int(candidate_overall.get("target_symbol_count") or 0) - int(baseline_overall.get("target_symbol_count") or 0),
        "signal_date_deltas": {
            "baseline": baseline_overall.get("signal_dates") or [],
            "candidate": candidate_overall.get("signal_dates") or [],
        },
        "winning_examples": (candidate_overall.get("winning_examples") or [])[:3],
        "losing_examples": (candidate_overall.get("losing_examples") or [])[:3],
        "top_conditions": (candidate_overall.get("top_reasons") or [])[:3],
        "review_focus": _candidate_review_focus(candidate),
        "symbol_summary": {
            "baseline_symbols": len(baseline_metrics.get("by_code") or {}),
            "candidate_symbols": len(candidate_metrics.get("by_code") or {}),
            "shared_symbols": len(set((baseline_metrics.get("by_code") or {}).keys()) & set((candidate_metrics.get("by_code") or {}).keys())),
        },
    }


def _generate_compare(family: dict[str, Any]) -> dict[str, Any] | None:
    baseline_id = _text(family.get("baseline_run_id"))
    baseline = load_run(family["family_id"], baseline_id) if baseline_id else None
    if not baseline or _text(baseline.get("status")) != "succeeded":
        return None
    candidate_ids = [str(item) for item in family.get("candidate_run_ids") or [] if _text(item)]
    candidates = [load_run(family["family_id"], run_id) for run_id in candidate_ids]
    candidates = [run for run in candidates if isinstance(run, dict) and _text(run.get("status")) == "succeeded"]
    if not candidates:
        return None
    for candidate in candidates:
        candidate["status"] = "compared"
        write_json(run_file(family["family_id"], str(candidate["run_id"])), candidate)
    payload = {
        "schema_version": TRADEX_COMPARE_SCHEMA_VERSION,
        "family_id": family["family_id"],
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "baseline_run_id": baseline_id,
        "candidate_results": [_compare_payload(family, baseline, candidate) for candidate in candidates],
    }
    write_json(family_compare_file(family["family_id"]), payload)
    return payload


def get_family_compare(family_id: str) -> dict[str, Any] | None:
    compare_path = family_compare_file(family_id)
    payload = read_json(compare_path) if compare_path.exists() else None
    if payload:
        return payload
    family = get_family(family_id)
    if not family:
        return None
    return _generate_compare(family)


def get_run_compare(family_id: str, run_id: str) -> dict[str, Any] | None:
    family = get_family(family_id)
    baseline = load_run(family_id, _text(family.get("baseline_run_id"))) if family and _text(family.get("baseline_run_id")) else None
    candidate = load_run(family_id, run_id)
    if not family or not baseline or not candidate:
        return None
    return _compare_payload(family, baseline, candidate)


def get_run_detail(family_id: str, run_id: str, code: str) -> dict[str, Any] | None:
    code = _text(code)
    if not code:
        return None
    cache_path = run_detail_file(family_id, run_id, code)
    if cache_path.exists():
        payload = read_json(cache_path)
        if payload:
            return payload
    family = get_family(family_id)
    run = load_run(family_id, run_id)
    if not family or not run:
        return None
    analysis = run.get("analysis") if isinstance(run.get("analysis"), dict) else {}
    by_code = analysis.get("by_code") if isinstance(analysis.get("by_code"), dict) else {}
    item = by_code.get(code)
    if not isinstance(item, dict):
        return None
    payload = {
        "schema_version": TRADEX_DETAIL_SCHEMA_VERSION,
        "family_id": family_id,
        "run_id": run_id,
        "run_kind": run.get("run_kind"),
        "plan_id": run.get("plan_id"),
        "plan_version": run.get("plan_version"),
        "code": code,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "sample_count": item.get("sample_count"),
            "signal_count": item.get("signal_count"),
            "signal_rate": item.get("signal_rate"),
            "ready_rate": item.get("ready_rate"),
            "mean_confidence": item.get("mean_confidence"),
            "top_symbol_share": item.get("top_symbol_share"),
            "signal_dates": item.get("signal_dates") or [],
            "top_reasons": item.get("top_reasons") or [],
        },
        "examples": {"winning": item.get("winning_examples") or [], "losing": item.get("losing_examples") or []},
        "samples": item.get("samples") or [],
    }
    with acquire_lock(family_lock_path(family_id)):
        write_json(cache_path, payload)
    return payload


def _top_code(run: dict[str, Any]) -> str:
    by_code = run.get("analysis", {}).get("by_code") if isinstance(run.get("analysis"), dict) else {}
    if isinstance(by_code, dict) and by_code:
        return sorted(by_code.keys())[0]
    return ""


def _gate_result(family: dict[str, Any], baseline: dict[str, Any], candidate: dict[str, Any], compare: dict[str, Any]) -> dict[str, Any]:
    config = _load_gate_config()
    primary_metrics = [str(item) for item in config.get("primary_metrics") or [] if _text(item)]
    metric_direction = config.get("metric_direction") if isinstance(config.get("metric_direction"), dict) else {}
    minimum_effect_size = config.get("minimum_effect_size") if isinstance(config.get("minimum_effect_size"), dict) else {}
    by_period_limit = float(config.get("by_period_deterioration_limit") or 0.5)
    concentration_limit = float(config.get("symbol_concentration_limit") or 0.35)
    detail_reason_required = _bool(config.get("detail_reason_required"), True)
    confirmed_rerun_match_required = _bool(config.get("confirmed_rerun_match_required"), True)

    baseline_metrics = baseline.get("metrics") if isinstance(baseline.get("metrics"), dict) else {}
    candidate_metrics = candidate.get("metrics") if isinstance(candidate.get("metrics"), dict) else {}
    baseline_overall = baseline_metrics.get("overall") if isinstance(baseline_metrics.get("overall"), dict) else {}
    candidate_overall = candidate_metrics.get("overall") if isinstance(candidate_metrics.get("overall"), dict) else {}
    baseline_primary = baseline_overall.get("primary") if isinstance(baseline_overall.get("primary"), dict) else {}
    candidate_primary = candidate_overall.get("primary") if isinstance(candidate_overall.get("primary"), dict) else {}
    primary_results: list[dict[str, Any]] = []
    primary_pass = False
    for metric_name in primary_metrics:
        direction = _text(metric_direction.get(metric_name), fallback="higher")
        minimum = _float(minimum_effect_size.get(metric_name)) or 0.0
        base_value = float(baseline_primary.get(metric_name) or 0.0)
        cand_value = float(candidate_primary.get(metric_name) or 0.0)
        delta = cand_value - base_value
        passed = delta >= minimum if direction != "lower" else delta <= -minimum
        primary_pass = primary_pass or passed
        primary_results.append({"metric": metric_name, "direction": direction, "minimum_effect_size": minimum, "baseline": base_value, "candidate": cand_value, "delta": delta, "pass": passed})

    primary_metric_name = primary_metrics[0] if primary_metrics else "signal_rate"
    baseline_periods = baseline_metrics.get("by_period") if isinstance(baseline_metrics.get("by_period"), list) else []
    candidate_periods = candidate_metrics.get("by_period") if isinstance(candidate_metrics.get("by_period"), list) else []
    period_results: list[dict[str, Any]] = []
    worse_count = 0
    for index, baseline_period in enumerate(baseline_periods):
        candidate_period = candidate_periods[index] if index < len(candidate_periods) else {}
        baseline_period_metrics = baseline_period.get("metrics") if isinstance(baseline_period.get("metrics"), dict) else {}
        candidate_period_metrics = candidate_period.get("metrics") if isinstance(candidate_period.get("metrics"), dict) else {}
        base_value = float(baseline_period_metrics.get(primary_metric_name) or 0.0)
        cand_value = float(candidate_period_metrics.get(primary_metric_name) or 0.0)
        delta = cand_value - base_value
        passed = delta >= 0.0
        if not passed:
            worse_count += 1
        period_results.append({"label": baseline_period.get("label"), "start_date": baseline_period.get("start_date"), "end_date": baseline_period.get("end_date"), "baseline": base_value, "candidate": cand_value, "delta": delta, "pass": passed})
    by_period_pass = True
    if period_results:
        by_period_pass = (worse_count / float(len(period_results))) <= by_period_limit
    symbol_concentration = float(candidate_overall.get("top_symbol_share") or 0.0)
    symbol_pass = symbol_concentration <= concentration_limit
    detail_reason = ""
    candidate_by_code = candidate.get("analysis", {}).get("by_code") if isinstance(candidate.get("analysis"), dict) else {}
    top_code = _top_code(candidate)
    if isinstance(candidate_by_code, dict) and top_code in candidate_by_code:
        detail_item = candidate_by_code.get(top_code)
        if isinstance(detail_item, dict):
            top_reasons = detail_item.get("top_reasons") if isinstance(detail_item.get("top_reasons"), list) else []
            if top_reasons:
                first_reason = top_reasons[0]
                if isinstance(first_reason, dict):
                    detail_reason = _text(first_reason.get("reason"))
                else:
                    detail_reason = _text(first_reason)
    rerun_match = True
    if confirmed_rerun_match_required:
        rerun_compare = get_run_compare(family["family_id"], candidate["run_id"])
        rerun_match = bool(rerun_compare) and _run_compare_signature(compare) == _run_compare_signature(rerun_compare)
    passed = bool(primary_pass and by_period_pass and symbol_pass and (not detail_reason_required or bool(detail_reason)) and (not confirmed_rerun_match_required or rerun_match))
    reasons = []
    if not primary_pass:
        reasons.append("primary_metric_not_improved")
    if not by_period_pass:
        reasons.append("by_period_deterioration_too_high")
    if not symbol_pass:
        reasons.append("symbol_concentration_too_high")
    if detail_reason_required and not detail_reason:
        reasons.append("detail_reason_empty")
    if confirmed_rerun_match_required and not rerun_match:
        reasons.append("confirmed_rerun_mismatch")
    return {
        "schema_version": TRADEX_ADOPT_SCHEMA_VERSION,
        "config": config,
        "primary_results": primary_results,
        "by_period_results": period_results,
        "symbol_concentration": symbol_concentration,
        "symbol_concentration_limit": concentration_limit,
        "detail_reason": detail_reason,
        "rerun_match": rerun_match,
        "pass": passed,
        "reasons": reasons,
    }


def adopt_run(*, family_id: str, run_id: str, reason: str | None = None, actor: str | None = None) -> dict[str, Any]:
    with acquire_lock(family_lock_path(family_id)):
        family = load_family(family_id)
        run = load_run(family_id, run_id)
        if not family or not run:
            raise ValueError("family or run not found")
        if _text(run.get("run_kind")) != "candidate":
            raise ValueError("only candidate runs can be adopted")
        if _text(run.get("status")) not in {"succeeded", "compared"}:
            raise ValueError("candidate run must succeed before adoption")
        compare = get_run_compare(family_id, run_id)
        baseline = load_run(family_id, _text(family.get("baseline_run_id")))
        if not compare or not baseline:
            raise ValueError("compare payload not available")
        gate = _gate_result(family, baseline, run, compare)
        status = "adopt_candidate" if gate["pass"] else "rejected"
        run["status"] = status
        run["completed_at"] = datetime.now(timezone.utc).isoformat()
        run["adopt"] = {
            "schema_version": TRADEX_ADOPT_SCHEMA_VERSION,
            "family_id": family_id,
            "run_id": run_id,
            "reason": _text(reason),
            "actor": _text(actor),
            "evaluated_at": datetime.now(timezone.utc).isoformat(),
            "gate": gate,
        }
        write_json(run_file(family_id, run_id), run)
        write_json(run_adopt_file(family_id, run_id), run["adopt"])
        _update_family_file(family)
        return {"ok": True, "family_id": family_id, "run_id": run_id, "status": status, "gate": gate, "adopt": run["adopt"], "compare": compare}
