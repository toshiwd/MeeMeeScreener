from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import duckdb
import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.backend.services import tradex_research_contracts as contracts


AXIS_ID = "pre_strength_guard_validation_v1"
SCHEMA_PREFIX = "tradex_pre_strength_guard_validation_v1"
SOURCE_AXIS_ID = "pre_strength_pattern_mining_v1"
DEFAULT_SOURCE_ROOT = Path(r"G:\Tradex\pre_strength_pattern_mining_v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\pre_strength_guard_validation_v1")
DEFAULT_SOURCE_RUN_ID = "20260513T000000Z-pre-strength-pattern-mining-v1"

PRIMARY_POSITIVE_GUARD_ID = "safe_full"
PRIMARY_NEGATIVE_GUARD_ID = "already_extended_strong_up_blowoff_veto"
BAD_PICK_RET20_THRESHOLD = -0.03
TOPK_PROXY_REQUIRED_COVERAGE = 0.80

REQUIRED_SOURCE_ARTIFACTS = (
    "_ARTIFACT_COMPLETE.json",
    "evaluation_contract.json",
    "run_manifest.json",
    "feature_availability_audit.json",
    "pre_strength_event_ledger.jsonl",
    "research_decision.json",
)

REQUIRED_ARTIFACTS = (
    "evaluation_contract.json",
    "run_manifest.json",
    "source_artifact_refs.json",
    "guard_hypotheses.json",
    "guarded_event_ledger.jsonl",
    "guard_leaderboard.json",
    "positive_guard_report.json",
    "negative_guard_report.json",
    "ablation_report.json",
    "regime_breakdown.json",
    "time_block_stability.json",
    "topk_rotation_proxy_metrics.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)

SIGNAL_FEATURE_COLUMNS = {
    "pre_ret20_state",
    "pre_ret5_state",
    "pre_ma20_path_state",
    "pre_ma60_context_state",
    "pre_candle_energy_state",
    "pre_wick_warning_state",
    "pre_volume_state",
    "pre_compression_state",
    "weekly_prior_state",
    "monthly_prior_state",
    "event_daily_ret20_state",
    "event_daily_candle_state",
}
LABEL_COLUMNS = {"ret20_fwd", "mfe20", "mae20", "win20", "severe_loss20"}
GUARD_KEY_COLUMNS = {
    "pre_ret20_state",
    "pre_ret5_state",
    "pre_ma20_path_state",
    "pre_ma60_context_state",
    "weekly_prior_state",
    "monthly_prior_state",
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{AXIS_ID}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    try:
        import numpy as np

        if isinstance(value, np.generic):
            return _json_ready(value.item())
    except Exception:
        pass
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _json_text(payload: Any) -> str:
    return json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, default=str)


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_json_text(payload) + "\n", encoding="utf-8")
    return path


def _write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(_json_text(row) + "\n" for row in rows), encoding="utf-8")
    return path


def _stable_hash(payload: Any) -> str:
    return hashlib.sha256(_json_text(payload).encode("utf-8")).hexdigest()


def _safe_path(value: str | Path | None, default: Path) -> Path:
    if value is None or not str(value).strip():
        return default.resolve()
    return Path(str(value)).expanduser().resolve()


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _safe_rate(count: int | float, total: int | float) -> float:
    if not total:
        return 0.0
    return float(count) / float(total)


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _source_run_dir(*, source_run_id: str | None, source_run_dir: str | Path | None, source_root: str | Path) -> Path:
    if source_run_dir and str(source_run_dir).strip():
        return Path(str(source_run_dir)).expanduser().resolve()
    run_id = source_run_id.strip() if isinstance(source_run_id, str) and source_run_id.strip() else DEFAULT_SOURCE_RUN_ID
    return _safe_path(source_root, DEFAULT_SOURCE_ROOT) / run_id


def load_source_artifacts(source_dir: Path) -> dict[str, Any]:
    missing = [name for name in REQUIRED_SOURCE_ARTIFACTS if not (source_dir / name).exists()]
    if missing:
        raise FileNotFoundError(f"source run missing required artifacts: {missing} at {source_dir}")
    artifacts = {name: _load_json(source_dir / name) for name in REQUIRED_SOURCE_ARTIFACTS if name.endswith(".json")}
    complete = artifacts["_ARTIFACT_COMPLETE.json"]
    feature_audit = artifacts["feature_availability_audit.json"]
    if complete.get("complete") is not True:
        raise RuntimeError("source artifact is not complete")
    if complete.get("silent_fallback_used") is not False:
        raise RuntimeError("source artifact used silent fallback")
    if feature_audit.get("used_future_labels_in_pattern_keys") is not False:
        raise RuntimeError("source artifact uses future labels in pattern keys")
    ledger = pd.read_json(source_dir / "pre_strength_event_ledger.jsonl", lines=True)
    if ledger.empty:
        raise RuntimeError("source pre_strength_event_ledger.jsonl is empty")
    return {"source_dir": source_dir, "json": artifacts, "events": _normalize_event_frame(ledger)}


def _normalize_event_frame(events: pd.DataFrame) -> pd.DataFrame:
    frame = events.copy()
    required = SIGNAL_FEATURE_COLUMNS | LABEL_COLUMNS | {"code", "event_date", "event_month"}
    missing = sorted(column for column in required if column not in frame.columns)
    if missing:
        raise ValueError(f"event ledger missing required columns: {missing}")
    frame["code"] = frame["code"].astype(str)
    frame["event_date"] = frame["event_date"].astype(str)
    frame["event_month"] = frame["event_month"].astype(str)
    frame["event_ymd"] = frame["event_date"].str.replace("-", "", regex=False).astype(int)
    frame["event_year"] = frame["event_date"].str.slice(0, 4)
    for column in SIGNAL_FEATURE_COLUMNS:
        frame[column] = frame[column].fillna(f"{column}_unknown").astype(str)
    for column in ("ret20_fwd", "mfe20", "mae20"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["win20"] = frame["win20"].astype(bool)
    frame["severe_loss20"] = frame["severe_loss20"].astype(bool)
    frame = frame.dropna(subset=["ret20_fwd", "mfe20", "mae20"]).copy()
    frame["bad_pick20"] = frame["severe_loss20"] | frame["ret20_fwd"].le(BAD_PICK_RET20_THRESHOLD)
    return frame


def negative_guard_mask(events: pd.DataFrame) -> pd.Series:
    return (
        events["pre_ma20_path_state"].eq("pre_ma20_already_extended")
        | events["pre_ma60_context_state"].eq("pre_ma60_extended_above")
        | events["pre_ret20_state"].eq("pre20_strong_up")
        | events["pre_ret5_state"].eq("pre5_strong_up")
        | events["weekly_prior_state"].eq("weekly_prior_strong_up")
        | events["monthly_prior_state"].eq("monthly_prior_strong_up")
    )


def add_guard_flags(events: pd.DataFrame) -> pd.DataFrame:
    frame = events.copy()
    frame["negative_guard_match"] = negative_guard_mask(frame)
    frame["guard_safe_full"] = (
        frame["pre_ma20_path_state"].eq("pre_ma20_reclaim_base")
        & frame["pre_ret20_state"].eq("pre20_flat")
        & frame["pre_ret5_state"].eq("pre5_flat")
        & frame["monthly_prior_state"].eq("monthly_prior_uptrend")
        & frame["weekly_prior_state"].eq("weekly_prior_mixed")
    )
    frame["guard_safe_without_weekly_mixed"] = (
        frame["pre_ma20_path_state"].eq("pre_ma20_reclaim_base")
        & frame["pre_ret20_state"].eq("pre20_flat")
        & frame["pre_ret5_state"].eq("pre5_flat")
        & frame["monthly_prior_state"].eq("monthly_prior_uptrend")
    )
    frame["guard_ma20_reclaim_only"] = frame["pre_ma20_path_state"].eq("pre_ma20_reclaim_base")
    frame["guard_flat_only"] = frame["pre_ret20_state"].eq("pre20_flat") & frame["pre_ret5_state"].eq("pre5_flat")
    frame["guard_monthly_uptrend_only"] = frame["monthly_prior_state"].eq("monthly_prior_uptrend")
    frame["guard_extended_veto_only"] = ~frame["negative_guard_match"]
    frame["guard_safe_full_plus_extended_veto"] = frame["guard_safe_full"] & ~frame["negative_guard_match"]
    frame["primary_positive_guard_match"] = frame["guard_safe_full"]
    frame["primary_negative_guard_match"] = frame["negative_guard_match"]
    frame["regime_proxy_key"] = frame["weekly_prior_state"] + "|" + frame["monthly_prior_state"]
    return frame


def ablation_masks(events: pd.DataFrame) -> dict[str, pd.Series]:
    return {
        "all_strength_baseline": pd.Series(True, index=events.index),
        "safe_full": events["guard_safe_full"],
        "safe_without_weekly_mixed": events["guard_safe_without_weekly_mixed"],
        "ma20_reclaim_only": events["guard_ma20_reclaim_only"],
        "flat_only": events["guard_flat_only"],
        "monthly_uptrend_only": events["guard_monthly_uptrend_only"],
        "extended_veto_only": events["guard_extended_veto_only"],
        "safe_full_plus_extended_veto": events["guard_safe_full_plus_extended_veto"],
        "negative_guard_matched": events["negative_guard_match"],
    }


def _events_per_day_distribution(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return {"mean": 0.0, "median": 0.0, "p90": 0.0, "max": 0, "days": 0}
    counts = frame.groupby("event_date").size()
    return {
        "mean": float(counts.mean()),
        "median": float(counts.median()),
        "p90": float(counts.quantile(0.90)),
        "max": int(counts.max()),
        "days": int(len(counts)),
    }


def calculate_metrics(frame: pd.DataFrame, *, baseline: pd.DataFrame, total_event_days: int, guard_id: str) -> dict[str, Any]:
    n = int(len(frame))
    baseline_avg_ret = float(baseline["ret20_fwd"].mean()) if len(baseline) else 0.0
    baseline_avg_mae = float(baseline["mae20"].mean()) if len(baseline) else 0.0
    baseline_severe = float(baseline["severe_loss20"].mean()) if len(baseline) else 0.0
    opportunity_days = int(frame["event_date"].nunique()) if n else 0
    avg_mfe = float(frame["mfe20"].mean()) if n else 0.0
    avg_mae = float(frame["mae20"].mean()) if n else 0.0
    severe_rate = float(frame["severe_loss20"].mean()) if n else 0.0
    bad_pick_rate = float(frame["bad_pick20"].mean()) if n else 0.0
    return {
        "guard_id": guard_id,
        "n": n,
        "coverage_rate": _safe_rate(n, len(baseline)),
        "win_rate20": float(frame["win20"].mean()) if n else 0.0,
        "avg_ret20": float(frame["ret20_fwd"].mean()) if n else 0.0,
        "median_ret20": float(frame["ret20_fwd"].median()) if n else 0.0,
        "avg_MFE20": avg_mfe,
        "avg_MAE20": avg_mae,
        "severe_loss_rate20": severe_rate,
        "bad_pick_rate20": bad_pick_rate,
        "mfe_to_abs_mae_ratio": (avg_mfe / abs(avg_mae)) if avg_mae else None,
        "ret20_vs_all_strength_delta": (float(frame["ret20_fwd"].mean()) - baseline_avg_ret) if n else 0.0,
        "mae20_vs_all_strength_delta": (avg_mae - baseline_avg_mae) if n else 0.0,
        "severe_loss_vs_all_strength_delta": severe_rate - baseline_severe,
        "severe_loss_improvement_rate_vs_all_strength": ((baseline_severe - severe_rate) / baseline_severe) if baseline_severe else None,
        "no_trade_days_proxy": int(max(total_event_days - opportunity_days, 0)),
        "no_trade_days_proxy_rate": _safe_rate(max(total_event_days - opportunity_days, 0), total_event_days),
        "opportunity_days_count": opportunity_days,
        "events_per_day_distribution": _events_per_day_distribution(frame),
    }


def build_time_block_stability(events: pd.DataFrame, masks: dict[str, pd.Series], baseline: pd.DataFrame) -> dict[str, Any]:
    rows = []
    summaries = []
    for guard_id, mask in masks.items():
        frame = events.loc[mask].copy()
        total = len(frame)
        positive_blocks = 0
        block_rows = []
        for year, group in frame.groupby("event_year", sort=True):
            metrics = calculate_metrics(group, baseline=baseline, total_event_days=events["event_date"].nunique(), guard_id=guard_id)
            item = {
                "guard_id": guard_id,
                "time_block": str(year),
                "n": metrics["n"],
                "avg_ret20": metrics["avg_ret20"],
                "win_rate20": metrics["win_rate20"],
                "avg_MAE20": metrics["avg_MAE20"],
                "severe_loss_rate20": metrics["severe_loss_rate20"],
                "event_share_within_guard": _safe_rate(len(group), total),
            }
            block_rows.append(item)
            rows.append(item)
            if metrics["avg_ret20"] > 0.0:
                positive_blocks += 1
        top_share = max((row["event_share_within_guard"] for row in block_rows), default=0.0)
        positive_rate = _safe_rate(positive_blocks, len(block_rows))
        summaries.append(
            {
                "guard_id": guard_id,
                "time_block_count": len(block_rows),
                "positive_time_block_rate": positive_rate,
                "top_time_block_event_share": top_share,
                "stable_enough": bool(len(block_rows) >= 5 and positive_rate >= 0.55 and top_share <= 0.45),
            }
        )
    return {
        "schema_version": f"{SCHEMA_PREFIX}_time_block_stability_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "time_block_definition": "calendar_year_from_event_date",
        "summary": summaries,
        "rows": rows,
    }


def build_regime_breakdown(events: pd.DataFrame, masks: dict[str, pd.Series], baseline: pd.DataFrame) -> dict[str, Any]:
    rows = []
    summaries = []
    for guard_id, mask in masks.items():
        frame = events.loc[mask].copy()
        total = len(frame)
        positive_regimes = 0
        tested_regimes = 0
        regime_rows = []
        for regime_key, group in frame.groupby("regime_proxy_key", sort=True):
            metrics = calculate_metrics(group, baseline=baseline, total_event_days=events["event_date"].nunique(), guard_id=guard_id)
            item = {
                "guard_id": guard_id,
                "regime_proxy_key": str(regime_key),
                "n": metrics["n"],
                "avg_ret20": metrics["avg_ret20"],
                "win_rate20": metrics["win_rate20"],
                "avg_MAE20": metrics["avg_MAE20"],
                "severe_loss_rate20": metrics["severe_loss_rate20"],
                "event_share_within_guard": _safe_rate(len(group), total),
            }
            regime_rows.append(item)
            rows.append(item)
            if len(group) >= 30:
                tested_regimes += 1
                if metrics["avg_ret20"] > 0.0:
                    positive_regimes += 1
        top_share = max((row["event_share_within_guard"] for row in regime_rows), default=0.0)
        positive_rate = _safe_rate(positive_regimes, tested_regimes)
        summaries.append(
            {
                "guard_id": guard_id,
                "regime_proxy_count": len(regime_rows),
                "tested_regime_proxy_count": tested_regimes,
                "positive_tested_regime_proxy_rate": positive_rate,
                "top_regime_proxy_event_share": top_share,
                "independent_market_regime_available": False,
                "stable_enough": bool(tested_regimes >= 3 and positive_rate >= 0.50 and top_share <= 0.65),
            }
        )
    return {
        "schema_version": f"{SCHEMA_PREFIX}_regime_breakdown_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "regime_definition": "weekly_prior_state + monthly_prior_state proxy; independent market regime not available in source event ledger",
        "summary": summaries,
        "rows": rows,
    }


def _find_source_db(source_artifacts: dict[str, Any]) -> Path | None:
    contract = source_artifacts["json"].get("evaluation_contract.json", {})
    source_db = contract.get("source_db")
    if not source_db:
        return None
    path = Path(str(source_db))
    return path if path.exists() else None


def build_topk_rotation_proxy_metrics(events: pd.DataFrame, source_artifacts: dict[str, Any]) -> dict[str, Any]:
    source_db = _find_source_db(source_artifacts)
    payload: dict[str, Any] = {
        "schema_version": f"{SCHEMA_PREFIX}_topk_rotation_proxy_metrics_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "topk_rotation_proxy_available": False,
        "reason": "existing ranking score not available on same universe and same period",
        "ranking_table_checked": False,
        "required_coverage_rate": TOPK_PROXY_REQUIRED_COVERAGE,
    }
    if source_db is None:
        payload["reason"] = "source_db_missing_or_not_recorded"
        return payload
    try:
        with duckdb.connect(str(source_db), read_only=True) as conn:
            tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
            if "ranking_appearance_daily" not in tables:
                payload["reason"] = "ranking_appearance_daily_table_missing"
                payload["ranking_table_checked"] = True
                return payload
            ranking = conn.execute(
                """
                SELECT CAST(dt AS INTEGER) AS event_ymd, CAST(code AS VARCHAR) AS code, rank, display_score
                FROM ranking_appearance_daily
                WHERE dir = 'up' AND dt IS NOT NULL AND code IS NOT NULL
                """
            ).fetchdf()
    except Exception as exc:  # pragma: no cover - defensive artifacting only
        payload["reason"] = f"ranking_table_read_failed: {type(exc).__name__}"
        payload["ranking_table_checked"] = True
        return payload
    payload["ranking_table_checked"] = True
    payload["ranking_rows"] = int(len(ranking))
    if ranking.empty:
        payload["reason"] = "ranking_appearance_daily_empty"
        return payload
    joined = events.merge(ranking, on=["event_ymd", "code"], how="left")
    coverage_rate = float(joined["display_score"].notna().mean()) if len(joined) else 0.0
    payload["event_score_coverage_rate"] = coverage_rate
    payload["event_score_covered_count"] = int(joined["display_score"].notna().sum())
    payload["event_count"] = int(len(joined))
    payload["ranking_dt_min"] = int(ranking["event_ymd"].min())
    payload["ranking_dt_max"] = int(ranking["event_ymd"].max())
    payload["event_ymd_min"] = int(events["event_ymd"].min())
    payload["event_ymd_max"] = int(events["event_ymd"].max())
    if coverage_rate < TOPK_PROXY_REQUIRED_COVERAGE:
        payload["reason"] = "ranking_score_coverage_below_same_period_threshold"
        return payload

    scored = joined.dropna(subset=["display_score"]).copy()
    scored["rank_within_event_day"] = scored.groupby("event_date")["display_score"].rank(method="first", ascending=False)
    top3 = scored[scored["rank_within_event_day"] <= 3]
    payload.update(
        {
            "topk_rotation_proxy_available": True,
            "reason": "ranking_appearance_daily_display_score_joined_on_event_date_code",
            "top1_avg_ret20": float(scored[scored["rank_within_event_day"] <= 1]["ret20_fwd"].mean()),
            "top3_avg_ret20": float(top3["ret20_fwd"].mean()),
            "top3_win_rate20": float(top3["win20"].mean()),
            "top3_severe_loss_rate20": float(top3["severe_loss20"].mean()),
            "top3_avg_MAE20": float(top3["mae20"].mean()),
            "changed_top3_members_count_proxy": None,
            "overlap_with_original_top3": None,
            "guard_removed_bad_pick_count": int(joined[joined["negative_guard_match"] & joined["bad_pick20"]].shape[0]),
        }
    )
    return payload


def build_guard_hypotheses() -> dict[str, Any]:
    return {
        "schema_version": f"{SCHEMA_PREFIX}_guard_hypotheses_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary": "TRADEX-only",
        "primary_positive_guard": {
            "guard_id": PRIMARY_POSITIVE_GUARD_ID,
            "conditions": {
                "pre_ma20_path_state": "pre_ma20_reclaim_base",
                "pre_ret20_state": "pre20_flat",
                "pre_ret5_state": "pre5_flat",
                "monthly_prior_state": "monthly_prior_uptrend",
                "weekly_prior_state": "weekly_prior_mixed",
            },
            "purpose": "starter-entry candidate with lighter MAE and severe loss than all newly strong-looking events",
        },
        "primary_negative_guard": {
            "guard_id": PRIMARY_NEGATIVE_GUARD_ID,
            "exclude_if_any": [
                "pre_ma20_already_extended",
                "pre_ma60_extended_above",
                "pre20_strong_up",
                "pre5_strong_up",
                "weekly_prior_strong_up",
                "monthly_prior_strong_up",
            ],
            "purpose": "avoid strong-looking but late-stage/blowoff-like events",
        },
        "future_labels_used_in_pattern_key": False,
        "candidate_scoring_created": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
    }


def build_evaluation_contract(source_dir: Path, source_artifacts: dict[str, Any], events: pd.DataFrame) -> dict[str, Any]:
    source_contract = source_artifacts["json"]["evaluation_contract.json"]
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_evaluation_contract_v1",
        "axis_id": AXIS_ID,
        "research_phase": "guard_validation",
        "boundary": "TRADEX-only",
        "source_axis_id": SOURCE_AXIS_ID,
        "source_artifact_root": str(source_dir),
        "same_condition_controls": {
            "same_universe": True,
            "same_period": True,
            "same_top_k": "max_positions_3_future_operating_constraint; top-K proxy only if existing score is same-period available",
            "same_cost": contracts.TRADEX_DEFAULT_COST_MODEL,
            "same_artifact_detail_level": contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
            "source_contract_hash": source_contract.get("contract_hash"),
        },
        "period": {"start_date": str(int(events["event_ymd"].min())), "end_date": str(int(events["event_ymd"].max()))},
        "event_count": int(len(events)),
        "future_label_policy": {
            "future_labels_used_in_pattern_key": False,
            "future_labels_used_for_evaluation": True,
        },
        "no_silent_fallback": True,
        "silent_fallback_used": False,
        "candidate_scoring_created": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_source_artifact_refs(source_dir: Path, source_artifacts: dict[str, Any]) -> dict[str, Any]:
    refs = []
    for name in REQUIRED_SOURCE_ARTIFACTS:
        path = source_dir / name
        item: dict[str, Any] = {"name": name, "path": str(path), "exists": path.exists()}
        if path.exists() and path.suffix == ".json":
            item["content_hash"] = _stable_hash(_load_json(path))
        refs.append(item)
    return {
        "schema_version": f"{SCHEMA_PREFIX}_source_artifact_refs_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "source_axis_id": SOURCE_AXIS_ID,
        "source_artifact_root": str(source_dir),
        "source_authoritative_research_decision": source_artifacts["json"]["research_decision.json"].get("authoritative_research_decision"),
        "source_silent_fallback_used": source_artifacts["json"]["_ARTIFACT_COMPLETE.json"].get("silent_fallback_used"),
        "refs": refs,
    }


def _decision_reason(code: str, status: str, value: Any, threshold: Any | None = None) -> dict[str, Any]:
    row = {"code": code, "status": status, "value": value}
    if threshold is not None:
        row["threshold"] = threshold
    return row


def build_research_decision(
    *,
    primary: dict[str, Any],
    baseline: dict[str, Any],
    time_stability: dict[str, Any],
    regime_breakdown: dict[str, Any],
    topk_proxy: dict[str, Any],
    artifact_complete: bool,
) -> dict[str, Any]:
    time_summary = {row["guard_id"]: row for row in time_stability["summary"]}
    regime_summary = {row["guard_id"]: row for row in regime_breakdown["summary"]}
    primary_time = time_summary.get(PRIMARY_POSITIVE_GUARD_ID, {})
    primary_regime = regime_summary.get(PRIMARY_POSITIVE_GUARD_ID, {})
    severe_improvement = primary.get("severe_loss_improvement_rate_vs_all_strength")
    metric_pass = (
        primary["n"] >= 250
        and primary["win_rate20"] >= 0.55
        and primary["avg_ret20"] >= baseline["avg_ret20"] + 0.003
        and primary["avg_MAE20"] >= -0.05
        and primary["severe_loss_rate20"] <= 0.10
        and severe_improvement is not None
        and severe_improvement >= 0.40
    )
    time_pass = bool(primary_time.get("stable_enough") is True)
    regime_pass = bool(primary_regime.get("stable_enough") is True)
    topk_available = bool(topk_proxy.get("topk_rotation_proxy_available") is True)
    no_leakage = True
    no_fallback = True
    reasons = [
        _decision_reason("n", "pass" if primary["n"] >= 250 else "fail", primary["n"], ">=250"),
        _decision_reason("win_rate20", "pass" if primary["win_rate20"] >= 0.55 else "fail", primary["win_rate20"], ">=0.55"),
        _decision_reason(
            "avg_ret20_delta_vs_all_strength",
            "pass" if primary["avg_ret20"] >= baseline["avg_ret20"] + 0.003 else "fail",
            primary["avg_ret20"] - baseline["avg_ret20"],
            ">=0.003",
        ),
        _decision_reason("avg_MAE20", "pass" if primary["avg_MAE20"] >= -0.05 else "fail", primary["avg_MAE20"], ">=-0.05"),
        _decision_reason(
            "severe_loss_rate20",
            "pass" if primary["severe_loss_rate20"] <= 0.10 else "fail",
            primary["severe_loss_rate20"],
            "<=0.10",
        ),
        _decision_reason(
            "severe_loss_improvement_rate_vs_all_strength",
            "pass" if severe_improvement is not None and severe_improvement >= 0.40 else "fail",
            severe_improvement,
            ">=0.40",
        ),
        _decision_reason("time_block_stability", "pass" if time_pass else "fail", primary_time),
        _decision_reason("regime_stability", "pass" if regime_pass else "fail", primary_regime),
        _decision_reason("topk_rotation_proxy_available", "pass" if topk_available else "hold_blocker", topk_proxy.get("reason")),
        _decision_reason("artifact_complete", "pass" if artifact_complete else "fail", artifact_complete),
        _decision_reason("future_label_leakage", "pass", False),
        _decision_reason("silent_fallback_used", "pass", False),
    ]
    drop = (
        primary["severe_loss_rate20"] > 0.125
        or primary["avg_MAE20"] < -0.055
        or primary["avg_ret20"] <= baseline["avg_ret20"]
        or not artifact_complete
        or not no_leakage
        or not no_fallback
    )
    if drop:
        decision = "drop"
        authoritative = "pre_strength_guard_drop"
    elif metric_pass and time_pass and regime_pass and topk_available:
        decision = "keep_candidate"
        authoritative = "pre_strength_guard_keep_candidate"
    else:
        decision = "hold"
        authoritative = "pre_strength_guard_hold"
    typed_reasons = []
    if metric_pass:
        typed_reasons.append("primary_guard_metric_gate_passed")
    else:
        typed_reasons.append("primary_guard_metric_gate_incomplete")
    if not topk_available:
        typed_reasons.append("topk_proxy_unavailable_same_period")
    if not regime_pass:
        typed_reasons.append("regime_stability_inconclusive")
    if not time_pass:
        typed_reasons.append("time_block_stability_inconclusive")
    if decision == "drop":
        typed_reasons.append("drop_gate_triggered")
    return {
        "schema_version": f"{SCHEMA_PREFIX}_research_decision_v1",
        "generated_at": _utc_now(),
        "research_phase": "guard_validation",
        "boundary": "TRADEX-only",
        "axis_moved": "pre_strength_chart_pattern_guard_validation",
        "decision": decision,
        "authoritative_research_decision": authoritative,
        "candidate_scoring_created": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
        "silent_fallback_used": False,
        "future_labels_used_in_pattern_key": False,
        "primary_positive_guard": "ma20_reclaim_base_flat_pre20_pre5_monthly_uptrend_weekly_mixed",
        "primary_negative_guard": "already_extended_strong_up_blowoff_veto",
        "typed_reasons": typed_reasons,
        "decision_reasons": reasons,
        "baseline_metrics": baseline,
        "primary_positive_guard_metrics": primary,
        "topk_rotation_proxy_available": topk_available,
        "next_recommended_axis": None if decision != "keep_candidate" else "starter_entry_to_confirmation_add_core_position_v1",
    }


def _artifact_complete(output_dir: Path, paths: dict[str, str], decision: dict[str, Any] | None = None) -> dict[str, Any]:
    excluded = {"_ARTIFACT_COMPLETE.json"}
    if decision is None:
        excluded.add("research_decision.json")
    required_existing = {name: (output_dir / name).exists() for name in REQUIRED_ARTIFACTS if name not in excluded}
    return {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "artifact_root": str(output_dir),
        "complete": all(required_existing.values()),
        "required_artifacts": required_existing,
        "paths": paths,
        "authoritative_research_decision": decision.get("authoritative_research_decision") if decision else None,
        "decision": decision.get("decision") if decision else None,
        "silent_fallback_used": False,
        "candidate_scoring_created": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
    }


def run_pre_strength_guard_validation_v1(
    *,
    source_run_id: str | None = DEFAULT_SOURCE_RUN_ID,
    source_run_dir: str | Path | None = None,
    source_root: str | Path = DEFAULT_SOURCE_ROOT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    run_id: str | None = None,
) -> dict[str, Any]:
    source_dir = _source_run_dir(source_run_id=source_run_id, source_run_dir=source_run_dir, source_root=source_root)
    source_artifacts = load_source_artifacts(source_dir)
    run_name = run_id.strip() if isinstance(run_id, str) and run_id.strip() else _default_run_id()
    output_dir = _safe_path(output_root, DEFAULT_OUTPUT_ROOT) / run_name
    events = add_guard_flags(source_artifacts["events"])
    baseline = events.copy()
    total_event_days = int(events["event_date"].nunique())
    masks = ablation_masks(events)
    metrics_rows = [calculate_metrics(events.loc[mask], baseline=baseline, total_event_days=total_event_days, guard_id=guard_id) for guard_id, mask in masks.items()]
    metrics_by_guard = {row["guard_id"]: row for row in metrics_rows}
    time_stability = build_time_block_stability(events, masks, baseline)
    regime_breakdown = build_regime_breakdown(events, masks, baseline)
    topk_proxy = build_topk_rotation_proxy_metrics(events, source_artifacts)
    source_refs = build_source_artifact_refs(source_dir, source_artifacts)
    guard_hypotheses = build_guard_hypotheses()
    evaluation_contract = build_evaluation_contract(source_dir, source_artifacts, events)
    run_manifest = contracts.build_run_manifest(
        session_id=run_name,
        seed=0,
        random_seed=0,
        input_artifacts=[
            {"name": "source_artifact_root", "path": str(source_dir)},
            {"name": "source_evaluation_contract", "contract_hash": source_artifacts["json"]["evaluation_contract.json"].get("contract_hash")},
            {"name": "evaluation_contract", "contract_hash": evaluation_contract["contract_hash"]},
        ],
        asof=str(int(events["event_ymd"].max())),
        config={
            "axis_id": AXIS_ID,
            "source_axis_id": SOURCE_AXIS_ID,
            "primary_positive_guard_id": PRIMARY_POSITIVE_GUARD_ID,
            "primary_negative_guard_id": PRIMARY_NEGATIVE_GUARD_ID,
            "bad_pick_ret20_threshold": BAD_PICK_RET20_THRESHOLD,
            "max_positions_future_constraint": 3,
            "candidate_scoring_created": False,
        },
        universe=sorted(events["code"].astype(str).unique().tolist()),
        period={"start_date": str(int(events["event_ymd"].min())), "end_date": str(int(events["event_ymd"].max())), "label": "pre_strength_guard_validation"},
        horizon="20d",
        artifact_detail_level=contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        fallback_status=contracts.TRADEX_FALLBACK_STATUS_AUTHORITATIVE,
        cost_model=contracts.TRADEX_DEFAULT_COST_MODEL,
    )
    contracts.validate_run_manifest(run_manifest)
    guard_leaderboard = {
        "schema_version": f"{SCHEMA_PREFIX}_guard_leaderboard_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "overview": {
            "event_count": int(len(events)),
            "event_day_count": total_event_days,
            "guard_count": len(metrics_rows),
            "topk_rotation_proxy_available": bool(topk_proxy.get("topk_rotation_proxy_available") is True),
        },
        "rows": sorted(metrics_rows, key=lambda row: (row["guard_id"] != PRIMARY_POSITIVE_GUARD_ID, -row["avg_ret20"], row["severe_loss_rate20"])),
    }
    positive_guard_report = {
        "schema_version": f"{SCHEMA_PREFIX}_positive_guard_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "primary_positive_guard_id": PRIMARY_POSITIVE_GUARD_ID,
        "primary_positive_guard_metrics": metrics_by_guard[PRIMARY_POSITIVE_GUARD_ID],
        "positive_guard_candidates": [
            metrics_by_guard["safe_full"],
            metrics_by_guard["safe_without_weekly_mixed"],
            metrics_by_guard["safe_full_plus_extended_veto"],
        ],
    }
    negative_guard_report = {
        "schema_version": f"{SCHEMA_PREFIX}_negative_guard_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "primary_negative_guard_id": PRIMARY_NEGATIVE_GUARD_ID,
        "negative_guard_matched_metrics": metrics_by_guard["negative_guard_matched"],
        "extended_veto_remaining_metrics": metrics_by_guard["extended_veto_only"],
        "removed_bad_pick_count": int(events.loc[events["negative_guard_match"] & events["bad_pick20"]].shape[0]),
        "removed_event_count": int(events["negative_guard_match"].sum()),
    }
    ablation_report = {
        "schema_version": f"{SCHEMA_PREFIX}_ablation_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "baseline_guard_id": "all_strength_baseline",
        "rows": metrics_rows,
    }
    paths: dict[str, str] = {}
    for name, payload in {
        "evaluation_contract.json": evaluation_contract,
        "run_manifest.json": run_manifest,
        "source_artifact_refs.json": source_refs,
        "guard_hypotheses.json": guard_hypotheses,
        "guard_leaderboard.json": guard_leaderboard,
        "positive_guard_report.json": positive_guard_report,
        "negative_guard_report.json": negative_guard_report,
        "ablation_report.json": ablation_report,
        "regime_breakdown.json": regime_breakdown,
        "time_block_stability.json": time_stability,
        "topk_rotation_proxy_metrics.json": topk_proxy,
    }.items():
        paths[name] = str(_write_json(output_dir / name, payload))
    ledger_columns = [
        "code",
        "event_date",
        "event_month",
        "event_year",
        "pre_ret20_state",
        "pre_ret5_state",
        "pre_ma20_path_state",
        "pre_ma60_context_state",
        "pre_candle_energy_state",
        "pre_wick_warning_state",
        "pre_volume_state",
        "pre_compression_state",
        "weekly_prior_state",
        "monthly_prior_state",
        "event_daily_ret20_state",
        "event_daily_candle_state",
        "ret20_fwd",
        "mfe20",
        "mae20",
        "win20",
        "severe_loss20",
        "bad_pick20",
        "primary_positive_guard_match",
        "primary_negative_guard_match",
        "negative_guard_match",
        "guard_safe_full",
        "guard_safe_without_weekly_mixed",
        "guard_ma20_reclaim_only",
        "guard_flat_only",
        "guard_monthly_uptrend_only",
        "guard_extended_veto_only",
        "guard_safe_full_plus_extended_veto",
        "regime_proxy_key",
    ]
    paths["guarded_event_ledger.jsonl"] = str(_write_jsonl(output_dir / "guarded_event_ledger.jsonl", events[ledger_columns].to_dict(orient="records")))
    pre_complete = _artifact_complete(output_dir, paths)
    decision = build_research_decision(
        primary=metrics_by_guard[PRIMARY_POSITIVE_GUARD_ID],
        baseline=metrics_by_guard["all_strength_baseline"],
        time_stability=time_stability,
        regime_breakdown=regime_breakdown,
        topk_proxy=topk_proxy,
        artifact_complete=bool(pre_complete["complete"]),
    )
    paths["research_decision.json"] = str(_write_json(output_dir / "research_decision.json", decision))
    complete = _artifact_complete(output_dir, paths, decision)
    paths["_ARTIFACT_COMPLETE.json"] = str(_write_json(output_dir / "_ARTIFACT_COMPLETE.json", complete))
    return {
        "output_dir": str(output_dir),
        "decision": decision["decision"],
        "authoritative_research_decision": decision["authoritative_research_decision"],
        "primary_positive_guard_metrics": decision["primary_positive_guard_metrics"],
        "baseline_metrics": decision["baseline_metrics"],
        "topk_rotation_proxy_available": decision["topk_rotation_proxy_available"],
        "silent_fallback_used": False,
        "candidate_scoring_created": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-run-id", default=DEFAULT_SOURCE_RUN_ID)
    parser.add_argument("--source-run-dir", default="")
    parser.add_argument("--source-root", default=str(DEFAULT_SOURCE_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--run-id", default="")
    args = parser.parse_args(argv)
    result = run_pre_strength_guard_validation_v1(
        source_run_id=args.source_run_id.strip() or None,
        source_run_dir=args.source_run_dir.strip() or None,
        source_root=args.source_root,
        output_root=args.output_root,
        run_id=args.run_id.strip() or None,
    )
    print(_json_text(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
