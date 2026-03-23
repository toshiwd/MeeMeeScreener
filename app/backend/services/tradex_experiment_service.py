from __future__ import annotations

import hashlib
import json
import math
import os
import subprocess
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from app.backend.api.dependencies import get_stock_repo
from app.backend.infra.duckdb.stock_repo import StockRepository
from app.backend.services import swing_expectancy_service
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
from app.db.session import get_conn
from external_analysis.contracts.analysis_input import AnalysisInputContract
from external_analysis.contracts.analysis_output import ANALYSIS_OUTPUT_SCHEMA_VERSION
from external_analysis.runtime.input_normalization import normalize_tradex_analysis_input
from external_analysis.runtime.orchestrator import run_tradex_analysis

TRADEX_FAMILY_SCHEMA_VERSION = "tradex_experiment_family_v1"
TRADEX_RUN_SCHEMA_VERSION = "tradex_experiment_run_v1"
TRADEX_COMPARE_SCHEMA_VERSION = "tradex_experiment_compare_v1"
TRADEX_DETAIL_SCHEMA_VERSION = "tradex_experiment_detail_v1"
TRADEX_ADOPT_SCHEMA_VERSION = "tradex_experiment_adopt_v1"
TRADEX_DIAGNOSTICS_SCHEMA_VERSION = "tradex_diagnostics_v1"
TRADEX_LIQUIDITY20D_MIN = 50_000_000.0
TRADEX_CHALLENGER_SELECTION_VARIANT = "challenger"
TRADEX_CHALLENGER_SELECTION_FORMULA = "analysis_ev_net + ret_20 + readiness + liquidity + regime - penalties"
TRADEX_EVAL_REGIME_LABEL_VERSION = "v1"
TRADEX_EVAL_WINDOW_MIN_TRADING_DAYS = 60
TRADEX_EVAL_REGIME_BUCKET_ORDER = ("up", "down", "flat")
TRADEX_EVAL_REGIME_UP_TAGS = {"risk_on", "risk_on_trend", "risk_on_range", "capitulation_rebound"}
TRADEX_EVAL_REGIME_DOWN_TAGS = {"risk_off", "risk_off_trend", "high_vol_chaos"}
TRADEX_EVAL_REGIME_FLAT_TAGS = {"neutral", "neutral_range"}
PROMOTE_MIN_MONTHLY_WIN_RATE = 0.60
PROMOTE_MAX_WORST_REGIME_UNDERPERFORM_BP = 50.0
PROMOTE_MAX_DD_DEGRADE_BP = 50.0
PROMOTE_MAX_TURNOVER_DEGRADE_RATIO = 0.10
PROMOTE_TOP10_MEAN_TOLERANCE_BP = 20.0
PROMOTE_CAPTURE_DEGRADE_TOLERANCE = 0
PROMOTE_MAX_ZERO_PASS_MONTH_DEGRADE = 0
PROMOTE_MAX_LIQUIDITY_FAIL_DEGRADE_RATIO = 0.0
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


def _stable_hash(value: Any) -> str:
    payload = json.dumps(_json_ready(value), ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _percentile(values: list[float], percent: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(float(item) for item in values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * max(0.0, min(1.0, float(percent)))
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def _trimmed_mean(values: list[float], trim_ratio: float = 0.1) -> float:
    if not values:
        return 0.0
    ordered = sorted(float(item) for item in values)
    if len(ordered) <= 2:
        return sum(ordered) / float(len(ordered))
    trim_ratio = max(0.0, min(0.45, float(trim_ratio)))
    trim = int(len(ordered) * trim_ratio)
    if trim <= 0:
        return sum(ordered) / float(len(ordered))
    upper = len(ordered) - trim
    if upper <= trim:
        return sum(ordered) / float(len(ordered))
    core = ordered[trim:upper]
    return sum(core) / float(len(core)) if core else sum(ordered) / float(len(ordered))


def _mean(values: list[float]) -> float:
    return sum(values) / float(len(values)) if values else 0.0


def _selection_rank_key(row: dict[str, Any]) -> tuple[float, float, str]:
    return (
        -float((row.get("analysis_ev_net") or {}).get("mean") or 0.0),
        -float((row.get("ret_20") or {}).get("mean") or 0.0),
        _text(row.get("code")),
    )


def _selection_period_label(date_text: str, segments: list[dict[str, Any]] | None) -> str:
    if not segments:
        return ""
    for segment in segments:
        if not isinstance(segment, dict):
            continue
        start = _text(segment.get("start_date"))
        end = _text(segment.get("end_date"))
        if not start or not end:
            continue
        if start <= date_text <= end:
            label = _text(segment.get("label"))
            return label or f"{start}..{end}"
    return ""


def _selection_challenger_score(row: dict[str, Any]) -> float:
    analysis_ev_net = _float((row.get("analysis_ev_net") or {}).get("mean")) or 0.0
    ret_20 = _float((row.get("ret_20") or {}).get("mean")) or 0.0
    ret_10 = _float((row.get("ret_10") or {}).get("mean")) or 0.0
    signal_rate = _float(row.get("signal_rate")) or 0.0
    publish_ready_rate = _float(row.get("publish_ready_rate")) or 0.0
    liquidity20d = _float((row.get("liquidity20d") or {}).get("mean")) or 0.0
    liquidity_quality = min(1.0, liquidity20d / TRADEX_LIQUIDITY20D_MIN) if liquidity20d > 0.0 else 0.0
    regime_stability = _float(row.get("regime_stability")) or 0.0
    missing_feature_rate = _float(row.get("missing_feature_rate")) or 0.0
    environment_unresolved_rate = _float(row.get("environment_unresolved_rate")) or 0.0
    liquidity_fail_rate = _float(row.get("liquidity_fail_rate")) or 0.0
    zero_pass_rate = _float(row.get("zero_pass_rate")) or max(0.0, 1.0 - signal_rate)
    score = (
        0.45 * analysis_ev_net
        + 0.30 * ret_20
        + 0.10 * ret_10
        + 0.05 * signal_rate
        + 0.05 * publish_ready_rate
        + 0.05 * liquidity_quality
        + 0.05 * regime_stability
        - 0.10 * missing_feature_rate
        - 0.08 * environment_unresolved_rate
        - 0.05 * liquidity_fail_rate
        - 0.05 * zero_pass_rate
    )
    return float(score)


def _challenger_rank_key(row: dict[str, Any]) -> tuple[float, float, str]:
    return (
        -_selection_challenger_score(row),
        -float((row.get("ret_20") or {}).get("mean") or 0.0),
        _text(row.get("code")),
    )


TRADEX_WATERFALL_STAGE_ORDER = [
    "retrieved",
    "ranked",
    "ready_inputs_complete",
    "gate_pass",
    "published",
]

TRADEX_WATERFALL_REASON_ORDER = [
    "data_missing",
    "as_of_invalid",
    "environment_unresolved",
    "liquidity_fail",
    "gate_rule_fail",
]

_WATERFALL_RAW_REASON_MAP = {
    "missing_feature": "data_missing",
    "data_missing": "data_missing",
    "as_of_invalid": "as_of_invalid",
    "environment_unresolved": "environment_unresolved",
    "liquidity_fail": "liquidity_fail",
    "confidence_below_threshold": "gate_rule_fail",
    "pattern_not_eligible": "gate_rule_fail",
    "top_k_excluded": "gate_rule_fail",
    "minimum_ready_rate_not_met": "gate_rule_fail",
    "other_fallback": "gate_rule_fail",
    "publish_not_ready": "gate_rule_fail",
}


def _waterfall_reason_order(reasons: list[str]) -> list[str]:
    ordered: list[str] = []
    for reason in reasons:
        canonical = _WATERFALL_RAW_REASON_MAP.get(_text(reason), "gate_rule_fail")
        if canonical not in ordered:
            ordered.append(canonical)
    return ordered


def _waterfall_reason(sample: dict[str, Any]) -> str:
    ordered = _waterfall_reason_order(_safe_list(sample.get("publish_not_ready_reasons")))
    if ordered:
        return ordered[0]
    if not _bool(sample.get("publish_ready"), False) or not _bool(sample.get("signal"), False):
        return "gate_rule_fail"
    return "passed"


def _waterfall_stage(sample: dict[str, Any]) -> str:
    if not _bool(sample.get("signal"), False):
        return "ranked"
    reason = _waterfall_reason(sample)
    if reason in {"data_missing", "as_of_invalid", "environment_unresolved", "liquidity_fail"}:
        return "ready_inputs_complete"
    if not _bool(sample.get("publish_ready"), False):
        return "gate_pass"
    return "published"


def _sample_waterfall(sample: dict[str, Any]) -> dict[str, Any]:
    reason_order = _waterfall_reason_order(_safe_list(sample.get("publish_not_ready_reasons")))
    reason = _waterfall_reason(sample)
    ranked = _bool(sample.get("signal"), False)
    ready_inputs_complete = reason not in {"data_missing", "as_of_invalid", "environment_unresolved", "liquidity_fail"}
    gate_pass = ranked and ready_inputs_complete and _bool(sample.get("publish_ready"), False)
    stage = "published" if gate_pass else ("ranked" if not ranked else ("ready_inputs_complete" if not ready_inputs_complete else "gate_pass"))
    return {
        "retrieved": True,
        "ranked": ranked,
        "ready_inputs_complete": ready_inputs_complete,
        "gate_pass": gate_pass,
        "published": gate_pass,
        "failure_stage": stage,
        "failure_reason": reason,
        "reason_order": reason_order or ([reason] if reason != "passed" else []),
        "shadow_gate": {
            "pass": gate_pass,
            "failure_stage": stage,
            "reason": reason,
            "reason_order": reason_order or ([reason] if reason != "passed" else []),
        },
    }


def _ranking_input_hash(sample: dict[str, Any]) -> str:
    payload = {
        "code": _text(sample.get("code")),
        "date": _text(sample.get("date")),
        "feature_hash": _text(sample.get("feature_hash")),
        "engine_input_hash": _text(sample.get("engine_input_hash")),
        "engine_plan_hash": _text(sample.get("engine_plan_hash")),
        "engine_feature_flags": _json_ready(sample.get("engine_feature_flags") or {}),
        "engine_scoring_params": _json_ready(sample.get("engine_scoring_params") or {}),
        "engine_readiness_params": _json_ready(sample.get("engine_readiness_params") or {}),
        "input": _json_ready(sample.get("input") or {}),
    }
    return _stable_hash(payload)


def _selection_value(sample: dict[str, Any], key: str) -> float | None:
    engine_params = sample.get("engine_scoring_params") if isinstance(sample.get("engine_scoring_params"), dict) else {}
    if key == "analysis_ev_net":
        return _float(engine_params.get("analysis_ev_net"))
    sell_analysis = engine_params.get("sell_analysis") if isinstance(engine_params.get("sell_analysis"), dict) else {}
    if key == "short_ret_20":
        return _float(sell_analysis.get("shortRet20"))
    if key == "short_ret_10":
        return _float(sell_analysis.get("shortRet10"))
    if key == "short_ret_5":
        return _float(sell_analysis.get("shortRet5"))
    return None


def _selection_code_summary(
    code: str,
    samples: list[dict[str, Any]],
    *,
    segments: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    expected_values = [value for value in (_selection_value(sample, "analysis_ev_net") for sample in samples) if value is not None]
    realized_returns = [value for value in (_selection_value(sample, "short_ret_20") for sample in samples) if value is not None]
    ret_10 = [value for value in (_selection_value(sample, "short_ret_10") for sample in samples) if value is not None]
    ret_5 = [value for value in (_selection_value(sample, "short_ret_5") for sample in samples) if value is not None]
    liquidity_values = [_float(sample.get("liquidity20d")) for sample in samples if _float(sample.get("liquidity20d")) is not None]
    signal_count = sum(1 for sample in samples if _bool(sample.get("signal"), False))
    publish_ready_count = sum(1 for sample in samples if _bool(sample.get("publish_ready"), False))
    missing_feature_count = sum(1 for sample in samples if "missing_feature" in _safe_list(sample.get("publish_not_ready_reasons")))
    environment_unresolved_count = sum(1 for sample in samples if "environment_unresolved" in _safe_list(sample.get("publish_not_ready_reasons")))
    liquidity_fail_count = sum(1 for sample in samples if "liquidity_fail" in _safe_list(sample.get("publish_not_ready_reasons")))
    zero_pass_count = sum(1 for sample in samples if not _bool(sample.get("signal"), False))
    top_k_base = realized_returns or expected_values
    positive_rate = (sum(1 for value in top_k_base if value > 0.0) / float(len(top_k_base))) if top_k_base else 0.0
    publish_ready_rate = publish_ready_count / float(len(samples)) if samples else 0.0
    signal_rate = signal_count / float(len(samples)) if samples else 0.0
    segment_summaries: list[dict[str, Any]] = []
    regime_stability = 0.0
    if segments:
        segment_return_means: list[float] = []
        for segment in segments:
            if not isinstance(segment, dict):
                continue
            label = _text(segment.get("label"))
            start = _text(segment.get("start_date"))
            end = _text(segment.get("end_date"))
            if not start or not end:
                continue
            segment_samples = [
                sample
                for sample in samples
                if start <= _text(sample.get("date")) <= end
            ]
            segment_expected_values = [value for value in (_selection_value(sample, "analysis_ev_net") for sample in segment_samples) if value is not None]
            segment_realized_returns = [value for value in (_selection_value(sample, "short_ret_20") for sample in segment_samples) if value is not None]
            segment_summary = {
                "label": label or f"{start}..{end}",
                "start_date": start,
                "end_date": end,
                "sample_count": len(segment_samples),
                "analysis_ev_net": {
                    "mean": _mean(segment_expected_values),
                    "median": _percentile(segment_expected_values, 0.5),
                    "trimmed_mean": _trimmed_mean(segment_expected_values),
                },
                "ret_20": {
                    "mean": _mean(segment_realized_returns),
                    "median": _percentile(segment_realized_returns, 0.5),
                    "trimmed_mean": _trimmed_mean(segment_realized_returns),
                },
                "signal_rate": (
                    sum(1 for sample in segment_samples if _bool(sample.get("signal"), False)) / float(len(segment_samples))
                    if segment_samples
                    else 0.0
                ),
                "ready_rate": (
                    sum(1 for sample in segment_samples if _bool(sample.get("publish_ready"), False)) / float(len(segment_samples))
                    if segment_samples
                    else 0.0
                ),
            }
            segment_summaries.append(segment_summary)
            segment_return_means.append(segment_summary["ret_20"]["mean"])
        if segment_return_means:
            spread = max(segment_return_means) - min(segment_return_means)
            denominator = abs(_mean(segment_return_means)) + 1.0
            regime_stability = max(0.0, 1.0 - min(1.0, spread / denominator))
    return {
        "code": code,
        "sample_count": len(samples),
        "signal_count": signal_count,
        "signal_rate": signal_rate,
        "publish_ready_count": publish_ready_count,
        "publish_ready_rate": publish_ready_rate,
        "analysis_ev_net": {
            "mean": _mean(expected_values),
            "median": _percentile(expected_values, 0.5),
            "trimmed_mean": _trimmed_mean(expected_values),
        },
        "ret_20": {
            "mean": _mean(realized_returns),
            "median": _percentile(realized_returns, 0.5),
            "trimmed_mean": _trimmed_mean(realized_returns),
        },
        "ret_10": {
            "mean": _mean(ret_10),
            "median": _percentile(ret_10, 0.5),
            "trimmed_mean": _trimmed_mean(ret_10),
        },
        "ret_5": {
            "mean": _mean(ret_5),
            "median": _percentile(ret_5, 0.5),
            "trimmed_mean": _trimmed_mean(ret_5),
        },
        "positive_contribution_rate": positive_rate,
        "liquidity20d": {
            "mean": _mean(liquidity_values),
            "median": _percentile(liquidity_values, 0.5),
            "trimmed_mean": _trimmed_mean(liquidity_values),
        },
        "missing_feature_rate": (missing_feature_count / float(len(samples))) if samples else 0.0,
        "environment_unresolved_rate": (environment_unresolved_count / float(len(samples))) if samples else 0.0,
        "liquidity_fail_rate": (liquidity_fail_count / float(len(samples))) if samples else 0.0,
        "zero_pass_rate": (zero_pass_count / float(len(samples))) if samples else 0.0,
        "regime_stability": regime_stability,
        "regime_summary": segment_summaries,
    }


def _selection_group_summary(code_summaries: list[dict[str, Any]]) -> dict[str, Any]:
    expected_values = [value for row in code_summaries if isinstance(row, dict) for value in [_float((row.get("analysis_ev_net") or {}).get("mean"))] if value is not None]
    realized_returns = [value for row in code_summaries if isinstance(row, dict) for value in [_float((row.get("ret_20") or {}).get("mean"))] if value is not None]
    liquidity_values = [value for row in code_summaries if isinstance(row, dict) for value in [_float((row.get("liquidity20d") or {}).get("mean"))] if value is not None]
    signal_rates = [value for row in code_summaries if isinstance(row, dict) for value in [_float(row.get("signal_rate"))] if value is not None]
    ready_rates = [value for row in code_summaries if isinstance(row, dict) for value in [_float(row.get("publish_ready_rate"))] if value is not None]
    zero_pass_rates = [value for row in code_summaries if isinstance(row, dict) for value in [_float(row.get("zero_pass_rate"))] if value is not None]
    missing_feature_rates = [value for row in code_summaries if isinstance(row, dict) for value in [_float(row.get("missing_feature_rate"))] if value is not None]
    environment_unresolved_rates = [value for row in code_summaries if isinstance(row, dict) for value in [_float(row.get("environment_unresolved_rate"))] if value is not None]
    liquidity_fail_rates = [value for row in code_summaries if isinstance(row, dict) for value in [_float(row.get("liquidity_fail_rate"))] if value is not None]
    regime_stabilities = [value for row in code_summaries if isinstance(row, dict) for value in [_float(row.get("regime_stability"))] if value is not None]
    return {
        "codes": [_text(row.get("code")) for row in code_summaries if isinstance(row, dict)],
        "analysis_ev_net": {
            "mean": _mean(expected_values),
            "median": _percentile(expected_values, 0.5),
            "trimmed_mean": _trimmed_mean(expected_values),
        },
        "ret_20": {
            "mean": _mean(realized_returns),
            "median": _percentile(realized_returns, 0.5),
            "trimmed_mean": _trimmed_mean(realized_returns),
        },
        "liquidity20d": {
            "mean": _mean(liquidity_values),
            "median": _percentile(liquidity_values, 0.5),
            "trimmed_mean": _trimmed_mean(liquidity_values),
        },
        "signal_rate": _mean(signal_rates),
        "publish_ready_rate": _mean(ready_rates),
        "zero_pass_rate": _mean(zero_pass_rates),
        "missing_feature_rate": _mean(missing_feature_rates),
        "environment_unresolved_rate": _mean(environment_unresolved_rates),
        "liquidity_fail_rate": _mean(liquidity_fail_rates),
        "regime_stability": _mean(regime_stabilities),
        "positive_contribution_rate": (
            sum(1 for value in realized_returns if value > 0.0) / float(len(realized_returns))
            if realized_returns
            else 0.0
        ),
    }


def _selection_summary(
    samples: list[dict[str, Any]],
    *,
    segments: list[dict[str, Any]] | None = None,
    variant: str = "champion",
) -> dict[str, Any]:
    rank_key = _selection_rank_key if variant != TRADEX_CHALLENGER_SELECTION_VARIANT else _challenger_rank_key
    if not samples:
        return {
            "kind": "proxy",
            "source": "timeline_metrics",
            "diagnostics_schema_version": TRADEX_DIAGNOSTICS_SCHEMA_VERSION,
            "selection_variant": variant,
            "selection_formula": "analysis_ev_net -> short_ret_20 -> code"
            if variant != TRADEX_CHALLENGER_SELECTION_VARIANT
            else TRADEX_CHALLENGER_SELECTION_FORMULA,
            "rank_metric": "analysis_ev_net",
            "outcome_metric": "short_ret_20",
            "code_rankings": [],
            "groups": {"top5": {}, "top10": {}, "bottom5": {}},
            "zero_pass_months": 0,
            "turnover_proxy": 0.0,
            "dd_proxy": 0.0,
            "regime_summary": [],
            "monthly_top5_capture": {
                "kind": "proxy",
                "source": "timeline_metrics",
                "definition": "intersection of monthly unions: realized ret_20 top5 vs model ranking top5",
                "month_count": 0,
                "mean": 0.0,
                "median": 0.0,
                "trimmed_mean": 0.0,
                "positive_month_rate": 0.0,
                "capture_count_mean": 0.0,
                "capture_count_median": 0.0,
                "capture_count_trimmed_mean": 0.0,
                "capture_rate_mean": 0.0,
                "capture_rate_median": 0.0,
                "capture_rate_trimmed_mean": 0.0,
                "target_union_count_mean": 0.0,
                "model_union_count_mean": 0.0,
                "target_ret20_mean": 0.0,
                "model_ret20_mean": 0.0,
                "capture_ret20_mean": 0.0,
                "turnover_mean": 0.0,
                "turnover_median": 0.0,
                "turnover_trimmed_mean": 0.0,
                "zero_pass_months": 0,
                "months": [],
            },
        }

    by_code: dict[str, list[dict[str, Any]]] = {}
    by_date: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for sample in samples:
        code = _text(sample.get("code"))
        date_text = _text(sample.get("date"))
        if not code or not date_text:
            continue
        by_code.setdefault(code, []).append(sample)
        by_date.setdefault(date_text, {}).setdefault(code, []).append(sample)

    code_rankings = sorted(
        (_selection_code_summary(code, code_samples, segments=segments) for code, code_samples in by_code.items()),
        key=rank_key,
    )
    code_summary_map = {_text(row.get("code")): row for row in code_rankings}
    top5 = code_rankings[:5]
    top10 = code_rankings[:10]
    bottom5 = list(reversed(code_rankings[-5:])) if len(code_rankings) >= 5 else list(reversed(code_rankings))

    month_capture_counts: list[float] = []
    month_capture_rates: list[float] = []
    month_target_union_counts: list[float] = []
    month_model_union_counts: list[float] = []
    month_target_ret20_means: list[float] = []
    month_model_ret20_means: list[float] = []
    month_capture_ret20_means: list[float] = []
    month_turnovers: list[float] = []
    month_positive_hits = 0
    month_zero_passes = 0
    month_count = 0
    month_details: list[dict[str, Any]] = []
    by_month: dict[str, dict[str, dict[str, list[dict[str, Any]]]]] = {}
    for date_text, code_samples_map in by_date.items():
        month = date_text[:7]
        by_month.setdefault(month, {})[date_text] = code_samples_map

    prev_model_union: set[str] | None = None
    cumulative_model_ret20 = 0.0
    peak_model_ret20 = 0.0
    drawdown_proxy = 0.0

    for month, date_map in sorted(by_month.items()):
        target_union: set[str] = set()
        model_union: set[str] = set()
        for date_text, code_samples_map in sorted(date_map.items()):
            day_rankings = sorted(
                (_selection_code_summary(code, code_samples, segments=segments) for code, code_samples in code_samples_map.items()),
                key=rank_key,
            )
            if not day_rankings:
                continue
            target_codes = {
                _text(row.get("code"))
                for row in sorted(day_rankings, key=lambda row: (-float((row.get("ret_20") or {}).get("mean") or 0.0), _text(row.get("code"))))[:5]
            }
            model_codes = {_text(row.get("code")) for row in day_rankings[:5]}
            target_union.update(code for code in target_codes if code)
            model_union.update(code for code in model_codes if code)
        if not target_union and not model_union:
            continue
        month_count += 1
        capture_codes = sorted(target_union & model_union)
        capture_count = float(len(capture_codes))
        target_count = float(len(target_union))
        model_count = float(len(model_union))
        capture_rate = (capture_count / target_count) if target_count else 0.0
        target_ret20_values = [
            _float((code_summary_map.get(code) or {}).get("ret_20", {}).get("mean"))
            for code in target_union
            if _float((code_summary_map.get(code) or {}).get("ret_20", {}).get("mean")) is not None
        ]
        model_ret20_values = [
            _float((code_summary_map.get(code) or {}).get("ret_20", {}).get("mean"))
            for code in model_union
            if _float((code_summary_map.get(code) or {}).get("ret_20", {}).get("mean")) is not None
        ]
        capture_ret20_values = [
            _float((code_summary_map.get(code) or {}).get("ret_20", {}).get("mean"))
            for code in capture_codes
            if _float((code_summary_map.get(code) or {}).get("ret_20", {}).get("mean")) is not None
        ]
        target_ret20_mean = _mean(target_ret20_values)
        model_ret20_mean = _mean(model_ret20_values)
        capture_ret20_mean = _mean(capture_ret20_values)
        month_capture_counts.append(capture_count)
        month_capture_rates.append(capture_rate)
        month_target_union_counts.append(target_count)
        month_model_union_counts.append(model_count)
        month_target_ret20_means.append(target_ret20_mean)
        month_model_ret20_means.append(model_ret20_mean)
        month_capture_ret20_means.append(capture_ret20_mean)
        if capture_rate > 0.0:
            month_positive_hits += 1
        else:
            month_zero_passes += 1
        if prev_model_union is not None:
            union_size = len(prev_model_union | model_union)
            turnover = 1.0 - (len(prev_model_union & model_union) / float(union_size)) if union_size else 0.0
            month_turnovers.append(turnover)
        prev_model_union = set(model_union)
        cumulative_model_ret20 += model_ret20_mean
        peak_model_ret20 = max(peak_model_ret20, cumulative_model_ret20)
        drawdown_proxy = max(drawdown_proxy, peak_model_ret20 - cumulative_model_ret20)
        month_details.append(
            {
                "month": month,
                "target_union_count": int(target_count),
                "model_union_count": int(model_count),
                "capture_count": int(capture_count),
                "capture_rate": capture_rate,
                "precision_rate": (capture_count / model_count) if model_count else 0.0,
                "target_union_codes": sorted(target_union),
                "model_union_codes": sorted(model_union),
                "capture_codes": capture_codes,
                "target_ret20_mean": target_ret20_mean,
                "model_ret20_mean": model_ret20_mean,
                "capture_ret20_mean": capture_ret20_mean,
                "turnover": month_turnovers[-1] if month_turnovers else 0.0,
            }
        )

    top5_group = _selection_group_summary(top5)
    top10_group = _selection_group_summary(top10)
    bottom5_group = _selection_group_summary(bottom5)
    regime_summary = list(segments or [])
    return {
        "kind": "proxy",
        "source": "timeline_metrics",
        "diagnostics_schema_version": TRADEX_DIAGNOSTICS_SCHEMA_VERSION,
        "selection_variant": variant,
        "selection_formula": "analysis_ev_net -> short_ret_20 -> code"
        if variant != TRADEX_CHALLENGER_SELECTION_VARIANT
        else TRADEX_CHALLENGER_SELECTION_FORMULA,
        "rank_metric": "analysis_ev_net",
        "outcome_metric": "short_ret_20",
        "code_rankings": code_rankings[:10],
        "groups": {
            "top5": top5_group,
            "top10": top10_group,
            "bottom5": bottom5_group,
            "spread": {
                "analysis_ev_net": top5_group["analysis_ev_net"]["mean"] - bottom5_group["analysis_ev_net"]["mean"],
                "ret_20": top5_group["ret_20"]["mean"] - bottom5_group["ret_20"]["mean"],
            },
        },
        "zero_pass_months": month_zero_passes,
        "turnover_proxy": _mean(month_turnovers),
        "dd_proxy": drawdown_proxy,
        "regime_summary": regime_summary,
        "monthly_top5_capture": {
            "kind": "proxy",
            "source": "timeline_metrics",
            "definition": "intersection of monthly unions: realized ret_20 top5 vs model ranking top5",
            "month_count": month_count,
            "mean": _mean(month_capture_rates),
            "median": _percentile(month_capture_rates, 0.5),
            "trimmed_mean": _trimmed_mean(month_capture_rates),
            "positive_month_rate": (month_positive_hits / float(month_count)) if month_count else 0.0,
            "capture_count_mean": _mean(month_capture_counts),
            "capture_count_median": _percentile(month_capture_counts, 0.5),
            "capture_count_trimmed_mean": _trimmed_mean(month_capture_counts),
            "capture_rate_mean": _mean(month_capture_rates),
            "capture_rate_median": _percentile(month_capture_rates, 0.5),
            "capture_rate_trimmed_mean": _trimmed_mean(month_capture_rates),
            "target_union_count_mean": _mean(month_target_union_counts),
            "target_union_count_median": _percentile(month_target_union_counts, 0.5),
            "target_union_count_trimmed_mean": _trimmed_mean(month_target_union_counts),
            "model_union_count_mean": _mean(month_model_union_counts),
            "model_union_count_median": _percentile(month_model_union_counts, 0.5),
            "model_union_count_trimmed_mean": _trimmed_mean(month_model_union_counts),
            "target_ret20_mean": _mean(month_target_ret20_means),
            "target_ret20_median": _percentile(month_target_ret20_means, 0.5),
            "target_ret20_trimmed_mean": _trimmed_mean(month_target_ret20_means),
            "model_ret20_mean": _mean(month_model_ret20_means),
            "model_ret20_median": _percentile(month_model_ret20_means, 0.5),
            "model_ret20_trimmed_mean": _trimmed_mean(month_model_ret20_means),
            "capture_ret20_mean": _mean(month_capture_ret20_means),
            "capture_ret20_median": _percentile(month_capture_ret20_means, 0.5),
            "capture_ret20_trimmed_mean": _trimmed_mean(month_capture_ret20_means),
            "turnover_mean": _mean(month_turnovers),
            "turnover_median": _percentile(month_turnovers, 0.5),
            "turnover_trimmed_mean": _trimmed_mean(month_turnovers),
            "zero_pass_months": month_zero_passes,
            "months": month_details,
        },
    }


def _selection_summary_metric(summary: dict[str, Any], group: str, metric: str, stat: str = "mean") -> float:
    groups = summary.get("groups") if isinstance(summary.get("groups"), dict) else {}
    group_payload = groups.get(group) if isinstance(groups.get(group), dict) else {}
    metric_payload = group_payload.get(metric) if isinstance(group_payload.get(metric), dict) else {}
    return _float(metric_payload.get(stat)) or 0.0


def _selection_month_map(summary: dict[str, Any]) -> dict[str, dict[str, Any]]:
    monthly = summary.get("monthly_top5_capture") if isinstance(summary.get("monthly_top5_capture"), dict) else {}
    months = monthly.get("months") if isinstance(monthly.get("months"), list) else []
    out: dict[str, dict[str, Any]] = {}
    for item in months:
        if not isinstance(item, dict):
            continue
        month = _text(item.get("month"))
        if month:
            out[month] = item
    return out


def _selection_regime_ret20_means(summary: dict[str, Any]) -> list[float]:
    regime_summary = summary.get("regime_summary") if isinstance(summary.get("regime_summary"), list) else []
    means: list[float] = []
    for item in regime_summary:
        if not isinstance(item, dict):
            continue
        metrics = item.get("metrics") if isinstance(item.get("metrics"), dict) else item
        ret_20 = metrics.get("ret_20") if isinstance(metrics.get("ret_20"), dict) else {}
        value = _float(ret_20.get("mean"))
        if value is not None:
            means.append(value)
    return means


def _promotion_thresholds() -> dict[str, float]:
    return {
        "top5_mean_min_delta": 0.0,
        "top10_mean_min_delta": -PROMOTE_TOP10_MEAN_TOLERANCE_BP / 10000.0,
        "top5_median_min_delta": 0.0,
        "monthly_capture_min_delta": float(PROMOTE_CAPTURE_DEGRADE_TOLERANCE),
        "monthly_improvement_min_rate": float(PROMOTE_MIN_MONTHLY_WIN_RATE),
        "worst_regime_min_delta": -PROMOTE_MAX_WORST_REGIME_UNDERPERFORM_BP / 10000.0,
        "turnover_max_delta": float(PROMOTE_MAX_TURNOVER_DEGRADE_RATIO),
        "dd_max_delta": PROMOTE_MAX_DD_DEGRADE_BP / 10000.0,
        "zero_pass_months_max_delta": float(PROMOTE_MAX_ZERO_PASS_MONTH_DEGRADE),
        "top5_liquidity_min_delta": -PROMOTE_MAX_LIQUIDITY_FAIL_DEGRADE_RATIO,
    }


def _evaluation_regime_bucket(regime_id: str) -> str:
    raw = _text(regime_id).lower()
    if raw in TRADEX_EVAL_REGIME_UP_TAGS:
        return "up"
    if raw in TRADEX_EVAL_REGIME_DOWN_TAGS:
        return "down"
    if raw in TRADEX_EVAL_REGIME_FLAT_TAGS:
        return "flat"
    return "flat"


def _format_ymd_int(value: int | str | None) -> str:
    raw = _text(value)
    if len(raw) == 8 and raw.isdigit():
        return f"{raw[0:4]}-{raw[4:6]}-{raw[6:8]}"
    return raw


def _load_evaluation_regime_rows(*, label_version: str = TRADEX_EVAL_REGIME_LABEL_VERSION) -> tuple[list[dict[str, Any]], list[str]]:
    try:
        with get_conn() as conn:
            rows = conn.execute(
                """
                SELECT dt, regime_id, regime_score, label_version
                FROM market_regime_daily
                WHERE label_version = ?
                ORDER BY dt ASC
                """,
                [str(label_version)],
            ).fetchall()
    except Exception as exc:
        return [], [f"market_regime_daily_unavailable:{exc.__class__.__name__}"]

    out: list[dict[str, Any]] = []
    for row in rows:
        dt = _int(row[0])
        regime_id = _text(row[1])
        if dt is None or not regime_id:
            continue
        out.append(
            {
                "dt": dt,
                "date": _format_ymd_int(dt),
                "regime_id": regime_id,
                "regime_tag": _evaluation_regime_bucket(regime_id),
                "regime_score": _float(row[2]),
                "label_version": _text(row[3], fallback=str(label_version)),
            }
        )
    if not out:
        return [], ["market_regime_daily_empty"]
    return out, []


def _select_evaluation_windows(
    regime_rows: list[dict[str, Any]],
    *,
    min_trading_days: int = TRADEX_EVAL_WINDOW_MIN_TRADING_DAYS,
) -> tuple[list[dict[str, Any]], list[str]]:
    if not regime_rows:
        return [], ["regime_rows_empty"]

    segments: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    for row in regime_rows:
        bucket = _text(row.get("regime_tag"), fallback="flat")
        regime_id = _text(row.get("regime_id"))
        dt = _int(row.get("dt"))
        if dt is None:
            continue
        if not current or current["regime_tag"] != bucket:
            if current:
                segments.append(current)
            current = {
                "regime_tag": bucket,
                "regime_ids": [regime_id] if regime_id else [],
                "start_dt": dt,
                "end_dt": dt,
                "trading_day_count": 1,
            }
            continue
        current["end_dt"] = dt
        current["trading_day_count"] = int(current["trading_day_count"]) + 1
        if regime_id:
            current.setdefault("regime_ids", []).append(regime_id)
    if current:
        segments.append(current)

    selected: list[dict[str, Any]] = []
    issues: list[str] = []
    for bucket in TRADEX_EVAL_REGIME_BUCKET_ORDER:
        eligible = [segment for segment in segments if segment.get("regime_tag") == bucket and int(segment.get("trading_day_count") or 0) >= int(min_trading_days)]
        if not eligible:
            issues.append(f"missing_{bucket}_window")
            continue
        chosen = sorted(
            eligible,
            key=lambda segment: (
                -int(segment.get("trading_day_count") or 0),
                int(segment.get("start_dt") or 0),
                int(segment.get("end_dt") or 0),
            ),
        )[0]
        regime_ids = [str(item) for item in chosen.get("regime_ids") or [] if str(item).strip()]
        regime_id_counts = {regime_id: regime_ids.count(regime_id) for regime_id in sorted(set(regime_ids))}
        representative_regime_id = ""
        if regime_id_counts:
            representative_regime_id = sorted(regime_id_counts.items(), key=lambda item: (-item[1], item[0]))[0][0]
        selected.append(
            {
                "evaluation_window_id": f"{bucket}:{chosen['start_dt']}:{chosen['end_dt']}",
                "regime_tag": bucket,
                "regime_id": representative_regime_id or bucket,
                "regime_ids": sorted(set(regime_ids)),
                "start_date": _format_ymd_int(chosen["start_dt"]),
                "end_date": _format_ymd_int(chosen["end_dt"]),
                "start_dt": int(chosen["start_dt"]),
                "end_dt": int(chosen["end_dt"]),
                "trading_day_count": int(chosen["trading_day_count"]),
            }
        )
    return selected, issues


def _window_filter_samples(samples: list[dict[str, Any]], window: dict[str, Any]) -> list[dict[str, Any]]:
    start = _text(window.get("start_date"))
    end = _text(window.get("end_date"))
    if not start or not end:
        return []
    return [
        sample
        for sample in samples
        if start <= _text(sample.get("date")) <= end
    ]


def _evaluation_window_summary(
    champion_samples: list[dict[str, Any]],
    challenger_samples: list[dict[str, Any]],
    window: dict[str, Any],
) -> dict[str, Any]:
    champion_summary = _selection_summary(champion_samples, segments=[window], variant="champion")
    challenger_summary = _selection_summary(challenger_samples, segments=[window], variant=TRADEX_CHALLENGER_SELECTION_VARIANT)
    window_compare = _selection_comparison_summary(champion_summary, challenger_summary)
    return {
        "evaluation_window_id": _text(window.get("evaluation_window_id")),
        "regime_tag": _text(window.get("regime_tag"), fallback="flat"),
        "regime_id": _text(window.get("regime_id")),
        "start_date": _text(window.get("start_date")),
        "end_date": _text(window.get("end_date")),
        "trading_day_count": int(window.get("trading_day_count") or 0),
        "champion_top5_ret20_mean": _selection_summary_metric(champion_summary, "top5", "ret_20", "mean"),
        "challenger_top5_ret20_mean": _selection_summary_metric(challenger_summary, "top5", "ret_20", "mean"),
        "champion_top5_ret20_median": _selection_summary_metric(champion_summary, "top5", "ret_20", "median"),
        "challenger_top5_ret20_median": _selection_summary_metric(challenger_summary, "top5", "ret_20", "median"),
        "champion_top10_ret20_mean": _selection_summary_metric(champion_summary, "top10", "ret_20", "mean"),
        "challenger_top10_ret20_mean": _selection_summary_metric(challenger_summary, "top10", "ret_20", "mean"),
        "champion_top10_ret20_median": _selection_summary_metric(champion_summary, "top10", "ret_20", "median"),
        "challenger_top10_ret20_median": _selection_summary_metric(challenger_summary, "top10", "ret_20", "median"),
        "champion_monthly_top5_capture": champion_summary.get("monthly_top5_capture") if isinstance(champion_summary.get("monthly_top5_capture"), dict) else {},
        "challenger_monthly_top5_capture": challenger_summary.get("monthly_top5_capture") if isinstance(challenger_summary.get("monthly_top5_capture"), dict) else {},
        "champion_zero_pass_months": int(champion_summary.get("zero_pass_months") or 0),
        "challenger_zero_pass_months": int(challenger_summary.get("zero_pass_months") or 0),
        "champion_turnover": _float(champion_summary.get("turnover_proxy")) or 0.0,
        "challenger_turnover": _float(challenger_summary.get("turnover_proxy")) or 0.0,
        "champion_dd": _float(champion_summary.get("dd_proxy")) or 0.0,
        "challenger_dd": _float(challenger_summary.get("dd_proxy")) or 0.0,
        "champion_liquidity_fail_rate": _float((champion_summary.get("groups") or {}).get("top5", {}).get("liquidity_fail_rate")) or 0.0,
        "challenger_liquidity_fail_rate": _float((challenger_summary.get("groups") or {}).get("top5", {}).get("liquidity_fail_rate")) or 0.0,
        "champion_regime_summary": champion_summary.get("regime_summary") if isinstance(champion_summary.get("regime_summary"), list) else [],
        "challenger_regime_summary": challenger_summary.get("regime_summary") if isinstance(challenger_summary.get("regime_summary"), list) else [],
        "selection_compare": window_compare,
        "champion_summary": champion_summary,
        "challenger_summary": challenger_summary,
    }


def _evaluation_overview_summary(
    champion_summary: dict[str, Any],
    challenger_summary: dict[str, Any],
    window_summaries: list[dict[str, Any]],
) -> dict[str, Any]:
    thresholds = _promotion_thresholds()
    champion_top5_mean = _selection_summary_metric(champion_summary, "top5", "ret_20", "mean")
    challenger_top5_mean = _selection_summary_metric(challenger_summary, "top5", "ret_20", "mean")
    champion_top5_median = _selection_summary_metric(champion_summary, "top5", "ret_20", "median")
    challenger_top5_median = _selection_summary_metric(challenger_summary, "top5", "ret_20", "median")
    champion_top10_mean = _selection_summary_metric(champion_summary, "top10", "ret_20", "mean")
    challenger_top10_mean = _selection_summary_metric(challenger_summary, "top10", "ret_20", "mean")
    champion_top10_median = _selection_summary_metric(champion_summary, "top10", "ret_20", "median")
    challenger_top10_median = _selection_summary_metric(challenger_summary, "top10", "ret_20", "median")
    champion_capture = champion_summary.get("monthly_top5_capture") if isinstance(champion_summary.get("monthly_top5_capture"), dict) else {}
    challenger_capture = challenger_summary.get("monthly_top5_capture") if isinstance(challenger_summary.get("monthly_top5_capture"), dict) else {}
    champion_capture_mean = _float(champion_capture.get("mean")) or 0.0
    challenger_capture_mean = _float(challenger_capture.get("mean")) or 0.0
    champion_zero_pass_months = int(champion_summary.get("zero_pass_months") or 0)
    challenger_zero_pass_months = int(challenger_summary.get("zero_pass_months") or 0)
    champion_turnover = _float(champion_summary.get("turnover_proxy")) or 0.0
    challenger_turnover = _float(challenger_summary.get("turnover_proxy")) or 0.0
    champion_dd = _float(champion_summary.get("dd_proxy")) or 0.0
    challenger_dd = _float(challenger_summary.get("dd_proxy")) or 0.0
    champion_liquidity_fail_rate = _float((champion_summary.get("groups") or {}).get("top5", {}).get("liquidity_fail_rate")) or 0.0
    challenger_liquidity_fail_rate = _float((challenger_summary.get("groups") or {}).get("top5", {}).get("liquidity_fail_rate")) or 0.0
    champion_regime_means = _selection_regime_ret20_means(champion_summary)
    challenger_regime_means = _selection_regime_ret20_means(challenger_summary)
    champion_worst_regime = min(champion_regime_means) if champion_regime_means else 0.0
    challenger_worst_regime = min(challenger_regime_means) if challenger_regime_means else 0.0
    window_win_count = sum(
        1
        for row in window_summaries
        if _float(row.get("challenger_top5_ret20_mean")) is not None
        and _float(row.get("champion_top5_ret20_mean")) is not None
        and (_float(row.get("challenger_top5_ret20_mean")) or 0.0) >= (_float(row.get("champion_top5_ret20_mean")) or 0.0)
    )
    window_win_rate = (window_win_count / float(len(window_summaries))) if window_summaries else 0.0
    promote_reasons: list[str] = []
    if challenger_top5_mean < champion_top5_mean + thresholds["top5_mean_min_delta"]:
        promote_reasons.append("top5_ret20_mean_not_improved")
    if challenger_top5_median < champion_top5_median + thresholds["top5_median_min_delta"]:
        promote_reasons.append("top5_ret20_median_not_improved")
    if challenger_top10_mean < champion_top10_mean + thresholds["top10_mean_min_delta"]:
        promote_reasons.append("top10_ret20_mean_too_weak")
    if challenger_top10_median < champion_top10_median + thresholds["top10_mean_min_delta"]:
        promote_reasons.append("top10_ret20_median_too_weak")
    if challenger_capture_mean < champion_capture_mean + thresholds["monthly_capture_min_delta"]:
        promote_reasons.append("monthly_capture_not_improved")
    if window_win_rate < thresholds["monthly_improvement_min_rate"]:
        promote_reasons.append("monthly_window_win_rate_too_low")
    if challenger_worst_regime < champion_worst_regime + thresholds["worst_regime_min_delta"]:
        promote_reasons.append("worst_regime_too_weak")
    if challenger_turnover > champion_turnover + thresholds["turnover_max_delta"]:
        promote_reasons.append("turnover_too_high")
    if challenger_dd > champion_dd + thresholds["dd_max_delta"]:
        promote_reasons.append("drawdown_too_high")
    if challenger_zero_pass_months > champion_zero_pass_months + int(thresholds["zero_pass_months_max_delta"]):
        promote_reasons.append("zero_pass_months_not_improved")
    if challenger_liquidity_fail_rate > champion_liquidity_fail_rate + thresholds["top5_liquidity_min_delta"]:
        promote_reasons.append("liquidity_fail_rate_too_high")
    return {
        "diagnostics_schema_version": TRADEX_DIAGNOSTICS_SCHEMA_VERSION,
        "thresholds": thresholds,
        "evaluation_window_count": len(window_summaries),
        "evaluation_window_ids": [str(row.get("evaluation_window_id") or "") for row in window_summaries],
        "regime_tag": "multi_regime",
        "champion_top5_ret20_mean": champion_top5_mean,
        "challenger_top5_ret20_mean": challenger_top5_mean,
        "champion_top5_ret20_median": champion_top5_median,
        "challenger_top5_ret20_median": challenger_top5_median,
        "champion_top10_ret20_mean": champion_top10_mean,
        "challenger_top10_ret20_mean": challenger_top10_mean,
        "champion_top10_ret20_median": champion_top10_median,
        "challenger_top10_ret20_median": challenger_top10_median,
        "champion_monthly_top5_capture": champion_capture,
        "challenger_monthly_top5_capture": challenger_capture,
        "champion_zero_pass_months": champion_zero_pass_months,
        "challenger_zero_pass_months": challenger_zero_pass_months,
        "champion_dd": champion_dd,
        "challenger_dd": challenger_dd,
        "champion_turnover": champion_turnover,
        "challenger_turnover": challenger_turnover,
        "champion_liquidity_fail_rate": champion_liquidity_fail_rate,
        "challenger_liquidity_fail_rate": challenger_liquidity_fail_rate,
        "champion_regime_summary": champion_summary.get("regime_summary") if isinstance(champion_summary.get("regime_summary"), list) else [],
        "challenger_regime_summary": challenger_summary.get("regime_summary") if isinstance(challenger_summary.get("regime_summary"), list) else [],
        "window_win_rate": window_win_rate,
        "promote_ready": bool(
            window_summaries
            and len(window_summaries) >= 3
            and not promote_reasons
        ),
        "promote_reasons": promote_reasons if window_summaries else ["evaluation_windows_unavailable"],
        "status": "ready" if window_summaries else "incomplete",
    }


def _format_champion_challenger_evaluation_markdown(
    evaluation: dict[str, Any],
    *,
    family_id: str,
    baseline_run_id: str,
    candidate_run_id: str,
    report_path: Path,
) -> str:
    thresholds = evaluation.get("thresholds") if isinstance(evaluation.get("thresholds"), dict) else {}
    windows = evaluation.get("windows") if isinstance(evaluation.get("windows"), list) else []
    lines: list[str] = []
    lines.append("# TRADEX Champion / Challenger Evaluation")
    lines.append("")
    lines.append(f"- family_id: `{family_id}`")
    lines.append(f"- baseline_run_id: `{baseline_run_id}`")
    lines.append(f"- candidate_run_id: `{candidate_run_id}`")
    lines.append(f"- report_path: `{report_path.as_posix()}`")
    lines.append(f"- evaluation_window_id: `{_text(evaluation.get('evaluation_window_id'))}`")
    lines.append(f"- regime_tag: `{_text(evaluation.get('regime_tag'))}`")
    baseline_method = evaluation.get("baseline_method") if isinstance(evaluation.get("baseline_method"), dict) else {}
    candidate_method = evaluation.get("candidate_method") if isinstance(evaluation.get("candidate_method"), dict) else {}
    lines.append(f"- baseline_method_title: `{_text(baseline_method.get('method_title'))}`")
    lines.append(f"- candidate_method_title: `{_text(candidate_method.get('method_title'))}`")
    lines.append(f"- candidate_method_family: `{_text(candidate_method.get('method_family'))}`")
    lines.append(f"- promote_ready: `{bool(evaluation.get('promote_ready'))}`")
    reasons = evaluation.get("promote_reasons") if isinstance(evaluation.get("promote_reasons"), list) else []
    lines.append(f"- promote_reasons: `{', '.join(str(item) for item in reasons) if reasons else 'none'}`")
    lines.append("")
    lines.append("## Definitions")
    lines.append("")
    lines.append("- champion: 現行ランキング")
    lines.append("- challenger: readiness / liquidity / regime / missing penalty を加味した 1 案")
    lines.append("- selection_summary は proxy 診断であり、独立 backtest ではない")
    lines.append("- MeeMee への自動反映はしない。昇格は手動のみ")
    lines.append("")
    lines.append("## Method")
    lines.append("")
    lines.append(f"- champion_method: `{_text(baseline_method.get('method_title'))}`")
    lines.append(f"- challenger_method: `{_text(candidate_method.get('method_title'))}`")
    lines.append(f"- challenger_family: `{_text(candidate_method.get('method_family'))}`")
    lines.append(f"- challenger_thesis: `{_text(candidate_method.get('method_thesis'))}`")
    lines.append("")
    lines.append("## Thresholds")
    lines.append("")
    for key in sorted(thresholds):
        lines.append(f"- {key}: `{thresholds[key]}`")
    lines.append("")
    lines.append("## Aggregate")
    lines.append("")
    lines.append("| metric | champion | challenger | delta |")
    lines.append("| --- | ---: | ---: | ---: |")
    aggregate_metrics = [
        ("top5_ret20_mean", "champion_top5_ret20_mean", "challenger_top5_ret20_mean"),
        ("top5_ret20_median", "champion_top5_ret20_median", "challenger_top5_ret20_median"),
        ("top10_ret20_mean", "champion_top10_ret20_mean", "challenger_top10_ret20_mean"),
        ("top10_ret20_median", "champion_top10_ret20_median", "challenger_top10_ret20_median"),
        ("monthly_top5_capture_mean", "champion_monthly_top5_capture", "challenger_monthly_top5_capture"),
        ("zero_pass_months", "champion_zero_pass_months", "challenger_zero_pass_months"),
        ("dd", "champion_dd", "challenger_dd"),
        ("turnover", "champion_turnover", "challenger_turnover"),
        ("liquidity_fail_rate", "champion_liquidity_fail_rate", "challenger_liquidity_fail_rate"),
        ("window_win_rate", "window_win_rate", None),
    ]
    for metric, champion_key, challenger_key in aggregate_metrics:
        champion_value = evaluation.get(champion_key)
        challenger_value = evaluation.get(challenger_key) if challenger_key else evaluation.get(champion_key)
        if metric == "monthly_top5_capture_mean":
            champion_value = _float((champion_value or {}).get("mean")) if isinstance(champion_value, dict) else _float(champion_value)
            challenger_value = _float((challenger_value or {}).get("mean")) if isinstance(challenger_value, dict) else _float(challenger_value)
        delta = None
        if isinstance(champion_value, (int, float)) and isinstance(challenger_value, (int, float)):
            delta = float(challenger_value) - float(champion_value)
        lines.append(
            f"| {metric} | {_fmt_report_value(champion_value)} | {_fmt_report_value(challenger_value)} | {_fmt_report_value(delta)} |"
        )
    lines.append("")
    lines.append("## Windows")
    lines.append("")
    lines.append("| window | regime | days | champion top5 mean | challenger top5 mean | promote |")
    lines.append("| --- | --- | ---: | ---: | ---: | --- |")
    for window in windows:
        compare = window.get("selection_compare") if isinstance(window.get("selection_compare"), dict) else {}
        lines.append(
            "| {window} | {regime} | {days} | {champion} | {challenger} | {promote} |".format(
                window=_text(window.get("evaluation_window_id")),
                regime=_text(window.get("regime_tag")),
                days=int(window.get("trading_day_count") or 0),
                champion=_fmt_report_value(window.get("champion_top5_ret20_mean")),
                challenger=_fmt_report_value(window.get("challenger_top5_ret20_mean")),
                promote=_fmt_report_value(compare.get("promote_ready")),
            )
        )
    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- champion / challenger の比較は同一 universe・同一期間・同一約定条件・同一 top-K で行う")
    lines.append("- shadow gate は観測専用であり、publish/adopt/compare の判定には干渉しない")
    lines.append("- MeeMee にはまだ反映しない")
    lines.append("- 残余リスク: window 抽出が market_regime_daily の品質に依存する")
    return "\n".join(lines).rstrip() + "\n"


def _fmt_report_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    number = _float(value)
    if number is None:
        return _text(value, fallback="--")
    return f"{number:.4f}"


def _write_champion_challenger_evaluation_report(
    evaluation: dict[str, Any],
    *,
    family_id: str,
    baseline_run_id: str,
    candidate_run_id: str,
) -> tuple[Path, Path]:
    report_dir = REPO_ROOT / "docs" / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"tradex_champion_challenger_eval_{family_id}_{candidate_run_id}.md"
    latest_report_path = report_dir / "tradex_champion_challenger_eval.md"
    markdown = _format_champion_challenger_evaluation_markdown(
        evaluation,
        family_id=family_id,
        baseline_run_id=baseline_run_id,
        candidate_run_id=candidate_run_id,
        report_path=report_path,
    )
    report_path.write_text(markdown, encoding="utf-8")
    latest_report_path.write_text(markdown, encoding="utf-8")
    return report_path, latest_report_path


def run_tradex_champion_challenger_evaluation(
    family_id: str,
    candidate_run_id: str | None = None,
    *,
    emit_report: bool = True,
) -> dict[str, Any] | None:
    family = get_family(family_id)
    if not family:
        return None
    baseline_run_id = _text(family.get("baseline_run_id"))
    if not baseline_run_id:
        return None
    baseline = load_run(family_id, baseline_run_id)
    if not isinstance(baseline, dict) or _text(baseline.get("status")) not in {"succeeded", "compared", "adopt_candidate", "rejected"}:
        return None
    candidate_ids = [str(item) for item in family.get("candidate_run_ids") or [] if _text(item)]
    if candidate_run_id:
        candidate_ids = [run_id for run_id in candidate_ids if run_id == candidate_run_id] or [candidate_run_id]
    for run_id in candidate_ids:
        candidate = load_run(family_id, run_id)
        if not isinstance(candidate, dict) or _text(candidate.get("status")) not in {"succeeded", "compared", "adopt_candidate", "rejected"}:
            continue
        return _build_champion_challenger_evaluation(
            family=family,
            baseline=baseline,
            candidate=candidate,
            emit_report=emit_report,
        )
    return None


def _build_champion_challenger_evaluation(
    *,
    family: dict[str, Any],
    baseline: dict[str, Any],
    candidate: dict[str, Any],
    emit_report: bool = False,
) -> dict[str, Any]:
    baseline_samples = baseline.get("metrics", {}).get("samples") if isinstance(baseline.get("metrics"), dict) else []
    candidate_samples = candidate.get("metrics", {}).get("samples") if isinstance(candidate.get("metrics"), dict) else []
    baseline_samples = [sample for sample in baseline_samples if isinstance(sample, dict)]
    candidate_samples = [sample for sample in candidate_samples if isinstance(sample, dict)]
    regime_rows, regime_issues = _load_evaluation_regime_rows()
    windows, window_issues = _select_evaluation_windows(regime_rows)
    window_summaries = []
    for window in windows:
        champion_window_samples = _window_filter_samples(baseline_samples, window)
        challenger_window_samples = _window_filter_samples(candidate_samples, window)
        window_summaries.append(_evaluation_window_summary(champion_window_samples, challenger_window_samples, window))
    champion_summary = _selection_summary(baseline_samples, segments=windows, variant="champion")
    challenger_summary = _selection_summary(candidate_samples, segments=windows, variant=TRADEX_CHALLENGER_SELECTION_VARIANT)
    champion_summary["regime_summary"] = list(window_summaries)
    challenger_summary["regime_summary"] = list(window_summaries)
    overview = _evaluation_overview_summary(champion_summary, challenger_summary, window_summaries)
    evaluation_window_id = _stable_hash(
        {
            "family_id": family.get("family_id"),
            "baseline_run_id": baseline.get("run_id"),
            "candidate_run_id": candidate.get("run_id"),
            "windows": windows,
        }
    )[:16]
    status_reasons = regime_issues + window_issues
    if not windows:
        status_reasons.append("no_evaluation_windows")
    if len(windows) < 3:
        status_reasons.append("evaluation_window_coverage_incomplete")
    overview.update(
        {
            "evaluation_window_id": evaluation_window_id,
            "family_id": family.get("family_id"),
            "baseline_run_id": baseline.get("run_id"),
            "candidate_run_id": candidate.get("run_id"),
            "candidate_plan_id": candidate.get("plan_id"),
            "candidate_plan_version": candidate.get("plan_version"),
            "baseline_method": _plan_method_metadata(baseline),
            "candidate_method": _plan_method_metadata(candidate),
            "selection_variant": TRADEX_CHALLENGER_SELECTION_VARIANT,
            "champion_selection_summary": champion_summary,
            "challenger_selection_summary": challenger_summary,
            "windows": window_summaries,
            "status_reasons": status_reasons,
        }
    )
    if status_reasons:
        overview["promote_reasons"] = sorted(set([*(overview.get("promote_reasons") or []), *status_reasons]))
        overview["promote_ready"] = bool(overview.get("promote_ready")) and not status_reasons
        if not windows:
            overview["promote_ready"] = False
    if emit_report:
        report_path, latest_report_path = _write_champion_challenger_evaluation_report(
            overview,
            family_id=str(family.get("family_id")),
            baseline_run_id=str(baseline.get("run_id")),
            candidate_run_id=str(candidate.get("run_id")),
        )
        overview["report_path"] = str(report_path)
        overview["latest_report_path"] = str(latest_report_path)
    else:
        overview["report_path"] = None
        overview["latest_report_path"] = None
    return overview


def _selection_comparison_summary(champion: dict[str, Any], challenger: dict[str, Any]) -> dict[str, Any]:
    champion_top5_ret20_mean = _selection_summary_metric(champion, "top5", "ret_20", "mean")
    challenger_top5_ret20_mean = _selection_summary_metric(challenger, "top5", "ret_20", "mean")
    champion_top5_ret20_median = _selection_summary_metric(champion, "top5", "ret_20", "median")
    challenger_top5_ret20_median = _selection_summary_metric(challenger, "top5", "ret_20", "median")
    champion_top10_ret20_mean = _selection_summary_metric(champion, "top10", "ret_20", "mean")
    challenger_top10_ret20_mean = _selection_summary_metric(challenger, "top10", "ret_20", "mean")
    champion_top10_ret20_median = _selection_summary_metric(champion, "top10", "ret_20", "median")
    challenger_top10_ret20_median = _selection_summary_metric(challenger, "top10", "ret_20", "median")
    champion_capture = champion.get("monthly_top5_capture") if isinstance(champion.get("monthly_top5_capture"), dict) else {}
    challenger_capture = challenger.get("monthly_top5_capture") if isinstance(challenger.get("monthly_top5_capture"), dict) else {}
    champion_capture_mean = _float(champion_capture.get("mean")) or 0.0
    challenger_capture_mean = _float(challenger_capture.get("mean")) or 0.0
    champion_months = _selection_month_map(champion)
    challenger_months = _selection_month_map(challenger)
    common_months = sorted(set(champion_months) & set(challenger_months))
    monthly_improvement_months = 0
    for month in common_months:
        champion_month = champion_months.get(month) or {}
        challenger_month = challenger_months.get(month) or {}
        if _float(challenger_month.get("model_ret20_mean")) is not None and _float(champion_month.get("model_ret20_mean")) is not None:
            if (_float(challenger_month.get("model_ret20_mean")) or 0.0) >= (_float(champion_month.get("model_ret20_mean")) or 0.0):
                monthly_improvement_months += 1
    monthly_improvement_rate = (monthly_improvement_months / float(len(common_months))) if common_months else 0.0
    champion_regime_means = _selection_regime_ret20_means(champion)
    challenger_regime_means = _selection_regime_ret20_means(challenger)
    champion_worst_regime = min(champion_regime_means) if champion_regime_means else 0.0
    challenger_worst_regime = min(challenger_regime_means) if challenger_regime_means else 0.0
    champion_top5_liquidity_mean = _selection_summary_metric(champion, "top5", "liquidity20d", "mean")
    challenger_top5_liquidity_mean = _selection_summary_metric(challenger, "top5", "liquidity20d", "mean")
    champion_zero_pass_months = int(champion.get("zero_pass_months") or 0)
    challenger_zero_pass_months = int(challenger.get("zero_pass_months") or 0)
    champion_turnover = _float(champion.get("turnover_proxy")) or 0.0
    challenger_turnover = _float(challenger.get("turnover_proxy")) or 0.0
    champion_dd = _float(champion.get("dd_proxy")) or 0.0
    challenger_dd = _float(challenger.get("dd_proxy")) or 0.0
    thresholds = _promotion_thresholds()
    promote_checks = [
        challenger_top5_ret20_mean >= champion_top5_ret20_mean + thresholds["top5_mean_min_delta"],
        challenger_top5_ret20_median >= champion_top5_ret20_median + thresholds["top5_median_min_delta"],
        challenger_top10_ret20_mean >= champion_top10_ret20_mean + thresholds["top10_mean_min_delta"],
        challenger_top10_ret20_median >= champion_top10_ret20_median + thresholds["top10_mean_min_delta"],
        challenger_capture_mean >= champion_capture_mean + thresholds["monthly_capture_min_delta"],
        monthly_improvement_rate >= thresholds["monthly_improvement_min_rate"],
        challenger_worst_regime >= champion_worst_regime + thresholds["worst_regime_min_delta"],
        challenger_turnover <= champion_turnover + thresholds["turnover_max_delta"],
        challenger_dd <= champion_dd + thresholds["dd_max_delta"],
        challenger_zero_pass_months <= champion_zero_pass_months + int(thresholds["zero_pass_months_max_delta"]),
        challenger_top5_liquidity_mean >= champion_top5_liquidity_mean + thresholds["top5_liquidity_min_delta"],
    ]
    promote_reasons = []
    if challenger_top5_ret20_mean < champion_top5_ret20_mean + thresholds["top5_mean_min_delta"]:
        promote_reasons.append("top5_ret20_mean_not_improved")
    if challenger_top10_ret20_mean < champion_top10_ret20_mean + thresholds["top10_mean_min_delta"]:
        promote_reasons.append("top10_ret20_mean_too_weak")
    if challenger_capture_mean < champion_capture_mean + thresholds["monthly_capture_min_delta"]:
        promote_reasons.append("monthly_capture_not_improved")
    if monthly_improvement_rate < thresholds["monthly_improvement_min_rate"]:
        promote_reasons.append("monthly_improvement_rate_too_low")
    if challenger_worst_regime < champion_worst_regime + thresholds["worst_regime_min_delta"]:
        promote_reasons.append("worst_regime_too_weak")
    if challenger_turnover > champion_turnover + thresholds["turnover_max_delta"]:
        promote_reasons.append("turnover_too_high")
    if challenger_dd > champion_dd + thresholds["dd_max_delta"]:
        promote_reasons.append("drawdown_too_high")
    if challenger_zero_pass_months > champion_zero_pass_months + int(thresholds["zero_pass_months_max_delta"]):
        promote_reasons.append("zero_pass_months_not_improved")
    if challenger_top5_liquidity_mean < champion_top5_liquidity_mean + thresholds["top5_liquidity_min_delta"]:
        promote_reasons.append("liquidity_quality_not_improved")
    return {
        "diagnostics_schema_version": TRADEX_DIAGNOSTICS_SCHEMA_VERSION,
        "thresholds": thresholds,
        "champion_topk_ret20_mean": champion_top5_ret20_mean,
        "challenger_topk_ret20_mean": challenger_top5_ret20_mean,
        "champion_topk_ret20_median": champion_top5_ret20_median,
        "challenger_topk_ret20_median": challenger_top5_ret20_median,
        "champion_topk10_ret20_mean": champion_top10_ret20_mean,
        "challenger_topk10_ret20_mean": challenger_top10_ret20_mean,
        "champion_topk10_ret20_median": champion_top10_ret20_median,
        "challenger_topk10_ret20_median": challenger_top10_ret20_median,
        "champion_monthly_top5_capture": champion_capture,
        "challenger_monthly_top5_capture": challenger_capture,
        "champion_monthly_top5_capture_mean": champion_capture_mean,
        "challenger_monthly_top5_capture_mean": challenger_capture_mean,
        "champion_zero_pass_months": champion_zero_pass_months,
        "challenger_zero_pass_months": challenger_zero_pass_months,
        "champion_regime_summary": champion.get("regime_summary") if isinstance(champion.get("regime_summary"), list) else [],
        "challenger_regime_summary": challenger.get("regime_summary") if isinstance(challenger.get("regime_summary"), list) else [],
        "champion_turnover": champion_turnover,
        "challenger_turnover": challenger_turnover,
        "champion_dd": champion_dd,
        "challenger_dd": challenger_dd,
        "champion_top5_liquidity20d_mean": champion_top5_liquidity_mean,
        "challenger_top5_liquidity20d_mean": challenger_top5_liquidity_mean,
        "monthly_improvement_rate": monthly_improvement_rate,
        "promote_ready": bool(promote_checks and all(promote_checks)),
        "promote_reasons": promote_reasons,
    }


def _waterfall_summary(samples: list[dict[str, Any]]) -> dict[str, Any]:
    stage_counts = {stage: 0 for stage in TRADEX_WATERFALL_STAGE_ORDER}
    failure_stage_counts: dict[str, int] = {}
    failure_reason_counts: dict[str, int] = {}
    shadow_pass_count = 0
    shadow_reason_counts: dict[str, int] = {}
    for sample in samples:
        waterfall = _sample_waterfall(sample)
        if waterfall["retrieved"]:
            stage_counts["retrieved"] += 1
        if waterfall["ranked"]:
            stage_counts["ranked"] += 1
        if waterfall["ready_inputs_complete"]:
            stage_counts["ready_inputs_complete"] += 1
        if waterfall["gate_pass"]:
            stage_counts["gate_pass"] += 1
        if waterfall["published"]:
            stage_counts["published"] += 1
        failure_stage = _text(waterfall.get("failure_stage"), fallback="gate_pass")
        failure_reason = _text(waterfall.get("failure_reason"), fallback="gate_rule_fail")
        if not waterfall["published"]:
            failure_stage_counts[failure_stage] = failure_stage_counts.get(failure_stage, 0) + 1
            failure_reason_counts[failure_reason] = failure_reason_counts.get(failure_reason, 0) + 1
        shadow = waterfall.get("shadow_gate") if isinstance(waterfall.get("shadow_gate"), dict) else {}
        if _bool(shadow.get("pass"), False):
            shadow_pass_count += 1
        elif _text(shadow.get("reason"), fallback="gate_rule_fail") != "passed":
            shadow_reason = _text(shadow.get("reason"), fallback="gate_rule_fail")
            shadow_reason_counts[shadow_reason] = shadow_reason_counts.get(shadow_reason, 0) + 1

    sample_count = len(samples)
    return {
        "diagnostics_schema_version": TRADEX_DIAGNOSTICS_SCHEMA_VERSION,
        "sample_count": sample_count,
        "stage_order": list(TRADEX_WATERFALL_STAGE_ORDER),
        "stage_counts": stage_counts,
        "stage_rates": {
            stage: (stage_counts[stage] / float(sample_count)) if sample_count else 0.0
            for stage in TRADEX_WATERFALL_STAGE_ORDER
        },
        "failure_stage_counts": dict(sorted(failure_stage_counts.items(), key=lambda item: (-item[1], item[0]))),
        "failure_reason_counts": dict(sorted(failure_reason_counts.items(), key=lambda item: (-item[1], item[0]))),
        "shadow_gate": {
            "pass_count": shadow_pass_count,
            "pass_rate": (shadow_pass_count / float(sample_count)) if sample_count else 0.0,
            "reason_order": list(TRADEX_WATERFALL_REASON_ORDER),
            "reason_counts": dict(sorted(shadow_reason_counts.items(), key=lambda item: (-item[1], item[0]))),
        },
    }


def _plan_method_metadata(plan: dict[str, Any]) -> dict[str, str]:
    plan_id = _text(plan.get("plan_id"))
    label = _text(plan.get("label"), fallback=plan_id)
    return {
        "method_id": _text(plan.get("method_id"), fallback=plan_id),
        "method_title": _text(plan.get("method_title"), fallback=label or plan_id),
        "method_thesis": _text(plan.get("method_thesis")),
        "method_family": _text(plan.get("method_family"), fallback=_text(plan.get("plan_family"), fallback="unknown")),
    }


def _plan_effective_parameters(plan: dict[str, Any]) -> dict[str, Any]:
    method_metadata = _plan_method_metadata(plan)
    return {
        "plan_id": _text(plan.get("plan_id")),
        "plan_version": _text(plan.get("plan_version"), fallback="v1"),
        "label": _text(plan.get("label")),
        "minimum_confidence": _float(plan.get("minimum_confidence")),
        "minimum_ready_rate": _float(plan.get("minimum_ready_rate")),
        "signal_bias": _text(plan.get("signal_bias"), fallback="balanced"),
        "top_k": max(1, _int(plan.get("top_k")) or 3),
        "playbook_up_score_bonus": _float(plan.get("playbook_up_score_bonus")) or 0.0,
        "playbook_down_score_bonus": _float(plan.get("playbook_down_score_bonus")) or 0.0,
        "notes": _text(plan.get("notes")),
        "method_id": method_metadata["method_id"],
        "method_title": method_metadata["method_title"],
        "method_thesis": method_metadata["method_thesis"],
        "method_family": method_metadata["method_family"],
    }


def _readiness_config_hash() -> str:
    return _stable_hash(_load_gate_config())


def _sample_gate_reason(output: dict[str, Any], plan: dict[str, Any]) -> str:
    readiness = output.get("publish_readiness") if isinstance(output.get("publish_readiness"), dict) else {}
    confidence = _float(output.get("confidence")) or 0.0
    min_confidence = _float(plan.get("minimum_confidence"))
    bias = _text(plan.get("signal_bias"), fallback="balanced")
    ratios = output.get("side_ratios") if isinstance(output.get("side_ratios"), dict) else {}
    buy = _float(ratios.get("buy")) or 0.0
    sell = _float(ratios.get("sell")) or 0.0
    if not _bool(readiness.get("ready"), False):
        return "publish_not_ready"
    if min_confidence is not None and confidence < min_confidence:
        return "confidence_below_minimum"
    if bias == "buy" and buy < sell:
        return "bias_mismatch"
    if bias == "sell" and sell < buy:
        return "bias_mismatch"
    return "passed"


def _readiness_summary(samples: list[dict[str, Any]], plan: dict[str, Any]) -> dict[str, Any]:
    confidences = [float(item.get("confidence") or 0.0) for item in samples]
    ready_count = sum(1 for item in samples if item.get("publish_ready"))
    signal_count = sum(1 for item in samples if item.get("signal"))
    gate_reason_counts: dict[str, int] = {}
    for item in samples:
        reasons = _safe_list(item.get("publish_not_ready_reasons"))
        if not reasons and not _bool(item.get("publish_ready"), False):
            reasons = ["other_fallback"]
        for reason in reasons:
            gate_reason_counts[reason] = gate_reason_counts.get(reason, 0) + 1
    sample_count = len(samples)
    minimum_ready_rate = _float(plan.get("minimum_ready_rate"))
    ready_post_gate_rate = signal_count / float(sample_count) if sample_count else 0.0
    ready_pre_gate_rate = ready_count / float(sample_count) if sample_count else 0.0
    if sample_count and minimum_ready_rate is not None and ready_post_gate_rate < minimum_ready_rate:
        gate_reason_counts["minimum_ready_rate_not_met"] = sample_count
    return {
        "sample_count": sample_count,
        "ready_pre_gate_rate": ready_pre_gate_rate,
        "ready_post_gate_rate": ready_post_gate_rate,
        "pre_gate_count": ready_count,
        "post_gate_count": signal_count,
        "pre_gate_rate": ready_pre_gate_rate,
        "post_gate_rate": ready_post_gate_rate,
        "raw_readiness_score": {
            "mean": (sum(confidences) / float(sample_count)) if sample_count else 0.0,
            "p50": _percentile(confidences, 0.50),
            "p90": _percentile(confidences, 0.90),
        },
        "signal_pre_filter_count": ready_count,
        "signal_post_filter_count": signal_count,
        "gate_reason_counts": dict(sorted(gate_reason_counts.items(), key=lambda item: (-item[1], item[0]))),
        "plan_thresholds": {
            "minimum_confidence": _float(plan.get("minimum_confidence")),
            "minimum_ready_rate": minimum_ready_rate,
            "signal_bias": _text(plan.get("signal_bias"), fallback="balanced"),
            "top_k": max(1, _int(plan.get("top_k")) or 3),
        },
    }


def _iso_date(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        numeric = int(value)
        if 10000000 <= numeric <= 99999999:
            try:
                return datetime.strptime(str(numeric), "%Y%m%d").date().isoformat()
            except ValueError:
                pass
        if numeric >= 1000000000000:
            numeric = int(numeric / 1000)
        if numeric >= 1000000000:
            return datetime.fromtimestamp(numeric, tz=timezone.utc).date().isoformat()
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        if len(text) == 8:
            try:
                return datetime.strptime(text, "%Y%m%d").date().isoformat()
            except ValueError:
                return None
        if len(text) == 10:
            try:
                return datetime.fromtimestamp(int(text), tz=timezone.utc).date().isoformat()
            except Exception:
                return None
        if len(text) == 13:
            try:
                return datetime.fromtimestamp(int(text) / 1000.0, tz=timezone.utc).date().isoformat()
            except Exception:
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
        "method_id": _text(plan.get("method_id"), fallback=plan_id),
        "method_title": _text(plan.get("method_title"), fallback=_text(plan.get("label"), fallback=plan_id)),
        "method_thesis": _text(plan.get("method_thesis")),
        "method_family": _text(plan.get("method_family"), fallback=_text(plan.get("plan_family"), fallback="unknown")),
        "minimum_confidence": _float(plan.get("minimum_confidence")),
        "minimum_ready_rate": _float(plan.get("minimum_ready_rate")),
        "signal_bias": signal_bias,
        "top_k": max(1, _int(plan.get("top_k")) or 3),
        "playbook_up_score_bonus": _float(plan.get("playbook_up_score_bonus")) or 0.0,
        "playbook_down_score_bonus": _float(plan.get("playbook_down_score_bonus")) or 0.0,
        "notes": _text(plan.get("notes")),
    }


def _normalize_probes(values: Any) -> list[dict[str, str]]:
    if values is None:
        return []
    if not isinstance(values, list):
        raise ValueError("probes must be an array")
    out: list[dict[str, str]] = []
    for index, item in enumerate(values, start=1):
        if not isinstance(item, dict):
            continue
        code = _text(item.get("code"))
        date_text = _iso_date(item.get("date"))
        if not code or not date_text:
            continue
        probe_id = _text(item.get("probe_id"), fallback=f"probe-{index}")
        payload = {"probe_id": probe_id, "code": code, "date": date_text}
        label = _text(item.get("label"))
        if label:
            payload["label"] = label
        out.append(payload)
    if values and len(out) != 3:
        raise ValueError("probes must contain exactly 3 items")
    return out


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
    start_key = start_date
    end_key = end_date
    for row in timeline:
        if not isinstance(row, dict):
            continue
        dt_iso = _iso_date(row.get("dt"))
        if not dt_iso:
            continue
        if dt_iso < start_key or dt_iso > end_key:
            continue
        out.append(row)
    return out


def _analysis_liquidity20d(repo: StockRepository, code: str, dt_key: int) -> float | None:
    asof_dt = _epoch_from_date(_iso_date(dt_key))
    try:
        daily_rows = repo.get_daily_bars(code, limit=20, asof_dt=asof_dt)
    except Exception:
        return None
    _, liquidity20d = swing_expectancy_service.compute_atr_pct_and_liquidity20d(daily_rows)
    return liquidity20d


def _family_probes(family: dict[str, Any]) -> list[dict[str, str]]:
    probes = family.get("probes")
    if isinstance(probes, list):
        normalized = _normalize_probes(probes) if probes else []
        if normalized:
            return normalized
    return []


def _analysis_input(
    code: str,
    dt_key: int,
    point: dict[str, Any],
    *,
    plan_effective: dict[str, Any] | None = None,
    feature_flags: dict[str, str] | None = None,
    readiness_config_hash: str | None = None,
) -> AnalysisInputContract:
    ymd = _iso_date(dt_key)
    if not ymd:
        raise ValueError("invalid analysis dt")
    compact = ymd.replace("-", "")
    effective_parameters = dict(plan_effective or _plan_effective_parameters({}))
    playbook_up_score_bonus = _float(effective_parameters.get("playbook_up_score_bonus")) or 0.0
    playbook_down_score_bonus = _float(effective_parameters.get("playbook_down_score_bonus")) or 0.0
    runtime_kwargs = {
        "symbol": code,
        "asof": ymd,
        "analysis_p_up": point.get("pUp"),
        "analysis_p_down": point.get("pDown"),
        "analysis_p_turn_up": point.get("pTurnUp"),
        "analysis_p_turn_down": point.get("pTurnDown"),
        "analysis_ev_net": point.get("ev20Net"),
        "playbook_up_score_bonus": playbook_up_score_bonus,
        "playbook_down_score_bonus": playbook_down_score_bonus,
        "additive_signals": None,
        "sell_analysis": {
            "dt": int(compact) if compact.isdigit() else dt_key,
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
        "scenarios": [],
    }
    input_contract = AnalysisInputContract(
        symbol=code,
        asof=ymd,
        analysis_p_up=point.get("pUp"),
        analysis_p_down=point.get("pDown"),
        analysis_p_turn_up=point.get("pTurnUp"),
        analysis_p_turn_down=point.get("pTurnDown"),
        analysis_ev_net=point.get("ev20Net"),
        playbook_up_score_bonus=playbook_up_score_bonus,
        playbook_down_score_bonus=playbook_down_score_bonus,
        sell_analysis=runtime_kwargs["sell_analysis"],
        diagnostics={},
    )
    normalized_input = normalize_tradex_analysis_input(input_contract)
    engine_input_hash = _stable_hash(normalized_input.decision_kwargs)
    diagnostics = {
        "feature_hash": engine_input_hash,
        "engine_plan_hash": _stable_hash(effective_parameters),
        "engine_feature_flags": dict(feature_flags or _feature_flags()),
        "engine_scoring_params": {
            "analysis_p_up": runtime_kwargs["analysis_p_up"],
            "analysis_p_down": runtime_kwargs["analysis_p_down"],
            "analysis_p_turn_up": runtime_kwargs["analysis_p_turn_up"],
            "analysis_p_turn_down": runtime_kwargs["analysis_p_turn_down"],
            "analysis_ev_net": runtime_kwargs["analysis_ev_net"],
            "playbook_up_score_bonus": runtime_kwargs["playbook_up_score_bonus"],
            "playbook_down_score_bonus": runtime_kwargs["playbook_down_score_bonus"],
            "sell_analysis": runtime_kwargs["sell_analysis"],
        },
        "engine_readiness_params": {
            "minimum_confidence": _float(effective_parameters.get("minimum_confidence")),
            "minimum_ready_rate": _float(effective_parameters.get("minimum_ready_rate")),
            "signal_bias": _text(effective_parameters.get("signal_bias"), fallback="balanced"),
            "top_k": max(1, _int(effective_parameters.get("top_k")) or 3),
            "readiness_config_hash": readiness_config_hash or _readiness_config_hash(),
        },
        "engine_input_hash": engine_input_hash,
        "liquidity20d": _float(point.get("liquidity20d")),
    }
    input_contract.diagnostics.update(diagnostics)
    return input_contract


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


def _publish_not_ready_reasons(input_contract: AnalysisInputContract, output: dict[str, Any], plan: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    runtime_kwargs = input_contract.to_runtime_kwargs()
    input_diagnostics = dict(input_contract.diagnostics or {})
    output_diagnostics = dict(output.get("diagnostics") or {}) if isinstance(output.get("diagnostics"), dict) else {}
    sell_analysis = dict(runtime_kwargs.get("sell_analysis") or {})
    required_fields = [
        runtime_kwargs.get("analysis_p_up"),
        runtime_kwargs.get("analysis_p_down"),
        runtime_kwargs.get("analysis_p_turn_up"),
        runtime_kwargs.get("analysis_p_turn_down"),
        runtime_kwargs.get("analysis_ev_net"),
        sell_analysis.get("pDown"),
        sell_analysis.get("pTurnDown"),
        sell_analysis.get("trendDown"),
        sell_analysis.get("trendDownStrict"),
    ]
    if any(value is None for value in required_fields):
        reasons.append("missing_feature")
    if _iso_date(input_contract.asof) is None:
        reasons.append("as_of_invalid")
    liquidity20d = _float(input_diagnostics.get("liquidity20d"))
    if liquidity20d is None:
        liquidity20d = _float(output_diagnostics.get("liquidity20d"))
    if liquidity20d is None:
        reasons.append("missing_feature")
    elif liquidity20d < TRADEX_LIQUIDITY20D_MIN:
        reasons.append("liquidity_fail")
    output_reasons = _safe_list(output.get("reasons"))
    if any(("environment=" in reason and ("?" in reason or "unknown" in reason.lower())) for reason in output_reasons):
        reasons.append("environment_unresolved")
    confidence = _float(output.get("confidence")) or 0.0
    min_confidence = _float(plan.get("minimum_confidence"))
    if min_confidence is not None and confidence < min_confidence:
        reasons.append("confidence_below_threshold")
    tone = _text(output.get("tone"))
    bias = _text(plan.get("signal_bias"), fallback="balanced")
    if bias == "buy" and tone not in {"up", "neutral"}:
        reasons.append("pattern_not_eligible")
    elif bias == "sell" and tone not in {"down", "neutral"}:
        reasons.append("pattern_not_eligible")
    comparisons = output.get("candidate_comparisons") if isinstance(output.get("candidate_comparisons"), list) else []
    top_k = max(1, _int(plan.get("top_k")) or 3)
    selected_rank = None
    for item in comparisons:
        if not isinstance(item, dict):
            continue
        if item.get("publish_ready") is True:
            selected_rank = _int(item.get("rank")) or selected_rank
            break
    if selected_rank is not None and selected_rank > top_k:
        reasons.append("top_k_excluded")
    if not reasons:
        reasons.append("other_fallback")
    return reasons


def _sample_trace(
    code: str,
    dt_key: int,
    input_contract: AnalysisInputContract,
    output: dict[str, Any],
    plan: dict[str, Any],
    *,
    plan_effective: dict[str, Any] | None = None,
    feature_flags: dict[str, str] | None = None,
    readiness_config_hash: str | None = None,
) -> dict[str, Any]:
    ymd = _iso_date(dt_key)
    raw_readiness_score = _float(output.get("confidence")) or 0.0
    effective_parameters = dict(plan_effective or _plan_effective_parameters(plan))
    engine_input = input_contract.to_runtime_kwargs()
    input_diagnostics = dict(input_contract.diagnostics or {})
    output_diagnostics = dict(output.get("diagnostics") or {}) if isinstance(output.get("diagnostics"), dict) else {}
    publish_not_ready_reasons = _publish_not_ready_reasons(input_contract, output, plan)
    liquidity20d = _float(input_diagnostics.get("liquidity20d"))
    if liquidity20d is None:
        liquidity20d = _float(output_diagnostics.get("liquidity20d"))
    liquidity20d_source = _text(input_diagnostics.get("liquidity20d_source"))
    if not liquidity20d_source:
        liquidity20d_source = _text(output_diagnostics.get("liquidity20d_source"))
    sample_trace = {
        "code": code,
        "date": ymd or str(dt_key),
        "signal": _signal_flag(output, plan),
        "feature_hash": _text(input_diagnostics.get("feature_hash") or input_diagnostics.get("engine_input_hash")),
        "confidence": raw_readiness_score,
        "readiness_pre_score": raw_readiness_score,
        "raw_readiness_score": raw_readiness_score,
        "analysis_ev_net": _float(engine_input.get("analysis_ev_net")) or 0.0,
        "short_ret_20": _float((engine_input.get("sell_analysis") or {}).get("shortRet20")) or 0.0,
        "short_ret_10": _float((engine_input.get("sell_analysis") or {}).get("shortRet10")) or 0.0,
        "short_ret_5": _float((engine_input.get("sell_analysis") or {}).get("shortRet5")) or 0.0,
        "reasons": _safe_list(output.get("reasons")),
        "candidate_reasons": _safe_list(
            reason
            for comparison in output.get("candidate_comparisons") or []
            if isinstance(comparison, dict)
            for reason in (comparison.get("reasons") or [])
        ),
        "publish_ready": _bool((output.get("publish_readiness") or {}).get("ready"), False) if isinstance(output.get("publish_readiness"), dict) else False,
        "publish_not_ready_reasons": publish_not_ready_reasons,
        "publish_not_ready_reason_label": "+".join(publish_not_ready_reasons),
        "side_ratios": _json_ready(output.get("side_ratios") or {}),
        "input": _json_ready(input_contract.to_dict()),
        "output": _json_ready(output),
        "engine_input_hash": _text(input_diagnostics.get("engine_input_hash") or _stable_hash(engine_input)),
        "engine_plan_hash": _text(input_diagnostics.get("engine_plan_hash") or _stable_hash(effective_parameters)),
        "engine_feature_flags": dict(input_diagnostics.get("engine_feature_flags") or feature_flags or _feature_flags()),
        "engine_scoring_params": dict(input_diagnostics.get("engine_scoring_params") or {
            "analysis_p_up": engine_input.get("analysis_p_up"),
            "analysis_p_down": engine_input.get("analysis_p_down"),
            "analysis_p_turn_up": engine_input.get("analysis_p_turn_up"),
            "analysis_p_turn_down": engine_input.get("analysis_p_turn_down"),
            "analysis_ev_net": engine_input.get("analysis_ev_net"),
            "playbook_up_score_bonus": engine_input.get("playbook_up_score_bonus"),
            "playbook_down_score_bonus": engine_input.get("playbook_down_score_bonus"),
            "sell_analysis": engine_input.get("sell_analysis"),
        }),
        "engine_readiness_params": dict(input_diagnostics.get("engine_readiness_params") or {
            "minimum_confidence": _float(effective_parameters.get("minimum_confidence")),
            "minimum_ready_rate": _float(effective_parameters.get("minimum_ready_rate")),
            "signal_bias": _text(effective_parameters.get("signal_bias"), fallback="balanced"),
            "top_k": max(1, _int(effective_parameters.get("top_k")) or 3),
            "readiness_config_hash": readiness_config_hash or _readiness_config_hash(),
        }),
        "engine_input_diagnostics": _json_ready(input_diagnostics),
        "engine_output_diagnostics": _json_ready(output_diagnostics),
        "liquidity20d": liquidity20d,
    }
    sample_trace["ranking_input_hash"] = _ranking_input_hash(sample_trace)
    sample_trace["liquidity20d_source"] = liquidity20d_source
    sample_trace["waterfall"] = _sample_waterfall(sample_trace)
    sample_trace["shadow_gate"] = sample_trace["waterfall"]["shadow_gate"]
    sample_trace["gate_reason"] = sample_trace["waterfall"]["failure_reason"]
    return sample_trace


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
        for reason in _safe_list(item.get("publish_not_ready_reasons")):
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
        "schema_version": compare.get("schema_version"),
        "diagnostics_schema_version": compare.get("diagnostics_schema_version"),
        "baseline_run_id": compare.get("baseline_run_id"),
        "candidate_results": compare.get("candidate_results"),
    }
    return hashlib.sha256(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _run_compare_signature(compare: dict[str, Any]) -> str:
    payload = {
        "schema_version": compare.get("schema_version"),
        "diagnostics_schema_version": compare.get("diagnostics_schema_version"),
        "metric_directions": compare.get("metric_directions"),
        "baseline_absolute": compare.get("baseline_absolute"),
        "candidate_absolute": compare.get("candidate_absolute"),
        "absolute_metric_comparisons": compare.get("absolute_metric_comparisons"),
        "primary_metric_deltas": compare.get("primary_metric_deltas"),
        "target_symbol_count_delta": compare.get("target_symbol_count_delta"),
        "signal_date_deltas": compare.get("signal_date_deltas"),
        "winning_examples": compare.get("winning_examples"),
        "losing_examples": compare.get("losing_examples"),
        "top_conditions": compare.get("top_conditions"),
        "review_focus": compare.get("review_focus"),
        "diagnostics": compare.get("diagnostics"),
        "symbol_summary": compare.get("symbol_summary"),
    }
    return hashlib.sha256(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _candidate_review_focus(candidate: dict[str, Any]) -> list[dict[str, Any]]:
    analysis = candidate.get("analysis") if isinstance(candidate.get("analysis"), dict) else {}
    by_code = analysis.get("by_code") if isinstance(analysis.get("by_code"), dict) else {}
    overall = candidate.get("metrics", {}).get("overall") if isinstance(candidate.get("metrics"), dict) else {}
    winning_examples = overall.get("winning_examples") if isinstance(overall, dict) and isinstance(overall.get("winning_examples"), list) else []
    losing_examples = overall.get("losing_examples") if isinstance(overall, dict) and isinstance(overall.get("losing_examples"), list) else []
    ranked_codes = sorted(
        [item for item in by_code.values() if isinstance(item, dict)],
        key=lambda item: (
            -int(item.get("signal_count") or 0),
            -float(item.get("mean_confidence") or 0.0),
            _text(item.get("code")),
        ),
    )
    focus: list[dict[str, Any]] = []
    seen: set[str] = set()

    def add_code(code: str, *, source: str, label: str) -> None:
        resolved = _text(code)
        if not resolved or resolved in seen:
            return
        item = by_code.get(resolved)
        seen.add(resolved)
        focus.append(
            {
                "code": resolved,
                "source": source,
                "label": label,
                "signal_count": int(item.get("signal_count") or 0) if isinstance(item, dict) else 0,
                "signal_rate": _float(item.get("signal_rate")) if isinstance(item, dict) else None,
                "ready_rate": _float(item.get("ready_rate")) if isinstance(item, dict) else None,
                "mean_confidence": _float(item.get("mean_confidence")) if isinstance(item, dict) else None,
                "top_symbol_share": _float(item.get("top_symbol_share")) if isinstance(item, dict) else None,
            }
        )

    for index, item in enumerate(ranked_codes[:3]):
        add_code(_text(item.get("code")), source="top_signal", label=f"signal-{index + 1}")

    for item in winning_examples:
        if isinstance(item, dict):
            add_code(_text(item.get("code")), source="winning_example", label="winning-example")
            if len(focus) >= 5:
                break

    for item in losing_examples:
        if len(focus) >= 5:
            break
        if isinstance(item, dict):
            add_code(_text(item.get("code")), source="losing_example", label="losing-example")

    return focus[:5]


def _sample_key(sample: dict[str, Any]) -> tuple[str, str]:
    return _text(sample.get("code")), _text(sample.get("date"))


def _compact_sample(sample: dict[str, Any]) -> dict[str, Any]:
    return {
        "code": _text(sample.get("code")),
        "date": _text(sample.get("date")),
        "feature_hash": _text(sample.get("feature_hash")),
        "engine_input_hash": _text(sample.get("engine_input_hash")),
        "engine_plan_hash": _text(sample.get("engine_plan_hash")),
        "ranking_input_hash": _text(sample.get("ranking_input_hash")),
        "engine_feature_flags": _json_ready(sample.get("engine_feature_flags") or {}),
        "engine_scoring_params": _json_ready(sample.get("engine_scoring_params") or {}),
        "engine_readiness_params": _json_ready(sample.get("engine_readiness_params") or {}),
        "signal": _bool(sample.get("signal"), False),
        "publish_ready": _bool(sample.get("publish_ready"), False),
        "gate_reason": _text(sample.get("gate_reason")),
        "confidence": _float(sample.get("confidence")) or 0.0,
        "readiness_pre_score": _float(sample.get("readiness_pre_score")) or 0.0,
        "raw_readiness_score": _float(sample.get("raw_readiness_score")) or 0.0,
        "analysis_ev_net": _float(sample.get("analysis_ev_net")) or 0.0,
        "short_ret_20": _float(sample.get("short_ret_20")) or 0.0,
        "short_ret_10": _float(sample.get("short_ret_10")) or 0.0,
        "short_ret_5": _float(sample.get("short_ret_5")) or 0.0,
        "reasons": _safe_list(sample.get("reasons")),
        "candidate_reasons": _safe_list(sample.get("candidate_reasons")),
        "publish_not_ready_reasons": _safe_list(sample.get("publish_not_ready_reasons")),
        "publish_not_ready_reason_label": _text(sample.get("publish_not_ready_reason_label")),
        "waterfall": _json_ready(sample.get("waterfall") or {}),
        "shadow_gate": _json_ready(sample.get("shadow_gate") or {}),
        "side_ratios": _json_ready(sample.get("side_ratios") or {}),
        "input_diagnostics": _json_ready(sample.get("engine_input_diagnostics") or {}),
        "output_diagnostics": _json_ready(sample.get("engine_output_diagnostics") or {}),
        "liquidity20d": _float(sample.get("liquidity20d")),
        "liquidity20d_source": _text(sample.get("liquidity20d_source")),
        "trace_hash": _stable_hash({"input": sample.get("input"), "output": sample.get("output")}),
        "ranking_input_hash": _ranking_input_hash(sample),
    }


def _probe_run_entry(sample: dict[str, Any], probe: dict[str, Any]) -> dict[str, Any]:
    compact = _compact_sample(sample)
    return {
        "probe_id": _text(probe.get("probe_id")),
        "code": _text(probe.get("code")),
        "date": _text(probe.get("date")),
        "label": _text(probe.get("label")),
        "feature_hash": compact["feature_hash"],
        "engine_input_hash": compact["engine_input_hash"],
        "engine_plan_hash": compact["engine_plan_hash"],
        "ranking_input_hash": compact["ranking_input_hash"],
        "engine_feature_flags": compact["engine_feature_flags"],
        "engine_scoring_params": compact["engine_scoring_params"],
        "engine_readiness_params": compact["engine_readiness_params"],
        "raw_readiness_score": compact["raw_readiness_score"],
        "readiness_pre_score": compact["readiness_pre_score"],
        "analysis_ev_net": compact["analysis_ev_net"],
        "short_ret_20": compact["short_ret_20"],
        "short_ret_10": compact["short_ret_10"],
        "short_ret_5": compact["short_ret_5"],
        "publish_ready": compact["publish_ready"],
        "publish_not_ready_reasons": compact["publish_not_ready_reasons"],
        "publish_not_ready_reason_label": compact["publish_not_ready_reason_label"],
        "rejection_reason": compact["gate_reason"],
        "missing_feature": "missing_feature" in compact["publish_not_ready_reasons"],
        "environment_unresolved": "environment_unresolved" in compact["publish_not_ready_reasons"],
        "liquidity20d": compact["liquidity20d"],
        "liquidity20d_source": compact["liquidity20d_source"],
        "waterfall": compact["waterfall"],
        "shadow_gate": compact["shadow_gate"],
        "trace_hash": compact["trace_hash"],
    }


def _probe_run_entries(by_code: dict[str, Any], family: dict[str, Any]) -> list[dict[str, Any]]:
    probes = _family_probes(family)
    entries: list[dict[str, Any]] = []
    for probe in probes:
        code = _text(probe.get("code"))
        date_text = _text(probe.get("date"))
        if not code or not date_text:
            continue
        item = by_code.get(code)
        samples = item.get("samples") if isinstance(item, dict) and isinstance(item.get("samples"), list) else []
        sample = next((sample for sample in samples if isinstance(sample, dict) and _text(sample.get("date")) == date_text), None)
        if isinstance(sample, dict):
            entries.append(_probe_run_entry(sample, probe))
    return entries


def _row_diff_entries(baseline: dict[str, Any], candidate: dict[str, Any], *, limit: int = 5) -> list[dict[str, Any]]:
    baseline_analysis = baseline.get("analysis") if isinstance(baseline.get("analysis"), dict) else {}
    candidate_analysis = candidate.get("analysis") if isinstance(candidate.get("analysis"), dict) else {}
    baseline_metrics = baseline.get("metrics") if isinstance(baseline.get("metrics"), dict) else {}
    candidate_metrics = candidate.get("metrics") if isinstance(candidate.get("metrics"), dict) else {}
    baseline_samples = baseline_analysis.get("samples") if isinstance(baseline_analysis.get("samples"), list) else baseline_metrics.get("samples") if isinstance(baseline_metrics.get("samples"), list) else []
    candidate_samples = candidate_analysis.get("samples") if isinstance(candidate_analysis.get("samples"), list) else candidate_metrics.get("samples") if isinstance(candidate_metrics.get("samples"), list) else []
    baseline_map: dict[tuple[str, str], dict[str, Any]] = {}
    candidate_map: dict[tuple[str, str], dict[str, Any]] = {}
    for sample in baseline_samples:
        if isinstance(sample, dict):
            baseline_map[_sample_key(sample)] = sample
    for sample in candidate_samples:
        if isinstance(sample, dict):
            candidate_map[_sample_key(sample)] = sample
    keys: list[tuple[str, str]] = []
    focus_codes = [_text(item.get("code")) for item in _candidate_review_focus(candidate)]
    for code in focus_codes:
        if not code:
            continue
        matching = sorted(
            [key for key in candidate_map if key[0] == code and key in baseline_map],
            key=lambda key: key[1],
        )
        if matching:
            key = matching[0]
            if key not in keys:
                keys.append(key)
        if len(keys) >= limit:
            break
    if len(keys) < limit:
        for key in sorted(set(baseline_map) & set(candidate_map), key=lambda value: (value[0], value[1])):
            if key not in keys:
                keys.append(key)
            if len(keys) >= limit:
                break
    rows: list[dict[str, Any]] = []
    for code, date_text in keys[:limit]:
        baseline_sample = baseline_map.get((code, date_text))
        candidate_sample = candidate_map.get((code, date_text))
        if not isinstance(baseline_sample, dict) or not isinstance(candidate_sample, dict):
            continue
        baseline_compact = _compact_sample(baseline_sample)
        candidate_compact = _compact_sample(candidate_sample)
        rows.append(
            {
                "code": code,
                "date": date_text,
                "baseline": baseline_compact,
                "candidate": candidate_compact,
                "delta": {
                    "feature_hash_changed": baseline_compact["feature_hash"] != candidate_compact["feature_hash"],
                    "engine_input_hash_changed": baseline_compact["engine_input_hash"] != candidate_compact["engine_input_hash"],
                    "engine_plan_hash_changed": baseline_compact["engine_plan_hash"] != candidate_compact["engine_plan_hash"],
                    "ranking_input_hash_changed": baseline_compact["ranking_input_hash"] != candidate_compact["ranking_input_hash"],
                    "confidence": candidate_compact["confidence"] - baseline_compact["confidence"],
                    "readiness_pre_score": candidate_compact["readiness_pre_score"] - baseline_compact["readiness_pre_score"],
                    "raw_readiness_score": candidate_compact["raw_readiness_score"] - baseline_compact["raw_readiness_score"],
                    "analysis_ev_net": candidate_compact["analysis_ev_net"] - baseline_compact["analysis_ev_net"],
                    "short_ret_20": candidate_compact["short_ret_20"] - baseline_compact["short_ret_20"],
                    "short_ret_10": candidate_compact["short_ret_10"] - baseline_compact["short_ret_10"],
                    "short_ret_5": candidate_compact["short_ret_5"] - baseline_compact["short_ret_5"],
                    "liquidity20d_source_changed": baseline_compact["liquidity20d_source"] != candidate_compact["liquidity20d_source"],
                    "signal_changed": baseline_compact["signal"] != candidate_compact["signal"],
                    "publish_ready_changed": baseline_compact["publish_ready"] != candidate_compact["publish_ready"],
                    "gate_reason_changed": baseline_compact["gate_reason"] != candidate_compact["gate_reason"],
                    "publish_not_ready_reason_changed": baseline_compact["publish_not_ready_reason_label"] != candidate_compact["publish_not_ready_reason_label"],
                    "waterfall_changed": baseline_compact["waterfall"] != candidate_compact["waterfall"],
                    "shadow_gate_changed": baseline_compact["shadow_gate"] != candidate_compact["shadow_gate"],
                    "trace_hash_changed": baseline_compact["trace_hash"] != candidate_compact["trace_hash"],
                },
            }
        )
    return rows


def _probe_row_comparison(baseline: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any] | None:
    rows = _row_diff_entries(baseline, candidate, limit=1)
    if not rows:
        return None
    row = rows[0]
    baseline_sample = row.get("baseline") if isinstance(row.get("baseline"), dict) else {}
    candidate_sample = row.get("candidate") if isinstance(row.get("candidate"), dict) else {}
    return {
        "code": row.get("code"),
        "date": row.get("date"),
        "baseline": {
            "feature_hash": baseline_sample.get("feature_hash"),
            "engine_input_hash": baseline_sample.get("engine_input_hash"),
            "engine_plan_hash": baseline_sample.get("engine_plan_hash"),
            "ranking_input_hash": baseline_sample.get("ranking_input_hash"),
            "engine_feature_flags": baseline_sample.get("engine_feature_flags"),
            "engine_scoring_params": baseline_sample.get("engine_scoring_params"),
            "engine_readiness_params": baseline_sample.get("engine_readiness_params"),
            "raw_readiness_score": baseline_sample.get("raw_readiness_score"),
            "readiness_pre_score": baseline_sample.get("readiness_pre_score"),
            "analysis_ev_net": baseline_sample.get("analysis_ev_net"),
            "short_ret_20": baseline_sample.get("short_ret_20"),
            "short_ret_10": baseline_sample.get("short_ret_10"),
            "short_ret_5": baseline_sample.get("short_ret_5"),
            "liquidity20d_source": baseline_sample.get("liquidity20d_source"),
            "publish_ready": baseline_sample.get("publish_ready"),
            "publish_not_ready_reasons": baseline_sample.get("publish_not_ready_reasons"),
            "publish_not_ready_reason_label": baseline_sample.get("publish_not_ready_reason_label"),
            "liquidity20d": baseline_sample.get("liquidity20d"),
            "liquidity20d_source": baseline_sample.get("liquidity20d_source"),
            "waterfall": baseline_sample.get("waterfall"),
            "shadow_gate": baseline_sample.get("shadow_gate"),
            "trace_hash": baseline_sample.get("trace_hash"),
        },
        "candidate": {
            "feature_hash": candidate_sample.get("feature_hash"),
            "engine_input_hash": candidate_sample.get("engine_input_hash"),
            "engine_plan_hash": candidate_sample.get("engine_plan_hash"),
            "ranking_input_hash": candidate_sample.get("ranking_input_hash"),
            "engine_feature_flags": candidate_sample.get("engine_feature_flags"),
            "engine_scoring_params": candidate_sample.get("engine_scoring_params"),
            "engine_readiness_params": candidate_sample.get("engine_readiness_params"),
            "raw_readiness_score": candidate_sample.get("raw_readiness_score"),
            "readiness_pre_score": candidate_sample.get("readiness_pre_score"),
            "analysis_ev_net": candidate_sample.get("analysis_ev_net"),
            "short_ret_20": candidate_sample.get("short_ret_20"),
            "short_ret_10": candidate_sample.get("short_ret_10"),
            "short_ret_5": candidate_sample.get("short_ret_5"),
            "liquidity20d_source": candidate_sample.get("liquidity20d_source"),
            "publish_ready": candidate_sample.get("publish_ready"),
            "publish_not_ready_reasons": candidate_sample.get("publish_not_ready_reasons"),
            "publish_not_ready_reason_label": candidate_sample.get("publish_not_ready_reason_label"),
            "liquidity20d": candidate_sample.get("liquidity20d"),
            "liquidity20d_source": candidate_sample.get("liquidity20d_source"),
            "waterfall": candidate_sample.get("waterfall"),
            "shadow_gate": candidate_sample.get("shadow_gate"),
            "trace_hash": candidate_sample.get("trace_hash"),
        },
        "delta": row.get("delta") if isinstance(row.get("delta"), dict) else {},
    }


def _probe_row_comparisons(
    baseline: dict[str, Any],
    candidate: dict[str, Any],
    family: dict[str, Any],
) -> list[dict[str, Any]]:
    comparisons: list[dict[str, Any]] = []
    probes = _family_probes(family)
    baseline_analysis = baseline.get("analysis") if isinstance(baseline.get("analysis"), dict) else {}
    candidate_analysis = candidate.get("analysis") if isinstance(candidate.get("analysis"), dict) else {}
    baseline_by_code = baseline_analysis.get("by_code") if isinstance(baseline_analysis.get("by_code"), dict) else {}
    candidate_by_code = candidate_analysis.get("by_code") if isinstance(candidate_analysis.get("by_code"), dict) else {}
    for probe in probes:
        code = _text(probe.get("code"))
        date_text = _text(probe.get("date"))
        if not code or not date_text:
            continue
        baseline_item = baseline_by_code.get(code)
        candidate_item = candidate_by_code.get(code)
        baseline_samples = baseline_item.get("samples") if isinstance(baseline_item, dict) and isinstance(baseline_item.get("samples"), list) else []
        candidate_samples = candidate_item.get("samples") if isinstance(candidate_item, dict) and isinstance(candidate_item.get("samples"), list) else []
        baseline_sample = next((sample for sample in baseline_samples if isinstance(sample, dict) and _text(sample.get("date")) == date_text), None)
        candidate_sample = next((sample for sample in candidate_samples if isinstance(sample, dict) and _text(sample.get("date")) == date_text), None)
        if not isinstance(baseline_sample, dict) or not isinstance(candidate_sample, dict):
            continue
        baseline_compact = _compact_sample(baseline_sample)
        candidate_compact = _compact_sample(candidate_sample)
        comparisons.append(
            {
                "probe_id": _text(probe.get("probe_id")),
                "code": code,
                "date": date_text,
                "label": _text(probe.get("label")),
                "baseline": baseline_compact,
                "candidate": candidate_compact,
                "delta": {
                    "feature_hash_changed": baseline_compact["feature_hash"] != candidate_compact["feature_hash"],
                    "engine_input_hash_changed": baseline_compact["engine_input_hash"] != candidate_compact["engine_input_hash"],
                    "engine_plan_hash_changed": baseline_compact["engine_plan_hash"] != candidate_compact["engine_plan_hash"],
                    "confidence": candidate_compact["confidence"] - baseline_compact["confidence"],
                    "readiness_pre_score": candidate_compact["readiness_pre_score"] - baseline_compact["readiness_pre_score"],
                    "raw_readiness_score": candidate_compact["raw_readiness_score"] - baseline_compact["raw_readiness_score"],
                    "signal_changed": baseline_compact["signal"] != candidate_compact["signal"],
                    "publish_ready_changed": baseline_compact["publish_ready"] != candidate_compact["publish_ready"],
                    "gate_reason_changed": baseline_compact["gate_reason"] != candidate_compact["gate_reason"],
                    "publish_not_ready_reason_changed": baseline_compact["publish_not_ready_reason_label"] != candidate_compact["publish_not_ready_reason_label"],
                    "trace_hash_changed": baseline_compact["trace_hash"] != candidate_compact["trace_hash"],
                },
                "missing_feature": {
                    "baseline": "missing_feature" in baseline_compact["publish_not_ready_reasons"],
                    "candidate": "missing_feature" in candidate_compact["publish_not_ready_reasons"],
                },
                "environment_unresolved": {
                    "baseline": "environment_unresolved" in baseline_compact["publish_not_ready_reasons"],
                    "candidate": "environment_unresolved" in candidate_compact["publish_not_ready_reasons"],
                },
            }
        )
    return comparisons


def _build_run_result(family: dict[str, Any], run: dict[str, Any]) -> dict[str, Any]:
    repo = get_stock_repo()
    plan = family["baseline_plan"] if _text(run.get("run_kind")) == "baseline" else next(
        (item for item in family.get("candidate_plans") or [] if isinstance(item, dict) and _text(item.get("plan_id")) == _text(run.get("plan_id"))),
        None,
    )
    if not isinstance(plan, dict):
        raise ValueError("plan not found")
    plan_effective = _plan_effective_parameters(plan)
    feature_flags = _feature_flags()
    readiness_config_hash = _readiness_config_hash()
    family_probes = _family_probes(family)
    segments = _period_segments(family)
    by_code: dict[str, dict[str, Any]] = {}
    all_samples: list[dict[str, Any]] = []
    liquidity_cache: dict[tuple[str, int], tuple[float | None, str]] = {}
    probe_lookup = {( _text(item.get("code")), _text(item.get("date")) ): item for item in family_probes}

    def _cached_liquidity20d(code: str, dt_key: int) -> tuple[float | None, str]:
        key = (code, dt_key)
        cached = liquidity_cache.get(key)
        if cached is not None:
            return cached
        liquidity20d = _analysis_liquidity20d(repo, code, dt_key)
        result = (liquidity20d, "daily_bars_20d" if liquidity20d is not None else "")
        liquidity_cache[key] = result
        return result

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
                liquidity20d, liquidity20d_source = _cached_liquidity20d(normalized_code, dt_key)
                point_with_liquidity = dict(point)
                point_with_liquidity["liquidity20d"] = liquidity20d
                if liquidity20d_source:
                    point_with_liquidity["liquidity20d_source"] = liquidity20d_source
                sample_input = _analysis_input(
                    normalized_code,
                    dt_key,
                    point_with_liquidity,
                    plan_effective=plan_effective,
                    feature_flags=feature_flags,
                    readiness_config_hash=readiness_config_hash,
                )
                sample_output = run_tradex_analysis(sample_input).to_dict()
                sample_trace = _sample_trace(
                    normalized_code,
                    dt_key,
                    sample_input,
                    sample_output,
                    plan,
                    plan_effective=plan_effective,
                    feature_flags=feature_flags,
                    readiness_config_hash=readiness_config_hash,
                )
                sample_trace["liquidity20d_source"] = liquidity20d_source
                probe = probe_lookup.get((normalized_code, _text(sample_trace.get("date"))))
                if probe:
                    sample_trace["probe_id"] = _text(probe.get("probe_id"))
                    sample_trace["probe_label"] = _text(probe.get("label"))
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
    readiness_summary = _readiness_summary(all_samples, plan)
    waterfall_summary = _waterfall_summary(all_samples)
    selection_summary = _selection_summary(all_samples, segments=segments, variant="champion")
    challenger_selection_summary = _selection_summary(all_samples, segments=segments, variant=TRADEX_CHALLENGER_SELECTION_VARIANT)
    selection_summary["regime_summary"] = list(by_period)
    challenger_selection_summary["regime_summary"] = list(by_period)
    engine_probe = all_samples[0] if all_samples else {}
    probe_entries = _probe_run_entries(by_code, family)
    probe_summary = probe_entries[0] if probe_entries else None
    run["status"] = "succeeded"
    run["completed_at"] = datetime.now(timezone.utc).isoformat()
    run["error"] = None
    run["metrics"] = metrics
    run["readiness_summary"] = readiness_summary
    run["waterfall_summary"] = waterfall_summary
    run["selection_summary"] = selection_summary
    run["selection_challenger_summary"] = challenger_selection_summary
    run["diagnostics_schema_version"] = TRADEX_DIAGNOSTICS_SCHEMA_VERSION
    run["engine_diagnostics"] = {
        "plan_effective": plan_effective,
        "feature_flags": feature_flags,
        "readiness_config_hash": readiness_config_hash,
        "probe": {
            "code": probe_summary.get("code") if isinstance(probe_summary, dict) else engine_probe.get("code"),
            "date": probe_summary.get("date") if isinstance(probe_summary, dict) else engine_probe.get("date"),
            "feature_hash": probe_summary.get("feature_hash") if isinstance(probe_summary, dict) else engine_probe.get("feature_hash"),
            "engine_input_hash": probe_summary.get("engine_input_hash") if isinstance(probe_summary, dict) else engine_probe.get("engine_input_hash"),
            "engine_plan_hash": probe_summary.get("engine_plan_hash") if isinstance(probe_summary, dict) else engine_probe.get("engine_plan_hash"),
            "engine_feature_flags": probe_summary.get("engine_feature_flags") if isinstance(probe_summary, dict) else engine_probe.get("engine_feature_flags"),
            "engine_scoring_params": probe_summary.get("engine_scoring_params") if isinstance(probe_summary, dict) else engine_probe.get("engine_scoring_params"),
            "engine_readiness_params": probe_summary.get("engine_readiness_params") if isinstance(probe_summary, dict) else engine_probe.get("engine_readiness_params"),
            "raw_readiness_score": probe_summary.get("raw_readiness_score") if isinstance(probe_summary, dict) else engine_probe.get("raw_readiness_score"),
            "readiness_pre_score": probe_summary.get("readiness_pre_score") if isinstance(probe_summary, dict) else engine_probe.get("readiness_pre_score"),
            "publish_ready": probe_summary.get("publish_ready") if isinstance(probe_summary, dict) else engine_probe.get("publish_ready"),
            "publish_not_ready_reasons": probe_summary.get("publish_not_ready_reasons") if isinstance(probe_summary, dict) else engine_probe.get("publish_not_ready_reasons"),
            "publish_not_ready_reason_label": probe_summary.get("publish_not_ready_reason_label") if isinstance(probe_summary, dict) else engine_probe.get("publish_not_ready_reason_label"),
        },
        "probes": probe_entries,
    }
    run["summary"] = {
        "run_signature": _metrics_signature(metrics),
        "diagnostics_schema_version": TRADEX_DIAGNOSTICS_SCHEMA_VERSION,
        "sample_count": overall["sample_count"],
        "signal_count": overall["signal_count"],
        "signal_dates": overall["signal_dates"],
        "top_reasons": overall["top_reasons"],
        "target_symbol_count": len(by_code),
        "top_symbol_share": overall["top_symbol_share"],
        "overall_score": overall_score,
        "by_period_stability": period_stability,
        "selection_summary": selection_summary,
        "selection_challenger_summary": challenger_selection_summary,
        "waterfall_summary": waterfall_summary,
    }
    run["analysis"] = {
        "diagnostics_schema_version": TRADEX_DIAGNOSTICS_SCHEMA_VERSION,
        "segments": segments,
        "by_code": by_code,
        "signature": run["summary"]["run_signature"],
        "readiness_summary": readiness_summary,
        "waterfall_summary": waterfall_summary,
        "selection_summary": selection_summary,
        "selection_challenger_summary": challenger_selection_summary,
        "effective_config": run.get("effective_config") or {},
        "engine_diagnostics": run["engine_diagnostics"],
        "probes": probe_entries,
    }
    return run


def _run_base(family: dict[str, Any], run_id: str, run_kind: str, plan: dict[str, Any], notes: str | None) -> dict[str, Any]:
    effective_parameters = _plan_effective_parameters(plan)
    method_metadata = _plan_method_metadata(plan)
    return {
        "schema_version": TRADEX_RUN_SCHEMA_VERSION,
        "diagnostics_schema_version": TRADEX_DIAGNOSTICS_SCHEMA_VERSION,
        "family_id": family["family_id"],
        "run_id": run_id,
        "run_kind": run_kind,
        "plan_id": plan["plan_id"],
        "plan_version": plan["plan_version"],
        "method_id": method_metadata["method_id"],
        "method_title": method_metadata["method_title"],
        "method_thesis": method_metadata["method_thesis"],
        "method_family": method_metadata["method_family"],
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
        "effective_config": {
            "plan_id": effective_parameters["plan_id"],
            "plan_version": effective_parameters["plan_version"],
            "plan_hash": _stable_hash(effective_parameters),
            "effective_parameters": effective_parameters,
            "method_metadata": method_metadata,
            "readiness_config_hash": _readiness_config_hash(),
        },
        "metrics": {},
        "summary": {},
        "readiness_summary": {},
        "engine_diagnostics": {},
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
    probes = _normalize_probes(body.get("probes"))
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
        "probes": probes,
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


def _compare_payload(
    family: dict[str, Any],
    baseline: dict[str, Any],
    candidate: dict[str, Any],
    *,
    emit_report: bool = False,
) -> dict[str, Any]:
    baseline_metrics = baseline.get("metrics") if isinstance(baseline.get("metrics"), dict) else {}
    candidate_metrics = candidate.get("metrics") if isinstance(candidate.get("metrics"), dict) else {}
    baseline_effective_config = baseline.get("effective_config") if isinstance(baseline.get("effective_config"), dict) else {}
    candidate_effective_config = candidate.get("effective_config") if isinstance(candidate.get("effective_config"), dict) else {}
    baseline_method = _plan_method_metadata(baseline)
    candidate_method = _plan_method_metadata(candidate)
    baseline_readiness = baseline.get("readiness_summary") if isinstance(baseline.get("readiness_summary"), dict) else {}
    candidate_readiness = candidate.get("readiness_summary") if isinstance(candidate.get("readiness_summary"), dict) else {}
    baseline_waterfall = baseline.get("waterfall_summary") if isinstance(baseline.get("waterfall_summary"), dict) else {}
    candidate_waterfall = candidate.get("waterfall_summary") if isinstance(candidate.get("waterfall_summary"), dict) else {}
    baseline_selection = baseline.get("selection_summary") if isinstance(baseline.get("selection_summary"), dict) else {}
    baseline_challenger_selection = baseline.get("selection_challenger_summary") if isinstance(baseline.get("selection_challenger_summary"), dict) else {}
    candidate_selection = candidate.get("selection_summary") if isinstance(candidate.get("selection_summary"), dict) else {}
    candidate_challenger_selection = candidate.get("selection_challenger_summary") if isinstance(candidate.get("selection_challenger_summary"), dict) else {}
    baseline_engine_diagnostics = baseline.get("engine_diagnostics") if isinstance(baseline.get("engine_diagnostics"), dict) else {}
    candidate_engine_diagnostics = candidate.get("engine_diagnostics") if isinstance(candidate.get("engine_diagnostics"), dict) else {}
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
    selection_compare = _selection_comparison_summary(baseline_selection, candidate_challenger_selection)
    evaluation_summary = _build_champion_challenger_evaluation(
        family=family,
        baseline=baseline,
        candidate=candidate,
        emit_report=emit_report,
    )
    probe_row_comparisons = _probe_row_comparisons(baseline, candidate, family)
    return {
        "diagnostics_schema_version": TRADEX_DIAGNOSTICS_SCHEMA_VERSION,
        "run_id": candidate.get("run_id"),
        "plan_id": candidate.get("plan_id"),
        "plan_version": candidate.get("plan_version"),
        "baseline_method": baseline_method,
        "candidate_method": candidate_method,
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
        "selection_compare": selection_compare,
        "evaluation_window_id": evaluation_summary.get("evaluation_window_id"),
        "regime_tag": evaluation_summary.get("regime_tag"),
        "promote_ready": evaluation_summary.get("promote_ready"),
        "promote_reasons": evaluation_summary.get("promote_reasons"),
        "evaluation_summary": evaluation_summary,
        "diagnostics": {
            "baseline_effective_config": baseline_effective_config,
            "candidate_effective_config": candidate_effective_config,
            "baseline_engine_diagnostics": baseline_engine_diagnostics,
            "candidate_engine_diagnostics": candidate_engine_diagnostics,
            "baseline_readiness_summary": baseline_readiness,
            "candidate_readiness_summary": candidate_readiness,
            "baseline_waterfall_summary": baseline_waterfall,
            "candidate_waterfall_summary": candidate_waterfall,
            "baseline_selection_summary": baseline_selection,
            "baseline_challenger_selection_summary": baseline_challenger_selection,
            "candidate_selection_summary": candidate_selection,
            "candidate_challenger_selection_summary": candidate_challenger_selection,
            "selection_compare": selection_compare,
            "evaluation_summary": evaluation_summary,
            "diagnostics_schema_version": TRADEX_DIAGNOSTICS_SCHEMA_VERSION,
            "row_diffs": _row_diff_entries(baseline, candidate, limit=5),
            "probe_row_comparisons": probe_row_comparisons,
            "probe_row_comparison": (probe_row_comparisons or [None])[0],
        },
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
    candidates = [
        run
        for run in candidates
        if isinstance(run, dict) and _text(run.get("status")) in {"succeeded", "compared", "adopt_candidate", "rejected"}
    ]
    if not candidates:
        return None
    for candidate in candidates:
        if _text(candidate.get("status")) == "succeeded":
            candidate["status"] = "compared"
        write_json(run_file(family["family_id"], str(candidate["run_id"])), candidate)
    payload = {
        "schema_version": TRADEX_COMPARE_SCHEMA_VERSION,
        "diagnostics_schema_version": TRADEX_DIAGNOSTICS_SCHEMA_VERSION,
        "family_id": family["family_id"],
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "baseline_run_id": baseline_id,
        "candidate_results": [_compare_payload(family, baseline, candidate, emit_report=True) for candidate in candidates],
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
    return _compare_payload(family, baseline, candidate, emit_report=False)


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
        _generate_compare(family)
        _update_family_file(family)
        return {"ok": True, "family_id": family_id, "run_id": run_id, "status": status, "gate": gate, "adopt": run["adopt"], "compare": compare}
