from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from app.backend.infra.duckdb.stock_repo import StockRepository
from app.backend.services import tradex_research_os_store as os_store

from .simulator import (
    REPLAY_SCHEMA_VERSION,
    _med,
    _parse_date,
    _simulate_window,
    _stable_hash,
    _text,
    build_replay_change_log,
    normalize_replay_run_config,
    prepare_replay_window_context,
)

POLICY_FAMILY_SCHEMA_VERSION = "tradex_policy_family_replay_v1"
POLICY_VARIANT_MANIFEST_FILE = "policy_variant_manifest.json"
POLICY_COMPARISON_MATRIX_FILE = "policy_comparison_matrix.json"
POLICY_DECISION_LOG_FILE = "policy_decision_log.json"
POLICY_KEEP_DROP_HOLD_FILE = "policy_keep_drop_hold.json"
POLICY_FAMILY_RESULT_FILE = "policy_family_result.json"
POLICY_VARIANT_RESULT_FILE = "policy_variant_replay.json"
POLICY_FAMILY_COHORT_SCHEMA_VERSION = "tradex_policy_family_cohort_v1"
POLICY_FAMILY_COHORT_MANIFEST_FILE = "policy_family_cohort_manifest.json"
POLICY_FAMILY_COHORT_RESULT_FILE = "policy_family_cohort_result.json"
POLICY_THRESHOLD_CALIBRATION_FILE = "policy_threshold_calibration.json"

POLICY_DECISION_REASON_TEXT: dict[str, str] = {
    "weak_relative_performance": "relative performance is not strong enough against the tradable universe",
    "unstable_across_windows": "window-level excess is too unstable across repeated 3-month periods",
    "excessive_turnover": "turnover is too high for the hold-longer preference",
    "insufficient_long_hold_behavior": "holding periods are too short for the trend-capture preference",
    "weekly_activity_failures": "the portfolio missed required weekly activity too often",
    "promising_but_sample_small": "the candidate looks promising but the sample is too small to keep confidently",
}


def _slug(text: str) -> str:
    raw = re.sub(r"[^0-9A-Za-z._-]+", "-", _text(text))
    raw = raw.strip("-._")
    return raw or "policy-family"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    return value


def _num(value: Any, fallback: float = 0.0) -> float:
    try:
        if value is None:
            return float(fallback)
        text = _text(value)
        if not text:
            return float(fallback)
        return float(text)
    except (TypeError, ValueError):
        return float(fallback)


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    return os_store.write_json(path, payload)


def _family_root() -> Path:
    root = os_store.research_os_root() / "policy_replay" / "families"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _family_dir(family_id: str) -> Path:
    path = _family_root() / _slug(family_id)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _variant_dir(family_dir: Path, variant_id: str) -> Path:
    path = family_dir / "policy_variants" / _slug(variant_id)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _quantile(values: list[float], fraction: float) -> float | None:
    clean = sorted(float(item) for item in values if item is not None)
    if not clean:
        return None
    if len(clean) == 1:
        return clean[0]
    fraction = max(0.0, min(1.0, float(fraction)))
    position = (len(clean) - 1) * fraction
    lower = int(position)
    upper = min(len(clean) - 1, lower + 1)
    if lower == upper:
        return clean[lower]
    return clean[lower] * (upper - position) + clean[upper] * (position - lower)


def _metrics_matrix(rows: list[dict[str, Any]], field_names: list[str]) -> dict[str, list[float]]:
    matrix: dict[str, list[float]] = {field: [] for field in field_names}
    for row in rows:
        for field in field_names:
            value = row.get(field)
            if value is None:
                continue
            matrix[field].append(float(value))
    return matrix


def _family_variant(
    *,
    variant_id: str,
    policy_id: str,
    policy_version: str,
    selection_rule: dict[str, Any] | None = None,
    entry_rule: dict[str, Any] | None = None,
    add_rule: dict[str, Any] | None = None,
    partial_take_rule: dict[str, Any] | None = None,
    full_exit_rule: dict[str, Any] | None = None,
    sizing_rule: dict[str, Any] | None = None,
    scoring_weights: dict[str, Any] | None = None,
    reason_code: str = "policy_variant_update",
    what_changed: str = "",
    why_it_changed: str = "",
    expected_effect: str = "",
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "policy_variant_id": variant_id,
        "policy_id": policy_id,
        "policy_version": policy_version,
        "selection_rule": dict(selection_rule or {}),
        "entry_rule": dict(entry_rule or {}),
        "add_rule": dict(add_rule or {}),
        "partial_take_rule": dict(partial_take_rule or {}),
        "full_exit_rule": dict(full_exit_rule or {}),
        "sizing_rule": dict(sizing_rule or {}),
        "scoring": {"weights": dict(scoring_weights or {})},
        "rationale": {
            "what_changed": what_changed,
            "why_it_changed": why_it_changed,
            "expected_effect": expected_effect,
            "reason_code": reason_code,
            "author_or_source": "system",
            "timestamp_or_run_id": "cohort",
        },
    }
    return payload


def _build_first_policy_family_cohort(payload: dict[str, Any]) -> list[dict[str, Any]]:
    shared = dict(payload.get("shared") or {})
    for key in (
        "policy_id",
        "policy_version",
        "window_start_date",
        "window_start_dates",
        "window_months",
        "universe",
        "market_benchmark_symbol",
        "capital",
        "scoring",
        "policy",
        "unit_scale",
        "addon_units",
        "execution_convention",
        "weekly_activity_required",
        "short_cash_reusable",
        "selection_rule_change_log",
    ):
        if key not in shared and key in payload:
            shared[key] = payload.get(key)
    shared.setdefault("policy_id", "policy_family_cohort")
    shared.setdefault("policy_version", "v1")
    shared.setdefault("window_months", 3)
    shared.setdefault("execution_convention", "close_close_research_convention")
    shared.setdefault("weekly_activity_required", True)
    shared.setdefault("short_cash_reusable", False)
    shared.setdefault("addon_units", [2, 3, 5])
    shared.setdefault("scoring", {})
    shared.setdefault("capital", {})
    shared.setdefault("policy", {})
    shared.setdefault("window_start_dates", [shared.get("window_start_date") or payload.get("window_start_date")])
    shared["window_start_dates"] = [str(item) for item in shared.get("window_start_dates") or [] if _text(item)]
    base_weights = dict((shared.get("scoring") or {}).get("weights") or payload.get("weights") or {
        "total_return": 0.08,
        "excess_vs_universe": 0.22,
        "exposure_adjusted_excess": 0.16,
        "median_window_excess": 0.16,
        "worst_window_excess": 0.10,
        "max_drawdown": -0.10,
        "turnover": -0.08,
        "concentration": -0.05,
        "weekly_activity": -0.08,
        "long_hold": 0.10,
        "premature_exit": -0.07,
    })
    base_policy = dict(shared.get("policy") or {})
    base_entry_threshold = _num(base_policy.get("entry_threshold") or payload.get("entry_threshold") or 0.04)
    base_add_threshold = _num(base_policy.get("add_threshold") or payload.get("add_threshold") or 0.08)
    base_partial_take_threshold = _num(base_policy.get("partial_take_threshold") or payload.get("partial_take_threshold") or 0.05)
    base_exit_threshold = _num(base_policy.get("exit_threshold") or payload.get("exit_threshold") or -0.03)
    base_stop_loss_threshold = _num(base_policy.get("stop_loss_threshold") or payload.get("stop_loss_threshold") or -0.06)
    base_unit_scale = int(shared.get("unit_scale") or payload.get("unit_scale") or 100)
    base_gross_cap = _num((shared.get("capital") or {}).get("gross_exposure_cap_jpy") or payload.get("gross_exposure_cap_jpy") or 10_000_000.0)
    base_initial_capital = _num((shared.get("capital") or {}).get("initial_capital_jpy") or payload.get("initial_capital_jpy") or 10_000_000.0)

    selection_family = {
        "family_id": _text(payload.get("selection_family_id"), fallback="selection-focused-family"),
        "family_name": "Selection-focused family",
        "family_edge": "selection_rule",
        "family_thesis": "Move the selection edge by changing the scoring/selection lens while keeping entry/add/exit mechanics fixed.",
        "policy_variants": [
            _family_variant(
                variant_id="selection_base",
                policy_id="selection_family",
                policy_version="base",
                selection_rule={"policy_id": "selection_family", "policy_version": "base", "weights": base_weights, "execution_convention": shared["execution_convention"], "weekly_activity_required": bool(shared["weekly_activity_required"])},
                entry_rule={"entry_threshold": base_entry_threshold},
                add_rule={"add_threshold": base_add_threshold, "addon_units": list(shared.get("addon_units") or [2, 3, 5])},
                partial_take_rule={"partial_take_threshold": base_partial_take_threshold},
                full_exit_rule={"exit_threshold": base_exit_threshold, "stop_loss_threshold": base_stop_loss_threshold},
                sizing_rule={"initial_capital_jpy": base_initial_capital, "gross_exposure_cap_jpy": base_gross_cap, "unit_scale": base_unit_scale, "short_cash_reusable": bool(shared["short_cash_reusable"])},
                scoring_weights=base_weights,
                reason_code="selection_lens",
                what_changed="baseline selection lens",
                why_it_changed="establish a stable baseline for selection-only comparison",
                expected_effect="reference point for selection-driven ranking",
            ),
            _family_variant(
                variant_id="selection_quality_tilt",
                policy_id="selection_family",
                policy_version="quality_tilt",
                selection_rule={"policy_id": "selection_family", "policy_version": "quality_tilt", "weights": {**base_weights, "excess_vs_universe": 0.28, "exposure_adjusted_excess": 0.20, "weekly_activity": -0.06}},
                entry_rule={"entry_threshold": base_entry_threshold},
                add_rule={"add_threshold": base_add_threshold, "addon_units": list(shared.get("addon_units") or [2, 3, 5])},
                partial_take_rule={"partial_take_threshold": base_partial_take_threshold},
                full_exit_rule={"exit_threshold": base_exit_threshold, "stop_loss_threshold": base_stop_loss_threshold},
                sizing_rule={"initial_capital_jpy": base_initial_capital, "gross_exposure_cap_jpy": base_gross_cap, "unit_scale": base_unit_scale, "short_cash_reusable": bool(shared["short_cash_reusable"])},
                scoring_weights={**base_weights, "excess_vs_universe": 0.28, "exposure_adjusted_excess": 0.20, "weekly_activity": -0.06},
                reason_code="selection_quality",
                what_changed="heavier relative-performance weights",
                why_it_changed="test whether selection can improve when relative performance is emphasized",
                expected_effect="better excess capture with similar turnover",
            ),
            _family_variant(
                variant_id="selection_hold_bias",
                policy_id="selection_family",
                policy_version="hold_bias",
                selection_rule={"policy_id": "selection_family", "policy_version": "hold_bias", "weights": {**base_weights, "long_hold": 0.18, "turnover": -0.12, "premature_exit": -0.10}},
                entry_rule={"entry_threshold": base_entry_threshold},
                add_rule={"add_threshold": base_add_threshold, "addon_units": list(shared.get("addon_units") or [2, 3, 5])},
                partial_take_rule={"partial_take_threshold": base_partial_take_threshold},
                full_exit_rule={"exit_threshold": base_exit_threshold, "stop_loss_threshold": base_stop_loss_threshold},
                sizing_rule={"initial_capital_jpy": base_initial_capital, "gross_exposure_cap_jpy": base_gross_cap, "unit_scale": base_unit_scale, "short_cash_reusable": bool(shared["short_cash_reusable"])},
                scoring_weights={**base_weights, "long_hold": 0.18, "turnover": -0.12, "premature_exit": -0.10},
                reason_code="selection_hold_bias",
                what_changed="hold bonus and churn penalty tilt",
                why_it_changed="see if selection can favor longer trend capture without changing execution rules",
                expected_effect="higher average hold with less churn",
            ),
        ],
        "shared": dict(shared),
    }

    entry_family = {
        "family_id": _text(payload.get("entry_family_id"), fallback="entry-timing-family"),
        "family_name": "Entry-timing family",
        "family_edge": "entry_rule",
        "family_thesis": "Move the entry timing edge by changing only the entry threshold while keeping selection, add, and exit discipline fixed.",
        "policy_variants": [
            _family_variant(
                variant_id="entry_early",
                policy_id="entry_family",
                policy_version="early",
                selection_rule={"policy_id": "entry_family", "policy_version": "early", "weights": base_weights, "execution_convention": shared["execution_convention"], "weekly_activity_required": bool(shared["weekly_activity_required"])},
                entry_rule={"entry_threshold": max(0.01, base_entry_threshold - 0.03)},
                add_rule={"add_threshold": base_add_threshold, "addon_units": list(shared.get("addon_units") or [2, 3, 5])},
                partial_take_rule={"partial_take_threshold": base_partial_take_threshold},
                full_exit_rule={"exit_threshold": base_exit_threshold, "stop_loss_threshold": base_stop_loss_threshold},
                sizing_rule={"initial_capital_jpy": base_initial_capital, "gross_exposure_cap_jpy": base_gross_cap, "unit_scale": base_unit_scale, "short_cash_reusable": bool(shared["short_cash_reusable"])},
                scoring_weights=base_weights,
                reason_code="entry_timing",
                what_changed="lower entry threshold",
                why_it_changed="test earlier entry capture",
                expected_effect="more early trend capture with higher turnover",
            ),
            _family_variant(
                variant_id="entry_base",
                policy_id="entry_family",
                policy_version="base",
                selection_rule={"policy_id": "entry_family", "policy_version": "base", "weights": base_weights, "execution_convention": shared["execution_convention"], "weekly_activity_required": bool(shared["weekly_activity_required"])},
                entry_rule={"entry_threshold": base_entry_threshold},
                add_rule={"add_threshold": base_add_threshold, "addon_units": list(shared.get("addon_units") or [2, 3, 5])},
                partial_take_rule={"partial_take_threshold": base_partial_take_threshold},
                full_exit_rule={"exit_threshold": base_exit_threshold, "stop_loss_threshold": base_stop_loss_threshold},
                sizing_rule={"initial_capital_jpy": base_initial_capital, "gross_exposure_cap_jpy": base_gross_cap, "unit_scale": base_unit_scale, "short_cash_reusable": bool(shared["short_cash_reusable"])},
                scoring_weights=base_weights,
                reason_code="entry_baseline",
                what_changed="baseline entry threshold",
                why_it_changed="reference point for entry timing",
                expected_effect="balanced entry cadence",
            ),
            _family_variant(
                variant_id="entry_late",
                policy_id="entry_family",
                policy_version="late",
                selection_rule={"policy_id": "entry_family", "policy_version": "late", "weights": base_weights, "execution_convention": shared["execution_convention"], "weekly_activity_required": bool(shared["weekly_activity_required"])},
                entry_rule={"entry_threshold": min(0.99, base_entry_threshold + 0.06)},
                add_rule={"add_threshold": base_add_threshold, "addon_units": list(shared.get("addon_units") or [2, 3, 5])},
                partial_take_rule={"partial_take_threshold": base_partial_take_threshold},
                full_exit_rule={"exit_threshold": base_exit_threshold, "stop_loss_threshold": base_stop_loss_threshold},
                sizing_rule={"initial_capital_jpy": base_initial_capital, "gross_exposure_cap_jpy": base_gross_cap, "unit_scale": base_unit_scale, "short_cash_reusable": bool(shared["short_cash_reusable"])},
                scoring_weights=base_weights,
                reason_code="entry_timing",
                what_changed="higher entry threshold",
                why_it_changed="test delayed entry discipline",
                expected_effect="lower turnover and later trend confirmation",
            ),
        ],
        "shared": dict(shared),
    }

    add_family = {
        "family_id": _text(payload.get("add_family_id"), fallback="add-hold-extension-family"),
        "family_name": "Add/hold extension family",
        "family_edge": "add_rule",
        "family_thesis": "Move the build-out/hold extension edge by changing only add-on cadence while keeping entry and exit discipline fixed.",
        "policy_variants": [
            _family_variant(
                variant_id="add_fast",
                policy_id="add_family",
                policy_version="fast",
                selection_rule={"policy_id": "add_family", "policy_version": "fast", "weights": base_weights, "execution_convention": shared["execution_convention"], "weekly_activity_required": bool(shared["weekly_activity_required"])},
                entry_rule={"entry_threshold": base_entry_threshold},
                add_rule={"add_threshold": max(0.01, base_add_threshold - 0.04), "addon_units": list(shared.get("addon_units") or [2, 3, 5])},
                partial_take_rule={"partial_take_threshold": base_partial_take_threshold},
                full_exit_rule={"exit_threshold": base_exit_threshold, "stop_loss_threshold": base_stop_loss_threshold},
                sizing_rule={"initial_capital_jpy": base_initial_capital, "gross_exposure_cap_jpy": base_gross_cap, "unit_scale": base_unit_scale, "short_cash_reusable": bool(shared["short_cash_reusable"])},
                scoring_weights=base_weights,
                reason_code="add_path",
                what_changed="earlier add-on trigger",
                why_it_changed="test whether faster build-out improves trend capture",
                expected_effect="more aggressive extension with higher exposure",
            ),
            _family_variant(
                variant_id="add_base",
                policy_id="add_family",
                policy_version="base",
                selection_rule={"policy_id": "add_family", "policy_version": "base", "weights": base_weights, "execution_convention": shared["execution_convention"], "weekly_activity_required": bool(shared["weekly_activity_required"])},
                entry_rule={"entry_threshold": base_entry_threshold},
                add_rule={"add_threshold": base_add_threshold, "addon_units": list(shared.get("addon_units") or [2, 3, 5])},
                partial_take_rule={"partial_take_threshold": base_partial_take_threshold},
                full_exit_rule={"exit_threshold": base_exit_threshold, "stop_loss_threshold": base_stop_loss_threshold},
                sizing_rule={"initial_capital_jpy": base_initial_capital, "gross_exposure_cap_jpy": base_gross_cap, "unit_scale": base_unit_scale, "short_cash_reusable": bool(shared["short_cash_reusable"])},
                scoring_weights=base_weights,
                reason_code="add_baseline",
                what_changed="baseline add-on cadence",
                why_it_changed="reference point for build-out behavior",
                expected_effect="balanced add-on extension",
            ),
            _family_variant(
                variant_id="add_patient",
                policy_id="add_family",
                policy_version="patient",
                selection_rule={"policy_id": "add_family", "policy_version": "patient", "weights": base_weights, "execution_convention": shared["execution_convention"], "weekly_activity_required": bool(shared["weekly_activity_required"])},
                entry_rule={"entry_threshold": base_entry_threshold},
                add_rule={"add_threshold": min(0.99, base_add_threshold + 0.04), "addon_units": list(shared.get("addon_units") or [2, 3, 5])},
                partial_take_rule={"partial_take_threshold": base_partial_take_threshold},
                full_exit_rule={"exit_threshold": base_exit_threshold, "stop_loss_threshold": base_stop_loss_threshold},
                sizing_rule={"initial_capital_jpy": base_initial_capital, "gross_exposure_cap_jpy": base_gross_cap, "unit_scale": base_unit_scale, "short_cash_reusable": bool(shared["short_cash_reusable"])},
                scoring_weights=base_weights,
                reason_code="add_path",
                what_changed="later add-on trigger",
                why_it_changed="test whether patient build-out improves hold quality",
                expected_effect="slower build-out with longer runs",
            ),
        ],
        "shared": dict(shared),
    }

    exit_family = {
        "family_id": _text(payload.get("exit_family_id"), fallback="exit-discipline-family"),
        "family_name": "Partial-take / exit discipline family",
        "family_edge": "partial_take_rule",
        "family_thesis": "Move the exit-discipline edge by changing only profit-take and full-exit discipline while keeping entry and add mechanics fixed.",
        "policy_variants": [
            _family_variant(
                variant_id="exit_early",
                policy_id="exit_family",
                policy_version="early",
                selection_rule={"policy_id": "exit_family", "policy_version": "early", "weights": base_weights, "execution_convention": shared["execution_convention"], "weekly_activity_required": bool(shared["weekly_activity_required"])},
                entry_rule={"entry_threshold": base_entry_threshold},
                add_rule={"add_threshold": base_add_threshold, "addon_units": list(shared.get("addon_units") or [2, 3, 5])},
                partial_take_rule={"partial_take_threshold": max(0.01, base_partial_take_threshold - 0.03)},
                full_exit_rule={"exit_threshold": min(-0.01, base_exit_threshold + 0.02), "stop_loss_threshold": base_stop_loss_threshold},
                sizing_rule={"initial_capital_jpy": base_initial_capital, "gross_exposure_cap_jpy": base_gross_cap, "unit_scale": base_unit_scale, "short_cash_reusable": bool(shared["short_cash_reusable"])},
                scoring_weights=base_weights,
                reason_code="exit_discipline",
                what_changed="earlier profit taking",
                why_it_changed="test tighter exit discipline",
                expected_effect="more realized gains but higher churn",
            ),
            _family_variant(
                variant_id="exit_base",
                policy_id="exit_family",
                policy_version="base",
                selection_rule={"policy_id": "exit_family", "policy_version": "base", "weights": base_weights, "execution_convention": shared["execution_convention"], "weekly_activity_required": bool(shared["weekly_activity_required"])},
                entry_rule={"entry_threshold": base_entry_threshold},
                add_rule={"add_threshold": base_add_threshold, "addon_units": list(shared.get("addon_units") or [2, 3, 5])},
                partial_take_rule={"partial_take_threshold": base_partial_take_threshold},
                full_exit_rule={"exit_threshold": base_exit_threshold, "stop_loss_threshold": base_stop_loss_threshold},
                sizing_rule={"initial_capital_jpy": base_initial_capital, "gross_exposure_cap_jpy": base_gross_cap, "unit_scale": base_unit_scale, "short_cash_reusable": bool(shared["short_cash_reusable"])},
                scoring_weights=base_weights,
                reason_code="exit_baseline",
                what_changed="baseline exit discipline",
                why_it_changed="reference point for exit discipline",
                expected_effect="balanced profit taking and loss cutting",
            ),
            _family_variant(
                variant_id="exit_patient",
                policy_id="exit_family",
                policy_version="patient",
                selection_rule={"policy_id": "exit_family", "policy_version": "patient", "weights": base_weights, "execution_convention": shared["execution_convention"], "weekly_activity_required": bool(shared["weekly_activity_required"])},
                entry_rule={"entry_threshold": base_entry_threshold},
                add_rule={"add_threshold": base_add_threshold, "addon_units": list(shared.get("addon_units") or [2, 3, 5])},
                partial_take_rule={"partial_take_threshold": min(0.50, base_partial_take_threshold + 0.04)},
                full_exit_rule={"exit_threshold": max(-0.50, base_exit_threshold - 0.05), "stop_loss_threshold": base_stop_loss_threshold},
                sizing_rule={"initial_capital_jpy": base_initial_capital, "gross_exposure_cap_jpy": base_gross_cap, "unit_scale": base_unit_scale, "short_cash_reusable": bool(shared["short_cash_reusable"])},
                scoring_weights=base_weights,
                reason_code="exit_discipline",
                what_changed="later profit taking",
                why_it_changed="test patient exit discipline",
                expected_effect="longer holds and fewer premature exits",
            ),
        ],
        "shared": dict(shared),
    }

    return [selection_family, entry_family, add_family, exit_family]


def _calibrate_thresholds_from_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    matrix = _metrics_matrix(
        rows,
        [
            "median_window_excess",
            "exposure_adjusted_excess_mean",
            "worst_window_excess",
            "weekly_activity_pass_rate_mean",
            "avg_holding_days_mean",
            "turnover_mean",
            "pct_trades_over_20d_mean",
            "final_score_mean",
        ],
    )
    quantiles = {
        field: {
            "q25": _quantile(values, 0.25),
            "q50": _quantile(values, 0.50),
            "q75": _quantile(values, 0.75),
        }
        for field, values in matrix.items()
    }
    keep = {
        "median_window_excess_min": quantiles["median_window_excess"]["q75"] if quantiles["median_window_excess"]["q75"] is not None else 0.0,
        "exposure_adjusted_excess_mean_min": quantiles["exposure_adjusted_excess_mean"]["q75"] if quantiles["exposure_adjusted_excess_mean"]["q75"] is not None else 0.0,
        "worst_window_excess_min": quantiles["worst_window_excess"]["q50"] if quantiles["worst_window_excess"]["q50"] is not None else 0.0,
        "weekly_activity_pass_rate_mean_min": quantiles["weekly_activity_pass_rate_mean"]["q50"] if quantiles["weekly_activity_pass_rate_mean"]["q50"] is not None else 0.0,
        "avg_holding_days_mean_min": quantiles["avg_holding_days_mean"]["q50"] if quantiles["avg_holding_days_mean"]["q50"] is not None else 20.0,
        "pct_trades_over_20d_mean_min": quantiles["pct_trades_over_20d_mean"]["q50"] if quantiles["pct_trades_over_20d_mean"]["q50"] is not None else 0.35,
        "turnover_mean_max": quantiles["turnover_mean"]["q50"] if quantiles["turnover_mean"]["q50"] is not None else 1.0,
        "final_score_mean_min": quantiles["final_score_mean"]["q75"] if quantiles["final_score_mean"]["q75"] is not None else 0.0,
    }
    drop = {
        "median_window_excess_max": quantiles["median_window_excess"]["q25"] if quantiles["median_window_excess"]["q25"] is not None else 0.0,
        "exposure_adjusted_excess_mean_max": quantiles["exposure_adjusted_excess_mean"]["q25"] if quantiles["exposure_adjusted_excess_mean"]["q25"] is not None else 0.0,
        "worst_window_excess_max": quantiles["worst_window_excess"]["q25"] if quantiles["worst_window_excess"]["q25"] is not None else -0.02,
        "weekly_activity_pass_rate_mean_max": quantiles["weekly_activity_pass_rate_mean"]["q25"] if quantiles["weekly_activity_pass_rate_mean"]["q25"] is not None else 1.0,
        "avg_holding_days_mean_max": quantiles["avg_holding_days_mean"]["q25"] if quantiles["avg_holding_days_mean"]["q25"] is not None else 20.0,
        "pct_trades_over_20d_mean_max": quantiles["pct_trades_over_20d_mean"]["q25"] if quantiles["pct_trades_over_20d_mean"]["q25"] is not None else 0.35,
        "turnover_mean_min": quantiles["turnover_mean"]["q75"] if quantiles["turnover_mean"]["q75"] is not None else 1.25,
        "final_score_mean_max": quantiles["final_score_mean"]["q25"] if quantiles["final_score_mean"]["q25"] is not None else 0.0,
    }
    return {
        "schema_version": POLICY_FAMILY_COHORT_SCHEMA_VERSION,
        "quantiles": quantiles,
        "thresholds": {
            "keep": keep,
            "drop": drop,
        },
    }
def _merge_shared_and_variant(shared: dict[str, Any], variant: dict[str, Any]) -> dict[str, Any]:
    payload = dict(shared)
    for key, value in variant.items():
        if key in {"selection_rule", "entry_rule", "add_rule", "partial_take_rule", "full_exit_rule", "sizing_rule"} and isinstance(value, dict):
            payload[key] = dict(value)
            continue
        if key == "policy" and isinstance(value, dict):
            payload[key] = dict(value)
            continue
        if key == "capital" and isinstance(value, dict):
            payload[key] = dict(value)
            continue
        if key == "scoring" and isinstance(value, dict):
            payload[key] = dict(value)
            continue
        payload[key] = value
    return payload


def _variant_change_log(variant: dict[str, Any], run_config: dict[str, Any], family_id: str) -> list[dict[str, Any]]:
    rationale = dict(variant.get("rationale") or {})
    raw_log = list(variant.get("selection_rule_change_log") or [])
    if not raw_log:
        raw_log = [{}]
    current = dict(run_config["selection_rule_signatures"])
    previous = _text(variant.get("previous_rule_signature"), fallback="initial")
    records: list[dict[str, Any]] = []
    for index, raw in enumerate(raw_log, start=1):
        item = dict(raw or {})
        item.setdefault("change_id", f"{_slug(_text(variant.get('policy_variant_id')))}_change_{index}")
        item.setdefault("policy_id", run_config["policy_id"])
        item.setdefault("previous_rule_signature", previous)
        item.setdefault("new_rule_signature", current["selection_rule_signature"])
        item.setdefault(
            "changed_fields",
            [
                "selection_rule_signature",
                "entry_rule_signature",
                "add_rule_signature",
                "partial_take_rule_signature",
                "full_exit_rule_signature",
                "sizing_rule_signature",
            ],
        )
        item.setdefault("reason_code", _text(rationale.get("reason_code"), fallback="policy_variant_update"))
        item.setdefault("reason_text", _text(rationale.get("why_it_changed"), fallback="policy variant updated"))
        item.setdefault("expected_effect", _text(rationale.get("expected_effect"), fallback="improve relative performance"))
        item.setdefault("author_or_source", _text(rationale.get("author_or_source"), fallback="system"))
        item.setdefault("timestamp_or_run_id", _text(rationale.get("timestamp_or_run_id"), fallback=family_id))
        previous = _text(item["new_rule_signature"]) or previous
        records.append(item)
    return records


def _window_summary_rows(windows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for window in windows:
        summary = dict(window.get("window_summary") or {})
        relative = dict(window.get("relative_performance") or {})
        rows.append(
            {
                "window_start_date": _text(summary.get("window_start_date")),
                "window_end_date": _text(summary.get("window_end_date")),
                "final_score": float(summary.get("final_score") or 0.0),
                "portfolio_return_3m": float(summary.get("portfolio_return_3m") or 0.0),
                "excess_vs_universe": float(summary.get("excess_vs_universe") or 0.0),
                "excess_vs_market": float(summary.get("excess_vs_market") or 0.0),
                "exposure_adjusted_excess": float(relative.get("exposure_adjusted_excess") or 0.0),
                "median_window_excess": float(relative.get("median_window_excess") or 0.0),
                "worst_window_excess": float(relative.get("worst_window_excess") or 0.0),
                "weekly_activity_pass_rate": float(summary.get("weekly_activity_pass_rate") or 0.0),
            }
        )
    rows.sort(key=lambda row: (row["window_start_date"], row["window_end_date"]))
    return rows


def _aggregate_variant_metrics(variant_result: dict[str, Any]) -> dict[str, Any]:
    windows = list(variant_result.get("windows") or [])
    if not windows:
        return {}
    summaries = [dict(window.get("window_summary") or {}) for window in windows]
    relatives = [dict(window.get("relative_performance") or {}) for window in windows]
    window_rows = _window_summary_rows(windows)
    excess_vs_universe = [float(row.get("excess_vs_universe") or 0.0) for row in window_rows]
    exposure_adjusted = [float(row.get("exposure_adjusted_excess") or 0.0) for row in window_rows]
    worst_excess = [float(row.get("worst_window_excess") or 0.0) for row in window_rows]
    final_scores = [float(row.get("final_score") or 0.0) for row in window_rows]
    total_return = [float(row.get("portfolio_return_3m") or 0.0) for row in window_rows]
    holdings = [float(summary.get("avg_holding_days") or 0.0) for summary in summaries]
    med_hold = [float(summary.get("median_holding_days") or 0.0) for summary in summaries]
    pct_over_20 = [float(summary.get("pct_trades_over_20d") or 0.0) for summary in summaries]
    turnover = [float(summary.get("turnover") or 0.0) for summary in summaries]
    weekly_activity = [float(summary.get("weekly_activity_pass_rate") or 0.0) for summary in summaries]
    max_drawdown = [float(summary.get("max_drawdown") or 0.0) for summary in summaries]
    concentration = [float(summary.get("concentration_penalty") or 0.0) for summary in summaries]
    long_hold_bonus = [float(summary.get("long_hold_bonus") or 0.0) for summary in summaries]
    premature_exit = [float(summary.get("premature_exit_penalty") or 0.0) for summary in summaries]
    avg_days_to_first_partial_exit = [float(summary.get("avg_days_to_first_partial_exit") or 0.0) for summary in summaries if summary.get("avg_days_to_first_partial_exit") is not None]
    return {
        "window_count": len(windows),
        "total_return_mean": sum(total_return) / len(total_return),
        "excess_vs_universe_mean": sum(excess_vs_universe) / len(excess_vs_universe),
        "exposure_adjusted_excess_mean": sum(exposure_adjusted) / len(exposure_adjusted),
        "median_window_excess": _med(excess_vs_universe) or 0.0,
        "worst_window_excess": min(worst_excess) if worst_excess else 0.0,
        "final_score_mean": sum(final_scores) / len(final_scores),
        "avg_holding_days_mean": sum(holdings) / len(holdings),
        "median_holding_days_mean": sum(med_hold) / len(med_hold),
        "pct_trades_over_20d_mean": sum(pct_over_20) / len(pct_over_20),
        "turnover_mean": sum(turnover) / len(turnover),
        "weekly_activity_pass_rate_mean": sum(weekly_activity) / len(weekly_activity),
        "max_drawdown_mean": sum(max_drawdown) / len(max_drawdown),
        "concentration_penalty_mean": sum(concentration) / len(concentration),
        "long_hold_bonus_mean": sum(long_hold_bonus) / len(long_hold_bonus),
        "premature_exit_penalty_mean": sum(premature_exit) / len(premature_exit),
        "avg_days_to_first_partial_exit_mean": sum(avg_days_to_first_partial_exit) / len(avg_days_to_first_partial_exit) if avg_days_to_first_partial_exit else None,
        "window_rows": window_rows,
        "window_summaries": summaries,
        "relative_performance_rows": relatives,
    }


def _decision_from_metrics(metrics: dict[str, Any], *, thresholds: dict[str, Any] | None = None) -> tuple[str, list[str], str]:
    thresholds = dict(thresholds or {})
    keep_thresholds = dict(thresholds.get("keep") or {})
    drop_thresholds = dict(thresholds.get("drop") or {})
    window_count = int(metrics.get("window_count") or 0)
    reason_codes: list[str] = []
    if window_count < 3:
        reason_codes.append("promising_but_sample_small")

    median_window_excess = float(metrics.get("median_window_excess") or 0.0)
    exposure_adjusted_excess_mean = float(metrics.get("exposure_adjusted_excess_mean") or 0.0)
    worst_window_excess = float(metrics.get("worst_window_excess") or 0.0)
    weekly_activity_pass_rate_mean = float(metrics.get("weekly_activity_pass_rate_mean") or 0.0)
    avg_holding_days_mean = float(metrics.get("avg_holding_days_mean") or 0.0)
    pct_trades_over_20d_mean = float(metrics.get("pct_trades_over_20d_mean") or 0.0)
    turnover_mean = float(metrics.get("turnover_mean") or 0.0)
    final_score_mean = float(metrics.get("final_score_mean") or 0.0)

    if median_window_excess <= float(drop_thresholds.get("median_window_excess_max", 0.0)) or exposure_adjusted_excess_mean <= float(drop_thresholds.get("exposure_adjusted_excess_mean_max", 0.0)):
        reason_codes.append("weak_relative_performance")
    if worst_window_excess <= float(drop_thresholds.get("worst_window_excess_max", -0.02)) and median_window_excess <= float(drop_thresholds.get("median_window_excess_max", 0.0)) + 0.01:
        reason_codes.append("unstable_across_windows")
    if turnover_mean >= float(drop_thresholds.get("turnover_mean_min", 1.25)):
        reason_codes.append("excessive_turnover")
    if avg_holding_days_mean <= float(drop_thresholds.get("avg_holding_days_mean_max", 20.0)) and pct_trades_over_20d_mean <= float(drop_thresholds.get("pct_trades_over_20d_mean_max", 0.35)):
        reason_codes.append("insufficient_long_hold_behavior")
    if weekly_activity_pass_rate_mean < float(drop_thresholds.get("weekly_activity_pass_rate_mean_max", 1.0)):
        reason_codes.append("weekly_activity_failures")

    if (
        median_window_excess >= float(keep_thresholds.get("median_window_excess_min", 0.0))
        and exposure_adjusted_excess_mean >= float(keep_thresholds.get("exposure_adjusted_excess_mean_min", 0.0))
        and worst_window_excess >= float(keep_thresholds.get("worst_window_excess_min", -0.01))
        and weekly_activity_pass_rate_mean >= float(keep_thresholds.get("weekly_activity_pass_rate_mean_min", 1.0))
        and avg_holding_days_mean >= float(keep_thresholds.get("avg_holding_days_mean_min", 20.0))
        and pct_trades_over_20d_mean >= float(keep_thresholds.get("pct_trades_over_20d_mean_min", 0.35))
        and turnover_mean <= float(keep_thresholds.get("turnover_mean_max", 1.25))
        and final_score_mean >= float(keep_thresholds.get("final_score_mean_min", float("-inf")))
        and window_count >= 3
    ):
        decision = "keep"
    elif "promising_but_sample_small" in reason_codes and len(reason_codes) == 1:
        decision = "hold"
    elif "weekly_activity_failures" in reason_codes or "excessive_turnover" in reason_codes:
        decision = "drop"
    elif final_score_mean <= float(drop_thresholds.get("final_score_mean_max", float("inf"))) and reason_codes:
        decision = "drop"
    elif "weak_relative_performance" in reason_codes and "promising_but_sample_small" not in reason_codes:
        decision = "hold" if final_score_mean >= float(keep_thresholds.get("final_score_mean_min", 0.0)) else "drop"
    elif "unstable_across_windows" in reason_codes and "promising_but_sample_small" not in reason_codes:
        decision = "hold" if final_score_mean >= float(keep_thresholds.get("final_score_mean_min", 0.0)) else "drop"
    elif float(metrics.get("median_window_excess") or 0.0) > 0.0 and float(metrics.get("worst_window_excess") or 0.0) >= -0.01 and float(metrics.get("exposure_adjusted_excess_mean") or 0.0) > 0.0 and not reason_codes:
        decision = "keep"
    elif float(metrics.get("median_window_excess") or 0.0) > 0.0 and "weak_relative_performance" not in reason_codes:
        decision = "hold"
    else:
        decision = "drop" if reason_codes else "hold"

    reason_text = "; ".join(POLICY_DECISION_REASON_TEXT.get(code, code) for code in reason_codes) if reason_codes else "policy meets current evaluation criteria"
    return decision, list(dict.fromkeys(reason_codes)), reason_text


def _policy_variant_signature(variant_run_config: dict[str, Any], *, family_id: str, variant_id: str) -> str:
    return _stable_hash(
        {
            "family_id": family_id,
            "variant_id": variant_id,
            "selection_rule_signatures": variant_run_config["selection_rule_signatures"],
            "policy_family_signature": variant_run_config["policy_family_signature"],
        }
    )


def _variant_result_payload(
    *,
    variant: dict[str, Any],
    variant_run_config: dict[str, Any],
    windows: list[dict[str, Any]],
    family_id: str,
    decision_thresholds: dict[str, Any] | None = None,
) -> dict[str, Any]:
    window_start_dates = [str(item) for item in variant_run_config.get("window_start_dates") or [] if str(item).strip()]
    leaderboard_rows = _window_summary_rows(windows)
    metrics = _aggregate_variant_metrics({"windows": windows})
    decision, reason_codes, reason_text = _decision_from_metrics(metrics, thresholds=decision_thresholds)
    policy_variant_id = _text(variant.get("policy_variant_id"), fallback=_text(variant_run_config.get("policy_version"), fallback="variant"))
    result = {
        "schema_version": POLICY_FAMILY_SCHEMA_VERSION,
        "family_id": family_id,
        "policy_variant_id": policy_variant_id,
        "policy_variant_signature": _policy_variant_signature(variant_run_config, family_id=family_id, variant_id=policy_variant_id),
        "window_start_dates": window_start_dates,
        "run_config": variant_run_config,
        "windows": windows,
        "multiwindow_leaderboard": {
            "schema_version": REPLAY_SCHEMA_VERSION,
            "policy_id": variant_run_config["policy_id"],
            "policy_version": variant_run_config["policy_version"],
            "selection_rule_signatures": variant_run_config["selection_rule_signatures"],
            "rows": sorted(
                leaderboard_rows,
                key=lambda row: (
                    -float(row["median_window_excess"]),
                    -float(row["exposure_adjusted_excess"]),
                    -float(row["worst_window_excess"]),
                    -float(row["final_score"]),
                    row["window_start_date"],
                ),
            ),
            "median_window_excess": metrics.get("median_window_excess") or 0.0,
            "worst_window_excess": metrics.get("worst_window_excess") or 0.0,
        },
        "summary": metrics,
        "decision_thresholds": _json_ready(decision_thresholds or {}),
        "decision": {
            "decision": decision,
            "reason_codes": reason_codes,
            "reason_text": reason_text,
        },
        "change_log": _variant_change_log(variant, variant_run_config, family_id),
    }
    result["variant_result_hash"] = _stable_hash(_json_ready(result))
    return result


def _family_manifest(
    *,
    family_id: str,
    family_name: str,
    shared_payload: dict[str, Any],
    variant_results: list[dict[str, Any]],
) -> dict[str, Any]:
    manifest_variants: list[dict[str, Any]] = []
    for result in variant_results:
        variant_run_config = dict(result["run_config"])
        summary = dict(result["summary"])
        manifest_variants.append(
            {
                "policy_variant_id": _text(result["policy_variant_id"]),
                "policy_variant_signature": _text(result["policy_variant_signature"]),
                "policy_id": _text(variant_run_config.get("policy_id")),
                "policy_version": _text(variant_run_config.get("policy_version")),
                "selection_rule_signatures": dict(variant_run_config.get("selection_rule_signatures") or {}),
                "window_start_dates": list(result.get("window_start_dates") or []),
                "decision": dict(result.get("decision") or {}),
                "summary": {
                    "window_count": summary.get("window_count"),
                    "median_window_excess": summary.get("median_window_excess"),
                    "worst_window_excess": summary.get("worst_window_excess"),
                    "exposure_adjusted_excess_mean": summary.get("exposure_adjusted_excess_mean"),
                    "weekly_activity_pass_rate_mean": summary.get("weekly_activity_pass_rate_mean"),
                    "avg_holding_days_mean": summary.get("avg_holding_days_mean"),
                    "turnover_mean": summary.get("turnover_mean"),
                },
                "change_log": list(result.get("change_log") or []),
                "variant_result_path": _text(result.get("variant_result_path")),
            }
        )
    condition_signature = _stable_hash(
        {
            "window_start_dates": list(shared_payload.get("window_start_dates") or []),
            "window_months": int(shared_payload.get("window_months") or 3),
            "universe": list(shared_payload.get("universe") or []),
            "market_benchmark_symbol": _text(shared_payload.get("market_benchmark_symbol")),
            "capital": dict(shared_payload.get("capital") or {}),
            "scoring": dict(shared_payload.get("scoring") or {}),
            "execution_convention": _text(shared_payload.get("execution_convention") or "close_close_research_convention"),
            "weekly_activity_required": bool(shared_payload.get("weekly_activity_required", True)),
            "short_cash_reusable": bool(shared_payload.get("short_cash_reusable", False)),
        }
    )
    family_signature = _stable_hash(
        {
            "family_id": family_id,
            "family_name": family_name,
            "condition_signature": condition_signature,
            "variant_signatures": [item["policy_variant_signature"] for item in manifest_variants],
        }
    )
    return {
        "schema_version": POLICY_FAMILY_SCHEMA_VERSION,
        "family_id": family_id,
        "family_name": family_name,
        "family_signature": family_signature,
        "condition_signature": condition_signature,
        "shared_condition": _json_ready(shared_payload),
        "policy_variants": manifest_variants,
    }


def build_policy_family_replay(
    repo: StockRepository,
    payload: dict[str, Any],
    *,
    shared_context_cache: dict[str, dict[str, Any]] | None = None,
    decision_thresholds: dict[str, Any] | None = None,
) -> dict[str, Any]:
    shared_payload = dict(payload.get("shared") or {})
    for key in (
        "policy_id",
        "policy_version",
        "window_start_date",
        "window_start_dates",
        "window_months",
        "universe",
        "market_benchmark_symbol",
        "capital",
        "scoring",
        "policy",
        "unit_scale",
        "addon_units",
        "execution_convention",
        "weekly_activity_required",
        "short_cash_reusable",
        "selection_rule_change_log",
    ):
        if key not in shared_payload and key in payload:
            shared_payload[key] = payload.get(key)
    variants_input = list(payload.get("policy_variants") or [])
    if not variants_input:
        variants_input = [
            {
                "policy_variant_id": _text(payload.get("policy_variant_id"), fallback=_text(shared_payload.get("policy_version"), fallback="variant_1")),
                "policy_id": shared_payload.get("policy_id"),
                "policy_version": shared_payload.get("policy_version"),
                "policy": dict(shared_payload.get("policy") or {}),
                "entry_rule": dict(payload.get("entry_rule") or {}),
                "add_rule": dict(payload.get("add_rule") or {}),
                "partial_take_rule": dict(payload.get("partial_take_rule") or {}),
                "full_exit_rule": dict(payload.get("full_exit_rule") or {}),
                "sizing_rule": dict(payload.get("sizing_rule") or {}),
                "selection_rule": dict(payload.get("selection_rule") or {}),
                "capital": dict(shared_payload.get("capital") or {}),
                "scoring": dict(shared_payload.get("scoring") or {}),
                "selection_rule_change_log": list(shared_payload.get("selection_rule_change_log") or []),
                "rationale": dict(payload.get("rationale") or {}),
            }
        ]

    window_start_dates = [str(item) for item in (shared_payload.get("window_start_dates") or payload.get("window_start_dates") or [shared_payload.get("window_start_date") or payload.get("window_start_date")]) if _text(item)]
    if not window_start_dates:
        raise ValueError("window_start_dates is required")
    shared_payload["window_start_dates"] = window_start_dates
    family_name = _text(payload.get("family_name") or payload.get("policy_family_name") or shared_payload.get("policy_id"), fallback="policy_family")
    family_id = _text(payload.get("family_id")) or f"policy-family-{_stable_hash({'family_name': family_name, 'shared': shared_payload, 'variant_count': len(variants_input)})[:16]}"
    family_dir = _family_dir(family_id)
    family_result_path = family_dir / POLICY_FAMILY_RESULT_FILE
    if family_result_path.exists():
        return os_store.read_json_object_strict(family_result_path, artifact_name="policy family result")

    variant_results: list[dict[str, Any]] = []
    context_cache: dict[str, dict[str, Any]] = shared_context_cache if shared_context_cache is not None else {}

    def _context_for(window_start_text: str) -> dict[str, Any]:
        cached = context_cache.get(window_start_text)
        if cached is not None:
            return cached
        context = prepare_replay_window_context(repo, normalize_replay_run_config(_merge_shared_and_variant(shared_payload, variants_input[0])), _parse_date(window_start_text))
        context_cache[window_start_text] = context
        return context

    for variant_index, variant in enumerate(variants_input, start=1):
        merged_payload = _merge_shared_and_variant(shared_payload, variant)
        merged_payload["window_start_dates"] = window_start_dates
        variant_run_config = normalize_replay_run_config(merged_payload)
        variant_id = _text(variant.get("policy_variant_id"), fallback=f"variant_{variant_index}")
        variant_run_config["policy_variant_id"] = variant_id
        variant_run_config["policy_family_id"] = family_id
        variant_run_config["policy_variant_signature"] = _policy_variant_signature(variant_run_config, family_id=family_id, variant_id=variant_id)

        windows: list[dict[str, Any]] = []
        for window_start_text in window_start_dates:
            context = _context_for(window_start_text)
            windows.append(_simulate_window(repo, variant_run_config, _parse_date(window_start_text), context=context))

        variant_result = _variant_result_payload(
            variant=variant,
            variant_run_config=variant_run_config,
            windows=windows,
            family_id=family_id,
            decision_thresholds=decision_thresholds,
        )
        variant_result_path = _variant_dir(family_dir, variant_id) / POLICY_VARIANT_RESULT_FILE
        variant_result["variant_result_path"] = str(variant_result_path)
        _write_json(variant_result_path, variant_result)
        _write_json(variant_result_path.parent / "replay_run_config.json", variant_result["run_config"])
        _write_json(variant_result_path.parent / "replay_multiwindow_leaderboard.json", variant_result["multiwindow_leaderboard"])
        _write_json(variant_result_path.parent / "replay_selection_rule_change_log.json", {"schema_version": POLICY_FAMILY_SCHEMA_VERSION, "items": variant_result["change_log"]})
        variant_results.append(variant_result)

    manifest = _family_manifest(
        family_id=family_id,
        family_name=family_name,
        shared_payload=shared_payload,
        variant_results=variant_results,
    )
    comparison_rows = [
        {
            "policy_variant_id": item["policy_variant_id"],
            "policy_variant_signature": item["policy_variant_signature"],
            "decision": item["decision"]["decision"],
            "reason_codes": list(item["decision"]["reason_codes"]),
            "reason_text": item["decision"]["reason_text"],
            **dict(item["summary"]),
        }
        for item in variant_results
    ]
    comparison_rows.sort(
        key=lambda row: (
            -float(row.get("median_window_excess") or 0.0),
            -float(row.get("exposure_adjusted_excess_mean") or 0.0),
            -float(row.get("worst_window_excess") or 0.0),
            -float(row.get("final_score_mean") or 0.0),
            _text(row.get("policy_variant_id")),
        )
    )
    comparison_matrix = {
        "schema_version": POLICY_FAMILY_SCHEMA_VERSION,
        "family_id": family_id,
        "family_name": family_name,
        "policy_variant_count": len(variant_results),
        "rows": comparison_rows,
    }
    decision_log = {
        "schema_version": POLICY_FAMILY_SCHEMA_VERSION,
        "family_id": family_id,
        "family_name": family_name,
        "rows": [
            {
                "policy_variant_id": item["policy_variant_id"],
                "policy_variant_signature": item["policy_variant_signature"],
                "decision": item["decision"]["decision"],
                "reason_codes": list(item["decision"]["reason_codes"]),
                "reason_text": item["decision"]["reason_text"],
                "what_changed": _json_ready(dict((item.get("change_log") or [{}])[0])),
                "why_it_changed": _text(dict(variant.get("rationale") or {}).get("why_it_changed"), fallback="policy variant update"),
                "expected_effect": _text(dict(variant.get("rationale") or {}).get("expected_effect"), fallback="improve relative performance"),
                "result_after_evaluation": {
                    "median_window_excess": item["summary"].get("median_window_excess"),
                    "worst_window_excess": item["summary"].get("worst_window_excess"),
                    "exposure_adjusted_excess_mean": item["summary"].get("exposure_adjusted_excess_mean"),
                    "weekly_activity_pass_rate_mean": item["summary"].get("weekly_activity_pass_rate_mean"),
                    "avg_holding_days_mean": item["summary"].get("avg_holding_days_mean"),
                    "turnover_mean": item["summary"].get("turnover_mean"),
                },
            }
            for item, variant in zip(variant_results, variants_input, strict=True)
        ],
    }
    keep_rows = [row for row in decision_log["rows"] if row["decision"] == "keep"]
    hold_rows = [row for row in decision_log["rows"] if row["decision"] == "hold"]
    drop_rows = [row for row in decision_log["rows"] if row["decision"] == "drop"]
    policy_keep_drop_hold = {
        "schema_version": POLICY_FAMILY_SCHEMA_VERSION,
        "family_id": family_id,
        "family_name": family_name,
        "overview": {
            "candidate_count": len(decision_log["rows"]),
            "keep_count": len(keep_rows),
            "hold_count": len(hold_rows),
            "drop_count": len(drop_rows),
        },
        "rows": decision_log["rows"],
    }
    family_result = {
        "schema_version": POLICY_FAMILY_SCHEMA_VERSION,
        "family_id": family_id,
        "family_name": family_name,
        "family_edge": _text(payload.get("family_edge")),
        "family_thesis": _text(payload.get("family_thesis")),
        "policy_variant_manifest": manifest,
        "policy_comparison_matrix": comparison_matrix,
        "policy_decision_log": decision_log,
        "policy_keep_drop_hold": policy_keep_drop_hold,
        "decision_thresholds": _json_ready(decision_thresholds or {}),
        "policy_variant_results": [
            {
                "policy_variant_id": item["policy_variant_id"],
                "policy_variant_signature": item["policy_variant_signature"],
                "decision": item["decision"],
                "summary": item["summary"],
                "variant_result_path": item["variant_result_path"],
            }
            for item in variant_results
        ],
    }
    _write_json(family_dir / POLICY_VARIANT_MANIFEST_FILE, manifest)
    _write_json(family_dir / POLICY_COMPARISON_MATRIX_FILE, comparison_matrix)
    _write_json(family_dir / POLICY_DECISION_LOG_FILE, decision_log)
    _write_json(family_dir / POLICY_KEEP_DROP_HOLD_FILE, policy_keep_drop_hold)
    _write_json(family_dir / POLICY_FAMILY_RESULT_FILE, family_result)
    return family_result


def load_policy_family_replay(family_id: str) -> dict[str, Any]:
    family_dir = _family_dir(family_id)
    return os_store.read_json_object_strict(family_dir / POLICY_FAMILY_RESULT_FILE, artifact_name="policy family result")


def apply_policy_family_thresholds(family_result: dict[str, Any], thresholds: dict[str, Any]) -> dict[str, Any]:
    family_result = dict(family_result)
    family_result["decision_thresholds"] = _json_ready(thresholds)
    variant_results = [dict(item) for item in (family_result.get("policy_variant_results") or []) if isinstance(item, dict)]
    summary_rows = []
    decision_rows = []
    for variant_result in variant_results:
        summary = dict(variant_result.get("summary") or {})
        decision, reason_codes, reason_text = _decision_from_metrics(summary, thresholds=thresholds)
        variant_result["decision"] = {"decision": decision, "reason_codes": reason_codes, "reason_text": reason_text}
        summary_rows.append(
            {
                "policy_variant_id": variant_result.get("policy_variant_id"),
                "policy_variant_signature": variant_result.get("policy_variant_signature"),
                "decision": decision,
                "reason_codes": list(reason_codes),
                "reason_text": reason_text,
                "window_count": summary.get("window_count"),
                "total_return_mean": summary.get("total_return_mean"),
                "excess_vs_universe_mean": summary.get("excess_vs_universe_mean"),
                "exposure_adjusted_excess_mean": summary.get("exposure_adjusted_excess_mean"),
                "median_window_excess": summary.get("median_window_excess"),
                "worst_window_excess": summary.get("worst_window_excess"),
                "final_score_mean": summary.get("final_score_mean"),
                "avg_holding_days_mean": summary.get("avg_holding_days_mean"),
                "median_holding_days_mean": summary.get("median_holding_days_mean"),
                "pct_trades_over_20d_mean": summary.get("pct_trades_over_20d_mean"),
                "turnover_mean": summary.get("turnover_mean"),
                "weekly_activity_pass_rate_mean": summary.get("weekly_activity_pass_rate_mean"),
                "max_drawdown_mean": summary.get("max_drawdown_mean"),
                "concentration_penalty_mean": summary.get("concentration_penalty_mean"),
                "long_hold_bonus_mean": summary.get("long_hold_bonus_mean"),
                "premature_exit_penalty_mean": summary.get("premature_exit_penalty_mean"),
                "avg_days_to_first_partial_exit_mean": summary.get("avg_days_to_first_partial_exit_mean"),
                "window_rows": summary.get("window_rows"),
                "window_summaries": summary.get("window_summaries"),
                "relative_performance_rows": summary.get("relative_performance_rows"),
            }
        )
        change_log = list(variant_result.get("change_log") or [])
        decision_rows.append(
            {
                "policy_variant_id": variant_result.get("policy_variant_id"),
                "policy_variant_signature": variant_result.get("policy_variant_signature"),
                "decision": decision,
                "reason_codes": list(reason_codes),
                "reason_text": reason_text,
                "what_changed": _json_ready(dict((change_log or [{}])[0])),
                "why_it_changed": _text(dict((change_log or [{}])[0]).get("reason_text"), fallback="policy variant update"),
                "expected_effect": _text(dict((change_log or [{}])[0]).get("expected_effect"), fallback="improve relative performance"),
                "result_after_evaluation": {
                    "median_window_excess": summary.get("median_window_excess"),
                    "worst_window_excess": summary.get("worst_window_excess"),
                    "exposure_adjusted_excess_mean": summary.get("exposure_adjusted_excess_mean"),
                    "weekly_activity_pass_rate_mean": summary.get("weekly_activity_pass_rate_mean"),
                    "avg_holding_days_mean": summary.get("avg_holding_days_mean"),
                    "turnover_mean": summary.get("turnover_mean"),
                },
            }
        )
    family_result["policy_variant_results"] = variant_results
    family_result["policy_comparison_matrix"] = {
        "schema_version": POLICY_FAMILY_SCHEMA_VERSION,
        "family_id": _text(family_result.get("family_id")),
        "family_name": _text(family_result.get("family_name")),
        "policy_variant_count": len(summary_rows),
        "rows": sorted(
            summary_rows,
            key=lambda row: (
                -float(row.get("median_window_excess") or 0.0),
                -float(row.get("exposure_adjusted_excess_mean") or 0.0),
                -float(row.get("worst_window_excess") or 0.0),
                -float(row.get("final_score_mean") or 0.0),
                _text(row.get("policy_variant_id")),
            ),
        ),
    }
    family_result["policy_decision_log"] = {
        "schema_version": POLICY_FAMILY_SCHEMA_VERSION,
        "family_id": _text(family_result.get("family_id")),
        "family_name": _text(family_result.get("family_name")),
        "rows": decision_rows,
    }
    family_result["policy_keep_drop_hold"] = {
        "schema_version": POLICY_FAMILY_SCHEMA_VERSION,
        "family_id": _text(family_result.get("family_id")),
        "family_name": _text(family_result.get("family_name")),
        "overview": {
            "candidate_count": len(decision_rows),
            "keep_count": sum(1 for row in decision_rows if row["decision"] == "keep"),
            "hold_count": sum(1 for row in decision_rows if row["decision"] == "hold"),
            "drop_count": sum(1 for row in decision_rows if row["decision"] == "drop"),
        },
        "rows": decision_rows,
    }
    return family_result


def _cohort_root() -> Path:
    root = os_store.research_os_root() / "policy_replay" / "cohorts"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _cohort_dir(cohort_id: str) -> Path:
    path = _cohort_root() / _slug(cohort_id)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _cohort_family_manifest(family_payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for family in family_payloads:
        rows.append(
            {
                "family_id": _text(family.get("family_id")),
                "family_name": _text(family.get("family_name")),
                "family_edge": _text(family.get("family_edge")),
                "family_thesis": _text(family.get("family_thesis")),
                "policy_variant_count": len(family.get("policy_variants") or []),
                "variant_ids": [_text(item.get("policy_variant_id")) for item in (family.get("policy_variants") or []) if isinstance(item, dict)],
            }
        )
    return rows


def run_policy_family_cohort(repo: StockRepository, payload: dict[str, Any]) -> dict[str, Any]:
    cohort_id = _text(payload.get("cohort_id"), fallback="policy-family-first-cohort")
    cohort_dir = _cohort_dir(cohort_id)
    cohort_result_path = cohort_dir / POLICY_FAMILY_COHORT_RESULT_FILE
    if cohort_result_path.exists():
        return os_store.read_json_object_strict(cohort_result_path, artifact_name="policy family cohort result")

    family_payloads = list(payload.get("family_payloads") or [])
    if not family_payloads:
        family_payloads = _build_first_policy_family_cohort(payload)

    shared_context_cache: dict[str, dict[str, Any]] = {}
    family_results: list[dict[str, Any]] = []
    for family_payload in family_payloads:
        family_result = build_policy_family_replay(
            repo,
            family_payload,
            shared_context_cache=shared_context_cache,
        )
        family_results.append(family_result)

    calibration_rows = [
        row
        for family_result in family_results
        for row in (family_result.get("policy_comparison_matrix") or {}).get("rows") or []
        if isinstance(row, dict)
    ]
    calibration = _calibrate_thresholds_from_rows(calibration_rows)
    family_calibrations: dict[str, dict[str, Any]] = {}
    calibrated_family_results = []
    for family_result in family_results:
        family_id = _text(family_result.get("family_id"))
        family_rows = list((family_result.get("policy_comparison_matrix") or {}).get("rows") or [])
        family_calibration = _calibrate_thresholds_from_rows(family_rows)
        family_calibrations[family_id] = family_calibration
        calibrated_family_results.append(apply_policy_family_thresholds(family_result, family_calibration["thresholds"]))
    for family_result in calibrated_family_results:
        family_dir = _family_dir(_text(family_result.get("family_id")))
        _write_json(family_dir / POLICY_FAMILY_RESULT_FILE, family_result)
        _write_json(family_dir / POLICY_COMPARISON_MATRIX_FILE, family_result["policy_comparison_matrix"])
        _write_json(family_dir / POLICY_DECISION_LOG_FILE, family_result["policy_decision_log"])
        _write_json(family_dir / POLICY_KEEP_DROP_HOLD_FILE, family_result["policy_keep_drop_hold"])

    cohort_manifest = {
        "schema_version": POLICY_FAMILY_COHORT_SCHEMA_VERSION,
        "cohort_id": cohort_id,
        "cohort_signature": _stable_hash(
            {
                "cohort_id": cohort_id,
                "family_manifest": _cohort_family_manifest(family_payloads),
                "shared_condition": _json_ready(dict(payload.get("shared") or {})),
            }
        ),
        "family_manifest": _cohort_family_manifest(family_payloads),
    }
    cohort_summary_rows = []
    for family_result in calibrated_family_results:
        comparison_rows = list((family_result.get("policy_comparison_matrix") or {}).get("rows") or [])
        decision_rows = list((family_result.get("policy_decision_log") or {}).get("rows") or [])
        keep_count = sum(1 for row in decision_rows if _text(row.get("decision")) == "keep")
        hold_count = sum(1 for row in decision_rows if _text(row.get("decision")) == "hold")
        drop_count = sum(1 for row in decision_rows if _text(row.get("decision")) == "drop")
        cohort_summary_rows.append(
            {
                "family_id": _text(family_result.get("family_id")),
                "family_name": _text(family_result.get("family_name")),
                "family_edge": _text(family_result.get("family_edge")),
                "family_signature": _text((family_result.get("policy_variant_manifest") or {}).get("family_signature")),
                "decision_thresholds": _json_ready(family_result.get("decision_thresholds") or {}),
                "median_window_excess": _med([float(row.get("median_window_excess") or 0.0) for row in comparison_rows]) or 0.0,
                "best_median_window_excess": max((float(row.get("median_window_excess") or 0.0) for row in comparison_rows), default=0.0),
                "best_exposure_adjusted_excess_mean": max((float(row.get("exposure_adjusted_excess_mean") or 0.0) for row in comparison_rows), default=0.0),
                "worst_worst_window_excess": min((float(row.get("worst_window_excess") or 0.0) for row in comparison_rows), default=0.0),
                "keep_count": keep_count,
                "hold_count": hold_count,
                "drop_count": drop_count,
            }
        )
    cohort_result = {
        "schema_version": POLICY_FAMILY_COHORT_SCHEMA_VERSION,
        "cohort_id": cohort_id,
        "cohort_manifest": cohort_manifest,
        "threshold_calibration": {
            "schema_version": POLICY_FAMILY_COHORT_SCHEMA_VERSION,
            "cohort_thresholds": calibration,
            "family_thresholds": family_calibrations,
        },
        "family_results": calibrated_family_results,
        "cohort_summary_rows": cohort_summary_rows,
        "keep_drop_hold_summary": {
            "keep_count": sum(row["keep_count"] for row in cohort_summary_rows),
            "hold_count": sum(row["hold_count"] for row in cohort_summary_rows),
            "drop_count": sum(row["drop_count"] for row in cohort_summary_rows),
        },
    }
    _write_json(cohort_dir / POLICY_FAMILY_COHORT_MANIFEST_FILE, cohort_manifest)
    _write_json(cohort_dir / POLICY_THRESHOLD_CALIBRATION_FILE, cohort_result["threshold_calibration"])
    _write_json(cohort_dir / POLICY_FAMILY_COHORT_RESULT_FILE, cohort_result)
    return cohort_result


def load_policy_family_cohort(cohort_id: str) -> dict[str, Any]:
    cohort_dir = _cohort_dir(cohort_id)
    return os_store.read_json_object_strict(cohort_dir / POLICY_FAMILY_COHORT_RESULT_FILE, artifact_name="policy family cohort result")
