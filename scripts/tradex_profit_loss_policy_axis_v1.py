from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "profit_loss_policy_axis_v1"
SCHEMA_PREFIX = "tradex_profit_loss_policy_axis_v1"
DEFAULT_OUTPUT_DIR_NAME = "profit_loss_policy_axis_v1"
REQUIRED_OUTPUTS = (
    "policy_axis_result.json",
    "policy_axis_rows.csv",
    "policy_axis_metrics.csv",
    "policy_axis_period_stability.json",
    "policy_axis_trigger_audit.csv",
    "policy_axis_holdout_validation.json",
    "policy_axis_candidate_grid.csv",
    "policy_axis_cutoff_robustness.json",
    "policy_axis_promotion_readiness.json",
    "policy_axis_operator_review_pack.json",
    "policy_axis_forward_gate.json",
    "no_lookahead_audit.json",
    "_ARTIFACT_COMPLETE.json",
)
DECISIONS = (
    "keep_deterioration_reduce_for_forward_replay",
    "keep_day5_shock_near_high_partial_reduce_for_forward_replay",
    "keep_day5_shock_partial_reduce_for_forward_replay",
    "hold_profit_extension_needs_confirmation",
    "drop_policy_axis_no_joint_return_risk_edge",
    "blocked_missing_required_columns",
    "blocked_insufficient_outcome_coverage",
)

DAY5_SHOCK_THRESHOLD = -0.05
DAY5_SHOCK_TRIM_RATIO = 0.50
RECENT_HIGH_DISTANCE_MAX_FOR_KEEP = 0.00286836
NUMERIC_TOLERANCE = 1e-12


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "item"):
        return _json_ready(value.item())
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _max_drawdown(returns: pd.Series) -> float | None:
    clean = pd.to_numeric(returns, errors="coerce").dropna()
    if clean.empty:
        return None
    equity = (1.0 + clean).cumprod()
    peak = equity.cummax()
    return float((equity / peak - 1.0).min())


def _bool_series(frame: pd.DataFrame, name: str) -> pd.Series:
    if name not in frame.columns:
        return pd.Series(False, index=frame.index)
    raw = frame[name]
    if raw.dtype == bool:
        return raw.fillna(False)
    return raw.astype(str).str.lower().isin({"1", "true", "yes", "y"})


def _date_col(frame: pd.DataFrame) -> str | None:
    for name in ("as_of_date", "decision_ymd", "decision_date", "anchor_date"):
        if name in frame.columns:
            return name
    return None


def _code_col(frame: pd.DataFrame) -> str | None:
    for name in ("code", "symbol", "ticker"):
        if name in frame.columns:
            return name
    return None


def _required_columns(frame: pd.DataFrame) -> list[str]:
    missing = []
    for name in ("ret5", "ret20"):
        if name not in frame.columns:
            missing.append(name)
    if _date_col(frame) is None:
        missing.append("as_of_date|decision_ymd|decision_date|anchor_date")
    if _code_col(frame) is None:
        missing.append("code|symbol|ticker")
    return missing


def _metrics(rows: pd.DataFrame, return_col: str) -> dict[str, Any]:
    values = pd.to_numeric(rows[return_col], errors="coerce").dropna()
    if values.empty:
        return {
            "sample_count": 0,
            "mean_return": None,
            "median_return": None,
            "winner_rate_ge_10pct": None,
            "bad_loss_rate_le_minus_5pct": None,
            "severe_loss_rate_le_minus_10pct": None,
            "max_drawdown_proxy": None,
            "positive_return_sum": None,
            "negative_return_sum": None,
        }
    return {
        "sample_count": int(len(values)),
        "mean_return": float(values.mean()),
        "median_return": float(values.median()),
        "winner_rate_ge_10pct": float((values >= 0.10).mean()),
        "bad_loss_rate_le_minus_5pct": float((values <= -0.05).mean()),
        "severe_loss_rate_le_minus_10pct": float((values <= -0.10).mean()),
        "max_drawdown_proxy": _max_drawdown(values),
        "positive_return_sum": float(values[values > 0].sum()),
        "negative_return_sum": float(values[values < 0].sum()),
    }


def _delta(challenger: dict[str, Any], baseline: dict[str, Any], key: str) -> float | None:
    if challenger.get(key) is None or baseline.get(key) is None:
        return None
    return float(challenger[key] - baseline[key])


def _period_col(frame: pd.DataFrame) -> str:
    return "period_bucket" if "period_bucket" in frame.columns else "policy_axis_date"


def _build_policy_rows(frame: pd.DataFrame) -> pd.DataFrame:
    rows = frame.copy()
    date_name = _date_col(rows)
    code_name = _code_col(rows)
    assert date_name is not None
    assert code_name is not None
    rows["policy_axis_date"] = rows[date_name]
    rows["policy_axis_code"] = rows[code_name].astype(str)
    rows["baseline_hold20_return"] = pd.to_numeric(rows["ret20"], errors="coerce")
    rows["observable_day5_return"] = pd.to_numeric(rows["ret5"], errors="coerce")

    invalidation = _bool_series(rows, "invalidation_hit_20d")
    signal_reject = _bool_series(rows, "latest_signal_reject")
    not_ranked = _bool_series(rows, "not_in_latest_top_ranking")
    unrealized_loss = rows["observable_day5_return"] <= -0.03
    deterioration = invalidation | signal_reject | not_ranked | unrealized_loss
    profit_confirmed = (rows["observable_day5_return"] > 0.03) & ~invalidation & ~signal_reject & ~not_ranked
    day5_shock = rows["observable_day5_return"] <= DAY5_SHOCK_THRESHOLD
    recent_high_distance = pd.to_numeric(rows["recent_high_distance_pct"], errors="coerce") if "recent_high_distance_pct" in rows.columns else None
    day5_shock_near_high = (
        day5_shock & recent_high_distance.le(RECENT_HIGH_DISTANCE_MAX_FOR_KEEP)
        if recent_high_distance is not None
        else pd.Series(False, index=rows.index)
    )

    rows["deterioration_reduce_trigger"] = deterioration
    rows["day5_shock_partial_reduce_trigger"] = day5_shock
    rows["day5_shock_near_high_partial_reduce_trigger"] = day5_shock_near_high
    rows["profit_extension_trigger"] = profit_confirmed
    rows["deterioration_reduce_return"] = rows["baseline_hold20_return"].where(~deterioration, rows["observable_day5_return"])
    rows["day5_shock_partial_reduce_return"] = rows["baseline_hold20_return"].where(
        ~day5_shock,
        (1.0 - DAY5_SHOCK_TRIM_RATIO) * rows["baseline_hold20_return"] + DAY5_SHOCK_TRIM_RATIO * rows["observable_day5_return"],
    )
    rows["day5_shock_near_high_partial_reduce_return"] = rows["baseline_hold20_return"].where(
        ~day5_shock_near_high,
        (1.0 - DAY5_SHOCK_TRIM_RATIO) * rows["baseline_hold20_return"] + DAY5_SHOCK_TRIM_RATIO * rows["observable_day5_return"],
    )
    rows["profit_extension_return"] = rows["baseline_hold20_return"].where(profit_confirmed, rows["observable_day5_return"])
    rows["combined_reduce_then_extend_return"] = rows["baseline_hold20_return"].where(~deterioration, rows["observable_day5_return"])
    rows.loc[~profit_confirmed & ~deterioration, "combined_reduce_then_extend_return"] = rows.loc[
        ~profit_confirmed & ~deterioration, "observable_day5_return"
    ]
    rows["policy_note"] = "ret5_is_observable_day5_proxy_no_entry_selector_change"
    return rows


def _metric_deltas(policy_metrics: dict[str, Any], baseline_metrics: dict[str, Any]) -> dict[str, float | None]:
    return {key: _delta(policy_metrics, baseline_metrics, key) for key in policy_metrics if key != "sample_count"}


def _period_stability(rows: pd.DataFrame, policy_col: str) -> dict[str, Any]:
    period_name = _period_col(rows)
    periods: dict[str, Any] = {}
    evaluated_period_count = 0
    improved_period_count = 0
    severe_not_worse_count = 0
    dd_not_worse_count = 0
    trigger_period_count = 0
    for period, group in rows.groupby(period_name, sort=True):
        baseline = _metrics(group, "baseline_hold20_return")
        policy = _metrics(group, policy_col)
        deltas = _metric_deltas(policy, baseline)
        trigger_count = int(group["day5_shock_partial_reduce_trigger"].fillna(False).astype(bool).sum())
        if baseline["sample_count"]:
            evaluated_period_count += 1
            if (deltas.get("mean_return") or 0.0) > 0:
                improved_period_count += 1
            if deltas.get("severe_loss_rate_le_minus_10pct") is not None and deltas["severe_loss_rate_le_minus_10pct"] <= 0:
                severe_not_worse_count += 1
            if deltas.get("max_drawdown_proxy") is not None and deltas["max_drawdown_proxy"] >= 0:
                dd_not_worse_count += 1
        if trigger_count:
            trigger_period_count += 1
        periods[str(period)] = {
            "baseline": baseline,
            "policy": policy,
            "deltas_vs_baseline": deltas,
            "trigger_count": trigger_count,
        }
    return {
        "schema_version": f"{SCHEMA_PREFIX}_period_stability_v1",
        "policy": "day5_shock_partial_reduce",
        "period_column": period_name,
        "evaluated_period_count": evaluated_period_count,
        "trigger_period_count": trigger_period_count,
        "improved_period_count": improved_period_count,
        "severe_not_worse_period_count": severe_not_worse_count,
        "dd_not_worse_period_count": dd_not_worse_count,
        "stable_enough_for_forward_replay": bool(
            evaluated_period_count > 0
            and trigger_period_count >= 2
            and severe_not_worse_count == evaluated_period_count
            and dd_not_worse_count == evaluated_period_count
        ),
        "periods": periods,
    }


def _trigger_audit_rows(rows: pd.DataFrame) -> pd.DataFrame:
    triggers = rows[rows["day5_shock_partial_reduce_trigger"].fillna(False).astype(bool)].copy()
    if triggers.empty:
        return pd.DataFrame(
            columns=[
                "policy_axis_date",
                "policy_axis_code",
                "period_bucket",
                "observable_day5_return",
                "baseline_hold20_return",
                "day5_shock_partial_reduce_return",
                "return_delta_vs_baseline",
                "trigger_classification",
                "severe_loss_avoided",
                "false_reduce",
            ]
        )
    triggers["period_bucket"] = triggers[_period_col(triggers)]
    triggers["return_delta_vs_baseline"] = triggers["day5_shock_partial_reduce_return"] - triggers["baseline_hold20_return"]
    triggers["severe_loss_avoided"] = (triggers["baseline_hold20_return"] <= -0.10) & (
        triggers["day5_shock_partial_reduce_return"] > -0.10
    )
    triggers["false_reduce"] = triggers["baseline_hold20_return"] >= 0.10
    triggers["trigger_classification"] = "neutral_or_loss_containment"
    triggers.loc[triggers["severe_loss_avoided"], "trigger_classification"] = "saved_severe_loss"
    triggers.loc[triggers["false_reduce"], "trigger_classification"] = "false_reduce_winner"
    return triggers[
        [
            "policy_axis_date",
            "policy_axis_code",
            "period_bucket",
            "observable_day5_return",
            "baseline_hold20_return",
            "day5_shock_partial_reduce_return",
            "return_delta_vs_baseline",
            "trigger_classification",
            "severe_loss_avoided",
            "false_reduce",
        ]
    ]


def _trigger_audit_summary(trigger_rows: pd.DataFrame) -> dict[str, Any]:
    if trigger_rows.empty:
        return {
            "trigger_count": 0,
            "saved_severe_loss_count": 0,
            "false_reduce_winner_count": 0,
            "mean_trigger_return_delta_vs_baseline": None,
        }
    return {
        "trigger_count": int(len(trigger_rows)),
        "saved_severe_loss_count": int(trigger_rows["severe_loss_avoided"].fillna(False).astype(bool).sum()),
        "false_reduce_winner_count": int(trigger_rows["false_reduce"].fillna(False).astype(bool).sum()),
        "mean_trigger_return_delta_vs_baseline": float(pd.to_numeric(trigger_rows["return_delta_vs_baseline"], errors="coerce").mean()),
    }


def _subset_validation_payload(rows: pd.DataFrame, label: str) -> dict[str, Any]:
    baseline = _metrics(rows, "baseline_hold20_return")
    policy = _metrics(rows, "day5_shock_partial_reduce_return")
    deltas = _metric_deltas(policy, baseline)
    trigger_count = int(rows["day5_shock_partial_reduce_trigger"].fillna(False).astype(bool).sum()) if not rows.empty else 0
    joint_pass = bool(
        baseline["sample_count"] > 0
        and trigger_count > 0
        and (deltas.get("mean_return") or 0.0) > 0
        and deltas.get("bad_loss_rate_le_minus_5pct") is not None
        and deltas["bad_loss_rate_le_minus_5pct"] <= 0
        and deltas.get("severe_loss_rate_le_minus_10pct") is not None
        and deltas["severe_loss_rate_le_minus_10pct"] <= 0
        and deltas.get("max_drawdown_proxy") is not None
        and deltas["max_drawdown_proxy"] >= -NUMERIC_TOLERANCE
    )
    return {
        "label": label,
        "row_count": int(len(rows)),
        "trigger_count": trigger_count,
        "baseline": baseline,
        "policy": policy,
        "deltas_vs_baseline": deltas,
        "joint_return_risk_pass": joint_pass,
    }


def _holdout_validation(rows: pd.DataFrame, cutoff_ymd: int | None) -> dict[str, Any]:
    date_values = pd.to_numeric(rows["policy_axis_date"], errors="coerce")
    if cutoff_ymd is None:
        unique_dates = sorted(int(value) for value in date_values.dropna().unique())
        cutoff_ymd = unique_dates[int(len(unique_dates) * 0.75) - 1] if len(unique_dates) >= 4 else None
    if cutoff_ymd is None:
        return {
            "schema_version": f"{SCHEMA_PREFIX}_holdout_validation_v1",
            "enabled": False,
            "reason": "insufficient_distinct_dates",
        }
    train = rows[date_values <= int(cutoff_ymd)].copy()
    holdout = rows[date_values > int(cutoff_ymd)].copy()
    train_payload = _subset_validation_payload(train, "train")
    holdout_payload = _subset_validation_payload(holdout, "holdout")
    return {
        "schema_version": f"{SCHEMA_PREFIX}_holdout_validation_v1",
        "enabled": True,
        "cutoff_ymd": int(cutoff_ymd),
        "fixed_policy": {
            "day5_shock_threshold": DAY5_SHOCK_THRESHOLD,
            "day5_shock_trim_ratio": DAY5_SHOCK_TRIM_RATIO,
        },
        "train": train_payload,
        "holdout": holdout_payload,
        "holdout_decision": "holdout_pass" if holdout_payload["joint_return_risk_pass"] else "holdout_fail_or_underpowered",
        "same_condition_check": {
            "same_input_schema": True,
            "same_policy_threshold": True,
            "same_trim_ratio": True,
            "entry_selector_changed": False,
        },
    }


def _cutoff_robustness(rows: pd.DataFrame, cutoffs: list[int] | None = None) -> dict[str, Any]:
    date_values = pd.to_numeric(rows["policy_axis_date"], errors="coerce")
    if cutoffs is None:
        if "period_bucket" in rows.columns:
            period_max_dates = (
                rows.groupby("period_bucket", sort=True)["policy_axis_date"]
                .max()
                .pipe(pd.to_numeric, errors="coerce")
                .dropna()
                .astype(int)
                .sort_values()
                .tolist()
            )
            max_date = int(date_values.max()) if date_values.notna().any() else None
            cutoffs = [value for value in period_max_dates if max_date is not None and value < max_date]
        else:
            unique_dates = sorted(int(value) for value in date_values.dropna().unique())
            cutoffs = [unique_dates[int(len(unique_dates) * ratio) - 1] for ratio in (0.5, 0.67, 0.75) if len(unique_dates) >= 4]
    rows_out: list[dict[str, Any]] = []
    for cutoff in sorted(set(int(value) for value in cutoffs or [])):
        grid = _candidate_grid(rows, cutoff)
        target = grid[
            grid["variant"].eq("day5_shock_near_or_below_recent_high_trim50") & grid["subset"].eq("holdout")
        ]
        if target.empty:
            continue
        row = target.iloc[0].to_dict()
        rows_out.append(row)
    valid_rows = [row for row in rows_out if int(row.get("trigger_count") or 0) > 0]
    pass_rows = [row for row in valid_rows if bool(row.get("joint_return_risk_pass"))]
    return {
        "schema_version": f"{SCHEMA_PREFIX}_cutoff_robustness_v1",
        "policy": "day5_shock_near_or_below_recent_high_trim50",
        "cutoff_count": int(len(rows_out)),
        "valid_holdout_cutoff_count": int(len(valid_rows)),
        "pass_holdout_cutoff_count": int(len(pass_rows)),
        "robustness_decision": "robustness_pass" if valid_rows and len(pass_rows) == len(valid_rows) else "robustness_hold_or_fail",
        "rows": rows_out,
    }


def _promotion_readiness(
    *,
    decision: str,
    decision_class: str,
    robustness: dict[str, Any],
    ret20_ready_count: int,
    input_row_count: int,
) -> dict[str, Any]:
    robustness_pass = robustness.get("robustness_decision") == "robustness_pass"
    forward_mature = bool(input_row_count > 0 and ret20_ready_count == input_row_count)
    promotable = bool(decision_class == "KEEP" and robustness_pass and forward_mature)
    blockers: list[str] = []
    if decision_class != "KEEP":
        blockers.append("policy_decision_not_keep")
    if not robustness_pass:
        blockers.append("robustness_not_pass")
    if not forward_mature:
        blockers.append("forward_ret20_not_fully_mature")
    readiness = "promotable_for_operational_review" if promotable else "not_promotable_forward_ret20_pending"
    return {
        "schema_version": f"{SCHEMA_PREFIX}_promotion_readiness_v1",
        "policy": "day5_shock_near_high_partial_reduce",
        "readiness_decision": readiness,
        "promotable_for_operational_review": promotable,
        "blockers": blockers,
        "required_before_meemee_reflection": [
            "forward_ret20_all_candidates_ready",
            "forward_policy_axis_keep",
            "manual_operator_review",
        ],
        "runtime_db_write": False,
        "meemee_reflection": False,
        "production_ranking_changed": False,
        "decision": decision,
        "decision_class": decision_class,
        "ret20_ready_count": int(ret20_ready_count),
        "input_row_count": int(input_row_count),
        "robustness_decision": robustness.get("robustness_decision"),
    }


def _operator_review_pack(
    *,
    result: dict[str, Any],
    promotion: dict[str, Any],
    robustness: dict[str, Any],
    trigger_summary: dict[str, Any],
    metrics: dict[str, Any] | None = None,
) -> dict[str, Any]:
    metrics = metrics or {}
    baseline = metrics.get("baseline_hold20", {})
    policy = metrics.get("day5_shock_near_high_partial_reduce", {})
    deltas = result.get("deltas_vs_baseline", {}).get("day5_shock_near_high_partial_reduce", {})
    if promotion.get("promotable_for_operational_review"):
        review_decision = "review_candidate_keep"
        next_action = "manual_operator_review_only"
    elif result.get("decision_class") == "BLOCKED":
        review_decision = "blocked_wait_for_forward_maturity"
        next_action = "rerun_after_forward_ret20_ready"
    else:
        review_decision = "hold_do_not_promote"
        next_action = "continue_research_no_meemee_reflection"
    return {
        "schema_version": f"{SCHEMA_PREFIX}_operator_review_pack_v1",
        "axis_id": AXIS_ID,
        "review_decision": review_decision,
        "next_action": next_action,
        "policy": "day5_shock_near_high_partial_reduce",
        "policy_rule": {
            "day5_shock_threshold": DAY5_SHOCK_THRESHOLD,
            "recent_high_distance_max_for_keep": RECENT_HIGH_DISTANCE_MAX_FOR_KEEP,
            "trim_ratio": DAY5_SHOCK_TRIM_RATIO,
            "plain_language": "If ret5 is at or below -5% and the candidate is near or below its recent high, treat half of the position as reduced in research replay.",
        },
        "authoritative_result": {
            "decision": result.get("decision"),
            "decision_class": result.get("decision_class"),
            "decision_evidence": result.get("decision_evidence", []),
            "robustness_decision": robustness.get("robustness_decision"),
            "promotion_readiness": promotion.get("readiness_decision"),
        },
        "metric_summary": {
            "baseline_mean_return": baseline.get("mean_return"),
            "policy_mean_return": policy.get("mean_return"),
            "mean_return_delta": deltas.get("mean_return"),
            "baseline_severe_loss_rate": baseline.get("severe_loss_rate_le_minus_10pct"),
            "policy_severe_loss_rate": policy.get("severe_loss_rate_le_minus_10pct"),
            "severe_loss_rate_delta": deltas.get("severe_loss_rate_le_minus_10pct"),
            "baseline_max_drawdown_proxy": baseline.get("max_drawdown_proxy"),
            "policy_max_drawdown_proxy": policy.get("max_drawdown_proxy"),
            "max_drawdown_proxy_delta": deltas.get("max_drawdown_proxy"),
        },
        "trigger_summary": trigger_summary,
        "promotion_blockers": promotion.get("blockers", []),
        "non_scope": [
            "No MeeMee reflection",
            "No runtime DB write",
            "No production ranking mutation",
            "No automatic trade instruction",
        ],
        "required_before_meemee_reflection": promotion.get("required_before_meemee_reflection", []),
    }


def _forward_gate(
    *,
    result: dict[str, Any],
    promotion: dict[str, Any],
    ret20_ready_count: int,
    input_row_count: int,
) -> dict[str, Any]:
    ret20_ready_rate = float(ret20_ready_count / input_row_count) if input_row_count else 0.0
    forward_ready = (
        result.get("decision_class") == "KEEP"
        and promotion.get("promotable_for_operational_review") is True
        and ret20_ready_count == input_row_count
        and input_row_count > 0
    )
    if forward_ready:
        gate_decision = "forward_gate_ready_for_manual_review"
        next_required_action = "manual_operator_review"
        blockers: list[str] = []
    else:
        gate_decision = "forward_gate_blocked"
        next_required_action = "rerun_same_condition_after_forward_ret20_ready"
        blockers = []
        if result.get("decision_class") != "KEEP":
            blockers.append("policy_axis_not_keep")
        if promotion.get("promotable_for_operational_review") is not True:
            blockers.append("promotion_readiness_not_true")
        if ret20_ready_count != input_row_count:
            blockers.append("forward_ret20_not_fully_mature")
        if input_row_count == 0:
            blockers.append("input_row_count_zero")
    return {
        "schema_version": f"{SCHEMA_PREFIX}_forward_gate_v1",
        "axis_id": AXIS_ID,
        "gate_decision": gate_decision,
        "next_required_action": next_required_action,
        "blockers": blockers,
        "input_row_count": int(input_row_count),
        "ret20_ready_count": int(ret20_ready_count),
        "ret20_ready_rate": ret20_ready_rate,
        "authoritative_result_decision": result.get("decision"),
        "authoritative_result_decision_class": result.get("decision_class"),
        "promotion_readiness": promotion.get("readiness_decision"),
        "required_to_advance": [
            "same_condition_forward_ret20_all_candidates_ready",
            "same_condition_forward_policy_axis_keep",
            "manual_operator_review",
        ],
        "non_scope": [
            "No MeeMee reflection",
            "No runtime DB write",
            "No production ranking mutation",
            "No automatic trade instruction",
        ],
    }


def _variant_masks(rows: pd.DataFrame) -> dict[str, pd.Series]:
    shock = rows["day5_shock_partial_reduce_trigger"].fillna(False).astype(bool)
    variants: dict[str, pd.Series] = {
        "day5_shock_all_trim50": shock,
    }
    if "variant_b_volatility_extension_clean" in rows.columns:
        variants["day5_shock_extension_clean_trim50"] = shock & _bool_series(rows, "variant_b_volatility_extension_clean")
    if "variant_c_combined_context_risk_clean" in rows.columns:
        variants["day5_shock_context_clean_trim50"] = shock & _bool_series(rows, "variant_c_combined_context_risk_clean")
    if "recent_high_distance_pct" in rows.columns:
        variants["day5_shock_below_recent_high_trim50"] = shock & (pd.to_numeric(rows["recent_high_distance_pct"], errors="coerce") < 0)
        variants["day5_shock_near_or_below_recent_high_trim50"] = shock & (
            pd.to_numeric(rows["recent_high_distance_pct"], errors="coerce") <= RECENT_HIGH_DISTANCE_MAX_FOR_KEEP
        )
    return variants


def _candidate_grid(rows: pd.DataFrame, cutoff_ymd: int | None) -> pd.DataFrame:
    date_values = pd.to_numeric(rows["policy_axis_date"], errors="coerce")
    if cutoff_ymd is None:
        unique_dates = sorted(int(value) for value in date_values.dropna().unique())
        cutoff_ymd = unique_dates[int(len(unique_dates) * 0.75) - 1] if len(unique_dates) >= 4 else None
    subset_masks = {"all": pd.Series(True, index=rows.index)}
    if cutoff_ymd is not None:
        subset_masks["train"] = date_values <= int(cutoff_ymd)
        subset_masks["holdout"] = date_values > int(cutoff_ymd)
    out_rows: list[dict[str, Any]] = []
    for variant, trigger_mask in _variant_masks(rows).items():
        candidate_return = rows["baseline_hold20_return"].where(
            ~trigger_mask,
            (1.0 - DAY5_SHOCK_TRIM_RATIO) * rows["baseline_hold20_return"] + DAY5_SHOCK_TRIM_RATIO * rows["observable_day5_return"],
        )
        candidate_rows = rows.copy()
        candidate_rows["_candidate_return"] = candidate_return
        for subset_name, subset_mask in subset_masks.items():
            subset = candidate_rows[subset_mask].copy()
            baseline = _metrics(subset, "baseline_hold20_return")
            policy = _metrics(subset, "_candidate_return")
            deltas = _metric_deltas(policy, baseline)
            trigger_count = int((trigger_mask & subset_mask).sum())
            joint_pass = bool(
                baseline["sample_count"] > 0
                and trigger_count > 0
                and (deltas.get("mean_return") or 0.0) > 0
                and deltas.get("bad_loss_rate_le_minus_5pct") is not None
                and deltas["bad_loss_rate_le_minus_5pct"] <= 0
                and deltas.get("severe_loss_rate_le_minus_10pct") is not None
                and deltas["severe_loss_rate_le_minus_10pct"] <= 0
                and deltas.get("max_drawdown_proxy") is not None
                and deltas["max_drawdown_proxy"] >= -NUMERIC_TOLERANCE
            )
            out_rows.append(
                {
                    "variant": variant,
                    "subset": subset_name,
                    "cutoff_ymd": cutoff_ymd,
                    "row_count": int(len(subset)),
                    "trigger_count": trigger_count,
                    "mean_return_delta": deltas.get("mean_return"),
                    "bad_loss_rate_delta": deltas.get("bad_loss_rate_le_minus_5pct"),
                    "severe_loss_rate_delta": deltas.get("severe_loss_rate_le_minus_10pct"),
                    "max_drawdown_proxy_delta": deltas.get("max_drawdown_proxy"),
                    "joint_return_risk_pass": joint_pass,
                }
            )
    return pd.DataFrame(out_rows)


def _judge(
    baseline: dict[str, Any],
    reduce_m: dict[str, Any],
    shock_near_high_m: dict[str, Any],
    shock_partial_m: dict[str, Any],
    extend_m: dict[str, Any],
    combined_m: dict[str, Any],
) -> tuple[str, str, list[str]]:
    reduce_mean_delta = _delta(reduce_m, baseline, "mean_return")
    reduce_bad_delta = _delta(reduce_m, baseline, "bad_loss_rate_le_minus_5pct")
    reduce_severe_delta = _delta(reduce_m, baseline, "severe_loss_rate_le_minus_10pct")
    reduce_dd_delta = _delta(reduce_m, baseline, "max_drawdown_proxy")
    shock_mean_delta = _delta(shock_partial_m, baseline, "mean_return")
    shock_bad_delta = _delta(shock_partial_m, baseline, "bad_loss_rate_le_minus_5pct")
    shock_severe_delta = _delta(shock_partial_m, baseline, "severe_loss_rate_le_minus_10pct")
    shock_dd_delta = _delta(shock_partial_m, baseline, "max_drawdown_proxy")
    near_high_mean_delta = _delta(shock_near_high_m, baseline, "mean_return")
    near_high_bad_delta = _delta(shock_near_high_m, baseline, "bad_loss_rate_le_minus_5pct")
    near_high_severe_delta = _delta(shock_near_high_m, baseline, "severe_loss_rate_le_minus_10pct")
    near_high_dd_delta = _delta(shock_near_high_m, baseline, "max_drawdown_proxy")
    extend_mean_delta = _delta(extend_m, baseline, "mean_return")
    combined_mean_delta = _delta(combined_m, baseline, "mean_return")
    combined_bad_delta = _delta(combined_m, baseline, "bad_loss_rate_le_minus_5pct")
    combined_severe_delta = _delta(combined_m, baseline, "severe_loss_rate_le_minus_10pct")

    evidence: list[str] = []
    if near_high_mean_delta is not None and near_high_bad_delta is not None and near_high_severe_delta is not None:
        if (
            near_high_mean_delta > 0
            and near_high_bad_delta <= 0
            and near_high_severe_delta < 0
            and (near_high_dd_delta is None or near_high_dd_delta >= -NUMERIC_TOLERANCE)
        ):
            evidence.append("day5_shock_near_high_partial_reduce_improves_return_and_risk")
            return "keep_day5_shock_near_high_partial_reduce_for_forward_replay", "KEEP", evidence
    if shock_mean_delta is not None and shock_bad_delta is not None and shock_severe_delta is not None:
        if shock_mean_delta > 0 and shock_bad_delta <= 0 and shock_severe_delta < 0 and (shock_dd_delta is None or shock_dd_delta >= 0):
            evidence.append("day5_shock_partial_reduce_improves_return_and_risk")
            return "keep_day5_shock_partial_reduce_for_forward_replay", "KEEP", evidence
    if reduce_mean_delta is not None and reduce_bad_delta is not None and reduce_severe_delta is not None:
        if reduce_mean_delta >= 0 and reduce_bad_delta <= 0 and reduce_severe_delta <= 0 and (reduce_dd_delta is None or reduce_dd_delta >= 0):
            evidence.append("deterioration_reduce_improves_or_preserves_return_and_risk")
            return "keep_deterioration_reduce_for_forward_replay", "KEEP", evidence
    if extend_mean_delta is not None and extend_mean_delta > 0 and (combined_mean_delta or 0) > 0:
        if (combined_bad_delta or 0) <= 0 and (combined_severe_delta or 0) <= 0:
            evidence.append("profit_extension_directional_but_requires_forward_confirmation")
            return "hold_profit_extension_needs_confirmation", "HOLD", evidence
    evidence.append("no_policy_jointly_improved_return_without_risk_worsening")
    return "drop_policy_axis_no_joint_return_risk_edge", "DROP", evidence


def run_profit_loss_policy_axis_v1(
    input_csv: str | Path,
    output_root: str | Path | None = None,
    *,
    holdout_cutoff_ymd: int | None = None,
) -> dict[str, Any]:
    input_path = Path(input_csv)
    out_dir = Path(output_root) if output_root is not None else input_path.parent / DEFAULT_OUTPUT_DIR_NAME
    out_dir.mkdir(parents=True, exist_ok=True)
    source = pd.read_csv(input_path, dtype={"code": str, "symbol": str, "ticker": str})
    missing = _required_columns(source)
    if missing:
        audit = {
            "schema_version": f"{SCHEMA_PREFIX}_no_lookahead_audit_v1",
            "audit_result": "blocked",
            "missing_required_columns": missing,
            "runtime_db_write": False,
            "meemee_reflection": False,
            "entry_selector_changed": False,
        }
        result = {
            "schema_version": f"{SCHEMA_PREFIX}_result_v1",
            "generated_at": _utc_now(),
            "axis_id": AXIS_ID,
            "decision": "blocked_missing_required_columns",
            "decision_class": "BLOCKED",
            "input_csv": str(input_path),
            "missing_required_columns": missing,
            "authoritative_artifact": str(out_dir / "policy_axis_result.json"),
        }
        _write_json(out_dir / "no_lookahead_audit.json", audit)
        _write_json(out_dir / "policy_axis_result.json", result)
        _write_json(out_dir / "_ARTIFACT_COMPLETE.json", {**result, "complete": False, "required_outputs": list(REQUIRED_OUTPUTS)})
        return result

    rows = _build_policy_rows(source)
    rows = rows.sort_values(["policy_axis_date", "policy_axis_code"], kind="stable")
    ret20_ready_count = int(rows["baseline_hold20_return"].notna().sum())
    if ret20_ready_count == 0:
        trigger_audit = _trigger_audit_rows(rows)
        audit = {
            "schema_version": f"{SCHEMA_PREFIX}_no_lookahead_audit_v1",
            "audit_result": "blocked",
            "runtime_db_write": False,
            "meemee_reflection": False,
            "entry_selector_changed": False,
            "post_entry_day5_observable_proxy_used": True,
            "ret20_used_for_evaluation_only": True,
            "ret20_ready_count": 0,
            "research_fallback_used": False,
        }
        result = {
            "schema_version": f"{SCHEMA_PREFIX}_result_v1",
            "generated_at": _utc_now(),
            "axis_id": AXIS_ID,
            "input_csv": str(input_path),
            "authoritative_artifact": str(out_dir / "policy_axis_result.json"),
            "decision": "blocked_insufficient_outcome_coverage",
            "decision_class": "BLOCKED",
            "decision_evidence": ["ret20_outcome_coverage_zero"],
            "fixed_evaluation_conditions": {
                "same_input_rows": True,
                "same_entry_selector": True,
                "same_topk": "preserved_from_input",
                "same_cost_slippage": "not_repriced_in_this_diagnostic",
                "same_artifact_detail_level": True,
                "day5_shock_threshold": DAY5_SHOCK_THRESHOLD,
                "day5_shock_trim_ratio": DAY5_SHOCK_TRIM_RATIO,
            },
            "non_scope": [
                "No MeeMee reflection",
                "No runtime DB write",
                "No production ranking mutation",
                "No entry selector change",
                "No threshold sweep",
            ],
            "outcome_coverage": {
                "input_row_count": int(len(rows)),
                "ret20_ready_count": ret20_ready_count,
                "ret20_ready_rate": 0.0,
            },
            "trigger_audit_summary": _trigger_audit_summary(trigger_audit),
        }
        rows.to_csv(out_dir / "policy_axis_rows.csv", index=False)
        pd.DataFrame(columns=["policy"]).to_csv(out_dir / "policy_axis_metrics.csv", index=False)
        _write_json(
            out_dir / "policy_axis_period_stability.json",
            {
                "schema_version": f"{SCHEMA_PREFIX}_period_stability_v1",
                "policy": "day5_shock_partial_reduce",
                "period_column": _period_col(rows),
                "stable_enough_for_forward_replay": False,
                "blocked_reason": "ret20_outcome_coverage_zero",
            },
        )
        _write_json(
            out_dir / "policy_axis_holdout_validation.json",
            {
                "schema_version": f"{SCHEMA_PREFIX}_holdout_validation_v1",
                "enabled": False,
                "reason": "ret20_outcome_coverage_zero",
            },
        )
        _write_json(
            out_dir / "policy_axis_cutoff_robustness.json",
            {
                "schema_version": f"{SCHEMA_PREFIX}_cutoff_robustness_v1",
                "policy": "day5_shock_near_or_below_recent_high_trim50",
                "robustness_decision": "blocked_insufficient_outcome_coverage",
                "rows": [],
            },
        )
        _write_json(
            out_dir / "policy_axis_promotion_readiness.json",
            _promotion_readiness(
                decision="blocked_insufficient_outcome_coverage",
                decision_class="BLOCKED",
                robustness={"robustness_decision": "blocked_insufficient_outcome_coverage"},
                ret20_ready_count=ret20_ready_count,
                input_row_count=int(len(rows)),
            ),
        )
        blocked_promotion = _promotion_readiness(
            decision="blocked_insufficient_outcome_coverage",
            decision_class="BLOCKED",
            robustness={"robustness_decision": "blocked_insufficient_outcome_coverage"},
            ret20_ready_count=ret20_ready_count,
            input_row_count=int(len(rows)),
        )
        _write_json(
            out_dir / "policy_axis_operator_review_pack.json",
            _operator_review_pack(
                result=result,
                promotion=blocked_promotion,
                robustness={"robustness_decision": "blocked_insufficient_outcome_coverage"},
                trigger_summary=_trigger_audit_summary(trigger_audit),
                metrics={},
            ),
        )
        _write_json(
            out_dir / "policy_axis_forward_gate.json",
            _forward_gate(
                result=result,
                promotion=blocked_promotion,
                ret20_ready_count=ret20_ready_count,
                input_row_count=int(len(rows)),
            ),
        )
        pd.DataFrame(columns=["variant", "subset"]).to_csv(out_dir / "policy_axis_candidate_grid.csv", index=False)
        trigger_audit.to_csv(out_dir / "policy_axis_trigger_audit.csv", index=False)
        _write_json(out_dir / "no_lookahead_audit.json", audit)
        _write_json(out_dir / "policy_axis_result.json", result)
        _write_json(
            out_dir / "_ARTIFACT_COMPLETE.json",
            {
                "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
                "complete": False,
                "axis_id": AXIS_ID,
                "authoritative_artifact": str(out_dir / "policy_axis_result.json"),
                "required_outputs": list(REQUIRED_OUTPUTS),
                "artifact_existence_check": {name: (out_dir / name).exists() for name in REQUIRED_OUTPUTS if name != "_ARTIFACT_COMPLETE.json"},
                "decision": "blocked_insufficient_outcome_coverage",
            },
        )
        return result
    metrics = {
        "baseline_hold20": _metrics(rows, "baseline_hold20_return"),
        "deterioration_reduce": _metrics(rows, "deterioration_reduce_return"),
        "day5_shock_near_high_partial_reduce": _metrics(rows, "day5_shock_near_high_partial_reduce_return"),
        "day5_shock_partial_reduce": _metrics(rows, "day5_shock_partial_reduce_return"),
        "profit_extension": _metrics(rows, "profit_extension_return"),
        "combined_reduce_then_extend": _metrics(rows, "combined_reduce_then_extend_return"),
    }
    deltas = {
        policy: _metric_deltas(value, metrics["baseline_hold20"])
        for policy, value in metrics.items()
        if policy != "baseline_hold20"
    }
    stability = _period_stability(rows, "day5_shock_partial_reduce_return")
    trigger_audit = _trigger_audit_rows(rows)
    trigger_summary = _trigger_audit_summary(trigger_audit)
    holdout = _holdout_validation(rows, holdout_cutoff_ymd)
    candidate_grid = _candidate_grid(rows, holdout_cutoff_ymd)
    robustness = _cutoff_robustness(rows)
    grid_pass = candidate_grid[candidate_grid["joint_return_risk_pass"].fillna(False).astype(bool)].copy()
    decision, decision_class, evidence = _judge(
        metrics["baseline_hold20"],
        metrics["deterioration_reduce"],
        metrics["day5_shock_near_high_partial_reduce"],
        metrics["day5_shock_partial_reduce"],
        metrics["profit_extension"],
        metrics["combined_reduce_then_extend"],
    )
    promotion = _promotion_readiness(
        decision=decision,
        decision_class=decision_class,
        robustness=robustness,
        ret20_ready_count=ret20_ready_count,
        input_row_count=int(len(rows)),
    )
    audit = {
        "schema_version": f"{SCHEMA_PREFIX}_no_lookahead_audit_v1",
        "audit_result": "pass",
        "runtime_db_write": False,
        "meemee_reflection": False,
        "entry_selector_changed": False,
        "post_entry_day5_observable_proxy_used": True,
        "ret20_used_for_evaluation_only": True,
        "research_fallback_used": False,
    }
    result = {
        "schema_version": f"{SCHEMA_PREFIX}_result_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "input_csv": str(input_path),
        "authoritative_artifact": str(out_dir / "policy_axis_result.json"),
        "decision": decision,
        "decision_class": decision_class,
        "decision_evidence": evidence,
        "fixed_evaluation_conditions": {
            "same_input_rows": True,
            "same_entry_selector": True,
            "same_topk": "preserved_from_input",
            "same_cost_slippage": "not_repriced_in_this_diagnostic",
            "same_artifact_detail_level": True,
            "day5_shock_threshold": DAY5_SHOCK_THRESHOLD,
            "day5_shock_trim_ratio": DAY5_SHOCK_TRIM_RATIO,
            "recent_high_distance_max_for_keep": RECENT_HIGH_DISTANCE_MAX_FOR_KEEP,
        },
        "non_scope": [
            "No MeeMee reflection",
            "No runtime DB write",
            "No production ranking mutation",
            "No entry selector change",
            "No threshold sweep",
        ],
        "metrics": metrics,
        "deltas_vs_baseline": deltas,
        "period_stability_summary": {
            key: stability[key]
            for key in (
                "policy",
                "period_column",
                "evaluated_period_count",
                "trigger_period_count",
                "improved_period_count",
                "severe_not_worse_period_count",
                "dd_not_worse_period_count",
                "stable_enough_for_forward_replay",
            )
        },
        "trigger_audit_summary": trigger_summary,
        "holdout_validation_summary": {
            "enabled": holdout.get("enabled"),
            "cutoff_ymd": holdout.get("cutoff_ymd"),
            "holdout_decision": holdout.get("holdout_decision"),
            "holdout_joint_return_risk_pass": (holdout.get("holdout") or {}).get("joint_return_risk_pass")
            if holdout.get("enabled")
            else None,
            "holdout_row_count": (holdout.get("holdout") or {}).get("row_count") if holdout.get("enabled") else None,
            "holdout_trigger_count": (holdout.get("holdout") or {}).get("trigger_count") if holdout.get("enabled") else None,
        },
        "candidate_grid_summary": {
            "variant_count": int(candidate_grid["variant"].nunique()) if not candidate_grid.empty else 0,
            "row_count": int(len(candidate_grid)),
            "joint_pass_rows": int(len(grid_pass)),
            "variants_with_holdout_pass": sorted(
                grid_pass.loc[grid_pass["subset"].eq("holdout"), "variant"].astype(str).unique().tolist()
            )
            if not grid_pass.empty
            else [],
        },
        "cutoff_robustness_summary": {
            "policy": robustness.get("policy"),
            "cutoff_count": robustness.get("cutoff_count"),
            "valid_holdout_cutoff_count": robustness.get("valid_holdout_cutoff_count"),
            "pass_holdout_cutoff_count": robustness.get("pass_holdout_cutoff_count"),
            "robustness_decision": robustness.get("robustness_decision"),
        },
        "promotion_readiness_summary": {
            "readiness_decision": promotion.get("readiness_decision"),
            "promotable_for_operational_review": promotion.get("promotable_for_operational_review"),
            "blockers": promotion.get("blockers"),
        },
        "observed_branching": {
            "changed_top5_members_count": 0,
            "changed_top10_members_count": 0,
            "changed_rank_count": 0,
            "selection_divergence_reason": "post_entry_policy_only_no_candidate_membership_change",
        },
    }
    operator_review_pack = _operator_review_pack(
        result=result,
        promotion=promotion,
        robustness=robustness,
        trigger_summary=trigger_summary,
        metrics=metrics,
    )
    rows.to_csv(out_dir / "policy_axis_rows.csv", index=False)
    pd.DataFrame(
        [
            {"policy": policy, **payload, **{f"delta_{k}": v for k, v in deltas.get(policy, {}).items()}}
            for policy, payload in metrics.items()
        ]
    ).to_csv(out_dir / "policy_axis_metrics.csv", index=False)
    _write_json(out_dir / "policy_axis_period_stability.json", stability)
    _write_json(out_dir / "policy_axis_holdout_validation.json", holdout)
    _write_json(out_dir / "policy_axis_cutoff_robustness.json", robustness)
    _write_json(out_dir / "policy_axis_promotion_readiness.json", promotion)
    _write_json(out_dir / "policy_axis_operator_review_pack.json", operator_review_pack)
    _write_json(
        out_dir / "policy_axis_forward_gate.json",
        _forward_gate(
            result=result,
            promotion=promotion,
            ret20_ready_count=ret20_ready_count,
            input_row_count=int(len(rows)),
        ),
    )
    candidate_grid.to_csv(out_dir / "policy_axis_candidate_grid.csv", index=False)
    trigger_audit.to_csv(out_dir / "policy_axis_trigger_audit.csv", index=False)
    _write_json(out_dir / "no_lookahead_audit.json", audit)
    _write_json(out_dir / "policy_axis_result.json", result)
    _write_json(
        out_dir / "_ARTIFACT_COMPLETE.json",
        {
            "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
            "complete": True,
            "axis_id": AXIS_ID,
            "authoritative_artifact": str(out_dir / "policy_axis_result.json"),
            "required_outputs": list(REQUIRED_OUTPUTS),
            "artifact_existence_check": {name: (out_dir / name).exists() for name in REQUIRED_OUTPUTS if name != "_ARTIFACT_COMPLETE.json"},
            "decision": decision,
        },
    )
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate one TRADEX post-entry profit/loss policy axis from replay rows.")
    parser.add_argument("input_csv", help="CSV with fixed replay rows. Requires ret5, ret20, date, and code columns.")
    parser.add_argument("--output-root", default=None)
    parser.add_argument("--holdout-cutoff-ymd", type=int, default=None)
    args = parser.parse_args()
    print(
        json.dumps(
            _json_ready(run_profit_loss_policy_axis_v1(args.input_csv, args.output_root, holdout_cutoff_ymd=args.holdout_cutoff_ymd)),
            ensure_ascii=False,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
