from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))


AXIS_ID = "candidate_lifecycle_audit_v1"
SCHEMA_PREFIX = "tradex_candidate_lifecycle_audit_v1"
DEFAULT_OUTPUT_DIR_NAME = "candidate_lifecycle_audit_v1"

ROOT_SOURCE_ARTIFACTS = (
    "daily_action_ledger.jsonl",
    "orders_ledger.csv",
    "positions_ledger.csv",
    "equity_curve.csv",
    "daily_candidate_snapshot.csv",
    "post_run_outcome_labels.csv",
    "bought_weak_candidate_cases.csv",
    "trade_contribution.csv",
)
REFERENCE_SOURCE_ARTIFACTS = (
    "bought_weak_candidate_decomposition_v1/weak_buy_cases_enriched.csv",
)

OUTPUT_ARTIFACTS = (
    "candidate_lifecycle_summary.json",
    "bought_candidate_lifecycle.csv",
    "invalidation_candle_cases.csv",
    "buy_to_escape_transition_cases.csv",
    "false_invalidation_recovery_cases.csv",
    "avoidable_loss_by_invalidation_exit.csv",
    "lifecycle_next_axis_decision.json",
    "_ARTIFACT_COMPLETE.json",
)

FEATURE_COLUMNS = (
    "daily_ma_stack",
    "daily_ma60_slope_state",
    "daily_ret20_state",
    "daily_candle_state",
    "daily_volume_state",
    "daily_sequence_state",
    "weekly_trend_state",
    "weekly_ret4_state",
    "monthly_trend_state",
    "monthly_ret6_state",
)

NEXT_AXIS_CANDIDATES = (
    "invalidation_exit_pretest",
    "stop_too_wide_pretest",
    "severe_loss_veto_pretest",
    "position_sizing_haircut_pretest",
    "abandon_bad_buy_axis",
)

RANK_DETERIORATION_DELTA = 20
RANK_DETERIORATION_ABSOLUTE = 50
SCORE_DETERIORATION_DELTA = 3
BIG_DOWN_DAY = -0.05
GAP_DOWN_PROXY = -0.04
PRIOR_LOW_BREAK_MARGIN = -0.02
FALSE_RECOVERY_RETURN = 0.0


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
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


def _write_csv(path: Path, rows: Iterable[dict[str, Any]] | pd.DataFrame, columns: list[str] | None = None) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = rows if isinstance(rows, pd.DataFrame) else pd.DataFrame(list(rows))
    if columns is not None:
        if frame.empty:
            frame = pd.DataFrame(columns=columns)
        else:
            for column in columns:
                if column not in frame.columns:
                    frame[column] = None
    frame.to_csv(path, index=False)
    return path


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _load_frame(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"missing required artifact: {path}")
    return pd.read_csv(path)


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _source_status(run_root: Path) -> dict[str, bool]:
    status = {name: (run_root / name).exists() for name in ROOT_SOURCE_ARTIFACTS}
    status.update({name: (run_root / name).exists() for name in REFERENCE_SOURCE_ARTIFACTS})
    return status


def _parse_components(raw: Any) -> dict[str, Any]:
    if raw is None or pd.isna(raw):
        return {}
    try:
        items = json.loads(str(raw))
    except json.JSONDecodeError:
        return {}
    out: dict[str, Any] = {}
    for item in items if isinstance(items, list) else []:
        if not isinstance(item, dict):
            continue
        feature = str(item.get("feature") or "").strip()
        if feature:
            out[feature] = item.get("value")
            out[f"{feature}_points"] = item.get("points")
    return out


def _add_features(frame: pd.DataFrame) -> pd.DataFrame:
    work = frame.copy()
    parsed = [_parse_components(value) for value in work.get("score_components_json", pd.Series(dtype=object))]
    for column in FEATURE_COLUMNS:
        if column not in work.columns:
            work[column] = [item.get(column) for item in parsed]
    if "reason_codes" not in work.columns:
        work["reason_codes"] = [
            "|".join(f"{column}={item.get(column)}" for column in FEATURE_COLUMNS if item.get(column) is not None)
            for item in parsed
        ]
    return work


def _actual_position_contributions(orders: pd.DataFrame, positions: pd.DataFrame) -> dict[str, dict[str, Any]]:
    buys = orders[(orders["action"] == "buy") & (orders["order_status"] == "filled")].copy()
    exits = orders[(orders["action"].isin(["exit", "stop"])) & (orders["order_status"] == "filled")].copy()
    latest = positions.sort_values("ymd", kind="stable").groupby("position_id", sort=False).tail(1) if not positions.empty else pd.DataFrame()
    out: dict[str, dict[str, Any]] = {}
    for _idx, buy in buys.iterrows():
        pid = str(buy["position_id"])
        exit_rows = exits[exits["position_id"].astype(str) == pid]
        if not exit_rows.empty:
            exit_row = exit_rows.iloc[-1]
            actual_pnl = _safe_float(exit_row.get("realized_pnl")) or 0.0
            exit_ymd = int(exit_row["execution_ymd"])
            exit_decision_ymd = int(exit_row["decision_ymd"])
            exit_reason = exit_row.get("reason_type")
        else:
            pos = latest[latest["position_id"].astype(str) == pid]
            actual_pnl = (_safe_float(pos.iloc[-1].get("unrealized_pnl")) or 0.0) if not pos.empty else 0.0
            exit_ymd = None
            exit_decision_ymd = None
            exit_reason = "open_at_run_end"
        out[pid] = {
            "position_id": pid,
            "entry_decision_ymd": int(buy["decision_ymd"]),
            "entry_ymd": int(buy["execution_ymd"]),
            "entry_price": _safe_float(buy.get("execution_price")),
            "shares": int(_safe_float(buy.get("shares")) or 0),
            "cost_basis": (_safe_float(buy.get("notional")) or 0.0) + (_safe_float(buy.get("cost_amount")) or 0.0),
            "actual_pnl": actual_pnl,
            "exit_ymd": exit_ymd,
            "exit_decision_ymd": exit_decision_ymd,
            "exit_reason": exit_reason,
        }
    return out


def _candidate_by_code_date(candidates: pd.DataFrame) -> dict[tuple[str, int], pd.Series]:
    return {
        (str(row["code"]), int(row["decision_ymd"])): row
        for _idx, row in candidates.iterrows()
    }


def _classify_invalidation(
    entry: pd.Series,
    day: pd.Series,
    prev_lifecycle: pd.Series | None,
    prev_min_return: float | None,
) -> tuple[str | None, str | None]:
    if bool(day.get("candidate_missing_from_top100")):
        return "rank_deterioration", "held_code_absent_from_candidate_top100"
    day_return = _safe_float(day.get("daily_close_return_proxy"))
    position_return = _safe_float(day.get("position_return"))
    prev_min = prev_min_return if prev_min_return is not None else position_return
    candle_state = str(day.get("daily_candle_state") or "")
    volume_state = str(day.get("daily_volume_state") or "")
    daily_ma_stack = str(day.get("daily_ma_stack") or "")
    sequence_state = str(day.get("daily_sequence_state") or "")
    rank = _safe_float(day.get("candidate_rank"))
    entry_rank = _safe_float(entry.get("candidate_rank"))
    score = _safe_float(day.get("selection_score"))
    entry_score = _safe_float(entry.get("selection_score"))

    if candle_state == "daily_strong_bear" or (day_return is not None and day_return <= BIG_DOWN_DAY):
        return "big_bearish_candle", "daily_strong_bear_or_close_down_5pct_proxy"
    if day_return is not None and day_return <= GAP_DOWN_PROXY:
        return "gap_down", "close_to_close_gap_down_proxy_no_open_available"
    if daily_ma_stack == "daily_pullback_20_over_60":
        return "ma20_break", "daily_ma_stack_pullback_20_over_60_proxy"
    if daily_ma_stack == "daily_near_bull_5_over_20_under_60":
        return "ma7_break", "daily_ma_stack_near_bull_proxy"
    if position_return is not None and prev_min is not None and position_return <= prev_min + PRIOR_LOW_BREAK_MARGIN:
        return "prior_low_break", "position_return_new_trough_proxy"
    if candle_state == "daily_upper_wick_warning" and prev_lifecycle is not None and str(prev_lifecycle.get("daily_candle_state") or "").endswith("bull"):
        return "bearish_engulfing", "upper_wick_after_bullish_candle_proxy"
    if volume_state == "daily_volume_expansion" and (
        candle_state == "daily_strong_bear" or sequence_state == "daily_sequence_warning" or (day_return is not None and day_return < -0.03)
    ):
        return "volume_down_break", "volume_expansion_with_downside_proxy"
    if rank is not None and entry_rank is not None and (rank - entry_rank >= RANK_DETERIORATION_DELTA or rank >= RANK_DETERIORATION_ABSOLUTE):
        return "rank_deterioration", "candidate_rank_worsened_or_fell_below_top50"
    if score is not None and entry_score is not None and score <= entry_score - SCORE_DETERIORATION_DELTA:
        return "score_deterioration", "selection_score_drop_3_or_more"
    return None, None


def _estimate_exit_at_next_close(history: pd.DataFrame, invalidation_ymd: int, shares: int, cost_basis: float) -> dict[str, Any]:
    future = history[pd.to_numeric(history["ymd"], errors="coerce").astype(int) > int(invalidation_ymd)].copy()
    if future.empty:
        row = history[history["ymd"].astype(int) == int(invalidation_ymd)]
        if row.empty:
            return {"estimate_status": "unavailable_no_holding_row_after_invalidation", "estimated_exit_ymd": None, "estimated_exit_pnl": None}
        exit_row = row.iloc[-1]
        status = "same_day_close_proxy_no_next_holding_row"
    else:
        exit_row = future.iloc[0]
        status = "next_holding_close_proxy_not_next_open"
    close_price = _safe_float(exit_row.get("close_price"))
    if close_price is None:
        return {"estimate_status": "unavailable_no_close_price", "estimated_exit_ymd": None, "estimated_exit_pnl": None}
    estimated_value = close_price * shares
    estimated_cost = abs(estimated_value) * 30.0 / 10_000.0
    return {
        "estimate_status": status,
        "estimated_exit_ymd": int(exit_row["ymd"]),
        "estimated_exit_price": close_price,
        "estimated_exit_pnl": estimated_value - cost_basis - estimated_cost,
    }


def build_lifecycle_audit(run_root: Path) -> dict[str, Any]:
    actions = pd.DataFrame(_read_jsonl(run_root / "daily_action_ledger.jsonl"))
    orders = _load_frame(run_root / "orders_ledger.csv")
    positions = _load_frame(run_root / "positions_ledger.csv")
    equity = _load_frame(run_root / "equity_curve.csv")
    candidates = _add_features(_load_frame(run_root / "daily_candidate_snapshot.csv"))
    labels = _load_frame(run_root / "post_run_outcome_labels.csv")
    weak_cases = _add_features(_load_frame(run_root / "bought_weak_candidate_cases.csv"))
    weak_enriched = _add_features(_load_frame(run_root / "bought_weak_candidate_decomposition_v1" / "weak_buy_cases_enriched.csv"))
    trade_contribution = _load_frame(run_root / "trade_contribution.csv")

    candidate_lookup = _candidate_by_code_date(candidates)
    contributions = _actual_position_contributions(orders, positions)
    lifecycle_rows: list[dict[str, Any]] = []
    invalidation_rows: list[dict[str, Any]] = []
    escape_rows: list[dict[str, Any]] = []
    recovery_rows: list[dict[str, Any]] = []
    avoidable_rows: list[dict[str, Any]] = []

    for _idx, weak in weak_enriched.iterrows():
        pid = str(weak.get("position_id"))
        if not pid or pid == "nan":
            continue
        meta = contributions.get(pid, {})
        history = positions[positions["position_id"].astype(str) == pid].copy()
        if history.empty:
            continue
        history = history.sort_values("ymd", kind="stable").reset_index(drop=True)
        history["position_return"] = pd.to_numeric(history["unrealized_pnl"], errors="coerce") / pd.to_numeric(history["cost_basis"], errors="coerce")
        code = str(weak["code"])
        entry_rank = _safe_float(weak.get("candidate_rank"))
        entry_score = _safe_float(weak.get("selection_score"))
        entry_ymd = int(meta.get("entry_ymd") or weak.get("entry_execution_ymd") or history.iloc[0]["entry_ymd"])
        entry_decision_ymd = int(weak.get("decision_ymd"))
        entry_price = _safe_float(meta.get("entry_price")) or _safe_float(weak.get("execution_price")) or _safe_float(history.iloc[0].get("entry_price"))
        shares = int(meta.get("shares") or _safe_float(weak.get("shares")) or _safe_float(history.iloc[0].get("shares")) or 0)
        cost_basis = float(meta.get("cost_basis") or _safe_float(history.iloc[0].get("cost_basis")) or 0.0)
        actual_pnl = float(meta.get("actual_pnl") or 0.0)
        first_invalidation: dict[str, Any] | None = None
        daily_states: list[dict[str, Any]] = []
        prev_row: pd.Series | None = None
        prev_close: float | None = None
        prev_min_return: float | None = None

        for hidx, hrow in history.iterrows():
            ymd = int(hrow["ymd"])
            candidate = candidate_lookup.get((code, ymd))
            candidate_missing = candidate is None
            row: dict[str, Any] = {
                "ymd": ymd,
                "holding_day_index": int(hidx + 1),
                "position_return": _safe_float(hrow.get("position_return")),
                "close_price": _safe_float(hrow.get("close_price")),
                "candidate_missing_from_top100": candidate_missing,
            }
            if candidate is not None:
                for column in ["candidate_rank", "selection_score", "close", *FEATURE_COLUMNS]:
                    row[column] = candidate.get(column)
            else:
                row["candidate_rank"] = None
                row["selection_score"] = None
            close_price = _safe_float(row.get("close_price"))
            row["daily_close_return_proxy"] = None if prev_close in {None, 0.0} or close_price is None else close_price / prev_close - 1.0
            row_series = pd.Series(row)
            invalidation_type, evidence = _classify_invalidation(weak, row_series, prev_row, prev_min_return)
            row["invalidation_type"] = invalidation_type
            row["invalidation_evidence"] = evidence
            daily_states.append(row)
            if invalidation_type and first_invalidation is None:
                first_invalidation = row
            if close_price is not None:
                prev_close = close_price
            position_return = _safe_float(row.get("position_return"))
            if position_return is not None:
                prev_min_return = position_return if prev_min_return is None else min(prev_min_return, position_return)
            prev_row = row_series

        estimate = {"estimate_status": None, "estimated_exit_ymd": None, "estimated_exit_pnl": None}
        avoidable_loss = 0.0
        one_candle_transition = False
        false_recovery = False
        first_type = None
        first_ymd = None
        first_holding_day = None
        if first_invalidation is not None:
            first_type = first_invalidation.get("invalidation_type")
            first_ymd = int(first_invalidation["ymd"])
            first_holding_day = int(first_invalidation["holding_day_index"])
            estimate = _estimate_exit_at_next_close(history, first_ymd, shares, cost_basis)
            estimated_exit_pnl = _safe_float(estimate.get("estimated_exit_pnl"))
            avoidable_loss = max(0.0, estimated_exit_pnl - actual_pnl) if estimated_exit_pnl is not None else 0.0
            candle_like = first_type in {"big_bearish_candle", "gap_down", "bearish_engulfing", "volume_down_break", "prior_low_break"}
            one_candle_transition = bool(candle_like and first_holding_day is not None and first_holding_day >= 1)
            after = history[pd.to_numeric(history["ymd"], errors="coerce").astype(int) >= first_ymd].head(21)
            max_after = _safe_float(after["position_return"].max()) if not after.empty else None
            false_recovery = bool(max_after is not None and max_after >= FALSE_RECOVERY_RETURN)
            invalidation_row = {
                "position_id": pid,
                "code": code,
                "entry_decision_ymd": entry_decision_ymd,
                "entry_ymd": entry_ymd,
                "invalidation_date": first_ymd,
                "holding_day_index": first_holding_day,
                "invalidation_type": first_type,
                "invalidation_evidence": first_invalidation.get("invalidation_evidence"),
                "candidate_rank": first_invalidation.get("candidate_rank"),
                "selection_score": first_invalidation.get("selection_score"),
                "daily_candle_state": first_invalidation.get("daily_candle_state"),
                "daily_ma_stack": first_invalidation.get("daily_ma_stack"),
                "daily_volume_state": first_invalidation.get("daily_volume_state"),
                "position_return": first_invalidation.get("position_return"),
                "daily_close_return_proxy": first_invalidation.get("daily_close_return_proxy"),
                "one_candle_transition": one_candle_transition,
                **estimate,
                "actual_exit_ymd": meta.get("exit_ymd"),
                "actual_pnl": actual_pnl,
                "avoidable_loss_estimate": avoidable_loss,
                "estimate_method": "next_holding_close_proxy_not_next_open",
            }
            invalidation_rows.append(invalidation_row)
            if one_candle_transition:
                escape_rows.append(invalidation_row)
            if false_recovery:
                recovery_rows.append(
                    {
                        **invalidation_row,
                        "max_return_within_20_holding_rows_after_invalidation": max_after,
                        "false_invalidation_recovery": True,
                    }
                )
            if avoidable_loss > 0:
                avoidable_rows.append(invalidation_row)

        lifecycle_bucket = "weak_vs_alternative_no_invalidation"
        if bool(weak.get("weakness_visible_at_decision_time")) or str(weak.get("entry_timing_bucket")) == "immediate_adverse_within_5d":
            lifecycle_bucket = "weak_at_entry"
        if first_invalidation is not None and lifecycle_bucket != "weak_at_entry":
            lifecycle_bucket = "post_entry_invalidation"
        if one_candle_transition:
            lifecycle_bucket = "one_candle_buy_to_escape_transition"
        lifecycle_rows.append(
            {
                "position_id": pid,
                "order_id": weak.get("order_id"),
                "code": code,
                "entry_decision_ymd": entry_decision_ymd,
                "entry_ymd": entry_ymd,
                "entry_rank": entry_rank,
                "entry_score": entry_score,
                "buy_reason_codes": weak.get("reason_codes"),
                "entry_daily_ma_stack": weak.get("daily_ma_stack"),
                "entry_daily_ma60_slope_state": weak.get("daily_ma60_slope_state"),
                "entry_daily_ret20_state": weak.get("daily_ret20_state"),
                "entry_daily_candle_state": weak.get("daily_candle_state"),
                "entry_daily_volume_state": weak.get("daily_volume_state"),
                "entry_daily_sequence_state": weak.get("daily_sequence_state"),
                "entry_weekly_trend_state": weak.get("weekly_trend_state"),
                "entry_weekly_ret4_state": weak.get("weekly_ret4_state"),
                "entry_monthly_trend_state": weak.get("monthly_trend_state"),
                "entry_monthly_ret6_state": weak.get("monthly_ret6_state"),
                "post_ret_20": weak.get("post_ret_20"),
                "mae_20": weak.get("mae_20"),
                "mfe_20": weak.get("mfe_20"),
                "entry_timing_bucket": weak.get("entry_timing_bucket"),
                "weakness_visible_at_decision_time": weak.get("weakness_visible_at_decision_time"),
                "first_invalidation_date": first_ymd,
                "first_invalidation_holding_day": first_holding_day,
                "first_invalidation_type": first_type,
                "one_candle_transition": one_candle_transition,
                "false_invalidation_recovery": false_recovery,
                "avoidable_loss_estimate": avoidable_loss,
                "actual_exit_ymd": meta.get("exit_ymd"),
                "actual_exit_reason": meta.get("exit_reason"),
                "actual_pnl": actual_pnl,
                "lifecycle_bucket": lifecycle_bucket,
                "observed_candidate_days_while_held": sum(1 for item in daily_states if not item.get("candidate_missing_from_top100")),
                "missing_candidate_days_while_held": sum(1 for item in daily_states if item.get("candidate_missing_from_top100")),
            }
        )

    lifecycle = pd.DataFrame(lifecycle_rows)
    invalidations = pd.DataFrame(invalidation_rows)
    escapes = pd.DataFrame(escape_rows)
    recoveries = pd.DataFrame(recovery_rows)
    avoidable = pd.DataFrame(avoidable_rows)
    return {
        "frames": {
            "lifecycle": lifecycle,
            "invalidations": invalidations,
            "escapes": escapes,
            "recoveries": recoveries,
            "avoidable": avoidable,
        },
        "inputs": {
            "actions_rows": int(len(actions)),
            "orders_rows": int(len(orders)),
            "positions_rows": int(len(positions)),
            "equity_rows": int(len(equity)),
            "candidate_rows": int(len(candidates)),
            "label_rows": int(len(labels)),
            "weak_case_rows": int(len(weak_cases)),
            "weak_enriched_rows": int(len(weak_enriched)),
            "trade_contribution_rows": int(len(trade_contribution)),
        },
    }


def _decision(metrics: dict[str, Any]) -> tuple[str, str]:
    invalidation_count = int(metrics.get("invalidation_cases", 0))
    one_candle_count = int(metrics.get("one_candle_transition_cases", 0))
    avoidable_loss = float(metrics.get("avoidable_loss_total", 0.0))
    false_count = int(metrics.get("false_invalidation_recovery_cases", 0))
    weak_at_entry = int(metrics.get("lifecycle_bucket_counts", {}).get("weak_at_entry", 0))
    if one_candle_count >= 5 and avoidable_loss > 500_000 and false_count <= one_candle_count:
        return "invalidation_exit_pretest", "one_candle_invalidation_has_material_avoidable_loss"
    if avoidable_loss > 1_000_000 and invalidation_count > 0:
        return "stop_too_wide_pretest", "invalidation_loss_material_but_not_clean_one_candle"
    if weak_at_entry >= max(5, invalidation_count):
        return "severe_loss_veto_pretest", "weakness_concentrated_at_entry"
    if invalidation_count > 0 and false_count > one_candle_count:
        return "position_sizing_haircut_pretest", "invalidation_signal_has_recovery_risk"
    return "abandon_bad_buy_axis", "no_clean_lifecycle_axis_from_artifacts"


def run_candidate_lifecycle_audit_v1(run_root: str | Path, output_root: str | Path | None = None) -> dict[str, Any]:
    run_root = Path(run_root)
    output_root = Path(output_root) if output_root is not None else run_root / DEFAULT_OUTPUT_DIR_NAME
    source_status = _source_status(run_root)
    missing = [name for name, exists in source_status.items() if not exists]
    if missing:
        raise FileNotFoundError(f"missing required source artifacts: {missing}")

    built = build_lifecycle_audit(run_root)
    lifecycle = built["frames"]["lifecycle"]
    invalidations = built["frames"]["invalidations"]
    escapes = built["frames"]["escapes"]
    recoveries = built["frames"]["recoveries"]
    avoidable = built["frames"]["avoidable"]
    metrics = {
        "bought_weak_candidate_lifecycle_rows": int(len(lifecycle)),
        "invalidation_cases": int(len(invalidations)),
        "one_candle_transition_cases": int(len(escapes)),
        "false_invalidation_recovery_cases": int(len(recoveries)),
        "avoidable_loss_cases": int(len(avoidable)),
        "avoidable_loss_total": float(pd.to_numeric(avoidable.get("avoidable_loss_estimate", pd.Series(dtype=float)), errors="coerce").sum()) if not avoidable.empty else 0.0,
        "lifecycle_bucket_counts": lifecycle["lifecycle_bucket"].value_counts().to_dict() if not lifecycle.empty else {},
        "invalidation_type_counts": invalidations["invalidation_type"].value_counts().to_dict() if not invalidations.empty else {},
    }
    selected_axis, reason_type = _decision(metrics)

    _write_csv(output_root / "bought_candidate_lifecycle.csv", lifecycle)
    _write_csv(output_root / "invalidation_candle_cases.csv", invalidations)
    _write_csv(output_root / "buy_to_escape_transition_cases.csv", escapes)
    _write_csv(output_root / "false_invalidation_recovery_cases.csv", recoveries)
    _write_csv(output_root / "avoidable_loss_by_invalidation_exit.csv", avoidable)
    decision_payload = {
        "schema_version": f"{SCHEMA_PREFIX}_lifecycle_next_axis_decision_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "decision_candidates": list(NEXT_AXIS_CANDIDATES),
        "selected_next_axis": selected_axis,
        "selected_next_axis_count": 1,
        "reason_type": reason_type,
        "metrics": metrics,
        "policy": {
            "single_axis_only": True,
            "replay_rerun": False,
            "rule_changed": False,
            "daily_volume_normal_veto_revived": False,
            "post_run_outcomes_used_for_trade_decision": False,
            "derived_artifacts_only": True,
        },
    }
    _write_json(output_root / "lifecycle_next_axis_decision.json", decision_payload)
    summary = {
        "schema_version": f"{SCHEMA_PREFIX}_summary_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "run_root": str(run_root),
        "source_artifacts": source_status,
        "input_rows": built["inputs"],
        "metrics": metrics,
        "selected_next_axis": selected_axis,
        "decision_reason_type": reason_type,
        "scope": {
            "tradex_only": True,
            "derived_artifacts_only": True,
            "replay_rerun": False,
            "rule_changed": False,
            "meemee_ui_changed": False,
            "runtime_db_written": False,
            "ranking_changed": False,
            "publish_registry_changed": False,
        },
        "observability_limits": {
            "full_ohlc_available": False,
            "exact_next_open_exit_available": False,
            "ma7_ma20_exact_values_available": False,
            "candidate_lifecycle_observed_only_when_symbol_in_top100": True,
            "exit_estimate_method": "next_holding_close_proxy_not_next_open",
            "silent_fallback_used": False,
        },
    }
    _write_json(output_root / "candidate_lifecycle_summary.json", summary)
    complete = {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "run_root": str(run_root),
        "output_root": str(output_root),
        "complete": True,
        "required_artifacts_all_present": all((output_root / name).exists() for name in OUTPUT_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json"),
        "source_artifacts_all_present": all(source_status.values()),
        "selected_next_axis": selected_axis,
        "selected_next_axis_count": 1,
        "replay_rerun": False,
        "rule_changed": False,
        "conditions_changed": False,
        "daily_volume_normal_veto_revived": False,
        "post_run_outcomes_used_for_trade_decision": False,
        "silent_fallback_used": False,
        "research_fallback_recorded": True,
        "artifact_logs_non_empty": {
            "bought_candidate_lifecycle": len(lifecycle) > 0,
            "invalidation_candle_cases": len(invalidations) > 0,
            "buy_to_escape_transition_cases": len(escapes) > 0,
            "false_invalidation_recovery_cases": len(recoveries) > 0,
            "avoidable_loss_by_invalidation_exit": len(avoidable) > 0,
        },
    }
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    return {"complete": True, "output_root": str(output_root), "selected_next_axis": selected_axis, "metrics": metrics}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit bought weak candidate lifecycle invalidation cases.")
    parser.add_argument("--run-root", required=True, type=Path)
    parser.add_argument("--output-root", type=Path, default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    print(_json_text(run_candidate_lifecycle_audit_v1(args.run_root, args.output_root)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
