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


AXIS_ID = "trade_reflection_audit_v1"
SCHEMA_PREFIX = "tradex_trade_reflection_audit_v1"
DEFAULT_OUTPUT_DIR_NAME = "trade_reflection_audit_v1"

REQUIRED_SOURCE_ARTIFACTS = (
    "daily_candidate_snapshot.csv",
    "daily_action_ledger.jsonl",
    "rejected_candidates.csv",
    "post_run_outcome_labels.csv",
    "orders_ledger.csv",
    "positions_ledger.csv",
    "equity_curve.csv",
    "trade_contribution.csv",
    "failure_diagnosis_summary.json",
    "no_lookahead_audit.json",
    "selection_feature_manifest.json",
)

OUTPUT_ARTIFACTS = (
    "trade_reflection_summary.json",
    "missed_entry_cases.csv",
    "late_entry_cases.csv",
    "early_entry_cases.csv",
    "early_profit_take_cases.csv",
    "late_exit_cases.csv",
    "hold_would_have_worked_cases.csv",
    "daily_reflection_calendar.csv",
    "reflection_priority_decision.json",
    "_ARTIFACT_COMPLETE.json",
)

CASE_COLUMNS = {
    "missed_entry_cases.csv": [
        "decision_ymd",
        "code",
        "candidate_rank",
        "selection_score",
        "reject_reason",
        "post_ret_20",
        "mfe_20",
        "mae_20",
        "outcome_bucket",
        "same_day_selected_best_post_ret20",
        "opportunity_gap_vs_same_day_bought",
        "clearly_better_than_same_day_bought",
        "missed_entry_class",
    ],
    "late_entry_cases.csv": [
        "order_id",
        "position_id",
        "code",
        "first_candidate_date",
        "buy_decision_ymd",
        "buy_date",
        "delay_days",
        "delay_calendar_days",
        "return_from_first_candidate_to_buy",
        "already_risen_before_buy",
        "late_entry_flag",
    ],
    "early_entry_cases.csv": [
        "order_id",
        "position_id",
        "code",
        "decision_ymd",
        "entry_ymd",
        "entry_to_5d_mae_proxy",
        "post_ret20",
        "early_entry_bucket",
        "early_entry_flag",
    ],
    "early_profit_take_cases.csv": [
        "order_id",
        "position_id",
        "code",
        "exit_decision_ymd",
        "exit_date",
        "exit_reason",
        "realized_pnl",
        "realized_return",
        "early_profit_take_flag",
        "post_exit_ret5",
        "post_exit_ret10",
        "post_exit_ret20",
        "post_exit_max_up20",
        "post_exit_observation_status",
        "post_exit_observed_sessions",
    ],
    "late_exit_cases.csv": [
        "order_id",
        "position_id",
        "code",
        "exit_decision_ymd",
        "exit_date",
        "exit_reason",
        "realized_pnl",
        "realized_return",
        "max_adverse_excursion",
        "mae_ymd",
        "days_from_mae_to_exit",
        "earlier_exit_loss_estimate",
        "avoidable_loss_estimate",
        "late_exit_flag",
    ],
    "hold_would_have_worked_cases.csv": [
        "position_id",
        "order_id",
        "code",
        "entry_ymd",
        "mae",
        "mae_ymd",
        "recovery_ymd",
        "sessions_from_mae_to_recovery",
        "max_recovery_return_20",
        "hold_would_have_worked_flag",
    ],
    "daily_reflection_calendar.csv": [
        "ymd",
        "bought_count",
        "rejected_big_winner_count",
        "early_entry_flag",
        "early_profit_take_flag",
        "late_exit_flag",
        "reflection_score",
        "main_reflection_type",
    ],
}

PRIORITY_CANDIDATES = (
    "missed_entry",
    "late_entry",
    "early_entry",
    "early_profit_take",
    "late_exit",
    "bought_weak_candidate",
    "cost_drag",
)

BIG_WINNER_RET20 = 0.10
BIG_WINNER_MFE20 = 0.15
SAME_DAY_BETTER_MARGIN = 0.05
LATE_ENTRY_DELAY_SESSIONS = 2
LATE_ENTRY_RUNUP = 0.05
EARLY_ENTRY_MAE5 = -0.03
LATE_EXIT_MAE = -0.08
LATE_EXIT_MIN_DAYS_FROM_MAE = 2
EARLY_PROFIT_TAKE_UP20 = 0.05
HOLD_RECOVERY_MAE = -0.03


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


def _write_csv(path: Path, rows: Iterable[dict[str, Any]] | pd.DataFrame) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = rows if isinstance(rows, pd.DataFrame) else pd.DataFrame(list(rows))
    frame.to_csv(path, index=False)
    return path


def _ensure_columns(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=columns)
    out = frame.copy()
    for column in columns:
        if column not in out.columns:
            out[column] = None
    return out


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _bool_series(series: pd.Series) -> pd.Series:
    return series.astype(str).str.lower().isin(["true", "1", "yes"])


def _load_frame(root: Path, name: str) -> pd.DataFrame:
    path = root / name
    if not path.exists():
        raise FileNotFoundError(f"missing required artifact: {path}")
    return pd.read_csv(path)


def _source_status(root: Path) -> dict[str, bool]:
    return {name: (root / name).exists() for name in REQUIRED_SOURCE_ARTIFACTS}


def _sort_by_ymd(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "ymd" not in frame.columns:
        return frame
    return frame.sort_values("ymd", kind="stable").reset_index(drop=True)


def _classify_reject(row: pd.Series) -> str:
    reason = str(row.get("reject_reason") or row.get("reason_type") or "").lower()
    if "max_position" in reason or "position" in reason:
        return "position_limit"
    if "cash" in reason:
        return "cash_limit"
    if any(token in reason for token in ["score", "guard", "no_next_open", "not_allowed", "blocked"]):
        return "rule_reject"
    rank = _safe_float(row.get("candidate_rank"))
    selected_rank = _safe_float(row.get("same_day_selected_best_rank"))
    if rank is not None and selected_rank is not None and rank > selected_rank:
        return "lower_rank"
    return "other_reject"


def _selected_labels(labels: pd.DataFrame) -> pd.DataFrame:
    if labels.empty or "was_selected" not in labels.columns:
        return pd.DataFrame(columns=labels.columns)
    return labels[_bool_series(labels["was_selected"])].copy()


def build_missed_entry_cases(rejected: pd.DataFrame, labels: pd.DataFrame) -> pd.DataFrame:
    if rejected.empty or labels.empty:
        return pd.DataFrame()
    selected = _selected_labels(labels)
    same_day = (
        selected.assign(
            post_ret_20_num=pd.to_numeric(selected.get("post_ret_20"), errors="coerce"),
            candidate_rank_num=pd.to_numeric(selected.get("candidate_rank"), errors="coerce") if "candidate_rank" in selected.columns else None,
        )
        .groupby("decision_ymd", as_index=False)
        .agg(
            same_day_selected_best_post_ret20=("post_ret_20_num", "max"),
            same_day_selected_count=("code", "count"),
        )
    )
    if not selected.empty:
        best_rank = (
            selected.merge(
                rejected[["decision_ymd", "selected_best_rank"]].drop_duplicates("decision_ymd"),
                on="decision_ymd",
                how="left",
            )[["decision_ymd", "selected_best_rank"]]
            .drop_duplicates("decision_ymd")
            .rename(columns={"selected_best_rank": "same_day_selected_best_rank"})
        )
        same_day = same_day.merge(best_rank, on="decision_ymd", how="left")
    merged = rejected.merge(
        labels[["decision_ymd", "code", "post_ret_20", "mfe_20", "mae_20", "outcome_bucket"]],
        on=["decision_ymd", "code"],
        how="left",
    ).merge(same_day, on="decision_ymd", how="left")
    merged["post_ret_20"] = pd.to_numeric(merged["post_ret_20"], errors="coerce")
    merged["mfe_20"] = pd.to_numeric(merged["mfe_20"], errors="coerce")
    merged["missed_entry_trigger"] = (
        (merged["post_ret_20"] >= BIG_WINNER_RET20) | (merged["mfe_20"] >= BIG_WINNER_MFE20)
    )
    missed = merged[merged["missed_entry_trigger"]].copy()
    if missed.empty:
        return missed
    missed["opportunity_gap_vs_same_day_bought"] = (
        missed["post_ret_20"] - pd.to_numeric(missed["same_day_selected_best_post_ret20"], errors="coerce")
    )
    missed["clearly_better_than_same_day_bought"] = missed["opportunity_gap_vs_same_day_bought"] >= SAME_DAY_BETTER_MARGIN
    missed["missed_entry_class"] = missed.apply(_classify_reject, axis=1)
    missed = missed.sort_values(
        ["clearly_better_than_same_day_bought", "post_ret_20", "mfe_20", "decision_ymd", "candidate_rank"],
        ascending=[False, False, False, True, True],
        kind="stable",
    )
    return missed


def build_late_entry_cases(candidates: pd.DataFrame, orders: pd.DataFrame) -> pd.DataFrame:
    buys = orders[(orders["action"] == "buy") & (orders["order_status"] == "filled")].copy()
    if buys.empty or candidates.empty:
        return pd.DataFrame()
    candidate_dates = sorted(pd.to_numeric(candidates["decision_ymd"], errors="coerce").dropna().astype(int).unique().tolist())
    rows: list[dict[str, Any]] = []
    for _idx, buy in buys.iterrows():
        code = str(buy["code"])
        buy_decision = int(buy["decision_ymd"])
        code_candidates = candidates[(candidates["code"].astype(str) == code) & (pd.to_numeric(candidates["decision_ymd"], errors="coerce") <= buy_decision)].copy()
        if code_candidates.empty:
            continue
        code_candidates = code_candidates.sort_values("decision_ymd", kind="stable")
        first = code_candidates.iloc[0]
        first_ymd = int(first["decision_ymd"])
        sessions_between = [ymd for ymd in candidate_dates if first_ymd <= ymd <= buy_decision]
        first_close = _safe_float(first.get("close"))
        buy_price = _safe_float(buy.get("execution_price"))
        runup = None if first_close in {None, 0.0} or buy_price is None else (buy_price / first_close) - 1.0
        delay_sessions = max(0, len(sessions_between) - 1)
        rows.append(
            {
                "order_id": buy.get("order_id"),
                "position_id": buy.get("position_id"),
                "code": code,
                "first_candidate_date": first_ymd,
                "buy_decision_ymd": buy_decision,
                "buy_date": int(buy["execution_ymd"]),
                "delay_days": delay_sessions,
                "delay_calendar_days": (pd.to_datetime(str(buy_decision), format="%Y%m%d") - pd.to_datetime(str(first_ymd), format="%Y%m%d")).days,
                "return_from_first_candidate_to_buy": runup,
                "already_risen_before_buy": bool(runup is not None and runup >= LATE_ENTRY_RUNUP),
                "late_entry_flag": bool(delay_sessions >= LATE_ENTRY_DELAY_SESSIONS and runup is not None and runup >= LATE_ENTRY_RUNUP),
                "first_candidate_rank": first.get("candidate_rank"),
                "buy_selection_score": buy.get("selection_score"),
            }
        )
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values(["late_entry_flag", "return_from_first_candidate_to_buy", "delay_days"], ascending=[False, False, False], kind="stable")


def _position_returns(positions: pd.DataFrame, position_id: Any) -> pd.DataFrame:
    history = positions[positions["position_id"].astype(str) == str(position_id)].copy()
    if history.empty:
        return history
    history = _sort_by_ymd(history)
    history["return_from_cost_basis"] = pd.to_numeric(history["unrealized_pnl"], errors="coerce") / pd.to_numeric(history["cost_basis"], errors="coerce")
    return history


def build_early_entry_cases(orders: pd.DataFrame, positions: pd.DataFrame, labels: pd.DataFrame) -> pd.DataFrame:
    buys = orders[(orders["action"] == "buy") & (orders["order_status"] == "filled")].copy()
    if buys.empty:
        return pd.DataFrame()
    selected = _selected_labels(labels)
    rows: list[dict[str, Any]] = []
    for _idx, buy in buys.iterrows():
        history = _position_returns(positions, buy.get("position_id"))
        first5 = history[pd.to_numeric(history.get("holding_days"), errors="coerce") <= 5] if not history.empty else pd.DataFrame()
        mae5 = None if first5.empty else float(first5["return_from_cost_basis"].min())
        label = selected[(selected["decision_ymd"].astype(int) == int(buy["decision_ymd"])) & (selected["code"].astype(str) == str(buy["code"]))]
        post_ret20 = _safe_float(label.iloc[0].get("post_ret_20")) if not label.empty else None
        early_flag = bool(mae5 is not None and mae5 <= EARLY_ENTRY_MAE5)
        if not early_flag:
            continue
        bucket = "early_but_eventual_winner" if post_ret20 is not None and post_ret20 > 0 else "bad_buy_or_not_recovered"
        rows.append(
            {
                "order_id": buy.get("order_id"),
                "position_id": buy.get("position_id"),
                "code": str(buy["code"]),
                "decision_ymd": int(buy["decision_ymd"]),
                "entry_ymd": int(buy["execution_ymd"]),
                "entry_to_5d_mae_proxy": mae5,
                "post_ret20": post_ret20,
                "early_entry_bucket": bucket,
                "early_entry_flag": early_flag,
            }
        )
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values(["entry_to_5d_mae_proxy", "decision_ymd"], kind="stable")


def _future_candidate_returns(candidates: pd.DataFrame, code: str, exit_ymd: int, exit_price: float) -> dict[str, Any]:
    future = candidates[(candidates["code"].astype(str) == code) & (pd.to_numeric(candidates["decision_ymd"], errors="coerce") > exit_ymd)].copy()
    if future.empty or "close" not in future.columns:
        return {
            "post_exit_ret5": None,
            "post_exit_ret10": None,
            "post_exit_ret20": None,
            "post_exit_max_up20": None,
            "post_exit_observation_status": "unavailable_no_future_candidate_snapshot",
            "post_exit_observed_sessions": 0,
        }
    future = future.sort_values("decision_ymd", kind="stable").head(20).copy()
    future["ret"] = pd.to_numeric(future["close"], errors="coerce") / exit_price - 1.0
    def nth_ret(n: int) -> float | None:
        if len(future) < n:
            return None
        return _safe_float(future.iloc[n - 1].get("ret"))

    status = "available_candidate_snapshot_subset" if len(future) >= 20 else "partial_candidate_snapshot_subset"
    return {
        "post_exit_ret5": nth_ret(5),
        "post_exit_ret10": nth_ret(10),
        "post_exit_ret20": nth_ret(20),
        "post_exit_max_up20": _safe_float(future["ret"].max()),
        "post_exit_observation_status": status,
        "post_exit_observed_sessions": int(len(future)),
    }


def build_early_profit_take_cases(orders: pd.DataFrame, candidates: pd.DataFrame) -> pd.DataFrame:
    exits = orders[(orders["action"] == "exit") & (orders["order_status"] == "filled")].copy()
    if not exits.empty:
        realized = pd.to_numeric(exits.get("realized_return"), errors="coerce")
        reasons = exits.get("reason_type", pd.Series(dtype=str)).astype(str)
        exits = exits[(realized >= 0.0) | (reasons == "profit_target")].copy()
    if exits.empty:
        return pd.DataFrame()
    rows: list[dict[str, Any]] = []
    for _idx, exit_order in exits.iterrows():
        price = _safe_float(exit_order.get("execution_price"))
        if price is None or price == 0.0:
            continue
        continuation = _future_candidate_returns(candidates, str(exit_order["code"]), int(exit_order["execution_ymd"]), price)
        max_up = _safe_float(continuation.get("post_exit_max_up20"))
        ret20 = _safe_float(continuation.get("post_exit_ret20"))
        early_flag = bool((max_up is not None and max_up >= EARLY_PROFIT_TAKE_UP20) or (ret20 is not None and ret20 >= EARLY_PROFIT_TAKE_UP20))
        if not early_flag and continuation["post_exit_observation_status"] == "unavailable_no_future_candidate_snapshot":
            continue
        rows.append(
            {
                "order_id": exit_order.get("order_id"),
                "position_id": exit_order.get("position_id"),
                "code": str(exit_order["code"]),
                "exit_decision_ymd": int(exit_order["decision_ymd"]),
                "exit_date": int(exit_order["execution_ymd"]),
                "exit_reason": exit_order.get("reason_type"),
                "realized_pnl": exit_order.get("realized_pnl"),
                "realized_return": exit_order.get("realized_return"),
                "early_profit_take_flag": early_flag,
                **continuation,
            }
        )
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values(["early_profit_take_flag", "post_exit_max_up20"], ascending=[False, False], kind="stable")


def build_late_exit_cases(orders: pd.DataFrame, positions: pd.DataFrame) -> pd.DataFrame:
    exits = orders[(orders["action"].isin(["exit", "stop"])) & (orders["order_status"] == "filled")].copy()
    if exits.empty:
        return pd.DataFrame()
    rows: list[dict[str, Any]] = []
    for _idx, exit_order in exits.iterrows():
        realized_return = _safe_float(exit_order.get("realized_return"))
        if realized_return is None or realized_return >= 0:
            continue
        history = _position_returns(positions, exit_order.get("position_id"))
        if history.empty:
            continue
        min_idx = history["return_from_cost_basis"].idxmin()
        mae_row = history.loc[min_idx]
        mae = _safe_float(mae_row.get("return_from_cost_basis"))
        days_from_mae = int((history["ymd"].astype(int) >= int(mae_row["ymd"])).sum() - 1)
        cost_basis = _safe_float(history.iloc[0].get("cost_basis")) or _safe_float(exit_order.get("notional")) or 0.0
        stop_loss_estimate = -0.06 * cost_basis
        actual_loss = _safe_float(exit_order.get("realized_pnl")) or realized_return * cost_basis
        avoidable = max(0.0, stop_loss_estimate - actual_loss)
        late_flag = bool(mae is not None and mae <= LATE_EXIT_MAE and days_from_mae >= LATE_EXIT_MIN_DAYS_FROM_MAE)
        if not late_flag:
            continue
        rows.append(
            {
                "order_id": exit_order.get("order_id"),
                "position_id": exit_order.get("position_id"),
                "code": str(exit_order["code"]),
                "exit_decision_ymd": int(exit_order["decision_ymd"]),
                "exit_date": int(exit_order["execution_ymd"]),
                "exit_reason": exit_order.get("reason_type"),
                "realized_pnl": actual_loss,
                "realized_return": realized_return,
                "max_adverse_excursion": mae,
                "mae_ymd": int(mae_row["ymd"]),
                "days_from_mae_to_exit": days_from_mae,
                "earlier_exit_loss_estimate": stop_loss_estimate,
                "avoidable_loss_estimate": avoidable,
                "late_exit_flag": late_flag,
            }
        )
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values(["late_exit_flag", "avoidable_loss_estimate", "max_adverse_excursion"], ascending=[False, False, True], kind="stable")


def build_hold_would_have_worked_cases(orders: pd.DataFrame, positions: pd.DataFrame) -> pd.DataFrame:
    buys = orders[(orders["action"] == "buy") & (orders["order_status"] == "filled")].copy()
    rows: list[dict[str, Any]] = []
    for _idx, buy in buys.iterrows():
        history = _position_returns(positions, buy.get("position_id"))
        if history.empty:
            continue
        mae = _safe_float(history["return_from_cost_basis"].min())
        if mae is None or mae > HOLD_RECOVERY_MAE:
            continue
        min_pos = history["return_from_cost_basis"].idxmin()
        after_mae = history.loc[min_pos:].head(21).copy()
        recovered = after_mae[after_mae["return_from_cost_basis"] >= 0.0]
        if recovered.empty:
            continue
        peak20 = _safe_float(after_mae["return_from_cost_basis"].max())
        rows.append(
            {
                "position_id": buy.get("position_id"),
                "order_id": buy.get("order_id"),
                "code": str(buy["code"]),
                "entry_ymd": int(buy["execution_ymd"]),
                "mae": mae,
                "mae_ymd": int(history.loc[min_pos]["ymd"]),
                "recovery_ymd": int(recovered.iloc[0]["ymd"]),
                "sessions_from_mae_to_recovery": int((after_mae["ymd"].astype(int) <= int(recovered.iloc[0]["ymd"])).sum() - 1),
                "max_recovery_return_20": peak20,
                "hold_would_have_worked_flag": True,
            }
        )
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values(["max_recovery_return_20", "mae"], ascending=[False, True], kind="stable")


def build_daily_reflection_calendar(
    equity: pd.DataFrame,
    actions: pd.DataFrame,
    missed: pd.DataFrame,
    early_entry: pd.DataFrame,
    early_profit: pd.DataFrame,
    late_exit: pd.DataFrame,
) -> pd.DataFrame:
    dates = sorted(set(pd.to_numeric(equity.get("ymd", pd.Series(dtype=int)), errors="coerce").dropna().astype(int).tolist()))
    if not dates:
        dates = sorted(set(pd.to_numeric(actions.get("decision_ymd", pd.Series(dtype=int)), errors="coerce").dropna().astype(int).tolist()))
    buys = actions[actions.get("action", pd.Series(dtype=str)).astype(str) == "buy"] if not actions.empty else pd.DataFrame()
    rows: list[dict[str, Any]] = []
    for ymd in dates:
        bought_count = int((pd.to_numeric(buys.get("decision_ymd", pd.Series(dtype=int)), errors="coerce") == ymd).sum()) if not buys.empty else 0
        rejected_big_winner_count = int((pd.to_numeric(missed.get("decision_ymd", pd.Series(dtype=int)), errors="coerce") == ymd).sum()) if not missed.empty else 0
        early_entry_flag = bool((pd.to_numeric(early_entry.get("decision_ymd", pd.Series(dtype=int)), errors="coerce") == ymd).any()) if not early_entry.empty else False
        early_profit_take_flag = bool((pd.to_numeric(early_profit.get("exit_decision_ymd", pd.Series(dtype=int)), errors="coerce") == ymd).any()) if not early_profit.empty else False
        late_exit_flag = bool((pd.to_numeric(late_exit.get("exit_decision_ymd", pd.Series(dtype=int)), errors="coerce") == ymd).any()) if not late_exit.empty else False
        typed_scores = {
            "missed_entry": rejected_big_winner_count * 3,
            "early_entry": 2 if early_entry_flag else 0,
            "early_profit_take": 2 if early_profit_take_flag else 0,
            "late_exit": 3 if late_exit_flag else 0,
        }
        reflection_score = sum(typed_scores.values())
        main_type = "none" if reflection_score == 0 else max(typed_scores.items(), key=lambda item: item[1])[0]
        rows.append(
            {
                "ymd": ymd,
                "bought_count": bought_count,
                "rejected_big_winner_count": rejected_big_winner_count,
                "early_entry_flag": early_entry_flag,
                "early_profit_take_flag": early_profit_take_flag,
                "late_exit_flag": late_exit_flag,
                "reflection_score": reflection_score,
                "main_reflection_type": main_type,
            }
        )
    return pd.DataFrame(rows)


def build_priority_decision(
    failure: dict[str, Any],
    missed: pd.DataFrame,
    late_entry: pd.DataFrame,
    early_entry: pd.DataFrame,
    early_profit: pd.DataFrame,
    late_exit: pd.DataFrame,
    orders: pd.DataFrame,
) -> dict[str, Any]:
    metrics = failure.get("metrics", {}) if isinstance(failure, dict) else {}
    bought_weak_count = int(metrics.get("bought_weak_candidate_count") or 0)
    total_cost = float(metrics.get("total_cost") or pd.to_numeric(orders.get("cost_amount", pd.Series(dtype=float)), errors="coerce").sum())
    gross_profit_proxy = float(abs(pd.to_numeric(orders.get("realized_pnl", pd.Series(dtype=float)), errors="coerce").dropna().sum()) or 1.0)
    scores = {
        "missed_entry": {
            "raw_count": int(len(missed)),
            "headroom_proxy": float(pd.to_numeric(missed.get("opportunity_gap_vs_same_day_bought", pd.Series(dtype=float)), errors="coerce").clip(lower=0).sum()) if not missed.empty else 0.0,
            "win_path_damage_risk": "high",
            "same_day_feature_traceability": "medium",
        },
        "late_entry": {
            "raw_count": int(pd.Series(late_entry.get("late_entry_flag", [])).astype(str).str.lower().isin(["true", "1"]).sum()) if not late_entry.empty else 0,
            "headroom_proxy": float(pd.to_numeric(late_entry.get("return_from_first_candidate_to_buy", pd.Series(dtype=float)), errors="coerce").clip(lower=0).sum()) if not late_entry.empty else 0.0,
            "win_path_damage_risk": "medium",
            "same_day_feature_traceability": "medium",
        },
        "early_entry": {
            "raw_count": int(len(early_entry)),
            "headroom_proxy": float(abs(pd.to_numeric(early_entry.get("entry_to_5d_mae_proxy", pd.Series(dtype=float)), errors="coerce").clip(upper=0).sum())) if not early_entry.empty else 0.0,
            "win_path_damage_risk": "medium",
            "same_day_feature_traceability": "medium",
        },
        "early_profit_take": {
            "raw_count": int(pd.Series(early_profit.get("early_profit_take_flag", [])).astype(str).str.lower().isin(["true", "1"]).sum()) if not early_profit.empty else 0,
            "headroom_proxy": float(pd.to_numeric(early_profit.get("post_exit_max_up20", pd.Series(dtype=float)), errors="coerce").clip(lower=0).sum()) if not early_profit.empty else 0.0,
            "win_path_damage_risk": "medium",
            "same_day_feature_traceability": "low",
        },
        "late_exit": {
            "raw_count": int(len(late_exit)),
            "headroom_proxy": float(pd.to_numeric(late_exit.get("avoidable_loss_estimate", pd.Series(dtype=float)), errors="coerce").sum()) if not late_exit.empty else 0.0,
            "win_path_damage_risk": "medium",
            "same_day_feature_traceability": "medium",
        },
        "bought_weak_candidate": {
            "raw_count": bought_weak_count,
            "headroom_proxy": float(bought_weak_count),
            "win_path_damage_risk": "low",
            "same_day_feature_traceability": "high",
        },
        "cost_drag": {
            "raw_count": int(len(orders)),
            "headroom_proxy": total_cost,
            "cost_as_abs_realized_pnl_proxy": total_cost / gross_profit_proxy,
            "win_path_damage_risk": "medium",
            "same_day_feature_traceability": "medium",
        },
    }
    selected = "bought_weak_candidate"
    reason = "conservative_single_axis_bad_buy_reduction"
    if scores["late_exit"]["headroom_proxy"] > max(total_cost * 0.5, 1_000_000.0):
        selected = "late_exit"
        reason = "avoidable_late_exit_loss_dominates"
    elif scores["early_profit_take"]["raw_count"] >= 10 and scores["early_profit_take"]["same_day_feature_traceability"] != "low":
        selected = "early_profit_take"
        reason = "large_observable_profit_take_headroom"
    elif bought_weak_count <= 0 and scores["early_entry"]["raw_count"] > 0:
        selected = "early_entry"
        reason = "no_bought_weak_signal_available"
    elif bought_weak_count <= 0 and scores["missed_entry"]["raw_count"] > 0:
        selected = "missed_entry"
        reason = "only_missed_entry_signal_available"
    return {
        "schema_version": f"{SCHEMA_PREFIX}_reflection_priority_decision_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "priority_candidates": list(PRIORITY_CANDIDATES),
        "selected_next_axis": selected,
        "selected_next_axis_count": 1,
        "selection_reason_type": reason,
        "scoreboard": scores,
        "policy": {
            "single_axis_only": True,
            "replay_rerun": False,
            "rule_changed": False,
            "post_run_diagnostics_not_used_as_trading_rules": True,
            "no_silent_fallback": True,
        },
    }


def run_trade_reflection_audit_v1(run_root: str | Path, output_root: str | Path | None = None) -> dict[str, Any]:
    run_root = Path(run_root)
    output_root = Path(output_root) if output_root is not None else run_root / DEFAULT_OUTPUT_DIR_NAME
    source_status = _source_status(run_root)
    missing = [name for name, exists in source_status.items() if not exists]
    if missing:
        raise FileNotFoundError(f"missing required source artifacts: {missing}")

    candidates = _load_frame(run_root, "daily_candidate_snapshot.csv")
    actions = pd.DataFrame(_read_jsonl(run_root / "daily_action_ledger.jsonl"))
    rejected = _load_frame(run_root, "rejected_candidates.csv")
    labels = _load_frame(run_root, "post_run_outcome_labels.csv")
    orders = _load_frame(run_root, "orders_ledger.csv")
    positions = _load_frame(run_root, "positions_ledger.csv")
    equity = _load_frame(run_root, "equity_curve.csv")
    trade_contribution = _load_frame(run_root, "trade_contribution.csv")
    failure = _read_json(run_root / "failure_diagnosis_summary.json")
    no_lookahead = _read_json(run_root / "no_lookahead_audit.json")
    manifest = _read_json(run_root / "selection_feature_manifest.json")

    missed = build_missed_entry_cases(rejected, labels)
    late_entry = build_late_entry_cases(candidates, orders)
    early_entry = build_early_entry_cases(orders, positions, labels)
    early_profit = build_early_profit_take_cases(orders, candidates)
    late_exit = build_late_exit_cases(orders, positions)
    hold_worked = build_hold_would_have_worked_cases(orders, positions)
    calendar = build_daily_reflection_calendar(equity, actions, missed, early_entry, early_profit, late_exit)
    decision = build_priority_decision(failure, missed, late_entry, early_entry, early_profit, late_exit, orders)

    _write_csv(output_root / "missed_entry_cases.csv", _ensure_columns(missed, CASE_COLUMNS["missed_entry_cases.csv"]))
    _write_csv(output_root / "late_entry_cases.csv", _ensure_columns(late_entry, CASE_COLUMNS["late_entry_cases.csv"]))
    _write_csv(output_root / "early_entry_cases.csv", _ensure_columns(early_entry, CASE_COLUMNS["early_entry_cases.csv"]))
    _write_csv(output_root / "early_profit_take_cases.csv", _ensure_columns(early_profit, CASE_COLUMNS["early_profit_take_cases.csv"]))
    _write_csv(output_root / "late_exit_cases.csv", _ensure_columns(late_exit, CASE_COLUMNS["late_exit_cases.csv"]))
    _write_csv(output_root / "hold_would_have_worked_cases.csv", _ensure_columns(hold_worked, CASE_COLUMNS["hold_would_have_worked_cases.csv"]))
    _write_csv(output_root / "daily_reflection_calendar.csv", _ensure_columns(calendar, CASE_COLUMNS["daily_reflection_calendar.csv"]))
    _write_json(output_root / "reflection_priority_decision.json", decision)

    summary = {
        "schema_version": f"{SCHEMA_PREFIX}_summary_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "run_root": str(run_root),
        "source_artifacts": source_status,
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
        "audit_inputs": {
            "no_lookahead_audit_result": no_lookahead.get("audit_result"),
            "selection_feature_manifest_result": manifest.get("audit_result"),
            "trade_contribution_rows": int(len(trade_contribution)),
        },
        "case_counts": {
            "missed_entry": int(len(missed)),
            "late_entry_rows": int(len(late_entry)),
            "late_entry_flagged": int(pd.Series(late_entry.get("late_entry_flag", [])).astype(str).str.lower().isin(["true", "1"]).sum()) if not late_entry.empty else 0,
            "early_entry": int(len(early_entry)),
            "early_profit_take_rows": int(len(early_profit)),
            "early_profit_take_flagged": int(pd.Series(early_profit.get("early_profit_take_flag", [])).astype(str).str.lower().isin(["true", "1"]).sum()) if not early_profit.empty else 0,
            "late_exit": int(len(late_exit)),
            "hold_would_have_worked": int(len(hold_worked)),
            "daily_reflection_calendar": int(len(calendar)),
        },
        "missed_entry_by_class": missed["missed_entry_class"].value_counts().to_dict() if not missed.empty and "missed_entry_class" in missed.columns else {},
        "reflection_calendar_top_type_counts": calendar["main_reflection_type"].value_counts().to_dict() if not calendar.empty else {},
        "observability_limits": {
            "post_exit_returns_source": "daily_candidate_snapshot_close_only_when_symbol_reappears_after_exit",
            "post_exit_returns_are_candidate_snapshot_subset": True,
            "post_exit_unavailable_rows": int((early_profit.get("post_exit_observation_status", pd.Series(dtype=str)) == "unavailable_no_future_candidate_snapshot").sum()) if not early_profit.empty else 0,
            "post_exit_partial_rows": int((early_profit.get("post_exit_observation_status", pd.Series(dtype=str)) == "partial_candidate_snapshot_subset").sum()) if not early_profit.empty else 0,
        },
        "selected_next_axis": decision["selected_next_axis"],
    }
    _write_json(output_root / "trade_reflection_summary.json", summary)

    complete_payload = {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "run_root": str(run_root),
        "output_root": str(output_root),
        "complete": True,
        "required_artifacts_all_present": all((output_root / name).exists() for name in OUTPUT_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json"),
        "source_artifacts_all_present": all(source_status.values()),
        "output_artifacts": list(OUTPUT_ARTIFACTS),
        "selected_next_axis": decision["selected_next_axis"],
        "selected_next_axis_count": decision["selected_next_axis_count"],
        "trade_reflection_logs_non_empty": {
            "missed_entry_cases": len(missed) > 0,
            "late_entry_cases": len(late_entry) > 0,
            "early_entry_cases": len(early_entry) > 0,
            "early_profit_take_cases": len(early_profit) > 0,
            "late_exit_cases": len(late_exit) > 0,
            "daily_reflection_calendar": len(calendar) > 0,
        },
        "replay_rerun": False,
        "rule_changed": False,
        "conditions_changed": False,
        "post_run_labels_used_for_diagnostics_only": True,
        "silent_fallback_used": False,
    }
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete_payload)
    return {
        "complete": True,
        "output_root": str(output_root),
        "summary": summary,
        "selected_next_axis": decision["selected_next_axis"],
    }


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build TRADEX trade reflection audit artifacts for portfolio_agent_replay_v1.")
    parser.add_argument("--run-root", required=True, type=Path)
    parser.add_argument("--output-root", type=Path, default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    result = run_trade_reflection_audit_v1(run_root=args.run_root, output_root=args.output_root)
    print(_json_text(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
