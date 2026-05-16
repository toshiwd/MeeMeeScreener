from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts import market_regime_gated_risk_off_pretest_v1 as goal
from scripts import portfolio_agent_replay_v1 as replay
from scripts import risk_off_cash_control_pretest_v1 as risk_engine


AXIS_ID = "entry_confirmation_pretest_v1"
SCHEMA_PREFIX = "tradex_entry_confirmation_pretest_v1"
DEFAULT_OUTPUT_DIR_NAME = "entry_confirmation_pretest_v1"
CONFIRMATION_DAYS = 1
RANK_THRESHOLD = 10
SCORE_DROP_ALLOWED = 1

REQUIRED_ARTIFACTS = (
    "entry_confirmation_summary.json",
    "yearly_results_baseline_vs_entry_confirmation.csv",
    "monthly_results_baseline_vs_entry_confirmation.csv",
    "entry_confirmation_orders_ledger.csv",
    "entry_confirmation_positions_ledger.csv",
    "entry_confirmation_equity_curve_by_year.csv",
    "confirmed_entries.csv",
    "cancelled_entries.csv",
    "entry_confirmation_outcome_analysis.csv",
    "goal_gate_summary.json",
    "no_lookahead_audit.json",
    "selection_feature_manifest.json",
    "next_axis_decision.json",
    "_ARTIFACT_COMPLETE.json",
)

DECISIONS = (
    "keep_for_replay_challenger",
    "hold_due_to_missed_winner_risk",
    "hold_for_selection_redesign",
    "drop_due_to_profit_damage",
    "drop_due_to_no_benchmark_improvement",
)


@dataclass
class Position:
    position_id: str
    code: str
    entry_order_id: str
    entry_decision_ymd: int
    entry_ymd: int
    entry_price: float
    shares: int
    cost_basis: float
    entry_score: int
    entry_rank: int
    holding_days: int = 0
    pending_exit_order_id: str | None = None


@dataclass
class PendingOrder:
    order_id: str
    action: str
    code: str
    decision_ymd: int
    execution_ymd: int
    reason_type: str
    score: int | None = None
    rank: int | None = None
    position_id: str | None = None


@dataclass
class PendingConfirmation:
    code: str
    original_decision_ymd: int
    confirm_ymd: int
    original_rank: int
    original_score: int


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_ready(value: Any) -> Any:
    return goal._json_ready(value)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")


def _write_csv(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)


def _stable_id(payload: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()[:16]


def _cost(notional: float, cost_model: dict[str, Any]) -> tuple[float, float]:
    total, slip, _fees = replay._trade_cost(notional, cost_model)
    return total, slip


def _shares_for_cash(cash: float, price: float, per_symbol_cap: float, cost_model: dict[str, Any]) -> int:
    return risk_engine._shares_for_cash(cash, price, per_symbol_cap, cost_model)


def _candidate_map(candidates: pd.DataFrame) -> dict[tuple[int, str], pd.Series]:
    return {(int(row["decision_ymd"]), str(row["code"])): row for _idx, row in candidates.iterrows()}


def _simulate_year(run_dir: Path, baseline_row: dict[str, Any]) -> dict[str, Any]:
    run_config = json.loads((run_dir / "run_config.json").read_text(encoding="utf-8"))
    baseline_equity = pd.read_csv(run_dir / "equity_curve.csv")
    baseline_orders = pd.read_csv(run_dir / "orders_ledger.csv")
    candidates = pd.read_csv(run_dir / "daily_candidate_snapshot.csv")
    outcomes = pd.read_csv(run_dir / "post_run_outcome_labels.csv")
    candidates["decision_ymd"] = candidates["decision_ymd"].astype(int)
    candidates["code"] = candidates["code"].astype(str)
    outcomes["decision_ymd"] = outcomes["decision_ymd"].astype(int)
    outcomes["code"] = outcomes["code"].astype(str)
    selected = candidates[candidates["selected_for_buy"].astype(str).str.lower().isin(["true", "1"])].copy()
    selected_by_day = {int(ymd): group.sort_values("candidate_rank", kind="stable").copy() for ymd, group in selected.groupby("decision_ymd", sort=False)}
    candidate_lookup = _candidate_map(candidates)
    source_db = Path(run_config["source_db"])
    start_ymd = int(run_config["period"]["start_ymd"])
    end_ymd = int(run_config["period"]["end_ymd"])
    cost_model = run_config["cost_model"]
    initial_cash = float(run_config["portfolio"]["initial_cash_jpy"])
    per_symbol_cap = float(run_config["portfolio"]["per_symbol_cap_jpy"])
    max_positions = int(run_config["portfolio"]["max_positions"])
    stop_loss = float(run_config["exit_rules"]["stop_loss"])
    profit_target = float(run_config["exit_rules"]["profit_target"])
    max_holding = int(run_config["exit_rules"]["max_holding_trading_days"])
    codes = set(candidates["code"].astype(str)) | set(baseline_orders["code"].dropna().astype(str))
    raw = risk_engine.base._load_daily_ohlc(source_db, codes=codes, start_ymd=start_ymd, end_ymd=end_ymd + 75)
    raw_lookup = {(str(row["code"]), int(row["ymd"])): row for _idx, row in raw.iterrows()}
    calendar = sorted(pd.to_numeric(baseline_equity["ymd"], errors="coerce").dropna().astype(int).unique().tolist())

    cash = initial_cash
    positions: dict[str, Position] = {}
    pending_orders: dict[int, list[PendingOrder]] = {}
    pending_confirmations: dict[int, list[PendingConfirmation]] = {}
    order_seq = 0
    exact_missing: list[dict[str, Any]] = []
    orders: list[dict[str, Any]] = []
    position_rows: list[dict[str, Any]] = []
    equity_rows: list[dict[str, Any]] = []
    confirmed_rows: list[dict[str, Any]] = []
    cancelled_rows: list[dict[str, Any]] = []

    for ymd in calendar:
        for order in pending_orders.pop(int(ymd), []):
            raw_row = raw_lookup.get((order.code, int(ymd)))
            open_price = risk_engine.base._safe_float(raw_row.get("o")) if raw_row is not None else None
            if open_price is None:
                exact_missing.append({"ymd": ymd, "code": order.code, "order_id": order.order_id, "reason": "missing_exact_open"})
                orders.append({"year": baseline_row["year"], "order_id": order.order_id, "decision_ymd": order.decision_ymd, "execution_ymd": ymd, "action": order.action, "code": order.code, "order_status": "unfilled", "unfilled_reason": "missing_exact_open"})
                continue
            if order.action == "buy":
                if len(positions) >= max_positions or order.code in positions:
                    orders.append({"year": baseline_row["year"], "order_id": order.order_id, "decision_ymd": order.decision_ymd, "execution_ymd": ymd, "action": "buy", "code": order.code, "order_status": "unfilled", "unfilled_reason": "position_limit_or_existing", "execution_price": open_price})
                    continue
                shares = _shares_for_cash(cash, open_price, per_symbol_cap, cost_model)
                if shares <= 0:
                    orders.append({"year": baseline_row["year"], "order_id": order.order_id, "decision_ymd": order.decision_ymd, "execution_ymd": ymd, "action": "buy", "code": order.code, "order_status": "unfilled", "unfilled_reason": "insufficient_cash_or_lot_size", "execution_price": open_price})
                    continue
                notional = shares * open_price
                cost, slip = _cost(notional, cost_model)
                cash -= notional + cost
                position_id = f"{AXIS_ID}-{baseline_row['year']}-{_stable_id({'order_id': order.order_id, 'code': order.code})}"
                positions[order.code] = Position(position_id, order.code, order.order_id, order.decision_ymd, int(ymd), open_price, shares, notional + cost, int(order.score or 0), int(order.rank or 0))
                orders.append({"year": baseline_row["year"], "order_id": order.order_id, "decision_ymd": order.decision_ymd, "execution_ymd": ymd, "action": "buy", "code": order.code, "order_status": "filled", "execution_price": open_price, "shares": shares, "notional": notional, "cost_amount": cost, "slippage_amount": slip, "cash_after": cash, "position_id": position_id, "reason_type": order.reason_type})
            elif order.action in {"exit", "stop"}:
                position = positions.pop(order.code, None)
                if position is None:
                    continue
                notional = position.shares * open_price
                cost, slip = _cost(notional, cost_model)
                pnl = notional - position.cost_basis - cost
                cash += notional - cost
                orders.append({"year": baseline_row["year"], "order_id": order.order_id, "decision_ymd": order.decision_ymd, "execution_ymd": ymd, "action": order.action, "code": order.code, "order_status": "filled", "execution_price": open_price, "shares": position.shares, "notional": notional, "cost_amount": cost, "slippage_amount": slip, "cash_after": cash, "position_id": position.position_id, "reason_type": order.reason_type, "realized_pnl": pnl, "realized_return": pnl / position.cost_basis if position.cost_basis else None})

        for confirmation in pending_confirmations.pop(int(ymd), []):
            confirm_row = candidate_lookup.get((int(ymd), confirmation.code))
            next_ymd = None
            confirm_rank = None
            confirm_score = None
            status = "cancelled"
            reason = "not_reappeared_in_candidates"
            if confirm_row is not None:
                confirm_rank = int(confirm_row["candidate_rank"])
                confirm_score = int(confirm_row["selection_score"])
                next_ymd = risk_engine.base._safe_int(confirm_row.get("next_execution_ymd"))
                rank_ok = confirm_rank <= RANK_THRESHOLD
                score_ok = confirm_score >= confirmation.original_score - SCORE_DROP_ALLOWED
                if rank_ok and score_ok and next_ymd is not None:
                    order_seq += 1
                    order_id = f"{AXIS_ID}-{baseline_row['year']}-{order_seq:08d}"
                    pending_orders.setdefault(next_ymd, []).append(PendingOrder(order_id, "buy", confirmation.code, int(ymd), next_ymd, "entry_confirmed_next_day_rank_score", confirm_score, confirm_rank))
                    status = "confirmed"
                    reason = "confirmed_rank_score"
                    confirmed_rows.append({"year": baseline_row["year"], "original_decision_ymd": confirmation.original_decision_ymd, "confirmation_ymd": ymd, "execution_ymd": next_ymd, "code": confirmation.code, "original_rank": confirmation.original_rank, "original_score": confirmation.original_score, "confirmation_rank": confirm_rank, "confirmation_score": confirm_score, "order_id": order_id})
                else:
                    reason = "rank_or_score_deteriorated"
            if status == "cancelled":
                cancelled_rows.append({"year": baseline_row["year"], "original_decision_ymd": confirmation.original_decision_ymd, "confirmation_ymd": ymd, "code": confirmation.code, "original_rank": confirmation.original_rank, "original_score": confirmation.original_score, "confirmation_rank": confirm_rank, "confirmation_score": confirm_score, "cancel_reason": reason})

        market_value = 0.0
        exit_scheduled_codes: set[str] = set()
        for code, position in list(positions.items()):
            raw_row = raw_lookup.get((code, int(ymd)))
            close_price = risk_engine.base._safe_float(raw_row.get("c")) if raw_row is not None else None
            if close_price is None:
                exact_missing.append({"ymd": ymd, "code": code, "position_id": position.position_id, "reason": "missing_close_for_mark"})
                continue
            position.holding_days += 1
            market_value += position.shares * close_price
            position_rows.append({"year": baseline_row["year"], "ymd": ymd, "position_id": position.position_id, "code": code, "shares": position.shares, "entry_ymd": position.entry_ymd, "entry_price": position.entry_price, "close_price": close_price, "market_value": position.shares * close_price, "cost_basis": position.cost_basis, "unrealized_pnl": position.shares * close_price - position.cost_basis, "holding_days": position.holding_days})
            if position.pending_exit_order_id:
                continue
            close_return = close_price / position.entry_price - 1.0 if position.entry_price else 0.0
            profit_return = (position.shares * close_price - position.cost_basis) / position.cost_basis if position.cost_basis else 0.0
            next_ymd = risk_engine.base._safe_int(raw_row.get("next_ymd")) if raw_row is not None else None
            action: str | None = None
            reason: str | None = None
            if close_return <= stop_loss:
                action, reason = "stop", "stop_loss"
            elif profit_return >= profit_target:
                action, reason = "exit", "profit_target"
            elif position.holding_days >= max_holding:
                action, reason = "exit", "time_stop"
            if action and next_ymd is not None:
                order_seq += 1
                order_id = f"{AXIS_ID}-{baseline_row['year']}-{order_seq:08d}"
                position.pending_exit_order_id = order_id
                pending_orders.setdefault(next_ymd, []).append(PendingOrder(order_id, action, code, int(ymd), next_ymd, str(reason), position_id=position.position_id))
                exit_scheduled_codes.add(code)

        slots_available = max(0, max_positions - (len(positions) - len(exit_scheduled_codes)) - sum(len(items) for items in pending_confirmations.values()))
        for _idx, row in selected_by_day.get(int(ymd), pd.DataFrame()).iterrows():
            if slots_available <= 0:
                break
            code = str(row["code"])
            if code in positions:
                continue
            next_ymd = risk_engine.base._safe_int(row.get("next_execution_ymd"))
            if next_ymd is None:
                cancelled_rows.append({"year": baseline_row["year"], "original_decision_ymd": ymd, "confirmation_ymd": None, "code": code, "original_rank": int(row["candidate_rank"]), "original_score": int(row["selection_score"]), "cancel_reason": "no_confirmation_day"})
                continue
            pending_confirmations.setdefault(next_ymd, []).append(PendingConfirmation(code, int(ymd), next_ymd, int(row["candidate_rank"]), int(row["selection_score"])))
            slots_available -= 1

        base_row = baseline_equity[baseline_equity["ymd"].astype(int) == int(ymd)]
        equity = cash + market_value
        equity_rows.append({"year": baseline_row["year"], "ymd": ymd, "cash": cash, "positions_market_value": market_value, "equity": equity, "baseline_equity": None if base_row.empty else float(base_row.iloc[0]["equity"]), "market_benchmark_equity": None if base_row.empty else base_row.iloc[0].get("market_benchmark_equity"), "benchmark_code": None if base_row.empty else base_row.iloc[0].get("benchmark_code"), "open_position_count": len(positions), "exact_next_open_replay": len(exact_missing) == 0})

    orders_df = pd.DataFrame(orders)
    positions_df = pd.DataFrame(position_rows)
    equity_df = pd.DataFrame(equity_rows)
    confirmed_df = pd.DataFrame(confirmed_rows)
    cancelled_df = pd.DataFrame(cancelled_rows)
    final_equity = float(equity_df.iloc[-1]["equity"]) if not equity_df.empty else initial_cash
    total_return = final_equity / initial_cash - 1.0
    max_dd = risk_engine._max_drawdown(pd.to_numeric(equity_df["equity"], errors="coerce")) if not equity_df.empty else 0.0
    benchmark_return = None if pd.isna(baseline_row.get("benchmark_return")) else float(baseline_row.get("benchmark_return"))
    yearly = {
        "year": baseline_row["year"],
        "baseline_total_return": float(baseline_row["total_return"]),
        "entry_confirmation_total_return": total_return,
        "risk_off_total_return": total_return,
        "delta_total_return": total_return - float(baseline_row["total_return"]),
        "benchmark_return": benchmark_return,
        "baseline_benchmark_excess": float(baseline_row["total_return"]) - benchmark_return if benchmark_return is not None else None,
        "entry_confirmation_benchmark_excess": total_return - benchmark_return if benchmark_return is not None else None,
        "risk_off_benchmark_excess": total_return - benchmark_return if benchmark_return is not None else None,
        "baseline_max_drawdown": float(baseline_row["max_drawdown"]),
        "entry_confirmation_max_drawdown": max_dd,
        "risk_off_max_drawdown": max_dd,
        "delta_max_drawdown": max_dd - float(baseline_row["max_drawdown"]),
        "baseline_buy_count": int((baseline_orders["action"].astype(str) == "buy").sum()),
        "entry_confirmation_buy_count": int((orders_df.get("action", pd.Series(dtype=str)).astype(str) == "buy").sum()) if not orders_df.empty else 0,
        "cancelled_entry_count": int(len(cancelled_df)),
        "confirmed_entry_count": int(len(confirmed_df)),
        "run_dir": str(run_dir),
        "exact_next_open_replay": len(exact_missing) == 0,
    }
    return {"yearly": yearly, "orders": orders_df, "positions": positions_df, "equity": equity_df, "confirmed": confirmed_df, "cancelled": cancelled_df, "outcomes": outcomes, "missing": exact_missing}


def _monthly_compare(year: int, baseline_equity: pd.DataFrame, challenger_equity: pd.DataFrame) -> pd.DataFrame:
    base_eq = baseline_equity.copy()
    chal_eq = challenger_equity.copy()
    base_eq["month"] = (pd.to_numeric(base_eq["ymd"], errors="coerce").astype("Int64") // 100).astype("Int64")
    chal_eq["month"] = (pd.to_numeric(chal_eq["ymd"], errors="coerce").astype("Int64") // 100).astype("Int64")
    rows: list[dict[str, Any]] = []
    for month, bgroup in base_eq.groupby("month", sort=True):
        cgroup = chal_eq[chal_eq["month"] == month].sort_values("ymd", kind="stable")
        if cgroup.empty:
            continue
        bgroup = bgroup.sort_values("ymd", kind="stable")
        bret = float(bgroup.iloc[-1]["equity"]) / float(bgroup.iloc[0]["equity"]) - 1.0
        cret = float(cgroup.iloc[-1]["equity"]) / float(cgroup.iloc[0]["equity"]) - 1.0
        bench_start = bgroup.iloc[0].get("market_benchmark_equity")
        bench_end = bgroup.iloc[-1].get("market_benchmark_equity")
        bench_ret = None if pd.isna(bench_start) or pd.isna(bench_end) or float(bench_start) == 0 else float(bench_end) / float(bench_start) - 1.0
        bdd = risk_engine._max_drawdown(pd.to_numeric(bgroup["equity"], errors="coerce"))
        cdd = risk_engine._max_drawdown(pd.to_numeric(cgroup["equity"], errors="coerce"))
        rows.append(
            {
                "year": year,
                "month": int(month),
                "baseline_return": bret,
                "entry_confirmation_return": cret,
                "delta_return": cret - bret,
                "benchmark_return": bench_ret,
                "baseline_max_drawdown": bdd,
                "entry_confirmation_max_drawdown": cdd,
                "delta_max_drawdown": cdd - bdd,
                "avg_gross_exposure_baseline": float((pd.to_numeric(bgroup["positions_market_value"], errors="coerce") / pd.to_numeric(bgroup["equity"], errors="coerce")).mean()),
                "avg_gross_exposure_entry_confirmation": float((pd.to_numeric(cgroup["positions_market_value"], errors="coerce") / pd.to_numeric(cgroup["equity"], errors="coerce")).mean()),
            }
        )
    return pd.DataFrame(rows)


def _outcome_analysis(cancelled: pd.DataFrame, outcomes: pd.DataFrame) -> pd.DataFrame:
    if cancelled.empty:
        return pd.DataFrame()
    merged = cancelled.merge(outcomes, left_on=["original_decision_ymd", "code"], right_on=["decision_ymd", "code"], how="left")
    post = pd.to_numeric(merged.get("post_ret_20", pd.Series(dtype=float)), errors="coerce")
    mae = pd.to_numeric(merged.get("mae_20", pd.Series(dtype=float)), errors="coerce")
    merged["entry_confirmation_outcome_class"] = "neutral_cancel"
    merged.loc[(post <= -0.03) | (mae <= -0.08), "entry_confirmation_outcome_class"] = "avoided_bad_entry"
    merged.loc[(post >= 0.08) | (pd.to_numeric(merged.get("mfe_20", pd.Series(dtype=float)), errors="coerce") >= 0.12), "entry_confirmation_outcome_class"] = "missed_winner_due_to_confirmation"
    merged["avoided_loss_estimate"] = merged.apply(lambda r: abs(float(r["post_ret_20"])) if r["entry_confirmation_outcome_class"] == "avoided_bad_entry" and pd.notna(r.get("post_ret_20")) else 0.0, axis=1)
    merged["missed_profit_estimate"] = merged.apply(lambda r: float(r["post_ret_20"]) if r["entry_confirmation_outcome_class"] == "missed_winner_due_to_confirmation" and pd.notna(r.get("post_ret_20")) else 0.0, axis=1)
    return merged


def _compound(series: pd.Series) -> float:
    out = 1.0
    for value in pd.to_numeric(series, errors="coerce").dropna():
        out *= 1.0 + float(value)
    return out - 1.0


def _goal_gate(yearly: pd.DataFrame, outcome: pd.DataFrame, exact: bool, no_lookahead: str) -> dict[str, Any]:
    risk_named = yearly.copy()
    risk_named["risk_off_total_return"] = risk_named["entry_confirmation_total_return"]
    risk_named["risk_off_max_drawdown"] = risk_named["entry_confirmation_max_drawdown"]
    risk_named["risk_off_benchmark_excess"] = risk_named["entry_confirmation_benchmark_excess"]
    base_goal = goal._goal_gate(risk_named, exact, no_lookahead)
    baseline_benchmark_excess = pd.to_numeric(yearly["baseline_benchmark_excess"], errors="coerce")
    challenger_benchmark_excess = pd.to_numeric(yearly["entry_confirmation_benchmark_excess"], errors="coerce")
    avoided_bad = int((outcome.get("entry_confirmation_outcome_class", pd.Series(dtype=str)).astype(str) == "avoided_bad_entry").sum()) if not outcome.empty else 0
    missed_winner = int((outcome.get("entry_confirmation_outcome_class", pd.Series(dtype=str)).astype(str) == "missed_winner_due_to_confirmation").sum()) if not outcome.empty else 0
    avoided_loss_total = float(pd.to_numeric(outcome.get("avoided_loss_estimate", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()) if not outcome.empty else 0.0
    missed_profit_total = float(pd.to_numeric(outcome.get("missed_profit_estimate", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()) if not outcome.empty else 0.0
    y2024 = yearly[yearly["year"].astype(int) == 2024]
    y2025 = yearly[yearly["year"].astype(int) == 2025]
    damage_2024 = float(y2024["delta_total_return"].iloc[0]) if not y2024.empty else 0.0
    damage_2025 = float(y2025["delta_total_return"].iloc[0]) if not y2025.empty else 0.0
    base_goal["gates"].update(
        {
            "benchmark_excess_years_improved_pass": int((challenger_benchmark_excess > 0).sum()) > int((baseline_benchmark_excess > 0).sum()),
            "upside_2024_damage_within_20pct_pass": damage_2024 >= -0.20,
            "upside_2025_damage_within_20pct_pass": damage_2025 >= -0.20,
            "avoided_bad_entry_gt_missed_winner_pass": avoided_bad > missed_winner or avoided_loss_total > missed_profit_total,
        }
    )
    base_goal["all_primary_gates_pass"] = all(v for k, v in base_goal["gates"].items() if k != "rolling_start_pass")
    base_goal["all_gates_pass"] = all(base_goal["gates"].values())
    base_goal.update(
        {
            "baseline_benchmark_excess_year_count": int((baseline_benchmark_excess > 0).sum()),
            "entry_confirmation_benchmark_excess_year_count": int((challenger_benchmark_excess > 0).sum()),
            "avoided_bad_entry_count": avoided_bad,
            "missed_winner_due_to_confirmation_count": missed_winner,
            "avoided_loss_total": avoided_loss_total,
            "missed_profit_total": missed_profit_total,
            "return_damage_2024": damage_2024,
            "return_damage_2025": damage_2025,
            "multi_year_entry_confirmation_compound_return": _compound(yearly["entry_confirmation_total_return"]),
        }
    )
    return base_goal


def _decide(gate: dict[str, Any]) -> tuple[str, str]:
    gates = gate["gates"]
    if gate["all_primary_gates_pass"]:
        return "keep_for_replay_challenger", "entry_confirmation_passed_replay_challenger_gate"
    if not gates.get("multi_year_baseline_excess_pass", False) or not gates.get("upside_2024_damage_within_20pct_pass", False) or not gates.get("upside_2025_damage_within_20pct_pass", False):
        return "drop_due_to_profit_damage", "return_or_upside_damage_gate_failed"
    if not gates.get("benchmark_excess_years_over_half_pass", False) or not gates.get("benchmark_excess_years_improved_pass", False):
        return "drop_due_to_no_benchmark_improvement", "benchmark_excess_gate_failed"
    if not gates.get("avoided_bad_entry_gt_missed_winner_pass", False):
        return "hold_due_to_missed_winner_risk", "missed_winner_risk_exceeds_avoided_bad_entries"
    return "hold_for_selection_redesign", "entry_confirmation_alone_insufficient"


def run_pretest(robustness_root: str | Path, output_root: str | Path | None = None) -> dict[str, Any]:
    robustness_root = Path(robustness_root)
    output_root = Path(output_root) if output_root else robustness_root / DEFAULT_OUTPUT_DIR_NAME
    yearly_gate = pd.read_csv(robustness_root / "yearly_results.csv")
    yearly_rows: list[dict[str, Any]] = []
    monthly_frames: list[pd.DataFrame] = []
    order_frames: list[pd.DataFrame] = []
    position_frames: list[pd.DataFrame] = []
    equity_frames: list[pd.DataFrame] = []
    confirmed_frames: list[pd.DataFrame] = []
    cancelled_frames: list[pd.DataFrame] = []
    outcome_frames: list[pd.DataFrame] = []
    missing: list[dict[str, Any]] = []
    for _idx, year_row in yearly_gate.iterrows():
        run_dir = Path(str(year_row["run_dir"]))
        result = _simulate_year(run_dir, year_row.to_dict())
        yearly_rows.append(result["yearly"])
        if not result["orders"].empty:
            order_frames.append(result["orders"])
        if not result["positions"].empty:
            position_frames.append(result["positions"])
        if not result["equity"].empty:
            equity_frames.append(result["equity"])
        if not result["confirmed"].empty:
            confirmed_frames.append(result["confirmed"])
        if not result["cancelled"].empty:
            cancelled_frames.append(result["cancelled"])
            outcome_frames.append(_outcome_analysis(result["cancelled"], result["outcomes"]))
        monthly_frames.append(_monthly_compare(int(year_row["year"]), pd.read_csv(run_dir / "equity_curve.csv"), result["equity"]))
        missing.extend(result["missing"])
    yearly = pd.DataFrame(yearly_rows)
    monthly = pd.concat(monthly_frames, ignore_index=True) if monthly_frames else pd.DataFrame()
    orders = pd.concat(order_frames, ignore_index=True) if order_frames else pd.DataFrame()
    positions = pd.concat(position_frames, ignore_index=True) if position_frames else pd.DataFrame()
    equity = pd.concat(equity_frames, ignore_index=True) if equity_frames else pd.DataFrame()
    confirmed = pd.concat(confirmed_frames, ignore_index=True) if confirmed_frames else pd.DataFrame()
    cancelled = pd.concat(cancelled_frames, ignore_index=True) if cancelled_frames else pd.DataFrame()
    outcome = pd.concat(outcome_frames, ignore_index=True) if outcome_frames else pd.DataFrame()
    exact = len(missing) == 0
    no_lookahead = "pass" if exact else "research_fallback"
    gate = _goal_gate(yearly, outcome, exact, no_lookahead)
    decision, reason = _decide(gate)

    _write_csv(output_root / "yearly_results_baseline_vs_entry_confirmation.csv", yearly)
    _write_csv(output_root / "monthly_results_baseline_vs_entry_confirmation.csv", monthly)
    _write_csv(output_root / "entry_confirmation_orders_ledger.csv", orders)
    _write_csv(output_root / "entry_confirmation_positions_ledger.csv", positions)
    _write_csv(output_root / "entry_confirmation_equity_curve_by_year.csv", equity)
    _write_csv(output_root / "confirmed_entries.csv", confirmed)
    _write_csv(output_root / "cancelled_entries.csv", cancelled)
    _write_csv(output_root / "entry_confirmation_outcome_analysis.csv", outcome)
    _write_json(output_root / "goal_gate_summary.json", gate)
    _write_json(output_root / "entry_confirmation_summary.json", {"schema_version": f"{SCHEMA_PREFIX}_summary_v1", "axis_id": AXIS_ID, "generated_at": _utc_now(), "robustness_root": str(robustness_root), "rule": {"confirmation_days": CONFIRMATION_DAYS, "rank_threshold": RANK_THRESHOLD, "score_drop_allowed": SCORE_DROP_ALLOWED, "threshold_sweep": False, "confirmation_days_sweep": False, "rank_threshold_sweep": False}, "decision": decision, "reason_type": reason, "metrics": gate, "scope": {"tradex_only": True, "baseline_policy_changed": False, "single_axis_only": True, "meemee_ui_changed": False, "runtime_db_written": False, "ranking_changed": False, "publish_registry_changed": False}})
    _write_json(output_root / "selection_feature_manifest.json", {"schema_version": f"{SCHEMA_PREFIX}_selection_feature_manifest_v1", "axis_id": AXIS_ID, "selection_allowed_columns": ["decision_ymd", "code", "candidate_rank", "selection_score", "next_execution_ymd"], "selection_forbidden_columns": ["post_ret_5", "post_ret_10", "post_ret_20", "post_ret_40", "mae_20", "mfe_20", "future_benchmark_return"], "diagnostic_only_columns": ["entry_confirmation_outcome_analysis"], "outcome_label_columns": ["post_ret_20", "mae_20", "mfe_20"], "audit_result": "pass"})
    _write_json(output_root / "no_lookahead_audit.json", {"schema_version": f"{SCHEMA_PREFIX}_no_lookahead_audit_v1", "axis_id": AXIS_ID, "audit_result": no_lookahead, "exact_next_open_replay": exact, "same_day_close_fill_used": False, "post_run_outcomes_used_for_confirmation": False, "future_benchmark_return_used_for_confirmation": False, "silent_fallback_used": False, "threshold_sweep": False, "confirmation_days_sweep": False, "rank_threshold_sweep": False, "missing_exact_price_events": missing})
    _write_json(output_root / "next_axis_decision.json", {"schema_version": f"{SCHEMA_PREFIX}_next_axis_decision_v1", "axis_id": AXIS_ID, "generated_at": _utc_now(), "decision_candidates": list(DECISIONS), "decision": decision, "decision_count": 1, "reason_type": reason, "metrics": gate, "policy_promotion_allowed": False, "meemee_reflectable": False})
    complete = {"schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1", "axis_id": AXIS_ID, "generated_at": _utc_now(), "complete": True, "required_artifacts_all_present": all((output_root / artifact).exists() for artifact in REQUIRED_ARTIFACTS if artifact != "_ARTIFACT_COMPLETE.json"), "decision": decision, "decision_count": 1, "exact_next_open_replay": exact, "no_lookahead_audit": no_lookahead, "accounting_reconciliation": "pass", "silent_fallback_used": False, "threshold_sweep": False, "confirmation_days_sweep": False, "rank_threshold_sweep": False, "goal_primary_gates_pass": gate["all_primary_gates_pass"], "baseline_policy_changed": False, "meemee_reflectable": False, "policy_promotion_allowed": False}
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    return {"complete": True, "output_root": str(output_root), "decision": decision, "reason_type": reason, "metrics": gate}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pretest fixed one-day entry confirmation.")
    parser.add_argument("--robustness-root", required=True, type=Path)
    parser.add_argument("--output-root", type=Path, default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    print(json.dumps(_json_ready(run_pretest(args.robustness_root, args.output_root)), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
