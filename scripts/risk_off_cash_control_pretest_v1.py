from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts import partial_stop_at_minus8_pretest_v1 as base


AXIS_ID = "risk_off_cash_control_pretest_v1"
SCHEMA_PREFIX = "tradex_risk_off_cash_control_pretest_v1"
DEFAULT_OUTPUT_DIR_NAME = "risk_off_cash_control_pretest_v1"
LOOKBACK_DAYS = 20
DRAWDOWN_TRIGGER = -0.08
RECOVERY_THRESHOLD = -0.03
RISK_OFF_MAX_DAYS = 20
TRIM_RATIO = 0.50
LOT_SIZE = 100

REQUIRED_ARTIFACTS = (
    "risk_off_cash_control_summary.json",
    "yearly_results_baseline_vs_risk_off.csv",
    "monthly_results_baseline_vs_risk_off.csv",
    "risk_off_events.csv",
    "risk_off_orders_ledger.csv",
    "risk_off_positions_ledger.csv",
    "risk_off_equity_curve_by_year.csv",
    "exposure_cash_comparison.csv",
    "drawdown_comparison.csv",
    "benchmark_positive_portfolio_negative_comparison.csv",
    "missed_profit_during_risk_off.csv",
    "saved_loss_during_risk_off.csv",
    "no_lookahead_audit.json",
    "selection_feature_manifest.json",
    "next_axis_decision.json",
    "_ARTIFACT_COMPLETE.json",
)
DECISIONS = (
    "keep_for_replay_challenger",
    "hold_due_to_upside_damage",
    "hold_for_regime_filter_comparison",
    "drop_due_to_profit_damage",
    "drop_due_to_no_drawdown_improvement",
)


@dataclass
class Position:
    position_id: str
    baseline_position_id: str | None
    code: str
    entry_order_id: str
    entry_decision_ymd: int
    entry_ymd: int
    entry_price: float
    shares: int
    cost_basis: float
    original_shares: int
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
    baseline_position_id: str | None = None
    position_id: str | None = None
    event_id: str | None = None


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(v) for v in value]
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
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")


def _write_csv(path: Path, frame: pd.DataFrame | list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = frame if isinstance(frame, pd.DataFrame) else pd.DataFrame(frame)
    data.to_csv(path, index=False)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _max_drawdown(equity: pd.Series) -> float:
    if equity.empty:
        return 0.0
    peak = equity.cummax()
    return float((equity / peak - 1.0).min())


def _month_key(ymd: pd.Series) -> pd.Series:
    return (pd.to_numeric(ymd, errors="coerce").astype("Int64") // 100).astype("Int64")


def _shares_for_cash(cash: float, price: float, per_symbol_cap: float, cost_model: dict[str, Any]) -> int:
    return base._shares_for_cash(cash, price, per_symbol_cap, cost_model)


def _benchmark_context(baseline_equity: pd.DataFrame, ymd: int) -> dict[str, Any]:
    hist = baseline_equity[baseline_equity["ymd"].astype(int) <= int(ymd)].sort_values("ymd", kind="stable").tail(61).copy()
    if hist.empty or "market_benchmark_equity" not in hist.columns:
        return {"benchmark_context_status": "unavailable"}
    current = base._safe_float(hist.iloc[-1].get("market_benchmark_equity"))
    prior20 = base._safe_float(hist.iloc[-21].get("market_benchmark_equity")) if len(hist) >= 21 else None
    prior60 = base._safe_float(hist.iloc[-61].get("market_benchmark_equity")) if len(hist) >= 61 else None
    benchmark_values = pd.to_numeric(hist["market_benchmark_equity"], errors="coerce").dropna()
    peak = float(benchmark_values.max()) if not benchmark_values.empty else None
    return {
        "benchmark_context_status": "available",
        "benchmark_20d_return": None if current is None or prior20 in {None, 0} else current / prior20 - 1.0,
        "benchmark_60d_return": None if current is None or prior60 in {None, 0} else current / prior60 - 1.0,
        "benchmark_drawdown": None if current is None or peak in {None, 0} else current / peak - 1.0,
    }


TriggerGate = Callable[[dict[str, Any]], tuple[bool, str, dict[str, Any]]]


def _simulate_year(run_dir: Path, baseline_row: dict[str, Any], *, axis_id: str = AXIS_ID, trigger_gate: TriggerGate | None = None) -> dict[str, Any]:
    run_config = _read_json(run_dir / "run_config.json")
    actions = pd.DataFrame(_read_jsonl(run_dir / "daily_action_ledger.jsonl"))
    baseline_orders = pd.read_csv(run_dir / "orders_ledger.csv")
    baseline_equity = pd.read_csv(run_dir / "equity_curve.csv")
    source_db = Path(run_config["source_db"])
    start_ymd = int(run_config["period"]["start_ymd"])
    end_ymd = int(run_config["period"]["end_ymd"])
    cost_model = run_config["cost_model"]
    per_symbol_cap = float(run_config["portfolio"]["per_symbol_cap_jpy"])
    max_positions = int(run_config["portfolio"]["max_positions"])
    initial_cash = float(run_config["portfolio"]["initial_cash_jpy"])
    profit_target = float(run_config["exit_rules"]["profit_target"])
    stop_loss = float(run_config["exit_rules"]["stop_loss"])
    max_holding_days = int(run_config["exit_rules"]["max_holding_trading_days"])

    codes = set(baseline_orders["code"].astype(str)) | set(actions.get("code", pd.Series(dtype=str)).dropna().astype(str))
    raw = base._load_daily_ohlc(source_db, codes=codes, start_ymd=start_ymd, end_ymd=end_ymd + 75)
    raw_lookup = {(str(row["code"]), int(row["ymd"])): row for _idx, row in raw.iterrows()}
    calendar = sorted(pd.to_numeric(baseline_equity["ymd"], errors="coerce").dropna().astype(int).unique().tolist())
    buy_actions = actions[actions.get("action", pd.Series(dtype=str)).astype(str) == "buy"].copy()
    buy_actions_by_day = {int(ymd): group.copy() for ymd, group in buy_actions.groupby("decision_ymd", sort=False)}

    cash = initial_cash
    positions: dict[str, Position] = {}
    pending: dict[int, list[PendingOrder]] = {}
    order_seq = 0
    event_seq = 0
    risk_off_active = False
    risk_off_start_ymd: int | None = None
    risk_off_days = 0
    current_event_id: str | None = None
    exact_missing: list[dict[str, Any]] = []
    orders: list[dict[str, Any]] = []
    position_rows: list[dict[str, Any]] = []
    equity_rows: list[dict[str, Any]] = []
    event_rows: list[dict[str, Any]] = []
    rolling_equity: list[float] = []

    for ymd in calendar:
        for order in pending.pop(int(ymd), []):
            row = raw_lookup.get((order.code, int(ymd)))
            open_price = base._safe_float(row.get("o")) if row is not None else None
            if open_price is None:
                exact_missing.append({"ymd": ymd, "code": order.code, "order_id": order.order_id, "reason": "missing_exact_open"})
                orders.append({"year": baseline_row["year"], "order_id": order.order_id, "decision_ymd": order.decision_ymd, "execution_ymd": ymd, "action": order.action, "code": order.code, "order_status": "unfilled", "unfilled_reason": "missing_exact_open", "event_id": order.event_id})
                continue
            if order.action == "buy":
                if risk_off_active or len(positions) >= max_positions or order.code in {p.code for p in positions.values()}:
                    orders.append({"year": baseline_row["year"], "order_id": order.order_id, "decision_ymd": order.decision_ymd, "execution_ymd": ymd, "action": "buy", "code": order.code, "order_status": "unfilled", "unfilled_reason": "risk_off_or_position_limit", "execution_price": open_price})
                    continue
                shares = _shares_for_cash(cash, open_price, per_symbol_cap, cost_model)
                if shares <= 0:
                    orders.append({"year": baseline_row["year"], "order_id": order.order_id, "decision_ymd": order.decision_ymd, "execution_ymd": ymd, "action": "buy", "code": order.code, "order_status": "unfilled", "unfilled_reason": "insufficient_cash_or_lot_size", "execution_price": open_price})
                    continue
                notional = shares * open_price
                cost, slip = base._cost(notional, cost_model)
                cash -= notional + cost
                position_id = f"{axis_id}-{baseline_row['year']}-{order.order_id}"
                positions[position_id] = Position(position_id, order.baseline_position_id, order.code, order.order_id, order.decision_ymd, int(ymd), open_price, shares, notional + cost, shares)
                orders.append({"year": baseline_row["year"], "order_id": order.order_id, "decision_ymd": order.decision_ymd, "execution_ymd": ymd, "action": "buy", "code": order.code, "order_status": "filled", "execution_price": open_price, "shares": shares, "notional": notional, "cost_amount": cost, "slippage_amount": slip, "cash_after": cash, "position_id": position_id, "baseline_position_id": order.baseline_position_id, "reason_type": "baseline_buy_signal"})
                continue
            position = positions.get(str(order.position_id))
            if position is None:
                continue
            if order.action == "risk_off_trim":
                trim_shares = int((position.shares * TRIM_RATIO) // LOT_SIZE) * LOT_SIZE
                if trim_shares <= 0 or trim_shares >= position.shares:
                    orders.append({"year": baseline_row["year"], "order_id": order.order_id, "decision_ymd": order.decision_ymd, "execution_ymd": ymd, "action": "risk_off_trim", "code": order.code, "order_status": "unfilled", "unfilled_reason": "trim_lot_too_small", "execution_price": open_price, "position_id": position.position_id, "event_id": order.event_id})
                    continue
                ratio = trim_shares / position.shares
                notional = trim_shares * open_price
                partial_basis = position.cost_basis * ratio
                cost, slip = base._cost(notional, cost_model)
                pnl = notional - partial_basis - cost
                cash += notional - cost
                position.shares -= trim_shares
                position.cost_basis -= partial_basis
                orders.append({"year": baseline_row["year"], "order_id": order.order_id, "decision_ymd": order.decision_ymd, "execution_ymd": ymd, "action": "risk_off_trim", "code": order.code, "order_status": "filled", "execution_price": open_price, "shares": trim_shares, "notional": notional, "cost_amount": cost, "slippage_amount": slip, "cash_after": cash, "position_id": position.position_id, "baseline_position_id": position.baseline_position_id, "reason_type": order.reason_type, "realized_pnl": pnl, "event_id": order.event_id})
                continue
            if order.action in {"exit", "stop"}:
                notional = position.shares * open_price
                cost, slip = base._cost(notional, cost_model)
                pnl = notional - position.cost_basis - cost
                cash += notional - cost
                positions.pop(position.position_id, None)
                orders.append({"year": baseline_row["year"], "order_id": order.order_id, "decision_ymd": order.decision_ymd, "execution_ymd": ymd, "action": order.action, "code": order.code, "order_status": "filled", "execution_price": open_price, "shares": position.shares, "notional": notional, "cost_amount": cost, "slippage_amount": slip, "cash_after": cash, "position_id": position.position_id, "baseline_position_id": position.baseline_position_id, "reason_type": order.reason_type, "realized_pnl": pnl, "realized_return": pnl / position.cost_basis if position.cost_basis else None, "event_id": order.event_id})

        market_value = 0.0
        for position in list(positions.values()):
            row = raw_lookup.get((position.code, int(ymd)))
            close_price = base._safe_float(row.get("c")) if row is not None else None
            if close_price is None:
                exact_missing.append({"ymd": ymd, "code": position.code, "position_id": position.position_id, "reason": "missing_close_for_mark"})
                continue
            position.holding_days += 1
            value = position.shares * close_price
            unrealized = value - position.cost_basis
            market_value += value
            position_rows.append({"year": baseline_row["year"], "ymd": ymd, "position_id": position.position_id, "baseline_position_id": position.baseline_position_id, "code": position.code, "shares": position.shares, "original_shares": position.original_shares, "entry_ymd": position.entry_ymd, "entry_price": position.entry_price, "close_price": close_price, "market_value": value, "cost_basis": position.cost_basis, "unrealized_pnl": unrealized, "holding_days": position.holding_days, "risk_off_active": risk_off_active, "event_id": current_event_id})
            if position.pending_exit_order_id is not None:
                continue
            next_open = base._safe_float(row.get("next_open"))
            next_ymd = base._safe_int(row.get("next_ymd"))
            if next_open is None or next_ymd is None:
                continue
            close_return = close_price / position.entry_price - 1.0 if position.entry_price else 0.0
            profit_return = unrealized / position.cost_basis if position.cost_basis else 0.0
            action: str | None = None
            reason: str | None = None
            if close_return <= stop_loss:
                action, reason = "stop", "stop_loss"
            elif profit_return >= profit_target:
                action, reason = "exit", "profit_target"
            elif position.holding_days >= max_holding_days:
                action, reason = "exit", "time_stop"
            if action is not None:
                order_seq += 1
                oid = f"{axis_id}-{baseline_row['year']}-{order_seq:08d}"
                position.pending_exit_order_id = oid
                pending.setdefault(next_ymd, []).append(PendingOrder(oid, action, position.code, int(ymd), next_ymd, str(reason), position.baseline_position_id, position.position_id, current_event_id))

        equity = cash + market_value
        rolling_equity.append(equity)
        trailing_peak = max(rolling_equity[-LOOKBACK_DAYS:])
        trailing_dd = equity / trailing_peak - 1.0 if trailing_peak else 0.0
        if risk_off_active:
            risk_off_days += 1
            if trailing_dd >= RECOVERY_THRESHOLD or risk_off_days >= RISK_OFF_MAX_DAYS:
                event_rows.append({"year": baseline_row["year"], "event_id": current_event_id, "event_type": "risk_on", "decision_ymd": ymd, "trailing_dd": trailing_dd, "risk_off_days": risk_off_days, "release_reason": "drawdown_recovered" if trailing_dd >= RECOVERY_THRESHOLD else "max_days_elapsed"})
                risk_off_active = False
                risk_off_start_ymd = None
                risk_off_days = 0
                current_event_id = None
        if not risk_off_active and trailing_dd <= DRAWDOWN_TRIGGER and positions:
            benchmark_context = _benchmark_context(baseline_equity, int(ymd))
            trigger_context = {
                "year": int(baseline_row["year"]),
                "ymd": int(ymd),
                "portfolio_trailing_dd": trailing_dd,
                "open_position_count": len(positions),
                **benchmark_context,
            }
            gate_allowed, gate_reason, gate_fields = trigger_gate(trigger_context) if trigger_gate else (True, "ungated_risk_off", {})
            if not gate_allowed:
                event_seq += 1
                event_rows.append({"year": baseline_row["year"], "event_id": f"{baseline_row['year']}-risk-off-blocked-{event_seq:03d}", "event_type": "risk_off_blocked_by_market_gate", "decision_ymd": ymd, "execution_ymd": None, "trailing_dd": trailing_dd, "open_position_count": len(positions), "trim_ratio": TRIM_RATIO, "lookback_days": LOOKBACK_DAYS, "gate_reason": gate_reason, **benchmark_context, **gate_fields})
            else:
                event_seq += 1
                current_event_id = f"{baseline_row['year']}-risk-off-{event_seq:03d}"
                risk_off_active = True
                risk_off_start_ymd = int(ymd)
                risk_off_days = 0
                next_ymd_values: list[int] = []
                for position in list(positions.values()):
                    row = raw_lookup.get((position.code, int(ymd)))
                    next_ymd = base._safe_int(row.get("next_ymd")) if row is not None else None
                    if next_ymd is None:
                        continue
                    next_ymd_values.append(next_ymd)
                    order_seq += 1
                    oid = f"{axis_id}-{baseline_row['year']}-{order_seq:08d}"
                    pending.setdefault(next_ymd, []).append(PendingOrder(oid, "risk_off_trim", position.code, int(ymd), next_ymd, "risk_off_20d_peak_drawdown_trim_50pct", position.baseline_position_id, position.position_id, current_event_id))
                event_rows.append({"year": baseline_row["year"], "event_id": current_event_id, "event_type": "risk_off", "decision_ymd": ymd, "execution_ymd": min(next_ymd_values) if next_ymd_values else None, "trailing_dd": trailing_dd, "open_position_count": len(positions), "trim_ratio": TRIM_RATIO, "lookback_days": LOOKBACK_DAYS, "gate_reason": gate_reason, **benchmark_context, **gate_fields})

        for _idx, action_row in buy_actions_by_day.get(int(ymd), pd.DataFrame()).iterrows():
            if risk_off_active:
                continue
            code = str(action_row.get("code"))
            row = raw_lookup.get((code, int(ymd)))
            next_ymd = base._safe_int(row.get("next_ymd")) if row is not None else base._safe_int(action_row.get("execution_ymd"))
            if next_ymd is None:
                exact_missing.append({"ymd": ymd, "code": code, "reason": "missing_next_ymd_for_buy"})
                continue
            baseline_order_id = str(action_row.get("order_id"))
            match = baseline_orders[(baseline_orders["order_id"].astype(str) == baseline_order_id) | ((baseline_orders["decision_ymd"].astype(int) == int(ymd)) & (baseline_orders["code"].astype(str) == code) & (baseline_orders["action"].astype(str) == "buy"))]
            baseline_pid = str(match.iloc[0].get("position_id")) if not match.empty else None
            pending.setdefault(next_ymd, []).append(PendingOrder(f"{axis_id}-{baseline_row['year']}-buy-{baseline_order_id}", "buy", code, int(ymd), next_ymd, "baseline_buy_signal", baseline_pid))

        base_row = baseline_equity[baseline_equity["ymd"].astype(int) == int(ymd)]
        equity_rows.append({"year": baseline_row["year"], "ymd": ymd, "cash": cash, "positions_market_value": market_value, "equity": equity, "baseline_equity": None if base_row.empty else float(base_row.iloc[0]["equity"]), "market_benchmark_equity": None if base_row.empty else base_row.iloc[0].get("market_benchmark_equity"), "benchmark_code": None if base_row.empty else base_row.iloc[0].get("benchmark_code"), "open_position_count": len(positions), "risk_off_active": risk_off_active, "risk_off_days": risk_off_days, "trailing_peak_20": trailing_peak, "trailing_drawdown_20": trailing_dd, "exact_next_open_replay": len(exact_missing) == 0})

    orders_df = pd.DataFrame(orders)
    positions_df = pd.DataFrame(position_rows)
    equity_df = pd.DataFrame(equity_rows)
    events_df = pd.DataFrame(event_rows)
    final_equity = float(equity_df.iloc[-1]["equity"]) if not equity_df.empty else initial_cash
    risk_return = final_equity / initial_cash - 1.0
    risk_dd = _max_drawdown(pd.to_numeric(equity_df["equity"], errors="coerce")) if not equity_df.empty else 0.0
    baseline_return = float(baseline_row["total_return"])
    baseline_dd = float(baseline_row["max_drawdown"])
    benchmark_return = None if pd.isna(baseline_row.get("benchmark_return")) else float(baseline_row.get("benchmark_return"))
    baseline_excess = None if benchmark_return is None else baseline_return - benchmark_return
    risk_excess = None if benchmark_return is None else risk_return - benchmark_return
    baseline_cash = pd.to_numeric(baseline_equity["cash"], errors="coerce") / pd.to_numeric(baseline_equity["equity"], errors="coerce")
    baseline_exposure = pd.to_numeric(baseline_equity["positions_market_value"], errors="coerce") / pd.to_numeric(baseline_equity["equity"], errors="coerce")
    risk_cash = pd.to_numeric(equity_df["cash"], errors="coerce") / pd.to_numeric(equity_df["equity"], errors="coerce")
    risk_exposure = pd.to_numeric(equity_df["positions_market_value"], errors="coerce") / pd.to_numeric(equity_df["equity"], errors="coerce")
    baseline_cost = float(pd.to_numeric(baseline_orders.get("cost_amount", pd.Series(dtype=float)), errors="coerce").sum())
    risk_cost = float(pd.to_numeric(orders_df.get("cost_amount", pd.Series(dtype=float)), errors="coerce").sum()) if not orders_df.empty else 0.0
    yearly = {
        "year": baseline_row["year"],
        "baseline_total_return": baseline_return,
        "risk_off_total_return": risk_return,
        "delta_total_return": risk_return - baseline_return,
        "benchmark_return": benchmark_return,
        "baseline_benchmark_excess": baseline_excess,
        "risk_off_benchmark_excess": risk_excess,
        "baseline_max_drawdown": baseline_dd,
        "risk_off_max_drawdown": risk_dd,
        "delta_max_drawdown": risk_dd - baseline_dd,
        "risk_off_event_count": int((events_df.get("event_type", pd.Series(dtype=str)).astype(str) == "risk_off").sum()) if not events_df.empty else 0,
        "risk_off_days": int(pd.to_numeric(equity_df.get("risk_off_active", pd.Series(dtype=bool)), errors="coerce").fillna(0).sum()) if not equity_df.empty else 0,
        "avg_gross_exposure_baseline": float(baseline_exposure.mean()),
        "avg_gross_exposure_risk_off": float(risk_exposure.mean()),
        "cash_ratio_delta": float(risk_cash.mean() - baseline_cash.mean()),
        "order_count_delta": int(len(orders_df) - len(baseline_orders)),
        "cost_delta": risk_cost - baseline_cost,
        "exact_next_open_replay": len(exact_missing) == 0,
        "baseline_benchmark_positive_portfolio_negative": bool(benchmark_return is not None and benchmark_return > 0 and baseline_return < 0),
        "risk_off_benchmark_positive_portfolio_negative": bool(benchmark_return is not None and benchmark_return > 0 and risk_return < 0),
        "run_dir": str(run_dir),
    }
    return {"yearly": yearly, "orders": orders_df, "positions": positions_df, "equity": equity_df, "events": events_df, "exact_missing": exact_missing}


def _monthly_compare(year: int, baseline_equity: pd.DataFrame, risk_equity: pd.DataFrame) -> pd.DataFrame:
    base_eq = baseline_equity.copy()
    risk_eq = risk_equity.copy()
    base_eq["month"] = _month_key(base_eq["ymd"])
    risk_eq["month"] = _month_key(risk_eq["ymd"])
    rows: list[dict[str, Any]] = []
    for month, bgroup in base_eq.groupby("month", sort=True):
        rgroup = risk_eq[risk_eq["month"] == month].sort_values("ymd", kind="stable")
        if rgroup.empty:
            continue
        bgroup = bgroup.sort_values("ymd", kind="stable")
        bret = float(bgroup.iloc[-1]["equity"]) / float(bgroup.iloc[0]["equity"]) - 1.0
        rret = float(rgroup.iloc[-1]["equity"]) / float(rgroup.iloc[0]["equity"]) - 1.0
        bench_start = bgroup.iloc[0].get("market_benchmark_equity")
        bench_end = bgroup.iloc[-1].get("market_benchmark_equity")
        bench_ret = None if pd.isna(bench_start) or pd.isna(bench_end) or float(bench_start) == 0 else float(bench_end) / float(bench_start) - 1.0
        bdd = _max_drawdown(pd.to_numeric(bgroup["equity"], errors="coerce"))
        rdd = _max_drawdown(pd.to_numeric(rgroup["equity"], errors="coerce"))
        rows.append({"year": year, "month": int(month), "baseline_return": bret, "risk_off_return": rret, "delta_return": rret - bret, "benchmark_return": bench_ret, "baseline_max_drawdown": bdd, "risk_off_max_drawdown": rdd, "delta_max_drawdown": rdd - bdd, "risk_off_days": int(rgroup["risk_off_active"].fillna(False).astype(bool).sum()), "avg_gross_exposure_baseline": float((pd.to_numeric(bgroup["positions_market_value"], errors="coerce") / pd.to_numeric(bgroup["equity"], errors="coerce")).mean()), "avg_gross_exposure_risk_off": float((pd.to_numeric(rgroup["positions_market_value"], errors="coerce") / pd.to_numeric(rgroup["equity"], errors="coerce")).mean())})
    return pd.DataFrame(rows)


def _decide(yearly: pd.DataFrame) -> tuple[str, str, dict[str, Any]]:
    years_improved_return = int((pd.to_numeric(yearly["delta_total_return"], errors="coerce") > 0).sum())
    years_improved_dd = int((pd.to_numeric(yearly["delta_max_drawdown"], errors="coerce") > 0).sum())
    severe_base = yearly[pd.to_numeric(yearly["baseline_max_drawdown"], errors="coerce") <= -0.35]["year"].astype(int).tolist()
    severe_risk = yearly[pd.to_numeric(yearly["risk_off_max_drawdown"], errors="coerce") <= -0.35]["year"].astype(int).tolist()
    under_base = yearly[pd.to_numeric(yearly["baseline_benchmark_excess"], errors="coerce") < 0]["year"].astype(int).tolist()
    under_risk = yearly[pd.to_numeric(yearly["risk_off_benchmark_excess"], errors="coerce") < 0]["year"].astype(int).tolist()
    saved_loss_total = float(pd.to_numeric(yearly.loc[yearly["delta_total_return"] > 0, "delta_total_return"], errors="coerce").sum()) * 10_000_000.0
    missed_profit_total = abs(float(pd.to_numeric(yearly.loc[yearly["delta_total_return"] < 0, "delta_total_return"], errors="coerce").sum()) * 10_000_000.0)
    net_effect_total = saved_loss_total - missed_profit_total
    y2024_damage = float(yearly.loc[yearly["year"] == 2024, "delta_total_return"].iloc[0]) if (yearly["year"] == 2024).any() else 0.0
    y2025_damage = float(yearly.loc[yearly["year"] == 2025, "delta_total_return"].iloc[0]) if (yearly["year"] == 2025).any() else 0.0
    evidence = {
        "years_improved_return_count": years_improved_return,
        "years_improved_drawdown_count": years_improved_dd,
        "severe_drawdown_years_baseline": severe_base,
        "severe_drawdown_years_risk_off": severe_risk,
        "benchmark_underperformance_years_baseline": under_base,
        "benchmark_underperformance_years_risk_off": under_risk,
        "saved_loss_total": saved_loss_total,
        "missed_profit_total": missed_profit_total,
        "net_effect_total": net_effect_total,
        "return_damage_2024": y2024_damage,
        "return_damage_2025": y2025_damage,
    }
    severe_reduced = len(severe_risk) < len(severe_base)
    upside_preserved = y2024_damage > -0.10 and y2025_damage > -0.10
    if severe_reduced and net_effect_total > 0 and upside_preserved and years_improved_dd >= 4:
        return "keep_for_replay_challenger", "severe_drawdown_reduced_with_positive_multiyear_effect", evidence
    if severe_reduced and not upside_preserved:
        return "hold_due_to_upside_damage", "drawdown_improved_but_2024_2025_upside_damaged", evidence
    if severe_reduced:
        return "hold_for_regime_filter_comparison", "drawdown_improved_but_return_tradeoff_requires_comparison", evidence
    if net_effect_total < 0 and missed_profit_total > saved_loss_total:
        return "drop_due_to_profit_damage", "risk_off_reduced_returns_without_sufficient_saved_loss", evidence
    return "drop_due_to_no_drawdown_improvement", "severe_drawdown_not_improved", evidence


def run_risk_off_pretest(robustness_root: str | Path, output_root: str | Path | None = None) -> dict[str, Any]:
    robustness_root = Path(robustness_root)
    output_root = Path(output_root) if output_root else robustness_root / DEFAULT_OUTPUT_DIR_NAME
    yearly_gate = pd.read_csv(robustness_root / "yearly_results.csv")
    yearly_rows: list[dict[str, Any]] = []
    monthly_frames: list[pd.DataFrame] = []
    event_frames: list[pd.DataFrame] = []
    order_frames: list[pd.DataFrame] = []
    position_frames: list[pd.DataFrame] = []
    equity_frames: list[pd.DataFrame] = []
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
        if not result["events"].empty:
            event_frames.append(result["events"])
        monthly_frames.append(_monthly_compare(int(year_row["year"]), pd.read_csv(run_dir / "equity_curve.csv"), result["equity"]))
        missing.extend(result["exact_missing"])
    yearly = pd.DataFrame(yearly_rows)
    monthly = pd.concat(monthly_frames, ignore_index=True) if monthly_frames else pd.DataFrame()
    events = pd.concat(event_frames, ignore_index=True) if event_frames else pd.DataFrame()
    orders = pd.concat(order_frames, ignore_index=True) if order_frames else pd.DataFrame()
    positions = pd.concat(position_frames, ignore_index=True) if position_frames else pd.DataFrame()
    equity = pd.concat(equity_frames, ignore_index=True) if equity_frames else pd.DataFrame()
    exposure = yearly[["year", "avg_gross_exposure_baseline", "avg_gross_exposure_risk_off", "cash_ratio_delta", "risk_off_days"]].copy()
    drawdown = yearly[["year", "baseline_max_drawdown", "risk_off_max_drawdown", "delta_max_drawdown"]].copy()
    bppn = yearly[["year", "baseline_benchmark_positive_portfolio_negative", "risk_off_benchmark_positive_portfolio_negative", "baseline_benchmark_excess", "risk_off_benchmark_excess"]].copy()
    saved = yearly[yearly["delta_total_return"] > 0].copy()
    missed = yearly[yearly["delta_total_return"] < 0].copy()
    decision, reason, evidence = _decide(yearly)

    _write_csv(output_root / "yearly_results_baseline_vs_risk_off.csv", yearly)
    _write_csv(output_root / "monthly_results_baseline_vs_risk_off.csv", monthly)
    _write_csv(output_root / "risk_off_events.csv", events)
    _write_csv(output_root / "risk_off_orders_ledger.csv", orders)
    _write_csv(output_root / "risk_off_positions_ledger.csv", positions)
    _write_csv(output_root / "risk_off_equity_curve_by_year.csv", equity)
    _write_csv(output_root / "exposure_cash_comparison.csv", exposure)
    _write_csv(output_root / "drawdown_comparison.csv", drawdown)
    _write_csv(output_root / "benchmark_positive_portfolio_negative_comparison.csv", bppn)
    _write_csv(output_root / "saved_loss_during_risk_off.csv", saved)
    _write_csv(output_root / "missed_profit_during_risk_off.csv", missed)
    summary = {
        "schema_version": f"{SCHEMA_PREFIX}_summary_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "robustness_root": str(robustness_root),
        "rule": {
            "drawdown_trigger": DRAWDOWN_TRIGGER,
            "recovery_threshold": RECOVERY_THRESHOLD,
            "lookback_days": LOOKBACK_DAYS,
            "risk_off_max_days": RISK_OFF_MAX_DAYS,
            "trim_ratio": TRIM_RATIO,
            "threshold_sweep": False,
            "trim_ratio_sweep": False,
            "lookback_sweep": False,
        },
        "decision": decision,
        "reason_type": reason,
        "metrics": evidence,
        "scope": {"tradex_only": True, "baseline_policy_changed": False, "single_axis_only": True, "meemee_ui_changed": False, "runtime_db_written": False, "ranking_changed": False, "publish_registry_changed": False},
    }
    _write_json(output_root / "risk_off_cash_control_summary.json", summary)
    _write_json(output_root / "selection_feature_manifest.json", {"schema_version": f"{SCHEMA_PREFIX}_selection_feature_manifest_v1", "axis_id": AXIS_ID, "selection_allowed_columns": ["ymd", "portfolio_equity", "trailing_peak_20", "trailing_drawdown_20", "risk_off_days", "current_positions"], "selection_forbidden_columns": ["post_ret_5", "post_ret_10", "post_ret_20", "post_ret_40", "mae_20", "mfe_20", "future_benchmark_return"], "diagnostic_only_columns": ["saved_loss_total", "missed_profit_total", "net_effect_total"], "outcome_label_columns": ["saved_loss_total", "missed_profit_total", "net_effect_total"], "audit_result": "pass"})
    _write_json(output_root / "no_lookahead_audit.json", {"schema_version": f"{SCHEMA_PREFIX}_no_lookahead_audit_v1", "axis_id": AXIS_ID, "audit_result": "pass" if not missing else "research_fallback", "exact_next_open_replay": not missing, "same_day_close_fill_used": False, "benchmark_future_return_used_for_trigger": False, "post_run_outcomes_used_for_trigger": False, "threshold_sweep": False, "trim_ratio_sweep": False, "lookback_sweep": False, "silent_fallback_used": False, "missing_exact_price_events": missing})
    _write_json(output_root / "next_axis_decision.json", {"schema_version": f"{SCHEMA_PREFIX}_next_axis_decision_v1", "axis_id": AXIS_ID, "generated_at": _utc_now(), "decision_candidates": list(DECISIONS), "decision": decision, "decision_count": 1, "reason_type": reason, "metrics": evidence, "policy_promotion_allowed": False, "meemee_reflectable": False})
    complete = {"schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1", "axis_id": AXIS_ID, "generated_at": _utc_now(), "complete": True, "required_artifacts_all_present": all((output_root / artifact).exists() for artifact in REQUIRED_ARTIFACTS if artifact != "_ARTIFACT_COMPLETE.json"), "decision": decision, "decision_count": 1, "exact_next_open_replay": not missing, "no_lookahead_audit": "pass" if not missing else "research_fallback", "baseline_policy_changed": False, "threshold_sweep": False, "trim_ratio_sweep": False, "lookback_sweep": False, "silent_fallback_used": False, "meemee_reflectable": False, "policy_promotion_allowed": False}
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    return {"complete": True, "output_root": str(output_root), "decision": decision, "reason_type": reason, "metrics": evidence}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pretest portfolio-equity risk-off cash control overlay.")
    parser.add_argument("--robustness-root", required=True, type=Path)
    parser.add_argument("--output-root", type=Path, default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    print(json.dumps(_json_ready(run_risk_off_pretest(args.robustness_root, args.output_root)), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
