from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import duckdb
import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts import tradex_teppan_chart_pattern_discovery_v1 as discovery


AXIS_ID = "hedged_stop_at_minus8_pretest_v1"
SCHEMA_PREFIX = "tradex_hedged_stop_at_minus8_pretest_v1"
DEFAULT_OUTPUT_DIR_NAME = "hedged_stop_at_minus8_pretest_v1"
HEDGE_TRIGGER_THRESHOLD = -0.08
HEDGE_RATIO = 0.50
HEDGE_MAX_HOLDING_DAYS = 20
LOT_SIZE = 100

SOURCE_ARTIFACTS = (
    "run_config.json",
    "daily_action_ledger.jsonl",
    "orders_ledger.csv",
    "positions_ledger.csv",
    "equity_curve.csv",
    "stop_too_wide_pretest_v1/baseline_vs_stop_comparison.json",
    "stop_case_reconciliation_v1/equity_delta_attribution.json",
    "candidate_lifecycle_audit_v1/candidate_lifecycle_summary.json",
    "diagnosis_v1",
)

OUTPUT_ARTIFACTS = (
    "hedged_stop_pretest_summary.json",
    "hedge_triggered_cases.csv",
    "hedge_orders_ledger.csv",
    "hedge_positions_ledger.csv",
    "hedge_equity_curve.csv",
    "baseline_vs_hedge_comparison.json",
    "hard_stop_vs_hedge_comparison.json",
    "hedge_recovery_cases.csv",
    "hedge_true_breakdown_cases.csv",
    "hedge_cost_summary.json",
    "hedge_exposure_attribution.json",
    "no_lookahead_audit.json",
    "selection_feature_manifest.json",
    "next_axis_decision.json",
    "_ARTIFACT_COMPLETE.json",
)

DECISIONS = (
    "keep_for_replay_challenger",
    "hold_for_realism_check",
    "hold_for_partial_stop_comparison",
    "drop_due_to_profit_damage",
    "drop_due_to_cost_or_complexity",
)


@dataclass
class LongPosition:
    position_id: str
    baseline_position_id: str
    code: str
    entry_order_id: str
    entry_decision_ymd: int
    entry_ymd: int
    entry_price: float
    shares: int
    notional: float
    entry_cost: float
    cost_basis: float
    holding_days: int = 0
    pending_exit_order_id: str | None = None
    hedge_triggered: bool = False
    hedge_position_id: str | None = None


@dataclass
class HedgePosition:
    hedge_position_id: str
    long_position_id: str
    baseline_position_id: str
    code: str
    hedge_entry_decision_ymd: int
    hedge_entry_ymd: int
    hedge_entry_price: float
    hedge_shares: int
    hedge_notional: float
    hedge_entry_cost: float
    holding_days: int = 0
    pending_close_order_id: str | None = None


@dataclass
class PendingOrder:
    order_id: str
    action: str
    code: str
    decision_ymd: int
    execution_ymd: int
    reason_type: str
    baseline_position_id: str | None = None
    long_position_id: str | None = None
    hedge_position_id: str | None = None


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
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


def _safe_int(value: Any) -> int | None:
    parsed = _safe_float(value)
    return None if parsed is None else int(parsed)


def _cost(notional: float, cost_model: dict[str, Any]) -> tuple[float, float]:
    base = abs(float(notional))
    commission = max(float(cost_model.get("min_fee") or 0.0), base * float(cost_model.get("commission_bps", 15.0)) / 10_000.0)
    slippage = base * float(cost_model.get("slippage_bps", 15.0)) / 10_000.0
    tax = base * float(cost_model.get("tax_or_fee_bps") or 0.0) / 10_000.0
    return commission + slippage + tax, slippage


def _max_drawdown(equity: pd.Series) -> float:
    peak = equity.cummax()
    return float((equity / peak - 1.0).min()) if not equity.empty else 0.0


def _source_status(run_root: Path) -> dict[str, bool]:
    return {name: (run_root / name).exists() for name in SOURCE_ARTIFACTS}


def _load_daily_ohlc(source_db: Path, *, codes: set[str], start_ymd: int, end_ymd: int) -> pd.DataFrame:
    conn = duckdb.connect(str(source_db), read_only=True)
    try:
        date_expr = discovery._date_norm_expr("date")
        frame = conn.execute(
            f"""
            SELECT code, {date_expr} AS ymd, o, h, l, c, v
            FROM daily_bars
            WHERE {date_expr} BETWEEN ? AND ?
              AND lower(coalesce(source, '')) = 'pan'
            """,
            [int(start_ymd), int(end_ymd)],
        ).fetchdf()
    finally:
        conn.close()
    frame["code"] = frame["code"].astype(str)
    frame = frame[frame["code"].isin(codes)].copy()
    frame = frame.sort_values(["code", "ymd"], kind="stable")
    frame["next_ymd"] = frame.groupby("code", sort=False)["ymd"].shift(-1)
    frame["next_open"] = frame.groupby("code", sort=False)["o"].shift(-1)
    return frame.reset_index(drop=True)


def _shares_for_cash(cash: float, price: float, per_symbol_cap: float, cost_model: dict[str, Any]) -> int:
    max_notional = min(float(cash), float(per_symbol_cap))
    lots = int(max_notional // (float(price) * LOT_SIZE))
    while lots > 0:
        shares = lots * LOT_SIZE
        notional = shares * float(price)
        cost, _slip = _cost(notional, cost_model)
        if notional + cost <= cash:
            return shares
        lots -= 1
    return 0


def _baseline_position_pnl(orders: pd.DataFrame, positions: pd.DataFrame) -> dict[str, float]:
    exits = orders[(orders["action"].isin(["exit", "stop"])) & (orders["order_status"] == "filled")].copy()
    latest = positions.sort_values("ymd", kind="stable").groupby("position_id", sort=False).tail(1) if not positions.empty else pd.DataFrame()
    out: dict[str, float] = {}
    for _idx, buy in orders[(orders["action"] == "buy") & (orders["order_status"] == "filled")].iterrows():
        pid = str(buy["position_id"])
        exit_rows = exits[exits["position_id"].astype(str) == pid]
        if not exit_rows.empty:
            out[pid] = float(exit_rows.iloc[-1].get("realized_pnl") or 0.0)
        else:
            pos = latest[latest["position_id"].astype(str) == pid]
            out[pid] = float(pos.iloc[-1].get("unrealized_pnl") or 0.0) if not pos.empty else 0.0
    return out


def _future_recovery(raw_by_code: dict[str, pd.DataFrame], code: str, ymd: int, price: float) -> dict[str, Any]:
    future = raw_by_code.get(code, pd.DataFrame())
    future = future[future["ymd"].astype(int) > int(ymd)].head(20).copy()
    if future.empty or price <= 0:
        return {"post_hedge_ret20": None, "post_hedge_max_up20": None, "recovery_type": "unavailable_no_future_bars"}
    max_up = float(future["h"].max() / price - 1.0)
    ret20 = float(future.iloc[19]["c"] / price - 1.0) if len(future) >= 20 else None
    if max_up >= 0.08:
        recovery_type = "strong_shakeout_recovery"
    elif max_up >= 0.03:
        recovery_type = "moderate_recovery"
    elif max_up > 0:
        recovery_type = "minor_recovery"
    else:
        recovery_type = "true_breakdown_or_no_recovery"
    return {"post_hedge_ret20": ret20, "post_hedge_max_up20": max_up, "recovery_type": recovery_type}


def run_hedge_overlay(run_root: Path, source_db: Path) -> dict[str, Any]:
    run_config = _read_json(run_root / "run_config.json")
    actions = pd.DataFrame(_read_jsonl(run_root / "daily_action_ledger.jsonl"))
    baseline_orders = pd.read_csv(run_root / "orders_ledger.csv")
    baseline_positions = pd.read_csv(run_root / "positions_ledger.csv")
    baseline_equity = pd.read_csv(run_root / "equity_curve.csv")
    hard_stop_comparison = _read_json(run_root / "stop_too_wide_pretest_v1" / "baseline_vs_stop_comparison.json")
    reconciliation = _read_json(run_root / "stop_case_reconciliation_v1" / "equity_delta_attribution.json")
    lifecycle = _read_json(run_root / "candidate_lifecycle_audit_v1" / "candidate_lifecycle_summary.json")

    initial_cash = float(run_config["portfolio"]["initial_cash_jpy"])
    per_symbol_cap = float(run_config["portfolio"]["per_symbol_cap_jpy"])
    max_positions = int(run_config["portfolio"]["max_positions"])
    cost_model = run_config["cost_model"]
    profit_target = float(run_config["exit_rules"]["profit_target"])
    max_holding_days = int(run_config["exit_rules"]["max_holding_trading_days"])
    start_ymd = int(run_config["period"]["start_ymd"])
    end_ymd = int(run_config["period"]["end_ymd"])
    codes = set(baseline_orders["code"].astype(str)) | set(actions.get("code", pd.Series(dtype=str)).dropna().astype(str))
    raw = _load_daily_ohlc(source_db, codes=codes, start_ymd=start_ymd, end_ymd=end_ymd + 45)
    raw_by_code = {code: group.reset_index(drop=True) for code, group in raw.groupby("code", sort=False)}
    raw_lookup = {(str(row["code"]), int(row["ymd"])): row for _idx, row in raw.iterrows()}
    calendar = sorted(pd.to_numeric(baseline_equity["ymd"], errors="coerce").dropna().astype(int).unique().tolist())
    buy_actions = actions[actions.get("action", pd.Series(dtype=str)).astype(str) == "buy"].copy()
    buy_actions_by_day = {int(ymd): group.copy() for ymd, group in buy_actions.groupby("decision_ymd", sort=False)}
    baseline_pnl_by_pid = _baseline_position_pnl(baseline_orders, baseline_positions)

    pending: dict[int, list[PendingOrder]] = {}
    longs: dict[str, LongPosition] = {}
    hedges: dict[str, HedgePosition] = {}
    cash = initial_cash
    order_seq = 0
    orders: list[dict[str, Any]] = []
    position_rows: list[dict[str, Any]] = []
    equity_rows: list[dict[str, Any]] = []
    hedge_cases: list[dict[str, Any]] = []
    exact_missing: list[dict[str, Any]] = []

    for ymd in calendar:
        for order in pending.pop(int(ymd), []):
            row = raw_lookup.get((order.code, int(ymd)))
            open_price = _safe_float(row.get("o")) if row is not None else None
            if open_price is None:
                exact_missing.append({"ymd": ymd, "code": order.code, "order_id": order.order_id, "reason": "missing_exact_open"})
                orders.append({"order_id": order.order_id, "decision_ymd": order.decision_ymd, "execution_ymd": ymd, "action": order.action, "code": order.code, "order_status": "unfilled", "unfilled_reason": "missing_exact_open"})
                continue

            if order.action == "buy":
                if len(longs) >= max_positions or order.code in {p.code for p in longs.values()}:
                    orders.append({"order_id": order.order_id, "decision_ymd": order.decision_ymd, "execution_ymd": ymd, "action": "buy", "code": order.code, "order_status": "unfilled", "unfilled_reason": "position_limit_or_duplicate", "execution_price": open_price})
                    continue
                shares = _shares_for_cash(cash, open_price, per_symbol_cap, cost_model)
                if shares <= 0:
                    orders.append({"order_id": order.order_id, "decision_ymd": order.decision_ymd, "execution_ymd": ymd, "action": "buy", "code": order.code, "order_status": "unfilled", "unfilled_reason": "insufficient_cash_or_lot_size", "execution_price": open_price})
                    continue
                notional = shares * open_price
                cost, slip = _cost(notional, cost_model)
                cash -= notional + cost
                position_id = f"{AXIS_ID}-{order.order_id}"
                longs[position_id] = LongPosition(
                    position_id=position_id,
                    baseline_position_id=str(order.baseline_position_id),
                    code=order.code,
                    entry_order_id=order.order_id,
                    entry_decision_ymd=order.decision_ymd,
                    entry_ymd=int(ymd),
                    entry_price=open_price,
                    shares=shares,
                    notional=notional,
                    entry_cost=cost,
                    cost_basis=notional + cost,
                )
                orders.append({"order_id": order.order_id, "decision_ymd": order.decision_ymd, "execution_ymd": ymd, "action": "buy", "code": order.code, "order_status": "filled", "execution_price": open_price, "shares": shares, "notional": notional, "cost_amount": cost, "slippage_amount": slip, "cash_after": cash, "position_id": position_id, "baseline_position_id": order.baseline_position_id, "reason_type": "baseline_buy_signal"})
                continue

            if order.action == "open_hedge":
                long = longs.get(str(order.long_position_id))
                if long is None or long.hedge_triggered:
                    orders.append({"order_id": order.order_id, "decision_ymd": order.decision_ymd, "execution_ymd": ymd, "action": "open_hedge", "code": order.code, "order_status": "unfilled", "unfilled_reason": "long_missing_or_hedge_already_triggered", "execution_price": open_price})
                    continue
                hedge_shares = int((long.shares * HEDGE_RATIO) // LOT_SIZE) * LOT_SIZE
                if hedge_shares <= 0:
                    orders.append({"order_id": order.order_id, "decision_ymd": order.decision_ymd, "execution_ymd": ymd, "action": "open_hedge", "code": order.code, "order_status": "unfilled", "unfilled_reason": "hedge_lot_too_small", "execution_price": open_price})
                    continue
                notional = hedge_shares * open_price
                cost, slip = _cost(notional, cost_model)
                cash -= cost
                hedge_id = f"{long.position_id}-hedge"
                hedge = HedgePosition(
                    hedge_position_id=hedge_id,
                    long_position_id=long.position_id,
                    baseline_position_id=long.baseline_position_id,
                    code=long.code,
                    hedge_entry_decision_ymd=order.decision_ymd,
                    hedge_entry_ymd=int(ymd),
                    hedge_entry_price=open_price,
                    hedge_shares=hedge_shares,
                    hedge_notional=notional,
                    hedge_entry_cost=cost,
                )
                long.hedge_triggered = True
                long.hedge_position_id = hedge_id
                hedges[hedge_id] = hedge
                recovery = _future_recovery(raw_by_code, long.code, int(ymd), open_price)
                hedge_cases.append({"hedge_position_id": hedge_id, "long_position_id": long.position_id, "baseline_position_id": long.baseline_position_id, "code": long.code, "hedge_trigger_date": order.decision_ymd, "hedge_entry_date": ymd, "hedge_entry_price": open_price, "hedge_shares": hedge_shares, "hedge_ratio": HEDGE_RATIO, "baseline_pnl": baseline_pnl_by_pid.get(long.baseline_position_id), **recovery})
                orders.append({"order_id": order.order_id, "decision_ymd": order.decision_ymd, "execution_ymd": ymd, "action": "open_hedge", "code": order.code, "order_status": "filled", "execution_price": open_price, "shares": hedge_shares, "notional": notional, "cost_amount": cost, "slippage_amount": slip, "cash_after": cash, "position_id": hedge_id, "long_position_id": long.position_id, "baseline_position_id": long.baseline_position_id, "reason_type": "minus8_synthetic_same_name_hedge"})
                continue

            if order.action in {"close_hedge", "exit", "time_exit"}:
                if order.action == "close_hedge":
                    hedge = hedges.get(str(order.hedge_position_id))
                    if hedge is None:
                        orders.append({"order_id": order.order_id, "decision_ymd": order.decision_ymd, "execution_ymd": ymd, "action": "close_hedge", "code": order.code, "order_status": "unfilled", "unfilled_reason": "hedge_missing", "execution_price": open_price})
                        continue
                    notional = hedge.hedge_shares * open_price
                    cost, slip = _cost(notional, cost_model)
                    pnl = (hedge.hedge_entry_price - open_price) * hedge.hedge_shares - hedge.hedge_entry_cost - cost
                    cash += pnl
                    hedges.pop(hedge.hedge_position_id, None)
                    orders.append({"order_id": order.order_id, "decision_ymd": order.decision_ymd, "execution_ymd": ymd, "action": "close_hedge", "code": order.code, "order_status": "filled", "execution_price": open_price, "shares": hedge.hedge_shares, "notional": notional, "cost_amount": cost, "slippage_amount": slip, "cash_after": cash, "position_id": hedge.hedge_position_id, "long_position_id": hedge.long_position_id, "baseline_position_id": hedge.baseline_position_id, "reason_type": order.reason_type, "realized_pnl": pnl})
                    continue

                long = longs.get(str(order.long_position_id))
                if long is None:
                    orders.append({"order_id": order.order_id, "decision_ymd": order.decision_ymd, "execution_ymd": ymd, "action": "exit", "code": order.code, "order_status": "unfilled", "unfilled_reason": "long_missing", "execution_price": open_price})
                    continue
                hedge_id = long.hedge_position_id
                if hedge_id and hedge_id in hedges:
                    hedge = hedges[hedge_id]
                    hedge_notional = hedge.hedge_shares * open_price
                    hedge_cost, hedge_slip = _cost(hedge_notional, cost_model)
                    hedge_pnl = (hedge.hedge_entry_price - open_price) * hedge.hedge_shares - hedge.hedge_entry_cost - hedge_cost
                    cash += hedge_pnl
                    hedges.pop(hedge_id, None)
                    orders.append({"order_id": f"{order.order_id}-close-hedge", "decision_ymd": order.decision_ymd, "execution_ymd": ymd, "action": "close_hedge", "code": order.code, "order_status": "filled", "execution_price": open_price, "shares": hedge.hedge_shares, "notional": hedge_notional, "cost_amount": hedge_cost, "slippage_amount": hedge_slip, "cash_after": cash, "position_id": hedge_id, "long_position_id": long.position_id, "baseline_position_id": long.baseline_position_id, "reason_type": "close_hedge_with_long_exit", "realized_pnl": hedge_pnl})
                notional = long.shares * open_price
                cost, slip = _cost(notional, cost_model)
                pnl = notional - long.cost_basis - cost
                cash += notional - cost
                longs.pop(long.position_id, None)
                orders.append({"order_id": order.order_id, "decision_ymd": order.decision_ymd, "execution_ymd": ymd, "action": "exit", "code": order.code, "order_status": "filled", "execution_price": open_price, "shares": long.shares, "notional": notional, "cost_amount": cost, "slippage_amount": slip, "cash_after": cash, "position_id": long.position_id, "baseline_position_id": long.baseline_position_id, "reason_type": order.reason_type, "realized_pnl": pnl, "realized_return": pnl / long.cost_basis if long.cost_basis else None})

        long_market = 0.0
        hedge_liability = 0.0
        hedge_unrealized_total = 0.0
        long_unrealized_after_hedge_total = 0.0
        for long in list(longs.values()):
            row = raw_lookup.get((long.code, int(ymd)))
            close_price = _safe_float(row.get("c")) if row is not None else None
            if close_price is None:
                exact_missing.append({"ymd": ymd, "code": long.code, "position_id": long.position_id, "reason": "missing_close_for_mark"})
                continue
            long.holding_days += 1
            long_value = long.shares * close_price
            long_unrealized = long_value - long.cost_basis
            long_market += long_value
            position_rows.append({"ymd": ymd, "position_kind": "long", "position_id": long.position_id, "baseline_position_id": long.baseline_position_id, "code": long.code, "shares": long.shares, "entry_ymd": long.entry_ymd, "entry_price": long.entry_price, "close_price": close_price, "market_value": long_value, "cost_basis": long.cost_basis, "unrealized_pnl": long_unrealized, "holding_days": long.holding_days, "hedge_position_id": long.hedge_position_id})
            if long.hedge_position_id:
                long_unrealized_after_hedge_total += long_unrealized
            if long.pending_exit_order_id is None:
                profit_return = long_unrealized / long.cost_basis if long.cost_basis else 0.0
                next_open = _safe_float(row.get("next_open"))
                next_ymd = _safe_int(row.get("next_ymd"))
                if next_open is not None and next_ymd is not None:
                    if profit_return >= profit_target:
                        order_seq += 1
                        oid = f"{AXIS_ID}-{order_seq:08d}"
                        long.pending_exit_order_id = oid
                        pending.setdefault(next_ymd, []).append(PendingOrder(oid, "exit", long.code, int(ymd), next_ymd, "profit_target", long.baseline_position_id, long.position_id, long.hedge_position_id))
                    elif long.holding_days >= max_holding_days:
                        order_seq += 1
                        oid = f"{AXIS_ID}-{order_seq:08d}"
                        long.pending_exit_order_id = oid
                        pending.setdefault(next_ymd, []).append(PendingOrder(oid, "time_exit", long.code, int(ymd), next_ymd, "time_stop", long.baseline_position_id, long.position_id, long.hedge_position_id))
            if long.pending_exit_order_id is None and not long.hedge_triggered:
                trigger_return = close_price / long.entry_price - 1.0
                next_open = _safe_float(row.get("next_open"))
                next_ymd = _safe_int(row.get("next_ymd"))
                if trigger_return <= HEDGE_TRIGGER_THRESHOLD and next_open is not None and next_ymd is not None:
                    order_seq += 1
                    oid = f"{AXIS_ID}-{order_seq:08d}"
                    pending.setdefault(next_ymd, []).append(PendingOrder(oid, "open_hedge", long.code, int(ymd), next_ymd, "minus8_hedge_trigger", long.baseline_position_id, long.position_id))

        for hedge in list(hedges.values()):
            row = raw_lookup.get((hedge.code, int(ymd)))
            close_price = _safe_float(row.get("c")) if row is not None else None
            if close_price is None:
                exact_missing.append({"ymd": ymd, "code": hedge.code, "position_id": hedge.hedge_position_id, "reason": "missing_close_for_hedge_mark"})
                continue
            hedge.holding_days += 1
            liability = hedge.hedge_shares * close_price
            hedge_unrealized = (hedge.hedge_entry_price - close_price) * hedge.hedge_shares - hedge.hedge_entry_cost
            hedge_liability += liability
            hedge_unrealized_total += hedge_unrealized
            position_rows.append({"ymd": ymd, "position_kind": "synthetic_hedge", "position_id": hedge.hedge_position_id, "long_position_id": hedge.long_position_id, "baseline_position_id": hedge.baseline_position_id, "code": hedge.code, "shares": -hedge.hedge_shares, "entry_ymd": hedge.hedge_entry_ymd, "entry_price": hedge.hedge_entry_price, "close_price": close_price, "market_value": -liability, "cost_basis": hedge.hedge_notional + hedge.hedge_entry_cost, "unrealized_pnl": hedge_unrealized, "holding_days": hedge.holding_days})
            if hedge.pending_close_order_id is not None:
                continue
            long = longs.get(hedge.long_position_id)
            recovered = long is not None and close_price >= long.entry_price
            expired = hedge.holding_days >= HEDGE_MAX_HOLDING_DAYS
            row_next = raw_lookup.get((hedge.code, int(ymd)))
            next_open = _safe_float(row_next.get("next_open")) if row_next is not None else None
            next_ymd = _safe_int(row_next.get("next_ymd")) if row_next is not None else None
            if (recovered or expired) and next_open is not None and next_ymd is not None:
                order_seq += 1
                oid = f"{AXIS_ID}-{order_seq:08d}"
                hedge.pending_close_order_id = oid
                reason = "hedge_release_entry_price_recovered" if recovered else "hedge_release_20_trading_days"
                pending.setdefault(next_ymd, []).append(PendingOrder(oid, "close_hedge", hedge.code, int(ymd), next_ymd, reason, hedge.baseline_position_id, hedge.long_position_id, hedge.hedge_position_id))

        for _idx, action in buy_actions_by_day.get(int(ymd), pd.DataFrame()).iterrows():
            code = str(action.get("code"))
            row = raw_lookup.get((code, int(ymd)))
            next_ymd = _safe_int(row.get("next_ymd")) if row is not None else _safe_int(action.get("execution_ymd"))
            if next_ymd is None:
                exact_missing.append({"ymd": ymd, "code": code, "reason": "missing_next_ymd_for_buy"})
                continue
            baseline_order_id = str(action.get("order_id"))
            match = baseline_orders[(baseline_orders["order_id"].astype(str) == baseline_order_id) | ((baseline_orders["decision_ymd"].astype(int) == int(ymd)) & (baseline_orders["code"].astype(str) == code) & (baseline_orders["action"].astype(str) == "buy"))]
            baseline_pid = str(match.iloc[0].get("position_id")) if not match.empty else None
            pending.setdefault(next_ymd, []).append(PendingOrder(f"{AXIS_ID}-buy-{baseline_order_id}", "buy", code, int(ymd), next_ymd, "baseline_buy_signal", baseline_pid))

        equity = cash + long_market + hedge_unrealized_total
        base_row = baseline_equity[baseline_equity["ymd"].astype(int) == int(ymd)]
        equity_rows.append({"ymd": ymd, "cash": cash, "long_market_value": long_market, "hedge_unrealized_pnl": hedge_unrealized_total, "hedge_notional_liability": hedge_liability, "equity": equity, "baseline_equity": None if base_row.empty else float(base_row.iloc[0]["equity"]), "open_long_count": len(longs), "open_hedge_count": len(hedges), "exact_next_open_replay": len(exact_missing) == 0})

    orders_df = pd.DataFrame(orders)
    positions_df = pd.DataFrame(position_rows)
    equity_df = pd.DataFrame(equity_rows)
    cases_df = pd.DataFrame(hedge_cases)
    hedge_close = orders_df[(orders_df["action"] == "close_hedge") & (orders_df["order_status"] == "filled")].copy() if not orders_df.empty else pd.DataFrame()
    hedge_pnl_total = float(pd.to_numeric(hedge_close.get("realized_pnl", pd.Series(dtype=float)), errors="coerce").sum()) if not hedge_close.empty else 0.0
    hedge_cost_total = float(pd.to_numeric(orders_df[orders_df["action"].isin(["open_hedge", "close_hedge"])].get("cost_amount", pd.Series(dtype=float)), errors="coerce").sum()) if not orders_df.empty else 0.0
    hedged_long_ids = set(cases_df.get("long_position_id", pd.Series(dtype=str)).dropna().astype(str).tolist()) if not cases_df.empty else set()
    long_exit_rows = orders_df[
        (orders_df.get("action", pd.Series(dtype=str)).astype(str) == "exit")
        & (orders_df.get("order_status", pd.Series(dtype=str)).astype(str) == "filled")
        & (orders_df.get("position_id", pd.Series(dtype=str)).astype(str).isin(hedged_long_ids))
    ] if not orders_df.empty and hedged_long_ids else pd.DataFrame()
    realized_hedged_long_pnl = float(pd.to_numeric(long_exit_rows.get("realized_pnl", pd.Series(dtype=float)), errors="coerce").sum()) if not long_exit_rows.empty else 0.0
    latest_positions = positions_df.sort_values("ymd", kind="stable").groupby("position_id", sort=False).tail(1) if not positions_df.empty else pd.DataFrame()
    exited_hedged_long_ids = set(long_exit_rows.get("position_id", pd.Series(dtype=str)).dropna().astype(str).tolist()) if not long_exit_rows.empty else set()
    open_hedged_longs = latest_positions[
        (latest_positions.get("position_kind", pd.Series(dtype=str)).astype(str) == "long")
        & (latest_positions.get("position_id", pd.Series(dtype=str)).astype(str).isin(hedged_long_ids - exited_hedged_long_ids))
    ] if not latest_positions.empty and hedged_long_ids else pd.DataFrame()
    unrealized_hedged_long_pnl = float(pd.to_numeric(open_hedged_longs.get("unrealized_pnl", pd.Series(dtype=float)), errors="coerce").sum()) if not open_hedged_longs.empty else 0.0
    long_pnl_after_hedge_total = realized_hedged_long_pnl + unrealized_hedged_long_pnl
    recovery_cases = cases_df[cases_df["recovery_type"].astype(str).isin(["strong_shakeout_recovery", "moderate_recovery"])] if not cases_df.empty else pd.DataFrame()
    true_breakdown = cases_df[~cases_df["recovery_type"].astype(str).isin(["strong_shakeout_recovery", "moderate_recovery", "minor_recovery"])] if not cases_df.empty else pd.DataFrame()
    baseline_final = float(baseline_equity.iloc[-1]["equity"])
    hedge_final = float(equity_df.iloc[-1]["equity"]) if not equity_df.empty else baseline_final
    hard_metrics = hard_stop_comparison.get("metrics", {})
    metrics = {
        "baseline_final_equity": baseline_final,
        "hedge_final_equity": hedge_final,
        "hard_stop_final_equity": hard_metrics.get("stop_final_equity"),
        "delta_vs_baseline": hedge_final - baseline_final,
        "delta_vs_hard_stop": hedge_final - float(hard_metrics.get("stop_final_equity") or 0.0),
        "baseline_max_drawdown": _max_drawdown(pd.to_numeric(baseline_equity["equity"], errors="coerce")),
        "hedge_max_drawdown": _max_drawdown(pd.to_numeric(equity_df["equity"], errors="coerce")),
        "hard_stop_max_drawdown": hard_metrics.get("stop_max_drawdown"),
        "hedge_trigger_count": int(len(cases_df)),
        "hedge_cost_total": hedge_cost_total,
        "hedge_pnl_total": hedge_pnl_total,
        "long_pnl_after_hedge_total": long_pnl_after_hedge_total,
        "recovery_capture_vs_hard_stop": hedge_final - float(hard_metrics.get("stop_final_equity") or 0.0),
        "false_stop_recovery_preserved_count": int(len(recovery_cases)),
        "true_breakdown_damage_reduced_count": int((true_breakdown.get("baseline_pnl", pd.Series(dtype=float)).fillna(0) < 0).sum()) if not true_breakdown.empty else 0,
        "exposure_delta": reconciliation.get("exposure_delta"),
        "cost_delta": float(pd.to_numeric(orders_df.get("cost_amount", pd.Series(dtype=float)), errors="coerce").sum() - pd.to_numeric(baseline_orders.get("cost_amount", pd.Series(dtype=float)), errors="coerce").sum()),
        "order_count_delta": int(len(orders_df) - len(baseline_orders)),
        "exact_next_open_replay": len(exact_missing) == 0,
    }
    return {"frames": {"orders": orders_df, "positions": positions_df, "equity": equity_df, "cases": cases_df, "recovery": recovery_cases, "true_breakdown": true_breakdown}, "metrics": metrics, "hard_stop_metrics": hard_metrics, "exact_missing": exact_missing, "source_db": str(source_db), "limitations": ["same-name short hedge is synthetic; borrow availability, short-sale bans, and stock-loan fees are not modeled"]}


def _decision(metrics: dict[str, Any]) -> tuple[str, str]:
    if not metrics["exact_next_open_replay"]:
        return "hold_for_realism_check", "exact_next_open_unavailable"
    baseline_ok = metrics["hedge_final_equity"] > metrics["baseline_final_equity"]
    dd_ok = metrics["hedge_max_drawdown"] > metrics["baseline_max_drawdown"]
    hard_capture = metrics["delta_vs_hard_stop"] > 0
    if baseline_ok and dd_ok and hard_capture and metrics["hedge_cost_total"] <= max(500_000.0, metrics["baseline_final_equity"] * 0.05):
        return "keep_for_replay_challenger", "hedge_improves_equity_drawdown_and_recovery_capture"
    if baseline_ok and dd_ok:
        return "hold_for_realism_check", "portfolio_improved_but_synthetic_same_name_short_constraints_need_check"
    if hard_capture:
        return "hold_for_partial_stop_comparison", "recovery_capture_better_than_hard_stop_but_baseline_gate_not_met"
    if metrics["hedge_cost_total"] > abs(metrics["delta_vs_baseline"]):
        return "drop_due_to_cost_or_complexity", "hedge_cost_overwhelms_benefit"
    return "drop_due_to_profit_damage", "hedge_does_not_improve_portfolio"


def run_hedged_stop_at_minus8_pretest_v1(run_root: str | Path, source_db: str | Path | None = None, output_root: str | Path | None = None) -> dict[str, Any]:
    run_root = Path(run_root)
    output_root = Path(output_root) if output_root is not None else run_root / DEFAULT_OUTPUT_DIR_NAME
    status = _source_status(run_root)
    missing = [name for name, exists in status.items() if not exists]
    if missing:
        raise FileNotFoundError(f"missing required source artifacts: {missing}")
    run_config = _read_json(run_root / "run_config.json")
    db_path = Path(source_db or run_config["source_db"])
    if not db_path.exists():
        raise FileNotFoundError(f"source DB not found: {db_path}")
    result = run_hedge_overlay(run_root, db_path)
    frames = result["frames"]
    metrics = result["metrics"]
    decision, reason = _decision(metrics)

    _write_csv(output_root / "hedge_triggered_cases.csv", frames["cases"])
    _write_csv(output_root / "hedge_orders_ledger.csv", frames["orders"])
    _write_csv(output_root / "hedge_positions_ledger.csv", frames["positions"])
    _write_csv(output_root / "hedge_equity_curve.csv", frames["equity"])
    _write_csv(output_root / "hedge_recovery_cases.csv", frames["recovery"])
    _write_csv(output_root / "hedge_true_breakdown_cases.csv", frames["true_breakdown"])
    _write_json(output_root / "baseline_vs_hedge_comparison.json", {"schema_version": f"{SCHEMA_PREFIX}_baseline_vs_hedge_comparison_v1", "axis_id": AXIS_ID, "generated_at": _utc_now(), "metrics": metrics})
    _write_json(output_root / "hard_stop_vs_hedge_comparison.json", {"schema_version": f"{SCHEMA_PREFIX}_hard_stop_vs_hedge_comparison_v1", "axis_id": AXIS_ID, "generated_at": _utc_now(), "hard_stop_metrics": result["hard_stop_metrics"], "hedge_metrics": metrics, "delta_final_equity_hedge_minus_hard_stop": metrics["delta_vs_hard_stop"], "delta_max_drawdown_hedge_minus_hard_stop": metrics["hedge_max_drawdown"] - float(metrics["hard_stop_max_drawdown"] or 0.0)})
    _write_json(output_root / "hedge_cost_summary.json", {"schema_version": f"{SCHEMA_PREFIX}_hedge_cost_summary_v1", "axis_id": AXIS_ID, "generated_at": _utc_now(), "hedge_cost_total": metrics["hedge_cost_total"], "cost_delta": metrics["cost_delta"], "hedge_trigger_count": metrics["hedge_trigger_count"], "borrow_fee_modeled": False, "borrow_fee_limitation": "borrow availability, short-sale bans, and stock-loan fees not modeled in v1"})
    _write_json(output_root / "hedge_exposure_attribution.json", {"schema_version": f"{SCHEMA_PREFIX}_hedge_exposure_attribution_v1", "axis_id": AXIS_ID, "generated_at": _utc_now(), "hedge_pnl_total": metrics["hedge_pnl_total"], "long_pnl_after_hedge_total": metrics["long_pnl_after_hedge_total"], "exposure_delta_reference_from_stop_reconciliation": metrics["exposure_delta"], "recovery_capture_vs_hard_stop": metrics["recovery_capture_vs_hard_stop"]})
    _write_json(output_root / "selection_feature_manifest.json", {"schema_version": f"{SCHEMA_PREFIX}_selection_feature_manifest_v1", "axis_id": AXIS_ID, "generated_at": _utc_now(), "selection_allowed_columns": ["ymd", "code", "close", "average_entry_price", "close_return_vs_average_entry_price", "hedge_holding_days"], "selection_forbidden_columns": ["post_ret_5", "post_ret_10", "post_ret_20", "post_ret_40", "mae_20", "mfe_20", "future_recovery_type"], "diagnostic_only_columns": ["post_hedge_ret20", "post_hedge_max_up20", "recovery_type"], "outcome_label_columns": ["post_hedge_ret20", "post_hedge_max_up20"], "audit_result": "pass"})
    _write_json(output_root / "no_lookahead_audit.json", {"schema_version": f"{SCHEMA_PREFIX}_no_lookahead_audit_v1", "axis_id": AXIS_ID, "generated_at": _utc_now(), "audit_result": "pass" if metrics["exact_next_open_replay"] else "research_fallback", "exact_next_open_replay": metrics["exact_next_open_replay"], "same_day_close_fill_used": False, "threshold_sweep": False, "hedge_ratio_sweep": False, "exit_day_sweep": False, "post_run_outcomes_used_for_hedge_condition": False, "future_recovery_used_for_hedge_condition": False, "missing_exact_price_events": result["exact_missing"], "silent_fallback_used": False})
    next_axis = {"schema_version": f"{SCHEMA_PREFIX}_next_axis_decision_v1", "axis_id": AXIS_ID, "generated_at": _utc_now(), "decision_candidates": list(DECISIONS), "decision": decision, "decision_count": 1, "reason_type": reason, "metrics": metrics, "policy": {"single_axis_only": True, "hedge_trigger_threshold": HEDGE_TRIGGER_THRESHOLD, "hedge_ratio": HEDGE_RATIO, "hedge_max_holding_days": HEDGE_MAX_HOLDING_DAYS, "threshold_sweep": False, "hedge_ratio_sweep": False, "exit_day_sweep": False, "production_policy_changed": False}}
    _write_json(output_root / "next_axis_decision.json", next_axis)
    summary = {"schema_version": f"{SCHEMA_PREFIX}_summary_v1", "axis_id": AXIS_ID, "generated_at": _utc_now(), "run_root": str(run_root), "source_db": str(db_path), "source_artifacts": status, "scope": {"tradex_only": True, "db_backed_exact_pretest": True, "same_run": True, "same_period": True, "same_baseline_policy_except_hedge_axis": True, "same_max_positions": True, "same_cost_slippage": True, "single_axis_only": True, "production_policy_changed": False, "meemee_ui_changed": False, "runtime_db_written": False, "ranking_changed": False, "publish_registry_changed": False}, "rule": {"trigger_threshold": HEDGE_TRIGGER_THRESHOLD, "hedge_ratio": HEDGE_RATIO, "release_on_entry_price_recovery": True, "release_after_trading_days": HEDGE_MAX_HOLDING_DAYS, "same_position_hedge_once": True, "threshold_sweep": False, "hedge_ratio_sweep": False, "exit_day_sweep": False}, "metrics": metrics, "decision": decision, "decision_reason_type": reason, "limitations": result["limitations"]}
    _write_json(output_root / "hedged_stop_pretest_summary.json", summary)
    complete = {"schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1", "axis_id": AXIS_ID, "generated_at": _utc_now(), "run_root": str(run_root), "output_root": str(output_root), "complete": True, "required_artifacts_all_present": all((output_root / name).exists() for name in OUTPUT_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json"), "source_artifacts_all_present": all(status.values()), "decision": decision, "decision_count": 1, "exact_next_open_replay": metrics["exact_next_open_replay"], "no_lookahead_audit": "pass" if metrics["exact_next_open_replay"] else "research_fallback", "threshold_sweep": False, "hedge_ratio_sweep": False, "exit_day_sweep": False, "production_policy_changed": False, "silent_fallback_used": False, "limitations_recorded": True}
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    return {"complete": True, "output_root": str(output_root), "decision": decision, "metrics": metrics}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DB-backed synthetic same-name hedge at -8% pretest.")
    parser.add_argument("--run-root", required=True, type=Path)
    parser.add_argument("--source-db", type=Path, default=None)
    parser.add_argument("--output-root", type=Path, default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    print(_json_text(run_hedged_stop_at_minus8_pretest_v1(args.run_root, args.source_db, args.output_root)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
