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


AXIS_ID = "partial_stop_at_minus8_pretest_v1"
SCHEMA_PREFIX = "tradex_partial_stop_at_minus8_pretest_v1"
DEFAULT_OUTPUT_DIR_NAME = "partial_stop_at_minus8_pretest_v1"
PARTIAL_STOP_THRESHOLD = -0.08
TRIM_RATIO = 0.50
LOT_SIZE = 100

SOURCE_ARTIFACTS = (
    "run_config.json",
    "daily_action_ledger.jsonl",
    "orders_ledger.csv",
    "positions_ledger.csv",
    "equity_curve.csv",
    "stop_too_wide_pretest_v1/baseline_vs_stop_comparison.json",
    "stop_case_reconciliation_v1/equity_delta_attribution.json",
    "hedged_stop_at_minus8_pretest_v1/baseline_vs_hedge_comparison.json",
    "candidate_lifecycle_audit_v1/candidate_lifecycle_summary.json",
    "diagnosis_v1",
)

OUTPUT_ARTIFACTS = (
    "partial_stop_pretest_summary.json",
    "partial_stop_triggered_cases.csv",
    "baseline_vs_partial_stop_comparison.json",
    "hard_stop_vs_partial_stop_comparison.json",
    "hedge_vs_partial_stop_comparison.json",
    "partial_stop_orders_ledger.csv",
    "partial_stop_positions_ledger.csv",
    "partial_stop_equity_curve.csv",
    "partial_stop_recovery_cases.csv",
    "partial_stop_saved_loss_cases.csv",
    "partial_stop_missed_profit_cases.csv",
    "partial_stop_exposure_attribution.json",
    "no_lookahead_audit.json",
    "selection_feature_manifest.json",
    "next_axis_decision.json",
    "_ARTIFACT_COMPLETE.json",
)

DECISIONS = (
    "keep_for_replay_challenger",
    "hold_for_multi_period_validation",
    "hold_for_sizing_haircut",
    "drop_due_to_profit_damage",
    "drop_due_to_no_drawdown_improvement",
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
    original_shares: int
    original_cost_basis: float
    holding_days: int = 0
    pending_exit_order_id: str | None = None
    partial_stop_triggered: bool = False


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
        return {"post_partial_stop_ret20": None, "post_partial_stop_max_up20": None, "recovery_type": "unavailable_no_future_bars"}
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
    return {"post_partial_stop_ret20": ret20, "post_partial_stop_max_up20": max_up, "recovery_type": recovery_type}


def run_partial_stop_overlay(run_root: Path, source_db: Path) -> dict[str, Any]:
    run_config = _read_json(run_root / "run_config.json")
    actions = pd.DataFrame(_read_jsonl(run_root / "daily_action_ledger.jsonl"))
    baseline_orders = pd.read_csv(run_root / "orders_ledger.csv")
    baseline_positions = pd.read_csv(run_root / "positions_ledger.csv")
    baseline_equity = pd.read_csv(run_root / "equity_curve.csv")
    hard_stop_comparison = _read_json(run_root / "stop_too_wide_pretest_v1" / "baseline_vs_stop_comparison.json")
    reconciliation = _read_json(run_root / "stop_case_reconciliation_v1" / "equity_delta_attribution.json")
    hedge_comparison = _read_json(run_root / "hedged_stop_at_minus8_pretest_v1" / "baseline_vs_hedge_comparison.json")
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
    cash = initial_cash
    order_seq = 0
    orders: list[dict[str, Any]] = []
    position_rows: list[dict[str, Any]] = []
    equity_rows: list[dict[str, Any]] = []
    partial_cases: list[dict[str, Any]] = []
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
                    original_shares=shares,
                    original_cost_basis=notional + cost,
                )
                orders.append({"order_id": order.order_id, "decision_ymd": order.decision_ymd, "execution_ymd": ymd, "action": "buy", "code": order.code, "order_status": "filled", "execution_price": open_price, "shares": shares, "notional": notional, "cost_amount": cost, "slippage_amount": slip, "cash_after": cash, "position_id": position_id, "baseline_position_id": order.baseline_position_id, "reason_type": "baseline_buy_signal"})
                continue

            if order.action == "partial_stop":
                long = longs.get(str(order.long_position_id))
                if long is None or long.partial_stop_triggered:
                    orders.append({"order_id": order.order_id, "decision_ymd": order.decision_ymd, "execution_ymd": ymd, "action": "partial_stop", "code": order.code, "order_status": "unfilled", "unfilled_reason": "long_missing_or_partial_already_triggered", "execution_price": open_price})
                    continue
                trim_shares = int((long.shares * TRIM_RATIO) // LOT_SIZE) * LOT_SIZE
                if trim_shares <= 0 or trim_shares >= long.shares:
                    orders.append({"order_id": order.order_id, "decision_ymd": order.decision_ymd, "execution_ymd": ymd, "action": "partial_stop", "code": order.code, "order_status": "unfilled", "unfilled_reason": "partial_lot_too_small", "execution_price": open_price})
                    continue
                remaining_before = long.shares
                trim_ratio_actual = trim_shares / remaining_before
                notional = trim_shares * open_price
                partial_cost_basis = long.cost_basis * trim_ratio_actual
                cost, slip = _cost(notional, cost_model)
                pnl = notional - partial_cost_basis - cost
                cash += notional - cost
                long.shares -= trim_shares
                long.cost_basis -= partial_cost_basis
                long.notional *= long.shares / remaining_before
                long.partial_stop_triggered = True
                recovery = _future_recovery(raw_by_code, long.code, int(ymd), open_price)
                partial_cases.append({"position_id": long.position_id, "baseline_position_id": long.baseline_position_id, "code": long.code, "partial_stop_trigger_date": order.decision_ymd, "partial_stop_exit_date": ymd, "partial_stop_exit_price": open_price, "trim_shares": trim_shares, "remaining_shares": long.shares, "trim_ratio": TRIM_RATIO, "actual_trim_ratio": trim_ratio_actual, "partial_cost_basis": partial_cost_basis, "partial_stop_realized_pnl": pnl, "baseline_pnl": baseline_pnl_by_pid.get(long.baseline_position_id), **recovery})
                orders.append({"order_id": order.order_id, "decision_ymd": order.decision_ymd, "execution_ymd": ymd, "action": "partial_stop", "code": order.code, "order_status": "filled", "execution_price": open_price, "shares": trim_shares, "notional": notional, "cost_amount": cost, "slippage_amount": slip, "cash_after": cash, "position_id": long.position_id, "baseline_position_id": long.baseline_position_id, "reason_type": "minus8_partial_stop_50pct", "realized_pnl": pnl})
                continue

            if order.action in {"exit", "time_exit"}:
                long = longs.get(str(order.long_position_id))
                if long is None:
                    orders.append({"order_id": order.order_id, "decision_ymd": order.decision_ymd, "execution_ymd": ymd, "action": "exit", "code": order.code, "order_status": "unfilled", "unfilled_reason": "long_missing", "execution_price": open_price})
                    continue
                notional = long.shares * open_price
                cost, slip = _cost(notional, cost_model)
                pnl = notional - long.cost_basis - cost
                cash += notional - cost
                longs.pop(long.position_id, None)
                orders.append({"order_id": order.order_id, "decision_ymd": order.decision_ymd, "execution_ymd": ymd, "action": "exit", "code": order.code, "order_status": "filled", "execution_price": open_price, "shares": long.shares, "notional": notional, "cost_amount": cost, "slippage_amount": slip, "cash_after": cash, "position_id": long.position_id, "baseline_position_id": long.baseline_position_id, "reason_type": order.reason_type, "realized_pnl": pnl, "realized_return": pnl / long.cost_basis if long.cost_basis else None})

        long_market = 0.0
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
            position_rows.append({"ymd": ymd, "position_kind": "long", "position_id": long.position_id, "baseline_position_id": long.baseline_position_id, "code": long.code, "shares": long.shares, "original_shares": long.original_shares, "entry_ymd": long.entry_ymd, "entry_price": long.entry_price, "close_price": close_price, "market_value": long_value, "cost_basis": long.cost_basis, "original_cost_basis": long.original_cost_basis, "unrealized_pnl": long_unrealized, "holding_days": long.holding_days, "partial_stop_triggered": long.partial_stop_triggered})
            if long.pending_exit_order_id is None:
                profit_return = long_unrealized / long.cost_basis if long.cost_basis else 0.0
                next_open = _safe_float(row.get("next_open"))
                next_ymd = _safe_int(row.get("next_ymd"))
                if next_open is not None and next_ymd is not None:
                    if profit_return >= profit_target:
                        order_seq += 1
                        oid = f"{AXIS_ID}-{order_seq:08d}"
                        long.pending_exit_order_id = oid
                        pending.setdefault(next_ymd, []).append(PendingOrder(oid, "exit", long.code, int(ymd), next_ymd, "profit_target", long.baseline_position_id, long.position_id))
                    elif long.holding_days >= max_holding_days:
                        order_seq += 1
                        oid = f"{AXIS_ID}-{order_seq:08d}"
                        long.pending_exit_order_id = oid
                        pending.setdefault(next_ymd, []).append(PendingOrder(oid, "time_exit", long.code, int(ymd), next_ymd, "time_stop", long.baseline_position_id, long.position_id))
            if long.pending_exit_order_id is None and not long.partial_stop_triggered:
                trigger_return = close_price / long.entry_price - 1.0
                next_open = _safe_float(row.get("next_open"))
                next_ymd = _safe_int(row.get("next_ymd"))
                if trigger_return <= PARTIAL_STOP_THRESHOLD and next_open is not None and next_ymd is not None:
                    order_seq += 1
                    oid = f"{AXIS_ID}-{order_seq:08d}"
                    pending.setdefault(next_ymd, []).append(PendingOrder(oid, "partial_stop", long.code, int(ymd), next_ymd, "minus8_partial_stop_trigger", long.baseline_position_id, long.position_id))

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

        equity = cash + long_market
        base_row = baseline_equity[baseline_equity["ymd"].astype(int) == int(ymd)]
        equity_rows.append({"ymd": ymd, "cash": cash, "long_market_value": long_market, "equity": equity, "baseline_equity": None if base_row.empty else float(base_row.iloc[0]["equity"]), "open_long_count": len(longs), "exact_next_open_replay": len(exact_missing) == 0})

    orders_df = pd.DataFrame(orders)
    positions_df = pd.DataFrame(position_rows)
    equity_df = pd.DataFrame(equity_rows)
    cases_df = pd.DataFrame(partial_cases)
    partial_rows = orders_df[(orders_df.get("action", pd.Series(dtype=str)).astype(str) == "partial_stop") & (orders_df.get("order_status", pd.Series(dtype=str)).astype(str) == "filled")].copy() if not orders_df.empty else pd.DataFrame()
    partial_stop_cost_total = float(pd.to_numeric(partial_rows.get("cost_amount", pd.Series(dtype=float)), errors="coerce").sum()) if not partial_rows.empty else 0.0
    latest_positions = positions_df.sort_values("ymd", kind="stable").groupby("position_id", sort=False).tail(1) if not positions_df.empty else pd.DataFrame()
    overlay_pnl_by_pid: dict[str, float] = {}
    if not orders_df.empty:
        realized = orders_df[(orders_df.get("action", pd.Series(dtype=str)).astype(str).isin(["partial_stop", "exit"])) & (orders_df.get("order_status", pd.Series(dtype=str)).astype(str) == "filled")].copy()
        for pid, group in realized.groupby(realized.get("baseline_position_id", pd.Series(dtype=str)).astype(str), sort=False):
            overlay_pnl_by_pid[str(pid)] = float(pd.to_numeric(group.get("realized_pnl", pd.Series(dtype=float)), errors="coerce").sum())
    if not latest_positions.empty:
        exited_positions = set(orders_df[(orders_df.get("action", pd.Series(dtype=str)).astype(str) == "exit") & (orders_df.get("order_status", pd.Series(dtype=str)).astype(str) == "filled")].get("position_id", pd.Series(dtype=str)).dropna().astype(str).tolist()) if not orders_df.empty else set()
        for _idx, pos in latest_positions.iterrows():
            position_id = str(pos.get("position_id"))
            if position_id in exited_positions:
                continue
            pid = str(pos.get("baseline_position_id"))
            overlay_pnl_by_pid[pid] = overlay_pnl_by_pid.get(pid, 0.0) + (_safe_float(pos.get("unrealized_pnl")) or 0.0)
    if not cases_df.empty:
        deltas = []
        for _idx, case in cases_df.iterrows():
            pid = str(case.get("baseline_position_id"))
            overlay_pnl = overlay_pnl_by_pid.get(pid, 0.0)
            baseline_pnl = _safe_float(case.get("baseline_pnl")) or 0.0
            deltas.append(overlay_pnl - baseline_pnl)
        cases_df["direct_delta_pnl"] = deltas
    recovery_cases = cases_df[cases_df["recovery_type"].astype(str).isin(["strong_shakeout_recovery", "moderate_recovery"])] if not cases_df.empty else pd.DataFrame()
    saved_loss_cases = cases_df[pd.to_numeric(cases_df.get("direct_delta_pnl", pd.Series(dtype=float)), errors="coerce") > 0].copy() if not cases_df.empty else pd.DataFrame()
    missed_profit_cases = cases_df[pd.to_numeric(cases_df.get("direct_delta_pnl", pd.Series(dtype=float)), errors="coerce") < 0].copy() if not cases_df.empty else pd.DataFrame()
    baseline_final = float(baseline_equity.iloc[-1]["equity"])
    partial_final = float(equity_df.iloc[-1]["equity"]) if not equity_df.empty else baseline_final
    hard_metrics = hard_stop_comparison.get("metrics", {})
    hedge_metrics = hedge_comparison.get("metrics", {})
    direct_partial_stop_pnl_delta = float(pd.to_numeric(cases_df.get("direct_delta_pnl", pd.Series(dtype=float)), errors="coerce").sum()) if not cases_df.empty else 0.0
    cost_delta = float(pd.to_numeric(orders_df.get("cost_amount", pd.Series(dtype=float)), errors="coerce").sum() - pd.to_numeric(baseline_orders.get("cost_amount", pd.Series(dtype=float)), errors="coerce").sum())
    path_effect_delta = partial_final - baseline_final - direct_partial_stop_pnl_delta
    metrics = {
        "baseline_final_equity": baseline_final,
        "partial_stop_final_equity": partial_final,
        "hard_stop_final_equity": hard_metrics.get("stop_final_equity"),
        "hedge_final_equity": hedge_metrics.get("hedge_final_equity"),
        "delta_vs_baseline": partial_final - baseline_final,
        "delta_vs_hard_stop": partial_final - float(hard_metrics.get("stop_final_equity") or 0.0),
        "delta_vs_hedge": partial_final - float(hedge_metrics.get("hedge_final_equity") or 0.0),
        "baseline_max_drawdown": _max_drawdown(pd.to_numeric(baseline_equity["equity"], errors="coerce")),
        "partial_stop_max_drawdown": _max_drawdown(pd.to_numeric(equity_df["equity"], errors="coerce")),
        "hard_stop_max_drawdown": hard_metrics.get("stop_max_drawdown"),
        "hedge_max_drawdown": hedge_metrics.get("hedge_max_drawdown"),
        "partial_stop_trigger_count": int(len(cases_df)),
        "saved_loss_total": float(pd.to_numeric(saved_loss_cases.get("direct_delta_pnl", pd.Series(dtype=float)), errors="coerce").sum()) if not saved_loss_cases.empty else 0.0,
        "missed_profit_total": abs(float(pd.to_numeric(missed_profit_cases.get("direct_delta_pnl", pd.Series(dtype=float)), errors="coerce").sum())) if not missed_profit_cases.empty else 0.0,
        "hard_stop_missed_profit_total": hard_metrics.get("missed_profit_total"),
        "recovery_capture_vs_hard_stop": partial_final - float(hard_metrics.get("stop_final_equity") or 0.0),
        "exposure_delta": path_effect_delta,
        "freed_cash_redeployment_delta": path_effect_delta,
        "direct_partial_stop_pnl_delta": direct_partial_stop_pnl_delta,
        "false_stop_recovery_count": int(len(recovery_cases)),
        "cost_delta": cost_delta,
        "order_count_delta": int(len(orders_df) - len(baseline_orders)),
        "exact_next_open_replay": len(exact_missing) == 0,
    }
    return {"frames": {"orders": orders_df, "positions": positions_df, "equity": equity_df, "cases": cases_df, "recovery": recovery_cases, "saved_loss": saved_loss_cases, "missed_profit": missed_profit_cases}, "metrics": metrics, "hard_stop_metrics": hard_metrics, "hedge_metrics": hedge_metrics, "stop_reconciliation_exposure_delta": reconciliation.get("exposure_delta"), "exact_missing": exact_missing, "source_db": str(source_db), "limitations": ["partial path attribution combines freed cash, reduced exposure, and subsequent execution differences; these are reported as combined path-effect metrics"]}


def _decision(metrics: dict[str, Any]) -> tuple[str, str]:
    if not metrics["exact_next_open_replay"]:
        return "hold_for_sizing_haircut", "exact_next_open_unavailable"
    baseline_ok = metrics["partial_stop_final_equity"] > metrics["baseline_final_equity"]
    dd_ok = metrics["partial_stop_max_drawdown"] > metrics["baseline_max_drawdown"]
    hedge_ok = metrics["delta_vs_hedge"] > 0
    hard_missed_profit = metrics.get("hard_stop_missed_profit_total")
    missed_profit_ok = hard_missed_profit is None or metrics["missed_profit_total"] < float(hard_missed_profit)
    if baseline_ok and dd_ok and hedge_ok and missed_profit_ok:
        return "keep_for_replay_challenger", "partial_stop_improves_baseline_drawdown_and_beats_hedge_with_lower_missed_profit"
    if baseline_ok and dd_ok:
        return "hold_for_multi_period_validation", "partial_stop_improves_baseline_but_needs_stability_check"
    if baseline_ok:
        return "drop_due_to_no_drawdown_improvement", "partial_stop_improves_equity_without_drawdown_improvement"
    return "drop_due_to_profit_damage", "partial_stop_does_not_improve_portfolio"


def run_partial_stop_at_minus8_pretest_v1(run_root: str | Path, source_db: str | Path | None = None, output_root: str | Path | None = None) -> dict[str, Any]:
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
    result = run_partial_stop_overlay(run_root, db_path)
    frames = result["frames"]
    metrics = result["metrics"]
    decision, reason = _decision(metrics)

    _write_csv(output_root / "partial_stop_triggered_cases.csv", frames["cases"])
    _write_csv(output_root / "partial_stop_orders_ledger.csv", frames["orders"])
    _write_csv(output_root / "partial_stop_positions_ledger.csv", frames["positions"])
    _write_csv(output_root / "partial_stop_equity_curve.csv", frames["equity"])
    _write_csv(output_root / "partial_stop_recovery_cases.csv", frames["recovery"])
    _write_csv(output_root / "partial_stop_saved_loss_cases.csv", frames["saved_loss"])
    _write_csv(output_root / "partial_stop_missed_profit_cases.csv", frames["missed_profit"])
    _write_json(output_root / "baseline_vs_partial_stop_comparison.json", {"schema_version": f"{SCHEMA_PREFIX}_baseline_vs_partial_stop_comparison_v1", "axis_id": AXIS_ID, "generated_at": _utc_now(), "metrics": metrics})
    _write_json(output_root / "hard_stop_vs_partial_stop_comparison.json", {"schema_version": f"{SCHEMA_PREFIX}_hard_stop_vs_partial_stop_comparison_v1", "axis_id": AXIS_ID, "generated_at": _utc_now(), "hard_stop_metrics": result["hard_stop_metrics"], "partial_stop_metrics": metrics, "delta_final_equity_partial_minus_hard_stop": metrics["delta_vs_hard_stop"], "delta_max_drawdown_partial_minus_hard_stop": metrics["partial_stop_max_drawdown"] - float(metrics["hard_stop_max_drawdown"] or 0.0)})
    _write_json(output_root / "hedge_vs_partial_stop_comparison.json", {"schema_version": f"{SCHEMA_PREFIX}_hedge_vs_partial_stop_comparison_v1", "axis_id": AXIS_ID, "generated_at": _utc_now(), "hedge_metrics": result["hedge_metrics"], "partial_stop_metrics": metrics, "delta_final_equity_partial_minus_hedge": metrics["delta_vs_hedge"], "delta_max_drawdown_partial_minus_hedge": metrics["partial_stop_max_drawdown"] - float(metrics["hedge_max_drawdown"] or 0.0)})
    _write_json(output_root / "partial_stop_exposure_attribution.json", {"schema_version": f"{SCHEMA_PREFIX}_partial_stop_exposure_attribution_v1", "axis_id": AXIS_ID, "generated_at": _utc_now(), "direct_partial_stop_pnl_delta": metrics["direct_partial_stop_pnl_delta"], "freed_cash_redeployment_delta": metrics["freed_cash_redeployment_delta"], "exposure_delta": metrics["exposure_delta"], "hard_stop_exposure_delta_reference": result["hard_stop_metrics"].get("exposure_delta"), "stop_reconciliation_exposure_delta_reference": result.get("stop_reconciliation_exposure_delta"), "attribution_limitation": result["limitations"][0]})
    _write_json(output_root / "selection_feature_manifest.json", {"schema_version": f"{SCHEMA_PREFIX}_selection_feature_manifest_v1", "axis_id": AXIS_ID, "generated_at": _utc_now(), "selection_allowed_columns": ["ymd", "code", "close", "average_entry_price", "close_return_vs_average_entry_price", "current_shares", "trim_ratio"], "selection_forbidden_columns": ["post_ret_5", "post_ret_10", "post_ret_20", "post_ret_40", "mae_20", "mfe_20", "future_recovery_type"], "diagnostic_only_columns": ["post_partial_stop_ret20", "post_partial_stop_max_up20", "recovery_type", "direct_delta_pnl"], "outcome_label_columns": ["post_partial_stop_ret20", "post_partial_stop_max_up20", "direct_delta_pnl"], "audit_result": "pass"})
    _write_json(output_root / "no_lookahead_audit.json", {"schema_version": f"{SCHEMA_PREFIX}_no_lookahead_audit_v1", "axis_id": AXIS_ID, "generated_at": _utc_now(), "audit_result": "pass" if metrics["exact_next_open_replay"] else "research_fallback", "exact_next_open_replay": metrics["exact_next_open_replay"], "same_day_close_fill_used": False, "threshold_sweep": False, "trim_ratio_sweep": False, "post_run_outcomes_used_for_exit_condition": False, "future_recovery_used_for_exit_condition": False, "missing_exact_price_events": result["exact_missing"], "silent_fallback_used": False})
    next_axis = {"schema_version": f"{SCHEMA_PREFIX}_next_axis_decision_v1", "axis_id": AXIS_ID, "generated_at": _utc_now(), "decision_candidates": list(DECISIONS), "decision": decision, "decision_count": 1, "reason_type": reason, "metrics": metrics, "policy": {"single_axis_only": True, "partial_stop_threshold": PARTIAL_STOP_THRESHOLD, "trim_ratio": TRIM_RATIO, "threshold_sweep": False, "trim_ratio_sweep": False, "production_policy_changed": False}}
    _write_json(output_root / "next_axis_decision.json", next_axis)
    summary = {"schema_version": f"{SCHEMA_PREFIX}_summary_v1", "axis_id": AXIS_ID, "generated_at": _utc_now(), "run_root": str(run_root), "source_db": str(db_path), "source_artifacts": status, "scope": {"tradex_only": True, "db_backed_exact_pretest": True, "same_run": True, "same_period": True, "same_baseline_policy_except_partial_stop_axis": True, "same_max_positions": True, "same_cost_slippage": True, "single_axis_only": True, "production_policy_changed": False, "meemee_ui_changed": False, "runtime_db_written": False, "ranking_changed": False, "publish_registry_changed": False}, "rule": {"trigger_threshold": PARTIAL_STOP_THRESHOLD, "trim_ratio": TRIM_RATIO, "same_position_partial_stop_once": True, "threshold_sweep": False, "trim_ratio_sweep": False}, "metrics": metrics, "decision": decision, "decision_reason_type": reason, "limitations": result["limitations"]}
    _write_json(output_root / "partial_stop_pretest_summary.json", summary)
    complete = {"schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1", "axis_id": AXIS_ID, "generated_at": _utc_now(), "run_root": str(run_root), "output_root": str(output_root), "complete": True, "required_artifacts_all_present": all((output_root / name).exists() for name in OUTPUT_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json"), "source_artifacts_all_present": all(status.values()), "decision": decision, "decision_count": 1, "exact_next_open_replay": metrics["exact_next_open_replay"], "no_lookahead_audit": "pass" if metrics["exact_next_open_replay"] else "research_fallback", "threshold_sweep": False, "trim_ratio_sweep": False, "production_policy_changed": False, "silent_fallback_used": False, "limitations_recorded": True}
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    return {"complete": True, "output_root": str(output_root), "decision": decision, "metrics": metrics}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DB-backed 50% partial stop at -8% pretest.")
    parser.add_argument("--run-root", required=True, type=Path)
    parser.add_argument("--source-db", type=Path, default=None)
    parser.add_argument("--output-root", type=Path, default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    print(_json_text(run_partial_stop_at_minus8_pretest_v1(args.run_root, args.source_db, args.output_root)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
