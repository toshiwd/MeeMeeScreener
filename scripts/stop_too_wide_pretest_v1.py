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


AXIS_ID = "stop_too_wide_pretest_v1"
SCHEMA_PREFIX = "tradex_stop_too_wide_pretest_v1"
DEFAULT_OUTPUT_DIR_NAME = "stop_too_wide_pretest_v1"
STOP_THRESHOLD = -0.08
LOT_SIZE = 100

REQUIRED_ARTIFACTS = (
    "stop_too_wide_pretest_summary.json",
    "stop_triggered_cases.csv",
    "baseline_vs_stop_comparison.json",
    "stop_overlay_orders_ledger.csv",
    "stop_overlay_positions_ledger.csv",
    "stop_overlay_equity_curve.csv",
    "saved_loss_cases.csv",
    "false_stop_recovery_cases.csv",
    "missed_profit_after_stop_cases.csv",
    "stop_feature_distribution.csv",
    "no_lookahead_audit.json",
    "selection_feature_manifest.json",
    "next_axis_decision.json",
    "_ARTIFACT_COMPLETE.json",
)

SOURCE_ARTIFACTS = (
    "run_config.json",
    "daily_action_ledger.jsonl",
    "orders_ledger.csv",
    "positions_ledger.csv",
    "equity_curve.csv",
    "daily_candidate_snapshot.csv",
    "candidate_lifecycle_audit_v1/candidate_lifecycle_summary.json",
    "candidate_lifecycle_audit_v1/invalidation_candle_cases.csv",
    "candidate_lifecycle_audit_v1/false_invalidation_recovery_cases.csv",
    "trade_reflection_audit_v1/reflection_priority_decision.json",
)

DECISIONS = (
    "keep_for_replay_challenger",
    "hold_due_to_recovery_risk",
    "drop_due_to_profit_damage",
    "drop_due_to_no_drawdown_improvement",
    "rerun_required_due_to_exact_price_unavailable",
)


@dataclass
class OverlayPosition:
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
    score: int | None = None
    rank: int | None = None


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
    if equity.empty:
        return 0.0
    peak = equity.cummax()
    return float((equity / peak - 1.0).min())


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


def _baseline_contributions(orders: pd.DataFrame, positions: pd.DataFrame) -> dict[str, dict[str, Any]]:
    buys = orders[(orders["action"] == "buy") & (orders["order_status"] == "filled")].copy()
    exits = orders[(orders["action"].isin(["exit", "stop"])) & (orders["order_status"] == "filled")].copy()
    latest = positions.sort_values("ymd", kind="stable").groupby("position_id", sort=False).tail(1) if not positions.empty else pd.DataFrame()
    out: dict[str, dict[str, Any]] = {}
    for _idx, buy in buys.iterrows():
        pid = str(buy["position_id"])
        exit_rows = exits[exits["position_id"].astype(str) == pid]
        if not exit_rows.empty:
            exit_row = exit_rows.iloc[-1]
            pnl = _safe_float(exit_row.get("realized_pnl")) or 0.0
            exit_ymd = int(exit_row["execution_ymd"])
            exit_reason = exit_row.get("reason_type")
        else:
            last = latest[latest["position_id"].astype(str) == pid]
            pnl = (_safe_float(last.iloc[-1].get("unrealized_pnl")) or 0.0) if not last.empty else 0.0
            exit_ymd = None
            exit_reason = "open_at_run_end"
        out[pid] = {"baseline_pnl": pnl, "baseline_exit_ymd": exit_ymd, "baseline_exit_reason": exit_reason}
    return out


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


def _future_recovery(raw_by_code: dict[str, pd.DataFrame], code: str, exit_ymd: int, exit_price: float) -> dict[str, Any]:
    frame = raw_by_code.get(code, pd.DataFrame())
    future = frame[frame["ymd"].astype(int) > int(exit_ymd)].head(20).copy()
    if future.empty or exit_price <= 0:
        return {
            "post_stop_ret5": None,
            "post_stop_ret10": None,
            "post_stop_ret20": None,
            "post_stop_max_up20": None,
            "recovery_type": "unavailable_no_future_bars",
        }

    def ret_at(n: int) -> float | None:
        if len(future) < n:
            return None
        return float(future.iloc[n - 1]["c"] / exit_price - 1.0)

    max_up = float(future["h"].max() / exit_price - 1.0)
    if max_up >= 0.08:
        recovery_type = "strong_shakeout_recovery"
    elif max_up >= 0.03:
        recovery_type = "moderate_recovery"
    elif max_up > 0:
        recovery_type = "minor_recovery"
    else:
        recovery_type = "no_recovery"
    return {
        "post_stop_ret5": ret_at(5),
        "post_stop_ret10": ret_at(10),
        "post_stop_ret20": ret_at(20),
        "post_stop_max_up20": max_up,
        "recovery_type": recovery_type,
    }


def _candidate_features_on(candidates: pd.DataFrame, code: str, ymd: int) -> dict[str, Any]:
    row = candidates[(candidates["code"].astype(str) == str(code)) & (candidates["decision_ymd"].astype(int) == int(ymd))]
    if row.empty:
        return {"candidate_snapshot_status": "not_in_top100_on_stop_trigger"}
    item = row.iloc[0]
    return {
        "candidate_snapshot_status": "available",
        "candidate_rank": item.get("candidate_rank"),
        "selection_score": item.get("selection_score"),
        "score_components_json": item.get("score_components_json"),
    }


def run_overlay(run_root: Path, source_db: Path) -> dict[str, Any]:
    run_config = _read_json(run_root / "run_config.json")
    actions = pd.DataFrame(_read_jsonl(run_root / "daily_action_ledger.jsonl"))
    orders = pd.read_csv(run_root / "orders_ledger.csv")
    positions = pd.read_csv(run_root / "positions_ledger.csv")
    baseline_equity = pd.read_csv(run_root / "equity_curve.csv")
    candidates = pd.read_csv(run_root / "daily_candidate_snapshot.csv")
    lifecycle = _read_json(run_root / "candidate_lifecycle_audit_v1" / "candidate_lifecycle_summary.json")
    invalidations = pd.read_csv(run_root / "candidate_lifecycle_audit_v1" / "invalidation_candle_cases.csv")
    false_recoveries = pd.read_csv(run_root / "candidate_lifecycle_audit_v1" / "false_invalidation_recovery_cases.csv")

    initial_cash = float(run_config["portfolio"]["initial_cash_jpy"])
    per_symbol_cap = float(run_config["portfolio"]["per_symbol_cap_jpy"])
    max_positions = int(run_config["portfolio"]["max_positions"])
    cost_model = run_config["cost_model"]
    profit_target = float(run_config["exit_rules"]["profit_target"])
    max_holding_days = int(run_config["exit_rules"]["max_holding_trading_days"])
    start_ymd = int(run_config["period"]["start_ymd"])
    end_ymd = int(run_config["period"]["end_ymd"])
    codes = set(orders["code"].astype(str)) | set(actions.get("code", pd.Series(dtype=str)).dropna().astype(str))
    raw = _load_daily_ohlc(source_db, codes=codes, start_ymd=start_ymd, end_ymd=end_ymd + 45)
    raw_by_code = {code: g.reset_index(drop=True) for code, g in raw.groupby("code", sort=False)}
    raw_lookup = {(str(row["code"]), int(row["ymd"])): row for _idx, row in raw.iterrows()}
    calendar = sorted(pd.to_numeric(baseline_equity["ymd"], errors="coerce").dropna().astype(int).unique().tolist())
    buy_actions = actions[actions.get("action", pd.Series(dtype=str)).astype(str) == "buy"].copy()
    buy_actions_by_day = {int(ymd): group.copy() for ymd, group in buy_actions.groupby("decision_ymd", sort=False)}
    baseline_by_pid = _baseline_contributions(orders, positions)

    pending: dict[int, list[PendingOrder]] = {}
    open_positions: dict[str, OverlayPosition] = {}
    cash = initial_cash
    order_seq = 0
    overlay_orders: list[dict[str, Any]] = []
    overlay_positions: list[dict[str, Any]] = []
    overlay_equity: list[dict[str, Any]] = []
    stop_cases: list[dict[str, Any]] = []
    exact_missing: list[dict[str, Any]] = []

    for ymd in calendar:
        for order in pending.pop(int(ymd), []):
            row = raw_lookup.get((order.code, int(ymd)))
            open_price = _safe_float(row.get("o")) if row is not None else None
            if open_price is None:
                exact_missing.append({"ymd": ymd, "code": order.code, "order_id": order.order_id, "reason": "missing_exact_open"})
                overlay_orders.append(
                    {
                        "order_id": order.order_id,
                        "decision_ymd": order.decision_ymd,
                        "execution_ymd": ymd,
                        "action": order.action,
                        "code": order.code,
                        "order_status": "unfilled",
                        "unfilled_reason": "missing_exact_open",
                    }
                )
                continue
            if order.action == "buy":
                if len(open_positions) >= max_positions or order.code in {p.code for p in open_positions.values()}:
                    status = "blocked_by_overlay_position_limit_or_duplicate"
                    shares = 0
                else:
                    shares = _shares_for_cash(cash, open_price, per_symbol_cap, cost_model)
                    status = "filled" if shares > 0 else "insufficient_cash_or_lot_size"
                if shares <= 0:
                    overlay_orders.append(
                        {
                            "order_id": order.order_id,
                            "decision_ymd": order.decision_ymd,
                            "execution_ymd": ymd,
                            "action": "buy",
                            "code": order.code,
                            "order_status": "unfilled",
                            "unfilled_reason": status,
                            "execution_price": open_price,
                        }
                    )
                    continue
                notional = shares * open_price
                cost_amount, slippage = _cost(notional, cost_model)
                cash -= notional + cost_amount
                position_id = f"{AXIS_ID}-{order.order_id}"
                pos = OverlayPosition(
                    position_id=position_id,
                    baseline_position_id=str(order.baseline_position_id),
                    code=order.code,
                    entry_order_id=order.order_id,
                    entry_decision_ymd=order.decision_ymd,
                    entry_ymd=ymd,
                    entry_price=open_price,
                    shares=shares,
                    notional=notional,
                    entry_cost=cost_amount,
                    cost_basis=notional + cost_amount,
                )
                open_positions[position_id] = pos
                overlay_orders.append(
                    {
                        "order_id": order.order_id,
                        "decision_ymd": order.decision_ymd,
                        "execution_ymd": ymd,
                        "action": "buy",
                        "code": order.code,
                        "order_status": "filled",
                        "execution_price": open_price,
                        "shares": shares,
                        "notional": notional,
                        "cost_amount": cost_amount,
                        "slippage_amount": slippage,
                        "cash_after": cash,
                        "position_id": position_id,
                        "baseline_position_id": order.baseline_position_id,
                        "reason_type": "baseline_buy_signal",
                    }
                )
                continue

            pos = open_positions.get(str(order.position_id))
            if pos is None:
                overlay_orders.append(
                    {
                        "order_id": order.order_id,
                        "decision_ymd": order.decision_ymd,
                        "execution_ymd": ymd,
                        "action": order.action,
                        "code": order.code,
                        "order_status": "unfilled",
                        "unfilled_reason": "position_already_closed",
                        "execution_price": open_price,
                        "position_id": order.position_id,
                    }
                )
                continue
            notional = pos.shares * open_price
            cost_amount, slippage = _cost(notional, cost_model)
            realized_pnl = notional - pos.cost_basis - cost_amount
            cash += notional - cost_amount
            open_positions.pop(pos.position_id, None)
            overlay_orders.append(
                {
                    "order_id": order.order_id,
                    "decision_ymd": order.decision_ymd,
                    "execution_ymd": ymd,
                    "action": order.action,
                    "code": order.code,
                    "order_status": "filled",
                    "execution_price": open_price,
                    "shares": pos.shares,
                    "notional": notional,
                    "cost_amount": cost_amount,
                    "slippage_amount": slippage,
                    "cash_after": cash,
                    "position_id": pos.position_id,
                    "baseline_position_id": pos.baseline_position_id,
                    "reason_type": order.reason_type,
                    "realized_pnl": realized_pnl,
                    "realized_return": realized_pnl / pos.cost_basis if pos.cost_basis else None,
                }
            )
            if order.reason_type == "fixed_stop_minus_8pct":
                base = baseline_by_pid.get(pos.baseline_position_id, {})
                recovery = _future_recovery(raw_by_code, pos.code, int(ymd), open_price)
                trigger_features = _candidate_features_on(candidates, pos.code, order.decision_ymd)
                stop_cases.append(
                    {
                        "position_id": pos.position_id,
                        "baseline_position_id": pos.baseline_position_id,
                        "code": pos.code,
                        "stop_trigger_date": order.decision_ymd,
                        "stop_exit_date": ymd,
                        "stop_threshold": STOP_THRESHOLD,
                        "average_entry_price": pos.entry_price,
                        "stop_exit_price": open_price,
                        "stop_realized_pnl": realized_pnl,
                        "baseline_pnl": base.get("baseline_pnl"),
                        "baseline_exit_ymd": base.get("baseline_exit_ymd"),
                        "baseline_exit_reason": base.get("baseline_exit_reason"),
                        "delta_vs_baseline": realized_pnl - float(base.get("baseline_pnl") or 0.0),
                        **recovery,
                        **trigger_features,
                    }
                )

        market_value = 0.0
        for pos in list(open_positions.values()):
            row = raw_lookup.get((pos.code, int(ymd)))
            close_price = _safe_float(row.get("c")) if row is not None else None
            if close_price is None:
                exact_missing.append({"ymd": ymd, "code": pos.code, "position_id": pos.position_id, "reason": "missing_close_for_mark"})
                continue
            pos.holding_days += 1
            value = pos.shares * close_price
            pnl = value - pos.cost_basis
            market_value += value
            overlay_positions.append(
                {
                    "ymd": ymd,
                    "position_id": pos.position_id,
                    "baseline_position_id": pos.baseline_position_id,
                    "code": pos.code,
                    "shares": pos.shares,
                    "entry_ymd": pos.entry_ymd,
                    "entry_price": pos.entry_price,
                    "close_price": close_price,
                    "market_value": value,
                    "cost_basis": pos.cost_basis,
                    "unrealized_pnl": pnl,
                    "holding_days": pos.holding_days,
                    "pending_exit_order_id": pos.pending_exit_order_id,
                }
            )
            if pos.pending_exit_order_id is not None:
                continue
            stop_return = close_price / pos.entry_price - 1.0
            profit_return = pnl / pos.cost_basis if pos.cost_basis else 0.0
            next_open = _safe_float(row.get("next_open"))
            next_ymd = _safe_int(row.get("next_ymd"))
            exit_action = None
            reason_type = None
            if stop_return <= STOP_THRESHOLD:
                exit_action = "stop"
                reason_type = "fixed_stop_minus_8pct"
            elif profit_return >= profit_target:
                exit_action = "exit"
                reason_type = "profit_target"
            elif pos.holding_days >= max_holding_days:
                exit_action = "exit"
                reason_type = "time_stop"
            if exit_action and next_open is not None and next_ymd is not None:
                order_seq += 1
                order_id = f"{AXIS_ID}-{order_seq:08d}"
                pos.pending_exit_order_id = order_id
                pending.setdefault(next_ymd, []).append(
                    PendingOrder(
                        order_id=order_id,
                        action=exit_action,
                        code=pos.code,
                        decision_ymd=int(ymd),
                        execution_ymd=next_ymd,
                        reason_type=reason_type,
                        baseline_position_id=pos.baseline_position_id,
                        position_id=pos.position_id,
                    )
                )
            elif exit_action:
                exact_missing.append({"ymd": ymd, "code": pos.code, "position_id": pos.position_id, "reason": "missing_next_open_for_exit"})

        for _idx, action in buy_actions_by_day.get(int(ymd), pd.DataFrame()).iterrows():
            code = str(action.get("code"))
            row = raw_lookup.get((code, int(ymd)))
            next_ymd = _safe_int(row.get("next_ymd")) if row is not None else _safe_int(action.get("execution_ymd"))
            if next_ymd is None:
                exact_missing.append({"ymd": ymd, "code": code, "reason": "missing_next_ymd_for_buy_signal"})
                continue
            baseline_order_id = str(action.get("order_id"))
            baseline_pid = None
            match = orders[(orders["order_id"].astype(str) == baseline_order_id) | ((orders["decision_ymd"].astype(int) == int(ymd)) & (orders["code"].astype(str) == code) & (orders["action"].astype(str) == "buy"))]
            if not match.empty:
                baseline_pid = str(match.iloc[0].get("position_id"))
            pending.setdefault(next_ymd, []).append(
                PendingOrder(
                    order_id=f"{AXIS_ID}-buy-{baseline_order_id}",
                    action="buy",
                    code=code,
                    decision_ymd=int(ymd),
                    execution_ymd=next_ymd,
                    reason_type="baseline_buy_signal",
                    baseline_position_id=baseline_pid,
                    score=_safe_int(action.get("selection_score")),
                    rank=_safe_int(action.get("candidate_rank")),
                )
            )

        equity_value = cash + market_value
        base_row = baseline_equity[baseline_equity["ymd"].astype(int) == int(ymd)]
        overlay_equity.append(
            {
                "ymd": ymd,
                "cash": cash,
                "positions_market_value": market_value,
                "equity": equity_value,
                "baseline_equity": None if base_row.empty else float(base_row.iloc[0]["equity"]),
                "open_position_count": len(open_positions),
                "exact_next_open_replay": len(exact_missing) == 0,
            }
        )

    stop_cases_df = pd.DataFrame(stop_cases)
    overlay_orders_df = pd.DataFrame(overlay_orders)
    overlay_positions_df = pd.DataFrame(overlay_positions)
    overlay_equity_df = pd.DataFrame(overlay_equity)
    exact_next_open = len(exact_missing) == 0
    saved = stop_cases_df[stop_cases_df["delta_vs_baseline"] > 0].copy() if not stop_cases_df.empty else pd.DataFrame()
    missed = stop_cases_df[stop_cases_df["delta_vs_baseline"] < 0].copy() if not stop_cases_df.empty else pd.DataFrame()
    false_stop = stop_cases_df[stop_cases_df["post_stop_max_up20"].fillna(0) >= 0.03].copy() if not stop_cases_df.empty else pd.DataFrame()
    feature_distribution = (
        stop_cases_df.groupby(["candidate_snapshot_status", "candidate_rank", "selection_score"], dropna=False)
        .size()
        .reset_index(name="stop_count")
        if not stop_cases_df.empty
        else pd.DataFrame(columns=["candidate_snapshot_status", "candidate_rank", "selection_score", "stop_count"])
    )

    baseline_final = float(baseline_equity.iloc[-1]["equity"])
    stop_final = float(overlay_equity_df.iloc[-1]["equity"]) if not overlay_equity_df.empty else baseline_final
    metrics = {
        "baseline_final_equity": baseline_final,
        "stop_final_equity": stop_final,
        "delta_final_equity": stop_final - baseline_final,
        "baseline_max_drawdown": _max_drawdown(pd.to_numeric(baseline_equity["equity"], errors="coerce")),
        "stop_max_drawdown": _max_drawdown(pd.to_numeric(overlay_equity_df["equity"], errors="coerce")),
        "delta_max_drawdown": _max_drawdown(pd.to_numeric(overlay_equity_df["equity"], errors="coerce")) - _max_drawdown(pd.to_numeric(baseline_equity["equity"], errors="coerce")),
        "stop_trigger_count": int(len(stop_cases_df)),
        "saved_loss_total": float(saved["delta_vs_baseline"].sum()) if not saved.empty else 0.0,
        "missed_profit_total": float((-missed["delta_vs_baseline"]).sum()) if not missed.empty else 0.0,
        "false_stop_recovery_count": int(len(false_stop)),
        "invalidation_count": int(lifecycle.get("metrics", {}).get("invalidation_cases") or len(invalidations)),
        "shakeout_recovery_count": int(lifecycle.get("metrics", {}).get("false_invalidation_recovery_cases") or len(false_recoveries)),
        "true_breakdown_count": int(max(0, (lifecycle.get("metrics", {}).get("invalidation_cases") or len(invalidations)) - (lifecycle.get("metrics", {}).get("false_invalidation_recovery_cases") or len(false_recoveries)))),
        "largest_saved_loss": float(saved["delta_vs_baseline"].max()) if not saved.empty else 0.0,
        "largest_missed_profit": float((-missed["delta_vs_baseline"]).max()) if not missed.empty else 0.0,
        "cost_delta": float(pd.to_numeric(overlay_orders_df.get("cost_amount", pd.Series(dtype=float)), errors="coerce").sum() - pd.to_numeric(orders.get("cost_amount", pd.Series(dtype=float)), errors="coerce").sum()),
        "order_count_delta": int(len(overlay_orders_df) - len(orders)),
        "exact_next_open_replay": exact_next_open,
    }
    return {
        "frames": {
            "orders": overlay_orders_df,
            "positions": overlay_positions_df,
            "equity": overlay_equity_df,
            "stops": stop_cases_df,
            "saved": saved,
            "false_stop": false_stop,
            "missed": missed,
            "feature_distribution": feature_distribution,
        },
        "metrics": metrics,
        "exact_missing": exact_missing,
        "source_db": str(source_db),
        "run_config": run_config,
    }


def _decision(metrics: dict[str, Any]) -> tuple[str, str]:
    if not metrics["exact_next_open_replay"]:
        return "rerun_required_due_to_exact_price_unavailable", "exact_next_open_price_missing"
    dd_improved = metrics["stop_max_drawdown"] > metrics["baseline_max_drawdown"]
    equity_improved = metrics["stop_final_equity"] > metrics["baseline_final_equity"]
    if equity_improved and dd_improved and metrics["saved_loss_total"] > metrics["missed_profit_total"] and metrics["false_stop_recovery_count"] <= max(3, metrics["stop_trigger_count"] // 2):
        return "keep_for_replay_challenger", "final_equity_and_saved_loss_improved"
    if equity_improved and dd_improved:
        return "hold_due_to_recovery_risk", "portfolio_improved_but_saved_loss_attribution_or_recovery_risk_not_clean"
    flat_equity = abs(metrics["delta_final_equity"]) <= metrics["baseline_final_equity"] * 0.01
    if dd_improved and flat_equity:
        return "hold_due_to_recovery_risk", "drawdown_improved_but_equity_flat_or_recovery_risk"
    if metrics["missed_profit_total"] > metrics["saved_loss_total"] or metrics["stop_final_equity"] < metrics["baseline_final_equity"]:
        return "drop_due_to_profit_damage", "profit_damage_or_final_equity_regression"
    if not dd_improved:
        return "drop_due_to_no_drawdown_improvement", "max_drawdown_not_improved"
    return "hold_due_to_recovery_risk", "mixed_result"


def run_stop_too_wide_pretest_v1(run_root: str | Path, source_db: str | Path | None = None, output_root: str | Path | None = None) -> dict[str, Any]:
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

    result = run_overlay(run_root, db_path)
    frames = result["frames"]
    metrics = result["metrics"]
    decision, reason = _decision(metrics)

    _write_csv(output_root / "stop_triggered_cases.csv", frames["stops"])
    _write_csv(output_root / "stop_overlay_orders_ledger.csv", frames["orders"])
    _write_csv(output_root / "stop_overlay_positions_ledger.csv", frames["positions"])
    _write_csv(output_root / "stop_overlay_equity_curve.csv", frames["equity"])
    _write_csv(output_root / "saved_loss_cases.csv", frames["saved"])
    _write_csv(output_root / "false_stop_recovery_cases.csv", frames["false_stop"])
    _write_csv(output_root / "missed_profit_after_stop_cases.csv", frames["missed"])
    _write_csv(output_root / "stop_feature_distribution.csv", frames["feature_distribution"])

    comparison = {
        "schema_version": f"{SCHEMA_PREFIX}_baseline_vs_stop_comparison_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "metrics": metrics,
        "method": {
            "db_backed": True,
            "exact_next_open_replay": metrics["exact_next_open_replay"],
            "stop_threshold": STOP_THRESHOLD,
            "stop_basis": "close_return_vs_average_entry_price",
            "decision_timing": "after_close",
            "fill_timing": "next_trading_session_open",
            "threshold_sweep": False,
            "invalidation_exit_mixed": False,
        },
    }
    _write_json(output_root / "baseline_vs_stop_comparison.json", comparison)

    manifest = {
        "schema_version": f"{SCHEMA_PREFIX}_selection_feature_manifest_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "selection_allowed_columns": ["ymd", "code", "close", "average_entry_price", "close_return_vs_average_entry_price"],
        "selection_forbidden_columns": ["post_ret_5", "post_ret_10", "post_ret_20", "post_ret_40", "mae_20", "mfe_20", "shakeout_recovery", "true_breakdown"],
        "diagnostic_only_columns": ["post_stop_ret5", "post_stop_ret10", "post_stop_ret20", "post_stop_max_up20", "recovery_type", "shakeout_recovery_count", "true_breakdown_count"],
        "outcome_label_columns": ["post_stop_ret5", "post_stop_ret10", "post_stop_ret20", "post_stop_max_up20"],
        "audit_result": "pass",
    }
    _write_json(output_root / "selection_feature_manifest.json", manifest)

    audit = {
        "schema_version": f"{SCHEMA_PREFIX}_no_lookahead_audit_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "audit_result": "pass" if metrics["exact_next_open_replay"] else "research_fallback",
        "selection_feature_manifest": "selection_feature_manifest.json",
        "post_run_outcomes_used_for_exit_condition": False,
        "shakeout_recovery_used_for_exit_condition": False,
        "true_breakdown_used_for_exit_condition": False,
        "threshold_sweep": False,
        "fixed_stop_threshold": STOP_THRESHOLD,
        "same_day_close_fill_used": False,
        "exact_next_open_replay": metrics["exact_next_open_replay"],
        "missing_exact_price_events": result["exact_missing"],
        "silent_fallback_used": False,
    }
    _write_json(output_root / "no_lookahead_audit.json", audit)

    next_axis = {
        "schema_version": f"{SCHEMA_PREFIX}_next_axis_decision_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "decision_candidates": list(DECISIONS),
        "decision": decision,
        "decision_count": 1,
        "reason_type": reason,
        "metrics": metrics,
        "policy": {
            "single_axis_only": True,
            "fixed_stop_threshold": STOP_THRESHOLD,
            "threshold_sweep": False,
            "invalidation_exit_mixed": False,
            "daily_volume_normal_veto_mixed": False,
            "post_run_outcome_used_for_exit_condition": False,
            "production_policy_changed": False,
        },
    }
    _write_json(output_root / "next_axis_decision.json", next_axis)

    summary = {
        "schema_version": f"{SCHEMA_PREFIX}_summary_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "run_root": str(run_root),
        "source_db": str(db_path),
        "scope": {
            "tradex_only": True,
            "db_backed_exact_pretest": True,
            "same_run": True,
            "same_period": True,
            "same_baseline_policy_except_stop_axis": True,
            "same_max_positions": True,
            "same_cost_slippage": True,
            "single_axis_only": True,
            "replay_rerun": "counterfactual_overlay_only",
            "production_policy_changed": False,
            "meemee_ui_changed": False,
            "runtime_db_written": False,
            "ranking_changed": False,
            "publish_registry_changed": False,
        },
        "stop_rule": {
            "threshold": STOP_THRESHOLD,
            "basis": "average_entry_price",
            "decision_timing": "after_close",
            "execution": "next_trading_session_open",
            "threshold_sweep": False,
        },
        "shakeout_handling": {
            "invalidation_is_exit_condition": False,
            "shakeout_recovery_is_exit_condition": False,
            "true_breakdown_is_exit_condition": False,
            "diagnostic_only": True,
        },
        "metrics": metrics,
        "decision": decision,
        "decision_reason_type": reason,
        "source_artifacts": status,
        "limitations": [] if metrics["exact_next_open_replay"] else ["missing exact next-open events; result is not keep-eligible"],
    }
    _write_json(output_root / "stop_too_wide_pretest_summary.json", summary)

    complete = {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "run_root": str(run_root),
        "output_root": str(output_root),
        "complete": True,
        "required_artifacts_all_present": all((output_root / name).exists() for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json"),
        "source_artifacts_all_present": all(status.values()),
        "decision": decision,
        "decision_count": 1,
        "exact_next_open_replay": metrics["exact_next_open_replay"],
        "no_lookahead_audit": audit["audit_result"],
        "fixed_stop_threshold": STOP_THRESHOLD,
        "threshold_sweep": False,
        "invalidation_exit_mixed": False,
        "daily_volume_normal_veto_mixed": False,
        "post_run_outcome_used_for_exit_condition": False,
        "silent_fallback_used": False,
        "research_fallback_recorded": not metrics["exact_next_open_replay"],
    }
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    return {"complete": True, "output_root": str(output_root), "decision": decision, "metrics": metrics}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DB-backed fixed -8% stop-too-wide pretest.")
    parser.add_argument("--run-root", required=True, type=Path)
    parser.add_argument("--source-db", type=Path, default=None)
    parser.add_argument("--output-root", type=Path, default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    print(_json_text(run_stop_too_wide_pretest_v1(args.run_root, args.source_db, args.output_root)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
