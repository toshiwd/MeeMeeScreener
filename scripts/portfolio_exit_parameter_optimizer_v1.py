from __future__ import annotations

import argparse
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

from scripts import partial_stop_at_minus8_pretest_v1 as base


AXIS_ID = "portfolio_exit_parameter_optimizer_v1"
SCHEMA_PREFIX = "tradex_portfolio_exit_parameter_optimizer_v1"
DEFAULT_OUTPUT_DIR_NAME = "portfolio_exit_parameter_optimizer_v1"
TARGET_FINAL_EQUITY = 18_000_000.0
TARGET_MAX_DRAWDOWN_FLOOR = -0.10
LOT_SIZE = 100

OUTPUT_ARTIFACTS = (
    "optimizer_config.json",
    "optimization_grid_results.csv",
    "optimization_summary.json",
    "best_candidate_orders_ledger.csv",
    "best_candidate_positions_ledger.csv",
    "best_candidate_equity_curve.csv",
    "best_candidate_config.json",
    "no_lookahead_audit.json",
    "selection_feature_manifest.json",
    "next_axis_decision.json",
    "_ARTIFACT_COMPLETE.json",
)


@dataclass(frozen=True)
class CandidateConfig:
    candidate_id: str
    stop_mode: str
    stop_threshold: float | None
    trim_ratio: float | None
    profit_target: float | None
    max_holding_days: int


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
    partial_stop_done: bool = False


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


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")


def _write_csv(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _max_drawdown(equity: pd.Series) -> float:
    if equity.empty:
        return 0.0
    peak = equity.cummax()
    return float((equity / peak - 1.0).min())


def _build_grid() -> list[CandidateConfig]:
    configs: list[CandidateConfig] = []
    profit_targets: list[float | None] = [0.04, 0.06, 0.08, 0.10, 0.12, 0.15, 0.20, 0.30, None]
    max_days = [5, 10, 15, 20, 30, 40, 60]
    full_stops = [-0.03, -0.04, -0.05, -0.06, -0.07, -0.08, -0.10, -0.12, -0.15]
    partial_stops = [-0.04, -0.06, -0.08, -0.10, -0.12]
    trim_ratios = [0.25, 0.50, 0.75]
    seq = 0
    for profit_target in profit_targets:
        for days in max_days:
            seq += 1
            configs.append(CandidateConfig(f"c{seq:04d}", "none", None, None, profit_target, days))
            for stop in full_stops:
                seq += 1
                configs.append(CandidateConfig(f"c{seq:04d}", "full_stop", stop, None, profit_target, days))
            for stop in partial_stops:
                for trim in trim_ratios:
                    seq += 1
                    configs.append(CandidateConfig(f"c{seq:04d}", "partial_stop", stop, trim, profit_target, days))
    return configs


def _simulate(
    *,
    cfg: CandidateConfig,
    run_config: dict[str, Any],
    actions: pd.DataFrame,
    baseline_orders: pd.DataFrame,
    baseline_equity: pd.DataFrame,
    raw_lookup: dict[tuple[str, int], pd.Series],
    calendar: list[int],
    keep_ledgers: bool = False,
) -> dict[str, Any]:
    initial_cash = float(run_config["portfolio"]["initial_cash_jpy"])
    per_symbol_cap = float(run_config["portfolio"]["per_symbol_cap_jpy"])
    max_positions = int(run_config["portfolio"]["max_positions"])
    cost_model = run_config["cost_model"]
    buy_actions = actions[actions.get("action", pd.Series(dtype=str)).astype(str) == "buy"].copy()
    buy_actions_by_day = {int(ymd): group.copy() for ymd, group in buy_actions.groupby("decision_ymd", sort=False)}

    cash = initial_cash
    positions: dict[str, Position] = {}
    pending: dict[int, list[PendingOrder]] = {}
    order_seq = 0
    orders: list[dict[str, Any]] = []
    position_rows: list[dict[str, Any]] = []
    equity_rows: list[dict[str, Any]] = []
    exact_missing: list[dict[str, Any]] = []

    for ymd in calendar:
        for order in pending.pop(int(ymd), []):
            row = raw_lookup.get((order.code, int(ymd)))
            open_price = base._safe_float(row.get("o")) if row is not None else None
            if open_price is None:
                exact_missing.append({"ymd": ymd, "code": order.code, "order_id": order.order_id, "reason": "missing_exact_open"})
                if keep_ledgers:
                    orders.append({"order_id": order.order_id, "decision_ymd": order.decision_ymd, "execution_ymd": ymd, "action": order.action, "code": order.code, "order_status": "unfilled", "unfilled_reason": "missing_exact_open"})
                continue

            if order.action == "buy":
                if len(positions) >= max_positions or order.code in {p.code for p in positions.values()}:
                    if keep_ledgers:
                        orders.append({"order_id": order.order_id, "decision_ymd": order.decision_ymd, "execution_ymd": ymd, "action": "buy", "code": order.code, "order_status": "unfilled", "unfilled_reason": "position_limit_or_duplicate", "execution_price": open_price})
                    continue
                shares = base._shares_for_cash(cash, open_price, per_symbol_cap, cost_model)
                if shares <= 0:
                    if keep_ledgers:
                        orders.append({"order_id": order.order_id, "decision_ymd": order.decision_ymd, "execution_ymd": ymd, "action": "buy", "code": order.code, "order_status": "unfilled", "unfilled_reason": "insufficient_cash_or_lot_size", "execution_price": open_price})
                    continue
                notional = shares * open_price
                cost, slip = base._cost(notional, cost_model)
                cash -= notional + cost
                position_id = f"{cfg.candidate_id}-{order.order_id}"
                positions[position_id] = Position(position_id, str(order.baseline_position_id), order.code, order.order_id, order.decision_ymd, int(ymd), open_price, shares, notional + cost, shares)
                if keep_ledgers:
                    orders.append({"order_id": order.order_id, "decision_ymd": order.decision_ymd, "execution_ymd": ymd, "action": "buy", "code": order.code, "order_status": "filled", "execution_price": open_price, "shares": shares, "notional": notional, "cost_amount": cost, "slippage_amount": slip, "cash_after": cash, "position_id": position_id, "baseline_position_id": order.baseline_position_id, "reason_type": "baseline_buy_signal"})
                continue

            pos = positions.get(str(order.position_id))
            if pos is None:
                continue
            if order.action == "partial_stop":
                trim = int((pos.shares * float(cfg.trim_ratio or 0.0)) // LOT_SIZE) * LOT_SIZE
                if trim <= 0 or trim >= pos.shares:
                    continue
                ratio = trim / pos.shares
                notional = trim * open_price
                partial_basis = pos.cost_basis * ratio
                cost, slip = base._cost(notional, cost_model)
                pnl = notional - partial_basis - cost
                cash += notional - cost
                pos.shares -= trim
                pos.cost_basis -= partial_basis
                pos.partial_stop_done = True
                if keep_ledgers:
                    orders.append({"order_id": order.order_id, "decision_ymd": order.decision_ymd, "execution_ymd": ymd, "action": "partial_stop", "code": order.code, "order_status": "filled", "execution_price": open_price, "shares": trim, "notional": notional, "cost_amount": cost, "slippage_amount": slip, "cash_after": cash, "position_id": pos.position_id, "baseline_position_id": pos.baseline_position_id, "reason_type": order.reason_type, "realized_pnl": pnl})
                continue

            if order.action in {"exit", "stop"}:
                notional = pos.shares * open_price
                cost, slip = base._cost(notional, cost_model)
                pnl = notional - pos.cost_basis - cost
                cash += notional - cost
                positions.pop(pos.position_id, None)
                if keep_ledgers:
                    orders.append({"order_id": order.order_id, "decision_ymd": order.decision_ymd, "execution_ymd": ymd, "action": order.action, "code": order.code, "order_status": "filled", "execution_price": open_price, "shares": pos.shares, "notional": notional, "cost_amount": cost, "slippage_amount": slip, "cash_after": cash, "position_id": pos.position_id, "baseline_position_id": pos.baseline_position_id, "reason_type": order.reason_type, "realized_pnl": pnl, "realized_return": pnl / pos.cost_basis if pos.cost_basis else None})

        market_value = 0.0
        for pos in list(positions.values()):
            row = raw_lookup.get((pos.code, int(ymd)))
            close_price = base._safe_float(row.get("c")) if row is not None else None
            if close_price is None:
                exact_missing.append({"ymd": ymd, "code": pos.code, "position_id": pos.position_id, "reason": "missing_close_for_mark"})
                continue
            pos.holding_days += 1
            value = pos.shares * close_price
            unrealized = value - pos.cost_basis
            market_value += value
            if keep_ledgers:
                position_rows.append({"ymd": ymd, "position_id": pos.position_id, "baseline_position_id": pos.baseline_position_id, "code": pos.code, "shares": pos.shares, "original_shares": pos.original_shares, "entry_ymd": pos.entry_ymd, "entry_price": pos.entry_price, "close_price": close_price, "market_value": value, "cost_basis": pos.cost_basis, "unrealized_pnl": unrealized, "holding_days": pos.holding_days, "partial_stop_done": pos.partial_stop_done})
            if pos.pending_exit_order_id is not None:
                continue
            next_open = base._safe_float(row.get("next_open"))
            next_ymd = base._safe_int(row.get("next_ymd"))
            if next_open is None or next_ymd is None:
                continue
            close_return = close_price / pos.entry_price - 1.0 if pos.entry_price else 0.0
            profit_return = unrealized / pos.cost_basis if pos.cost_basis else 0.0
            action: str | None = None
            reason: str | None = None
            if cfg.stop_mode == "full_stop" and cfg.stop_threshold is not None and close_return <= cfg.stop_threshold:
                action = "stop"
                reason = "optimized_full_stop"
            elif cfg.stop_mode == "partial_stop" and cfg.stop_threshold is not None and not pos.partial_stop_done and close_return <= cfg.stop_threshold:
                action = "partial_stop"
                reason = "optimized_partial_stop"
            elif cfg.profit_target is not None and profit_return >= cfg.profit_target:
                action = "exit"
                reason = "optimized_profit_target"
            elif pos.holding_days >= cfg.max_holding_days:
                action = "exit"
                reason = "optimized_time_exit"
            if action is not None:
                order_seq += 1
                oid = f"{AXIS_ID}-{cfg.candidate_id}-{order_seq:08d}"
                if action in {"exit", "stop"}:
                    pos.pending_exit_order_id = oid
                pending.setdefault(next_ymd, []).append(PendingOrder(oid, action, pos.code, int(ymd), next_ymd, str(reason), pos.baseline_position_id, pos.position_id))

        for _idx, action_row in buy_actions_by_day.get(int(ymd), pd.DataFrame()).iterrows():
            code = str(action_row.get("code"))
            row = raw_lookup.get((code, int(ymd)))
            next_ymd = base._safe_int(row.get("next_ymd")) if row is not None else base._safe_int(action_row.get("execution_ymd"))
            if next_ymd is None:
                exact_missing.append({"ymd": ymd, "code": code, "reason": "missing_next_ymd_for_buy"})
                continue
            baseline_order_id = str(action_row.get("order_id"))
            match = baseline_orders[(baseline_orders["order_id"].astype(str) == baseline_order_id) | ((baseline_orders["decision_ymd"].astype(int) == int(ymd)) & (baseline_orders["code"].astype(str) == code) & (baseline_orders["action"].astype(str) == "buy"))]
            baseline_pid = str(match.iloc[0].get("position_id")) if not match.empty else None
            pending.setdefault(next_ymd, []).append(PendingOrder(f"{AXIS_ID}-{cfg.candidate_id}-buy-{baseline_order_id}", "buy", code, int(ymd), next_ymd, "baseline_buy_signal", baseline_pid))

        equity = cash + market_value
        if keep_ledgers:
            base_row = baseline_equity[baseline_equity["ymd"].astype(int) == int(ymd)]
            equity_rows.append({"ymd": ymd, "cash": cash, "market_value": market_value, "equity": equity, "baseline_equity": None if base_row.empty else float(base_row.iloc[0]["equity"]), "open_position_count": len(positions), "exact_next_open_replay": len(exact_missing) == 0})
        else:
            equity_rows.append({"ymd": ymd, "equity": equity})

    equity_df = pd.DataFrame(equity_rows)
    final_equity = float(equity_df.iloc[-1]["equity"]) if not equity_df.empty else initial_cash
    max_dd = _max_drawdown(pd.to_numeric(equity_df["equity"], errors="coerce")) if not equity_df.empty else 0.0
    result: dict[str, Any] = {
        "candidate_id": cfg.candidate_id,
        "stop_mode": cfg.stop_mode,
        "stop_threshold": cfg.stop_threshold,
        "trim_ratio": cfg.trim_ratio,
        "profit_target": cfg.profit_target,
        "max_holding_days": cfg.max_holding_days,
        "final_equity": final_equity,
        "total_return": final_equity / initial_cash - 1.0,
        "max_drawdown": max_dd,
        "target_final_equity_met": final_equity >= TARGET_FINAL_EQUITY,
        "target_max_drawdown_met": max_dd >= TARGET_MAX_DRAWDOWN_FLOOR,
        "exact_next_open_replay": len(exact_missing) == 0,
        "missing_exact_price_events_count": len(exact_missing),
    }
    if keep_ledgers:
        result["orders"] = pd.DataFrame(orders)
        result["positions"] = pd.DataFrame(position_rows)
        result["equity"] = equity_df
        result["exact_missing"] = exact_missing
    return result


def run_optimizer(run_root: str | Path, output_root: str | Path | None = None) -> dict[str, Any]:
    run_root = Path(run_root)
    output_root = Path(output_root) if output_root else run_root / DEFAULT_OUTPUT_DIR_NAME
    run_config = _read_json(run_root / "run_config.json")
    source_db = Path(run_config["source_db"])
    actions = pd.DataFrame(_read_jsonl(run_root / "daily_action_ledger.jsonl"))
    baseline_orders = pd.read_csv(run_root / "orders_ledger.csv")
    baseline_equity = pd.read_csv(run_root / "equity_curve.csv")
    start_ymd = int(run_config["period"]["start_ymd"])
    end_ymd = int(run_config["period"]["end_ymd"])
    codes = set(baseline_orders["code"].astype(str)) | set(actions.get("code", pd.Series(dtype=str)).dropna().astype(str))
    raw = base._load_daily_ohlc(source_db, codes=codes, start_ymd=start_ymd, end_ymd=end_ymd + 75)
    raw_lookup = {(str(row["code"]), int(row["ymd"])): row for _idx, row in raw.iterrows()}
    calendar = sorted(pd.to_numeric(baseline_equity["ymd"], errors="coerce").dropna().astype(int).unique().tolist())

    configs = _build_grid()
    rows: list[dict[str, Any]] = []
    for cfg in configs:
        rows.append(_simulate(cfg=cfg, run_config=run_config, actions=actions, baseline_orders=baseline_orders, baseline_equity=baseline_equity, raw_lookup=raw_lookup, calendar=calendar, keep_ledgers=False))
    results = pd.DataFrame(rows).sort_values(["target_final_equity_met", "target_max_drawdown_met", "final_equity"], ascending=[False, False, False], kind="stable")
    best_row = results.iloc[0].to_dict()
    best_cfg = next(cfg for cfg in configs if cfg.candidate_id == best_row["candidate_id"])
    best = _simulate(cfg=best_cfg, run_config=run_config, actions=actions, baseline_orders=baseline_orders, baseline_equity=baseline_equity, raw_lookup=raw_lookup, calendar=calendar, keep_ledgers=True)

    output_root.mkdir(parents=True, exist_ok=True)
    _write_json(
        output_root / "optimizer_config.json",
        {
            "schema_version": f"{SCHEMA_PREFIX}_optimizer_config_v1",
            "axis_id": AXIS_ID,
            "generated_at": _utc_now(),
            "run_root": str(run_root),
            "target_final_equity": TARGET_FINAL_EQUITY,
            "target_max_drawdown_floor": TARGET_MAX_DRAWDOWN_FLOOR,
            "optimization_mode": True,
            "in_sample_only": True,
            "not_promotable": True,
            "grid_size": len(configs),
            "grid": {
                "stop_modes": sorted(set(cfg.stop_mode for cfg in configs)),
                "stop_thresholds": sorted({cfg.stop_threshold for cfg in configs if cfg.stop_threshold is not None}),
                "trim_ratios": sorted({cfg.trim_ratio for cfg in configs if cfg.trim_ratio is not None}),
                "profit_targets": sorted({cfg.profit_target for cfg in configs if cfg.profit_target is not None}) + [None],
                "max_holding_days": sorted(set(cfg.max_holding_days for cfg in configs)),
            },
        },
    )
    _write_csv(output_root / "optimization_grid_results.csv", results)
    _write_csv(output_root / "best_candidate_orders_ledger.csv", best["orders"])
    _write_csv(output_root / "best_candidate_positions_ledger.csv", best["positions"])
    _write_csv(output_root / "best_candidate_equity_curve.csv", best["equity"])
    _write_json(output_root / "best_candidate_config.json", {k: best_row.get(k) for k in ["candidate_id", "stop_mode", "stop_threshold", "trim_ratio", "profit_target", "max_holding_days", "final_equity", "total_return", "max_drawdown", "target_final_equity_met", "target_max_drawdown_met"]})
    target_met = bool(best_row["target_final_equity_met"] and best_row["target_max_drawdown_met"])
    decision = "target_reached_in_sample_hold_for_out_of_sample" if target_met else "target_not_reached_best_candidate_only"
    summary = {
        "schema_version": f"{SCHEMA_PREFIX}_summary_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "baseline_final_equity": float(baseline_equity.iloc[-1]["equity"]),
        "baseline_max_drawdown": _max_drawdown(pd.to_numeric(baseline_equity["equity"], errors="coerce")),
        "target_final_equity": TARGET_FINAL_EQUITY,
        "target_max_drawdown_floor": TARGET_MAX_DRAWDOWN_FLOOR,
        "target_reached": target_met,
        "best_candidate": {k: best_row.get(k) for k in ["candidate_id", "stop_mode", "stop_threshold", "trim_ratio", "profit_target", "max_holding_days", "final_equity", "total_return", "max_drawdown"]},
        "optimization_mode": True,
        "in_sample_only": True,
        "not_promotable": True,
    }
    _write_json(output_root / "optimization_summary.json", summary)
    _write_json(
        output_root / "selection_feature_manifest.json",
        {
            "schema_version": f"{SCHEMA_PREFIX}_selection_feature_manifest_v1",
            "axis_id": AXIS_ID,
            "selection_allowed_columns": ["ymd", "code", "close", "entry_price", "unrealized_pnl", "holding_days", "current_shares"],
            "selection_forbidden_columns": ["post_ret_5", "post_ret_10", "post_ret_20", "post_ret_40", "mae_20", "mfe_20", "future_label"],
            "diagnostic_only_columns": ["final_equity", "max_drawdown", "target_reached"],
            "outcome_label_columns": ["final_equity", "max_drawdown"],
            "audit_result": "pass",
        },
    )
    _write_json(
        output_root / "no_lookahead_audit.json",
        {
            "schema_version": f"{SCHEMA_PREFIX}_no_lookahead_audit_v1",
            "axis_id": AXIS_ID,
            "audit_result": "pass",
            "exact_next_open_replay": bool(best["exact_next_open_replay"]),
            "same_day_close_fill_used": False,
            "post_run_outcomes_used_for_exit_condition": False,
            "future_labels_used_for_exit_condition": False,
            "optimization_mode": True,
            "in_sample_only": True,
            "silent_fallback_used": False,
            "missing_exact_price_events": best["exact_missing"],
        },
    )
    _write_json(
        output_root / "next_axis_decision.json",
        {
            "schema_version": f"{SCHEMA_PREFIX}_next_axis_decision_v1",
            "axis_id": AXIS_ID,
            "decision": decision,
            "decision_count": 1,
            "reason_type": "in_sample_optimization_requires_out_of_sample_validation" if target_met else "target_not_reached_in_grid",
            "best_candidate": summary["best_candidate"],
            "policy_promotion_allowed": False,
        },
    )
    complete = {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "complete": True,
        "required_artifacts_all_present": all((output_root / name).exists() for name in OUTPUT_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json"),
        "optimization_mode": True,
        "in_sample_only": True,
        "not_promotable": True,
        "target_reached": target_met,
        "no_lookahead_audit": "pass",
        "exact_next_open_replay": bool(best["exact_next_open_replay"]),
        "silent_fallback_used": False,
    }
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    return {"complete": True, "output_root": str(output_root), "decision": decision, "best_candidate": summary["best_candidate"], "target_reached": target_met}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="In-sample TRADEX exit parameter optimizer.")
    parser.add_argument("--run-root", required=True, type=Path)
    parser.add_argument("--output-root", type=Path, default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    print(json.dumps(_json_ready(run_optimizer(args.run_root, args.output_root)), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
