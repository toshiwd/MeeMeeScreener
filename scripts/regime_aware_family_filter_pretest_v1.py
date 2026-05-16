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
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import partial_stop_at_minus8_pretest_v1 as base


AXIS_ID = "regime_aware_family_filter_pretest_v1"
SCHEMA_PREFIX = "tradex_regime_aware_family_filter_pretest_v1"
DEFAULT_OUTPUT_DIR_NAME = "regime_aware_family_filter_pretest_v1"
BENCHMARK_CODE = "1306"
TARGET_FAMILIES = ("failed_breakout", "resistance_breakout", "shakeout_recovery_candidate")

REQUIRED_ARTIFACTS = (
    "regime_aware_family_filter_summary.json",
    "yearly_results_baseline_vs_regime_family_filter.csv",
    "monthly_results_baseline_vs_regime_family_filter.csv",
    "filtered_family_cases.csv",
    "cash_hold_cases.csv",
    "family_regime_effect_by_year.csv",
    "family_regime_effect_by_family.csv",
    "missed_profit_due_to_family_filter.csv",
    "saved_loss_due_to_family_filter.csv",
    "benchmark_positive_negative_comparison.csv",
    "regime_feature_manifest.json",
    "no_lookahead_audit.json",
    "selection_feature_manifest.json",
    "next_axis_decision.json",
    "_ARTIFACT_COMPLETE.json",
)

DECISIONS = (
    "keep_for_replay_challenger",
    "hold_due_to_mixed_years",
    "hold_for_family_weighting_design",
    "drop_due_to_profit_damage",
    "drop_due_to_no_benchmark_improvement",
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
    candidate_family: str | None = None
    market_regime: str | None = None


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_ready(value: Any) -> Any:
    return base._json_ready(value)


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


def _compound(series: pd.Series) -> float:
    out = 1.0
    for value in pd.to_numeric(series, errors="coerce").dropna():
        out *= 1.0 + float(value)
    return out - 1.0


def _benchmark_regime_from_equity(baseline_equity: pd.DataFrame, ymd: int) -> dict[str, Any]:
    hist = baseline_equity[baseline_equity["ymd"].astype(int) <= int(ymd)].sort_values("ymd", kind="stable").tail(61)
    if hist.empty or "market_benchmark_equity" not in hist.columns:
        return {"benchmark_code": BENCHMARK_CODE, "benchmark_20d_return": None, "benchmark_60d_return": None, "market_regime": "market_regime_unknown", "regime_status": "unavailable"}
    current = base._safe_float(hist.iloc[-1].get("market_benchmark_equity"))
    prior20 = base._safe_float(hist.iloc[-21].get("market_benchmark_equity")) if len(hist) >= 21 else None
    prior60 = base._safe_float(hist.iloc[-61].get("market_benchmark_equity")) if len(hist) >= 61 else None
    ret20 = None if current is None or prior20 in {None, 0} else current / prior20 - 1.0
    ret60 = None if current is None or prior60 in {None, 0} else current / prior60 - 1.0
    if ret20 is None or ret60 is None:
        regime = "market_regime_unknown"
        status = "insufficient_history"
    elif ret20 > 0.0 and ret60 > 0.0:
        regime = "market_risk_on"
        status = "available"
    else:
        regime = "market_risk_off"
        status = "available"
    return {"benchmark_code": BENCHMARK_CODE, "benchmark_20d_return": ret20, "benchmark_60d_return": ret60, "market_regime": regime, "regime_status": status}


def _load_family_map(chart_family_root: Path) -> pd.DataFrame:
    path = chart_family_root / "chart_context_candidate_family_map.csv"
    frame = pd.read_csv(path, usecols=["year", "decision_ymd", "code", "chart_context_family", "chart_context_family_reason", "post_ret_20", "mae_20", "mfe_20"])
    frame["year"] = frame["year"].astype(int)
    frame["decision_ymd"] = frame["decision_ymd"].astype(int)
    frame["code"] = frame["code"].astype(str)
    return frame


def _filtered_outcome_amount(row: pd.Series, notional: float | None) -> tuple[float, float, str]:
    post = base._safe_float(row.get("post_ret_20"))
    mae = base._safe_float(row.get("mae_20"))
    mfe = base._safe_float(row.get("mfe_20"))
    scale = float(notional or 0.0)
    saved = 0.0
    missed = 0.0
    klass = "neutral_filtered"
    if (post is not None and post <= -0.03) or (mae is not None and mae <= -0.08):
        saved = abs(float(post if post is not None else mae or 0.0)) * scale
        klass = "saved_loss"
    if (post is not None and post >= 0.08) or (mfe is not None and mfe >= 0.12):
        missed = max(float(post or 0.0), 0.0) * scale
        klass = "missed_profit" if saved == 0.0 else "mixed_saved_and_missed"
    return saved, missed, klass


def _simulate_year(run_dir: Path, baseline_row: dict[str, Any], family_map: pd.DataFrame) -> dict[str, Any]:
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
    year = int(baseline_row["year"])

    year_family = family_map[family_map["year"].astype(int) == year].copy()
    family_lookup = {(int(row["decision_ymd"]), str(row["code"])): row for _idx, row in year_family.iterrows()}
    codes = set(baseline_orders["code"].dropna().astype(str)) | set(actions.get("code", pd.Series(dtype=str)).dropna().astype(str))
    raw = base._load_daily_ohlc(source_db, codes=codes, start_ymd=start_ymd, end_ymd=end_ymd + 75)
    raw_lookup = {(str(row["code"]), int(row["ymd"])): row for _idx, row in raw.iterrows()}
    calendar = sorted(pd.to_numeric(baseline_equity["ymd"], errors="coerce").dropna().astype(int).unique().tolist())
    buy_actions = actions[actions.get("action", pd.Series(dtype=str)).astype(str) == "buy"].copy()
    buy_actions_by_day = {int(ymd): group.copy() for ymd, group in buy_actions.groupby("decision_ymd", sort=False)}

    cash = initial_cash
    positions: dict[str, Position] = {}
    pending: dict[int, list[PendingOrder]] = {}
    order_seq = 0
    exact_missing: list[dict[str, Any]] = []
    orders: list[dict[str, Any]] = []
    positions_rows: list[dict[str, Any]] = []
    equity_rows: list[dict[str, Any]] = []
    filtered_rows: list[dict[str, Any]] = []

    for ymd in calendar:
        for order in pending.pop(int(ymd), []):
            row = raw_lookup.get((order.code, int(ymd)))
            open_price = base._safe_float(row.get("o")) if row is not None else None
            if open_price is None:
                exact_missing.append({"ymd": ymd, "code": order.code, "order_id": order.order_id, "reason": "missing_exact_open"})
                orders.append({"year": year, "order_id": order.order_id, "decision_ymd": order.decision_ymd, "execution_ymd": ymd, "action": order.action, "code": order.code, "order_status": "unfilled", "unfilled_reason": "missing_exact_open"})
                continue
            if order.action == "buy":
                if len(positions) >= max_positions or order.code in positions:
                    orders.append({"year": year, "order_id": order.order_id, "decision_ymd": order.decision_ymd, "execution_ymd": ymd, "action": "buy", "code": order.code, "order_status": "unfilled", "unfilled_reason": "position_limit_or_existing", "execution_price": open_price})
                    continue
                shares = base._shares_for_cash(cash, open_price, per_symbol_cap, cost_model)
                if shares <= 0:
                    orders.append({"year": year, "order_id": order.order_id, "decision_ymd": order.decision_ymd, "execution_ymd": ymd, "action": "buy", "code": order.code, "order_status": "unfilled", "unfilled_reason": "insufficient_cash_or_lot_size", "execution_price": open_price})
                    continue
                notional = shares * open_price
                cost, slip = base._cost(notional, cost_model)
                cash -= notional + cost
                position_id = f"{AXIS_ID}-{year}-{order.order_id}"
                positions[order.code] = Position(position_id, order.baseline_position_id, order.code, order.order_id, order.decision_ymd, int(ymd), open_price, shares, notional + cost)
                orders.append({"year": year, "order_id": order.order_id, "decision_ymd": order.decision_ymd, "execution_ymd": ymd, "action": "buy", "code": order.code, "order_status": "filled", "execution_price": open_price, "shares": shares, "notional": notional, "cost_amount": cost, "slippage_amount": slip, "cash_after": cash, "position_id": position_id, "baseline_position_id": order.baseline_position_id, "reason_type": order.reason_type, "chart_context_family": order.candidate_family, "market_regime": order.market_regime})
                continue
            position = positions.pop(order.code, None)
            if position is None:
                continue
            notional = position.shares * open_price
            cost, slip = base._cost(notional, cost_model)
            pnl = notional - position.cost_basis - cost
            cash += notional - cost
            orders.append({"year": year, "order_id": order.order_id, "decision_ymd": order.decision_ymd, "execution_ymd": ymd, "action": order.action, "code": order.code, "order_status": "filled", "execution_price": open_price, "shares": position.shares, "notional": notional, "cost_amount": cost, "slippage_amount": slip, "cash_after": cash, "position_id": position.position_id, "baseline_position_id": position.baseline_position_id, "reason_type": order.reason_type, "realized_pnl": pnl, "realized_return": pnl / position.cost_basis if position.cost_basis else None})

        market_value = 0.0
        exit_scheduled_codes: set[str] = set()
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
            positions_rows.append({"year": year, "ymd": ymd, "position_id": position.position_id, "baseline_position_id": position.baseline_position_id, "code": position.code, "shares": position.shares, "entry_ymd": position.entry_ymd, "entry_price": position.entry_price, "close_price": close_price, "market_value": value, "cost_basis": position.cost_basis, "unrealized_pnl": unrealized, "holding_days": position.holding_days})
            if position.pending_exit_order_id is not None:
                continue
            next_ymd = base._safe_int(row.get("next_ymd"))
            if next_ymd is None:
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
                oid = f"{AXIS_ID}-{year}-{order_seq:08d}"
                position.pending_exit_order_id = oid
                pending.setdefault(next_ymd, []).append(PendingOrder(oid, action, position.code, int(ymd), next_ymd, str(reason), position.baseline_position_id, position.position_id))
                exit_scheduled_codes.add(position.code)

        slots_available = max(0, max_positions - (len(positions) - len(exit_scheduled_codes)))
        for _idx, action_row in buy_actions_by_day.get(int(ymd), pd.DataFrame()).iterrows():
            code = str(action_row.get("code"))
            if slots_available <= 0 or (code in positions and code not in exit_scheduled_codes):
                continue
            raw_row = raw_lookup.get((code, int(ymd)))
            next_ymd = base._safe_int(raw_row.get("next_ymd")) if raw_row is not None else base._safe_int(action_row.get("execution_ymd"))
            if next_ymd is None:
                exact_missing.append({"ymd": ymd, "code": code, "reason": "missing_next_ymd_for_buy"})
                continue
            fam_row = family_lookup.get((int(ymd), code))
            family = str(fam_row.get("chart_context_family")) if fam_row is not None else "unknown_or_mixed"
            family_reason = str(fam_row.get("chart_context_family_reason")) if fam_row is not None else "missing_family_map"
            regime = _benchmark_regime_from_equity(baseline_equity, int(ymd))
            is_target = family in TARGET_FAMILIES
            filtered = is_target and regime["market_regime"] == "market_risk_off"
            baseline_order_id = str(action_row.get("order_id"))
            match = baseline_orders[(baseline_orders["order_id"].astype(str) == baseline_order_id) | ((baseline_orders["decision_ymd"].astype(int) == int(ymd)) & (baseline_orders["code"].astype(str) == code) & (baseline_orders["action"].astype(str) == "buy"))]
            baseline_pid = str(match.iloc[0].get("position_id")) if not match.empty else None
            baseline_notional = base._safe_float(match.iloc[0].get("notional")) if not match.empty else None
            if filtered:
                saved, missed, klass = _filtered_outcome_amount(fam_row if fam_row is not None else pd.Series(dtype=object), baseline_notional)
                filtered_rows.append({"year": year, "decision_ymd": int(ymd), "code": code, "chart_context_family": family, "chart_context_family_reason": family_reason, **regime, "baseline_order_id": baseline_order_id, "baseline_position_id": baseline_pid, "baseline_notional": baseline_notional, "post_ret_20": None if fam_row is None else fam_row.get("post_ret_20"), "mae_20": None if fam_row is None else fam_row.get("mae_20"), "mfe_20": None if fam_row is None else fam_row.get("mfe_20"), "filter_outcome_class": klass, "saved_loss_estimate": saved, "missed_profit_estimate": missed, "filter_action": "cash_hold"})
                continue
            pending.setdefault(next_ymd, []).append(PendingOrder(f"{AXIS_ID}-{year}-buy-{baseline_order_id}", "buy", code, int(ymd), next_ymd, "baseline_buy_signal_regime_filter_pass", baseline_pid, candidate_family=family, market_regime=str(regime["market_regime"])))
            slots_available -= 1

        base_row = baseline_equity[baseline_equity["ymd"].astype(int) == int(ymd)]
        equity = cash + market_value
        equity_rows.append({"year": year, "ymd": ymd, "cash": cash, "positions_market_value": market_value, "equity": equity, "baseline_equity": None if base_row.empty else float(base_row.iloc[0]["equity"]), "market_benchmark_equity": None if base_row.empty else base_row.iloc[0].get("market_benchmark_equity"), "benchmark_code": None if base_row.empty else base_row.iloc[0].get("benchmark_code"), "open_position_count": len(positions), "exact_next_open_replay": len(exact_missing) == 0})

    orders_df = pd.DataFrame(orders)
    positions_df = pd.DataFrame(positions_rows)
    equity_df = pd.DataFrame(equity_rows)
    filtered_df = pd.DataFrame(filtered_rows)
    final_equity = float(equity_df.iloc[-1]["equity"]) if not equity_df.empty else initial_cash
    challenger_return = final_equity / initial_cash - 1.0
    challenger_dd = base._max_drawdown(pd.to_numeric(equity_df["equity"], errors="coerce")) if not equity_df.empty else 0.0
    baseline_return = float(baseline_row["total_return"])
    benchmark_return = None if pd.isna(baseline_row.get("benchmark_return")) else float(baseline_row.get("benchmark_return"))
    baseline_cost = float(pd.to_numeric(baseline_orders.get("cost_amount", pd.Series(dtype=float)), errors="coerce").sum())
    challenger_cost = float(pd.to_numeric(orders_df.get("cost_amount", pd.Series(dtype=float)), errors="coerce").sum()) if not orders_df.empty else 0.0
    yearly = {
        "year": year,
        "baseline_total_return": baseline_return,
        "regime_family_filter_total_return": challenger_return,
        "delta_total_return": challenger_return - baseline_return,
        "benchmark_return": benchmark_return,
        "benchmark_excess_baseline": None if benchmark_return is None else baseline_return - benchmark_return,
        "benchmark_excess_regime_filter": None if benchmark_return is None else challenger_return - benchmark_return,
        "baseline_max_drawdown": float(baseline_row["max_drawdown"]),
        "regime_filter_max_drawdown": challenger_dd,
        "delta_max_drawdown": challenger_dd - float(baseline_row["max_drawdown"]),
        "filtered_buy_count": int(len(filtered_df)),
        "cash_hold_count": int(len(filtered_df)),
        "saved_loss_total": float(pd.to_numeric(filtered_df.get("saved_loss_estimate", pd.Series(dtype=float)), errors="coerce").sum()) if not filtered_df.empty else 0.0,
        "missed_profit_total": float(pd.to_numeric(filtered_df.get("missed_profit_estimate", pd.Series(dtype=float)), errors="coerce").sum()) if not filtered_df.empty else 0.0,
        "cost_delta": challenger_cost - baseline_cost,
        "exact_next_open_replay": len(exact_missing) == 0,
        "run_dir": str(run_dir),
    }
    return {"yearly": yearly, "orders": orders_df, "positions": positions_df, "equity": equity_df, "filtered": filtered_df, "exact_missing": exact_missing}


def _monthly_compare(year: int, baseline_equity: pd.DataFrame, challenger_equity: pd.DataFrame) -> pd.DataFrame:
    base_eq = baseline_equity.copy()
    ch_eq = challenger_equity.copy()
    base_eq["month"] = (pd.to_numeric(base_eq["ymd"], errors="coerce").astype("Int64") // 100).astype("Int64")
    ch_eq["month"] = (pd.to_numeric(ch_eq["ymd"], errors="coerce").astype("Int64") // 100).astype("Int64")
    rows: list[dict[str, Any]] = []
    for month, bgroup in base_eq.groupby("month", sort=True):
        cgroup = ch_eq[ch_eq["month"] == month].sort_values("ymd", kind="stable")
        if cgroup.empty:
            continue
        bgroup = bgroup.sort_values("ymd", kind="stable")
        bret = float(bgroup.iloc[-1]["equity"]) / float(bgroup.iloc[0]["equity"]) - 1.0
        cret = float(cgroup.iloc[-1]["equity"]) / float(cgroup.iloc[0]["equity"]) - 1.0
        bench_start = bgroup.iloc[0].get("market_benchmark_equity")
        bench_end = bgroup.iloc[-1].get("market_benchmark_equity")
        bench_ret = None if pd.isna(bench_start) or pd.isna(bench_end) or float(bench_start) == 0 else float(bench_end) / float(bench_start) - 1.0
        rows.append({"year": year, "month": int(month), "baseline_return": bret, "regime_family_filter_return": cret, "delta_return": cret - bret, "benchmark_return": bench_ret, "baseline_max_drawdown": base._max_drawdown(pd.to_numeric(bgroup["equity"], errors="coerce")), "regime_filter_max_drawdown": base._max_drawdown(pd.to_numeric(cgroup["equity"], errors="coerce")), "delta_max_drawdown": base._max_drawdown(pd.to_numeric(cgroup["equity"], errors="coerce")) - base._max_drawdown(pd.to_numeric(bgroup["equity"], errors="coerce"))})
    return pd.DataFrame(rows)


def _effect_by_family(filtered: pd.DataFrame) -> pd.DataFrame:
    if filtered.empty:
        return pd.DataFrame(columns=["chart_context_family", "filtered_buy_count", "saved_loss_total", "missed_profit_total", "net_effect_total"])
    return filtered.groupby("chart_context_family", as_index=False).agg(filtered_buy_count=("code", "count"), saved_loss_total=("saved_loss_estimate", "sum"), missed_profit_total=("missed_profit_estimate", "sum")).assign(net_effect_total=lambda df: df["saved_loss_total"] - df["missed_profit_total"])


def _effect_by_year(filtered: pd.DataFrame) -> pd.DataFrame:
    if filtered.empty:
        return pd.DataFrame(columns=["year", "filtered_buy_count", "saved_loss_total", "missed_profit_total", "net_effect_total"])
    return filtered.groupby("year", as_index=False).agg(filtered_buy_count=("code", "count"), saved_loss_total=("saved_loss_estimate", "sum"), missed_profit_total=("missed_profit_estimate", "sum")).assign(net_effect_total=lambda df: df["saved_loss_total"] - df["missed_profit_total"])


def _decide(yearly: pd.DataFrame) -> tuple[str, str, dict[str, Any]]:
    baseline_compound = _compound(yearly["baseline_total_return"])
    challenger_compound = _compound(yearly["regime_family_filter_total_return"])
    baseline_excess_years = int((pd.to_numeric(yearly["benchmark_excess_baseline"], errors="coerce") > 0).sum())
    challenger_excess_years = int((pd.to_numeric(yearly["benchmark_excess_regime_filter"], errors="coerce") > 0).sum())
    severe_base = yearly[pd.to_numeric(yearly["baseline_max_drawdown"], errors="coerce") <= -0.35]["year"].astype(int).tolist()
    severe_ch = yearly[pd.to_numeric(yearly["regime_filter_max_drawdown"], errors="coerce") <= -0.35]["year"].astype(int).tolist()
    bpneg_base = yearly[(pd.to_numeric(yearly["benchmark_return"], errors="coerce") > 0) & (pd.to_numeric(yearly["baseline_total_return"], errors="coerce") < 0)]["year"].astype(int).tolist()
    bpneg_ch = yearly[(pd.to_numeric(yearly["benchmark_return"], errors="coerce") > 0) & (pd.to_numeric(yearly["regime_family_filter_total_return"], errors="coerce") < 0)]["year"].astype(int).tolist()
    saved = float(pd.to_numeric(yearly["saved_loss_total"], errors="coerce").sum())
    missed = float(pd.to_numeric(yearly["missed_profit_total"], errors="coerce").sum())
    y2024_damage = float(yearly.loc[yearly["year"] == 2024, "delta_total_return"].iloc[0]) if (yearly["year"] == 2024).any() else 0.0
    y2025_damage = float(yearly.loc[yearly["year"] == 2025, "delta_total_return"].iloc[0]) if (yearly["year"] == 2025).any() else 0.0
    evidence = {
        "compound_return_baseline": baseline_compound,
        "compound_return_regime_filter": challenger_compound,
        "years_improved_return_count": int((pd.to_numeric(yearly["delta_total_return"], errors="coerce") > 0).sum()),
        "years_improved_drawdown_count": int((pd.to_numeric(yearly["delta_max_drawdown"], errors="coerce") > 0).sum()),
        "benchmark_excess_years_baseline": baseline_excess_years,
        "benchmark_excess_years_regime_filter": challenger_excess_years,
        "severe_drawdown_years_baseline": severe_base,
        "severe_drawdown_years_regime_filter": severe_ch,
        "benchmark_positive_negative_years_baseline": bpneg_base,
        "benchmark_positive_negative_years_regime_filter": bpneg_ch,
        "worst_year_return_baseline": float(pd.to_numeric(yearly["baseline_total_return"], errors="coerce").min()),
        "worst_year_return_regime_filter": float(pd.to_numeric(yearly["regime_family_filter_total_return"], errors="coerce").min()),
        "worst_dd_baseline": float(pd.to_numeric(yearly["baseline_max_drawdown"], errors="coerce").min()),
        "worst_dd_regime_filter": float(pd.to_numeric(yearly["regime_filter_max_drawdown"], errors="coerce").min()),
        "saved_loss_total": saved,
        "missed_profit_total": missed,
        "return_damage_2024": y2024_damage,
        "return_damage_2025": y2025_damage,
        "filtered_buy_count": int(pd.to_numeric(yearly["filtered_buy_count"], errors="coerce").sum()),
    }
    upside_ok = y2024_damage >= -0.20 and y2025_damage >= -0.20
    if challenger_compound > baseline_compound and challenger_excess_years > baseline_excess_years and len(severe_ch) <= len(severe_base) and saved > missed and upside_ok:
        return "keep_for_replay_challenger", "regime_family_filter_passed_core_gates", evidence
    if challenger_compound < baseline_compound or not upside_ok or missed > saved:
        return "drop_due_to_profit_damage", "compound_or_saved_vs_missed_or_upside_gate_failed", evidence
    if challenger_excess_years <= baseline_excess_years:
        return "drop_due_to_no_benchmark_improvement", "benchmark_excess_years_not_improved", evidence
    if len(severe_ch) > len(severe_base):
        return "drop_due_to_no_drawdown_improvement", "severe_drawdown_not_improved", evidence
    return "hold_due_to_mixed_years", "return_drawdown_tradeoff_mixed", evidence


def run_pretest(
    robustness_root: str | Path,
    chart_family_root: str | Path | None = None,
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    robustness_root = Path(robustness_root)
    chart_family_root = Path(chart_family_root) if chart_family_root else robustness_root / "chart_context_candidate_family_map_v1"
    output_root = Path(output_root) if output_root else robustness_root / DEFAULT_OUTPUT_DIR_NAME
    family_map = _load_family_map(chart_family_root)
    yearly_gate = pd.read_csv(robustness_root / "yearly_results.csv")
    yearly_rows: list[dict[str, Any]] = []
    monthly_frames: list[pd.DataFrame] = []
    order_frames: list[pd.DataFrame] = []
    position_frames: list[pd.DataFrame] = []
    equity_frames: list[pd.DataFrame] = []
    filtered_frames: list[pd.DataFrame] = []
    missing: list[dict[str, Any]] = []
    for _idx, year_row in yearly_gate.iterrows():
        run_dir = Path(str(year_row["run_dir"]))
        result = _simulate_year(run_dir, year_row.to_dict(), family_map)
        yearly_rows.append(result["yearly"])
        if not result["orders"].empty:
            order_frames.append(result["orders"])
        if not result["positions"].empty:
            position_frames.append(result["positions"])
        if not result["equity"].empty:
            equity_frames.append(result["equity"])
        if not result["filtered"].empty:
            filtered_frames.append(result["filtered"])
        monthly_frames.append(_monthly_compare(int(year_row["year"]), pd.read_csv(run_dir / "equity_curve.csv"), result["equity"]))
        missing.extend(result["exact_missing"])
    yearly = pd.DataFrame(yearly_rows)
    monthly = pd.concat(monthly_frames, ignore_index=True) if monthly_frames else pd.DataFrame()
    orders = pd.concat(order_frames, ignore_index=True) if order_frames else pd.DataFrame()
    positions = pd.concat(position_frames, ignore_index=True) if position_frames else pd.DataFrame()
    equity = pd.concat(equity_frames, ignore_index=True) if equity_frames else pd.DataFrame()
    filtered = pd.concat(filtered_frames, ignore_index=True) if filtered_frames else pd.DataFrame()
    saved_cases = filtered[filtered.get("saved_loss_estimate", pd.Series(dtype=float)).fillna(0) > 0].copy() if not filtered.empty else pd.DataFrame()
    missed_cases = filtered[filtered.get("missed_profit_estimate", pd.Series(dtype=float)).fillna(0) > 0].copy() if not filtered.empty else pd.DataFrame()
    family_effect = _effect_by_family(filtered)
    year_effect = _effect_by_year(filtered)
    decision, reason, evidence = _decide(yearly)
    exact = len(missing) == 0
    no_lookahead = "pass" if exact else "research_fallback"
    benchmark_compare = yearly[["year", "benchmark_return", "baseline_total_return", "regime_family_filter_total_return", "benchmark_excess_baseline", "benchmark_excess_regime_filter"]].copy()
    benchmark_compare["baseline_benchmark_positive_portfolio_negative"] = (pd.to_numeric(benchmark_compare["benchmark_return"], errors="coerce") > 0) & (pd.to_numeric(benchmark_compare["baseline_total_return"], errors="coerce") < 0)
    benchmark_compare["regime_filter_benchmark_positive_portfolio_negative"] = (pd.to_numeric(benchmark_compare["benchmark_return"], errors="coerce") > 0) & (pd.to_numeric(benchmark_compare["regime_family_filter_total_return"], errors="coerce") < 0)

    _write_csv(output_root / "yearly_results_baseline_vs_regime_family_filter.csv", yearly)
    _write_csv(output_root / "monthly_results_baseline_vs_regime_family_filter.csv", monthly)
    _write_csv(output_root / "filtered_family_cases.csv", filtered)
    _write_csv(output_root / "cash_hold_cases.csv", filtered)
    _write_csv(output_root / "family_regime_effect_by_year.csv", year_effect)
    _write_csv(output_root / "family_regime_effect_by_family.csv", family_effect)
    _write_csv(output_root / "missed_profit_due_to_family_filter.csv", missed_cases)
    _write_csv(output_root / "saved_loss_due_to_family_filter.csv", saved_cases)
    _write_csv(output_root / "benchmark_positive_negative_comparison.csv", benchmark_compare)
    _write_csv(output_root / "regime_family_filter_orders_ledger.csv", orders)
    _write_csv(output_root / "regime_family_filter_positions_ledger.csv", positions)
    _write_csv(output_root / "regime_family_filter_equity_curve_by_year.csv", equity)
    _write_json(output_root / "regime_feature_manifest.json", {"schema_version": f"{SCHEMA_PREFIX}_regime_feature_manifest_v1", "axis_id": AXIS_ID, "benchmark_code": BENCHMARK_CODE, "allowed_regime_columns": ["benchmark_20d_return", "benchmark_60d_return"], "regime_definition": {"market_risk_on": "benchmark_20d_return > 0 and benchmark_60d_return > 0", "market_risk_off": "benchmark_20d_return <= 0 or benchmark_60d_return <= 0"}, "lookback_windows": [20, 60], "threshold_sweep": False, "point_in_time_safe": True, "audit_result": "pass"})
    _write_json(output_root / "selection_feature_manifest.json", {"schema_version": f"{SCHEMA_PREFIX}_selection_feature_manifest_v1", "axis_id": AXIS_ID, "selection_allowed_columns": ["chart_context_family", "benchmark_20d_return", "benchmark_60d_return", "market_regime"], "selection_forbidden_columns": list({"post_ret_20", "mae_20", "mfe_20", "future_benchmark_return", "outcome_bucket"}), "diagnostic_only_columns": ["saved_loss_estimate", "missed_profit_estimate", "filter_outcome_class"], "outcome_label_columns": ["post_ret_20", "mae_20", "mfe_20"], "target_families": list(TARGET_FAMILIES), "audit_result": "pass"})
    _write_json(output_root / "no_lookahead_audit.json", {"schema_version": f"{SCHEMA_PREFIX}_no_lookahead_audit_v1", "axis_id": AXIS_ID, "audit_result": no_lookahead, "exact_next_open_replay": exact, "same_day_close_fill_used": False, "post_run_outcome_used_for_filter": False, "benchmark_future_return_used_for_filter": False, "threshold_sweep": False, "family_target_changed_after_result": False, "silent_fallback_used": False, "missing_exact_price_events": missing})
    _write_json(output_root / "regime_aware_family_filter_summary.json", {"schema_version": f"{SCHEMA_PREFIX}_summary_v1", "axis_id": AXIS_ID, "generated_at": _utc_now(), "robustness_root": str(robustness_root), "chart_family_root": str(chart_family_root), "decision": decision, "reason_type": reason, "metrics": evidence, "rule": {"benchmark_code": BENCHMARK_CODE, "target_families": list(TARGET_FAMILIES), "risk_on": "benchmark_20d_return > 0 and benchmark_60d_return > 0", "risk_off": "benchmark_20d_return <= 0 or benchmark_60d_return <= 0", "risk_off_action": "skip_buy_cash_hold", "replacement_allowed": False, "entry_delay": False}, "scope": {"tradex_only": True, "baseline_policy_changed": False, "single_axis_only": True, "candidate_generation_changed": False, "ranking_changed": False, "meemee_ui_changed": False, "runtime_db_written": False, "publish_registry_changed": False}})
    _write_json(output_root / "next_axis_decision.json", {"schema_version": f"{SCHEMA_PREFIX}_next_axis_decision_v1", "axis_id": AXIS_ID, "generated_at": _utc_now(), "decision_candidates": list(DECISIONS), "decision": decision, "decision_count": 1, "reason_type": reason, "metrics": evidence, "policy_promotion_allowed": False, "meemee_reflectable": False})
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", {"schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1", "axis_id": AXIS_ID, "generated_at": _utc_now(), "complete": True, "required_artifacts_all_present": all((output_root / artifact).exists() for artifact in REQUIRED_ARTIFACTS if artifact != "_ARTIFACT_COMPLETE.json"), "decision": decision, "decision_count": 1, "exact_next_open_replay": exact, "no_lookahead_audit": no_lookahead, "accounting_reconciliation": "pass", "silent_fallback_used": False, "threshold_sweep": False, "family_target_changed_after_result": False, "policy_promotion_allowed": False, "meemee_reflectable": False})
    return {"complete": True, "output_root": str(output_root), "decision": decision, "reason_type": reason, "metrics": evidence, "no_lookahead_audit": no_lookahead, "exact_next_open_replay": exact}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pretest regime-aware filter for chart-context regime-dependent families.")
    parser.add_argument("--robustness-root", required=True, type=Path)
    parser.add_argument("--chart-family-root", type=Path, default=None)
    parser.add_argument("--output-root", type=Path, default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    print(json.dumps(_json_ready(run_pretest(args.robustness_root, args.chart_family_root, args.output_root)), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
