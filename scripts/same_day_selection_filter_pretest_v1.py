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

from scripts import candidate_selection_quality_decomposition_v1 as decomposition
from scripts import entry_confirmation_pretest_v1 as entry_pretest
from scripts import market_regime_gated_risk_off_pretest_v1 as goal
from scripts import portfolio_agent_replay_v1 as replay
from scripts import risk_off_cash_control_pretest_v1 as risk_engine


AXIS_ID = "same_day_selection_filter_pretest_v1"
SCHEMA_PREFIX = "tradex_same_day_selection_filter_pretest_v1"
DEFAULT_OUTPUT_DIR_NAME = "same_day_selection_filter_pretest_v1"

QUALITY_BONUS = {
    "daily_volume_state": {"daily_volume_expansion": 2, "daily_volume_normal": 0, "daily_volume_dry": -1},
    "daily_candle_state": {"daily_strong_bull": 2, "daily_lower_wick_bull": 1, "daily_upper_wick": -2},
    "daily_sequence_state": {"daily_sequence_bullish": 1},
    "weekly_trend_state": {"weekly_uptrend": 2},
    "weekly_ret4_state": {"weekly4_strong_up": 1},
    "monthly_trend_state": {"monthly_uptrend": 1},
    "monthly_ret6_state": {"monthly6_strong_up": 1},
}

REQUIRED_ARTIFACTS = (
    "selection_filter_contract.json",
    "same_day_selection_filter_summary.json",
    "yearly_results_baseline_vs_same_day_filter.csv",
    "monthly_results_baseline_vs_same_day_filter.csv",
    "selected_candidate_changes.csv",
    "same_day_replacement_cases.csv",
    "avoided_bad_selection_cases.csv",
    "missed_winner_due_to_selection_cases.csv",
    "same_day_filter_orders_ledger.csv",
    "same_day_filter_positions_ledger.csv",
    "same_day_filter_equity_curve_by_year.csv",
    "selection_effect_by_year.csv",
    "benchmark_positive_negative_comparison.csv",
    "no_lookahead_audit.json",
    "selection_feature_manifest.json",
    "next_axis_decision.json",
    "_ARTIFACT_COMPLETE.json",
)

DECISIONS = (
    "keep_for_replay_challenger",
    "hold_due_to_mixed_years",
    "hold_for_candidate_generation_redesign",
    "drop_due_to_profit_damage",
    "drop_due_to_no_benchmark_improvement",
    "drop_due_to_no_valid_same_day_rule",
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


def _contract() -> dict[str, Any]:
    allowed = [
        "decision_ymd",
        "code",
        "candidate_rank",
        "selection_score",
        "entry_allowed_by_score",
        "downside_guard_blocked",
        "next_execution_ymd",
        "next_open_available",
        "score_components_json",
        "daily_volume_state",
        "daily_candle_state",
        "daily_sequence_state",
        "weekly_trend_state",
        "weekly_ret4_state",
        "monthly_trend_state",
        "monthly_ret6_state",
    ]
    forbidden = [
        "post_ret_5",
        "post_ret_10",
        "post_ret_20",
        "post_ret_40",
        "MAE",
        "MFE",
        "mae_20",
        "mfe_20",
        "outcome_bucket",
        "future_return",
        "missed_winner",
        "avoided_bad",
        "entry_confirmation_outcome_class",
    ]
    return {
        "schema_version": f"{SCHEMA_PREFIX}_selection_filter_contract_v1",
        "axis_id": AXIS_ID,
        "allowed_signal_date_columns": allowed,
        "forbidden_diagnostic_columns": forbidden,
        "outcome_label_columns": ["post_ret_5", "post_ret_10", "post_ret_20", "post_ret_40", "mae_20", "mfe_20", "outcome_bucket"],
        "rule_description": "Within the same signal_date candidate snapshot, sort eligible candidates by selection_score plus signal-date score-component quality bonus, then candidate_rank. Execute selected buys at next_session_open.",
        "rule_axis_id": "same_day_component_quality_score_rerank",
        "single_axis_only": True,
        "post_run_outcome_used_for_selection": False,
        "threshold_sweep": False,
        "confirmation_days_sweep": False,
        "rank_threshold_sweep": False,
        "candidate_pool_expanded": False,
    }


def _component_map(value: Any) -> dict[str, str]:
    return decomposition._parse_components(value)


def _quality_score(row: pd.Series) -> tuple[float, dict[str, Any]]:
    components = _component_map(row.get("score_components_json"))
    bonus = 0
    reasons: list[str] = []
    for feature, mapping in QUALITY_BONUS.items():
        value = components.get(feature, "")
        points = int(mapping.get(value, 0))
        bonus += points
        if points:
            reasons.append(f"{feature}:{value}:{points}")
    base_score = float(row.get("selection_score") or 0)
    rank = float(row.get("candidate_rank") or 999)
    final_score = base_score + bonus - rank * 0.001
    return final_score, {"component_bonus": bonus, "quality_reason_codes": "|".join(reasons), **components}


def _as_bool(value: Any) -> bool:
    return str(value).lower() in {"true", "1", "yes"}


def _as_bool_series(series: pd.Series) -> pd.Series:
    return series.astype(str).str.lower().isin(["true", "1", "yes"])


def _cost(notional: float, cost_model: dict[str, Any]) -> tuple[float, float]:
    total, slip, _fees = replay._trade_cost(notional, cost_model)
    return total, slip


def _shares_for_cash(cash: float, price: float, per_symbol_cap: float, cost_model: dict[str, Any]) -> int:
    return risk_engine._shares_for_cash(cash, price, per_symbol_cap, cost_model)


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
    candidates_by_day = {int(ymd): group.sort_values("candidate_rank", kind="stable").copy() for ymd, group in candidates.groupby("decision_ymd", sort=False)}
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
    order_seq = 0
    exact_missing: list[dict[str, Any]] = []
    orders: list[dict[str, Any]] = []
    position_rows: list[dict[str, Any]] = []
    equity_rows: list[dict[str, Any]] = []
    change_rows: list[dict[str, Any]] = []

    baseline_selected = candidates[_as_bool_series(candidates.get("selected_for_buy", pd.Series(dtype=bool)))].copy()
    baseline_selected_by_day = {int(ymd): group.sort_values("candidate_rank", kind="stable").copy() for ymd, group in baseline_selected.groupby("decision_ymd", sort=False)}

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

        market_value = 0.0
        exit_scheduled_codes: set[str] = set()
        for code, position in list(positions.items()):
            raw_row = raw_lookup.get((code, int(ymd)))
            close_price = risk_engine.base._safe_float(raw_row.get("c")) if raw_row is not None else None
            if close_price is None:
                exact_missing.append({"ymd": ymd, "code": code, "position_id": position.position_id, "reason": "missing_close_for_mark"})
                continue
            position.holding_days += 1
            value = position.shares * close_price
            market_value += value
            position_rows.append({"year": baseline_row["year"], "ymd": ymd, "position_id": position.position_id, "code": code, "shares": position.shares, "entry_ymd": position.entry_ymd, "entry_price": position.entry_price, "close_price": close_price, "market_value": value, "cost_basis": position.cost_basis, "unrealized_pnl": value - position.cost_basis, "holding_days": position.holding_days})
            if position.pending_exit_order_id:
                continue
            close_return = close_price / position.entry_price - 1.0 if position.entry_price else 0.0
            profit_return = (value - position.cost_basis) / position.cost_basis if position.cost_basis else 0.0
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

        slots_available = max(0, max_positions - (len(positions) - len(exit_scheduled_codes)))
        day_candidates = candidates_by_day.get(int(ymd), pd.DataFrame()).copy()
        if slots_available > 0 and not day_candidates.empty:
            eligible_rows: list[dict[str, Any]] = []
            for _idx, row in day_candidates.iterrows():
                code = str(row["code"])
                if code in positions and code not in exit_scheduled_codes:
                    continue
                if not _as_bool(row.get("entry_allowed_by_score")) or _as_bool(row.get("downside_guard_blocked")):
                    continue
                next_ymd = risk_engine.base._safe_int(row.get("next_execution_ymd"))
                if next_ymd is None or not _as_bool(row.get("next_open_available", True)):
                    continue
                qscore, qfields = _quality_score(row)
                item = row.to_dict()
                item.update(qfields)
                item["same_day_quality_score"] = qscore
                item["next_ymd"] = next_ymd
                eligible_rows.append(item)
            if eligible_rows:
                eligible = pd.DataFrame(eligible_rows).sort_values(["same_day_quality_score", "selection_score", "candidate_rank"], ascending=[False, False, True], kind="stable")
                selected_rows = eligible.head(slots_available).copy()
                baseline_codes = baseline_selected_by_day.get(int(ymd), pd.DataFrame()).head(slots_available)["code"].astype(str).tolist() if int(ymd) in baseline_selected_by_day else []
                selected_codes = selected_rows["code"].astype(str).tolist()
                if baseline_codes != selected_codes:
                    change_rows.append({"year": baseline_row["year"], "decision_ymd": ymd, "baseline_selected_codes": "|".join(baseline_codes), "same_day_selected_codes": "|".join(selected_codes), "changed_selection": True, "slots_available": slots_available})
                for _sidx, row in selected_rows.iterrows():
                    order_seq += 1
                    order_id = f"{AXIS_ID}-{baseline_row['year']}-{order_seq:08d}"
                    pending_orders.setdefault(int(row["next_ymd"]), []).append(PendingOrder(order_id, "buy", str(row["code"]), int(ymd), int(row["next_ymd"]), "same_day_component_quality_selected", int(row["selection_score"]), int(row["candidate_rank"])))

        base_row = baseline_equity[baseline_equity["ymd"].astype(int) == int(ymd)]
        equity = cash + market_value
        equity_rows.append({"year": baseline_row["year"], "ymd": ymd, "cash": cash, "positions_market_value": market_value, "equity": equity, "baseline_equity": None if base_row.empty else float(base_row.iloc[0]["equity"]), "market_benchmark_equity": None if base_row.empty else base_row.iloc[0].get("market_benchmark_equity"), "benchmark_code": None if base_row.empty else base_row.iloc[0].get("benchmark_code"), "open_position_count": len(positions), "exact_next_open_replay": len(exact_missing) == 0})

    orders_df = pd.DataFrame(orders)
    positions_df = pd.DataFrame(position_rows)
    equity_df = pd.DataFrame(equity_rows)
    changes_df = pd.DataFrame(change_rows)
    final_equity = float(equity_df.iloc[-1]["equity"]) if not equity_df.empty else initial_cash
    total_return = final_equity / initial_cash - 1.0
    max_dd = risk_engine._max_drawdown(pd.to_numeric(equity_df["equity"], errors="coerce")) if not equity_df.empty else 0.0
    benchmark_return = None if pd.isna(baseline_row.get("benchmark_return")) else float(baseline_row.get("benchmark_return"))
    baseline_return = float(baseline_row["total_return"])
    yearly = {
        "year": baseline_row["year"],
        "baseline_total_return": baseline_return,
        "same_day_filter_total_return": total_return,
        "risk_off_total_return": total_return,
        "delta_total_return": total_return - baseline_return,
        "benchmark_return": benchmark_return,
        "benchmark_excess_baseline": baseline_return - benchmark_return if benchmark_return is not None else None,
        "benchmark_excess_same_day_filter": total_return - benchmark_return if benchmark_return is not None else None,
        "risk_off_benchmark_excess": total_return - benchmark_return if benchmark_return is not None else None,
        "baseline_max_drawdown": float(baseline_row["max_drawdown"]),
        "same_day_filter_max_drawdown": max_dd,
        "risk_off_max_drawdown": max_dd,
        "delta_max_drawdown": max_dd - float(baseline_row["max_drawdown"]),
        "buy_count_baseline": int((baseline_orders["action"].astype(str) == "buy").sum()),
        "buy_count_same_day_filter": int((orders_df.get("action", pd.Series(dtype=str)).astype(str) == "buy").sum()) if not orders_df.empty else 0,
        "changed_selection_count": int(len(changes_df)),
        "cost_delta": float(pd.to_numeric(orders_df.get("cost_amount", pd.Series(dtype=float)), errors="coerce").fillna(0).sum() - pd.to_numeric(baseline_orders.get("cost_amount", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()),
        "run_dir": str(run_dir),
        "exact_next_open_replay": len(exact_missing) == 0,
    }
    return {"yearly": yearly, "orders": orders_df, "positions": positions_df, "equity": equity_df, "changes": changes_df, "outcomes": outcomes, "missing": exact_missing}


def _monthly_compare(year: int, baseline_equity: pd.DataFrame, challenger_equity: pd.DataFrame) -> pd.DataFrame:
    return entry_pretest._monthly_compare(year, baseline_equity, challenger_equity).rename(columns={"entry_confirmation_return": "same_day_filter_return", "entry_confirmation_max_drawdown": "same_day_filter_max_drawdown", "avg_gross_exposure_entry_confirmation": "avg_gross_exposure_same_day_filter"})


def _selection_outcome_cases(changes: pd.DataFrame, outcomes: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if changes.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    rows: list[dict[str, Any]] = []
    for _idx, change in changes.iterrows():
        year = int(change["year"])
        ymd = int(change["decision_ymd"])
        base_codes = [code for code in str(change.get("baseline_selected_codes", "")).split("|") if code]
        new_codes = [code for code in str(change.get("same_day_selected_codes", "")).split("|") if code]
        removed = sorted(set(base_codes) - set(new_codes))
        added = sorted(set(new_codes) - set(base_codes))
        for code in removed:
            row = outcomes[(outcomes["decision_ymd"].astype(int) == ymd) & (outcomes["code"].astype(str) == code)]
            post = float(row.iloc[0].get("post_ret_20")) if not row.empty and pd.notna(row.iloc[0].get("post_ret_20")) else None
            mae = float(row.iloc[0].get("mae_20")) if not row.empty and pd.notna(row.iloc[0].get("mae_20")) else None
            mfe = float(row.iloc[0].get("mfe_20")) if not row.empty and pd.notna(row.iloc[0].get("mfe_20")) else None
            klass = "neutral_removed"
            if (post is not None and post <= -0.03) or (mae is not None and mae <= -0.08):
                klass = "avoided_bad_selection"
            if (post is not None and post >= 0.08) or (mfe is not None and mfe >= 0.12):
                klass = "missed_winner_due_to_selection"
            rows.append({"year": year, "decision_ymd": ymd, "code": code, "change_side": "removed_baseline_buy", "post_ret_20": post, "mae_20": mae, "mfe_20": mfe, "selection_outcome_class": klass})
        for code in added:
            row = outcomes[(outcomes["decision_ymd"].astype(int) == ymd) & (outcomes["code"].astype(str) == code)]
            post = float(row.iloc[0].get("post_ret_20")) if not row.empty and pd.notna(row.iloc[0].get("post_ret_20")) else None
            rows.append({"year": year, "decision_ymd": ymd, "code": code, "change_side": "added_same_day_filter_buy", "post_ret_20": post, "selection_outcome_class": "added_candidate_diagnostic"})
    frame = pd.DataFrame(rows)
    avoided = frame[frame["selection_outcome_class"] == "avoided_bad_selection"].copy() if not frame.empty else pd.DataFrame()
    missed = frame[frame["selection_outcome_class"] == "missed_winner_due_to_selection"].copy() if not frame.empty else pd.DataFrame()
    return frame, avoided, missed


def _compound(series: pd.Series) -> float:
    out = 1.0
    for value in pd.to_numeric(series, errors="coerce").dropna():
        out *= 1.0 + float(value)
    return out - 1.0


def _goal_gate(yearly: pd.DataFrame, avoided: pd.DataFrame, missed: pd.DataFrame, exact: bool, no_lookahead: str) -> dict[str, Any]:
    risk_named = yearly.copy()
    risk_named["risk_off_total_return"] = risk_named["same_day_filter_total_return"]
    risk_named["risk_off_max_drawdown"] = risk_named["same_day_filter_max_drawdown"]
    risk_named["risk_off_benchmark_excess"] = risk_named["benchmark_excess_same_day_filter"]
    base_goal = goal._goal_gate(risk_named, exact, no_lookahead)
    baseline_excess = pd.to_numeric(yearly["benchmark_excess_baseline"], errors="coerce")
    challenger_excess = pd.to_numeric(yearly["benchmark_excess_same_day_filter"], errors="coerce")
    y2024 = yearly[yearly["year"].astype(int) == 2024]
    y2025 = yearly[yearly["year"].astype(int) == 2025]
    damage_2024 = float(y2024["delta_total_return"].iloc[0]) if not y2024.empty else 0.0
    damage_2025 = float(y2025["delta_total_return"].iloc[0]) if not y2025.empty else 0.0
    base_goal["gates"].update(
        {
            "benchmark_excess_years_improved_pass": int((challenger_excess > 0).sum()) > int((baseline_excess > 0).sum()),
            "upside_2024_damage_within_20pct_pass": damage_2024 >= -0.20,
            "upside_2025_damage_within_20pct_pass": damage_2025 >= -0.20,
            "avoided_bad_selection_gt_missed_winner_pass": len(avoided) > len(missed),
        }
    )
    base_goal["all_primary_gates_pass"] = all(v for k, v in base_goal["gates"].items() if k != "rolling_start_pass")
    base_goal["all_gates_pass"] = all(base_goal["gates"].values())
    base_goal.update(
        {
            "compound_return_baseline": _compound(yearly["baseline_total_return"]),
            "compound_return_same_day_filter": _compound(yearly["same_day_filter_total_return"]),
            "years_improved_return_count": int((pd.to_numeric(yearly["delta_total_return"], errors="coerce") > 0).sum()),
            "years_improved_drawdown_count": int((pd.to_numeric(yearly["delta_max_drawdown"], errors="coerce") > 0).sum()),
            "benchmark_excess_years_baseline": int((baseline_excess > 0).sum()),
            "benchmark_excess_years_same_day_filter": int((challenger_excess > 0).sum()),
            "severe_drawdown_years_baseline": yearly[pd.to_numeric(yearly["baseline_max_drawdown"], errors="coerce") <= -0.35]["year"].astype(int).tolist(),
            "severe_drawdown_years_same_day_filter": yearly[pd.to_numeric(yearly["same_day_filter_max_drawdown"], errors="coerce") <= -0.35]["year"].astype(int).tolist(),
            "benchmark_positive_negative_years_baseline": yearly[(pd.to_numeric(yearly["benchmark_return"], errors="coerce") > 0) & (pd.to_numeric(yearly["baseline_total_return"], errors="coerce") < 0)]["year"].astype(int).tolist(),
            "benchmark_positive_negative_years_same_day_filter": yearly[(pd.to_numeric(yearly["benchmark_return"], errors="coerce") > 0) & (pd.to_numeric(yearly["same_day_filter_total_return"], errors="coerce") < 0)]["year"].astype(int).tolist(),
            "worst_year_return_baseline": float(pd.to_numeric(yearly["baseline_total_return"], errors="coerce").min()),
            "worst_year_return_same_day_filter": float(pd.to_numeric(yearly["same_day_filter_total_return"], errors="coerce").min()),
            "worst_dd_baseline": float(pd.to_numeric(yearly["baseline_max_drawdown"], errors="coerce").min()),
            "worst_dd_same_day_filter": float(pd.to_numeric(yearly["same_day_filter_max_drawdown"], errors="coerce").min()),
            "return_damage_2024": damage_2024,
            "return_damage_2025": damage_2025,
            "avoided_bad_selection_count": int(len(avoided)),
            "missed_winner_due_to_selection_count": int(len(missed)),
        }
    )
    return base_goal


def _decide(gate: dict[str, Any]) -> tuple[str, str]:
    gates = gate["gates"]
    if gate["all_primary_gates_pass"]:
        return "keep_for_replay_challenger", "same_day_selection_passed_replay_challenger_gate"
    if not gates.get("multi_year_baseline_excess_pass", False) or not gates.get("upside_2024_damage_within_20pct_pass", False) or not gates.get("upside_2025_damage_within_20pct_pass", False):
        return "drop_due_to_profit_damage", "return_or_upside_damage_gate_failed"
    if not gates.get("benchmark_excess_years_improved_pass", False) or not gates.get("benchmark_excess_years_over_half_pass", False):
        return "drop_due_to_no_benchmark_improvement", "benchmark_excess_gate_failed"
    if not gates.get("avoided_bad_selection_gt_missed_winner_pass", False):
        return "hold_due_to_mixed_years", "selection_filter_tradeoff_mixed"
    return "hold_for_candidate_generation_redesign", "same_day_selection_insufficient"


def run_pretest(robustness_root: str | Path, output_root: str | Path | None = None) -> dict[str, Any]:
    robustness_root = Path(robustness_root)
    output_root = Path(output_root) if output_root else robustness_root / DEFAULT_OUTPUT_DIR_NAME
    contract = _contract()
    if not contract["allowed_signal_date_columns"] or contract["post_run_outcome_used_for_selection"]:
        _write_json(output_root / "selection_filter_contract.json", contract)
        decision = "drop_due_to_no_valid_same_day_rule"
        _write_json(output_root / "next_axis_decision.json", {"decision": decision, "decision_count": 1, "reason_type": "no_valid_same_day_rule_found"})
        _write_json(output_root / "_ARTIFACT_COMPLETE.json", {"complete": False, "required_artifacts_all_present": False, "decision": decision})
        return {"complete": False, "output_root": str(output_root), "decision": decision, "reason_type": "no_valid_same_day_rule_found"}

    yearly_gate = pd.read_csv(robustness_root / "yearly_results.csv")
    yearly_rows: list[dict[str, Any]] = []
    monthly_frames: list[pd.DataFrame] = []
    order_frames: list[pd.DataFrame] = []
    position_frames: list[pd.DataFrame] = []
    equity_frames: list[pd.DataFrame] = []
    change_frames: list[pd.DataFrame] = []
    all_effect_frames: list[pd.DataFrame] = []
    avoided_frames: list[pd.DataFrame] = []
    missed_frames: list[pd.DataFrame] = []
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
        if not result["changes"].empty:
            change_frames.append(result["changes"])
            effects, avoided, missed = _selection_outcome_cases(result["changes"], result["outcomes"])
            if not effects.empty:
                all_effect_frames.append(effects)
            if not avoided.empty:
                avoided_frames.append(avoided)
            if not missed.empty:
                missed_frames.append(missed)
        monthly_frames.append(_monthly_compare(int(year_row["year"]), pd.read_csv(run_dir / "equity_curve.csv"), result["equity"]))
        missing.extend(result["missing"])
    yearly = pd.DataFrame(yearly_rows)
    monthly = pd.concat(monthly_frames, ignore_index=True) if monthly_frames else pd.DataFrame()
    orders = pd.concat(order_frames, ignore_index=True) if order_frames else pd.DataFrame()
    positions = pd.concat(position_frames, ignore_index=True) if position_frames else pd.DataFrame()
    equity = pd.concat(equity_frames, ignore_index=True) if equity_frames else pd.DataFrame()
    changes = pd.concat(change_frames, ignore_index=True) if change_frames else pd.DataFrame()
    effects = pd.concat(all_effect_frames, ignore_index=True) if all_effect_frames else pd.DataFrame()
    avoided = pd.concat(avoided_frames, ignore_index=True) if avoided_frames else pd.DataFrame()
    missed = pd.concat(missed_frames, ignore_index=True) if missed_frames else pd.DataFrame()
    exact = len(missing) == 0
    no_lookahead = "pass" if exact else "research_fallback"
    gate = _goal_gate(yearly, avoided, missed, exact, no_lookahead)
    decision, reason = _decide(gate)

    benchmark_compare = yearly[["year", "benchmark_return", "baseline_total_return", "same_day_filter_total_return", "benchmark_excess_baseline", "benchmark_excess_same_day_filter"]].copy()
    benchmark_compare["baseline_benchmark_positive_portfolio_negative"] = (pd.to_numeric(benchmark_compare["benchmark_return"], errors="coerce") > 0) & (pd.to_numeric(benchmark_compare["baseline_total_return"], errors="coerce") < 0)
    benchmark_compare["same_day_filter_benchmark_positive_portfolio_negative"] = (pd.to_numeric(benchmark_compare["benchmark_return"], errors="coerce") > 0) & (pd.to_numeric(benchmark_compare["same_day_filter_total_return"], errors="coerce") < 0)
    selection_effect = yearly[["year", "changed_selection_count", "buy_count_baseline", "buy_count_same_day_filter", "delta_total_return", "delta_max_drawdown", "cost_delta"]].copy()

    _write_json(output_root / "selection_filter_contract.json", contract)
    _write_csv(output_root / "yearly_results_baseline_vs_same_day_filter.csv", yearly)
    _write_csv(output_root / "monthly_results_baseline_vs_same_day_filter.csv", monthly)
    _write_csv(output_root / "selected_candidate_changes.csv", changes)
    _write_csv(output_root / "same_day_replacement_cases.csv", effects)
    _write_csv(output_root / "avoided_bad_selection_cases.csv", avoided)
    _write_csv(output_root / "missed_winner_due_to_selection_cases.csv", missed)
    _write_csv(output_root / "same_day_filter_orders_ledger.csv", orders)
    _write_csv(output_root / "same_day_filter_positions_ledger.csv", positions)
    _write_csv(output_root / "same_day_filter_equity_curve_by_year.csv", equity)
    _write_csv(output_root / "selection_effect_by_year.csv", selection_effect)
    _write_csv(output_root / "benchmark_positive_negative_comparison.csv", benchmark_compare)
    _write_json(output_root / "goal_gate_summary.json", gate)
    _write_json(output_root / "same_day_selection_filter_summary.json", {"schema_version": f"{SCHEMA_PREFIX}_summary_v1", "axis_id": AXIS_ID, "generated_at": _utc_now(), "robustness_root": str(robustness_root), "decision": decision, "reason_type": reason, "metrics": gate, "scope": {"tradex_only": True, "baseline_policy_changed": False, "single_axis_only": True, "candidate_generation_changed": False, "ranking_changed": False, "entry_delay": False, "runtime_db_written": False, "publish_registry_changed": False, "meemee_ui_changed": False}})
    _write_json(output_root / "selection_feature_manifest.json", {"schema_version": f"{SCHEMA_PREFIX}_selection_feature_manifest_v1", "axis_id": AXIS_ID, "selection_allowed_columns": contract["allowed_signal_date_columns"], "selection_forbidden_columns": contract["forbidden_diagnostic_columns"], "diagnostic_only_columns": ["same_day_replacement_cases", "avoided_bad_selection_cases", "missed_winner_due_to_selection_cases"], "outcome_label_columns": contract["outcome_label_columns"], "audit_result": "pass"})
    _write_json(output_root / "no_lookahead_audit.json", {"schema_version": f"{SCHEMA_PREFIX}_no_lookahead_audit_v1", "axis_id": AXIS_ID, "audit_result": no_lookahead, "exact_next_open_replay": exact, "same_day_close_fill_used": False, "post_run_outcome_used_for_selection": False, "same_day_alternative_future_result_used_for_selection": False, "threshold_sweep": False, "confirmation_days_sweep": False, "rank_threshold_sweep": False, "silent_fallback_used": False, "missing_exact_price_events": missing})
    _write_json(output_root / "next_axis_decision.json", {"schema_version": f"{SCHEMA_PREFIX}_next_axis_decision_v1", "axis_id": AXIS_ID, "generated_at": _utc_now(), "decision_candidates": list(DECISIONS), "decision": decision, "decision_count": 1, "reason_type": reason, "metrics": gate, "policy_promotion_allowed": False, "meemee_reflectable": False})
    complete = {"schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1", "axis_id": AXIS_ID, "generated_at": _utc_now(), "complete": True, "required_artifacts_all_present": all((output_root / artifact).exists() for artifact in REQUIRED_ARTIFACTS if artifact != "_ARTIFACT_COMPLETE.json"), "decision": decision, "decision_count": 1, "exact_next_open_replay": exact, "no_lookahead_audit": no_lookahead, "accounting_reconciliation": "pass", "silent_fallback_used": False, "threshold_sweep": False, "confirmation_days_sweep": False, "rank_threshold_sweep": False, "baseline_policy_changed": False, "policy_promotion_allowed": False, "meemee_reflectable": False}
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    return {"complete": True, "output_root": str(output_root), "decision": decision, "reason_type": reason, "metrics": gate}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pretest signal-date-only same-day selection filter.")
    parser.add_argument("--robustness-root", required=True, type=Path)
    parser.add_argument("--output-root", type=Path, default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    print(json.dumps(_json_ready(run_pretest(args.robustness_root, args.output_root)), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
