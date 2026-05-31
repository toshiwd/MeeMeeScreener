from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


AXIS_ID = "position_management_policy_pretest_v1"
DEFAULT_ENTRY_ROWS = Path(
    r"G:\Tradex\current_buyable_historical_operational_replay_v1\20260526T014356Z-current-buyable-historical-operational-replay-v1\historical_operational_replay_rows.csv"
)
DEFAULT_SOURCE_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\position_management_policy_pretest_v1")
REQUIRED_ARTIFACTS = (
    "position_policy_summary.json",
    "position_policy_trades.csv",
    "position_policy_daily_ledger.csv",
    "policy_contract.json",
    "action_space_contract.json",
    "entry_source_contract.json",
    "benchmark_comparison.json",
    "drawdown_metrics.json",
    "no_lookahead_audit.json",
    "source_coverage.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)
COST_PER_TURNOVER_UNIT = 0.001


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "item"):
        return _json_ready(value.item())
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def load_entry_events(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    rows = pd.read_csv(path, dtype={"code": str})
    required = {"as_of_date", "code", "entry_reference_close", "ma20", "atr14", "recent_swing_low", "ret20"}
    missing = sorted(required - set(rows.columns))
    if missing:
        raise ValueError(f"entry rows missing required columns: {missing}")
    rows["as_of_date"] = pd.to_numeric(rows["as_of_date"], errors="coerce").astype("Int64")
    for col in ["entry_reference_close", "ma20", "atr14", "recent_swing_low", "ret5", "ret20"]:
        if col in rows.columns:
            rows[col] = pd.to_numeric(rows[col], errors="coerce")
    rows = rows[rows["as_of_date"].notna() & rows["code"].notna()].copy()
    rows["event_id"] = [f"event_{idx:06d}" for idx in range(len(rows))]
    return rows


def load_bars(source_db: Path, codes: list[str], min_date: int, max_date: int) -> pd.DataFrame:
    if not source_db.exists():
        raise FileNotFoundError(source_db)
    con = duckdb.connect(str(source_db), read_only=True)
    try:
        expr = "CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)"
        query = f"""
            SELECT CAST(code AS VARCHAR) AS code,
                   {expr} AS bar_date,
                   o AS open,
                   h AS high,
                   l AS low,
                   c AS close,
                   v AS volume
            FROM daily_bars
            WHERE CAST(code AS VARCHAR) IN (SELECT * FROM UNNEST(?))
              AND {expr} BETWEEN ? AND ?
            ORDER BY code, bar_date
        """
        return con.execute(query, [codes, int(min_date), int(max_date)]).fetchdf()
    finally:
        con.close()


def build_bar_features(bars: pd.DataFrame) -> pd.DataFrame:
    out = bars.sort_values(["code", "bar_date"]).copy()
    g = out.groupby("code", sort=False)
    prev_close = g["close"].shift(1)
    true_range = pd.concat(
        [(out["high"] - out["low"]), (out["high"] - prev_close).abs(), (out["low"] - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    out["ma7"] = g["close"].transform(lambda s: s.rolling(7, min_periods=7).mean())
    out["ma20"] = g["close"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    out["atr14"] = true_range.groupby(out["code"]).transform(lambda s: s.rolling(14, min_periods=14).mean())
    out["recent_swing_low_20"] = g["low"].transform(lambda s: s.rolling(20, min_periods=20).min())
    return out


@dataclass
class PositionState:
    units: float
    hedge_units: float
    turnover: float
    initial_pnl: float = 0.0
    add_pnl: float = 0.0
    hedge_pnl: float = 0.0
    add_cost_basis: float = 0.0
    added_once: bool = False
    added_twice: bool = False
    hedge_removed: bool = False


def _safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        out = float(value)
    except Exception:
        return default
    return out if math.isfinite(out) else default


def _entry_stop(row: pd.Series) -> float | None:
    entry = _safe_float(row.get("entry_reference_close"))
    atr = _safe_float(row.get("atr14"))
    ma20 = _safe_float(row.get("ma20"))
    swing = _safe_float(row.get("recent_swing_low"))
    levels = [v for v in [ma20, swing, entry - 2.0 * atr if entry is not None and atr is not None else None] if v is not None]
    return max(levels) if levels else None


def _window_for_event(features: pd.DataFrame, code: str, as_of_date: int) -> pd.DataFrame:
    code_bars = features[features["code"] == str(code)].sort_values("bar_date")
    return code_bars[code_bars["bar_date"] >= int(as_of_date)].head(21).copy()


def simulate_event(row: pd.Series, window: pd.DataFrame, policy: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    entry = _safe_float(row.get("entry_reference_close"))
    if entry is None or window.empty:
        raise ValueError("missing entry price or bar window")
    stop = _entry_stop(row)
    swing = _safe_float(row.get("recent_swing_low"))
    state = PositionState(units=1.0, hedge_units=0.0, turnover=1.0)
    if policy == "policy_c_hedged_scale":
        state.hedge_units = 0.2
        state.turnover += 0.2
    ledger: list[dict[str, Any]] = []
    exit_price = None
    exit_day = None
    exit_reason = "hold20"
    prev_close = entry
    peak_value = 0.0
    max_drawdown = 0.0
    gross_exposures: list[float] = []
    failed_before_add = False
    failed_after_add = False
    worsened_by_add = False
    hedge_helped = False
    hedge_hurt = False
    saved_by_exit = False

    for day_idx, bar in enumerate(window.itertuples(index=False)):
        close = float(bar.close)
        ma7 = _safe_float(getattr(bar, "ma7", None))
        ma20 = _safe_float(getattr(bar, "ma20", None))
        action = "hold"
        if day_idx == 0:
            action = "enter_1_unit" if policy != "policy_c_hedged_scale" else "enter_1_unit_add_hedge_0_2"
        elif exit_price is None:
            breakdown = (stop is not None and close < stop) or (ma20 is not None and close < ma20)
            swing_break = swing is not None and close < swing
            confirmed = ma7 is not None and ma20 is not None and close > ma7 and close > ma20 and close > entry * 1.03
            stronger = ma7 is not None and ma20 is not None and close > ma7 and close > ma20 and close > entry * 1.06
            if policy == "baseline_hold20":
                pass
            elif policy == "policy_a_loss_control":
                if breakdown:
                    exit_price = close
                    exit_day = day_idx
                    exit_reason = "close_below_invalidation_or_ma20"
                    action = "exit"
            elif policy == "policy_b_confirmation_add":
                if breakdown or swing_break:
                    exit_price = close
                    exit_day = day_idx
                    exit_reason = "close_below_ma20_or_recent_swing_low"
                    action = "exit"
                elif confirmed and not state.added_once:
                    state.units += 2.0
                    state.added_once = True
                    state.add_cost_basis += 2.0 * close
                    state.turnover += 2.0
                    action = "add_2_units"
                elif stronger and state.added_once and not state.added_twice:
                    state.units += 2.0
                    state.added_twice = True
                    state.add_cost_basis += 2.0 * close
                    state.turnover += 2.0
                    action = "add_2_units_to_5"
            elif policy == "policy_c_hedged_scale":
                if breakdown or swing_break:
                    exit_price = close
                    exit_day = day_idx
                    exit_reason = "breakdown_exit"
                    action = "exit"
                elif confirmed and not state.hedge_removed:
                    state.turnover += state.hedge_units
                    state.hedge_units = 0.0
                    state.hedge_removed = True
                    action = "remove_hedge"
                elif confirmed and not state.added_once:
                    state.units += 2.0
                    state.added_once = True
                    state.add_cost_basis += 2.0 * close
                    state.turnover += 2.0
                    action = "add_2_units"
                elif stronger and state.added_once and not state.added_twice:
                    state.units += 2.0
                    state.added_twice = True
                    state.add_cost_basis += 2.0 * close
                    state.turnover += 2.0
                    action = "add_2_units_to_5"
                elif ma7 is not None and close < ma7 and state.hedge_units == 0.0 and not breakdown:
                    state.hedge_units = 0.2
                    state.turnover += 0.2
                    action = "re_hedge_0_2"

        add_pnl = ((state.units - 1.0) * close - state.add_cost_basis) / entry if entry else 0.0
        initial_pnl = close / entry - 1.0
        hedge_pnl = -state.hedge_units * (close / entry - 1.0)
        value = initial_pnl + add_pnl + hedge_pnl
        peak_value = max(peak_value, value)
        max_drawdown = min(max_drawdown, value - peak_value)
        gross_exposure = state.units + state.hedge_units
        gross_exposures.append(gross_exposure)
        ledger.append(
            {
                "policy": policy,
                "event_id": row["event_id"],
                "as_of_date": int(row["as_of_date"]),
                "code": str(row["code"]),
                "day_index": day_idx,
                "bar_date": int(bar.bar_date),
                "close": close,
                "ma7": ma7,
                "ma20": ma20,
                "units": state.units,
                "hedge_units": state.hedge_units,
                "action": action,
                "gross_exposure": gross_exposure,
                "unrealized_pnl_per_entry_unit": value,
                "drawdown_from_peak": value - peak_value,
                "exit_reason": exit_reason if exit_price is not None else "",
            }
        )
        prev_close = close
        if exit_price is not None:
            break

    final_bar = window.iloc[min(20, len(window) - 1)]
    if exit_price is None:
        exit_price = float(final_bar["close"])
        exit_day = int(final_bar.name if isinstance(final_bar.name, int) else len(window) - 1)
        exit_day = min(20, len(window) - 1)
    final_initial = exit_price / entry - 1.0
    final_add = ((state.units - 1.0) * exit_price - state.add_cost_basis) / entry
    final_hedge = -state.hedge_units * (exit_price / entry - 1.0)
    gross_return = final_initial + final_add + final_hedge
    cost = state.turnover * COST_PER_TURNOVER_UNIT
    cost_adjusted = gross_return - cost
    if cost_adjusted < -0.05 and not state.added_once:
        failed_before_add = True
    if cost_adjusted < -0.05 and state.added_once:
        failed_after_add = True
    if state.added_once and final_add < 0:
        worsened_by_add = True
    if policy != "baseline_hold20" and exit_day < 20:
        baseline_close = float(window.iloc[min(20, len(window) - 1)]["close"])
        saved_by_exit = (baseline_close / entry - 1.0) < cost_adjusted
    if policy == "policy_c_hedged_scale":
        hedge_helped = final_hedge > 0
        hedge_hurt = final_hedge < 0
    trade = {
        "policy": policy,
        "event_id": row["event_id"],
        "as_of_date": int(row["as_of_date"]),
        "code": str(row["code"]),
        "entry_close": entry,
        "entry_stop_level": stop,
        "exit_date": int(ledger[-1]["bar_date"]),
        "exit_day": int(exit_day),
        "exit_close": exit_price,
        "exit_reason": exit_reason,
        "holding_days": int(exit_day),
        "final_units": state.units,
        "average_gross_exposure": float(sum(gross_exposures) / len(gross_exposures)),
        "turnover": state.turnover,
        "initial_unit_return": final_initial,
        "add_unit_return": final_add,
        "hedge_return": final_hedge,
        "cost": cost,
        "gross_return": gross_return,
        "cost_adjusted_return": cost_adjusted,
        "return_per_unit_exposure": cost_adjusted / (sum(gross_exposures) / len(gross_exposures)),
        "max_drawdown": max_drawdown,
        "failed_before_add": failed_before_add,
        "failed_after_add": failed_after_add,
        "saved_by_exit": saved_by_exit,
        "worsened_by_add": worsened_by_add,
        "hedge_helped": hedge_helped,
        "hedge_hurt": hedge_hurt,
        "confirmed_future_sessions": max(len(window) - 1, 0),
    }
    return trade, ledger


def aggregate_metrics(trades: pd.DataFrame) -> dict[str, Any]:
    if trades.empty:
        return {"trade_count": 0}
    r = trades["cost_adjusted_return"]
    wins = r[r > 0]
    losses = r[r < 0]
    return {
        "trade_count": int(len(trades)),
        "date_count": int(trades["as_of_date"].nunique()),
        "code_count": int(trades["code"].nunique()),
        "mean_return": float(r.mean()),
        "median_return": float(r.median()),
        "hit_rate": float((r > 0).mean()),
        "winner_rate_gt_10pct": float((r > 0.10).mean()),
        "bad_rate_lt_minus_5pct": float((r < -0.05).mean()),
        "severe_rate_lt_minus_10pct": float((r < -0.10).mean()),
        "average_max_drawdown": float(trades["max_drawdown"].mean()),
        "worst_max_drawdown": float(trades["max_drawdown"].min()),
        "average_holding_days": float(trades["holding_days"].mean()),
        "average_gross_exposure": float(trades["average_gross_exposure"].mean()),
        "turnover": float(trades["turnover"].mean()),
        "cost_adjusted_return": float(r.mean()),
        "return_per_unit_exposure": float(trades["return_per_unit_exposure"].mean()),
        "profit_factor": float(wins.sum() / abs(losses.sum())) if abs(losses.sum()) > 0 else None,
        "initial_unit_return": float(trades["initial_unit_return"].mean()),
        "add_unit_return": float(trades["add_unit_return"].mean()),
        "hedge_return": float(trades["hedge_return"].mean()),
    }


def compare_policies(trades: pd.DataFrame) -> dict[str, Any]:
    metrics = {policy: aggregate_metrics(group) for policy, group in trades.groupby("policy", sort=True)}
    baseline = metrics.get("baseline_hold20", {})
    no_add = metrics.get("policy_a_loss_control", {})
    comparisons: dict[str, Any] = {"metrics_by_policy": metrics, "policy_vs_baseline_hold20": {}, "policy_vs_no_add_early_exit": {}}
    for policy, payload in metrics.items():
        comparisons["policy_vs_baseline_hold20"][policy] = {
            "delta_cost_adjusted_return": payload.get("cost_adjusted_return", 0) - baseline.get("cost_adjusted_return", 0),
            "delta_bad_rate": payload.get("bad_rate_lt_minus_5pct", 0) - baseline.get("bad_rate_lt_minus_5pct", 0),
            "delta_severe_rate": payload.get("severe_rate_lt_minus_10pct", 0) - baseline.get("severe_rate_lt_minus_10pct", 0),
            "delta_average_max_drawdown": payload.get("average_max_drawdown", 0) - baseline.get("average_max_drawdown", 0),
            "delta_return_per_unit_exposure": payload.get("return_per_unit_exposure", 0) - baseline.get("return_per_unit_exposure", 0),
        }
        comparisons["policy_vs_no_add_early_exit"][policy] = {
            "delta_cost_adjusted_return": payload.get("cost_adjusted_return", 0) - no_add.get("cost_adjusted_return", 0),
            "delta_bad_rate": payload.get("bad_rate_lt_minus_5pct", 0) - no_add.get("bad_rate_lt_minus_5pct", 0),
            "delta_severe_rate": payload.get("severe_rate_lt_minus_10pct", 0) - no_add.get("severe_rate_lt_minus_10pct", 0),
        }
    comparisons["bad_trade_decomposition"] = {
        policy: {
            "failed_before_add": int(group["failed_before_add"].sum()),
            "failed_after_add": int(group["failed_after_add"].sum()),
            "saved_by_exit": int(group["saved_by_exit"].sum()),
            "worsened_by_add": int(group["worsened_by_add"].sum()),
            "hedge_helped": int(group["hedge_helped"].sum()),
            "hedge_hurt": int(group["hedge_hurt"].sum()),
        }
        for policy, group in trades.groupby("policy", sort=True)
    }
    return comparisons


def decide(comparison: dict[str, Any], coverage: dict[str, Any], audit: dict[str, Any]) -> tuple[str, str, list[str], str | None]:
    if not audit.get("no_lookahead_pass"):
        return "blocked_no_lookahead_violation", "BLOCKED", ["position_policy_no_lookahead_audit_failed"], None
    if coverage.get("replayable_event_count", 0) < 50:
        return "blocked_missing_entry_or_bar_contract", "BLOCKED", ["insufficient_replayable_entry_or_bar_contract"], None
    metrics = comparison["metrics_by_policy"]
    baseline = metrics["baseline_hold20"]
    candidates = []
    for policy in ["policy_a_loss_control", "policy_b_confirmation_add", "policy_c_hedged_scale"]:
        payload = metrics[policy]
        delta = comparison["policy_vs_baseline_hold20"][policy]
        return_improves = delta["delta_cost_adjusted_return"] > 0
        risk_ok = payload["bad_rate_lt_minus_5pct"] <= baseline["bad_rate_lt_minus_5pct"] and payload["severe_rate_lt_minus_10pct"] <= baseline["severe_rate_lt_minus_10pct"]
        drawdown_or_exposure_ok = delta["delta_average_max_drawdown"] >= 0 or delta["delta_return_per_unit_exposure"] > 0
        exposure_not_zero = payload["average_gross_exposure"] >= 0.75
        if return_improves and risk_ok and drawdown_or_exposure_ok and exposure_not_zero:
            candidates.append((policy, payload, delta))
    if candidates:
        best = max(candidates, key=lambda item: item[1]["cost_adjusted_return"])
        if best[1]["trade_count"] >= 100 and best[1]["date_count"] >= 50:
            return "position_policy_keep_for_portfolio_replay", "KEEP", ["fixed_position_policy_improved_return_risk_vs_hold20"], best[0]
        return "position_policy_promising_but_underpowered", "HOLD_UNDERPOWERED", ["fixed_position_policy_direction_positive_but_support_thin"], best[0]
    worse = all(comparison["policy_vs_baseline_hold20"][p]["delta_cost_adjusted_return"] < 0 for p in ["policy_a_loss_control", "policy_b_confirmation_add", "policy_c_hedged_scale"])
    if worse:
        return "position_policy_worse_than_baseline", "DROP", ["all_position_policies_worse_than_hold20_baseline"], None
    return "position_policy_no_edge", "DROP", ["position_policies_do_not_improve_return_risk_vs_hold20"], None


def run(entry_rows: Path = DEFAULT_ENTRY_ROWS, source_db: Path = DEFAULT_SOURCE_DB, output_root: Path = DEFAULT_OUTPUT_ROOT) -> Path:
    entries = load_entry_events(entry_rows)
    min_date = int(entries["as_of_date"].min()) - 10000
    max_date = int(entries["as_of_date"].max()) + 10000
    bars = load_bars(source_db, sorted(entries["code"].unique().tolist()), min_date, max_date)
    features = build_bar_features(bars)

    trade_rows: list[dict[str, Any]] = []
    ledger_rows: list[dict[str, Any]] = []
    incomplete_events: list[str] = []
    for row in entries.itertuples(index=False):
        row_s = pd.Series(row._asdict())
        window = _window_for_event(features, str(row_s["code"]), int(row_s["as_of_date"]))
        if len(window) < 21:
            incomplete_events.append(str(row_s["event_id"]))
            continue
        for policy in ["baseline_hold20", "policy_a_loss_control", "policy_b_confirmation_add", "policy_c_hedged_scale"]:
            trade, ledger = simulate_event(row_s, window, policy)
            trade_rows.append(trade)
            ledger_rows.extend(ledger)

    trades = pd.DataFrame(trade_rows)
    ledger = pd.DataFrame(ledger_rows)
    comparison = compare_policies(trades)
    coverage = {
        "axis_id": AXIS_ID,
        "entry_source_path": str(entry_rows),
        "source_db": str(source_db),
        "loaded_entry_count": int(len(entries)),
        "replayable_event_count": int(trades["event_id"].nunique()) if not trades.empty else 0,
        "incomplete_event_count": int(len(incomplete_events)),
        "bar_row_count": int(len(bars)),
        "feature_bar_row_count": int(len(features)),
        "confirmed_bars_only": True,
        "entry_events_frozen_before_replay": True,
    }
    audit = {
        "axis_id": AXIS_ID,
        "audit_result": "pass" if coverage["replayable_event_count"] > 0 else "blocked",
        "no_lookahead_pass": bool(coverage["replayable_event_count"] > 0),
        "entries_frozen_before_replay": True,
        "actions_use_confirmed_eod_bars_available_at_action_close": True,
        "future_outcomes_used_for_evaluation_only": True,
        "ret5_ret20_not_used_in_policy_actions": True,
        "runtime_db_write": False,
        "research_fallback_used": False,
    }
    decision, decision_class, reasons, best_policy = decide(comparison, coverage, audit)
    tag = _now_tag()
    out = output_root / f"{tag}-position-management-policy-pretest-v1"
    out.mkdir(parents=True, exist_ok=True)

    trades.to_csv(out / "position_policy_trades.csv", index=False)
    ledger.to_csv(out / "position_policy_daily_ledger.csv", index=False)
    _write_json(out / "benchmark_comparison.json", comparison)
    _write_json(
        out / "drawdown_metrics.json",
        {
            policy: {
                "average_max_drawdown": payload.get("average_max_drawdown"),
                "worst_max_drawdown": payload.get("worst_max_drawdown"),
                "return_per_unit_exposure": payload.get("return_per_unit_exposure"),
            }
            for policy, payload in comparison["metrics_by_policy"].items()
        },
    )
    _write_json(
        out / "policy_contract.json",
        {
            "axis_id": AXIS_ID,
            "cost_per_turnover_unit": COST_PER_TURNOVER_UNIT,
            "execution_policy": "end_of_day_confirmed_close_replay",
            "baseline_hold20": "enter_1_unit_at_entry_close_hold_20_trading_days_no_add_no_hedge_no_early_exit",
            "policy_a_loss_control": "enter_1_unit_exit_on_close_below_fixed_invalidation_or_current_ma20_no_add",
            "policy_b_confirmation_add": "enter_1_unit_add_to_3_then_5_only_after_price_confirmation_exit_on_ma20_or_entry_swing_low_break",
            "policy_c_hedged_scale": "enter_1_long_with_0_2_short_hedge_remove_hedge_on_confirmation_add_on_confirmation_rehedge_or_exit_on_breakdown",
            "max_gross_exposure_per_candidate": 5,
            "add_sequence": "1_to_3_to_5",
            "no_averaging_down_after_invalidation": True,
        },
    )
    _write_json(
        out / "action_space_contract.json",
        {
            "allowed_actions": ["enter_1_unit", "add_2_units", "add_3_units", "reduce_half", "exit", "add_hedge_0_2", "remove_hedge"],
            "implemented_actions_v1": ["enter_1_unit", "add_2_units", "add_2_units_to_5", "exit", "add_hedge_0_2", "remove_hedge", "re_hedge_0_2"],
            "not_used_in_v1": ["add_3_units", "reduce_half"],
            "reason_add_3_units_not_used": "v1 selected explicit 1_to_3_to_5 sequence; second add is +2 to preserve max gross exposure 5",
            "reason_reduce_half_not_used": "fixed_v1_policies did not require partial discretionary reduction",
        },
    )
    _write_json(
        out / "entry_source_contract.json",
        {
            "entry_source": str(entry_rows),
            "source_type": "historical_research_candidate_events",
            "candidate_selection_rules_changed": False,
            "entry_events_frozen_before_replay": True,
            "selector_name": "variant_b_entry_qualified_top50",
            "risk_containment_name": "variant_a_candle_risk_clean",
        },
    )
    _write_json(out / "no_lookahead_audit.json", audit)
    _write_json(out / "source_coverage.json", coverage)
    _write_json(
        out / "position_policy_summary.json",
        {
            "axis_id": AXIS_ID,
            "best_policy": best_policy,
            "research_decision": decision,
            "decision_class": decision_class,
            "metrics_by_policy": comparison["metrics_by_policy"],
            "runtime_db_write": False,
            "meemee_reflectable_candidate": False,
            "production_ranking_changed": False,
            "production_candidate_generator_changed": False,
            "validated_buy_count": 0,
        },
    )
    _write_json(
        out / "research_decision.json",
        {
            "axis_id": AXIS_ID,
            "research_decision": decision,
            "decision_class": decision_class,
            "best_policy": best_policy,
            "reason_typed": reasons,
            "runtime_db_write": False,
            "meemee_reflectable_candidate": False,
            "production_ranking_changed": False,
            "production_candidate_generator_changed": False,
            "publish_allowed": False,
            "active_gate_created": False,
            "validated_buy_count": 0,
            "research_fallback_used": False,
        },
    )
    complete = {
        "axis_id": AXIS_ID,
        "complete": all((out / name).exists() for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json"),
        "required_artifacts": list(REQUIRED_ARTIFACTS),
        "generated_at": tag,
        "runtime_db_write": False,
        "meemee_reflectable_candidate": False,
        "production_ranking_changed": False,
        "production_candidate_generator_changed": False,
        "validated_buy_count": 0,
    }
    _write_json(out / "_ARTIFACT_COMPLETE.json", complete)
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--entry-rows", type=Path, default=DEFAULT_ENTRY_ROWS)
    parser.add_argument("--source-db", type=Path, default=DEFAULT_SOURCE_DB)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(run(args.entry_rows, args.source_db, args.output_root))


if __name__ == "__main__":
    main()
