from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


AXIS_ID = "shallow_high_zone_next_open_execution_v1"
OUT_ROOT = Path(r"G:\Tradex\chart_entry_geometry_research_v1")
TP, SL, H = 0.08, 0.05, 10


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _pf(values: pd.Series) -> float | None:
    positive, negative = values[values > 0].sum(), values[values < 0].sum()
    return None if negative == 0 else float(positive / abs(negative))


def _metrics(frame: pd.DataFrame, value: str) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    splits: dict[str, dict[str, Any]] = {}
    yearly: list[dict[str, Any]] = []
    for split, group in frame.groupby("split"):
        daily = group.groupby("date", as_index=False)[value].mean()
        splits[str(split)] = {
            "sample_count": int(len(group)),
            "signal_days": int(group.date.nunique()),
            "expectancy": float(group[value].mean()),
            "profit_factor": _pf(group[value]),
            "daily_profit_factor": _pf(daily[value]),
        }
    for year, group in frame.groupby("year"):
        daily = group.groupby("date", as_index=False)[value].mean()
        yearly.append({"year": int(year), "daily_profit_factor": _pf(daily[value]), "daily_expectancy": float(daily[value].mean())})
    return splits, yearly


def _cap_three(frame: pd.DataFrame, feature: str, ascending: bool) -> pd.DataFrame:
    parts = [
        group.sort_values([feature, "code"], ascending=[ascending, True]).head(3)
        for _, group in frame.groupby("next_entry_date")
    ]
    return pd.concat(parts, ignore_index=True) if parts else frame.iloc[0:0].copy()


def _portfolio_cap(frame: pd.DataFrame, maximum_positions: int) -> pd.DataFrame:
    accepted: list[int] = []
    active_exit_dates: list[int] = []
    for entry_date, group in frame.sort_values(["next_entry_date", "tie_gap_ma60", "code"], ascending=[True, False, True]).groupby("next_entry_date"):
        active_exit_dates = [exit_date for exit_date in active_exit_dates if exit_date >= int(entry_date)]
        available = maximum_positions - len(active_exit_dates)
        if available <= 0:
            continue
        for index, row in group.head(available).iterrows():
            accepted.append(index)
            active_exit_dates.append(int(row["exit_date"]))
    return frame.loc[accepted].copy()


def run() -> Path:
    sys.path.insert(0, str(Path.cwd()))
    sys.path.insert(0, "app")
    from backend.services.codex_bridge_service import get_runtime_stock_db_status

    runtime = get_runtime_stock_db_status()
    db = Path(str(runtime["selected_runtime_db_path"]))
    output = OUT_ROOT / f"{_tag()}-{AXIS_ID}"
    output.mkdir(parents=True, exist_ok=False)
    next_highs = ", ".join(f"LEAD(h,{i}) OVER w AS h{i}" for i in range(1, H + 1))
    next_lows = ", ".join(f"LEAD(l,{i}) OVER w AS l{i}" for i in range(1, H + 1))
    next_closes = ", ".join(f"LEAD(c,{i}) OVER w AS c{i}" for i in range(1, H + 1))
    next_dates = ", ".join(f"LEAD(b.date,{i}) OVER w AS next_d{i}" for i in range(1, H + 1))
    next_hit_tp = "LEAST(" + ", ".join(f"CASE WHEN h{i} >= next_open * {1 + TP} THEN {i} ELSE 99 END" for i in range(1, H + 1)) + ")"
    next_hit_sl = "LEAST(" + ", ".join(f"CASE WHEN l{i} <= next_open * {1 - SL} THEN {i} ELSE 99 END" for i in range(1, H + 1)) + ")"
    close_hit_tp = "LEAST(" + ", ".join(f"CASE WHEN h{i} >= c * {1 + TP} THEN {i} ELSE 99 END" for i in range(1, H + 1)) + ")"
    close_hit_sl = "LEAST(" + ", ".join(f"CASE WHEN l{i} <= c * {1 - SL} THEN {i} ELSE 99 END" for i in range(1, H + 1)) + ")"
    sql = f"""
    WITH latest AS (SELECT MAX(date) AS date FROM daily_bars WHERE source = 'pan'),
    eligible AS (
        SELECT code FROM daily_bars WHERE source = 'pan' GROUP BY code HAVING MAX(date) = (SELECT date FROM latest)
    ), bars AS (
        SELECT b.code,b.date,b.o,b.h,b.l,b.c,b.v,
            LAG(b.h,1) OVER w AS prior_high1,
            MAX(b.h) OVER p20 AS prior_high20,
            LAG(b.c,10) OVER w AS c10, AVG(b.c) OVER m20 AS ma20, AVG(b.c) OVER m20lag5 AS ma20_5ago,
            AVG(b.c) OVER m60 AS ma60, AVG(b.v) OVER m20 AS avg_volume20,
            MAX(b.h) OVER last10 AS high10, MIN(b.l) OVER last10 AS low10,
            LEAD(b.o,1) OVER w AS next_open, LEAD(b.date,1) OVER w AS next_entry_date,
            LEAD(b.date,{H}) OVER w AS horizon_date, LEAD(b.c,{H}) OVER w AS close_horizon,
            {next_highs}, {next_lows}, {next_closes}, {next_dates}
        FROM daily_bars b JOIN eligible e USING(code) WHERE b.source = 'pan'
        WINDOW w AS (PARTITION BY b.code ORDER BY b.date),
            p20 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING),
            m20 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
            m20lag5 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 24 PRECEDING AND 5 PRECEDING),
            m60 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW),
            last10 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW)
    ), signals AS (
        SELECT *, {next_hit_tp} AS next_tp_day, {next_hit_sl} AS next_sl_day,
            {close_hit_tp} AS close_tp_day, {close_hit_sl} AS close_sl_day,
            CAST(year(to_timestamp(CAST(date AS BIGINT))) AS INTEGER) AS year,
            CASE
                WHEN (c / ma60) - 1.0 <= 0.0370751 AND (ma20 / ma20_5ago) - 1.0 > 0.0107939 THEN 9
                WHEN (c / ma60) - 1.0 > 0.0370751 AND (high10 / low10) - 1.0 <= 0.0544289
                     AND v / avg_volume20 <= 0.995031 AND (c / prior_high1) - 1.0 > 0.000984905 THEN 14
                WHEN (c / ma60) - 1.0 > 0.0370751 AND (high10 / low10) - 1.0 > 0.0544289
                     AND (c / prior_high1) - 1.0 > 0.0233188 THEN 20
            END AS shape_leaf
        FROM bars
        WHERE date BETWEEN 1420070400 AND (SELECT date FROM latest)
            AND next_open IS NOT NULL AND close_horizon IS NOT NULL AND prior_high1 IS NOT NULL AND prior_high20 IS NOT NULL
            AND ma20 IS NOT NULL AND ma20_5ago IS NOT NULL AND ma60 IS NOT NULL AND avg_volume20 > 0 AND h > l
            AND ma20 > ma60 AND c >= prior_high20 * 0.95 AND l BETWEEN ma20 * 0.99 AND ma20
            AND c > ma20 AND c > o AND (c-l)/(h-l) >= 0.70
            AND (
                ((c / ma60) - 1.0 <= 0.0370751 AND (ma20 / ma20_5ago) - 1.0 > 0.0107939)
                OR ((c / ma60) - 1.0 > 0.0370751 AND (high10 / low10) - 1.0 <= 0.0544289 AND v / avg_volume20 <= 0.995031 AND (c / prior_high1) - 1.0 > 0.000984905)
                OR ((c / ma60) - 1.0 > 0.0370751 AND (high10 / low10) - 1.0 > 0.0544289 AND (c / prior_high1) - 1.0 > 0.0233188)
            )
    )
    SELECT code,date,next_entry_date,horizon_date,year,shape_leaf,c AS signal_close,next_open AS entry_price,
        (c / ma60) - 1.0 AS tie_gap_ma60, (ma20 / ma20_5ago) - 1.0 AS tie_ma20_slope,
        v / avg_volume20 AS tie_volume_ratio, (high10 / low10) - 1.0 AS tie_range10,
        (c / prior_high1) - 1.0 AS tie_prior_high1_gap, (c / c10) - 1.0 AS tie_ret10,
        (next_open / c) - 1.0 AS next_open_gap, close_horizon,
        {', '.join(f'h{i},l{i},c{i},next_d{i}' for i in range(1, H + 1))},
        CASE WHEN next_sl_day <= {H} AND next_sl_day <= next_tp_day THEN next_sl_day
             WHEN next_tp_day <= {H} THEN next_tp_day ELSE {H} END AS exit_offset,
        CASE WHEN next_sl_day <= {H} AND next_sl_day <= next_tp_day THEN
             CASE next_sl_day {''.join(f' WHEN {i} THEN next_d{i}' for i in range(1, H + 1))} END
             WHEN next_tp_day <= {H} THEN CASE next_tp_day {''.join(f' WHEN {i} THEN next_d{i}' for i in range(1, H + 1))} END
             ELSE next_d{H} END AS exit_date,
        CASE WHEN next_sl_day <= {H} AND next_sl_day <= next_tp_day THEN -{SL}
             WHEN next_tp_day <= {H} THEN {TP} ELSE (close_horizon / next_open) - 1.0 END AS next_open_return,
        CASE WHEN close_sl_day <= {H} AND close_sl_day <= close_tp_day THEN -{SL}
             WHEN close_tp_day <= {H} THEN {TP} ELSE (close_horizon / c) - 1.0 END AS signal_close_return,
        CASE WHEN year <= 2021 THEN 'train' WHEN year <= 2023 THEN 'validation' ELSE 'test' END AS split
    FROM signals
    """
    conn = duckdb.connect(str(db), read_only=True)
    try:
        data = conn.execute(sql).fetchdf()
    finally:
        conn.close()
    data[data["next_open_gap"] <= 0.0].to_csv(output / "prehistory_reference_events.csv", index=False)
    next_splits, next_yearly = _metrics(data, "next_open_return")
    close_splits, close_yearly = _metrics(data, "signal_close_return")
    gates = {
        split: bool(metrics.get("sample_count", 0) >= 300 and (metrics.get("expectancy") or 0) > 0 and (metrics.get("profit_factor") or 0) >= 1.2 and (metrics.get("daily_profit_factor") or 0) >= 1.15)
        for split, metrics in next_splits.items()
    }
    adoptable = all(gates.get(split, False) for split in ("train", "validation", "test")) and len(next_yearly) == 7 and all((row["daily_profit_factor"] or 0) >= 1.0 for row in next_yearly)
    gap_variants: list[dict[str, Any]] = []
    for maximum_gap in (-0.02, -0.01, 0.0, 0.01, 0.02):
        filtered = data[data["next_open_gap"] <= maximum_gap]
        splits, yearly = _metrics(filtered, "next_open_return")
        split_gate = {
            split: bool(item.get("sample_count", 0) >= 300 and (item.get("expectancy") or 0) > 0 and (item.get("profit_factor") or 0) >= 1.2 and (item.get("daily_profit_factor") or 0) >= 1.15)
            for split, item in splits.items()
        }
        keep = all(split_gate.get(split, False) for split in ("train", "validation", "test")) and len(yearly) == 7 and all((row["daily_profit_factor"] or 0) >= 1.0 for row in yearly)
        gap_variants.append({"maximum_next_open_gap": maximum_gap, "metrics_by_split": splits, "yearly_daily_basket_metrics": yearly, "split_gate_pass": split_gate, "candidate_local_decision": "keep" if keep else "drop"})
    train_eligible = [
        row for row in gap_variants
        if row["split_gate_pass"].get("train", False)
    ]
    selected_gap_variant = max(
        train_eligible,
        key=lambda row: float(row["metrics_by_split"]["train"]["daily_profit_factor"] or float("-inf")),
        default=None,
    )
    selected_gap_execution = None
    if selected_gap_variant is not None:
        out_of_sample_pass = all(selected_gap_variant["split_gate_pass"].get(split, False) for split in ("validation", "test"))
        yearly_pass = all((row["daily_profit_factor"] or 0) >= 1.0 for row in selected_gap_variant["yearly_daily_basket_metrics"])
        selected_gap_execution = {
            "selection_protocol": "choose the highest train daily PF among train-gate-passing predeclared gap thresholds; validation and test were not used for selection",
            "selected_maximum_next_open_gap": selected_gap_variant["maximum_next_open_gap"],
            "train_daily_profit_factor": selected_gap_variant["metrics_by_split"]["train"]["daily_profit_factor"],
            "validation_test_gate_pass": out_of_sample_pass,
            "all_years_daily_pf_pass": yearly_pass,
            "candidate_local_decision": "keep" if out_of_sample_pass and yearly_pass else "drop",
        }
    capacity = None
    if selected_gap_execution and selected_gap_execution["candidate_local_decision"] == "keep":
        selected = data[data["next_open_gap"] <= selected_gap_execution["selected_maximum_next_open_gap"]].copy()
        selected.to_csv(output / "eligible_execution_events.csv", index=False)
        leaf_train = selected[selected["year"] <= 2021].groupby("shape_leaf")["next_open_return"].agg(["count", "mean"]).sort_values("mean", ascending=False)
        leaf_scores = {int(leaf): float(row["mean"]) for leaf, row in leaf_train.iterrows()}
        leaf_risk_breakdown: list[dict[str, Any]] = []
        for leaf, group in selected.groupby("shape_leaf"):
            splits, yearly = _metrics(group, "next_open_return")
            losses = group.loc[group["next_open_return"] < 0, "next_open_return"]
            daily_counts = group.groupby("next_entry_date").size()
            leaf_risk_breakdown.append({
                "leaf": int(leaf),
                "metrics_by_split": splits,
                "yearly_daily_basket_metrics": yearly,
                "loss_rate": float((group["next_open_return"] < 0).mean()),
                "loss_mean": float(losses.mean()) if len(losses) else None,
                "loss_p05": float(losses.quantile(0.05)) if len(losses) else None,
                "execution_day_count": int(len(daily_counts)),
                "mean_candidates_per_execution_day": float(daily_counts.mean()),
                "max_candidates_per_execution_day": int(daily_counts.max()),
                "days_with_multiple_candidates": int((daily_counts > 1).sum()),
            })
        cutoff_tie_days = 0
        concentrated_days = 0
        for _, group in selected.groupby("next_entry_date"):
            if len(group) <= 3:
                continue
            concentrated_days += 1
            ordered = group["shape_leaf"].map(leaf_scores).sort_values(ascending=False)
            cutoff_score = ordered.iloc[2]
            if int((ordered == cutoff_score).sum()) > int((ordered > cutoff_score).sum()) + 1:
                cutoff_tie_days += 1
        per_day = selected.groupby("next_entry_date").size().rename("new_entries")
        execution_days = sorted(int(value) for value in per_day.index)
        active_counts: list[int] = []
        for execution_day in execution_days:
            active_counts.append(int(((selected["next_entry_date"] <= execution_day) & (selected["horizon_date"] >= execution_day)).sum()))
        capacity = {
            "fixed_allocation_policy": "equal notional across up to three same-day eligible new entries; a fourth or later name is wait-only because no within-day rank tie-breaker is validated",
            "eligible_execution_count": int(len(selected)),
            "execution_day_count": int(len(per_day)),
            "mean_new_entries_per_execution_day": float(per_day.mean()),
            "max_new_entries_per_execution_day": int(per_day.max()),
            "execution_days_with_multiple_entries": int((per_day > 1).sum()),
            "execution_days_with_more_than_three_entries": int((per_day > 3).sum()),
            "signals_above_three_same_day_cap": int((per_day - 3).clip(lower=0).sum()),
            "maximum_concurrent_positions_at_10_day_horizon": max(active_counts) if active_counts else 0,
            "median_concurrent_positions_at_entry": float(pd.Series(active_counts).median()) if active_counts else 0.0,
            "concurrency_definition": "open positions are conservatively counted from next-session entry through the 10th session horizon; early TP/SL exits are not netted out",
            "holding_conflict_status": "not_historically_reconstructable_from_current positions_live; current holdings are checked only in the current board",
            "train_only_leaf_priority_probe": {
                "leaf_train_metrics": [
                    {"leaf": int(leaf), "sample_count": int(row["count"]), "mean_return": float(row["mean"])}
                    for leaf, row in leaf_train.iterrows()
                ],
                "concentrated_execution_days": concentrated_days,
                "days_where_leaf_priority_still_ties_at_third_slot": cutoff_tie_days,
                "status": "usable_only_if_third-slot ties are sufficiently rare; no secondary feature is adopted in this probe",
            },
            "leaf_risk_breakdown": leaf_risk_breakdown,
        }
        bootstrap_rows: list[dict[str, Any]] = []
        for seed in range(100):
            capped = (
                selected.groupby("next_entry_date", group_keys=False)
                .apply(lambda group: group.sample(n=min(len(group), 3), random_state=seed), include_groups=False)
                .reset_index()
            )
            splits, yearly = _metrics(capped, "next_open_return")
            split_pass = all(
                item.get("sample_count", 0) >= 300 and (item.get("expectancy") or 0) > 0
                and (item.get("profit_factor") or 0) >= 1.2 and (item.get("daily_profit_factor") or 0) >= 1.15
                for item in splits.values()
            )
            yearly_pass = len(yearly) == 7 and all((item["daily_profit_factor"] or 0) >= 1.0 for item in yearly)
            bootstrap_rows.append({
                "seed": seed,
                "split_metrics": splits,
                "all_years_daily_pf_pass": yearly_pass,
                "all_split_gate_pass": split_pass,
            })
        capacity["same_day_cap_three_randomized_stress_test"] = {
            "method": "100 deterministic random subsets of at most three candidates per execution day; this measures capacity robustness, not an executable name-ranking rule",
            "pass_count": int(sum(row["all_years_daily_pf_pass"] and row["all_split_gate_pass"] for row in bootstrap_rows)),
            "run_count": len(bootstrap_rows),
            "minimum_test_daily_pf": min(float(row["split_metrics"]["test"]["daily_profit_factor"] or 0) for row in bootstrap_rows),
            "median_test_daily_pf": float(pd.Series([row["split_metrics"]["test"]["daily_profit_factor"] for row in bootstrap_rows]).median()),
            "minimum_validation_daily_pf": min(float(row["split_metrics"]["validation"]["daily_profit_factor"] or 0) for row in bootstrap_rows),
            "median_validation_daily_pf": float(pd.Series([row["split_metrics"]["validation"]["daily_profit_factor"] for row in bootstrap_rows]).median()),
            "executable_selection_rule_status": "missing: no deterministic within-day tie-breaker is adopted",
        }
        tie_break_variants: list[dict[str, Any]] = []
        for feature, ascending in (
            ("tie_gap_ma60", False), ("tie_gap_ma60", True),
            ("tie_ma20_slope", False), ("tie_volume_ratio", True),
            ("tie_range10", True), ("tie_prior_high1_gap", False), ("tie_ret10", False),
        ):
            capped = _cap_three(selected, feature, ascending)
            splits, yearly = _metrics(capped, "next_open_return")
            train_metrics = splits.get("train", {})
            train_gate = bool(train_metrics.get("sample_count", 0) >= 300 and (train_metrics.get("expectancy") or 0) > 0 and (train_metrics.get("profit_factor") or 0) >= 1.2 and (train_metrics.get("daily_profit_factor") or 0) >= 1.15)
            tie_break_variants.append({
                "feature": feature, "ascending": ascending, "metrics_by_split": splits,
                "yearly_daily_basket_metrics": yearly, "train_gate_pass": train_gate,
            })
        train_tie_candidates = [row for row in tie_break_variants if row["train_gate_pass"]]
        chosen_tie_break = max(
            train_tie_candidates,
            key=lambda row: float(row["metrics_by_split"]["train"]["daily_profit_factor"] or float("-inf")),
            default=None,
        )
        if chosen_tie_break is not None:
            validation_test_pass = all(
                row.get("sample_count", 0) >= 300 and (row.get("expectancy") or 0) > 0
                and (row.get("profit_factor") or 0) >= 1.2 and (row.get("daily_profit_factor") or 0) >= 1.15
                for split, row in chosen_tie_break["metrics_by_split"].items() if split in {"validation", "test"}
            )
            annual_pass = len(chosen_tie_break["yearly_daily_basket_metrics"]) == 7 and all((row["daily_profit_factor"] or 0) >= 1.0 for row in chosen_tie_break["yearly_daily_basket_metrics"])
            capacity["train_only_secondary_tie_break"] = {
                "predeclared_feature_variants": [{"feature": row["feature"], "ascending": row["ascending"], "train_daily_profit_factor": row["metrics_by_split"].get("train", {}).get("daily_profit_factor")} for row in tie_break_variants],
                "selection_protocol": "choose the highest train daily PF among predeclared one-feature tie-breaks; validation and test were not used for selection",
                "selected_feature": chosen_tie_break["feature"], "ascending": chosen_tie_break["ascending"],
                "metrics_by_split": chosen_tie_break["metrics_by_split"],
                "yearly_daily_basket_metrics": chosen_tie_break["yearly_daily_basket_metrics"],
                "validation_test_gate_pass": validation_test_pass, "all_years_daily_pf_pass": annual_pass,
                "candidate_local_decision": "keep" if validation_test_pass and annual_pass else "drop",
            }
            priority_selected = _cap_three(selected, "tie_gap_ma60", False)
            portfolio_variants: list[dict[str, Any]] = []
            for maximum_positions in (5, 8, 10, 12, 15, 20):
                portfolio = _portfolio_cap(priority_selected, maximum_positions)
                splits, yearly = _metrics(portfolio, "next_open_return")
                train_metrics = splits.get("train", {})
                train_gate = bool(train_metrics.get("sample_count", 0) >= 300 and (train_metrics.get("expectancy") or 0) > 0 and (train_metrics.get("profit_factor") or 0) >= 1.2 and (train_metrics.get("daily_profit_factor") or 0) >= 1.15)
                portfolio_variants.append({
                    "maximum_positions": maximum_positions, "accepted_trade_count": int(len(portfolio)),
                    "skipped_trade_count": int(len(priority_selected) - len(portfolio)),
                    "metrics_by_split": splits, "yearly_daily_basket_metrics": yearly, "train_gate_pass": train_gate,
                    "all_split_sample_coverage_pass": all(item.get("sample_count", 0) >= 300 for item in splits.values()),
                })
            budget_cap5 = _portfolio_cap(priority_selected, 5).copy()
            budget_cap5.to_csv(output / "budget_10m_cap5_events.csv", index=False)
            exit_pnl = (
                budget_cap5.assign(pnl_yen=budget_cap5["next_open_return"] * 2_000_000.0)
                .groupby("exit_date", as_index=False)["pnl_yen"].sum()
                .sort_values("exit_date")
            )
            equity = 10_000_000.0 + exit_pnl["pnl_yen"].cumsum()
            drawdown = equity - equity.cummax()
            cap5_splits, cap5_yearly = _metrics(budget_cap5, "next_open_return")
            capacity["budget_10m_cap5_replay"] = {
                "starting_capital_yen": 10_000_000.0,
                "slot_notional_yen": 2_000_000.0,
                "maximum_positions": 5,
                "accepted_trade_count": int(len(budget_cap5)),
                "skipped_trade_count": int(len(priority_selected) - len(budget_cap5)),
                "net_pnl_yen": float((budget_cap5["next_open_return"] * 2_000_000.0).sum()),
                "ending_capital_yen": float(10_000_000.0 + (budget_cap5["next_open_return"] * 2_000_000.0).sum()),
                "max_realized_drawdown_yen": float(drawdown.min()) if len(drawdown) else 0.0,
                "metrics_by_split": cap5_splits,
                "yearly_daily_basket_metrics": cap5_yearly,
                "capital_reuse_contract": "fixed 2m notional per accepted position; realized PnL booked on simulated exit date; no leverage",
            }
            train_cap_candidates = [
                row for row in portfolio_variants
                if row["train_gate_pass"] and row["all_split_sample_coverage_pass"]
            ]
            selected_cap = min(train_cap_candidates, key=lambda row: row["maximum_positions"], default=None)
            if selected_cap is not None:
                validation_test_pass = all(
                    row.get("sample_count", 0) >= 300 and (row.get("expectancy") or 0) > 0
                    and (row.get("profit_factor") or 0) >= 1.2 and (row.get("daily_profit_factor") or 0) >= 1.15
                    for split, row in selected_cap["metrics_by_split"].items() if split in {"validation", "test"}
                )
                annual_pass = len(selected_cap["yearly_daily_basket_metrics"]) == 7 and all((row["daily_profit_factor"] or 0) >= 1.0 for row in selected_cap["yearly_daily_basket_metrics"])
                capacity["train_only_portfolio_cap"] = {
                    "selection_protocol": "choose the smallest predeclared cap with outcome-free >=300 sample coverage in every split and a passing train gate; validation and test returns were not used for selection",
                    "variants": portfolio_variants,
                    "selected_maximum_positions": selected_cap["maximum_positions"],
                    "validation_test_gate_pass": validation_test_pass,
                    "all_years_daily_pf_pass": annual_pass,
                    "candidate_local_decision": "keep" if validation_test_pass and annual_pass else "drop",
                    "replay_contract": "same-day entries use the fixed top-three priority first; a position occupies a slot through its simulated TP, SL, or 10th-session exit date; exits on an entry day are not reused intraday",
                }
    payload = {
        "schema_version": f"tradex_{AXIS_ID}.compare.v1",
        "authoritative_result": True,
        "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {
            "source_db": str(db), "source_filter": "pan", "confirmed_latest_date": runtime.get("latest_confirmed_daily_bars_date_iso"),
            "selection": "previously adopted train-only shallow_high_zone leaves 9,14,20; no selection rule changed",
            "changed_axis": "execution only: signal-day close versus next-session open",
            "take_profit": TP, "stop_loss": SL, "max_holding_days": H, "same_day_dual_hit": "stop first", "costs": "excluded", "runtime_db_write": False,
        },
        "signal_close_reference": {"metrics_by_split": close_splits, "yearly_daily_basket_metrics": close_yearly},
        "next_session_open": {"metrics_by_split": next_splits, "yearly_daily_basket_metrics": next_yearly, "split_gate_pass": gates},
        "next_session_open_gap_filter_variants": gap_variants,
        "train_only_selected_gap_execution": selected_gap_execution,
        "capacity_and_allocation": capacity,
        "authoritative_rollup_decision": (
            "adoptable_next_session_open"
            if adoptable
            else "adoptable_next_session_open_no_gap" if selected_gap_execution and selected_gap_execution["candidate_local_decision"] == "keep"
            else "hold_not_execution_validated"
        ),
        "production_ranking_changed": False,
        "runtime_db_write": False,
    }
    (output / "compare.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return output


if __name__ == "__main__":
    print(run())
