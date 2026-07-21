from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from tradex_long_fresh_family_events_v1 import FAMILIES, add_scores
from tradex_long_fresh_final_practical_v1 import prepare, routed, run, summary
from tradex_long_fresh_pullback_tail_guard_v1 import (
    DEVELOPMENT_RETENTION_QUANTILE, FEATURES, model,
)
from tradex_long_ordinary_pit_compound_tree_v1 import load_rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--events", type=Path, required=True)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--router-threshold", type=float, default=0.7)
    parser.add_argument("--sizing-threshold", type=float, default=0.7)
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=False)

    rows = load_rows(str(args.db), broad_trigger=False, min_date="2016-01-01")
    rows["signal_dt"] = pd.to_datetime(rows.date, unit="s")
    rows = add_scores(rows)
    pullback = FAMILIES[1]
    pullback_events = (rows.sort_values(["date", pullback, "code"], ascending=[True, False, True])
                       .groupby("date", sort=False).head(3).copy())
    matured = pullback_events[pullback_events.p1_o.notna() & pullback_events.p20_c.notna()].copy()
    matured["realized_ret"] = 100.0 * (matured.p20_c / matured.p1_o - 1.0) - 0.3
    matured["severe_loss"] = matured.realized_ret.le(-5).astype(int)
    matured["year"] = matured.signal_dt.dt.year

    development = matured[matured.year.between(2016, 2023)].copy()
    oof = pd.Series(index=development.index, dtype=float)
    for validation_year in range(2020, 2024):
        train = development[development.year < validation_year]
        valid = development[development.year == validation_year]
        fitted = model().fit(train[FEATURES], train.severe_loss)
        oof.loc[valid.index] = fitted.predict_proba(valid[FEATURES])[:, 1]
    threshold = float(oof.dropna().quantile(DEVELOPMENT_RETENTION_QUANTILE))
    fixed = model().fit(development[FEATURES], development.severe_loss)

    source = prepare(args.events)
    breakout = sorted(source.family.unique())[0]
    baseline_events = routed(source, breakout, args.router_threshold)
    missing_features = [name for name in FEATURES if name not in baseline_events.columns]
    score_rows = rows[["code", "date", *missing_features]].copy()
    scored = baseline_events.merge(score_rows, on=["code", "date"], how="left", validate="many_to_one")
    scored["tail_risk"] = fixed.predict_proba(scored[FEATURES])[:, 1]
    signal_year = pd.to_datetime(scored.date, unit="s").dt.year
    reject = (scored.family == pullback) & signal_year.ge(2024) & scored.tail_risk.gt(threshold)
    guarded_events = scored[~reject].copy()

    baseline_daily, baseline_ledger = run(baseline_events, args.db, args.sizing_threshold)
    guarded_daily, guarded_ledger = run(guarded_events, args.db, args.sizing_threshold)
    baseline = summary(baseline_daily, baseline_ledger)
    guarded = summary(guarded_daily, guarded_ledger)

    def delta(period_name: str) -> dict:
        bp, gp = baseline["periods"][period_name], guarded["periods"][period_name]
        bt, gt = baseline["trade_metrics"][period_name], guarded["trade_metrics"][period_name]
        return {
            "return_pct_points": gp["return_pct"] - bp["return_pct"],
            "max_drawdown_pct_points": gp["max_drawdown_pct"] - bp["max_drawdown_pct"],
            "trade_count": gt["trades"] - bt["trades"],
            "raw_mean_return_pct_points": gt["raw_mean_return_pct"] - bt["raw_mean_return_pct"],
            "raw_win_rate_points": gt["raw_win_rate"] - bt["raw_win_rate"],
            "raw_loss5_rate_points": gt["raw_loss5_rate"] - bt["raw_loss5_rate"],
        }

    deltas = {name: delta(name) for name in ["development", "validation_2024_2025", "audit_2026"]}
    checks = {
        "development_identical_by_design": deltas["development"]["return_pct_points"] == 0 and deltas["development"]["trade_count"] == 0,
        "threshold_fixed_without_2024plus": True,
        "validation_return_improves": deltas["validation_2024_2025"]["return_pct_points"] > 0,
        "validation_drawdown_not_worse": deltas["validation_2024_2025"]["max_drawdown_pct_points"] >= 0,
        "validation_raw_loss5_improves": deltas["validation_2024_2025"]["raw_loss5_rate_points"] < 0,
        "audit_return_improves": deltas["audit_2026"]["return_pct_points"] > 0,
        "audit_drawdown_not_worse": deltas["audit_2026"]["max_drawdown_pct_points"] >= 0,
        "audit_raw_loss5_improves": deltas["audit_2026"]["raw_loss5_rate_points"] < 0,
        "audit_keeps_at_least_75pct_portfolio_trades": guarded["trade_metrics"]["audit_2026"]["trades"] >= 0.75 * baseline["trade_metrics"]["audit_2026"]["trades"],
        "max_positions_at_most_20": all(guarded["periods"][name]["max_positions"] <= 20 for name in guarded["periods"]),
    }
    decision = "keep_review_only" if all(checks.values()) else "drop"
    payload = {
        "schema_version": "tradex_long_fresh_tail_guard_portfolio_v1.compare.v1",
        "artifact_role": "authoritative",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "fixed_evaluation_conditions": {
            "source": str(args.events), "runtime_db": str(args.db),
            "router_threshold": args.router_threshold, "market_sizing_threshold": args.sizing_threshold,
            "tail_guard_scope": "pullback family only, signal years 2024+",
            "tail_guard_training": "2016-2023 only",
            "tail_guard_threshold": threshold,
            "entry": "next session open", "exit": "session-20 close", "round_trip_cost_pct": 0.3,
            "max_positions": 20, "production_changed": False,
        },
        "authoritative_result": {
            "baseline": baseline, "guarded": guarded, "deltas": deltas,
            "source_events_after_router": int(len(baseline_events)),
            "rejected_pullback_events_2024plus": int(reject.sum()),
            "checks": checks,
        },
        "observed_branching": {
            "changed_top5_members_count": None, "changed_top10_members_count": None,
            "changed_rank_count": int(reject.sum()),
            "selection_divergence_reason": "fixed multi-feature tail-risk guard rejects high-risk pullback events only; router, sizing, other families, and ranks are fixed",
        },
        "judgment": {
            "candidate_local_decision": decision,
            "session_aggregate_decision": decision,
            "authoritative_rollup_decision": decision,
            "reason_type": "same_condition_compounded_portfolio_improvement",
        },
        "remaining_risks": [
            "The guard is deliberately inactive before 2024 because earlier point-in-time model versions were not frozen",
            "Current-candidate scoring must use a model refit only through the last fully completed year",
            "No MeeMee or production ranking reflection is authorized",
        ],
    }
    guarded_daily.to_parquet(output / "guarded_daily_nav.parquet", index=False)
    guarded_ledger.to_parquet(output / "guarded_trade_ledger.parquet", index=False)
    (output / "compare.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    (output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json"}), encoding="utf-8")
    print(json.dumps({"decision": decision, "rejected": int(reject.sum()), "deltas": deltas, "checks": checks}, ensure_ascii=False))


if __name__ == "__main__":
    main()
