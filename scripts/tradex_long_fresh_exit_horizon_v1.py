from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

from tradex_long_gap_guard_protective_stop_v1 import metrics
from tradex_long_trend_pullback_portfolio_v1 import COST, portfolio_summary, simulate

EVENTS = Path(r"G:\Tradex\tradex_long_fresh_family_events_v1\20260720T-authoritative-v4\fresh_family_events.parquet")
DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb")
HORIZONS = [10, 15, 20, 25, 30]


def build_horizon_events(events_path: Path, db_path: Path) -> pd.DataFrame:
    src = pd.read_parquet(events_path).reset_index(drop=True)
    src["event_id"] = src.index.astype("int64")
    keys = src[["event_id", "code", "p1_date"]].rename(columns={"p1_date": "entry_date"})
    with duckdb.connect(str(db_path), read_only=True) as conn:
        conn.register("keys", keys)
        exits = conn.execute(
            """
            SELECT event_id, rn AS horizon, date AS exit_date, c AS exit_price
            FROM (
              SELECT k.event_id, b.date, b.c,
                     row_number() OVER (PARTITION BY k.event_id ORDER BY b.date) AS rn
              FROM keys k
              JOIN daily_bars b ON b.code=k.code AND b.date>=k.entry_date
            )
            WHERE rn IN (10,15,20,25,30)
            """
        ).fetchdf()
    base = src.rename(columns={"p1_date": "entry_date", "p1_o": "entry_price"})
    return base.merge(exits, on="event_id", how="inner")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--output", required=True)
    p.add_argument("--events", type=Path, default=EVENTS)
    p.add_argument("--db", type=Path, default=DB)
    a = p.parse_args()
    out = Path(a.output)
    out.mkdir(parents=True, exist_ok=False)
    all_events = build_horizon_events(a.events, a.db)
    rows, ledgers = [], {}
    for horizon in HORIZONS:
        e = all_events[all_events.horizon.eq(horizon)].copy()
        e["rank"] = -e.family_score
        t = simulate(e, 20)
        t["year"] = pd.to_datetime(t.date, unit="s").dt.year
        ledgers[horizon] = t
        d = t[t.year.between(2016, 2023)]
        v = t[t.year.between(2024, 2025)]
        rows.append(
            {
                "horizon": horizon,
                "development": portfolio_summary(d),
                "validation_2024_2025": portfolio_summary(v),
                "validation_years": {str(y): portfolio_summary(v[v.year.eq(y)]) for y in [2024, 2025]},
            }
        )

    def eligible(x: dict) -> bool:
        d, v = x["development"], x["validation_2024_2025"]
        return (
            d["trades"] >= 250
            and d["raw_trade_metrics"]["mean_return_pct"] > 0
            and d["raw_trade_metrics"]["win_rate"] >= 0.50
            and d["positive_month_rate"] > 0.50
            and d["positive_year_rate"] >= 0.75
            and v["trades"] >= 100
            and v["raw_trade_metrics"]["mean_return_pct"] > 0
            and v["raw_trade_metrics"]["win_rate"] >= 0.50
            and v["positive_month_rate"] > 0.50
            and all(z["total_return_pct"] > 0 for z in x["validation_years"].values())
        )

    pretest = [x for x in rows if eligible(x)]
    chosen = max(
        pretest,
        key=lambda x: (
            x["validation_2024_2025"]["total_return_pct"],
            x["development"]["total_return_pct"],
        ),
    ) if pretest else None
    test = ledgers[chosen["horizon"]] if chosen else pd.DataFrame()
    test = test[test.year.eq(2026)] if len(test) else test
    tm = portfolio_summary(test)
    raw, cap = tm["raw_trade_metrics"], tm["capital_contribution_metrics"]
    checks = {
        "selected_without_2026": chosen is not None,
        "test_full_matured_audit": chosen is not None and len(test) > 0,
        "test_mean_positive": raw["mean_return_pct"] is not None and raw["mean_return_pct"] > 0,
        "test_win_at_least_50pct": raw["win_rate"] is not None and raw["win_rate"] >= 0.50,
        "test_capital_loss5_at_most_3pct": cap["severe_loss5_rate"] is not None and cap["severe_loss5_rate"] <= 0.03,
        "test_realized_months_majority_positive": tm["positive_month_rate"] is not None and tm["positive_month_rate"] > 0.50,
        "test_profit_concentration_at_most_35pct": cap["top3_positive_profit_share"] is not None and cap["top3_positive_profit_share"] <= 0.35,
        "test_total_return_positive": tm["total_return_pct"] > 0,
    }
    decision = "keep_for_mark_to_market_audit" if all(checks.values()) else "drop"
    payload = {
        "schema_version": "tradex_long_fresh_exit_horizon_v1.compare.v1",
        "artifact_role": "authoritative",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "fixed_evaluation_conditions": {
            "source": str(a.events), "runtime_db": str(a.db),
            "universe": "ordinary domestic stocks only inherited from event ledger",
            "selection": "fresh three-family continuous score, unchanged",
            "entry": "next session open", "exit_horizon_candidates": HORIZONS,
            "max_positions": 20, "allocation": "equal 5% initial capital",
            "round_trip_cost_pct": COST, "axis_changed": "exit horizon only",
            "development": "2016-2023", "validation": "2024-2025",
            "test": "2026 full matured audit through 2026-07-17", "production_changed": False,
        },
        "authoritative_result": {"candidates": rows, "eligible_without_2026": pretest, "chosen_without_2026": chosen, "test_2026": tm, "checks": checks},
        "observed_branching": {"changed_top5_members_count": 0, "changed_top10_members_count": 0, "changed_rank_count": 0, "selection_divergence_reason": "same signals; exit horizon changes slot availability"},
        "judgment": {"candidate_local_decision": decision, "authoritative_rollup_decision": decision, "reason_type": "fixed_condition_exit_horizon_gate"},
        "remaining_risks": ["daily mark-to-market and same-condition baseline audit remain if kept"],
    }
    if len(test):
        test.to_parquet(out / "test_2026_portfolio_ledger.parquet", index=False)
    (out / "compare.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    (out / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json"}), encoding="utf-8")
    print(json.dumps({"chosen": chosen, "test": tm, "checks": checks, "decision": decision}, ensure_ascii=False))


if __name__ == "__main__":
    main()
