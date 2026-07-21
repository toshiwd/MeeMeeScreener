"""Select early breakdowns above a flat/falling MA20 and block rising-MA20 pullbacks."""
import argparse
import hashlib
import json
from pathlib import Path

import duckdb
import pandas as pd


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def metrics(frame):
    if frame.empty:
        return {"n": 0}
    ret = frame.return_fixed3_pct.astype(float)
    x = frame.assign(year=frame.signal_ymd // 10000)
    years = {str(year): {"n": int(len(rows)), "mean_return": float(rows.return_fixed3_pct.mean())}
             for year, rows in x.groupby("year")}
    return {
        "n": int(len(frame)),
        "codes": int(frame.code.nunique()),
        "mean_return": float(ret.mean()),
        "median_return": float(ret.median()),
        "win_rate": float((ret > 0).mean()),
        "target_rate": float(frame.exit_reason.isin(["target", "gap_target"]).mean()),
        "stop_rate": float(frame.exit_reason.isin(["stop_first", "gap_stop"]).mean()),
        "years": years,
    }


def current_anchor(db):
    con = duckdb.connect(str(db), read_only=True)
    rows = con.execute(
        """
        SELECT strftime(to_timestamp(date),'%Y%m%d')::integer ymd, c, source
        FROM daily_bars
        WHERE code='9381' AND strftime(to_timestamp(date),'%Y%m%d') <= '20260716'
        ORDER BY date
        """
    ).fetchdf()
    con.close()
    rows = rows.drop_duplicates("ymd", keep="last")
    rows["ma20"] = rows.c.rolling(20, min_periods=20).mean()
    rows["ma20_slope5_pct"] = (rows.ma20 / rows.ma20.shift(5) - 1) * 100
    row = rows.loc[rows.ymd.eq(20260716)].iloc[-1]
    above_rising = bool(row.c >= row.ma20 and row.ma20_slope5_pct > 0)
    return {
        "code": "9381",
        "signal_ymd": 20260716,
        "source": str(row.source),
        "data_status": "provisional" if str(row.source).lower() == "yahoo" else "confirmed",
        "close": float(row.c),
        "ma20": float(row.ma20),
        "ma20_slope5_pct": float(row.ma20_slope5_pct),
        "close_vs_ma20_pct": 100 * float(row.c / row.ma20 - 1),
        "state": "AboveRisingMA20" if above_rising else "Other",
        "decision": "TrendPullbackBlock" if above_rising else "not_blocked",
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=Path, required=True)
    ap.add_argument("--db", type=Path, required=True)
    ap.add_argument("--parent-compare", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    a = ap.parse_args()
    a.output.mkdir(parents=True, exist_ok=False)

    data = pd.read_parquet(a.events)
    early = data.loc[data.timing_state.eq("Early")].copy()
    candidate = early.loc[early.ma20_state.eq("AboveFlatFallingMA20")].copy()
    blocked_rising = early.loc[early.ma20_state.eq("AboveRisingMA20")].copy()
    below = early.loc[early.ma20_state.eq("BelowMA20")].copy()
    candidate.to_parquet(a.output / "early_above_flat_falling_ma20_events.parquet", index=False)

    baseline = metrics(early)
    candidate_metrics = metrics(candidate)
    rising_metrics = metrics(blocked_rising)
    below_metrics = metrics(below)
    anchor = current_anchor(a.db)
    recent_positive = sum(
        row["mean_return"] > 0
        for year, row in candidate_metrics["years"].items()
        if int(year) >= 2024
    )
    checks = {
        "9381_trend_pullback_block": anchor["decision"] == "TrendPullbackBlock",
        "candidate_n_ge_500": candidate_metrics["n"] >= 500,
        "candidate_mean_gt_zero": candidate_metrics["mean_return"] > 0,
        "candidate_mean_gt_baseline": candidate_metrics["mean_return"] > baseline["mean_return"],
        "candidate_stop_rate_lt_baseline": candidate_metrics["stop_rate"] < baseline["stop_rate"],
        "above_rising_mean_lt_candidate": rising_metrics["mean_return"] < candidate_metrics["mean_return"],
        "positive_2024plus_years_ge_2": recent_positive >= 2,
    }
    keep = all(checks.values())
    result = {
        "schema_version": "tradex_short_early_ma20_state_v1.compare.v1",
        "artifact_role": "authoritative_short_early_ma20_state",
        "review_only": True,
        "research_phase": "effectiveness_judgment",
        "fixed_conditions": {
            "parent_population": "Early episode impulse, age 0-3",
            "axis_changed": "MA20 position and 5-session slope only",
            "candidate": "close>=MA20 and MA20 slope5<=0",
            "trend_pullback_block": "close>=MA20 and MA20 slope5>0",
            "below_ma20": "diagnostic only",
            "execution": "next open; 5 sessions; target -3%; stop +3%; same-bar stop-first",
            "period": "2019-2026",
            "costs": "ignored",
            "weekly_inputs": [],
            "future_selection_columns": [],
        },
        "authoritative_result": {
            "baseline_early": baseline,
            "candidate_above_flat_falling_ma20": candidate_metrics,
            "blocked_above_rising_ma20": rising_metrics,
            "below_ma20_diagnostic": below_metrics,
            "anchor_9381": anchor,
            "gate_checks": checks,
        },
        "observed_branching": {
            "changed_top5_members_count": None,
            "changed_top10_members_count": None,
            "changed_rank_count": int(len(early) - len(candidate)),
            "selection_divergence_reason": "keeps first breakdown only when MA20 has stopped rising but price has not already broken below it",
            "candidate_count": int(len(candidate)),
            "trend_pullback_block_count": int(len(blocked_rising)),
        },
        "judgment": {
            "candidate_local_decision": "keep" if keep else "hold",
            "session_aggregate_decision": "keep_early_above_flat_falling_ma20" if keep else "hold",
            "authoritative_rollup_decision": "keep_early_ma20_turn_short_challenger_review_only" if keep else "hold",
            "reason_type": "anchor_and_fixed_outcome_gates_passed" if keep else "one_or_more_ma20_state_gates_failed",
        },
        "not_changed": ["event axis", "MeeMee", "ranking", "runtime DB", "production logic"],
        "remaining_risks": ["2024 mean remains negative", "9381 anchor uses Yahoo provisional 2026-07-16 bar"],
    }
    cp = a.output / "compare.json"
    cp.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {
        "sources": {
            "events": {"path": str(a.events.resolve()), "sha256": sha(a.events)},
            "db": {"path": str(a.db.resolve()), "read_only": True},
            "parent_compare": {"path": str(a.parent_compare.resolve()), "sha256": sha(a.parent_compare)},
        },
        "events": int(len(candidate)),
        "future_selection_columns": [],
        "weekly_columns_used": [],
        "ledger_sha256": sha(a.output / "early_above_flat_falling_ma20_events.parquet"),
        "compare_sha256": sha(cp),
    }
    (a.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (a.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(cp)}, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"output": str(a.output), "candidate": candidate_metrics, "rising": rising_metrics, "anchor": anchor, "checks": checks}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
