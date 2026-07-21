"""Validate 6-7 month range-bottom capitulation followed by next-session GU."""
import argparse, hashlib, json
from pathlib import Path
import duckdb
import pandas as pd


def sha(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def metrics(frame):
    if frame.empty: return {"n": 0}
    return {"n": int(len(frame)), "codes": int(frame.code.nunique()),
            "ret5_mean": float(frame.ret5_pct.mean()), "ret10_mean": float(frame.ret10_pct.mean()),
            "ret20_mean": float(frame.ret20_pct.mean()), "ret20_positive_rate": float((frame.ret20_pct > 0).mean()),
            "max_up20_mean": float(frame.max_up20_pct.mean()),
            "max_up20_ge_10_rate": float((frame.max_up20_pct >= 10).mean()),
            "max_down20_le_minus5_rate": float((frame.max_down20_pct <= -5).mean())}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=Path, required=True); ap.add_argument("--db", type=Path, required=True)
    ap.add_argument("--parent-compare", type=Path, required=True); ap.add_argument("--output", type=Path, required=True)
    a = ap.parse_args(); a.output.mkdir(parents=True, exist_ok=False)
    events = pd.read_parquet(a.events)
    con = duckdb.connect(str(a.db), read_only=True)
    prices = con.execute("select code,strftime(to_timestamp(date),'%Y%m%d')::integer ymd,c from daily_bars where code in (select unnest(?)) order by code,date",
                         [events.code.unique().tolist()]).fetchdf(); con.close()
    prices.code = prices.code.astype(str).str.zfill(4)
    prices["signal_ret1_pct"] = prices.groupby("code").c.pct_change() * 100
    events = events.merge(prices[["code", "ymd", "signal_ret1_pct"]], left_on=["code", "signal_ymd"], right_on=["code", "ymd"], validate="one_to_one")
    selected = events.loc[(events.signal_ret1_pct <= -2) & (events.next_open_gap_pct > 0)].copy()
    selected["age_group"] = pd.cut(selected.range_months, [2, 4, 5, 7, 11, 14],
                                   labels=["3_4", "5", "6_7", "8_11", "12_14"])
    selected.to_parquet(a.output / "monthly_range_age_capitulation_gu_events.parquet", index=False)
    groups = {name: metrics(selected.loc[selected.age_group.eq(name)]) for name in ["3_4", "5", "6_7", "8_11", "12_14"]}
    exact = {str(month): metrics(selected.loc[selected.range_months.eq(month)]) for month in range(3, 15)}
    years = {str(year): {name: metrics(rows.loc[rows.age_group.eq(name)]) for name in groups}
             for year, rows in selected.groupby("year")}
    target = groups["6_7"]; early = groups["3_4"]; stale = groups["12_14"]
    eligible_target_years = [row["6_7"] for row in years.values() if row["6_7"].get("n", 0) >= 10]
    checks = {"target_n_ge_80": target.get("n", 0) >= 80,
              "target_ret20_gt_early": target.get("ret20_mean", -999) > early.get("ret20_mean", 999),
              "target_ret20_gt_stale": target.get("ret20_mean", -999) > stale.get("ret20_mean", 999),
              "target_positive_rate_ge_0_60": target.get("ret20_positive_rate", 0) >= .60,
              "target_large_up_rate_gt_stale": target.get("max_up20_ge_10_rate", 0) > stale.get("max_up20_ge_10_rate", 1),
              "positive_eligible_years_ge_4": sum(row["ret20_mean"] > 0 for row in eligible_target_years) >= 4}
    keep = all(checks.values())
    result = {"schema_version": "tradex_monthly_range_age_capitulation_gu_v1.compare.v1",
              "artifact_role": "authoritative_monthly_range_age_capitulation_gu", "review_only": True,
              "research_fallback": True, "research_phase": "effectiveness_judgment",
              "fixed_conditions": {"parent_range": "monthly close cluster width<=15%, bottom position<=25%, first touch per box start",
                                   "axis_changed": "signal-day return<=-2% plus next-session GU confirmation",
                                   "target_age": "6-7 months", "comparison_ages": ["3-4", "5", "8-11", "12-14"],
                                   "execution": "next-session open after GU", "horizons": [5, 10, 20],
                                   "future_selection_columns": [], "weekly_inputs": []},
              "authoritative_result": {"all_selected": metrics(selected), "groups": groups, "exact_months": exact,
                                       "years": years, "eligible_target_years": len(eligible_target_years), "gate_checks": checks},
              "observed_branching": {"changed_top5_members_count": None, "changed_top10_members_count": None, "changed_rank_count": None,
                                     "selection_divergence_reason": "range-bottom events split by age after capitulation and GU confirmation",
                                     "group_counts": {name: row.get("n", 0) for name, row in groups.items()}},
              "judgment": {"candidate_local_decision": "keep" if keep else "hold",
                           "session_aggregate_decision": "keep_6_7_month_bottom_rebound" if keep else "hold_6_7_month_hypothesis",
                           "authoritative_rollup_decision": "keep_6_7_month_capitulation_gu_review_only" if keep else "hold",
                           "reason_type": "fixed_duration_trigger_gates_passed" if keep else "one_or_more_duration_trigger_gates_failed"},
              "not_changed": ["MeeMee box detector", "ranking", "runtime DB", "production logic"]}
    cp = a.output / "compare.json"; cp.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {"sources": {"events": {"path": str(a.events.resolve()), "sha256": sha(a.events)}, "db": {"path": str(a.db.resolve()), "read_only": True},
                         "parent_compare": {"path": str(a.parent_compare.resolve()), "sha256": sha(a.parent_compare)}},
             "selected_events": int(len(selected)), "future_selection_columns": [], "weekly_columns_used": [],
             "ledger_sha256": sha(a.output / "monthly_range_age_capitulation_gu_events.parquet"), "compare_sha256": sha(cp)}
    (a.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (a.output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(cp)}, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(a.output), "groups": groups, "gate_checks": checks, "judgment": result["judgment"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__": main()
