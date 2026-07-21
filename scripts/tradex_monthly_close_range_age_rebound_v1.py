"""Outcome study for monthly close-cluster age and lower-zone rebound."""
import argparse, hashlib, json
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

BANDS = [(3, 4, "3_4"), (5, 8, "5_8"), (9, 11, "9_11"), (12, 14, "12_plus")]


def sha(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def band_for(months):
    for low, high, name in BANDS:
        if low <= months <= high: return name
    return None


def metrics(frame):
    if frame.empty: return {"n": 0}
    result = {"n": int(len(frame)), "codes": int(frame.code.nunique()),
              "next_gu_rate": float((frame.next_open_gap_pct > 0).mean()),
              "next_gu_1pct_rate": float((frame.next_open_gap_pct >= 1).mean())}
    for horizon in (5, 10, 20):
        ret = frame[f"ret{horizon}_pct"]
        result[f"ret{horizon}_mean"] = float(ret.mean())
        result[f"ret{horizon}_median"] = float(ret.median())
        result[f"ret{horizon}_positive_rate"] = float((ret > 0).mean())
    result.update({"max_up20_mean": float(frame.max_up20_pct.mean()),
                   "max_up20_ge_5_rate": float((frame.max_up20_pct >= 5).mean()),
                   "max_up20_ge_10_rate": float((frame.max_up20_pct >= 10).mean()),
                   "max_down20_le_minus5_rate": float((frame.max_down20_pct <= -5).mean())})
    return result


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--universe-ledger", type=Path, required=True)
    ap.add_argument("--db", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    a = ap.parse_args(); a.output.mkdir(parents=True, exist_ok=False)
    universe = sorted(pd.read_parquet(a.universe_ledger, columns=["code"]).code.astype(str).str.zfill(4).unique())
    con = duckdb.connect(str(a.db), read_only=True)
    prices = con.execute(
        """select code,strftime(to_timestamp(date),'%Y%m%d')::integer ymd,o,h,l,c
           from daily_bars where code in (select unnest(?))
           and strftime(to_timestamp(date),'%Y%m%d') between '20160101' and '20260715'
           order by code,date""", [universe]).fetchdf()
    con.close(); prices.code = prices.code.astype(str).str.zfill(4)
    rows = []
    for code, group in prices.groupby("code"):
        group = group.reset_index(drop=True); group["month"] = group.ymd // 100
        finalized_months, finalized_closes = [], []
        current_month, current_close = None, None
        seen_keys = set()
        for idx, row in group.iterrows():
            month = int(row.month)
            if current_month is None:
                current_month = month
            elif month != current_month:
                finalized_months.append(current_month); finalized_closes.append(float(current_close))
                current_month = month
            current_close = float(row.c)
            if not (20190101 <= int(row.ymd) <= 20260630) or idx + 20 >= len(group):
                continue
            labels = finalized_months + [current_month]
            closes = finalized_closes + [current_close]
            selected = None
            for length in range(min(14, len(closes)), 2, -1):
                window = np.asarray(closes[-length:], dtype=float)
                lower, upper = float(window.min()), float(window.max())
                range_pct = (upper - lower) / max(abs(lower), 1e-9)
                if range_pct <= .15 and upper > lower:
                    selected = (length, labels[-length], labels[-1], lower, upper, range_pct)
                    break
            if selected is None:
                continue
            months, start_month, end_month, lower, upper, range_pct = selected
            position = (current_close - lower) / (upper - lower)
            if not (0 <= position <= .25):
                continue
            event_key = start_month
            if event_key in seen_keys:
                continue
            seen_keys.add(event_key)
            entry = float(group.loc[idx + 1, "o"])
            outcome = {"code": code, "signal_ymd": int(row.ymd), "entry_ymd": int(group.loc[idx + 1, "ymd"]),
                       "box_start_month": int(start_month), "box_end_month": int(end_month),
                       "range_months": int(months), "range_month_band": band_for(months),
                       "range_lower_close": lower, "range_upper_close": upper, "range_pct": range_pct,
                       "range_position": position, "signal_close": current_close, "entry_open": entry,
                       "next_open_gap_pct": 100 * (entry / current_close - 1), "year": int(row.ymd) // 10000}
            for horizon in (5, 10, 20):
                outcome[f"ret{horizon}_pct"] = 100 * (float(group.loc[idx + horizon, "c"]) / entry - 1)
            window20 = group.loc[idx + 1: idx + 20]
            outcome["max_up20_pct"] = 100 * (float(window20.h.max()) / entry - 1)
            outcome["max_down20_pct"] = 100 * (float(window20.l.min()) / entry - 1)
            rows.append(outcome)
    events = pd.DataFrame(rows)
    events.to_parquet(a.output / "monthly_close_range_bottom_events.parquet", index=False)
    by_band = {name: metrics(events.loc[events.range_month_band.eq(name)]) for _, _, name in BANDS}
    by_exact_month = {str(months): metrics(events.loc[events.range_months.eq(months)]) for months in range(3, 15)}
    by_year_band = {str(year): {name: metrics(year_rows.loc[year_rows.range_month_band.eq(name)]) for _, _, name in BANDS}
                    for year, year_rows in events.groupby("year")}
    target = by_band["5_8"]; stale = by_band["12_plus"]; early = by_band["3_4"]
    checks = {
        "target_n_ge_50": target.get("n", 0) >= 50,
        "target_ret20_positive": target.get("ret20_mean", -999) > 0,
        "target_max_up10_rate_gt_early": target.get("max_up20_ge_10_rate", 0) > early.get("max_up20_ge_10_rate", 0),
        "target_ret20_gt_stale": target.get("ret20_mean", -999) > stale.get("ret20_mean", 999),
        "target_large_up_rate_gt_stale": target.get("max_up20_ge_10_rate", 0) > stale.get("max_up20_ge_10_rate", 1),
    }
    keep = all(checks.values())
    result = {
        "schema_version": "tradex_monthly_close_range_age_rebound_v1.compare.v1",
        "artifact_role": "authoritative_monthly_close_range_age_rebound",
        "review_only": True, "research_fallback": True, "research_phase": "effectiveness_judgment",
        "fixed_conditions": {"universe": "codes in natural event ledger", "period": "2019-2026H1",
                             "range_definition": "longest 3-14 month close cluster including current partial month, width<=15%",
                             "bottom_definition": "current close position 0-25%; first event per box-start",
                             "execution": "next session open", "horizons": [5, 10, 20],
                             "bands": ["3_4", "5_8", "9_11", "12_plus"],
                             "future_inputs_used_for_selection": [], "weekly_inputs": [],
                             "fallback_reason": "existing monthlyBoxMonths does not detect user-observed 6857 five-month close cluster"},
        "authoritative_result": {"all_events": metrics(events), "bands": by_band, "exact_months": by_exact_month,
                                 "years": by_year_band, "gate_checks": checks},
        "observed_branching": {"changed_top5_members_count": None, "changed_top10_members_count": None, "changed_rank_count": None,
                               "selection_divergence_reason": "monthly close-range bottom events split only by range age band",
                               "band_counts": {name: row.get("n", 0) for name, row in by_band.items()}},
        "judgment": {"candidate_local_decision": "keep" if keep else "hold",
                     "session_aggregate_decision": "keep_5_8_month_rebound_hypothesis" if keep else "hold_range_age_hypothesis",
                     "authoritative_rollup_decision": "keep_5_8_month_bottom_rebound_review_only" if keep else "hold_no_clear_non_linear_duration_edge",
                     "reason_type": "fixed_non_linear_duration_gates_passed" if keep else "one_or_more_duration_gates_failed"},
        "not_changed": ["MeeMee box detector", "ranking", "runtime DB", "production logic"],
    }
    cp = a.output / "compare.json"; cp.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {"sources": {"universe_ledger": {"path": str(a.universe_ledger.resolve()), "sha256": sha(a.universe_ledger)},
                         "db": {"path": str(a.db.resolve()), "read_only": True}},
             "codes": len(universe), "events": int(len(events)), "unique_event_keys": int(events[["code", "box_start_month", "box_end_month"]].drop_duplicates().shape[0]),
             "future_selection_columns": [], "weekly_columns_used": [], "ledger_sha256": sha(a.output / "monthly_close_range_bottom_events.parquet"),
             "compare_sha256": sha(cp)}
    (a.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (a.output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(cp)}, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(a.output), "bands": by_band, "judgment": result["judgment"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__": main()
