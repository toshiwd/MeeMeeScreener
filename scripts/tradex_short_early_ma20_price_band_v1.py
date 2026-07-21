"""Validate a nominal price band for the early MA20-turn short challenger."""
import argparse
import hashlib
import json
from pathlib import Path

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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=Path, required=True)
    ap.add_argument("--early-compare", type=Path, required=True)
    ap.add_argument("--parent-compare", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    a = ap.parse_args()
    a.output.mkdir(parents=True, exist_ok=False)

    data = pd.read_parquet(a.events)
    selected = data.loc[data.c.ge(900) & data.c.lt(5000)].copy()
    selected.to_parquet(a.output / "early_ma20_price_900_5000_events.parquet", index=False)
    baseline = metrics(data)
    candidate = metrics(selected)
    early_compare = json.loads(a.early_compare.read_text(encoding="utf-8"))
    anchor_rows = early_compare["authoritative_result"]["anchor_6996"]
    anchor = [
        {
            **row,
            "price_band_ok": 900 <= float(row["signal_close"]) < 5000,
            "review_state": (
                "LateChase" if row["timing_state"] == "LateChase"
                else "EarlyCrashReview" if 900 <= float(row["signal_close"]) < 5000
                else "PriceBlock"
            ),
        }
        for row in anchor_rows
        if int(row["signal_ymd"]) in {20260706, 20260707, 20260713}
    ]
    anchor_dates = {int(row["signal_ymd"]): row for row in anchor}
    recent_years = {
        year: row for year, row in candidate["years"].items() if int(year) >= 2024
    }
    checks = {
        "candidate_n_ge_500": candidate["n"] >= 500,
        "candidate_mean_gt_baseline": candidate["mean_return"] > baseline["mean_return"],
        "candidate_stop_rate_lt_baseline": candidate["stop_rate"] < baseline["stop_rate"],
        "all_2024plus_years_positive": all(row["mean_return"] > 0 for row in recent_years.values()),
        "6996_jul06_review_candidate": anchor_dates.get(20260706, {}).get("review_state") == "EarlyCrashReview",
        "6996_jul07_review_candidate": anchor_dates.get(20260707, {}).get("review_state") == "EarlyCrashReview",
        "6996_jul13_late_chase": anchor_dates.get(20260713, {}).get("timing_state") == "LateChase",
    }
    keep = all(checks.values())
    result = {
        "schema_version": "tradex_short_early_ma20_price_band_v1.compare.v1",
        "artifact_role": "authoritative_short_early_ma20_price_band",
        "review_only": True,
        "research_phase": "effectiveness_judgment",
        "fixed_conditions": {
            "parent_population": "Early impulse above flat/falling MA20",
            "axis_changed": "nominal signal close price only",
            "candidate_price_yen": "900<=close<5000",
            "execution": "next open; 5 sessions; target -3%; stop +3%; same-bar stop-first",
            "period": "2019-2026",
            "costs": "ignored",
            "weekly_inputs": [],
            "future_selection_columns": [],
        },
        "authoritative_result": {
            "baseline_ma20_candidate": baseline,
            "price_900_5000_candidate": candidate,
            "anchor_6996": anchor,
            "gate_checks": checks,
        },
        "observed_branching": {
            "changed_top5_members_count": None,
            "changed_top10_members_count": None,
            "changed_rank_count": int(len(data) - len(selected)),
            "selection_divergence_reason": "removes low-priced and high-priced branches with unstable recent outcomes",
            "candidate_count": int(len(selected)),
        },
        "judgment": {
            "candidate_local_decision": "keep" if keep else "hold",
            "session_aggregate_decision": "keep_price_900_5000" if keep else "hold_price_axis",
            "authoritative_rollup_decision": "keep_early_ma20_turn_price_900_5000_review_only" if keep else "hold",
            "reason_type": "anchor_and_2024plus_stability_gates_passed" if keep else "one_or_more_price_gates_failed",
        },
        "not_changed": ["event annotation", "MeeMee", "ranking", "runtime DB", "production logic"],
    }
    cp = a.output / "compare.json"
    cp.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {
        "sources": {
            "events": {"path": str(a.events.resolve()), "sha256": sha(a.events)},
            "early_compare": {"path": str(a.early_compare.resolve()), "sha256": sha(a.early_compare)},
            "parent_compare": {"path": str(a.parent_compare.resolve()), "sha256": sha(a.parent_compare)},
        },
        "events": int(len(selected)),
        "future_selection_columns": [],
        "weekly_columns_used": [],
        "ledger_sha256": sha(a.output / "early_ma20_price_900_5000_events.parquet"),
        "compare_sha256": sha(cp),
    }
    (a.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (a.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(cp)}, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"output": str(a.output), "candidate": candidate, "anchor": anchor, "checks": checks}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
