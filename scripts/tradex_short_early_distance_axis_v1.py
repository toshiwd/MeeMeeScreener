"""Evaluate prior-80-day-high distance inside the kept early short population."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd


THRESHOLDS = (-0.08, -0.12, -0.18, -0.24, -0.30)


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def metrics(frame: pd.DataFrame) -> dict:
    if frame.empty:
        return {"n": 0}
    years = {}
    for year, rows in frame.assign(year=frame.signal_ymd // 10000).groupby("year"):
        years[str(int(year))] = {
            "n": int(len(rows)),
            "target_3pct_hit_rate": float(rows.target_3pct_hit.mean()),
            "fast_clean_target_3pct_hit_rate": float(rows.fast_clean_target_3pct_hit.mean()),
        }
    return {
        "n": int(len(frame)),
        "codes": int(frame.code.astype(str).nunique()),
        "target_3pct_hit_rate": float(frame.target_3pct_hit.mean()),
        "clean_target_3pct_hit_rate": float(frame.clean_target_3pct_hit.mean()),
        "fast_clean_target_3pct_hit_rate": float(frame.fast_clean_target_3pct_hit.mean()),
        "median_max_high_5d_pct": float(frame.max_high_5d_pct.median()),
        "median_min_low_5d_pct": float(frame.min_low_5d_pct.median()),
        "positive_years_fast_clean_gt_40": int(
            sum(row["fast_clean_target_3pct_hit_rate"] > 0.40 for row in years.values())
        ),
        "year_count": int(len(years)),
        "years": years,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    args = ap.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)

    data = pd.read_parquet(args.events)
    eligible = data.loc[data.dist_prior_80_high.notna()].copy()
    baseline = metrics(eligible)
    candidates = {}
    for threshold in THRESHOLDS:
        selected = eligible.loc[eligible.dist_prior_80_high.le(threshold)].copy()
        candidates[f"le_{abs(int(threshold * 100))}pct"] = metrics(selected)
    valid = [
        (name, row) for name, row in candidates.items()
        if row["n"] >= 50
        and row["fast_clean_target_3pct_hit_rate"] > baseline["fast_clean_target_3pct_hit_rate"]
        and row["target_3pct_hit_rate"] >= baseline["target_3pct_hit_rate"]
        and row["positive_years_fast_clean_gt_40"] >= 4
    ]
    winner_name = max(
        valid,
        key=lambda item: (
            item[1]["fast_clean_target_3pct_hit_rate"],
            item[1]["target_3pct_hit_rate"],
            item[1]["n"],
        ),
        default=(None, None),
    )[0]
    winner = None if winner_name is None else candidates[winner_name]
    result = {
        "schema_version": "tradex_short_early_distance_axis_v1.compare.v1",
        "artifact_role": "authoritative_early_distance_axis",
        "review_only": True,
        "research_phase": "effectiveness_judgment",
        "fixed_conditions": {
            "parent_population": (
                "episode Early age0-3; close>=MA20; MA20 slope5<=0; "
                "900<=close<5000"
            ),
            "axis_changed": "distance from prior 80-session high only",
            "thresholds": list(THRESHOLDS),
            "entry": "next session open",
            "primary_win": "3% decline within 5 sessions regardless of prior rebound",
            "timing_quality": "3% decline within 3 sessions before 3% adverse move",
            "period": "2019-2026",
            "costs": "ignored",
            "future_selection_columns": [],
        },
        "authoritative_result": {
            "eligible_baseline": baseline,
            "threshold_candidates": candidates,
            "winner_name": winner_name,
            "winner": winner,
            "gate_contract": {
                "n_ge": 50,
                "fast_clean_gt_baseline": True,
                "target3_not_worse_than_baseline": True,
                "positive_years_fast_clean_gt_40_ge": 4,
            },
        },
        "observed_branching": {
            "changed_top5_members_count": None,
            "changed_top10_members_count": None,
            "changed_rank_count": (
                None if winner is None else int(len(eligible) - winner["n"])
            ),
            "selection_divergence_reason": (
                "retains early candidates at or below a fixed distance from "
                "their prior 80-session high"
            ),
            "eligible_members": int(len(eligible)),
            "winner_members": None if winner is None else int(winner["n"]),
        },
        "judgment": {
            "candidate_local_decision": "keep" if winner is not None else "drop",
            "session_aggregate_decision": (
                f"keep_{winner_name}" if winner_name else "drop_distance_axis"
            ),
            "authoritative_rollup_decision": (
                "keep_early_distance_axis_v1_review_only"
                if winner_name else "drop_early_distance_axis"
            ),
            "reason_type": (
                "fixed_distance_candidate_passed_breadth_and_stability_gates"
                if winner_name else "no_distance_candidate_passed_fixed_gates"
            ),
        },
        "not_changed": [
            "pattern membership", "range gates", "volume gates",
            "MeeMee", "ranking", "runtime DB", "production logic",
        ],
        "remaining_risks": [
            "deep distance may represent a mature downtrend rather than a fresh decline",
            "minimum sample gate remains modest",
            "discretionary exits remain unsimulated",
        ],
    }
    compare = args.output / "compare.json"
    compare.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {
        "sources": {"events": {"path": str(args.events.resolve()), "sha256": sha(args.events)}},
        "compare_sha256": sha(compare),
    }
    (args.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(compare)}, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
