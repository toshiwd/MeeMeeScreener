"""Evaluate one monotonic feature threshold inside an early short population."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd


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
    ap.add_argument("--feature", required=True)
    ap.add_argument("--direction", choices=("ge", "le"), required=True)
    ap.add_argument("--thresholds", nargs="+", type=float, required=True)
    ap.add_argument("--axis-id", required=True)
    ap.add_argument("--output", type=Path, required=True)
    args = ap.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)

    data = pd.read_parquet(args.events)
    if args.feature not in data:
        raise KeyError(args.feature)
    eligible = data.loc[data[args.feature].notna()].copy()
    baseline = metrics(eligible)
    candidates = {}
    for threshold in args.thresholds:
        mask = (
            eligible[args.feature].ge(threshold)
            if args.direction == "ge"
            else eligible[args.feature].le(threshold)
        )
        candidates[f"{args.direction}_{threshold:g}"] = metrics(eligible.loc[mask])
    valid = [
        (name, row) for name, row in candidates.items()
        if row["n"] >= 50
        and row["fast_clean_target_3pct_hit_rate"] > baseline["fast_clean_target_3pct_hit_rate"]
        and row["target_3pct_hit_rate"] >= baseline["target_3pct_hit_rate"]
        and row["positive_years_fast_clean_gt_40"] >= 4
    ]
    winner_name, winner = max(
        valid,
        key=lambda item: (
            item[1]["fast_clean_target_3pct_hit_rate"],
            item[1]["target_3pct_hit_rate"],
            item[1]["n"],
        ),
        default=(None, None),
    )
    keep = winner is not None
    result = {
        "schema_version": "tradex_short_early_feature_threshold_axis_v1.compare.v1",
        "axis_id": args.axis_id,
        "artifact_role": "authoritative_early_feature_threshold_axis",
        "review_only": True,
        "research_phase": "effectiveness_judgment",
        "fixed_conditions": {
            "parent_population": "kept early short population",
            "axis_changed": args.feature,
            "direction": args.direction,
            "thresholds": args.thresholds,
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
        },
        "observed_branching": {
            "changed_top5_members_count": None,
            "changed_top10_members_count": None,
            "changed_rank_count": None if winner is None else int(len(eligible) - winner["n"]),
            "selection_divergence_reason": f"fixed {args.direction} thresholds on {args.feature}",
            "eligible_members": int(len(eligible)),
            "winner_members": None if winner is None else int(winner["n"]),
        },
        "judgment": {
            "candidate_local_decision": "keep" if keep else "drop",
            "session_aggregate_decision": (
                f"keep_{args.axis_id}_{winner_name}" if keep else f"drop_{args.axis_id}"
            ),
            "authoritative_rollup_decision": (
                f"keep_{args.axis_id}_review_only" if keep else f"drop_{args.axis_id}"
            ),
            "reason_type": (
                "fixed_feature_candidate_passed_breadth_and_stability_gates"
                if keep else "no_feature_candidate_passed_fixed_gates"
            ),
        },
        "not_changed": ["other features", "MeeMee", "ranking", "runtime DB", "production logic"],
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
