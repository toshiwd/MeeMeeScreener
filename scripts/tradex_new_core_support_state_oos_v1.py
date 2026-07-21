"""Measure each PIT support-role state for NEW_CORE events under fixed OOS years."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd


YEARS = (2023, 2024, 2025)
STATES = (
    "TOUCH_AND_HOLD",
    "BREAK_AND_RECLAIM",
    "UNTOUCHED_NEAR_SUPPORT",
    "DECISIVE_BREAK",
    "RETEST_FROM_BELOW",
    "BELOW_UNRESOLVED",
    "ABOVE_FAR",
)
LEVELS = ("MA60", "MA100", "MA200", "SUPPORT20")


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def rates(frame: pd.DataFrame) -> dict:
    return {
        "n": int(len(frame)),
        "codes": int(frame.code.nunique()),
        "down_first": None if frame.empty else float(frame.outcome.eq("down_first").mean()),
        "rebound_first": None if frame.empty else float(frame.outcome.eq("rebound_first").mean()),
        "neutral": None if frame.empty else float(frame.outcome.str.startswith("neutral").mean()),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--action-ledger", type=Path, required=True)
    parser.add_argument("--support-ledger", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)

    actions = pd.read_parquet(args.action_ledger)
    actions = actions[actions.action_type.eq("NEW_CORE")].copy()
    actions["code"] = actions.code.astype(str).str.zfill(4)
    actions["source_year"] = actions.year
    actions["year"] = actions.action_ymd.astype(str).str[:4].astype(int)
    actions = actions[actions.year.isin(YEARS)]
    duplicate_events_removed = int(
        actions.duplicated(["code", "action_ymd", "outcome", "path_family"]).sum()
    )
    cross_year_events_reassigned = int(actions.source_year.ne(actions.year).sum())
    actions = actions.drop_duplicates(
        ["code", "action_ymd", "outcome", "path_family"]
    )[["code", "year", "action_ymd", "outcome", "path_family"]]

    support = pd.read_parquet(args.support_ledger)
    support["code"] = support.code.astype(str).str.zfill(4)
    joined = actions.merge(
        support,
        left_on=["code", "action_ymd"],
        right_on=["code", "ymd"],
        how="left",
        validate="one_to_many",
    )

    results: dict[str, dict[str, dict]] = {}
    candidates = []
    for level in LEVELS:
        results[level] = {}
        for state in STATES:
            yearly = {}
            for year in YEARS:
                z = joined[
                    joined.year.eq(year)
                    & joined.level_type.eq(level)
                    & joined.state.eq(state)
                ]
                yearly[str(year)] = rates(z)
            valid = all(
                yearly[str(year)]["n"] >= 20
                and yearly[str(year)]["down_first"] > yearly[str(year)]["rebound_first"]
                for year in YEARS
            )
            margins = [
                yearly[str(year)]["down_first"] - yearly[str(year)]["rebound_first"]
                for year in YEARS
                if yearly[str(year)]["down_first"] is not None
            ]
            results[level][state] = {
                "years": yearly,
                "all_years_down_gt_rebound_min_n20": valid,
                "worst_year_margin": None if not margins else float(min(margins)),
            }
            if valid:
                candidates.append(
                    {
                        "level_type": level,
                        "state": state,
                        "worst_year_margin": float(min(margins)),
                        "min_year_n": min(yearly[str(year)]["n"] for year in YEARS),
                    }
                )
    candidates.sort(key=lambda row: (row["worst_year_margin"], row["min_year_n"]), reverse=True)

    data = {
        "schema_version": "tradex_new_core_support_state_oos_v1.compare.v1",
        "artifact_role": "authoritative_diagnostic",
        "axis": "NEW_CORE individual support state by level type",
        "fixed_conditions": {
            "action_type": "NEW_CORE",
            "path_family": "WEAK_REBOUND_MA20_REBREAK_CORE",
            "outcome": "fixed3 h5",
            "years": list(YEARS),
            "candidate_gate": "each year n>=20 and down_first>rebound_first",
            "threshold_selection": "none",
        },
        "results": results,
        "stable_candidates": candidates,
        "observed_branching": {
            "new_core_events": int(len(actions)),
            "joined_state_rows": int(len(joined)),
            "changed_rank_count": int(len(candidates)),
            "selection_divergence_reason": "support roles are separated by MA/support level instead of pooled",
        },
        "judgment": {
            "decision": "keep_candidate_for_next_oos" if candidates else "drop_axis",
            "reason": "stable individual states exist" if candidates else "no individual state passes every validation year",
        },
        "not_changed": [
            "sequence path",
            "monthly environment",
            "candlestick thresholds",
            "add logic",
            "profit logic",
            "MeeMee",
            "ranking",
            "runtime DB",
        ],
    }
    compare = args.output / "compare.json"
    compare.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    joined.to_parquet(args.output / "new_core_support_state_ledger.parquet", index=False)
    audit = {
        "new_core_events": int(len(actions)),
        "expected_join_rows": int(len(actions) * len(LEVELS)),
        "joined_rows": int(len(joined)),
        "missing_state_rows": int(joined.state.isna().sum()),
        "duplicate_level_rows": int(joined.duplicated(["code", "action_ymd", "level_type"]).sum()),
        "duplicate_source_events_removed": duplicate_events_removed,
        "cross_year_events_reassigned_to_action_year": cross_year_events_reassigned,
        "future_used_for_selection": False,
        "review_only": True,
        "action_sha256": sha(args.action_ledger),
        "support_sha256": sha(args.support_ledger),
    }
    (args.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(compare)}, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"output": str(args.output), "stable_candidates": candidates, "judgment": data["judgment"], "audit": audit}, indent=2))


if __name__ == "__main__":
    main()
