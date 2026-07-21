"""Audit monthly-state gates inside BOX with close_pos<=0.20 fixed."""
import argparse, hashlib, json
from pathlib import Path

import pandas as pd


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def contrast(frame):
    roles = {}
    for role in ("D", "CONTROL"):
        values = frame.loc[frame.pair_role.eq(role), "hit"]
        roles[role] = {"n": int(len(values)), "hits": int(values.sum()), "prevalence": float(values.mean()) if len(values) else None}
    diff = roles["D"]["prevalence"] - roles["CONTROL"]["prevalence"] if all(roles[r]["n"] for r in roles) else None
    return {**roles, "D_minus_control": diff}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--matched", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    source = pd.read_parquet(args.matched)
    source = source.loc[source.base_regime.eq("BOX")].copy()
    source["monthly_state_norm"] = source.monthly_state.fillna("UNCLASSIFIED")
    source["hit"] = source.close_pos <= 0.20
    pivot = source.pivot(index="pair_id", columns="pair_role", values="monthly_state_norm")
    consistent_ids = pivot.index[pivot.D.eq(pivot.CONTROL)]
    data = source.loc[source.pair_id.isin(consistent_ids)].copy()

    states = {}
    for state, group in data.groupby("monthly_state_norm"):
        overall = contrast(group)
        years = {str(year): contrast(year_group) for year, year_group in group.groupby("year")}
        eligible = {year: row for year, row in years.items() if row["D"]["n"] >= 10 and row["CONTROL"]["n"] >= 10}
        positive = sum(row["D_minus_control"] > 0 for row in eligible.values())
        keep = overall["D"]["hits"] >= 20 and overall["D_minus_control"] >= 0.03 and len(eligible) >= 4 and positive >= 4
        states[str(state)] = {
            "overall": overall, "years": years, "eligible_year_count": len(eligible),
            "positive_eligible_years": positive, "decision": "keep" if keep else "drop",
        }

    kept = [name for name, row in states.items() if row["decision"] == "keep"]
    mismatch = pd.crosstab(pivot.D, pivot.CONTROL).to_dict()
    result = {
        "schema_version": "tradex_box_close_pos_monthly_state_gate_v1.compare.v1",
        "artifact_role": "authoritative_box_close_pos_monthly_state_review",
        "review_only": True,
        "research_phase": "effectiveness_judgment",
        "fixed_conditions": {
            "source_pairs": "frozen 771 D/non-D matched pairs; base_regime=BOX",
            "champion_fixed": "close_pos<=0.20",
            "axis_changed": "monthly_state only",
            "comparison_contract": "use only pairs with identical monthly_state; mismatched pairs excluded explicitly",
            "execution": "next_session_open; 5 sessions; target -3%; stop +3%; same-bar stop-first",
            "costs": "ignored", "weekly_inputs": [],
            "keep_contract": "D hits>=20, prevalence difference>=0.03, >=4 eligible years with >=10 pairs and >=4 positive",
        },
        "authoritative_result": {
            "BOX_pairs": int(source.pair_id.nunique()), "monthly_state_consistent_pairs": int(len(consistent_ids)),
            "monthly_state_mismatched_pairs": int(len(pivot) - len(consistent_ids)),
            "consistent_pairs_all_states": contrast(data), "states": states, "kept_states": kept,
            "year_2023_resolved": bool(kept and all(states[state]["years"].get("2023", {}).get("D_minus_control", 0) >= 0 for state in kept)),
        },
        "observed_branching": {
            "changed_top5_members_count": None, "changed_top10_members_count": None, "changed_rank_count": None,
            "selection_divergence_reason": "BOX and close_pos<=0.20 split only by PIT monthly_state",
            "mismatch_table": mismatch,
        },
        "judgment": {
            "candidate_local_decision": "keep" if kept else "drop",
            "session_aggregate_decision": "keep_review_only" if kept else "drop",
            "authoritative_rollup_decision": "keep_MATURE_BOX_UPPER_review_only_2023_unresolved" if kept == ["MATURE_BOX_UPPER"] else "hold_monthly_state_gate",
            "reason_type": "strong_matched_contrast_but_reverse_year_remains" if kept else "no_monthly_state_passed_fixed_contract",
        },
        "not_changed": ["base_regime", "close_pos threshold", "other features", "score weights", "MeeMee", "ranking", "runtime DB", "production logic"],
    }
    compare = args.output / "compare.json"
    compare.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {
        "source": {"path": str(args.matched.resolve()), "sha256": sha(args.matched)},
        "source_BOX_pairs": int(source.pair_id.nunique()), "analyzed_pairs": int(data.pair_id.nunique()),
        "excluded_monthly_state_mismatch_pairs": int(len(pivot) - len(consistent_ids)),
        "two_rows_per_pair": bool(data.groupby("pair_id").size().eq(2).all()),
        "monthly_state_consistent": bool(data.groupby("pair_id").monthly_state_norm.nunique().eq(1).all()),
        "weekly_columns_used": [], "future_columns_used": [], "compare_sha256": sha(compare),
    }
    (args.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(compare)}, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(args.output), "kept": kept, "year_2023_resolved": result["authoritative_result"]["year_2023_resolved"], "judgment": result["judgment"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
