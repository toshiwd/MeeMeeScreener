"""Audit the MA7-rejection axis inside failed_rebound on frozen matched pairs."""
import argparse, hashlib, json
from pathlib import Path

import pandas as pd


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def prevalence(frame, column):
    result = {}
    for role in ("D", "CONTROL"):
        values = frame.loc[frame.pair_role.eq(role), column]
        result[role] = {"n": int(len(values)), "hits": int(values.sum()), "prevalence": float(values.mean())}
    result["D_minus_control"] = result["D"]["prevalence"] - result["CONTROL"]["prevalence"]
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--matched", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)

    data = pd.read_parquet(args.matched)
    data["ma7_rejection"] = (data.h >= data.ma7) & (data.c < data.ma7) & (data.upper_wick_ratio >= 0.20)
    data["alternate_failed_rebound"] = (data.ret3 > 0) & (data.c < data.ma20) & (data.close_pos <= 0.35)
    data["alternate_without_ma7"] = data.alternate_failed_rebound & ~data.ma7_rejection

    overall = prevalence(data, "ma7_rejection")
    yearly = {str(year): prevalence(group, "ma7_rejection") for year, group in data.groupby("year")}
    positive_years = sum(row["D_minus_control"] > 0 for row in yearly.values())
    pair_table = data.pivot(index="pair_id", columns="pair_role", values="ma7_rejection")
    discordance = {
        "D_only": int((pair_table.D & ~pair_table.CONTROL).sum()),
        "control_only": int((~pair_table.D & pair_table.CONTROL).sum()),
        "both": int((pair_table.D & pair_table.CONTROL).sum()),
        "neither": int((~pair_table.D & ~pair_table.CONTROL).sum()),
    }
    keep = overall["D"]["hits"] >= 20 and overall["D_minus_control"] >= 0.03 and positive_years >= 4
    result = {
        "schema_version": "tradex_failed_rebound_ma7_axis_v1.compare.v1",
        "artifact_role": "authoritative_failed_rebound_single_axis_review",
        "review_only": True,
        "research_phase": "effectiveness_judgment",
        "fixed_conditions": {
            "source_pairs": "frozen 771 D/non-D matched pairs",
            "execution": "next_session_open",
            "horizon_sessions": 5,
            "barriers": "short target -3%, stop +3%, same-bar stop-first",
            "costs": "ignored",
            "weekly_inputs": [],
            "axis_changed": "MA7 rejection only",
            "definition": "high >= MA7 and close < MA7 and upper_wick_ratio >= 0.20",
            "keep_contract": "D hits >=20, D-minus-control prevalence >=0.03, positive contrast in >=4 of 6 years",
        },
        "authoritative_result": {
            "overall": overall,
            "years": yearly,
            "positive_contrast_years": positive_years,
            "pair_discordance": discordance,
            "alternate_without_ma7": prevalence(data, "alternate_without_ma7"),
        },
        "observed_branching": {
            "changed_top5_members_count": None,
            "changed_top10_members_count": None,
            "changed_rank_count": None,
            "selection_divergence_reason": "failed_rebound split by MA7 rejection evidence",
            "D_axis_hits": overall["D"]["hits"],
            "control_axis_hits": overall["CONTROL"]["hits"],
        },
        "judgment": {
            "candidate_local_decision": "keep" if keep else "drop",
            "session_aggregate_decision": "keep" if keep else "drop",
            "authoritative_rollup_decision": "keep_for_next_validation" if keep else "drop_ma7_single_axis",
            "reason_type": "fixed_keep_contract_passed" if keep else "insufficient_matched_control_contrast_or_year_stability",
        },
        "not_changed": ["other family definitions", "score weights", "MeeMee", "ranking", "runtime DB", "production logic"],
    }
    compare = args.output / "compare.json"
    compare.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {
        "source": {"path": str(args.matched.resolve()), "sha256": sha(args.matched)},
        "rows": int(len(data)),
        "pairs": int(data.pair_id.nunique()),
        "two_rows_per_pair": bool(data.groupby("pair_id").size().eq(2).all()),
        "roles_per_pair": bool(data.groupby("pair_id").pair_role.nunique().eq(2).all()),
        "weekly_columns_used": [],
        "future_columns_used": [],
        "compare_sha256": sha(compare),
    }
    (args.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(compare)}, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(args.output), "result": result["authoritative_result"], "judgment": result["judgment"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
