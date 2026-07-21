"""Compare close-position thresholds on the frozen downside matched pairs."""
import argparse, hashlib, json
from pathlib import Path

import pandas as pd

THRESHOLDS = (0.20, 0.35, 0.50)


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def contrast(frame, mask):
    work = frame.assign(hit=mask)
    roles = {}
    for role in ("D", "CONTROL"):
        values = work.loc[work.pair_role.eq(role), "hit"]
        roles[role] = {"n": int(len(values)), "hits": int(values.sum()), "prevalence": float(values.mean())}
    diff = roles["D"]["prevalence"] - roles["CONTROL"]["prevalence"]
    return {**roles, "D_minus_control": diff}


def discordance(frame, mask):
    pairs = frame.assign(hit=mask).pivot(index="pair_id", columns="pair_role", values="hit")
    return {
        "D_only": int((pairs.D & ~pairs.CONTROL).sum()),
        "control_only": int((~pairs.D & pairs.CONTROL).sum()),
        "both": int((pairs.D & pairs.CONTROL).sum()),
        "neither": int((~pairs.D & ~pairs.CONTROL).sum()),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--matched", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    data = pd.read_parquet(args.matched)

    challengers = {}
    previous = None
    for threshold in THRESHOLDS:
        key = f"close_pos_le_{threshold:.2f}"
        overall = contrast(data, data.close_pos <= threshold)
        years = {str(year): contrast(group, group.close_pos <= threshold) for year, group in data.groupby("year")}
        positive_years = sum(row["D_minus_control"] > 0 for row in years.values())
        keep = overall["D"]["hits"] >= 20 and overall["D_minus_control"] >= 0.03 and positive_years >= 4
        band_mask = data.close_pos <= threshold if previous is None else (data.close_pos > previous) & (data.close_pos <= threshold)
        incremental = contrast(data, band_mask)
        challengers[key] = {
            "threshold": threshold,
            "overall": overall,
            "years": years,
            "positive_contrast_years": positive_years,
            "pair_discordance": discordance(data, data.close_pos <= threshold),
            "incremental_band": {"lower_exclusive": previous, "upper_inclusive": threshold, **incremental},
            "decision": "keep" if keep else "drop",
        }
        previous = threshold

    kept = [name for name, row in challengers.items() if row["decision"] == "keep"]
    decision = "keep_close_pos_le_0.20_review_only" if kept == ["close_pos_le_0.20"] else "hold_threshold_axis" if kept else "drop_close_pos_axis"
    result = {
        "schema_version": "tradex_failed_rebound_close_pos_axis_v1.compare.v1",
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
            "axis_changed": "close_pos threshold only",
            "challengers": list(THRESHOLDS),
            "keep_contract": "D hits >=20, D-minus-control prevalence >=0.03, positive contrast in >=4 of 6 years",
        },
        "authoritative_result": {"challengers": challengers, "kept_challengers": kept},
        "observed_branching": {
            "changed_top5_members_count": None,
            "changed_top10_members_count": None,
            "changed_rank_count": None,
            "selection_divergence_reason": "same matched events split only by close_pos threshold",
        },
        "judgment": {
            "candidate_local_decision": "keep" if kept else "drop",
            "session_aggregate_decision": "keep_review_only" if kept else "drop",
            "authoritative_rollup_decision": decision,
            "reason_type": "fixed_keep_contract_passed_with_year_instability_risk" if kept else "fixed_keep_contract_failed",
        },
        "not_changed": ["other feature conditions", "score weights", "MeeMee", "ranking", "runtime DB", "production logic"],
    }
    compare = args.output / "compare.json"
    compare.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {
        "source": {"path": str(args.matched.resolve()), "sha256": sha(args.matched)},
        "rows": int(len(data)), "pairs": int(data.pair_id.nunique()),
        "two_rows_per_pair": bool(data.groupby("pair_id").size().eq(2).all()),
        "roles_per_pair": bool(data.groupby("pair_id").pair_role.nunique().eq(2).all()),
        "weekly_columns_used": [], "future_columns_used": [], "compare_sha256": sha(compare),
    }
    (args.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(compare)}, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(args.output), "kept": kept, "judgment": result["judgment"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
