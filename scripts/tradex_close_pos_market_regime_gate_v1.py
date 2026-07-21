"""Audit base-regime gates for the frozen close_pos<=0.20 challenger."""
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
    data = pd.read_parquet(args.matched)
    data["hit"] = data.close_pos <= 0.20
    pair_regimes = data.groupby("pair_id").base_regime.nunique()
    if not pair_regimes.eq(1).all():
        raise RuntimeError("base_regime differs inside matched pair")

    regimes = {}
    for regime, group in data.groupby("base_regime"):
        overall = contrast(group)
        years = {str(year): contrast(year_group) for year, year_group in group.groupby("year")}
        eligible_years = {year: row for year, row in years.items() if row["D"]["n"] >= 10 and row["CONTROL"]["n"] >= 10}
        positive_years = sum(row["D_minus_control"] > 0 for row in eligible_years.values())
        keep = overall["D"]["hits"] >= 20 and overall["D_minus_control"] >= 0.03 and len(eligible_years) >= 4 and positive_years >= 4
        regimes[str(regime)] = {
            "overall": overall,
            "years": years,
            "eligible_year_count": len(eligible_years),
            "positive_eligible_years": positive_years,
            "decision": "keep" if keep else "drop",
        }

    kept = [name for name, row in regimes.items() if row["decision"] == "keep"]
    exclusions = {str(regime): contrast(data.loc[~data.base_regime.eq(regime)]) for regime in data.base_regime.unique()}
    reverse_years = {
        str(year): {str(regime): contrast(group) for regime, group in year_data.groupby("base_regime")}
        for year, year_data in data.loc[data.year.isin([2023, 2024])].groupby("year")
    }
    result = {
        "schema_version": "tradex_close_pos_market_regime_gate_v1.compare.v1",
        "artifact_role": "authoritative_close_pos_single_axis_regime_review",
        "review_only": True,
        "research_phase": "effectiveness_judgment",
        "fixed_conditions": {
            "source_pairs": "frozen 771 D/non-D matched pairs",
            "champion_fixed": "close_pos<=0.20",
            "axis_changed": "base_regime only",
            "execution": "next_session_open; 5 sessions; target -3%; stop +3%; same-bar stop-first",
            "costs": "ignored", "weekly_inputs": [],
            "keep_contract": "D hits>=20, prevalence difference>=0.03, >=4 eligible years with >=10 pairs and all 4 positive",
        },
        "authoritative_result": {
            "all_pairs": contrast(data), "regimes": regimes, "kept_regimes": kept,
            "exclusion_effects": exclusions, "reverse_year_contribution": reverse_years,
        },
        "observed_branching": {
            "changed_top5_members_count": None, "changed_top10_members_count": None, "changed_rank_count": None,
            "selection_divergence_reason": "close_pos<=0.20 split only by PIT base_regime",
            "eligible_regime_count": sum(row["overall"]["D"]["n"] >= 20 for row in regimes.values()),
        },
        "judgment": {
            "candidate_local_decision": "keep" if kept else "drop",
            "session_aggregate_decision": "keep_review_only" if kept else "drop",
            "authoritative_rollup_decision": "keep_BOX_gate_review_only" if kept == ["BOX"] else "hold_regime_gate",
            "reason_type": "BOX_passes_fixed_contract_but_2023_unresolved" if kept == ["BOX"] else "no_clean_regime_gate",
        },
        "not_changed": ["close_pos threshold", "monthly state", "other features", "score weights", "MeeMee", "ranking", "runtime DB", "production logic"],
    }
    compare = args.output / "compare.json"
    compare.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {
        "source": {"path": str(args.matched.resolve()), "sha256": sha(args.matched)},
        "rows": int(len(data)), "pairs": int(data.pair_id.nunique()), "pair_regime_consistent": bool(pair_regimes.eq(1).all()),
        "two_rows_per_pair": bool(data.groupby("pair_id").size().eq(2).all()), "weekly_columns_used": [], "future_columns_used": [],
        "compare_sha256": sha(compare),
    }
    (args.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(compare)}, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(args.output), "kept": kept, "judgment": result["judgment"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
