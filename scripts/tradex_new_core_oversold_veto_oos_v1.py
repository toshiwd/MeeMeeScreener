"""Evaluate the existing oversold-risk flag as a NEW_CORE veto, one fixed axis."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd


YEARS = tuple(range(2019, 2026))


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def metrics(frame: pd.DataFrame) -> dict:
    down = None if frame.empty else float(frame.outcome.eq("down_first").mean())
    rebound = None if frame.empty else float(frame.outcome.eq("rebound_first").mean())
    return {
        "n": int(len(frame)),
        "codes": int(frame.code.nunique()),
        "down_first": down,
        "rebound_first": rebound,
        "neutral": None if frame.empty else float(frame.outcome.str.startswith("neutral").mean()),
        "margin": None if down is None else down - rebound,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sequence-ledger", type=Path, required=True)
    parser.add_argument("--features", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)

    seq = pd.read_parquet(args.sequence_ledger)
    core = seq[
        seq.gd_ymd.notna()
        & seq.ma20_rebreak_ymd.notna()
        & (seq.ma20_rebreak_ymd > seq.gd_ymd)
        & seq.weak_rebound
        & (seq.max_consecutive_closes_above_ma7 < 7)
    ].copy()
    core["action_ymd"] = core.ma20_rebreak_ymd.astype(int)
    core["year"] = core.action_ymd.astype(str).str[:4].astype(int)
    core["outcome"] = core.ma20_rebreak_outcome_fixed3_h5
    core["code"] = core.code.astype(str).str.zfill(4)
    core = core[core.year.isin(YEARS)]
    duplicates_removed = int(core.duplicated(["code", "action_ymd", "outcome"]).sum())
    core = core.drop_duplicates(["code", "action_ymd", "outcome"])

    features = pd.read_parquet(
        args.features,
        columns=["code", "ymd", "oversold_risk", "ret5", "dist_ma7_atr", "dist_ma20_atr", "dist_ma60_atr"],
    )
    features["code"] = features.code.astype(str).str.zfill(4)
    joined = core.merge(
        features,
        left_on=["code", "action_ymd"],
        right_on=["code", "ymd"],
        how="left",
        validate="one_to_one",
    )
    joined["oversold_risk"] = joined.oversold_risk.astype("boolean")
    challenger = joined[joined.oversold_risk.eq(False)].copy()
    vetoed = joined[joined.oversold_risk.eq(True)].copy()

    baseline_results = {str(year): metrics(joined[joined.year.eq(year)]) for year in YEARS}
    challenger_results = {str(year): metrics(challenger[challenger.year.eq(year)]) for year in YEARS}
    vetoed_results = {str(year): metrics(vetoed[vetoed.year.eq(year)]) for year in YEARS}
    direction_pass = all(
        challenger_results[str(year)]["down_first"] > challenger_results[str(year)]["rebound_first"]
        for year in YEARS
    )
    sample_pass = all(challenger_results[str(year)]["n"] >= 30 for year in YEARS)
    improved_years = [
        year for year in YEARS
        if challenger_results[str(year)]["margin"] > baseline_results[str(year)]["margin"]
    ]
    decision = "keep" if direction_pass and sample_pass else "drop"
    data = {
        "schema_version": "tradex_new_core_oversold_veto_oos_v1.compare.v1",
        "artifact_role": "authoritative",
        "axis": "existing oversold_risk veto at NEW_CORE close",
        "fixed_conditions": {
            "path_family": "WEAK_REBOUND_MA20_REBREAK_CORE",
            "veto": "oversold_risk == true",
            "oversold_definition": "unchanged from daily assessment feature ledger",
            "years": list(YEARS),
            "outcome": "fixed3 h5 at core close",
            "keep_gate": "down_first>rebound_first and n>=30 in every year",
            "threshold_sweep": False,
        },
        "baseline_results": baseline_results,
        "challenger_results": challenger_results,
        "vetoed_event_results": vetoed_results,
        "observed_branching": {
            "baseline_events": int(len(joined)),
            "retained_events": int(len(challenger)),
            "vetoed_events": int(len(vetoed)),
            "improved_years": improved_years,
            "changed_rank_count": int(len(vetoed)),
            "selection_divergence_reason": "existing oversold flag removes late core entries",
        },
        "judgment": {
            "decision": decision,
            "direction_pass_all_years": direction_pass,
            "sample_pass_all_years": sample_pass,
            "reason": "veto passes every fixed year" if decision == "keep" else "veto does not make core down-first exceed rebound-first in every fixed year",
        },
        "not_changed": [
            "oversold definition", "sequence path", "monthly environment", "candle thresholds",
            "MA state", "add logic", "profit logic", "MeeMee", "ranking", "runtime DB",
        ],
    }
    compare = args.output / "compare.json"
    compare.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    joined.to_parquet(args.output / "new_core_oversold_veto_ledger.parquet", index=False)
    audit = {
        "source_sequences": int(len(seq)),
        "core_events": int(len(joined)),
        "duplicates_removed": duplicates_removed,
        "missing_feature_join": int(joined.oversold_risk.isna().sum()),
        "duplicate_join_keys": int(joined.duplicated(["code", "action_ymd"]).sum()),
        "future_used_for_selection": False,
        "review_only": True,
        "sequence_sha256": sha(args.sequence_ledger),
        "feature_sha256": sha(args.features),
    }
    (args.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(compare)}, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"output": str(args.output), "challenger": challenger_results, "vetoed": vetoed_results, "judgment": data["judgment"], "audit": audit}, indent=2))


if __name__ == "__main__":
    main()
