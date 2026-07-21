"""Evaluate a fixed late-core veto from probe close to core close."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd


YEARS = tuple(range(2019, 2026))
LATE_THRESHOLD_ATR = -1.0


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
        "median_probe_to_core_atr": None if frame.empty else float(frame.probe_to_core_atr.median()),
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

    feature = pd.read_parquet(args.features, columns=["code", "ymd", "c", "atr14"])
    feature["code"] = feature.code.astype(str).str.zfill(4)
    probe = feature.rename(columns={"ymd": "erasure_ymd", "c": "probe_close", "atr14": "probe_atr"})
    action = feature.rename(columns={"ymd": "action_ymd", "c": "core_close", "atr14": "core_atr"})
    joined = core.merge(
        probe[["code", "erasure_ymd", "probe_close", "probe_atr"]],
        on=["code", "erasure_ymd"], how="left", validate="many_to_one",
    ).merge(
        action[["code", "action_ymd", "core_close", "core_atr"]],
        on=["code", "action_ymd"], how="left", validate="many_to_one",
    )
    joined["probe_to_core_atr"] = (joined.core_close - joined.probe_close) / joined.probe_atr
    joined["late_core"] = joined.probe_to_core_atr.le(LATE_THRESHOLD_ATR)
    retained = joined[~joined.late_core].copy()
    vetoed = joined[joined.late_core].copy()

    baseline = {str(year): metrics(joined[joined.year.eq(year)]) for year in YEARS}
    challenger = {str(year): metrics(retained[retained.year.eq(year)]) for year in YEARS}
    vetoed_results = {str(year): metrics(vetoed[vetoed.year.eq(year)]) for year in YEARS}
    direction_pass = all(
        challenger[str(year)]["down_first"] > challenger[str(year)]["rebound_first"]
        for year in YEARS
    )
    sample_pass = all(challenger[str(year)]["n"] >= 30 for year in YEARS)
    improved_years = [
        year for year in YEARS if challenger[str(year)]["margin"] > baseline[str(year)]["margin"]
    ]
    decision = "keep" if direction_pass and sample_pass else "drop"
    data = {
        "schema_version": "tradex_probe_to_core_late_veto_oos_v1.compare.v1",
        "artifact_role": "authoritative",
        "axis": "probe-to-core close displacement",
        "fixed_conditions": {
            "probe": "ordered erasure close",
            "core": "weak-rebound MA20 rebreak close",
            "veto": "probe_to_core_atr <= -1.0",
            "atr_reference": "probe-day ATR14",
            "years": list(YEARS),
            "outcome": "fixed3 h5 at core close",
            "keep_gate": "down_first>rebound_first and n>=30 in every year",
            "threshold_sweep": False,
        },
        "baseline_results": baseline,
        "challenger_results": challenger,
        "vetoed_event_results": vetoed_results,
        "observed_branching": {
            "baseline_events": int(len(joined)),
            "retained_events": int(len(retained)),
            "vetoed_events": int(len(vetoed)),
            "improved_years": improved_years,
            "changed_rank_count": int(len(vetoed)),
            "selection_divergence_reason": "core entries more than one probe-day ATR below probe close are removed",
        },
        "judgment": {
            "decision": decision,
            "direction_pass_all_years": direction_pass,
            "sample_pass_all_years": sample_pass,
            "reason": "late-core veto passes every year" if decision == "keep" else "late-core veto remains directionally unstable across years",
        },
        "not_changed": [
            "probe detector", "core detector", "monthly environment", "candles", "MA states",
            "add logic", "profit logic", "MeeMee", "ranking", "runtime DB",
        ],
    }
    compare = args.output / "compare.json"
    compare.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    joined.to_parquet(args.output / "probe_to_core_late_veto_ledger.parquet", index=False)
    audit = {
        "source_sequences": int(len(seq)),
        "core_events": int(len(joined)),
        "duplicates_removed": duplicates_removed,
        "missing_probe_features": int(joined.probe_close.isna().sum()),
        "missing_core_features": int(joined.core_close.isna().sum()),
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
    print(json.dumps({"output": str(args.output), "challenger": challenger, "vetoed": vetoed_results, "judgment": data["judgment"], "audit": audit}, indent=2))


if __name__ == "__main__":
    main()
