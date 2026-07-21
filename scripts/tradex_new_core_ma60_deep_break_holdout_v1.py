"""Locked 2019-2022 holdout for the NEW_CORE fresh MA60 deep-break challenger."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd


YEARS = (2019, 2020, 2021, 2022)


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
    parser.add_argument("--support-ledger", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)

    seq = pd.read_parquet(args.sequence_ledger)
    weak = seq[
        seq.gd_ymd.notna()
        & seq.ma20_rebreak_ymd.notna()
        & (seq.ma20_rebreak_ymd > seq.gd_ymd)
        & seq.weak_rebound
        & (seq.max_consecutive_closes_above_ma7 < 7)
    ].copy()
    weak["action_ymd"] = weak.ma20_rebreak_ymd.astype(int)
    weak["year"] = weak.action_ymd.astype(str).str[:4].astype(int)
    weak["outcome"] = weak.ma20_rebreak_outcome_fixed3_h5
    weak["code"] = weak.code.astype(str).str.zfill(4)
    weak = weak[weak.year.isin(YEARS)]
    duplicate_events_removed = int(
        weak.duplicated(["code", "action_ymd", "outcome"]).sum()
    )
    weak = weak.drop_duplicates(["code", "action_ymd", "outcome"])

    support = pd.read_parquet(args.support_ledger)
    support["code"] = support.code.astype(str).str.zfill(4)
    ma60 = support[support.level_type.eq("MA60")][
        ["code", "ymd", "state", "distance_close_atr", "touch_count_prior20"]
    ]
    joined = weak.merge(
        ma60,
        left_on=["code", "action_ymd"],
        right_on=["code", "ymd"],
        how="left",
        validate="one_to_one",
    )
    selected = joined[
        joined.state.eq("DECISIVE_BREAK")
        & joined.distance_close_atr.le(-0.80)
    ].copy()

    baseline = {str(year): metrics(joined[joined.year.eq(year)]) for year in YEARS}
    challenger = {str(year): metrics(selected[selected.year.eq(year)]) for year in YEARS}
    direction_pass = all(
        challenger[str(year)]["down_first"] is not None
        and challenger[str(year)]["down_first"] > challenger[str(year)]["rebound_first"]
        for year in YEARS
    )
    sample_pass = all(challenger[str(year)]["n"] >= 20 for year in YEARS)
    decision = "keep" if direction_pass and sample_pass else "hold" if direction_pass else "drop"
    reason = (
        "locked challenger passes direction and sample gates in every untouched year"
        if decision == "keep"
        else "locked challenger is direction-stable but misses the sample gate"
        if decision == "hold"
        else "locked challenger reverses in at least one untouched year"
    )

    data = {
        "schema_version": "tradex_new_core_ma60_deep_break_holdout_v1.compare.v1",
        "artifact_role": "authoritative_holdout",
        "axis": "locked fresh MA60 decisive break deeper than 0.80ATR",
        "fixed_conditions": {
            "discovery_years_not_reused_for_holdout": [2023, 2024, 2025],
            "holdout_years": list(YEARS),
            "path_family": "WEAK_REBOUND_MA20_REBREAK_CORE",
            "path_contract": "GD before MA20 rebreak; weak rebound; max consecutive closes above MA7 < 7",
            "state": "MA60 DECISIVE_BREAK",
            "depth": "distance_close_atr <= -0.80",
            "outcome": "fixed3 h5 at core close",
            "keep_gate": "down_first>rebound_first and n>=20 in each holdout year",
            "threshold_sweep": False,
        },
        "baseline_results": baseline,
        "challenger_results": challenger,
        "observed_branching": {
            "baseline_events": int(len(joined)),
            "challenger_events": int(len(selected)),
            "changed_rank_count": 1,
            "selection_divergence_reason": "locked PIT MA60 state and close-depth gate",
        },
        "judgment": {
            "decision": decision,
            "direction_pass_all_holdout_years": direction_pass,
            "sample_pass_all_holdout_years": sample_pass,
            "reason": reason,
        },
        "not_changed": [
            "sequence thresholds", "monthly environment", "candle thresholds", "other MA states",
            "add logic", "profit logic", "MeeMee", "ranking", "runtime DB",
        ],
    }
    compare = args.output / "compare.json"
    compare.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    selected.to_parquet(args.output / "locked_challenger_events.parquet", index=False)
    audit = {
        "source_sequences": int(len(seq)),
        "baseline_holdout_events": int(len(joined)),
        "duplicate_events_removed": duplicate_events_removed,
        "missing_support_join": int(joined.state.isna().sum()),
        "duplicate_join_keys": int(joined.duplicated(["code", "action_ymd"]).sum()),
        "future_used_for_selection": False,
        "review_only": True,
        "sequence_sha256": sha(args.sequence_ledger),
        "support_sha256": sha(args.support_ledger),
    }
    (args.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(compare)}, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"output": str(args.output), "challenger": challenger, "judgment": data["judgment"], "audit": audit}, indent=2))


if __name__ == "__main__":
    main()
