"""One-axis filter for MA200 rejection episodes: isolated ordered long-MA resistance."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd


YEARS = (2023, 2024, 2025)


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def cell(z: pd.DataFrame) -> dict:
    core = z[z.core_ymd.notna()]
    add = z[z.add_ymd.notna()]
    return {
        "probes": int(len(z)), "codes": int(z.code.nunique()), "cores": int(len(core)), "adds": int(len(add)),
        "core_h5_down_first": None if core.empty else float(core.core_outcome_fixed3_h5.eq("down_first").mean()),
        "core_h5_rebound_first": None if core.empty else float(core.core_outcome_fixed3_h5.eq("rebound_first").mean()),
        "add_h5_down_first": None if add.empty else float(add.add_outcome_fixed3_h5.eq("down_first").mean()),
        "add_h5_rebound_first": None if add.empty else float(add.add_outcome_fixed3_h5.eq("rebound_first").mean()),
        "end_to_end_probe_core_down": None if z.empty else float(core.core_outcome_fixed3_h5.eq("down_first").sum()/len(z)),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--episodes", type=Path, required=True)
    ap.add_argument("--features", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    args = ap.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    ep = pd.read_parquet(args.episodes)
    ep["code"] = ep.code.astype(str).str.zfill(4)
    ft = pd.read_parquet(args.features)
    ft["code"] = ft.code.astype(str).str.zfill(4)
    cols = ["code", "ymd", "ma60", "ma100", "ma200", "atr14"]
    x = ep.merge(ft[cols], left_on=["code", "probe_ymd"], right_on=["code", "ymd"], how="left", validate="one_to_one")
    x["ma200_ma100_gap_atr"] = (x.ma200-x.ma100)/x.atr14
    x["ma100_ma60_gap_atr"] = (x.ma100-x.ma60)/x.atr14
    x["isolated_resistance"] = (
        (x.ma200 > x.ma100) & (x.ma100 > x.ma60)
        & (x.ma200_ma100_gap_atr >= .35)
        & (x.ma100_ma60_gap_atr >= .35)
    )
    ch = x[x.isolated_resistance].copy()
    baseline = {str(y): cell(x[x.year.eq(y)]) for y in YEARS}
    challenger = {str(y): cell(ch[ch.year.eq(y)]) for y in YEARS}
    breadth = all(challenger[str(y)]["cores"] >= 30 for y in YEARS)
    positive = breadth and all(challenger[str(y)]["core_h5_down_first"] > challenger[str(y)]["core_h5_rebound_first"] for y in YEARS)
    anchor = ch[(ch.code == "9107") & ch.probe_ymd.eq(20241121)].where(pd.notna(ch), None).to_dict("records")
    payload = {
        "schema_version": "tradex_box_ma200_isolated_resistance_oos_v1.compare.v1",
        "artifact_role": "authoritative",
        "axis": "isolated ordered MA200 resistance versus long-MA density",
        "fixed_conditions": {
            "base_family": "unchanged BOX MA200 rejection lifecycle",
            "single_filter": "MA200>MA100>MA60 and each adjacent gap >=0.35ATR at probe close",
            "threshold_sweep": False, "years": list(YEARS),
            "outcome": "inherited exact OHLC symmetric fixed 3 percent h5",
        },
        "baseline": baseline, "challenger": challenger,
        "human_anchor_9107": anchor,
        "observed_branching": {
            "removed_probe_count": int(len(x)-len(ch)),
            "kept_probe_count": int(len(ch)),
            "changed_top5_members_count": None, "changed_top10_members_count": None, "changed_rank_count": None,
            "selection_divergence_reason": "reject MA200 signals embedded in dense or unordered long-MA bands",
        },
        "judgment": {
            "decision": "keep" if positive and len(anchor) == 1 else "drop",
            "breadth_pass": breadth, "core_h5_down_exceeds_rebound_all_years": positive,
            "human_anchor_preserved": len(anchor) == 1,
            "reason": "keep requires the 9107 anchor, >=30 cores and down-first dominance in every OOS year",
        },
        "not_changed": ["MA200 rejection events", "probe/core/add dates", "monthly classifier", "MeeMee", "ranking", "runtime DB"],
    }
    compare = args.output / "compare.json"
    compare.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    ch.to_parquet(args.output / "filtered_episode_ledger.parquet", index=False)
    audit = {"base_episodes": int(len(x)), "filtered_episodes": int(len(ch)), "feature_missing": int(x.ma200.isna().sum()), "duplicate_episode": int(x.duplicated(["code", "probe_ymd"]).sum()), "episode_sha256": sha(args.episodes), "feature_sha256": sha(args.features), "future_used_for_selection": False, "review_only": True}
    (args.output / "audit.json").write_text(json.dumps(audit, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json", "compare_sha256": sha(compare)}, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(args.output), "challenger": challenger, "judgment": payload["judgment"], "branching": payload["observed_branching"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
