"""Connect raw profit-taking opportunities to existing short-position episodes."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd


YEARS = range(2019, 2027)


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def rates(z: pd.DataFrame) -> dict:
    valid = z[z.outcome_fixed3_h5.notna()]
    return {
        "n": int(len(valid)),
        "rebound_first": None if valid.empty else float(valid.outcome_fixed3_h5.eq("rebound_first").mean()),
        "further_down_first": None if valid.empty else float(valid.outcome_fixed3_h5.eq("further_down_first").mean()),
        "neutral": None if valid.empty else float(valid.outcome_fixed3_h5.str.startswith("neutral").mean()),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--opportunities", type=Path, required=True)
    ap.add_argument("--event-ledger", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    args = ap.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    opp = pd.read_parquet(args.opportunities)
    opp["code"] = opp.code.astype(str).str.zfill(4)
    ev = pd.read_parquet(args.event_ledger, columns=["code", "ymd", "position_stage", "position_family"])
    ev["code"] = ev.code.astype(str).str.zfill(4)
    ev = ev.sort_values(["code", "ymd"]).reset_index(drop=True)

    connected = []
    for reason in sorted(opp.exit_reason.unique()):
        signal = opp[opp.exit_reason.eq(reason)].copy()
        signal["raw_signal"] = True
        cols = ["code", "ymd", "raw_signal", "below_ma7_streak", "held_mas", "lower_wick_ratio", "close_pos", "outcome_fixed3_h5"]
        x = ev.merge(signal[cols], on=["code", "ymd"], how="left", validate="one_to_one")
        x["raw_signal"] = x.raw_signal.fillna(False).astype(bool)
        x["eligible_state"] = x.position_stage.ge(2)
        grp = x.groupby("code", sort=False)
        prior_same = grp.raw_signal.shift(1).fillna(False) & grp.eligible_state.shift(1).fillna(False)
        x["exit_event"] = x.raw_signal & x.eligible_state & (~prior_same)
        z = x[x.raw_signal].copy()
        z["exit_reason"] = reason
        connected.append(z)
    joined = pd.concat(connected, ignore_index=True)
    exits = joined[joined.exit_event].copy()
    exits["year"] = exits.ymd.astype(str).str[:4].astype(int)

    year_results = {}
    for reason in sorted(joined.exit_reason.unique()):
        year_results[reason] = {
            str(y): {
                "all_opportunities": rates(joined[(joined.exit_reason == reason) & joined.ymd.astype(str).str[:4].astype(int).eq(y)]),
                "executable_first_onsets": rates(exits[(exits.exit_reason == reason) & exits.year.eq(y)]),
            }
            for y in YEARS
        }
    family_results = {
        str(fam): rates(z) for fam, z in exits.groupby("position_family", dropna=False)
    }
    anchor_specs = {"9007": 20231004, "2802": 20240216}
    anchors = {}
    for code, ymd in anchor_specs.items():
        z = joined[(joined.code == code) & joined.ymd.eq(ymd)]
        anchors[code] = {
            "human_expected_existing_short": True,
            "rows": z.where(pd.notna(z), None).to_dict("records"),
            "machine_position_path_present": bool(z.position_stage.ge(2).any()),
            "opportunity_detected": not z.empty,
        }
    strict = True
    breadth = True
    for reason in year_results:
        for y in YEARS:
            c = year_results[reason][str(y)]["executable_first_onsets"]
            if c["n"] < 20:
                breadth = False
            if c["n"] and not (c["rebound_first"] > c["further_down_first"]):
                strict = False
    payload = {
        "schema_version": "tradex_profit_take_position_connector_v1.compare.v1",
        "artifact_role": "authoritative",
        "review_only": True,
        "axis": "connect position-independent exit opportunities to first onset while short stage>=2",
        "fixed_conditions": {
            "executable_position": "position_stage>=2 at close",
            "event_dedup": "first raw signal onset while stage>=2 per reason",
            "outcome": "inherited exact OHLC symmetric fixed 3 percent h5",
            "entry_lifecycle": "unchanged",
        },
        "year_results": year_results, "family_results": family_results,
        "human_anchors": anchors,
        "observed_branching": {
            "raw_opportunities": int(len(joined)), "executable_exit_events": int(len(exits)),
            "selection_divergence_reason": "only raw opportunities occurring inside a core-or-larger short position become executable",
            "changed_top5_members_count": None, "changed_top10_members_count": None, "changed_rank_count": None,
        },
        "judgment": {
            "decision": "keep" if strict and breadth else "hold",
            "strict_rebound_dominance_all_reason_years": strict,
            "breadth_pass": breadth,
            "both_human_opportunities_detected": all(v["opportunity_detected"] for v in anchors.values()),
            "both_human_position_paths_present": all(v["machine_position_path_present"] for v in anchors.values()),
            "reason": "connector can be kept only when exit rebound dominates further decline with adequate yearly breadth; missing human entry paths remain separate failures",
        },
        "not_changed": ["exit opportunity rules", "entry events", "position lifecycle", "monthly classifier", "MeeMee", "ranking", "runtime DB"],
    }
    compare = args.output / "compare.json"
    compare.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    exits.to_parquet(args.output / "executable_profit_take_ledger.parquet", index=False)
    audit = {"event_rows": int(len(ev)), "opportunity_rows": int(len(opp)), "joined_signal_rows": int(len(joined)), "exit_rows": int(len(exits)), "duplicate_exit": int(exits.duplicated(["code", "ymd", "exit_reason"]).sum()), "opportunity_sha256": sha(args.opportunities), "event_sha256": sha(args.event_ledger), "future_used_for_selection": False, "review_only": True}
    (args.output / "audit.json").write_text(json.dumps(audit, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json", "compare_sha256": sha(compare)}, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(args.output), "judgment": payload["judgment"], "branching": payload["observed_branching"], "anchors": anchors}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
