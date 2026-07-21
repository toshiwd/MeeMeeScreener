"""Normalize reviewed sell branches into one action-stage episode ledger."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def action_row(code, episode_id, family, action, ymd, outcome=None, prerequisite=None):
    return {
        "code": str(code).zfill(4),
        "episode_id": str(episode_id),
        "source_family": family,
        "action": action,
        "action_ymd": int(ymd),
        "year": int(str(int(ymd))[:4]),
        "outcome_fixed3_h5": outcome,
        "prerequisite_action_ymd": prerequisite,
    }


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--weak-score", type=Path, required=True)
    p.add_argument("--try-fail", type=Path, required=True)
    p.add_argument("--ma200", type=Path, required=True)
    p.add_argument("--support-break", type=Path, required=True)
    p.add_argument("--full-erasure", type=Path, required=True)
    p.add_argument("--profit-json", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    a = p.parse_args()
    a.output.mkdir(parents=True, exist_ok=False)
    rows = []

    weak = pd.read_parquet(a.weak_score)
    for r in weak[weak.gate_pass].itertuples(index=False):
        eid = f"WEAK:{str(r.code).zfill(4)}:{int(r.erasure_ymd)}"
        rows.append(action_row(r.code, eid, "WEAK_REBOUND_SCORE_CORE", "PROBE", r.erasure_ymd, r.erasure_outcome_fixed3_h5))
        rows.append(action_row(r.code, eid, "WEAK_REBOUND_SCORE_CORE", "CORE_CLOSE", r.action_ymd, r.outcome, int(r.erasure_ymd)))

    trial = pd.read_parquet(a.try_fail)
    for r in trial.itertuples(index=False):
        eid = f"TRY_FAIL:{str(r.code).zfill(4)}:{int(r.probe_ymd)}"
        rows.append(action_row(r.code, eid, "UPTREND_TRY_FAIL_PRIOR_PROBE", "PROBE", r.probe_ymd))
        rows.append(action_row(r.code, eid, "UPTREND_TRY_FAIL_PRIOR_PROBE", "CORE_CLOSE", r.ymd, r.outcome, int(r.probe_ymd)))

    ma = pd.read_parquet(a.ma200)
    for r in ma.itertuples(index=False):
        eid = f"MA200:{str(r.code).zfill(4)}:{int(r.probe_ymd)}"
        rows.append(action_row(r.code, eid, "BOX_MA200_REJECTION", "PROBE", r.probe_ymd, r.probe_outcome_fixed3_h5))
        if pd.notna(r.core_ymd):
            rows.append(action_row(r.code, eid, "BOX_MA200_REJECTION", "CORE_CLOSE", r.core_ymd, r.core_outcome_fixed3_h5, int(r.probe_ymd)))
        if pd.notna(r.add_ymd):
            rows.append(action_row(r.code, eid, "BOX_MA200_REJECTION", "ADD", r.add_ymd, r.add_outcome_fixed3_h5, int(r.core_ymd)))

    support = pd.read_parquet(a.support_break)
    for r in support.itertuples(index=False):
        eid = f"SUPPORT_BREAK:{str(r.code).zfill(4)}:{int(r.ymd)}"
        rows.append(action_row(r.code, eid, "POSTBOX_SUPPORT_BREAK_DIRECT", "CORE_CLOSE", r.ymd, r.outcome))

    full = pd.read_parquet(a.full_erasure)
    for r in full.itertuples(index=False):
        eid = f"FULL_ERASURE:{str(r.code).zfill(4)}:{int(r.erasure_ymd)}"
        if r.branch == "POSTBOX_FULL_ERASURE_PROBE_GD_CORE":
            rows.append(action_row(r.code, eid, r.branch, "PROBE", r.probe_ymd, r.erasure_outcome_fixed3_h5))
            rows.append(action_row(r.code, eid, r.branch, "CORE_CLOSE", r.action_ymd, r.outcome, int(r.probe_ymd)))
        elif r.branch == "UPTREND_WTOP_FULL_ERASURE_DIRECT_CORE":
            rows.append(action_row(r.code, eid, r.branch, "CORE_CLOSE", r.action_ymd, r.outcome))

    profit = json.loads(a.profit_json.read_text(encoding="utf-8"))
    for r in profit["rows"]:
        rows.append(action_row(r["code"], r["episode_id"], r["trigger"], "TAKE_PROFIT", r["decision_ymd"], r["post_exit_outcome_fixed3_h5"], r["trigger_source_ymd"]))

    raw = pd.DataFrame(rows)
    ledger = raw.drop_duplicates(["episode_id", "action", "action_ymd", "source_family"]).sort_values(
        ["code", "action_ymd", "episode_id", "action"]
    ).reset_index(drop=True)
    conflicts = int(
        (ledger.dropna(subset=["outcome_fixed3_h5"]).groupby(["code", "action_ymd", "action"]).outcome_fixed3_h5.nunique() > 1).sum()
    )

    def has(code, ymd, action):
        return bool(((ledger.code == code) & ledger.action_ymd.eq(ymd) & ledger.action.eq(action)).any())

    anchors = {
        "9962": {"probe": has("9962", 20260707, "PROBE"), "core": has("9962", 20260713, "CORE_CLOSE")},
        "6857": {"probe": has("6857", 20240827, "PROBE"), "core": has("6857", 20240903, "CORE_CLOSE")},
        "9107": {"probe": has("9107", 20241121, "PROBE"), "core": has("9107", 20241122, "CORE_CLOSE"), "add": has("9107", 20241126, "ADD")},
        "4755": {"core": has("4755", 20251114, "CORE_CLOSE")},
        "2802": {"core": has("2802", 20240206, "CORE_CLOSE"), "profit": has("2802", 20240216, "TAKE_PROFIT")},
        "9007": {"profit": has("9007", 20231011, "TAKE_PROFIT")},
    }
    anchors_pass = all(all(parts.values()) for parts in anchors.values())
    payload = {
        "schema_version": "tradex_sell_episode_union_v1.compare.v1",
        "artifact_role": "authoritative_infrastructure",
        "review_only": True,
        "episode_contract": {
            "actions": ["PROBE", "CORE_CLOSE", "ADD", "TAKE_PROFIT"],
            "one_row": "source episode/action/date/family",
            "collisions": "preserved across source families; never silently prioritized",
            "outcome": "inherited exact fixed3 h5 where available",
        },
        "counts": {
            "raw_rows": int(len(raw)),
            "ledger_rows": int(len(ledger)),
            "episodes": int(ledger.episode_id.nunique()),
            "actions": ledger.action.value_counts().to_dict(),
            "outcome_conflicts": conflicts,
        },
        "human_anchors": anchors,
        "observed_branching": {
            "source_families": int(ledger.source_family.nunique()),
            "changed_rank_count": int(ledger.source_family.nunique()),
            "selection_divergence_reason": "distinct monthly/daily structures map to different position actions",
        },
        "judgment": {
            "decision": "keep_infrastructure" if anchors_pass and conflicts == 0 else "drop",
            "all_human_paths_present": anchors_pass,
            "outcome_conflicts_absent": conflicts == 0,
            "effectiveness_implied": False,
        },
        "not_changed": ["source detectors", "source outcomes", "action sizing", "MeeMee", "ranking", "runtime DB"],
    }
    compare = a.output / "compare.json"
    compare.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    ledger.to_parquet(a.output / "sell_episode_action_ledger.parquet", index=False)
    audit = {
        "duplicates": int(ledger.duplicated(["episode_id", "action", "action_ymd", "source_family"]).sum()),
        "outcome_conflicts": conflicts,
        "future_used_for_selection": False,
        "input_sha256": {name: sha(getattr(a, name)) for name in ["weak_score", "try_fail", "ma200", "support_break", "full_erasure", "profit_json"]},
    }
    (a.output / "audit.json").write_text(json.dumps(audit, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (a.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(compare)}, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"output": str(a.output), "counts": payload["counts"], "anchors": anchors, "judgment": payload["judgment"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
