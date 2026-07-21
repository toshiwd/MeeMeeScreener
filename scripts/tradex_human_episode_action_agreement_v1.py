"""Measure human-vs-machine sell episode agreement without collapsing path and action."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


CONTRACT_DEFAULT = Path(
    r"G:\Tradex\sell_human_episode_contract_v1\20260715T025100Z-tradex-sell-human-episode-contract-v1\human_episode_contract.json"
)
EVENT_DEFAULT = Path(
    r"G:\Tradex\monthly_env_probe_add_oos_v1\20260715T021134Z-tradex_monthly_env_probe_add_oos_v1\probe_add_event_ledger.parquet"
)


def expected_event(row: dict) -> str:
    if row["position_action"] == "ADD_UNSPECIFIED":
        return "ADD"
    if row["position_action"] == "TAKE_PROFIT":
        return "TAKE_PROFIT"
    return row["new_short_action"]


def exact_match(expected: str, machine: pd.Series) -> bool | None:
    any_entry = bool(machine.probe_event or machine.add1_event or machine.add2_event)
    if expected in {"AVOID", "NO_ENTRY"}:
        return not any_entry
    if expected == "PROBE":
        return bool(machine.probe_event)
    if expected == "CORE_CLOSE":
        return bool(machine.add1_event)
    if expected == "ADD":
        return bool(machine.add1_event or machine.add2_event)
    # Profit-taking is not represented by the entry-only event ledger.
    return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--contract", type=Path, default=CONTRACT_DEFAULT)
    ap.add_argument("--event-ledger", type=Path, default=EVENT_DEFAULT)
    ap.add_argument("--output", type=Path, required=True)
    args = ap.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)

    episodes = json.loads(args.contract.read_text(encoding="utf-8"))["episodes"]
    events = pd.read_parquet(args.event_ledger).copy()
    events["code"] = events.code.astype(str).str.zfill(4)
    events["ymd"] = events.ymd.astype(int)
    event_index = events.set_index(["code", "ymd"], drop=False)

    rows = []
    for human in episodes:
        code = str(human["code"]).zfill(4)
        ymd = int(human["decision_ymd"])
        key = (code, ymd)
        expected = expected_event(human)
        if key not in event_index.index:
            rows.append({
                "episode_id": human["episode_id"], "code": code, "decision_ymd": ymd,
                "expected_event": expected, "ledger_present": False, "exact_action_match": None,
                "prior_position_present": None, "path_clean_for_no_entry": None,
            })
            continue
        machine = event_index.loc[key]
        if isinstance(machine, pd.DataFrame):
            raise ValueError(f"duplicate event rows for {key}")
        prior = events[(events.code == code) & (events.ymd < ymd)].sort_values("ymd")
        prior_stage = int(prior.iloc[-1].position_stage) if len(prior) else 0
        prior_position = prior_stage > 0
        stage = int(machine.position_stage)
        is_no_entry = expected in {"AVOID", "NO_ENTRY"}
        rows.append({
            "episode_id": human["episode_id"], "code": code, "decision_ymd": ymd,
            "expected_event": expected, "ledger_present": True,
            "machine_probe_event": bool(machine.probe_event),
            "machine_add1_event": bool(machine.add1_event),
            "machine_add2_event": bool(machine.add2_event),
            "machine_position_stage": stage,
            "machine_prior_close_stage": prior_stage,
            "machine_environment": machine.environment,
            "machine_probe_family": machine.probe_family,
            "machine_position_family": machine.position_family,
            "exact_action_match": exact_match(expected, machine),
            "prior_position_present": prior_position,
            "path_prerequisite_match": (
                prior_position if expected in {"ADD", "TAKE_PROFIT"}
                else (not prior_position if expected in {"PROBE", "CORE_CLOSE"} else None)
            ),
            # Human labels do not state whether an older short was held.  AVOID
            # therefore scores same-day safety only, not historical path purity.
            "path_clean_for_no_entry": None if is_no_entry else None,
        })

    comparable = [r for r in rows if r.get("exact_action_match") is not None]
    no_entry = [r for r in rows if r["expected_event"] in {"AVOID", "NO_ENTRY"} and r["ledger_present"]]
    positive = [r for r in rows if r["expected_event"] in {"PROBE", "CORE_CLOSE", "ADD"} and r["ledger_present"]]
    payload = {
        "schema_version": "tradex_human_episode_action_agreement_v1.compare.v1",
        "artifact_role": "authoritative_diagnostic",
        "review_only": True,
        "fixed_conditions": {
            "human_contract": str(args.contract),
            "machine_event_ledger": str(args.event_ledger),
            "date_rule": "close decision on exact annotated trading date",
            "separation": ["exact_action", "prior_position_path", "profit_exit_not_in_entry_ledger"],
        },
        "metrics": {
            "episodes": len(rows),
            "ledger_present": sum(r["ledger_present"] for r in rows),
            "exact_action_comparable_n": len(comparable),
            "exact_action_match_n": sum(bool(r["exact_action_match"]) for r in comparable),
            "exact_action_match_rate": None if not comparable else sum(bool(r["exact_action_match"]) for r in comparable) / len(comparable),
            "no_entry_exact_n": len(no_entry),
            "no_entry_exact_match_n": sum(bool(r["exact_action_match"]) for r in no_entry),
            "no_entry_clean_path_n": None,
            "positive_exact_n": len(positive),
            "positive_exact_match_n": sum(bool(r["exact_action_match"]) for r in positive),
        },
        "rows": rows,
        "judgment": {
            "decision": "hold",
            "reason": "baseline diagnostic only; entry actions and prior position path are measured separately, while profit exits require a separate exit ledger",
        },
        "not_changed": ["event generation", "position lifecycle", "monthly classifier", "MeeMee", "ranking", "runtime DB"],
    }
    compare = args.output / "compare.json"
    compare.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {
        "missing_event_rows": sum(not r["ledger_present"] for r in rows),
        "profit_rows_excluded_from_exact_action": sum(r["expected_event"] == "TAKE_PROFIT" for r in rows),
        "future_data_used": False,
        "runtime_db_write": False,
    }
    (args.output / "audit.json").write_text(json.dumps(audit, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json"}, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps({"output": str(args.output), "metrics": payload["metrics"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
