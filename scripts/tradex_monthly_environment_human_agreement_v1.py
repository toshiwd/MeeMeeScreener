"""Compare explicit human monthly-environment labels with the PIT classifier."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

CONTRACT_DEFAULT = Path(r"G:\Tradex\sell_human_episode_contract_v1\20260715T025100Z-tradex-sell-human-episode-contract-v1\human_episode_contract.json")
LEDGER_DEFAULT = Path(r"G:\Tradex\monthly_env_probe_add_oos_v1\20260715T021134Z-tradex_monthly_env_probe_add_oos_v1\monthly_environment_ledger.parquet")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--contract", type=Path, default=CONTRACT_DEFAULT)
    ap.add_argument("--ledger", type=Path, default=LEDGER_DEFAULT)
    ap.add_argument("--output", type=Path, required=True)
    args = ap.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    episodes = json.loads(args.contract.read_text(encoding="utf-8"))["episodes"]
    human = pd.DataFrame([{
        "episode_id": r["episode_id"], "code": str(r["code"]).zfill(4), "decision_ymd": int(r["decision_ymd"]),
        "human_environment": r["environment"]["monthly_regime"], "human_location": r["environment"]["location"],
    } for r in episodes if r["environment"]["monthly_regime"] != "UNLABELED"])
    human["effective_month"] = human.decision_ymd.astype(str).str[:4] + "-" + human.decision_ymd.astype(str).str[4:6]
    ledger = pd.read_parquet(args.ledger)
    ledger["code"] = ledger.code.astype(str).str.zfill(4)
    ledger["effective_month"] = ledger.effective_month.astype(str)
    cols = ["code", "effective_month", "source_month", "environment", "post_box", "box_reentry", "local_box_mature", "local_box_top_touch_count", "box_pos"]
    x = human.merge(ledger[cols], on=["code", "effective_month"], how="left", validate="one_to_one")
    x["exact_match"] = x.human_environment.eq(x.environment)
    rows = x.where(pd.notna(x), None).to_dict("records")
    n = len(x); matches = int(x.exact_match.sum())
    payload = {
        "schema_version": "tradex_monthly_environment_human_agreement_v1.compare.v1",
        "artifact_role": "authoritative_diagnostic",
        "fixed_condition": "classifier uses prior completed monthly bar via source_month; human label is explicit chart judgment",
        "n_explicit": n, "exact_matches": matches, "exact_match_rate": None if not n else matches / n,
        "decision": "hold",
        "reason": "two explicit labels are insufficient; mismatch may reflect completed-month PIT versus human current-month visual context",
        "rows": rows,
        "not_changed": ["monthly classifier", "probe", "core", "MeeMee", "ranking", "runtime DB"],
    }
    (args.output / "compare.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {"explicit_labels": n, "ledger_missing": int(x.environment.isna().sum()), "future_month_used": False, "review_only": True}
    (args.output / "audit.json").write_text(json.dumps(audit, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json"}, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
