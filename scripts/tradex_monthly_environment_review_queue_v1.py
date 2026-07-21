"""Build a small MeeMee screenshot queue for human monthly-environment labels."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

LEDGER_DEFAULT = Path(r"G:\Tradex\monthly_env_probe_add_oos_v1\20260715T021134Z-tradex_monthly_env_probe_add_oos_v1\monthly_environment_ledger.parquet")

SAMPLES = [
    ("5233", 20260630, "CONFIRMED_STABLE", "BOX"),
    ("4151", 20260630, "CONFIRMED_STABLE", "BOX"),
    ("3099", 20260630, "CONFIRMED_STABLE", "POST_BOX_BREAKOUT_CONSOLIDATION"),
    ("4021", 20260630, "CONFIRMED_STABLE", "POST_BOX_BREAKOUT_CONSOLIDATION"),
    ("7974", 20260630, "CONFIRMED_STABLE", "DOWNTREND"),
    ("9602", 20260630, "CONFIRMED_STABLE", "DOWNTREND"),
    ("8031", 20260630, "CONFIRMED_STABLE", "AMBIGUOUS"),
    ("5020", 20260630, "CONFIRMED_STABLE", "AMBIGUOUS"),
    ("7752", 20260714, "PROVISIONAL_SENSITIVE", "POST_BOX_BREAKOUT_CONSOLIDATION"),
    ("7203", 20260714, "PROVISIONAL_SENSITIVE", "DOWNTREND"),
    ("3405", 20260714, "PROVISIONAL_ONLY", "UPTREND"),
    ("4188", 20260714, "PROVISIONAL_ONLY", "UPTREND"),
    ("6301", 20230531, "HUMAN_ANCHOR", "POST_BOX_BREAKOUT_CONSOLIDATION"),
    ("6532", 20230626, "HUMAN_ANCHOR_DISAGREEMENT", "POST_BOX_BREAKOUT_CONSOLIDATION"),
]


def ymd_iso(ymd: int) -> str:
    s = str(ymd)
    return f"{s[:4]}-{s[4:6]}-{s[6:]}"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ledger", type=Path, default=LEDGER_DEFAULT)
    ap.add_argument("--output", type=Path, required=True)
    args = ap.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    ledger = pd.read_parquet(args.ledger)
    ledger["code"] = ledger.code.astype(str).str.zfill(4)
    ledger["effective_month"] = ledger.effective_month.astype(str)
    rows = []
    for code, ymd, sample_role, expected in SAMPLES:
        effective = f"{str(ymd)[:4]}-{str(ymd)[4:6]}"
        hit = ledger[(ledger.code == code) & (ledger.effective_month == effective)]
        machine = None if hit.empty else str(hit.iloc[-1].environment)
        rows.append({
            "sample_id": f"MONTHLY-{code}-{ymd}", "code": code, "as_of": ymd, "as_of_iso": ymd_iso(ymd),
            "sample_role": sample_role, "machine_environment": machine, "selection_expected_environment": expected,
            "accuracy_eligible": sample_role in {"CONFIRMED_STABLE", "HUMAN_ANCHOR"},
            "requested_human_fields": ["monthly_regime", "location", "confidence", "reason_codes"],
        })
    queue = args.output / "review_queue.jsonl"
    queue.write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows), encoding="utf-8")
    samples_arg = ",".join(f"{r['code']}:{r['as_of_iso']}" for r in rows)
    command = f"node scripts\\meemee_detail_clean_screenshot_batch_v1.mjs --samples {samples_arg} --viewport 1440x1000 --output-root {args.output / 'screenshots'}"
    (args.output / "screenshot_command.txt").write_text(command + "\n", encoding="utf-8")
    audit = {
        "schema_version": "tradex_monthly_environment_review_queue_v1.audit",
        "artifact_role": "review_queue_not_labels", "samples": len(rows),
        "confirmed_accuracy_eligible": sum(r["accuracy_eligible"] for r in rows),
        "provisional_excluded": sum(not r["accuracy_eligible"] for r in rows),
        "ledger_missing": sum(r["machine_environment"] is None for r in rows),
        "uptrend_confirmed_samples": sum(r["machine_environment"] == "UPTREND" and r["accuracy_eligible"] for r in rows),
        "screenshot_policy": "MeeMee clean detail route, non-centered mainAsOf, no lookahead",
        "not_changed": ["classifier", "MeeMee", "ranking", "runtime DB"],
    }
    (args.output / "audit.json").write_text(json.dumps(audit, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "review_queue.jsonl"}, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(args.output), "audit": audit, "samples_arg": samples_arg}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
