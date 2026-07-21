"""Freeze an outcome-blind unused-code sample for downside-room validation."""
import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd


ACTIONS = ("ADD", "CORE", "REENTRY_PROBE", "PROBE")
QUOTAS = {"ADD": 6, "CORE": 18, "REENTRY_PROBE": 18, "PROBE": 18}
KNOWN_TEACHER_ADJUSTMENT_CODES = {"9107", "7733", "3405", "4208", "7004", "9531", "4188", "5631"}


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def stable_key(code: str, ymd: int, action: str) -> str:
    return hashlib.sha256(f"unseen-room-v1|{code}|{ymd}|{action}".encode()).hexdigest()


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--actions", type=Path, required=True)
    p.add_argument("--discovery-board", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--latest-signal", type=int, default=20260630)
    args = p.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)

    source = pd.read_parquet(args.actions)
    discovery = pd.read_parquet(args.discovery_board)
    source["code"] = source.code.astype(str).str.zfill(4)
    discovery["code"] = discovery.code.astype(str).str.zfill(4)
    discovery_codes = set(discovery.code)
    excluded = discovery_codes | KNOWN_TEACHER_ADJUSTMENT_CODES
    pool = source[
        source.action.isin(ACTIONS)
        & ~source.code.isin(excluded)
        & source.ymd.le(args.latest_signal)
    ].copy()
    pool["selection_hash"] = [stable_key(r.code, int(r.ymd), r.action) for r in pool.itertuples()]
    pool["year"] = pool.ymd.astype(str).str[:4].astype(int)

    # Rare actions select first. One code is used once across the frozen sample.
    chosen = []
    used_codes: set[str] = set()
    for action in ACTIONS:
        candidates = pool[pool.action.eq(action)].sort_values(["selection_hash", "code", "ymd"])
        for row in candidates.itertuples(index=False):
            if row.code in used_codes:
                continue
            chosen.append(row._asdict())
            used_codes.add(row.code)
            if sum(item["action"] == action for item in chosen) >= QUOTAS[action]:
                break
    sample = pd.DataFrame(chosen)
    counts = sample.action.value_counts().to_dict()
    if len(sample) < 30 or counts.get("ADD", 0) < 1 or counts.get("CORE", 0) < 10 or counts.get("PROBE", 0) < 10:
        raise RuntimeError(f"insufficient frozen breadth: rows={len(sample)} counts={counts}")
    if sample.code.duplicated().any() or set(sample.code) & excluded:
        raise RuntimeError("sample independence invariant failed")

    keep = ["code", "ymd", "action", "reason", "monthly_state", "year", "selection_hash"]
    ledger_path = args.output / "unseen_sample_frozen.parquet"
    sample[keep].sort_values(["action", "selection_hash"]).to_parquet(ledger_path, index=False)
    result = {
        "schema_version": "tradex_downside_room_unseen_sample_v1.compare.v1",
        "artifact_role": "authoritative_outcome_blind_sample_freeze",
        "review_only": True,
        "selection": {
            "source_rows": int(len(source)), "eligible_pool_rows": int(len(pool)),
            "frozen_rows": int(len(sample)), "unique_codes": int(sample.code.nunique()),
            "excluded_discovery_codes": int(len(discovery_codes)),
            "excluded_teacher_adjustment_codes": int(len(KNOWN_TEACHER_ADJUSTMENT_CODES)),
            "counts_by_action": {str(k): int(v) for k, v in sample.action.value_counts().items()},
            "counts_by_year": {str(k): int(v) for k, v in sample.year.value_counts().sort_index().items()},
            "latest_signal_ymd": args.latest_signal,
            "selection_order": "rare action first then deterministic SHA256; one code once",
            "outcome_columns_available_to_selection": [], "future_columns_available_to_selection": [],
            "weekly_columns_used": [],
        },
        "fixed_gate": {"threshold_atr": 0.5, "CORE": "always pass", "gated_actions": ["PROBE", "REENTRY_PROBE", "ADD"]},
        "not_changed": ["model actions", "MeeMee", "ranking", "runtime DB", "production trading logic"],
    }
    compare_path = args.output / "compare.json"
    compare_path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {
        "actions_sha256": sha(args.actions), "discovery_board_sha256": sha(args.discovery_board),
        "sample_sha256": sha(ledger_path),
        "discovery_code_overlap": int(sample.code.isin(discovery_codes).sum()),
        "teacher_adjustment_code_overlap": int(sample.code.isin(KNOWN_TEACHER_ADJUSTMENT_CODES).sum()),
        "duplicate_code_count": int(sample.code.duplicated().sum()),
        "outcome_join_performed": False, "db_accessed": False,
    }
    (args.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(compare_path)}, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"output": str(args.output), **result["selection"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
