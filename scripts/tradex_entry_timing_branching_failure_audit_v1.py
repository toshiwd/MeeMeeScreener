from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_RUN_ROOT = Path(r"G:\Tradex\entry_timing_confirmed_signal_v1\20260511T064802Z-entry_timing_confirmed_signal_v1")
AUDIT_SCHEMA_VERSION = "tradex_entry_timing_branching_failure_audit_v1"
DECISION_SCHEMA_VERSION = "tradex_entry_timing_branching_repairability_decision_v1"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _is_severe(row: dict[str, Any]) -> bool:
    return bool(row.get("bottom15_label")) or ((_as_float(row.get("forward_ret_20d")) or 0.0) <= -0.15)


def _month(value: Any) -> str:
    text = str(value or "")
    return text[:7] if len(text) >= 7 else "unknown"


def _pair_swaps(branching: dict[str, Any], top_key: str) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    added = branching.get("added_challenger_members", {}).get(top_key, [])
    removed = branching.get("removed_champion_members", {}).get(top_key, [])
    removed_by_key: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in removed:
        removed_by_key[(str(row.get("anchor_date")), str(row.get("side")))].append(row)

    pairs: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for add in added:
        key = (str(add.get("anchor_date")), str(add.get("side")))
        bucket = removed_by_key.get(key, [])
        if not bucket:
            pairs.append((add, {}))
            continue
        bucket.sort(key=lambda row: abs((int(row.get("champion_rank") or 999) - int(add.get("candidate_rank") or 999))))
        pairs.append((add, bucket.pop(0)))
    return pairs


def _classify_harmful(add: dict[str, Any], removed: dict[str, Any], *, has_context_fields: bool) -> tuple[bool, str, list[str]]:
    add_ret = _as_float(add.get("forward_ret_20d"))
    removed_ret = _as_float(removed.get("forward_ret_20d"))
    add_severe = _is_severe(add)
    removed_strong = removed_ret is not None and removed_ret >= 0.05
    underperformed = add_ret is not None and removed_ret is not None and add_ret < removed_ret
    harmful = bool(underperformed or add_severe or removed_strong)
    triggers = []
    if underperformed:
        triggers.append("added_underperformed_removed")
    if add_severe:
        triggers.append("added_severe_loser")
    if removed_strong:
        triggers.append("removed_strong_positive")

    if not harmful:
        return False, "not_harmful", triggers
    if not has_context_fields:
        return True, "insufficient_fields", triggers

    state = str(add.get("entry_timing_state") or "")
    if state == "entry_confirmed" and add_severe:
        return True, "falling_knife_not_blocked", triggers
    if state == "entry_neutral" and underperformed:
        return True, "weak_reclaim_false_positive", triggers
    return True, "unknown_no_common_pattern", triggers


def build_audit(run_root: Path) -> dict[str, Any]:
    candidate_decision = _load_json(run_root / "candidate_decision.json")
    compare = _load_json(run_root / "compare.json")
    branching = _load_json(run_root / "branching_summary.json")
    by_month = _load_json(run_root / "by_month.json")
    by_regime = _load_json(run_root / "by_regime.json")
    family_leaderboard = _load_json(run_root / "family_leaderboard.json")

    context_fields = {
        "gap_pct",
        "vol_ratio5_20",
        "candle_body_ratio",
        "candle_upper_wick_ratio",
        "candle_lower_wick_ratio",
        "monthly_context_no_lookahead",
        "weekly_context_no_lookahead",
    }
    sample_rows = (
        branching.get("added_challenger_members", {}).get("top5", [])
        + branching.get("removed_champion_members", {}).get("top5", [])
    )
    available_fields = set(sample_rows[0]) if sample_rows else set()
    has_context_fields = bool(context_fields & available_fields)

    audit_rows: list[dict[str, Any]] = []
    harmful_examples: list[dict[str, Any]] = []
    pattern_counter: Counter[str] = Counter()
    harmful_month_counter: Counter[str] = Counter()
    repairable_patterns = {"falling_knife_not_blocked", "weak_reclaim_false_positive"}

    for top_key in ("top5", "top10", "top20"):
        for add, removed in _pair_swaps(branching, top_key):
            harmful, pattern, triggers = _classify_harmful(add, removed, has_context_fields=has_context_fields)
            month_bucket = _month(add.get("anchor_date"))
            row = {
                "topk_bucket": top_key,
                "anchor_date": add.get("anchor_date"),
                "month_bucket": month_bucket,
                "regime_bucket": "unverified" if by_regime.get("breadth", {}).get("bucket_count") == 1 else "mixed",
                "side": add.get("side"),
                "added_symbol": add.get("symbol"),
                "removed_symbol": removed.get("symbol"),
                "added_ret20": add.get("forward_ret_20d"),
                "removed_ret20": removed.get("forward_ret_20d"),
                "added_severe_loser": _is_severe(add),
                "removed_severe_loser": _is_severe(removed),
                "added_champion_rank": add.get("champion_rank"),
                "added_candidate_rank": add.get("candidate_rank"),
                "removed_champion_rank": removed.get("champion_rank"),
                "removed_candidate_rank": removed.get("candidate_rank"),
                "added_entry_timing_state": add.get("entry_timing_state"),
                "removed_entry_timing_state": removed.get("entry_timing_state"),
                "selection_divergence_reason": branching.get("selection_divergence_reason"),
                "harmful": harmful,
                "harmful_triggers": "|".join(triggers),
                "failure_pattern": pattern,
            }
            audit_rows.append(row)
            if harmful:
                pattern_counter[pattern] += 1
                if top_key == "top5":
                    harmful_month_counter[month_bucket] += 1
                harmful_examples.append(row)

    top5_improved_months = [
        row["month_bucket"]
        for row in by_month.get("rows", [])
        if row.get("changed_top5_members_count", 0) > 0 and (row.get("top5_forward_ret_20d_mean_delta") or 0) > 0
    ]
    top5_harmful_months = [
        row["month_bucket"]
        for row in by_month.get("rows", [])
        if row.get("changed_top5_members_count", 0) > 0 and (row.get("top5_forward_ret_20d_mean_delta") or 0) < 0
    ]

    harmful_count = sum(1 for row in audit_rows if row["harmful"])
    repairable_count = sum(1 for row in audit_rows if row["harmful"] and row["failure_pattern"] in repairable_patterns)
    unknown_count = sum(1 for row in audit_rows if row["harmful"] and row["failure_pattern"] in {"insufficient_fields", "unknown_no_common_pattern"})
    dominant = [{"pattern": pattern, "count": count} for pattern, count in pattern_counter.most_common()]

    if not has_context_fields and harmful_count:
        repairability_decision = "needs_more_fields"
        reason = "branching artifacts include outcomes and timing state but not enough feature context to distinguish overextension, falling knife, spike exhaustion, or weak reclaim"
        recommended_next_axis = "enrich_branching_artifacts_with_existing_timing_feature_context"
    elif harmful_count and len(pattern_counter) <= 2 and repairable_count / harmful_count >= 0.7:
        repairability_decision = "repair_candidate"
        reason = "harmful swaps cluster into repairable patterns under available artifact fields"
        recommended_next_axis = "single_rule_repair_against_dominant_failure_pattern"
    else:
        repairability_decision = "drop_candidate"
        reason = "harmful swaps do not cluster into a small verified repairable pattern"
        recommended_next_axis = "drop_entry_timing_confirmed_signal_v1_or_reframe_after_new_evidence"

    audit = {
        "schema_version": AUDIT_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "run_root": str(run_root),
        "inputs": {
            "candidate_decision": candidate_decision.get("authoritative_rollup_decision"),
            "compare_schema": compare.get("schema_version"),
            "branching_schema": branching.get("schema_version"),
            "family_leaderboard_schema": family_leaderboard.get("schema_version"),
        },
        "available_added_removed_fields": sorted(available_fields),
        "has_context_fields_for_pattern_classification": has_context_fields,
        "audit_rows": audit_rows,
        "summary": {
            "swap_count": len(audit_rows),
            "harmful_swap_count": harmful_count,
            "repairable_harmful_swap_count": repairable_count,
            "unknown_harmful_swap_count": unknown_count,
            "failure_pattern_counts": dict(pattern_counter),
            "top5_harmful_months": top5_harmful_months,
            "top5_improved_months": top5_improved_months,
            "top5_harmful_month_counts_from_swaps": dict(harmful_month_counter),
        },
    }
    decision = {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "decision": repairability_decision,
        "reason": reason,
        "dominant_failure_patterns": dominant,
        "harmful_swap_count": harmful_count,
        "repairable_harmful_swap_count": repairable_count,
        "unknown_harmful_swap_count": unknown_count,
        "top5_harmful_months": top5_harmful_months,
        "top5_improved_months": top5_improved_months,
        "recommended_next_axis": recommended_next_axis,
        "non_scope": [
            "No entry timing rule tuning",
            "No champion mutation",
            "No MeeMee mutation",
            "No live ranking mutation",
            "No publish action",
        ],
    }
    return {"audit": audit, "decision": decision, "harmful_examples": {"schema_version": AUDIT_SCHEMA_VERSION, "generated_at": _utc_now(), "examples": harmful_examples[:50]}}


def write_outputs(run_root: Path) -> dict[str, str]:
    payload = build_audit(run_root)
    audit = payload["audit"]
    paths = {
        "branching_failure_audit.json": str(_write_json(run_root / "branching_failure_audit.json", audit)),
        "harmful_swap_examples.json": str(_write_json(run_root / "harmful_swap_examples.json", payload["harmful_examples"])),
        "repairability_decision.json": str(_write_json(run_root / "repairability_decision.json", payload["decision"])),
    }
    csv_path = run_root / "branching_failure_audit.csv"
    rows = audit["audit_rows"]
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()) if rows else ["topk_bucket"])
        writer.writeheader()
        writer.writerows(rows)
    paths["branching_failure_audit.csv"] = str(csv_path)
    return paths


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-root", default=str(DEFAULT_RUN_ROOT))
    args = parser.parse_args(argv)
    paths = write_outputs(Path(args.run_root))
    print(json.dumps(paths, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
