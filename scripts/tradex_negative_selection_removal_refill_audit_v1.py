from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_RUN_ROOT = Path(r"G:\Tradex\negative_selection_avoidance_v1\20260511T090438Z-negative_selection_avoidance_v1")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> str:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return str(path)


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> str:
    fieldnames = sorted({key for row in rows for key in row}) if rows else ["empty"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return str(path)


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _mean(values: list[float]) -> float | None:
    return None if not values else float(sum(values) / len(values))


def _rate(values: list[bool]) -> float | None:
    return None if not values else float(sum(1 for value in values if value) / len(values))


def _is_severe(row: dict[str, Any]) -> bool:
    ret20 = _as_float(row.get("forward_ret_20d", row.get("ret20")))
    return bool(row.get("bottom15_label", row.get("severe_loser_flag", False))) or (ret20 is not None and ret20 <= -0.15)


def _month(row: dict[str, Any]) -> str:
    return str(row.get("anchor_date", row.get("decision_date", "")))[:7] or "unknown"


def _classify_removal(row: dict[str, Any]) -> str:
    ret20 = _as_float(row.get("forward_ret_20d", row.get("ret20")))
    if _is_severe(row) or (ret20 is not None and ret20 <= -0.03):
        return "useful_removal"
    if ret20 is not None and ret20 >= 0.05:
        return "harmful_removal"
    return "neutral_removal"


def _classify_refill(row: dict[str, Any], removed_avg: float | None) -> str:
    ret20 = _as_float(row.get("forward_ret_20d", row.get("ret20")))
    if _is_severe(row) or (removed_avg is not None and ret20 is not None and ret20 < removed_avg):
        return "harmful_refill"
    if ret20 is not None and (ret20 >= 0.05 or (removed_avg is not None and ret20 > removed_avg)):
        return "useful_refill"
    return "neutral_refill"


def _pair_rows(branching: dict[str, Any], top_key: str) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    removed = [dict(row) for row in branching["removed_champion_members"].get(top_key, [])]
    added = [dict(row) for row in branching["added_challenger_members"].get(top_key, [])]
    removed_by_key: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in removed:
        removed_by_key[(str(row.get("anchor_date")), str(row.get("side")))].append(row)
    pairs = []
    for add in added:
        key = (str(add.get("anchor_date")), str(add.get("side")))
        bucket = removed_by_key.get(key, [])
        if not bucket:
            continue
        bucket.sort(key=lambda row: abs(int(row.get("champion_rank") or 999) - int(add.get("candidate_rank") or 999)))
        pairs.append((bucket.pop(0), add))
    return pairs


def _member_row(row: dict[str, Any], *, top_key: str, side: str, classification: str, removed_avg: float | None = None) -> dict[str, Any]:
    ret20 = _as_float(row.get("forward_ret_20d"))
    return {
        "topk_bucket": top_key,
        "branch_side": side,
        "classification": classification,
        "decision_date": row.get("anchor_date"),
        "symbol": row.get("symbol"),
        "champion_rank": row.get("champion_rank"),
        "challenger_rank": row.get("candidate_rank"),
        "ret20": ret20,
        "removed_side_avg_ret20": removed_avg,
        "severe_loser_flag": _is_severe(row),
        "breakdown_after_failed_high_flag": row.get("breakdown_after_failed_high_flag"),
        "distance_from_20d_high_pct": row.get("distance_from_20d_high_pct"),
        "drawdown_10d": row.get("drawdown_10d"),
        "drawdown_20d": row.get("drawdown_20d"),
        "close_position_in_range": row.get("close_position_in_range"),
        "close_vs_ma7_pct": row.get("close_vs_ma7_pct"),
        "close_vs_ma20_pct": row.get("close_vs_ma20_pct"),
        "ma7_slope_5d": row.get("ma7_slope_5d"),
        "month_bucket": _month(row),
        "regime_bucket": "unverified",
    }


def _summarize(top_key: str, pairs: list[tuple[dict[str, Any], dict[str, Any]]]) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    removed_rows: list[dict[str, Any]] = []
    refill_rows: list[dict[str, Any]] = []
    pair_rows: list[dict[str, Any]] = []
    removed_rets = [_as_float(row.get("forward_ret_20d")) for row, _ in pairs]
    removed_rets = [value for value in removed_rets if value is not None]
    removed_avg_all = _mean(removed_rets)
    month_flags: dict[str, dict[str, bool]] = defaultdict(lambda: {"removal_helped": False, "refill_hurt": False})
    for removed, added in pairs:
        rem_class = _classify_removal(removed)
        ref_class = _classify_refill(added, removed_avg_all)
        rem_row = _member_row(removed, top_key=top_key, side="removed", classification=rem_class)
        ref_row = _member_row(added, top_key=top_key, side="refill", classification=ref_class, removed_avg=removed_avg_all)
        removed_rows.append(rem_row)
        refill_rows.append(ref_row)
        month = _month(removed)
        month_flags[month]["removal_helped"] = month_flags[month]["removal_helped"] or rem_class == "useful_removal"
        month_flags[month]["refill_hurt"] = month_flags[month]["refill_hurt"] or ref_class == "harmful_refill"
        pair_rows.append(
            {
                "topk_bucket": top_key,
                "decision_date": removed.get("anchor_date"),
                "month_bucket": month,
                "removed_symbol": removed.get("symbol"),
                "replacement_symbol": added.get("symbol"),
                "removed_ret20": _as_float(removed.get("forward_ret_20d")),
                "replacement_ret20": _as_float(added.get("forward_ret_20d")),
                "net_return_delta": None if _as_float(removed.get("forward_ret_20d")) is None or _as_float(added.get("forward_ret_20d")) is None else _as_float(added.get("forward_ret_20d")) - _as_float(removed.get("forward_ret_20d")),
                "removed_severe_loser": _is_severe(removed),
                "replacement_severe_loser": _is_severe(added),
                "removal_classification": rem_class,
                "refill_classification": ref_class,
                "demote_without_full_refill_pressure_note": "diagnostic_only_same_topk_no_refill_not_tradeable",
            }
        )
    refill_rets = [_as_float(row["ret20"]) for row in refill_rows if _as_float(row["ret20"]) is not None]
    removed_sev = [bool(row["severe_loser_flag"]) for row in removed_rows]
    refill_sev = [bool(row["severe_loser_flag"]) for row in refill_rows]
    rem_classes = [row["classification"] for row in removed_rows]
    ref_classes = [row["classification"] for row in refill_rows]
    affected = len(removed_rows)
    summary = {
        "affected_group_count": affected,
        "useful_removal_count": rem_classes.count("useful_removal"),
        "harmful_removal_count": rem_classes.count("harmful_removal"),
        "neutral_removal_count": rem_classes.count("neutral_removal"),
        "useful_refill_count": ref_classes.count("useful_refill"),
        "harmful_refill_count": ref_classes.count("harmful_refill"),
        "neutral_refill_count": ref_classes.count("neutral_refill"),
        "removed_avg_ret20": _mean(removed_rets),
        "replacement_avg_ret20": _mean(refill_rets),
        "removed_severe_loser_rate": _rate(removed_sev),
        "replacement_severe_loser_rate": _rate(refill_sev),
        "removal_precision": None if affected == 0 else rem_classes.count("useful_removal") / affected,
        "harmful_removal_rate": None if affected == 0 else rem_classes.count("harmful_removal") / affected,
        "harmful_refill_rate": None if affected == 0 else ref_classes.count("harmful_refill") / affected,
        "net_return_delta": None if _mean(removed_rets) is None or _mean(refill_rets) is None else _mean(refill_rets) - _mean(removed_rets),
        "net_severe_loser_delta": None if _rate(removed_sev) is None or _rate(refill_sev) is None else _rate(refill_sev) - _rate(removed_sev),
        "months_where_removal_helped": sorted([m for m, flags in month_flags.items() if flags["removal_helped"]]),
        "months_where_refill_hurt": sorted([m for m, flags in month_flags.items() if flags["refill_hurt"]]),
        "months_where_both_removal_helped_and_refill_hurt": sorted([m for m, flags in month_flags.items() if flags["removal_helped"] and flags["refill_hurt"]]),
        "demote_without_full_refill_pressure": {
            "diagnostic_only": True,
            "removed_severe_loser_rate": _rate(removed_sev),
            "removed_avg_ret20": _mean(removed_rets),
            "interpretation": "positive only if removed side is genuinely weak independent of replacement quality",
        },
    }
    return summary, removed_rows, refill_rows, pair_rows


def build_audit(run_root: Path) -> dict[str, Any]:
    compare = _load_json(run_root / "compare.json")
    decision = _load_json(run_root / "candidate_decision.json")
    branching = _load_json(run_root / "branching_summary.json")
    feature = _load_json(run_root / "failure_pattern_feature_summary.json")
    top5_summary, top5_removed, top5_refill, top5_pairs = _summarize("top5", _pair_rows(branching, "top5"))
    top10_summary, top10_removed, top10_refill, top10_pairs = _summarize("top10", _pair_rows(branching, "top10"))
    removal_precision = top5_summary["removal_precision"] or 0.0
    harmful_removal = top5_summary["harmful_removal_rate"] or 0.0
    harmful_refill = top5_summary["harmful_refill_rate"] or 0.0
    if removal_precision >= 0.30 and harmful_removal < 0.35 and (compare["deltas"]["top5"]["severe_loser_rate_delta"] < 0 or compare["deltas"]["top10"]["severe_loser_rate_delta"] < 0):
        removal_decision = "useful_removal_signal"
    elif harmful_removal >= removal_precision:
        removal_decision = "harmful_removal_signal"
    else:
        removal_decision = "weak_removal_signal"
    if harmful_refill >= 0.50 and (top5_summary["net_return_delta"] or 0) < 0:
        refill_decision = "refill_quality_harmful"
    elif harmful_refill >= 0.35:
        refill_decision = "refill_quality_weak"
    else:
        refill_decision = "refill_quality_ok"
    if removal_decision != "useful_removal_signal":
        repair = "drop_pattern"
        reason = "removal signal is not precise enough; useful removals are too rare versus harmful removals"
        next_axis = "choose_new_negative_selection_pattern_or_stop"
    elif refill_decision == "refill_quality_harmful":
        repair = "test_demote_only_policy"
        reason = "removal signal is useful but same-topK refill causes return damage"
        next_axis = "negative_selection_breakdown_after_failed_high_demote_only_v1"
    elif refill_decision == "refill_quality_weak":
        repair = "test_stricter_top5_only_veto"
        reason = "removal signal is useful but refill quality is weak"
        next_axis = "negative_selection_breakdown_after_failed_high_top5_only_v1"
    else:
        repair = "needs_more_diagnostics"
        reason = "removal/refill split is not decisive"
        next_axis = "inspect_removed_member_feature_bins"
    return {
        "audit": {
            "schema_version": "tradex_negative_selection_removal_refill_decomposition_v1",
            "generated_at": _utc_now(),
            "candidate_name": "negative_selection_avoidance_v1",
            "pattern_name": "breakdown_after_failed_high",
            "run_root": str(run_root),
            "current_decision": decision["authoritative_rollup_decision"],
            "top5_summary": top5_summary,
            "top10_summary": top10_summary,
            "no_lookahead_field_check": feature.get("no_lookahead_check", {}),
            "pair_rows": top5_pairs + top10_pairs,
        },
        "decision": {
            "schema_version": "tradex_negative_selection_repairability_decision_v1",
            "generated_at": _utc_now(),
            "candidate_name": "negative_selection_avoidance_v1",
            "pattern_name": "breakdown_after_failed_high",
            "current_decision": decision["authoritative_rollup_decision"],
            "removal_quality_decision": removal_decision,
            "refill_quality_decision": refill_decision,
            "repairability_decision": repair,
            "reason": reason,
            "top5_removal_summary": {k: v for k, v in top5_summary.items() if "removal" in k or "removed" in k or k == "affected_group_count"},
            "top5_refill_summary": {k: v for k, v in top5_summary.items() if "refill" in k or "replacement" in k or "net_" in k},
            "top10_removal_summary": {k: v for k, v in top10_summary.items() if "removal" in k or "removed" in k or k == "affected_group_count"},
            "top10_refill_summary": {k: v for k, v in top10_summary.items() if "refill" in k or "replacement" in k or "net_" in k},
            "recommended_next_axis": next_axis,
            "non_scope": ["No scoring change", "No threshold change", "No additional failure pattern", "No replay implementation", "No MeeMee change"],
        },
        "removed": top5_removed + top10_removed,
        "refill": top5_refill + top10_refill,
        "pairs": top5_pairs + top10_pairs,
    }


def write_outputs(run_root: Path) -> dict[str, str]:
    payload = build_audit(run_root)
    return {
        "negative_selection_removal_refill_decomposition.json": _write_json(run_root / "negative_selection_removal_refill_decomposition.json", payload["audit"]),
        "negative_selection_repairability_decision.json": _write_json(run_root / "negative_selection_repairability_decision.json", payload["decision"]),
        "negative_selection_removal_refill_decomposition.csv": _write_csv(run_root / "negative_selection_removal_refill_decomposition.csv", payload["pairs"]),
        "negative_selection_removed_members_audit.csv": _write_csv(run_root / "negative_selection_removed_members_audit.csv", payload["removed"]),
        "negative_selection_replacement_members_audit.csv": _write_csv(run_root / "negative_selection_replacement_members_audit.csv", payload["refill"]),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-root", default=str(DEFAULT_RUN_ROOT))
    args = parser.parse_args(argv)
    print(json.dumps(write_outputs(Path(args.run_root)), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
