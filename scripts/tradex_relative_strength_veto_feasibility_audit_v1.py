from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_RUN_ROOT = Path(r"G:\Tradex\relative_strength_persistence_v1\20260511T073456Z-relative_strength_persistence_v1")
AUDIT_SCHEMA_VERSION = "tradex_relative_strength_veto_feasibility_audit_v1"
DECISION_SCHEMA_VERSION = "tradex_relative_strength_veto_feasibility_decision_v1"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> str:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return str(path)


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _is_severe(row: dict[str, Any]) -> bool:
    return bool(row.get("bottom15_label")) or ((_as_float(row.get("forward_ret_20d")) or 0.0) <= -0.15)


def _month(row: dict[str, Any]) -> str:
    return str(row.get("anchor_date") or "")[:7] or "unknown"


def _classify_removal(row: dict[str, Any]) -> str:
    ret20 = _as_float(row.get("forward_ret_20d"))
    if _is_severe(row) or (ret20 is not None and ret20 <= -0.03):
        return "good_removal"
    if ret20 is not None and ret20 >= 0.05:
        return "bad_removal"
    return "neutral_removal"


def _classify_replacement(row: dict[str, Any], removed_avg: float | None) -> str:
    ret20 = _as_float(row.get("forward_ret_20d"))
    if _is_severe(row) or (ret20 is not None and removed_avg is not None and ret20 < removed_avg):
        return "bad_replacement"
    if ret20 is not None and (ret20 >= 0.05 or (removed_avg is not None and ret20 > removed_avg)):
        return "good_replacement"
    return "neutral_replacement"


def _pair_rows(branching: dict[str, Any], top_key: str) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    added = branching.get("added_challenger_members", {}).get(top_key, [])
    removed = branching.get("removed_champion_members", {}).get(top_key, [])
    removed_by_key: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in removed:
        removed_by_key[(str(row.get("anchor_date")), str(row.get("side")))].append(dict(row))
    pairs = []
    for add in added:
        key = (str(add.get("anchor_date")), str(add.get("side")))
        bucket = removed_by_key.get(key, [])
        if not bucket:
            continue
        bucket.sort(key=lambda row: abs(int(row.get("champion_rank") or 999) - int(add.get("candidate_rank") or 999)))
        pairs.append((dict(add), bucket.pop(0)))
    return pairs


def _row_common(row: dict[str, Any], *, branch_side: str, top_key: str, klass: str) -> dict[str, Any]:
    return {
        "topk_bucket": top_key,
        "branch_side": branch_side,
        "classification": klass,
        "symbol": row.get("symbol"),
        "decision_date": row.get("anchor_date"),
        "champion_rank": row.get("champion_rank"),
        "challenger_rank": row.get("candidate_rank"),
        "champion_score": row.get("champion_score"),
        "relative_strength_score": row.get("relative_strength_score_v1"),
        "ret20": row.get("forward_ret_20d"),
        "severe_loser_flag": _is_severe(row),
        "rel_ret_5d": row.get("rel_ret_5d"),
        "rel_ret_10d": row.get("rel_ret_10d"),
        "rel_ret_20d": row.get("rel_ret_20d"),
        "rel_strength_persistence_ratio_20d": row.get("rel_strength_persistence_ratio_20d"),
        "month_bucket": _month(row),
        "regime_bucket": "unverified",
    }


def _mean(values: list[float]) -> float | None:
    return None if not values else float(sum(values) / len(values))


def _rate(values: list[bool]) -> float | None:
    return None if not values else float(sum(1 for value in values if value) / len(values))


def _summarize(rows: list[dict[str, Any]], branch_side: str) -> dict[str, Any]:
    side_rows = [row for row in rows if row["branch_side"] == branch_side]
    ret_values = [_as_float(row.get("ret20")) for row in side_rows]
    ret_values = [value for value in ret_values if value is not None]
    classes = [str(row["classification"]) for row in side_rows]
    prefix = "removal" if branch_side == "removed" else "replacement"
    return {
        f"{branch_side}_count": len(side_rows),
        f"good_{prefix}_count": classes.count(f"good_{prefix}"),
        f"bad_{prefix}_count": classes.count(f"bad_{prefix}"),
        f"neutral_{prefix}_count": classes.count(f"neutral_{prefix}"),
        f"{branch_side}_avg_ret20": _mean(ret_values),
        f"{branch_side}_severe_loser_rate": _rate([bool(row["severe_loser_flag"]) for row in side_rows]),
    }


def _topk_decomposition(branching: dict[str, Any], top_key: str) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    pairs = _pair_rows(branching, top_key)
    removed_rows: list[dict[str, Any]] = []
    added_rows: list[dict[str, Any]] = []
    pair_rows: list[dict[str, Any]] = []
    by_month: dict[str, dict[str, list[float]]] = defaultdict(lambda: {"removed": [], "added": []})
    by_date: dict[str, dict[str, Any]] = defaultdict(lambda: {"good_removal": False, "bad_replacement": False})

    for added, removed in pairs:
        removed_avg = _as_float(removed.get("forward_ret_20d"))
        removal_class = _classify_removal(removed)
        replacement_class = _classify_replacement(added, removed_avg)
        removed_common = _row_common(removed, branch_side="removed", top_key=top_key, klass=removal_class)
        added_common = _row_common(added, branch_side="added", top_key=top_key, klass=replacement_class)
        removed_rows.append(removed_common)
        added_rows.append(added_common)
        pair_rows.append(
            {
                "topk_bucket": top_key,
                "decision_date": added.get("anchor_date"),
                "month_bucket": _month(added),
                "removed_symbol": removed.get("symbol"),
                "added_symbol": added.get("symbol"),
                "removed_ret20": removed.get("forward_ret_20d"),
                "added_ret20": added.get("forward_ret_20d"),
                "replacement_return_delta": None if _as_float(added.get("forward_ret_20d")) is None or _as_float(removed.get("forward_ret_20d")) is None else _as_float(added.get("forward_ret_20d")) - _as_float(removed.get("forward_ret_20d")),
                "removed_severe_loser": _is_severe(removed),
                "added_severe_loser": _is_severe(added),
                "removal_classification": removal_class,
                "replacement_classification": replacement_class,
            }
        )
        month = _month(added)
        if _as_float(removed.get("forward_ret_20d")) is not None:
            by_month[month]["removed"].append(float(removed["forward_ret_20d"]))
        if _as_float(added.get("forward_ret_20d")) is not None:
            by_month[month]["added"].append(float(added["forward_ret_20d"]))
        date_key = str(added.get("anchor_date"))
        by_date[date_key]["good_removal"] = by_date[date_key]["good_removal"] or removal_class == "good_removal"
        by_date[date_key]["bad_replacement"] = by_date[date_key]["bad_replacement"] or replacement_class == "bad_replacement"

    all_rows = removed_rows + added_rows
    removed_summary = _summarize(all_rows, "removed")
    added_summary = _summarize(all_rows, "added")
    removed_avg = removed_summary["removed_avg_ret20"]
    added_avg = added_summary["added_avg_ret20"]
    removed_sev = removed_summary["removed_severe_loser_rate"]
    added_sev = added_summary["added_severe_loser_rate"]
    months_removal_helped = []
    months_replacement_hurt = []
    for month, values in by_month.items():
        rem = _mean(values["removed"])
        add = _mean(values["added"])
        if rem is not None and rem <= -0.01:
            months_removal_helped.append(month)
        if add is not None and rem is not None and add < rem:
            months_replacement_hurt.append(month)

    summary = {
        **removed_summary,
        **added_summary,
        "net_replacement_return_delta": None if removed_avg is None or added_avg is None else added_avg - removed_avg,
        "net_severe_loser_delta": None if removed_sev is None or added_sev is None else added_sev - removed_sev,
        "months_where_removal_helped": sorted(months_removal_helped),
        "months_where_replacement_hurt": sorted(months_replacement_hurt),
        "dates_where_both_removal_helped_and_replacement_hurt": sorted(date for date, flags in by_date.items() if flags["good_removal"] and flags["bad_replacement"]),
    }
    return summary, removed_rows, added_rows, pair_rows


def build_audit(run_root: Path) -> dict[str, Any]:
    compare = _load_json(run_root / "compare.json")
    decision = _load_json(run_root / "candidate_decision.json")
    branching = _load_json(run_root / "branching_summary.json")
    feature_summary = _load_json(run_root / "relative_strength_feature_summary.json")

    top5_summary, top5_removed, top5_added, top5_pairs = _topk_decomposition(branching, "top5")
    top10_summary, top10_removed, top10_added, top10_pairs = _topk_decomposition(branching, "top10")
    removed_members = top5_removed + top10_removed
    added_members = top5_added + top10_added
    pair_rows = top5_pairs + top10_pairs

    top5_removed_sev = top5_summary["removed_severe_loser_rate"] or 0.0
    top5_added_sev = top5_summary["added_severe_loser_rate"] or 0.0
    top5_delta = top5_summary["net_replacement_return_delta"] or 0.0
    good_removal_ratio = top5_summary["good_removal_count"] / max(1, top5_summary["removed_count"])
    bad_replacement_ratio = top5_summary["bad_replacement_count"] / max(1, top5_summary["added_count"])
    veto_potential_score = max(0.0, (top5_removed_sev - top5_added_sev) + good_removal_ratio + bad_replacement_ratio + (0.25 if top5_delta < 0 else 0.0))
    reranker_quality_score = max(0.0, (top5_summary["good_replacement_count"] / max(1, top5_summary["added_count"])) - bad_replacement_ratio)

    if compare["deltas"]["top5"]["forward_ret_20d_mean_delta"] < 0 and reranker_quality_score <= 0.1:
        reranker_decision = "drop_reranker"
    elif compare["deltas"]["top5"]["forward_ret_20d_mean_delta"] > 0 and reranker_quality_score > 0.4:
        reranker_decision = "keep_reranker"
    else:
        reranker_decision = "hold_reranker"

    enough_context = feature_summary.get("feature_missing_count", 1) == 0
    severe_reduction = compare["deltas"]["top5"]["severe_loser_rate_delta"] < 0 and compare["deltas"]["top10"]["severe_loser_rate_delta"] < 0
    replacement_damage = top5_delta < 0 and bad_replacement_ratio >= 0.45
    branching_potential = compare["branching"]["changed_top5_members_count"] > 0 and compare["overlap"]["top5"]["overlap_with_champion"] < 0.9
    if enough_context and severe_reduction and replacement_damage and branching_potential:
        veto_decision = "ready_for_veto_replay"
        reason = "relative strength removes weak/severe names, but added replacements are the main top5 return damage; use as veto candidate only"
        recommended = "relative_strength_persistence_veto_v1"
    elif not enough_context:
        veto_decision = "needs_more_diagnostics"
        reason = "row-level feature context is incomplete"
        recommended = "add_relative_strength_row_context"
    else:
        veto_decision = "drop_axis"
        reason = "removed-side effect is not clearly helpful enough for veto replay"
        recommended = "choose_new_independent_axis"

    audit = {
        "schema_version": AUDIT_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "candidate_name": "relative_strength_persistence_v1",
        "run_root": str(run_root),
        "current_decision": decision["authoritative_rollup_decision"],
        "top5_summary": top5_summary,
        "top10_summary": top10_summary,
        "veto_potential_score": veto_potential_score,
        "reranker_quality_score": reranker_quality_score,
        "no_lookahead_field_check": feature_summary.get("market_proxy", {}),
        "pair_rows": pair_rows,
    }
    decision_payload = {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "candidate_name": "relative_strength_persistence_v1",
        "current_decision": decision["authoritative_rollup_decision"],
        "reranker_decision": reranker_decision,
        "veto_feasibility_decision": veto_decision,
        "reason": reason,
        "top5_removed_side_summary": {k: v for k, v in top5_summary.items() if "removed" in k or "removal" in k},
        "top5_added_side_summary": {k: v for k, v in top5_summary.items() if "added" in k or "replacement" in k},
        "top10_removed_side_summary": {k: v for k, v in top10_summary.items() if "removed" in k or "removal" in k},
        "top10_added_side_summary": {k: v for k, v in top10_summary.items() if "added" in k or "replacement" in k},
        "veto_potential_score": veto_potential_score,
        "reranker_quality_score": reranker_quality_score,
        "recommended_next_axis": recommended,
        "non_scope": ["No scoring change", "No ranking change", "No threshold change", "No champion change", "No MeeMee change", "No veto implementation"],
    }
    return {
        "audit": audit,
        "decision": decision_payload,
        "removed_members": removed_members,
        "added_members": added_members,
        "pair_rows": pair_rows,
    }


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> str:
    fieldnames = sorted({key for row in rows for key in row.keys()}) if rows else ["empty"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return str(path)


def write_outputs(run_root: Path) -> dict[str, str]:
    payload = build_audit(run_root)
    return {
        "relative_strength_swap_decomposition.json": _write_json(run_root / "relative_strength_swap_decomposition.json", payload["audit"]),
        "relative_strength_veto_feasibility_decision.json": _write_json(run_root / "relative_strength_veto_feasibility_decision.json", payload["decision"]),
        "relative_strength_swap_decomposition.csv": _write_csv(run_root / "relative_strength_swap_decomposition.csv", payload["pair_rows"]),
        "relative_strength_removed_members.csv": _write_csv(run_root / "relative_strength_removed_members.csv", payload["removed_members"]),
        "relative_strength_added_members.csv": _write_csv(run_root / "relative_strength_added_members.csv", payload["added_members"]),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-root", default=str(DEFAULT_RUN_ROOT))
    args = parser.parse_args(argv)
    print(json.dumps(write_outputs(Path(args.run_root)), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
