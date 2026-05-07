from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import sys

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts.tradex_reflectability_funnel_common_v1 import (
    _artifact_root_for,
    _classify_publishability,
    _json_text,
    _load_json,
    _mean_or_none,
    _normalize_text,
    _safe_float,
    _safe_int,
    _safe_path,
    _scan_candidate_artifacts,
    _utc_now,
    _write_json,
    build_artifact_complete,
    build_candidate_record,
)

DEFAULT_SEARCH_ROOTS = (
    Path(r"G:\Tradex"),
    Path(r"G:\Tradex\research_sessions"),
    Path(r"G:\Tradex\scratch\research_sessions"),
    Path(r"C:\work\meemee-screener\artifacts\research_inventory"),
    Path(r"C:\work\meemee-screener\external_analysis\publish_candidates"),
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\reflectability_gap_audit_v1")
SCHEMA_VERSION = "tradex_reflectability_gap_audit_v1"
REPORT_SCHEMA_VERSION = "tradex_reflectability_gap_report_v1"
INVENTORY_SCHEMA_VERSION = "tradex_reflectability_candidate_failure_inventory_v1"
FAMILY_MAP_SCHEMA_VERSION = "tradex_reflectability_candidate_family_map_v1"
CONSISTENCY_SCHEMA_VERSION = "tradex_reflectability_decision_consistency_audit_v1"
NEXT_AXIS_SCHEMA_VERSION = "tradex_reflectability_next_axis_decision_v1"


def _load_candidate_artifact(path: Path) -> dict[str, Any] | None:
    try:
        return _load_json(path)
    except Exception:
        return None


def _classify_blockers(record: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    has_topk_metrics = record.get("top10_mean_ret20") is not None or bool(record.get("topk"))
    if not record.get("artifact_complete"):
        blockers.append("artifact_incomplete")
    if record.get("publishability") == "blocked":
        blockers.append("blocked_publishability")
    if has_topk_metrics and _safe_int(record.get("changed_top10_members_count"), 0) <= 0:
        blockers.append("no_branching")
    elif has_topk_metrics:
        if _safe_int(record.get("changed_top10_members_count"), 0) <= 3:
            blockers.append("ineffective_branching")
    if record.get("fallback_status") in {"research-fallback", "research_fallback"}:
        blockers.append("fallback_contamination")
    if record.get("candidate_local_decision") not in {"keep", "hold", "drop"}:
        blockers.append("ambiguous_candidate_decision")
    if record.get("authoritative_rollup_decision") not in {"keep", "hold", "drop"}:
        blockers.append("ambiguous_authoritative_decision")
    if record.get("candidate_local_decision") != record.get("authoritative_rollup_decision") and record.get("authoritative_rollup_decision") is not None:
        blockers.append("decision_mismatch")
    top10_delta = _safe_float(record.get("top10_mean_ret20"), 0.0)
    if has_topk_metrics and record.get("top10_mean_ret20") is not None and top10_delta < -0.002:
        blockers.append("top10_uplift_degraded")
    topk = record.get("topk") if isinstance(record.get("topk"), dict) else {}
    bottom15_delta = topk.get("10", {}).get("delta", {}).get("bottom15_contamination_rate") if isinstance(topk.get("10"), dict) else None
    if bottom15_delta is not None and _safe_float(bottom15_delta, 0.0) > 0:
        blockers.append("bottom15_contamination_worsened")
    if has_topk_metrics and record.get("monthly_top5_capture") is None:
        blockers.append("monthly_top5_capture_uninstrumented")
    return sorted(set(blockers))


def _score_blocker(blockers: list[str]) -> str:
    if not blockers:
        return "none"
    priority = [
        "artifact_incomplete",
        "fallback_contamination",
        "blocked_publishability",
        "no_branching",
        "ineffective_branching",
        "top10_uplift_degraded",
        "bottom15_contamination_worsened",
        "decision_mismatch",
        "monthly_top5_capture_uninstrumented",
        "ambiguous_candidate_decision",
        "ambiguous_authoritative_decision",
    ]
    for tag in priority:
        if tag in blockers:
            return tag
    return blockers[0]


def _build_report_rows(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for record in records:
        blockers = _classify_blockers(record)
        rows.append(
            {
                **record,
                "blockers": blockers,
                "dominant_blocker": _score_blocker(blockers),
                "publishability": record.get("publishability") or _classify_publishability(record),
                "artifact_root": record.get("artifact_root") or _artifact_root_for(Path(record.get("artifact_path") or ".")),
            }
        )
    return rows


def _summarize_blockers(rows: list[dict[str, Any]]) -> dict[str, Any]:
    counts = Counter()
    for row in rows:
        counts.update(row.get("blockers") or [])
    dominant = counts.most_common(1)[0][0] if counts else "none"
    return {
        "counts": dict(counts),
        "dominant_blocker": dominant,
        "dominant_blocker_count": int(counts[dominant]) if dominant in counts else 0,
        "record_count": len(rows),
    }


def _build_decision_consistency(rows: list[dict[str, Any]]) -> dict[str, Any]:
    mismatches = [
        {
            "candidate_id": row.get("candidate_id"),
            "family_id": row.get("family_id"),
            "artifact_path": row.get("artifact_path"),
            "candidate_local_decision": row.get("candidate_local_decision"),
            "session_aggregate_decision": row.get("session_aggregate_decision"),
            "authoritative_rollup_decision": row.get("authoritative_rollup_decision"),
            "typed_reason": row.get("typed_reason"),
        }
        for row in rows
        if row.get("candidate_local_decision") is not None
        and (
            row.get("candidate_local_decision") != row.get("authoritative_rollup_decision")
            or (
                row.get("session_aggregate_decision") is not None
                and row.get("session_aggregate_decision") != row.get("authoritative_rollup_decision")
            )
        )
    ]
    return {
        "schema_version": CONSISTENCY_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "checked_record_count": len(rows),
        "mismatch_count": len(mismatches),
        "mismatches": mismatches,
    }


def _build_family_map(rows: list[dict[str, Any]]) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get("family_id") or "unknown")].append(row)
    families = []
    for family_id, family_rows in sorted(grouped.items()):
        families.append(
            {
                "family_id": family_id,
                "candidate_count": len(family_rows),
                "candidate_ids": sorted({str(row.get("candidate_id")) for row in family_rows if row.get("candidate_id")}),
                "artifact_roots": sorted({str(row.get("artifact_root")) for row in family_rows if row.get("artifact_root")}),
                "decision_counts": dict(Counter(str(row.get("authoritative_rollup_decision") or "unknown") for row in family_rows)),
                "dominant_blocker": _score_blocker([blocker for row in family_rows for blocker in row.get("blockers") or []]),
            }
        )
    return {
        "schema_version": FAMILY_MAP_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "family_count": len(families),
        "families": families,
    }


def _build_next_axis_decision(summary: dict[str, Any], rows: list[dict[str, Any]]) -> dict[str, Any]:
    blocker_counts = summary.get("counts") or {}
    dominant = summary.get("dominant_blocker") or "none"
    next_axis = "champion_topk_bad_pick_veto_v1"
    expected_value = "high" if blocker_counts.get("no_branching", 0) > 0 else "moderate"
    if dominant in {"fallback_contamination", "artifact_incomplete"}:
        expected_value = "low"
    return {
        "schema_version": NEXT_AXIS_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "next_axis": next_axis,
        "axis_type": "branching_generation",
        "why_chosen": "highest_expected_value_for_narrow_bad_pick_removal_after_audit",
        "dominant_blocker": dominant,
        "dominant_blocker_count": int(blocker_counts.get(dominant, 0)) if dominant in blocker_counts else 0,
        "expected_value": expected_value,
        "freeze_or_retry": [
            "freeze lines that are no-op or decision-mismatched",
            "retry only with narrow top10 bad-pick veto mechanics",
        ],
        "evidence": {
            "record_count": len(rows),
            "branching_records": int(sum(1 for row in rows if _safe_int(row.get("changed_top10_members_count"), 0) > 0)),
            "decision_mismatch_count": int(sum(1 for row in rows if row.get("candidate_local_decision") != row.get("authoritative_rollup_decision"))),
        },
    }


def _build_reflectability_gap_report(rows: list[dict[str, Any]], summary: dict[str, Any], next_axis: dict[str, Any]) -> dict[str, Any]:
    blockers = summary.get("counts") or {}
    no_branching = int(blockers.get("no_branching", 0))
    ineffective = int(blockers.get("ineffective_branching", 0))
    fallback = int(blockers.get("fallback_contamination", 0))
    blocked = int(blockers.get("blocked_publishability", 0))
    artifact_incomplete = int(blockers.get("artifact_incomplete", 0))
    decision_mismatch = int(blockers.get("decision_mismatch", 0))
    dominant = summary.get("dominant_blocker") or "none"
    if blocked > 0 or artifact_incomplete > 0:
        dominant_blocker = "artifact/publishability_blocker"
    elif no_branching > 0:
        dominant_blocker = "no branching"
    elif ineffective > 0:
        dominant_blocker = "ineffective branching"
    elif fallback > 0:
        dominant_blocker = "fallback contamination"
    elif decision_mismatch > 0:
        dominant_blocker = "decision artifact ambiguity"
    else:
        dominant_blocker = dominant
    answer_1 = "No MeeMee-reflectable candidate has emerged because the available artifacts are dominated by narrow or incomplete branching and decision-layer ambiguity, not by a stable publishable improvement."
    answer_2 = dominant_blocker
    answer_3 = "Reusable signals are the narrow top10 bad-pick veto pattern, the existing same-condition compare contract, and the boundary-instrumentation style of typed branching counts."
    answer_4 = "Freeze no-op lines, broad reranker behavior, fallback-contaminated lines, and any candidate whose decision artifact cannot be aligned across local/session/authoritative layers."
    answer_5 = next_axis["next_axis"]
    return {
        "schema_version": REPORT_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "answers": {
            "why_no_reflectable_candidate": answer_1,
            "dominant_blocker": answer_2,
            "reusable_signals": answer_3,
            "frozen_lines": answer_4,
            "next_axis": answer_5,
        },
        "summary": summary,
        "record_count": len(rows),
        "publishability_counts": dict(Counter(str(row.get("publishability") or "unknown") for row in rows)),
        "decision_counts": dict(Counter(str(row.get("authoritative_rollup_decision") or "unknown") for row in rows)),
    }


def run_reflectability_gap_audit(
    *,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    search_roots: tuple[Path, ...] = DEFAULT_SEARCH_ROOTS,
) -> dict[str, Any]:
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    session_output_root = output_root / run_id
    resolved_roots = tuple(_safe_path(root, root) for root in search_roots)
    existing_roots = [root for root in resolved_roots if root.exists()]
    scanned_paths = _scan_candidate_artifacts(existing_roots)
    records: list[dict[str, Any]] = []
    skipped = []
    for path in scanned_paths:
        payload = _load_candidate_artifact(path)
        if payload is None:
            skipped.append(str(path))
            continue
        if not any(
            payload.get(key) is not None
            for key in (
                "candidate_local_decision",
                "authoritative_rollup_decision",
                "decision",
                "branching_metrics",
                "compare_topk",
                "champion_vs_challenger",
                "best_result",
                "compare_status",
                "topk_brief",
                "comparison_summary",
            )
        ):
            continue
        record = build_candidate_record(path, payload)
        record["blockers"] = _classify_blockers(record)
        record["dominant_blocker"] = _score_blocker(record["blockers"])
        records.append(record)
    if not records:
        summary = {
            "schema_version": REPORT_SCHEMA_VERSION,
            "generated_at": _utc_now(),
            "record_count": 0,
            "dominant_blocker": "none",
            "counts": {},
        }
    else:
        summary = _summarize_blockers(records)
    next_axis = _build_next_axis_decision(summary, records)
    report = _build_reflectability_gap_report(records, summary, next_axis)
    family_map = _build_family_map(records)
    decision_consistency = _build_decision_consistency(records)

    artifact_paths = {
        "reflectability_gap_report.json": _write_json(session_output_root / "reflectability_gap_report.json", report),
        "candidate_failure_inventory.json": _write_json(
            session_output_root / "candidate_failure_inventory.json",
            {
                "schema_version": INVENTORY_SCHEMA_VERSION,
                "generated_at": _utc_now(),
                "record_count": len(records),
                "records": records,
                "skipped_artifacts": skipped,
            },
        ),
        "candidate_family_map.json": _write_json(session_output_root / "candidate_family_map.json", family_map),
        "decision_consistency_audit.json": _write_json(session_output_root / "decision_consistency_audit.json", decision_consistency),
        "next_axis_decision.json": _write_json(session_output_root / "next_axis_decision.json", next_axis),
    }
    report_md = session_output_root / "report.md"
    report_md.parent.mkdir(parents=True, exist_ok=True)
    report_md.write_text(
        "\n".join(
            [
                "# reflectability_gap_audit_v1",
                "",
                f"- records: {len(records)}",
                f"- dominant_blocker: {summary.get('dominant_blocker')}",
                f"- next_axis: {next_axis['next_axis']}",
                "",
                "JSON artifacts are authoritative.",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    artifact_paths["report.md"] = report_md
    complete = build_artifact_complete(
        {
            "schema_version": SCHEMA_VERSION,
        },
        sorted([*artifact_paths.keys(), "_ARTIFACT_COMPLETE.json"]),
        schema_version=f"{SCHEMA_VERSION}_artifact_complete_v1",
    )
    _write_json(session_output_root / "_ARTIFACT_COMPLETE.json", complete)
    artifact_paths["_ARTIFACT_COMPLETE.json"] = session_output_root / "_ARTIFACT_COMPLETE.json"
    return {
        "ok": True,
        "output_root": str(session_output_root.resolve()),
        "session_output_root": str(session_output_root.resolve()),
        "record_count": len(records),
        "summary": summary,
        "next_axis": next_axis,
        "report": report,
        "decision_consistency": decision_consistency,
        "artifact_paths": {key: str(value) for key, value in artifact_paths.items()},
        "search_roots": [str(root) for root in existing_roots],
        "scanned_paths": [str(path) for path in scanned_paths],
        "run_id": run_id,
        "session_output_root": str(session_output_root.resolve()),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run TRADEX reflectability gap audit.")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args(argv)
    payload = run_reflectability_gap_audit(output_root=_safe_path(args.output_root, DEFAULT_OUTPUT_ROOT))
    print(_json_text(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
