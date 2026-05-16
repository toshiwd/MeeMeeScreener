from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.backend.services import tradex_research_contracts as contracts
from scripts import tradex_candidate_generation_hypothesis_map_refresh_v1 as refresh_mod


AXIS_ID = "candidate_generation_source_mechanism_validation_v1"
SCHEMA_PREFIX = "tradex_candidate_generation_source_mechanism_validation_v1"

DEFAULT_HYPOTHESIS_REFRESH_RUN_ID = "20260513T180000Z-candidate-generation-hypothesis-map-refresh-v1"
DEFAULT_SECOND_REVIEW_RUN_ID = "20260513T170000Z-candidate-generation-hypothesis-map-second-hypothesis-review-v1"
DEFAULT_APPLICABILITY_RUN_ID = "20260513T160000Z-source-specific-timeblock-applicability-audit-v1"
DEFAULT_VALIDATION_RUN_ID = "20260513T150000Z-source-specific-candidate-generation-validation-v1"
DEFAULT_MISSED_WINNER_RUN_ID = "20260513T140000Z-missed-winner-event-source-candidate-generation-v1"
DEFAULT_ROOT_CAUSE_RUN_ID = "20260513T130000Z-oracle-gap-and-candidate-generation-root-cause-v1"
DEFAULT_WIDE_RUN_ID = "20260513T030000Z-wide-strength-pool-upside-rerank-v1"

DEFAULT_HYPOTHESIS_REFRESH_ROOT = Path(r"G:\Tradex\candidate_generation_hypothesis_map_refresh_v1")
DEFAULT_SECOND_REVIEW_ROOT = Path(r"G:\Tradex\candidate_generation_hypothesis_map_second_hypothesis_review_v1")
DEFAULT_APPLICABILITY_ROOT = Path(r"G:\Tradex\source_specific_timeblock_applicability_audit_v1")
DEFAULT_VALIDATION_ROOT = Path(r"G:\Tradex\source_specific_candidate_generation_validation_v1")
DEFAULT_MISSED_WINNER_ROOT = Path(r"G:\Tradex\missed_winner_event_source_candidate_generation_v1")
DEFAULT_ROOT_CAUSE_ROOT = Path(r"G:\Tradex\oracle_gap_and_candidate_generation_root_cause_v1")
DEFAULT_WIDE_ROOT = Path(r"G:\Tradex\wide_strength_pool_upside_rerank_v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\candidate_generation_source_mechanism_validation_v1")

REQUIRED_ARTIFACTS = (
    "evaluation_contract.json",
    "run_manifest.json",
    "source_artifact_refs.json",
    "source_mechanism_validation_contract.json",
    "refreshed_hypothesis_readback.json",
    "archived_source_distinctness_audit.json",
    "hypothesis_source_mechanism_report.json",
    "per_source_same_date_support_report.json",
    "hypothesis_validation_readiness_leaderboard.json",
    "selected_next_validation_target.json",
    "rejected_hypothesis_report.json",
    "next_axis_recommendation.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{AXIS_ID}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _json_text(payload: Any) -> str:
    return json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, default=str)


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_json_text(payload) + "\n", encoding="utf-8")
    return path


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _stable_hash(payload: Any) -> str:
    return hashlib.sha256(_json_text(payload).encode("utf-8")).hexdigest()


def _file_hash(path: Path) -> str | None:
    if not path.exists() or not path.is_file():
        return None
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _safe_path(value: str | Path | None, default: Path) -> Path:
    if value is None or not str(value).strip():
        return default.resolve()
    return Path(str(value)).expanduser().resolve()


def _run_dir(root: str | Path, run_id: str, default_root: Path) -> Path:
    return _safe_path(root, default_root) / run_id


def _float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _parse_source_family(source_family: str) -> dict[str, str]:
    return refresh_mod._parse_source_family(source_family)


def _shared_tag_rate(source_family: str, reference_family: str) -> float:
    return refresh_mod._shared_tag_rate(source_family, reference_family)


def validate_sources(
    *,
    hypothesis_refresh_dir: Path,
    second_review_dir: Path,
    applicability_dir: Path,
    validation_dir: Path,
    missed_winner_dir: Path,
    root_cause_dir: Path,
    wide_dir: Path,
) -> dict[str, Any]:
    required_by_source = {
        "hypothesis_refresh": [
            "_ARTIFACT_COMPLETE.json",
            "research_decision.json",
            "refreshed_candidate_generation_hypothesis_map.json",
            "remaining_source_scan_report.json",
            "archived_source_failure_summary.json",
        ],
        "second_review": [
            "_ARTIFACT_COMPLETE.json",
            "research_decision.json",
            "hypothesis_distinctness_audit.json",
        ],
        "applicability": [
            "_ARTIFACT_COMPLETE.json",
            "research_decision.json",
            "point_in_time_applicability_proxy_report.json",
        ],
        "validation": [
            "_ARTIFACT_COMPLETE.json",
            "research_decision.json",
            "validation_outcome_classification.json",
        ],
        "missed_winner": [
            "_ARTIFACT_COMPLETE.json",
            "research_decision.json",
            "same_date_source_miss_report.json",
            "time_block_source_stability.json",
            "max3_source_structure_report.json",
        ],
        "root_cause": [
            "_ARTIFACT_COMPLETE.json",
            "research_decision.json",
            "failure_mode_classification.json",
        ],
        "wide": [
            "_ARTIFACT_COMPLETE.json",
            "research_decision.json",
            "ranking_coverage_audit.json",
        ],
    }
    dirs = {
        "hypothesis_refresh": hypothesis_refresh_dir,
        "second_review": second_review_dir,
        "applicability": applicability_dir,
        "validation": validation_dir,
        "missed_winner": missed_winner_dir,
        "root_cause": root_cause_dir,
        "wide": wide_dir,
    }
    status: dict[str, Any] = {}
    for source_name, names in required_by_source.items():
        root = dirs[source_name]
        missing = [name for name in names if not (root / name).exists()]
        if missing:
            raise FileNotFoundError(f"{source_name} missing required artifacts: {missing} at {root}")
        complete = _load_json(root / "_ARTIFACT_COMPLETE.json")
        decision = _load_json(root / "research_decision.json")
        if complete.get("complete") is not True:
            raise RuntimeError(f"{source_name} artifact is not complete")
        if complete.get("silent_fallback_used") is not False or decision.get("silent_fallback_used") is True:
            raise RuntimeError(f"{source_name} used silent fallback")
        if complete.get("research_fallback_used") is True or decision.get("research_fallback_used") is True:
            raise RuntimeError(f"{source_name} used research fallback")
        status[source_name] = {"_ARTIFACT_COMPLETE.json": complete, "research_decision.json": decision}
        for name in names:
            if name in {"_ARTIFACT_COMPLETE.json", "research_decision.json"}:
                continue
            status[source_name][name] = _load_json(root / name)

    refresh_decision = status["hypothesis_refresh"]["research_decision.json"]
    if refresh_decision.get("authoritative_research_decision") != "hypothesis_map_refreshed_next_validation_ready":
        raise RuntimeError("hypothesis refresh source is not next-validation-ready")
    if _int(refresh_decision.get("refreshed_hypothesis_count")) != 2:
        raise RuntimeError("hypothesis refresh source does not contain exactly two hypotheses")
    if refresh_decision.get("candidate_generation_challenger_created") is not False:
        raise RuntimeError("hypothesis refresh source already created a challenger")
    if status["second_review"]["research_decision.json"].get("authoritative_research_decision") != "second_hypothesis_drop":
        raise RuntimeError("second review source is not a drop")
    if status["validation"]["research_decision.json"].get("authoritative_research_decision") != "source_specific_candidate_generation_drop":
        raise RuntimeError("source validation source is not a first-source drop")
    return status


def _hypotheses(status: dict[str, Any]) -> list[dict[str, Any]]:
    hypotheses = status["hypothesis_refresh"]["refreshed_candidate_generation_hypothesis_map.json"].get("hypotheses") or []
    return [dict(item) for item in hypotheses if isinstance(item, dict)]


def build_refreshed_hypothesis_readback(status: dict[str, Any]) -> dict[str, Any]:
    hypotheses = _hypotheses(status)
    archived = {refresh_mod.FIRST_ARCHIVED_SOURCE, refresh_mod.SECOND_DROPPED_SOURCE}
    rows = [
        {
            "hypothesis_id": hyp.get("hypothesis_id"),
            "mechanism": hyp.get("expected_mechanism"),
            "source_family": hyp.get("source_family"),
            "target_failure_mode": hyp.get("target_failure_mode"),
            "archived_source_reintroduced": hyp.get("source_family") in archived,
        }
        for hyp in hypotheses
    ]
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_refreshed_hypothesis_readback_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "source_hypothesis_refresh_decision": status["hypothesis_refresh"]["research_decision.json"].get("authoritative_research_decision"),
        "refreshed_hypothesis_count": len(hypotheses),
        "refreshed_hypothesis_count_expected": 2,
        "archived_first_second_reintroduced": any(row["archived_source_reintroduced"] for row in rows),
        "hypotheses": rows,
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_archived_source_distinctness_audit(status: dict[str, Any]) -> dict[str, Any]:
    rows = []
    for hyp in _hypotheses(status):
        source = str(hyp.get("source_family") or "")
        source_tags = _parse_source_family(source)
        first_tags = _parse_source_family(refresh_mod.FIRST_ARCHIVED_SOURCE)
        second_tags = _parse_source_family(refresh_mod.SECOND_DROPPED_SOURCE)
        unique_vs_first = {key: value for key, value in source_tags.items() if first_tags.get(key) != value}
        unique_vs_second = {key: value for key, value in source_tags.items() if second_tags.get(key) != value}
        shared_first = _shared_tag_rate(source, refresh_mod.FIRST_ARCHIVED_SOURCE)
        shared_second = _shared_tag_rate(source, refresh_mod.SECOND_DROPPED_SOURCE)
        same_failure_risk = (hyp.get("risk_profile") or {}).get("same_failure_risk")
        rows.append(
            {
                "hypothesis_id": hyp.get("hypothesis_id"),
                "source_family": source,
                "expected_mechanism": hyp.get("expected_mechanism"),
                "overlap_with_archived_first_source": source == refresh_mod.FIRST_ARCHIVED_SOURCE,
                "overlap_with_dropped_second_source": source == refresh_mod.SECOND_DROPPED_SOURCE,
                "shared_tag_rate_with_archived_first": shared_first,
                "shared_tag_rate_with_dropped_second": shared_second,
                "shared_tag_rate_max": max(shared_first, shared_second),
                "unique_mechanism_tags_vs_archived_first": unique_vs_first,
                "unique_mechanism_tags_vs_dropped_second": unique_vs_second,
                "same_failure_risk": same_failure_risk,
                "meaningfully_distinct_from_archived_sources": source not in {refresh_mod.FIRST_ARCHIVED_SOURCE, refresh_mod.SECOND_DROPPED_SOURCE}
                and max(shared_first, shared_second) <= 0.60
                and same_failure_risk != "high",
            }
        )
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_archived_source_distinctness_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "rows": rows,
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def _scan_row_by_source(status: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = status["hypothesis_refresh"]["remaining_source_scan_report.json"].get("rows") or []
    return {str(row.get("event_source")): dict(row) for row in rows if isinstance(row, dict) and row.get("event_source")}


def build_hypothesis_source_mechanism_report(status: dict[str, Any]) -> dict[str, Any]:
    by_source = _scan_row_by_source(status)
    rows = []
    for hyp in _hypotheses(status):
        source = str(hyp.get("source_family") or "")
        scan = by_source.get(source, {})
        risk = hyp.get("risk_profile") or {}
        rows.append(
            {
                "hypothesis_id": hyp.get("hypothesis_id"),
                "source_family": source,
                "expected_mechanism": hyp.get("expected_mechanism"),
                "target_failure_mode": hyp.get("target_failure_mode"),
                "sample_count": _int(scan.get("sample_count")),
                "missed_winner_count": _int(scan.get("missed_winner_count", risk.get("missed_winner_count"))),
                "future_winner_rate": _float(scan.get("future_winner_rate", risk.get("future_winner_rate"))),
                "severe_loss_rate20": _float(scan.get("severe_loss_rate20", risk.get("severe_loss_rate20"))),
                "selected_nonwinner_rate": _float(scan.get("selected_nonwinner_rate", risk.get("selected_nonwinner_rate"))),
                "time_block_stability": _float(scan.get("time_block_stability")),
                "same_date_source_miss_support": scan.get("same_date_source_miss_support"),
                "same_date_source_miss_support_available": bool(scan.get("same_date_source_miss_support_available")),
                "max3_structure_fit": _float(scan.get("max3_structure_fit")),
                "selected_capture_rate_among_source_winners": _float(scan.get("selected_capture_rate_among_source_winners")),
                "mechanisms": scan.get("mechanisms") or [hyp.get("expected_mechanism")],
            }
        )
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_hypothesis_source_mechanism_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "rows": rows,
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_per_source_same_date_support_report(*, status: dict[str, Any], wide_dir: Path) -> dict[str, Any]:
    ledger_path = wide_dir / "date_level_selection_ledger.jsonl"
    required_source_fields = {"pre_ma20_path_state", "pre_ma60_context_state", "weekly_prior_state", "negative_guard_match"}
    ledger_exists = ledger_path.exists()
    observed_fields: set[str] = set()
    row_count_checked = 0
    if ledger_exists:
        with ledger_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                row = json.loads(line)
                if isinstance(row, dict):
                    observed_fields.update(str(key) for key in row.keys())
                    row_count_checked += 1
                if row_count_checked >= 200:
                    break
    missing_fields = sorted(required_source_fields - observed_fields)
    aggregate = status["missed_winner"]["same_date_source_miss_report.json"]
    available = bool(ledger_exists and not missing_fields and row_count_checked > 0)
    rows = []
    for hyp in _hypotheses(status):
        rows.append(
            {
                "hypothesis_id": hyp.get("hypothesis_id"),
                "source_family": hyp.get("source_family"),
                "per_source_same_date_support_available": False,
                "same_date_source_miss_support": None,
                "support_basis": "unavailable_no_full_source_family_same_date_ledger",
            }
        )
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_per_source_same_date_support_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "attempted_per_source_same_date_support_repair": True,
        "per_source_same_date_support_available": False,
        "per_source_support_not_faked_from_aggregate": True,
        "candidate_ledger_checked": str(ledger_path),
        "candidate_ledger_exists": ledger_exists,
        "candidate_ledger_row_count_checked": row_count_checked,
        "required_source_fields": sorted(required_source_fields),
        "observed_source_fields_sample": sorted(observed_fields),
        "missing_required_source_fields": missing_fields,
        "repair_possible_if_all_fields_present": available,
        "aggregate_same_date_support_only": {
            "winner_available_day_count": aggregate.get("winner_available_day_count"),
            "winner_source_present_but_under_ranked_rate": aggregate.get("winner_source_present_but_under_ranked_rate"),
            "source_mismatch_explains_miss_rate": aggregate.get("source_mismatch_explains_miss_rate"),
        },
        "rows": rows,
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def _failure_checks(row: dict[str, Any], distinct: dict[str, Any], per_source_same_date_available: bool) -> list[str]:
    return [
        reason
        for reason, present in [
            ("near_duplicate_of_archived_source", not distinct.get("meaningfully_distinct_from_archived_sources")),
            ("severe_loss_structurally_high", _float(row.get("severe_loss_rate20")) > 0.26),
            ("nonwinner_profile_unacceptable", _float(row.get("selected_nonwinner_rate")) > 0.70),
            ("same_date_support_missing", not per_source_same_date_available),
            ("same_date_support_weak", False),
            ("max3_overfill_risk_high", _float(row.get("max3_structure_fit")) < 0.50),
            ("sample_too_small", _int(row.get("sample_count")) < 100 or _int(row.get("missed_winner_count")) < 20),
            ("mechanism_too_vague", not row.get("expected_mechanism") or row.get("expected_mechanism") == "insufficient_distinct_mechanism"),
        ]
        if present
    ]


def build_hypothesis_validation_readiness_leaderboard(
    *,
    mechanism_report: dict[str, Any],
    distinctness: dict[str, Any],
    same_date_report: dict[str, Any],
) -> dict[str, Any]:
    distinct_by_id = {row["hypothesis_id"]: row for row in distinctness.get("rows") or []}
    per_source_available = bool(same_date_report.get("per_source_same_date_support_available"))
    rows = []
    for row in mechanism_report.get("rows") or []:
        distinct = distinct_by_id.get(row["hypothesis_id"], {})
        failure_checks = _failure_checks(row, distinct, per_source_available)
        score = (
            _float(distinct.get("meaningfully_distinct_from_archived_sources")) * 1.0
            + _int(row.get("missed_winner_count")) * 0.015
            + _float(row.get("future_winner_rate")) * 0.8
            + _float(row.get("max3_structure_fit")) * 0.55
            + _float(row.get("time_block_stability")) * 0.25
            - _float(row.get("severe_loss_rate20")) * 1.1
            - _float(row.get("selected_nonwinner_rate")) * 0.45
        )
        rows.append(
            {
                **row,
                "meaningfully_distinct_from_archived_sources": distinct.get("meaningfully_distinct_from_archived_sources"),
                "shared_tag_rate_max": distinct.get("shared_tag_rate_max"),
                "same_failure_risk": distinct.get("same_failure_risk"),
                "per_source_same_date_support_available": per_source_available,
                "failure_mode_checks": failure_checks,
                "testability": bool(
                    distinct.get("meaningfully_distinct_from_archived_sources")
                    and distinct.get("same_failure_risk") != "high"
                    and _int(row.get("sample_count")) >= 100
                    and _int(row.get("missed_winner_count")) >= 20
                    and _float(row.get("severe_loss_rate20")) <= 0.26
                    and _float(row.get("selected_nonwinner_rate")) <= 0.70
                ),
                "validation_readiness_score_diagnostic_only": score,
            }
        )
    rows.sort(key=lambda item: item["validation_readiness_score_diagnostic_only"], reverse=True)
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_hypothesis_validation_readiness_leaderboard_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "rows": rows,
        "ranking_basis": [
            "distinctness",
            "missed_winner_concentration",
            "severe_loss_risk",
            "nonwinner_risk",
            "same_date_support",
            "max3_fit",
            "time_block_stability",
            "testability",
        ],
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_selected_next_validation_target(leaderboard: dict[str, Any]) -> dict[str, Any]:
    rows = leaderboard.get("rows") or []
    eligible = [row for row in rows if row.get("testability") and row.get("same_failure_risk") != "high"]
    selected_row = eligible[0] if eligible else None
    rejected = []
    for row in rows:
        if selected_row and row["hypothesis_id"] == selected_row["hypothesis_id"]:
            continue
        reasons = row.get("failure_mode_checks") or []
        if selected_row and not reasons:
            reasons = ["lower_validation_readiness_score"]
        rejected.append({"hypothesis_id": row.get("hypothesis_id"), "reason": "_or_".join(reasons) if reasons else "not_selected"})
    selected = selected_row is not None
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_selected_next_validation_target_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "selected": selected,
        "selected_hypothesis_id": selected_row.get("hypothesis_id") if selected_row else None,
        "selected_source_family": selected_row.get("source_family") if selected_row else None,
        "selected_next_axis": "source_specific_candidate_generation_validation_v2" if selected else None,
        "reason": [
            reason
            for reason, present in [
                ("distinct_from_archived_sources", bool(selected_row and selected_row.get("meaningfully_distinct_from_archived_sources"))),
                ("missed_winner_concentration_supported", bool(selected_row and _int(selected_row.get("missed_winner_count")) >= 50)),
                ("severe_loss_acceptable", bool(selected_row and _float(selected_row.get("severe_loss_rate20")) <= 0.26)),
                ("nonwinner_profile_acceptable_relative_to_alternative", bool(selected_row and _float(selected_row.get("selected_nonwinner_rate")) <= 0.70)),
                ("max3_fit_better_than_alternative", bool(selected_row and rows and selected_row.get("hypothesis_id") == rows[0].get("hypothesis_id"))),
                ("per_source_same_date_support_missing_but_not_faked", bool(selected_row and not selected_row.get("per_source_same_date_support_available"))),
            ]
            if present
        ],
        "rejected_hypotheses": rejected,
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_rejected_hypothesis_report(leaderboard: dict[str, Any], selection: dict[str, Any]) -> dict[str, Any]:
    selected_id = selection.get("selected_hypothesis_id")
    rows = []
    for row in leaderboard.get("rows") or []:
        if row.get("hypothesis_id") == selected_id:
            continue
        rows.append(
            {
                "hypothesis_id": row.get("hypothesis_id"),
                "source_family": row.get("source_family"),
                "failure_mode_checks": row.get("failure_mode_checks"),
                "validation_readiness_score_diagnostic_only": row.get("validation_readiness_score_diagnostic_only"),
                "reason": "same_date_support_missing_or_higher_risk" if "same_date_support_missing" in (row.get("failure_mode_checks") or []) else "lower_validation_readiness_score",
            }
        )
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_rejected_hypothesis_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "rows": rows,
        "rejected_count": len(rows),
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def _decision_class(selection: dict[str, Any], leaderboard: dict[str, Any]) -> str:
    if selection.get("selected") is True:
        return "source_mechanism_validation_next_target_ready"
    if any(row.get("testability") for row in leaderboard.get("rows") or []):
        return "source_mechanism_validation_hold"
    return "source_mechanism_validation_failed"


def build_next_axis_recommendation(selection: dict[str, Any], decision_class: str) -> dict[str, Any]:
    if decision_class == "source_mechanism_validation_next_target_ready":
        recommended = selection.get("selected_next_axis")
        reason = "one refreshed source mechanism is selected for the next fixed-condition source validation"
    elif decision_class == "source_mechanism_validation_hold":
        recommended = "candidate_generation_source_mechanism_same_date_repair_v1"
        reason = "both hypotheses need stronger per-source same-date evidence before source validation"
    else:
        recommended = "candidate_generation_hypothesis_map_refresh_followup_v1"
        reason = "no refreshed hypothesis remained testable after source mechanism validation"
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_next_axis_recommendation_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "one_recommended_next_axis_only": True,
        "recommended_next_axis": recommended,
        "reason": reason,
        "candidate_generation_challenger_created": False,
        "do_not_continue_axes": [
            "create challenger in this run",
            "create scorer",
            "tune threshold",
            "image or fusion",
            "production ranking",
            "MeeMee reflection",
            "rescue archived first source",
            "validate dropped second source",
        ],
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_contract_artifacts(*, source_dirs: dict[str, Path], status: dict[str, Any]) -> dict[str, dict[str, Any]]:
    refs = []
    for source_name, root in source_dirs.items():
        for path in sorted(root.glob("*.json")):
            refs.append({"source": source_name, "name": path.name, "path": str(path), "exists": path.exists(), "content_hash": _stable_hash(_load_json(path))})
        for path in sorted(root.glob("*.jsonl")):
            refs.append({"source": source_name, "name": path.name, "path": str(path), "exists": path.exists(), "file_hash": _file_hash(path)})
    evaluation_contract = {
        "schema_version": f"{SCHEMA_PREFIX}_evaluation_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "research_phase": "candidate_generation_source_mechanism_validation",
        "boundary": "TRADEX-only",
        "axis_moved": "candidate_generation_source_mechanism_validation",
        "diagnosis_only": True,
        "source_hypothesis_refresh_decision": status["hypothesis_refresh"]["research_decision.json"].get("authoritative_research_decision"),
        "candidate_generation_challenger_created": False,
        "candidate_scoring_created": False,
        "ranking_objective_created": False,
        "threshold_policy_created": False,
        "image_score_used": False,
        "fusion_reranker_created": False,
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
        "silent_fallback_used": False,
        "research_fallback_used": False,
    }
    evaluation_contract["contract_hash"] = _stable_hash(evaluation_contract)
    validation_contract = {
        "schema_version": f"{SCHEMA_PREFIX}_source_mechanism_validation_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "source_mechanism_validation_created": True,
        "select_at_most_one_next_validation_target": True,
        "do_not_create_candidate_generation_challenger": True,
        "do_not_create_scorer": True,
        "do_not_tune_thresholds": True,
        "do_not_use_image_or_fusion": True,
        "do_not_touch_meemee": True,
        "selected_next_validation_target_authoritative": "selected_next_validation_target.json",
    }
    validation_contract["contract_hash"] = _stable_hash(validation_contract)
    source_refs = {
        "schema_version": f"{SCHEMA_PREFIX}_source_artifact_refs_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "source_roots": {key: str(value) for key, value in source_dirs.items()},
        "refs": refs,
    }
    source_refs["contract_hash"] = _stable_hash(source_refs)
    return {
        "evaluation_contract.json": evaluation_contract,
        "source_artifact_refs.json": source_refs,
        "source_mechanism_validation_contract.json": validation_contract,
    }


def build_research_decision(*, selection: dict[str, Any], same_date: dict[str, Any], decision_class: str, artifact_complete: bool) -> dict[str, Any]:
    if artifact_complete and decision_class == "source_mechanism_validation_next_target_ready":
        decision = "keep_candidate"
        authoritative = "source_mechanism_validation_next_target_ready"
    elif artifact_complete and decision_class == "source_mechanism_validation_hold":
        decision = "hold"
        authoritative = "source_mechanism_validation_hold"
    else:
        decision = "drop"
        authoritative = "source_mechanism_validation_failed"
    typed_reasons = [
        "source_mechanism_validation_created",
        "selected_next_validation_target_created" if selection.get("selected") else "no_selected_next_validation_target",
        "per_source_same_date_support_available" if same_date.get("per_source_same_date_support_available") else "per_source_same_date_support_unavailable_not_faked",
        "diagnosis_only_no_challenger_created",
        "artifact_complete" if artifact_complete else "artifact_incomplete",
    ]
    return {
        "schema_version": f"{SCHEMA_PREFIX}_research_decision_v1",
        "generated_at": _utc_now(),
        "research_phase": "candidate_generation_source_mechanism_validation",
        "boundary": "TRADEX-only",
        "axis_moved": "candidate_generation_source_mechanism_validation",
        "source_hypothesis_refresh_decision": "hypothesis_map_refreshed_next_validation_ready",
        "source_mechanism_validation_created": True,
        "selected_next_validation_target_created": bool(selection.get("selected")),
        "selected_hypothesis_id": selection.get("selected_hypothesis_id"),
        "candidate_generation_challenger_created": False,
        "candidate_scoring_created": False,
        "ranking_objective_created": False,
        "threshold_policy_created": False,
        "image_score_used": False,
        "fusion_reranker_created": False,
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
        "safe_full_used_as_hard_filter": False,
        "negative_guard_used_as_hard_veto": False,
        "future_labels_used_for_diagnosis_only": True,
        "future_labels_used_in_score_inputs": False,
        "per_source_same_date_support_available": bool(same_date.get("per_source_same_date_support_available")),
        "silent_fallback_used": False,
        "research_fallback_used": False,
        "decision": decision,
        "authoritative_research_decision": authoritative,
        "typed_reasons": typed_reasons,
        "decision_classification": decision_class,
    }


def _artifact_complete(output_dir: Path, paths: dict[str, str], decision: dict[str, Any] | None = None) -> dict[str, Any]:
    excluded = {"_ARTIFACT_COMPLETE.json"}
    if decision is None:
        excluded.add("research_decision.json")
    required = {name: (output_dir / name).exists() for name in REQUIRED_ARTIFACTS if name not in excluded}
    return {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "artifact_root": str(output_dir),
        "complete": all(required.values()),
        "required_artifacts": required,
        "paths": paths,
        "decision": decision.get("decision") if decision else None,
        "authoritative_research_decision": decision.get("authoritative_research_decision") if decision else None,
        "source_mechanism_validation_created": True,
        "selected_next_validation_target_created": bool(decision and decision.get("selected_next_validation_target_created")),
        "candidate_generation_challenger_created": False,
        "candidate_scoring_created": False,
        "ranking_objective_created": False,
        "threshold_policy_created": False,
        "image_score_used": False,
        "fusion_reranker_created": False,
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
        "silent_fallback_used": False,
        "research_fallback_used": False,
    }


def run_candidate_generation_source_mechanism_validation_v1(
    *,
    source_hypothesis_refresh_run_id: str = DEFAULT_HYPOTHESIS_REFRESH_RUN_ID,
    source_second_hypothesis_review_run_id: str = DEFAULT_SECOND_REVIEW_RUN_ID,
    source_applicability_run_id: str = DEFAULT_APPLICABILITY_RUN_ID,
    source_validation_run_id: str = DEFAULT_VALIDATION_RUN_ID,
    source_missed_winner_run_id: str = DEFAULT_MISSED_WINNER_RUN_ID,
    source_root_cause_run_id: str = DEFAULT_ROOT_CAUSE_RUN_ID,
    source_wide_run_id: str = DEFAULT_WIDE_RUN_ID,
    hypothesis_refresh_root: str | Path = DEFAULT_HYPOTHESIS_REFRESH_ROOT,
    second_review_root: str | Path = DEFAULT_SECOND_REVIEW_ROOT,
    applicability_root: str | Path = DEFAULT_APPLICABILITY_ROOT,
    validation_root: str | Path = DEFAULT_VALIDATION_ROOT,
    missed_winner_root: str | Path = DEFAULT_MISSED_WINNER_ROOT,
    root_cause_root: str | Path = DEFAULT_ROOT_CAUSE_ROOT,
    wide_root: str | Path = DEFAULT_WIDE_ROOT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    run_id: str | None = None,
) -> dict[str, Any]:
    hypothesis_refresh_dir = _run_dir(hypothesis_refresh_root, source_hypothesis_refresh_run_id, DEFAULT_HYPOTHESIS_REFRESH_ROOT)
    second_review_dir = _run_dir(second_review_root, source_second_hypothesis_review_run_id, DEFAULT_SECOND_REVIEW_ROOT)
    applicability_dir = _run_dir(applicability_root, source_applicability_run_id, DEFAULT_APPLICABILITY_ROOT)
    validation_dir = _run_dir(validation_root, source_validation_run_id, DEFAULT_VALIDATION_ROOT)
    missed_winner_dir = _run_dir(missed_winner_root, source_missed_winner_run_id, DEFAULT_MISSED_WINNER_ROOT)
    root_cause_dir = _run_dir(root_cause_root, source_root_cause_run_id, DEFAULT_ROOT_CAUSE_ROOT)
    wide_dir = _run_dir(wide_root, source_wide_run_id, DEFAULT_WIDE_ROOT)
    output_dir = _safe_path(output_root, DEFAULT_OUTPUT_ROOT) / (run_id.strip() if isinstance(run_id, str) and run_id.strip() else _default_run_id())

    status = validate_sources(
        hypothesis_refresh_dir=hypothesis_refresh_dir,
        second_review_dir=second_review_dir,
        applicability_dir=applicability_dir,
        validation_dir=validation_dir,
        missed_winner_dir=missed_winner_dir,
        root_cause_dir=root_cause_dir,
        wide_dir=wide_dir,
    )
    source_dirs = {
        "hypothesis_refresh": hypothesis_refresh_dir,
        "second_review": second_review_dir,
        "applicability": applicability_dir,
        "validation": validation_dir,
        "missed_winner": missed_winner_dir,
        "root_cause": root_cause_dir,
        "wide": wide_dir,
    }
    contract_artifacts = build_contract_artifacts(source_dirs=source_dirs, status=status)
    readback = build_refreshed_hypothesis_readback(status)
    distinctness = build_archived_source_distinctness_audit(status)
    mechanism = build_hypothesis_source_mechanism_report(status)
    same_date = build_per_source_same_date_support_report(status=status, wide_dir=wide_dir)
    leaderboard = build_hypothesis_validation_readiness_leaderboard(mechanism_report=mechanism, distinctness=distinctness, same_date_report=same_date)
    selection = build_selected_next_validation_target(leaderboard)
    rejected = build_rejected_hypothesis_report(leaderboard, selection)
    decision_class = _decision_class(selection, leaderboard)
    next_axis = build_next_axis_recommendation(selection, decision_class)
    run_manifest = contracts.build_run_manifest(
        session_id=output_dir.name,
        seed=0,
        random_seed=0,
        input_artifacts=[{"name": key, "path": str(value)} for key, value in source_dirs.items()],
        asof="20260513",
        config={
            "axis_id": AXIS_ID,
            "diagnosis_only": True,
            "source_mechanism_validation_created": True,
            "candidate_generation_challenger_created": False,
            "candidate_scoring_created": False,
            "ranking_objective_created": False,
            "threshold_policy_created": False,
            "image_score_used": False,
            "fusion_reranker_created": False,
            "production_ranking_changed": False,
        },
        universe=[],
        period={"start_date": "20160509", "end_date": "20260513", "label": "candidate_generation_source_mechanism_validation"},
        horizon="20d",
        artifact_detail_level=contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        fallback_status=contracts.TRADEX_FALLBACK_STATUS_AUTHORITATIVE,
        cost_model=contracts.TRADEX_DEFAULT_COST_MODEL,
    )
    contracts.validate_run_manifest(run_manifest)

    paths: dict[str, str] = {}
    for name, payload in {
        **contract_artifacts,
        "run_manifest.json": run_manifest,
        "refreshed_hypothesis_readback.json": readback,
        "archived_source_distinctness_audit.json": distinctness,
        "hypothesis_source_mechanism_report.json": mechanism,
        "per_source_same_date_support_report.json": same_date,
        "hypothesis_validation_readiness_leaderboard.json": leaderboard,
        "selected_next_validation_target.json": selection,
        "rejected_hypothesis_report.json": rejected,
        "next_axis_recommendation.json": next_axis,
    }.items():
        paths[name] = str(_write_json(output_dir / name, payload))

    pre_complete = _artifact_complete(output_dir, paths)
    decision = build_research_decision(selection=selection, same_date=same_date, decision_class=decision_class, artifact_complete=bool(pre_complete["complete"]))
    paths["research_decision.json"] = str(_write_json(output_dir / "research_decision.json", decision))
    complete = _artifact_complete(output_dir, paths, decision)
    paths["_ARTIFACT_COMPLETE.json"] = str(_write_json(output_dir / "_ARTIFACT_COMPLETE.json", complete))
    return {
        "output_dir": str(output_dir),
        "decision": decision["decision"],
        "authoritative_research_decision": decision["authoritative_research_decision"],
        "decision_classification": decision_class,
        "selected": selection.get("selected"),
        "selected_hypothesis_id": selection.get("selected_hypothesis_id"),
        "selected_next_axis": selection.get("selected_next_axis"),
        "per_source_same_date_support_available": same_date.get("per_source_same_date_support_available"),
        "candidate_generation_challenger_created": False,
        "candidate_scoring_created": False,
        "ranking_objective_created": False,
        "threshold_policy_created": False,
        "image_score_used": False,
        "fusion_reranker_created": False,
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
        "silent_fallback_used": False,
        "research_fallback_used": False,
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-hypothesis-refresh-run-id", default=DEFAULT_HYPOTHESIS_REFRESH_RUN_ID)
    parser.add_argument("--source-second-hypothesis-review-run-id", default=DEFAULT_SECOND_REVIEW_RUN_ID)
    parser.add_argument("--source-applicability-run-id", default=DEFAULT_APPLICABILITY_RUN_ID)
    parser.add_argument("--source-validation-run-id", default=DEFAULT_VALIDATION_RUN_ID)
    parser.add_argument("--source-missed-winner-run-id", default=DEFAULT_MISSED_WINNER_RUN_ID)
    parser.add_argument("--source-root-cause-run-id", default=DEFAULT_ROOT_CAUSE_RUN_ID)
    parser.add_argument("--source-wide-run-id", default=DEFAULT_WIDE_RUN_ID)
    parser.add_argument("--hypothesis-refresh-root", default=str(DEFAULT_HYPOTHESIS_REFRESH_ROOT))
    parser.add_argument("--second-review-root", default=str(DEFAULT_SECOND_REVIEW_ROOT))
    parser.add_argument("--applicability-root", default=str(DEFAULT_APPLICABILITY_ROOT))
    parser.add_argument("--validation-root", default=str(DEFAULT_VALIDATION_ROOT))
    parser.add_argument("--missed-winner-root", default=str(DEFAULT_MISSED_WINNER_ROOT))
    parser.add_argument("--root-cause-root", default=str(DEFAULT_ROOT_CAUSE_ROOT))
    parser.add_argument("--wide-root", default=str(DEFAULT_WIDE_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--run-id", default="")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    result = run_candidate_generation_source_mechanism_validation_v1(
        source_hypothesis_refresh_run_id=args.source_hypothesis_refresh_run_id,
        source_second_hypothesis_review_run_id=args.source_second_hypothesis_review_run_id,
        source_applicability_run_id=args.source_applicability_run_id,
        source_validation_run_id=args.source_validation_run_id,
        source_missed_winner_run_id=args.source_missed_winner_run_id,
        source_root_cause_run_id=args.source_root_cause_run_id,
        source_wide_run_id=args.source_wide_run_id,
        hypothesis_refresh_root=args.hypothesis_refresh_root,
        second_review_root=args.second_review_root,
        applicability_root=args.applicability_root,
        validation_root=args.validation_root,
        missed_winner_root=args.missed_winner_root,
        root_cause_root=args.root_cause_root,
        wide_root=args.wide_root,
        output_root=args.output_root,
        run_id=args.run_id,
    )
    print(_json_text(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
