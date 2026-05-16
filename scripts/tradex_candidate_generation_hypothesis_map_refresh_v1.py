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


AXIS_ID = "candidate_generation_hypothesis_map_refresh_v1"
SCHEMA_PREFIX = "tradex_candidate_generation_hypothesis_map_refresh_v1"

DEFAULT_SECOND_REVIEW_RUN_ID = "20260513T170000Z-candidate-generation-hypothesis-map-second-hypothesis-review-v1"
DEFAULT_APPLICABILITY_RUN_ID = "20260513T160000Z-source-specific-timeblock-applicability-audit-v1"
DEFAULT_VALIDATION_RUN_ID = "20260513T150000Z-source-specific-candidate-generation-validation-v1"
DEFAULT_MISSED_WINNER_RUN_ID = "20260513T140000Z-missed-winner-event-source-candidate-generation-v1"
DEFAULT_ROOT_CAUSE_RUN_ID = "20260513T130000Z-oracle-gap-and-candidate-generation-root-cause-v1"
DEFAULT_WIDE_RUN_ID = "20260513T030000Z-wide-strength-pool-upside-rerank-v1"

DEFAULT_SECOND_REVIEW_ROOT = Path(r"G:\Tradex\candidate_generation_hypothesis_map_second_hypothesis_review_v1")
DEFAULT_APPLICABILITY_ROOT = Path(r"G:\Tradex\source_specific_timeblock_applicability_audit_v1")
DEFAULT_VALIDATION_ROOT = Path(r"G:\Tradex\source_specific_candidate_generation_validation_v1")
DEFAULT_MISSED_WINNER_ROOT = Path(r"G:\Tradex\missed_winner_event_source_candidate_generation_v1")
DEFAULT_ROOT_CAUSE_ROOT = Path(r"G:\Tradex\oracle_gap_and_candidate_generation_root_cause_v1")
DEFAULT_WIDE_ROOT = Path(r"G:\Tradex\wide_strength_pool_upside_rerank_v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\candidate_generation_hypothesis_map_refresh_v1")

FIRST_ARCHIVED_SOURCE = (
    "pre_ma20_path_state=pre_ma20_reclaim_base|"
    "pre_ma60_context_state=pre_ma60_near_or_above|"
    "weekly_prior_state=weekly_prior_uptrend|"
    "negative_guard_match=False"
)
SECOND_DROPPED_SOURCE = (
    "pre_ma20_path_state=pre_ma20_reclaim_base|"
    "pre_ma60_context_state=pre_ma60_near_or_above|"
    "weekly_prior_state=weekly_prior_uptrend|"
    "negative_guard_match=True"
)
ARCHIVED_SOURCES = (FIRST_ARCHIVED_SOURCE, SECOND_DROPPED_SOURCE)

REQUIRED_ARTIFACTS = (
    "evaluation_contract.json",
    "run_manifest.json",
    "source_artifact_refs.json",
    "hypothesis_refresh_contract.json",
    "archived_source_failure_summary.json",
    "remaining_source_scan_report.json",
    "mechanism_diversity_report.json",
    "refreshed_candidate_generation_hypothesis_map.json",
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


def _parse_source_family(source_family: str) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for part in str(source_family).split("|"):
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        parsed[key] = value
    return parsed


def _shared_tag_rate(source_family: str, reference_family: str) -> float:
    source = _parse_source_family(source_family)
    reference = _parse_source_family(reference_family)
    if not source:
        return 0.0
    shared = sum(1 for key, value in source.items() if reference.get(key) == value)
    return shared / len(source)


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


def validate_sources(
    *,
    second_review_dir: Path,
    applicability_dir: Path,
    validation_dir: Path,
    missed_winner_dir: Path,
    root_cause_dir: Path,
    wide_dir: Path,
) -> dict[str, Any]:
    required_by_source = {
        "second_review": [
            "_ARTIFACT_COMPLETE.json",
            "research_decision.json",
            "hypothesis_distinctness_audit.json",
            "second_hypothesis_failure_mode_risk_report.json",
            "second_hypothesis_profile.json",
            "first_hypothesis_archive_context.json",
        ],
        "applicability": [
            "_ARTIFACT_COMPLETE.json",
            "research_decision.json",
            "source_archive_or_refine_decision.json",
            "point_in_time_applicability_proxy_report.json",
        ],
        "validation": [
            "_ARTIFACT_COMPLETE.json",
            "research_decision.json",
            "validation_outcome_classification.json",
            "baseline_comparison_report.json",
        ],
        "missed_winner": [
            "_ARTIFACT_COMPLETE.json",
            "research_decision.json",
            "candidate_generation_hypothesis_map.json",
            "event_source_quality_leaderboard.json",
            "missed_winner_source_decomposition.json",
            "selected_nonwinner_source_decomposition.json",
            "same_date_source_miss_report.json",
            "time_block_source_stability.json",
            "max3_source_structure_report.json",
        ],
        "root_cause": [
            "_ARTIFACT_COMPLETE.json",
            "research_decision.json",
            "candidate_generation_hypothesis_map.json",
            "failure_mode_classification.json",
            "max3_deployment_fit_report.json",
        ],
        "wide": [
            "_ARTIFACT_COMPLETE.json",
            "research_decision.json",
            "missed_winner_selection_report.json",
            "ranking_coverage_audit.json",
        ],
    }
    dirs = {
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

    second_decision = status["second_review"]["research_decision.json"]
    if second_decision.get("authoritative_research_decision") != "second_hypothesis_drop":
        raise RuntimeError("second review source is not a second_hypothesis_drop decision")
    if second_decision.get("decision_classification") != "second_hypothesis_drop_or_skip":
        raise RuntimeError("second review source is not classified as drop_or_skip")
    if status["validation"]["research_decision.json"].get("authoritative_research_decision") != "source_specific_candidate_generation_drop":
        raise RuntimeError("first source validation source is not a frozen drop")
    if status["applicability"]["research_decision.json"].get("authoritative_research_decision") != "source_applicability_hold":
        raise RuntimeError("first source applicability source is not hold")
    return status


def _source_rows(status: dict[str, Any]) -> list[dict[str, Any]]:
    rows = status["missed_winner"]["event_source_quality_leaderboard.json"].get("rows") or []
    return [dict(row) for row in rows if isinstance(row, dict) and row.get("event_source")]


def _time_block_stability_by_source(status: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = status["missed_winner"]["time_block_source_stability.json"].get("rows") or []
    by_source: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if not isinstance(row, dict) or not row.get("event_source"):
            continue
        by_source.setdefault(str(row["event_source"]), []).append(dict(row))
    result: dict[str, dict[str, Any]] = {}
    for source, source_rows in by_source.items():
        meaningful = [row for row in source_rows if _int(row.get("sample_count")) >= 5]
        stable = [
            row
            for row in meaningful
            if _float(row.get("future_winner_rate")) >= 0.15 and _float(row.get("severe_loss_rate20")) <= 0.35
        ]
        result[source] = {
            "time_block_count": len(meaningful),
            "stable_time_block_count": len(stable),
            "time_block_stability": len(stable) / len(meaningful) if meaningful else 0.0,
            "rows": meaningful[:12],
        }
    return result


def _max3_fit_by_source(status: dict[str, Any]) -> dict[str, dict[str, Any]]:
    report = status["missed_winner"]["max3_source_structure_report.json"]
    rows = report.get("source_mix_overfill_rows") or []
    by_source: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        source_mix = str(row.get("source_mix") or "")
        for source in source_mix.split("+"):
            if source:
                by_source.setdefault(source, []).append(dict(row))
    result: dict[str, dict[str, Any]] = {}
    for source, source_rows in by_source.items():
        day_count = sum(_int(row.get("day_count")) for row in source_rows)
        overfill_count = sum(_int(row.get("overfill_day_count")) for row in source_rows)
        result[source] = {
            "source_mix_row_count": len(source_rows),
            "source_mix_day_count": day_count,
            "source_mix_overfill_day_count": overfill_count,
            "max3_structure_fit": overfill_count / day_count if day_count else 0.0,
            "top_source_mix_rows": source_rows[:8],
        }
    return result


def _classify_mechanisms(row: dict[str, Any], *, stability: dict[str, Any], max3_fit: dict[str, Any]) -> list[str]:
    mechanisms: list[str] = []
    missed = _int(row.get("missed_winner_count"))
    capture_rate = _float(row.get("selected_capture_rate_among_source_winners"))
    severe = _float(row.get("severe_loss_rate20"))
    nonwinner = _float(row.get("selected_nonwinner_rate"))
    future_rate = _float(row.get("future_winner_rate"))
    avg_mfe = _float(row.get("avg_MFE20"))
    avg_ret = _float(row.get("avg_ret20"))
    max3 = _float(max3_fit.get("max3_structure_fit"))
    time_stability = _float(stability.get("time_block_stability"))

    if missed >= 10 and capture_rate <= 0.35:
        mechanisms.append("source_under_ranked")
    if missed >= 5 and max3 >= 0.65:
        mechanisms.append("source_not_selected_due_to_max3_overfill")
    if missed >= 5 and 0 < time_stability < 0.70:
        mechanisms.append("source_regime_specific")
    if missed >= 5 and avg_mfe >= 0.08 and avg_ret <= 0.015:
        mechanisms.append("source_label_mismatch_high_MFE_low_ret20")
    if missed >= 5 and severe <= 0.20 and nonwinner <= 0.65:
        mechanisms.append("risk_return_tradeoff_reduction")
    if missed >= 20 and future_rate >= 0.18 and capture_rate <= 0.35:
        mechanisms.append("pool_recall_gap_proxy")
    return mechanisms or ["insufficient_distinct_mechanism"]


def build_archived_source_failure_summary(status: dict[str, Any]) -> dict[str, Any]:
    distinctness = status["second_review"]["hypothesis_distinctness_audit.json"]
    risk = status["second_review"]["second_hypothesis_failure_mode_risk_report.json"]
    validation = status["validation"]["research_decision.json"]
    applicability = status["applicability"]["research_decision.json"]
    proxy = status["applicability"]["point_in_time_applicability_proxy_report.json"]
    second_profile = status["second_review"]["second_hypothesis_profile.json"]
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_archived_source_failure_summary_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "first_source": FIRST_ARCHIVED_SOURCE,
        "second_source": SECOND_DROPPED_SOURCE,
        "first_hypothesis_archived": True,
        "second_hypothesis_dropped": True,
        "first_source_failure_mode": {
            "validation_decision": validation.get("authoritative_research_decision"),
            "validation_drop": validation.get("authoritative_research_decision") == "source_specific_candidate_generation_drop",
            "applicability_decision": applicability.get("authoritative_research_decision"),
            "timeblock_audit_hold": applicability.get("authoritative_research_decision") == "source_applicability_hold",
            "point_in_time_proxy_found": proxy.get("point_in_time_proxy_found"),
            "practical_status": "archive_do_not_rescue",
        },
        "second_source_failure_mode": {
            "second_hypothesis_decision": status["second_review"]["research_decision.json"].get("authoritative_research_decision"),
            "decision_classification": status["second_review"]["research_decision.json"].get("decision_classification"),
            "same_failure_mode_risk": risk.get("same_failure_mode_risk"),
            "severe_loss_rate20": second_profile.get("severe_loss_rate20"),
            "selected_nonwinner_rate": second_profile.get("selected_nonwinner_rate"),
            "practical_status": "drop_do_not_validate",
        },
        "shared_tags": distinctness.get("shared_tags"),
        "shared_tag_rate": distinctness.get("shared_tag_rate"),
        "failed_mechanism": "pre_ma20_reclaim_near_ma60_weekly_uptrend_source_promotion",
        "why_simple_source_promotion_failed": [
            "first source validation dropped under source-specific generation",
            "first source timeblock rescue lacked a point-in-time proxy",
            "second source mainly changed negative_guard_match while sharing most source tags",
            "second source carried high same-failure risk and high severe/nonwinner risk",
        ],
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_remaining_source_scan_report(status: dict[str, Any]) -> dict[str, Any]:
    stability_by_source = _time_block_stability_by_source(status)
    max3_by_source = _max3_fit_by_source(status)
    same_date = status["missed_winner"]["same_date_source_miss_report.json"]
    rows: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for row in _source_rows(status):
        source = str(row["event_source"])
        share_rates = {archived: _shared_tag_rate(source, archived) for archived in ARCHIVED_SOURCES}
        max_share = max(share_rates.values()) if share_rates else 0.0
        tags = _parse_source_family(source)
        archived = source in ARCHIVED_SOURCES
        near_duplicate = max_share > 0.60
        calendar_only = str(tags.get("weekly_prior_state") or "").endswith("unknown") or _int(row.get("day_count")) <= 1
        stability = stability_by_source.get(source, {})
        max3_fit = max3_by_source.get(source, {})
        mechanisms = _classify_mechanisms(row, stability=stability, max3_fit=max3_fit)
        explicitly_different_mechanism = bool(set(mechanisms) - {"source_under_ranked", "insufficient_distinct_mechanism"})
        excluded = archived or calendar_only or (near_duplicate and not explicitly_different_mechanism)
        scan_row = {
            "event_source": source,
            "source_tags": tags,
            "missed_winner_count": _int(row.get("missed_winner_count")),
            "future_winner_rate": _float(row.get("future_winner_rate")),
            "severe_loss_rate20": _float(row.get("severe_loss_rate20")),
            "selected_nonwinner_rate": _float(row.get("selected_nonwinner_rate")),
            "time_block_stability": stability.get("time_block_stability", 0.0),
            "same_date_source_miss_support": None,
            "same_date_source_miss_support_available": False,
            "same_date_source_miss_support_basis": "same_date_source_miss_report has aggregate miss support but no per-source rows",
            "max3_structure_fit": max3_fit.get("max3_structure_fit", 0.0),
            "selected_capture_rate_among_source_winners": _float(row.get("selected_capture_rate_among_source_winners")),
            "sample_count": _int(row.get("sample_count")),
            "shared_tag_rate_vs_archived_max": max_share,
            "shared_tag_rates_vs_archived": share_rates,
            "mechanisms": mechanisms,
            "excluded": excluded,
            "exclusion_reasons": [
                reason
                for reason, present in [
                    ("archived_or_dropped_source", archived),
                    ("calendar_only_or_too_small_day_count", calendar_only),
                    ("near_duplicate_without_explicitly_different_mechanism", near_duplicate and not explicitly_different_mechanism),
                ]
                if present
            ],
        }
        quality_score = (
            scan_row["missed_winner_count"] * 0.01
            + scan_row["future_winner_rate"]
            + scan_row["max3_structure_fit"] * 0.10
            + scan_row["time_block_stability"] * 0.08
            - scan_row["severe_loss_rate20"] * 1.25
            - scan_row["selected_nonwinner_rate"] * 0.35
            - scan_row["shared_tag_rate_vs_archived_max"] * 0.12
        )
        scan_row["refresh_quality_score_diagnostic_only"] = quality_score
        if excluded:
            rejected.append(scan_row)
        else:
            rows.append(scan_row)
    rows.sort(key=lambda item: item["refresh_quality_score_diagnostic_only"], reverse=True)
    rejected.sort(key=lambda item: item["refresh_quality_score_diagnostic_only"], reverse=True)
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_remaining_source_scan_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "archived_sources": list(ARCHIVED_SOURCES),
        "excluded_archived_and_near_duplicate_families": True,
        "ranking_metrics": [
            "missed_winner_count",
            "future_winner_rate",
            "severe_loss_rate20",
            "selected_nonwinner_rate",
            "time_block_stability",
            "same_date_source_miss_support",
            "max3_structure_fit",
        ],
        "same_date_source_miss_report_summary": {
            "winner_available_day_count": same_date.get("winner_available_day_count"),
            "winner_source_present_but_under_ranked_rate": same_date.get("winner_source_present_but_under_ranked_rate"),
            "source_mismatch_explains_miss_rate": same_date.get("source_mismatch_explains_miss_rate"),
            "per_source_rows_available": False,
        },
        "rows": rows,
        "rejected_rows": rejected,
        "remaining_count": len(rows),
        "rejected_count": len(rejected),
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_mechanism_diversity_report(scan: dict[str, Any]) -> dict[str, Any]:
    buckets = {
        "source_under_ranked": [],
        "source_not_selected_due_to_max3_overfill": [],
        "source_regime_specific": [],
        "source_label_mismatch_high_MFE_low_ret20": [],
        "risk_return_tradeoff_reduction": [],
        "pool_recall_gap_proxy": [],
        "insufficient_distinct_mechanism": [],
    }
    for row in scan.get("rows") or []:
        for mechanism in row.get("mechanisms") or []:
            buckets.setdefault(mechanism, []).append(row)
    bucket_summaries = {
        mechanism: {
            "candidate_count": len(rows),
            "top_sources": [
                {
                    "event_source": row["event_source"],
                    "missed_winner_count": row["missed_winner_count"],
                    "future_winner_rate": row["future_winner_rate"],
                    "severe_loss_rate20": row["severe_loss_rate20"],
                    "selected_nonwinner_rate": row["selected_nonwinner_rate"],
                    "refresh_quality_score_diagnostic_only": row["refresh_quality_score_diagnostic_only"],
                }
                for row in rows[:5]
            ],
        }
        for mechanism, rows in buckets.items()
    }
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_mechanism_diversity_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "mechanism_buckets": bucket_summaries,
        "distinct_mechanism_count": sum(1 for key, value in bucket_summaries.items() if key != "insufficient_distinct_mechanism" and value["candidate_count"] > 0),
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def _same_failure_risk(row: dict[str, Any]) -> str:
    if row["shared_tag_rate_vs_archived_max"] >= 0.75:
        return "high"
    if row["shared_tag_rate_vs_archived_max"] > 0.60 and "source_under_ranked" in row.get("mechanisms", []):
        return "high"
    if row["shared_tag_rate_vs_archived_max"] >= 0.50 and _float(row.get("severe_loss_rate20")) >= 0.30:
        return "medium"
    return "low"


def build_refreshed_candidate_generation_hypothesis_map(scan: dict[str, Any]) -> dict[str, Any]:
    hypotheses: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    used_mechanisms: set[str] = set()
    for row in scan.get("rows") or []:
        mechanisms = [item for item in row.get("mechanisms", []) if item != "insufficient_distinct_mechanism"]
        primary_mechanism = mechanisms[0] if mechanisms else "insufficient_distinct_mechanism"
        same_failure_risk = _same_failure_risk(row)
        severe = _float(row.get("severe_loss_rate20"))
        nonwinner = _float(row.get("selected_nonwinner_rate"))
        testable = (
            row["missed_winner_count"] >= 20
            and row["sample_count"] >= 100
            and row["future_winner_rate"] >= 0.18
            and severe <= 0.26
            and same_failure_risk != "high"
            and primary_mechanism != "insufficient_distinct_mechanism"
        )
        if primary_mechanism in used_mechanisms and len(hypotheses) >= 1:
            testable = False
        candidate = {
            "hypothesis_id": f"candidate_generation_map_refresh_hypothesis_{len(hypotheses) + 1}",
            "source_family": row["event_source"],
            "setup_definition": row["source_tags"],
            "target_failure_mode": primary_mechanism,
            "expected_mechanism": primary_mechanism,
            "why_distinct_from_archived_sources": {
                "shared_tag_rate_vs_archived_max": row["shared_tag_rate_vs_archived_max"],
                "mechanisms": row["mechanisms"],
                "archived_mechanism": "pre_ma20_reclaim_near_ma60_weekly_uptrend_source_promotion",
            },
            "risk_profile": {
                "same_failure_risk": same_failure_risk,
                "severe_loss_rate20": severe,
                "selected_nonwinner_rate": nonwinner,
                "future_winner_rate": row["future_winner_rate"],
                "missed_winner_count": row["missed_winner_count"],
            },
            "next_validation_runner_name": "tradex_candidate_generation_source_mechanism_validation_v1.py",
            "required_validation_scope": "fixed_conditions_source_generation_only_no_scorer_threshold_image_fusion",
            "future_labels_used_for_diagnosis_only": True,
            "future_labels_used_in_score_inputs": False,
        }
        rejection_reasons = [
            reason
            for reason, present in [
                ("missed_winner_count_too_low", row["missed_winner_count"] < 20),
                ("sample_count_too_low", row["sample_count"] < 100),
                ("future_winner_rate_too_low", row["future_winner_rate"] < 0.18),
                ("severe_loss_rate_too_high", severe > 0.26),
                ("same_failure_risk_high", same_failure_risk == "high"),
                ("mechanism_not_distinct", primary_mechanism == "insufficient_distinct_mechanism"),
                ("mechanism_already_represented", primary_mechanism in used_mechanisms and len(hypotheses) >= 1),
            ]
            if present
        ]
        if testable and len(hypotheses) < 2:
            hypotheses.append(candidate)
            used_mechanisms.add(primary_mechanism)
        else:
            rejected.append({"event_source": row["event_source"], "rejection_reasons": rejection_reasons, "candidate_profile": candidate})
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_refreshed_candidate_generation_hypothesis_map_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "candidate_generation_challenger_created": False,
        "candidate_scoring_created": False,
        "threshold_policy_created": False,
        "production_ranking_changed": False,
        "meemee_reflectable": False,
        "hypotheses": hypotheses,
        "hypothesis_count": len(hypotheses),
        "rejected_candidate_count": len(rejected),
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_rejected_hypothesis_report(scan: dict[str, Any], refreshed_map: dict[str, Any]) -> dict[str, Any]:
    kept_sources = {row["source_family"] for row in refreshed_map.get("hypotheses") or []}
    rejected = []
    for row in (scan.get("rejected_rows") or []) + (scan.get("rows") or []):
        if row["event_source"] in kept_sources:
            continue
        rejected.append(
            {
                "event_source": row["event_source"],
                "rejection_reasons": row.get("exclusion_reasons") or [
                    reason
                    for reason, present in [
                        ("insufficient_missed_winner_count", row.get("missed_winner_count", 0) < 20),
                        ("severe_loss_or_nonwinner_profile_unacceptable", row.get("severe_loss_rate20", 0) > 0.26 or row.get("selected_nonwinner_rate", 0) > 0.70),
                        ("same_failure_risk_not_low", _same_failure_risk(row) != "low"),
                    ]
                    if present
                ],
                "mechanisms": row.get("mechanisms"),
                "shared_tag_rate_vs_archived_max": row.get("shared_tag_rate_vs_archived_max"),
                "severe_loss_rate20": row.get("severe_loss_rate20"),
                "selected_nonwinner_rate": row.get("selected_nonwinner_rate"),
            }
        )
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_rejected_hypothesis_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "rejected_count": len(rejected),
        "rows": rejected,
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def _decision_class(refreshed_map: dict[str, Any]) -> str:
    hypotheses = refreshed_map.get("hypotheses") or []
    if not hypotheses:
        return "hypothesis_map_refresh_failed"
    if any(row.get("risk_profile", {}).get("same_failure_risk") == "medium" for row in hypotheses):
        return "hypothesis_map_refresh_hold"
    return "hypothesis_map_refreshed_next_validation_ready"


def build_next_axis_recommendation(refreshed_map: dict[str, Any], decision_class: str) -> dict[str, Any]:
    hypotheses = refreshed_map.get("hypotheses") or []
    if decision_class == "hypothesis_map_refreshed_next_validation_ready" and hypotheses:
        recommended = hypotheses[0]["next_validation_runner_name"].removesuffix(".py")
        reason = "top refreshed source mechanism is distinct, testable, and not high same-failure risk"
    elif decision_class == "hypothesis_map_refresh_hold" and hypotheses:
        recommended = "candidate_generation_hypothesis_map_refresh_followup_diagnosis_v1"
        reason = "refreshed mechanism exists but needs extra diagnosis before validation"
    else:
        recommended = "candidate_generation_hypothesis_inventory_reset_v1"
        reason = "no testable refreshed source hypothesis remained after archive and near-duplicate exclusions"
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_next_axis_recommendation_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "one_recommended_next_axis_only": True,
        "recommended_next_axis": recommended,
        "reason": reason,
        "candidate_generation_challenger_created": False,
        "do_not_continue_axes": [
            "rescue first source",
            "validate second source",
            "negative_guard true false variant",
            "learned scorer",
            "threshold policy",
            "image fusion",
            "production ranking",
        ],
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_contract_artifacts(*, source_dirs: dict[str, Path], status: dict[str, Any]) -> dict[str, dict[str, Any]]:
    refs = []
    for source_name, root in source_dirs.items():
        for path in sorted(root.glob("*.json")):
            refs.append(
                {
                    "source": source_name,
                    "name": path.name,
                    "path": str(path),
                    "exists": path.exists(),
                    "content_hash": _stable_hash(_load_json(path)),
                }
            )
        for path in sorted(root.glob("*.jsonl")):
            refs.append({"source": source_name, "name": path.name, "path": str(path), "exists": path.exists(), "file_hash": _file_hash(path)})
    evaluation_contract = {
        "schema_version": f"{SCHEMA_PREFIX}_evaluation_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "research_phase": "candidate_generation_hypothesis_map_refresh",
        "boundary": "TRADEX-only",
        "axis_moved": "candidate_generation_hypothesis_map_refresh",
        "diagnosis_only": True,
        "fixed_evaluation_conditions": {
            "same_universe": True,
            "same_period": True,
            "same_top_k": True,
            "same_regime_condition": True,
            "same_cost_slippage": True,
            "same_artifact_detail_level": True,
        },
        "source_second_hypothesis_decision": status["second_review"]["research_decision.json"].get("authoritative_research_decision"),
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
    refresh_contract = {
        "schema_version": f"{SCHEMA_PREFIX}_hypothesis_refresh_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "archive_first_source": FIRST_ARCHIVED_SOURCE,
        "drop_second_source": SECOND_DROPPED_SOURCE,
        "exclude_negative_guard_only_variants": True,
        "near_duplicate_shared_tag_limit": 0.60,
        "generate_hypothesis_count_min": 1,
        "generate_hypothesis_count_max": 2,
        "diagnosis_only": True,
        "no_source_validation": True,
        "no_scorer": True,
        "no_threshold": True,
        "no_image_or_fusion": True,
        "no_meemee_touch": True,
    }
    refresh_contract["contract_hash"] = _stable_hash(refresh_contract)
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
        "hypothesis_refresh_contract.json": refresh_contract,
    }


def build_research_decision(*, refreshed_map: dict[str, Any], decision_class: str, artifact_complete: bool) -> dict[str, Any]:
    hypothesis_count = len(refreshed_map.get("hypotheses") or [])
    if artifact_complete and decision_class == "hypothesis_map_refreshed_next_validation_ready":
        decision = "keep_candidate"
        authoritative = "hypothesis_map_refreshed_next_validation_ready"
    elif artifact_complete and decision_class == "hypothesis_map_refresh_hold":
        decision = "hold"
        authoritative = "hypothesis_map_refresh_hold"
    else:
        decision = "drop"
        authoritative = "hypothesis_map_refresh_failed"
    typed_reasons = [
        "first_source_archived",
        "second_source_dropped",
        f"refreshed_hypothesis_count_{hypothesis_count}",
        "diagnosis_only_no_challenger_created",
        "no_scorer_threshold_image_fusion_or_meemee_change",
        "artifact_complete" if artifact_complete else "artifact_incomplete",
    ]
    if decision_class == "hypothesis_map_refresh_failed":
        typed_reasons.append("no_testable_distinct_hypothesis_remaining")
    return {
        "schema_version": f"{SCHEMA_PREFIX}_research_decision_v1",
        "generated_at": _utc_now(),
        "research_phase": "candidate_generation_hypothesis_map_refresh",
        "boundary": "TRADEX-only",
        "axis_moved": "candidate_generation_hypothesis_map_refresh",
        "source_second_hypothesis_decision": "second_hypothesis_drop",
        "first_hypothesis_archived": True,
        "second_hypothesis_dropped": True,
        "hypothesis_map_refreshed": bool(hypothesis_count > 0),
        "refreshed_hypothesis_count": hypothesis_count,
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
        "first_hypothesis_archived": True,
        "second_hypothesis_dropped": True,
        "hypothesis_map_refreshed": bool(decision and decision.get("hypothesis_map_refreshed")),
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


def run_candidate_generation_hypothesis_map_refresh_v1(
    *,
    source_second_hypothesis_review_run_id: str = DEFAULT_SECOND_REVIEW_RUN_ID,
    source_applicability_run_id: str = DEFAULT_APPLICABILITY_RUN_ID,
    source_validation_run_id: str = DEFAULT_VALIDATION_RUN_ID,
    source_missed_winner_run_id: str = DEFAULT_MISSED_WINNER_RUN_ID,
    source_root_cause_run_id: str = DEFAULT_ROOT_CAUSE_RUN_ID,
    source_wide_run_id: str = DEFAULT_WIDE_RUN_ID,
    second_review_root: str | Path = DEFAULT_SECOND_REVIEW_ROOT,
    applicability_root: str | Path = DEFAULT_APPLICABILITY_ROOT,
    validation_root: str | Path = DEFAULT_VALIDATION_ROOT,
    missed_winner_root: str | Path = DEFAULT_MISSED_WINNER_ROOT,
    root_cause_root: str | Path = DEFAULT_ROOT_CAUSE_ROOT,
    wide_root: str | Path = DEFAULT_WIDE_ROOT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    run_id: str | None = None,
) -> dict[str, Any]:
    second_review_dir = _run_dir(second_review_root, source_second_hypothesis_review_run_id, DEFAULT_SECOND_REVIEW_ROOT)
    applicability_dir = _run_dir(applicability_root, source_applicability_run_id, DEFAULT_APPLICABILITY_ROOT)
    validation_dir = _run_dir(validation_root, source_validation_run_id, DEFAULT_VALIDATION_ROOT)
    missed_winner_dir = _run_dir(missed_winner_root, source_missed_winner_run_id, DEFAULT_MISSED_WINNER_ROOT)
    root_cause_dir = _run_dir(root_cause_root, source_root_cause_run_id, DEFAULT_ROOT_CAUSE_ROOT)
    wide_dir = _run_dir(wide_root, source_wide_run_id, DEFAULT_WIDE_ROOT)
    output_dir = _safe_path(output_root, DEFAULT_OUTPUT_ROOT) / (run_id.strip() if isinstance(run_id, str) and run_id.strip() else _default_run_id())

    status = validate_sources(
        second_review_dir=second_review_dir,
        applicability_dir=applicability_dir,
        validation_dir=validation_dir,
        missed_winner_dir=missed_winner_dir,
        root_cause_dir=root_cause_dir,
        wide_dir=wide_dir,
    )
    source_dirs = {
        "second_review": second_review_dir,
        "applicability": applicability_dir,
        "validation": validation_dir,
        "missed_winner": missed_winner_dir,
        "root_cause": root_cause_dir,
        "wide": wide_dir,
    }
    contract_artifacts = build_contract_artifacts(source_dirs=source_dirs, status=status)
    archive_summary = build_archived_source_failure_summary(status)
    scan = build_remaining_source_scan_report(status)
    diversity = build_mechanism_diversity_report(scan)
    refreshed_map = build_refreshed_candidate_generation_hypothesis_map(scan)
    rejected = build_rejected_hypothesis_report(scan, refreshed_map)
    decision_class = _decision_class(refreshed_map)
    next_axis = build_next_axis_recommendation(refreshed_map, decision_class)
    run_manifest = contracts.build_run_manifest(
        session_id=output_dir.name,
        seed=0,
        random_seed=0,
        input_artifacts=[{"name": key, "path": str(value)} for key, value in source_dirs.items()],
        asof="20260513",
        config={
            "axis_id": AXIS_ID,
            "diagnosis_only": True,
            "first_hypothesis_archived": True,
            "second_hypothesis_dropped": True,
            "candidate_generation_challenger_created": False,
            "candidate_scoring_created": False,
            "ranking_objective_created": False,
            "threshold_policy_created": False,
            "image_score_used": False,
            "fusion_reranker_created": False,
            "production_ranking_changed": False,
        },
        universe=[],
        period={"start_date": "20160509", "end_date": "20260513", "label": "candidate_generation_hypothesis_map_refresh"},
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
        "archived_source_failure_summary.json": archive_summary,
        "remaining_source_scan_report.json": scan,
        "mechanism_diversity_report.json": diversity,
        "refreshed_candidate_generation_hypothesis_map.json": refreshed_map,
        "rejected_hypothesis_report.json": rejected,
        "next_axis_recommendation.json": next_axis,
    }.items():
        paths[name] = str(_write_json(output_dir / name, payload))

    pre_complete = _artifact_complete(output_dir, paths)
    decision = build_research_decision(refreshed_map=refreshed_map, decision_class=decision_class, artifact_complete=bool(pre_complete["complete"]))
    paths["research_decision.json"] = str(_write_json(output_dir / "research_decision.json", decision))
    complete = _artifact_complete(output_dir, paths, decision)
    paths["_ARTIFACT_COMPLETE.json"] = str(_write_json(output_dir / "_ARTIFACT_COMPLETE.json", complete))
    return {
        "output_dir": str(output_dir),
        "decision": decision["decision"],
        "authoritative_research_decision": decision["authoritative_research_decision"],
        "decision_classification": decision_class,
        "refreshed_hypothesis_count": decision["refreshed_hypothesis_count"],
        "recommended_next_axis": next_axis.get("recommended_next_axis"),
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
    parser.add_argument("--source-second-hypothesis-review-run-id", default=DEFAULT_SECOND_REVIEW_RUN_ID)
    parser.add_argument("--source-applicability-run-id", default=DEFAULT_APPLICABILITY_RUN_ID)
    parser.add_argument("--source-validation-run-id", default=DEFAULT_VALIDATION_RUN_ID)
    parser.add_argument("--source-missed-winner-run-id", default=DEFAULT_MISSED_WINNER_RUN_ID)
    parser.add_argument("--source-root-cause-run-id", default=DEFAULT_ROOT_CAUSE_RUN_ID)
    parser.add_argument("--source-wide-run-id", default=DEFAULT_WIDE_RUN_ID)
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
    result = run_candidate_generation_hypothesis_map_refresh_v1(
        source_second_hypothesis_review_run_id=args.source_second_hypothesis_review_run_id,
        source_applicability_run_id=args.source_applicability_run_id,
        source_validation_run_id=args.source_validation_run_id,
        source_missed_winner_run_id=args.source_missed_winner_run_id,
        source_root_cause_run_id=args.source_root_cause_run_id,
        source_wide_run_id=args.source_wide_run_id,
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
