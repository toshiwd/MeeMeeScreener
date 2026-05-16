from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.backend.services import tradex_research_contracts as contracts
from scripts import tradex_ranking_loss_or_topk_objective_repair_v1 as ranking_mod


AXIS_ID = "candidate_generation_hypothesis_map_second_hypothesis_review_v1"
SCHEMA_PREFIX = "tradex_candidate_generation_hypothesis_map_second_hypothesis_review_v1"
DEFAULT_MISSED_WINNER_RUN_ID = "20260513T140000Z-missed-winner-event-source-candidate-generation-v1"
DEFAULT_SOURCE_VALIDATION_RUN_ID = "20260513T150000Z-source-specific-candidate-generation-validation-v1"
DEFAULT_APPLICABILITY_RUN_ID = "20260513T160000Z-source-specific-timeblock-applicability-audit-v1"
DEFAULT_ROOT_CAUSE_RUN_ID = "20260513T130000Z-oracle-gap-and-candidate-generation-root-cause-v1"
DEFAULT_MISSED_WINNER_ROOT = Path(r"G:\Tradex\missed_winner_event_source_candidate_generation_v1")
DEFAULT_SOURCE_VALIDATION_ROOT = Path(r"G:\Tradex\source_specific_candidate_generation_validation_v1")
DEFAULT_APPLICABILITY_ROOT = Path(r"G:\Tradex\source_specific_timeblock_applicability_audit_v1")
DEFAULT_ROOT_CAUSE_ROOT = Path(r"G:\Tradex\oracle_gap_and_candidate_generation_root_cause_v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\candidate_generation_hypothesis_map_second_hypothesis_review_v1")

REQUIRED_ARTIFACTS = (
    "evaluation_contract.json",
    "run_manifest.json",
    "source_artifact_refs.json",
    "second_hypothesis_review_contract.json",
    "first_hypothesis_archive_context.json",
    "second_hypothesis_profile.json",
    "hypothesis_distinctness_audit.json",
    "second_hypothesis_quality_precheck.json",
    "second_hypothesis_failure_mode_risk_report.json",
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
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, np.generic):
        return _json_ready(value.item())
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
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


def validate_sources(
    *,
    missed_winner_dir: Path,
    source_validation_dir: Path,
    applicability_dir: Path,
    root_cause_dir: Path,
) -> dict[str, Any]:
    required_by_source = {
        "missed_winner": [
            "_ARTIFACT_COMPLETE.json",
            "research_decision.json",
            "candidate_generation_hypothesis_map.json",
            "event_source_quality_leaderboard.json",
            "selected_nonwinner_source_decomposition.json",
        ],
        "source_validation": [
            "_ARTIFACT_COMPLETE.json",
            "research_decision.json",
            "validation_outcome_classification.json",
            "baseline_comparison_report.json",
        ],
        "applicability": [
            "_ARTIFACT_COMPLETE.json",
            "research_decision.json",
            "source_archive_or_refine_decision.json",
            "overfit_risk_report.json",
            "point_in_time_applicability_proxy_report.json",
        ],
        "root_cause": ["_ARTIFACT_COMPLETE.json", "research_decision.json", "candidate_generation_hypothesis_map.json"],
    }
    dirs = {
        "missed_winner": missed_winner_dir,
        "source_validation": source_validation_dir,
        "applicability": applicability_dir,
        "root_cause": root_cause_dir,
    }
    status: dict[str, Any] = {}
    for source, names in required_by_source.items():
        root = dirs[source]
        missing = [name for name in names if not (root / name).exists()]
        if missing:
            raise FileNotFoundError(f"{source} missing required artifacts: {missing} at {root}")
        complete = _load_json(root / "_ARTIFACT_COMPLETE.json")
        decision = _load_json(root / "research_decision.json")
        if complete.get("complete") is not True:
            raise RuntimeError(f"{source} artifact is not complete")
        if complete.get("silent_fallback_used") is not False or decision.get("silent_fallback_used") is not False:
            raise RuntimeError(f"{source} used silent fallback")
        if complete.get("research_fallback_used") is not False or decision.get("research_fallback_used") is not False:
            raise RuntimeError(f"{source} used research fallback")
        status[source] = {"_ARTIFACT_COMPLETE.json": complete, "research_decision.json": decision}

    if status["applicability"]["research_decision.json"].get("authoritative_research_decision") != "source_applicability_hold":
        raise RuntimeError("source applicability source is not hold archive context")
    if status["source_validation"]["research_decision.json"].get("authoritative_research_decision") != "source_specific_candidate_generation_drop":
        raise RuntimeError("source validation source is not frozen drop")
    missed_decision = status["missed_winner"]["research_decision.json"]
    if missed_decision.get("authoritative_research_decision") != "missed_winner_source_hypothesis_ready":
        raise RuntimeError("missed winner source did not produce hypothesis-ready diagnosis")

    status["missed_winner"]["candidate_generation_hypothesis_map.json"] = _load_json(missed_winner_dir / "candidate_generation_hypothesis_map.json")
    status["missed_winner"]["event_source_quality_leaderboard.json"] = _load_json(missed_winner_dir / "event_source_quality_leaderboard.json")
    status["missed_winner"]["selected_nonwinner_source_decomposition.json"] = _load_json(missed_winner_dir / "selected_nonwinner_source_decomposition.json")
    status["source_validation"]["validation_outcome_classification.json"] = _load_json(source_validation_dir / "validation_outcome_classification.json")
    status["source_validation"]["baseline_comparison_report.json"] = _load_json(source_validation_dir / "baseline_comparison_report.json")
    status["applicability"]["source_archive_or_refine_decision.json"] = _load_json(applicability_dir / "source_archive_or_refine_decision.json")
    status["applicability"]["overfit_risk_report.json"] = _load_json(applicability_dir / "overfit_risk_report.json")
    status["applicability"]["point_in_time_applicability_proxy_report.json"] = _load_json(applicability_dir / "point_in_time_applicability_proxy_report.json")
    status["root_cause"]["candidate_generation_hypothesis_map.json"] = _load_json(root_cause_dir / "candidate_generation_hypothesis_map.json")
    return status


def _hypotheses(status: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    hypotheses = status["missed_winner"]["candidate_generation_hypothesis_map.json"].get("hypotheses", [])
    if len(hypotheses) < 2:
        raise RuntimeError("candidate_generation_hypothesis_map.json does not contain a second hypothesis")
    return dict(hypotheses[0]), dict(hypotheses[1])


def _quality_row(status: dict[str, Any], source_family: str) -> dict[str, Any]:
    for row in status["missed_winner"]["event_source_quality_leaderboard.json"].get("rows", []):
        if row.get("event_source") == source_family:
            return dict(row)
    for row in status["missed_winner"]["selected_nonwinner_source_decomposition.json"].get("rows", []):
        if row.get("event_source") == source_family:
            return dict(row)
    return {}


def build_first_hypothesis_archive_context(status: dict[str, Any]) -> dict[str, Any]:
    first, _second = _hypotheses(status)
    validation = status["source_validation"]["research_decision.json"]
    applicability = status["applicability"]["research_decision.json"]
    proxy = status["applicability"]["point_in_time_applicability_proxy_report.json"]
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_first_hypothesis_archive_context_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "first_hypothesis": first,
        "first_hypothesis_validated": True,
        "first_hypothesis_validation_decision": validation.get("authoritative_research_decision"),
        "first_hypothesis_rescue_stopped": True,
        "applicability_decision": applicability.get("authoritative_research_decision"),
        "point_in_time_proxy_found": proxy.get("point_in_time_proxy_found"),
        "overfit_risk_high": status["applicability"]["overfit_risk_report.json"].get("overfit_risk_high"),
        "practical_status": "archive_leaning_do_not_rescue",
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_second_hypothesis_profile(status: dict[str, Any]) -> dict[str, Any]:
    _first, second = _hypotheses(status)
    quality = _quality_row(status, second.get("source_family", ""))
    evidence = dict(second.get("evidence") or {})
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_second_hypothesis_profile_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "second_hypothesis": second,
        "source_family": second.get("source_family"),
        "target_failure_mode": second.get("target_failure_mode"),
        "testable_next_axis": second.get("testable_next_axis"),
        "sample_count": evidence.get("sample_count", quality.get("sample_count")),
        "missed_winner_count": evidence.get("missed_winner_count", quality.get("missed_winner_count")),
        "future_winner_rate": evidence.get("future_winner_rate", quality.get("future_winner_rate")),
        "severe_loss_rate20": evidence.get("severe_loss_rate20", quality.get("severe_loss_rate20")),
        "time_block_stability": evidence.get("time_block_stability"),
        "selected_nonwinner_count": quality.get("selected_nonwinner_count"),
        "selected_nonwinner_rate": quality.get("selected_nonwinner_rate"),
        "quality_row_found": bool(quality),
        "second_hypothesis_already_validated": False,
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_hypothesis_distinctness_audit(status: dict[str, Any]) -> dict[str, Any]:
    first, second = _hypotheses(status)
    first_source = str(first.get("source_family"))
    second_source = str(second.get("source_family"))
    first_tags = _parse_source_family(first_source)
    second_tags = _parse_source_family(second_source)
    shared_tags = {key: value for key, value in second_tags.items() if first_tags.get(key) == value}
    unique_second_tags = {key: value for key, value in second_tags.items() if first_tags.get(key) != value}
    overlap_count = 0 if first_source != second_source else int(second.get("evidence", {}).get("sample_count") or 0)
    second_sample = int(second.get("evidence", {}).get("sample_count") or 0)
    shared_rate = len(shared_tags) / len(second_tags) if second_tags else 0.0
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_hypothesis_distinctness_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "first_source_family": first_source,
        "second_source_family": second_source,
        "overlap_count": overlap_count,
        "overlap_rate": overlap_count / second_sample if second_sample else 0.0,
        "shared_tags": shared_tags,
        "unique_tags": unique_second_tags,
        "shared_tag_rate": shared_rate,
        "negative_guard_distinguishes_second": unique_second_tags.get("negative_guard_match") == "True",
        "clearly_distinct_from_failed_first_source": bool(first_source != second_source and overlap_count == 0 and unique_second_tags),
        "likely_same_failure_mode_risk": "high" if shared_rate >= 0.75 else "medium" if shared_rate >= 0.50 else "low",
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_second_hypothesis_quality_precheck(profile: dict[str, Any]) -> dict[str, Any]:
    sample_count = int(profile.get("sample_count") or 0)
    missed_count = int(profile.get("missed_winner_count") or 0)
    future_rate = float(profile.get("future_winner_rate") or 0.0)
    severe_rate = float(profile.get("severe_loss_rate20") or 0.0)
    stability = float(profile.get("time_block_stability") or 0.0)
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_second_hypothesis_quality_precheck_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "sample_count": sample_count,
        "missed_winner_count": missed_count,
        "future_winner_rate": future_rate,
        "severe_loss_rate20": severe_rate,
        "time_block_stability": stability,
        "sample_size_sufficient": sample_count >= 500,
        "missed_winner_count_meaningful": missed_count >= 50,
        "future_winner_rate_meaningful": future_rate >= 0.15,
        "severe_loss_not_structurally_unacceptable": severe_rate <= 0.22,
        "time_block_stability_acceptable": stability >= 0.50,
        "source_mismatch_support": profile.get("second_hypothesis", {}).get("evidence", {}).get("same_date_under_ranked_rate"),
    }
    payload["quality_precheck_passed"] = bool(
        payload["sample_size_sufficient"]
        and payload["missed_winner_count_meaningful"]
        and payload["future_winner_rate_meaningful"]
        and payload["severe_loss_not_structurally_unacceptable"]
        and payload["time_block_stability_acceptable"]
    )
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_second_hypothesis_failure_mode_risk_report(
    *,
    distinctness: dict[str, Any],
    quality: dict[str, Any],
    first_context: dict[str, Any],
    profile: dict[str, Any],
) -> dict[str, Any]:
    severe_rate = float(quality.get("severe_loss_rate20") or 0.0)
    risk_under_ranked_unusable = distinctness.get("likely_same_failure_mode_risk") == "high" and first_context.get("first_hypothesis_validation_decision") == "source_specific_candidate_generation_drop"
    risk_too_noisy = severe_rate >= 0.20 or profile.get("source_family", "").endswith("negative_guard_match=True")
    risk_calendar_only = first_context.get("overfit_risk_high") is True and distinctness.get("shared_tag_rate", 0.0) >= 0.75
    risk_max3_overfill = True
    risk_score = sum(bool(item) for item in [risk_under_ranked_unusable, risk_too_noisy, risk_calendar_only, risk_max3_overfill])
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_second_hypothesis_failure_mode_risk_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "risk_source_under_ranked_but_unusable": bool(risk_under_ranked_unusable),
        "risk_source_recovers_winners_but_too_noisy": bool(risk_too_noisy),
        "risk_calendar_pocket_only_behavior": bool(risk_calendar_only),
        "risk_max3_overfill": bool(risk_max3_overfill),
        "same_failure_mode_risk": "high" if risk_score >= 3 else "medium" if risk_score == 2 else "low",
        "risk_score": risk_score,
        "risk_reasons": [
            reason
            for reason, present in [
                ("shares most source tags with dropped first hypothesis", risk_under_ranked_unusable),
                ("negative_guard true and severe loss is high", risk_too_noisy),
                ("first source only had calendar pockets without PIT proxy", risk_calendar_only),
                ("max3 overfill remained a primary root-cause mode", risk_max3_overfill),
            ]
            if present
        ],
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_next_axis_recommendation(*, decision_class: str) -> dict[str, Any]:
    if decision_class == "second_hypothesis_ready_for_validation":
        next_axis = "source_specific_candidate_generation_validation_v2"
        reason = "second hypothesis is distinct and passes quality/risk precheck"
    elif decision_class == "second_hypothesis_hold":
        next_axis = "second_hypothesis_additional_diagnosis_v1"
        reason = "second hypothesis is partly distinct or promising but same-failure risk is unclear"
    else:
        next_axis = "candidate_generation_hypothesis_map_refresh_v1"
        reason = "second hypothesis carries excessive same-failure/noise risk or is not worth validation"
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_next_axis_recommendation_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "one_recommended_next_axis_only": True,
        "recommended_next_axis": next_axis,
        "reason": reason,
        "do_not_continue_axes": [
            "rescue first source",
            "create source-specific challenger in this run",
            "learned scorer",
            "threshold/no-trade",
            "image fusion",
            "safe_full hard filter",
            "negative_guard hard veto",
        ],
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def _decision_class(distinctness: dict[str, Any], quality: dict[str, Any], risk: dict[str, Any]) -> str:
    if (
        distinctness.get("clearly_distinct_from_failed_first_source")
        and quality.get("quality_precheck_passed")
        and risk.get("same_failure_mode_risk") != "high"
    ):
        return "second_hypothesis_ready_for_validation"
    if distinctness.get("clearly_distinct_from_failed_first_source") and quality.get("sample_size_sufficient") and risk.get("same_failure_mode_risk") == "medium":
        return "second_hypothesis_hold"
    return "second_hypothesis_drop_or_skip"


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
        "research_phase": "candidate_generation_hypothesis_map_second_hypothesis_review",
        "boundary": "TRADEX-only",
        "axis_moved": "candidate_generation_hypothesis_map_second_hypothesis_review",
        "source_applicability_decision": status["applicability"]["research_decision.json"].get("authoritative_research_decision"),
        "diagnosis_only": True,
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
    review_contract = {
        "schema_version": f"{SCHEMA_PREFIX}_second_hypothesis_review_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "review_only": True,
        "first_hypothesis_rescue_stopped": True,
        "second_hypothesis_review_created": True,
        "candidate_generation_challenger_created": False,
        "candidate_scoring_created": False,
        "threshold_policy_created": False,
        "production_ranking_changed": False,
    }
    review_contract["contract_hash"] = _stable_hash(review_contract)
    source_refs = {
        "schema_version": f"{SCHEMA_PREFIX}_source_artifact_refs_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "source_roots": {key: str(value) for key, value in source_dirs.items()},
        "refs": refs,
    }
    return {
        "evaluation_contract.json": evaluation_contract,
        "source_artifact_refs.json": source_refs,
        "second_hypothesis_review_contract.json": review_contract,
    }


def build_research_decision(
    *,
    decision_class: str,
    distinctness: dict[str, Any],
    quality: dict[str, Any],
    risk: dict[str, Any],
    artifact_complete: bool,
) -> dict[str, Any]:
    if artifact_complete and decision_class == "second_hypothesis_ready_for_validation":
        decision = "keep_candidate"
        authoritative = "second_hypothesis_ready_for_validation"
    elif artifact_complete and decision_class == "second_hypothesis_hold":
        decision = "hold"
        authoritative = "second_hypothesis_hold"
    else:
        decision = "drop"
        authoritative = "second_hypothesis_drop"
    typed_reasons = [
        "second_hypothesis_distinct" if distinctness.get("clearly_distinct_from_failed_first_source") else "second_hypothesis_not_distinct",
        "quality_precheck_passed" if quality.get("quality_precheck_passed") else "quality_precheck_failed",
        f"same_failure_mode_risk_{risk.get('same_failure_mode_risk')}",
        "diagnosis_only_no_challenger_created",
        "artifact_complete" if artifact_complete else "artifact_incomplete",
    ]
    return {
        "schema_version": f"{SCHEMA_PREFIX}_research_decision_v1",
        "generated_at": _utc_now(),
        "research_phase": "candidate_generation_hypothesis_map_second_hypothesis_review",
        "boundary": "TRADEX-only",
        "axis_moved": "candidate_generation_hypothesis_map_second_hypothesis_review",
        "source_applicability_decision": "source_applicability_hold",
        "first_hypothesis_rescue_stopped": True,
        "second_hypothesis_review_created": True,
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
        "decision_reasons": [
            {"code": "clearly_distinct_from_failed_first_source", "status": "pass" if distinctness.get("clearly_distinct_from_failed_first_source") else "fail", "value": distinctness.get("clearly_distinct_from_failed_first_source")},
            {"code": "quality_precheck_passed", "status": "pass" if quality.get("quality_precheck_passed") else "fail", "value": quality.get("quality_precheck_passed")},
            {"code": "same_failure_mode_risk_not_high", "status": "pass" if risk.get("same_failure_mode_risk") != "high" else "fail", "value": risk.get("same_failure_mode_risk")},
            {"code": "no_challenger_scorer_threshold_or_fusion_created", "status": "pass", "value": True},
            {"code": "artifact_complete", "status": "pass" if artifact_complete else "fail", "value": artifact_complete},
        ],
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
        "first_hypothesis_rescue_stopped": True,
        "second_hypothesis_review_created": True,
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


def run_candidate_generation_hypothesis_map_second_hypothesis_review_v1(
    *,
    source_missed_winner_run_id: str = DEFAULT_MISSED_WINNER_RUN_ID,
    source_validation_run_id: str = DEFAULT_SOURCE_VALIDATION_RUN_ID,
    source_applicability_run_id: str = DEFAULT_APPLICABILITY_RUN_ID,
    source_root_cause_run_id: str = DEFAULT_ROOT_CAUSE_RUN_ID,
    missed_winner_root: str | Path = DEFAULT_MISSED_WINNER_ROOT,
    source_validation_root: str | Path = DEFAULT_SOURCE_VALIDATION_ROOT,
    applicability_root: str | Path = DEFAULT_APPLICABILITY_ROOT,
    root_cause_root: str | Path = DEFAULT_ROOT_CAUSE_ROOT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    run_id: str | None = None,
) -> dict[str, Any]:
    missed_dir = _run_dir(missed_winner_root, source_missed_winner_run_id, DEFAULT_MISSED_WINNER_ROOT)
    validation_dir = _run_dir(source_validation_root, source_validation_run_id, DEFAULT_SOURCE_VALIDATION_ROOT)
    applicability_dir = _run_dir(applicability_root, source_applicability_run_id, DEFAULT_APPLICABILITY_ROOT)
    root_cause_dir = _run_dir(root_cause_root, source_root_cause_run_id, DEFAULT_ROOT_CAUSE_ROOT)
    output_dir = _safe_path(output_root, DEFAULT_OUTPUT_ROOT) / (run_id.strip() if isinstance(run_id, str) and run_id.strip() else _default_run_id())
    status = validate_sources(
        missed_winner_dir=missed_dir,
        source_validation_dir=validation_dir,
        applicability_dir=applicability_dir,
        root_cause_dir=root_cause_dir,
    )
    first_context = build_first_hypothesis_archive_context(status)
    second_profile = build_second_hypothesis_profile(status)
    distinctness = build_hypothesis_distinctness_audit(status)
    quality = build_second_hypothesis_quality_precheck(second_profile)
    risk = build_second_hypothesis_failure_mode_risk_report(
        distinctness=distinctness,
        quality=quality,
        first_context=first_context,
        profile=second_profile,
    )
    decision_class = _decision_class(distinctness, quality, risk)
    next_axis = build_next_axis_recommendation(decision_class=decision_class)
    source_dirs = {
        "missed_winner": missed_dir,
        "source_validation": validation_dir,
        "applicability": applicability_dir,
        "root_cause": root_cause_dir,
    }
    contract_artifacts = build_contract_artifacts(source_dirs=source_dirs, status=status)
    run_manifest = contracts.build_run_manifest(
        session_id=output_dir.name,
        seed=ranking_mod.RANDOM_SEED,
        random_seed=ranking_mod.RANDOM_SEED,
        input_artifacts=[{"name": key, "path": str(value)} for key, value in source_dirs.items()],
        asof="20260513",
        config={
            "axis_id": AXIS_ID,
            "diagnosis_only": True,
            "first_hypothesis_rescue_stopped": True,
            "second_hypothesis_review_created": True,
            "candidate_generation_challenger_created": False,
            "candidate_scoring_created": False,
            "ranking_objective_created": False,
            "threshold_policy_created": False,
            "image_score_used": False,
            "fusion_reranker_created": False,
            "production_ranking_changed": False,
        },
        universe=[],
        period={"start_date": "20160509", "end_date": "20260513", "label": "candidate_generation_hypothesis_map_second_hypothesis_review"},
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
        "first_hypothesis_archive_context.json": first_context,
        "second_hypothesis_profile.json": second_profile,
        "hypothesis_distinctness_audit.json": distinctness,
        "second_hypothesis_quality_precheck.json": quality,
        "second_hypothesis_failure_mode_risk_report.json": risk,
        "next_axis_recommendation.json": next_axis,
    }.items():
        paths[name] = str(_write_json(output_dir / name, payload))
    pre_complete = _artifact_complete(output_dir, paths)
    decision = build_research_decision(
        decision_class=decision_class,
        distinctness=distinctness,
        quality=quality,
        risk=risk,
        artifact_complete=bool(pre_complete["complete"]),
    )
    paths["research_decision.json"] = str(_write_json(output_dir / "research_decision.json", decision))
    complete = _artifact_complete(output_dir, paths, decision)
    paths["_ARTIFACT_COMPLETE.json"] = str(_write_json(output_dir / "_ARTIFACT_COMPLETE.json", complete))
    return {
        "output_dir": str(output_dir),
        "decision": decision["decision"],
        "authoritative_research_decision": decision["authoritative_research_decision"],
        "decision_classification": decision_class,
        "recommended_next_axis": next_axis.get("recommended_next_axis"),
        "first_hypothesis_rescue_stopped": True,
        "second_hypothesis_review_created": True,
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
    parser.add_argument("--source-missed-winner-run-id", default=DEFAULT_MISSED_WINNER_RUN_ID)
    parser.add_argument("--source-validation-run-id", default=DEFAULT_SOURCE_VALIDATION_RUN_ID)
    parser.add_argument("--source-applicability-run-id", default=DEFAULT_APPLICABILITY_RUN_ID)
    parser.add_argument("--source-root-cause-run-id", default=DEFAULT_ROOT_CAUSE_RUN_ID)
    parser.add_argument("--missed-winner-root", default=str(DEFAULT_MISSED_WINNER_ROOT))
    parser.add_argument("--source-validation-root", default=str(DEFAULT_SOURCE_VALIDATION_ROOT))
    parser.add_argument("--applicability-root", default=str(DEFAULT_APPLICABILITY_ROOT))
    parser.add_argument("--root-cause-root", default=str(DEFAULT_ROOT_CAUSE_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--run-id", default="")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    result = run_candidate_generation_hypothesis_map_second_hypothesis_review_v1(
        source_missed_winner_run_id=args.source_missed_winner_run_id,
        source_validation_run_id=args.source_validation_run_id,
        source_applicability_run_id=args.source_applicability_run_id,
        source_root_cause_run_id=args.source_root_cause_run_id,
        missed_winner_root=args.missed_winner_root,
        source_validation_root=args.source_validation_root,
        applicability_root=args.applicability_root,
        root_cause_root=args.root_cause_root,
        output_root=args.output_root,
        run_id=args.run_id,
    )
    print(_json_text(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
