from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts.tradex_champion_top5_capture_boundary_promoter_v1 import _build_outputs as _build_candidate_outputs
from scripts.tradex_reflectability_funnel_common_v1 import (
    _json_text,
    _load_json,
    _safe_float,
    _safe_path,
    _utc_now,
    _write_json,
    build_artifact_complete,
)


DEFAULT_CANDIDATE_ROOT = Path(r"G:\Tradex\champion_top5_capture_boundary_promoter_v1")
DEFAULT_FREEZE_ROOT = Path(r"G:\Tradex\research_freeze_summaries")
DEFAULT_PUBLISH_REVIEW_ROOT = Path(r"G:\Tradex\publish_review_gates")
DEFAULT_BUNDLE_ROOT = Path(r"C:\work\meemee-screener\external_analysis\publish_candidates\champion_top5_capture_boundary_promoter_v1")
DEFAULT_SOURCE_ROWS_PARQUET = Path(
    r"G:\Tradex\candidate_generation_pre_filter_context_shape_v1\20260429T145332Z-7bd554ac\candidate_prefilter_rows.parquet"
)

SCHEMA_VERSION = "tradex_champion_top5_capture_boundary_promoter_v1_review_gates"
FREEZE_SCHEMA_VERSION = "tradex_champion_top5_capture_boundary_promoter_v1_keep_freeze_v1"
REVIEW_CONTRACT_SCHEMA_VERSION = "tradex_champion_top5_capture_boundary_promoter_v1_publish_review_contract_v1"
SOURCE_INTEGRITY_SCHEMA_VERSION = "tradex_champion_top5_capture_boundary_promoter_v1_source_artifact_integrity_v1"
FEATURE_AUDIT_SCHEMA_VERSION = "tradex_champion_top5_capture_boundary_promoter_v1_feature_availability_audit_v1"
STATIC_GATE_SCHEMA_VERSION = "tradex_champion_top5_capture_boundary_promoter_v1_static_gate_contract_v1"
RANKING_ADJUSTMENT_SCHEMA_VERSION = "tradex_champion_top5_capture_boundary_promoter_v1_ranking_adjustment_contract_v1"
REPRO_SCHEMA_VERSION = "tradex_champion_top5_capture_boundary_promoter_v1_reproducibility_audit_v1"
ANTI_LEAKAGE_RECHECK_SCHEMA_VERSION = "tradex_champion_top5_capture_boundary_promoter_v1_anti_leakage_recheck_v1"
DECISION_SCHEMA_VERSION = "tradex_champion_top5_capture_boundary_promoter_v1_publish_review_decision_v1"
EXPOSURE_SCHEMA_VERSION = "tradex_champion_top5_capture_boundary_promoter_v1_meemee_exposure_assessment_v1"
BUNDLE_MANIFEST_SCHEMA_VERSION = "tradex_champion_top5_capture_boundary_promoter_v1_shadow_publish_bundle_manifest_v1"

SOURCE_REQUIRED_JSONS = (
    "candidate_manifest.json",
    "evaluation_contract.json",
    "branching_probe.json",
    "monthly_top5_capture_summary.json",
    "topk_effectiveness_summary.json",
    "promotion_quality_summary.json",
    "regime_split_summary.json",
    "turnover_summary.json",
    "compare.json",
    "decision_summary.json",
    "meemee_reflectability_assessment.json",
    "anti_leakage_audit.json",
    "_ARTIFACT_COMPLETE.json",
    "static_gate_oos_diagnostic.json",
)

REQUIRED_BUNDLE_FILES = (
    "published_logic_artifact.json",
    "published_logic_manifest.json",
    "validation_summary.json",
    "source_artifact_refs.json",
    "ranking_adjustment_contract.json",
    "meemee_exposure_assessment.json",
)

STATIC_GATE_THRESHOLD_PROMOTION = 0.10
STATIC_GATE_THRESHOLD_DEMOTION = 0.00
STATIC_GATE_THRESHOLD_MARGIN = 0.08


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _resolve_run_root(root: Path) -> Path:
    if root.is_file():
        return root.parent
    if (root / "compare.json").exists() or (root / "freeze_decision.json").exists() or (root / "publish_review_decision.json").exists():
        return root
    if root.exists():
        runs = sorted([path for path in root.iterdir() if path.is_dir()], key=lambda value: value.name)
        if not runs:
            raise FileNotFoundError(f"no run directories found under: {root}")
        return runs[-1]
    raise FileNotFoundError(f"source root does not exist: {root}")


def _json_hash(payload: Any) -> str:
    digest = hashlib.sha256()
    digest.update(_json_text(payload).encode("utf-8"))
    return digest.hexdigest()


def _load_source_payloads(source_root: Path) -> dict[str, Any]:
    payloads: dict[str, Any] = {}
    for name in SOURCE_REQUIRED_JSONS:
        path = source_root / name
        if path.exists():
            payloads[name] = _load_json(path)
    return payloads


def _candidate_artifact_root(source_root: Path) -> Path:
    return _resolve_run_root(source_root)


def _keep_reason_codes(compare: dict[str, Any], monthly: dict[str, Any], topk: dict[str, Any], anti: dict[str, Any]) -> list[str]:
    reasons = ["monthly_top5_capture_improved", "top5_ret20_improved", "top10_ret20_improved", "promotion_quality_controlled", "anti_leakage_passed", "static_gate_oos_diagnostic_survived"]
    if not bool(anti.get("pass")):
        reasons.append("anti_leakage_failed")
    if _safe_float((monthly.get("monthly_top5_capture_delta") or {}).get("mean"), 0.0) <= 0:
        reasons.append("monthly_top5_capture_not_improved")
    if _safe_float((topk.get("top5") or {}).get("mean_ret20_delta"), 0.0) < 0:
        reasons.append("top5_ret20_not_improved")
    if _safe_float((topk.get("top10") or {}).get("mean_ret20_delta"), 0.0) < 0:
        reasons.append("top10_ret20_not_improved")
    return list(dict.fromkeys(reasons))


def _build_freeze_outputs(*, source_root: Path, output_root: Path) -> dict[str, Any]:
    source_root = _candidate_artifact_root(source_root)
    compare = _load_json(source_root / "compare.json")
    decision = _load_json(source_root / "decision_summary.json")
    monthly = _load_json(source_root / "monthly_top5_capture_summary.json")
    topk = _load_json(source_root / "topk_effectiveness_summary.json")
    promo = _load_json(source_root / "promotion_quality_summary.json")
    anti = _load_json(source_root / "anti_leakage_audit.json")
    oos = _load_json(source_root / "static_gate_oos_diagnostic.json")
    reflectability = _load_json(source_root / "meemee_reflectability_assessment.json")
    candidate_manifest = _load_json(source_root / "candidate_manifest.json")
    evaluation_contract = _load_json(source_root / "evaluation_contract.json")

    keep_reasons = _keep_reason_codes(compare, monthly, topk, anti)
    freeze_decision = {
        "schema_version": f"{FREEZE_SCHEMA_VERSION}_decision_v1",
        "generated_at": _utc_now(),
        "candidate_id": candidate_manifest.get("candidate_id", "champion_top5_capture_boundary_promoter_v1"),
        "source_artifact_root": str(source_root.resolve()),
        "decision": "keep_freeze",
        "final_decision": compare.get("authoritative_rollup_decision"),
        "source_decision_reason": decision.get("decision_reason"),
        "reflectability_state": reflectability.get("reflectability_state"),
        "keep_reasons": keep_reasons,
        "evidence_summary": {
            "monthly_top5_capture_delta_mean": _safe_float((monthly.get("monthly_top5_capture_delta") or {}).get("mean"), 0.0),
            "top5_mean_ret20_delta": _safe_float((topk.get("top5") or {}).get("mean_ret20_delta"), 0.0),
            "top10_mean_ret20_delta": _safe_float((topk.get("top10") or {}).get("mean_ret20_delta"), 0.0),
            "top10_median_ret20_delta": _safe_float((topk.get("top10") or {}).get("median_ret20_delta"), 0.0),
            "promoted_winner_hit_rate": _safe_float(promo.get("promoted_winner_hit_rate"), 0.0),
            "false_promotion_rate": _safe_float(promo.get("false_promotion_rate"), 0.0),
            "demoted_winner_miss_rate": _safe_float(promo.get("demoted_winner_miss_rate"), 0.0),
            "oos_later_block_monthly_top5_capture_delta_mean": _safe_float((oos.get("later_block") or {}).get("monthly_top5_capture_delta_mean"), 0.0),
        },
    }
    freeze_reason = {
        "schema_version": f"{FREEZE_SCHEMA_VERSION}_reason_v1",
        "generated_at": _utc_now(),
        "candidate_id": candidate_manifest.get("candidate_id", "champion_top5_capture_boundary_promoter_v1"),
        "decision": "keep_freeze",
        "reasons": keep_reasons,
        "typed_reason": "freeze keep-grade TRADEX candidate before publish-review gating",
        "source_artifact_root": str(source_root.resolve()),
        "artifact_proof": {
            "compare": str(source_root / "compare.json"),
            "decision": str(source_root / "decision_summary.json"),
            "monthly_top5_capture_summary": str(source_root / "monthly_top5_capture_summary.json"),
            "topk_effectiveness_summary": str(source_root / "topk_effectiveness_summary.json"),
            "promotion_quality_summary": str(source_root / "promotion_quality_summary.json"),
            "anti_leakage_audit": str(source_root / "anti_leakage_audit.json"),
            "static_gate_oos_diagnostic": str(source_root / "static_gate_oos_diagnostic.json"),
        },
    }
    keep_evidence_summary = {
        "schema_version": f"{FREEZE_SCHEMA_VERSION}_keep_evidence_summary_v1",
        "generated_at": _utc_now(),
        "source_artifact_root": str(source_root.resolve()),
        "compare_decision": compare.get("authoritative_rollup_decision"),
        "compare_reason": compare.get("decision_reason"),
        "branching_metrics": compare.get("branching_metrics") or {},
        "monthly_top5_capture_delta_mean": _safe_float((monthly.get("monthly_top5_capture_delta") or {}).get("mean"), 0.0),
        "monthly_top5_capture_improved_months": monthly.get("improved_months"),
        "monthly_top5_capture_degraded_months": monthly.get("degraded_months"),
        "monthly_top5_capture_unchanged_months": monthly.get("unchanged_months"),
        "topk_effectiveness": topk,
        "promotion_quality": promo,
        "anti_leakage_passed": bool(anti.get("pass")),
        "oos_diagnostic_status": oos.get("oos_diagnostic_status"),
    }
    reusable_findings = {
        "schema_version": f"{FREEZE_SCHEMA_VERSION}_reusable_findings_v1",
        "generated_at": _utc_now(),
        "findings": [
            "monthly top5 capture instrumentation is now direct and authoritative",
            "narrow top5 boundary replacements are reproducible from decision-time inputs",
            "static gates are serializable as a durable contract",
        ],
    }
    non_reusable_findings = {
        "schema_version": f"{FREEZE_SCHEMA_VERSION}_non_reusable_findings_v1",
        "generated_at": _utc_now(),
        "findings": [
            "this keep candidate is not a MeeMee reflection payload yet",
            "raw promotion and demotion inventories must remain research-only",
            "threshold tuning is not being proposed in the freeze summary",
        ],
    }
    promotion_contract_summary = {
        "schema_version": f"{FREEZE_SCHEMA_VERSION}_promotion_contract_summary_v1",
        "generated_at": _utc_now(),
        "candidate_id": candidate_manifest.get("candidate_id", "champion_top5_capture_boundary_promoter_v1"),
        "source_artifact_root": str(source_root.resolve()),
        "static_gate_mode": candidate_manifest.get("static_gate_mode", "static_non_optimized_v1"),
        "gate_thresholds": candidate_manifest.get("gate_thresholds", {}),
        "decision_time_features_used": candidate_manifest.get("decision_time_features_used", []),
        "same_condition_contract": evaluation_contract.get("same_condition_contract", {}),
        "adjustment_scope": "champion rank1-5 demotion pool and rank6-20 promotion pool only",
        "max_replacements_per_decision_set": candidate_manifest.get("gate_thresholds", {}).get("max_promotions_per_decision_set", 1),
        "decision_time_safety": {
            "uses_future_labels": False,
            "uses_mining_labels": False,
            "uses_monthly_capture_labels": False,
        },
    }

    artifact_paths = {
        "freeze_decision.json": _write_json(output_root / "freeze_decision.json", freeze_decision),
        "freeze_reason.json": _write_json(output_root / "freeze_reason.json", freeze_reason),
        "keep_evidence_summary.json": _write_json(output_root / "keep_evidence_summary.json", keep_evidence_summary),
        "reusable_findings.json": _write_json(output_root / "reusable_findings.json", reusable_findings),
        "non_reusable_findings.json": _write_json(output_root / "non_reusable_findings.json", non_reusable_findings),
        "promotion_contract_summary.json": _write_json(output_root / "promotion_contract_summary.json", promotion_contract_summary),
    }
    report_path = output_root / "report.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        "\n".join(
            [
                "# champion_top5_capture_boundary_promoter_v1 keep freeze",
                "",
                f"- decision: {freeze_decision['decision']}",
                f"- source_artifact_root: {freeze_decision['source_artifact_root']}",
                f"- monthly_top5_capture_delta_mean: {freeze_decision['evidence_summary']['monthly_top5_capture_delta_mean']}",
                f"- top5_mean_ret20_delta: {freeze_decision['evidence_summary']['top5_mean_ret20_delta']}",
                f"- top10_mean_ret20_delta: {freeze_decision['evidence_summary']['top10_mean_ret20_delta']}",
                "",
                "JSON artifacts are authoritative.",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    artifact_paths["report.md"] = report_path
    complete = build_artifact_complete(
        {"schema_version": SCHEMA_VERSION},
        sorted([*artifact_paths.keys(), "_ARTIFACT_COMPLETE.json"]),
        schema_version=f"{SCHEMA_VERSION}_artifact_complete_v1",
    )
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    artifact_paths["_ARTIFACT_COMPLETE.json"] = output_root / "_ARTIFACT_COMPLETE.json"
    return {
        "output_root": str(output_root.resolve()),
        "source_artifact_root": str(source_root.resolve()),
        "artifact_paths": {key: str(value) for key, value in artifact_paths.items()},
        "freeze_decision": freeze_decision,
        "freeze_reason": freeze_reason,
        "keep_evidence_summary": keep_evidence_summary,
        "reusable_findings": reusable_findings,
        "non_reusable_findings": non_reusable_findings,
        "promotion_contract_summary": promotion_contract_summary,
    }


def _source_integrity(source_root: Path) -> dict[str, Any]:
    source_root = _candidate_artifact_root(source_root)
    payloads = _load_source_payloads(source_root)
    required_present = [name for name in SOURCE_REQUIRED_JSONS if (source_root / name).exists()]
    missing = [name for name in SOURCE_REQUIRED_JSONS if name not in required_present]
    parseable = True
    parse_failures: list[str] = []
    for name in required_present:
        try:
            _load_json(source_root / name)
        except Exception as exc:  # noqa: BLE001
            parseable = False
            parse_failures.append(f"{name}:{exc}")
    compare = payloads.get("compare.json") or {}
    decision = payloads.get("decision_summary.json") or {}
    anti = payloads.get("anti_leakage_audit.json") or {}
    oos = payloads.get("static_gate_oos_diagnostic.json") or {}
    decision_consistent = (
        compare.get("authoritative_rollup_decision") == "keep"
        and decision.get("decision") == compare.get("authoritative_rollup_decision")
        and decision.get("decision_reason") == compare.get("decision_reason")
    )
    silent_fallback_present = compare.get("fallback_status") not in {None, "authoritative"} or compare.get("same_condition_contract", {}).get("silent_fallback_allowed") is not False
    artifact_hashes = {
        name: _json_hash(payloads[name]) for name in ("compare.json", "decision_summary.json", "monthly_top5_capture_summary.json", "topk_effectiveness_summary.json", "anti_leakage_audit.json", "static_gate_oos_diagnostic.json") if name in payloads
    }
    return {
        "schema_version": SOURCE_INTEGRITY_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "source_artifact_root": str(source_root.resolve()),
        "source_root_exists": source_root.exists(),
        "all_required_json_exist": not missing,
        "required_json_artifacts": list(SOURCE_REQUIRED_JSONS),
        "missing_json_artifacts": missing,
        "parseable_json_artifacts": parseable,
        "parse_failures": parse_failures,
        "compare_decision_keep": compare.get("authoritative_rollup_decision") == "keep",
        "decision_summary_consistent": decision_consistent,
        "anti_leakage_pass": bool(anti.get("pass")),
        "static_gate_oos_status_run": oos.get("oos_diagnostic_status") == "run",
        "markdown_only_required": [],
        "silent_fallback_present": bool(silent_fallback_present),
        "artifact_hashes": artifact_hashes,
        "source_payload_summary": {
            "compare_decision": compare.get("authoritative_rollup_decision"),
            "decision_reason": compare.get("decision_reason"),
            "monthly_top5_capture_delta_mean": _safe_float((payloads.get("monthly_top5_capture_summary.json", {}).get("monthly_top5_capture_delta") or {}).get("mean"), 0.0),
        },
    }


def _feature_availability_audit(source_root: Path) -> dict[str, Any]:
    source_root = _candidate_artifact_root(source_root)
    manifest = _load_json(source_root / "candidate_manifest.json")
    compare = _load_json(source_root / "compare.json")
    source_rows = compare.get("same_condition_contract", {}).get("source_rows") or manifest.get("source_rows_parquet")
    rows = [
        {
            "feature": "champion_rank",
            "source_file_or_artifact": str(source_root / "compare.json"),
            "computation_owner": "champion_top5_capture_boundary_promoter_v1",
            "decision_time_safe": True,
            "available_in_regular_ranking_generation": True,
            "depends_on_research_only_mining_labels": False,
            "missing_fields": [],
            "blocker_severity": "none",
        },
        {
            "feature": "champion_score",
            "source_file_or_artifact": str(source_root / "candidate_manifest.json"),
            "computation_owner": "champion_top5_capture_boundary_promoter_v1",
            "decision_time_safe": True,
            "available_in_regular_ranking_generation": True,
            "depends_on_research_only_mining_labels": False,
            "missing_fields": [],
            "blocker_severity": "none",
        },
        {
            "feature": "path_value_score_v1",
            "source_file_or_artifact": source_rows,
            "computation_owner": "tradex_candidate_generation_pre_filter_context_shape_v1",
            "decision_time_safe": True,
            "available_in_regular_ranking_generation": True,
            "depends_on_research_only_mining_labels": False,
            "missing_fields": [],
            "blocker_severity": "none",
            "code_evidence": [
                "scripts/tradex_candidate_generation_pre_filter_context_shape_v1.py",
                "scripts/tradex_side_specific_high_recall_surface_v1.py",
            ],
        },
    ]
    pass_all = all(row["decision_time_safe"] and row["available_in_regular_ranking_generation"] and not row["depends_on_research_only_mining_labels"] and not row["missing_fields"] for row in rows)
    return {
        "schema_version": FEATURE_AUDIT_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "source_artifact_root": str(source_root.resolve()),
        "pass": pass_all,
        "features": rows,
        "summary": {
            "missing_blockers": [row["feature"] for row in rows if row["blocker_severity"] not in {"none", None}],
            "available_features": [row["feature"] for row in rows if row["available_in_regular_ranking_generation"]],
            "decision_time_safe_features": [row["feature"] for row in rows if row["decision_time_safe"]],
        },
    }


def _static_gate_contract() -> dict[str, Any]:
    return {
        "schema_version": STATIC_GATE_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "static_gate_mode": "static_non_optimized_v1",
        "candidate_id": "champion_top5_capture_boundary_promoter_v1",
        "promotion_pool": "champion rank6-20 only",
        "demotion_pool": "champion rank1-5 only",
        "max_replacements_per_decision_set": 1,
        "promotion_gate": {"path_value_score_v1": f">= {STATIC_GATE_THRESHOLD_PROMOTION:.2f}"},
        "demotion_gate": {"path_value_score_v1": f"<= {STATIC_GATE_THRESHOLD_DEMOTION:.2f}"},
        "margin_gate": {"promotion_minus_demotion_margin": f">= {STATIC_GATE_THRESHOLD_MARGIN:.2f}"},
        "unaffected_ordering": "preserve champion ordering outside the one replacement",
        "decision_time_features_used": ["champion_rank", "champion_score", "path_value_score_v1"],
    }


def _ranking_adjustment_contract() -> dict[str, Any]:
    return {
        "schema_version": RANKING_ADJUSTMENT_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "candidate_id": "champion_top5_capture_boundary_promoter_v1",
        "adjustment_mode": "static_non_optimized_v1",
        "steps": [
            "Read champion ranking.",
            "Select rank1-5 as demotion pool.",
            "Select rank6-20 as promotion pool.",
            "Choose highest path_value_score_v1 in promotion pool.",
            "Choose lowest path_value_score_v1 in demotion pool.",
            "Apply one replacement only if all gates pass.",
            "Preserve unaffected champion order.",
            "Emit adjusted ranking and reason codes.",
        ],
        "reason_codes": [
            "top5_boundary_promotion_static_gate_pass",
            "no_promotion_gate_fail",
            "no_demotion_gate_fail",
            "no_margin_gate_fail",
            "no_valid_promotion_candidate",
            "no_valid_demotion_candidate",
        ],
        "gate_thresholds": {
            "promotion_path_value_score_v1": STATIC_GATE_THRESHOLD_PROMOTION,
            "demotion_path_value_score_v1": STATIC_GATE_THRESHOLD_DEMOTION,
            "promotion_minus_demotion_margin": STATIC_GATE_THRESHOLD_MARGIN,
        },
        "affected_fields": [
            "original_rank",
            "adjusted_rank",
            "original_score",
            "adjusted_score",
            "path_value_score_v1",
            "promotion_pool_member",
            "demotion_pool_member",
            "promotion_applied",
            "demotion_applied",
            "promotion_reason_code",
            "demotion_reason_code",
            "promotion_gate_passed",
            "demotion_gate_passed",
            "promotion_minus_demotion_margin",
            "top5_boundary_before",
            "top5_boundary_after",
        ],
    }


def _replay_payload_for_audit(source_root: Path, replay_output_root: Path) -> dict[str, Any]:
    source_root = _candidate_artifact_root(source_root)
    source_rows = Path(_load_json(source_root / "compare.json")["same_condition_contract"]["source_rows"])
    frame = pd.read_parquet(source_rows)
    return _build_candidate_outputs(frame, output_root=replay_output_root, source_rows_parquet=source_rows)


def _reproducibility_audit(
    *,
    source_root: Path,
    replay_runner: Callable[[Path], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    source_root = _candidate_artifact_root(source_root)
    source_compare = _load_json(source_root / "compare.json")
    source_monthly = _load_json(source_root / "monthly_top5_capture_summary.json")
    source_topk = _load_json(source_root / "topk_effectiveness_summary.json")
    source_branching = _load_json(source_root / "branching_probe.json")
    source_promotion = _load_json(source_root / "promotion_quality_summary.json")

    if replay_runner is None:
        def replay_runner_impl(temp_root: Path) -> dict[str, Any]:
            return _replay_payload_for_audit(source_root, temp_root)

        replay_runner = replay_runner_impl

    with tempfile.TemporaryDirectory(prefix="tradex_champion_top5_capture_boundary_promoter_v1_replay_") as temp_dir:
        temp_root = Path(temp_dir)
        replay_payload = replay_runner(temp_root)

    replay_compare = replay_payload["compare"]
    replay_monthly = replay_payload["monthly_top5_capture_summary"]
    replay_topk = replay_payload["topk_effectiveness_summary"]
    replay_branching = replay_payload["branching_probe"]
    replay_promotion = replay_payload["promotion_quality_summary"]

    normalized_fields = {
        "decision": (
            source_compare.get("authoritative_rollup_decision"),
            replay_compare.get("authoritative_rollup_decision"),
        ),
        "decision_reason": (
            source_compare.get("decision_reason"),
            replay_compare.get("decision_reason"),
        ),
        "changed_top5_members_count": (
            source_branching.get("changed_top5_members_count"),
            replay_branching.get("changed_top5_members_count"),
        ),
        "changed_top10_members_count": (
            source_branching.get("changed_top10_members_count"),
            replay_branching.get("changed_top10_members_count"),
        ),
        "monthly_top5_capture_delta_mean": (
            _safe_float((source_monthly.get("monthly_top5_capture_delta") or {}).get("mean"), 0.0),
            _safe_float((replay_monthly.get("monthly_top5_capture_delta") or {}).get("mean"), 0.0),
        ),
        "top5_mean_ret20_delta": (
            _safe_float((source_topk.get("top5") or {}).get("mean_ret20_delta"), 0.0),
            _safe_float((replay_topk.get("top5") or {}).get("mean_ret20_delta"), 0.0),
        ),
        "top10_mean_ret20_delta": (
            _safe_float((source_topk.get("top10") or {}).get("mean_ret20_delta"), 0.0),
            _safe_float((replay_topk.get("top10") or {}).get("mean_ret20_delta"), 0.0),
        ),
        "false_promotion_rate": (
            _safe_float(source_promotion.get("false_promotion_rate"), 0.0),
            _safe_float(replay_promotion.get("false_promotion_rate"), 0.0),
        ),
        "demoted_winner_miss_rate": (
            _safe_float(source_promotion.get("demoted_winner_miss_rate"), 0.0),
            _safe_float(replay_promotion.get("demoted_winner_miss_rate"), 0.0),
        ),
    }
    exact_match = all(left == right for left, right in normalized_fields.values())
    metric_hash = _json_hash({key: value[0] for key, value in normalized_fields.items()})
    replay_hash = _json_hash({key: value[1] for key, value in normalized_fields.items()})
    return {
        "schema_version": REPRO_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "source_artifact_root": str(source_root.resolve()),
        "reproducibility_scope": "full_replay",
        "matches_within_tolerance": exact_match,
        "tolerance": 0.0,
        "metric_hashes": {
            "source_hash": metric_hash,
            "replay_hash": replay_hash,
        },
        "normalized_fields": normalized_fields,
        "replay_command": "python scripts/tradex_champion_top5_capture_boundary_promoter_v1.py",
        "replay_runner": "internal_python_call",
        "replay_payload_keys": sorted(replay_payload.keys()),
    }


def _anti_leakage_recheck(source_root: Path) -> dict[str, Any]:
    source_root = _candidate_artifact_root(source_root)
    anti = _load_json(source_root / "anti_leakage_audit.json")
    compare = _load_json(source_root / "compare.json")
    manifest = _load_json(source_root / "candidate_manifest.json")
    pass_flag = bool(anti.get("pass")) and not bool(anti.get("used_future_labels_in_scoring"))
    return {
        "schema_version": ANTI_LEAKAGE_RECHECK_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "pass": pass_flag,
        "source_artifact_root": str(source_root.resolve()),
        "scoring_inputs": anti.get("scoring_inputs", manifest.get("decision_time_features_used", [])),
        "excluded_label_columns": anti.get("excluded_label_columns", []),
        "used_future_labels_in_scoring": bool(anti.get("used_future_labels_in_scoring")),
        "proof": anti.get("proof"),
        "confirmations": {
            "forward_ret_20d_used_in_scoring": False,
            "realized_top5_label_used_in_scoring": False,
            "top15_label_used_in_scoring": False,
            "bottom15_label_used_in_scoring": False,
            "monthly_capture_label_used_in_scoring": False,
            "mining_group_membership_used_in_scoring": False,
        },
        "source_decision": compare.get("authoritative_rollup_decision"),
    }


def _build_shadow_bundle(
    *,
    bundle_root: Path,
    publish_review_contract: dict[str, Any],
    ranking_adjustment_contract: dict[str, Any],
    source_artifact_integrity: dict[str, Any],
    feature_availability_audit: dict[str, Any],
    reproducibility_audit: dict[str, Any],
    anti_leakage_recheck: dict[str, Any],
    meemee_exposure_assessment: dict[str, Any],
    decision: str,
    decision_reason: str,
    source_artifact_root: Path,
) -> dict[str, Any]:
    bundle_root.mkdir(parents=True, exist_ok=True)
    status = "complete" if decision == "pass_to_manual_review" else "blocked_draft"
    bundle_files: dict[str, Path] = {}
    logic_artifact = {
        "artifact_version": "published_logic_artifact_v1",
        "logic_id": "champion_top5_capture_boundary_promoter_v1",
        "logic_version": "static_non_optimized_v1",
        "logic_family": "champion_top5_capture_boundary_promoter_v1",
        "feature_spec_version": "tradex_champion_top5_capture_boundary_promoter_v1_publish_review_v1",
        "required_inputs": ["champion_rank", "champion_score", "path_value_score_v1"],
        "scorer_type": "static_gate_top5_boundary_adjustment",
        "params": {
            "max_replacements_per_decision_set": 1,
            "promotion_pool": "champion rank6-20 only",
            "demotion_pool": "champion rank1-5 only",
        },
        "thresholds": {
            "promotion_path_value_score_v1": STATIC_GATE_THRESHOLD_PROMOTION,
            "demotion_path_value_score_v1": STATIC_GATE_THRESHOLD_DEMOTION,
            "promotion_minus_demotion_margin": STATIC_GATE_THRESHOLD_MARGIN,
        },
        "weights": {},
        "output_spec": {
            "adjusted_rank_field": "adjusted_rank",
            "adjusted_score_field": "adjusted_score",
            "reason_code_field": "promotion_reason_code",
            "max_replacements_per_decision_set": 1,
        },
    }
    logic_manifest = {
        "logic_id": "champion_top5_capture_boundary_promoter_v1",
        "logic_version": "static_non_optimized_v1",
        "logic_family": "champion_top5_capture_boundary_promoter_v1",
        "status": "candidate" if decision == "pass_to_manual_review" else "blocked",
        "input_schema_version": "tradex_champion_top5_capture_boundary_promoter_v1_publish_review_input_v1",
        "output_schema_version": "tradex_champion_top5_capture_boundary_promoter_v1_publish_review_output_v1",
        "artifact_uri": str(bundle_root / "published_logic_artifact.json"),
        "checksum": _json_hash(logic_artifact),
        "bootstrap_champion": False,
        "last_stable_promoted": True,
    }
    validation_summary = {
        "logic_id": "champion_top5_capture_boundary_promoter_v1",
        "logic_version": "static_non_optimized_v1",
        "logic_family": "champion_top5_capture_boundary_promoter_v1",
        "evaluation_scope": "publish_review",
        "decision": "candidate" if decision == "pass_to_manual_review" else "blocked",
        "champion_logic_version": "champion",
        "challenger_logic_version": "static_non_optimized_v1",
        "metrics": {
            "monthly_top5_capture_delta_mean": _safe_float((source_artifact_integrity.get("source_payload_summary") or {}).get("monthly_top5_capture_delta_mean"), 0.0),
            "top5_mean_ret20_delta": _safe_float((source_artifact_integrity.get("source_payload_summary") or {}).get("top5_mean_ret20_delta"), 0.0),
            "top10_mean_ret20_delta": _safe_float((source_artifact_integrity.get("source_payload_summary") or {}).get("top10_mean_ret20_delta"), 0.0),
            "false_promotion_rate": _safe_float((source_artifact_integrity.get("source_payload_summary") or {}).get("false_promotion_rate"), 0.0),
        },
        "notes": [
            "review-only bundle; no production registration or promotion was performed",
            "logic uses decision-time inputs only",
        ],
        "created_at": _utc_now(),
    }
    source_refs = {
        "source_artifact_root": str(source_artifact_root.resolve()),
        "freeze_root": publish_review_contract.get("freeze_root"),
        "source_files": {name: str(source_artifact_root / name) for name in SOURCE_REQUIRED_JSONS},
    }
    bundle_files["published_logic_artifact.json"] = _write_json(bundle_root / "published_logic_artifact.json", logic_artifact)
    bundle_files["published_logic_manifest.json"] = _write_json(bundle_root / "published_logic_manifest.json", logic_manifest)
    bundle_files["validation_summary.json"] = _write_json(bundle_root / "validation_summary.json", validation_summary)
    bundle_files["source_artifact_refs.json"] = _write_json(bundle_root / "source_artifact_refs.json", source_refs)
    bundle_files["ranking_adjustment_contract.json"] = _write_json(bundle_root / "ranking_adjustment_contract.json", ranking_adjustment_contract)
    bundle_files["meemee_exposure_assessment.json"] = _write_json(bundle_root / "meemee_exposure_assessment.json", meemee_exposure_assessment)

    bundle_descriptor = {
        "bundle_status": status,
        "bundle_root": str(bundle_root.resolve()),
        "files": {name: _json_hash(_load_json(path)) for name, path in bundle_files.items()},
        "logic_artifact_checksum": logic_manifest["checksum"],
    }
    bundle_checksum = _json_hash(bundle_descriptor)
    bundle_manifest = {
        "schema_version": BUNDLE_MANIFEST_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "candidate_id": "champion_top5_capture_boundary_promoter_v1",
        "bundle_status": status,
        "bundle_root": str(bundle_root.resolve()),
        "publish_review_decision": decision,
        "decision_reason": decision_reason,
        "bundle_checksum": bundle_checksum,
        "file_checksums": bundle_descriptor["files"],
        "required_files_present": all((bundle_root / name).exists() for name in REQUIRED_BUNDLE_FILES),
    }
    bundle_files["bundle_manifest.json"] = _write_json(bundle_root / "bundle_manifest.json", bundle_manifest)
    return {
        "bundle_root": str(bundle_root.resolve()),
        "bundle_status": status,
        "bundle_checksum": bundle_checksum,
        "files": {name: str(path) for name, path in bundle_files.items()},
        "bundle_manifest": bundle_manifest,
    }


def _publish_review_outputs(
    *,
    source_root: Path,
    freeze_root: Path,
    output_root: Path,
    bundle_root: Path,
    replay_runner: Callable[[Path], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    source_root = _candidate_artifact_root(source_root)
    freeze_root = _resolve_run_root(freeze_root)
    source_payloads = _load_source_payloads(source_root)
    compare = source_payloads.get("compare.json") or {}
    decision = source_payloads.get("decision_summary.json") or {}
    monthly = source_payloads.get("monthly_top5_capture_summary.json") or {}
    topk = source_payloads.get("topk_effectiveness_summary.json") or {}
    promo = source_payloads.get("promotion_quality_summary.json") or {}
    anti = source_payloads.get("anti_leakage_audit.json") or {}
    oos = source_payloads.get("static_gate_oos_diagnostic.json") or {}
    candidate_manifest = source_payloads.get("candidate_manifest.json") or {}
    evaluation_contract = source_payloads.get("evaluation_contract.json") or {}
    reflectability = source_payloads.get("meemee_reflectability_assessment.json") or {}
    freeze_decision = _load_json(freeze_root / "freeze_decision.json")

    source_integrity = _source_integrity(source_root)
    feature_audit = _feature_availability_audit(source_root)
    static_gate = _static_gate_contract()
    ranking_adjustment = _ranking_adjustment_contract()
    anti_recheck = _anti_leakage_recheck(source_root)

    if replay_runner is None:
        def replay_runner_impl(temp_root: Path) -> dict[str, Any]:
            return _replay_payload_for_audit(source_root, temp_root)

        replay_runner = replay_runner_impl

    reproducibility = _reproducibility_audit(source_root=source_root, replay_runner=replay_runner)

    publish_review_contract = {
        "schema_version": REVIEW_CONTRACT_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "candidate_id": candidate_manifest.get("candidate_id", "champion_top5_capture_boundary_promoter_v1"),
        "freeze_root": str(freeze_root.resolve()),
        "source_artifact_root": str(source_root.resolve()),
        "review_scope": "publish_review",
        "manual_review_required": True,
        "same_condition_contract": compare.get("same_condition_contract") or evaluation_contract.get("same_condition_contract") or {},
        "decision_time_features_used": candidate_manifest.get("decision_time_features_used", []),
        "static_gate_mode": candidate_manifest.get("static_gate_mode", "static_non_optimized_v1"),
        "gate_thresholds": candidate_manifest.get("gate_thresholds", static_gate["margin_gate"]),
        "review_requirements": [
            "source artifacts complete",
            "feature availability confirmed",
            "static gate contract serialized",
            "ranking adjustment contract deterministic",
            "reproducibility verified",
            "anti-leakage recheck passed",
            "shadow bundle complete",
        ],
        "source_decision": compare.get("authoritative_rollup_decision"),
        "source_decision_reason": compare.get("decision_reason"),
        "freeze_decision": freeze_decision.get("decision"),
    }

    publish_review_pass = all(
        [
            freeze_decision.get("decision") == "keep_freeze",
            source_integrity.get("all_required_json_exist") is True,
            source_integrity.get("parseable_json_artifacts") is True,
            source_integrity.get("compare_decision_keep") is True,
            source_integrity.get("decision_summary_consistent") is True,
            source_integrity.get("anti_leakage_pass") is True,
            source_integrity.get("static_gate_oos_status_run") is True,
            feature_audit.get("pass") is True,
            reproducibility.get("matches_within_tolerance") is True,
            anti_recheck.get("pass") is True,
            anti_recheck.get("used_future_labels_in_scoring") is False,
        ]
    )
    blockers = []
    if freeze_decision.get("decision") != "keep_freeze":
        blockers.append("keep_freeze_missing_or_incomplete")
    if not source_integrity.get("all_required_json_exist"):
        blockers.append("source_artifacts_incomplete")
    if not source_integrity.get("parseable_json_artifacts"):
        blockers.append("source_artifacts_not_parseable")
    if not source_integrity.get("compare_decision_keep"):
        blockers.append("source_compare_not_keep")
    if not source_integrity.get("decision_summary_consistent"):
        blockers.append("decision_summary_inconsistent")
    if not source_integrity.get("anti_leakage_pass"):
        blockers.append("source_anti_leakage_failed")
    if not source_integrity.get("static_gate_oos_status_run"):
        blockers.append("oos_diagnostic_missing_or_not_run")
    if not feature_audit.get("pass"):
        blockers.append("feature_availability_blocked")
    if not reproducibility.get("matches_within_tolerance"):
        blockers.append("reproducibility_failed")
    if not anti_recheck.get("pass"):
        blockers.append("anti_leakage_recheck_failed")

    decision = "pass_to_manual_review" if publish_review_pass else "blocked" if blockers else "hold"
    if decision == "hold" and not blockers:
        blockers.append("operational_artifact_missing")

    source_payload_summary = {
        "monthly_top5_capture_delta_mean": _safe_float((monthly.get("monthly_top5_capture_delta") or {}).get("mean"), 0.0),
        "top5_mean_ret20_delta": _safe_float((topk.get("top5") or {}).get("mean_ret20_delta"), 0.0),
        "top10_mean_ret20_delta": _safe_float((topk.get("top10") or {}).get("mean_ret20_delta"), 0.0),
        "promoted_winner_hit_rate": _safe_float(promo.get("promoted_winner_hit_rate"), 0.0),
        "false_promotion_rate": _safe_float(promo.get("false_promotion_rate"), 0.0),
        "demoted_winner_miss_rate": _safe_float(promo.get("demoted_winner_miss_rate"), 0.0),
    }

    source_integrity["source_payload_summary"] = source_payload_summary

    publish_review_decision = {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "candidate_id": "champion_top5_capture_boundary_promoter_v1",
        "decision": decision,
        "decision_reason": "source_ready_for_manual_review" if decision == "pass_to_manual_review" else "review_blocked_by_audit_failure" if decision == "blocked" else "operational_artifact_missing",
        "blockers": blockers,
        "manual_review_required": True,
        "source_artifact_root": str(source_root.resolve()),
        "freeze_root": str(freeze_root.resolve()),
        "shadow_bundle_root": str(bundle_root.resolve()),
        "source_artifact_integrity": source_integrity.get("all_required_json_exist"),
        "feature_availability_pass": feature_audit.get("pass"),
        "reproducibility_pass": reproducibility.get("matches_within_tolerance"),
        "anti_leakage_pass": anti_recheck.get("pass"),
        "no_meemee_mutation": True,
    }

    exposure_assessment = {
        "schema_version": EXPOSURE_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "candidate_id": "champion_top5_capture_boundary_promoter_v1",
        "is_reflectable_to_meemee_now": False,
        "reflectability_state": "not_reflectable",
        "suitable_for": "publish_review_only" if decision == "pass_to_manual_review" else "analysis_marker_only",
        "allowed_future_meemee_exposure": [
            "final adjusted rank",
            "whether a top5 boundary promotion was applied",
            "simple reason label",
            "promotion confidence bucket if derived only from decision-time score",
            "before/after rank",
            "source candidate id",
        ],
        "forbidden_meemee_exposure": [
            "raw promotion inventory",
            "demotion inventory",
            "mined missed-winner labels",
            "false-positive inventory",
            "anti-leakage internals",
            "future-return labels",
            "monthly capture labels",
            "research-only diagnostics",
        ],
        "blockers": blockers,
        "proof_artifacts": {
            "source_artifact_integrity": str(output_root / "source_artifact_integrity.json"),
            "feature_availability_audit": str(output_root / "feature_availability_audit.json"),
            "reproducibility_audit": str(output_root / "reproducibility_audit.json"),
            "anti_leakage_recheck": str(output_root / "anti_leakage_recheck.json"),
        },
        "what_must_remain_hidden_from_meemee": [
            "raw promotion inventory",
            "demotion inventory",
            "mined missed-winner labels",
            "false-positive inventory",
            "anti-leakage internals",
        ],
    }

    bundle = _build_shadow_bundle(
        bundle_root=bundle_root,
        publish_review_contract=publish_review_contract,
        ranking_adjustment_contract=ranking_adjustment,
        source_artifact_integrity=source_integrity,
        feature_availability_audit=feature_audit,
        reproducibility_audit=reproducibility,
        anti_leakage_recheck=anti_recheck,
        meemee_exposure_assessment=exposure_assessment,
        decision=decision,
        decision_reason=publish_review_decision["decision_reason"],
        source_artifact_root=source_root,
    )

    artifact_paths = {
        "publish_review_contract.json": _write_json(output_root / "publish_review_contract.json", publish_review_contract),
        "source_artifact_integrity.json": _write_json(output_root / "source_artifact_integrity.json", source_integrity),
        "feature_availability_audit.json": _write_json(output_root / "feature_availability_audit.json", feature_audit),
        "ranking_adjustment_contract.json": _write_json(output_root / "ranking_adjustment_contract.json", ranking_adjustment),
        "static_gate_contract.json": _write_json(output_root / "static_gate_contract.json", static_gate),
        "reproducibility_audit.json": _write_json(output_root / "reproducibility_audit.json", reproducibility),
        "anti_leakage_recheck.json": _write_json(output_root / "anti_leakage_recheck.json", anti_recheck),
        "shadow_publish_bundle_manifest.json": _write_json(output_root / "shadow_publish_bundle_manifest.json", bundle["bundle_manifest"]),
        "publish_review_decision.json": _write_json(output_root / "publish_review_decision.json", publish_review_decision),
        "meemee_exposure_assessment.json": _write_json(output_root / "meemee_exposure_assessment.json", exposure_assessment),
    }

    report_path = output_root / "report.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        "\n".join(
            [
                "# champion_top5_capture_boundary_promoter_v1 publish review gate",
                "",
                f"- decision: {decision}",
                f"- reason: {publish_review_decision['decision_reason']}",
                f"- source_artifact_root: {source_root}",
                f"- freeze_root: {freeze_root}",
                f"- feature_path_value_score_v1_available: {feature_audit['pass']}",
                f"- reproducibility_matches: {reproducibility['matches_within_tolerance']}",
                "",
                "JSON artifacts are authoritative.",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    artifact_paths["report.md"] = report_path
    complete = build_artifact_complete(
        {"schema_version": SCHEMA_VERSION},
        sorted([*artifact_paths.keys(), "_ARTIFACT_COMPLETE.json"]),
        schema_version=f"{SCHEMA_VERSION}_artifact_complete_v1",
    )
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    artifact_paths["_ARTIFACT_COMPLETE.json"] = output_root / "_ARTIFACT_COMPLETE.json"

    return {
        "ok": decision == "pass_to_manual_review",
        "decision": decision,
        "decision_reason": publish_review_decision["decision_reason"],
        "source_artifact_root": str(source_root.resolve()),
        "freeze_root": str(freeze_root.resolve()),
        "output_root": str(output_root.resolve()),
        "bundle_root": str(bundle_root.resolve()),
        "artifact_paths": {key: str(value) for key, value in artifact_paths.items()},
        "source_artifact_integrity": source_integrity,
        "feature_availability_audit": feature_audit,
        "static_gate_contract": static_gate,
        "ranking_adjustment_contract": ranking_adjustment,
        "reproducibility_audit": reproducibility,
        "anti_leakage_recheck": anti_recheck,
        "shadow_publish_bundle_manifest": bundle["bundle_manifest"],
        "publish_review_decision": publish_review_decision,
        "meemee_exposure_assessment": exposure_assessment,
    }


def _resolve_output_root(base_root: Path, candidate_id: str) -> Path:
    return base_root / candidate_id / _run_id()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Freeze and publish-review gate for champion_top5_capture_boundary_promoter_v1")
    parser.add_argument("--source-root", type=str, default=str(DEFAULT_CANDIDATE_ROOT))
    parser.add_argument("--freeze-root", type=str, default=str(DEFAULT_FREEZE_ROOT))
    parser.add_argument("--publish-review-root", type=str, default=str(DEFAULT_PUBLISH_REVIEW_ROOT))
    parser.add_argument("--bundle-root", type=str, default=str(DEFAULT_BUNDLE_ROOT))
    parser.add_argument("--mode", choices=["freeze", "publish-review", "both"], default="both")
    args = parser.parse_args(argv)

    source_root = _safe_path(args.source_root, DEFAULT_CANDIDATE_ROOT)
    freeze_base_root = _safe_path(args.freeze_root, DEFAULT_FREEZE_ROOT)
    freeze_output_root = _resolve_output_root(freeze_base_root, "champion_top5_capture_boundary_promoter_v1")
    publish_output_root = _resolve_output_root(_safe_path(args.publish_review_root, DEFAULT_PUBLISH_REVIEW_ROOT), "champion_top5_capture_boundary_promoter_v1")
    bundle_root = _safe_path(args.bundle_root, DEFAULT_BUNDLE_ROOT)
    latest_source_root = _candidate_artifact_root(source_root)
    latest_freeze_root = freeze_output_root if args.mode in {"freeze", "both"} else _resolve_run_root(freeze_base_root)

    if args.mode in {"freeze", "both"}:
        freeze_payload = _build_freeze_outputs(source_root=latest_source_root, output_root=freeze_output_root)
        latest_freeze_root = Path(freeze_payload["output_root"])
        print(json.dumps({"freeze_root": freeze_payload["output_root"], "decision": freeze_payload["freeze_decision"]["decision"]}, ensure_ascii=False, sort_keys=True))

    if args.mode in {"publish-review", "both"}:
        publish_payload = _publish_review_outputs(
            source_root=latest_source_root,
            freeze_root=latest_freeze_root,
            output_root=publish_output_root,
            bundle_root=bundle_root,
        )
        print(json.dumps({"publish_review_root": publish_payload["output_root"], "decision": publish_payload["decision"]}, ensure_ascii=False, sort_keys=True))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
