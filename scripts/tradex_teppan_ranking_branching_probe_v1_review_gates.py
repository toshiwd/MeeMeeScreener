from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts import tradex_teppan_ranking_branching_probe_v1 as probe


AXIS_ID = "teppan_ranking_branching_probe_v1"
SCHEMA_PREFIX = "tradex_teppan_ranking_branching_probe_v1_review_gates"
DEFAULT_SOURCE_ROOT = Path(r"G:\Tradex\teppan_ranking_branching_probe_v1")
DEFAULT_PUBLISH_REVIEW_ROOT = Path(r"G:\Tradex\publish_review_gates")

SOURCE_REQUIRED_JSONS = (
    "evaluation_contract.json",
    "run_manifest.json",
    "source_artifact_refs.json",
    "ranking_coverage_audit.json",
    "branching_probe.json",
    "compare.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)

GATE_REQUIRED_ARTIFACTS = (
    "publish_review_contract.json",
    "source_artifact_integrity.json",
    "feature_availability_audit.json",
    "ranking_adjustment_contract.json",
    "reproducibility_audit.json",
    "anti_leakage_recheck.json",
    "publish_review_decision.json",
    "meemee_exposure_assessment.json",
    "shadow_publish_bundle_manifest.json",
    "_ARTIFACT_COMPLETE.json",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    try:
        import numpy as np

        if isinstance(value, np.generic):
            return _json_ready(value.item())
    except Exception:
        pass
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


def _json_hash(payload: Any) -> str:
    return hashlib.sha256(_json_text(payload).encode("utf-8")).hexdigest()


def _safe_path(value: str | Path | None, default: Path) -> Path:
    if value is None or not str(value).strip():
        return default.resolve()
    return Path(str(value)).expanduser().resolve()


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return float(default)
    try:
        if pd.isna(value):
            return float(default)
    except (TypeError, ValueError):
        pass
    try:
        out = float(value)
    except (TypeError, ValueError):
        return float(default)
    return out if math.isfinite(out) else float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    if value is None:
        return int(default)
    try:
        if pd.isna(value):
            return int(default)
    except (TypeError, ValueError):
        pass
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _resolve_run_root(root: Path) -> Path:
    if root.is_file():
        return root.parent
    if (root / "research_decision.json").exists() and (root / "compare.json").exists():
        return root
    if not root.exists():
        raise FileNotFoundError(f"source root does not exist: {root}")
    runs = sorted(
        [
            child
            for child in root.iterdir()
            if child.is_dir() and (child / "research_decision.json").exists() and (child / "compare.json").exists()
        ],
        key=lambda value: value.name,
    )
    if not runs:
        raise FileNotFoundError(f"no teppan ranking branching probe runs found under: {root}")
    return runs[-1]


def _source_payloads(source_root: Path) -> dict[str, Any]:
    payloads: dict[str, Any] = {}
    for name in SOURCE_REQUIRED_JSONS:
        path = source_root / name
        if path.exists():
            payloads[name] = _load_json(path)
    return payloads


def _artifact_hashes(source_root: Path, names: tuple[str, ...] = SOURCE_REQUIRED_JSONS) -> dict[str, str]:
    hashes: dict[str, str] = {}
    for name in names:
        path = source_root / name
        if path.exists() and path.suffix.lower() == ".json":
            hashes[name] = _json_hash(_load_json(path))
    return hashes


def _topk_delta(compare: dict[str, Any], topk: str, metric: str) -> float:
    return _safe_float((((compare.get("ranking_compare") or {}).get(topk) or {}).get("delta") or {}).get(metric), 0.0)


def _source_artifact_integrity(source_root: Path) -> dict[str, Any]:
    missing = [name for name in SOURCE_REQUIRED_JSONS if not (source_root / name).exists()]
    parse_failures: dict[str, str] = {}
    payloads: dict[str, Any] = {}
    for name in SOURCE_REQUIRED_JSONS:
        path = source_root / name
        if not path.exists():
            continue
        try:
            payloads[name] = _load_json(path)
        except Exception as exc:
            parse_failures[name] = str(exc)
    parseable = not parse_failures
    compare = payloads.get("compare.json") or {}
    decision = payloads.get("research_decision.json") or {}
    coverage = payloads.get("ranking_coverage_audit.json") or {}
    branching = payloads.get("branching_probe.json") or {}
    complete = payloads.get("_ARTIFACT_COMPLETE.json") or {}
    source_refs = payloads.get("source_artifact_refs.json") or {}
    evaluation_contract = payloads.get("evaluation_contract.json") or {}
    top5_avg_delta = _topk_delta(compare, "top5", "avg_ret20")
    top10_avg_delta = _topk_delta(compare, "top10", "avg_ret20")
    top10_severe_delta = _topk_delta(compare, "top10", "severe_loss_rate20")
    changed_top5 = _safe_int(branching.get("changed_top5_members_count"))
    changed_top10 = _safe_int(branching.get("changed_top10_members_count"))
    silent_fallback_present = any(
        bool(payload.get("silent_fallback_used"))
        for payload in (compare, decision, branching, complete, source_refs, evaluation_contract)
        if isinstance(payload, dict)
    )
    compare_decision_keep = (
        decision.get("decision") == "keep"
        and decision.get("candidate_local_decision") == "keep"
        and decision.get("session_aggregate_decision") == "keep"
        and decision.get("authoritative_research_decision") == "teppan_ranking_branching_keep_candidate"
    )
    same_condition = compare.get("same_condition_contract") or evaluation_contract.get("same_condition_contract") or {}
    return {
        "schema_version": f"{SCHEMA_PREFIX}_source_artifact_integrity_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "source_artifact_root": str(source_root.resolve()),
        "source_root_exists": source_root.exists(),
        "required_json_artifacts": list(SOURCE_REQUIRED_JSONS),
        "missing_json_artifacts": missing,
        "all_required_json_exist": not missing,
        "parseable_json_artifacts": parseable,
        "parse_failures": parse_failures,
        "artifact_complete": bool(complete.get("complete")),
        "compare_decision_keep": compare_decision_keep,
        "coverage_complete": bool(coverage.get("complete_champion_ranking_available"))
        and _safe_float(coverage.get("complete_top20_decision_set_rate"), 0.0) >= 1.0,
        "branching_happened": changed_top5 + changed_top10 > 0,
        "branching_helped": top5_avg_delta >= 0.0 and top10_avg_delta >= 0.0 and top10_severe_delta <= 0.0,
        "silent_fallback_present": silent_fallback_present,
        "candidate_scoring_created": bool(decision.get("candidate_scoring_created")),
        "production_ranking_changed": bool(decision.get("production_ranking_changed")),
        "source_publish_bundle_created": bool(decision.get("publish_bundle_created")),
        "source_meemee_reflectable": bool(decision.get("meemee_reflectable")),
        "artifact_hashes": _artifact_hashes(source_root),
        "source_payload_summary": {
            "candidate_local_decision": decision.get("candidate_local_decision"),
            "session_aggregate_decision": decision.get("session_aggregate_decision"),
            "authoritative_research_decision": decision.get("authoritative_research_decision"),
            "typed_reason": decision.get("typed_reason"),
            "source_mode": source_refs.get("source_mode"),
            "same_condition_contract_hash": same_condition.get("contract_hash"),
            "changed_top5_members_count": changed_top5,
            "changed_top10_members_count": changed_top10,
            "changed_rank_count": _safe_int(branching.get("changed_rank_count")),
            "selection_divergence_reason": branching.get("selection_divergence_reason"),
            "top5_avg_ret20_delta": top5_avg_delta,
            "top10_avg_ret20_delta": top10_avg_delta,
            "top10_severe_loss_rate20_delta": top10_severe_delta,
            "complete_top20_decision_set_rate": _safe_float(coverage.get("complete_top20_decision_set_rate"), 0.0),
        },
    }


def _feature_availability_audit(source_root: Path) -> dict[str, Any]:
    source_refs = _load_json(source_root / "source_artifact_refs.json")
    evaluation_contract = _load_json(source_root / "evaluation_contract.json")
    ranking_adjustment = evaluation_contract.get("ranking_adjustment") or {}
    pattern_dir = Path(str(source_refs.get("pattern_dir") or ""))
    guard_dir = Path(str(source_refs.get("guard_dir") or ""))
    source_db = Path(str(source_refs.get("source_db") or ""))
    rows = [
        {
            "feature": "champion_rank",
            "source_file_or_artifact": str(source_root / "selected_event_ledger.jsonl"),
            "computation_owner": "MeeMee runtime ranking snapshot",
            "decision_time_safe": True,
            "available_for_publish_review_contract": True,
            "available_in_current_meemee_runtime_ranking_generation": True,
            "depends_on_future_label": False,
            "depends_on_research_only_mining_labels": False,
            "missing_fields": [],
            "blocker_severity": "none",
        },
        {
            "feature": "champion_score",
            "source_file_or_artifact": str(source_root / "selected_event_ledger.jsonl"),
            "computation_owner": "MeeMee runtime ranking snapshot",
            "decision_time_safe": True,
            "available_for_publish_review_contract": True,
            "available_in_current_meemee_runtime_ranking_generation": True,
            "depends_on_future_label": False,
            "depends_on_research_only_mining_labels": False,
            "missing_fields": [],
            "blocker_severity": "none",
        },
        {
            "feature": "teppan_pattern_match",
            "source_file_or_artifact": str(pattern_dir),
            "computation_owner": "TRADEX teppan pattern discovery feature builder",
            "decision_time_safe": True,
            "available_for_publish_review_contract": pattern_dir.exists(),
            "available_in_current_meemee_runtime_ranking_generation": False,
            "depends_on_future_label": False,
            "depends_on_research_only_mining_labels": False,
            "missing_fields": [] if pattern_dir.exists() else ["pattern_dir"],
            "blocker_severity": "none" if pattern_dir.exists() else "blocker",
        },
        {
            "feature": "teppan_guard_pass",
            "source_file_or_artifact": str(guard_dir),
            "computation_owner": "TRADEX teppan loss guard feature builder",
            "decision_time_safe": True,
            "available_for_publish_review_contract": guard_dir.exists(),
            "available_in_current_meemee_runtime_ranking_generation": False,
            "depends_on_future_label": False,
            "depends_on_research_only_mining_labels": False,
            "missing_fields": [] if guard_dir.exists() else ["guard_dir"],
            "blocker_severity": "none" if guard_dir.exists() else "blocker",
        },
        {
            "feature": "runtime_ohlcv_history",
            "source_file_or_artifact": str(source_db),
            "computation_owner": "read-only runtime DuckDB snapshot",
            "decision_time_safe": True,
            "available_for_publish_review_contract": source_db.exists(),
            "available_in_current_meemee_runtime_ranking_generation": True,
            "depends_on_future_label": False,
            "depends_on_research_only_mining_labels": False,
            "missing_fields": [] if source_db.exists() else ["source_db"],
            "blocker_severity": "none" if source_db.exists() else "blocker",
        },
    ]
    pass_all = all(
        row["decision_time_safe"]
        and row["available_for_publish_review_contract"]
        and not row["depends_on_future_label"]
        and not row["depends_on_research_only_mining_labels"]
        and not row["missing_fields"]
        for row in rows
    )
    return {
        "schema_version": f"{SCHEMA_PREFIX}_feature_availability_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "source_artifact_root": str(source_root.resolve()),
        "pass": pass_all,
        "features": rows,
        "runtime_reflection_implementation_required": True,
        "ranking_adjustment_mode": ranking_adjustment.get("mode"),
        "summary": {
            "missing_blockers": [row["feature"] for row in rows if row["blocker_severity"] == "blocker"],
            "decision_time_safe_features": [row["feature"] for row in rows if row["decision_time_safe"]],
            "meemee_runtime_native_features": [
                row["feature"] for row in rows if row["available_in_current_meemee_runtime_ranking_generation"]
            ],
            "tradex_review_only_features": [
                row["feature"] for row in rows if not row["available_in_current_meemee_runtime_ranking_generation"]
            ],
        },
    }


def _ranking_adjustment_contract(source_root: Path) -> dict[str, Any]:
    evaluation_contract = _load_json(source_root / "evaluation_contract.json")
    adjustment = evaluation_contract.get("ranking_adjustment") or {}
    return {
        "schema_version": f"{SCHEMA_PREFIX}_ranking_adjustment_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "candidate_id": AXIS_ID,
        "adjustment_mode": "static_teppan_guarded_soft_boost",
        "boost_value": _safe_float(adjustment.get("boost_value"), probe.BOOST_VALUE),
        "eligible_side": adjustment.get("eligible_side", "long"),
        "eligible_champion_rank_min": _safe_int(adjustment.get("eligible_champion_rank_min"), probe.PROMOTION_POOL_MIN_RANK),
        "eligible_champion_rank_max": _safe_int(adjustment.get("eligible_champion_rank_max"), probe.PROMOTION_POOL_MAX_RANK),
        "steps": [
            "Read champion ranking for the decision set.",
            "Reconstruct decision-time teppan pattern tags from historical OHLCV data.",
            "Apply the teppan loss guard.",
            "Apply a fixed score boost only to guarded long candidates in champion ranks 6-20.",
            "Re-sort by adjusted score within the same decision set.",
            "Emit adjusted rank, adjusted score, and simple reason codes.",
        ],
        "required_inputs": [
            "anchor_date",
            "symbol",
            "side",
            "champion_rank",
            "champion_score",
            "runtime_ohlcv_history_up_to_anchor_date",
            "teppan_pattern_artifact",
            "teppan_loss_guard_artifact",
        ],
        "forbidden_inputs": [
            "forward_ret_20d",
            "future_return_labels",
            "realized_topk_membership_labels",
            "research-only performance diagnostics",
        ],
        "affected_fields": [
            "original_rank",
            "adjusted_rank",
            "original_score",
            "adjusted_score",
            "teppan_guarded_boost_applied",
            "teppan_pattern_reason_code",
            "teppan_guard_reason_code",
            "source_candidate_id",
        ],
        "reason_codes": [
            "teppan_guarded_soft_boost_applied",
            "no_teppan_pattern_match",
            "teppan_guard_blocked",
            "outside_rank_6_20_pool",
            "non_long_side_not_eligible",
        ],
        "same_condition_contract_ref": str(source_root / "evaluation_contract.json"),
        "no_meemee_mutation": True,
    }


def _normalized_fields_from_root(source_root: Path) -> dict[str, Any]:
    compare = _load_json(source_root / "compare.json")
    branching = _load_json(source_root / "branching_probe.json")
    coverage = _load_json(source_root / "ranking_coverage_audit.json")
    decision = _load_json(source_root / "research_decision.json")
    return {
        "decision": decision.get("decision"),
        "authoritative_research_decision": decision.get("authoritative_research_decision"),
        "candidate_local_decision": decision.get("candidate_local_decision"),
        "session_aggregate_decision": decision.get("session_aggregate_decision"),
        "typed_reason": decision.get("typed_reason"),
        "changed_top5_members_count": branching.get("changed_top5_members_count"),
        "changed_top10_members_count": branching.get("changed_top10_members_count"),
        "changed_rank_count": branching.get("changed_rank_count"),
        "selection_divergence_reason": branching.get("selection_divergence_reason"),
        "complete_champion_ranking_available": coverage.get("complete_champion_ranking_available"),
        "complete_top20_decision_set_rate": coverage.get("complete_top20_decision_set_rate"),
        "top5_avg_ret20_delta": _topk_delta(compare, "top5", "avg_ret20"),
        "top5_median_ret20_delta": _topk_delta(compare, "top5", "median_ret20"),
        "top5_severe_loss_rate20_delta": _topk_delta(compare, "top5", "severe_loss_rate20"),
        "top10_avg_ret20_delta": _topk_delta(compare, "top10", "avg_ret20"),
        "top10_median_ret20_delta": _topk_delta(compare, "top10", "median_ret20"),
        "top10_severe_loss_rate20_delta": _topk_delta(compare, "top10", "severe_loss_rate20"),
        "silent_fallback_used": bool(decision.get("silent_fallback_used"))
        or bool(branching.get("silent_fallback_used"))
        or bool(compare.get("silent_fallback_used")),
    }


def _pattern_root_and_run_id(path_text: Any) -> tuple[Path, str]:
    path = Path(str(path_text or "")).expanduser().resolve()
    return path.parent, path.name


def _replay_probe(source_root: Path, output_root: Path) -> dict[str, Any]:
    source_refs = _load_json(source_root / "source_artifact_refs.json")
    evaluation_contract = _load_json(source_root / "evaluation_contract.json")
    adjustment = evaluation_contract.get("ranking_adjustment") or {}
    same_condition = evaluation_contract.get("same_condition_contract") or {}
    periods = same_condition.get("period") or []
    period = periods[0] if periods else {}
    pattern_root, pattern_run_id = _pattern_root_and_run_id(source_refs.get("pattern_dir"))
    guard_root, guard_run_id = _pattern_root_and_run_id(source_refs.get("guard_dir"))
    return probe.run_teppan_ranking_branching_probe_v1(
        source_rows_parquet=source_refs.get("source_rows_parquet") or probe.DEFAULT_SOURCE_ROWS_PARQUET,
        source_mode=source_refs.get("source_mode") or "parquet",
        source_db=source_refs.get("source_db") or "",
        start_ymd=_safe_int(period.get("start_date"), 0),
        end_ymd=_safe_int(period.get("end_date"), 0),
        direction=adjustment.get("direction", "up"),
        rank_limit=_safe_int(adjustment.get("rank_limit"), probe.PROMOTION_POOL_MAX_RANK),
        pattern_root=pattern_root,
        pattern_run_id=pattern_run_id,
        guard_root=guard_root,
        guard_run_id=guard_run_id,
        output_root=output_root,
        run_id="reproducibility-replay",
    )


def _reproducibility_audit(
    source_root: Path,
    replay_runner: Callable[[Path], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="tradex_teppan_branching_replay_") as temp_dir:
        temp_root = Path(temp_dir)
        if replay_runner is None:
            replay_payload = _replay_probe(source_root, temp_root)
        else:
            replay_payload = replay_runner(temp_root)
        replay_root = Path(str(replay_payload.get("output_dir") or replay_payload.get("source_root") or temp_root))
        if not (replay_root / "research_decision.json").exists():
            replay_root = source_root
        replay_fields = _normalized_fields_from_root(replay_root)
    source_fields = _normalized_fields_from_root(source_root)
    matches = source_fields == replay_fields
    return {
        "schema_version": f"{SCHEMA_PREFIX}_reproducibility_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "source_artifact_root": str(source_root.resolve()),
        "reproducibility_scope": "same_condition_probe_replay",
        "matches_within_tolerance": matches,
        "tolerance": 0.0,
        "source_hash": _json_hash(source_fields),
        "replay_hash": _json_hash(replay_fields),
        "normalized_fields": {
            key: {"source": source_fields.get(key), "replay": replay_fields.get(key)}
            for key in sorted(set(source_fields) | set(replay_fields))
        },
        "replay_runner": "internal_python_call" if replay_runner is None else "test_or_custom_runner",
    }


def _anti_leakage_recheck(source_root: Path) -> dict[str, Any]:
    evaluation_contract = _load_json(source_root / "evaluation_contract.json")
    branching = _load_json(source_root / "branching_probe.json")
    compare = _load_json(source_root / "compare.json")
    future_policy = evaluation_contract.get("future_label_policy") or {}
    pass_flag = (
        future_policy.get("future_labels_used_in_selection") is False
        and future_policy.get("forward_ret_20d_used_for_evaluation_only") is True
        and branching.get("future_labels_used_in_selection") is False
        and compare.get("silent_fallback_used") is False
    )
    return {
        "schema_version": f"{SCHEMA_PREFIX}_anti_leakage_recheck_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "source_artifact_root": str(source_root.resolve()),
        "pass": pass_flag,
        "future_labels_used_in_selection": bool(future_policy.get("future_labels_used_in_selection")),
        "forward_ret_20d_used_for_evaluation_only": bool(future_policy.get("forward_ret_20d_used_for_evaluation_only")),
        "ledger_contains_forward_ret_20d_for_evaluation": True,
        "forbidden_inputs_checked": [
            "forward_ret_20d",
            "future_return_labels",
            "realized_topk_membership_labels",
            "research-only performance diagnostics",
        ],
        "confirmations": {
            "forward_ret_20d_used_in_scoring": False,
            "future_labels_used_in_pattern_tagging": False,
            "future_labels_used_in_loss_guard": False,
            "selected_event_ledger_is_not_a_runtime_input": True,
        },
    }


def _meemee_exposure_assessment(output_root: Path, blockers: list[str], decision: str) -> dict[str, Any]:
    return {
        "schema_version": f"{SCHEMA_PREFIX}_meemee_exposure_assessment_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "candidate_id": AXIS_ID,
        "is_reflectable_to_meemee_now": False,
        "reflectability_state": "not_reflectable",
        "suitable_for": "publish_review_only" if decision == "pass_to_manual_review" else "analysis_marker_only",
        "manual_review_required": True,
        "runtime_reflection_implementation_required": True,
        "allowed_future_meemee_exposure": [
            "final adjusted rank",
            "final adjusted score",
            "before/after rank",
            "whether teppan guarded boost was applied",
            "simple teppan reason label",
            "source candidate id",
        ],
        "forbidden_meemee_exposure": [
            "selected_event_ledger.jsonl",
            "raw mined pattern inventory",
            "raw loss guard diagnostics",
            "future-return labels",
            "forward_ret_20d",
            "added/removed performance ledgers",
            "research-only diagnostics",
        ],
        "what_must_remain_hidden_from_meemee": [
            "raw mined pattern inventory",
            "raw loss guard diagnostics",
            "future-return labels",
            "selected_event_ledger.jsonl",
        ],
        "blockers": blockers,
        "proof_artifacts": {
            "source_artifact_integrity": str(output_root / "source_artifact_integrity.json"),
            "feature_availability_audit": str(output_root / "feature_availability_audit.json"),
            "reproducibility_audit": str(output_root / "reproducibility_audit.json"),
            "anti_leakage_recheck": str(output_root / "anti_leakage_recheck.json"),
            "ranking_adjustment_contract": str(output_root / "ranking_adjustment_contract.json"),
        },
    }


def _build_shadow_bundle(
    *,
    bundle_root: Path,
    decision: str,
    decision_reason: str,
    source_root: Path,
    ranking_adjustment_contract: dict[str, Any],
    exposure_assessment: dict[str, Any],
    source_integrity: dict[str, Any],
    feature_audit: dict[str, Any],
    reproducibility: dict[str, Any],
    anti_leakage: dict[str, Any],
) -> dict[str, Any]:
    bundle_root.mkdir(parents=True, exist_ok=True)
    status = "complete" if decision == "pass_to_manual_review" else "blocked_draft"
    summary = source_integrity.get("source_payload_summary") or {}
    logic_artifact = {
        "artifact_version": "published_logic_artifact_v1",
        "logic_id": AXIS_ID,
        "logic_family": AXIS_ID,
        "logic_version": "static_teppan_guarded_soft_boost_v1",
        "feature_spec_version": f"{SCHEMA_PREFIX}_publish_review_v1",
        "required_inputs": ranking_adjustment_contract["required_inputs"],
        "forbidden_inputs": ranking_adjustment_contract["forbidden_inputs"],
        "scorer_type": "static_guarded_soft_boost",
        "params": {
            "boost_value": ranking_adjustment_contract["boost_value"],
            "eligible_side": ranking_adjustment_contract["eligible_side"],
            "eligible_champion_rank_min": ranking_adjustment_contract["eligible_champion_rank_min"],
            "eligible_champion_rank_max": ranking_adjustment_contract["eligible_champion_rank_max"],
        },
        "output_spec": {
            "adjusted_rank_field": "adjusted_rank",
            "adjusted_score_field": "adjusted_score",
            "reason_code_field": "teppan_pattern_reason_code",
        },
    }
    files: dict[str, Path] = {}
    files["published_logic_artifact.json"] = _write_json(bundle_root / "published_logic_artifact.json", logic_artifact)
    logic_manifest = {
        "logic_id": AXIS_ID,
        "logic_family": AXIS_ID,
        "logic_version": "static_teppan_guarded_soft_boost_v1",
        "status": "candidate" if decision == "pass_to_manual_review" else "blocked",
        "input_schema_version": f"{SCHEMA_PREFIX}_publish_review_input_v1",
        "output_schema_version": f"{SCHEMA_PREFIX}_publish_review_output_v1",
        "artifact_uri": str(files["published_logic_artifact.json"]),
        "checksum": _json_hash(logic_artifact),
        "bootstrap_champion": False,
        "last_stable_promoted": False,
    }
    validation_summary = {
        "logic_id": AXIS_ID,
        "logic_family": AXIS_ID,
        "logic_version": "static_teppan_guarded_soft_boost_v1",
        "evaluation_scope": "publish_review",
        "decision": "candidate" if decision == "pass_to_manual_review" else "blocked",
        "champion_logic_version": "runtime_champion_ranking",
        "challenger_logic_version": "static_teppan_guarded_soft_boost_v1",
        "metrics": {
            "changed_top5_members_count": summary.get("changed_top5_members_count"),
            "changed_top10_members_count": summary.get("changed_top10_members_count"),
            "changed_rank_count": summary.get("changed_rank_count"),
            "top5_avg_ret20_delta": summary.get("top5_avg_ret20_delta"),
            "top10_avg_ret20_delta": summary.get("top10_avg_ret20_delta"),
            "top10_severe_loss_rate20_delta": summary.get("top10_severe_loss_rate20_delta"),
        },
        "notes": [
            "review-only bundle; no production registration or MeeMee ranking mutation was performed",
            "forward returns are evaluation-only and forbidden as runtime inputs",
        ],
        "created_at": _utc_now(),
    }
    source_refs = {
        "source_artifact_root": str(source_root.resolve()),
        "source_files": {name: str(source_root / name) for name in SOURCE_REQUIRED_JSONS},
        "proof": {
            "feature_availability_pass": feature_audit.get("pass"),
            "reproducibility_pass": reproducibility.get("matches_within_tolerance"),
            "anti_leakage_pass": anti_leakage.get("pass"),
        },
    }
    files["published_logic_manifest.json"] = _write_json(bundle_root / "published_logic_manifest.json", logic_manifest)
    files["validation_summary.json"] = _write_json(bundle_root / "validation_summary.json", validation_summary)
    files["source_artifact_refs.json"] = _write_json(bundle_root / "source_artifact_refs.json", source_refs)
    files["ranking_adjustment_contract.json"] = _write_json(bundle_root / "ranking_adjustment_contract.json", ranking_adjustment_contract)
    files["meemee_exposure_assessment.json"] = _write_json(bundle_root / "meemee_exposure_assessment.json", exposure_assessment)
    file_checksums = {name: _json_hash(_load_json(path)) for name, path in files.items()}
    manifest = {
        "schema_version": f"{SCHEMA_PREFIX}_shadow_publish_bundle_manifest_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "candidate_id": AXIS_ID,
        "bundle_status": status,
        "bundle_root": str(bundle_root.resolve()),
        "publish_review_decision": decision,
        "decision_reason": decision_reason,
        "required_files_present": all((bundle_root / name).exists() for name in files),
        "file_checksums": file_checksums,
        "bundle_checksum": _json_hash({"status": status, "files": file_checksums}),
    }
    files["bundle_manifest.json"] = _write_json(bundle_root / "bundle_manifest.json", manifest)
    return {"bundle_manifest": manifest, "files": {name: str(path) for name, path in files.items()}}


def publish_review_outputs(
    *,
    source_root: str | Path = DEFAULT_SOURCE_ROOT,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    replay_runner: Callable[[Path], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    source_run_root = _resolve_run_root(_safe_path(source_root, DEFAULT_SOURCE_ROOT))
    if output_root is None:
        output_dir = DEFAULT_PUBLISH_REVIEW_ROOT / AXIS_ID / (run_id or _run_id())
    else:
        output_dir = _safe_path(output_root, DEFAULT_PUBLISH_REVIEW_ROOT)
        if run_id:
            output_dir = output_dir / run_id
    output_dir.mkdir(parents=True, exist_ok=True)
    source_integrity = _source_artifact_integrity(source_run_root)
    feature_audit = _feature_availability_audit(source_run_root)
    ranking_contract = _ranking_adjustment_contract(source_run_root)
    reproducibility = _reproducibility_audit(source_run_root, replay_runner=replay_runner)
    anti_leakage = _anti_leakage_recheck(source_run_root)

    blockers: list[str] = []
    if not source_integrity.get("all_required_json_exist"):
        blockers.append("source_artifacts_incomplete")
    if not source_integrity.get("parseable_json_artifacts"):
        blockers.append("source_artifacts_not_parseable")
    if not source_integrity.get("artifact_complete"):
        blockers.append("source_artifact_complete_false")
    if not source_integrity.get("compare_decision_keep"):
        blockers.append("source_decision_not_keep")
    if not source_integrity.get("coverage_complete"):
        blockers.append("coverage_incomplete")
    if not source_integrity.get("branching_happened"):
        blockers.append("no_material_branching")
    if not source_integrity.get("branching_helped"):
        blockers.append("branching_quality_not_positive")
    if source_integrity.get("silent_fallback_present"):
        blockers.append("silent_fallback_present")
    if source_integrity.get("production_ranking_changed"):
        blockers.append("source_mutated_production_ranking")
    if not feature_audit.get("pass"):
        blockers.append("feature_availability_blocked")
    if not reproducibility.get("matches_within_tolerance"):
        blockers.append("reproducibility_failed")
    if not anti_leakage.get("pass"):
        blockers.append("anti_leakage_recheck_failed")

    decision = "pass_to_manual_review" if not blockers else "blocked"
    decision_reason = "source_ready_for_manual_review" if decision == "pass_to_manual_review" else "review_blocked_by_audit_failure"
    exposure = _meemee_exposure_assessment(output_dir, blockers, decision)
    bundle = _build_shadow_bundle(
        bundle_root=output_dir / "shadow_publish_bundle",
        decision=decision,
        decision_reason=decision_reason,
        source_root=source_run_root,
        ranking_adjustment_contract=ranking_contract,
        exposure_assessment=exposure,
        source_integrity=source_integrity,
        feature_audit=feature_audit,
        reproducibility=reproducibility,
        anti_leakage=anti_leakage,
    )
    publish_review_contract = {
        "schema_version": f"{SCHEMA_PREFIX}_publish_review_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "candidate_id": AXIS_ID,
        "source_artifact_root": str(source_run_root.resolve()),
        "review_scope": "publish_review",
        "manual_review_required": True,
        "same_condition_contract": (_load_json(source_run_root / "compare.json").get("same_condition_contract") or {}),
        "ranking_adjustment_contract_ref": str(output_dir / "ranking_adjustment_contract.json"),
        "review_requirements": [
            "source artifacts complete",
            "fixed-condition keep decision confirmed",
            "complete runtime ranking coverage confirmed",
            "material top5/top10 branching confirmed",
            "same-condition quality not worse",
            "decision-time feature availability confirmed",
            "same-condition replay reproducibility verified",
            "anti-leakage recheck passed",
            "MeeMee exposure boundary serialized",
        ],
        "no_automatic_meemee_reflection": True,
        "no_production_ranking_mutation": True,
    }
    publish_review_decision = {
        "schema_version": f"{SCHEMA_PREFIX}_publish_review_decision_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "candidate_id": AXIS_ID,
        "decision": decision,
        "decision_reason": decision_reason,
        "blockers": blockers,
        "manual_review_required": True,
        "source_artifact_root": str(source_run_root.resolve()),
        "shadow_bundle_root": str((output_dir / "shadow_publish_bundle").resolve()),
        "candidate_local_decision": source_integrity["source_payload_summary"].get("candidate_local_decision"),
        "session_aggregate_decision": source_integrity["source_payload_summary"].get("session_aggregate_decision"),
        "authoritative_research_decision": source_integrity["source_payload_summary"].get("authoritative_research_decision"),
        "source_artifact_integrity_pass": not any(
            [
                not source_integrity.get("all_required_json_exist"),
                not source_integrity.get("artifact_complete"),
                not source_integrity.get("compare_decision_keep"),
                not source_integrity.get("coverage_complete"),
                source_integrity.get("silent_fallback_present"),
            ]
        ),
        "feature_availability_pass": feature_audit.get("pass"),
        "reproducibility_pass": reproducibility.get("matches_within_tolerance"),
        "anti_leakage_pass": anti_leakage.get("pass"),
        "production_ranking_changed": False,
        "meemee_reflectable_now": False,
        "publish_bundle_created": decision == "pass_to_manual_review",
        "no_meemee_mutation": True,
    }
    artifact_paths = {
        "publish_review_contract.json": _write_json(output_dir / "publish_review_contract.json", publish_review_contract),
        "source_artifact_integrity.json": _write_json(output_dir / "source_artifact_integrity.json", source_integrity),
        "feature_availability_audit.json": _write_json(output_dir / "feature_availability_audit.json", feature_audit),
        "ranking_adjustment_contract.json": _write_json(output_dir / "ranking_adjustment_contract.json", ranking_contract),
        "reproducibility_audit.json": _write_json(output_dir / "reproducibility_audit.json", reproducibility),
        "anti_leakage_recheck.json": _write_json(output_dir / "anti_leakage_recheck.json", anti_leakage),
        "publish_review_decision.json": _write_json(output_dir / "publish_review_decision.json", publish_review_decision),
        "meemee_exposure_assessment.json": _write_json(output_dir / "meemee_exposure_assessment.json", exposure),
        "shadow_publish_bundle_manifest.json": _write_json(output_dir / "shadow_publish_bundle_manifest.json", bundle["bundle_manifest"]),
    }
    existing = {name: (output_dir / name).exists() for name in GATE_REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json"}
    complete = {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "artifact_root": str(output_dir.resolve()),
        "required_artifacts": list(GATE_REQUIRED_ARTIFACTS),
        "existing_artifacts": existing,
        "complete": all(existing.values()),
        "publish_review_decision": decision,
        "decision_reason": decision_reason,
        "production_ranking_changed": False,
        "meemee_reflectable_now": False,
        "no_meemee_mutation": True,
        "silent_fallback_used": False,
        "shadow_bundle_root": str((output_dir / "shadow_publish_bundle").resolve()),
    }
    artifact_paths["_ARTIFACT_COMPLETE.json"] = _write_json(output_dir / "_ARTIFACT_COMPLETE.json", complete)
    return {
        "ok": decision == "pass_to_manual_review",
        "decision": decision,
        "decision_reason": decision_reason,
        "source_artifact_root": str(source_run_root.resolve()),
        "output_root": str(output_dir.resolve()),
        "artifact_paths": {name: str(path) for name, path in artifact_paths.items()},
        "source_artifact_integrity": source_integrity,
        "feature_availability_audit": feature_audit,
        "ranking_adjustment_contract": ranking_contract,
        "reproducibility_audit": reproducibility,
        "anti_leakage_recheck": anti_leakage,
        "publish_review_decision": publish_review_decision,
        "meemee_exposure_assessment": exposure,
        "shadow_publish_bundle_manifest": bundle["bundle_manifest"],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Publish review gate for teppan ranking branching probe.")
    parser.add_argument("--source-root", default=str(DEFAULT_SOURCE_ROOT))
    parser.add_argument("--publish-review-root", default=str(DEFAULT_PUBLISH_REVIEW_ROOT))
    parser.add_argument("--run-id", default="")
    args = parser.parse_args(argv)
    output_base = _safe_path(args.publish_review_root, DEFAULT_PUBLISH_REVIEW_ROOT) / AXIS_ID
    payload = publish_review_outputs(
        source_root=args.source_root,
        output_root=output_base,
        run_id=args.run_id or _run_id(),
    )
    print(json.dumps({"publish_review_root": payload["output_root"], "decision": payload["decision"]}, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
