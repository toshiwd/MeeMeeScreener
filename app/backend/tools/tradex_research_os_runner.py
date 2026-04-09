from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

from app.backend.services import tradex_research_contracts as tradex_contracts
from app.backend.services import tradex_research_decision_policy as decision_policy
from app.backend.services import tradex_research_preflight as preflight_service
from app.backend.services import tradex_research_trader_benchmark as trader_benchmark
from app.backend.services import tradex_research_trader_foundation as trader_foundation
from app.backend.services import tradex_research_os_contracts as os_contracts
from app.backend.services import tradex_research_os_store as os_store
from app.backend.services.tradex_experiment_store import family_compare_file, read_json, run_manifest_file
from app.backend.tools import tradex_research_runner as tradex_runner
from shared.tradex_storage import tradex_research_sessions_root


def _text(value: Any, fallback: str = "") -> str:
    if value is None:
        return fallback
    if isinstance(value, str):
        text = value.strip()
        return text or fallback
    text = str(value).strip()
    return text or fallback


def _slug(text: str) -> str:
    raw = re.sub(r"[^0-9A-Za-z._-]+", "-", _text(text))
    raw = raw.strip("-._")
    return raw or "session"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    return value


def _stable_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _repo_commit() -> str:
    completed = subprocess.run(["git", "rev-parse", "HEAD"], check=False, capture_output=True, text=True)
    if completed.returncode != 0:
        raise RuntimeError("failed to resolve repo commit")
    return completed.stdout.strip()


def _session_root() -> Path:
    root = tradex_research_sessions_root()
    root.mkdir(parents=True, exist_ok=True)
    return root


def _session_dir(session_id: str) -> Path:
    path = _session_root() / _slug(session_id)
    path.mkdir(parents=True, exist_ok=True)
    return path


def load_hypothesis(path: Path) -> dict[str, Any]:
    payload = os_store.read_json_object_strict(path, artifact_name="hypothesis")
    os_contracts.validate_hypothesis(payload)
    return payload


def _session_state_file(session_id: str) -> Path:
    return _session_dir(session_id) / "session.json"


def _session_family_leaderboard_file(session_id: str) -> Path:
    return _session_dir(session_id) / "family_leaderboard.json"


def _session_family_id(session_id: str, method_family: str) -> str:
    return f"tradex-research-{_slug(session_id)}-{_slug(method_family)}"


def _find_family_result(session_state: dict[str, Any], target_method_family: str) -> dict[str, Any]:
    matches = [
        item
        for item in session_state.get("family_results") or []
        if isinstance(item, dict) and _text(item.get("method_family")) == _text(target_method_family)
    ]
    if not matches:
        raise ValueError(f"target_method_family not found in session results: {target_method_family}")
    if len(matches) > 1:
        raise ValueError(f"target_method_family is ambiguous in session results: {target_method_family}")
    return matches[0]


def _family_leaderboard_row(family_leaderboard: dict[str, Any], target_method_family: str) -> dict[str, Any]:
    matches = [
        row
        for row in family_leaderboard.get("family_summary") or []
        if isinstance(row, dict) and _text(row.get("method_family")) == _text(target_method_family)
    ]
    if not matches:
        raise ValueError(f"target_method_family not found in family leaderboard: {target_method_family}")
    if len(matches) > 1:
        raise ValueError(f"target_method_family is ambiguous in family leaderboard: {target_method_family}")
    return matches[0]


def _compare_path_candidates(
    *,
    session_id: str,
    family_leaderboard: dict[str, Any],
    family_result: dict[str, Any],
    family_id: str,
) -> list[Path]:
    candidates: list[Path] = []
    session_compare_path = (_session_dir(session_id) / "compare.json").resolve()
    for raw_path in (
        family_result.get("compare_path"),
        family_leaderboard.get("source_compare_path"),
        str(family_compare_file(family_id)),
    ):
        path_text = _text(raw_path)
        if not path_text:
            continue
        path = Path(path_text).expanduser()
        try:
            if path.resolve() == session_compare_path:
                continue
        except Exception:
            pass
        if path not in candidates:
            candidates.append(path)
    return candidates


def _resolve_family_compare_artifact(
    *,
    session_id: str,
    target_method_family: str,
    session_state: dict[str, Any],
    family_leaderboard: dict[str, Any],
) -> tuple[str, Path, dict[str, Any], dict[str, Any], dict[str, Any]]:
    family_result = _find_family_result(session_state, target_method_family)
    family_row = _family_leaderboard_row(family_leaderboard, target_method_family)
    family_id_candidates = [
        _text(family_result.get("family_id")),
        _text(family_row.get("family_id")),
        _session_family_id(session_id, target_method_family),
    ]
    family_id_values = [item for item in family_id_candidates if item]
    if not family_id_values:
        raise ValueError(f"unable to resolve family_id for target_method_family: {target_method_family}")
    unique_family_ids = sorted(set(family_id_values))
    if len(unique_family_ids) > 1:
        raise ValueError(
            "target_method_family is ambiguous across resolved family_ids: "
            f"{target_method_family} -> {', '.join(unique_family_ids)}"
        )
    family_id = unique_family_ids[0]

    compare_path_candidates = _compare_path_candidates(
        session_id=session_id,
        family_leaderboard=family_leaderboard,
        family_result=family_result,
        family_id=family_id,
    )
    existing_candidates = [path.resolve() for path in compare_path_candidates if path.exists()]
    if not existing_candidates:
        raise ValueError(
            "family compare missing for target_method_family: "
            f"{target_method_family}; candidates={', '.join(str(path) for path in compare_path_candidates)}"
        )
    unique_existing = sorted({str(path) for path in existing_candidates})
    if len(unique_existing) > 1:
        raise ValueError(
            "family compare path is ambiguous for target_method_family: "
            f"{target_method_family}; candidates={', '.join(unique_existing)}"
        )
    family_compare_path = Path(unique_existing[0])
    family_compare = read_json(family_compare_path)
    tradex_contracts.validate_compare_artifact(family_compare)
    compare_family_id = _text(family_compare.get("family_id"))
    if compare_family_id and compare_family_id != family_id:
        raise ValueError(
            "family compare family_id mismatch: "
            f"expected={family_id}, actual={compare_family_id}, path={family_compare_path}"
        )
    return family_id, family_compare_path, family_compare, family_result, family_row


def _candidate_result(family_compare: dict[str, Any], family_row: dict[str, Any]) -> dict[str, Any]:
    candidate_results = [item for item in family_compare.get("candidate_results") or [] if isinstance(item, dict)]
    if not candidate_results:
        raise ValueError("family compare has no candidate_results")
    best_candidate_method_id = _text(family_row.get("best_candidate_method_id"))
    if best_candidate_method_id:
        for row in candidate_results:
            candidate_method = row.get("candidate_method") if isinstance(row.get("candidate_method"), dict) else {}
            if _text(candidate_method.get("method_id")) == best_candidate_method_id or _text(row.get("candidate_run_id")) == best_candidate_method_id:
                return row
    for row in candidate_results:
        if _text(row.get("candidate_local_decision")) == _text(family_row.get("best_candidate_decision")):
            return row
    return candidate_results[0]


def _comparison_scope(
    *,
    hypothesis: dict[str, Any],
    session_id: str,
    family_id: str,
    family_compare: dict[str, Any],
    family_row: dict[str, Any],
    candidate_row: dict[str, Any],
) -> dict[str, Any]:
    same_condition = family_compare.get("same_condition_contract") if isinstance(family_compare.get("same_condition_contract"), dict) else {}
    candidate_method = candidate_row.get("candidate_method") if isinstance(candidate_row.get("candidate_method"), dict) else {}
    return {
        "session_id": session_id,
        "session_scope_id": _text(hypothesis.get("execution", {}).get("session_scope_id")),
        "family_id": family_id,
        "target_method_family": _text(hypothesis.get("target_method_family")),
        "target_candidate_method_id": _text(candidate_method.get("method_id")),
        "regime": _text(same_condition.get("regime"), fallback="unknown"),
        "top_k": int(same_condition.get("top_k") or 0),
        "artifact_detail_level": _text(same_condition.get("artifact_detail_level"), fallback="unknown"),
        "fallback_status": _text(same_condition.get("fallback_status"), fallback="unknown"),
        "family_decision": _text(family_row.get("decision")),
        "best_candidate_decision": _text(family_row.get("best_candidate_decision")),
    }


def build_judge_input(
    *,
    experiment_id: str,
    hypothesis: dict[str, Any],
    session_id: str,
    family_id: str,
    run_manifest: dict[str, Any],
    family_leaderboard: dict[str, Any],
    family_compare: dict[str, Any],
) -> dict[str, Any]:
    family_row = _family_leaderboard_row(family_leaderboard, _text(hypothesis.get("target_method_family")))
    candidate_row = _candidate_result(family_compare, family_row)
    summary_metrics = {
        "family_decision": _text(family_row.get("decision")),
        "authoritative_rollup_decision": _text(family_row.get("authoritative_rollup_decision"), fallback=_text(family_row.get("decision"))),
        "candidate_local_decision": _text(candidate_row.get("candidate_local_decision")),
        "session_aggregate_decision": _text(candidate_row.get("session_aggregate_decision"), fallback=_text(candidate_row.get("candidate_local_decision"))),
        "decision_reasons": _json_ready(candidate_row.get("decision_reasons") or []),
        "promote_ready": bool(candidate_row.get("promote_ready")),
        "insufficient_samples": bool(candidate_row.get("insufficient_samples")),
        "victory_metrics": _json_ready(candidate_row.get("victory_metrics") or {}),
        "changed_top5_members_count": int(candidate_row.get("changed_top5_members_count") or 0),
        "changed_top10_members_count": int(candidate_row.get("changed_top10_members_count") or 0),
        "changed_rank_count": int(candidate_row.get("changed_rank_count") or 0),
        "top5_boundary_score_gap": float(candidate_row.get("top5_boundary_score_gap") or 0.0),
        "top10_boundary_score_gap": float(candidate_row.get("top10_boundary_score_gap") or 0.0),
        "selection_divergence_reason": _text(candidate_row.get("selection_divergence_reason"), fallback="no_meaningful_branching"),
        "topk_branching_block_reason": _text(candidate_row.get("topk_branching_block_reason")),
        "effective_universe_count": int(candidate_row.get("effective_universe_count") or 0),
        "top_k": int(candidate_row.get("top_k") or 0),
        "meaningful_topk_branching_possible": bool(candidate_row.get("meaningful_topk_branching_possible")),
        "run_manifest_hash": _text(run_manifest.get("run_manifest_hash")),
        "family_compare_hash": _text(family_compare.get("compare_hash")),
    }
    available_sample_count = int((family_leaderboard.get("session_meta") or {}).get("sample_count") or (family_leaderboard.get("coverage_waterfall") or {}).get("sample_count") or 0)
    return os_contracts.build_judge_input(
        experiment_id=experiment_id,
        comparison_scope=_comparison_scope(
            hypothesis=hypothesis,
            session_id=session_id,
            family_id=family_id,
            family_compare=family_compare,
            family_row=family_row,
            candidate_row=candidate_row,
        ),
        changed_top5_members_count=int(candidate_row.get("changed_top5_members_count") or 0),
        changed_top10_members_count=int(candidate_row.get("changed_top10_members_count") or 0),
        changed_rank_count=int(candidate_row.get("changed_rank_count") or 0),
        top5_boundary_score_gap=float(candidate_row.get("top5_boundary_score_gap") or 0.0),
        top10_boundary_score_gap=float(candidate_row.get("top10_boundary_score_gap") or 0.0),
        selection_divergence_reason=_text(candidate_row.get("selection_divergence_reason"), fallback="no_meaningful_branching"),
        available_sample_count=available_sample_count,
        available_session_count=1,
        summary_metrics=summary_metrics,
    )


def _decision_audit(
    *,
    family_decision: str,
    family_row: dict[str, Any],
    candidate_row: dict[str, Any],
    judge_input: dict[str, Any],
    decision: dict[str, Any],
) -> dict[str, Any]:
    typed_reason = decision.get("typed_reason") if isinstance(decision.get("typed_reason"), dict) else {}
    blocking_unknowns = [item for item in decision.get("blocking_unknowns") or [] if _text(item)]
    return {
        "schema_version": "tradex_research_os_phase1_audit_v1",
        "provisional_policy": "phase1_provisional",
        "decision_source": "family_compare",
        "family_decision": _text(family_decision),
        "typed_reason_coverage": {
            "has_code": bool(_text(typed_reason.get("code"))),
            "has_source_artifact": bool(_text(typed_reason.get("source_artifact"))),
            "has_source_field": bool(_text(typed_reason.get("source_field"))),
            "has_detail": isinstance(typed_reason.get("detail"), dict),
        },
        "blocking_unknowns_coverage": {
            "count": len(blocking_unknowns),
            "values": blocking_unknowns,
        },
        "threshold_dependency": {
            "available_sample_count": int(judge_input.get("available_sample_count") or 0),
            "confidence": float(decision.get("confidence") or 0.0),
            "confidence_basis": "family_decision_and_available_sample_count",
            "provisional_only": True,
            "candidate_local_decision": _text(candidate_row.get("candidate_local_decision")),
            "authoritative_rollup_decision": _text(family_row.get("authoritative_rollup_decision"), fallback=_text(family_row.get("decision"))),
        },
    }


def _authoritative_context(
    *,
    hypothesis: dict[str, Any],
    session_id: str,
    family_id: str,
    family_compare_path: Path,
    family_compare: dict[str, Any],
    family_row: dict[str, Any],
    candidate_row: dict[str, Any],
) -> dict[str, Any]:
    candidate_method = candidate_row.get("candidate_method") if isinstance(candidate_row.get("candidate_method"), dict) else {}
    return {
        "session_id": session_id,
        "family_id": family_id,
        "method_family": _text(hypothesis.get("target_method_family")),
        "family_compare_state": "present",
        "family_compare_path": str(family_compare_path),
        "family_compare_hash": _text(family_compare.get("compare_hash")),
        "family_decision": _text(family_row.get("decision")),
        "authoritative_rollup_decision": _text(family_row.get("authoritative_rollup_decision"), fallback=_text(family_row.get("decision"))),
        "best_candidate_decision": _text(family_row.get("best_candidate_decision")),
        "candidate_local_decision": _text(candidate_row.get("candidate_local_decision")),
        "promote_ready": bool(candidate_row.get("promote_ready")),
        "insufficient_samples": bool(candidate_row.get("insufficient_samples")),
        "meaningful_topk_branching_possible": bool(candidate_row.get("meaningful_topk_branching_possible")),
        "topk_branching_block_reason": _text(candidate_row.get("topk_branching_block_reason")),
        "decision_reasons": _json_ready(candidate_row.get("decision_reasons") or []),
        "selection_divergence_reason": _text(candidate_row.get("selection_divergence_reason")),
        "candidate_method_id": _text(candidate_method.get("method_id")),
    }


def _derive_decision(
    *,
    experiment_id: str,
    hypothesis: dict[str, Any],
    family_leaderboard: dict[str, Any],
    family_compare: dict[str, Any],
    judge_input: dict[str, Any],
) -> dict[str, Any]:
    family_row = _family_leaderboard_row(family_leaderboard, _text(hypothesis.get("target_method_family")))
    candidate_row = _candidate_result(family_compare, family_row)
    family_decision = _text(family_row.get("decision"), fallback="hold")
    reasons = candidate_row.get("decision_reasons") if isinstance(candidate_row.get("decision_reasons"), list) else []
    primary_reason = reasons[0] if reasons and isinstance(reasons[0], dict) else {}
    if not primary_reason:
        primary_reason = {"code": "family_decision", "status": family_decision}
    confidence = 0.85 if family_decision in {"keep", "drop"} and int(judge_input.get("available_sample_count") or 0) > 0 else 0.45
    if family_decision == "hold":
        confidence = 0.35 if int(judge_input.get("available_sample_count") or 0) <= 0 else 0.55
    blocking_unknowns: list[str] = []
    if family_decision == "hold":
        blocking_unknowns.extend(
            [
                _text(candidate_row.get("topk_branching_block_reason")),
                _text(candidate_row.get("selection_divergence_reason")),
            ]
        )
    blocking_unknowns = [item for item in blocking_unknowns if item]
    next_action = "manual_review" if family_decision == "keep" else "collect_more_evidence" if family_decision == "hold" else "new_hypothesis"
    typed_reason = {
        "code": _text(primary_reason.get("code"), fallback="family_decision"),
        "source_artifact": "family_compare" if reasons else "family_leaderboard",
        "source_field": "decision_reasons[0]" if reasons else "decision",
        "detail": {
            "family_decision": family_decision,
            "family_row": _json_ready(family_row),
            "candidate_row": _json_ready(candidate_row),
            "judge_input": {
                "available_sample_count": judge_input.get("available_sample_count"),
                "selection_divergence_reason": judge_input.get("selection_divergence_reason"),
            },
        },
    }
    decision = os_contracts.build_judge_decision(
        experiment_id=experiment_id,
        decision=family_decision,
        typed_reason=typed_reason,
        confidence=confidence,
        next_action=next_action,
        blocking_unknowns=blocking_unknowns,
        decided_at=os_contracts.now_utc_iso(),
    )
    decision["decision_audit"] = _decision_audit(
        family_decision=family_decision,
        family_row=family_row,
        candidate_row=candidate_row,
        judge_input=judge_input,
        decision=decision,
    )
    return decision


def _build_config_fingerprint(hypothesis: dict[str, Any]) -> str:
    execution = dict(hypothesis.get("execution") or {})
    payload = {
        "hypothesis_id": _text(hypothesis.get("hypothesis_id")),
        "hypothesis_type": _text(hypothesis.get("hypothesis_type")),
        "changed_axis": _text(hypothesis.get("changed_axis")),
        "fixed_contracts": [_text(item) for item in hypothesis.get("fixed_contracts") or [] if _text(item)],
        "expected_effect": _text(hypothesis.get("expected_effect")),
        "metrics_to_watch": [_text(item) for item in hypothesis.get("metrics_to_watch") or [] if _text(item)],
        "acceptance_gate": hypothesis.get("acceptance_gate") or {},
        "rejection_gate": hypothesis.get("rejection_gate") or {},
        "target_method_family": _text(hypothesis.get("target_method_family")),
        "execution": {
            "runner": _text(execution.get("runner")),
            "session_id": _text(execution.get("session_id")),
            "random_seed": int(execution.get("random_seed") or 0),
            "session_scope_id": _text(execution.get("session_scope_id")),
            "universe_size": int(execution.get("universe_size") or 0),
            "max_candidates_per_family": int(execution.get("max_candidates_per_family") or 0),
            "ret20_source_mode": _text(execution.get("ret20_source_mode")),
        },
    }
    if isinstance(hypothesis.get("strategy_target"), dict):
        payload["strategy_target"] = _json_ready(hypothesis.get("strategy_target") or {})
    if isinstance(hypothesis.get("strategy_judgement"), dict):
        payload["strategy_judgement"] = _json_ready(hypothesis.get("strategy_judgement") or {})
    return _stable_hash(payload)


def _build_scope_fingerprint(
    *,
    hypothesis: dict[str, Any],
    session_state: dict[str, Any],
    run_manifest: dict[str, Any],
    family_leaderboard: dict[str, Any],
    family_compare: dict[str, Any],
) -> str:
    target_method_family = _text(hypothesis.get("target_method_family"))
    family_row = _family_leaderboard_row(family_leaderboard, target_method_family)
    same_condition = family_compare.get("same_condition_contract") if isinstance(family_compare.get("same_condition_contract"), dict) else {}
    payload = {
        "session_id": _text(hypothesis.get("execution", {}).get("session_id")),
        "session_scope_id": _text(hypothesis.get("execution", {}).get("session_scope_id")),
        "target_method_family": target_method_family,
        "family_id": _text(family_row.get("family_id"), fallback=_session_family_id(_text(hypothesis.get("execution", {}).get("session_id")), target_method_family)),
        "run_manifest_hash": _text(run_manifest.get("run_manifest_hash")),
        "family_compare_hash": _text(family_compare.get("compare_hash")),
        "same_condition_contract": {
            "schema_version": _text(same_condition.get("schema_version")),
            "universe": list(same_condition.get("universe") or []),
            "period": list(same_condition.get("period") or []),
            "top_k": int(same_condition.get("top_k") or 0),
            "regime": _text(same_condition.get("regime")),
            "artifact_detail_level": _text(same_condition.get("artifact_detail_level")),
            "fallback_status": _text(same_condition.get("fallback_status")),
            "feature_family": _text(same_condition.get("feature_family")),
        },
        "sample_count": int((family_leaderboard.get("session_meta") or {}).get("sample_count") or 0),
        "session_id_state": _text(session_state.get("session_id")),
    }
    return _stable_hash(payload)


def _experiment_id(*, hypothesis_id: str, config_fingerprint: str, scope_fingerprint: str, repo_commit: str, runner_version: str, seed: int, started_at: str) -> str:
    payload = {
        "hypothesis_id": _text(hypothesis_id),
        "config_fingerprint": _text(config_fingerprint),
        "scope_fingerprint": _text(scope_fingerprint),
        "repo_commit": _text(repo_commit),
        "runner_version": _text(runner_version),
        "seed": int(seed),
        "started_at": _text(started_at),
    }
    return f"exp_{_stable_hash(payload)[:20]}"


def _generated_artifacts(
    *,
    session_id: str,
    family_id: str,
    experiment_id: str,
    hypothesis_id: str,
    include_strategy_foundation: bool = False,
) -> list[dict[str, Any]]:
    artifacts = [
        {"name": "session_state", "path": str(_session_state_file(session_id)), "source": "tradex"},
        {"name": "run_manifest", "path": str(run_manifest_file(session_id)), "source": "tradex"},
        {"name": "family_leaderboard", "path": str(_session_family_leaderboard_file(session_id)), "source": "tradex"},
        {"name": "family_compare", "path": str(family_compare_file(family_id)), "source": "tradex"},
        {"name": "hypothesis", "path": str(os_store.hypothesis_file(hypothesis_id)), "source": "research_os"},
        {"name": "preflight_report", "path": str(os_store.preflight_report_file(experiment_id)), "source": "research_os"},
        {"name": "experiment_manifest", "path": str(os_store.experiment_manifest_file(experiment_id)), "source": "research_os"},
        {"name": "judge_input", "path": str(os_store.judge_input_file(experiment_id)), "source": "research_os"},
        {"name": "judge_decision", "path": str(os_store.judge_decision_file(experiment_id)), "source": "research_os"},
        {"name": "authoritative_decision", "path": str(os_store.authoritative_decision_file(experiment_id)), "source": "research_os"},
        {"name": "research_memory", "path": str(os_store.memory_file(hypothesis_id)), "source": "research_os"},
    ]
    if include_strategy_foundation:
        artifacts.extend(
            [
                {"name": "observation_snapshot", "path": str(os_store.observation_snapshot_file(experiment_id)), "source": "research_os"},
                {"name": "strategy_judgement", "path": str(os_store.strategy_judgement_file(experiment_id)), "source": "research_os"},
                {"name": "teacher_evaluation_row", "path": str(os_store.teacher_evaluation_row_file(experiment_id)), "source": "research_os"},
            ]
        )
    return artifacts


def _preflight_report_kwargs(preflight_result: dict[str, Any]) -> dict[str, Any]:
    readiness_checks = preflight_result.get("readiness_checks") if isinstance(preflight_result.get("readiness_checks"), list) else []
    readiness_summary = preflight_result.get("readiness_summary") if isinstance(preflight_result.get("readiness_summary"), dict) else {}
    return {
        "cause_class": _text(preflight_result.get("cause_class")),
        "cause_source": _text(preflight_result.get("cause_source")),
        "remediation_hint": _text(preflight_result.get("remediation_hint")),
        "readiness_checks": [dict(item) for item in readiness_checks if isinstance(item, dict)],
        "readiness_summary": dict(readiness_summary),
    }


def update_research_memory(
    *,
    hypothesis_id: str,
    decision: dict[str, Any],
    experiment_id: str,
    family_id: str,
    judge_input: dict[str, Any],
    provisional_decision: dict[str, Any] | None = None,
    memory_path: Path | None = None,
) -> dict[str, Any]:
    memory_path = memory_path or os_store.memory_file(hypothesis_id)
    current = os_store.read_json(memory_path)
    history = [item for item in current.get("decision_history") or [] if isinstance(item, dict)]
    if isinstance(provisional_decision, dict) and provisional_decision:
        history.append(
            {
                "experiment_id": experiment_id,
                "family_id": family_id,
                "decision_stage": "provisional",
                "decision_source": "judge_decision",
                "decision": _text(provisional_decision.get("decision")),
                "decision_policy_version": _text(provisional_decision.get("decision_policy_version")),
                "typed_reason": _json_ready(provisional_decision.get("typed_reason") or {}),
                "decision_audit": _json_ready(provisional_decision.get("decision_audit") or {}),
                "confidence": float(provisional_decision.get("confidence") or 0.0),
                "next_action": _text(provisional_decision.get("next_action")),
                "blocking_unknowns": [_text(item) for item in provisional_decision.get("blocking_unknowns") or [] if _text(item)],
                "decided_at": _text(provisional_decision.get("decided_at")),
                "available_sample_count": int(judge_input.get("available_sample_count") or 0),
                "selection_divergence_reason": _text(judge_input.get("selection_divergence_reason")),
            }
        )
    history.append(
        {
            "experiment_id": experiment_id,
            "family_id": family_id,
            "decision_stage": "authoritative",
            "decision_source": "authoritative_decision",
            "decision": _text(decision.get("decision")),
            "decision_policy_version": _text(decision.get("decision_policy_version")),
            "typed_reason": _json_ready(decision.get("typed_reason") or {}),
            "decision_audit": _json_ready(decision.get("decision_audit") or {}),
            "confidence": float(decision.get("confidence") or 0.0),
            "next_action": _text(decision.get("next_action")),
            "blocking_unknowns": [_text(item) for item in decision.get("blocking_unknowns") or [] if _text(item)],
            "decided_at": _text(decision.get("decided_at")),
            "available_sample_count": int(judge_input.get("available_sample_count") or 0),
            "selection_divergence_reason": _text(judge_input.get("selection_divergence_reason")),
        }
    )
    retry_blockers = [_text(item) for item in decision.get("blocking_unknowns") or [] if _text(item)]
    notes = f"next_action={_text(decision.get('next_action'))}"
    if retry_blockers:
        notes += f"; blockers={', '.join(retry_blockers)}"
    payload = os_contracts.build_research_memory(
        hypothesis_id=hypothesis_id,
        latest_decision=_text(decision.get("decision")),
        decision_history=history,
        retry_blockers=retry_blockers,
        related_hypotheses=[_text(item) for item in current.get("related_hypotheses") or [] if _text(item)],
        notes_for_next_iteration=notes,
    )
    os_store.write_json(memory_path, payload)
    return payload


def run_hypothesis(hypothesis_path: Path | str) -> dict[str, Any]:
    hypothesis = load_hypothesis(Path(hypothesis_path))
    execution = dict(hypothesis.get("execution") or {})
    session_id = _text(execution.get("session_id"))
    target_method_family = _text(hypothesis.get("target_method_family"))
    if not session_id or not target_method_family:
        raise ValueError("hypothesis execution/session_id and target_method_family are required")
    if _text(hypothesis.get("status")) != "ready":
        raise ValueError("hypothesis.status must be ready")

    started_at = os_contracts.now_utc_iso()
    repo_commit = _repo_commit()
    runner_version = os_contracts.TRADEX_RESEARCH_OS_RUNNER_VERSION
    config_fingerprint = _build_config_fingerprint(hypothesis)

    preflight_result = preflight_service.evaluate_preflight(
        hypothesis=hypothesis,
        repo_commit=repo_commit,
        runner_version=runner_version,
        started_at=started_at,
    )
    preflight_report = os_contracts.build_preflight_report(
        experiment_id=_text(preflight_result.get("experiment_id")),
        hypothesis_id=_text(hypothesis.get("hypothesis_id")),
        runner=_text(preflight_result.get("runner"), fallback="tradex_research_session"),
        status=_text(preflight_result.get("status"), fallback=preflight_service.TRADEX_RESEARCH_PREFLIGHT_REPORT_STATUS_FAILED),
        passed=bool(preflight_result.get("passed")),
        failure_code=_text(preflight_result.get("failure_code")),
        failure_detail=preflight_result.get("failure_detail") if isinstance(preflight_result.get("failure_detail"), dict) else {},
        checked_inputs=preflight_result.get("checked_inputs") if isinstance(preflight_result.get("checked_inputs"), dict) else {},
        normalization_applied=[_text(item) for item in preflight_result.get("normalization_applied") or [] if _text(item)],
        checked_at=_text(preflight_result.get("checked_at"), fallback=os_contracts.now_utc_iso()),
        **_preflight_report_kwargs(preflight_result),
    )
    if not bool(preflight_result.get("passed")):
        preflight_report_path = os_store.preflight_report_file(_text(preflight_report.get("experiment_id")))
        os_store.write_json(preflight_report_path, preflight_report)
        return {
            "status": preflight_service.TRADEX_RESEARCH_PREFLIGHT_REPORT_STATUS_FAILED,
            "experiment_id": _text(preflight_report.get("experiment_id")),
            "hypothesis_id": _text(hypothesis.get("hypothesis_id")),
            "session_id": session_id,
            "preflight_report_path": str(preflight_report_path),
            "preflight_report": _json_ready(preflight_report),
            "failure_code": _text(preflight_report.get("failure_code")),
            "failure_detail": _json_ready(preflight_report.get("failure_detail") or {}),
            "passed": False,
        }

    tradex_runner.run_tradex_research_session(
        session_id=session_id,
        random_seed=int(execution.get("random_seed") or 0),
        universe_size=int(execution.get("universe_size") or 0),
        max_candidates_per_family=int(execution.get("max_candidates_per_family") or 0),
        session_scope_id=_text(execution.get("session_scope_id")) or None,
        ret20_source_mode=_text(execution.get("ret20_source_mode")),
    )

    session_state_path = _session_state_file(session_id)
    session_state = read_json(session_state_path)
    if not session_state:
        raise RuntimeError(f"session state missing after run: {session_id}")

    run_manifest_path = run_manifest_file(session_id)
    run_manifest = read_json(run_manifest_path)
    tradex_contracts.validate_run_manifest(run_manifest)

    family_leaderboard_path = _session_family_leaderboard_file(session_id)
    family_leaderboard = read_json(family_leaderboard_path)
    tradex_contracts.validate_family_leaderboard_artifact(family_leaderboard)

    family_id, family_compare_path, family_compare, family_result, family_row = _resolve_family_compare_artifact(
        session_id=session_id,
        target_method_family=target_method_family,
        session_state=session_state,
        family_leaderboard=family_leaderboard,
    )
    candidate_row = _candidate_result(family_compare, family_row)
    authoritative_context = _authoritative_context(
        hypothesis=hypothesis,
        session_id=session_id,
        family_id=family_id,
        family_compare_path=family_compare_path,
        family_compare=family_compare,
        family_row=family_row,
        candidate_row=candidate_row,
    )

    scope_fingerprint = _build_scope_fingerprint(
        hypothesis=hypothesis,
        session_state=session_state,
        run_manifest=run_manifest,
        family_leaderboard=family_leaderboard,
        family_compare=family_compare,
    )
    experiment_id = _experiment_id(
        hypothesis_id=_text(hypothesis.get("hypothesis_id")),
        config_fingerprint=config_fingerprint,
        scope_fingerprint=scope_fingerprint,
        repo_commit=repo_commit,
        runner_version=runner_version,
        seed=int(execution.get("random_seed") or 0),
        started_at=started_at,
    )
    strategy_foundation_artifacts = trader_foundation.build_strategy_foundation_artifacts(
        experiment_id=experiment_id,
        hypothesis=hypothesis,
    )

    judge_input = build_judge_input(
        experiment_id=experiment_id,
        hypothesis=hypothesis,
        session_id=session_id,
        family_id=family_id,
        run_manifest=run_manifest,
        family_leaderboard=family_leaderboard,
        family_compare=family_compare,
    )
    judge_decision = _derive_decision(
        experiment_id=experiment_id,
        hypothesis=hypothesis,
        family_leaderboard=family_leaderboard,
        family_compare=family_compare,
        judge_input=judge_input,
    )
    authoritative_decision = decision_policy.evaluate_authoritative_decision(
        experiment_id=experiment_id,
        hypothesis_id=_text(hypothesis.get("hypothesis_id")),
        method_family=target_method_family,
        judge_input=judge_input,
        authoritative_context=authoritative_context,
        provisional_decision=judge_decision,
        decided_at=os_contracts.now_utc_iso(),
    )
    finished_at = os_contracts.now_utc_iso()
    experiment_manifest = os_contracts.build_experiment_manifest(
        experiment_id=experiment_id,
        hypothesis_id=_text(hypothesis.get("hypothesis_id")),
        repo_commit=repo_commit,
        runner_version=runner_version,
        config_fingerprint=config_fingerprint,
        scope_fingerprint=scope_fingerprint,
        seed=int(execution.get("random_seed") or 0),
        started_at=started_at,
        finished_at=finished_at,
        generated_artifacts=_generated_artifacts(
            session_id=session_id,
            family_id=family_id,
            experiment_id=experiment_id,
            hypothesis_id=_text(hypothesis.get("hypothesis_id")),
            include_strategy_foundation=isinstance(strategy_foundation_artifacts, dict),
        ),
    )
    passed_preflight_report = os_contracts.build_preflight_report(
        experiment_id=experiment_id,
        hypothesis_id=_text(hypothesis.get("hypothesis_id")),
        runner=_text(preflight_result.get("runner"), fallback="tradex_research_session"),
        status=preflight_service.TRADEX_RESEARCH_PREFLIGHT_REPORT_STATUS_PASSED,
        passed=True,
        failure_code="",
        failure_detail={},
        checked_inputs=preflight_result.get("checked_inputs") if isinstance(preflight_result.get("checked_inputs"), dict) else {},
        normalization_applied=[_text(item) for item in preflight_result.get("normalization_applied") or [] if _text(item)],
        checked_at=_text(preflight_result.get("checked_at"), fallback=os_contracts.now_utc_iso()),
        **_preflight_report_kwargs(preflight_result),
    )

    os_store.write_json(os_store.preflight_report_file(experiment_id), passed_preflight_report)

    os_store.write_json(os_store.hypothesis_file(_text(hypothesis.get("hypothesis_id"))), os_contracts.build_hypothesis_manifest(hypothesis))
    os_store.write_json(os_store.experiment_manifest_file(experiment_id), experiment_manifest)
    os_store.write_json(os_store.judge_input_file(experiment_id), judge_input)
    os_store.write_json(os_store.judge_decision_file(experiment_id), judge_decision)
    os_store.write_json(os_store.authoritative_decision_file(experiment_id), authoritative_decision)
    if isinstance(strategy_foundation_artifacts, dict):
        os_store.write_json(
            os_store.observation_snapshot_file(experiment_id),
            strategy_foundation_artifacts["observation_snapshot"],
        )
        os_store.write_json(
            os_store.strategy_judgement_file(experiment_id),
            strategy_foundation_artifacts["strategy_judgement"],
        )
        os_store.write_json(
            os_store.teacher_evaluation_row_file(experiment_id),
            strategy_foundation_artifacts["teacher_evaluation_row"],
        )
    research_memory = update_research_memory(
        hypothesis_id=_text(hypothesis.get("hypothesis_id")),
        decision=authoritative_decision,
        experiment_id=experiment_id,
        family_id=family_id,
        judge_input=judge_input,
        provisional_decision=judge_decision,
    )
    result = {
        "status": "ok",
        "experiment_id": experiment_id,
        "hypothesis_id": _text(hypothesis.get("hypothesis_id")),
        "family_id": family_id,
        "session_id": session_id,
        "session_state_path": str(session_state_path),
        "run_manifest_path": str(run_manifest_path),
        "family_leaderboard_path": str(family_leaderboard_path),
        "family_compare_path": str(family_compare_path),
        "experiment_manifest_path": str(os_store.experiment_manifest_file(experiment_id)),
        "judge_input_path": str(os_store.judge_input_file(experiment_id)),
        "judge_decision_path": str(os_store.judge_decision_file(experiment_id)),
        "authoritative_decision_path": str(os_store.authoritative_decision_file(experiment_id)),
        "preflight_report_path": str(os_store.preflight_report_file(experiment_id)),
        "research_memory_path": str(os_store.memory_file(_text(hypothesis.get("hypothesis_id")))),
        "observation_snapshot_path": str(os_store.observation_snapshot_file(experiment_id)) if isinstance(strategy_foundation_artifacts, dict) else "",
        "strategy_judgement_path": str(os_store.strategy_judgement_file(experiment_id)) if isinstance(strategy_foundation_artifacts, dict) else "",
        "teacher_evaluation_row_path": str(os_store.teacher_evaluation_row_file(experiment_id)) if isinstance(strategy_foundation_artifacts, dict) else "",
        "judge_decision": _json_ready(judge_decision),
        "decision_audit": _json_ready(judge_decision.get("decision_audit") or {}),
        "authoritative_decision": _json_ready(authoritative_decision),
        "preflight_report": _json_ready(passed_preflight_report),
        "judge_input": _json_ready(judge_input),
        "research_memory": _json_ready(research_memory),
        "experiment_manifest": _json_ready(experiment_manifest),
    }
    if isinstance(strategy_foundation_artifacts, dict):
        result["observation_snapshot"] = _json_ready(strategy_foundation_artifacts["observation_snapshot"])
        result["strategy_judgement"] = _json_ready(strategy_foundation_artifacts["strategy_judgement"])
        result["teacher_evaluation_row"] = _json_ready(strategy_foundation_artifacts["teacher_evaluation_row"])
    return result

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the TRADEX Research OS Phase 1 skeleton.")
    sub = parser.add_subparsers(dest="cmd", required=True)

    validate_parser = sub.add_parser("validate-hypothesis", help="Validate a hypothesis JSON file.")
    validate_parser.add_argument("--hypothesis-path", required=True)

    run_parser = sub.add_parser("run-hypothesis", help="Run a hypothesis through the existing TRADEX session path.")
    run_parser.add_argument("--hypothesis-path", required=True)

    sub.add_parser("rebuild-trader-benchmark", help="Rebuild canonical trader benchmark artifacts from experiment outputs.")

    return parser


def main(argv: list[str] | None = None) -> int:
    try:
        args = _build_parser().parse_args(argv)
        if args.cmd == "validate-hypothesis":
            hypothesis = load_hypothesis(Path(args.hypothesis_path))
            print(json.dumps(_json_ready(hypothesis), ensure_ascii=False, indent=2, sort_keys=True))
            return 0
        if args.cmd == "run-hypothesis":
            result = run_hypothesis(Path(args.hypothesis_path))
            print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))
            return 0
        if args.cmd == "rebuild-trader-benchmark":
            result = trader_benchmark.rebuild_trader_benchmark()
            print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))
            return 0
        raise RuntimeError(f"unknown command: {args.cmd}")
    except (os_store.JsonReadError, FileNotFoundError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
