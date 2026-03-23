from __future__ import annotations

import json
import os
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from app.backend.api.dependencies import get_config_repo
from app.backend.api.operator_console_gate import require_operator_console_access
from app.backend.api.routers.system import _raise_mutation_failure, _run_operator_mutation, _set_cached_snapshot
from app.backend.services.analysis_bridge.reader import (
    get_analysis_bridge_snapshot,
    get_internal_replay_progress,
    get_internal_state_eval_action_queue,
)
from app.backend.services.publish_promotion_service import build_publish_promotion_snapshot, promote_logic_key
from app.backend.services.runtime_selection_service import build_runtime_selection_snapshot
from app.backend.services.tradex_experiment_service import (
    adopt_run,
    create_family,
    create_run,
    get_family,
    get_family_compare,
    get_run,
    get_run_compare,
    get_run_detail,
    list_families,
)
from app.backend.services.tradex_experiment_store import find_family_id_by_run_id
from external_analysis.results.publish_candidates import list_publish_candidate_bundles, load_publish_candidate_bundle

router = APIRouter(prefix="/api/tradex", tags=["tradex"])
OPERATOR_CONSOLE_DEPENDENCIES = [Depends(require_operator_console_access)]


class TradexAdoptRequest(BaseModel):
    candidate_id: str | None = Field(default=None, min_length=1)
    baseline_publish_id: str | None = Field(default=None, min_length=1)
    comparison_snapshot_id: str | None = Field(default=None, min_length=1)
    family_id: str | None = Field(default=None, min_length=1)
    run_id: str | None = Field(default=None, min_length=1)
    reason: str | None = None
    actor: str | None = None


class TradexPeriodSegment(BaseModel):
    start_date: str = Field(min_length=1)
    end_date: str = Field(min_length=1)
    label: str | None = None


class TradexPlanSpec(BaseModel):
    plan_id: str = Field(min_length=1)
    plan_version: str | None = None
    label: str | None = None
    method_id: str | None = None
    method_title: str | None = None
    method_thesis: str | None = None
    method_family: str | None = None
    minimum_confidence: float | None = None
    minimum_ready_rate: float | None = None
    signal_bias: str | None = None
    top_k: int | None = None
    notes: str | None = None


class TradexCreateFamilyRequest(BaseModel):
    family_id: str | None = Field(default=None, min_length=1)
    family_name: str | None = None
    universe: list[str] = Field(default_factory=list)
    period: dict[str, Any] = Field(default_factory=dict)
    probes: list[dict[str, Any]] = Field(default_factory=list)
    baseline_plan: TradexPlanSpec = Field(default_factory=lambda: TradexPlanSpec(plan_id="baseline"))
    candidate_plans: list[TradexPlanSpec] = Field(default_factory=list)
    confirmed_only: bool = True
    input_dataset_version: str | None = None
    code_revision: str | None = None
    timezone: str | None = None
    price_source: str | None = None
    data_cutoff_at: str | None = None
    random_seed: int | None = None
    notes: str | None = None


class TradexCreateRunRequest(BaseModel):
    run_kind: str = Field(pattern="^(baseline|candidate)$")
    plan_id: str | None = None
    notes: str | None = None


def _text(value: Any, fallback: str = "") -> str:
    if value is None:
        return fallback
    if isinstance(value, str):
        text = value.strip()
        return text or fallback
    text = str(value).strip()
    return text or fallback


def _num(value: Any) -> float | None:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = float(value)
        except Exception:
            return None
        return parsed if parsed == parsed else None
    return None


def _compact_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _comparison_snapshot_id(candidate_id: str, baseline_publish_id: str | None, comparison: dict[str, Any]) -> str:
    metric_deltas = comparison.get("metric_deltas") or {}
    ranking_impact = comparison.get("ranking_impact") or {}
    decision_summary = comparison.get("decision_summary") or {}
    seed = {
        "candidate_id": candidate_id,
        "baseline_publish_id": baseline_publish_id,
        "metric_deltas": {
            "total_score_delta": metric_deltas.get("total_score_delta"),
            "max_drawdown_delta": metric_deltas.get("max_drawdown_delta"),
            "sample_count_delta": metric_deltas.get("sample_count_delta"),
            "win_rate_delta": metric_deltas.get("win_rate_delta"),
            "expected_value_delta": metric_deltas.get("expected_value_delta"),
        },
        "ranking_impact": {
            "current_rank": ranking_impact.get("current_rank"),
            "candidate_rank": ranking_impact.get("candidate_rank"),
            "rank_shift": ranking_impact.get("rank_shift"),
            "score_delta": ranking_impact.get("score_delta"),
            "direction": ranking_impact.get("direction"),
            "note": ranking_impact.get("note"),
        },
        "decision_summary": {
            "headline": decision_summary.get("headline"),
            "detail": decision_summary.get("detail"),
            "suggested_action": decision_summary.get("suggested_action"),
            "confidence": decision_summary.get("confidence"),
        },
    }
    source = _compact_json(seed)
    hash_value = 0
    for char in source:
        hash_value = (31 * hash_value + ord(char)) & 0xFFFFFFFF
    if hash_value >= 0x80000000:
        hash_value -= 0x100000000
    normalized = abs(hash_value)
    return f"tradex_cmp_{normalized:08x}{len(source):08x}"


def _build_metric_deltas(bundle: dict[str, Any]) -> dict[str, float | None]:
    summary = bundle.get("validation_summary") if isinstance(bundle.get("validation_summary"), dict) else {}
    metrics = summary.get("metrics") if isinstance(summary, dict) and isinstance(summary.get("metrics"), dict) else {}
    return {
        "total_score_delta": _num(metrics.get("total_score_delta") or metrics.get("score_delta") or metrics.get("expectancy_delta")),
        "max_drawdown_delta": _num(metrics.get("max_drawdown_delta") or metrics.get("adverse_move_delta") or metrics.get("max_drawdown_pct_delta")),
        "sample_count_delta": _num(metrics.get("sample_count_delta")),
        "win_rate_delta": _num(metrics.get("win_rate_delta")),
        "expected_value_delta": _num(metrics.get("expected_value_delta") or metrics.get("expectancy_delta")),
    }


def _build_comparison_snapshot(bundle: dict[str, Any], baseline_publish_id: str | None) -> dict[str, Any]:
    summary = bundle.get("validation_summary") if isinstance(bundle.get("validation_summary"), dict) else {}
    metrics = summary.get("metrics") if isinstance(summary, dict) and isinstance(summary.get("metrics"), dict) else {}
    metric_deltas = _build_metric_deltas(bundle)
    readiness_pass = bool(metrics.get("readiness_pass"))
    improved_expectancy = bool(metrics.get("improved_expectancy"))
    sample_count = _num(metrics.get("sample_count"))
    expectancy_delta = _num(metrics.get("expectancy_delta"))
    rank_shift = _num(metrics.get("rank_shift") or metrics.get("ranking_impact"))
    score_delta = _num(metrics.get("total_score_delta") or metrics.get("score_delta"))
    direction = "上昇" if improved_expectancy else "中立" if readiness_pass else "下落"
    decision_summary = {
        "headline": "採用を進める" if readiness_pass else "比較差分を確認",
        "detail": (
            "backend enforcement で正式採用に進めます。"
            if readiness_pass
            else "現行版との差分と検証結果を見てから、保留か再検証を判断してください。"
        ),
        "suggested_action": "採用" if readiness_pass else "再検証",
        "confidence": min(0.95, max(0.25, (sample_count or 0) / 100.0)) if sample_count is not None else None,
    }
    comparison = {
        "baseline_publish_id": baseline_publish_id,
        "metric_deltas": metric_deltas,
        "ranking_impact": {
            "current_rank": None,
            "candidate_rank": None,
            "rank_shift": int(rank_shift) if rank_shift is not None else None,
            "score_delta": score_delta,
            "direction": direction,
            "note": (
                f"期待値差 {expectancy_delta:.4f} / 件数 {int(sample_count)}"
                if expectancy_delta is not None and sample_count is not None
                else "比較差分を確認"
            ),
        },
        "decision_summary": decision_summary,
    }
    comparison["comparison_snapshot_id"] = _comparison_snapshot_id(
        _text(bundle.get("candidate_id") or bundle.get("logic_key"), "unknown"),
        baseline_publish_id,
        comparison,
    )
    return comparison


def _build_validation_result(bundle: dict[str, Any]) -> dict[str, Any]:
    summary = bundle.get("validation_summary") if isinstance(bundle.get("validation_summary"), dict) else {}
    metrics = summary.get("metrics") if isinstance(summary, dict) and isinstance(summary.get("metrics"), dict) else {}
    notes = summary.get("notes") if isinstance(summary, dict) else []
    return {
        "status": _text(bundle.get("validation_state") or bundle.get("status"), "未検証"),
        "sample_count": _num(metrics.get("sample_count")),
        "expectancy_delta": _num(metrics.get("expectancy_delta")),
        "win_rate": _num(metrics.get("win_rate")),
        "max_loss": _num(metrics.get("max_drawdown_pct") or metrics.get("adverse_move_mean") or metrics.get("adverse_move")),
        "notes": [ _text(item) for item in notes if _text(item) ][:4] if isinstance(notes, list) else [],
    }


def _build_anomaly_report(bundle: dict[str, Any], validation_result: dict[str, Any], comparison: dict[str, Any]) -> dict[str, Any] | None:
    status = _text(validation_result.get("status")).lower()
    if status in {"healthy", "ready", "ok", "active", "採用"}:
        return None
    probable_causes = []
    if validation_result.get("sample_count") is None:
        probable_causes.append("sample_count_missing")
    if comparison.get("metric_deltas", {}).get("expected_value_delta") is None:
        probable_causes.append("expected_value_delta_missing")
    if comparison.get("decision_summary", {}).get("suggested_action") == "再検証":
        probable_causes.append("comparison_not_ready")
    return {
        "error_type": _text(validation_result.get("status"), "candidate_validation_pending"),
        "target": _text(bundle.get("logic_key") or bundle.get("candidate_id"), "unknown"),
        "probable_causes": probable_causes,
        "impact_scope": "候補詳細 / 候補比較 / 反映判定",
        "suggested_fix": "validation_summary.metrics を整え、差分 DTO を再計算してください。",
        "ai_prompt": (
            f"TRADEX の候補検証で異常が発生。対象は {_text(bundle.get('logic_key') or bundle.get('candidate_id'), 'unknown')}。"
            f"症状は {_text(validation_result.get('status'), 'unknown')}。"
            "期待する正常動作は、比較差分と検証結果が揃った候補だけが採用候補として表示されることです。"
            f"原因候補は {', '.join(probable_causes) if probable_causes else 'validation_summary 不整合'}。"
            "影響範囲は候補詳細、候補比較、反映判定です。"
            "再現条件は同じ候補を開いたときに差分値または件数が欠落していることです。"
            "最小修正ではなく、構造的に直してください。"
        ),
    }


def _resolve_baseline_publish_id(analysis_status: dict[str, Any], publish_state: dict[str, Any]) -> str | None:
    publish = analysis_status.get("publish") if isinstance(analysis_status.get("publish"), dict) else None
    manifest = analysis_status.get("manifest") if isinstance(analysis_status.get("manifest"), dict) else None
    publish_id = _text((publish or manifest or {}).get("publish_id"))
    if publish_id:
        return publish_id
    return _text(publish_state.get("last_sync_time"))


def _build_baseline(analysis_status: dict[str, Any], runtime_selection: dict[str, Any], publish_state: dict[str, Any]) -> dict[str, Any]:
    publish = analysis_status.get("publish") if isinstance(analysis_status.get("publish"), dict) else None
    manifest = analysis_status.get("manifest") if isinstance(analysis_status.get("manifest"), dict) else None
    source = publish or manifest or {}
    logic_id = _text(runtime_selection.get("selected_logic_id") or publish_state.get("champion_logic_key") or publish_state.get("default_logic_pointer"))
    version = _text(runtime_selection.get("selected_logic_version") or publish_state.get("external_registry_version"))
    published_at = _text(source.get("published_at") or publish_state.get("last_sync_time"))
    publish_id = _text(source.get("publish_id"))
    return {
        "logic_id": logic_id or None,
        "version": version or None,
        "published_at": published_at or None,
        "publish_id": publish_id or None,
    }


def _build_summary(
    analysis_status: dict[str, Any],
    action_queue: dict[str, Any],
    replay_progress: dict[str, Any],
    publish_state: dict[str, Any],
    candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    publish = analysis_status.get("publish") if isinstance(analysis_status.get("publish"), dict) else None
    manifest = analysis_status.get("manifest") if isinstance(analysis_status.get("manifest"), dict) else None
    current = replay_progress.get("current_run") if isinstance(replay_progress.get("current_run"), dict) else None
    replay_phase = _text(current.get("current_phase")) if current else ""
    return {
        "as_of_date": _text((publish or manifest or {}).get("as_of_date")) or None,
        "freshness_state": _text((publish or manifest or {}).get("freshness_state") or publish_state.get("registry_sync_state")) or None,
        "replay_status": f"{_text(current.get('status'), '待機中')}{f' / {replay_phase}' if replay_phase else ''}" if current else "待機中",
        "replay_phase": replay_phase or None,
        "attention_count": len(action_queue.get("actions") or []) if isinstance(action_queue.get("actions"), list) else 0,
        "candidate_count": len(candidates),
        "champion_logic_key": _text(publish_state.get("champion_logic_key") or publish_state.get("default_logic_pointer")) or None,
        "publish_id": _text((publish or manifest or {}).get("publish_id")) or None,
    }


def _candidate_match(bundle: dict[str, Any], candidate_id: str) -> bool:
    resolved = _text(candidate_id)
    if not resolved:
        return False
    return resolved in {_text(bundle.get("candidate_id")), _text(bundle.get("logic_key"))}


def _build_candidate_payload(bundle: dict[str, Any], baseline_publish_id: str | None) -> dict[str, Any]:
    summary = bundle.get("validation_summary") if isinstance(bundle.get("validation_summary"), dict) else {}
    metrics = summary.get("metrics") if isinstance(summary, dict) and isinstance(summary.get("metrics"), dict) else {}
    validation_result = _build_validation_result(bundle)
    comparison_snapshot = _build_comparison_snapshot(bundle, baseline_publish_id)
    anomaly_report = _build_anomaly_report(bundle, validation_result, comparison_snapshot)
    return {
        "candidate_id": _text(bundle.get("candidate_id") or bundle.get("logic_key"), "unknown"),
        "logic_key": _text(bundle.get("logic_key") or bundle.get("candidate_id"), "unknown"),
        "name": _text(bundle.get("logic_family") or bundle.get("logic_key"), "候補"),
        "kind": _text(bundle.get("logic_family"), "候補"),
        "status": _text(bundle.get("status"), "unknown"),
        "validation_state": _text(bundle.get("validation_state"), "unknown"),
        "created_at": bundle.get("created_at"),
        "updated_at": bundle.get("updated_at"),
        "logic_id": bundle.get("logic_id"),
        "logic_version": bundle.get("logic_version"),
        "logic_family": bundle.get("logic_family"),
        "source_publish_id": bundle.get("source_publish_id"),
        "readiness_pass": bool(metrics.get("readiness_pass")),
        "sample_count": _num(metrics.get("sample_count")),
        "expectancy_delta": _num(metrics.get("expectancy_delta")),
        "has_snapshot": bool(bundle.get("published_ranking_snapshot")),
        "validation_summary": bundle.get("validation_summary"),
        "published_logic_manifest": bundle.get("published_logic_manifest"),
        "published_logic_artifact": bundle.get("published_logic_artifact"),
        "published_ranking_snapshot": bundle.get("published_ranking_snapshot"),
        "comparison_snapshot": comparison_snapshot,
        "comparison_snapshot_id": comparison_snapshot["comparison_snapshot_id"],
        "validation_result": validation_result,
        "anomaly_report": anomaly_report,
    }


@router.get("/families", dependencies=OPERATOR_CONSOLE_DEPENDENCIES)
def list_tradex_families():
    return {"ok": True, "items": list_families()}


@router.post("/families", dependencies=OPERATOR_CONSOLE_DEPENDENCIES)
def post_tradex_family(payload: TradexCreateFamilyRequest):
    body = payload.model_dump()
    body["baseline_plan"] = payload.baseline_plan.model_dump()
    body["candidate_plans"] = [item.model_dump() for item in payload.candidate_plans]
    family = create_family(body)
    return {"ok": True, "family": family}


@router.get("/families/{family_id}", dependencies=OPERATOR_CONSOLE_DEPENDENCIES)
def get_tradex_family(family_id: str):
    family = get_family(family_id)
    if not family:
        raise HTTPException(status_code=404, detail={"ok": False, "reason": "family_not_found", "family_id": family_id})
    return {"ok": True, "family": family}


@router.post("/families/{family_id}/runs", dependencies=OPERATOR_CONSOLE_DEPENDENCIES)
def post_tradex_family_run(family_id: str, payload: TradexCreateRunRequest):
    try:
        run = create_run(family_id=family_id, run_kind=payload.run_kind, plan_id=payload.plan_id, notes=payload.notes)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail={"ok": False, "reason": str(exc), "family_id": family_id}) from exc
    return {"ok": True, "run": run}


@router.get("/families/{family_id}/compare", dependencies=OPERATOR_CONSOLE_DEPENDENCIES)
def get_tradex_family_compare(family_id: str):
    compare = get_family_compare(family_id)
    if not compare:
        raise HTTPException(status_code=409, detail={"ok": False, "reason": "compare_not_ready", "family_id": family_id})
    return {"ok": True, "compare": compare}


@router.get("/runs/{run_id}", dependencies=OPERATOR_CONSOLE_DEPENDENCIES)
def get_tradex_run(run_id: str):
    family_id = find_family_id_by_run_id(run_id)
    if not family_id:
        raise HTTPException(status_code=404, detail={"ok": False, "reason": "run_not_found", "run_id": run_id})
    run = get_run(family_id, run_id)
    if not run:
        raise HTTPException(status_code=404, detail={"ok": False, "reason": "run_not_found", "run_id": run_id})
    return {"ok": True, "run": run}


@router.get("/runs/{run_id}/compare", dependencies=OPERATOR_CONSOLE_DEPENDENCIES)
def get_tradex_run_compare(run_id: str):
    family_id = find_family_id_by_run_id(run_id)
    if not family_id:
        raise HTTPException(status_code=404, detail={"ok": False, "reason": "run_not_found", "run_id": run_id})
    compare = get_run_compare(family_id, run_id)
    if not compare:
        raise HTTPException(status_code=404, detail={"ok": False, "reason": "compare_not_ready", "run_id": run_id})
    return {"ok": True, "compare": compare}


@router.get("/runs/{run_id}/detail", dependencies=OPERATOR_CONSOLE_DEPENDENCIES)
def get_tradex_run_detail(run_id: str, code: str):
    family_id = find_family_id_by_run_id(run_id)
    if not family_id:
        raise HTTPException(status_code=404, detail={"ok": False, "reason": "run_not_found", "run_id": run_id})
    detail = get_run_detail(family_id, run_id, code)
    if not detail:
        raise HTTPException(status_code=404, detail={"ok": False, "reason": "detail_not_found", "run_id": run_id, "code": code})
    return {"ok": True, "detail": detail}


@router.get("/bootstrap", dependencies=OPERATOR_CONSOLE_DEPENDENCIES)
def get_tradex_bootstrap(
    request: Request,
    config=Depends(get_config_repo),
):
    db_path = os.getenv("MEEMEE_RESULT_DB_PATH")
    ops_db_path = os.getenv("MEEMEE_OPS_DB_PATH")
    analysis_status = get_analysis_bridge_snapshot()
    runtime_selection = build_runtime_selection_snapshot(config_repo=config, db_path=db_path)
    publish_state = build_publish_promotion_snapshot(config_repo=config, db_path=db_path, ops_db_path=ops_db_path)
    replay_progress = get_internal_replay_progress()
    action_queue = get_internal_state_eval_action_queue()
    raw_candidates = list_publish_candidate_bundles(db_path=db_path)
    baseline = _build_baseline(analysis_status, runtime_selection, publish_state)
    baseline_publish_id = _resolve_baseline_publish_id(analysis_status, publish_state)
    candidates = [_build_candidate_payload(bundle, baseline_publish_id) for bundle in raw_candidates]
    summary = _build_summary(analysis_status, action_queue, replay_progress, publish_state, candidates)
    return {
        "ok": True,
        "baseline": baseline,
        "summary": summary,
        "candidates": candidates,
        "raw": {
            "analysis_status": analysis_status,
            "runtime_selection": runtime_selection,
            "publish_state": publish_state,
            "publish_queue": {},
            "replay_progress": replay_progress,
            "action_queue": action_queue,
        },
    }


@router.post("/adopt", dependencies=OPERATOR_CONSOLE_DEPENDENCIES)
def adopt_tradex_candidate(
    payload: TradexAdoptRequest,
    request: Request,
    config=Depends(get_config_repo),
):
    if payload.family_id and payload.run_id:
        try:
            result = adopt_run(family_id=payload.family_id, run_id=payload.run_id, reason=payload.reason, actor=payload.actor)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail={"ok": False, "reason": str(exc), "family_id": payload.family_id, "run_id": payload.run_id}) from exc
        return result

    db_path = os.getenv("MEEMEE_RESULT_DB_PATH")
    ops_db_path = os.getenv("MEEMEE_OPS_DB_PATH")
    if not payload.candidate_id or not payload.baseline_publish_id or not payload.comparison_snapshot_id:
        raise HTTPException(
            status_code=400,
            detail={
                "ok": False,
                "reason": "legacy_adopt_payload_required",
            },
        )
    candidate = load_publish_candidate_bundle(db_path=db_path, candidate_id=payload.candidate_id)
    if not candidate:
        candidate = load_publish_candidate_bundle(db_path=db_path, logic_key=payload.candidate_id)
    if not candidate:
        raise HTTPException(status_code=404, detail={"ok": False, "reason": "tradex_candidate_not_found", "candidate_id": payload.candidate_id})

    analysis_status = get_analysis_bridge_snapshot()
    runtime_selection = build_runtime_selection_snapshot(config_repo=config, db_path=db_path)
    publish_state = build_publish_promotion_snapshot(config_repo=config, db_path=db_path, ops_db_path=ops_db_path)
    baseline_publish_id = _resolve_baseline_publish_id(analysis_status, publish_state)
    comparison = _build_comparison_snapshot(candidate, baseline_publish_id)
    expected_snapshot_id = comparison["comparison_snapshot_id"]

    if not payload.baseline_publish_id or payload.baseline_publish_id != baseline_publish_id:
        raise HTTPException(
            status_code=409,
            detail={
                "ok": False,
                "reason": "baseline_publish_id_mismatch",
                "candidate_id": payload.candidate_id,
                "expected_baseline_publish_id": baseline_publish_id,
                "received_baseline_publish_id": payload.baseline_publish_id,
                "comparison_snapshot_id": payload.comparison_snapshot_id,
            },
        )
    if not payload.comparison_snapshot_id or payload.comparison_snapshot_id != expected_snapshot_id:
        raise HTTPException(
            status_code=409,
            detail={
                "ok": False,
                "reason": "comparison_snapshot_mismatch",
                "candidate_id": payload.candidate_id,
                "baseline_publish_id": baseline_publish_id,
                "expected_comparison_snapshot_id": expected_snapshot_id,
                "received_comparison_snapshot_id": payload.comparison_snapshot_id,
            },
        )

    logic_key = _text(candidate.get("logic_key") or candidate.get("candidate_id"))
    if not logic_key:
        raise HTTPException(
            status_code=400,
            detail={
                "ok": False,
                "reason": "logic_key_required",
                "candidate_id": payload.candidate_id,
            },
        )

    result = _run_operator_mutation(
        "tradex_adopt",
        lambda: promote_logic_key(
            config_repo=config,
            logic_key=logic_key,
            source="api.tradex.adopt",
            reason=payload.reason,
            actor=payload.actor,
            db_path=db_path,
            ops_db_path=ops_db_path,
        ),
    )
    if not result.get("ok"):
        _raise_mutation_failure(action="tradex_adopt", logic_key=logic_key, result=result)
    _set_cached_snapshot(request, "publish_promotion_snapshot", result.get("snapshot"))
    return {
        "ok": True,
        "candidate_id": payload.candidate_id,
        "logic_key": logic_key,
        "baseline_publish_id": baseline_publish_id,
        "comparison_snapshot_id": expected_snapshot_id,
        "result": result,
    }
