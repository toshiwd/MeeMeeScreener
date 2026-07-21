from __future__ import annotations

import csv
import io
import json
import math
import os
from pathlib import Path
from typing import Any

import duckdb
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi import Body
from fastapi.responses import Response
from pydantic import BaseModel, Field

from app.backend.api.dependencies import get_config_repo
from app.backend.api.dependencies import get_stock_repo
from app.backend.infra.duckdb.stock_repo import StockRepository
from app.backend.api.operator_console_gate import require_operator_console_access
from app.backend.api.routers.system import _raise_mutation_failure, _run_operator_mutation, _set_cached_snapshot
from app.backend.services.analysis_bridge.reader import get_analysis_bridge_snapshot
from app.backend.services.publish_promotion_service import build_publish_promotion_snapshot, promote_logic_key
from app.backend.services.runtime_selection_service import build_runtime_selection_snapshot
from app.backend.services.tradex_readonly_reflection_service import (
    ReadonlyReflectionError,
    build_readonly_reflection_snapshot,
)
from app.backend.services.tradex_research_bridge_service import (
    get_internal_forecast_surface_projection,
    get_internal_forecast_surface_review,
    get_internal_replay_progress,
    get_internal_state_eval_action_queue,
    get_internal_state_eval_candle_combo_summary,
    get_internal_state_eval_candle_combo_trend_summary,
    get_internal_state_eval_candle_summary,
    get_internal_state_eval_daily_summary,
    get_internal_state_eval_daily_summary_history,
    get_latest_strategy_judgement_summary,
    get_internal_state_eval_promotion_review,
    get_internal_state_eval_tag_rows,
    get_internal_state_eval_tag_summary,
    get_internal_state_eval_trend_summary,
    save_internal_state_eval_promotion_decision,
)
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
from app.backend.services.tradex_portfolio_replay_service import load_replay_run, run_portfolio_replay
from external_analysis.results.publish_candidates import list_publish_candidate_bundles, load_publish_candidate_bundle
from scripts.tradex_intraday_short_preview_v1 import build_intraday_short_preview

router = APIRouter(prefix="/api/tradex", tags=["tradex"])
OPERATOR_CONSOLE_DEPENDENCIES = [Depends(require_operator_console_access)]
SHORT_LIFECYCLE_BOARD_ROOT = Path(
    os.getenv("TRADEX_SHORT_LIFECYCLE_BOARD_ROOT", r"G:\Tradex\current_short_lifecycle_rank_board_v1")
)
BUY_LIFECYCLE_BOARD_ROOT = Path(
    os.getenv("TRADEX_BUY_LIFECYCLE_BOARD_ROOT", r"G:\Tradex\current_buy_lifecycle_board_v1")
)
SHAPE_ENTRY_BOARD_ROOT = Path(
    os.getenv("TRADEX_SHAPE_ENTRY_BOARD_ROOT", r"G:\Tradex\leaf20_vol3_current_selection_v1")
)
ADAPTIVE_RULE_ROUTER_ROOT = Path(
    os.getenv("TRADEX_ADAPTIVE_RULE_ROUTER_ROOT", r"G:\Tradex\adaptive_rule_router_v1")
)
INTEGRATED_ENTRY_BOARD_ROOT = Path(
    os.getenv("TRADEX_INTEGRATED_ENTRY_BOARD_ROOT", r"G:\Tradex\integrated_entry_board_v1")
)
TWO_SIDED_MAIN_RULE_ROOT = Path(
    os.getenv(
        "TRADEX_TWO_SIDED_MAIN_RULE_ROOT",
        r"G:\Tradex\tradex_two_sided_sell_only_exposure_cap_completion_v1",
    )
)


def _adaptive_json_safe(value: Any) -> Any:
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {key: _adaptive_json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_adaptive_json_safe(item) for item in value]
    return value


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
    feature_family: str | None = None
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


class TradexReplayRunRequest(BaseModel):
    run_id: str | None = Field(default=None, min_length=1)
    suite_id: str | None = Field(default=None, min_length=1)
    policy_id: str | None = None
    policy_version: str | None = None
    window_start_date: str | None = None
    window_start_dates: list[str] = Field(default_factory=list)
    window_months: int = 3
    universe: list[str] = Field(default_factory=list)
    market_benchmark_symbol: str | None = None
    capital: dict[str, Any] = Field(default_factory=dict)
    scoring: dict[str, Any] = Field(default_factory=dict)
    policy: dict[str, Any] = Field(default_factory=dict)
    unit_scale: int | None = None
    addon_units: list[int] = Field(default_factory=list)
    execution_convention: str | None = None
    weekly_activity_required: bool = True
    short_cash_reusable: bool = False
    selection_rule_change_log: list[dict[str, Any]] = Field(default_factory=list)


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


def _latest_artifact_json(root: Path, file_name: str) -> Path | None:
    if not root.exists() or not root.is_dir():
        return None
    candidates = [path / file_name for path in root.iterdir() if path.is_dir() and (path / file_name).exists()]
    if not candidates:
        return None
    return max(candidates, key=lambda path: (path.stat().st_mtime, path.parent.name))


def _limited_int(value: int, minimum: int, maximum: int) -> int:
    return max(minimum, min(maximum, int(value)))


def _load_two_sided_main_rule_readout() -> dict[str, Any]:
    compare_path = _latest_artifact_json(TWO_SIDED_MAIN_RULE_ROOT, "compare.json")
    if compare_path is None:
        return {
            "available": False,
            "reason": "two_sided_main_rule_not_found",
            "artifact_root": str(TWO_SIDED_MAIN_RULE_ROOT),
        }
    complete_path = compare_path.parent / "_ARTIFACT_COMPLETE.json"
    if not complete_path.is_file():
        return {
            "available": False,
            "reason": "two_sided_main_rule_incomplete",
            "artifact_path": str(compare_path),
        }
    try:
        payload = json.loads(compare_path.read_text(encoding="utf-8"))
    except Exception:
        return {
            "available": False,
            "reason": "two_sided_main_rule_read_failed",
            "artifact_path": str(compare_path),
        }
    decision = payload.get("decision") if isinstance(payload.get("decision"), dict) else {}
    selected = payload.get("selected_variant") if isinstance(payload.get("selected_variant"), dict) else {}
    metrics = payload.get("metrics") if isinstance(payload.get("metrics"), dict) else {}
    cutoffs = payload.get("data_cutoffs") if isinstance(payload.get("data_cutoffs"), dict) else {}
    is_keep = decision.get("candidate_local_decision") == "keep"
    is_review_only = decision.get("authoritative_rollup_decision") == "review_only"
    if not is_keep or not is_review_only:
        return {
            "available": False,
            "reason": "two_sided_main_rule_not_keep_review_only",
            "artifact_path": str(compare_path),
            "candidate_local_decision": decision.get("candidate_local_decision"),
            "authoritative_rollup_decision": decision.get("authoritative_rollup_decision"),
        }
    buy_rank_path = compare_path.parent / "all_buy_ranks.parquet"
    sell_rank_path = compare_path.parent / "all_sell_ranks.parquet"
    if not buy_rank_path.is_file() or not sell_rank_path.is_file():
        return {
            "available": False,
            "reason": "two_sided_main_rule_rank_artifacts_missing",
            "artifact_path": str(compare_path),
        }
    try:
        with duckdb.connect() as conn:
            buy_rows = conn.execute(
                """
                SELECT signal_ymd, code, rank, rank_source,
                       rank_source = 'meemee_priority' AS actionable
                FROM read_parquet(?)
                WHERE signal_ymd = (SELECT max(signal_ymd) FROM read_parquet(?)) AND top10
                ORDER BY rank, code
                """,
                [str(buy_rank_path), str(buy_rank_path)],
            ).fetchall()
            sell_rows = conn.execute(
                """
                SELECT signal_ymd, code, rank, family_hit AS actionable,
                       setup_hit, breadth_hit, readiness_hit
                FROM read_parquet(?)
                WHERE signal_ymd = (SELECT max(signal_ymd) FROM read_parquet(?)) AND top10
                ORDER BY rank, code
                """,
                [str(sell_rank_path), str(sell_rank_path)],
            ).fetchall()
    except Exception:
        return {
            "available": False,
            "reason": "two_sided_main_rule_ranks_read_failed",
            "artifact_path": str(compare_path),
        }
    buy_ranks = [
        {"signal_ymd": row[0], "code": row[1], "rank": row[2], "rank_source": row[3], "actionable": row[4]}
        for row in buy_rows
    ]
    sell_ranks = [
        {
            "signal_ymd": row[0], "code": row[1], "rank": row[2], "actionable": row[3],
            "setup_hit": row[4], "breadth_hit": row[5], "readiness_hit": row[6],
        }
        for row in sell_rows
    ]
    buy_active = any(row["actionable"] for row in buy_ranks)
    sell_active = any(row["actionable"] for row in sell_ranks)
    current_state = "both" if buy_active and sell_active else "buy_only" if buy_active else "sell_only" if sell_active else "no_entry"
    sell_only_cap = _num(selected.get("sell_only_exposure_cap"))
    return _adaptive_json_safe({
        "available": True,
        "label": "買い・空売り 統合主力候補",
        "status": "研究採用候補（表示のみ）",
        "candidate_local_decision": "keep",
        "authoritative_rollup_decision": "review_only",
        "confirmed_as_of": cutoffs.get("runtime_db_max_pan_date"),
        "allocation": {
            "buy_only_buy": 1.0,
            "both_buy": 0.9,
            "both_sell": 0.1,
            "sell_only_sell": sell_only_cap,
            "sell_only_cash": _num(selected.get("sell_only_cash")),
        },
        "current_state": current_state,
        "buy_ranks": buy_ranks,
        "sell_ranks": sell_ranks,
        "validation_2025": metrics.get("validation") if isinstance(metrics.get("validation"), dict) else {},
        "shadow_2026": metrics.get("shadow") if isinstance(metrics.get("shadow"), dict) else {},
        "artifact_path": str(compare_path),
        "display_only": True,
        "production_ranking_changed": False,
        "runtime_db_write": False,
    })


def _short_lifecycle_candidate(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "code": _text(row.get("code")),
        "name": _text(row.get("name")) or None,
        "signal_ymd": _text(row.get("signal_ymd")) or None,
        "lifecycle_rank": row.get("lifecycle_rank"),
        "lifecycle_state": _text(row.get("lifecycle_state"), "Unknown"),
        "lifecycle_rank_score": _num(row.get("lifecycle_rank_score")),
        "original_rank": row.get("original_rank"),
        "original_score": _num(row.get("original_score")),
        "final_review_status": _text(row.get("final_review_status")) or None,
        "setup_state": _text(row.get("setup_state")) or None,
        "continuation_status": _text(row.get("continuation_status")) or None,
        "expected_downside_pct": _num(row.get("expected_downside_pct")),
        "risk_reward_to_sl8": _num(row.get("risk_reward_to_sl8")),
        "base_target_actionability": _text(row.get("base_target_actionability")) or None,
        "regime_permission_status": _text(row.get("regime_permission_status")) or None,
        "advancers_ratio": _num(row.get("advancers_ratio")),
        "visual_micro_label": _text(row.get("visual_micro_label")) or None,
        "lifecycle_reasons": [str(item) for item in row.get("lifecycle_reasons", []) if item],
        "profit_target_rule": _text(row.get("profit_target_rule"), "pt20"),
        "stop_loss_rule": _text(row.get("stop_loss_rule"), "sl8"),
    }


def _buy_lifecycle_candidate(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "code": _text(row.get("code")),
        "as_of_date": _text(row.get("as_of_date")) or None,
        "lifecycle_rank": row.get("lifecycle_rank"),
        "entry_state": _text(row.get("entry_state"), "Unknown"),
        "held_position_review_state": _text(row.get("held_position_review_state"), "Unknown"),
        "entry_actionability_score": _num(row.get("entry_actionability_score")),
        "upside_probability_20d": _num(row.get("upside_probability_20d")),
        "downside_risk_probability_20d": _num(row.get("downside_risk_probability_20d")),
        "review_bucket": _text(row.get("review_bucket")) or None,
        "avoid_level": _text(row.get("avoid_level")) or None,
        "event_risk_contract_status": _text(row.get("event_risk_contract_status")) or None,
        "lifecycle_reasons": [str(item) for item in row.get("lifecycle_reasons", []) if item],
    }


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
    live_strategy_judgement: dict[str, Any] | None = None,
) -> dict[str, Any]:
    publish = analysis_status.get("publish") if isinstance(analysis_status.get("publish"), dict) else None
    manifest = analysis_status.get("manifest") if isinstance(analysis_status.get("manifest"), dict) else None
    current = replay_progress.get("current_run") if isinstance(replay_progress.get("current_run"), dict) else None
    replay_phase = _text(current.get("current_phase")) if current else ""
    live_judgement = live_strategy_judgement if isinstance(live_strategy_judgement, dict) else {}
    live_target = live_judgement.get("target") if isinstance(live_judgement.get("target"), dict) else {}
    return {
        "as_of_date": _text((publish or manifest or {}).get("as_of_date")) or None,
        "freshness_state": _text((publish or manifest or {}).get("freshness_state") or publish_state.get("registry_sync_state")) or None,
        "replay_status": f"{_text(current.get('status'), '待機中')}{f' / {replay_phase}' if replay_phase else ''}" if current else "待機中",
        "replay_phase": replay_phase or None,
        "attention_count": len(action_queue.get("actions") or []) if isinstance(action_queue.get("actions"), list) else 0,
        "candidate_count": len(candidates),
        "champion_logic_key": _text(publish_state.get("champion_logic_key") or publish_state.get("default_logic_pointer")) or None,
        "publish_id": _text((publish or manifest or {}).get("publish_id")) or None,
        "live_strategy_judgement_status": _text(live_judgement.get("status")) or None,
        "live_strategy_judgement_state": _text(live_judgement.get("human_readable_judgement")) or None,
        "live_strategy_machine_action_state": _text(live_judgement.get("machine_action_state")) or None,
        "live_strategy_buy_score": live_judgement.get("buy_score") if isinstance(live_judgement.get("buy_score"), (int, float)) else None,
        "live_strategy_target_code": _text(live_target.get("code")) or None,
        "live_strategy_target_as_of_date": _text(live_target.get("as_of_date")) or None,
        "live_strategy_is_buy_signal": bool(live_judgement.get("is_buy_signal")),
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


@router.get("/research/state-eval-tags", dependencies=OPERATOR_CONSOLE_DEPENDENCIES)
def get_tradex_research_state_eval_tags(side: str | None = None, strategy_tag: str | None = None, limit: int = 40):
    return get_internal_state_eval_tag_rows(side=side, strategy_tag=strategy_tag, limit=limit)


@router.get("/research/state-eval-tags/summary", dependencies=OPERATOR_CONSOLE_DEPENDENCIES)
def get_tradex_research_state_eval_tags_summary(side: str | None = None, limit: int = 5):
    return get_internal_state_eval_tag_summary(side=side, limit=limit)


@router.get("/research/state-eval-candles/summary", dependencies=OPERATOR_CONSOLE_DEPENDENCIES)
def get_tradex_research_state_eval_candles_summary(side: str | None = None, limit: int = 5):
    return get_internal_state_eval_candle_summary(side=side, limit=limit)


@router.get("/research/state-eval-candle-combos/summary", dependencies=OPERATOR_CONSOLE_DEPENDENCIES)
def get_tradex_research_state_eval_candle_combos_summary(side: str | None = None, limit: int = 5):
    return get_internal_state_eval_candle_combo_summary(side=side, limit=limit)


@router.get("/research/state-eval-daily-summary", dependencies=OPERATOR_CONSOLE_DEPENDENCIES)
def get_tradex_research_state_eval_daily_summary(side: str | None = None):
    return get_internal_state_eval_daily_summary(side=side)


@router.get("/research/state-eval-daily-summary/history", dependencies=OPERATOR_CONSOLE_DEPENDENCIES)
def get_tradex_research_state_eval_daily_summary_history(side: str | None = None, limit: int = 30):
    return get_internal_state_eval_daily_summary_history(side=side, limit=limit)


@router.get("/research/state-eval-trends", dependencies=OPERATOR_CONSOLE_DEPENDENCIES)
def get_tradex_research_state_eval_trends(side: str | None = None, lookback: int = 14, limit: int = 5):
    return get_internal_state_eval_trend_summary(side=side, lookback=lookback, limit=limit)


@router.get("/research/state-eval-candle-combo-trends", dependencies=OPERATOR_CONSOLE_DEPENDENCIES)
def get_tradex_research_state_eval_candle_combo_trends(side: str | None = None, lookback: int = 14, limit: int = 5):
    return get_internal_state_eval_candle_combo_trend_summary(side=side, lookback=lookback, limit=limit)


@router.get("/research/state-eval-action-queue", dependencies=OPERATOR_CONSOLE_DEPENDENCIES)
def get_tradex_research_state_eval_action_queue(side: str | None = None):
    return get_internal_state_eval_action_queue(side=side)


@router.get("/research/replay-progress", dependencies=OPERATOR_CONSOLE_DEPENDENCIES)
def get_tradex_research_replay_progress(replay_id: str | None = None, recent_limit: int = 5):
    return get_internal_replay_progress(replay_id=replay_id, recent_limit=recent_limit)


@router.get("/research/forecast-surface-review", dependencies=OPERATOR_CONSOLE_DEPENDENCIES)
def get_tradex_research_forecast_surface_review():
    return get_internal_forecast_surface_review()


@router.get("/research/forecast-surface-projection", dependencies=OPERATOR_CONSOLE_DEPENDENCIES)
def get_tradex_research_forecast_surface_projection(limit_per_side: int = 20):
    return get_internal_forecast_surface_projection(limit_per_side=limit_per_side)


@router.get("/research/short-lifecycle-board", dependencies=OPERATOR_CONSOLE_DEPENDENCIES)
def get_tradex_research_short_lifecycle_board(limit: int = 30):
    artifact_path = _latest_artifact_json(SHORT_LIFECYCLE_BOARD_ROOT, "current_short_lifecycle_rank_board.json")
    if artifact_path is None:
        return {
            "available": False,
            "reason": "short_lifecycle_board_not_found",
            "artifact_root": str(SHORT_LIFECYCLE_BOARD_ROOT),
            "candidates": [],
        }
    try:
        payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail={"ok": False, "reason": "short_lifecycle_board_read_failed", "artifact_path": str(artifact_path)},
        ) from exc
    candidates = payload.get("candidates") if isinstance(payload.get("candidates"), list) else []
    row_limit = _limited_int(limit, 1, 100)
    return {
        "available": True,
        "artifact_path": str(artifact_path),
        "run_id": payload.get("run_id"),
        "created_at": payload.get("created_at"),
        "authoritative_decision": payload.get("authoritative_decision"),
        "counts": payload.get("counts") if isinstance(payload.get("counts"), dict) else {},
        "classification_contract": payload.get("classification_contract")
        if isinstance(payload.get("classification_contract"), dict)
        else {},
        "source_artifact_paths": payload.get("source_artifact_paths")
        if isinstance(payload.get("source_artifact_paths"), dict)
        else {},
        "runtime_db_write": bool(payload.get("runtime_db_write")),
        "meemee_modified": bool(payload.get("meemee_modified")),
        "production_ranking_modified": bool(payload.get("production_ranking_modified")),
        "candidates": [_short_lifecycle_candidate(row) for row in candidates[:row_limit] if isinstance(row, dict)],
    }


@router.get("/research/intraday-short-preview", dependencies=OPERATOR_CONSOLE_DEPENDENCIES)
def get_tradex_research_intraday_short_preview(limit: int = 30):
    """Display-only TRADEX preview from persisted Yahoo intraday bars.

    This never changes the production ranking and never promotes a provisional row
    to a confirmed entry signal.
    """
    try:
        return build_intraday_short_preview(limit=_limited_int(limit, 1, 100))
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail={"ok": False, "reason": "intraday_short_preview_failed", "error": str(exc)},
        ) from exc


@router.get("/research/buy-lifecycle-board", dependencies=OPERATOR_CONSOLE_DEPENDENCIES)
def get_tradex_research_buy_lifecycle_board(limit: int = 100):
    artifact_path = _latest_artifact_json(BUY_LIFECYCLE_BOARD_ROOT, "current_buy_lifecycle_board.json")
    if artifact_path is None:
        return {"available": False, "reason": "buy_lifecycle_board_not_found", "artifact_root": str(BUY_LIFECYCLE_BOARD_ROOT), "candidates": []}
    try:
        payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise HTTPException(status_code=500, detail={"ok": False, "reason": "buy_lifecycle_board_read_failed", "artifact_path": str(artifact_path)}) from exc
    candidates = payload.get("candidates") if isinstance(payload.get("candidates"), list) else []
    return {
        "available": True,
        "artifact_path": str(artifact_path),
        "run_id": payload.get("run_id"),
        "created_at": payload.get("created_at"),
        "authoritative_decision": payload.get("authoritative_decision"),
        "counts": payload.get("counts") if isinstance(payload.get("counts"), dict) else {},
        "classification_contract": payload.get("classification_contract") if isinstance(payload.get("classification_contract"), dict) else {},
        "runtime_db_write": bool(payload.get("runtime_db_write")),
        "meemee_modified": bool(payload.get("meemee_modified")),
        "production_ranking_modified": bool(payload.get("production_ranking_modified")),
        "candidates": [_buy_lifecycle_candidate(row) for row in candidates[: _limited_int(limit, 1, 200)] if isinstance(row, dict)],
    }


@router.get("/research/shape-entry-board", dependencies=OPERATOR_CONSOLE_DEPENDENCIES)
def get_tradex_research_shape_entry_board(limit: int = 30):
    """Read-only operational view of the latest TRADEX leaf-shape board."""
    artifact_path = _latest_artifact_json(SHAPE_ENTRY_BOARD_ROOT, "current_selection_board.json")
    if artifact_path is None:
        return {"available": False, "reason": "shape_entry_board_not_found", "artifact_root": str(SHAPE_ENTRY_BOARD_ROOT), "board": []}
    try:
        payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise HTTPException(status_code=500, detail={"ok": False, "reason": "shape_entry_board_read_failed", "artifact_path": str(artifact_path)}) from exc
    rows = payload.get("candidates") if isinstance(payload.get("candidates"), list) else []
    return {
        "available": True,
        "artifact_path": str(artifact_path),
        "confirmed_signal_date": payload.get("confirmed_signal_date"),
        "default_verdict": payload.get("default_verdict"),
        "candidate_count": payload.get("candidate_count"),
        "selection_contract": payload.get("selection_contract") if isinstance(payload.get("selection_contract"), dict) else {},
        "quality_metrics_2026": payload.get("quality_metrics_2026") if isinstance(payload.get("quality_metrics_2026"), dict) else {},
        "research_status": "2026_oos_positive_candidate",
        "automatic_trading": bool(payload.get("automatic_trading")),
        "production_ranking_changed": bool(payload.get("production_ranking_changed")),
        "runtime_db_write": bool(payload.get("runtime_db_write")),
        "candidates": rows[: _limited_int(limit, 1, 100)],
    }


@router.get("/research/adaptive-rule-board", dependencies=OPERATOR_CONSOLE_DEPENDENCIES)
def get_tradex_research_adaptive_rule_board(limit: int = 30):
    """Display-only view of the latest point-in-time TRADEX rule router."""
    artifact_path = _latest_artifact_json(ADAPTIVE_RULE_ROUTER_ROOT, "compare.json")
    if artifact_path is None:
        return {"available": False, "reason": "adaptive_rule_router_not_found", "artifact_root": str(ADAPTIVE_RULE_ROUTER_ROOT), "current_candidates": []}
    try:
        payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise HTTPException(status_code=500, detail={"ok": False, "reason": "adaptive_rule_router_read_failed", "artifact_path": str(artifact_path)}) from exc
    reports = payload.get("reports") if isinstance(payload.get("reports"), dict) else {}
    oos = reports.get("untouched_2026") if isinstance(reports.get("untouched_2026"), dict) else {}
    return _adaptive_json_safe({
        "available": True,
        "artifact_path": str(artifact_path),
        "current_as_of": payload.get("current_as_of"),
        "current_regime": payload.get("current_regime"),
        "selected_policy": payload.get("selected_policy") if isinstance(payload.get("selected_policy"), dict) else {},
        "quality_2026": oos,
        "adoption_gate": payload.get("adoption_gate") if isinstance(payload.get("adoption_gate"), dict) else {},
        "current_rule_states": payload.get("current_rule_states") if isinstance(payload.get("current_rule_states"), list) else [],
        "current_active_rule_priority": payload.get("current_active_rule_priority") if isinstance(payload.get("current_active_rule_priority"), list) else [],
        "current_candidates": (payload.get("current_candidates") if isinstance(payload.get("current_candidates"), list) else [])[: _limited_int(limit, 1, 100)],
        "automatic_trading": bool(payload.get("automatic_trading")),
        "production_ranking_changed": bool(payload.get("production_ranking_changed")),
        "runtime_db_write": bool(payload.get("runtime_db_write")),
    })


@router.get("/research/integrated-entry-board", dependencies=OPERATOR_CONSOLE_DEPENDENCIES)
def get_tradex_research_integrated_entry_board(limit: int = 30):
    """Latest review-only buy/sell board with official and provisional rows separated."""
    artifact_path = _latest_artifact_json(INTEGRATED_ENTRY_BOARD_ROOT, "integrated_entry_board.json")
    if artifact_path is None:
        return {
            "available": False,
            "reason": "integrated_entry_board_not_found",
            "artifact_root": str(INTEGRATED_ENTRY_BOARD_ROOT),
            "actionable": [],
            "watch": [],
            "main_rule": _load_two_sided_main_rule_readout(),
        }
    try:
        payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail={"ok": False, "reason": "integrated_entry_board_read_failed", "artifact_path": str(artifact_path)},
        ) from exc
    row_limit = _limited_int(limit, 1, 100)
    actionable = payload.get("actionable") if isinstance(payload.get("actionable"), list) else []
    watch = payload.get("watch") if isinstance(payload.get("watch"), list) else []
    return _adaptive_json_safe({
        "available": True,
        "artifact_path": str(artifact_path),
        "confirmed_as_of": payload.get("confirmed_as_of"),
        "current_regime": payload.get("current_regime"),
        "directional_bias": payload.get("directional_bias"),
        "intraday_short_status": payload.get("intraday_short_status"),
        "intraday_short_available": bool(payload.get("intraday_short_available")),
        "decision": payload.get("decision"),
        "ranking_contract": payload.get("ranking_contract"),
        "actionable_count": payload.get("actionable_count"),
        "watch_count": payload.get("watch_count"),
        # Explicit display boundary. Missing legacy fields fail closed to display-only/current-regime.
        "display_only": bool(payload.get("display_only", True)),
        "current_regime_only": bool(payload.get("current_regime_only", True)),
        "actionable": actionable[:row_limit],
        "watch": watch[:row_limit],
        "boundary": payload.get("boundary") if isinstance(payload.get("boundary"), dict) else {},
        "production_ranking_changed": bool(payload.get("production_ranking_changed")),
        "runtime_db_write": bool(payload.get("runtime_db_write")),
        "main_rule": _load_two_sided_main_rule_readout(),
    })


@router.get("/research/state-eval-promotion-review", dependencies=OPERATOR_CONSOLE_DEPENDENCIES)
def get_tradex_research_state_eval_promotion_review():
    return get_internal_state_eval_promotion_review()


@router.post("/research/state-eval-promotion-decision", dependencies=OPERATOR_CONSOLE_DEPENDENCIES)
def post_tradex_research_state_eval_promotion_decision(
    decision: str = Body(...),
    note: str | None = Body(default=None),
    actor: str | None = Body(default="ui_manual"),
):
    return save_internal_state_eval_promotion_decision(decision=decision, note=note, actor=actor)


@router.get("/research/state-eval-tags.csv", dependencies=OPERATOR_CONSOLE_DEPENDENCIES)
def get_tradex_research_state_eval_tags_csv(side: str | None = None, strategy_tag: str | None = None, limit: int = 200):
    payload = get_internal_state_eval_tag_rows(side=side, strategy_tag=strategy_tag, limit=limit)
    rows = list(payload.get("rows") or [])
    fieldnames = [
        "publish_id",
        "as_of_date",
        "side",
        "holding_band",
        "strategy_tag",
        "observation_count",
        "labeled_count",
        "enter_count",
        "wait_count",
        "skip_count",
        "expectancy_mean",
        "adverse_mean",
        "large_loss_rate",
        "win_rate",
        "teacher_alignment_mean",
        "failure_count",
        "readiness_hint",
        "latest_failure_examples",
        "worst_failure_examples",
        "summary_json",
    ]
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow({name: row.get(name) for name in fieldnames})
    publish_id = str(payload.get("publish_id") or "unknown")
    return Response(
        content=buffer.getvalue(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="state_eval_tags_{publish_id}.csv"'},
    )


@router.get("/research/state-eval-daily-summary.csv", dependencies=OPERATOR_CONSOLE_DEPENDENCIES)
def get_tradex_research_state_eval_daily_summary_csv(side: str | None = None, limit: int = 60):
    payload = get_internal_state_eval_daily_summary_history(side=side, limit=limit)
    rows = list(payload.get("rows") or [])
    fieldnames = [
        "publish_id",
        "as_of_date",
        "side_scope",
        "top_strategy_tag",
        "top_strategy_expectancy",
        "top_candle_tag",
        "top_candle_expectancy",
        "risk_watch_tag",
        "risk_watch_loss_rate",
        "sample_watch_tag",
        "sample_watch_labeled_count",
        "promotion_ready",
        "promotion_sample_count",
        "decision_status",
        "codex_command",
        "summary_json",
    ]
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow({name: row.get(name) for name in fieldnames})
    side_scope = str(side or "all")
    return Response(
        content=buffer.getvalue(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="state_eval_daily_summary_{side_scope}.csv"'},
    )


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
    forecast_surface_projection = get_internal_forecast_surface_projection(limit_per_side=8)
    replay_progress = get_internal_replay_progress()
    action_queue = get_internal_state_eval_action_queue()
    live_strategy_judgement = get_latest_strategy_judgement_summary()
    readonly_reflections = build_readonly_reflection_snapshot()
    raw_candidates = list_publish_candidate_bundles(db_path=db_path)
    baseline = _build_baseline(analysis_status, runtime_selection, publish_state)
    baseline_publish_id = _resolve_baseline_publish_id(analysis_status, publish_state)
    candidates = [_build_candidate_payload(bundle, baseline_publish_id) for bundle in raw_candidates]
    summary = _build_summary(
        analysis_status,
        action_queue,
        replay_progress,
        publish_state,
        candidates,
        live_strategy_judgement=live_strategy_judgement,
    )
    return {
        "ok": True,
        "baseline": baseline,
        "summary": summary,
        "candidates": candidates,
        "live_strategy_judgement": live_strategy_judgement,
        "forecast_surface_projection": forecast_surface_projection,
        "readonly_reflections": readonly_reflections,
        "raw": {
            "analysis_status": analysis_status,
            "runtime_selection": runtime_selection,
            "publish_state": publish_state,
            "publish_queue": {},
            "replay_progress": replay_progress,
            "action_queue": action_queue,
            "live_strategy_judgement": live_strategy_judgement,
            "forecast_surface_projection": forecast_surface_projection,
            "readonly_reflections": readonly_reflections,
        },
    }


@router.get("/readonly-reflections", dependencies=OPERATOR_CONSOLE_DEPENDENCIES)
def get_tradex_readonly_reflections():
    try:
        return build_readonly_reflection_snapshot(strict=True)
    except ReadonlyReflectionError as exc:
        raise HTTPException(status_code=503, detail={"ok": False, "reason": str(exc)}) from exc


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


@router.post("/replay/runs")
def create_tradex_replay_run(
    payload: TradexReplayRunRequest,
    repo: StockRepository = Depends(get_stock_repo),
):
    body = payload.model_dump()
    if payload.run_id:
        body["run_id"] = payload.run_id
    if payload.suite_id:
        body["suite_id"] = payload.suite_id
    result = run_portfolio_replay(repo, body)
    return result


@router.get("/replay/runs/{run_id}")
def get_tradex_replay_run(run_id: str):
    return load_replay_run(run_id)
