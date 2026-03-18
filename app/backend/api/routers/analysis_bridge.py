from __future__ import annotations

import csv
import io

from fastapi import APIRouter, Body
from fastapi.responses import Response

from app.backend.services.analysis_bridge.reader import (
    get_analysis_bridge_snapshot,
    get_candidate_daily_rows,
    get_internal_replay_progress,
    get_internal_state_eval_promotion_review,
    save_internal_state_eval_promotion_decision,
    get_internal_state_eval_candle_summary,
    get_internal_state_eval_candle_combo_summary,
    get_internal_state_eval_candle_combo_trend_summary,
    get_internal_state_eval_action_queue,
    get_internal_state_eval_daily_summary,
    get_internal_state_eval_daily_summary_history,
    get_internal_state_eval_trend_summary,
    get_internal_state_eval_tag_rows,
    get_internal_state_eval_tag_summary,
    get_regime_daily_rows,
    get_state_eval_rows,
    get_similar_cases_rows,
    get_similar_case_paths_rows,
)

router = APIRouter(prefix="/api/analysis-bridge", tags=["analysis-bridge"])


@router.get("/status")
def get_analysis_bridge_status():
    return get_analysis_bridge_snapshot()


@router.get("/candidates")
def get_analysis_bridge_candidates(limit_per_side: int = 20):
    return get_candidate_daily_rows(limit_per_side=limit_per_side)


@router.get("/regime")
def get_analysis_bridge_regime():
    return get_regime_daily_rows()


@router.get("/state-eval")
def get_analysis_bridge_state_eval(side: str | None = None, code: str | None = None, limit: int = 40):
    return get_state_eval_rows(side=side, code=code, limit=limit)


@router.get("/internal/state-eval-tags")
def get_analysis_bridge_internal_state_eval_tags(side: str | None = None, strategy_tag: str | None = None, limit: int = 40):
    return get_internal_state_eval_tag_rows(side=side, strategy_tag=strategy_tag, limit=limit)


@router.get("/internal/state-eval-tags/summary")
def get_analysis_bridge_internal_state_eval_tags_summary(side: str | None = None, limit: int = 5):
    return get_internal_state_eval_tag_summary(side=side, limit=limit)


@router.get("/internal/state-eval-candles/summary")
def get_analysis_bridge_internal_state_eval_candles_summary(side: str | None = None, limit: int = 5):
    return get_internal_state_eval_candle_summary(side=side, limit=limit)


@router.get("/internal/state-eval-candle-combos/summary")
def get_analysis_bridge_internal_state_eval_candle_combos_summary(side: str | None = None, limit: int = 5):
    return get_internal_state_eval_candle_combo_summary(side=side, limit=limit)


@router.get("/internal/state-eval-daily-summary")
def get_analysis_bridge_internal_state_eval_daily_summary(side: str | None = None):
    return get_internal_state_eval_daily_summary(side=side)


@router.get("/internal/state-eval-action-queue")
def get_analysis_bridge_internal_state_eval_action_queue(side: str | None = None):
    return get_internal_state_eval_action_queue(side=side)


@router.get("/internal/replay-progress")
def get_analysis_bridge_internal_replay_progress(replay_id: str | None = None, recent_limit: int = 5):
    return get_internal_replay_progress(replay_id=replay_id, recent_limit=recent_limit)


@router.get("/internal/state-eval-daily-summary/history")
def get_analysis_bridge_internal_state_eval_daily_summary_history(side: str | None = None, limit: int = 30):
    return get_internal_state_eval_daily_summary_history(side=side, limit=limit)


@router.get("/internal/state-eval-trends")
def get_analysis_bridge_internal_state_eval_trends(side: str | None = None, lookback: int = 14, limit: int = 5):
    return get_internal_state_eval_trend_summary(side=side, lookback=lookback, limit=limit)


@router.get("/internal/state-eval-candle-combo-trends")
def get_analysis_bridge_internal_state_eval_candle_combo_trends(side: str | None = None, lookback: int = 14, limit: int = 5):
    return get_internal_state_eval_candle_combo_trend_summary(side=side, lookback=lookback, limit=limit)


@router.get("/internal/state-eval-promotion-review")
def get_analysis_bridge_internal_state_eval_promotion_review():
    return get_internal_state_eval_promotion_review()


@router.post("/internal/state-eval-promotion-decision")
def post_analysis_bridge_internal_state_eval_promotion_decision(
    decision: str = Body(...),
    note: str | None = Body(default=None),
    actor: str | None = Body(default="ui_manual"),
):
    return save_internal_state_eval_promotion_decision(decision=decision, note=note, actor=actor)


@router.get("/internal/state-eval-tags.csv")
def get_analysis_bridge_internal_state_eval_tags_csv(side: str | None = None, strategy_tag: str | None = None, limit: int = 200):
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


@router.get("/internal/state-eval-daily-summary.csv")
def get_analysis_bridge_internal_state_eval_daily_summary_csv(side: str | None = None, limit: int = 60):
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


@router.get("/similar-cases")
def get_analysis_bridge_similar_cases(code: str, limit: int = 10):
    return get_similar_cases_rows(code=code, limit=limit)


@router.get("/similar-case-paths")
def get_analysis_bridge_similar_case_paths(code: str, case_id: str):
    return get_similar_case_paths_rows(code=code, case_id=case_id)
