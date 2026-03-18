from __future__ import annotations

import logging
import os
import time
from datetime import datetime

from .jobs import job_manager
from app.backend.core.legacy_analysis_control import (
    is_legacy_analysis_disabled,
    legacy_analysis_disabled_log_value,
)
from .txt_update_job import (
    _TXT_FOLLOWUP_JOB_TYPE,
    _exit_followup_if_canceled,
    _load_update_state,
    _record_followup_failure,
    _record_followup_success,
    _scale_progress,
    _set_followup_stage,
    _to_bool,
    _to_float,
    _to_int,
    _to_optional_int,
)

logger = logging.getLogger(__name__)


def handle_txt_followup(job_id: str, payload: dict) -> None:
    source_txt_job_id = str(payload.get("source_txt_job_id") or "").strip()
    summary_line = str(payload.get("summary_line") or "Export completed")
    phase_dt = _to_optional_int(payload.get("phase_dt"))
    changed_files = _to_int(payload.get("changed_files"), 0, minimum=0)
    pan_finalized_rows = _to_int(payload.get("pan_finalized_rows"), 0, minimum=0)
    auto_ml_predict = _to_bool(payload.get("auto_ml_predict"), True)
    auto_ml_train = _to_bool(payload.get("auto_ml_train"), True)
    force_ml_train = _to_bool(payload.get("force_ml_train"), False)
    force_recompute_on_pan_finalize = _to_bool(payload.get("force_recompute_on_pan_finalize"), True)
    skip_ml_train_if_no_change = _to_bool(
        payload.get("skip_ml_train_if_no_change"),
        _to_bool(os.getenv("MEEMEE_TXT_UPDATE_SKIP_ML_TRAIN_IF_NO_CHANGE"), True),
    )
    auto_fill_missing_history = _to_bool(payload.get("auto_fill_missing_history"), False)
    backfill_lookback_days = _to_int(
        payload.get("backfill_lookback_days"),
        int(os.getenv("MEEMEE_NIGHTLY_BACKFILL_LOOKBACK_DAYS", "130")),
        minimum=20,
    )
    backfill_max_missing_days = _to_int(
        payload.get("backfill_max_missing_days"),
        int(os.getenv("MEEMEE_NIGHTLY_BACKFILL_MAX_MISSING_DAYS", "260")),
        minimum=1,
    )
    auto_walkforward_gate = _to_bool(
        payload.get("auto_walkforward_gate"),
        _to_bool(os.getenv("MEEMEE_TXT_UPDATE_AUTO_WALKFORWARD_GATE"), True),
    )
    walkforward_gate_monthly_only = _to_bool(
        payload.get("walkforward_gate_monthly_only"),
        _to_bool(os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_GATE_MONTHLY_ONLY"), True),
    )
    walkforward_gate_strict = _to_bool(
        payload.get("walkforward_gate_strict"),
        _to_bool(os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_GATE_STRICT"), False),
    )
    walkforward_gate_min_oos_total = _to_float(
        payload.get("walkforward_gate_min_oos_total_realized_unit_pnl"),
        _to_float(
            os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_GATE_MIN_OOS_TOTAL_REALIZED_UNIT_PNL"),
            0.0,
            minimum=-1_000_000_000.0,
        ),
        minimum=-1_000_000_000.0,
    )
    walkforward_gate_min_oos_pf = _to_float(
        payload.get("walkforward_gate_min_oos_mean_profit_factor"),
        _to_float(
            os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_GATE_MIN_OOS_MEAN_PROFIT_FACTOR"),
            1.05,
            minimum=0.0,
        ),
        minimum=0.0,
    )
    walkforward_gate_min_oos_pos_ratio = _to_float(
        payload.get("walkforward_gate_min_oos_positive_window_ratio"),
        _to_float(
            os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_GATE_MIN_OOS_POSITIVE_WINDOW_RATIO"),
            0.40,
            minimum=0.0,
        ),
        minimum=0.0,
    )
    walkforward_gate_min_oos_worst_dd = _to_float(
        payload.get("walkforward_gate_min_oos_worst_max_drawdown_unit"),
        _to_float(
            os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_GATE_MIN_OOS_WORST_MAX_DRAWDOWN_UNIT"),
            -0.12,
            minimum=-1.0,
        ),
        minimum=-1.0,
    )
    auto_walkforward_run = _to_bool(
        payload.get("auto_walkforward_run"),
        _to_bool(os.getenv("MEEMEE_TXT_UPDATE_AUTO_WALKFORWARD_RUN"), True),
    )
    walkforward_run_monthly_only = _to_bool(
        payload.get("walkforward_run_monthly_only"),
        _to_bool(os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_RUN_MONTHLY_ONLY"), True),
    )
    walkforward_run_strict = _to_bool(
        payload.get("walkforward_run_strict"),
        _to_bool(os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_RUN_STRICT"), False),
    )
    walkforward_run_start_dt = _to_optional_int(payload.get("walkforward_run_start_dt"))
    walkforward_run_end_dt = _to_optional_int(payload.get("walkforward_run_end_dt"))
    walkforward_run_max_codes = _to_int(
        payload.get("walkforward_run_max_codes"),
        _to_int(os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_RUN_MAX_CODES"), 500, minimum=50),
        minimum=50,
    )
    walkforward_run_train_months = _to_int(
        payload.get("walkforward_run_train_months"),
        _to_int(os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_RUN_TRAIN_MONTHS"), 24, minimum=1),
        minimum=1,
    )
    walkforward_run_test_months = _to_int(
        payload.get("walkforward_run_test_months"),
        _to_int(os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_RUN_TEST_MONTHS"), 3, minimum=1),
        minimum=1,
    )
    walkforward_run_step_months = _to_int(
        payload.get("walkforward_run_step_months"),
        _to_int(os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_RUN_STEP_MONTHS"), 12, minimum=1),
        minimum=1,
    )
    walkforward_run_min_windows = _to_int(
        payload.get("walkforward_run_min_windows"),
        _to_int(os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_RUN_MIN_WINDOWS"), 1, minimum=1),
        minimum=1,
    )
    walkforward_run_allowed_sides = str(
        payload.get("walkforward_run_allowed_sides")
        or os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_RUN_ALLOWED_SIDES")
        or "long"
    ).strip().lower()
    if walkforward_run_allowed_sides not in {"both", "long", "short"}:
        walkforward_run_allowed_sides = "long"
    walkforward_run_allowed_long_setups = tuple(
        s.strip()
        for s in str(
            payload.get("walkforward_run_allowed_long_setups")
            or os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_RUN_ALLOWED_LONG_SETUPS")
            or "long_breakout_p2"
        ).split(",")
        if s.strip()
    ) or ("long_breakout_p2",)
    walkforward_run_allowed_short_setups = tuple(
        s.strip()
        for s in str(
            payload.get("walkforward_run_allowed_short_setups")
            or os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_RUN_ALLOWED_SHORT_SETUPS")
            or (
                "short_crash_top_p3,short_downtrend_p4,short_failed_high_p1,"
                "short_box_fail_p2,short_ma20_break_p5,short_decision_down,short_entry"
            )
        ).split(",")
        if s.strip()
    )
    walkforward_run_use_regime_filter = _to_bool(
        payload.get("walkforward_run_use_regime_filter"),
        _to_bool(os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_RUN_USE_REGIME_FILTER"), True),
    )
    walkforward_run_min_long_score = _to_float(
        payload.get("walkforward_run_min_long_score"),
        _to_float(os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_RUN_MIN_LONG_SCORE"), 2.0, minimum=-1000.0),
        minimum=-1000.0,
    )
    walkforward_run_min_short_score = _to_float(
        payload.get("walkforward_run_min_short_score"),
        _to_float(os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_RUN_MIN_SHORT_SCORE"), 99.0, minimum=-1000.0),
        minimum=-1000.0,
    )
    walkforward_run_max_new_entries_per_day = _to_int(
        payload.get("walkforward_run_max_new_entries_per_day"),
        _to_int(os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_RUN_MAX_NEW_ENTRIES_PER_DAY"), 1, minimum=1),
        minimum=1,
    )
    walkforward_run_regime_long_min_breadth_above60 = _to_float(
        payload.get("walkforward_run_regime_long_min_breadth_above60"),
        _to_float(
            os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_RUN_REGIME_LONG_MIN_BREADTH_ABOVE60"),
            0.57,
            minimum=0.0,
        ),
        minimum=0.0,
    )
    walkforward_run_range_bias_width_min = _to_float(
        payload.get("walkforward_run_range_bias_width_min"),
        _to_float(os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_RUN_RANGE_BIAS_WIDTH_MIN"), 0.08, minimum=0.0),
        minimum=0.0,
    )
    walkforward_run_range_bias_long_pos_min = _to_float(
        payload.get("walkforward_run_range_bias_long_pos_min"),
        _to_float(os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_RUN_RANGE_BIAS_LONG_POS_MIN"), 0.60, minimum=0.0),
        minimum=0.0,
    )
    walkforward_run_range_bias_short_pos_max = _to_float(
        payload.get("walkforward_run_range_bias_short_pos_max"),
        _to_float(os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_RUN_RANGE_BIAS_SHORT_POS_MAX"), 0.40, minimum=0.0),
        minimum=0.0,
    )
    walkforward_run_ma20_count20_min_long = _to_int(
        payload.get("walkforward_run_ma20_count20_min_long"),
        _to_int(os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_RUN_MA20_COUNT20_MIN_LONG"), 12, minimum=1),
        minimum=1,
    )
    walkforward_run_ma60_count60_min_long = _to_int(
        payload.get("walkforward_run_ma60_count60_min_long"),
        _to_int(os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_RUN_MA60_COUNT60_MIN_LONG"), 30, minimum=1),
        minimum=1,
    )

    state = _load_update_state()
    state["last_followup_job_id"] = str(job_id)
    if source_txt_job_id:
        state["last_followup_source_txt_job_id"] = source_txt_job_id
    state["last_followup_started_at"] = datetime.now().isoformat()
    state["last_followup_status"] = "running"
    _set_followup_stage(state, "init", message="Initializing follow-up update...")
    job_manager._update_db(
        job_id,
        _TXT_FOLLOWUP_JOB_TYPE,
        "running",
        message="Initializing follow-up update...",
        progress=0,
    )

    if phase_dt is None:
        phase_dt = _to_optional_int(state.get("last_phase_dt"))
    if phase_dt is None:
        error_msg = "phase_dt is missing for txt_followup"
        _record_followup_failure(state, stage="init", error=error_msg, message=error_msg)
        job_manager._update_db(
            job_id,
            _TXT_FOLLOWUP_JOB_TYPE,
            "failed",
            error=error_msg,
            message=error_msg,
            finished_at=datetime.now(),
        )
        return

    if _exit_followup_if_canceled(job_id, state, stage="init", message="Canceled before follow-up start"):
        return

    force_recompute_due_to_pan_finalize = bool(force_recompute_on_pan_finalize and pan_finalized_rows > 0)
    legacy_analysis_disabled = is_legacy_analysis_disabled()
    effective_auto_ml_train = False if legacy_analysis_disabled else bool(auto_ml_train or force_recompute_due_to_pan_finalize)
    effective_auto_ml_predict = False if legacy_analysis_disabled else bool(auto_ml_predict or force_recompute_due_to_pan_finalize)
    effective_auto_walkforward_run = bool(auto_walkforward_run or force_recompute_due_to_pan_finalize)
    effective_auto_walkforward_gate = bool(auto_walkforward_gate or force_recompute_due_to_pan_finalize)
    if force_recompute_due_to_pan_finalize:
        state["last_forced_recompute_at"] = datetime.now().isoformat()

    ml_note_parts: list[str] = []
    ML_TRAIN_PROGRESS_START = 10
    ML_TRAIN_PROGRESS_DONE = 40
    ML_PREDICT_PROGRESS = 52
    ML_LIVE_GUARD_PROGRESS = 58
    ANALYSIS_BACKFILL_PROGRESS = 70
    CACHE_REFRESH_PROGRESS = 82
    WALKFORWARD_RUN_PROGRESS = 90
    WALKFORWARD_GATE_PROGRESS = 95
    FINALIZING_PROGRESS = 99

    try:
        from app.backend.services import ml_service

        if force_recompute_due_to_pan_finalize:
            ml_note_parts.append(f"pan_finalize_force_recompute(rows={int(pan_finalized_rows)})")

        if effective_auto_ml_train:
            if _exit_followup_if_canceled(job_id, state, stage="ml_train", message="Canceled before ML training"):
                return
            latest_pred_dt = _to_optional_int(state.get("last_ml_predict_dt"))
            has_prior_ml = bool(state.get("last_ml_train_at") or state.get("last_ml_model_version"))
            skip_train = (
                (not force_ml_train)
                and bool(skip_ml_train_if_no_change)
                and (not force_recompute_due_to_pan_finalize)
                and int(changed_files) == 0
                and has_prior_ml
            )
            if skip_train:
                if latest_pred_dt is not None and int(latest_pred_dt) == int(phase_dt):
                    skip_message = f"Skipping ML training (no data change, dt={int(phase_dt)})"
                else:
                    skip_message = (
                        "Skipping ML training (no data change; "
                        f"prediction refresh only, dt={int(phase_dt)})"
                    )
                _set_followup_stage(state, "ml_train", message=skip_message)
                job_manager._update_db(
                    job_id,
                    _TXT_FOLLOWUP_JOB_TYPE,
                    "running",
                    message=skip_message,
                    progress=ML_TRAIN_PROGRESS_DONE,
                )
                ml_note_parts.append("ml_train=skip(no_change)")
            else:
                _set_followup_stage(state, "ml_train", message="Refreshing ML training...")
                job_manager._update_db(
                    job_id,
                    _TXT_FOLLOWUP_JOB_TYPE,
                    "running",
                    message="Refreshing ML training...",
                    progress=ML_TRAIN_PROGRESS_START,
                )
                ml_report = {"progress": -1, "at": 0.0}

                def _on_ml_train_progress(progress: int, message: str) -> None:
                    progress_clamped = max(0, min(100, int(progress)))
                    now_ts = time.monotonic()
                    prev_progress = int(ml_report["progress"])
                    prev_ts = float(ml_report["at"])
                    if (
                        progress_clamped < 100
                        and prev_progress >= 0
                        and (progress_clamped - prev_progress) < 2
                        and (now_ts - prev_ts) < 1.5
                    ):
                        return
                    ml_report["progress"] = progress_clamped
                    ml_report["at"] = now_ts
                    total_progress = ML_TRAIN_PROGRESS_START + int(
                        round(((ML_TRAIN_PROGRESS_DONE - ML_TRAIN_PROGRESS_START) * progress_clamped) / 100)
                    )
                    total_progress = max(ML_TRAIN_PROGRESS_START, min(ML_TRAIN_PROGRESS_DONE, total_progress))
                    detail = f"Refreshing ML training... {message} ({progress_clamped}%)"
                    _set_followup_stage(state, "ml_train", message=detail)
                    job_manager._update_db(
                        job_id,
                        _TXT_FOLLOWUP_JOB_TYPE,
                        "running",
                        message=detail,
                        progress=total_progress,
                    )

                train_result = ml_service.train_models(dry_run=False, progress_cb=_on_ml_train_progress)
                state["last_ml_train_at"] = datetime.now().isoformat()
                model_version = train_result.get("model_version")
                if model_version:
                    state["last_ml_model_version"] = str(model_version)
                ml_note_parts.append("ml_train=ok")
        else:
            ml_note_parts.append("ml_train=skip(disabled)")

        if effective_auto_ml_predict:
            if _exit_followup_if_canceled(job_id, state, stage="ml_predict", message="Canceled before ML prediction"):
                return
            _set_followup_stage(state, "ml_predict", message="Refreshing ML prediction...")
            job_manager._update_db(
                job_id,
                _TXT_FOLLOWUP_JOB_TYPE,
                "running",
                message="Refreshing ML prediction...",
                progress=ML_PREDICT_PROGRESS,
            )
            pred_result = ml_service.predict_for_dt(dt=phase_dt)
            state["last_ml_predict_at"] = datetime.now().isoformat()
            state["last_ml_predict_dt"] = int(pred_result.get("dt") or phase_dt)
            state["last_ml_predict_rows"] = int(pred_result.get("rows") or 0)
            ml_note_parts.append(f"ml_predict=ok(rows={state['last_ml_predict_rows']})")

            if _exit_followup_if_canceled(job_id, state, stage="ml_live_guard", message="Canceled before ML live guard"):
                return
            _set_followup_stage(state, "ml_live_guard", message="Evaluating live guard...")
            job_manager._update_db(
                job_id,
                _TXT_FOLLOWUP_JOB_TYPE,
                "running",
                message="Evaluating ML live guard...",
                progress=ML_LIVE_GUARD_PROGRESS,
            )
            guard_result = ml_service.enforce_live_guard()
            state["last_ml_live_guard_at"] = datetime.now().isoformat()
            state["last_ml_live_guard_action"] = str(guard_result.get("action") or "unknown")
            state["last_ml_live_guard_reason"] = str(guard_result.get("reason") or "")
            rolled_back_to = guard_result.get("rolled_back_to")
            if rolled_back_to:
                state["last_ml_model_version"] = str(rolled_back_to)
                ml_note_parts.append(f"ml_live_guard=rollback({rolled_back_to})")
            else:
                ml_note_parts.append(f"ml_live_guard={state['last_ml_live_guard_action']}")
        else:
            ml_note_parts.append("ml_predict=skip")
    except Exception as exc:
        logger.exception("Follow-up ML refresh failed: %s", exc)
        state["last_ml_error"] = str(exc)
        ml_note_parts.append(f"ml=failed({exc})")
    else:
        state.pop("last_ml_error", None)

    if auto_fill_missing_history and not legacy_analysis_disabled:
        try:
            if _exit_followup_if_canceled(
                job_id,
                state,
                stage="analysis_backfill",
                message="Canceled before analysis backfill",
            ):
                return
            _set_followup_stage(
                state,
                "analysis_backfill",
                message=(
                    "Backfilling missing analysis history "
                    f"(lookback={backfill_lookback_days}, max_missing={backfill_max_missing_days})..."
                ),
            )
            job_manager._update_db(
                job_id,
                _TXT_FOLLOWUP_JOB_TYPE,
                "running",
                message=(
                    "Backfilling missing analysis history "
                    f"(lookback={backfill_lookback_days}, max_missing={backfill_max_missing_days})..."
                ),
                progress=ANALYSIS_BACKFILL_PROGRESS,
            )
            from app.backend.services.analysis.analysis_backfill_service import backfill_missing_analysis_history

            analysis_backfill_report = {"message": ""}

            def _on_analysis_backfill_progress(progress: int, message: str) -> None:
                detail = f"Backfilling missing analysis history... {message}"
                if str(analysis_backfill_report["message"]) == detail:
                    return
                analysis_backfill_report["message"] = detail
                _set_followup_stage(state, "analysis_backfill", message=detail)
                job_manager._update_db(
                    job_id,
                    _TXT_FOLLOWUP_JOB_TYPE,
                    "running",
                    message=detail,
                    progress=ANALYSIS_BACKFILL_PROGRESS,
                )

            backfill_result = backfill_missing_analysis_history(
                lookback_days=backfill_lookback_days,
                max_missing_days=backfill_max_missing_days,
                include_sell=True,
                include_phase=False,
                progress_cb=_on_analysis_backfill_progress,
            )
            state["last_analysis_backfill_at"] = datetime.now().isoformat()
            state["last_analysis_backfill_result"] = {
                "anchor_dt": backfill_result.get("anchor_dt"),
                "missing_ml_total": backfill_result.get("missing_ml_total"),
                "missing_ml_selected": backfill_result.get("missing_ml_selected"),
                "predicted": len(backfill_result.get("predicted_dates") or []),
                "sell_refreshed": len(backfill_result.get("sell_refreshed_dates") or []),
                "errors": len(backfill_result.get("errors") or []),
            }
            state.pop("last_analysis_backfill_error", None)
            ml_note_parts.append(
                "analysis_backfill="
                f"ok(pred={state['last_analysis_backfill_result']['predicted']},"
                f"sell={state['last_analysis_backfill_result']['sell_refreshed']},"
                f"errors={state['last_analysis_backfill_result']['errors']})"
            )
        except Exception as exc:
            logger.exception("Analysis backfill failed during txt_followup: %s", exc)
            state["last_analysis_backfill_error"] = str(exc)
            ml_note_parts.append(f"analysis_backfill=failed({exc})")
    elif legacy_analysis_disabled:
        ml_note_parts.append("analysis_backfill=skip(disabled)")
    else:
        ml_note_parts.append("analysis_backfill=skip")

    try:
        from app.backend.core.analysis_prewarm_job import schedule_analysis_prewarm_if_needed

        prewarm_job_id = schedule_analysis_prewarm_if_needed(source=f"txt_followup:{job_id}")
        state["last_analysis_prewarm_submit_at"] = datetime.now().isoformat()
        state["last_analysis_prewarm_job_id"] = prewarm_job_id
        if prewarm_job_id:
            ml_note_parts.append(f"analysis_prewarm=queued({prewarm_job_id})")
        else:
            ml_note_parts.append("analysis_prewarm=skip(covered_or_active)")
    except Exception as exc:
        logger.warning("Analysis prewarm submit skipped during txt_followup: %s", exc)
        state["last_analysis_prewarm_error"] = str(exc)
        ml_note_parts.append(f"analysis_prewarm=failed({exc})")

    try:
        if _exit_followup_if_canceled(job_id, state, stage="cache_refresh", message="Canceled before cache refresh"):
            return
        _set_followup_stage(state, "cache_refresh", message="Refreshing rankings cache...")
        job_manager._update_db(
            job_id,
            _TXT_FOLLOWUP_JOB_TYPE,
            "running",
            message="Refreshing rankings cache...",
            progress=CACHE_REFRESH_PROGRESS,
        )
        from app.backend.services import rankings_cache

        rankings_cache.refresh_cache()
        state["last_cache_refresh_at"] = datetime.now().isoformat()
        try:
            from app.backend.core.screener_snapshot_job import schedule_screener_snapshot_refresh

            snapshot_job_id = schedule_screener_snapshot_refresh(source=f"txt_followup:{job_id}")
            state["last_screener_snapshot_submit_at"] = datetime.now().isoformat()
            state["last_screener_snapshot_job_id"] = snapshot_job_id
            if snapshot_job_id:
                ml_note_parts.append(f"screener_snapshot=queued({snapshot_job_id})")
            else:
                ml_note_parts.append("screener_snapshot=skip(active)")
        except Exception as exc:
            logger.warning("Screener snapshot submit skipped during txt_followup: %s", exc)
            state["last_screener_snapshot_error"] = str(exc)
            ml_note_parts.append(f"screener_snapshot=failed({exc})")
    except Exception as exc:
        logger.exception("Rankings cache refresh failed during txt_followup: %s", exc)
        _record_followup_failure(
            state,
            stage="cache_refresh",
            error=str(exc),
            message="Rankings cache refresh failed",
        )
        job_manager._update_db(
            job_id,
            _TXT_FOLLOWUP_JOB_TYPE,
            "failed",
            error="Rankings cache refresh failed",
            message=f"Rankings cache refresh failed: {exc}",
            finished_at=datetime.now(),
        )
        return

    walkforward_run_failed = False
    try:
        if _exit_followup_if_canceled(
            job_id,
            state,
            stage="walkforward_run",
            message="Canceled before walkforward run",
        ):
            return
        run_now = datetime.now()
        run_month_key = run_now.strftime("%Y-%m")
        if not effective_auto_walkforward_run:
            state["last_walkforward_run_skipped_at"] = run_now.isoformat()
            state["last_walkforward_run_skipped_reason"] = "disabled"
            state.pop("last_walkforward_run_error", None)
            state.pop("last_walkforward_run_error_at", None)
            ml_note_parts.append("walkforward_run=skip(disabled)")
        elif (
            (not force_recompute_due_to_pan_finalize)
            and walkforward_run_monthly_only
            and str(state.get("last_walkforward_run_month_key") or "") == run_month_key
        ):
            state["last_walkforward_run_skipped_at"] = run_now.isoformat()
            state["last_walkforward_run_skipped_reason"] = f"already_ran_month:{run_month_key}"
            state.pop("last_walkforward_run_error", None)
            state.pop("last_walkforward_run_error_at", None)
            ml_note_parts.append(f"walkforward_run=skip(month={run_month_key})")
        else:
            _set_followup_stage(state, "walkforward_run", message="Running strategy walkforward...")
            job_manager._update_db(
                job_id,
                _TXT_FOLLOWUP_JOB_TYPE,
                "running",
                message="Running strategy walkforward...",
                progress=WALKFORWARD_RUN_PROGRESS,
            )
            from app.backend.services import strategy_backtest_service

            walkforward_report = {"message": "", "progress": -1}

            def _on_walkforward_run_progress(progress: int, message: str) -> None:
                total_progress = _scale_progress(progress, WALKFORWARD_RUN_PROGRESS - 1, WALKFORWARD_RUN_PROGRESS)
                detail = f"Running strategy walkforward... {message}"
                if (
                    int(walkforward_report["progress"]) == int(total_progress)
                    and str(walkforward_report["message"]) == detail
                ):
                    return
                walkforward_report["progress"] = int(total_progress)
                walkforward_report["message"] = detail
                _set_followup_stage(state, "walkforward_run", message=detail)
                job_manager._update_db(
                    job_id,
                    _TXT_FOLLOWUP_JOB_TYPE,
                    "running",
                    message=detail,
                    progress=int(total_progress),
                )

            walkforward_cfg = strategy_backtest_service.StrategyBacktestConfig(
                min_long_score=float(walkforward_run_min_long_score),
                min_short_score=float(walkforward_run_min_short_score),
                max_new_entries_per_day=int(walkforward_run_max_new_entries_per_day),
                allowed_sides=str(walkforward_run_allowed_sides),
                allowed_long_setups=tuple(walkforward_run_allowed_long_setups),
                allowed_short_setups=tuple(walkforward_run_allowed_short_setups),
                use_regime_filter=bool(walkforward_run_use_regime_filter),
                regime_long_min_breadth_above60=float(walkforward_run_regime_long_min_breadth_above60),
                range_bias_width_min=float(walkforward_run_range_bias_width_min),
                range_bias_long_pos_min=float(walkforward_run_range_bias_long_pos_min),
                range_bias_short_pos_max=float(walkforward_run_range_bias_short_pos_max),
                ma20_count20_min_long=int(walkforward_run_ma20_count20_min_long),
                ma60_count60_min_long=int(walkforward_run_ma60_count60_min_long),
            )
            run_result = strategy_backtest_service.run_strategy_walkforward(
                start_dt=walkforward_run_start_dt,
                end_dt=walkforward_run_end_dt,
                max_codes=int(walkforward_run_max_codes),
                dry_run=False,
                config=walkforward_cfg,
                train_months=int(walkforward_run_train_months),
                test_months=int(walkforward_run_test_months),
                step_months=int(walkforward_run_step_months),
                min_windows=int(walkforward_run_min_windows),
                progress_cb=_on_walkforward_run_progress,
            )
            run_id = str(run_result.get("run_id") or "")
            run_summary = run_result.get("summary") if isinstance(run_result.get("summary"), dict) else {}
            state["last_walkforward_run_at"] = datetime.now().isoformat()
            state["last_walkforward_run_month_key"] = run_month_key
            state["last_walkforward_run_run_id"] = run_id
            state["last_walkforward_run_windowing"] = run_result.get("windowing") or {}
            state["last_walkforward_run_summary"] = run_summary
            state.pop("last_walkforward_run_error", None)
            state.pop("last_walkforward_run_error_at", None)
            state.pop("last_walkforward_run_skipped_at", None)
            state.pop("last_walkforward_run_skipped_reason", None)
            ml_note_parts.append(
                "walkforward_run="
                f"ok(run={run_id or 'unknown'},"
                f"oos_pnl={run_summary.get('oos_total_realized_unit_pnl')},"
                f"oos_pf={run_summary.get('oos_mean_profit_factor')})"
            )
    except Exception as exc:
        logger.exception("Walkforward run failed during txt_followup: %s", exc)
        state["last_walkforward_run_error"] = str(exc)
        state["last_walkforward_run_error_at"] = datetime.now().isoformat()
        walkforward_run_failed = True
        ml_note_parts.append(f"walkforward_run=failed({exc})")
        if walkforward_run_strict:
            _record_followup_failure(
                state,
                stage="walkforward_run",
                error=str(exc),
                message="Walkforward run failed",
            )
            job_manager._update_db(
                job_id,
                _TXT_FOLLOWUP_JOB_TYPE,
                "failed",
                error="Walkforward run failed",
                message=f"Walkforward run failed: {exc}",
                finished_at=datetime.now(),
            )
            return

    try:
        if _exit_followup_if_canceled(
            job_id,
            state,
            stage="walkforward_gate",
            message="Canceled before walkforward gate",
        ):
            return
        gate_now = datetime.now()
        gate_month_key = gate_now.strftime("%Y-%m")
        latest_run_id = str(state.get("last_walkforward_run_run_id") or "")
        last_gate_source_run_id = str(state.get("last_walkforward_gate_source_run_id") or "")
        if walkforward_run_failed:
            state["last_walkforward_gate_skipped_at"] = gate_now.isoformat()
            state["last_walkforward_gate_skipped_reason"] = "walkforward_run_failed"
            state.pop("last_walkforward_gate_error", None)
            state.pop("last_walkforward_gate_error_at", None)
            ml_note_parts.append("walkforward_gate=skip(run_failed)")
        elif not effective_auto_walkforward_gate:
            state["last_walkforward_gate_skipped_at"] = gate_now.isoformat()
            state["last_walkforward_gate_skipped_reason"] = "disabled"
            state.pop("last_walkforward_gate_error", None)
            state.pop("last_walkforward_gate_error_at", None)
            ml_note_parts.append("walkforward_gate=skip(disabled)")
        elif (
            (not force_recompute_due_to_pan_finalize)
            and walkforward_gate_monthly_only
            and str(state.get("last_walkforward_gate_month_key") or "") == gate_month_key
            and ((not latest_run_id) or latest_run_id == last_gate_source_run_id)
        ):
            state["last_walkforward_gate_skipped_at"] = gate_now.isoformat()
            state["last_walkforward_gate_skipped_reason"] = f"already_ran_month:{gate_month_key}"
            state.pop("last_walkforward_gate_error", None)
            state.pop("last_walkforward_gate_error_at", None)
            ml_note_parts.append(f"walkforward_gate=skip(month={gate_month_key})")
        else:
            _set_followup_stage(state, "walkforward_gate", message="Evaluating strategy walkforward gate...")
            job_manager._update_db(
                job_id,
                _TXT_FOLLOWUP_JOB_TYPE,
                "running",
                message="Evaluating strategy walkforward gate...",
                progress=WALKFORWARD_GATE_PROGRESS,
            )
            from app.backend.services import strategy_backtest_service

            gate_result = strategy_backtest_service.run_strategy_walkforward_gate(
                min_oos_total_realized_unit_pnl=walkforward_gate_min_oos_total,
                min_oos_mean_profit_factor=walkforward_gate_min_oos_pf,
                min_oos_positive_window_ratio=walkforward_gate_min_oos_pos_ratio,
                min_oos_worst_max_drawdown_unit=walkforward_gate_min_oos_worst_dd,
                dry_run=False,
                note=f"txt_followup_job:{job_id}:run={latest_run_id or 'unknown'}",
                source_run_id=latest_run_id or None,
                source_finished_at=None,
                source_status="success" if latest_run_id else None,
                source_report={
                    "summary": state.get("last_walkforward_run_summary") or {},
                    "windowing": state.get("last_walkforward_run_windowing") or {},
                }
                if latest_run_id and isinstance(state.get("last_walkforward_run_summary"), dict)
                else None,
            )
            source = gate_result.get("source") if isinstance(gate_result.get("source"), dict) else {}
            source_run_id = str(source.get("run_id") or "")
            state["last_walkforward_gate_at"] = datetime.now().isoformat()
            state["last_walkforward_gate_month_key"] = gate_month_key
            state["last_walkforward_gate_gate_id"] = str(gate_result.get("gate_id") or "")
            state["last_walkforward_gate_source_run_id"] = source_run_id
            state["last_walkforward_gate_source_finished_at"] = source.get("finished_at")
            state["last_walkforward_gate_status"] = str(gate_result.get("status") or "")
            state["last_walkforward_gate_passed"] = bool(gate_result.get("passed"))
            state["last_walkforward_gate_thresholds"] = gate_result.get("thresholds") or {}
            state.pop("last_walkforward_gate_error", None)
            state.pop("last_walkforward_gate_error_at", None)
            state.pop("last_walkforward_gate_skipped_at", None)
            state.pop("last_walkforward_gate_skipped_reason", None)
            passed = bool(gate_result.get("passed"))
            ml_note_parts.append(f"walkforward_gate={'pass' if passed else 'fail'}(run={source_run_id or 'unknown'})")
            if walkforward_gate_strict and not passed:
                error_msg = "Walkforward gate failed"
                _record_followup_failure(
                    state,
                    stage="walkforward_gate",
                    error=error_msg,
                    message=f"{error_msg} (source_run_id={source_run_id or 'unknown'})",
                )
                job_manager._update_db(
                    job_id,
                    _TXT_FOLLOWUP_JOB_TYPE,
                    "failed",
                    error=error_msg,
                    message=f"{error_msg} (source_run_id={source_run_id or 'unknown'})",
                    finished_at=datetime.now(),
                )
                return
    except Exception as exc:
        logger.exception("Walkforward gate evaluation failed during txt_followup: %s", exc)
        state["last_walkforward_gate_error"] = str(exc)
        state["last_walkforward_gate_error_at"] = datetime.now().isoformat()
        ml_note_parts.append(f"walkforward_gate=failed({exc})")
        if walkforward_gate_strict:
            _record_followup_failure(
                state,
                stage="walkforward_gate",
                error=str(exc),
                message="Walkforward gate failed",
            )
            job_manager._update_db(
                job_id,
                _TXT_FOLLOWUP_JOB_TYPE,
                "failed",
                error="Walkforward gate failed",
                message=f"Walkforward gate failed: {exc}",
                finished_at=datetime.now(),
            )
            return

    if legacy_analysis_disabled:
        logger.info(
            "TXT followup skipping walkforward research snapshot (%s)",
            legacy_analysis_disabled_log_value(),
        )
        ml_note_parts.append("walkforward_research_snapshot=skip(legacy_analysis_disabled)")
    else:
        try:
            from app.backend.services import strategy_backtest_service

            research_snapshot = strategy_backtest_service.save_daily_walkforward_research_snapshot()
            if bool(research_snapshot.get("saved")):
                state["last_walkforward_research_snapshot_at"] = datetime.now().isoformat()
                state["last_walkforward_research_source_run_id"] = str(research_snapshot.get("source_run_id") or "")
                state["last_walkforward_research_snapshot_date"] = research_snapshot.get("snapshot_date")
                ml_note_parts.append(
                    f"walkforward_research_snapshot=ok(date={research_snapshot.get('snapshot_date')})"
                )
        except Exception as exc:
            logger.warning("Walkforward research snapshot skipped during txt_followup: %s", exc)
            ml_note_parts.append(f"walkforward_research_snapshot=skip({exc})")

    if _exit_followup_if_canceled(job_id, state, stage="finalize", message="Canceled before finalize"):
        return

    completion_ts = datetime.now()
    _set_followup_stage(state, "finalize", message="Finalizing follow-up status...")
    job_manager._update_db(
        job_id,
        _TXT_FOLLOWUP_JOB_TYPE,
        "running",
        message="Finalizing follow-up status...",
        progress=FINALIZING_PROGRESS,
    )
    ml_note = f" [{' / '.join(ml_note_parts)}]" if ml_note_parts else ""
    final_message = f"{summary_line}. Follow-up completed.{ml_note}"
    _record_followup_success(state, stage="finalize", message=final_message)
    job_manager._update_db(
        job_id,
        _TXT_FOLLOWUP_JOB_TYPE,
        "success",
        message=final_message,
        progress=100,
        finished_at=completion_ts,
    )
