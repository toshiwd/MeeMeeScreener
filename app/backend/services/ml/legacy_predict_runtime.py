from __future__ import annotations

import logging
from typing import Any, Callable

logger = logging.getLogger(__name__)


def enforce_live_guard() -> dict[str, Any]:
    from . import ml_service as _ml

    if _ml.is_legacy_analysis_disabled():
        return {
            "checked": False,
            "passed": True,
            "action": "disabled",
            "reason": "legacy_analysis_disabled",
            "active_model_version": None,
            "rolled_back_to": None,
            "metrics": _ml._summarize_daily_scores([]),
            "checks": [],
        }
    cfg = _ml.load_ml_config()
    with _ml.get_conn() as conn:
        _ml._ensure_ml_schema(conn)
        if not bool(cfg.live_guard_enabled):
            return {
                "checked": False,
                "passed": True,
                "action": "disabled",
                "reason": "live_guard_disabled",
                "active_model_version": None,
                "rolled_back_to": None,
                "metrics": _ml._summarize_daily_scores([]),
                "checks": [],
            }
        return _ml._apply_live_guard(conn, cfg=cfg)


def get_latest_live_guard_status() -> dict[str, Any]:
    from . import ml_service as _ml

    if _ml.is_legacy_analysis_disabled():
        return {
            "has_check": False,
            "disabled_reason": "legacy_analysis_disabled",
            "latest": None,
        }
    with _ml.get_conn() as conn:
        _ml._ensure_ml_schema(conn)
        latest = _ml._load_latest_live_guard_audit(conn)
    return {
        "has_check": bool(latest),
        "latest": latest,
    }


def predict_monthly_for_dt(dt: int | None = None) -> dict[str, Any]:
    from . import ml_service as _ml

    if _ml.is_legacy_analysis_disabled():
        logger.info(
            "Skipping predict_monthly_for_dt because %s",
            _ml.legacy_analysis_disabled_log_value(),
        )
        return _ml._legacy_monthly_prediction_disabled_result(dt)
    with _ml.get_conn() as conn:
        _ml._ensure_ml_schema(conn)
        if conn.execute("SELECT COUNT(*) FROM ml_feature_daily").fetchone()[0] == 0:
            _ml.refresh_ml_feature_table(conn, feature_version=_ml.FEATURE_VERSION)
        target_dt = int(dt) if dt is not None else None
        if target_dt is None:
            row = conn.execute("SELECT MAX(dt) FROM ml_feature_daily").fetchone()
            if not row or row[0] is None:
                raise RuntimeError("ml_feature_daily is empty")
            target_dt = int(row[0])
        return _ml._predict_monthly_for_dt_with_conn(conn, target_dt)


def predict_for_dates_bulk(
    *,
    dates: list[int],
    chunk_size_days: int = 40,
    include_monthly: bool = False,
    progress_cb: Callable[[int, int, int], None] | None = None,
) -> dict[str, Any]:
    from . import ml_service as _ml

    requested_dates = sorted(
        {
            int(value)
            for value in (_ml._normalize_daily_dt_key(item) for item in dates)
            if value is not None
        }
    )
    if not requested_dates:
        return {
            "requested_dates": [],
            "resolved_dates": [],
            "predicted_dates": [],
            "rows_total": 0,
            "model_version": None,
            "n_train": 0,
            "skipped_dates": [],
            "monthly": None,
        }
    if _ml.is_legacy_analysis_disabled():
        logger.info(
            "Skipping predict_for_dates_bulk because %s dates=%s",
            _ml.legacy_analysis_disabled_log_value(),
            len(requested_dates),
        )
        return _ml._legacy_bulk_prediction_disabled_result(requested_dates)
    chunk_size_days = max(1, int(chunk_size_days))
    cfg = _ml.load_ml_config()
    with _ml.get_conn() as conn:
        _ml._ensure_ml_schema(conn)
        feature_rows = int(conn.execute("SELECT COUNT(*) FROM ml_feature_daily").fetchone()[0] or 0)
        refresh_start_dt: int | None = None
        refresh_end_dt: int | None = None
        repair_dates = _ml._feature_input_repair_dates(conn, target_date_keys=requested_dates)
        if repair_dates:
            refresh_start_dt, refresh_end_dt = _ml._feature_refresh_bounds(
                conn,
                start_key=repair_dates[0],
                end_key=repair_dates[-1],
            )
            _ml._rebuild_feature_inputs_from_daily_bars(
                conn,
                start_dt=refresh_start_dt,
                end_dt=refresh_end_dt,
            )
            _ml.refresh_ml_feature_table(
                conn,
                feature_version=_ml.FEATURE_VERSION,
                start_dt=refresh_start_dt,
                end_dt=refresh_end_dt,
            )
            feature_rows = int(conn.execute("SELECT COUNT(*) FROM ml_feature_daily").fetchone()[0] or 0)
        if feature_rows <= 0:
            if requested_dates:
                refresh_start_dt, refresh_end_dt = _ml._feature_refresh_bounds(
                    conn,
                    start_key=requested_dates[0],
                    end_key=requested_dates[-1],
                )
                _ml.refresh_ml_feature_table(
                    conn,
                    feature_version=_ml.FEATURE_VERSION,
                    start_dt=refresh_start_dt,
                    end_dt=refresh_end_dt,
                )
            else:
                _ml.refresh_ml_feature_table(conn, feature_version=_ml.FEATURE_VERSION)
        elif requested_dates:
            placeholders = ", ".join("?" for _ in requested_dates)
            dt_key_sql = _ml._normalized_daily_dt_sql("dt")
            existing_rows = conn.execute(
                f"""
                SELECT DISTINCT {dt_key_sql} AS dt_key
                FROM ml_feature_daily
                WHERE {dt_key_sql} IN ({placeholders})
                """,
                [int(value) for value in requested_dates],
            ).fetchall()
            existing_requested_dates = {
                int(row[0]) for row in existing_rows if row and row[0] is not None
            }
            missing_requested_dates = [
                int(value) for value in requested_dates if int(value) not in existing_requested_dates
            ]
            if missing_requested_dates:
                refresh_start_dt, refresh_end_dt = _ml._feature_refresh_bounds(
                    conn,
                    start_key=missing_requested_dates[0],
                    end_key=missing_requested_dates[-1],
                )
                _ml.refresh_ml_feature_table(
                    conn,
                    feature_version=_ml.FEATURE_VERSION,
                    start_dt=refresh_start_dt,
                    end_dt=refresh_end_dt,
                )

        dt_key_sql = _ml._normalized_daily_dt_sql("dt")
        available_rows = conn.execute(
            f"""
            SELECT DISTINCT dt, {dt_key_sql} AS dt_key
            FROM ml_feature_daily
            WHERE {dt_key_sql} <= ?
            ORDER BY dt_key, dt
            """,
            [int(requested_dates[-1])],
        ).fetchall()
        available_by_key: dict[int, int] = {}
        for row in available_rows:
            if not row or row[0] is None or row[1] is None:
                continue
            available_by_key[int(row[1])] = int(row[0])
        available_date_keys = sorted(available_by_key)
        if not available_date_keys:
            raise RuntimeError("ml_feature_daily is empty")
        available_set = set(available_date_keys)

        resolved_dates_raw: list[int] = []
        skipped_dates: list[int] = []
        for req_dt in requested_dates:
            if req_dt in available_set:
                resolved_dates_raw.append(int(available_by_key[int(req_dt)]))
                continue
            from bisect import bisect_right

            idx = bisect_right(available_date_keys, int(req_dt)) - 1
            if idx >= 0:
                resolved_dates_raw.append(int(available_by_key[int(available_date_keys[idx])]))
            else:
                skipped_dates.append(int(req_dt))
        resolved_dates = sorted(set(resolved_dates_raw))
        if not resolved_dates:
            return {
                "requested_dates": requested_dates,
                "resolved_dates": [],
                "predicted_dates": [],
                "rows_total": 0,
                "model_version": None,
                "n_train": 0,
                "skipped_dates": skipped_dates,
                "monthly": None,
            }

        models, model_version, n_train = _ml._load_models_from_registry(conn)
        monthly_bootstrap: dict[str, Any] | None = None
        if include_monthly and _ml._load_active_monthly_model_row(conn) is None:
            try:
                monthly_bootstrap = _ml._train_monthly_models_with_conn(conn)
            except Exception as exc:
                monthly_bootstrap = {"ok": False, "error": str(exc)}

        total_dates = int(len(resolved_dates))
        processed_dates = 0
        predicted_dates: set[int] = set()
        rows_total = 0
        for start in range(0, total_dates, chunk_size_days):
            chunk_dates = resolved_dates[start : start + chunk_size_days]
            frame = _ml._load_prediction_feature_frame(conn, chunk_dates)
            if frame.empty:
                processed_dates += len(chunk_dates)
                if progress_cb is not None:
                    progress_cb(int(processed_dates), int(total_dates), int(chunk_dates[-1]))
                continue

            pred = _ml._predict_frame(frame, models, cfg)
            rows = _ml._build_ml_pred_rows(pred, model_version=str(model_version), n_train=int(n_train))
            chunk_predicted_dates = sorted({int(value) for value in pred["dt"].tolist()})
            _ml._replace_ml_predictions_for_dates(conn, chunk_predicted_dates, rows)

            predicted_dates.update(chunk_predicted_dates)
            rows_total += int(len(rows))
            processed_dates += len(chunk_dates)
            if progress_cb is not None:
                progress_cb(int(processed_dates), int(total_dates), int(chunk_dates[-1]))

        monthly_result: dict[str, Any] | None = None
        if include_monthly:
            monthly_rows: list[dict[str, Any]] = []
            for dt_value in resolved_dates:
                try:
                    monthly_rows.append(_ml._predict_monthly_for_dt_with_conn(conn, int(dt_value)))
                except Exception as exc:
                    monthly_rows.append(
                        {
                            "dt": int(dt_value),
                            "pred_dt": None,
                            "rows": 0,
                            "model_version": None,
                            "n_train_abs": 0,
                            "n_train_dir": 0,
                            "disabled_reason": str(exc),
                        }
                    )
            monthly_result = {"results": monthly_rows, "bootstrap": monthly_bootstrap}

        return {
            "requested_dates": requested_dates,
            "resolved_dates": resolved_dates,
            "predicted_dates": sorted(predicted_dates),
            "rows_total": int(rows_total),
            "model_version": str(model_version),
            "n_train": int(n_train),
            "skipped_dates": skipped_dates,
            "monthly": monthly_result,
        }


def predict_for_dt(dt: int | None = None) -> dict[str, Any]:
    from . import ml_service as _ml

    if _ml.is_legacy_analysis_disabled():
        logger.info(
            "Skipping predict_for_dt because %s dt=%s",
            _ml.legacy_analysis_disabled_log_value(),
            dt,
        )
        return _ml._legacy_prediction_disabled_result(dt)
    cfg = _ml.load_ml_config()
    with _ml.get_conn() as conn:
        _ml._ensure_ml_schema(conn)

        target_dt = int(dt) if dt is not None else None
        target_dt_key = _ml._normalize_daily_dt_key(target_dt)
        feature_rows = int(conn.execute("SELECT COUNT(*) FROM ml_feature_daily").fetchone()[0] or 0)
        repair_dates = _ml._feature_input_repair_dates(
            conn,
            target_date_keys=[int(target_dt_key)] if target_dt_key is not None else [],
        )
        if repair_dates:
            refresh_start_dt, refresh_end_dt = _ml._feature_refresh_bounds(
                conn,
                start_key=repair_dates[0],
                end_key=repair_dates[-1],
            )
            _ml._rebuild_feature_inputs_from_daily_bars(
                conn,
                start_dt=refresh_start_dt,
                end_dt=refresh_end_dt,
            )
            _ml.refresh_ml_feature_table(
                conn,
                feature_version=_ml.FEATURE_VERSION,
                start_dt=refresh_start_dt,
                end_dt=refresh_end_dt,
            )
            feature_rows = int(conn.execute("SELECT COUNT(*) FROM ml_feature_daily").fetchone()[0] or 0)
        needs_feature_refresh = feature_rows == 0
        dt_key_sql = _ml._normalized_daily_dt_sql("dt")
        if not needs_feature_refresh and target_dt_key is not None:
            has_target = conn.execute(
                f"SELECT 1 FROM ml_feature_daily WHERE {dt_key_sql} = ? LIMIT 1",
                [int(target_dt_key)],
            ).fetchone()
            needs_feature_refresh = has_target is None
        if needs_feature_refresh:
            if target_dt_key is not None and feature_rows > 0:
                refresh_start_dt, refresh_end_dt = _ml._feature_refresh_bounds(
                    conn,
                    start_key=int(target_dt_key),
                    end_key=int(target_dt_key),
                )
                _ml.refresh_ml_feature_table(
                    conn,
                    feature_version=_ml.FEATURE_VERSION,
                    start_dt=refresh_start_dt,
                    end_dt=refresh_end_dt,
                )
            else:
                _ml.refresh_ml_feature_table(conn, feature_version=_ml.FEATURE_VERSION)

        if target_dt is None:
            row = conn.execute("SELECT MAX(dt) FROM ml_feature_daily").fetchone()
            if not row or row[0] is None:
                raise RuntimeError("ml_feature_daily is empty")
            target_dt = int(row[0])
        else:
            has_target = conn.execute(
                f"SELECT MAX(dt) FROM ml_feature_daily WHERE {dt_key_sql} = ?",
                [int(target_dt_key)] if target_dt_key is not None else [int(target_dt)],
            ).fetchone()
            if not has_target or has_target[0] is None:
                fallback_row = conn.execute(
                    f"SELECT MAX(dt) FROM ml_feature_daily WHERE {dt_key_sql} <= ?",
                    [int(target_dt_key)] if target_dt_key is not None else [int(target_dt)],
                ).fetchone()
                if not fallback_row or fallback_row[0] is None:
                    raise RuntimeError(f"No features found for dt={target_dt}")
                target_dt = int(fallback_row[0])
            else:
                target_dt = int(has_target[0])

        frame = _ml._load_prediction_feature_frame(conn, [int(target_dt)])
        if frame.empty:
            raise RuntimeError(f"No features found for dt={target_dt}")

        models, model_version, n_train = _ml._load_models_from_registry(conn)
        pred = _ml._predict_frame(frame, models, cfg)
        monthly_bootstrap: dict[str, Any] | None = None
        if _ml._load_active_monthly_model_row(conn) is None:
            try:
                monthly_bootstrap = _ml._train_monthly_models_with_conn(conn)
            except Exception as exc:
                monthly_bootstrap = {"ok": False, "error": str(exc)}
        rows = _ml._build_ml_pred_rows(pred, model_version=str(model_version), n_train=int(n_train))
        _ml._replace_ml_predictions_for_dates(conn, [int(target_dt)], rows)
        try:
            monthly_result = _ml._predict_monthly_for_dt_with_conn(conn, target_dt)
        except Exception as exc:
            monthly_result = {
                "dt": int(target_dt),
                "pred_dt": None,
                "rows": 0,
                "model_version": None,
                "n_train_abs": 0,
                "n_train_dir": 0,
                "disabled_reason": str(exc),
            }
        if monthly_bootstrap is not None:
            monthly_result["bootstrap"] = monthly_bootstrap
        return {
            "dt": int(target_dt),
            "rows": int(len(rows)),
            "model_version": model_version,
            "monthly": monthly_result,
        }


def predict_latest() -> dict[str, Any]:
    return predict_for_dt(dt=None)


def get_ml_status() -> dict[str, Any]:
    from . import ml_service as _ml

    if _ml.is_legacy_analysis_disabled():
        cfg = _ml.load_ml_config()
        return {
            "has_active_model": False,
            "disabled_reason": "legacy_analysis_disabled",
            "config": {
                "neutral_band_pct": cfg.neutral_band_pct,
                "p_up_threshold": cfg.p_up_threshold,
                "top_n": cfg.top_n,
                "cost_bps": cfg.cost_bps,
                "train_days": cfg.train_days,
                "test_days": cfg.test_days,
                "step_days": cfg.step_days,
                "embargo_days": cfg.embargo_days,
                "rank_boost_round": cfg.rank_boost_round,
                "rule_weight": cfg.rule_weight,
                "ev_weight": cfg.ev_weight,
                "prob_weight": cfg.prob_weight,
                "rank_weight": cfg.rank_weight,
                "turn_weight": cfg.turn_weight,
                "min_prob_up": cfg.min_prob_up,
                "min_prob_down": cfg.min_prob_down,
                "min_turn_prob_up": cfg.min_turn_prob_up,
                "min_turn_prob_down": cfg.min_turn_prob_down,
                "replay_recent_days": cfg.replay_recent_days,
                "min_daily_samples": cfg.min_daily_samples,
                "min_mean_ret20_net": cfg.min_mean_ret20_net,
                "min_win_rate": cfg.min_win_rate,
                "max_cvar05_loss": cfg.max_cvar05_loss,
                "min_p05_ret20_net": cfg.min_p05_ret20_net,
                "min_lcb95_ret20_net": cfg.min_lcb95_ret20_net,
                "max_p_value_mean_gt0": cfg.max_p_value_mean_gt0,
                "min_robust_lb": cfg.min_robust_lb,
                "fallback_model_version": cfg.fallback_model_version,
                "live_guard_enabled": cfg.live_guard_enabled,
            },
            "active_model": None,
            "latest_training_audit": None,
            "latest_live_guard": None,
        }
    return _ml._get_ml_status_enabled()
