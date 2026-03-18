from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def inspect_analysis_backfill_coverage(
    *,
    svc,
    lookback_days: int = 130,
    anchor_dt: int | None = None,
    start_dt: int | None = None,
    end_dt: int | None = None,
    include_sell: bool = True,
    include_phase: bool = False,
    force_recompute: bool = False,
) -> dict[str, Any]:
    lookback_days = max(1, int(lookback_days))
    if svc.is_legacy_analysis_disabled():
        logger.info(
            "Skipping inspect_analysis_backfill_coverage because %s",
            svc.legacy_analysis_disabled_log_value(),
        )
        return svc._legacy_analysis_coverage_disabled_result(
            lookback_days=lookback_days,
            force_recompute=bool(force_recompute),
        )
    with svc.get_conn() as conn:
        svc.ensure_legacy_analysis_schema(conn)
        feature_count_row = conn.execute("SELECT COUNT(*) FROM ml_feature_daily").fetchone()
        feature_count = int(feature_count_row[0]) if feature_count_row and feature_count_row[0] is not None else 0
        if feature_count <= 0:
            svc.ml_service.refresh_ml_feature_table(conn, feature_version=svc.ml_service.FEATURE_VERSION)

        coverage = svc._resolve_analysis_cache_coverage(
            conn,
            lookback_days=lookback_days,
            anchor_dt=anchor_dt,
            start_dt=start_dt,
            end_dt=end_dt,
            include_sell=include_sell,
            include_phase=include_phase,
            force_recompute=force_recompute,
        )
        if coverage["anchor_dt"] is None:
            return {
                "anchor_dt": None,
                "start_dt": None,
                "end_dt": None,
                "lookback_days": int(lookback_days),
                "target_dates": [],
                "target_count": 0,
                "missing_ml_dates": [],
                "missing_sell_dates": [],
                "missing_phase_dates": [],
                "active_ml_model_version": None,
                "sell_calc_version": svc.SELL_ANALYSIS_CALC_VERSION,
                "covered": True,
                "force_recompute": bool(force_recompute),
            }

    return {
        "anchor_dt": int(coverage["anchor_dt"]),
        "start_dt": coverage["start_dt"],
        "end_dt": coverage["end_dt"],
        "lookback_days": int(lookback_days),
        "target_dates": [int(value) for value in coverage["target_dates"]],
        "target_count": int(len(coverage["target_dates"])),
        "missing_ml_dates": [int(value) for value in coverage["missing_ml_dates"]],
        "missing_sell_dates": [int(value) for value in coverage["missing_sell_dates"]],
        "missing_phase_dates": [int(value) for value in coverage["missing_phase_dates"]],
        "active_ml_model_version": coverage["active_ml_model_version"],
        "sell_calc_version": coverage["sell_calc_version"],
        "covered": not coverage["missing_ml_dates"] and not coverage["missing_sell_dates"] and not coverage["missing_phase_dates"],
        "force_recompute": bool(coverage["force_recompute"]),
    }


def backfill_missing_analysis_history(
    *,
    svc,
    lookback_days: int = 130,
    anchor_dt: int | None = None,
    start_dt: int | None = None,
    end_dt: int | None = None,
    max_missing_days: int | None = None,
    include_sell: bool = True,
    include_phase: bool = False,
    force_recompute: bool = False,
    progress_cb=None,
) -> dict[str, Any]:
    lookback_days = max(1, int(lookback_days))
    max_missing_days = None if max_missing_days is None else max(1, int(max_missing_days))
    if svc.is_legacy_analysis_disabled():
        logger.info(
            "Skipping backfill_missing_analysis_history because %s",
            svc.legacy_analysis_disabled_log_value(),
        )
        return svc._legacy_analysis_backfill_disabled_result(
            lookback_days=lookback_days,
            force_recompute=bool(force_recompute),
        )

    svc._notify(progress_cb, 2, "Checking backfill targets...")
    with svc.get_conn() as conn:
        svc.ensure_legacy_analysis_schema(conn)
        feature_count_row = conn.execute("SELECT COUNT(*) FROM ml_feature_daily").fetchone()
        feature_count = int(feature_count_row[0]) if feature_count_row and feature_count_row[0] is not None else 0
        if feature_count <= 0:
            svc._notify(progress_cb, 4, "Refreshing ml_feature_daily...")
            svc.ml_service.refresh_ml_feature_table(conn, feature_version=svc.ml_service.FEATURE_VERSION)

        coverage = svc._resolve_analysis_cache_coverage(
            conn,
            lookback_days=lookback_days,
            anchor_dt=anchor_dt,
            start_dt=start_dt,
            end_dt=end_dt,
            include_sell=include_sell,
            include_phase=include_phase,
            force_recompute=force_recompute,
        )
        resolved_anchor_dt = coverage["anchor_dt"]
        if resolved_anchor_dt is None:
            return {
                "ok": True,
                "anchor_dt": None,
                "start_dt": None,
                "end_dt": None,
                "lookback_days": lookback_days,
                "target_dates": [],
                "missing_ml_total": 0,
                "missing_ml_selected": 0,
                "predicted_dates": [],
                "predicted_rows_total": 0,
                "sell_refreshed_dates": [],
                "phase_refreshed_range": None,
                "errors": [],
                "message": "No source dates found.",
                "force_recompute": bool(force_recompute),
            }

        target_dates = [int(value) for value in coverage["target_dates"]]
        missing_ml_dates = [int(value) for value in coverage["missing_ml_dates"]]
        missing_ml_total = len(missing_ml_dates)
        if max_missing_days is not None and len(missing_ml_dates) > max_missing_days:
            missing_ml_dates = missing_ml_dates[:max_missing_days]

        missing_sell_dates = [int(value) for value in coverage["missing_sell_dates"]] if include_sell else []
        missing_phase_dates = [int(value) for value in coverage["missing_phase_dates"]] if include_phase else []

    errors: list[str] = []
    predicted_dates: list[int] = []
    predicted_rows_total = 0

    if missing_ml_dates:
        def _on_bulk_progress(done: int, total: int, last_dt: int) -> None:
            progress = 10 + int(60 * done / max(1, total))
            svc._notify(progress_cb, progress, f"ML backfill ({done}/{total}) dt={int(last_dt)}")

        try:
            bulk_result = svc.ml_service.predict_for_dates_bulk(
                dates=[int(value) for value in missing_ml_dates],
                chunk_size_days=40,
                include_monthly=False,
                progress_cb=_on_bulk_progress,
            )
            predicted_dates = [int(value) for value in (bulk_result.get("predicted_dates") or [])]
            predicted_rows_total = int(bulk_result.get("rows_total") or 0)
            for skipped_dt in [int(value) for value in (bulk_result.get("skipped_dates") or [])]:
                errors.append(f"ml_pred_20d dt={skipped_dt}: skipped (no matching feature date)")
        except Exception as bulk_exc:
            logger.exception("ML bulk backfill failed; fallback to per-date mode: %s", bulk_exc)
            errors.append(f"ml_pred_20d bulk: {bulk_exc}")
            for idx, dt in enumerate(missing_ml_dates, start=1):
                progress = 10 + int(60 * idx / max(1, len(missing_ml_dates)))
                svc._notify(progress_cb, progress, f"ML fallback ({idx}/{len(missing_ml_dates)}) dt={dt}")
                try:
                    result = svc.ml_service.predict_for_dt(dt=int(dt))
                    predicted_dates.append(int(dt))
                    predicted_rows_total += int(result.get("rows") or 0)
                except Exception as exc:
                    logger.exception("ML backfill failed dt=%s: %s", dt, exc)
                    errors.append(f"ml_pred_20d dt={dt}: {exc}")

    sell_refreshed_dates: list[int] = []
    if include_sell:
        sell_targets = sorted({int(value) for value in [*missing_sell_dates, *predicted_dates]})
        if sell_targets:
            svc._notify(progress_cb, 80, f"Refreshing sell analysis for {len(sell_targets)} dates...")
            try:
                def _on_sell_progress(progress: int, message: str) -> None:
                    mapped_progress = 80 + int(15 * max(0, min(100, int(progress))) / 100)
                    svc._notify(progress_cb, mapped_progress, message)

                sell_result = svc.accumulate_sell_analysis_for_dates(
                    target_dates=sell_targets,
                    progress_cb=_on_sell_progress,
                )
                sell_refreshed_dates = [int(v) for v in sell_result.get("target_dates", [])]
            except Exception as exc:
                logger.exception("Sell analysis backfill failed: %s", exc)
                errors.append(f"sell_analysis_daily: {exc}")

    phase_refreshed_range: dict[str, int] | None = None
    if include_phase and missing_phase_dates:
        phase_start_dt = int(missing_phase_dates[0])
        phase_end_dt = int(missing_phase_dates[-1])
        svc._notify(progress_cb, 90, f"Refreshing phase prediction range={phase_start_dt}..{phase_end_dt}")
        try:
            svc.run_batch(phase_start_dt, phase_end_dt, dry_run=False)
            phase_refreshed_range = {"start_dt": phase_start_dt, "end_dt": phase_end_dt}
        except Exception as exc:
            logger.exception("Phase backfill failed range=%s..%s: %s", phase_start_dt, phase_end_dt, exc)
            errors.append(f"phase_pred_daily range={phase_start_dt}..{phase_end_dt}: {exc}")

    svc._notify(progress_cb, 100, "Backfill completed.")
    return {
        "ok": len(errors) == 0,
        "anchor_dt": int(resolved_anchor_dt) if resolved_anchor_dt is not None else None,
        "start_dt": coverage.get("start_dt"),
        "end_dt": coverage.get("end_dt"),
        "lookback_days": int(lookback_days),
        "target_dates": [int(value) for value in target_dates],
        "missing_ml_total": int(missing_ml_total),
        "missing_ml_selected": int(len(missing_ml_dates)),
        "predicted_dates": predicted_dates,
        "predicted_rows_total": int(predicted_rows_total),
        "sell_refreshed_dates": sell_refreshed_dates,
        "phase_refreshed_range": phase_refreshed_range,
        "force_recompute": bool(force_recompute),
        "errors": errors,
        "message": (
            f"ml={len(predicted_dates)}/{len(missing_ml_dates)} "
            f"sell={len(sell_refreshed_dates)} "
            f"phase={'yes' if phase_refreshed_range else 'no'}"
        ),
    }
