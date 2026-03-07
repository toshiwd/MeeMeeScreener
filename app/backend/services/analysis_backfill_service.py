from __future__ import annotations

from datetime import datetime, timezone
import logging
from typing import Any, Callable

from app.db.session import get_conn
from app.backend.services import ml_service
from app.backend.services.sell_analysis_accumulator import (
    SELL_ANALYSIS_CALC_VERSION,
    accumulate_sell_analysis_for_dates,
)

try:
    from app.backend.jobs.phase_batch import run_batch
except ModuleNotFoundError:  # pragma: no cover - legacy tooling may import from app/backend on sys.path
    from jobs.phase_batch import run_batch  # type: ignore


logger = logging.getLogger(__name__)
ProgressCallback = Callable[[int, str], None]
_SUPPORTED_DATE_TABLES = {"ml_pred_20d", "sell_analysis_daily", "phase_pred_daily"}


def _normalized_date_sql(column_name: str) -> str:
    return (
        f"CASE "
        f"WHEN {column_name} >= 1000000000 THEN CAST(strftime(to_timestamp({column_name}), '%Y%m%d') AS BIGINT) "
        f"ELSE {column_name} "
        f"END"
    )


def _resolve_anchor_dt(conn, anchor_dt: int | None) -> int | None:
    if anchor_dt is not None:
        normalized_anchor_dt = _normalize_dt_key(int(anchor_dt))
        if normalized_anchor_dt is None:
            return int(anchor_dt)
        row = conn.execute(
            f"SELECT MAX(dt) FROM ml_feature_daily WHERE {_normalized_date_sql('dt')} <= ?",
            [int(normalized_anchor_dt)],
        ).fetchone()
        if row and row[0] is not None:
            return int(row[0])
        row = conn.execute(
            f"SELECT MAX(date) FROM daily_bars WHERE {_normalized_date_sql('date')} <= ?",
            [int(normalized_anchor_dt)],
        ).fetchone()
        if row and row[0] is not None:
            return int(row[0])
        return None
    row = conn.execute("SELECT MAX(dt) FROM ml_feature_daily").fetchone()
    if row and row[0] is not None:
        return int(row[0])
    row = conn.execute("SELECT MAX(date) FROM daily_bars").fetchone()
    if row and row[0] is not None:
        return int(row[0])
    return None


def _resolve_target_dates(conn, *, lookback_days: int, anchor_dt: int) -> list[int]:
    rows = conn.execute(
        """
        SELECT DISTINCT dt
        FROM ml_feature_daily
        WHERE dt <= ?
        ORDER BY dt DESC
        LIMIT ?
        """,
        [int(anchor_dt), int(lookback_days)],
    ).fetchall()
    values = [int(row[0]) for row in rows if row and row[0] is not None]
    values.sort()
    return values


def _normalize_dt_key(value: int | None) -> int | None:
    if value is None:
        return None
    normalized = int(value)
    if normalized >= 1_000_000_000:
        return int(datetime.fromtimestamp(normalized, tz=timezone.utc).strftime("%Y%m%d"))
    return normalized


def _resolve_target_dates_in_range(conn, *, start_dt: int, end_dt: int) -> list[int]:
    lower = min(int(start_dt), int(end_dt))
    upper = max(int(start_dt), int(end_dt))
    rows = conn.execute(
        f"""
        SELECT DISTINCT dt
        FROM ml_feature_daily
        WHERE {_normalized_date_sql('dt')} BETWEEN ? AND ?
        ORDER BY dt ASC
        """,
        [int(lower), int(upper)],
    ).fetchall()
    return [int(row[0]) for row in rows if row and row[0] is not None]


def _resolve_target_scope(
    conn,
    *,
    lookback_days: int,
    anchor_dt: int | None,
    start_dt: int | None,
    end_dt: int | None,
) -> dict[str, Any]:
    normalized_start_dt = _normalize_dt_key(start_dt)
    normalized_end_dt = _normalize_dt_key(end_dt)
    if normalized_start_dt is not None or normalized_end_dt is not None:
        range_start = normalized_start_dt if normalized_start_dt is not None else normalized_end_dt
        range_end = normalized_end_dt if normalized_end_dt is not None else normalized_start_dt
        assert range_start is not None and range_end is not None
        target_dates = _resolve_target_dates_in_range(
            conn,
            start_dt=int(range_start),
            end_dt=int(range_end),
        )
        ordered_start = min(int(range_start), int(range_end))
        ordered_end = max(int(range_start), int(range_end))
        return {
            "anchor_dt": int(ordered_end),
            "start_dt": int(ordered_start),
            "end_dt": int(ordered_end),
            "target_dates": [int(value) for value in target_dates],
        }

    resolved_anchor_dt = _resolve_anchor_dt(conn, anchor_dt)
    if resolved_anchor_dt is None:
        return {
            "anchor_dt": None,
            "start_dt": None,
            "end_dt": None,
            "target_dates": [],
        }
    target_dates = _resolve_target_dates(conn, lookback_days=lookback_days, anchor_dt=resolved_anchor_dt)
    return {
        "anchor_dt": int(resolved_anchor_dt),
        "start_dt": int(target_dates[0]) if target_dates else int(resolved_anchor_dt),
        "end_dt": int(target_dates[-1]) if target_dates else int(resolved_anchor_dt),
        "target_dates": [int(value) for value in target_dates],
    }


def _query_existing_dates(conn, *, table_name: str, target_dates: list[int]) -> set[int]:
    if table_name not in _SUPPORTED_DATE_TABLES:
        raise ValueError(f"unsupported table for date query: {table_name}")
    if not target_dates:
        return set()
    placeholders = ", ".join("?" for _ in target_dates)
    rows = conn.execute(
        f"SELECT DISTINCT dt FROM {table_name} WHERE dt IN ({placeholders})",
        [int(value) for value in target_dates],
    ).fetchall()
    return {int(row[0]) for row in rows if row and row[0] is not None}


def _active_ml_model_version(conn) -> str | None:
    try:
        row = conn.execute(
            """
            SELECT model_version
            FROM ml_model_registry
            WHERE is_active = TRUE
            ORDER BY created_at DESC
            LIMIT 1
            """
        ).fetchone()
    except Exception:
        row = None
    if not row or row[0] is None:
        return None
    return str(row[0])


def _query_existing_ml_dates(
    conn,
    *,
    target_dates: list[int],
    active_model_version: str | None,
) -> set[int]:
    if not target_dates:
        return set()
    if not active_model_version:
        return _query_existing_dates(conn, table_name="ml_pred_20d", target_dates=target_dates)
    placeholders = ", ".join("?" for _ in target_dates)
    rows = conn.execute(
        f"""
        SELECT DISTINCT dt
        FROM ml_pred_20d
        WHERE dt IN ({placeholders})
          AND model_version = ?
        """,
        [int(value) for value in target_dates] + [str(active_model_version)],
    ).fetchall()
    return {int(row[0]) for row in rows if row and row[0] is not None}


def _query_existing_sell_dates(
    conn,
    *,
    target_dates: list[int],
) -> set[int]:
    if not target_dates:
        return set()
    placeholders = ", ".join("?" for _ in target_dates)
    try:
        rows = conn.execute(
            f"""
            SELECT DISTINCT dt
            FROM sell_analysis_daily
            WHERE dt IN ({placeholders})
              AND calc_version = ?
            """,
            [int(value) for value in target_dates] + [SELL_ANALYSIS_CALC_VERSION],
        ).fetchall()
    except Exception:
        return set()
    return {int(row[0]) for row in rows if row and row[0] is not None}


def _resolve_analysis_cache_coverage(
    conn,
    *,
    lookback_days: int,
    anchor_dt: int | None,
    start_dt: int | None,
    end_dt: int | None,
    include_sell: bool,
    include_phase: bool,
    force_recompute: bool = False,
) -> dict[str, Any]:
    scope = _resolve_target_scope(
        conn,
        lookback_days=lookback_days,
        anchor_dt=anchor_dt,
        start_dt=start_dt,
        end_dt=end_dt,
    )
    resolved_anchor_dt = scope["anchor_dt"]
    if resolved_anchor_dt is None:
        return {
            "anchor_dt": None,
            "start_dt": None,
            "end_dt": None,
            "target_dates": [],
            "active_ml_model_version": None,
            "sell_calc_version": SELL_ANALYSIS_CALC_VERSION,
            "missing_ml_dates": [],
            "missing_sell_dates": [],
            "missing_phase_dates": [],
            "force_recompute": bool(force_recompute),
        }

    target_dates = [int(value) for value in scope["target_dates"]]
    active_model_version = _active_ml_model_version(conn)
    if force_recompute:
        missing_ml_dates = [int(value) for value in target_dates]
    else:
        existing_ml_dates = _query_existing_ml_dates(
            conn,
            target_dates=target_dates,
            active_model_version=active_model_version,
        )
        missing_ml_dates = [dt for dt in target_dates if dt not in existing_ml_dates]

    if include_sell:
        if force_recompute:
            missing_sell_dates = [int(value) for value in target_dates]
        else:
            existing_sell_dates = _query_existing_sell_dates(conn, target_dates=target_dates)
            missing_sell_dates = [dt for dt in target_dates if dt not in existing_sell_dates]
    else:
        missing_sell_dates = []

    if include_phase:
        if force_recompute:
            missing_phase_dates = [int(value) for value in target_dates]
        else:
            existing_phase_dates = _query_existing_dates(conn, table_name="phase_pred_daily", target_dates=target_dates)
            missing_phase_dates = [dt for dt in target_dates if dt not in existing_phase_dates]
    else:
        missing_phase_dates = []

    return {
        "anchor_dt": int(resolved_anchor_dt),
        "start_dt": scope["start_dt"],
        "end_dt": scope["end_dt"],
        "target_dates": [int(value) for value in target_dates],
        "active_ml_model_version": active_model_version,
        "sell_calc_version": SELL_ANALYSIS_CALC_VERSION,
        "missing_ml_dates": [int(value) for value in missing_ml_dates],
        "missing_sell_dates": [int(value) for value in missing_sell_dates],
        "missing_phase_dates": [int(value) for value in missing_phase_dates],
        "force_recompute": bool(force_recompute),
    }


def _notify(progress_cb: ProgressCallback | None, progress: int, message: str) -> None:
    if progress_cb is None:
        return
    progress_cb(max(0, min(100, int(progress))), message)


def inspect_analysis_backfill_coverage(
    *,
    lookback_days: int = 130,
    anchor_dt: int | None = None,
    start_dt: int | None = None,
    end_dt: int | None = None,
    include_sell: bool = True,
    include_phase: bool = False,
    force_recompute: bool = False,
) -> dict[str, Any]:
    lookback_days = max(1, int(lookback_days))
    with get_conn() as conn:
        ml_service._ensure_ml_schema(conn)
        feature_count_row = conn.execute("SELECT COUNT(*) FROM ml_feature_daily").fetchone()
        feature_count = int(feature_count_row[0]) if feature_count_row and feature_count_row[0] is not None else 0
        if feature_count <= 0:
            ml_service.refresh_ml_feature_table(conn, feature_version=ml_service.FEATURE_VERSION)

        coverage = _resolve_analysis_cache_coverage(
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
                "sell_calc_version": SELL_ANALYSIS_CALC_VERSION,
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
    lookback_days: int = 130,
    anchor_dt: int | None = None,
    start_dt: int | None = None,
    end_dt: int | None = None,
    max_missing_days: int | None = None,
    include_sell: bool = True,
    include_phase: bool = False,
    force_recompute: bool = False,
    progress_cb: ProgressCallback | None = None,
) -> dict[str, Any]:
    lookback_days = max(1, int(lookback_days))
    max_missing_days = None if max_missing_days is None else max(1, int(max_missing_days))

    _notify(progress_cb, 2, "Checking backfill targets...")
    with get_conn() as conn:
        ml_service._ensure_ml_schema(conn)
        feature_count_row = conn.execute("SELECT COUNT(*) FROM ml_feature_daily").fetchone()
        feature_count = int(feature_count_row[0]) if feature_count_row and feature_count_row[0] is not None else 0
        if feature_count <= 0:
            _notify(progress_cb, 4, "Refreshing ml_feature_daily...")
            ml_service.refresh_ml_feature_table(conn, feature_version=ml_service.FEATURE_VERSION)

        coverage = _resolve_analysis_cache_coverage(
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
            _notify(progress_cb, progress, f"ML backfill ({done}/{total}) dt={int(last_dt)}")

        try:
            bulk_result = ml_service.predict_for_dates_bulk(
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
                _notify(progress_cb, progress, f"ML fallback ({idx}/{len(missing_ml_dates)}) dt={dt}")
                try:
                    result = ml_service.predict_for_dt(dt=int(dt))
                    predicted_dates.append(int(dt))
                    predicted_rows_total += int(result.get("rows") or 0)
                except Exception as exc:
                    logger.exception("ML backfill failed dt=%s: %s", dt, exc)
                    errors.append(f"ml_pred_20d dt={dt}: {exc}")

    sell_refreshed_dates: list[int] = []
    if include_sell:
        # Recompute sell rows for newly predicted dates even if row already exists.
        sell_targets = sorted({int(value) for value in [*missing_sell_dates, *predicted_dates]})
        if sell_targets:
            _notify(progress_cb, 80, f"Refreshing sell analysis for {len(sell_targets)} dates...")
            try:
                sell_result = accumulate_sell_analysis_for_dates(target_dates=sell_targets)
                sell_refreshed_dates = [int(v) for v in sell_result.get("target_dates", [])]
            except Exception as exc:
                logger.exception("Sell analysis backfill failed: %s", exc)
                errors.append(f"sell_analysis_daily: {exc}")

    phase_refreshed_range: dict[str, int] | None = None
    if include_phase and missing_phase_dates:
        start_dt = int(missing_phase_dates[0])
        end_dt = int(missing_phase_dates[-1])
        _notify(progress_cb, 90, f"Refreshing phase prediction range={start_dt}..{end_dt}")
        try:
            run_batch(start_dt, end_dt, dry_run=False)
            phase_refreshed_range = {"start_dt": start_dt, "end_dt": end_dt}
        except Exception as exc:
            logger.exception("Phase backfill failed range=%s..%s: %s", start_dt, end_dt, exc)
            errors.append(f"phase_pred_daily range={start_dt}..{end_dt}: {exc}")

    _notify(progress_cb, 100, "Backfill completed.")
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
            f"sell={len(sell_refreshed_dates)} phase={'ok' if phase_refreshed_range else 'skip'}"
        ),
    }
