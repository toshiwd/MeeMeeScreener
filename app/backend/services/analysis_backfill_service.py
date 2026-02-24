from __future__ import annotations

import logging
from typing import Any, Callable

from app.db.session import get_conn
from app.backend.services import ml_service
from app.backend.services.sell_analysis_accumulator import accumulate_sell_analysis_for_dates

try:
    from app.backend.jobs.phase_batch import run_batch
except ModuleNotFoundError:  # pragma: no cover - legacy tooling may import from app/backend on sys.path
    from jobs.phase_batch import run_batch  # type: ignore


logger = logging.getLogger(__name__)
ProgressCallback = Callable[[int, str], None]
_SUPPORTED_DATE_TABLES = {"ml_pred_20d", "sell_analysis_daily", "phase_pred_daily"}


def _resolve_anchor_dt(conn, anchor_dt: int | None) -> int | None:
    if anchor_dt is not None:
        return int(anchor_dt)
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


def _notify(progress_cb: ProgressCallback | None, progress: int, message: str) -> None:
    if progress_cb is None:
        return
    progress_cb(max(0, min(100, int(progress))), message)


def backfill_missing_analysis_history(
    *,
    lookback_days: int = 130,
    anchor_dt: int | None = None,
    max_missing_days: int | None = None,
    include_sell: bool = True,
    include_phase: bool = False,
    progress_cb: ProgressCallback | None = None,
) -> dict[str, Any]:
    lookback_days = max(1, int(lookback_days))
    max_missing_days = None if max_missing_days is None else max(1, int(max_missing_days))

    _notify(progress_cb, 2, "不足期間をスキャン中...")
    with get_conn() as conn:
        ml_service._ensure_ml_schema(conn)
        feature_count_row = conn.execute("SELECT COUNT(*) FROM ml_feature_daily").fetchone()
        feature_count = int(feature_count_row[0]) if feature_count_row and feature_count_row[0] is not None else 0
        if feature_count <= 0:
            _notify(progress_cb, 4, "ML特徴量テーブルを再生成中...")
            ml_service.refresh_ml_feature_table(conn, feature_version=ml_service.FEATURE_VERSION)

        resolved_anchor_dt = _resolve_anchor_dt(conn, anchor_dt)
        if resolved_anchor_dt is None:
            return {
                "ok": True,
                "anchor_dt": None,
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
            }

        target_dates = _resolve_target_dates(conn, lookback_days=lookback_days, anchor_dt=resolved_anchor_dt)
        existing_ml_dates = _query_existing_dates(conn, table_name="ml_pred_20d", target_dates=target_dates)
        missing_ml_dates = [dt for dt in target_dates if dt not in existing_ml_dates]
        missing_ml_total = len(missing_ml_dates)
        if max_missing_days is not None and len(missing_ml_dates) > max_missing_days:
            missing_ml_dates = missing_ml_dates[:max_missing_days]

        existing_sell_dates = (
            _query_existing_dates(conn, table_name="sell_analysis_daily", target_dates=target_dates)
            if include_sell
            else set()
        )
        missing_sell_dates = [dt for dt in target_dates if dt not in existing_sell_dates] if include_sell else []

        existing_phase_dates = (
            _query_existing_dates(conn, table_name="phase_pred_daily", target_dates=target_dates)
            if include_phase
            else set()
        )
        missing_phase_dates = [dt for dt in target_dates if dt not in existing_phase_dates] if include_phase else []

    errors: list[str] = []
    predicted_dates: list[int] = []
    predicted_rows_total = 0

    for idx, dt in enumerate(missing_ml_dates, start=1):
        # 10-70%: ML prediction by missing date.
        progress = 10 + int(60 * idx / max(1, len(missing_ml_dates)))
        _notify(progress_cb, progress, f"ML不足日を補完中 ({idx}/{len(missing_ml_dates)}) dt={dt}")
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
            _notify(progress_cb, 80, f"売り分析スナップショットを補完中 ({len(sell_targets)}日)")
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
        _notify(progress_cb, 90, f"局面予測を補完中 (range={start_dt}..{end_dt})")
        try:
            run_batch(start_dt, end_dt, dry_run=False)
            phase_refreshed_range = {"start_dt": start_dt, "end_dt": end_dt}
        except Exception as exc:
            logger.exception("Phase backfill failed range=%s..%s: %s", start_dt, end_dt, exc)
            errors.append(f"phase_pred_daily range={start_dt}..{end_dt}: {exc}")

    _notify(progress_cb, 100, "不足期間バックフィル完了")
    return {
        "ok": len(errors) == 0,
        "anchor_dt": int(resolved_anchor_dt) if resolved_anchor_dt is not None else None,
        "lookback_days": int(lookback_days),
        "target_dates": [int(value) for value in target_dates],
        "missing_ml_total": int(missing_ml_total),
        "missing_ml_selected": int(len(missing_ml_dates)),
        "predicted_dates": predicted_dates,
        "predicted_rows_total": int(predicted_rows_total),
        "sell_refreshed_dates": sell_refreshed_dates,
        "phase_refreshed_range": phase_refreshed_range,
        "errors": errors,
        "message": (
            f"ml={len(predicted_dates)}/{len(missing_ml_dates)} "
            f"sell={len(sell_refreshed_dates)} phase={'ok' if phase_refreshed_range else 'skip'}"
        ),
    }
