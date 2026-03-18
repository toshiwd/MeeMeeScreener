from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from external_analysis.exporter.export_schema import connect_export_db
from external_analysis.labels.store import connect_label_db, ensure_label_schema
from external_analysis.runtime.incremental_cache import LABEL_RELEVANT_EXPORT_TABLES, probe_label_cache, upsert_manifest

HORIZONS: tuple[int, ...] = (5, 10, 20, 40, 60)
EMBARGO_BY_HORIZON: dict[int, int] = {5: 2, 10: 3, 20: 5, 40: 5, 60: 5}
POLICY_VERSION = "purged-walk-forward-v1"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _run_id(kind: str) -> str:
    return _utcnow().strftime(f"{kind}_%Y%m%dT%H%M%S%fZ")


def _load_export_frames(export_db_path: str | None = None) -> tuple[list[int], dict[str, list[dict[str, Any]]]]:
    export_conn = connect_export_db(export_db_path)
    try:
        bar_rows = export_conn.execute(
            """
            SELECT b.code, b.trade_date, b.o, b.h, b.l, b.c, b.v, i.ma20
            FROM bars_daily_export b
            LEFT JOIN indicator_daily_export i
              ON i.code = b.code AND i.trade_date = b.trade_date
            ORDER BY b.code, b.trade_date
            """
        ).fetchall()
    finally:
        export_conn.close()
    trading_dates = sorted({int(row[1]) for row in bar_rows})
    by_code: dict[str, list[dict[str, Any]]] = {}
    for row in bar_rows:
        by_code.setdefault(str(row[0]), []).append(
            {
                "code": str(row[0]),
                "trade_date": int(row[1]),
                "o": row[2],
                "h": row[3],
                "l": row[4],
                "c": row[5],
                "v": row[6],
                "ma20": row[7],
            }
        )
    return trading_dates, by_code


def _quantile_flag(rank_position: int, total_count: int, pct: float) -> bool:
    if total_count <= 0:
        return False
    cutoff = max(1, int(total_count * pct))
    return rank_position <= cutoff


def _affected_as_of_dates(
    *,
    trading_dates: list[int],
    dirty_ranges: list[dict[str, Any]],
) -> set[int]:
    if not dirty_ranges:
        return set(trading_dates)
    affected: set[int] = set()
    trading_index = {int(value): idx for idx, value in enumerate(trading_dates)}
    for dirty in dirty_ranges:
        date_from = int(dirty["date_from"])
        date_to = int(dirty["date_to"])
        if date_from not in trading_index or date_to not in trading_index:
            continue
        start_idx = max(0, trading_index[date_from] - max(HORIZONS))
        end_idx = min(len(trading_dates) - 1, trading_index[date_to])
        affected.update(int(value) for value in trading_dates[start_idx : end_idx + 1])
    return affected


def build_rolling_labels(
    export_db_path: str | None = None,
    label_db_path: str | None = None,
    *,
    horizons: tuple[int, ...] | None = None,
) -> dict[str, Any]:
    started_at = _utcnow()
    run_id = _run_id("label")
    selected_horizons = tuple(horizons or HORIZONS)
    probe = probe_label_cache(
        export_db_path=export_db_path,
        label_db_path=label_db_path,
        generation_key="rolling_labels",
        dependency_version=POLICY_VERSION,
        relevant_tables=LABEL_RELEVANT_EXPORT_TABLES,
    )
    if probe["action"] == "skip":
        return {
            "ok": True,
            "run_id": run_id,
            "summary": {},
            "policy_version": POLICY_VERSION,
            "skipped": True,
            "cache_state": probe["cache_state"],
            "reason": probe["reason"],
            "dirty_ranges": [],
            "source_signature": probe.get("source_signature"),
        }
    trading_dates, by_code = _load_export_frames(export_db_path)
    index_by_date = {value: idx for idx, value in enumerate(trading_dates)}
    affected_dates = _affected_as_of_dates(trading_dates=trading_dates, dirty_ranges=probe["dirty_ranges"])
    label_rows_by_horizon: dict[int, list[dict[str, Any]]] = {horizon: [] for horizon in selected_horizons}
    for code, bars in by_code.items():
        for idx, bar in enumerate(bars):
            as_of_date = int(bar["trade_date"])
            if affected_dates and as_of_date not in affected_dates:
                continue
            current_close = bar["c"]
            if current_close in (None, 0):
                continue
            for horizon in selected_horizons:
                future_idx = idx + horizon
                if future_idx >= len(bars):
                    continue
                future_slice = bars[idx + 1 : future_idx + 1]
                future_close = bars[future_idx]["c"]
                if future_close is None:
                    continue
                highs = [float(item["h"]) for item in future_slice if item["h"] is not None]
                lows = [float(item["l"]) for item in future_slice if item["l"] is not None]
                if not highs or not lows:
                    continue
                max_high = max(highs)
                min_low = min(lows)
                days_to_mfe = next(
                    offset
                    for offset, item in enumerate(future_slice, start=1)
                    if item["h"] is not None and float(item["h"]) == max_high
                )
                days_to_stop = next(
                    offset
                    for offset, item in enumerate(future_slice, start=1)
                    if item["l"] is not None and float(item["l"]) == min_low
                )
                future_end_date = int(bars[future_idx]["trade_date"])
                future_end_index = index_by_date[future_end_date]
                embargo_index = min(len(trading_dates) - 1, future_end_index + EMBARGO_BY_HORIZON[horizon])
                label_rows_by_horizon[horizon].append(
                    {
                        "code": code,
                        "as_of_date": as_of_date,
                        "horizon_days": horizon,
                        "ret_h": (float(future_close) / float(current_close)) - 1.0,
                        "mfe_h": (max_high / float(current_close)) - 1.0,
                        "mae_h": (min_low / float(current_close)) - 1.0,
                        "days_to_mfe_h": days_to_mfe,
                        "days_to_stop_h": days_to_stop,
                        "cross_section_count": 0,
                        "rank_ret_h": None,
                        "top_1pct_h": False,
                        "top_3pct_h": False,
                        "top_5pct_h": False,
                        "future_window_start_date": int(future_slice[0]["trade_date"]),
                        "future_window_end_date": future_end_date,
                        "purge_end_date": future_end_date,
                        "embargo_until_date": int(trading_dates[embargo_index]),
                        "leakage_group_id": f"{code}:{as_of_date}:{future_end_date}",
                        "policy_version": POLICY_VERSION,
                        "generation_run_id": run_id,
                    }
                )
    for horizon, rows in label_rows_by_horizon.items():
        by_date: dict[int, list[dict[str, Any]]] = {}
        for row in rows:
            by_date.setdefault(int(row["as_of_date"]), []).append(row)
        for as_of_date, date_rows in by_date.items():
            ranked = sorted(date_rows, key=lambda item: float(item["ret_h"]), reverse=True)
            total_count = len(ranked)
            for position, row in enumerate(ranked, start=1):
                row["cross_section_count"] = total_count
                row["rank_ret_h"] = position
                row["top_1pct_h"] = _quantile_flag(position, total_count, 0.01)
                row["top_3pct_h"] = _quantile_flag(position, total_count, 0.03)
                row["top_5pct_h"] = _quantile_flag(position, total_count, 0.05)
    label_conn = connect_label_db(label_db_path)
    try:
        ensure_label_schema(label_conn)
        for horizon, rows in label_rows_by_horizon.items():
            if probe["action"] == "partial" and affected_dates:
                label_conn.execute(
                    f"DELETE FROM label_daily_h{horizon} WHERE as_of_date IN ({', '.join(['?'] * len(affected_dates))})",
                    sorted(affected_dates),
                )
            else:
                label_conn.execute(f"DELETE FROM label_daily_h{horizon}")
            if rows:
                columns = list(rows[0].keys())
                label_conn.executemany(
                    f"INSERT INTO label_daily_h{horizon} ({', '.join(columns)}) VALUES ({', '.join(['?'] * len(columns))})",
                    [[row[column] for column in columns] for row in rows],
                )
        label_conn.execute("DELETE FROM label_aux_monthly")
        summary = {
            f"label_daily_h{horizon}": len(label_rows_by_horizon[horizon]) for horizon in selected_horizons
        }
        label_conn.execute(
            """
            INSERT OR REPLACE INTO label_generation_runs (
                run_id, started_at, finished_at, status, kind, export_db_path, policy_version,
                horizon_set, collision_guard_enabled, overlap_guard_enabled, embargo_days, summary_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                run_id,
                started_at,
                _utcnow(),
                "success",
                "rolling_labels",
                str(export_db_path or ""),
                POLICY_VERSION,
                json.dumps(list(selected_horizons)),
                False,
                True,
                EMBARGO_BY_HORIZON[20],
                json.dumps(summary, ensure_ascii=False, sort_keys=True),
            ],
        )
        total_row_count = sum(int(value) for value in summary.values())
        upsert_manifest(
            conn=label_conn,
            table_name="label_generation_manifest",
            generation_key="rolling_labels",
            source_signature=str(probe.get("source_signature") or ""),
            dependency_version=POLICY_VERSION,
            cache_state="partial_stale" if probe["action"] == "partial" else "fresh",
            row_count=total_row_count,
            dirty_ranges=probe["dirty_ranges"],
            run_id=run_id,
        )
        label_conn.execute("CHECKPOINT")
        return {
            "ok": True,
            "run_id": run_id,
            "summary": summary,
            "policy_version": POLICY_VERSION,
            "skipped": False,
            "cache_state": "partial_stale" if probe["action"] == "partial" else "fresh",
            "reason": probe["reason"],
            "dirty_ranges": probe["dirty_ranges"],
            "source_signature": probe.get("source_signature"),
        }
    finally:
        label_conn.close()
