from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from external_analysis.contracts.paths import resolve_export_db_path
from external_analysis.exporter.export_schema import connect_export_db
from external_analysis.labels.store import connect_label_db, ensure_label_schema
from external_analysis.runtime.incremental_cache import LABEL_RELEVANT_EXPORT_TABLES, probe_label_cache, upsert_manifest

HORIZONS: tuple[int, ...] = (5, 10, 20, 40, 60)
EMBARGO_BY_HORIZON: dict[int, int] = {5: 2, 10: 3, 20: 5, 40: 5, 60: 5}
POLICY_VERSION = "purged-walk-forward-v1"
LABEL_CODE_CHUNK_SIZE = 64


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _run_id(kind: str) -> str:
    return _utcnow().strftime(f"{kind}_%Y%m%dT%H%M%S%fZ")


def _load_trading_dates(export_db_path: str | None = None) -> list[int]:
    export_conn = connect_export_db(export_db_path, read_only=True)
    try:
        rows = export_conn.execute(
            """
            SELECT DISTINCT trade_date
            FROM bars_daily_export
            WHERE trade_date IS NOT NULL
            ORDER BY trade_date
            """
        ).fetchall()
    finally:
        export_conn.close()
    return [int(row[0]) for row in rows if row and row[0] is not None]


def _load_export_codes(conn, *, export_alias: str) -> list[str]:
    rows = conn.execute(f"SELECT DISTINCT code FROM {export_alias}.bars_daily_export ORDER BY code").fetchall()
    return [str(row[0]) for row in rows if row and row[0] is not None]


def _quantile_sql(count_column: str, pct: float) -> str:
    return f"GREATEST(1, CAST(FLOOR({count_column} * {pct}) AS INTEGER))"


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


def _affected_filter_sql(affected_dates: set[int]) -> tuple[str, list[int]]:
    if not affected_dates:
        return "", []
    placeholders = ", ".join(["?"] * len(affected_dates))
    clause = f" AND cur.trade_date IN ({placeholders})"
    return clause, sorted(int(value) for value in affected_dates)


def _chunk_values(values: list[str], chunk_size: int) -> list[list[str]]:
    if chunk_size <= 0:
        raise ValueError("chunk_size_must_be_positive")
    return [values[index : index + chunk_size] for index in range(0, len(values), chunk_size)]


def _insert_horizon_labels(
    conn,
    *,
    export_alias: str,
    horizon: int,
    run_id: str,
    probe_action: str,
    affected_dates: set[int],
) -> int:
    table_name = f"label_daily_h{horizon}"
    if probe_action == "partial" and affected_dates:
        placeholders = ", ".join(["?"] * len(affected_dates))
        conn.execute(f"DELETE FROM {table_name} WHERE as_of_date IN ({placeholders})", sorted(affected_dates))
    else:
        conn.execute(f"DELETE FROM {table_name}")

    affected_filter_sql, affected_params = _affected_filter_sql(affected_dates if probe_action == "partial" else set())
    codes = _load_export_codes(conn, export_alias=export_alias)
    raw_table_name = f"temp_label_raw_h{horizon}"
    conn.execute(f"DROP TABLE IF EXISTS {raw_table_name}")
    conn.execute(
        f"""
        CREATE TEMP TABLE {raw_table_name} (
            code TEXT,
            as_of_date INTEGER,
            horizon_days INTEGER,
            ret_h DOUBLE,
            mfe_h DOUBLE,
            mae_h DOUBLE,
            days_to_mfe_h INTEGER,
            days_to_stop_h INTEGER,
            future_window_start_date INTEGER,
            future_window_end_date INTEGER,
            purge_end_date INTEGER,
            embargo_until_date INTEGER
        )
        """
    )
    for code_chunk in _chunk_values(codes, LABEL_CODE_CHUNK_SIZE):
        code_placeholders = ", ".join(["?"] * len(code_chunk))
        bars_code_filter_sql = f"WHERE code IN ({code_placeholders})"
        sql = f"""
        INSERT INTO {raw_table_name} (
            code, as_of_date, horizon_days, ret_h, mfe_h, mae_h, days_to_mfe_h, days_to_stop_h,
            future_window_start_date, future_window_end_date, purge_end_date, embargo_until_date
        )
        WITH bars AS (
            SELECT
                code,
                trade_date,
                h,
                l,
                c,
                ROW_NUMBER() OVER (PARTITION BY code ORDER BY trade_date) AS rn
            FROM {export_alias}.bars_daily_export
            {bars_code_filter_sql}
        ),
        market_dates AS (
            SELECT
                trade_date,
                ROW_NUMBER() OVER (ORDER BY trade_date) AS market_rn
            FROM (
                SELECT DISTINCT trade_date
                FROM {export_alias}.bars_daily_export
                WHERE trade_date IS NOT NULL
            )
        ),
        market_cap AS (
            SELECT MAX(market_rn) AS max_market_rn
            FROM market_dates
        ),
        eligible AS (
            SELECT
                cur.code,
                cur.trade_date AS as_of_date,
            cur.c AS current_close,
                next_bar.trade_date AS future_window_start_date,
                end_bar.trade_date AS future_window_end_date,
                end_bar.trade_date AS purge_end_date,
                end_bar.c AS future_close
            FROM bars cur
        JOIN bars next_bar
              ON next_bar.code = cur.code AND next_bar.rn = cur.rn + 1
            JOIN bars end_bar
              ON end_bar.code = cur.code AND end_bar.rn = cur.rn + {horizon}
            WHERE cur.c IS NOT NULL AND cur.c <> 0 AND end_bar.c IS NOT NULL
            {affected_filter_sql}
        ),
        future_window AS (
            SELECT
                cur.code,
                cur.trade_date AS as_of_date,
            window_bar.rn - cur.rn AS rel_day,
            window_bar.h,
            window_bar.l
            FROM bars cur
            JOIN bars window_bar
              ON window_bar.code = cur.code
             AND window_bar.rn BETWEEN cur.rn + 1 AND cur.rn + {horizon}
            WHERE cur.c IS NOT NULL AND cur.c <> 0
            {affected_filter_sql}
        ),
        window_stats AS (
            SELECT
                code,
                as_of_date,
                MAX(h) AS max_high,
                MIN(l) AS min_low
            FROM future_window
            GROUP BY code, as_of_date
        ),
        window_days AS (
            SELECT
                future_window.code,
                future_window.as_of_date,
            MIN(CASE WHEN future_window.h = window_stats.max_high THEN future_window.rel_day END) AS days_to_mfe_h,
            MIN(CASE WHEN future_window.l = window_stats.min_low THEN future_window.rel_day END) AS days_to_stop_h
            FROM future_window
            JOIN window_stats
              ON window_stats.code = future_window.code
             AND window_stats.as_of_date = future_window.as_of_date
            GROUP BY future_window.code, future_window.as_of_date
        ),
        scored AS (
            SELECT
                eligible.code,
                eligible.as_of_date,
            {horizon} AS horizon_days,
            (CAST(eligible.future_close AS DOUBLE) / CAST(eligible.current_close AS DOUBLE)) - 1.0 AS ret_h,
            (CAST(window_stats.max_high AS DOUBLE) / CAST(eligible.current_close AS DOUBLE)) - 1.0 AS mfe_h,
            (CAST(window_stats.min_low AS DOUBLE) / CAST(eligible.current_close AS DOUBLE)) - 1.0 AS mae_h,
            window_days.days_to_mfe_h,
            window_days.days_to_stop_h,
            COUNT(*) OVER (PARTITION BY eligible.as_of_date) AS cross_section_count,
            ROW_NUMBER() OVER (
                PARTITION BY eligible.as_of_date
                ORDER BY ((CAST(eligible.future_close AS DOUBLE) / CAST(eligible.current_close AS DOUBLE)) - 1.0) DESC, eligible.code ASC
            ) AS rank_ret_h,
            eligible.future_window_start_date,
            eligible.future_window_end_date,
            eligible.purge_end_date,
            embargo.trade_date AS embargo_until_date
        FROM eligible
        JOIN window_stats
          ON window_stats.code = eligible.code
         AND window_stats.as_of_date = eligible.as_of_date
        JOIN window_days
          ON window_days.code = eligible.code
         AND window_days.as_of_date = eligible.as_of_date
        JOIN market_dates market_end
          ON market_end.trade_date = eligible.future_window_end_date
            CROSS JOIN market_cap
            JOIN market_dates embargo
              ON embargo.market_rn = LEAST(market_cap.max_market_rn, market_end.market_rn + {EMBARGO_BY_HORIZON[horizon]})
            WHERE window_stats.max_high IS NOT NULL AND window_stats.min_low IS NOT NULL
        )
        SELECT
            code,
            as_of_date,
            horizon_days,
            ret_h,
            mfe_h,
            mae_h,
            days_to_mfe_h,
            days_to_stop_h,
            future_window_start_date,
            future_window_end_date,
            purge_end_date,
            embargo_until_date
        FROM scored
        """
        params = [*code_chunk, *affected_params, *affected_params]
        conn.execute(sql, params)
    conn.execute(
        f"""
        INSERT INTO {table_name} (
            code, as_of_date, horizon_days, ret_h, mfe_h, mae_h, days_to_mfe_h, days_to_stop_h,
            cross_section_count, rank_ret_h, top_1pct_h, top_3pct_h, top_5pct_h,
            future_window_start_date, future_window_end_date, purge_end_date, embargo_until_date,
            leakage_group_id, policy_version, generation_run_id
        )
        SELECT
            code,
            as_of_date,
            horizon_days,
            ret_h,
            mfe_h,
            mae_h,
            days_to_mfe_h,
            days_to_stop_h,
            cross_section_count,
            rank_ret_h,
            rank_ret_h <= {_quantile_sql('cross_section_count', 0.01)} AS top_1pct_h,
            rank_ret_h <= {_quantile_sql('cross_section_count', 0.03)} AS top_3pct_h,
            rank_ret_h <= {_quantile_sql('cross_section_count', 0.05)} AS top_5pct_h,
            future_window_start_date,
            future_window_end_date,
            purge_end_date,
            embargo_until_date,
            code || ':' || CAST(as_of_date AS VARCHAR) || ':' || CAST(future_window_end_date AS VARCHAR) AS leakage_group_id,
            ?,
            ?
        FROM (
            SELECT
                *,
                COUNT(*) OVER (PARTITION BY as_of_date) AS cross_section_count,
                ROW_NUMBER() OVER (PARTITION BY as_of_date ORDER BY ret_h DESC, code ASC) AS rank_ret_h
            FROM {raw_table_name}
        )
        """,
        [POLICY_VERSION, run_id],
    )
    conn.execute(f"DROP TABLE IF EXISTS {raw_table_name}")
    return int(
        conn.execute(f"SELECT COUNT(*) FROM {table_name} WHERE generation_run_id = ?", [run_id]).fetchone()[0] or 0
    )


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

    trading_dates = _load_trading_dates(export_db_path)
    affected_dates = _affected_as_of_dates(trading_dates=trading_dates, dirty_ranges=probe["dirty_ranges"])
    label_conn = connect_label_db(label_db_path)
    export_alias = "rolling_export"
    try:
        ensure_label_schema(label_conn)
        label_conn.execute("PRAGMA threads=1")
        label_conn.execute("PRAGMA preserve_insertion_order=false")
        export_path_sql = str(resolve_export_db_path(export_db_path)).replace("'", "''")
        label_conn.execute(f"ATTACH '{export_path_sql}' AS {export_alias} (READ_ONLY)")
        summary = {
            f"label_daily_h{horizon}": _insert_horizon_labels(
                label_conn,
                export_alias=export_alias,
                horizon=horizon,
                run_id=run_id,
                probe_action=str(probe["action"]),
                affected_dates=affected_dates,
            )
            for horizon in selected_horizons
        }
        label_conn.execute("DELETE FROM label_aux_monthly")
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
        try:
            label_conn.execute(f"DETACH {export_alias}")
        except Exception:
            pass
        label_conn.close()
