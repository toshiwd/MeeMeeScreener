from __future__ import annotations

import logging
from typing import Any, Callable

import duckdb
import pandas as pd

from app.backend.core.legacy_analysis_control import is_legacy_analysis_disabled
from app.db.session import get_conn


logger = logging.getLogger(__name__)
SELL_ANALYSIS_CALC_VERSION = "1"
ProgressCallback = Callable[[int, str], None]


def _notify(progress_cb: ProgressCallback | None, progress: int, message: str) -> None:
    if progress_cb is None:
        return
    progress_cb(max(0, min(100, int(progress))), str(message))


def _ensure_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sell_analysis_daily (
            dt INTEGER,
            code TEXT,
            close DOUBLE,
            day_change_pct DOUBLE,
            p_down DOUBLE,
            p_turn_down DOUBLE,
            ev20_net DOUBLE,
            rank_down_20 DOUBLE,
            pred_dt INTEGER,
            p_up_5 DOUBLE,
            p_up_10 DOUBLE,
            p_up_20 DOUBLE,
            short_score DOUBLE,
            a_score DOUBLE,
            b_score DOUBLE,
            ma20 DOUBLE,
            ma60 DOUBLE,
            ma20_slope DOUBLE,
            ma60_slope DOUBLE,
            dist_ma20_signed DOUBLE,
            dist_ma60_signed DOUBLE,
            trend_down BOOLEAN,
            trend_down_strict BOOLEAN,
            fwd_close_5 DOUBLE,
            fwd_close_10 DOUBLE,
            fwd_close_20 DOUBLE,
            short_ret_5 DOUBLE,
            short_ret_10 DOUBLE,
            short_ret_20 DOUBLE,
            short_win_5 BOOLEAN,
            short_win_10 BOOLEAN,
            short_win_20 BOOLEAN,
            calc_version TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            PRIMARY KEY(code, dt)
        );
        """
    )
    conn.execute("ALTER TABLE sell_analysis_daily ADD COLUMN IF NOT EXISTS fwd_close_5 DOUBLE")
    conn.execute("ALTER TABLE sell_analysis_daily ADD COLUMN IF NOT EXISTS fwd_close_10 DOUBLE")
    conn.execute("ALTER TABLE sell_analysis_daily ADD COLUMN IF NOT EXISTS fwd_close_20 DOUBLE")
    conn.execute("ALTER TABLE sell_analysis_daily ADD COLUMN IF NOT EXISTS short_ret_5 DOUBLE")
    conn.execute("ALTER TABLE sell_analysis_daily ADD COLUMN IF NOT EXISTS short_ret_10 DOUBLE")
    conn.execute("ALTER TABLE sell_analysis_daily ADD COLUMN IF NOT EXISTS short_ret_20 DOUBLE")
    conn.execute("ALTER TABLE sell_analysis_daily ADD COLUMN IF NOT EXISTS short_win_5 BOOLEAN")
    conn.execute("ALTER TABLE sell_analysis_daily ADD COLUMN IF NOT EXISTS short_win_10 BOOLEAN")
    conn.execute("ALTER TABLE sell_analysis_daily ADD COLUMN IF NOT EXISTS short_win_20 BOOLEAN")
    conn.execute("ALTER TABLE sell_analysis_daily ADD COLUMN IF NOT EXISTS calc_version TEXT")


def _refresh_future_outcomes(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        WITH future AS (
            SELECT
                code,
                date AS dt,
                LEAD(c, 5) OVER (PARTITION BY code ORDER BY date) AS fwd_close_5,
                LEAD(c, 10) OVER (PARTITION BY code ORDER BY date) AS fwd_close_10,
                LEAD(c, 20) OVER (PARTITION BY code ORDER BY date) AS fwd_close_20
            FROM daily_bars
        ),
        calc AS (
            SELECT
                s.code,
                s.dt,
                f.fwd_close_5,
                f.fwd_close_10,
                f.fwd_close_20,
                CASE
                    WHEN s.close IS NULL OR s.close = 0 OR f.fwd_close_5 IS NULL THEN NULL
                    ELSE (s.close - f.fwd_close_5) / s.close
                END AS short_ret_5,
                CASE
                    WHEN s.close IS NULL OR s.close = 0 OR f.fwd_close_10 IS NULL THEN NULL
                    ELSE (s.close - f.fwd_close_10) / s.close
                END AS short_ret_10,
                CASE
                    WHEN s.close IS NULL OR s.close = 0 OR f.fwd_close_20 IS NULL THEN NULL
                    ELSE (s.close - f.fwd_close_20) / s.close
                END AS short_ret_20,
                CASE
                    WHEN s.close IS NULL OR s.close = 0 OR f.fwd_close_5 IS NULL THEN NULL
                    WHEN (s.close - f.fwd_close_5) / s.close > 0 THEN TRUE
                    ELSE FALSE
                END AS short_win_5,
                CASE
                    WHEN s.close IS NULL OR s.close = 0 OR f.fwd_close_10 IS NULL THEN NULL
                    WHEN (s.close - f.fwd_close_10) / s.close > 0 THEN TRUE
                    ELSE FALSE
                END AS short_win_10,
                CASE
                    WHEN s.close IS NULL OR s.close = 0 OR f.fwd_close_20 IS NULL THEN NULL
                    WHEN (s.close - f.fwd_close_20) / s.close > 0 THEN TRUE
                    ELSE FALSE
                END AS short_win_20
            FROM sell_analysis_daily s
            LEFT JOIN future f
                ON f.code = s.code
               AND f.dt = s.dt
        )
        UPDATE sell_analysis_daily AS s
        SET
            fwd_close_5 = c.fwd_close_5,
            fwd_close_10 = c.fwd_close_10,
            fwd_close_20 = c.fwd_close_20,
            short_ret_5 = c.short_ret_5,
            short_ret_10 = c.short_ret_10,
            short_ret_20 = c.short_ret_20,
            short_win_5 = c.short_win_5,
            short_win_10 = c.short_win_10,
            short_win_20 = c.short_win_20,
            updated_at = CURRENT_TIMESTAMP
        FROM calc c
        WHERE s.code = c.code
          AND s.dt = c.dt
          AND (
              s.fwd_close_5 IS DISTINCT FROM c.fwd_close_5
              OR s.fwd_close_10 IS DISTINCT FROM c.fwd_close_10
              OR s.fwd_close_20 IS DISTINCT FROM c.fwd_close_20
              OR s.short_ret_5 IS DISTINCT FROM c.short_ret_5
              OR s.short_ret_10 IS DISTINCT FROM c.short_ret_10
              OR s.short_ret_20 IS DISTINCT FROM c.short_ret_20
              OR s.short_win_5 IS DISTINCT FROM c.short_win_5
              OR s.short_win_10 IS DISTINCT FROM c.short_win_10
              OR s.short_win_20 IS DISTINCT FROM c.short_win_20
          )
        """
    )


def _resolve_target_dates(
    conn: duckdb.DuckDBPyConnection,
    *,
    lookback_days: int,
    anchor_dt: int | None,
) -> list[int]:
    n = max(1, int(lookback_days))
    if anchor_dt is None:
        rows = conn.execute(
            """
            SELECT DISTINCT date
            FROM daily_bars
            ORDER BY date DESC
            LIMIT ?
            """,
            [n],
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT DISTINCT date
            FROM daily_bars
            WHERE date <= ?
            ORDER BY date DESC
            LIMIT ?
            """,
            [int(anchor_dt), n],
        ).fetchall()
    values = [int(row[0]) for row in rows if row and row[0] is not None]
    values.sort()
    return values


def _upsert_snapshot_for_date(conn: duckdb.DuckDBPyConnection, dt: int) -> int:
    conn.execute(
        """
        INSERT OR REPLACE INTO sell_analysis_daily (
            dt,
            code,
            close,
            day_change_pct,
            p_down,
            p_turn_down,
            ev20_net,
            rank_down_20,
            pred_dt,
            p_up_5,
            p_up_10,
            p_up_20,
            short_score,
            a_score,
            b_score,
            ma20,
            ma60,
            ma20_slope,
            ma60_slope,
            dist_ma20_signed,
            dist_ma60_signed,
            trend_down,
            trend_down_strict,
            calc_version,
            created_at,
            updated_at
        )
        WITH daily_rank AS (
            SELECT
                code,
                date,
                c,
                ROW_NUMBER() OVER (PARTITION BY code ORDER BY date DESC) AS rn
            FROM daily_bars
            WHERE date <= ?
        ),
        snap AS (
            SELECT
                code,
                c AS close
            FROM daily_rank
            WHERE rn = 1
        ),
        prev_close AS (
            SELECT
                code,
                c AS prev_close
            FROM daily_rank
            WHERE rn = 2
        ),
        ma_rank AS (
            SELECT
                code,
                date,
                ma20,
                ma60,
                ROW_NUMBER() OVER (PARTITION BY code ORDER BY date DESC) AS rn
            FROM daily_ma
            WHERE date <= ?
        ),
        ma_curr AS (
            SELECT
                code,
                ma20,
                ma60
            FROM ma_rank
            WHERE rn = 1
        ),
        ma_prev AS (
            SELECT
                code,
                ma20 AS prev_ma20,
                ma60 AS prev_ma60
            FROM ma_rank
            WHERE rn = 2
        ),
        ml_rank AS (
            SELECT
                code,
                dt AS pred_dt,
                p_down,
                p_turn_down,
                ev20_net,
                rank_down_20,
                p_up_5,
                p_up_10,
                p_up,
                ROW_NUMBER() OVER (PARTITION BY code ORDER BY dt DESC) AS rn
            FROM ml_pred_20d
            WHERE dt <= ?
        ),
        ml_curr AS (
            SELECT
                code,
                pred_dt,
                p_down,
                p_turn_down,
                ev20_net,
                rank_down_20,
                p_up_5,
                p_up_10,
                p_up
            FROM ml_rank
            WHERE rn = 1
        ),
        score_curr AS (
            SELECT
                code,
                CAST(COALESCE(score_a, 0) + COALESCE(score_b, 0) AS DOUBLE) AS short_score,
                CAST(score_a AS DOUBLE) AS a_score,
                CAST(score_b AS DOUBLE) AS b_score
            FROM stock_scores
        )
        SELECT
            ? AS dt,
            s.code,
            s.close,
            CASE
                WHEN p.prev_close IS NULL OR p.prev_close = 0 THEN NULL
                ELSE (s.close - p.prev_close) / p.prev_close
            END AS day_change_pct,
            ml.p_down,
            ml.p_turn_down,
            ml.ev20_net,
            ml.rank_down_20,
            ml.pred_dt,
            ml.p_up_5,
            ml.p_up_10,
            ml.p_up AS p_up_20,
            sc.short_score,
            sc.a_score,
            sc.b_score,
            mc.ma20,
            mc.ma60,
            CASE
                WHEN mp.prev_ma20 IS NULL OR mc.ma20 IS NULL THEN NULL
                ELSE mc.ma20 - mp.prev_ma20
            END AS ma20_slope,
            CASE
                WHEN mp.prev_ma60 IS NULL OR mc.ma60 IS NULL THEN NULL
                ELSE mc.ma60 - mp.prev_ma60
            END AS ma60_slope,
            CASE
                WHEN mc.ma20 IS NULL OR mc.ma20 = 0 THEN NULL
                ELSE (s.close - mc.ma20) / mc.ma20
            END AS dist_ma20_signed,
            CASE
                WHEN mc.ma60 IS NULL OR mc.ma60 = 0 THEN NULL
                ELSE (s.close - mc.ma60) / mc.ma60
            END AS dist_ma60_signed,
            CASE
                WHEN mc.ma20 IS NOT NULL
                 AND mc.ma60 IS NOT NULL
                 AND s.close < mc.ma20
                 AND mc.ma20 < mc.ma60
                THEN TRUE
                ELSE FALSE
            END AS trend_down,
            CASE
                WHEN mc.ma20 IS NOT NULL
                 AND mc.ma60 IS NOT NULL
                 AND mp.prev_ma20 IS NOT NULL
                 AND mp.prev_ma60 IS NOT NULL
                 AND s.close < mc.ma20
                 AND mc.ma20 < mc.ma60
                 AND (mc.ma20 - mp.prev_ma20) < 0
                 AND (mc.ma60 - mp.prev_ma60) < 0
                 AND mc.ma20 <> 0
                 AND mc.ma60 <> 0
                 AND ((s.close - mc.ma20) / mc.ma20) <= -0.005
                 AND ((s.close - mc.ma60) / mc.ma60) <= -0.01
                THEN TRUE
                ELSE FALSE
            END AS trend_down_strict,
            ? AS calc_version,
            CURRENT_TIMESTAMP AS created_at,
            CURRENT_TIMESTAMP AS updated_at
        FROM snap s
        LEFT JOIN prev_close p
            ON p.code = s.code
        LEFT JOIN ma_curr mc
            ON mc.code = s.code
        LEFT JOIN ma_prev mp
            ON mp.code = s.code
        LEFT JOIN ml_curr ml
            ON ml.code = s.code
        LEFT JOIN score_curr sc
            ON sc.code = s.code
        """,
        [int(dt), int(dt), int(dt), int(dt), SELL_ANALYSIS_CALC_VERSION],
    )
    row = conn.execute(
        "SELECT COUNT(*) FROM sell_analysis_daily WHERE dt = ?",
        [int(dt)],
    ).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _upsert_snapshot_for_dates(conn: duckdb.DuckDBPyConnection, target_dates: list[int]) -> int:
    values = sorted({int(value) for value in target_dates if value is not None})
    if not values:
        return 0
    target_dates_df = pd.DataFrame({"dt": values})
    conn.register("sell_target_dates_df", target_dates_df)
    try:
        conn.execute(
            """
            CREATE OR REPLACE TEMP TABLE _tmp_sell_target_dates AS
            SELECT DISTINCT CAST(dt AS INTEGER) AS dt
            FROM sell_target_dates_df
            """
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO sell_analysis_daily (
                dt,
                code,
                close,
                day_change_pct,
                p_down,
                p_turn_down,
                ev20_net,
                rank_down_20,
                pred_dt,
                p_up_5,
                p_up_10,
                p_up_20,
                short_score,
                a_score,
                b_score,
                ma20,
                ma60,
                ma20_slope,
                ma60_slope,
                dist_ma20_signed,
                dist_ma60_signed,
                trend_down,
                trend_down_strict,
                calc_version,
                created_at,
                updated_at
            )
            WITH target_dates AS (
                SELECT dt
                FROM _tmp_sell_target_dates
            ),
            daily_rank AS (
                SELECT
                    t.dt AS target_dt,
                    d.code,
                    d.date,
                    d.c,
                    ROW_NUMBER() OVER (
                        PARTITION BY t.dt, d.code
                        ORDER BY d.date DESC
                    ) AS rn
                FROM target_dates t
                JOIN daily_bars d
                    ON d.date <= t.dt
            ),
            snap AS (
                SELECT
                    target_dt AS dt,
                    code,
                    c AS close
                FROM daily_rank
                WHERE rn = 1
            ),
            prev_close AS (
                SELECT
                    target_dt AS dt,
                    code,
                    c AS prev_close
                FROM daily_rank
                WHERE rn = 2
            ),
            ma_rank AS (
                SELECT
                    t.dt AS target_dt,
                    m.code,
                    m.date,
                    m.ma20,
                    m.ma60,
                    ROW_NUMBER() OVER (
                        PARTITION BY t.dt, m.code
                        ORDER BY m.date DESC
                    ) AS rn
                FROM target_dates t
                JOIN daily_ma m
                    ON m.date <= t.dt
            ),
            ma_curr AS (
                SELECT
                    target_dt AS dt,
                    code,
                    ma20,
                    ma60
                FROM ma_rank
                WHERE rn = 1
            ),
            ma_prev AS (
                SELECT
                    target_dt AS dt,
                    code,
                    ma20 AS prev_ma20,
                    ma60 AS prev_ma60
                FROM ma_rank
                WHERE rn = 2
            ),
            ml_rank AS (
                SELECT
                    t.dt AS target_dt,
                    ml.code,
                    ml.dt AS pred_dt,
                    ml.p_down,
                    ml.p_turn_down,
                    ml.ev20_net,
                    ml.rank_down_20,
                    ml.p_up_5,
                    ml.p_up_10,
                    ml.p_up,
                    ROW_NUMBER() OVER (
                        PARTITION BY t.dt, ml.code
                        ORDER BY ml.dt DESC
                    ) AS rn
                FROM target_dates t
                JOIN ml_pred_20d ml
                    ON ml.dt <= t.dt
            ),
            ml_curr AS (
                SELECT
                    target_dt AS dt,
                    code,
                    pred_dt,
                    p_down,
                    p_turn_down,
                    ev20_net,
                    rank_down_20,
                    p_up_5,
                    p_up_10,
                    p_up
                FROM ml_rank
                WHERE rn = 1
            ),
            score_curr AS (
                SELECT
                    code,
                    CAST(COALESCE(score_a, 0) + COALESCE(score_b, 0) AS DOUBLE) AS short_score,
                    CAST(score_a AS DOUBLE) AS a_score,
                    CAST(score_b AS DOUBLE) AS b_score
                FROM stock_scores
            ),
            existing AS (
                SELECT
                    s.dt,
                    s.code,
                    s.created_at
                FROM sell_analysis_daily s
                JOIN target_dates t
                    ON t.dt = s.dt
            )
            SELECT
                s.dt,
                s.code,
                s.close,
                CASE
                    WHEN p.prev_close IS NULL OR p.prev_close = 0 THEN NULL
                    ELSE (s.close - p.prev_close) / p.prev_close
                END AS day_change_pct,
                ml.p_down,
                ml.p_turn_down,
                ml.ev20_net,
                ml.rank_down_20,
                ml.pred_dt,
                ml.p_up_5,
                ml.p_up_10,
                ml.p_up AS p_up_20,
                sc.short_score,
                sc.a_score,
                sc.b_score,
                mc.ma20,
                mc.ma60,
                CASE
                    WHEN mp.prev_ma20 IS NULL OR mc.ma20 IS NULL THEN NULL
                    ELSE mc.ma20 - mp.prev_ma20
                END AS ma20_slope,
                CASE
                    WHEN mp.prev_ma60 IS NULL OR mc.ma60 IS NULL THEN NULL
                    ELSE mc.ma60 - mp.prev_ma60
                END AS ma60_slope,
                CASE
                    WHEN mc.ma20 IS NULL OR mc.ma20 = 0 THEN NULL
                    ELSE (s.close - mc.ma20) / mc.ma20
                END AS dist_ma20_signed,
                CASE
                    WHEN mc.ma60 IS NULL OR mc.ma60 = 0 THEN NULL
                    ELSE (s.close - mc.ma60) / mc.ma60
                END AS dist_ma60_signed,
                CASE
                    WHEN mc.ma20 IS NOT NULL
                     AND mc.ma60 IS NOT NULL
                     AND s.close < mc.ma20
                     AND mc.ma20 < mc.ma60
                    THEN TRUE
                    ELSE FALSE
                END AS trend_down,
                CASE
                    WHEN mc.ma20 IS NOT NULL
                     AND mc.ma60 IS NOT NULL
                     AND mp.prev_ma20 IS NOT NULL
                     AND mp.prev_ma60 IS NOT NULL
                     AND s.close < mc.ma20
                     AND mc.ma20 < mc.ma60
                     AND (mc.ma20 - mp.prev_ma20) < 0
                     AND (mc.ma60 - mp.prev_ma60) < 0
                     AND mc.ma20 <> 0
                     AND mc.ma60 <> 0
                     AND ((s.close - mc.ma20) / mc.ma20) <= -0.005
                     AND ((s.close - mc.ma60) / mc.ma60) <= -0.01
                    THEN TRUE
                    ELSE FALSE
                END AS trend_down_strict,
                ? AS calc_version,
                COALESCE(e.created_at, CURRENT_TIMESTAMP) AS created_at,
                CURRENT_TIMESTAMP AS updated_at
            FROM snap s
            LEFT JOIN prev_close p
                ON p.dt = s.dt
               AND p.code = s.code
            LEFT JOIN ma_curr mc
                ON mc.dt = s.dt
               AND mc.code = s.code
            LEFT JOIN ma_prev mp
                ON mp.dt = s.dt
               AND mp.code = s.code
            LEFT JOIN ml_curr ml
                ON ml.dt = s.dt
               AND ml.code = s.code
            LEFT JOIN score_curr sc
                ON sc.code = s.code
            LEFT JOIN existing e
                ON e.dt = s.dt
               AND e.code = s.code
            """,
            [SELL_ANALYSIS_CALC_VERSION],
        )
        last_dt = int(values[-1])
        row = conn.execute(
            "SELECT COUNT(*) FROM sell_analysis_daily WHERE dt = ?",
            [last_dt],
        ).fetchone()
        return int(row[0]) if row and row[0] is not None else 0
    finally:
        try:
            conn.execute("DROP TABLE IF EXISTS _tmp_sell_target_dates")
        except Exception:
            pass
        try:
            conn.unregister("sell_target_dates_df")
        except Exception:
            pass


def accumulate_sell_analysis(
    *,
    lookback_days: int = 1,
    anchor_dt: int | None = None,
    progress_cb: ProgressCallback | None = None,
) -> dict[str, Any]:
    if is_legacy_analysis_disabled():
        logger.info("Skipping sell analysis accumulation because legacy analysis is disabled.")
        return {
            "target_dates": [],
            "last_dt": None,
            "rows_last_dt": 0,
            "disabled": True,
        }
    with get_conn() as conn:
        _ensure_table(conn)
        target_dates = _resolve_target_dates(
            conn,
            lookback_days=lookback_days,
            anchor_dt=anchor_dt,
        )
        if not target_dates:
            return {
                "target_dates": [],
                "last_dt": None,
                "rows_last_dt": 0,
            }
        _notify(progress_cb, 20, f"Building sell snapshots for {len(target_dates)} dates...")
        rows_last_dt = _upsert_snapshot_for_dates(conn, target_dates)
        _notify(progress_cb, 80, "Refreshing sell future outcomes...")
        _refresh_future_outcomes(conn)
        _notify(progress_cb, 100, "Sell analysis refresh completed.")
        return {
            "target_dates": target_dates,
            "last_dt": int(target_dates[-1]),
            "rows_last_dt": int(rows_last_dt),
        }


def accumulate_sell_analysis_for_dates(
    *,
    target_dates: list[int],
    progress_cb: ProgressCallback | None = None,
) -> dict[str, Any]:
    values = sorted({int(value) for value in target_dates if value is not None})
    if is_legacy_analysis_disabled():
        logger.info("Skipping sell analysis accumulation for explicit dates because legacy analysis is disabled.")
        return {
            "target_dates": values,
            "last_dt": int(values[-1]) if values else None,
            "rows_last_dt": 0,
            "disabled": True,
        }
    with get_conn() as conn:
        _ensure_table(conn)
        if not values:
            return {
                "target_dates": [],
                "last_dt": None,
                "rows_last_dt": 0,
            }
        _notify(progress_cb, 20, f"Building sell snapshots for {len(values)} dates...")
        rows_last_dt = _upsert_snapshot_for_dates(conn, values)
        _notify(progress_cb, 80, "Refreshing sell future outcomes...")
        _refresh_future_outcomes(conn)
        _notify(progress_cb, 100, "Sell analysis refresh completed.")
        return {
            "target_dates": values,
            "last_dt": int(values[-1]),
            "rows_last_dt": int(rows_last_dt),
        }
