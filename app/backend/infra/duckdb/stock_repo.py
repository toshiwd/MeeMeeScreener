from __future__ import annotations
import duckdb
import os
import logging
import math
import time
from threading import Lock
from typing import List, Optional, Tuple, Any, Dict
import json
from datetime import datetime, timezone

from app.backend.core.legacy_analysis_control import is_legacy_analysis_disabled
from app.backend.core.bar_segments import (
    build_monthly_rows_from_daily,
    needs_history_backfill,
    prefer_richer_history,
    should_replace_monthly_with_daily,
    trim_to_latest_continuous_segment,
)
from app.backend.core.yahoo_history_rows import get_historical_daily_rows_from_chart
from app.db.session import get_conn_for_path

logger = logging.getLogger(__name__)
_SCHEMA_CACHE_TTL_SEC = max(5, int(os.getenv("MEEMEE_SCHEMA_CACHE_TTL_SEC", "60")))

class StockRepository:
    _instance = None
    _lock = Lock()

    def __init__(self, db_path: str):
        self._db_path = db_path
        self._conn = None
        self._schema_cache_ttl_sec = _SCHEMA_CACHE_TTL_SEC
        self._schema_cache_lock = Lock()
        self._table_exists_cache: dict[str, tuple[float, bool]] = {}
        self._column_exists_cache: dict[tuple[str, str], tuple[float, bool]] = {}
        self._column_type_cache: dict[tuple[str, str], tuple[float, str | None]] = {}
        # Cache: table_name -> (monotonic_ts, frozenset of lowercase column names)
        self._schema_columns_cache: dict[str, tuple[float, frozenset[str]]] = {}

    def _get_read_conn(self):
        # Use read-only access for query paths to reduce lock pressure.
        return get_conn_for_path(self._db_path, timeout_sec=2.5, read_only=True)

    def _get_write_conn(self):
        # Use the shared retry/connect policy from app.db.session.
        return get_conn_for_path(self._db_path, timeout_sec=2.5, read_only=False)

    def _legacy_analysis_reads_disabled(self) -> bool:
        return is_legacy_analysis_disabled()

    def get_all_codes(self) -> List[str]:
        with self._get_read_conn() as conn:
             rows = conn.execute("SELECT DISTINCT code FROM daily_bars ORDER BY code").fetchall()
        return [r[0] for r in rows]

    def get_daily_bars(
        self,
        code: str,
        limit: int = 400,
        asof_dt: int | None = None
    ) -> List[Tuple]:
        query = """
            SELECT date, o, h, l, c, v
            FROM daily_bars
            WHERE code = ?
        """
        params: List[Any] = [code]
        if asof_dt is not None:
            # daily_bars.date can be either epoch seconds or YYYYMMDD integer.
            asof_ymd = int(datetime.fromtimestamp(asof_dt, tz=timezone.utc).strftime("%Y%m%d"))
            query += " AND date <= CASE WHEN date >= 1000000000 THEN ? ELSE ? END"
            params.extend([asof_dt, asof_ymd])
        query += """
            ORDER BY date DESC
            LIMIT ?
        """
        params.append(limit)
        with self._get_read_conn() as conn:
            rows = conn.execute(query, params).fetchall()
        # Return valid sort order (ASC)
        rows = sorted(rows, key=lambda x: x[0])
        rows = trim_to_latest_continuous_segment(rows)
        return self._maybe_fill_sparse_daily_history(code, rows, limit=limit, asof_dt=asof_dt)

    def get_daily_bars_batch(
        self,
        codes: List[str],
        limit: int = 400,
        asof_dt: int | None = None,
    ) -> Dict[str, List[Tuple]]:
        unique_codes = [code for code in dict.fromkeys(str(code).strip() for code in codes) if code]
        if not unique_codes:
            return {}

        placeholders = ",".join(["?"] * len(unique_codes))
        query = f"""
            SELECT code, date, o, h, l, c, v
            FROM (
                SELECT
                    code,
                    date,
                    o,
                    h,
                    l,
                    c,
                    v,
                    ROW_NUMBER() OVER (PARTITION BY code ORDER BY date DESC) AS rn
                FROM daily_bars
                WHERE code IN ({placeholders})
        """
        params: List[Any] = list(unique_codes)
        if asof_dt is not None:
            asof_ymd = int(datetime.fromtimestamp(asof_dt, tz=timezone.utc).strftime("%Y%m%d"))
            query += " AND date <= CASE WHEN date >= 1000000000 THEN ? ELSE ? END"
            params.extend([asof_dt, asof_ymd])
        query += """
            )
            WHERE rn <= ?
            ORDER BY code, date
        """
        params.append(limit)

        with self._get_read_conn() as conn:
            rows = conn.execute(query, params).fetchall()

        grouped: Dict[str, List[Tuple]] = {code: [] for code in unique_codes}
        for row in rows:
            code = str(row[0])
            grouped.setdefault(code, []).append(tuple(row[1:]))
        for code, code_rows in list(grouped.items()):
            trimmed_rows = trim_to_latest_continuous_segment(code_rows)
            grouped[code] = self._maybe_fill_sparse_daily_history(code, trimmed_rows, limit=limit, asof_dt=asof_dt)
        return grouped

    def _maybe_fill_sparse_daily_history(
        self,
        code: str,
        rows: List[Tuple],
        *,
        limit: int,
        asof_dt: int | None,
    ) -> List[Tuple]:
        if not needs_history_backfill(rows):
            return rows
        yahoo_rows = get_historical_daily_rows_from_chart(code)
        if asof_dt is not None:
            yahoo_rows = [row for row in yahoo_rows if int(row[0]) <= int(asof_dt)]
        preferred = prefer_richer_history(rows, yahoo_rows)
        if limit > 0 and len(preferred) > limit:
            return preferred[-limit:]
        return preferred

    def get_monthly_bars(
        self,
        code: str,
        limit: int = 120,
        asof_dt: int | None = None
    ) -> List[Tuple]:
        query = """
            SELECT month, o, h, l, c, v
            FROM monthly_bars
            WHERE code = ?
        """
        params: List[Any] = [code]
        if asof_dt is not None:
            # monthly_bars.month can be epoch seconds, YYYYMMDD, or YYYYMM.
            asof_ymd = int(datetime.fromtimestamp(asof_dt, tz=timezone.utc).strftime("%Y%m%d"))
            asof_ym = int(datetime.fromtimestamp(asof_dt, tz=timezone.utc).strftime("%Y%m"))
            query += """
                AND month <= CASE
                    WHEN month >= 1000000000 THEN ?
                    WHEN month >= 10000000 THEN ?
                    ELSE ?
                END
            """
            params.extend([asof_dt, asof_ymd, asof_ym])
        query += """
            ORDER BY month DESC
            LIMIT ?
        """
        params.append(limit)
        with self._get_read_conn() as conn:
            rows = conn.execute(query, params).fetchall()
            if not rows:
                fallback_query = """
                    SELECT
                        CAST(epoch(date_trunc('month', to_timestamp(date))) AS BIGINT) AS month,
                        arg_min(o, date) AS o,
                        max(h) AS h,
                        min(l) AS l,
                        arg_max(c, date) AS c,
                        sum(v) AS v
                    FROM daily_bars
                    WHERE code = ?
                """
                fallback_params: List[Any] = [code]
                if asof_dt is not None:
                    asof_ymd = int(datetime.fromtimestamp(asof_dt, tz=timezone.utc).strftime("%Y%m%d"))
                    fallback_query += " AND date <= CASE WHEN date >= 1000000000 THEN ? ELSE ? END"
                    fallback_params.extend([asof_dt, asof_ymd])
                fallback_query += """
                    GROUP BY 1
                    ORDER BY 1 DESC
                    LIMIT ?
                """
                fallback_params.append(limit)
                rows = conn.execute(
                    fallback_query,
                    fallback_params,
                ).fetchall()
        rows = sorted(rows, key=lambda x: x[0])
        rows = trim_to_latest_continuous_segment(rows)
        recent_daily_rows = self.get_daily_bars(code, limit=max(limit * 25, 260), asof_dt=asof_dt)
        if should_replace_monthly_with_daily(recent_daily_rows, rows):
            rows = build_monthly_rows_from_daily(recent_daily_rows, limit=limit)
        return rows

    def get_monthly_bars_batch(
        self,
        codes: List[str],
        limit: int = 120,
        asof_dt: int | None = None,
    ) -> Dict[str, List[Tuple]]:
        unique_codes = [code for code in dict.fromkeys(str(code).strip() for code in codes) if code]
        if not unique_codes:
            return {}

        placeholders = ",".join(["?"] * len(unique_codes))
        query = f"""
            SELECT code, month, o, h, l, c, v
            FROM (
                SELECT
                    code,
                    month,
                    o,
                    h,
                    l,
                    c,
                    v,
                    ROW_NUMBER() OVER (PARTITION BY code ORDER BY month DESC) AS rn
                FROM monthly_bars
                WHERE code IN ({placeholders})
        """
        params: List[Any] = list(unique_codes)
        if asof_dt is not None:
            asof_ymd = int(datetime.fromtimestamp(asof_dt, tz=timezone.utc).strftime("%Y%m%d"))
            asof_ym = int(datetime.fromtimestamp(asof_dt, tz=timezone.utc).strftime("%Y%m"))
            query += """
                AND month <= CASE
                    WHEN month >= 1000000000 THEN ?
                    WHEN month >= 10000000 THEN ?
                    ELSE ?
                END
            """
            params.extend([asof_dt, asof_ymd, asof_ym])
        query += """
            )
            WHERE rn <= ?
            ORDER BY code, month
        """
        params.append(limit)

        with self._get_read_conn() as conn:
            rows = conn.execute(query, params).fetchall()
            grouped: Dict[str, List[Tuple]] = {code: [] for code in unique_codes}
            for row in rows:
                code = str(row[0])
                grouped.setdefault(code, []).append(tuple(row[1:]))

            missing_codes = [code for code, code_rows in grouped.items() if not code_rows]
            if not missing_codes:
                return grouped

            fallback_placeholders = ",".join(["?"] * len(missing_codes))
            fallback_query = f"""
                WITH monthly_agg AS (
                    SELECT
                        code,
                        CAST(epoch(date_trunc('month', to_timestamp(date))) AS BIGINT) AS month,
                        arg_min(o, date) AS o,
                        max(h) AS h,
                        min(l) AS l,
                        arg_max(c, date) AS c,
                        sum(v) AS v
                    FROM daily_bars
                    WHERE code IN ({fallback_placeholders})
            """
            fallback_params: List[Any] = list(missing_codes)
            if asof_dt is not None:
                asof_ymd = int(datetime.fromtimestamp(asof_dt, tz=timezone.utc).strftime("%Y%m%d"))
                fallback_query += " AND date <= CASE WHEN date >= 1000000000 THEN ? ELSE ? END"
                fallback_params.extend([asof_dt, asof_ymd])
            fallback_query += """
                    GROUP BY 1, 2
                )
                SELECT code, month, o, h, l, c, v
                FROM (
                    SELECT
                        code,
                        month,
                        o,
                        h,
                        l,
                        c,
                        v,
                        ROW_NUMBER() OVER (PARTITION BY code ORDER BY month DESC) AS rn
                    FROM monthly_agg
                )
                WHERE rn <= ?
                ORDER BY code, month
            """
            fallback_params.append(limit)
            fallback_rows = conn.execute(fallback_query, fallback_params).fetchall()
            for row in fallback_rows:
                code = str(row[0])
                grouped.setdefault(code, []).append(tuple(row[1:]))
        recent_daily_rows_by_code = self.get_daily_bars_batch(unique_codes, limit=max(limit * 25, 260), asof_dt=asof_dt)
        for code, code_rows in list(grouped.items()):
            trimmed_monthly = trim_to_latest_continuous_segment(code_rows)
            recent_daily = recent_daily_rows_by_code.get(code, [])
            if should_replace_monthly_with_daily(recent_daily, trimmed_monthly):
                grouped[code] = build_monthly_rows_from_daily(recent_daily, limit=limit)
            else:
                grouped[code] = trimmed_monthly
        return grouped

    def get_latest_params_for_screening(self, codes: Optional[List[str]] = None) -> List[Tuple]:
        if codes is not None and len(codes) == 0:
            return []

        with self._get_read_conn() as conn:
            code_filter = ""
            params: List[Any] = []
            if codes:
                placeholders = ",".join(["?"] * len(codes))
                code_filter = f"WHERE code IN ({placeholders})"
                params.extend(codes)

            try:
                query = f"""
                    SELECT
                        fs.code,
                        fs.dt,
                        fs.close,
                        fs.ma7,
                        fs.ma20,
                        fs.ma60,
                        fs.atr14,
                        fs.diff20_pct,
                        fs.diff20_atr,
                        fs.cnt_20_above,
                        fs.cnt_7_above,
                        fs.day_count,
                        fs.candle_flags
                    FROM feature_snapshot_daily fs
                    INNER JOIN (
                        SELECT code, MAX(dt) AS max_dt
                        FROM feature_snapshot_daily
                        {code_filter}
                        GROUP BY code
                    ) latest
                      ON latest.code = fs.code AND latest.max_dt = fs.dt
                    ORDER BY fs.code
                """
                return conn.execute(query, params).fetchall()
            except Exception as exc:
                # Fallback for environments where feature_snapshot_daily is not populated yet.
                logger.warning("feature_snapshot_daily query failed, fallback to daily_bars: %s", exc)

            fallback_filter = ""
            fallback_params: List[Any] = []
            if codes:
                placeholders = ",".join(["?"] * len(codes))
                fallback_filter = f"WHERE code IN ({placeholders})"
                fallback_params.extend(codes)

            fallback_query = f"""
                SELECT
                    b.code,
                    b.date AS dt,
                    b.c AS close,
                    m.ma7,
                    m.ma20,
                    m.ma60,
                    NULL AS atr14,
                    NULL AS diff20_pct,
                    NULL AS diff20_atr,
                    NULL AS cnt_20_above,
                    NULL AS cnt_7_above,
                    NULL AS day_count,
                    NULL AS candle_flags
                FROM daily_bars b
                INNER JOIN (
                    SELECT code, MAX(date) AS max_date
                    FROM daily_bars
                    {fallback_filter}
                    GROUP BY code
                ) latest
                  ON latest.code = b.code AND latest.max_date = b.date
                LEFT JOIN daily_ma m
                  ON m.code = b.code AND m.date = b.date
                ORDER BY b.code
            """
            return conn.execute(fallback_query, fallback_params).fetchall()

    def ensure_score_table(self):
        with self._get_write_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS stock_scores (
                    code VARCHAR PRIMARY KEY,
                    score_a FLOAT,
                    score_b FLOAT,
                    reasons VARCHAR,
                    badges VARCHAR,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            row = conn.execute(
                "SELECT sql FROM duckdb_tables() WHERE table_name = 'stock_scores'"
            ).fetchone()
            table_sql = str(row[0]).lower() if row and row[0] is not None else ""
            if "primary key" not in table_sql:
                conn.execute("DROP TABLE IF EXISTS _tmp_stock_scores_migration")
                try:
                    conn.execute("""
                        CREATE TABLE _tmp_stock_scores_migration (
                            code VARCHAR PRIMARY KEY,
                            score_a FLOAT,
                            score_b FLOAT,
                            reasons VARCHAR,
                            badges VARCHAR,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    conn.execute("""
                        INSERT INTO _tmp_stock_scores_migration (
                            code,
                            score_a,
                            score_b,
                            reasons,
                            badges,
                            updated_at
                        )
                        SELECT
                            code,
                            ANY_VALUE(score_a) AS score_a,
                            ANY_VALUE(score_b) AS score_b,
                            ANY_VALUE(reasons) AS reasons,
                            ANY_VALUE(badges) AS badges,
                            MAX(updated_at) AS updated_at
                        FROM stock_scores
                        GROUP BY code
                    """)
                    conn.execute("DROP TABLE stock_scores")
                    conn.execute("ALTER TABLE _tmp_stock_scores_migration RENAME TO stock_scores")
                finally:
                    conn.execute("DROP TABLE IF EXISTS _tmp_stock_scores_migration")

    def save_scores(self, scores: List[Dict[str, Any]], *, replace: bool = False):
        self.ensure_score_table()
        # scores: list of dicts with code, score_a, score_b, reasons, badges
        with self._get_write_conn() as conn:
            if replace:
                conn.execute("DELETE FROM stock_scores")

            if not scores:
                return

            # Use appender or executemany
            # DuckDB executemany is good
            data = []
            for s in scores:
                data.append((
                    s["code"], 
                    s["score_a"], 
                    s["score_b"], 
                    json.dumps(s["reasons"], ensure_ascii=False),
                    json.dumps(s["badges"], ensure_ascii=False)
                ))
            
            conn.executemany("""
                INSERT INTO stock_scores (code, score_a, score_b, reasons, badges, updated_at)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(code) DO UPDATE SET
                    score_a = excluded.score_a,
                    score_b = excluded.score_b,
                    reasons = excluded.reasons,
                    badges = excluded.badges,
                    updated_at = excluded.updated_at
            """, data)

    def get_scores(self) -> Dict[str, Dict]:
        with self._get_read_conn() as conn:
            if not self._table_exists(conn, "stock_scores"):
                return {}
            rows = conn.execute("SELECT code, score_a, score_b, reasons, badges FROM stock_scores").fetchall()
        
        result = {}
        for r in rows:
            result[r[0]] = {
                "score_a": r[1],
                "score_b": r[2],
                "reasons": json.loads(r[3]),
                "badges": json.loads(r[4])
            }
        return result

    def get_phase_pred(self, code: str, asof_dt: int | None) -> Optional[Tuple]:
        if self._legacy_analysis_reads_disabled():
            return None
        query = """
            SELECT dt, early_score, late_score, body_score, n, reasons_top3
            FROM phase_pred_daily
            WHERE code = ?
        """
        params: List[Any] = [code]
        if asof_dt is not None:
            query += " AND dt <= ?"
            params.append(asof_dt)
        query += " ORDER BY dt DESC LIMIT 1"
        with self._get_read_conn() as conn:
            row = conn.execute(query, params).fetchone()
        return row

    def _table_exists(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
        now = time.monotonic()
        with self._schema_cache_lock:
            cached = self._table_exists_cache.get(table_name)
        if cached and now - cached[0] <= self._schema_cache_ttl_sec:
            return cached[1]
        row = conn.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = ? LIMIT 1",
            [table_name],
        ).fetchone()
        exists = row is not None
        with self._schema_cache_lock:
            self._table_exists_cache[table_name] = (now, exists)
        return exists

    def _column_exists(self, conn: duckdb.DuckDBPyConnection, table_name: str, column_name: str) -> bool:
        key = (table_name, column_name)
        now = time.monotonic()
        with self._schema_cache_lock:
            cached = self._column_exists_cache.get(key)
        if cached and now - cached[0] <= self._schema_cache_ttl_sec:
            return cached[1]
        row = conn.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_name = ? AND column_name = ?
            LIMIT 1
            """,
            [table_name, column_name],
        ).fetchone()
        exists = row is not None
        with self._schema_cache_lock:
            self._column_exists_cache[key] = (now, exists)
        return exists

    def _column_type(
        self, conn: duckdb.DuckDBPyConnection, table_name: str, column_name: str
    ) -> str | None:
        key = (table_name, column_name)
        now = time.monotonic()
        with self._schema_cache_lock:
            cached = self._column_type_cache.get(key)
        if cached and now - cached[0] <= self._schema_cache_ttl_sec:
            return cached[1]
        row = conn.execute(
            """
            SELECT data_type
            FROM information_schema.columns
            WHERE table_name = ? AND column_name = ?
            LIMIT 1
            """,
            [table_name, column_name],
        ).fetchone()
        if not row:
            with self._schema_cache_lock:
                self._column_type_cache[key] = (now, None)
            return None
        value = row[0]
        column_type = str(value) if value is not None else None
        with self._schema_cache_lock:
            self._column_type_cache[key] = (now, column_type)
        return column_type

    def _get_schema_columns(
        self, conn: duckdb.DuckDBPyConnection, table_name: str
    ) -> frozenset[str]:
        """Return frozenset of lowercase column names for table_name.

        Uses a TTL cache so repeated calls within the same request window cost
        only one PRAGMA query instead of one information_schema query per column.
        """
        now = time.monotonic()
        with self._schema_cache_lock:
            cached = self._schema_columns_cache.get(table_name)
        if cached and now - cached[0] <= self._schema_cache_ttl_sec:
            return cached[1]
        try:
            rows = conn.execute(
                f"PRAGMA table_info('{table_name}')"
            ).fetchall()
            # PRAGMA table_info columns: cid, name, type, notnull, dflt_value, pk
            names: frozenset[str] = frozenset(
                str(row[1]).lower() for row in rows if row[1] is not None
            )
        except Exception:
            names = frozenset()
        with self._schema_cache_lock:
            self._schema_columns_cache[table_name] = (now, names)
        return names

    def _normalize_dt_key(self, value: Any) -> int | None:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            iv = int(value)
            if iv >= 1_000_000_000:
                try:
                    return int(datetime.fromtimestamp(iv, tz=timezone.utc).strftime("%Y%m%d"))
                except Exception:
                    return None
            if 19_000_101 <= iv <= 21_001_231:
                return iv
            return None
        text = str(value).strip()
        if not text:
            return None
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"):
            try:
                return int(datetime.strptime(text, fmt).strftime("%Y%m%d"))
            except ValueError:
                continue
        return None

    def get_ml_analysis_pred(self, code: str, asof_dt: int | None) -> Optional[Tuple]:
        if self._legacy_analysis_reads_disabled():
            return None
        with self._get_read_conn() as conn:
            if not self._table_exists(conn, "ml_pred_20d"):
                return None
            names = self._get_schema_columns(conn, "ml_pred_20d")
            if "code" not in names or "dt" not in names:
                return None

            dt_type = self._column_type(conn, "ml_pred_20d", "dt")
            select_parts = [
                "dt",
                "p_up" if "p_up" in names else "NULL::DOUBLE AS p_up",
                "p_down" if "p_down" in names else "NULL::DOUBLE AS p_down",
                "p_up_5" if "p_up_5" in names else "NULL::DOUBLE AS p_up_5",
                "p_up_10" if "p_up_10" in names else "NULL::DOUBLE AS p_up_10",
                "p_turn_up" if "p_turn_up" in names else "NULL::DOUBLE AS p_turn_up",
                "p_turn_down" if "p_turn_down" in names else "NULL::DOUBLE AS p_turn_down",
                "p_turn_down_5" if "p_turn_down_5" in names else "NULL::DOUBLE AS p_turn_down_5",
                "p_turn_down_10" if "p_turn_down_10" in names else "NULL::DOUBLE AS p_turn_down_10",
                "p_turn_down_20" if "p_turn_down_20" in names else "NULL::DOUBLE AS p_turn_down_20",
                "ret_pred5" if "ret_pred5" in names else "NULL::DOUBLE AS ret_pred5",
                "ret_pred10" if "ret_pred10" in names else "NULL::DOUBLE AS ret_pred10",
                "ret_pred20" if "ret_pred20" in names else "NULL::DOUBLE AS ret_pred20",
                "ev20" if "ev20" in names else "NULL::DOUBLE AS ev20",
                "ev20_net" if "ev20_net" in names else "NULL::DOUBLE AS ev20_net",
                "ev5_net" if "ev5_net" in names else "NULL::DOUBLE AS ev5_net",
                "ev10_net" if "ev10_net" in names else "NULL::DOUBLE AS ev10_net",
                "model_version" if "model_version" in names else "NULL::VARCHAR AS model_version",
            ]
            query = f"""
                SELECT {", ".join(select_parts)}
                FROM ml_pred_20d
                WHERE code = ?
            """
            params: List[Any] = [code]
            if asof_dt is not None:
                normalized_type = str(dt_type or "").upper()
                if any(
                    token in normalized_type
                    for token in ("INT", "DECIMAL", "NUMERIC", "DOUBLE", "REAL", "FLOAT")
                ):
                    asof_ymd = int(datetime.fromtimestamp(asof_dt, tz=timezone.utc).strftime("%Y%m%d"))
                    query += " AND dt <= CASE WHEN dt >= 1000000000 THEN ? ELSE ? END"
                    params.extend([asof_dt, asof_ymd])
                elif normalized_type:
                    asof_date = datetime.fromtimestamp(asof_dt, tz=timezone.utc).strftime("%Y-%m-%d")
                    query += " AND CAST(dt AS DATE) <= CAST(? AS DATE)"
                    params.append(asof_date)
                else:
                    asof_ymd = int(datetime.fromtimestamp(asof_dt, tz=timezone.utc).strftime("%Y%m%d"))
                    query += " AND dt <= CASE WHEN dt >= 1000000000 THEN ? ELSE ? END"
                    params.extend([asof_dt, asof_ymd])
            query += " ORDER BY dt DESC LIMIT 1"
            row = conn.execute(query, params).fetchone()
        return row

    def get_analysis_timeline(
        self,
        code: str,
        asof_dt: int | None,
        *,
        limit: int = 400,
    ) -> List[Dict[str, Any]]:
        if self._legacy_analysis_reads_disabled():
            return []
        resolved_limit = max(1, min(2000, int(limit)))

        def _to_float_or_none(value: Any) -> float | None:
            if not isinstance(value, (int, float)):
                return None
            fv = float(value)
            return fv if math.isfinite(fv) else None

        ml_rows_desc: List[Tuple[Any, ...]] = []
        sell_rows_desc: List[Tuple[Any, ...]] = []
        with self._get_read_conn() as conn:
            ml_names: frozenset[str] = frozenset()
            if self._table_exists(conn, "ml_pred_20d"):
                ml_names = self._get_schema_columns(conn, "ml_pred_20d")
            if "code" in ml_names and "dt" in ml_names:
                dt_type = self._column_type(conn, "ml_pred_20d", "dt")
                query = f"""
                    SELECT
                        dt,
                        {"p_up" if "p_up" in ml_names else "NULL::DOUBLE AS p_up"},
                        {"p_down" if "p_down" in ml_names else "NULL::DOUBLE AS p_down"},
                        {"p_turn_up" if "p_turn_up" in ml_names else "NULL::DOUBLE AS p_turn_up"},
                        {"p_turn_down" if "p_turn_down" in ml_names else "NULL::DOUBLE AS p_turn_down"},
                        {"ev20_net" if "ev20_net" in ml_names else "NULL::DOUBLE AS ev20_net"}
                    FROM ml_pred_20d
                    WHERE code = ?
                """
                params: List[Any] = [code]
                if asof_dt is not None:
                    normalized_type = str(dt_type or "").upper()
                    if any(
                        token in normalized_type
                        for token in ("INT", "DECIMAL", "NUMERIC", "DOUBLE", "REAL", "FLOAT")
                    ):
                        asof_ymd = int(datetime.fromtimestamp(asof_dt, tz=timezone.utc).strftime("%Y%m%d"))
                        query += " AND dt <= CASE WHEN dt >= 1000000000 THEN ? ELSE ? END"
                        params.extend([asof_dt, asof_ymd])
                    elif normalized_type:
                        asof_date = datetime.fromtimestamp(asof_dt, tz=timezone.utc).strftime("%Y-%m-%d")
                        query += " AND CAST(dt AS DATE) <= CAST(? AS DATE)"
                        params.append(asof_date)
                    else:
                        asof_ymd = int(datetime.fromtimestamp(asof_dt, tz=timezone.utc).strftime("%Y%m%d"))
                        query += " AND dt <= CASE WHEN dt >= 1000000000 THEN ? ELSE ? END"
                        params.extend([asof_dt, asof_ymd])
                query += " ORDER BY dt DESC LIMIT ?"
                params.append(resolved_limit)
                ml_rows_desc = conn.execute(query, params).fetchall()

            sell_names: frozenset[str] = frozenset()
            if self._table_exists(conn, "sell_analysis_daily"):
                sell_names = self._get_schema_columns(conn, "sell_analysis_daily")
            if "code" in sell_names and "dt" in sell_names:
                sell_dt_type = self._column_type(conn, "sell_analysis_daily", "dt")

                query = f"""
                    SELECT
                        dt,
                        {"p_down" if "p_down" in sell_names else "NULL::DOUBLE AS p_down"},
                        {"p_turn_down" if "p_turn_down" in sell_names else "NULL::DOUBLE AS p_turn_down"},
                        {"trend_down" if "trend_down" in sell_names else "NULL::BOOLEAN AS trend_down"},
                        {"trend_down_strict" if "trend_down_strict" in sell_names else "NULL::BOOLEAN AS trend_down_strict"},
                        {"short_ret_5" if "short_ret_5" in sell_names else "NULL::DOUBLE AS short_ret_5"},
                        {"short_ret_10" if "short_ret_10" in sell_names else "NULL::DOUBLE AS short_ret_10"},
                        {"short_ret_20" if "short_ret_20" in sell_names else "NULL::DOUBLE AS short_ret_20"},
                        {"short_win_5" if "short_win_5" in sell_names else "NULL::BOOLEAN AS short_win_5"},
                        {"short_win_10" if "short_win_10" in sell_names else "NULL::BOOLEAN AS short_win_10"},
                        {"short_win_20" if "short_win_20" in sell_names else "NULL::BOOLEAN AS short_win_20"}
                    FROM sell_analysis_daily
                    WHERE code = ?
                """
                params = [code]
                if asof_dt is not None:
                    normalized_type = str(sell_dt_type or "").upper()
                    if any(
                        token in normalized_type
                        for token in ("INT", "DECIMAL", "NUMERIC", "DOUBLE", "REAL", "FLOAT")
                    ):
                        asof_ymd = int(datetime.fromtimestamp(asof_dt, tz=timezone.utc).strftime("%Y%m%d"))
                        query += " AND dt <= CASE WHEN dt >= 1000000000 THEN ? ELSE ? END"
                        params.extend([asof_dt, asof_ymd])
                    elif normalized_type:
                        asof_date = datetime.fromtimestamp(asof_dt, tz=timezone.utc).strftime("%Y-%m-%d")
                        query += " AND CAST(dt AS DATE) <= CAST(? AS DATE)"
                        params.append(asof_date)
                    else:
                        asof_ymd = int(datetime.fromtimestamp(asof_dt, tz=timezone.utc).strftime("%Y%m%d"))
                        query += " AND dt <= CASE WHEN dt >= 1000000000 THEN ? ELSE ? END"
                        params.extend([asof_dt, asof_ymd])
                query += " ORDER BY dt DESC LIMIT ?"
                params.append(max(resolved_limit * 2, resolved_limit))
                sell_rows_desc = conn.execute(query, params).fetchall()

        if not ml_rows_desc and not sell_rows_desc:
            return []

        timeline_by_key: Dict[int, Dict[str, Any]] = {}

        def _ensure_point(dt_key: int) -> Dict[str, Any]:
            point = timeline_by_key.get(dt_key)
            if point is not None:
                return point
            payload: Dict[str, Any] = {
                "dt": dt_key,
                "pUp": None,
                "pDown": None,
                "pTurnUp": None,
                "pTurnDown": None,
                "ev20Net": None,
                "sellPDown": None,
                "sellPTurnDown": None,
                "trendDown": None,
                "trendDownStrict": None,
                "shortRet5": None,
                "shortRet10": None,
                "shortRet20": None,
                "shortWin5": None,
                "shortWin10": None,
                "shortWin20": None,
            }
            timeline_by_key[dt_key] = payload
            return payload

        for row in reversed(ml_rows_desc):
            if not row:
                continue
            dt_key = self._normalize_dt_key(row[0])
            if dt_key is None:
                continue
            point = _ensure_point(dt_key)
            p_up = _to_float_or_none(row[1] if len(row) > 1 else None)
            p_down = _to_float_or_none(row[2] if len(row) > 2 else None)
            if p_down is None and p_up is not None:
                p_down = 1.0 - p_up
            point["pUp"] = p_up
            point["pDown"] = p_down
            point["pTurnUp"] = _to_float_or_none(row[3] if len(row) > 3 else None)
            point["pTurnDown"] = _to_float_or_none(row[4] if len(row) > 4 else None)
            point["ev20Net"] = _to_float_or_none(row[5] if len(row) > 5 else None)

        for row in reversed(sell_rows_desc):
            if not row:
                continue
            dt_key = self._normalize_dt_key(row[0])
            if dt_key is None:
                continue
            point = _ensure_point(dt_key)
            point["sellPDown"] = _to_float_or_none(row[1] if len(row) > 1 else None)
            point["sellPTurnDown"] = _to_float_or_none(row[2] if len(row) > 2 else None)
            point["trendDown"] = bool(row[3]) if len(row) > 3 and row[3] is not None else None
            point["trendDownStrict"] = bool(row[4]) if len(row) > 4 and row[4] is not None else None
            point["shortRet5"] = _to_float_or_none(row[5] if len(row) > 5 else None)
            point["shortRet10"] = _to_float_or_none(row[6] if len(row) > 6 else None)
            point["shortRet20"] = _to_float_or_none(row[7] if len(row) > 7 else None)
            point["shortWin5"] = bool(row[8]) if len(row) > 8 and row[8] is not None else None
            point["shortWin10"] = bool(row[9]) if len(row) > 9 and row[9] is not None else None
            point["shortWin20"] = bool(row[10]) if len(row) > 10 and row[10] is not None else None

        keys = sorted(timeline_by_key.keys())
        if len(keys) > resolved_limit:
            keys = keys[-resolved_limit:]
        return [timeline_by_key[key] for key in keys]

    def get_buy_stage_precision(
        self,
        code: str,
        asof_dt: int | None,
        *,
        lookback_bars: int = 360,
        horizon: int = 20,
    ) -> Dict[str, Any] | None:
        if self._legacy_analysis_reads_disabled():
            return None
        horizon = max(1, int(horizon))
        lookback_bars = max(60, int(lookback_bars))
        limit_bars = max(lookback_bars + horizon + 120, 240)
        with self._get_read_conn() as conn:
            if not self._table_exists(conn, "ml_pred_20d"):
                return None
            if not self._table_exists(conn, "daily_bars"):
                return None
            pred_names = self._get_schema_columns(conn, "ml_pred_20d")
            daily_bar_names = self._get_schema_columns(conn, "daily_bars")
            if not {"code", "dt", "p_up"}.issubset(pred_names):
                return None
            if not {"code", "date", "c"}.issubset(daily_bar_names):
                return None

            dt_type = self._column_type(conn, "ml_pred_20d", "dt")
            has_p_turn_up = "p_turn_up" in pred_names
            has_p_turn_down = "p_turn_down" in pred_names

            pred_query = f"""
                SELECT
                    dt,
                    p_up,
                    {"p_turn_up" if has_p_turn_up else "NULL::DOUBLE AS p_turn_up"},
                    {"p_turn_down" if has_p_turn_down else "NULL::DOUBLE AS p_turn_down"}
                FROM ml_pred_20d
                WHERE code = ?
            """
            pred_params: List[Any] = [code]
            if asof_dt is not None:
                normalized_type = str(dt_type or "").upper()
                if any(
                    token in normalized_type
                    for token in ("INT", "DECIMAL", "NUMERIC", "DOUBLE", "REAL", "FLOAT")
                ):
                    asof_ymd = int(datetime.fromtimestamp(asof_dt, tz=timezone.utc).strftime("%Y%m%d"))
                    pred_query += " AND dt <= CASE WHEN dt >= 1000000000 THEN ? ELSE ? END"
                    pred_params.extend([asof_dt, asof_ymd])
                elif normalized_type:
                    asof_date = datetime.fromtimestamp(asof_dt, tz=timezone.utc).strftime("%Y-%m-%d")
                    pred_query += " AND CAST(dt AS DATE) <= CAST(? AS DATE)"
                    pred_params.append(asof_date)
                else:
                    asof_ymd = int(datetime.fromtimestamp(asof_dt, tz=timezone.utc).strftime("%Y%m%d"))
                    pred_query += " AND dt <= CASE WHEN dt >= 1000000000 THEN ? ELSE ? END"
                    pred_params.extend([asof_dt, asof_ymd])
            pred_query += " ORDER BY dt DESC LIMIT ?"
            pred_params.append(limit_bars)
            pred_rows_desc = conn.execute(pred_query, pred_params).fetchall()

            daily_query = """
                SELECT date, c
                FROM daily_bars
                WHERE code = ?
            """
            daily_params: List[Any] = [code]
            if asof_dt is not None:
                asof_ymd = int(datetime.fromtimestamp(asof_dt, tz=timezone.utc).strftime("%Y%m%d"))
                daily_query += " AND date <= CASE WHEN date >= 1000000000 THEN ? ELSE ? END"
                daily_params.extend([asof_dt, asof_ymd])
            daily_query += " ORDER BY date DESC LIMIT ?"
            daily_params.append(max(limit_bars + 240, 900))
            daily_rows_desc = conn.execute(daily_query, daily_params).fetchall()

        if not pred_rows_desc or not daily_rows_desc:
            return None

        daily_close_by_key: Dict[int, float] = {}
        for row in reversed(daily_rows_desc):
            if len(row) < 2:
                continue
            dt_key = self._normalize_dt_key(row[0])
            if dt_key is None:
                continue
            close_val = row[1]
            if not isinstance(close_val, (int, float)):
                continue
            close_f = float(close_val)
            if not math.isfinite(close_f) or close_f <= 0:
                continue
            daily_close_by_key[dt_key] = close_f

        if len(daily_close_by_key) <= horizon:
            return None

        daily_keys = sorted(daily_close_by_key.keys())
        daily_closes = [float(daily_close_by_key[key]) for key in daily_keys]
        daily_index_by_key = {key: idx for idx, key in enumerate(daily_keys)}

        stage_stats: Dict[str, Dict[str, int]] = {
            "probe": {"samples": 0, "wins": 0},
            "add": {"samples": 0, "wins": 0},
            "core": {"samples": 0, "wins": 0},
        }

        pred_points: List[Dict[str, Any]] = []
        for row in reversed(pred_rows_desc):
            if len(row) < 2:
                continue
            dt_key = self._normalize_dt_key(row[0])
            if dt_key is None:
                continue
            daily_idx = daily_index_by_key.get(dt_key)
            if daily_idx is None:
                continue
            p_up_raw = row[1]
            if not isinstance(p_up_raw, (int, float)):
                continue
            p_up = float(p_up_raw)
            if not math.isfinite(p_up):
                continue
            p_up = min(1.0, max(0.0, p_up))
            p_down = 1.0 - p_up

            spread = abs(p_up - p_down)
            turn_up_raw = row[2] if len(row) > 2 else None
            turn_down_raw = row[3] if len(row) > 3 else None
            turn_up = (
                float(turn_up_raw)
                if isinstance(turn_up_raw, (int, float)) and math.isfinite(float(turn_up_raw))
                else None
            )
            turn_down = (
                float(turn_down_raw)
                if isinstance(turn_down_raw, (int, float)) and math.isfinite(float(turn_down_raw))
                else None
            )

            is_probe = p_up > p_down
            is_add = p_up >= 0.54 and spread >= 0.06
            turn_ok = True if (turn_up is None or turn_down is None) else (turn_up >= turn_down)
            is_core = p_up >= 0.58 and spread >= 0.10 and turn_ok

            pred_points.append(
                {
                    "dt_key": dt_key,
                    "daily_idx": daily_idx,
                    "probe": is_probe,
                    "add": is_add,
                    "core": is_core,
                }
            )

        if not pred_points:
            return None

        pred_by_key = {int(point["dt_key"]): point for point in pred_points}

        for point in pred_points:
            idx = int(point["daily_idx"])
            future_idx = idx + horizon
            if future_idx >= len(daily_keys):
                continue
            entry_close = daily_closes[idx]
            future_close = daily_closes[future_idx]
            is_win = future_close > entry_close

            if bool(point["probe"]):
                stage_stats["probe"]["samples"] += 1
                if is_win:
                    stage_stats["probe"]["wins"] += 1
            if bool(point["add"]):
                stage_stats["add"]["samples"] += 1
                if is_win:
                    stage_stats["add"]["wins"] += 1
            if bool(point["core"]):
                stage_stats["core"]["samples"] += 1
                if is_win:
                    stage_stats["core"]["wins"] += 1

        probe_shares = 100
        add_shares = 300
        core_shares = 500
        topup_shares = 100
        target_shares = probe_shares + add_shares + core_shares + topup_shares
        take_profit_pct = 0.06

        strategy_samples = 0
        strategy_wins = 0
        for point in pred_points:
            start_idx = int(point["daily_idx"])
            end_idx = start_idx + horizon
            if end_idx >= len(daily_keys):
                continue

            units = 0
            total_cost = 0.0
            probe_filled = False
            add_filled = False
            core_filled = False
            topup_filled = False
            take_profit_price = None
            hit_take_profit = False

            for day_idx in range(start_idx, end_idx + 1):
                day_key = daily_keys[day_idx]
                day_close = daily_closes[day_idx]
                signal = pred_by_key.get(day_key)

                if signal is not None:
                    if (not probe_filled) and bool(signal["probe"]):
                        units += probe_shares
                        total_cost += day_close * float(probe_shares)
                        probe_filled = True
                    if probe_filled and (not add_filled) and bool(signal["add"]):
                        units += add_shares
                        total_cost += day_close * float(add_shares)
                        add_filled = True
                    if probe_filled and add_filled and (not core_filled) and bool(signal["core"]):
                        units += core_shares
                        total_cost += day_close * float(core_shares)
                        core_filled = True
                    if core_filled and (not topup_filled) and bool(signal["probe"]):
                        units += topup_shares
                        total_cost += day_close * float(topup_shares)
                        topup_filled = True

                    if units >= target_shares and take_profit_price is None and total_cost > 0:
                        avg_price = total_cost / float(units)
                        if math.isfinite(avg_price) and avg_price > 0:
                            take_profit_price = avg_price * (1.0 + take_profit_pct)

                if take_profit_price is not None and day_close >= take_profit_price:
                    hit_take_profit = True
                    break

            if take_profit_price is None:
                continue

            strategy_samples += 1
            if hit_take_profit:
                strategy_wins += 1

        def _build_stage_payload(stage_key: str) -> Dict[str, Any]:
            stats = stage_stats[stage_key]
            samples = int(stats["samples"])
            wins = int(min(stats["wins"], samples))
            precision = (float(wins) / float(samples)) if samples > 0 else None
            return {
                "precision": precision,
                "samples": samples,
                "wins": wins,
            }

        strategy_wins = min(strategy_wins, strategy_samples)
        strategy_precision = (
            float(strategy_wins) / float(strategy_samples)
            if strategy_samples > 0
            else None
        )

        return {
            "horizon": int(horizon),
            "lookbackBars": int(lookback_bars),
            "probe": _build_stage_payload("probe"),
            "add": _build_stage_payload("add"),
            "core": _build_stage_payload("core"),
            "strategy": {
                "precision": strategy_precision,
                "samples": int(strategy_samples),
                "wins": int(strategy_wins),
                "probeShares": int(probe_shares),
                "addShares": int(add_shares),
                "coreShares": int(core_shares),
                "topupShares": int(topup_shares),
                "targetShares": int(target_shares),
                "takeProfitPct": float(take_profit_pct),
            },
        }

    def get_sell_analysis_snapshot(self, code: str, asof_dt: int | None) -> Optional[Tuple]:
        if self._legacy_analysis_reads_disabled():
            return None
        with self._get_read_conn() as conn:
            if not self._table_exists(conn, "sell_analysis_daily"):
                return None
            if not self._column_exists(conn, "sell_analysis_daily", "code"):
                return None
            if not self._column_exists(conn, "sell_analysis_daily", "dt"):
                return None
            has_fwd_close_5 = self._column_exists(conn, "sell_analysis_daily", "fwd_close_5")
            has_fwd_close_10 = self._column_exists(conn, "sell_analysis_daily", "fwd_close_10")
            has_fwd_close_20 = self._column_exists(conn, "sell_analysis_daily", "fwd_close_20")
            has_short_ret_5 = self._column_exists(conn, "sell_analysis_daily", "short_ret_5")
            has_short_ret_10 = self._column_exists(conn, "sell_analysis_daily", "short_ret_10")
            has_short_ret_20 = self._column_exists(conn, "sell_analysis_daily", "short_ret_20")
            has_short_win_5 = self._column_exists(conn, "sell_analysis_daily", "short_win_5")
            has_short_win_10 = self._column_exists(conn, "sell_analysis_daily", "short_win_10")
            has_short_win_20 = self._column_exists(conn, "sell_analysis_daily", "short_win_20")
            dt_type = self._column_type(conn, "sell_analysis_daily", "dt")

            query = f"""
                SELECT
                    dt,
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
                    {"fwd_close_5" if has_fwd_close_5 else "NULL::DOUBLE AS fwd_close_5"},
                    {"fwd_close_10" if has_fwd_close_10 else "NULL::DOUBLE AS fwd_close_10"},
                    {"fwd_close_20" if has_fwd_close_20 else "NULL::DOUBLE AS fwd_close_20"},
                    {"short_ret_5" if has_short_ret_5 else "NULL::DOUBLE AS short_ret_5"},
                    {"short_ret_10" if has_short_ret_10 else "NULL::DOUBLE AS short_ret_10"},
                    {"short_ret_20" if has_short_ret_20 else "NULL::DOUBLE AS short_ret_20"},
                    {"short_win_5" if has_short_win_5 else "NULL::BOOLEAN AS short_win_5"},
                    {"short_win_10" if has_short_win_10 else "NULL::BOOLEAN AS short_win_10"},
                    {"short_win_20" if has_short_win_20 else "NULL::BOOLEAN AS short_win_20"}
                FROM sell_analysis_daily
                WHERE code = ?
            """
            params: List[Any] = [code]
            if asof_dt is not None:
                normalized_type = str(dt_type or "").upper()
                if any(
                    token in normalized_type
                    for token in ("INT", "DECIMAL", "NUMERIC", "DOUBLE", "REAL", "FLOAT")
                ):
                    asof_ymd = int(datetime.fromtimestamp(asof_dt, tz=timezone.utc).strftime("%Y%m%d"))
                    query += " AND dt <= CASE WHEN dt >= 1000000000 THEN ? ELSE ? END"
                    params.extend([asof_dt, asof_ymd])
                elif normalized_type:
                    asof_date = datetime.fromtimestamp(asof_dt, tz=timezone.utc).strftime("%Y-%m-%d")
                    query += " AND CAST(dt AS DATE) <= CAST(? AS DATE)"
                    params.append(asof_date)
                else:
                    asof_ymd = int(datetime.fromtimestamp(asof_dt, tz=timezone.utc).strftime("%Y%m%d"))
                    query += " AND dt <= CASE WHEN dt >= 1000000000 THEN ? ELSE ? END"
                    params.extend([asof_dt, asof_ymd])
            query += " ORDER BY dt DESC LIMIT 1"
            row = conn.execute(query, params).fetchone()
        return row

    def get_latest_ml_pred_map(self, codes: List[str]) -> Dict[str, Dict[str, Any]]:
        if self._legacy_analysis_reads_disabled():
            return {}
        unique_codes = sorted({str(code).strip() for code in codes if str(code).strip()})
        if not unique_codes:
            return {}

        def _to_float_or_none(value: Any) -> float | None:
            if not isinstance(value, (int, float)):
                return None
            fv = float(value)
            return fv if math.isfinite(fv) else None

        def _first_finite(*values: Any) -> float | None:
            for value in values:
                fv = _to_float_or_none(value)
                if fv is not None:
                    return fv
            return None

        with self._get_read_conn() as conn:
            if not self._table_exists(conn, "ml_pred_20d"):
                return {}
            names = self._get_schema_columns(conn, "ml_pred_20d")
            if not {"code", "dt"}.issubset(names):
                return {}

            p_up_expr = "p_up" if "p_up" in names else "NULL::DOUBLE AS p_up"
            p_up_5_expr = "p_up_5" if "p_up_5" in names else "NULL::DOUBLE AS p_up_5"
            p_up_10_expr = "p_up_10" if "p_up_10" in names else "NULL::DOUBLE AS p_up_10"
            p_down_expr = "p_down" if "p_down" in names else "NULL::DOUBLE AS p_down"
            p_turn_down_expr = (
                "p_turn_down" if "p_turn_down" in names else "NULL::DOUBLE AS p_turn_down"
            )
            p_turn_down_5_expr = (
                "p_turn_down_5" if "p_turn_down_5" in names else "NULL::DOUBLE AS p_turn_down_5"
            )
            p_turn_down_10_expr = (
                "p_turn_down_10" if "p_turn_down_10" in names else "NULL::DOUBLE AS p_turn_down_10"
            )
            p_turn_down_20_expr = (
                "p_turn_down_20" if "p_turn_down_20" in names else "NULL::DOUBLE AS p_turn_down_20"
            )
            ev20_net_expr = "ev20_net" if "ev20_net" in names else "NULL::DOUBLE AS ev20_net"
            ev5_net_expr = "ev5_net" if "ev5_net" in names else "NULL::DOUBLE AS ev5_net"
            ev10_net_expr = "ev10_net" if "ev10_net" in names else "NULL::DOUBLE AS ev10_net"
            model_version_expr = (
                "model_version" if "model_version" in names else "NULL::VARCHAR AS model_version"
            )

            placeholders = ",".join(["?"] * len(unique_codes))
            pred_dt_row = conn.execute(
                f"""
                SELECT MAX(dt)
                FROM ml_pred_20d
                WHERE code IN ({placeholders})
                """,
                unique_codes,
            ).fetchone()
            if not pred_dt_row or pred_dt_row[0] is None:
                return {}
            pred_dt = pred_dt_row[0]

            rows = conn.execute(
                f"""
                SELECT
                    code,
                    {p_up_expr},
                    {p_up_5_expr},
                    {p_up_10_expr},
                    {p_down_expr},
                    {p_turn_down_expr},
                    {p_turn_down_5_expr},
                    {p_turn_down_10_expr},
                    {p_turn_down_20_expr},
                    {ev20_net_expr},
                    {ev5_net_expr},
                    {ev10_net_expr},
                    {model_version_expr}
                FROM ml_pred_20d
                WHERE dt = ? AND code IN ({placeholders})
                """,
                [pred_dt, *unique_codes],
            ).fetchall()

        out: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            code = str(row[0])
            p_up = _to_float_or_none(row[1])
            p_up_5 = _to_float_or_none(row[2])
            p_up_10 = _to_float_or_none(row[3])
            p_down = _to_float_or_none(row[4])
            p_turn_down = _to_float_or_none(row[5])
            p_turn_down_5 = _to_float_or_none(row[6])
            p_turn_down_10 = _to_float_or_none(row[7])
            p_turn_down_20 = _to_float_or_none(row[8])
            ev20_net = _to_float_or_none(row[9])
            ev5_net = _to_float_or_none(row[10])
            ev10_net = _to_float_or_none(row[11])

            p_up_short = _first_finite(p_up_5, p_up_10, p_up)
            p_down_short = _first_finite(
                p_down,
                (1.0 - p_up_short) if p_up_short is not None else None,
            )
            p_turn_down_short = _first_finite(
                p_turn_down_5,
                p_turn_down_10,
                p_turn_down_20,
                p_turn_down,
                p_down_short,
            )
            ev_short_net = _first_finite(ev5_net, ev10_net, ev20_net)

            out[code] = {
                "p_up": p_up,
                "p_up_5": p_up_5,
                "p_up_10": p_up_10,
                "p_up_short": p_up_short,
                "p_down": p_down,
                "p_down_short": p_down_short,
                "p_turn_down": p_turn_down,
                "p_turn_down_5": p_turn_down_5,
                "p_turn_down_10": p_turn_down_10,
                "p_turn_down_20": p_turn_down_20,
                "p_turn_down_short": p_turn_down_short,
                "ev20_net": ev20_net,
                "ev5_net": ev5_net,
                "ev10_net": ev10_net,
                "ev_short_net": ev_short_net,
                "model_version": row[12],
            }
        return out
