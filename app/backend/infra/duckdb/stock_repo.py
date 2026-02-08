from __future__ import annotations
import duckdb
import os
from threading import Lock
from typing import List, Optional, Tuple, Any, Dict
import json
from datetime import datetime, timezone

class StockRepository:
    _instance = None
    _lock = Lock()

    def __init__(self, db_path: str):
        self._db_path = db_path
        self._conn = None

    def _get_conn(self):
        # Use the default (read/write) config to match other connections.
        return duckdb.connect(self._db_path)

    def get_all_codes(self) -> List[str]:
        with self._get_conn() as conn:
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
        with self._get_conn() as conn:
            rows = conn.execute(query, params).fetchall()
        # Return valid sort order (ASC)
        return sorted(rows, key=lambda x: x[0])

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
        with self._get_conn() as conn:
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
        return sorted(rows, key=lambda x: x[0])

    def get_latest_params_for_screening(self, codes: Optional[List[str]] = None) -> List[Tuple]:
        # This replaces the complex query in screener.py (or supports it)
        # For now, simplistic implementation
        pass

    def ensure_score_table(self):
        with self._get_conn() as conn:
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

    def save_scores(self, scores: List[Dict[str, Any]]):
        self.ensure_score_table()
        # scores: list of dicts with code, score_a, score_b, reasons, badges
        with self._get_conn() as conn:
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
                INSERT OR REPLACE INTO stock_scores (code, score_a, score_b, reasons, badges, updated_at)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, data)

    def get_scores(self) -> Dict[str, Dict]:
        self.ensure_score_table()
        with self._get_conn() as conn:
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
        with self._get_conn() as conn:
            row = conn.execute(query, params).fetchone()
        return row

    def _table_exists(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
        row = conn.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = ? LIMIT 1",
            [table_name],
        ).fetchone()
        return row is not None

    def _column_exists(self, conn: duckdb.DuckDBPyConnection, table_name: str, column_name: str) -> bool:
        row = conn.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_name = ? AND column_name = ?
            LIMIT 1
            """,
            [table_name, column_name],
        ).fetchone()
        return row is not None

    def _column_type(
        self, conn: duckdb.DuckDBPyConnection, table_name: str, column_name: str
    ) -> str | None:
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
            return None
        value = row[0]
        return str(value) if value is not None else None

    def get_ml_analysis_pred(self, code: str, asof_dt: int | None) -> Optional[Tuple]:
        with self._get_conn() as conn:
            if not self._table_exists(conn, "ml_pred_20d"):
                return None
            if not self._column_exists(conn, "ml_pred_20d", "code"):
                return None
            if not self._column_exists(conn, "ml_pred_20d", "dt"):
                return None

            has_p_up = self._column_exists(conn, "ml_pred_20d", "p_up")
            has_p_turn_up = self._column_exists(conn, "ml_pred_20d", "p_turn_up")
            has_p_turn_down = self._column_exists(conn, "ml_pred_20d", "p_turn_down")
            has_ret_pred20 = self._column_exists(conn, "ml_pred_20d", "ret_pred20")
            has_ev20 = self._column_exists(conn, "ml_pred_20d", "ev20")
            has_ev20_net = self._column_exists(conn, "ml_pred_20d", "ev20_net")
            has_model_version = self._column_exists(conn, "ml_pred_20d", "model_version")
            dt_type = self._column_type(conn, "ml_pred_20d", "dt")

            select_parts = [
                "dt",
                "p_up" if has_p_up else "NULL::DOUBLE AS p_up",
                "p_turn_up" if has_p_turn_up else "NULL::DOUBLE AS p_turn_up",
                "p_turn_down" if has_p_turn_down else "NULL::DOUBLE AS p_turn_down",
                "ret_pred20" if has_ret_pred20 else "NULL::DOUBLE AS ret_pred20",
                "ev20" if has_ev20 else "NULL::DOUBLE AS ev20",
                "ev20_net" if has_ev20_net else "NULL::DOUBLE AS ev20_net",
                "model_version" if has_model_version else "NULL::VARCHAR AS model_version",
            ]
            query = f"""
                SELECT {", ".join(select_parts)}
                FROM ml_pred_20d
                WHERE code = ?
            """
            params: List[Any] = [code]
            if asof_dt is not None and dt_type:
                normalized_type = dt_type.upper()
                if any(
                    token in normalized_type
                    for token in ("INT", "DECIMAL", "NUMERIC", "DOUBLE", "REAL", "FLOAT")
                ):
                    asof_ymd = int(datetime.fromtimestamp(asof_dt, tz=timezone.utc).strftime("%Y%m%d"))
                    query += " AND dt <= CASE WHEN dt >= 1000000000 THEN ? ELSE ? END"
                    params.extend([asof_dt, asof_ymd])
                else:
                    asof_date = datetime.fromtimestamp(asof_dt, tz=timezone.utc).strftime("%Y-%m-%d")
                    query += " AND CAST(dt AS DATE) <= CAST(? AS DATE)"
                    params.append(asof_date)
            query += " ORDER BY dt DESC LIMIT 1"
            row = conn.execute(query, params).fetchone()
        return row
