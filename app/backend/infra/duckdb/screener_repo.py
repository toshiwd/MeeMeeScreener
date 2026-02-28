from __future__ import annotations
from datetime import date, timedelta
from typing import List, Tuple, Dict, Any, Optional

import pandas as pd

import duckdb
from app.backend.core.text_encoding import repair_cp932_mojibake

class ScreenerRepository:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def _get_conn(self):
        # Use the default (read/write) config to match other connections.
        return duckdb.connect(self.db_path)

    def _table_exists(self, conn: duckdb.DuckDBPyConnection, name: str) -> bool:
        row = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [name],
        ).fetchone()
        return bool(row and row[0])

    def fetch_screener_batch(
        self,
        daily_limit: int,
        earnings_start: date,
        earnings_end: date,
        rights_min_date: date,
        monthly_limit: int = 120,
    ) -> Tuple[List[str], List[Tuple], List[Tuple], List[Tuple], List[Tuple], List[Tuple]]:
        """
        Fetch all necessary data for screener generation in one go (or efficient batching).
        Returns:
            (codes, meta_rows, daily_rows, monthly_rows, earnings_rows, rights_rows)
        """
        with self._get_conn() as conn:
            # 1. Get all codes
            codes_rows = conn.execute("SELECT DISTINCT code FROM daily_bars ORDER BY code").fetchall()
            codes = [row[0] for row in codes_rows]

            # 2. Get Meta
            meta_rows_raw = conn.execute(
                "SELECT code, name, stage, score, reason, score_status, missing_reasons_json, score_breakdown_json FROM stock_meta"
            ).fetchall()
            meta_rows = [
                (
                    row[0],
                    repair_cp932_mojibake(str(row[1] or row[0])),
                    row[2],
                    row[3],
                    row[4],
                    row[5],
                    row[6],
                    row[7],
                )
                for row in meta_rows_raw
            ]

            # 3. Get Daily Bars (Windowed)
            daily_rows = conn.execute(
                """
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
                )
                WHERE rn <= ?
                ORDER BY code, date
                """,
                [daily_limit]
            ).fetchall()

            # 4. Get Monthly Bars (Windowed)
            monthly_rows = conn.execute(
                """
                SELECT code, month, o, h, l, c
                FROM (
                    SELECT
                        code,
                        month,
                        o,
                        h,
                        l,
                        c,
                        ROW_NUMBER() OVER (PARTITION BY code ORDER BY month DESC) AS rn
                    FROM monthly_bars
                )
                WHERE rn <= ?
                ORDER BY code, month
                """,
                [monthly_limit],
            ).fetchall()

            # 5. Earnings (prefer upcoming; fallback to recent past)
            past_start = earnings_start - timedelta(days=180)
            future_rows = conn.execute(
                """
                SELECT code, MIN(planned_date) AS planned_date
                FROM earnings_planned
                WHERE planned_date BETWEEN ? AND ?
                GROUP BY code
                """,
                [earnings_start, earnings_end],
            ).fetchall()
            past_rows = conn.execute(
                """
                SELECT code, MAX(planned_date) AS planned_date
                FROM earnings_planned
                WHERE planned_date BETWEEN ? AND ?
                GROUP BY code
                """,
                [past_start, earnings_start],
            ).fetchall()
            future_map = {row[0]: row[1] for row in future_rows}
            past_map = {row[0]: row[1] for row in past_rows}
            earnings_rows = []
            for code in set(future_map.keys()) | set(past_map.keys()):
                planned_date = future_map.get(code) or past_map.get(code)
                if planned_date:
                    earnings_rows.append((code, planned_date))

            # 6. Rights
            rights_rows = conn.execute(
                """
                SELECT code, MIN(COALESCE(last_rights_date, ex_date)) AS rights_date
                FROM ex_rights
                WHERE COALESCE(last_rights_date, ex_date) >= ?
                GROUP BY code
                """,
                [rights_min_date]
            ).fetchall()

            return codes, meta_rows, daily_rows, monthly_rows, earnings_rows, rights_rows

    def fetch_sector_map(
        self, codes: List[str]
    ) -> Dict[str, Tuple[Optional[str], Optional[str], Optional[str]]]:
        if not codes:
            return {}
        with self._get_conn() as conn:
            if not self._table_exists(conn, "industry_master"):
                return {}
            placeholders = ",".join(["?"] * len(codes))
            rows = conn.execute(
                f"""
                SELECT code, name, sector33_code, sector33_name
                FROM industry_master
                WHERE code IN ({placeholders})
                """,
                codes,
            ).fetchall()
            return {
                row[0]: (
                    repair_cp932_mojibake(str(row[1] or row[0])),
                    row[2],
                    repair_cp932_mojibake(str(row[3] or "")) if row[3] is not None else None,
                )
                for row in rows
            }

    def fetch_phase_pred_map(self, asof_map: Dict[str, int | None]) -> Dict[str, Dict[str, Any]]:
        with self._get_conn() as conn:
            if not self._table_exists(conn, "phase_pred_daily"):
                return {}

            rows = []
            with_asof = [(code, asof) for code, asof in asof_map.items() if asof is not None]
            if with_asof:
                df = pd.DataFrame(with_asof, columns=["code", "asof"])
                conn.register("phase_asof_map", df)
                rows.extend(
                    conn.execute(
                        """
                        SELECT code, dt, early_score, late_score, body_score, n
                        FROM (
                            SELECT
                                p.code,
                                p.dt,
                                p.early_score,
                                p.late_score,
                                p.body_score,
                                p.n,
                                ROW_NUMBER() OVER (PARTITION BY p.code ORDER BY p.dt DESC) AS rn
                            FROM phase_pred_daily p
                            JOIN phase_asof_map a ON p.code = a.code
                            WHERE p.dt <= a.asof
                        )
                        WHERE rn = 1
                        """
                    ).fetchall()
                )

            without_asof = [code for code, asof in asof_map.items() if asof is None]
            if without_asof:
                placeholders = ",".join(["?"] * len(without_asof))
                rows.extend(
                    conn.execute(
                        f"""
                        SELECT code, dt, early_score, late_score, body_score, n
                        FROM (
                            SELECT
                                code,
                                dt,
                                early_score,
                                late_score,
                                body_score,
                                n,
                                ROW_NUMBER() OVER (PARTITION BY code ORDER BY dt DESC) AS rn
                            FROM phase_pred_daily
                            WHERE code IN ({placeholders})
                        )
                        WHERE rn = 1
                        """,
                        without_asof,
                    ).fetchall()
                )

            return {
                row[0]: {
                    "dt": row[1],
                    "early_score": row[2],
                    "late_score": row[3],
                    "body_score": row[4],
                    "n": row[5],
                }
                for row in rows
            }

    def fetch_daily_rows_for_codes(self, codes: List[str], as_of: int | None, limit: int) -> Dict[str, List[Tuple]]:
        if not codes:
            return {}
        
        with self._get_conn() as conn:
            placeholders = ",".join(["?"] * len(codes))
            where_clauses = [f"code IN ({placeholders})"]
            params: list = list(codes)
            if as_of is not None:
                where_clauses.append("date <= ?")
                params.append(as_of)
            where_sql = " AND ".join(where_clauses)

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
                    WHERE {where_sql}
                )
                WHERE rn <= ?
                ORDER BY code, date
            """
            params.append(limit)
            rows = conn.execute(query, params).fetchall()
            
            grouped: Dict[str, List[Tuple]] = {}
            for row in rows:
                code = row[0]
                grouped.setdefault(code, []).append(row[1:])
            return grouped

    def fetch_monthly_rows_for_codes(self, codes: List[str], as_of_month: int | None, limit: int) -> Dict[str, List[Tuple]]:
        if not codes:
            return {}
        
        with self._get_conn() as conn:
            placeholders = ",".join(["?"] * len(codes))
            where_clauses = [f"code IN ({placeholders})"]
            params: list = list(codes)
            if as_of_month is not None:
                where_clauses.append("month <= ?")
                params.append(as_of_month)
            where_sql = " AND ".join(where_clauses)

            query = f"""
                SELECT code, month, o, h, l, c
                FROM (
                    SELECT
                        code,
                        month,
                        o,
                        h,
                        l,
                        c,
                        ROW_NUMBER() OVER (PARTITION BY code ORDER BY month DESC) AS rn
                    FROM monthly_bars
                    WHERE {where_sql}
                )
                WHERE rn <= ?
                ORDER BY code, month
            """
            params.append(limit)
            rows = conn.execute(query, params).fetchall()

            grouped: Dict[str, List[Tuple]] = {}
            for row in rows:
                code = row[0]
                grouped.setdefault(code, []).append(row[1:])
            return grouped

    def fetch_meta_map(self, codes: List[str]) -> Dict[str, str]:
        if not codes:
            return {}
        with self._get_conn() as conn:
            placeholders = ",".join(["?"] * len(codes))
            rows = conn.execute(
                f"SELECT code, name FROM stock_meta WHERE code IN ({placeholders})",
                codes
            ).fetchall()
            return {row[0]: repair_cp932_mojibake(str(row[1] or row[0])) for row in rows}

    def fetch_all_codes(self) -> List[str]:
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT DISTINCT code FROM daily_bars ORDER BY code"
            ).fetchall()
            return [row[0] for row in rows]
