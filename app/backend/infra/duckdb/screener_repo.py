from __future__ import annotations
from datetime import date
from typing import List, Tuple, Dict, Any, Optional

import duckdb

class ScreenerRepository:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def _get_conn(self):
        # Use the default (read/write) config to match other connections.
        return duckdb.connect(self.db_path)

    def fetch_screener_batch(
        self,
        daily_limit: int,
        earnings_start: date,
        earnings_end: date,
        rights_min_date: date
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
            meta_rows = conn.execute(
                "SELECT code, name, stage, score, reason, score_status, missing_reasons_json, score_breakdown_json FROM stock_meta"
            ).fetchall()

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

            # 4. Get Monthly Bars (All? Or limit? screener_engine gets all)
            monthly_rows = conn.execute(
                """
                SELECT code, month, o, h, l, c
                FROM monthly_bars
                ORDER BY code, month
                """
            ).fetchall()

            # 5. Earnings
            earnings_rows = conn.execute(
                """
                SELECT code, MIN(planned_date) AS planned_date
                FROM earnings_planned
                WHERE planned_date BETWEEN ? AND ?
                GROUP BY code
                """,
                [earnings_start, earnings_end]
            ).fetchall()

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
        with self.get_conn() as conn:
            placeholders = ",".join(["?"] * len(codes))
            rows = conn.execute(
                f"SELECT code, name FROM stock_meta WHERE code IN ({placeholders})",
                codes
            ).fetchall()
            return {row[0]: row[1] for row in rows}

    def fetch_all_codes(self) -> List[str]:
        with self.get_conn() as conn:
            rows = conn.execute(
                "SELECT DISTINCT code FROM daily_bars ORDER BY code"
            ).fetchall()
            return [row[0] for row in rows]
