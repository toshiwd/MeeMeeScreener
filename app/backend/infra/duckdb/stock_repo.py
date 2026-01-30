from __future__ import annotations
import duckdb
import os
from threading import Lock
from typing import List, Optional, Tuple, Any, Dict
import json

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

    def get_daily_bars(self, code: str, limit: int = 400) -> List[Tuple]:
        query = """
            SELECT date, o, h, l, c, v 
            FROM daily_bars 
            WHERE code = ? 
            ORDER BY date DESC 
            LIMIT ?
        """
        with self._get_conn() as conn:
            rows = conn.execute(query, [code, limit]).fetchall()
        # Return valid sort order (ASC)
        return sorted(rows, key=lambda x: x[0]) 

    def get_monthly_bars(self, code: str, limit: int = 120) -> List[Tuple]:
        query = """
            SELECT month, o, h, l, c, v
            FROM monthly_bars
            WHERE code = ?
            ORDER BY month DESC
            LIMIT ?
        """
        with self._get_conn() as conn:
             rows = conn.execute(query, [code, limit]).fetchall()
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
