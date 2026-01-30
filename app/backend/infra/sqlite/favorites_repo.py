import sqlite3
import os
from typing import List, Tuple

class FavoritesRepository:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._ensure_table()

    def _get_conn(self):
        return sqlite3.connect(self.db_path)

    def _ensure_table(self):
        with self._get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS favorites (
                    code TEXT PRIMARY KEY,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

    def get_all(self) -> List[Tuple[str, str]]:
        """Returns list of (code, created_at)"""
        with self._get_conn() as conn:
            return conn.execute("SELECT code, created_at FROM favorites ORDER BY created_at DESC").fetchall()

    def add(self, code: str):
        with self._get_conn() as conn:
            conn.execute("INSERT OR IGNORE INTO favorites (code) VALUES (?)", (code,))

    def remove(self, code: str):
        with self._get_conn() as conn:
            conn.execute("DELETE FROM favorites WHERE code = ?", (code,))
