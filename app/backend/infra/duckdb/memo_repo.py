from datetime import datetime
import duckdb
from app.utils.date_utils import jst_now

class MemoRepository:
    @staticmethod
    def get_memo(conn: duckdb.DuckDBPyConnection, symbol: str, date: str, timeframe: str = "D") -> dict | None:
        row = conn.execute(
            """
            SELECT memo, updated_at
            FROM daily_memos
            WHERE symbol = ? AND date = ? AND timeframe = ?
            """,
            [symbol, date, timeframe]
        ).fetchone()

        if not row:
            return None

        return {
            "memo": row[0],
            "updated_at": row[1].isoformat() if row[1] else None
        }

    @staticmethod
    def upsert_memo(conn: duckdb.DuckDBPyConnection, symbol: str, date: str, timeframe: str, memo: str) -> dict:
        now = jst_now().replace(tzinfo=None) # DuckDB usually handles naive/UTC
        
        # Check if exists
        exists = conn.execute(
            "SELECT 1 FROM daily_memos WHERE symbol = ? AND date = ? AND timeframe = ?",
            [symbol, date, timeframe]
        ).fetchone()

        if exists:
            conn.execute(
                """
                UPDATE daily_memos
                SET memo = ?, updated_at = ?
                WHERE symbol = ? AND date = ? AND timeframe = ?
                """,
                [memo, now, symbol, date, timeframe]
            )
        else:
            conn.execute(
                """
                INSERT INTO daily_memos (symbol, date, timeframe, memo, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                [symbol, date, timeframe, memo, now]
            )
            
        return {
            "ok": True,
            "updated_at": now.isoformat()
        }
