import json
from datetime import datetime
import duckdb
from app.utils.date_utils import jst_now


def _json_array(value: object) -> list:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    try:
        parsed = json.loads(str(value))
    except Exception:
        return []
    return parsed if isinstance(parsed, list) else []


class MemoRepository:
    @staticmethod
    def get_memo(conn: duckdb.DuckDBPyConnection, symbol: str, date: str, timeframe: str = "D") -> dict | None:
        row = conn.execute(
            """
            SELECT note_text, tags_json, linked_objects_json, no_lookahead, created_at, updated_at, title, paragraphs_json
            FROM chart_notes
            WHERE code = ? AND as_of_date = ? AND timeframe = ?
            """,
            [symbol, date, timeframe]
        ).fetchone()
        if row:
            updated_at = row[5]
            return {
                "schema_version": "chart_note_v1",
                "code": symbol,
                "as_of_date": date,
                "timeframe": timeframe,
                "memo": row[0] or "",
                "note_text": row[0] or "",
                "tags": _json_array(row[1]),
                "linked_objects": _json_array(row[2]),
                "no_lookahead": bool(row[3]),
                "created_at": row[4].isoformat() if row[4] else None,
                "updated_at": updated_at.isoformat() if updated_at else None,
                "title": row[6] or "",
                "paragraphs": _json_array(row[7]),
            }

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
            "note_text": row[0] or "",
            "tags": [],
            "linked_objects": [],
            "no_lookahead": True,
            "updated_at": row[1].isoformat() if row[1] else None
        }

    @staticmethod
    def upsert_memo(
        conn: duckdb.DuckDBPyConnection,
        symbol: str,
        date: str,
        timeframe: str,
        memo: str,
        *,
        tags: list | None = None,
        linked_objects: list | None = None,
        no_lookahead: bool = True,
    ) -> dict:
        now = jst_now().replace(tzinfo=None) # DuckDB usually handles naive/UTC
        tags_json = json.dumps(tags or [], ensure_ascii=False, separators=(",", ":"))
        linked_objects_json = json.dumps(linked_objects or [], ensure_ascii=False, separators=(",", ":"))
        
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

        chart_note_exists = conn.execute(
            "SELECT 1 FROM chart_notes WHERE code = ? AND as_of_date = ? AND timeframe = ?",
            [symbol, date, timeframe],
        ).fetchone()
        if chart_note_exists:
            conn.execute(
                """
                UPDATE chart_notes
                SET note_text = ?,
                    paragraphs_json = ?,
                    tags_json = ?,
                    linked_objects_json = ?,
                    no_lookahead = ?,
                    updated_at = ?
                WHERE code = ? AND as_of_date = ? AND timeframe = ?
                """,
                [memo, "[]", tags_json, linked_objects_json, bool(no_lookahead), now, symbol, date, timeframe],
            )
        else:
            conn.execute(
                """
                INSERT INTO chart_notes (
                    code,
                    as_of_date,
                    timeframe,
                    title,
                    note_text,
                    paragraphs_json,
                    tags_json,
                    linked_objects_json,
                    no_lookahead,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [symbol, date, timeframe, None, memo, "[]", tags_json, linked_objects_json, bool(no_lookahead), now, now],
            )
            
        return {
            "ok": True,
            "schema_version": "chart_note_v1",
            "updated_at": now.isoformat()
        }
