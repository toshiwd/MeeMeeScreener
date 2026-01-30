from __future__ import annotations

import json
import calendar
from datetime import datetime

from fastapi import APIRouter, Body
from fastapi.responses import JSONResponse

from app.db.schema import _get_practice_conn
from app.db.session import get_conn

router = APIRouter()


def _parse_practice_date(value: int | str | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, int):
        raw = str(value).zfill(8)
        try:
            return datetime(int(raw[:4]), int(raw[4:6]), int(raw[6:8]))
        except (TypeError, ValueError):
            return None
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt)
        except (TypeError, ValueError):
            continue
    return None


def _format_practice_date(value: int | str | None) -> str | None:
    parsed = _parse_practice_date(value)
    if parsed is None:
        return None
    return parsed.date().isoformat()


def _resolve_practice_start_date(session_id: str | None, start_date: int | str | None) -> datetime | None:
    if start_date is not None:
        parsed = _parse_practice_date(start_date)
        if parsed is not None:
            return parsed
    if not session_id:
        return None
    with _get_practice_conn() as conn:
        row = conn.execute("SELECT start_date FROM practice_sessions WHERE session_id = ?", [session_id]).fetchone()
    if not row:
        return None
    return _parse_practice_date(row["start_date"])


@router.get("/api/practice/session")
def practice_session(session_id: str | None = None):
    if not session_id:
        return JSONResponse(content={"error": "session_id_required"}, status_code=400)
    with _get_practice_conn() as conn:
        row = conn.execute(
            """
            SELECT
                session_id,
                code,
                start_date,
                end_date,
                cursor_time,
                max_unlocked_time,
                lot_size,
                range_months,
                trades,
                notes,
                ui_state
            FROM practice_sessions
            WHERE session_id = ?
            """,
            [session_id],
        ).fetchone()
    if not row:
        return JSONResponse(content={"session": None})
    trades_raw = row["trades"] or "[]"
    try:
        trades = json.loads(trades_raw)
        if not isinstance(trades, list):
            trades = []
    except Exception:
        trades = []
    ui_state_raw = row["ui_state"] or "{}"
    try:
        ui_state = json.loads(ui_state_raw)
        if not isinstance(ui_state, dict):
            ui_state = {}
    except Exception:
        ui_state = {}
    session = {
        "session_id": row["session_id"],
        "code": row["code"],
        "start_date": row["start_date"],
        "end_date": row["end_date"],
        "cursor_time": row["cursor_time"],
        "max_unlocked_time": row["max_unlocked_time"],
        "lot_size": row["lot_size"],
        "range_months": row["range_months"],
        "trades": trades,
        "notes": row["notes"],
        "ui_state": ui_state,
    }
    return JSONResponse(content={"session": session})


@router.post("/api/practice/session")
def practice_session_upsert(payload: dict = Body(...)):
    session_id = payload.get("session_id")
    code = payload.get("code")
    if not session_id or not code:
        return JSONResponse(content={"error": "session_id_code_required"}, status_code=400)
    start_date = _format_practice_date(payload.get("start_date"))
    end_date = _format_practice_date(payload.get("end_date"))
    cursor_time = payload.get("cursor_time")
    max_unlocked_time = payload.get("max_unlocked_time")
    lot_size = payload.get("lot_size")
    range_months = payload.get("range_months")
    trades = payload.get("trades")
    if not isinstance(trades, list):
        trades = []
    notes = payload.get("notes")
    if notes is not None:
        notes = str(notes)
    ui_state = payload.get("ui_state")
    if ui_state is None:
        ui_state = {}
    if not isinstance(ui_state, dict):
        ui_state = {}

    trades_json = json.dumps(trades, ensure_ascii=False)
    ui_state_json = json.dumps(ui_state, ensure_ascii=False)

    with _get_practice_conn() as conn:
        conn.execute(
            """
            INSERT INTO practice_sessions (
                session_id,
                code,
                start_date,
                end_date,
                cursor_time,
                max_unlocked_time,
                lot_size,
                range_months,
                trades,
                notes,
                ui_state
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(session_id) DO UPDATE SET
                code = excluded.code,
                start_date = excluded.start_date,
                end_date = excluded.end_date,
                cursor_time = excluded.cursor_time,
                max_unlocked_time = excluded.max_unlocked_time,
                lot_size = excluded.lot_size,
                range_months = excluded.range_months,
                trades = excluded.trades,
                notes = excluded.notes,
                ui_state = excluded.ui_state,
                updated_at = CURRENT_TIMESTAMP
            """,
            [
                session_id,
                code,
                start_date,
                end_date,
                cursor_time,
                max_unlocked_time,
                lot_size,
                range_months,
                trades_json,
                notes,
                ui_state_json,
            ],
        )
    return JSONResponse(content={"session_id": session_id, "code": code, "start_date": start_date})


@router.delete("/api/practice/session")
def practice_session_delete(session_id: str | None = None):
    if not session_id:
        return JSONResponse(content={"error": "session_id_required"}, status_code=400)
    with _get_practice_conn() as conn:
        conn.execute("DELETE FROM practice_sessions WHERE session_id = ?", [session_id])
    return JSONResponse(content={"deleted": True})


@router.get("/api/practice/sessions")
def practice_sessions(code: str | None = None):
    query = """
        SELECT
            session_id,
            code,
            start_date,
            end_date,
            cursor_time,
            max_unlocked_time,
            lot_size,
            range_months,
            trades,
            notes,
            ui_state,
            created_at,
            updated_at
        FROM practice_sessions
        {where_clause}
        ORDER BY datetime(updated_at) DESC
    """
    params: list = []
    where_clause = ""
    if code:
        where_clause = "WHERE code = ?"
        params.append(code)
    with _get_practice_conn() as conn:
        rows = conn.execute(query.format(where_clause=where_clause), params).fetchall()
    sessions: list[dict] = []
    for row in rows:
        trades_raw = row["trades"] or "[]"
        try:
            trades = json.loads(trades_raw)
            if not isinstance(trades, list):
                trades = []
        except Exception:
            trades = []
        ui_state_raw = row["ui_state"] or "{}"
        try:
            ui_state = json.loads(ui_state_raw)
            if not isinstance(ui_state, dict):
                ui_state = {}
        except Exception:
            ui_state = {}
        sessions.append(
            {
                "session_id": row["session_id"],
                "code": row["code"],
                "start_date": row["start_date"],
                "end_date": row["end_date"],
                "cursor_time": row["cursor_time"],
                "max_unlocked_time": row["max_unlocked_time"],
                "lot_size": row["lot_size"],
                "range_months": row["range_months"],
                "trades": trades,
                "notes": row["notes"],
                "ui_state": ui_state,
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            }
        )
    return JSONResponse(content={"sessions": sessions})


@router.get("/api/practice/daily")
def practice_daily(code: str, limit: int = 400, session_id: str | None = None, start_date: str | None = None):
    query_with_ma = """
        WITH base AS (
            SELECT
                b.date,
                b.o,
                b.h,
                b.l,
                b.c,
                b.v,
                m.ma7,
                m.ma20,
                m.ma60
            FROM daily_bars b
            LEFT JOIN daily_ma m
              ON b.code = m.code AND b.date = m.date
            WHERE b.code = ?
            {date_filter}
            ORDER BY b.date
        ),
        tail AS (
            SELECT *
            FROM base
            ORDER BY date DESC
            LIMIT ?
        )
        SELECT date, o, h, l, c, v, ma7, ma20, ma60
        FROM tail
        ORDER BY date
    """
    query_basic = """
        WITH base AS (
            SELECT
                b.date,
                b.o,
                b.h,
                b.l,
                b.c,
                b.v
            FROM daily_bars b
            WHERE b.code = ?
            {date_filter}
            ORDER BY b.date
        ),
        tail AS (
            SELECT *
            FROM base
            ORDER BY date DESC
            LIMIT ?
        )
        SELECT date, o, h, l, c, v
        FROM tail
        ORDER BY date
    """

    resolved = _resolve_practice_start_date(session_id, start_date)
    date_filter = ""
    params: list = [code]
    if resolved:
        date_filter = "AND b.date >= ?"
        params.append(int(resolved.strftime("%Y%m%d")))
    params.append(limit)

    with get_conn() as conn:
        try:
            rows = conn.execute(query_with_ma.format(date_filter=date_filter), params).fetchall()
        except Exception:
            rows = conn.execute(query_basic.format(date_filter=date_filter), params).fetchall()

    return JSONResponse(content={"data": rows, "errors": []})


@router.get("/api/practice/monthly")
def practice_monthly(
    code: str,
    limit: int = 240,
    session_id: str | None = None,
    start_date: str | None = None,
):
    errors: list[str] = []
    parsed_start = _parse_practice_date(start_date) if start_date is not None else None
    resolved = _resolve_practice_start_date(session_id, start_date)
    if start_date is not None and parsed_start is None and resolved is None:
        errors.append("practice_start_date_invalid")
    month_filter = ""
    params: list = [code]
    month_value = None
    try:
        with get_conn() as conn:
            if resolved:
                max_month = conn.execute(
                    "SELECT MAX(month) FROM monthly_bars WHERE code = ?",
                    [code],
                ).fetchone()[0]
                use_epoch = max_month is not None and max_month >= 1_000_000_000
                if use_epoch:
                    month_value = int(calendar.timegm(resolved.replace(day=1).timetuple()))
                else:
                    month_value = resolved.year * 100 + resolved.month
                month_filter = "AND month <= ?"
                params.append(month_value)
            params.append(limit)
            rows = conn.execute(
                f"""
                WITH base AS (
                    SELECT
                        month,
                        o,
                        h,
                        l,
                        c
                    FROM monthly_bars
                    WHERE code = ?
                    {month_filter}
                    ORDER BY month DESC
                    LIMIT ?
                )
                SELECT month, o, h, l, c
                FROM base
                ORDER BY month
                """,
                params,
            ).fetchall()
        return JSONResponse(content={"data": rows, "errors": errors})
    except Exception as exc:
        errors.append(f"monthly_query_failed:{exc}")
        return JSONResponse(content={"data": [], "errors": errors})
