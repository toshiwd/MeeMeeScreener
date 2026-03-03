from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException
from fastapi.responses import JSONResponse

from app.backend.infra.duckdb.memo_repo import MemoRepository
from app.db.session import is_transient_duckdb_error, try_get_conn
from app.utils.text_utils import _normalize_code

router = APIRouter()


def _normalize_symbol(value: str) -> str:
    normalized = _normalize_code(value)
    if not normalized:
        raise HTTPException(status_code=400, detail="invalid_symbol")
    return normalized


def _db_retryable_response(*, error_detail: str | None = None) -> JSONResponse:
    payload: dict[str, object] = {
        "error": "db_unavailable",
        "retryable": True,
        "message": "Database is temporarily unavailable",
    }
    if error_detail:
        payload["error_detail"] = error_detail
    return JSONResponse(status_code=503, content=payload, headers={"Retry-After": "1"})


@router.get("/api/memo")
def get_daily_memo(symbol: str, date: str, timeframe: str = "D"):
    normalized_symbol = _normalize_symbol(symbol)
    try:
        with try_get_conn(timeout_sec=0.4) as conn:
            if conn is None:
                return _db_retryable_response()
            memo = MemoRepository.get_memo(conn, normalized_symbol, date, timeframe)
    except Exception as exc:
        if is_transient_duckdb_error(exc):
            return _db_retryable_response(error_detail=str(exc))
        return JSONResponse(status_code=500, content={"error": str(exc)})
    if not memo:
        return {"memo": "", "updated_at": None}
    return memo


@router.get("/api/memo/list")
def list_daily_memo(symbol: str, timeframe: str = "D"):
    normalized_symbol = _normalize_symbol(symbol)
    try:
        with try_get_conn(timeout_sec=0.4) as conn:
            if conn is None:
                return _db_retryable_response()
            rows = conn.execute(
                """
                SELECT date, memo
                FROM daily_memos
                WHERE symbol = ? AND timeframe = ?
                ORDER BY date
                """,
                [normalized_symbol, timeframe],
            ).fetchall()
    except Exception as exc:
        if is_transient_duckdb_error(exc):
            return _db_retryable_response(error_detail=str(exc))
        return JSONResponse(status_code=500, content={"error": str(exc)})

    items = [{"date": row[0], "memo": row[1] or ""} for row in rows]
    return {"items": items}


@router.put("/api/memo")
def save_daily_memo(payload: dict = Body(...)):
    symbol = _normalize_code(payload.get("symbol"))
    date = payload.get("date")
    timeframe = payload.get("timeframe", "D")
    memo = (payload.get("memo") or "").strip()

    if not symbol or not date:
        return JSONResponse(status_code=400, content={"error": "symbol_and_date_required"})

    if len(memo) > 100:
        return JSONResponse(status_code=400, content={"error": "memo_too_long", "max_length": 100})

    try:
        with try_get_conn(timeout_sec=0.4) as conn:
            if conn is None:
                return _db_retryable_response()
            if not memo:
                conn.execute(
                    """
                    DELETE FROM daily_memos
                    WHERE symbol = ? AND date = ? AND timeframe = ?
                    """,
                    [symbol, date, timeframe],
                )
                return {"ok": True, "deleted": True, "updated_at": None}

            return MemoRepository.upsert_memo(conn, symbol, date, timeframe, memo)
    except Exception as exc:
        if is_transient_duckdb_error(exc):
            return _db_retryable_response(error_detail=str(exc))
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.delete("/api/memo")
def delete_daily_memo(symbol: str, date: str, timeframe: str = "D"):
    normalized_symbol = _normalize_symbol(symbol)
    try:
        with try_get_conn(timeout_sec=0.4) as conn:
            if conn is None:
                return _db_retryable_response()
            cursor = conn.execute(
                """
                DELETE FROM daily_memos
                WHERE symbol = ? AND date = ? AND timeframe = ?
                """,
                [normalized_symbol, date, timeframe],
            )
            deleted = cursor.rowcount > 0
    except Exception as exc:
        if is_transient_duckdb_error(exc):
            return _db_retryable_response(error_detail=str(exc))
        return JSONResponse(status_code=500, content={"error": str(exc)})
    return {"ok": True, "deleted": deleted}
