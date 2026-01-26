from __future__ import annotations

import threading
import traceback

from fastapi import APIRouter, Body, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.core.config import DEBUG
from app.db.schema import _get_favorites_conn
from app.db.session import get_conn
from app.services.screener_engine import _build_monthly_ranking, _build_weekly_ranking, _get_screener_rows
from app.services.box_detector import detect_boxes
from app.utils.date_utils import _format_event_timestamp, jst_now
from app.utils.text_utils import _normalize_code

router = APIRouter()

_similarity_import_error: str | None = None
try:
    from app.backend.similarity import SimilarityService, SearchResult
except Exception as exc:  # pragma: no cover - packaged runtime may miss heavy deps (e.g. numpy)
    SimilarityService = None  # type: ignore[assignment]
    _similarity_import_error = str(exc)

    class SearchResult(BaseModel):
        ticker: str
        asof: str  # YYYY-MM-DD
        score_total: float
        score60: float
        score24: float
        tag_id: str
        tags: dict
        vec60: list[float] | None = None
        vec24: list[float] | None = None


_similarity_service = SimilarityService() if SimilarityService is not None else None
_similarity_refresh_lock = threading.Lock()
_similarity_refresh_status = {
    "running": False,
    "mode": None,
    "started_at": None,
    "finished_at": None,
    "error": None,
    "import_error": _similarity_import_error,
}


def _classify_exception(exc: Exception) -> tuple[int, str, str]:
    detail = str(exc)
    lower = detail.lower()
    if "io error" in lower or "failed to open" in lower or "cannot open" in lower:
        return 503, "DB_OPEN_FAILED", "Database open failed"
    if "no such table" in lower or "does not exist" in lower or "catalog error" in lower or "table with name" in lower:
        return 503, "DATA_NOT_INITIALIZED", "Data not initialized"
    return 500, "UNHANDLED_EXCEPTION", "Internal server error"


def _build_error_payload(exc: Exception, trace_id: str) -> dict:
    _, error_code, message = _classify_exception(exc)
    payload = {"trace_id": trace_id, "error_code": error_code, "message": message, "detail": str(exc)}
    if DEBUG:
        payload["stack"] = traceback.format_exc()
    return payload


@router.get("/api/search/similar", response_model=list[SearchResult])
def search_similar(ticker: str, asof: str, k: int = 50, alpha: float = 0.5):
    if _similarity_service is None:
        raise HTTPException(status_code=503, detail="類似検索が利用できません (SimilarityService unavailable)")
    try:
        return _similarity_service.search(ticker, asof, k, alpha)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Search processing failed: {exc}")


@router.post("/api/search/similar/refresh")
def refresh_similarity(payload: dict = Body(default={})):
    if _similarity_service is None:
        return JSONResponse(status_code=503, content={"ok": False, "error": "similarity_unavailable"})
    mode = (payload.get("mode") or "full").strip().lower()
    if mode not in ("full", "incremental"):
        mode = "full"
    with _similarity_refresh_lock:
        if _similarity_refresh_status.get("running"):
            return JSONResponse(status_code=409, content={"ok": False, "error": "already_running"})
        _similarity_refresh_status.update(
            {"running": True, "mode": mode, "started_at": jst_now().isoformat(), "finished_at": None, "error": None}
        )
    try:
        _similarity_service.refresh_data(incremental=(mode == "incremental"))
        _similarity_refresh_status.update({"finished_at": jst_now().isoformat()})
        return {"ok": True, "status": _similarity_refresh_status}
    except Exception as exc:
        _similarity_refresh_status.update({"error": str(exc), "finished_at": jst_now().isoformat()})
        return JSONResponse(status_code=500, content={"ok": False, "error": str(exc), "status": _similarity_refresh_status})
    finally:
        _similarity_refresh_status["running"] = False


@router.get("/api/search/similar/status")
def similarity_refresh_status():
    if _similarity_service is None:
        return JSONResponse(status_code=503, content={"ok": False, "error": "similarity_unavailable"})
    return {"ok": True, "status": _similarity_refresh_status}


@router.get("/api/memo")
def get_daily_memo(symbol: str, date: str, timeframe: str = "D"):
    normalized_symbol = _normalize_code(symbol)
    if not normalized_symbol:
        return JSONResponse(status_code=400, content={"error": "invalid_symbol"})

    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT memo, updated_at
            FROM daily_memo
            WHERE symbol = ? AND date = ? AND timeframe = ?
            """,
            [normalized_symbol, date, timeframe],
        ).fetchone()

        if not row:
            return JSONResponse(content={"memo": "", "updated_at": None})

        memo, updated_at = row
        return JSONResponse(content={"memo": memo or "", "updated_at": updated_at.isoformat() if updated_at else None})


@router.get("/api/memo/list")
def list_daily_memo(symbol: str, timeframe: str = "D"):
    normalized_symbol = _normalize_code(symbol)
    if not normalized_symbol:
        return JSONResponse(status_code=400, content={"error": "invalid_symbol"})

    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT date, memo
            FROM daily_memo
            WHERE symbol = ? AND timeframe = ?
            ORDER BY date
            """,
            [normalized_symbol, timeframe],
        ).fetchall()

    items = [{"date": row[0], "memo": row[1] or ""} for row in rows]
    return JSONResponse(content={"items": items})


@router.put("/api/memo")
def save_daily_memo(payload: dict = Body(...)):
    symbol = _normalize_code(payload.get("symbol"))
    date = payload.get("date")
    timeframe = payload.get("timeframe", "D")
    memo = payload.get("memo", "").strip()

    if not symbol or not date:
        return JSONResponse(status_code=400, content={"error": "symbol_and_date_required"})

    if len(memo) > 100:
        return JSONResponse(status_code=400, content={"error": "memo_too_long", "max_length": 100})

    now = jst_now().replace(tzinfo=None)

    with get_conn() as conn:
        if not memo:
            conn.execute(
                """
                DELETE FROM daily_memo
                WHERE symbol = ? AND date = ? AND timeframe = ?
                """,
                [symbol, date, timeframe],
            )
            return JSONResponse(content={"ok": True, "deleted": True, "updated_at": None})
        conn.execute(
            """
            INSERT INTO daily_memo (symbol, date, timeframe, memo, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (symbol, date, timeframe) DO UPDATE SET
                memo = excluded.memo,
                updated_at = excluded.updated_at
            """,
            [symbol, date, timeframe, memo, now, now],
        )
        return JSONResponse(content={"ok": True, "updated_at": now.isoformat()})


@router.delete("/api/memo")
def delete_daily_memo(symbol: str, date: str, timeframe: str = "D"):
    normalized_symbol = _normalize_code(symbol)
    if not normalized_symbol:
        return JSONResponse(status_code=400, content={"error": "invalid_symbol"})

    with get_conn() as conn:
        cursor = conn.execute(
            """
            DELETE FROM daily_memo
            WHERE symbol = ? AND date = ? AND timeframe = ?
            """,
            [normalized_symbol, date, timeframe],
        )
        deleted = cursor.rowcount > 0

    return JSONResponse(content={"ok": True, "deleted": deleted})


def _load_favorite_items() -> list[dict]:
    with _get_favorites_conn() as conn:
        rows = conn.execute(
            """
            SELECT code, created_at
            FROM favorites
            ORDER BY created_at DESC
            """
        ).fetchall()
    return [{"code": row["code"], "created_at": _format_event_timestamp(row["created_at"])} for row in rows]


def _load_favorite_codes() -> list[str]:
    with _get_favorites_conn() as conn:
        rows = conn.execute("SELECT code FROM favorites").fetchall()
    return [row[0] for row in rows if row and row[0]]


@router.get("/favorites")
@router.get("/api/favorites")
def favorites_list():
    try:
        items = _load_favorite_items()
        return JSONResponse(content={"items": items, "errors": []})
    except Exception as exc:
        return JSONResponse(content={"items": [], "errors": [f"favorites_failed:{exc}"]})


@router.post("/favorites/{code}")
@router.post("/api/favorites/{code}")
def favorites_add(code: str):
    normalized = _normalize_code(code)
    if not normalized:
        return JSONResponse(status_code=400, content={"ok": False, "error": "invalid_code"})
    try:
        with _get_favorites_conn() as conn:
            conn.execute("INSERT OR IGNORE INTO favorites (code) VALUES (?)", (normalized,))
        return JSONResponse(content={"ok": True, "code": normalized})
    except Exception as exc:
        return JSONResponse(status_code=200, content={"ok": False, "error": f"favorite_add_failed:{exc}"})


@router.delete("/favorites/{code}")
@router.delete("/api/favorites/{code}")
def favorites_remove(code: str):
    normalized = _normalize_code(code)
    if not normalized:
        return JSONResponse(status_code=400, content={"ok": False, "error": "invalid_code"})
    try:
        with _get_favorites_conn() as conn:
            conn.execute("DELETE FROM favorites WHERE code = ?", (normalized,))
        return JSONResponse(content={"ok": True, "code": normalized})
    except Exception as exc:
        return JSONResponse(status_code=200, content={"ok": False, "error": f"favorite_remove_failed:{exc}"})


@router.get("/rank/weekly")
def rank_weekly(as_of: str | None = None, limit: int = 50, universe: str | None = None):
    try:
        limit_value = max(1, min(200, int(limit)))
    except (TypeError, ValueError):
        limit_value = 50
    try:
        result = _build_weekly_ranking(as_of, limit_value, universe)
        return JSONResponse(content=result)
    except Exception as exc:
        return JSONResponse(content={"up": [], "down": [], "meta": {"as_of": as_of, "count": 0, "errors": [f"rank_weekly_failed:{exc}"]}})


@router.get("/rank/monthly")
def rank_monthly(as_of: str | None = None, limit: int = 50, universe: str | None = None):
    try:
        limit_value = max(1, min(200, int(limit)))
    except (TypeError, ValueError):
        limit_value = 50
    try:
        result = _build_monthly_ranking(as_of, limit_value, universe)
        return JSONResponse(content=result)
    except Exception as exc:
        return JSONResponse(content={"box": [], "meta": {"as_of": as_of, "count": 0, "errors": [f"rank_monthly_failed:{exc}"]}})


@router.get("/rank")
@router.get("/api/rank")
def rank(dir: str = "up", as_of: str | None = None, limit: int = 50, universe: str | None = None):
    try:
        limit_value = max(1, min(200, int(limit)))
    except (TypeError, ValueError):
        limit_value = 50
    direction = (dir or "up").lower()
    if direction not in ("up", "down"):
        direction = "up"
    try:
        result = _build_weekly_ranking(as_of, limit_value, universe)
        favorites = set(_load_favorite_codes())
        items = []
        for item in result.get(direction, []):
            code = item.get("code")
            items.append({**item, "is_favorite": bool(code and code in favorites)})
        return JSONResponse(
            content={
                "items": items,
                "meta": {
                    "as_of": result.get("meta", {}).get("as_of"),
                    "count": len(items),
                    "dir": direction,
                    "universe": result.get("meta", {}).get("universe"),
                },
                "errors": [],
            }
        )
    except Exception as exc:
        return JSONResponse(
            content={
                "items": [],
                "meta": {"as_of": as_of, "count": 0, "dir": direction, "universe": universe},
                "errors": [f"rank_failed:{exc}"],
            }
        )


@router.get("/api/screener")
def screener():
    try:
        rows = _get_screener_rows()
        return JSONResponse(content={"items": rows, "errors": []})
    except Exception as exc:
        return JSONResponse(content={"items": [], "errors": [f"screener_failed:{exc}"]})


@router.get("/api/list")
def list_tickers():
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT d.code,
                   COALESCE(m.name, d.code) AS name,
                   COALESCE(m.stage, 'UNKNOWN') AS stage,
                   m.score AS score,
                   COALESCE(m.reason, 'TXT_ONLY') AS reason,
                   p.spot_qty,
                   p.margin_long_qty,
                   p.margin_short_qty,
                   p.has_issue,
                   p.issue_note
            FROM (SELECT DISTINCT code FROM daily_bars) d
            LEFT JOIN stock_meta m ON d.code = m.code
            LEFT JOIN positions_live p ON d.code = p.symbol
            ORDER BY d.code
            """
        ).fetchall()
    return JSONResponse(content=rows)


@router.post("/api/batch_bars")
def batch_bars(payload: dict = Body(default={})):  # { timeframe, codes, limit }
    timeframe = payload.get("timeframe", "monthly")
    codes = payload.get("codes", [])
    limit = min(int(payload.get("limit", 60)), 2000)

    if not codes:
        return JSONResponse(content={"timeframe": timeframe, "limit": limit, "items": {}})

    if timeframe == "daily":
        bars_table = "daily_bars"
        ma_table = "daily_ma"
        time_col = "date"
    else:
        bars_table = "monthly_bars"
        ma_table = "monthly_ma"
        time_col = "month"

    placeholders = ",".join(["?"] * len(codes))
    query = f"""
        WITH base AS (
            SELECT b.code,
                   b.{time_col} AS t,
                   b.o,
                   b.h,
                   b.l,
                   b.c,
                   b.v,
                   m.ma7,
                   m.ma20,
                   m.ma60,
                   ROW_NUMBER() OVER (PARTITION BY b.code ORDER BY b.{time_col} DESC) AS rn
            FROM {bars_table} b
            LEFT JOIN {ma_table} m
              ON b.code = m.code AND b.{time_col} = m.{time_col}
            WHERE b.code IN ({placeholders})
        )
        SELECT code, t, o, h, l, c, v, ma7, ma20, ma60
        FROM base
        WHERE rn <= ?
        ORDER BY code, t
    """

    with get_conn() as conn:
        rows = conn.execute(query, codes + [limit]).fetchall()
        monthly_rows = conn.execute(
            f"""
            SELECT code, month, o, h, l, c, v
            FROM monthly_bars
            WHERE code IN ({placeholders})
            ORDER BY code, month
            """,
            codes,
        ).fetchall()

    monthly_by_code: dict[str, list[tuple]] = {}
    for code, month, o, h, l, c, v in monthly_rows:
        monthly_by_code.setdefault(code, []).append((month, o, h, l, c, v))

    boxes_by_code = {code: detect_boxes(monthly_by_code.get(code, [])) for code in codes}

    items: dict[str, dict[str, list]] = {
        code: {"bars": [], "ma": {"ma7": [], "ma20": [], "ma60": []}, "boxes": boxes_by_code.get(code, [])}
        for code in codes
    }
    for code, t, o, h, l, c, v, ma7, ma20, ma60 in rows:
        payload = items.setdefault(
            code, {"bars": [], "ma": {"ma7": [], "ma20": [], "ma60": []}, "boxes": boxes_by_code.get(code, [])}
        )
        payload["bars"].append([t, o, h, l, c, v])
        payload["ma"]["ma7"].append([t, ma7])
        payload["ma"]["ma20"].append([t, ma20])
        payload["ma"]["ma60"].append([t, ma60])

    return JSONResponse(content={"timeframe": timeframe, "limit": limit, "items": items})


@router.get("/api/ticker/daily")
def daily(code: str, limit: int = 400):
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

    with get_conn() as conn:
        try:
            rows = conn.execute(query_with_ma, [code, limit]).fetchall()
        except Exception:
            rows = conn.execute(query_basic, [code, limit]).fetchall()
    return JSONResponse(content={"data": rows, "errors": []})


@router.get("/api/ticker/monthly")
def monthly(code: str, limit: int = 240):
    try:
        with get_conn() as conn:
            rows = conn.execute(
                """
                WITH base AS (
                    SELECT
                        month,
                        o,
                        h,
                        l,
                        c
                    FROM monthly_bars
                    WHERE code = ?
                    ORDER BY month DESC
                    LIMIT ?
                )
                SELECT month, o, h, l, c
                FROM base
                ORDER BY month
                """,
                [code, limit],
            ).fetchall()
        return JSONResponse(content={"data": rows, "errors": []})
    except Exception as exc:
        return JSONResponse(content={"data": [], "errors": [f"ticker_monthly_failed:{exc}"]})


@router.get("/api/ticker/boxes")
def ticker_boxes(code: str):
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT month, o, h, l, c
            FROM monthly_bars
            WHERE code = ?
            ORDER BY month
            """,
            [code],
        ).fetchall()
    return JSONResponse(content=detect_boxes(rows))
