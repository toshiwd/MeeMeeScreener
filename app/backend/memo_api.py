
# Daily Memo API endpoints
@app.get("/api/memo")
def get_daily_memo(symbol: str, date: str, timeframe: str = "D"):
    """Get memo for a specific symbol and date"""
    normalized_symbol = _normalize_code(symbol)
    if not normalized_symbol:
        return JSONResponse(status_code=400, content={"error": "invalid_symbol"})
    
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT memo, updated_at
            FROM daily_memos
            WHERE symbol = ? AND date = ? AND timeframe = ?
            """,
            [normalized_symbol, date, timeframe]
        ).fetchone()
        
        if not row:
            return JSONResponse(content={"memo": "", "updated_at": None})
        
        memo, updated_at = row
        return JSONResponse(content={
            "memo": memo or "",
            "updated_at": updated_at.isoformat() if updated_at else None
        })


@app.get("/api/memo/list")
def list_daily_memo(symbol: str, timeframe: str = "D"):
    """List memos for a symbol/timeframe"""
    normalized_symbol = _normalize_code(symbol)
    if not normalized_symbol:
        return JSONResponse(status_code=400, content={"error": "invalid_symbol"})

    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT date, memo
            FROM daily_memos
            WHERE symbol = ? AND timeframe = ?
            ORDER BY date
            """,
            [normalized_symbol, timeframe]
        ).fetchall()

    items = [{"date": row[0], "memo": row[1] or ""} for row in rows]
    return JSONResponse(content={"items": items})


@app.put("/api/memo")
def save_daily_memo(payload: dict = Body(...)):
    """Save or update memo for a specific symbol and date"""
    symbol = _normalize_code(payload.get("symbol"))
    date = payload.get("date")
    timeframe = payload.get("timeframe", "D")
    memo = payload.get("memo", "").strip()
    
    if not symbol or not date:
        return JSONResponse(status_code=400, content={"error": "symbol_and_date_required"})
    
    # Validate memo length (max 100 characters)
    if len(memo) > 100:
        return JSONResponse(status_code=400, content={"error": "memo_too_long", "max_length": 100})
    
    now = jst_now().replace(tzinfo=None)
    
    with get_conn() as conn:
        if not memo:
            # Delete if memo is empty
            conn.execute(
                """
                DELETE FROM daily_memos
                WHERE symbol = ? AND date = ? AND timeframe = ?
                """,
                [symbol, date, timeframe]
            )
            return JSONResponse(content={"ok": True, "deleted": True, "updated_at": None})
        else:
            # Upsert memo
            conn.execute(
                """
                INSERT INTO daily_memos (symbol, date, timeframe, memo, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (symbol, date, timeframe) DO UPDATE SET
                    memo = excluded.memo,
                    updated_at = excluded.updated_at
                """,
                [symbol, date, timeframe, memo, now, now]
            )
            return JSONResponse(content={
                "ok": True,
                "updated_at": now.isoformat()
            })


@app.delete("/api/memo")
def delete_daily_memo(symbol: str, date: str, timeframe: str = "D"):
    """Delete memo for a specific symbol and date"""
    normalized_symbol = _normalize_code(symbol)
    if not normalized_symbol:
        return JSONResponse(status_code=400, content={"error": "invalid_symbol"})
    
    with get_conn() as conn:
        cursor = conn.execute(
            """
                DELETE FROM daily_memos
            WHERE symbol = ? AND date = ? AND timeframe = ?
            """,
            [normalized_symbol, date, timeframe]
        )
        deleted = cursor.rowcount > 0
        
    return JSONResponse(content={"ok": True, "deleted": deleted})

