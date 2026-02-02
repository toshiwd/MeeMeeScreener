from __future__ import annotations

import os
import traceback
from typing import List, Optional

from fastapi import APIRouter, Depends, UploadFile, File, Form, HTTPException, Query, Body
from fastapi.responses import JSONResponse

from app.services import (
    position_calc,
    positions_ops,
    trade_events,
    trade_ingest
)
from app.db.session import get_conn
from app.utils.date_utils import _format_event_timestamp, jst_now
from app.utils.text_utils import _normalize_code
from app.backend.infra.files.trade_repo import TradeRepository

# Re-export or re-implement helpers if needed, or import from services
# The legacy router imported _calc_* from position_calc.

router = APIRouter()

@router.get("/api/trades/{code}")
def trades_by_code(code: str):
    try:
        daily_positions = position_calc._get_daily_positions_db([code])
        daily_for_code = daily_positions.get(code, [])

        with get_conn() as conn:
            db_events = trade_events.get_events(conn, [code])

            row = conn.execute(
                "SELECT spot_qty, margin_long_qty, margin_short_qty, buy_qty, sell_qty, has_issue, issue_note FROM positions_live WHERE symbol = ?",
                [code],
            ).fetchone()

            if row:
                current_position = {
                    "spotLots": row[0],
                    "marginLongLots": row[1],
                    "marginShortLots": row[2],
                    "longLots": row[3],
                    "shortLots": row[4],
                    "hasIssue": row[5],
                    "issueNote": row[6],
                }
            else:
                current_position = None

        events_payload = []
        for ev in db_events:
            events_payload.append(position_calc._map_trade_event(ev))
            
        current_positions = position_calc._calc_current_positions_by_broker(events_payload)
        current_metrics = position_calc._calc_position_metrics(events_payload)

        return JSONResponse(
            content={
                "events": events_payload,
                "dailyPositions": daily_for_code,
                "currentPosition": {"longLots": current_metrics["longLots"], "shortLots": current_metrics["shortLots"]},
                "currentPositions": current_positions,
                "warnings": {"items": []},
                "errors": [],
            }
        )
    except Exception as exc:
        return JSONResponse(
            content={
                "events": [],
                "dailyPositions": [],
                "currentPosition": None,
                "warnings": {"items": []},
                "errors": [f"trades_by_code_failed:{exc}"],
            }
        )

@router.get("/api/trades")
def trades(code: str | None = None):
    try:
        code_list = [code] if code else None
        daily_positions_map = position_calc._get_daily_positions_db(code_list)

        with get_conn() as conn:
            db_events = trade_events.get_events(conn, code_list)

        events_payload = []
        for ev in db_events:
            events_payload.append(position_calc._map_trade_event(ev))

        all_daily = []
        for d_list in daily_positions_map.values():
            all_daily.extend(d_list)

        return JSONResponse(
            content={
                "events": events_payload,
                "dailyPositions": all_daily,
                "currentPosition": None,
                "warnings": {"items": []},
                "errors": [],
            }
        )
    except Exception as exc:
        return JSONResponse(
            content={
                "events": [],
                "dailyPositions": [],
                "currentPosition": None,
                "warnings": {"items": []},
                "errors": [f"trades_failed:{exc}"],
            }
        )

@router.get("/api/positions/current")
def positions_current():
    with get_conn() as conn:
        live_rows = conn.execute(
            """
            SELECT symbol, buy_qty, sell_qty, opened_at, has_issue, issue_note
            FROM positions_live
            """
        ).fetchall()
        traded_rows = conn.execute(
            """
            SELECT DISTINCT symbol
            FROM trade_events
            """
        ).fetchall()

    def to_lots(value: float | None) -> float:
        if value is None:
            return 0.0
        try:
            return float(value) / 100.0
        except (TypeError, ValueError):
            return 0.0

    current_positions_by_code: dict[str, dict] = {}
    holding_codes: list[str] = []

    for symbol, buy_qty, sell_qty, opened_at, has_issue, issue_note in live_rows:
        buy_lots = to_lots(buy_qty)
        sell_lots = to_lots(sell_qty)
        current_positions_by_code[str(symbol)] = {
            "buyShares": buy_lots,
            "sellShares": sell_lots,
            "opened_at": _format_event_timestamp(opened_at),
            "has_issue": bool(has_issue),
            "issue_note": issue_note,
        }
        if buy_lots > 0 or sell_lots > 0:
            holding_codes.append(str(symbol))

    all_traded_codes = sorted({str(row[0]) for row in traded_rows if row and row[0]})

    return JSONResponse(
        content={
            "holding_codes": holding_codes,
            "current_positions_by_code": current_positions_by_code,
            "all_traded_codes": all_traded_codes,
        }
    )

@router.post("/api/positions/rebuild")
def positions_rebuild():
    """Force rebuild all positions from trade events"""
    try:
        with get_conn() as conn:
            summary = positions_ops.rebuild_positions(conn)

            holdings_count = conn.execute(
                """
                SELECT COUNT(*) FROM positions_live
                WHERE buy_qty > 0 OR sell_qty > 0
                """
            ).fetchone()[0]

        return JSONResponse(
            content={
                "success": True,
                "summary": summary,
                "holdings_count": holdings_count,
                "message": f"Positions rebuilt successfully. {holdings_count} symbols with holdings.",
            }
        )
    except Exception as e:
        return JSONResponse(status_code=500, content={"success": False, "error": str(e), "message": "Failed to rebuild positions"})

@router.post("/api/trade_csv/upload")
async def trade_csv_upload(file: UploadFile = File(...)):
    if not file.filename or not file.filename.lower().endswith(".csv"):
        return JSONResponse(status_code=400, content={"ok": False, "error": "csv_required"})
    
    # We don't necessarily need to save to disk if we ingest directly, but legacy did save.
    # We will use TradeRepository to save if we want compliance, or just use temp saving logic from legacy.
    # Legacy saved to `_canonical_trade_csv_path`.
    
    content = await file.read()
    
    # Detect Broker
    broker, detected_method = TradeRepository.detect_broker_from_bytes(content, file.filename)
    
    # Save to canonical path
    if broker in ("sbi", "rakuten"):
        path = TradeRepository.get_canonical_path(broker)
        TradeRepository.save_raw_content(path, content)
        saved_as = os.path.basename(path)
        dest_path = path
    else:
        # Unknown broker but stick to default?
        path = TradeRepository.get_canonical_path("rakuten") # Fallback?
        # Legacy did not save if unknown? No, legacy detect returned "unknown".
        # If unknown, we error?
        # Legacy: if broker == "sbi": ... else: ... (defaults to rakuten).
        # Let's assume Rakuten.
        path = TradeRepository.get_canonical_path("rakuten")
        TradeRepository.save_raw_content(path, content)
        saved_as = os.path.basename(path)
        dest_path = path

    try:
        if broker == "sbi":
            result = trade_ingest.process_import_sbi(content, replace_existing=True)
        else:
             # Default to Rakuten if unknown
            result = trade_ingest.process_import_rakuten(content, replace_existing=True)
            
        return JSONResponse(
            content={
                "ok": True,
                "broker": broker,
                "detected_by": detected_method,
                "original_filename": file.filename,
                "saved_as": saved_as,
                "path": dest_path,
                "ingest": result,
            }
        )
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"ingest_failed:{e}"})

@router.post("/api/imports/trade-history")
async def import_trade_history(
    file: UploadFile = File(...),
    broker: str = Form("auto"),
):
    try:
        raw_data = await file.read()
        original_filename = file.filename or ""
        
        detected_broker, detected_by = TradeRepository.detect_broker_from_bytes(raw_data, original_filename)
        
        selected = (broker or "auto").strip().lower()
        if selected not in ("auto", "rakuten", "sbi"):
             return JSONResponse(status_code=400, content={"error": f"Unknown broker: {broker}"})
             
        final_broker = detected_broker if selected == "auto" else selected
        mismatch = selected in ("rakuten", "sbi") and detected_broker != selected

        path = TradeRepository.get_canonical_path(final_broker)
        TradeRepository.save_raw_content(path, raw_data)

        if final_broker == "rakuten":
            result = trade_ingest.process_import_rakuten(raw_data, replace_existing=True)
        else:
            result = trade_ingest.process_import_sbi(raw_data, replace_existing=True)
            
        if result.get("received", 0) == 0:
            return JSONResponse(status_code=400, content={"error": "No events parsed", "warnings": result.get("warnings")})
            
        return JSONResponse(
            content={
                "result": "success",
                "broker": final_broker,
                "saved_as": os.path.basename(path),
                "path": path,
                "original_filename": original_filename,
                "detected_broker": detected_broker,
                "detected_by": detected_by,
                "detected_mismatch": mismatch,
                **result,
            }
        )

    except Exception as e:
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"error": str(e)})


@router.get("/api/debug/trade-sync")
def trade_sync_debug():
    try:
        from app.backend.core.csv_sync import sync_trade_csvs
        result = sync_trade_csvs()
        return JSONResponse(content={"ok": True, "errors": [], **result})
    except Exception as exc:
        return JSONResponse(status_code=500, content={"ok": False, "errors": [str(exc)]})

@router.post("/api/positions/seed")
def upsert_position_seed(payload: dict = Body(...)):
    symbol = _normalize_code(payload.get("symbol"))
    if not symbol:
        return JSONResponse(content={"error": "symbol_required"}, status_code=400)
    buy_qty = float(payload.get("buy_qty") or 0)
    sell_qty = float(payload.get("sell_qty") or 0)
    asof_dt = payload.get("asof_dt") or jst_now().replace(tzinfo=None).isoformat()
    memo = payload.get("memo")
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO initial_positions_seed (symbol, buy_qty, sell_qty, asof_dt, memo)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET
                buy_qty = excluded.buy_qty,
                sell_qty = excluded.sell_qty,
                asof_dt = excluded.asof_dt,
                memo = excluded.memo
            """,
            [symbol, buy_qty, sell_qty, asof_dt, memo],
        )
        rebuild_summary = positions_ops.rebuild_positions(conn)
    return JSONResponse(content={"symbol": symbol, "rebuild": rebuild_summary})

@router.get("/api/positions/held")
def get_held_positions():
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT symbol, buy_qty, sell_qty, opened_at, has_issue, issue_note
            FROM positions_live
            WHERE buy_qty > 0 OR sell_qty > 0
            ORDER BY opened_at DESC
            """
        ).fetchall()

        def to_lots(value: float | None) -> float:
            if value is None:
                return 0.0
            try:
                return float(value) / 100.0
            except (TypeError, ValueError):
                return 0.0

        result = []
        for r in rows:
            sym = r[0]
            name_row = conn.execute("SELECT name FROM tickers WHERE code = ?", [sym]).fetchone()
            name = name_row[0] if name_row else ""

            b_qty = to_lots(r[1])
            s_qty = to_lots(r[2])
            b_str = f"{int(b_qty)}" if b_qty.is_integer() else f"{b_qty}"
            s_str = f"{int(s_qty)}" if s_qty.is_integer() else f"{s_qty}"

            result.append(
                {
                    "symbol": sym,
                    "name": name,
                    "buy_qty": b_qty,
                    "sell_qty": s_qty,
                    "sell_buy_text": f"{s_str}-{b_str}",
                    "opened_at": r[3],
                    "has_issue": r[4],
                    "issue_note": r[5],
                }
            )

        return {"items": result}

@router.get("/api/positions/history")
def get_position_history(symbol: str | None = None):
    with get_conn() as conn:
        query = """
            SELECT round_id, symbol, opened_at, closed_at, closed_reason, last_state_sell_buy, has_issue, issue_note
            FROM position_rounds
        """
        params = []
        if symbol:
            query += " WHERE symbol = ?"
            params.append(symbol)

        query += " ORDER BY closed_at DESC"

        rows = conn.execute(query, params).fetchall()
        result = []
        for r in rows:
            sym = r[1]
            name_row = conn.execute("SELECT name FROM tickers WHERE code = ?", [sym]).fetchone()
            name = name_row[0] if name_row else ""

            result.append(
                {
                    "round_id": r[0],
                    "symbol": sym,
                    "name": name,
                    "opened_at": r[2],
                    "closed_at": r[3],
                    "closed_reason": r[4],
                    "last_state_sell_buy": r[5],
                    "has_issue": r[6],
                    "issue_note": r[7],
                }
            )
        return {"items": result}

@router.get("/api/positions/history/events")
def get_round_events(round_id: str):
    with get_conn() as conn:
        round_info = conn.execute("SELECT symbol, opened_at, closed_at FROM position_rounds WHERE round_id = ?", [round_id]).fetchone()

        if not round_info:
            return {"events": []}

        symbol = round_info[0]
        start_at = round_info[1]
        end_at = round_info[2]

        query = "SELECT broker, exec_dt, action, qty, price FROM trade_events WHERE symbol = ?"
        params = [symbol]

        if start_at:
            query += " AND exec_dt >= ?"
            params.append(start_at)
        if end_at:
            query += " AND exec_dt <= ?"
            params.append(end_at)

        query += " ORDER BY exec_dt ASC"

        rows = conn.execute(query, params).fetchall()
        events = []
        for r in rows:
            events.append({"broker": r[0], "exec_dt": r[1], "action": r[2], "qty": r[3], "price": r[4]})

        return {"events": events}
