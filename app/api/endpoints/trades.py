from __future__ import annotations

import os
import traceback

from fastapi import APIRouter, Body, File, Form, UploadFile
from fastapi.responses import JSONResponse

from app.core.config import resolve_trade_csv_dir
from app.core.config import canonical_trade_csv_path as _canonical_trade_csv_path
from app.db.session import get_conn
from app.services.position_calc import (
    _calc_current_positions_by_broker,
    _calc_position_metrics,
    _get_daily_positions_db,
    _map_trade_event,
)
from app.services.positions_ops import rebuild_positions
from app.services.trade_events import get_events
from app.services.trade_ingest import process_import_rakuten, process_import_sbi
from app.services.trade_importer import _detect_trade_broker
from app.utils.date_utils import _format_event_timestamp, jst_now
from app.utils.text_utils import _normalize_code

router = APIRouter()


@router.post("/api/trade_csv/upload")
async def trade_csv_upload(file: UploadFile = File(...)):
    if not file.filename or not file.filename.lower().endswith(".csv"):
        return JSONResponse(status_code=400, content={"ok": False, "error": "csv_required"})
    dest_dir = resolve_trade_csv_dir()
    os.makedirs(dest_dir, exist_ok=True)
    original_filename = os.path.basename(file.filename)

    file.file.seek(0)
    content = file.file.read()

    try:
        broker, detected_by = _detect_trade_broker(content, original_filename)
        dest_path = _canonical_trade_csv_path(broker)
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        with open(dest_path, "wb") as handle:
            handle.write(content)

        if broker == "sbi":
            result = process_import_sbi(content, replace_existing=True)
        else:
            result = process_import_rakuten(content, replace_existing=True)
        return JSONResponse(
            content={
                "ok": True,
                "broker": broker,
                "detected_by": detected_by,
                "original_filename": original_filename,
                "saved_as": os.path.basename(dest_path),
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
        detected_broker, detected_by = _detect_trade_broker(raw_data, original_filename)
        selected = (broker or "auto").strip().lower()
        if selected not in ("auto", "rakuten", "sbi"):
            return JSONResponse(status_code=400, content={"error": f"Unknown broker: {broker}"})
        final_broker = detected_broker if selected == "auto" else selected
        mismatch = selected in ("rakuten", "sbi") and detected_broker != selected

        dest_path = _canonical_trade_csv_path(final_broker)
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        with open(dest_path, "wb") as handle:
            handle.write(raw_data)

        if final_broker == "rakuten":
            result = process_import_rakuten(raw_data, replace_existing=True)
        else:
            result = process_import_sbi(raw_data, replace_existing=True)
        if result.get("received", 0) == 0:
            return JSONResponse(status_code=400, content={"error": "No events parsed", "warnings": result.get("warnings")})
        return JSONResponse(
            content={
                "result": "success",
                "broker": final_broker,
                "saved_as": os.path.basename(dest_path),
                "path": dest_path,
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


@router.get("/api/trades/{code}")
def trades_by_code(code: str):
    try:
        daily_positions = _get_daily_positions_db([code])
        daily_for_code = daily_positions.get(code, [])

        with get_conn() as conn:
            db_events = get_events(conn, [code])

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
            events_payload.append(_map_trade_event(ev))
        current_positions = _calc_current_positions_by_broker(events_payload)
        current_metrics = _calc_position_metrics(events_payload)

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
        daily_positions_map = _get_daily_positions_db(code_list)

        with get_conn() as conn:
            db_events = get_events(conn, code_list)

        events_payload = []
        for ev in db_events:
            events_payload.append(_map_trade_event(ev))

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
        rebuild_summary = rebuild_positions(conn)
    return JSONResponse(content={"symbol": symbol, "rebuild": rebuild_summary})


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
            summary = rebuild_positions(conn)

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

        result = []
        for r in rows:
            sym = r[0]
            name_row = conn.execute("SELECT name FROM tickers WHERE code = ?", [sym]).fetchone()
            name = name_row[0] if name_row else ""

            b_qty = r[1]
            s_qty = r[2]
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
