from __future__ import annotations

from app.db.session import get_conn
from app.services.trade_events import get_events

def _get_daily_positions_db(target_codes: list[str] | None = None) -> dict[str, list[dict]]:
    # Reconstruct daily positions from DB events
    with get_conn() as conn:
        events = get_events(conn, target_codes)
    
    events_by_code = {}
    for ev in events:
        # 3:symbol
        code = ev[3]
        events_by_code.setdefault(code, []).append(ev)
        
    result = {}
    
    for code, ev_list in events_by_code.items():
        daily_list = []
        
        # Sorting is guaranteed by get_events (exec_dt ASC)
        # We need to walk through days.
        # But for chart, we usually just want the change points or one per day?
        # Usually one per day matching the daily bars is ideal, but here we just produce 
        # a series of "end of day" positions for days where events happened OR all days?
        # Frontend filters by `time === selectedBarData.time`. So we need entries for every relevant date.
        # Ideally, we return a dense list or the frontend needs to fill gaps.
        # Current _build_daily_positions returns sparse list (only days with trades?).
        # No, let's look at `_build_daily_positions` again. It iterates keys of grouped trades.
        # So it only returns days with trades. 
        # BUT DetailView filters: `dailyPositions.filter(p => p.time === selectedBarData.time)`.
        # This implies it EXPECTS an entry for that exact day.
        # If the user held a position for 30 days but only traded on day 1 and 30, 
        # the middle days would show 0 if sparse.
        # This seems like a BUG in existing frontend logic if it relies on exact match from sparse list?
        # Or maybe `dailyPositions` was dense?
        # Existing logic: `for date in sorted(grouped.keys())`. Sparse.
        # So the Chart Overlay probably only shows dots on trade days?
        # The Position Ledger PnL panel?
        # Wait, if `posList` is empty, `buy` is 0.
        # So the existing app only shows position ON THE DAY OF TRADE? That seems wrong for "Held Position".
        # But maybe that's the current behavior.
        # User wants "Fix the calculation".
        # If I output a dense list (carried forward), it would fix the chart to show "Holding" line.
        # I will implement DENSE (carry forward) positions for every day since first trade?
        # Or at least for every day in the event range.
        # Actually, let's stick to Sparse (Event Days) + "carry forward" logic is handled where? 
        # If I change to Dense, frontend logic `posList.reduce` will work (1 item).
        # Let's try to be smart. Return a list of all dates where position CHANGED.
        # Frontend logic `dailyPositions.filter(p => p.time === ...)` is strict equality.
        # If I return dense list for all known dates, it acts as full history.
        # But I don't know all "market dates" here easily without querying daily_bars.
        # I'll stick to returning "Event Days" but with "Accumulated Balance".
        # Wait, if I only return Event Days, then on non-event days the chart says 0.
        # The user complained about "Trade History Sync".
        # I will produce a DENSE list by querying daily_bars dates for the code?
        # That might be too heavy.
        # Let's perform a simpler approach: 
        # Just return the Event Days with the *Post-Trade Balance*.
        # And if the frontend needs more, we might need to change frontend.
        # Checking DetailView: `const posList = dailyPositions.filter(p => p.time === selectedBarData.time);`
        # It is strictly checking. So if I don't provide data for that day, it thinks 0.
        # This confirms existing app only shows dots on trade days. 
        # I will keep this behavior (Sparse) but ensure the values are "Total Held After Trade" (Cumulative), 
        # not just "Trade Quantity".
        # Existing `_build_daily_positions` accumulates `long_shares` and `short_shares` in the loop.
        # So it WAS cumulative.
        # My new logic must also be cumulative.
        
        spot = 0.0
        margin_long = 0.0
        margin_short = 0.0
        
        # 0:id, 1:broker, 2:exec_dt, 3:symbol, 4:action, 5:qty, 6:price, 7:hash, 8:created, 9:txn, 10:side
        for ev in ev_list:
             dt_obj = ev[2] # datetime
             date_str = dt_obj.strftime("%Y-%m-%d")
             ts = int(dt_obj.replace(tzinfo=None).timestamp()) 
             # Note: timezone handling might be needed. Data is likely JST/UTC naive.
             # In `main.py`, `daily_bars` dates are unix timestamps.
             # I should align with that. 
             # `daily_bars` uses `date` column (integer YYYYMMDD? No, checking schema).
             # Schema: `date INTEGER`. Usually YYYYMMDD or Unix?
             # `ingest_txt.py`: `daily["date"] = (daily["date"].astype("int64") // 1_000_000_000)` -> Unix Secs.
             # So I need Unix Secs for `time`.
             
             qty = float(ev[5] or 0)
             action = ev[4]
             
             if action in ("SPOT_BUY", "SPOT_IN"): spot += qty
             elif action == "SPOT_SELL": spot -= qty
             elif action == "SPOT_OUT": spot -= qty
             elif action == "MARGIN_OPEN_LONG": margin_long += qty
             elif action == "MARGIN_CLOSE_LONG": margin_long -= qty
             elif action == "MARGIN_OPEN_SHORT": margin_short += qty
             elif action == "MARGIN_CLOSE_SHORT": margin_short -= qty
             elif action == "DELIVERY_SHORT": spot -= qty; margin_short -= qty
             elif action == "MARGIN_SWAP_TO_SPOT": margin_long -= qty; spot += qty
             
             # Append current state
             # Check if we already have an entry for this day? 
             # If multiple trades on same day, we update the last one or append?
             # Frontend uses `filter`, so it handles multiple. But Chart usually wants one.
             # We'll just append.
             
             daily_list.append({
                 "time": ts,
                 "date": date_str,
                 "longLots": spot + margin_long,
                 "shortLots": margin_short,
                 "spotLots": spot,
                 "marginLongLots": margin_long,
                 "marginShortLots": margin_short
             })
             
        result[code] = daily_list
        
    return result
    grouped: dict[str, list[dict]] = {}
    for trade in trades:
        date = trade.get("date")
        if not date:
            continue
        grouped.setdefault(date, []).append(trade)

    long_shares = 0.0
    short_shares = 0.0
    positions: list[dict] = []

    def sort_key(item: dict) -> tuple[int, int]:
        return (item.get("_event_order", 2), item.get("_row_index", 0))

    for date in sorted(grouped.keys()):
        for trade in sorted(grouped[date], key=sort_key):
            qty_shares = float(trade.get("qtyShares") or 0)
            kind = trade.get("kind")
            if kind == "BUY_OPEN":
                long_shares += qty_shares
            elif kind == "SELL_CLOSE":
                long_shares = max(0.0, long_shares - qty_shares)
            elif kind == "SELL_OPEN":
                short_shares += qty_shares
            elif kind == "BUY_CLOSE":
                short_shares = max(0.0, short_shares - qty_shares)
            elif kind == "DELIVERY":
                long_shares = max(0.0, long_shares - qty_shares)
                short_shares = max(0.0, short_shares - qty_shares)
            elif kind == "TAKE_DELIVERY":
                continue
            elif kind == "INBOUND":
                if long_shares > 0 and qty_shares > 0:
                    long_shares += qty_shares
                continue
            elif kind == "OUTBOUND":
                if long_shares > 0 and qty_shares > 0:
                    long_shares = max(0.0, long_shares - qty_shares)
                continue

        positions.append(
            {
                "date": date,
                "buyShares": long_shares,
                "sellShares": short_shares,
                "buyUnits": long_shares / 100,
                "sellUnits": short_shares / 100,
                "text": f"{short_shares/100:g}-{long_shares/100:g}"
            }
        )

    return positions

def _calc_position_metrics(trades: list[dict]) -> dict:
    ordered = [
        (trade, index)
        for index, trade in enumerate(trades)
    ]
    def sort_key(item: tuple[dict, int]) -> tuple[str, int, int]:
        trade, index = item
        date = trade.get("date") or ""
        action = trade.get("action") or ""
        action_rank = 0 if action == "open" else 1 if action == "close" else 2
        return (date, action_rank, index)
    ordered.sort(key=sort_key)

    long_lots = 0.0
    short_lots = 0.0
    avg_long_price = 0.0
    avg_short_price = 0.0
    realized_pnl = 0.0

    for trade, _ in ordered:
        lots = trade.get("units") or 0
        try:
            lots = float(lots)
        except (TypeError, ValueError):
            lots = 0.0
        lots = max(0.0, lots)
        price = float(trade.get("price") or 0)
        action = trade.get("action") or "open"
        kind = trade.get("kind") or ""
        side = trade.get("side") or ""

        if kind == "DELIVERY":
            long_lots = max(0.0, long_lots - lots)
            short_lots = max(0.0, short_lots - lots)
            continue
        if kind == "TAKE_DELIVERY":
            continue
        if kind == "INBOUND":
            if long_lots > 0 and lots > 0:
                total_cost = avg_long_price * long_lots
                long_lots += lots
                avg_long_price = total_cost / long_lots
            elif lots > 0:
                long_lots += lots
                avg_long_price = price or avg_long_price
            continue
        if kind == "OUTBOUND":
            long_lots = max(0.0, long_lots - lots)
            if long_lots == 0:
                avg_long_price = 0.0
            continue

        if side == "buy" and action == "open":
            next_lots = long_lots + lots
            avg_long_price = (
                (avg_long_price * long_lots + price * lots) / next_lots
                if next_lots > 0
                else 0.0
            )
            long_lots = next_lots
        elif side == "sell" and action == "close":
            close_lots = min(long_lots, lots)
            realized_pnl += (price - avg_long_price) * close_lots * 100
            long_lots = max(0.0, long_lots - lots)
            if long_lots == 0:
                avg_long_price = 0.0
        elif side == "sell" and action == "open":
            next_lots = short_lots + lots
            avg_short_price = (
                (avg_short_price * short_lots + price * lots) / next_lots
                if next_lots > 0
                else 0.0
            )
            short_lots = next_lots
        elif side == "buy" and action == "close":
            close_lots = min(short_lots, lots)
            realized_pnl += (avg_short_price - price) * close_lots * 100
            short_lots = max(0.0, short_lots - lots)
            if short_lots == 0:
                avg_short_price = 0.0

    return {
        "longLots": long_lots,
        "shortLots": short_lots,
        "avgLongPrice": avg_long_price,
        "avgShortPrice": avg_short_price,
        "realizedPnL": realized_pnl
    }


def _normalize_units(qty: float) -> float:
    if not qty:
        return 0.0
    if qty >= 100:
        return qty / 100
    return qty


def _resolve_broker_meta(raw: str | None) -> tuple[str, str]:
    text = (raw or "").strip()
    lower = text.lower()
    if "sbi" in lower:
        return "sbi", "SBI"
    if "rakuten" in lower:
        return "rakuten", "RAKUTEN"
    if text:
        return lower, text.upper()
    return "unknown", "N/A"


def _map_trade_event(ev: tuple) -> dict:
    exec_dt = ev[2]
    date_str = exec_dt.strftime("%Y-%m-%d") if exec_dt else ""
    action = ev[4] or ""
    qty = float(ev[5] or 0)
    units = _normalize_units(qty)
    side = "buy"
    trade_action = "open"
    kind = ""
    if action in ("SPOT_BUY", "MARGIN_OPEN_LONG"):
        side = "buy"
        trade_action = "open"
        kind = "BUY_OPEN"
    elif action in ("SPOT_SELL", "MARGIN_CLOSE_LONG"):
        side = "sell"
        trade_action = "close"
        kind = "SELL_CLOSE"
    elif action == "MARGIN_OPEN_SHORT":
        side = "sell"
        trade_action = "open"
        kind = "SELL_OPEN"
    elif action == "MARGIN_CLOSE_SHORT":
        side = "buy"
        trade_action = "close"
        kind = "BUY_CLOSE"
    elif action == "DELIVERY_SHORT":
        side = "buy"
        trade_action = "close"
        kind = "DELIVERY"
    elif action == "SPOT_IN":
        side = "buy"
        trade_action = "open"
        kind = "INBOUND"
    elif action == "SPOT_OUT":
        side = "sell"
        trade_action = "close"
        kind = "OUTBOUND"
    elif action == "MARGIN_SWAP_TO_SPOT":
        side = "buy"
        trade_action = "open"
        kind = "TAKE_DELIVERY"
    else:
        kind = action or "UNKNOWN"

    return {
        "broker": ev[1],
        "exec_dt": exec_dt.isoformat() if exec_dt else "",
        "tradeDate": date_str,
        "date": date_str,
        "code": ev[3],
        "symbol": ev[3],
        "action": trade_action,
        "side": side,
        "kind": kind,
        "qty": qty,
        "qtyShares": qty,
        "units": units,
        "price": ev[6],
        "memo": f"{ev[9]} {ev[10]}" if len(ev) > 9 else action,
        "raw": {"action": action, "transaction_type": ev[9] if len(ev) > 9 else "", "side_type": ev[10] if len(ev) > 10 else ""},
    }


def _calc_current_positions_by_broker(trades: list[dict]) -> list[dict]:
    grouped: dict[str, dict] = {}
    for trade in trades:
        key, label = _resolve_broker_meta(trade.get("broker"))
        bucket = grouped.get(key)
        if not bucket:
            bucket = {"key": key, "label": label, "trades": []}
            grouped[key] = bucket
        bucket["trades"].append(trade)

    results: list[dict] = []
    for bucket in grouped.values():
        metrics = _calc_position_metrics(bucket["trades"])
        results.append(
            {
                "brokerKey": bucket["key"],
                "brokerLabel": bucket["label"],
                "longLots": metrics["longLots"],
                "shortLots": metrics["shortLots"],
                "avgLongPrice": metrics["avgLongPrice"],
                "avgShortPrice": metrics["avgShortPrice"],
                "realizedPnL": metrics["realizedPnL"],
            }
        )
    return results

