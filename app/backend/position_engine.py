
import uuid
from datetime import datetime
from db import get_conn

def get_events(conn, symbols=None):
    query = """
        SELECT id, broker, exec_dt, symbol, action, qty, price, source_row_hash, created_at, transaction_type, side_type
        FROM trade_events
    """
    params = []
    if symbols:
        placeholders = ",".join(["?"] * len(symbols))
        query += f" WHERE symbol IN ({placeholders})"
        params.extend(symbols)
    
    query += " ORDER BY exec_dt ASC, created_at ASC"
    return conn.execute(query, params).fetchall()

def recalculate_positions(target_symbols=None):
    # target_symbols: list of str or None (all)
    with get_conn() as conn:
        try:
            if target_symbols:
                # Delete existing live positions for these symbols
                placeholders = ",".join(["?"] * len(target_symbols))
                conn.execute(f"DELETE FROM positions_live WHERE symbol IN ({placeholders})", target_symbols)
                # Not deleting history to preserve rounds? Or clear rounds too?
                # User wants "Re-aggregation from scratch". Safest is to clear everything for the symbol.
                # But position_rounds might contain user notes?
                # Implementation spec: "Safest: Re-aggregate from stored trade details every time"
                # So clearing is correct for "calculated" state.
                # But positions_live has "issue_note" which might be valuable?
                # Actually, issue_note is generated from calculation in the new logic ("Integrity Warning").

                events = get_events(conn, target_symbols)
            else:
                conn.execute("DELETE FROM positions_live")
                events = get_events(conn)

            events_by_symbol = {}
            for ev in events:
                # ev index: 0:id, 1:broker, 2:exec_dt, 3:symbol, 4:action, 5:qty, 6:price, 7:hash, 8:created, 9:txn, 10:side
                sym = ev[3]
                if sym not in events_by_symbol:
                    events_by_symbol[sym] = []
                events_by_symbol[sym].append(ev)

            for symbol, ev_list in events_by_symbol.items():
                process_symbol_events(conn, symbol, ev_list)

        except Exception as e:
            print(f"Error recalculating positions: {e}")
            raise e

def process_symbol_events(conn, symbol, events):
    spot_qty = 0.0
    margin_long_qty = 0.0
    margin_short_qty = 0.0
    
    opened_at = None
    last_action_dt = None
    
    # Track negative balance issues
    integrity_warning = False
    issue_notes = []

    for ev in events:
        exec_dt = ev[2]
        action = ev[4]
        qty = float(ev[5] or 0)
        
        # Delta Logic
        if action == "SPOT_BUY" or action == "SPOT_IN":
            spot_qty += qty
        elif action == "SPOT_SELL":
            if qty > spot_qty:
                integrity_warning = True
                issue_notes.append(f"Spot sell exceeds holdings at {exec_dt}")
            spot_qty = max(0.0, spot_qty - qty)
        elif action == "SPOT_OUT":
            if qty > spot_qty:
                integrity_warning = True
                issue_notes.append(f"Spot outbound exceeds holdings at {exec_dt}")
            spot_qty = max(0.0, spot_qty - qty)
        
        elif action == "MARGIN_OPEN_LONG":
            margin_long_qty += qty
        elif action == "MARGIN_CLOSE_LONG":
            if qty > margin_long_qty:
                integrity_warning = True
                issue_notes.append(f"Margin long close exceeds holdings at {exec_dt}")
            margin_long_qty = max(0.0, margin_long_qty - qty)
            
        elif action == "MARGIN_OPEN_SHORT":
            margin_short_qty += qty
        elif action == "MARGIN_CLOSE_SHORT":
            if qty > margin_short_qty:
                integrity_warning = True
                issue_notes.append(f"Margin short close exceeds holdings at {exec_dt}")
            margin_short_qty = max(0.0, margin_short_qty - qty)
            
        elif action == "DELIVERY_SHORT":
            # Delivery: Hand over Spot to Cover Short
            if qty > spot_qty or qty > margin_short_qty:
                integrity_warning = True
                issue_notes.append(f"Delivery exceeds holdings at {exec_dt}")
            spot_qty = max(0.0, spot_qty - qty)
            margin_short_qty = max(0.0, margin_short_qty - qty)
            
        elif action == "MARGIN_SWAP_TO_SPOT":
            # Genbiki: Pay cash to take delivery of Margin Long -> Spot
            if qty > margin_long_qty:
                integrity_warning = True
                issue_notes.append(f"Genbiki exceeds holdings at {exec_dt}")
            move_qty = min(qty, margin_long_qty)
            margin_long_qty -= move_qty
            spot_qty += move_qty
        elif action == "UNKNOWN":
            integrity_warning = True
            issue_notes.append(f"Unknown action at {exec_dt}")

        # Integrity Check
        if spot_qty < -0.0001:
            integrity_warning = True
            issue_notes.append(f"Negative Spot {spot_qty} at {exec_dt}")
        if margin_long_qty < -0.0001:
            integrity_warning = True
            issue_notes.append(f"Negative Margin Long {margin_long_qty} at {exec_dt}")
        if margin_short_qty < -0.0001:
            integrity_warning = True
            issue_notes.append(f"Negative Margin Short {margin_short_qty} at {exec_dt}")
            
        last_action_dt = exec_dt
        if not opened_at:
            opened_at = exec_dt

    # Final Rounding to integer (User said "Integers")
    spot_qty = round(spot_qty)
    margin_long_qty = round(margin_long_qty)
    margin_short_qty = round(margin_short_qty)
    
    # "Holding" check
    has_position = (spot_qty != 0) or (margin_long_qty != 0) or (margin_short_qty != 0)
    
    if has_position:
        # Save to DB
        # User defined display: sell = margin_short, buy = spot + margin_long
        # positions_live schema updated to hold specific columns
        
        issue_msg = "; ".join(issue_notes) if integrity_warning else None
        
        conn.execute("""
            INSERT OR REPLACE INTO positions_live (
                symbol, spot_qty, margin_long_qty, margin_short_qty, 
                buy_qty, sell_qty,
                opened_at, updated_at, has_issue, issue_note
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            symbol, 
            spot_qty, margin_long_qty, margin_short_qty,
            (spot_qty + margin_long_qty), # aggregated buy_qty for generic display
            margin_short_qty,             # aggregated sell_qty for generic display
            opened_at, 
            last_action_dt or datetime.now(), 
            integrity_warning, 
            issue_msg
        ])
