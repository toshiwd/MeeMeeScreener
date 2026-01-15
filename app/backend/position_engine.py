import uuid
from datetime import datetime
from db import get_conn

def get_events(conn, symbol=None):
    query = "SELECT id, broker, exec_dt, symbol, action, qty, price, source_row_hash, created_at FROM trade_events"
    params = []
    if symbol:
        query += " WHERE symbol = ?"
        params.append(symbol)
    query += " ORDER BY exec_dt ASC, created_at ASC"
    return conn.execute(query, params).fetchall()

def recalculate_positions(target_symbol=None):
    conn = get_conn()
    try:
        if target_symbol:
            conn.execute("DELETE FROM positions_live WHERE symbol = ?", [target_symbol])
            # position_rounds for the symbol should be cleared? 
            # Or should we be careful not to delete history if we are just updating?
            # Ideally, full recalc means full clear for that symbol.
            conn.execute("DELETE FROM position_rounds WHERE symbol = ?", [target_symbol])
            events = get_events(conn, target_symbol)
        else:
            conn.execute("DELETE FROM positions_live")
            conn.execute("DELETE FROM position_rounds")
            events = get_events(conn)
            
        events_by_symbol = {}
        for ev in events:
            # ev format: (id, broker, exec_dt, symbol, action, qty, price, hash, created_at)
            sym = ev[3]
            if sym not in events_by_symbol:
                events_by_symbol[sym] = []
            events_by_symbol[sym].append(ev)
            
        for symbol, ev_list in events_by_symbol.items():
            process_symbol_events(conn, symbol, ev_list)
            
    except Exception as e:
        print(f"Error recalculating positions: {e}")
        raise e
    finally:
        conn.close()

def process_symbol_events(conn, symbol, events):
    buy_qty = 0.0
    sell_qty = 0.0
    
    # Round state
    current_round_id = str(uuid.uuid4())
    current_round_opened_at = None
    is_round_active = False
    
    has_issue = False
    issue_note = ""
    
    # For display of last state in round history
    def get_state_str():
        # Clean float format
        b = f"{buy_qty:.0f}" if buy_qty.is_integer() else f"{buy_qty:.2f}"
        s = f"{sell_qty:.0f}" if sell_qty.is_integer() else f"{sell_qty:.2f}"
        return f"{s}-{b}"

    for ev in events:
        # action index = 4, qty index = 5, exec_dt index = 2
        action = ev[4]
        qty = float(ev[5])
        dt = ev[2]
        
        # Round Start Check
        if not is_round_active:
            # If we acquire any position, round starts
            # Note: qty is just the trade quantity here. We need to check if the trade OPENS a position?
            # Actually, simply: if we are flat, and after this trade we are NOT flat, it's a start.
            # But here we check BEFORE applying.
            # Wait, user definition: "Start: Flat(0-0) -> Non-flat"
            pass

        # Apply Event
        prev_flat = (abs(buy_qty) < 0.0001 and abs(sell_qty) < 0.0001)
        
        if action == "LONG_OPEN":
            buy_qty += qty
        elif action == "LONG_CLOSE":
            buy_qty -= qty
        elif action == "SHORT_OPEN":
            sell_qty += qty
        elif action == "SHORT_CLOSE":
            sell_qty -= qty
            
        # Validation
        if buy_qty < -0.0001 or sell_qty < -0.0001:
             has_issue = True
             issue_note = f"Negative quantity at {dt}. Buy: {buy_qty}, Sell: {sell_qty}"
        
        current_flat = (abs(buy_qty) < 0.0001 and abs(sell_qty) < 0.0001)
        
        # Handle Round Transition
        if prev_flat and not current_flat:
            # Started
            is_round_active = True
            current_round_opened_at = dt
            if not current_round_id:
                current_round_id = str(uuid.uuid4())
                
        elif is_round_active and current_flat:
            # Finished
            conn.execute("""
                INSERT INTO position_rounds (round_id, symbol, opened_at, closed_at, closed_reason, last_state_sell_buy, has_issue, issue_note)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                current_round_id,
                symbol,
                current_round_opened_at,
                dt,
                "FLAT",
                "0-0",
                has_issue,
                issue_note
            ])
            # Reset Round
            current_round_id = str(uuid.uuid4())
            current_round_opened_at = None
            is_round_active = False
            has_issue = False
            issue_note = ""

    # Live Position Snapshot
    if is_round_active or has_issue:
        # Only save if there is an active position OR there was an issue (so user can see it)
        # But actually, specs say "positions_live (current snapshot)".
        # Even if flat, maybe we don't need to show it in the "Held" list.
        # User requirement: "Held list... has_issue -> warning"
        # If it's flat and no issue, we probably don't need it in positions_live.
        
        # However, to be safe, upsert
        if is_round_active or has_issue: # Only insert if held or problematic
             conn.execute("""
                INSERT OR REPLACE INTO positions_live (symbol, buy_qty, sell_qty, opened_at, updated_at, has_issue, issue_note)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?)
            """, [
                symbol,
                buy_qty,
                sell_qty,
                current_round_opened_at,
                has_issue,
                issue_note
            ])
