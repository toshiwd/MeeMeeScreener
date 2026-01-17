
import duckdb
import os
from collections import defaultdict

DB_PATH = os.path.join("app", "backend", "stocks.duckdb")
CODE = "3288"

def debug_openhouse():
    if not os.path.exists(DB_PATH):
        print(f"DB not found at {DB_PATH}")
        return

    conn = duckdb.connect(DB_PATH)
    
    print(f"--- Trade Events for {CODE} ---")
    events = conn.execute("SELECT exec_dt, action, qty, price, transaction_type, side_type FROM trade_events WHERE symbol = ? ORDER BY exec_dt", [CODE]).fetchall()
    
    spot = 0
    margin_long = 0
    margin_short = 0
    
    for ev in events:
        dt, action, qty, price, t_type, s_type = ev
        print(f"{dt} | {action} | {qty} | {t_type} {s_type}")
        
        q = float(qty)
        if action in ("SPOT_BUY", "SPOT_IN"): spot += q
        elif action == "SPOT_SELL": spot -= q
        elif action == "MARGIN_OPEN_LONG": margin_long += q
        elif action == "MARGIN_CLOSE_LONG": margin_long -= q
        elif action == "MARGIN_OPEN_SHORT": margin_short += q
        elif action == "MARGIN_CLOSE_SHORT": margin_short -= q
        elif action == "DELIVERY_SHORT": spot -= q; margin_short -= q

    print(f"\n--- Calculated State ---")
    print(f"Spot: {spot}")
    print(f"Margin Long: {margin_long}")
    print(f"Margin Short: {margin_short}")
    
    print(f"\n--- DB positions_live ---")
    live = conn.execute("SELECT * FROM positions_live WHERE symbol = ?", [CODE]).fetchall()
    print(live)

if __name__ == "__main__":
    debug_openhouse()
