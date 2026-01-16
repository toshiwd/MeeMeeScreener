
import duckdb
import os

PATHS = [
    os.path.join("app", "backend", "stocks.duckdb"),
    "stocks.duckdb",
    os.path.join("release", "MeeMeeScreener", "_internal", "app", "backend", "stocks.duckdb")
]
CODE = "3288"

def check_db(path):
    print(f"\nChecking: {path}")
    if not os.path.exists(path):
        print("  Not found.")
        return
        
    try:
        conn = duckdb.connect(path, read_only=True)
        # Check if table exists
        tables = conn.execute("SHOW TABLES").fetchall()
        t_names = [t[0] for t in tables]
        if 'trade_events' not in t_names:
             print("  Table trade_events missing.")
             return
             
        events = conn.execute("SELECT exec_dt, action, qty, price FROM trade_events WHERE symbol = ? ORDER BY exec_dt", [CODE]).fetchall()
        print(f"  Events count: {len(events)}")
        for e in events:
            print(f"    {e}")
            
        if 'positions_live' in t_names:
            live = conn.execute("SELECT * FROM positions_live WHERE symbol = ?", [CODE]).fetchall()
            print(f"  Live Positions: {live}")
        else:
            print("  Table positions_live missing.")
            
    except Exception as e:
        print(f"  Error: {e}")

if __name__ == "__main__":
    for p in PATHS:
        check_db(p)
