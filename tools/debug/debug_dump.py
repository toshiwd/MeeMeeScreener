
import duckdb
import os

DB_PATH = os.path.join("app", "backend", "stocks.duckdb")

def debug_db_contents():
    conn = duckdb.connect(DB_PATH, read_only=True)
    
    print("--- positions_live (All) ---")
    rows = conn.execute("SELECT * FROM positions_live").fetchall()
    for r in rows:
        print(r)
        
    print("\n--- initial_positions_seed (All) ---")
    try:
        rows = conn.execute("SELECT * FROM initial_positions_seed").fetchall()
        for r in rows:
            print(r)
    except:
        print("Table initial_positions_seed missing")

    print("\n--- trade_events (Sample 5) ---")
    rows = conn.execute("SELECT * FROM trade_events LIMIT 5").fetchall()
    for r in rows:
        print(r)

if __name__ == "__main__":
    debug_db_contents()
