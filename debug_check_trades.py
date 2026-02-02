
import sys
import os

# Add project root to sys.path
sys.path.append(os.getcwd())

from app.db.session import get_conn

codes = ["9684", "1803", "2492"]


def check_trades():
    with open("debug_output.txt", "w", encoding="utf-8") as f:
        with get_conn() as conn:
            f.write("--- Checking positions_live ---\n")
            rows = conn.execute(
                f"SELECT symbol, buy_qty, sell_qty, opened_at, has_issue, issue_note FROM positions_live WHERE symbol IN ({','.join(['?']*len(codes))})",
                codes
            ).fetchall()
            for r in rows:
                f.write(f"Symbol: {r[0]}, Buy: {r[1]}, Sell: {r[2]}, Opened: {r[3]}\n")
            
            f.write("\n--- Checking trade_events ---\n")
            for code in codes:
                f.write(f"\nEvents for {code}:\n")
                events = conn.execute(
                    "SELECT * FROM trade_events WHERE symbol = ? ORDER BY exec_dt",
                    [code]
                ).fetchall()
                f.write(f"  Event Count: {len(events)}\n")
                for e in events:
                    f.write(f"  {e}\n")
            
            f.write("\n--- Checking initial_positions_seed ---\n")
            seeds = conn.execute("SELECT * FROM initial_positions_seed WHERE symbol IN ('9684', '1803', '2492')").fetchall()
            for s in seeds:
                f.write(f"  {s}\n")



if __name__ == "__main__":
    try:
        check_trades()
    except Exception as e:
        print(f"Error: {e}")
