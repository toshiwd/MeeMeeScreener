import sqlite3
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app', 'backend'))

from db import get_conn

def main():
    print("=== Database Status Check ===\n")
    
    with get_conn() as conn:
        # Check trade_events
        trade_count = conn.execute("SELECT COUNT(*) FROM trade_events").fetchone()[0]
        print(f"Trade events: {trade_count}")
        
        if trade_count > 0:
            print("\nSample trade events:")
            rows = conn.execute("""
                SELECT broker, exec_dt, symbol, action, qty, price
                FROM trade_events
                ORDER BY exec_dt DESC
                LIMIT 5
            """).fetchall()
            for row in rows:
                print(f"  {row}")
        
        # Check positions_live
        print(f"\n=== Positions Live ===")
        live_count = conn.execute("SELECT COUNT(*) FROM positions_live").fetchone()[0]
        print(f"Total positions_live records: {live_count}")
        
        holdings_count = conn.execute("""
            SELECT COUNT(*) FROM positions_live 
            WHERE buy_qty > 0 OR sell_qty > 0
        """).fetchone()[0]
        print(f"Holdings (buy_qty > 0 OR sell_qty > 0): {holdings_count}")
        
        if holdings_count > 0:
            print("\nCurrent holdings:")
            rows = conn.execute("""
                SELECT symbol, buy_qty, sell_qty, spot_qty, margin_long_qty, margin_short_qty, has_issue
                FROM positions_live
                WHERE buy_qty > 0 OR sell_qty > 0
                ORDER BY symbol
                LIMIT 10
            """).fetchall()
            for symbol, buy, sell, spot, margin_long, margin_short, has_issue in rows:
                issue = " ⚠️" if has_issue else ""
                print(f"  {symbol}: buy={buy}, sell={sell}, spot={spot}, margin_long={margin_long}, margin_short={margin_short}{issue}")
        
        # Check API response simulation
        print(f"\n=== API Response Simulation ===")
        live_rows = conn.execute("""
            SELECT symbol, buy_qty, sell_qty, opened_at, has_issue, issue_note
            FROM positions_live
        """).fetchall()
        
        holding_codes = []
        for symbol, buy_qty, sell_qty, opened_at, has_issue, issue_note in live_rows:
            buy_lots = float(buy_qty or 0) / 100.0
            sell_lots = float(sell_qty or 0) / 100.0
            if buy_lots > 0 or sell_lots > 0:
                holding_codes.append(str(symbol))
        
        print(f"holding_codes count: {len(holding_codes)}")
        if holding_codes:
            print(f"Sample holding_codes: {holding_codes[:10]}")

if __name__ == "__main__":
    main()
