# Temporary script to rebuild positions
import sqlite3
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from positions import rebuild_positions
from db import get_conn

def main():
    print("Rebuilding positions...")
    try:
        with get_conn() as conn:
            summary = rebuild_positions(conn)
            print("Rebuild completed successfully!")
            print(f"Summary: {summary}")
            
            # Show current positions
            rows = conn.execute("""
                SELECT symbol, buy_qty, sell_qty, has_issue
                FROM positions_live
                WHERE buy_qty > 0 OR sell_qty > 0
                ORDER BY symbol
            """).fetchall()
            
            print(f"\nCurrent holdings ({len(rows)} symbols):")
            for symbol, buy, sell, has_issue in rows:
                issue_mark = " ⚠️" if has_issue else ""
                print(f"  {symbol}: 売{sell:g}-買{buy:g}{issue_mark}")
                
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
