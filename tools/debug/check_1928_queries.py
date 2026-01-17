import sys
import os
from datetime import datetime, timedelta, date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'app', 'backend'))

from db import get_conn

def main():
    print("=== Checking Data for 1928 ===\n")
    
    today = datetime.now().date()
    
    with get_conn() as conn:
        # Check earnings data
        print("1. Earnings data:")
        earnings = conn.execute("""
            SELECT code, planned_date
            FROM earnings_planned
            WHERE code = '1928'
            ORDER BY planned_date
            LIMIT 5
        """).fetchall()
        
        if earnings:
            for code, planned_date in earnings:
                print(f"   {code}: {planned_date}")
        else:
            print("   No earnings data")
        
        # Check rights data
        print("\n2. Rights data:")
        rights = conn.execute("""
            SELECT code, ex_date, last_rights_date, category
            FROM ex_rights
            WHERE code = '1928'
            ORDER BY ex_date
            LIMIT 5
        """).fetchall()
        
        if rights:
            for code, ex_date, last_rights_date, category in rights:
                print(f"   {code}: ex_date={ex_date}, last_rights={last_rights_date}, category={category}")
        else:
            print("   No rights data")
        
        # Check what the screener query would return
        print("\n3. Screener query for earnings (next 30 days):")
        window_end = today + timedelta(days=30)
        earnings_result = conn.execute("""
            SELECT code, MIN(planned_date) AS planned_date
            FROM earnings_planned
            WHERE code = '1928' AND planned_date BETWEEN ? AND ?
            GROUP BY code
        """, [today, window_end]).fetchall()
        
        if earnings_result:
            for code, planned_date in earnings_result:
                print(f"   {code}: {planned_date}")
        else:
            print(f"   No earnings between {today} and {window_end}")
        
        # Check what the screener query would return for rights
        print("\n4. Screener query for rights (from today):")
        rights_result = conn.execute("""
            SELECT code, MIN(COALESCE(last_rights_date, ex_date)) AS rights_date
            FROM ex_rights
            WHERE code = '1928' AND COALESCE(last_rights_date, ex_date) >= ?
            GROUP BY code
        """, [today]).fetchall()
        
        if rights_result:
            for code, rights_date in rights_result:
                print(f"   {code}: {rights_date}")
                print(f"   Formatted: {rights_date if isinstance(rights_date, str) else (rights_date.isoformat() if isinstance(rights_date, date) else str(rights_date))}")
        else:
            print(f"   No rights from {today}")

if __name__ == "__main__":
    main()
