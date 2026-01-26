"""
イベントデータ（決算日・権利落ち日）を手動で取得するスクリプト
"""
import sys
import os

# Add backend to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'app', 'backend'))

from db import get_conn
from events import fetch_earnings_snapshot, fetch_rights_snapshot

def main():
    print("=== Fetching Events Data ===\n")
    
    print("1. Fetching earnings data...")
    try:
        earnings_data = fetch_earnings_snapshot()
        print(f"   Found {len(earnings_data)} earnings records")
    except Exception as e:
        print(f"   Error: {e}")
        earnings_data = []
    
    print("\n2. Fetching rights data...")
    try:
        rights_data = fetch_rights_snapshot()
        print(f"   Found {len(rights_data)} rights records")
    except Exception as e:
        print(f"   Error: {e}")
        rights_data = []
    
    if not earnings_data and not rights_data:
        print("\nNo data fetched. Exiting.")
        return
    
    print("\n3. Saving to database...")
    with get_conn() as conn:
        # Save earnings data
        if earnings_data:
            conn.execute("DELETE FROM earnings_planned WHERE source = 'JPX'")
            for item in earnings_data:
                conn.execute("""
                    INSERT INTO earnings_planned (code, planned_date, kind, company_name, source, fetched_at)
                    VALUES (?, ?, ?, ?, 'JPX', CURRENT_TIMESTAMP)
                """, [item['code'], item['planned_date'], item.get('kind'), item.get('company_name')])
            print(f"   Saved {len(earnings_data)} earnings records")
        
        # Save rights data
        if rights_data:
            conn.execute("DELETE FROM ex_rights WHERE source = 'JPX'")
            for item in rights_data:
                conn.execute("""
                    INSERT INTO ex_rights (
                        code, ex_date, record_date, category, 
                        last_rights_date, source, fetched_at
                    )
                    VALUES (?, ?, ?, ?, ?, 'JPX', CURRENT_TIMESTAMP)
                """, [
                    item['code'],
                    item.get('ex_date'),
                    item.get('record_date'),
                    item.get('category'),
                    item.get('last_rights_date')
                ])
            print(f"   Saved {len(rights_data)} rights records")
    
    print("\n4. Verifying data for 1928 (Sekisui House)...")
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT code, ex_date, category, last_rights_date
            FROM ex_rights
            WHERE code = '1928'
            ORDER BY ex_date DESC
            LIMIT 5
        """).fetchall()
        
        if rows:
            print("   Rights data for 1928:")
            for code, ex_date, category, last_rights_date in rows:
                print(f"     {ex_date}: {category} (last rights: {last_rights_date})")
        else:
            print("   No rights data found for 1928")
    
    print("\n✓ Done!")

if __name__ == "__main__":
    main()
