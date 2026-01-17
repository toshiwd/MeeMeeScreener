import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app', 'backend'))

from db import get_conn

def main():
    print("=== Checking Rights Data ===\n")
    
    with get_conn() as conn:
        # Check ex_rights table
        count = conn.execute("SELECT COUNT(*) FROM ex_rights").fetchone()[0]
        print(f"Total ex_rights records: {count}")
        
        if count > 0:
            print("\nSample ex_rights data:")
            rows = conn.execute("""
                SELECT code, ex_date, category, last_rights_date
                FROM ex_rights
                ORDER BY ex_date DESC
                LIMIT 10
            """).fetchall()
            for code, ex_date, category, last_rights_date in rows:
                print(f"  {code}: ex_date={ex_date}, category={category}, last_rights={last_rights_date}")
            
            # Check upcoming rights
            print("\nUpcoming rights (next 30 days):")
            rows = conn.execute("""
                SELECT code, ex_date, category
                FROM ex_rights
                WHERE ex_date >= date('now') AND ex_date <= date('now', '+30 days')
                ORDER BY ex_date
                LIMIT 10
            """).fetchall()
            for code, ex_date, category in rows:
                print(f"  {code}: {ex_date} ({category})")

if __name__ == "__main__":
    main()
