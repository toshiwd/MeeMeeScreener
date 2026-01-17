import sys
import os

# Add backend to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'app', 'backend'))

from db import get_conn

def main():
    print("=== Checking Rights Data for 1928 (Sekisui House) ===\n")
    
    with get_conn() as conn:
        # Check ex_rights table
        count = conn.execute("SELECT COUNT(*) FROM ex_rights").fetchone()[0]
        print(f"Total ex_rights records: {count}\n")
        
        if count > 0:
            # Check for 1928
            print("Rights data for 1928:")
            rows = conn.execute("""
                SELECT code, ex_date, category, last_rights_date, record_date
                FROM ex_rights
                WHERE code = '1928'
                ORDER BY ex_date DESC
                LIMIT 10
            """).fetchall()
            
            if rows:
                for code, ex_date, category, last_rights_date, record_date in rows:
                    print(f"  Code: {code}")
                    print(f"  Ex-date: {ex_date}")
                    print(f"  Category: {category}")
                    print(f"  Last rights date: {last_rights_date}")
                    print(f"  Record date: {record_date}")
                    print()
            else:
                print("  No rights data found for 1928")
            
            # Check upcoming rights (next 60 days)
            print("\nUpcoming rights for 1928 (next 60 days):")
            rows = conn.execute("""
                SELECT code, ex_date, category
                FROM ex_rights
                WHERE code = '1928'
                  AND ex_date >= date('now') 
                  AND ex_date <= date('now', '+60 days')
                ORDER BY ex_date
            """).fetchall()
            
            if rows:
                for code, ex_date, category in rows:
                    print(f"  {code}: {ex_date} ({category})")
            else:
                print("  No upcoming rights found for 1928")
        else:
            print("No rights data in database. Need to fetch events data.")

if __name__ == "__main__":
    main()
