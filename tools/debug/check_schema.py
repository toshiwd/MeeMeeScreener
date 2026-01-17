import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'app', 'backend'))

from db import get_conn

def main():
    with get_conn() as conn:
        # Check if earnings_planned table exists
        tables = conn.execute("SELECT table_name FROM duckdb_tables() WHERE table_name LIKE '%earnings%' OR table_name LIKE '%rights%'").fetchall()
        print("Tables with 'earnings' or 'rights':")
        for (table_name,) in tables:
            print(f"  - {table_name}")
            
            # Get schema
            schema = conn.execute(f"DESCRIBE {table_name}").fetchall()
            print(f"    Columns:")
            for row in schema:
                print(f"      {row[0]}: {row[1]}")
            print()

if __name__ == "__main__":
    main()
