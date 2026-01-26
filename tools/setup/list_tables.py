import sys
import os
sys.path.append(os.path.join(os.getcwd(), "app", "backend"))

import duckdb

db_path = os.path.join(os.getcwd(), "app", "backend", "stocks.duckdb")
conn = duckdb.connect(db_path, read_only=True)

print("Available tables:")
tables = conn.execute("SHOW TABLES").fetchall()
for table in tables:
    print(f"  - {table[0]}")

conn.close()
