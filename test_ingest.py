#!/usr/bin/env python3
"""Test script to manually run TXT data ingestion"""

import os
import sys

# Set environment variables to point to the correct data directory
os.environ["PAN_OUT_TXT_DIR"] = r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\txt"
os.environ["TXT_DATA_DIR"] = r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\txt"
os.environ["STOCKS_DB_PATH"] = r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb"

# Add backend to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "app", "backend"))

# Import and run ingest
from ingest_txt import ingest

print("=" * 80)
print("Starting TXT data ingestion...")
print("=" * 80)
print(f"TXT Directory: {os.environ['PAN_OUT_TXT_DIR']}")
print(f"Database: {os.environ['STOCKS_DB_PATH']}")
print("=" * 80)

try:
    ingest()
    print("=" * 80)
    print("✅ Ingestion completed successfully!")
    print("=" * 80)
except Exception as e:
    print("=" * 80)
    print(f"❌ Ingestion failed: {e}")
    print("=" * 80)
    import traceback
    traceback.print_exc()
    sys.exit(1)
