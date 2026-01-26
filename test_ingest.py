#!/usr/bin/env python3
"""Manual TXT ingestion runner (not a pytest test).

This file starts with `test_` for historical reasons. It must not execute on import,
otherwise `pytest` collection will run it unintentionally.
"""

from __future__ import annotations

import os
import sys


def main() -> int:
    os.environ.setdefault("PAN_OUT_TXT_DIR", r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\txt")
    os.environ.setdefault("TXT_DATA_DIR", r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\txt")
    os.environ.setdefault("STOCKS_DB_PATH", r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")

    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "app", "backend"))

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
        print("Ingestion completed successfully!")
        print("=" * 80)
        return 0
    except Exception as exc:
        print("=" * 80)
        print(f"Ingestion failed: {exc}")
        print("=" * 80)
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

