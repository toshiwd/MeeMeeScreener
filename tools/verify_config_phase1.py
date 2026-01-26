
import sys
import os
from pathlib import Path


# Fix path to include repo root and backend
repo_root = Path(__file__).resolve().parent.parent
sys.path.append(str(repo_root))
sys.path.append(str(repo_root / "app" / "backend"))

print("--- Testing Core Config ---")
try:
    from core.config import config
    print(f"Repo Root: {config.REPO_ROOT}")
    print(f"Data Dir: {config.DATA_DIR}")
    print(f"DB Path: {config.DB_PATH}")
    print(f"Lock Path: {config.LOCK_FILE_PATH}")
    
    if not config.DATA_DIR.exists():
        print("ERROR: Data Dir does not exist!")
    else:
        print("OK: Data Dir exists.")

except ImportError as e:
    print(f"FATAL: Import Error: {e}")
    sys.exit(1)

print("\n--- Testing Imports ---")
try:
    import main
    print("OK: main imported.")
    import db
    print(f"OK: db imported. Default Path: {main.DEFAULT_DB_PATH}")
except Exception as e:
    print(f"FATAL: Import Check Failed: {e}")
    sys.exit(1)

print("\n--- Phase 1 Verification Passed ---")
