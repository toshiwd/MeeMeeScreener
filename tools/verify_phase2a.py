
import sys
import os
import time
from pathlib import Path

# Setup paths
repo_root = Path(__file__).resolve().parent.parent
sys.path.append(str(repo_root))
sys.path.append(str(repo_root / "app" / "backend"))

try:
    from core.jobs import job_manager
    from db import init_schema, get_conn
    from core.config import config
except ImportError as e:
    print(f"Import Error: {e}")
    sys.exit(1)

print("--- Initializing Schema ---")
init_schema()

print("--- Registering Test Handler ---")
def test_handler(job_id, payload):
    print(f"  [Worker] Processing job {job_id} with payload {payload}")
    time.sleep(1)
    print(f"  [Worker] Finished job {job_id}")

job_manager.register_handler("test_job", test_handler)

print("--- Submitting Job ---")
job_id = job_manager.submit("test_job", {"foo": "bar"})
print(f"Job ID: {job_id}")

print("--- Polling Status ---")
for i in range(10):
    status = job_manager.get_status(job_id)
    print(f"[{i}s] Status: {status['status']} Progress: {status.get('progress')}")
    if status['status'] in ('success', 'failed'):
        break
    time.sleep(0.5)

print("\n--- History ---")
history = job_manager.get_history()
for h in history:
    print(h)

print("\n--- DB Verification ---")
with get_conn() as conn:
    row = conn.execute("SELECT status, message FROM sys_jobs WHERE id = ?", [job_id]).fetchone()
    print(f"DB Row: {row}")
    if row and row[0] == 'success':
        print("SUCCESS: Job completed and persisted.")
    else:
        print("FAILURE: Job did not complete successfully in DB.")
        sys.exit(1)
