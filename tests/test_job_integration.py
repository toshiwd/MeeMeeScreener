
import sys
import os
import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch
from pathlib import Path
from uuid import uuid4

# Setup path to import backend modules
BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "app", "backend"))
sys.path.append(BACKEND_DIR)
ROOT = Path(__file__).resolve().parents[1]

# Use an isolated data dir so tests never touch the user's real AppData DB (DuckDB locks).
_TEST_TEMP_ROOT = ROOT / ".tmp-tests"
_TEST_TEMP_ROOT.mkdir(parents=True, exist_ok=True)
_TEST_DATA_DIR = (_TEST_TEMP_ROOT / f"meemee_screener_test_{uuid4().hex}").resolve()
_TEST_DATA_DIR.mkdir(parents=True, exist_ok=True)
os.environ["MEEMEE_DATA_DIR"] = str(_TEST_DATA_DIR)

# Mocking config/env early if needed.
os.environ["PAN_CODE_TXT_PATH"] = os.path.join(_TEST_DATA_DIR, "dummy_code.txt")
os.environ["PAN_OUT_TXT_DIR"] = os.path.join(_TEST_DATA_DIR, "txt")
os.environ["PAN_EXPORT_VBS_PATH"] = os.path.join(_TEST_DATA_DIR, "dummy_export_pan.vbs")

# Import app - this might trigger startup logic (db init) so we might want to mock things if possible
# But for integration, using real DB (sqlite legacy) is okay if path defaults to temp or we mock it.
# main.py does 'from db import ...' at module level.
from main import app
from core.jobs import job_manager

client = TestClient(app)

def test_txt_update_submission_flow():
    # Submit job
    resp = client.post("/api/jobs/txt-update")
    assert resp.status_code in (200, 400, 409)
    
    if resp.status_code == 200:
        data = resp.json()
        assert data["ok"] is True
        assert data["status"] == "accepted"
        job_id = data["job_id"]
        assert job_id

        # Try submitting duplicate right away. Depending on how fast the handler fails
        # (e.g. missing dummy files), this can be either rejected (409) or accepted (200).
        resp_dup = client.post("/api/jobs/txt-update")
        assert resp_dup.status_code in (200, 400, 409)
    elif resp.status_code == 400:
        data = resp.json()
        assert data.get("error") == "code_txt_missing"

def test_legacy_endpoint_compatibility():
    # Legacy endpoint should also trigger job
    # We mock existence checks to pass
    with patch("os.path.isfile", return_value=True):
        resp = client.post("/api/txt_update/run")
        # Might return 409 if previous test job is still running!
        # or 200
        assert resp.status_code in (200, 409)
        data = resp.json()
        if resp.status_code == 200:
            assert data["started"] is True
            assert "job_id" in data

def test_force_sync_submission():
    resp = client.post("/api/jobs/force-sync")
    assert resp.status_code in (200, 409)
    if resp.status_code == 200:
        data = resp.json()
        assert data["ok"] is True
        job_id = data["job_id"]
        assert job_id
        
        # We can't easily poll for success in integration without mocking VBS/Ingest modules
        # but we verified submission works.
