
import sys
import os
import time
import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock, PropertyMock

# Setup path to import backend modules
BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "app", "backend"))
sys.path.append(BACKEND_DIR)

# Mocking config/env early if needed
os.environ["PAN_CODE_TXT_PATH"] = "dummy_code.txt"

from pathlib import Path

# Import app - this might trigger startup logic (db init) so we might want to mock things if possible
# But for integration, using real DB (sqlite legacy) is okay if path defaults to temp or we mock it.
# main.py does 'from db import ...' at module level.
with patch("core.config.config.DATA_DIR", new=Path(".")), \
     patch("core.config.AppConfig.DB_PATH", new_callable=PropertyMock) as mock_db_path,\
     patch("core.config.AppConfig.PAN_CODE_TXT_PATH", new_callable=PropertyMock) as mock_code_path,\
     patch("core.config.AppConfig.PAN_EXPORT_VBS_PATH", new_callable=PropertyMock) as mock_vbs_path:
     mock_db_path.return_value = ":memory:"
     mock_code_path.return_value = Path("dummy_code.txt")
     mock_vbs_path.return_value = Path("dummy_vbs.vbs")
     from main import app
     from core.jobs import job_manager

client = TestClient(app)

def test_txt_update_submission_flow():
    # Submit job
    resp = client.post("/api/jobs/txt-update")
    assert resp.status_code in (200, 409)
    
    if resp.status_code == 200:
        data = resp.json()
        assert data["ok"] is True
        job_id = data["job_id"]
        assert job_id
        
        # Check status
        start_time = time.time()
        while time.time() - start_time < 2:
            resp_status = client.get("/api/txt_update/status")
            assert resp_status.status_code == 200
            status_data = resp_status.json()
            # Just verify keys exist
            assert "running" in status_data
            assert "phase" in status_data
            if status_data["running"]:
                # Try submitting duplicate
                resp_dup = client.post("/api/jobs/txt-update")
                assert resp_dup.status_code == 409
                break
            time.sleep(0.1)

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
