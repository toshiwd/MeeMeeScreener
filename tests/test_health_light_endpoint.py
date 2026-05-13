import json
import os
import sys
from unittest.mock import patch

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from app.backend.api.routers import health as health_router


def test_health_light_skips_txt_status_collection():
    readiness = {
        "missing_tables": [],
        "errors": [],
        "db_retryable": False,
        "db_connect_stats": {"open_calls": 1},
        "readiness_state": {"boot_ready": True, "db_ready": True},
    }

    with (
        patch.object(health_router, "_HEALTH_LIGHT", True),
        patch.object(health_router, "_collect_db_readiness", return_value=readiness),
        patch.object(health_router, "get_txt_status", side_effect=AssertionError("should not be called")),
    ):
        payload = health_router.health()

    assert payload["ok"] is True
    assert payload["ready"] is True
    assert payload["transient_db_busy"] is False
    assert payload["txt_count"] is None
    assert payload["last_updated"] is None
    assert payload["code_txt_missing"] is None


def test_health_live_is_db_independent():
    with patch.object(
        health_router,
        "_collect_db_readiness",
        side_effect=AssertionError("should not be called"),
    ):
        payload = health_router.health_live()

    assert payload["ok"] is True
    assert payload["status"] == "ok"
    assert payload["ready"] is True
    assert payload["phase"] == "alive"
    assert payload["message"] == "alive"


def test_health_returns_not_ready_200_when_db_temporarily_busy_after_boot():
    readiness = {
        "missing_tables": [],
        "errors": ["db_unavailable"],
        "db_retryable": True,
        "db_connect_stats": {"open_calls": 3},
        "readiness_state": {"boot_ready": True, "db_ready": True},
    }

    with (
        patch.object(health_router, "_HEALTH_LIGHT", True),
        patch.object(health_router, "_collect_db_readiness", return_value=readiness),
    ):
        response = health_router.health()

    assert response.status_code == 200
    payload = json.loads(response.body)
    assert payload["ok"] is False
    assert payload["status"] == "db_busy"
    assert payload["ready"] is False
    assert payload["phase"] == "db_busy"
    assert payload["message"] == "database is temporarily busy"
    assert payload["retryAfterMs"] == 1000
    assert payload["db_retryable"] is True
    assert payload["transient_db_busy"] is True
    assert payload["errors"] == ["db_unavailable"]


def test_health_deep_returns_not_ready_200_when_db_temporarily_busy_after_boot():
    stats = {
        "tickers": None,
        "daily_rows": None,
        "monthly_rows": None,
        "missing_tables": [],
        "errors": ["db_unavailable"],
        "db_retryable": True,
        "db_connect_stats": {"open_calls": 3},
    }

    with (
        patch.object(health_router, "_collect_db_stats", return_value=stats),
        patch.object(health_router, "get_readiness_state", return_value={"boot_ready": True, "db_ready": True}),
        patch.object(health_router, "get_txt_status", return_value={"txt_count": None, "last_updated": None, "code_txt_missing": None}),
    ):
        response = health_router.health_deep()

    assert response.status_code == 200
    payload = json.loads(response.body)
    assert payload["ok"] is False
    assert payload["status"] == "db_busy"
    assert payload["ready"] is False
    assert payload["phase"] == "db_busy"
    assert payload["message"] == "database is temporarily busy"
    assert payload["retryAfterMs"] == 1000
    assert payload["db_retryable"] is True
    assert payload["transient_db_busy"] is True
    assert payload["errors"] == ["db_unavailable"]
