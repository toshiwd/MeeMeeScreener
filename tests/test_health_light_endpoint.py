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


def test_health_returns_degraded_not_503_when_db_temporarily_busy():
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
        payload = health_router.health()

    assert payload["ok"] is True
    assert payload["status"] == "degraded"
    assert payload["ready"] is True
    assert payload["message"] == "backend ready (database busy)"
