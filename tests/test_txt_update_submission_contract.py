import json
import os
import sys
from datetime import datetime, timezone
from unittest.mock import patch

from fastapi.responses import JSONResponse

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from app.backend.api.routers import jobs, system


def _json_body(response: JSONResponse) -> dict:
    return json.loads(response.body.decode("utf-8"))


def test_submit_txt_update_job_returns_canonical_payload():
    with (
        patch("app.backend.api.routers.jobs.os.path.isfile", return_value=True),
        patch("app.backend.api.routers.jobs.cleanup_stale_jobs"),
        patch("app.backend.api.routers.jobs._count_active_jobs", return_value=0),
        patch("app.backend.api.routers.jobs.job_manager.submit", return_value="job-123") as mock_submit,
    ):
        payload = jobs.submit_txt_update_job(
            {"auto_ml_predict": True, "auto_ml_train": False},
            source="/api/jobs/txt-update",
        )

    assert isinstance(payload, dict)
    assert payload["ok"] is True
    assert payload["started"] is True
    assert payload["status"] == "accepted"
    assert payload["type"] == "txt_update"
    assert payload["state"] == "queued"
    assert payload["job_id"] == "job-123"
    assert payload["jobId"] == "job-123"
    mock_submit.assert_called_once_with(
        "txt_update",
        {"auto_ml_predict": True, "auto_ml_train": False},
        unique=True,
    )


def test_submit_txt_update_job_returns_conflict_payload():
    with (
        patch("app.backend.api.routers.jobs.os.path.isfile", return_value=True),
        patch("app.backend.api.routers.jobs.cleanup_stale_jobs"),
        patch("app.backend.api.routers.jobs._count_active_jobs", return_value=1),
    ):
        response = jobs.submit_txt_update_job({}, source="/api/jobs/txt-update")

    assert isinstance(response, JSONResponse)
    assert response.status_code == 409
    payload = _json_body(response)
    assert payload["ok"] is False
    assert payload["status"] == "conflict"
    assert payload["error"] == "update_in_progress"
    assert payload["type"] == "txt_update"


def test_submit_txt_update_job_legacy_adds_deprecation_headers():
    with (
        patch("app.backend.api.routers.jobs.os.path.isfile", return_value=True),
        patch("app.backend.api.routers.jobs.cleanup_stale_jobs"),
        patch("app.backend.api.routers.jobs._count_active_jobs", return_value=0),
        patch("app.backend.api.routers.jobs.job_manager.submit", return_value="job-legacy"),
    ):
        response = jobs.submit_txt_update_job(
            {},
            source="/api/txt_update/run",
            legacy_endpoint="/api/txt_update/run",
        )

    assert isinstance(response, JSONResponse)
    assert response.status_code == 200
    payload = _json_body(response)
    assert payload["job_id"] == "job-legacy"
    assert response.headers.get("Deprecation") == "true"
    assert response.headers.get("Sunset") == "Tue, 30 Jun 2026 00:00:00 GMT"
    assert "/api/jobs/txt-update" in (response.headers.get("Link") or "")


def test_submit_txt_update_job_legacy_can_be_disabled_with_env_flag():
    with (
        patch.dict("os.environ", {"MEEMEE_DISABLE_LEGACY_TXT_UPDATE_ENDPOINTS": "1"}, clear=False),
    ):
        response = jobs.submit_txt_update_job(
            {},
            source="/api/txt_update/run",
            legacy_endpoint="/api/txt_update/run",
        )

    assert isinstance(response, JSONResponse)
    assert response.status_code == 410
    payload = _json_body(response)
    assert payload["error"] == "legacy_endpoint_removed"
    assert payload["successor_endpoint"] == "/api/jobs/txt-update"
    assert response.headers.get("Deprecation") == "true"


def test_legacy_sunset_boundary_check():
    before = datetime(2026, 6, 29, 23, 59, 59, tzinfo=timezone.utc)
    at_sunset = datetime(2026, 6, 30, 0, 0, 0, tzinfo=timezone.utc)

    assert jobs._legacy_endpoint_sunset_reached(before) is False
    assert jobs._legacy_endpoint_sunset_reached(at_sunset) is True


def test_legacy_sunset_boundary_check_accepts_naive_datetime():
    before_naive = datetime(2026, 6, 29, 23, 59, 59)
    at_sunset_naive = datetime(2026, 6, 30, 0, 0, 0)

    assert jobs._legacy_endpoint_sunset_reached(before_naive) is False
    assert jobs._legacy_endpoint_sunset_reached(at_sunset_naive) is True


def test_submit_txt_update_job_legacy_can_be_disabled_by_sunset():
    with patch("app.backend.api.routers.jobs._legacy_endpoint_sunset_reached", return_value=True):
        response = jobs.submit_txt_update_job(
            {},
            source="/api/txt_update/run",
            legacy_endpoint="/api/txt_update/run",
        )

    assert isinstance(response, JSONResponse)
    assert response.status_code == 410
    payload = _json_body(response)
    assert payload["error"] == "legacy_endpoint_removed"
    assert payload["successor_endpoint"] == "/api/jobs/txt-update"


def test_legacy_route_uses_common_submitter():
    with patch("app.backend.api.routers.jobs.submit_txt_update_job", return_value={"ok": True}) as mock_submit:
        response = jobs.run_txt_update_legacy()

    assert response == {"ok": True}
    mock_submit.assert_called_once_with(
        {},
        source="/api/txt_update/run",
        legacy_endpoint="/api/txt_update/run",
    )


def test_system_update_data_uses_common_submitter():
    with patch(
        "app.backend.api.routers.system.submit_txt_update_job", return_value={"ok": True, "job_id": "job-1"}
    ) as mock_submit:
        response = system.trigger_update_data()

    assert response == {"ok": True, "job_id": "job-1"}
    mock_submit.assert_called_once_with(
        {},
        source="/api/system/update_data",
        legacy_endpoint="/api/system/update_data",
    )


def test_submit_txt_update_includes_force_recompute_on_pan_finalize_flag():
    with patch("app.backend.api.routers.jobs.submit_txt_update_job", return_value={"ok": True}) as mock_submit:
        response = jobs.submit_txt_update(
            auto_ml_predict=False,
            auto_ml_train=False,
            force_recompute_on_pan_finalize=False,
        )

    assert response == {"ok": True}
    submitted_payload = mock_submit.call_args.args[0]
    assert submitted_payload["completion_mode"] == "full"
    assert submitted_payload["auto_ml_predict"] is False
    assert submitted_payload["auto_ml_train"] is False
    assert submitted_payload["force_recompute_on_pan_finalize"] is False


def test_submit_txt_update_forwards_practical_fast_completion_mode():
    with patch("app.backend.api.routers.jobs.submit_txt_update_job", return_value={"ok": True}) as mock_submit:
        response = jobs.submit_txt_update(
            completion_mode="practical_fast",
            auto_ml_predict=True,
            auto_ml_train=True,
        )

    assert response == {"ok": True}
    submitted_payload = mock_submit.call_args.args[0]
    assert submitted_payload["completion_mode"] == "practical_fast"


def test_walkforward_latest_response_keeps_summary_and_adds_attribution():
    latest_payload = {
        "has_run": True,
        "latest": {
            "run_id": "swf_20260306010101",
            "finished_at": "2026-03-06T01:01:01+00:00",
            "status": "success",
            "report": {
                "summary": {
                    "windows_total": 3,
                    "executed_windows": 3,
                },
                "attribution": {
                    "code": {"rows": [{"key": "1301", "trades": 5}]},
                    "sector33_code": {"rows": [{"key": "16", "trades": 12}]},
                    "setup_id": {"rows": [{"key": "long_breakout_p2", "trades": 8}]},
                    "side": {"rows": [{"key": "long", "trades": 20}]},
                },
            },
        },
    }
    with patch(
        "app.backend.api.routers.jobs.strategy_backtest_service.get_latest_strategy_walkforward",
        return_value=latest_payload,
    ):
        response = jobs.get_strategy_walkforward_latest()

    assert isinstance(response, dict)
    assert response["has_run"] is True
    assert response["latest"]["report"]["summary"]["windows_total"] == 3
    assert response["latest"]["report"]["attribution"]["code"]["rows"][0]["key"] == "1301"


def test_walkforward_research_latest_route_forwards_payload():
    research_payload = {
        "has_snapshot": True,
        "latest": {
            "snapshot_date": 20260306,
            "source_run_id": "swf_20260306010101",
            "report": {
                "adopted_setups": [{"setup_id": "long_breakout_p2", "trades": 8}],
                "rejected_reasons": [{"reason": "insufficient_window_rows", "count": 2}],
                "hedge_contribution": {"hedge_share": 0.27},
            },
        },
    }
    with patch(
        "app.backend.api.routers.jobs.strategy_backtest_service.get_latest_strategy_walkforward_research_snapshot",
        return_value=research_payload,
    ):
        response = jobs.get_strategy_walkforward_research_latest()

    assert response == research_payload
