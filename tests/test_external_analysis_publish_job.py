from __future__ import annotations

from app.backend.core import external_analysis_publish_job as publish_job_module
from external_analysis.runtime.load_control import ResearchLoadDecision


def test_external_analysis_publish_job_runs_background_request_when_outside_heavy_window(monkeypatch) -> None:
    updates: list[tuple[str, str, str]] = []
    captured: dict[str, object] = {}

    class _DummyJobManager:
        def _update_db(self, job_id, job_type, status, **kwargs):
            updates.append((str(job_id), str(job_type), str(status)))

        def update_status_cache_only(self, **kwargs):
            return None

    monkeypatch.setattr(publish_job_module, "job_manager", _DummyJobManager())
    monkeypatch.setattr(publish_job_module, "resolve_latest_external_analysis_as_of_date", lambda: 20260314)
    monkeypatch.setattr(
        publish_job_module,
        "evaluate_research_load_control",
        lambda: ResearchLoadDecision(
            mode="deferred",
            reason="outside_heavy_window",
            active_window_title=None,
            active_process_name=None,
            within_heavy_window=False,
            interaction_detected=False,
        ),
    )
    monkeypatch.setattr(
        publish_job_module,
        "run_nightly_candidate_pipeline",
        lambda **kwargs: (
            captured.update({"load_control": kwargs.get("load_control")}),
            {"ok": True, "baseline": {"publish_id": "pub_background"}},
        )[1],
    )

    publish_job_module.handle_external_analysis_publish_latest("job-1", {})

    assert updates[-1] == ("job-1", "external_analysis_publish_latest", "success")
    assert captured["load_control"] == {
        "mode": "throttled",
        "reason": "background_override_outside_heavy_window",
        "active_window_title": None,
        "active_process_name": None,
        "within_heavy_window": False,
        "interaction_detected": False,
    }


def test_external_analysis_publish_job_runs_manual_request_when_load_control_defers(monkeypatch) -> None:
    updates: list[tuple[str, str, str]] = []
    captured: dict[str, object] = {}

    class _DummyJobManager:
        def _update_db(self, job_id, job_type, status, **kwargs):
            updates.append((str(job_id), str(job_type), str(status)))

        def update_status_cache_only(self, **kwargs):
            return None

    monkeypatch.setattr(publish_job_module, "job_manager", _DummyJobManager())
    monkeypatch.setattr(publish_job_module, "resolve_latest_external_analysis_as_of_date", lambda: 20260314)
    monkeypatch.setattr(
        publish_job_module,
        "evaluate_research_load_control",
        lambda: ResearchLoadDecision(
            mode="deferred",
            reason="outside_heavy_window",
            active_window_title=None,
            active_process_name=None,
            within_heavy_window=False,
            interaction_detected=False,
        ),
    )
    monkeypatch.setattr(
        publish_job_module,
        "run_nightly_candidate_pipeline",
        lambda **kwargs: (
            captured.update({"load_control": kwargs.get("load_control")}),
            {"ok": True, "baseline": {"publish_id": "pub_manual"}},
        )[1],
    )

    publish_job_module.handle_external_analysis_publish_latest("job-2", {"source": "manual_api"})

    assert updates[-1] == ("job-2", "external_analysis_publish_latest", "success")
    assert captured["load_control"] == {
        "mode": "throttled",
        "reason": "manual_override_outside_heavy_window",
        "active_window_title": None,
        "active_process_name": None,
        "within_heavy_window": False,
        "interaction_detected": False,
    }


def test_external_analysis_publish_job_still_skips_non_window_deferred_reason(monkeypatch) -> None:
    updates: list[tuple[str, str, str]] = []

    class _DummyJobManager:
        def _update_db(self, job_id, job_type, status, **kwargs):
            updates.append((str(job_id), str(job_type), str(status)))

        def update_status_cache_only(self, **kwargs):
            return None

    monkeypatch.setattr(publish_job_module, "job_manager", _DummyJobManager())
    monkeypatch.setattr(publish_job_module, "resolve_latest_external_analysis_as_of_date", lambda: 20260314)
    monkeypatch.setattr(
        publish_job_module,
        "evaluate_research_load_control",
        lambda: ResearchLoadDecision(
            mode="deferred",
            reason="custom_deferred_reason",
            active_window_title=None,
            active_process_name=None,
            within_heavy_window=False,
            interaction_detected=False,
        ),
    )

    publish_job_module.handle_external_analysis_publish_latest("job-3", {})

    assert updates[-1] == ("job-3", "external_analysis_publish_latest", "skipped")


def test_schedule_external_analysis_publish_latest_submits_unique_job(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class _DummyJobManager:
        def submit(self, job_type, payload=None, unique=False, message=None, progress=None):
            captured["job_type"] = job_type
            captured["payload"] = dict(payload or {})
            captured["unique"] = unique
            captured["message"] = message
            captured["progress"] = progress
            return "job-submit-1"

    monkeypatch.setattr(publish_job_module, "job_manager", _DummyJobManager())

    job_id = publish_job_module.schedule_external_analysis_publish_latest(
        source="yf_daily_ingest:job-1",
        as_of=20260314,
    )

    assert job_id == "job-submit-1"
    assert captured == {
        "job_type": "external_analysis_publish_latest",
        "payload": {
            "source": "yf_daily_ingest:job-1",
            "freshness_state": "fresh",
            "as_of": 20260314,
        },
        "unique": True,
        "message": "Waiting in queue...",
        "progress": 0,
    }
