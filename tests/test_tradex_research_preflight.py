from __future__ import annotations

from datetime import date, timedelta

import pytest

import app.backend.services.tradex_experiment_service as service
from app.backend.core.legacy_analysis_control import LEGACY_ANALYSIS_DISABLE_ENV
from app.backend.services import tradex_research_environment_readiness as readiness_service
from app.backend.services import tradex_research_preflight as preflight_service
from tests.test_tradex_research_os_phase1 import _hypothesis_payload


def _valid_regime_rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    current = date(2025, 1, 1)
    for regime_tag in ("up", "down", "flat"):
        for _ in range(3):
            rows.append({"dt": int(current.strftime("%Y%m%d")), "regime_id": f"{regime_tag}_01", "regime_tag": regime_tag, "regime_score": 0.1, "label_version": "tradex_eval_regime_v1"})
            current += timedelta(days=1)
    return rows


def _valid_windows() -> list[dict[str, object]]:
    return [
        {"evaluation_window_id": "up:1:3", "regime_tag": "up", "regime_id": "up_01", "start_date": "2025-01-01", "end_date": "2025-01-03", "trading_day_count": 3},
        {"evaluation_window_id": "down:4:6", "regime_tag": "down", "regime_id": "down_01", "start_date": "2025-01-04", "end_date": "2025-01-06", "trading_day_count": 3},
        {"evaluation_window_id": "flat:7:9", "regime_tag": "flat", "regime_id": "flat_01", "start_date": "2025-01-07", "end_date": "2025-01-09", "trading_day_count": 3},
    ]


def _evaluate(monkeypatch: pytest.MonkeyPatch, hypothesis: dict[str, object]) -> dict[str, object]:
    monkeypatch.setenv(LEGACY_ANALYSIS_DISABLE_ENV, "0")
    monkeypatch.setattr(
        readiness_service,
        "evaluate_environment_readiness",
        lambda: {
            "schema_version": "tradex_research_environment_readiness_v1",
            "environment_readiness_version": "v1",
            "runner": "tradex_research_session",
            "status": "ready",
            "ready": True,
            "cause_class": "ready",
            "cause_source": "environment_ready",
            "remediation_hint": "",
            "readiness_checks": [],
            "readiness_summary": {},
            "checked_at": "2025-01-31T00:00:00+09:00",
        },
    )
    return preflight_service.evaluate_preflight(
        hypothesis=hypothesis,
        repo_commit="abc123",
        runner_version="tradex_research_os_runner_v1",
        started_at="2025-01-31T00:00:00+09:00",
    )


def test_legacy_analysis_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(LEGACY_ANALYSIS_DISABLE_ENV, "1")
    hypothesis = _hypothesis_payload()
    monkeypatch.setattr(
        readiness_service,
        "evaluate_environment_readiness",
        lambda: {
            "schema_version": "tradex_research_environment_readiness_v1",
            "environment_readiness_version": "v1",
            "runner": "tradex_research_session",
            "status": "not_ready",
            "ready": False,
            "cause_class": "environment_not_ready",
            "cause_source": "legacy_analysis",
            "remediation_hint": f"set {LEGACY_ANALYSIS_DISABLE_ENV}=0 before running unshimmed TRADEX research",
            "readiness_checks": [],
            "readiness_summary": {},
            "checked_at": "2025-01-31T00:00:00+09:00",
        },
    )
    report = preflight_service.evaluate_preflight(
        hypothesis=hypothesis,
        repo_commit="abc123",
        runner_version="tradex_research_os_runner_v1",
        started_at="2025-01-31T00:00:00+09:00",
    )
    assert report["passed"] is False
    assert report["failure_code"] == "legacy_analysis_disabled"
    assert report["checked_inputs"]["environment_readiness"]["cause_class"] == "environment_not_ready"


def test_evaluation_windows_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(LEGACY_ANALYSIS_DISABLE_ENV, "0")
    monkeypatch.setattr(
        readiness_service,
        "evaluate_environment_readiness",
        lambda: {
            "schema_version": "tradex_research_environment_readiness_v1",
            "environment_readiness_version": "v1",
            "runner": "tradex_research_session",
            "status": "ready",
            "ready": True,
            "cause_class": "ready",
            "cause_source": "environment_ready",
            "remediation_hint": "",
            "readiness_checks": [],
            "readiness_summary": {},
            "checked_at": "2025-01-31T00:00:00+09:00",
        },
    )
    monkeypatch.setattr(service, "_load_evaluation_regime_rows", lambda: ([], ["market_regime_daily_unavailable:CatalogException"]))
    hypothesis = _hypothesis_payload()
    report = preflight_service.evaluate_preflight(
        hypothesis=hypothesis,
        repo_commit="abc123",
        runner_version="tradex_research_os_runner_v1",
        started_at="2025-01-31T00:00:00+09:00",
    )
    assert report["passed"] is False
    assert report["failure_code"] == "evaluation_windows_unavailable"
    assert report["checked_inputs"]["environment_readiness"]["cause_class"] == "ready"


def test_regime_rows_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(LEGACY_ANALYSIS_DISABLE_ENV, "0")
    monkeypatch.setattr(
        readiness_service,
        "evaluate_environment_readiness",
        lambda: {
            "schema_version": "tradex_research_environment_readiness_v1",
            "environment_readiness_version": "v1",
            "runner": "tradex_research_session",
            "status": "ready",
            "ready": True,
            "cause_class": "ready",
            "cause_source": "environment_ready",
            "remediation_hint": "",
            "readiness_checks": [],
            "readiness_summary": {},
            "checked_at": "2025-01-31T00:00:00+09:00",
        },
    )
    monkeypatch.setattr(service, "_load_evaluation_regime_rows", lambda: ([], ["market_regime_daily_empty"]))
    hypothesis = _hypothesis_payload()
    report = preflight_service.evaluate_preflight(
        hypothesis=hypothesis,
        repo_commit="abc123",
        runner_version="tradex_research_os_runner_v1",
        started_at="2025-01-31T00:00:00+09:00",
    )
    assert report["passed"] is False
    assert report["failure_code"] == "regime_rows_empty"


def test_insufficient_windows(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(LEGACY_ANALYSIS_DISABLE_ENV, "0")
    monkeypatch.setattr(
        readiness_service,
        "evaluate_environment_readiness",
        lambda: {
            "schema_version": "tradex_research_environment_readiness_v1",
            "environment_readiness_version": "v1",
            "runner": "tradex_research_session",
            "status": "ready",
            "ready": True,
            "cause_class": "ready",
            "cause_source": "environment_ready",
            "remediation_hint": "",
            "readiness_checks": [],
            "readiness_summary": {},
            "checked_at": "2025-01-31T00:00:00+09:00",
        },
    )
    monkeypatch.setattr(service, "_load_evaluation_regime_rows", lambda: (_valid_regime_rows(), []))
    monkeypatch.setattr(service, "_select_evaluation_windows", lambda regime_rows: (_valid_windows()[:2], ["missing_flat_window"]))
    hypothesis = _hypothesis_payload()
    report = preflight_service.evaluate_preflight(
        hypothesis=hypothesis,
        repo_commit="abc123",
        runner_version="tradex_research_os_runner_v1",
        started_at="2025-01-31T00:00:00+09:00",
    )
    assert report["passed"] is False
    assert report["failure_code"] == "insufficient_evaluation_windows"


def test_missing_required_inputs(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(LEGACY_ANALYSIS_DISABLE_ENV, "0")
    monkeypatch.setattr(
        readiness_service,
        "evaluate_environment_readiness",
        lambda: {
            "schema_version": "tradex_research_environment_readiness_v1",
            "environment_readiness_version": "v1",
            "runner": "tradex_research_session",
            "status": "ready",
            "ready": True,
            "cause_class": "ready",
            "cause_source": "environment_ready",
            "remediation_hint": "",
            "readiness_checks": [],
            "readiness_summary": {},
            "checked_at": "2025-01-31T00:00:00+09:00",
        },
    )
    hypothesis = _hypothesis_payload()
    hypothesis["execution"] = dict(hypothesis["execution"], runner="wrong_runner")
    report = preflight_service.evaluate_preflight(
        hypothesis=hypothesis,
        repo_commit="abc123",
        runner_version="tradex_research_os_runner_v1",
        started_at="2025-01-31T00:00:00+09:00",
    )
    assert report["passed"] is False
    assert report["failure_code"] == "missing_required_inputs"


def test_artifact_shape_unrecognized(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(LEGACY_ANALYSIS_DISABLE_ENV, "0")
    monkeypatch.setattr(
        readiness_service,
        "evaluate_environment_readiness",
        lambda: {
            "schema_version": "tradex_research_environment_readiness_v1",
            "environment_readiness_version": "v1",
            "runner": "tradex_research_session",
            "status": "ready",
            "ready": True,
            "cause_class": "ready",
            "cause_source": "environment_ready",
            "remediation_hint": "",
            "readiness_checks": [],
            "readiness_summary": {},
            "checked_at": "2025-01-31T00:00:00+09:00",
        },
    )
    monkeypatch.setattr(service, "_load_evaluation_regime_rows", lambda: ([{"dt": 20250101, "regime_tag": "up"}], []))
    hypothesis = _hypothesis_payload()
    report = preflight_service.evaluate_preflight(
        hypothesis=hypothesis,
        repo_commit="abc123",
        runner_version="tradex_research_os_runner_v1",
        started_at="2025-01-31T00:00:00+09:00",
    )
    assert report["passed"] is False
    assert report["failure_code"] == "artifact_shape_unrecognized"


def test_valid_preflight_pass(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(LEGACY_ANALYSIS_DISABLE_ENV, "0")
    monkeypatch.setattr(
        readiness_service,
        "evaluate_environment_readiness",
        lambda: {
            "schema_version": "tradex_research_environment_readiness_v1",
            "environment_readiness_version": "v1",
            "runner": "tradex_research_session",
            "status": "ready",
            "ready": True,
            "cause_class": "ready",
            "cause_source": "environment_ready",
            "remediation_hint": "",
            "readiness_checks": [],
            "readiness_summary": {},
            "checked_at": "2025-01-31T00:00:00+09:00",
        },
    )
    monkeypatch.setattr(service, "_load_evaluation_regime_rows", lambda: (_valid_regime_rows(), []))
    monkeypatch.setattr(service, "_select_evaluation_windows", lambda regime_rows: (_valid_windows(), []))
    hypothesis = _hypothesis_payload()
    report = _evaluate(monkeypatch, hypothesis)
    assert report["passed"] is True
    assert report["status"] == preflight_service.TRADEX_RESEARCH_PREFLIGHT_REPORT_STATUS_PASSED
    assert report["failure_code"] == ""
    assert report["provisional_experiment_id"].startswith("exp_preflight_")
    assert report["checked_inputs"]["environment_readiness"]["cause_class"] == "ready"
