from __future__ import annotations

from datetime import date, timedelta

import pytest

from app.backend.core.legacy_analysis_control import LEGACY_ANALYSIS_DISABLE_ENV
import app.backend.services.tradex_experiment_service as service
from app.backend.services import tradex_research_preflight as preflight_service
from tests.test_tradex_research_os_phase1 import _hypothesis_payload


def _regime_rows_without_tag() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    current = date(2025, 1, 1)
    for regime_id in ("risk_on_trend", "risk_off_trend", "neutral_range"):
        for _ in range(60):
            rows.append(
                {
                    "dt": int(current.strftime("%Y%m%d")),
                    "regime_id": regime_id,
                    "regime_score": 0.1,
                    "label_version": service.TRADEX_EVAL_REGIME_LABEL_VERSION,
                }
            )
            current += timedelta(days=1)
    return rows


def test_missing_regime_tag_is_normalized(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(LEGACY_ANALYSIS_DISABLE_ENV, "0")
    monkeypatch.setattr(service, "_load_evaluation_regime_rows", lambda: (_regime_rows_without_tag(), []))
    hypothesis = _hypothesis_payload()
    report = preflight_service.evaluate_preflight(
        hypothesis=hypothesis,
        repo_commit="abc123",
        runner_version="tradex_research_os_runner_v1",
        started_at="2025-01-31T00:00:00+09:00",
    )
    assert report["passed"] is True
    assert report["status"] == preflight_service.TRADEX_RESEARCH_PREFLIGHT_REPORT_STATUS_PASSED
    assert report["checked_inputs"]["regime_row_normalization_applied"]
    assert report["checked_inputs"]["evaluation_window_count"] >= 3


def test_missing_regime_id_still_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(LEGACY_ANALYSIS_DISABLE_ENV, "0")
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

