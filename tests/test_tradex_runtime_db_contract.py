from __future__ import annotations

import pytest

from app.backend.tools import tradex_research_runner as research_runner


def test_runtime_db_contract_validation_passes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        research_runner.tradex_environment_readiness,
        "evaluate_environment_readiness",
        lambda: {
            "ready": True,
            "cause_class": "ready",
            "cause_source": "environment_ready",
            "checked_at": "2026-04-17T00:00:00+00:00",
            "remediation_hint": "",
            "readiness_summary": {
                "database_path": r"C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb",
                "required_table": "market_regime_daily",
                "table_exists": True,
                "table_row_count": 770,
                "label_version_row_count": 770,
                "selected_window_count": 3,
                "selected_window_issues": [],
            },
        },
    )

    report = research_runner._validate_runtime_db_contract()

    assert report["ready"] is True
    assert report["database_path"].endswith(r"MeeMeeScreener-dev\data\stocks.duckdb")
    assert report["required_table"] == "market_regime_daily"
    assert report["table_exists"] is True
    assert report["selected_window_count"] == 3


def test_runtime_db_contract_validation_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        research_runner.tradex_environment_readiness,
        "evaluate_environment_readiness",
        lambda: {
            "ready": False,
            "cause_class": "genuine_data_unavailable",
            "cause_source": "evaluation_window_probe",
            "checked_at": "2026-04-17T00:00:00+00:00",
            "remediation_hint": "populate market_regime_daily",
            "readiness_summary": {
                "database_path": r"G:\Tradex\db\stocks.duckdb",
                "required_table": "market_regime_daily",
                "table_exists": True,
                "table_row_count": 1,
                "label_version_row_count": 0,
                "selected_window_count": 0,
                "selected_window_issues": ["missing_up_window"],
            },
        },
    )

    with pytest.raises(RuntimeError, match="TRADEX runtime DB contract failed"):
        research_runner._validate_runtime_db_contract()
