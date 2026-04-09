from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.backend.services import tradex_research_decision_policy as decision_policy
from app.backend.services import tradex_research_os_contracts as os_contracts
from app.backend.services import tradex_research_preflight as preflight_service
from app.backend.tools import tradex_research_os_runner as os_runner
from tests.test_tradex_research_os_phase1 import _hypothesis_payload


def test_load_hypothesis_missing_file_is_explicit(tmp_path: Path) -> None:
    missing_path = tmp_path / "missing-hypothesis.json"
    with pytest.raises(FileNotFoundError, match="hypothesis missing"):
        os_runner.load_hypothesis(missing_path)


def test_load_hypothesis_malformed_json_is_parse_error(tmp_path: Path) -> None:
    hypothesis_path = tmp_path / "broken-hypothesis.json"
    hypothesis_path.write_text("{not-json", encoding="utf-8")
    with pytest.raises(ValueError, match="parse error"):
        os_runner.load_hypothesis(hypothesis_path)


def test_load_hypothesis_invalid_schema_stays_schema_error(tmp_path: Path) -> None:
    hypothesis_path = tmp_path / "invalid-schema-hypothesis.json"
    hypothesis_path.write_text(json.dumps({"schema_version": os_contracts.TRADEX_RESEARCH_OS_HYPOTHESIS_SCHEMA_VERSION}, ensure_ascii=False), encoding="utf-8")
    with pytest.raises(ValueError, match="missing required fields"):
        os_runner.load_hypothesis(hypothesis_path)


def test_cli_validate_hypothesis_reports_operator_error(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    hypothesis_path = tmp_path / "broken-hypothesis.json"
    hypothesis_path.write_text("{not-json", encoding="utf-8")
    status = os_runner.main(["validate-hypothesis", "--hypothesis-path", str(hypothesis_path)])
    captured = capsys.readouterr()
    assert status == 2
    assert "error:" in captured.err
    assert "parse error" in captured.err
    assert captured.out == ""


def test_cli_validate_hypothesis_success(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    hypothesis_path = tmp_path / "valid-hypothesis.json"
    hypothesis_path.write_text(json.dumps(_hypothesis_payload(), ensure_ascii=False, indent=2), encoding="utf-8")
    status = os_runner.main(["validate-hypothesis", "--hypothesis-path", str(hypothesis_path)])
    captured = capsys.readouterr()
    assert status == 0
    assert '"hypothesis_id": "hypothesis-regime-aware-v1"' in captured.out
    assert captured.err == ""


def test_malformed_decision_policy_is_parse_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    policy_path = tmp_path / "decision_policy_v1.json"
    policy_path.write_text("{broken-json", encoding="utf-8")
    monkeypatch.setattr(decision_policy, "decision_policy_path", lambda: policy_path)
    with pytest.raises(ValueError, match="parse error"):
        decision_policy.load_decision_policy()


def test_malformed_preflight_policy_is_parse_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    policy_path = tmp_path / "preflight_policy_v1.json"
    policy_path.write_text("{broken-json", encoding="utf-8")
    monkeypatch.setattr(preflight_service, "preflight_policy_path", lambda: policy_path)
    with pytest.raises(ValueError, match="parse error"):
        preflight_service.load_preflight_policy()
