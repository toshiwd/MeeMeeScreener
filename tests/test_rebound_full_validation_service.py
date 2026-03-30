from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.backend.services.analysis import rebound_full_validation_service


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _diagnosis_payload(*, primary_failure_axis: str) -> dict:
    return {
        "period": {
            "start_date": "2024-03-29",
            "end_date": "2026-03-29",
        },
        "primary_failure_axis": primary_failure_axis,
        "variant_results": [
            {"variant_name": "base_current", "total_return_pct": -20.0, "max_drawdown_pct": -21.0, "worst_month_pct": -11.0},
            {"variant_name": "holdings_1", "total_return_pct": -4.0, "max_drawdown_pct": -6.0, "worst_month_pct": -3.0},
            {"variant_name": "holdings_2", "total_return_pct": -9.0, "max_drawdown_pct": -12.0, "worst_month_pct": -6.0},
            {"variant_name": "turnover_tight", "total_return_pct": -15.0, "max_drawdown_pct": -16.0, "worst_month_pct": -8.0},
            {"variant_name": "gate_disabled_for_diagnosis", "total_return_pct": -18.0, "max_drawdown_pct": -19.0, "worst_month_pct": -10.0},
        ],
    }


def test_decide_validation_outcome_adopts_soft_bonus_only() -> None:
    decision = rebound_full_validation_service._decide_validation_outcome(  # type: ignore[attr-defined]
        monitor_summary={
            "candidate_variant": "soft_bonus_only",
            "days_with_bonus_delta_vs_baseline": 2,
            "max_entry_rank_changed_count": 4,
            "median_entry_rank_changed_count": 1.0,
        },
        diagnosis_summary={
            "primary_failure_axis": "turnover",
            "base_current": {"total_return_pct": -20.0},
            "holdings_1": {"total_return_pct": -10.0},
            "holdings_2": {"total_return_pct": -12.0},
            "turnover_tight": {"total_return_pct": -8.0},
            "gate_disabled_for_diagnosis": {"total_return_pct": -9.0},
        },
    )

    assert decision["decision"] == "adopt_soft_bonus_only"
    assert decision["recommended_policy"] == "soft_bonus_only"


def test_decide_validation_outcome_keeps_tag_centered_when_delta_is_zero() -> None:
    decision = rebound_full_validation_service._decide_validation_outcome(  # type: ignore[attr-defined]
        monitor_summary={
            "candidate_variant": "soft_bonus_only",
            "days_with_bonus_delta_vs_baseline": 0,
            "max_entry_rank_changed_count": 3,
            "median_entry_rank_changed_count": 0.0,
        },
        diagnosis_summary={
            "primary_failure_axis": "risk_gate_only",
            "base_current": {"total_return_pct": -20.0},
            "holdings_1": {"total_return_pct": -19.5},
            "holdings_2": {"total_return_pct": -19.8},
            "turnover_tight": {"total_return_pct": -19.0},
            "gate_disabled_for_diagnosis": {"total_return_pct": -18.5},
        },
    )

    assert decision["decision"] == "keep_tag_centered"
    assert decision["recommended_policy"] == "tag_only_keep_bonus_as_is"


def test_decide_validation_outcome_prioritizes_holdings_fix() -> None:
    decision = rebound_full_validation_service._decide_validation_outcome(  # type: ignore[attr-defined]
        monitor_summary={
            "candidate_variant": "soft_bonus_only",
            "days_with_bonus_delta_vs_baseline": 1,
            "max_entry_rank_changed_count": 4,
            "median_entry_rank_changed_count": 1.0,
        },
        diagnosis_summary={
            "primary_failure_axis": "holdings",
            "base_current": {"total_return_pct": -20.0},
            "holdings_1": {"total_return_pct": -4.0},
            "holdings_2": {"total_return_pct": -8.0},
            "turnover_tight": {"total_return_pct": -15.0},
            "gate_disabled_for_diagnosis": {"total_return_pct": -18.0},
        },
    )

    assert decision["decision"] == "fix_holdings_before_policy_change"
    assert decision["recommended_policy"] == "holdings_fix_first"


def test_run_rebound_full_validation_serializes_and_writes_summary(monkeypatch, tmp_path: Path) -> None:
    execution_order: list[str] = []
    monitor_dir = tmp_path / "monitor"
    diagnosis_dir = tmp_path / "diagnosis"

    monkeypatch.setattr(
        rebound_full_validation_service,
        "_check_target_db_lock",
        lambda _db_path: {
            "status": "ok",
            "db_path": "C:/db/stocks.duckdb",
            "blocking_processes": [],
        },
    )

    def _fake_monitor(*, dataset_id: str, days: int, output_root: Path):
        assert dataset_id == "monthly-event-meemee-registered-sample100-v12"
        assert days == 60
        assert output_root == monitor_dir
        execution_order.append("monitor")
        payload = {
            "baseline_variant": "baseline_live",
            "candidate_variant": "soft_bonus_only",
            "summary": {
                "baseline": {"days_with_bonus": 2, "median_bonus_candidate_count": 1.0},
                "candidate": {
                    "days_with_bonus": 4,
                    "median_bonus_candidate_count": 1.0,
                    "median_entry_rank_changed_count": 2.0,
                    "max_entry_rank_changed_count": 5,
                },
                "comparison": {"days_with_bonus_delta_vs_baseline": 2},
            },
        }
        _write_json(output_root / "rebound_live_monitor.json", payload)
        (output_root / "rebound_live_monitor.md").write_text("# monitor\n", encoding="utf-8")
        return {
            "rebound_live_monitor_path": str(output_root / "rebound_live_monitor.json"),
            "rebound_live_monitor_report_path": str(output_root / "rebound_live_monitor.md"),
        }

    def _fake_diagnosis(*, start_date=None, end_date=None, output_dir: Path):
        assert execution_order == ["monitor"]
        assert output_dir == diagnosis_dir
        execution_order.append("diagnosis")
        payload = _diagnosis_payload(primary_failure_axis="turnover")
        _write_json(output_dir / "toredex_policy_diagnosis.json", payload)
        (output_dir / "toredex_policy_diagnosis.md").write_text("# diagnosis\n", encoding="utf-8")
        return {
            "run_id": "diagnosis-run",
            "toredex_policy_diagnosis_path": str(output_dir / "toredex_policy_diagnosis.json"),
            "toredex_policy_diagnosis_report_path": str(output_dir / "toredex_policy_diagnosis.md"),
            "primary_failure_axis": "turnover",
        }

    monkeypatch.setattr(rebound_full_validation_service, "build_event_image_rebound_live_monitor", _fake_monitor)
    monkeypatch.setattr(rebound_full_validation_service, "run_toredex_policy_diagnosis", _fake_diagnosis)

    result = rebound_full_validation_service.run_rebound_full_validation(output_dir=tmp_path)

    assert execution_order == ["monitor", "diagnosis"]
    summary_path = tmp_path / "rebound_full_validation_summary.json"
    assert result["decision"] == "adopt_soft_bonus_only"
    assert summary_path.exists()
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["monitor_run_id"] == "monitor"
    assert summary["diagnosis_run_id"] == "diagnosis-run"
    assert summary["lock_check"]["status"] == "ok"
    assert summary["wall_clock_seconds"] >= 0.0
    metadata = json.loads((tmp_path / "batch_metadata.json").read_text(encoding="utf-8"))
    assert metadata["status"] == "completed"


def test_run_rebound_full_validation_stops_after_monitor_failure(monkeypatch, tmp_path: Path) -> None:
    calls: list[str] = []
    monkeypatch.setattr(
        rebound_full_validation_service,
        "_check_target_db_lock",
        lambda _db_path: {
            "status": "ok",
            "db_path": "C:/db/stocks.duckdb",
            "blocking_processes": [],
        },
    )

    def _failing_monitor(**_kwargs):
        calls.append("monitor")
        raise RuntimeError("monitor failed")

    def _unexpected_diagnosis(**_kwargs):
        calls.append("diagnosis")
        raise AssertionError("diagnosis should not run after monitor failure")

    monkeypatch.setattr(rebound_full_validation_service, "build_event_image_rebound_live_monitor", _failing_monitor)
    monkeypatch.setattr(rebound_full_validation_service, "run_toredex_policy_diagnosis", _unexpected_diagnosis)

    with pytest.raises(RuntimeError, match="monitor failed"):
        rebound_full_validation_service.run_rebound_full_validation(output_dir=tmp_path)

    assert calls == ["monitor"]
    assert not (tmp_path / "rebound_full_validation_summary.json").exists()
    metadata = json.loads((tmp_path / "batch_metadata.json").read_text(encoding="utf-8"))
    assert metadata["status"] == "failed"
    assert metadata["failed_step"] == "monitor"


def test_run_rebound_full_validation_fails_fast_on_db_lock(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        rebound_full_validation_service,
        "_check_target_db_lock",
        lambda _db_path: {
            "status": "blocked",
            "db_path": "C:/db/stocks.duckdb",
            "blocking_processes": [{"pid": 1234, "name": "python.exe", "command_line": "python -m app.backend.tools.run_rebound_monitor"}],
        },
    )
    monkeypatch.setattr(
        rebound_full_validation_service,
        "build_event_image_rebound_live_monitor",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("monitor should not run when lock check fails")),
    )

    with pytest.raises(RuntimeError, match="target DB is locked"):
        rebound_full_validation_service.run_rebound_full_validation(output_dir=tmp_path)

    metadata = json.loads((tmp_path / "batch_metadata.json").read_text(encoding="utf-8"))
    assert metadata["status"] == "failed"
    assert metadata["failed_step"] == "lock_check"
    assert metadata["lock_check"]["status"] == "blocked"
