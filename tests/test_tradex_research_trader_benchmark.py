from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.backend.services import tradex_research_os_contracts as os_contracts
from app.backend.services import tradex_research_os_store as os_store
from app.backend.services import tradex_research_trader_benchmark as benchmark_service
from app.backend.tools import tradex_research_os_runner as os_runner
from tests.test_tradex_research_os_phase1 import _reset_tradex_root


def _adapter_output(
    *,
    adapter_id: str,
    action: str,
    judgement: str,
    buy_score: float,
    confidence: float,
) -> dict[str, object]:
    return {
        "adapter_id": adapter_id,
        "adapter_kind": "classical" if adapter_id == "numeric_baseline_v1" else "llm_structured",
        "machine_action_state": action,
        "human_readable_judgement": judgement,
        "buy_score": buy_score,
        "environment_score": 0.62,
        "trend_score": 0.58,
        "trigger_score": 0.71,
        "risk_score": 0.49,
        "invalidation_price": 120.0,
        "invalidation_reason_code": "daily_swing_low_break",
        "reason_codes": ["close_breakout_20", "ma_trend_aligned"],
        "explanation": "benchmark fixture",
        "confidence": confidence,
    }


def _write_success_experiment(
    *,
    experiment_id: str,
    hypothesis_id: str,
    family_id: str,
    method_family: str,
    code: str,
    as_of_date: int,
    adapter_outputs: list[dict[str, object]],
    primary_adapter_id: str,
    complete_horizon: bool = True,
    return_close_basis: float | None = 0.1,
    return_next_open_basis: float | None = 0.08,
    mfe: float | None = 0.15,
    mae: float | None = -0.05,
) -> None:
    generated_at = "2026-04-09T00:00:00+00:00"
    target = {
        "code": code,
        "as_of_date": as_of_date,
        "side": "long",
        "judgement_type": "close_based_daily_buy_v1",
    }
    observation_snapshot = os_contracts.build_observation_snapshot(
        experiment_id=experiment_id,
        hypothesis_id=hypothesis_id,
        target=target,
        observation_contract_version="tradex_close_based_buy_judgement_v1",
        confirmed_bar={"market_date": as_of_date, "open": 100.0, "high": 125.0, "low": 98.0, "close": 122.0, "volume": 1_000_000.0},
        recent_bars=[{"market_date": as_of_date - 1, "open": 99.0, "high": 101.0, "low": 98.0, "close": 100.0, "volume": 900_000.0}],
        derived_features={"moving_averages": {"ma20": 110.0}, "breakout_context": {"close_above_prior_high_20": True}},
        market_context={"price_source": "daily_bars", "teacher_horizon_bars": 20, "future_bar_count": 20 if complete_horizon else 8},
        lineage={"source_method": "fixture", "source_code": code, "anchor_market_date": as_of_date},
        generated_at=generated_at,
    )
    primary_output = next(row for row in adapter_outputs if str(row["adapter_id"]) == primary_adapter_id)
    strategy_judgement = os_contracts.build_strategy_judgement(
        experiment_id=experiment_id,
        hypothesis_id=hypothesis_id,
        target=target,
        primary_adapter_id=primary_adapter_id,
        machine_action_state=str(primary_output["machine_action_state"]),
        human_readable_judgement=str(primary_output["human_readable_judgement"]),
        buy_score=float(primary_output["buy_score"]),
        environment_score=float(primary_output["environment_score"]),
        trend_score=float(primary_output["trend_score"]),
        trigger_score=float(primary_output["trigger_score"]),
        risk_score=float(primary_output["risk_score"]),
        invalidation_price=float(primary_output["invalidation_price"]),
        invalidation_reason_code=str(primary_output["invalidation_reason_code"]),
        reason_codes=[str(item) for item in primary_output["reason_codes"]],
        adapter_outputs=[dict(item) for item in adapter_outputs],
        observation_snapshot_hash=str(observation_snapshot["observation_snapshot_hash"]),
        generated_at=generated_at,
        explanation=str(primary_output["explanation"]),
        adapter_agreement=False,
    )
    teacher_row = os_contracts.build_teacher_evaluation_row(
        experiment_id=experiment_id,
        hypothesis_id=hypothesis_id,
        target=target,
        observation_snapshot_hash=str(observation_snapshot["observation_snapshot_hash"]),
        strategy_judgement_hash=str(strategy_judgement["strategy_judgement_hash"]),
        realized_outcome_window={
            "teacher_horizon_bars": 20,
            "future_bar_count": 20 if complete_horizon else 8,
            "complete_horizon": complete_horizon,
            "anchor_close_price": 122.0,
            "next_open_price": 123.0 if return_next_open_basis is not None else None,
            "final_close_price": 134.2 if return_close_basis is not None else None,
            "return_close_basis": return_close_basis,
            "return_next_open_basis": return_next_open_basis,
            "max_favorable_excursion_close_basis": mfe,
            "max_adverse_excursion_close_basis": mae,
            "future_dates": [as_of_date + 1],
        },
        lineage={
            "observation_contract_version": "tradex_close_based_buy_judgement_v1",
            "primary_adapter_id": primary_adapter_id,
            "adapter_ids": [str(item["adapter_id"]) for item in adapter_outputs],
        },
        generated_at=generated_at,
    )
    judge_input = os_contracts.build_judge_input(
        experiment_id=experiment_id,
        comparison_scope={
            "session_id": "session-a",
            "family_id": family_id,
            "target_method_family": method_family,
        },
        changed_top5_members_count=1,
        changed_top10_members_count=1,
        changed_rank_count=2,
        top5_boundary_score_gap=0.12,
        top10_boundary_score_gap=0.08,
        selection_divergence_reason="top5_branch_changed",
        available_sample_count=120,
        available_session_count=1,
        summary_metrics={"family_decision": "hold"},
    )
    preflight_report = os_contracts.build_preflight_report(
        experiment_id=experiment_id,
        hypothesis_id=hypothesis_id,
        runner="tradex_research_session",
        status="preflight_passed",
        passed=True,
        failure_code="",
        failure_detail={},
        checked_inputs={"runner": "tradex_research_session"},
        normalization_applied=["hypothesis_validated"],
        checked_at=generated_at,
    )
    os_store.write_json(os_store.preflight_report_file(experiment_id), preflight_report)
    os_store.write_json(os_store.observation_snapshot_file(experiment_id), observation_snapshot)
    os_store.write_json(os_store.strategy_judgement_file(experiment_id), strategy_judgement)
    os_store.write_json(os_store.teacher_evaluation_row_file(experiment_id), teacher_row)
    os_store.write_json(os_store.judge_input_file(experiment_id), judge_input)


def test_benchmark_materializes_rows_and_scoreboard(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _reset_tradex_root(monkeypatch, tmp_path)
    _write_success_experiment(
        experiment_id="exp_alpha",
        hypothesis_id="hyp_alpha",
        family_id="family-alpha",
        method_family="regime-aware",
        code="6963",
        as_of_date=20260403,
        adapter_outputs=[
            _adapter_output(adapter_id="numeric_baseline_v1", action="enter", judgement="buy", buy_score=0.61, confidence=0.66),
            _adapter_output(adapter_id="structured_reasoner_v1", action="wait", judgement="hold", buy_score=0.57, confidence=0.72),
        ],
        primary_adapter_id="structured_reasoner_v1",
    )
    _write_success_experiment(
        experiment_id="exp_beta",
        hypothesis_id="hyp_beta",
        family_id="family-beta",
        method_family="regime-aware",
        code="6501",
        as_of_date=20260402,
        adapter_outputs=[
            _adapter_output(adapter_id="numeric_baseline_v1", action="enter", judgement="buy", buy_score=0.68, confidence=0.70),
        ],
        primary_adapter_id="numeric_baseline_v1",
        complete_horizon=False,
        return_close_basis=0.03,
        return_next_open_basis=0.02,
        mfe=0.07,
        mae=-0.02,
    )

    result = benchmark_service.rebuild_trader_benchmark()

    assert result["status"] == "ok"
    rows_path = Path(result["rows_path"])
    assert rows_path.exists()
    rows = [json.loads(line) for line in rows_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(rows) == 3
    alpha_rows = [row for row in rows if row["experiment_id"] == "exp_alpha"]
    assert len(alpha_rows) == 2
    assert sum(1 for row in alpha_rows if row["is_primary_adapter"]) == 1
    assert alpha_rows[1]["adapter_id"] == "structured_reasoner_v1"
    numeric_alpha = next(row for row in alpha_rows if row["adapter_id"] == "numeric_baseline_v1")
    beta_row = next(row for row in rows if row["experiment_id"] == "exp_beta")
    assert numeric_alpha["close_positive_20"] is True
    assert numeric_alpha["mfe_ge_10pct_20"] is True
    assert numeric_alpha["mae_worse_than_7pct_20"] is False
    assert numeric_alpha["judgement_outcome_class"] == "good"
    assert numeric_alpha["label_policy_version"] == "v1"
    assert beta_row["judgement_outcome_class"] == "incomplete"
    assert beta_row["close_positive_20"] is None
    scoreboard = json.loads(Path(result["scoreboard_path"]).read_text(encoding="utf-8"))
    by_adapter = {row["adapter_id"]: row for row in scoreboard["adapters"]}
    assert by_adapter["numeric_baseline_v1"]["sample_count"] == 2
    assert by_adapter["numeric_baseline_v1"]["labeled_sample_count"] == 1
    assert by_adapter["numeric_baseline_v1"]["enter_count"] == 2
    assert by_adapter["numeric_baseline_v1"]["avg_return_close_basis_enter"] == pytest.approx(0.065)
    assert by_adapter["numeric_baseline_v1"]["avg_mfe_enter"] == pytest.approx(0.11)
    assert by_adapter["numeric_baseline_v1"]["close_positive_rate_all"] == pytest.approx(1.0)
    assert by_adapter["numeric_baseline_v1"]["good_outcome_rate_enter"] == pytest.approx(1.0)
    assert by_adapter["numeric_baseline_v1"]["bad_outcome_rate_enter"] == pytest.approx(0.0)
    assert by_adapter["structured_reasoner_v1"]["sample_count"] == 1
    assert by_adapter["structured_reasoner_v1"]["wait_count"] == 1
    assert by_adapter["structured_reasoner_v1"]["primary_count"] == 1


def test_benchmark_manifest_records_skip_reasons(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _reset_tradex_root(monkeypatch, tmp_path)
    _write_success_experiment(
        experiment_id="exp_ok",
        hypothesis_id="hyp_ok",
        family_id="family-ok",
        method_family="regime-aware",
        code="6963",
        as_of_date=20260403,
        adapter_outputs=[_adapter_output(adapter_id="numeric_baseline_v1", action="enter", judgement="buy", buy_score=0.61, confidence=0.66)],
        primary_adapter_id="numeric_baseline_v1",
    )
    failed_preflight = os_contracts.build_preflight_report(
        experiment_id="exp_preflight_failed",
        hypothesis_id="hyp_fail",
        runner="tradex_research_session",
        status="preflight_failed",
        passed=False,
        failure_code="evaluation_windows_unavailable",
        failure_detail={"cause": "fixture"},
        checked_inputs={"runner": "tradex_research_session"},
        normalization_applied=[],
        checked_at="2026-04-09T00:00:00+00:00",
    )
    os_store.write_json(os_store.preflight_report_file("exp_preflight_failed"), failed_preflight)
    malformed_path = os_store.observation_snapshot_file("exp_malformed")
    malformed_path.parent.mkdir(parents=True, exist_ok=True)
    malformed_path.write_text("{not-json", encoding="utf-8")
    os_store.write_json(os_store.strategy_judgement_file("exp_malformed"), {"schema_version": "broken"})
    os_store.write_json(os_store.teacher_evaluation_row_file("exp_malformed"), {"schema_version": "broken"})
    os_store.write_json(os_store.judge_input_file("exp_malformed"), {"schema_version": "broken"})

    result = benchmark_service.rebuild_trader_benchmark()
    manifest = json.loads(Path(result["manifest_path"]).read_text(encoding="utf-8"))
    reasons = {entry["experiment_id"]: entry["reason_code"] for entry in manifest["skipped_experiments"]}
    assert reasons["exp_preflight_failed"] == "preflight_failed"
    assert reasons["exp_malformed"] == "malformed_artifact"


def test_benchmark_manifest_records_label_input_incomplete(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _reset_tradex_root(monkeypatch, tmp_path)
    _write_success_experiment(
        experiment_id="exp_label_incomplete",
        hypothesis_id="hyp_label_incomplete",
        family_id="family-label",
        method_family="regime-aware",
        code="9984",
        as_of_date=20260401,
        adapter_outputs=[_adapter_output(adapter_id="numeric_baseline_v1", action="enter", judgement="buy", buy_score=0.64, confidence=0.71)],
        primary_adapter_id="numeric_baseline_v1",
        complete_horizon=True,
        return_close_basis=None,
        return_next_open_basis=0.02,
        mfe=0.12,
        mae=-0.03,
    )

    result = benchmark_service.rebuild_trader_benchmark()

    manifest = json.loads(Path(result["manifest_path"]).read_text(encoding="utf-8"))
    reasons = {entry["experiment_id"]: entry["reason_code"] for entry in manifest["skipped_experiments"]}
    assert reasons["exp_label_incomplete"] == "label_inputs_incomplete"
    rows = [json.loads(line) for line in Path(result["rows_path"]).read_text(encoding="utf-8").splitlines() if line.strip()]
    assert rows == []


def test_rebuild_trader_benchmark_cli_is_idempotent(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _reset_tradex_root(monkeypatch, tmp_path)
    _write_success_experiment(
        experiment_id="exp_cli",
        hypothesis_id="hyp_cli",
        family_id="family-cli",
        method_family="regime-aware",
        code="7203",
        as_of_date=20260401,
        adapter_outputs=[
            _adapter_output(adapter_id="numeric_baseline_v1", action="enter", judgement="buy", buy_score=0.63, confidence=0.68),
            _adapter_output(adapter_id="structured_reasoner_v1", action="enter", judgement="buy", buy_score=0.64, confidence=0.74),
        ],
        primary_adapter_id="structured_reasoner_v1",
    )

    assert os_runner.main(["rebuild-trader-benchmark"]) == 0
    first_stdout = capsys.readouterr().out
    first_result = json.loads(first_stdout)
    rows_path = Path(first_result["rows_path"])
    first_rows = [json.loads(line) for line in rows_path.read_text(encoding="utf-8").splitlines() if line.strip()]

    assert os_runner.main(["rebuild-trader-benchmark"]) == 0
    second_stdout = capsys.readouterr().out
    second_result = json.loads(second_stdout)
    second_rows = [json.loads(line) for line in Path(second_result["rows_path"]).read_text(encoding="utf-8").splitlines() if line.strip()]

    assert len(first_rows) == 2
    assert len(second_rows) == 2
    assert first_rows == second_rows
    assert Path(second_result["manifest_path"]).exists()
    assert Path(second_result["scoreboard_path"]).exists()
