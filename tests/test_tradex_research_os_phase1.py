from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import pytest

import app.backend.api.dependencies as dependencies
from app.backend.core.legacy_analysis_control import LEGACY_ANALYSIS_DISABLE_ENV
import app.backend.services.tradex_experiment_service as service
from app.backend.services import tradex_research_preflight as preflight_service
from app.backend.services import tradex_research_os_contracts as os_contracts
from app.backend.services import tradex_research_os_store as os_store
from app.backend.tools import tradex_research_os_runner as os_runner
from app.backend.tools import tradex_research_runner as tradex_runner
from shared import tradex_storage


def _reset_tradex_root(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    root = (tmp_path / "tradex-root").resolve()
    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(root))
    tradex_storage.resolve_tradex_root.cache_clear()
    return root


class _FakeOutput:
    def __init__(self, payload: dict[str, object]) -> None:
        self.payload = payload

    def to_dict(self) -> dict[str, object]:
        return dict(self.payload)


class _FakeRepo:
    def get_all_codes(self) -> list[str]:
        return [f"10{idx:02d}" for idx in range(1, 21)]

    def get_analysis_timeline(self, code: str, asof_dt: int | None, limit: int = 400):  # noqa: ARG002
        del asof_dt, limit
        base = int(code[-1]) if code[-1].isdigit() else 0
        return [
            {
                "dt": 20250105,
                "pUp": 0.45 + base * 0.01,
                "pDown": 0.55 - base * 0.01,
                "pTurnUp": 0.2,
                "pTurnDown": 0.1,
                "ev20Net": 0.1,
                "sellPDown": 0.2,
                "sellPTurnDown": 0.1,
                "trendDown": False,
                "trendDownStrict": False,
                "shortRet5": 0.01,
                "shortRet10": 0.02,
                "shortRet20": 0.03,
                "shortWin5": True,
                "shortWin10": True,
                "shortWin20": True,
            }
        ]

    def get_daily_bars(self, code: str, limit: int = 400, asof_dt: int | None = None):  # noqa: ARG002
        del code, asof_dt
        rows = []
        current = 0
        while len(rows) < 120:
            rows.append((20250101 + current, 99.0, 101.0, 98.0, 100.0 + current, 1_000_000.0))
            current += 1
        return rows[-max(1, min(limit, len(rows))):]


def _fake_regime_rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    current = date(2025, 1, 1)
    for regime_id, regime_tag in (("risk_on_trend", "up"), ("risk_off_trend", "down"), ("neutral_range", "flat")):
        for _ in range(60):
            rows.append(
                {
                    "dt": int(current.strftime("%Y%m%d")),
                    "regime_id": regime_id,
                    "regime_tag": regime_tag,
                    "regime_score": 0.1,
                    "label_version": service.TRADEX_EVAL_REGIME_LABEL_VERSION,
                }
            )
            current += timedelta(days=1)
    return rows


def _fake_run_tradex_analysis(input_contract):
    p_up = float(input_contract.analysis_p_up or 0.0)
    return _FakeOutput(
        {
            "symbol": input_contract.symbol,
            "asof": input_contract.asof,
            "analysis_p_up": p_up,
            "analysis_p_down": max(0.0, 1.0 - p_up),
            "status": "ok",
        }
    )


def _passing_preflight(*, hypothesis: dict[str, object], repo_commit: str, runner_version: str, started_at: str) -> dict[str, object]:
    del repo_commit, runner_version, started_at
    execution = dict(hypothesis.get("execution") or {})
    return {
        "provisional_experiment_id": "exp_preflight_pass",
        "experiment_id": "exp_preflight_pass",
        "hypothesis_id": str(hypothesis["hypothesis_id"]),
        "runner": str(execution.get("runner") or "tradex_research_session"),
        "status": preflight_service.TRADEX_RESEARCH_PREFLIGHT_REPORT_STATUS_PASSED,
        "passed": True,
        "failure_code": "",
        "failure_detail": {},
        "checked_inputs": {
            "policy": {"schema_version": "tradex_research_preflight_policy_v1", "preflight_policy_version": "v1", "check_order": [], "minimum_evaluation_window_count": 3},
            "hypothesis_id": str(hypothesis["hypothesis_id"]),
            "target_method_family": str(hypothesis["target_method_family"]),
            "runner": str(execution.get("runner") or "tradex_research_session"),
            "session_id": str(execution.get("session_id") or "session"),
            "session_scope_id": str(execution.get("session_scope_id") or "scope"),
            "random_seed": int(execution.get("random_seed") or 0),
            "universe_size": int(execution.get("universe_size") or 0),
            "max_candidates_per_family": int(execution.get("max_candidates_per_family") or 0),
            "ret20_source_mode": str(execution.get("ret20_source_mode") or "derived_from_daily_bars"),
            "legacy_analysis_disabled": False,
            "legacy_analysis_env": "0",
            "required_execution_fields": ["session_id", "session_scope_id", "random_seed", "universe_size", "max_candidates_per_family", "ret20_source_mode", "target_method_family"],
            "minimum_window_count": 3,
            "preflight_policy_version": "v1",
            "regime_row_count": 6,
            "selected_window_ids": ["up:1:3", "down:4:6", "flat:7:9"],
            "evaluation_window_count": 3,
            "regime_row_issues": [],
            "evaluation_window_issues": [],
        },
        "normalization_applied": ["hypothesis_validated", "runner_checked", "required_inputs_checked", "legacy_analysis_checked", "evaluation_regime_rows_loaded", "regime_rows_checked", "evaluation_windows_selected", "artifact_shape_checked"],
        "checked_at": "2025-01-31T00:00:00+09:00",
    }


def _failing_preflight(*, failure_code: str, failure_detail: dict[str, object]) -> dict[str, object]:
    return {
        "provisional_experiment_id": "exp_preflight_fail",
        "experiment_id": "exp_preflight_fail",
        "hypothesis_id": "hypothesis-regime-aware-v1",
        "runner": "tradex_research_session",
        "status": preflight_service.TRADEX_RESEARCH_PREFLIGHT_REPORT_STATUS_FAILED,
        "passed": False,
        "failure_code": failure_code,
        "failure_detail": failure_detail,
        "checked_inputs": {
            "policy": {"schema_version": "tradex_research_preflight_policy_v1", "preflight_policy_version": "v1", "check_order": [], "minimum_evaluation_window_count": 3},
            "hypothesis_id": "hypothesis-regime-aware-v1",
            "target_method_family": "regime-aware",
            "runner": "tradex_research_session",
            "session_id": "os-smoke-session",
            "session_scope_id": "scope-a",
            "random_seed": 7,
            "universe_size": 30,
            "max_candidates_per_family": 2,
            "ret20_source_mode": "derived_from_daily_bars",
        },
        "normalization_applied": ["hypothesis_validated", "runner_checked"],
        "checked_at": "2025-01-31T00:00:00+09:00",
    }


def _hypothesis_payload(
    *,
    session_id: str = "os-smoke-session",
    target_method_family: str = "regime-aware",
    status: str = "ready",
) -> dict[str, object]:
    return {
        "schema_version": os_contracts.TRADEX_RESEARCH_OS_HYPOTHESIS_SCHEMA_VERSION,
        "hypothesis_id": "hypothesis-regime-aware-v1",
        "hypothesis_type": "candidate-family-comparison",
        "changed_axis": "regime_adaptation",
        "fixed_contracts": ["same_condition", "authoritative_compare", "single_session"],
        "expected_effect": "improve regime-specific family judgment without changing TRADEX compare contracts",
        "metrics_to_watch": ["changed_top5_members_count", "changed_rank_count", "hold_end_return_20d"],
        "acceptance_gate": {"mode": "provisional", "criteria": ["family_decision in keep/drop/hold"]},
        "rejection_gate": {"mode": "provisional", "criteria": ["missing family compare", "invalid lineage"]},
        "notes": "phase 1 additive skeleton",
        "status": status,
        "target_method_family": target_method_family,
        "execution": {
            "runner": "tradex_research_session",
            "session_id": session_id,
            "random_seed": 7,
            "session_scope_id": "scope-a",
            "universe_size": 30,
            "max_candidates_per_family": 2,
            "ret20_source_mode": "derived_from_daily_bars",
        },
    }


@pytest.fixture(autouse=True)
def _enable_legacy_analysis_for_tradex_research(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(LEGACY_ANALYSIS_DISABLE_ENV, "0")


def _write_fake_session_artifacts(root: Path, session_id: str, method_family: str) -> dict[str, object]:
    session_dir = tradex_runner._session_state_file(session_id).parent
    family_id = tradex_runner._session_family_id(session_id, method_family)
    family_dir = tradex_storage.tradex_research_families_root() / family_id
    session_dir.mkdir(parents=True, exist_ok=True)
    family_dir.mkdir(parents=True, exist_ok=True)

    run_manifest_path = tradex_runner._session_state_file(session_id).parent / "run_manifest.json"
    family_compare_path = family_dir / "compare.json"
    family_leaderboard_path = tradex_runner._session_family_leaderboard_file(session_id)
    session_state_path = tradex_runner._session_state_file(session_id)
    session_compare_path = session_dir / "compare.json"

    run_manifest = {
        "schema_version": "tradex_research_run_manifest_v1",
        "session_id": session_id,
        "seed": 7,
        "random_seed": 7,
        "input_artifacts": [{"kind": "confirmed_universe", "path": "fixture"}],
        "asof": "2025-01-31",
        "config": {"session_scope_id": "scope-a"},
        "universe": ["1001", "1002"],
        "period": {"segments": [{"label": "core", "start_date": "2025-01-01", "end_date": "2025-01-31"}]},
        "horizon": "20d",
        "artifact_detail_level": "authoritative_full",
        "fallback_status": "authoritative",
        "cost_model": {
            "schema_version": "tradex_cost_model_v1",
            "mode": "flat_zero_cost",
            "transaction_cost_bps": 0.0,
            "slippage_bps": 0.0,
            "fee_bps": 0.0,
        },
        "run_manifest_hash": "run-manifest-hash",
    }
    family_compare = {
        "schema_version": "tradex_experiment_compare_v1",
        "diagnostics_schema_version": "tradex_diagnostics_v1",
        "family_id": family_id,
        "generated_at": "2025-01-31T00:00:00+09:00",
        "baseline_run_id": f"{family_id}-baseline",
        "same_condition_contract": {
            "schema_version": "tradex_research_contract_v1",
            "universe": ["1001", "1002"],
            "period": [{"label": "core", "start_date": "2025-01-01", "end_date": "2025-01-31"}],
            "top_k": 5,
            "regime": "trend_long",
            "cost_model": {
                "schema_version": "tradex_cost_model_v1",
                "mode": "flat_zero_cost",
                "transaction_cost_bps": 0.0,
                "slippage_bps": 0.0,
                "fee_bps": 0.0,
            },
            "artifact_detail_level": "authoritative_full",
            "fallback_status": "authoritative",
            "feature_family": "regime_adjustment",
            "contract_hash": "contract-hash",
        },
        "compare_hash": "compare-hash",
        "candidate_results": [
            {
                "candidate_method": {
                    "method_id": "regime_aware_v1",
                    "method_family": method_family,
                    "method_title": "Regime aware",
                    "method_thesis": "fixture",
                    "feature_family": "regime_adjustment",
                },
                "candidate_run_id": f"{family_id}-regime_aware_v1",
                "candidate_local_decision": "keep",
                "session_aggregate_decision": "keep",
                "authoritative_rollup_decision": "keep",
                "decision_reasons": [{"code": "candidate_keep_present", "status": "keep"}],
                "artifact_detail_level": "authoritative_full",
                "fallback_status": "authoritative",
                "feature_family": "regime_adjustment",
                "victory_metrics": {
                    "hold_end_return_20d": 0.1,
                    "mfe_20d": 0.2,
                    "mae_20d": -0.05,
                    "win_flag_hold_end": True,
                    "win_flag_mfe": True,
                    "addability_score": None,
                    "trimability_score": None,
                    "opportunity_count": 10,
                    "avg_holding_days": 20.0,
                    "max_drawdown": -0.03,
                },
                "long_horizon_regime_score": 0.1,
                "recent_adaptation_score": 0.05,
                "changed_top5_members_count": 2,
                "changed_top10_members_count": 3,
                "changed_rank_count": 4,
                "top5_boundary_score_gap": 0.12,
                "top10_boundary_score_gap": 0.08,
                "selection_divergence_reason": "top5_member_replacement",
                "effective_universe_count": 2,
                "top_k": 5,
                "meaningful_topk_branching_possible": True,
                "topk_branching_block_reason": "",
                "comparison": {"comparison_hash": "comparison-hash"},
            }
        ],
    }
    family_leaderboard = {
        "schema_version": "tradex_family_leaderboard_v1",
        "session_meta": {
            "session_id": session_id,
            "random_seed": 7,
            "generated_at": "2025-01-31T00:00:00+09:00",
            "manifest_hash": "manifest-hash",
            "compare_schema_version": "tradex_experiment_compare_v1",
            "eval_window_mode": "standard",
            "eval_window_mode_reason": "standard",
            "ret20_source_mode": "derived_from_daily_bars",
            "ret20_source_mode_reason": "explicit_session_mode",
            "sample_count": 12,
            "insufficient_samples": False,
            "scope_filter_applied_stage": "analysis",
            "candidate_scope_gap_reason_counts": {},
            "candidate_in_scope_before_build_count": 12,
            "candidate_in_scope_after_build_count": 12,
            "session_failure_reason_counts": {},
        },
        "source_compare_path": str(family_compare_path),
        "source_report_path": str(root / "keep" / "research" / "reports" / "fixture.md"),
        "coverage_waterfall": {"sample_count": 12},
        "overview": {
            "family_count": 1,
            "candidate_count": 1,
            "keep_family_count": 1,
            "hold_family_count": 0,
            "drop_family_count": 0,
            "keep_candidate_count": 1,
            "hold_candidate_count": 0,
            "drop_candidate_count": 0,
            "insufficient_samples": False,
        },
        "authoritative_rollup_decision": "keep",
        "family_summary": [
            {
                "family_id": family_id,
                "method_family": method_family,
                "family_title": "Regime aware",
                "family_thesis": "fixture",
                "decision": "keep",
                "session_aggregate_decision": "keep",
                "authoritative_rollup_decision": "keep",
                "decision_reasons": [{"code": "candidate_keep_present", "status": "keep"}],
                "candidate_count": 1,
                "keep_count": 1,
                "drop_count": 0,
                "hold_count": 0,
                "hold_budget_remaining": 0,
                "best_candidate_method_id": "regime_aware_v1",
                "best_candidate_method_title": "Regime aware",
                "best_candidate_method_thesis": "fixture",
                "best_candidate_decision": "keep",
                "best_candidate_feature_family": "regime_adjustment",
                "effective_universe_count": 2,
                "top_k": 5,
                "meaningful_topk_branching_possible": True,
                "topk_branching_block_reason": "",
            }
        ],
        "candidate_rows": [
            {
                "method_signature_hash": "sig",
                "method_family": method_family,
                "method_title": "Regime aware",
                "method_thesis": "fixture",
                "feature_family": "regime_adjustment",
                "keep_count": 1,
                "drop_count": 0,
                "hold_count": 0,
                "session_count": 1,
                "session_ids": [session_id],
                "avg_top5_ret20_mean_delta": 0.1,
                "avg_top10_ret20_mean_delta": 0.1,
                "avg_monthly_capture_delta": 0.0,
                "avg_zero_pass_delta": 0.0,
                "avg_worst_regime_delta": 0.0,
                "avg_dd_delta": 0.0,
                "avg_turnover_delta": 0.0,
                "avg_liquidity_fail_delta": 0.0,
                "avg_changed_top5_members_count": 2.0,
                "avg_changed_top10_members_count": 3.0,
                "avg_changed_rank_count": 4.0,
                "avg_top5_boundary_score_gap": 0.12,
                "avg_top10_boundary_score_gap": 0.08,
                "latest_session_id": session_id,
                "latest_generated_at": "2025-01-31T00:00:00+09:00",
                "latest_eval_window_mode": "standard",
                "latest_eval_window_mode_reason": "standard",
                "latest_decision": "keep",
                "candidate_local_decision": "keep",
                "session_aggregate_decision": "keep",
                "selection_divergence_reason": "top5_member_replacement",
                "decision_reasons": [{"code": "candidate_keep_present", "status": "keep"}],
                "latest_decision_reasons": [{"code": "candidate_keep_present", "status": "keep"}],
                "artifact_detail_level": "authoritative_full",
                "fallback_status": "authoritative",
                "victory_metrics": {
                    "hold_end_return_20d": 0.1,
                    "mfe_20d": 0.2,
                    "mae_20d": -0.05,
                    "win_flag_hold_end": True,
                    "win_flag_mfe": True,
                    "addability_score": None,
                    "trimability_score": None,
                    "opportunity_count": 10,
                    "avg_holding_days": 20.0,
                    "max_drawdown": -0.03,
                },
                "long_horizon_regime_score": 0.1,
                "recent_adaptation_score": 0.05,
                "insufficient_samples": False,
            }
        ],
    }
    session_state = {
        "schema_version": "tradex_research_session_v1",
        "session_id": session_id,
        "random_seed": 7,
        "status": "complete",
        "family_results": [
            {
                "family_id": family_id,
                "method_family": method_family,
                "compare_path": str(family_compare_path),
            }
        ],
        "compare_schema_version": "tradex_research_session_compare_v1",
        "run_manifest": run_manifest,
        "run_manifest_hash": run_manifest["run_manifest_hash"],
        "session_scope_id": "scope-a",
    }
    os_store.write_json(run_manifest_path, run_manifest)
    os_store.write_json(family_compare_path, family_compare)
    os_store.write_json(family_leaderboard_path, family_leaderboard)
    os_store.write_json(session_state_path, session_state)
    os_store.write_json(session_compare_path, {"schema_version": "session_not_compare", "broken": True})
    return {
        "family_id": family_id,
        "family_compare_path": family_compare_path,
        "family_leaderboard_path": family_leaderboard_path,
        "run_manifest_path": run_manifest_path,
        "session_state_path": session_state_path,
        "session_compare_path": session_compare_path,
        "run_manifest": run_manifest,
    }


def test_hypothesis_schema_validation_and_manifest_generation() -> None:
    hypothesis = _hypothesis_payload()
    os_contracts.validate_hypothesis(hypothesis)
    manifest = os_contracts.build_hypothesis_manifest(hypothesis)
    assert manifest["schema_version"] == os_contracts.TRADEX_RESEARCH_OS_HYPOTHESIS_SCHEMA_VERSION
    assert len(manifest["hypothesis_hash"]) == 64

    experiment = os_contracts.build_experiment_manifest(
        experiment_id="exp_test",
        hypothesis_id="hypothesis-regime-aware-v1",
        repo_commit="abc123",
        runner_version=os_contracts.TRADEX_RESEARCH_OS_RUNNER_VERSION,
        config_fingerprint="config-hash",
        scope_fingerprint="scope-hash",
        seed=7,
        started_at="2025-01-31T00:00:00+09:00",
        finished_at="2025-01-31T00:00:01+09:00",
        generated_artifacts=[{"name": "x", "path": "y"}],
    )
    assert experiment["schema_version"] == os_contracts.TRADEX_RESEARCH_OS_EXPERIMENT_MANIFEST_SCHEMA_VERSION
    assert len(experiment["experiment_manifest_hash"]) == 64


def test_compare_adapter_uses_family_compare_not_session_compare(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    root = _reset_tradex_root(monkeypatch, tmp_path)
    hypothesis = _hypothesis_payload()
    session_id = str(hypothesis["execution"]["session_id"])
    method_family = str(hypothesis["target_method_family"])
    artifacts = _write_fake_session_artifacts(root, session_id, method_family)

    session_state = os_store.read_json(artifacts["session_state_path"])
    run_manifest = os_store.read_json(artifacts["run_manifest_path"])
    family_leaderboard = os_store.read_json(artifacts["family_leaderboard_path"])
    family_compare = os_store.read_json(artifacts["family_compare_path"])

    judge_input = os_runner.build_judge_input(
        experiment_id="exp_test",
        hypothesis=hypothesis,
        session_id=session_id,
        family_id=artifacts["family_id"],
        run_manifest=run_manifest,
        family_leaderboard=family_leaderboard,
        family_compare=family_compare,
    )
    assert judge_input["available_session_count"] == 1
    assert judge_input["available_sample_count"] == 12
    assert judge_input["comparison_scope"]["family_id"] == artifacts["family_id"]
    assert judge_input["summary_metrics"]["family_decision"] == "keep"
    assert judge_input["changed_top5_members_count"] == 2
    assert session_state["family_results"][0]["compare_path"] == str(artifacts["family_compare_path"])


def test_compare_resolution_rejects_ambiguous_or_missing_paths(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _reset_tradex_root(monkeypatch, tmp_path)
    session_id = "ambiguous-session"
    target_method_family = "regime-aware"
    family_id = tradex_runner._session_family_id(session_id, target_method_family)
    session_state = {
        "session_id": session_id,
        "family_results": [
            {"family_id": family_id, "method_family": target_method_family, "compare_path": str(tmp_path / "a" / "compare.json")},
            {"family_id": family_id, "method_family": target_method_family, "compare_path": str(tmp_path / "b" / "compare.json")},
        ],
    }
    Path(tmp_path / "a").mkdir(parents=True, exist_ok=True)
    Path(tmp_path / "b").mkdir(parents=True, exist_ok=True)
    (tmp_path / "a" / "compare.json").write_text("{}", encoding="utf-8")
    (tmp_path / "b" / "compare.json").write_text("{}", encoding="utf-8")
    family_leaderboard = {
        "family_summary": [
            {"family_id": family_id, "method_family": target_method_family, "decision": "keep", "best_candidate_decision": "keep"}
        ]
    }

    with pytest.raises(ValueError, match="ambiguous"):
        os_runner._resolve_family_compare_artifact(
            session_id=session_id,
            target_method_family=target_method_family,
            session_state=session_state,
            family_leaderboard=family_leaderboard,
        )

    session_state = {
        "session_id": session_id,
        "family_results": [
            {"family_id": family_id, "method_family": target_method_family, "compare_path": ""}
        ],
    }
    with pytest.raises(ValueError, match="family compare missing"):
        os_runner._resolve_family_compare_artifact(
            session_id=session_id,
            target_method_family=target_method_family,
            session_state=session_state,
            family_leaderboard=family_leaderboard,
        )


def test_judge_input_stable_when_session_metadata_is_incomplete(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    root = _reset_tradex_root(monkeypatch, tmp_path)
    hypothesis = _hypothesis_payload()
    session_id = str(hypothesis["execution"]["session_id"])
    method_family = str(hypothesis["target_method_family"])
    artifacts = _write_fake_session_artifacts(root, session_id, method_family)

    family_leaderboard = os_store.read_json(artifacts["family_leaderboard_path"])
    family_leaderboard.pop("session_meta", None)
    family_leaderboard.pop("coverage_waterfall", None)

    judge_input = os_runner.build_judge_input(
        experiment_id="exp_test",
        hypothesis=hypothesis,
        session_id=session_id,
        family_id=artifacts["family_id"],
        run_manifest=os_store.read_json(artifacts["run_manifest_path"]),
        family_leaderboard=family_leaderboard,
        family_compare=os_store.read_json(artifacts["family_compare_path"]),
    )
    assert judge_input["available_sample_count"] == 0
    assert judge_input["available_session_count"] == 1
    assert judge_input["summary_metrics"]["family_decision"] == "keep"


def test_judge_decision_persistence_updates_memory(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _reset_tradex_root(monkeypatch, tmp_path)
    decision = os_contracts.build_judge_decision(
        experiment_id="exp_test",
        decision="hold",
        typed_reason={
            "code": "insufficient_samples",
            "source_artifact": "family_leaderboard",
            "source_field": "coverage_waterfall.sample_count",
            "detail": {"sample_count": 0},
        },
        confidence=0.35,
        next_action="collect_more_evidence",
        blocking_unknowns=["insufficient_samples"],
        decided_at="2025-01-31T00:00:00+09:00",
    )
    memory = os_runner.update_research_memory(
        hypothesis_id="hypothesis-regime-aware-v1",
        decision=decision,
        experiment_id="exp_test",
        family_id="tradex-research-os-fake",
        judge_input={"available_sample_count": 0, "selection_divergence_reason": "insufficient_samples"},
    )
    assert memory["latest_decision"] == "hold"
    assert len(memory["decision_history"]) == 1
    assert memory["retry_blockers"] == ["insufficient_samples"]

    second = os_contracts.build_judge_decision(
        experiment_id="exp_test-2",
        decision="keep",
        typed_reason={
            "code": "candidate_keep_present",
            "source_artifact": "family_compare",
            "source_field": "decision_reasons[0]",
            "detail": {},
        },
        confidence=0.85,
        next_action="manual_review",
        blocking_unknowns=[],
        decided_at="2025-01-31T01:00:00+09:00",
    )
    memory = os_runner.update_research_memory(
        hypothesis_id="hypothesis-regime-aware-v1",
        decision=second,
        experiment_id="exp_test-2",
        family_id="tradex-research-os-fake",
        judge_input={"available_sample_count": 12, "selection_divergence_reason": "top5_member_replacement"},
    )
    assert memory["latest_decision"] == "keep"
    assert len(memory["decision_history"]) == 2
    assert memory["retry_blockers"] == []


def test_end_to_end_additive_skeleton_flow(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    root = _reset_tradex_root(monkeypatch, tmp_path)
    hypothesis = _hypothesis_payload(session_id="os-smoke-session", target_method_family="regime-aware")
    hypothesis_path = tmp_path / "hypothesis.json"
    hypothesis_path.write_text(json.dumps(hypothesis, ensure_ascii=False, indent=2), encoding="utf-8")

    fake_artifacts = _write_fake_session_artifacts(root, "os-smoke-session", "regime-aware")
    monkeypatch.setattr(
        tradex_runner,
        "run_tradex_research_session",
        lambda **kwargs: {
            "status": "complete",
            "session_id": kwargs["session_id"],
            "family_results": [
                {
                    "family_id": fake_artifacts["family_id"],
                    "method_family": "regime-aware",
                    "compare_path": str(fake_artifacts["family_compare_path"]),
                }
            ],
        },
    )
    monkeypatch.setattr(preflight_service, "evaluate_preflight", lambda **kwargs: _passing_preflight(**kwargs))

    result = os_runner.run_hypothesis(hypothesis_path)
    assert result["status"] == "ok"
    assert result["family_id"] == fake_artifacts["family_id"]
    assert Path(result["preflight_report_path"]).exists()
    assert Path(result["experiment_manifest_path"]).exists()
    assert Path(result["judge_input_path"]).exists()
    assert Path(result["judge_decision_path"]).exists()
    assert Path(result["authoritative_decision_path"]).exists()
    assert Path(result["research_memory_path"]).exists()
    assert result["preflight_report"]["passed"] is True
    preflight_report = os_store.read_json(Path(result["preflight_report_path"]))
    assert preflight_report["passed"] is True
    assert preflight_report["status"] == preflight_service.TRADEX_RESEARCH_PREFLIGHT_REPORT_STATUS_PASSED
    memory = os_store.read_json(Path(result["research_memory_path"]))
    assert memory["latest_decision"] == result["authoritative_decision"]["decision"]
    assert len(memory["decision_history"]) == 2
    assert memory["decision_history"][0]["decision_stage"] == "provisional"
    assert memory["decision_history"][1]["decision_stage"] == "authoritative"
    assert memory["decision_history"][0]["decision_audit"]["decision_source"] == "family_compare"
    assert memory["decision_history"][1]["decision_source"] == "authoritative_decision"
    judge_decision = os_store.read_json(Path(result["judge_decision_path"]))
    assert judge_decision["decision_audit"]["provisional_policy"] == "phase1_provisional"
    assert judge_decision["decision_audit"]["typed_reason_coverage"]["has_code"] is True
    assert judge_decision["decision_audit"]["blocking_unknowns_coverage"]["count"] >= 0
    authoritative_decision = os_store.read_json(Path(result["authoritative_decision_path"]))
    assert authoritative_decision["decision"] == memory["latest_decision"]
    assert authoritative_decision["decision_policy_version"] == "v1"


def test_acceptance_run_hypothesis_with_real_tradex_runner_artifacts(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    root = _reset_tradex_root(monkeypatch, tmp_path)
    tradex_storage.tradex_keep_path("research", "reports").mkdir(parents=True, exist_ok=True)
    hypothesis = _hypothesis_payload(session_id="real-run-session", target_method_family="regime-aware")
    hypothesis_path = tmp_path / "real-run-hypothesis.json"
    hypothesis_path.write_text(json.dumps(hypothesis, ensure_ascii=False, indent=2), encoding="utf-8")

    monkeypatch.setattr(dependencies, "_stock_repo", _FakeRepo(), raising=False)
    monkeypatch.setattr(dependencies, "_config_repo", object(), raising=False)
    monkeypatch.setattr(service, "_load_evaluation_regime_rows", lambda *args, **kwargs: (_fake_regime_rows(), []))
    monkeypatch.setattr(service, "run_tradex_analysis", _fake_run_tradex_analysis)
    monkeypatch.setattr(
        service,
        "_build_champion_challenger_evaluation",
        lambda **kwargs: {
            "regime_tag": "trend_long",
            "artifact_detail_level": service.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
            "fallback_status": service.TRADEX_FALLBACK_STATUS_AUTHORITATIVE,
            "promote_ready": False,
            "promote_reasons": [],
            "status_reasons": [],
            "meaningful_topk_branching_possible": True,
            "insufficient_samples": False,
            "victory_metrics": {},
            "long_horizon_regime_score": 0.1,
            "recent_adaptation_score": 0.1,
        },
    )
    def _mkdiring_write_json(path, payload):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
        return path

    monkeypatch.setattr(service, "write_json", _mkdiring_write_json)

    regime_family_spec = tradex_runner.FamilySpec(
        method_family="regime-aware",
        family_title="Regime aware",
        family_thesis="fixture",
        candidates=(
            tradex_runner.CandidateMethodSpec(
                method_family="regime-aware",
                method_id="regime_aware_v1",
                method_title="Regime aware v1",
                method_thesis="fixture",
                plan_overrides={
                    "minimum_confidence": 0.55,
                    "minimum_ready_rate": 0.5,
                    "signal_bias": "balanced",
                    "top_k": 5,
                    "playbook_up_score_bonus": 0.0,
                    "playbook_down_score_bonus": 0.0,
                },
            ),
            tradex_runner.CandidateMethodSpec(
                method_family="regime-aware",
                method_id="regime_aware_v2",
                method_title="Regime aware v2",
                method_thesis="fixture-two",
                plan_overrides={
                    "minimum_confidence": 0.6,
                    "minimum_ready_rate": 0.55,
                    "signal_bias": "balanced",
                    "top_k": 5,
                    "playbook_up_score_bonus": 0.0,
                    "playbook_down_score_bonus": 0.0,
                },
            ),
        ),
    )
    monkeypatch.setattr(tradex_runner, "_build_family_specs", lambda: (regime_family_spec,))
    monkeypatch.setattr(tradex_runner, "_train_phase4_ranker", lambda **kwargs: {"status": "skipped", "reason": "test"})
    monkeypatch.setattr(preflight_service, "evaluate_preflight", lambda **kwargs: _passing_preflight(**kwargs))

    result = os_runner.run_hypothesis(hypothesis_path)
    assert result["status"] == "ok"
    assert result["family_id"] == "tradex-research-real-run-session-regime-aware"
    assert Path(result["session_state_path"]).exists()
    assert Path(result["run_manifest_path"]).exists()
    assert Path(result["family_leaderboard_path"]).exists()
    assert Path(result["family_compare_path"]).exists()
    assert Path(result["experiment_manifest_path"]).exists()
    assert Path(result["judge_input_path"]).exists()
    assert Path(result["judge_decision_path"]).exists()
    assert Path(result["authoritative_decision_path"]).exists()
    assert Path(result["preflight_report_path"]).exists()
    assert Path(result["research_memory_path"]).exists()
    assert Path(result["family_compare_path"]).resolve() != Path(result["session_state_path"]).with_name("compare.json").resolve()
    assert result["decision_audit"]["decision_source"] == "family_compare"
    assert result["preflight_report"]["passed"] is True

    judge_decision = os_store.read_json(Path(result["judge_decision_path"]))
    assert judge_decision["decision_audit"]["decision_source"] == "family_compare"
    assert judge_decision["decision_audit"]["typed_reason_coverage"]["has_source_artifact"] is True
    assert "blocking_unknowns_coverage" in judge_decision["decision_audit"]
    assert "threshold_dependency" in judge_decision["decision_audit"]
    assert result["decision_audit"]["provisional_policy"] == "phase1_provisional"

    judge_input = os_store.read_json(Path(result["judge_input_path"]))
    authoritative_decision = os_store.read_json(Path(result["authoritative_decision_path"]))
    assert authoritative_decision["decision_policy_version"] == "v1"
    assert authoritative_decision["decision"] in {"keep", "drop", "hold"}
    assert authoritative_decision["decision_inputs"]["policy"]["decision_policy_version"] == "v1"
    assert authoritative_decision["evidence_summary"]["available_sample_count"] == judge_input["available_sample_count"]

    assert judge_input["comparison_scope"]["target_method_family"] == "regime-aware"
    assert judge_input["available_session_count"] == 1
    assert judge_input["summary_metrics"]["family_decision"] in {"keep", "drop", "hold"}
    memory = os_store.read_json(Path(result["research_memory_path"]))
    assert memory["latest_decision"] == authoritative_decision["decision"]
    assert len(memory["decision_history"]) == 2
    assert memory["decision_history"][0]["decision_stage"] == "provisional"
    assert memory["decision_history"][1]["decision_stage"] == "authoritative"


def test_preflight_failure_writes_report_and_skips_decision_artifacts(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _reset_tradex_root(monkeypatch, tmp_path)
    hypothesis = _hypothesis_payload(session_id="preflight-fail-session", target_method_family="regime-aware")
    hypothesis_path = tmp_path / "preflight-fail-hypothesis.json"
    hypothesis_path.write_text(json.dumps(hypothesis, ensure_ascii=False, indent=2), encoding="utf-8")

    monkeypatch.setattr(preflight_service, "evaluate_preflight", lambda **kwargs: _failing_preflight(failure_code="evaluation_windows_unavailable", failure_detail={"reason": "evaluation_windows_unavailable"}))
    monkeypatch.setattr(tradex_runner, "run_tradex_research_session", lambda **kwargs: pytest.fail("runner must not be called when preflight fails"))

    result = os_runner.run_hypothesis(hypothesis_path)
    assert result["status"] == preflight_service.TRADEX_RESEARCH_PREFLIGHT_REPORT_STATUS_FAILED
    assert result["failure_code"] == "evaluation_windows_unavailable"
    assert Path(result["preflight_report_path"]).exists()
    preflight_report = os_store.read_json(Path(result["preflight_report_path"]))
    assert preflight_report["passed"] is False
    assert preflight_report["failure_code"] == "evaluation_windows_unavailable"
    assert not os_store.judge_input_file(result["experiment_id"]).exists()
    assert not os_store.judge_decision_file(result["experiment_id"]).exists()
    assert not os_store.authoritative_decision_file(result["experiment_id"]).exists()
    assert not os_store.memory_file(str(hypothesis["hypothesis_id"])).exists()


def test_run_hypothesis_rejects_non_ready_status(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _reset_tradex_root(monkeypatch, tmp_path)
    hypothesis = _hypothesis_payload(status="draft")
    hypothesis_path = tmp_path / "draft-hypothesis.json"
    hypothesis_path.write_text(json.dumps(hypothesis, ensure_ascii=False, indent=2), encoding="utf-8")

    with pytest.raises(ValueError, match="hypothesis.status must be ready"):
        os_runner.run_hypothesis(hypothesis_path)
