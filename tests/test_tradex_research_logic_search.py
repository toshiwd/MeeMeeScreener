from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from app.backend.tools import tradex_research_runner as runner


def _fake_candidate(
    *,
    method_family: str,
    method_id: str,
    method_title: str,
    promote_ready: bool,
    top5: float,
    top10: float,
    monthly_capture: float,
    worst_regime: float,
    dd: float,
    turnover: float,
    liquidity_fail_rate: float,
    sample_count: int,
    signal_bias: str = "balanced",
) -> dict[str, object]:
    return {
        "plan_id": method_id,
        "decision": "keep" if promote_ready else "hold",
        "promote_ready": promote_ready,
        "promote_reasons": [{"code": "test"}],
        "candidate_method": {
            "method_family": method_family,
            "method_id": method_id,
            "method_title": method_title,
            "method_thesis": f"{method_title} thesis",
        },
        "diagnostics": {
            "candidate_effective_config": {
                "minimum_confidence": 0.62,
                "minimum_ready_rate": 0.55,
                "signal_bias": signal_bias,
                "top_k": 5,
                "playbook_up_score_bonus": 0.01,
                "playbook_down_score_bonus": 0.0,
                "bad_pick_penalty_scale": 2.0,
            }
        },
        "evaluation_summary": {
            "promote_ready": promote_ready,
            "sample_count": sample_count,
            "meaningful_topk_branching_possible": True,
            "challenger_topk_ret20_mean": top5,
            "challenger_topk10_ret20_mean": top10,
            "challenger_monthly_top5_capture": {"mean": monthly_capture},
            "challenger_worst_regime_ret20_mean": worst_regime,
            "challenger_dd": dd,
            "challenger_turnover": turnover,
            "challenger_liquidity_fail_rate": liquidity_fail_rate,
            "challenger_zero_pass_months": 0,
        },
    }


def _fake_family_result(*, method_family: str, candidate: dict[str, object]) -> dict[str, object]:
    return {
        "family_id": f"family-{method_family}",
        "method_family": method_family,
        "family_title": f"{method_family} family",
        "family_thesis": f"{method_family} thesis",
        "candidate_count": 1,
        "candidate_order": [candidate["plan_id"]],
        "compare_path": f"compare/{method_family}.json",
        "compare": {"candidate_results": [candidate]},
        "candidate_results": [candidate],
        "best_candidate": candidate,
        "promote_ready": bool(candidate["promote_ready"]),
        "promote_reasons": candidate["promote_reasons"],
        "best_method_title": candidate["candidate_method"]["method_title"],
        "best_method_thesis": candidate["candidate_method"]["method_thesis"],
    }


def _fake_research_session(*, session_id: str, random_seed: int, session_scope_id: str | None = None, family_specs=None, **kwargs) -> dict[str, object]:
    del kwargs, family_specs
    regime_candidate = _fake_candidate(
        method_family="regime-aware",
        method_id="regime_aware_v1",
        method_title="Regime aware v1",
        promote_ready=True,
        top5=0.08 + (random_seed % 3) * 0.005,
        top10=0.06 + (random_seed % 3) * 0.004,
        monthly_capture=0.18,
        worst_regime=0.05,
        dd=0.03,
        turnover=0.12,
        liquidity_fail_rate=0.01,
        sample_count=18,
    )
    prune_candidate = _fake_candidate(
        method_family="bad-pick-prune",
        method_id="bad_pick_prune_v1",
        method_title="Bad pick prune v1",
        promote_ready=False,
        top5=0.03 + (random_seed % 2) * 0.002,
        top10=0.01,
        monthly_capture=0.08,
        worst_regime=-0.02,
        dd=0.06,
        turnover=0.18,
        liquidity_fail_rate=0.03,
        sample_count=18,
    )
    family_results = [
        _fake_family_result(method_family="regime-aware", candidate=regime_candidate),
        _fake_family_result(method_family="bad-pick-prune", candidate=prune_candidate),
    ]
    best_result = regime_candidate
    return {
        "status": "complete",
        "session_id": session_id,
        "session_scope_id": session_scope_id or session_id,
        "random_seed": random_seed,
        "manifest_hash": f"hash-{session_id}",
        "family_results": family_results,
        "best_result": best_result,
        "phase4": {"status": "skipped"},
        "eval_window_mode": "standard",
        "ret20_source_mode": runner.tradex.TRADEX_RET20_SOURCE_MODE_PRECOMPUTED,
    }


def test_tradex_logic_search_ranks_best_family_and_writes_artifacts(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(tmp_path / "tradex-root"))
    monkeypatch.setattr(runner, "run_tradex_research_session", _fake_research_session)
    monkeypatch.setattr(runner, "tradex_reports_root", lambda: tmp_path / "reports")

    result = runner.run_tradex_logic_search(
        search_id="logic-search",
        random_seeds=[7, 11],
        session_scope_ids=["scope-a", "scope-b"],
        universe_size=20,
        max_candidates_per_family=2,
        rounds=1,
    )

    assert result["status"] == "complete"
    assert result["schema_version"] == runner.LOGIC_SEARCH_ROLLUP_SCHEMA_VERSION
    assert result["session_count"] == 4
    assert result["candidate_count"] == 8
    assert result["family_count"] == 2
    assert result["best_candidate"]["method_family"] == "regime-aware"
    assert result["best_candidate"]["method_id"] == "regime_aware_v1"
    assert result["best_family"]["method_family"] == "regime-aware"
    assert result["next_round_proposals"]
    assert result["next_round_proposals"][0]["method_family"] == "regime-aware"

    rollup_path = Path(result["rollup_path"])
    report_path = Path(result["report_path"])
    assert rollup_path.exists()
    assert report_path.exists()

    rollup = json.loads(rollup_path.read_text(encoding="utf-8"))
    assert rollup["schema_version"] == runner.LOGIC_SEARCH_ROLLUP_SCHEMA_VERSION
    assert rollup["best_family"]["method_family"] == "regime-aware"
    assert rollup["candidate_rows"][0]["mutation_hints"]
    assert "Next Round Proposals" in report_path.read_text(encoding="utf-8")


def test_tradex_logic_search_auto_explores_mutated_families(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(tmp_path / "tradex-root"))
    monkeypatch.setattr(runner, "tradex_reports_root", lambda: tmp_path / "reports")

    observed_family_specs: list[object] = []

    def _recording_research_session(*, family_specs=None, **kwargs) -> dict[str, object]:
        observed_family_specs.append(family_specs)
        return _fake_research_session(family_specs=family_specs, **kwargs)

    monkeypatch.setattr(runner, "run_tradex_research_session", _recording_research_session)

    result = runner.run_tradex_logic_search(
        search_id="logic-search-auto",
        random_seeds=[7],
        session_scope_ids=["scope-a"],
        universe_size=20,
        max_candidates_per_family=2,
        rounds=2,
        max_mutated_families=2,
        max_mutations_per_family=2,
    )

    assert result["round_count"] == 2
    assert len(result["round_summaries"]) == 2
    assert observed_family_specs[0] is None
    assert observed_family_specs[-1] is not None
    assert any("Auto search /" in spec.family_title for spec in observed_family_specs[-1])
    assert any("r1__" in candidate.method_id for spec in observed_family_specs[-1] for candidate in spec.candidates)
    assert result["best_candidate"]["method_family"] == "regime-aware"


def test_tradex_logic_search_cli_smoke(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(tmp_path / "tradex-root"))
    monkeypatch.setattr(runner, "run_tradex_research_session", _fake_research_session)
    monkeypatch.setattr(runner, "tradex_reports_root", lambda: tmp_path / "reports")

    argv = [
        "tradex_research_runner",
        "--session-id",
        "logic-search-cli",
        "--random-seed",
        "7",
        "--logic-search",
        "--logic-search-seeds",
        "7,11",
        "--logic-search-scope-ids",
        "scope-a,scope-b",
        "--logic-search-universe-size",
        "20",
        "--logic-search-max-candidates-per-family",
        "2",
        "--logic-search-rounds",
        "1",
    ]
    monkeypatch.setattr(sys, "argv", argv)
    assert runner.main() == 0


def test_tradex_period_segments_use_exploratory_fallback_when_needed(monkeypatch) -> None:
    regime_rows = [
        {"dt": 20250101, "regime_id": "risk_on_trend"},
        {"dt": 20250102, "regime_id": "risk_on_trend"},
        {"dt": 20250103, "regime_id": "neutral_range"},
    ]

    def _fake_load_evaluation_regime_rows():
        return regime_rows, []

    def _fake_select_evaluation_windows(rows, *, min_trading_days):
        if min_trading_days >= runner.tradex.TRADEX_STANDARD_EVAL_WINDOW_MIN_TRADING_DAYS:
            return [], ["missing_up_window", "missing_down_window", "missing_flat_window"]
        if min_trading_days >= runner.tradex.TRADEX_RESEARCH_FALLBACK_EVAL_WINDOW_MIN_TRADING_DAYS:
            return [], ["missing_up_window", "missing_down_window", "missing_flat_window"]
        return (
            [
                {
                    "evaluation_window_id": "up:1:2",
                    "regime_tag": "up",
                    "regime_id": "risk_on_trend",
                    "regime_ids": ["risk_on_trend"],
                    "start_date": "2025-01-01",
                    "end_date": "2025-01-02",
                    "start_dt": 20250101,
                    "end_dt": 20250102,
                    "trading_day_count": 2,
                },
                {
                    "evaluation_window_id": "down:3:4",
                    "regime_tag": "down",
                    "regime_id": "risk_off_trend",
                    "regime_ids": ["risk_off_trend"],
                    "start_date": "2025-01-03",
                    "end_date": "2025-01-04",
                    "start_dt": 20250103,
                    "end_dt": 20250104,
                    "trading_day_count": 2,
                },
                {
                    "evaluation_window_id": "flat:5:6",
                    "regime_tag": "flat",
                    "regime_id": "neutral_range",
                    "regime_ids": ["neutral_range"],
                    "start_date": "2025-01-05",
                    "end_date": "2025-01-06",
                    "start_dt": 20250105,
                    "end_dt": 20250106,
                    "trading_day_count": 2,
                },
            ],
            [],
        )

    monkeypatch.setattr(runner.tradex, "_load_evaluation_regime_rows", _fake_load_evaluation_regime_rows)
    monkeypatch.setattr(runner.tradex, "_select_evaluation_windows", _fake_select_evaluation_windows)

    segments, meta = runner._build_period_segments_with_mode()

    assert len(segments) == 3
    assert meta["mode"] == "exploratory"
    assert meta["mode_reason"].startswith("exploratory_required_standard_and_fallback_windows_unavailable")
