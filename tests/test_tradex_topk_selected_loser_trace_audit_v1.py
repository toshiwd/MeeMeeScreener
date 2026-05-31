from __future__ import annotations

import pandas as pd

from scripts.tradex_topk_selected_loser_trace_audit_v1 import repair_axis_candidates, score_component_failure_summary, trace_coverage_summary


def test_trace_coverage_summary_reports_missing_semantics() -> None:
    trace = pd.DataFrame(
        [
            {
                "candidate_source": None,
                "signal_family": None,
                "setup_name": None,
                "reason_codes_json": "[]",
                "regime_bucket": None,
                "score_component_attribution_available": True,
                "gate_flags_json": "{}",
                "risk_flags_json": "{}",
                "event_flags_json": "{}",
                "liquidity_flags_json": "{}",
            }
        ]
    )

    coverage = trace_coverage_summary(trace)

    assert coverage["candidate_source_available_rate"] == 0.0
    assert coverage["score_component_attribution_available_rate"] == 1.0


def test_score_component_failure_summary_computes_loser_winner_spread() -> None:
    trace = pd.DataFrame(
        [
            _row("A", 1, True, False, -0.1),
            _row("B", 2, False, True, 0.1),
        ]
    )

    summary = score_component_failure_summary(trace)

    assert not summary.empty
    assert "loser_minus_winner_spread" in summary.columns


def test_repair_axis_candidates_prefers_contract_when_source_missing() -> None:
    coverage = {"rows": 10, "candidate_source_available_rate": 0.0, "signal_family_available_rate": 0.0}
    candidates = repair_axis_candidates(pd.DataFrame(), pd.DataFrame(), coverage)

    assert candidates[0]["recommended_next"] == "contract_repair"


def _row(code: str, rank: int, loser: bool, winner: bool, ret20: float) -> dict[str, object]:
    return {
        "year": 2024,
        "decision_date": 20240104,
        "code": code,
        "baseline_rank": rank,
        "score_components_json": '{"daily_ma_stack":{"points":3,"value":"bull"}}',
        "selected_loser": loser,
        "selected_winner": winner,
        "ret20": ret20,
    }
