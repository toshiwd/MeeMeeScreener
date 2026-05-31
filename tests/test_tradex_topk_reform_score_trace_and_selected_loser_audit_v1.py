from __future__ import annotations

import pandas as pd

from scripts.tradex_topk_reform_score_trace_and_selected_loser_audit_v1 import _parse_components, build_trace_rows, decide


def test_parse_components_falls_back_to_unattributed_score() -> None:
    components, available = _parse_components(None, 12)

    assert not available
    assert components[0]["feature"] == "unattributed_score"


def test_build_trace_rows_exports_existing_components_without_fabricating_reason_codes() -> None:
    rows = pd.DataFrame(
        [
            {
                "code": "1001",
                "decision_ymd": 20240104,
                "baseline_rank": 1,
                "baseline_score": 12,
                "score_components_json": '[{"feature":"daily_ma_stack","points":3,"value":"bull"}]',
                "entry_allowed_by_score": True,
                "downside_guard_blocked": False,
                "next_open_available": True,
                "source_artifact_path": "x.csv",
                "source_run_id": "run",
                "ret20": -0.1,
                "ret20_pct_rank_by_date": 0.1,
                "selected_loser": True,
                "selected_winner": False,
                "year": 2024,
            }
        ]
    )

    trace, report = build_trace_rows(rows)

    assert report["score_component_attribution_available_rate"] == 1.0
    assert trace.iloc[0]["reason_codes"] == "[]"
    assert "daily_ma_stack" in trace.iloc[0]["score_component_json"]


def test_decide_score_contract_gap_when_trace_fields_missing() -> None:
    profile = pd.DataFrame()
    gap = {"missing_fields_union": ["candidate_source", "signal_family", "setup_name", "reason_code"]}
    decision = decide(profile, gap, {"score_component_attribution_available_rate": 1.0})

    assert decision["research_decision"] == "score_contract_gap"
