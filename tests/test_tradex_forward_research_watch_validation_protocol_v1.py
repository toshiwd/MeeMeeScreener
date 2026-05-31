from __future__ import annotations

import pandas as pd

from scripts import tradex_forward_research_watch_validation_protocol_v1 as mod


def _surface() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "as_of_date": 20260420,
                "code": "1002",
                "forward_research_watch_score": 0.2,
                "forward_research_watch_rank": 2,
                "forward_research_watch_bucket": "watch_top10_unvalidated",
                "forward_surface_live_feature_available_flag": True,
            },
            {
                "as_of_date": 20260420,
                "code": "1001",
                "forward_research_watch_score": 0.9,
                "forward_research_watch_rank": 1,
                "forward_research_watch_bucket": "watch_top10_unvalidated",
                "forward_surface_live_feature_available_flag": True,
            },
            {
                "as_of_date": 20260420,
                "code": "1003",
                "forward_research_watch_score": 0.1,
                "forward_research_watch_rank": 3,
                "forward_research_watch_bucket": "watch_top10_unvalidated",
                "forward_surface_live_feature_available_flag": True,
            },
        ]
    )


def _source_decision() -> dict[str, object]:
    return {
        "research_decision": "forward_current_surface_ready_for_research_watch_pretest",
        "research_watch_only": True,
        "buyable_selection_ready": False,
    }


def test_build_watch_rows_freezes_top_n_without_buy_claim() -> None:
    rows = mod.build_watch_rows(_surface(), top_n=2)
    assert rows["code"].tolist() == ["1001", "1002"]
    assert rows["forward_watch_protocol_rank"].tolist() == [1, 2]
    assert rows["buy_recommendation"].eq(False).all()
    assert rows["validated_buy"].eq(False).all()
    assert rows["active_gate_created"].eq(False).all()


def test_no_lookahead_blocks_outcome_columns_in_watch_rows() -> None:
    rows = mod.build_watch_rows(_surface(), top_n=2)
    assert mod.no_lookahead_audit(rows, _source_decision())["no_lookahead_pass"] is True
    bad = rows.copy()
    bad["ret20"] = 0.1
    audit = mod.no_lookahead_audit(bad, _source_decision())
    assert audit["no_lookahead_pass"] is False
    assert audit["offline_outcome_columns_present_in_watch_rows"] == ["ret20"]


def test_future_outcome_join_contract_keeps_outcomes_evaluation_only() -> None:
    contract = mod.future_outcome_join_contract()
    assert "ret20" in contract["allowed_future_outcome_columns"]
    assert contract["live_feature_construction_after_join_allowed"] is False
    assert contract["research_watch_only"] is True


def test_decide_ready_is_hold_underpowered_not_buyable() -> None:
    rows = mod.build_watch_rows(_surface(), top_n=2)
    audit = mod.no_lookahead_audit(rows, _source_decision())
    decision, decision_class, reasons = mod.decide(rows, audit, _source_decision())
    assert decision == "forward_watch_protocol_ready_for_future_outcome_validation"
    assert decision_class == "HOLD_UNDERPOWERED"
    assert "not_buyable_until_future_outcomes_validate_selector" in reasons
