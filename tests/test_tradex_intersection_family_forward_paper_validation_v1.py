from __future__ import annotations

import pandas as pd

from scripts import tradex_intersection_family_forward_paper_validation_v1 as mod


def _rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"as_of_date": 20260520, "code": "1002", "fresh_runtime_research_watch_rank": 2, "fresh_runtime_research_watch_score": 0.8, "buy_entry_qualified": True, "buy_breakout_surface": True, "variant_b_entry_qualified_top50": True},
            {"as_of_date": 20260520, "code": "1001", "fresh_runtime_research_watch_rank": 1, "fresh_runtime_research_watch_score": 0.9, "buy_entry_qualified": True, "buy_breakout_surface": True, "variant_b_entry_qualified_top50": True},
            {"as_of_date": 20260519, "code": "1003", "fresh_runtime_research_watch_rank": 1, "fresh_runtime_research_watch_score": 0.7, "buy_entry_qualified": True, "buy_breakout_surface": True, "variant_b_entry_qualified_top50": True},
        ]
    )


def test_current_candidate_shape_matches_freeze_contract() -> None:
    rows = _rows()
    latest = rows[rows["as_of_date"] == rows["as_of_date"].max()].sort_values(["fresh_runtime_research_watch_rank", "code"]).copy()
    latest["forward_paper_rank"] = range(1, len(latest) + 1)
    latest["buy_recommendation"] = False
    latest["validated_buy"] = False
    assert latest["code"].tolist() == ["1001", "1002"]
    assert latest["forward_paper_rank"].tolist() == [1, 2]
    assert latest["validated_buy"].eq(False).all()


def test_validation_schedule_has_ret5_and_ret20() -> None:
    sched = mod.validation_schedule(20260520)
    windows = {w["outcome_column"] for w in sched["validation_windows"]}
    assert windows == {"ret5", "ret20"}
    assert sched["validated_buy_count"] == 0


def test_no_lookahead_blocks_outcome_columns() -> None:
    rows = _rows().copy()
    decision = {"research_decision": "intersection_family_ready_for_forward_paper_validation"}
    assert mod.no_lookahead_audit(rows, decision)["no_lookahead_pass"] is True
    rows["ret20"] = 0.1
    audit = mod.no_lookahead_audit(rows, decision)
    assert audit["no_lookahead_pass"] is False
    assert audit["offline_outcome_columns_present_in_candidate_rows"] == ["ret20"]


def test_decide_keep_when_candidates_exist_and_audit_passes() -> None:
    decision, decision_class, reasons = mod.decide(_rows(), {"no_lookahead_pass": True})
    assert decision == "intersection_family_forward_paper_candidates_frozen"
    assert decision_class == "KEEP"
    assert reasons
