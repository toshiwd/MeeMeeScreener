from __future__ import annotations

import pandas as pd

from scripts import tradex_intersection_family_forward_outcome_availability_audit_v1 as mod


def _forward_rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"as_of_date": 20260520, "code": "1001", "forward_paper_rank": 1},
            {"as_of_date": 20260520, "code": "1002", "forward_paper_rank": 2},
        ]
    )


def _bars(session_count: int) -> pd.DataFrame:
    rows = []
    dates = [20260520 + i for i in range(session_count + 1)]
    for code in ["1001", "1002"]:
        for date in dates:
            rows.append({"code": code, "bar_date": date, "close": 100.0})
    return pd.DataFrame(rows)


def _forward_decision() -> dict[str, object]:
    return {"research_decision": "intersection_family_forward_paper_candidates_frozen"}


def test_build_availability_rows_counts_future_sessions_only() -> None:
    rows = mod.build_availability_rows(_forward_rows(), _bars(0))
    assert rows["available_future_session_count"].tolist() == [0, 0]
    assert rows["ret5_evaluation_ready"].eq(False).all()
    assert rows["ret20_evaluation_ready"].eq(False).all()
    assert rows["validated_buy"].eq(False).all()


def test_status_marks_ret5_ready_before_ret20() -> None:
    rows = mod.build_availability_rows(_forward_rows(), _bars(5))
    status = mod.outcome_window_status(rows)
    assert status["ret5_all_rows_ready"] is True
    assert status["ret20_all_rows_ready"] is False
    assert status["ret5_ready_count"] == 2


def test_no_lookahead_blocks_outcome_columns_before_evaluation() -> None:
    rows = mod.build_availability_rows(_forward_rows(), _bars(0))
    assert mod.no_lookahead_audit(rows, _forward_decision())["no_lookahead_pass"] is True
    bad = rows.copy()
    bad["ret20"] = 0.1
    audit = mod.no_lookahead_audit(bad, _forward_decision())
    assert audit["no_lookahead_pass"] is False
    assert audit["forbidden_outcome_columns_present"] == ["ret20"]


def test_decide_pending_when_future_sessions_are_insufficient() -> None:
    rows = mod.build_availability_rows(_forward_rows(), _bars(0))
    status = mod.outcome_window_status(rows)
    decision, decision_class, reasons = mod.decide(status, mod.no_lookahead_audit(rows, _forward_decision()))
    assert decision == "intersection_forward_outcomes_pending_more_confirmed_bars"
    assert decision_class == "HOLD_UNDERPOWERED"
    assert "insufficient_future_confirmed_sessions_for_ret5_ret20_evaluation" in reasons
