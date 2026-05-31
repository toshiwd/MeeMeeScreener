from __future__ import annotations

import pandas as pd

from scripts import tradex_fresh_watch_outcome_availability_audit_v1 as mod


def _watch_rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"as_of_date": 20260522, "code": "1001", "fresh_watch_protocol_rank": 1},
            {"as_of_date": 20260522, "code": "1002", "fresh_watch_protocol_rank": 2},
        ]
    )


def _bars(session_count: int) -> pd.DataFrame:
    rows = []
    dates = [20260522 + i for i in range(session_count + 1)]
    for code in ["1001", "1002"]:
        for date in dates:
            rows.append({"code": code, "bar_date": date, "close": 100.0})
    return pd.DataFrame(rows)


def _watch_decision() -> dict[str, object]:
    return {"research_decision": "fresh_runtime_watch_protocol_ready_for_future_outcome_validation"}


def test_build_availability_rows_counts_only_future_sessions() -> None:
    rows = mod.build_availability_rows(_watch_rows(), _bars(0))
    assert rows["available_future_session_count"].tolist() == [0, 0]
    assert rows["ret5_evaluation_ready"].eq(False).all()
    assert rows["ret20_evaluation_ready"].eq(False).all()
    assert rows["buyable_selection_ready"].eq(False).all()


def test_status_marks_ret5_ready_only_when_all_rows_have_5_sessions() -> None:
    rows = mod.build_availability_rows(_watch_rows(), _bars(5))
    status = mod.outcome_window_status(rows)
    assert status["ret5_all_rows_ready"] is True
    assert status["ret20_all_rows_ready"] is False
    assert status["ret5_ready_count"] == 2


def test_no_lookahead_blocks_outcome_columns() -> None:
    rows = mod.build_availability_rows(_watch_rows(), _bars(0))
    assert mod.no_lookahead_audit(rows, _watch_decision())["no_lookahead_pass"] is True
    bad = rows.copy()
    bad["ret20"] = 0.1
    audit = mod.no_lookahead_audit(bad, _watch_decision())
    assert audit["no_lookahead_pass"] is False
    assert audit["forbidden_outcome_columns_present"] == ["ret20"]


def test_decide_pending_when_no_future_sessions() -> None:
    rows = mod.build_availability_rows(_watch_rows(), _bars(0))
    status = mod.outcome_window_status(rows)
    decision, decision_class, reasons = mod.decide(status, mod.no_lookahead_audit(rows, _watch_decision()))
    assert decision == "fresh_watch_outcomes_pending_more_confirmed_bars"
    assert decision_class == "HOLD_UNDERPOWERED"
    assert "insufficient_future_confirmed_sessions_for_fresh_ret5_ret20_evaluation" in reasons
