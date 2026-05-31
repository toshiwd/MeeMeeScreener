from __future__ import annotations

import pandas as pd

from scripts import tradex_forward_watch_outcome_evaluation_v1 as mod


def _watch_rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"as_of_date": 20260420, "code": "1001", "forward_watch_protocol_rank": 1},
            {"as_of_date": 20260420, "code": "1002", "forward_watch_protocol_rank": 2},
        ]
    )


def _bars(days: int, end_multiplier: float = 1.2) -> pd.DataFrame:
    rows = []
    dates = [20260420 + i for i in range(days + 1)]
    for code in ["1001", "1002"]:
        for idx, date in enumerate(dates):
            close = 100.0 * (1.0 + (end_multiplier - 1.0) * idx / max(days, 1))
            rows.append({"code": code, "bar_date": date, "close": close})
    return pd.DataFrame(rows)


def _watch_decision() -> dict[str, object]:
    return {"research_decision": "forward_watch_protocol_ready_for_future_outcome_validation"}


def test_attach_forward_outcomes_keeps_pending_when_less_than_20_future_sessions() -> None:
    rows = mod.attach_forward_outcomes(_watch_rows(), _bars(2))
    assert rows["available_future_session_count"].tolist() == [2, 2]
    assert rows["ret5"].isna().all()
    assert rows["ret20"].isna().all()
    assert rows["buy_recommendation"].eq(False).all()


def test_attach_forward_outcomes_computes_ret20_after_20_future_sessions() -> None:
    rows = mod.attach_forward_outcomes(_watch_rows(), _bars(20, end_multiplier=1.3))
    assert rows["ret20"].notna().all()
    assert round(float(rows.iloc[0]["ret20"]), 6) == 0.3
    assert rows["winner_ret20_gt_10pct"].eq(True).all()


def test_decide_pending_until_full_outcome_coverage() -> None:
    rows = mod.attach_forward_outcomes(_watch_rows(), _bars(2))
    metrics = mod.metric_payload(rows)
    gate = mod.buyability_gate_audit(metrics)
    audit = mod.no_lookahead_audit(rows, _watch_decision())
    decision, decision_class, reasons = mod.decide(metrics, gate, audit)
    assert decision == "forward_watch_outcome_evaluation_pending_more_confirmed_bars"
    assert decision_class == "HOLD_UNDERPOWERED"
    assert "ret20_outcomes_not_available_for_all_watch_rows" in reasons


def test_buyability_gate_can_pass_only_after_metrics_are_available() -> None:
    rows = mod.attach_forward_outcomes(_watch_rows(), _bars(20, end_multiplier=1.3))
    metrics = mod.metric_payload(rows)
    gate = mod.buyability_gate_audit(metrics)
    assert gate["buyability_gate_pass"] is False
    expanded = pd.concat([rows] * 10, ignore_index=True)
    metrics = mod.metric_payload(expanded)
    gate = mod.buyability_gate_audit(metrics)
    assert gate["buyability_gate_pass"] is True
