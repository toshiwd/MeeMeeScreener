from __future__ import annotations

import pandas as pd

from scripts import tradex_current_buyable_forward_paper_validation_v1 as mod


def _candidates() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"as_of_date": 20260520, "code": "8086", "research_buyable_rank": 1},
            {"as_of_date": 20260520, "code": "9831", "research_buyable_rank": 2},
        ]
    )


def _bars(days: int, end_multiplier: float = 1.1) -> pd.DataFrame:
    rows = []
    for code in ["8086", "9831"]:
        for idx in range(days + 1):
            close = 100.0 * (1.0 + (end_multiplier - 1.0) * idx / max(days, 1))
            rows.append({"code": code, "bar_date": 20260520 + idx, "open": close, "high": close * 1.01, "low": close * 0.99, "close": close})
    return pd.DataFrame(rows)


def test_attach_forward_outcomes_marks_pending_before_ret5() -> None:
    rows = mod.attach_forward_outcomes(_candidates(), _bars(2))
    assert rows["available_future_session_count"].tolist() == [2, 2]
    assert rows["ret5"].isna().all()
    assert rows["status"].eq("pending_ret5").all()
    assert rows["validated_buy"].eq(False).all()


def test_attach_forward_outcomes_computes_ret5_and_drawdown() -> None:
    rows = mod.attach_forward_outcomes(_candidates(), _bars(5, end_multiplier=1.05))
    assert rows["ret5"].notna().all()
    assert rows["ret20"].isna().all()
    assert rows["status"].eq("ret5_ready_ret20_pending").all()
    assert rows["max_drawdown_5d"].notna().all()


def test_decide_pending_when_less_than_five_future_sessions() -> None:
    rows = mod.attach_forward_outcomes(_candidates(), _bars(2))
    status = mod.outcome_window_status(rows)
    metrics = mod.ret_metrics(rows)
    audit = {"no_lookahead_pass": True}
    decision, decision_class, reasons = mod.decide(status, metrics, audit)
    assert decision == "forward_validation_pending_more_confirmed_bars"
    assert decision_class == "HOLD_UNDERPOWERED"
    assert "future_confirmed_sessions_below_ret5_window" in reasons


def test_decide_ret20_passes_only_after_full_window() -> None:
    rows = mod.attach_forward_outcomes(_candidates(), _bars(20, end_multiplier=1.25))
    status = mod.outcome_window_status(rows)
    metrics = mod.ret_metrics(rows)
    audit = {"no_lookahead_pass": True}
    decision, decision_class, reasons = mod.decide(status, metrics, audit)
    assert decision == "ret20_pass_ready_for_robustness_gate"
    assert decision_class == "KEEP"
    assert "ret20_forward_paper_gate_passed_next_robustness_gate" in reasons


def test_freeze_contract_records_no_replacement() -> None:
    summary = {
        "as_of_date": 20260520,
        "research_buyable_candidate_codes": ["8086", "9831"],
        "buyable_selection_ready": True,
        "validated_buy_count": 0,
    }
    contract = mod.build_candidate_freeze_contract(mod.DEFAULT_PROJECTION_ROOT, mod.DEFAULT_RISK_ROOT, summary, "ts")
    assert contract["selected_codes"] == ["8086", "9831"]
    assert contract["no_candidate_replacement"] is True
    assert contract["validated_buy_count_at_projection"] == 0
