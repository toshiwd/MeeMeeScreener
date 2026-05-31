from __future__ import annotations

import pandas as pd

from scripts import tradex_current_buyable_invalidation_tracking_status_v1 as mod


def _contract() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"as_of_date": 20260520, "code": "8086", "primary_invalidation_level": 95.0, "invalidation_reason": "atr"},
            {"as_of_date": 20260520, "code": "9831", "primary_invalidation_level": 50.0, "invalidation_reason": "atr"},
        ]
    )


def _bars(hit: bool = False) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"code": "8086", "bar_date": 20260520, "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0},
            {"code": "8086", "bar_date": 20260521, "open": 100.0, "high": 101.0, "low": 98.0, "close": 94.0 if hit else 99.0},
            {"code": "9831", "bar_date": 20260520, "open": 60.0, "high": 61.0, "low": 58.0, "close": 60.0},
            {"code": "9831", "bar_date": 20260521, "open": 60.0, "high": 62.0, "low": 59.0, "close": 61.0},
        ]
    )


def test_build_tracking_rows_marks_active_without_hit() -> None:
    rows = mod.build_tracking_rows(_contract(), _bars(hit=False))
    assert rows["invalidation_hit"].eq(False).all()
    assert rows["tracking_status"].eq("active_pending_ret5").all()


def test_build_tracking_rows_marks_invalidation_hit() -> None:
    rows = mod.build_tracking_rows(_contract(), _bars(hit=True))
    hit = rows[rows["code"] == "8086"].iloc[0]
    assert bool(hit["invalidation_hit"]) is True
    assert hit["first_invalidation_hit_date"] == 20260521
    assert hit["tracking_status"] == "invalidated"


def test_no_lookahead_accepts_repaired_contract_and_pending_forward() -> None:
    audit = mod.no_lookahead_audit(
        {"research_decision": "invalidation_contract_repaired_full_levels_ready"},
        {"research_decision": "forward_validation_pending_more_confirmed_bars"},
    )
    assert audit["no_lookahead_pass"] is True
    assert audit["future_outcomes_used_for_selection"] is False


def test_no_lookahead_accepts_v2_stop_contract() -> None:
    audit = mod.no_lookahead_audit(
        {"research_decision": "invalidation_contract_v2_stop_atr2_ready_for_forward_tracking"},
        {"research_decision": "forward_validation_pending_more_confirmed_bars"},
    )
    assert audit["no_lookahead_pass"] is True


def test_decide_holds_when_no_invalidation_hit() -> None:
    rows = mod.build_tracking_rows(_contract(), _bars(hit=False))
    decision, decision_class, reasons = mod.decide(rows, {"no_lookahead_pass": True})
    assert decision == "current_candidates_active_no_invalidation_hit"
    assert decision_class == "HOLD_UNDERPOWERED"
    assert "no_primary_invalidation_hit_ret5_ret20_still_pending" in reasons
