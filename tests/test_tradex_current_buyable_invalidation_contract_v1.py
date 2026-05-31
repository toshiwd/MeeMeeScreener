from __future__ import annotations

import pandas as pd

from scripts import tradex_current_buyable_invalidation_contract_v1 as mod


def _candidates() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"as_of_date": 20260520, "code": "8086", "entry_close": 100.0},
            {"as_of_date": 20260520, "code": "9831", "entry_close": 200.0},
        ]
    )


def _snapshot() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"as_of_date": 20260520, "code": "8086", "close": 100.0, "ma7": 98.0, "ma20": 95.0, "ma60": 90.0, "atr14": 4.0},
            {"as_of_date": 20260520, "code": "9831", "close": 200.0, "ma7": 198.0, "ma20": 180.0, "ma60": 170.0, "atr14": 10.0},
        ]
    )


def _bars() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"code": "8086", "bar_date": 20260519, "low": 94.0},
            {"code": "8086", "bar_date": 20260520, "low": 96.0},
            {"code": "9831", "bar_date": 20260519, "low": 185.0},
            {"code": "9831", "bar_date": 20260520, "low": 188.0},
        ]
    )


def test_recent_swing_lows_uses_min_low_by_code() -> None:
    lows = mod.recent_swing_lows(_bars())
    assert lows.set_index("code").loc["8086", "recent_swing_low"] == 94.0
    assert lows.set_index("code").loc["9831", "recent_swing_low"] == 185.0


def test_build_invalidation_rows_selects_highest_long_stop() -> None:
    rows = mod.build_invalidation_rows(_candidates(), _snapshot(), mod.recent_swing_lows(_bars()))
    first = rows.set_index("code").loc["8086"]
    assert first["invalidation_close_below_ma20_flag_level"] == 95.0
    assert first["invalidation_atr_stop_level"] == 96.0
    assert first["primary_invalidation_level"] == 96.0
    assert first["invalidation_reason"] == "invalidation_atr_stop_level"


def test_no_lookahead_passes_with_complete_freeze_and_levels() -> None:
    rows = mod.build_invalidation_rows(_candidates(), _snapshot(), mod.recent_swing_lows(_bars()))
    freeze = {"no_candidate_replacement": True, "validated_buy_count_at_projection": 0}
    decision = {"research_decision": "forward_validation_pending_more_confirmed_bars"}
    audit = mod.no_lookahead_audit(rows, freeze, decision)
    assert audit["no_lookahead_pass"] is True
    assert audit["runtime_db_write"] is False


def test_decide_ready_when_audit_passes() -> None:
    rows = mod.build_invalidation_rows(_candidates(), _snapshot(), mod.recent_swing_lows(_bars()))
    decision, decision_class, reasons = mod.decide(rows, {"no_lookahead_pass": True})
    assert decision == "invalidation_contract_ready_for_forward_tracking"
    assert decision_class == "KEEP"
    assert "point_in_time_invalidation_levels_ready_for_frozen_candidates" in reasons
