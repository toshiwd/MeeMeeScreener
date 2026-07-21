import pandas as pd

from scripts.tradex_failed_high_priority_promotion_v1 import (
    _interleave_promoted,
    is_failed_high_keep,
)


def test_keep_atoms_are_exact_and_stage_sensitive():
    signal = {"peak_age": 120, "peak_prominence": 0.03, "pullback_depth": 0.20, "stage": "forming"}
    assert is_failed_high_keep(signal)
    assert not is_failed_high_keep({**signal, "stage": "confirmed"})
    assert not is_failed_high_keep({**signal, "peak_age": 119})


def test_sell_match_is_promoted_without_changing_buy_order_or_suppressing_candidates():
    day = pd.DataFrame([
        {"code": "B1", "side": "buy", "rank": 1, "failed_high_promotion": False},
        {"code": "B2", "side": "buy", "rank": 2, "failed_high_promotion": False},
        {"code": "S1", "side": "sell", "rank": 1, "failed_high_promotion": False},
        {"code": "S2", "side": "sell", "rank": 2, "failed_high_promotion": True},
    ])
    selected = _interleave_promoted(day)
    assert selected.code.tolist() == ["B1", "S2", "B2"]
    assert selected.global_rank.tolist() == [1, 2, 3]
    assert len(day) == 4
