from __future__ import annotations

from scripts.tradex_c_phase_long_touch_bounce_guarded_oos_v1 import (
    MAX_ABS_MA20_DISTANCE,
    MAX_LATEST_PRICE_POSITION,
    _pass_guard,
)


def test_c_phase_guard_accepts_only_lower_position_close_to_ma20() -> None:
    assert MAX_LATEST_PRICE_POSITION == 0.66
    assert MAX_ABS_MA20_DISTANCE == 0.01
    assert _pass_guard({"latest_price_position_pct": 0.65, "ma20_distance": -0.009}) is True
    assert _pass_guard({"latest_price_position_pct": 0.67, "ma20_distance": 0.001}) is False
    assert _pass_guard({"latest_price_position_pct": 0.6, "ma20_distance": 0.011}) is False
    assert _pass_guard({"latest_price_position_pct": None, "ma20_distance": 0.001}) is False
