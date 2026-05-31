from __future__ import annotations

from scripts.tradex_short_scene_visual_additive_a_phase_slope_floor_tight_oos_v1 import MA20_SLOPE_10_FLOOR_TIGHT


def test_tight_floor_is_single_axis_minus_half_percent() -> None:
    assert MA20_SLOPE_10_FLOOR_TIGHT == -0.005
