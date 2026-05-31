from __future__ import annotations

from scripts.tradex_short_scene_visual_additive_a_phase_slope_floor_v1 import MA20_SLOPE_10_FLOOR, _apply_slope_floor


def test_apply_slope_floor_drops_too_steep_candidates() -> None:
    rows = [
        {"code": "1001", "ma20_slope_10": -0.02},
        {"code": "1002", "ma20_slope_10": -0.01},
        {"code": "1003", "ma20_slope_10": -0.003},
        {"code": "1004", "ma20_slope_10": None},
    ]

    kept = _apply_slope_floor(rows, floor=MA20_SLOPE_10_FLOOR)

    assert [row["code"] for row in kept] == ["1002", "1003"]
