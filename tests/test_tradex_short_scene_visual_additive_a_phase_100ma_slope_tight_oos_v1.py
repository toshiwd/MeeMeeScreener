from __future__ import annotations

from scripts.tradex_short_scene_visual_additive_a_phase_100ma_slope_tight_oos_v1 import REQUIRED_SHAPE_INTENT, _apply_100ma_rejection


def test_apply_100ma_rejection_keeps_only_required_shape() -> None:
    rows = [
        {"code": "1001", "shape_intent": REQUIRED_SHAPE_INTENT},
        {"code": "1002", "shape_intent": "a_phase_downtrend_60ma_rejection"},
    ]

    assert _apply_100ma_rejection(rows) == [rows[0]]
