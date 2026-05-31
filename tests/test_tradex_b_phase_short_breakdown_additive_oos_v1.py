from __future__ import annotations

from scripts.tradex_b_phase_short_breakdown_additive_oos_v1 import BASE_SIGNAL_KEY, REQUIRED_SHAPE_INTENT


def test_b_phase_axis_targets_short_breakdown_only() -> None:
    assert REQUIRED_SHAPE_INTENT == "b_phase_breakdown_down"
    assert BASE_SIGNAL_KEY == "sideways_b_phase|sell_breakdown_or_rejection|short"
