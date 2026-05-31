from __future__ import annotations

from scripts.tradex_short_scene_visual_additive_a_phase_v1 import BASE_SIGNAL_KEY


def test_a_phase_additive_axis_uses_downtrend_pullback_key() -> None:
    assert BASE_SIGNAL_KEY == "downtrend_a_phase|sell_rebound_rejection_or_lower_low|short|pullback_probe_candidate"
