from __future__ import annotations

from scripts.tradex_scene_iron_chart_candidate_board_v1 import ARTIFACTS


def test_candidate_board_tracks_required_scene_artifacts() -> None:
    assert "a_phase_downtrend_100ma_rejection" in ARTIFACTS
    assert "b_phase_short_breakdown" in ARTIFACTS
    assert "c_phase_20ma_touch_bounce" in ARTIFACTS
    assert "crash_bottom_20ma_reclaim_long" in ARTIFACTS
    assert "two_ma_simultaneous_breakout_weekly" in ARTIFACTS
