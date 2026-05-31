from __future__ import annotations

from scripts.tradex_short_scene_visual_screenshot_gate_v1 import _rollup


def test_rollup_rejects_when_all_candidates_rejected_by_screenshot() -> None:
    result = _rollup([{"screenshot_gate": "reject"}])

    assert result["judgment"] == "hold_screenshot_rejected"
    assert result["paper_replay_ready"] is True


def test_rollup_continues_when_any_candidate_passes() -> None:
    result = _rollup([{"screenshot_gate": "reject"}, {"screenshot_gate": "pass"}])

    assert result["judgment"] == "continue_live_shadow_screenshot_confirmed"
    assert result["paper_replay_ready"] is True


def test_rollup_blocks_when_screenshot_missing() -> None:
    result = _rollup([{"screenshot_gate": "blocked"}])

    assert result["judgment"] == "hold"
    assert result["paper_replay_ready"] is False
