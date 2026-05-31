from __future__ import annotations

from scripts.tradex_short_scene_visual_additive_pool_gate_v1 import _apply_crowded_pool_veto


def test_apply_crowded_pool_veto_keeps_only_uncrowded_dates() -> None:
    selected = {
        20260401: {"dt": 20260401, "code": "2001"},
        20260402: {"dt": 20260402, "code": "2002"},
    }
    groups = {
        20260401: [{"code": str(i)} for i in range(9)],
        20260402: [{"code": str(i)} for i in range(10)],
    }

    gated = _apply_crowded_pool_veto(selected, groups, max_existing_pool_size=9)

    assert set(gated) == {20260401}
