from __future__ import annotations

from scripts.tradex_short_scene_visual_additive_a_phase_100ma_slope_tight_oos_v1 import REQUIRED_SHAPE_INTENT
from scripts.tradex_short_scene_visual_additive_a_phase_100ma_slope_tight_visual_consensus_oos_v1 import (
    _apply_visual_consensus,
    _short_visual_consensus_decision,
)


def _event(**overrides: object) -> dict:
    event = {
        "trade_side": "short",
        "shape_intent": REQUIRED_SHAPE_INTENT,
        "visual_decision": "pullback_probe_candidate",
        "ma20_slope_10": 0.0,
    }
    event.update(overrides)
    return event


def test_short_visual_consensus_passes_required_sell_shape() -> None:
    assert _short_visual_consensus_decision(_event()) == "pass"


def test_short_visual_consensus_rejects_wrong_side() -> None:
    assert _short_visual_consensus_decision(_event(trade_side="long")) == "reject"


def test_short_visual_consensus_rejects_slope_below_floor() -> None:
    assert _short_visual_consensus_decision(_event(ma20_slope_10=-0.006)) == "reject"


def test_apply_visual_consensus_keeps_only_passes() -> None:
    rows = [_event(code="1001"), _event(code="1002", trade_side="long")]

    filtered = _apply_visual_consensus(rows)

    assert [row["code"] for row in filtered] == ["1001"]
    assert filtered[0]["screenshot_proxy_gate"] == "pass"
