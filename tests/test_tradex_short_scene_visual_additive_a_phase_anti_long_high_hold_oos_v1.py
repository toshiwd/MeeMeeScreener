from __future__ import annotations

from scripts.tradex_short_scene_visual_additive_a_phase_anti_long_high_hold_oos_v1 import (
    _anti_long_high_hold_features,
    _apply_anti_long_high_hold,
)


def _bar(close: float, high: float | None = None, low: float | None = None) -> dict:
    return {"c": close, "h": high if high is not None else close + 1, "l": low if low is not None else close - 1}


def test_anti_long_high_hold_features_detects_high_hold_uptrend() -> None:
    window = [_bar(100 + index) for index in range(240)]

    features = _anti_long_high_hold_features(window)

    assert features["confirmed"] is True
    assert features["anti_short_high_hold"] is True


def test_anti_long_high_hold_features_does_not_reject_low_zone() -> None:
    window = [_bar(100 + index) for index in range(180)] + [_bar(280 - index * 3) for index in range(60)]

    features = _anti_long_high_hold_features(window)

    assert features["confirmed"] is True
    assert features["anti_short_high_hold"] is False


def test_apply_anti_long_high_hold_splits_pass_and_reject() -> None:
    high_hold = [_bar(100 + index) for index in range(240)]
    low_zone = [_bar(100 + index) for index in range(180)] + [_bar(280 - index * 3) for index in range(60)]
    events = [{"dt": 20250110, "code": "1001"}, {"dt": 20250110, "code": "1002"}]
    windows = {(20250110, "1001"): high_hold, (20250110, "1002"): low_zone}

    passed, rejected = _apply_anti_long_high_hold(events, windows)

    assert [row["code"] for row in rejected] == ["1001"]
    assert [row["code"] for row in passed] == ["1002"]
