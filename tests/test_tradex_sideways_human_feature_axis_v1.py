from __future__ import annotations

import numpy as np
import pandas as pd

from scripts.tradex_sideways_human_feature_axis_v1 import event_features, feature_sets


def bars(close: np.ndarray) -> pd.DataFrame:
    open_ = np.r_[close[0], close[:-1]]
    return pd.DataFrame({"o": open_, "h": np.maximum(open_, close) + 1, "l": np.minimum(open_, close) - 1, "c": close, "v": 1000})


def test_event_features_distinguish_flat_from_trending_history() -> None:
    flat_close = 100 + np.sin(np.arange(100) * np.pi / 2)
    trend_close = np.arange(100, 200, dtype=float)
    flat = event_features(bars(flat_close))
    trend = event_features(bars(trend_close))
    assert flat["efficiency_15"] < trend["efficiency_15"]
    assert flat["slope_share_15"] < trend["slope_share_15"]
    assert flat["direction_flip_share15"] > trend["direction_flip_share15"]


def test_feature_sets_add_exactly_one_axis_at_a_time() -> None:
    sets = feature_sets()
    assert list(sets) == ["baseline", "duration", "high_low_compression", "center_movement", "candle_structure", "trend_pause"]
    previous: set[str] = set()
    for features in sets.values():
        current = set(features)
        assert previous <= current
        assert len(current) > len(previous)
        previous = current
