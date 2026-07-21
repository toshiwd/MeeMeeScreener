from __future__ import annotations

import numpy as np
import pandas as pd

from scripts.tradex_sideways_variable_segment_v1 import long_context_features, variable_segment_features


def history(close: np.ndarray) -> pd.DataFrame:
    open_ = np.r_[close[0], close[:-1]]
    return pd.DataFrame({
        "o": open_, "h": np.maximum(open_, close) + 0.5, "l": np.minimum(open_, close) - 0.5,
        "c": close, "v": 1000, "ymd": np.arange(len(close)),
    })


def test_variable_segment_selects_flatter_recent_region() -> None:
    trend_then_flat = np.r_[np.linspace(50, 100, 200), 100 + np.sin(np.arange(60) * np.pi / 2)]
    trending = np.linspace(50, 110, 260)
    flat = variable_segment_features(history(trend_then_flat))
    trend = variable_segment_features(history(trending))
    assert flat["best_segment_plateau_score"] < trend["best_segment_plateau_score"]
    assert flat["best_segment_window"] >= 20


def test_long_context_uses_only_trailing_history_shape() -> None:
    close = np.linspace(100, 200, 260)
    features = long_context_features(history(close))
    assert features["context_ret_252"] > 0
    assert 0 <= features["context_range_pos_252"] <= 1
    assert len([key for key in features if key.startswith("context_shape_")]) == 12


def test_long_context_marks_unavailable_year_history_as_missing() -> None:
    close = np.linspace(100, 150, 168)
    features = long_context_features(history(close))
    assert features["context_history_bars"] == 168
    assert np.isnan(features["context_ret_252"])
    assert np.isnan(features["context_shape_00"])
    assert features["context_ret_126"] > 0
