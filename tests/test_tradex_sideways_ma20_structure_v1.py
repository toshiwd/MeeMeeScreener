from __future__ import annotations

import numpy as np
import pandas as pd

from scripts.tradex_sideways_ma20_structure_v1 import ma20_features


def history(close: np.ndarray) -> pd.DataFrame:
    close = np.asarray(close, dtype=float)
    return pd.DataFrame({
        "ymd": np.arange(len(close)), "o": np.r_[close[0], close[:-1]],
        "h": close + 0.6, "l": close - 0.6, "c": close, "v": 1000.0,
    })


def test_flat_price_has_flatter_ma20_than_trend() -> None:
    flat = ma20_features(history(100 + np.sin(np.arange(100)) * 0.4))
    trend = ma20_features(history(np.linspace(80, 120, 100)))
    assert flat["ma20_slope5_atr60"] < trend["ma20_slope5_atr60"]
    assert flat["close_near_ma20_share15"] > trend["close_near_ma20_share15"]


def test_ma20_crossing_is_counted() -> None:
    result = ma20_features(history(100 + np.sin(np.arange(100) * np.pi / 2)))
    assert result["ma20_cross_count15"] > 0
