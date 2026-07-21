from __future__ import annotations

import numpy as np
import pandas as pd

from scripts.tradex_sideways_compression_band_v1 import breakout_labels, infer_band


def frame(close: np.ndarray) -> pd.DataFrame:
    close = np.asarray(close, dtype=float)
    return pd.DataFrame({
        "ymd": np.arange(20200101, 20200101 + len(close)),
        "o": np.r_[close[0], close[:-1]], "h": close + 0.5,
        "l": close - 0.5, "c": close, "v": 1000.0,
    })


def test_infer_band_uses_only_history_and_finds_recent_compression() -> None:
    history = frame(np.r_[np.linspace(80, 100, 80), 100 + np.sin(np.arange(60)) * 0.3])
    band = infer_band(history)
    assert band["band_window"] in (10, 15, 20, 30, 40, 50, 60)
    assert band["band_lower"] < band["band_upper"]
    assert band["band_upper"] - band["band_lower"] < 2.0


def test_breakout_labels_separate_wick_close_and_two_close_settle() -> None:
    future = pd.DataFrame({
        "o": [99, 100, 101, 102], "h": [101, 103, 104, 105],
        "l": [98, 99, 100, 101], "c": [100, 101, 103, 104], "v": 1000,
    })
    labels = breakout_labels(future, lower=98, upper=102)
    assert labels["wick_break_direction"] == "UP"
    assert labels["wick_break_day"] == 2
    assert labels["close_break_direction"] == "UP"
    assert labels["close_break_day"] == 3
    assert labels["settled_break_direction"] == "UP"
    assert labels["settled_break_day"] == 4
