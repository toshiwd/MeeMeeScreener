from __future__ import annotations

import numpy as np
import pandas as pd

from scripts.tradex_sideways_ma20_breakout_timing_v1 import snapshot_features


def history(close: np.ndarray) -> pd.DataFrame:
    close = np.asarray(close, dtype=float)
    return pd.DataFrame({
        "ymd": np.arange(len(close)), "o": np.r_[close[0], close[:-1]],
        "h": close + 0.5, "l": close - 0.5, "c": close, "v": 1000.0,
    })


def test_snapshot_tracks_signed_ma20_departure() -> None:
    rising = snapshot_features(history(np.r_[np.full(80, 100.0), np.linspace(100, 106, 20)]), 95, 110)
    falling = snapshot_features(history(np.r_[np.full(80, 100.0), np.linspace(100, 94, 20)]), 90, 105)
    assert rising["close_ma20_signed_atr14"] > 0
    assert falling["close_ma20_signed_atr14"] < 0
    assert rising["ma20_signed_slope5_atr60"] > 0
    assert falling["ma20_signed_slope5_atr60"] < 0


def test_snapshot_tracks_band_position_and_edge_distance() -> None:
    result = snapshot_features(history(np.full(100, 104.0)), 100, 110)
    assert result["band_position"] == 0.4
    assert result["distance_to_nearest_edge_atr14"] > 0
