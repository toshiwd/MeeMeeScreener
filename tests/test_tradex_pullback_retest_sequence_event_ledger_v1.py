from __future__ import annotations

import pandas as pd

from scripts.tradex_pullback_retest_sequence_event_ledger_v1 import build_events


def test_build_events_materializes_ordered_confirmation_without_future_columns() -> None:
    rows = pd.DataFrame([
        {"code": "1001", "as_of_date": 20250101, "close_vs_ma20_pct": -0.02, "bullish_body_flag": False, "bearish_body_flag": True, "lower_wick_ratio": 0.0, "recent_low_distance_pct": 0.0, "weekly_supportive_flag": True, "monthly_supportive_flag": True, "volume_vs_20d_avg": 1.0},
        {"code": "1001", "as_of_date": 20250102, "close_vs_ma20_pct": 0.01, "bullish_body_flag": False, "bearish_body_flag": False, "lower_wick_ratio": 0.0, "recent_low_distance_pct": 0.01, "weekly_supportive_flag": True, "monthly_supportive_flag": True, "volume_vs_20d_avg": 1.0},
        {"code": "1001", "as_of_date": 20250103, "close_vs_ma20_pct": 0.02, "bullish_body_flag": True, "bearish_body_flag": False, "lower_wick_ratio": 0.3, "recent_low_distance_pct": 0.02, "weekly_supportive_flag": True, "monthly_supportive_flag": True, "volume_vs_20d_avg": 1.0},
    ])

    events = build_events(rows)

    assert len(events) == 1
    assert events.iloc[0]["pullback_start_as_of"] == 20250101
    assert events.iloc[0]["reclaim_as_of"] == 20250102
    assert events.iloc[0]["retest_as_of"] == 20250103
    assert events.iloc[0]["confirmation_as_of"] == 20250103
    assert events.iloc[0]["invalidation_price_as_of_confirmation"] is None
