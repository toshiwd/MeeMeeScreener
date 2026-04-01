from __future__ import annotations

import pandas as pd

from scripts.ma20_cross_timing_study import (
    _add_tf_bar_features,
    _bucket_gap_days,
    _build_report,
)


def _make_frame(rows: list[dict[str, object]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    frame["dt"] = pd.to_datetime(frame["dt"])
    return frame


def test_add_tf_bar_features_detects_cross_up_and_down() -> None:
    rows = []
    for idx in range(21):
        close = 100.0
        if idx == 18:
            close = 99.0
        elif idx == 19:
            close = 101.0
        elif idx == 20:
            close = 97.0
        rows.append(
            {
                "code": "1001",
                "name": "sample",
                "dt": f"2025-01-{idx + 1:02d}",
                "o": close - 0.5,
                "h": close + 1.0,
                "l": close - 1.0,
                "c": close,
                "v": 1_000,
            }
        )

    frame = _add_tf_bar_features(_make_frame(rows), timeframe="D")

    assert bool(frame.iloc[19]["cross_up"]) is True
    assert bool(frame.iloc[20]["cross_down"]) is True
    assert frame.iloc[19]["cross_signal"] == "cross_up"
    assert frame.iloc[20]["cross_signal"] == "cross_down"


def test_add_tf_bar_features_classifies_gap_candles() -> None:
    rows = []
    for idx in range(12):
        if idx == 9:
            rows.append(
                {
                    "code": "1001",
                    "name": "sample",
                    "dt": f"2025-01-{idx + 1:02d}",
                    "o": 103.0,
                    "h": 105.0,
                    "l": 102.0,
                    "c": 104.0,
                    "v": 1_000,
                }
            )
            continue
        if idx == 10:
            rows.append(
                {
                    "code": "1001",
                    "name": "sample",
                    "dt": f"2025-01-{idx + 1:02d}",
                    "o": 99.0,
                    "h": 100.0,
                    "l": 95.0,
                    "c": 96.0,
                    "v": 1_000,
                }
            )
            continue
        rows.append(
            {
                "code": "1001",
                "name": "sample",
                "dt": f"2025-01-{idx + 1:02d}",
                "o": 100.0,
                "h": 101.0,
                "l": 99.0,
                "c": 100.0,
                "v": 1_000,
            }
        )

    frame = _add_tf_bar_features(_make_frame(rows), timeframe="D")

    assert frame.iloc[9]["candle_class"] == "gap_up_bull"
    assert frame.iloc[10]["candle_class"] == "gap_down_bear"
    assert frame.iloc[11]["candle_class"] == "doji"


def test_bucket_gap_days_and_report_sections() -> None:
    assert _bucket_gap_days(0) == "same_day"
    assert _bucket_gap_days(2) == "lead_1_3"
    assert _bucket_gap_days(-2) == "lag_1_3"
    assert _bucket_gap_days(11) == "lead_8_20"

    result = {
        "meta": {
            "db_paths": ["data/stocks.duckdb"],
            "date_min": "2020-01-01",
            "date_max": "2026-03-31",
            "direction_mode": "both",
            "events_total": 2,
            "round_trip_cost": 0.002,
            "direction_labels": ["買い", "売り"],
            "timeframes": ["日足", "週足", "月足"],
        },
        "direction_summary": [
            {
                "direction_label": "買い",
                "timeframe_label": "日足",
                "n": 1,
                "mean_5d": 0.01,
                "mean_10d": 0.02,
                "mean_20d": 0.03,
                "win_rate_10d": 1.0,
                "win_rate_20d": 1.0,
                "profit_factor_10d": 2.0,
                "profit_factor_20d": 3.0,
                "mfe20d": 0.04,
                "mae20d": -0.01,
            }
        ],
        "candle_summary": [
            {
                "direction_label": "買い",
                "signal_timeframe_label": "日足",
                "candle_class": "gap_up_bull",
                "n": 1,
                "mean_10d": 0.02,
                "mean_20d": 0.03,
                "win_rate_10d": 1.0,
            }
        ],
        "daily_candle_summary": [
            {
                "direction_label": "買い",
                "signal_timeframe_label": "日足",
                "daily_candle_class": "impulse_bull",
                "n": 1,
                "mean_10d": 0.02,
                "mean_20d": 0.03,
                "win_rate_10d": 1.0,
            }
        ],
        "bar_tag_summary": [
            {
                "direction_label": "買い",
                "signal_timeframe_label": "日足",
                "bar_tag": "UL-N-GU-HB",
                "n": 1,
                "mean_10d": 0.02,
                "mean_20d": 0.03,
                "win_rate_10d": 1.0,
            }
        ],
        "daily_bar_tag_summary": [
            {
                "direction_label": "買い",
                "signal_timeframe_label": "日足",
                "daily_bar_tag": "UL-N-GU-HB",
                "n": 1,
                "mean_10d": 0.02,
                "mean_20d": 0.03,
                "win_rate_10d": 1.0,
            }
        ],
        "pattern_summary": {
            "all": {
                "pattern_2": [
                    {
                        "pattern_2": "UL-N-GU-HB>XS-N-GU-HB",
                        "n": 1,
                        "mean_10d": 0.02,
                        "mean_20d": 0.03,
                        "win_rate_10d": 1.0,
                    }
                ],
                "daily_pattern_2": [
                    {
                        "daily_pattern_2": "UL-N-GU-HB>XS-N-GU-HB",
                        "n": 1,
                        "mean_10d": 0.02,
                        "mean_20d": 0.03,
                        "win_rate_10d": 1.0,
                    }
                ],
            }
        },
        "monthly_context_summary": [],
        "weekly_context_summary": [],
        "box_zone_summary": [],
        "timing_summary": [],
        "alignment_summary": [],
        "dist_summary": [],
        "replay_examples": {
            "up": {
                "6301": [
                    {
                        "date": "2025-01-31",
                        "timeframe_label": "月足",
                        "candle_class": "impulse_bull",
                        "daily_candle_class": "impulse_bull",
                        "bar_tag": "UL-N-GU-HB",
                        "daily_bar_tag": "UL-N-GU-HB",
                        "pattern_2": "UL-N-GU-HB>XS-N-GU-HB",
                        "daily_pattern_2": "UL-N-GU-HB>XS-N-GU-HB",
                        "monthly_context": "box_upper_pressure",
                        "weekly_context": "up_support_intact",
                        "box_zone": "upper",
                        "daily_alignment_bucket": "same_day",
                        "ret10d": 0.02,
                        "ret20d": 0.03,
                    }
                ]
            }
        },
    }

    report = _build_report(result)

    assert "# 20か月線割れタイミング研究" in report
    assert "## Direction Summary" in report
    assert "## Daily Candle Summary" in report
    assert "## Replay Examples" in report
    assert "6301" in report
