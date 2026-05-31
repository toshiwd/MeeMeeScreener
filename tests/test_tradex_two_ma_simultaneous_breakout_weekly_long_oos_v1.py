from __future__ import annotations

from scripts.tradex_two_ma_simultaneous_breakout_weekly_long_oos_v1 import _aggregate_week, _weekly_bars


def test_weekly_bars_aggregate_by_iso_week() -> None:
    rows = [
        {"ymd": 20250106, "o": 10, "h": 12, "l": 9, "c": 11, "v": 100},
        {"ymd": 20250107, "o": 11, "h": 13, "l": 10, "c": 12, "v": 200},
        {"ymd": 20250113, "o": 20, "h": 21, "l": 19, "c": 20, "v": 300},
    ]

    weeks = _weekly_bars(rows)

    assert weeks == [
        {"ymd": 20250107, "o": 10.0, "h": 13.0, "l": 9.0, "c": 12.0, "v": 300.0, "daily_bar_count": 2},
        {"ymd": 20250113, "o": 20.0, "h": 21.0, "l": 19.0, "c": 20.0, "v": 300.0, "daily_bar_count": 1},
    ]


def test_aggregate_week_uses_first_open_last_close() -> None:
    row = _aggregate_week(
        [
            {"ymd": 20250106, "o": 10, "h": 11, "l": 9, "c": 10.5, "v": 1},
            {"ymd": 20250110, "o": 10.5, "h": 12, "l": 8, "c": 11.5, "v": 2},
        ]
    )

    assert row["ymd"] == 20250110
    assert row["o"] == 10.0
    assert row["h"] == 12.0
    assert row["l"] == 8.0
    assert row["c"] == 11.5
    assert row["v"] == 3.0
