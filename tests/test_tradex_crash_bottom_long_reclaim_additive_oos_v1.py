from __future__ import annotations

from scripts.tradex_crash_bottom_long_reclaim_additive_oos_v1 import (
    BASE_SIGNAL_KEY,
    REQUIRED_SHAPE_INTENT,
    _cheap_crash_bottom_reclaim_prefilter,
    _select_one_per_date,
)


def test_crash_bottom_axis_targets_confirmed_20ma_reclaim_only() -> None:
    assert REQUIRED_SHAPE_INTENT == "crash_bottom_confirmed_above_20ma"
    assert BASE_SIGNAL_KEY == "crash_bottoming_phase|probe_bottom_only_after_ma_reclaim|long_probe_or_short_cover|keep_probe_candidate"


def test_select_one_per_date_prefers_deeper_drop_then_slope() -> None:
    rows = [
        {"dt": 20250110, "code": "2000", "recent_downtrend_drop_pct_abs": 0.09, "ma20_slope_10": 0.2},
        {"dt": 20250110, "code": "1000", "recent_downtrend_drop_pct_abs": 0.12, "ma20_slope_10": 0.01},
        {"dt": 20250111, "code": "3000", "recent_downtrend_drop_pct_abs": 0.12, "ma20_slope_10": 0.01},
        {"dt": 20250111, "code": "4000", "recent_downtrend_drop_pct_abs": 0.12, "ma20_slope_10": 0.03},
    ]

    selected = _select_one_per_date(rows)

    assert selected[20250110]["code"] == "1000"
    assert selected[20250111]["code"] == "4000"


def test_cheap_prefilter_requires_drop_and_multi_day_20ma_reclaim() -> None:
    rows = [{"c": 100.0, "h": 130.0, "l": 99.0}]
    rows.extend({"c": 108.0, "h": 109.0, "l": 107.0} for _ in range(57))
    rows.append({"c": 112.0, "h": 113.0, "l": 111.0})
    rows.append({"c": 113.0, "h": 114.0, "l": 112.0})

    assert _cheap_crash_bottom_reclaim_prefilter(rows) is True

    no_drop = [dict(row) for row in rows]
    no_drop[0] = {"c": 100.0, "h": 115.0, "l": 99.0}
    assert _cheap_crash_bottom_reclaim_prefilter(no_drop) is False
