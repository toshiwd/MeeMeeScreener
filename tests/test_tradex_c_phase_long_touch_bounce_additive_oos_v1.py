from __future__ import annotations

from scripts.tradex_c_phase_long_touch_bounce_additive_oos_v1 import (
    BASE_SIGNAL_KEY,
    REQUIRED_SHAPE_INTENT,
    _cheap_c_phase_touch_bounce_prefilter,
    _select_one_per_date,
)


def test_c_phase_axis_targets_long_20ma_touch_bounce_only() -> None:
    assert REQUIRED_SHAPE_INTENT == "c_phase_uptrend_20ma_touch_bounce"
    assert BASE_SIGNAL_KEY == "uptrend_c_phase|hold_or_add_long|long|pullback_probe_candidate"


def test_select_one_per_date_prefers_pullback_and_closer_ma20() -> None:
    rows = [
        {
            "dt": 20250110,
            "code": "2000",
            "visual_decision": "keep_probe_candidate",
            "ma20_distance": 0.001,
            "ma20_slope_10": 0.1,
        },
        {
            "dt": 20250110,
            "code": "1000",
            "visual_decision": "pullback_probe_candidate",
            "ma20_distance": 0.02,
            "ma20_slope_10": 0.01,
        },
        {
            "dt": 20250111,
            "code": "3000",
            "visual_decision": "pullback_probe_candidate",
            "ma20_distance": 0.03,
            "ma20_slope_10": 0.01,
        },
        {
            "dt": 20250111,
            "code": "4000",
            "visual_decision": "pullback_probe_candidate",
            "ma20_distance": -0.01,
            "ma20_slope_10": 0.01,
        },
    ]

    selected = _select_one_per_date(rows)

    assert selected[20250110]["code"] == "1000"
    assert selected[20250111]["code"] == "4000"


def test_cheap_prefilter_accepts_only_c_phase_touch_window() -> None:
    closes = [100.0 + index * 0.1 for index in range(159)]
    ma20 = sum(closes[-19:] + [116.0]) / 20
    rows = [{"c": close, "l": close - 0.2, "h": close + 0.2} for close in closes]
    rows.append({"c": 116.0, "l": ma20 - 0.01, "h": ma20 + 0.5})

    assert _cheap_c_phase_touch_bounce_prefilter(rows) is True

    broken = [dict(row) for row in rows]
    broken[-1] = {"c": 112.0, "l": 111.5, "h": 112.5}
    assert _cheap_c_phase_touch_bounce_prefilter(broken) is False
