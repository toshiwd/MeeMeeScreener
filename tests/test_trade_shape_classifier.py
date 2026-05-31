from __future__ import annotations

from tools.debug.trade_shape_classifier import classify_shape_from_bars


def _bar(index: int, close: float, high: float | None = None, low: float | None = None) -> list[float]:
    high = close + 1 if high is None else high
    low = close - 1 if low is None else low
    return [20260101 + index, close, high, low, close, 1000]


def test_2411_like_range_lower_drift_is_not_failed_high_retest() -> None:
    bars = []
    for index in range(45):
        close = 470 + (index % 6)
        bars.append(_bar(index, close, high=close + 3, low=close - 3))
    bars.extend(
        [
            _bar(45, 490, high=497, low=488),
            _bar(46, 486, high=491, low=482),
            _bar(47, 477, high=480, low=474),
            _bar(48, 466, high=471, low=463),
            _bar(49, 451, high=454, low=449),
        ]
    )

    result = classify_shape_from_bars(bars)

    assert result["confirmed"] is True
    assert result["is_try_fail_7ma_break"] is False
    assert result["shape_intent"] in {"range_lower_drift", "late_breakdown_chase"}


def test_failed_high_retest_requires_upper_retest_and_7ma_break() -> None:
    bars = []
    for index in range(50):
        close = 88 + min(index, 25) * 0.25
        bars.append(_bar(index, close, high=close + 1.0, low=close - 1.0))
    bars.extend(
        [
            _bar(50, 100.0, high=102.0, low=98.5),
            _bar(51, 96.0, high=98.0, low=95.0),
            _bar(52, 97.0, high=99.2, low=96.5),
            _bar(53, 97.0, high=98.8, low=96.5),
            _bar(54, 96.0, high=97.0, low=95.5),
            _bar(55, 95.0, high=96.0, low=94.5),
        ]
    )

    result = classify_shape_from_bars(bars)

    assert result["confirmed"] is True
    assert result["is_try_fail_7ma_break"] is True
    assert result["shape_intent"] == "failed_high_retest_7ma_break"
    assert result["entry_timing"] == "initial_short_trigger_7ma_break"


def test_mature_uptrend_crash_setup_detects_marubeni_style_context() -> None:
    bars = []
    price = 500.0
    for index in range(120):
        price += 1.25
        bars.append(_bar(index, price, high=price + 4.0, low=price - 4.0))
    # First MA60 contact and rebound.
    bars.extend(
        [
            _bar(120, 640.0, high=648.0, low=636.0),
            _bar(121, 622.0, high=630.0, low=618.0),
            _bar(122, 642.0, high=646.0, low=634.0),
            _bar(123, 655.0, high=662.0, low=650.0),
            _bar(124, 668.0, high=672.0, low=660.0),
        ]
    )
    # Failed high retest, then roll under the 60MA.
    bars.extend(
        [
            _bar(125, 660.0, high=671.0, low=656.0),
            _bar(126, 646.0, high=660.0, low=640.0),
            _bar(127, 622.0, high=632.0, low=614.0),
            _bar(128, 592.0, high=604.0, low=588.0),
        ]
    )

    result = classify_shape_from_bars(bars)

    assert result["confirmed"] is True
    assert result["is_mature_uptrend_crash_setup"] is True
    assert result["shape_intent"] in {
        "mature_uptrend_failed_high_retest_distribution",
        "mature_uptrend_crash_setup_second_60ma_break",
    }


def test_crash_warning_detects_round_level_upper_wick_after_big_rise() -> None:
    bars = []
    price = 1000.0
    for index in range(120):
        price += 7.5
        bars.append(_bar(index, price, high=price + 12.0, low=price - 12.0))
    # Push into a round-number ceiling, reject intraday, and close near the level.
    bars.append([20260221, 1995.0, 2020.0, 1880.0, 1990.0, 1000])

    result = classify_shape_from_bars(bars)

    assert result["confirmed"] is True
    assert result["is_crash_warning_setup"] is True
    assert result["shape_intent"] == "crash_warning_round_level_upper_wick"
    assert result["entry_timing"] in {"watch_wait_for_ma_break", "probe_trigger_5ma_break", "probe_active_below_5ma_wait_7ma"}


def test_crash_warning_detects_round_level_failed_high_retest() -> None:
    bars = []
    price = 1000.0
    for index in range(118):
        price += 8.0
        bars.append(_bar(index, price, high=price + 10.0, low=price - 10.0))
    bars.extend(
        [
            [20260301, 1960.0, 1990.0, 1940.0, 1988.0, 1000],
            [20260302, 1980.0, 1995.0, 1945.0, 1950.0, 1000],
            [20260303, 1960.0, 1988.0, 1930.0, 1985.0, 1000],
            [20260304, 1988.0, 1990.0, 1940.0, 1978.0, 1000],
        ]
    )

    result = classify_shape_from_bars(bars)

    assert result["confirmed"] is True
    assert result["is_crash_warning_setup"] is True
    assert result["shape_intent"] == "crash_warning_round_level_failed_high_retest"


def test_crash_initial_detects_mountain_failed_high_retest_shape() -> None:
    bars = []
    price = 1000.0
    for index in range(80):
        price += 3.0
        bars.append(_bar(index, price, high=price + 8.0, low=price - 8.0))
    for offset, close in enumerate([1260, 1300, 1340, 1380, 1420, 1460, 1500, 1540]):
        bars.append(_bar(80 + offset, close, high=close + 10.0, low=close - 12.0))
    bars.extend(
        [
            [20260401, 1540.0, 1580.0, 1510.0, 1520.0, 1000],
            [20260402, 1490.0, 1510.0, 1450.0, 1460.0, 1000],
            [20260403, 1465.0, 1525.0, 1440.0, 1505.0, 1000],
            [20260404, 1500.0, 1530.0, 1450.0, 1468.0, 1000],
        ]
    )

    result = classify_shape_from_bars(bars)

    assert result["confirmed"] is True
    assert result["is_crash_warning_setup"] is True
    assert result["shape_intent"] == "crash_initial_mountain_failed_high_retest"
    assert result["entry_timing"] in {
        "initial_short_trigger_7ma_break",
        "initial_short_trigger_20ma_break",
        "short_trigger_active_below_7ma",
        "probe_trigger_5ma_break",
    }


def test_crash_bottom_warns_on_first_5ma_touch_after_selloff() -> None:
    bars = []
    price = 2000.0
    for index in range(30):
        price -= 25.0
        bars.append(_bar(index, price, high=price + 8.0, low=price - 12.0))
    bars.append([20260501, 1250.0, 1305.0, 1235.0, 1298.0, 1000])

    result = classify_shape_from_bars(bars)

    assert result["confirmed"] is True
    assert result["is_crash_bottoming_setup"] is True
    assert result["shape_intent"] == "crash_bottom_warning_first_5ma_touch"
    assert result["entry_timing"] == "bottom_probe_very_small_or_short_cover_watch"


def test_crash_bottom_confirms_after_multi_day_20ma_hold() -> None:
    bars = []
    price = 2000.0
    for index in range(35):
        price -= 20.0
        bars.append(_bar(index, price, high=price + 8.0, low=price - 12.0))
    for offset, close in enumerate([1320, 1350, 1380, 1410, 1440, 1470]):
        bars.append(_bar(35 + offset, close, high=close + 18.0, low=close - 10.0))

    result = classify_shape_from_bars(bars)

    assert result["confirmed"] is True
    assert result["is_crash_bottoming_setup"] is True
    assert result["shape_intent"] in {
        "crash_bottom_warning_first_20ma_break",
        "crash_bottom_confirmed_above_20ma",
    }


def test_crash_longterm_detects_300ma_retest_rejection() -> None:
    bars = []
    price = 1000.0
    for index in range(320):
        price += 1.0
        bars.append(_bar(index, price, high=price + 5.0, low=price - 5.0))
    for offset, close in enumerate([1310, 1270, 1220, 1160, 1130, 1150, 1165, 1160]):
        bars.append(_bar(320 + offset, close, high=close + 12.0, low=close - 12.0))

    result = classify_shape_from_bars(bars)

    assert result["confirmed"] is True
    assert result["is_crash_longterm_300ma_setup"] is True
    assert result["shape_intent"] in {
        "crash_longterm_first_300ma_break",
        "crash_longterm_300ma_retest_rejection",
    }


def test_crash_longterm_detects_300ma_false_reclaim_second_break() -> None:
    bars = []
    price = 1000.0
    for index in range(320):
        price += 1.0
        bars.append(_bar(index, price, high=price + 5.0, low=price - 5.0))
    for offset, close in enumerate([1300, 1240, 1190, 1160, 1185, 1210, 1170]):
        bars.append(_bar(320 + offset, close, high=close + 10.0, low=close - 10.0))

    result = classify_shape_from_bars(bars)

    assert result["confirmed"] is True
    assert result["is_crash_longterm_300ma_setup"] is True
    assert result["shape_intent"] == "crash_longterm_300ma_false_reclaim_second_break"


def test_b_phase_detects_sideways_breakout_up() -> None:
    bars = []
    price = 930.0
    for index in range(40):
        price += 0.2
        bars.append(_bar(index, price, high=price + 5.0, low=price - 5.0))
    for offset in range(20):
        close = 940.0 + (offset % 4) * 2.0
        bars.append(_bar(40 + offset, close, high=close + 4.0, low=close - 4.0))
    bars.append(_bar(61, 955.0, high=962.0, low=950.0))

    result = classify_shape_from_bars(bars)

    assert result["confirmed"] is True
    assert result["is_b_phase_setup"] is True
    assert result["shape_intent"] == "b_phase_breakout_up"
    assert result["entry_timing"] == "b_phase_buy_breakout"
    assert result["market_scene"] == "sideways_b_phase"
    assert result["trade_side"] == "long"
    assert result["action_bias"] == "buy_breakout"


def test_b_phase_detects_sideways_breakdown_down() -> None:
    bars = []
    price = 1000.0
    for index in range(40):
        price += 3.0
        bars.append(_bar(index, price, high=price + 5.0, low=price - 5.0))
    for offset in range(20):
        close = 1120.0 + (offset % 4) * 2.0
        bars.append(_bar(40 + offset, close, high=close + 4.0, low=close - 4.0))
    bars.append(_bar(61, 1105.0, high=1110.0, low=1098.0))

    result = classify_shape_from_bars(bars)

    assert result["confirmed"] is True
    assert result["shape_intent"] in {"b_phase_breakdown_down", "failed_high_retest_7ma_break"}


def test_c_phase_detects_ma5_ride_uptrend() -> None:
    bars = []
    price = 1000.0
    for index in range(80):
        price += 3.0
        bars.append(_bar(index, price, high=price + 5.0, low=price - 5.0))

    result = classify_shape_from_bars(bars)

    assert result["confirmed"] is True
    assert result["is_c_phase_setup"] is True
    assert result["shape_intent"] == "c_phase_uptrend_ma5_ride"
    assert result["market_scene"] == "uptrend_c_phase"
    assert result["trade_side"] == "long"
    assert result["action_bias"] == "hold_or_add_long"


def test_c_phase_detects_20ma_touch_bounce() -> None:
    bars = []
    price = 1000.0
    for index in range(70):
        price += 3.0
        bars.append(_bar(index, price, high=price + 5.0, low=price - 5.0))
    bars.append(_bar(71, 1185.0, high=1195.0, low=1172.0))

    result = classify_shape_from_bars(bars)

    assert result["confirmed"] is True
    assert "c_phase_touch_ma20_bounce" in result["reasons"]


def test_c_phase_detects_20ma_break_end() -> None:
    bars = []
    price = 1000.0
    for index in range(70):
        price += 3.0
        bars.append(_bar(index, price, high=price + 5.0, low=price - 5.0))
    bars.append(_bar(71, 1140.0, high=1160.0, low=1135.0))

    result = classify_shape_from_bars(bars)

    assert result["confirmed"] is True
    assert result["shape_intent"] in {"c_phase_uptrend_end_20ma_break", "failed_high_retest_7ma_break"}


def test_a_phase_detects_20ma_rejection() -> None:
    bars = []
    price = 1200.0
    for index in range(80):
        price -= 2.0
        bars.append(_bar(index, price, high=price + 5.0, low=price - 5.0))
    bars.append(_bar(81, 1050.0, high=1070.0, low=1040.0))

    result = classify_shape_from_bars(bars)

    assert result["confirmed"] is True
    assert "a_phase_20ma_rejection" in result["reasons"]
    if result["shape_intent"].startswith("a_phase_"):
        assert result["market_scene"] == "downtrend_a_phase"
        assert result["trade_side"] == "short"


def test_a_phase_detects_recovery_20ma_hold() -> None:
    bars = []
    price = 1200.0
    for index in range(80):
        price -= 2.0
        bars.append(_bar(index, price, high=price + 5.0, low=price - 5.0))
    for offset, close in enumerate([1050, 1060, 1070, 1080, 1090]):
        bars.append(_bar(81 + offset, close, high=close + 8.0, low=close - 5.0))

    result = classify_shape_from_bars(bars)

    assert result["confirmed"] is True
    assert "a_phase_downtrend_context" in result["reasons"]


def test_a_phase_detects_lower_low_break_short_add() -> None:
    bars = []
    price = 1200.0
    for index in range(80):
        price -= 2.0
        bars.append(_bar(index, price, high=price + 5.0, low=price - 5.0))
    for offset, close in enumerate([1040, 1035, 1038, 1032, 1026, 1005]):
        bars.append(_bar(81 + offset, close, high=close + 3.0, low=close - 4.0))

    result = classify_shape_from_bars(bars)

    assert result["confirmed"] is True
    assert result["shape_intent"] in {
        "a_phase_downtrend_lower_low_break",
        "a_phase_downtrend_ma5_inside_continuation",
    }
    assert "a_phase_lower_low_break" in result["reasons"]


def test_a_phase_detects_bottom_range_breakdown_turn_short() -> None:
    bars = []
    price = 1400.0
    for index in range(55):
        price -= 8.0
        bars.append(_bar(index, price, high=price + 4.0, low=price - 4.0))
    for index in range(55, 74):
        close = 960 + (index % 5) * 2
        bars.append(_bar(index, close, high=close + 5.0, low=close - 5.0))
    bars.append(_bar(75, 940.0, high=948.0, low=936.0))

    result = classify_shape_from_bars(bars)

    assert result["confirmed"] is True
    assert result["shape_intent"] in {
        "a_phase_bottom_range_breakdown_turn_short",
        "b_phase_breakdown_down",
        "a_phase_downtrend_lower_low_break",
    }
    assert "a_phase_bottom_range_breakdown" in result["reasons"]


def test_crash_warning_scene_profile_is_short_or_watch() -> None:
    bars = []
    price = 1000.0
    for index in range(120):
        price += 7.5
        bars.append(_bar(index, price, high=price + 12.0, low=price - 12.0))
    bars.append([20260221, 1995.0, 2020.0, 1880.0, 1990.0, 1000])

    result = classify_shape_from_bars(bars)

    assert result["confirmed"] is True
    assert result["market_scene"] in {"crash_or_distribution_phase", "distribution_or_failed_retest"}
    assert result["action_bias"] in {
        "wait_for_short_trigger",
        "sell_distribution_or_ma_break",
        "sell_failed_high_retest_after_7ma_break",
    }


def test_crash_bottoming_scene_profile_is_probe_not_full_buy() -> None:
    bars = []
    price = 1300.0
    for index in range(70):
        price -= 4.0
        bars.append(_bar(index, price, high=price + 6.0, low=price - 6.0))
    bars.append(_bar(71, 1030.0, high=1040.0, low=1010.0))

    result = classify_shape_from_bars(bars)

    assert result["confirmed"] is True
    assert result["market_scene"] == "crash_bottoming_phase"
    assert result["trade_side"] == "long_probe_or_short_cover"
    assert result["action_bias"] == "probe_bottom_only_after_ma_reclaim"
