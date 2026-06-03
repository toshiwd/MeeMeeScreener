from __future__ import annotations

from scripts import tradex_ma_role_transition_research_phase15 as mod


def test_state_uses_fixed_ma_roles() -> None:
    state = mod._state(
        {
            "close": 120.0,
            "open": 116.0,
            "high": 122.0,
            "low": 114.0,
            "ma7": 115.0,
            "ma20": 110.0,
            "ma60": 100.0,
            "ma100": 95.0,
            "ma200": 90.0,
            "open_prev2": 110.0,
            "high_prev2": 113.0,
            "low_prev2": 109.0,
            "close_prev2": 112.0,
            "open_prev1": 112.0,
            "high_prev1": 116.0,
            "low_prev1": 111.0,
            "close_prev1": 115.0,
            "ma7_prev5": 110.0,
            "ma20_prev5": 108.0,
            "ma60_prev5": 99.0,
            "ma100_prev20": 94.0,
            "ma200_prev20": 89.0,
        }
    )
    assert state["entry_exit"] == "candle_shape:normal_bull|three_candle:three_white_soldiers|close_ma7:above|close_ma20:above|candle_ma7:mostly_above|candle_ma20:all_above|ma7_ma20:above|ma7_slope:up|ma20_slope:up"
    assert state["trend"] == "close_ma60:above|candle_ma60:all_above|ma20_ma60:above|ma60_slope:up"
    assert state["environment"] == "alignment:bull_alignment|candle_ma100:all_above|candle_ma200:all_above|ma100_slope:up|ma200_slope:up"


def test_environment_detects_bear_alignment() -> None:
    assert mod._environment(80.0, 90.0, 100.0) == "bear_alignment"


def test_candle_vs_ma_detects_half_or_more_above() -> None:
    assert mod._candle_vs_ma(110.0, 100.0, 105.0) == "mostly_above"
    assert mod._candle_vs_ma(110.0, 100.0, 106.0) == "mostly_below"
    assert mod._candle_vs_ma(110.0, 105.0, 104.0) == "all_above"
    assert mod._candle_vs_ma(104.0, 100.0, 105.0) == "all_below"


def test_candle_shape_detects_hammer_and_spinning_top() -> None:
    assert mod._candle_shape(108.0, 110.0, 90.0, 109.0) == "hammer_bull"
    assert mod._candle_shape(101.0, 110.0, 100.0, 102.0) == "inverted_hammer_bull"
    assert mod._candle_shape(99.0, 110.0, 90.0, 101.0) == "spinning_top"
    assert mod._candle_shape(100.0, 111.0, 99.0, 110.0) == "wide_body_bull"


def test_three_candle_pattern_detects_context_patterns() -> None:
    assert mod._three_candle_pattern(10, 12, 9, 11, 11, 13, 10, 12, 12, 14, 11, 13) == "three_white_soldiers"
    assert mod._three_candle_pattern(13, 14, 12, 12, 12, 13, 11, 11, 11, 12, 10, 10) == "three_black_crows"
    assert mod._three_candle_pattern(10, 11, 9, 10, 12, 13, 11, 11, 10, 13, 9, 13) == "bullish_engulfing"
    assert mod._three_candle_pattern(10, 11, 9, 10, 10, 13, 9, 12, 13, 14, 8, 9) == "bearish_engulfing"


def test_stable_candidates_require_all_splits() -> None:
    def row(split: str, value: float) -> dict[str, object]:
        return {
            "split": split,
            "entry_exit": "entry",
            "trend": "trend",
            "environment": "environment",
            "count": 120,
            "ret20_mean": value,
            "positive_ret20_rate": 0.6,
            "bad_ret20_lt_minus_5pct_rate": 0.1,
        }

    candidates = mod._stable_candidates([row("train", 0.01), row("validation", 0.02), row("test", 0.03)])
    assert candidates[0]["minimum_split_count"] == 120
    assert candidates[0]["minimum_ret20_mean"] == 0.01
    assert candidates[0]["stable_positive_ret20"] is True
