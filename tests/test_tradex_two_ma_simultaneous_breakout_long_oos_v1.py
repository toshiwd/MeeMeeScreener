from __future__ import annotations

from scripts.tradex_two_ma_simultaneous_breakout_long_oos_v1 import PATTERN_LABEL, _select_one_per_date, _two_ma_features


def _row(close: float, *, open_: float | None = None, high: float | None = None, low: float | None = None) -> dict[str, float]:
    open_value = close if open_ is None else open_
    high_value = max(open_value, close) + 0.5 if high is None else high
    low_value = min(open_value, close) - 0.5 if low is None else low
    return {"o": open_value, "h": high_value, "l": low_value, "c": close}


def test_two_ma_features_match_text_contract() -> None:
    window = [_row(100.0) for _ in range(15)]
    window.extend(_row(96.0) for _ in range(5))
    window.extend(_row(96.0) for _ in range(4))
    window.append(_row(94.0))
    window.append(_row(106.0, open_=95.0, high=107.0, low=94.5))

    features = _two_ma_features(window)

    assert PATTERN_LABEL == "two_ma_simultaneous_breakout"
    assert features["matched"] is True
    assert features["prior_low_state"] is True
    assert features["opens_below_5ma"] is True
    assert features["closes_above_20ma"] is True
    assert features["wick_ok"] is True


def test_two_ma_features_reject_long_wicks() -> None:
    window = [_row(100.0) for _ in range(15)]
    window.extend(_row(96.0) for _ in range(5))
    window.extend(_row(96.0) for _ in range(4))
    window.append(_row(94.0))
    window.append(_row(106.0, open_=95.0, high=125.0, low=94.5))

    features = _two_ma_features(window)

    assert features["wick_ok"] is False
    assert features["matched"] is False


def test_select_one_per_date_prefers_stronger_body_then_flatter_ma() -> None:
    rows = [
        {"dt": 20250110, "code": "2000", "body_return": 0.05, "ma20_slope_5": 0.001, "ma5_slope_5": 0.001, "upper_wick_ratio": 0.1, "lower_wick_ratio": 0.1},
        {"dt": 20250110, "code": "1000", "body_return": 0.08, "ma20_slope_5": 0.02, "ma5_slope_5": 0.02, "upper_wick_ratio": 0.2, "lower_wick_ratio": 0.1},
        {"dt": 20250111, "code": "4000", "body_return": 0.05, "ma20_slope_5": 0.002, "ma5_slope_5": 0.001, "upper_wick_ratio": 0.1, "lower_wick_ratio": 0.1},
        {"dt": 20250111, "code": "3000", "body_return": 0.05, "ma20_slope_5": 0.001, "ma5_slope_5": 0.001, "upper_wick_ratio": 0.1, "lower_wick_ratio": 0.1},
    ]

    selected = _select_one_per_date(rows)

    assert selected[20250110]["code"] == "1000"
    assert selected[20250111]["code"] == "3000"
