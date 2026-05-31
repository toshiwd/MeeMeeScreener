from __future__ import annotations

from scripts.tradex_hanarekojima_bottom_reversal_long_oos_v1 import PATTERN_LABEL, _hanarekojima_features, _select_one_per_date


def _row(open_: float, high: float, low: float, close: float) -> dict[str, float]:
    return {"o": open_, "h": high, "l": low, "c": close}


def test_hanarekojima_features_match_text_contract() -> None:
    rows = [_row(110 - index, 111 - index, 109 - index, 110 - index) for index in range(24)]
    rows.append(_row(86, 87, 84, 85))
    rows.append(_row(83, 84, 80, 82))
    rows.append(_row(79, 82, 77, 81))

    features = _hanarekojima_features(rows)

    assert PATTERN_LABEL == "hanarekojima_bottom_reversal"
    assert features["matched"] is True
    assert features["ma_down"] is True
    assert features["day1_gap_down"] is True
    assert features["day2_gap_down"] is True
    assert features["day2_lower_open"] is True
    assert features["day2_lower_close"] is True
    assert features["wick_balanced"] is True


def test_hanarekojima_rejects_extreme_wicks() -> None:
    rows = [_row(110 - index, 111 - index, 109 - index, 110 - index) for index in range(24)]
    rows.append(_row(86, 87, 84, 85))
    rows.append(_row(83, 84, 80, 82))
    rows.append(_row(79, 95, 77, 81))

    features = _hanarekojima_features(rows)

    assert features["wick_balanced"] is False
    assert features["matched"] is False


def test_select_one_per_date_prefers_stronger_second_day_body_and_balanced_wicks() -> None:
    rows = [
        {"dt": 20250110, "code": "2000", "day2_body_return": 0.02, "upper_wick_ratio": 0.08, "lower_wick_ratio": 0.08},
        {"dt": 20250110, "code": "1000", "day2_body_return": 0.04, "upper_wick_ratio": 0.2, "lower_wick_ratio": 0.08},
        {"dt": 20250111, "code": "4000", "day2_body_return": 0.02, "upper_wick_ratio": 0.2, "lower_wick_ratio": 0.05},
        {"dt": 20250111, "code": "3000", "day2_body_return": 0.02, "upper_wick_ratio": 0.1, "lower_wick_ratio": 0.09},
    ]

    selected = _select_one_per_date(rows)

    assert selected[20250110]["code"] == "1000"
    assert selected[20250111]["code"] == "3000"
