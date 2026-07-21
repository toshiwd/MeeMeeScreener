from datetime import date, timedelta

from scripts.tradex_watchlist_chart_state_board_v1 import classify_chart_state


def _rows(closes):
    start = date(2025, 1, 1)
    return [{"ymd": int((start + timedelta(days=i)).strftime("%Y%m%d")), "open": close - 1, "high": close + 2, "low": close - 2, "close": close, "volume": 1000} for i, close in enumerate(closes)]


def test_multitimeframe_high_zone_is_breakout_watch():
    result = classify_chart_state(_rows([100 + i * .5 for i in range(100)]))
    assert result["state"] == "breakout_watch"
    assert result["image_review_required"] is True
    assert result["context"]["daily_up"] is True


def test_short_history_is_undetermined():
    result = classify_chart_state(_rows([100.] * 20))
    assert result["state"] == "undetermined"
    assert result["confidence"] == "low"
