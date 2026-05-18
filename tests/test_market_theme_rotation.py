from app.backend.api.routers.market import (
    _build_visual_frame,
    _render_svg_candles,
    _candidate_candlestick_state,
    _detail_visual_decision,
    _score_theme_candidate,
    _theme_for_row,
    _theme_status,
)


def test_theme_for_row_uses_assigned_theme_before_sector_fallback():
    assert _theme_for_row("9041", "近鉄G HD", "5050", "陸運業") == (
        "inbound_transport",
        "インバウンド/交通",
    )
    assert _theme_for_row("8035", "東京エレクトロン", "3650", "電気機器") == (
        "semiconductor",
        "半導体",
    )


def test_theme_for_row_falls_back_to_sector_name_when_no_theme_matches():
    assert _theme_for_row("0000", "無名銘柄", "9999", "その他") == ("sector:9999", "その他")


def test_theme_status_marks_new_acceleration_and_fade():
    assert _theme_status(0.8, 0.1, None) == "NEW"
    assert _theme_status(0.7, 0.2, 0.3) == "ACCEL"
    assert _theme_status(0.1, 0.8, 0.7) == "FADE"


def test_candidate_candlestick_state_flags_weak_bearish_close():
    state = _candidate_candlestick_state({"open": 100, "high": 105, "low": 90, "close": 92})

    assert "bearish_large_body" in state["states"]
    assert "weak_close" in state["states"]


def test_score_theme_candidate_is_deterministic_and_penalizes_rejection():
    row = {
        "open": 100,
        "high": 120,
        "low": 98,
        "close": 101,
        "prevClose": 99,
        "volume": 3000,
        "avgVolume20": 1000,
        "ma20": 95,
        "ma60": 90,
        "high60": 101,
        "low60": 70,
    }
    stats = {"count": 2, "advancerRatio": 1.0, "avgChangePct": 3.0, "avgVolumeRatio": 2.0}

    scored = _score_theme_candidate(row, stats, "entry_ease")

    assert scored["score"] == _score_theme_candidate(row, stats, "entry_ease")["score"]
    assert "volume_expansion" in scored["reasonCodes"]
    assert "breakout_60d" in scored["reasonCodes"]
    assert "upper_wick_rejection" in scored["penaltyCodes"]
    assert "thin_theme" in scored["penaltyCodes"]
    assert scored["visualCheck"]["required"] is True
    assert scored["visualCheck"]["detailRouteRequired"] is True
    assert "upper_wick" in scored["visualCheck"]["warnings"]


def test_build_visual_frame_returns_screenshot_free_chart_summary():
    rows = []
    for index in range(1, 70):
        close = 100 + index
        rows.append((20250100 + index, close - 2, close + 1, close - 4, close, 1000 + index))

    frame = _build_visual_frame(rows, "daily")

    assert frame["status"] == "ok"
    assert frame["visualCheck"]["detailRouteRequired"] is True
    assert frame["movingAverages"]["ma20"] is not None
    assert frame["levels"]["high60"] is not None


def test_detail_visual_decision_does_not_require_screenshot_for_entry_candidate():
    frame = {
        "status": "ok",
        "trend": "uptrend",
        "visualCheck": {"setup": "constructive"},
    }

    decision = _detail_visual_decision({"daily": frame, "weekly": frame, "monthly": frame})

    assert decision["decision"] == "entry_candidate"
    assert decision["requiresScreenshot"] is False


def test_render_svg_candles_contains_three_timeframes():
    rows = []
    for index in range(1, 70):
        close = 100 + index
        rows.append((20250100 + index, close - 2, close + 1, close - 4, close, 1000 + index))

    svg = _render_svg_candles(
        code="0000",
        title="Sample",
        row_sets={"daily": rows, "weekly": rows, "monthly": rows},
        max_bars=40,
    )

    assert svg.startswith("<svg")
    assert "Daily" in svg
    assert "Weekly" in svg
    assert "Monthly" in svg
