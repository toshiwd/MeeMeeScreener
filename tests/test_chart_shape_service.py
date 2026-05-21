from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.backend.api.dependencies import get_stock_repo
from app.backend.api.routers import ticker
from app.backend.services.chart_shape_service import (
    classify_daily_chart_shape,
    classify_daily_chart_shapes_by_window,
    get_chart_shape_pattern_catalog,
)


def test_classify_daily_chart_shape_detects_gap_up_stall_fade() -> None:
    rows = [
        (20260508, 100.0, 102.0, 99.0, 101.0, 1000.0),
        (20260509, 101.0, 103.0, 100.0, 102.0, 1000.0),
        (20260510, 120.0, 125.0, 119.0, 124.0, 5000.0),
        (20260511, 125.0, 126.0, 116.0, 117.0, 4000.0),
        (20260512, 117.0, 119.0, 115.0, 116.0, 3000.0),
        (20260513, 116.0, 118.0, 114.0, 115.0, 2500.0),
    ]

    result = classify_daily_chart_shape(rows, requested_window=5)

    assert result["confirmed"] is True
    assert result["shape_label"] == "gap_up_stall_fade"
    assert result["shape_family"] == "gap_failure"
    assert result["bias"] == "caution"
    assert result["actionability"] == "avoid_chase"
    assert "large_gap_up" in result["reasons"]
    assert "failed_to_extend_after_gap" in result["reasons"]
    assert result["metrics"]["gap_date"] == 20260510
    assert result["metrics"]["post_gap_return_pct"] < 0


def test_classify_daily_chart_shape_detects_sideways_range() -> None:
    rows = [
        (20260508, 100.0, 102.0, 99.0, 100.0, 1000.0),
        (20260509, 100.0, 101.0, 99.0, 100.5, 1000.0),
        (20260510, 100.5, 101.5, 99.5, 100.2, 1000.0),
        (20260511, 100.2, 101.0, 99.8, 100.4, 1000.0),
    ]

    result = classify_daily_chart_shape(rows, requested_window=4)

    assert result["shape_label"] == "sideways_range"
    assert result["shape_family"] == "range"
    assert result["bias"] == "neutral"
    assert "narrow_close_range" in result["reasons"]


def test_classify_daily_chart_shape_detects_gap_up_hold_high() -> None:
    rows = [
        (20260508, 100.0, 102.0, 99.0, 101.0, 1000.0),
        (20260509, 101.0, 103.0, 100.0, 102.0, 1000.0),
        (20260510, 112.0, 118.0, 111.0, 116.0, 5000.0),
        (20260511, 116.0, 117.0, 114.0, 115.5, 3000.0),
        (20260512, 115.5, 117.0, 114.5, 116.2, 2600.0),
        (20260513, 116.2, 118.0, 115.0, 116.8, 2500.0),
    ]

    result = classify_daily_chart_shape(rows, requested_window=5)

    assert result["shape_label"] == "gap_up_hold_high"
    assert result["shape_family"] == "gap_hold"
    assert result["bias"] == "bullish_watch"


def test_classify_daily_chart_shape_detects_gap_up_upper_wick_failure() -> None:
    rows = [
        (20260508, 100.0, 101.0, 99.0, 100.0, 1000.0),
        (20260509, 116.0, 130.0, 114.0, 115.0, 6000.0),
        (20260510, 115.0, 116.0, 110.0, 112.0, 4000.0),
        (20260511, 112.0, 113.0, 109.0, 110.0, 3000.0),
    ]

    result = classify_daily_chart_shape(rows, requested_window=4)

    assert result["shape_label"] == "gap_up_upper_wick_failure"
    assert result["shape_family"] == "gap_failure"
    assert "gap_event_upper_wick_rejection" in result["reasons"]


class _ShapeRepo:
    def get_daily_bars(self, code: str, limit: int = 400, asof_dt=None):
        return [
            (20260508, 100.0, 102.0, 99.0, 101.0, 1000.0),
            (20260509, 101.0, 103.0, 100.0, 102.0, 1000.0),
            (20260510, 120.0, 125.0, 119.0, 124.0, 5000.0),
            (20260511, 125.0, 126.0, 116.0, 117.0, 4000.0),
            (20260512, 117.0, 119.0, 115.0, 116.0, 3000.0),
            (20260513, 116.0, 118.0, 114.0, 115.0, 2500.0),
        ][-limit:]


def test_ticker_daily_shape_endpoint_returns_shape_contract() -> None:
    app = FastAPI()
    app.include_router(ticker.router)
    app.dependency_overrides[get_stock_repo] = lambda: _ShapeRepo()
    client = TestClient(app)

    response = client.get("/api/ticker/daily/shape", params={"code": "1001", "window": 5})

    assert response.status_code == 200
    payload = response.json()
    assert payload["code"] == "1001"
    assert payload["timeframe"] == "D"
    assert payload["shape"]["shape_label"] == "gap_up_stall_fade"
    assert payload["item"]["shape"]["shape_label"] == "gap_up_stall_fade"


def test_ticker_daily_shape_endpoint_returns_multi_window_contract() -> None:
    app = FastAPI()
    app.include_router(ticker.router)
    app.dependency_overrides[get_stock_repo] = lambda: _ShapeRepo()
    client = TestClient(app)

    response = client.get("/api/ticker/daily/shape", params={"code": "1001", "windows": "10,20,60"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["multi_window"]["windows"] == [10, 20, 60]
    assert payload["multi_window"]["event_shape"]["window"] == 10
    assert payload["multi_window"]["context_shape"]["window"] == 20
    assert payload["multi_window"]["trend_shape"]["window"] == 60
    assert payload["item"]["multi_window"]["by_window"]["10"]["shape_label"] == "gap_up_stall_fade"


def test_classify_daily_chart_shapes_by_window_returns_event_context_and_trend() -> None:
    rows = [
        (20260401 + idx, 100.0 + idx * 0.2, 101.0 + idx * 0.2, 99.0 + idx * 0.2, 100.5 + idx * 0.2, 1000.0)
        for idx in range(55)
    ] + [
        (20260526, 125.0, 128.0, 124.0, 126.0, 5000.0),
        (20260527, 126.0, 127.0, 119.0, 120.0, 4000.0),
        (20260528, 120.0, 121.0, 118.0, 119.0, 3000.0),
        (20260529, 119.0, 120.0, 117.0, 118.0, 2500.0),
        (20260530, 118.0, 119.0, 116.0, 117.0, 2200.0),
    ]

    result = classify_daily_chart_shapes_by_window(rows, requested_windows=(10, 20, 60))

    assert result["windows"] == [10, 20, 60]
    assert result["event_shape"]["window"] == 10
    assert result["context_shape"]["window"] == 20
    assert result["trend_shape"]["window"] == 60
    assert set(result["by_window"].keys()) == {"10", "20", "60"}
    assert result["event_shape"]["shape_label"] == "gap_up_stall_fade"
    assert result["context_shape"]["shape_label"] != "gap_up_stall_fade"
    assert result["trend_shape"]["shape_label"] != "gap_up_stall_fade"


def test_pattern_catalog_exposes_boundary_contract() -> None:
    catalog = get_chart_shape_pattern_catalog()

    assert catalog["gap_up_stall_fade"]["bias"] == "caution"
    assert catalog["breakout_hold"]["bias"] == "bullish_watch"
    assert catalog["sideways_range"]["actionability"] == "wait"


def test_ticker_daily_shape_patterns_endpoint_returns_catalog_contract() -> None:
    app = FastAPI()
    app.include_router(ticker.router)
    client = TestClient(app)

    response = client.get("/api/ticker/daily/shape/patterns")

    assert response.status_code == 200
    payload = response.json()
    assert payload["contract"]["scope"] == "display_confirmation_only"
    assert payload["contract"]["ranking_changed"] is False
    assert payload["contract"]["tradex_research_changed"] is False
    assert payload["contract"]["expectancy_validated"] is False
    assert payload["patterns"]["gap_up_stall_fade"]["actionability"] == "avoid_chase"
