from __future__ import annotations

from datetime import datetime, timezone

from app.backend.services import yahoo_provisional as yp


def setup_function() -> None:
    yp._clear_caches_for_tests()


def test_merge_appends_only_when_newer() -> None:
    base = [(1772150400, 3900.0, 3950.0, 3890.0, 3940.0, 1000.0)]
    provisional = (1772433000, 3944.0, 3990.0, 3930.0, 3980.0, 1500.0)
    merged = yp.merge_daily_rows_with_provisional(base, provisional)
    assert len(merged) == 2
    assert merged[-1][0] == 1772433000

    same_day = yp.merge_daily_rows_with_provisional(base, (1772150400, 4000, 4010, 3990, 4005, 2000))
    assert same_day == base

    older = yp.merge_daily_rows_with_provisional(base, (1772064000, 3900, 3910, 3890, 3905, 900))
    assert older == base


def test_merge_respects_asof_limit() -> None:
    base = [(1772150400, 3900.0, 3950.0, 3890.0, 3940.0, 1000.0)]
    provisional = (1772433000, 3944.0, 3990.0, 3930.0, 3980.0, 1500.0)
    asof_dt = int(datetime(2026, 3, 1, tzinfo=timezone.utc).timestamp())
    merged = yp.merge_daily_rows_with_provisional(base, provisional, asof_dt=asof_dt)
    assert merged == base


def test_merge_skips_close_only_zero_volume_provisional() -> None:
    base = [(1772150400, 3900.0, 3950.0, 3890.0, 3940.0, 1000.0)]
    close_only = (1772433000, 3980.0, 3980.0, 3980.0, 3980.0, 0.0)
    merged = yp.merge_daily_rows_with_provisional(base, close_only)
    assert len(merged) == 2
    assert merged[-1] == close_only


def test_merge_replaces_same_day_close_only_row() -> None:
    base = [(1772433000, 3980.0, 3980.0, 3980.0, 3980.0, 0.0)]
    chart_row = (1772433000, 3944.0, 3990.0, 3930.0, 3980.0, 1500.0)
    merged = yp.merge_daily_rows_with_provisional(base, chart_row)
    assert merged == [chart_row]


def test_merge_keeps_same_day_close_only_when_provisional_is_close_only() -> None:
    base = [
        (1772150400, 3900.0, 3950.0, 3890.0, 3940.0, 1000.0),
        (1772433000, 3980.0, 3980.0, 3980.0, 3980.0, 0.0),
    ]
    close_only = (1772433000, 3981.0, 3981.0, 3981.0, 3981.0, 0.0)
    merged = yp.merge_daily_rows_with_provisional(base, close_only)
    assert merged == base


def test_split_gap_adjustment_scales_forward_split_rows() -> None:
    rows = [
        (1, 100.0, 110.0, 95.0, 100.0, 10.0),
        (2, 102.0, 111.0, 98.0, 102.0, 12.0),
        (3, 51.0, 56.0, 50.0, 51.0, 14.0),
        (4, 52.0, 57.0, 51.0, 52.0, 16.0),
    ]
    adjusted = yp.apply_split_gap_adjustment(rows)
    assert adjusted[0][4] == 100.0
    assert adjusted[1][4] == 102.0
    assert adjusted[2][4] == 102.0
    assert adjusted[3][4] == 104.0


def test_split_gap_adjustment_scales_reverse_split_rows() -> None:
    rows = [
        (1, 100.0, 101.0, 99.0, 100.0, 10.0),
        (2, 98.0, 99.0, 97.0, 98.0, 12.0),
        (3, 490.0, 495.0, 480.0, 490.0, 14.0),
        (4, 500.0, 505.0, 499.0, 500.0, 16.0),
    ]
    adjusted = yp.apply_split_gap_adjustment(rows)
    assert adjusted[0][4] == 100.0
    assert adjusted[1][4] == 98.0
    assert adjusted[2][4] == 98.0
    assert adjusted[3][4] == 100.0


def test_split_gap_adjustment_supports_large_integer_factor() -> None:
    rows = [
        (1, 2600.0, 2600.0, 2597.0, 2597.0, 10.0),
        (2, 153.0, 153.0, 153.0, 153.0, 0.0),
    ]
    adjusted = yp.apply_split_gap_adjustment(rows)
    assert abs(adjusted[1][4] - 2601.0) < 1e-6


def test_split_gap_adjustment_does_not_adjust_non_uniform_drop() -> None:
    rows = [
        (1, 100.0, 105.0, 95.0, 100.0, 10.0),
        (2, 60.0, 70.0, 50.0, 50.0, 12.0),
    ]
    adjusted = yp.apply_split_gap_adjustment(rows)
    assert adjusted == rows


def test_split_gap_adjustment_keeps_normal_moves() -> None:
    rows = [
        (1, 100.0, 103.0, 99.0, 100.0, 10.0),
        (2, 101.0, 104.0, 100.0, 101.0, 12.0),
        (3, 102.0, 105.0, 101.0, 102.0, 11.0),
    ]
    adjusted = yp.apply_split_gap_adjustment(rows)
    assert adjusted == rows


def test_invalid_code_skips_fetch(monkeypatch) -> None:
    monkeypatch.setenv("MEEMEE_YF_PROVISIONAL_ENABLED", "1")
    called = {"count": 0}

    def _fake_fetch_json(_url: str, *, timeout_sec: float):
        called["count"] += 1
        return {}

    monkeypatch.setattr(yp, "_fetch_json", _fake_fetch_json)

    assert yp.get_provisional_daily_row_from_chart("72031") is None
    assert yp.get_provisional_daily_row_from_chart("ABCD") is None
    assert called["count"] == 0


def test_spark_builds_close_only_synthetic_ohlc(monkeypatch) -> None:
    monkeypatch.setenv("MEEMEE_YF_PROVISIONAL_ENABLED", "1")

    def _fake_fetch_json(_url: str, *, timeout_sec: float):
        _ = timeout_sec
        return {
            "spark": {
                "result": [
                    {
                        "symbol": "7203.T",
                        "response": [
                            {
                                "timestamp": [1772433000],
                                "indicators": {"quote": [{"close": [3944.0]}]},
                            }
                        ],
                    },
                    {
                        "symbol": "1306.T",
                        "response": [
                            {
                                "timestamp": [1772433000],
                                "indicators": {"quote": [{"close": [None]}]},
                            }
                        ],
                    },
                ]
            }
        }

    monkeypatch.setattr(yp, "_fetch_json", _fake_fetch_json)
    rows = yp.get_provisional_daily_rows_from_spark(["7203", "1306", "abc"])

    assert "7203" in rows
    assert "1306" not in rows

    row = rows["7203"]
    assert row[0] == 1772433000
    assert row[1] == row[2] == row[3] == row[4] == 3944.0
    assert row[5] == 0.0


def test_chart_fetch_failure_is_cached_and_merge_keeps_original(monkeypatch) -> None:
    monkeypatch.setenv("MEEMEE_YF_PROVISIONAL_ENABLED", "1")
    called = {"count": 0}

    def _fake_fetch_json(_url: str, *, timeout_sec: float):
        _ = timeout_sec
        called["count"] += 1
        raise TimeoutError("network timeout")

    monkeypatch.setattr(yp, "_fetch_json", _fake_fetch_json)

    first = yp.get_provisional_daily_row_from_chart("7203")
    second = yp.get_provisional_daily_row_from_chart("7203")
    assert first is None
    assert second is None
    assert called["count"] == 1

    base = [(1772150400, 3900.0, 3950.0, 3890.0, 3940.0, 1000.0)]
    merged = yp.merge_daily_rows_with_provisional(base, first)
    assert merged == base


def test_chart_volume_is_converted_to_pan_unit(monkeypatch) -> None:
    monkeypatch.setenv("MEEMEE_YF_PROVISIONAL_ENABLED", "1")

    def _fake_fetch_json(_url: str, *, timeout_sec: float):
        _ = timeout_sec
        return {
            "chart": {
                "result": [
                    {
                        "timestamp": [1772433000],
                        "indicators": {
                            "quote": [
                                {
                                    "open": [3900.0],
                                    "high": [4000.0],
                                    "low": [3890.0],
                                    "close": [3944.0],
                                    "volume": [28621000],
                                }
                            ]
                        },
                    }
                ]
            }
        }

    monkeypatch.setattr(yp, "_fetch_json", _fake_fetch_json)
    row = yp.get_provisional_daily_row_from_chart("7203")
    assert row is not None
    assert row[5] == 28621.0
