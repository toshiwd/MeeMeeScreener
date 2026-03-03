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
