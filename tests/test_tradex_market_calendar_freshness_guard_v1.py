from __future__ import annotations

from datetime import date, datetime, time, timezone

import duckdb

from scripts import tradex_meemee_update_client_v1 as update_client
from scripts.tradex_market_calendar_v1 import (
    is_japan_market_business_day,
    previous_japan_market_business_day,
)
from scripts.tradex_runtime_freshness_guard_v1 import build_runtime_freshness_guard


def _epoch_ymd(value: date) -> int:
    return int(datetime.combine(value, time.min, tzinfo=timezone.utc).timestamp())


def _yyyymmdd(value: date) -> int:
    return int(value.strftime("%Y%m%d"))


def test_market_calendar_skips_weekends_holidays_and_year_end() -> None:
    assert previous_japan_market_business_day(date(2026, 7, 4)) == date(2026, 7, 3)
    assert previous_japan_market_business_day(date(2026, 7, 21)) == date(2026, 7, 17)
    assert previous_japan_market_business_day(date(2026, 9, 24)) == date(2026, 9, 18)
    assert previous_japan_market_business_day(date(2027, 1, 4)) == date(2026, 12, 30)

    assert is_japan_market_business_day(date(2026, 9, 23)) is False
    assert is_japan_market_business_day(date(2026, 9, 24)) is True


def test_runtime_freshness_guard_uses_market_business_day(tmp_path) -> None:
    db_path = tmp_path / "stocks.duckdb"
    with duckdb.connect(str(db_path)) as con:
        con.execute("CREATE TABLE daily_bars(code VARCHAR, date INTEGER, source VARCHAR)")
        con.execute(
            "INSERT INTO daily_bars VALUES ('0001', ?, 'pan'), ('0001', ?, 'yahoo')",
            [_epoch_ymd(date(2026, 7, 17)), _epoch_ymd(date(2026, 7, 20))],
        )
        con.execute(
            """
CREATE TABLE ranking_appearance_daily(
  ranking_logic_version VARCHAR,
  dir VARCHAR,
  dt INTEGER
)
"""
        )
        con.execute(
            "INSERT INTO ranking_appearance_daily VALUES ('ranking:trade:top50:v1', 'down', ?)",
            [_yyyymmdd(date(2026, 7, 17))],
        )

    guard = build_runtime_freshness_guard(
        db_path=db_path,
        today=date(2026, 7, 21),
        max_stale_calendar_days=4,
    )

    assert guard["pass"] is True
    assert guard["confirmed_max_date"] == "2026-07-17"
    assert guard["expected_latest_confirmed_date"] == "2026-07-17"
    assert guard["expected_latest_calendar"]["market"] == "JPX/TSE"
    assert guard["ranking_appearance_freshness"][0]["max_date"] == "2026-07-17"


def test_runtime_freshness_guard_fails_before_market_business_day(tmp_path) -> None:
    db_path = tmp_path / "stocks.duckdb"
    with duckdb.connect(str(db_path)) as con:
        con.execute("CREATE TABLE daily_bars(code VARCHAR, date INTEGER, source VARCHAR)")
        con.execute("INSERT INTO daily_bars VALUES ('0001', ?, 'pan')", [_epoch_ymd(date(2026, 7, 16))])

    guard = build_runtime_freshness_guard(
        db_path=db_path,
        today=date(2026, 7, 21),
        max_stale_calendar_days=7,
    )

    assert guard["pass"] is False
    assert guard["confirmed_max_date"] == "2026-07-16"
    assert guard["expected_latest_confirmed_date"] == "2026-07-17"
    assert "confirmed_daily_bars_before_effective_min_confirmed_date" in guard["failure_reasons"]


def test_meemee_update_client_submits_txt_update(monkeypatch) -> None:
    calls = []

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self) -> bytes:
            return b'{"ok": true, "job_id": "job-123"}'

    def fake_urlopen(request, timeout):
        calls.append((request.full_url, request.get_method(), timeout))
        return FakeResponse()

    monkeypatch.setattr(update_client, "urlopen", fake_urlopen)

    result = update_client.submit_txt_update(base_url="http://127.0.0.1:28888", timeout=3.0)

    assert result["ok"] is True
    assert result["job_id"] == "job-123"
    assert calls[0][1] == "POST"
    assert calls[0][0].startswith("http://127.0.0.1:28888/api/jobs/txt-update?")
