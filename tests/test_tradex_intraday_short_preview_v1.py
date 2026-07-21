from __future__ import annotations

from pathlib import Path

import duckdb

from scripts.tradex_intraday_short_preview_v1 import build_intraday_short_preview


def _db(path: Path, *, with_yahoo: bool) -> None:
    with duckdb.connect(str(path)) as conn:
        conn.execute("CREATE TABLE daily_bars(code VARCHAR,date BIGINT,o DOUBLE,h DOUBLE,l DOUBLE,c DOUBLE,v DOUBLE,source VARCHAR)")
        conn.execute("CREATE TABLE industry_master(code VARCHAR,name VARCHAR)")
        conn.execute("INSERT INTO industry_master VALUES ('1111','sample')")
        for day in range(1, 22):
            conn.execute("INSERT INTO daily_bars VALUES ('1111',?,?,?,?,?,?, 'pan')", [20260600 + day, 100, 101, 99, 100, 100])
        if with_yahoo:
            conn.execute("INSERT INTO daily_bars VALUES ('1111',20260622,100,100,80,80,400,'yahoo')")


def test_preview_reports_unavailable_when_sync_has_removed_yahoo_rows(tmp_path: Path) -> None:
    path = tmp_path / "stocks.duckdb"
    _db(path, with_yahoo=False)

    result = build_intraday_short_preview(path)

    assert result["status"] == "no_newer_provisional_bar"
    assert result["intraday_available"] is False
    assert result["candidate_count"] == 0


def test_preview_uses_newer_yahoo_row_without_treating_it_as_confirmed(tmp_path: Path) -> None:
    path = tmp_path / "stocks.duckdb"
    _db(path, with_yahoo=True)

    result = build_intraday_short_preview(path)

    assert result["confirmed_ymd"] == 20260621
    assert result["provisional_ymd"] == 20260622
    assert result["intraday_available"] is True
    assert result["contract"]["not_entry_signal"] is True
    assert result["candidates"][0]["state"] == "引け前候補"
