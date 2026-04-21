from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone

import duckdb

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from app.backend.infra.duckdb.stock_repo import StockRepository


def _ymd(value: datetime) -> int:
    return int(value.strftime("%Y%m%d"))


def _seed_timeframe_db(db_path: str) -> None:
    with duckdb.connect(db_path, read_only=False) as conn:
        conn.execute(
            """
            CREATE TABLE daily_bars (
                code VARCHAR,
                date INTEGER,
                o DOUBLE,
                h DOUBLE,
                l DOUBLE,
                c DOUBLE,
                v BIGINT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE monthly_bars (
                code VARCHAR,
                month INTEGER,
                o DOUBLE,
                h DOUBLE,
                l DOUBLE,
                c DOUBLE,
                v BIGINT
            )
            """
        )

        start = datetime(2026, 4, 6, tzinfo=timezone.utc)
        weekly_rows = []
        current = start
        while len(weekly_rows) < 10:
            if current.weekday() < 5:
                offset = len(weekly_rows)
                open_ = 100.0 + offset
                weekly_rows.append(
                    (
                        "W1",
                        _ymd(current),
                        open_,
                        open_ + 10.0,
                        open_ - 5.0,
                        open_ + 3.0,
                        1000 + offset,
                    )
                )
            current += timedelta(days=1)
        conn.executemany("INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?)", weekly_rows)

        conn.executemany(
            "INSERT INTO monthly_bars VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                ("FRESH", 202603, 90.0, 100.0, 88.0, 98.0, 10000),
                ("FRESH", 202604, 99.0, 110.0, 95.0, 108.0, 12000),
                ("STALE", 202511, 80.0, 90.0, 75.0, 88.0, 9000),
            ],
        )


def test_get_weekly_bars_batch_aggregates_and_limits(tmp_path) -> None:
    db_path = tmp_path / "stocks.duckdb"
    _seed_timeframe_db(str(db_path))
    repo = StockRepository(str(db_path))

    rows = repo.get_weekly_bars_batch(["W1"], limit=2)["W1"]

    assert len(rows) == 2
    assert rows[0][0] < rows[1][0]
    assert rows[0][1:] == (100.0, 114.0, 95.0, 107.0, 5010.0)
    assert rows[1][1:] == (105.0, 119.0, 100.0, 112.0, 5035.0)


def test_get_monthly_bars_batch_uses_small_patch_then_large_fallback(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "stocks.duckdb"
    _seed_timeframe_db(str(db_path))
    repo = StockRepository(str(db_path))
    calls: list[tuple[tuple[str, ...], int]] = []

    def _make_rows(start: datetime, count: int) -> list[tuple[int, float, float, float, float, float]]:
        rows: list[tuple[int, float, float, float, float, float]] = []
        current = start
        while len(rows) < count:
            if current.weekday() < 5:
                base = 100.0 + len(rows)
                rows.append((_ymd(current), base, base + 2.0, base - 2.0, base + 1.0, 1000.0 + len(rows)))
            current += timedelta(days=1)
        return rows

    fresh_patch = _make_rows(datetime(2026, 4, 1, tzinfo=timezone.utc), 3)
    stale_patch = _make_rows(datetime(2026, 4, 1, tzinfo=timezone.utc), 3)
    missing_patch = _make_rows(datetime(2026, 4, 1, tzinfo=timezone.utc), 3)
    stale_full = _make_rows(datetime(2026, 1, 5, tzinfo=timezone.utc), 70)
    missing_full = _make_rows(datetime(2026, 2, 2, tzinfo=timezone.utc), 70)

    def fake_get_daily_bars_batch(codes, limit, asof_dt=None):
        ordered_codes = tuple(codes)
        calls.append((ordered_codes, limit))
        if limit == 45:
            source = {
                "FRESH": fresh_patch,
                "STALE": stale_patch,
                "MISSING": missing_patch,
            }
        elif limit == 300:
            source = {
                "STALE": stale_full,
                "MISSING": missing_full,
            }
        else:  # pragma: no cover - guardrail
            raise AssertionError(f"unexpected daily limit {limit}")
        return {code: list(source.get(code, [])) for code in codes}

    monkeypatch.setattr(repo, "get_daily_bars_batch", fake_get_daily_bars_batch)

    rows = repo.get_monthly_bars_batch(["FRESH", "STALE", "MISSING"], limit=12)

    assert calls == [
        (("FRESH", "STALE", "MISSING"), 45),
        (("STALE", "MISSING"), 300),
    ]
    assert rows["FRESH"][-1][0] == 202604
    assert rows["STALE"]
    assert rows["MISSING"]
