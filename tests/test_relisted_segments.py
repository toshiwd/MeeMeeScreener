from __future__ import annotations

from datetime import datetime, timezone

from app.backend.infra.duckdb.screener_repo import ScreenerRepository
from app.backend.infra.duckdb.stock_repo import StockRepository


def _ts(date_text: str) -> int:
    return int(datetime.fromisoformat(f"{date_text}T00:00:00+00:00").timestamp())


def _seed_tables(repo_db: str) -> None:
    import duckdb

    con = duckdb.connect(repo_db)
    con.execute("CREATE TABLE daily_bars (code TEXT, date BIGINT, o DOUBLE, h DOUBLE, l DOUBLE, c DOUBLE, v BIGINT)")
    con.execute("CREATE TABLE monthly_bars (code TEXT, month BIGINT, o DOUBLE, h DOUBLE, l DOUBLE, c DOUBLE, v BIGINT)")
    con.execute(
        "CREATE TABLE stock_meta (code TEXT, name TEXT, stage TEXT, score DOUBLE, reason TEXT, score_status TEXT, missing_reasons_json TEXT, score_breakdown_json TEXT)"
    )
    con.execute("CREATE TABLE earnings_planned (code TEXT, planned_date DATE)")
    con.execute("CREATE TABLE ex_rights (code TEXT, ex_date DATE, last_rights_date DATE)")
    con.execute("INSERT INTO stock_meta VALUES ('8729', 'ソニーフィナンシャルHD', 'C', 10, 'OK', 'OK', NULL, NULL)")
    con.executemany(
        "INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            ("8729", _ts("2020-08-28"), 2596.0, 2610.0, 2593.0, 2594.0, 1000),
            ("8729", _ts("2020-08-31"), 2598.0, 2608.0, 2590.0, 2597.0, 1000),
            ("8729", _ts("2026-03-12"), 149.0, 150.0, 148.0, 149.0, 1000),
            ("8729", _ts("2026-03-13"), 149.0, 150.3, 148.5, 149.6, 1000),
        ],
    )
    con.executemany(
        "INSERT INTO monthly_bars VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            ("8729", _ts("2020-08-01"), 2595.0, 2600.0, 2594.0, 2597.0, 1000),
            ("8729", _ts("2020-07-01"), 2596.0, 2610.0, 2593.0, 2594.0, 1000),
        ],
    )
    con.close()


def test_stock_repo_trims_relisted_daily_and_rebuilds_monthly(tmp_path, monkeypatch) -> None:
    db_path = str(tmp_path / "relisted.duckdb")
    _seed_tables(db_path)
    repo = StockRepository(db_path)
    monkeypatch.setattr(
        "app.backend.infra.duckdb.stock_repo.get_historical_daily_rows_from_chart",
        lambda code: [],
    )

    daily_rows = repo.get_daily_bars("8729", limit=260)
    monthly_rows = repo.get_monthly_bars("8729", limit=120)

    assert [row[0] for row in daily_rows] == [_ts("2026-03-12"), _ts("2026-03-13")]
    assert [row[0] for row in monthly_rows] == [_ts("2026-03-01")]
    assert monthly_rows[0][4] == 149.6


def test_stock_repo_prefers_yahoo_history_for_sparse_relisted_segment(tmp_path, monkeypatch) -> None:
    db_path = str(tmp_path / "relisted.duckdb")
    _seed_tables(db_path)
    repo = StockRepository(db_path)

    yahoo_rows = [
        (_ts("2025-09-29"), 173.8, 176.0, 172.5, 175.5, 1000.0),
        (_ts("2025-09-30"), 175.5, 177.0, 174.0, 176.2, 1000.0),
        (_ts("2026-03-12"), 149.0, 150.0, 148.0, 149.0, 1000.0),
        (_ts("2026-03-13"), 149.0, 150.3, 148.5, 149.6, 1000.0),
    ]
    monkeypatch.setattr(
        "app.backend.infra.duckdb.stock_repo.get_historical_daily_rows_from_chart",
        lambda code: yahoo_rows if code == "8729" else [],
    )

    daily_rows = repo.get_daily_bars("8729", limit=260)
    monthly_rows = repo.get_monthly_bars("8729", limit=120)

    assert [row[0] for row in daily_rows] == [row[0] for row in yahoo_rows]
    assert [row[0] for row in monthly_rows] == [_ts("2025-09-01"), _ts("2026-03-01")]
    assert monthly_rows[-1][4] == 149.6


def test_screener_repo_uses_latest_listing_segment(tmp_path, monkeypatch) -> None:
    db_path = str(tmp_path / "relisted.duckdb")
    _seed_tables(db_path)
    repo = ScreenerRepository(db_path)
    monkeypatch.setattr(
        "app.backend.infra.duckdb.screener_repo.get_historical_daily_rows_from_chart",
        lambda code: [],
    )

    codes, _meta, daily_rows, monthly_rows, _earnings, _rights = repo.fetch_screener_batch(
        daily_limit=260,
        earnings_start=datetime(2026, 3, 13, tzinfo=timezone.utc).date(),
        earnings_end=datetime(2026, 4, 30, tzinfo=timezone.utc).date(),
        rights_min_date=datetime(2026, 3, 13, tzinfo=timezone.utc).date(),
        monthly_limit=120,
    )

    assert codes == ["8729"]
    assert [row[1] for row in daily_rows] == [_ts("2026-03-12"), _ts("2026-03-13")]
    assert [row[1] for row in monthly_rows] == [_ts("2026-03-01")]


def test_screener_repo_prefers_yahoo_history_for_sparse_relisted_segment(tmp_path, monkeypatch) -> None:
    db_path = str(tmp_path / "relisted.duckdb")
    _seed_tables(db_path)
    repo = ScreenerRepository(db_path)

    yahoo_rows = [
        (_ts("2025-09-29"), 173.8, 176.0, 172.5, 175.5, 1000.0),
        (_ts("2025-09-30"), 175.5, 177.0, 174.0, 176.2, 1000.0),
        (_ts("2026-03-12"), 149.0, 150.0, 148.0, 149.0, 1000.0),
        (_ts("2026-03-13"), 149.0, 150.3, 148.5, 149.6, 1000.0),
    ]
    monkeypatch.setattr(
        "app.backend.infra.duckdb.screener_repo.get_historical_daily_rows_from_chart",
        lambda code: yahoo_rows if code == "8729" else [],
    )

    codes, _meta, daily_rows, monthly_rows, _earnings, _rights = repo.fetch_screener_batch(
        daily_limit=260,
        earnings_start=datetime(2026, 3, 13, tzinfo=timezone.utc).date(),
        earnings_end=datetime(2026, 4, 30, tzinfo=timezone.utc).date(),
        rights_min_date=datetime(2026, 3, 13, tzinfo=timezone.utc).date(),
        monthly_limit=120,
    )

    assert codes == ["8729"]
    assert [row[1] for row in daily_rows] == [row[0] for row in yahoo_rows]
    assert [row[1] for row in monthly_rows] == [_ts("2025-09-01"), _ts("2026-03-01")]
