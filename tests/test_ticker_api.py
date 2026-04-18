from __future__ import annotations

import os

import duckdb
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.backend.api.dependencies import get_stock_repo
from app.backend.api.routers import ticker
from app.backend.infra.duckdb.stock_repo import StockRepository


def _build_ticker_client(repo: object) -> TestClient:
    app = FastAPI()
    app.include_router(ticker.router)
    app.dependency_overrides[get_stock_repo] = lambda: repo
    return TestClient(app)


def test_ticker_monthly_endpoint_returns_success_for_real_repo_shape(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "stocks.duckdb"
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            CREATE TABLE daily_bars (
                code TEXT,
                date BIGINT,
                o DOUBLE,
                h DOUBLE,
                l DOUBLE,
                c DOUBLE,
                v DOUBLE,
                source TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE monthly_bars (
                code TEXT,
                month BIGINT,
                o DOUBLE,
                h DOUBLE,
                l DOUBLE,
                c DOUBLE,
                v DOUBLE
            )
            """
        )
        conn.execute(
            """
            INSERT INTO daily_bars (code, date, o, h, l, c, v, source)
            VALUES
                ('1001', 20260110, 100, 105, 99, 104, 1000, 'pan'),
                ('1001', 20260210, 104, 109, 103, 108, 1100, 'pan'),
                ('1001', 20260310, 108, 112, 107, 111, 1200, 'pan')
            """
        )
        conn.execute(
            """
            INSERT INTO monthly_bars (code, month, o, h, l, c, v)
            VALUES
                ('1001', 202601, 100, 105, 99, 104, 1000),
                ('1001', 202602, 104, 109, 103, 108, 1100),
                ('1001', 202603, 108, 112, 107, 111, 1200)
            """
        )

    repo = StockRepository(str(db_path))
    monkeypatch.setattr(ticker, "get_provisional_daily_row_from_chart", lambda _code: None)
    monkeypatch.setattr(ticker, "_load_market_data_meta", lambda *args, **kwargs: None)
    client = _build_ticker_client(repo)

    response = client.get("/api/ticker/monthly", params={"code": "1001", "limit": 120})

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["data"]) == 3
    assert payload["errors"] == []


def test_ticker_detail_meta_separates_confirmed_and_provisional_basis(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "stocks.duckdb"
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            CREATE TABLE daily_bars (
                code TEXT,
                date BIGINT,
                o DOUBLE,
                h DOUBLE,
                l DOUBLE,
                c DOUBLE,
                v DOUBLE,
                source TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE monthly_bars (
                code TEXT,
                month BIGINT,
                o DOUBLE,
                h DOUBLE,
                l DOUBLE,
                c DOUBLE,
                v DOUBLE
            )
            """
        )
        conn.execute(
            """
            INSERT INTO daily_bars (code, date, o, h, l, c, v, source)
            VALUES
                ('1001', 20260401, 100, 105, 99, 104, 1000, 'pan'),
                ('1001', 20260403, 104, 109, 103, 108, 1100, 'pan')
            """
        )
        conn.execute(
            """
            INSERT INTO monthly_bars (code, month, o, h, l, c, v)
            VALUES
                ('1001', 202604, 100, 109, 99, 108, 2100)
            """
        )

    repo = StockRepository(str(db_path))
    monkeypatch.setattr(ticker, "_today_jst_key", lambda: 20260416)
    monkeypatch.setattr(
        ticker,
        "get_provisional_daily_row_from_chart",
        lambda _code: (20260416, 109.0, 112.0, 108.0, 111.0, 1000.0),
    )
    client = _build_ticker_client(repo)

    daily_response = client.get("/api/ticker/daily", params={"code": "1001", "limit": 120})
    monthly_response = client.get("/api/ticker/monthly", params={"code": "1001", "limit": 120})

    assert daily_response.status_code == 200
    assert monthly_response.status_code == 200

    daily_meta = daily_response.json()["meta"]
    monthly_meta = monthly_response.json()["meta"]

    assert daily_meta["confirmed_chart_source_provider"] == "chart_gallery_confirmed_source"
    assert daily_meta["provisional_chart_source_provider"] in {None, "yahoo_intraday_unconfirmed_source"}
    assert daily_meta["display_basis_classification"] in {"confirmed", "mixed", "provisional", None}
    assert daily_meta["judgment_basis_classification"] in {"confirmed", "provisional", "dual", None}
    assert daily_meta["overwrite_status"] in {
        "authoritative_confirmed",
        "provisional_only",
        "provisional_replaced_by_confirmed",
    }
    assert daily_meta["confirmed_last_available_date"] is not None
    assert daily_meta["provisional_last_available_date"] == 20260416

    assert monthly_meta["confirmed_chart_source_provider"] == "chart_gallery_confirmed_source"
    assert monthly_meta["provisional_chart_source_provider"] in {None, "yahoo_intraday_unconfirmed_source"}
    assert monthly_meta["display_basis_classification"] in {"confirmed", "mixed", "provisional", None}
    assert monthly_meta["judgment_basis_classification"] in {"confirmed", "provisional", "dual", None}
    assert monthly_meta["overwrite_status"] in {
        "authoritative_confirmed",
        "provisional_only",
        "provisional_replaced_by_confirmed",
    }
    assert monthly_meta["confirmed_last_available_date"] == daily_meta["confirmed_last_available_date"]


class _TimelineRepo:
    def __init__(self) -> None:
        self.daily_calls = 0
        self.monthly_calls = 0
        self.analysis_calls = 0
        self.timeline_calls = 0

    def get_analysis_timeline(self, code: str, asof_dt, *, limit: int = 400):
        self.timeline_calls += 1
        return [{"dt": 20260310, "pUp": 0.6}]

    def get_daily_bars(self, code: str, limit: int = 500, asof_dt=None):
        self.daily_calls += 1
        return [
            (20260306, 100.0, 102.0, 99.0, 101.0, 1000.0),
            (20260307, 101.0, 103.0, 100.0, 102.0, 1100.0),
            (20260310, 102.0, 105.0, 101.0, 104.0, 1200.0),
        ]

    def get_monthly_bars(self, code: str, limit: int, asof_dt=None, recent_daily_rows=None):
        self.monthly_calls += 1
        return [
            (202601, 100.0, 103.0, 99.0, 102.0, 1000.0),
            (202602, 102.0, 104.0, 101.0, 103.0, 1100.0),
        ]

    def get_ml_analysis_pred(self, code: str, asof_dt: int | None):
        self.analysis_calls += 1
        return (
            20260416,
            0.61,
            0.39,
            0.64,
            0.66,
            0.28,
            0.18,
            0.14,
            0.11,
            0.08,
            None,
            None,
            0.021,
            0.032,
            0.041,
            0.051,
            0.061,
            "model:v1",
        )

    def get_buy_stage_precision(self, code: str, asof_dt: int | None, *args, **kwargs):
        return None

    def get_sell_analysis_snapshot(self, code: str, asof_dt: int | None):
        return None


def test_ticker_analysis_timeline_reuses_cached_ranking_score(monkeypatch) -> None:
    repo = _TimelineRepo()
    ticker._TIMELINE_RANKING_CACHE.clear()
    ticker.inspect_detail_request_stats(reset=True)
    score_calls = {"count": 0}

    def _fake_score_weekly_candidate(code, name, daily_rows_asc, config, _unused):
        score_calls["count"] += 1
        return ({"total_score": 42.0}, None, None)

    monkeypatch.setattr(ticker.ranking, "score_weekly_candidate", _fake_score_weekly_candidate)
    client = _build_ticker_client(repo)

    first = client.get("/api/ticker/analysis/timeline", params={"code": "1001", "limit": 400})
    second = client.get("/api/ticker/analysis/timeline", params={"code": "1001", "limit": 400})

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["items"][0]["rankingScore"] == 42.0
    assert second.json()["items"][0]["rankingScore"] == 42.0
    assert repo.daily_calls == 1
    assert score_calls["count"] == 1

    stats = ticker.inspect_detail_request_stats(reset=False)
    assert stats["same_code_repeated_request_count"] >= 1


def test_ticker_analysis_cache_invalidates_after_runtime_db_mtime_change(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "stocks.duckdb"
    db_path.touch()
    os.utime(db_path, (1_700_000_000, 1_700_000_000))

    repo = _TimelineRepo()
    ticker._ANALYSIS_SERIES_CACHE.clear()
    ticker._TIMELINE_RANKING_CACHE.clear()
    monkeypatch.setattr(
        ticker,
        "_runtime_db_cache_marker",
        lambda: (str(db_path), os.path.getmtime(db_path)),
    )
    client = _build_ticker_client(repo)

    first = client.get("/api/ticker/analysis", params={"code": "1001", "asof": "2026-04-16"})
    second = client.get("/api/ticker/analysis", params={"code": "1001", "asof": "2026-04-16"})

    assert first.status_code == 200
    assert second.status_code == 200
    assert repo.daily_calls == 1
    assert repo.monthly_calls == 1
    assert repo.analysis_calls == 2

    os.utime(db_path, (1_700_000_100, 1_700_000_100))

    third = client.get("/api/ticker/analysis", params={"code": "1001", "asof": "2026-04-16"})

    assert third.status_code == 200
    assert repo.daily_calls == 2
    assert repo.monthly_calls == 2
    assert repo.analysis_calls == 3
