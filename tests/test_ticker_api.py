from __future__ import annotations

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


class _TimelineRepo:
    def __init__(self) -> None:
        self.daily_calls = 0

    def get_analysis_timeline(self, code: str, asof_dt, *, limit: int = 400):
        return [{"dt": 20260310, "pUp": 0.6}]

    def get_daily_bars(self, code: str, limit: int = 500, asof_dt=None):
        self.daily_calls += 1
        return [
            (20260306, 100.0, 102.0, 99.0, 101.0, 1000.0),
            (20260307, 101.0, 103.0, 100.0, 102.0, 1100.0),
            (20260310, 102.0, 105.0, 101.0, 104.0, 1200.0),
        ]


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
