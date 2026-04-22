from __future__ import annotations

from datetime import datetime, timezone
import os
from fastapi import FastAPI
from fastapi.testclient import TestClient

import app.backend.api.routers.bars as bars_module
from app.backend.api.dependencies import get_stock_repo


class _FakeRepo:
    def get_daily_bars_batch(self, codes, limit, asof_dt=None):
        rows = [
            (20260310, 100.0, 110.0, 95.0, 105.0, 1000.0),
            (20260311, 106.0, 112.0, 101.0, 111.0, 1200.0),
            (20260312, 111.0, 115.0, 109.0, 114.0, 900.0),
        ]
        return {code: rows[-limit:] for code in codes}

    def get_weekly_bars_batch(self, codes, limit, asof_dt=None):
        rows = [
            (1773014400, 100.0, 115.0, 95.0, 114.0, 3100.0),
        ]
        return {code: rows[-limit:] for code in codes}

    def get_monthly_bars_batch(self, codes, limit, asof_dt=None, recent_daily_rows_by_code=None):
        rows = [
            (202601, 90.0, 101.0, 88.0, 100.0, 10000.0),
            (202602, 100.0, 108.0, 96.0, 107.0, 11000.0),
            (202603, 107.0, 109.0, 103.0, 108.0, 5000.0),
        ]
        return {code: rows[-limit:] for code in codes}


class _ProvenanceRepo:
    def get_daily_bars_batch(self, codes, limit, asof_dt=None):
        rows = [
            (20260401, 100.0, 110.0, 95.0, 105.0, 1000.0),
            (20260402, 106.0, 112.0, 101.0, 111.0, 1200.0),
            (20260403, 111.0, 115.0, 109.0, 114.0, 900.0),
        ]
        return {code: rows[-limit:] for code in codes}

    def get_weekly_bars_batch(self, codes, limit, asof_dt=None):
        rows = [
            (1775433600, 100.0, 115.0, 95.0, 114.0, 3100.0),
        ]
        return {code: rows[-limit:] for code in codes}

    def get_monthly_bars_batch(self, codes, limit, asof_dt=None, recent_daily_rows_by_code=None):
        rows = [
            (202603, 90.0, 101.0, 88.0, 100.0, 10000.0),
            (202604, 100.0, 115.0, 95.0, 114.0, 12000.0),
        ]
        return {code: rows[-limit:] for code in codes}


def _clear_batch_bars_cache() -> None:
    bars_module._batch_v3_cache.clear()
    bars_module._batch_v3_inflight.clear()


def _build_client() -> TestClient:
    app = FastAPI()
    app.include_router(bars_module.router)
    app.dependency_overrides[get_stock_repo] = lambda: _FakeRepo()
    return TestClient(app)


def test_batch_bars_v3_skips_monthly_box_detection_when_include_boxes_is_false(monkeypatch) -> None:
    calls: list[int] = []

    def _fail_if_called(rows, range_basis="body", max_range_pct=0.2):
        calls.append(len(rows))
        return [{"startTime": 1, "endTime": 2}]

    monkeypatch.setattr(bars_module, "detect_boxes", _fail_if_called)
    _clear_batch_bars_cache()
    client = _build_client()

    response = client.post(
        "/api/batch_bars_v3",
        json={
            "codes": ["7203"],
            "timeframes": ["monthly"],
            "limit": 24,
            "includeProvisional": True,
            "includeBoxes": False,
        },
    )

    assert response.status_code == 200
    payload = response.json()["items"]["7203"]["monthly"]
    assert payload["bars"]
    assert payload["boxes"] == []
    assert calls == []


def test_batch_bars_v3_returns_monthly_boxes_when_include_boxes_is_true(monkeypatch) -> None:
    calls: list[int] = []

    def _fake_detect_boxes(rows, range_basis="body", max_range_pct=0.2):
        calls.append(len(rows))
        return [{"startTime": 1, "endTime": 2}]

    monkeypatch.setattr(bars_module, "detect_boxes", _fake_detect_boxes)
    _clear_batch_bars_cache()
    client = _build_client()

    response = client.post(
        "/api/batch_bars_v3",
        json={
            "codes": ["7203"],
            "timeframes": ["monthly"],
            "limit": 24,
            "includeProvisional": True,
            "includeBoxes": True,
        },
    )

    assert response.status_code == 200
    payload = response.json()["items"]["7203"]["monthly"]
    assert payload["bars"]
    assert payload["boxes"] == [{"startTime": 1, "endTime": 2}]
    assert calls == [3]


def test_batch_bars_v3_marks_live_provisional_data_version(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "stocks.duckdb"
    db_path.touch()
    os.utime(db_path, (1_700_000_000, 1_700_000_000))
    monkeypatch.setenv("STOCKS_DB_PATH", str(db_path))

    class _FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):  # type: ignore[override]
            return datetime(2026, 4, 13, 0, 0, 0, tzinfo=timezone.utc)

    monkeypatch.setattr(bars_module, "datetime", _FixedDatetime)
    _clear_batch_bars_cache()

    client = _build_client()
    response = client.post(
        "/api/batch_bars_v3",
        json={
            "codes": ["7203"],
            "timeframes": ["daily"],
            "limit": 24,
            "includeProvisional": True,
            "includeBoxes": False,
        },
    )

    assert response.status_code == 200
    data_version = response.json()["meta"]["data_version"]
    assert data_version == "duckdb-mtime:1700000000.000000|yf-live:202604130900"


def test_batch_bars_v3_exposes_chart_provenance_for_provisional_overlay(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "stocks.duckdb"
    db_path.touch()
    os.utime(db_path, (1_700_000_000, 1_700_000_000))
    monkeypatch.setenv("STOCKS_DB_PATH", str(db_path))

    class _FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):  # type: ignore[override]
            return datetime(2026, 4, 16, 12, 0, 0, tzinfo=timezone.utc)

    monkeypatch.setattr(bars_module, "datetime", _FixedDatetime)
    monkeypatch.setattr(
        bars_module,
        "get_provisional_daily_rows_from_spark",
        lambda codes, prefer_chart_ohlc=True: {code: (1776297600, 116.0, 118.0, 114.0, 117.5, 1500.0) for code in codes},
    )
    _clear_batch_bars_cache()

    client = FastAPI()
    client.include_router(bars_module.router)
    client.dependency_overrides[get_stock_repo] = lambda: _ProvenanceRepo()
    test_client = TestClient(client)

    response = test_client.post(
        "/api/batch_bars_v3",
        json={
            "codes": ["7203"],
            "timeframes": ["daily", "weekly", "monthly"],
            "limit": 24,
            "includeProvisional": True,
            "includeBoxes": True,
        },
    )

    assert response.status_code == 200
    payload = response.json()["items"]["7203"]
    daily = payload["daily"]
    weekly = payload["weekly"]
    monthly = payload["monthly"]

    assert daily["provenance"]["chart_source_provider"] == "runtime_stock_db.daily_bars+yahoo_chart_overlay"
    assert daily["provenance"]["chart_last_confirmed_date"] == 20260403
    assert daily["provenance"]["chart_last_provisional_date"] == 20260416
    assert daily["provenance"]["chart_date_match_status"] == "lagged_provisional"
    assert daily["provenance"]["chart_source_freshness_status"] == "lagged"
    assert daily["provenance"]["chart_data_classification"] == "mixed"
    assert daily["provenance"]["confirmed_chart_source_provider"] == "chart_gallery_confirmed_source"
    assert daily["provenance"]["provisional_chart_source_provider"] == "yahoo_intraday_unconfirmed_source"
    assert daily["provenance"]["confirmed_judgment_available"] is False
    assert daily["provenance"]["provisional_judgment_available"] is True
    assert daily["provenance"]["display_basis_classification"] == "mixed"
    assert daily["provenance"]["judgment_basis_classification"] == "provisional"
    assert daily["provenance"]["confirmed_last_available_date"] == 20260403
    assert daily["provenance"]["provisional_last_available_date"] == 20260416
    assert daily["provenance"]["overwrite_status"] == "provisional_only"
    assert weekly["provenance"]["chart_aggregation_source"] == "derived"
    assert monthly["provenance"]["chart_source_provider"].startswith("runtime_stock_db.monthly_bars+runtime_stock_db.daily_bars")


def test_batch_bars_v3_prefers_confirmed_overlapping_chart_gallery_data(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "stocks.duckdb"
    db_path.touch()
    os.utime(db_path, (1_700_000_000, 1_700_000_000))
    monkeypatch.setenv("STOCKS_DB_PATH", str(db_path))

    class _FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):  # type: ignore[override]
            return datetime(2026, 4, 16, 12, 0, 0, tzinfo=timezone.utc)

    class _ConfirmedOverlapRepo:
        def get_daily_bars_batch(self, codes, limit, asof_dt=None):
            rows = [
                (20260414, 100.0, 110.0, 95.0, 105.0, 1000.0),
                (20260415, 106.0, 112.0, 101.0, 111.0, 1200.0),
                (20260416, 111.0, 115.0, 109.0, 114.0, 900.0),
            ]
            return {code: rows[-limit:] for code in codes}

        def get_weekly_bars_batch(self, codes, limit, asof_dt=None):
            rows = [
                (1776038400, 100.0, 115.0, 95.0, 114.0, 3100.0),
            ]
            return {code: rows[-limit:] for code in codes}

        def get_monthly_bars_batch(self, codes, limit, asof_dt=None, recent_daily_rows_by_code=None):
            rows = [
                (202603, 90.0, 101.0, 88.0, 100.0, 10000.0),
                (202604, 100.0, 115.0, 95.0, 114.0, 12000.0),
            ]
            return {code: rows[-limit:] for code in codes}

    monkeypatch.setattr(bars_module, "datetime", _FixedDatetime)
    monkeypatch.setattr(
        bars_module,
        "get_provisional_daily_rows_from_spark",
        lambda codes, prefer_chart_ohlc=True: {code: (1776297600, 116.0, 118.0, 114.0, 117.5, 1500.0) for code in codes},
    )
    _clear_batch_bars_cache()

    client = FastAPI()
    client.include_router(bars_module.router)
    client.dependency_overrides[get_stock_repo] = lambda: _ConfirmedOverlapRepo()
    test_client = TestClient(client)

    response = test_client.post(
        "/api/batch_bars_v3",
        json={
            "codes": ["7203"],
            "timeframes": ["daily", "weekly", "monthly"],
            "limit": 24,
            "includeProvisional": True,
            "includeBoxes": True,
        },
    )

    assert response.status_code == 200
    payload = response.json()["items"]["7203"]
    daily = payload["daily"]
    assert daily["provenance"]["chart_source_provider"] == "runtime_stock_db.daily_bars"
    assert daily["provenance"]["chart_data_classification"] == "confirmed"
    assert daily["provenance"]["confirmed_judgment_available"] is True
    assert daily["provenance"]["provisional_judgment_available"] is False
    assert daily["provenance"]["display_basis_classification"] == "confirmed"
    assert daily["provenance"]["judgment_basis_classification"] == "confirmed"
    assert daily["provenance"]["overwrite_status"] == "provisional_replaced_by_confirmed"


def test_batch_bars_v3_uses_direct_weekly_source_without_daily_fetch(monkeypatch) -> None:
    class _WeeklyOnlyRepo:
        def get_daily_bars_batch(self, codes, limit, asof_dt=None):  # pragma: no cover - guardrail
            raise AssertionError("weekly-only request should not fetch daily bars without live patch")

        def get_weekly_bars_batch(self, codes, limit, asof_dt=None):
            rows = [
                (1773014400, 100.0, 120.0, 95.0, 118.0, 4000.0),
                (1773619200, 119.0, 125.0, 110.0, 123.0, 3500.0),
            ]
            return {code: rows[-limit:] for code in codes}

        def get_monthly_bars_batch(self, codes, limit, asof_dt=None, recent_daily_rows_by_code=None):
            raise AssertionError("monthly source should not be used")

    _clear_batch_bars_cache()
    client = FastAPI()
    client.include_router(bars_module.router)
    client.dependency_overrides[get_stock_repo] = lambda: _WeeklyOnlyRepo()
    test_client = TestClient(client)

    response = test_client.post(
        "/api/batch_bars_v3",
        json={
            "codes": ["7203"],
            "timeframes": ["weekly"],
            "limit": 120,
            "includeProvisional": False,
            "includeBoxes": False,
        },
    )

    assert response.status_code == 200
    weekly = response.json()["items"]["7203"]["weekly"]["bars"]
    assert len(weekly) == 2
    assert weekly[-1][4] == 123.0


def test_batch_bars_v3_patches_current_week_with_daily_tail(monkeypatch) -> None:
    class _WeeklyPatchRepo:
        def get_daily_bars_batch(self, codes, limit, asof_dt=None):
            rows = [
                (20260413, 100.0, 110.0, 95.0, 106.0, 1000.0),
                (20260414, 107.0, 112.0, 104.0, 110.0, 1200.0),
            ]
            return {code: rows[-limit:] for code in codes}

        def get_weekly_bars_batch(self, codes, limit, asof_dt=None):
            rows = [
                (1776038400, 100.0, 112.0, 95.0, 110.0, 2200.0),
            ]
            return {code: rows[-limit:] for code in codes}

        def get_monthly_bars_batch(self, codes, limit, asof_dt=None, recent_daily_rows_by_code=None):
            raise AssertionError("monthly source should not be used")

    class _FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):  # type: ignore[override]
            return datetime(2026, 4, 15, 12, 0, 0, tzinfo=timezone.utc)

    monkeypatch.setattr(bars_module, "datetime", _FixedDatetime)
    monkeypatch.setattr(
        bars_module,
        "get_provisional_daily_rows_from_spark",
        lambda codes, prefer_chart_ohlc=True: {code: (20260415, 109.0, 118.0, 103.0, 117.0, 1500.0) for code in codes},
    )
    _clear_batch_bars_cache()
    client = FastAPI()
    client.include_router(bars_module.router)
    client.dependency_overrides[get_stock_repo] = lambda: _WeeklyPatchRepo()
    test_client = TestClient(client)

    response = test_client.post(
        "/api/batch_bars_v3",
        json={
            "codes": ["7203"],
            "timeframes": ["weekly"],
            "limit": 120,
            "includeProvisional": True,
            "includeBoxes": False,
        },
    )

    assert response.status_code == 200
    weekly = response.json()["items"]["7203"]["weekly"]
    assert weekly["bars"][-1] == [1776038400, 100.0, 118.0, 95.0, 117.0, 3700.0]
    assert weekly["provenance"]["chart_aggregation_source"] == "derived"


def test_batch_bars_v3_uses_timeframe_specific_sources(monkeypatch) -> None:
    calls: list[tuple[str, tuple[str, ...], int]] = []

    class _MixedSourceRepo:
        def get_daily_bars_batch(self, codes, limit, asof_dt=None):
            calls.append(("daily", tuple(codes), limit))
            rows = [
                (20260401, 100.0, 110.0, 95.0, 105.0, 1000.0),
                (20260402, 106.0, 112.0, 101.0, 111.0, 1200.0),
            ]
            return {code: rows[-limit:] for code in codes}

        def get_weekly_bars_batch(self, codes, limit, asof_dt=None):
            calls.append(("weekly", tuple(codes), limit))
            rows = [
                (1775433600, 100.0, 115.0, 95.0, 114.0, 3100.0),
            ]
            return {code: rows[-limit:] for code in codes}

        def get_monthly_bars_batch(self, codes, limit, asof_dt=None, recent_daily_rows_by_code=None):
            calls.append(("monthly", tuple(codes), limit))
            rows = [
                (202603, 90.0, 101.0, 88.0, 100.0, 10000.0),
                (202604, 100.0, 115.0, 95.0, 114.0, 12000.0),
            ]
            return {code: rows[-limit:] for code in codes}

    _clear_batch_bars_cache()
    client = FastAPI()
    client.include_router(bars_module.router)
    client.dependency_overrides[get_stock_repo] = lambda: _MixedSourceRepo()
    test_client = TestClient(client)

    response = test_client.post(
        "/api/batch_bars_v3",
        json={
            "codes": ["7203"],
            "timeframes": ["daily", "weekly", "monthly"],
            "limit": 120,
            "timeframeLimits": {"daily": 5, "weekly": 40, "monthly": 12},
            "includeProvisional": False,
            "includeBoxes": False,
        },
    )

    assert response.status_code == 200
    assert calls == [
        ("daily", ("7203",), 5),
        ("weekly", ("7203",), 40),
        ("monthly", ("7203",), 12),
    ]


def test_batch_bars_v3_omits_live_bucket_for_historical_asof(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "stocks.duckdb"
    db_path.touch()
    os.utime(db_path, (1_700_000_000, 1_700_000_000))
    monkeypatch.setenv("STOCKS_DB_PATH", str(db_path))

    class _FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):  # type: ignore[override]
            return datetime(2026, 4, 13, 0, 0, 0, tzinfo=timezone.utc)

    monkeypatch.setattr(bars_module, "datetime", _FixedDatetime)

    client = _build_client()
    response = client.post(
        "/api/batch_bars_v3",
        json={
            "codes": ["7203"],
            "timeframes": ["daily"],
            "limit": 24,
            "includeProvisional": True,
            "includeBoxes": False,
            "asof": "2026-04-12",
        },
    )

    assert response.status_code == 200
    data_version = response.json()["meta"]["data_version"]
    assert data_version == "duckdb-mtime:1700000000.000000"
