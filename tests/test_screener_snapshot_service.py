from __future__ import annotations

from datetime import date

import pytest

from app.backend.services import screener_snapshot_service as service


def test_refresh_persists_snapshot_and_reuses_it(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = tmp_path / "stocks.duckdb"
    service.invalidate_screener_snapshot_cache()
    calls = {"count": 0}

    def _fake_compute(limit: int, screener_repo, stock_repo):
        calls["count"] += 1
        return [
            {"code": "1001", "name": "A", "asOf": "2026-03-13"},
            {"code": "1002", "name": "B", "asOf": "2026-03-12"},
        ]

    monkeypatch.setattr(service, "_compute_snapshot_items", _fake_compute)

    refreshed = service.refresh_screener_snapshot(limit=260, source="test", db_path=str(db_path))
    assert refreshed["stale"] is False
    assert refreshed["buildFailed"] is False
    assert refreshed["asOf"] == "2026-03-13"
    assert len(refreshed["items"]) == 2
    assert calls["count"] == 1

    payload = service.get_screener_snapshot_response(limit=260, db_path=str(db_path))
    assert payload["stale"] is False
    assert payload["items"][0]["code"] == "1001"
    assert calls["count"] == 1


def test_refresh_returns_stale_snapshot_when_rebuild_fails(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = tmp_path / "stocks.duckdb"
    service.invalidate_screener_snapshot_cache()

    monkeypatch.setattr(
        service,
        "_compute_snapshot_items",
        lambda limit, screener_repo, stock_repo: [{"code": "1306", "name": "ETF", "asOf": "2026-03-13"}],
    )
    service.refresh_screener_snapshot(limit=260, source="seed", db_path=str(db_path))
    service.invalidate_screener_snapshot_cache()

    def _raise(*_args, **_kwargs):
        raise RuntimeError("forced_snapshot_failure")

    monkeypatch.setattr(service, "_compute_snapshot_items", _raise)

    payload = service.refresh_screener_snapshot(limit=260, source="retry", db_path=str(db_path))
    assert payload["stale"] is True
    assert payload["buildFailed"] is True
    assert payload["lastError"] == "forced_snapshot_failure"
    assert payload["items"][0]["code"] == "1306"


def test_refresh_without_prior_snapshot_raises(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = tmp_path / "stocks.duckdb"
    service.invalidate_screener_snapshot_cache()

    def _raise(*_args, **_kwargs):
        raise RuntimeError("no_seed_snapshot")

    monkeypatch.setattr(service, "_compute_snapshot_items", _raise)

    with pytest.raises(RuntimeError, match="no_seed_snapshot"):
        service.refresh_screener_snapshot(limit=260, source="first", db_path=str(db_path))


def test_refresh_serializes_date_fields(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = tmp_path / "stocks.duckdb"
    service.invalidate_screener_snapshot_cache()

    monkeypatch.setattr(
        service,
        "_compute_snapshot_items",
        lambda limit, screener_repo, stock_repo: [
            {"code": "9432", "name": "NTT", "eventEarningsDate": date(2026, 3, 31), "asOf": "2026-03-13"}
        ],
    )

    payload = service.refresh_screener_snapshot(limit=260, source="date-test", db_path=str(db_path))
    assert payload["items"][0]["eventEarningsDate"] == "2026-03-31"


def test_get_response_reuses_persisted_stale_metadata(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = tmp_path / "stocks.duckdb"
    service.invalidate_screener_snapshot_cache()

    monkeypatch.setattr(
        service,
        "_compute_snapshot_items",
        lambda limit, screener_repo, stock_repo: [{"code": "7203", "name": "Toyota", "asOf": "2026-03-13"}],
    )
    seeded = service.refresh_screener_snapshot(limit=260, source="seed", db_path=str(db_path))
    assert seeded["stale"] is False
    service.invalidate_screener_snapshot_cache()

    def _raise(*_args, **_kwargs):
        raise RuntimeError("refresh_retry_failed")

    monkeypatch.setattr(service, "_compute_snapshot_items", _raise)
    stale_payload = service.refresh_screener_snapshot(limit=260, source="retry", db_path=str(db_path))
    assert stale_payload["stale"] is True
    assert stale_payload["lastError"] == "refresh_retry_failed"

    service.invalidate_screener_snapshot_cache()
    cached = service.get_screener_snapshot_response(limit=260, db_path=str(db_path))
    assert cached["items"][0]["code"] == "7203"
    assert cached["stale"] is True
    assert cached["lastError"] == "refresh_retry_failed"
    assert cached["updatedAt"] is not None
    assert cached["generation"] == seeded["generation"]
