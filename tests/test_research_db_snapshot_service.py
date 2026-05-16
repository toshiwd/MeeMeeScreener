from __future__ import annotations

from pathlib import Path

import duckdb
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.backend.api.routers import system as system_router
from app.backend.services import research_db_snapshot_service as snapshot_service


def _create_source_db(path: Path) -> None:
    with duckdb.connect(str(path)) as conn:
        conn.execute("CREATE TABLE daily_bars(code TEXT, date INTEGER, close DOUBLE)")
        conn.execute("INSERT INTO daily_bars VALUES ('7203', 20260507, 1825.0)")
        conn.execute("CREATE TABLE feature_snapshot_daily(code TEXT, dt INTEGER)")
        conn.execute("INSERT INTO feature_snapshot_daily VALUES ('7203', 20260507)")


def test_create_research_db_snapshot_copies_live_db_to_tradex_snapshot_dir(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "live" / "stocks.duckdb"
    source_db.parent.mkdir()
    _create_source_db(source_db)
    snapshot_root = tmp_path / "tradex_snapshots"
    monkeypatch.setenv("STOCKS_DB_PATH", str(source_db))
    monkeypatch.setenv("MEEMEE_RESEARCH_DB_SNAPSHOT_DIR", str(snapshot_root))

    result = snapshot_service.create_research_db_snapshot(
        reason="ranking_validation",
        actor="pytest",
    )

    assert result["ok"] is True
    assert result["source_db_path"] == str(source_db.resolve(strict=False))
    assert result["usage"]["live_meemee_db_unchanged"] is True
    assert result["usage"]["automatic_ranking_path"] is False
    assert result["latest_dates"]["daily_bars"] == 20260507
    snapshot_db = Path(result["snapshot_db_path"])
    manifest = Path(result["manifest_path"])
    assert snapshot_db.exists()
    assert manifest.exists()
    with duckdb.connect(str(snapshot_db), read_only=True) as conn:
        assert conn.execute("SELECT code, date, close FROM daily_bars").fetchall() == [
            ("7203", 20260507, 1825.0)
        ]


def test_research_db_snapshot_endpoint_fails_closed_when_live_db_busy(monkeypatch) -> None:
    app = FastAPI()
    app.include_router(system_router.router)
    client = TestClient(app)

    monkeypatch.setattr(
        system_router,
        "create_research_db_snapshot",
        lambda **_kwargs: {
            "ok": False,
            "reason": "live_db_busy",
            "message": "Live DB is busy.",
        },
    )

    response = client.post("/api/system/research-db-snapshot", json={"reason": "test"})

    assert response.status_code == 409
    assert response.json()["detail"]["reason"] == "live_db_busy"
