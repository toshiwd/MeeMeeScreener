from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient
import duckdb

from app.db.schema import ensure_schema


def _table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema = 'main' AND table_name = ?",
        [table_name],
    ).fetchone()
    return bool(row)


def _build_app() -> FastAPI:
    from app.backend.api.events_routes import router as events_router
    from app.backend.api.routers.ticker import router as ticker_router

    app = FastAPI()
    app.include_router(events_router)
    app.include_router(ticker_router)
    return app


def test_ensure_schema_bootstraps_events_and_tdnet_tables() -> None:
    conn = duckdb.connect(":memory:")
    try:
        ensure_schema(conn)

        assert _table_exists(conn, "events_meta")
        assert _table_exists(conn, "events_refresh_jobs")
        assert _table_exists(conn, "tdnet_disclosures")
        assert _table_exists(conn, "tdnet_disclosure_features")
    finally:
        conn.close()


def test_empty_events_and_tdnet_routes_do_not_500(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "stocks.duckdb"
    monkeypatch.setenv("STOCKS_DB_PATH", str(db_path))

    from app.backend.api.routers import ticker as ticker_router

    monkeypatch.setattr(ticker_router, "_TDNET_REPO", None, raising=False)

    with TestClient(_build_app()) as client:
        events_response = client.get("/api/events/meta")
        assert events_response.status_code == 200
        events_payload = events_response.json()
        assert events_payload["is_refreshing"] is False
        assert "data_coverage" in events_payload

        tdnet_response = client.get("/api/ticker/tdnet/disclosures", params={"code": "7203"})
        assert tdnet_response.status_code == 200
        assert tdnet_response.json() == {"items": []}
