from __future__ import annotations

from contextlib import AbstractContextManager
from pathlib import Path

import duckdb
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.backend.api.routers import trades


class _ConnProxy:
    def __init__(self, conn: duckdb.DuckDBPyConnection, recorder: list[str]) -> None:
        self._conn = conn
        self._recorder = recorder

    def execute(self, sql: str, params=None):  # noqa: ANN001 - duckdb signature is dynamic
        self._recorder.append(sql)
        return self._conn.execute(sql, params or [])

    def close(self) -> None:
        self._conn.close()


class _ConnContext(AbstractContextManager[_ConnProxy]):
    def __init__(self, db_path: Path, recorder: list[str]) -> None:
        self._db_path = db_path
        self._recorder = recorder
        self._conn: duckdb.DuckDBPyConnection | None = None

    def __enter__(self) -> _ConnProxy:
        self._conn = duckdb.connect(str(self._db_path), read_only=True)
        return _ConnProxy(self._conn, self._recorder)

    def __exit__(self, exc_type, exc, tb) -> bool:
        if self._conn is not None:
            self._conn.close()
        return False


def _build_client(db_path: Path, recorder: list[str], monkeypatch) -> TestClient:
    app = FastAPI()
    app.include_router(trades.router)
    monkeypatch.setattr(
        trades,
        "try_get_conn",
        lambda timeout_sec=0.4: _ConnContext(db_path, recorder),
    )
    return TestClient(app)


def _seed_positions_db(db_path: Path) -> None:
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            CREATE TABLE tickers (
                code TEXT,
                name TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE positions_live (
                symbol TEXT,
                buy_qty DOUBLE,
                sell_qty DOUBLE,
                opened_at TIMESTAMP,
                updated_at TIMESTAMP,
                has_issue BOOLEAN,
                issue_note TEXT,
                spot_qty DOUBLE,
                margin_long_qty DOUBLE,
                margin_short_qty DOUBLE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE position_rounds (
                round_id TEXT,
                symbol TEXT,
                opened_at TIMESTAMP,
                closed_at TIMESTAMP,
                closed_reason TEXT,
                last_state_sell_buy TEXT,
                has_issue BOOLEAN,
                issue_note TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE trade_events (
                symbol TEXT,
                broker TEXT,
                exec_dt TIMESTAMP,
                action TEXT,
                qty DOUBLE,
                price DOUBLE
            )
            """
        )
        conn.execute("INSERT INTO tickers VALUES ('2413', 'エムスリー')")
        conn.execute(
            """
            INSERT INTO positions_live
            VALUES
                ('1234', 100, 0, TIMESTAMP '2024-01-05 00:00:00', TIMESTAMP '2024-01-05 00:00:00', FALSE, NULL, 100, 0, 0),
                ('2413', 200, 0, TIMESTAMP '2024-02-05 00:00:00', TIMESTAMP '2024-02-05 00:00:00', FALSE, NULL, 200, 0, 0)
            """
        )
        conn.execute(
            """
            INSERT INTO position_rounds
            VALUES
                ('round-a', '2413', TIMESTAMP '2024-01-05 00:00:00', TIMESTAMP '2024-02-01 00:00:00', 'close', 'buy', FALSE, NULL),
                ('round-b', '3681', TIMESTAMP '2024-03-01 00:00:00', TIMESTAMP '2024-04-01 00:00:00', 'close', 'sell', FALSE, NULL)
            """
        )


def test_positions_routes_hide_orphan_live_codes_and_use_joined_names(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "stocks.duckdb"
    _seed_positions_db(db_path)

    held_sqls: list[str] = []
    client = _build_client(db_path, held_sqls, monkeypatch)

    held_response = client.get("/api/positions/held")
    assert held_response.status_code == 200
    held_items = held_response.json()["items"]
    assert [item["symbol"] for item in held_items] == ["2413"]
    assert held_items[0]["name"] == "エムスリー"
    assert len(held_sqls) == 1

    current_sqls: list[str] = []
    client = _build_client(db_path, current_sqls, monkeypatch)
    current_response = client.get("/api/positions/current")
    assert current_response.status_code == 200
    payload = current_response.json()
    assert payload["holding_codes"] == ["2413"]
    assert "1234" not in payload["current_positions_by_code"]
    assert payload["current_positions_by_code"]["2413"]["name"] == "エムスリー"
    assert len(current_sqls) == 2


def test_positions_history_uses_join_in_one_query(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "stocks.duckdb"
    _seed_positions_db(db_path)

    sqls: list[str] = []
    client = _build_client(db_path, sqls, monkeypatch)

    response = client.get("/api/positions/history")
    assert response.status_code == 200
    items = response.json()["items"]
    assert [item["symbol"] for item in items] == ["3681", "2413"]
    assert items[0]["name"] == "3681"
    assert items[1]["name"] == "エムスリー"
    assert len(sqls) == 1
