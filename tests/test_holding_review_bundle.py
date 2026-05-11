from __future__ import annotations

from contextlib import AbstractContextManager
from pathlib import Path

import duckdb
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.backend.api.routers import trades
from app.backend.services import holding_review as holding_review_service


class _ConnProxy:
    def __init__(self, conn: duckdb.DuckDBPyConnection, recorder: list[str]) -> None:
        self._conn = conn
        self._recorder = recorder

    def execute(self, sql: str, params=None):  # noqa: ANN001
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


def _client(db_path: Path, recorder: list[str], monkeypatch) -> TestClient:
    app = FastAPI()
    app.include_router(trades.router)
    monkeypatch.setattr(trades, "try_get_conn", lambda timeout_sec=0.4: _ConnContext(db_path, recorder))
    return TestClient(app)


def _seed_review_db(db_path: Path) -> None:
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            CREATE TABLE positions_live (
                symbol TEXT,
                buy_qty DOUBLE,
                sell_qty DOUBLE
            )
            """
        )
        conn.execute("INSERT INTO positions_live VALUES ('2531', 1000, 300)")
        conn.execute(
            """
            CREATE TABLE trade_events (
                id INTEGER,
                broker TEXT,
                exec_dt TIMESTAMP,
                symbol TEXT,
                action TEXT,
                qty DOUBLE,
                price DOUBLE,
                source_row_hash TEXT,
                created_at TIMESTAMP,
                transaction_type TEXT,
                side_type TEXT,
                margin_type TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO trade_events VALUES
                (1, 'sbi', TIMESTAMP '2026-05-07 09:00:00', '2531', 'SPOT_BUY', 1000, 1836.1, 'a', TIMESTAMP '2026-05-07', 'buy', 'long', NULL),
                (2, 'sbi', TIMESTAMP '2026-04-24 09:00:00', '2531', 'MARGIN_OPEN_SHORT', 300, 1780.0, 'b', TIMESTAMP '2026-04-24', 'sell', 'short', NULL)
            """
        )
        conn.execute(
            """
            CREATE TABLE daily_bars (
                code TEXT,
                date INTEGER,
                o DOUBLE,
                h DOUBLE,
                l DOUBLE,
                c DOUBLE,
                v DOUBLE,
                source TEXT
            )
            """
        )
        conn.execute("INSERT INTO daily_bars VALUES ('2531', 20260508, 1834, 1849.5, 1780, 1810, 679, 'pan')")
        conn.execute(
            """
            CREATE TABLE feature_snapshot_daily (
                code TEXT,
                date INTEGER,
                ma7 DOUBLE,
                ma20 DOUBLE,
                ma60 DOUBLE
            )
            """
        )
        conn.execute("INSERT INTO feature_snapshot_daily VALUES ('2531', 20260508, 1782.0, 1791.875, 1649.49)")
        conn.execute(
            """
            CREATE TABLE ranking_appearances (
                code TEXT,
                date INTEGER,
                rank INTEGER,
                tone TEXT
            )
            """
        )
        conn.execute("INSERT INTO ranking_appearances VALUES ('2531', 20260507, 13, 'up')")
        conn.execute(
            """
            CREATE TABLE signal_decisions (
                code TEXT,
                date INTEGER,
                buy_entry_qualified BOOLEAN,
                sell_entry_qualified BOOLEAN
            )
            """
        )
        conn.execute("INSERT INTO signal_decisions VALUES ('2531', 20260508, FALSE, FALSE)")
        conn.execute(
            """
            CREATE TABLE events_meta (
                code TEXT,
                event_type TEXT,
                event_date INTEGER
            )
            """
        )
        conn.execute("INSERT INTO events_meta VALUES ('2531', 'earnings', 20260513)")


def test_holding_review_bundle_aggregates_read_only_payload(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "stocks.duckdb"
    _seed_review_db(db_path)
    monkeypatch.setattr(
        holding_review_service.yahoo_provisional,
        "get_provisional_daily_rows_from_spark",
        lambda codes, **kwargs: {},
    )
    monkeypatch.setattr(
        holding_review_service.yahoo_provisional,
        "get_provisional_daily_row_from_chart",
        lambda code: (20260511, 1799.0, 1831.5, 1780.0, 1819.5, 131.0),
    )

    sqls: list[str] = []
    response = _client(db_path, sqls, monkeypatch).get("/api/positions/holding-review")

    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == "meemee_holding_review_bundle_v1"
    item = payload["items"][0]
    assert item["code"] == "2531"
    assert item["as_of"] == {
        "confirmed_date": "2026-05-08",
        "provisional_date": "2026-05-11",
        "provisional_source": "yahoo",
        "confirmed_freshness_status": "fresh",
    }
    assert item["position"]["long_qty"] == 1000
    assert item["position"]["short_qty"] == 300
    assert item["position"]["hedge_ratio"] == 0.3
    assert item["entry_reason_snapshot"]["latest_alive_rank"] == 13
    assert item["current_hold_reason"]["reason_alive"] is True
    assert item["current_hold_reason"]["chart_structure_alive"] is True
    assert item["current_hold_reason"]["deterioration_reasons"] == [
        "latest_signal_reject",
        "earnings_nearby",
        "unrealized_loss",
    ]
    assert item["confirmed_bar"]["ma20"] == 1791.875
    assert item["provisional_bar"]["above_ma20"] is True
    assert item["chart_context"]["chart_structure_state"] == "structurally_alive"
    assert item["chart_context"]["daily_6m_context"]["above_ma_count"] == 3
    assert item["event_gate"]["event_risk_level"] == "high"
    assert item["decision"]["action"] == "reduce"
    assert item["decision"]["position_proposal"] == {"from": "short300-long1000", "to": "short300-long500~700"}
    assert item["data_quality"]["provisional_is_confirmed"] is False
    assert item["data_quality"]["missing_fields"] == []
    assert not any(sql.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER")) for sql in sqls)


def test_holding_review_by_code_degrades_when_optional_inputs_missing(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "stocks.duckdb"
    with duckdb.connect(str(db_path)) as conn:
        conn.execute("CREATE TABLE positions_live (symbol TEXT, buy_qty DOUBLE, sell_qty DOUBLE)")
        conn.execute("INSERT INTO positions_live VALUES ('2531', 100, 0)")
    monkeypatch.setattr(
        holding_review_service.yahoo_provisional,
        "get_provisional_daily_rows_from_spark",
        lambda codes, **kwargs: {},
    )
    monkeypatch.setattr(holding_review_service.yahoo_provisional, "get_provisional_daily_row_from_chart", lambda code: None)

    response = _client(db_path, [], monkeypatch).get("/api/positions/holding-review/2531")

    assert response.status_code == 200
    item = response.json()["items"][0]
    assert item["confirmed_bar"] is None
    assert item["provisional_bar"] is None
    assert item["decision"]["action"] == "hold"
    assert "daily_bars" in item["data_quality"]["missing_fields"]
    assert "trade_events" in item["data_quality"]["missing_fields"]
    assert item["data_quality"]["provisional_is_confirmed"] is False


def test_structural_hold_guard_prevents_reduce_for_small_low_risk_loss() -> None:
    decision = holding_review_service._decision(
        {
            "reason_alive": True,
            "chart_structure_alive": True,
            "deterioration_reasons": [
                "latest_signal_reject",
                "not_in_latest_top_ranking",
                "unrealized_loss",
            ],
        },
        {
            "long_qty": 300.0,
            "short_qty": 100.0,
            "avg_long_price": 2814.0,
            "avg_short_price": 2814.0,
            "unrealized_pnl_using_provisional": -2800.0,
        },
        {"event_risk_level": "low"},
        {
            "short_term_price_state": "firm_intraday",
            "chart_structure_state": "structurally_alive",
            "daily_6m_context": {
                "move_state": "uptrend_continuation",
                "provisional_above_ma": {"ma20": True, "ma60": True},
            },
        },
    )

    assert decision["action"] == "hold"
    assert decision["confidence"] == "medium"
    assert decision["position_proposal"] == {"from": "short100-long300", "to": "short100-long300"}
    assert decision["structural_hold_guard"] is True
    assert "structural_hold_guard_small_loss" in decision["decision_reasons"]


def test_structural_hold_guard_does_not_override_high_event_risk() -> None:
    decision = holding_review_service._decision(
        {
            "reason_alive": True,
            "chart_structure_alive": True,
            "deterioration_reasons": ["earnings_nearby", "unrealized_loss"],
        },
        {
            "long_qty": 1000.0,
            "short_qty": 300.0,
            "avg_long_price": 1836.1,
            "avg_short_price": 1780.0,
            "unrealized_pnl_using_provisional": -36500.0,
        },
        {"event_risk_level": "high"},
        {
            "short_term_price_state": "firm_intraday",
            "chart_structure_state": "structurally_alive",
            "daily_6m_context": {
                "move_state": "high_zone_failure",
                "provisional_above_ma": {"ma20": True, "ma60": True},
            },
        },
    )

    assert decision["action"] == "reduce"
    assert decision["structural_hold_guard"] is False
