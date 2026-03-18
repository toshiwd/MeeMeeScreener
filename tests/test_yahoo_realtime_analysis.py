from __future__ import annotations

import contextlib
import os
import sys
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

import duckdb
import pandas as pd

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.backend.core import yahoo_daily_ingest_runtime
from app.backend.api.routers import ticker
from app.backend.services.data import yahoo_daily_ingest
from app.backend.services.data import yahoo_provisional
from app.backend.services.ml import ml_service


class _FakeRows:
    def __init__(self, rows):
        self._rows = list(rows)

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _PredictBulkConn:
    def __init__(self):
        self.refreshed = False

    def execute(self, query: str, params=None):
        sql = " ".join(str(query).split())
        args = list(params or [])
        if "SELECT COUNT(*) FROM ml_feature_daily" in sql:
            return _FakeRows([(5,)])
        if "SELECT DISTINCT CASE WHEN dt >= 1000000000" in sql and "FROM ml_feature_daily" in sql and "IN (" in sql:
            return _FakeRows([(20260311,)])
        if "SELECT DISTINCT dt, CASE WHEN dt >= 1000000000" in sql and "ORDER BY dt_key, dt" in sql:
            assert args == [20260312]
            if not self.refreshed:
                return _FakeRows([(20260311, 20260311)])
            return _FakeRows([(20260311, 20260311), (20260312, 20260312)])
        raise AssertionError(f"Unexpected query: {sql} params={args}")


@contextlib.contextmanager
def _fake_conn_ctx(conn):
    yield conn


def test_predict_for_dates_bulk_refreshes_features_for_missing_requested_date() -> None:
    conn = _PredictBulkConn()
    replaced = {}

    def _mark_refresh(*args, **kwargs):
        conn.refreshed = True
        return 1

    def _replace_rows(_conn, target_dates, rows):
        replaced["target_dates"] = list(target_dates)
        replaced["rows"] = list(rows)

    with (
        patch("app.backend.services.ml.ml_service.get_conn", return_value=_fake_conn_ctx(conn)),
        patch("app.backend.services.ml.ml_service._ensure_ml_schema"),
        patch("app.backend.services.ml.ml_service.load_ml_config", return_value={}),
        patch("app.backend.services.ml.ml_service.refresh_ml_feature_table", side_effect=_mark_refresh) as mock_refresh,
        patch("app.backend.services.ml.ml_service._load_models_from_registry", return_value=({}, "model-v1", 42)),
        patch(
            "app.backend.services.ml.ml_service._load_prediction_feature_frame",
            return_value=pd.DataFrame({"dt": [20260312], "code": ["1605"]}),
        ),
        patch(
            "app.backend.services.ml.ml_service._predict_frame",
            return_value=pd.DataFrame({"dt": [20260312], "code": ["1605"]}),
        ),
        patch(
            "app.backend.services.ml.ml_service._build_ml_pred_rows",
            return_value=[("1605", 20260312, 0.6)],
        ),
        patch("app.backend.services.ml.ml_service._replace_ml_predictions_for_dates", side_effect=_replace_rows),
    ):
        result = ml_service.predict_for_dates_bulk(dates=[20260312], chunk_size_days=10)

    mock_refresh.assert_called_once()
    assert result["resolved_dates"] == [20260312]
    assert result["predicted_dates"] == [20260312]
    assert replaced["target_dates"] == [20260312]


class _PredictBulkEpochConn:
    def __init__(self):
        self.refreshed = False

    def execute(self, query: str, params=None):
        sql = " ".join(str(query).split())
        args = list(params or [])
        if "SELECT COUNT(*) FROM ml_feature_daily" in sql:
            return _FakeRows([(5,)])
        if "SELECT MAX(dt) FROM ml_feature_daily" in sql:
            return _FakeRows([(1773187200,)])
        if "SELECT DISTINCT CASE WHEN dt >= 1000000000" in sql and "WHERE CASE WHEN dt >= 1000000000" in sql and "IN (" in sql:
            return _FakeRows([(20260311,)])
        if "SELECT DISTINCT dt, CASE WHEN dt >= 1000000000" in sql and "ORDER BY dt_key, dt" in sql:
            assert args == [20260312]
            if not self.refreshed:
                return _FakeRows([(1773187200, 20260311)])
            return _FakeRows([(1773187200, 20260311), (1773273600, 20260312)])
        raise AssertionError(f"Unexpected query: {sql} params={args}")


def test_predict_for_dates_bulk_resolves_epoch_feature_dates_from_yyyymmdd_requests() -> None:
    conn = _PredictBulkEpochConn()
    replaced = {}

    def _mark_refresh(*args, **kwargs):
        conn.refreshed = True
        return 1

    def _replace_rows(_conn, target_dates, rows):
        replaced["target_dates"] = list(target_dates)
        replaced["rows"] = list(rows)

    with (
        patch("app.backend.services.ml.ml_service.get_conn", return_value=_fake_conn_ctx(conn)),
        patch("app.backend.services.ml.ml_service._ensure_ml_schema"),
        patch("app.backend.services.ml.ml_service.load_ml_config", return_value={}),
        patch("app.backend.services.ml.ml_service.refresh_ml_feature_table", side_effect=_mark_refresh) as mock_refresh,
        patch("app.backend.services.ml.ml_service._load_models_from_registry", return_value=({}, "model-v1", 42)),
        patch(
            "app.backend.services.ml.ml_service._load_prediction_feature_frame",
            return_value=pd.DataFrame({"dt": [1773273600], "code": ["2413"]}),
        ),
        patch(
            "app.backend.services.ml.ml_service._predict_frame",
            return_value=pd.DataFrame({"dt": [1773273600], "code": ["2413"]}),
        ),
        patch(
            "app.backend.services.ml.ml_service._build_ml_pred_rows",
            return_value=[(1773273600, "2413", 0.6)],
        ),
        patch("app.backend.services.ml.ml_service._replace_ml_predictions_for_dates", side_effect=_replace_rows),
    ):
        result = ml_service.predict_for_dates_bulk(dates=[20260312], chunk_size_days=10)

    mock_refresh.assert_called_once()
    assert result["requested_dates"] == [20260312]
    assert result["resolved_dates"] == [1773273600]
    assert result["predicted_dates"] == [1773273600]
    assert replaced["target_dates"] == [1773273600]


def test_yahoo_daily_ingest_triggers_analysis_prewarm_after_insert() -> None:
    with (
        patch(
            "app.backend.core.yahoo_daily_ingest_runtime.ingest_latest_provisional_daily_rows",
            return_value={
                "inserted": 3,
                "target_codes": 10,
                "coverage": {"covered_codes": 7, "target_date": 20260312},
            },
        ),
        patch("app.backend.core.yahoo_daily_ingest_runtime.job_manager._update_db"),
        patch("app.backend.core.yahoo_daily_ingest_runtime.schedule_analysis_prewarm_if_needed") as mock_prewarm,
        patch("app.backend.core.yahoo_daily_ingest_runtime.schedule_external_analysis_publish_latest") as mock_external_publish,
        patch("app.backend.core.yahoo_daily_ingest_runtime.schedule_screener_snapshot_refresh"),
        patch("app.backend.services.rankings_cache.refresh_cache"),
        patch(
            "app.backend.core.yahoo_daily_ingest_runtime.jst_now",
            return_value=datetime(2026, 3, 12, 12, 27, tzinfo=timezone(timedelta(hours=9))),
        ),
    ):
        yahoo_daily_ingest_runtime.handle_yf_daily_ingest("job-123", {})

    mock_prewarm.assert_called_once_with(source="yf_daily_ingest:job-123")
    mock_external_publish.assert_called_once_with(
        source="yf_daily_ingest:job-123",
        as_of=20260312,
    )


def test_yahoo_daily_ingest_triggers_refreshes_after_same_day_update() -> None:
    with (
        patch(
            "app.backend.core.yahoo_daily_ingest_runtime.ingest_latest_provisional_daily_rows",
            return_value={
                "inserted": 0,
                "updated": 2,
                "target_codes": 10,
                "coverage": {"covered_codes": 7, "target_date": 20260312},
            },
        ),
        patch("app.backend.core.yahoo_daily_ingest_runtime.job_manager._update_db"),
        patch("app.backend.core.yahoo_daily_ingest_runtime.schedule_analysis_prewarm_if_needed") as mock_prewarm,
        patch("app.backend.core.yahoo_daily_ingest_runtime.schedule_external_analysis_publish_latest") as mock_external_publish,
        patch("app.backend.core.yahoo_daily_ingest_runtime.schedule_screener_snapshot_refresh"),
        patch("app.backend.services.rankings_cache.refresh_cache"),
        patch(
            "app.backend.core.yahoo_daily_ingest_runtime.jst_now",
            return_value=datetime(2026, 3, 12, 12, 27, tzinfo=timezone(timedelta(hours=9))),
        ),
    ):
        yahoo_daily_ingest_runtime.handle_yf_daily_ingest("job-456", {})

    mock_prewarm.assert_called_once_with(source="yf_daily_ingest:job-456")
    mock_external_publish.assert_called_once_with(
        source="yf_daily_ingest:job-456",
        as_of=20260312,
    )


def test_insert_rows_updates_same_day_yahoo_without_overwriting_pan() -> None:
    conn = duckdb.connect(":memory:")
    try:
        conn.execute(
            """
            CREATE TABLE daily_bars (
                code TEXT,
                date BIGINT,
                o DOUBLE,
                h DOUBLE,
                l DOUBLE,
                c DOUBLE,
                v BIGINT,
                source TEXT,
                PRIMARY KEY(code, date)
            )
            """
        )
        same_day = 1773273600
        conn.execute(
            """
            INSERT INTO daily_bars VALUES
            ('1301', ?, 100, 101, 99, 100, 1000, 'yahoo'),
            ('1302', ?, 200, 201, 199, 200, 2000, 'pan')
            """,
            [same_day, same_day],
        )

        inserted, updated, conflicts, cleaned_stale = yahoo_daily_ingest._insert_rows(
            conn,
            [
                ("1301", same_day, 110.0, 112.0, 109.0, 111.0, 1500.0),
                ("1302", same_day, 210.0, 212.0, 209.0, 211.0, 2500.0),
            ],
        )

        assert inserted == 0
        assert updated == 1
        assert conflicts == 1
        assert cleaned_stale == 0
        assert conn.execute(
            "SELECT o, h, l, c, v, source FROM daily_bars WHERE code = '1301'"
        ).fetchone() == (110.0, 112.0, 109.0, 111.0, 1500, "yahoo")
        assert conn.execute(
            "SELECT o, h, l, c, v, source FROM daily_bars WHERE code = '1302'"
        ).fetchone() == (200.0, 201.0, 199.0, 200.0, 2000, "pan")
    finally:
        conn.close()


def test_market_data_status_message_includes_latest_fetch_time() -> None:
    session = SimpleNamespace(day_type="full_day")

    message = ticker._build_market_data_status_message(
        has_provisional=True,
        pan_delayed=False,
        delayed_pending_date=None,
        pending_yahoo_date=20260312,
        session=session,
        provisional_fetched_at_text="2026-03-12 12:27 JST",
    )

    assert message is not None
    assert "最終取得 2026-03-12 12:27 JST" in message
    assert "[2026-03-12]" in message


def test_intraday_refresh_helper_respects_session_window_and_interval(monkeypatch) -> None:
    monkeypatch.setenv("MEEMEE_YF_DAILY_INGEST_INTRADAY_ENABLED", "1")
    monkeypatch.setenv("MEEMEE_YF_DAILY_INGEST_INTRADAY_INTERVAL_SEC", "900")
    session = SimpleNamespace(is_trading_day=True, day_type="full_day", close_time_jst="15:30")
    jst = timezone(timedelta(hours=9))
    first = datetime(2026, 3, 12, 10, 0, tzinfo=jst)
    too_soon = datetime(2026, 3, 12, 10, 10, tzinfo=jst)
    enough_gap = datetime(2026, 3, 12, 10, 20, tzinfo=jst)
    lunch = datetime(2026, 3, 12, 12, 10, tzinfo=jst)

    assert yahoo_daily_ingest_runtime._should_submit_intraday_refresh(
        now=first,
        session=session,
        last_submitted_at=None,
    )
    assert not yahoo_daily_ingest_runtime._should_submit_intraday_refresh(
        now=too_soon,
        session=session,
        last_submitted_at=first,
    )
    assert yahoo_daily_ingest_runtime._should_submit_intraday_refresh(
        now=enough_gap,
        session=session,
        last_submitted_at=first,
    )
    assert not yahoo_daily_ingest_runtime._should_submit_intraday_refresh(
        now=lunch,
        session=session,
        last_submitted_at=None,
    )


def test_get_provisional_daily_rows_from_spark_falls_back_to_chart_for_missing_rows() -> None:
    with (
        patch("app.backend.services.data.yahoo_provisional._enabled", return_value=True),
        patch(
            "app.backend.services.data.yahoo_provisional._fetch_spark_chunk",
            return_value={"2413.T": None},
        ),
        patch(
            "app.backend.services.data.yahoo_provisional._get_provisional_daily_rows_from_chart_symbols",
            return_value={"2413": (1773273600, 1627.0, 1665.0, 1624.0, 1639.0, 2312.0)},
        ) as mock_chart,
        patch.dict("app.backend.services.data.yahoo_provisional._spark_cache", {}, clear=True),
        patch.dict("app.backend.services.data.yahoo_provisional._chart_cache", {}, clear=True),
    ):
        rows = yahoo_provisional.get_provisional_daily_rows_from_spark(["2413"])

    mock_chart.assert_called_once()
    assert rows["2413"][0] == 1773273600
