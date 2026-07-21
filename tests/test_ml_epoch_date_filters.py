from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd
import duckdb

from app.backend.services.ml import ml_service


def _epoch(date_key: int) -> int:
    return ml_service._yyyymmdd_to_utc_epoch(date_key)  # type: ignore[attr-defined]


class _Rows:
    def __init__(self, rows):
        self._rows = list(rows)

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def df(self):
        columns = ["dt", "code"]
        return pd.DataFrame(self._rows, columns=columns[: len(self._rows[0])] if self._rows else columns)


class _LabelConn:
    def __init__(self, daily_rows):
        self.daily_rows = list(daily_rows)
        self.delete_sql = ""
        self.delete_params = []
        self.inserted = []

    def execute(self, query: str, params=None):
        sql = " ".join(str(query).split())
        args = list(params or [])
        if sql.startswith("CREATE TABLE IF NOT EXISTS") or sql.startswith("ALTER TABLE") or sql.startswith("DROP VIEW"):
            return _Rows([])
        if sql.startswith("SELECT 1 FROM information_schema.tables") or sql.startswith("SELECT COUNT(*) FROM information_schema.tables"):
            return _Rows([(1,)])
        if sql.startswith("PRAGMA table_info('daily_bars')"):
            return _Rows([(0, "code"), (1, "date"), (2, "o"), (3, "h"), (4, "l"), (5, "c"), (6, "v")])
        if "SELECT code, date, h, l, c FROM daily_bars" in sql:
            return _Rows(self.daily_rows)
        if sql.startswith("DELETE FROM ml_label_20d WHERE"):
            self.delete_sql = sql
            self.delete_params = args
            return _Rows([])
        raise AssertionError(f"Unexpected query: {sql} params={args}")

    def executemany(self, query: str, rows) -> None:
        self.inserted.extend(list(rows))


class _TrainingConn:
    def __init__(self):
        self.sql = ""
        self.params = []

    def execute(self, query: str, params=None):
        self.sql = " ".join(str(query).split())
        self.params = list(params or [])
        return _Rows([(_epoch(20260312), "1306")])


def test_refresh_ml_label_table_uses_yyyymmdd_bounds_for_epoch_daily_bars() -> None:
    start = datetime(2026, 3, 1, tzinfo=timezone.utc)
    daily_rows = []
    price = 100.0
    for idx in range(50):
        dt = start + timedelta(days=idx)
        close = price + idx
        daily_rows.append(("1306", int(dt.timestamp()), close + 1, close - 1, close))

    conn = _LabelConn(daily_rows)
    inserted = ml_service.refresh_ml_label_table(
        conn,
        cfg=ml_service.load_ml_config(),
        start_dt=20260310,
        end_dt=20260312,
    )

    normalized = [ml_service._normalize_daily_dt_key(row[0]) for row in conn.inserted]  # type: ignore[attr-defined]

    assert inserted == 3
    assert normalized == [20260310, 20260311, 20260312]
    assert "CASE WHEN dt >= 1000000000" in conn.delete_sql
    assert conn.delete_params == [20260310, 20260312]


def test_load_training_df_uses_yyyymmdd_bounds_for_epoch_feature_dates() -> None:
    conn = _TrainingConn()

    df = ml_service._load_training_df(conn, start_dt=20260312, end_dt=20260312)  # type: ignore[attr-defined]

    assert df["dt"].tolist() == [_epoch(20260312)]
    assert "CASE WHEN f.dt >= 1000000000" in conn.sql
    assert conn.params == [20260312, 20260312]


def test_refresh_ml_labels_incremental_advances_from_latest_label() -> None:
    conn = duckdb.connect(":memory:")
    ml_service._ensure_ml_schema(conn)  # type: ignore[attr-defined]
    conn.execute("CREATE TABLE daily_bars (code VARCHAR, date INTEGER, h DOUBLE, l DOUBLE, c DOUBLE)")
    dates = [_epoch(20260101) + i * 86400 for i in range(45)]
    conn.executemany(
        "INSERT INTO daily_bars VALUES ('1001', ?, ?, ?, ?)",
        [(dt, 101.0 + i, 99.0 + i, 100.0 + i) for i, dt in enumerate(dates)],
    )

    first = ml_service.refresh_ml_labels_incremental(conn)
    assert first["updated"] is True
    assert first["rows"] == 25

    conn.executemany(
        "INSERT INTO daily_bars VALUES ('1001', ?, ?, ?, ?)",
        [(dates[-1] + (i + 1) * 86400, 146.0 + i, 144.0 + i, 145.0 + i) for i in range(5)],
    )
    second = ml_service.refresh_ml_labels_incremental(conn)
    assert second["updated"] is True
    assert second["latest_label_dt"] > first["latest_label_dt"]


def test_refresh_ml_features_incremental_skips_when_current(monkeypatch) -> None:
    class _Conn:
        def execute(self, sql):
            if "feature_snapshot_daily" in sql:
                return _Rows([(_epoch(20260619),)])
            if "ml_feature_daily" in sql:
                return _Rows([(_epoch(20260619),)])
            raise AssertionError(sql)

    monkeypatch.setattr(ml_service, "ensure_ml_runtime_schema", lambda conn, legacy_schema_enabled: None)
    result = ml_service.refresh_ml_features_incremental(_Conn())
    assert result == {
        "updated": False,
        "rows": 0,
        "previous_feature_dt": 20260619,
        "latest_feature_dt": 20260619,
    }


def test_refresh_ml_predictions_incremental_advances_to_latest_feature(monkeypatch) -> None:
    class _Conn:
        def __init__(self):
            self.replaced_dates = []

        def execute(self, sql, params=None):
            compact = " ".join(str(sql).split())
            if compact.startswith("SELECT MAX(dt) FROM ml_feature_daily WHERE"):
                assert params == [20260619]
                return _Rows([(_epoch(20260619),)])
            if compact.startswith("SELECT MAX(dt) FROM ml_feature_daily"):
                return _Rows([(_epoch(20260619),)])
            if compact.startswith("SELECT MAX(dt) FROM ml_pred_20d"):
                if self.replaced_dates:
                    return _Rows([(_epoch(20260619),)])
                return _Rows([(_epoch(20260610),)])
            raise AssertionError(compact)

    conn = _Conn()
    pred_frame = pd.DataFrame({"dt": [_epoch(20260619)], "code": ["1001"]})

    monkeypatch.setattr(ml_service, "ensure_ml_runtime_schema", lambda conn, legacy_schema_enabled: None)
    monkeypatch.setattr(ml_service, "_load_prediction_feature_frame", lambda conn, dates: pred_frame)
    monkeypatch.setattr(ml_service, "_load_models_from_registry", lambda conn: ("models", "model-v1", 123))
    monkeypatch.setattr(ml_service, "_predict_frame", lambda frame, models, cfg: frame)
    monkeypatch.setattr(ml_service, "_build_ml_pred_rows", lambda pred, model_version, n_train: [("row",)])

    def _replace(_conn, dates, rows):
        _conn.replaced_dates.extend(dates)

    monkeypatch.setattr(ml_service, "_replace_ml_predictions_for_dates", _replace)

    result = ml_service.refresh_ml_predictions_incremental(conn)

    assert conn.replaced_dates == [_epoch(20260619)]
    assert result == {
        "updated": True,
        "rows": 1,
        "previous_prediction_dt": 20260610,
        "latest_prediction_dt": 20260619,
        "model_version": "model-v1",
        "skipped_reason": None,
    }


def test_train_models_wrapper_uses_current_impl(monkeypatch) -> None:
    captured = {}

    def _fake_impl(**kwargs):
        captured.update(kwargs)
        return {"ok": True}

    monkeypatch.setattr(ml_service, "_train_models_impl", _fake_impl)

    result = ml_service.train_models(start_dt=20010910, end_dt=20260313, dry_run=False)

    assert result == {"ok": True}
    assert captured == {
        "start_dt": 20010910,
        "end_dt": 20260313,
        "dry_run": False,
        "progress_cb": None,
    }
