from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd

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
        if sql.startswith("CREATE TABLE IF NOT EXISTS") or sql.startswith("ALTER TABLE"):
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
