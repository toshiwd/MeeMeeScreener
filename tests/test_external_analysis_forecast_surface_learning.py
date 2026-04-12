from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import duckdb

from external_analysis.models import forecast_surface_learning as fsl


def _weekday_dates(start: date, count: int) -> list[int]:
    values: list[int] = []
    current = start
    while len(values) < count:
        if current.weekday() < 5:
            values.append(int(current.strftime("%Y%m%d")))
        current += timedelta(days=1)
    return values


def _epoch_seconds(as_of_date: int) -> int:
    parsed = datetime.strptime(str(as_of_date), "%Y%m%d").replace(tzinfo=timezone.utc)
    return int(parsed.timestamp())


def _seed_source_db(path: str, dates: list[int]) -> None:
    conn = duckdb.connect(path)
    try:
        conn.execute(
            """
            CREATE TABLE ml_feature_daily (
                dt BIGINT,
                code TEXT,
                feature_version TEXT,
                computed_at TIMESTAMP,
                feature_a DOUBLE,
                feature_b DOUBLE
            )
            """
        )
        for idx, as_of_date in enumerate(dates):
            for code, shift in (("1301", 1.0), ("1302", 0.0), ("1303", -1.0)):
                conn.execute(
                    "INSERT INTO ml_feature_daily VALUES (?, ?, ?, ?, ?, ?)",
                    [
                        _epoch_seconds(as_of_date),
                        code,
                        "v1",
                        datetime(2026, 1, 1, tzinfo=timezone.utc),
                        float(idx) * shift,
                        float(idx % 5) - shift,
                    ],
                )
        conn.execute(
            """
            CREATE VIEW feature_frame_daily AS
            SELECT
                dt,
                code,
                feature_version,
                feature_version AS feature_frame_version,
                computed_at,
                CAST(strftime(to_timestamp(dt), '%Y-%m-%d 00:00:00') AS TIMESTAMP) AS available_at,
                '{"feature_frame_daily":"present","tdnet_disclosures":"source_absent"}' AS source_presence_flag,
                feature_a,
                feature_b
            FROM ml_feature_daily
            """
        )
    finally:
        conn.close()


def _seed_label_db(path: str, dates: list[int]) -> None:
    conn = duckdb.connect(path)
    try:
        for horizon in (5, 10, 20):
            conn.execute(
                f"""
                CREATE TABLE label_daily_h{horizon} (
                    as_of_date INTEGER,
                    code TEXT,
                    ret_h DOUBLE,
                    mfe_h DOUBLE,
                    mae_h DOUBLE,
                    days_to_mfe_h INTEGER,
                    days_to_stop_h INTEGER
                )
                """
            )
        for idx, as_of_date in enumerate(dates):
            for code, sign in (("1301", 1.0), ("1302", -0.25), ("1303", -1.0)):
                ret_5 = sign * (0.01 + idx * 0.0005)
                ret_10 = sign * (0.015 + idx * 0.0004)
                ret_20 = sign * (0.02 + idx * 0.0003)
                mfe_20 = max(ret_20 + 0.01, 0.005)
                mae_20 = min(ret_20 - 0.015, -0.005)
                for horizon, ret_h in ((5, ret_5), (10, ret_10), (20, ret_20)):
                    conn.execute(
                        f"INSERT INTO label_daily_h{horizon} VALUES (?, ?, ?, ?, ?, ?, ?)",
                        [
                            as_of_date,
                            code,
                            ret_h,
                            mfe_20,
                            mae_20,
                            2,
                            4,
                        ],
                    )
    finally:
        conn.close()


def test_forecast_surface_learning_branch_trains_and_predicts(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    label_db = tmp_path / "label.duckdb"
    tradex_root = tmp_path / "tradex"
    dates = _weekday_dates(date(2026, 1, 5), 40)
    _seed_source_db(str(source_db), dates)
    _seed_label_db(str(label_db), dates)

    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(tradex_root))
    monkeypatch.setattr(fsl, "MIN_TRAIN_ROWS", 1)

    bundle = fsl.load_or_train_forecast_surface_bundle(
        source_db_path=str(source_db),
        label_db_path=str(label_db),
        as_of_date=dates[-1],
        lookback_dates=20,
    )

    assert bundle is not None
    assert sorted(bundle["horizons"]) == [5, 10, 20]
    assert bundle["meta"]["feature_frame_version"] == "v1"
    assert int(bundle["meta"]["training_feature_rows"]) > 0
    assert set((bundle["meta"]["calibration_methods"] or {}).keys()) == {"long", "short"}

    predictions = fsl.predict_current_surface(
        bundle=bundle,
        source_db_path=str(source_db),
        as_of_date=dates[-1],
    )

    assert set(predictions) == {"1301", "1302", "1303"}
    for code, side_map in predictions.items():
        assert set(side_map) == {"long", "short"}
        for side, payload in side_map.items():
            assert 0.0 <= float(payload["direction_prob"]) <= 1.0
            assert payload["expected_ret_10"] is not None
            assert payload["expected_ret_20"] is not None
            assert isinstance(float(payload["expected_ret_20"]), float)


def test_forecast_surface_learning_keeps_expected_ret_10_null_when_horizon_missing(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    label_db = tmp_path / "label.duckdb"
    tradex_root = tmp_path / "tradex"
    dates = _weekday_dates(date(2026, 1, 5), 30)
    _seed_source_db(str(source_db), dates)
    _seed_label_db(str(label_db), dates)

    conn = duckdb.connect(str(label_db), read_only=False)
    try:
        conn.execute("DROP TABLE label_daily_h10")
    finally:
        conn.close()

    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(tradex_root))
    monkeypatch.setattr(fsl, "MIN_TRAIN_ROWS", 1)

    bundle = fsl.load_or_train_forecast_surface_bundle(
        source_db_path=str(source_db),
        label_db_path=str(label_db),
        as_of_date=dates[-1],
        lookback_dates=20,
    )

    assert bundle is not None
    assert sorted(bundle["horizons"]) == [5, 20]

    predictions = fsl.predict_current_surface(
        bundle=bundle,
        source_db_path=str(source_db),
        as_of_date=dates[-1],
    )

    for side_map in predictions.values():
        for payload in side_map.values():
            assert payload["expected_ret_10"] is None


def test_forecast_surface_learning_rejects_future_available_at(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    label_db = tmp_path / "label.duckdb"
    tradex_root = tmp_path / "tradex"
    dates = _weekday_dates(date(2026, 1, 5), 20)
    _seed_source_db(str(source_db), dates)
    _seed_label_db(str(label_db), dates)

    conn = duckdb.connect(str(source_db), read_only=False)
    try:
        conn.execute("DROP VIEW feature_frame_daily")
        conn.execute(
            """
            CREATE TABLE feature_frame_daily AS
            SELECT
                dt,
                code,
                feature_version,
                feature_version AS feature_frame_version,
                computed_at,
                CAST('2026-12-31 00:00:00' AS TIMESTAMP) AS available_at,
                1 AS source_presence_flag_feature_frame_daily,
                0 AS source_presence_flag_tdnet_disclosures,
                feature_a,
                feature_b
            FROM ml_feature_daily
            """
        )
    finally:
        conn.close()

    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(tradex_root))
    monkeypatch.setattr(fsl, "MIN_TRAIN_ROWS", 1)

    try:
        fsl.load_or_train_forecast_surface_bundle(
            source_db_path=str(source_db),
            label_db_path=str(label_db),
            as_of_date=dates[-1],
            lookback_dates=10,
        )
    except RuntimeError as exc:
        assert str(exc) == "feature_frame_future_availability_detected"
    else:
        raise AssertionError("expected point-in-time validation failure")


def test_forecast_surface_learning_prefers_feature_frame_daily_over_legacy_ml_feature_daily(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    label_db = tmp_path / "label.duckdb"
    tradex_root = tmp_path / "tradex"
    dates = _weekday_dates(date(2026, 1, 5), 40)
    _seed_label_db(str(label_db), dates)

    conn = duckdb.connect(str(source_db))
    try:
        conn.execute(
            """
            CREATE TABLE feature_frame_daily (
                dt BIGINT,
                code TEXT,
                close DOUBLE,
                ma7 DOUBLE,
                ma20 DOUBLE,
                ma60 DOUBLE,
                atr14 DOUBLE,
                diff20_pct DOUBLE,
                cnt_20_above INTEGER,
                cnt_7_above INTEGER,
                feature_version INTEGER,
                feature_frame_version TEXT,
                computed_at TIMESTAMP,
                available_at BIGINT,
                source_presence_flag_feature_frame_daily INTEGER,
                source_presence_flag_tdnet_disclosures INTEGER
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE ml_feature_daily (
                dt BIGINT,
                code TEXT,
                close DOUBLE,
                ma7 DOUBLE,
                ma20 DOUBLE,
                ma60 DOUBLE,
                atr14 DOUBLE,
                diff20_pct DOUBLE,
                cnt_20_above INTEGER,
                cnt_7_above INTEGER,
                feature_version INTEGER,
                computed_at TIMESTAMP
            )
            """
        )
        for idx, as_of_date in enumerate(dates):
            dt_value = _epoch_seconds(as_of_date)
            for code, close_shift in (("1301", 1.0), ("1302", -1.0)):
                conn.execute(
                    """
                    INSERT INTO feature_frame_daily VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        dt_value,
                        code,
                        200.0 + idx + close_shift,
                        199.0 + idx + close_shift,
                        198.0 + idx + close_shift,
                        197.0 + idx + close_shift,
                        1.5,
                        0.02,
                        11,
                        7,
                        9,
                        "frame_v9",
                        datetime(2026, 1, 1, tzinfo=timezone.utc),
                        as_of_date,
                        1,
                        0,
                    ],
                )
                conn.execute(
                    """
                    INSERT INTO ml_feature_daily VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        dt_value,
                        code,
                        50.0 + idx - close_shift,
                        49.0 + idx - close_shift,
                        48.0 + idx - close_shift,
                        47.0 + idx - close_shift,
                        0.5,
                        0.01,
                        4,
                        2,
                        3,
                        datetime(2026, 1, 2, tzinfo=timezone.utc),
                    ],
                )
    finally:
        conn.close()

    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(tradex_root))
    monkeypatch.setattr(fsl, "MIN_TRAIN_ROWS", 1)

    bundle = fsl.load_or_train_forecast_surface_bundle(
        source_db_path=str(source_db),
        label_db_path=str(label_db),
        as_of_date=dates[-1],
        lookback_dates=20,
    )

    assert bundle is not None
    assert bundle["meta"]["feature_frame_version"] == "frame_v9"
    assert "source_presence_flag_tdnet_disclosures" in bundle["meta"]["feature_columns"]
    assert "available_at" not in bundle["meta"]["feature_columns"]


def test_forecast_surface_learning_normalizes_pre_2001_epoch_dates(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    label_db = tmp_path / "label.duckdb"
    tradex_root = tmp_path / "tradex"
    dates = _weekday_dates(date(2001, 8, 20), 25)
    _seed_source_db(str(source_db), dates)
    _seed_label_db(str(label_db), dates)

    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(tradex_root))
    monkeypatch.setattr(fsl, "MIN_TRAIN_ROWS", 1)

    bundle = fsl.load_or_train_forecast_surface_bundle(
        source_db_path=str(source_db),
        label_db_path=str(label_db),
        as_of_date=dates[-1],
        lookback_dates=20,
    )

    assert bundle is not None
    assert int(bundle["meta"]["source_max_dt"]) == int(dates[-1])

    predictions = fsl.predict_current_surface(
        bundle=bundle,
        source_db_path=str(source_db),
        as_of_date=dates[-1],
    )

    assert set(predictions) == {"1301", "1302", "1303"}
