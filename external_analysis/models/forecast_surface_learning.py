from __future__ import annotations

import os
import pickle
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd

from external_analysis.exporter.source_reader import connect_source_db, source_table_exists
from external_analysis.labels.store import connect_label_db

MODEL_VERSION = "forecast_surface_lgb_v1"
DEFAULT_LOOKBACK_DATES = 252
MIN_TRAIN_ROWS = 5000


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _as_of_date_text(value: int) -> str:
    text = str(int(value))
    return f"{text[0:4]}-{text[4:6]}-{text[6:8]}"


def _tradex_root() -> Path:
    root = os.environ.get("MEEMEE_TRADEX_ROOT") or r"G:\Tradex"
    return Path(root)


def _cache_path() -> Path:
    return _tradex_root() / "models" / "forecast_surface_bundle.pkl"


def _table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'main' AND table_name = ?
        LIMIT 1
        """,
        [table_name],
    ).fetchone()
    return bool(row)


def _feature_table_name(source_conn: duckdb.DuckDBPyConnection) -> str | None:
    if _table_exists(source_conn, "feature_frame_daily"):
        return "feature_frame_daily"
    if _table_exists(source_conn, "ml_feature_daily"):
        return "ml_feature_daily"
    return None


def _feature_table_columns(source_conn: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    rows = source_conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    return {str(row[1]) for row in rows}


def _feature_table_column_types(source_conn: duckdb.DuckDBPyConnection, table_name: str) -> dict[str, str]:
    rows = source_conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    return {str(row[1]): str(row[2]).upper() for row in rows}


def _feature_date_expr(table_columns: set[str]) -> str:
    if "dt" in table_columns:
        return """
            CASE
                WHEN CAST(dt AS BIGINT) >= 100000000 THEN CAST(strftime(to_timestamp(CAST(dt AS BIGINT)), '%Y%m%d') AS INTEGER)
                ELSE CAST(dt AS INTEGER)
            END
        """
    if "as_of_date" in table_columns:
        return "CAST(as_of_date AS INTEGER)"
    raise ValueError("feature_table_missing_date_column")


def _feature_version_column(table_columns: set[str]) -> str | None:
    if "feature_frame_version" in table_columns:
        return "feature_frame_version"
    if "feature_version" in table_columns:
        return "feature_version"
    return None


def _available_at_date_expr(table_columns: dict[str, str]) -> str | None:
    column_type = table_columns.get("available_at")
    if column_type is None:
        return None
    if column_type.startswith("TIMESTAMP") or column_type == "DATE":
        return "CAST(strftime(CAST(available_at AS TIMESTAMP), '%Y%m%d') AS INTEGER)"
    return """
        CASE
            WHEN CAST(available_at AS BIGINT) >= 100000000 THEN CAST(strftime(to_timestamp(CAST(available_at AS BIGINT)), '%Y%m%d') AS INTEGER)
            ELSE CAST(available_at AS INTEGER)
        END
    """


def _validate_point_in_time_feature_frame(
    source_conn: duckdb.DuckDBPyConnection,
    *,
    dates: list[int],
    as_of_date: int,
) -> None:
    table_name = _feature_table_name(source_conn)
    if not table_name or not dates:
        return
    table_columns = _feature_table_columns(source_conn, table_name)
    table_column_types = _feature_table_column_types(source_conn, table_name)
    available_at_expr = _available_at_date_expr(table_column_types)
    if available_at_expr is None:
        return
    date_expr = _feature_date_expr(table_columns)
    max_allowed_date = max(int(value) for value in dates if value is not None)
    effective_as_of_date = max(int(as_of_date), int(max_allowed_date))
    placeholders = ", ".join(["?"] * len(dates))
    row = source_conn.execute(
        f"""
        SELECT COUNT(*)
        FROM {table_name}
        WHERE {date_expr} IN ({placeholders})
          AND {available_at_expr} IS NOT NULL
          AND ({available_at_expr} > {date_expr} OR {available_at_expr} > ?)
        """,
        [*dates, effective_as_of_date],
    ).fetchone()
    if row and int(row[0] or 0) > 0:
        raise RuntimeError("feature_frame_future_availability_detected")


def _label_table_name(horizon: int) -> str:
    return f"label_daily_h{int(horizon)}"


def _get_feature_columns(source_conn: duckdb.DuckDBPyConnection) -> list[str]:
    table_name = _feature_table_name(source_conn)
    if not table_name:
        return []
    rows = source_conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    excluded = {
        "dt",
        "as_of_date",
        "code",
        "feature_version",
        "feature_frame_version",
        "computed_at",
        "available_at",
        "source_presence_flag",
    }
    return [str(row[1]) for row in rows if str(row[1]) not in excluded]


def _current_feature_max_dt(source_conn: duckdb.DuckDBPyConnection) -> int | None:
    table_name = _feature_table_name(source_conn)
    if not table_name:
        return None
    columns = _feature_table_columns(source_conn, table_name)
    row = source_conn.execute(f"SELECT MAX({_feature_date_expr(columns)}) FROM {table_name}").fetchone()
    if not row or row[0] is None:
        return None
    return int(row[0])


def _current_feature_version(source_conn: duckdb.DuckDBPyConnection) -> str | None:
    table_name = _feature_table_name(source_conn)
    if not table_name:
        return None
    columns = _feature_table_columns(source_conn, table_name)
    version_column = _feature_version_column(columns)
    if not version_column:
        return None
    row = source_conn.execute(
        f"SELECT MAX(CAST({version_column} AS VARCHAR)) FROM {table_name} WHERE {version_column} IS NOT NULL"
    ).fetchone()
    if not row or row[0] is None:
        return None
    return str(row[0])


def _count_feature_rows(
    source_conn: duckdb.DuckDBPyConnection,
    *,
    dates: list[int],
) -> int:
    table_name = _feature_table_name(source_conn)
    if not table_name or not dates:
        return 0
    columns = _feature_table_columns(source_conn, table_name)
    date_expr = _feature_date_expr(columns)
    placeholders = ", ".join(["?"] * len(dates))
    row = source_conn.execute(
        f"SELECT COUNT(*) FROM {table_name} WHERE {date_expr} IN ({placeholders})",
        dates,
    ).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _load_training_dates(
    label_conn: duckdb.DuckDBPyConnection,
    *,
    as_of_date: int,
    lookback_dates: int,
) -> list[int]:
    query = []
    for horizon in (5, 10, 20):
        table_name = _label_table_name(horizon)
        if not _table_exists(label_conn, table_name):
            continue
        rows = label_conn.execute(
            f"""
            SELECT DISTINCT as_of_date
            FROM {table_name}
            WHERE as_of_date <= ?
            ORDER BY as_of_date DESC
            LIMIT ?
            """,
            [as_of_date, lookback_dates],
        ).fetchall()
        query.extend(int(row[0]) for row in rows if row and row[0] is not None)
    return sorted(set(query), reverse=True)[:lookback_dates]


def _load_feature_frame(
    source_conn: duckdb.DuckDBPyConnection,
    *,
    dates: list[int],
    feature_columns: list[str],
) -> pd.DataFrame:
    if not dates:
        return pd.DataFrame()
    table_name = _feature_table_name(source_conn)
    if not table_name:
        return pd.DataFrame()
    table_columns = _feature_table_columns(source_conn, table_name)
    placeholders = ", ".join(["?"] * len(dates))
    select_columns = ", ".join([f"CAST({col} AS DOUBLE) AS {col}" if col not in {"cnt_20_above", "cnt_7_above"} else f"CAST({col} AS DOUBLE) AS {col}" for col in feature_columns])
    if select_columns:
        select_columns = ", " + select_columns
    query = f"""
        SELECT
            {_feature_date_expr(table_columns)} AS as_of_date,
            code
            {select_columns}
        FROM {table_name}
        WHERE {_feature_date_expr(table_columns)} IN ({placeholders})
    """
    return source_conn.execute(query, dates).fetchdf()


def _load_label_frame(
    label_conn: duckdb.DuckDBPyConnection,
    *,
    horizon: int,
    dates: list[int],
) -> pd.DataFrame:
    if not dates:
        return pd.DataFrame()
    table_name = _label_table_name(horizon)
    if not _table_exists(label_conn, table_name):
        return pd.DataFrame()
    placeholders = ", ".join(["?"] * len(dates))
    query = f"""
        SELECT
            as_of_date,
            code,
            ret_h,
            mfe_h,
            mae_h,
            days_to_mfe_h,
            days_to_stop_h
        FROM {table_name}
        WHERE as_of_date IN ({placeholders})
    """
    return label_conn.execute(query, dates).fetchdf()


def _fit_lightgbm_models(X: np.ndarray, y: np.ndarray, *, kind: str) -> Any:
    try:
        import lightgbm as lgb  # type: ignore

        if kind == "classifier":
            pos = float(np.sum(y > 0.5))
            neg = float(len(y) - pos)
            params = {
                "objective": "binary",
                "n_estimators": 200,
                "learning_rate": 0.05,
                "max_depth": 5,
                "num_leaves": 31,
                "min_child_samples": 20,
                "subsample": 0.8,
                "colsample_bytree": 0.8,
                "random_state": 42,
                "n_jobs": -1,
                "verbose": -1,
                "scale_pos_weight": max(1.0, neg / max(pos, 1.0)),
            }
            model = lgb.LGBMClassifier(**params)
            model.fit(X, y.astype(int))
            return model
        params = {
            "objective": "regression_l1",
            "n_estimators": 300,
            "learning_rate": 0.05,
            "max_depth": 5,
            "num_leaves": 31,
            "min_child_samples": 20,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
            "random_state": 42,
            "n_jobs": -1,
            "verbose": -1,
        }
        model = lgb.LGBMRegressor(**params)
        model.fit(X, y.astype(float))
        return model
    except Exception:
        from sklearn.linear_model import LogisticRegression, Ridge
        from sklearn.pipeline import make_pipeline
        from sklearn.preprocessing import StandardScaler

        if kind == "classifier":
            model = make_pipeline(StandardScaler(with_mean=True), LogisticRegression(max_iter=500))
            model.fit(X, y.astype(int))
            return model
        model = make_pipeline(StandardScaler(with_mean=True), Ridge(alpha=1.0))
        model.fit(X, y.astype(float))
        return model


def _fit_probability_calibrator(probs: np.ndarray, y: np.ndarray) -> tuple[Any | None, str]:
    clipped = np.clip(np.asarray(probs, dtype=float), 1e-6, 1.0 - 1e-6)
    labels = np.asarray(y, dtype=int)
    unique_labels = set(labels.tolist())
    if len(clipped) < 20 or len(unique_labels) < 2:
        return None, "none"
    try:
        from sklearn.isotonic import IsotonicRegression

        if len(np.unique(np.round(clipped, 4))) >= 10 and len(clipped) >= 100:
            calibrator = IsotonicRegression(out_of_bounds="clip")
            calibrator.fit(clipped, labels)
            return calibrator, "isotonic"
    except Exception:
        pass
    try:
        from sklearn.linear_model import LogisticRegression

        calibrator = LogisticRegression(max_iter=500)
        calibrator.fit(clipped.reshape(-1, 1), labels)
        return calibrator, "platt"
    except Exception:
        return None, "none"


def _apply_probability_calibrator(calibrator: Any | None, method: str, probs: np.ndarray) -> np.ndarray:
    clipped = np.clip(np.asarray(probs, dtype=float), 1e-6, 1.0 - 1e-6)
    if calibrator is None or method == "none":
        return clipped
    try:
        if method == "isotonic":
            return np.clip(np.asarray(calibrator.predict(clipped), dtype=float), 0.0, 1.0)
        if method == "platt":
            return np.clip(np.asarray(calibrator.predict_proba(clipped.reshape(-1, 1))[:, 1], dtype=float), 0.0, 1.0)
    except Exception:
        return clipped
    return clipped


def _matrix_from_frame(frame: pd.DataFrame, feature_columns: list[str], medians: dict[str, float]) -> np.ndarray:
    if frame.empty:
        return np.zeros((0, len(feature_columns)), dtype=float)
    cols = []
    for column in feature_columns:
        series = pd.to_numeric(frame.get(column), errors="coerce")
        cols.append(series.fillna(medians.get(column, 0.0)).to_numpy(dtype=float))
    if not cols:
        return np.zeros((len(frame), 1), dtype=float)
    return np.column_stack(cols)


def _train_one_horizon(
    *,
    frame: pd.DataFrame,
    feature_columns: list[str],
    horizon: int,
) -> dict[str, Any] | None:
    if frame.empty or len(frame) < MIN_TRAIN_ROWS:
        return None
    medians = {
        column: float(pd.to_numeric(frame[column], errors="coerce").median()) if column in frame.columns else 0.0
        for column in feature_columns
    }
    X = _matrix_from_frame(frame, feature_columns, medians)
    ret = pd.to_numeric(frame["ret_h"], errors="coerce").fillna(0.0).to_numpy(dtype=float)
    mfe = pd.to_numeric(frame["mfe_h"], errors="coerce").fillna(0.0).to_numpy(dtype=float)
    mae = pd.to_numeric(frame["mae_h"], errors="coerce").fillna(0.0).to_numpy(dtype=float)

    side_targets = {
        "long": {
            "direction": (ret > 0).astype(int),
            "ret": ret,
            "favorable": np.maximum(mfe, 0.0),
            "adverse": np.maximum(-mae, 0.0),
        },
        "short": {
            "direction": (ret < 0).astype(int),
            "ret": -ret,
            "favorable": np.maximum(-mae, 0.0),
            "adverse": np.maximum(mfe, 0.0),
        },
    }
    side_models: dict[str, dict[str, Any]] = {}
    for side, targets in side_targets.items():
        clf = _fit_lightgbm_models(X, targets["direction"], kind="classifier")
        reg = _fit_lightgbm_models(X, targets["ret"], kind="regressor")
        raw_probs = (
            np.asarray(clf.predict_proba(pd.DataFrame(X, columns=feature_columns))[:, 1], dtype=float)
            if hasattr(clf, "predict_proba")
            else np.asarray(clf.predict(pd.DataFrame(X, columns=feature_columns)), dtype=float)
        )
        calibrator, calibration_method = _fit_probability_calibrator(raw_probs, targets["direction"])
        side_bundle: dict[str, Any] = {
            "classifier": clf,
            "probability_calibrator": calibrator,
            "calibration_method": calibration_method,
            "ret_regressor": reg,
        }
        if horizon == 20:
            side_bundle["favorable_regressor"] = _fit_lightgbm_models(X, targets["favorable"], kind="regressor")
            side_bundle["adverse_regressor"] = _fit_lightgbm_models(X, targets["adverse"], kind="regressor")
        side_models[side] = side_bundle
    return {
        "horizon": horizon,
        "train_rows": int(len(frame)),
        "medians": medians,
        "models": side_models,
    }


def load_or_train_forecast_surface_bundle(
    *,
    source_db_path: str,
    label_db_path: str,
    as_of_date: int,
    lookback_dates: int = DEFAULT_LOOKBACK_DATES,
) -> dict[str, Any] | None:
    source_conn = connect_source_db(source_db_path)
    label_conn = connect_label_db(label_db_path, read_only=True)
    try:
        if _feature_table_name(source_conn) is None:
            return None
        if not any(_table_exists(label_conn, _label_table_name(h)) for h in (5, 10, 20)):
            return None
        feature_columns = _get_feature_columns(source_conn)
        if not feature_columns:
            return None
        source_max_dt = _current_feature_max_dt(source_conn)
        feature_frame_version = _current_feature_version(source_conn)
        train_dates = _load_training_dates(label_conn, as_of_date=as_of_date, lookback_dates=lookback_dates)
        if not train_dates:
            return None
        _validate_point_in_time_feature_frame(source_conn, dates=train_dates, as_of_date=as_of_date)
        training_feature_rows = _count_feature_rows(source_conn, dates=train_dates)
        cache_file = _cache_path()
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        if cache_file.exists():
            try:
                with cache_file.open("rb") as fh:
                    bundle = pickle.load(fh)
                meta = bundle.get("meta", {})
                if (
                    meta.get("model_version") == MODEL_VERSION
                    and int(meta.get("source_max_dt") or 0) == int(source_max_dt or 0)
                    and str(meta.get("feature_frame_version") or "") == str(feature_frame_version or "")
                    and int(meta.get("label_max_as_of_date") or 0) == int(max(train_dates) if train_dates else 0)
                    and int(meta.get("lookback_dates") or 0) == int(lookback_dates)
                    and int(meta.get("training_feature_rows") or 0) == int(training_feature_rows)
                ):
                    return bundle
            except Exception:
                pass

        features_df = _load_feature_frame(source_conn, dates=train_dates, feature_columns=feature_columns)
        if features_df.empty:
            return None
        horizons: dict[int, dict[str, Any]] = {}
        for horizon in (5, 10, 20):
            labels_df = _load_label_frame(label_conn, horizon=horizon, dates=train_dates)
            if labels_df.empty:
                continue
            merged = features_df.merge(labels_df, on=["as_of_date", "code"], how="inner")
            learned = _train_one_horizon(frame=merged, feature_columns=feature_columns, horizon=horizon)
            if learned:
                horizons[horizon] = learned
        if not horizons:
            return None
        calibration_methods = {
            side: str(
                (horizons.get(20, {}).get("models", {}).get(side, {}) or {}).get("calibration_method")
                or next(
                    (
                        (horizon_bundle.get("models", {}).get(side, {}) or {}).get("calibration_method")
                        for horizon_bundle in horizons.values()
                        if (horizon_bundle.get("models", {}).get(side, {}) or {}).get("calibration_method")
                    ),
                    "none",
                )
            )
            for side in ("long", "short")
        }
        bundle = {
            "meta": {
                "model_version": MODEL_VERSION,
                "trained_at": _utcnow().isoformat(timespec="seconds"),
                "source_max_dt": int(source_max_dt or 0),
                "feature_frame_version": feature_frame_version,
                "label_max_as_of_date": int(max(train_dates) if train_dates else 0),
                "lookback_dates": int(lookback_dates),
                "training_feature_rows": int(training_feature_rows),
                "feature_columns": feature_columns,
                "horizons": sorted(horizons),
                "calibration_methods": calibration_methods,
            },
            "feature_columns": feature_columns,
            "horizons": horizons,
        }
        with cache_file.open("wb") as fh:
            pickle.dump(bundle, fh)
        return bundle
    finally:
        source_conn.close()
        label_conn.close()


def predict_current_surface(
    *,
    bundle: dict[str, Any],
    source_db_path: str,
    as_of_date: int,
) -> dict[str, dict[str, Any]]:
    source_conn = connect_source_db(source_db_path)
    try:
        feature_columns = list(bundle.get("feature_columns") or [])
        if not feature_columns:
            return {}
        table_name = _feature_table_name(source_conn)
        if not table_name:
            return {}
        table_columns = _feature_table_columns(source_conn, table_name)
        _validate_point_in_time_feature_frame(source_conn, dates=[as_of_date], as_of_date=as_of_date)
        date_expr = _feature_date_expr(table_columns)
        version_column = _feature_version_column(table_columns)
        version_filter = ""
        params: list[Any] = [as_of_date]
        if version_column:
            version_filter = f" AND {version_column} = (SELECT MAX({version_column}) FROM {table_name})"
        feature_rows = source_conn.execute(
            f"""
            SELECT
                code,
                {", ".join(feature_columns)}
            FROM {table_name}
            WHERE {date_expr} = ?{version_filter}
            ORDER BY code
            """,
            params,
        ).fetchdf()
        if feature_rows.empty:
            return {}
        feature_rows = feature_rows.reset_index(drop=True)
        medians = {}
        for column in feature_columns:
            if column in feature_rows.columns:
                series = pd.to_numeric(feature_rows[column], errors="coerce")
                medians[column] = float(series.median()) if not series.dropna().empty else 0.0
            else:
                medians[column] = 0.0
        X = _matrix_from_frame(feature_rows, feature_columns, medians)
        X_df = pd.DataFrame(X, columns=feature_columns)
        preds: dict[str, dict[str, Any]] = {}
        for side in ("long", "short"):
            horizon_predictions: dict[int, dict[str, np.ndarray]] = {}
            for horizon, horizon_bundle in sorted((bundle.get("horizons") or {}).items()):
                side_models = horizon_bundle.get("models", {}).get(side)
                if not side_models:
                    continue
                clf = side_models.get("classifier")
                reg = side_models.get("ret_regressor")
                calibration_method = str(side_models.get("calibration_method") or "none")
                calibrator = side_models.get("probability_calibrator")
                if clf is not None:
                    try:
                        prob = clf.predict_proba(X_df)[:, 1].astype(float)
                    except Exception:
                        prob = np.asarray(clf.predict(X_df), dtype=float)
                else:
                    prob = np.full(len(feature_rows), 0.5, dtype=float)
                prob = _apply_probability_calibrator(calibrator, calibration_method, prob)
                if reg is not None:
                    try:
                        ret = np.asarray(reg.predict(X_df), dtype=float)
                    except Exception:
                        ret = np.zeros(len(feature_rows), dtype=float)
                else:
                    ret = np.zeros(len(feature_rows), dtype=float)
                horizon_predictions[int(horizon)] = {"prob": prob, "ret": ret}
            if not horizon_predictions:
                continue
            for idx, row in feature_rows.iterrows():
                code = str(row["code"])
                if side == "long":
                    weights = {5: 0.2, 10: 0.3, 20: 0.5}
                else:
                    weights = {5: 0.2, 10: 0.3, 20: 0.5}
                prob_values = []
                ret_values = []
                for horizon, weight in weights.items():
                    pred = horizon_predictions.get(horizon)
                    if pred is None:
                        continue
                    prob_values.append((weight, float(pred["prob"][idx])))
                    ret_values.append((weight, float(pred["ret"][idx])))
                if not prob_values:
                    continue
                total_w = sum(weight for weight, _ in prob_values) or 1.0
                direction_prob = sum(weight * value for weight, value in prob_values) / total_w
                expected_ret_20 = 0.0
                pred_20 = horizon_predictions.get(20)
                if pred_20 is not None:
                    expected_ret_20 = float(pred_20["ret"][idx])
                elif ret_values:
                    expected_ret_20 = sum(weight * value for weight, value in ret_values) / total_w
                if 5 in horizon_predictions:
                    expected_ret_5 = float(horizon_predictions[5]["ret"][idx])
                else:
                    expected_ret_5 = expected_ret_20 * 0.35
                if 10 in horizon_predictions:
                    expected_ret_10 = float(horizon_predictions[10]["ret"][idx])
                else:
                    expected_ret_10 = None
                expected_mfe_20 = None
                expected_mae_20 = None
                side_models_20 = (bundle.get("horizons") or {}).get(20, {}).get("models", {}).get(side, {})
                mfereg = side_models_20.get("favorable_regressor")
                maereg = side_models_20.get("adverse_regressor")
                if mfereg is not None:
                    try:
                        expected_mfe_20 = float(np.asarray(mfereg.predict(X_df.iloc[[idx]]), dtype=float)[0])
                    except Exception:
                        expected_mfe_20 = None
                if maereg is not None:
                    try:
                        expected_mae_20 = float(np.asarray(maereg.predict(X_df.iloc[[idx]]), dtype=float)[0])
                    except Exception:
                        expected_mae_20 = None
                preds[code] = preds.get(code, {})
                preds[code][side] = {
                    "direction_prob": float(max(0.0, min(1.0, direction_prob))),
                    "expected_ret_5": float(expected_ret_5),
                    "expected_ret_10": None if expected_ret_10 is None else float(expected_ret_10),
                    "expected_ret_20": float(expected_ret_20),
                    "expected_mfe_20": expected_mfe_20,
                    "expected_mae_20": expected_mae_20,
                }
        return preds
    finally:
        source_conn.close()
