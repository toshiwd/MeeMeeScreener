from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from app.core.config import config as core_config
from app.db.session import get_conn
from app.backend.services.ml_config import MLConfig, load_ml_config

FEATURE_VERSION = 2
LABEL_VERSION = 2
MODEL_KEY = "ml_ev20_simple_v1"
OBJECTIVE = "ret20_regression_with_p_up_gate"
TURN_HORIZON_DAYS = 10
TURN_TARGET_PCT = 0.06
TURN_STOP_PCT = 0.03

BASE_FEATURE_COLUMNS: list[str] = [
    "close",
    "ma7",
    "ma20",
    "ma60",
    "atr14",
    "diff20_pct",
    "cnt_20_above",
    "cnt_7_above",
]

DERIVED_FEATURE_COLUMNS: list[str] = [
    "dist_ma20",
    "dist_ma60",
    "ma7_ma20_gap",
    "ma20_ma60_gap",
    "close_ret1",
    "close_ret5",
    "close_ret10",
    "ma7_slope1",
    "ma20_slope1",
    "ma60_slope1",
    "ma20_slope_delta1",
    "dist_ma20_delta1",
    "cnt_7_above_norm",
    "cnt_20_above_norm",
]

FEATURE_COLUMNS: list[str] = [*BASE_FEATURE_COLUMNS, *DERIVED_FEATURE_COLUMNS]


@dataclass(frozen=True)
class TrainedModels:
    cls: Any
    reg: Any
    turn_up: Any | None
    turn_down: Any | None
    feature_columns: list[str]
    medians: dict[str, float]
    n_train_cls: int
    n_train_reg: int
    n_train_turn_up: int
    n_train_turn_down: int


def _import_lightgbm():
    try:
        import lightgbm as lgb  # type: ignore
    except ModuleNotFoundError as exc:  # pragma: no cover - runtime dependency
        raise RuntimeError(
            "lightgbm is not installed. Please install app/backend/requirements.txt first."
        ) from exc
    return lgb


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table_name],
    ).fetchone()
    return bool(row and row[0])


def _ensure_ml_schema(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ml_feature_daily (
            dt INTEGER,
            code TEXT,
            close DOUBLE,
            ma7 DOUBLE,
            ma20 DOUBLE,
            ma60 DOUBLE,
            atr14 DOUBLE,
            diff20_pct DOUBLE,
            cnt_20_above INTEGER,
            cnt_7_above INTEGER,
            close_prev1 DOUBLE,
            close_prev5 DOUBLE,
            close_prev10 DOUBLE,
            ma7_prev1 DOUBLE,
            ma20_prev1 DOUBLE,
            ma60_prev1 DOUBLE,
            diff20_prev1 DOUBLE,
            cnt_20_prev1 INTEGER,
            cnt_7_prev1 INTEGER,
            feature_version INTEGER,
            computed_at TIMESTAMP,
            PRIMARY KEY(code, dt)
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ml_label_20d (
            dt INTEGER,
            code TEXT,
            ret20 DOUBLE,
            up20_label INTEGER,
            train_mask_cls INTEGER,
            turn_up_label INTEGER,
            turn_down_label INTEGER,
            train_mask_turn INTEGER,
            n_forward INTEGER,
            label_version INTEGER,
            computed_at TIMESTAMP,
            PRIMARY KEY(code, dt)
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ml_pred_20d (
            dt INTEGER,
            code TEXT,
            p_up DOUBLE,
            p_turn_up DOUBLE,
            p_turn_down DOUBLE,
            ret_pred20 DOUBLE,
            ev20 DOUBLE,
            ev20_net DOUBLE,
            model_version TEXT,
            n_train INTEGER,
            computed_at TIMESTAMP,
            PRIMARY KEY(code, dt)
        );
        """
    )
    # Backward-compatible schema migrations.
    conn.execute("ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS close_prev1 DOUBLE")
    conn.execute("ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS close_prev5 DOUBLE")
    conn.execute("ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS close_prev10 DOUBLE")
    conn.execute("ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS ma7_prev1 DOUBLE")
    conn.execute("ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS ma20_prev1 DOUBLE")
    conn.execute("ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS ma60_prev1 DOUBLE")
    conn.execute("ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS diff20_prev1 DOUBLE")
    conn.execute("ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS cnt_20_prev1 INTEGER")
    conn.execute("ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS cnt_7_prev1 INTEGER")
    conn.execute("ALTER TABLE ml_label_20d ADD COLUMN IF NOT EXISTS turn_up_label INTEGER")
    conn.execute("ALTER TABLE ml_label_20d ADD COLUMN IF NOT EXISTS turn_down_label INTEGER")
    conn.execute("ALTER TABLE ml_label_20d ADD COLUMN IF NOT EXISTS train_mask_turn INTEGER")
    conn.execute("ALTER TABLE ml_pred_20d ADD COLUMN IF NOT EXISTS p_turn_up DOUBLE")
    conn.execute("ALTER TABLE ml_pred_20d ADD COLUMN IF NOT EXISTS p_turn_down DOUBLE")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ml_model_registry (
            model_version TEXT PRIMARY KEY,
            model_key TEXT,
            objective TEXT,
            feature_version INTEGER,
            label_version INTEGER,
            train_start_dt INTEGER,
            train_end_dt INTEGER,
            metrics_json TEXT,
            artifact_path TEXT,
            n_train INTEGER,
            created_at TIMESTAMP,
            is_active BOOLEAN
        );
        """
    )


def _safe_float(value: object) -> float | None:
    try:
        f = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if not math.isfinite(f):
        return None
    return f


def compute_label_fields(ret20: float, neutral_band_pct: float) -> tuple[int, int]:
    up20_label = 1 if ret20 > 0 else 0
    train_mask_cls = 1 if abs(ret20) >= neutral_band_pct else 0
    return up20_label, train_mask_cls


def compute_ev20_net(ev20: float, cost_rate: float) -> float:
    return float(ev20) - float(cost_rate)


def _get_close_column(conn) -> str:
    cols = conn.execute("PRAGMA table_info('daily_bars')").fetchall()
    names = {str(row[1]).lower() for row in cols}
    if "adj_close" in names:
        return "adj_close"
    return "c"


def refresh_ml_feature_table(
    conn,
    feature_version: int = FEATURE_VERSION,
    start_dt: int | None = None,
    end_dt: int | None = None,
) -> int:
    _ensure_ml_schema(conn)
    where: list[str] = []
    params: list[object] = [int(feature_version)]
    if start_dt is not None:
        where.append("dt >= ?")
        params.append(int(start_dt))
    if end_dt is not None:
        where.append("dt <= ?")
        params.append(int(end_dt))
    where_sql = (" WHERE " + " AND ".join(where)) if where else ""

    if where:
        del_where = " AND ".join(where)
        conn.execute(f"DELETE FROM ml_feature_daily WHERE {del_where}", params[1:])
    else:
        conn.execute("DELETE FROM ml_feature_daily")

    conn.execute(
        f"""
        INSERT INTO ml_feature_daily (
            dt,
            code,
            close,
            ma7,
            ma20,
            ma60,
            atr14,
            diff20_pct,
            cnt_20_above,
            cnt_7_above,
            close_prev1,
            close_prev5,
            close_prev10,
            ma7_prev1,
            ma20_prev1,
            ma60_prev1,
            diff20_prev1,
            cnt_20_prev1,
            cnt_7_prev1,
            feature_version,
            computed_at
        )
        SELECT
            dt,
            code,
            close,
            ma7,
            ma20,
            ma60,
            atr14,
            diff20_pct,
            cnt_20_above,
            cnt_7_above,
            LAG(close, 1) OVER (PARTITION BY code ORDER BY dt) AS close_prev1,
            LAG(close, 5) OVER (PARTITION BY code ORDER BY dt) AS close_prev5,
            LAG(close, 10) OVER (PARTITION BY code ORDER BY dt) AS close_prev10,
            LAG(ma7, 1) OVER (PARTITION BY code ORDER BY dt) AS ma7_prev1,
            LAG(ma20, 1) OVER (PARTITION BY code ORDER BY dt) AS ma20_prev1,
            LAG(ma60, 1) OVER (PARTITION BY code ORDER BY dt) AS ma60_prev1,
            LAG(diff20_pct, 1) OVER (PARTITION BY code ORDER BY dt) AS diff20_prev1,
            LAG(cnt_20_above, 1) OVER (PARTITION BY code ORDER BY dt) AS cnt_20_prev1,
            LAG(cnt_7_above, 1) OVER (PARTITION BY code ORDER BY dt) AS cnt_7_prev1,
            ?,
            CURRENT_TIMESTAMP
        FROM feature_snapshot_daily
        {where_sql}
        """,
        params,
    )
    return int(conn.execute("SELECT COUNT(*) FROM ml_feature_daily").fetchone()[0])


def refresh_ml_label_table(
    conn,
    cfg: MLConfig,
    label_version: int = LABEL_VERSION,
    start_dt: int | None = None,
    end_dt: int | None = None,
) -> int:
    _ensure_ml_schema(conn)
    if not _table_exists(conn, "daily_bars"):
        return 0

    close_col = _get_close_column(conn)
    where: list[str] = [f"{close_col} IS NOT NULL"]
    params: list[object] = []
    if start_dt is not None:
        where.append("date >= ?")
        params.append(int(start_dt))
    if end_dt is not None:
        # Keep a forward margin for 20 business-day labels.
        where.append("date <= ?")
        params.append(int(end_dt) + 90 * 86400)
    where_sql = " AND ".join(where)
    rows = conn.execute(
        f"""
        SELECT code, date, {close_col}
        FROM daily_bars
        WHERE {where_sql}
        ORDER BY code, date
        """,
        params,
    ).fetchall()
    if start_dt is not None or end_dt is not None:
        del_where: list[str] = []
        del_params: list[object] = []
        if start_dt is not None:
            del_where.append("dt >= ?")
            del_params.append(int(start_dt))
        if end_dt is not None:
            del_where.append("dt <= ?")
            del_params.append(int(end_dt))
        conn.execute(f"DELETE FROM ml_label_20d WHERE {' AND '.join(del_where)}", del_params)
    else:
        conn.execute("DELETE FROM ml_label_20d")
    if not rows:
        return 0

    neutral = float(cfg.neutral_band_pct)
    computed_at = datetime.now(tz=timezone.utc).replace(tzinfo=None)
    records: list[tuple] = []
    inserted = 0

    def _insert_chunk(chunk: list[tuple]) -> int:
        if not chunk:
            return 0
        conn.executemany(
            """
            INSERT INTO ml_label_20d (
                dt,
                code,
                ret20,
                up20_label,
                train_mask_cls,
                turn_up_label,
                turn_down_label,
                train_mask_turn,
                n_forward,
                label_version,
                computed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            chunk,
        )
        return len(chunk)

    def _flush(code: str, dates: list[int], closes: list[float]) -> None:
        nonlocal inserted
        if not code:
            return
        max_i = len(closes) - 20
        if max_i <= 0:
            return
        for i in range(max_i):
            dt_i = int(dates[i])
            if start_dt is not None and dt_i < int(start_dt):
                continue
            if end_dt is not None and dt_i > int(end_dt):
                continue
            base = closes[i]
            future = closes[i + 20]
            if base == 0:
                continue
            ret20 = (future / base) - 1.0
            up20_label, train_mask_cls = compute_label_fields(ret20, neutral)
            future_turn = closes[i + 1 : i + 1 + TURN_HORIZON_DAYS]
            if len(future_turn) < TURN_HORIZON_DAYS:
                continue
            max_ret_turn = (max(future_turn) / base) - 1.0
            min_ret_turn = (min(future_turn) / base) - 1.0
            turn_up_label = 1 if (max_ret_turn >= TURN_TARGET_PCT and min_ret_turn > -TURN_STOP_PCT) else 0
            turn_down_label = 1 if (min_ret_turn <= -TURN_TARGET_PCT and max_ret_turn < TURN_STOP_PCT) else 0
            train_mask_turn = 1 if (turn_up_label == 1 or turn_down_label == 1 or abs(ret20) >= neutral) else 0
            records.append(
                (
                    dt_i,
                    code,
                    float(ret20),
                    int(up20_label),
                    int(train_mask_cls),
                    int(turn_up_label),
                    int(turn_down_label),
                    int(train_mask_turn),
                    20,
                    int(label_version),
                    computed_at,
                )
            )
            if len(records) >= 50_000:
                inserted += _insert_chunk(records)
                records.clear()

    current_code = ""
    dates: list[int] = []
    closes: list[float] = []
    for code_raw, dt_raw, close_raw in rows:
        code = str(code_raw)
        dt = int(dt_raw)
        close = _safe_float(close_raw)
        if close is None:
            continue
        if current_code and code != current_code:
            _flush(current_code, dates, closes)
            dates = []
            closes = []
        current_code = code
        dates.append(dt)
        closes.append(close)
    _flush(current_code, dates, closes)

    inserted += _insert_chunk(records)
    return int(inserted)


def _load_training_df(conn, start_dt: int | None = None, end_dt: int | None = None) -> pd.DataFrame:
    where = ["l.ret20 IS NOT NULL"]
    params: list[object] = []
    if start_dt is not None:
        where.append("f.dt >= ?")
        params.append(int(start_dt))
    if end_dt is not None:
        where.append("f.dt <= ?")
        params.append(int(end_dt))
    where_sql = " AND ".join(where)
    sql = (
        """
        SELECT
            f.dt,
            f.code,
            f.close,
            f.ma7,
            f.ma20,
            f.ma60,
            f.atr14,
            f.diff20_pct,
            f.cnt_20_above,
            f.cnt_7_above,
            f.close_prev1,
            f.close_prev5,
            f.close_prev10,
            f.ma7_prev1,
            f.ma20_prev1,
            f.ma60_prev1,
            f.diff20_prev1,
            f.cnt_20_prev1,
            f.cnt_7_prev1,
            l.ret20,
            l.up20_label,
            l.train_mask_cls,
            l.turn_up_label,
            l.turn_down_label,
            l.train_mask_turn
        FROM ml_feature_daily f
        JOIN ml_label_20d l ON f.code = l.code AND f.dt = l.dt
        WHERE
        """
        + where_sql
        + """
        ORDER BY f.dt, f.code
        """
    )
    return conn.execute(sql, params).df()


def _add_derived_features(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    ma20 = pd.to_numeric(result.get("ma20"), errors="coerce")
    ma60 = pd.to_numeric(result.get("ma60"), errors="coerce")
    ma7 = pd.to_numeric(result.get("ma7"), errors="coerce")
    close = pd.to_numeric(result.get("close"), errors="coerce")
    close_prev1 = pd.to_numeric(result.get("close_prev1"), errors="coerce")
    close_prev5 = pd.to_numeric(result.get("close_prev5"), errors="coerce")
    close_prev10 = pd.to_numeric(result.get("close_prev10"), errors="coerce")
    ma7_prev1 = pd.to_numeric(result.get("ma7_prev1"), errors="coerce")
    ma20_prev1 = pd.to_numeric(result.get("ma20_prev1"), errors="coerce")
    ma60_prev1 = pd.to_numeric(result.get("ma60_prev1"), errors="coerce")
    diff20_prev1 = pd.to_numeric(result.get("diff20_prev1"), errors="coerce")
    cnt_7_above = pd.to_numeric(result.get("cnt_7_above"), errors="coerce")
    cnt_20_above = pd.to_numeric(result.get("cnt_20_above"), errors="coerce")

    result["dist_ma20"] = (close - ma20) / ma20.replace(0, np.nan)
    result["dist_ma60"] = (close - ma60) / ma60.replace(0, np.nan)
    result["ma7_ma20_gap"] = (ma7 - ma20) / ma20.replace(0, np.nan)
    result["ma20_ma60_gap"] = (ma20 - ma60) / ma60.replace(0, np.nan)
    result["close_ret1"] = (close - close_prev1) / close_prev1.replace(0, np.nan)
    result["close_ret5"] = (close - close_prev5) / close_prev5.replace(0, np.nan)
    result["close_ret10"] = (close - close_prev10) / close_prev10.replace(0, np.nan)
    result["ma7_slope1"] = (ma7 - ma7_prev1) / ma7_prev1.replace(0, np.nan)
    result["ma20_slope1"] = (ma20 - ma20_prev1) / ma20_prev1.replace(0, np.nan)
    result["ma60_slope1"] = (ma60 - ma60_prev1) / ma60_prev1.replace(0, np.nan)
    result["ma20_slope_delta1"] = result["ma20_slope1"] - pd.to_numeric(
        result.get("ma20_slope1"), errors="coerce"
    ).groupby(result.get("code")).shift(1)
    result["dist_ma20_delta1"] = result["dist_ma20"] - diff20_prev1
    result["cnt_7_above_norm"] = cnt_7_above / 7.0
    result["cnt_20_above_norm"] = cnt_20_above / 20.0
    return result


def _prepare_feature_matrix(
    df: pd.DataFrame,
    medians: dict[str, float] | None = None,
    feature_columns: list[str] | None = None,
) -> tuple[pd.DataFrame, dict[str, float]]:
    feat = _add_derived_features(df)
    columns = feature_columns or FEATURE_COLUMNS
    for col in columns:
        if col not in feat.columns:
            feat[col] = np.nan
    matrix = feat[columns].copy()
    matrix = matrix.apply(pd.to_numeric, errors="coerce")
    resolved: dict[str, float] = {}
    if medians is None:
        for col in columns:
            values = matrix[col].to_numpy(dtype=float, copy=True)
            finite = values[np.isfinite(values)]
            resolved[col] = float(np.median(finite)) if finite.size else 0.0
    else:
        for col in columns:
            resolved[col] = float(medians.get(col, 0.0))
    for col in columns:
        matrix[col] = matrix[col].fillna(resolved[col])
    return matrix, resolved


def _fit_models(train_df: pd.DataFrame, cfg: MLConfig) -> TrainedModels:
    lgb = _import_lightgbm()
    if train_df.empty:
        raise RuntimeError("train_df is empty")

    train_df = train_df.copy()
    train_df["ret20"] = pd.to_numeric(train_df["ret20"], errors="coerce")
    train_df["up20_label"] = pd.to_numeric(train_df["up20_label"], errors="coerce")
    train_df["train_mask_cls"] = pd.to_numeric(train_df["train_mask_cls"], errors="coerce")
    train_df["turn_up_label"] = pd.to_numeric(train_df.get("turn_up_label"), errors="coerce").fillna(0)
    train_df["turn_down_label"] = pd.to_numeric(train_df.get("turn_down_label"), errors="coerce").fillna(0)
    train_df["train_mask_turn"] = pd.to_numeric(train_df.get("train_mask_turn"), errors="coerce").fillna(0)
    train_df = train_df[np.isfinite(train_df["ret20"].to_numpy(dtype=float, copy=False))]
    if train_df.empty:
        raise RuntimeError("No valid ret20 rows in training data")

    cls_df = train_df[train_df["train_mask_cls"] == 1].copy()
    if len(cls_df) < 200:
        raise RuntimeError("Insufficient classification rows (train_mask_cls=1)")

    x_cls, medians = _prepare_feature_matrix(cls_df, medians=None, feature_columns=FEATURE_COLUMNS)
    y_cls = cls_df["up20_label"].astype(int).to_numpy()
    cls_train = lgb.Dataset(
        x_cls.to_numpy(dtype=float),
        label=y_cls,
        feature_name=FEATURE_COLUMNS,
        free_raw_data=False,
    )
    cls_params = {
        "objective": "binary",
        "metric": "binary_logloss",
        "learning_rate": 0.05,
        "num_leaves": 31,
        "feature_fraction": 0.9,
        "bagging_fraction": 0.9,
        "bagging_freq": 1,
        "min_data_in_leaf": 40,
        "verbosity": -1,
    }
    cls_model = lgb.train(cls_params, cls_train, num_boost_round=int(cfg.cls_boost_round))

    x_reg, _ = _prepare_feature_matrix(train_df, medians=medians, feature_columns=FEATURE_COLUMNS)
    y_reg = train_df["ret20"].astype(float).to_numpy()
    reg_train = lgb.Dataset(
        x_reg.to_numpy(dtype=float),
        label=y_reg,
        feature_name=FEATURE_COLUMNS,
        free_raw_data=False,
    )
    reg_params = {
        "objective": "regression",
        "metric": "l2",
        "learning_rate": 0.05,
        "num_leaves": 31,
        "feature_fraction": 0.9,
        "bagging_fraction": 0.9,
        "bagging_freq": 1,
        "min_data_in_leaf": 40,
        "verbosity": -1,
    }
    reg_model = lgb.train(reg_params, reg_train, num_boost_round=int(cfg.reg_boost_round))

    def _fit_turn_binary(label_col: str) -> tuple[Any | None, int]:
        turn_df = train_df[train_df["train_mask_turn"] == 1].copy()
        if len(turn_df) < 400:
            return None, int(len(turn_df))
        y = turn_df[label_col].astype(int).to_numpy()
        positives = int(np.sum(y == 1))
        negatives = int(np.sum(y == 0))
        if positives < 80 or negatives < 80:
            return None, int(len(turn_df))
        x_turn, _ = _prepare_feature_matrix(turn_df, medians=medians, feature_columns=FEATURE_COLUMNS)
        turn_train = lgb.Dataset(
            x_turn.to_numpy(dtype=float),
            label=y,
            feature_name=FEATURE_COLUMNS,
            free_raw_data=False,
        )
        turn_params = {
            "objective": "binary",
            "metric": "binary_logloss",
            "learning_rate": 0.04,
            "num_leaves": 63,
            "feature_fraction": 0.9,
            "bagging_fraction": 0.9,
            "bagging_freq": 1,
            "min_data_in_leaf": 60,
            "is_unbalance": True,
            "verbosity": -1,
        }
        model = lgb.train(turn_params, turn_train, num_boost_round=int(cfg.cls_boost_round))
        return model, int(len(turn_df))

    turn_up_model, n_turn_up = _fit_turn_binary("turn_up_label")
    turn_down_model, n_turn_down = _fit_turn_binary("turn_down_label")

    return TrainedModels(
        cls=cls_model,
        reg=reg_model,
        turn_up=turn_up_model,
        turn_down=turn_down_model,
        feature_columns=list(FEATURE_COLUMNS),
        medians=medians,
        n_train_cls=len(cls_df),
        n_train_reg=len(train_df),
        n_train_turn_up=n_turn_up,
        n_train_turn_down=n_turn_down,
    )


def _predict_frame(df: pd.DataFrame, models: TrainedModels, cfg: MLConfig) -> pd.DataFrame:
    matrix, _ = _prepare_feature_matrix(
        df,
        medians=models.medians,
        feature_columns=models.feature_columns,
    )
    pred = df.copy()
    matrix_np = matrix.to_numpy(dtype=float)
    pred["p_up"] = models.cls.predict(matrix_np)
    pred["ret_pred20"] = models.reg.predict(matrix_np)
    pred["ev20"] = pred["ret_pred20"]
    pred["ev20_net"] = pred["ev20"].apply(lambda v: compute_ev20_net(float(v), cfg.cost_rate))
    if models.turn_up is not None:
        pred["p_turn_up"] = models.turn_up.predict(matrix_np)
    else:
        pred["p_turn_up"] = pred["p_up"]
    if models.turn_down is not None:
        pred["p_turn_down"] = models.turn_down.predict(matrix_np)
    else:
        pred["p_turn_down"] = 1.0 - pd.to_numeric(pred["p_up"], errors="coerce").fillna(0.5)
    return pred


def select_top_n_ml(
    items: list[dict[str, Any]],
    top_n: int,
    p_up_threshold: float,
    direction: str = "up",
) -> list[dict[str, Any]]:
    def _ev_value(item: dict[str, Any]) -> float | None:
        return _safe_float(item.get("ev20_net")) or _safe_float(item.get("mlEv20Net"))

    def _pup_value(item: dict[str, Any]) -> float | None:
        return _safe_float(item.get("p_up")) or _safe_float(item.get("mlPUp"))

    if top_n <= 0:
        return []
    valid = [item for item in items if _ev_value(item) is not None]
    if not valid:
        return []

    if direction == "down":
        ordered = sorted(
            valid,
            key=lambda item: (
                float(_ev_value(item) or 0.0),
                item.get("code") or "",
            ),
        )
        return ordered[:top_n]

    preferred = [
        item
        for item in valid
        if _pup_value(item) is not None and float(_pup_value(item) or 0.0) >= p_up_threshold
    ]
    preferred_sorted = sorted(
        preferred,
        key=lambda item: (-(float(_ev_value(item) or 0.0)), item.get("code") or ""),
    )
    selected = preferred_sorted[:top_n]
    if len(selected) >= top_n:
        return selected

    selected_codes = {str(item.get("code")) for item in selected}
    remaining = [item for item in valid if str(item.get("code")) not in selected_codes]
    remaining_sorted = sorted(
        remaining,
        key=lambda item: (-(float(_ev_value(item) or 0.0)), item.get("code") or ""),
    )
    selected.extend(remaining_sorted[: max(0, top_n - len(selected))])
    return selected


def _walk_forward_eval(df: pd.DataFrame, cfg: MLConfig) -> dict[str, Any]:
    if df.empty:
        return {
            "fold_count": 0,
            "daily_count": 0,
            "top30_mean_ret20_net": None,
            "top30_win_rate": None,
            "top30_median_ret20_net": None,
            "top30_p05_ret20_net": None,
            "turn_long_mean_ret10_proxy_net": None,
            "turn_long_win_rate": None,
            "turn_short_mean_ret10_proxy_net": None,
            "turn_short_win_rate": None,
            "folds": [],
        }

    all_dates = sorted(int(v) for v in pd.Series(df["dt"]).dropna().unique().tolist())
    windows = build_walk_forward_windows(all_dates, cfg)
    if not windows:
        return {
            "fold_count": 0,
            "daily_count": 0,
            "top30_mean_ret20_net": None,
            "top30_win_rate": None,
            "top30_median_ret20_net": None,
            "top30_p05_ret20_net": None,
            "turn_long_mean_ret10_proxy_net": None,
            "turn_long_win_rate": None,
            "turn_short_mean_ret10_proxy_net": None,
            "turn_short_win_rate": None,
            "folds": [],
        }

    daily_scores: list[float] = []
    daily_turn_long_scores: list[float] = []
    daily_turn_short_scores: list[float] = []
    fold_rows: list[dict[str, Any]] = []
    for window in windows:
        train_dates = set(window["train_dates"])
        test_dates = window["test_dates"]
        train_df = df[df["dt"].isin(train_dates)].copy()
        test_df = df[df["dt"].isin(test_dates)].copy()
        if train_df.empty or test_df.empty:
            continue

        try:
            models = _fit_models(train_df, cfg)
        except Exception:
            continue

        pred_df = _predict_frame(test_df, models, cfg)
        fold_daily: list[float] = []
        fold_turn_long: list[float] = []
        fold_turn_short: list[float] = []
        for dt_value, group in pred_df.groupby("dt"):
            selected = select_top_n_ml(
                group.to_dict(orient="records"),
                top_n=int(cfg.top_n),
                p_up_threshold=float(cfg.p_up_threshold),
                direction="up",
            )
            if not selected:
                continue
            realized = []
            for item in selected:
                ret20 = _safe_float(item.get("ret20"))
                if ret20 is None:
                    continue
                realized.append(compute_ev20_net(ret20, cfg.cost_rate))
            if not realized:
                continue
            day_score = float(np.mean(realized))
            daily_scores.append(day_score)
            fold_daily.append(day_score)

            turn_long = (
                group.sort_values(["p_turn_up", "code"], ascending=[False, True])
                .head(int(cfg.top_n))
                .to_dict(orient="records")
            )
            turn_long_realized = []
            for item in turn_long:
                ret20 = _safe_float(item.get("ret20"))
                if ret20 is None:
                    continue
                turn_long_realized.append(compute_ev20_net(ret20, cfg.cost_rate))
            if turn_long_realized:
                turn_long_score = float(np.mean(turn_long_realized))
                daily_turn_long_scores.append(turn_long_score)
                fold_turn_long.append(turn_long_score)

            turn_short = (
                group.sort_values(["p_turn_down", "code"], ascending=[False, True])
                .head(int(cfg.top_n))
                .to_dict(orient="records")
            )
            turn_short_realized = []
            for item in turn_short:
                ret20 = _safe_float(item.get("ret20"))
                if ret20 is None:
                    continue
                turn_short_realized.append(compute_ev20_net(-ret20, cfg.cost_rate))
            if turn_short_realized:
                turn_short_score = float(np.mean(turn_short_realized))
                daily_turn_short_scores.append(turn_short_score)
                fold_turn_short.append(turn_short_score)

        fold_rows.append(
            {
                "train_start_dt": int(window["train_start_dt"]),
                "train_end_dt": int(window["train_end_dt"]),
                "test_start_dt": int(window["test_start_dt"]),
                "test_end_dt": int(window["test_end_dt"]),
                "embargo_days": int(window["embargo_days"]),
                "daily_count": len(fold_daily),
                "mean_ret20_net": float(np.mean(fold_daily)) if fold_daily else None,
                "turn_long_mean_ret10_proxy_net": float(np.mean(fold_turn_long)) if fold_turn_long else None,
                "turn_short_mean_ret10_proxy_net": float(np.mean(fold_turn_short)) if fold_turn_short else None,
            }
        )

    if not daily_scores:
        return {
            "fold_count": len(fold_rows),
            "daily_count": 0,
            "top30_mean_ret20_net": None,
            "top30_win_rate": None,
            "top30_median_ret20_net": None,
            "top30_p05_ret20_net": None,
            "turn_long_mean_ret10_proxy_net": None,
            "turn_long_win_rate": None,
            "turn_short_mean_ret10_proxy_net": None,
            "turn_short_win_rate": None,
            "folds": fold_rows,
        }

    arr = np.array(daily_scores, dtype=float)
    arr_turn_long = np.array(daily_turn_long_scores, dtype=float) if daily_turn_long_scores else np.array([], dtype=float)
    arr_turn_short = np.array(daily_turn_short_scores, dtype=float) if daily_turn_short_scores else np.array([], dtype=float)
    return {
        "fold_count": len(fold_rows),
        "daily_count": int(arr.size),
        "top30_mean_ret20_net": float(np.mean(arr)),
        "top30_win_rate": float(np.mean(arr > 0)),
        "top30_median_ret20_net": float(np.median(arr)),
        "top30_p05_ret20_net": float(np.percentile(arr, 5)),
        "turn_long_mean_ret10_proxy_net": float(np.mean(arr_turn_long)) if arr_turn_long.size else None,
        "turn_long_win_rate": float(np.mean(arr_turn_long > 0)) if arr_turn_long.size else None,
        "turn_short_mean_ret10_proxy_net": float(np.mean(arr_turn_short)) if arr_turn_short.size else None,
        "turn_short_win_rate": float(np.mean(arr_turn_short > 0)) if arr_turn_short.size else None,
        "folds": fold_rows,
    }


def build_walk_forward_windows(all_dates: list[int], cfg: MLConfig) -> list[dict[str, Any]]:
    need = int(cfg.train_days + cfg.embargo_days + cfg.test_days)
    if len(all_dates) < need:
        return []
    windows: list[dict[str, Any]] = []
    start_idx = 0
    while True:
        train_start_idx = start_idx
        train_end_idx = train_start_idx + int(cfg.train_days) - 1
        test_start_idx = train_end_idx + int(cfg.embargo_days) + 1
        test_end_idx = test_start_idx + int(cfg.test_days) - 1
        if test_end_idx >= len(all_dates):
            break
        windows.append(
            {
                "train_dates": all_dates[train_start_idx : train_end_idx + 1],
                "test_dates": all_dates[test_start_idx : test_end_idx + 1],
                "train_start_dt": int(all_dates[train_start_idx]),
                "train_end_dt": int(all_dates[train_end_idx]),
                "test_start_dt": int(all_dates[test_start_idx]),
                "test_end_dt": int(all_dates[test_end_idx]),
                "embargo_days": int(cfg.embargo_days),
            }
        )
        start_idx += int(cfg.step_days)
    return windows


def _artifact_dir() -> Path:
    path = Path(core_config.DATA_DIR) / "models" / "ml"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _load_active_model_row(conn) -> tuple | None:
    if not _table_exists(conn, "ml_model_registry"):
        return None
    return conn.execute(
        """
        SELECT
            model_version,
            model_key,
            objective,
            feature_version,
            label_version,
            train_start_dt,
            train_end_dt,
            metrics_json,
            artifact_path,
            n_train,
            created_at
        FROM ml_model_registry
        WHERE is_active = TRUE
        ORDER BY created_at DESC
        LIMIT 1
        """
    ).fetchone()


def _save_registry_row(
    conn,
    *,
    model_version: str,
    metrics: dict[str, Any],
    artifact_path: str,
    n_train: int,
    train_start_dt: int | None,
    train_end_dt: int | None,
) -> None:
    conn.execute("UPDATE ml_model_registry SET is_active = FALSE WHERE model_key = ?", [MODEL_KEY])
    conn.execute(
        """
        INSERT INTO ml_model_registry (
            model_version,
            model_key,
            objective,
            feature_version,
            label_version,
            train_start_dt,
            train_end_dt,
            metrics_json,
            artifact_path,
            n_train,
            created_at,
            is_active
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, TRUE)
        """,
        [
            model_version,
            MODEL_KEY,
            OBJECTIVE,
            FEATURE_VERSION,
            LABEL_VERSION,
            int(train_start_dt) if train_start_dt is not None else None,
            int(train_end_dt) if train_end_dt is not None else None,
            json.dumps(metrics, ensure_ascii=False),
            artifact_path,
            int(n_train),
        ],
    )


def train_models(
    *,
    start_dt: int | None = None,
    end_dt: int | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    cfg = load_ml_config()
    with get_conn() as conn:
        _ensure_ml_schema(conn)
        feature_rows = refresh_ml_feature_table(
            conn,
            feature_version=FEATURE_VERSION,
            start_dt=start_dt,
            end_dt=end_dt,
        )
        label_rows = refresh_ml_label_table(
            conn,
            cfg=cfg,
            label_version=LABEL_VERSION,
            start_dt=start_dt,
            end_dt=end_dt,
        )
        df = _load_training_df(conn, start_dt=start_dt, end_dt=end_dt)
        if df.empty:
            raise RuntimeError("No joined rows for ML training")

        wf_metrics = _walk_forward_eval(df, cfg)
        models = _fit_models(df, cfg)

        model_version = datetime.now(tz=timezone.utc).strftime("%Y%m%d%H%M%S")
        payload = {
            "feature_rows": int(feature_rows),
            "label_rows": int(label_rows),
            "train_rows": int(len(df)),
            "n_train_cls": int(models.n_train_cls),
            "n_train_reg": int(models.n_train_reg),
            "n_train_turn_up": int(models.n_train_turn_up),
            "n_train_turn_down": int(models.n_train_turn_down),
            "model_version": model_version,
            "walk_forward": wf_metrics,
        }
        metrics_json = {
            **payload,
            "feature_columns": FEATURE_COLUMNS,
            "medians": models.medians,
            "config": {
                "neutral_band_pct": cfg.neutral_band_pct,
                "p_up_threshold": cfg.p_up_threshold,
                "top_n": cfg.top_n,
                "cost_bps": cfg.cost_bps,
                "train_days": cfg.train_days,
                "test_days": cfg.test_days,
                "step_days": cfg.step_days,
                "embargo_days": cfg.embargo_days,
                "rule_weight": cfg.rule_weight,
                "ev_weight": cfg.ev_weight,
                "prob_weight": cfg.prob_weight,
                "turn_weight": cfg.turn_weight,
                "min_prob_up": cfg.min_prob_up,
                "min_prob_down": cfg.min_prob_down,
                "min_turn_prob_up": cfg.min_turn_prob_up,
                "min_turn_prob_down": cfg.min_turn_prob_down,
                "min_turn_margin": cfg.min_turn_margin,
            },
        }
        if dry_run:
            return {
                **payload,
                "dry_run": True,
            }

        art_dir = _artifact_dir()
        cls_path = art_dir / f"{model_version}_cls.txt"
        reg_path = art_dir / f"{model_version}_reg.txt"
        turn_up_path = art_dir / f"{model_version}_turn_up.txt"
        turn_down_path = art_dir / f"{model_version}_turn_down.txt"
        models.cls.save_model(str(cls_path))
        models.reg.save_model(str(reg_path))
        if models.turn_up is not None:
            models.turn_up.save_model(str(turn_up_path))
        if models.turn_down is not None:
            models.turn_down.save_model(str(turn_down_path))
        artifact = {
            "cls_model_path": str(cls_path),
            "reg_model_path": str(reg_path),
            "turn_up_model_path": str(turn_up_path) if models.turn_up is not None else None,
            "turn_down_model_path": str(turn_down_path) if models.turn_down is not None else None,
        }
        train_start = int(df["dt"].min()) if not df.empty else None
        train_end = int(df["dt"].max()) if not df.empty else None
        _save_registry_row(
            conn,
            model_version=model_version,
            metrics=metrics_json,
            artifact_path=json.dumps(artifact, ensure_ascii=False),
            n_train=models.n_train_reg,
            train_start_dt=train_start,
            train_end_dt=train_end,
        )
        return {
            **payload,
            "dry_run": False,
            "artifact": artifact,
        }


def _load_models_from_registry(conn) -> tuple[TrainedModels, str, int]:
    row = _load_active_model_row(conn)
    if not row:
        raise RuntimeError("No active model in ml_model_registry")
    (
        model_version,
        _model_key,
        _objective,
        _feature_version,
        _label_version,
        _train_start_dt,
        _train_end_dt,
        metrics_json_raw,
        artifact_path_raw,
        n_train,
        _created_at,
    ) = row
    try:
        metrics_json = json.loads(metrics_json_raw) if metrics_json_raw else {}
    except Exception:
        metrics_json = {}
    medians = metrics_json.get("medians") or {}
    try:
        artifact = json.loads(artifact_path_raw) if artifact_path_raw else {}
    except Exception:
        artifact = {}
    cls_model_path = artifact.get("cls_model_path")
    reg_model_path = artifact.get("reg_model_path")
    if not cls_model_path or not reg_model_path:
        raise RuntimeError("Model artifact path is invalid")
    turn_up_model_path = artifact.get("turn_up_model_path")
    turn_down_model_path = artifact.get("turn_down_model_path")
    lgb = _import_lightgbm()
    cls_model = lgb.Booster(model_file=str(cls_model_path))
    reg_model = lgb.Booster(model_file=str(reg_model_path))
    metric_cols = metrics_json.get("feature_columns")
    model_feature_columns = (
        [str(c) for c in metric_cols if isinstance(c, str) and c]
        if isinstance(metric_cols, list)
        else []
    )
    if not model_feature_columns:
        try:
            n_features = int(cls_model.num_feature())
        except Exception:
            n_features = len(FEATURE_COLUMNS)
        model_feature_columns = list(FEATURE_COLUMNS[: max(1, min(n_features, len(FEATURE_COLUMNS)))])
    turn_up_model = (
        lgb.Booster(model_file=str(turn_up_model_path))
        if turn_up_model_path and Path(str(turn_up_model_path)).exists()
        else None
    )
    turn_down_model = (
        lgb.Booster(model_file=str(turn_down_model_path))
        if turn_down_model_path and Path(str(turn_down_model_path)).exists()
        else None
    )
    return (
        TrainedModels(
            cls=cls_model,
            reg=reg_model,
            turn_up=turn_up_model,
            turn_down=turn_down_model,
            feature_columns=model_feature_columns,
            medians={str(k): float(v) for k, v in medians.items()},
            n_train_cls=int(metrics_json.get("n_train_cls") or 0),
            n_train_reg=int(metrics_json.get("n_train_reg") or n_train or 0),
            n_train_turn_up=int(metrics_json.get("n_train_turn_up") or 0),
            n_train_turn_down=int(metrics_json.get("n_train_turn_down") or 0),
        ),
        str(model_version),
        int(n_train or 0),
    )


def predict_for_dt(dt: int | None = None) -> dict[str, Any]:
    cfg = load_ml_config()
    with get_conn() as conn:
        _ensure_ml_schema(conn)
        if conn.execute("SELECT COUNT(*) FROM ml_feature_daily").fetchone()[0] == 0:
            refresh_ml_feature_table(conn, feature_version=FEATURE_VERSION)

        target_dt = int(dt) if dt is not None else None
        if target_dt is None:
            row = conn.execute("SELECT MAX(dt) FROM ml_feature_daily").fetchone()
            if not row or row[0] is None:
                raise RuntimeError("ml_feature_daily is empty")
            target_dt = int(row[0])

        frame = conn.execute(
            """
            SELECT
                dt,
                code,
                close,
                ma7,
                ma20,
                ma60,
                atr14,
                diff20_pct,
                cnt_20_above,
                cnt_7_above,
                close_prev1,
                close_prev5,
                close_prev10,
                ma7_prev1,
                ma20_prev1,
                ma60_prev1,
                diff20_prev1,
                cnt_20_prev1,
                cnt_7_prev1
            FROM ml_feature_daily
            WHERE dt = ?
            ORDER BY code
            """,
            [target_dt],
        ).df()
        if frame.empty:
            raise RuntimeError(f"No features found for dt={target_dt}")

        models, model_version, n_train = _load_models_from_registry(conn)
        pred = _predict_frame(frame, models, cfg)
        rows = [
            (
                int(target_dt),
                str(item.code),
                float(item.p_up),
                float(item.p_turn_up),
                float(item.p_turn_down),
                float(item.ret_pred20),
                float(item.ev20),
                float(item.ev20_net),
                str(model_version),
                int(n_train),
            )
            for item in pred.itertuples(index=False)
        ]
        conn.execute("DELETE FROM ml_pred_20d WHERE dt = ?", [target_dt])
        if rows:
            conn.executemany(
                """
                INSERT INTO ml_pred_20d (
                    dt,
                    code,
                    p_up,
                    p_turn_up,
                    p_turn_down,
                    ret_pred20,
                    ev20,
                    ev20_net,
                    model_version,
                    n_train,
                    computed_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                rows,
            )
        return {
            "dt": int(target_dt),
            "rows": int(len(rows)),
            "model_version": model_version,
        }


def predict_latest() -> dict[str, Any]:
    return predict_for_dt(dt=None)


def get_ml_status() -> dict[str, Any]:
    cfg = load_ml_config()
    with get_conn() as conn:
        _ensure_ml_schema(conn)
        active = _load_active_model_row(conn)
        latest_pred = None
        if _table_exists(conn, "ml_pred_20d"):
            latest_pred = conn.execute(
                """
                SELECT dt, model_version, COUNT(*) AS n
                FROM ml_pred_20d
                GROUP BY dt, model_version
                ORDER BY dt DESC
                LIMIT 1
                """
            ).fetchone()
        payload: dict[str, Any] = {
            "has_active_model": bool(active),
            "config": {
                "neutral_band_pct": cfg.neutral_band_pct,
                "p_up_threshold": cfg.p_up_threshold,
                "top_n": cfg.top_n,
                "cost_bps": cfg.cost_bps,
                "train_days": cfg.train_days,
                "test_days": cfg.test_days,
                "step_days": cfg.step_days,
                "embargo_days": cfg.embargo_days,
                "rule_weight": cfg.rule_weight,
                "ev_weight": cfg.ev_weight,
                "prob_weight": cfg.prob_weight,
                "turn_weight": cfg.turn_weight,
                "min_prob_up": cfg.min_prob_up,
                "min_prob_down": cfg.min_prob_down,
                "min_turn_prob_up": cfg.min_turn_prob_up,
                "min_turn_prob_down": cfg.min_turn_prob_down,
                "min_turn_margin": cfg.min_turn_margin,
            },
        }
        if active:
            payload["active_model"] = {
                "model_version": active[0],
                "model_key": active[1],
                "objective": active[2],
                "feature_version": active[3],
                "label_version": active[4],
                "train_start_dt": active[5],
                "train_end_dt": active[6],
                "n_train": active[9],
                "created_at": active[10],
            }
            try:
                payload["metrics"] = json.loads(active[7]) if active[7] else {}
            except Exception:
                payload["metrics"] = {}
        else:
            payload["active_model"] = None
            payload["metrics"] = {}

        if latest_pred:
            payload["latest_prediction"] = {
                "dt": int(latest_pred[0]),
                "model_version": latest_pred[1],
                "rows": int(latest_pred[2]),
            }
        else:
            payload["latest_prediction"] = None
        return payload
