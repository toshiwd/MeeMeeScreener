from __future__ import annotations

import json
import logging
import math
from bisect import bisect_right
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd

from app.backend.core.legacy_analysis_control import (
    is_legacy_analysis_disabled,
    legacy_analysis_disabled_log_value,
    legacy_analysis_disabled_payload,
)
from app.core.config import config as core_config
from app.db.session import get_conn
from . import legacy_predict_runtime
from . import legacy_train_runtime
from .ml_config import MLConfig, load_ml_config
from .legacy_schema_runtime import ensure_ml_runtime_schema

logger = logging.getLogger(__name__)

FEATURE_VERSION = 4
LABEL_VERSION = 4
MODEL_KEY = "ml_ev20_simple_v1"
OBJECTIVE = "dual_sided_lambdarank_v1"
MONTHLY_MODEL_KEY = "ml_monthly_abs_dir_1m_v1"
MONTHLY_OBJECTIVE = "monthly_abs_dir_1m_v1"
MONTHLY_LABEL_VERSION = 1
MONTHLY_LABEL_QUANTILE = 0.10
MONTHLY_LIQUIDITY_BOTTOM_RATIO = 0.30
MONTHLY_GATE_ABS_CANDIDATES: tuple[float, ...] = (0.35, 0.32, 0.30, 0.28, 0.25, 0.22, 0.20, 0.18, 0.15)
MONTHLY_GATE_SIDE_CANDIDATES: tuple[float, ...] = (0.30, 0.25, 0.22, 0.20, 0.18, 0.16, 0.14, 0.12, 0.10)
MONTHLY_GATE_MIN_MONTH_COVERAGE = 0.40
MONTHLY_GATE_MIN_AVG_PICKS = 2.0
MONTHLY_GATE_TARGET_AVG_PICKS = 8.0
MONTHLY_RET20_TARGET = 0.20
MONTHLY_RET20_BIN_QUANTILES: tuple[float, ...] = (0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)
MONTHLY_RET20_MIN_BIN_SAMPLES = 80
MONTHLY_TARGET20_GATE_CANDIDATES: tuple[float, ...] = (
    0.06,
    0.08,
    0.10,
    0.12,
    0.14,
    0.16,
    0.18,
    0.20,
    0.22,
    0.24,
    0.26,
    0.28,
    0.30,
    0.32,
)
MONTHLY_TARGET20_MIN_MONTH_COVERAGE = 0.30
MONTHLY_TARGET20_MIN_AVG_PICKS = 1.0
MONTHLY_TARGET20_TARGET_AVG_PICKS = 4.0
MONTHLY_TARGET20_MIN_LIFT = 1.10
TURN_HORIZON_DAYS = 10
TURN_UP_TARGET_PCT = 0.06
TURN_UP_STOP_PCT = 0.03
SHORT_STOP_PCT = 0.05
SHORT_TARGET_PCT_5D = 0.05
SHORT_TARGET_PCT_10D = 0.10
SHORT_TARGET_PCT_20D = 0.10
SHORT_COUNTER_RANGE_LOOKBACK = 20
SHORT_COUNTER_RANGE_POS_MIN = 0.62
SHORT_COUNTER_MA20_TOLERANCE = 0.02
SHORT_SUPPORT_LOOKBACK = 10
SHORT_SUPPORT_BREAK_BUFFER = 0.0015
SHORT_SUPPORT_PREV_TOLERANCE = 0.002
PREDICTION_HORIZONS: tuple[int, ...] = (5, 10, 20)
LIQ_COST_TURNOVER_LOW = 50_000_000.0
LIQ_COST_TURNOVER_MID = 200_000_000.0
LIQ_SLIPPAGE_BPS_LOW = 14.0
LIQ_SLIPPAGE_BPS_MID = 7.0
LIQ_SLIPPAGE_BPS_HIGH = 2.0
LIQ_SLIPPAGE_BPS_UNKNOWN = 18.0
SHORT_BORROW_BPS_20D = 6.0

RET_COL_BY_HORIZON: dict[int, str] = {
    5: "ret5",
    10: "ret10",
    20: "ret20",
}


def _legacy_monthly_prediction_disabled_result(dt: int | None = None) -> dict[str, Any]:
    payload = legacy_analysis_disabled_payload(job_type="ml_predict", source="ml_service.predict_monthly_for_dt")
    payload.update(
        {
            "dt": int(dt) if dt is not None else None,
            "pred_dt": None,
            "rows": 0,
            "model_version": None,
            "n_train_abs": 0,
            "n_train_dir": 0,
            "disabled_reason": "legacy_analysis_disabled",
        }
    )
    return payload


def _legacy_bulk_prediction_disabled_result(requested_dates: list[int]) -> dict[str, Any]:
    payload = legacy_analysis_disabled_payload(job_type="ml_predict", source="ml_service.predict_for_dates_bulk")
    payload.update(
        {
            "requested_dates": [int(value) for value in requested_dates],
            "resolved_dates": [],
            "predicted_dates": [],
            "rows_total": 0,
            "model_version": None,
            "n_train": 0,
            "skipped_dates": [int(value) for value in requested_dates],
            "monthly": None,
        }
    )
    return payload


def _legacy_prediction_disabled_result(dt: int | None = None) -> dict[str, Any]:
    payload = legacy_analysis_disabled_payload(job_type="ml_predict", source="ml_service.predict_for_dt")
    payload.update(
        {
            "dt": int(dt) if dt is not None else None,
            "rows": 0,
            "model_version": None,
            "monthly": _legacy_monthly_prediction_disabled_result(dt),
        }
    )
    return payload
UP_LABEL_COL_BY_HORIZON: dict[int, str] = {
    5: "up5_label",
    10: "up10_label",
    20: "up20_label",
}
CLS_MASK_COL_BY_HORIZON: dict[int, str] = {
    5: "train_mask_cls_5",
    10: "train_mask_cls_10",
    20: "train_mask_cls",
}
TURN_DOWN_COL_BY_HORIZON: dict[int, str] = {
    5: "turn_down_label_5",
    10: "turn_down_label",
    20: "turn_down_label_20",
}
TURN_MASK_COL_BY_HORIZON: dict[int, str] = {
    5: "train_mask_turn_5",
    10: "train_mask_turn",
    20: "train_mask_turn_20",
}

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
    "weekly_breakout_up_prob",
    "weekly_breakout_down_prob",
    "weekly_range_prob",
    "monthly_breakout_up_prob",
    "monthly_breakout_down_prob",
    "monthly_range_prob",
    "candle_triplet_up_prob",
    "candle_triplet_down_prob",
    "candle_body_ratio",
    "candle_upper_wick_ratio",
    "candle_lower_wick_ratio",
    "atr14_pct",
    "range_pct",
    "gap_pct",
    "close_ret2",
    "close_ret3",
    "close_ret20",
    "close_ret60",
    "vol_ret5",
    "vol_ret20",
    "vol_ratio5_20",
    "turnover20",
    "turnover_z20",
    "high20_dist",
    "low20_dist",
    "breakout20_up",
    "breakout20_down",
    "drawdown60",
    "rebound60",
    "market_ret1",
    "market_ret5",
    "market_ret20",
    "rel_ret5",
    "rel_ret20",
    "breadth_above_ma20",
    "breadth_above_ma60",
    "sector_ret5",
    "sector_ret20",
    "rel_sector_ret5",
    "rel_sector_ret20",
    "sector_breadth_ma20",
    "cal_dow_sin",
    "cal_dow_cos",
    "cal_month_sin",
    "cal_month_cos",
    "cal_month_start",
    "cal_month_end",
]

FEATURE_COLUMNS: list[str] = [*BASE_FEATURE_COLUMNS, *DERIVED_FEATURE_COLUMNS]


@dataclass(frozen=True)
class TrainedModels:
    cls: Any
    reg: Any
    turn_up: Any | None
    turn_down: Any | None
    rank_up: Any | None
    rank_down: Any | None
    cls_by_horizon: dict[int, Any]
    cls_temperature_by_horizon: dict[int, float]
    reg_by_horizon: dict[int, Any]
    turn_down_by_horizon: dict[int, Any | None]
    feature_columns: list[str]
    medians: dict[str, float]
    n_train_cls: int
    n_train_reg: int
    n_train_turn_up: int
    n_train_turn_down: int
    n_train_rank: int
    n_train_rank_groups: int
    n_train_cls_by_horizon: dict[int, int]
    n_train_reg_by_horizon: dict[int, int]
    n_train_turn_down_by_horizon: dict[int, int]


@dataclass(frozen=True)
class MonthlyTrainedModels:
    abs_cls: Any | None
    dir_cls: Any | None
    feature_columns: list[str]
    medians: dict[str, float]
    abs_temperature: float
    dir_temperature: float
    n_train_abs: int
    n_train_dir: int


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
    legacy_schema_enabled = not is_legacy_analysis_disabled()
    ensure_ml_runtime_schema(conn, legacy_schema_enabled=legacy_schema_enabled)


def _safe_float(value: object) -> float | None:
    try:
        f = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if not math.isfinite(f):
        return None
    return f


def _normalize_daily_dt_key(value: int | str | float | None) -> int | None:
    if value is None:
        return None
    try:
        raw = int(value)
    except (TypeError, ValueError):
        return None
    if raw >= 1_000_000_000:
        try:
            return int(datetime.fromtimestamp(raw, tz=timezone.utc).strftime("%Y%m%d"))
        except Exception:
            return None
    if 19_000_101 <= raw <= 21_001_231:
        return raw
    return None


def _normalized_daily_dt_sql(column_name: str) -> str:
    return (
        f"CASE "
        f"WHEN {column_name} >= 1000000000 THEN CAST(strftime(to_timestamp({column_name}), '%Y%m%d') AS BIGINT) "
        f"ELSE {column_name} "
        f"END"
    )


def _yyyymmdd_to_utc_epoch(date_key: int) -> int:
    dt = datetime.strptime(str(int(date_key)), "%Y%m%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def _offset_yyyymmdd(date_key: int, days: int) -> int:
    dt = datetime.strptime(str(int(date_key)), "%Y%m%d").replace(tzinfo=timezone.utc) + timedelta(days=int(days))
    return int(dt.strftime("%Y%m%d"))


def _feature_dt_uses_epoch(conn) -> bool:
    try:
        row = conn.execute("SELECT MAX(dt) FROM ml_feature_daily").fetchone()
    except Exception:
        row = None
    value = row[0] if row and row[0] is not None else None
    if value is None:
        try:
            row = conn.execute("SELECT MAX(dt) FROM feature_snapshot_daily").fetchone()
        except Exception:
            row = None
        value = row[0] if row and row[0] is not None else None
    try:
        return int(value) >= 1_000_000_000 if value is not None else False
    except Exception:
        return False


def _feature_refresh_bounds(conn, *, start_key: int, end_key: int) -> tuple[int, int]:
    end_date = datetime.strptime(str(int(end_key)), "%Y%m%d").replace(tzinfo=timezone.utc)
    start_date = datetime.strptime(str(int(start_key)), "%Y%m%d").replace(tzinfo=timezone.utc) - timedelta(days=365 * 6)
    if _feature_dt_uses_epoch(conn):
        return int(start_date.timestamp()), int(end_date.timestamp())
    return int(start_date.strftime("%Y%m%d")), int(end_date.strftime("%Y%m%d"))


def _feature_input_repair_dates(conn, *, target_date_keys: list[int]) -> list[int]:
    normalized_targets = sorted({int(value) for value in target_date_keys if value is not None})
    if not normalized_targets:
        return []
    placeholders = ", ".join("?" for _ in normalized_targets)
    daily_dt_sql = _normalized_daily_dt_sql("date")
    feature_dt_sql = _normalized_daily_dt_sql("dt")
    daily_rows = conn.execute(
        f"""
        SELECT {daily_dt_sql} AS dt_key, COUNT(DISTINCT code) AS code_count
        FROM daily_bars
        WHERE {daily_dt_sql} IN ({placeholders})
        GROUP BY 1
        """,
        [int(value) for value in normalized_targets],
    ).fetchall()
    feature_rows = conn.execute(
        f"""
        SELECT {feature_dt_sql} AS dt_key, COUNT(DISTINCT code) AS code_count
        FROM feature_snapshot_daily
        WHERE {feature_dt_sql} IN ({placeholders})
        GROUP BY 1
        """,
        [int(value) for value in normalized_targets],
    ).fetchall()
    daily_counts = {int(row[0]): int(row[1]) for row in daily_rows if row and row[0] is not None}
    feature_counts = {int(row[0]): int(row[1]) for row in feature_rows if row and row[0] is not None}
    repair_dates: list[int] = []
    for dt_key in normalized_targets:
        expected = int(daily_counts.get(int(dt_key), 0))
        if expected < 30:
            continue
        actual = int(feature_counts.get(int(dt_key), 0))
        minimum_expected = max(10, int(expected * 0.8))
        if actual < minimum_expected:
            repair_dates.append(int(dt_key))
    return repair_dates


def _rebuild_feature_inputs_from_daily_bars(
    conn,
    *,
    start_dt: int | None = None,
    end_dt: int | None = None,
) -> dict[str, int]:
    from app.backend.ingest_txt import build_daily_ma, build_feature_snapshot_daily

    filters: list[str] = []
    params: list[object] = []
    if start_dt is not None:
        filters.append("date >= ?")
        params.append(int(start_dt))
    if end_dt is not None:
        filters.append("date <= ?")
        params.append(int(end_dt))
    where_sql = f"WHERE {' AND '.join(filters)}" if filters else ""
    daily_rows = conn.execute(
        f"""
        SELECT code, date, o, h, l, c, v
        FROM daily_bars
        {where_sql}
        ORDER BY code, date
        """,
        params,
    ).fetchall()
    if not daily_rows:
        return {"daily_rows": 0, "daily_ma_rows": 0, "feature_rows": 0}

    daily = pd.DataFrame(daily_rows, columns=["code", "date", "o", "h", "l", "c", "v"])
    daily_ma = build_daily_ma(daily)
    feature_snapshot = build_feature_snapshot_daily(daily, daily_ma)

    delete_filters: list[str] = []
    delete_params: list[object] = []
    if start_dt is not None:
        delete_filters.append("date >= ?")
        delete_params.append(int(start_dt))
    if end_dt is not None:
        delete_filters.append("date <= ?")
        delete_params.append(int(end_dt))
    delete_where_sql = f"WHERE {' AND '.join(delete_filters)}" if delete_filters else ""
    feature_delete_where_sql = delete_where_sql.replace("date", "dt")

    if delete_where_sql:
        conn.execute(f"DELETE FROM daily_ma {delete_where_sql}", delete_params)
        conn.execute(f"DELETE FROM feature_snapshot_daily {feature_delete_where_sql}", delete_params)
    else:
        conn.execute("DELETE FROM daily_ma")
        conn.execute("DELETE FROM feature_snapshot_daily")

    conn.register("repair_daily_ma_df", daily_ma)
    conn.register("repair_feature_snapshot_df", feature_snapshot)
    try:
        conn.execute("INSERT INTO daily_ma SELECT code, date, ma7, ma20, ma60 FROM repair_daily_ma_df")
        conn.execute(
            """
            INSERT INTO feature_snapshot_daily (
                dt,
                code,
                close,
                ma7,
                ma20,
                ma60,
                atr14,
                diff20_pct,
                diff20_atr,
                cnt_20_above,
                cnt_7_above,
                day_count,
                candle_flags
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
                diff20_atr,
                cnt_20_above,
                cnt_7_above,
                day_count,
                candle_flags
            FROM repair_feature_snapshot_df
            """
        )
    finally:
        conn.unregister("repair_daily_ma_df")
        conn.unregister("repair_feature_snapshot_df")

    return {
        "daily_rows": int(len(daily)),
        "daily_ma_rows": int(len(daily_ma)),
        "feature_rows": int(len(feature_snapshot)),
    }


def _to_month_start_int(value: int | str | float | None) -> int | None:
    if value is None:
        return None
    try:
        raw = int(value)
    except (TypeError, ValueError):
        return None
    if raw >= 1_000_000_000:
        try:
            dt = datetime.fromtimestamp(raw, tz=timezone.utc)
            return int(dt.year * 10_000 + dt.month * 100 + 1)
        except Exception:
            return None
    if 10_000_000 <= raw <= 99_991_231:
        year = raw // 10_000
        month = (raw // 100) % 100
        if 1 <= month <= 12:
            return int(year * 10_000 + month * 100 + 1)
        return None
    if 100_000 <= raw <= 999_912:
        year = raw // 100
        month = raw % 100
        if 1 <= month <= 12:
            return int(year * 10_000 + month * 100 + 1)
    return None


def _month_start_to_yyyymm(value: int | None) -> int | None:
    if value is None:
        return None
    raw = int(value)
    if raw < 10_001_01:
        return None
    year = raw // 10_000
    month = (raw // 100) % 100
    if not (1 <= month <= 12):
        return None
    return int(year * 100 + month)


def _summarize_daily_scores(scores: list[float]) -> dict[str, Any]:
    if not scores:
        return {
            "daily_count": 0,
            "top30_mean_ret20_net": None,
            "top30_win_rate": None,
            "top30_median_ret20_net": None,
            "top30_p05_ret20_net": None,
            "top30_cvar05_ret20_net": None,
            "top30_lcb95_ret20_net": None,
            "top30_p_value_mean_gt0": None,
        }
    arr = np.array(scores, dtype=float)
    mean_ret = float(np.mean(arr))
    std_ret = float(np.std(arr, ddof=1)) if arr.size >= 2 else 0.0
    sem = (std_ret / math.sqrt(float(arr.size))) if arr.size >= 2 else 0.0
    if sem > 0:
        z = mean_ret / sem
        p_value_mean_gt0 = float(0.5 * (1.0 - math.erf(z / math.sqrt(2.0))))
        lcb95_ret20 = float(mean_ret - 1.6448536269514722 * sem)
    else:
        p_value_mean_gt0 = 0.0 if mean_ret > 0 else 1.0
        lcb95_ret20 = mean_ret
    p05 = float(np.percentile(arr, 5))
    cvar05 = float(np.mean(arr[arr <= p05])) if np.any(arr <= p05) else p05
    return {
        "daily_count": int(arr.size),
        "top30_mean_ret20_net": mean_ret,
        "top30_win_rate": float(np.mean(arr > 0)),
        "top30_median_ret20_net": float(np.median(arr)),
        "top30_p05_ret20_net": p05,
        "top30_cvar05_ret20_net": cvar05,
        "top30_lcb95_ret20_net": lcb95_ret20,
        "top30_p_value_mean_gt0": p_value_mean_gt0,
    }


def _robust_lb_from_metrics(metrics: dict[str, Any] | None, cfg: MLConfig) -> float | None:
    if not isinstance(metrics, dict):
        return None
    mean_ret = _safe_float(metrics.get("top30_mean_ret20_net"))
    p05_ret = _safe_float(metrics.get("top30_p05_ret20_net"))
    cvar05_ret = _safe_float(metrics.get("top30_cvar05_ret20_net"))
    if mean_ret is None:
        return None
    downside = abs(min(0.0, float(cvar05_ret if cvar05_ret is not None else (p05_ret or 0.0))))
    return float(mean_ret) - float(cfg.robust_lb_lambda) * downside


def compute_label_fields(ret20: float, neutral_band_pct: float) -> tuple[int, int]:
    up20_label = 1 if ret20 > 0 else 0
    train_mask_cls = 1 if abs(ret20) >= neutral_band_pct else 0
    return up20_label, train_mask_cls


def compute_ev20_net(ev20: float, cost_rate: float) -> float:
    return float(ev20) - float(cost_rate)


def _liquidity_slippage_bps(turnover20: float | None) -> float:
    turnover = _safe_float(turnover20)
    if turnover is None:
        return float(LIQ_SLIPPAGE_BPS_UNKNOWN)
    if turnover < float(LIQ_COST_TURNOVER_LOW):
        return float(LIQ_SLIPPAGE_BPS_LOW)
    if turnover < float(LIQ_COST_TURNOVER_MID):
        return float(LIQ_SLIPPAGE_BPS_MID)
    return float(LIQ_SLIPPAGE_BPS_HIGH)


def _trade_cost_rate(*, base_cost_rate: float, turnover20: float | None, side: str) -> float:
    slippage_rate = _liquidity_slippage_bps(turnover20) / 10_000.0
    borrow_rate = (SHORT_BORROW_BPS_20D / 10_000.0) if str(side) == "short" else 0.0
    return float(base_cost_rate) + float(slippage_rate) + float(borrow_rate)


def _rolling_mean(values: list[float], period: int) -> list[float | None]:
    if period <= 0:
        return [None for _ in values]
    out: list[float | None] = [None for _ in values]
    running = 0.0
    for idx, value in enumerate(values):
        running += float(value)
        if idx >= period:
            running -= float(values[idx - period])
        if idx >= period - 1:
            out[idx] = float(running / period)
    return out


def _compute_short_entry_flags(
    closes: list[float],
    highs: list[float],
    lows: list[float],
    ma20_series: list[float | None],
    idx: int,
) -> tuple[bool, bool]:
    close = float(closes[idx])
    ma20 = ma20_series[idx] if idx < len(ma20_series) else None

    countertrend = False
    if (
        ma20 is not None
        and idx >= max(0, SHORT_COUNTER_RANGE_LOOKBACK - 1)
        and idx < len(highs)
        and idx < len(lows)
    ):
        start = idx - (SHORT_COUNTER_RANGE_LOOKBACK - 1)
        hi = max(highs[start : idx + 1])
        lo = min(lows[start : idx + 1])
        span = max(1.0e-9, float(hi - lo))
        range_pos = (close - float(lo)) / span
        countertrend = bool(
            range_pos >= float(SHORT_COUNTER_RANGE_POS_MIN)
            and close >= float(ma20) * (1.0 - float(SHORT_COUNTER_MA20_TOLERANCE))
        )

    support_break = False
    if idx >= max(1, SHORT_SUPPORT_LOOKBACK):
        support = min(lows[idx - SHORT_SUPPORT_LOOKBACK : idx])
        prev_close = float(closes[idx - 1])
        support_break = bool(
            support > 0
            and close <= float(support) * (1.0 - float(SHORT_SUPPORT_BREAK_BUFFER))
            and prev_close >= float(support) * (1.0 - float(SHORT_SUPPORT_PREV_TOLERANCE))
        )

    return countertrend, support_break


def _p_up_pred_col(horizon: int) -> str:
    return "p_up" if int(horizon) == 20 else f"p_up_{int(horizon)}"


def _ret_pred_col(horizon: int) -> str:
    return "ret_pred20" if int(horizon) == 20 else f"ret_pred{int(horizon)}"


def _ev_col(horizon: int) -> str:
    return "ev20" if int(horizon) == 20 else f"ev{int(horizon)}"


def _ev_net_col(horizon: int) -> str:
    return "ev20_net" if int(horizon) == 20 else f"ev{int(horizon)}_net"


def _turn_down_pred_col(horizon: int) -> str:
    return "p_turn_down" if int(horizon) == 10 else f"p_turn_down_{int(horizon)}"


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
            atr14_pct,
            range_pct,
            gap_pct,
            diff20_pct,
            cnt_20_above,
            cnt_7_above,
            close_prev1,
            close_prev5,
            close_prev10,
            close_ret2,
            close_ret3,
            close_ret20,
            close_ret60,
            ma7_prev1,
            ma20_prev1,
            ma60_prev1,
            diff20_prev1,
            cnt_20_prev1,
            cnt_7_prev1,
            vol_ret5,
            vol_ret20,
            vol_ratio5_20,
            turnover20,
            turnover_z20,
            high20_dist,
            low20_dist,
            breakout20_up,
            breakout20_down,
            drawdown60,
            rebound60,
            market_ret1,
            market_ret5,
            market_ret20,
            rel_ret5,
            rel_ret20,
            breadth_above_ma20,
            breadth_above_ma60,
            sector_ret5,
            sector_ret20,
            rel_sector_ret5,
            rel_sector_ret20,
            sector_breadth_ma20,
            weekly_breakout_up_prob,
            weekly_breakout_down_prob,
            weekly_range_prob,
            monthly_breakout_up_prob,
            monthly_breakout_down_prob,
            monthly_range_prob,
            candle_triplet_up_prob,
            candle_triplet_down_prob,
            candle_body_ratio,
            candle_upper_wick_ratio,
            candle_lower_wick_ratio,
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
            NULL::DOUBLE AS atr14_pct,
            NULL::DOUBLE AS range_pct,
            NULL::DOUBLE AS gap_pct,
            diff20_pct,
            cnt_20_above,
            cnt_7_above,
            LAG(close, 1) OVER (PARTITION BY code ORDER BY dt) AS close_prev1,
            LAG(close, 5) OVER (PARTITION BY code ORDER BY dt) AS close_prev5,
            LAG(close, 10) OVER (PARTITION BY code ORDER BY dt) AS close_prev10,
            NULL::DOUBLE AS close_ret2,
            NULL::DOUBLE AS close_ret3,
            NULL::DOUBLE AS close_ret20,
            NULL::DOUBLE AS close_ret60,
            LAG(ma7, 1) OVER (PARTITION BY code ORDER BY dt) AS ma7_prev1,
            LAG(ma20, 1) OVER (PARTITION BY code ORDER BY dt) AS ma20_prev1,
            LAG(ma60, 1) OVER (PARTITION BY code ORDER BY dt) AS ma60_prev1,
            LAG(diff20_pct, 1) OVER (PARTITION BY code ORDER BY dt) AS diff20_prev1,
            LAG(cnt_20_above, 1) OVER (PARTITION BY code ORDER BY dt) AS cnt_20_prev1,
            LAG(cnt_7_above, 1) OVER (PARTITION BY code ORDER BY dt) AS cnt_7_prev1,
            NULL::DOUBLE AS vol_ret5,
            NULL::DOUBLE AS vol_ret20,
            NULL::DOUBLE AS vol_ratio5_20,
            NULL::DOUBLE AS turnover20,
            NULL::DOUBLE AS turnover_z20,
            NULL::DOUBLE AS high20_dist,
            NULL::DOUBLE AS low20_dist,
            NULL::DOUBLE AS breakout20_up,
            NULL::DOUBLE AS breakout20_down,
            NULL::DOUBLE AS drawdown60,
            NULL::DOUBLE AS rebound60,
            NULL::DOUBLE AS market_ret1,
            NULL::DOUBLE AS market_ret5,
            NULL::DOUBLE AS market_ret20,
            NULL::DOUBLE AS rel_ret5,
            NULL::DOUBLE AS rel_ret20,
            NULL::DOUBLE AS breadth_above_ma20,
            NULL::DOUBLE AS breadth_above_ma60,
            NULL::DOUBLE AS sector_ret5,
            NULL::DOUBLE AS sector_ret20,
            NULL::DOUBLE AS rel_sector_ret5,
            NULL::DOUBLE AS rel_sector_ret20,
            NULL::DOUBLE AS sector_breadth_ma20,
            NULL::DOUBLE AS weekly_breakout_up_prob,
            NULL::DOUBLE AS weekly_breakout_down_prob,
            NULL::DOUBLE AS weekly_range_prob,
            NULL::DOUBLE AS monthly_breakout_up_prob,
            NULL::DOUBLE AS monthly_breakout_down_prob,
            NULL::DOUBLE AS monthly_range_prob,
            NULL::DOUBLE AS candle_triplet_up_prob,
            NULL::DOUBLE AS candle_triplet_down_prob,
            NULL::DOUBLE AS candle_body_ratio,
            NULL::DOUBLE AS candle_upper_wick_ratio,
            NULL::DOUBLE AS candle_lower_wick_ratio,
            ?,
            CURRENT_TIMESTAMP
        FROM feature_snapshot_daily
        {where_sql}
        """,
        params,
    )

    target_filters: list[str] = []
    target_params: list[object] = []
    if start_dt is not None:
        target_filters.append("dt >= ?")
        target_params.append(int(start_dt))
    if end_dt is not None:
        target_filters.append("dt <= ?")
        target_params.append(int(end_dt))
    target_where_sql = ("WHERE " + " AND ".join(target_filters)) if target_filters else ""

    conn.execute(
        f"""
        WITH target AS (
            SELECT
                code,
                dt,
                CAST(date_trunc('week', to_timestamp(dt)) - INTERVAL 1 WEEK AS DATE) AS prev_week_key,
                CAST(date_trunc('month', to_timestamp(dt)) - INTERVAL 1 MONTH AS DATE) AS prev_month_key
            FROM ml_feature_daily
            {target_where_sql}
        ),
        weekly_close AS (
            SELECT code, week_key, close
            FROM (
                SELECT
                    code,
                    CAST(date_trunc('week', to_timestamp(date)) AS DATE) AS week_key,
                    c AS close,
                    ROW_NUMBER() OVER (
                        PARTITION BY code, CAST(date_trunc('week', to_timestamp(date)) AS DATE)
                        ORDER BY date DESC
                    ) AS rn
                FROM daily_bars
                WHERE c IS NOT NULL
            ) t
            WHERE rn = 1
        ),
        weekly_window AS (
            SELECT
                code,
                week_key,
                close,
                MAX(close) OVER (PARTITION BY code ORDER BY week_key ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS hi,
                MIN(close) OVER (PARTITION BY code ORDER BY week_key ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS lo
            FROM weekly_close
        ),
        weekly_comp AS (
            SELECT
                code,
                week_key,
                close,
                hi,
                lo,
                CASE
                    WHEN hi IS NULL OR lo IS NULL THEN NULL
                    ELSE GREATEST(ABS((hi + lo) / 2.0), ABS(hi), ABS(lo), 1e-9)
                END AS scale,
                CASE
                    WHEN hi IS NULL OR lo IS NULL THEN NULL
                    ELSE GREATEST(0.0, (hi - lo) / GREATEST(ABS((hi + lo) / 2.0), ABS(hi), ABS(lo), 1e-9))
                END AS width,
                CASE
                    WHEN hi IS NULL OR lo IS NULL THEN NULL
                    WHEN hi > lo THEN LEAST(1.0, GREATEST(0.0, (close - lo) / (hi - lo)))
                    ELSE 0.5
                END AS range_pos
            FROM weekly_window
        ),
        weekly_scores AS (
            SELECT
                code,
                week_key,
                width,
                range_pos,
                LEAST(1.0, GREATEST(0.0, (0.45 - width) / 0.45)) AS compression,
                LEAST(
                    1.0,
                    GREATEST(0.0, GREATEST(0.0, (close - hi) / GREATEST(ABS(hi), 1e-9)) / 0.08)
                ) AS up_break,
                LEAST(
                    1.0,
                    GREATEST(0.0, GREATEST(0.0, (lo - close) / GREATEST(ABS(lo), 1e-9)) / 0.08)
                ) AS down_break
            FROM weekly_comp
            WHERE hi IS NOT NULL AND lo IS NOT NULL
        ),
        weekly_regime AS (
            SELECT
                code,
                week_key,
                CASE
                    WHEN up_break > 0.0
                        THEN GREATEST(
                            LEAST(1.0, GREATEST(0.0, 0.55 * compression + 0.35 * range_pos + 0.10 * up_break)),
                            LEAST(1.0, GREATEST(0.0, 0.72 + 0.28 * up_break))
                        )
                    ELSE LEAST(1.0, GREATEST(0.0, 0.55 * compression + 0.35 * range_pos + 0.10 * up_break))
                END AS weekly_breakout_up_prob,
                CASE
                    WHEN down_break > 0.0
                        THEN GREATEST(
                            LEAST(1.0, GREATEST(0.0, 0.55 * compression + 0.35 * (1.0 - range_pos) + 0.10 * down_break)),
                            LEAST(1.0, GREATEST(0.0, 0.72 + 0.28 * down_break))
                        )
                    ELSE LEAST(1.0, GREATEST(0.0, 0.55 * compression + 0.35 * (1.0 - range_pos) + 0.10 * down_break))
                END AS weekly_breakout_down_prob,
                LEAST(
                    1.0,
                    GREATEST(
                        0.0,
                        0.65 * compression
                        + 0.35 * GREATEST(0.0, 1.0 - ABS(range_pos - 0.5) * 2.0)
                        - 0.40 * GREATEST(up_break, down_break)
                    )
                ) AS weekly_range_prob
            FROM weekly_scores
        ),
        monthly_close AS (
            SELECT
                code,
                CAST(date_trunc('month', to_timestamp(month)) AS DATE) AS month_key,
                c AS close
            FROM monthly_bars
            WHERE c IS NOT NULL
        ),
        monthly_window AS (
            SELECT
                code,
                month_key,
                close,
                MAX(close) OVER (PARTITION BY code ORDER BY month_key ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS hi,
                MIN(close) OVER (PARTITION BY code ORDER BY month_key ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS lo
            FROM monthly_close
        ),
        monthly_comp AS (
            SELECT
                code,
                month_key,
                close,
                hi,
                lo,
                CASE
                    WHEN hi IS NULL OR lo IS NULL THEN NULL
                    ELSE GREATEST(ABS((hi + lo) / 2.0), ABS(hi), ABS(lo), 1e-9)
                END AS scale,
                CASE
                    WHEN hi IS NULL OR lo IS NULL THEN NULL
                    ELSE GREATEST(0.0, (hi - lo) / GREATEST(ABS((hi + lo) / 2.0), ABS(hi), ABS(lo), 1e-9))
                END AS width,
                CASE
                    WHEN hi IS NULL OR lo IS NULL THEN NULL
                    WHEN hi > lo THEN LEAST(1.0, GREATEST(0.0, (close - lo) / (hi - lo)))
                    ELSE 0.5
                END AS range_pos
            FROM monthly_window
        ),
        monthly_scores AS (
            SELECT
                code,
                month_key,
                width,
                range_pos,
                LEAST(1.0, GREATEST(0.0, (0.45 - width) / 0.45)) AS compression,
                LEAST(
                    1.0,
                    GREATEST(0.0, GREATEST(0.0, (close - hi) / GREATEST(ABS(hi), 1e-9)) / 0.08)
                ) AS up_break,
                LEAST(
                    1.0,
                    GREATEST(0.0, GREATEST(0.0, (lo - close) / GREATEST(ABS(lo), 1e-9)) / 0.08)
                ) AS down_break
            FROM monthly_comp
            WHERE hi IS NOT NULL AND lo IS NOT NULL
        ),
        monthly_regime AS (
            SELECT
                code,
                month_key,
                CASE
                    WHEN up_break > 0.0
                        THEN GREATEST(
                            LEAST(1.0, GREATEST(0.0, 0.55 * compression + 0.35 * range_pos + 0.10 * up_break)),
                            LEAST(1.0, GREATEST(0.0, 0.72 + 0.28 * up_break))
                        )
                    ELSE LEAST(1.0, GREATEST(0.0, 0.55 * compression + 0.35 * range_pos + 0.10 * up_break))
                END AS monthly_breakout_up_prob,
                CASE
                    WHEN down_break > 0.0
                        THEN GREATEST(
                            LEAST(1.0, GREATEST(0.0, 0.55 * compression + 0.35 * (1.0 - range_pos) + 0.10 * down_break)),
                            LEAST(1.0, GREATEST(0.0, 0.72 + 0.28 * down_break))
                        )
                    ELSE LEAST(1.0, GREATEST(0.0, 0.55 * compression + 0.35 * (1.0 - range_pos) + 0.10 * down_break))
                END AS monthly_breakout_down_prob,
                LEAST(
                    1.0,
                    GREATEST(
                        0.0,
                        0.65 * compression
                        + 0.35 * GREATEST(0.0, 1.0 - ABS(range_pos - 0.5) * 2.0)
                        - 0.40 * GREATEST(up_break, down_break)
                    )
                ) AS monthly_range_prob
            FROM monthly_scores
        ),
        daily_candle_seq AS (
            SELECT
                code,
                date AS dt,
                o,
                h,
                l,
                c,
                LAG(o, 1) OVER (PARTITION BY code ORDER BY date) AS o_1,
                LAG(o, 2) OVER (PARTITION BY code ORDER BY date) AS o_2,
                LAG(c, 1) OVER (PARTITION BY code ORDER BY date) AS c_1,
                LAG(c, 2) OVER (PARTITION BY code ORDER BY date) AS c_2,
                LAG(c, 9) OVER (PARTITION BY code ORDER BY date) AS c_9
            FROM daily_bars
            WHERE o IS NOT NULL AND h IS NOT NULL AND l IS NOT NULL AND c IS NOT NULL
        ),
        daily_candle_pre AS (
            SELECT
                code,
                dt,
                ABS(c - o) / GREATEST(h - l, 1e-12) AS candle_body_ratio,
                (h - GREATEST(o, c)) / GREATEST(h - l, 1e-12) AS candle_upper_wick_ratio,
                (LEAST(o, c) - l) / GREATEST(h - l, 1e-12) AS candle_lower_wick_ratio,
                (
                    CASE WHEN (c - o) > 0 THEN 1.0 ELSE 0.0 END
                    + CASE WHEN (c_1 - o_1) > 0 THEN 1.0 ELSE 0.0 END
                    + CASE WHEN (c_2 - o_2) > 0 THEN 1.0 ELSE 0.0 END
                ) AS bull_count,
                (
                    CASE WHEN (c - o) < 0 THEN 1.0 ELSE 0.0 END
                    + CASE WHEN (c_1 - o_1) < 0 THEN 1.0 ELSE 0.0 END
                    + CASE WHEN (c_2 - o_2) < 0 THEN 1.0 ELSE 0.0 END
                ) AS bear_count,
                CASE WHEN c_2 < c_1 AND c_1 < c THEN 1.0 ELSE 0.0 END AS higher_close,
                CASE WHEN c_2 > c_1 AND c_1 > c THEN 1.0 ELSE 0.0 END AS lower_close,
                CASE
                    WHEN c_2 IS NULL THEN NULL
                    ELSE (c - c_2) / GREATEST(ABS(c_2), 1e-12)
                END AS move_3,
                CASE
                    WHEN c_2 IS NULL THEN NULL
                    ELSE (
                        c_2 - COALESCE(NULLIF(c_9, 0.0), o_2)
                    ) / GREATEST(ABS(COALESCE(NULLIF(c_9, 0.0), o_2)), 1e-12)
                END AS prior_move,
                o_1,
                o_2,
                c_1,
                c_2
            FROM daily_candle_seq
        ),
        daily_candle AS (
            SELECT
                code,
                dt,
                candle_body_ratio,
                candle_upper_wick_ratio,
                candle_lower_wick_ratio,
                CASE
                    WHEN o_2 IS NULL OR o_1 IS NULL OR c_2 IS NULL OR c_1 IS NULL OR move_3 IS NULL OR prior_move IS NULL
                        THEN NULL
                    ELSE LEAST(
                        1.0,
                        GREATEST(
                            0.0,
                            0.10
                            + 0.26 * (bear_count / 3.0)
                            + 0.18 * lower_close
                            + 0.16 * LEAST(1.0, GREATEST(0.0, ((-move_3) - 0.003) / 0.05))
                            + 0.12 * LEAST(1.0, GREATEST(0.0, (prior_move + 0.06) / 0.18))
                            + 0.11 * LEAST(1.0, GREATEST(0.0, (candle_lower_wick_ratio + 0.02) / 0.30))
                            + 0.07 * LEAST(1.0, GREATEST(0.0, (0.55 - candle_upper_wick_ratio) / 0.55))
                        )
                    )
                END AS candle_triplet_up_prob,
                CASE
                    WHEN o_2 IS NULL OR o_1 IS NULL OR c_2 IS NULL OR c_1 IS NULL OR move_3 IS NULL OR prior_move IS NULL
                        THEN NULL
                    ELSE LEAST(
                        1.0,
                        GREATEST(
                            0.0,
                            0.10
                            + 0.26 * (bull_count / 3.0)
                            + 0.18 * higher_close
                            + 0.16 * LEAST(1.0, GREATEST(0.0, (move_3 - 0.003) / 0.05))
                            + 0.12 * LEAST(1.0, GREATEST(0.0, ((-prior_move) + 0.06) / 0.18))
                            + 0.11 * LEAST(1.0, GREATEST(0.0, (candle_upper_wick_ratio + 0.02) / 0.30))
                            + 0.07 * LEAST(1.0, GREATEST(0.0, (0.55 - candle_lower_wick_ratio) / 0.55))
                        )
                    )
                END AS candle_triplet_down_prob
            FROM daily_candle_pre
        ),
        joined AS (
            SELECT
                t.code,
                t.dt,
                wr.weekly_breakout_up_prob,
                wr.weekly_breakout_down_prob,
                wr.weekly_range_prob,
                mr.monthly_breakout_up_prob,
                mr.monthly_breakout_down_prob,
                mr.monthly_range_prob,
                dc.candle_triplet_up_prob,
                dc.candle_triplet_down_prob,
                dc.candle_body_ratio,
                dc.candle_upper_wick_ratio,
                dc.candle_lower_wick_ratio
            FROM target t
            LEFT JOIN weekly_regime wr
                ON wr.code = t.code AND wr.week_key = t.prev_week_key
            LEFT JOIN monthly_regime mr
                ON mr.code = t.code AND mr.month_key = t.prev_month_key
            LEFT JOIN daily_candle dc
                ON dc.code = t.code AND dc.dt = t.dt
        )
        UPDATE ml_feature_daily AS f
        SET
            weekly_breakout_up_prob = j.weekly_breakout_up_prob,
            weekly_breakout_down_prob = j.weekly_breakout_down_prob,
            weekly_range_prob = j.weekly_range_prob,
            monthly_breakout_up_prob = j.monthly_breakout_up_prob,
            monthly_breakout_down_prob = j.monthly_breakout_down_prob,
            monthly_range_prob = j.monthly_range_prob,
            candle_triplet_up_prob = j.candle_triplet_up_prob,
            candle_triplet_down_prob = j.candle_triplet_down_prob,
            candle_body_ratio = j.candle_body_ratio,
            candle_upper_wick_ratio = j.candle_upper_wick_ratio,
            candle_lower_wick_ratio = j.candle_lower_wick_ratio
        FROM joined j
        WHERE f.code = j.code AND f.dt = j.dt
        """,
        target_params,
    )
    conn.execute(
        f"""
        WITH target AS (
            SELECT code, dt
            FROM ml_feature_daily
            {target_where_sql}
        ),
        daily_ext AS (
            SELECT
                b.code,
                b.date AS dt,
                b.o,
                b.h,
                b.l,
                b.c,
                b.v,
                LAG(b.c, 1) OVER (PARTITION BY b.code ORDER BY b.date) AS c_prev1,
                LAG(b.c, 2) OVER (PARTITION BY b.code ORDER BY b.date) AS c_prev2,
                LAG(b.c, 3) OVER (PARTITION BY b.code ORDER BY b.date) AS c_prev3,
                LAG(b.c, 5) OVER (PARTITION BY b.code ORDER BY b.date) AS c_prev5,
                LAG(b.c, 20) OVER (PARTITION BY b.code ORDER BY b.date) AS c_prev20,
                LAG(b.c, 60) OVER (PARTITION BY b.code ORDER BY b.date) AS c_prev60,
                LAG(b.v, 5) OVER (PARTITION BY b.code ORDER BY b.date) AS v_prev5,
                LAG(b.v, 20) OVER (PARTITION BY b.code ORDER BY b.date) AS v_prev20,
                AVG(COALESCE(b.v, 0.0)) OVER (
                    PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                ) AS vol_ma5,
                AVG(COALESCE(b.v, 0.0)) OVER (
                    PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) AS vol_ma20,
                AVG(COALESCE(b.c, 0.0) * COALESCE(b.v, 0.0)) OVER (
                    PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) AS turnover20,
                STDDEV_POP(COALESCE(b.c, 0.0) * COALESCE(b.v, 0.0)) OVER (
                    PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) AS turnover_std20,
                MAX(b.h) OVER (
                    PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) AS high20,
                MIN(b.l) OVER (
                    PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) AS low20,
                MAX(b.h) OVER (
                    PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
                ) AS high20_prev,
                MIN(b.l) OVER (
                    PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
                ) AS low20_prev,
                MAX(b.h) OVER (
                    PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                ) AS high60,
                MIN(b.l) OVER (
                    PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                ) AS low60,
                COUNT(*) OVER (
                    PARTITION BY b.code ORDER BY b.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS day_count,
                GREATEST(
                    b.h - b.l,
                    ABS(b.h - LAG(b.c, 1) OVER (PARTITION BY b.code ORDER BY b.date)),
                    ABS(b.l - LAG(b.c, 1) OVER (PARTITION BY b.code ORDER BY b.date))
                ) AS tr
            FROM daily_bars b
            WHERE b.c IS NOT NULL
        ),
        daily_feat AS (
            SELECT
                d.*,
                AVG(d.tr) OVER (
                    PARTITION BY d.code ORDER BY d.dt ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                ) AS atr14
            FROM daily_ext d
        ),
        market_series AS (
            SELECT
                b.date AS dt,
                b.c AS m_close,
                LAG(b.c, 1) OVER (ORDER BY b.date) AS m_prev1,
                LAG(b.c, 5) OVER (ORDER BY b.date) AS m_prev5,
                LAG(b.c, 20) OVER (ORDER BY b.date) AS m_prev20
            FROM daily_bars b
            WHERE b.code = '1001' AND b.c IS NOT NULL
        ),
        market_ret AS (
            SELECT
                dt,
                CASE
                    WHEN m_prev1 IS NULL OR ABS(m_prev1) <= 1e-12 THEN NULL
                    ELSE (m_close - m_prev1) / m_prev1
                END AS market_ret1,
                CASE
                    WHEN m_prev5 IS NULL OR ABS(m_prev5) <= 1e-12 THEN NULL
                    ELSE (m_close - m_prev5) / m_prev5
                END AS market_ret5,
                CASE
                    WHEN m_prev20 IS NULL OR ABS(m_prev20) <= 1e-12 THEN NULL
                    ELSE (m_close - m_prev20) / m_prev20
                END AS market_ret20
            FROM market_series
        ),
        breadth AS (
            SELECT
                b.date AS dt,
                AVG(
                    CASE
                        WHEN m.ma20 IS NOT NULL AND ABS(m.ma20) > 1e-12 AND b.c > m.ma20
                            THEN 1.0
                        ELSE 0.0
                    END
                ) AS breadth_above_ma20,
                AVG(
                    CASE
                        WHEN m.ma60 IS NOT NULL AND ABS(m.ma60) > 1e-12 AND b.c > m.ma60
                            THEN 1.0
                        ELSE 0.0
                    END
                ) AS breadth_above_ma60
            FROM daily_bars b
            LEFT JOIN daily_ma m ON m.code = b.code AND m.date = b.date
            WHERE b.c IS NOT NULL
            GROUP BY b.date
        ),
        sector_map AS (
            SELECT
                t.code,
                COALESCE(im.sector33_code, '__NA__') AS sector33_code
            FROM tickers t
            LEFT JOIN industry_master im ON im.code = t.code
        ),
        sector_base AS (
            SELECT
                b.code,
                sm.sector33_code,
                b.date AS dt,
                b.c AS close,
                m.ma20,
                LAG(b.c, 5) OVER (PARTITION BY b.code ORDER BY b.date) AS prev5,
                LAG(b.c, 20) OVER (PARTITION BY b.code ORDER BY b.date) AS prev20
            FROM daily_bars b
            LEFT JOIN daily_ma m ON m.code = b.code AND m.date = b.date
            LEFT JOIN sector_map sm ON sm.code = b.code
            WHERE b.c IS NOT NULL
        ),
        sector_daily AS (
            SELECT
                dt,
                sector33_code,
                AVG(
                    CASE
                        WHEN prev5 IS NULL OR ABS(prev5) <= 1e-12 THEN NULL
                        ELSE (close - prev5) / prev5
                    END
                ) AS sector_ret5,
                AVG(
                    CASE
                        WHEN prev20 IS NULL OR ABS(prev20) <= 1e-12 THEN NULL
                        ELSE (close - prev20) / prev20
                    END
                ) AS sector_ret20,
                AVG(
                    CASE
                        WHEN ma20 IS NOT NULL AND ABS(ma20) > 1e-12 AND close > ma20
                            THEN 1.0
                        ELSE 0.0
                    END
                ) AS sector_breadth_ma20
            FROM sector_base
            GROUP BY dt, sector33_code
        ),
        joined AS (
            SELECT
                t.code,
                t.dt,
                df.atr14 AS atr14_calc,
                CASE
                    WHEN df.c IS NULL OR ABS(df.c) <= 1e-12 THEN NULL
                    ELSE df.atr14 / ABS(df.c)
                END AS atr14_pct,
                CASE
                    WHEN df.c IS NULL OR ABS(df.c) <= 1e-12 THEN NULL
                    ELSE (df.h - df.l) / ABS(df.c)
                END AS range_pct,
                CASE
                    WHEN df.c_prev1 IS NULL OR ABS(df.c_prev1) <= 1e-12 THEN NULL
                    ELSE (df.o - df.c_prev1) / df.c_prev1
                END AS gap_pct,
                CASE
                    WHEN df.c_prev2 IS NULL OR ABS(df.c_prev2) <= 1e-12 THEN NULL
                    ELSE (df.c - df.c_prev2) / df.c_prev2
                END AS close_ret2,
                CASE
                    WHEN df.c_prev3 IS NULL OR ABS(df.c_prev3) <= 1e-12 THEN NULL
                    ELSE (df.c - df.c_prev3) / df.c_prev3
                END AS close_ret3,
                CASE
                    WHEN df.c_prev20 IS NULL OR ABS(df.c_prev20) <= 1e-12 THEN NULL
                    ELSE (df.c - df.c_prev20) / df.c_prev20
                END AS close_ret20,
                CASE
                    WHEN df.c_prev60 IS NULL OR ABS(df.c_prev60) <= 1e-12 THEN NULL
                    ELSE (df.c - df.c_prev60) / df.c_prev60
                END AS close_ret60,
                CASE
                    WHEN df.v_prev5 IS NULL OR ABS(df.v_prev5) <= 1e-12 THEN NULL
                    ELSE (df.v - df.v_prev5) / df.v_prev5
                END AS vol_ret5,
                CASE
                    WHEN df.v_prev20 IS NULL OR ABS(df.v_prev20) <= 1e-12 THEN NULL
                    ELSE (df.v - df.v_prev20) / df.v_prev20
                END AS vol_ret20,
                CASE
                    WHEN df.vol_ma20 IS NULL OR ABS(df.vol_ma20) <= 1e-12 THEN NULL
                    ELSE df.vol_ma5 / df.vol_ma20
                END AS vol_ratio5_20,
                df.turnover20 AS turnover20,
                CASE
                    WHEN df.turnover_std20 IS NULL OR ABS(df.turnover_std20) <= 1e-12 THEN NULL
                    ELSE ((COALESCE(df.c, 0.0) * COALESCE(df.v, 0.0)) - df.turnover20) / df.turnover_std20
                END AS turnover_z20,
                CASE
                    WHEN df.high20 IS NULL OR ABS(df.high20) <= 1e-12 THEN NULL
                    ELSE (df.c - df.high20) / df.high20
                END AS high20_dist,
                CASE
                    WHEN df.low20 IS NULL OR ABS(df.low20) <= 1e-12 THEN NULL
                    ELSE (df.c - df.low20) / df.low20
                END AS low20_dist,
                CASE
                    WHEN df.high20_prev IS NULL OR ABS(df.high20_prev) <= 1e-12 THEN NULL
                    ELSE (df.c - df.high20_prev) / df.high20_prev
                END AS breakout20_up,
                CASE
                    WHEN df.low20_prev IS NULL OR ABS(df.low20_prev) <= 1e-12 THEN NULL
                    ELSE (df.low20_prev - df.c) / df.low20_prev
                END AS breakout20_down,
                CASE
                    WHEN df.high60 IS NULL OR ABS(df.high60) <= 1e-12 THEN NULL
                    ELSE (df.c - df.high60) / df.high60
                END AS drawdown60,
                CASE
                    WHEN df.low60 IS NULL OR ABS(df.low60) <= 1e-12 THEN NULL
                    ELSE (df.c - df.low60) / df.low60
                END AS rebound60,
                mr.market_ret1,
                mr.market_ret5,
                mr.market_ret20,
                CASE
                    WHEN df.c_prev5 IS NULL OR ABS(df.c_prev5) <= 1e-12 OR mr.market_ret5 IS NULL THEN NULL
                    ELSE ((df.c - df.c_prev5) / df.c_prev5) - mr.market_ret5
                END AS rel_ret5,
                CASE
                    WHEN df.c_prev20 IS NULL OR ABS(df.c_prev20) <= 1e-12 OR mr.market_ret20 IS NULL THEN NULL
                    ELSE ((df.c - df.c_prev20) / df.c_prev20) - mr.market_ret20
                END AS rel_ret20,
                br.breadth_above_ma20,
                br.breadth_above_ma60,
                sd.sector_ret5,
                sd.sector_ret20,
                CASE
                    WHEN df.c_prev5 IS NULL OR ABS(df.c_prev5) <= 1e-12 OR sd.sector_ret5 IS NULL THEN NULL
                    ELSE ((df.c - df.c_prev5) / df.c_prev5) - sd.sector_ret5
                END AS rel_sector_ret5,
                CASE
                    WHEN df.c_prev20 IS NULL OR ABS(df.c_prev20) <= 1e-12 OR sd.sector_ret20 IS NULL THEN NULL
                    ELSE ((df.c - df.c_prev20) / df.c_prev20) - sd.sector_ret20
                END AS rel_sector_ret20,
                sd.sector_breadth_ma20
            FROM target t
            LEFT JOIN daily_feat df ON df.code = t.code AND df.dt = t.dt
            LEFT JOIN market_ret mr ON mr.dt = t.dt
            LEFT JOIN breadth br ON br.dt = t.dt
            LEFT JOIN sector_map sm ON sm.code = t.code
            LEFT JOIN sector_daily sd ON sd.dt = t.dt AND sd.sector33_code = sm.sector33_code
        )
        UPDATE ml_feature_daily AS f
        SET
            atr14 = j.atr14_calc,
            atr14_pct = j.atr14_pct,
            range_pct = j.range_pct,
            gap_pct = j.gap_pct,
            close_ret2 = j.close_ret2,
            close_ret3 = j.close_ret3,
            close_ret20 = j.close_ret20,
            close_ret60 = j.close_ret60,
            vol_ret5 = j.vol_ret5,
            vol_ret20 = j.vol_ret20,
            vol_ratio5_20 = j.vol_ratio5_20,
            turnover20 = j.turnover20,
            turnover_z20 = j.turnover_z20,
            high20_dist = j.high20_dist,
            low20_dist = j.low20_dist,
            breakout20_up = j.breakout20_up,
            breakout20_down = j.breakout20_down,
            drawdown60 = j.drawdown60,
            rebound60 = j.rebound60,
            market_ret1 = j.market_ret1,
            market_ret5 = j.market_ret5,
            market_ret20 = j.market_ret20,
            rel_ret5 = j.rel_ret5,
            rel_ret20 = j.rel_ret20,
            breadth_above_ma20 = j.breadth_above_ma20,
            breadth_above_ma60 = j.breadth_above_ma60,
            sector_ret5 = j.sector_ret5,
            sector_ret20 = j.sector_ret20,
            rel_sector_ret5 = j.rel_sector_ret5,
            rel_sector_ret20 = j.rel_sector_ret20,
            sector_breadth_ma20 = j.sector_breadth_ma20
        FROM joined j
        WHERE f.code = j.code AND f.dt = j.dt
        """,
        target_params,
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
    daily_dt_sql = _normalized_daily_dt_sql("date")
    label_dt_sql = _normalized_daily_dt_sql("dt")
    where: list[str] = [f"{close_col} IS NOT NULL"]
    params: list[object] = []
    if start_dt is not None:
        where.append(f"{daily_dt_sql} >= ?")
        params.append(int(start_dt))
    if end_dt is not None:
        # Keep a forward margin for 20 business-day labels.
        where.append(f"{daily_dt_sql} <= ?")
        params.append(_offset_yyyymmdd(int(end_dt), 90))
    where_sql = " AND ".join(where)
    rows = conn.execute(
        f"""
        SELECT code, date, h, l, {close_col}
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
            del_where.append(f"{label_dt_sql} >= ?")
            del_params.append(int(start_dt))
        if end_dt is not None:
            del_where.append(f"{label_dt_sql} <= ?")
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
                ret5,
                ret10,
                ret20,
                up5_label,
                up10_label,
                up20_label,
                train_mask_cls_5,
                train_mask_cls_10,
                train_mask_cls,
                turn_up_label,
                turn_down_reversion_label_5,
                turn_down_reversion_label_10,
                turn_down_reversion_label_20,
                turn_down_break_label_5,
                turn_down_break_label_10,
                turn_down_break_label_20,
                turn_down_label_5,
                turn_down_label,
                turn_down_label_20,
                train_mask_turn_5,
                train_mask_turn,
                train_mask_turn_20,
                n_forward,
                label_version,
                computed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            chunk,
        )
        return len(chunk)

    def _flush(
        code: str,
        dates: list[int],
        closes: list[float],
        highs: list[float],
        lows: list[float],
    ) -> None:
        nonlocal inserted
        if not code:
            return
        max_i = len(closes) - 20
        if max_i <= 0:
            return
        ma20_series = _rolling_mean(closes, 20)
        for i in range(max_i):
            dt_i = int(dates[i])
            dt_key = _normalize_daily_dt_key(dt_i)
            if dt_key is None:
                continue
            if start_dt is not None and dt_key < int(start_dt):
                continue
            if end_dt is not None and dt_key > int(end_dt):
                continue
            base = closes[i]
            if base == 0:
                continue
            ret5 = (closes[i + 5] / base) - 1.0
            ret10 = (closes[i + 10] / base) - 1.0
            ret20 = (closes[i + 20] / base) - 1.0

            up5_label, train_mask_cls_5 = compute_label_fields(ret5, neutral)
            up10_label, train_mask_cls_10 = compute_label_fields(ret10, neutral)
            up20_label, train_mask_cls = compute_label_fields(ret20, neutral)

            future_turn_5 = closes[i + 1 : i + 1 + 5]
            future_turn_10 = closes[i + 1 : i + 1 + TURN_HORIZON_DAYS]
            future_turn_20 = closes[i + 1 : i + 1 + 20]
            if (
                len(future_turn_5) < 5
                or len(future_turn_10) < TURN_HORIZON_DAYS
                or len(future_turn_20) < 20
            ):
                continue

            max_ret_turn_5 = (max(future_turn_5) / base) - 1.0
            min_ret_turn_5 = (min(future_turn_5) / base) - 1.0
            max_ret_turn_10 = (max(future_turn_10) / base) - 1.0
            min_ret_turn_10 = (min(future_turn_10) / base) - 1.0
            max_ret_turn_20 = (max(future_turn_20) / base) - 1.0
            min_ret_turn_20 = (min(future_turn_20) / base) - 1.0

            countertrend_setup, support_break_setup = _compute_short_entry_flags(
                closes,
                highs,
                lows,
                ma20_series,
                i,
            )

            short_success_5 = bool(
                min_ret_turn_5 <= -float(SHORT_TARGET_PCT_5D)
                and max_ret_turn_5 < float(SHORT_STOP_PCT)
            )
            short_success_10 = bool(
                min_ret_turn_10 <= -float(SHORT_TARGET_PCT_10D)
                and max_ret_turn_10 < float(SHORT_STOP_PCT)
            )
            short_success_20 = bool(
                min_ret_turn_20 <= -float(SHORT_TARGET_PCT_20D)
                and max_ret_turn_20 < float(SHORT_STOP_PCT)
            )

            turn_up_label = (
                1
                if (max_ret_turn_10 >= float(TURN_UP_TARGET_PCT) and min_ret_turn_10 > -float(TURN_UP_STOP_PCT))
                else 0
            )

            turn_down_reversion_label_5 = 1 if (countertrend_setup and short_success_5) else 0
            turn_down_reversion_label_10 = 1 if (countertrend_setup and short_success_10) else 0
            turn_down_reversion_label_20 = 1 if (countertrend_setup and short_success_20) else 0
            turn_down_break_label_5 = 1 if (support_break_setup and short_success_5) else 0
            turn_down_break_label_10 = 1 if (support_break_setup and short_success_10) else 0
            turn_down_break_label_20 = 1 if (support_break_setup and short_success_20) else 0

            turn_down_label_5 = 1 if (turn_down_reversion_label_5 or turn_down_break_label_5) else 0
            turn_down_label = 1 if (turn_down_reversion_label_10 or turn_down_break_label_10) else 0
            turn_down_label_20 = 1 if (turn_down_reversion_label_20 or turn_down_break_label_20) else 0

            train_mask_turn_5 = 1 if (turn_down_label_5 == 1 or abs(ret5) >= neutral) else 0
            train_mask_turn = 1 if (turn_up_label == 1 or turn_down_label == 1 or abs(ret10) >= neutral) else 0
            train_mask_turn_20 = 1 if (turn_down_label_20 == 1 or abs(ret20) >= neutral) else 0
            records.append(
                (
                    dt_i,
                    code,
                    float(ret5),
                    float(ret10),
                    float(ret20),
                    int(up5_label),
                    int(up10_label),
                    int(up20_label),
                    int(train_mask_cls_5),
                    int(train_mask_cls_10),
                    int(train_mask_cls),
                    int(turn_up_label),
                    int(turn_down_reversion_label_5),
                    int(turn_down_reversion_label_10),
                    int(turn_down_reversion_label_20),
                    int(turn_down_break_label_5),
                    int(turn_down_break_label_10),
                    int(turn_down_break_label_20),
                    int(turn_down_label_5),
                    int(turn_down_label),
                    int(turn_down_label_20),
                    int(train_mask_turn_5),
                    int(train_mask_turn),
                    int(train_mask_turn_20),
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
    highs: list[float] = []
    lows: list[float] = []
    for code_raw, dt_raw, high_raw, low_raw, close_raw in rows:
        code = str(code_raw)
        dt = int(dt_raw)
        high = _safe_float(high_raw)
        low = _safe_float(low_raw)
        close = _safe_float(close_raw)
        if close is None or high is None or low is None:
            continue
        if current_code and code != current_code:
            _flush(current_code, dates, closes, highs, lows)
            dates = []
            closes = []
            highs = []
            lows = []
        current_code = code
        dates.append(dt)
        closes.append(close)
        highs.append(high)
        lows.append(low)
    _flush(current_code, dates, closes, highs, lows)

    inserted += _insert_chunk(records)
    return int(inserted)


def _load_monthly_feature_snapshots(
    conn,
    *,
    max_ym: int | None = None,
) -> pd.DataFrame:
    ym_case = """
        CASE
            WHEN f.dt BETWEEN 10000000 AND 99991231 THEN CAST(SUBSTR(CAST(f.dt AS VARCHAR), 1, 6) AS INTEGER)
            WHEN f.dt >= 1000000000 THEN CAST(strftime(to_timestamp(f.dt), '%Y%m') AS INTEGER)
            ELSE NULL
        END
    """
    ts_case = """
        CASE
            WHEN f.dt BETWEEN 10000000 AND 99991231 THEN strptime(CAST(f.dt AS VARCHAR), '%Y%m%d')
            WHEN f.dt >= 1000000000 THEN to_timestamp(f.dt)
            ELSE NULL
        END
    """
    where_sql = "rn = 1 AND ym IS NOT NULL"
    params: list[object] = []
    if max_ym is not None:
        where_sql += " AND ym <= ?"
        params.append(int(max_ym))
    sql = f"""
        WITH ranked AS (
            SELECT
                f.*,
                {ym_case} AS ym,
                {ts_case} AS dt_ts,
                ROW_NUMBER() OVER (
                    PARTITION BY f.code, {ym_case}
                    ORDER BY {ts_case} DESC
                ) AS rn
            FROM ml_feature_daily f
        )
        SELECT *
        FROM ranked
        WHERE {where_sql}
        ORDER BY code, ym
    """
    df = conn.execute(sql, params).df()
    if df.empty:
        return df
    out = df.copy()
    out = out.rename(columns={"dt": "snap_dt"})
    ym_values = pd.to_numeric(out.get("ym"), errors="coerce").astype("Int64")
    out["dt"] = (ym_values * 100 + 1).astype("Int64")
    out["dt"] = pd.to_numeric(out["dt"], errors="coerce")
    return out


def _load_monthly_forward_returns(conn) -> pd.DataFrame:
    ym_case = """
        CASE
            WHEN m.month BETWEEN 100000 AND 999912 THEN CAST(m.month AS INTEGER)
            WHEN m.month >= 100000000 THEN CAST(strftime(to_timestamp(m.month), '%Y%m') AS INTEGER)
            ELSE NULL
        END
    """
    ts_case = """
        CASE
            WHEN m.month BETWEEN 100000 AND 999912 THEN strptime(CAST(m.month AS VARCHAR), '%Y%m')
            WHEN m.month >= 100000000 THEN to_timestamp(m.month)
            ELSE NULL
        END
    """
    sql = f"""
        WITH base AS (
            SELECT
                m.code,
                {ym_case} AS ym,
                {ts_case} AS month_ts,
                m.c AS close
            FROM monthly_bars m
            WHERE m.c IS NOT NULL
        ),
        dedup AS (
            SELECT
                code,
                ym,
                month_ts,
                close,
                ROW_NUMBER() OVER (
                    PARTITION BY code, ym
                    ORDER BY month_ts DESC
                ) AS rn
            FROM base
            WHERE ym IS NOT NULL AND month_ts IS NOT NULL
        ),
        ordered AS (
            SELECT
                code,
                ym,
                close,
                LEAD(close) OVER (PARTITION BY code ORDER BY ym) AS next_close
            FROM dedup
            WHERE rn = 1
        )
        SELECT
            code,
            ym,
            ((next_close / close) - 1.0) AS ret1m
        FROM ordered
        WHERE next_close IS NOT NULL AND close IS NOT NULL AND close <> 0
        ORDER BY code, ym
    """
    return conn.execute(sql).df()


def refresh_ml_monthly_label_table(
    conn,
    *,
    label_version: int = MONTHLY_LABEL_VERSION,
    start_dt: int | None = None,
    end_dt: int | None = None,
) -> int:
    _ensure_ml_schema(conn)
    if not _table_exists(conn, "ml_feature_daily") or not _table_exists(conn, "monthly_bars"):
        return 0

    start_month = _to_month_start_int(start_dt) if start_dt is not None else None
    end_month = _to_month_start_int(end_dt) if end_dt is not None else None
    max_ym = _month_start_to_yyyymm(end_month) if end_month is not None else None

    snapshots = _load_monthly_feature_snapshots(conn, max_ym=max_ym)
    returns_df = _load_monthly_forward_returns(conn)
    if start_month is not None or end_month is not None:
        where: list[str] = []
        params: list[object] = []
        if start_month is not None:
            where.append("dt >= ?")
            params.append(int(start_month))
        if end_month is not None:
            where.append("dt <= ?")
            params.append(int(end_month))
        conn.execute(f"DELETE FROM ml_monthly_label WHERE {' AND '.join(where)}", params)
    else:
        conn.execute("DELETE FROM ml_monthly_label")
    if snapshots.empty or returns_df.empty:
        return 0

    snap = snapshots.copy()
    snap["ym"] = pd.to_numeric(snap.get("ym"), errors="coerce")
    snap["dt"] = pd.to_numeric(snap.get("dt"), errors="coerce")
    snap = snap[np.isfinite(snap["ym"].to_numpy(dtype=float, copy=False))]
    snap = snap[np.isfinite(snap["dt"].to_numpy(dtype=float, copy=False))]
    if start_month is not None:
        snap = snap[snap["dt"] >= int(start_month)]
    if end_month is not None:
        snap = snap[snap["dt"] <= int(end_month)]
    if snap.empty:
        return 0

    ret_df = returns_df.copy()
    ret_df["ym"] = pd.to_numeric(ret_df.get("ym"), errors="coerce")
    ret_df["ret1m"] = pd.to_numeric(ret_df.get("ret1m"), errors="coerce")
    merged = snap.merge(
        ret_df[["code", "ym", "ret1m"]],
        on=["code", "ym"],
        how="inner",
    )
    if merged.empty:
        return 0

    merged["ret1m"] = pd.to_numeric(merged.get("ret1m"), errors="coerce")
    merged["liquidity_proxy"] = pd.to_numeric(merged.get("turnover20"), errors="coerce")
    merged = merged[np.isfinite(merged["ret1m"].to_numpy(dtype=float, copy=False))]
    merged = merged[np.isfinite(merged["dt"].to_numpy(dtype=float, copy=False))]
    if merged.empty:
        return 0

    computed_at = datetime.now(tz=timezone.utc).replace(tzinfo=None)
    records: list[tuple] = []
    inserted = 0

    def _insert_chunk(chunk: list[tuple]) -> int:
        if not chunk:
            return 0
        conn.executemany(
            """
            INSERT INTO ml_monthly_label (
                dt,
                code,
                ret1m,
                up_big,
                down_big,
                abs_big,
                dir_up,
                liquidity_proxy,
                liquidity_pass,
                label_version,
                computed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            chunk,
        )
        return len(chunk)

    for month_dt, group in merged.groupby("dt", sort=True):
        month_df = group.copy()
        liq = pd.to_numeric(month_df.get("liquidity_proxy"), errors="coerce")
        ret = pd.to_numeric(month_df.get("ret1m"), errors="coerce")
        liq_np = liq.to_numpy(dtype=float, copy=False)
        ret_np = ret.to_numpy(dtype=float, copy=False)

        liq_valid = np.isfinite(liq_np)
        if np.any(liq_valid):
            liq_threshold = float(np.nanquantile(liq_np[liq_valid], MONTHLY_LIQUIDITY_BOTTOM_RATIO))
            liq_pass = liq_valid & (liq_np >= liq_threshold)
        else:
            liq_pass = np.zeros_like(liq_np, dtype=bool)

        ret_valid = np.isfinite(ret_np)
        pass_valid = liq_pass & ret_valid
        up_big = np.zeros_like(liq_pass, dtype=bool)
        down_big = np.zeros_like(liq_pass, dtype=bool)
        if int(np.sum(pass_valid)) >= 10:
            up_threshold = float(np.nanquantile(ret_np[pass_valid], 1.0 - MONTHLY_LABEL_QUANTILE))
            down_threshold = float(np.nanquantile(ret_np[pass_valid], MONTHLY_LABEL_QUANTILE))
            up_big = pass_valid & (ret_np >= up_threshold)
            down_big = pass_valid & (ret_np <= down_threshold)
            down_big = down_big & (~up_big)
        abs_big = up_big | down_big

        month_codes = month_df.get("code").astype(str).tolist()
        for idx, code in enumerate(month_codes):
            ret1m = ret_np[idx]
            if not math.isfinite(float(ret1m)):
                continue
            liquidity_proxy = liq_np[idx] if math.isfinite(float(liq_np[idx])) else None
            records.append(
                (
                    int(month_dt),
                    str(code),
                    float(ret1m),
                    int(up_big[idx]),
                    int(down_big[idx]),
                    int(abs_big[idx]),
                    int(up_big[idx]) if abs_big[idx] else 0,
                    float(liquidity_proxy) if liquidity_proxy is not None else None,
                    int(liq_pass[idx]),
                    int(label_version),
                    computed_at,
                )
            )
            if len(records) >= 50_000:
                inserted += _insert_chunk(records)
                records.clear()

    inserted += _insert_chunk(records)
    return int(inserted)


def _load_monthly_training_df(
    conn,
    *,
    start_dt: int | None = None,
    end_dt: int | None = None,
) -> pd.DataFrame:
    start_month = _to_month_start_int(start_dt) if start_dt is not None else None
    end_month = _to_month_start_int(end_dt) if end_dt is not None else None
    max_ym = _month_start_to_yyyymm(end_month) if end_month is not None else None
    snap = _load_monthly_feature_snapshots(conn, max_ym=max_ym)
    if snap.empty:
        return pd.DataFrame()

    where: list[str] = []
    params: list[object] = []
    if start_month is not None:
        where.append("dt >= ?")
        params.append(int(start_month))
    if end_month is not None:
        where.append("dt <= ?")
        params.append(int(end_month))
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    labels = conn.execute(
        f"""
        SELECT
            dt,
            code,
            ret1m,
            up_big,
            down_big,
            abs_big,
            dir_up,
            liquidity_proxy,
            liquidity_pass
        FROM ml_monthly_label
        {where_sql}
        ORDER BY dt, code
        """,
        params,
    ).df()
    if labels.empty:
        return pd.DataFrame()

    labels["dt"] = pd.to_numeric(labels.get("dt"), errors="coerce")
    snap["dt"] = pd.to_numeric(snap.get("dt"), errors="coerce")
    merged = snap.merge(labels, on=["dt", "code"], how="inner")
    if merged.empty:
        return pd.DataFrame()
    merged["ret1m"] = pd.to_numeric(merged.get("ret1m"), errors="coerce")
    merged["abs_big"] = pd.to_numeric(merged.get("abs_big"), errors="coerce").fillna(0).astype(int)
    merged["dir_up"] = pd.to_numeric(merged.get("dir_up"), errors="coerce").fillna(0).astype(int)
    merged["liquidity_pass"] = pd.to_numeric(merged.get("liquidity_pass"), errors="coerce").fillna(0).astype(int)
    merged = merged[
        np.isfinite(merged["ret1m"].to_numpy(dtype=float, copy=False))
        & (merged["liquidity_pass"] == 1)
    ]
    if merged.empty:
        return pd.DataFrame()
    return merged.sort_values(["dt", "code"]).reset_index(drop=True)


def _fit_monthly_models(train_df: pd.DataFrame, cfg: MLConfig) -> MonthlyTrainedModels:
    lgb = _import_lightgbm()
    if train_df.empty:
        raise RuntimeError("monthly train_df is empty")

    cls_params = {
        "objective": "binary",
        "metric": "binary_logloss",
        "learning_rate": 0.05,
        "num_leaves": 31,
        "feature_fraction": 0.9,
        "bagging_fraction": 0.9,
        "bagging_freq": 1,
        "min_data_in_leaf": 30,
        "verbosity": -1,
    }

    abs_df = train_df.copy()
    abs_df["abs_big"] = pd.to_numeric(abs_df.get("abs_big"), errors="coerce").fillna(0).astype(int)
    y_abs = abs_df["abs_big"].to_numpy(dtype=int, copy=False)
    n_abs = int(len(abs_df))
    if n_abs < 200:
        raise RuntimeError("Insufficient monthly abs rows")
    abs_pos = int(np.sum(y_abs == 1))
    abs_neg = int(np.sum(y_abs == 0))
    if abs_pos < 40 or abs_neg < 40:
        raise RuntimeError("Insufficient monthly abs class balance")

    x_abs, medians = _prepare_feature_matrix(abs_df, medians=None, feature_columns=FEATURE_COLUMNS)
    abs_train = lgb.Dataset(
        x_abs.to_numpy(dtype=float),
        label=y_abs,
        feature_name=FEATURE_COLUMNS,
        free_raw_data=False,
    )
    abs_model = lgb.train(cls_params, abs_train, num_boost_round=int(cfg.cls_boost_round))
    abs_temperature = float(
        _fit_temperature_for_binary_classifier(
            model=abs_model,
            train_slice=abs_df,
            label_col="abs_big",
            medians=medians,
            feature_columns=FEATURE_COLUMNS,
        )
    )

    dir_model: Any | None = None
    dir_temperature = 1.0
    dir_df = abs_df[abs_df["abs_big"] == 1].copy()
    dir_df["dir_up"] = pd.to_numeric(dir_df.get("dir_up"), errors="coerce").fillna(0).astype(int)
    n_dir = int(len(dir_df))
    if n_dir >= 120:
        y_dir = dir_df["dir_up"].to_numpy(dtype=int, copy=False)
        dir_pos = int(np.sum(y_dir == 1))
        dir_neg = int(np.sum(y_dir == 0))
        if dir_pos >= 30 and dir_neg >= 30:
            x_dir, _ = _prepare_feature_matrix(dir_df, medians=medians, feature_columns=FEATURE_COLUMNS)
            dir_train = lgb.Dataset(
                x_dir.to_numpy(dtype=float),
                label=y_dir,
                feature_name=FEATURE_COLUMNS,
                free_raw_data=False,
            )
            dir_model = lgb.train(cls_params, dir_train, num_boost_round=int(cfg.cls_boost_round))
            dir_temperature = float(
                _fit_temperature_for_binary_classifier(
                    model=dir_model,
                    train_slice=dir_df,
                    label_col="dir_up",
                    medians=medians,
                    feature_columns=FEATURE_COLUMNS,
                )
            )

    return MonthlyTrainedModels(
        abs_cls=abs_model,
        dir_cls=dir_model,
        feature_columns=list(FEATURE_COLUMNS),
        medians=medians,
        abs_temperature=float(abs_temperature),
        dir_temperature=float(dir_temperature),
        n_train_abs=int(n_abs),
        n_train_dir=int(n_dir),
    )


def _predict_monthly_frame(df: pd.DataFrame, models: MonthlyTrainedModels) -> pd.DataFrame:
    if models.abs_cls is None:
        raise RuntimeError("monthly abs model is not available")
    matrix, _ = _prepare_feature_matrix(
        df,
        medians=models.medians,
        feature_columns=models.feature_columns,
    )
    matrix_np = matrix.to_numpy(dtype=float)
    pred = df.copy()
    raw_abs = np.asarray(models.abs_cls.predict(matrix_np), dtype=float)
    p_abs_big = _apply_temperature_to_prob_array(raw_abs, models.abs_temperature)
    if models.dir_cls is not None:
        raw_dir = np.asarray(models.dir_cls.predict(matrix_np), dtype=float)
        p_up_given_big = _apply_temperature_to_prob_array(raw_dir, models.dir_temperature)
    else:
        p_up_given_big = np.full(len(pred), 0.5, dtype=float)
    p_up_big = np.clip(p_abs_big * p_up_given_big, 0.0, 1.0)
    p_down_big = np.clip(p_abs_big * (1.0 - p_up_given_big), 0.0, 1.0)
    pred["p_abs_big"] = p_abs_big
    pred["p_up_given_big"] = p_up_given_big
    pred["p_up_big"] = p_up_big
    pred["p_down_big"] = p_down_big
    pred["score_up"] = np.clip(0.7 * p_up_big + 0.3 * p_abs_big, 0.0, 1.0)
    pred["score_down"] = np.clip(0.7 * p_down_big + 0.3 * p_abs_big, 0.0, 1.0)
    return pred


def _search_monthly_gate_for_direction(
    pred_df: pd.DataFrame,
    *,
    direction: str,
) -> dict[str, Any]:
    side_col = "p_up_big" if direction == "up" else "p_down_big"
    if pred_df.empty or side_col not in pred_df.columns:
        return {}
    required_cols = {"dt", "ret1m", "p_abs_big", side_col}
    if not required_cols.issubset(set(pred_df.columns)):
        return {}
    work = pred_df[list(required_cols)].copy()
    for col in required_cols:
        work[col] = pd.to_numeric(work.get(col), errors="coerce")
    finite_mask = np.isfinite(work["dt"].to_numpy(dtype=float, copy=False))
    finite_mask &= np.isfinite(work["ret1m"].to_numpy(dtype=float, copy=False))
    finite_mask &= np.isfinite(work["p_abs_big"].to_numpy(dtype=float, copy=False))
    finite_mask &= np.isfinite(work[side_col].to_numpy(dtype=float, copy=False))
    work = work[finite_mask].copy()
    if work.empty:
        return {}
    work["dt"] = work["dt"].astype(int)
    total_months = int(work["dt"].nunique())
    if total_months <= 0:
        return {}
    min_months = max(6, int(math.floor(total_months * MONTHLY_GATE_MIN_MONTH_COVERAGE)))
    best: dict[str, Any] | None = None
    for abs_gate in MONTHLY_GATE_ABS_CANDIDATES:
        for side_gate in MONTHLY_GATE_SIDE_CANDIDATES:
            selected = work[(work["p_abs_big"] >= float(abs_gate)) & (work[side_col] >= float(side_gate))]
            if selected.empty:
                continue
            month_counts = selected.groupby("dt").size()
            covered_months = int(month_counts.size)
            avg_month_picks = float(month_counts.mean()) if covered_months > 0 else 0.0
            if covered_months < min_months or avg_month_picks < MONTHLY_GATE_MIN_AVG_PICKS:
                continue
            side_ret = (
                selected["ret1m"].to_numpy(dtype=float, copy=False)
                if direction == "up"
                else -selected["ret1m"].to_numpy(dtype=float, copy=False)
            )
            if side_ret.size <= 0:
                continue
            mean_ret = float(np.mean(side_ret))
            hit_rate = float(np.mean(side_ret > 0.0))
            size_factor = min(1.0, avg_month_picks / MONTHLY_GATE_TARGET_AVG_PICKS)
            coverage_ratio = float(covered_months) / float(max(1, total_months))
            score = float(mean_ret * (0.8 + 0.2 * size_factor) * (0.9 + 0.1 * coverage_ratio))
            candidate = {
                "abs_gate": float(abs_gate),
                "side_gate": float(side_gate),
                "mean_ret": mean_ret,
                "hit_rate": hit_rate,
                "avg_month_picks": avg_month_picks,
                "covered_months": covered_months,
                "total_months": total_months,
                "coverage_ratio": coverage_ratio,
                "samples": int(len(selected)),
                "score": score,
            }
            if best is None:
                best = candidate
                continue
            prev_key = (
                float(best.get("score") or -1e9),
                float(best.get("mean_ret") or -1e9),
                float(best.get("avg_month_picks") or -1e9),
                -float(best.get("abs_gate") or 0.0),
                -float(best.get("side_gate") or 0.0),
            )
            next_key = (
                float(candidate.get("score") or -1e9),
                float(candidate.get("mean_ret") or -1e9),
                float(candidate.get("avg_month_picks") or -1e9),
                -float(candidate.get("abs_gate") or 0.0),
                -float(candidate.get("side_gate") or 0.0),
            )
            if next_key > prev_key:
                best = candidate
    return best or {}


def _derive_monthly_gate_recommendation(
    train_df: pd.DataFrame,
    models: MonthlyTrainedModels,
    *,
    pred_df: pd.DataFrame | None = None,
    ret20_lookup: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if train_df.empty or models.abs_cls is None:
        return {}
    pred = pred_df.copy() if pred_df is not None else _predict_monthly_frame(train_df, models)
    if "ret1m" not in pred.columns:
        pred["ret1m"] = pd.to_numeric(train_df.get("ret1m"), errors="coerce")
    resolved_ret20_lookup = (
        ret20_lookup
        if isinstance(ret20_lookup, dict) and ret20_lookup
        else _derive_monthly_ret20_lookup(
            train_df,
            models,
            pred_df=pred,
        )
    )
    up = _search_monthly_gate_for_direction(pred, direction="up")
    down = _search_monthly_gate_for_direction(pred, direction="down")
    up_target20 = _search_monthly_target20_gate_for_direction(
        pred,
        direction="up",
        lookup_dir=resolved_ret20_lookup.get("up") if isinstance(resolved_ret20_lookup, dict) else None,
    )
    down_target20 = _search_monthly_target20_gate_for_direction(
        pred,
        direction="down",
        lookup_dir=resolved_ret20_lookup.get("down") if isinstance(resolved_ret20_lookup, dict) else None,
    )
    if isinstance(up_target20, dict) and up_target20:
        up = {**up, **up_target20}
    if isinstance(down_target20, dict) and down_target20:
        down = {**down, **down_target20}
    return {
        "up": up,
        "down": down,
        "search": {
            "abs_candidates": [float(x) for x in MONTHLY_GATE_ABS_CANDIDATES],
            "side_candidates": [float(x) for x in MONTHLY_GATE_SIDE_CANDIDATES],
            "min_month_coverage": float(MONTHLY_GATE_MIN_MONTH_COVERAGE),
            "min_avg_month_picks": float(MONTHLY_GATE_MIN_AVG_PICKS),
            "target_avg_month_picks": float(MONTHLY_GATE_TARGET_AVG_PICKS),
            "target20_candidates": [float(x) for x in MONTHLY_TARGET20_GATE_CANDIDATES],
            "target20_min_month_coverage": float(MONTHLY_TARGET20_MIN_MONTH_COVERAGE),
            "target20_min_avg_month_picks": float(MONTHLY_TARGET20_MIN_AVG_PICKS),
            "target20_target_avg_month_picks": float(MONTHLY_TARGET20_TARGET_AVG_PICKS),
            "target20_min_lift": float(MONTHLY_TARGET20_MIN_LIFT),
        },
    }


def _build_monthly_ret20_lookup_for_direction(
    pred_df: pd.DataFrame,
    *,
    direction: str,
) -> dict[str, Any]:
    side_col = "p_up_big" if direction == "up" else "p_down_big"
    if pred_df.empty or side_col not in pred_df.columns or "ret1m" not in pred_df.columns:
        return {"baseline_rate": 0.0, "bins": []}
    work = pred_df[[side_col, "ret1m"]].copy()
    work[side_col] = pd.to_numeric(work.get(side_col), errors="coerce")
    work["ret1m"] = pd.to_numeric(work.get("ret1m"), errors="coerce")
    finite_mask = np.isfinite(work[side_col].to_numpy(dtype=float, copy=False))
    finite_mask &= np.isfinite(work["ret1m"].to_numpy(dtype=float, copy=False))
    work = work[finite_mask].copy()
    if work.empty:
        return {"baseline_rate": 0.0, "bins": []}
    side_prob = work[side_col].to_numpy(dtype=float, copy=False)
    ret1m = work["ret1m"].to_numpy(dtype=float, copy=False)
    event = (ret1m >= MONTHLY_RET20_TARGET) if direction == "up" else (ret1m <= -MONTHLY_RET20_TARGET)
    baseline = float(np.mean(event))
    try:
        edges = np.quantile(side_prob, MONTHLY_RET20_BIN_QUANTILES)
    except Exception:
        edges = np.array([float(np.min(side_prob)), float(np.max(side_prob))], dtype=float)
    edges = np.unique(np.asarray(edges, dtype=float))
    if edges.size < 2:
        return {"baseline_rate": baseline, "bins": []}
    bins: list[dict[str, Any]] = []
    for idx in range(edges.size - 1):
        low = float(edges[idx])
        high = float(edges[idx + 1])
        if not math.isfinite(low) or not math.isfinite(high):
            continue
        if high <= low:
            continue
        if idx >= edges.size - 2:
            mask = (side_prob >= low) & (side_prob <= high)
        else:
            mask = (side_prob >= low) & (side_prob < high)
        samples = int(np.sum(mask))
        if samples < MONTHLY_RET20_MIN_BIN_SAMPLES:
            continue
        event_rate = float(np.mean(event[mask]))
        mean_ret = float(np.mean(ret1m[mask]))
        bins.append(
            {
                "min_prob": low,
                "max_prob": high,
                "event_rate": event_rate,
                "samples": samples,
                "mean_ret1m": mean_ret,
            }
        )
    if not bins:
        return {"baseline_rate": baseline, "bins": []}
    bins = sorted(bins, key=lambda row: float(row.get("min_prob") or 0.0))
    running = 0.0
    for row in bins:
        rate = float(row.get("event_rate") or 0.0)
        running = max(running, rate)
        row["event_rate"] = float(running)
    return {
        "baseline_rate": baseline,
        "bins": bins,
    }


def _estimate_monthly_side20_from_lookup(
    prob_side: float | None,
    lookup_dir: dict[str, Any] | None,
) -> float | None:
    if prob_side is None or not math.isfinite(float(prob_side)):
        return None
    lookup = lookup_dir if isinstance(lookup_dir, dict) else {}
    p = float(max(0.0, min(1.0, float(prob_side))))
    baseline = _safe_float(lookup.get("baseline_rate")) or 0.0
    raw_bins = lookup.get("bins")
    bins = raw_bins if isinstance(raw_bins, list) else []
    fallback = baseline * 0.5 + 0.20 * p
    for idx, row in enumerate(bins):
        if not isinstance(row, dict):
            continue
        low = _safe_float(row.get("min_prob"))
        high = _safe_float(row.get("max_prob"))
        rate = _safe_float(row.get("event_rate"))
        if low is None or high is None or rate is None or high < low:
            continue
        in_bin = (p >= low and p < high) if idx < len(bins) - 1 else (p >= low and p <= high)
        if in_bin:
            mixed = 0.70 * float(rate) + 0.30 * float(fallback)
            return float(max(0.0, min(1.0, mixed)))
    return float(max(0.0, min(1.0, fallback)))


def _search_monthly_target20_gate_for_direction(
    pred_df: pd.DataFrame,
    *,
    direction: str,
    lookup_dir: dict[str, Any] | None,
) -> dict[str, Any]:
    side_col = "p_up_big" if direction == "up" else "p_down_big"
    required_cols = {"dt", "ret1m", side_col}
    if pred_df.empty or not required_cols.issubset(set(pred_df.columns)):
        return {}
    work = pred_df[list(required_cols)].copy()
    for col in required_cols:
        work[col] = pd.to_numeric(work.get(col), errors="coerce")
    finite_mask = np.isfinite(work["dt"].to_numpy(dtype=float, copy=False))
    finite_mask &= np.isfinite(work["ret1m"].to_numpy(dtype=float, copy=False))
    finite_mask &= np.isfinite(work[side_col].to_numpy(dtype=float, copy=False))
    work = work[finite_mask].copy()
    if work.empty:
        return {}

    work["dt"] = work["dt"].astype(int)
    side_prob = work[side_col].to_numpy(dtype=float, copy=False)
    work["p_side20"] = [
        _estimate_monthly_side20_from_lookup(float(p), lookup_dir)
        for p in side_prob
    ]
    work["p_side20"] = pd.to_numeric(work.get("p_side20"), errors="coerce")
    work = work[np.isfinite(work["p_side20"].to_numpy(dtype=float, copy=False))].copy()
    if work.empty:
        return {}

    if direction == "up":
        work["event20"] = work["ret1m"] >= float(MONTHLY_RET20_TARGET)
    else:
        work["event20"] = work["ret1m"] <= -float(MONTHLY_RET20_TARGET)
    total_event = int(work["event20"].sum())
    baseline = float(work["event20"].mean())
    total_months = int(work["dt"].nunique())
    if total_months <= 0:
        return {}

    min_months = max(6, int(math.floor(total_months * MONTHLY_TARGET20_MIN_MONTH_COVERAGE)))
    quantiles = [0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90]
    try:
        quantile_values = [
            float(np.nanquantile(work["p_side20"].to_numpy(dtype=float, copy=False), q)) for q in quantiles
        ]
    except Exception:
        quantile_values = []
    candidate_set = {float(v) for v in MONTHLY_TARGET20_GATE_CANDIDATES}
    candidate_set.update(v for v in quantile_values if math.isfinite(v))
    candidates = sorted(
        max(0.0, min(0.95, float(v))) for v in candidate_set if math.isfinite(float(v))
    )
    if not candidates:
        return {}

    best: dict[str, Any] | None = None
    for gate in candidates:
        selected = work[work["p_side20"] >= float(gate)]
        if selected.empty:
            continue
        month_counts = selected.groupby("dt").size()
        covered_months = int(month_counts.size)
        avg_month_picks = float(month_counts.mean()) if covered_months > 0 else 0.0
        if covered_months < min_months or avg_month_picks < MONTHLY_TARGET20_MIN_AVG_PICKS:
            continue
        precision = float(selected["event20"].mean())
        selected_event = int(selected["event20"].sum())
        recall = float(selected_event / max(1, total_event))
        lift = float(precision / max(1e-9, baseline))
        if lift < MONTHLY_TARGET20_MIN_LIFT and precision <= baseline + 0.01:
            continue
        coverage_ratio = float(covered_months / max(1, total_months))
        size_factor = min(1.0, avg_month_picks / max(1.0, MONTHLY_TARGET20_TARGET_AVG_PICKS))
        score = float(
            (precision - baseline) * (0.60 + 0.25 * coverage_ratio + 0.15 * size_factor)
            + 0.03 * recall
        )
        candidate = {
            "target20_gate": float(gate),
            "baseline_rate": float(baseline),
            "event_rate": float(precision),
            "lift": float(lift),
            "recall": float(recall),
            "avg_month_picks": float(avg_month_picks),
            "covered_months": int(covered_months),
            "total_months": int(total_months),
            "coverage_ratio": float(coverage_ratio),
            "samples": int(len(selected)),
            "score": float(score),
        }
        if best is None:
            best = candidate
            continue
        prev_key = (
            float(best.get("score") or -1e9),
            float(best.get("event_rate") or -1e9),
            float(best.get("lift") or -1e9),
            float(best.get("coverage_ratio") or -1e9),
            float(best.get("target20_gate") or 0.0),
        )
        next_key = (
            float(candidate.get("score") or -1e9),
            float(candidate.get("event_rate") or -1e9),
            float(candidate.get("lift") or -1e9),
            float(candidate.get("coverage_ratio") or -1e9),
            float(candidate.get("target20_gate") or 0.0),
        )
        if next_key > prev_key:
            best = candidate

    if best is not None:
        best["source"] = "backtest_search"
        return best

    fallback_gate = _safe_float(np.nanquantile(work["p_side20"].to_numpy(dtype=float, copy=False), 0.80))
    if fallback_gate is None:
        return {}
    return {
        "target20_gate": float(max(0.0, min(0.95, fallback_gate))),
        "baseline_rate": float(baseline),
        "event_rate": None,
        "lift": None,
        "recall": None,
        "avg_month_picks": None,
        "covered_months": None,
        "total_months": int(total_months),
        "coverage_ratio": None,
        "samples": int(len(work)),
        "score": None,
        "source": "fallback_quantile80",
    }


def _derive_monthly_ret20_lookup(
    train_df: pd.DataFrame,
    models: MonthlyTrainedModels,
    *,
    pred_df: pd.DataFrame | None = None,
) -> dict[str, Any]:
    if train_df.empty or models.abs_cls is None:
        return {}
    pred = pred_df.copy() if pred_df is not None else _predict_monthly_frame(train_df, models)
    if "ret1m" not in pred.columns:
        pred["ret1m"] = pd.to_numeric(train_df.get("ret1m"), errors="coerce")
    return {
        "target_abs_ret": float(MONTHLY_RET20_TARGET),
        "up": _build_monthly_ret20_lookup_for_direction(pred, direction="up"),
        "down": _build_monthly_ret20_lookup_for_direction(pred, direction="down"),
    }


def _load_training_df(conn, start_dt: int | None = None, end_dt: int | None = None) -> pd.DataFrame:
    feature_dt_sql = _normalized_daily_dt_sql("f.dt")
    where = ["l.ret20 IS NOT NULL"]
    params: list[object] = []
    if start_dt is not None:
        where.append(f"{feature_dt_sql} >= ?")
        params.append(int(start_dt))
    if end_dt is not None:
        where.append(f"{feature_dt_sql} <= ?")
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
            f.weekly_breakout_up_prob,
            f.weekly_breakout_down_prob,
            f.weekly_range_prob,
            f.monthly_breakout_up_prob,
            f.monthly_breakout_down_prob,
            f.monthly_range_prob,
            f.candle_triplet_up_prob,
            f.candle_triplet_down_prob,
            f.candle_body_ratio,
            f.candle_upper_wick_ratio,
            f.candle_lower_wick_ratio,
            f.atr14_pct,
            f.range_pct,
            f.gap_pct,
            f.close_ret2,
            f.close_ret3,
            f.close_ret20,
            f.close_ret60,
            f.vol_ret5,
            f.vol_ret20,
            f.vol_ratio5_20,
            f.turnover20,
            f.turnover_z20,
            f.high20_dist,
            f.low20_dist,
            f.breakout20_up,
            f.breakout20_down,
            f.drawdown60,
            f.rebound60,
            f.market_ret1,
            f.market_ret5,
            f.market_ret20,
            f.rel_ret5,
            f.rel_ret20,
            f.breadth_above_ma20,
            f.breadth_above_ma60,
            f.sector_ret5,
            f.sector_ret20,
            f.rel_sector_ret5,
            f.rel_sector_ret20,
            f.sector_breadth_ma20,
            l.ret5,
            l.ret10,
            l.ret20,
            l.up5_label,
            l.up10_label,
            l.up20_label,
            l.train_mask_cls_5,
            l.train_mask_cls_10,
            l.train_mask_cls,
            l.turn_up_label,
            l.turn_down_label_5,
            l.turn_down_label,
            l.turn_down_label_20,
            l.train_mask_turn_5,
            l.train_mask_turn,
            l.train_mask_turn_20
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

    # Non-technical calendar features (weekday/month seasonality and month-turn effects).
    dt_raw = pd.to_numeric(result.get("dt"), errors="coerce")
    dt_series = pd.to_datetime(dt_raw, unit="s", utc=True, errors="coerce")
    ymd_mask = dt_raw.between(19_000_101, 21_001_231, inclusive="both")
    if bool(ymd_mask.fillna(False).any()):
        dt_ymd = pd.to_datetime(
            dt_raw[ymd_mask].astype("Int64").astype(str),
            format="%Y%m%d",
            utc=True,
            errors="coerce",
        )
        dt_series.loc[ymd_mask] = dt_ymd
    valid_cal = dt_series.notna()
    dow = dt_series.dt.weekday
    month = dt_series.dt.month
    day = dt_series.dt.day
    days_in_month = dt_series.dt.days_in_month
    dow_rad = (2.0 * np.pi * dow) / 7.0
    month_rad = (2.0 * np.pi * (month - 1.0)) / 12.0
    result["cal_dow_sin"] = np.where(valid_cal, np.sin(dow_rad), np.nan)
    result["cal_dow_cos"] = np.where(valid_cal, np.cos(dow_rad), np.nan)
    result["cal_month_sin"] = np.where(valid_cal, np.sin(month_rad), np.nan)
    result["cal_month_cos"] = np.where(valid_cal, np.cos(month_rad), np.nan)
    result["cal_month_start"] = np.where(valid_cal, (day <= 5).astype(float), np.nan)
    result["cal_month_end"] = np.where(valid_cal, ((days_in_month - day) <= 4).astype(float), np.nan)
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


def _safe_prob_array(values: np.ndarray) -> np.ndarray:
    arr = np.asarray(values, dtype=float)
    arr = np.where(np.isfinite(arr), arr, 0.5)
    return np.clip(arr, 1e-6, 1.0 - 1e-6)


def _apply_temperature_to_prob_array(values: np.ndarray, temperature: float) -> np.ndarray:
    probs = _safe_prob_array(values)
    try:
        temp = float(temperature)
    except (TypeError, ValueError):
        temp = 1.0
    if not np.isfinite(temp) or temp <= 0.0:
        temp = 1.0
    if abs(temp - 1.0) <= 1e-9:
        return probs
    logits = np.log(probs / (1.0 - probs))
    logits = np.clip(logits / temp, -40.0, 40.0)
    calibrated = 1.0 / (1.0 + np.exp(-logits))
    return _safe_prob_array(calibrated)


def _binary_logloss(y_true: np.ndarray, probs: np.ndarray) -> float:
    y = np.asarray(y_true, dtype=float)
    mask = np.isfinite(y)
    if not np.any(mask):
        return float("inf")
    y = np.clip(y[mask], 0.0, 1.0)
    p = _safe_prob_array(np.asarray(probs, dtype=float)[mask])
    loss = -(y * np.log(p) + (1.0 - y) * np.log(1.0 - p))
    return float(np.mean(loss)) if loss.size else float("inf")


def _fit_temperature_for_binary_classifier(
    model: Any | None,
    train_slice: pd.DataFrame,
    label_col: str,
    medians: dict[str, float],
    feature_columns: list[str],
) -> float:
    if model is None or train_slice.empty or label_col not in train_slice.columns:
        return 1.0
    calib_df = train_slice.copy()
    if "dt" in calib_df.columns:
        dt_series = pd.to_numeric(calib_df.get("dt"), errors="coerce")
        dt_values = np.sort(dt_series.dropna().unique())
        if dt_values.size >= 20:
            n_tail = int(max(10, round(float(dt_values.size) * 0.2)))
            n_tail = min(n_tail, int(dt_values.size))
            tail_dates = set(float(v) for v in dt_values[-n_tail:])
            calib_df = calib_df[dt_series.astype(float).isin(tail_dates)].copy()
        else:
            tail_rows = int(max(120, round(float(len(calib_df)) * 0.2)))
            calib_df = calib_df.tail(tail_rows).copy()
    else:
        tail_rows = int(max(120, round(float(len(calib_df)) * 0.2)))
        calib_df = calib_df.tail(tail_rows).copy()

    if calib_df.empty:
        return 1.0
    y = pd.to_numeric(calib_df.get(label_col), errors="coerce").to_numpy(dtype=float, copy=False)
    valid = np.isfinite(y)
    if int(np.sum(valid)) < 120:
        return 1.0
    y = y[valid]
    positives = int(np.sum(y >= 0.5))
    negatives = int(np.sum(y < 0.5))
    if positives < 20 or negatives < 20:
        return 1.0

    calib_rows = calib_df.loc[valid].copy()
    x_calib, _ = _prepare_feature_matrix(
        calib_rows,
        medians=medians,
        feature_columns=feature_columns,
    )
    raw_probs = np.asarray(model.predict(x_calib.to_numpy(dtype=float)), dtype=float)
    if raw_probs.size != y.size:
        return 1.0

    baseline_loss = _binary_logloss(y, raw_probs)
    if not np.isfinite(baseline_loss):
        return 1.0
    candidate_temps = [0.6, 0.75, 0.9, 1.0, 1.1, 1.25, 1.5, 1.8, 2.2, 2.8]
    best_temp = 1.0
    best_loss = baseline_loss
    for temp in candidate_temps:
        calibrated = _apply_temperature_to_prob_array(raw_probs, temp)
        loss = _binary_logloss(y, calibrated)
        if np.isfinite(loss) and (loss + 1e-8) < best_loss:
            best_loss = float(loss)
            best_temp = float(temp)
    return float(best_temp)


def _project_nonincreasing_triplet(v1: float, v2: float, v3: float) -> tuple[float, float, float]:
    blocks: list[dict[str, float | int]] = [
        {"sum": float(v1), "count": 1},
        {"sum": float(v2), "count": 1},
        {"sum": float(v3), "count": 1},
    ]
    idx = 0
    while idx < len(blocks) - 1:
        left = float(blocks[idx]["sum"]) / float(blocks[idx]["count"])
        right = float(blocks[idx + 1]["sum"]) / float(blocks[idx + 1]["count"])
        if left < right:
            blocks[idx]["sum"] = float(blocks[idx]["sum"]) + float(blocks[idx + 1]["sum"])
            blocks[idx]["count"] = int(blocks[idx]["count"]) + int(blocks[idx + 1]["count"])
            del blocks[idx + 1]
            if idx > 0:
                idx -= 1
        else:
            idx += 1
    projected: list[float] = []
    for block in blocks:
        mean = float(block["sum"]) / float(block["count"])
        projected.extend([mean] * int(block["count"]))
    out = projected[:3]
    return float(out[0]), float(out[1]), float(out[2])


def _enforce_nonincreasing_pup_curve(pred: pd.DataFrame) -> None:
    col5 = _p_up_pred_col(5)
    col10 = _p_up_pred_col(10)
    col20 = _p_up_pred_col(20)
    if col5 not in pred.columns or col10 not in pred.columns or col20 not in pred.columns:
        return
    p5 = pd.to_numeric(pred[col5], errors="coerce").to_numpy(dtype=float, copy=True)
    p10 = pd.to_numeric(pred[col10], errors="coerce").to_numpy(dtype=float, copy=True)
    p20 = pd.to_numeric(pred[col20], errors="coerce").to_numpy(dtype=float, copy=True)
    for idx in range(len(pred)):
        if not (np.isfinite(p5[idx]) and np.isfinite(p10[idx]) and np.isfinite(p20[idx])):
            continue
        y5, y10, y20 = _project_nonincreasing_triplet(float(p5[idx]), float(p10[idx]), float(p20[idx]))
        p5[idx] = y5
        p10[idx] = y10
        p20[idx] = y20
    pred[col5] = np.clip(p5, 0.0, 1.0)
    pred[col10] = np.clip(p10, 0.0, 1.0)
    pred[col20] = np.clip(p20, 0.0, 1.0)


def _fit_models(train_df: pd.DataFrame, cfg: MLConfig) -> TrainedModels:
    lgb = _import_lightgbm()
    if train_df.empty:
        raise RuntimeError("train_df is empty")

    train_df = train_df.copy()
    neutral = float(cfg.neutral_band_pct)
    for horizon in PREDICTION_HORIZONS:
        ret_col = RET_COL_BY_HORIZON[horizon]
        up_col = UP_LABEL_COL_BY_HORIZON[horizon]
        cls_mask_col = CLS_MASK_COL_BY_HORIZON[horizon]
        turn_down_col = TURN_DOWN_COL_BY_HORIZON[horizon]
        turn_mask_col = TURN_MASK_COL_BY_HORIZON[horizon]

        if ret_col not in train_df.columns:
            train_df[ret_col] = np.nan
        train_df[ret_col] = pd.to_numeric(train_df.get(ret_col), errors="coerce")

        if up_col not in train_df.columns:
            train_df[up_col] = (train_df[ret_col] > 0).astype(int)
        else:
            train_df[up_col] = pd.to_numeric(train_df.get(up_col), errors="coerce").fillna(0)

        if cls_mask_col not in train_df.columns:
            train_df[cls_mask_col] = (train_df[ret_col].abs() >= neutral).astype(int)
        else:
            train_df[cls_mask_col] = pd.to_numeric(train_df.get(cls_mask_col), errors="coerce").fillna(0)

        if turn_down_col not in train_df.columns:
            train_df[turn_down_col] = 0
        else:
            train_df[turn_down_col] = pd.to_numeric(train_df.get(turn_down_col), errors="coerce").fillna(0)

        if turn_mask_col not in train_df.columns:
            train_df[turn_mask_col] = (
                (train_df[turn_down_col] == 1) | (train_df[ret_col].abs() >= neutral)
            ).astype(int)
        else:
            train_df[turn_mask_col] = pd.to_numeric(train_df.get(turn_mask_col), errors="coerce").fillna(0)

    train_df["turn_up_label"] = pd.to_numeric(train_df.get("turn_up_label"), errors="coerce").fillna(0)
    train_df = train_df[np.isfinite(train_df[RET_COL_BY_HORIZON[20]].to_numpy(dtype=float, copy=False))]
    if train_df.empty:
        raise RuntimeError("No valid ret20 rows in training data")

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
    rank_params = {
        "objective": "lambdarank",
        "metric": "ndcg",
        "ndcg_eval_at": [5, 10, 20],
        "learning_rate": 0.05,
        "num_leaves": 63,
        "feature_fraction": 0.9,
        "bagging_fraction": 0.9,
        "bagging_freq": 1,
        "min_data_in_leaf": 80,
        "label_gain": [0, 1, 3, 7, 15],
        "verbosity": -1,
    }

    base_cls_df = train_df[train_df[CLS_MASK_COL_BY_HORIZON[20]] == 1].copy()
    if len(base_cls_df) < 200:
        raise RuntimeError("Insufficient classification rows (train_mask_cls=1)")
    x_base_cls, medians = _prepare_feature_matrix(base_cls_df, medians=None, feature_columns=FEATURE_COLUMNS)
    y_base_cls = base_cls_df[UP_LABEL_COL_BY_HORIZON[20]].astype(int).to_numpy()
    cls_base_train = lgb.Dataset(
        x_base_cls.to_numpy(dtype=float),
        label=y_base_cls,
        feature_name=FEATURE_COLUMNS,
        free_raw_data=False,
    )
    cls20_model = lgb.train(cls_params, cls_base_train, num_boost_round=int(cfg.cls_boost_round))

    base_reg_df = train_df[np.isfinite(train_df[RET_COL_BY_HORIZON[20]].to_numpy(dtype=float, copy=False))].copy()
    x_base_reg, _ = _prepare_feature_matrix(base_reg_df, medians=medians, feature_columns=FEATURE_COLUMNS)
    y_base_reg = base_reg_df[RET_COL_BY_HORIZON[20]].astype(float).to_numpy()
    reg_base_train = lgb.Dataset(
        x_base_reg.to_numpy(dtype=float),
        label=y_base_reg,
        feature_name=FEATURE_COLUMNS,
        free_raw_data=False,
    )
    reg20_model = lgb.train(reg_params, reg_base_train, num_boost_round=int(cfg.reg_boost_round))

    cls_models: dict[int, Any] = {20: cls20_model}
    reg_models: dict[int, Any] = {20: reg20_model}
    n_train_cls_by_h: dict[int, int] = {20: int(len(base_cls_df))}
    n_train_reg_by_h: dict[int, int] = {20: int(len(base_reg_df))}
    cls_train_slices: dict[int, pd.DataFrame] = {
        20: base_cls_df.sort_values(["dt", "code"]).reset_index(drop=True)
    }

    for horizon in (5, 10):
        ret_col = RET_COL_BY_HORIZON[horizon]
        up_col = UP_LABEL_COL_BY_HORIZON[horizon]
        mask_col = CLS_MASK_COL_BY_HORIZON[horizon]

        cls_df = train_df[
            (train_df[mask_col] == 1) & np.isfinite(train_df[ret_col].to_numpy(dtype=float, copy=False))
        ].copy()
        cls_df = cls_df.sort_values(["dt", "code"]).reset_index(drop=True)
        cls_train_slices[horizon] = cls_df
        n_train_cls_by_h[horizon] = int(len(cls_df))
        if len(cls_df) >= 200:
            y_cls = cls_df[up_col].astype(int).to_numpy()
            positives = int(np.sum(y_cls == 1))
            negatives = int(np.sum(y_cls == 0))
            if positives >= 40 and negatives >= 40:
                x_cls, _ = _prepare_feature_matrix(cls_df, medians=medians, feature_columns=FEATURE_COLUMNS)
                cls_train = lgb.Dataset(
                    x_cls.to_numpy(dtype=float),
                    label=y_cls,
                    feature_name=FEATURE_COLUMNS,
                    free_raw_data=False,
                )
                cls_models[horizon] = lgb.train(cls_params, cls_train, num_boost_round=int(cfg.cls_boost_round))

        reg_df = train_df[np.isfinite(train_df[ret_col].to_numpy(dtype=float, copy=False))].copy()
        n_train_reg_by_h[horizon] = int(len(reg_df))
        if len(reg_df) >= 200:
            x_reg, _ = _prepare_feature_matrix(reg_df, medians=medians, feature_columns=FEATURE_COLUMNS)
            y_reg = reg_df[ret_col].astype(float).to_numpy()
            reg_train = lgb.Dataset(
                x_reg.to_numpy(dtype=float),
                label=y_reg,
                feature_name=FEATURE_COLUMNS,
                free_raw_data=False,
            )
            reg_models[horizon] = lgb.train(reg_params, reg_train, num_boost_round=int(cfg.reg_boost_round))

    cls_temperature_by_h: dict[int, float] = {}
    for horizon in PREDICTION_HORIZONS:
        cls_temperature_by_h[horizon] = float(
            _fit_temperature_for_binary_classifier(
                model=cls_models.get(horizon),
                train_slice=cls_train_slices.get(horizon, pd.DataFrame()),
                label_col=UP_LABEL_COL_BY_HORIZON[horizon],
                medians=medians,
                feature_columns=FEATURE_COLUMNS,
            )
        )

    def _fit_turn_binary(label_col: str, mask_col: str) -> tuple[Any | None, int]:
        turn_df = train_df[train_df[mask_col] == 1].copy()
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
        model = lgb.train(turn_params, turn_train, num_boost_round=int(cfg.cls_boost_round))
        return model, int(len(turn_df))

    def _build_relevance(values: np.ndarray, bins: int = 5) -> np.ndarray:
        if values.size <= 0:
            return np.array([], dtype=int)
        if values.size == 1:
            return np.array([bins - 1], dtype=int)
        order = np.argsort(values, kind="mergesort")
        ranks = np.empty(values.size, dtype=float)
        ranks[order] = np.arange(values.size, dtype=float)
        pct = ranks / max(1.0, float(values.size - 1))
        labels = np.floor(pct * bins).astype(int)
        return np.clip(labels, 0, bins - 1)

    def _build_side_relevance(values: np.ndarray, side: str) -> np.ndarray:
        labels = np.zeros(values.size, dtype=int)
        if values.size <= 0:
            return labels
        if side == "up":
            mask = values > 0.0
            side_values = values[mask]
        else:
            mask = values < 0.0
            side_values = -values[mask]
        if side_values.size <= 0:
            return labels
        idx = np.where(mask)[0]
        if side_values.size == 1:
            labels[idx[0]] = 4
            return labels
        # 0 is reserved for opposite-side/no-edge rows.
        side_labels = _build_relevance(side_values, bins=4) + 1
        labels[idx] = np.clip(side_labels, 1, 4)
        return labels

    rank_up_model: Any | None = None
    rank_down_model: Any | None = None
    n_train_rank = 0
    n_train_rank_groups = 0

    rank_df = train_df[np.isfinite(train_df[RET_COL_BY_HORIZON[20]].to_numpy(dtype=float, copy=False))].copy()
    if not rank_df.empty:
        rank_df = rank_df.sort_values(["dt", "code"]).reset_index(drop=True)
        n_train_rank = int(len(rank_df))
        group_sizes = rank_df.groupby("dt", sort=True).size().astype(int).tolist()
        n_train_rank_groups = int(len(group_sizes))
        if n_train_rank >= 2000 and n_train_rank_groups >= 30:
            x_rank, _ = _prepare_feature_matrix(rank_df, medians=medians, feature_columns=FEATURE_COLUMNS)
            ret20_values = rank_df[RET_COL_BY_HORIZON[20]].astype(float).to_numpy()
            rel_up = np.zeros_like(ret20_values, dtype=int)
            rel_down = np.zeros_like(ret20_values, dtype=int)
            pos = 0
            for g in group_sizes:
                nxt = pos + int(g)
                group_values = ret20_values[pos:nxt]
                rel_up[pos:nxt] = _build_side_relevance(group_values, side="up")
                rel_down[pos:nxt] = _build_side_relevance(group_values, side="down")
                pos = nxt
            rank_train_up = lgb.Dataset(
                x_rank.to_numpy(dtype=float),
                label=rel_up,
                group=group_sizes,
                feature_name=FEATURE_COLUMNS,
                free_raw_data=False,
            )
            rank_train_down = lgb.Dataset(
                x_rank.to_numpy(dtype=float),
                label=rel_down,
                group=group_sizes,
                feature_name=FEATURE_COLUMNS,
                free_raw_data=False,
            )
            rank_boost_round = int(max(20, int(getattr(cfg, "rank_boost_round", 300))))
            rank_up_model = lgb.train(rank_params, rank_train_up, num_boost_round=rank_boost_round)
            rank_down_model = lgb.train(rank_params, rank_train_down, num_boost_round=rank_boost_round)

    turn_up_model, n_turn_up = _fit_turn_binary("turn_up_label", TURN_MASK_COL_BY_HORIZON[10])
    turn_down_models: dict[int, Any | None] = {}
    n_train_turn_down_by_h: dict[int, int] = {}
    for horizon in PREDICTION_HORIZONS:
        label_col = TURN_DOWN_COL_BY_HORIZON[horizon]
        mask_col = TURN_MASK_COL_BY_HORIZON[horizon]
        model, n_rows = _fit_turn_binary(label_col, mask_col)
        turn_down_models[horizon] = model
        n_train_turn_down_by_h[horizon] = int(n_rows)

    turn_down_model_10 = turn_down_models.get(10)
    return TrainedModels(
        cls=cls20_model,
        reg=reg20_model,
        turn_up=turn_up_model,
        turn_down=turn_down_model_10,
        rank_up=rank_up_model,
        rank_down=rank_down_model,
        cls_by_horizon=cls_models,
        cls_temperature_by_horizon=cls_temperature_by_h,
        reg_by_horizon=reg_models,
        turn_down_by_horizon=turn_down_models,
        feature_columns=list(FEATURE_COLUMNS),
        medians=medians,
        n_train_cls=int(n_train_cls_by_h.get(20, len(base_cls_df))),
        n_train_reg=int(n_train_reg_by_h.get(20, len(base_reg_df))),
        n_train_turn_up=int(n_turn_up),
        n_train_turn_down=int(n_train_turn_down_by_h.get(10, 0)),
        n_train_rank=int(n_train_rank),
        n_train_rank_groups=int(n_train_rank_groups),
        n_train_cls_by_horizon=n_train_cls_by_h,
        n_train_reg_by_horizon=n_train_reg_by_h,
        n_train_turn_down_by_horizon=n_train_turn_down_by_h,
    )


def _predict_frame(df: pd.DataFrame, models: TrainedModels, cfg: MLConfig) -> pd.DataFrame:
    matrix, _ = _prepare_feature_matrix(
        df,
        medians=models.medians,
        feature_columns=models.feature_columns,
    )
    pred = df.copy()
    matrix_np = matrix.to_numpy(dtype=float)
    _turnover_raw = pred.get("turnover20")
    if isinstance(_turnover_raw, pd.Series):
        turnover_series = pd.to_numeric(_turnover_raw, errors="coerce")
    else:
        turnover_series = pd.Series([_turnover_raw] * len(pred), index=pred.index, dtype=float)
    long_cost_rate = turnover_series.apply(
        lambda v: _trade_cost_rate(base_cost_rate=cfg.cost_rate, turnover20=_safe_float(v), side="long")
    )

    cls_models = {int(k): v for k, v in (models.cls_by_horizon or {}).items() if v is not None}
    reg_models = {int(k): v for k, v in (models.reg_by_horizon or {}).items() if v is not None}
    turn_down_models = {int(k): v for k, v in (models.turn_down_by_horizon or {}).items()}
    cls_temperatures = {
        int(k): float(v)
        for k, v in (models.cls_temperature_by_horizon or {}).items()
        if v is not None
    }
    if 20 not in cls_models:
        cls_models[20] = models.cls
    if 20 not in reg_models:
        reg_models[20] = models.reg
    if 10 not in turn_down_models:
        turn_down_models[10] = models.turn_down
    for horizon in PREDICTION_HORIZONS:
        cls_temperatures.setdefault(horizon, 1.0)

    for horizon in PREDICTION_HORIZONS:
        pup_col = _p_up_pred_col(horizon)
        ret_col = _ret_pred_col(horizon)
        ev_col = _ev_col(horizon)
        ev_net_col = _ev_net_col(horizon)

        cls_model = cls_models.get(horizon)
        reg_model = reg_models.get(horizon)
        if cls_model is None:
            pred[pup_col] = np.nan
        else:
            raw_p = np.asarray(cls_model.predict(matrix_np), dtype=float)
            pred[pup_col] = _apply_temperature_to_prob_array(raw_p, cls_temperatures.get(horizon, 1.0))
        if reg_model is None:
            pred[ret_col] = np.nan
        else:
            pred[ret_col] = reg_model.predict(matrix_np)
        pred[ev_col] = pred[ret_col]
        pred[ev_net_col] = pred[ev_col] - long_cost_rate

    _enforce_nonincreasing_pup_curve(pred)
    pred["p_up"] = pred[_p_up_pred_col(20)]
    pred["p_down"] = 1.0 - pd.to_numeric(pred["p_up"], errors="coerce").fillna(0.5)
    pred["ret_pred20"] = pred[_ret_pred_col(20)]
    pred["ev20"] = pred[_ev_col(20)]
    pred["ev20_net"] = pred[_ev_net_col(20)]

    if models.rank_up is not None:
        pred["rank_up_20"] = models.rank_up.predict(matrix_np)
    else:
        ev_rank = pd.to_numeric(pred["ev20_net"], errors="coerce")
        pred["rank_up_20"] = ev_rank.groupby(pred["dt"]).rank(pct=True, method="average")

    if models.rank_down is not None:
        pred["rank_down_20"] = models.rank_down.predict(matrix_np)
    else:
        ev_rank = pd.to_numeric(pred["ev20_net"], errors="coerce")
        pred["rank_down_20"] = (-ev_rank).groupby(pred["dt"]).rank(pct=True, method="average")

    for horizon in PREDICTION_HORIZONS:
        col = _turn_down_pred_col(horizon)
        model = turn_down_models.get(horizon)
        if model is not None:
            pred[col] = model.predict(matrix_np)
        elif horizon == 10:
            fallback_up = _p_up_pred_col(10) if _p_up_pred_col(10) in pred.columns else "p_up"
            fallback_series = pd.to_numeric(pred[fallback_up], errors="coerce")
            if fallback_series.isna().all():
                fallback_series = pd.to_numeric(pred["p_up"], errors="coerce")
            pred[col] = 1.0 - fallback_series.fillna(0.5)
        else:
            pred[col] = np.nan
    pred["p_turn_down_10"] = pred[_turn_down_pred_col(10)]
    pred["p_turn_down"] = pred[_turn_down_pred_col(10)]

    # Downside probability blends directional classifier output and
    # short-pattern continuation probability to reflect counter-trend entries.
    p_down_base = pd.to_numeric(pred["p_down"], errors="coerce").fillna(0.5)
    p_turn_down = pd.to_numeric(pred["p_turn_down_10"], errors="coerce")
    p_down_blended = p_down_base.copy()
    valid_turn = p_turn_down.notna()
    if bool(valid_turn.any()):
        p_down_blended.loc[valid_turn] = (
            0.40 * p_down_base.loc[valid_turn] + 0.60 * p_turn_down.loc[valid_turn]
        )
    pred["p_down"] = p_down_blended.clip(lower=0.0, upper=1.0)

    if models.turn_up is not None:
        pred["p_turn_up"] = models.turn_up.predict(matrix_np)
    else:
        fallback_up = _p_up_pred_col(10) if _p_up_pred_col(10) in pred.columns else "p_up"
        pred["p_turn_up"] = pd.to_numeric(pred[fallback_up], errors="coerce").fillna(0.5)
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

    def _pdown_value(item: dict[str, Any]) -> float | None:
        p_down = _safe_float(item.get("p_down")) or _safe_float(item.get("mlPDown"))
        if p_down is not None:
            return p_down
        p_up = _pup_value(item)
        if p_up is None:
            return None
        return max(0.0, min(1.0, 1.0 - float(p_up)))

    def _rank_up_value(item: dict[str, Any]) -> float | None:
        return _safe_float(item.get("rank_up_20")) or _safe_float(item.get("mlRankUp"))

    def _rank_down_value(item: dict[str, Any]) -> float | None:
        return _safe_float(item.get("rank_down_20")) or _safe_float(item.get("mlRankDown"))

    def _down_ev_ok(item: dict[str, Any]) -> bool:
        ev = _ev_value(item)
        return ev is None or float(ev) <= 0.0

    if top_n <= 0:
        return []
    valid = [
        item
        for item in items
        if (_ev_value(item) is not None or _rank_up_value(item) is not None or _rank_down_value(item) is not None)
    ]
    if not valid:
        return []

    if direction == "down":
        preferred = [
            item
            for item in valid
            if (_pdown_value(item) is None and _rank_down_value(item) is not None and _down_ev_ok(item))
            or (_pdown_value(item) is not None and float(_pdown_value(item) or 0.0) >= p_up_threshold)
            and _down_ev_ok(item)
        ]
        has_rank = any(_rank_down_value(item) is not None for item in preferred)
        if has_rank:
            ordered = sorted(
                preferred,
                key=lambda item: (
                    _rank_down_value(item) is None,
                    -float(_rank_down_value(item) or 0.0),
                    float(_ev_value(item) or 0.0),
                    -(float(_pdown_value(item) or 0.0)),
                    item.get("code") or "",
                ),
            )
        else:
            ordered = sorted(
                preferred,
                key=lambda item: (
                    float(_ev_value(item) or 0.0),
                    -(float(_pdown_value(item) or 0.0)),
                    item.get("code") or "",
                ),
            )
        selected = ordered[:top_n]
        if len(selected) >= top_n:
            return selected
        selected_codes = {str(item.get("code")) for item in selected}
        remaining = [item for item in valid if str(item.get("code")) not in selected_codes]
        has_rank_remaining = any(_rank_down_value(item) is not None for item in remaining)
        remaining_sorted = sorted(
            remaining,
            key=lambda item: (
                _rank_down_value(item) is None if has_rank_remaining else False,
                -(float(_rank_down_value(item) or 0.0)) if has_rank_remaining else 0.0,
                float(_ev_value(item) or 0.0),
                -(float(_pdown_value(item) or 0.0)),
                item.get("code") or "",
            ),
        )
        selected.extend(remaining_sorted[: max(0, top_n - len(selected))])
        return selected

    preferred = [
        item
        for item in valid
        if (_pup_value(item) is None and _rank_up_value(item) is not None)
        or (_pup_value(item) is not None and float(_pup_value(item) or 0.0) >= p_up_threshold)
    ]
    has_rank_up = any(_rank_up_value(item) is not None for item in preferred)
    preferred_sorted = sorted(
        preferred,
        key=lambda item: (
            _rank_up_value(item) is None if has_rank_up else False,
            -(float(_rank_up_value(item) or 0.0)) if has_rank_up else 0.0,
            -(float(_ev_value(item) or 0.0)),
            item.get("code") or "",
        ),
    )
    selected = preferred_sorted[:top_n]
    if len(selected) >= top_n:
        return selected

    selected_codes = {str(item.get("code")) for item in selected}
    remaining = [item for item in valid if str(item.get("code")) not in selected_codes]
    has_rank_up_remaining = any(_rank_up_value(item) is not None for item in remaining)
    remaining_sorted = sorted(
        remaining,
        key=lambda item: (
            _rank_up_value(item) is None if has_rank_up_remaining else False,
            -(float(_rank_up_value(item) or 0.0)) if has_rank_up_remaining else 0.0,
            -(float(_ev_value(item) or 0.0)),
            item.get("code") or "",
        ),
    )
    selected.extend(remaining_sorted[: max(0, top_n - len(selected))])
    return selected


def _walk_forward_eval(
    df: pd.DataFrame,
    cfg: MLConfig,
    *,
    progress_cb: Callable[[int, int], None] | None = None,
) -> dict[str, Any]:
    def _empty_payload() -> dict[str, Any]:
        return {
            "fold_count": 0,
            "daily_count": 0,
            "top30_mean_ret20_net": None,
            "top30_win_rate": None,
            "top30_median_ret20_net": None,
            "top30_p05_ret20_net": None,
            "top30_cvar05_ret20_net": None,
            "top30_lcb95_ret20_net": None,
            "top30_p_value_mean_gt0": None,
            "up_daily_count": 0,
            "up_mean_ret20_net": None,
            "up_win_rate": None,
            "down_daily_count": 0,
            "down_mean_ret20_net": None,
            "down_win_rate": None,
            "combined_daily_count": 0,
            "combined_mean_ret20_net": None,
            "combined_win_rate": None,
            "turn_long_mean_ret10_proxy_net": None,
            "turn_long_win_rate": None,
            "turn_short_mean_ret10_proxy_net": None,
            "turn_short_win_rate": None,
            "folds": [],
        }

    if df.empty:
        return _empty_payload()

    all_dates = sorted(int(v) for v in pd.Series(df["dt"]).dropna().unique().tolist())
    windows = build_walk_forward_windows(all_dates, cfg)
    if not windows:
        return _empty_payload()

    daily_scores_up: list[float] = []
    daily_scores_down: list[float] = []
    daily_scores_combined: list[float] = []
    daily_turn_long_scores: list[float] = []
    daily_turn_short_scores: list[float] = []
    fold_rows: list[dict[str, Any]] = []
    use_expanding = bool(getattr(cfg, "wf_use_expanding_train", True))
    wf_max_train_days = int(max(0, int(getattr(cfg, "wf_max_train_days", 0))))

    total_windows = int(len(windows))
    for index, window in enumerate(windows, start=1):
        if use_expanding:
            train_dates_seq = [d for d in all_dates if d <= int(window["train_end_dt"])]
            if wf_max_train_days > 0 and len(train_dates_seq) > wf_max_train_days:
                train_dates_seq = train_dates_seq[-wf_max_train_days:]
            train_dates = set(train_dates_seq)
        else:
            train_dates = set(window["train_dates"])
        test_dates = window["test_dates"]
        train_df = df[df["dt"].isin(train_dates)].copy()
        test_df = df[df["dt"].isin(test_dates)].copy()
        if train_df.empty or test_df.empty:
            if progress_cb is not None:
                progress_cb(index, total_windows)
            continue

        try:
            models = _fit_models(train_df, cfg)
        except Exception:
            if progress_cb is not None:
                progress_cb(index, total_windows)
            continue

        pred_df = _predict_frame(test_df, models, cfg)
        fold_up: list[float] = []
        fold_down: list[float] = []
        fold_combined: list[float] = []
        fold_turn_long: list[float] = []
        fold_turn_short: list[float] = []
        for _dt_value, group in pred_df.groupby("dt"):
            rows = group.to_dict(orient="records")
            selected_up = select_top_n_ml(
                rows,
                top_n=int(cfg.top_n),
                p_up_threshold=float(cfg.p_up_threshold),
                direction="up",
            )
            selected_down = select_top_n_ml(
                rows,
                top_n=int(cfg.top_n),
                p_up_threshold=float(cfg.min_prob_down),
                direction="down",
            )

            up_realized: list[float] = []
            for item in selected_up:
                ret20 = _safe_float(item.get("ret20"))
                if ret20 is None:
                    continue
                cost_rate = _trade_cost_rate(
                    base_cost_rate=cfg.cost_rate,
                    turnover20=_safe_float(item.get("turnover20")),
                    side="long",
                )
                up_realized.append(compute_ev20_net(ret20, cost_rate))
            down_realized: list[float] = []
            for item in selected_down:
                ret20 = _safe_float(item.get("ret20"))
                if ret20 is None:
                    continue
                cost_rate = _trade_cost_rate(
                    base_cost_rate=cfg.cost_rate,
                    turnover20=_safe_float(item.get("turnover20")),
                    side="short",
                )
                down_realized.append(compute_ev20_net(-ret20, cost_rate))

            up_score = float(np.mean(up_realized)) if up_realized else None
            down_score = float(np.mean(down_realized)) if down_realized else None
            if up_score is not None:
                daily_scores_up.append(up_score)
                fold_up.append(up_score)
            if down_score is not None:
                daily_scores_down.append(down_score)
                fold_down.append(down_score)
            if up_score is not None and down_score is not None:
                combo_score = float(0.5 * up_score + 0.5 * down_score)
                daily_scores_combined.append(combo_score)
                fold_combined.append(combo_score)

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
                cost_rate = _trade_cost_rate(
                    base_cost_rate=cfg.cost_rate,
                    turnover20=_safe_float(item.get("turnover20")),
                    side="long",
                )
                turn_long_realized.append(compute_ev20_net(ret20, cost_rate))
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
                cost_rate = _trade_cost_rate(
                    base_cost_rate=cfg.cost_rate,
                    turnover20=_safe_float(item.get("turnover20")),
                    side="short",
                )
                turn_short_realized.append(compute_ev20_net(-ret20, cost_rate))
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
                "up_daily_count": len(fold_up),
                "up_mean_ret20_net": float(np.mean(fold_up)) if fold_up else None,
                "down_daily_count": len(fold_down),
                "down_mean_ret20_net": float(np.mean(fold_down)) if fold_down else None,
                "combined_daily_count": len(fold_combined),
                "combined_mean_ret20_net": float(np.mean(fold_combined)) if fold_combined else None,
                "turn_long_mean_ret10_proxy_net": float(np.mean(fold_turn_long)) if fold_turn_long else None,
                "turn_short_mean_ret10_proxy_net": float(np.mean(fold_turn_short)) if fold_turn_short else None,
            }
        )
        if progress_cb is not None:
            progress_cb(index, total_windows)

    up_summary = _summarize_daily_scores(daily_scores_up)
    down_summary = _summarize_daily_scores(daily_scores_down)
    combined_summary = _summarize_daily_scores(daily_scores_combined)
    primary_summary = combined_summary if int(combined_summary.get("daily_count") or 0) > 0 else up_summary
    arr_turn_long = np.array(daily_turn_long_scores, dtype=float) if daily_turn_long_scores else np.array([], dtype=float)
    arr_turn_short = np.array(daily_turn_short_scores, dtype=float) if daily_turn_short_scores else np.array([], dtype=float)

    return {
        "fold_count": len(fold_rows),
        **primary_summary,
        "up_daily_count": int(up_summary.get("daily_count") or 0),
        "up_mean_ret20_net": up_summary.get("top30_mean_ret20_net"),
        "up_win_rate": up_summary.get("top30_win_rate"),
        "up_p05_ret20_net": up_summary.get("top30_p05_ret20_net"),
        "up_cvar05_ret20_net": up_summary.get("top30_cvar05_ret20_net"),
        "up_lcb95_ret20_net": up_summary.get("top30_lcb95_ret20_net"),
        "up_p_value_mean_gt0": up_summary.get("top30_p_value_mean_gt0"),
        "down_daily_count": int(down_summary.get("daily_count") or 0),
        "down_mean_ret20_net": down_summary.get("top30_mean_ret20_net"),
        "down_win_rate": down_summary.get("top30_win_rate"),
        "down_p05_ret20_net": down_summary.get("top30_p05_ret20_net"),
        "down_cvar05_ret20_net": down_summary.get("top30_cvar05_ret20_net"),
        "down_lcb95_ret20_net": down_summary.get("top30_lcb95_ret20_net"),
        "down_p_value_mean_gt0": down_summary.get("top30_p_value_mean_gt0"),
        "combined_daily_count": int(combined_summary.get("daily_count") or 0),
        "combined_mean_ret20_net": combined_summary.get("top30_mean_ret20_net"),
        "combined_win_rate": combined_summary.get("top30_win_rate"),
        "combined_p05_ret20_net": combined_summary.get("top30_p05_ret20_net"),
        "combined_cvar05_ret20_net": combined_summary.get("top30_cvar05_ret20_net"),
        "combined_lcb95_ret20_net": combined_summary.get("top30_lcb95_ret20_net"),
        "combined_p_value_mean_gt0": combined_summary.get("top30_p_value_mean_gt0"),
        "turn_long_mean_ret10_proxy_net": float(np.mean(arr_turn_long)) if arr_turn_long.size else None,
        "turn_long_win_rate": float(np.mean(arr_turn_long > 0)) if arr_turn_long.size else None,
        "turn_short_mean_ret10_proxy_net": float(np.mean(arr_turn_short)) if arr_turn_short.size else None,
        "turn_short_win_rate": float(np.mean(arr_turn_short > 0)) if arr_turn_short.size else None,
        "folds": fold_rows,
    }


def _extract_walk_forward_metrics_from_registry_row(row: tuple | None) -> dict[str, Any] | None:
    if not row or len(row) < 8:
        return None
    try:
        metrics_json = json.loads(row[7]) if row[7] else {}
    except Exception:
        return None
    if not isinstance(metrics_json, dict):
        return None
    wf = metrics_json.get("walk_forward")
    if isinstance(wf, dict):
        return wf
    if "top30_mean_ret20_net" in metrics_json:
        return metrics_json
    return None


def _load_recent_labeled_predictions(
    conn,
    *,
    model_version: str,
    lookback_days: int,
) -> pd.DataFrame:
    return conn.execute(
        """
        WITH recent_dt AS (
            SELECT DISTINCT dt
            FROM ml_pred_20d
            WHERE model_version = ?
            ORDER BY dt DESC
            LIMIT ?
        )
        SELECT
            p.dt AS dt,
            p.code AS code,
            p.p_up AS p_up,
            p.ev20_net AS ev20_net,
            l.ret20 AS ret20,
            f.turnover20 AS turnover20
        FROM ml_pred_20d p
        JOIN recent_dt r ON r.dt = p.dt
        LEFT JOIN ml_label_20d l ON l.dt = p.dt AND l.code = p.code
        LEFT JOIN ml_feature_daily f ON f.dt = p.dt AND f.code = p.code
        WHERE p.model_version = ?
        ORDER BY p.dt, p.code
        """,
        [str(model_version), int(max(1, lookback_days)), str(model_version)],
    ).df()


def _evaluate_live_metrics_for_model(
    conn,
    *,
    model_version: str,
    cfg: MLConfig,
) -> dict[str, Any]:
    frame = _load_recent_labeled_predictions(
        conn,
        model_version=model_version,
        lookback_days=cfg.live_guard_lookback_days,
    )
    if frame.empty:
        summary = _summarize_daily_scores([])
        return {
            "model_version": str(model_version),
            **summary,
        }

    daily_scores: list[float] = []
    for _dt, group in frame.groupby("dt"):
        selected = select_top_n_ml(
            group.to_dict(orient="records"),
            top_n=int(cfg.top_n),
            p_up_threshold=float(cfg.p_up_threshold),
            direction="up",
        )
        realized: list[float] = []
        for item in selected:
            ret20 = _safe_float(item.get("ret20"))
            if ret20 is None:
                continue
            cost_rate = _trade_cost_rate(
                base_cost_rate=cfg.cost_rate,
                turnover20=_safe_float(item.get("turnover20")),
                side="long",
            )
            realized.append(compute_ev20_net(ret20, cost_rate))
        if realized:
            daily_scores.append(float(np.mean(realized)))

    summary = _summarize_daily_scores(daily_scores)
    return {
        "model_version": str(model_version),
        **summary,
    }


def _find_best_fallback_model(
    conn,
    *,
    exclude_model_version: str,
    cfg: MLConfig,
) -> dict[str, Any] | None:
    promoted_versions: set[str] = set()
    if _table_exists(conn, "ml_training_audit"):
        promoted_rows = conn.execute(
            """
            SELECT DISTINCT model_version
            FROM ml_training_audit
            WHERE promoted = TRUE
            """
        ).fetchall()
        promoted_versions = {str(row[0]) for row in promoted_rows if row and row[0]}
    rows = conn.execute(
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
        WHERE model_key = ? AND is_active = FALSE AND model_version <> ?
        ORDER BY created_at DESC
        """,
        [MODEL_KEY, str(exclude_model_version)],
    ).fetchall()
    if not rows:
        return None

    def _is_promoted(row: tuple) -> tuple[bool, dict[str, Any]]:
        model_version = str(row[0])
        metrics_payload: dict[str, Any] = {}
        try:
            metrics_payload = json.loads(row[7]) if row[7] else {}
        except Exception:
            metrics_payload = {}
        promoted_by_metrics = bool(metrics_payload.get("promoted"))
        promotion_payload = metrics_payload.get("promotion")
        if not promoted_by_metrics and isinstance(promotion_payload, dict):
            promoted_by_metrics = bool(promotion_payload.get("promoted"))
        return model_version in promoted_versions or promoted_by_metrics, metrics_payload

    def _pick_best(
        *,
        require_promoted: bool,
        enforce_guard_floor: bool,
        source: str,
    ) -> dict[str, Any] | None:
        best_item: dict[str, Any] | None = None
        best_key: tuple[float, float, float, str] | None = None
        for row in rows:
            model_version = str(row[0])
            is_promoted_model, _ = _is_promoted(row)
            if require_promoted and not is_promoted_model:
                continue

            wf = _extract_walk_forward_metrics_from_registry_row(row)
            robust = _robust_lb_from_metrics(wf, cfg)
            mean_ret = _safe_float((wf or {}).get("top30_mean_ret20_net"))
            lcb95 = _safe_float((wf or {}).get("top30_lcb95_ret20_net"))
            if robust is None and mean_ret is None and lcb95 is None:
                continue

            if enforce_guard_floor:
                robust_val = float(robust) if robust is not None else float("-inf")
                mean_val = float(mean_ret) if mean_ret is not None else float("-inf")
                lcb_val = float(lcb95) if lcb95 is not None else float("-inf")
                if (
                    robust_val < float(cfg.live_guard_min_robust_lb)
                    and mean_val < float(cfg.live_guard_min_mean_ret20_net)
                    and lcb_val < float(cfg.live_guard_min_lcb95_ret20_net)
                ):
                    continue

            score_robust = float(robust) if robust is not None else float("-inf")
            score_lcb = float(lcb95) if lcb95 is not None else float("-inf")
            score_mean = float(mean_ret) if mean_ret is not None else float("-inf")
            key = (score_robust, score_lcb, score_mean, model_version)
            if best_key is None or key > best_key:
                best_key = key
                best_item = {
                    "model_version": model_version,
                    "robust_lb": robust,
                    "mean_ret20_net": mean_ret,
                    "lcb95_ret20_net": lcb95,
                    "created_at": row[10],
                    "selection_source": source,
                }
        return best_item

    # Pass-1: prefer promoted models when promotion history exists.
    if promoted_versions:
        promoted_best = _pick_best(
            require_promoted=True,
            enforce_guard_floor=False,
            source="promoted_only",
        )
        if promoted_best is not None:
            return promoted_best

    # Pass-2: if no promoted fallback exists, allow non-promoted candidates
    # that at least clear one live-guard floor condition.
    guarded_best = _pick_best(
        require_promoted=False,
        enforce_guard_floor=True,
        source="guard_floor",
    )
    if guarded_best is not None:
        return guarded_best

    # Pass-3: final permissive fallback to keep rollback path available.
    permissive = _pick_best(
        require_promoted=False,
        enforce_guard_floor=False,
        source="permissive",
    )
    if permissive is not None:
        return permissive
    latest_row = rows[0]
    return {
        "model_version": str(latest_row[0]),
        "robust_lb": None,
        "mean_ret20_net": None,
        "lcb95_ret20_net": None,
        "created_at": latest_row[10],
        "selection_source": "latest_registry_no_metrics",
    }


def _save_live_guard_audit(
    conn,
    *,
    run_id: str,
    checked_at: datetime,
    active_model_version: str | None,
    passed: bool,
    action: str,
    reason: str,
    metrics: dict[str, Any],
    checks: list[dict[str, Any]],
    fallback_model_version: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO ml_live_guard_audit (
            run_id,
            checked_at,
            active_model_version,
            passed,
            action,
            reason,
            daily_count,
            mean_ret20_net,
            win_rate,
            p05_ret20_net,
            cvar05_ret20_net,
            lcb95_ret20_net,
            p_value_mean_gt0,
            robust_lb,
            fallback_model_version,
            checks_json,
            metrics_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            str(run_id),
            checked_at,
            str(active_model_version) if active_model_version else None,
            bool(passed),
            str(action),
            str(reason),
            int(metrics.get("daily_count") or 0),
            _safe_float(metrics.get("top30_mean_ret20_net")),
            _safe_float(metrics.get("top30_win_rate")),
            _safe_float(metrics.get("top30_p05_ret20_net")),
            _safe_float(metrics.get("top30_cvar05_ret20_net")),
            _safe_float(metrics.get("top30_lcb95_ret20_net")),
            _safe_float(metrics.get("top30_p_value_mean_gt0")),
            _safe_float(metrics.get("robust_lb")),
            str(fallback_model_version) if fallback_model_version else None,
            json.dumps(checks, ensure_ascii=False),
            json.dumps(metrics, ensure_ascii=False),
        ],
    )


def _load_latest_live_guard_audit(conn) -> dict[str, Any] | None:
    if not _table_exists(conn, "ml_live_guard_audit"):
        return None
    row = conn.execute(
        """
        SELECT
            run_id,
            checked_at,
            active_model_version,
            passed,
            action,
            reason,
            daily_count,
            mean_ret20_net,
            win_rate,
            p05_ret20_net,
            cvar05_ret20_net,
            lcb95_ret20_net,
            p_value_mean_gt0,
            robust_lb,
            fallback_model_version,
            checks_json
        FROM ml_live_guard_audit
        ORDER BY checked_at DESC
        LIMIT 1
        """
    ).fetchone()
    if not row:
        return None
    checks: list[dict[str, Any]] = []
    try:
        checks = json.loads(row[15]) if row[15] else []
    except Exception:
        checks = []
    return {
        "run_id": row[0],
        "checked_at": row[1],
        "active_model_version": row[2],
        "passed": bool(row[3]),
        "action": row[4],
        "reason": row[5],
        "daily_count": int(row[6] or 0),
        "mean_ret20_net": _safe_float(row[7]),
        "win_rate": _safe_float(row[8]),
        "p05_ret20_net": _safe_float(row[9]),
        "cvar05_ret20_net": _safe_float(row[10]),
        "lcb95_ret20_net": _safe_float(row[11]),
        "p_value_mean_gt0": _safe_float(row[12]),
        "robust_lb": _safe_float(row[13]),
        "fallback_model_version": row[14],
        "checks": checks,
    }


def _apply_live_guard(
    conn,
    *,
    cfg: MLConfig,
) -> dict[str, Any]:
    checked_at = datetime.now(tz=timezone.utc)
    run_id = checked_at.strftime("guard_%Y%m%d%H%M%S_%f")
    active_row = _load_active_model_row(conn)
    if not active_row:
        result = {
            "checked": False,
            "passed": True,
            "action": "noop",
            "reason": "no_active_model",
            "active_model_version": None,
            "rolled_back_to": None,
            "metrics": _summarize_daily_scores([]),
            "checks": [],
        }
        _save_live_guard_audit(
            conn,
            run_id=run_id,
            checked_at=checked_at,
            active_model_version=None,
            passed=True,
            action="noop",
            reason="no_active_model",
            metrics=result["metrics"],
            checks=[],
            fallback_model_version=None,
        )
        return result

    active_model_version = str(active_row[0])
    metrics = _evaluate_live_metrics_for_model(conn, model_version=active_model_version, cfg=cfg)
    robust_lb = _robust_lb_from_metrics(metrics, cfg)
    metrics["robust_lb"] = robust_lb

    checks: list[dict[str, Any]] = [
        {
            "name": "live_daily_count",
            "required": int(cfg.live_guard_min_daily_count),
            "actual": int(metrics.get("daily_count") or 0),
            "ok": int(metrics.get("daily_count") or 0) >= int(cfg.live_guard_min_daily_count),
        },
        {
            "name": "live_mean_ret20_net",
            "required": float(cfg.live_guard_min_mean_ret20_net),
            "actual": _safe_float(metrics.get("top30_mean_ret20_net")),
            "ok": _safe_float(metrics.get("top30_mean_ret20_net")) is not None
            and float(_safe_float(metrics.get("top30_mean_ret20_net")) or 0.0) >= float(cfg.live_guard_min_mean_ret20_net),
        },
        {
            "name": "live_robust_lb",
            "required": float(cfg.live_guard_min_robust_lb),
            "actual": robust_lb,
            "ok": robust_lb is not None and float(robust_lb) >= float(cfg.live_guard_min_robust_lb),
        },
        {
            "name": "live_p_value_mean_gt0",
            "required": float(cfg.live_guard_max_p_value_mean_gt0),
            "actual": _safe_float(metrics.get("top30_p_value_mean_gt0")),
            "ok": _safe_float(metrics.get("top30_p_value_mean_gt0")) is not None
            and float(_safe_float(metrics.get("top30_p_value_mean_gt0")) or 1.0)
            <= float(cfg.live_guard_max_p_value_mean_gt0),
        },
        {
            "name": "live_lcb95_ret20_net",
            "required": float(cfg.live_guard_min_lcb95_ret20_net),
            "actual": _safe_float(metrics.get("top30_lcb95_ret20_net")),
            "ok": _safe_float(metrics.get("top30_lcb95_ret20_net")) is not None
            and float(_safe_float(metrics.get("top30_lcb95_ret20_net")) or 0.0)
            >= float(cfg.live_guard_min_lcb95_ret20_net),
        },
    ]

    daily_count = int(metrics.get("daily_count") or 0)
    if daily_count < int(cfg.live_guard_min_daily_count):
        action = "insufficient_data"
        passed = True
        reason = "insufficient_data_for_live_guard"
        _save_live_guard_audit(
            conn,
            run_id=run_id,
            checked_at=checked_at,
            active_model_version=active_model_version,
            passed=passed,
            action=action,
            reason=reason,
            metrics=metrics,
            checks=checks,
            fallback_model_version=None,
        )
        return {
            "checked": True,
            "passed": passed,
            "action": action,
            "reason": reason,
            "active_model_version": active_model_version,
            "rolled_back_to": None,
            "metrics": metrics,
            "checks": checks,
        }

    all_ok = all(bool(c.get("ok")) for c in checks)
    if all_ok:
        action = "keep"
        passed = True
        reason = "live_guard_passed"
        _save_live_guard_audit(
            conn,
            run_id=run_id,
            checked_at=checked_at,
            active_model_version=active_model_version,
            passed=passed,
            action=action,
            reason=reason,
            metrics=metrics,
            checks=checks,
            fallback_model_version=None,
        )
        return {
            "checked": True,
            "passed": passed,
            "action": action,
            "reason": reason,
            "active_model_version": active_model_version,
            "rolled_back_to": None,
            "metrics": metrics,
            "checks": checks,
        }

    fallback_version: str | None = None
    action = "alert_only"
    reason = "live_guard_failed"
    if bool(cfg.live_guard_allow_rollback):
        fallback = _find_best_fallback_model(
            conn,
            exclude_model_version=active_model_version,
            cfg=cfg,
        )
        fallback_robust = _safe_float((fallback or {}).get("robust_lb"))
        active_robust = _safe_float(metrics.get("robust_lb"))
        if fallback and fallback.get("model_version") and (
            active_robust is None
            or (fallback_robust is not None and float(fallback_robust) > float(active_robust))
        ):
            fallback_version = str(fallback["model_version"])
            conn.execute("UPDATE ml_model_registry SET is_active = FALSE WHERE model_key = ?", [MODEL_KEY])
            conn.execute("UPDATE ml_model_registry SET is_active = TRUE WHERE model_version = ?", [fallback_version])
            action = "rollback"
            reason = "live_guard_failed_rollback_applied"
        else:
            action = "alert_no_suitable_fallback"
            reason = "live_guard_failed_no_suitable_fallback"

    _save_live_guard_audit(
        conn,
        run_id=run_id,
        checked_at=checked_at,
        active_model_version=active_model_version,
        passed=False,
        action=action,
        reason=reason,
        metrics=metrics,
        checks=checks,
        fallback_model_version=fallback_version,
    )
    return {
        "checked": True,
        "passed": False,
        "action": action,
        "reason": reason,
        "active_model_version": active_model_version,
        "rolled_back_to": fallback_version,
        "metrics": metrics,
        "checks": checks,
    }


def enforce_live_guard() -> dict[str, Any]:
    return legacy_predict_runtime.enforce_live_guard()


def get_latest_live_guard_status() -> dict[str, Any]:
    return legacy_predict_runtime.get_latest_live_guard_status()


def _evaluate_promotion_policy(
    *,
    wf_metrics: dict[str, Any],
    cfg: MLConfig,
    has_active_model: bool,
    compare_to_champion: bool = True,
    champion_wf_metrics: dict[str, Any] | None = None,
) -> dict[str, Any]:
    fold_count = int(wf_metrics.get("fold_count") or 0)
    daily_count = int(wf_metrics.get("daily_count") or 0)
    mean_ret = _safe_float(wf_metrics.get("top30_mean_ret20_net"))
    win_rate = _safe_float(wf_metrics.get("top30_win_rate"))
    p05_ret = _safe_float(wf_metrics.get("top30_p05_ret20_net"))
    cvar05_ret = _safe_float(wf_metrics.get("top30_cvar05_ret20_net"))
    lcb95_ret = _safe_float(wf_metrics.get("top30_lcb95_ret20_net"))
    p_value_mean_gt0 = _safe_float(wf_metrics.get("top30_p_value_mean_gt0"))
    up_mean_ret = _safe_float(wf_metrics.get("up_mean_ret20_net"))
    down_mean_ret = _safe_float(wf_metrics.get("down_mean_ret20_net"))
    combined_mean_ret = _safe_float(wf_metrics.get("combined_mean_ret20_net"))

    downside = abs(min(0.0, float(cvar05_ret if cvar05_ret is not None else (p05_ret or 0.0))))
    robust_lb = (
        float(mean_ret) - float(cfg.robust_lb_lambda) * downside
        if mean_ret is not None
        else None
    )
    champion_mean = _safe_float((champion_wf_metrics or {}).get("top30_mean_ret20_net"))
    champion_up_mean = _safe_float((champion_wf_metrics or {}).get("up_mean_ret20_net"))
    champion_down_mean = _safe_float((champion_wf_metrics or {}).get("down_mean_ret20_net"))
    champion_combined_mean = _safe_float((champion_wf_metrics or {}).get("combined_mean_ret20_net"))
    if champion_combined_mean is None:
        champion_combined_mean = champion_mean
    champion_p05 = _safe_float((champion_wf_metrics or {}).get("top30_p05_ret20_net"))
    champion_cvar05 = _safe_float((champion_wf_metrics or {}).get("top30_cvar05_ret20_net"))
    champion_lcb95 = _safe_float((champion_wf_metrics or {}).get("top30_lcb95_ret20_net"))
    if champion_lcb95 is None:
        champion_lcb95 = champion_mean
    champion_downside = abs(
        min(0.0, float(champion_cvar05 if champion_cvar05 is not None else (champion_p05 or 0.0)))
    )
    champion_robust_lb = (
        float(champion_mean) - float(cfg.robust_lb_lambda) * champion_downside
        if champion_mean is not None
        else None
    )
    delta_mean = (
        float(mean_ret) - float(champion_mean)
        if mean_ret is not None and champion_mean is not None
        else None
    )
    delta_up_mean = (
        float(up_mean_ret) - float(champion_up_mean)
        if up_mean_ret is not None and champion_up_mean is not None
        else None
    )
    delta_down_mean = (
        float(down_mean_ret) - float(champion_down_mean)
        if down_mean_ret is not None and champion_down_mean is not None
        else None
    )
    delta_combined_mean = (
        float(combined_mean_ret) - float(champion_combined_mean)
        if combined_mean_ret is not None and champion_combined_mean is not None
        else None
    )
    delta_robust_lb = (
        float(robust_lb) - float(champion_robust_lb)
        if robust_lb is not None and champion_robust_lb is not None
        else None
    )
    delta_lcb95 = (
        float(lcb95_ret) - float(champion_lcb95)
        if lcb95_ret is not None and champion_lcb95 is not None
        else None
    )
    champion_metrics_available = (
        champion_mean is not None
        and champion_robust_lb is not None
        and champion_lcb95 is not None
    )

    checks = [
        {
            "name": "wf_fold_count",
            "required": int(cfg.min_wf_fold_count),
            "actual": int(fold_count),
            "ok": int(fold_count) >= int(cfg.min_wf_fold_count),
        },
        {
            "name": "wf_daily_count",
            "required": int(cfg.min_wf_daily_count),
            "actual": int(daily_count),
            "ok": int(daily_count) >= int(cfg.min_wf_daily_count),
        },
        {
            "name": "wf_mean_ret20_net",
            "required": float(cfg.min_wf_mean_ret20_net),
            "actual": mean_ret,
            "ok": mean_ret is not None and float(mean_ret) >= float(cfg.min_wf_mean_ret20_net),
        },
        {
            "name": "wf_up_mean_ret20_net",
            "required": float(getattr(cfg, "min_wf_up_mean_ret20_net", 0.0)),
            "actual": up_mean_ret,
            "ok": up_mean_ret is not None
            and float(up_mean_ret) >= float(getattr(cfg, "min_wf_up_mean_ret20_net", 0.0)),
        },
        {
            "name": "wf_down_mean_ret20_net",
            "required": float(getattr(cfg, "min_wf_down_mean_ret20_net", 0.0)),
            "actual": down_mean_ret,
            "ok": down_mean_ret is not None
            and float(down_mean_ret) >= float(getattr(cfg, "min_wf_down_mean_ret20_net", 0.0)),
        },
        {
            "name": "wf_combined_mean_ret20_net",
            "required": float(getattr(cfg, "min_wf_combined_mean_ret20_net", 0.0)),
            "actual": combined_mean_ret,
            "ok": combined_mean_ret is not None
            and float(combined_mean_ret) >= float(getattr(cfg, "min_wf_combined_mean_ret20_net", 0.0)),
        },
        {
            "name": "wf_win_rate",
            "required": float(cfg.min_wf_win_rate),
            "actual": win_rate,
            "ok": win_rate is not None and float(win_rate) >= float(cfg.min_wf_win_rate),
        },
        {
            "name": "wf_p05_ret20_net",
            "required": float(cfg.min_wf_p05_ret20_net),
            "actual": p05_ret,
            "ok": p05_ret is not None and float(p05_ret) >= float(cfg.min_wf_p05_ret20_net),
        },
        {
            "name": "wf_cvar05_ret20_net",
            "required": float(cfg.min_wf_cvar05_ret20_net),
            "actual": cvar05_ret,
            "ok": cvar05_ret is not None and float(cvar05_ret) >= float(cfg.min_wf_cvar05_ret20_net),
        },
        {
            "name": "wf_robust_lb",
            "required": float(cfg.min_wf_robust_lb),
            "actual": robust_lb,
            "ok": robust_lb is not None and float(robust_lb) >= float(cfg.min_wf_robust_lb),
        },
        {
            "name": "wf_p_value_mean_gt0",
            "required": float(cfg.max_wf_p_value_mean_gt0),
            "actual": p_value_mean_gt0,
            "ok": p_value_mean_gt0 is not None and float(p_value_mean_gt0) <= float(cfg.max_wf_p_value_mean_gt0),
        },
        {
            "name": "wf_lcb95_ret20_net",
            "required": float(cfg.min_wf_lcb95_ret20_net),
            "actual": lcb95_ret,
            "ok": lcb95_ret is not None and float(lcb95_ret) >= float(cfg.min_wf_lcb95_ret20_net),
        },
    ]
    if (
        bool(compare_to_champion)
        and bool(has_active_model)
        and bool(cfg.require_champion_improvement)
        and champion_metrics_available
    ):
        checks.extend(
            [
                {
                    "name": "champion_delta_mean_ret20_net",
                    "required": float(cfg.min_delta_mean_ret20_net),
                    "actual": delta_mean,
                    "ok": delta_mean is not None and float(delta_mean) >= float(cfg.min_delta_mean_ret20_net),
                },
                {
                    "name": "champion_delta_robust_lb",
                    "required": float(cfg.min_delta_robust_lb),
                    "actual": delta_robust_lb,
                    "ok": delta_robust_lb is not None and float(delta_robust_lb) >= float(cfg.min_delta_robust_lb),
                },
                {
                    "name": "champion_delta_lcb95_ret20_net",
                    "required": float(cfg.min_delta_lcb95_ret20_net),
                    "actual": delta_lcb95,
                    "ok": delta_lcb95 is not None and float(delta_lcb95) >= float(cfg.min_delta_lcb95_ret20_net),
                },
            ]
        )
    elif bool(compare_to_champion) and bool(has_active_model) and bool(cfg.require_champion_improvement):
        checks.append(
            {
                "name": "champion_metrics_missing_skip_delta",
                "required": True,
                "actual": False,
                "ok": True,
            }
        )
    elif (not bool(compare_to_champion)) and bool(has_active_model) and bool(cfg.require_champion_improvement):
        checks.append(
            {
                "name": "champion_objective_mismatch_skip_delta",
                "required": True,
                "actual": False,
                "ok": True,
            }
        )
    all_ok = all(bool(c.get("ok")) for c in checks)
    context = {
        "candidate": {
            "mean_ret20_net": mean_ret,
            "up_mean_ret20_net": up_mean_ret,
            "down_mean_ret20_net": down_mean_ret,
            "combined_mean_ret20_net": combined_mean_ret,
            "robust_lb": robust_lb,
            "lcb95_ret20_net": lcb95_ret,
            "p_value_mean_gt0": p_value_mean_gt0,
        },
        "champion": {
            "mean_ret20_net": champion_mean,
            "up_mean_ret20_net": champion_up_mean,
            "down_mean_ret20_net": champion_down_mean,
            "combined_mean_ret20_net": champion_combined_mean,
            "robust_lb": champion_robust_lb,
            "lcb95_ret20_net": champion_lcb95,
            "metrics_available": champion_metrics_available,
        },
        "delta_vs_champion": {
            "mean_ret20_net": delta_mean,
            "up_mean_ret20_net": delta_up_mean,
            "down_mean_ret20_net": delta_down_mean,
            "combined_mean_ret20_net": delta_combined_mean,
            "robust_lb": delta_robust_lb,
            "lcb95_ret20_net": delta_lcb95,
        },
    }

    if not bool(cfg.auto_promote):
        return {
            "promoted": False,
            "reason": "auto_promote_disabled",
            "has_active_model": bool(has_active_model),
            "robust_lb": robust_lb,
            "checks": checks,
            **context,
        }

    if all_ok:
        return {
            "promoted": True,
            "reason": "gate_passed",
            "has_active_model": bool(has_active_model),
            "robust_lb": robust_lb,
            "checks": checks,
            **context,
        }

    if (not has_active_model) and bool(cfg.allow_bootstrap_promotion):
        # Keep system operational on first boot while retaining failed checks for auditability.
        return {
            "promoted": True,
            "reason": "bootstrap_override_no_active_model",
            "has_active_model": False,
            "robust_lb": robust_lb,
            "checks": checks,
            **context,
        }

    return {
        "promoted": False,
        "reason": "gate_failed",
        "has_active_model": bool(has_active_model),
        "robust_lb": robust_lb,
        "checks": checks,
        **context,
    }


def _save_training_audit(
    conn,
    *,
    run_id: str,
    trained_at: datetime,
    model_version: str,
    promoted: bool,
    reason: str,
    wf_metrics: dict[str, Any],
    gate: dict[str, Any],
) -> None:
    conn.execute(
        """
        INSERT INTO ml_training_audit (
            run_id,
            trained_at,
            model_version,
            promoted,
            reason,
            wf_fold_count,
            wf_daily_count,
            wf_mean_ret20_net,
            wf_win_rate,
            wf_p05_ret20_net,
            wf_cvar05_ret20_net,
            wf_lcb95_ret20_net,
            wf_p_value_mean_gt0,
            wf_robust_lb,
            gate_json,
            metrics_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            str(run_id),
            trained_at,
            str(model_version),
            bool(promoted),
            str(reason),
            int(wf_metrics.get("fold_count") or 0),
            int(wf_metrics.get("daily_count") or 0),
            _safe_float(wf_metrics.get("top30_mean_ret20_net")),
            _safe_float(wf_metrics.get("top30_win_rate")),
            _safe_float(wf_metrics.get("top30_p05_ret20_net")),
            _safe_float(wf_metrics.get("top30_cvar05_ret20_net")),
            _safe_float(wf_metrics.get("top30_lcb95_ret20_net")),
            _safe_float(wf_metrics.get("top30_p_value_mean_gt0")),
            _safe_float(gate.get("robust_lb")),
            json.dumps(gate, ensure_ascii=False),
            json.dumps(wf_metrics, ensure_ascii=False),
        ],
    )


def _load_latest_training_audit(conn) -> dict[str, Any] | None:
    if not _table_exists(conn, "ml_training_audit"):
        return None
    row = conn.execute(
        """
        SELECT
            run_id,
            trained_at,
            model_version,
            promoted,
            reason,
            wf_fold_count,
            wf_daily_count,
            wf_mean_ret20_net,
            wf_win_rate,
            wf_p05_ret20_net,
            wf_cvar05_ret20_net,
            wf_lcb95_ret20_net,
            wf_p_value_mean_gt0,
            wf_robust_lb,
            gate_json
        FROM ml_training_audit
        ORDER BY trained_at DESC
        LIMIT 1
        """
    ).fetchone()
    if not row:
        return None
    gate: dict[str, Any] = {}
    try:
        gate = json.loads(row[14]) if row[14] else {}
    except Exception:
        gate = {}
    return {
        "run_id": row[0],
        "trained_at": row[1],
        "model_version": row[2],
        "promoted": bool(row[3]),
        "reason": row[4],
        "wf_fold_count": int(row[5] or 0),
        "wf_daily_count": int(row[6] or 0),
        "wf_mean_ret20_net": _safe_float(row[7]),
        "wf_win_rate": _safe_float(row[8]),
        "wf_p05_ret20_net": _safe_float(row[9]),
        "wf_cvar05_ret20_net": _safe_float(row[10]),
        "wf_lcb95_ret20_net": _safe_float(row[11]),
        "wf_p_value_mean_gt0": _safe_float(row[12]),
        "wf_robust_lb": _safe_float(row[13]),
        "gate": gate,
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
    activate: bool,
) -> None:
    if activate:
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
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
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
            bool(activate),
        ],
    )


def _load_active_monthly_model_row(conn) -> tuple | None:
    if not _table_exists(conn, "ml_monthly_model_registry"):
        return None
    return conn.execute(
        """
        SELECT
            model_version,
            model_key,
            label_version,
            metrics_json,
            artifact_path,
            n_train_abs,
            n_train_dir,
            created_at
        FROM ml_monthly_model_registry
        WHERE is_active = TRUE
        ORDER BY created_at DESC
        LIMIT 1
        """
    ).fetchone()


def _save_monthly_registry_row(
    conn,
    *,
    model_version: str,
    metrics: dict[str, Any],
    artifact_path: str,
    n_train_abs: int,
    n_train_dir: int,
    activate: bool,
) -> None:
    if activate:
        conn.execute(
            "UPDATE ml_monthly_model_registry SET is_active = FALSE WHERE model_key = ?",
            [MONTHLY_MODEL_KEY],
        )
    conn.execute(
        """
        INSERT INTO ml_monthly_model_registry (
            model_version,
            model_key,
            label_version,
            metrics_json,
            artifact_path,
            n_train_abs,
            n_train_dir,
            created_at,
            is_active
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
        """,
        [
            model_version,
            MONTHLY_MODEL_KEY,
            MONTHLY_LABEL_VERSION,
            json.dumps(metrics, ensure_ascii=False),
            artifact_path,
            int(n_train_abs),
            int(n_train_dir),
            bool(activate),
        ],
    )


def _train_monthly_models_with_conn(
    conn,
    *,
    start_dt: int | None = None,
    end_dt: int | None = None,
) -> dict[str, Any]:
    cfg = load_ml_config()
    _ensure_ml_schema(conn)
    monthly_label_rows = refresh_ml_monthly_label_table(
        conn,
        label_version=MONTHLY_LABEL_VERSION,
        start_dt=start_dt,
        end_dt=end_dt,
    )
    monthly_df = _load_monthly_training_df(conn, start_dt=start_dt, end_dt=end_dt)
    if monthly_df.empty:
        raise RuntimeError("No joined rows for monthly ML training")
    monthly_models = _fit_monthly_models(monthly_df, cfg)
    if monthly_models.abs_cls is None:
        raise RuntimeError("monthly abs model is not available")
    monthly_pred_train = _predict_monthly_frame(monthly_df, monthly_models)
    monthly_pred_train["ret1m"] = pd.to_numeric(monthly_df.get("ret1m"), errors="coerce")
    monthly_ret20_lookup = _derive_monthly_ret20_lookup(
        monthly_df,
        monthly_models,
        pred_df=monthly_pred_train,
    )
    monthly_gate_recommendation = _derive_monthly_gate_recommendation(
        monthly_df,
        monthly_models,
        pred_df=monthly_pred_train,
        ret20_lookup=monthly_ret20_lookup,
    )
    trained_at = datetime.now(tz=timezone.utc)
    monthly_model_version = f"{trained_at.strftime('%Y%m%d%H%M%S')}_m1"
    art_dir = _artifact_dir()
    monthly_abs_path = art_dir / f"{monthly_model_version}_monthly_abs.txt"
    monthly_dir_path = art_dir / f"{monthly_model_version}_monthly_dir.txt"
    monthly_models.abs_cls.save_model(str(monthly_abs_path))
    if monthly_models.dir_cls is not None:
        monthly_models.dir_cls.save_model(str(monthly_dir_path))
    monthly_artifact = {
        "abs_model_path": str(monthly_abs_path),
        "dir_model_path": str(monthly_dir_path) if monthly_models.dir_cls is not None else None,
    }
    monthly_metrics_json = {
        "monthly_label_rows": int(monthly_label_rows),
        "monthly_train_rows": int(len(monthly_df)),
        "n_train_abs": int(monthly_models.n_train_abs),
        "n_train_dir": int(monthly_models.n_train_dir),
        "model_version": monthly_model_version,
        "feature_columns": monthly_models.feature_columns,
        "medians": monthly_models.medians,
        "abs_temperature": float(monthly_models.abs_temperature),
        "dir_temperature": float(monthly_models.dir_temperature),
        "gate_recommendation": monthly_gate_recommendation,
        "ret20_lookup": monthly_ret20_lookup,
        "config": {
            "label_quantile": MONTHLY_LABEL_QUANTILE,
            "liquidity_bottom_ratio": MONTHLY_LIQUIDITY_BOTTOM_RATIO,
        },
    }
    _save_monthly_registry_row(
        conn,
        model_version=monthly_model_version,
        metrics=monthly_metrics_json,
        artifact_path=json.dumps(monthly_artifact, ensure_ascii=False),
        n_train_abs=monthly_models.n_train_abs,
        n_train_dir=monthly_models.n_train_dir,
        activate=True,
    )
    return {
        "model_version": monthly_model_version,
        "label_rows": int(monthly_label_rows),
        "train_rows": int(len(monthly_df)),
        "n_train_abs": int(monthly_models.n_train_abs),
        "n_train_dir": int(monthly_models.n_train_dir),
        "gate_recommendation": monthly_gate_recommendation,
        "ret20_lookup": monthly_ret20_lookup,
        "artifact": monthly_artifact,
    }


def _train_models_impl(
    *,
    start_dt: int | None = None,
    end_dt: int | None = None,
    dry_run: bool = False,
    progress_cb: Callable[[int, str], None] | None = None,
) -> dict[str, Any]:
    cfg = load_ml_config()

    def _notify(progress: int, message: str) -> None:
        if progress_cb is None:
            return
        try:
            progress_cb(max(0, min(100, int(progress))), str(message))
        except Exception:
            return

    _notify(2, "Preparing ML training...")
    with get_conn() as conn:
        _ensure_ml_schema(conn)
        active_row = _load_active_model_row(conn)
        has_active_model = bool(active_row)
        active_objective = str(active_row[2]) if active_row and len(active_row) > 2 and active_row[2] is not None else None
        compare_to_champion = bool(has_active_model and active_objective == OBJECTIVE)
        champion_wf_metrics = _extract_walk_forward_metrics_from_registry_row(active_row)
        _notify(6, "Refreshing feature table...")
        feature_rows = refresh_ml_feature_table(
            conn,
            feature_version=FEATURE_VERSION,
            start_dt=start_dt,
            end_dt=end_dt,
        )
        _notify(14, f"Feature table refreshed ({int(feature_rows)} rows).")
        _notify(18, "Refreshing label table...")
        label_rows = refresh_ml_label_table(
            conn,
            cfg=cfg,
            label_version=LABEL_VERSION,
            start_dt=start_dt,
            end_dt=end_dt,
        )
        _notify(26, f"Label table refreshed ({int(label_rows)} rows).")
        _notify(30, "Refreshing monthly labels...")
        monthly_label_rows = refresh_ml_monthly_label_table(
            conn,
            label_version=MONTHLY_LABEL_VERSION,
            start_dt=start_dt,
            end_dt=end_dt,
        )
        _notify(36, "Loading training datasets...")
        df = _load_training_df(conn, start_dt=start_dt, end_dt=end_dt)
        if df.empty:
            raise RuntimeError("No joined rows for ML training")
        monthly_df = _load_monthly_training_df(conn, start_dt=start_dt, end_dt=end_dt)
        monthly_models: MonthlyTrainedModels | None = None
        monthly_gate_recommendation: dict[str, Any] = {}
        monthly_ret20_lookup: dict[str, Any] = {}
        monthly_train_error: str | None = None

        _notify(40, f"Running walk-forward ({int(len(df))} rows)...")
        wf_start = 40
        wf_span = 30

        def _on_wf_progress(done: int, total: int) -> None:
            pct = wf_start + int(wf_span * max(0, done) / max(1, total))
            _notify(pct, f"Walk-forward {int(done)}/{int(total)}")

        wf_metrics = _walk_forward_eval(df, cfg, progress_cb=_on_wf_progress)
        _notify(72, "Fitting production models...")
        models = _fit_models(df, cfg)
        _notify(80, "Fitting monthly models...")
        if monthly_df.empty:
            monthly_train_error = "No joined rows for monthly ML training"
        else:
            try:
                monthly_models = _fit_monthly_models(monthly_df, cfg)
                if monthly_models.abs_cls is not None:
                    monthly_pred_train = _predict_monthly_frame(monthly_df, monthly_models)
                    monthly_pred_train["ret1m"] = pd.to_numeric(monthly_df.get("ret1m"), errors="coerce")
                    monthly_ret20_lookup = _derive_monthly_ret20_lookup(
                        monthly_df,
                        monthly_models,
                        pred_df=monthly_pred_train,
                    )
                    monthly_gate_recommendation = _derive_monthly_gate_recommendation(
                        monthly_df,
                        monthly_models,
                        pred_df=monthly_pred_train,
                        ret20_lookup=monthly_ret20_lookup,
                    )
            except Exception as exc:
                monthly_train_error = str(exc)

        monthly_enabled = bool(monthly_models is not None and monthly_models.abs_cls is not None)

        _notify(88, "Evaluating promotion policy...")
        trained_at = datetime.now(tz=timezone.utc)
        model_version = trained_at.strftime("%Y%m%d%H%M%S")
        monthly_model_version = (
            f"{trained_at.strftime('%Y%m%d%H%M%S')}_m1" if monthly_enabled else None
        )
        gate = _evaluate_promotion_policy(
            wf_metrics=wf_metrics,
            cfg=cfg,
            has_active_model=has_active_model,
            compare_to_champion=compare_to_champion,
            champion_wf_metrics=champion_wf_metrics,
        )
        payload = {
            "feature_rows": int(feature_rows),
            "label_rows": int(label_rows),
            "monthly_label_rows": int(monthly_label_rows),
            "train_rows": int(len(df)),
            "monthly_train_rows": int(len(monthly_df)),
            "n_train_cls": int(models.n_train_cls),
            "n_train_reg": int(models.n_train_reg),
            "n_train_turn_up": int(models.n_train_turn_up),
            "n_train_turn_down": int(models.n_train_turn_down),
            "n_train_rank": int(models.n_train_rank),
            "n_train_rank_groups": int(models.n_train_rank_groups),
            "n_train_monthly_abs": int(monthly_models.n_train_abs if monthly_models is not None else 0),
            "n_train_monthly_dir": int(monthly_models.n_train_dir if monthly_models is not None else 0),
            "n_train_cls_by_horizon": {str(k): int(v) for k, v in (models.n_train_cls_by_horizon or {}).items()},
            "n_train_reg_by_horizon": {str(k): int(v) for k, v in (models.n_train_reg_by_horizon or {}).items()},
            "n_train_turn_down_by_horizon": {
                str(k): int(v) for k, v in (models.n_train_turn_down_by_horizon or {}).items()
            },
            "model_version": model_version,
            "monthly_model_version": monthly_model_version,
            "monthly_enabled": monthly_enabled,
            "monthly_train_error": monthly_train_error,
            "monthly_gate_recommendation": monthly_gate_recommendation,
            "monthly_ret20_lookup": monthly_ret20_lookup,
            "walk_forward": wf_metrics,
            "promotion": gate,
            "promoted": bool(gate.get("promoted")),
        }
        metrics_json = {
            **payload,
            "feature_columns": FEATURE_COLUMNS,
            "medians": models.medians,
            "cls_temperature_by_horizon": {
                str(k): float(v) for k, v in (models.cls_temperature_by_horizon or {}).items()
            },
            "config": {
                "neutral_band_pct": cfg.neutral_band_pct,
                "p_up_threshold": cfg.p_up_threshold,
                "top_n": cfg.top_n,
                "cost_bps": cfg.cost_bps,
                "liq_cost_turnover_low": LIQ_COST_TURNOVER_LOW,
                "liq_cost_turnover_mid": LIQ_COST_TURNOVER_MID,
                "liq_slippage_bps_low": LIQ_SLIPPAGE_BPS_LOW,
                "liq_slippage_bps_mid": LIQ_SLIPPAGE_BPS_MID,
                "liq_slippage_bps_high": LIQ_SLIPPAGE_BPS_HIGH,
                "liq_slippage_bps_unknown": LIQ_SLIPPAGE_BPS_UNKNOWN,
                "short_borrow_bps_20d": SHORT_BORROW_BPS_20D,
                "train_days": cfg.train_days,
                "test_days": cfg.test_days,
                "step_days": cfg.step_days,
                "embargo_days": cfg.embargo_days,
                "rank_boost_round": cfg.rank_boost_round,
                "rule_weight": cfg.rule_weight,
                "ev_weight": cfg.ev_weight,
                "prob_weight": cfg.prob_weight,
                "rank_weight": cfg.rank_weight,
                "turn_weight": cfg.turn_weight,
                "min_prob_up": cfg.min_prob_up,
                "min_prob_down": cfg.min_prob_down,
                "min_turn_prob_up": cfg.min_turn_prob_up,
                "min_turn_prob_down": cfg.min_turn_prob_down,
                "min_turn_margin": cfg.min_turn_margin,
                "auto_promote": cfg.auto_promote,
                "allow_bootstrap_promotion": cfg.allow_bootstrap_promotion,
                "min_wf_fold_count": cfg.min_wf_fold_count,
                "min_wf_daily_count": cfg.min_wf_daily_count,
                "min_wf_mean_ret20_net": cfg.min_wf_mean_ret20_net,
                "min_wf_win_rate": cfg.min_wf_win_rate,
                "min_wf_p05_ret20_net": cfg.min_wf_p05_ret20_net,
                "min_wf_cvar05_ret20_net": cfg.min_wf_cvar05_ret20_net,
                "robust_lb_lambda": cfg.robust_lb_lambda,
                "min_wf_robust_lb": cfg.min_wf_robust_lb,
                "max_wf_p_value_mean_gt0": cfg.max_wf_p_value_mean_gt0,
                "min_wf_lcb95_ret20_net": cfg.min_wf_lcb95_ret20_net,
                "min_wf_up_mean_ret20_net": cfg.min_wf_up_mean_ret20_net,
                "min_wf_down_mean_ret20_net": cfg.min_wf_down_mean_ret20_net,
                "min_wf_combined_mean_ret20_net": cfg.min_wf_combined_mean_ret20_net,
                "require_champion_improvement": cfg.require_champion_improvement,
                "min_delta_mean_ret20_net": cfg.min_delta_mean_ret20_net,
                "min_delta_robust_lb": cfg.min_delta_robust_lb,
                "min_delta_lcb95_ret20_net": cfg.min_delta_lcb95_ret20_net,
                "live_guard_enabled": cfg.live_guard_enabled,
                "live_guard_lookback_days": cfg.live_guard_lookback_days,
                "live_guard_min_daily_count": cfg.live_guard_min_daily_count,
                "live_guard_min_mean_ret20_net": cfg.live_guard_min_mean_ret20_net,
                "live_guard_min_robust_lb": cfg.live_guard_min_robust_lb,
                "live_guard_max_p_value_mean_gt0": cfg.live_guard_max_p_value_mean_gt0,
                "live_guard_min_lcb95_ret20_net": cfg.live_guard_min_lcb95_ret20_net,
                "live_guard_allow_rollback": cfg.live_guard_allow_rollback,
                "wf_use_expanding_train": cfg.wf_use_expanding_train,
                "wf_max_train_days": cfg.wf_max_train_days,
            },
        }
        monthly_metrics_json = (
            {
                "monthly_label_rows": int(monthly_label_rows),
                "monthly_train_rows": int(len(monthly_df)),
                "n_train_abs": int(monthly_models.n_train_abs),
                "n_train_dir": int(monthly_models.n_train_dir),
                "model_version": monthly_model_version,
                "feature_columns": monthly_models.feature_columns,
                "medians": monthly_models.medians,
                "abs_temperature": float(monthly_models.abs_temperature),
                "dir_temperature": float(monthly_models.dir_temperature),
                "gate_recommendation": monthly_gate_recommendation,
                "ret20_lookup": monthly_ret20_lookup,
                "config": {
                    "label_quantile": MONTHLY_LABEL_QUANTILE,
                    "liquidity_bottom_ratio": MONTHLY_LIQUIDITY_BOTTOM_RATIO,
                },
            }
            if monthly_models is not None and monthly_model_version is not None
            else {
                "monthly_label_rows": int(monthly_label_rows),
                "monthly_train_rows": int(len(monthly_df)),
                "n_train_abs": 0,
                "n_train_dir": 0,
                "model_version": None,
                "disabled_reason": monthly_train_error,
                "gate_recommendation": {},
                "ret20_lookup": {},
                "config": {
                    "label_quantile": MONTHLY_LABEL_QUANTILE,
                    "liquidity_bottom_ratio": MONTHLY_LIQUIDITY_BOTTOM_RATIO,
                },
            }
        )
        if dry_run:
            _notify(100, "ML training completed (dry-run).")
            return {
                **payload,
                "monthly": {
                    "model_version": monthly_model_version,
                    "label_rows": int(monthly_label_rows),
                    "train_rows": int(len(monthly_df)),
                    "n_train_abs": int(monthly_models.n_train_abs if monthly_models is not None else 0),
                    "n_train_dir": int(monthly_models.n_train_dir if monthly_models is not None else 0),
                    "enabled": monthly_enabled,
                    "disabled_reason": monthly_train_error,
                    "gate_recommendation": monthly_gate_recommendation,
                    "ret20_lookup": monthly_ret20_lookup,
                    "dry_run": True,
                },
                "dry_run": True,
            }

        _notify(92, "Saving model artifacts...")
        art_dir = _artifact_dir()
        cls_path = art_dir / f"{model_version}_cls.txt"
        reg_path = art_dir / f"{model_version}_reg.txt"
        turn_up_path = art_dir / f"{model_version}_turn_up.txt"
        turn_down_path = art_dir / f"{model_version}_turn_down.txt"
        rank_up_path = art_dir / f"{model_version}_rank_up_20.txt"
        rank_down_path = art_dir / f"{model_version}_rank_down_20.txt"
        models.cls.save_model(str(cls_path))
        models.reg.save_model(str(reg_path))
        if models.turn_up is not None:
            models.turn_up.save_model(str(turn_up_path))
        if models.turn_down is not None:
            models.turn_down.save_model(str(turn_down_path))
        if models.rank_up is not None:
            models.rank_up.save_model(str(rank_up_path))
        if models.rank_down is not None:
            models.rank_down.save_model(str(rank_down_path))
        monthly_abs_path: Path | None = None
        monthly_dir_path: Path | None = None
        if monthly_models is not None and monthly_model_version is not None and monthly_models.abs_cls is not None:
            monthly_abs_path = art_dir / f"{monthly_model_version}_monthly_abs.txt"
            monthly_models.abs_cls.save_model(str(monthly_abs_path))
            if monthly_models.dir_cls is not None:
                monthly_dir_path = art_dir / f"{monthly_model_version}_monthly_dir.txt"
                monthly_models.dir_cls.save_model(str(monthly_dir_path))
        horizon_artifacts: dict[str, dict[str, str | None]] = {}
        for horizon in PREDICTION_HORIZONS:
            cls_h = models.cls_by_horizon.get(horizon)
            reg_h = models.reg_by_horizon.get(horizon)
            turn_down_h = models.turn_down_by_horizon.get(horizon)
            cls_h_path: str | None = None
            reg_h_path: str | None = None
            turn_down_h_path: str | None = None
            if cls_h is not None:
                target = art_dir / f"{model_version}_cls_{horizon}.txt"
                cls_h.save_model(str(target))
                cls_h_path = str(target)
            if reg_h is not None:
                target = art_dir / f"{model_version}_reg_{horizon}.txt"
                reg_h.save_model(str(target))
                reg_h_path = str(target)
            if turn_down_h is not None:
                target = art_dir / f"{model_version}_turn_down_{horizon}.txt"
                turn_down_h.save_model(str(target))
                turn_down_h_path = str(target)
            horizon_artifacts[str(horizon)] = {
                "cls_model_path": cls_h_path,
                "reg_model_path": reg_h_path,
                "turn_down_model_path": turn_down_h_path,
            }
        artifact = {
            "cls_model_path": str(cls_path),
            "reg_model_path": str(reg_path),
            "turn_up_model_path": str(turn_up_path) if models.turn_up is not None else None,
            "turn_down_model_path": str(turn_down_path) if models.turn_down is not None else None,
            "rank_up_model_path": str(rank_up_path) if models.rank_up is not None else None,
            "rank_down_model_path": str(rank_down_path) if models.rank_down is not None else None,
            "horizon_models": horizon_artifacts,
        }
        monthly_artifact = (
            {
                "abs_model_path": str(monthly_abs_path),
                "dir_model_path": str(monthly_dir_path) if monthly_dir_path is not None else None,
            }
            if monthly_abs_path is not None
            else None
        )
        _notify(96, "Writing model registry...")
        train_start = int(df["dt"].min()) if not df.empty else None
        train_end = int(df["dt"].max()) if not df.empty else None
        promote = bool(gate.get("promoted"))
        _save_registry_row(
            conn,
            model_version=model_version,
            metrics=metrics_json,
            artifact_path=json.dumps(artifact, ensure_ascii=False),
            n_train=models.n_train_reg,
            train_start_dt=train_start,
            train_end_dt=train_end,
            activate=promote,
        )
        if monthly_models is not None and monthly_model_version is not None and monthly_artifact is not None:
            _save_monthly_registry_row(
                conn,
                model_version=monthly_model_version,
                metrics=monthly_metrics_json,
                artifact_path=json.dumps(monthly_artifact, ensure_ascii=False),
                n_train_abs=monthly_models.n_train_abs,
                n_train_dir=monthly_models.n_train_dir,
                activate=True,
            )
        _save_training_audit(
            conn,
            run_id=f"train_{model_version}",
            trained_at=trained_at,
            model_version=model_version,
            promoted=promote,
            reason=str(gate.get("reason") or "unknown"),
            wf_metrics=wf_metrics,
            gate=gate,
        )
        _notify(100, "ML training completed.")
        return {
            **payload,
            "dry_run": False,
            "artifact": artifact,
            "monthly": {
                "model_version": monthly_model_version,
                "label_rows": int(monthly_label_rows),
                "train_rows": int(len(monthly_df)),
                "n_train_abs": int(monthly_models.n_train_abs if monthly_models is not None else 0),
                "n_train_dir": int(monthly_models.n_train_dir if monthly_models is not None else 0),
                "enabled": monthly_enabled,
                "disabled_reason": monthly_train_error,
                "gate_recommendation": monthly_gate_recommendation,
                "ret20_lookup": monthly_ret20_lookup,
                "artifact": monthly_artifact,
            },
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

    def _resolve_artifact_path(path_value: Any) -> str | None:
        if not path_value:
            return None
        path = Path(str(path_value))
        if path.exists():
            return str(path)
        # Portable fallback: resolve by basename in current DATA_DIR/models/ml.
        fallback = _artifact_dir() / path.name
        if fallback.exists():
            return str(fallback)
        return None

    cls_model_path = _resolve_artifact_path(artifact.get("cls_model_path"))
    reg_model_path = _resolve_artifact_path(artifact.get("reg_model_path"))
    if not cls_model_path or not reg_model_path:
        raise RuntimeError("Model artifact path is invalid")
    turn_up_model_path = _resolve_artifact_path(artifact.get("turn_up_model_path"))
    turn_down_model_path = _resolve_artifact_path(artifact.get("turn_down_model_path"))
    rank_up_model_path = _resolve_artifact_path(artifact.get("rank_up_model_path"))
    rank_down_model_path = _resolve_artifact_path(artifact.get("rank_down_model_path"))
    horizon_models_raw = artifact.get("horizon_models") if isinstance(artifact, dict) else None
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
    rank_up_model = (
        lgb.Booster(model_file=str(rank_up_model_path))
        if rank_up_model_path and Path(str(rank_up_model_path)).exists()
        else None
    )
    rank_down_model = (
        lgb.Booster(model_file=str(rank_down_model_path))
        if rank_down_model_path and Path(str(rank_down_model_path)).exists()
        else None
    )
    cls_by_horizon: dict[int, Any] = {20: cls_model}
    reg_by_horizon: dict[int, Any] = {20: reg_model}
    turn_down_by_horizon: dict[int, Any | None] = {10: turn_down_model}

    def _load_optional_model(path_value: Any) -> Any | None:
        resolved = _resolve_artifact_path(path_value)
        if not resolved:
            return None
        path = Path(resolved)
        if not path.exists():
            return None
        return lgb.Booster(model_file=str(path))

    if isinstance(horizon_models_raw, dict):
        for key, payload in horizon_models_raw.items():
            try:
                horizon = int(key)
            except (TypeError, ValueError):
                continue
            if not isinstance(payload, dict):
                continue
            cls_h = _load_optional_model(payload.get("cls_model_path"))
            reg_h = _load_optional_model(payload.get("reg_model_path"))
            turn_down_h = _load_optional_model(payload.get("turn_down_model_path"))
            if cls_h is not None:
                cls_by_horizon[horizon] = cls_h
            if reg_h is not None:
                reg_by_horizon[horizon] = reg_h
            if turn_down_h is not None or horizon not in turn_down_by_horizon:
                turn_down_by_horizon[horizon] = turn_down_h

    if 20 not in cls_by_horizon:
        cls_by_horizon[20] = cls_model
    if 20 not in reg_by_horizon:
        reg_by_horizon[20] = reg_model
    if 10 not in turn_down_by_horizon:
        turn_down_by_horizon[10] = turn_down_model

    cls_temperature_by_horizon: dict[int, float] = {}
    raw_temperatures = metrics_json.get("cls_temperature_by_horizon")
    if isinstance(raw_temperatures, dict):
        for key, raw in raw_temperatures.items():
            try:
                horizon = int(key)
                temp = float(raw)
            except (TypeError, ValueError):
                continue
            if np.isfinite(temp) and temp > 0.0:
                cls_temperature_by_horizon[horizon] = temp
    for horizon in PREDICTION_HORIZONS:
        cls_temperature_by_horizon.setdefault(horizon, 1.0)

    def _parse_horizon_counts(value: Any) -> dict[int, int]:
        result: dict[int, int] = {}
        if not isinstance(value, dict):
            return result
        for key, raw in value.items():
            try:
                horizon = int(key)
                result[horizon] = int(raw)
            except (TypeError, ValueError):
                continue
        return result

    cls_counts = _parse_horizon_counts(metrics_json.get("n_train_cls_by_horizon"))
    reg_counts = _parse_horizon_counts(metrics_json.get("n_train_reg_by_horizon"))
    turn_down_counts = _parse_horizon_counts(metrics_json.get("n_train_turn_down_by_horizon"))
    if 20 not in cls_counts:
        cls_counts[20] = int(metrics_json.get("n_train_cls") or 0)
    if 20 not in reg_counts:
        reg_counts[20] = int(metrics_json.get("n_train_reg") or n_train or 0)
    if 10 not in turn_down_counts:
        turn_down_counts[10] = int(metrics_json.get("n_train_turn_down") or 0)

    turn_down_default = turn_down_by_horizon.get(10)
    if turn_down_default is None:
        turn_down_default = turn_down_model
    return (
        TrainedModels(
            cls=cls_model,
            reg=reg_model,
            turn_up=turn_up_model,
            turn_down=turn_down_default,
            rank_up=rank_up_model,
            rank_down=rank_down_model,
            cls_by_horizon=cls_by_horizon,
            cls_temperature_by_horizon=cls_temperature_by_horizon,
            reg_by_horizon=reg_by_horizon,
            turn_down_by_horizon=turn_down_by_horizon,
            feature_columns=model_feature_columns,
            medians={str(k): float(v) for k, v in medians.items()},
            n_train_cls=int(metrics_json.get("n_train_cls") or 0),
            n_train_reg=int(metrics_json.get("n_train_reg") or n_train or 0),
            n_train_turn_up=int(metrics_json.get("n_train_turn_up") or 0),
            n_train_turn_down=int(metrics_json.get("n_train_turn_down") or 0),
            n_train_rank=int(metrics_json.get("n_train_rank") or 0),
            n_train_rank_groups=int(metrics_json.get("n_train_rank_groups") or 0),
            n_train_cls_by_horizon=cls_counts,
            n_train_reg_by_horizon=reg_counts,
            n_train_turn_down_by_horizon=turn_down_counts,
        ),
        str(model_version),
        int(n_train or 0),
    )


def _load_monthly_models_from_registry(conn) -> tuple[MonthlyTrainedModels, str, int, int]:
    row = _load_active_monthly_model_row(conn)
    if not row:
        raise RuntimeError("No active model in ml_monthly_model_registry")
    (
        model_version,
        _model_key,
        _label_version,
        metrics_json_raw,
        artifact_path_raw,
        n_train_abs,
        n_train_dir,
        _created_at,
    ) = row
    try:
        metrics_json = json.loads(metrics_json_raw) if metrics_json_raw else {}
    except Exception:
        metrics_json = {}
    medians_raw = metrics_json.get("medians")
    medians: dict[str, float] = {}
    if isinstance(medians_raw, dict):
        for key, raw in medians_raw.items():
            casted = _safe_float(raw)
            if casted is not None:
                medians[str(key)] = float(casted)
    try:
        artifact = json.loads(artifact_path_raw) if artifact_path_raw else {}
    except Exception:
        artifact = {}

    def _resolve_artifact_path(path_value: Any) -> str | None:
        if not path_value:
            return None
        path = Path(str(path_value))
        if path.exists():
            return str(path)
        fallback = _artifact_dir() / path.name
        if fallback.exists():
            return str(fallback)
        return None

    abs_model_path = _resolve_artifact_path(artifact.get("abs_model_path"))
    if not abs_model_path:
        raise RuntimeError("Monthly abs model artifact path is invalid")
    dir_model_path = _resolve_artifact_path(artifact.get("dir_model_path"))
    lgb = _import_lightgbm()
    abs_model = lgb.Booster(model_file=str(abs_model_path))
    dir_model = (
        lgb.Booster(model_file=str(dir_model_path))
        if dir_model_path and Path(str(dir_model_path)).exists()
        else None
    )
    feature_cols = metrics_json.get("feature_columns")
    model_feature_columns = (
        [str(col) for col in feature_cols if isinstance(col, str) and col]
        if isinstance(feature_cols, list)
        else []
    )
    if not model_feature_columns:
        try:
            n_features = int(abs_model.num_feature())
        except Exception:
            n_features = len(FEATURE_COLUMNS)
        model_feature_columns = list(FEATURE_COLUMNS[: max(1, min(n_features, len(FEATURE_COLUMNS)))])
    try:
        abs_temperature = float(metrics_json.get("abs_temperature", 1.0))
    except (TypeError, ValueError):
        abs_temperature = 1.0
    if not math.isfinite(abs_temperature) or abs_temperature <= 0:
        abs_temperature = 1.0
    try:
        dir_temperature = float(metrics_json.get("dir_temperature", 1.0))
    except (TypeError, ValueError):
        dir_temperature = 1.0
    if not math.isfinite(dir_temperature) or dir_temperature <= 0:
        dir_temperature = 1.0
    n_abs = int(
        n_train_abs
        if n_train_abs is not None
        else metrics_json.get("n_train_abs") or 0
    )
    n_dir = int(
        n_train_dir
        if n_train_dir is not None
        else metrics_json.get("n_train_dir") or 0
    )
    return (
        MonthlyTrainedModels(
            abs_cls=abs_model,
            dir_cls=dir_model,
            feature_columns=model_feature_columns,
            medians=medians,
            abs_temperature=float(abs_temperature),
            dir_temperature=float(dir_temperature),
            n_train_abs=n_abs,
            n_train_dir=n_dir,
        ),
        str(model_version),
        n_abs,
        n_dir,
    )


def _predict_monthly_for_dt_with_conn(conn, target_dt: int) -> dict[str, Any]:
    _ensure_ml_schema(conn)
    max_month = _to_month_start_int(target_dt)
    max_ym = _month_start_to_yyyymm(max_month) if max_month is not None else None
    frame = _load_monthly_feature_snapshots(conn, max_ym=max_ym)
    if frame.empty:
        return {
            "dt": int(target_dt),
            "pred_dt": None,
            "rows": 0,
            "model_version": None,
            "n_train_abs": 0,
            "n_train_dir": 0,
            "disabled_reason": "No monthly feature snapshots",
        }
    dt_series = pd.to_numeric(frame.get("dt"), errors="coerce")
    finite_mask = np.isfinite(dt_series.to_numpy(dtype=float, copy=False))
    frame = frame[finite_mask].copy()
    frame["dt"] = dt_series[finite_mask].to_numpy(dtype=int, copy=False)
    if max_month is not None:
        frame = frame[frame["dt"] <= int(max_month)]
    if frame.empty:
        return {
            "dt": int(target_dt),
            "pred_dt": None,
            "rows": 0,
            "model_version": None,
            "n_train_abs": 0,
            "n_train_dir": 0,
            "disabled_reason": "No monthly anchor <= target dt",
        }
    pred_dt = int(frame["dt"].max())
    pred_frame = frame[frame["dt"] == pred_dt].copy()
    if pred_frame.empty:
        return {
            "dt": int(target_dt),
            "pred_dt": pred_dt,
            "rows": 0,
            "model_version": None,
            "n_train_abs": 0,
            "n_train_dir": 0,
            "disabled_reason": "No monthly rows for anchor dt",
        }
    try:
        models, model_version, n_train_abs, n_train_dir = _load_monthly_models_from_registry(conn)
    except Exception as exc:
        return {
            "dt": int(target_dt),
            "pred_dt": pred_dt,
            "rows": 0,
            "model_version": None,
            "n_train_abs": 0,
            "n_train_dir": 0,
            "disabled_reason": str(exc),
        }
    pred = _predict_monthly_frame(pred_frame, models)
    rows: list[tuple] = []
    for item in pred.itertuples(index=False):
        code = str(getattr(item, "code", ""))
        p_abs_big = _safe_float(getattr(item, "p_abs_big", None))
        p_up_given_big = _safe_float(getattr(item, "p_up_given_big", None))
        p_up_big = _safe_float(getattr(item, "p_up_big", None))
        p_down_big = _safe_float(getattr(item, "p_down_big", None))
        score_up = _safe_float(getattr(item, "score_up", None))
        score_down = _safe_float(getattr(item, "score_down", None))
        if (
            not code
            or p_abs_big is None
            or p_up_given_big is None
            or p_up_big is None
            or p_down_big is None
            or score_up is None
            or score_down is None
        ):
            continue
        rows.append(
            (
                int(pred_dt),
                code,
                float(p_abs_big),
                float(p_up_given_big),
                float(p_up_big),
                float(p_down_big),
                float(score_up),
                float(score_down),
                str(model_version),
                int(n_train_abs),
                int(n_train_dir),
            )
        )
    conn.execute("DELETE FROM ml_monthly_pred WHERE dt = ?", [pred_dt])
    if rows:
        conn.executemany(
            """
            INSERT INTO ml_monthly_pred (
                dt,
                code,
                p_abs_big,
                p_up_given_big,
                p_up_big,
                p_down_big,
                score_up,
                score_down,
                model_version,
                n_train_abs,
                n_train_dir,
                computed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            rows,
        )
    return {
        "dt": int(target_dt),
        "pred_dt": int(pred_dt),
        "rows": int(len(rows)),
        "model_version": str(model_version),
        "n_train_abs": int(n_train_abs),
        "n_train_dir": int(n_train_dir),
    }


def predict_monthly_for_dt(dt: int | None = None) -> dict[str, Any]:
    if is_legacy_analysis_disabled():
        logger.info(
            "Skipping predict_monthly_for_dt because %s",
            legacy_analysis_disabled_log_value(),
        )
        return _legacy_monthly_prediction_disabled_result(dt)
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
        return _predict_monthly_for_dt_with_conn(conn, target_dt)


def _load_prediction_feature_frame(conn, target_dates: list[int]) -> pd.DataFrame:
    if not target_dates:
        return pd.DataFrame()
    placeholders = ", ".join("?" for _ in target_dates)
    return conn.execute(
        f"""
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
            cnt_7_prev1,
            weekly_breakout_up_prob,
            weekly_breakout_down_prob,
            weekly_range_prob,
            monthly_breakout_up_prob,
            monthly_breakout_down_prob,
            monthly_range_prob,
            candle_triplet_up_prob,
            candle_triplet_down_prob,
            candle_body_ratio,
            candle_upper_wick_ratio,
            candle_lower_wick_ratio,
            atr14_pct,
            range_pct,
            gap_pct,
            close_ret2,
            close_ret3,
            close_ret20,
            close_ret60,
            vol_ret5,
            vol_ret20,
            vol_ratio5_20,
            turnover20,
            turnover_z20,
            high20_dist,
            low20_dist,
            breakout20_up,
            breakout20_down,
            drawdown60,
            rebound60,
            market_ret1,
            market_ret5,
            market_ret20,
            rel_ret5,
            rel_ret20,
            breadth_above_ma20,
            breadth_above_ma60,
            sector_ret5,
            sector_ret20,
            rel_sector_ret5,
            rel_sector_ret20,
            sector_breadth_ma20
        FROM ml_feature_daily
        WHERE dt IN ({placeholders})
        ORDER BY dt, code
        """,
        [int(value) for value in target_dates],
    ).df()


def _build_ml_pred_rows(pred: pd.DataFrame, *, model_version: str, n_train: int) -> list[tuple[Any, ...]]:
    if pred is None or pred.empty:
        return []
    return [
        (
            int(item.dt),
            str(item.code),
            float(item.p_up),
            float(item.p_down),
            float(item.p_up_5),
            float(item.p_up_10),
            float(item.p_turn_up),
            float(item.p_turn_down),
            float(item.p_turn_down_5),
            float(item.p_turn_down_10),
            float(item.p_turn_down_20),
            float(item.rank_up_20),
            float(item.rank_down_20),
            float(item.ret_pred5),
            float(item.ret_pred10),
            float(item.ret_pred20),
            float(item.ev5),
            float(item.ev10),
            float(item.ev20),
            float(item.ev5_net),
            float(item.ev10_net),
            float(item.ev20_net),
            str(model_version),
            int(n_train),
        )
        for item in pred.itertuples(index=False)
    ]


def _replace_ml_predictions_for_dates(conn, target_dates: list[int], rows: list[tuple[Any, ...]]) -> None:
    if target_dates:
        placeholders = ", ".join("?" for _ in target_dates)
        conn.execute(
            f"DELETE FROM ml_pred_20d WHERE dt IN ({placeholders})",
            [int(value) for value in target_dates],
        )
    if rows:
        deduped_rows_by_key: dict[tuple[Any, Any], tuple[Any, ...]] = {}
        for row in rows:
            if len(row) < 2:
                continue
            dt_value = row[0]
            code_value = row[1]
            deduped_rows_by_key[(code_value, dt_value)] = row
        conn.executemany(
            """
            INSERT INTO ml_pred_20d (
                dt,
                code,
                p_up,
                p_down,
                p_up_5,
                p_up_10,
                p_turn_up,
                p_turn_down,
                p_turn_down_5,
                p_turn_down_10,
                p_turn_down_20,
                rank_up_20,
                rank_down_20,
                ret_pred5,
                ret_pred10,
                ret_pred20,
                ev5,
                ev10,
                ev20,
                ev5_net,
                ev10_net,
                ev20_net,
                model_version,
                n_train,
                computed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            list(deduped_rows_by_key.values()),
        )


def predict_for_dates_bulk(
    *,
    dates: list[int],
    chunk_size_days: int = 40,
    include_monthly: bool = False,
    progress_cb: Callable[[int, int, int], None] | None = None,
) -> dict[str, Any]:
    requested_dates = sorted(
        {
            int(value)
            for value in (_normalize_daily_dt_key(item) for item in dates)
            if value is not None
        }
    )
    if not requested_dates:
        return {
            "requested_dates": [],
            "resolved_dates": [],
            "predicted_dates": [],
            "rows_total": 0,
            "model_version": None,
            "n_train": 0,
            "skipped_dates": [],
            "monthly": None,
        }
    if is_legacy_analysis_disabled():
        logger.info(
            "Skipping predict_for_dates_bulk because %s dates=%s",
            legacy_analysis_disabled_log_value(),
            len(requested_dates),
        )
        return _legacy_bulk_prediction_disabled_result(requested_dates)
    chunk_size_days = max(1, int(chunk_size_days))
    cfg = load_ml_config()
    with get_conn() as conn:
        _ensure_ml_schema(conn)
        feature_rows = int(conn.execute("SELECT COUNT(*) FROM ml_feature_daily").fetchone()[0] or 0)
        refresh_start_dt: int | None = None
        refresh_end_dt: int | None = None
        repair_dates = _feature_input_repair_dates(conn, target_date_keys=requested_dates)
        if repair_dates:
            refresh_start_dt, refresh_end_dt = _feature_refresh_bounds(
                conn,
                start_key=repair_dates[0],
                end_key=repair_dates[-1],
            )
            _rebuild_feature_inputs_from_daily_bars(
                conn,
                start_dt=refresh_start_dt,
                end_dt=refresh_end_dt,
            )
            refresh_ml_feature_table(
                conn,
                feature_version=FEATURE_VERSION,
                start_dt=refresh_start_dt,
                end_dt=refresh_end_dt,
            )
            feature_rows = int(conn.execute("SELECT COUNT(*) FROM ml_feature_daily").fetchone()[0] or 0)
        if feature_rows <= 0:
            if requested_dates:
                refresh_start_dt, refresh_end_dt = _feature_refresh_bounds(
                    conn,
                    start_key=requested_dates[0],
                    end_key=requested_dates[-1],
                )
                refresh_ml_feature_table(
                    conn,
                    feature_version=FEATURE_VERSION,
                    start_dt=refresh_start_dt,
                    end_dt=refresh_end_dt,
                )
            else:
                refresh_ml_feature_table(conn, feature_version=FEATURE_VERSION)
        elif requested_dates:
            placeholders = ", ".join("?" for _ in requested_dates)
            dt_key_sql = _normalized_daily_dt_sql("dt")
            existing_rows = conn.execute(
                f"""
                SELECT DISTINCT {dt_key_sql} AS dt_key
                FROM ml_feature_daily
                WHERE {dt_key_sql} IN ({placeholders})
                """,
                [int(value) for value in requested_dates],
            ).fetchall()
            existing_requested_dates = {
                int(row[0]) for row in existing_rows if row and row[0] is not None
            }
            missing_requested_dates = [
                int(value) for value in requested_dates if int(value) not in existing_requested_dates
            ]
            if missing_requested_dates:
                refresh_start_dt, refresh_end_dt = _feature_refresh_bounds(
                    conn,
                    start_key=missing_requested_dates[0],
                    end_key=missing_requested_dates[-1],
                )
                refresh_ml_feature_table(
                    conn,
                    feature_version=FEATURE_VERSION,
                    start_dt=refresh_start_dt,
                    end_dt=refresh_end_dt,
                )

        dt_key_sql = _normalized_daily_dt_sql("dt")
        available_rows = conn.execute(
            f"""
            SELECT DISTINCT dt, {dt_key_sql} AS dt_key
            FROM ml_feature_daily
            WHERE {dt_key_sql} <= ?
            ORDER BY dt_key, dt
            """,
            [int(requested_dates[-1])],
        ).fetchall()
        available_by_key: dict[int, int] = {}
        for row in available_rows:
            if not row or row[0] is None or row[1] is None:
                continue
            available_by_key[int(row[1])] = int(row[0])
        available_date_keys = sorted(available_by_key)
        if not available_date_keys:
            raise RuntimeError("ml_feature_daily is empty")
        available_set = set(available_date_keys)

        resolved_dates_raw: list[int] = []
        skipped_dates: list[int] = []
        for req_dt in requested_dates:
            if req_dt in available_set:
                resolved_dates_raw.append(int(available_by_key[int(req_dt)]))
                continue
            idx = bisect_right(available_date_keys, int(req_dt)) - 1
            if idx >= 0:
                resolved_dates_raw.append(int(available_by_key[int(available_date_keys[idx])]))
            else:
                skipped_dates.append(int(req_dt))
        resolved_dates = sorted(set(resolved_dates_raw))
        if not resolved_dates:
            return {
                "requested_dates": requested_dates,
                "resolved_dates": [],
                "predicted_dates": [],
                "rows_total": 0,
                "model_version": None,
                "n_train": 0,
                "skipped_dates": skipped_dates,
                "monthly": None,
            }

        models, model_version, n_train = _load_models_from_registry(conn)
        monthly_bootstrap: dict[str, Any] | None = None
        if include_monthly and _load_active_monthly_model_row(conn) is None:
            try:
                monthly_bootstrap = _train_monthly_models_with_conn(conn)
            except Exception as exc:
                monthly_bootstrap = {
                    "ok": False,
                    "error": str(exc),
                }

        total_dates = int(len(resolved_dates))
        processed_dates = 0
        predicted_dates: set[int] = set()
        rows_total = 0
        for start in range(0, total_dates, chunk_size_days):
            chunk_dates = resolved_dates[start : start + chunk_size_days]
            frame = _load_prediction_feature_frame(conn, chunk_dates)
            if frame.empty:
                processed_dates += len(chunk_dates)
                if progress_cb is not None:
                    progress_cb(int(processed_dates), int(total_dates), int(chunk_dates[-1]))
                continue

            pred = _predict_frame(frame, models, cfg)
            rows = _build_ml_pred_rows(pred, model_version=str(model_version), n_train=int(n_train))
            chunk_predicted_dates = sorted({int(value) for value in pred["dt"].tolist()})
            _replace_ml_predictions_for_dates(conn, chunk_predicted_dates, rows)

            predicted_dates.update(chunk_predicted_dates)
            rows_total += int(len(rows))
            processed_dates += len(chunk_dates)
            if progress_cb is not None:
                progress_cb(int(processed_dates), int(total_dates), int(chunk_dates[-1]))

        monthly_result: dict[str, Any] | None = None
        if include_monthly:
            monthly_rows: list[dict[str, Any]] = []
            for dt_value in resolved_dates:
                try:
                    monthly_rows.append(_predict_monthly_for_dt_with_conn(conn, int(dt_value)))
                except Exception as exc:
                    monthly_rows.append(
                        {
                            "dt": int(dt_value),
                            "pred_dt": None,
                            "rows": 0,
                            "model_version": None,
                            "n_train_abs": 0,
                            "n_train_dir": 0,
                            "disabled_reason": str(exc),
                        }
                    )
            monthly_result = {
                "results": monthly_rows,
                "bootstrap": monthly_bootstrap,
            }

        return {
            "requested_dates": requested_dates,
            "resolved_dates": resolved_dates,
            "predicted_dates": sorted(predicted_dates),
            "rows_total": int(rows_total),
            "model_version": str(model_version),
            "n_train": int(n_train),
            "skipped_dates": skipped_dates,
            "monthly": monthly_result,
        }


def predict_for_dt(dt: int | None = None) -> dict[str, Any]:
    if is_legacy_analysis_disabled():
        logger.info(
            "Skipping predict_for_dt because %s dt=%s",
            legacy_analysis_disabled_log_value(),
            dt,
        )
        return _legacy_prediction_disabled_result(dt)
    cfg = load_ml_config()
    with get_conn() as conn:
        _ensure_ml_schema(conn)

        target_dt = int(dt) if dt is not None else None
        target_dt_key = _normalize_daily_dt_key(target_dt)
        feature_rows = int(conn.execute("SELECT COUNT(*) FROM ml_feature_daily").fetchone()[0] or 0)
        repair_dates = _feature_input_repair_dates(
            conn,
            target_date_keys=[int(target_dt_key)] if target_dt_key is not None else [],
        )
        if repair_dates:
            refresh_start_dt, refresh_end_dt = _feature_refresh_bounds(
                conn,
                start_key=repair_dates[0],
                end_key=repair_dates[-1],
            )
            _rebuild_feature_inputs_from_daily_bars(
                conn,
                start_dt=refresh_start_dt,
                end_dt=refresh_end_dt,
            )
            refresh_ml_feature_table(
                conn,
                feature_version=FEATURE_VERSION,
                start_dt=refresh_start_dt,
                end_dt=refresh_end_dt,
            )
            feature_rows = int(conn.execute("SELECT COUNT(*) FROM ml_feature_daily").fetchone()[0] or 0)
        needs_feature_refresh = feature_rows == 0
        dt_key_sql = _normalized_daily_dt_sql("dt")
        if not needs_feature_refresh and target_dt_key is not None:
            has_target = conn.execute(
                f"SELECT 1 FROM ml_feature_daily WHERE {dt_key_sql} = ? LIMIT 1",
                [int(target_dt_key)],
            ).fetchone()
            needs_feature_refresh = has_target is None
        if needs_feature_refresh:
            if target_dt_key is not None and feature_rows > 0:
                refresh_start_dt, refresh_end_dt = _feature_refresh_bounds(
                    conn,
                    start_key=int(target_dt_key),
                    end_key=int(target_dt_key),
                )
                refresh_ml_feature_table(
                    conn,
                    feature_version=FEATURE_VERSION,
                    start_dt=refresh_start_dt,
                    end_dt=refresh_end_dt,
                )
            else:
                refresh_ml_feature_table(conn, feature_version=FEATURE_VERSION)

        if target_dt is None:
            row = conn.execute("SELECT MAX(dt) FROM ml_feature_daily").fetchone()
            if not row or row[0] is None:
                raise RuntimeError("ml_feature_daily is empty")
            target_dt = int(row[0])
        else:
            has_target = conn.execute(
                f"SELECT MAX(dt) FROM ml_feature_daily WHERE {dt_key_sql} = ?",
                [int(target_dt_key)] if target_dt_key is not None else [int(target_dt)],
            ).fetchone()
            if not has_target or has_target[0] is None:
                fallback_row = conn.execute(
                    f"SELECT MAX(dt) FROM ml_feature_daily WHERE {dt_key_sql} <= ?",
                    [int(target_dt_key)] if target_dt_key is not None else [int(target_dt)],
                ).fetchone()
                if not fallback_row or fallback_row[0] is None:
                    raise RuntimeError(f"No features found for dt={target_dt}")
                target_dt = int(fallback_row[0])
            else:
                target_dt = int(has_target[0])

        frame = _load_prediction_feature_frame(conn, [int(target_dt)])
        if frame.empty:
            raise RuntimeError(f"No features found for dt={target_dt}")

        models, model_version, n_train = _load_models_from_registry(conn)
        pred = _predict_frame(frame, models, cfg)
        monthly_bootstrap: dict[str, Any] | None = None
        if _load_active_monthly_model_row(conn) is None:
            try:
                monthly_bootstrap = _train_monthly_models_with_conn(conn)
            except Exception as exc:
                monthly_bootstrap = {
                    "ok": False,
                    "error": str(exc),
                }
        rows = _build_ml_pred_rows(pred, model_version=str(model_version), n_train=int(n_train))
        _replace_ml_predictions_for_dates(conn, [int(target_dt)], rows)
        try:
            monthly_result = _predict_monthly_for_dt_with_conn(conn, target_dt)
        except Exception as exc:
            monthly_result = {
                "dt": int(target_dt),
                "pred_dt": None,
                "rows": 0,
                "model_version": None,
                "n_train_abs": 0,
                "n_train_dir": 0,
                "disabled_reason": str(exc),
            }
        if monthly_bootstrap is not None:
            monthly_result["bootstrap"] = monthly_bootstrap
        return {
            "dt": int(target_dt),
            "rows": int(len(rows)),
            "model_version": model_version,
            "monthly": monthly_result,
        }


def predict_latest() -> dict[str, Any]:
    return predict_for_dt(dt=None)


def get_ml_status() -> dict[str, Any]:
    if is_legacy_analysis_disabled():
        cfg = load_ml_config()
        return {
            "has_active_model": False,
            "disabled_reason": "legacy_analysis_disabled",
            "config": {
                "neutral_band_pct": cfg.neutral_band_pct,
                "p_up_threshold": cfg.p_up_threshold,
                "top_n": cfg.top_n,
                "cost_bps": cfg.cost_bps,
                "train_days": cfg.train_days,
                "test_days": cfg.test_days,
                "step_days": cfg.step_days,
                "embargo_days": cfg.embargo_days,
                "rank_boost_round": cfg.rank_boost_round,
                "rule_weight": cfg.rule_weight,
                "ev_weight": cfg.ev_weight,
                "prob_weight": cfg.prob_weight,
                "rank_weight": cfg.rank_weight,
                "turn_weight": cfg.turn_weight,
                "min_prob_up": cfg.min_prob_up,
                "min_prob_down": cfg.min_prob_down,
                "min_turn_prob_up": cfg.min_turn_prob_up,
                "min_turn_prob_down": cfg.min_turn_prob_down,
                "min_turn_margin": cfg.min_turn_margin,
                "auto_promote": cfg.auto_promote,
                "allow_bootstrap_promotion": cfg.allow_bootstrap_promotion,
                "min_wf_fold_count": cfg.min_wf_fold_count,
                "min_wf_daily_count": cfg.min_wf_daily_count,
                "min_wf_mean_ret20_net": cfg.min_wf_mean_ret20_net,
                "min_wf_win_rate": cfg.min_wf_win_rate,
                "min_wf_p05_ret20_net": cfg.min_wf_p05_ret20_net,
                "min_wf_cvar05_ret20_net": cfg.min_wf_cvar05_ret20_net,
                "robust_lb_lambda": cfg.robust_lb_lambda,
                "min_wf_robust_lb": cfg.min_wf_robust_lb,
                "max_wf_p_value_mean_gt0": cfg.max_wf_p_value_mean_gt0,
                "min_wf_lcb95_ret20_net": cfg.min_wf_lcb95_ret20_net,
                "min_wf_up_mean_ret20_net": cfg.min_wf_up_mean_ret20_net,
                "min_wf_down_mean_ret20_net": cfg.min_wf_down_mean_ret20_net,
                "min_wf_combined_mean_ret20_net": cfg.min_wf_combined_mean_ret20_net,
                "require_champion_improvement": cfg.require_champion_improvement,
                "min_delta_mean_ret20_net": cfg.min_delta_mean_ret20_net,
                "min_delta_robust_lb": cfg.min_delta_robust_lb,
                "min_delta_lcb95_ret20_net": cfg.min_delta_lcb95_ret20_net,
                "live_guard_enabled": cfg.live_guard_enabled,
                "live_guard_lookback_days": cfg.live_guard_lookback_days,
                "live_guard_min_daily_count": cfg.live_guard_min_daily_count,
                "live_guard_min_mean_ret20_net": cfg.live_guard_min_mean_ret20_net,
                "live_guard_min_robust_lb": cfg.live_guard_min_robust_lb,
                "live_guard_max_p_value_mean_gt0": cfg.live_guard_max_p_value_mean_gt0,
                "live_guard_min_lcb95_ret20_net": cfg.live_guard_min_lcb95_ret20_net,
                "live_guard_allow_rollback": cfg.live_guard_allow_rollback,
                "wf_use_expanding_train": cfg.wf_use_expanding_train,
                "wf_max_train_days": cfg.wf_max_train_days,
            },
            "monthly": {
                "model_version": None,
                "pred_dt": None,
                "n_train_abs": None,
                "n_train_dir": None,
                "label_rows": 0,
            },
            "active_model": None,
            "metrics": {},
            "latest_prediction": None,
            "latest_training_audit": None,
            "latest_live_guard_audit": None,
        }
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
        active_monthly = _load_active_monthly_model_row(conn)
        latest_monthly_pred = None
        if _table_exists(conn, "ml_monthly_pred"):
            latest_monthly_pred = conn.execute(
                """
                SELECT dt, model_version, COUNT(*) AS n
                FROM ml_monthly_pred
                GROUP BY dt, model_version
                ORDER BY dt DESC
                LIMIT 1
                """
            ).fetchone()
        monthly_label_rows = 0
        if _table_exists(conn, "ml_monthly_label"):
            row = conn.execute("SELECT COUNT(*) FROM ml_monthly_label").fetchone()
            monthly_label_rows = int(row[0]) if row and row[0] is not None else 0
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
                "rank_boost_round": cfg.rank_boost_round,
                "rule_weight": cfg.rule_weight,
                "ev_weight": cfg.ev_weight,
                "prob_weight": cfg.prob_weight,
                "rank_weight": cfg.rank_weight,
                "turn_weight": cfg.turn_weight,
                "min_prob_up": cfg.min_prob_up,
                "min_prob_down": cfg.min_prob_down,
                "min_turn_prob_up": cfg.min_turn_prob_up,
                "min_turn_prob_down": cfg.min_turn_prob_down,
                "min_turn_margin": cfg.min_turn_margin,
                "auto_promote": cfg.auto_promote,
                "allow_bootstrap_promotion": cfg.allow_bootstrap_promotion,
                "min_wf_fold_count": cfg.min_wf_fold_count,
                "min_wf_daily_count": cfg.min_wf_daily_count,
                "min_wf_mean_ret20_net": cfg.min_wf_mean_ret20_net,
                "min_wf_win_rate": cfg.min_wf_win_rate,
                "min_wf_p05_ret20_net": cfg.min_wf_p05_ret20_net,
                "min_wf_cvar05_ret20_net": cfg.min_wf_cvar05_ret20_net,
                "robust_lb_lambda": cfg.robust_lb_lambda,
                "min_wf_robust_lb": cfg.min_wf_robust_lb,
                "max_wf_p_value_mean_gt0": cfg.max_wf_p_value_mean_gt0,
                "min_wf_lcb95_ret20_net": cfg.min_wf_lcb95_ret20_net,
                "min_wf_up_mean_ret20_net": cfg.min_wf_up_mean_ret20_net,
                "min_wf_down_mean_ret20_net": cfg.min_wf_down_mean_ret20_net,
                "min_wf_combined_mean_ret20_net": cfg.min_wf_combined_mean_ret20_net,
                "require_champion_improvement": cfg.require_champion_improvement,
                "min_delta_mean_ret20_net": cfg.min_delta_mean_ret20_net,
                "min_delta_robust_lb": cfg.min_delta_robust_lb,
                "min_delta_lcb95_ret20_net": cfg.min_delta_lcb95_ret20_net,
                "live_guard_enabled": cfg.live_guard_enabled,
                "live_guard_lookback_days": cfg.live_guard_lookback_days,
                "live_guard_min_daily_count": cfg.live_guard_min_daily_count,
                "live_guard_min_mean_ret20_net": cfg.live_guard_min_mean_ret20_net,
                "live_guard_min_robust_lb": cfg.live_guard_min_robust_lb,
                "live_guard_max_p_value_mean_gt0": cfg.live_guard_max_p_value_mean_gt0,
                "live_guard_min_lcb95_ret20_net": cfg.live_guard_min_lcb95_ret20_net,
                "live_guard_allow_rollback": cfg.live_guard_allow_rollback,
                "wf_use_expanding_train": cfg.wf_use_expanding_train,
                "wf_max_train_days": cfg.wf_max_train_days,
            },
            "monthly": {
                "model_version": str(active_monthly[0]) if active_monthly and active_monthly[0] is not None else None,
                "pred_dt": int(latest_monthly_pred[0]) if latest_monthly_pred and latest_monthly_pred[0] is not None else None,
                "n_train_abs": int(active_monthly[5]) if active_monthly and active_monthly[5] is not None else None,
                "n_train_dir": int(active_monthly[6]) if active_monthly and active_monthly[6] is not None else None,
                "label_rows": int(monthly_label_rows),
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
        payload["latest_training_audit"] = _load_latest_training_audit(conn)
        payload["latest_live_guard_audit"] = _load_latest_live_guard_audit(conn)
        return payload


def train_models(
    *,
    start_dt: int | None = None,
    end_dt: int | None = None,
    dry_run: bool = False,
    progress_cb: Callable[[int, str], None] | None = None,
) -> dict[str, Any]:
    return _train_models_impl(
        start_dt=start_dt,
        end_dt=end_dt,
        dry_run=dry_run,
        progress_cb=progress_cb,
    )

def enforce_live_guard() -> dict[str, Any]:
    return legacy_predict_runtime.enforce_live_guard()


def get_latest_live_guard_status() -> dict[str, Any]:
    return legacy_predict_runtime.get_latest_live_guard_status()


def predict_monthly_for_dt(dt: int | None = None) -> dict[str, Any]:
    return legacy_predict_runtime.predict_monthly_for_dt(dt)


def predict_for_dates_bulk(
    *,
    dates: list[int],
    chunk_size_days: int = 40,
    include_monthly: bool = False,
    progress_cb: Callable[[int, int, int], None] | None = None,
) -> dict[str, Any]:
    return legacy_predict_runtime.predict_for_dates_bulk(
        dates=dates,
        chunk_size_days=chunk_size_days,
        include_monthly=include_monthly,
        progress_cb=progress_cb,
    )


def predict_for_dt(dt: int | None = None) -> dict[str, Any]:
    return legacy_predict_runtime.predict_for_dt(dt)


def predict_latest() -> dict[str, Any]:
    return legacy_predict_runtime.predict_latest()
