from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import duckdb
import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.backend.services import tradex_research_contracts as contracts


AXIS_ID = "teppan_chart_pattern_discovery_v1"
SCHEMA_PREFIX = "tradex_teppan_chart_pattern_discovery_v1"
DEFAULT_SOURCE_DB = Path(
    r"G:\Tradex\db\meemee_snapshots\20260512T130453Z_winner_lookalike_candle_decomposition_v1\stocks.duckdb"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\teppan_chart_pattern_discovery_v1")

DEFAULT_YEARS = 10
MIN_HISTORY_DAYS = 140
MIN_PATTERN_TRADES = 250
MIN_PATTERN_SYMBOLS = 30
MIN_PATTERN_MONTHS = 18
SEVERE_LOSS_THRESHOLD = -0.10

SIGNAL_FEATURE_COLUMNS = {
    "daily_ma_stack",
    "daily_ma60_slope_state",
    "daily_ret20_state",
    "daily_candle_state",
    "daily_volume_state",
    "daily_sequence_state",
    "weekly_trend_state",
    "weekly_ret4_state",
    "weekly_candle_state",
    "weekly_volume_state",
    "monthly_trend_state",
    "monthly_ret6_state",
    "monthly_candle_state",
    "monthly_volume_state",
}
LABEL_COLUMNS = {
    "entry_next_open",
    "ret20",
    "ret40",
    "mfe20",
    "mae20",
    "win20",
    "win40",
    "severe_loss20",
}

PATTERN_FAMILIES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("multi_tf_trend_core", ("daily_ma_stack", "weekly_trend_state", "monthly_trend_state")),
    (
        "multi_tf_trend_volume",
        ("daily_ma_stack", "daily_volume_state", "weekly_trend_state", "weekly_volume_state", "monthly_trend_state"),
    ),
    (
        "daily_weekly_action",
        ("daily_ma_stack", "daily_candle_state", "daily_volume_state", "daily_sequence_state", "weekly_trend_state"),
    ),
    (
        "full_teppan_shape",
        (
            "daily_ma_stack",
            "daily_ma60_slope_state",
            "daily_candle_state",
            "daily_volume_state",
            "weekly_trend_state",
            "weekly_candle_state",
            "monthly_trend_state",
            "monthly_candle_state",
        ),
    ),
    (
        "higher_frame_confirmed_daily",
        (
            "daily_ma_stack",
            "daily_ret20_state",
            "daily_candle_state",
            "daily_volume_state",
            "weekly_ret4_state",
            "monthly_ret6_state",
        ),
    ),
)

REQUIRED_ARTIFACTS = (
    "evaluation_contract.json",
    "run_manifest.json",
    "feature_availability_audit.json",
    "pattern_ledger.jsonl",
    "pattern_leaderboard.json",
    "teppan_candidates.json",
    "negative_pattern_contrast.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{AXIS_ID}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    try:
        import numpy as np

        if isinstance(value, np.generic):
            return _json_ready(value.item())
    except Exception:
        pass
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _json_text(payload: Any) -> str:
    return json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, default=str)


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_json_text(payload) + "\n", encoding="utf-8")
    return path


def _write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(_json_text(row) + "\n" for row in rows), encoding="utf-8")
    return path


def _stable_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(_json_text(payload).encode("utf-8")).hexdigest()


def _safe_path(value: str | Path | None, default: Path) -> Path:
    if value is None or not str(value).strip():
        return default.resolve()
    return Path(str(value)).expanduser().resolve()


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _safe_div(num: pd.Series, den: pd.Series) -> pd.Series:
    denominator = pd.to_numeric(den, errors="coerce").astype(float).mask(lambda s: s == 0.0)
    return pd.to_numeric(num, errors="coerce").astype(float).div(denominator).replace([math.inf, -math.inf], pd.NA)


def _date_norm_expr(column: str) -> str:
    num = f"TRY_CAST({column} AS BIGINT)"
    dte = f"TRY_CAST({column} AS DATE)"
    return (
        "CASE "
        f"WHEN {dte} IS NOT NULL THEN CAST(strftime({dte}, '%Y%m%d') AS INTEGER) "
        f"WHEN {num} BETWEEN 19000101 AND 20991231 THEN CAST({num} AS INTEGER) "
        f"WHEN {num} >= 1000000000000 THEN CAST(strftime(to_timestamp({num} / 1000), '%Y%m%d') AS INTEGER) "
        f"WHEN {num} BETWEEN 600000000 AND 5000000000 THEN CAST(strftime(to_timestamp({num}), '%Y%m%d') AS INTEGER) "
        "ELSE NULL END"
    )


def _resolve_source_db(source_db: str | Path | None) -> Path:
    if source_db and str(source_db).strip():
        path = Path(str(source_db)).expanduser().resolve()
    elif os.getenv("STOCKS_DB_PATH"):
        path = Path(os.environ["STOCKS_DB_PATH"]).expanduser().resolve()
    else:
        path = DEFAULT_SOURCE_DB.resolve()
    if not path.exists():
        raise FileNotFoundError(f"source DB not found: {path}")
    return path


def _ymd_to_timestamp(value: int) -> pd.Timestamp:
    return pd.to_datetime(str(int(value)), format="%Y%m%d")


def _timestamp_to_ymd(value: pd.Timestamp) -> int:
    return int(value.strftime("%Y%m%d"))


def _load_max_daily_ymd(conn: duckdb.DuckDBPyConnection) -> int:
    expr = _date_norm_expr("date")
    row = conn.execute(f"SELECT MAX({expr}) FROM daily_bars WHERE lower(coalesce(source, '')) = 'pan'").fetchone()
    if not row or row[0] is None:
        raise RuntimeError("daily_bars has no PAN max date")
    return int(row[0])


def _load_daily_rows(conn: duckdb.DuckDBPyConnection, *, start_ymd: int, end_ymd: int) -> pd.DataFrame:
    b_expr = _date_norm_expr("b.date")
    m_expr = _date_norm_expr("m.date")
    frame = conn.execute(
        f"""
        WITH b AS (
            SELECT code, {b_expr} AS ymd, o, h, l, c, v, source
            FROM daily_bars AS b
        ),
        m AS (
            SELECT code, {m_expr} AS ymd, ma20, ma60
            FROM daily_ma AS m
        )
        SELECT b.code, b.ymd, b.o, b.h, b.l, b.c, b.v, m.ma20, m.ma60
        FROM b
        LEFT JOIN m ON b.code = m.code AND b.ymd = m.ymd
        WHERE b.ymd BETWEEN ? AND ?
          AND lower(coalesce(b.source, '')) = 'pan'
          AND b.o > 0 AND b.h > 0 AND b.l > 0 AND b.c > 0
        ORDER BY b.code, b.ymd
        """,
        [int(start_ymd), int(end_ymd)],
    ).fetchdf()
    if frame.empty:
        raise RuntimeError("daily_bars query returned no rows")
    frame["code"] = frame["code"].astype(str)
    frame["date"] = pd.to_datetime(frame["ymd"].astype(str), format="%Y%m%d")
    return frame


def _load_monthly_rows(conn: duckdb.DuckDBPyConnection, *, start_ymd: int, end_ymd: int) -> pd.DataFrame:
    b_expr = _date_norm_expr("b.month")
    m_expr = _date_norm_expr("m.month")
    frame = conn.execute(
        f"""
        WITH b AS (
            SELECT code, {b_expr} AS ymd, o, h, l, c, v
            FROM monthly_bars AS b
        ),
        m AS (
            SELECT code, {m_expr} AS ymd, ma20, ma60
            FROM monthly_ma AS m
        )
        SELECT b.code, b.ymd, b.o, b.h, b.l, b.c, b.v, m.ma20, m.ma60
        FROM b
        LEFT JOIN m ON b.code = m.code AND b.ymd = m.ymd
        WHERE b.ymd BETWEEN ? AND ?
          AND b.o > 0 AND b.h > 0 AND b.l > 0 AND b.c > 0
        ORDER BY b.code, b.ymd
        """,
        [int(start_ymd), int(end_ymd)],
    ).fetchdf()
    if frame.empty:
        raise RuntimeError("monthly_bars query returned no rows")
    frame["code"] = frame["code"].astype(str)
    frame["month_date"] = pd.to_datetime(frame["ymd"].astype(str), format="%Y%m%d")
    frame["month_key"] = frame["month_date"].dt.to_period("M")
    return frame


def _bucket_return(series: pd.Series, *, strong_down: float, down: float, up: float, strong_up: float, prefix: str) -> pd.Series:
    out = pd.Series(f"{prefix}_flat", index=series.index, dtype="object")
    out[pd.to_numeric(series, errors="coerce") <= strong_down] = f"{prefix}_strong_down"
    out[(pd.to_numeric(series, errors="coerce") > strong_down) & (pd.to_numeric(series, errors="coerce") <= down)] = f"{prefix}_down"
    out[(pd.to_numeric(series, errors="coerce") >= up) & (pd.to_numeric(series, errors="coerce") < strong_up)] = f"{prefix}_up"
    out[pd.to_numeric(series, errors="coerce") >= strong_up] = f"{prefix}_strong_up"
    return out.fillna(f"{prefix}_unknown")


def _candle_state(open_: pd.Series, high: pd.Series, low: pd.Series, close: pd.Series, *, prefix: str) -> pd.Series:
    candle_range = (high - low).replace(0, pd.NA)
    body = pd.to_numeric((close - open_).abs() / candle_range, errors="coerce").fillna(0.0)
    close_position = pd.to_numeric((close - low) / candle_range, errors="coerce").fillna(0.5)
    upper = pd.to_numeric((high - pd.concat([open_, close], axis=1).max(axis=1)) / candle_range, errors="coerce").fillna(0.0)
    lower = pd.to_numeric((pd.concat([open_, close], axis=1).min(axis=1) - low) / candle_range, errors="coerce").fillna(0.0)
    out = pd.Series(f"{prefix}_small_neutral", index=open_.index, dtype="object")
    out[(close > open_) & (body >= 0.45) & (close_position >= 0.65)] = f"{prefix}_strong_bull"
    out[(close > open_) & (lower >= 0.35) & (upper <= 0.35)] = f"{prefix}_lower_wick_bull"
    out[(upper >= 0.45) & (close_position <= 0.55)] = f"{prefix}_upper_wick_warning"
    out[(close < open_) & (body >= 0.45) & (close_position <= 0.35)] = f"{prefix}_strong_bear"
    out[body <= 0.12] = f"{prefix}_doji"
    return out


def build_daily_feature_frame(daily: pd.DataFrame, *, anchor_start_ymd: int) -> pd.DataFrame:
    frame = daily.sort_values(["code", "date"], kind="stable").copy()
    grouped = frame.groupby("code", sort=False)
    frame["history_days"] = grouped.cumcount() + 1
    frame["ma5"] = grouped["c"].transform(lambda s: s.rolling(5, min_periods=5).mean())
    if "ma20" not in frame.columns or frame["ma20"].isna().all():
        frame["ma20"] = grouped["c"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    if "ma60" not in frame.columns or frame["ma60"].isna().all():
        frame["ma60"] = grouped["c"].transform(lambda s: s.rolling(60, min_periods=60).mean())
    frame["ret5"] = grouped["c"].transform(lambda s: s / s.shift(5) - 1.0)
    frame["ret20"] = grouped["c"].transform(lambda s: s / s.shift(20) - 1.0)
    frame["ma20_slope_20d"] = grouped["ma20"].transform(lambda s: s / s.shift(20) - 1.0)
    frame["ma60_slope_20d"] = grouped["ma60"].transform(lambda s: s / s.shift(20) - 1.0)
    vol5 = grouped["v"].transform(lambda s: s.rolling(5, min_periods=5).mean())
    vol20 = grouped["v"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    frame["vol_ratio5_20"] = _safe_div(vol5, vol20)
    frame["entry_next_open"] = grouped["o"].shift(-1)
    frame["future_close_20"] = grouped["c"].shift(-20)
    frame["future_close_40"] = grouped["c"].shift(-40)
    frame["future_high_20"] = grouped["h"].transform(lambda s: s.shift(-1).iloc[::-1].rolling(20, min_periods=20).max().iloc[::-1])
    frame["future_low_20"] = grouped["l"].transform(lambda s: s.shift(-1).iloc[::-1].rolling(20, min_periods=20).min().iloc[::-1])
    frame["ret20_fwd"] = _safe_div(frame["future_close_20"] - frame["entry_next_open"], frame["entry_next_open"])
    frame["ret40_fwd"] = _safe_div(frame["future_close_40"] - frame["entry_next_open"], frame["entry_next_open"])
    frame["mfe20"] = _safe_div(frame["future_high_20"] - frame["entry_next_open"], frame["entry_next_open"])
    frame["mae20"] = _safe_div(frame["future_low_20"] - frame["entry_next_open"], frame["entry_next_open"])

    frame["daily_ma_stack"] = "daily_stack_mixed"
    frame.loc[(frame["ma5"] > frame["ma20"]) & (frame["ma20"] > frame["ma60"]), "daily_ma_stack"] = "daily_bull_stack_5_20_60"
    frame.loc[(frame["ma5"] > frame["ma20"]) & (frame["ma20"] <= frame["ma60"]), "daily_ma_stack"] = "daily_near_bull_5_over_20_under_60"
    frame.loc[(frame["ma5"] <= frame["ma20"]) & (frame["ma20"] > frame["ma60"]), "daily_ma_stack"] = "daily_pullback_20_over_60"
    frame.loc[(frame["ma5"] < frame["ma20"]) & (frame["ma20"] < frame["ma60"]), "daily_ma_stack"] = "daily_bear_stack_5_20_60"
    frame["daily_ma60_slope_state"] = "daily_ma60_flat"
    frame.loc[frame["ma60_slope_20d"] >= 0.02, "daily_ma60_slope_state"] = "daily_ma60_rising"
    frame.loc[frame["ma60_slope_20d"] <= -0.02, "daily_ma60_slope_state"] = "daily_ma60_falling"
    frame["daily_ret20_state"] = _bucket_return(frame["ret20"], strong_down=-0.08, down=-0.03, up=0.03, strong_up=0.08, prefix="daily20")
    frame["daily_candle_state"] = _candle_state(frame["o"], frame["h"], frame["l"], frame["c"], prefix="daily")
    frame["daily_volume_state"] = "daily_volume_normal"
    frame.loc[frame["vol_ratio5_20"] >= 1.6, "daily_volume_state"] = "daily_volume_expansion"
    frame.loc[frame["vol_ratio5_20"] <= 0.7, "daily_volume_state"] = "daily_volume_dry"
    strong_bull = frame["daily_candle_state"].isin({"daily_strong_bull", "daily_lower_wick_bull"})
    weak_bear = frame["daily_candle_state"].isin({"daily_strong_bear", "daily_upper_wick_warning"})
    frame["strong_bull_count_5"] = strong_bull.astype(float).groupby(frame["code"], sort=False).transform(lambda s: s.rolling(5, min_periods=5).sum())
    frame["weak_bear_count_5"] = weak_bear.astype(float).groupby(frame["code"], sort=False).transform(lambda s: s.rolling(5, min_periods=5).sum())
    frame["daily_sequence_state"] = "daily_sequence_mixed"
    frame.loc[(frame["strong_bull_count_5"] >= 2) & (frame["weak_bear_count_5"] <= 1), "daily_sequence_state"] = "daily_sequence_bullish"
    frame.loc[(frame["weak_bear_count_5"] >= 2), "daily_sequence_state"] = "daily_sequence_warning"
    frame["anchor_month"] = frame["date"].dt.to_period("M").astype(str)
    frame["week_key"] = frame["date"].dt.to_period("W-FRI").astype(str)
    frame["month_key"] = frame["date"].dt.to_period("M").astype(str)
    eligible = frame[
        (frame["ymd"] >= int(anchor_start_ymd))
        & (frame["history_days"] >= MIN_HISTORY_DAYS)
        & frame["entry_next_open"].notna()
        & frame["ret20_fwd"].notna()
        & frame["ret40_fwd"].notna()
        & frame["mfe20"].notna()
        & frame["mae20"].notna()
    ].copy()
    return eligible


def build_weekly_feature_frame(daily: pd.DataFrame) -> pd.DataFrame:
    work = daily.sort_values(["code", "date"], kind="stable").copy()
    work["week_period"] = work["date"].dt.to_period("W-FRI")
    weekly = (
        work.groupby(["code", "week_period"], sort=True)
        .agg(o=("o", "first"), h=("h", "max"), l=("l", "min"), c=("c", "last"), v=("v", "sum"), week_end=("date", "max"))
        .reset_index()
    )
    grouped = weekly.groupby("code", sort=False)
    weekly["weekly_ret1"] = grouped["c"].transform(lambda s: s / s.shift(1) - 1.0)
    weekly["weekly_ret4"] = grouped["c"].transform(lambda s: s / s.shift(4) - 1.0)
    weekly["weekly_ma4"] = grouped["c"].transform(lambda s: s.rolling(4, min_periods=4).mean())
    weekly["weekly_ma13"] = grouped["c"].transform(lambda s: s.rolling(13, min_periods=13).mean())
    vol4 = grouped["v"].transform(lambda s: s.rolling(4, min_periods=4).mean())
    vol13 = grouped["v"].transform(lambda s: s.rolling(13, min_periods=13).mean())
    weekly["weekly_vol_ratio4_13"] = _safe_div(vol4, vol13)
    weekly["weekly_trend_state"] = "weekly_mixed"
    weekly.loc[(weekly["c"] > weekly["weekly_ma4"]) & (weekly["weekly_ma4"] > weekly["weekly_ma13"]), "weekly_trend_state"] = "weekly_uptrend"
    weekly.loc[(weekly["c"] < weekly["weekly_ma4"]) & (weekly["weekly_ma4"] < weekly["weekly_ma13"]), "weekly_trend_state"] = "weekly_downtrend"
    weekly.loc[(weekly["c"] > weekly["weekly_ma13"]) & (weekly["weekly_ma4"] <= weekly["weekly_ma13"]), "weekly_trend_state"] = "weekly_recovery"
    weekly["weekly_ret4_state"] = _bucket_return(weekly["weekly_ret4"], strong_down=-0.10, down=-0.04, up=0.04, strong_up=0.10, prefix="weekly4")
    weekly["weekly_candle_state"] = _candle_state(weekly["o"], weekly["h"], weekly["l"], weekly["c"], prefix="weekly")
    weekly["weekly_volume_state"] = "weekly_volume_normal"
    weekly.loc[weekly["weekly_vol_ratio4_13"] >= 1.5, "weekly_volume_state"] = "weekly_volume_expansion"
    weekly.loc[weekly["weekly_vol_ratio4_13"] <= 0.75, "weekly_volume_state"] = "weekly_volume_dry"
    weekly["effective_week_key"] = (weekly["week_period"] + 1).astype(str)
    return weekly[["code", "effective_week_key", "weekly_trend_state", "weekly_ret4_state", "weekly_candle_state", "weekly_volume_state"]]


def build_monthly_feature_frame(monthly: pd.DataFrame) -> pd.DataFrame:
    frame = monthly.sort_values(["code", "month_key"], kind="stable").copy()
    grouped = frame.groupby("code", sort=False)
    frame["monthly_ret3"] = grouped["c"].transform(lambda s: s / s.shift(3) - 1.0)
    frame["monthly_ret6"] = grouped["c"].transform(lambda s: s / s.shift(6) - 1.0)
    vol3 = grouped["v"].transform(lambda s: s.rolling(3, min_periods=3).mean())
    vol6 = grouped["v"].transform(lambda s: s.rolling(6, min_periods=6).mean())
    frame["monthly_vol_ratio3_6"] = _safe_div(vol3, vol6)
    frame["monthly_trend_state"] = "monthly_mixed"
    frame.loc[(frame["c"] > frame["ma20"]) & (frame["ma20"] > frame["ma60"]), "monthly_trend_state"] = "monthly_uptrend"
    frame.loc[(frame["c"] < frame["ma20"]) & (frame["ma20"] < frame["ma60"]), "monthly_trend_state"] = "monthly_downtrend"
    frame.loc[(frame["c"] > frame["ma20"]) & (frame["ma20"] <= frame["ma60"]), "monthly_trend_state"] = "monthly_recovery"
    frame["monthly_ret6_state"] = _bucket_return(frame["monthly_ret6"], strong_down=-0.18, down=-0.08, up=0.08, strong_up=0.18, prefix="monthly6")
    frame["monthly_candle_state"] = _candle_state(frame["o"], frame["h"], frame["l"], frame["c"], prefix="monthly")
    frame["monthly_volume_state"] = "monthly_volume_normal"
    frame.loc[frame["monthly_vol_ratio3_6"] >= 1.4, "monthly_volume_state"] = "monthly_volume_expansion"
    frame.loc[frame["monthly_vol_ratio3_6"] <= 0.75, "monthly_volume_state"] = "monthly_volume_dry"
    frame["effective_month_key"] = (frame["month_key"] + 1).astype(str)
    return frame[
        ["code", "effective_month_key", "monthly_trend_state", "monthly_ret6_state", "monthly_candle_state", "monthly_volume_state"]
    ]


def build_anchor_features(daily: pd.DataFrame, monthly: pd.DataFrame, *, anchor_start_ymd: int) -> pd.DataFrame:
    anchors = build_daily_feature_frame(daily, anchor_start_ymd=anchor_start_ymd)
    weekly_features = build_weekly_feature_frame(daily)
    monthly_features = build_monthly_feature_frame(monthly)
    anchors = anchors.merge(weekly_features, left_on=["code", "week_key"], right_on=["code", "effective_week_key"], how="left")
    anchors = anchors.merge(monthly_features, left_on=["code", "month_key"], right_on=["code", "effective_month_key"], how="left")
    for column in SIGNAL_FEATURE_COLUMNS:
        if column in anchors.columns:
            anchors[column] = anchors[column].fillna(f"{column}_unknown").astype(str)
    anchors["win20"] = anchors["ret20_fwd"] > 0.0
    anchors["win40"] = anchors["ret40_fwd"] > 0.0
    anchors["severe_loss20"] = (anchors["ret20_fwd"] <= SEVERE_LOSS_THRESHOLD) | (anchors["mae20"] <= SEVERE_LOSS_THRESHOLD)
    return anchors


def _profit_factor(ret: pd.Series) -> float | None:
    values = pd.to_numeric(ret, errors="coerce")
    gains = float(values[values > 0.0].sum())
    losses = float(values[values < 0.0].sum())
    if losses == 0.0:
        return None if gains == 0.0 else 999.0
    return float(gains / abs(losses))


def _decision_for(row: dict[str, Any]) -> str:
    if row["trade_count"] < MIN_PATTERN_TRADES or row["symbol_count"] < MIN_PATTERN_SYMBOLS or row["month_count"] < MIN_PATTERN_MONTHS:
        return "insufficient_sample"
    if (
        row["win_rate20"] >= 0.58
        and row["avg_ret20"] >= 0.015
        and row["profit_factor20"] >= 1.35
        and row["severe_loss_rate20"] <= 0.08
        and row["positive_month_rate20"] >= 0.60
    ):
        return "teppan_candidate"
    if row["avg_ret20"] >= 0.03 and row["profit_factor20"] >= 1.45 and row["win_rate20"] >= 0.50:
        return "high_return_candidate"
    if row["win_rate20"] >= 0.56 and row["avg_ret20"] > 0.0 and row["positive_month_rate20"] >= 0.58:
        return "high_win_rate_candidate"
    if row["avg_ret20"] <= 0.0 or row["profit_factor20"] < 1.0:
        return "negative_pattern"
    return "weak_or_mixed"


def evaluate_pattern_families(anchors: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for family_id, columns in PATTERN_FAMILIES:
        grouped = anchors.groupby(list(columns), dropna=False, sort=False)
        monthly_means = (
            anchors.groupby([*columns, "anchor_month"], dropna=False, sort=False)["ret20_fwd"].mean().reset_index(name="month_mean_ret20")
        )
        monthly_stats = monthly_means.groupby(list(columns), dropna=False, sort=False).agg(
            month_count=("anchor_month", "nunique"),
            positive_month_rate20=("month_mean_ret20", lambda s: float((s > 0.0).mean())),
            worst_month_mean_ret20=("month_mean_ret20", "min"),
        )
        for keys, group in grouped:
            if not isinstance(keys, tuple):
                keys = (keys,)
            key_dict = {column: str(value) for column, value in zip(columns, keys)}
            monthly_key = keys if len(columns) > 1 else keys[0]
            try:
                mrow = monthly_stats.loc[monthly_key]
            except KeyError:
                continue
            ret20 = pd.to_numeric(group["ret20_fwd"], errors="coerce")
            ret40 = pd.to_numeric(group["ret40_fwd"], errors="coerce")
            row = {
                "pattern_family": family_id,
                "pattern_key": "|".join(f"{k}={v}" for k, v in key_dict.items()),
                "pattern_features": key_dict,
                "trade_count": int(len(group)),
                "symbol_count": int(group["code"].nunique()),
                "month_count": int(mrow["month_count"]),
                "avg_ret20": _safe_float(ret20.mean()),
                "median_ret20": _safe_float(ret20.median()),
                "win_rate20": _safe_float(group["win20"].astype(float).mean()),
                "profit_factor20": _profit_factor(ret20),
                "avg_ret40": _safe_float(ret40.mean()),
                "win_rate40": _safe_float(group["win40"].astype(float).mean()),
                "avg_mfe20": _safe_float(pd.to_numeric(group["mfe20"], errors="coerce").mean()),
                "avg_mae20": _safe_float(pd.to_numeric(group["mae20"], errors="coerce").mean()),
                "severe_loss_rate20": _safe_float(group["severe_loss20"].astype(float).mean()),
                "positive_month_rate20": _safe_float(mrow["positive_month_rate20"]),
                "worst_month_mean_ret20": _safe_float(mrow["worst_month_mean_ret20"]),
                "entry_convention": "next_session_open_after_anchor_close",
                "no_lookahead_features": True,
            }
            row["pattern_decision"] = _decision_for(row)
            score = (
                (row["avg_ret20"] or 0.0) * 100.0
                + ((row["win_rate20"] or 0.0) - 0.5) * 4.0
                + min(row["profit_factor20"] or 0.0, 3.0) * 0.5
                - (row["severe_loss_rate20"] or 0.0) * 3.0
            )
            row["teppan_score"] = float(score)
            rows.append(row)
    rows = [row for row in rows if row["trade_count"] >= 20]
    decision_order = {"teppan_candidate": 0, "high_return_candidate": 1, "high_win_rate_candidate": 2, "weak_or_mixed": 3, "negative_pattern": 4, "insufficient_sample": 5}
    return sorted(rows, key=lambda row: (decision_order.get(row["pattern_decision"], 9), -(row["teppan_score"]), -(row["trade_count"])))


def build_feature_availability_audit(anchors: pd.DataFrame) -> dict[str, Any]:
    rows = []
    for column in sorted(SIGNAL_FEATURE_COLUMNS):
        present = column in anchors.columns
        non_null = int(anchors[column].notna().sum()) if present else 0
        rows.append({"column": column, "present": present, "non_null_count": non_null, "non_null_rate": None if len(anchors) == 0 else non_null / len(anchors)})
    overlap = sorted(SIGNAL_FEATURE_COLUMNS & LABEL_COLUMNS)
    return {
        "schema_version": f"{SCHEMA_PREFIX}_feature_availability_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "anchor_rows": int(len(anchors)),
        "symbol_count": int(anchors["code"].nunique()),
        "anchor_month_count": int(anchors["anchor_month"].nunique()),
        "feature_rows": rows,
        "signal_feature_columns": sorted(SIGNAL_FEATURE_COLUMNS),
        "label_columns_excluded_from_pattern_keys": sorted(LABEL_COLUMNS),
        "signal_label_overlap": overlap,
        "used_future_labels_in_pattern_keys": bool(overlap),
        "weekly_source": "derived_from_daily_bars_previous_completed_week",
        "monthly_source": "monthly_bars_previous_completed_month",
        "silent_fallback_used": False,
    }


def build_evaluation_contract(*, source_db: Path, anchor_start_ymd: int, max_daily_ymd: int, years: int) -> dict[str, Any]:
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_evaluation_contract_v1",
        "axis_id": AXIS_ID,
        "boundary": "TRADEX-only",
        "research_phase": "multi-factor chart pattern discovery",
        "source_db": str(source_db),
        "anchor_start_ymd": int(anchor_start_ymd),
        "max_daily_ymd": int(max_daily_ymd),
        "requested_years": int(years),
        "entry_convention": "buy next session open after pattern is observable at anchor close",
        "outcomes": ["ret20", "ret40", "mfe20", "mae20", "win_rate20", "severe_loss_rate20"],
        "feature_surfaces": {
            "daily": ["MA5/20/60", "MA slopes", "candle state", "volume ratio", "5-bar candle sequence"],
            "weekly": ["previous completed week trend/candle/volume"],
            "monthly": ["previous completed month trend/candle/volume"],
        },
        "future_label_policy": {
            "future_labels_used_for_pattern_keys": False,
            "future_labels_used_for_evaluation": True,
        },
        "same_condition_controls": {
            "same_universe_source": "runtime snapshot daily_bars PAN source",
            "same_period": True,
            "same_cost_slippage": contracts.TRADEX_DEFAULT_COST_MODEL,
            "artifact_detail_level": contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        },
        "candidate_scoring_created": False,
        "meemee_reflection_allowed": False,
        "publish_bundle_allowed": False,
        "silent_fallback_used": False,
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_research_decision(pattern_rows: list[dict[str, Any]], anchors: pd.DataFrame, output_dir: Path) -> dict[str, Any]:
    teppan = [row for row in pattern_rows if row["pattern_decision"] == "teppan_candidate"]
    high_return = [row for row in pattern_rows if row["pattern_decision"] == "high_return_candidate"]
    high_win = [row for row in pattern_rows if row["pattern_decision"] == "high_win_rate_candidate"]
    if teppan:
        decision = "teppan_patterns_found"
    elif high_return or high_win:
        decision = "promising_patterns_found"
    else:
        decision = "no_teppan_pattern_found"
    return {
        "schema_version": f"{SCHEMA_PREFIX}_research_decision_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "artifact_root": str(output_dir),
        "decision": decision,
        "authoritative_research_decision": decision,
        "sample_summary": {
            "anchor_rows": int(len(anchors)),
            "symbol_count": int(anchors["code"].nunique()),
            "anchor_month_count": int(anchors["anchor_month"].nunique()),
            "pattern_row_count": len(pattern_rows),
        },
        "teppan_count": len(teppan),
        "high_return_count": len(high_return),
        "high_win_rate_count": len(high_win),
        "top_patterns": pattern_rows[:20],
        "decision_reasons": [
            {"code": "teppan_count", "value": len(teppan)},
            {"code": "high_return_count", "value": len(high_return)},
            {"code": "high_win_rate_count", "value": len(high_win)},
            {"code": "future_labels_used_for_pattern_keys", "value": False},
        ],
        "candidate_scoring_created": False,
        "meemee_reflectable": False,
        "publish_bundle_created": False,
        "silent_fallback_used": False,
    }


def _artifact_complete(output_dir: Path, paths: dict[str, str], decision: dict[str, Any]) -> dict[str, Any]:
    existing = {name: Path(path).exists() for name, path in paths.items()}
    required_existing = {name: (output_dir / name).exists() for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json"}
    return {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "artifact_root": str(output_dir),
        "required_artifacts": list(REQUIRED_ARTIFACTS),
        "existing_artifacts": {**existing, **required_existing},
        "complete": all(existing.values()) and all(required_existing.values()),
        "authoritative_research_decision": decision["authoritative_research_decision"],
        "silent_fallback_used": False,
        "candidate_scoring_created": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
    }


def run_teppan_chart_pattern_discovery_v1(
    *,
    source_db: str | Path | None = None,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    run_id: str | None = None,
    years: int = DEFAULT_YEARS,
) -> dict[str, Any]:
    source_path = _resolve_source_db(source_db)
    output_base = _safe_path(output_root, DEFAULT_OUTPUT_ROOT)
    run_name = run_id.strip() if run_id else _default_run_id()
    if not run_name.endswith(AXIS_ID):
        run_name = f"{run_name}-{AXIS_ID}"
    output_dir = output_base / run_name
    output_dir.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(str(source_path), read_only=True)
    try:
        max_daily_ymd = _load_max_daily_ymd(conn)
        max_daily_ts = _ymd_to_timestamp(max_daily_ymd)
        anchor_start_ts = max_daily_ts - pd.DateOffset(years=int(years))
        data_start_ts = anchor_start_ts - pd.DateOffset(days=520)
        anchor_start_ymd = _timestamp_to_ymd(anchor_start_ts)
        data_start_ymd = _timestamp_to_ymd(data_start_ts)
        daily = _load_daily_rows(conn, start_ymd=data_start_ymd, end_ymd=max_daily_ymd)
        monthly = _load_monthly_rows(conn, start_ymd=data_start_ymd, end_ymd=max_daily_ymd)
    finally:
        conn.close()

    anchors = build_anchor_features(daily, monthly, anchor_start_ymd=anchor_start_ymd)
    pattern_rows = evaluate_pattern_families(anchors)
    teppan_candidates = [row for row in pattern_rows if row["pattern_decision"] in {"teppan_candidate", "high_return_candidate", "high_win_rate_candidate"}]
    negative_patterns = [row for row in pattern_rows if row["pattern_decision"] == "negative_pattern"]
    evaluation_contract = build_evaluation_contract(
        source_db=source_path,
        anchor_start_ymd=anchor_start_ymd,
        max_daily_ymd=max_daily_ymd,
        years=years,
    )
    run_manifest = contracts.build_run_manifest(
        session_id=run_name,
        seed=0,
        random_seed=0,
        input_artifacts=[
            {"name": "source_db", "path": str(source_path)},
            {"name": "evaluation_contract", "contract_hash": evaluation_contract["contract_hash"]},
        ],
        asof=str(max_daily_ymd),
        config={
            "axis_id": AXIS_ID,
            "years": int(years),
            "pattern_families": [family for family, _columns in PATTERN_FAMILIES],
            "candidate_scoring_created": False,
        },
        universe=sorted(anchors["code"].astype(str).unique().tolist()),
        period={"start_date": str(anchor_start_ymd), "end_date": str(max_daily_ymd), "label": "daily_anchor_chart_pattern_discovery"},
        horizon="20d_and_40d",
        artifact_detail_level=contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        fallback_status=contracts.TRADEX_FALLBACK_STATUS_AUTHORITATIVE,
        cost_model=contracts.TRADEX_DEFAULT_COST_MODEL,
    )
    contracts.validate_run_manifest(run_manifest)
    feature_audit = build_feature_availability_audit(anchors)
    leaderboard = {
        "schema_version": f"{SCHEMA_PREFIX}_pattern_leaderboard_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "overview": {
            "pattern_row_count": len(pattern_rows),
            "teppan_count": sum(1 for row in pattern_rows if row["pattern_decision"] == "teppan_candidate"),
            "high_return_count": sum(1 for row in pattern_rows if row["pattern_decision"] == "high_return_candidate"),
            "high_win_rate_count": sum(1 for row in pattern_rows if row["pattern_decision"] == "high_win_rate_candidate"),
            "negative_pattern_count": len(negative_patterns),
        },
        "rows": pattern_rows[:300],
    }
    teppan_payload = {
        "schema_version": f"{SCHEMA_PREFIX}_teppan_candidates_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "candidate_count": len(teppan_candidates),
        "candidates": teppan_candidates[:100],
    }
    negative_payload = {
        "schema_version": f"{SCHEMA_PREFIX}_negative_pattern_contrast_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "negative_pattern_count": len(negative_patterns),
        "negative_patterns": sorted(negative_patterns, key=lambda row: (row["avg_ret20"] or 0.0))[:100],
    }
    decision = build_research_decision(pattern_rows, anchors, output_dir)

    paths: dict[str, str] = {}
    for name, payload in {
        "evaluation_contract.json": evaluation_contract,
        "run_manifest.json": run_manifest,
        "feature_availability_audit.json": feature_audit,
        "pattern_leaderboard.json": leaderboard,
        "teppan_candidates.json": teppan_payload,
        "negative_pattern_contrast.json": negative_payload,
        "research_decision.json": decision,
    }.items():
        paths[name] = str(_write_json(output_dir / name, payload))
    paths["pattern_ledger.jsonl"] = str(_write_jsonl(output_dir / "pattern_ledger.jsonl", pattern_rows))
    complete = _artifact_complete(output_dir, paths, decision)
    paths["_ARTIFACT_COMPLETE.json"] = str(_write_json(output_dir / "_ARTIFACT_COMPLETE.json", complete))

    return {
        "output_dir": str(output_dir),
        "paths": paths,
        "authoritative_research_decision": decision["authoritative_research_decision"],
        "sample_summary": decision["sample_summary"],
        "top_patterns": decision["top_patterns"][:10],
        "silent_fallback_used": False,
        "candidate_scoring_created": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-db", default="")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--run-id", default="")
    parser.add_argument("--years", type=int, default=DEFAULT_YEARS)
    args = parser.parse_args(argv)
    result = run_teppan_chart_pattern_discovery_v1(
        source_db=args.source_db.strip() or None,
        output_root=args.output_root,
        run_id=args.run_id.strip() or None,
        years=args.years,
    )
    print(_json_text(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
