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


AXIS_ID = "pre_strength_pattern_mining_v1"
SCHEMA_PREFIX = "tradex_pre_strength_pattern_mining_v1"
DEFAULT_SOURCE_DB = Path(
    r"G:\Tradex\db\meemee_snapshots\20260512T130453Z_winner_lookalike_candle_decomposition_v1\stocks.duckdb"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\pre_strength_pattern_mining_v1")

DEFAULT_YEARS = 10
MIN_HISTORY_DAYS = 160
PRE_WINDOW_DAYS = 20
FORWARD_DAYS = 20
MIN_PATTERN_EVENTS = 120
MIN_PATTERN_SYMBOLS = 25
MIN_PATTERN_MONTHS = 18
SEVERE_LOSS_THRESHOLD = -0.10

SIGNAL_FEATURE_COLUMNS = {
    "pre_ret20_state",
    "pre_ret5_state",
    "pre_ma20_path_state",
    "pre_ma60_context_state",
    "pre_candle_energy_state",
    "pre_wick_warning_state",
    "pre_volume_state",
    "pre_compression_state",
    "weekly_prior_state",
    "monthly_prior_state",
    "event_daily_ret20_state",
    "event_daily_candle_state",
}
LABEL_COLUMNS = {
    "entry_next_open",
    "ret20_fwd",
    "mfe20",
    "mae20",
    "win20",
    "severe_loss20",
}

PATTERN_FAMILIES: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "pre_base_to_strength",
        ("pre_ret20_state", "pre_ret5_state", "pre_ma20_path_state", "weekly_prior_state", "monthly_prior_state"),
    ),
    (
        "pre_reclaim_accumulation",
        ("pre_ma20_path_state", "pre_ma60_context_state", "pre_volume_state", "pre_compression_state", "weekly_prior_state"),
    ),
    (
        "pre_candle_quality",
        ("pre_candle_energy_state", "pre_wick_warning_state", "pre_volume_state", "pre_ret5_state", "weekly_prior_state"),
    ),
    (
        "pre_strength_transition_full",
        (
            "pre_ret20_state",
            "pre_ma20_path_state",
            "pre_candle_energy_state",
            "pre_wick_warning_state",
            "pre_volume_state",
            "weekly_prior_state",
            "monthly_prior_state",
        ),
    ),
    (
        "pre_to_event_confirmation",
        (
            "pre_ma20_path_state",
            "pre_candle_energy_state",
            "pre_wick_warning_state",
            "event_daily_ret20_state",
            "event_daily_candle_state",
        ),
    ),
)

EVENT_LEDGER_COLUMNS = (
    "code",
    "event_date",
    "event_month",
    "pre_strength_key",
    "pre_ret20_state",
    "pre_ret5_state",
    "pre_ma20_path_state",
    "pre_ma60_context_state",
    "pre_candle_energy_state",
    "pre_wick_warning_state",
    "pre_volume_state",
    "pre_compression_state",
    "weekly_prior_state",
    "monthly_prior_state",
    "event_daily_ret20_state",
    "event_daily_candle_state",
    "event_strength_score",
    "ret20_fwd",
    "mfe20",
    "mae20",
    "win20",
    "severe_loss20",
)

REQUIRED_ARTIFACTS = (
    "evaluation_contract.json",
    "run_manifest.json",
    "feature_availability_audit.json",
    "pre_strength_event_ledger.jsonl",
    "pattern_leaderboard.json",
    "pre_strength_patterns.json",
    "false_strength_patterns.json",
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


def _safe_div(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    denom = pd.to_numeric(denominator, errors="coerce").astype(float)
    denom = denom.mask(denom == 0.0)
    return pd.to_numeric(numerator, errors="coerce").astype(float).div(denom).replace([math.inf, -math.inf], pd.NA)


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


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


def _ymd_to_timestamp(value: int) -> pd.Timestamp:
    return pd.to_datetime(str(int(value)), format="%Y%m%d")


def _timestamp_to_ymd(value: pd.Timestamp) -> int:
    return int(value.strftime("%Y%m%d"))


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


def _load_max_daily_ymd(conn: duckdb.DuckDBPyConnection) -> int:
    expr = _date_norm_expr("date")
    row = conn.execute(f"SELECT MAX({expr}) FROM daily_bars WHERE lower(coalesce(source, '')) = 'pan'").fetchone()
    if not row or row[0] is None:
        row = conn.execute(f"SELECT MAX({expr}) FROM daily_bars").fetchone()
    if not row or row[0] is None:
        raise RuntimeError("daily_bars has no resolvable max date")
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
    numeric = pd.to_numeric(series, errors="coerce")
    out = pd.Series(f"{prefix}_flat", index=series.index, dtype="object")
    out[numeric <= strong_down] = f"{prefix}_strong_down"
    out[(numeric > strong_down) & (numeric <= down)] = f"{prefix}_down"
    out[(numeric >= up) & (numeric < strong_up)] = f"{prefix}_up"
    out[numeric >= strong_up] = f"{prefix}_strong_up"
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


def _forward_window(series: pd.Series, *, days: int, op: str) -> pd.Series:
    shifted = series.shift(-1)
    rev = shifted.iloc[::-1]
    if op == "max":
        out = rev.rolling(days, min_periods=days).max().iloc[::-1]
    elif op == "min":
        out = rev.rolling(days, min_periods=days).min().iloc[::-1]
    else:
        raise ValueError(f"unsupported op: {op}")
    return out.reindex(series.index)


def _rolling_prior_count(values: pd.Series, keys: pd.Series, *, days: int) -> pd.Series:
    return values.astype(float).groupby(keys, sort=False).transform(lambda s: s.shift(1).rolling(days, min_periods=days).sum())


def _rolling_prior_max(values: pd.Series, keys: pd.Series, *, days: int) -> pd.Series:
    return values.astype(float).groupby(keys, sort=False).transform(lambda s: s.shift(1).rolling(days, min_periods=1).max())


def _build_daily_event_features(daily: pd.DataFrame, *, anchor_start_ymd: int, min_history_days: int) -> pd.DataFrame:
    frame = daily.sort_values(["code", "date"], kind="stable").copy()
    grouped = frame.groupby("code", sort=False)
    frame["history_days"] = grouped.cumcount() + 1
    frame["ma5"] = grouped["c"].transform(lambda s: s.rolling(5, min_periods=5).mean())
    if "ma20" not in frame.columns or frame["ma20"].isna().all():
        frame["ma20"] = grouped["c"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    if "ma60" not in frame.columns or frame["ma60"].isna().all():
        frame["ma60"] = grouped["c"].transform(lambda s: s.rolling(60, min_periods=60).mean())

    frame["prev_c"] = grouped["c"].shift(1)
    frame["ret20"] = grouped["c"].transform(lambda s: s / s.shift(20) - 1.0)
    frame["ma60_slope_20d"] = grouped["ma60"].transform(lambda s: s / s.shift(20) - 1.0)
    frame["daily_candle_state"] = _candle_state(frame["o"], frame["h"], frame["l"], frame["c"], prefix="daily")
    vol5 = grouped["v"].transform(lambda s: s.rolling(5, min_periods=5).mean())
    vol20 = grouped["v"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    frame["vol_ratio5_20"] = _safe_div(vol5, vol20)
    frame["daily_ret20_state"] = _bucket_return(frame["ret20"], strong_down=-0.08, down=-0.03, up=0.03, strong_up=0.08, prefix="daily20")

    frame["daily_ma_stack"] = "daily_stack_mixed"
    frame.loc[(frame["ma5"] > frame["ma20"]) & (frame["ma20"] > frame["ma60"]), "daily_ma_stack"] = "daily_bull_stack_5_20_60"
    bull_candle = frame["daily_candle_state"].isin({"daily_strong_bull", "daily_lower_wick_bull"})
    frame["event_strength_score"] = 0
    frame.loc[frame["daily_ma_stack"].eq("daily_bull_stack_5_20_60"), "event_strength_score"] += 2
    frame.loc[frame["ret20"].ge(0.03), "event_strength_score"] += 1
    frame.loc[frame["ret20"].ge(0.08), "event_strength_score"] += 1
    frame.loc[frame["ma60_slope_20d"].ge(0.01), "event_strength_score"] += 1
    frame.loc[bull_candle, "event_strength_score"] += 1
    frame.loc[frame["vol_ratio5_20"].ge(1.35), "event_strength_score"] += 1
    frame["strong_looking"] = frame["daily_ma_stack"].eq("daily_bull_stack_5_20_60") & frame["event_strength_score"].ge(4)
    frame["prev_strong_10"] = _rolling_prior_max(frame["strong_looking"], frame["code"], days=10).fillna(0.0)
    frame["is_new_strength_event"] = frame["strong_looking"] & frame["prev_strong_10"].le(0.0)

    candle_range = (frame["h"] - frame["l"]).replace(0, pd.NA)
    body_ratio = pd.to_numeric((frame["c"] - frame["o"]).abs() / candle_range, errors="coerce").fillna(0.0)
    close_position = pd.to_numeric((frame["c"] - frame["l"]) / candle_range, errors="coerce").fillna(0.5)
    upper_wick = pd.to_numeric((frame["h"] - frame[["o", "c"]].max(axis=1)) / candle_range, errors="coerce").fillna(0.0)
    lower_wick = pd.to_numeric((frame[["o", "c"]].min(axis=1) - frame["l"]) / candle_range, errors="coerce").fillna(0.0)
    strong_bull = (frame["c"] > frame["o"]) & (body_ratio >= 0.35) & (close_position >= 0.65)
    weak_bear = (frame["c"] < frame["o"]) & ((close_position <= 0.35) | (upper_wick >= 0.45))
    upper_warning = upper_wick >= 0.45
    lower_support = lower_wick >= 0.45
    ma20_below = frame["c"] < frame["ma20"]
    ma20_reclaim = (frame["c"] > frame["ma20"]) & (grouped["c"].shift(1) <= grouped["ma20"].shift(1))
    strong_followed_by_weak = weak_bear & strong_bull.groupby(frame["code"], sort=False).shift(1, fill_value=False).astype(bool)

    frame["pre_strong_bull_count_20"] = _rolling_prior_count(strong_bull, frame["code"], days=PRE_WINDOW_DAYS)
    frame["pre_weak_bear_count_20"] = _rolling_prior_count(weak_bear, frame["code"], days=PRE_WINDOW_DAYS)
    frame["pre_upper_wick_count_20"] = _rolling_prior_count(upper_warning, frame["code"], days=PRE_WINDOW_DAYS)
    frame["pre_lower_wick_count_20"] = _rolling_prior_count(lower_support, frame["code"], days=PRE_WINDOW_DAYS)
    frame["pre_ma20_below_count_20"] = _rolling_prior_count(ma20_below, frame["code"], days=PRE_WINDOW_DAYS)
    frame["pre_ma20_reclaim_count_20"] = _rolling_prior_count(ma20_reclaim, frame["code"], days=PRE_WINDOW_DAYS)
    frame["pre_strong_followed_by_weak_count_20"] = _rolling_prior_count(strong_followed_by_weak, frame["code"], days=PRE_WINDOW_DAYS)

    prior_close = grouped["c"].shift(1)
    frame["pre_ret20"] = prior_close / grouped["c"].shift(PRE_WINDOW_DAYS + 1) - 1.0
    frame["pre_ret5"] = prior_close / grouped["c"].shift(6) - 1.0
    prior_high20 = grouped["h"].transform(lambda s: s.shift(1).rolling(PRE_WINDOW_DAYS, min_periods=PRE_WINDOW_DAYS).max())
    prior_low20 = grouped["l"].transform(lambda s: s.shift(1).rolling(PRE_WINDOW_DAYS, min_periods=PRE_WINDOW_DAYS).min())
    frame["pre_range20"] = _safe_div(prior_high20 - prior_low20, prior_close)
    frame["pre_vol_ratio5_20"] = grouped["vol_ratio5_20"].shift(1)
    frame["pre_ma20_dist"] = _safe_div(prior_close - grouped["ma20"].shift(1), grouped["ma20"].shift(1))
    frame["pre_ma60_dist"] = _safe_div(prior_close - grouped["ma60"].shift(1), grouped["ma60"].shift(1))

    frame["pre_ret20_state"] = _bucket_return(frame["pre_ret20"], strong_down=-0.10, down=-0.04, up=0.04, strong_up=0.10, prefix="pre20")
    frame["pre_ret5_state"] = _bucket_return(frame["pre_ret5"], strong_down=-0.05, down=-0.02, up=0.02, strong_up=0.06, prefix="pre5")

    frame["pre_ma20_path_state"] = "pre_ma20_near"
    frame.loc[(frame["pre_ma20_dist"] < -0.02) & (frame["pre_ma20_below_count_20"] >= 10), "pre_ma20_path_state"] = "pre_ma20_below_base"
    frame.loc[(frame["pre_ma20_dist"].between(-0.02, 0.04)) & (frame["pre_ma20_reclaim_count_20"] >= 1), "pre_ma20_path_state"] = "pre_ma20_reclaim_base"
    frame.loc[frame["pre_ma20_dist"] >= 0.08, "pre_ma20_path_state"] = "pre_ma20_already_extended"
    frame["pre_ma60_context_state"] = "pre_ma60_near_or_above"
    frame.loc[frame["pre_ma60_dist"] <= -0.04, "pre_ma60_context_state"] = "pre_ma60_below"
    frame.loc[frame["pre_ma60_dist"] >= 0.10, "pre_ma60_context_state"] = "pre_ma60_extended_above"

    candle_energy = frame["pre_strong_bull_count_20"].fillna(0.0) - frame["pre_weak_bear_count_20"].fillna(0.0)
    frame["pre_candle_energy_state"] = "pre_candle_energy_mixed"
    frame.loc[candle_energy >= 4, "pre_candle_energy_state"] = "pre_candle_energy_positive"
    frame.loc[candle_energy <= -2, "pre_candle_energy_state"] = "pre_candle_energy_warning"
    frame["pre_wick_warning_state"] = "pre_wicks_clean"
    frame.loc[(frame["pre_upper_wick_count_20"] >= 4) | (frame["pre_strong_followed_by_weak_count_20"] >= 2), "pre_wick_warning_state"] = "pre_upper_wick_or_failed_push"
    frame.loc[(frame["pre_lower_wick_count_20"] >= 4) & (frame["pre_upper_wick_count_20"] < 4), "pre_wick_warning_state"] = "pre_lower_wick_support"
    frame["pre_volume_state"] = "pre_volume_normal"
    frame.loc[frame["pre_vol_ratio5_20"] >= 1.45, "pre_volume_state"] = "pre_volume_expansion"
    frame.loc[frame["pre_vol_ratio5_20"] <= 0.75, "pre_volume_state"] = "pre_volume_dry"
    frame["pre_compression_state"] = "pre_range_normal"
    frame.loc[frame["pre_range20"] <= 0.12, "pre_compression_state"] = "pre_range_compressed"
    frame.loc[frame["pre_range20"] >= 0.28, "pre_compression_state"] = "pre_range_wide"

    frame["event_daily_ret20_state"] = frame["daily_ret20_state"].astype(str)
    frame["event_daily_candle_state"] = frame["daily_candle_state"].astype(str)
    frame["entry_next_open"] = grouped["o"].shift(-1)
    frame["future_close_20"] = grouped["c"].shift(-FORWARD_DAYS)
    frame["future_high_20"] = grouped["h"].transform(lambda s: _forward_window(s, days=FORWARD_DAYS, op="max"))
    frame["future_low_20"] = grouped["l"].transform(lambda s: _forward_window(s, days=FORWARD_DAYS, op="min"))
    frame["ret20_fwd"] = _safe_div(frame["future_close_20"] - frame["entry_next_open"], frame["entry_next_open"])
    frame["mfe20"] = _safe_div(frame["future_high_20"] - frame["entry_next_open"], frame["entry_next_open"])
    frame["mae20"] = _safe_div(frame["future_low_20"] - frame["entry_next_open"], frame["entry_next_open"])
    frame["week_key"] = frame["date"].dt.to_period("W-FRI").astype(str)
    frame["month_key"] = frame["date"].dt.to_period("M").astype(str)
    frame["event_month"] = frame["date"].dt.to_period("M").astype(str)
    frame["event_date"] = frame["date"].dt.strftime("%Y-%m-%d")

    eligible = frame[
        (frame["ymd"] >= int(anchor_start_ymd))
        & (frame["history_days"] >= int(min_history_days))
        & frame["is_new_strength_event"]
        & frame["entry_next_open"].notna()
        & frame["ret20_fwd"].notna()
        & frame["mfe20"].notna()
        & frame["mae20"].notna()
        & frame["pre_ret20"].notna()
        & frame["pre_range20"].notna()
    ].copy()
    if eligible.empty:
        raise RuntimeError("no eligible pre-strength events after filters")
    return eligible


def _build_weekly_prior_features(daily: pd.DataFrame) -> pd.DataFrame:
    work = daily.sort_values(["code", "date"], kind="stable").copy()
    work["week_period"] = work["date"].dt.to_period("W-FRI")
    weekly = (
        work.groupby(["code", "week_period"], sort=True)
        .agg(o=("o", "first"), h=("h", "max"), l=("l", "min"), c=("c", "last"), v=("v", "sum"))
        .reset_index()
    )
    grouped = weekly.groupby("code", sort=False)
    weekly["weekly_ret4"] = grouped["c"].transform(lambda s: s / s.shift(4) - 1.0)
    weekly["weekly_ma4"] = grouped["c"].transform(lambda s: s.rolling(4, min_periods=4).mean())
    weekly["weekly_ma13"] = grouped["c"].transform(lambda s: s.rolling(13, min_periods=13).mean())
    weekly["weekly_prior_state"] = "weekly_prior_mixed"
    weekly.loc[(weekly["c"] > weekly["weekly_ma4"]) & (weekly["weekly_ma4"] > weekly["weekly_ma13"]), "weekly_prior_state"] = "weekly_prior_uptrend"
    weekly.loc[(weekly["c"] > weekly["weekly_ma13"]) & (weekly["weekly_ma4"] <= weekly["weekly_ma13"]), "weekly_prior_state"] = "weekly_prior_recovery"
    weekly.loc[(weekly["c"] < weekly["weekly_ma4"]) & (weekly["weekly_ma4"] < weekly["weekly_ma13"]), "weekly_prior_state"] = "weekly_prior_downtrend"
    weekly.loc[weekly["weekly_ret4"] >= 0.10, "weekly_prior_state"] = "weekly_prior_strong_up"
    weekly["effective_week_key"] = (weekly["week_period"] + 1).astype(str)
    return weekly[["code", "effective_week_key", "weekly_prior_state"]]


def _build_monthly_prior_features(monthly: pd.DataFrame) -> pd.DataFrame:
    frame = monthly.sort_values(["code", "month_key"], kind="stable").copy()
    grouped = frame.groupby("code", sort=False)
    frame["monthly_ret3"] = grouped["c"].transform(lambda s: s / s.shift(3) - 1.0)
    frame["monthly_ret6"] = grouped["c"].transform(lambda s: s / s.shift(6) - 1.0)
    frame["monthly_prior_state"] = "monthly_prior_mixed"
    frame.loc[(frame["c"] > frame["ma20"]) & (frame["ma20"] > frame["ma60"]), "monthly_prior_state"] = "monthly_prior_uptrend"
    frame.loc[(frame["c"] > frame["ma20"]) & (frame["ma20"] <= frame["ma60"]), "monthly_prior_state"] = "monthly_prior_recovery"
    frame.loc[(frame["c"] < frame["ma20"]) & (frame["ma20"] < frame["ma60"]), "monthly_prior_state"] = "monthly_prior_downtrend"
    frame.loc[frame["monthly_ret6"] >= 0.18, "monthly_prior_state"] = "monthly_prior_strong_up"
    frame.loc[frame["monthly_ret6"] <= -0.12, "monthly_prior_state"] = "monthly_prior_down_or_drawdown"
    frame["effective_month_key"] = (frame["month_key"] + 1).astype(str)
    return frame[["code", "effective_month_key", "monthly_prior_state"]]


def build_pre_strength_events(daily: pd.DataFrame, monthly: pd.DataFrame, *, anchor_start_ymd: int, min_history_days: int) -> pd.DataFrame:
    events = _build_daily_event_features(daily, anchor_start_ymd=anchor_start_ymd, min_history_days=min_history_days)
    weekly = _build_weekly_prior_features(daily)
    monthly_prior = _build_monthly_prior_features(monthly)
    events = events.merge(weekly, left_on=["code", "week_key"], right_on=["code", "effective_week_key"], how="left")
    events = events.merge(monthly_prior, left_on=["code", "month_key"], right_on=["code", "effective_month_key"], how="left")
    events["weekly_prior_state"] = events["weekly_prior_state"].fillna("weekly_prior_unknown").astype(str)
    events["monthly_prior_state"] = events["monthly_prior_state"].fillna("monthly_prior_unknown").astype(str)
    events["win20"] = events["ret20_fwd"] > 0.0
    events["severe_loss20"] = (events["ret20_fwd"] <= SEVERE_LOSS_THRESHOLD) | (events["mae20"] <= SEVERE_LOSS_THRESHOLD)
    events["pre_strength_key"] = events[
        [
            "pre_ret20_state",
            "pre_ma20_path_state",
            "pre_candle_energy_state",
            "pre_wick_warning_state",
            "pre_volume_state",
            "weekly_prior_state",
            "monthly_prior_state",
        ]
    ].astype(str).agg("|".join, axis=1)
    return events


def _profit_factor(ret: pd.Series) -> float | None:
    values = pd.to_numeric(ret, errors="coerce")
    gains = float(values[values > 0.0].sum())
    losses = float(values[values < 0.0].sum())
    if losses == 0.0:
        return None if gains == 0.0 else 999.0
    return float(gains / abs(losses))


def _pattern_decision_for(row: dict[str, Any]) -> str:
    if row["event_count"] < MIN_PATTERN_EVENTS or row["symbol_count"] < MIN_PATTERN_SYMBOLS or row["month_count"] < MIN_PATTERN_MONTHS:
        return "insufficient_sample"
    if (
        row["win_rate20"] >= 0.60
        and row["avg_ret20"] >= 0.018
        and row["profit_factor20"] >= 1.40
        and row["severe_loss_rate20"] <= 0.08
        and row["positive_month_rate20"] >= 0.62
    ):
        return "pre_strength_teppan_pattern"
    if row["avg_ret20"] >= 0.03 and row["profit_factor20"] >= 1.45 and row["win_rate20"] >= 0.50:
        return "high_return_pre_strength_pattern"
    if row["win_rate20"] >= 0.57 and row["avg_ret20"] > 0.0 and row["positive_month_rate20"] >= 0.58:
        return "high_win_pre_strength_pattern"
    if row["avg_ret20"] <= 0.0 or row["profit_factor20"] < 1.0 or row["severe_loss_rate20"] >= 0.12:
        return "false_strength_pattern"
    return "weak_or_mixed"


def evaluate_pre_strength_patterns(events: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for family_id, columns in PATTERN_FAMILIES:
        grouped = events.groupby(list(columns), dropna=False, sort=False)
        monthly_means = (
            events.groupby([*columns, "event_month"], dropna=False, sort=False)["ret20_fwd"].mean().reset_index(name="month_mean_ret20")
        )
        monthly_stats = monthly_means.groupby(list(columns), dropna=False, sort=False).agg(
            month_count=("event_month", "nunique"),
            positive_month_rate20=("month_mean_ret20", lambda s: float((s > 0.0).mean())),
        )
        for key, group in grouped:
            if not isinstance(key, tuple):
                key = (key,)
            key_payload = {column: str(value) for column, value in zip(columns, key, strict=False)}
            month_row = monthly_stats.loc[key]
            row = {
                "family_id": family_id,
                "pattern_key": "|".join(f"{column}={value}" for column, value in key_payload.items()),
                "pattern_features": key_payload,
                "event_count": int(len(group)),
                "symbol_count": int(group["code"].nunique()),
                "month_count": int(month_row["month_count"]),
                "win_rate20": float(group["win20"].mean()),
                "avg_ret20": float(pd.to_numeric(group["ret20_fwd"], errors="coerce").mean()),
                "median_ret20": float(pd.to_numeric(group["ret20_fwd"], errors="coerce").median()),
                "avg_mfe20": float(pd.to_numeric(group["mfe20"], errors="coerce").mean()),
                "avg_mae20": float(pd.to_numeric(group["mae20"], errors="coerce").mean()),
                "profit_factor20": _profit_factor(group["ret20_fwd"]),
                "severe_loss_rate20": float(group["severe_loss20"].mean()),
                "positive_month_rate20": float(month_row["positive_month_rate20"]),
                "example_symbols": sorted(group["code"].astype(str).unique().tolist())[:12],
            }
            row["pattern_decision"] = _pattern_decision_for(row)
            row["pattern_score"] = (
                (row["avg_ret20"] or 0.0) * 100.0
                + ((row["win_rate20"] or 0.0) - 0.50) * 5.0
                + min(float(row["profit_factor20"] or 0.0), 5.0) * 0.35
                + (row["positive_month_rate20"] or 0.0)
                - (row["severe_loss_rate20"] or 0.0) * 3.0
            )
            rows.append(row)
    decision_order = {
        "pre_strength_teppan_pattern": 0,
        "high_return_pre_strength_pattern": 1,
        "high_win_pre_strength_pattern": 2,
        "weak_or_mixed": 3,
        "false_strength_pattern": 4,
        "insufficient_sample": 5,
    }
    return sorted(rows, key=lambda row: (decision_order.get(row["pattern_decision"], 9), -(row["pattern_score"]), -(row["event_count"])))


def build_feature_availability_audit(events: pd.DataFrame) -> dict[str, Any]:
    rows = []
    for column in sorted(SIGNAL_FEATURE_COLUMNS):
        present = column in events.columns
        rows.append(
            {
                "column": column,
                "present": present,
                "non_null_rate": float(events[column].notna().mean()) if present and len(events) else 0.0,
                "source_timing": "point_in_time_pre_event_or_event_close",
            }
        )
    key_columns = {column for _family, columns in PATTERN_FAMILIES for column in columns}
    return {
        "schema_version": f"{SCHEMA_PREFIX}_feature_availability_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "silent_fallback_used": False,
        "used_future_labels_in_pattern_keys": bool(key_columns.intersection(LABEL_COLUMNS)),
        "label_columns": sorted(LABEL_COLUMNS),
        "pattern_key_columns": sorted(key_columns),
        "feature_rows": rows,
    }


def build_evaluation_contract(*, source_db: Path, anchor_start_ymd: int, max_daily_ymd: int, years: int) -> dict[str, Any]:
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_evaluation_contract_v1",
        "axis_id": AXIS_ID,
        "boundary": "TRADEX-only",
        "research_phase": "pre-strength chart pattern mining",
        "source_db": str(source_db),
        "anchor_start_ymd": int(anchor_start_ymd),
        "max_daily_ymd": int(max_daily_ymd),
        "requested_years": int(years),
        "event_definition": {
            "name": "new_strong_looking_daily_event",
            "strong_state": "daily bull MA5>MA20>MA60 plus fixed strength score >= 4",
            "new_event_filter": "no strong-looking state in previous 10 sessions",
        },
        "pre_event_window": f"{PRE_WINDOW_DAYS} trading days ending before the event day",
        "entry_convention_for_evaluation": "buy next session open after the strong-looking event is observable",
        "future_label_policy": {
            "future_labels_used_for_pattern_keys": False,
            "future_labels_used_for_evaluation": True,
            "event_oracle_used_for_discovery": True,
            "candidate_scoring_created": False,
        },
        "feature_surfaces": {
            "daily": ["pre-event returns", "MA20/MA60 distance", "MA20 reclaim/below counts", "candle sequence", "volume", "compression"],
            "weekly": ["previous completed week state"],
            "monthly": ["previous completed month state"],
        },
        "same_condition_controls": {
            "same_universe_source": "runtime snapshot daily_bars PAN source",
            "same_period": True,
            "same_cost_slippage": contracts.TRADEX_DEFAULT_COST_MODEL,
            "artifact_detail_level": contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        },
        "meemee_reflection_allowed": False,
        "publish_bundle_allowed": False,
        "silent_fallback_used": False,
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_research_decision(pattern_rows: list[dict[str, Any]], events: pd.DataFrame, output_dir: Path) -> dict[str, Any]:
    teppan = [row for row in pattern_rows if row["pattern_decision"] == "pre_strength_teppan_pattern"]
    high_return = [row for row in pattern_rows if row["pattern_decision"] == "high_return_pre_strength_pattern"]
    high_win = [row for row in pattern_rows if row["pattern_decision"] == "high_win_pre_strength_pattern"]
    false_patterns = [row for row in pattern_rows if row["pattern_decision"] == "false_strength_pattern"]
    if teppan:
        decision = "pre_strength_teppan_patterns_found"
    elif high_return or high_win:
        decision = "promising_pre_strength_patterns_found"
    else:
        decision = "no_stable_pre_strength_pattern_found"
    reasons = [
        {
            "code": "event_sample_count",
            "status": "pass" if len(events) >= MIN_PATTERN_EVENTS else "fail",
            "value": int(len(events)),
        },
        {
            "code": "pre_strength_teppan_pattern_count",
            "status": "pass" if teppan else "fail",
            "value": int(len(teppan)),
        },
        {
            "code": "promising_pattern_count",
            "status": "pass" if high_return or high_win else "fail",
            "value": int(len(high_return) + len(high_win)),
        },
        {
            "code": "future_label_is_evaluation_only",
            "status": "pass",
            "value": True,
        },
    ]
    return {
        "schema_version": f"{SCHEMA_PREFIX}_research_decision_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "artifact_root": str(output_dir),
        "decision": decision,
        "authoritative_research_decision": decision,
        "decision_reasons": reasons,
        "sample_summary": {
            "event_count": int(len(events)),
            "symbol_count": int(events["code"].nunique()),
            "month_count": int(events["event_month"].nunique()),
            "win_rate20": float(events["win20"].mean()),
            "avg_ret20": float(pd.to_numeric(events["ret20_fwd"], errors="coerce").mean()),
            "avg_mfe20": float(pd.to_numeric(events["mfe20"], errors="coerce").mean()),
            "avg_mae20": float(pd.to_numeric(events["mae20"], errors="coerce").mean()),
            "severe_loss_rate20": float(events["severe_loss20"].mean()),
        },
        "pattern_counts": {
            "pre_strength_teppan_pattern": len(teppan),
            "high_return_pre_strength_pattern": len(high_return),
            "high_win_pre_strength_pattern": len(high_win),
            "false_strength_pattern": len(false_patterns),
        },
        "top_patterns": pattern_rows[:10],
        "top_false_strength_patterns": false_patterns[:10],
        "candidate_scoring_created": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
        "silent_fallback_used": False,
        "label_usage": "forward labels used only for post-event evaluation; pattern keys use pre-event/current observable features only",
    }


def _artifact_complete(output_dir: Path, paths: dict[str, str], decision: dict[str, Any]) -> dict[str, Any]:
    required_existing = {name: (output_dir / name).exists() for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json"}
    return {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "artifact_root": str(output_dir),
        "complete": all(required_existing.values()),
        "required_artifacts": required_existing,
        "paths": paths,
        "authoritative_research_decision": decision["authoritative_research_decision"],
        "silent_fallback_used": False,
        "candidate_scoring_created": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
    }


def _event_ledger_rows(events: pd.DataFrame) -> list[dict[str, Any]]:
    available = [column for column in EVENT_LEDGER_COLUMNS if column in events.columns]
    return events[available].sort_values(["event_date", "code"], kind="stable").to_dict(orient="records")


def run_pre_strength_pattern_mining_v1(
    *,
    source_db: str | Path | None = None,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    run_id: str | None = None,
    years: int = DEFAULT_YEARS,
    min_history_days: int = MIN_HISTORY_DAYS,
) -> dict[str, Any]:
    source_path = _resolve_source_db(source_db)
    run_name = run_id.strip() if isinstance(run_id, str) and run_id.strip() else _default_run_id()
    output_dir = _safe_path(output_root, DEFAULT_OUTPUT_ROOT) / run_name
    with duckdb.connect(str(source_path), read_only=True) as conn:
        max_daily_ymd = _load_max_daily_ymd(conn)
        max_daily_ts = _ymd_to_timestamp(max_daily_ymd)
        anchor_start_ts = max_daily_ts - pd.Timedelta(days=int(years * 365.25))
        load_start_ts = anchor_start_ts - pd.Timedelta(days=420)
        anchor_start_ymd = _timestamp_to_ymd(anchor_start_ts)
        load_start_ymd = _timestamp_to_ymd(load_start_ts)
        daily = _load_daily_rows(conn, start_ymd=load_start_ymd, end_ymd=max_daily_ymd)
        monthly = _load_monthly_rows(conn, start_ymd=load_start_ymd, end_ymd=max_daily_ymd)

    events = build_pre_strength_events(daily, monthly, anchor_start_ymd=anchor_start_ymd, min_history_days=min_history_days)
    pattern_rows = evaluate_pre_strength_patterns(events)
    positive_patterns = [
        row
        for row in pattern_rows
        if row["pattern_decision"] in {"pre_strength_teppan_pattern", "high_return_pre_strength_pattern", "high_win_pre_strength_pattern"}
    ]
    false_patterns = [row for row in pattern_rows if row["pattern_decision"] == "false_strength_pattern"]
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
            "min_history_days": int(min_history_days),
            "pre_window_days": PRE_WINDOW_DAYS,
            "pattern_families": [family for family, _columns in PATTERN_FAMILIES],
            "candidate_scoring_created": False,
        },
        universe=sorted(events["code"].astype(str).unique().tolist()),
        period={"start_date": str(anchor_start_ymd), "end_date": str(max_daily_ymd), "label": "new_strength_event_pre_pattern_mining"},
        horizon=f"{FORWARD_DAYS}d",
        artifact_detail_level=contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        fallback_status=contracts.TRADEX_FALLBACK_STATUS_AUTHORITATIVE,
        cost_model=contracts.TRADEX_DEFAULT_COST_MODEL,
    )
    contracts.validate_run_manifest(run_manifest)
    feature_audit = build_feature_availability_audit(events)
    pattern_leaderboard = {
        "schema_version": f"{SCHEMA_PREFIX}_pattern_leaderboard_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "overview": {
            "pattern_row_count": len(pattern_rows),
            "positive_pattern_count": len(positive_patterns),
            "false_strength_pattern_count": len(false_patterns),
            "event_count": int(len(events)),
            "symbol_count": int(events["code"].nunique()),
            "month_count": int(events["event_month"].nunique()),
        },
        "rows": pattern_rows[:400],
    }
    pre_strength_patterns = {
        "schema_version": f"{SCHEMA_PREFIX}_pre_strength_patterns_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "pattern_count": len(positive_patterns),
        "patterns": positive_patterns[:120],
    }
    false_strength_payload = {
        "schema_version": f"{SCHEMA_PREFIX}_false_strength_patterns_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "pattern_count": len(false_patterns),
        "patterns": false_patterns[:120],
    }
    decision = build_research_decision(pattern_rows, events, output_dir)

    paths: dict[str, str] = {}
    for name, payload in {
        "evaluation_contract.json": evaluation_contract,
        "run_manifest.json": run_manifest,
        "feature_availability_audit.json": feature_audit,
        "pattern_leaderboard.json": pattern_leaderboard,
        "pre_strength_patterns.json": pre_strength_patterns,
        "false_strength_patterns.json": false_strength_payload,
        "research_decision.json": decision,
    }.items():
        paths[name] = str(_write_json(output_dir / name, payload))
    paths["pre_strength_event_ledger.jsonl"] = str(_write_jsonl(output_dir / "pre_strength_event_ledger.jsonl", _event_ledger_rows(events)))
    complete = _artifact_complete(output_dir, paths, decision)
    paths["_ARTIFACT_COMPLETE.json"] = str(_write_json(output_dir / "_ARTIFACT_COMPLETE.json", complete))
    return {
        "output_dir": str(output_dir),
        "authoritative_research_decision": decision["authoritative_research_decision"],
        "sample_summary": decision["sample_summary"],
        "top_patterns": decision["top_patterns"][:8],
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
    parser.add_argument("--min-history-days", type=int, default=MIN_HISTORY_DAYS)
    args = parser.parse_args(argv)
    result = run_pre_strength_pattern_mining_v1(
        source_db=args.source_db.strip() or None,
        output_root=args.output_root,
        run_id=args.run_id.strip() or None,
        years=args.years,
        min_history_days=args.min_history_days,
    )
    print(_json_text(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
