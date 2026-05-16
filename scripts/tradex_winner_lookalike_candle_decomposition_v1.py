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


AXIS_ID = "winner_lookalike_candle_decomposition_v1"
SCHEMA_PREFIX = "tradex_winner_lookalike_candle_decomposition_v1"
DEFAULT_SOURCE_DB = Path(
    r"G:\Tradex\db\meemee_snapshots\20260512T130453Z_winner_lookalike_candle_decomposition_v1\stocks.duckdb"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\winner_lookalike_candle_decomposition_v1")

FORWARD_DAYS = 20
DEFAULT_YEARS = 10
DEFAULT_MIN_HISTORY_DAYS = 120
DEFAULT_MAX_PAIR_ROWS = 1000
WINNER_FLOOR_RETURN = 0.05
FAILURE_CEILING_RETURN = -0.03
FALSE_LOOKALIKE_RETURN_MAX = 0.0
SEVERE_MAE_THRESHOLD = -0.10

LABEL_COLUMNS = {
    "future_close_20",
    "future_high_20",
    "future_low_20",
    "forward_ret_20d",
    "mfe_20d",
    "mae_20d",
    "month_ret20_p20",
    "month_ret20_p80",
    "is_winner",
    "is_failure",
    "is_false_lookalike",
}

COARSE_LOOKALIKE_FEATURES = (
    "monthly_state",
    "daily_trend_state",
    "daily_ma_state",
    "daily_candle_energy_state",
)

DAILY_SEQUENCE_FEATURES = (
    "daily20_return",
    "daily5_return",
    "latest_body_ratio",
    "latest_upper_wick_ratio",
    "latest_lower_wick_ratio",
    "latest_close_position",
    "latest_gap_pct",
    "latest_ma20_dist",
    "latest_ma60_dist",
    "latest_vol_ratio_5_20",
    "strong_bull_count_20",
    "weak_bear_count_20",
    "upper_wick_count_20",
    "lower_wick_count_20",
    "ma20_below_count_20",
    "ma20_reclaim_count_20",
    "strong_followed_by_weak_count_20",
    "weak_negated_by_strong_count_20",
)

MONTHLY_DAILY_FEATURES = (
    "monthly_ret_1m",
    "monthly_ret_3m",
    "monthly_ret_6m",
    "monthly_body_ratio",
    "monthly_upper_wick_ratio",
    "monthly_lower_wick_ratio",
    "monthly_close_position",
    "monthly_ma20_dist",
    "monthly_ma60_dist",
    "monthly_up_count_6",
    "monthly_upper_wick_count_6",
    "monthly_close_above_ma20_count_6",
    "monthly_drawdown_6m",
    "monthly_range_6m",
)

DISCOVERY_FEATURES = DAILY_SEQUENCE_FEATURES + MONTHLY_DAILY_FEATURES

REQUIRED_ARTIFACTS = (
    "evaluation_contract.json",
    "run_manifest.json",
    "feature_availability_audit.json",
    "winner_cluster_summary.json",
    "false_lookalike_pairs.jsonl",
    "candle_sequence_feature_importance.json",
    "monthly_daily_pattern_contrast.json",
    "actionable_pattern_candidates.json",
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


def _load_daily_frame(conn: duckdb.DuckDBPyConnection, *, start_ymd: int, end_ymd: int) -> pd.DataFrame:
    b_expr = _date_norm_expr("b.date")
    m_expr = _date_norm_expr("m.date")
    query = f"""
        WITH b AS (
            SELECT code, {b_expr} AS ymd, o, h, l, c, v, source
            FROM daily_bars AS b
        ),
        m AS (
            SELECT code, {m_expr} AS ymd, ma7, ma20, ma60
            FROM daily_ma AS m
        )
        SELECT b.code, b.ymd, b.o, b.h, b.l, b.c, b.v, m.ma7, m.ma20, m.ma60
        FROM b
        LEFT JOIN m ON b.code = m.code AND b.ymd = m.ymd
        WHERE b.ymd BETWEEN ? AND ?
          AND lower(coalesce(b.source, '')) = 'pan'
          AND b.o > 0 AND b.h > 0 AND b.l > 0 AND b.c > 0
        ORDER BY b.code, b.ymd
    """
    frame = conn.execute(query, [int(start_ymd), int(end_ymd)]).fetchdf()
    if frame.empty:
        raise RuntimeError("daily_bars query returned no rows")
    frame["date"] = pd.to_datetime(frame["ymd"].astype(str), format="%Y%m%d")
    frame["code"] = frame["code"].astype(str)
    return frame


def _load_monthly_frame(conn: duckdb.DuckDBPyConnection, *, start_ymd: int, end_ymd: int) -> pd.DataFrame:
    b_expr = _date_norm_expr("b.month")
    m_expr = _date_norm_expr("m.month")
    query = f"""
        WITH b AS (
            SELECT code, {b_expr} AS ymd, o, h, l, c, v
            FROM monthly_bars AS b
        ),
        m AS (
            SELECT code, {m_expr} AS ymd, ma7, ma20, ma60
            FROM monthly_ma AS m
        )
        SELECT b.code, b.ymd, b.o, b.h, b.l, b.c, b.v, m.ma7, m.ma20, m.ma60
        FROM b
        LEFT JOIN m ON b.code = m.code AND b.ymd = m.ymd
        WHERE b.ymd BETWEEN ? AND ?
          AND b.o > 0 AND b.h > 0 AND b.l > 0 AND b.c > 0
        ORDER BY b.code, b.ymd
    """
    frame = conn.execute(query, [int(start_ymd), int(end_ymd)]).fetchdf()
    if frame.empty:
        raise RuntimeError("monthly_bars query returned no rows")
    frame["month_date"] = pd.to_datetime(frame["ymd"].astype(str), format="%Y%m%d")
    frame["month_key"] = frame["month_date"].dt.to_period("M").astype(str)
    frame["code"] = frame["code"].astype(str)
    return frame


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


def build_daily_features(daily: pd.DataFrame, *, forward_days: int = FORWARD_DAYS) -> pd.DataFrame:
    frame = daily.sort_values(["code", "date"], kind="stable").copy()
    grouped = frame.groupby("code", sort=False)
    frame["prev_c"] = grouped["c"].shift(1)
    candle_range = (frame["h"] - frame["l"]).replace(0, pd.NA)
    frame["daily_return"] = _safe_div(frame["c"] - frame["prev_c"], frame["prev_c"])
    frame["latest_gap_pct"] = _safe_div(frame["o"] - frame["prev_c"], frame["prev_c"])
    frame["latest_body_ratio"] = pd.to_numeric((frame["c"] - frame["o"]).abs() / candle_range, errors="coerce").fillna(0.0)
    frame["latest_close_position"] = pd.to_numeric((frame["c"] - frame["l"]) / candle_range, errors="coerce").fillna(0.5)
    frame["latest_upper_wick_ratio"] = pd.to_numeric(
        (frame["h"] - frame[["o", "c"]].max(axis=1)) / candle_range,
        errors="coerce",
    ).fillna(0.0)
    frame["latest_lower_wick_ratio"] = pd.to_numeric(
        (frame[["o", "c"]].min(axis=1) - frame["l"]) / candle_range,
        errors="coerce",
    ).fillna(0.0)
    frame["latest_ma20_dist"] = _safe_div(frame["c"] - frame["ma20"], frame["ma20"])
    frame["latest_ma60_dist"] = _safe_div(frame["c"] - frame["ma60"], frame["ma60"])
    frame["history_days"] = grouped.cumcount() + 1

    rolling5_vol = grouped["v"].transform(lambda s: s.rolling(5, min_periods=5).mean())
    rolling20_vol = grouped["v"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    frame["latest_vol_ratio_5_20"] = _safe_div(rolling5_vol, rolling20_vol)

    frame["daily20_return"] = grouped["c"].transform(lambda s: s / s.shift(19) - 1.0)
    frame["daily5_return"] = grouped["c"].transform(lambda s: s / s.shift(4) - 1.0)

    strong_bull = (frame["c"] > frame["o"]) & (frame["latest_body_ratio"] >= 0.35) & (frame["latest_close_position"] >= 0.65)
    weak_bear = (frame["c"] < frame["o"]) & ((frame["latest_close_position"] <= 0.35) | (frame["latest_upper_wick_ratio"] >= 0.45))
    upper_wick = frame["latest_upper_wick_ratio"] >= 0.45
    lower_wick = frame["latest_lower_wick_ratio"] >= 0.45
    ma20_below = frame["c"] < frame["ma20"]
    ma20_reclaim = (frame["c"] > frame["ma20"]) & (grouped["c"].shift(1) <= grouped["ma20"].shift(1))
    strong_followed_by_weak = weak_bear & strong_bull.groupby(frame["code"], sort=False).shift(1, fill_value=False).astype(bool)
    weak_negated_by_strong = strong_bull & weak_bear.groupby(frame["code"], sort=False).shift(1, fill_value=False).astype(bool)

    bool_features = {
        "strong_bull_count_20": strong_bull,
        "weak_bear_count_20": weak_bear,
        "upper_wick_count_20": upper_wick,
        "lower_wick_count_20": lower_wick,
        "ma20_below_count_20": ma20_below,
        "ma20_reclaim_count_20": ma20_reclaim,
        "strong_followed_by_weak_count_20": strong_followed_by_weak,
        "weak_negated_by_strong_count_20": weak_negated_by_strong,
    }
    for name, values in bool_features.items():
        temp = values.astype(float)
        frame[name] = temp.groupby(frame["code"], sort=False).transform(lambda s: s.rolling(20, min_periods=20).sum())

    frame[f"future_close_{forward_days}"] = grouped["c"].shift(-forward_days)
    frame[f"future_high_{forward_days}"] = grouped["h"].transform(lambda s: _forward_window(s, days=forward_days, op="max"))
    frame[f"future_low_{forward_days}"] = grouped["l"].transform(lambda s: _forward_window(s, days=forward_days, op="min"))
    frame["forward_ret_20d"] = _safe_div(frame[f"future_close_{forward_days}"] - frame["c"], frame["c"])
    frame["mfe_20d"] = _safe_div(frame[f"future_high_{forward_days}"] - frame["c"], frame["c"])
    frame["mae_20d"] = _safe_div(frame[f"future_low_{forward_days}"] - frame["c"], frame["c"])
    frame["month_key"] = frame["date"].dt.to_period("M").astype(str)
    return frame


def build_monthly_features(monthly: pd.DataFrame) -> pd.DataFrame:
    frame = monthly.sort_values(["code", "month_date"], kind="stable").copy()
    grouped = frame.groupby("code", sort=False)
    candle_range = (frame["h"] - frame["l"]).replace(0, pd.NA)
    frame["monthly_ret_1m"] = grouped["c"].transform(lambda s: s / s.shift(1) - 1.0)
    frame["monthly_ret_3m"] = grouped["c"].transform(lambda s: s / s.shift(3) - 1.0)
    frame["monthly_ret_6m"] = grouped["c"].transform(lambda s: s / s.shift(6) - 1.0)
    frame["monthly_body_ratio"] = pd.to_numeric((frame["c"] - frame["o"]).abs() / candle_range, errors="coerce").fillna(0.0)
    frame["monthly_close_position"] = pd.to_numeric((frame["c"] - frame["l"]) / candle_range, errors="coerce").fillna(0.5)
    frame["monthly_upper_wick_ratio"] = pd.to_numeric(
        (frame["h"] - frame[["o", "c"]].max(axis=1)) / candle_range,
        errors="coerce",
    ).fillna(0.0)
    frame["monthly_lower_wick_ratio"] = pd.to_numeric(
        (frame[["o", "c"]].min(axis=1) - frame["l"]) / candle_range,
        errors="coerce",
    ).fillna(0.0)
    frame["monthly_ma20_dist"] = _safe_div(frame["c"] - frame["ma20"], frame["ma20"])
    frame["monthly_ma60_dist"] = _safe_div(frame["c"] - frame["ma60"], frame["ma60"])
    monthly_up = frame["c"] > frame["o"]
    monthly_upper = frame["monthly_upper_wick_ratio"] >= 0.45
    monthly_above_ma20 = frame["c"] > frame["ma20"]
    frame["monthly_up_count_6"] = monthly_up.astype(float).groupby(frame["code"], sort=False).transform(lambda s: s.rolling(6, min_periods=6).sum())
    frame["monthly_upper_wick_count_6"] = monthly_upper.astype(float).groupby(frame["code"], sort=False).transform(lambda s: s.rolling(6, min_periods=6).sum())
    frame["monthly_close_above_ma20_count_6"] = monthly_above_ma20.astype(float).groupby(frame["code"], sort=False).transform(lambda s: s.rolling(6, min_periods=6).sum())
    rolling_high_6 = grouped["h"].transform(lambda s: s.rolling(6, min_periods=6).max())
    rolling_low_6 = grouped["l"].transform(lambda s: s.rolling(6, min_periods=6).min())
    frame["monthly_drawdown_6m"] = _safe_div(frame["c"] - rolling_high_6, rolling_high_6)
    frame["monthly_range_6m"] = _safe_div(rolling_high_6 - rolling_low_6, rolling_low_6)
    return frame[["code", "month_key", *MONTHLY_DAILY_FEATURES]]


def _bucket(value: Any, *, low: float, high: float, labels: tuple[str, str, str]) -> str:
    numeric = _safe_float(value)
    if numeric is None:
        return "unknown"
    if numeric <= low:
        return labels[0]
    if numeric >= high:
        return labels[2]
    return labels[1]


def add_pattern_labels(anchors: pd.DataFrame) -> pd.DataFrame:
    frame = anchors.copy()
    frame["monthly_state"] = "monthly_mixed"
    frame.loc[(frame["monthly_ret_6m"] >= 0.12) & (frame["monthly_close_above_ma20_count_6"] >= 5), "monthly_state"] = "monthly_uptrend"
    frame.loc[(frame["monthly_ret_6m"] < -0.08) | (frame["monthly_drawdown_6m"] <= -0.18), "monthly_state"] = "monthly_down_or_drawdown"
    frame.loc[(frame["monthly_ret_3m"] >= 0.06) & (frame["monthly_close_above_ma20_count_6"] >= 3), "monthly_state"] = "monthly_recovery"

    frame["daily_trend_state"] = frame["daily20_return"].map(
        lambda value: _bucket(value, low=-0.03, high=0.06, labels=("daily_fading", "daily_flat", "daily_advancing"))
    )
    frame["daily_ma_state"] = "daily_ma_mixed"
    frame.loc[(frame["latest_ma20_dist"] >= 0.0) & (frame["ma20_below_count_20"] <= 5), "daily_ma_state"] = "daily_above_ma20"
    frame.loc[(frame["latest_ma20_dist"] < 0.0) & (frame["ma20_below_count_20"] >= 12), "daily_ma_state"] = "daily_below_ma20"

    energy = pd.to_numeric(frame["strong_bull_count_20"], errors="coerce").fillna(0.0) - pd.to_numeric(
        frame["weak_bear_count_20"], errors="coerce"
    ).fillna(0.0)
    frame["daily_candle_energy_state"] = "daily_energy_mixed"
    frame.loc[energy >= 3, "daily_candle_energy_state"] = "daily_energy_positive"
    frame.loc[energy <= -3, "daily_candle_energy_state"] = "daily_energy_negative"
    frame["lookalike_key"] = frame[list(COARSE_LOOKALIKE_FEATURES)].astype(str).agg("|".join, axis=1)
    return frame


def build_anchor_frame(
    daily: pd.DataFrame,
    monthly: pd.DataFrame,
    *,
    anchor_start_ymd: int,
    max_daily_ymd: int,
    min_history_days: int,
    forward_days: int,
) -> pd.DataFrame:
    daily_features = build_daily_features(daily, forward_days=forward_days)
    monthly_features = build_monthly_features(monthly)
    last_idx = daily_features.groupby(["code", "month_key"], sort=False)["date"].idxmax()
    anchors = daily_features.loc[last_idx].copy()
    anchors = anchors.merge(monthly_features, on=["code", "month_key"], how="left")
    anchors = anchors[
        (anchors["ymd"] >= int(anchor_start_ymd))
        & (anchors["ymd"] <= int(max_daily_ymd))
        & (anchors["history_days"] >= int(min_history_days))
        & anchors["forward_ret_20d"].notna()
        & anchors["mfe_20d"].notna()
        & anchors["mae_20d"].notna()
    ].copy()
    if anchors.empty:
        raise RuntimeError("no eligible monthly anchors after history and forward-label filters")
    anchors["anchor_date"] = anchors["date"].dt.strftime("%Y-%m-%d")
    anchors["anchor_month"] = anchors["month_key"]
    anchors = add_pattern_labels(anchors)

    grouped = anchors.groupby("anchor_month", sort=False)["forward_ret_20d"]
    anchors["month_ret20_p80"] = grouped.transform(lambda s: s.quantile(0.80))
    anchors["month_ret20_p20"] = grouped.transform(lambda s: s.quantile(0.20))
    winner_cutoff = anchors["month_ret20_p80"].clip(lower=WINNER_FLOOR_RETURN)
    failure_cutoff = anchors["month_ret20_p20"].clip(upper=FAILURE_CEILING_RETURN)
    anchors["is_winner"] = anchors["forward_ret_20d"] >= winner_cutoff
    anchors["is_failure"] = (anchors["forward_ret_20d"] <= failure_cutoff) | (anchors["mae_20d"] <= SEVERE_MAE_THRESHOLD)

    key_counts = anchors.groupby("lookalike_key").agg(
        key_winner_count=("is_winner", "sum"),
        key_row_count=("code", "count"),
    )
    anchors = anchors.merge(key_counts, on="lookalike_key", how="left")
    anchors["is_false_lookalike"] = (
        anchors["key_winner_count"].fillna(0).astype(int).gt(0)
        & ~anchors["is_winner"]
        & anchors["forward_ret_20d"].le(FALSE_LOOKALIKE_RETURN_MAX)
    )
    return anchors


def _mean(values: pd.Series) -> float | None:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    if numeric.empty:
        return None
    return float(numeric.mean())


def _median(values: pd.Series) -> float | None:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    if numeric.empty:
        return None
    return float(numeric.median())


def _cohen_d(left: pd.Series, right: pd.Series) -> float | None:
    a = pd.to_numeric(left, errors="coerce").dropna()
    b = pd.to_numeric(right, errors="coerce").dropna()
    if len(a) < 2 or len(b) < 2:
        return None
    var_a = float(a.var(ddof=1))
    var_b = float(b.var(ddof=1))
    pooled = math.sqrt(max(((len(a) - 1) * var_a + (len(b) - 1) * var_b) / max(len(a) + len(b) - 2, 1), 0.0))
    delta = float(a.mean() - b.mean())
    if pooled == 0.0:
        if abs(delta) <= 1e-12:
            return 0.0
        return 99.0 if delta > 0.0 else -99.0
    return float(delta / pooled)


def _monthly_stability(frame: pd.DataFrame, feature: str) -> dict[str, Any]:
    rows = []
    for month, group in frame.groupby("anchor_month", sort=True):
        winners = group.loc[group["is_winner"], feature]
        false_rows = group.loc[group["is_false_lookalike"], feature]
        if winners.notna().sum() < 2 or false_rows.notna().sum() < 2:
            continue
        delta = _mean(winners)
        false_mean = _mean(false_rows)
        if delta is None or false_mean is None:
            continue
        rows.append({"anchor_month": month, "delta": float(delta - false_mean)})
    non_zero = [row for row in rows if abs(row["delta"]) > 1e-12]
    if not non_zero:
        return {"months_with_pairs": len(rows), "dominant_sign": "none", "sign_stability": 0.0}
    positives = sum(1 for row in non_zero if row["delta"] > 0)
    negatives = sum(1 for row in non_zero if row["delta"] < 0)
    dominant = "winner_higher" if positives >= negatives else "false_lookalike_higher"
    return {
        "months_with_pairs": len(rows),
        "dominant_sign": dominant,
        "sign_stability": float(max(positives, negatives) / len(non_zero)),
    }


def build_feature_contrast(anchors: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    sample = anchors.loc[anchors["is_winner"] | anchors["is_false_lookalike"]].copy()
    winner_rows = sample.loc[sample["is_winner"]]
    false_rows = sample.loc[sample["is_false_lookalike"]]
    for feature in DISCOVERY_FEATURES:
        if feature not in sample.columns:
            continue
        winner_mean = _mean(winner_rows[feature])
        false_mean = _mean(false_rows[feature])
        if winner_mean is None or false_mean is None:
            continue
        cohen = _cohen_d(winner_rows[feature], false_rows[feature])
        stability = _monthly_stability(sample, feature)
        abs_effect = abs(cohen or 0.0)
        months = int(stability["months_with_pairs"])
        stable = float(stability["sign_stability"])
        if abs_effect >= 0.35 and stable >= 0.62 and months >= 18:
            usefulness = "high"
        elif abs_effect >= 0.20 and stable >= 0.58 and months >= 12:
            usefulness = "medium"
        else:
            usefulness = "low"
        rows.append(
            {
                "feature": feature,
                "feature_group": "daily_sequence" if feature in DAILY_SEQUENCE_FEATURES else "monthly_daily_context",
                "winner_count": int(winner_rows[feature].notna().sum()),
                "false_lookalike_count": int(false_rows[feature].notna().sum()),
                "winner_mean": winner_mean,
                "false_lookalike_mean": false_mean,
                "winner_median": _median(winner_rows[feature]),
                "false_lookalike_median": _median(false_rows[feature]),
                "winner_minus_false_mean_delta": float(winner_mean - false_mean),
                "cohen_d_winner_vs_false": cohen,
                "direction": "winner_higher" if winner_mean > false_mean else "false_lookalike_higher",
                "months_with_pairs": months,
                "monthly_dominant_sign": stability["dominant_sign"],
                "monthly_sign_stability": stable,
                "research_usefulness": usefulness,
                "production_status": "discovery_only_not_scoring",
            }
        )
    return sorted(rows, key=lambda row: (row["research_usefulness"] != "high", -(abs(row.get("cohen_d_winner_vs_false") or 0.0)), row["feature"]))


def build_winner_cluster_summary(anchors: pd.DataFrame, contrast_rows: list[dict[str, Any]]) -> dict[str, Any]:
    top_features = [row["feature"] for row in contrast_rows[:8]]
    clusters: list[dict[str, Any]] = []
    for key, group in anchors.groupby("lookalike_key", sort=False):
        winners = group[group["is_winner"]]
        false_rows = group[group["is_false_lookalike"]]
        if len(winners) == 0 or len(false_rows) == 0:
            continue
        cluster_features = []
        for feature in top_features:
            wm = _mean(winners[feature])
            fm = _mean(false_rows[feature])
            if wm is None or fm is None:
                continue
            cluster_features.append({"feature": feature, "winner_mean": wm, "false_lookalike_mean": fm, "delta": float(wm - fm)})
        clusters.append(
            {
                "lookalike_key": key,
                "row_count": int(len(group)),
                "winner_count": int(len(winners)),
                "false_lookalike_count": int(len(false_rows)),
                "failure_count": int(group["is_failure"].sum()),
                "mean_winner_ret20": _mean(winners["forward_ret_20d"]),
                "mean_false_ret20": _mean(false_rows["forward_ret_20d"]),
                "mean_winner_mae20": _mean(winners["mae_20d"]),
                "mean_false_mae20": _mean(false_rows["mae_20d"]),
                "top_contrast_features": cluster_features[:5],
            }
        )
    clusters = sorted(clusters, key=lambda row: (row["winner_count"] + row["false_lookalike_count"], row["winner_count"]), reverse=True)
    return {
        "schema_version": f"{SCHEMA_PREFIX}_winner_cluster_summary_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "cluster_count": len(clusters),
        "clusters": clusters[:80],
    }


def _normalized_distance(left: pd.Series, right: pd.Series, scales: dict[str, float], features: list[str]) -> float:
    total = 0.0
    used = 0
    for feature in features:
        a = _safe_float(left.get(feature))
        b = _safe_float(right.get(feature))
        scale = scales.get(feature) or 1.0
        if a is None or b is None or scale == 0.0:
            continue
        total += abs(a - b) / scale
        used += 1
    return float(total / used) if used else 999.0


def build_false_lookalike_pairs(anchors: pd.DataFrame, *, max_rows: int = DEFAULT_MAX_PAIR_ROWS) -> list[dict[str, Any]]:
    pair_features = [
        "daily20_return",
        "daily5_return",
        "latest_ma20_dist",
        "strong_bull_count_20",
        "weak_bear_count_20",
        "monthly_ret_3m",
        "monthly_ret_6m",
        "monthly_ma20_dist",
    ]
    scales = {
        feature: float(pd.to_numeric(anchors[feature], errors="coerce").std() or 1.0)
        for feature in pair_features
        if feature in anchors.columns
    }
    pairs: list[dict[str, Any]] = []
    for key, group in anchors.groupby("lookalike_key", sort=False):
        winners = group[group["is_winner"]].head(80)
        false_rows = group[group["is_false_lookalike"]].head(160)
        if winners.empty or false_rows.empty:
            continue
        for _, winner in winners.iterrows():
            distances = false_rows.apply(lambda row: _normalized_distance(winner, row, scales, pair_features), axis=1)
            best_idx = distances.idxmin()
            false_row = false_rows.loc[best_idx]
            diff_reasons = []
            for feature in pair_features:
                if feature not in anchors.columns:
                    continue
                wv = _safe_float(winner.get(feature))
                fv = _safe_float(false_row.get(feature))
                if wv is None or fv is None:
                    continue
                diff_reasons.append({"feature": feature, "winner": wv, "false_lookalike": fv, "delta": float(wv - fv)})
            diff_reasons = sorted(diff_reasons, key=lambda item: abs(item["delta"]), reverse=True)[:5]
            pairs.append(
                {
                    "lookalike_key": key,
                    "distance": float(distances.loc[best_idx]),
                    "winner": {
                        "symbol": str(winner["code"]),
                        "anchor_date": str(winner["anchor_date"]),
                        "ret20": _safe_float(winner["forward_ret_20d"]),
                        "mfe20": _safe_float(winner["mfe_20d"]),
                        "mae20": _safe_float(winner["mae_20d"]),
                    },
                    "false_lookalike": {
                        "symbol": str(false_row["code"]),
                        "anchor_date": str(false_row["anchor_date"]),
                        "ret20": _safe_float(false_row["forward_ret_20d"]),
                        "mfe20": _safe_float(false_row["mfe_20d"]),
                        "mae20": _safe_float(false_row["mae_20d"]),
                    },
                    "typed_difference_reasons": diff_reasons,
                }
            )
            if len(pairs) >= max_rows:
                return pairs
    return pairs


def build_actionable_candidates(contrast_rows: list[dict[str, Any]]) -> dict[str, Any]:
    candidates = []
    for row in contrast_rows:
        if row["research_usefulness"] == "low":
            continue
        candidates.append(
            {
                "pattern_id": f"{AXIS_ID}:{row['feature']}",
                "feature": row["feature"],
                "feature_group": row["feature_group"],
                "direction": row["direction"],
                "typed_research_reason": "winner_false_lookalike_separation",
                "research_usefulness": row["research_usefulness"],
                "effect_size": row["cohen_d_winner_vs_false"],
                "monthly_sign_stability": row["monthly_sign_stability"],
                "months_with_pairs": row["months_with_pairs"],
                "production_status": "not_a_candidate_no_scoring_created",
                "allowed_next_step": "design_point_in_time_proxy_only_after_review",
            }
        )
    return {
        "schema_version": f"{SCHEMA_PREFIX}_actionable_pattern_candidates_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "candidate_count": len(candidates),
        "candidates": candidates[:50],
    }


def build_feature_availability_audit(anchors: pd.DataFrame) -> dict[str, Any]:
    feature_rows = []
    for column in [*DISCOVERY_FEATURES, *COARSE_LOOKALIKE_FEATURES]:
        present = column in anchors.columns
        non_null = int(anchors[column].notna().sum()) if present else 0
        feature_rows.append(
            {
                "column": column,
                "present": present,
                "non_null_count": non_null,
                "non_null_rate": None if len(anchors) == 0 else float(non_null / len(anchors)),
            }
        )
    label_overlap = sorted(set(COARSE_LOOKALIKE_FEATURES) & LABEL_COLUMNS)
    return {
        "schema_version": f"{SCHEMA_PREFIX}_feature_availability_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "row_count": int(len(anchors)),
        "anchor_month_count": int(anchors["anchor_month"].nunique()),
        "feature_rows": feature_rows,
        "coarse_lookalike_features": list(COARSE_LOOKALIKE_FEATURES),
        "future_label_columns": sorted(LABEL_COLUMNS),
        "coarse_key_label_overlap": label_overlap,
        "used_future_labels_in_pattern_grouping": bool(label_overlap),
        "label_usage": "oracle_discovery_only_not_production_scoring",
        "candidate_scoring_created": False,
        "silent_fallback_used": False,
    }


def build_evaluation_contract(
    *,
    source_db: Path,
    anchor_start_ymd: int,
    max_daily_ymd: int,
    years: int,
    forward_days: int,
    min_history_days: int,
) -> dict[str, Any]:
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_evaluation_contract_v1",
        "axis_id": AXIS_ID,
        "research_phase": "pattern_discovery_oracle_audit",
        "boundary": "TRADEX-only",
        "source_db": str(source_db),
        "anchor_start_ymd": int(anchor_start_ymd),
        "max_daily_ymd": int(max_daily_ymd),
        "requested_years": int(years),
        "forward_days": int(forward_days),
        "min_history_days": int(min_history_days),
        "same_condition_controls": {
            "same_universe_source": "runtime snapshot daily_bars pan source",
            "same_period": True,
            "same_top_k": "not_applicable_discovery_not_ranking",
            "same_regime_condition": "all_monthly_anchors_in_snapshot",
            "same_cost_slippage": contracts.TRADEX_DEFAULT_COST_MODEL,
            "same_artifact_detail_level": contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        },
        "future_label_policy": {
            "future_labels_used_for_oracle_classification": True,
            "future_labels_used_for_candidate_scoring": False,
            "candidate_scoring_created": False,
        },
        "silent_fallback_used": False,
        "meemee_reflection_allowed": False,
        "publish_bundle_allowed": False,
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_research_decision(
    *,
    anchors: pd.DataFrame,
    contrast_rows: list[dict[str, Any]],
    source_db: Path,
    output_dir: Path,
) -> dict[str, Any]:
    high = [row for row in contrast_rows if row["research_usefulness"] == "high"]
    medium = [row for row in contrast_rows if row["research_usefulness"] == "medium"]
    false_count = int(anchors["is_false_lookalike"].sum())
    winner_count = int(anchors["is_winner"].sum())
    months = int(anchors["anchor_month"].nunique())
    months_with_pairs = max([int(row["months_with_pairs"]) for row in contrast_rows], default=0)
    sample_ok = winner_count >= 100 and false_count >= 100 and months_with_pairs >= 24
    if sample_ok and len(high) >= 3:
        decision = "pattern_found"
    elif sample_ok and (len(high) + len(medium)) >= 3:
        decision = "pattern_weak"
    else:
        decision = "no_pattern"

    reasons: list[dict[str, Any]] = []
    reasons.append({"code": "winner_sample_count", "status": "pass" if winner_count >= 100 else "fail", "value": winner_count})
    reasons.append({"code": "false_lookalike_sample_count", "status": "pass" if false_count >= 100 else "fail", "value": false_count})
    reasons.append({"code": "months_with_pairs", "status": "pass" if months_with_pairs >= 24 else "fail", "value": months_with_pairs})
    reasons.append({"code": "high_usefulness_features", "status": "pass" if len(high) >= 3 else "fail", "value": len(high)})
    reasons.append({"code": "medium_or_high_usefulness_features", "status": "pass" if len(high) + len(medium) >= 3 else "fail", "value": len(high) + len(medium)})
    return {
        "schema_version": f"{SCHEMA_PREFIX}_research_decision_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "source_db": str(source_db),
        "artifact_root": str(output_dir),
        "decision": decision,
        "authoritative_research_decision": decision,
        "decision_reasons": reasons,
        "label_usage": "oracle_discovery_only",
        "candidate_scoring_created": False,
        "meemee_reflectable": False,
        "publish_bundle_created": False,
        "silent_fallback_used": False,
        "sample_summary": {
            "anchor_rows": int(len(anchors)),
            "symbol_count": int(anchors["code"].nunique()),
            "anchor_month_count": months,
            "winner_count": winner_count,
            "false_lookalike_count": false_count,
            "failure_count": int(anchors["is_failure"].sum()),
            "lookalike_key_count": int(anchors["lookalike_key"].nunique()),
            "months_with_pairs": months_with_pairs,
        },
        "top_research_features": high[:10] if high else contrast_rows[:10],
        "non_scope": [
            "no MeeMee UI change",
            "no runtime DB mutation",
            "no publish registry change",
            "no ranking formula change",
            "no production challenger scoring",
        ],
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
        "candidate_scoring_created": False,
        "silent_fallback_used": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
    }


def run_winner_lookalike_candle_decomposition_v1(
    *,
    source_db: str | Path | None = None,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    run_id: str | None = None,
    years: int = DEFAULT_YEARS,
    forward_days: int = FORWARD_DAYS,
    min_history_days: int = DEFAULT_MIN_HISTORY_DAYS,
    max_pair_rows: int = DEFAULT_MAX_PAIR_ROWS,
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
        data_start_ts = anchor_start_ts - pd.DateOffset(years=2)
        anchor_start_ymd = _timestamp_to_ymd(anchor_start_ts)
        data_start_ymd = _timestamp_to_ymd(data_start_ts)
        daily = _load_daily_frame(conn, start_ymd=data_start_ymd, end_ymd=max_daily_ymd)
        monthly = _load_monthly_frame(conn, start_ymd=data_start_ymd, end_ymd=max_daily_ymd)
    finally:
        conn.close()

    anchors = build_anchor_frame(
        daily,
        monthly,
        anchor_start_ymd=anchor_start_ymd,
        max_daily_ymd=max_daily_ymd,
        min_history_days=min_history_days,
        forward_days=forward_days,
    )
    contrast_rows = build_feature_contrast(anchors)
    cluster_summary = build_winner_cluster_summary(anchors, contrast_rows)
    false_pairs = build_false_lookalike_pairs(anchors, max_rows=max_pair_rows)
    actionable = build_actionable_candidates(contrast_rows)
    evaluation_contract = build_evaluation_contract(
        source_db=source_path,
        anchor_start_ymd=anchor_start_ymd,
        max_daily_ymd=max_daily_ymd,
        years=years,
        forward_days=forward_days,
        min_history_days=min_history_days,
    )
    feature_audit = build_feature_availability_audit(anchors)
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
            "forward_days": int(forward_days),
            "min_history_days": int(min_history_days),
            "winner_floor_return": WINNER_FLOOR_RETURN,
            "false_lookalike_return_max": FALSE_LOOKALIKE_RETURN_MAX,
            "failure_ceiling_return": FAILURE_CEILING_RETURN,
            "severe_mae_threshold": SEVERE_MAE_THRESHOLD,
            "candidate_scoring_created": False,
        },
        universe=sorted(anchors["code"].astype(str).unique().tolist()),
        period={"start_date": str(anchor_start_ymd), "end_date": str(max_daily_ymd), "label": "monthly_anchor_discovery"},
        horizon=f"{forward_days}d",
        artifact_detail_level=contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        fallback_status=contracts.TRADEX_FALLBACK_STATUS_AUTHORITATIVE,
        cost_model=contracts.TRADEX_DEFAULT_COST_MODEL,
    )
    contracts.validate_run_manifest(run_manifest)
    research_decision = build_research_decision(anchors=anchors, contrast_rows=contrast_rows, source_db=source_path, output_dir=output_dir)

    candle_importance = {
        "schema_version": f"{SCHEMA_PREFIX}_candle_sequence_feature_importance_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "feature_rows": [row for row in contrast_rows if row["feature_group"] == "daily_sequence"],
    }
    monthly_contrast = {
        "schema_version": f"{SCHEMA_PREFIX}_monthly_daily_pattern_contrast_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "feature_rows": contrast_rows,
        "coarse_lookalike_features": list(COARSE_LOOKALIKE_FEATURES),
    }

    paths: dict[str, str] = {}
    json_artifacts = {
        "evaluation_contract.json": evaluation_contract,
        "run_manifest.json": run_manifest,
        "feature_availability_audit.json": feature_audit,
        "winner_cluster_summary.json": cluster_summary,
        "candle_sequence_feature_importance.json": candle_importance,
        "monthly_daily_pattern_contrast.json": monthly_contrast,
        "actionable_pattern_candidates.json": actionable,
        "research_decision.json": research_decision,
    }
    for name, payload in json_artifacts.items():
        paths[name] = str(_write_json(output_dir / name, payload))
    paths["false_lookalike_pairs.jsonl"] = str(_write_jsonl(output_dir / "false_lookalike_pairs.jsonl", false_pairs))
    complete = _artifact_complete(output_dir, paths, research_decision)
    paths["_ARTIFACT_COMPLETE.json"] = str(_write_json(output_dir / "_ARTIFACT_COMPLETE.json", complete))

    return {
        "output_dir": str(output_dir),
        "paths": paths,
        "authoritative_research_decision": research_decision["authoritative_research_decision"],
        "sample_summary": research_decision["sample_summary"],
        "top_research_features": research_decision["top_research_features"][:8],
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
    parser.add_argument("--forward-days", type=int, default=FORWARD_DAYS)
    parser.add_argument("--min-history-days", type=int, default=DEFAULT_MIN_HISTORY_DAYS)
    parser.add_argument("--max-pair-rows", type=int, default=DEFAULT_MAX_PAIR_ROWS)
    args = parser.parse_args(argv)

    result = run_winner_lookalike_candle_decomposition_v1(
        source_db=args.source_db.strip() or None,
        output_root=args.output_root,
        run_id=args.run_id.strip() or None,
        years=args.years,
        forward_days=args.forward_days,
        min_history_days=args.min_history_days,
        max_pair_rows=args.max_pair_rows,
    )
    print(_json_text(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
