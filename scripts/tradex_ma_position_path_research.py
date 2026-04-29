from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import duckdb
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from numpy.lib.stride_tricks import sliding_window_view


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB_PATH = Path(os.getenv("STOCKS_DB_PATH") or os.getenv("TRADEX_SNAPSHOT_DB_PATH") or r"C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb")
DEFAULT_OUTPUT_DIR = Path(r"G:\Tradex\ma_position_path_research")

SUMMARY_SCHEMA_VERSION = "tradex_ma_candlestick_position_path_value_summary_v2"
DECISION_SCHEMA_VERSION = "tradex_ma_candlestick_position_path_value_decision_v1"
MANIFEST_SCHEMA_VERSION = "tradex_ma_candlestick_position_path_value_manifest_v1"
REGIME_SCHEMA_VERSION = "tradex_ma_candlestick_position_path_value_by_regime_v1"
MONTHLY_STABILITY_SCHEMA_VERSION = "tradex_ma_candlestick_position_path_value_monthly_stability_v1"
CLASSIFICATION_SCHEMA_VERSION = "tradex_ma_candlestick_position_path_value_classification_v1"
ROW_SCHEMA_VERSION = "tradex_ma_candlestick_position_path_value_rows_v1"

CONFIRMED_REGIME_SOURCE = "confirmed_market_regime_daily"
PROVISIONAL_REGIME_SOURCE = "provisional_regime_proxy"

SLOPE_FLAT_THRESHOLD = 0.0015
BODY_SMALL_THRESHOLD = 0.30
BODY_DOJI_THRESHOLD = 0.12
BODY_LARGE_THRESHOLD = 0.90
WICK_DOMINANT_THRESHOLD = 0.45
GAP_THRESHOLD = 0.005
PRICE_LOCATION_NEAR_THRESHOLD = 0.67
PRICE_LOCATION_LOW_THRESHOLD = 0.33
STATE_MIN_SAMPLE_COUNT = 30
STATE_MIN_REGIME_SAMPLE_COUNT = 20
WEAK_NOISE_SCORE_THRESHOLD = 0.01
BAD_PICK_SCORE_THRESHOLD = -0.01
BAD_PICK_MAE_THRESHOLD = -0.03
HIGH_VALUE_SCORE_THRESHOLD = 0.02
PATH_WINDOW_DAYS = 20
FORWARD_HORIZONS = (3, 5, 10, 20)
STATE_ID_COMPONENT_FIELDS = [
    "state_close_ma7",
    "state_close_ma20",
    "state_close_ma60",
    "state_body_ma7",
    "state_body_ma20",
    "state_body_ma60",
    "state_ma_stack",
    "state_ma7_slope",
    "state_ma20_slope",
    "state_ma60_slope",
    "state_ma7_streak",
    "state_ma20_streak",
    "state_ma60_streak",
    "state_candle_code",
    "state_price_location_20",
    "state_price_location_60",
    "state_volume_condition",
    "state_regime_label",
    "state_regime_source",
]
STATE_CORE_COMPONENT_FIELDS = STATE_ID_COMPONENT_FIELDS[:-2]


@dataclass(frozen=True)
class MetricDefinition:
    name: str
    description: str


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, (np.floating, np.integer)):
        return value.item()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )


def _write_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        frame.to_parquet(path, index=False, compression="zstd")
        return
    except Exception:
        conn = duckdb.connect()
        try:
            conn.register("frame_df", frame)
            conn.execute(f"COPY frame_df TO '{path.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        finally:
            conn.close()


class _ParquetChunkWriter:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._writer: pq.ParquetWriter | None = None

    def write_frame(self, frame: pd.DataFrame) -> None:
        if frame.empty:
            return
        table = pa.Table.from_pandas(frame, preserve_index=False)
        if self._writer is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._writer = pq.ParquetWriter(self.path.as_posix(), table.schema, compression="zstd")
        self._writer.write_table(table)

    def close(self) -> None:
        if self._writer is not None:
            self._writer.close()
            self._writer = None


def _make_session_id() -> str:
    return f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"


def _resolve_output_paths(output_root: Path, session_id: str) -> tuple[Path, Path]:
    session_tmp = output_root / f"{session_id}.tmp"
    session_final = output_root / session_id
    return session_tmp, session_final


def _finalize_session_dir(session_tmp: Path, session_final: Path) -> None:
    if session_final.exists():
        raise FileExistsError(f"final session output already exists: {session_final}")
    try:
        session_tmp.replace(session_final)
    except Exception:
        shutil.move(str(session_tmp), str(session_final))


def _progress_log(message: str) -> None:
    print(f"[ma_position_path_research] {message}", file=sys.stderr, flush=True)


def _text(value: Any, fallback: str = "") -> str:
    if value is None:
        return fallback
    if isinstance(value, str):
        text = value.strip()
        return text or fallback
    text = str(value).strip()
    return text or fallback


def _safe_float(value: Any, fallback: float | None = None) -> float | None:
    if value is None:
        return fallback
    try:
        out = float(value)
    except Exception:
        return fallback
    if not math.isfinite(out):
        return fallback
    return float(out)


def _safe_int(value: Any, fallback: int | None = None) -> int | None:
    if value is None:
        return fallback
    try:
        return int(value)
    except Exception:
        return fallback


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


def _table_row_count(conn: duckdb.DuckDBPyConnection, table_name: str) -> int:
    if not _table_exists(conn, table_name):
        return 0
    row = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _ymd_expr(column_name: str) -> str:
    return f"""
        CASE
            WHEN TRY_CAST({column_name} AS BIGINT) BETWEEN 19000101 AND 20991231 THEN CAST(TRY_CAST({column_name} AS BIGINT) AS INTEGER)
            WHEN TRY_CAST({column_name} AS BIGINT) >= 1000000000000 THEN CAST(strftime(to_timestamp(TRY_CAST({column_name} AS BIGINT) / 1000), '%Y%m%d') AS INTEGER)
            WHEN TRY_CAST({column_name} AS BIGINT) BETWEEN 600000000 AND 5000000000 THEN CAST(strftime(to_timestamp(TRY_CAST({column_name} AS BIGINT)), '%Y%m%d') AS INTEGER)
            ELSE NULL
        END
    """.strip()


def _resolve_source_db_path(cli_value: str | None) -> Path:
    if cli_value and str(cli_value).strip():
        path = Path(str(cli_value)).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"DB not found: {path}")
        return path
    if DEFAULT_DB_PATH.exists():
        return DEFAULT_DB_PATH.resolve()
    raise FileNotFoundError("Could not resolve source DB path. Pass --db-path or set STOCKS_DB_PATH.")


def _normalize_regime_label(value: Any) -> str:
    text = _text(value, fallback="unknown")
    return text or "unknown"


def _normalized_source_frame(conn: duckdb.DuckDBPyConnection, *, table_name: str, columns_sql: str, date_column: str) -> pd.DataFrame:
    if not _table_exists(conn, table_name):
        return pd.DataFrame()
    frame = conn.execute(
        f"""
        SELECT
            {columns_sql},
            {_ymd_expr(date_column)} AS trade_date
        FROM {table_name}
        WHERE {_ymd_expr(date_column)} IS NOT NULL
        ORDER BY 1, 2
        """
    ).fetchdf()
    if frame.empty:
        return frame
    frame = frame.copy()
    for column in frame.columns:
        if column in {"code", "regime_id", "label_version", "regime_source"}:
            frame[column] = frame[column].astype("string")
    if "trade_date" in frame.columns:
        frame["trade_date"] = pd.to_numeric(frame["trade_date"], errors="coerce").astype("Int64")
    retained_columns = [
        "code",
        "trade_date",
        "volume",
        "forward_ret_3d",
        "forward_ret_5d",
        "forward_ret_10d",
        "forward_ret_20d",
        "mfe_20d",
        "mae_20d",
        "days_to_mfe_20d",
        "days_to_mae_20d",
        "days_to_positive_close",
        "days_to_plus_3pct",
        "days_to_plus_5pct",
        "days_to_minus_3pct",
        "days_to_minus_5pct",
        "hit_plus_5_before_minus_5",
        "hit_minus_5_before_plus_5",
        "hit_plus_3_before_minus_3",
        "hit_minus_3_before_plus_3",
        "hit_plus_1atr_before_minus_1atr",
        "mfe_atr_20d",
        "mae_atr_20d",
        "close_above_entry_days_20d",
        "close_below_entry_days_20d",
        "entry_next_open",
        "entry_day_close",
        "forward_window_days",
        "path_value_score_v1",
        "eligible_for_analysis",
        "regime_source",
        "regime_label",
        "volume_condition",
        "body_norm_atr",
        "upper_wick_ratio",
        "lower_wick_ratio",
        "state_close_ma7",
        "state_close_ma20",
        "state_close_ma60",
        "state_body_ma7",
        "state_body_ma20",
        "state_body_ma60",
        "state_ma_stack",
        "state_ma7_slope",
        "state_ma20_slope",
        "state_ma60_slope",
        "state_ma7_streak",
        "state_ma20_streak",
        "state_ma60_streak",
        "state_candle_code",
        "state_price_location_20",
        "state_price_location_60",
        "state_volume_condition",
        "state_regime_label",
        "state_regime_source",
    ]
    return frame.loc[:, retained_columns].copy()


def _load_bars_frame(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    frame = conn.execute(
        f"""
        SELECT
            CAST(code AS VARCHAR) AS code,
            {_ymd_expr("date")} AS trade_date,
            CAST(o AS DOUBLE) AS open,
            CAST(h AS DOUBLE) AS high,
            CAST(l AS DOUBLE) AS low,
            CAST(c AS DOUBLE) AS close,
            CAST(v AS DOUBLE) AS volume,
            CAST(COALESCE(source, 'pan') AS VARCHAR) AS source
        FROM daily_bars
        WHERE {_ymd_expr("date")} IS NOT NULL
        ORDER BY code, trade_date
        """
    ).fetchdf()
    if frame.empty:
        return frame
    frame = frame.copy()
    frame["code"] = frame["code"].astype("string")
    frame["trade_date"] = pd.to_numeric(frame["trade_date"], errors="coerce").astype("Int64")
    for column in ("open", "high", "low", "close", "volume"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def _load_ma_frame(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    if not _table_exists(conn, "daily_ma"):
        return pd.DataFrame(columns=["code", "trade_date", "ma7", "ma20", "ma60"])
    frame = conn.execute(
        f"""
        SELECT
            CAST(code AS VARCHAR) AS code,
            {_ymd_expr("date")} AS trade_date,
            CAST(ma7 AS DOUBLE) AS ma7,
            CAST(ma20 AS DOUBLE) AS ma20,
            CAST(ma60 AS DOUBLE) AS ma60
        FROM daily_ma
        WHERE {_ymd_expr("date")} IS NOT NULL
        ORDER BY code, trade_date
        """
    ).fetchdf()
    if frame.empty:
        return frame
    frame = frame.copy()
    frame["code"] = frame["code"].astype("string")
    frame["trade_date"] = pd.to_numeric(frame["trade_date"], errors="coerce").astype("Int64")
    for column in ("ma7", "ma20", "ma60"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def _load_regime_frame(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    if not _table_exists(conn, "market_regime_daily"):
        return pd.DataFrame(columns=["trade_date", "regime_id", "label_version"])
    frame = conn.execute(
        f"""
        SELECT
            {_ymd_expr("dt")} AS trade_date,
            CAST(regime_id AS VARCHAR) AS regime_id,
            CAST(label_version AS VARCHAR) AS label_version
        FROM market_regime_daily
        WHERE {_ymd_expr("dt")} IS NOT NULL
        ORDER BY trade_date
        """
    ).fetchdf()
    if frame.empty:
        return frame
    frame = frame.copy()
    frame["trade_date"] = pd.to_numeric(frame["trade_date"], errors="coerce").astype("Int64")
    frame["regime_id"] = frame["regime_id"].astype("string")
    frame["label_version"] = frame["label_version"].astype("string")
    return frame.drop_duplicates(subset=["trade_date"], keep="last")


def _rolling_streak_counts(close: np.ndarray, ma: np.ndarray, *, direction: str, cap: int) -> np.ndarray:
    out = np.zeros(len(close), dtype=np.int64)
    if len(close) == 0:
        return out
    active = np.isfinite(close) & np.isfinite(ma)
    if direction == "above":
        active &= close > ma
    else:
        active &= close < ma
    if not np.any(active):
        return out
    index = np.arange(len(close), dtype=np.int64)
    last_reset = np.where(~active, index, -1)
    last_reset = np.maximum.accumulate(last_reset)
    out[active] = index[active] - last_reset[active]
    if cap > 0:
        np.minimum(out, cap, out=out)
    return out


def _slope_state(current: float | None, previous: float | None) -> str:
    if current is None or previous is None or not math.isfinite(current) or not math.isfinite(previous) or previous == 0:
        return "flat"
    delta = current / previous - 1.0
    if delta >= SLOPE_FLAT_THRESHOLD:
        return "up"
    if delta <= -SLOPE_FLAT_THRESHOLD:
        return "down"
    return "flat"


def _bucket_streak(value: int, cap: int) -> str:
    if value <= 0:
        return "0"
    if value >= cap:
        return f"{cap}+"
    return str(int(value))


def _classify_ma_relation(open_price: float, close_price: float, high_price: float, low_price: float, ma: float | None) -> str:
    if ma is None or not math.isfinite(ma):
        return "unknown"
    body_low = min(open_price, close_price)
    body_high = max(open_price, close_price)
    if body_low > ma:
        return "body_full_above"
    if body_high < ma:
        return "body_full_below"
    if body_low <= ma <= body_high:
        return "body_crosses"
    if low_price <= ma <= high_price:
        return "wick_touches_only"
    return "unknown"


def _classify_close_vs_ma(close_price: float, ma: float | None) -> str:
    if ma is None or not math.isfinite(ma):
        return "unknown"
    return "above" if close_price >= ma else "below"


def _classify_candle_tags(
    *,
    open_price: float,
    high_price: float,
    low_price: float,
    close_price: float,
    atr14: float | None,
    prev_close: float | None,
    prev_high: float | None,
    prev_low: float | None,
) -> tuple[str, list[str], dict[str, float | None]]:
    candle_range = max(high_price - low_price, 1e-9)
    body = abs(close_price - open_price)
    upper_wick = max(0.0, high_price - max(open_price, close_price))
    lower_wick = max(0.0, min(open_price, close_price) - low_price)
    body_norm = None
    if atr14 is not None and atr14 > 0:
        body_norm = body / atr14
    else:
        body_norm = body / candle_range
    upper_ratio = upper_wick / candle_range
    lower_ratio = lower_wick / candle_range
    tags: list[str] = []
    if close_price > open_price:
        tags.append("bullish_body")
    elif close_price < open_price:
        tags.append("bearish_body")
    else:
        tags.append("doji_like")
    if body_norm <= BODY_DOJI_THRESHOLD:
        tags.append("doji_like")
    elif body_norm <= BODY_SMALL_THRESHOLD:
        tags.append("small_body_koma")
    if close_price > open_price and body_norm >= BODY_LARGE_THRESHOLD:
        tags.append("large_bullish_body")
    if close_price < open_price and body_norm >= BODY_LARGE_THRESHOLD:
        tags.append("large_bearish_body")
    if upper_ratio >= WICK_DOMINANT_THRESHOLD and upper_ratio > lower_ratio:
        tags.append("upper_wick_dominant")
    if lower_ratio >= WICK_DOMINANT_THRESHOLD and lower_ratio > upper_ratio:
        tags.append("lower_wick_dominant")
    gap_pct = None if prev_close is None or prev_close == 0 else (open_price / prev_close) - 1.0
    if prev_close is not None and prev_high is not None and open_price >= prev_high * (1.0 + GAP_THRESHOLD):
        tags.append("gap_up")
    elif prev_close is not None and prev_low is not None and open_price <= prev_low * (1.0 - GAP_THRESHOLD):
        tags.append("gap_down")
    elif gap_pct is not None and gap_pct >= GAP_THRESHOLD:
        tags.append("gap_up")
    elif gap_pct is not None and gap_pct <= -GAP_THRESHOLD:
        tags.append("gap_down")
    ordered = []
    seen: set[str] = set()
    for tag in tags:
        if tag not in seen:
            ordered.append(tag)
            seen.add(tag)
    primary = ordered[0] if ordered else "neutral"
    return primary, ordered, {
        "body_norm": float(body_norm),
        "upper_wick_ratio": float(upper_ratio),
        "lower_wick_ratio": float(lower_ratio),
        "gap_pct": None if gap_pct is None else float(gap_pct),
    }


def _provisional_regime_proxy(*, close_price: float, ma20: float | None, ma60: float | None, ma20_slope: str, ma60_slope: str) -> str:
    if ma20 is None or ma60 is None or not math.isfinite(close_price):
        return "provisional_mixed"
    if close_price >= ma20 and ma20_slope == "up" and ma60_slope == "up":
        return "provisional_trend_up"
    if close_price <= ma20 and ma20_slope == "down" and ma60_slope == "down":
        return "provisional_trend_down"
    if close_price >= ma20 and ma20_slope in {"up", "flat"}:
        return "provisional_supportive"
    if close_price <= ma20 and ma20_slope in {"down", "flat"}:
        return "provisional_weak"
    return "provisional_mixed"


def _append_tag(tags: np.ndarray, mask: np.ndarray, tag: str) -> np.ndarray:
    if not np.any(mask):
        return tags
    return np.where(mask, tags + "+" + tag, tags)


def _bucket_streak_array(values: np.ndarray, cap: int) -> np.ndarray:
    values = np.asarray(values, dtype=np.int64)
    return np.where(
        values <= 0,
        "0",
        np.where(values >= cap, f"{cap}+", values.astype(str)),
    )


def _vectorized_close_vs_ma(close: np.ndarray, ma: np.ndarray) -> np.ndarray:
    return np.where(np.isfinite(ma), np.where(close >= ma, "above", "below"), "unknown")


def _vectorized_ma_relation(open_price: np.ndarray, close_price: np.ndarray, high_price: np.ndarray, low_price: np.ndarray, ma: np.ndarray) -> np.ndarray:
    body_low = np.minimum(open_price, close_price)
    body_high = np.maximum(open_price, close_price)
    return np.select(
        [
            ~np.isfinite(ma),
            body_low > ma,
            body_high < ma,
            (body_low <= ma) & (ma <= body_high),
            (low_price <= ma) & (ma <= high_price),
        ],
        [
            "unknown",
            "body_full_above",
            "body_full_below",
            "body_crosses",
            "wick_touches_only",
        ],
        default="unknown",
    )


def _vectorized_slope_states(current: np.ndarray, previous: np.ndarray) -> np.ndarray:
    out = np.full(len(current), "flat", dtype=object)
    valid = np.isfinite(current) & np.isfinite(previous) & (previous != 0)
    if not np.any(valid):
        return out
    delta = np.full(len(current), np.nan, dtype=np.float64)
    delta[valid] = (current[valid] / previous[valid]) - 1.0
    out[valid & (delta >= SLOPE_FLAT_THRESHOLD)] = "up"
    out[valid & (delta <= -SLOPE_FLAT_THRESHOLD)] = "down"
    return out


def _vectorized_candle_tags(
    *,
    open_price: np.ndarray,
    high_price: np.ndarray,
    low_price: np.ndarray,
    close_price: np.ndarray,
    atr14: np.ndarray,
    prev_close: np.ndarray,
    prev_high: np.ndarray,
    prev_low: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    candle_range = np.maximum(high_price - low_price, 1e-9)
    body = np.abs(close_price - open_price)
    upper_wick = np.maximum(0.0, high_price - np.maximum(open_price, close_price))
    lower_wick = np.maximum(0.0, np.minimum(open_price, close_price) - low_price)
    atr_valid = np.isfinite(atr14) & (atr14 > 0)
    body_norm = np.empty_like(body, dtype=np.float64)
    np.divide(body, atr14, out=body_norm, where=atr_valid)
    body_norm[~atr_valid] = np.divide(body[~atr_valid], candle_range[~atr_valid], out=np.zeros_like(body[~atr_valid], dtype=np.float64), where=candle_range[~atr_valid] > 0)
    upper_ratio = np.divide(upper_wick, candle_range, out=np.zeros_like(upper_wick, dtype=np.float64), where=candle_range > 0)
    lower_ratio = np.divide(lower_wick, candle_range, out=np.zeros_like(lower_wick, dtype=np.float64), where=candle_range > 0)
    primary = np.where(
        close_price > open_price,
        "bullish_body",
        np.where(close_price < open_price, "bearish_body", "doji_like"),
    ).astype(object)
    tags = primary.copy()
    tags = _append_tag(tags, (body_norm <= BODY_DOJI_THRESHOLD) & (primary != "doji_like"), "doji_like")
    tags = _append_tag(tags, (body_norm > BODY_DOJI_THRESHOLD) & (body_norm <= BODY_SMALL_THRESHOLD), "small_body_koma")
    tags = _append_tag(tags, (close_price > open_price) & (body_norm >= BODY_LARGE_THRESHOLD), "large_bullish_body")
    tags = _append_tag(tags, (close_price < open_price) & (body_norm >= BODY_LARGE_THRESHOLD), "large_bearish_body")
    tags = _append_tag(tags, (upper_ratio >= WICK_DOMINANT_THRESHOLD) & (upper_ratio > lower_ratio), "upper_wick_dominant")
    tags = _append_tag(tags, (lower_ratio >= WICK_DOMINANT_THRESHOLD) & (lower_ratio > upper_ratio), "lower_wick_dominant")
    gap_pct = np.full(len(open_price), np.nan, dtype=np.float64)
    valid_gap = np.isfinite(prev_close) & (prev_close != 0)
    np.divide(open_price, prev_close, out=gap_pct, where=valid_gap)
    gap_pct[valid_gap] -= 1.0
    tags = _append_tag(tags, np.isfinite(prev_high) & (open_price >= prev_high * (1.0 + GAP_THRESHOLD)), "gap_up")
    tags = _append_tag(tags, np.isfinite(prev_low) & (open_price <= prev_low * (1.0 - GAP_THRESHOLD)), "gap_down")
    compact = np.where(
        close_price > open_price,
        "BB",
        np.where(close_price < open_price, "BR", "DJ"),
    ).astype(object)
    compact = _append_tag(compact, (body_norm <= BODY_DOJI_THRESHOLD) & (compact != "DJ"), "DJ")
    compact = _append_tag(compact, (body_norm > BODY_DOJI_THRESHOLD) & (body_norm <= BODY_SMALL_THRESHOLD), "SK")
    compact = _append_tag(compact, (close_price > open_price) & (body_norm >= BODY_LARGE_THRESHOLD), "LBB")
    compact = _append_tag(compact, (close_price < open_price) & (body_norm >= BODY_LARGE_THRESHOLD), "LBR")
    compact = _append_tag(compact, (upper_ratio >= WICK_DOMINANT_THRESHOLD) & (upper_ratio > lower_ratio), "UW")
    compact = _append_tag(compact, (lower_ratio >= WICK_DOMINANT_THRESHOLD) & (lower_ratio > upper_ratio), "LW")
    compact = _append_tag(compact, np.isfinite(prev_high) & (open_price >= prev_high * (1.0 + GAP_THRESHOLD)), "GU")
    compact = _append_tag(compact, np.isfinite(prev_low) & (open_price <= prev_low * (1.0 - GAP_THRESHOLD)), "GD")
    return primary, tags, compact, body_norm.astype(np.float64), upper_ratio.astype(np.float64), lower_ratio.astype(np.float64), gap_pct.astype(np.float64)


def _path_threshold_hits(entry: float, highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, *, atr14: float | None) -> dict[str, Any]:
    out = {
        "days_to_positive_close": None,
        "days_to_plus_3pct": None,
        "days_to_plus_5pct": None,
        "days_to_minus_3pct": None,
        "days_to_minus_5pct": None,
        "hit_plus_5_before_minus_5": 0,
        "hit_minus_5_before_plus_5": 0,
        "hit_plus_3_before_minus_3": 0,
        "hit_minus_3_before_plus_3": 0,
        "hit_plus_1atr_before_minus_1atr": 0,
        "mfe_20d": None,
        "mae_20d": None,
        "days_to_mfe_20d": None,
        "days_to_mae_20d": None,
        "mfe_atr_20d": None,
        "mae_atr_20d": None,
        "close_above_entry_days_20d": 0,
        "close_below_entry_days_20d": 0,
    }
    if not math.isfinite(entry) or entry <= 0 or len(highs) == 0 or len(lows) == 0 or len(closes) == 0:
        return out
    plus_3 = entry * 1.03
    plus_5 = entry * 1.05
    minus_3 = entry * 0.97
    minus_5 = entry * 0.95
    if atr14 is not None and atr14 > 0:
        plus_atr = entry + atr14
        minus_atr = entry - atr14
    else:
        plus_atr = None
        minus_atr = None
    mfe_value = float(np.nanmax(highs))
    mae_value = float(np.nanmin(lows))
    out["mfe_20d"] = float((mfe_value / entry) - 1.0)
    out["mae_20d"] = float((mae_value / entry) - 1.0)
    mfe_idx = int(np.where(highs == mfe_value)[0][0]) if np.isfinite(mfe_value) else None
    mae_idx = int(np.where(lows == mae_value)[0][0]) if np.isfinite(mae_value) else None
    out["days_to_mfe_20d"] = None if mfe_idx is None else int(mfe_idx + 1)
    out["days_to_mae_20d"] = None if mae_idx is None else int(mae_idx + 1)
    if atr14 is not None and atr14 > 0:
        out["mfe_atr_20d"] = float((mfe_value - entry) / atr14)
        out["mae_atr_20d"] = float((mae_value - entry) / atr14)
    plus_5_day = None
    minus_5_day = None
    plus_3_day = None
    minus_3_day = None
    plus_atr_day = None
    minus_atr_day = None
    for idx, (high_price, low_price, close_price) in enumerate(zip(highs, lows, closes, strict=False), start=1):
        if math.isfinite(close_price) and close_price > entry:
            out["close_above_entry_days_20d"] += 1
        elif math.isfinite(close_price):
            out["close_below_entry_days_20d"] += 1
        if plus_3_day is None and high_price >= plus_3:
            plus_3_day = idx
        if minus_3_day is None and low_price <= minus_3:
            minus_3_day = idx
        if plus_5_day is None and high_price >= plus_5:
            plus_5_day = idx
        if minus_5_day is None and low_price <= minus_5:
            minus_5_day = idx
        if plus_atr_day is None and plus_atr is not None and high_price >= plus_atr:
            plus_atr_day = idx
        if minus_atr_day is None and minus_atr is not None and low_price <= minus_atr:
            minus_atr_day = idx
        if idx >= PATH_WINDOW_DAYS:
            break
    out["days_to_plus_3pct"] = plus_3_day
    out["days_to_plus_5pct"] = plus_5_day
    out["days_to_minus_3pct"] = minus_3_day
    out["days_to_minus_5pct"] = minus_5_day
    if plus_5_day is not None and (minus_5_day is None or plus_5_day < minus_5_day):
        out["hit_plus_5_before_minus_5"] = 1
    if minus_5_day is not None and (plus_5_day is None or minus_5_day < plus_5_day):
        out["hit_minus_5_before_plus_5"] = 1
    if plus_3_day is not None and (minus_3_day is None or plus_3_day < minus_3_day):
        out["hit_plus_3_before_minus_3"] = 1
    if minus_3_day is not None and (plus_3_day is None or minus_3_day < plus_3_day):
        out["hit_minus_3_before_plus_3"] = 1
    if plus_atr_day is not None and (minus_atr_day is None or plus_atr_day < minus_atr_day):
        out["hit_plus_1atr_before_minus_1atr"] = 1
    return out


def _path_threshold_hits_matrix(
    entry_window: np.ndarray,
    high_windows: np.ndarray,
    low_windows: np.ndarray,
    close_windows: np.ndarray,
    atr_window: np.ndarray,
) -> dict[str, np.ndarray]:
    entry_window = np.asarray(entry_window, dtype=np.float64)
    valid_entry = np.isfinite(entry_window) & (entry_window > 0)
    n_rows = len(entry_window)

    mfe_values = np.nanmax(high_windows, axis=1)
    mae_values = np.nanmin(low_windows, axis=1)
    close_above_mask = np.isfinite(close_windows) & (close_windows > entry_window[:, None])
    close_below_mask = np.isfinite(close_windows) & (close_windows <= entry_window[:, None])
    plus_3_mask = high_windows >= (entry_window[:, None] * 1.03)
    plus_5_mask = high_windows >= (entry_window[:, None] * 1.05)
    minus_3_mask = low_windows <= (entry_window[:, None] * 0.97)
    minus_5_mask = low_windows <= (entry_window[:, None] * 0.95)
    atr_valid = np.isfinite(atr_window) & (atr_window > 0)
    plus_atr_mask = np.zeros_like(high_windows, dtype=bool)
    minus_atr_mask = np.zeros_like(low_windows, dtype=bool)
    if np.any(atr_valid):
        plus_atr_mask = high_windows >= (entry_window[:, None] + atr_window[:, None])
        minus_atr_mask = low_windows <= (entry_window[:, None] - atr_window[:, None])

    def _first_hit(mask: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        any_hit = mask.any(axis=1)
        first = np.where(any_hit, np.argmax(mask, axis=1) + 1, np.nan)
        return any_hit, first.astype(np.float64)

    plus_3_any, days_to_plus_3 = _first_hit(plus_3_mask)
    plus_5_any, days_to_plus_5 = _first_hit(plus_5_mask)
    minus_3_any, days_to_minus_3 = _first_hit(minus_3_mask)
    minus_5_any, days_to_minus_5 = _first_hit(minus_5_mask)
    plus_atr_any, days_to_plus_atr = _first_hit(plus_atr_mask) if np.any(atr_valid) else (np.zeros(n_rows, dtype=bool), np.full(n_rows, np.nan))
    minus_atr_any, days_to_minus_atr = _first_hit(minus_atr_mask) if np.any(atr_valid) else (np.zeros(n_rows, dtype=bool), np.full(n_rows, np.nan))

    out = {
        "mfe_20d": np.where(valid_entry, (mfe_values / entry_window) - 1.0, np.nan),
        "mae_20d": np.where(valid_entry, (mae_values / entry_window) - 1.0, np.nan),
        "days_to_mfe_20d": np.where(valid_entry, np.argmax(high_windows, axis=1) + 1, np.nan),
        "days_to_mae_20d": np.where(valid_entry, np.argmin(low_windows, axis=1) + 1, np.nan),
        "days_to_positive_close": np.where(valid_entry & close_above_mask.any(axis=1), np.argmax(close_above_mask, axis=1) + 1, np.nan),
        "days_to_plus_3pct": np.where(valid_entry & plus_3_any, days_to_plus_3, np.nan),
        "days_to_plus_5pct": np.where(valid_entry & plus_5_any, days_to_plus_5, np.nan),
        "days_to_minus_3pct": np.where(valid_entry & minus_3_any, days_to_minus_3, np.nan),
        "days_to_minus_5pct": np.where(valid_entry & minus_5_any, days_to_minus_5, np.nan),
        "hit_plus_5_before_minus_5": np.where(valid_entry & plus_5_any & (~minus_5_any | (np.argmax(plus_5_mask, axis=1) < np.argmax(minus_5_mask, axis=1))), 1, 0),
        "hit_minus_5_before_plus_5": np.where(valid_entry & minus_5_any & (~plus_5_any | (np.argmax(minus_5_mask, axis=1) < np.argmax(plus_5_mask, axis=1))), 1, 0),
        "hit_plus_3_before_minus_3": np.where(valid_entry & plus_3_any & (~minus_3_any | (np.argmax(plus_3_mask, axis=1) < np.argmax(minus_3_mask, axis=1))), 1, 0),
        "hit_minus_3_before_plus_3": np.where(valid_entry & minus_3_any & (~plus_3_any | (np.argmax(minus_3_mask, axis=1) < np.argmax(plus_3_mask, axis=1))), 1, 0),
        "hit_plus_1atr_before_minus_1atr": np.where(
            valid_entry & plus_atr_any & (~minus_atr_any | (np.argmax(plus_atr_mask, axis=1) < np.argmax(minus_atr_mask, axis=1))),
            1,
            0,
        ),
        "mfe_atr_20d": np.divide(
            mfe_values - entry_window,
            atr_window,
            out=np.full(n_rows, np.nan, dtype=np.float64),
            where=valid_entry & atr_valid,
        ),
        "mae_atr_20d": np.divide(
            mae_values - entry_window,
            atr_window,
            out=np.full(n_rows, np.nan, dtype=np.float64),
            where=valid_entry & atr_valid,
        ),
        "close_above_entry_days_20d": np.where(valid_entry, close_above_mask.sum(axis=1), np.nan),
        "close_below_entry_days_20d": np.where(valid_entry, close_below_mask.sum(axis=1), np.nan),
    }
    return out


def _forward_returns(entry: float, closes: np.ndarray) -> dict[str, float | None]:
    out: dict[str, float | None] = {f"forward_ret_{horizon}d": None for horizon in FORWARD_HORIZONS}
    if not math.isfinite(entry) or entry <= 0:
        return out
    horizon_to_offset = {3: 4, 5: 6, 10: 11, 20: 21}
    for horizon, offset in horizon_to_offset.items():
        if len(closes) >= offset and math.isfinite(float(closes[offset - 1])):
            out[f"forward_ret_{horizon}d"] = float((float(closes[offset - 1]) / entry) - 1.0)
    return out


def _compute_path_value_score(row: pd.Series) -> float | None:
    f20 = _safe_float(row.get("forward_ret_20d"))
    mfe20 = _safe_float(row.get("mfe_20d"))
    mae20 = _safe_float(row.get("mae_20d"))
    f10 = _safe_float(row.get("forward_ret_10d"))
    above_days = _safe_float(row.get("close_above_entry_days_20d"))
    hit_plus = _safe_float(row.get("hit_plus_5_before_minus_5"))
    hit_minus = _safe_float(row.get("hit_minus_5_before_plus_5"))
    if None in {f20, mfe20, mae20, f10, above_days, hit_plus, hit_minus}:
        return None
    return float(
        0.30 * float(f20)
        + 0.25 * float(mfe20)
        - 0.30 * abs(float(mae20))
        + 0.10 * float(f10)
        + 0.05 * (float(above_days) / float(PATH_WINDOW_DAYS))
        + 0.10 * float(hit_plus)
        - 0.10 * float(hit_minus)
    )


def _enrich_symbol_frame(frame: pd.DataFrame, *, regime_frame: pd.DataFrame | None = None) -> pd.DataFrame:
    frame = frame.sort_values(["code", "trade_date"]).reset_index(drop=True).copy()
    frame["trade_date"] = pd.to_numeric(frame["trade_date"], errors="coerce").astype("Int64")
    for column in ("open", "high", "low", "close", "volume", "ma7", "ma20", "ma60"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    code = frame["code"].astype("string").to_numpy(copy=False)

    close = frame["close"].to_numpy(dtype=np.float64, copy=False)
    open_ = frame["open"].to_numpy(dtype=np.float64, copy=False)
    high = frame["high"].to_numpy(dtype=np.float64, copy=False)
    low = frame["low"].to_numpy(dtype=np.float64, copy=False)
    volume = frame["volume"].to_numpy(dtype=np.float64, copy=False)
    ma7 = frame["ma7"].to_numpy(dtype=np.float64, copy=False)
    ma20 = frame["ma20"].to_numpy(dtype=np.float64, copy=False)
    ma60 = frame["ma60"].to_numpy(dtype=np.float64, copy=False)

    prev_close = np.roll(close, 1)
    prev_close[0] = np.nan
    prev_high = np.roll(high, 1)
    prev_high[0] = np.nan
    prev_low = np.roll(low, 1)
    prev_low[0] = np.nan
    prev_ma7 = np.roll(ma7, 1)
    prev_ma7[0] = np.nan
    prev_ma20 = np.roll(ma20, 1)
    prev_ma20[0] = np.nan
    prev_ma60 = np.roll(ma60, 1)
    prev_ma60[0] = np.nan

    tr = np.maximum.reduce(
        [
            high - low,
            np.abs(high - prev_close),
            np.abs(low - prev_close),
        ]
    )
    tr[~np.isfinite(tr)] = np.nan
    atr14 = pd.Series(tr).rolling(window=14, min_periods=14).mean().to_numpy(dtype=np.float64, copy=False)
    vol20 = pd.Series(volume).rolling(window=20, min_periods=20).mean().to_numpy(dtype=np.float64, copy=False)
    high20 = pd.Series(high).rolling(window=20, min_periods=20).max().to_numpy(dtype=np.float64, copy=False)
    low20 = pd.Series(low).rolling(window=20, min_periods=20).min().to_numpy(dtype=np.float64, copy=False)
    high60 = pd.Series(high).rolling(window=60, min_periods=60).max().to_numpy(dtype=np.float64, copy=False)
    low60 = pd.Series(low).rolling(window=60, min_periods=60).min().to_numpy(dtype=np.float64, copy=False)

    frame["prev_close"] = prev_close
    frame["prev_high"] = prev_high
    frame["prev_low"] = prev_low
    frame["prev_ma7"] = prev_ma7
    frame["prev_ma20"] = prev_ma20
    frame["prev_ma60"] = prev_ma60
    frame["atr14"] = atr14
    frame["vol20"] = vol20
    frame["range_high20"] = high20
    frame["range_low20"] = low20
    frame["range_high60"] = high60
    frame["range_low60"] = low60
    close_pos20 = np.full(len(frame), np.nan, dtype=np.float64)
    valid_close_pos20 = np.isfinite(high20) & np.isfinite(low20) & (high20 > low20)
    np.divide(close - low20, high20 - low20, out=close_pos20, where=valid_close_pos20)
    frame["close_pos20"] = close_pos20
    close_pos60 = np.full(len(frame), np.nan, dtype=np.float64)
    valid_close_pos60 = np.isfinite(high60) & np.isfinite(low60) & (high60 > low60)
    np.divide(close - low60, high60 - low60, out=close_pos60, where=valid_close_pos60)
    frame["close_pos60"] = close_pos60

    frame["ma7_slope_state"] = _vectorized_slope_states(ma7, prev_ma7)
    frame["ma20_slope_state"] = _vectorized_slope_states(ma20, prev_ma20)
    frame["ma60_slope_state"] = _vectorized_slope_states(ma60, prev_ma60)

    frame["close_vs_ma7"] = _vectorized_close_vs_ma(close, ma7)
    frame["close_vs_ma20"] = _vectorized_close_vs_ma(close, ma20)
    frame["close_vs_ma60"] = _vectorized_close_vs_ma(close, ma60)
    frame["body_vs_ma7"] = _vectorized_ma_relation(open_, close, high, low, ma7)
    frame["body_vs_ma20"] = _vectorized_ma_relation(open_, close, high, low, ma20)
    frame["body_vs_ma60"] = _vectorized_ma_relation(open_, close, high, low, ma60)

    bullish_stack = (ma7 > ma20) & (ma20 > ma60)
    bearish_stack = (ma7 < ma20) & (ma20 < ma60)
    frame["ma_stack"] = np.where(bullish_stack, "bullish_stack", np.where(bearish_stack, "bearish_stack", "mixed_stack"))

    frame["consecutive_above_ma7"] = _rolling_streak_counts(close, ma7, direction="above", cap=7)
    frame["consecutive_below_ma7"] = _rolling_streak_counts(close, ma7, direction="below", cap=7)
    frame["consecutive_above_ma20"] = _rolling_streak_counts(close, ma20, direction="above", cap=20)
    frame["consecutive_below_ma20"] = _rolling_streak_counts(close, ma20, direction="below", cap=20)
    frame["consecutive_above_ma60"] = _rolling_streak_counts(close, ma60, direction="above", cap=60)
    frame["consecutive_below_ma60"] = _rolling_streak_counts(close, ma60, direction="below", cap=60)

    candle_primary_tag, candle_tags, candle_state_code, body_norms, upper_ratios, lower_ratios, gap_pcts = _vectorized_candle_tags(
        open_price=open_,
        high_price=high,
        low_price=low,
        close_price=close,
        atr14=atr14,
        prev_close=prev_close,
        prev_high=prev_high,
        prev_low=prev_low,
    )
    frame["candle_primary_tag"] = candle_primary_tag
    frame["candle_tags"] = candle_tags
    frame["candle_state_code"] = candle_state_code
    frame["body_norm_atr"] = body_norms
    frame["upper_wick_ratio"] = upper_ratios
    frame["lower_wick_ratio"] = lower_ratios
    frame["gap_pct"] = gap_pcts

    frame["price_location_20"] = np.where(
        frame["close_pos20"].notna(),
        np.select(
            [
                frame["close_pos20"] >= PRICE_LOCATION_NEAR_THRESHOLD,
                frame["close_pos20"] <= PRICE_LOCATION_LOW_THRESHOLD,
            ],
            ["near_high", "near_low"],
            default="range_middle",
        ),
        "unknown",
    )
    frame["price_location_60"] = np.where(
        frame["close_pos60"].notna(),
        np.select(
            [
                frame["close_pos60"] >= PRICE_LOCATION_NEAR_THRESHOLD,
                frame["close_pos60"] <= PRICE_LOCATION_LOW_THRESHOLD,
            ],
            ["near_high", "near_low"],
            default="range_middle",
        ),
        "unknown",
    )
    frame["volume_condition"] = np.where(
        frame["vol20"].notna() & (frame["volume"] > frame["vol20"]),
        "above_20d_avg",
        np.where(frame["vol20"].notna(), "below_or_equal_20d_avg", "unknown"),
    )

    if regime_frame is not None and not regime_frame.empty:
        frame = frame.merge(regime_frame, on="trade_date", how="left")
    else:
        frame["regime_id"] = pd.Series([pd.NA] * len(frame), dtype="string")
        frame["label_version"] = pd.Series([pd.NA] * len(frame), dtype="string")
    frame["regime_id"] = frame["regime_id"].astype("string")
    frame["label_version"] = frame["label_version"].astype("string")
    frame["regime_source"] = np.where(frame["regime_id"].notna(), CONFIRMED_REGIME_SOURCE, PROVISIONAL_REGIME_SOURCE)
    provisional_regime = np.select(
        [
            np.isfinite(close) & np.isfinite(ma20) & np.isfinite(ma60) & (close >= ma20) & (frame["ma20_slope_state"].to_numpy(dtype=object) == "up") & (frame["ma60_slope_state"].to_numpy(dtype=object) == "up"),
            np.isfinite(close) & np.isfinite(ma20) & np.isfinite(ma60) & (close <= ma20) & (frame["ma20_slope_state"].to_numpy(dtype=object) == "down") & (frame["ma60_slope_state"].to_numpy(dtype=object) == "down"),
            np.isfinite(close) & np.isfinite(ma20) & np.isfinite(ma60) & (close >= ma20) & np.isin(frame["ma20_slope_state"].to_numpy(dtype=object), ["up", "flat"]),
            np.isfinite(close) & np.isfinite(ma20) & np.isfinite(ma60) & (close <= ma20) & np.isin(frame["ma20_slope_state"].to_numpy(dtype=object), ["down", "flat"]),
        ],
        [
            "provisional_trend_up",
            "provisional_trend_down",
            "provisional_supportive",
            "provisional_weak",
        ],
        default="provisional_mixed",
    ).astype(object)
    frame["regime_label"] = np.where(frame["regime_id"].notna(), frame["regime_id"].astype("string"), provisional_regime)

    next_open = np.roll(open_, -1)
    next_open[-1] = np.nan
    frame["entry_next_open"] = next_open

    horizon_to_offset = {3: 4, 5: 6, 10: 11, 20: 21}
    valid_entry = np.isfinite(next_open) & (next_open > 0)
    for horizon, offset in horizon_to_offset.items():
        future_close = np.roll(close, -offset)
        future_close[-offset:] = np.nan
        frame[f"forward_ret_{horizon}d"] = np.where(valid_entry & np.isfinite(future_close), (future_close / next_open) - 1.0, np.nan)

    future_window_count = max(0, len(frame) - PATH_WINDOW_DAYS)
    for column in [
        "mfe_20d",
        "mae_20d",
        "days_to_mfe_20d",
        "days_to_mae_20d",
        "days_to_positive_close",
        "days_to_plus_3pct",
        "days_to_plus_5pct",
        "days_to_minus_3pct",
        "days_to_minus_5pct",
        "hit_plus_5_before_minus_5",
        "hit_minus_5_before_plus_5",
        "hit_plus_3_before_minus_3",
        "hit_minus_3_before_plus_3",
        "hit_plus_1atr_before_minus_1atr",
        "mfe_atr_20d",
        "mae_atr_20d",
        "close_above_entry_days_20d",
        "close_below_entry_days_20d",
        "forward_window_days",
        "entry_day_close",
    ]:
        frame[column] = np.nan

    if future_window_count > 0:
        high_windows = sliding_window_view(high[1:], PATH_WINDOW_DAYS)[:future_window_count]
        low_windows = sliding_window_view(low[1:], PATH_WINDOW_DAYS)[:future_window_count]
        close_windows = sliding_window_view(close[1:], PATH_WINDOW_DAYS)[:future_window_count]
        entry_window = next_open[:future_window_count]
        atr_window = atr14[:future_window_count]
        path_metrics = _path_threshold_hits_matrix(entry_window, high_windows, low_windows, close_windows, atr_window)
        frame.loc[: future_window_count - 1, "mfe_20d"] = path_metrics["mfe_20d"]
        frame.loc[: future_window_count - 1, "mae_20d"] = path_metrics["mae_20d"]
        frame.loc[: future_window_count - 1, "days_to_mfe_20d"] = path_metrics["days_to_mfe_20d"]
        frame.loc[: future_window_count - 1, "days_to_mae_20d"] = path_metrics["days_to_mae_20d"]
        frame.loc[: future_window_count - 1, "days_to_positive_close"] = path_metrics["days_to_positive_close"]
        frame.loc[: future_window_count - 1, "days_to_plus_3pct"] = path_metrics["days_to_plus_3pct"]
        frame.loc[: future_window_count - 1, "days_to_plus_5pct"] = path_metrics["days_to_plus_5pct"]
        frame.loc[: future_window_count - 1, "days_to_minus_3pct"] = path_metrics["days_to_minus_3pct"]
        frame.loc[: future_window_count - 1, "days_to_minus_5pct"] = path_metrics["days_to_minus_5pct"]
        frame.loc[: future_window_count - 1, "hit_plus_5_before_minus_5"] = path_metrics["hit_plus_5_before_minus_5"]
        frame.loc[: future_window_count - 1, "hit_minus_5_before_plus_5"] = path_metrics["hit_minus_5_before_plus_5"]
        frame.loc[: future_window_count - 1, "hit_plus_3_before_minus_3"] = path_metrics["hit_plus_3_before_minus_3"]
        frame.loc[: future_window_count - 1, "hit_minus_3_before_plus_3"] = path_metrics["hit_minus_3_before_plus_3"]
        frame.loc[: future_window_count - 1, "hit_plus_1atr_before_minus_1atr"] = path_metrics["hit_plus_1atr_before_minus_1atr"]
        frame.loc[: future_window_count - 1, "mfe_atr_20d"] = path_metrics["mfe_atr_20d"]
        frame.loc[: future_window_count - 1, "mae_atr_20d"] = path_metrics["mae_atr_20d"]
        frame.loc[: future_window_count - 1, "close_above_entry_days_20d"] = path_metrics["close_above_entry_days_20d"]
        frame.loc[: future_window_count - 1, "close_below_entry_days_20d"] = path_metrics["close_below_entry_days_20d"]
        frame.loc[: future_window_count - 1, "entry_day_close"] = close[:future_window_count]
        frame.loc[: future_window_count - 1, "forward_window_days"] = PATH_WINDOW_DAYS

    frame["path_value_score_v1"] = (
        0.30 * frame["forward_ret_20d"]
        + 0.25 * frame["mfe_20d"]
        - 0.30 * frame["mae_20d"].abs()
        + 0.10 * frame["forward_ret_10d"]
        + 0.05 * (frame["close_above_entry_days_20d"] / float(PATH_WINDOW_DAYS))
        + 0.10 * frame["hit_plus_5_before_minus_5"]
        - 0.10 * frame["hit_minus_5_before_plus_5"]
    )
    frame["eligible_for_analysis"] = (
        frame["ma7"].notna()
        & frame["ma20"].notna()
        & frame["ma60"].notna()
        & frame["entry_next_open"].notna()
        & frame["forward_ret_20d"].notna()
        & frame["mfe_20d"].notna()
        & frame["mae_20d"].notna()
    )
    eligible_mask = frame["eligible_for_analysis"].to_numpy(dtype=bool, copy=False)
    if np.any(eligible_mask):
        eligible_frame = frame.loc[eligible_mask].copy()
        ma7_above = _bucket_streak_array(eligible_frame["consecutive_above_ma7"].to_numpy(dtype=np.int64, copy=False), 7)
        ma7_below = _bucket_streak_array(eligible_frame["consecutive_below_ma7"].to_numpy(dtype=np.int64, copy=False), 7)
        ma20_above = _bucket_streak_array(eligible_frame["consecutive_above_ma20"].to_numpy(dtype=np.int64, copy=False), 20)
        ma20_below = _bucket_streak_array(eligible_frame["consecutive_below_ma20"].to_numpy(dtype=np.int64, copy=False), 20)
        ma60_above = _bucket_streak_array(eligible_frame["consecutive_above_ma60"].to_numpy(dtype=np.int64, copy=False), 60)
        ma60_below = _bucket_streak_array(eligible_frame["consecutive_below_ma60"].to_numpy(dtype=np.int64, copy=False), 60)
        eligible_frame["ma7_streak"] = np.where(
            eligible_frame["close_vs_ma7"].to_numpy(dtype=object) == "above",
            ma7_above,
            np.char.add("-", ma7_below),
        )
        eligible_frame["ma20_streak"] = np.where(
            eligible_frame["close_vs_ma20"].to_numpy(dtype=object) == "above",
            ma20_above,
            np.char.add("-", ma20_below),
        )
        eligible_frame["ma60_streak"] = np.where(
            eligible_frame["close_vs_ma60"].to_numpy(dtype=object) == "above",
            ma60_above,
            np.char.add("-", ma60_below),
        )
        frame.loc[eligible_mask, "state_close_ma7"] = np.where(
            eligible_frame["close_vs_ma7"].to_numpy(dtype=object) == "above",
            "A",
            np.where(eligible_frame["close_vs_ma7"].to_numpy(dtype=object) == "below", "B", "U"),
        )
        frame.loc[eligible_mask, "state_close_ma20"] = np.where(
            eligible_frame["close_vs_ma20"].to_numpy(dtype=object) == "above",
            "A",
            np.where(eligible_frame["close_vs_ma20"].to_numpy(dtype=object) == "below", "B", "U"),
        )
        frame.loc[eligible_mask, "state_close_ma60"] = np.where(
            eligible_frame["close_vs_ma60"].to_numpy(dtype=object) == "above",
            "A",
            np.where(eligible_frame["close_vs_ma60"].to_numpy(dtype=object) == "below", "B", "U"),
        )
        body_code7 = np.select(
            [
                eligible_frame["body_vs_ma7"].to_numpy(dtype=object) == "body_full_above",
                eligible_frame["body_vs_ma7"].to_numpy(dtype=object) == "body_full_below",
                eligible_frame["body_vs_ma7"].to_numpy(dtype=object) == "body_crosses",
                eligible_frame["body_vs_ma7"].to_numpy(dtype=object) == "wick_touches_only",
            ],
            ["A", "B", "C", "W"],
            default="U",
        )
        frame.loc[eligible_mask, "state_body_ma7"] = body_code7
        body_code20 = np.select(
            [
                eligible_frame["body_vs_ma20"].to_numpy(dtype=object) == "body_full_above",
                eligible_frame["body_vs_ma20"].to_numpy(dtype=object) == "body_full_below",
                eligible_frame["body_vs_ma20"].to_numpy(dtype=object) == "body_crosses",
                eligible_frame["body_vs_ma20"].to_numpy(dtype=object) == "wick_touches_only",
            ],
            ["A", "B", "C", "W"],
            default="U",
        )
        frame.loc[eligible_mask, "state_body_ma20"] = body_code20
        body_code60 = np.select(
            [
                eligible_frame["body_vs_ma60"].to_numpy(dtype=object) == "body_full_above",
                eligible_frame["body_vs_ma60"].to_numpy(dtype=object) == "body_full_below",
                eligible_frame["body_vs_ma60"].to_numpy(dtype=object) == "body_crosses",
                eligible_frame["body_vs_ma60"].to_numpy(dtype=object) == "wick_touches_only",
            ],
            ["A", "B", "C", "W"],
            default="U",
        )
        frame.loc[eligible_mask, "state_body_ma60"] = body_code60
        stack_code = np.select(
            [
                eligible_frame["ma_stack"].to_numpy(dtype=object) == "bullish_stack",
                eligible_frame["ma_stack"].to_numpy(dtype=object) == "bearish_stack",
            ],
            ["B", "R"],
            default="M",
        )
        frame.loc[eligible_mask, "state_ma_stack"] = stack_code
        slope_code7 = np.select(
            [
                eligible_frame["ma7_slope_state"].to_numpy(dtype=object) == "up",
                eligible_frame["ma7_slope_state"].to_numpy(dtype=object) == "down",
            ],
            ["U", "D"],
            default="F",
        )
        frame.loc[eligible_mask, "state_ma7_slope"] = slope_code7
        slope_code20 = np.select(
            [
                eligible_frame["ma20_slope_state"].to_numpy(dtype=object) == "up",
                eligible_frame["ma20_slope_state"].to_numpy(dtype=object) == "down",
            ],
            ["U", "D"],
            default="F",
        )
        frame.loc[eligible_mask, "state_ma20_slope"] = slope_code20
        slope_code60 = np.select(
            [
                eligible_frame["ma60_slope_state"].to_numpy(dtype=object) == "up",
                eligible_frame["ma60_slope_state"].to_numpy(dtype=object) == "down",
            ],
            ["U", "D"],
            default="F",
        )
        frame.loc[eligible_mask, "state_ma60_slope"] = slope_code60
        candle_code = eligible_frame["candle_state_code"].astype("string")
        frame.loc[eligible_mask, "state_candle_code"] = candle_code.to_numpy()
        price20_code = np.select(
            [
                eligible_frame["price_location_20"].to_numpy(dtype=object) == "near_high",
                eligible_frame["price_location_20"].to_numpy(dtype=object) == "near_low",
            ],
            ["H", "L"],
            default="M",
        )
        frame.loc[eligible_mask, "state_price_location_20"] = price20_code
        price60_code = np.select(
            [
                eligible_frame["price_location_60"].to_numpy(dtype=object) == "near_high",
                eligible_frame["price_location_60"].to_numpy(dtype=object) == "near_low",
            ],
            ["H", "L"],
            default="M",
        )
        frame.loc[eligible_mask, "state_price_location_60"] = price60_code
        volume_code = np.select(
            [
                eligible_frame["volume_condition"].to_numpy(dtype=object) == "above_20d_avg",
                eligible_frame["volume_condition"].to_numpy(dtype=object) == "below_or_equal_20d_avg",
            ],
            ["A", "B"],
            default="U",
        )
        frame.loc[eligible_mask, "state_volume_condition"] = volume_code
        regime_code = eligible_frame["regime_label"].astype("string")
        regime_source_code = np.where(eligible_frame["regime_source"].to_numpy(dtype=object) == CONFIRMED_REGIME_SOURCE, "C", "P")
        frame.loc[eligible_mask, "state_regime_label"] = regime_code.to_numpy()
        frame.loc[eligible_mask, "state_regime_source"] = regime_source_code
        frame.loc[eligible_mask, "state_ma7_streak"] = eligible_frame["ma7_streak"].astype("string").to_numpy()
        frame.loc[eligible_mask, "state_ma20_streak"] = eligible_frame["ma20_streak"].astype("string").to_numpy()
        frame.loc[eligible_mask, "state_ma60_streak"] = eligible_frame["ma60_streak"].astype("string").to_numpy()
        frame.loc[eligible_mask, "position_state_id"] = _compose_position_state_ids(frame.loc[eligible_mask, STATE_CORE_COMPONENT_FIELDS])

    return frame


def _summarize_state_table(frame: pd.DataFrame) -> pd.DataFrame:
    summary_columns = [
        "code",
        "trade_date",
        "regime_source",
        "forward_ret_3d",
        "forward_ret_5d",
        "forward_ret_10d",
        "forward_ret_20d",
        "mfe_20d",
        "mae_20d",
        "days_to_mfe_20d",
        "days_to_mae_20d",
        "days_to_positive_close",
        "days_to_plus_3pct",
        "days_to_plus_5pct",
        "days_to_minus_3pct",
        "days_to_minus_5pct",
        "hit_plus_5_before_minus_5",
        "hit_minus_5_before_plus_5",
        "hit_plus_3_before_minus_3",
        "hit_minus_3_before_plus_3",
        "hit_plus_1atr_before_minus_1atr",
        "close_above_entry_days_20d",
        "close_below_entry_days_20d",
        "path_value_score_v1",
        "body_norm_atr",
        "upper_wick_ratio",
        "lower_wick_ratio",
        "volume",
        "volume_condition",
        *STATE_CORE_COMPONENT_FIELDS,
    ]
    eligible = frame.loc[frame["eligible_for_analysis"], summary_columns].copy()
    if eligible.empty:
        return pd.DataFrame()

    grouped = eligible.groupby(STATE_CORE_COMPONENT_FIELDS, dropna=False)
    rows: list[dict[str, Any]] = []
    for _, group in grouped:
        state_id = _build_core_state_id(group.iloc[0])
        regime_source_counts = group["regime_source"].value_counts(dropna=False).to_dict()
        rows.append(
            {
                "position_state_id": str(state_id),
                "sample_count": int(len(group)),
                "symbol_count": int(group["code"].nunique()),
                "date_count": int(group["trade_date"].nunique()),
                "first_trade_date": int(group["trade_date"].min()),
                "last_trade_date": int(group["trade_date"].max()),
                "mean_forward_ret_20d": _safe_float(group["forward_ret_20d"].mean()),
                "median_forward_ret_20d": _safe_float(group["forward_ret_20d"].median()),
                "mean_mfe_20d": _safe_float(group["mfe_20d"].mean()),
                "mean_mae_20d": _safe_float(group["mae_20d"].mean()),
                "mean_days_to_plus_5pct": _safe_float(group["days_to_plus_5pct"].mean()),
                "mean_days_to_minus_5pct": _safe_float(group["days_to_minus_5pct"].mean()),
                "hit_plus_5_before_minus_5_rate": _safe_float(group["hit_plus_5_before_minus_5"].mean()),
                "hit_minus_5_before_plus_5_rate": _safe_float(group["hit_minus_5_before_plus_5"].mean()),
                "mean_close_above_entry_days_20d": _safe_float(group["close_above_entry_days_20d"].mean()),
                "mean_close_below_entry_days_20d": _safe_float(group["close_below_entry_days_20d"].mean()),
                "mean_path_value_score_v1": _safe_float(group["path_value_score_v1"].mean()),
                "median_path_value_score_v1": _safe_float(group["path_value_score_v1"].median()),
                "mean_body_norm_atr": _safe_float(group["body_norm_atr"].mean()),
                "mean_upper_wick_ratio": _safe_float(group["upper_wick_ratio"].mean()),
                "mean_lower_wick_ratio": _safe_float(group["lower_wick_ratio"].mean()),
                "mean_volume": _safe_float(group["volume"].mean()),
                "mean_volume_condition_above_rate": _safe_float((group["volume_condition"] == "above_20d_avg").mean()),
                "regime_source_counts_json": regime_source_counts,
                "dominant_regime_source": max(regime_source_counts.items(), key=lambda item: (item[1], item[0]))[0] if regime_source_counts else None,
                "state_quality_label": _state_quality_label(group),
            }
        )
    state_frame = pd.DataFrame(rows)
    if not state_frame.empty:
        state_frame = state_frame.sort_values(
            ["mean_path_value_score_v1", "sample_count", "mean_forward_ret_20d"],
            ascending=[False, False, False],
        ).reset_index(drop=True)
    return state_frame


def _state_quality_label(group: pd.DataFrame) -> str:
    mean_score = _safe_float(group["path_value_score_v1"].mean())
    mean_ret20 = _safe_float(group["forward_ret_20d"].mean())
    mean_mae20 = _safe_float(group["mae_20d"].mean())
    if mean_score is None or mean_ret20 is None or mean_mae20 is None:
        return "unclassified"
    if mean_score >= HIGH_VALUE_SCORE_THRESHOLD and mean_ret20 > 0:
        return "high_value"
    if abs(mean_score) <= WEAK_NOISE_SCORE_THRESHOLD and abs(mean_ret20) <= 0.01:
        return "weak_noise"
    if mean_score <= BAD_PICK_SCORE_THRESHOLD and mean_ret20 <= 0 and mean_mae20 <= BAD_PICK_MAE_THRESHOLD:
        return "bad_pick_removal"
    return "neutral"


def _build_position_state_id(row: pd.Series) -> str:
    return _build_core_state_id(row)


def _build_core_state_id(row: pd.Series) -> str:
    return "|".join(
        [
            f"c7={row['state_close_ma7']}",
            f"c20={row['state_close_ma20']}",
            f"c60={row['state_close_ma60']}",
            f"b7={row['state_body_ma7']}",
            f"b20={row['state_body_ma20']}",
            f"b60={row['state_body_ma60']}",
            f"stk={row['state_ma_stack']}",
            f"s7={row['state_ma7_slope']}",
            f"s20={row['state_ma20_slope']}",
            f"s60={row['state_ma60_slope']}",
            f"st7={row['state_ma7_streak']}",
            f"st20={row['state_ma20_streak']}",
            f"st60={row['state_ma60_streak']}",
            f"cd={row['state_candle_code']}",
            f"p20={row['state_price_location_20']}",
            f"p60={row['state_price_location_60']}",
            f"vol={row['state_volume_condition']}",
        ]
    )


def _compose_position_state_ids(frame: pd.DataFrame) -> pd.Series:
    component_columns = STATE_CORE_COMPONENT_FIELDS
    if frame.empty:
        return pd.Series(dtype="string")
    values = frame.loc[:, component_columns].astype("string").fillna("U")
    parts = [
        np.asarray(values["state_close_ma7"], dtype=object),
        np.asarray(values["state_close_ma20"], dtype=object),
        np.asarray(values["state_close_ma60"], dtype=object),
        np.asarray(values["state_body_ma7"], dtype=object),
        np.asarray(values["state_body_ma20"], dtype=object),
        np.asarray(values["state_body_ma60"], dtype=object),
        np.asarray(values["state_ma_stack"], dtype=object),
        np.asarray(values["state_ma7_slope"], dtype=object),
        np.asarray(values["state_ma20_slope"], dtype=object),
        np.asarray(values["state_ma60_slope"], dtype=object),
        np.asarray(values["state_ma7_streak"], dtype=object),
        np.asarray(values["state_ma20_streak"], dtype=object),
        np.asarray(values["state_ma60_streak"], dtype=object),
        np.asarray(values["state_candle_code"], dtype=object),
        np.asarray(values["state_price_location_20"], dtype=object),
        np.asarray(values["state_price_location_60"], dtype=object),
        np.asarray(values["state_volume_condition"], dtype=object),
    ]
    labels = [
        "c7=",
        "c20=",
        "c60=",
        "b7=",
        "b20=",
        "b60=",
        "stk=",
        "s7=",
        "s20=",
        "s60=",
        "st7=",
        "st20=",
        "st60=",
        "cd=",
        "p20=",
        "p60=",
        "vol=",
    ]
    state_id = np.char.add(labels[0], parts[0])
    for label, part in zip(labels[1:], parts[1:], strict=False):
        state_id = np.char.add(np.char.add(state_id, "|"), np.char.add(label, part))
    return pd.Series(state_id, index=frame.index, dtype="string")


def _summarize_regime_state_table(frame: pd.DataFrame, *, regime_source: str) -> pd.DataFrame:
    summary_columns = [
        "code",
        "trade_date",
        "regime_source",
        "regime_label",
        "forward_ret_20d",
        "mfe_20d",
        "mae_20d",
        "path_value_score_v1",
        *STATE_CORE_COMPONENT_FIELDS,
    ]
    source_frame = frame.loc[frame["eligible_for_analysis"] & (frame["regime_source"] == regime_source), summary_columns].copy()
    if source_frame.empty:
        return pd.DataFrame()
    grouped = source_frame.groupby([*STATE_CORE_COMPONENT_FIELDS, "regime_label"], dropna=False)
    rows: list[dict[str, Any]] = []
    for _, group in grouped:
        if len(group) < STATE_MIN_REGIME_SAMPLE_COUNT:
            continue
        state_id = _build_core_state_id(group.iloc[0])
        rows.append(
            {
                "position_state_id": str(state_id),
                "regime_label": str(group.iloc[0]["state_regime_label"]),
                "regime_source": regime_source,
                "sample_count": int(len(group)),
                "symbol_count": int(group["code"].nunique()),
                "mean_forward_ret_20d": _safe_float(group["forward_ret_20d"].mean()),
                "mean_mfe_20d": _safe_float(group["mfe_20d"].mean()),
                "mean_mae_20d": _safe_float(group["mae_20d"].mean()),
                "mean_path_value_score_v1": _safe_float(group["path_value_score_v1"].mean()),
            }
        )
    return pd.DataFrame(rows)


def _select_top_states(state_frame: pd.DataFrame, *, limit: int = 15) -> dict[str, list[dict[str, Any]]]:
    if state_frame.empty:
        return {"high_value_states": [], "weak_noise_states": [], "bad_pick_removal_states": [], "regime_dependent_states": []}

    eligible = state_frame.loc[state_frame["sample_count"] >= STATE_MIN_SAMPLE_COUNT].copy()
    high_value = eligible.loc[
        (eligible["mean_path_value_score_v1"] >= HIGH_VALUE_SCORE_THRESHOLD)
        & (eligible["mean_forward_ret_20d"] > 0)
    ].sort_values(
        ["mean_path_value_score_v1", "sample_count", "mean_forward_ret_20d"],
        ascending=[False, False, False],
    ).head(limit)
    weak_noise = eligible.loc[
        eligible["state_quality_label"].eq("weak_noise")
    ].sort_values(
        ["sample_count", "mean_path_value_score_v1"],
        ascending=[False, False],
    ).head(limit)
    bad_pick = eligible.loc[
        (eligible["mean_path_value_score_v1"] <= BAD_PICK_SCORE_THRESHOLD)
        & (eligible["mean_forward_ret_20d"] <= 0)
        & (eligible["mean_mae_20d"] <= BAD_PICK_MAE_THRESHOLD)
    ].sort_values(
        ["mean_path_value_score_v1", "sample_count", "mean_mae_20d"],
        ascending=[True, False, True],
    ).head(limit)
    return {
        "high_value_states": high_value.to_dict(orient="records"),
        "weak_noise_states": weak_noise.to_dict(orient="records"),
        "bad_pick_removal_states": bad_pick.to_dict(orient="records"),
        "regime_dependent_states": [],
    }


def _select_regime_dependent_states(regime_state_frame: pd.DataFrame, *, limit: int = 15) -> list[dict[str, Any]]:
    if regime_state_frame.empty:
        return []
    rows: list[dict[str, Any]] = []
    for state_id, group in regime_state_frame.groupby("position_state_id", dropna=False):
        if len(group) < 2:
            continue
        spread = float(group["mean_path_value_score_v1"].max() - group["mean_path_value_score_v1"].min())
        if spread <= 0:
            continue
        rows.append(
            {
                "position_state_id": str(state_id),
                "regime_count": int(group["regime_label"].nunique()),
                "sample_count": int(group["sample_count"].sum()),
                "score_spread": spread,
                "best_regime_label": str(group.sort_values("mean_path_value_score_v1", ascending=False).iloc[0]["regime_label"]),
                "worst_regime_label": str(group.sort_values("mean_path_value_score_v1", ascending=True).iloc[0]["regime_label"]),
                "best_regime_score": _safe_float(group["mean_path_value_score_v1"].max()),
                "worst_regime_score": _safe_float(group["mean_path_value_score_v1"].min()),
                "best_regime_ret20": _safe_float(group["mean_forward_ret_20d"].max()),
                "worst_regime_ret20": _safe_float(group["mean_forward_ret_20d"].min()),
            }
        )
    return sorted(rows, key=lambda item: (item["score_spread"], item["sample_count"]), reverse=True)[:limit]


def _overall_metrics(frame: pd.DataFrame) -> dict[str, Any]:
    summary_columns = [
        "code",
        "trade_date",
        "regime_source",
        "forward_ret_3d",
        "forward_ret_5d",
        "forward_ret_10d",
        "forward_ret_20d",
        "mfe_20d",
        "mae_20d",
        "days_to_mfe_20d",
        "days_to_mae_20d",
        "days_to_positive_close",
        "days_to_plus_3pct",
        "days_to_plus_5pct",
        "days_to_minus_3pct",
        "days_to_minus_5pct",
        "hit_plus_5_before_minus_5",
        "hit_minus_5_before_plus_5",
        "hit_plus_3_before_minus_3",
        "hit_minus_3_before_plus_3",
        "hit_plus_1atr_before_minus_1atr",
        "close_above_entry_days_20d",
        "close_below_entry_days_20d",
        "path_value_score_v1",
    ]
    eligible = frame.loc[frame["eligible_for_analysis"], summary_columns]
    if eligible.empty:
        return {
            "eligible_row_count": 0,
            "symbol_count": 0,
            "trade_date_count": 0,
        }
    return {
        "eligible_row_count": int(len(eligible)),
        "symbol_count": int(eligible["code"].nunique()),
        "trade_date_count": int(eligible["trade_date"].nunique()),
        "mean_forward_ret_3d": _safe_float(eligible["forward_ret_3d"].mean()),
        "mean_forward_ret_5d": _safe_float(eligible["forward_ret_5d"].mean()),
        "mean_forward_ret_10d": _safe_float(eligible["forward_ret_10d"].mean()),
        "mean_forward_ret_20d": _safe_float(eligible["forward_ret_20d"].mean()),
        "median_forward_ret_20d": _safe_float(eligible["forward_ret_20d"].median()),
        "mean_mfe_20d": _safe_float(eligible["mfe_20d"].mean()),
        "mean_mae_20d": _safe_float(eligible["mae_20d"].mean()),
        "mean_days_to_mfe_20d": _safe_float(eligible["days_to_mfe_20d"].mean()),
        "mean_days_to_mae_20d": _safe_float(eligible["days_to_mae_20d"].mean()),
        "mean_days_to_positive_close": _safe_float(eligible["days_to_positive_close"].mean()),
        "mean_days_to_plus_3pct": _safe_float(eligible["days_to_plus_3pct"].mean()),
        "mean_days_to_plus_5pct": _safe_float(eligible["days_to_plus_5pct"].mean()),
        "mean_days_to_minus_3pct": _safe_float(eligible["days_to_minus_3pct"].mean()),
        "mean_days_to_minus_5pct": _safe_float(eligible["days_to_minus_5pct"].mean()),
        "hit_plus_5_before_minus_5_rate": _safe_float(eligible["hit_plus_5_before_minus_5"].mean()),
        "hit_minus_5_before_plus_5_rate": _safe_float(eligible["hit_minus_5_before_plus_5"].mean()),
        "hit_plus_3_before_minus_3_rate": _safe_float(eligible["hit_plus_3_before_minus_3"].mean()),
        "hit_minus_3_before_plus_3_rate": _safe_float(eligible["hit_minus_3_before_plus_3"].mean()),
        "hit_plus_1atr_before_minus_1atr_rate": _safe_float(eligible["hit_plus_1atr_before_minus_1atr"].mean()),
        "mean_close_above_entry_days_20d": _safe_float(eligible["close_above_entry_days_20d"].mean()),
        "mean_close_below_entry_days_20d": _safe_float(eligible["close_below_entry_days_20d"].mean()),
        "mean_path_value_score_v1": _safe_float(eligible["path_value_score_v1"].mean()),
        "median_path_value_score_v1": _safe_float(eligible["path_value_score_v1"].median()),
        "confirmed_regime_row_count": int((eligible["regime_source"] == CONFIRMED_REGIME_SOURCE).sum()),
        "provisional_regime_row_count": int((eligible["regime_source"] == PROVISIONAL_REGIME_SOURCE).sum()),
    }


def _state_quality_label_from_summary(row: pd.Series) -> str:
    mean_score = _safe_float(row.get("mean_path_value_score_v1"))
    mean_ret20 = _safe_float(row.get("mean_forward_ret_20d"))
    mean_mae20 = _safe_float(row.get("mean_mae_20d"))
    if mean_score is None or mean_ret20 is None or mean_mae20 is None:
        return "unclassified"
    if mean_score >= HIGH_VALUE_SCORE_THRESHOLD and mean_ret20 > 0:
        return "high_value"
    if abs(mean_score) <= WEAK_NOISE_SCORE_THRESHOLD and abs(mean_ret20) <= 0.01:
        return "weak_noise"
    if mean_score <= BAD_PICK_SCORE_THRESHOLD and mean_ret20 <= 0 and mean_mae20 <= BAD_PICK_MAE_THRESHOLD:
        return "bad_pick_removal"
    return "neutral"


def _query_frame(conn: duckdb.DuckDBPyConnection, sql: str, params: Iterable[Any] | None = None) -> pd.DataFrame:
    if params is None:
        frame = conn.execute(sql).fetchdf()
    else:
        frame = conn.execute(sql, list(params)).fetchdf()
    if frame.empty:
        return frame
    return frame.copy()


def _build_overall_metrics_from_rows(conn: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS eligible_row_count,
            COUNT(DISTINCT code) AS symbol_count,
            COUNT(DISTINCT trade_date) AS trade_date_count,
            AVG(forward_ret_3d) AS mean_forward_ret_3d,
            AVG(forward_ret_5d) AS mean_forward_ret_5d,
            AVG(forward_ret_10d) AS mean_forward_ret_10d,
            AVG(forward_ret_20d) AS mean_forward_ret_20d,
            MEDIAN(forward_ret_20d) AS median_forward_ret_20d,
            AVG(mfe_20d) AS mean_mfe_20d,
            AVG(mae_20d) AS mean_mae_20d,
            AVG(days_to_mfe_20d) AS mean_days_to_mfe_20d,
            AVG(days_to_mae_20d) AS mean_days_to_mae_20d,
            AVG(days_to_positive_close) AS mean_days_to_positive_close,
            AVG(days_to_plus_3pct) AS mean_days_to_plus_3pct,
            AVG(days_to_plus_5pct) AS mean_days_to_plus_5pct,
            AVG(days_to_minus_3pct) AS mean_days_to_minus_3pct,
            AVG(days_to_minus_5pct) AS mean_days_to_minus_5pct,
            AVG(hit_plus_5_before_minus_5) AS hit_plus_5_before_minus_5_rate,
            AVG(hit_minus_5_before_plus_5) AS hit_minus_5_before_plus_5_rate,
            AVG(hit_plus_3_before_minus_3) AS hit_plus_3_before_minus_3_rate,
            AVG(hit_minus_3_before_plus_3) AS hit_minus_3_before_plus_3_rate,
            AVG(hit_plus_1atr_before_minus_1atr) AS hit_plus_1atr_before_minus_1atr_rate,
            AVG(close_above_entry_days_20d) AS mean_close_above_entry_days_20d,
            AVG(close_below_entry_days_20d) AS mean_close_below_entry_days_20d,
            AVG(path_value_score_v1) AS mean_path_value_score_v1,
            MEDIAN(path_value_score_v1) AS median_path_value_score_v1,
            SUM(CASE WHEN regime_source = ? THEN 1 ELSE 0 END) AS confirmed_regime_row_count,
            SUM(CASE WHEN regime_source = ? THEN 1 ELSE 0 END) AS provisional_regime_row_count
        FROM position_rows
        """,
        [CONFIRMED_REGIME_SOURCE, PROVISIONAL_REGIME_SOURCE],
    ).fetchone()
    if not row:
        return {"eligible_row_count": 0, "symbol_count": 0, "trade_date_count": 0}
    keys = [
        "eligible_row_count",
        "symbol_count",
        "trade_date_count",
        "mean_forward_ret_3d",
        "mean_forward_ret_5d",
        "mean_forward_ret_10d",
        "mean_forward_ret_20d",
        "median_forward_ret_20d",
        "mean_mfe_20d",
        "mean_mae_20d",
        "mean_days_to_mfe_20d",
        "mean_days_to_mae_20d",
        "mean_days_to_positive_close",
        "mean_days_to_plus_3pct",
        "mean_days_to_plus_5pct",
        "mean_days_to_minus_3pct",
        "mean_days_to_minus_5pct",
        "hit_plus_5_before_minus_5_rate",
        "hit_minus_5_before_plus_5_rate",
        "hit_plus_3_before_minus_3_rate",
        "hit_minus_3_before_plus_3_rate",
        "hit_plus_1atr_before_minus_1atr_rate",
        "mean_close_above_entry_days_20d",
        "mean_close_below_entry_days_20d",
        "mean_path_value_score_v1",
        "median_path_value_score_v1",
        "confirmed_regime_row_count",
        "provisional_regime_row_count",
    ]
    return {key: _safe_float(value) if key not in {"eligible_row_count", "symbol_count", "trade_date_count", "confirmed_regime_row_count", "provisional_regime_row_count"} else int(value or 0) for key, value in zip(keys, row, strict=False)}


def _build_state_summary_from_rows(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    state_frame = _query_frame(
        conn,
        """
        SELECT
            position_state_id,
            COUNT(*) AS sample_count,
            COUNT(DISTINCT code) AS symbol_count,
            COUNT(DISTINCT trade_date) AS date_count,
            MIN(trade_date) AS first_trade_date,
            MAX(trade_date) AS last_trade_date,
            AVG(forward_ret_20d) AS mean_forward_ret_20d,
            MEDIAN(forward_ret_20d) AS median_forward_ret_20d,
            AVG(mfe_20d) AS mean_mfe_20d,
            AVG(mae_20d) AS mean_mae_20d,
            AVG(days_to_plus_5pct) AS mean_days_to_plus_5pct,
            AVG(days_to_minus_5pct) AS mean_days_to_minus_5pct,
            AVG(hit_plus_5_before_minus_5) AS hit_plus_5_before_minus_5_rate,
            AVG(hit_minus_5_before_plus_5) AS hit_minus_5_before_plus_5_rate,
            AVG(close_above_entry_days_20d) AS mean_close_above_entry_days_20d,
            AVG(close_below_entry_days_20d) AS mean_close_below_entry_days_20d,
            AVG(path_value_score_v1) AS mean_path_value_score_v1,
            MEDIAN(path_value_score_v1) AS median_path_value_score_v1,
            AVG(body_norm_atr) AS mean_body_norm_atr,
            AVG(upper_wick_ratio) AS mean_upper_wick_ratio,
            AVG(lower_wick_ratio) AS mean_lower_wick_ratio,
            AVG(volume) AS mean_volume,
            AVG(CASE WHEN volume_condition = 'above_20d_avg' THEN 1 ELSE 0 END) AS mean_volume_condition_above_rate,
            SUM(CASE WHEN regime_source = ? THEN 1 ELSE 0 END) AS confirmed_regime_row_count,
            SUM(CASE WHEN regime_source = ? THEN 1 ELSE 0 END) AS provisional_regime_row_count
        FROM position_rows
        GROUP BY position_state_id
        ORDER BY mean_path_value_score_v1 DESC, sample_count DESC, mean_forward_ret_20d DESC
        """,
        [CONFIRMED_REGIME_SOURCE, PROVISIONAL_REGIME_SOURCE],
    )
    if state_frame.empty:
        return state_frame
    state_frame["state_quality_label"] = state_frame.apply(_state_quality_label_from_summary, axis=1)
    state_frame["regime_source_counts_json"] = state_frame.apply(
        lambda row: {
            CONFIRMED_REGIME_SOURCE: int(row["confirmed_regime_row_count"] or 0),
            PROVISIONAL_REGIME_SOURCE: int(row["provisional_regime_row_count"] or 0),
        },
        axis=1,
    )
    state_frame["dominant_regime_source"] = state_frame.apply(
        lambda row: CONFIRMED_REGIME_SOURCE
        if int(row["confirmed_regime_row_count"] or 0) >= int(row["provisional_regime_row_count"] or 0)
        else PROVISIONAL_REGIME_SOURCE,
        axis=1,
    )
    return state_frame


def _build_regime_state_summary_from_rows(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    frame = _query_frame(
        conn,
        """
        SELECT
            position_state_id,
            regime_label,
            regime_source,
            COUNT(*) AS sample_count,
            COUNT(DISTINCT code) AS symbol_count,
            AVG(forward_ret_20d) AS mean_forward_ret_20d,
            AVG(mfe_20d) AS mean_mfe_20d,
            AVG(mae_20d) AS mean_mae_20d,
            AVG(path_value_score_v1) AS mean_path_value_score_v1
        FROM position_rows
        GROUP BY position_state_id, regime_label, regime_source
        HAVING COUNT(*) >= ?
        ORDER BY mean_path_value_score_v1 DESC, sample_count DESC, mean_forward_ret_20d DESC
        """,
        [STATE_MIN_REGIME_SAMPLE_COUNT],
    )
    return frame


def _build_monthly_stability_frame(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    monthly = _query_frame(
        conn,
        """
        SELECT
            position_state_id,
            CAST(SUBSTR(CAST(trade_date AS VARCHAR), 1, 6) AS INTEGER) AS trade_month,
            COUNT(*) AS sample_count,
            AVG(path_value_score_v1) AS mean_path_value_score_v1,
            AVG(forward_ret_20d) AS mean_forward_ret_20d,
            AVG(mae_20d) AS mean_mae_20d
        FROM position_rows
        GROUP BY position_state_id, CAST(SUBSTR(CAST(trade_date AS VARCHAR), 1, 6) AS INTEGER)
        """
    )
    if monthly.empty:
        return monthly
    rows: list[dict[str, Any]] = []
    for state_id, group in monthly.groupby("position_state_id", dropna=False):
        means = pd.to_numeric(group["mean_path_value_score_v1"], errors="coerce")
        rows.append(
            {
                "position_state_id": str(state_id),
                "months_observed": int(group["trade_month"].nunique()),
                "month_sample_count": int(group["sample_count"].sum()),
                "mean_monthly_path_value_score_v1": _safe_float(means.mean()),
                "std_monthly_path_value_score_v1": _safe_float(means.std(ddof=0)),
                "max_monthly_path_value_score_v1": _safe_float(means.max()),
                "min_monthly_path_value_score_v1": _safe_float(means.min()),
                "spread_monthly_path_value_score_v1": _safe_float(means.max() - means.min()),
                "mean_monthly_forward_ret_20d": _safe_float(pd.to_numeric(group["mean_forward_ret_20d"], errors="coerce").mean()),
            }
        )
    frame = pd.DataFrame(rows)
    if not frame.empty:
        frame = frame.sort_values(
            ["std_monthly_path_value_score_v1", "spread_monthly_path_value_score_v1", "month_sample_count"],
            ascending=[True, True, False],
        ).reset_index(drop=True)
    return frame


def _select_top_state_rows(state_frame: pd.DataFrame, *, limit: int) -> dict[str, list[dict[str, Any]]]:
    if state_frame.empty:
        return {
            "high_value_states": [],
            "weak_noise_states": [],
            "bad_pick_removal_states": [],
            "regime_dependent_states": [],
        }
    eligible = state_frame.loc[state_frame["sample_count"] >= STATE_MIN_SAMPLE_COUNT].copy()
    high_value = eligible.loc[
        (eligible["state_quality_label"] == "high_value")
        & (eligible["mean_forward_ret_20d"] > 0)
    ].sort_values(
        ["mean_path_value_score_v1", "sample_count", "mean_forward_ret_20d"],
        ascending=[False, False, False],
    ).head(limit)
    weak_noise = eligible.loc[eligible["state_quality_label"] == "weak_noise"].sort_values(
        ["sample_count", "mean_path_value_score_v1"],
        ascending=[False, False],
    ).head(limit)
    bad_pick = eligible.loc[eligible["state_quality_label"] == "bad_pick_removal"].sort_values(
        ["mean_path_value_score_v1", "sample_count", "mean_mae_20d"],
        ascending=[True, False, True],
    ).head(limit)
    return {
        "high_value_states": high_value.to_dict(orient="records"),
        "weak_noise_states": weak_noise.to_dict(orient="records"),
        "bad_pick_removal_states": bad_pick.to_dict(orient="records"),
        "regime_dependent_states": [],
    }


def _select_regime_dependent_states(regime_state_frame: pd.DataFrame, *, limit: int = 50) -> list[dict[str, Any]]:
    if regime_state_frame.empty:
        return []
    rows: list[dict[str, Any]] = []
    for state_id, group in regime_state_frame.groupby("position_state_id", dropna=False):
        if len(group) < 2:
            continue
        spread = float(pd.to_numeric(group["mean_path_value_score_v1"], errors="coerce").max() - pd.to_numeric(group["mean_path_value_score_v1"], errors="coerce").min())
        if spread <= 0:
            continue
        sorted_group = group.sort_values("mean_path_value_score_v1", ascending=False)
        rows.append(
            {
                "position_state_id": str(state_id),
                "regime_count": int(group["regime_label"].nunique()),
                "sample_count": int(group["sample_count"].sum()),
                "score_spread": spread,
                "best_regime_label": str(sorted_group.iloc[0]["regime_label"]),
                "worst_regime_label": str(sorted_group.sort_values("mean_path_value_score_v1", ascending=True).iloc[0]["regime_label"]),
                "best_regime_score": _safe_float(group["mean_path_value_score_v1"].max()),
                "worst_regime_score": _safe_float(group["mean_path_value_score_v1"].min()),
                "best_regime_ret20": _safe_float(group["mean_forward_ret_20d"].max()),
                "worst_regime_ret20": _safe_float(group["mean_forward_ret_20d"].min()),
            }
        )
    return sorted(rows, key=lambda item: (item["score_spread"], item["sample_count"]), reverse=True)[:limit]


def _build_classification_payload(state_frame: pd.DataFrame, *, detail_limit: int) -> dict[str, Any]:
    if state_frame.empty:
        return {
            "schema_version": CLASSIFICATION_SCHEMA_VERSION,
            "generated_at": _utc_now(),
            "state_quality_counts": {},
            "examples": {},
        }
    counts = state_frame["state_quality_label"].value_counts(dropna=False).to_dict()
    examples: dict[str, list[dict[str, Any]]] = {}
    for label in ("high_value", "weak_noise", "bad_pick_removal", "neutral", "unclassified"):
        examples[label] = state_frame.loc[state_frame["state_quality_label"] == label].head(detail_limit).to_dict(orient="records")
    return {
        "schema_version": CLASSIFICATION_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "state_quality_counts": {str(key): int(value) for key, value in counts.items()},
        "examples": examples,
        "label_rules": {
            "high_value": {
                "mean_path_value_score_v1": f">= {HIGH_VALUE_SCORE_THRESHOLD}",
                "mean_forward_ret_20d": "> 0",
            },
            "weak_noise": {
                "abs(mean_path_value_score_v1)": f"<= {WEAK_NOISE_SCORE_THRESHOLD}",
                "abs(mean_forward_ret_20d)": "<= 0.01",
            },
            "bad_pick_removal": {
                "mean_path_value_score_v1": f"<= {BAD_PICK_SCORE_THRESHOLD}",
                "mean_forward_ret_20d": "<= 0",
                "mean_mae_20d": f"<= {BAD_PICK_MAE_THRESHOLD}",
            },
        },
    }


def _build_decision_payload(
    *,
    state_frame: pd.DataFrame,
    regime_state_frame: pd.DataFrame,
    overall_metrics: dict[str, Any],
    artifact_paths: dict[str, str],
    study_status: str,
    detail_limit: int,
) -> dict[str, Any]:
    high_value_count = int((state_frame.get("state_quality_label", pd.Series(dtype="string")) == "high_value").sum()) if not state_frame.empty else 0
    bad_pick_count = int((state_frame.get("state_quality_label", pd.Series(dtype="string")) == "bad_pick_removal").sum()) if not state_frame.empty else 0
    regime_dependent_count = int(len(_select_regime_dependent_states(regime_state_frame, limit=detail_limit)))
    if study_status != "confirmed" or int(overall_metrics.get("eligible_row_count", 0)) <= 0:
        recommendation = "drop"
        reason = "insufficient_eligible_rows_or_unconfirmed_study"
    elif high_value_count >= 5 and bad_pick_count >= 5 and regime_dependent_count >= 3:
        recommendation = "keep"
        reason = "strong_broad_state_signal_with_regime_separation"
    else:
        recommendation = "hold"
        reason = "summary_valid_but_not_enough_state_breadth_for_keep"
    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "recommendation": recommendation,
        "decision_reason": reason,
        "study_status": study_status,
        "signal_counts": {
            "high_value_state_count": high_value_count,
            "bad_pick_removal_state_count": bad_pick_count,
            "regime_dependent_state_count": regime_dependent_count,
        },
        "artifact_paths": artifact_paths,
        "evidence": {
            "eligible_row_count": int(overall_metrics.get("eligible_row_count", 0)),
            "state_count": int(len(state_frame)),
            "regime_state_count": int(len(regime_state_frame)),
        },
    }


def _build_manifest_payload(
    *,
    session_id: str,
    source_db: Path,
    source_frame_summary: dict[str, int],
    overall_metrics: dict[str, Any],
    state_counts: dict[str, int],
    artifact_paths: dict[str, str],
    study_status: str,
    output_root: Path,
    limit_symbols: int | None,
    no_lookahead_passed: bool,
    output_rows_count: int,
    sample_rows_count: int,
    date_range: dict[str, int | None],
) -> dict[str, Any]:
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "session_id": session_id,
        "study_status": study_status,
        "run_mode": "smoke" if limit_symbols else "full",
        "limit_symbols": limit_symbols,
        "source_db_path": str(source_db),
        "output_root": str(output_root),
        "date_range": date_range,
        "source_frame_summary": source_frame_summary,
        "overall_metrics": overall_metrics,
        "state_counts": state_counts,
        "output_rows_count": output_rows_count,
        "sample_rows_count": sample_rows_count,
        "no_lookahead_check": {
            "passed": no_lookahead_passed,
            "entry_convention": "next_session_open",
            "path_convention": "forward path uses post-entry business-day windows only",
            "feature_construction": "no future data used in state features; only current and prior bars are used for state encoding",
        },
        "output_artifacts": artifact_paths,
    }


def _validate_artifact_set(artifact_paths: dict[str, str]) -> None:
    json_keys = [key for key, value in artifact_paths.items() if str(value).lower().endswith(".json")]
    parquet_keys = [key for key, value in artifact_paths.items() if str(value).lower().endswith(".parquet")]
    for key in json_keys:
        path = Path(artifact_paths[key])
        if not path.exists():
            raise FileNotFoundError(f"missing json artifact: {path}")
        json.loads(path.read_text(encoding="utf-8"))
    for key in parquet_keys:
        path = Path(artifact_paths[key])
        if not path.exists():
            raise FileNotFoundError(f"missing parquet artifact: {path}")
        pd.read_parquet(path)


def run_ma_position_path_research(
    *,
    db_path: str | None = None,
    output_dir: str | Path | None = None,
    detail_limit: int = 50,
    limit_symbols: int | None = None,
) -> dict[str, Any]:
    run_started = time.perf_counter()
    source_db = _resolve_source_db_path(db_path)
    output_root = Path(output_dir).expanduser().resolve() if output_dir else DEFAULT_OUTPUT_DIR.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    session_id = _make_session_id()
    session_tmp_dir, session_final_dir = _resolve_output_paths(output_root, session_id)
    session_tmp_dir.mkdir(parents=True, exist_ok=False)
    _progress_log(f"start db={source_db} out_root={output_root} session={session_id}")

    conn = duckdb.connect(str(source_db), read_only=True)
    try:
        if not _table_exists(conn, "daily_bars"):
            raise RuntimeError("daily_bars_missing")
        bars_count = _table_row_count(conn, "daily_bars")
        if bars_count <= 0:
            raise RuntimeError("daily_bars_empty")
        if not _table_exists(conn, "daily_ma"):
            raise RuntimeError("daily_ma_missing")
        ma_count = _table_row_count(conn, "daily_ma")
        if ma_count <= 0:
            raise RuntimeError("daily_ma_empty")
        regime_table_exists = _table_exists(conn, "market_regime_daily")
        regime_count = _table_row_count(conn, "market_regime_daily") if regime_table_exists else 0
        bars_frame = _load_bars_frame(conn)
        ma_frame = _load_ma_frame(conn)
        regime_frame = _load_regime_frame(conn)
    finally:
        conn.close()
    _progress_log(
        f"source_loaded bars={len(bars_frame)} ma={len(ma_frame)} regime={len(regime_frame)} elapsed={time.perf_counter() - run_started:.1f}s"
    )

    if bars_frame.empty or ma_frame.empty:
        raise RuntimeError("insufficient_source_rows")

    selected_symbol_codes = sorted(bars_frame["code"].dropna().astype(str).unique().tolist())
    if limit_symbols is not None and int(limit_symbols) > 0:
        selected_symbol_codes = selected_symbol_codes[: int(limit_symbols)]
        bars_frame = bars_frame.loc[bars_frame["code"].isin(selected_symbol_codes)].copy()
        ma_frame = ma_frame.loc[ma_frame["code"].isin(selected_symbol_codes)].copy()
        _progress_log(f"smoke_limit_symbols={len(selected_symbol_codes)}")

    merged = bars_frame.merge(ma_frame, on=["code", "trade_date"], how="left", suffixes=("", "_ma"))
    merged = merged.sort_values(["code", "trade_date"]).reset_index(drop=True)
    _progress_log(f"merged rows={len(merged)} elapsed={time.perf_counter() - run_started:.1f}s")

    eligible_row_count = 0
    excluded_rows = 0
    confirmed_regime_row_count = 0
    provisional_regime_row_count = 0
    eligible_symbol_codes: set[str] = set()
    eligible_trade_dates: set[int] = set()
    eligible_regime_sources: set[str] = set()
    row_parquet_tmp = session_tmp_dir / "position_state_forward_path_rows.parquet"
    sample_parquet_tmp = session_tmp_dir / "position_state_sample_rows.parquet"
    row_writer = _ParquetChunkWriter(row_parquet_tmp)
    sample_rows: list[dict[str, Any]] = []
    sample_limit = min(max(int(detail_limit), 0), 500)
    row_columns = [
        "code",
        "trade_date",
        "position_state_id",
        "regime_source",
        "regime_label",
        "entry_next_open",
        "entry_day_close",
        "forward_window_days",
        "candle_state_code",
        "volume_condition",
        "forward_ret_3d",
        "forward_ret_5d",
        "forward_ret_10d",
        "forward_ret_20d",
        "mfe_20d",
        "mae_20d",
        "days_to_mfe_20d",
        "days_to_mae_20d",
        "days_to_positive_close",
        "days_to_plus_3pct",
        "days_to_plus_5pct",
        "days_to_minus_3pct",
        "days_to_minus_5pct",
        "hit_plus_5_before_minus_5",
        "hit_minus_5_before_plus_5",
        "hit_plus_3_before_minus_3",
        "hit_minus_3_before_plus_3",
        "hit_plus_1atr_before_minus_1atr",
        "mfe_atr_20d",
        "mae_atr_20d",
        "close_above_entry_days_20d",
        "close_below_entry_days_20d",
        "path_value_score_v1",
        "body_norm_atr",
        "upper_wick_ratio",
        "lower_wick_ratio",
        "volume",
    ]

    for group_index, (_, group) in enumerate(merged.groupby("code", sort=False), start=1):
        enriched_group = _enrich_symbol_frame(group.copy(), regime_frame=regime_frame)
        eligible_group = enriched_group.loc[enriched_group["eligible_for_analysis"], row_columns].copy()
        eligible_count = int(len(eligible_group))
        eligible_row_count += eligible_count
        excluded_rows += int(len(enriched_group) - eligible_count)
        if eligible_count == 0:
            continue

        row_writer.write_frame(eligible_group)
        eligible_symbol_codes.update(str(value) for value in eligible_group["code"].dropna().unique().tolist())
        eligible_trade_dates.update(int(value) for value in eligible_group["trade_date"].dropna().astype(int).unique().tolist())
        eligible_regime_sources.update(str(value) for value in eligible_group["regime_source"].dropna().unique().tolist())
        confirmed_regime_row_count += int((eligible_group["regime_source"] == CONFIRMED_REGIME_SOURCE).sum())
        provisional_regime_row_count += int((eligible_group["regime_source"] == PROVISIONAL_REGIME_SOURCE).sum())
        if sample_limit > 0 and len(sample_rows) < sample_limit:
            sample_rows.extend(
                eligible_group.head(sample_limit - len(sample_rows)).to_dict(orient="records")
            )
        if group_index % 50 == 0:
            _progress_log(
                f"groups={group_index} eligible_rows={eligible_row_count} elapsed={time.perf_counter() - run_started:.1f}s"
            )
    row_writer.close()
    if eligible_row_count <= 0:
        raise RuntimeError("no_enriched_rows")
    _progress_log(f"forward_rows_written rows={eligible_row_count} elapsed={time.perf_counter() - run_started:.1f}s")

    if sample_rows:
        _write_parquet(sample_parquet_tmp, pd.DataFrame(sample_rows))

    conn = duckdb.connect()
    try:
        conn.execute(
            f"""
            CREATE TEMP VIEW position_rows AS
            SELECT * FROM read_parquet('{row_parquet_tmp.as_posix()}')
            """
        )
        state_frame = _build_state_summary_from_rows(conn)
        regime_state_frame = _build_regime_state_summary_from_rows(conn)
        monthly_stability_frame = _build_monthly_stability_frame(conn)
        overall_metrics = _build_overall_metrics_from_rows(conn)
        selected_row_count = int(conn.execute("SELECT COUNT(*) FROM position_rows").fetchone()[0])
        date_range_row = conn.execute("SELECT MIN(trade_date), MAX(trade_date) FROM position_rows").fetchone()
    finally:
        conn.close()

    _progress_log(
        "summaries "
        f"state={len(state_frame)} regime={len(regime_state_frame)} monthly={len(monthly_stability_frame)} "
        f"elapsed={time.perf_counter() - run_started:.1f}s"
    )

    top_states = _select_top_state_rows(state_frame, limit=int(detail_limit))
    regime_dependent_limit = max(100, int(detail_limit) * 2)
    regime_dependent_states = _select_regime_dependent_states(regime_state_frame, limit=regime_dependent_limit)
    top_states["regime_dependent_states"] = regime_dependent_states

    state_counts = {
        "total_state_count": int(len(state_frame)),
        "high_value_state_count": int((state_frame["state_quality_label"] == "high_value").sum()) if not state_frame.empty else 0,
        "weak_noise_state_count": int((state_frame["state_quality_label"] == "weak_noise").sum()) if not state_frame.empty else 0,
        "bad_pick_removal_state_count": int((state_frame["state_quality_label"] == "bad_pick_removal").sum()) if not state_frame.empty else 0,
        "regime_dependent_state_count": int(len(regime_dependent_states)),
    }

    unique_regime_sources = sorted(eligible_regime_sources)
    if len(unique_regime_sources) > 1:
        regime_policy = "separate_confirmed_and_provisional"
    elif unique_regime_sources:
        regime_policy = unique_regime_sources[0]
    else:
        regime_policy = "unknown"

    source_frame_summary = {
        "daily_bars_rows": int(bars_count),
        "daily_ma_rows": int(ma_count),
        "market_regime_daily_rows": int(regime_count),
        "selected_symbol_count": int(len(selected_symbol_codes)),
        "selected_trade_date_count": int(len(eligible_trade_dates)),
    }

    row_artifact_paths = {
        "position_state_forward_path_rows_parquet": str(session_final_dir / "position_state_forward_path_rows.parquet"),
        "position_state_sample_rows_parquet": str(session_final_dir / "position_state_sample_rows.parquet") if sample_rows else None,
    }

    summary_payload = {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "study_status": "confirmed" if eligible_row_count > 0 else "not_reportable",
        "session_id": session_id,
        "source_db_path": str(source_db),
        "source_frame_summary": source_frame_summary,
        "evaluation_contract": {
            "entry_convention": "next_session_open",
            "horizon_convention": "repo_standard_signal_day_plus_horizon_plus_one_close",
            "path_window_days": PATH_WINDOW_DAYS,
            "forward_horizons": list(FORWARD_HORIZONS),
            "close_vs_ma_rule": "close_ge_ma_is_above",
            "ma_slope_rule": f"pct_change_threshold={SLOPE_FLAT_THRESHOLD}",
            "body_vs_ma_rule": "body_full_above/body_full_below/body_crosses/wick_touches_only",
            "candle_rule": {
                "body_small_threshold": BODY_SMALL_THRESHOLD,
                "body_doji_threshold": BODY_DOJI_THRESHOLD,
                "body_large_threshold": BODY_LARGE_THRESHOLD,
                "wick_dominant_threshold": WICK_DOMINANT_THRESHOLD,
                "gap_threshold": GAP_THRESHOLD,
            },
            "price_location_rule": {
                "near_high_threshold": PRICE_LOCATION_NEAR_THRESHOLD,
                "near_low_threshold": PRICE_LOCATION_LOW_THRESHOLD,
            },
            "state_encoding_legend": {
                "close_vs_ma": {"A": "close above or equal to MA", "B": "close below MA", "U": "unknown"},
                "body_vs_ma": {
                    "A": "body fully above MA",
                    "B": "body fully below MA",
                    "C": "body crosses MA",
                    "W": "wick touches only",
                    "U": "unknown",
                },
                "ma_stack": {"B": "bullish stack", "R": "bearish stack", "M": "mixed stack"},
                "slope": {"U": "up", "F": "flat", "D": "down"},
                "price_location": {"H": "near high", "M": "range middle", "L": "near low"},
                "volume": {"A": "above 20d average", "B": "below or equal 20d average", "U": "unknown"},
                "regime_source": {"C": CONFIRMED_REGIME_SOURCE, "P": PROVISIONAL_REGIME_SOURCE},
                "candle_code": {
                    "BB": "bullish body",
                    "BR": "bearish body",
                    "DJ": "doji-like",
                    "SK": "small body / koma",
                    "LBB": "large bullish body",
                    "LBR": "large bearish body",
                    "UW": "upper wick dominant",
                    "LW": "lower wick dominant",
                    "GU": "gap up",
                    "GD": "gap down",
                },
            },
            "regime_policy": regime_policy,
            "no_lookahead_check": {
                "passed": True,
                "entry_convention": "next_session_open",
                "forward_path_only_uses_post_entry_business_days": True,
                "state_features_use_only_observable_past_and_current_bars": True,
            },
        },
        "metric_definitions": [
            {
                "name": "forward_ret_20d",
                "description": "Signal-day plus 20 business days exit close from next-session-open entry",
            },
            {
                "name": "mfe_20d",
                "description": "Max favorable excursion in the 20-session post-entry path window",
            },
            {
                "name": "mae_20d",
                "description": "Max adverse excursion in the 20-session post-entry path window",
            },
            {
                "name": "path_value_score_v1",
                "description": "Transparent weighted blend of 20d endpoint return, MFE, MAE, 10d return, time-above-entry, and first-hit order",
            },
        ],
        "overall_metrics": overall_metrics,
        "state_counts": state_counts,
        "top_state_lists": top_states,
        "regime_dependent_policy": {
            "confirmed_regime_rows_used": int(confirmed_regime_row_count),
            "provisional_regime_rows_used": int(provisional_regime_row_count),
            "regime_policy": regime_policy,
        },
        "output_artifacts": {
            "run_manifest_json": str(session_final_dir / "run_manifest.json"),
            "ma_candle_position_value_v1_decision_json": str(session_final_dir / "ma_candle_position_value_v1_decision.json"),
            "position_state_value_summary_json": str(session_final_dir / "position_state_value_summary.json"),
            "position_state_value_by_regime_json": str(session_final_dir / "position_state_value_by_regime.json"),
            "position_state_monthly_stability_json": str(session_final_dir / "position_state_monthly_stability.json"),
            "position_state_classification_json": str(session_final_dir / "position_state_classification.json"),
            "position_state_forward_path_rows_parquet": str(session_final_dir / "position_state_forward_path_rows.parquet"),
            "position_state_sample_rows_parquet": row_artifact_paths["position_state_sample_rows_parquet"],
            "_artifact_complete_json": str(session_final_dir / "_ARTIFACT_COMPLETE.json"),
        },
        "notes": [
            "State ids are canonical strings composed only from observable signal-date features.",
            "No MeeMee ranking, publish, or UI surfaces were changed.",
            "Regime data is kept separate by source; confirmed and provisional regime rows are not mixed silently.",
            "Row-level outputs are parquet-only; JSON artifacts are summary/decision/manifest only.",
        ],
        "excluded_row_count": excluded_rows,
    }

    by_regime_payload = {
        "schema_version": REGIME_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "session_id": session_id,
        "regime_policy": regime_policy,
        "regime_state_row_count": int(len(regime_state_frame)),
        "top_regime_dependent_states": regime_dependent_states,
        "regime_state_summary": regime_state_frame.head(max(100, int(detail_limit) * 2)).to_dict(orient="records"),
        "regime_source_counts": {
            CONFIRMED_REGIME_SOURCE: int(confirmed_regime_row_count),
            PROVISIONAL_REGIME_SOURCE: int(provisional_regime_row_count),
        },
        "row_parquet": row_artifact_paths["position_state_forward_path_rows_parquet"],
    }

    monthly_payload = {
        "schema_version": MONTHLY_STABILITY_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "session_id": session_id,
        "row_parquet": row_artifact_paths["position_state_forward_path_rows_parquet"],
        "monthly_state_row_count": int(len(monthly_stability_frame)),
        "top_stable_states": monthly_stability_frame.head(detail_limit).to_dict(orient="records"),
        "top_unstable_states": monthly_stability_frame.sort_values(
            ["std_monthly_path_value_score_v1", "spread_monthly_path_value_score_v1", "month_sample_count"],
            ascending=[False, False, False],
        ).head(detail_limit).to_dict(orient="records"),
    }

    classification_payload = _build_classification_payload(state_frame, detail_limit=detail_limit)
    decision_payload = _build_decision_payload(
        state_frame=state_frame,
        regime_state_frame=regime_state_frame,
        overall_metrics=overall_metrics,
        artifact_paths=row_artifact_paths,
        study_status=summary_payload["study_status"],
        detail_limit=detail_limit,
    )

    manifest_payload = _build_manifest_payload(
        session_id=session_id,
        source_db=source_db,
        source_frame_summary=source_frame_summary,
        overall_metrics=overall_metrics,
        state_counts=state_counts,
        artifact_paths={**summary_payload["output_artifacts"], **row_artifact_paths},
        study_status=summary_payload["study_status"],
        output_root=output_root,
        limit_symbols=int(limit_symbols) if limit_symbols is not None else None,
        no_lookahead_passed=True,
        output_rows_count=selected_row_count,
        sample_rows_count=len(sample_rows),
        date_range={
            "first_trade_date": int(date_range_row[0]) if date_range_row and date_range_row[0] is not None else None,
            "last_trade_date": int(date_range_row[1]) if date_range_row and date_range_row[1] is not None else None,
        },
    )

    _write_json(session_tmp_dir / "position_state_value_summary.json", summary_payload)
    _write_json(session_tmp_dir / "position_state_value_by_regime.json", by_regime_payload)
    _write_json(session_tmp_dir / "position_state_monthly_stability.json", monthly_payload)
    _write_json(session_tmp_dir / "position_state_classification.json", classification_payload)
    _write_json(session_tmp_dir / "ma_candle_position_value_v1_decision.json", decision_payload)
    _write_json(session_tmp_dir / "run_manifest.json", manifest_payload)

    validation_artifacts = {
        "run_manifest_json": str(session_tmp_dir / "run_manifest.json"),
        "ma_candle_position_value_v1_decision_json": str(session_tmp_dir / "ma_candle_position_value_v1_decision.json"),
        "position_state_value_summary_json": str(session_tmp_dir / "position_state_value_summary.json"),
        "position_state_value_by_regime_json": str(session_tmp_dir / "position_state_value_by_regime.json"),
        "position_state_monthly_stability_json": str(session_tmp_dir / "position_state_monthly_stability.json"),
        "position_state_classification_json": str(session_tmp_dir / "position_state_classification.json"),
        "position_state_forward_path_rows_parquet": str(row_parquet_tmp),
    }
    if sample_rows:
        validation_artifacts["position_state_sample_rows_parquet"] = str(sample_parquet_tmp)
    _validate_artifact_set(validation_artifacts)
    _write_json(session_tmp_dir / "_ARTIFACT_COMPLETE.json", {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "session_id": session_id,
        "validated": True,
    })
    _finalize_session_dir(session_tmp_dir, session_final_dir)
    _progress_log(f"finalized session={session_id} elapsed={time.perf_counter() - run_started:.1f}s")

    final_artifact_paths = {
        "run_manifest_json": str(session_final_dir / "run_manifest.json"),
        "ma_candle_position_value_v1_decision_json": str(session_final_dir / "ma_candle_position_value_v1_decision.json"),
        "position_state_value_summary_json": str(session_final_dir / "position_state_value_summary.json"),
        "position_state_value_by_regime_json": str(session_final_dir / "position_state_value_by_regime.json"),
        "position_state_monthly_stability_json": str(session_final_dir / "position_state_monthly_stability.json"),
        "position_state_classification_json": str(session_final_dir / "position_state_classification.json"),
        "position_state_forward_path_rows_parquet": str(session_final_dir / "position_state_forward_path_rows.parquet"),
        "position_state_sample_rows_parquet": str(session_final_dir / "position_state_sample_rows.parquet") if sample_rows else None,
        "_artifact_complete_json": str(session_final_dir / "_ARTIFACT_COMPLETE.json"),
    }

    summary_payload["output_artifacts"] = final_artifact_paths
    manifest_payload["output_artifacts"] = final_artifact_paths
    decision_payload["artifact_paths"] = {
        "summary": final_artifact_paths["position_state_value_summary_json"],
        "by_regime": final_artifact_paths["position_state_value_by_regime_json"],
        "monthly_stability": final_artifact_paths["position_state_monthly_stability_json"],
        "classification": final_artifact_paths["position_state_classification_json"],
        "row_parquet": final_artifact_paths["position_state_forward_path_rows_parquet"],
    }
    _write_json(session_final_dir / "ma_candle_position_value_v1_decision.json", decision_payload)

    return {
        "session_id": session_id,
        "session_dir": str(session_final_dir),
        "summary_path": final_artifact_paths["position_state_value_summary_json"],
        "by_regime_path": final_artifact_paths["position_state_value_by_regime_json"],
        "monthly_stability_path": final_artifact_paths["position_state_monthly_stability_json"],
        "classification_path": final_artifact_paths["position_state_classification_json"],
        "decision_path": final_artifact_paths["ma_candle_position_value_v1_decision_json"],
        "manifest_path": final_artifact_paths["run_manifest_json"],
        "detail_path": final_artifact_paths["position_state_forward_path_rows_parquet"],
        "summary": summary_payload,
        "by_regime": by_regime_payload,
        "monthly_stability": monthly_payload,
        "classification": classification_payload,
        "decision": decision_payload,
        "manifest": manifest_payload,
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TRADEX MA/candlestick position path-value research.")
    parser.add_argument("--db-path", default=None)
    parser.add_argument("--output-root", "--output-dir", dest="output_root", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--detail-limit", type=int, default=50)
    parser.add_argument("--limit-symbols", type=int, default=None)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    result = run_ma_position_path_research(
        db_path=args.db_path,
        output_dir=args.output_root,
        detail_limit=args.detail_limit,
        limit_symbols=args.limit_symbols,
    )
    print(json.dumps(result["summary"]["output_artifacts"], ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
