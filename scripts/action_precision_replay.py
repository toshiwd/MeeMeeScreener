from __future__ import annotations

import argparse
import json
import math
import os
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, asdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from external_analysis.runtime.source_snapshot import create_source_snapshot

DEFAULT_DB_CANDIDATES = (
    Path.home() / "AppData" / "Local" / "MeeMeeScreener" / "data" / "stocks.duckdb",
    Path.home() / "AppData" / "Local" / "MeeMeeScreener-dev" / "data" / "stocks.duckdb",
    REPO_ROOT / "data" / "stocks.duckdb",
    Path(r"G:\Tradex\scratch\source_snapshots\tradex_research_snapshot_full_20230101_20260226.duckdb"),
)
DEFAULT_OUTPUT_DIR = REPO_ROOT / "artifacts" / "research_inventory"

ACTIVE_LOGIC_FALLBACK = "logic:trade:v1"
ACTIVE_BASIS_FALLBACK = "basis:v1"
ANALYSIS_CUTOFF_YMD = 20260226

DEFAULT_TRAIN_MONTHS = 12
DEFAULT_TUNE_MONTHS = 6
DEFAULT_VALIDATION_MONTHS = 6

FROZEN_LONG_TOO_LATE_BLOCK_CLUSTERS = (
    "regime=risk_on|monthly=box_upper|weekly=weekly_bull|monthly_state=monthly_bull|daily=daily_bull|candle=candle_other|change=up",
    "regime=risk_on|monthly=box_upper|weekly=weekly_bull|monthly_state=monthly_bull|daily=daily_bull|candle=candle_other|change=flat",
)
LONG_WEAK_DIRECTION_MIN_SAMPLE_COUNT = 150
LONG_WEAK_DIRECTION_MAX_SAMPLE_COUNT = 190
LONG_WEAK_DIRECTION_BLOCK_CLUSTER_COUNT = 1


@dataclass(frozen=True)
class ActionPrecisionThresholds:
    timing_refinement_window_days: int = 5
    pre_signal_context_window_days: int = 10
    timing_early_gap_threshold_pct: float = 0.02
    timing_late_remaining_ratio_threshold: float = 0.35
    timing_late_pre_move_threshold_pct: float = 0.05
    target_gain_pct_long: float = 0.10
    target_gain_pct_short: float = 0.10
    soft_gain_pct_long: float = 0.05
    soft_gain_pct_short: float = 0.05
    max_adverse_pct_long: float = 0.08
    max_adverse_pct_short: float = 0.08
    max_days_to_favorable_move: int = 20
    atr_scaled_stop_enabled: bool = False
    atr_stop_multiple: float | None = None


@dataclass(frozen=True)
class SplitContract:
    train_months: tuple[int, ...]
    tune_months: tuple[int, ...]
    validation_months: tuple[int, ...]
    analysis_cutoff_ymd: int


def _resolve_db_path(cli_value: str | None) -> Path:
    if cli_value:
        path = Path(cli_value).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"DB not found: {path}")
        return path
    env = os.getenv("TRADEX_SNAPSHOT_DB_PATH") or os.getenv("STOCKS_DB_PATH")
    if env:
        path = Path(env).expanduser().resolve()
        if path.exists():
            return path
    for candidate in DEFAULT_DB_CANDIDATES:
        if candidate.exists():
            return candidate.resolve()
    raise FileNotFoundError("Could not resolve snapshot DB path. Pass --db-path or set TRADEX_SNAPSHOT_DB_PATH.")


def _ymd_expr(col: str) -> str:
    return f"""
        CASE
          WHEN TRY_CAST({col} AS BIGINT) BETWEEN 19000101 AND 20991231 THEN CAST({col} AS BIGINT)
          WHEN TRY_CAST({col} AS BIGINT) >= 1000000000000 THEN CAST(strftime(to_timestamp(CAST({col} AS BIGINT) / 1000), '%Y%m%d') AS INTEGER)
          WHEN TRY_CAST({col} AS BIGINT) BETWEEN 600000000 AND 5000000000 THEN CAST(strftime(to_timestamp(CAST({col} AS BIGINT)), '%Y%m%d') AS INTEGER)
          ELSE NULL
        END
    """


def _ymd_to_date(value: int) -> date:
    text = f"{int(value):08d}"
    return date(int(text[0:4]), int(text[4:6]), int(text[6:8]))


def _date_to_ymd(value: date) -> int:
    return int(value.strftime("%Y%m%d"))


def _shift_years_ymd(value: int, *, years: int) -> int:
    work = _ymd_to_date(int(value))
    try:
        shifted = work.replace(year=work.year + int(years))
    except ValueError:
        shifted = work.replace(year=work.year + int(years), day=28)
    return _date_to_ymd(shifted)


def _normalized_trade_dates(conn: duckdb.DuckDBPyConnection) -> list[int]:
    rows = conn.execute(
        f"""
        SELECT DISTINCT {_ymd_expr("date")} AS ymd
        FROM daily_bars
        WHERE {_ymd_expr("date")} IS NOT NULL
        ORDER BY ymd
        """
    ).fetchall()
    return [int(row[0]) for row in rows if row and row[0] is not None]


def _snapshot_horizon_contract(conn: duckdb.DuckDBPyConnection) -> dict[str, int | None]:
    dates = _normalized_trade_dates(conn)
    snapshot_max_trade_date = dates[-1] if dates else None
    replay_lookback_start_date = _shift_years_ymd(snapshot_max_trade_date, years=-10) if snapshot_max_trade_date is not None else None
    last_fully_confirmable_month = None
    analysis_cutoff_ymd = None
    if dates:
        last_index_by_month: dict[int, int] = {}
        for idx, ymd in enumerate(dates):
            last_index_by_month[_month_bucket(int(ymd))] = idx
        eligible_months = [
            month
            for month, last_idx in last_index_by_month.items()
            if last_idx + int(ActionPrecisionThresholds().max_days_to_favorable_move) < len(dates)
        ]
        if eligible_months:
            last_fully_confirmable_month = int(max(eligible_months))
            analysis_cutoff_ymd = int(dates[last_index_by_month[last_fully_confirmable_month]])
    return {
        "snapshot_max_trade_date": snapshot_max_trade_date,
        "replay_lookback_start_date": replay_lookback_start_date,
        "last_fully_confirmable_month": last_fully_confirmable_month,
        "analysis_cutoff_ymd": analysis_cutoff_ymd,
    }


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except Exception:
        return None
    return out if math.isfinite(out) else None


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def _parse_json(value: Any) -> dict[str, Any]:
    if not value:
        return {}
    if isinstance(value, dict):
        return dict(value)
    try:
        parsed = json.loads(value)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _bucket_numeric(value: float | None, edges: tuple[float, ...], labels: tuple[str, ...]) -> str:
    if value is None or len(labels) != len(edges) + 1:
        return "unknown"
    for idx, edge in enumerate(edges):
        if value < edge:
            return labels[idx]
    return labels[-1]


def _prob_bucket(value: float | None) -> str:
    return _bucket_numeric(
        value,
        edges=(0.35, 0.50, 0.65),
        labels=("lt_035", "lt_050", "lt_065", "ge_065"),
    )


def _change_bucket(value: float | None) -> str:
    if value is None:
        return "unknown"
    if value <= -0.03:
        return "deep_down"
    if value <= -0.01:
        return "down"
    if value <= 0.01:
        return "flat"
    if value <= 0.05:
        return "up"
    return "extended_up"


def _state_combo(side: str, payload: dict[str, Any]) -> str:
    market_regime = str(payload.get("marketRegime") or "unknown")
    monthly_box = str(payload.get("monthlyBoxState") or "unknown")
    weekly_up = _safe_float(payload.get("weeklyBreakoutUpProb"))
    weekly_down = _safe_float(payload.get("weeklyBreakoutDownProb"))
    monthly_up = _safe_float(payload.get("monthlyBreakoutUpProb"))
    monthly_down = _safe_float(payload.get("monthlyBreakoutDownProb"))
    change_pct = _safe_float(payload.get("changePct"))
    candle_body = _safe_float(payload.get("candleBodyRatio"))
    upper_wick = _safe_float(payload.get("candleUpperWickRatio"))
    lower_wick = _safe_float(payload.get("candleLowerWickRatio"))
    reclaim60 = (_safe_float(payload.get("reclaim60")) or 0.0) >= 0.5
    v60_core = (_safe_float(payload.get("v60Core")) or 0.0) >= 0.5
    v60_strong = (_safe_float(payload.get("v60Strong")) or 0.0) >= 0.5
    bull_marubozu = (_safe_float(payload.get("bullMarubozu")) or 0.0) >= 0.5
    bear_marubozu = (_safe_float(payload.get("bearMarubozu")) or 0.0) >= 0.5
    shooting_star = (_safe_float(payload.get("shootingStarLike")) or 0.0) >= 0.5
    morning_star = (_safe_float(payload.get("morningStar")) or 0.0) >= 0.5
    weekly_state = "weekly_bull" if (weekly_up or 0.0) >= (weekly_down or 0.0) else "weekly_bear"
    if (weekly_up or 0.0) < 0.50 and (weekly_down or 0.0) < 0.50:
        weekly_state = "weekly_mixed"
    monthly_state = "monthly_bull" if (monthly_up or 0.0) >= (monthly_down or 0.0) else "monthly_bear"
    if (monthly_up or 0.0) < 0.50 and (monthly_down or 0.0) < 0.50:
        monthly_state = "monthly_mixed"
    if side == "buy":
        daily_state = "daily_bull" if (bull_marubozu or morning_star or reclaim60 or v60_core or v60_strong) else "daily_other"
    else:
        daily_state = "daily_bear" if (bear_marubozu or shooting_star or not reclaim60 or not v60_core) else "daily_other"
    candle_state = "candle_bull" if (candle_body or 0.0) >= 0.6 and (lower_wick or 0.0) >= (upper_wick or 0.0) else "candle_other"
    return "|".join(
        [
            f"regime={market_regime}",
            f"monthly={monthly_box}",
            f"weekly={weekly_state}",
            f"monthly_state={monthly_state}",
            f"daily={daily_state}",
            f"candle={candle_state}",
            f"change={_change_bucket(change_pct)}",
        ]
    )


def _long_context_runup(
    *,
    opens: np.ndarray,
    lows: np.ndarray,
    entry_idx: int,
    context_window: int,
    entry_price: float,
) -> tuple[float | None, int]:
    start = max(0, int(entry_idx) - int(context_window))
    if entry_idx <= start:
        return None, 0
    anchor = float(np.nanmin(np.minimum(opens[start:entry_idx], lows[start:entry_idx])))
    if not math.isfinite(anchor) or anchor <= 0.0:
        return None, int(entry_idx - start)
    return float(max(0.0, (entry_price - anchor) / anchor)), int(entry_idx - start)


def _short_context_drop(
    *,
    opens: np.ndarray,
    highs: np.ndarray,
    entry_idx: int,
    context_window: int,
    entry_price: float,
) -> tuple[float | None, int]:
    start = max(0, int(entry_idx) - int(context_window))
    if entry_idx <= start:
        return None, 0
    anchor = float(np.nanmax(np.maximum(opens[start:entry_idx], highs[start:entry_idx])))
    if not math.isfinite(anchor) or anchor <= 0.0:
        return None, int(entry_idx - start)
    return float(max(0.0, (anchor - entry_price) / anchor)), int(entry_idx - start)


def _label_timing_long(
    *,
    entry_price: float,
    best_refined_entry_open: float | None,
    long_mfe_20: float,
    pre_signal_runup_long: float | None,
    thresholds: ActionPrecisionThresholds,
) -> tuple[str, float]:
    early_gap = 0.0
    if best_refined_entry_open is not None:
        early_gap = float(max(0.0, (entry_price - best_refined_entry_open) / entry_price))
    remaining_ratio = float(long_mfe_20 / max((pre_signal_runup_long or 0.0) + long_mfe_20, 1e-12))
    early = early_gap >= float(thresholds.timing_early_gap_threshold_pct) and early_gap > 0.0
    late = (
        (pre_signal_runup_long or 0.0) >= float(thresholds.timing_late_pre_move_threshold_pct)
        and remaining_ratio < float(thresholds.timing_late_remaining_ratio_threshold)
    )
    if late:
        label = "BUY_TOO_LATE"
    elif early:
        label = "BUY_TOO_EARLY"
    else:
        label = "BUY_ON_TIME"
    early_norm = min(1.0, early_gap / max(float(thresholds.timing_early_gap_threshold_pct), 1e-12))
    late_norm = 0.0
    denom = max(float(thresholds.timing_late_remaining_ratio_threshold), 1e-12)
    if remaining_ratio < float(thresholds.timing_late_remaining_ratio_threshold):
        late_norm = min(1.0, (float(thresholds.timing_late_remaining_ratio_threshold) - remaining_ratio) / denom)
    score = float(max(0.0, min(100.0, 100.0 * (1.0 - 0.5 * early_norm - 0.5 * late_norm))))
    return label, score


def _label_timing_short(
    *,
    entry_price: float,
    best_refined_entry_open: float | None,
    short_mfe_20: float,
    pre_signal_drop_short: float | None,
    thresholds: ActionPrecisionThresholds,
) -> tuple[str, float]:
    early_gap = 0.0
    if best_refined_entry_open is not None:
        early_gap = float(max(0.0, (best_refined_entry_open - entry_price) / entry_price))
    remaining_ratio = float(short_mfe_20 / max((pre_signal_drop_short or 0.0) + short_mfe_20, 1e-12))
    early = early_gap >= float(thresholds.timing_early_gap_threshold_pct) and early_gap > 0.0
    late = (
        (pre_signal_drop_short or 0.0) >= float(thresholds.timing_late_pre_move_threshold_pct)
        and remaining_ratio < float(thresholds.timing_late_remaining_ratio_threshold)
    )
    if late:
        label = "SELL_TOO_LATE"
    elif early:
        label = "SELL_TOO_EARLY"
    else:
        label = "SELL_ON_TIME"
    early_norm = min(1.0, early_gap / max(float(thresholds.timing_early_gap_threshold_pct), 1e-12))
    late_norm = 0.0
    denom = max(float(thresholds.timing_late_remaining_ratio_threshold), 1e-12)
    if remaining_ratio < float(thresholds.timing_late_remaining_ratio_threshold):
        late_norm = min(1.0, (float(thresholds.timing_late_remaining_ratio_threshold) - remaining_ratio) / denom)
    score = float(max(0.0, min(100.0, 100.0 * (1.0 - 0.5 * early_norm - 0.5 * late_norm))))
    return label, score


def _label_directional(
    *,
    side: str,
    mfe_20: float,
    mae_20: float,
    days_to_mfe: int | None,
    thresholds: ActionPrecisionThresholds,
) -> tuple[str, dict[str, Any]]:
    if side == "buy":
        strong = (
            mfe_20 >= float(thresholds.target_gain_pct_long)
            and mae_20 <= float(thresholds.max_adverse_pct_long)
            and (days_to_mfe is not None and days_to_mfe <= int(thresholds.max_days_to_favorable_move))
        )
        weak = mfe_20 >= float(thresholds.soft_gain_pct_long)
        label = "BUY_STRONG" if strong else "BUY_WEAK" if weak else "NO_BUY"
        target = float(thresholds.target_gain_pct_long)
        max_adverse = float(thresholds.max_adverse_pct_long)
    else:
        strong = (
            mfe_20 >= float(thresholds.target_gain_pct_short)
            and mae_20 <= float(thresholds.max_adverse_pct_short)
            and (days_to_mfe is not None and days_to_mfe <= int(thresholds.max_days_to_favorable_move))
        )
        weak = mfe_20 >= float(thresholds.soft_gain_pct_short)
        label = "SELL_STRONG" if strong else "SELL_WEAK" if weak else "NO_SELL"
        target = float(thresholds.target_gain_pct_short)
        max_adverse = float(thresholds.max_adverse_pct_short)

    gain_component = min(1.0, float(mfe_20) / max(target, 1e-12))
    risk_component = max(0.0, 1.0 - float(mae_20) / max(max_adverse, 1e-12))
    days_component = 0.0
    if days_to_mfe is not None:
        days_component = max(0.0, 1.0 - (float(days_to_mfe) / max(float(thresholds.max_days_to_favorable_move), 1.0)))
    tradeability_score = float(max(0.0, min(100.0, 100.0 * (0.45 * gain_component + 0.35 * risk_component + 0.20 * days_component))))
    return label, {
        "strong": bool(strong),
        "weak": bool(weak),
        "target": target,
        "max_adverse": max_adverse,
        "tradeability_score": tradeability_score,
    }


def _failure_kind(
    *,
    side: str,
    directional_label: str,
    timing_label: str,
    mfe_20: float,
    mae_20: float,
    days_to_mfe: int | None,
    thresholds: ActionPrecisionThresholds,
) -> str | None:
    strong = directional_label in {"BUY_STRONG", "SELL_STRONG"}
    on_time = timing_label.endswith("_ON_TIME")
    if strong and on_time:
        return None
    if timing_label.endswith("_TOO_EARLY"):
        return "too_early"
    if timing_label.endswith("_TOO_LATE"):
        return "too_late"
    if days_to_mfe is not None and days_to_mfe > int(thresholds.max_days_to_favorable_move):
        return "slow_followthrough"
    if mae_20 > (thresholds.max_adverse_pct_long if side == "buy" else thresholds.max_adverse_pct_short):
        return "adverse_first"
    if directional_label in {"BUY_WEAK", "SELL_WEAK", "NO_BUY", "NO_SELL"}:
        return "weak_direction"
    return "slow_followthrough"


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        frame.to_parquet(path, index=False)
        return
    except Exception:
        con = duckdb.connect(":memory:")
        try:
            con.register("frame_df", frame)
            con.execute(f"COPY frame_df TO '{path.as_posix()}' (FORMAT PARQUET)")
        finally:
            con.close()


def _load_active_logic(conn: duckdb.DuckDBPyConnection) -> tuple[str, str]:
    row = conn.execute(
        """
        SELECT logic_version, COALESCE(basis_version, 'basis:v1') AS basis_version
        FROM signal_logic_registry
        WHERE is_active = TRUE
        ORDER BY created_at DESC
        LIMIT 1
        """
    ).fetchone()
    if row and row[0]:
        return str(row[0]), str(row[1] or ACTIVE_BASIS_FALLBACK)
    return ACTIVE_LOGIC_FALLBACK, ACTIVE_BASIS_FALLBACK


def _load_signal_rows(
    conn: duckdb.DuckDBPyConnection,
    *,
    logic_version: str,
    basis_version: str,
    cutoff_ymd: int,
) -> pd.DataFrame:
    df = conn.execute(
        """
        SELECT
          d.dt,
          d.code,
          d.side,
          d.logic_version,
          d.basis_version,
          d.name,
          d.entry_qualified,
          d.setup_type,
          d.reason_snapshot_json,
          d.score_snapshot_json,
          d.rank_snapshot_json,
          d.forward_return_20,
          d.max_favorable_30,
          d.max_adverse_30,
          d.days_to_max_favorable_30,
          d.days_to_max_adverse_30,
          d.date_of_max_favorable_30,
          d.date_of_max_adverse_30,
          b.basis_payload_json
        FROM signal_decision_daily AS d
        LEFT JOIN signal_basis_daily AS b
          ON b.dt = d.dt
         AND b.code = d.code
         AND b.basis_version = d.basis_version
        WHERE d.logic_version = ?
          AND d.entry_qualified = TRUE
          AND d.dt <= ?
        ORDER BY d.dt, d.code
        """,
        [logic_version, int(cutoff_ymd)],
    ).fetchdf()
    if df.empty:
        return df
    df["dt"] = pd.to_numeric(df["dt"], errors="coerce").astype("Int64")
    df["code"] = df["code"].astype(str)
    df["side"] = df["side"].astype(str).str.lower()
    df["entry_qualified"] = df["entry_qualified"].astype(bool)
    df["basis_payload"] = df["basis_payload_json"].map(_parse_json)
    df["reason_snapshot"] = df["reason_snapshot_json"].map(_parse_json)
    df["score_snapshot"] = df["score_snapshot_json"].map(_parse_json)
    df["rank_snapshot"] = df["rank_snapshot_json"].map(_parse_json)
    return df


def _load_price_store(
    conn: duckdb.DuckDBPyConnection,
    *,
    codes: list[str],
    min_ymd: int,
) -> dict[str, dict[str, np.ndarray]]:
    if not codes:
        return {}
    out: dict[str, dict[str, list[float] | list[int]]] = {}
    chunk_size = 250
    for start in range(0, len(codes), chunk_size):
        chunk = codes[start : start + chunk_size]
        placeholders = ", ".join("?" for _ in chunk)
        rows = conn.execute(
            f"""
            SELECT
              CAST(code AS VARCHAR) AS code,
              {_ymd_expr("date")} AS ymd,
              CAST(o AS DOUBLE) AS o,
              CAST(h AS DOUBLE) AS h,
              CAST(l AS DOUBLE) AS l,
              CAST(c AS DOUBLE) AS c
            FROM daily_bars
            WHERE CAST(code AS VARCHAR) IN ({placeholders})
              AND {_ymd_expr("date")} >= ?
              AND COALESCE(source, 'pan') <> 'yahoo'
            ORDER BY code, ymd
            """,
            [*chunk, int(min_ymd)],
        ).fetchall()
        for code, ymd, o, h, l, c in rows:
            if code is None or ymd is None:
                continue
            key = str(code)
            bucket = out.setdefault(key, {"ymd": [], "o": [], "h": [], "l": [], "c": []})
            bucket["ymd"].append(int(ymd))
            bucket["o"].append(float(o))
            bucket["h"].append(float(h))
            bucket["l"].append(float(l))
            bucket["c"].append(float(c))
    store: dict[str, dict[str, np.ndarray]] = {}
    for code, series in out.items():
        store[code] = {
            "ymd": np.asarray(series["ymd"], dtype=np.int64),
            "o": np.asarray(series["o"], dtype=np.float64),
            "h": np.asarray(series["h"], dtype=np.float64),
            "l": np.asarray(series["l"], dtype=np.float64),
            "c": np.asarray(series["c"], dtype=np.float64),
        }
    return store


def _month_bucket(ymd: int) -> int:
    return int(str(int(ymd))[:6])


def _next_month(month: int) -> int:
    year = int(month) // 100
    mon = int(month) % 100
    if mon == 12:
        return (year + 1) * 100 + 1
    return year * 100 + mon + 1


def _contiguous_month_block(months: list[int]) -> list[int]:
    if not months:
        return []
    ordered = sorted(int(month) for month in months)
    block = [ordered[0]]
    for month in ordered[1:]:
        if month == _next_month(block[-1]):
            block.append(month)
            continue
        break
    return block


def _available_months(rows: pd.DataFrame) -> list[int]:
    months = sorted({_month_bucket(int(v)) for v in rows["dt"].tolist() if pd.notna(v)})
    return months


def _split_months(
    months: list[int],
    *,
    train_months: int,
    tune_months: int,
    validation_months: int,
    analysis_cutoff_ymd: int | None = None,
) -> SplitContract:
    sorted_months = sorted(int(m) for m in months)
    required = train_months + tune_months + validation_months
    if len(sorted_months) < required:
        raise ValueError(
            f"insufficient months for fixed split: need {required}, got {len(sorted_months)}"
        )
    selected = sorted_months[-required:]
    return SplitContract(
        train_months=tuple(selected[:train_months]),
        tune_months=tuple(selected[train_months : train_months + tune_months]),
        validation_months=tuple(selected[train_months + tune_months : required]),
        analysis_cutoff_ymd=int(analysis_cutoff_ymd) if analysis_cutoff_ymd is not None else ANALYSIS_CUTOFF_YMD,
    )


def _subset_by_month(rows: pd.DataFrame, months: tuple[int, ...]) -> pd.DataFrame:
    if rows.empty:
        return rows.copy()
    month_set = set(int(m) for m in months)
    work = rows.copy()
    work["month_bucket"] = work["dt"].astype(int).map(_month_bucket)
    return work.loc[work["month_bucket"].isin(month_set)].copy()


def _compute_replay_row(
    row: pd.Series,
    *,
    price_store: dict[str, dict[str, np.ndarray]],
    thresholds: ActionPrecisionThresholds,
) -> dict[str, Any] | None:
    code = str(row["code"])
    side = str(row["side"]).lower()
    signal_dt = int(row["dt"])
    series = price_store.get(code)
    if not series:
        return None
    ymd = series["ymd"]
    idxs = np.where(ymd == signal_dt)[0]
    if idxs.size == 0:
        return None
    idx = int(idxs[-1])
    entry_idx = idx + 1
    horizon_end = idx + 20
    if horizon_end >= len(ymd):
        return None
    entry_price = float(series["o"][entry_idx])
    future_opens = series["o"][entry_idx : horizon_end + 1]
    future_highs = series["h"][entry_idx : horizon_end + 1]
    future_lows = series["l"][entry_idx : horizon_end + 1]
    if len(future_opens) < 20 or len(future_highs) < 20 or len(future_lows) < 20:
        return None

    if side == "buy":
        mfe_20 = float(max(0.0, (float(np.nanmax(future_highs)) - entry_price) / entry_price))
        mae_20 = float(max(0.0, (entry_price - float(np.nanmin(future_lows))) / entry_price))
        days_to_mfe = int(np.nanargmax(future_highs) + 1)
        days_to_mae = int(np.nanargmin(future_lows) + 1)
    else:
        mfe_20 = float(max(0.0, (entry_price - float(np.nanmin(future_lows))) / entry_price))
        mae_20 = float(max(0.0, (float(np.nanmax(future_highs)) - entry_price) / entry_price))
        days_to_mfe = int(np.nanargmin(future_lows) + 1)
        days_to_mae = int(np.nanargmax(future_highs) + 1)

    stop_price_long = entry_price * (1.0 - float(thresholds.max_adverse_pct_long))
    stop_price_short = entry_price * (1.0 + float(thresholds.max_adverse_pct_short))
    stop_idx: int | None = None
    if side == "buy":
        breaches = np.where(future_lows <= stop_price_long)[0]
        if breaches.size:
            stop_idx = int(breaches[0] + 1)
            mfe_before_stop = float(max(0.0, (float(np.nanmax(future_highs[: stop_idx or 1])) - entry_price) / entry_price))
        else:
            mfe_before_stop = mfe_20
    else:
        breaches = np.where(future_highs >= stop_price_short)[0]
        if breaches.size:
            stop_idx = int(breaches[0] + 1)
            mfe_before_stop = float(max(0.0, (entry_price - float(np.nanmin(future_lows[: stop_idx or 1]))) / entry_price))
        else:
            mfe_before_stop = mfe_20

    cap = min(int(thresholds.timing_refinement_window_days), int(days_to_mfe))
    if cap <= 0:
        cap = int(thresholds.timing_refinement_window_days)
    if side == "buy":
        refined_window = future_opens[:cap]
        best_refined = float(np.nanmin(refined_window)) if len(refined_window) else None
        pre_signal_runup, context_days_used = _long_context_runup(
            opens=series["o"],
            lows=series["l"],
            entry_idx=entry_idx,
            context_window=int(thresholds.pre_signal_context_window_days),
            entry_price=entry_price,
        )
        timing_label, timing_score = _label_timing_long(
            entry_price=entry_price,
            best_refined_entry_open=best_refined,
            long_mfe_20=mfe_20,
            pre_signal_runup_long=pre_signal_runup,
            thresholds=thresholds,
        )
        directional_label, dir_meta = _label_directional(
            side=side,
            mfe_20=mfe_20,
            mae_20=mae_20,
            days_to_mfe=days_to_mfe,
            thresholds=thresholds,
        )
        pre_move_pct = pre_signal_runup
        remaining_ratio = float(mfe_20 / max((pre_signal_runup or 0.0) + mfe_20, 1e-12))
        entry_gap = float(max(0.0, (entry_price - float(best_refined or entry_price)) / entry_price)) if best_refined is not None else 0.0
        raw_entry_price = entry_price
    else:
        refined_window = future_opens[:cap]
        best_refined = float(np.nanmax(refined_window)) if len(refined_window) else None
        pre_signal_drop, context_days_used = _short_context_drop(
            opens=series["o"],
            highs=series["h"],
            entry_idx=entry_idx,
            context_window=int(thresholds.pre_signal_context_window_days),
            entry_price=entry_price,
        )
        timing_label, timing_score = _label_timing_short(
            entry_price=entry_price,
            best_refined_entry_open=best_refined,
            short_mfe_20=mfe_20,
            pre_signal_drop_short=pre_signal_drop,
            thresholds=thresholds,
        )
        directional_label, dir_meta = _label_directional(
            side=side,
            mfe_20=mfe_20,
            mae_20=mae_20,
            days_to_mfe=days_to_mfe,
            thresholds=thresholds,
        )
        pre_move_pct = pre_signal_drop
        remaining_ratio = float(mfe_20 / max((pre_signal_drop or 0.0) + mfe_20, 1e-12))
        entry_gap = float(max(0.0, (float(best_refined or entry_price) - entry_price) / entry_price)) if best_refined is not None else 0.0
        raw_entry_price = entry_price

    tradeability_score = float(dir_meta["tradeability_score"]) * 0.75 + float(timing_score) * 0.25
    return {
        "dt": signal_dt,
        "code": code,
        "side": side,
        "logic_version": str(row["logic_version"]),
        "basis_version": str(row["basis_version"]),
        "name": str(row.get("name") or code),
        "setup_type": str(row.get("setup_type") or "").strip() or None,
        "entry_qualified": bool(row.get("entry_qualified")),
        "signal_month": _month_bucket(signal_dt),
        "signal_date_ymd": signal_dt,
        "entry_date_ymd": int(ymd[entry_idx]),
        "entry_price": raw_entry_price,
        "long_entry_price": raw_entry_price if side == "buy" else None,
        "short_entry_price": raw_entry_price if side == "sell" else None,
        "long_mfe_20": mfe_20 if side == "buy" else None,
        "long_mae_20": mae_20 if side == "buy" else None,
        "short_mfe_20": mfe_20 if side == "sell" else None,
        "short_mae_20": mae_20 if side == "sell" else None,
        "days_to_long_mfe": days_to_mfe if side == "buy" else None,
        "days_to_long_mae": days_to_mae if side == "buy" else None,
        "days_to_short_mfe": days_to_mfe if side == "sell" else None,
        "days_to_short_mae": days_to_mae if side == "sell" else None,
        "long_mfe_before_stop_20": mfe_before_stop if side == "buy" else None,
        "short_mfe_before_stop_20": mfe_before_stop if side == "sell" else None,
        "best_refined_long_entry_open": best_refined if side == "buy" else None,
        "best_refined_short_entry_open": best_refined if side == "sell" else None,
        "long_entry_improvement_gap": entry_gap if side == "buy" else None,
        "short_entry_improvement_gap": entry_gap if side == "sell" else None,
        "pre_signal_runup_long": pre_move_pct if side == "buy" else None,
        "pre_signal_drop_short": pre_move_pct if side == "sell" else None,
        "remaining_upside_ratio_long": remaining_ratio if side == "buy" else None,
        "remaining_downside_ratio_short": remaining_ratio if side == "sell" else None,
        "long_timing_label": timing_label if side == "buy" else None,
        "short_timing_label": timing_label if side == "sell" else None,
        "long_timing_score": timing_score if side == "buy" else None,
        "short_timing_score": timing_score if side == "sell" else None,
        "tradeability_score": tradeability_score,
        "directional_label": directional_label,
        "directional_strong": bool(dir_meta["strong"]),
        "directional_weak": bool(dir_meta["weak"]),
        "state_combination": _state_combo(side, _parse_json(row.get("basis_payload"))),
        "basis_payload": row.get("basis_payload"),
        "reason_snapshot": row.get("reason_snapshot"),
        "score_snapshot": row.get("score_snapshot"),
        "rank_snapshot": row.get("rank_snapshot"),
        "context_days_used": int(context_days_used),
        "pre_signal_context_full": int(context_days_used) >= int(thresholds.pre_signal_context_window_days),
        "timing_refinement_window_days": int(thresholds.timing_refinement_window_days),
        "pre_signal_context_window_days": int(thresholds.pre_signal_context_window_days),
        "timing_early_gap_threshold_pct": float(thresholds.timing_early_gap_threshold_pct),
        "timing_late_remaining_ratio_threshold": float(thresholds.timing_late_remaining_ratio_threshold),
        "timing_late_pre_move_threshold_pct": float(thresholds.timing_late_pre_move_threshold_pct),
        "target_gain_pct_long": float(thresholds.target_gain_pct_long),
        "target_gain_pct_short": float(thresholds.target_gain_pct_short),
        "soft_gain_pct_long": float(thresholds.soft_gain_pct_long),
        "soft_gain_pct_short": float(thresholds.soft_gain_pct_short),
        "max_adverse_pct_long": float(thresholds.max_adverse_pct_long),
        "max_adverse_pct_short": float(thresholds.max_adverse_pct_short),
        "max_days_to_favorable_move": int(thresholds.max_days_to_favorable_move),
        "atr_scaled_stop_enabled": bool(thresholds.atr_scaled_stop_enabled),
        "atr_stop_multiple": thresholds.atr_stop_multiple,
        "favorable_move_days_cap": int(thresholds.max_days_to_favorable_move),
    }


def _apply_thresholds(rows: pd.DataFrame, thresholds: ActionPrecisionThresholds) -> pd.DataFrame:
    if rows.empty:
        return rows.copy()
    work = rows.copy()
    work["directional_label"] = None
    work["timing_label"] = None
    work["timing_score"] = np.nan
    work["tradeability_score"] = np.nan
    work["failure_kind"] = None
    for idx, row in work.iterrows():
        side = str(row["side"])
        directional_label, dir_meta = _label_directional(
            side=side,
            mfe_20=float(row["long_mfe_20"] if side == "buy" else row["short_mfe_20"]),
            mae_20=float(row["long_mae_20"] if side == "buy" else row["short_mae_20"]),
            days_to_mfe=_safe_int(row["days_to_long_mfe"] if side == "buy" else row["days_to_short_mfe"]),
            thresholds=thresholds,
        )
        if side == "buy":
            timing_label, timing_score = _label_timing_long(
                entry_price=float(row["entry_price"]),
                best_refined_entry_open=_safe_float(row["best_refined_long_entry_open"]),
                long_mfe_20=float(row["long_mfe_20"]),
                pre_signal_runup_long=_safe_float(row["pre_signal_runup_long"]),
                thresholds=thresholds,
            )
        else:
            timing_label, timing_score = _label_timing_short(
                entry_price=float(row["entry_price"]),
                best_refined_entry_open=_safe_float(row["best_refined_short_entry_open"]),
                short_mfe_20=float(row["short_mfe_20"]),
                pre_signal_drop_short=_safe_float(row["pre_signal_drop_short"]),
                thresholds=thresholds,
            )
        work.at[idx, "directional_label"] = directional_label
        work.at[idx, "timing_label"] = timing_label
        work.at[idx, "timing_score"] = float(timing_score)
        work.at[idx, "tradeability_score"] = float(dir_meta["tradeability_score"]) * 0.75 + float(timing_score) * 0.25
        work.at[idx, "failure_kind"] = _failure_kind(
            side=side,
            directional_label=directional_label,
            timing_label=timing_label,
            mfe_20=float(row["long_mfe_20"] if side == "buy" else row["short_mfe_20"]),
            mae_20=float(row["long_mae_20"] if side == "buy" else row["short_mae_20"]),
            days_to_mfe=_safe_int(row["days_to_long_mfe"] if side == "buy" else row["days_to_short_mfe"]),
            thresholds=thresholds,
        )
    work["is_success"] = work["failure_kind"].isna()
    return work


def _metrics_for_frame(frame: pd.DataFrame, *, side: str) -> dict[str, Any]:
    if frame.empty:
        return {
            "buy_signal_count" if side == "buy" else "sell_signal_count": 0,
            "count": 0,
            "strong_count": 0,
            "weak_count": 0,
            "no_count": 0,
            "buy_strong_rate" if side == "buy" else "sell_strong_rate": None,
            "buy_weak_rate" if side == "buy" else "sell_weak_rate": None,
            "buy_precision_strong" if side == "buy" else "sell_precision_strong": None,
            "buy_mfe_20_mean" if side == "buy" else "sell_mfe_20_mean": None,
            "buy_mfe_20_median" if side == "buy" else "sell_mfe_20_median": None,
            "buy_mae_20_mean" if side == "buy" else "sell_mae_20_mean": None,
            "buy_days_to_mfe_mean" if side == "buy" else "sell_days_to_mfe_mean": None,
            "buy_mfe_before_stop_mean" if side == "buy" else "sell_mfe_before_stop_mean": None,
            "buy_too_early_rate" if side == "buy" else "sell_too_early_rate": None,
            "buy_on_time_rate" if side == "buy" else "sell_on_time_rate": None,
            "buy_too_late_rate" if side == "buy" else "sell_too_late_rate": None,
            "buy_timing_score_mean" if side == "buy" else "sell_timing_score_mean": None,
            "precision_strong": None,
            "strong_rate": None,
            "weak_rate": None,
            "timing_score_mean": None,
            "tradeability_score_mean": None,
            "tradeability_score_median": None,
            "strong_tradeability_score_mean": None,
            "weak_tradeability_score_mean": None,
        }
    side_label = "buy" if side == "buy" else "sell"
    strong_mask = frame["directional_label"].isin({"BUY_STRONG", "SELL_STRONG"})
    weak_mask = frame["directional_label"].isin({"BUY_WEAK", "SELL_WEAK"})
    no_mask = frame["directional_label"].isin({"NO_BUY", "NO_SELL"})
    timing_col = "long_timing_label" if side == "buy" else "short_timing_label"
    timing_score_col = "long_timing_score" if side == "buy" else "short_timing_score"
    mfe_col = "long_mfe_20" if side == "buy" else "short_mfe_20"
    mae_col = "long_mae_20" if side == "buy" else "short_mae_20"
    days_col = "days_to_long_mfe" if side == "buy" else "days_to_short_mfe"
    mfe_before_stop_col = "long_mfe_before_stop_20" if side == "buy" else "short_mfe_before_stop_20"
    prefix = "buy" if side == "buy" else "sell"
    timing_early = frame[timing_col].eq(f"{prefix.upper()}_TOO_EARLY")
    timing_on = frame[timing_col].eq(f"{prefix.upper()}_ON_TIME")
    timing_late = frame[timing_col].eq(f"{prefix.upper()}_TOO_LATE")
    precision_strong = float(strong_mask.mean()) if len(frame) else None
    actionable = strong_mask | weak_mask
    strong_precision = float(strong_mask.sum() / max(int(actionable.sum()), 1))
    return {
        f"{prefix}_signal_count": int(len(frame)),
        "count": int(len(frame)),
        "strong_count": int(strong_mask.sum()),
        "weak_count": int(weak_mask.sum()),
        "no_count": int(no_mask.sum()),
        f"{prefix}_strong_rate": float(strong_mask.mean()),
        f"{prefix}_weak_rate": float(weak_mask.mean()),
        f"{prefix}_precision_strong": strong_precision,
        f"{prefix}_mfe_20_mean": float(pd.to_numeric(frame[mfe_col], errors="coerce").mean()),
        f"{prefix}_mfe_20_median": float(pd.to_numeric(frame[mfe_col], errors="coerce").median()),
        f"{prefix}_mae_20_mean": float(pd.to_numeric(frame[mae_col], errors="coerce").mean()),
        f"{prefix}_days_to_mfe_mean": float(pd.to_numeric(frame[days_col], errors="coerce").mean()),
        f"{prefix}_mfe_before_stop_mean": float(pd.to_numeric(frame[mfe_before_stop_col], errors="coerce").mean()),
        f"{prefix}_too_early_rate": float(timing_early.mean()),
        f"{prefix}_on_time_rate": float(timing_on.mean()),
        f"{prefix}_too_late_rate": float(timing_late.mean()),
        f"{prefix}_timing_score_mean": float(pd.to_numeric(frame[timing_score_col], errors="coerce").mean()),
        "precision_strong": strong_precision,
        "strong_rate": float(strong_mask.mean()),
        "weak_rate": float(weak_mask.mean()),
        "timing_score_mean": float(pd.to_numeric(frame[timing_score_col], errors="coerce").mean()),
        "tradeability_score_mean": float(pd.to_numeric(frame["tradeability_score"], errors="coerce").mean()),
        "tradeability_score_median": float(pd.to_numeric(frame["tradeability_score"], errors="coerce").median()),
        "strong_tradeability_score_mean": float(pd.to_numeric(frame.loc[strong_mask, "tradeability_score"], errors="coerce").mean()),
        "weak_tradeability_score_mean": float(pd.to_numeric(frame.loc[weak_mask, "tradeability_score"], errors="coerce").mean()),
    }


def _split_metrics(frame: pd.DataFrame, *, split: SplitContract) -> dict[str, Any]:
    if frame.empty:
        return {
            "train": {"buy": _metrics_for_frame(frame, side="buy"), "sell": _metrics_for_frame(frame, side="sell")},
            "tuning": {"buy": _metrics_for_frame(frame, side="buy"), "sell": _metrics_for_frame(frame, side="sell")},
            "validation": {"buy": _metrics_for_frame(frame, side="buy"), "sell": _metrics_for_frame(frame, side="sell")},
        }
    work = frame.copy()
    work["month_bucket"] = work["signal_month"].astype(int)
    return {
        "train": {
            "buy": _metrics_for_frame(work.loc[(work["side"] == "buy") & work["month_bucket"].isin(split.train_months)], side="buy"),
            "sell": _metrics_for_frame(work.loc[(work["side"] == "sell") & work["month_bucket"].isin(split.train_months)], side="sell"),
        },
        "tuning": {
            "buy": _metrics_for_frame(work.loc[(work["side"] == "buy") & work["month_bucket"].isin(split.tune_months)], side="buy"),
            "sell": _metrics_for_frame(work.loc[(work["side"] == "sell") & work["month_bucket"].isin(split.tune_months)], side="sell"),
        },
        "validation": {
            "buy": _metrics_for_frame(work.loc[(work["side"] == "buy") & work["month_bucket"].isin(split.validation_months)], side="buy"),
            "sell": _metrics_for_frame(work.loc[(work["side"] == "sell") & work["month_bucket"].isin(split.validation_months)], side="sell"),
        },
    }


def _dominant_failure_family(frame: pd.DataFrame, *, side: str) -> dict[str, Any]:
    work = frame.loc[frame["side"] == side].copy()
    work = work.loc[work["failure_kind"].notna()].copy()
    counts = Counter(str(v) for v in work["failure_kind"].tolist())
    top = counts.most_common(1)[0][0] if counts else "weak_direction"
    return {
        "side": side,
        "dominant_failure_family": top,
        "counts": dict(counts),
        "sample_count": int(len(work)),
    }


def _axis_candidates(axis: str, base: ActionPrecisionThresholds) -> list[ActionPrecisionThresholds]:
    if axis == "weak_direction_long":
        return [
            base,
            ActionPrecisionThresholds(**{**asdict(base), "target_gain_pct_long": 0.12}),
            ActionPrecisionThresholds(**{**asdict(base), "target_gain_pct_long": 0.14}),
        ]
    if axis == "weak_direction_short":
        return [
            base,
            ActionPrecisionThresholds(**{**asdict(base), "target_gain_pct_short": 0.12}),
            ActionPrecisionThresholds(**{**asdict(base), "target_gain_pct_short": 0.14}),
        ]
    if axis == "adverse_first_long":
        return [
            base,
            ActionPrecisionThresholds(**{**asdict(base), "max_adverse_pct_long": 0.06}),
            ActionPrecisionThresholds(**{**asdict(base), "max_adverse_pct_long": 0.05}),
        ]
    if axis == "adverse_first_short":
        return [
            base,
            ActionPrecisionThresholds(**{**asdict(base), "max_adverse_pct_short": 0.06}),
            ActionPrecisionThresholds(**{**asdict(base), "max_adverse_pct_short": 0.05}),
        ]
    if axis == "slow_followthrough_long":
        return [
            base,
            ActionPrecisionThresholds(**{**asdict(base), "max_days_to_favorable_move": 15}),
            ActionPrecisionThresholds(**{**asdict(base), "max_days_to_favorable_move": 10}),
        ]
    if axis == "slow_followthrough_short":
        return [
            base,
            ActionPrecisionThresholds(**{**asdict(base), "max_days_to_favorable_move": 15}),
            ActionPrecisionThresholds(**{**asdict(base), "max_days_to_favorable_move": 10}),
        ]
    if axis == "too_early_long":
        return [
            base,
            ActionPrecisionThresholds(**{**asdict(base), "timing_early_gap_threshold_pct": 0.03}),
            ActionPrecisionThresholds(**{**asdict(base), "timing_early_gap_threshold_pct": 0.04}),
        ]
    if axis == "too_early_short":
        return [
            base,
            ActionPrecisionThresholds(**{**asdict(base), "timing_early_gap_threshold_pct": 0.03}),
            ActionPrecisionThresholds(**{**asdict(base), "timing_early_gap_threshold_pct": 0.04}),
        ]
    if axis == "too_late_long":
        return [
            base,
            ActionPrecisionThresholds(**{**asdict(base), "timing_late_remaining_ratio_threshold": 0.40}),
            ActionPrecisionThresholds(**{**asdict(base), "timing_late_remaining_ratio_threshold": 0.45}),
        ]
    if axis == "too_late_short":
        return [
            base,
            ActionPrecisionThresholds(**{**asdict(base), "timing_late_remaining_ratio_threshold": 0.40}),
            ActionPrecisionThresholds(**{**asdict(base), "timing_late_remaining_ratio_threshold": 0.45}),
        ]
    return [base]


def _axis_from_failure(side: str, family: str) -> str:
    if family == "weak_direction":
        return f"weak_direction_{side}"
    if family == "adverse_first":
        return f"adverse_first_{side}"
    if family == "slow_followthrough":
        return f"slow_followthrough_{side}"
    if family == "too_early":
        return f"too_early_{side}"
    if family == "too_late":
        return f"too_late_{side}"
    return f"weak_direction_{side}"


def _choose_candidate(
    diagnosis_frame: pd.DataFrame,
    tuning_frame: pd.DataFrame,
    *,
    side: str,
    base: ActionPrecisionThresholds,
) -> tuple[str, ActionPrecisionThresholds, dict[str, Any]]:
    dominant = _dominant_failure_family(diagnosis_frame, side=side)
    axis = _axis_from_failure(side, dominant["dominant_failure_family"])
    candidates = _axis_candidates(axis, base)
    scored: list[dict[str, Any]] = []
    for candidate in candidates:
        labeled = _apply_thresholds(tuning_frame, candidate)
        metrics = _metrics_for_frame(labeled.loc[labeled["side"] == side], side=side)
        baseline_labeled = _apply_thresholds(tuning_frame, base)
        baseline_metrics = _metrics_for_frame(baseline_labeled.loc[baseline_labeled["side"] == side], side=side)
        precision_delta = float(metrics["precision_strong"] - baseline_metrics["precision_strong"]) if metrics["precision_strong"] is not None and baseline_metrics["precision_strong"] is not None else None
        tradeability_delta = float(metrics["tradeability_score_mean"] - baseline_metrics["tradeability_score_mean"]) if metrics["tradeability_score_mean"] is not None and baseline_metrics["tradeability_score_mean"] is not None else None
        timing_delta = float(metrics["timing_score_mean"] - baseline_metrics["timing_score_mean"]) if metrics["timing_score_mean"] is not None and baseline_metrics["timing_score_mean"] is not None else None
        strong_count_delta = int(metrics["strong_count"] - baseline_metrics["strong_count"])
        score = -1e9
        if precision_delta is not None and tradeability_delta is not None and timing_delta is not None:
            score = (precision_delta * 4.0) + (tradeability_delta * 0.01) + (timing_delta * 0.01) - (abs(strong_count_delta) * 0.02)
        scored.append(
            {
                "candidate": asdict(candidate),
                "metrics": metrics,
                "precision_delta": precision_delta,
                "tradeability_delta": tradeability_delta,
                "timing_delta": timing_delta,
                "strong_count_delta": strong_count_delta,
                "score": score,
            }
        )
    best = max(scored, key=lambda item: item["score"])
    return axis, ActionPrecisionThresholds(**best["candidate"]), {"dominant_failure": dominant, "candidate_scores": scored, "selected": best}


def _compare_payload(
    *,
    side: str,
    split_name: str,
    baseline_frame: pd.DataFrame,
    candidate_frame: pd.DataFrame,
) -> dict[str, Any]:
    baseline_metrics = _metrics_for_frame(baseline_frame.loc[baseline_frame["side"] == side], side=side)
    candidate_metrics = _metrics_for_frame(candidate_frame.loc[candidate_frame["side"] == side], side=side)
    prefix = "buy" if side == "buy" else "sell"
    delta = {
        "signal_count_loss": int(baseline_metrics["strong_count"] - candidate_metrics["strong_count"]),
        "strong_rate_delta": float(candidate_metrics["strong_rate"] - baseline_metrics["strong_rate"]) if candidate_metrics["strong_rate"] is not None else None,
        "precision_strong_delta": float(candidate_metrics["precision_strong"] - baseline_metrics["precision_strong"]) if candidate_metrics["precision_strong"] is not None else None,
        "tradeability_score_delta": float(candidate_metrics["tradeability_score_mean"] - baseline_metrics["tradeability_score_mean"]) if candidate_metrics["tradeability_score_mean"] is not None else None,
        "timing_score_delta": float(candidate_metrics["timing_score_mean"] - baseline_metrics["timing_score_mean"]) if candidate_metrics["timing_score_mean"] is not None else None,
        "mae_delta": float(candidate_metrics[f"{prefix}_mae_20_mean"] - baseline_metrics[f"{prefix}_mae_20_mean"]) if candidate_metrics[f"{prefix}_mae_20_mean"] is not None else None,
        "mfe_delta": float(candidate_metrics[f"{prefix}_mfe_20_mean"] - baseline_metrics[f"{prefix}_mfe_20_mean"]) if candidate_metrics[f"{prefix}_mfe_20_mean"] is not None else None,
        "timing_label_shift": {
            "too_early_delta": float(candidate_metrics[f"{prefix}_too_early_rate"] - baseline_metrics[f"{prefix}_too_early_rate"]) if candidate_metrics[f"{prefix}_too_early_rate"] is not None else None,
            "on_time_delta": float(candidate_metrics[f"{prefix}_on_time_rate"] - baseline_metrics[f"{prefix}_on_time_rate"]) if candidate_metrics[f"{prefix}_on_time_rate"] is not None else None,
            "too_late_delta": float(candidate_metrics[f"{prefix}_too_late_rate"] - baseline_metrics[f"{prefix}_too_late_rate"]) if candidate_metrics[f"{prefix}_too_late_rate"] is not None else None,
        },
    }
    return {
        "split": split_name,
        "baseline": baseline_metrics,
        "candidate": candidate_metrics,
        "delta": delta,
    }


def _recommendation(metrics: dict[str, Any], *, side: str) -> str:
    prefix = "buy" if side == "buy" else "sell"
    strong_rate = metrics.get(f"{prefix}_strong_rate")
    timing = metrics.get(f"{prefix}_timing_score_mean")
    count = metrics.get(f"{prefix}_signal_count") or 0
    if count < 20:
        return "diagnose_more"
    if strong_rate is not None and timing is not None and strong_rate >= 0.60 and timing >= 70.0:
        return "keep"
    if strong_rate is not None and strong_rate < 0.40:
        return "block"
    return "tighten"


def _failure_map(frame: pd.DataFrame) -> dict[str, Any]:
    buckets: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in frame.to_dict(orient="records"):
        failure = row.get("failure_kind") or "success"
        key = (str(row["side"]), str(failure), str(row["state_combination"]))
        buckets[key].append(row)
    out: list[dict[str, Any]] = []
    for (side, failure_kind, state_combo), rows in sorted(buckets.items(), key=lambda item: (item[0][0], item[0][1], -len(item[1]))):
        df = pd.DataFrame(rows)
        side_prefix = "buy" if side == "buy" else "sell"
        success_rate = float((df["failure_kind"].isna()).mean())
        mean_mfe = float(pd.to_numeric(df["long_mfe_20"] if side == "buy" else df["short_mfe_20"], errors="coerce").mean())
        mean_mae = float(pd.to_numeric(df["long_mae_20"] if side == "buy" else df["short_mae_20"], errors="coerce").mean())
        mean_days = float(pd.to_numeric(df["days_to_long_mfe"] if side == "buy" else df["days_to_short_mfe"], errors="coerce").mean())
        mean_timing = float(pd.to_numeric(df["long_timing_score"] if side == "buy" else df["short_timing_score"], errors="coerce").mean())
        recommendation = _recommendation(
            {
                f"{side_prefix}_signal_count": int(len(df)),
                f"{side_prefix}_strong_rate": float((df["directional_label"].isin({"BUY_STRONG", "SELL_STRONG"})).mean()),
                f"{side_prefix}_timing_score_mean": mean_timing,
            },
            side=side,
        )
        out.append(
            {
                "signal_side": side,
                "failure_kind": failure_kind,
                "state_combination": state_combo,
                "sample_count": int(len(df)),
                "success_rate": success_rate,
                "mean_mfe": mean_mfe,
                "mean_mae": mean_mae,
                "mean_days_to_favorable_move": mean_days,
                "mean_timing_score": mean_timing,
                "recommendation": recommendation,
            }
        )
    return {
        "schema_version": "tradex_action_precision_failure_map_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "rows": out,
    }


def _state_decomposition(frame: pd.DataFrame) -> dict[str, Any]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in frame.to_dict(orient="records"):
        key = (str(row["side"]), str(row["state_combination"]))
        grouped[key].append(row)
    rows: list[dict[str, Any]] = []
    for (side, combo), items in sorted(grouped.items(), key=lambda item: (item[0][0], -len(item[1]))):
        df = pd.DataFrame(items)
        rows.append(
            {
                "signal_side": side,
                "state_combination": combo,
                "sample_count": int(len(df)),
                "success_rate": float((df["failure_kind"].isna()).mean()),
                "mean_mfe": float(pd.to_numeric(df["long_mfe_20"] if side == "buy" else df["short_mfe_20"], errors="coerce").mean()),
                "mean_mae": float(pd.to_numeric(df["long_mae_20"] if side == "buy" else df["short_mae_20"], errors="coerce").mean()),
                "mean_days_to_favorable_move": float(pd.to_numeric(df["days_to_long_mfe"] if side == "buy" else df["days_to_short_mfe"], errors="coerce").mean()),
                "mean_timing_score": float(pd.to_numeric(df["long_timing_score"] if side == "buy" else df["short_timing_score"], errors="coerce").mean()),
            }
        )
    return {
        "schema_version": "tradex_action_precision_state_decomposition_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "rows": rows,
    }


def _emit_buy_subset(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    return frame.loc[(frame["side"] == "buy") & frame["directional_label"].isin({"BUY_STRONG", "BUY_WEAK"})].copy()


def _long_too_late_candidate_table(
    diagnosis_frame: pd.DataFrame,
    *,
    min_sample_count: int,
    block_cluster_count: int,
) -> tuple[pd.DataFrame, list[str]]:
    work = diagnosis_frame.loc[diagnosis_frame["side"] == "buy"].copy()
    if work.empty:
        empty = pd.DataFrame(
            columns=[
                "state_combination",
                "sample_count",
                "buy_precision_strong",
                "long_mfe_20_mean",
                "long_mae_20_mean",
                "buy_timing_score_mean",
                "remaining_upside_ratio_long_mean",
                "pre_signal_runup_long_mean",
                "buy_too_late_rate",
                "confidence_proxy",
                "selected_for_block",
                "recommended_action",
            ]
        )
        return empty, []
    agg = (
        work.groupby("state_combination", as_index=False)
        .agg(
            sample_count=("state_combination", "size"),
            buy_precision_strong=("directional_label", lambda s: float((s == "BUY_STRONG").mean())),
            long_mfe_20_mean=("long_mfe_20", "mean"),
            long_mae_20_mean=("long_mae_20", "mean"),
            buy_timing_score_mean=("long_timing_score", "mean"),
            remaining_upside_ratio_long_mean=("remaining_upside_ratio_long", "mean"),
            pre_signal_runup_long_mean=("pre_signal_runup_long", "mean"),
            buy_too_late_rate=("long_timing_label", lambda s: float((s == "BUY_TOO_LATE").mean())),
        )
        .sort_values(
            by=["sample_count", "buy_too_late_rate", "remaining_upside_ratio_long_mean", "pre_signal_runup_long_mean"],
            ascending=[False, False, True, False],
        )
        .reset_index(drop=True)
    )
    agg["confidence_proxy"] = (
        pd.to_numeric(agg["sample_count"], errors="coerce").fillna(0.0)
        * pd.to_numeric(agg["buy_too_late_rate"], errors="coerce").fillna(0.0)
        * (1.0 - pd.to_numeric(agg["remaining_upside_ratio_long_mean"], errors="coerce").fillna(0.0))
        * pd.to_numeric(agg["pre_signal_runup_long_mean"], errors="coerce").fillna(0.0)
    )
    eligible = agg.loc[agg["sample_count"] >= int(min_sample_count)].copy()
    eligible = eligible.sort_values(
        by=["confidence_proxy", "buy_too_late_rate", "sample_count", "buy_precision_strong"],
        ascending=[False, False, False, True],
    ).reset_index(drop=True)
    selected_block_clusters = eligible.head(int(block_cluster_count))["state_combination"].astype(str).tolist()
    selected_set = set(selected_block_clusters)
    agg["selected_for_block"] = agg["state_combination"].isin(selected_set)
    agg["recommended_action"] = np.where(
        agg["selected_for_block"],
        "block",
        np.where(agg["buy_too_late_rate"] >= 0.05, "downgrade_to_buy_weak", "keep"),
    )
    agg = agg.sort_values(
        by=["selected_for_block", "confidence_proxy", "buy_too_late_rate", "sample_count"],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)
    return agg, selected_block_clusters


def _long_weak_direction_candidate_table(
    diagnosis_frame: pd.DataFrame,
    *,
    min_sample_count: int,
    max_sample_count: int,
    block_cluster_count: int,
    excluded_clusters: set[str] | None = None,
) -> tuple[pd.DataFrame, list[str]]:
    work = diagnosis_frame.loc[diagnosis_frame["side"] == "buy"].copy()
    excluded_clusters = excluded_clusters or set()
    if work.empty:
        empty = pd.DataFrame(
            columns=[
                "state_combination",
                "sample_count",
                "buy_precision_strong",
                "long_mfe_20_mean",
                "long_mae_20_mean",
                "buy_timing_score_mean",
                "tradeability_score_mean",
                "weak_direction_rate",
                "confidence_proxy",
                "in_coverage_band",
                "in_frozen_block",
                "selected_for_block",
                "recommended_action",
            ]
        )
        return empty, []
    agg = (
        work.groupby("state_combination", as_index=False)
        .agg(
            sample_count=("state_combination", "size"),
            buy_precision_strong=("directional_label", lambda s: float((s == "BUY_STRONG").mean())),
            long_mfe_20_mean=("long_mfe_20", "mean"),
            long_mae_20_mean=("long_mae_20", "mean"),
            buy_timing_score_mean=("long_timing_score", "mean"),
            tradeability_score_mean=("tradeability_score", "mean"),
            weak_direction_rate=("failure_kind", lambda s: float((s == "weak_direction").mean())),
        )
        .reset_index(drop=True)
    )
    agg["in_frozen_block"] = agg["state_combination"].isin(excluded_clusters)
    agg["in_coverage_band"] = (
        (pd.to_numeric(agg["sample_count"], errors="coerce") >= int(min_sample_count))
        & (pd.to_numeric(agg["sample_count"], errors="coerce") <= int(max_sample_count))
        & (~agg["in_frozen_block"])
    )
    agg["confidence_proxy"] = (
        pd.to_numeric(agg["weak_direction_rate"], errors="coerce").fillna(0.0)
        * (1.0 - pd.to_numeric(agg["buy_precision_strong"], errors="coerce").fillna(0.0))
        * (1.0 - pd.to_numeric(agg["tradeability_score_mean"], errors="coerce").fillna(0.0) / 100.0)
        / (1.0 + (pd.to_numeric(agg["sample_count"], errors="coerce").fillna(0.0) - 170.0).abs() / 170.0)
    )
    eligible = agg.loc[agg["in_coverage_band"]].copy()
    eligible = eligible.sort_values(
        by=[
            "buy_precision_strong",
            "long_mfe_20_mean",
            "long_mae_20_mean",
            "tradeability_score_mean",
            "weak_direction_rate",
            "buy_timing_score_mean",
            "sample_count",
        ],
        ascending=[True, True, True, True, False, True, False],
    ).reset_index(drop=True)
    selected_block_clusters = eligible.head(int(block_cluster_count))["state_combination"].astype(str).tolist()
    selected_set = set(selected_block_clusters)
    agg["selected_for_block"] = agg["state_combination"].isin(selected_set)
    agg["recommended_action"] = np.where(
        agg["selected_for_block"],
        "block",
        np.where(agg["weak_direction_rate"] >= 0.25, "downgrade_to_buy_weak", "keep"),
    )
    agg = agg.sort_values(
        by=[
            "selected_for_block",
            "in_coverage_band",
            "confidence_proxy",
            "weak_direction_rate",
            "buy_precision_strong",
            "sample_count",
        ],
        ascending=[False, False, False, False, True, False],
    ).reset_index(drop=True)
    return agg, selected_block_clusters


def _apply_long_cluster_variant(
    frame: pd.DataFrame,
    *,
    selected_clusters: set[str],
    mode: str,
) -> pd.DataFrame:
    return _apply_long_too_late_variant(frame, selected_clusters=selected_clusters, mode=mode)


def _apply_long_too_late_variant(
    frame: pd.DataFrame,
    *,
    selected_clusters: set[str],
    mode: str,
) -> pd.DataFrame:
    work = _emit_buy_subset(frame)
    if work.empty:
        return work
    selected_mask = work["state_combination"].isin(selected_clusters)
    if mode == "block":
        return work.loc[~selected_mask].copy()
    if mode == "downgrade":
        work = work.copy()
        work.loc[selected_mask, "directional_label"] = "BUY_WEAK"
        work.loc[selected_mask, "directional_strong"] = False
        work.loc[selected_mask, "directional_weak"] = True
        return work
    raise ValueError(f"Unknown long too-late variant mode: {mode}")


def _repair_metrics(frame: pd.DataFrame) -> dict[str, Any]:
    return _metrics_for_frame(frame, side="buy")


def _variant_decision(
    *,
    baseline_metrics: dict[str, Any],
    candidate_metrics: dict[str, Any],
    tuning_metrics: dict[str, Any],
    validation_metrics: dict[str, Any],
    side: str,
) -> tuple[str, list[str]]:
    prefix = "buy" if side == "buy" else "sell"
    reasons: list[str] = []
    if candidate_metrics["strong_count"] < baseline_metrics["strong_count"]:
        reasons.append("signal_count_tightened")
    if validation_metrics[f"{prefix}_precision_strong"] is not None and baseline_metrics[f"{prefix}_precision_strong"] is not None:
        if validation_metrics[f"{prefix}_precision_strong"] > baseline_metrics[f"{prefix}_precision_strong"]:
            reasons.append("precision_strong_improved")
    if validation_metrics[f"{prefix}_timing_score_mean"] is not None and baseline_metrics[f"{prefix}_timing_score_mean"] is not None:
        if validation_metrics[f"{prefix}_timing_score_mean"] >= baseline_metrics[f"{prefix}_timing_score_mean"]:
            reasons.append("timing_score_non_degrading")
    if validation_metrics[f"{prefix}_mae_20_mean"] is not None and baseline_metrics[f"{prefix}_mae_20_mean"] is not None:
        if validation_metrics[f"{prefix}_mae_20_mean"] <= baseline_metrics[f"{prefix}_mae_20_mean"] + 0.005:
            reasons.append("mae_not_worse")
    if tuning_metrics[f"{prefix}_precision_strong"] is not None and validation_metrics[f"{prefix}_precision_strong"] is not None:
        if validation_metrics[f"{prefix}_precision_strong"] >= tuning_metrics[f"{prefix}_precision_strong"] - 0.02:
            reasons.append("validation_stable_vs_tuning")

    score = 0
    if validation_metrics[f"{prefix}_precision_strong"] is not None and baseline_metrics[f"{prefix}_precision_strong"] is not None:
        score += 2 if validation_metrics[f"{prefix}_precision_strong"] >= baseline_metrics[f"{prefix}_precision_strong"] + 0.02 else -1
    if validation_metrics[f"{prefix}_timing_score_mean"] is not None and baseline_metrics[f"{prefix}_timing_score_mean"] is not None:
        score += 2 if validation_metrics[f"{prefix}_timing_score_mean"] >= baseline_metrics[f"{prefix}_timing_score_mean"] else -1
    if validation_metrics[f"{prefix}_mae_20_mean"] is not None and baseline_metrics[f"{prefix}_mae_20_mean"] is not None:
        score += 1 if validation_metrics[f"{prefix}_mae_20_mean"] <= baseline_metrics[f"{prefix}_mae_20_mean"] + 0.005 else -1
    if candidate_metrics["strong_count"] < baseline_metrics["strong_count"] * 0.8:
        score -= 2
    if tuning_metrics[f"{prefix}_precision_strong"] is not None and candidate_metrics[f"{prefix}_precision_strong"] is not None:
        if candidate_metrics[f"{prefix}_precision_strong"] < tuning_metrics[f"{prefix}_precision_strong"] - 0.05:
            score -= 2

    if score >= 2:
        return "keep", reasons + ["validation_better_or_stable"]
    if score <= -2:
        return "drop", reasons + ["validation_or_stability_failed"]
    return "hold", reasons + ["evidence_mixed"]


def run(args: argparse.Namespace) -> dict[str, Any]:
    db_path = _resolve_db_path(args.db_path or None)
    output_dir = Path(args.output_dir).expanduser().resolve() if args.output_dir else DEFAULT_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    base_thresholds = ActionPrecisionThresholds()
    snapshot_payload = create_source_snapshot(source_db_path=str(db_path), label="action_precision_replay")
    analysis_db_path = str(snapshot_payload["snapshot_db_path"])
    with duckdb.connect(analysis_db_path, read_only=True) as conn:
        horizon_contract = _snapshot_horizon_contract(conn)
        snapshot_max_trade_date = horizon_contract["snapshot_max_trade_date"]
        replay_lookback_start_date = horizon_contract["replay_lookback_start_date"]
        last_fully_confirmable_month = horizon_contract["last_fully_confirmable_month"]
        analysis_cutoff_ymd = horizon_contract["analysis_cutoff_ymd"] or ANALYSIS_CUTOFF_YMD
        logic_version, basis_version = _load_active_logic(conn)
        raw_signals = _load_signal_rows(
            conn,
            logic_version=logic_version,
            basis_version=basis_version,
            cutoff_ymd=int(analysis_cutoff_ymd),
        )
        if raw_signals.empty:
            raise RuntimeError("No emitted signals found for the active logic version within the analysis cutoff.")
        codes = sorted(set(raw_signals["code"].astype(str).tolist()))
        min_ymd = int(raw_signals["dt"].min())
        replay_lookback_start_ymd = int(replay_lookback_start_date or min_ymd)
        price_store = _load_price_store(conn, codes=codes, min_ymd=max(19000101, replay_lookback_start_ymd - 100))

    replay_rows: list[dict[str, Any]] = []
    for row in raw_signals.itertuples(index=False):
        replay = _compute_replay_row(pd.Series(row._asdict()), price_store=price_store, thresholds=base_thresholds)
        if replay is not None:
            replay_rows.append(replay)
    frame = pd.DataFrame(replay_rows)
    if frame.empty:
        raise RuntimeError("No replay rows could be computed with full 20-day horizon.")
    if replay_lookback_start_date is not None:
        frame = frame.loc[frame["signal_date_ymd"].astype(int) >= int(replay_lookback_start_date)].copy()
    if last_fully_confirmable_month is not None:
        frame = frame.loc[frame["signal_month"].astype(int) <= int(last_fully_confirmable_month)].copy()
    if frame.empty:
        raise RuntimeError("No replay rows remained after applying the 10-year lookback and confirmable-month filter.")

    months = _available_months(frame)
    split = _split_months(
        months,
        train_months=DEFAULT_TRAIN_MONTHS,
        tune_months=DEFAULT_TUNE_MONTHS,
        validation_months=DEFAULT_VALIDATION_MONTHS,
        analysis_cutoff_ymd=int(analysis_cutoff_ymd),
    )
    frame = frame.copy()
    frame["month_bucket"] = frame["signal_month"].astype(int)
    train_frame = _subset_by_month(frame, split.train_months)
    tune_frame = _subset_by_month(frame, split.tune_months)
    validation_frame = _subset_by_month(frame, split.validation_months)

    baseline_labeled = _apply_thresholds(frame, base_thresholds)
    train_labeled = _apply_thresholds(train_frame, base_thresholds)
    tune_labeled = _apply_thresholds(tune_frame, base_thresholds)
    validation_labeled = _apply_thresholds(validation_frame, base_thresholds)

    long_axis, long_candidate_thresholds, long_selection = _choose_candidate(train_labeled, tune_labeled, side="buy", base=base_thresholds)
    short_axis, short_candidate_thresholds, short_selection = _choose_candidate(train_labeled, tune_labeled, side="sell", base=base_thresholds)

    train_long_candidate_frame = _apply_thresholds(train_frame, long_candidate_thresholds)
    tune_long_candidate_frame = _apply_thresholds(tune_frame, long_candidate_thresholds)
    validation_long_candidate_frame = _apply_thresholds(validation_frame, long_candidate_thresholds)

    train_short_candidate_frame = _apply_thresholds(train_frame, short_candidate_thresholds)
    tune_short_candidate_frame = _apply_thresholds(tune_frame, short_candidate_thresholds)
    validation_short_candidate_frame = _apply_thresholds(validation_frame, short_candidate_thresholds)

    combined_candidate_thresholds = ActionPrecisionThresholds(
        **{
            **asdict(base_thresholds),
            **{k: v for k, v in asdict(long_candidate_thresholds).items() if k != "atr_stop_multiple" and v != getattr(base_thresholds, k)},
            **{k: v for k, v in asdict(short_candidate_thresholds).items() if k != "atr_stop_multiple" and v != getattr(base_thresholds, k)},
        }
    )
    validation_combined_candidate_frame = _apply_thresholds(validation_frame, combined_candidate_thresholds)

    split_payload = {
        "schema_version": "tradex_action_precision_split_contract_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "logic_version": logic_version,
        "basis_version": basis_version,
        "snapshot_db_path": analysis_db_path,
        "snapshot_max_trade_date": snapshot_max_trade_date,
        "replay_lookback_start_date": replay_lookback_start_ymd,
        "last_fully_confirmable_month": last_fully_confirmable_month,
        "analysis_cutoff_ymd": int(analysis_cutoff_ymd),
        "train_months": list(split.train_months),
        "tune_months": list(split.tune_months),
        "validation_months": list(split.validation_months),
        "note": "Signals after the cutoff are excluded. The split is fixed by month and uses only complete 20-day replay rows.",
    }

    _write_json(
        output_dir / "action_precision_thresholds.json",
        {
            "schema_version": "tradex_action_precision_thresholds_v1",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "logic_version": logic_version,
            "basis_version": basis_version,
            "source_snapshot": snapshot_payload,
            "signal_replay_contract": {
                "entry_convention": "next_session_open_after_signal_date",
                "evaluation_horizon_days": 20,
                "source_of_truth": "signal_decision_daily.entry_qualified = TRUE joined to signal_basis_daily",
            },
            "thresholds": {
                "baseline": asdict(base_thresholds),
                "long_candidate": asdict(long_candidate_thresholds),
                "short_candidate": asdict(short_candidate_thresholds),
                "combined_candidate": asdict(combined_candidate_thresholds),
            },
            "split_contract": split_payload,
            "long_revision_axis": long_axis,
            "short_revision_axis": short_axis,
            "long_revision_selection": long_selection,
            "short_revision_selection": short_selection,
        },
    )

    sample_columns = [
        "dt",
        "code",
        "side",
        "logic_version",
        "basis_version",
        "name",
        "setup_type",
        "entry_qualified",
        "signal_month",
        "signal_date_ymd",
        "entry_date_ymd",
        "entry_price",
        "long_entry_price",
        "short_entry_price",
        "long_mfe_20",
        "long_mae_20",
        "short_mfe_20",
        "short_mae_20",
        "days_to_long_mfe",
        "days_to_long_mae",
        "days_to_short_mfe",
        "days_to_short_mae",
        "long_mfe_before_stop_20",
        "short_mfe_before_stop_20",
        "best_refined_long_entry_open",
        "best_refined_short_entry_open",
        "long_entry_improvement_gap",
        "short_entry_improvement_gap",
        "pre_signal_runup_long",
        "pre_signal_drop_short",
        "remaining_upside_ratio_long",
        "remaining_downside_ratio_short",
        "long_timing_label",
        "short_timing_label",
        "long_timing_score",
        "short_timing_score",
        "tradeability_score",
        "directional_label",
        "directional_strong",
        "directional_weak",
        "state_combination",
        "reason_snapshot",
        "score_snapshot",
        "rank_snapshot",
        "basis_payload",
        "failure_kind",
        "is_success",
        "month_bucket",
        "context_days_used",
        "pre_signal_context_full",
    ]
    sample_frame = baseline_labeled.copy()
    sample_frame["failure_kind"] = sample_frame["failure_kind"].fillna("success")
    sample_frame["is_success"] = sample_frame["is_success"].astype(bool)
    for column in ("reason_snapshot", "score_snapshot", "rank_snapshot", "basis_payload"):
        if column in sample_frame.columns:
            sample_frame[column] = sample_frame[column].map(
                lambda value: json.dumps(value, ensure_ascii=False) if isinstance(value, dict) else None
            )
    sample_frame = sample_frame[[col for col in sample_columns if col in sample_frame.columns]]
    _write_parquet(output_dir / "action_precision_samples.parquet", sample_frame)

    diagnosis_frame = pd.concat([train_labeled, tune_labeled], ignore_index=True)
    long_too_late_candidates, selected_block_clusters = _long_too_late_candidate_table(
        diagnosis_frame,
        min_sample_count=20,
        block_cluster_count=2,
    )
    if tuple(selected_block_clusters) != FROZEN_LONG_TOO_LATE_BLOCK_CLUSTERS:
        raise RuntimeError(
            "Frozen long too-late block clusters no longer match the approved revision. "
            f"Expected {list(FROZEN_LONG_TOO_LATE_BLOCK_CLUSTERS)}, got {selected_block_clusters}"
        )
    selected_block_clusters = list(FROZEN_LONG_TOO_LATE_BLOCK_CLUSTERS)
    selected_block_set = set(selected_block_clusters)

    baseline_repair_by_split = {
        "train": _emit_buy_subset(train_labeled),
        "tuning": _emit_buy_subset(tune_labeled),
        "validation": _emit_buy_subset(validation_labeled),
    }
    block_repair_by_split = {
        "train": _apply_long_too_late_variant(train_labeled, selected_clusters=selected_block_set, mode="block"),
        "tuning": _apply_long_too_late_variant(tune_labeled, selected_clusters=selected_block_set, mode="block"),
        "validation": _apply_long_too_late_variant(validation_labeled, selected_clusters=selected_block_set, mode="block"),
    }
    downgrade_repair_by_split = {
        "train": _apply_long_too_late_variant(train_labeled, selected_clusters=selected_block_set, mode="downgrade"),
        "tuning": _apply_long_too_late_variant(tune_labeled, selected_clusters=selected_block_set, mode="downgrade"),
        "validation": _apply_long_too_late_variant(validation_labeled, selected_clusters=selected_block_set, mode="downgrade"),
    }

    def _repair_compare_payload(split_name: str, *, baseline_frame: pd.DataFrame, candidate_frame: pd.DataFrame) -> dict[str, Any]:
        baseline_metrics = _repair_metrics(baseline_frame)
        candidate_metrics = _repair_metrics(candidate_frame)
        baseline_count = int(baseline_metrics["buy_signal_count"] or 0)
        candidate_count = int(candidate_metrics["buy_signal_count"] or 0)
        precision_gain = None
        if baseline_metrics["buy_precision_strong"] is not None and candidate_metrics["buy_precision_strong"] is not None:
            precision_gain = float(candidate_metrics["buy_precision_strong"] - baseline_metrics["buy_precision_strong"])
        late_delta = None
        if baseline_metrics["buy_too_late_rate"] is not None and candidate_metrics["buy_too_late_rate"] is not None:
            late_delta = float(candidate_metrics["buy_too_late_rate"] - baseline_metrics["buy_too_late_rate"])
        return {
            "split": split_name,
            "baseline": baseline_metrics,
            "candidate": candidate_metrics,
            "delta": {
                "signal_count_loss": int(baseline_count - candidate_count),
                "coverage_loss_pct": float((baseline_count - candidate_count) / baseline_count) if baseline_count else None,
                "precision_gain": precision_gain,
                "too_late_rate_delta": late_delta,
                "mfe_delta": float(candidate_metrics["buy_mfe_20_mean"] - baseline_metrics["buy_mfe_20_mean"]) if candidate_metrics["buy_mfe_20_mean"] is not None and baseline_metrics["buy_mfe_20_mean"] is not None else None,
                "mae_delta": float(candidate_metrics["buy_mae_20_mean"] - baseline_metrics["buy_mae_20_mean"]) if candidate_metrics["buy_mae_20_mean"] is not None and baseline_metrics["buy_mae_20_mean"] is not None else None,
                "timing_score_delta": float(candidate_metrics["buy_timing_score_mean"] - baseline_metrics["buy_timing_score_mean"]) if candidate_metrics["buy_timing_score_mean"] is not None and baseline_metrics["buy_timing_score_mean"] is not None else None,
                "on_time_rate_delta": float(candidate_metrics["buy_on_time_rate"] - baseline_metrics["buy_on_time_rate"]) if candidate_metrics["buy_on_time_rate"] is not None and baseline_metrics["buy_on_time_rate"] is not None else None,
            },
        }

    long_too_late_compare = {
        "schema_version": "tradex_action_precision_long_too_late_compare_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "logic_version": logic_version,
        "basis_version": basis_version,
        "split_contract": split_payload,
        "selection_contract": {
            "diagnosis_splits": ["train", "tuning"],
            "validation_split": "validation",
            "selection_metric": "confidence_proxy",
            "minimum_sample_count": 20,
            "block_cluster_count": 2,
            "selected_block_clusters": selected_block_clusters,
        },
        "baseline": _repair_metrics(baseline_repair_by_split["validation"]),
        "block": _repair_metrics(block_repair_by_split["validation"]),
        "downgrade": _repair_metrics(downgrade_repair_by_split["validation"]),
        "delta": {
            "block_vs_baseline": _repair_compare_payload(
                "validation",
                baseline_frame=baseline_repair_by_split["validation"],
                candidate_frame=block_repair_by_split["validation"],
            )["delta"],
            "downgrade_vs_baseline": _repair_compare_payload(
                "validation",
                baseline_frame=baseline_repair_by_split["validation"],
                candidate_frame=downgrade_repair_by_split["validation"],
            )["delta"],
        },
        "by_split": {
            "train": {
                "baseline": _repair_compare_payload("train", baseline_frame=baseline_repair_by_split["train"], candidate_frame=baseline_repair_by_split["train"])["baseline"],
                "block": _repair_compare_payload("train", baseline_frame=baseline_repair_by_split["train"], candidate_frame=block_repair_by_split["train"])["candidate"],
                "downgrade": _repair_compare_payload("train", baseline_frame=baseline_repair_by_split["train"], candidate_frame=downgrade_repair_by_split["train"])["candidate"],
                "block_delta": _repair_compare_payload("train", baseline_frame=baseline_repair_by_split["train"], candidate_frame=block_repair_by_split["train"])["delta"],
                "downgrade_delta": _repair_compare_payload("train", baseline_frame=baseline_repair_by_split["train"], candidate_frame=downgrade_repair_by_split["train"])["delta"],
            },
            "tuning": {
                "baseline": _repair_compare_payload("tuning", baseline_frame=baseline_repair_by_split["tuning"], candidate_frame=baseline_repair_by_split["tuning"])["baseline"],
                "block": _repair_compare_payload("tuning", baseline_frame=baseline_repair_by_split["tuning"], candidate_frame=block_repair_by_split["tuning"])["candidate"],
                "downgrade": _repair_compare_payload("tuning", baseline_frame=baseline_repair_by_split["tuning"], candidate_frame=downgrade_repair_by_split["tuning"])["candidate"],
                "block_delta": _repair_compare_payload("tuning", baseline_frame=baseline_repair_by_split["tuning"], candidate_frame=block_repair_by_split["tuning"])["delta"],
                "downgrade_delta": _repair_compare_payload("tuning", baseline_frame=baseline_repair_by_split["tuning"], candidate_frame=downgrade_repair_by_split["tuning"])["delta"],
            },
            "validation": {
                "baseline": _repair_compare_payload("validation", baseline_frame=baseline_repair_by_split["validation"], candidate_frame=baseline_repair_by_split["validation"])["baseline"],
                "block": _repair_compare_payload("validation", baseline_frame=baseline_repair_by_split["validation"], candidate_frame=block_repair_by_split["validation"])["candidate"],
                "downgrade": _repair_compare_payload("validation", baseline_frame=baseline_repair_by_split["validation"], candidate_frame=downgrade_repair_by_split["validation"])["candidate"],
                "block_delta": _repair_compare_payload("validation", baseline_frame=baseline_repair_by_split["validation"], candidate_frame=block_repair_by_split["validation"])["delta"],
                "downgrade_delta": _repair_compare_payload("validation", baseline_frame=baseline_repair_by_split["validation"], candidate_frame=downgrade_repair_by_split["validation"])["delta"],
            },
        },
    }

    block_validation_metrics = _repair_metrics(block_repair_by_split["validation"])
    downgrade_validation_metrics = _repair_metrics(downgrade_repair_by_split["validation"])
    baseline_validation_metrics = _repair_metrics(baseline_repair_by_split["validation"])
    block_precision_gain = None
    block_late_delta = None
    block_mfe_delta = None
    block_timing_delta = None
    if baseline_validation_metrics["buy_precision_strong"] is not None and block_validation_metrics["buy_precision_strong"] is not None:
        block_precision_gain = float(block_validation_metrics["buy_precision_strong"] - baseline_validation_metrics["buy_precision_strong"])
    if baseline_validation_metrics["buy_too_late_rate"] is not None and block_validation_metrics["buy_too_late_rate"] is not None:
        block_late_delta = float(block_validation_metrics["buy_too_late_rate"] - baseline_validation_metrics["buy_too_late_rate"])
    if baseline_validation_metrics["buy_mfe_20_mean"] is not None and block_validation_metrics["buy_mfe_20_mean"] is not None:
        block_mfe_delta = float(block_validation_metrics["buy_mfe_20_mean"] - baseline_validation_metrics["buy_mfe_20_mean"])
    if baseline_validation_metrics["buy_timing_score_mean"] is not None and block_validation_metrics["buy_timing_score_mean"] is not None:
        block_timing_delta = float(block_validation_metrics["buy_timing_score_mean"] - baseline_validation_metrics["buy_timing_score_mean"])
    block_coverage_loss_pct = None
    baseline_validation_count = int(baseline_validation_metrics["buy_signal_count"] or 0)
    if baseline_validation_count:
        block_coverage_loss_pct = float((baseline_validation_count - int(block_validation_metrics["buy_signal_count"] or 0)) / baseline_validation_count)
    long_too_late_decision = "hold"
    long_too_late_reasons: list[str] = []
    if block_late_delta is not None and block_precision_gain is not None and block_mfe_delta is not None and block_timing_delta is not None:
        if block_late_delta <= -0.002 and block_precision_gain > 0 and block_mfe_delta >= -0.001 and block_timing_delta >= -0.5 and (block_coverage_loss_pct is not None and block_coverage_loss_pct <= 0.15):
            long_too_late_decision = "keep"
            long_too_late_reasons = [
                "validation_too_late_rate_reduced",
                "validation_precision_improved",
                "validation_mfe_not_worse",
                "validation_timing_non_degrading",
                "coverage_loss_acceptable",
            ]
        elif block_late_delta < 0 or block_precision_gain > 0:
            long_too_late_decision = "hold"
            long_too_late_reasons = [
                "some_validation_improvement_detected",
                "coverage_or_timing_margin_not_strong_enough",
            ]
        else:
            long_too_late_decision = "drop"
            long_too_late_reasons = [
                "validation_did_not_improve_late_rate_and_precision_together",
            ]
    long_too_late_rules = {
        "schema_version": "tradex_action_precision_long_too_late_rules_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "logic_version": logic_version,
        "basis_version": basis_version,
        "split_contract": split_payload,
        "diagnosis_scope": {
            "splits": ["train", "tuning"],
            "selection_metric": "confidence_proxy",
            "minimum_sample_count": 20,
            "block_cluster_count": 2,
        },
        "selected_block_clusters": selected_block_clusters,
        "compare_modes": {
            "block": {
                "action": "block",
                "applies_to_clusters": selected_block_clusters,
                "effect": "remove_emitted_buy_rows_from_selected_clusters",
            },
            "downgrade": {
                "action": "downgrade_to_buy_weak",
                "applies_to_clusters": selected_block_clusters,
                "effect": "retain_emitted_buy_rows_and_downgrade_selected_clusters_to_BUY_WEAK",
            },
        },
        "decision": {
            "typed_decision": long_too_late_decision,
            "reasons": long_too_late_reasons,
        },
        "validation_summary": {
            "baseline": baseline_validation_metrics,
            "block": block_validation_metrics,
            "downgrade": downgrade_validation_metrics,
        },
    }

    weak_diagnosis_frame = pd.concat([train_labeled, tune_labeled], ignore_index=True)
    long_weak_direction_candidates, weak_selected_clusters = _long_weak_direction_candidate_table(
        weak_diagnosis_frame,
        min_sample_count=LONG_WEAK_DIRECTION_MIN_SAMPLE_COUNT,
        max_sample_count=LONG_WEAK_DIRECTION_MAX_SAMPLE_COUNT,
        block_cluster_count=LONG_WEAK_DIRECTION_BLOCK_CLUSTER_COUNT,
        excluded_clusters=set(FROZEN_LONG_TOO_LATE_BLOCK_CLUSTERS),
    )
    selected_weak_block_set = set(weak_selected_clusters)
    weak_baseline_repair_by_split = {
        "train": _emit_buy_subset(train_labeled),
        "tuning": _emit_buy_subset(tune_labeled),
        "validation": _emit_buy_subset(validation_labeled),
    }
    weak_block_repair_by_split = {
        "train": _apply_long_cluster_variant(train_labeled, selected_clusters=selected_weak_block_set, mode="block"),
        "tuning": _apply_long_cluster_variant(tune_labeled, selected_clusters=selected_weak_block_set, mode="block"),
        "validation": _apply_long_cluster_variant(validation_labeled, selected_clusters=selected_weak_block_set, mode="block"),
    }
    weak_downgrade_repair_by_split = {
        "train": _apply_long_cluster_variant(train_labeled, selected_clusters=selected_weak_block_set, mode="downgrade"),
        "tuning": _apply_long_cluster_variant(tune_labeled, selected_clusters=selected_weak_block_set, mode="downgrade"),
        "validation": _apply_long_cluster_variant(validation_labeled, selected_clusters=selected_weak_block_set, mode="downgrade"),
    }

    def _weak_compare_payload(split_name: str, *, baseline_frame: pd.DataFrame, candidate_frame: pd.DataFrame) -> dict[str, Any]:
        baseline_metrics = _repair_metrics(baseline_frame)
        candidate_metrics = _repair_metrics(candidate_frame)
        baseline_count = int(baseline_metrics["buy_signal_count"] or 0)
        candidate_count = int(candidate_metrics["buy_signal_count"] or 0)
        precision_gain = None
        if baseline_metrics["buy_precision_strong"] is not None and candidate_metrics["buy_precision_strong"] is not None:
            precision_gain = float(candidate_metrics["buy_precision_strong"] - baseline_metrics["buy_precision_strong"])
        mae_delta = None
        if baseline_metrics["buy_mae_20_mean"] is not None and candidate_metrics["buy_mae_20_mean"] is not None:
            mae_delta = float(candidate_metrics["buy_mae_20_mean"] - baseline_metrics["buy_mae_20_mean"])
        mfe_delta = None
        if baseline_metrics["buy_mfe_20_mean"] is not None and candidate_metrics["buy_mfe_20_mean"] is not None:
            mfe_delta = float(candidate_metrics["buy_mfe_20_mean"] - baseline_metrics["buy_mfe_20_mean"])
        timing_score_delta = None
        if baseline_metrics["buy_timing_score_mean"] is not None and candidate_metrics["buy_timing_score_mean"] is not None:
            timing_score_delta = float(candidate_metrics["buy_timing_score_mean"] - baseline_metrics["buy_timing_score_mean"])
        too_late_delta = None
        if baseline_metrics["buy_too_late_rate"] is not None and candidate_metrics["buy_too_late_rate"] is not None:
            too_late_delta = float(candidate_metrics["buy_too_late_rate"] - baseline_metrics["buy_too_late_rate"])
        on_time_delta = None
        if baseline_metrics["buy_on_time_rate"] is not None and candidate_metrics["buy_on_time_rate"] is not None:
            on_time_delta = float(candidate_metrics["buy_on_time_rate"] - baseline_metrics["buy_on_time_rate"])
        return {
            "split": split_name,
            "baseline": baseline_metrics,
            "candidate": candidate_metrics,
            "delta": {
                "signal_count_loss": int(baseline_count - candidate_count),
                "coverage_loss_pct": float((baseline_count - candidate_count) / baseline_count) if baseline_count else None,
                "precision_gain": precision_gain,
                "mfe_delta": mfe_delta,
                "mae_delta": mae_delta,
                "too_late_rate_delta": too_late_delta,
                "on_time_rate_delta": on_time_delta,
                "timing_score_delta": timing_score_delta,
            },
        }

    weak_compare = {
        "schema_version": "tradex_action_precision_long_weak_direction_compare_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "logic_version": logic_version,
        "basis_version": basis_version,
        "split_contract": split_payload,
        "selection_contract": {
            "diagnosis_splits": ["train", "tuning"],
            "validation_split": "validation",
            "selection_metric": "bounded_weak_direction_rank",
            "minimum_sample_count": LONG_WEAK_DIRECTION_MIN_SAMPLE_COUNT,
            "maximum_sample_count": LONG_WEAK_DIRECTION_MAX_SAMPLE_COUNT,
            "block_cluster_count": LONG_WEAK_DIRECTION_BLOCK_CLUSTER_COUNT,
            "selected_block_clusters": weak_selected_clusters,
            "excluded_clusters": list(FROZEN_LONG_TOO_LATE_BLOCK_CLUSTERS),
        },
        "baseline": _repair_metrics(weak_baseline_repair_by_split["validation"]),
        "block": _repair_metrics(weak_block_repair_by_split["validation"]),
        "downgrade": _repair_metrics(weak_downgrade_repair_by_split["validation"]),
        "delta": {
            "block_vs_baseline": _weak_compare_payload(
                "validation",
                baseline_frame=weak_baseline_repair_by_split["validation"],
                candidate_frame=weak_block_repair_by_split["validation"],
            )["delta"],
            "downgrade_vs_baseline": _weak_compare_payload(
                "validation",
                baseline_frame=weak_baseline_repair_by_split["validation"],
                candidate_frame=weak_downgrade_repair_by_split["validation"],
            )["delta"],
        },
        "by_split": {
            "train": {
                "baseline": _weak_compare_payload("train", baseline_frame=weak_baseline_repair_by_split["train"], candidate_frame=weak_baseline_repair_by_split["train"])["baseline"],
                "block": _weak_compare_payload("train", baseline_frame=weak_baseline_repair_by_split["train"], candidate_frame=weak_block_repair_by_split["train"])["candidate"],
                "downgrade": _weak_compare_payload("train", baseline_frame=weak_baseline_repair_by_split["train"], candidate_frame=weak_downgrade_repair_by_split["train"])["candidate"],
                "block_delta": _weak_compare_payload("train", baseline_frame=weak_baseline_repair_by_split["train"], candidate_frame=weak_block_repair_by_split["train"])["delta"],
                "downgrade_delta": _weak_compare_payload("train", baseline_frame=weak_baseline_repair_by_split["train"], candidate_frame=weak_downgrade_repair_by_split["train"])["delta"],
            },
            "tuning": {
                "baseline": _weak_compare_payload("tuning", baseline_frame=weak_baseline_repair_by_split["tuning"], candidate_frame=weak_baseline_repair_by_split["tuning"])["baseline"],
                "block": _weak_compare_payload("tuning", baseline_frame=weak_baseline_repair_by_split["tuning"], candidate_frame=weak_block_repair_by_split["tuning"])["candidate"],
                "downgrade": _weak_compare_payload("tuning", baseline_frame=weak_baseline_repair_by_split["tuning"], candidate_frame=weak_downgrade_repair_by_split["tuning"])["candidate"],
                "block_delta": _weak_compare_payload("tuning", baseline_frame=weak_baseline_repair_by_split["tuning"], candidate_frame=weak_block_repair_by_split["tuning"])["delta"],
                "downgrade_delta": _weak_compare_payload("tuning", baseline_frame=weak_baseline_repair_by_split["tuning"], candidate_frame=weak_downgrade_repair_by_split["tuning"])["delta"],
            },
            "validation": {
                "baseline": _weak_compare_payload("validation", baseline_frame=weak_baseline_repair_by_split["validation"], candidate_frame=weak_baseline_repair_by_split["validation"])["baseline"],
                "block": _weak_compare_payload("validation", baseline_frame=weak_baseline_repair_by_split["validation"], candidate_frame=weak_block_repair_by_split["validation"])["candidate"],
                "downgrade": _weak_compare_payload("validation", baseline_frame=weak_baseline_repair_by_split["validation"], candidate_frame=weak_downgrade_repair_by_split["validation"])["candidate"],
                "block_delta": _weak_compare_payload("validation", baseline_frame=weak_baseline_repair_by_split["validation"], candidate_frame=weak_block_repair_by_split["validation"])["delta"],
                "downgrade_delta": _weak_compare_payload("validation", baseline_frame=weak_baseline_repair_by_split["validation"], candidate_frame=weak_downgrade_repair_by_split["validation"])["delta"],
            },
        },
    }

    weak_block_validation_metrics = _repair_metrics(weak_block_repair_by_split["validation"])
    weak_baseline_validation_metrics = _repair_metrics(weak_baseline_repair_by_split["validation"])
    weak_precision_gain = None
    weak_mfe_delta = None
    weak_mae_delta = None
    weak_too_late_delta = None
    weak_timing_delta = None
    weak_coverage_loss_pct = None
    weak_blocked_signal_count = 0
    weak_blocked_signal_share = None
    if weak_baseline_validation_metrics["buy_signal_count"]:
        weak_blocked_signal_count = int(weak_baseline_validation_metrics["buy_signal_count"] - weak_block_validation_metrics["buy_signal_count"])
        weak_blocked_signal_share = float(weak_blocked_signal_count / weak_baseline_validation_metrics["buy_signal_count"])
        weak_coverage_loss_pct = weak_blocked_signal_share
    if weak_baseline_validation_metrics["buy_precision_strong"] is not None and weak_block_validation_metrics["buy_precision_strong"] is not None:
        weak_precision_gain = float(weak_block_validation_metrics["buy_precision_strong"] - weak_baseline_validation_metrics["buy_precision_strong"])
    if weak_baseline_validation_metrics["buy_mfe_20_mean"] is not None and weak_block_validation_metrics["buy_mfe_20_mean"] is not None:
        weak_mfe_delta = float(weak_block_validation_metrics["buy_mfe_20_mean"] - weak_baseline_validation_metrics["buy_mfe_20_mean"])
    if weak_baseline_validation_metrics["buy_mae_20_mean"] is not None and weak_block_validation_metrics["buy_mae_20_mean"] is not None:
        weak_mae_delta = float(weak_block_validation_metrics["buy_mae_20_mean"] - weak_baseline_validation_metrics["buy_mae_20_mean"])
    if weak_baseline_validation_metrics["buy_too_late_rate"] is not None and weak_block_validation_metrics["buy_too_late_rate"] is not None:
        weak_too_late_delta = float(weak_block_validation_metrics["buy_too_late_rate"] - weak_baseline_validation_metrics["buy_too_late_rate"])
    if weak_baseline_validation_metrics["buy_timing_score_mean"] is not None and weak_block_validation_metrics["buy_timing_score_mean"] is not None:
        weak_timing_delta = float(weak_block_validation_metrics["buy_timing_score_mean"] - weak_baseline_validation_metrics["buy_timing_score_mean"])
    weak_decision = "hold"
    weak_reasons: list[str] = []
    if (
        weak_precision_gain is not None
        and weak_mfe_delta is not None
        and weak_mae_delta is not None
        and weak_too_late_delta is not None
        and weak_coverage_loss_pct is not None
        and weak_precision_gain > 0
        and weak_mfe_delta >= -0.001
        and weak_mae_delta <= 0.001
        and weak_too_late_delta <= 0.0
        and weak_coverage_loss_pct <= 0.12
    ):
        weak_decision = "keep"
        weak_reasons = [
            "validation_precision_improved",
            "validation_mfe_not_worse",
            "validation_mae_not_worse",
            "validation_too_late_non_degrading",
            "coverage_loss_acceptable",
        ]
    elif (
        weak_precision_gain is not None
        and weak_mfe_delta is not None
        and weak_mae_delta is not None
        and weak_too_late_delta is not None
        and weak_coverage_loss_pct is not None
        and weak_precision_gain > 0
        and weak_coverage_loss_pct <= 0.12
        and (weak_mfe_delta >= -0.002 and weak_mae_delta <= 0.002)
    ):
        weak_decision = "hold"
        weak_reasons = [
            "validation_direction_is_favorable_but_effect_size_is_small",
        ]
    else:
        weak_decision = "drop"
        weak_reasons = [
            "validation_did_not_clear_precision_or_tradeability_gates",
        ]
    weak_rules = {
        "schema_version": "tradex_action_precision_long_weak_direction_rules_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "logic_version": logic_version,
        "basis_version": basis_version,
        "split_contract": split_payload,
        "diagnosis_scope": {
            "splits": ["train", "tuning"],
            "selection_metric": "bounded_weak_direction_rank",
            "minimum_sample_count": LONG_WEAK_DIRECTION_MIN_SAMPLE_COUNT,
            "maximum_sample_count": LONG_WEAK_DIRECTION_MAX_SAMPLE_COUNT,
            "block_cluster_count": LONG_WEAK_DIRECTION_BLOCK_CLUSTER_COUNT,
            "excluded_clusters": list(FROZEN_LONG_TOO_LATE_BLOCK_CLUSTERS),
        },
        "selected_block_clusters": weak_selected_clusters,
        "compare_modes": {
            "block": {
                "action": "block",
                "applies_to_clusters": weak_selected_clusters,
                "effect": "remove_emitted_buy_rows_from_selected_clusters",
            },
            "downgrade": {
                "action": "downgrade_to_buy_weak",
                "applies_to_clusters": weak_selected_clusters,
                "effect": "retain_emitted_buy_rows_and_downgrade_selected_clusters_to_BUY_WEAK",
            },
        },
        "decision": {
            "typed_decision": weak_decision,
            "reasons": weak_reasons,
        },
        "validation_summary": {
            "baseline": weak_baseline_validation_metrics,
            "block": weak_block_validation_metrics,
            "downgrade": _repair_metrics(weak_downgrade_repair_by_split["validation"]),
        },
    }

    baseline_by_split = {
        "train": train_labeled,
        "tuning": tune_labeled,
        "validation": validation_labeled,
    }
    candidate_by_split = {
        "buy": {
            "train": train_long_candidate_frame,
            "tuning": tune_long_candidate_frame,
            "validation": validation_long_candidate_frame,
        },
        "sell": {
            "train": train_short_candidate_frame,
            "tuning": tune_short_candidate_frame,
            "validation": validation_short_candidate_frame,
        },
    }
    long_compare = {
        "schema_version": "tradex_action_precision_long_compare_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "logic_version": logic_version,
        "basis_version": basis_version,
        "baseline_thresholds": asdict(base_thresholds),
        "candidate_thresholds": asdict(long_candidate_thresholds),
        "revision_axis": long_axis,
        "split_contract": split_payload,
        "baseline": {},
        "candidate": {},
        "delta": {},
        "by_split": {},
    }
    short_compare = {
        "schema_version": "tradex_action_precision_short_compare_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "logic_version": logic_version,
        "basis_version": basis_version,
        "baseline_thresholds": asdict(base_thresholds),
        "candidate_thresholds": asdict(short_candidate_thresholds),
        "revision_axis": short_axis,
        "split_contract": split_payload,
        "baseline": {},
        "candidate": {},
        "delta": {},
        "by_split": {},
    }

    for side, compare_payload in [("buy", long_compare), ("sell", short_compare)]:
        candidate_split_frames = candidate_by_split[side]
        compare_payload["baseline"] = _metrics_for_frame(validation_labeled.loc[validation_labeled["side"] == side], side=side)
        compare_payload["candidate"] = _metrics_for_frame(
            candidate_split_frames["validation"].loc[candidate_split_frames["validation"]["side"] == side],
            side=side,
        )
        compare_payload["delta"] = _compare_payload(
            side=side,
            split_name="validation",
            baseline_frame=validation_labeled,
            candidate_frame=candidate_split_frames["validation"],
        )["delta"]
        compare_payload["by_split"] = {
            "train": _compare_payload(
                side=side,
                split_name="train",
                baseline_frame=train_labeled,
                candidate_frame=candidate_split_frames["train"],
            ),
            "tuning": _compare_payload(
                side=side,
                split_name="tuning",
                baseline_frame=tune_labeled,
                candidate_frame=candidate_split_frames["tuning"],
            ),
            "validation": _compare_payload(
                side=side,
                split_name="validation",
                baseline_frame=validation_labeled,
                candidate_frame=candidate_split_frames["validation"],
            ),
        }

    timing_compare = {
        "schema_version": "tradex_action_precision_timing_compare_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "logic_version": logic_version,
        "basis_version": basis_version,
        "split_contract": split_payload,
        "baseline_thresholds": asdict(base_thresholds),
        "long_candidate_thresholds": asdict(long_candidate_thresholds),
        "short_candidate_thresholds": asdict(short_candidate_thresholds),
        "validation": {
            "baseline": {
                "buy": {
                    "buy_too_early_rate": long_compare["baseline"]["buy_too_early_rate"],
                    "buy_on_time_rate": long_compare["baseline"]["buy_on_time_rate"],
                    "buy_too_late_rate": long_compare["baseline"]["buy_too_late_rate"],
                    "buy_timing_score_mean": long_compare["baseline"]["buy_timing_score_mean"],
                },
                "sell": {
                    "sell_too_early_rate": short_compare["baseline"]["sell_too_early_rate"],
                    "sell_on_time_rate": short_compare["baseline"]["sell_on_time_rate"],
                    "sell_too_late_rate": short_compare["baseline"]["sell_too_late_rate"],
                    "sell_timing_score_mean": short_compare["baseline"]["sell_timing_score_mean"],
                },
            },
            "candidate": {
                "buy": {
                    "buy_too_early_rate": _metrics_for_frame(validation_long_candidate_frame.loc[validation_long_candidate_frame["side"] == "buy"], side="buy")["buy_too_early_rate"],
                    "buy_on_time_rate": _metrics_for_frame(validation_long_candidate_frame.loc[validation_long_candidate_frame["side"] == "buy"], side="buy")["buy_on_time_rate"],
                    "buy_too_late_rate": _metrics_for_frame(validation_long_candidate_frame.loc[validation_long_candidate_frame["side"] == "buy"], side="buy")["buy_too_late_rate"],
                    "buy_timing_score_mean": _metrics_for_frame(validation_long_candidate_frame.loc[validation_long_candidate_frame["side"] == "buy"], side="buy")["buy_timing_score_mean"],
                },
                "sell": {
                    "sell_too_early_rate": _metrics_for_frame(validation_short_candidate_frame.loc[validation_short_candidate_frame["side"] == "sell"], side="sell")["sell_too_early_rate"],
                    "sell_on_time_rate": _metrics_for_frame(validation_short_candidate_frame.loc[validation_short_candidate_frame["side"] == "sell"], side="sell")["sell_on_time_rate"],
                    "sell_too_late_rate": _metrics_for_frame(validation_short_candidate_frame.loc[validation_short_candidate_frame["side"] == "sell"], side="sell")["sell_too_late_rate"],
                    "sell_timing_score_mean": _metrics_for_frame(validation_short_candidate_frame.loc[validation_short_candidate_frame["side"] == "sell"], side="sell")["sell_timing_score_mean"],
                },
            },
        },
    }

    failure_map = _failure_map(baseline_labeled)
    state_decomposition = _state_decomposition(baseline_labeled)

    long_baseline_validation = _metrics_for_frame(validation_labeled.loc[validation_labeled["side"] == "buy"], side="buy")
    short_baseline_validation = _metrics_for_frame(validation_labeled.loc[validation_labeled["side"] == "sell"], side="sell")
    long_candidate_validation = _metrics_for_frame(validation_long_candidate_frame.loc[validation_long_candidate_frame["side"] == "buy"], side="buy")
    short_candidate_validation = _metrics_for_frame(validation_short_candidate_frame.loc[validation_short_candidate_frame["side"] == "sell"], side="sell")

    long_revision_selected = asdict(long_candidate_thresholds) != asdict(base_thresholds)
    short_revision_selected = asdict(short_candidate_thresholds) != asdict(base_thresholds)
    if long_revision_selected:
        long_decision, long_reasons = _variant_decision(
            baseline_metrics=long_baseline_validation,
            candidate_metrics=long_candidate_validation,
            tuning_metrics=_metrics_for_frame(tune_labeled.loc[tune_labeled["side"] == "buy"], side="buy"),
            validation_metrics=long_candidate_validation,
            side="buy",
        )
    else:
        long_decision = "hold"
        long_reasons = ["no_threshold_revision_selected", "candidate_thresholds_equal_baseline"]
    if short_revision_selected:
        short_decision, short_reasons = _variant_decision(
            baseline_metrics=short_baseline_validation,
            candidate_metrics=short_candidate_validation,
            tuning_metrics=_metrics_for_frame(tune_labeled.loc[tune_labeled["side"] == "sell"], side="sell"),
            validation_metrics=short_candidate_validation,
            side="sell",
        )
    else:
        short_decision = "hold"
        short_reasons = ["no_threshold_revision_selected", "candidate_thresholds_equal_baseline"]
    combined_validation = {
        "buy": long_candidate_validation,
        "sell": short_candidate_validation,
    }
    combined_decision = "hold"
    combined_reasons = ["combined_revision_is_evaluated_only_when_both_sides_are_material"]
    if long_decision == "keep" and short_decision == "keep":
        combined_decision = "keep"
        combined_reasons = ["both_sides_improved_on_validation"]
    elif long_decision == "drop" or short_decision == "drop":
        combined_decision = "drop"
        combined_reasons = ["one_side_failed_validation"]

    decision_payload = {
        "schema_version": "tradex_action_precision_decision_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "logic_version": logic_version,
        "basis_version": basis_version,
        "split_contract": split_payload,
        "keep_long_precision_revision": long_decision == "keep",
        "hold_long_precision_revision": long_decision == "hold",
        "drop_long_precision_revision": long_decision == "drop",
        "keep_short_precision_revision": short_decision == "keep",
        "hold_short_precision_revision": short_decision == "hold",
        "drop_short_precision_revision": short_decision == "drop",
        "keep_action_precision_revision": combined_decision == "keep",
        "hold_action_precision_revision": combined_decision == "hold",
        "drop_action_precision_revision": combined_decision == "drop",
        "long": {
            "axis": long_axis,
            "decision": long_decision,
            "reasons": long_reasons,
            "baseline_validation": long_baseline_validation,
            "candidate_validation": long_candidate_validation,
        },
        "short": {
            "axis": short_axis,
            "decision": short_decision,
            "reasons": short_reasons,
            "baseline_validation": short_baseline_validation,
            "candidate_validation": short_candidate_validation,
        },
        "combined": {
            "decision": combined_decision,
            "reasons": combined_reasons,
            "validation": combined_validation,
        },
        "remaining_risks": [
            "partial_future_months_202603_202604_were_excluded_from_split_to_keep_the_validation_block_complete",
            "timing_labels_use_future_path_information_by_design_and_are_evaluation_only",
            "state_decomposition_uses_confirmed_payload_fields_that_are_equivalent_but_not_named_exactly_like_the_prompt",
        ],
    }

    forward_confirm_payload = {
        "schema_version": "tradex_action_precision_long_too_late_forward_confirm_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "logic_version": logic_version,
        "basis_version": basis_version,
        "original_split_contract": split_payload,
        "frozen_block_clusters": list(FROZEN_LONG_TOO_LATE_BLOCK_CLUSTERS),
        "forward_confirm_period": None,
        "forward_confirm_period_reason": "no_complete_month_available_after_validation",
        "baseline_metrics": None,
        "block_metrics": None,
        "blocked_signal_count": 0,
        "blocked_signal_share": None,
        "coverage_loss_pct": None,
        "precision_gain": None,
        "mfe_delta": None,
        "mae_delta": None,
        "timing_score_delta": None,
        "decision": "hold_long_too_late_revision_needs_more_time",
        "decision_reason": "no_complete_forward_month exists after validation; 202603 and 202604 are partial in the current snapshot",
        "blocked_cluster_drilldown": [],
        "forward_month_candidates": [],
        "forward_month_candidate_replay_counts": {},
        "forward_month_candidate_raw_counts": {},
    }
    weak_forward_confirm_payload = {
        "schema_version": "tradex_action_precision_long_weak_direction_forward_confirm_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "logic_version": logic_version,
        "basis_version": basis_version,
        "original_split_contract": split_payload,
        "selected_block_clusters": list(weak_selected_clusters),
        "forward_confirm_period": None,
        "forward_confirm_period_reason": "no_complete_month_available_after_validation",
        "baseline_metrics": None,
        "block_metrics": None,
        "blocked_signal_count": 0,
        "blocked_signal_share": None,
        "coverage_loss_pct": None,
        "precision_gain": None,
        "mfe_delta": None,
        "mae_delta": None,
        "timing_score_delta": None,
        "decision": "hold_long_weak_direction_revision_needs_more_time",
        "decision_reason": "no_complete_month_available_after_validation; 202603 and 202604 are partial in the current snapshot",
        "blocked_cluster_drilldown": [],
        "forward_month_candidates": [],
        "forward_month_candidate_replay_counts": {},
        "forward_month_candidate_raw_counts": {},
    }

    forward_samples_frame = sample_frame.head(0).copy()
    forward_complete_frame = pd.DataFrame()
    forward_month_candidates: list[int] = []
    forward_month_candidate_replay_counts: dict[int, int] = {}
    forward_month_candidate_raw_counts: dict[int, int] = {}
    weak_forward_samples_frame = sample_frame.head(0).copy()
    validation_buy_frame = _emit_buy_subset(validation_labeled)
    with duckdb.connect(str(db_path), read_only=True) as conn:
        future_raw_signals = conn.execute(
            """
            SELECT
              d.dt,
              d.code,
              d.side,
              d.logic_version,
              d.basis_version,
              d.name,
              d.entry_qualified,
              d.setup_type,
              d.reason_snapshot_json,
              d.score_snapshot_json,
              d.rank_snapshot_json,
              d.forward_return_20,
              d.max_favorable_30,
              d.max_adverse_30,
              d.days_to_max_favorable_30,
              d.days_to_max_adverse_30,
              d.date_of_max_favorable_30,
              d.date_of_max_adverse_30,
              b.basis_payload_json
            FROM signal_decision_daily AS d
            LEFT JOIN signal_basis_daily AS b
              ON b.dt = d.dt
             AND b.code = d.code
             AND b.basis_version = d.basis_version
            WHERE d.logic_version = ?
              AND d.entry_qualified = TRUE
              AND d.dt > ?
            ORDER BY d.dt, d.code
            """,
            [logic_version, int(analysis_cutoff_ymd)],
        ).fetchdf()
        if not future_raw_signals.empty:
            future_raw_signals["dt"] = pd.to_numeric(future_raw_signals["dt"], errors="coerce").astype("Int64")
            future_raw_signals["code"] = future_raw_signals["code"].astype(str)
            future_raw_signals["side"] = future_raw_signals["side"].astype(str).str.lower()
            future_raw_signals["entry_qualified"] = future_raw_signals["entry_qualified"].astype(bool)
            future_raw_signals["basis_payload"] = future_raw_signals["basis_payload_json"].map(_parse_json)
            future_raw_signals["reason_snapshot"] = future_raw_signals["reason_snapshot_json"].map(_parse_json)
            future_raw_signals["score_snapshot"] = future_raw_signals["score_snapshot_json"].map(_parse_json)
            future_raw_signals["rank_snapshot"] = future_raw_signals["rank_snapshot_json"].map(_parse_json)
            future_codes = sorted(set(future_raw_signals["code"].astype(str).tolist()) | set(raw_signals["code"].astype(str).tolist()))
            forward_price_store = _load_price_store(conn, codes=future_codes, min_ymd=min_ymd - 100)
            future_replay_rows: list[dict[str, Any]] = []
            for row in future_raw_signals.itertuples(index=False):
                replay = _compute_replay_row(pd.Series(row._asdict()), price_store=forward_price_store, thresholds=base_thresholds)
                if replay is not None:
                    future_replay_rows.append(replay)
            future_replay_frame = pd.DataFrame(future_replay_rows)
            future_raw_month_counts = future_raw_signals.assign(month_bucket=future_raw_signals["dt"].astype(int).map(_month_bucket))["month_bucket"].value_counts().sort_index()
            future_replay_month_counts = (
                future_replay_frame.assign(month_bucket=future_replay_frame["dt"].astype(int).map(_month_bucket))["month_bucket"].value_counts().sort_index()
                if not future_replay_frame.empty
                else pd.Series(dtype=int)
            )
            eligible_forward_months = [
                int(month)
                for month, raw_count in future_raw_month_counts.items()
                if int(month) > int(max(split.validation_months))
                and int(future_replay_month_counts.get(month, 0)) == int(raw_count)
            ]
            forward_month_candidates = _contiguous_month_block(eligible_forward_months)
            forward_month_candidate_raw_counts = {int(month): int(count) for month, count in future_raw_month_counts.items()}
            forward_month_candidate_replay_counts = {
                int(month): int(future_replay_month_counts.get(month, 0))
                for month in future_raw_month_counts.index
            }
            if forward_month_candidates:
                forward_complete_frame = future_replay_frame.loc[
                    future_replay_frame["signal_month"].astype(int).isin(forward_month_candidates)
                ].copy()
                forward_samples_frame = _emit_buy_subset(forward_complete_frame)
                baseline_forward_metrics = _repair_metrics(forward_samples_frame)
                block_forward_frame = _apply_long_too_late_variant(
                    forward_complete_frame,
                    selected_clusters=set(FROZEN_LONG_TOO_LATE_BLOCK_CLUSTERS),
                    mode="block",
                )
                block_forward_metrics = _repair_metrics(block_forward_frame)
                blocked_signal_count = int(baseline_forward_metrics["buy_signal_count"] - block_forward_metrics["buy_signal_count"])
                blocked_signal_share = float(blocked_signal_count / baseline_forward_metrics["buy_signal_count"]) if baseline_forward_metrics["buy_signal_count"] else None
                coverage_loss_pct = blocked_signal_share
                precision_gain = None
                mfe_delta = None
                mae_delta = None
                timing_score_delta = None
                too_late_delta = None
                if baseline_forward_metrics["buy_precision_strong"] is not None and block_forward_metrics["buy_precision_strong"] is not None:
                    precision_gain = float(block_forward_metrics["buy_precision_strong"] - baseline_forward_metrics["buy_precision_strong"])
                if baseline_forward_metrics["buy_mfe_20_mean"] is not None and block_forward_metrics["buy_mfe_20_mean"] is not None:
                    mfe_delta = float(block_forward_metrics["buy_mfe_20_mean"] - baseline_forward_metrics["buy_mfe_20_mean"])
                if baseline_forward_metrics["buy_mae_20_mean"] is not None and block_forward_metrics["buy_mae_20_mean"] is not None:
                    mae_delta = float(block_forward_metrics["buy_mae_20_mean"] - baseline_forward_metrics["buy_mae_20_mean"])
                if baseline_forward_metrics["buy_timing_score_mean"] is not None and block_forward_metrics["buy_timing_score_mean"] is not None:
                    timing_score_delta = float(block_forward_metrics["buy_timing_score_mean"] - baseline_forward_metrics["buy_timing_score_mean"])
                if baseline_forward_metrics["buy_too_late_rate"] is not None and block_forward_metrics["buy_too_late_rate"] is not None:
                    too_late_delta = float(block_forward_metrics["buy_too_late_rate"] - baseline_forward_metrics["buy_too_late_rate"])
                if baseline_forward_metrics["buy_signal_count"] < 20:
                    forward_decision = "hold_long_too_late_revision_needs_more_time"
                    forward_reason = "forward_period_exists_but_sample_size_is_too_small"
                elif (
                    precision_gain is not None
                    and too_late_delta is not None
                    and mfe_delta is not None
                    and mae_delta is not None
                    and blocked_signal_share is not None
                    and precision_gain >= 0.001
                    and too_late_delta <= 0.0
                    and mfe_delta >= -0.001
                    and mae_delta <= 0.001
                    and blocked_signal_share <= 0.12
                ):
                    forward_decision = "keep_long_too_late_revision_confirmed"
                    forward_reason = "forward_period_confirms_late_block_benefit"
                elif (
                    precision_gain is not None
                    and too_late_delta is not None
                    and mfe_delta is not None
                    and mae_delta is not None
                    and (
                        precision_gain <= 0.0
                        or too_late_delta >= 0.0
                        or mfe_delta < -0.001
                        or mae_delta > 0.001
                        or (blocked_signal_share is not None and blocked_signal_share > 0.12)
                    )
                ):
                    forward_decision = "drop_long_too_late_revision"
                    forward_reason = "forward_period_does_not_clear_acceptance_thresholds"
                else:
                    forward_decision = "hold_long_too_late_revision_needs_more_time"
                    forward_reason = "forward_period_direction_is_favorable_but_effect_size_is_still_small"
                forward_confirm_payload.update(
                    {
                        "forward_confirm_period": f"{forward_month_candidates[0]}..{forward_month_candidates[-1]}",
                        "baseline_metrics": baseline_forward_metrics,
                        "block_metrics": block_forward_metrics,
                        "blocked_signal_count": blocked_signal_count,
                        "blocked_signal_share": blocked_signal_share,
                        "coverage_loss_pct": coverage_loss_pct,
                        "precision_gain": precision_gain,
                        "mfe_delta": mfe_delta,
                        "mae_delta": mae_delta,
                        "timing_score_delta": timing_score_delta,
                        "decision": forward_decision,
                        "decision_reason": forward_reason,
                    }
                )
                drilldown_rows: list[dict[str, Any]] = []
                for cluster in FROZEN_LONG_TOO_LATE_BLOCK_CLUSTERS:
                    validation_cluster = validation_buy_frame.loc[validation_buy_frame["state_combination"] == cluster]
                    forward_cluster = forward_samples_frame.loc[forward_samples_frame["state_combination"] == cluster]
                    drilldown_rows.append(
                        {
                            "state_combination": cluster,
                            "validation_period_sample_count": int(len(validation_cluster)),
                            "forward_confirm_sample_count": int(len(forward_cluster)),
                            "forward_confirm_precision": float(forward_cluster["directional_label"].eq("BUY_STRONG").mean()) if len(forward_cluster) else None,
                            "forward_confirm_mfe_20_mean": float(forward_cluster["long_mfe_20"].mean()) if len(forward_cluster) else None,
                            "forward_confirm_mae_20_mean": float(forward_cluster["long_mae_20"].mean()) if len(forward_cluster) else None,
                            "still_too_late_heavy": bool((forward_cluster["long_timing_label"] == "BUY_TOO_LATE").mean() >= 0.05) if len(forward_cluster) else None,
                            "block_still_appears_justified": bool((forward_cluster["long_timing_label"] == "BUY_TOO_LATE").mean() >= 0.05 and (forward_cluster["directional_label"].eq("BUY_STRONG").mean() <= 0.45)) if len(forward_cluster) else None,
                        }
                    )
                forward_confirm_payload["blocked_cluster_drilldown"] = drilldown_rows
            else:
                forward_confirm_payload.update(
                    {
                        "forward_confirm_period": None,
                        "baseline_metrics": None,
                        "block_metrics": None,
                        "blocked_signal_count": 0,
                        "blocked_signal_share": None,
                        "coverage_loss_pct": None,
                        "precision_gain": None,
                        "mfe_delta": None,
                        "mae_delta": None,
                        "timing_score_delta": None,
                        "decision": "hold_long_too_late_revision_needs_more_time",
                        "decision_reason": "no_complete_month_available_after_validation; 202603 and 202604 are partial in the current snapshot",
                        "blocked_cluster_drilldown": [
                            {
                                "state_combination": cluster,
                                "validation_period_sample_count": int((validation_buy_frame["state_combination"] == cluster).sum()),
                                "forward_confirm_sample_count": 0,
                                "forward_confirm_precision": None,
                                "forward_confirm_mfe_20_mean": None,
                                "forward_confirm_mae_20_mean": None,
                                "still_too_late_heavy": None,
                                "block_still_appears_justified": None,
                            }
                            for cluster in FROZEN_LONG_TOO_LATE_BLOCK_CLUSTERS
                        ],
                    }
                )
            if forward_month_candidates:
                weak_forward_samples_frame = _emit_buy_subset(forward_complete_frame)
                weak_baseline_forward_metrics = _repair_metrics(weak_forward_samples_frame)
                weak_block_forward_frame = _apply_long_cluster_variant(
                    forward_complete_frame,
                    selected_clusters=selected_weak_block_set,
                    mode="block",
                )
                weak_block_forward_metrics = _repair_metrics(weak_block_forward_frame)
                weak_blocked_signal_count = int(weak_baseline_forward_metrics["buy_signal_count"] - weak_block_forward_metrics["buy_signal_count"])
                weak_blocked_signal_share = float(weak_blocked_signal_count / weak_baseline_forward_metrics["buy_signal_count"]) if weak_baseline_forward_metrics["buy_signal_count"] else None
                weak_coverage_loss_pct = weak_blocked_signal_share
                weak_precision_gain = None
                weak_mfe_delta = None
                weak_mae_delta = None
                weak_timing_delta = None
                weak_too_late_delta = None
                if weak_baseline_forward_metrics["buy_precision_strong"] is not None and weak_block_forward_metrics["buy_precision_strong"] is not None:
                    weak_precision_gain = float(weak_block_forward_metrics["buy_precision_strong"] - weak_baseline_forward_metrics["buy_precision_strong"])
                if weak_baseline_forward_metrics["buy_mfe_20_mean"] is not None and weak_block_forward_metrics["buy_mfe_20_mean"] is not None:
                    weak_mfe_delta = float(weak_block_forward_metrics["buy_mfe_20_mean"] - weak_baseline_forward_metrics["buy_mfe_20_mean"])
                if weak_baseline_forward_metrics["buy_mae_20_mean"] is not None and weak_block_forward_metrics["buy_mae_20_mean"] is not None:
                    weak_mae_delta = float(weak_block_forward_metrics["buy_mae_20_mean"] - weak_baseline_forward_metrics["buy_mae_20_mean"])
                if weak_baseline_forward_metrics["buy_timing_score_mean"] is not None and weak_block_forward_metrics["buy_timing_score_mean"] is not None:
                    weak_timing_delta = float(weak_block_forward_metrics["buy_timing_score_mean"] - weak_baseline_forward_metrics["buy_timing_score_mean"])
                if weak_baseline_forward_metrics["buy_too_late_rate"] is not None and weak_block_forward_metrics["buy_too_late_rate"] is not None:
                    weak_too_late_delta = float(weak_block_forward_metrics["buy_too_late_rate"] - weak_baseline_forward_metrics["buy_too_late_rate"])
                if weak_baseline_forward_metrics["buy_signal_count"] < 20:
                    weak_forward_decision = "hold_long_weak_direction_revision_needs_more_time"
                    weak_forward_reason = "forward_period_exists_but_sample_size_is_too_small"
                elif (
                    weak_precision_gain is not None
                    and weak_too_late_delta is not None
                    and weak_mfe_delta is not None
                    and weak_mae_delta is not None
                    and weak_coverage_loss_pct is not None
                    and weak_precision_gain > 0
                    and weak_too_late_delta <= 0.0
                    and weak_mfe_delta >= -0.001
                    and weak_mae_delta <= 0.001
                    and weak_coverage_loss_pct <= 0.12
                ):
                    weak_forward_decision = "keep_long_weak_direction_revision_confirmed"
                    weak_forward_reason = "forward_period_confirms_weak_direction_block_benefit"
                elif (
                    weak_precision_gain is not None
                    and weak_too_late_delta is not None
                    and weak_mfe_delta is not None
                    and weak_mae_delta is not None
                    and (
                        weak_precision_gain <= 0.0
                        or weak_too_late_delta >= 0.0
                        or weak_mfe_delta < -0.001
                        or weak_mae_delta > 0.001
                        or (weak_coverage_loss_pct is not None and weak_coverage_loss_pct > 0.12)
                    )
                ):
                    weak_forward_decision = "drop_long_weak_direction_revision"
                    weak_forward_reason = "forward_period_does_not_clear_acceptance_thresholds"
                else:
                    weak_forward_decision = "hold_long_weak_direction_revision_needs_more_time"
                    weak_forward_reason = "forward_period_direction_is_favorable_but_effect_size_is_still_small"
                weak_forward_confirm_payload.update(
                    {
                        "forward_confirm_period": f"{forward_month_candidates[0]}..{forward_month_candidates[-1]}",
                        "baseline_metrics": weak_baseline_forward_metrics,
                        "block_metrics": weak_block_forward_metrics,
                        "blocked_signal_count": weak_blocked_signal_count,
                        "blocked_signal_share": weak_blocked_signal_share,
                        "coverage_loss_pct": weak_coverage_loss_pct,
                        "precision_gain": weak_precision_gain,
                        "mfe_delta": weak_mfe_delta,
                        "mae_delta": weak_mae_delta,
                        "timing_score_delta": weak_timing_delta,
                        "decision": weak_forward_decision,
                        "decision_reason": weak_forward_reason,
                    }
                )
                weak_drilldown_rows: list[dict[str, Any]] = []
                for cluster in weak_selected_clusters:
                    validation_cluster = validation_buy_frame.loc[validation_buy_frame["state_combination"] == cluster]
                    forward_cluster = weak_forward_samples_frame.loc[weak_forward_samples_frame["state_combination"] == cluster]
                    weak_drilldown_rows.append(
                        {
                            "state_combination": cluster,
                            "validation_period_sample_count": int(len(validation_cluster)),
                            "forward_confirm_sample_count": int(len(forward_cluster)),
                            "forward_confirm_precision": float(forward_cluster["directional_label"].eq("BUY_STRONG").mean()) if len(forward_cluster) else None,
                            "forward_confirm_mfe_20_mean": float(forward_cluster["long_mfe_20"].mean()) if len(forward_cluster) else None,
                            "forward_confirm_mae_20_mean": float(forward_cluster["long_mae_20"].mean()) if len(forward_cluster) else None,
                            "still_weak_direction_heavy": bool((forward_cluster["failure_kind"] == "weak_direction").mean() >= 0.05) if len(forward_cluster) else None,
                            "block_still_appears_justified": bool((forward_cluster["failure_kind"] == "weak_direction").mean() >= 0.05 and (forward_cluster["directional_label"].eq("BUY_STRONG").mean() <= 0.45)) if len(forward_cluster) else None,
                        }
                    )
                weak_forward_confirm_payload["blocked_cluster_drilldown"] = weak_drilldown_rows
            else:
                weak_forward_confirm_payload.update(
                    {
                        "forward_confirm_period": None,
                        "baseline_metrics": None,
                        "block_metrics": None,
                        "blocked_signal_count": 0,
                        "blocked_signal_share": None,
                        "coverage_loss_pct": None,
                        "precision_gain": None,
                        "mfe_delta": None,
                        "mae_delta": None,
                        "timing_score_delta": None,
                        "decision": "hold_long_weak_direction_revision_needs_more_time",
                        "decision_reason": "no_complete_month_available_after_validation; 202603 and 202604 are partial in the current snapshot",
                        "blocked_cluster_drilldown": [
                            {
                                "state_combination": cluster,
                                "validation_period_sample_count": int((validation_buy_frame["state_combination"] == cluster).sum()),
                                "forward_confirm_sample_count": 0,
                                "forward_confirm_precision": None,
                                "forward_confirm_mfe_20_mean": None,
                                "forward_confirm_mae_20_mean": None,
                                "still_weak_direction_heavy": None,
                                "block_still_appears_justified": None,
                            }
                            for cluster in weak_selected_clusters
                        ],
                    }
                )
        else:
            forward_confirm_payload["decision_reason"] = "no_future_signals_available_after_validation"

    _write_json(output_dir / "action_precision_long_compare.json", long_compare)
    _write_json(output_dir / "action_precision_short_compare.json", short_compare)
    _write_json(output_dir / "action_precision_timing_compare.json", timing_compare)
    _write_json(output_dir / "action_precision_failure_map.json", failure_map)
    _write_json(output_dir / "action_precision_state_decomposition.json", state_decomposition)
    _write_json(
        output_dir / "action_precision_long_weak_direction_candidates.json",
        {
            "schema_version": "tradex_action_precision_long_weak_direction_candidates_v1",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "logic_version": logic_version,
            "basis_version": basis_version,
            "split_contract": split_payload,
            "diagnosis_splits": ["train", "tuning"],
            "validation_split": "validation",
            "minimum_sample_count": LONG_WEAK_DIRECTION_MIN_SAMPLE_COUNT,
            "maximum_sample_count": LONG_WEAK_DIRECTION_MAX_SAMPLE_COUNT,
            "block_cluster_count": LONG_WEAK_DIRECTION_BLOCK_CLUSTER_COUNT,
            "selected_block_clusters": weak_selected_clusters,
            "frozen_too_late_block_clusters": list(FROZEN_LONG_TOO_LATE_BLOCK_CLUSTERS),
            "rows": long_weak_direction_candidates.to_dict(orient="records"),
        },
    )
    _write_json(
        output_dir / "action_precision_long_too_late_candidates.json",
        {
            "schema_version": "tradex_action_precision_long_too_late_candidates_v1",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "logic_version": logic_version,
            "basis_version": basis_version,
            "split_contract": split_payload,
            "diagnosis_splits": ["train", "tuning"],
            "validation_split": "validation",
            "selected_block_clusters": selected_block_clusters,
            "rows": long_too_late_candidates.to_dict(orient="records"),
        },
    )
    _write_json(output_dir / "action_precision_long_weak_direction_rules.json", weak_rules)
    _write_json(output_dir / "action_precision_long_weak_direction_compare.json", weak_compare)
    _write_json(output_dir / "authoritative_decision.long_weak_direction.json", weak_rules)
    _write_json(output_dir / "action_precision_long_too_late_rules.json", long_too_late_rules)
    _write_json(output_dir / "action_precision_long_too_late_compare.json", long_too_late_compare)
    _write_json(output_dir / "action_precision_long_too_late_forward_confirm.json", forward_confirm_payload)
    _write_parquet(output_dir / "action_precision_long_too_late_forward_samples.parquet", forward_samples_frame)
    _write_json(output_dir / "authoritative_decision.long_too_late_forward_confirm.json", forward_confirm_payload)
    _write_json(output_dir / "action_precision_long_weak_direction_forward_confirm.json", weak_forward_confirm_payload)
    _write_parquet(output_dir / "action_precision_long_weak_direction_forward_samples.parquet", weak_forward_samples_frame)
    _write_json(output_dir / "authoritative_decision.long_weak_direction_forward_confirm.json", weak_forward_confirm_payload)
    _write_json(output_dir / "authoritative_decision.action_precision.json", decision_payload)

    return {
        "ok": True,
        "db_path": str(db_path),
        "output_dir": str(output_dir),
        "logic_version": logic_version,
        "basis_version": basis_version,
        "train_months": list(split.train_months),
        "tune_months": list(split.tune_months),
        "validation_months": list(split.validation_months),
        "long_weak_direction_decision": weak_decision,
        "long_too_late_decision": long_too_late_decision,
        "long_decision": long_decision,
        "short_decision": short_decision,
        "combined_decision": combined_decision,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay BUY/SELL action precision with timing repair loops.")
    parser.add_argument("--db-path", default="", help="Path to authoritative snapshot DB")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args()
    result = run(args)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
