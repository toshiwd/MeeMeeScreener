from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from research.config import ResearchConfig
from research.study_scoring import build_walkforward_windows
from scripts.note_trade_repro_backtest import (
    ROUND_TRIP_COST,
    _add_daily_coordinates,
    _add_forward_path_metrics,
    _add_pattern_columns,
    _assign_period_bucket,
    _build_weekly_context_map,
    _load_daily_frame,
    _resolve_default_db_paths,
    _summary_from_returns,
)

HORIZONS = (5, 10, 20)
BAND_MODES = {
    "extrema20": 20,
    "swing10": 10,
}


def _fmt(value: Any, digits: int = 3) -> str:
    if value is None:
        return "-"
    if isinstance(value, float) and not math.isfinite(value):
        return "-"
    if isinstance(value, (int, np.integer)):
        return str(int(value))
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def _summary_from_series(values: pd.Series) -> dict[str, Any]:
    arr = pd.to_numeric(values, errors="coerce").dropna().to_numpy(dtype=np.float64, copy=False)
    if arr.size == 0:
        return {
            "n": 0,
            "mean": None,
            "median": None,
            "win_rate": None,
            "profit_factor": None,
            "sum": None,
            "mdd": None,
        }
    gains = float(arr[arr > 0.0].sum())
    losses = float(-arr[arr < 0.0].sum())
    if losses <= 1e-12:
        profit_factor = float("inf") if gains > 0.0 else 0.0
    else:
        profit_factor = float(gains / losses)
    growth = np.clip(1.0 + arr, 1e-6, 1e6)
    log_equity = np.cumsum(np.log(growth))
    log_equity = np.clip(log_equity, -60.0, 60.0)
    equity = np.exp(log_equity)
    peak = np.maximum.accumulate(equity)
    drawdown = np.where(peak > 0.0, equity / peak - 1.0, 0.0)
    return {
        "n": int(arr.size),
        "mean": float(arr.mean()),
        "median": float(np.median(arr)),
        "win_rate": float(np.mean(arr > 0.0)),
        "profit_factor": profit_factor,
        "sum": float(arr.sum()),
        "mdd": float(max(0.0, -drawdown.min())),
    }


def _resolve_db_paths(db_paths: list[Path] | None) -> list[Path]:
    if db_paths:
        existing = [path for path in db_paths if path.exists()]
        if existing:
            return existing
        raise FileNotFoundError(f"DB path not found: {db_paths}")
    return _resolve_default_db_paths()


def _add_close_execution_metrics(daily: pd.DataFrame) -> pd.DataFrame:
    daily = daily.copy()
    grouped = daily.groupby("code", sort=False)
    for horizon in HORIZONS:
        future_close = grouped["c"].shift(-horizon)
        daily[f"ret_long_close_{horizon}d"] = (future_close / daily["c"]) - 1.0 - ROUND_TRIP_COST
        daily[f"ret_short_close_{horizon}d"] = (daily["c"] / future_close) - 1.0 - ROUND_TRIP_COST
    return daily


def _add_ma_context(daily: pd.DataFrame) -> pd.DataFrame:
    daily = daily.copy()
    daily["dist_ma60"] = np.where(
        daily["ma60"].notna() & (daily["ma60"] > 0.0),
        (daily["c"] / daily["ma60"]) - 1.0,
        np.nan,
    )
    daily["ma20_vs_ma60"] = np.where(
        daily["ma20"].notna() & daily["ma60"].notna() & (daily["ma60"] > 0.0),
        (daily["ma20"] / daily["ma60"]) - 1.0,
        np.nan,
    )
    daily["ma_relation"] = np.select(
        [
            daily["ma20_vs_ma60"].notna() & (daily["ma20_vs_ma60"] <= -0.015),
            daily["ma20_vs_ma60"].notna() & (daily["ma20_vs_ma60"].abs() <= 0.015),
            daily["ma20_vs_ma60"].notna() & (daily["ma20_vs_ma60"] >= 0.015),
        ],
        ["20_below_60", "20_near_60", "20_above_60"],
        default="na",
    )
    grouped = daily.groupby("code", sort=False)
    ma20_prev5 = grouped["ma20"].shift(5)
    ma60_prev10 = grouped["ma60"].shift(10)
    daily["ma20_slope5"] = np.where(
        daily["ma20"].notna() & ma20_prev5.notna() & (ma20_prev5 > 0.0),
        (daily["ma20"] / ma20_prev5) - 1.0,
        np.nan,
    )
    daily["ma60_slope10"] = np.where(
        daily["ma60"].notna() & ma60_prev10.notna() & (ma60_prev10 > 0.0),
        (daily["ma60"] / ma60_prev10) - 1.0,
        np.nan,
    )
    daily["ma20_state"] = np.select(
        [
            daily["ma20_slope5"].notna() & (daily["ma20_slope5"] <= -0.015),
            daily["ma20_slope5"].notna() & (daily["ma20_slope5"].abs() <= 0.015),
            daily["ma20_slope5"].notna() & (daily["ma20_slope5"] >= 0.015),
        ],
        ["down", "flat", "up"],
        default="na",
    )
    daily["ma60_state"] = np.select(
        [
            daily["ma60_slope10"].notna() & (daily["ma60_slope10"] <= -0.02),
            daily["ma60_slope10"].notna() & (daily["ma60_slope10"].abs() <= 0.02),
            daily["ma60_slope10"].notna() & (daily["ma60_slope10"] >= 0.02),
        ],
        ["down", "flat", "up"],
        default="na",
    )
    daily["dist_ma20_bucket"] = np.select(
        [
            daily["dist_ma20"].isna(),
            daily["dist_ma20"] <= -0.08,
            daily["dist_ma20"] <= -0.03,
            daily["dist_ma20"] < 0.03,
            daily["dist_ma20"] < 0.08,
        ],
        ["na", "far_below", "below", "near", "above"],
        default="far_above",
    )
    daily["dist_ma60_bucket"] = np.select(
        [
            daily["dist_ma60"].isna(),
            daily["dist_ma60"] <= -0.08,
            daily["dist_ma60"] <= -0.03,
            daily["dist_ma60"] < 0.03,
            daily["dist_ma60"] < 0.08,
        ],
        ["na", "far_below", "below", "near", "above"],
        default="far_above",
    )
    return daily


def _add_bar_parts(daily: pd.DataFrame) -> pd.DataFrame:
    daily = daily.copy()
    parts = daily["bar_tag"].fillna("NA-NA-NA-NA-NA").str.split("-", expand=True)
    parts = parts.reindex(columns=range(5), fill_value="NA")
    daily["bar_dir"] = parts[0].fillna("N")
    daily["bar_size"] = parts[1].fillna("N")
    daily["bar_wick"] = parts[2].fillna("N")
    daily["bar_gap"] = parts[3].fillna("NG")
    daily["bar_break"] = parts[4].fillna("IN")
    grouped = daily.groupby("code", sort=False)
    daily["prev_bar_dir"] = grouped["bar_dir"].shift(1).fillna("N")
    daily["prev_bar_size"] = grouped["bar_size"].shift(1).fillna("N")
    daily["prev_bar_wick"] = grouped["bar_wick"].shift(1).fillna("N")
    daily["prev_bar_gap"] = grouped["bar_gap"].shift(1).fillna("NG")
    daily["prev_bar_break"] = grouped["bar_break"].shift(1).fillna("IN")
    daily["bullish_reversal_2"] = (
        daily["prev_bar_dir"].isin(["D", "X"])
        & daily["bar_dir"].eq("U")
        & (
            daily["bar_break"].eq("HB")
            | daily["bar_gap"].eq("GU")
            | daily["bar_wick"].isin(["WL", "WB"])
        )
        & (
            daily["prev_bar_break"].isin(["LB", "IN"])
            | daily["prev_bar_gap"].eq("GD")
            | daily["prev_bar_wick"].isin(["WU", "WB"])
        )
    )
    daily["bearish_reversal_2"] = (
        daily["prev_bar_dir"].isin(["U", "X"])
        & daily["bar_dir"].eq("D")
        & (
            daily["bar_break"].eq("LB")
            | daily["bar_gap"].eq("GD")
            | daily["bar_wick"].isin(["WU", "WB"])
        )
        & (
            daily["prev_bar_break"].isin(["HB", "IN"])
            | daily["prev_bar_gap"].eq("GU")
            | daily["prev_bar_wick"].isin(["WL", "WB"])
        )
    )
    daily["bullish_exhaustion_2"] = (
        daily["prev_bar_dir"].eq("U")
        & daily["bar_dir"].isin(["U", "X"])
        & daily["bar_wick"].isin(["WU", "WB"])
        & daily["bar_break"].isin(["HB", "IN"])
        & daily["dist_ma20"].fillna(0.0).ge(0.06)
    )
    daily["bearish_exhaustion_2"] = (
        daily["prev_bar_dir"].eq("D")
        & daily["bar_dir"].isin(["D", "X"])
        & daily["bar_wick"].isin(["WL", "WB"])
        & daily["bar_break"].isin(["LB", "IN"])
        & daily["dist_ma20"].fillna(0.0).le(-0.06)
    )
    daily["pattern_2_family"] = np.select(
        [
            daily["bullish_reversal_2"],
            daily["bearish_reversal_2"],
            daily["bullish_exhaustion_2"],
            daily["bearish_exhaustion_2"],
        ],
        [
            "bullish_reversal_2",
            "bearish_reversal_2",
            "bullish_exhaustion_2",
            "bearish_exhaustion_2",
        ],
        default="neutral_2",
    )
    return daily


def _load_base_frame(db_paths: list[Path]) -> pd.DataFrame:
    daily = _load_daily_frame(db_paths)
    weekly_map = _build_weekly_context_map(daily)
    daily = daily.merge(weekly_map, how="left", on=["code", "week_end"])
    daily["week_slope"] = daily["week_slope"].fillna("na")
    for col in ("week_lower_high", "week_near_prev_low", "week_support_hold", "week_climactic"):
        daily[col] = daily[col].fillna(False).astype(bool)
    daily["period_bucket"] = _assign_period_bucket(daily["dt"])
    daily["month_bucket"] = daily["dt"].dt.to_period("M").astype(str)
    daily = _add_daily_coordinates(daily)
    daily = _add_pattern_columns(daily)
    daily = _add_forward_path_metrics(daily)
    daily = _add_close_execution_metrics(daily)
    daily = _add_ma_context(daily)
    daily = _add_bar_parts(daily)
    return daily


def _compute_band_features(daily: pd.DataFrame, mode: str) -> pd.DataFrame:
    if mode not in BAND_MODES:
        raise ValueError(f"Unknown band mode: {mode}")
    lookback = BAND_MODES[mode]
    suffix = f"_{mode}"
    rows: list[pd.DataFrame] = []
    for _, group in daily.groupby("code", sort=False):
        g = group.sort_values("dt").reset_index(drop=True).copy()
        ref_high = g["h"].shift(1).rolling(lookback, min_periods=max(5, lookback // 2)).max()
        ref_low = g["l"].shift(1).rolling(lookback, min_periods=max(5, lookback // 2)).min()
        width_pct = np.where(
            g["atr20"].notna() & (g["c"] > 0.0),
            (g["atr20"] / g["c"]) * 0.75,
            np.nan,
        )
        width_pct = pd.Series(width_pct, index=g.index).clip(0.01, 0.03)
        g[f"support_ref{suffix}"] = ref_low
        g[f"resistance_ref{suffix}"] = ref_high
        g[f"support_low{suffix}"] = ref_low * (1.0 - width_pct)
        g[f"support_high{suffix}"] = ref_low * (1.0 + width_pct)
        g[f"resistance_low{suffix}"] = ref_high * (1.0 - width_pct)
        g[f"resistance_high{suffix}"] = ref_high * (1.0 + width_pct)
        g[f"band_width_pct{suffix}"] = width_pct
        g[f"support_touch{suffix}"] = (
            g[f"support_low{suffix}"].notna()
            & g[f"support_high{suffix}"].notna()
            & (g["l"] <= g[f"support_high{suffix}"])
            & (g["h"] >= g[f"support_low{suffix}"])
        )
        g[f"resistance_touch{suffix}"] = (
            g[f"resistance_low{suffix}"].notna()
            & g[f"resistance_high{suffix}"].notna()
            & (g["h"] >= g[f"resistance_low{suffix}"])
            & (g["l"] <= g[f"resistance_high{suffix}"])
        )
        g[f"support_hold{suffix}"] = g[f"support_touch{suffix}"] & (g["c"] >= g[f"support_high{suffix}"])
        g[f"reclaim_support{suffix}"] = (
            g["prev_c"].notna()
            & g[f"support_low{suffix}"].notna()
            & (g["prev_c"] < g[f"support_low{suffix}"])
            & (g["c"] >= g[f"support_high{suffix}"])
        )
        g[f"lose_support{suffix}"] = g[f"support_low{suffix}"].notna() & (g["c"] < g[f"support_low{suffix}"])
        g[f"reject_resistance{suffix}"] = g[f"resistance_touch{suffix}"] & (g["c"] <= g[f"resistance_low{suffix}"])
        g[f"breakout_resistance{suffix}"] = (
            g[f"resistance_high{suffix}"].notna()
            & g["prev_c"].notna()
            & (g["prev_c"] <= g[f"resistance_high{suffix}"])
            & (g["c"] >= g[f"resistance_high{suffix}"])
        )
        g[f"reclaim_breakout{suffix}"] = (
            g[f"resistance_low{suffix}"].notna()
            & g["prev_c"].notna()
            & (g["prev_c"] < g[f"resistance_low{suffix}"])
            & (g["c"] >= g[f"resistance_high{suffix}"])
        )
        support_counts = g[f"support_touch{suffix}"].rolling(20, min_periods=1).sum()
        resist_counts = g[f"resistance_touch{suffix}"].rolling(20, min_periods=1).sum()
        g[f"support_touch_count{suffix}"] = support_counts.astype(float)
        g[f"resistance_touch_count{suffix}"] = resist_counts.astype(float)
        support_age: list[float] = []
        resistance_age: list[float] = []
        last_support = None
        last_resistance = None
        for idx in range(len(g)):
            if bool(g.loc[idx, f"support_touch{suffix}"]):
                last_support = idx
            if bool(g.loc[idx, f"resistance_touch{suffix}"]):
                last_resistance = idx
            support_age.append(float(idx - last_support) if last_support is not None else np.nan)
            resistance_age.append(float(idx - last_resistance) if last_resistance is not None else np.nan)
        g[f"support_touch_age{suffix}"] = support_age
        g[f"resistance_touch_age{suffix}"] = resistance_age
        span = g[f"resistance_ref{suffix}"] - g[f"support_ref{suffix}"]
        pos = np.where(
            span.notna() & (span > 0.0),
            (g["c"] - g[f"support_ref{suffix}"]) / span,
            np.nan,
        )
        pos_series = pd.Series(pos, index=g.index)
        g[f"band_zone{suffix}"] = np.select(
            [
                pos_series.isna(),
                pos_series <= 0.25,
                pos_series < 0.75,
                pos_series <= 1.0,
            ],
            ["na", "lower", "mid", "upper"],
            default="breakout",
        )
        rows.append(g)
    return pd.concat(rows, ignore_index=True) if rows else daily.iloc[0:0].copy()


def _bucket_touch_count(value: Any) -> str:
    if value is None or (isinstance(value, float) and not math.isfinite(value)):
        return "na"
    try:
        v = float(value)
    except (TypeError, ValueError):
        return "na"
    if v <= 1.0:
        return "0_1"
    if v <= 3.0:
        return "2_3"
    return "4p"


def _bucket_age(value: Any) -> str:
    if value is None or (isinstance(value, float) and not math.isfinite(value)):
        return "na"
    try:
        v = int(float(value))
    except (TypeError, ValueError):
        return "na"
    if v <= 3:
        return "0_3"
    if v <= 10:
        return "4_10"
    return "11p"


def _state_label_series(daily: pd.DataFrame, mode: str) -> pd.DataFrame:
    suffix = f"_{mode}"
    out = daily.copy()
    support_touch = out[f"support_touch{suffix}"]
    support_hold = out[f"support_hold{suffix}"]
    reclaim_support = out[f"reclaim_support{suffix}"]
    lose_support = out[f"lose_support{suffix}"]
    reject_resistance = out[f"reject_resistance{suffix}"]
    breakout_resistance = out[f"breakout_resistance{suffix}"]
    reclaim_breakout = out[f"reclaim_breakout{suffix}"]
    band_zone = out[f"band_zone{suffix}"]

    long_support_watch = out["week_slope"].isin(["flat", "up"]) & out["week_support_hold"] & (~out["week_climactic"])
    long_support_entry = out["week_slope"].isin(["flat", "up"]) & (~out["week_climactic"])
    short_resistance_watch = out["week_slope"].isin(["flat", "down"]) & (~out["week_climactic"])
    short_resistance_entry = out["week_slope"].isin(["flat", "down"]) & (~out["week_climactic"])
    long_near_ma = out["ma_relation"].isin(["20_below_60", "20_near_60"])
    short_near_ma = out["ma_relation"].isin(["20_near_60", "20_above_60"])

    long_entry_mask = (
        (support_hold | reclaim_support)
        & long_support_entry
        & long_near_ma
        & band_zone.isin(["lower", "mid"])
        & out["bullish_reversal_2"]
        & (~out["bullish_exhaustion_2"])
    )
    long_hold_mask = (
        (support_hold | reclaim_support)
        & long_support_watch
        & long_near_ma
        & band_zone.isin(["lower", "mid"])
        & out["dist_ma20"].between(-0.03, 0.06)
        & (~out["bullish_exhaustion_2"])
        & (~long_entry_mask)
    )
    long_entry_watch_mask = (
        support_touch
        & long_support_watch
        & band_zone.isin(["lower", "mid"])
        & (~long_entry_mask)
        & (~long_hold_mask)
        & (~out["climactic_day"])
    )
    long_takeprofit_mask = (
        (~long_entry_mask)
        & (~long_hold_mask)
        & (~long_entry_watch_mask)
        & (
            out["climactic_day"]
            | out["bullish_exhaustion_2"]
            | reject_resistance
            | (band_zone.isin(["upper", "breakout"]) & out["dist_ma20"].fillna(0.0).ge(0.06))
        )
    )
    long_exit_mask = (
        (~long_entry_mask)
        & (~long_hold_mask)
        & (~long_entry_watch_mask)
        & (~long_takeprofit_mask)
        & (
            lose_support
            | out["support_break_day"]
            | out["bearish_reversal_2"]
            | (out["ma60_state"].eq("down") & band_zone.isin(["lower", "mid"]) & out["dist_ma20"].fillna(0.0).lt(-0.03))
        )
    )

    short_entry_mask = (
        reject_resistance
        & short_resistance_entry
        & short_near_ma
        & band_zone.isin(["upper", "breakout"])
        & out["bearish_reversal_2"]
        & (~out["bearish_exhaustion_2"])
    )
    short_hold_mask = (
        reject_resistance
        & short_resistance_watch
        & short_near_ma
        & band_zone.isin(["upper", "breakout"])
        & out["dist_ma20"].fillna(0.0).ge(0.03)
        & (~out["bearish_exhaustion_2"])
        & (~short_entry_mask)
    )
    short_entry_watch_mask = (
        out[f"resistance_touch{suffix}"]
        & short_resistance_watch
        & band_zone.isin(["upper", "breakout"])
        & (~short_entry_mask)
        & (~short_hold_mask)
        & (~out["climactic_day"])
    )
    short_takeprofit_mask = (
        (~short_entry_mask)
        & (~short_hold_mask)
        & (~short_entry_watch_mask)
        & (
            out["climactic_day"]
            | out["bearish_exhaustion_2"]
            | support_hold
            | reclaim_support
            | (band_zone.isin(["lower", "mid"]) & out["dist_ma20"].fillna(0.0).le(-0.06))
        )
    )
    short_exit_mask = (
        (~short_entry_mask)
        & (~short_hold_mask)
        & (~short_entry_watch_mask)
        & (~short_takeprofit_mask)
        & (
            breakout_resistance
            | reclaim_breakout
            | out["bullish_reversal_2"]
            | (out["ma60_state"].eq("up") & band_zone.isin(["upper", "breakout"]) & out["dist_ma20"].fillna(0.0).gt(0.03))
        )
    )

    out["long_state_label"] = np.select(
        [long_exit_mask, long_takeprofit_mask, long_entry_mask, long_hold_mask, long_entry_watch_mask],
        ["long_exit", "long_takeprofit", "long_entry", "long_hold", "long_entry_watch"],
        default="na",
    )
    out["short_state_label"] = np.select(
        [short_exit_mask, short_takeprofit_mask, short_entry_mask, short_hold_mask, short_entry_watch_mask],
        ["short_exit", "short_takeprofit", "short_entry", "short_hold", "short_entry_watch"],
        default="na",
    )
    out["state_combo"] = (
        band_zone.astype(str)
        + "|"
        + out["ma60_state"].astype(str)
        + "|"
        + out["ma_relation"].astype(str)
        + "|"
        + out["pattern_2_family"].astype(str)
    )
    return out


def _signal_return_mode(state_label: str) -> str:
    if state_label.startswith("long_"):
        return "long" if state_label in {"long_entry_watch", "long_entry", "long_hold"} else "short"
    if state_label.startswith("short_"):
        return "short" if state_label in {"short_entry_watch", "short_entry", "short_hold"} else "long"
    return "na"


def _add_signal_columns(daily: pd.DataFrame) -> pd.DataFrame:
    daily = daily.copy()
    grouped = daily.groupby("code", sort=False)
    long_ret_cols: dict[int, str] = {}
    short_ret_cols: dict[int, str] = {}
    long_close_cols: dict[int, str] = {}
    short_close_cols: dict[int, str] = {}
    long_mfe_cols: dict[int, str] = {}
    long_mae_cols: dict[int, str] = {}
    short_mfe_cols: dict[int, str] = {}
    short_mae_cols: dict[int, str] = {}
    for horizon in HORIZONS:
        entry_next = daily["entry_next_open"]
        exit_close = grouped["c"].shift(-(horizon + 1))
        future_high = pd.concat([grouped["h"].shift(-step) for step in range(1, horizon + 1)], axis=1).max(axis=1)
        future_low = pd.concat([grouped["l"].shift(-step) for step in range(1, horizon + 1)], axis=1).min(axis=1)
        long_ret_cols[horizon] = f"ret_long_{horizon}d"
        short_ret_cols[horizon] = f"ret_short_{horizon}d"
        long_close_cols[horizon] = f"ret_long_close_{horizon}d"
        short_close_cols[horizon] = f"ret_short_close_{horizon}d"
        long_mfe_cols[horizon] = f"mfe_long_{horizon}d"
        long_mae_cols[horizon] = f"mae_long_{horizon}d"
        short_mfe_cols[horizon] = f"mfe_short_{horizon}d"
        short_mae_cols[horizon] = f"mae_short_{horizon}d"
        daily[long_ret_cols[horizon]] = (exit_close / entry_next) - 1.0 - ROUND_TRIP_COST
        daily[short_ret_cols[horizon]] = (entry_next / exit_close) - 1.0 - ROUND_TRIP_COST
        daily[long_close_cols[horizon]] = daily[f"ret_long_close_{horizon}d"]
        daily[short_close_cols[horizon]] = daily[f"ret_short_close_{horizon}d"]
        daily[long_mfe_cols[horizon]] = (future_high / entry_next) - 1.0
        daily[long_mae_cols[horizon]] = 1.0 - (future_low / entry_next)
        daily[short_mfe_cols[horizon]] = (entry_next / future_low) - 1.0
        daily[short_mae_cols[horizon]] = (future_high / entry_next) - 1.0

    def _choose(row: pd.Series, horizon: int, close_exec: bool = False) -> float:
        label = str(row["signal_label"])
        mode = _signal_return_mode(label)
        if mode == "na":
            return float("nan")
        col = (
            long_close_cols[horizon]
            if close_exec and mode == "long"
            else short_close_cols[horizon]
            if close_exec and mode == "short"
            else long_ret_cols[horizon]
            if mode == "long"
            else short_ret_cols[horizon]
        )
        return float(row[col]) if pd.notna(row[col]) else float("nan")

    def _choose_mfe(row: pd.Series, horizon: int) -> float:
        label = str(row["signal_label"])
        mode = _signal_return_mode(label)
        if mode == "long":
            return float(row[long_mfe_cols[horizon]]) if pd.notna(row[long_mfe_cols[horizon]]) else float("nan")
        if mode == "short":
            return float(row[short_mfe_cols[horizon]]) if pd.notna(row[short_mfe_cols[horizon]]) else float("nan")
        return float("nan")

    def _choose_mae(row: pd.Series, horizon: int) -> float:
        label = str(row["signal_label"])
        mode = _signal_return_mode(label)
        if mode == "long":
            return float(row[long_mae_cols[horizon]]) if pd.notna(row[long_mae_cols[horizon]]) else float("nan")
        if mode == "short":
            return float(row[short_mae_cols[horizon]]) if pd.notna(row[short_mae_cols[horizon]]) else float("nan")
        return float("nan")

    for horizon in HORIZONS:
        daily[f"signal_ret_{horizon}d"] = daily.apply(lambda row, h=horizon: _choose(row, h, close_exec=False), axis=1)
        daily[f"signal_ret_close_{horizon}d"] = daily.apply(lambda row, h=horizon: _choose(row, h, close_exec=True), axis=1)
        daily[f"signal_mfe_{horizon}d"] = daily.apply(lambda row, h=horizon: _choose_mfe(row, h), axis=1)
        daily[f"signal_mae_{horizon}d"] = daily.apply(lambda row, h=horizon: _choose_mae(row, h), axis=1)
    return daily


def _positive_period_ratio(frame: pd.DataFrame, value_col: str) -> float:
    grouped = frame.groupby("period_bucket", sort=False)[value_col].mean()
    grouped = pd.to_numeric(grouped, errors="coerce").dropna()
    if grouped.empty:
        return 0.0
    return float((grouped > 0.0).mean())


def _period_means(frame: pd.DataFrame, value_col: str) -> list[dict[str, Any]]:
    grouped = frame.groupby("period_bucket", sort=False)[value_col].mean().reset_index()
    rows: list[dict[str, Any]] = []
    for _, row in grouped.iterrows():
        rows.append({"period_bucket": str(row["period_bucket"]), "mean": float(row[value_col]) if pd.notna(row[value_col]) else None})
    return rows


def _meets_gate(ret20: pd.Series, subset: pd.DataFrame) -> dict[str, bool]:
    metrics = _summary_from_series(ret20)
    n = int(metrics["n"])
    months = int(subset["period_bucket"].nunique()) if "period_bucket" in subset.columns else 0
    top_code_share = float(subset["code"].value_counts(normalize=True).iloc[0]) if not subset.empty else 1.0
    return {
        "oos_positive": bool((metrics["mean"] or 0.0) > 0.0),
        "pf_ge_1_2": bool((metrics["profit_factor"] or 0.0) >= 1.2),
        "mdd_le_0_25": bool((metrics["mdd"] or 1.0) <= 0.25),
        "positive_period_ratio_ge_0_55": bool(_positive_period_ratio(subset, "signal_ret_20d") >= 0.55),
        "top_code_share_le_0_25": bool(top_code_share <= 0.25),
        "min_samples_ge_80": bool(n >= 80),
        "months_ge_6": bool(months >= 6),
    }


def _summary_from_signal(frame: pd.DataFrame, label: str, min_samples: int) -> dict[str, Any] | None:
    subset = frame.loc[frame["signal_label"].eq(label)].copy()
    if subset.empty:
        return None
    ret20 = pd.to_numeric(subset["signal_ret_20d"], errors="coerce").dropna()
    if ret20.empty:
        return None
    stats20 = _summary_from_series(ret20)
    stats5 = _summary_from_series(subset["signal_ret_5d"])
    stats10 = _summary_from_series(subset["signal_ret_10d"])
    close20 = _summary_from_series(subset["signal_ret_close_20d"])
    return {
        "signal_label": label,
        "n": int(stats20["n"]),
        "mean_5d": stats5["mean"],
        "mean_10d": stats10["mean"],
        "mean_20d": stats20["mean"],
        "median_20d": stats20["median"],
        "win_rate_20d": stats20["win_rate"],
        "pf_20d": stats20["profit_factor"],
        "mdd_20d": stats20["mdd"],
        "mfe_20d": float(pd.to_numeric(subset["signal_mfe_20d"], errors="coerce").dropna().mean()) if subset["signal_mfe_20d"].notna().any() else None,
        "mae_20d": float(pd.to_numeric(subset["signal_mae_20d"], errors="coerce").dropna().mean()) if subset["signal_mae_20d"].notna().any() else None,
        "close_mean_20d": close20["mean"],
        "close_pf_20d": close20["profit_factor"],
        "top_code_share": float(subset["code"].value_counts(normalize=True).iloc[0]),
        "unique_codes": int(subset["code"].nunique()),
        "positive_period_ratio": _positive_period_ratio(subset, "signal_ret_20d"),
        "period_means": _period_means(subset, "signal_ret_20d"),
        "meets_gate": _meets_gate(ret20, subset),
        "sample_warning": int(stats20["n"]) < min_samples,
    }


def _support_strength_summary(frame: pd.DataFrame, min_samples: int, mode: str) -> list[dict[str, Any]]:
    suffix = f"_{mode}"
    support_cols = [col for col in frame.columns if col == f"support_hold{suffix}" or col == f"reclaim_support{suffix}"]
    if not support_cols:
        return []
    support_flag = frame[support_cols].any(axis=1)
    work = frame.loc[support_flag].copy()
    if work.empty:
        return []
    touch_col = f"support_touch_count{suffix}"
    age_col = f"support_touch_age{suffix}"
    work["support_touch_bucket"] = work[touch_col].map(_bucket_touch_count)
    work["support_age_bucket"] = work[age_col].map(_bucket_age)
    grouped = (
        work.groupby(["ma60_state", "ma_relation", "support_touch_bucket", "support_age_bucket"], dropna=False)["signal_ret_20d"]
        .agg(["mean", "count"])
        .reset_index()
    )
    grouped = grouped[grouped["count"] >= min_samples]
    grouped = grouped.sort_values(["mean", "count"], ascending=[False, False]).head(12)
    rows: list[dict[str, Any]] = []
    for _, row in grouped.iterrows():
        rows.append(
            {
                "ma60_state": str(row["ma60_state"]),
                "ma_relation": str(row["ma_relation"]),
                "support_touch_bucket": str(row["support_touch_bucket"]),
                "support_age_bucket": str(row["support_age_bucket"]),
                "n": int(row["count"]),
                "mean20": float(row["mean"]),
            }
        )
    return rows


def _resistance_strength_summary(frame: pd.DataFrame, min_samples: int, mode: str) -> list[dict[str, Any]]:
    suffix = f"_{mode}"
    resist_cols = [col for col in frame.columns if col == f"reject_resistance{suffix}" or col == f"breakout_resistance{suffix}"]
    if not resist_cols:
        return []
    resist_flag = frame[resist_cols].any(axis=1)
    work = frame.loc[resist_flag].copy()
    if work.empty:
        return []
    touch_col = f"resistance_touch_count{suffix}"
    age_col = f"resistance_touch_age{suffix}"
    work["resist_touch_bucket"] = work[touch_col].map(_bucket_touch_count)
    work["resist_age_bucket"] = work[age_col].map(_bucket_age)
    grouped = (
        work.groupby(["ma60_state", "ma_relation", "resist_touch_bucket", "resist_age_bucket"], dropna=False)["signal_ret_20d"]
        .agg(["mean", "count"])
        .reset_index()
    )
    grouped = grouped[grouped["count"] >= min_samples]
    grouped = grouped.sort_values(["mean", "count"], ascending=[False, False]).head(12)
    rows: list[dict[str, Any]] = []
    for _, row in grouped.iterrows():
        rows.append(
            {
                "ma60_state": str(row["ma60_state"]),
                "ma_relation": str(row["ma_relation"]),
                "resist_touch_bucket": str(row["resist_touch_bucket"]),
                "resist_age_bucket": str(row["resist_age_bucket"]),
                "n": int(row["count"]),
                "mean20": float(row["mean"]),
            }
        )
    return rows


def _combo_summary(frame: pd.DataFrame, min_samples: int) -> list[dict[str, Any]]:
    grouped = frame.groupby("state_combo", dropna=False)["signal_ret_20d"].agg(["mean", "count"]).reset_index()
    grouped = grouped[grouped["count"] >= min_samples]
    grouped = grouped.sort_values(["mean", "count"], ascending=[False, False]).head(15)
    rows: list[dict[str, Any]] = []
    for _, row in grouped.iterrows():
        rows.append({"state_combo": str(row["state_combo"]), "n": int(row["count"]), "mean20": float(row["mean"])})
    return rows


def _pattern_summary(frame: pd.DataFrame, min_samples: int) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for state in ("long_entry", "long_hold", "long_takeprofit", "long_exit", "short_entry", "short_hold", "short_takeprofit", "short_exit"):
        subset = frame.loc[frame["signal_label"].eq(state)].copy()
        if subset.empty:
            out[state] = []
            continue
        grouped = (
            subset.groupby(["pattern_2_family", "pattern_2"], dropna=False)["signal_ret_20d"].agg(["mean", "count"]).reset_index()
        )
        grouped = grouped[grouped["count"] >= min_samples]
        grouped = grouped.sort_values(["mean", "count"], ascending=[False, False]).head(10)
        rows: list[dict[str, Any]] = []
        for _, row in grouped.iterrows():
            rows.append(
                {
                    "pattern_2_family": str(row["pattern_2_family"]),
                    "pattern_2": str(row["pattern_2"]),
                    "n": int(row["count"]),
                    "mean20": float(row["mean"]),
                }
            )
        out[state] = rows
    return out


def _transition_summary(frame: pd.DataFrame, min_samples: int) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for prefix in ("long", "short"):
        label_col = f"{prefix}_state_label"
        next_col = f"{prefix}_next_state_label"
        work = frame.copy()
        work[next_col] = work.groupby("code", sort=False)[label_col].shift(-1)
        work = work.loc[work[label_col].ne("na") & work[next_col].ne("na")].copy()
        if work.empty:
            out[prefix] = []
            continue
        from_counts = work.groupby(label_col, dropna=False)["signal_ret_20d"].count().to_dict()
        from_means = {
            str(label): _summary_from_series(pd.to_numeric(series, errors="coerce").dropna())
            for label, series in work.groupby(label_col, dropna=False)["signal_ret_20d"]
        }
        grouped = work.groupby([label_col, next_col], dropna=False)["signal_ret_20d"].agg(["mean", "count"]).reset_index()
        grouped = grouped[grouped["count"] >= min_samples]
        grouped = grouped.sort_values(["count", "mean"], ascending=[False, False]).head(15)
        rows: list[dict[str, Any]] = []
        for _, row in grouped.iterrows():
            from_label = str(row[label_col])
            base_stats = from_means.get(from_label, {})
            base_n = int(from_counts.get(from_label, 0) or 0)
            conditional_mean = float(row["mean"])
            base_mean = float(base_stats.get("mean")) if base_stats else None
            rows.append({"from": str(row[label_col]), "to": str(row[next_col]), "n": int(row["count"]), "mean20": float(row["mean"])})
            rows[-1]["share_from"] = float(row["count"]) / float(base_n) if base_n else None
            rows[-1]["base_n"] = base_n
            rows[-1]["base_mean20"] = base_mean
            rows[-1]["delta_mean20"] = conditional_mean - base_mean if base_mean is not None else None
        out[prefix] = rows
    return out


def _transition_focus_summary(frame: pd.DataFrame, min_samples: int) -> list[dict[str, Any]]:
    work = frame.copy()
    work["long_next_state_label"] = work.groupby("code", sort=False)["long_state_label"].shift(-1)
    work = work.loc[work["long_state_label"].ne("na") & work["long_next_state_label"].ne("na")].copy()
    if work.empty:
        return []
    focus_pairs = [
        ("long_entry_watch", "long_hold"),
        ("long_hold", "long_takeprofit"),
        ("long_hold", "long_exit"),
        ("long_takeprofit", "long_exit"),
    ]
    from_counts = work.groupby("long_state_label", dropna=False)["signal_ret_20d"].count().to_dict()
    from_means = {
        str(label): _summary_from_series(pd.to_numeric(series, errors="coerce").dropna())
        for label, series in work.groupby("long_state_label", dropna=False)["signal_ret_20d"]
    }
    rows: list[dict[str, Any]] = []
    for from_label, to_label in focus_pairs:
        subset = work.loc[(work["long_state_label"].eq(from_label)) & (work["long_next_state_label"].eq(to_label))].copy()
        if subset.empty:
            continue
        ret20 = pd.to_numeric(subset["signal_ret_20d"], errors="coerce").dropna()
        if ret20.empty:
            continue
        stats20 = _summary_from_series(ret20)
        close20 = _summary_from_series(subset["signal_ret_close_20d"])
        base_stats = from_means.get(from_label, {})
        base_n = int(from_counts.get(from_label, 0) or 0)
        base_mean = float(base_stats.get("mean")) if base_stats else None
        rows.append(
            {
                "from": from_label,
                "to": to_label,
                "n": int(stats20["n"]),
                "share_from": float(stats20["n"]) / float(base_n) if base_n else None,
                "mean20": stats20["mean"],
                "close_mean20": close20["mean"],
                "pf20": stats20["profit_factor"],
                "close_pf20": close20["profit_factor"],
                "win20": stats20["win_rate"],
                "mdd20": stats20["mdd"],
                "mae20": float(pd.to_numeric(subset["signal_mae_20d"], errors="coerce").dropna().mean()) if subset["signal_mae_20d"].notna().any() else None,
                "mfe20": float(pd.to_numeric(subset["signal_mfe_20d"], errors="coerce").dropna().mean()) if subset["signal_mfe_20d"].notna().any() else None,
                "base_n": base_n,
                "base_mean20": base_mean,
                "delta_mean20": stats20["mean"] - base_mean if base_mean is not None else None,
                "sample_warning": int(stats20["n"]) < min_samples,
            }
        )
    return rows


def _fold_metrics(frame: pd.DataFrame, min_samples: int) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for label in [
        "long_entry_watch",
        "long_entry",
        "long_hold",
        "long_takeprofit",
        "long_exit",
        "short_entry_watch",
        "short_entry",
        "short_hold",
        "short_takeprofit",
        "short_exit",
    ]:
        subset = frame.loc[frame["signal_label"].eq(label)]
        if subset.empty:
            continue
        ret20 = pd.to_numeric(subset["signal_ret_20d"], errors="coerce").dropna()
        if ret20.empty:
            continue
        metrics = _summary_from_series(ret20)
        out[label] = {
            "n": metrics["n"],
            "mean20": metrics["mean"],
            "pf20": metrics["profit_factor"],
            "win_rate20": metrics["win_rate"],
            "mdd20": metrics["mdd"],
            "top_code_share": float(subset["code"].value_counts(normalize=True).iloc[0]),
            "positive_period_ratio": _positive_period_ratio(subset, "signal_ret_20d"),
            "meets_gate": _meets_gate(ret20, subset),
        }
        if metrics["n"] < min_samples:
            out[label]["sample_warning"] = True
    return out


def _walkforward_summary(frame: pd.DataFrame, min_samples: int) -> list[dict[str, Any]]:
    config = ResearchConfig()
    windows = build_walkforward_windows(frame, config)
    fold_rows: list[dict[str, Any]] = []
    for idx, window in enumerate(windows, start=1):
        train_mask = frame["month_bucket"].isin(window["train_months"])
        valid_mask = frame["month_bucket"].isin(window["valid_months"])
        test_mask = frame["month_bucket"].isin(window["test_months"])
        fold_rows.append(
            {
                "fold": idx,
                "train_months": list(window["train_months"]),
                "valid_months": list(window["valid_months"]),
                "test_months": list(window["test_months"]),
                "train": _fold_metrics(frame.loc[train_mask], min_samples=min_samples),
                "valid": _fold_metrics(frame.loc[valid_mask], min_samples=min_samples),
                "test": _fold_metrics(frame.loc[test_mask], min_samples=min_samples),
            }
        )
    return fold_rows


def _oos_combined_summary(frame: pd.DataFrame) -> dict[str, Any]:
    config = ResearchConfig()
    windows = build_walkforward_windows(frame, config)
    if not windows:
        return {"available": False}
    test_months: set[str] = set()
    for window in windows:
        test_months.update(window["test_months"])
    oos = frame.loc[frame["month_bucket"].isin(sorted(test_months))].copy()
    rows: dict[str, Any] = {"available": True, "n": int(len(oos)), "months": int(oos["month_bucket"].nunique())}
    for label in [
        "long_entry_watch",
        "long_entry",
        "long_hold",
        "long_takeprofit",
        "long_exit",
        "short_entry_watch",
        "short_entry",
        "short_hold",
        "short_takeprofit",
        "short_exit",
    ]:
        subset = oos.loc[oos["signal_label"].eq(label)]
        if subset.empty:
            continue
        ret20 = pd.to_numeric(subset["signal_ret_20d"], errors="coerce").dropna()
        if ret20.empty:
            continue
        stats20 = _summary_from_series(ret20)
        rows[label] = {
            "n": int(stats20["n"]),
            "mean20": stats20["mean"],
            "pf20": stats20["profit_factor"],
            "mdd20": stats20["mdd"],
            "win_rate20": stats20["win_rate"],
            "mean5": float(pd.to_numeric(subset["signal_ret_5d"], errors="coerce").dropna().mean()) if subset["signal_ret_5d"].notna().any() else None,
            "mean10": float(pd.to_numeric(subset["signal_ret_10d"], errors="coerce").dropna().mean()) if subset["signal_ret_10d"].notna().any() else None,
            "mean_close20": float(pd.to_numeric(subset["signal_ret_close_20d"], errors="coerce").dropna().mean()) if subset["signal_ret_close_20d"].notna().any() else None,
            "mae20": float(pd.to_numeric(subset["signal_mae_20d"], errors="coerce").dropna().mean()) if subset["signal_mae_20d"].notna().any() else None,
            "mfe20": float(pd.to_numeric(subset["signal_mfe_20d"], errors="coerce").dropna().mean()) if subset["signal_mfe_20d"].notna().any() else None,
            "top_code_share": float(subset["code"].value_counts(normalize=True).iloc[0]),
            "positive_period_ratio": _positive_period_ratio(subset, "signal_ret_20d"),
            "meets_gate": _meets_gate(ret20, subset),
        }
    return rows


def _summarize_mode(mode_frame: pd.DataFrame, min_samples: int, mode: str) -> dict[str, Any]:
    long_frame = _add_signal_columns(mode_frame.assign(signal_label=mode_frame["long_state_label"]))
    short_frame = _add_signal_columns(mode_frame.assign(signal_label=mode_frame["short_state_label"]))
    combined = pd.concat([long_frame, short_frame], ignore_index=True)
    combined = combined.loc[combined["signal_label"].ne("na")].copy()
    if not combined.empty:
        combined = combined.sort_values(["code", "dt", "signal_label"]).reset_index(drop=True)
    state_rows: list[dict[str, Any]] = []
    for label in [
        "long_entry_watch",
        "long_entry",
        "long_hold",
        "long_takeprofit",
        "long_exit",
        "short_entry_watch",
        "short_entry",
        "short_hold",
        "short_takeprofit",
        "short_exit",
    ]:
        summary = _summary_from_signal(combined, label, min_samples=min_samples)
        if summary is not None:
            state_rows.append(summary)
    state_rows.sort(key=lambda row: (row["signal_label"].startswith("long_"), row["n"], row["mean_20d"] or -999.0), reverse=True)
    return {
        "states": state_rows,
        "support_summary": _support_strength_summary(combined, min_samples=min_samples, mode=mode),
        "resistance_summary": _resistance_strength_summary(combined, min_samples=min_samples, mode=mode),
        "combo_summary": _combo_summary(combined, min_samples=min_samples),
        "pattern_summary": _pattern_summary(combined, min_samples=min_samples),
        "transition_focus_summary": _transition_focus_summary(combined, min_samples=min_samples),
        "transition_summary": _transition_summary(combined, min_samples=min_samples),
        "walkforward": _walkforward_summary(combined, min_samples=min_samples),
        "oos_combined": _oos_combined_summary(combined),
    }


def _compare_modes(results: dict[str, dict[str, Any]]) -> dict[str, Any]:
    comparison: dict[str, Any] = {}
    key_labels = ["long_entry", "long_hold", "long_takeprofit", "long_exit", "short_entry", "short_takeprofit", "short_exit"]
    for label in key_labels:
        comparison[label] = {}
        for mode, payload in results.items():
            states = {row["signal_label"]: row for row in payload["states"]}
            if label in states:
                comparison[label][mode] = {
                    "mean20": states[label]["mean_20d"],
                    "pf20": states[label]["pf_20d"],
                    "win_rate20": states[label]["win_rate_20d"],
                    "mdd20": states[label]["mdd_20d"],
                    "n": states[label]["n"],
                    "meets_gate": states[label]["meets_gate"],
                }
    return comparison


def _state_snapshot(result: dict[str, Any], mode: str, label: str) -> dict[str, Any]:
    mode_payload = result.get("modes", {}).get(mode, {})
    oos = mode_payload.get("oos_combined", {})
    snapshot = oos.get(label)
    return snapshot if isinstance(snapshot, dict) else {}


def _state_has_edge(snapshot: dict[str, Any], *, min_pf: float = 1.2) -> bool:
    mean20 = snapshot.get("mean20")
    pf20 = snapshot.get("pf20")
    return bool((mean20 or 0.0) > 0.0 and (pf20 or 0.0) >= min_pf)


def _build_research_plan(result: dict[str, Any]) -> dict[str, Any]:
    modes = {name: payload for name, payload in result.get("modes", {}).items() if isinstance(payload, dict)}
    current = {
        mode: {
            "entry_watch": _state_snapshot(result, mode, "long_entry_watch"),
            "hold": _state_snapshot(result, mode, "long_hold"),
            "takeprofit": _state_snapshot(result, mode, "long_takeprofit"),
            "exit": _state_snapshot(result, mode, "long_exit"),
        }
        for mode in sorted(modes.keys())
    }

    long_edge_stable = bool(
        current
        and all(_state_has_edge(payload["entry_watch"]) for payload in current.values())
        and all(_state_has_edge(payload["hold"]) for payload in current.values())
    )
    takeprofit_exit_negative = bool(
        current
        and all((payload["takeprofit"].get("mean20") or 0.0) <= 0.0 for payload in current.values())
        and all((payload["exit"].get("mean20") or 0.0) <= 0.0 for payload in current.values())
    )

    extrema = current.get("extrema20", {})
    swing = current.get("swing10", {})
    band_delta = {}
    for label in ("entry_watch", "hold", "takeprofit", "exit"):
        ext = extrema.get(label, {})
        sw = swing.get(label, {})
        if ext and sw:
            band_delta[label] = {
                "mean20_delta": float((ext.get("mean20") or 0.0) - (sw.get("mean20") or 0.0)),
                "pf20_delta": float((ext.get("pf20") or 0.0) - (sw.get("pf20") or 0.0)),
            }

    next_tasks = [
        {
            "task_key": "long_state_transition",
            "title": "Long の state 遷移を利益検証に落とす",
            "focus": ["entry_watch", "hold", "takeprofit", "exit"],
            "why": "entry_watch と hold が OOS で正で、takeprofit / exit は劣勢なので、最初に詰めるべきは遷移条件。",
            "acceptance": [
                "next-open 約定でも close 約定でも OOS expectancy が正",
                "PF が 1.2 以上",
                "月次安定性が崩れない",
                "entry_watch -> hold / hold -> takeprofit / hold -> exit の分岐が説明できる",
            ],
        },
        {
            "task_key": "band_sensitivity",
            "title": "帯定義の感度を extremas20 と swing10 で比較する",
            "focus": ["extrema20", "swing10"],
            "why": "帯の取り方で結論が反転しないかを確認し、主系を決める必要がある。",
            "acceptance": [
                "long の主要 state の符号が両 band で一致する",
                "entry_watch / hold の順位が大きく逆転しない",
                "帯定義を変えても OOS の優位が維持される",
            ],
        },
        {
            "task_key": "short_defer",
            "title": "Short は reject_resistance だけ先に評価し、late short は後回しにする",
            "focus": ["reject_resistance", "lose_support"],
            "why": "lose_support は late short の罠になりやすく、まず long の state machine を固める方が安全。",
            "acceptance": [
                "short は long の検証を壊さない独立 family として扱う",
                "reject_resistance 以外の short は後段へ送る",
            ],
        },
    ]

    return {
        "current_read": {
            "long_first": True,
            "entry_state_policy": "entry_watch を実運用上の entry 候補として扱う",
            "long_edge_stable": long_edge_stable,
            "takeprofit_exit_negative": takeprofit_exit_negative,
            "modes": current,
            "band_delta": band_delta,
        },
        "next_tasks": next_tasks,
        "final_destination": {
            "phase_1": "analysis-only の state engine を固定する",
            "phase_2": "walk-forward で profitability proof を通す",
            "phase_3": "その後に bounded prior として candidate selection に小さく反映する",
            "ranking": "ranking bonus は最終段であり、現時点では進めない",
        },
    }


def _mode_summary_text(summary: dict[str, Any] | None) -> str:
    if not summary:
        return "-"
    return (
        f"n={_fmt(summary.get('n'), 0)} "
        f"mean20={_fmt(summary.get('mean20'))} "
        f"pf20={_fmt(summary.get('pf20'))} "
        f"mdd20={_fmt(summary.get('mdd20'))} "
        f"win20={_fmt(summary.get('win_rate20'))}"
    )


def _render_table(rows: list[dict[str, Any]], headers: list[str], keys: list[str]) -> list[str]:
    if not rows:
        return ["-"]
    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for row in rows:
        cells = []
        for key in keys:
            value = row.get(key)
            if isinstance(value, dict):
                cells.append(json.dumps(value, ensure_ascii=False))
            else:
                cells.append(_fmt(value))
        lines.append("| " + " | ".join(cells) + " |")
    return lines


def _render_markdown(result: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.extend(
        [
            "# Zone State Profit Study",
            "",
            "終値シグナルを前提に、next open 約定を default とした state study。",
            "",
            "## Meta",
            "",
        ]
    )
    meta = result["meta"]
    for key in ["db_paths", "date_min", "date_max", "round_trip_cost", "min_samples", "walkforward_windows", "rows_base"]:
        value = meta.get(key)
        if key == "db_paths":
            lines.append(f"- `{key}`: {', '.join(map(str, value or []))}")
        else:
            lines.append(f"- `{key}`: {_fmt(value)}")
    lines.append("")
    lines.append("## Mode Comparison")
    lines.append("")
    comp_rows = []
    for label, modes in result["comparison"].items():
        row = {
            "label": label,
            "extrema20": _mode_summary_text(modes.get("extrema20")),
            "swing10": _mode_summary_text(modes.get("swing10")),
        }
        comp_rows.append(row)
    lines.extend(_render_table(comp_rows, ["label", "extrema20", "swing10"], ["label", "extrema20", "swing10"]))
    lines.append("")
    plan = result.get("research_plan") or {}
    if plan:
        current = plan.get("current_read", {})
        lines.extend(["## Research Plan", ""])
        lines.append("### Current Read")
        lines.append("")
        lines.append(f"- `long_first`: {current.get('long_first')}")
        lines.append(f"- `entry_state_policy`: {current.get('entry_state_policy')}")
        lines.append(f"- `long_edge_stable`: {current.get('long_edge_stable')}")
        lines.append(f"- `takeprofit_exit_negative`: {current.get('takeprofit_exit_negative')}")
        lines.append("")
        lines.append("### Next Tasks")
        lines.append("")
        for idx, task in enumerate(plan.get("next_tasks", []), start=1):
            lines.append(f"{idx}. `{task.get('title')}`")
            lines.append(f"   - `task_key`: `{task.get('task_key')}`")
            lines.append(f"   - `focus`: {', '.join(f'`{item}`' for item in task.get('focus', []))}")
            lines.append(f"   - `why`: {task.get('why')}")
            acceptance = task.get("acceptance") or []
            if acceptance:
                lines.append("   - `acceptance`:")
                for item in acceptance:
                    lines.append(f"     - {item}")
        lines.append("")
        lines.append("### Final Destination")
        lines.append("")
        destination = plan.get("final_destination", {})
        for key, value in destination.items():
            lines.append(f"- `{key}`: {value}")
        lines.append("")
    for mode, payload in result["modes"].items():
        lines.extend([f"## Band Mode: `{mode}`", ""])
        oos = payload["oos_combined"]
        lines.extend(
            [
                "### OOS Combined",
                "",
                f"- `available`: {oos.get('available')}",
                f"- `n`: {_fmt(oos.get('n'), 0)}",
                f"- `months`: {_fmt(oos.get('months'), 0)}",
                "",
            ]
        )
        lines.extend(["### Key States", ""])
        lines.extend(
            _render_table(
                payload["states"],
                ["state", "n", "mean20", "pf20", "win20", "mdd20", "mae20", "mfe20", "top_code_share"],
                ["signal_label", "n", "mean_20d", "pf_20d", "win_rate_20d", "mdd_20d", "mae_20d", "mfe_20d", "top_code_share"],
            )
        )
        lines.append("")
        lines.extend(["### Support Strength", ""])
        lines.extend(
            _render_table(
                payload["support_summary"],
                ["ma60_state", "ma_relation", "support_touch_bucket", "support_age_bucket", "n", "mean20"],
                ["ma60_state", "ma_relation", "support_touch_bucket", "support_age_bucket", "n", "mean20"],
            )
        )
        lines.append("")
        lines.extend(["### Resistance Strength", ""])
        lines.extend(
            _render_table(
                payload["resistance_summary"],
                ["ma60_state", "ma_relation", "resist_touch_bucket", "resist_age_bucket", "n", "mean20"],
                ["ma60_state", "ma_relation", "resist_touch_bucket", "resist_age_bucket", "n", "mean20"],
            )
        )
        lines.append("")
        lines.extend(["### Top Combos", ""])
        lines.extend(_render_table(payload["combo_summary"], ["state_combo", "n", "mean20"], ["state_combo", "n", "mean20"]))
        lines.append("")
        lines.extend(["### Pattern Summary", ""])
        for state, rows in payload["pattern_summary"].items():
            lines.append(f"- `{state}`")
            lines.extend(_render_table(rows, ["pattern_2_family", "pattern_2", "n", "mean20"], ["pattern_2_family", "pattern_2", "n", "mean20"]))
        lines.append("")
        lines.extend(["### Transition Focus", ""])
        lines.extend(
            _render_table(
                payload.get("transition_focus_summary", []),
                ["from", "to", "n", "share_from", "mean20", "close_mean20", "pf20", "win20", "mdd20", "delta_mean20"],
                ["from", "to", "n", "share_from", "mean20", "close_mean20", "pf20", "win20", "mdd20", "delta_mean20"],
            )
        )
        lines.append("")
        lines.extend(["### Transition Summary", ""])
        for side, rows in payload["transition_summary"].items():
            lines.append(f"- `{side}`")
            lines.extend(_render_table(rows, ["from", "to", "n", "mean20"], ["from", "to", "n", "mean20"]))
        lines.append("")
        lines.extend(["### Walk-forward", ""])
        for fold in payload["walkforward"]:
            lines.append(
                f"- fold `{fold['fold']}`: train `{len(fold['train_months'])}` months / valid `{len(fold['valid_months'])}` months / test `{len(fold['test_months'])}` months"
            )
            for split in ("train", "valid", "test"):
                lines.append(f"  - `{split}`")
                summary = fold[split]
                for label in ("long_entry", "long_hold", "long_takeprofit", "long_exit", "short_entry", "short_hold", "short_takeprofit", "short_exit"):
                    if label in summary:
                        item = summary[label]
                        lines.append(
                            f"    - `{label}` n={_fmt(item['n'], 0)} mean20={_fmt(item['mean20'])} pf20={_fmt(item['pf20'])} win20={_fmt(item['win_rate20'])} mdd20={_fmt(item['mdd20'])}"
                        )
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def run_zone_state_profit_study(db_paths: list[Path], min_samples: int) -> dict[str, Any]:
    base = _load_base_frame(db_paths)
    results: dict[str, dict[str, Any]] = {}
    for mode in BAND_MODES:
        mode_frame = _compute_band_features(base, mode)
        mode_frame = _state_label_series(mode_frame, mode)
        results[mode] = _summarize_mode(mode_frame, min_samples=min_samples, mode=mode)
    comparison = _compare_modes(results)
    research_plan = _build_research_plan({"modes": results})
    windows = build_walkforward_windows(base.assign(month_bucket=base["month_bucket"]), ResearchConfig())
    return {
        "meta": {
            "db_paths": [str(path) for path in db_paths],
            "date_min": str(base["dt"].min().date()) if not base.empty else None,
            "date_max": str(base["dt"].max().date()) if not base.empty else None,
            "round_trip_cost": ROUND_TRIP_COST,
            "min_samples": int(min_samples),
            "walkforward_windows": int(len(windows)),
            "band_modes": list(BAND_MODES.keys()),
            "rows_base": int(len(base)),
        },
        "modes": results,
        "comparison": comparison,
        "research_plan": research_plan,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Zone reaction x MA x candle state profitability study")
    parser.add_argument("--db-path", action="append", type=Path, default=None, help="DuckDB path. Can be specified multiple times.")
    parser.add_argument("--min-samples", type=int, default=80, help="Minimum samples for combo tables and gate hints.")
    parser.add_argument("--output-json", type=Path, default=Path("tmp/zone_state_profit_study.json"))
    parser.add_argument("--output-md", type=Path, default=Path("tmp/zone_state_profit_study.md"))
    args = parser.parse_args()

    db_paths = _resolve_db_paths(args.db_path)
    result = run_zone_state_profit_study(db_paths, min_samples=max(1, int(args.min_samples)))
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_md.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    args.output_md.write_text(_render_markdown(result), encoding="utf-8")
    print(json.dumps(result["meta"], ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
