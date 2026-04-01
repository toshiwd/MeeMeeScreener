from __future__ import annotations

import argparse
import json
import math
import os
from pathlib import Path
import sys
from typing import Any

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.core.config import config
from scripts.monthly_box_breakout_research import (
    _bucket_box_month_index,
    _bucket_dist_ma20,
    _prepare_frame as _prepare_monthly_box_frame,
)
from scripts.monthly_box_time_window_study import _add_time_window_features
from scripts.note_trade_repro_backtest import ROUND_TRIP_COST, _resolve_default_db_paths


PATH_HORIZONS = (5, 10, 20)
DIRS = ("up", "down")
TIMEFRAMES = ("D", "W", "M")
TF_LABELS = {"D": "日足", "W": "週足", "M": "月足"}
DIR_LABELS = {"up": "買い", "down": "売り"}
REPLAY_CODES = ("6301", "4661", "6976", "8136")


def _summary_from_returns(values: pd.Series) -> dict[str, Any]:
    arr = values.dropna().to_numpy(dtype=np.float64, copy=False)
    if arr.size == 0:
        return {
            "n": 0,
            "mean": None,
            "median": None,
            "win_rate": None,
            "profit_factor": None,
            "p10": None,
        }
    gains = arr[arr > 0.0].sum()
    losses = -arr[arr < 0.0].sum()
    profit_factor = None
    if losses > 0.0:
        profit_factor = float(gains / losses)
    elif gains > 0.0:
        profit_factor = float("inf")
    return {
        "n": int(arr.size),
        "mean": float(arr.mean()),
        "median": float(np.median(arr)),
        "win_rate": float(np.mean(arr > 0.0)),
        "profit_factor": profit_factor,
        "p10": float(np.quantile(arr, 0.10)),
    }


def _resolve_db_paths_from_args(values: list[str]) -> list[Path]:
    if values:
        return [Path(value).expanduser().resolve() for value in values]

    candidates: list[Path] = []
    local_appdata = os.getenv("LOCALAPPDATA")
    if local_appdata:
        candidates.append(Path(local_appdata) / "MeeMeeScreener" / "data" / "stocks.duckdb")

    env_db = os.getenv("STOCKS_DB_PATH")
    if env_db:
        candidates.append(Path(env_db).expanduser().resolve())

    env_data_dir = os.getenv("MEEMEE_DATA_DIR")
    if env_data_dir:
        candidates.append((Path(env_data_dir).expanduser().resolve() / "stocks.duckdb"))

    candidates.append(Path(config.DB_PATH).expanduser().resolve())

    for candidate in candidates:
        if candidate.exists():
            return [candidate]
    return _resolve_default_db_paths()


def _add_short_forward_path_metrics(daily: pd.DataFrame) -> pd.DataFrame:
    daily = daily.copy()
    daily["entry_next_open"] = daily.groupby("code", sort=False)["o"].shift(-1)
    grouped = daily.groupby("code", sort=False)
    for horizon in PATH_HORIZONS:
        exit_close = grouped["c"].shift(-(horizon + 1))
        daily[f"ret_short_{horizon}d"] = (daily["entry_next_open"] / exit_close) - 1.0 - ROUND_TRIP_COST
        high_shifts = [grouped["h"].shift(-step) for step in range(1, horizon + 1)]
        low_shifts = [grouped["l"].shift(-step) for step in range(1, horizon + 1)]
        future_high = pd.concat(high_shifts, axis=1).max(axis=1)
        future_low = pd.concat(low_shifts, axis=1).min(axis=1)
        daily[f"mfe_short_{horizon}d"] = (daily["entry_next_open"] / future_low) - 1.0
        daily[f"mae_short_{horizon}d"] = (future_high / daily["entry_next_open"]) - 1.0
    return daily


def _add_tf_bar_features(frame: pd.DataFrame, *, timeframe: str) -> pd.DataFrame:
    frame = frame.copy()
    frame = frame.sort_values(["code", "dt"]).reset_index(drop=True)
    frame["timeframe"] = timeframe
    frame["trade_idx"] = frame.groupby("code", sort=False).cumcount()
    frame["prev_o"] = frame.groupby("code", sort=False)["o"].shift(1)
    frame["prev_h"] = frame.groupby("code", sort=False)["h"].shift(1)
    frame["prev_l"] = frame.groupby("code", sort=False)["l"].shift(1)
    frame["prev_c"] = frame.groupby("code", sort=False)["c"].shift(1)
    frame["range"] = frame["h"] - frame["l"]
    frame["body"] = (frame["c"] - frame["o"]).abs()
    frame["lower_wick"] = np.minimum(frame["o"], frame["c"]) - frame["l"]
    frame["upper_wick"] = frame["h"] - np.maximum(frame["o"], frame["c"])
    prev_close = frame.groupby("code", sort=False)["c"].shift(1)
    tr1 = frame["h"] - frame["l"]
    tr2 = (frame["h"] - prev_close).abs()
    tr3 = (frame["l"] - prev_close).abs()
    frame["tr"] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    frame["atr20"] = frame.groupby("code", sort=False)["tr"].transform(lambda s: s.rolling(20, min_periods=10).mean())
    frame["vol20"] = frame.groupby("code", sort=False)["v"].transform(lambda s: s.rolling(20, min_periods=10).mean())
    frame["ma20"] = frame.groupby("code", sort=False)["c"].transform(lambda s: s.rolling(20, min_periods=10).mean())
    frame["ma20_prev"] = frame.groupby("code", sort=False)["ma20"].shift(1)
    frame["ma20_slope5"] = frame.groupby("code", sort=False)["ma20"].transform(lambda s: s - s.shift(5))
    frame["dist_ma20"] = np.where(
        frame["ma20"].notna() & (frame["ma20"] > 0.0),
        (frame["c"] / frame["ma20"]) - 1.0,
        np.nan,
    )
    frame["dist_bucket"] = frame["dist_ma20"].map(_bucket_dist_ma20)
    frame["timing_label"] = "other"
    frame["timing_gate"] = False
    frame["box_month_bucket"] = "na"
    frame["box_month_index"] = np.nan
    frame["box_zone"] = "na"
    frame["monthly_context"] = "na"
    frame["weekly_context"] = "na"
    body_ratio = np.where(frame["range"] > 0.0, frame["body"] / frame["range"], np.nan)
    direction = np.where(body_ratio <= 0.2, "X", np.where(frame["c"] >= frame["o"], "U", "D"))
    size = np.where(body_ratio >= 0.6, "L", np.where(body_ratio >= 0.3, "M", "S"))
    wick = np.where(
        (frame["lower_wick"] >= np.maximum(frame["body"], 1e-9) * 1.2) & (frame["lower_wick"] > frame["upper_wick"] * 1.1),
        "WL",
        np.where(
            (frame["upper_wick"] >= np.maximum(frame["body"], 1e-9) * 1.2) & (frame["upper_wick"] > frame["lower_wick"] * 1.1),
            "WU",
            np.where(
                (frame["lower_wick"] >= np.maximum(frame["body"], 1e-9))
                & (frame["upper_wick"] >= np.maximum(frame["body"], 1e-9)),
                "WB",
                "N",
            ),
        ),
    )
    gap = np.where(
        frame["prev_h"].notna() & (frame["o"] > frame["prev_h"] * 1.005),
        "GU",
        np.where(frame["prev_l"].notna() & (frame["o"] < frame["prev_l"] * 0.995), "GD", "NG"),
    )
    break_pos = np.where(
        frame["prev_h"].notna() & (frame["h"] > frame["prev_h"] * 1.005),
        "HB",
        np.where(frame["prev_l"].notna() & (frame["l"] < frame["prev_l"] * 0.995), "LB", "IN"),
    )
    frame["bar_tag"] = pd.Series(direction, index=frame.index).str.cat(pd.Series(size, index=frame.index), sep="")
    frame["bar_tag"] = frame["bar_tag"].str.cat(pd.Series(wick, index=frame.index), sep="-")
    frame["bar_tag"] = frame["bar_tag"].str.cat(pd.Series(gap, index=frame.index), sep="-")
    frame["bar_tag"] = frame["bar_tag"].str.cat(pd.Series(break_pos, index=frame.index), sep="-")
    frame["pattern_2"] = (
        frame.groupby("code", sort=False)["bar_tag"].shift(1).astype("string").fillna("")
        .str.cat(frame["bar_tag"].astype("string"), sep=">")
    )
    frame["pattern_3"] = (
        frame.groupby("code", sort=False)["bar_tag"].shift(2).astype("string").fillna("")
        .str.cat(frame.groupby("code", sort=False)["bar_tag"].shift(1).astype("string").fillna(""), sep=">")
        .str.cat(frame["bar_tag"].astype("string"), sep=">")
    )
    frame["candle_class"] = np.select(
        [
            body_ratio <= 0.2,
            (frame["o"] < frame["prev_l"] * 0.995) & (frame["c"] < frame["o"]),
            (frame["o"] > frame["prev_h"] * 1.005) & (frame["c"] >= frame["o"]),
            (frame["c"] >= frame["o"]) & (frame["lower_wick"] >= frame["body"] * 1.2),
            (frame["c"] < frame["o"]) & (frame["upper_wick"] >= frame["body"] * 1.2),
            (frame["c"] >= frame["o"]) & (body_ratio >= 0.6),
            (frame["c"] < frame["o"]) & (body_ratio >= 0.6),
        ],
        [
            "doji",
            "gap_down_bear",
            "gap_up_bull",
            "reclaim_bull",
            "reject_bear",
            "impulse_bull",
            "impulse_bear",
        ],
        default="other",
    )
    frame["cross_up"] = frame["prev_c"].notna() & frame["ma20_prev"].notna() & (frame["prev_c"] <= frame["ma20_prev"] * 0.995) & (frame["c"] > frame["ma20"] * 1.0)
    frame["cross_down"] = frame["prev_c"].notna() & frame["ma20_prev"].notna() & (frame["prev_c"] >= frame["ma20_prev"] * 1.005) & (frame["c"] < frame["ma20"] * 0.995)
    frame["cross_signal"] = np.select(
        [frame["cross_up"], frame["cross_down"]],
        ["cross_up", "cross_down"],
        default="none",
    )
    return frame


def _aggregate_timeframe_bars(daily: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    if timeframe == "D":
        frame = daily.copy()
        frame["signal_date"] = pd.to_datetime(frame["dt"]).dt.normalize()
        return _add_tf_bar_features(frame, timeframe=timeframe)
    if timeframe == "W":
        period_col = "week_end"
    elif timeframe == "M":
        period_col = "month"
    else:
        raise ValueError(f"unsupported timeframe: {timeframe}")

    frame = (
        daily.sort_values(["code", "dt"])
        .groupby(["code", period_col], as_index=False)
        .agg(
            dt=("dt", "last"),
            o=("o", "first"),
            h=("h", "max"),
            l=("l", "min"),
            c=("c", "last"),
            v=("v", "sum"),
            name=("name", "last"),
        )
        .reset_index(drop=True)
    )
    frame["signal_date"] = pd.to_datetime(frame["dt"]).dt.normalize()
    return _add_tf_bar_features(frame, timeframe=timeframe)


def _add_daily_return_fields(daily: pd.DataFrame) -> pd.DataFrame:
    daily = daily.copy()
    daily["entry_next_open"] = daily.groupby("code", sort=False)["o"].shift(-1)
    grouped = daily.groupby("code", sort=False)
    for horizon in PATH_HORIZONS:
        exit_close = grouped["c"].shift(-(horizon + 1))
        daily[f"ret_long_{horizon}d"] = (exit_close / daily["entry_next_open"]) - 1.0 - ROUND_TRIP_COST
        high_shifts = [grouped["h"].shift(-step) for step in range(1, horizon + 1)]
        low_shifts = [grouped["l"].shift(-step) for step in range(1, horizon + 1)]
        future_high = pd.concat(high_shifts, axis=1).max(axis=1)
        future_low = pd.concat(low_shifts, axis=1).min(axis=1)
        daily[f"mfe_long_{horizon}d"] = (future_high / daily["entry_next_open"]) - 1.0
        daily[f"mae_long_{horizon}d"] = (future_low / daily["entry_next_open"]) - 1.0
        daily[f"ret_short_{horizon}d"] = (daily["entry_next_open"] / exit_close) - 1.0 - ROUND_TRIP_COST
        daily[f"mfe_short_{horizon}d"] = (daily["entry_next_open"] / future_low) - 1.0
        daily[f"mae_short_{horizon}d"] = (future_high / daily["entry_next_open"]) - 1.0
    return daily


def _load_base_frame(db_paths: list[Path]) -> pd.DataFrame:
    daily = _prepare_monthly_box_frame(db_paths)
    daily = _add_time_window_features(daily)
    daily = _add_tf_bar_features(daily, timeframe="D")
    daily = _add_daily_return_fields(daily)
    daily = daily.copy()
    daily["trade_idx"] = daily.groupby("code", sort=False).cumcount()
    daily["signal_date"] = pd.to_datetime(daily["dt"]).dt.normalize()
    daily["name"] = daily["name"].fillna("")
    daily["timing_label"] = daily["timing_label"].fillna("other")
    daily["timing_gate"] = daily["timing_gate"].fillna(False).astype(bool)
    daily["bar_tag"] = daily["bar_tag"].fillna("").astype(str)
    daily["pattern_2"] = daily["pattern_2"].fillna("").astype(str)
    daily["pattern_3"] = daily["pattern_3"].fillna("").astype(str)
    daily["monthly_context"] = daily["monthly_context"].fillna("na").astype(str)
    daily["weekly_context"] = daily["weekly_context"].fillna("na").astype(str)
    daily["box_zone"] = daily["box_zone"].fillna("na").astype(str)
    if "box_month_bucket" not in daily.columns:
        daily["box_month_bucket"] = daily["box_month_index"].map(_bucket_box_month_index)
    daily["box_month_bucket"] = daily["box_month_bucket"].fillna("na").astype(str)
    daily["box_month_index"] = daily["box_month_index"].fillna(np.nan)
    return daily


def _timeframe_cross_events(base_daily: pd.DataFrame, timeframe: str, direction: str) -> pd.DataFrame:
    tf_frame = _aggregate_timeframe_bars(base_daily, timeframe)
    if direction == "up":
        mask = tf_frame["cross_up"]
    else:
        mask = tf_frame["cross_down"]

    signal_rows = tf_frame.loc[mask].copy()
    if signal_rows.empty:
        return signal_rows

    attach_cols = [
        "code",
        "signal_date",
        "trade_idx",
        "name",
        "monthly_context",
        "weekly_context",
        "box_zone",
        "box_month_bucket",
        "box_month_index",
        "timing_label",
        "timing_gate",
        "bar_tag",
        "pattern_2",
        "pattern_3",
        "candle_class",
        "dist_ma20",
        "ma20_slope5",
        "ret_long_5d",
        "ret_long_10d",
        "ret_long_20d",
        "mfe_long_20d",
        "mae_long_20d",
        "ret_short_5d",
        "ret_short_10d",
        "ret_short_20d",
        "mfe_short_20d",
        "mae_short_20d",
    ]
    daily_attach = base_daily[attach_cols].copy()
    daily_attach = daily_attach.rename(
        columns={
            "trade_idx": "daily_trade_idx",
            "name": "daily_name",
            "monthly_context": "daily_monthly_context",
            "weekly_context": "daily_weekly_context",
            "box_zone": "daily_box_zone",
            "box_month_bucket": "daily_box_month_bucket",
            "box_month_index": "daily_box_month_index",
            "timing_label": "daily_timing_label",
            "timing_gate": "daily_timing_gate",
            "bar_tag": "daily_bar_tag",
            "pattern_2": "daily_pattern_2",
            "pattern_3": "daily_pattern_3",
            "candle_class": "daily_candle_class",
            "dist_ma20": "daily_dist_ma20",
            "ma20_slope5": "daily_ma20_slope5",
        }
    )
    merged = signal_rows.merge(daily_attach, how="left", on=["code", "signal_date"], suffixes=("_tf", "_day"))
    merged["signal_timeframe"] = timeframe
    merged["direction"] = direction
    merged["signal_type"] = np.where(direction == "up", "cross_up", "cross_down")
    merged["direction_label"] = merged["direction"].map(DIR_LABELS)
    merged["signal_timeframe_label"] = merged["signal_timeframe"].map(TF_LABELS)
    merged["signal_strength"] = merged["dist_ma20"].abs()
    merged["cross_candle_class"] = merged["candle_class"].fillna("other").astype(str)
    merged["daily_cross_candle_class"] = merged["daily_candle_class"].fillna("other").astype(str)
    merged["daily_cross_gap_days"] = 0
    return merged


def _load_events(base_daily: pd.DataFrame, direction: str) -> pd.DataFrame:
    directions = DIRS if direction == "both" else (direction,)
    frames: list[pd.DataFrame] = []
    for tf in TIMEFRAMES:
        for dir_ in directions:
            events = _timeframe_cross_events(base_daily, tf, dir_)
            if not events.empty:
                frames.append(events)
    if not frames:
        return base_daily.iloc[0:0].copy()
    events = pd.concat(frames, ignore_index=True)
    events["direction_label"] = events["direction"].map(DIR_LABELS)
    return events


def _bucket_gap_days(value: Any) -> str:
    if value is None or not np.isfinite(value):
        return "na"
    gap = int(value)
    if gap == 0:
        return "same_day"
    if gap > 0:
        if gap <= 3:
            return "lead_1_3"
        if gap <= 7:
            return "lead_4_7"
        if gap <= 20:
            return "lead_8_20"
        return "lead_21p"
    gap = abs(gap)
    if gap <= 3:
        return "lag_1_3"
    if gap <= 7:
        return "lag_4_7"
    if gap <= 20:
        return "lag_8_20"
    return "lag_21p"


def _attach_daily_alignment(events: pd.DataFrame) -> pd.DataFrame:
    if events.empty:
        return events
    daily_events = (
        events.loc[events["signal_timeframe"].eq("D"), ["code", "direction", "trade_idx", "signal_date"]]
        .dropna()
        .copy()
    )
    if daily_events.empty:
        events["daily_alignment_gap"] = np.nan
        events["daily_alignment_bucket"] = "na"
        return events

    lookup: dict[tuple[str, str], list[int]] = {}
    for (code, direction), group in daily_events.groupby(["code", "direction"], sort=False):
        lookup[(str(code), str(direction))] = sorted(group["trade_idx"].astype(int).tolist())

    gaps: list[float | None] = []
    buckets: list[str] = []
    for _, row in events.iterrows():
        key = (str(row["code"]), str(row["direction"]))
        trade_idx = row.get("trade_idx")
        candidates = lookup.get(key, [])
        if pd.isna(trade_idx) or not candidates:
            gaps.append(None)
            buckets.append("na")
            continue
        trade_idx_i = int(trade_idx)
        best_gap = min(candidates, key=lambda idx: abs(trade_idx_i - int(idx)))
        gap = trade_idx_i - int(best_gap)
        gaps.append(float(gap))
        buckets.append(_bucket_gap_days(gap))
    events = events.copy()
    events["daily_alignment_gap"] = gaps
    events["daily_alignment_bucket"] = buckets
    return events


def _maybe_add_tf_return_columns(events: pd.DataFrame, direction: str) -> pd.Series:
    if direction == "up":
        return events["ret_long_20d"]
    return events["ret_short_20d"]


def _maybe_add_tf_return_columns_10(events: pd.DataFrame, direction: str) -> pd.Series:
    if direction == "up":
        return events["ret_long_10d"]
    return events["ret_short_10d"]


def _maybe_add_tf_return_columns_5(events: pd.DataFrame, direction: str) -> pd.Series:
    if direction == "up":
        return events["ret_long_5d"]
    return events["ret_short_5d"]


def _directional_summary(frame: pd.DataFrame, direction: str) -> dict[str, Any]:
    ret5 = _summary_from_returns(_maybe_add_tf_return_columns_5(frame, direction))
    ret10 = _summary_from_returns(_maybe_add_tf_return_columns_10(frame, direction))
    ret20 = _summary_from_returns(_maybe_add_tf_return_columns(frame, direction))
    return {
        "n": int(ret20["n"]),
        "mean_5d": ret5["mean"],
        "mean_10d": ret10["mean"],
        "mean_20d": ret20["mean"],
        "win_rate_10d": ret10["win_rate"],
        "win_rate_20d": ret20["win_rate"],
        "profit_factor_10d": ret10["profit_factor"],
        "profit_factor_20d": ret20["profit_factor"],
        "mfe20d": float(frame["mfe_long_20d"].mean()) if direction == "up" else float(frame["mfe_short_20d"].mean()),
        "mae20d": float(frame["mae_long_20d"].mean()) if direction == "up" else float(frame["mae_short_20d"].mean()),
        "avg_signal_strength": float(frame["signal_strength"].mean()) if not frame.empty else None,
        "expected_yen_10d_1m": round((ret10["mean"] or 0.0) * 1_000_000),
        "expected_yen_20d_1m": round((ret20["mean"] or 0.0) * 1_000_000),
    }


def _group_summary(events: pd.DataFrame, group_cols: list[str], direction: str, min_samples: int = 5) -> list[dict[str, Any]]:
    if events.empty:
        return []
    rows: list[dict[str, Any]] = []
    ret_col = "ret_long_20d" if direction == "up" else "ret_short_20d"
    ret10_col = "ret_long_10d" if direction == "up" else "ret_short_10d"
    for keys, group in events.loc[events[ret_col].notna()].groupby(group_cols, dropna=False):
        summary5 = _summary_from_returns(group["ret_long_5d"] if direction == "up" else group["ret_short_5d"])
        summary10 = _summary_from_returns(group[ret10_col])
        summary20 = _summary_from_returns(group[ret_col])
        if int(summary20["n"]) < int(min_samples):
            continue
        row: dict[str, Any] = {}
        if len(group_cols) == 1:
            row[group_cols[0]] = keys if not isinstance(keys, tuple) else keys[0]
        else:
            for idx, col in enumerate(group_cols):
                row[col] = keys[idx]
        row.update(
            {
                "n": int(summary20["n"]),
                "mean_5d": summary5["mean"],
                "mean_10d": summary10["mean"],
                "mean_20d": summary20["mean"],
                "win_rate_10d": summary10["win_rate"],
                "win_rate_20d": summary20["win_rate"],
                "profit_factor_10d": summary10["profit_factor"],
                "profit_factor_20d": summary20["profit_factor"],
                "mfe20d": float(group["mfe_long_20d"].mean()) if direction == "up" else float(group["mfe_short_20d"].mean()),
                "mae20d": float(group["mae_long_20d"].mean()) if direction == "up" else float(group["mae_short_20d"].mean()),
            }
        )
        rows.append(row)
    rows.sort(key=lambda item: (item.get("mean_20d") or -999.0, item.get("win_rate_20d") or -999.0, item.get("n") or 0), reverse=True)
    return rows


def _build_result(base_daily: pd.DataFrame, events: pd.DataFrame, direction: str) -> dict[str, Any]:
    if direction not in {"up", "down"}:
        raise ValueError("direction must be up/down")
    event_frame = events.loc[events["direction"].eq(direction)].copy()
    event_frame = _attach_daily_alignment(event_frame)
    cross_summary = []
    for timeframe in TIMEFRAMES:
        tf_frame = event_frame.loc[event_frame["signal_timeframe"].eq(timeframe)].copy()
        if tf_frame.empty:
            continue
        summary = _directional_summary(tf_frame, direction)
        cross_summary.append(
            {
                "timeframe": timeframe,
                "timeframe_label": TF_LABELS[timeframe],
                "direction": direction,
                "direction_label": DIR_LABELS[direction],
                **summary,
            }
        )
    cross_summary.sort(key=lambda item: (item["timeframe"], -(item.get("mean_20d") or -999.0)))

    candle_summary = _group_summary(event_frame, ["signal_timeframe", "candle_class"], direction)
    daily_candle_summary = _group_summary(event_frame, ["signal_timeframe", "daily_candle_class"], direction)
    bar_tag_summary = _group_summary(event_frame, ["signal_timeframe", "bar_tag"], direction)
    daily_bar_tag_summary = _group_summary(event_frame, ["signal_timeframe", "daily_bar_tag"], direction)
    pattern_summary = _group_summary(event_frame, ["signal_timeframe", "pattern_2"], direction)
    daily_pattern_summary = _group_summary(event_frame, ["signal_timeframe", "daily_pattern_2"], direction)
    monthly_context_summary = _group_summary(event_frame, ["signal_timeframe", "monthly_context"], direction)
    weekly_context_summary = _group_summary(event_frame, ["signal_timeframe", "weekly_context"], direction)
    box_zone_summary = _group_summary(event_frame, ["signal_timeframe", "box_zone"], direction)
    timing_summary = _group_summary(event_frame, ["signal_timeframe", "timing_label"], direction)
    alignment_summary = _group_summary(event_frame, ["signal_timeframe", "daily_alignment_bucket"], direction)
    dist_summary = _group_summary(event_frame, ["signal_timeframe", "dist_bucket"], direction)

    def _label_groups(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        labeled: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["direction"] = direction
            item["direction_label"] = DIR_LABELS[direction]
            tf = item.get("signal_timeframe")
            if tf is not None:
                item["signal_timeframe_label"] = TF_LABELS.get(str(tf), str(tf))
            labeled.append(item)
        return labeled

    candle_summary = _label_groups(candle_summary)
    daily_candle_summary = _label_groups(daily_candle_summary)
    bar_tag_summary = _label_groups(bar_tag_summary)
    daily_bar_tag_summary = _label_groups(daily_bar_tag_summary)
    pattern_summary = _label_groups(pattern_summary)
    daily_pattern_summary = _label_groups(daily_pattern_summary)
    monthly_context_summary = _label_groups(monthly_context_summary)
    weekly_context_summary = _label_groups(weekly_context_summary)
    box_zone_summary = _label_groups(box_zone_summary)
    timing_summary = _label_groups(timing_summary)
    alignment_summary = _label_groups(alignment_summary)
    dist_summary = _label_groups(dist_summary)

    return {
        "cross_summary": cross_summary,
        "candle_summary": candle_summary,
        "daily_candle_summary": daily_candle_summary,
        "bar_tag_summary": bar_tag_summary,
        "daily_bar_tag_summary": daily_bar_tag_summary,
        "pattern_summary": pattern_summary,
        "daily_pattern_summary": daily_pattern_summary,
        "monthly_context_summary": monthly_context_summary,
        "weekly_context_summary": weekly_context_summary,
        "box_zone_summary": box_zone_summary,
        "timing_summary": timing_summary,
        "alignment_summary": alignment_summary,
        "dist_summary": dist_summary,
        "replay_examples": _build_replay_examples(event_frame, direction),
    }


def _build_replay_examples(events: pd.DataFrame, direction: str) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {}
    frame = events.loc[events["direction"].eq(direction)].copy()
    if frame.empty:
        return {code: [] for code in REPLAY_CODES}
    for code in REPLAY_CODES:
        code_frame = frame.loc[frame["code"].astype(str).eq(str(code))].sort_values("signal_date").tail(12).copy()
        rows: list[dict[str, Any]] = []
        for _, row in code_frame.iterrows():
            rows.append(
                {
                    "date": pd.Timestamp(row["signal_date"]).date().isoformat() if pd.notna(row["signal_date"]) else None,
                    "timeframe": str(row.get("signal_timeframe") or ""),
                    "timeframe_label": str(row.get("signal_timeframe_label") or ""),
                    "direction": str(row.get("direction") or ""),
                    "direction_label": str(row.get("direction_label") or ""),
                    "candle_class": str(row.get("candle_class") or ""),
                    "daily_candle_class": str(row.get("daily_candle_class") or ""),
                    "bar_tag": str(row.get("bar_tag") or ""),
                    "daily_bar_tag": str(row.get("daily_bar_tag") or ""),
                    "pattern_2": str(row.get("pattern_2") or ""),
                    "daily_pattern_2": str(row.get("daily_pattern_2") or ""),
                    "monthly_context": str(row.get("monthly_context") or ""),
                    "weekly_context": str(row.get("weekly_context") or ""),
                    "box_zone": str(row.get("box_zone") or ""),
                    "box_month_bucket": str(row.get("box_month_bucket") or ""),
                    "daily_alignment_bucket": str(row.get("daily_alignment_bucket") or "na"),
                    "ret10d": float(row["ret_long_10d"]) if direction == "up" and pd.notna(row["ret_long_10d"]) else (
                        float(row["ret_short_10d"]) if direction == "down" and pd.notna(row["ret_short_10d"]) else None
                    ),
                    "ret20d": float(row["ret_long_20d"]) if direction == "up" and pd.notna(row["ret_long_20d"]) else (
                        float(row["ret_short_20d"]) if direction == "down" and pd.notna(row["ret_short_20d"]) else None
                    ),
                }
            )
        result[str(code)] = rows
    return result


def _build_report(result: dict[str, Any]) -> str:
    def fmt(value: Any, digits: int = 4) -> str:
        if value is None:
            return "na"
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return "na" if math.isnan(value) else "inf"
        if isinstance(value, (int, np.integer)):
            return str(int(value))
        return f"{float(value):.{digits}f}"

    def render_summary_table(title: str, headers: str, rows: list[dict[str, Any]], row_renderer) -> list[str]:
        lines = ["", title, "", headers]
        for row in rows:
            lines.append(row_renderer(row))
        return lines

    lines: list[str] = [
        "# 20か月線割れタイミング研究",
        "",
        "## Setup",
        "",
        f"- DBs: `{', '.join(result['meta']['db_paths'])}`",
        f"- Date range: `{result['meta']['date_min']}` to `{result['meta']['date_max']}`",
        f"- Direction mode: `{result['meta']['direction_mode']}`",
        f"- Events total: `{result['meta']['events_total']}`",
        f"- Round-trip cost: `{result['meta']['round_trip_cost']:.3f}`",
        "",
        "## Direction Summary",
        "",
        "| direction | timeframe | n | mean5d | mean10d | mean20d | win10d | win20d | pf10d | pf20d | mfe20d | mae20d |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in result.get("direction_summary", []):
        lines.append(
            "| "
            + f"{row['direction_label']} | {row['timeframe_label']} | {row['n']} | {fmt(row['mean_5d'])} | {fmt(row['mean_10d'])} | {fmt(row['mean_20d'])} | {fmt(row['win_rate_10d'], 3)} | {fmt(row['win_rate_20d'], 3)} | {fmt(row['profit_factor_10d'])} | {fmt(row['profit_factor_20d'])} | {fmt(row['mfe20d'])} | {fmt(row['mae20d'])} |"
        )

    lines.extend(
        [
            "",
            "## Candle Summary",
            "",
            "| direction | timeframe | candle_class | n | mean10d | mean20d | win10d |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in result.get("candle_summary", []):
        lines.append(
            "| "
            + f"{row['direction_label']} | {row['signal_timeframe_label']} | {row['candle_class']} | {row['n']} | {fmt(row['mean_10d'])} | {fmt(row['mean_20d'])} | {fmt(row['win_rate_10d'], 3)} |"
        )

    lines.extend(
        [
            "",
            "## Daily Candle Summary",
            "",
            "| direction | timeframe | daily_candle_class | n | mean10d | mean20d | win10d |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in result.get("daily_candle_summary", []):
        lines.append(
            "| "
            + f"{row['direction_label']} | {row['signal_timeframe_label']} | {row['daily_candle_class']} | {row['n']} | {fmt(row['mean_10d'])} | {fmt(row['mean_20d'])} | {fmt(row['win_rate_10d'], 3)} |"
        )

    lines.extend(
        [
            "",
            "## Bar Tag Summary",
            "",
            "| direction | timeframe | bar_tag | n | mean10d | mean20d | win10d |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in result.get("bar_tag_summary", []):
        lines.append(
            "| "
            + f"{row['direction_label']} | {row['signal_timeframe_label']} | {row['bar_tag']} | {row['n']} | {fmt(row['mean_10d'])} | {fmt(row['mean_20d'])} | {fmt(row['win_rate_10d'], 3)} |"
        )

    lines.extend(
        [
            "",
            "## Daily Bar Tag Summary",
            "",
            "| direction | timeframe | daily_bar_tag | n | mean10d | mean20d | win10d |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in result.get("daily_bar_tag_summary", []):
        lines.append(
            "| "
            + f"{row['direction_label']} | {row['signal_timeframe_label']} | {row['daily_bar_tag']} | {row['n']} | {fmt(row['mean_10d'])} | {fmt(row['mean_20d'])} | {fmt(row['win_rate_10d'], 3)} |"
        )

    lines.extend(["", "## Pattern Summary", ""])
    pattern_summary = result.get("pattern_summary", {})
    for key, phase_summary in pattern_summary.items():
        lines.extend([f"### {key} pattern_2", "", "| pattern_2 | n | mean10d | mean20d | win10d |", "| --- | ---: | ---: | ---: | ---: |"])
        for row in phase_summary.get("pattern_2", []):
            lines.append(
                "| "
                + f"{row['pattern_2']} | {row['n']} | {fmt(row['mean_10d'])} | {fmt(row['mean_20d'])} | {fmt(row['win_rate_10d'], 3)} |"
            )
        lines.extend([f"### {key} daily_pattern_2", "", "| daily_pattern_2 | n | mean10d | mean20d | win10d |", "| --- | ---: | ---: | ---: | ---: |"])
        for row in phase_summary.get("daily_pattern_2", []):
            lines.append(
                "| "
                + f"{row['daily_pattern_2']} | {row['n']} | {fmt(row['mean_10d'])} | {fmt(row['mean_20d'])} | {fmt(row['win_rate_10d'], 3)} |"
            )
        lines.append("")

    lines.extend(
        [
            "## Monthly Context Summary",
            "",
            "| direction | timeframe | monthly_context | n | mean10d | mean20d | win10d |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in result.get("monthly_context_summary", []):
        lines.append(
            "| "
            + f"{row['direction_label']} | {row['signal_timeframe_label']} | {row['monthly_context']} | {row['n']} | {fmt(row['mean_10d'])} | {fmt(row['mean_20d'])} | {fmt(row['win_rate_10d'], 3)} |"
        )

    lines.extend(
        [
            "",
            "## Weekly Context Summary",
            "",
            "| direction | timeframe | weekly_context | n | mean10d | mean20d | win10d |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in result.get("weekly_context_summary", []):
        lines.append(
            "| "
            + f"{row['direction_label']} | {row['signal_timeframe_label']} | {row['weekly_context']} | {row['n']} | {fmt(row['mean_10d'])} | {fmt(row['mean_20d'])} | {fmt(row['win_rate_10d'], 3)} |"
        )

    lines.extend(
        [
            "",
            "## Box Zone Summary",
            "",
            "| direction | timeframe | box_zone | n | mean10d | mean20d | win10d |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in result.get("box_zone_summary", []):
        lines.append(
            "| "
            + f"{row['direction_label']} | {row['signal_timeframe_label']} | {row['box_zone']} | {row['n']} | {fmt(row['mean_10d'])} | {fmt(row['mean_20d'])} | {fmt(row['win_rate_10d'], 3)} |"
        )

    lines.extend(
        [
            "",
            "## Timing Summary",
            "",
            "| direction | timeframe | timing_label | n | mean10d | mean20d | win10d |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in result.get("timing_summary", []):
        lines.append(
            "| "
            + f"{row['direction_label']} | {row['signal_timeframe_label']} | {row['timing_label']} | {row['n']} | {fmt(row['mean_10d'])} | {fmt(row['mean_20d'])} | {fmt(row['win_rate_10d'], 3)} |"
        )

    lines.extend(
        [
            "",
            "## Daily Alignment Summary",
            "",
            "| direction | timeframe | daily_alignment_bucket | n | mean10d | mean20d | win10d |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in result.get("alignment_summary", []):
        lines.append(
            "| "
            + f"{row['direction_label']} | {row['signal_timeframe_label']} | {row['daily_alignment_bucket']} | {row['n']} | {fmt(row['mean_10d'])} | {fmt(row['mean_20d'])} | {fmt(row['win_rate_10d'], 3)} |"
        )

    lines.extend(
        [
            "",
            "## Dist Summary",
            "",
            "| direction | timeframe | dist_bucket | n | mean10d | mean20d | win10d |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in result.get("dist_summary", []):
        lines.append(
            "| "
            + f"{row['direction_label']} | {row['signal_timeframe_label']} | {row['dist_bucket']} | {row['n']} | {fmt(row['mean_10d'])} | {fmt(row['mean_20d'])} | {fmt(row['win_rate_10d'], 3)} |"
        )

    lines.extend(["", "## Replay Examples", ""])
    replay_examples = result.get("replay_examples", {})
    for direction_key, replay_map in replay_examples.items():
        lines.append(f"### {direction_key}")
        lines.append("")
        for code, rows in replay_map.items():
            lines.extend(
                [
                    f"#### {code}",
                    "",
                    "| date | timeframe | candle | daily_candle | bar_tag | daily_bar_tag | pattern_2 | daily_pattern_2 | monthly_context | weekly_context | box_zone | alignment | ret10d | ret20d |",
                    "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | ---: | ---: |",
                ]
            )
            for row in rows:
                lines.append(
                    "| "
                    + f"{row['date']} | {row['timeframe_label']} | {row['candle_class'] or 'na'} | {row['daily_candle_class'] or 'na'} | {row['bar_tag'] or 'na'} | {row['daily_bar_tag'] or 'na'} | "
                    + f"{row['pattern_2'] or 'na'} | {row['daily_pattern_2'] or 'na'} | {row['monthly_context'] or 'na'} | {row['weekly_context'] or 'na'} | {row['box_zone'] or 'na'} | {row['daily_alignment_bucket'] or 'na'} | "
                    + f"{fmt(row['ret10d'])} | {fmt(row['ret20d'])} |"
                )
            lines.append("")
    return "\n".join(lines) + "\n"


def run_ma20_cross_timing_study(db_paths: list[Path], direction: str = "both") -> dict[str, Any]:
    if direction not in {"up", "down", "both"}:
        raise ValueError("direction must be up/down/both")
    base_daily = _load_base_frame(db_paths)
    events = _load_events(base_daily, direction)
    events["signal_timeframe_label"] = events["signal_timeframe"].map(TF_LABELS)
    events["direction_label"] = events["direction"].map(DIR_LABELS)
    if "box_month_bucket" in events.columns:
        events["analysis_box_month_bucket"] = events["box_month_bucket"].fillna("na").astype(str)
    else:
        events["analysis_box_month_bucket"] = "na"
    events["daily_candle_class"] = events["daily_candle_class"].fillna("other").astype(str)
    events["daily_bar_tag"] = events["daily_bar_tag"].fillna("").astype(str)
    events["daily_pattern_2"] = events["daily_pattern_2"].fillna("").astype(str)
    if direction == "both":
        direction_results = {dir_: _build_result(base_daily, events, dir_) for dir_ in DIRS}
        combined_candle = []
        combined_daily_candle = []
        combined_bar_tag = []
        combined_daily_bar_tag = []
        combined_monthly = []
        combined_weekly = []
        combined_box_zone = []
        combined_timing = []
        combined_alignment = []
        combined_dist = []
        combined_pattern = {}
        combined_replay = {}
        combined_summary = []
        for dir_ in DIRS:
            result = direction_results[dir_]
            combined_summary.extend(result["cross_summary"])
            combined_candle.extend(result["candle_summary"])
            combined_daily_candle.extend(result["daily_candle_summary"])
            combined_bar_tag.extend(result["bar_tag_summary"])
            combined_daily_bar_tag.extend(result["daily_bar_tag_summary"])
            combined_monthly.extend(result["monthly_context_summary"])
            combined_weekly.extend(result["weekly_context_summary"])
            combined_box_zone.extend(result["box_zone_summary"])
            combined_timing.extend(result["timing_summary"])
            combined_alignment.extend(result["alignment_summary"])
            combined_dist.extend(result["dist_summary"])
            combined_pattern[dir_] = {
                "pattern_2": result["pattern_summary"],
                "daily_pattern_2": result["daily_pattern_summary"],
            }
            combined_replay[dir_] = result["replay_examples"]
        return {
            "meta": {
                "db_paths": [str(path) for path in db_paths],
                "date_min": str(base_daily["dt"].min().date()),
                "date_max": str(base_daily["dt"].max().date()),
                "events_total": int(len(events)),
                "round_trip_cost": ROUND_TRIP_COST,
                "direction_mode": direction,
                "direction_labels": [DIR_LABELS[d] for d in DIRS],
                "timeframes": [TF_LABELS[tf] for tf in TIMEFRAMES],
            },
            "direction_summary": combined_summary,
            "candle_summary": combined_candle,
            "daily_candle_summary": combined_daily_candle,
            "bar_tag_summary": combined_bar_tag,
            "daily_bar_tag_summary": combined_daily_bar_tag,
            "pattern_summary": combined_pattern,
            "monthly_context_summary": combined_monthly,
            "weekly_context_summary": combined_weekly,
            "box_zone_summary": combined_box_zone,
            "timing_summary": combined_timing,
            "alignment_summary": combined_alignment,
            "dist_summary": combined_dist,
            "replay_examples": combined_replay,
            "direction_results": direction_results,
        }
    result = _build_result(base_daily, events, direction)
    return {
        "meta": {
            "db_paths": [str(path) for path in db_paths],
            "date_min": str(base_daily["dt"].min().date()),
            "date_max": str(base_daily["dt"].max().date()),
            "events_total": int(len(events)),
            "round_trip_cost": ROUND_TRIP_COST,
            "direction_mode": direction,
            "direction_label": DIR_LABELS[direction],
            "timeframes": [TF_LABELS[tf] for tf in TIMEFRAMES],
        },
        "direction_summary": result["cross_summary"],
        "candle_summary": result["candle_summary"],
        "daily_candle_summary": result["daily_candle_summary"],
        "bar_tag_summary": result["bar_tag_summary"],
        "daily_bar_tag_summary": result["daily_bar_tag_summary"],
        "pattern_summary": {"all": {"pattern_2": result["pattern_summary"], "daily_pattern_2": result["daily_pattern_summary"]}},
        "monthly_context_summary": result["monthly_context_summary"],
        "weekly_context_summary": result["weekly_context_summary"],
        "box_zone_summary": result["box_zone_summary"],
        "timing_summary": result["timing_summary"],
        "alignment_summary": result["alignment_summary"],
        "dist_summary": result["dist_summary"],
        "replay_examples": {direction: result["replay_examples"]},
        "direction_results": {direction: result},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="20-month MA cross timing study with daily candle analysis")
    parser.add_argument("--db-path", action="append", default=[], help="stocks.duckdb path; repeatable")
    parser.add_argument("--dir", choices=("up", "down", "both"), default="both", help="direction mode")
    parser.add_argument("--output-json", type=Path, default=Path("tmp/ma20_cross_timing_study.json"))
    parser.add_argument("--output-md", type=Path, default=Path("tmp/ma20_cross_timing_study.md"))
    args = parser.parse_args()

    db_paths = _resolve_db_paths_from_args(args.db_path)
    result = run_ma20_cross_timing_study(db_paths, direction=args.dir)
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_md.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    args.output_md.write_text(_build_report(result), encoding="utf-8")
    print(f"[ok] wrote {args.output_json}")
    print(f"[ok] wrote {args.output_md}")
    print(json.dumps(result["meta"], ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
