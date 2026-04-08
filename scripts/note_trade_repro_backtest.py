from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
import sys
from typing import Any

import duckdb
import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.month_end_shape_study import classify_5541_premise_bucket
from shared.tradex_storage import tradex_scratch_path


ROUND_TRIP_COST = 0.002
PATH_HORIZONS = [3, 5, 10, 20]
PATTERN_LENGTHS = [2, 3, 4]
FOCUS_CODE = "5541"
PERIOD_BUCKETS: list[tuple[str, str, str]] = [
    ("2016-01-01", "2019-12-31", "2016-2019"),
    ("2020-01-01", "2022-12-31", "2020-2022"),
    ("2023-01-01", "2026-12-31", "2023-2026"),
]
EXIT_CASES = ("climactic_partial", "trend_break", "time_stop")


def _summary_from_returns(values: pd.Series) -> dict[str, Any]:
    arr = values.dropna().to_numpy(dtype=np.float64, copy=False)
    if arr.size == 0:
        return {"n": 0, "mean": None, "median": None, "win_rate": None, "profit_factor": None, "sum": None}
    gains = arr[arr > 0.0].sum()
    losses = -arr[arr < 0.0].sum()
    if losses <= 0.0:
        profit_factor = None if gains <= 0.0 else float("inf")
    else:
        profit_factor = float(gains / losses)
    return {
        "n": int(arr.size),
        "mean": float(arr.mean()),
        "median": float(np.median(arr)),
        "win_rate": float(np.mean(arr > 0.0)),
        "profit_factor": profit_factor,
        "sum": float(arr.sum()),
    }


def _detect_body_box(monthly_rows: list[tuple[pd.Period, float, float, float, float]]) -> dict[str, float] | None:
    if len(monthly_rows) < 3:
        return None
    bars: list[dict[str, float]] = []
    for _, open_, high, low, close in monthly_rows:
        body_high = max(float(open_), float(close))
        body_low = min(float(open_), float(close))
        bars.append(
            {
                "high": float(high),
                "low": float(low),
                "body_high": float(body_high),
                "body_low": float(body_low),
            }
        )
    for length in range(min(14, len(bars)), 2, -1):
        window = bars[-length:]
        upper = max(item["body_high"] for item in window)
        lower = min(item["body_low"] for item in window)
        base = max(abs(lower), 1e-9)
        range_pct = (upper - lower) / base
        if range_pct > 0.2:
            continue
        wild = False
        for item in window:
            if item["high"] > upper * 1.1 or item["low"] < lower * 0.9:
                wild = True
                break
        return {
            "upper": float(upper),
            "lower": float(lower),
            "months": float(length),
            "range_pct": float(range_pct),
            "wild": 1.0 if wild else 0.0,
        }
    return None


def _resolve_default_db_paths() -> list[Path]:
    candidates = [
        Path(".local/meemee/research_db/stocks_research_20160226_20191231.duckdb"),
        Path(".local/meemee/research_db/stocks_research_20200101_20221231.duckdb"),
        Path(".local/meemee/research_db/stocks_research_20230101_20260226.duckdb"),
        Path("data/stocks.duckdb"),
    ]
    existing = [candidate for candidate in candidates if candidate.exists()]
    if existing:
        return existing
    raise FileNotFoundError("stocks.duckdb not found. Pass --db-path.")


def _load_daily_frame(db_paths: list[Path]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for db_path in db_paths:
        with duckdb.connect(str(db_path), read_only=True) as con:
            df = con.execute(
                """
                SELECT
                  b.code,
                  b.date,
                  b.o,
                  b.h,
                  b.l,
                  b.c,
                  b.v,
                  m.ma7,
                  m.ma20,
                  m.ma60
                FROM daily_bars b
                LEFT JOIN daily_ma m
                  ON m.code = b.code AND m.date = b.date
                ORDER BY b.code, b.date
                """
            ).df()
        if not df.empty:
            frames.append(df)
    if not frames:
        raise RuntimeError(f"daily_bars empty: {[str(path) for path in db_paths]}")
    df = pd.concat(frames, ignore_index=True)
    df = df.sort_values(["code", "date"]).drop_duplicates(["code", "date"], keep="last").reset_index(drop=True)
    df["code"] = df["code"].astype(str)
    df["dt"] = pd.to_datetime(df["date"], unit="s", utc=True).dt.tz_localize(None)
    df["month"] = df["dt"].dt.to_period("M")
    df["week_end"] = (df["dt"] + pd.to_timedelta((4 - df["dt"].dt.weekday) % 7, unit="D")).dt.normalize()
    return df


def _consecutive_count(close: pd.Series, avg: pd.Series, below: bool) -> pd.Series:
    values = close.to_numpy(dtype=np.float64, copy=False)
    means = avg.to_numpy(dtype=np.float64, copy=False)
    out = np.zeros(len(close), dtype=np.int32)
    streak = 0
    for idx, (price, ref) in enumerate(zip(values, means)):
        if not np.isfinite(price) or not np.isfinite(ref):
            streak = 0
        else:
            cond = price < ref if below else price > ref
            streak = streak + 1 if cond else 0
        out[idx] = streak
    return pd.Series(out, index=close.index)


def _bucket_count(value: Any, cuts: list[int], labels: list[str]) -> str:
    if value is None or not np.isfinite(value):
        return "na"
    val = int(value)
    for limit, label in zip(cuts, labels):
        if val <= limit:
            return label
    return labels[-1]


def _build_monthly_premise_map(daily: pd.DataFrame) -> pd.DataFrame:
    monthly = (
        daily.sort_values(["code", "dt"])
        .groupby(["code", "month"], as_index=False)
        .agg(o=("o", "first"), h=("h", "max"), l=("l", "min"), c=("c", "last"))
    )
    rows: list[dict[str, Any]] = []
    for code, group in monthly.groupby("code", sort=False):
        g = group.sort_values("month").reset_index(drop=True)
        closes = g["c"].to_numpy(dtype=np.float64)
        opens = g["o"].to_numpy(dtype=np.float64)
        ma6 = g["c"].rolling(6, min_periods=4).mean()
        ma12 = g["c"].rolling(12, min_periods=8).mean()
        for idx in range(len(g)):
            hist_rows = list(g.loc[:idx, ["month", "o", "h", "l", "c"]].itertuples(index=False, name=None))
            box = _detect_body_box(hist_rows)
            close_now = float(closes[idx])
            open_now = float(opens[idx])
            prev6 = closes[max(0, idx - 6) : idx]
            prev3 = closes[max(0, idx - 3) : idx]
            premise_label = "other"
            box_zone = "na"
            premise_bucket = "other"
            monthly_long_trend = bool(
                pd.notna(ma6.iloc[idx]) and pd.notna(ma12.iloc[idx]) and (close_now > float(ma6.iloc[idx]) > float(ma12.iloc[idx]))
            )
            box_months = int(box["months"]) if box else None
            box_range_pct = float(box["range_pct"]) if box else None
            if box and box["upper"] > box["lower"]:
                pos = (close_now - box["lower"]) / (box["upper"] - box["lower"])
                if pos <= 0.25:
                    box_zone = "lower"
                elif pos < 0.75:
                    box_zone = "mid"
                elif pos <= 1.0:
                    box_zone = "upper"
                else:
                    box_zone = "breakout"
                if pos >= 0.75:
                    premise_label = "top_box_reversal"
                elif 0.25 <= pos < 0.75 and box["months"] >= 4:
                    premise_label = "sideways"
                if box["months"] >= 4 and pos >= 0.75:
                    premise_bucket = "5541_long_base_breakout" if monthly_long_trend else "base_breakout_watch"
            if prev6.size >= 3:
                if (
                    close_now > float(np.max(prev6)) * 1.01
                    and close_now > open_now
                    and close_now > float(np.mean(prev6))
                ):
                    premise_label = "up_init"
            if prev3.size >= 3:
                monthly_ret = (close_now / max(open_now, 1e-9)) - 1.0
                expected_range = (float(np.max(prev3)) / max(float(np.min(prev3)), 1e-9)) - 1.0
                if abs(monthly_ret) < 0.02 and expected_range < 0.12 and idx >= 4:
                    premise_label = "unexpected_stagnation"
            rows.append(
                {
                    "code": str(code),
                    "month": g.loc[idx, "month"],
                    "premise_label": premise_label,
                    "premise_bucket": premise_bucket,
                    "box_zone": box_zone,
                    "monthly_box_months": box_months,
                    "monthly_box_range_pct": box_range_pct,
                    "monthly_long_trend": monthly_long_trend,
                }
            )
    premise = pd.DataFrame(rows)
    premise["apply_month"] = premise["month"] + 1
    return premise[
        [
            "code",
            "apply_month",
            "premise_label",
            "premise_bucket",
            "box_zone",
            "monthly_box_months",
            "monthly_box_range_pct",
            "monthly_long_trend",
        ]
    ]


def _build_weekly_context_map(daily: pd.DataFrame) -> pd.DataFrame:
    weekly = (
        daily.sort_values(["code", "dt"])
        .groupby(["code", "week_end"], as_index=False)
        .agg(o=("o", "first"), h=("h", "max"), l=("l", "min"), c=("c", "last"), v=("v", "sum"))
    )
    rows: list[pd.DataFrame] = []
    for _, group in weekly.groupby("code", sort=False):
        g = group.sort_values("week_end").reset_index(drop=True)
        g["wk_ma20"] = g["c"].rolling(20, min_periods=10).mean()
        g["wk_ma20_slope3"] = g["wk_ma20"] - g["wk_ma20"].shift(3)
        g["wk_range_pct"] = (g["h"] - g["l"]) / g["o"].replace(0.0, np.nan)
        g["wk_range_pct_avg8"] = g["wk_range_pct"].rolling(8, min_periods=4).mean().shift(1)
        g["wk_vol_avg8"] = g["v"].rolling(8, min_periods=4).mean().shift(1)
        g["week_slope"] = np.where(
            g["wk_ma20_slope3"] > 0.0,
            "up",
            np.where(g["wk_ma20_slope3"] < 0.0, "down", "flat"),
        )
        g["week_lower_high"] = (g["h"] < g["h"].shift(1)) & (g["h"].shift(1) < g["h"].shift(2))
        prev8_low = g["l"].shift(1).rolling(8, min_periods=3).min()
        g["week_near_prev_low"] = prev8_low.notna() & (g["l"] <= prev8_low * 1.03)
        g["week_support_hold"] = prev8_low.notna() & (g["c"] >= prev8_low * 1.03)
        g["week_climactic"] = (
            g["wk_range_pct_avg8"].notna()
            & g["wk_vol_avg8"].notna()
            & (g["wk_range_pct"] >= g["wk_range_pct_avg8"] * 1.8)
            & (g["v"] >= g["wk_vol_avg8"] * 1.4)
            & (g["c"] > g["o"])
        )
        rows.append(
            g[
                [
                    "code",
                    "week_end",
                    "week_slope",
                    "week_lower_high",
                    "week_near_prev_low",
                    "week_support_hold",
                    "week_climactic",
                ]
            ]
        )
    return pd.concat(rows, ignore_index=True) if rows else weekly.iloc[0:0]


def _add_daily_coordinates(daily: pd.DataFrame) -> pd.DataFrame:
    daily = daily.copy()
    daily["prev_o"] = daily.groupby("code", sort=False)["o"].shift(1)
    daily["prev_h"] = daily.groupby("code", sort=False)["h"].shift(1)
    daily["prev_l"] = daily.groupby("code", sort=False)["l"].shift(1)
    daily["prev_c"] = daily.groupby("code", sort=False)["c"].shift(1)
    daily["prev_v"] = daily.groupby("code", sort=False)["v"].shift(1)
    daily["range"] = daily["h"] - daily["l"]
    daily["body"] = (daily["c"] - daily["o"]).abs()
    daily["lower_wick"] = np.minimum(daily["o"], daily["c"]) - daily["l"]
    daily["upper_wick"] = daily["h"] - np.maximum(daily["o"], daily["c"])
    prev_close = daily.groupby("code", sort=False)["c"].shift(1)
    tr1 = daily["h"] - daily["l"]
    tr2 = (daily["h"] - prev_close).abs()
    tr3 = (daily["l"] - prev_close).abs()
    daily["tr"] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    daily["atr20"] = daily.groupby("code", sort=False)["tr"].transform(lambda s: s.rolling(20, min_periods=10).mean())
    daily["vol20"] = daily.groupby("code", sort=False)["v"].transform(lambda s: s.rolling(20, min_periods=10).mean())
    cnt7_parts = [_consecutive_count(group["c"], group["ma7"], below=True) for _, group in daily.groupby("code", sort=False)]
    cnt20_parts = [_consecutive_count(group["c"], group["ma20"], below=True) for _, group in daily.groupby("code", sort=False)]
    daily["cnt7_down"] = pd.concat(cnt7_parts).sort_index()
    daily["cnt20_down"] = pd.concat(cnt20_parts).sort_index()
    daily["day_pos_ma20"] = np.where(daily["c"] >= daily["ma20"], "above20", "below20")
    daily["day_pos_ma60"] = np.where(daily["c"] >= daily["ma60"], "above60", "below60")
    daily["cnt7_bucket"] = daily["cnt7_down"].map(lambda v: _bucket_count(v, [0, 3, 6], ["0", "1_3", "4_6", "7p"]))
    daily["cnt20_bucket"] = daily["cnt20_down"].map(lambda v: _bucket_count(v, [0, 3, 8], ["0", "1_3", "4_8", "9p"]))
    daily["atr_bucket"] = np.where(
        daily["atr20"].isna(),
        "na",
        np.where(daily["tr"] >= daily["atr20"] * 1.5, "high", np.where(daily["tr"] <= daily["atr20"] * 0.7, "low", "mid")),
    )
    daily["vol_bucket"] = np.where(
        daily["vol20"].isna(),
        "na",
        np.where(daily["v"] >= daily["vol20"] * 1.5, "surge", np.where(daily["v"] <= daily["vol20"] * 0.7, "dry", "mid")),
    )
    daily["dist_ma20"] = np.where(
        daily["ma20"].notna() & (daily["ma20"] > 0.0),
        (daily["c"] / daily["ma20"]) - 1.0,
        np.nan,
    )
    daily["touch_ma20"] = daily["ma20"].notna() & (daily["l"] <= daily["ma20"] * 1.02) & (daily["h"] >= daily["ma20"] * 0.98)
    prev20_high = daily.groupby("code", sort=False)["h"].transform(lambda s: s.shift(1).rolling(20, min_periods=10).max())
    daily["breakout_day"] = prev20_high.notna() & (daily["c"] > prev20_high * 1.01)
    daily["recent_breakout_15d"] = (
        daily.groupby("code", sort=False)["breakout_day"]
        .transform(lambda s: s.shift(1).rolling(15, min_periods=1).max())
        .fillna(0.0)
        .astype(bool)
    )
    daily["climactic_day"] = (
        (daily["atr_bucket"] == "high")
        & (daily["vol_bucket"] == "surge")
        & (daily["c"] >= daily["o"])
        & (
            (daily["prev_h"].notna() & (daily["h"] > daily["prev_h"] * 1.01))
            | (daily["dist_ma20"] >= 0.12)
        )
    )
    daily["support_break_day"] = daily["ma20"].notna() & (daily["c"] < daily["ma20"] * 0.99)
    return daily


def _add_pattern_columns(daily: pd.DataFrame) -> pd.DataFrame:
    daily = daily.copy()
    body_ratio = np.where(daily["range"] > 0.0, daily["body"] / daily["range"], 0.0)
    dir_tag = np.where(body_ratio <= 0.2, "X", np.where(daily["c"] >= daily["o"], "U", "D"))
    size_tag = np.where(body_ratio >= 0.6, "L", np.where(body_ratio >= 0.3, "M", "S"))
    wick_tag = np.where(
        (daily["lower_wick"] >= np.maximum(daily["body"], 1e-9) * 1.2) & (daily["lower_wick"] > daily["upper_wick"] * 1.1),
        "WL",
        np.where(
            (daily["upper_wick"] >= np.maximum(daily["body"], 1e-9) * 1.2) & (daily["upper_wick"] > daily["lower_wick"] * 1.1),
            "WU",
            np.where(
                (daily["lower_wick"] >= np.maximum(daily["body"], 1e-9)) & (daily["upper_wick"] >= np.maximum(daily["body"], 1e-9)),
                "WB",
                "N",
            ),
        ),
    )
    gap_tag = np.where(
        daily["prev_h"].notna() & (daily["o"] > daily["prev_h"] * 1.005),
        "GU",
        np.where(daily["prev_l"].notna() & (daily["o"] < daily["prev_l"] * 0.995), "GD", "NG"),
    )
    break_tag = np.where(
        daily["prev_h"].notna() & (daily["h"] > daily["prev_h"] * 1.005),
        "HB",
        np.where(daily["prev_l"].notna() & (daily["l"] < daily["prev_l"] * 0.995), "LB", "IN"),
    )
    daily["bar_tag"] = pd.Series(dir_tag, index=daily.index).str.cat(pd.Series(size_tag, index=daily.index), sep="")
    daily["bar_tag"] = daily["bar_tag"].str.cat(pd.Series(wick_tag, index=daily.index), sep="-")
    daily["bar_tag"] = daily["bar_tag"].str.cat(pd.Series(gap_tag, index=daily.index), sep="-")
    daily["bar_tag"] = daily["bar_tag"].str.cat(pd.Series(break_tag, index=daily.index), sep="-")
    for length in PATTERN_LENGTHS:
        pieces = [daily.groupby("code", sort=False)["bar_tag"].shift(step) for step in range(length - 1, -1, -1)]
        pattern = pieces[0].astype("string")
        for part in pieces[1:]:
            pattern = pattern.str.cat(part.astype("string"), sep=">")
        daily[f"pattern_{length}"] = pattern
    return daily


def _add_forward_path_metrics(daily: pd.DataFrame, study_mask: pd.Series | None = None) -> pd.DataFrame:
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
        daily[f"mfe_{horizon}d"] = (future_high / daily["entry_next_open"]) - 1.0
        daily[f"mae_{horizon}d"] = (future_low / daily["entry_next_open"]) - 1.0
    daily["hit_up5_before_dn5_20d"] = False
    daily["hit_dn5_before_up5_20d"] = False
    for _, group in daily.groupby("code", sort=False):
        highs = group["h"].to_numpy(dtype=np.float64, copy=False)
        lows = group["l"].to_numpy(dtype=np.float64, copy=False)
        opens = group["o"].to_numpy(dtype=np.float64, copy=False)
        hit_up = np.zeros(len(group), dtype=bool)
        hit_dn = np.zeros(len(group), dtype=bool)
        local_mask = None
        if study_mask is not None:
            local_mask = study_mask.loc[group.index].to_numpy(dtype=bool, copy=False)
        for idx in range(len(group)):
            if local_mask is not None and not local_mask[idx]:
                continue
            if idx + 1 >= len(group) or not np.isfinite(opens[idx + 1]):
                continue
            entry = opens[idx + 1]
            up_thr = entry * 1.05
            dn_thr = entry * 0.95
            up_idx = None
            dn_idx = None
            end = min(len(group), idx + 21)
            for future_idx in range(idx + 1, end):
                if up_idx is None and highs[future_idx] >= up_thr:
                    up_idx = future_idx
                if dn_idx is None and lows[future_idx] <= dn_thr:
                    dn_idx = future_idx
                if up_idx is not None and dn_idx is not None:
                    break
            if up_idx is not None and (dn_idx is None or up_idx < dn_idx):
                hit_up[idx] = True
            if dn_idx is not None and (up_idx is None or dn_idx < up_idx):
                hit_dn[idx] = True
        daily.loc[group.index, "hit_up5_before_dn5_20d"] = hit_up
        daily.loc[group.index, "hit_dn5_before_up5_20d"] = hit_dn
    return daily


def _assign_period_bucket(dt_series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(dt_series)
    bucket = pd.Series("other", index=dt_series.index, dtype="object")
    for start, end, label in PERIOD_BUCKETS:
        mask = (dt >= pd.Timestamp(start)) & (dt <= pd.Timestamp(end))
        bucket.loc[mask] = label
    return bucket


def _classify_entry_case(daily: pd.DataFrame) -> pd.Series:
    premise_ok = daily["premise_bucket"].eq("5541_long_base_breakout")
    weekly_ok = daily["week_slope"].eq("up") & daily["week_support_hold"] & (~daily["week_climactic"])
    pilot_mask = (
        premise_ok
        & weekly_ok
        & daily["box_zone"].isin(["upper", "breakout"])
        & (~daily["recent_breakout_15d"])
        & (~daily["climactic_day"])
        & daily["dist_ma20"].fillna(0.0).between(-0.03, 0.08)
    )
    support_mask = (
        premise_ok
        & weekly_ok
        & daily["recent_breakout_15d"]
        & (daily["touch_ma20"] | daily["cnt7_down"].between(1, 4))
        & (~daily["climactic_day"])
    )
    chase_mask = (
        premise_ok
        & daily["week_slope"].eq("up")
        & daily["climactic_day"]
        & ((daily["dist_ma20"] >= 0.12) | daily["box_zone"].eq("breakout"))
    )
    return pd.Series(
        np.select(
            [pilot_mask, support_mask, chase_mask],
            ["anticipatory_pilot", "first_support_add", "vertical_chase"],
            default="other",
        ),
        index=daily.index,
        dtype="object",
    )


def _classify_path_quality(daily: pd.DataFrame) -> pd.Series:
    clean_mask = daily["hit_up5_before_dn5_20d"] & (daily["mae_20d"] > -0.05) & (daily["ret_long_10d"] > 0.0)
    volatile_mask = daily["ret_long_10d"] > 0.0
    failed_mask = daily["hit_dn5_before_up5_20d"] | (daily["mae_20d"] <= -0.08)
    return pd.Series(
        np.select(
            [clean_mask, volatile_mask, failed_mask],
            ["clean_trend", "volatile_win", "failed_fast"],
            default="stalled",
        ),
        index=daily.index,
        dtype="object",
    )


def _simulate_exit_cases(daily: pd.DataFrame, study_mask: pd.Series) -> pd.DataFrame:
    daily = daily.copy()
    for exit_case in EXIT_CASES:
        daily[f"ret_{exit_case}"] = np.nan
        daily[f"days_{exit_case}"] = np.nan

    grouped = daily.groupby("code", sort=False)
    for _, group in grouped:
        opens = group["o"].to_numpy(dtype=np.float64, copy=False)
        closes = group["c"].to_numpy(dtype=np.float64, copy=False)
        highs = group["h"].to_numpy(dtype=np.float64, copy=False)
        lows = group["l"].to_numpy(dtype=np.float64, copy=False)
        ma20 = group["ma20"].to_numpy(dtype=np.float64, copy=False)
        atr20 = group["atr20"].to_numpy(dtype=np.float64, copy=False)
        vol = group["v"].to_numpy(dtype=np.float64, copy=False)
        vol20 = group["vol20"].to_numpy(dtype=np.float64, copy=False)
        prev_h = group["prev_h"].to_numpy(dtype=np.float64, copy=False)
        tr = group["tr"].to_numpy(dtype=np.float64, copy=False)
        local_mask = study_mask.loc[group.index].to_numpy(dtype=bool, copy=False)

        for local_idx, row_index in enumerate(group.index):
            if not local_mask[local_idx]:
                continue
            entry_idx = local_idx + 1
            if entry_idx >= len(group) or not np.isfinite(opens[entry_idx]) or opens[entry_idx] <= 0.0:
                continue
            horizon_end = min(len(group), local_idx + 21)
            if entry_idx >= horizon_end:
                continue
            entry_price = opens[entry_idx]
            fallback_idx = horizon_end - 1
            fallback_price = closes[fallback_idx]
            if not np.isfinite(fallback_price) or fallback_price <= 0.0:
                continue

            clim_idx = None
            trend_idx = None
            up_hit_10d = False
            eval_end = min(len(group), local_idx + 11)

            for future_idx in range(entry_idx, horizon_end):
                if (
                    clim_idx is None
                    and np.isfinite(tr[future_idx])
                    and np.isfinite(atr20[future_idx])
                    and np.isfinite(vol[future_idx])
                    and np.isfinite(vol20[future_idx])
                    and np.isfinite(prev_h[future_idx])
                    and atr20[future_idx] > 0.0
                    and vol20[future_idx] > 0.0
                    and tr[future_idx] >= atr20[future_idx] * 1.8
                    and vol[future_idx] >= vol20[future_idx] * 1.4
                    and closes[future_idx] >= opens[future_idx]
                    and highs[future_idx] > prev_h[future_idx] * 1.01
                ):
                    clim_idx = future_idx
                if (
                    trend_idx is None
                    and np.isfinite(ma20[future_idx])
                    and closes[future_idx] < ma20[future_idx] * 0.99
                ):
                    trend_idx = future_idx
                if future_idx < eval_end and highs[future_idx] >= entry_price * 1.05:
                    up_hit_10d = True

            trend_exit_idx = fallback_idx
            if trend_idx is not None and trend_idx + 1 < len(group):
                trend_exit_idx = min(trend_idx + 1, fallback_idx)
            time_exit_idx = fallback_idx
            ten_day_idx = min(len(group) - 1, local_idx + 10)
            if (not up_hit_10d) and ten_day_idx >= entry_idx and np.isfinite(closes[ten_day_idx]) and closes[ten_day_idx] <= entry_price * 1.02:
                time_exit_idx = ten_day_idx

            clim_ret = (fallback_price / entry_price) - 1.0
            clim_days = float(fallback_idx - entry_idx + 1)
            if clim_idx is not None and clim_idx + 1 < len(group):
                partial_idx = min(clim_idx + 1, fallback_idx)
                partial_price = opens[partial_idx] if np.isfinite(opens[partial_idx]) and opens[partial_idx] > 0.0 else closes[partial_idx]
                if np.isfinite(partial_price) and partial_price > 0.0:
                    clim_ret = 0.5 * ((partial_price / entry_price) - 1.0) + 0.5 * ((fallback_price / entry_price) - 1.0)
                    clim_days = float(partial_idx - entry_idx + 1)

            trend_price = closes[trend_exit_idx]
            time_price = closes[time_exit_idx]
            if np.isfinite(trend_price) and trend_price > 0.0:
                daily.at[row_index, "ret_trend_break"] = (trend_price / entry_price) - 1.0 - ROUND_TRIP_COST
                daily.at[row_index, "days_trend_break"] = float(trend_exit_idx - entry_idx + 1)
            if np.isfinite(time_price) and time_price > 0.0:
                daily.at[row_index, "ret_time_stop"] = (time_price / entry_price) - 1.0 - ROUND_TRIP_COST
                daily.at[row_index, "days_time_stop"] = float(time_exit_idx - entry_idx + 1)
            daily.at[row_index, "ret_climactic_partial"] = clim_ret - ROUND_TRIP_COST
            daily.at[row_index, "days_climactic_partial"] = clim_days
    return daily


def _group_case_summary(frame: pd.DataFrame, group_cols: list[str], ret_col: str, min_samples: int) -> list[dict[str, Any]]:
    work = frame.loc[frame[ret_col].notna()].copy()
    rows: list[dict[str, Any]] = []
    if work.empty:
        return rows
    for keys, group in work.groupby(group_cols, dropna=False):
        summary = _summary_from_returns(group[ret_col])
        if int(summary["n"]) < int(min_samples):
            continue
        row: dict[str, Any] = {}
        if len(group_cols) == 1:
            row[group_cols[0]] = keys[0] if isinstance(keys, tuple) else keys
        else:
            for idx, col in enumerate(group_cols):
                row[col] = keys[idx]
        row.update(summary)
        row["mfe20d"] = float(group["mfe_20d"].mean())
        row["mae20d"] = float(group["mae_20d"].mean())
        row["up5_before_dn5_20d"] = float(group["hit_up5_before_dn5_20d"].mean())
        row["dn5_before_up5_20d"] = float(group["hit_dn5_before_up5_20d"].mean())
        if "period_bucket" in group.columns:
            row["periods"] = sorted(group["period_bucket"].dropna().unique().tolist())
        rows.append(row)
    rows.sort(key=lambda item: (item.get("mean") or -999.0, item.get("win_rate") or -999.0, item.get("n") or 0), reverse=True)
    return rows


def _build_exit_case_summary(frame: pd.DataFrame, min_samples: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for exit_case in EXIT_CASES:
        ret_col = f"ret_{exit_case}"
        days_col = f"days_{exit_case}"
        work = frame.loc[frame[ret_col].notna()].copy()
        summary = _summary_from_returns(work[ret_col])
        if int(summary["n"]) < int(min_samples):
            continue
        rows.append(
            {
                "exit_case": exit_case,
                **summary,
                "avg_days": float(work[days_col].mean()) if work[days_col].notna().any() else None,
                "median_days": float(work[days_col].median()) if work[days_col].notna().any() else None,
                "mfe20d": float(work["mfe_20d"].mean()) if not work.empty else None,
                "mae20d": float(work["mae_20d"].mean()) if not work.empty else None,
            }
        )
    rows.sort(key=lambda item: (item.get("mean") or -999.0, -(item.get("avg_days") or 999.0)), reverse=True)
    return rows


def _build_replay_rows(frame: pd.DataFrame, code: str) -> list[dict[str, Any]]:
    replay = frame.loc[frame["code"].eq(str(code))].copy()
    if replay.empty:
        return []
    keep_cols = [
        "dt",
        "entry_case",
        "premise_bucket",
        "path_quality",
        "ret_long_10d",
        "ret_long_20d",
        "mfe_20d",
        "mae_20d",
        "ret_climactic_partial",
        "ret_trend_break",
        "ret_time_stop",
    ]
    replay = replay.sort_values("dt")
    rows: list[dict[str, Any]] = []
    for _, row in replay[keep_cols].tail(20).iterrows():
        rows.append(
            {
                "date": pd.Timestamp(row["dt"]).date().isoformat(),
                "entry_case": str(row["entry_case"]),
                "premise_bucket": str(row["premise_bucket"]),
                "path_quality": str(row["path_quality"]),
                "ret10d": float(row["ret_long_10d"]),
                "ret20d": float(row["ret_long_20d"]),
                "mfe20d": float(row["mfe_20d"]),
                "mae20d": float(row["mae_20d"]),
                "ret_climactic_partial": float(row["ret_climactic_partial"]) if pd.notna(row["ret_climactic_partial"]) else None,
                "ret_trend_break": float(row["ret_trend_break"]) if pd.notna(row["ret_trend_break"]) else None,
                "ret_time_stop": float(row["ret_time_stop"]) if pd.notna(row["ret_time_stop"]) else None,
            }
        )
    return rows


def _build_exclusion_rules(frame: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    candidates = [
        ("late_vertical_chase", frame["entry_case"].eq("vertical_chase")),
        ("weekly_climactic", frame["week_climactic"]),
        ("support_not_intact", ~frame["week_support_hold"]),
        ("failed_fast", frame["path_quality"].eq("failed_fast")),
    ]
    baseline = _summary_from_returns(frame["ret_long_10d"])
    baseline_mean = baseline.get("mean")
    for label, mask in candidates:
        subset = frame.loc[mask & frame["ret_long_10d"].notna()]
        if subset.empty:
            continue
        summary = _summary_from_returns(subset["ret_long_10d"])
        rows.append(
            {
                "rule": label,
                "n": int(summary["n"]),
                "mean_ret10d": summary["mean"],
                "win_rate": summary["win_rate"],
                "delta_vs_baseline": (summary["mean"] - baseline_mean) if baseline_mean is not None and summary["mean"] is not None else None,
            }
        )
    rows.sort(key=lambda item: item.get("delta_vs_baseline") if item.get("delta_vs_baseline") is not None else 999.0)
    return rows


def _aggregate_pattern_study(frame: pd.DataFrame, pattern_len: int, min_samples: int) -> list[dict[str, Any]]:
    pattern_col = f"pattern_{pattern_len}"
    work = frame.loc[frame[pattern_col].notna() & frame["ret_long_10d"].notna()].copy()
    baseline = (
        work.groupby("regime_key", as_index=False)
        .agg(
            regime_n=("ret_long_10d", "size"),
            regime_mean=("ret_long_10d", "mean"),
            regime_win_rate=("ret_long_10d", lambda s: float(np.mean(s > 0.0))),
        )
    )
    grouped = (
        work.groupby(["regime_key", pattern_col], as_index=False)
        .agg(
            n=("ret_long_10d", "size"),
            mean_ret_3d=("ret_long_3d", "mean"),
            mean_ret_5d=("ret_long_5d", "mean"),
            mean_ret_10d=("ret_long_10d", "mean"),
            mean_ret_20d=("ret_long_20d", "mean"),
            win_rate_10d=("ret_long_10d", lambda s: float(np.mean(s > 0.0))),
            mfe_20d=("mfe_20d", "mean"),
            mae_20d=("mae_20d", "mean"),
            up5_before_dn5_20d=("hit_up5_before_dn5_20d", "mean"),
            dn5_before_up5_20d=("hit_dn5_before_up5_20d", "mean"),
        )
    )
    merged = grouped.merge(baseline, on="regime_key", how="left")
    merged["delta_mean_10d_vs_regime"] = merged["mean_ret_10d"] - merged["regime_mean"]
    merged["pattern_len"] = pattern_len
    merged = merged.loc[merged["n"] >= int(min_samples)]
    merged = merged.sort_values(["delta_mean_10d_vs_regime", "mean_ret_10d", "n"], ascending=[False, False, False])
    return merged.head(30).to_dict(orient="records")


def _build_markdown_report(result: dict[str, Any]) -> str:
    def _fmt(value: Any, digits: int = 4) -> str:
        if value is None or (isinstance(value, float) and (math.isnan(value) or math.isinf(value))):
            return "na"
        if isinstance(value, (int, np.integer)):
            return str(int(value))
        return f"{float(value):.{digits}f}"

    lines = [
        "# 5541型の再現性検証と早仕込み建玉研究",
        "",
        "## Setup",
        "",
        f"- DBs: `{', '.join(result['meta']['db_paths'])}`",
        f"- Codes: `{result['meta']['codes']}`",
        f"- Date range: `{result['meta']['date_min']}` to `{result['meta']['date_max']}`",
        f"- Round-trip cost: `{result['meta']['round_trip_cost']:.3f}`",
        f"- Min samples per summary: `{result['meta']['min_samples']}`",
        f"- Focus code replay: `{result['meta']['focus_code']}`",
        "",
    ]
    lines.extend(
        [
            "## 5541の局面分解",
            "",
            "| date | entry_case | premise_bucket | path_quality | ret10d | ret20d | exit_climactic | exit_trend_break | exit_time_stop |",
            "| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in result.get("replay", {}).get(result["meta"]["focus_code"], [])[:20]:
        lines.append(
            "| "
            + f"{row['date']} | {row['entry_case']} | {row['premise_bucket']} | {row['path_quality']} | "
            + f"{_fmt(row['ret10d'])} | {_fmt(row['ret20d'])} | {_fmt(row['ret_climactic_partial'])} | "
            + f"{_fmt(row['ret_trend_break'])} | {_fmt(row['ret_time_stop'])} |"
        )
    lines.append("")

    lines.extend(
        [
            "## 同型サンプルの統計",
            "",
            "| premise_bucket | n | mean_ret10d | win_rate | mfe20d | mae20d | up5_before_dn5 |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in result.get("cross_section", {}).get("premise_bucket", [])[:10]:
        lines.append(
            "| "
            + f"{row['premise_bucket']} | {int(row['n'])} | {_fmt(row['mean'])} | {_fmt(row['win_rate'], 3)} | "
            + f"{_fmt(row['mfe20d'])} | {_fmt(row['mae20d'])} | {_fmt(row['up5_before_dn5_20d'], 3)} |"
        )
    lines.append("")

    lines.extend(
        [
            "## 建玉3案の比較",
            "",
            "| entry_case | n | mean_ret10d | win_rate | mfe20d | mae20d |",
            "| --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in result.get("cross_section", {}).get("entry_case", [])[:10]:
        lines.append(
            "| "
            + f"{row['entry_case']} | {int(row['n'])} | {_fmt(row['mean'])} | {_fmt(row['win_rate'], 3)} | "
            + f"{_fmt(row['mfe20d'])} | {_fmt(row['mae20d'])} |"
        )
    lines.append("")

    lines.extend(
        [
            "## 利確・撤退3案の比較",
            "",
            "| exit_case | n | mean_ret | win_rate | avg_days | median_days |",
            "| --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in result.get("exit_case_stats", [])[:10]:
        lines.append(
            "| "
            + f"{row['exit_case']} | {int(row['n'])} | {_fmt(row['mean'])} | {_fmt(row['win_rate'], 3)} | "
            + f"{_fmt(row['avg_days'], 1)} | {_fmt(row['median_days'], 1)} |"
        )
    lines.append("")

    lines.extend(
        [
            "## 失敗しやすい局面の除外条件",
            "",
            "| rule | n | mean_ret10d | delta_vs_baseline | win_rate |",
            "| --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in result.get("exclusion_rules", [])[:10]:
        lines.append(
            "| "
            + f"{row['rule']} | {int(row['n'])} | {_fmt(row['mean_ret10d'])} | {_fmt(row['delta_vs_baseline'])} | {_fmt(row['win_rate'], 3)} |"
        )
    lines.append("")

    for pattern_len in PATTERN_LENGTHS:
        rows = result["pattern_study"].get(f"pattern_{pattern_len}", [])
        lines.extend([f"## Top Pattern {pattern_len}", "", "| regime_key | pattern | n | ret10d | delta_vs_regime | win10d | mfe20d | mae20d | up5_before_dn5 |", "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"])
        for row in rows[:10]:
            lines.append(
                "| "
                + f"{row['regime_key']} | {row[f'pattern_{pattern_len}']} | {int(row['n'])} | "
                + f"{_fmt(row['mean_ret_10d'])} | {_fmt(row['delta_mean_10d_vs_regime'])} | {_fmt(row['win_rate_10d'], 3)} | "
                + f"{_fmt(row['mfe_20d'])} | {_fmt(row['mae_20d'])} | {_fmt(row['up5_before_dn5_20d'], 3)} |"
            )
        lines.append("")
    lines.extend(
        [
            "## Notes",
            "",
            "- `premise_bucket` は 4ヶ月以上のボックス、上限到達/上抜け、上位トレンド維持を優先した 5541型の前提分類。",
            "- `entry_case` は 先回り型、初回押し目追加、垂直追撃 を分離して比較する。",
            "- `path_quality` は `up5_before_dn5_20d` と `mae20d` を使い、勝ち方の質を粗く分類したもの。",
            "- `exit_case` は 同一エントリーに対する利確・撤退案の比較で、本番売買ロジックではない。",
        ]
    )
    return "\n".join(lines) + "\n"


def run_backtest(db_paths: list[Path], min_samples: int) -> dict[str, Any]:
    daily = _load_daily_frame(db_paths)
    premise_map = _build_monthly_premise_map(daily)
    weekly_map = _build_weekly_context_map(daily)
    daily = daily.merge(premise_map, how="left", left_on=["code", "month"], right_on=["code", "apply_month"])
    daily = daily.merge(weekly_map, how="left", on=["code", "week_end"])
    daily["premise_label"] = daily["premise_label"].fillna("other")
    daily["premise_bucket"] = daily["premise_bucket"].fillna("other")
    daily["box_zone"] = daily["box_zone"].fillna("na")
    daily["week_slope"] = daily["week_slope"].fillna("na")
    daily["week_lower_high"] = daily["week_lower_high"].fillna(False).astype(bool)
    daily["week_near_prev_low"] = daily["week_near_prev_low"].fillna(False).astype(bool)
    daily["week_support_hold"] = daily["week_support_hold"].fillna(False).astype(bool)
    daily["week_climactic"] = daily["week_climactic"].fillna(False).astype(bool)
    daily["period_bucket"] = _assign_period_bucket(daily["dt"])
    daily = _add_daily_coordinates(daily)
    monthly_premise_bucket = daily["premise_bucket"].copy()
    derived_premise_bucket = classify_5541_premise_bucket(
        pd.DataFrame(
            {
                "box_months": daily["monthly_box_months"].fillna(0),
                "box_state": np.where(
                    daily["box_zone"].eq("breakout"),
                    "breakout_up",
                    np.where(
                        daily["box_zone"].eq("upper"),
                        "box_upper",
                        np.where(
                            daily["box_zone"].eq("mid"),
                            "box_mid",
                            np.where(daily["box_zone"].eq("lower"), "box_lower", "no_box"),
                        ),
                    ),
                ),
                "trend_bucket": np.where(
                    daily["week_slope"].eq("up") & daily["day_pos_ma60"].eq("above60"),
                    "up",
                    np.where(daily["day_pos_ma60"].eq("above60"), "mixed", "down"),
                ),
                "cnt60_up": np.where(
                    daily["day_pos_ma60"].eq("above60"),
                    30 + daily["cnt7_down"].fillna(0),
                    daily["cnt7_down"].fillna(0),
                ),
                "dist_bucket": np.where(
                    daily["dist_ma20"].isna(),
                    "na",
                    np.where(
                        daily["dist_ma20"] < 0.0,
                        "below",
                        np.where(daily["dist_ma20"] < 0.05, "near", np.where(daily["dist_ma20"] < 0.12, "extended", "overheat")),
                    ),
                ),
                "box_wild": False,
            },
            index=daily.index,
        )
    )
    daily["premise_bucket"] = np.where(derived_premise_bucket.eq("other"), monthly_premise_bucket, derived_premise_bucket)
    daily = _add_pattern_columns(daily)
    daily["entry_case"] = _classify_entry_case(daily)
    study_mask = daily["entry_case"].ne("other")
    daily = _add_forward_path_metrics(daily, study_mask=study_mask)
    daily["path_quality"] = _classify_path_quality(daily)
    daily = _simulate_exit_cases(daily, study_mask=study_mask)
    week_structure = np.where(daily["week_near_prev_low"], "support", np.where(daily["week_lower_high"], "lowerhigh", "neutral"))
    daily["regime_key"] = pd.Series(daily["premise_bucket"], index=daily.index)
    daily["regime_key"] = daily["regime_key"].str.cat(pd.Series("wk_" + daily["week_slope"].astype(str), index=daily.index), sep="|")
    daily["regime_key"] = daily["regime_key"].str.cat(pd.Series(week_structure, index=daily.index), sep="|")
    daily["regime_key"] = daily["regime_key"].str.cat(daily["day_pos_ma20"].astype(str), sep="|")
    daily["regime_key"] = daily["regime_key"].str.cat(daily["day_pos_ma60"].astype(str), sep="|")
    daily["regime_key"] = daily["regime_key"].str.cat(pd.Series("7d_" + daily["cnt7_bucket"].astype(str), index=daily.index), sep="|")
    daily["regime_key"] = daily["regime_key"].str.cat(pd.Series("20d_" + daily["cnt20_bucket"].astype(str), index=daily.index), sep="|")
    daily["regime_key"] = daily["regime_key"].str.cat(pd.Series("atr_" + daily["atr_bucket"].astype(str), index=daily.index), sep="|")
    daily["regime_key"] = daily["regime_key"].str.cat(pd.Series("vol_" + daily["vol_bucket"].astype(str), index=daily.index), sep="|")
    daily["regime_key"] = daily["regime_key"].str.cat(pd.Series("box_" + daily["box_zone"].astype(str), index=daily.index), sep="|")
    daily = daily.loc[study_mask].copy()

    pattern_study = {
        f"pattern_{pattern_len}": _aggregate_pattern_study(daily, pattern_len=pattern_len, min_samples=min_samples)
        for pattern_len in PATTERN_LENGTHS
    }
    cross_section = {
        "premise_bucket": _group_case_summary(daily, ["premise_bucket"], "ret_long_10d", min_samples=max(20, min_samples // 2)),
        "entry_case": _group_case_summary(daily, ["entry_case"], "ret_long_10d", min_samples=max(20, min_samples // 2)),
        "entry_case_by_period": _group_case_summary(
            daily,
            ["period_bucket", "entry_case"],
            "ret_long_10d",
            min_samples=max(10, min_samples // 3),
        ),
        "path_quality": _group_case_summary(daily, ["path_quality"], "ret_long_10d", min_samples=max(20, min_samples // 2)),
    }
    exit_case_stats = _build_exit_case_summary(daily, min_samples=max(20, min_samples // 2))
    strategy_matrix_rows: list[dict[str, Any]] = []
    for exit_case in EXIT_CASES:
        ret_col = f"ret_{exit_case}"
        strategy_matrix_rows.extend(
            {
                **row,
                "exit_case": exit_case,
            }
            for row in _group_case_summary(
                daily,
                ["premise_bucket", "entry_case", "path_quality"],
                ret_col,
                min_samples=max(10, min_samples // 3),
            )
        )
    replay = {FOCUS_CODE: _build_replay_rows(daily, FOCUS_CODE)}
    exclusion_rules = _build_exclusion_rules(daily)
    return {
        "meta": {
            "db_paths": [str(path) for path in db_paths],
            "codes": int(daily["code"].nunique()),
            "date_min": str(daily["dt"].min().date()),
            "date_max": str(daily["dt"].max().date()),
            "round_trip_cost": ROUND_TRIP_COST,
            "min_samples": int(min_samples),
            "study_rows": int(len(daily)),
            "focus_code": FOCUS_CODE,
        },
        "pattern_study": pattern_study,
        "cross_section": cross_section,
        "exit_case_stats": exit_case_stats,
        "strategy_matrix": strategy_matrix_rows,
        "replay": replay,
        "exclusion_rules": exclusion_rules,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Regime x pattern x path backtest for note-style studies")
    parser.add_argument("--db-path", type=Path, action="append", default=None, help="stocks.duckdb path; repeatable")
    parser.add_argument("--min-samples", type=int, default=80)
    parser.add_argument(
        "--output-json",
        type=Path,
        default=tradex_scratch_path("reports", "note_trade_repro_backtest.json").resolve(),
    )
    parser.add_argument(
        "--output-md",
        type=Path,
        default=tradex_scratch_path("reports", "note_trade_repro_backtest.md").resolve(),
    )
    args = parser.parse_args()

    db_paths = args.db_path or _resolve_default_db_paths()
    result = run_backtest(db_paths, min_samples=max(20, int(args.min_samples)))
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_md.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    args.output_md.write_text(_build_markdown_report(result), encoding="utf-8")
    print(f"[ok] wrote {args.output_json}")
    print(f"[ok] wrote {args.output_md}")
    print(json.dumps(result["meta"], ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
