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

from scripts.month_end_shape_study import _detect_body_box
from scripts.note_trade_repro_backtest import (
    ROUND_TRIP_COST,
    _add_daily_coordinates,
    _add_forward_path_metrics,
    _add_pattern_columns,
    _assign_period_bucket,
    _build_weekly_context_map,
    _load_daily_frame,
    _resolve_default_db_paths,
)


MIN_BOX_MONTHS = 4
MAX_BOX_MONTHS = 14
MAX_BOX_RANGE_PCT = 0.20
PATTERN_MIN_SAMPLES = 5
FAILED_BREAKOUT_WINDOW = 20
REPLAY_CODES = ("1605", "5541")
CASE_COLUMNS = [
    "code",
    "name",
    "signal_date",
    "phase",
    "box_id",
    "box_start_month",
    "box_end_month",
    "box_months",
    "box_month_index",
    "box_upper",
    "box_lower",
    "box_range_pct",
    "box_zone",
    "monthly_context",
    "weekly_context",
    "daily_pattern_2",
    "daily_pattern_3",
    "entry_style",
    "breakout_result",
    "failure_reason",
    "ret_5d",
    "ret_10d",
    "ret_20d",
    "mfe_20d",
    "mae_20d",
    "manual_note",
    "source_example",
]
PHASES = ("bottom_entry", "breakout_entry", "failed_breakout_exit")
FAILURE_PRIORITY = (
    "climactic_exhaustion",
    "late_breakout",
    "weak_volume_break",
    "support_break_after_breakout",
    "reentry_into_box",
)


def _month_int_to_period(value: int) -> pd.Period:
    return pd.to_datetime(int(value), unit="s", utc=True).tz_localize(None).to_period("M")


def _compute_box_month_index(box_start: pd.Period, apply_month: pd.Period) -> int:
    return (apply_month.year - box_start.year) * 12 + (apply_month.month - box_start.month) + 1


def _bucket_box_month_index(value: Any) -> str:
    if value is None or not np.isfinite(value):
        return "na"
    months = int(value)
    if months <= 5:
        return "4-5"
    if months <= 8:
        return "6-8"
    if months <= 12:
        return "9-12"
    return "13-14"


def _bucket_dist_ma20(value: Any) -> str:
    if value is None or not np.isfinite(value):
        return "na"
    dist = float(value)
    if dist < -0.05:
        return "far_below"
    if dist < 0.0:
        return "below"
    if dist < 0.05:
        return "near"
    if dist < 0.12:
        return "extended"
    return "overheat"


def _summary_from_series(values: pd.Series) -> dict[str, Any]:
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


def _load_ticker_names(db_paths: list[Path]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for db_path in db_paths:
        with duckdb.connect(str(db_path), read_only=True) as con:
            try:
                df = con.execute("SELECT code, name FROM tickers").df()
            except duckdb.Error:
                continue
        if not df.empty:
            frames.append(df)
    if not frames:
        return pd.DataFrame(columns=["code", "name"])
    out = pd.concat(frames, ignore_index=True)
    out["code"] = out["code"].astype(str)
    out["name"] = out["name"].astype(str)
    return out.drop_duplicates(["code"], keep="last").reset_index(drop=True)


def _load_monthly_frame(db_paths: list[Path]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for db_path in db_paths:
        with duckdb.connect(str(db_path), read_only=True) as con:
            df = con.execute(
                """
                SELECT
                  code,
                  month,
                  o,
                  h,
                  l,
                  c,
                  v
                FROM monthly_bars
                ORDER BY code, month
                """
            ).df()
        if not df.empty:
            frames.append(df)
    if not frames:
        raise RuntimeError(f"monthly_bars empty: {[str(path) for path in db_paths]}")
    monthly = pd.concat(frames, ignore_index=True)
    monthly = monthly.sort_values(["code", "month"]).drop_duplicates(["code", "month"], keep="last").reset_index(drop=True)
    monthly["code"] = monthly["code"].astype(str)
    monthly["period"] = pd.to_datetime(monthly["month"], unit="s", utc=True).dt.tz_localize(None).dt.to_period("M")
    return monthly


def _classify_monthly_context(box_zone: str, monthly_long_trend: bool) -> str:
    if box_zone == "lower":
        return "box_lower_accumulation"
    if box_zone == "mid":
        return "box_mid_repair"
    if box_zone == "upper":
        return "box_upper_pressure" if monthly_long_trend else "box_upper_watch"
    if box_zone == "breakout":
        return "box_breakout_trend" if monthly_long_trend else "box_breakout_watch"
    return "no_active_box"


def _classify_weekly_context(frame: pd.DataFrame) -> pd.Series:
    return pd.Series(
        np.select(
            [
                frame["week_climactic"],
                frame["week_slope"].eq("up") & frame["week_support_hold"],
                frame["week_slope"].eq("flat") & frame["week_support_hold"],
                ~frame["week_support_hold"],
            ],
            [
                "weekly_climactic",
                "up_support_intact",
                "flat_support_intact",
                "support_broken",
            ],
            default="mixed_or_down",
        ),
        index=frame.index,
        dtype="object",
    )


def _daily_box_zone(close: Any, lower: Any, upper: Any) -> str:
    if not np.isfinite(close) or not np.isfinite(lower) or not np.isfinite(upper) or float(upper) <= float(lower):
        return "na"
    pos = (float(close) - float(lower)) / (float(upper) - float(lower))
    if pos < 0.0:
        return "below"
    if pos <= 0.25:
        return "lower"
    if pos < 0.75:
        return "mid"
    if pos <= 1.0:
        return "upper"
    return "breakout"


def _phase_start(mask: pd.Series, codes: pd.Series) -> pd.Series:
    out = np.zeros(len(mask), dtype=bool)
    mask_arr = mask.fillna(False).to_numpy(dtype=bool, copy=False)
    code_arr = codes.astype(str).to_numpy(copy=False)
    prev_code = ""
    prev_active = False
    for idx, active in enumerate(mask_arr):
        code = code_arr[idx]
        if idx == 0 or code != prev_code:
            prev_active = False
        out[idx] = bool(active and not prev_active)
        prev_code = code
        prev_active = bool(active)
    return pd.Series(out, index=mask.index)


def _build_monthly_box_map(monthly: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for code, group in monthly.groupby("code", sort=False):
        g = group.sort_values("period").reset_index(drop=True)
        ma6 = g["c"].rolling(6, min_periods=4).mean()
        ma12 = g["c"].rolling(12, min_periods=8).mean()
        monthly_rows = list(
            zip(
                g["month"].astype(int).tolist(),
                g["o"].astype(float).tolist(),
                g["h"].astype(float).tolist(),
                g["l"].astype(float).tolist(),
                g["c"].astype(float).tolist(),
            )
        )
        for idx, period in enumerate(g["period"]):
            box = _detect_body_box(monthly_rows[: idx + 1])
            if not box:
                continue
            box_months = int(box["months"])
            box_range_pct = float(box["range_pct"])
            if box_months < MIN_BOX_MONTHS or box_months > MAX_BOX_MONTHS or box_range_pct > MAX_BOX_RANGE_PCT:
                continue
            box_start = _month_int_to_period(int(box["start"]))
            box_end = _month_int_to_period(int(box["end"]))
            apply_month = period + 1
            box_month_index = _compute_box_month_index(box_start, apply_month)
            close_now = float(g.loc[idx, "c"])
            lower = float(box["lower"])
            upper = float(box["upper"])
            box_zone = _daily_box_zone(close_now, lower, upper)
            monthly_long_trend = bool(
                pd.notna(ma6.iloc[idx]) and pd.notna(ma12.iloc[idx]) and (close_now > float(ma6.iloc[idx]) > float(ma12.iloc[idx]))
            )
            rows.append(
                {
                    "code": str(code),
                    "apply_month": apply_month,
                    "box_id": f"{code}:{box_start}:{box_end}",
                    "box_start_month": str(box_start),
                    "box_end_month": str(box_end),
                    "box_months": box_months,
                    "box_month_index": box_month_index,
                    "box_month_bucket": _bucket_box_month_index(box_month_index),
                    "box_upper": upper,
                    "box_lower": lower,
                    "box_range_pct": box_range_pct,
                    "box_wild": bool(box["wild"]),
                    "monthly_long_trend": monthly_long_trend,
                    "monthly_context_base": _classify_monthly_context(box_zone, monthly_long_trend),
                }
            )
    return pd.DataFrame(rows)


def _prepare_frame(db_paths: list[Path]) -> pd.DataFrame:
    daily = _load_daily_frame(db_paths)
    names = _load_ticker_names(db_paths)
    monthly = _load_monthly_frame(db_paths)
    box_map = _build_monthly_box_map(monthly)
    weekly_map = _build_weekly_context_map(daily)

    daily = daily.merge(names, how="left", on="code")
    daily = daily.merge(box_map, how="left", left_on=["code", "month"], right_on=["code", "apply_month"])
    daily = daily.merge(weekly_map, how="left", on=["code", "week_end"])
    daily["name"] = daily["name"].fillna("")
    daily["week_slope"] = daily["week_slope"].fillna("na")
    daily["week_support_hold"] = daily["week_support_hold"].fillna(False).astype(bool)
    daily["week_climactic"] = daily["week_climactic"].fillna(False).astype(bool)
    daily = _add_daily_coordinates(daily)
    daily = _add_pattern_columns(daily)
    daily = _add_forward_path_metrics(daily)
    daily["period_bucket"] = _assign_period_bucket(daily["dt"])
    daily["box_active"] = daily["box_months"].fillna(0).ge(MIN_BOX_MONTHS)
    daily["box_zone"] = [
        _daily_box_zone(close, lower, upper)
        for close, lower, upper in zip(daily["c"], daily["box_lower"], daily["box_upper"])
    ]
    prev_ma20 = daily.groupby("code", sort=False)["ma20"].shift(1)
    daily["daily_ma20_reclaim"] = (
        prev_ma20.notna()
        & daily["prev_c"].notna()
        & daily["ma20"].notna()
        & (daily["prev_c"] < prev_ma20 * 0.995)
        & (daily["c"] >= daily["ma20"] * 1.0)
    )
    daily["dist_bucket"] = daily["dist_ma20"].map(_bucket_dist_ma20)
    monthly_trend = daily["monthly_long_trend"].where(daily["monthly_long_trend"].notna(), False).astype(bool)
    daily["monthly_context"] = [
        _classify_monthly_context(zone, bool(trend))
        for zone, trend in zip(daily["box_zone"], monthly_trend)
    ]
    daily["weekly_context"] = _classify_weekly_context(daily)
    daily["daily_pattern_2"] = daily["pattern_2"]
    daily["daily_pattern_3"] = daily["pattern_3"]
    daily["source_example"] = np.where(
        daily["code"].eq("1605"),
        "1605_monthly_box",
        np.where(daily["code"].eq("5541"), "5541_monthly_box", ""),
    )
    return daily


def _build_phase_masks(daily: pd.DataFrame) -> dict[str, pd.Series]:
    bottom_candle = (
        daily["bar_tag"].fillna("").str.contains("WL", regex=False)
        | (
            (daily["c"] >= daily["o"])
            & daily["range"].gt(0.0)
            & (daily["body"] / daily["range"]).le(0.55)
        )
        | daily["daily_ma20_reclaim"]
    )
    bottom_raw = (
        daily["box_active"]
        & daily["box_zone"].isin(["lower", "mid"])
        & daily["week_slope"].isin(["flat", "up"])
        & bottom_candle
        & (daily["dist_bucket"] != "overheat")
    )
    breakout_raw = (
        daily["box_active"]
        & daily["box_zone"].isin(["upper", "breakout"])
        & daily["week_slope"].isin(["flat", "up"])
        & daily["week_support_hold"]
        & daily["bar_tag"].fillna("").str.endswith("HB")
        & daily["box_upper"].notna()
        & (daily["h"] >= daily["box_upper"] * 1.005)
        & (daily["c"] >= daily["box_upper"] * 0.995)
    )
    return {
        "bottom_entry": _phase_start(bottom_raw, daily["code"]),
        "breakout_entry": _phase_start(breakout_raw, daily["code"]),
    }


def _failed_breakout_position(group: pd.DataFrame, breakout_pos: int, window: int = FAILED_BREAKOUT_WINDOW) -> int | None:
    breakout_row = group.iloc[breakout_pos]
    if not np.isfinite(breakout_row["box_upper"]):
        return None
    box_upper = float(breakout_row["box_upper"])
    end = min(len(group) - 1, breakout_pos + int(window))
    need_support_fail = bool(breakout_row["hit_dn5_before_up5_20d"])
    for pos in range(breakout_pos + 1, end + 1):
        row = group.iloc[pos]
        reentry = bool(np.isfinite(row["c"]) and row["c"] < box_upper * 0.99)
        support_fail = bool(row["support_break_day"]) or (not bool(row["week_support_hold"]))
        if reentry or (need_support_fail and support_fail):
            return pos
    return None


def _classify_failure_reason(breakout_row: dict[str, Any] | pd.Series, fail_row: dict[str, Any] | pd.Series) -> str:
    breakout = dict(breakout_row)
    fail = dict(fail_row)
    breakout_upper = float(breakout.get("box_upper") or 0.0)
    fail_close = float(fail.get("c") or 0.0)
    climactic = bool(breakout.get("climactic_day")) or bool(breakout.get("week_climactic"))
    late = bool((breakout.get("box_month_index") or 0) >= 9)
    weak_volume = str(breakout.get("vol_bucket") or "na") in {"dry", "mid"}
    support_break = bool(fail.get("support_break_day")) or (not bool(fail.get("week_support_hold", True)))
    reentry = breakout_upper > 0.0 and fail_close < breakout_upper * 0.99

    checks = {
        "climactic_exhaustion": climactic,
        "late_breakout": late,
        "weak_volume_break": weak_volume,
        "support_break_after_breakout": support_break and not reentry,
        "reentry_into_box": reentry,
    }
    for label in FAILURE_PRIORITY:
        if checks.get(label):
            return label
    return "reentry_into_box"


def _classify_entry_style(row: pd.Series) -> str:
    if row["phase"] == "bottom_entry":
        if bool(row.get("daily_ma20_reclaim")):
            return "bottom_ma20_reclaim"
        if "WL" in str(row.get("bar_tag") or ""):
            return "bottom_lower_wick"
        return "bottom_small_up"
    if row["phase"] == "breakout_entry":
        if bool(row.get("climactic_day")):
            return "breakout_climactic"
        if "GU" in str(row.get("bar_tag") or ""):
            return "breakout_gap_hb"
        return "breakout_hb"
    return "failed_breakout_exit"


def _build_failed_breakout_events(daily: pd.DataFrame, breakout_mask: pd.Series) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, group in daily.groupby("code", sort=False):
        local_mask = breakout_mask.loc[group.index].to_numpy(dtype=bool, copy=False)
        breakout_positions = np.flatnonzero(local_mask)
        used_positions: set[int] = set()
        for breakout_pos in breakout_positions:
            fail_pos = _failed_breakout_position(group, int(breakout_pos))
            if fail_pos is None or fail_pos in used_positions:
                continue
            used_positions.add(int(fail_pos))
            breakout_row = group.iloc[int(breakout_pos)]
            fail_row = group.iloc[int(fail_pos)].copy()
            fail_row["phase"] = "failed_breakout_exit"
            fail_row["breakout_origin_date"] = pd.Timestamp(breakout_row["dt"]).date().isoformat()
            fail_row["breakout_result"] = "failed_breakout"
            fail_row["failure_reason"] = _classify_failure_reason(breakout_row, fail_row)
            fail_row["origin_box_upper"] = breakout_row["box_upper"]
            rows.append(fail_row.to_dict())
    if not rows:
        return daily.iloc[0:0].copy()
    return pd.DataFrame(rows)


def _build_event_frame(daily: pd.DataFrame, phase_masks: dict[str, pd.Series]) -> pd.DataFrame:
    breakout_events = daily.loc[phase_masks["breakout_entry"] & daily["ret_long_10d"].notna()].copy()
    breakout_events["phase"] = "breakout_entry"
    failure_events = _build_failed_breakout_events(daily, phase_masks["breakout_entry"])
    failure_lookup = {}
    if not failure_events.empty:
        for _, row in failure_events.iterrows():
            origin_date = str(row["breakout_origin_date"])
            failure_lookup[(str(row["code"]), origin_date)] = str(row["failure_reason"])

    bottom_events = daily.loc[phase_masks["bottom_entry"] & daily["ret_long_10d"].notna()].copy()
    bottom_events["phase"] = "bottom_entry"
    bottom_events["breakout_result"] = np.where(bottom_events["ret_long_20d"] > 0.0, "bottom_working", "bottom_failed")
    bottom_events["failure_reason"] = None

    breakout_events["breakout_result"] = [
        "failed_breakout"
        if (str(code), pd.Timestamp(dt).date().isoformat()) in failure_lookup
        else ("successful_breakout" if bool(hit_up) and float(ret20) > 0.0 else "stalled_breakout")
        for code, dt, hit_up, ret20 in zip(
            breakout_events["code"],
            breakout_events["dt"],
            breakout_events["hit_up5_before_dn5_20d"],
            breakout_events["ret_long_20d"],
        )
    ]
    breakout_events["failure_reason"] = [
        failure_lookup.get((str(code), pd.Timestamp(dt).date().isoformat()))
        for code, dt in zip(breakout_events["code"], breakout_events["dt"])
    ]

    if not failure_events.empty:
        failure_events["breakout_result"] = "failed_breakout"
    frames = [bottom_events, breakout_events]
    if not failure_events.empty:
        frames.append(failure_events)
    events = pd.concat(frames, ignore_index=True) if frames else daily.iloc[0:0].copy()
    if events.empty:
        return events
    events["entry_style"] = events.apply(_classify_entry_style, axis=1)
    events["daily_pattern_2"] = events["pattern_2"]
    events["daily_pattern_3"] = events["pattern_3"]
    events["signal_date"] = events["dt"].dt.date.astype(str)
    events["manual_note"] = ""
    return events


def _phase_summary_row(phase: str, frame: pd.DataFrame) -> dict[str, Any]:
    summary10 = _summary_from_series(frame["ret_long_10d"])
    summary20 = _summary_from_series(frame["ret_long_20d"])
    row = {
        "phase": phase,
        **summary10,
        "mean_ret20d": summary20["mean"],
        "median_ret20d": summary20["median"],
        "mfe20d": float(frame["mfe_20d"].mean()) if not frame.empty else None,
        "mae20d": float(frame["mae_20d"].mean()) if not frame.empty else None,
        "up5_before_dn5_20d": float(frame["hit_up5_before_dn5_20d"].mean()) if not frame.empty else None,
        "dn5_before_up5_20d": float(frame["hit_dn5_before_up5_20d"].mean()) if not frame.empty else None,
        "expected_yen_10d_1m": round((summary10["mean"] or 0.0) * 1_000_000),
        "expected_yen_20d_1m": round((summary20["mean"] or 0.0) * 1_000_000),
    }
    if phase == "failed_breakout_exit":
        row["avoid_yen_10d_1m"] = round(-(summary10["mean"] or 0.0) * 1_000_000)
        row["avoid_yen_20d_1m"] = round(-(summary20["mean"] or 0.0) * 1_000_000)
    return row


def _build_phase_summary(events: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for phase in PHASES:
        frame = events.loc[events["phase"].eq(phase)]
        if frame.empty:
            continue
        rows.append(_phase_summary_row(phase, frame))
    return rows


def _group_phase_stats(
    events: pd.DataFrame,
    group_cols: list[str],
    ret_col10: str = "ret_long_10d",
    ret_col20: str = "ret_long_20d",
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    work = events.loc[events[ret_col10].notna()].copy()
    if work.empty:
        return rows
    for keys, group in work.groupby(group_cols, dropna=False):
        summary10 = _summary_from_series(group[ret_col10])
        summary20 = _summary_from_series(group[ret_col20])
        if int(summary10["n"]) < PATTERN_MIN_SAMPLES:
            continue
        row: dict[str, Any] = {}
        if len(group_cols) == 1:
            row[group_cols[0]] = keys
        else:
            for idx, col in enumerate(group_cols):
                row[col] = keys[idx]
        row["n"] = int(summary10["n"])
        row["mean_ret10d"] = summary10["mean"]
        row["mean_ret20d"] = summary20["mean"]
        row["win_rate10d"] = summary10["win_rate"]
        row["mfe20d"] = float(group["mfe_20d"].mean())
        row["mae20d"] = float(group["mae_20d"].mean())
        rows.append(row)
    rows.sort(key=lambda item: ((item.get("mean_ret10d") or -999.0), item.get("n") or 0), reverse=True)
    return rows


def _build_month_index_summary(events: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for (phase, bucket), group in events.groupby(["phase", "box_month_bucket"], dropna=False):
        summary = _phase_summary_row(str(phase), group)
        summary["box_month_bucket"] = str(bucket)
        rows.append(summary)
    rows.sort(key=lambda item: (item["phase"], item.get("box_month_bucket", "")))
    return rows


def _build_period_summary(events: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for (phase, period_bucket), group in events.groupby(["phase", "period_bucket"], dropna=False):
        summary = _phase_summary_row(str(phase), group)
        summary["period_bucket"] = str(period_bucket)
        rows.append(summary)
    rows.sort(key=lambda item: (item["phase"], item.get("period_bucket", "")))
    return rows


def _build_pattern_summary(events: pd.DataFrame) -> dict[str, dict[str, list[dict[str, Any]]]]:
    summary: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for phase in PHASES:
        frame = events.loc[events["phase"].eq(phase)].copy()
        if frame.empty:
            summary[phase] = {"pattern_2": [], "pattern_3": [], "shape_combo": []}
            continue
        frame["shape_combo"] = (
            frame["weekly_context"].fillna("na")
            + "|"
            + frame["box_month_bucket"].fillna("na")
            + "|"
            + frame["vol_bucket"].fillna("na")
            + "|"
            + frame["atr_bucket"].fillna("na")
            + "|"
            + frame["dist_bucket"].fillna("na")
        )
        pattern_2 = _group_phase_stats(frame, ["phase", "daily_pattern_2"])
        pattern_3 = _group_phase_stats(frame, ["phase", "daily_pattern_3"])
        shape_combo = _group_phase_stats(frame, ["phase", "shape_combo"])
        for rows in (pattern_2, pattern_3, shape_combo):
            for row in rows:
                row.pop("phase", None)
        summary[phase] = {
            "pattern_2": pattern_2[:10],
            "pattern_3": pattern_3[:10],
            "shape_combo": shape_combo[:10],
        }
    return summary


def _build_failure_summary(events: pd.DataFrame) -> list[dict[str, Any]]:
    frame = events.loc[events["phase"].eq("failed_breakout_exit") & events["failure_reason"].notna()].copy()
    if frame.empty:
        return []
    rows: list[dict[str, Any]] = []
    for reason, group in frame.groupby("failure_reason", dropna=False):
        summary10 = _summary_from_series(group["ret_long_10d"])
        summary20 = _summary_from_series(group["ret_long_20d"])
        rows.append(
            {
                "failure_reason": str(reason),
                "n": int(summary10["n"]),
                "mean_ret10d": summary10["mean"],
                "mean_ret20d": summary20["mean"],
                "win_rate10d": summary10["win_rate"],
                "avoid_yen_10d_1m": round(-(summary10["mean"] or 0.0) * 1_000_000),
                "avoid_yen_20d_1m": round(-(summary20["mean"] or 0.0) * 1_000_000),
            }
        )
    rows.sort(key=lambda item: (item.get("mean_ret10d") or 999.0, -(item.get("n") or 0)))
    return rows


def _build_bottom_entry_summary(events: pd.DataFrame) -> list[dict[str, Any]]:
    frame = events.loc[events["phase"].eq("bottom_entry")].copy()
    if frame.empty:
        return []
    rows = _group_phase_stats(frame, ["box_zone", "entry_style"])
    return rows[:20]


def _build_replay_examples(daily: pd.DataFrame, events: pd.DataFrame) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {}
    for code in REPLAY_CODES:
        frame = events.loc[events["code"].eq(code)].sort_values("dt").tail(12).copy()
        if frame.empty:
            frame = (
                daily.loc[daily["code"].eq(code) & daily["box_active"]]
                .sort_values("dt")
                .tail(6)
                .copy()
            )
            if not frame.empty:
                frame["phase"] = "watch"
                frame["entry_style"] = "watch"
                frame["breakout_result"] = None
                frame["failure_reason"] = None
        rows: list[dict[str, Any]] = []
        for _, row in frame.iterrows():
            rows.append(
                {
                    "date": pd.Timestamp(row["dt"]).date().isoformat(),
                    "phase": str(row["phase"]),
                    "box_id": str(row["box_id"]) if pd.notna(row["box_id"]) else None,
                    "box_month_index": int(row["box_month_index"]) if pd.notna(row["box_month_index"]) else None,
                    "monthly_context": str(row["monthly_context"]),
                    "weekly_context": str(row["weekly_context"]),
                    "daily_pattern_2": str(row["daily_pattern_2"]) if pd.notna(row["daily_pattern_2"]) else None,
                    "entry_style": str(row["entry_style"]) if pd.notna(row["entry_style"]) else None,
                    "breakout_result": str(row["breakout_result"]) if pd.notna(row["breakout_result"]) else None,
                    "failure_reason": str(row["failure_reason"]) if pd.notna(row["failure_reason"]) else None,
                    "ret10d": float(row["ret_long_10d"]) if pd.notna(row["ret_long_10d"]) else None,
                    "ret20d": float(row["ret_long_20d"]) if pd.notna(row["ret_long_20d"]) else None,
                }
            )
        result[code] = rows
    return result


def _build_cases_frame(events: pd.DataFrame) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame(columns=CASE_COLUMNS)
    cases = pd.DataFrame(
        {
            "code": events["code"].astype(str),
            "name": events["name"].fillna("").astype(str),
            "signal_date": events["signal_date"].astype(str),
            "phase": events["phase"].astype(str),
            "box_id": events["box_id"].fillna("").astype(str),
            "box_start_month": events["box_start_month"].fillna("").astype(str),
            "box_end_month": events["box_end_month"].fillna("").astype(str),
            "box_months": events["box_months"],
            "box_month_index": events["box_month_index"],
            "box_upper": events["box_upper"],
            "box_lower": events["box_lower"],
            "box_range_pct": events["box_range_pct"],
            "box_zone": events["box_zone"].fillna("na").astype(str),
            "monthly_context": events["monthly_context"].fillna("na").astype(str),
            "weekly_context": events["weekly_context"].fillna("na").astype(str),
            "daily_pattern_2": events["daily_pattern_2"].fillna("").astype(str),
            "daily_pattern_3": events["daily_pattern_3"].fillna("").astype(str),
            "entry_style": events["entry_style"].fillna("").astype(str),
            "breakout_result": events["breakout_result"].fillna("").astype(str),
            "failure_reason": events["failure_reason"].fillna("").astype(str),
            "ret_5d": events["ret_long_5d"],
            "ret_10d": events["ret_long_10d"],
            "ret_20d": events["ret_long_20d"],
            "mfe_20d": events["mfe_20d"],
            "mae_20d": events["mae_20d"],
            "manual_note": events["manual_note"].fillna("").astype(str),
            "source_example": events["source_example"].fillna("").astype(str),
        }
    )
    return cases[CASE_COLUMNS].sort_values(["signal_date", "code", "phase"]).reset_index(drop=True)


def _build_report(result: dict[str, Any]) -> str:
    def fmt(value: Any, digits: int = 4) -> str:
        if value is None:
            return "na"
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return "na" if math.isnan(value) else "inf"
        if isinstance(value, (int, np.integer)):
            return str(int(value))
        return f"{float(value):.{digits}f}"

    lines = [
        "# 月足ボックス breakout / failed breakout 研究",
        "",
        "## Setup",
        "",
        f"- DBs: `{', '.join(result['meta']['db_paths'])}`",
        f"- Date range: `{result['meta']['date_min']}` to `{result['meta']['date_max']}`",
        f"- Events total: `{result['meta']['events_total']}`",
        f"- Round-trip cost: `{result['meta']['round_trip_cost']:.3f}`",
        "",
        "## Phase Summary",
        "",
        "| phase | n | mean10d | mean20d | win10d | mfe20d | mae20d | 1m_yen_10d | 1m_yen_20d |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in result["phase_summary"]:
        lines.append(
            "| "
            + f"{row['phase']} | {row['n']} | {fmt(row['mean'])} | {fmt(row['mean_ret20d'])} | {fmt(row['win_rate'], 3)} | "
            + f"{fmt(row['mfe20d'])} | {fmt(row['mae20d'])} | {row['expected_yen_10d_1m']} | {row['expected_yen_20d_1m']} |"
        )

    lines.extend(
        [
            "",
            "## Month Index Summary",
            "",
            "| phase | box_month_bucket | n | mean10d | mean20d | win10d |",
            "| --- | --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in result["month_index_summary"]:
        lines.append(
            "| "
            + f"{row['phase']} | {row['box_month_bucket']} | {row['n']} | {fmt(row['mean'])} | {fmt(row['mean_ret20d'])} | {fmt(row['win_rate'], 3)} |"
        )

    lines.extend(
        [
            "",
            "## Period Summary",
            "",
            "| phase | period | n | mean10d | mean20d | win10d |",
            "| --- | --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in result.get("period_summary", []):
        lines.append(
            "| "
            + f"{row['phase']} | {row['period_bucket']} | {row['n']} | {fmt(row['mean'])} | {fmt(row['mean_ret20d'])} | {fmt(row['win_rate'], 3)} |"
        )

    lines.extend(
        [
            "",
            "## Failure Summary",
            "",
            "| failure_reason | n | mean10d | mean20d | win10d | avoid_yen_10d |",
            "| --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in result["failure_summary"]:
        lines.append(
            "| "
            + f"{row['failure_reason']} | {row['n']} | {fmt(row['mean_ret10d'])} | {fmt(row['mean_ret20d'])} | {fmt(row['win_rate10d'], 3)} | {row['avoid_yen_10d_1m']} |"
        )

    lines.extend(
        [
            "",
            "## Bottom Entry Summary",
            "",
            "| box_zone | entry_style | n | mean10d | mean20d | win10d |",
            "| --- | --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in result["bottom_entry_summary"]:
        lines.append(
            "| "
            + f"{row['box_zone']} | {row['entry_style']} | {row['n']} | {fmt(row['mean_ret10d'])} | {fmt(row['mean_ret20d'])} | {fmt(row['win_rate10d'], 3)} |"
        )

    lines.extend(["", "## Pattern Summary", ""])
    for phase, phase_summary in result["pattern_summary"].items():
        lines.extend([f"### {phase} pattern_2", "", "| pattern | n | mean10d | mean20d | win10d |", "| --- | ---: | ---: | ---: | ---: |"])
        for row in phase_summary.get("pattern_2", []):
            lines.append(
                "| "
                + f"{row['daily_pattern_2']} | {row['n']} | {fmt(row['mean_ret10d'])} | {fmt(row['mean_ret20d'])} | {fmt(row['win_rate10d'], 3)} |"
            )
        lines.extend(["", f"### {phase} shape_combo", "", "| shape_combo | n | mean10d | mean20d | win10d |", "| --- | ---: | ---: | ---: | ---: |"])
        for row in phase_summary.get("shape_combo", []):
            lines.append(
                "| "
                + f"{row['shape_combo']} | {row['n']} | {fmt(row['mean_ret10d'])} | {fmt(row['mean_ret20d'])} | {fmt(row['win_rate10d'], 3)} |"
            )
        lines.append("")

    lines.extend(["## Replay Examples", ""])
    for code, rows in result["replay_examples"].items():
        lines.extend(
            [
                f"### {code}",
                "",
                "| date | phase | box_month_index | monthly_context | weekly_context | pattern_2 | breakout_result | failure_reason | ret10d | ret20d |",
                "| --- | --- | ---: | --- | --- | --- | --- | --- | ---: | ---: |",
            ]
        )
        for row in rows:
            lines.append(
                "| "
                + f"{row['date']} | {row['phase']} | {row['box_month_index'] or 'na'} | {row['monthly_context']} | {row['weekly_context']} | "
                + f"{row['daily_pattern_2'] or 'na'} | {row['breakout_result'] or 'na'} | {row['failure_reason'] or 'na'} | "
                + f"{fmt(row['ret10d'])} | {fmt(row['ret20d'])} |"
            )
        lines.append("")
    return "\n".join(lines) + "\n"


def run_monthly_box_research(db_paths: list[Path]) -> dict[str, Any]:
    daily = _prepare_frame(db_paths)
    phase_masks = _build_phase_masks(daily)
    events = _build_event_frame(daily, phase_masks)
    cases = _build_cases_frame(events)
    return {
        "meta": {
            "db_paths": [str(path) for path in db_paths],
            "date_min": str(daily["dt"].min().date()),
            "date_max": str(daily["dt"].max().date()),
            "events_total": int(len(events)),
            "cases_total": int(len(cases)),
            "round_trip_cost": ROUND_TRIP_COST,
            "pattern_min_samples": PATTERN_MIN_SAMPLES,
            "failed_breakout_window": FAILED_BREAKOUT_WINDOW,
            "replay_codes": list(REPLAY_CODES),
        },
        "phase_summary": _build_phase_summary(events),
        "month_index_summary": _build_month_index_summary(events),
        "period_summary": _build_period_summary(events),
        "pattern_summary": _build_pattern_summary(events),
        "failure_summary": _build_failure_summary(events),
        "bottom_entry_summary": _build_bottom_entry_summary(events),
        "replay_examples": _build_replay_examples(daily, events),
        "cases_preview": cases.head(50).to_dict(orient="records"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Monthly box breakout / failed breakout research")
    parser.add_argument("--db-path", type=Path, action="append", default=None, help="stocks.duckdb path; repeatable")
    parser.add_argument("--output-json", type=Path, default=Path("tmp/monthly_box_breakout_research.json"))
    parser.add_argument("--output-md", type=Path, default=Path("tmp/monthly_box_breakout_research.md"))
    args = parser.parse_args()

    db_paths = args.db_path or _resolve_default_db_paths()
    result = run_monthly_box_research(db_paths)
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
