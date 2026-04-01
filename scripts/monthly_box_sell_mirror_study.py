from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
import sys
from typing import Any

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.monthly_box_breakout_research import (
    FAILED_BREAKOUT_WINDOW,
    PATTERN_MIN_SAMPLES,
    REPLAY_CODES,
    _bucket_box_month_index,
    _bucket_dist_ma20,
    _build_monthly_box_map,
    _classify_monthly_context,
    _compute_box_month_index,
    _daily_box_zone,
    _load_monthly_frame,
    _load_ticker_names,
    _month_int_to_period,
    _phase_start,
    _summary_from_series,
)
from scripts.monthly_box_time_window_study import TIME_LABELS, _add_time_window_features, _prepare_monthly_box_frame
from scripts.note_trade_repro_backtest import ROUND_TRIP_COST, _assign_period_bucket, _resolve_default_db_paths


SELL_PHASES = ("upper_rejection_short", "lower_breakdown_short", "lower_breakdown_strict_short", "failed_box_short_exit")
SELL_FAILURE_PRIORITY = ("climactic_exhaustion", "late_short", "weak_volume_break", "reclaim_into_box")
PATH_HORIZONS = (5, 10, 20)
PATTERN_MIN_SAMPLES_LOCAL = max(5, PATTERN_MIN_SAMPLES)
REPLAY_CODES_SELL = ("4661", "6976", "8136")

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
    "short_result",
    "failure_reason",
    "ret_5d",
    "ret_10d",
    "ret_20d",
    "mfe_20d",
    "mae_20d",
    "manual_note",
    "source_example",
]


def _load_db_paths_from_args(values: list[str]) -> list[Path]:
    if values:
        return [Path(value).expanduser().resolve() for value in values]
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


def _prepare_frame(db_paths: list[Path]) -> pd.DataFrame:
    daily = _prepare_monthly_box_frame(db_paths)
    daily = _add_time_window_features(daily)
    daily = _add_short_forward_path_metrics(daily)
    daily = daily.copy()
    daily["period_bucket"] = _assign_period_bucket(daily["dt"])
    daily["analysis_box_month_bucket"] = daily["box_month_bucket"].copy()
    daily["analysis_box_month_index"] = daily["box_month_index"].copy()
    daily["name"] = daily["name"].fillna("")
    daily["daily_bar_tag"] = daily["bar_tag"].fillna("").astype(str)
    daily["box_zone"] = daily["box_zone"].fillna("na").astype(str)
    daily["weekly_context"] = daily["weekly_context"].fillna("na").astype(str)
    daily["daily_pattern_2"] = daily["pattern_2"].fillna("").astype(str)
    daily["daily_pattern_3"] = daily["pattern_3"].fillna("").astype(str)
    recent_reclaim = (
        daily.groupby("code", sort=False)["daily_ma20_reclaim"]
        .transform(lambda s: s.shift(1).rolling(5, min_periods=1).max())
        .fillna(False)
        .astype(bool)
    )
    daily["recent_ma20_reclaim"] = recent_reclaim
    daily["lower_break_after_reclaim"] = daily["support_break_day"].fillna(False).astype(bool) & daily["recent_ma20_reclaim"]
    daily["source_example"] = np.where(
        daily["code"].eq("4661"),
        "4661_short_mirror",
        np.where(daily["code"].eq("6976"), "6976_short_mirror", np.where(daily["code"].eq("8136"), "8136_short_mirror", "")),
    )
    return daily


def _bearish_candle_mask(daily: pd.DataFrame) -> pd.Series:
    bar_tag = daily["bar_tag"].fillna("")
    body_ratio = np.where(daily["range"].gt(0.0), daily["body"] / daily["range"], np.nan)
    return (
        bar_tag.str.contains("LB", regex=False)
        | bar_tag.str.contains("WU", regex=False)
        | ((daily["c"] < daily["o"]) & daily["range"].gt(0.0) & pd.Series(body_ratio, index=daily.index).le(0.55))
    )


def _build_phase_masks(daily: pd.DataFrame) -> dict[str, pd.Series]:
    bearish_candle = _bearish_candle_mask(daily)
    upper_rejection_raw = (
        daily["box_active"]
        & daily["box_zone"].isin(["upper", "breakout"])
        & daily["week_slope"].isin(["flat", "down"])
        & (~daily["week_climactic"])
        & daily["box_upper"].notna()
        & (daily["h"] >= daily["box_upper"] * 1.005)
        & (daily["c"] <= daily["box_upper"] * 0.995)
        & bearish_candle
        & (daily["dist_bucket"] != "overheat")
    )
    lower_break_support = (
        daily["box_active"]
        & daily["box_zone"].isin(["lower", "mid"])
        & daily["week_slope"].isin(["flat", "down"])
        & (~daily["week_climactic"])
        & daily["support_break_day"].fillna(False).astype(bool)
        & bearish_candle
        & (daily["dist_bucket"] != "overheat")
    )
    lower_break_reclaim = (
        daily["box_active"]
        & daily["box_zone"].isin(["lower", "mid"])
        & daily["week_slope"].isin(["flat", "down"])
        & (~daily["week_climactic"])
        & daily["lower_break_after_reclaim"].fillna(False).astype(bool)
        & bearish_candle
        & (daily["dist_bucket"] != "overheat")
    )
    lower_breakdown_raw = lower_break_support | lower_break_reclaim
    lower_breakdown_strict_raw = (
        lower_break_support
        & daily["analysis_box_month_bucket"].eq("4-5")
        & daily["timing_label"].eq("day9_window")
        & daily["box_zone"].eq("mid")
    )
    return {
        "upper_rejection_short": _phase_start(upper_rejection_raw, daily["code"]),
        "lower_breakdown_short": _phase_start(lower_breakdown_raw, daily["code"]),
        "lower_breakdown_strict_short": _phase_start(lower_breakdown_strict_raw, daily["code"]),
    }


def _failed_short_position(group: pd.DataFrame, short_pos: int, phase: str, window: int = FAILED_BREAKOUT_WINDOW) -> int | None:
    row = group.iloc[short_pos]
    ref = float(row["box_upper"]) if phase == "upper_rejection_short" else float(row["box_lower"])
    if not np.isfinite(ref) or ref <= 0.0:
        return None
    end = min(len(group) - 1, short_pos + int(window))
    for pos in range(short_pos + 1, end + 1):
        current = group.iloc[pos]
        reclaim = bool(np.isfinite(current["c"]) and current["c"] > ref * 1.01)
        if reclaim:
            return pos
    return None


def _classify_short_failure_reason(entry_row: dict[str, Any] | pd.Series, fail_row: dict[str, Any] | pd.Series) -> str:
    entry = dict(entry_row)
    fail = dict(fail_row)
    ref = float(entry.get("box_upper") or entry.get("box_lower") or 0.0)
    fail_close = float(fail.get("c") or 0.0)
    climactic = bool(entry.get("climactic_day")) or bool(entry.get("week_climactic"))
    late = bool((entry.get("box_month_index") or 0) >= 9)
    weak_volume = str(entry.get("vol_bucket") or "na") in {"dry", "mid"}
    reclaim = ref > 0.0 and fail_close > ref * 1.01
    checks = {
        "climactic_exhaustion": climactic,
        "late_short": late,
        "weak_volume_break": weak_volume,
        "reclaim_into_box": reclaim,
    }
    for label in SELL_FAILURE_PRIORITY:
        if checks.get(label):
            return label
    return "reclaim_into_box"


def _classify_short_entry_style(row: pd.Series) -> str:
    if row["phase"] == "upper_rejection_short":
        if bool(row.get("climactic_day")):
            return "upper_reject_climactic"
        bar_tag = str(row.get("bar_tag") or "")
        if "LB" in bar_tag:
            return "upper_reject_lb"
        if "WU" in bar_tag:
            return "upper_reject_wu"
        if "GD" in bar_tag:
            return "upper_reject_gap_down"
        return "upper_reject_other"
    if row["phase"].startswith("lower_breakdown"):
        if row["phase"] == "lower_breakdown_strict_short":
            if bool(row.get("lower_break_after_reclaim")):
                return "lower_breakdown_strict_reclaim"
            if bool(row.get("support_break_day")):
                return "lower_breakdown_strict_support"
        if bool(row.get("lower_break_after_reclaim")):
            return "lower_breakdown_reclaim"
        if bool(row.get("support_break_day")):
            return "lower_breakdown_support"
        bar_tag = str(row.get("bar_tag") or "")
        if "GD" in bar_tag:
            return "lower_breakdown_gap_down"
        if "LB" in bar_tag:
            return "lower_breakdown_lb"
        return "lower_breakdown_other"
    return "failed_short_exit"


def _build_failed_short_events(daily: pd.DataFrame, phase_masks: dict[str, pd.Series]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for phase in ("upper_rejection_short", "lower_breakdown_short", "lower_breakdown_strict_short"):
        mask = phase_masks[phase]
        for _, group in daily.groupby("code", sort=False):
            local_mask = mask.loc[group.index].to_numpy(dtype=bool, copy=False)
            positions = np.flatnonzero(local_mask)
            used_positions: set[int] = set()
            for pos in positions:
                fail_pos = _failed_short_position(group, int(pos), phase)
                if fail_pos is None or fail_pos in used_positions:
                    continue
                used_positions.add(int(fail_pos))
                entry_row = group.iloc[int(pos)]
                fail_row = group.iloc[int(fail_pos)].copy()
                fail_row["phase"] = "failed_box_short_exit"
                fail_row["short_origin_date"] = pd.Timestamp(entry_row["dt"]).date().isoformat()
                fail_row["short_origin_phase"] = phase
                fail_row["short_result"] = "failed_short"
                fail_row["failure_reason"] = _classify_short_failure_reason(entry_row, fail_row)
                fail_row["origin_box_level"] = entry_row["box_upper"] if phase == "upper_rejection_short" else entry_row["box_lower"]
                rows.append(fail_row.to_dict())
    if not rows:
        return daily.iloc[0:0].copy()
    return pd.DataFrame(rows)


def _build_event_frame(daily: pd.DataFrame, phase_masks: dict[str, pd.Series]) -> pd.DataFrame:
    upper_events = daily.loc[phase_masks["upper_rejection_short"] & daily["ret_short_10d"].notna()].copy()
    upper_events["phase"] = "upper_rejection_short"
    upper_events["short_result"] = np.where(
        upper_events["hit_dn5_before_up5_20d"] & (upper_events["ret_short_20d"] > 0.0),
        "successful_short",
        "stalled_short",
    )
    upper_events["failure_reason"] = None

    lower_events = daily.loc[phase_masks["lower_breakdown_short"] & daily["ret_short_10d"].notna()].copy()
    lower_events["phase"] = "lower_breakdown_short"
    lower_events["short_result"] = np.where(
        lower_events["hit_dn5_before_up5_20d"] & (lower_events["ret_short_20d"] > 0.0),
        "successful_short",
        "stalled_short",
    )
    lower_events["failure_reason"] = None

    strict_events = daily.loc[phase_masks["lower_breakdown_strict_short"] & daily["ret_short_10d"].notna()].copy()
    strict_events["phase"] = "lower_breakdown_strict_short"
    strict_events["short_result"] = np.where(
        strict_events["hit_dn5_before_up5_20d"] & (strict_events["ret_short_20d"] > 0.0),
        "successful_short",
        "stalled_short",
    )
    strict_events["failure_reason"] = None

    failure_events = _build_failed_short_events(daily, phase_masks)
    failure_lookup: dict[tuple[str, str, str], str] = {}
    if not failure_events.empty:
        for _, row in failure_events.iterrows():
            origin_date = str(row["short_origin_date"])
            origin_phase = str(row["short_origin_phase"])
            failure_lookup[(str(row["code"]), origin_date, origin_phase)] = str(row["failure_reason"])

    for frame in (upper_events, lower_events, strict_events):
        frame["failure_reason"] = [
            failure_lookup.get((str(code), pd.Timestamp(dt).date().isoformat(), str(phase)))
            for code, dt, phase in zip(frame["code"], frame["dt"], frame["phase"])
        ]
        frame["short_result"] = [
            "failed_short"
            if (str(code), pd.Timestamp(dt).date().isoformat(), str(phase)) in failure_lookup
            else result
            for code, dt, phase, result in zip(frame["code"], frame["dt"], frame["phase"], frame["short_result"])
        ]

    if not failure_events.empty:
        failure_events["short_result"] = "failed_short"

    events = pd.concat([upper_events, lower_events, strict_events, failure_events], ignore_index=True)
    if events.empty:
        return events
    events["entry_style"] = events.apply(_classify_short_entry_style, axis=1)
    events["daily_pattern_2"] = events["pattern_2"]
    events["daily_pattern_3"] = events["pattern_3"]
    events["signal_date"] = events["dt"].dt.date.astype(str)
    events["manual_note"] = ""
    return events


def _phase_summary_row(phase: str, frame: pd.DataFrame) -> dict[str, Any]:
    summary5 = _summary_from_series(frame["ret_short_5d"])
    summary10 = _summary_from_series(frame["ret_short_10d"])
    summary20 = _summary_from_series(frame["ret_short_20d"])
    row = {
        "phase": phase,
        **summary10,
        "mean_ret5d": summary5["mean"],
        "mean_ret20d": summary20["mean"],
        "median_ret20d": summary20["median"],
        "mfe20d": float(frame["mfe_short_20d"].mean()) if not frame.empty else None,
        "mae20d": float(frame["mae_short_20d"].mean()) if not frame.empty else None,
        "expected_yen_5d_1m": round((summary5["mean"] or 0.0) * 1_000_000),
        "expected_yen_10d_1m": round((summary10["mean"] or 0.0) * 1_000_000),
        "expected_yen_20d_1m": round((summary20["mean"] or 0.0) * 1_000_000),
    }
    if phase == "failed_box_short_exit":
        row["avoid_yen_10d_1m"] = round(-(summary10["mean"] or 0.0) * 1_000_000)
        row["avoid_yen_20d_1m"] = round(-(summary20["mean"] or 0.0) * 1_000_000)
    return row


def _build_phase_summary(events: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for phase in SELL_PHASES:
        frame = events.loc[events["phase"].eq(phase)]
        if frame.empty:
            continue
        rows.append(_phase_summary_row(phase, frame))
    return rows


def _group_phase_stats(
    events: pd.DataFrame,
    group_cols: list[str],
    ret_col5: str = "ret_short_5d",
    ret_col10: str = "ret_short_10d",
    ret_col20: str = "ret_short_20d",
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    work = events.loc[events[ret_col10].notna()].copy()
    if work.empty:
        return rows
    for keys, group in work.groupby(group_cols, dropna=False):
        summary5 = _summary_from_series(group[ret_col5])
        summary10 = _summary_from_series(group[ret_col10])
        summary20 = _summary_from_series(group[ret_col20])
        if int(summary10["n"]) < PATTERN_MIN_SAMPLES_LOCAL:
            continue
        row: dict[str, Any] = {}
        if len(group_cols) == 1:
            label = keys
            if isinstance(label, tuple) and len(label) == 1:
                label = label[0]
            row[group_cols[0]] = label
        else:
            for idx, col in enumerate(group_cols):
                row[col] = keys[idx]
        row["n"] = int(summary10["n"])
        row["mean_ret5d"] = summary5["mean"]
        row["mean_ret10d"] = summary10["mean"]
        row["mean_ret20d"] = summary20["mean"]
        row["win_rate10d"] = summary10["win_rate"]
        row["profit_factor10d"] = summary10["profit_factor"]
        row["mfe20d"] = float(group["mfe_short_20d"].mean())
        row["mae20d"] = float(group["mae_short_20d"].mean())
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


def _build_timing_summary(events: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for phase in SELL_PHASES:
        frame = events.loc[events["phase"].eq(phase)].copy()
        if frame.empty:
            continue
        rows.append(_phase_summary_row(phase, frame) | {"label": "all"})
        rows.append(_phase_summary_row(phase, frame.loc[frame["timing_gate"]]) | {"label": "timing_gate"})
        rows.append(_phase_summary_row(phase, frame.loc[~frame["timing_gate"]]) | {"label": "other"})
        for label in TIME_LABELS:
            if label == "other":
                continue
            sub = frame.loc[frame["timing_label"].eq(label)]
            if sub.empty or len(sub) < PATTERN_MIN_SAMPLES_LOCAL:
                continue
            rows.append(_phase_summary_row(phase, sub) | {"label": label})
    rows.sort(key=lambda item: (item["phase"], item["label"]))
    return rows


def _build_timing_period_summary(events: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for (phase, period_bucket, timing_label), group in events.groupby(["phase", "period_bucket", "timing_label"], dropna=False):
        summary = _phase_summary_row(str(phase), group)
        if int(summary["n"]) < PATTERN_MIN_SAMPLES_LOCAL:
            continue
        rows.append(
            {
                "phase": str(phase),
                "period_bucket": str(period_bucket),
                "timing_label": str(timing_label),
                "n": int(summary["n"]),
                "mean_ret5d": summary["mean_ret5d"],
                "mean_ret10d": summary["mean"],
                "mean_ret20d": summary["mean_ret20d"],
                "win_rate10d": summary["win_rate"],
                "profit_factor10d": summary["profit_factor"],
            }
        )
    rows.sort(key=lambda item: (item["phase"], item["period_bucket"], item["timing_label"]))
    return rows


def _build_box_time_combo_summary(events: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    frame = events.loc[events["timing_gate"]].copy()
    if frame.empty:
        return rows
    for (phase, box_bucket, timing_label), group in frame.groupby(["phase", "analysis_box_month_bucket", "timing_label"], dropna=False):
        summary = _phase_summary_row(str(phase), group)
        if int(summary["n"]) < PATTERN_MIN_SAMPLES_LOCAL:
            continue
        rows.append(
            {
                "phase": str(phase),
                "box_month_bucket": str(box_bucket),
                "timing_label": str(timing_label),
                "n": int(summary["n"]),
                "mean_ret5d": summary["mean_ret5d"],
                "mean_ret10d": summary["mean"],
                "mean_ret20d": summary["mean_ret20d"],
                "win_rate10d": summary["win_rate"],
                "profit_factor10d": summary["profit_factor"],
                "mfe20d": summary["mfe20d"],
                "mae20d": summary["mae20d"],
            }
        )
    rows.sort(key=lambda item: ((item.get("mean_ret20d") or -999.0), item.get("n") or 0), reverse=True)
    return rows[:40]


def _build_failure_summary(events: pd.DataFrame) -> list[dict[str, Any]]:
    frame = events.loc[events["phase"].eq("failed_box_short_exit") & events["failure_reason"].notna()].copy()
    if frame.empty:
        return []
    rows: list[dict[str, Any]] = []
    for reason, group in frame.groupby("failure_reason", dropna=False):
        summary10 = _summary_from_series(group["ret_short_10d"])
        summary20 = _summary_from_series(group["ret_short_20d"])
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


def _build_entry_summary(events: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for phase in ("upper_rejection_short", "lower_breakdown_short", "lower_breakdown_strict_short"):
        frame = events.loc[events["phase"].eq(phase)].copy()
        if frame.empty:
            continue
        rows.extend(_group_phase_stats(frame, ["box_zone", "entry_style"]))
    rows.sort(key=lambda item: ((item.get("mean_ret10d") or -999.0), item.get("n") or 0), reverse=True)
    return rows[:20]


def _build_pattern_effect_summary(events: pd.DataFrame) -> dict[str, list[dict[str, Any]]]:
    summary: dict[str, list[dict[str, Any]]] = {}
    for phase in SELL_PHASES:
        frame = events.loc[events["phase"].eq(phase)].copy()
        if frame.empty:
            summary[phase] = []
            continue
        frame["shape_combo"] = (
            frame["weekly_context"].fillna("na")
            + "|"
            + frame["analysis_box_month_bucket"].fillna("na")
            + "|"
            + frame["timing_label"].fillna("na")
            + "|"
            + frame["vol_bucket"].fillna("na")
            + "|"
            + frame["atr_bucket"].fillna("na")
            + "|"
            + frame["dist_bucket"].fillna("na")
        )
        rows: list[dict[str, Any]] = []
        for kind, cols in (
            ("entry_style", ["entry_style"]),
            ("daily_bar_tag", ["daily_bar_tag"]),
            ("daily_pattern_2", ["daily_pattern_2"]),
            ("daily_pattern_3", ["daily_pattern_3"]),
            ("shape_combo", ["shape_combo"]),
        ):
            for row in _group_phase_stats(frame, cols):
                label = row.get(cols[0])
                if not label:
                    continue
                rows.append(
                    {
                        "phase": phase,
                        "kind": kind,
                        "pattern": str(label),
                        "n": int(row["n"]),
                        "mean_ret10d": row["mean_ret10d"],
                        "mean_ret20d": row["mean_ret20d"],
                        "win_rate10d": row["win_rate10d"],
                        "profit_factor10d": row["profit_factor10d"],
                        "mfe20d": row["mfe20d"],
                        "mae20d": row["mae20d"],
                    }
                )
        rows.sort(key=lambda item: ((item.get("mean_ret10d") or -999.0), item.get("n") or 0), reverse=True)
        summary[phase] = rows[:10]
    return summary


def _build_pattern_summary(events: pd.DataFrame) -> dict[str, dict[str, list[dict[str, Any]]]]:
    summary: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for phase in SELL_PHASES:
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


def _attach_origin_box_context(events: pd.DataFrame) -> pd.DataFrame:
    events = events.copy()
    events["analysis_box_month_bucket"] = events["box_month_bucket"].copy()
    events["analysis_box_month_index"] = events["box_month_index"].copy()
    if events.empty:
        return events
    origin_lookup = (
        events.loc[
            events["phase"].isin(["upper_rejection_short", "lower_breakdown_short", "lower_breakdown_strict_short"]),
            ["code", "signal_date", "phase", "box_month_bucket", "box_month_index"],
        ]
        .dropna(subset=["box_month_bucket", "box_month_index"], how="all")
        .drop_duplicates(["code", "signal_date", "phase"], keep="last")
        .set_index(["code", "signal_date", "phase"])
    )
    fail_mask = events["phase"].eq("failed_box_short_exit") & events["short_origin_date"].notna() & events["short_origin_phase"].notna()
    for idx, row in events.loc[fail_mask, ["code", "short_origin_date", "short_origin_phase"]].iterrows():
        key = (str(row["code"]), str(row["short_origin_date"]), str(row["short_origin_phase"]))
        if key not in origin_lookup.index:
            continue
        origin = origin_lookup.loc[key]
        if isinstance(origin, pd.DataFrame):
            origin = origin.iloc[-1]
        if pd.isna(events.at[idx, "analysis_box_month_bucket"]) and pd.notna(origin.get("box_month_bucket")):
            events.at[idx, "analysis_box_month_bucket"] = origin.get("box_month_bucket")
        if pd.isna(events.at[idx, "analysis_box_month_index"]) and pd.notna(origin.get("box_month_index")):
            events.at[idx, "analysis_box_month_index"] = origin.get("box_month_index")
    return events


def _build_replay_examples(daily: pd.DataFrame, events: pd.DataFrame) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {}
    for code in REPLAY_CODES_SELL:
        frame = events.loc[events["code"].eq(code)].sort_values("dt").tail(12).copy()
        if frame.empty:
            frame = daily.loc[daily["code"].eq(code) & daily["box_active"]].sort_values("dt").tail(6).copy()
            if not frame.empty:
                frame["phase"] = "watch"
                frame["entry_style"] = "watch"
                frame["short_result"] = None
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
                    "short_result": str(row["short_result"]) if pd.notna(row["short_result"]) else None,
                    "failure_reason": str(row["failure_reason"]) if pd.notna(row["failure_reason"]) else None,
                    "ret10d": float(row["ret_short_10d"]) if pd.notna(row["ret_short_10d"]) else None,
                    "ret20d": float(row["ret_short_20d"]) if pd.notna(row["ret_short_20d"]) else None,
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
            "short_result": events["short_result"].fillna("").astype(str),
            "failure_reason": events["failure_reason"].fillna("").astype(str),
            "ret_5d": events["ret_short_5d"],
            "ret_10d": events["ret_short_10d"],
            "ret_20d": events["ret_short_20d"],
            "mfe_20d": events["mfe_short_20d"],
            "mae_20d": events["mae_short_20d"],
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
        "# 月足ボックス 売りミラー",
        "",
        "## Setup",
        "",
        f"- DBs: `{', '.join(result['meta']['db_paths'])}`",
        f"- Date range: `{result['meta']['date_min']}` to `{result['meta']['date_max']}`",
        f"- Events total: `{result['meta']['events_total']}`",
        f"- Round-trip cost: `{result['meta']['round_trip_cost']:.3f}`",
        "- Note: positive mean values indicate profitable short performance.",
        "",
        "## Pattern Effect Summary",
        "",
        "| phase | kind | pattern | n | mean10d | mean20d | win10d | pf10d | mfe20d | mae20d |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for phase, rows in result.get("pattern_effect_summary", {}).items():
        for row in rows:
            lines.append(
                "| "
                + f"{phase} | {row['kind']} | {row['pattern']} | {row['n']} | {fmt(row['mean_ret10d'])} | {fmt(row['mean_ret20d'])} | "
                + f"{fmt(row['win_rate10d'], 3)} | {fmt(row['profit_factor10d'])} | {fmt(row['mfe20d'])} | {fmt(row['mae20d'])} |"
            )

    lines.extend(
        [
            "## Phase Summary",
            "",
            "| phase | n | mean5d | mean10d | mean20d | win10d | mfe20d | mae20d | 1m_yen_10d | 1m_yen_20d |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in result["phase_summary"]:
        lines.append(
            "| "
            + f"{row['phase']} | {row['n']} | {fmt(row['mean_ret5d'])} | {fmt(row['mean'])} | {fmt(row['mean_ret20d'])} | {fmt(row['win_rate'], 3)} | "
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
            "## Timing Summary",
            "",
            "| phase | label | n | mean5d | mean10d | mean20d | win10d |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in result["timing_summary"]:
        lines.append(
            "| "
            + f"{row['phase']} | {row['label']} | {row['n']} | {fmt(row['mean_ret5d'])} | {fmt(row['mean'])} | {fmt(row['mean_ret20d'])} | {fmt(row['win_rate'], 3)} |"
        )

    lines.extend(
        [
            "",
            "## Timing Period Summary",
            "",
            "| phase | period | timing | n | mean10d | mean20d | win10d | pf10d |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in result.get("timing_period_summary", []):
        lines.append(
            "| "
            + f"{row['phase']} | {row['period_bucket']} | {row['timing_label']} | {row['n']} | {fmt(row['mean_ret10d'])} | {fmt(row['mean_ret20d'])} | {fmt(row['win_rate10d'], 3)} | {fmt(row['profit_factor10d'])} |"
        )

    lines.extend(
        [
            "",
            "## Box Time Combo Summary",
            "",
            "| phase | box_bucket | timing | n | mean10d | mean20d | win10d | pf10d |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in result["box_time_combo_summary"]:
        lines.append(
            "| "
            + f"{row['phase']} | {row['box_month_bucket']} | {row['timing_label']} | {row['n']} | {fmt(row['mean_ret10d'])} | {fmt(row['mean_ret20d'])} | {fmt(row['win_rate10d'], 3)} | {fmt(row['profit_factor10d'])} |"
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
            "## Entry Summary",
            "",
            "| box_zone | entry_style | n | mean5d | mean10d | mean20d | win10d |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in result["entry_summary"]:
        lines.append(
            "| "
            + f"{row['box_zone']} | {row['entry_style']} | {row['n']} | {fmt(row['mean_ret5d'])} | {fmt(row['mean_ret10d'])} | {fmt(row['mean_ret20d'])} | {fmt(row['win_rate10d'], 3)} |"
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
                "| date | phase | box_month_index | monthly_context | weekly_context | pattern_2 | short_result | failure_reason | ret10d | ret20d |",
                "| --- | --- | ---: | --- | --- | --- | --- | --- | ---: | ---: |",
            ]
        )
        for row in rows:
            lines.append(
                "| "
                + f"{row['date']} | {row['phase']} | {row['box_month_index'] or 'na'} | {row['monthly_context']} | {row['weekly_context']} | "
                + f"{row['daily_pattern_2'] or 'na'} | {row['short_result'] or 'na'} | {row['failure_reason'] or 'na'} | "
                + f"{fmt(row['ret10d'])} | {fmt(row['ret20d'])} |"
        )
        lines.append("")
    return "\n".join(lines) + "\n"


def run_monthly_box_sell_mirror_study(db_paths: list[Path]) -> dict[str, Any]:
    daily = _prepare_frame(db_paths)
    phase_masks = _build_phase_masks(daily)
    events = _build_event_frame(daily, phase_masks)
    events = _attach_origin_box_context(events)
    cases = _build_cases_frame(events)
    return {
        "meta": {
            "db_paths": [str(path) for path in db_paths],
            "date_min": str(daily["dt"].min().date()),
            "date_max": str(daily["dt"].max().date()),
            "events_total": int(len(events)),
            "cases_total": int(len(cases)),
            "round_trip_cost": ROUND_TRIP_COST,
            "timing_labels": list(TIME_LABELS),
            "sell_phases": list(SELL_PHASES),
            "pattern_min_samples": PATTERN_MIN_SAMPLES_LOCAL,
            "failed_breakout_window": FAILED_BREAKOUT_WINDOW,
            "replay_codes": list(REPLAY_CODES_SELL),
        },
        "phase_summary": _build_phase_summary(events),
        "month_index_summary": _build_month_index_summary(events),
        "period_summary": _build_period_summary(events),
        "timing_summary": _build_timing_summary(events),
        "timing_period_summary": _build_timing_period_summary(events),
        "box_time_combo_summary": _build_box_time_combo_summary(events),
        "failure_summary": _build_failure_summary(events),
        "entry_summary": _build_entry_summary(events),
        "pattern_effect_summary": _build_pattern_effect_summary(events),
        "pattern_summary": _build_pattern_summary(events),
        "replay_examples": _build_replay_examples(daily, events),
        "cases": cases.to_dict(orient="records"),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Sell-side mirror study for monthly box timing / breakout logic")
    parser.add_argument("--db-path", action="append", default=[])
    parser.add_argument("--output-json", type=Path, default=Path("tmp/monthly_box_sell_mirror_study.json"))
    parser.add_argument("--output-md", type=Path, default=Path("tmp/monthly_box_sell_mirror_study.md"))
    args = parser.parse_args()

    db_paths = _load_db_paths_from_args(args.db_path)
    result = run_monthly_box_sell_mirror_study(db_paths)
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    args.output_md.parent.mkdir(parents=True, exist_ok=True)
    args.output_md.write_text(_build_report(result), encoding="utf-8")


if __name__ == "__main__":
    main()
