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

from scripts.month_end_shape_study import classify_5541_premise_bucket
from scripts.note_trade_repro_backtest import (
    ROUND_TRIP_COST,
    _add_daily_coordinates,
    _add_forward_path_metrics,
    _add_pattern_columns,
    _assign_period_bucket,
    _build_monthly_premise_map,
    _build_weekly_context_map,
    _load_daily_frame,
    _resolve_default_db_paths,
)


FOCUS_CODE = "5541"
CAPITAL_BASE_YEN = 1_000_000
PATTERN_MIN_SAMPLES = 3
PHASES = ("entry", "add", "takeprofit")


def _summary_from_series(values: pd.Series) -> dict[str, Any]:
    arr = values.dropna().to_numpy(dtype=np.float64, copy=False)
    if arr.size == 0:
        return {
            "n": 0,
            "mean": None,
            "median": None,
            "win_rate": None,
            "p10": None,
            "profit_factor": None,
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
        "p10": float(np.quantile(arr, 0.10)),
        "profit_factor": profit_factor,
    }


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


def _prepare_frame(db_paths: list[Path]) -> pd.DataFrame:
    daily = _load_daily_frame(db_paths)
    premise_map = _build_monthly_premise_map(daily)
    weekly_map = _build_weekly_context_map(daily)
    daily = daily.merge(premise_map, how="left", left_on=["code", "month"], right_on=["code", "apply_month"])
    daily = daily.merge(weekly_map, how="left", on=["code", "week_end"])
    daily["premise_bucket"] = daily["premise_bucket"].fillna("other")
    daily["box_zone"] = daily["box_zone"].fillna("na")
    daily["week_slope"] = daily["week_slope"].fillna("na")
    daily["week_support_hold"] = daily["week_support_hold"].fillna(False).astype(bool)
    daily["week_climactic"] = daily["week_climactic"].fillna(False).astype(bool)
    daily["period_bucket"] = _assign_period_bucket(daily["dt"])
    daily = _add_daily_coordinates(daily)

    monthly_premise_bucket = daily["premise_bucket"].copy()
    box_state = np.where(
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
    )
    trend_bucket = np.where(
        daily["week_slope"].eq("up") & daily["c"].ge(daily["ma60"].fillna(np.inf)),
        "up",
        np.where(daily["c"].ge(daily["ma60"].fillna(np.inf)), "mixed", "down"),
    )
    cnt60_up = np.where(
        daily["c"].ge(daily["ma60"].fillna(np.inf)),
        30 + daily["cnt7_down"].fillna(0),
        daily["cnt7_down"].fillna(0),
    )
    dist_bucket = np.where(
        daily["dist_ma20"].isna(),
        "na",
        np.where(
            daily["dist_ma20"] < 0.0,
            "below",
            np.where(daily["dist_ma20"] < 0.05, "near", np.where(daily["dist_ma20"] < 0.12, "extended", "overheat")),
        ),
    )
    derived = classify_5541_premise_bucket(
        pd.DataFrame(
            {
                "box_months": daily["monthly_box_months"].fillna(0),
                "box_state": box_state,
                "trend_bucket": trend_bucket,
                "cnt60_up": cnt60_up,
                "dist_bucket": dist_bucket,
                "box_wild": False,
            },
            index=daily.index,
        )
    )
    daily["premise_bucket"] = np.where(derived.eq("other"), monthly_premise_bucket, derived)
    daily["recent_breakout_20d"] = (
        daily.groupby("code", sort=False)["breakout_day"]
        .transform(lambda s: s.shift(1).rolling(20, min_periods=1).max())
        .fillna(0.0)
        .astype(bool)
    )
    daily["ret_prev10"] = daily.groupby("code", sort=False)["c"].pct_change(10)
    daily["ret_prev20"] = daily.groupby("code", sort=False)["c"].pct_change(20)
    daily = _add_pattern_columns(daily)
    daily = _add_forward_path_metrics(daily)
    return daily


def _build_phase_masks(daily: pd.DataFrame) -> dict[str, pd.Series]:
    entry_raw = (
        daily["premise_bucket"].isin(["5541_long_base_breakout", "base_breakout_watch"])
        & daily["week_slope"].isin(["up", "flat"])
        & daily["box_zone"].eq("upper")
        & daily["c"].ge(daily["ma60"].fillna(np.inf))
        & daily["dist_ma20"].fillna(0.0).between(-0.03, 0.06)
        & (~daily["climactic_day"])
    )
    add_raw = (
        daily["premise_bucket"].eq("5541_long_base_breakout")
        & daily["week_slope"].eq("up")
        & daily["week_support_hold"]
        & (~daily["week_climactic"])
        & daily["recent_breakout_20d"]
        & (daily["touch_ma20"] | daily["cnt7_down"].between(1, 4))
        & (~daily["climactic_day"])
    )
    takeprofit_raw = (
        daily["premise_bucket"].eq("5541_long_base_breakout")
        & daily["ret_prev20"].ge(0.12)
        & daily["cnt7_down"].ge(1)
    )
    return {
        "entry": _phase_start(entry_raw, daily["code"]),
        "add": _phase_start(add_raw, daily["code"]),
        "takeprofit": _phase_start(takeprofit_raw, daily["code"]),
    }


def _build_phase_events(daily: pd.DataFrame, phase_masks: dict[str, pd.Series]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for phase, mask in phase_masks.items():
        subset = daily.loc[mask & daily["ret_long_10d"].notna()].copy()
        if subset.empty:
            continue
        subset["phase"] = phase
        frames.append(subset)
    if not frames:
        return daily.iloc[0:0].copy()
    return pd.concat(frames, ignore_index=True)


def _pattern_summary(frame: pd.DataFrame, phase: str, pattern_col: str, ascending: bool) -> list[dict[str, Any]]:
    work = frame.loc[frame["phase"].eq(phase) & frame[pattern_col].notna()].copy()
    if work.empty:
        return []
    grouped = (
        work.groupby(pattern_col, as_index=False)
        .agg(
            n=("ret_long_10d", "size"),
            mean_ret10d=("ret_long_10d", "mean"),
            mean_ret20d=("ret_long_20d", "mean"),
            win_rate10d=("ret_long_10d", lambda s: float(np.mean(s > 0.0))),
            dn5_before_up5_20d=("hit_dn5_before_up5_20d", "mean"),
        )
    )
    grouped = grouped.loc[grouped["n"] >= PATTERN_MIN_SAMPLES]
    if grouped.empty:
        return []
    sort_cols = ["mean_ret10d", "n"]
    grouped = grouped.sort_values(sort_cols, ascending=[ascending, False]).head(10)
    return grouped.to_dict(orient="records")


def _phase_summary_row(phase: str, frame: pd.DataFrame) -> dict[str, Any]:
    summary10 = _summary_from_series(frame["ret_long_10d"])
    summary20 = _summary_from_series(frame["ret_long_20d"])
    row = {
        "phase": phase,
        **summary10,
        "mean_ret20d": summary20["mean"],
        "median_ret20d": summary20["median"],
        "mfe20d": float(frame["mfe_20d"].mean()),
        "mae20d": float(frame["mae_20d"].mean()),
        "up5_before_dn5_20d": float(frame["hit_up5_before_dn5_20d"].mean()),
        "dn5_before_up5_20d": float(frame["hit_dn5_before_up5_20d"].mean()),
        "expected_yen_10d_1m": round((summary10["mean"] or 0.0) * CAPITAL_BASE_YEN),
        "expected_yen_20d_1m": round((summary20["mean"] or 0.0) * CAPITAL_BASE_YEN),
    }
    if phase == "takeprofit":
        row["avoid_yen_10d_1m"] = round(-(summary10["mean"] or 0.0) * CAPITAL_BASE_YEN)
        row["avoid_yen_20d_1m"] = round(-(summary20["mean"] or 0.0) * CAPITAL_BASE_YEN)
    return row


def _build_phase_summary(events: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for phase in PHASES:
        frame = events.loc[events["phase"].eq(phase)]
        if frame.empty:
            continue
        rows.append(_phase_summary_row(phase, frame))
    return rows


def _build_period_summary(events: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for (phase, period_bucket), frame in events.groupby(["phase", "period_bucket"], dropna=False):
        row = _phase_summary_row(str(phase), frame)
        row["period_bucket"] = str(period_bucket)
        rows.append(row)
    rows.sort(key=lambda item: (item["phase"], item.get("period_bucket", "")))
    return rows


def _build_replay(events: pd.DataFrame, code: str) -> list[dict[str, Any]]:
    replay = events.loc[events["code"].eq(str(code))].sort_values(["dt", "phase"])
    rows: list[dict[str, Any]] = []
    for _, row in replay.iterrows():
        rows.append(
            {
                "date": pd.Timestamp(row["dt"]).date().isoformat(),
                "phase": str(row["phase"]),
                "premise_bucket": str(row["premise_bucket"]),
                "pattern_2": str(row["pattern_2"]) if pd.notna(row["pattern_2"]) else None,
                "ret10d": float(row["ret_long_10d"]),
                "ret20d": float(row["ret_long_20d"]),
                "mfe20d": float(row["mfe_20d"]),
                "mae20d": float(row["mae_20d"]),
            }
        )
    return rows


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
        "# 5541型 Phase Study",
        "",
        "## Setup",
        "",
        f"- DBs: `{', '.join(result['meta']['db_paths'])}`",
        f"- Focus code: `{result['meta']['focus_code']}`",
        f"- Date range: `{result['meta']['date_min']}` to `{result['meta']['date_max']}`",
        f"- Capital base: `{result['meta']['capital_base_yen']}`",
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
            "## Period Summary",
            "",
            "| phase | period | n | mean10d | mean20d | win10d |",
            "| --- | --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in result["period_summary"]:
        lines.append(
            "| "
            + f"{row['phase']} | {row['period_bucket']} | {row['n']} | {fmt(row['mean'])} | {fmt(row['mean_ret20d'])} | {fmt(row['win_rate'], 3)} |"
        )

    lines.extend(
        [
            "",
            "## Similar Patterns",
            "",
        ]
    )
    for phase in PHASES:
        lines.extend(
            [
                f"### {phase} pattern_2",
                "",
                "| pattern | n | mean10d | mean20d | win10d | dn5_before_up5 |",
                "| --- | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for row in result["pattern_summary"].get(phase, []):
            lines.append(
                "| "
                + f"{row['pattern_2']} | {row['n']} | {fmt(row['mean_ret10d'])} | {fmt(row['mean_ret20d'])} | "
                + f"{fmt(row['win_rate10d'], 3)} | {fmt(row['dn5_before_up5_20d'], 3)} |"
            )
        lines.append("")

    lines.extend(
        [
            "## 5541 Replay",
            "",
            "| date | phase | premise_bucket | pattern_2 | ret10d | ret20d | mfe20d | mae20d |",
            "| --- | --- | --- | --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in result["replay"]:
        lines.append(
            "| "
            + f"{row['date']} | {row['phase']} | {row['premise_bucket']} | {row['pattern_2'] or 'na'} | "
            + f"{fmt(row['ret10d'])} | {fmt(row['ret20d'])} | {fmt(row['mfe20d'])} | {fmt(row['mae20d'])} |"
        )
    return "\n".join(lines) + "\n"


def run_phase_study(db_paths: list[Path]) -> dict[str, Any]:
    daily = _prepare_frame(db_paths)
    phase_masks = _build_phase_masks(daily)
    events = _build_phase_events(daily, phase_masks)
    pattern_summary = {
        phase: _pattern_summary(events, phase, "pattern_2", ascending=(phase == "takeprofit"))
        for phase in PHASES
    }
    return {
        "meta": {
            "db_paths": [str(path) for path in db_paths],
            "focus_code": FOCUS_CODE,
            "date_min": str(daily["dt"].min().date()),
            "date_max": str(daily["dt"].max().date()),
            "capital_base_yen": CAPITAL_BASE_YEN,
            "round_trip_cost": ROUND_TRIP_COST,
            "events_total": int(len(events)),
        },
        "phase_summary": _build_phase_summary(events),
        "period_summary": _build_period_summary(events),
        "pattern_summary": pattern_summary,
        "replay": _build_replay(events, FOCUS_CODE),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="5541-like long base breakout phase study")
    parser.add_argument("--db-path", type=Path, action="append", default=None, help="stocks.duckdb path; repeatable")
    parser.add_argument("--output-json", type=Path, default=Path("tmp/long_base_breakout_phase_study.json"))
    parser.add_argument("--output-md", type=Path, default=Path("tmp/long_base_breakout_phase_study.md"))
    args = parser.parse_args()

    db_paths = args.db_path or _resolve_default_db_paths()
    result = run_phase_study(db_paths)
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
