#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def _to_numeric(df: pd.DataFrame, column: str) -> pd.Series:
    return pd.to_numeric(df[column], errors="coerce")


def _metrics(frame: pd.DataFrame) -> dict[str, float]:
    ret = _to_numeric(frame, "ret")
    hold = _to_numeric(frame, "hold_months")
    return {
        "n": int(len(frame)),
        "avg_short_ret": float(ret.mean()),
        "med_short_ret": float(ret.median()),
        "win_rate_short": float((ret > 0).mean()),
        "gain_ge_10pct_rate": float((ret >= 0.10).mean()),
        "loss_le_-5pct_rate": float((ret <= -0.05).mean()),
        "avg_hold_months": float(hold.mean()),
    }


def _progress_bins(frame: pd.DataFrame) -> pd.DataFrame:
    entry = _to_numeric(frame, "entry_price")
    exit_price = _to_numeric(frame, "exit_price")
    box_low = _to_numeric(frame, "box_low")

    denom = entry - box_low
    with np.errstate(divide="ignore", invalid="ignore"):
        progress = (entry - exit_price) / denom
    progress = progress.where(np.isfinite(progress), np.nan)
    progress = progress.where(denom != 0, np.nan)

    bins = np.full(len(frame), "", dtype=object)
    bins[(progress < 0) & progress.notna()] = "neg"
    bins[(progress >= 0) & (progress < 0.25)] = "0-25%"
    bins[(progress >= 0.25) & (progress < 0.50)] = "25-50%"
    bins[(progress >= 0.50) & (progress < 0.75)] = "50-75%"
    bins[(progress >= 0.75) & (progress < 1.00)] = "75-100%"
    bins[(progress >= 1.00)] = "100%+"

    work = frame.copy()
    work["progress_bin"] = bins
    ret = _to_numeric(work, "ret")
    hold = _to_numeric(work, "hold_months")
    grouped = (
        work.assign(ret_num=ret, hold_num=hold)
        .groupby("progress_bin", dropna=False, as_index=False)
        .agg(
            n=("ret_num", "size"),
            avg_short_ret=("ret_num", "mean"),
            med_short_ret=("ret_num", "median"),
            win_rate_short=("ret_num", lambda s: (s > 0).mean()),
            gain_ge_10pct_rate=("ret_num", lambda s: (s >= 0.10).mean()),
            avg_hold_months=("hold_num", "mean"),
        )
    )
    order = {name: idx for idx, name in enumerate(["neg", "0-25%", "25-50%", "50-75%", "75-100%", "100%+", ""])}
    grouped["_ord"] = grouped["progress_bin"].map(lambda x: order.get(x, 999))
    grouped = grouped.sort_values(["_ord", "progress_bin"]).drop(columns=["_ord"]).reset_index(drop=True)
    return grouped


def build_outputs(input_csv: Path, out_dir: Path) -> list[Path]:
    if not input_csv.exists():
        raise FileNotFoundError(f"input csv not found: {input_csv}")
    out_dir.mkdir(parents=True, exist_ok=True)

    raw = pd.read_csv(input_csv, low_memory=False)
    if "side" not in raw.columns:
        raise ValueError("input csv is missing 'side' column")
    if "exit_rule" not in raw.columns:
        raise ValueError("input csv is missing 'exit_rule' column")

    sell = raw[raw["side"].astype(str).str.lower() == "sell"].copy()
    if sell.empty:
        raise ValueError("no sell rows found in input csv")

    sell["full_retrace_hit"] = sell["exit_rule"].astype(str).str.lower().eq("target")

    all_metrics = _metrics(sell)
    full_metrics = _metrics(sell[sell["full_retrace_hit"]])
    not_full_metrics = _metrics(sell[~sell["full_retrace_hit"]])

    summary_rows = [
        {
            "segment": "all",
            "n": all_metrics["n"],
            "hit_rate_full_retrace": float(sell["full_retrace_hit"].mean()),
            **all_metrics,
        },
        {
            "segment": "full_retrace",
            "n": full_metrics["n"],
            "hit_rate_full_retrace": 1.0,
            **full_metrics,
        },
        {
            "segment": "not_full_retrace",
            "n": not_full_metrics["n"],
            "hit_rate_full_retrace": 0.0,
            **not_full_metrics,
        },
    ]
    summary_df = pd.DataFrame(summary_rows)[
        [
            "segment",
            "n",
            "hit_rate_full_retrace",
            "avg_short_ret",
            "med_short_ret",
            "win_rate_short",
            "gain_ge_10pct_rate",
            "loss_le_-5pct_rate",
            "avg_hold_months",
        ]
    ]

    progress_df = _progress_bins(sell)

    breakout = sell.copy()
    breakout["breakout_dir_episode"] = breakout["breakout_dir_episode"].astype(str).str.lower()
    breakout = breakout[breakout["breakout_dir_episode"].isin(["up", "down"])].copy()
    by_breakout_df = (
        breakout.groupby(["full_retrace_hit", "breakout_dir_episode"], as_index=False)
        .agg(
            n=("ret", "size"),
            avg_short_ret=("ret", "mean"),
            med_short_ret=("ret", "median"),
            win_rate_short=("ret", lambda s: (pd.to_numeric(s, errors="coerce") > 0).mean()),
            gain_ge_10pct_rate=("ret", lambda s: (pd.to_numeric(s, errors="coerce") >= 0.10).mean()),
        )
        .sort_values(["full_retrace_hit", "breakout_dir_episode"])
        .reset_index(drop=True)
    )

    output_summary = out_dir / "full_retrace_impact_summary.csv"
    output_progress = out_dir / "full_retrace_progress_bins.csv"
    output_breakout = out_dir / "full_retrace_by_breakout_dir.csv"

    summary_df.to_csv(output_summary, index=False)
    progress_df.to_csv(output_progress, index=False)
    by_breakout_df.to_csv(output_breakout, index=False)
    return [output_summary, output_progress, output_breakout]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate full retrace impact summaries from range trade events.")
    parser.add_argument("--input-csv", required=True, help="Path to monthly_box3_range_trade_events.csv")
    parser.add_argument("--out-dir", required=True, help="Output directory for summary csv files")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_csv = Path(args.input_csv).resolve()
    out_dir = Path(args.out_dir).resolve()
    outputs = build_outputs(input_csv=input_csv, out_dir=out_dir)
    for path in outputs:
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
