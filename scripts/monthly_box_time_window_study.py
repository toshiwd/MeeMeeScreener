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
    PHASES,
    REPLAY_CODES,
    _build_bottom_entry_summary,
    _build_event_frame,
    _build_failure_summary,
    _build_monthly_box_map,
    _build_pattern_summary,
    _build_phase_masks,
    _build_phase_summary,
    _build_replay_examples,
    _build_month_index_summary,
    _bucket_box_month_index,
    _classify_monthly_context,
    _daily_box_zone,
    _load_monthly_frame,
    _load_ticker_names,
    _month_int_to_period,
    _prepare_frame as _prepare_monthly_box_frame,
    _summary_from_series,
)
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


TIME_LABELS = (
    "month_start_1_3",
    "month_end_1_3",
    "day9_window",
    "day17_window",
    "day26_window",
    "other",
)


def _add_time_window_features(daily: pd.DataFrame) -> pd.DataFrame:
    daily = daily.copy()
    month_key = daily["dt"].dt.to_period("M")
    daily["month_key"] = month_key.astype(str)
    daily["month_trade_day"] = daily.groupby(["code", month_key], sort=False).cumcount() + 1
    monthly_counts = daily.groupby(["code", month_key], sort=False)["month_trade_day"].transform("max")
    daily["days_to_month_end"] = monthly_counts - daily["month_trade_day"] + 1

    def _timing_label(day_idx: Any, days_to_end: Any) -> str:
        if day_idx is None or days_to_end is None:
            return "other"
        if not np.isfinite(day_idx) or not np.isfinite(days_to_end):
            return "other"
        day = int(day_idx)
        remain = int(days_to_end)
        if day <= 3:
            return "month_start_1_3"
        if remain <= 3:
            return "month_end_1_3"
        if abs(day - 9) <= 1:
            return "day9_window"
        if abs(day - 17) <= 1:
            return "day17_window"
        if abs(day - 26) <= 1:
            return "day26_window"
        return "other"

    daily["timing_label"] = [
        _timing_label(day_idx, days_to_end)
        for day_idx, days_to_end in zip(daily["month_trade_day"], daily["days_to_month_end"])
    ]
    daily["timing_gate"] = daily["timing_label"].ne("other")
    return daily


def _summary_row(frame: pd.DataFrame, *, phase: str, label: str) -> dict[str, Any]:
    summary10 = _summary_from_series(frame["ret_long_10d"])
    summary20 = _summary_from_series(frame["ret_long_20d"])
    return {
        "phase": phase,
        "label": label,
        "n": int(summary10["n"]),
        "mean_ret10d": summary10["mean"],
        "mean_ret20d": summary20["mean"],
        "win_rate10d": summary10["win_rate"],
        "profit_factor10d": summary10["profit_factor"],
        "mfe20d": float(frame["mfe_20d"].mean()) if not frame.empty else None,
        "mae20d": float(frame["mae_20d"].mean()) if not frame.empty else None,
        "expected_yen_10d_1m": round((summary10["mean"] or 0.0) * 1_000_000),
        "expected_yen_20d_1m": round((summary20["mean"] or 0.0) * 1_000_000),
    }


def _build_timing_summary(events: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for phase in PHASES:
        frame = events.loc[events["phase"].eq(phase)].copy()
        if frame.empty:
            continue
        rows.append(_summary_row(frame, phase=phase, label="all"))
        rows.append(_summary_row(frame.loc[frame["timing_gate"]], phase=phase, label="timing_gate"))
        rows.append(_summary_row(frame.loc[~frame["timing_gate"]], phase=phase, label="other"))
        for label in TIME_LABELS:
            if label == "other":
                continue
            sub = frame.loc[frame["timing_label"].eq(label)]
            if sub.empty or len(sub) < PATTERN_MIN_SAMPLES:
                continue
            rows.append(_summary_row(sub, phase=phase, label=label))
    rows.sort(key=lambda item: (item["phase"], item["label"]))
    return rows


def _build_box_time_combo_summary(events: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    frame = events.loc[events["timing_gate"]].copy()
    if frame.empty:
        return rows
    for (phase, box_bucket, timing_label), group in frame.groupby(["phase", "analysis_box_month_bucket", "timing_label"], dropna=False):
        summary10 = _summary_from_series(group["ret_long_10d"])
        summary20 = _summary_from_series(group["ret_long_20d"])
        if int(summary10["n"]) < PATTERN_MIN_SAMPLES:
            continue
        rows.append(
            {
                "phase": str(phase),
                "box_month_bucket": str(box_bucket),
                "timing_label": str(timing_label),
                "n": int(summary10["n"]),
                "mean_ret10d": summary10["mean"],
                "mean_ret20d": summary20["mean"],
                "win_rate10d": summary10["win_rate"],
                "profit_factor10d": summary10["profit_factor"],
                "mfe20d": float(group["mfe_20d"].mean()),
                "mae20d": float(group["mae_20d"].mean()),
            }
        )
    rows.sort(key=lambda item: ((item.get("mean_ret20d") or -999.0), item.get("n") or 0), reverse=True)
    return rows[:40]


def _attach_origin_box_context(events: pd.DataFrame) -> pd.DataFrame:
    events = events.copy()
    events["analysis_box_month_bucket"] = events["box_month_bucket"].copy()
    events["analysis_box_month_index"] = events["box_month_index"].copy()
    if events.empty:
        return events
    origin_lookup = (
        events.loc[events["phase"].eq("breakout_entry"), ["code", "signal_date", "box_month_bucket", "box_month_index"]]
        .dropna(subset=["box_month_bucket", "box_month_index"], how="all")
        .drop_duplicates(["code", "signal_date"], keep="last")
        .set_index(["code", "signal_date"])
    )
    fail_mask = events["phase"].eq("failed_breakout_exit") & events["breakout_origin_date"].notna()
    for idx, row in events.loc[fail_mask, ["code", "breakout_origin_date"]].iterrows():
        key = (str(row["code"]), str(row["breakout_origin_date"]))
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


def _build_timing_period_summary(events: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for (phase, period_bucket, timing_label), group in events.groupby(["phase", "period_bucket", "timing_label"], dropna=False):
        summary10 = _summary_from_series(group["ret_long_10d"])
        summary20 = _summary_from_series(group["ret_long_20d"])
        if int(summary10["n"]) < PATTERN_MIN_SAMPLES:
            continue
        rows.append(
            {
                "phase": str(phase),
                "period_bucket": str(period_bucket),
                "timing_label": str(timing_label),
                "n": int(summary10["n"]),
                "mean_ret10d": summary10["mean"],
                "mean_ret20d": summary20["mean"],
                "win_rate10d": summary10["win_rate"],
                "profit_factor10d": summary10["profit_factor"],
            }
        )
    rows.sort(key=lambda item: (item["phase"], item["period_bucket"], item["timing_label"]))
    return rows


def _build_article_gate_assessment(events: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for phase in PHASES:
        frame = events.loc[events["phase"].eq(phase)].copy()
        if frame.empty:
            continue
        all20 = _summary_from_series(frame["ret_long_20d"])
        gate20 = _summary_from_series(frame.loc[frame["timing_gate"], "ret_long_20d"])
        start20 = _summary_from_series(frame.loc[frame["timing_label"].eq("month_start_1_3"), "ret_long_20d"])
        end20 = _summary_from_series(frame.loc[frame["timing_label"].eq("month_end_1_3"), "ret_long_20d"])
        day9 = _summary_from_series(frame.loc[frame["timing_label"].eq("day9_window"), "ret_long_20d"])
        day17 = _summary_from_series(frame.loc[frame["timing_label"].eq("day17_window"), "ret_long_20d"])
        day26 = _summary_from_series(frame.loc[frame["timing_label"].eq("day26_window"), "ret_long_20d"])
        best_label = "other"
        best_mean = None
        for label in TIME_LABELS:
            if label == "other":
                continue
            sub = frame.loc[frame["timing_label"].eq(label), "ret_long_20d"]
            if sub.dropna().empty:
                continue
            mean = float(sub.dropna().mean())
            if best_mean is None or mean > best_mean:
                best_mean = mean
                best_label = label
        out[phase] = {
            "all_mean20": all20["mean"],
            "all_pf20": all20["profit_factor"],
            "gate_mean20": gate20["mean"],
            "gate_pf20": gate20["profit_factor"],
            "month_start_mean20": start20["mean"],
            "month_end_mean20": end20["mean"],
            "day9_mean20": day9["mean"],
            "day17_mean20": day17["mean"],
            "day26_mean20": day26["mean"],
            "best_timing_label": best_label,
            "best_timing_mean20": best_mean,
            "gate_minus_all_mean20": None if gate20["mean"] is None or all20["mean"] is None else float(gate20["mean"] - all20["mean"]),
        }
    return out


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
                    "month_trade_day": int(row["month_trade_day"]) if pd.notna(row.get("month_trade_day")) else None,
                    "timing_label": str(row.get("timing_label") or "other"),
                    "box_id": str(row["box_id"]) if pd.notna(row.get("box_id")) else None,
                    "box_month_index": int(row["box_month_index"]) if pd.notna(row.get("box_month_index")) else None,
                    "monthly_context": str(row.get("monthly_context") or "na"),
                    "weekly_context": str(row.get("weekly_context") or "na"),
                    "daily_pattern_2": str(row["daily_pattern_2"]) if pd.notna(row.get("daily_pattern_2")) else None,
                    "entry_style": str(row["entry_style"]) if pd.notna(row.get("entry_style")) else None,
                    "breakout_result": str(row["breakout_result"]) if pd.notna(row.get("breakout_result")) else None,
                    "failure_reason": str(row["failure_reason"]) if pd.notna(row.get("failure_reason")) else None,
                    "ret10d": float(row["ret_long_10d"]) if pd.notna(row.get("ret_long_10d")) else None,
                    "ret20d": float(row["ret_long_20d"]) if pd.notna(row.get("ret_long_20d")) else None,
                }
            )
        result[code] = rows
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

    lines = [
        "# 月足ボックス + 日柄時間論 study",
        "",
        "## Setup",
        "",
        f"- DBs: `{', '.join(result['meta']['db_paths'])}`",
        f"- Date range: `{result['meta']['date_min']}` to `{result['meta']['date_max']}`",
        f"- Events total: `{result['meta']['events_total']}`",
        f"- Round-trip cost: `{result['meta']['round_trip_cost']:.3f}`",
        f"- Timing labels: `{', '.join(result['meta']['timing_labels'])}`",
        "",
        "## Phase Summary",
        "",
        "| phase | n | mean10d | mean20d | win10d | pf10d | mfe20d | mae20d | 1m_yen_10d | 1m_yen_20d |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in result["phase_summary"]:
        lines.append(
            "| "
            + f"{row['phase']} | {row['n']} | {fmt(row['mean'])} | {fmt(row['mean_ret20d'])} | {fmt(row['win_rate'], 3)} | "
            + f"{fmt(row['profit_factor'])} | {fmt(row['mfe20d'])} | {fmt(row['mae20d'])} | {row['expected_yen_10d_1m']} | {row['expected_yen_20d_1m']} |"
        )

    lines.extend(
        [
            "",
            "## Timing Gate Comparison",
            "",
            "| phase | label | n | mean10d | mean20d | win10d | pf10d | mfe20d | mae20d |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in result["timing_summary"]:
        if row["label"] not in {"all", "timing_gate", "other"}:
            continue
        lines.append(
            "| "
            + f"{row['phase']} | {row['label']} | {row['n']} | {fmt(row['mean_ret10d'])} | {fmt(row['mean_ret20d'])} | "
            + f"{fmt(row['win_rate10d'], 3)} | {fmt(row['profit_factor10d'])} | {fmt(row['mfe20d'])} | {fmt(row['mae20d'])} |"
        )

    lines.extend(
        [
            "",
            "## Timing Label Summary",
            "",
            "| phase | label | n | mean10d | mean20d | win10d | pf10d |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in result["timing_summary"]:
        if row["label"] in {"all", "timing_gate", "other"}:
            continue
        lines.append(
            "| "
            + f"{row['phase']} | {row['label']} | {row['n']} | {fmt(row['mean_ret10d'])} | {fmt(row['mean_ret20d'])} | "
            + f"{fmt(row['win_rate10d'], 3)} | {fmt(row['profit_factor10d'])} |"
        )

    lines.extend(
        [
            "",
            "## Box / Timing Combo",
            "",
            "| phase | box_bucket | timing | n | mean10d | mean20d | win10d | pf10d |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in result["box_time_combo_summary"]:
        lines.append(
            "| "
            + f"{row['phase']} | {row['box_month_bucket']} | {row['timing_label']} | {row['n']} | {fmt(row['mean_ret10d'])} | {fmt(row['mean_ret20d'])} | "
            + f"{fmt(row['win_rate10d'], 3)} | {fmt(row['profit_factor10d'])} |"
        )

    lines.extend(
        [
            "",
            "## Period / Timing",
            "",
            "| phase | period | timing | n | mean10d | mean20d | win10d | pf10d |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in result["timing_period_summary"][:40]:
        lines.append(
            "| "
            + f"{row['phase']} | {row['period_bucket']} | {row['timing_label']} | {row['n']} | {fmt(row['mean_ret10d'])} | {fmt(row['mean_ret20d'])} | "
            + f"{fmt(row['win_rate10d'], 3)} | {fmt(row['profit_factor10d'])} |"
        )

    lines.extend(
        [
            "",
            "## Article Gate Assessment",
            "",
            "| phase | all_mean20 | gate_mean20 | gate_minus_all | gate_pf20 | best_timing | best_mean20 |",
            "| --- | ---: | ---: | ---: | ---: | --- | ---: |",
        ]
    )
    for phase, row in result["article_gate_assessment"].items():
        lines.append(
            "| "
            + f"{phase} | {fmt(row['all_mean20'])} | {fmt(row['gate_mean20'])} | {fmt(row['gate_minus_all_mean20'])} | {fmt(row['gate_pf20'])} | "
            + f"{row['best_timing_label']} | {fmt(row['best_timing_mean20'])} |"
        )

    lines.extend(["", "## Replay Examples", ""])
    for code, rows in result["replay_examples"].items():
        lines.extend(
            [
                f"### {code}",
                "",
                "| date | phase | month_trade_day | timing_label | box_month_index | monthly_context | weekly_context | daily_pattern_2 | breakout_result | failure_reason | ret10d | ret20d |",
                "| --- | --- | ---: | --- | ---: | --- | --- | --- | --- | --- | ---: | ---: |",
            ]
        )
        for row in rows:
            lines.append(
                "| "
                + f"{row['date']} | {row['phase']} | {row['month_trade_day'] or 'na'} | {row['timing_label']} | "
                + f"{row['box_month_index'] or 'na'} | {row['monthly_context']} | {row['weekly_context']} | "
                + f"{row['daily_pattern_2'] or 'na'} | {row['breakout_result'] or 'na'} | {row['failure_reason'] or 'na'} | "
                + f"{fmt(row['ret10d'])} | {fmt(row['ret20d'])} |"
            )
        lines.append("")
    return "\n".join(lines) + "\n"


def run_monthly_box_time_window_study(db_paths: list[Path]) -> dict[str, Any]:
    daily = _prepare_monthly_box_frame(db_paths)
    daily = _add_time_window_features(daily)
    phase_masks = _build_phase_masks(daily)
    events = _attach_origin_box_context(_build_event_frame(daily, phase_masks))
    return {
        "meta": {
            "db_paths": [str(path) for path in db_paths],
            "date_min": str(daily["dt"].min().date()),
            "date_max": str(daily["dt"].max().date()),
            "events_total": int(len(events)),
            "round_trip_cost": ROUND_TRIP_COST,
            "pattern_min_samples": PATTERN_MIN_SAMPLES,
            "failed_breakout_window": FAILED_BREAKOUT_WINDOW,
            "replay_codes": list(REPLAY_CODES),
            "timing_labels": list(TIME_LABELS),
        },
        "phase_summary": _build_phase_summary(events),
        "timing_summary": _build_timing_summary(events),
        "box_time_combo_summary": _build_box_time_combo_summary(events),
        "timing_period_summary": _build_timing_period_summary(events),
        "article_gate_assessment": _build_article_gate_assessment(events),
        "month_index_summary": _build_month_index_summary(events),
        "period_summary": [
            {
                "phase": str(phase),
                "period_bucket": str(period_bucket),
                **_summary_row(group, phase=str(phase), label=str(period_bucket)),
            }
            for (phase, period_bucket), group in events.groupby(["phase", "period_bucket"], dropna=False)
            if not group.empty
        ],
        "pattern_summary": _build_pattern_summary(events),
        "failure_summary": _build_failure_summary(events),
        "bottom_entry_summary": _build_bottom_entry_summary(events),
        "replay_examples": _build_replay_examples(daily, events),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Study monthly box timing windows inspired by article-style day-count logic.")
    parser.add_argument("--db-path", action="append", dest="db_paths", type=Path, default=None)
    parser.add_argument("--output-json", type=Path, default=Path("tmp/monthly_box_time_window_study.json"))
    parser.add_argument("--output-md", type=Path, default=Path("tmp/monthly_box_time_window_study.md"))
    args = parser.parse_args()

    db_paths = args.db_paths or _resolve_default_db_paths()
    result = run_monthly_box_time_window_study(db_paths)
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    args.output_md.write_text(_build_report(result), encoding="utf-8")


if __name__ == "__main__":
    main()
