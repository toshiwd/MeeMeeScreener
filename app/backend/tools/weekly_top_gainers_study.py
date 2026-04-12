from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd

from app.db.session import get_conn
from app.backend.services.analysis.swing_expectancy_service import _to_ymd_expr


DEFAULT_LOOKBACK_DAYS = 1095
DEFAULT_TOP_N = 10
DEFAULT_REPORT_DIR = Path("G:/Tradex/reports")


@dataclass(frozen=True)
class WeeklyTopGainersStudyConfig:
    lookback_days: int = DEFAULT_LOOKBACK_DAYS
    top_n: int = DEFAULT_TOP_N
    report_dir: Path = DEFAULT_REPORT_DIR


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table_name],
    ).fetchone()
    return bool(row and row[0])


def _resolve_latest_ymd(conn) -> int | None:
    row = conn.execute(f"SELECT MAX({_to_ymd_expr('date')}) FROM daily_bars").fetchone()
    if not row or row[0] is None:
        return None
    return int(row[0])


def _resolve_cutoff_ymd(latest_ymd: int, lookback_days: int) -> int:
    latest_dt = datetime.strptime(str(int(latest_ymd)), "%Y%m%d")
    cutoff_dt = latest_dt - timedelta(days=max(1, int(lookback_days)))
    return int(cutoff_dt.strftime("%Y%m%d"))


def _load_daily_frame(conn, *, lookback_days: int) -> pd.DataFrame:
    latest_ymd = _resolve_latest_ymd(conn)
    if latest_ymd is None:
        return pd.DataFrame()
    cutoff_ymd = _resolve_cutoff_ymd(int(latest_ymd), int(lookback_days))
    query = f"""
        SELECT
            code,
            {_to_ymd_expr("date")} AS ymd,
            o,
            h,
            l,
            c,
            v
        FROM daily_bars
        WHERE {_to_ymd_expr("date")} >= ?
        ORDER BY code ASC, {_to_ymd_expr("date")} ASC
    """
    frame = conn.execute(query, [int(cutoff_ymd)]).df()
    if frame.empty:
        return frame
    frame["code"] = frame["code"].astype(str).str.strip()
    frame["ymd"] = pd.to_numeric(frame["ymd"], errors="coerce").astype("Int64")
    frame = frame.dropna(subset=["code", "ymd"]).copy()
    frame["ymd"] = frame["ymd"].astype(int)
    frame["date_dt"] = pd.to_datetime(frame["ymd"].astype(str), format="%Y%m%d", errors="coerce")
    frame = frame.dropna(subset=["date_dt"]).copy()
    return frame


def _build_streak(series: pd.Series) -> pd.Series:
    arr = series.fillna(False).to_numpy(dtype=np.bool_)
    out = np.zeros(arr.shape[0], dtype=np.int32)
    streak = 0
    for idx, value in enumerate(arr):
        if value:
            streak += 1
        else:
            streak = 0
        out[idx] = streak
    return pd.Series(out, index=series.index)


def _safe_ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return numerator / denominator.replace(0, np.nan)


def build_weekly_top_gainers_study_frame(daily_frame: pd.DataFrame, *, top_n: int = DEFAULT_TOP_N) -> pd.DataFrame:
    if daily_frame.empty:
        return pd.DataFrame()
    frame = daily_frame.copy()
    frame["code"] = frame["code"].astype(str).str.strip()
    frame["date_dt"] = pd.to_datetime(frame["date_dt"], errors="coerce")
    frame = frame.dropna(subset=["date_dt", "o", "h", "l", "c"]).copy()
    frame.sort_values(["code", "date_dt"], inplace=True)
    frame["week_start_dt"] = frame["date_dt"] - pd.to_timedelta(frame["date_dt"].dt.weekday, unit="D")
    grouped = frame.groupby(["code", "week_start_dt"], as_index=False, sort=True)
    weekly = grouped.agg(
        week_last_dt=("date_dt", "max"),
        o=("o", "first"),
        h=("h", "max"),
        l=("l", "min"),
        c=("c", "last"),
        v=("v", "sum"),
        day_count=("date_dt", "count"),
    )
    weekly.sort_values(["code", "week_start_dt"], inplace=True)
    g = weekly.groupby("code", sort=False)

    weekly["week_start_ymd"] = weekly["week_start_dt"].dt.strftime("%Y%m%d").astype(int)
    weekly["week_last_ymd"] = weekly["week_last_dt"].dt.strftime("%Y%m%d").astype(int)
    weekly["prev_close"] = g["c"].shift(1)
    weekly["next_close"] = g["c"].shift(-1)
    weekly["target_week_start_dt"] = g["week_start_dt"].shift(-1)
    weekly["target_week_start_ymd"] = g["week_start_ymd"].shift(-1)

    weekly["week_ret_cc"] = _safe_ratio(weekly["c"], weekly["prev_close"]) - 1.0
    weekly["target_next_week_ret_cc"] = _safe_ratio(weekly["next_close"], weekly["c"]) - 1.0
    weekly["trend_4w"] = _safe_ratio(weekly["c"], g["c"].shift(4)) - 1.0
    weekly["trend_12w"] = _safe_ratio(weekly["c"], g["c"].shift(12)) - 1.0

    weekly["ma4"] = g["c"].transform(lambda s: s.rolling(4, min_periods=4).mean())
    weekly["ma13"] = g["c"].transform(lambda s: s.rolling(13, min_periods=13).mean())
    weekly["ma26"] = g["c"].transform(lambda s: s.rolling(26, min_periods=26).mean())
    weekly["vol_ma4"] = g["v"].transform(lambda s: s.rolling(4, min_periods=4).mean())
    weekly["vol_ma12"] = g["v"].transform(lambda s: s.rolling(12, min_periods=12).mean())
    weekly["range_pct"] = _safe_ratio(weekly["h"] - weekly["l"], weekly["c"])
    weekly["range_ma4"] = g["range_pct"].transform(lambda s: s.rolling(4, min_periods=4).mean())
    weekly["range_ma12"] = g["range_pct"].transform(lambda s: s.rolling(12, min_periods=12).mean())
    weekly["prev4_high"] = g["h"].transform(lambda s: s.shift(1).rolling(4, min_periods=4).max())
    weekly["prev12_high"] = g["h"].transform(lambda s: s.shift(1).rolling(12, min_periods=12).max())
    weekly["body_pct"] = _safe_ratio((weekly["c"] - weekly["o"]).abs(), weekly["o"])
    weekly["upper_wick_pct"] = _safe_ratio(weekly["h"] - weekly[["o", "c"]].max(axis=1), weekly["o"])
    weekly["lower_wick_pct"] = _safe_ratio(weekly[["o", "c"]].min(axis=1) - weekly["l"], weekly["o"])
    weekly["close_pos_in_range"] = _safe_ratio(weekly["c"] - weekly["l"], weekly["h"] - weekly["l"])
    weekly["bullish_candle"] = weekly["c"] > weekly["o"]
    weekly["up_week"] = weekly["c"] > weekly["prev_close"]
    weekly["up_streak"] = g["up_week"].transform(_build_streak)
    weekly["close_above_ma4"] = weekly["c"] > weekly["ma4"]
    weekly["close_above_ma13"] = weekly["c"] > weekly["ma13"]
    weekly["ma4_gt_ma13"] = weekly["ma4"] > weekly["ma13"]
    weekly["ma13_gt_ma26"] = weekly["ma13"] > weekly["ma26"]
    weekly["ma_stack_bull"] = weekly["close_above_ma4"] & weekly["ma4_gt_ma13"] & weekly["ma13_gt_ma26"]
    weekly["breakout_4w_high"] = weekly["c"] > weekly["prev4_high"]
    weekly["breakout_12w_high"] = weekly["c"] > weekly["prev12_high"]
    weekly["range_contraction"] = weekly["range_ma4"] <= (0.8 * weekly["range_ma12"])
    weekly["vol_ratio_4w"] = _safe_ratio(weekly["v"], weekly["vol_ma4"])
    weekly["vol_ratio_12w"] = _safe_ratio(weekly["v"], weekly["vol_ma12"])
    weekly["close_near_high"] = weekly["close_pos_in_range"] >= 0.8
    weekly["gap_up_week"] = weekly["o"] > weekly["prev_close"]
    weekly["candidate_score"] = (
        weekly["trend_4w"].gt(0).astype(int)
        + weekly["trend_12w"].gt(0).astype(int)
        + weekly["ma_stack_bull"].astype(int)
        + weekly["breakout_4w_high"].astype(int)
        + weekly["range_contraction"].astype(int)
        + weekly["vol_ratio_4w"].ge(1.2).astype(int)
        + weekly["bullish_candle"].astype(int)
        + weekly["close_near_high"].astype(int)
        + weekly["up_streak"].ge(2).astype(int)
    )

    study = weekly.dropna(subset=["target_week_start_ymd", "target_next_week_ret_cc"]).copy()
    if study.empty:
        return study
    study["target_week_start_ymd"] = study["target_week_start_ymd"].astype(int)
    study["target_rank"] = study.groupby("target_week_start_ymd")["target_next_week_ret_cc"].rank(
        method="first",
        ascending=False,
    )
    study["is_top_n"] = study["target_rank"] <= int(top_n)
    return study


def _mean_or_none(series: pd.Series) -> float | None:
    if series.empty:
        return None
    value = series.mean()
    if pd.isna(value):
        return None
    return float(value)


def _median_or_none(series: pd.Series) -> float | None:
    if series.empty:
        return None
    value = series.median()
    if pd.isna(value):
        return None
    return float(value)


def _std_or_none(series: pd.Series) -> float | None:
    if series.empty:
        return None
    value = series.std(ddof=0)
    if pd.isna(value):
        return None
    return float(value)


def _summarize_feature_lift(study: pd.DataFrame) -> list[dict[str, Any]]:
    binary_features = [
        "trend_4w_pos",
        "trend_12w_pos",
        "ma_stack_bull",
        "breakout_4w_high",
        "breakout_12w_high",
        "range_contraction",
        "vol_ratio_4w_high",
        "bullish_candle",
        "close_near_high",
        "up_streak_2",
    ]
    numeric_features = [
        "trend_4w",
        "trend_12w",
        "range_pct",
        "body_pct",
        "upper_wick_pct",
        "lower_wick_pct",
        "close_pos_in_range",
        "vol_ratio_4w",
        "vol_ratio_12w",
        "candidate_score",
    ]
    working = study.copy()
    working["trend_4w_pos"] = working["trend_4w"] > 0
    working["trend_12w_pos"] = working["trend_12w"] > 0
    working["vol_ratio_4w_high"] = working["vol_ratio_4w"] >= 1.2
    working["up_streak_2"] = working["up_streak"] >= 2

    rows: list[dict[str, Any]] = []
    event = working[working["is_top_n"]]
    control = working[~working["is_top_n"]]
    for feature in binary_features:
        event_rate = _mean_or_none(event[feature].astype(float).fillna(0.0)) if feature in event.columns else None
        control_rate = _mean_or_none(control[feature].astype(float).fillna(0.0)) if feature in control.columns else None
        lift = None
        if event_rate is not None and control_rate is not None and control_rate > 0:
            lift = float(event_rate / control_rate)
        rows.append(
            {
                "feature": feature,
                "kind": "binary",
                "event_mean": event_rate,
                "control_mean": control_rate,
                "delta": None if event_rate is None or control_rate is None else float(event_rate - control_rate),
                "lift": lift,
            }
        )

    for feature in numeric_features:
        event_mean = _mean_or_none(event[feature]) if feature in event.columns else None
        control_mean = _mean_or_none(control[feature]) if feature in control.columns else None
        event_std = _std_or_none(event[feature]) if feature in event.columns else None
        control_std = _std_or_none(control[feature]) if feature in control.columns else None
        pooled_std = None
        if event_std is not None and control_std is not None:
            pooled_std = math.sqrt((event_std**2 + control_std**2) / 2.0) if (event_std > 0 or control_std > 0) else None
        effect_size = None
        if event_mean is not None and control_mean is not None and pooled_std and pooled_std > 0:
            effect_size = float((event_mean - control_mean) / pooled_std)
        rows.append(
            {
                "feature": feature,
                "kind": "numeric",
                "event_mean": event_mean,
                "control_mean": control_mean,
                "delta": None if event_mean is None or control_mean is None else float(event_mean - control_mean),
                "lift": effect_size,
            }
        )

    def _sort_key(row: dict[str, Any]) -> tuple[float, float]:
        lift = row.get("lift")
        delta = row.get("delta")
        return (float(abs(lift)) if lift is not None else -1.0, float(abs(delta)) if delta is not None else -1.0)

    return sorted(rows, key=_sort_key, reverse=True)


def _summarize_thresholds(study: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    total = int(len(study))
    total_positive = int(study["is_top_n"].sum())
    base_rate = float(total_positive / total) if total > 0 else 0.0
    max_score = int(study["candidate_score"].max()) if not study.empty else 0
    for threshold in range(0, max_score + 1):
        selected = study["candidate_score"] >= threshold
        selected_count = int(selected.sum())
        hit_count = int((selected & study["is_top_n"]).sum())
        precision = float(hit_count / selected_count) if selected_count > 0 else None
        recall = float(hit_count / total_positive) if total_positive > 0 else None
        lift = float(precision / base_rate) if precision is not None and base_rate > 0 else None
        mean_ret = _mean_or_none(study.loc[selected, "target_next_week_ret_cc"]) if selected_count > 0 else None
        median_ret = _median_or_none(study.loc[selected, "target_next_week_ret_cc"]) if selected_count > 0 else None
        rows.append(
            {
                "threshold": int(threshold),
                "selected_count": int(selected_count),
                "selected_rate": float(selected_count / total) if total > 0 else None,
                "precision": precision,
                "recall": recall,
                "lift": lift,
                "mean_next_week_ret": mean_ret,
                "median_next_week_ret": median_ret,
            }
        )
    return rows


def build_weekly_top_gainers_study(
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    top_n: int = DEFAULT_TOP_N,
) -> dict[str, Any]:
    with get_conn() as conn:
        if not _table_exists(conn, "daily_bars"):
            return {"ok": False, "reason": "daily_bars_missing"}
        daily_frame = _load_daily_frame(conn, lookback_days=lookback_days)
    if daily_frame.empty:
        return {"ok": False, "reason": "daily_frame_empty"}

    study = build_weekly_top_gainers_study_frame(daily_frame, top_n=top_n)
    if study.empty:
        return {"ok": False, "reason": "weekly_study_empty"}

    total_rows = int(len(study))
    top_rows = study[study["is_top_n"]].copy()
    threshold_rows = _summarize_thresholds(study)
    feature_lift_rows = _summarize_feature_lift(study)

    weeks = int(study["target_week_start_ymd"].nunique())
    codes = int(study["code"].nunique())
    total_positive = int(top_rows.shape[0])
    baseline_rate = float(total_positive / total_rows) if total_rows > 0 else 0.0
    best_threshold = max(
        threshold_rows,
        key=lambda row: (
            float(row["precision"] or 0.0),
            float(row["lift"] or 0.0),
            float(row["selected_count"] or 0.0),
        ),
    )

    top_events = (
        top_rows.sort_values(["target_week_start_ymd", "target_next_week_ret_cc"], ascending=[False, False])
        .head(30)[
            [
                "target_week_start_ymd",
                "code",
                "week_start_ymd",
                "week_last_ymd",
                "target_next_week_ret_cc",
                "candidate_score",
                "trend_4w",
                "trend_12w",
                "ma_stack_bull",
                "breakout_4w_high",
                "range_contraction",
                "vol_ratio_4w",
                "bullish_candle",
                "close_near_high",
                "up_streak",
            ]
        ]
        .copy()
    )

    result = {
        "ok": True,
        "as_of_ymd": int(study["target_week_start_ymd"].max()) if not study.empty else None,
        "lookback_days": int(lookback_days),
        "top_n": int(top_n),
        "codes": int(codes),
        "weeks": int(weeks),
        "rows": int(total_rows),
        "positive_rows": int(total_positive),
        "baseline_top_n_rate": baseline_rate,
        "threshold_rows": threshold_rows,
        "feature_lift_rows": feature_lift_rows,
        "best_threshold": best_threshold,
        "top_events": top_events.to_dict(orient="records"),
    }
    return result


def _format_pct(value: Any) -> str:
    if value is None:
        return "-"
    try:
        if pd.isna(value):
            return "-"
        return f"{float(value):.2%}"
    except Exception:
        return "-"


def _format_num(value: Any, digits: int = 4) -> str:
    if value is None:
        return "-"
    try:
        if pd.isna(value):
            return "-"
        return f"{float(value):.{digits}f}"
    except Exception:
        return "-"


def _write_markdown_report(result: dict[str, Any], output_path: Path) -> None:
    lines: list[str] = []
    lines.append("# Weekly Top Gainers Study")
    lines.append("")
    lines.append(f"- ok: `{result.get('ok')}`")
    lines.append(f"- as_of_ymd: `{result.get('as_of_ymd')}`")
    lines.append(f"- lookback_days: `{result.get('lookback_days')}`")
    lines.append(f"- top_n: `{result.get('top_n')}`")
    lines.append(f"- codes: `{result.get('codes')}`")
    lines.append(f"- weeks: `{result.get('weeks')}`")
    lines.append(f"- rows: `{result.get('rows')}`")
    lines.append(f"- positive_rows: `{result.get('positive_rows')}`")
    lines.append(f"- baseline_top_n_rate: `{_format_pct(result.get('baseline_top_n_rate'))}`")
    best = result.get("best_threshold") or {}
    lines.append("")
    lines.append("## Best Threshold")
    lines.append("")
    lines.append(f"- threshold: `{best.get('threshold')}`")
    lines.append(f"- selected_count: `{best.get('selected_count')}`")
    lines.append(f"- selected_rate: `{_format_pct(best.get('selected_rate'))}`")
    lines.append(f"- precision: `{_format_pct(best.get('precision'))}`")
    lines.append(f"- recall: `{_format_pct(best.get('recall'))}`")
    lines.append(f"- lift: `{_format_num(best.get('lift'))}`")
    lines.append(f"- mean_next_week_ret: `{_format_pct(best.get('mean_next_week_ret'))}`")
    lines.append(f"- median_next_week_ret: `{_format_pct(best.get('median_next_week_ret'))}`")
    lines.append("")
    lines.append("## Feature Lift")
    lines.append("")
    lines.append("| feature | kind | event_mean | control_mean | delta | lift/effect |")
    lines.append("| --- | --- | ---: | ---: | ---: | ---: |")
    for row in (result.get("feature_lift_rows") or [])[:15]:
        lines.append(
            "| {feature} | {kind} | {event_mean} | {control_mean} | {delta} | {lift} |".format(
                feature=str(row.get("feature")),
                kind=str(row.get("kind")),
                event_mean=_format_num(row.get("event_mean")),
                control_mean=_format_num(row.get("control_mean")),
                delta=_format_num(row.get("delta")),
                lift=_format_num(row.get("lift")),
            )
        )
    lines.append("")
    lines.append("## Threshold Table")
    lines.append("")
    lines.append("| threshold | selected | selected_rate | precision | recall | lift | mean_next_week_ret | median_next_week_ret |")
    lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for row in result.get("threshold_rows") or []:
        lines.append(
            "| {threshold} | {selected_count} | {selected_rate} | {precision} | {recall} | {lift} | {mean_ret} | {median_ret} |".format(
                threshold=row.get("threshold"),
                selected_count=row.get("selected_count"),
                selected_rate=_format_pct(row.get("selected_rate")),
                precision=_format_pct(row.get("precision")),
                recall=_format_pct(row.get("recall")),
                lift=_format_num(row.get("lift")),
                mean_ret=_format_pct(row.get("mean_next_week_ret")),
                median_ret=_format_pct(row.get("median_next_week_ret")),
            )
        )
    lines.append("")
    lines.append("## Recent Top Events")
    lines.append("")
    lines.append("| target_week | code | score | ret_next_week | trend_4w | trend_12w | breakout_4w_high | range_contraction | vol_ratio_4w |")
    lines.append("| --- | --- | ---: | ---: | ---: | ---: | --- | --- | ---: |")
    for row in result.get("top_events") or []:
        lines.append(
            "| {target_week} | {code} | {score} | {ret} | {trend4} | {trend12} | {breakout} | {range_contraction} | {vol} |".format(
                target_week=row.get("target_week_start_ymd"),
                code=row.get("code"),
                score=_format_num(row.get("candidate_score"), digits=0),
                ret=_format_pct(row.get("target_next_week_ret_cc")),
                trend4=_format_pct(row.get("trend_4w")),
                trend12=_format_pct(row.get("trend_12w")),
                breakout=str(bool(row.get("breakout_4w_high"))),
                range_contraction=str(bool(row.get("range_contraction"))),
                vol=_format_num(row.get("vol_ratio_4w")),
            )
        )

    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_json_report(result: dict[str, Any], output_path: Path) -> None:
    def _jsonable(value: Any) -> Any:
        if isinstance(value, dict):
            return {str(k): _jsonable(v) for k, v in value.items()}
        if isinstance(value, list):
            return [_jsonable(v) for v in value]
        if isinstance(value, tuple):
            return [_jsonable(v) for v in value]
        if isinstance(value, (pd.Timestamp, datetime)):
            return value.isoformat()
        if isinstance(value, np.generic):
            return value.item()
        return value

    output_path.write_text(json.dumps(_jsonable(result), ensure_ascii=False, indent=2), encoding="utf-8")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Study weekly top gainers and pre-move candle patterns.")
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS)
    parser.add_argument("--top-n", type=int, default=DEFAULT_TOP_N)
    parser.add_argument("--output-dir", default=str(DEFAULT_REPORT_DIR))
    parser.add_argument("--prefix", default="weekly_top_gainers_study")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    config = WeeklyTopGainersStudyConfig(
        lookback_days=int(args.lookback_days),
        top_n=int(args.top_n),
        report_dir=Path(str(args.output_dir)),
    )
    result = build_weekly_top_gainers_study(
        lookback_days=config.lookback_days,
        top_n=config.top_n,
    )
    config.report_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    json_path = config.report_dir / f"{args.prefix}_{stamp}.json"
    md_path = config.report_dir / f"{args.prefix}_{stamp}.md"
    _write_json_report(result, json_path)
    _write_markdown_report(result, md_path)
    print(json.dumps({"json": str(json_path), "markdown": str(md_path), "result": result}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
