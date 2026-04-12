from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from app.backend.services.analysis.swing_expectancy_service import _to_ymd_expr
from app.backend.tools.sell_path_relearn import _fmt_pct, _jsonable, _table_exists
from app.backend.tools.weekly_top_gainers_study import _load_daily_frame
from app.db.session import get_conn

DEFAULT_LOOKBACK_DAYS = 365 * 3
DEFAULT_REPORT_DIR = Path("G:/Tradex/reports")


@dataclass(frozen=True)
class SellSpikeWeeklyConfig:
    lookback_days: int = DEFAULT_LOOKBACK_DAYS
    report_dir: Path = DEFAULT_REPORT_DIR
    min_rule_count: int = 20
    bullish_body_min: float = 0.03
    bullish_close_pos_min: float = 0.75
    bullish_upper_wick_max: float = 0.12
    bearish_body_min: float = 0.03
    bearish_close_pos_max: float = 0.25
    bearish_lower_wick_max: float = 0.12
    weekly_extension_min: float = 0.05


def _load_sell_signal_frame(conn, *, start_ymd: int, end_ymd: int) -> pd.DataFrame:
    if not _table_exists(conn, "signal_decision_daily") or not _table_exists(conn, "ml_feature_daily"):
        return pd.DataFrame()
    frame = conn.execute(
        f"""
        SELECT
            s.dt AS signal_dt,
            s.code,
            s.side,
            s.setup_type,
            s.entry_qualified,
            s.forward_return_5,
            s.forward_return_10,
            s.forward_return_20,
            s.forward_return_30,
            s.max_favorable_30,
            s.max_adverse_30,
            CAST(json_extract(s.score_snapshot_json, '$.entryScore') AS DOUBLE) AS entry_score,
            b.o AS bar_open,
            b.h AS bar_high,
            b.l AS bar_low,
            b.c AS bar_close,
            f.close,
            f.ma7,
            f.ma20,
            f.ma60,
            f.candle_body_ratio AS body_ratio,
            f.candle_upper_wick_ratio AS upper_wick_ratio,
            f.candle_lower_wick_ratio AS lower_wick_ratio,
            f.candle_triplet_up_prob,
            f.candle_triplet_down_prob,
            f.gap_pct,
            f.close_ret2,
            f.close_ret3,
            f.close_ret20,
            f.close_ret60,
            f.breakout20_down,
            f.rebound60,
            f.drawdown60,
            f.market_ret20,
            f.breadth_above_ma20,
            f.breadth_above_ma60,
            f.sector_ret20,
            f.rel_sector_ret20,
            r.regime_id
        FROM signal_decision_daily s
        LEFT JOIN daily_bars b
            ON b.code = s.code
           AND {_to_ymd_expr("b.date")} = s.dt
        LEFT JOIN ml_feature_daily f
            ON f.dt = epoch(strptime(cast(s.dt AS VARCHAR), '%Y%m%d'))::BIGINT
           AND f.code = s.code
        LEFT JOIN market_regime_daily r
            ON r.dt = s.dt
        WHERE s.entry_qualified = TRUE
          AND s.side = 'sell'
          AND s.dt BETWEEN ? AND ?
        ORDER BY s.dt ASC, s.code ASC
        """,
        [int(start_ymd), int(end_ymd)],
    ).df()
    if frame.empty:
        return frame
    frame["code"] = frame["code"].astype(str).str.strip()
    frame["side"] = frame["side"].astype(str).str.strip().str.lower()
    frame["setup_type"] = frame["setup_type"].astype(str).str.strip().str.lower()
    frame["signal_dt"] = pd.to_numeric(frame["signal_dt"], errors="coerce").astype("Int64")
    frame = frame.dropna(subset=["signal_dt", "code", "side"]).copy()
    frame["signal_dt"] = frame["signal_dt"].astype(int)
    for col in [
        "forward_return_5",
        "forward_return_10",
        "forward_return_20",
        "forward_return_30",
        "max_favorable_30",
        "max_adverse_30",
        "entry_score",
        "bar_open",
        "bar_high",
        "bar_low",
        "bar_close",
        "close",
        "ma7",
        "ma20",
        "ma60",
        "body_ratio",
        "upper_wick_ratio",
        "lower_wick_ratio",
        "candle_triplet_up_prob",
        "candle_triplet_down_prob",
        "gap_pct",
        "close_ret2",
        "close_ret3",
        "close_ret20",
        "close_ret60",
        "breakout20_down",
        "rebound60",
        "drawdown60",
        "market_ret20",
        "breadth_above_ma20",
        "breadth_above_ma60",
        "sector_ret20",
        "rel_sector_ret20",
    ]:
        if col in frame.columns:
            frame[col] = pd.to_numeric(frame[col], errors="coerce")
    return frame


def _build_weekly_frame(daily: pd.DataFrame) -> pd.DataFrame:
    if daily.empty:
        return pd.DataFrame()
    frame = daily.copy()
    frame["code"] = frame["code"].astype(str).str.strip()
    frame["date_dt"] = pd.to_datetime(frame["date_dt"], errors="coerce")
    frame = frame.dropna(subset=["date_dt", "o", "h", "l", "c"]).copy()
    frame.sort_values(["code", "date_dt"], inplace=True)
    frame["week_start_dt"] = frame["date_dt"] - pd.to_timedelta(frame["date_dt"].dt.weekday, unit="D")
    weekly = (
        frame.groupby(["code", "week_start_dt"], as_index=False, sort=True)
        .agg(
            week_last_dt=("date_dt", "max"),
            o=("o", "first"),
            h=("h", "max"),
            l=("l", "min"),
            c=("c", "last"),
            v=("v", "sum"),
            day_count=("date_dt", "count"),
        )
        .sort_values(["code", "week_start_dt"])
        .copy()
    )
    g = weekly.groupby("code", sort=False)
    weekly["week_start_ymd"] = weekly["week_start_dt"].dt.strftime("%Y%m%d").astype(int)
    weekly["week_last_ymd"] = weekly["week_last_dt"].dt.strftime("%Y%m%d").astype(int)
    weekly["prev_close"] = g["c"].shift(1)
    weekly["week_ret_cc"] = weekly["c"] / weekly["prev_close"] - 1.0
    weekly["trend_4w"] = weekly["c"] / g["c"].shift(4) - 1.0
    weekly["trend_12w"] = weekly["c"] / g["c"].shift(12) - 1.0
    weekly["ma4"] = g["c"].transform(lambda s: s.rolling(4, min_periods=4).mean())
    weekly["ma13"] = g["c"].transform(lambda s: s.rolling(13, min_periods=13).mean())
    weekly["ma26"] = g["c"].transform(lambda s: s.rolling(26, min_periods=26).mean())
    weekly["prev4_high"] = g["h"].transform(lambda s: s.shift(1).rolling(4, min_periods=4).max())
    weekly["prev4_low"] = g["l"].transform(lambda s: s.shift(1).rolling(4, min_periods=4).min())
    weekly["body_pct"] = (weekly["c"] - weekly["o"]).abs() / weekly["o"]
    weekly["upper_wick_pct"] = (weekly["h"] - weekly[["o", "c"]].max(axis=1)) / weekly["o"]
    weekly["lower_wick_pct"] = (weekly[["o", "c"]].min(axis=1) - weekly["l"]) / weekly["o"]
    weekly["close_pos_in_range"] = (weekly["c"] - weekly["l"]) / (weekly["h"] - weekly["l"])
    weekly["close_above_ma4"] = weekly["c"] > weekly["ma4"]
    weekly["close_above_ma13"] = weekly["c"] > weekly["ma13"]
    weekly["close_above_ma26"] = weekly["c"] > weekly["ma26"]
    weekly["ma4_gt_ma13"] = weekly["ma4"] > weekly["ma13"]
    weekly["ma13_gt_ma26"] = weekly["ma13"] > weekly["ma26"]
    weekly["ma_stack_bull"] = weekly["close_above_ma4"] & weekly["ma4_gt_ma13"] & weekly["ma13_gt_ma26"]
    weekly["ma_stack_bear"] = (~weekly["close_above_ma4"]) & (~weekly["ma4_gt_ma13"]) & (~weekly["ma13_gt_ma26"])
    weekly["breakout_4w_high"] = weekly["c"] > weekly["prev4_high"]
    weekly["breakdown_4w_low"] = weekly["c"] < weekly["prev4_low"]
    weekly["week_gap_ma13"] = weekly["c"] / weekly["ma13"] - 1.0
    weekly["week_gap_ma26"] = weekly["c"] / weekly["ma26"] - 1.0
    weekly["weekly_extension_up"] = (weekly["week_gap_ma13"] >= 0.05) | (weekly["week_gap_ma26"] >= 0.05)
    weekly["weekly_extension_down"] = (weekly["week_gap_ma13"] <= -0.05) | (weekly["week_gap_ma26"] <= -0.05)
    weekly["weekly_zone"] = "mid"
    weekly.loc[weekly["ma_stack_bull"], "weekly_zone"] = "bull_stack"
    weekly.loc[weekly["ma_stack_bear"], "weekly_zone"] = "bear_stack"
    weekly.loc[weekly["weekly_extension_up"] & ~weekly["ma_stack_bull"], "weekly_zone"] = "bull_extension"
    weekly.loc[weekly["weekly_extension_down"] & ~weekly["ma_stack_bear"], "weekly_zone"] = "bear_extension"
    weekly["weekly_spike_up"] = (
        (weekly["body_pct"] >= 0.03)
        & (weekly["close_pos_in_range"] >= 0.75)
        & (weekly["upper_wick_pct"] <= 0.12)
    )
    weekly["weekly_spike_down"] = (
        (weekly["body_pct"] >= 0.03)
        & (weekly["close_pos_in_range"] <= 0.25)
        & (weekly["lower_wick_pct"] <= 0.12)
    )
    return weekly


def _merge_weekly_context(frame: pd.DataFrame, weekly: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or weekly.empty:
        return frame.copy()
    left = frame.sort_values(["code", "signal_dt"]).copy()
    right = weekly.sort_values(["code", "week_last_ymd"]).copy()
    merged_frames: list[pd.DataFrame] = []
    right_groups = {code: group.sort_values("week_last_ymd").copy() for code, group in right.groupby("code", sort=False)}
    for code, left_group in left.groupby("code", sort=False):
        right_group = right_groups.get(code)
        if right_group is None or right_group.empty:
            merged_frames.append(left_group.copy())
            continue
        right_group = right_group.drop(columns=["code"], errors="ignore")
        merged_group = pd.merge_asof(
            left_group.sort_values("signal_dt"),
            right_group,
            left_on="signal_dt",
            right_on="week_last_ymd",
            direction="backward",
            allow_exact_matches=True,
        )
        merged_frames.append(merged_group)
    if not merged_frames:
        return frame.copy()
    merged = pd.concat(merged_frames, ignore_index=True)
    return merged.sort_values(["code", "signal_dt"]).reset_index(drop=True)


def _augment_features(frame: pd.DataFrame, *, config: SellSpikeWeeklyConfig) -> pd.DataFrame:
    out = frame.copy()
    out["daily_range_pct"] = (out["bar_high"] - out["bar_low"]) / out["bar_close"]
    out["daily_close_pos_in_range"] = (out["bar_close"] - out["bar_low"]) / (out["bar_high"] - out["bar_low"])
    out["bullish_spike_day"] = (
        (out["bar_close"] > out["bar_open"])
        & (out["body_ratio"] >= float(config.bullish_body_min))
        & (out["daily_close_pos_in_range"] >= float(config.bullish_close_pos_min))
        & (out["upper_wick_ratio"] <= float(config.bullish_upper_wick_max))
    )
    out["bearish_spike_day"] = (
        (out["bar_close"] < out["bar_open"])
        & (out["body_ratio"] >= float(config.bearish_body_min))
        & (out["daily_close_pos_in_range"] <= float(config.bearish_close_pos_max))
        & (out["lower_wick_ratio"] <= float(config.bearish_lower_wick_max))
    )
    out["weekly_zone"] = out["weekly_zone"].fillna("missing")
    out["weekly_bullish_context"] = out["weekly_zone"].isin({"bull_stack", "bull_extension"})
    out["weekly_bearish_context"] = out["weekly_zone"].isin({"bear_stack", "bear_extension"})
    out["weekly_overextended_up"] = out["weekly_extension_up"].fillna(False) & (out["close_pos_in_range"].fillna(0.0) >= 0.75)
    out["weekly_overextended_down"] = out["weekly_extension_down"].fillna(False) & (out["close_pos_in_range"].fillna(0.0) <= 0.25)
    out["weekly_breakdown_context"] = out["weekly_bearish_context"] | out["breakdown_4w_low"].fillna(False) | (out["market_ret20"].fillna(0.0) < 0.0) | (out["breadth_above_ma20"].fillna(1.0) <= 0.45)
    out["weekly_extension_context"] = out["weekly_overextended_up"] | out["weekly_overextended_down"]
    return out


def _short_profit(frame: pd.DataFrame, horizon: int) -> pd.Series:
    return -pd.to_numeric(frame[f"forward_return_{int(horizon)}"], errors="coerce")


def _summary(frame: pd.DataFrame, *, label: str) -> dict[str, Any]:
    if frame.empty:
        return {"label": label, "count": 0, "mean5": None, "mean10": None, "mean20": None, "win20": None, "median20": None}
    short5 = _short_profit(frame, 5)
    short10 = _short_profit(frame, 10)
    short20 = _short_profit(frame, 20)
    short30 = _short_profit(frame, 30)
    return {
        "label": label,
        "count": int(len(frame)),
        "mean5": float(short5.mean()),
        "mean10": float(short10.mean()),
        "mean20": float(short20.mean()),
        "win20": float((short20 > 0).mean()),
        "median20": float(short20.median()),
        "mean30": float(short30.mean()),
        "mean_week_gap_ma13": float(frame["week_gap_ma13"].mean()) if "week_gap_ma13" in frame.columns else None,
        "mean_week_gap_ma26": float(frame["week_gap_ma26"].mean()) if "week_gap_ma26" in frame.columns else None,
        "mean_week_close_pos": float(frame["close_pos_in_range"].mean()) if "close_pos_in_range" in frame.columns else None,
    }


def _bucket_summary(frame: pd.DataFrame, *, bucket_col: str, bucket_order: list[str], label_prefix: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for bucket in bucket_order:
        subset = frame[frame[bucket_col] == bucket]
        rows.append({"bucket": f"{label_prefix}:{bucket}", **_summary(subset, label=f"{label_prefix}:{bucket}")})
    return rows


def _combo_summary(frame: pd.DataFrame, *, label: str, mask: pd.Series) -> dict[str, Any]:
    subset = frame[mask.fillna(False)].copy()
    payload = _summary(subset, label=label)
    payload["weekly_bullish_context_rate"] = float(subset["weekly_bullish_context"].mean()) if len(subset) else None
    payload["weekly_bearish_context_rate"] = float(subset["weekly_bearish_context"].mean()) if len(subset) else None
    payload["bullish_spike_day_rate"] = float(subset["bullish_spike_day"].mean()) if len(subset) else None
    payload["bearish_spike_day_rate"] = float(subset["bearish_spike_day"].mean()) if len(subset) else None
    payload["weekly_extension_rate"] = float(subset["weekly_extension_context"].mean()) if len(subset) else None
    return payload


def run_sell_spike_weekly_relearn(*, config: SellSpikeWeeklyConfig = SellSpikeWeeklyConfig()) -> dict[str, Any]:
    with get_conn() as conn:
        if not _table_exists(conn, "signal_decision_daily") or not _table_exists(conn, "ml_feature_daily"):
            return {"ok": False, "reason": "required_tables_missing"}
        latest_row = conn.execute("SELECT MAX(dt) FROM signal_decision_daily").fetchone()
        if not latest_row or latest_row[0] is None:
            return {"ok": False, "reason": "signal_frame_empty"}
        latest_ymd = int(latest_row[0])
        start_dt = datetime.strptime(str(latest_ymd), "%Y%m%d")
        start_ymd = int((start_dt - pd.Timedelta(days=max(1, int(config.lookback_days)))).strftime("%Y%m%d"))
        daily = _load_daily_frame(conn, lookback_days=config.lookback_days)
        frame = _load_sell_signal_frame(conn, start_ymd=start_ymd, end_ymd=latest_ymd)
    if frame.empty or daily.empty:
        return {"ok": False, "reason": "signal_or_daily_empty"}
    weekly = _build_weekly_frame(daily)
    if weekly.empty:
        return {"ok": False, "reason": "weekly_frame_empty"}
    frame = _merge_weekly_context(frame, weekly)
    frame = _augment_features(frame, config=config)
    frame["short_profit_5"] = _short_profit(frame, 5)
    frame["short_profit_10"] = _short_profit(frame, 10)
    frame["short_profit_20"] = _short_profit(frame, 20)
    frame["short_profit_30"] = _short_profit(frame, 30)

    buckets = {
        "bull_spike": frame["bullish_spike_day"],
        "bear_spike": frame["bearish_spike_day"],
        "no_spike": ~(frame["bullish_spike_day"] | frame["bearish_spike_day"]),
        "bull_spike__weekly_bull_stack": frame["bullish_spike_day"] & (frame["weekly_zone"] == "bull_stack"),
        "bull_spike__weekly_bull_extension": frame["bullish_spike_day"] & (frame["weekly_zone"] == "bull_extension"),
        "bull_spike__weekly_bear_stack": frame["bullish_spike_day"] & (frame["weekly_zone"] == "bear_stack"),
        "bull_spike__weekly_bear_extension": frame["bullish_spike_day"] & (frame["weekly_zone"] == "bear_extension"),
        "bull_spike__weekly_breakdown_context": frame["bullish_spike_day"] & frame["weekly_breakdown_context"],
        "bear_spike__weekly_bear_stack": frame["bearish_spike_day"] & (frame["weekly_zone"] == "bear_stack"),
        "bear_spike__weekly_bear_extension": frame["bearish_spike_day"] & (frame["weekly_zone"] == "bear_extension"),
        "breakdown_setup": frame["setup_type"] == "breakdown",
        "pressure_setup": frame["setup_type"] == "pressure",
    }
    bucket_rows = [{"bucket": label, **_summary(frame[mask], label=label)} for label, mask in buckets.items()]
    bucket_rows = sorted(bucket_rows, key=lambda row: (-int(row["count"]), str(row["bucket"])))

    weekly_zone_rows = _bucket_summary(
        frame,
        bucket_col="weekly_zone",
        bucket_order=["bull_stack", "bull_extension", "mid", "bear_extension", "bear_stack", "missing"],
        label_prefix="weekly_zone",
    )

    top_rule_rows = sorted(
        [
            row
            for row in bucket_rows
            if row.get("count") and int(row["count"]) >= int(config.min_rule_count) and row.get("mean20") is not None
        ],
        key=lambda row: (float(row["mean20"]), float(row["win20"] or 0.0), int(row["count"])),
        reverse=True,
    )

    result = {
        "ok": True,
        "as_of_ymd": latest_ymd,
        "period_start_ymd": int(frame["signal_dt"].min()),
        "period_end_ymd": int(frame["signal_dt"].max()),
        "lookback_days": int(config.lookback_days),
        "row_count": int(len(frame)),
        "overall": _summary(frame, label="all_sell_entries"),
        "daily_spike": {
            "bullish": _summary(frame[frame["bullish_spike_day"]], label="bullish_spike"),
            "bearish": _summary(frame[frame["bearish_spike_day"]], label="bearish_spike"),
            "none": _summary(frame[~(frame["bullish_spike_day"] | frame["bearish_spike_day"])], label="no_spike"),
        },
        "weekly_zone": weekly_zone_rows,
        "bucket_summary": bucket_rows,
        "top_rules": top_rule_rows[:10],
        "recommendation": top_rule_rows[0] if top_rule_rows else None,
    }
    return result


def _write_markdown_report(result: dict[str, Any], output_path: Path) -> None:
    lines = [
        "# Sell Spike Weekly Relearn",
        "",
        f"- ok: `{result.get('ok')}`",
        f"- as_of_ymd: `{result.get('as_of_ymd')}`",
        f"- period_start_ymd: `{result.get('period_start_ymd')}`",
        f"- period_end_ymd: `{result.get('period_end_ymd')}`",
        f"- row_count: `{result.get('row_count')}`",
        "",
        "## Overall",
        "",
        "| count | mean5 | mean10 | mean20 | win20 | median20 | week_gap_ma13 | week_gap_ma26 |",
        "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    overall = result.get("overall") or {}
    lines.append(
        "| {count} | {mean5} | {mean10} | {mean20} | {win20} | {median20} | {gap13} | {gap26} |".format(
            count=overall.get("count"),
            mean5=_fmt_pct(overall.get("mean5")),
            mean10=_fmt_pct(overall.get("mean10")),
            mean20=_fmt_pct(overall.get("mean20")),
            win20=_fmt_pct(overall.get("win20")),
            median20=_fmt_pct(overall.get("median20")),
            gap13=_fmt_pct(overall.get("mean_week_gap_ma13")),
            gap26=_fmt_pct(overall.get("mean_week_gap_ma26")),
        )
    )
    lines += [
        "",
        "## Daily Spikes",
        "",
        "| bucket | count | mean5 | mean10 | mean20 | win20 |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for key in ("bullish", "bearish", "none"):
        subset = (result.get("daily_spike") or {}).get(key) or {}
        lines.append(
            f"| daily:{key} | {subset.get('count')} | {_fmt_pct(subset.get('mean5'))} | {_fmt_pct(subset.get('mean10'))} | {_fmt_pct(subset.get('mean20'))} | {_fmt_pct(subset.get('win20'))} |"
        )
    lines += [
        "",
        "## Weekly Zones",
        "",
        "| bucket | count | mean5 | mean10 | mean20 | win20 |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in result.get("weekly_zone") or []:
        lines.append(
            f"| {row.get('bucket')} | {row.get('count')} | {_fmt_pct(row.get('mean5'))} | {_fmt_pct(row.get('mean10'))} | {_fmt_pct(row.get('mean20'))} | {_fmt_pct(row.get('win20'))} |"
        )
    lines += [
        "",
        "## Top Rules",
        "",
        "| bucket | count | mean20 | win20 | weekly_bullish_context_rate | weekly_extension_rate |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in result.get("top_rules") or []:
        lines.append(
            f"| {row.get('bucket')} | {row.get('count')} | {_fmt_pct(row.get('mean20'))} | {_fmt_pct(row.get('win20'))} | {_fmt_pct(row.get('weekly_bullish_context_rate'))} | {_fmt_pct(row.get('weekly_extension_rate'))} |"
        )
    recommendation = result.get("recommendation") or {}
    if recommendation:
        lines += [
            "",
            "## Recommendation",
            "",
            f"- bucket: `{recommendation.get('bucket')}`",
            f"- count: `{recommendation.get('count')}`",
            f"- mean20: `{_fmt_pct(recommendation.get('mean20'))}`",
            f"- win20: `{_fmt_pct(recommendation.get('win20'))}`",
        ]
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_json_report(result: dict[str, Any], output_path: Path) -> None:
    output_path.write_text(json.dumps(_jsonable(result), ensure_ascii=False, indent=2), encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Relearn sell-side spike and weekly position relationships")
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS)
    parser.add_argument("--bullish-body-min", type=float, default=0.03)
    parser.add_argument("--bullish-close-pos-min", type=float, default=0.75)
    parser.add_argument("--bullish-upper-wick-max", type=float, default=0.12)
    parser.add_argument("--bearish-body-min", type=float, default=0.03)
    parser.add_argument("--bearish-close-pos-max", type=float, default=0.25)
    parser.add_argument("--bearish-lower-wick-max", type=float, default=0.12)
    parser.add_argument("--weekly-extension-min", type=float, default=0.05)
    parser.add_argument("--min-rule-count", type=int, default=20)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
    parser.add_argument("--prefix", default="sell_spike_weekly_relearn")
    args = parser.parse_args(argv)
    result = run_sell_spike_weekly_relearn(
        config=SellSpikeWeeklyConfig(
            lookback_days=int(args.lookback_days),
            report_dir=Path(args.report_dir),
            bullish_body_min=float(args.bullish_body_min),
            bullish_close_pos_min=float(args.bullish_close_pos_min),
            bullish_upper_wick_max=float(args.bullish_upper_wick_max),
            bearish_body_min=float(args.bearish_body_min),
            bearish_close_pos_max=float(args.bearish_close_pos_max),
            bearish_lower_wick_max=float(args.bearish_lower_wick_max),
            weekly_extension_min=float(args.weekly_extension_min),
            min_rule_count=int(args.min_rule_count),
        )
    )
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d%H%M%S")
    json_path = report_dir / f"{args.prefix}_{stamp}.json"
    md_path = report_dir / f"{args.prefix}_{stamp}.md"
    _write_json_report(result, json_path)
    _write_markdown_report(result, md_path)
    print(json.dumps({"ok": result.get("ok"), "json": str(json_path), "markdown": str(md_path)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
