from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from app.backend.services.analysis.swing_expectancy_service import _to_ymd_expr
from app.backend.tools import sell_monthly_crash_relearn as monthly_crash
from app.backend.tools import sell_path_relearn as sell_path
from app.backend.tools.weekly_top_gainers_study import _load_daily_frame
from app.db.session import get_conn

DEFAULT_LOOKBACK_DAYS = 365 * 3
DEFAULT_REPORT_DIR = Path("G:/Tradex/reports")


@dataclass(frozen=True)
class BuyMonthlyReboundConfig:
    lookback_days: int = DEFAULT_LOOKBACK_DAYS
    report_dir: Path = DEFAULT_REPORT_DIR
    min_rule_count: int = 20
    daily_body_min: float = 0.04
    daily_close_pos_min: float = 0.70
    daily_lower_wick_min: float = 0.12
    daily_upper_wick_max: float = 0.12
    monthly_body_min: float = 0.06
    big_rebound_min: float = 0.10


def _long_return(frame: pd.DataFrame, horizon: int) -> pd.Series:
    return pd.to_numeric(frame[f"forward_return_{int(horizon)}"], errors="coerce")


def _load_buy_signal_frame(conn, *, start_ymd: int, end_ymd: int) -> pd.DataFrame:
    if not sell_path._table_exists(conn, "signal_decision_daily") or not sell_path._table_exists(conn, "ml_feature_daily"):
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
          AND s.side = 'buy'
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


def _augment_features(frame: pd.DataFrame, *, config: BuyMonthlyReboundConfig) -> pd.DataFrame:
    out = frame.copy()
    out["daily_range_pct"] = (out["bar_high"] - out["bar_low"]) / out["bar_close"].replace(0, pd.NA)
    out["daily_close_pos_in_range"] = (out["bar_close"] - out["bar_low"]) / (out["bar_high"] - out["bar_low"]).replace(0, pd.NA)
    out["daily_hammer_like"] = (
        (out["bar_close"] > out["bar_open"])
        & (out["body_ratio"] >= float(config.daily_body_min))
        & (out["lower_wick_ratio"] >= float(config.daily_lower_wick_min))
        & (out["upper_wick_ratio"] <= float(config.daily_upper_wick_max))
    )
    out["daily_bullish_close"] = (
        (out["bar_close"] > out["bar_open"])
        & (out["daily_close_pos_in_range"] >= float(config.daily_close_pos_min))
    )
    out["daily_reversal_up"] = out["daily_hammer_like"] | out["daily_bullish_close"] | (out["rebound60"].fillna(0.0) > 0)
    out["monthly_zone"] = out["monthly_zone"].fillna("missing")
    out["monthly_bullish_context"] = out["monthly_zone"].isin({"bull_stack", "bull_extension"})
    out["monthly_bearish_context"] = out["monthly_zone"].isin({"bear_stack", "bear_extension"})
    out["monthly_sideways_context"] = out["monthly_zone"].eq("sideways")
    out["monthly_capitulation_context"] = (
        out["monthly_bearish_context"]
        | out["monthly_sideways_context"]
        | (out["market_ret20"].fillna(0.0) < 0.0)
        | (out["breadth_above_ma20"].fillna(1.0) <= 0.45)
    )
    out["monthly_rebound_context"] = out["monthly_capitulation_context"] & (out["daily_reversal_up"] | (out["ma20"] > out["ma60"]))
    out["monthly_ma_reclaim"] = (out["close"] > out["ma20"]) | (out["close"] > out["ma60"])
    out["monthly_bullish_spike"] = out["daily_bullish_close"] & (out["upper_wick_ratio"] <= float(config.daily_upper_wick_max))
    return out


def _summary(frame: pd.DataFrame, *, label: str, big_rebound_min: float) -> dict[str, Any]:
    if frame.empty:
        return {
            "label": label,
            "count": 0,
            "mean5": None,
            "mean10": None,
            "mean20": None,
            "win20": None,
            "median20": None,
            "mean30": None,
            "big_rebound20_rate": None,
            "mean_month_gap_ma3": None,
            "mean_month_gap_ma6": None,
            "mean_month_gap_ma12": None,
            "mean_month_close_pos": None,
        }
    long5 = _long_return(frame, 5)
    long10 = _long_return(frame, 10)
    long20 = _long_return(frame, 20)
    long30 = _long_return(frame, 30)
    return {
        "label": label,
        "count": int(len(frame)),
        "mean5": float(long5.mean()),
        "mean10": float(long10.mean()),
        "mean20": float(long20.mean()),
        "win20": float((long20 > 0).mean()),
        "median20": float(long20.median()),
        "mean30": float(long30.mean()),
        "big_rebound20_rate": float((long20 >= float(big_rebound_min)).mean()),
        "mean_month_gap_ma3": float(frame["gap_ma3"].mean()) if "gap_ma3" in frame.columns else None,
        "mean_month_gap_ma6": float(frame["gap_ma6"].mean()) if "gap_ma6" in frame.columns else None,
        "mean_month_gap_ma12": float(frame["gap_ma12"].mean()) if "gap_ma12" in frame.columns else None,
        "mean_month_close_pos": float(frame["close_pos_in_range"].mean()) if "close_pos_in_range" in frame.columns else None,
    }


def _combo_summary(frame: pd.DataFrame, *, label: str, mask: pd.Series, big_rebound_min: float) -> dict[str, Any]:
    subset = frame[mask.fillna(False)].copy()
    payload = _summary(subset, label=label, big_rebound_min=big_rebound_min)
    payload["monthly_capitulation_context_rate"] = float(subset["monthly_capitulation_context"].mean()) if len(subset) else None
    payload["daily_reversal_up_rate"] = float(subset["daily_reversal_up"].mean()) if len(subset) else None
    payload["monthly_ma_reclaim_rate"] = float(subset["monthly_ma_reclaim"].mean()) if len(subset) else None
    return payload


def _build_buy_monthly_rebound_frame(*, config: BuyMonthlyReboundConfig = BuyMonthlyReboundConfig()) -> tuple[pd.DataFrame, int]:
    with get_conn() as conn:
        if not sell_path._table_exists(conn, "signal_decision_daily") or not sell_path._table_exists(conn, "ml_feature_daily"):
            return pd.DataFrame(), 0
        latest_row = conn.execute("SELECT MAX(dt) FROM signal_decision_daily").fetchone()
        if not latest_row or latest_row[0] is None:
            return pd.DataFrame(), 0
        latest_ymd = int(latest_row[0])
        start_dt = datetime.strptime(str(latest_ymd), "%Y%m%d")
        start_ymd = int((start_dt - pd.Timedelta(days=max(1, int(config.lookback_days)))).strftime("%Y%m%d"))
        daily = _load_daily_frame(conn, lookback_days=config.lookback_days)
        frame = _load_buy_signal_frame(conn, start_ymd=start_ymd, end_ymd=latest_ymd)
    if frame.empty or daily.empty:
        return pd.DataFrame(), 0
    monthly = monthly_crash._build_monthly_frame(daily, config=monthly_crash.SellMonthlyCrashConfig(lookback_days=config.lookback_days, report_dir=config.report_dir))  # type: ignore[attr-defined]
    if monthly.empty:
        return pd.DataFrame(), 0
    frame = monthly_crash._merge_monthly_context(frame, monthly)  # type: ignore[attr-defined]
    frame = _augment_features(frame, config=config)
    frame["long_return_5"] = _long_return(frame, 5)
    frame["long_return_10"] = _long_return(frame, 10)
    frame["long_return_20"] = _long_return(frame, 20)
    frame["long_return_30"] = _long_return(frame, 30)
    return frame, int(latest_ymd)


def run_buy_monthly_rebound_relearn(*, config: BuyMonthlyReboundConfig = BuyMonthlyReboundConfig()) -> dict[str, Any]:
    frame, latest_ymd = _build_buy_monthly_rebound_frame(config=config)
    if frame.empty or latest_ymd <= 0:
        return {"ok": False, "reason": "signal_or_monthly_frame_empty" if frame.empty else "signal_frame_empty"}

    buckets = {
        "monthly_rebound_context": frame["monthly_rebound_context"],
        "monthly_capitulation_context": frame["monthly_capitulation_context"],
        "monthly_ma_reclaim": frame["monthly_ma_reclaim"],
        "daily_reversal_up": frame["daily_reversal_up"],
        "daily_hammer_like": frame["daily_hammer_like"],
        "daily_bullish_close": frame["daily_bullish_close"],
        "monthly_bear_stack": frame["monthly_zone"] == "bear_stack",
        "monthly_bear_extension": frame["monthly_zone"] == "bear_extension",
        "monthly_sideways": frame["monthly_zone"] == "sideways",
        "monthly_bear_stack__reversal_up": (frame["monthly_zone"] == "bear_stack") & frame["daily_reversal_up"],
        "monthly_bear_extension__reversal_up": (frame["monthly_zone"] == "bear_extension") & frame["daily_reversal_up"],
        "monthly_sideways__reversal_up": frame["monthly_sideways_context"] & frame["daily_reversal_up"],
        "monthly_bear_stack__ma_reclaim": (frame["monthly_zone"] == "bear_stack") & frame["monthly_ma_reclaim"],
        "monthly_bear_extension__ma_reclaim": (frame["monthly_zone"] == "bear_extension") & frame["monthly_ma_reclaim"],
        "monthly_sideways__ma_reclaim": frame["monthly_sideways_context"] & frame["monthly_ma_reclaim"],
        "monthly_bear_stack__capitulation": (frame["monthly_zone"] == "bear_stack") & frame["monthly_capitulation_context"],
        "monthly_sideways__capitulation": frame["monthly_sideways_context"] & frame["monthly_capitulation_context"],
        "setup_rebound": frame["setup_type"].isin({"rebound", "turn", "breakout"}),
    }
    bucket_rows = [{"bucket": label, **_summary(frame[mask], label=label, big_rebound_min=float(config.big_rebound_min))} for label, mask in buckets.items()]
    bucket_rows = sorted(bucket_rows, key=lambda row: (-int(row["count"]), str(row["bucket"])))

    combo_rows = [
        _combo_summary(frame, label="monthly_bear_stack__reversal_up", mask=buckets["monthly_bear_stack__reversal_up"], big_rebound_min=float(config.big_rebound_min)),
        _combo_summary(frame, label="monthly_bear_extension__reversal_up", mask=buckets["monthly_bear_extension__reversal_up"], big_rebound_min=float(config.big_rebound_min)),
        _combo_summary(frame, label="monthly_sideways__reversal_up", mask=buckets["monthly_sideways__reversal_up"], big_rebound_min=float(config.big_rebound_min)),
        _combo_summary(frame, label="monthly_bear_stack__ma_reclaim", mask=buckets["monthly_bear_stack__ma_reclaim"], big_rebound_min=float(config.big_rebound_min)),
        _combo_summary(frame, label="monthly_bear_extension__ma_reclaim", mask=buckets["monthly_bear_extension__ma_reclaim"], big_rebound_min=float(config.big_rebound_min)),
        _combo_summary(frame, label="monthly_sideways__ma_reclaim", mask=buckets["monthly_sideways__ma_reclaim"], big_rebound_min=float(config.big_rebound_min)),
        _combo_summary(frame, label="monthly_bear_stack__capitulation", mask=buckets["monthly_bear_stack__capitulation"], big_rebound_min=float(config.big_rebound_min)),
        _combo_summary(frame, label="monthly_sideways__capitulation", mask=buckets["monthly_sideways__capitulation"], big_rebound_min=float(config.big_rebound_min)),
        _combo_summary(frame, label="monthly_rebound_context", mask=buckets["monthly_rebound_context"], big_rebound_min=float(config.big_rebound_min)),
    ]
    combo_rows = sorted(combo_rows, key=lambda row: (-float(row.get("mean20") or -999.0), -int(row.get("count") or 0), str(row.get("label"))))

    top_rule_rows = sorted(
        [row for row in bucket_rows if row.get("count") and int(row["count"]) >= int(config.min_rule_count) and row.get("mean20") is not None],
        key=lambda row: (float(row["mean20"]), float(row["big_rebound20_rate"] or 0.0), float(row["win20"] or 0.0), int(row["count"])),
        reverse=True,
    )
    big_rebound_rows = sorted(
        [row for row in bucket_rows if row.get("count") and int(row["count"]) >= int(config.min_rule_count) and row.get("big_rebound20_rate") is not None],
        key=lambda row: (float(row["big_rebound20_rate"]), float(row["mean20"] or 0.0), int(row["count"])),
        reverse=True,
    )

    result = {
        "ok": True,
        "as_of_ymd": latest_ymd,
        "period_start_ymd": int(frame["signal_dt"].min()),
        "period_end_ymd": int(frame["signal_dt"].max()),
        "lookback_days": int(config.lookback_days),
        "row_count": int(len(frame)),
        "overall": _summary(frame, label="all_buy_entries", big_rebound_min=float(config.big_rebound_min)),
        "daily_reversal": {
            "reversal_up": _summary(frame[frame["daily_reversal_up"]], label="daily_reversal_up", big_rebound_min=float(config.big_rebound_min)),
            "hammer_like": _summary(frame[frame["daily_hammer_like"]], label="daily_hammer_like", big_rebound_min=float(config.big_rebound_min)),
            "bullish_close": _summary(frame[frame["daily_bullish_close"]], label="daily_bullish_close", big_rebound_min=float(config.big_rebound_min)),
            "none": _summary(frame[~frame["daily_reversal_up"]], label="daily_no_reversal", big_rebound_min=float(config.big_rebound_min)),
        },
        "monthly_zone": [
            {
                "bucket": f"monthly_zone:{bucket}",
                **_summary(frame[frame["monthly_zone"] == bucket], label=f"monthly_zone:{bucket}", big_rebound_min=float(config.big_rebound_min)),
            }
            for bucket in ["sideways", "bull_stack", "bull_extension", "mid", "bear_extension", "bear_stack", "missing"]
        ],
        "bucket_summary": bucket_rows,
        "combo_summary": combo_rows,
        "big_rebound_rules": big_rebound_rows[:10],
        "top_rules": top_rule_rows[:10],
        "recommendation": top_rule_rows[0] if top_rule_rows else None,
    }
    return result


def _write_markdown_report(result: dict[str, Any], output_path: Path) -> None:
    lines = [
        "# Buy Monthly Rebound Relearn",
        "",
        f"- ok: `{result.get('ok')}`",
        f"- as_of_ymd: `{result.get('as_of_ymd')}`",
        f"- period_start_ymd: `{result.get('period_start_ymd')}`",
        f"- period_end_ymd: `{result.get('period_end_ymd')}`",
        f"- row_count: `{result.get('row_count')}`",
        "",
        "## Overall",
        "",
        "| count | mean5 | mean10 | mean20 | win20 | median20 | big_rebound20_rate | month_gap_ma3 | month_gap_ma6 | month_gap_ma12 |",
        "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    overall = result.get("overall") or {}
    lines.append(
        "| {count} | {mean5} | {mean10} | {mean20} | {win20} | {median20} | {big_rebound20_rate} | {gap3} | {gap6} | {gap12} |".format(
            count=overall.get("count"),
            mean5=sell_path._fmt_pct(overall.get("mean5")),
            mean10=sell_path._fmt_pct(overall.get("mean10")),
            mean20=sell_path._fmt_pct(overall.get("mean20")),
            win20=sell_path._fmt_pct(overall.get("win20")),
            median20=sell_path._fmt_pct(overall.get("median20")),
            big_rebound20_rate=sell_path._fmt_pct(overall.get("big_rebound20_rate")),
            gap3=sell_path._fmt_pct(overall.get("mean_month_gap_ma3")),
            gap6=sell_path._fmt_pct(overall.get("mean_month_gap_ma6")),
            gap12=sell_path._fmt_pct(overall.get("mean_month_gap_ma12")),
        )
    )
    lines += [
        "",
        "## Daily Reversal",
        "",
        "| bucket | count | mean5 | mean10 | mean20 | win20 | big_rebound20_rate |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for key in ("reversal_up", "hammer_like", "bullish_close", "none"):
        subset = (result.get("daily_reversal") or {}).get(key) or {}
        lines.append(
            f"| daily:{key} | {subset.get('count')} | {sell_path._fmt_pct(subset.get('mean5'))} | {sell_path._fmt_pct(subset.get('mean10'))} | {sell_path._fmt_pct(subset.get('mean20'))} | {sell_path._fmt_pct(subset.get('win20'))} | {sell_path._fmt_pct(subset.get('big_rebound20_rate'))} |"
        )
    lines += [
        "",
        "## Monthly Zone",
        "",
        "| bucket | count | mean5 | mean10 | mean20 | win20 | big_rebound20_rate |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in result.get("monthly_zone") or []:
        lines.append(
            f"| {row.get('bucket')} | {row.get('count')} | {sell_path._fmt_pct(row.get('mean5'))} | {sell_path._fmt_pct(row.get('mean10'))} | {sell_path._fmt_pct(row.get('mean20'))} | {sell_path._fmt_pct(row.get('win20'))} | {sell_path._fmt_pct(row.get('big_rebound20_rate'))} |"
        )
    lines += [
        "",
        "## Combo Summary",
        "",
        "| bucket | count | mean20 | win20 | big_rebound20_rate | monthly_capitulation_context_rate | monthly_ma_reclaim_rate |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in result.get("combo_summary") or []:
        lines.append(
            f"| {row.get('label')} | {row.get('count')} | {sell_path._fmt_pct(row.get('mean20'))} | {sell_path._fmt_pct(row.get('win20'))} | {sell_path._fmt_pct(row.get('big_rebound20_rate'))} | {sell_path._fmt_pct(row.get('monthly_capitulation_context_rate'))} | {sell_path._fmt_pct(row.get('monthly_ma_reclaim_rate'))} |"
        )
    lines += [
        "",
        "## Big Rebound Rules",
        "",
        "| bucket | count | mean20 | win20 | big_rebound20_rate |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for row in result.get("big_rebound_rules") or []:
        lines.append(
            f"| {row.get('bucket')} | {row.get('count')} | {sell_path._fmt_pct(row.get('mean20'))} | {sell_path._fmt_pct(row.get('win20'))} | {sell_path._fmt_pct(row.get('big_rebound20_rate'))} |"
        )
    recommendation = result.get("recommendation") or {}
    if recommendation:
        lines += [
            "",
            "## Recommendation",
            "",
            f"- bucket: `{recommendation.get('bucket')}`",
            f"- count: `{recommendation.get('count')}`",
            f"- mean20: `{sell_path._fmt_pct(recommendation.get('mean20'))}`",
            f"- win20: `{sell_path._fmt_pct(recommendation.get('win20'))}`",
            f"- big_rebound20_rate: `{sell_path._fmt_pct(recommendation.get('big_rebound20_rate'))}`",
        ]
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_json_report(result: dict[str, Any], output_path: Path) -> None:
    output_path.write_text(json.dumps(sell_path._jsonable(result), ensure_ascii=False, indent=2), encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Relearn monthly rebound after crash relationships")
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS)
    parser.add_argument("--min-rule-count", type=int, default=20)
    parser.add_argument("--daily-body-min", type=float, default=0.04)
    parser.add_argument("--daily-close-pos-min", type=float, default=0.70)
    parser.add_argument("--daily-lower-wick-min", type=float, default=0.12)
    parser.add_argument("--daily-upper-wick-max", type=float, default=0.12)
    parser.add_argument("--monthly-body-min", type=float, default=0.06)
    parser.add_argument("--big-rebound-min", type=float, default=0.10)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
    parser.add_argument("--prefix", default="buy_monthly_rebound_relearn")
    args = parser.parse_args(argv)
    result = run_buy_monthly_rebound_relearn(
        config=BuyMonthlyReboundConfig(
            lookback_days=int(args.lookback_days),
            report_dir=Path(args.report_dir),
            min_rule_count=int(args.min_rule_count),
            daily_body_min=float(args.daily_body_min),
            daily_close_pos_min=float(args.daily_close_pos_min),
            daily_lower_wick_min=float(args.daily_lower_wick_min),
            daily_upper_wick_max=float(args.daily_upper_wick_max),
            monthly_body_min=float(args.monthly_body_min),
            big_rebound_min=float(args.big_rebound_min),
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
