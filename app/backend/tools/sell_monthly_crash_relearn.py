from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from app.backend.services.analysis.swing_expectancy_service import _to_ymd_expr
from app.backend.tools import sell_path_relearn as sell_path
from app.backend.tools.weekly_top_gainers_study import _load_daily_frame
from app.db.session import get_conn

DEFAULT_LOOKBACK_DAYS = 365 * 3
DEFAULT_REPORT_DIR = Path("G:/Tradex/reports")


@dataclass(frozen=True)
class SellMonthlyCrashConfig:
    lookback_days: int = DEFAULT_LOOKBACK_DAYS
    report_dir: Path = DEFAULT_REPORT_DIR
    min_rule_count: int = 20
    monthly_body_min: float = 0.06
    monthly_close_pos_min: float = 0.75
    monthly_close_pos_max: float = 0.25
    monthly_upper_wick_max: float = 0.15
    monthly_lower_wick_max: float = 0.15
    monthly_sideways_trend_max: float = 0.06
    monthly_sideways_range_ratio_max: float = 0.85
    monthly_extension_min: float = 0.08
    big_drop_min: float = 0.10


def _short_profit(frame: pd.DataFrame, horizon: int) -> pd.Series:
    return -pd.to_numeric(frame[f"forward_return_{int(horizon)}"], errors="coerce")


def _load_sell_signal_frame(conn, *, start_ymd: int, end_ymd: int) -> pd.DataFrame:
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


def _build_monthly_frame(daily: pd.DataFrame, *, config: SellMonthlyCrashConfig) -> pd.DataFrame:
    if daily.empty:
        return pd.DataFrame()
    frame = daily.copy()
    frame["code"] = frame["code"].astype(str).str.strip()
    frame["date_dt"] = pd.to_datetime(frame["date_dt"], errors="coerce")
    frame = frame.dropna(subset=["date_dt", "o", "h", "l", "c"]).copy()
    frame.sort_values(["code", "date_dt"], inplace=True)
    frame["month_start_dt"] = frame["date_dt"].dt.to_period("M").dt.to_timestamp()
    monthly = (
        frame.groupby(["code", "month_start_dt"], as_index=False, sort=True)
        .agg(
            month_last_dt=("date_dt", "max"),
            o=("o", "first"),
            h=("h", "max"),
            l=("l", "min"),
            c=("c", "last"),
            v=("v", "sum"),
            day_count=("date_dt", "count"),
        )
        .sort_values(["code", "month_start_dt"])
        .copy()
    )
    if monthly.empty:
        return monthly

    g = monthly.groupby("code", sort=False)
    monthly["month_start_ymd"] = monthly["month_start_dt"].dt.strftime("%Y%m%d").astype(int)
    monthly["month_last_ymd"] = monthly["month_last_dt"].dt.strftime("%Y%m%d").astype(int)
    monthly["prev_close"] = g["c"].shift(1)
    monthly["trend_3m"] = monthly["c"] / g["c"].shift(3) - 1.0
    monthly["trend_6m"] = monthly["c"] / g["c"].shift(6) - 1.0
    monthly["trend_12m"] = monthly["c"] / g["c"].shift(12) - 1.0
    monthly["ma3"] = g["c"].transform(lambda s: s.rolling(3, min_periods=3).mean())
    monthly["ma6"] = g["c"].transform(lambda s: s.rolling(6, min_periods=6).mean())
    monthly["ma12"] = g["c"].transform(lambda s: s.rolling(12, min_periods=12).mean())
    monthly["prev3_high"] = g["h"].transform(lambda s: s.shift(1).rolling(3, min_periods=3).max())
    monthly["prev3_low"] = g["l"].transform(lambda s: s.shift(1).rolling(3, min_periods=3).min())
    monthly["prev6_high"] = g["h"].transform(lambda s: s.shift(1).rolling(6, min_periods=6).max())
    monthly["prev6_low"] = g["l"].transform(lambda s: s.shift(1).rolling(6, min_periods=6).min())
    monthly["range_pct"] = (monthly["h"] - monthly["l"]) / monthly["c"].replace(0, pd.NA)
    monthly["range_ma3"] = g["range_pct"].transform(lambda s: s.rolling(3, min_periods=3).mean())
    monthly["range_ma6"] = g["range_pct"].transform(lambda s: s.rolling(6, min_periods=6).mean())
    monthly["body_pct"] = (monthly["c"] - monthly["o"]).abs() / monthly["o"].replace(0, pd.NA)
    monthly["upper_wick_pct"] = (monthly["h"] - monthly[["o", "c"]].max(axis=1)) / monthly["o"].replace(0, pd.NA)
    monthly["lower_wick_pct"] = (monthly[["o", "c"]].min(axis=1) - monthly["l"]) / monthly["o"].replace(0, pd.NA)
    monthly["close_pos_in_range"] = (monthly["c"] - monthly["l"]) / (monthly["h"] - monthly["l"]).replace(0, pd.NA)
    monthly["close_above_ma3"] = monthly["c"] > monthly["ma3"]
    monthly["close_above_ma6"] = monthly["c"] > monthly["ma6"]
    monthly["close_above_ma12"] = monthly["c"] > monthly["ma12"]
    monthly["ma3_gt_ma6"] = monthly["ma3"] > monthly["ma6"]
    monthly["ma6_gt_ma12"] = monthly["ma6"] > monthly["ma12"]
    monthly["ma_stack_bull"] = monthly["close_above_ma3"] & monthly["ma3_gt_ma6"] & monthly["ma6_gt_ma12"]
    monthly["ma_stack_bear"] = (~monthly["close_above_ma3"]) & (~monthly["ma3_gt_ma6"]) & (~monthly["ma6_gt_ma12"])
    monthly["breakout_3m_high"] = monthly["c"] > monthly["prev3_high"]
    monthly["breakdown_3m_low"] = monthly["c"] < monthly["prev3_low"]
    monthly["breakout_6m_high"] = monthly["c"] > monthly["prev6_high"]
    monthly["breakdown_6m_low"] = monthly["c"] < monthly["prev6_low"]
    monthly["gap_ma3"] = monthly["c"] / monthly["ma3"] - 1.0
    monthly["gap_ma6"] = monthly["c"] / monthly["ma6"] - 1.0
    monthly["gap_ma12"] = monthly["c"] / monthly["ma12"] - 1.0
    monthly["monthly_extension_up"] = (monthly["gap_ma6"] >= float(config.monthly_extension_min)) | (
        monthly["gap_ma12"] >= float(config.monthly_extension_min)
    )
    monthly["monthly_extension_down"] = (monthly["gap_ma6"] <= -float(config.monthly_extension_min)) | (
        monthly["gap_ma12"] <= -float(config.monthly_extension_min)
    )
    monthly["monthly_bullish_spike"] = (
        (monthly["body_pct"] >= float(config.monthly_body_min))
        & (monthly["close_pos_in_range"] >= float(config.monthly_close_pos_min))
        & (monthly["upper_wick_pct"] <= float(config.monthly_upper_wick_max))
    )
    monthly["monthly_bearish_spike"] = (
        (monthly["body_pct"] >= float(config.monthly_body_min))
        & (monthly["close_pos_in_range"] <= float(config.monthly_close_pos_max))
        & (monthly["lower_wick_pct"] <= float(config.monthly_lower_wick_max))
    )
    monthly["monthly_sideways"] = (
        monthly["trend_3m"].abs() <= float(config.monthly_sideways_trend_max)
    ) & (
        monthly["range_ma3"] <= (float(config.monthly_sideways_range_ratio_max) * monthly["range_ma6"])
    ) & (
        monthly["close_pos_in_range"].between(0.35, 0.65, inclusive="both")
    )
    monthly["monthly_zone"] = "mid"
    monthly.loc[monthly["monthly_sideways"], "monthly_zone"] = "sideways"
    monthly.loc[monthly["ma_stack_bull"], "monthly_zone"] = "bull_stack"
    monthly.loc[monthly["ma_stack_bear"], "monthly_zone"] = "bear_stack"
    monthly.loc[monthly["monthly_extension_up"] & ~monthly["ma_stack_bull"] & ~monthly["monthly_sideways"], "monthly_zone"] = "bull_extension"
    monthly.loc[monthly["monthly_extension_down"] & ~monthly["ma_stack_bear"] & ~monthly["monthly_sideways"], "monthly_zone"] = "bear_extension"
    return monthly


def _merge_monthly_context(frame: pd.DataFrame, monthly: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or monthly.empty:
        return frame.copy()
    left = frame.sort_values(["code", "signal_dt"]).copy()
    right = monthly.sort_values(["code", "month_last_ymd"]).copy()
    merged_frames: list[pd.DataFrame] = []
    right_groups = {code: group.sort_values("month_last_ymd").copy() for code, group in right.groupby("code", sort=False)}
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
            right_on="month_last_ymd",
            direction="backward",
            allow_exact_matches=True,
        )
        merged_frames.append(merged_group)
    if not merged_frames:
        return frame.copy()
    merged = pd.concat(merged_frames, ignore_index=True)
    return merged.sort_values(["code", "signal_dt"]).reset_index(drop=True)


def _augment_features(frame: pd.DataFrame, *, config: SellMonthlyCrashConfig) -> pd.DataFrame:
    out = frame.copy()
    breakdown20 = out["breakdown20_down"].fillna(False).astype(bool) if "breakdown20_down" in out.columns else pd.Series(False, index=out.index)
    market_ret20 = pd.to_numeric(out["market_ret20"], errors="coerce") if "market_ret20" in out.columns else pd.Series(pd.NA, index=out.index, dtype="Float64")
    breadth_above_ma20 = pd.to_numeric(out["breadth_above_ma20"], errors="coerce") if "breadth_above_ma20" in out.columns else pd.Series(pd.NA, index=out.index, dtype="Float64")
    out["daily_range_pct"] = (out["bar_high"] - out["bar_low"]) / out["bar_close"].replace(0, pd.NA)
    out["daily_close_pos_in_range"] = (out["bar_close"] - out["bar_low"]) / (out["bar_high"] - out["bar_low"]).replace(0, pd.NA)
    out["daily_bullish_spike"] = (
        (out["bar_close"] > out["bar_open"])
        & (out["body_ratio"] >= float(config.monthly_body_min))
        & (out["daily_close_pos_in_range"] >= float(config.monthly_close_pos_min))
        & (out["upper_wick_ratio"] <= float(config.monthly_upper_wick_max))
    )
    out["daily_bearish_spike"] = (
        (out["bar_close"] < out["bar_open"])
        & (out["body_ratio"] >= float(config.monthly_body_min))
        & (out["daily_close_pos_in_range"] <= float(config.monthly_close_pos_max))
        & (out["lower_wick_ratio"] <= float(config.monthly_lower_wick_max))
    )
    out["monthly_zone"] = out["monthly_zone"].fillna("missing")
    out["monthly_bullish_context"] = out["monthly_zone"].isin({"bull_stack", "bull_extension"})
    out["monthly_bearish_context"] = out["monthly_zone"].isin({"bear_stack", "bear_extension"})
    out["monthly_sideways_context"] = out["monthly_zone"].eq("sideways")
    out["monthly_breakdown_context"] = (
        out["monthly_bearish_context"]
        | out["monthly_sideways_context"]
        | breakdown20
        | (market_ret20.fillna(0.0) < 0.0)
        | (breadth_above_ma20.fillna(1.0) <= 0.45)
    )
    out["monthly_breakout_context"] = out["monthly_bullish_context"] | out["monthly_extension_up"].fillna(False)
    if "short_profit_20" in out.columns:
        out["monthly_sideways_crash"] = out["monthly_sideways_context"] & (out["short_profit_20"] >= float(config.big_drop_min))
    else:
        out["monthly_sideways_crash"] = False
    out["monthly_bullish_spike_contrarian"] = out["daily_bullish_spike"] & out["monthly_bearish_context"]
    out["monthly_bearish_spike_breakdown"] = out["daily_bearish_spike"] & out["monthly_bearish_context"]
    return out


def _summary(frame: pd.DataFrame, *, label: str, big_drop_min: float) -> dict[str, Any]:
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
            "big_drop20_rate": None,
            "mean_month_gap_ma3": None,
            "mean_month_gap_ma6": None,
            "mean_month_gap_ma12": None,
            "mean_month_close_pos": None,
        }
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
        "big_drop20_rate": float((short20 >= float(big_drop_min)).mean()),
        "mean_month_gap_ma3": float(frame["gap_ma3"].mean()) if "gap_ma3" in frame.columns else None,
        "mean_month_gap_ma6": float(frame["gap_ma6"].mean()) if "gap_ma6" in frame.columns else None,
        "mean_month_gap_ma12": float(frame["gap_ma12"].mean()) if "gap_ma12" in frame.columns else None,
        "mean_month_close_pos": float(frame["close_pos_in_range"].mean()) if "close_pos_in_range" in frame.columns else None,
    }


def _bucket_summary(
    frame: pd.DataFrame,
    *,
    bucket_col: str,
    bucket_order: list[str],
    label_prefix: str,
    big_drop_min: float,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for bucket in bucket_order:
        subset = frame[frame[bucket_col] == bucket]
        rows.append({"bucket": f"{label_prefix}:{bucket}", **_summary(subset, label=f"{label_prefix}:{bucket}", big_drop_min=big_drop_min)})
    return rows


def _combo_summary(frame: pd.DataFrame, *, label: str, mask: pd.Series, big_drop_min: float) -> dict[str, Any]:
    subset = frame[mask.fillna(False)].copy()
    payload = _summary(subset, label=label, big_drop_min=big_drop_min)
    payload["monthly_bullish_context_rate"] = float(subset["monthly_bullish_context"].mean()) if len(subset) else None
    payload["monthly_bearish_context_rate"] = float(subset["monthly_bearish_context"].mean()) if len(subset) else None
    payload["monthly_sideways_context_rate"] = float(subset["monthly_sideways_context"].mean()) if len(subset) else None
    payload["daily_bullish_spike_rate"] = float(subset["daily_bullish_spike"].mean()) if len(subset) else None
    payload["daily_bearish_spike_rate"] = float(subset["daily_bearish_spike"].mean()) if len(subset) else None
    return payload


def run_sell_monthly_crash_relearn(*, config: SellMonthlyCrashConfig = SellMonthlyCrashConfig()) -> dict[str, Any]:
    with get_conn() as conn:
        if not sell_path._table_exists(conn, "signal_decision_daily") or not sell_path._table_exists(conn, "ml_feature_daily"):
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
    monthly = _build_monthly_frame(daily, config=config)
    if monthly.empty:
        return {"ok": False, "reason": "monthly_frame_empty"}
    frame = _merge_monthly_context(frame, monthly)
    frame = _augment_features(frame, config=config)
    frame["short_profit_5"] = _short_profit(frame, 5)
    frame["short_profit_10"] = _short_profit(frame, 10)
    frame["short_profit_20"] = _short_profit(frame, 20)
    frame["short_profit_30"] = _short_profit(frame, 30)

    buckets = {
        "monthly_bull_spike": frame["daily_bullish_spike"],
        "monthly_bear_spike": frame["daily_bearish_spike"],
        "monthly_sideways": frame["monthly_sideways_context"],
        "monthly_bull_stack": frame["monthly_zone"] == "bull_stack",
        "monthly_bull_extension": frame["monthly_zone"] == "bull_extension",
        "monthly_mid": frame["monthly_zone"] == "mid",
        "monthly_bear_extension": frame["monthly_zone"] == "bear_extension",
        "monthly_bear_stack": frame["monthly_zone"] == "bear_stack",
        "monthly_sideways__breakdown": frame["monthly_sideways_context"] & (frame["setup_type"] == "breakdown"),
        "monthly_sideways__pressure": frame["monthly_sideways_context"] & (frame["setup_type"] == "pressure"),
        "monthly_bear_extension__breakdown": (frame["monthly_zone"] == "bear_extension") & (frame["setup_type"] == "breakdown"),
        "monthly_bear_stack__breakdown": (frame["monthly_zone"] == "bear_stack") & (frame["setup_type"] == "breakdown"),
        "monthly_bear_spike__breakdown": frame["daily_bearish_spike"] & (frame["setup_type"] == "breakdown"),
        "monthly_bull_stack__bearish_spike": (frame["monthly_zone"] == "bull_stack") & frame["daily_bearish_spike"],
        "monthly_bull_extension__bearish_spike": (frame["monthly_zone"] == "bull_extension") & frame["daily_bearish_spike"],
        "monthly_sideways__bearish_spike": frame["monthly_sideways_context"] & frame["daily_bearish_spike"],
        "monthly_bull_spike__pressure": frame["daily_bullish_spike"] & (frame["setup_type"] == "pressure"),
        "monthly_bull_spike__breakdown": frame["daily_bullish_spike"] & (frame["setup_type"] == "breakdown"),
        "breakdown_setup": frame["setup_type"] == "breakdown",
        "pressure_setup": frame["setup_type"] == "pressure",
    }
    bucket_rows = [{"bucket": label, **_summary(frame[mask], label=label, big_drop_min=float(config.big_drop_min))} for label, mask in buckets.items()]
    bucket_rows = sorted(bucket_rows, key=lambda row: (-int(row["count"]), str(row["bucket"])))

    combo_rows = [
        _combo_summary(frame, label="monthly_mid", mask=frame["monthly_zone"] == "mid", big_drop_min=float(config.big_drop_min)),
        _combo_summary(frame, label="monthly_sideways__breakdown", mask=buckets["monthly_sideways__breakdown"], big_drop_min=float(config.big_drop_min)),
        _combo_summary(frame, label="monthly_sideways__pressure", mask=buckets["monthly_sideways__pressure"], big_drop_min=float(config.big_drop_min)),
        _combo_summary(frame, label="monthly_bear_extension__breakdown", mask=buckets["monthly_bear_extension__breakdown"], big_drop_min=float(config.big_drop_min)),
        _combo_summary(frame, label="monthly_bear_stack__breakdown", mask=buckets["monthly_bear_stack__breakdown"], big_drop_min=float(config.big_drop_min)),
        _combo_summary(frame, label="monthly_bear_spike__breakdown", mask=buckets["monthly_bear_spike__breakdown"], big_drop_min=float(config.big_drop_min)),
        _combo_summary(frame, label="monthly_bull_stack__bearish_spike", mask=buckets["monthly_bull_stack__bearish_spike"], big_drop_min=float(config.big_drop_min)),
        _combo_summary(frame, label="monthly_bull_extension__bearish_spike", mask=buckets["monthly_bull_extension__bearish_spike"], big_drop_min=float(config.big_drop_min)),
        _combo_summary(frame, label="monthly_sideways__bearish_spike", mask=buckets["monthly_sideways__bearish_spike"], big_drop_min=float(config.big_drop_min)),
        _combo_summary(frame, label="monthly_bull_spike__pressure", mask=buckets["monthly_bull_spike__pressure"], big_drop_min=float(config.big_drop_min)),
        _combo_summary(frame, label="monthly_bull_spike__breakdown", mask=buckets["monthly_bull_spike__breakdown"], big_drop_min=float(config.big_drop_min)),
    ]
    combo_rows = sorted(combo_rows, key=lambda row: (-float(row.get("mean20") or -999.0), -int(row.get("count") or 0), str(row.get("label"))))

    top_rule_rows = sorted(
        [row for row in bucket_rows if row.get("count") and int(row["count"]) >= int(config.min_rule_count) and row.get("mean20") is not None],
        key=lambda row: (float(row["mean20"]), float(row["big_drop20_rate"] or 0.0), float(row["win20"] or 0.0), int(row["count"])),
        reverse=True,
    )
    big_drop_rows = sorted(
        [row for row in bucket_rows if row.get("count") and int(row["count"]) >= int(config.min_rule_count) and row.get("big_drop20_rate") is not None],
        key=lambda row: (float(row["big_drop20_rate"]), float(row["mean20"] or 0.0), int(row["count"])),
        reverse=True,
    )

    result = {
        "ok": True,
        "as_of_ymd": latest_ymd,
        "period_start_ymd": int(frame["signal_dt"].min()),
        "period_end_ymd": int(frame["signal_dt"].max()),
        "lookback_days": int(config.lookback_days),
        "row_count": int(len(frame)),
        "overall": _summary(frame, label="all_sell_entries", big_drop_min=float(config.big_drop_min)),
        "daily_spike": {
            "bullish": _summary(frame[frame["daily_bullish_spike"]], label="daily_bullish_spike", big_drop_min=float(config.big_drop_min)),
            "bearish": _summary(frame[frame["daily_bearish_spike"]], label="daily_bearish_spike", big_drop_min=float(config.big_drop_min)),
            "none": _summary(frame[~(frame["daily_bullish_spike"] | frame["daily_bearish_spike"])], label="daily_no_spike", big_drop_min=float(config.big_drop_min)),
        },
        "monthly_zone": _bucket_summary(
            frame,
            bucket_col="monthly_zone",
            bucket_order=["sideways", "bull_stack", "bull_extension", "mid", "bear_extension", "bear_stack", "missing"],
            label_prefix="monthly_zone",
            big_drop_min=float(config.big_drop_min),
        ),
        "bucket_summary": bucket_rows,
        "combo_summary": combo_rows,
        "big_drop_rules": big_drop_rows[:10],
        "top_rules": top_rule_rows[:10],
        "recommendation": top_rule_rows[0] if top_rule_rows else None,
    }
    return result


def _write_markdown_report(result: dict[str, Any], output_path: Path) -> None:
    lines = [
        "# Sell Monthly Crash Relearn",
        "",
        f"- ok: `{result.get('ok')}`",
        f"- as_of_ymd: `{result.get('as_of_ymd')}`",
        f"- period_start_ymd: `{result.get('period_start_ymd')}`",
        f"- period_end_ymd: `{result.get('period_end_ymd')}`",
        f"- row_count: `{result.get('row_count')}`",
        "",
        "## Overall",
        "",
        "| count | mean5 | mean10 | mean20 | win20 | median20 | big_drop20_rate | month_gap_ma3 | month_gap_ma6 | month_gap_ma12 |",
        "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    overall = result.get("overall") or {}
    lines.append(
        "| {count} | {mean5} | {mean10} | {mean20} | {win20} | {median20} | {big_drop20_rate} | {gap3} | {gap6} | {gap12} |".format(
            count=overall.get("count"),
            mean5=sell_path._fmt_pct(overall.get("mean5")),
            mean10=sell_path._fmt_pct(overall.get("mean10")),
            mean20=sell_path._fmt_pct(overall.get("mean20")),
            win20=sell_path._fmt_pct(overall.get("win20")),
            median20=sell_path._fmt_pct(overall.get("median20")),
            big_drop20_rate=sell_path._fmt_pct(overall.get("big_drop20_rate")),
            gap3=sell_path._fmt_pct(overall.get("mean_month_gap_ma3")),
            gap6=sell_path._fmt_pct(overall.get("mean_month_gap_ma6")),
            gap12=sell_path._fmt_pct(overall.get("mean_month_gap_ma12")),
        )
    )
    lines += [
        "",
        "## Daily Spikes",
        "",
        "| bucket | count | mean5 | mean10 | mean20 | win20 | big_drop20_rate |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for key in ("bullish", "bearish", "none"):
        subset = (result.get("daily_spike") or {}).get(key) or {}
        lines.append(
            f"| daily:{key} | {subset.get('count')} | {sell_path._fmt_pct(subset.get('mean5'))} | {sell_path._fmt_pct(subset.get('mean10'))} | {sell_path._fmt_pct(subset.get('mean20'))} | {sell_path._fmt_pct(subset.get('win20'))} | {sell_path._fmt_pct(subset.get('big_drop20_rate'))} |"
        )
    lines += [
        "",
        "## Monthly Zone",
        "",
        "| bucket | count | mean5 | mean10 | mean20 | win20 | big_drop20_rate |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in result.get("monthly_zone") or []:
        lines.append(
            f"| {row.get('bucket')} | {row.get('count')} | {sell_path._fmt_pct(row.get('mean5'))} | {sell_path._fmt_pct(row.get('mean10'))} | {sell_path._fmt_pct(row.get('mean20'))} | {sell_path._fmt_pct(row.get('win20'))} | {sell_path._fmt_pct(row.get('big_drop20_rate'))} |"
        )
    lines += [
        "",
        "## Combo Summary",
        "",
        "| bucket | count | mean20 | win20 | big_drop20_rate | monthly_sideways_context_rate |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in result.get("combo_summary") or []:
        lines.append(
            f"| {row.get('label')} | {row.get('count')} | {sell_path._fmt_pct(row.get('mean20'))} | {sell_path._fmt_pct(row.get('win20'))} | {sell_path._fmt_pct(row.get('big_drop20_rate'))} | {sell_path._fmt_pct(row.get('monthly_sideways_context_rate'))} |"
        )
    lines += [
        "",
        "## Big Drop Rules",
        "",
        "| bucket | count | mean20 | win20 | big_drop20_rate |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for row in result.get("big_drop_rules") or []:
        lines.append(
            f"| {row.get('bucket')} | {row.get('count')} | {sell_path._fmt_pct(row.get('mean20'))} | {sell_path._fmt_pct(row.get('win20'))} | {sell_path._fmt_pct(row.get('big_drop20_rate'))} |"
        )
    lines += [
        "",
        "## Top Rules",
        "",
        "| bucket | count | mean20 | win20 | big_drop20_rate | monthly_sideways_context_rate |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in result.get("top_rules") or []:
        lines.append(
            f"| {row.get('bucket')} | {row.get('count')} | {sell_path._fmt_pct(row.get('mean20'))} | {sell_path._fmt_pct(row.get('win20'))} | {sell_path._fmt_pct(row.get('big_drop20_rate'))} | {sell_path._fmt_pct(row.get('monthly_sideways_context_rate'))} |"
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
            f"- big_drop20_rate: `{sell_path._fmt_pct(recommendation.get('big_drop20_rate'))}`",
        ]
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_json_report(result: dict[str, Any], output_path: Path) -> None:
    output_path.write_text(json.dumps(sell_path._jsonable(result), ensure_ascii=False, indent=2), encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Relearn monthly spike and sideways-crash sell relationships")
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS)
    parser.add_argument("--min-rule-count", type=int, default=20)
    parser.add_argument("--monthly-body-min", type=float, default=0.06)
    parser.add_argument("--monthly-close-pos-min", type=float, default=0.75)
    parser.add_argument("--monthly-close-pos-max", type=float, default=0.25)
    parser.add_argument("--monthly-upper-wick-max", type=float, default=0.15)
    parser.add_argument("--monthly-lower-wick-max", type=float, default=0.15)
    parser.add_argument("--monthly-sideways-trend-max", type=float, default=0.06)
    parser.add_argument("--monthly-sideways-range-ratio-max", type=float, default=0.85)
    parser.add_argument("--monthly-extension-min", type=float, default=0.08)
    parser.add_argument("--big-drop-min", type=float, default=0.10)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
    parser.add_argument("--prefix", default="sell_monthly_crash_relearn")
    args = parser.parse_args(argv)
    result = run_sell_monthly_crash_relearn(
        config=SellMonthlyCrashConfig(
            lookback_days=int(args.lookback_days),
            report_dir=Path(args.report_dir),
            min_rule_count=int(args.min_rule_count),
            monthly_body_min=float(args.monthly_body_min),
            monthly_close_pos_min=float(args.monthly_close_pos_min),
            monthly_close_pos_max=float(args.monthly_close_pos_max),
            monthly_upper_wick_max=float(args.monthly_upper_wick_max),
            monthly_lower_wick_max=float(args.monthly_lower_wick_max),
            monthly_sideways_trend_max=float(args.monthly_sideways_trend_max),
            monthly_sideways_range_ratio_max=float(args.monthly_sideways_range_ratio_max),
            monthly_extension_min=float(args.monthly_extension_min),
            big_drop_min=float(args.big_drop_min),
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
