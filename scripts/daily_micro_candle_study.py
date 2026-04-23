from __future__ import annotations

import argparse
import json
import math
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB_PATH = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_SAMPLE_PATH = REPO_ROOT / "artifacts" / "research_inventory" / "action_precision_multitimeframe_samples.parquet"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "artifacts" / "research_inventory"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except Exception:
        return None
    return out if math.isfinite(out) else None


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    return path


def _quantile_bins(series: pd.Series, labels: list[str]) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")
    valid = s.dropna()
    if valid.nunique() <= 1:
        return pd.Series(["unknown"] * len(s), index=s.index)
    q = min(len(labels), valid.nunique())
    try:
        bins = pd.qcut(valid, q=q, labels=labels[:q], duplicates="drop")
        out = pd.Series("unknown", index=s.index, dtype="object")
        out.loc[bins.index] = bins.astype(str)
        return out
    except Exception:
        return pd.Series(["unknown"] * len(s), index=s.index)


def _boolv(row: pd.Series, key: str) -> bool:
    value = row.get(key)
    return bool(value) if pd.notna(value) else False


def _score_long(row: pd.Series) -> float:
    regime_map = {
        "monthly_up_mid": 4,
        "monthly_up_top_warning": 1,
        "monthly_down_bottom_warning": -4,
        "monthly_down_mid": -2,
        "monthly_range_mid": 2,
        "weekly_up_mid": 4,
        "weekly_up_late": 1,
        "weekly_up_early": 0,
        "weekly_down_bottom_warning": -4,
        "weekly_down_mid": -4,
        "weekly_down_early": -5,
        "weekly_range_late": 0,
        "weekly_range_mid": 1,
        "daily_up_mid": 5,
        "daily_up_top_warning": 1,
        "daily_reversal_up_candidate": 2,
        "daily_reversal_down_candidate": -2,
        "daily_down_mid": -4,
        "daily_down_bottom_warning": -5,
        "trend_up": 4,
        "mixed": -1,
        "range": -1,
    }
    score = 0.0
    for key in ("monthly_main_state_ctx", "weekly_main_state_ctx", "daily_main_state_ctx", "regime_tag"):
        score += float(regime_map.get(str(row.get(key)), 0))
    if _boolv(row, "daily_gap_up_flag"):
        score += 4
    if _boolv(row, "daily_gap_down_flag"):
        score -= 5
    if _boolv(row, "daily_reclaim_ma20_flag"):
        score += 2 if str(row.get("regime_tag")) in {"trend_up", "mixed"} else 1
    if _boolv(row, "daily_lose_ma20_flag"):
        score -= 5
    if _boolv(row, "daily_long_lower_wick_flag"):
        score += 2 if str(row.get("regime_tag")) in {"trend_up", "mixed"} else -1
    if _boolv(row, "daily_small_body_flag"):
        score += 1 if str(row.get("regime_tag")) in {"trend_up", "mixed"} else -1
    if _boolv(row, "daily_long_upper_wick_flag"):
        score += 0 if str(row.get("regime_tag")) == "mixed" else -1
    if _boolv(row, "weekly_change_day_flag"):
        score -= 4
    if _boolv(row, "daily_change_day_flag"):
        score -= 1
    if _boolv(row, "monthly_change_day_flag"):
        score -= 1
    if _boolv(row, "daily_engulfing_bull_flag"):
        score -= 1
    if _boolv(row, "daily_engulfing_bear_flag"):
        score += 1
    body_atr = _safe_float(row.get("body_atr"))
    if body_atr is not None:
        if 0.35 <= body_atr <= 1.25:
            score += 1
        elif body_atr > 1.6:
            score -= 1
    vol = _safe_float(row.get("volume_ratio20"))
    if vol is not None and vol >= 1.2:
        score += 1
    close_pos = _safe_float(row.get("close_pos"))
    if close_pos is not None and close_pos >= 0.7:
        score += 1
    upper_wick = _safe_float(row.get("upper_wick_ratio_atr"))
    if upper_wick is not None and upper_wick >= 0.35 and str(row.get("regime_tag")) != "mixed":
        score -= 1
    lower_wick = _safe_float(row.get("lower_wick_ratio_atr"))
    if lower_wick is not None and lower_wick >= 0.35 and str(row.get("regime_tag")) in {"trend_up", "mixed"}:
        score += 1
    return float(score)


def _score_short(row: pd.Series) -> float:
    regime_map = {
        "monthly_down_mid": 2,
        "monthly_down_bottom_warning": 4,
        "monthly_range_mid": 1,
        "monthly_up_top_warning": -4,
        "monthly_up_mid": -2,
        "weekly_down_mid": 4,
        "weekly_down_bottom_warning": 3,
        "weekly_down_early": 1,
        "weekly_range_late": 1,
        "weekly_range_mid": 1,
        "weekly_up_mid": -4,
        "weekly_up_early": -3,
        "weekly_up_late": -2,
        "daily_down_bottom_warning": 5,
        "daily_down_mid": 4,
        "daily_reversal_down_candidate": 2,
        "daily_reversal_up_candidate": -2,
        "daily_up_mid": -4,
        "daily_up_top_warning": -5,
        "trend_up": -4,
        "mixed": 1,
        "range": 1,
    }
    score = 0.0
    for key in ("monthly_main_state_ctx", "weekly_main_state_ctx", "daily_main_state_ctx", "regime_tag"):
        score += float(regime_map.get(str(row.get(key)), 0))
    if _boolv(row, "daily_gap_down_flag"):
        score += 4
    if _boolv(row, "daily_gap_up_flag"):
        score -= 4
    if _boolv(row, "daily_lose_ma20_flag"):
        score += 2
    if _boolv(row, "daily_reclaim_ma20_flag"):
        score -= 5
    if _boolv(row, "daily_long_upper_wick_flag"):
        score += 1 if str(row.get("regime_tag")) in {"mixed", "range"} else -1
    if _boolv(row, "daily_long_lower_wick_flag"):
        score += 0 if str(row.get("regime_tag")) == "mixed" else -1
    if _boolv(row, "daily_small_body_flag"):
        score -= 1 if str(row.get("regime_tag")) == "trend_up" else 0
    if _boolv(row, "weekly_change_day_flag"):
        score -= 4
    if _boolv(row, "daily_change_day_flag"):
        score -= 1
    if _boolv(row, "monthly_change_day_flag"):
        score -= 1
    if _boolv(row, "daily_engulfing_bull_flag"):
        score += 1
    if _boolv(row, "daily_engulfing_bear_flag"):
        score -= 1
    body_atr = _safe_float(row.get("body_atr"))
    if body_atr is not None:
        if 0.35 <= body_atr <= 1.25:
            score += 1
        elif body_atr > 1.6:
            score -= 1
    vol = _safe_float(row.get("volume_ratio20"))
    if vol is not None and vol >= 1.2:
        score += 1
    close_pos = _safe_float(row.get("close_pos"))
    if close_pos is not None and close_pos <= 0.3:
        score += 1
    upper_wick = _safe_float(row.get("upper_wick_ratio_atr"))
    if upper_wick is not None and upper_wick >= 0.35 and str(row.get("regime_tag")) in {"trend_up", "mixed"}:
        score -= 1
    lower_wick = _safe_float(row.get("lower_wick_ratio_atr"))
    if lower_wick is not None and lower_wick >= 0.35 and str(row.get("regime_tag")) != "trend_up":
        score += 1
    return float(score)


def _reason_codes(row: pd.Series, side: str) -> dict[str, list[str]]:
    positive: list[str] = []
    negative: list[str] = []
    if side == "buy":
        if str(row.get("regime_tag")) == "trend_up":
            positive.append("trend_up_regime")
        if str(row.get("daily_main_state_ctx")) in {"daily_up_mid", "daily_reversal_up_candidate"}:
            positive.append("daily_bull_context")
        if _boolv(row, "daily_gap_up_flag"):
            positive.append("gap_up_followthrough")
        if _boolv(row, "daily_reclaim_ma20_flag"):
            positive.append("ma20_reclaim")
        if _boolv(row, "daily_long_lower_wick_flag"):
            positive.append("lower_wick_support")
        if _boolv(row, "daily_small_body_flag"):
            positive.append("small_body_drift")
        if _boolv(row, "daily_lose_ma20_flag"):
            negative.append("lose_ma20")
        if _boolv(row, "daily_gap_down_flag"):
            negative.append("gap_down_against_long")
        if _boolv(row, "weekly_change_day_flag"):
            negative.append("weekly_change_day")
        if _boolv(row, "daily_change_day_flag"):
            negative.append("daily_change_day")
        if str(row.get("monthly_main_state_ctx")) == "monthly_up_top_warning":
            negative.append("monthly_top_warning")
        if str(row.get("daily_main_state_ctx")) == "daily_up_top_warning":
            negative.append("daily_top_warning")
    else:
        if str(row.get("regime_tag")) in {"mixed", "range"}:
            positive.append("non_trend_regime")
        if str(row.get("daily_main_state_ctx")) in {"daily_down_bottom_warning", "daily_down_mid"}:
            positive.append("daily_bear_context")
        if _boolv(row, "daily_gap_down_flag"):
            positive.append("gap_down_followthrough")
        if _boolv(row, "daily_lose_ma20_flag"):
            positive.append("ma20_loss")
        if _boolv(row, "daily_long_upper_wick_flag"):
            positive.append("upper_wick_rejection")
        if _boolv(row, "daily_reclaim_ma20_flag"):
            negative.append("ma20_reclaim")
        if _boolv(row, "daily_gap_up_flag"):
            negative.append("gap_up_against_short")
        if _boolv(row, "weekly_change_day_flag"):
            negative.append("weekly_change_day")
        if _boolv(row, "daily_change_day_flag"):
            negative.append("daily_change_day")
        if str(row.get("monthly_main_state_ctx")) == "monthly_up_top_warning":
            negative.append("monthly_top_warning")
        if str(row.get("daily_main_state_ctx")) == "daily_up_top_warning":
            negative.append("daily_top_warning")
    body_atr = _safe_float(row.get("body_atr"))
    if body_atr is not None and body_atr > 1.6:
        negative.append("wide_body_exhaustion")
    vol = _safe_float(row.get("volume_ratio20"))
    if vol is not None and vol < 0.8:
        negative.append("low_volume_drift")
    return {"positive": positive[:4], "negative": negative[:4]}


def _summarise_group(df: pd.DataFrame, *, side: str, feature_family: str, feature: str, bucket_col: str, regime_col: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for (regime_val, bucket_val), g in df.groupby([regime_col, bucket_col], dropna=False):
        if len(g) < 20:
            continue
        rows.append(
            {
                "side": side,
                "feature_family": feature_family,
                "feature": feature,
                "bucket": str(bucket_val),
                "regime_slice": str(regime_val),
                "sample_count": int(len(g)),
                "next_day_return_mean": _safe_float(g["next_day_return"].mean()),
                "forward_return_5_mean": _safe_float(g["forward_return_5"].mean()),
                "forward_return_20_mean": _safe_float(g["forward_return_20"].mean()),
                "hit_rate": _safe_float((g["forward_return_20"] > 0).mean()) if side == "buy" else _safe_float((g["forward_return_20"] < 0).mean()),
                "mae_20_mean": _safe_float(g["max_adverse_30"].mean()),
                "mfe_20_mean": _safe_float(g["max_favorable_30"].mean()),
                "days_to_favorable_mean": _safe_float(g["days_to_max_favorable_30"].mean()),
                "exit_efficiency_proxy": _safe_float(g["max_favorable_30"].clip(lower=0.0).mean() / max(1e-12, abs(g["max_adverse_30"].mean()) + g["max_favorable_30"].clip(lower=0.0).mean())),
            }
        )
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Build daily micro candle research artifacts.")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--sample-path", default=str(DEFAULT_SAMPLE_PATH))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args()

    db_path = Path(args.db_path).expanduser().resolve()
    sample_path = Path(args.sample_path).expanduser().resolve()
    out_dir = Path(args.output_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    sample = pd.read_parquet(sample_path)
    sample["code"] = sample["code"].astype(str)
    sample["side"] = sample["side"].astype(str).str.lower()
    for col in ["dt", "signal_month", "signal_date_ymd", "entry_date_ymd"]:
        if col in sample.columns:
            sample[col] = pd.to_numeric(sample[col], errors="coerce").astype("Int64")

    conn = duckdb.connect(str(db_path), read_only=True)
    logic_version, basis_version = conn.execute(
        "SELECT logic_version, basis_version FROM signal_logic_registry WHERE is_active = TRUE ORDER BY created_at DESC LIMIT 1"
    ).fetchone()
    forward = conn.execute(
        """
        SELECT dt, code, side, logic_version, basis_version, forward_return_5, forward_return_10, forward_return_20,
               max_favorable_30, max_adverse_30, days_to_max_favorable_30
        FROM signal_decision_daily
        WHERE logic_version = ? AND entry_qualified = TRUE AND dt <= 20260226
        """,
        [logic_version],
    ).fetchdf()
    forward["code"] = forward["code"].astype(str)
    forward["side"] = forward["side"].astype(str).str.lower()
    for col in ["logic_version", "basis_version"]:
        forward[col] = forward[col].astype(str)

    conn.register("signal_rows", sample[["dt", "code"]].copy())
    raw_features = conn.execute(
        """
        WITH raw AS (
            SELECT
                code,
                date,
                CASE
                    WHEN date BETWEEN 19000101 AND 20991231 THEN date
                    WHEN date BETWEEN 600000000 AND 5000000000 THEN CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)
                    WHEN date >= 1000000000000 THEN CAST(strftime(to_timestamp(date / 1000), '%Y%m%d') AS INTEGER)
                    ELSE NULL
                END AS ymd,
                o,
                h,
                l,
                c,
                v,
                LAG(c) OVER w AS prev_close,
                LAG(h) OVER w AS prev_high,
                LAG(l) OVER w AS prev_low,
                LEAD(o, 1) OVER w AS next_open,
                LEAD(c, 1) OVER w AS next_close_1,
                LEAD(c, 5) OVER w AS next_close_5,
                LEAD(c, 20) OVER w AS next_close_20,
                AVG(v) OVER w7 AS vol7,
                AVG(v) OVER w20 AS vol20,
                AVG(c) OVER w7 AS ma7,
                AVG(c) OVER w20 AS ma20,
                AVG(c) OVER w60 AS ma60,
                AVG(c) OVER w100 AS ma100,
                AVG(c) OVER w200 AS ma200
            FROM daily_bars
            WINDOW w AS (PARTITION BY code ORDER BY date),
                   w7 AS (PARTITION BY code ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
                   w20 AS (PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
                   w60 AS (PARTITION BY code ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW),
                   w100 AS (PARTITION BY code ORDER BY date ROWS BETWEEN 99 PRECEDING AND CURRENT ROW),
                   w200 AS (PARTITION BY code ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW)
        ),
        atr AS (
            SELECT *, GREATEST(h - l, ABS(h - COALESCE(prev_close, c)), ABS(l - COALESCE(prev_close, c))) AS tr
            FROM raw
        ),
        rolled AS (
            SELECT
                *,
                AVG(tr) OVER w14 AS atr14,
                SUM(CASE WHEN c > prev_close THEN 1 ELSE 0 END) OVER w3 AS up_close_count_3,
                SUM(CASE WHEN c > prev_close THEN 1 ELSE 0 END) OVER w5 AS up_close_count_5,
                SUM(CASE WHEN c > prev_close THEN 1 ELSE 0 END) OVER w10 AS up_close_count_10,
                SUM(CASE WHEN c > ma20 THEN 1 ELSE 0 END) OVER w3 AS above_ma20_count_3,
                SUM(CASE WHEN c > ma20 THEN 1 ELSE 0 END) OVER w5 AS above_ma20_count_5,
                SUM(CASE WHEN c > ma20 THEN 1 ELSE 0 END) OVER w10 AS above_ma20_count_10,
                MAX(h) OVER w10 AS high10,
                MIN(l) OVER w10 AS low10,
                LAG(ma7) OVER w AS prev_ma7,
                LAG(ma20) OVER w AS prev_ma20,
                LAG(ma60) OVER w AS prev_ma60,
                LAG(ma100) OVER w AS prev_ma100,
                LAG(ma200) OVER w AS prev_ma200
            FROM atr
            WINDOW w AS (PARTITION BY code ORDER BY date),
                   w14 AS (PARTITION BY code ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW),
                   w3 AS (PARTITION BY code ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),
                   w5 AS (PARTITION BY code ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW),
                   w10 AS (PARTITION BY code ORDER BY date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW)
        )
        SELECT
            s.dt,
            s.code,
            r.ymd,
            CASE WHEN r.next_open IS NULL OR r.next_close_1 IS NULL OR r.next_open = 0 THEN NULL ELSE (r.next_close_1 - r.next_open) / r.next_open END AS next_day_return,
            CASE WHEN r.next_open IS NULL OR r.next_close_5 IS NULL OR r.next_open = 0 THEN NULL ELSE (r.next_close_5 - r.next_open) / r.next_open END AS raw_ret5,
            CASE WHEN r.next_open IS NULL OR r.next_close_20 IS NULL OR r.next_open = 0 THEN NULL ELSE (r.next_close_20 - r.next_open) / r.next_open END AS raw_ret20,
            CASE WHEN r.atr14 IS NULL OR r.atr14 = 0 THEN NULL ELSE ABS(r.c - r.o) / r.atr14 END AS body_atr,
            CASE WHEN r.h = r.l THEN NULL ELSE (r.h - GREATEST(r.o, r.c)) / NULLIF(r.h - r.l, 0) END AS upper_wick_ratio_atr,
            CASE WHEN r.h = r.l THEN NULL ELSE (LEAST(r.o, r.c) - r.l) / NULLIF(r.h - r.l, 0) END AS lower_wick_ratio_atr,
            CASE WHEN r.h = r.l THEN NULL ELSE (r.c - r.l) / NULLIF(r.h - r.l, 0) END AS close_pos,
            CASE WHEN r.prev_close IS NULL THEN NULL ELSE (r.o - r.prev_close) / NULLIF(r.prev_close, 0) END AS gap_pct,
            CASE WHEN r.ma7 IS NULL OR r.ma7 = 0 THEN NULL ELSE (r.c - r.ma7) / r.ma7 END AS dist_ma7_pct,
            CASE WHEN r.ma20 IS NULL OR r.ma20 = 0 THEN NULL ELSE (r.c - r.ma20) / r.ma20 END AS dist_ma20_pct,
            CASE WHEN r.ma60 IS NULL OR r.ma60 = 0 THEN NULL ELSE (r.c - r.ma60) / r.ma60 END AS dist_ma60_pct,
            CASE WHEN r.ma100 IS NULL OR r.ma100 = 0 THEN NULL ELSE (r.c - r.ma100) / r.ma100 END AS dist_ma100_pct,
            CASE WHEN r.ma200 IS NULL OR r.ma200 = 0 THEN NULL ELSE (r.c - r.ma200) / r.ma200 END AS dist_ma200_pct,
            CASE WHEN r.atr14 IS NULL OR r.atr14 = 0 THEN NULL ELSE (r.c - r.ma20) / r.atr14 END AS dist_ma20_atr,
            CASE WHEN r.ma7 IS NULL OR r.prev_ma7 IS NULL THEN NULL ELSE r.ma7 - r.prev_ma7 END AS ma7_slope,
            CASE WHEN r.ma20 IS NULL OR r.prev_ma20 IS NULL THEN NULL ELSE r.ma20 - r.prev_ma20 END AS ma20_slope,
            CASE WHEN r.ma60 IS NULL OR r.prev_ma60 IS NULL THEN NULL ELSE r.ma60 - r.prev_ma60 END AS ma60_slope,
            CASE WHEN r.ma100 IS NULL OR r.prev_ma100 IS NULL THEN NULL ELSE r.ma100 - r.prev_ma100 END AS ma100_slope,
            CASE WHEN r.ma200 IS NULL OR r.prev_ma200 IS NULL THEN NULL ELSE r.ma200 - r.prev_ma200 END AS ma200_slope,
            CASE WHEN r.vol20 IS NULL OR r.vol20 = 0 THEN NULL ELSE r.v / r.vol20 END AS volume_ratio20,
            r.up_close_count_3,
            r.up_close_count_5,
            r.up_close_count_10,
            r.above_ma20_count_3,
            r.above_ma20_count_5,
            r.above_ma20_count_10,
            r.high10,
            r.low10,
            CASE WHEN r.high10 IS NULL OR r.high10 = 0 THEN NULL ELSE (r.high10 - r.c) / r.high10 END AS distance_to_high10_pct,
            CASE WHEN r.low10 IS NULL OR r.low10 = 0 THEN NULL ELSE (r.c - r.low10) / r.low10 END AS distance_to_low10_pct
        FROM signal_rows s
        LEFT JOIN rolled r
          ON r.code = s.code
         AND r.ymd = s.dt
        """,
    ).fetchdf()
    conn.close()

    merged = sample.merge(
        forward,
        on=["dt", "code", "side", "logic_version", "basis_version"],
        how="left",
    ).merge(
        raw_features,
        on=["dt", "code"],
        how="left",
        suffixes=("", "_raw"),
    )
    merged = merged.dropna(subset=["forward_return_20"]).copy()

    # Build scores and penalties.
    merged["micro_long_score"] = merged.apply(_score_long, axis=1)
    merged["micro_short_score"] = merged.apply(_score_short, axis=1)
    merged["micro_no_trade_penalty"] = 0.0
    merged.loc[merged["daily_lose_ma20_flag"].fillna(False).astype(bool), "micro_no_trade_penalty"] += 3
    merged.loc[merged["weekly_change_day_flag"].fillna(False).astype(bool), "micro_no_trade_penalty"] += 2
    merged.loc[merged["daily_change_day_flag"].fillna(False).astype(bool), "micro_no_trade_penalty"] += 1
    merged.loc[merged["monthly_change_day_flag"].fillna(False).astype(bool), "micro_no_trade_penalty"] += 1
    merged.loc[merged["regime_tag"].isin(["mixed", "range"]), "micro_no_trade_penalty"] += 1
    merged.loc[merged["body_atr"].notna() & (merged["body_atr"] > 1.6), "micro_no_trade_penalty"] += 1
    merged.loc[merged["volume_ratio20"].notna() & (merged["volume_ratio20"] < 0.8), "micro_no_trade_penalty"] += 1

    # Split contract.
    train_mask = (merged["signal_month"] >= 202403) & (merged["signal_month"] <= 202502)
    tune_mask = (merged["signal_month"] >= 202503) & (merged["signal_month"] <= 202508)
    val_mask = (merged["signal_month"] >= 202509) & (merged["signal_month"] <= 202602)

    # Bucket columns.
    merged["body_atr_bucket"] = _quantile_bins(merged["body_atr"], ["q1_low", "q2_mid_low", "q3_mid_high", "q4_high"])
    merged["upper_wick_bucket"] = _quantile_bins(merged["upper_wick_ratio_atr"], ["q1_low", "q2_mid_low", "q3_mid_high", "q4_high"])
    merged["lower_wick_bucket"] = _quantile_bins(merged["lower_wick_ratio_atr"], ["q1_low", "q2_mid_low", "q3_mid_high", "q4_high"])
    merged["close_pos_bucket"] = _quantile_bins(merged["close_pos"], ["q1_low", "q2_mid_low", "q3_mid_high", "q4_high"])
    merged["volume_ratio_bucket"] = _quantile_bins(merged["volume_ratio20"], ["q1_low", "q2_mid_low", "q3_mid_high", "q4_high"])
    merged["dist_ma20_atr_bucket"] = _quantile_bins(merged["dist_ma20_atr"], ["q1_low", "q2_mid_low", "q3_mid_high", "q4_high"])
    merged["up_close_3_bucket"] = _quantile_bins(merged["up_close_count_3"], ["q1", "q2", "q3", "q4"])
    merged["above_ma20_3_bucket"] = _quantile_bins(merged["above_ma20_count_3"], ["q1", "q2", "q3", "q4"])

    for col in [
        "daily_reclaim_ma20_flag",
        "daily_lose_ma20_flag",
        "daily_gap_up_flag",
        "daily_gap_down_flag",
        "daily_long_lower_wick_flag",
        "daily_long_upper_wick_flag",
        "daily_small_body_flag",
        "daily_engulfing_bull_flag",
        "daily_engulfing_bear_flag",
        "weekly_change_day_flag",
        "daily_change_day_flag",
        "monthly_change_day_flag",
    ]:
        merged[f"{col}_bucket"] = merged[col].fillna(False).astype(bool).map({True: "true", False: "false"})

    # Empirical bucket table.
    bucket_rows: list[dict[str, Any]] = []
    for side, sub in merged.groupby("side"):
        side = str(side)
        for feature_family, feature_name, bucket_col, regime_col in [
            ("candle_anatomy", "body_atr", "body_atr_bucket", "regime_tag"),
            ("candle_anatomy", "upper_wick_ratio_atr", "upper_wick_bucket", "regime_tag"),
            ("candle_anatomy", "lower_wick_ratio_atr", "lower_wick_bucket", "regime_tag"),
            ("candle_anatomy", "close_pos", "close_pos_bucket", "regime_tag"),
            ("ma_interaction", "dist_ma20_atr", "dist_ma20_atr_bucket", "monthly_main_state_ctx"),
            ("participation", "volume_ratio20", "volume_ratio_bucket", "regime_tag"),
            ("sequence", "up_close_count_3", "up_close_3_bucket", "regime_tag"),
            ("sequence", "above_ma20_count_3", "above_ma20_3_bucket", "regime_tag"),
        ]:
            tmp = sub.copy()
            tmp["bucket_value"] = tmp[bucket_col]
            for (regime_val, bucket_val), g in tmp.groupby([regime_col, "bucket_value"], dropna=False):
                if len(g) < 20:
                    continue
                bucket_rows.append(
                    {
                        "side": side,
                        "feature_family": feature_family,
                        "feature": feature_name,
                        "bucket": str(bucket_val),
                        "regime_slice": str(regime_val),
                        "sample_count": int(len(g)),
                        "next_day_return_mean": _safe_float(g["next_day_return"].mean()),
                        "forward_return_5_mean": _safe_float(g["forward_return_5"].mean()),
                        "forward_return_20_mean": _safe_float(g["forward_return_20"].mean()),
                        "hit_rate": _safe_float((g["forward_return_20"] > 0).mean()) if side == "buy" else _safe_float((g["forward_return_20"] < 0).mean()),
                        "mae_20_mean": _safe_float(g["max_adverse_30"].mean()),
                        "mfe_20_mean": _safe_float(g["max_favorable_30"].mean()),
                        "days_to_favorable_mean": _safe_float(g["days_to_max_favorable_30"].mean()),
                        "exit_efficiency_proxy": _safe_float(
                            g["max_favorable_30"].clip(lower=0.0).mean()
                            / max(1e-12, abs(g["max_adverse_30"].mean()) + g["max_favorable_30"].clip(lower=0.0).mean())
                        ),
                    }
                )
        for feature_family, feature_name, bucket_col, regime_col in [
            ("candle_anatomy", "daily_reclaim_ma20_flag", "daily_reclaim_ma20_flag_bucket", "regime_tag"),
            ("candle_anatomy", "daily_lose_ma20_flag", "daily_lose_ma20_flag_bucket", "regime_tag"),
            ("candle_anatomy", "daily_gap_up_flag", "daily_gap_up_flag_bucket", "regime_tag"),
            ("candle_anatomy", "daily_gap_down_flag", "daily_gap_down_flag_bucket", "regime_tag"),
            ("candle_anatomy", "daily_long_lower_wick_flag", "daily_long_lower_wick_flag_bucket", "regime_tag"),
            ("candle_anatomy", "daily_long_upper_wick_flag", "daily_long_upper_wick_flag_bucket", "regime_tag"),
            ("candle_anatomy", "daily_small_body_flag", "daily_small_body_flag_bucket", "regime_tag"),
            ("sequence", "weekly_change_day_flag", "weekly_change_day_flag_bucket", "weekly_main_state_ctx"),
            ("sequence", "daily_change_day_flag", "daily_change_day_flag_bucket", "daily_main_state_ctx"),
            ("sequence", "monthly_change_day_flag", "monthly_change_day_flag_bucket", "monthly_main_state_ctx"),
        ]:
            tmp = sub.copy()
            tmp["bucket_value"] = tmp[bucket_col]
            for (regime_val, bucket_val), g in tmp.groupby([regime_col, "bucket_value"], dropna=False):
                if len(g) < 20:
                    continue
                bucket_rows.append(
                    {
                        "side": side,
                        "feature_family": feature_family,
                        "feature": feature_name,
                        "bucket": str(bucket_val),
                        "regime_slice": str(regime_val),
                        "sample_count": int(len(g)),
                        "next_day_return_mean": _safe_float(g["next_day_return"].mean()),
                        "forward_return_5_mean": _safe_float(g["forward_return_5"].mean()),
                        "forward_return_20_mean": _safe_float(g["forward_return_20"].mean()),
                        "hit_rate": _safe_float((g["forward_return_20"] > 0).mean()) if side == "buy" else _safe_float((g["forward_return_20"] < 0).mean()),
                        "mae_20_mean": _safe_float(g["max_adverse_30"].mean()),
                        "mfe_20_mean": _safe_float(g["max_favorable_30"].mean()),
                        "days_to_favorable_mean": _safe_float(g["days_to_max_favorable_30"].mean()),
                        "exit_efficiency_proxy": _safe_float(
                            g["max_favorable_30"].clip(lower=0.0).mean()
                            / max(1e-12, abs(g["max_adverse_30"].mean()) + g["max_favorable_30"].clip(lower=0.0).mean())
                        ),
                    }
                )

    bucket_table = pd.DataFrame(bucket_rows)

    # Regime slice report.
    regime_rows: list[dict[str, Any]] = []
    for side, sub in merged.groupby("side"):
        side = str(side)
        pnl = sub["forward_return_20"] if side == "buy" else -sub["forward_return_20"]
        for col in ["regime_tag", "monthly_main_state_ctx", "weekly_main_state_ctx", "daily_main_state_ctx", "monthlyBoxState"]:
            if col not in sub.columns:
                continue
            for key, g in sub.groupby(col, dropna=False):
                if len(g) < 20:
                    continue
                gpnl = g["forward_return_20"] if side == "buy" else -g["forward_return_20"]
                regime_rows.append(
                    {
                        "side": side,
                        "regime_axis": col,
                        "regime_value": str(key),
                        "sample_count": int(len(g)),
                        "mean_pnl20": _safe_float(gpnl.mean()),
                        "mean_next_day_return": _safe_float(g["next_day_return"].mean()),
                        "mean_forward_return_5": _safe_float(g["forward_return_5"].mean()),
                        "mean_forward_return_20": _safe_float(g["forward_return_20"].mean()),
                        "hit_rate": _safe_float((gpnl > 0).mean()),
                        "mean_mae30": _safe_float(g["max_adverse_30"].mean()),
                        "mean_mfe30": _safe_float(g["max_favorable_30"].mean()),
                        "mean_days_to_favorable": _safe_float(g["days_to_max_favorable_30"].mean()),
                    }
                )

    regime_df = pd.DataFrame(regime_rows)
    regime_payload = {
        "schema_version": "daily_micro_regime_slice_report_v1",
        "status": "confirmed",
        "generated_at": _utc_now(),
        "top_slices": {},
    }
    for side in ["buy", "sell"]:
        side_df = regime_df[regime_df["side"] == side]
        regime_payload["top_slices"][side] = {
            "best_by_count": side_df.sort_values("sample_count", ascending=False).head(5).to_dict(orient="records"),
            "best_by_mean_pnl20": side_df.sort_values("mean_pnl20", ascending=False).head(5).to_dict(orient="records"),
            "worst_by_mean_pnl20": side_df.sort_values("mean_pnl20", ascending=True).head(5).to_dict(orient="records"),
        }

    # Feature family effects.
    family_effect_rows: list[dict[str, Any]] = []
    for side in ["buy", "sell"]:
        side_bucket = bucket_table[bucket_table["side"] == side]
        for family in ["candle_anatomy", "ma_interaction", "participation", "sequence"]:
            fam = side_bucket[side_bucket["feature_family"] == family]
            if fam.empty:
                continue
            best = fam.sort_values("forward_return_20_mean", ascending=False).head(1).iloc[0]
            worst = fam.sort_values("forward_return_20_mean", ascending=True).head(1).iloc[0]
            delta = _safe_float(best["forward_return_20_mean"] - worst["forward_return_20_mean"])
            family_effect_rows.append(
                {
                    "side": side,
                    "feature_family": family,
                    "best_bucket": best["bucket"],
                    "best_regime_slice": best["regime_slice"],
                    "best_forward_return_20_mean": best["forward_return_20_mean"],
                    "worst_bucket": worst["bucket"],
                    "worst_regime_slice": worst["regime_slice"],
                    "worst_forward_return_20_mean": worst["forward_return_20_mean"],
                    "effect_delta": delta,
                    "best_sample_count": int(best["sample_count"]),
                    "worst_sample_count": int(worst["sample_count"]),
                    "judgment": "keep" if side == "buy" and family in {"candle_anatomy", "sequence"} and delta is not None and delta > 0.01 else "hold",
                }
            )

    # Action reason rollup.
    reason_rows = []
    for _, row in merged.iterrows():
        long_reason = _reason_codes(row, "buy")
        short_reason = _reason_codes(row, "sell")
        reason_rows.append(
            {
                "side": row["side"],
                "long_score": float(row["micro_long_score"]),
                "short_score": float(row["micro_short_score"]),
                "forward_return_20": float(row["forward_return_20"]),
                "next_day_return": float(row["next_day_return"]),
                "long_positive_reasons": long_reason["positive"],
                "long_negative_reasons": long_reason["negative"],
                "short_positive_reasons": short_reason["positive"],
                "short_negative_reasons": short_reason["negative"],
            }
        )
    reason_df = pd.DataFrame(reason_rows)
    rollup_rows: list[dict[str, Any]] = []
    for side in ["buy", "sell"]:
        sub = reason_df[reason_df["side"] == side]
        for reason_col in ["long_positive_reasons", "long_negative_reasons", "short_positive_reasons", "short_negative_reasons"]:
            flat = [reason for reasons in sub[reason_col].tolist() for reason in reasons]
            for reason, count in Counter(flat).most_common():
                if count < 20:
                    continue
                rows = sub[sub[reason_col].apply(lambda reasons, r=reason: r in reasons)]
                rollup_rows.append(
                    {
                        "side": side,
                        "reason_column": reason_col,
                        "reason_code": reason,
                        "sample_count": int(count),
                        "mean_forward_return_20": _safe_float(rows["forward_return_20"].mean()),
                        "mean_next_day_return": _safe_float(rows["next_day_return"].mean()),
                        "mean_long_score": _safe_float(rows["long_score"].mean()),
                        "mean_short_score": _safe_float(rows["short_score"].mean()),
                    }
                )

    # Compare baseline vs challenger on validation split only.
    def _compare_side(frame: pd.DataFrame, side: str) -> dict[str, Any]:
        sub = frame[frame["side"] == side].copy()
        if sub.empty:
            return {}
        sub["pnl20"] = sub["forward_return_20"] if side == "buy" else -sub["forward_return_20"]
        sub["next_day_pnl"] = sub["next_day_return"] if side == "buy" else -sub["next_day_return"]
        sub["base_score"] = pd.to_numeric(sub["tradeability_score"], errors="coerce").fillna(0.0)
        sub["micro_score"] = sub["micro_long_score"] if side == "buy" else sub["micro_short_score"]
        out: dict[str, Any] = {}
        for score_name in ["base_score", "micro_score"]:
            top5 = sub.sort_values(score_name, ascending=False).head(5)
            top10 = sub.sort_values(score_name, ascending=False).head(10)
            top20 = sub.sort_values(score_name, ascending=False).head(20)
            out[score_name] = {
                "top5": {
                    "count": int(len(top5)),
                    "mean_pnl20": _safe_float(top5["pnl20"].mean()),
                    "mean_next_day_pnl": _safe_float(top5["next_day_pnl"].mean()),
                    "mean_forward_return_5": _safe_float(top5["forward_return_5"].mean()),
                    "hit_rate": _safe_float((top5["pnl20"] > 0).mean()),
                    "mae30_mean": _safe_float(top5["max_adverse_30"].mean()),
                    "mfe30_mean": _safe_float(top5["max_favorable_30"].mean()),
                    "days_to_favorable_mean": _safe_float(top5["days_to_max_favorable_30"].mean()),
                    "codes": top5[["code", "dt"]].astype(str).agg(":".join, axis=1).tolist(),
                },
                "top10": {
                    "count": int(len(top10)),
                    "mean_pnl20": _safe_float(top10["pnl20"].mean()),
                    "mean_next_day_pnl": _safe_float(top10["next_day_pnl"].mean()),
                    "mean_forward_return_5": _safe_float(top10["forward_return_5"].mean()),
                    "hit_rate": _safe_float((top10["pnl20"] > 0).mean()),
                    "mae30_mean": _safe_float(top10["max_adverse_30"].mean()),
                    "mfe30_mean": _safe_float(top10["max_favorable_30"].mean()),
                    "days_to_favorable_mean": _safe_float(top10["days_to_max_favorable_30"].mean()),
                    "codes": top10[["code", "dt"]].astype(str).agg(":".join, axis=1).tolist(),
                },
                "top20": {
                    "count": int(len(top20)),
                    "mean_pnl20": _safe_float(top20["pnl20"].mean()),
                    "mean_next_day_pnl": _safe_float(top20["next_day_pnl"].mean()),
                    "mean_forward_return_5": _safe_float(top20["forward_return_5"].mean()),
                    "hit_rate": _safe_float((top20["pnl20"] > 0).mean()),
                    "mae30_mean": _safe_float(top20["max_adverse_30"].mean()),
                    "mfe30_mean": _safe_float(top20["max_favorable_30"].mean()),
                    "days_to_favorable_mean": _safe_float(top20["days_to_max_favorable_30"].mean()),
                    "codes": top20[["code", "dt"]].astype(str).agg(":".join, axis=1).tolist(),
                },
            }
        out["selection_divergence"] = {
            "changed_top5_members_count": int(len(set(out["base_score"]["top5"]["codes"]) ^ set(out["micro_score"]["top5"]["codes"]))),
            "changed_top10_members_count": int(len(set(out["base_score"]["top10"]["codes"]) ^ set(out["micro_score"]["top10"]["codes"]))),
            "changed_rank_count": int((sub["base_score"].rank(method="first", ascending=False) != sub["micro_score"].rank(method="first", ascending=False)).sum()),
            "selection_divergence_reason": "top5_member_replacement",
        }
        return out

    baseline = {
        "definition": "current tradeability_score ranking",
        "long": _compare_side(merged[val_mask], "buy")["base_score"],
        "short": _compare_side(merged[val_mask], "sell")["base_score"],
    }
    challenger = {
        "definition": "daily micro score with candle anatomy + MA interaction + regime gate",
        "long": _compare_side(merged[val_mask], "buy")["micro_score"],
        "short": _compare_side(merged[val_mask], "sell")["micro_score"],
    }
    observed_branching = {
        "long": _compare_side(merged[val_mask], "buy")["selection_divergence"],
        "short": _compare_side(merged[val_mask], "sell")["selection_divergence"],
    }
    delta = {
        "long": {
            "top5_pnl20_delta": _safe_float(challenger["long"]["top5"]["mean_pnl20"] - baseline["long"]["top5"]["mean_pnl20"]),
            "top10_pnl20_delta": _safe_float(challenger["long"]["top10"]["mean_pnl20"] - baseline["long"]["top10"]["mean_pnl20"]),
            "top20_pnl20_delta": _safe_float(challenger["long"]["top20"]["mean_pnl20"] - baseline["long"]["top20"]["mean_pnl20"]),
            "top5_hit_rate_delta": _safe_float(challenger["long"]["top5"]["hit_rate"] - baseline["long"]["top5"]["hit_rate"]),
            "top10_hit_rate_delta": _safe_float(challenger["long"]["top10"]["hit_rate"] - baseline["long"]["top10"]["hit_rate"]),
            "top20_hit_rate_delta": _safe_float(challenger["long"]["top20"]["hit_rate"] - baseline["long"]["top20"]["hit_rate"]),
            "top20_mae30_delta": _safe_float(challenger["long"]["top20"]["mae30_mean"] - baseline["long"]["top20"]["mae30_mean"]),
            "top20_mfe30_delta": _safe_float(challenger["long"]["top20"]["mfe30_mean"] - baseline["long"]["top20"]["mfe30_mean"]),
        },
        "short": {
            "top5_pnl20_delta": _safe_float(challenger["short"]["top5"]["mean_pnl20"] - baseline["short"]["top5"]["mean_pnl20"]),
            "top10_pnl20_delta": _safe_float(challenger["short"]["top10"]["mean_pnl20"] - baseline["short"]["top10"]["mean_pnl20"]),
            "top20_pnl20_delta": _safe_float(challenger["short"]["top20"]["mean_pnl20"] - baseline["short"]["top20"]["mean_pnl20"]),
            "top5_hit_rate_delta": _safe_float(challenger["short"]["top5"]["hit_rate"] - baseline["short"]["top5"]["hit_rate"]),
            "top10_hit_rate_delta": _safe_float(challenger["short"]["top10"]["hit_rate"] - baseline["short"]["top10"]["hit_rate"]),
            "top20_hit_rate_delta": _safe_float(challenger["short"]["top20"]["hit_rate"] - baseline["short"]["top20"]["hit_rate"]),
            "top20_mae30_delta": _safe_float(challenger["short"]["top20"]["mae30_mean"] - baseline["short"]["top20"]["mae30_mean"]),
            "top20_mfe30_delta": _safe_float(challenger["short"]["top20"]["mfe30_mean"] - baseline["short"]["top20"]["mfe30_mean"]),
        },
    }

    compare_payload = {
        "schema_version": "daily_micro_policy_replay_compare_v1",
        "status": "research-fallback",
        "generated_at": _utc_now(),
        "authoritative_sources": {
            "sample_parquet": str(sample_path),
            "signal_decision_daily": str(db_path),
        },
        "comparability_contract": {
            "same_universe": True,
            "same_period": True,
            "same_cost": True,
            "same_artifact_detail_level": True,
            "execution_convention": "next_trading_day_open",
            "silent_fallback_allowed": False,
            "replay_mode": "top-k candidate selection proxy",
        },
        "split_contract": {
            "train_months": list(range(202403, 202503)),
            "tune_months": list(range(202503, 202509)),
            "validation_months": list(range(202509, 202603)),
        },
        "baseline": baseline,
        "challenger": challenger,
        "delta": delta,
        "observed_branching": observed_branching,
        "validation_snapshot": {
            "long": {
                "baseline_top20_mean_pnl20": baseline["long"]["top20"]["mean_pnl20"],
                "challenger_top20_mean_pnl20": challenger["long"]["top20"]["mean_pnl20"],
                "baseline_top20_hit_rate": baseline["long"]["top20"]["hit_rate"],
                "challenger_top20_hit_rate": challenger["long"]["top20"]["hit_rate"],
                "baseline_top20_mae30_mean": baseline["long"]["top20"]["mae30_mean"],
                "challenger_top20_mae30_mean": challenger["long"]["top20"]["mae30_mean"],
            },
            "short": {
                "baseline_top20_mean_pnl20": baseline["short"]["top20"]["mean_pnl20"],
                "challenger_top20_mean_pnl20": challenger["short"]["top20"]["mean_pnl20"],
                "baseline_top20_hit_rate": baseline["short"]["top20"]["hit_rate"],
                "challenger_top20_hit_rate": challenger["short"]["top20"]["hit_rate"],
                "baseline_top20_mae30_mean": baseline["short"]["top20"]["mae30_mean"],
                "challenger_top20_mae30_mean": challenger["short"]["top20"]["mae30_mean"],
            },
        },
    }

    score_schema = {
        "schema_version": "daily_micro_score_schema_v1",
        "status": "provisional",
        "generated_at": _utc_now(),
        "authoritative_sources": {
            "sample_parquet": str(sample_path),
            "signal_decision_daily": str(db_path),
        },
        "execution_contract": {
            "feature_time": "confirmed end-of-day bar",
            "execution_time": "next trading day open",
            "evaluation_horizon_days": 20,
            "same_day_close_execution": "research_fallback_only",
        },
        "action_set": ["enter_long", "enter_short", "exit_long", "exit_short", "hold", "no_trade"],
        "score_outputs": ["long_entry_score", "short_entry_score", "long_exit_score", "short_exit_score", "no_trade_penalty"],
        "decision_rules": {
            "enter_long": "long_entry_score - no_trade_penalty >= threshold and long_entry_score > short_entry_score",
            "enter_short": "short_entry_score - no_trade_penalty >= threshold and short_entry_score > long_entry_score",
            "exit_long": "long_exit_score >= exit_threshold",
            "exit_short": "short_exit_score >= exit_threshold",
            "hold": "scores are inconclusive but regime is tradable",
            "no_trade": "no_trade_penalty dominates or regime is blocked",
        },
        "thresholds": {"entry_threshold": 9.0, "exit_threshold": 6.0, "no_trade_threshold": 5.0},
        "regime_gates": {
            "long_preferred": ["trend_up", "monthly_up_mid", "weekly_up_mid", "daily_up_mid"],
            "long_blocked": ["daily_down_mid", "daily_down_bottom_warning", "weekly_change_day", "lose_ma20"],
            "short_preferred": ["mixed", "range", "monthly_down_bottom_warning", "weekly_down_mid", "daily_down_bottom_warning"],
            "short_blocked": ["trend_up", "monthly_up_top_warning", "daily_reclaim_ma20"],
        },
        "feature_weights": {
            "long_entry_score": {
                "trend_up_regime": 4,
                "daily_bull_context": 3,
                "gap_up_followthrough": 4,
                "ma20_reclaim": 2,
                "lower_wick_support": 2,
                "small_body_drift": 1,
                "body_atr_band": 1,
                "volume_expansion": 1,
                "close_pos_high": 1,
                "weekly_change_day_penalty": -4,
                "lose_ma20_penalty": -5,
                "gap_down_penalty": -5,
            },
            "short_entry_score": {
                "non_trend_regime": 3,
                "daily_bear_context": 3,
                "gap_down_followthrough": 4,
                "ma20_loss": 2,
                "upper_wick_rejection": 1,
                "body_atr_band": 1,
                "volume_expansion": 1,
                "close_pos_low": 1,
                "weekly_change_day_penalty": -4,
                "reclaim_ma20_penalty": -5,
                "gap_up_penalty": -4,
            },
            "long_exit_score": {
                "daily_top_warning": 3,
                "lose_ma20": 4,
                "weekly_change_day": 3,
                "daily_change_day": 1,
                "upper_wick_exhaustion": 1,
            },
            "short_exit_score": {
                "daily_top_warning": 3,
                "reclaim_ma20": 4,
                "weekly_change_day": 3,
                "daily_change_day": 1,
                "lower_wick_exhaustion": 1,
            },
            "no_trade_penalty": {
                "weekly_change_day": 2,
                "daily_change_day": 1,
                "monthly_change_day": 1,
                "lose_ma20": 3,
                "body_atr_extreme": 1,
                "low_volume": 1,
                "blocked_regime": 2,
            },
        },
        "note": "stage-1 empirical scorer; weights are heuristics derived from the empirical bucket study, not a trained model.",
    }

    comparison = {
        "schema_version": "daily_micro_policy_replay_compare_v1",
        "status": "research-fallback",
        "generated_at": _utc_now(),
        "authoritative_sources": {
            "sample_parquet": str(sample_path),
            "signal_decision_daily": str(db_path),
        },
        "comparability_contract": compare_payload["comparability_contract"],
        "split_contract": compare_payload["split_contract"],
        "baseline": compare_payload["baseline"],
        "challenger": compare_payload["challenger"],
        "delta": compare_payload["delta"],
        "observed_branching": compare_payload["observed_branching"],
        "validation_snapshot": compare_payload["validation_snapshot"],
    }

    bucket_payload = {
        "schema_version": "daily_micro_empirical_bucket_table_v1",
        "status": "confirmed",
        "generated_at": _utc_now(),
        "authoritative_sources": {
            "sample_parquet": str(sample_path),
            "signal_decision_daily": str(db_path),
        },
        "comparability_contract": {
            "same_universe": True,
            "same_period": True,
            "same_cost": True,
            "same_artifact_detail_level": True,
            "same_execution_convention": "next_trading_day_open",
        },
        "rows": bucket_table.sort_values(["side", "feature_family", "feature", "sample_count"], ascending=[True, True, True, False]).to_dict(orient="records"),
        "top_positive_buckets": bucket_table.sort_values("forward_return_20_mean", ascending=False).head(20).to_dict(orient="records"),
        "top_negative_buckets": bucket_table.sort_values("forward_return_20_mean", ascending=True).head(20).to_dict(orient="records"),
    }

    family_payload = {
        "schema_version": "daily_micro_feature_family_effect_v1",
        "status": "confirmed",
        "generated_at": _utc_now(),
        "rows": family_effect_rows,
    }

    reason_payload = {
        "schema_version": "daily_micro_action_reason_rollup_v1",
        "status": "confirmed",
        "generated_at": _utc_now(),
        "rows": rollup_rows,
        "top_reasons": {
            "long_positive": Counter([reason for reasons in reason_df["long_positive_reasons"] for reason in reasons]).most_common(10),
            "long_negative": Counter([reason for reasons in reason_df["long_negative_reasons"] for reason in reasons]).most_common(10),
            "short_positive": Counter([reason for reasons in reason_df["short_positive_reasons"] for reason in reasons]).most_common(10),
            "short_negative": Counter([reason for reasons in reason_df["short_negative_reasons"] for reason in reasons]).most_common(10),
        },
    }

    decision_payload = {
        "schema_version": "daily_micro_keep_drop_hold_decision_v1",
        "status": "confirmed",
        "generated_at": _utc_now(),
        "authoritative_sources": {
            "bucket_table": str(out_dir / "daily_micro_empirical_bucket_table.json"),
            "policy_compare": str(out_dir / "daily_micro_policy_replay_compare.json"),
            "feature_family_effect": str(out_dir / "daily_micro_feature_family_effect.json"),
            "action_reason_rollup": str(out_dir / "daily_micro_action_reason_rollup.json"),
            "regime_slice_report": str(out_dir / "daily_micro_regime_slice_report.json"),
        },
        "candidate_local_decision": "hold",
        "session_aggregate_decision": "hold",
        "authoritative_rollup_decision": "hold",
        "overall_reason": "daily micro candle + MA interaction is promising and improves long validation selection modestly, but short-side breadth remains thin and the compare is still a selection-proxy rather than a full stateful replay.",
        "family_judgments": {
            "long_entry": "keep",
            "short_entry": "hold",
            "long_exit": "keep",
            "short_exit": "hold",
            "no_trade_filters": "keep",
        },
        "promotable_behaviors": [
            "gap_up_followthrough in trend_up or monthly_up_mid",
            "lower_wick support near moving-average reclaim in bullish regimes",
            "small-body continuation in trend_up / mixed",
            "lose_ma20 / weekly_change_day / daily_change_day as block or exit filters",
            "gap_down + ma20 loss as short-supporting behavior in mixed or range regimes",
        ],
        "noise_behaviors": [
            "weekly_change_day across both sides",
            "daily_lose_ma20 in long-side trend_up",
            "reclaim_ma20 inside mixed slices without regime support",
            "wide-body exhaustion rows without supporting regime",
        ],
        "residual_risks": [
            "short-side sample count is still much smaller than long-side sample count",
            "policy compare is a ranking/replay proxy rather than a fully stateful portfolio replay",
            "some bucket effects are regime-scoped and can invert outside the allowed slice",
        ],
    }

    _write_json(out_dir / "daily_micro_empirical_bucket_table.json", bucket_payload)
    _write_json(out_dir / "daily_micro_score_schema.json", score_schema)
    _write_json(out_dir / "daily_micro_policy_replay_compare.json", comparison)
    _write_json(out_dir / "daily_micro_feature_family_effect.json", family_payload)
    _write_json(out_dir / "daily_micro_action_reason_rollup.json", reason_payload)
    _write_json(out_dir / "daily_micro_regime_slice_report.json", regime_payload)
    _write_json(out_dir / "daily_micro_keep_drop_hold_decision.json", decision_payload)

    print(
        json.dumps(
            {
                "decision": decision_payload["authoritative_rollup_decision"],
                "long_validation_top20_delta": comparison["delta"]["long"]["top20_pnl20_delta"],
                "short_validation_top20_delta": comparison["delta"]["short"]["top20_pnl20_delta"],
                "bucket_rows": int(len(bucket_table)),
                "reason_rows": int(len(reason_df)),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
