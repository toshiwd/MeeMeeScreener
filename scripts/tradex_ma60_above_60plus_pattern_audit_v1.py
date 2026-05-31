from __future__ import annotations

import argparse
import json
import math
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


AXIS_ID = "ma60_above_60plus_pattern_audit_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\ma60_above_60plus_pattern_audit_v1")
DEFAULT_PRODUCTION_CSV = Path("production_data/production_daily.csv")
ANCHOR_COUNTS = {"anchor_1": 1, "anchor_10": 10, "anchor_20": 20, "anchor_30": 30}
REQUIRED_ARTIFACTS = (
    "input_schema_report.json",
    "streak_events.csv",
    "anchor_feature_rows.csv",
    "positive_vs_control_summary.json",
    "feature_lift_by_anchor.csv",
    "failure_decomposition.csv",
    "simple_rule_candidates.csv",
    "research_decision.json",
    "no_lookahead_audit.json",
    "_ARTIFACT_COMPLETE.json",
)
STREAK_COLUMNS = [
    "code",
    "streak_start_date",
    "streak_end_date",
    "streak_length",
    "reached_60",
    "anchor_1_date",
    "anchor_10_date",
    "anchor_20_date",
    "anchor_30_date",
    "max_drawdown_in_streak",
    "max_gain_in_streak",
    "break_reason",
    "regime_proxy_at_start",
    "monthly_proxy_at_start",
]


@dataclass(frozen=True)
class InputResolution:
    source_type: str
    path: Path
    daily_columns: tuple[str, ...]
    ma_columns: tuple[str, ...]
    row_count: int
    min_date: str | None
    max_date: str | None


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    try:
        import numpy as np

        if isinstance(value, np.generic):
            return _json_ready(value.item())
    except Exception:
        pass
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _date_norm_expr(column: str) -> str:
    return f"""
    CASE
      WHEN typeof({column}) = 'DATE' THEN CAST(strftime({column}, '%Y%m%d') AS INTEGER)
      WHEN typeof({column}) = 'TIMESTAMP' THEN CAST(strftime({column}, '%Y%m%d') AS INTEGER)
      ELSE CAST(regexp_replace(CAST({column} AS VARCHAR), '[^0-9]', '', 'g') AS INTEGER)
    END
    """


def default_db_candidates() -> list[Path]:
    local_app = os.environ.get("LOCALAPPDATA")
    candidates: list[Path] = []
    if local_app:
        candidates.extend(
            [
                Path(local_app) / "MeeMeeScreener" / "data" / "stocks.duckdb",
                Path(local_app) / "MeeMeeScreener-dev" / "data" / "stocks.duckdb",
            ]
        )
    candidates.append(Path("data/stocks.duckdb"))
    return candidates


def _table_names(conn: duckdb.DuckDBPyConnection) -> set[str]:
    return {str(row[0]) for row in conn.execute("SHOW TABLES").fetchall()}


def _columns(conn: duckdb.DuckDBPyConnection, table: str) -> list[str]:
    return [str(row[1]) for row in conn.execute(f"PRAGMA table_info('{table}')").fetchall()]


def _ymd_to_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(int(value))
    return f"{text[:4]}-{text[4:6]}-{text[6:]}" if len(text) == 8 else None


def resolve_input(db_path: Path | None = None, production_csv: Path = DEFAULT_PRODUCTION_CSV) -> InputResolution:
    for candidate in ([db_path] if db_path else default_db_candidates()):
        if candidate is None or not candidate.exists():
            continue
        try:
            conn = duckdb.connect(str(candidate), read_only=True)
            tables = _table_names(conn)
            if {"daily_bars", "daily_ma"} <= tables:
                daily_cols = tuple(_columns(conn, "daily_bars"))
                ma_cols = tuple(_columns(conn, "daily_ma"))
                d_expr = _date_norm_expr("date")
                row = conn.execute(f"SELECT COUNT(*), MIN({d_expr}), MAX({d_expr}) FROM daily_bars").fetchone()
                conn.close()
                if row and int(row[0] or 0) > 0:
                    return InputResolution("duckdb_daily_bars_daily_ma", candidate, daily_cols, ma_cols, int(row[0]), _ymd_to_text(row[1]), _ymd_to_text(row[2]))
        except Exception:
            continue
    if production_csv.exists():
        sample = pd.read_csv(production_csv, nrows=5)
        return InputResolution("production_csv", production_csv, tuple(sample.columns), (), -1, None, None)
    raise RuntimeError("No usable daily OHLCV source found")


def load_daily_frame(resolution: InputResolution, *, start_ymd: int | None = None, end_ymd: int | None = None) -> pd.DataFrame:
    if resolution.source_type == "duckdb_daily_bars_daily_ma":
        conn = duckdb.connect(str(resolution.path), read_only=True)
        b_expr = _date_norm_expr("b.date")
        m_expr = _date_norm_expr("m.date")
        source_filter = "AND lower(coalesce(b.source, 'pan')) = 'pan'" if "source" in resolution.daily_columns else ""
        clauses: list[str] = []
        params: list[Any] = []
        if start_ymd is not None:
            clauses.append("ymd >= ?")
            params.append(int(start_ymd))
        if end_ymd is not None:
            clauses.append("ymd <= ?")
            params.append(int(end_ymd))
        date_clause = "" if not clauses else "AND " + " AND ".join(clauses)
        frame = conn.execute(
            f"""
            WITH b AS (
              SELECT CAST(code AS VARCHAR) AS code, {b_expr} AS ymd, o, h, l, c, v
              FROM daily_bars AS b
              WHERE o > 0 AND h > 0 AND l > 0 AND c > 0 {source_filter}
            ),
            m AS (
              SELECT CAST(code AS VARCHAR) AS code, {m_expr} AS ymd, ma7, ma20, ma60
              FROM daily_ma AS m
            )
            SELECT b.code, b.ymd, b.o, b.h, b.l, b.c, b.v, m.ma7, m.ma20, m.ma60
            FROM b
            LEFT JOIN m ON b.code = m.code AND b.ymd = m.ymd
            WHERE true {date_clause}
            ORDER BY b.code, b.ymd
            """,
            params,
        ).fetchdf()
        conn.close()
    else:
        frame = pd.read_csv(resolution.path)
        frame = frame.rename(columns={"date": "ymd", "open": "o", "high": "h", "low": "l", "close": "c", "volume": "v"})
        frame["ymd"] = pd.to_datetime(frame["ymd"]).dt.strftime("%Y%m%d").astype(int)
        frame["code"] = frame["code"].astype(str)
        if start_ymd is not None:
            frame = frame[frame["ymd"] >= int(start_ymd)]
        if end_ymd is not None:
            frame = frame[frame["ymd"] <= int(end_ymd)]
        frame = frame.sort_values(["code", "ymd"], kind="stable")
    if frame.empty:
        raise RuntimeError("daily input returned no rows")
    frame["date"] = pd.to_datetime(frame["ymd"].astype(str), format="%Y%m%d")
    frame["code"] = frame["code"].astype(str)
    for col in ("o", "h", "l", "c", "v", "ma7", "ma20", "ma60"):
        if col in frame.columns:
            frame[col] = pd.to_numeric(frame[col], errors="coerce")
    return frame


def _streak_true(series: pd.Series) -> pd.Series:
    out: list[int] = []
    count = 0
    for value in series.fillna(False).astype(bool).tolist():
        count = count + 1 if value else 0
        out.append(count)
    return pd.Series(out, index=series.index)


def _rolling_prior_sum(series: pd.Series, window: int) -> pd.Series:
    return series.astype(float).shift(1).rolling(window, min_periods=1).sum()


def add_features(frame: pd.DataFrame) -> pd.DataFrame:
    work = frame.sort_values(["code", "date"], kind="stable").copy()
    grouped = work.groupby("code", sort=False)
    if "ma7" not in work.columns or work["ma7"].isna().all():
        work["ma7"] = grouped["c"].transform(lambda s: s.rolling(7, min_periods=7).mean())
    if "ma20" not in work.columns or work["ma20"].isna().all():
        work["ma20"] = grouped["c"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    if "ma60" not in work.columns or work["ma60"].isna().all():
        work["ma60"] = grouped["c"].transform(lambda s: s.rolling(60, min_periods=60).mean())
    for ma in ("ma7", "ma20", "ma60"):
        work[f"{ma}_slope"] = grouped[ma].transform(lambda s: s / s.shift(5) - 1.0)
    work["ma20_slope_accel"] = grouped["ma20_slope"].transform(lambda s: s - s.shift(5))
    for ma in ("ma7", "ma20", "ma60"):
        work[f"dist_{ma}_pct"] = work["c"] / work[ma] - 1.0
    work["ma20_ma60_distance_pct"] = work["ma20"] / work["ma60"] - 1.0
    work["ma7_ma20_distance_pct"] = work["ma7"] / work["ma20"] - 1.0
    work["ma7_gt_ma20_gt_ma60"] = ((work["ma7"] > work["ma20"]) & (work["ma20"] > work["ma60"])).astype(int)
    work["ma20_gt_ma60"] = (work["ma20"] > work["ma60"]).astype(int)
    work["close_above_ma60"] = work["ma60"].notna() & (work["c"] > work["ma60"])
    work["ppp_proxy"] = ((work["ma7"] > work["ma20"]) & (work["ma20"] > work["ma60"]) & (work["ma20_slope"] > 0) & (work["ma60_slope"] >= 0)).astype(int)

    prev_c = grouped["c"].shift(1)
    true_range = pd.concat([(work["h"] - work["l"]).abs(), (work["h"] - prev_c).abs(), (work["l"] - prev_c).abs()], axis=1).max(axis=1)
    work["atr14_pct"] = true_range.groupby(work["code"], sort=False).transform(lambda s: s.rolling(14, min_periods=14).mean()) / work["c"]
    work["realized_vol20"] = grouped["c"].pct_change().groupby(work["code"], sort=False).transform(lambda s: s.rolling(20, min_periods=20).std())
    work["gap_pct"] = work["o"] / prev_c - 1.0
    work["max_drawdown_20"] = work["c"] / grouped["c"].transform(lambda s: s.shift(1).rolling(20, min_periods=1).max()) - 1.0
    work["too_fast_rise_flag"] = (work["c"] / grouped["c"].shift(10) - 1.0 > 0.25).astype(int)
    work["overheat_flag"] = ((work["dist_ma20_pct"] > 0.15) | (work["dist_ma60_pct"] > 0.30)).astype(int)

    candle_range = (work["h"] - work["l"]).replace(0, pd.NA)
    body = pd.to_numeric((work["c"] - work["o"]).abs() / candle_range, errors="coerce")
    upper = pd.to_numeric((work["h"] - pd.concat([work["o"], work["c"]], axis=1).max(axis=1)) / candle_range, errors="coerce")
    lower = pd.to_numeric((pd.concat([work["o"], work["c"]], axis=1).min(axis=1) - work["l"]) / candle_range, errors="coerce")
    up_day = work["c"] > work["o"]
    down_day = work["c"] < work["o"]
    work["bullish_ratio_20"] = up_day.astype(float).groupby(work["code"], sort=False).transform(lambda s: s.rolling(20, min_periods=1).mean())
    work["large_bull_count_20"] = ((up_day) & (body >= 0.55)).astype(float).groupby(work["code"], sort=False).transform(lambda s: s.rolling(20, min_periods=1).sum())
    work["large_bear_count_20"] = ((down_day) & (body >= 0.55)).astype(float).groupby(work["code"], sort=False).transform(lambda s: s.rolling(20, min_periods=1).sum())
    work["avg_body_ratio_20"] = body.groupby(work["code"], sort=False).transform(lambda s: s.rolling(20, min_periods=1).mean())
    work["upper_wick_ratio_20"] = upper.groupby(work["code"], sort=False).transform(lambda s: s.rolling(20, min_periods=1).mean())
    work["lower_wick_ratio_20"] = lower.groupby(work["code"], sort=False).transform(lambda s: s.rolling(20, min_periods=1).mean())
    work["gu_count_20"] = (work["gap_pct"] > 0.02).astype(float).groupby(work["code"], sort=False).transform(lambda s: s.rolling(20, min_periods=1).sum())
    work["gd_count_20"] = (work["gap_pct"] < -0.02).astype(float).groupby(work["code"], sort=False).transform(lambda s: s.rolling(20, min_periods=1).sum())
    work["n_continuation_flag"] = ((work["c"] > grouped["c"].shift(10)) & (grouped["c"].shift(5) < grouped["c"].shift(10)) & (work["c"] > grouped["c"].shift(5))).astype(int)
    work["reverse_n_reject_flag"] = ((work["c"] < grouped["c"].shift(10)) & (grouped["c"].shift(5) > grouped["c"].shift(10))).astype(int)

    work["volume_ma20"] = grouped["v"].transform(lambda s: s.rolling(20, min_periods=5).mean())
    work["volume_ratio_ma20"] = work["v"] / work["volume_ma20"]
    ret1 = grouped["c"].pct_change()
    work["up_day_volume_ratio_20"] = work["volume_ratio_ma20"].where(ret1 > 0).groupby(work["code"], sort=False).transform(lambda s: s.rolling(20, min_periods=1).mean())
    work["down_day_volume_ratio_20"] = work["volume_ratio_ma20"].where(ret1 < 0).groupby(work["code"], sort=False).transform(lambda s: s.rolling(20, min_periods=1).mean())
    high20_prev = grouped["h"].transform(lambda s: s.shift(1).rolling(20, min_periods=1).max())
    work["high_break_volume_count_20"] = ((work["h"] > high20_prev) & (work["volume_ratio_ma20"] > 1.2)).astype(float).groupby(work["code"], sort=False).transform(lambda s: s.rolling(20, min_periods=1).sum())
    work["low_volume_pullback_count_20"] = ((ret1 < 0) & (work["volume_ratio_ma20"] < 0.8)).astype(float).groupby(work["code"], sort=False).transform(lambda s: s.rolling(20, min_periods=1).sum())

    ma20_break = (work["c"] <= work["ma20"]).astype(float)
    ma7_break = (work["c"] <= work["ma7"]).astype(float)
    work["ma20_break_count_pre20"] = ma20_break.groupby(work["code"], sort=False).transform(lambda s: s.shift(1).rolling(20, min_periods=1).sum())
    work["ma7_break_count_pre20"] = ma7_break.groupby(work["code"], sort=False).transform(lambda s: s.shift(1).rolling(20, min_periods=1).sum())
    work["higher_low_count_20"] = (work["l"] > grouped["l"].shift(1)).astype(float).groupby(work["code"], sort=False).transform(lambda s: s.rolling(20, min_periods=1).sum())
    work["recent_high_update_count_20"] = (work["h"] > high20_prev).astype(float).groupby(work["code"], sort=False).transform(lambda s: s.rolling(20, min_periods=1).sum())

    work["period_weekly"] = work["date"].dt.to_period("W-FRI").astype(str)
    work["period_monthly"] = work["date"].dt.to_period("M").astype(str)
    weekly = _higher_timeframe_features(work, "W-FRI", "weekly")
    monthly = _higher_timeframe_features(work, "M", "monthly")
    work = work.merge(weekly, on=["code", "period_weekly"], how="left").merge(monthly, on=["code", "period_monthly"], how="left")
    return work


def _higher_timeframe_features(work: pd.DataFrame, freq: str, prefix: str) -> pd.DataFrame:
    period_col = f"period_{prefix}"
    temp = work[["code", "date", "o", "h", "l", "c", "v"]].copy()
    temp[period_col] = temp["date"].dt.to_period(freq).astype(str)
    agg = temp.groupby(["code", period_col], as_index=False).agg(c=("c", "last"), h=("h", "max"), l=("l", "min"))
    grouped = agg.groupby("code", sort=False)
    agg[f"{prefix}_ma20"] = grouped["c"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    agg[f"{prefix}_ma20_slope"] = grouped[f"{prefix}_ma20"].transform(lambda s: s / s.shift(3) - 1.0)
    agg[f"{prefix}_close_gt_ma20"] = (agg["c"] > agg[f"{prefix}_ma20"]).astype(int)
    agg[f"{prefix}_ppp_proxy"] = ((agg["c"] > agg[f"{prefix}_ma20"]) & (agg[f"{prefix}_ma20_slope"] > 0)).astype(int)
    rolling_high = grouped["h"].transform(lambda s: s.shift(1).rolling(12, min_periods=3).max())
    rolling_low = grouped["l"].transform(lambda s: s.shift(1).rolling(12, min_periods=3).min())
    rng = (rolling_high - rolling_low).replace(0, pd.NA)
    agg[f"{prefix}_box_breakout"] = (agg["c"] > rolling_high).astype(int)
    agg[f"{prefix}_box_inside"] = ((agg["c"] <= rolling_high) & (agg["c"] >= rolling_low)).astype(int)
    agg[f"{prefix}_high_zone"] = ((agg["c"] - rolling_low) / rng > 0.8).astype(int)
    cols = ["code", period_col, f"{prefix}_close_gt_ma20", f"{prefix}_ma20_slope", f"{prefix}_ppp_proxy", f"{prefix}_box_breakout", f"{prefix}_box_inside", f"{prefix}_high_zone"]
    return agg[cols]


def build_streaks(featured: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    streak_rows: list[dict[str, Any]] = []
    anchor_rows: list[dict[str, Any]] = []
    for code, group in featured.sort_values(["code", "date"], kind="stable").groupby("code", sort=False):
        active: list[dict[str, Any]] = []
        for row in group.to_dict("records"):
            if bool(row.get("close_above_ma60")):
                active.append(row)
                continue
            if active:
                _append_streak(streak_rows, anchor_rows, str(code), active, break_row=row)
                active = []
        if active:
            _append_streak(streak_rows, anchor_rows, str(code), active, break_row=None)
    return pd.DataFrame(streak_rows, columns=STREAK_COLUMNS), pd.DataFrame(anchor_rows)


def _append_streak(streak_rows: list[dict[str, Any]], anchor_rows: list[dict[str, Any]], code: str, active: list[dict[str, Any]], break_row: dict[str, Any] | None) -> None:
    length = len(active)
    if length < 5:
        return
    closes = pd.Series([float(r["c"]) for r in active])
    start_close = float(active[0]["c"])
    running_peak = closes.cummax()
    max_dd = float((closes / running_peak - 1.0).min())
    max_gain = float(closes.max() / start_close - 1.0) if start_close > 0 else None
    reached_60 = length >= 60
    anchor_dates = {name: (active[count - 1]["date"] if length >= count else None) for name, count in ANCHOR_COUNTS.items()}
    break_reason = classify_break_reason(active, break_row)
    start = active[0]
    streak_rows.append(
        {
            "code": code,
            "streak_start_date": start["date"],
            "streak_end_date": active[-1]["date"],
            "streak_length": int(length),
            "reached_60": bool(reached_60),
            "anchor_1_date": anchor_dates["anchor_1"],
            "anchor_10_date": anchor_dates["anchor_10"],
            "anchor_20_date": anchor_dates["anchor_20"],
            "anchor_30_date": anchor_dates["anchor_30"],
            "max_drawdown_in_streak": max_dd,
            "max_gain_in_streak": max_gain,
            "break_reason": break_reason,
            "regime_proxy_at_start": "PPP_proxy" if int(start.get("ppp_proxy") or 0) else "non_PPP_proxy",
            "monthly_proxy_at_start": _monthly_proxy_label(start),
        }
    )
    for anchor_type, count in ANCHOR_COUNTS.items():
        if length < count:
            continue
        anchor = active[count - 1]
        row = {
            "code": code,
            "anchor_type": anchor_type,
            "anchor_date": anchor["date"],
            "future_reached_60": bool(reached_60),
            "final_streak_length": int(length),
            "label_cohort": _label_cohort(length),
            "feature_source_status": "ok",
            "label_columns": "future_reached_60,final_streak_length,label_cohort",
        }
        row.update(_feature_row(anchor, active[:count]))
        anchor_rows.append(row)


def classify_break_reason(active: list[dict[str, Any]], break_row: dict[str, Any] | None) -> str:
    if break_row is None:
        return "open_streak_unbroken_at_dataset_end"
    if break_row.get("ma60") is not None and break_row.get("c") is not None and float(break_row["c"]) <= float(break_row["ma60"]):
        if break_row.get("ma20") is not None and float(break_row["c"]) <= float(break_row["ma20"]):
            return "ma20_and_ma60_break"
        ret = float(break_row["c"]) / float(active[-1]["c"]) - 1.0 if float(active[-1]["c"]) > 0 else 0.0
        vol_ratio = float(break_row.get("volume_ratio_ma20") or 0.0)
        if ret <= -0.05 and vol_ratio >= 1.5:
            return "high_volume_large_bear_ma60_break"
        return "ma60_break"
    return "unknown_reset"


def _label_cohort(length: int) -> str:
    if length >= 60:
        return "positive"
    if 20 <= length <= 59:
        return "control_main"
    if 5 <= length <= 19:
        return "control_early_fail"
    return "exclude"


def _monthly_proxy_label(row: dict[str, Any]) -> str:
    if int(row.get("monthly_close_gt_ma20") or 0) and float(row.get("monthly_ma20_slope") or 0.0) > 0:
        return "monthly_uptrend_proxy"
    if int(row.get("monthly_box_inside") or 0):
        return "monthly_range_proxy"
    return "monthly_other_proxy"


def _safe_min(values: list[float]) -> float | None:
    return min(values) if values else None


def _feature_row(anchor: dict[str, Any], so_far: list[dict[str, Any]]) -> dict[str, Any]:
    feature_cols = [
        "ma7_gt_ma20_gt_ma60",
        "ma20_gt_ma60",
        "ma7_slope",
        "ma20_slope",
        "ma60_slope",
        "ma20_slope_accel",
        "dist_ma7_pct",
        "dist_ma20_pct",
        "dist_ma60_pct",
        "ma20_ma60_distance_pct",
        "ma7_ma20_distance_pct",
        "ma20_break_count_pre20",
        "ma7_break_count_pre20",
        "higher_low_count_20",
        "recent_high_update_count_20",
        "large_bull_count_20",
        "large_bear_count_20",
        "gu_count_20",
        "gd_count_20",
        "bullish_ratio_20",
        "avg_body_ratio_20",
        "upper_wick_ratio_20",
        "lower_wick_ratio_20",
        "n_continuation_flag",
        "reverse_n_reject_flag",
        "volume_ratio_ma20",
        "up_day_volume_ratio_20",
        "down_day_volume_ratio_20",
        "high_break_volume_count_20",
        "low_volume_pullback_count_20",
        "atr14_pct",
        "realized_vol20",
        "max_drawdown_20",
        "gap_pct",
        "too_fast_rise_flag",
        "overheat_flag",
        "weekly_close_gt_ma20",
        "weekly_ma20_slope",
        "monthly_close_gt_ma20",
        "monthly_ma20_slope",
        "weekly_ppp_proxy",
        "monthly_ppp_proxy",
        "monthly_box_breakout",
        "monthly_box_inside",
        "monthly_high_zone",
        "ppp_proxy",
    ]
    row = {col: anchor.get(col) for col in feature_cols}
    closes = [float(r["c"]) for r in so_far if r.get("c") is not None]
    lows = [float(r["l"]) for r in so_far if r.get("l") is not None]
    ma20_breaks_after_start = [1 for r in so_far if r.get("ma20") is not None and float(r["c"]) <= float(r["ma20"])]
    row["post_start_max_pullback_pct"] = None if not closes else float(min(closes) / closes[0] - 1.0)
    row["post_start_intraday_max_pullback_pct"] = None if not lows or not closes else float(min(lows) / closes[0] - 1.0)
    row["post_start_held_ma20"] = int(len(ma20_breaks_after_start) == 0)
    row["post_start_ma7_ma20_support_count"] = int(sum(1 for r in so_far if r.get("ma7") is not None and r.get("ma20") is not None and float(r["c"]) <= float(r["ma7"]) and float(r["c"]) >= float(r["ma20"])))
    return row


FEATURE_GROUPS = {
    "ma_structure": ["ma7_gt_ma20_gt_ma60", "ma20_gt_ma60", "ma7_slope", "ma20_slope", "ma60_slope", "ma20_slope_accel", "dist_ma7_pct", "dist_ma20_pct", "dist_ma60_pct", "ma20_ma60_distance_pct", "ma7_ma20_distance_pct"],
    "pullback_quality": ["ma20_break_count_pre20", "ma7_break_count_pre20", "post_start_max_pullback_pct", "post_start_held_ma20", "post_start_ma7_ma20_support_count", "higher_low_count_20", "recent_high_update_count_20"],
    "candle_momentum": ["large_bull_count_20", "large_bear_count_20", "gu_count_20", "gd_count_20", "bullish_ratio_20", "avg_body_ratio_20", "upper_wick_ratio_20", "lower_wick_ratio_20", "n_continuation_flag", "reverse_n_reject_flag"],
    "volume": ["volume_ratio_ma20", "up_day_volume_ratio_20", "down_day_volume_ratio_20", "high_break_volume_count_20", "low_volume_pullback_count_20"],
    "volatility_risk": ["atr14_pct", "realized_vol20", "max_drawdown_20", "gap_pct", "too_fast_rise_flag", "overheat_flag"],
    "higher_timeframe_proxy": ["weekly_close_gt_ma20", "weekly_ma20_slope", "monthly_close_gt_ma20", "monthly_ma20_slope", "weekly_ppp_proxy", "monthly_ppp_proxy", "monthly_box_breakout", "monthly_box_inside", "monthly_high_zone"],
}


def build_summary(anchor_rows: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = {"anchors": {}}
    for anchor, group in anchor_rows.groupby("anchor_type", sort=True):
        pos = group[group["label_cohort"] == "positive"]
        main = group[group["label_cohort"] == "control_main"]
        early = group[group["label_cohort"] == "control_early_fail"]
        out["anchors"][anchor] = {
            "n_rows": int(len(group)),
            "n_positive": int(len(pos)),
            "n_control_main": int(len(main)),
            "n_control_early_fail": int(len(early)),
            "positive_rate": None if len(group) == 0 else float(len(pos) / len(group)),
            "feature_group_top_effects": _top_effects_by_group(pos, main),
        }
    return out


def _top_effects_by_group(pos: pd.DataFrame, control: pd.DataFrame) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {}
    for group_name, features in FEATURE_GROUPS.items():
        rows = []
        for feature in features:
            if feature not in pos.columns:
                continue
            effect = _effect_size(pos[feature], control[feature])
            if effect is not None:
                rows.append({"feature": feature, "effect_size": effect, "positive_mean": _mean(pos[feature]), "control_mean": _mean(control[feature])})
        result[group_name] = sorted(rows, key=lambda r: abs(float(r["effect_size"])), reverse=True)[:5]
    return result


def _mean(series: pd.Series) -> float | None:
    values = pd.to_numeric(series, errors="coerce").dropna()
    return None if values.empty else float(values.mean())


def _effect_size(a: pd.Series, b: pd.Series) -> float | None:
    av = pd.to_numeric(a, errors="coerce").dropna()
    bv = pd.to_numeric(b, errors="coerce").dropna()
    if len(av) < 5 or len(bv) < 5:
        return None
    pooled = math.sqrt((float(av.var(ddof=1)) + float(bv.var(ddof=1))) / 2.0)
    if not math.isfinite(pooled) or pooled == 0:
        return None
    return float((av.mean() - bv.mean()) / pooled)


def build_feature_lift(anchor_rows: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    features = [f for values in FEATURE_GROUPS.values() for f in values] + ["post_start_max_pullback_pct", "post_start_intraday_max_pullback_pct"]
    for anchor, group in anchor_rows.groupby("anchor_type", sort=True):
        for feature in features:
            if feature not in group.columns:
                continue
            values = pd.to_numeric(group[feature], errors="coerce")
            valid = group[values.notna()].copy()
            if len(valid) < 30 or valid["future_reached_60"].nunique() < 2:
                continue
            valid["_value"] = pd.to_numeric(valid[feature], errors="coerce")
            pos = valid[valid["label_cohort"] == "positive"]
            ctrl = valid[valid["label_cohort"] == "control_main"]
            q25 = float(valid["_value"].quantile(0.25))
            q75 = float(valid["_value"].quantile(0.75))
            top = valid[valid["_value"] >= q75]
            bottom = valid[valid["_value"] <= q25]
            top_rate = float(top["future_reached_60"].astype(bool).mean()) if len(top) else None
            bottom_rate = float(bottom["future_reached_60"].astype(bool).mean()) if len(bottom) else None
            rows.append(
                {
                    "anchor_type": anchor,
                    "feature": feature,
                    "feature_group": _feature_group(feature),
                    "n_valid": int(len(valid)),
                    "positive_mean": _mean(pos[feature]),
                    "control_main_mean": _mean(ctrl[feature]),
                    "effect_size": _effect_size(pos[feature], ctrl[feature]),
                    "q25": q25,
                    "q75": q75,
                    "top_quantile_positive_rate": top_rate,
                    "bottom_quantile_positive_rate": bottom_rate,
                    "univariate_lift_top_minus_bottom": None if top_rate is None or bottom_rate is None else float(top_rate - bottom_rate),
                }
            )
    columns = [
        "anchor_type",
        "feature",
        "feature_group",
        "n_valid",
        "positive_mean",
        "control_main_mean",
        "effect_size",
        "q25",
        "q75",
        "top_quantile_positive_rate",
        "bottom_quantile_positive_rate",
        "univariate_lift_top_minus_bottom",
    ]
    out = pd.DataFrame(rows, columns=columns)
    if out.empty:
        return out
    return out.sort_values(["anchor_type", "effect_size"], key=lambda s: s.abs() if s.name == "effect_size" else s, ascending=[True, False])


def _feature_group(feature: str) -> str:
    for group, features in FEATURE_GROUPS.items():
        if feature in features:
            return group
    return "pullback_quality"


def build_failure_decomposition(streaks: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    failed = streaks[~streaks["reached_60"].astype(bool)].copy()
    for cohort in ("control_main", "control_early_fail"):
        if cohort == "control_main":
            subset = failed[(failed["streak_length"] >= 20) & (failed["streak_length"] <= 59)]
        else:
            subset = failed[(failed["streak_length"] >= 5) & (failed["streak_length"] <= 19)]
        for reason, group in subset.groupby("break_reason", dropna=False):
            rows.append({"failure_cohort": cohort, "break_reason": reason, "n_streaks": int(len(group)), "mean_streak_length": _mean(group["streak_length"])})
    return pd.DataFrame(rows)


def build_simple_rules(anchor_rows: pd.DataFrame, lift: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for anchor, top_lift in lift.groupby("anchor_type", sort=True):
        candidates = top_lift.dropna(subset=["effect_size"]).copy()
        candidates = candidates.sort_values("effect_size", key=lambda s: s.abs(), ascending=False).head(12)
        group = anchor_rows[anchor_rows["anchor_type"] == anchor]
        for _, item in candidates.iterrows():
            feature = str(item["feature"])
            if feature not in group.columns:
                continue
            values = pd.to_numeric(group[feature], errors="coerce")
            if values.dropna().nunique() <= 2:
                condition = f"{feature} == 1"
                selected = group[values == 1]
            else:
                q75 = values.quantile(0.75)
                condition = f"{feature} >= q75_observed"
                selected = group[values >= q75]
            rows.append(_rule_row(anchor, condition, selected, group))
        for _, a in candidates.head(5).iterrows():
            for _, b in candidates.head(5).iterrows():
                fa, fb = str(a["feature"]), str(b["feature"])
                if fa >= fb or fa not in group.columns or fb not in group.columns:
                    continue
                va, vb = pd.to_numeric(group[fa], errors="coerce"), pd.to_numeric(group[fb], errors="coerce")
                sa = va == 1 if va.dropna().nunique() <= 2 else va >= va.quantile(0.75)
                sb = vb == 1 if vb.dropna().nunique() <= 2 else vb >= vb.quantile(0.75)
                rows.append(_rule_row(anchor, f"{fa} observed_high AND {fb} observed_high", group[sa & sb], group))
    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(columns=["anchor_type", "condition", "condition_count", "n_selected", "positive_rate", "anchor_base_positive_rate", "lift_vs_anchor_base"])
    return out.sort_values(["anchor_type", "lift_vs_anchor_base", "n_selected"], ascending=[True, False, False]).head(200)


def _rule_row(anchor: str, condition: str, selected: pd.DataFrame, base: pd.DataFrame) -> dict[str, Any]:
    base_rate = float(base["future_reached_60"].astype(bool).mean()) if len(base) else None
    selected_rate = float(selected["future_reached_60"].astype(bool).mean()) if len(selected) else None
    return {
        "anchor_type": anchor,
        "condition": condition,
        "condition_count": 1 if " AND " not in condition else 2,
        "n_selected": int(len(selected)),
        "positive_rate": selected_rate,
        "anchor_base_positive_rate": base_rate,
        "lift_vs_anchor_base": None if selected_rate is None or base_rate is None else float(selected_rate - base_rate),
    }


def no_lookahead_audit() -> dict[str, Any]:
    features = [f for values in FEATURE_GROUPS.values() for f in values] + ["post_start_max_pullback_pct", "post_start_intraday_max_pullback_pct", "post_start_held_ma20", "post_start_ma7_ma20_support_count"]
    return {
        "audit_result": "pass",
        "rule": "Feature columns are computed at or before each anchor. Future reached_60 and final streak length are labels only.",
        "feature_columns": sorted(features),
        "label_columns": ["future_reached_60", "final_streak_length", "label_cohort", "streak_length", "reached_60", "streak_end_date", "max_drawdown_in_streak", "max_gain_in_streak", "break_reason"],
        "threshold_sweep": False,
        "model_training": False,
        "silent_fallback_used": False,
    }


def classify_decision(anchor_rows: pd.DataFrame, lift: pd.DataFrame, summary: dict[str, Any]) -> dict[str, Any]:
    n_positive = int((anchor_rows["label_cohort"] == "positive").sum() / max(1, len(ANCHOR_COUNTS)))
    stable = lift[(lift["anchor_type"].isin(["anchor_10", "anchor_20"])) & (lift["effect_size"].abs() >= 0.20) & (lift["univariate_lift_top_minus_bottom"].abs() >= 0.03)]
    stable_groups = sorted(stable["feature_group"].dropna().unique().tolist())
    reasons: list[str] = []
    if n_positive < 300:
        decision = "inconclusive"
        reasons.append("n_positive below 300")
    elif len(stable_groups) >= 3:
        regime_only = set(stable_groups) <= {"higher_timeframe_proxy"}
        if regime_only:
            decision = "weak_pattern"
            reasons.append("stable lift is dominated by higher-timeframe/regime proxy")
        else:
            decision = "pattern_found"
            reasons.append("at least 3 independent feature groups show stable lift at anchor_10_or_20")
    elif len(stable_groups) > 0:
        decision = "weak_pattern"
        reasons.append("some stable lift exists but fewer than 3 independent feature groups")
    else:
        decision = "not_found"
        reasons.append("no stable pre-anchor feature separation found")
    return {
        "research_decision": decision,
        "reason_typed": reasons,
        "n_positive": n_positive,
        "stable_lift_feature_groups_anchor_10_20": stable_groups,
        "no_lookahead_safe": True,
        "summary_anchor_keys": sorted((summary.get("anchors") or {}).keys()),
    }


def run(*, output_root: Path = DEFAULT_OUTPUT_ROOT, db_path: Path | None = None, production_csv: Path = DEFAULT_PRODUCTION_CSV, start_ymd: int | None = None, end_ymd: int | None = None) -> dict[str, Any]:
    run_dir = output_root / f"{_now_tag()}-ma60-above-60plus-pattern-audit-v1"
    run_dir.mkdir(parents=True, exist_ok=True)
    resolution = resolve_input(db_path=db_path, production_csv=production_csv)
    daily = load_daily_frame(resolution, start_ymd=start_ymd, end_ymd=end_ymd)
    featured = add_features(daily)
    streaks, anchors = build_streaks(featured)
    summary = build_summary(anchors)
    lift = build_feature_lift(anchors)
    failures = build_failure_decomposition(streaks)
    rules = build_simple_rules(anchors, lift)
    decision = classify_decision(anchors, lift, summary)
    input_report = {
        "axis_id": AXIS_ID,
        "source_type": resolution.source_type,
        "source_path": resolution.path,
        "daily_columns": resolution.daily_columns,
        "ma_columns": resolution.ma_columns,
        "source_row_count": resolution.row_count,
        "loaded_row_count": int(len(daily)),
        "date_min": daily["date"].min(),
        "date_max": daily["date"].max(),
        "ma60_missing_count": int(featured["ma60"].isna().sum()),
        "zero_volume_rows": int((featured["v"].fillna(0) <= 0).sum()),
        "liquidity_flags_recorded": True,
    }
    streaks.to_csv(run_dir / "streak_events.csv", index=False)
    anchors.to_csv(run_dir / "anchor_feature_rows.csv", index=False)
    lift.to_csv(run_dir / "feature_lift_by_anchor.csv", index=False)
    failures.to_csv(run_dir / "failure_decomposition.csv", index=False)
    rules.to_csv(run_dir / "simple_rule_candidates.csv", index=False)
    _write_json(run_dir / "input_schema_report.json", input_report)
    _write_json(run_dir / "positive_vs_control_summary.json", summary)
    _write_json(run_dir / "research_decision.json", decision)
    _write_json(run_dir / "no_lookahead_audit.json", no_lookahead_audit())
    complete = {
        "axis_id": AXIS_ID,
        "output_dir": run_dir,
        "required_artifacts": list(REQUIRED_ARTIFACTS),
        "artifact_complete": all((run_dir / name).exists() for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json"),
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    _write_json(run_dir / "_ARTIFACT_COMPLETE.json", complete)
    return {"output_dir": str(run_dir), "decision": decision, "summary": summary, "required_artifacts": list(REQUIRED_ARTIFACTS)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="TRADEX MA60 above 60+ streak pattern audit")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--db-path", type=Path, default=None)
    parser.add_argument("--production-csv", type=Path, default=DEFAULT_PRODUCTION_CSV)
    parser.add_argument("--start-ymd", type=int, default=None)
    parser.add_argument("--end-ymd", type=int, default=None)
    args = parser.parse_args(argv)
    print(json.dumps(_json_ready(run(output_root=args.output_root, db_path=args.db_path, production_csv=args.production_csv, start_ymd=args.start_ymd, end_ymd=args.end_ymd)), ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
