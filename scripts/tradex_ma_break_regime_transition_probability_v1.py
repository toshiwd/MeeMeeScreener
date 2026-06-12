from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.services.codex_bridge_service import get_rankings_freshness, get_runtime_stock_db_status
from shared.runtime_stock_db_contract import inspect_runtime_stock_db, resolve_runtime_stock_db_path


AXIS_ID = "ma_break_regime_transition_probability_v1"
PREVIOUS_AXIS_ID = "ma_break_recovery_by_regime_v1"
DEFAULT_OUT_ROOT = Path("G:/Tradex/ma_break_regime_transition_probability_v1")
DEFAULT_PREVIOUS_EVENTS = Path(
    "G:/Tradex/ma_break_recovery_by_regime_v1/20260603T091908Z-ma-break-recovery-by-regime-v1/ma_break_events_by_regime.csv"
)
MA_WINDOWS = (5, 7, 20, 60, 100, 200)
HORIZONS = (5, 10, 20, 60)
SEVERE_LOSS_THRESHOLD_PCT = -10.0
SEVERE_DRAWDOWN_THRESHOLD_PCT = -10.0
RANGE_RET_ABS_THRESHOLD_PCT = 3.0
RANGE_WIDTH_THRESHOLD_PCT = 18.0
REBREAK_LOOKAHEAD_BARS = 10
MIN_CONTRAST_SAMPLE = 200
MIN_YEAR_SAMPLE = 50
REQUIRED_ARTIFACTS = (
    "input_audit.json",
    "transition_definition.json",
    "ma_break_transition_events.csv",
    "transition_probability_by_ma_regime_horizon.csv",
    "transition_contrast_summary.json",
    "failed_recovery_summary.csv",
    "yearly_stability_summary.csv",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
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


def _date_expr(column: str) -> str:
    return f"""
    CASE
      WHEN {column} BETWEEN 19000101 AND 20991231 THEN CAST({column} AS INTEGER)
      WHEN {column} >= 1000000000000 THEN CAST(strftime(to_timestamp({column} / 1000), '%Y%m%d') AS INTEGER)
      WHEN {column} >= 100000000 THEN CAST(strftime(to_timestamp({column}), '%Y%m%d') AS INTEGER)
      ELSE CAST(regexp_replace(CAST({column} AS VARCHAR), '[^0-9]', '', 'g') AS INTEGER)
    END
    """


def _load_daily_bars(conn: duckdb.DuckDBPyConnection, *, start_ymd: int, end_ymd: int | None) -> pd.DataFrame:
    end_clause = "" if end_ymd is None else "AND ymd <= ?"
    params: list[Any] = [int(start_ymd)]
    if end_ymd is not None:
        params.append(int(end_ymd))
    query = f"""
    WITH normalized AS (
      SELECT
        CAST(code AS VARCHAR) AS code,
        {_date_expr("date")} AS ymd,
        CAST(o AS DOUBLE) AS o,
        CAST(h AS DOUBLE) AS h,
        CAST(l AS DOUBLE) AS l,
        CAST(c AS DOUBLE) AS c,
        CAST(v AS DOUBLE) AS v,
        lower(coalesce(source, '')) AS source
      FROM daily_bars
      WHERE o > 0 AND h > 0 AND l > 0 AND c > 0
        AND lower(coalesce(source, '')) IN ('pan', 'txt', 'confirmed')
    )
    SELECT code, ymd, o, h, l, c, v, source
    FROM normalized
    WHERE ymd >= ? {end_clause}
    ORDER BY code, ymd
    """
    frame = conn.execute(query, params).fetchdf()
    if frame.empty:
        raise RuntimeError("daily_bars query returned no rows")
    frame["code"] = frame["code"].astype(str)
    frame["ymd"] = pd.to_numeric(frame["ymd"], errors="coerce").astype("Int64")
    frame = frame.dropna(subset=["ymd", "c"]).copy()
    frame["ymd"] = frame["ymd"].astype(int)
    return frame.sort_values(["code", "ymd"], kind="stable").reset_index(drop=True)


def _load_monthly(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    query = f"""
    SELECT
      CAST(mb.code AS VARCHAR) AS code,
      {_date_expr("mb.month")} AS month_ymd,
      CAST(mb.c AS DOUBLE) AS monthly_close,
      CAST(mm.ma20 AS DOUBLE) AS monthly_ma20
    FROM monthly_bars mb
    LEFT JOIN monthly_ma mm ON mb.code = mm.code AND mb.month = mm.month
    WHERE mb.c > 0
    ORDER BY mb.code, mb.month
    """
    frame = conn.execute(query).fetchdf()
    if frame.empty:
        return frame
    frame["month_dt"] = pd.to_datetime(frame["month_ymd"].astype(str), format="%Y%m%d", errors="coerce")
    frame["monthly_ma20_slope_3m_pct"] = frame.groupby("code")["monthly_ma20"].transform(lambda s: (s / s.shift(3) - 1.0) * 100.0)
    frame["monthly_above_ma20"] = frame["monthly_close"] >= frame["monthly_ma20"]
    frame["monthly_ma20_slope_state"] = pd.cut(
        frame["monthly_ma20_slope_3m_pct"],
        bins=[-float("inf"), -1.0, 1.0, float("inf")],
        labels=["down", "flat", "up"],
    ).astype("object")
    return frame


def _add_daily_features(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    grouped = out.groupby("code", group_keys=False)
    for window in MA_WINDOWS:
        out[f"ma{window}"] = grouped["c"].transform(lambda s, w=window: s.rolling(w, min_periods=w).mean())
        out[f"prev_c_ma{window}"] = grouped["c"].shift(1)
        out[f"prev_ma{window}"] = grouped[f"ma{window}"].shift(1)
        out[f"below_ma{window}"] = out["c"] < out[f"ma{window}"]
        out[f"break_ma{window}"] = (out[f"prev_c_ma{window}"] >= out[f"prev_ma{window}"]) & out[f"below_ma{window}"]
    for window in (20, 60, 100):
        out[f"ma{window}_slope_20d_pct"] = grouped[f"ma{window}"].transform(lambda s: (s / s.shift(20) - 1.0) * 100.0)
        out[f"ma{window}_slope_state"] = pd.cut(
            out[f"ma{window}_slope_20d_pct"],
            bins=[-float("inf"), -0.5, 0.5, float("inf")],
            labels=["down", "flat", "up"],
        ).astype("object")
    out["ma_alignment"] = "mixed_stack"
    out.loc[(out["ma20"] > out["ma60"]) & (out["ma60"] > out["ma100"]) & (out["ma100"] > out["ma200"]), "ma_alignment"] = "bullish_stack"
    out.loc[(out["ma20"] < out["ma60"]) & (out["ma60"] < out["ma100"]) & (out["ma100"] < out["ma200"]), "ma_alignment"] = "bearish_stack"
    out["ma200_stack_bucket"] = "mixed_stack"
    out.loc[out["ma_alignment"] == "bearish_stack", "ma200_stack_bucket"] = "bearish_stack"
    out.loc[out["ma_alignment"] == "bullish_stack", "ma200_stack_bucket"] = "non_bearish"
    out["daily_close_vs_ma60_pct"] = (out["c"] / out["ma60"] - 1.0) * 100.0
    out["daily_close_vs_ma100_pct"] = (out["c"] / out["ma100"] - 1.0) * 100.0
    out["oscillating_around_ma60_100"] = (
        out["daily_close_vs_ma60_pct"].abs().le(5.0)
        & out["daily_close_vs_ma100_pct"].abs().le(7.5)
        & out["ma60_slope_20d_pct"].abs().le(1.0)
        & out["ma100_slope_20d_pct"].abs().le(1.0)
    )
    return out


def _weekly_context(daily: pd.DataFrame) -> pd.DataFrame:
    weekly = daily[["code", "ymd", "c"]].copy()
    weekly["date"] = pd.to_datetime(weekly["ymd"].astype(str), format="%Y%m%d")
    weekly = (
        weekly.set_index("date")
        .groupby("code", group_keys=False)
        .resample("W-FRI")
        .agg({"code": "last", "ymd": "max", "c": "last"})
        .dropna(subset=["code", "ymd", "c"])
        .reset_index(drop=True)
    )
    grouped = weekly.groupby("code", group_keys=False)
    weekly["weekly_ma20"] = grouped["c"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    weekly["weekly_ma60"] = grouped["c"].transform(lambda s: s.rolling(60, min_periods=60).mean())
    weekly["weekly_ma20_slope_4w_pct"] = grouped["weekly_ma20"].transform(lambda s: (s / s.shift(4) - 1.0) * 100.0)
    weekly["weekly_ma60_slope_4w_pct"] = grouped["weekly_ma60"].transform(lambda s: (s / s.shift(4) - 1.0) * 100.0)
    weekly["weekly_above_ma20"] = weekly["c"] >= weekly["weekly_ma20"]
    weekly["weekly_above_ma60"] = weekly["c"] >= weekly["weekly_ma60"]
    for col in ("weekly_ma20_slope_4w_pct", "weekly_ma60_slope_4w_pct"):
        weekly[col.replace("_4w_pct", "_state")] = pd.cut(
            weekly[col],
            bins=[-float("inf"), -0.5, 0.5, float("inf")],
            labels=["down", "flat", "up"],
        ).astype("object")
    return weekly.rename(columns={"ymd": "week_ymd"})


def _attach_regime_context(daily: pd.DataFrame, monthly: pd.DataFrame, weekly: pd.DataFrame) -> pd.DataFrame:
    out = daily.copy()
    out["event_dt"] = pd.to_datetime(out["ymd"].astype(str), format="%Y%m%d")
    if not monthly.empty:
        parts = []
        month_cols = ["code", "month_dt", "monthly_above_ma20", "monthly_ma20_slope_state", "monthly_ma20_slope_3m_pct"]
        for code, rows in out.groupby("code", sort=False):
            m = monthly[monthly["code"] == code][month_cols].dropna(subset=["month_dt"]).sort_values("month_dt")
            if m.empty:
                chunk = rows.copy()
                for col in month_cols[2:]:
                    chunk[col] = None
                parts.append(chunk)
            else:
                parts.append(pd.merge_asof(rows.sort_values("event_dt"), m, left_on="event_dt", right_on="month_dt", by="code", direction="backward"))
        out = pd.concat(parts, ignore_index=True)
    else:
        out["monthly_above_ma20"] = None
        out["monthly_ma20_slope_state"] = None

    parts = []
    weekly_cols = [
        "code",
        "week_ymd",
        "weekly_above_ma20",
        "weekly_above_ma60",
        "weekly_ma20_slope_state",
        "weekly_ma60_slope_state",
    ]
    for code, rows in out.groupby("code", sort=False):
        w = weekly[weekly["code"] == code][weekly_cols].copy().sort_values("week_ymd")
        w["week_dt"] = pd.to_datetime(w["week_ymd"].astype(int).astype(str), format="%Y%m%d")
        parts.append(pd.merge_asof(rows.sort_values("event_dt"), w.drop(columns=["code"]), left_on="event_dt", right_on="week_dt", direction="backward"))
    out = pd.concat(parts, ignore_index=True)

    monthly_up = (out["monthly_above_ma20"] == True) & (out["monthly_ma20_slope_state"].isin(["up", "flat"]))
    weekly_up = (out["weekly_above_ma20"] == True) & (out["weekly_ma20_slope_state"].isin(["up", "flat"]))
    trend_down = (
        ((out["c"] < out["ma60"]) & (out["ma60_slope_state"] == "down"))
        | ((out["c"] < out["ma100"]) & (out["ma100_slope_state"] == "down"))
        | ((out["ma20"] < out["ma60"]) & (out["ma60_slope_state"] == "down"))
    )
    range_candidate = out["oscillating_around_ma60_100"].fillna(False)
    uptrend_pullback = (monthly_up | weekly_up) & ((out["c"] < out["ma20"]) | (out["c"] < out["ma60"])) & (
        out["ma60_slope_state"].isin(["up", "flat"]) | out["ma100_slope_state"].isin(["up", "flat"])
    )
    out["range_vs_trend"] = "other"
    out.loc[range_candidate, "range_vs_trend"] = "range_candidate"
    out.loc[uptrend_pullback, "range_vs_trend"] = "uptrend_pullback_candidate"
    out.loc[trend_down, "range_vs_trend"] = "trend_down_candidate"
    return out.sort_values(["code", "ymd"], kind="stable").reset_index(drop=True)


def _transition_state(
    *,
    recovered_once: bool,
    rebreak_after_recovery: bool | None,
    still_below_broken_ma: bool,
    future_close: float,
    future_ma20: float,
    future_ma60: float,
    future_ma100: float,
    future_ma20_slope_state: str | None,
    future_ma60_slope_state: str | None,
    future_ret_pct: float,
    max_drawdown_pct: float,
    future_range_width_pct: float | None,
) -> str:
    if any(pd.isna(value) for value in [future_close, future_ma20, future_ma60, future_ma100, future_ret_pct, max_drawdown_pct]):
        return "unresolved"
    severe_drawdown = max_drawdown_pct <= SEVERE_DRAWDOWN_THRESHOLD_PCT
    if recovered_once and (rebreak_after_recovery or future_ret_pct < 0 or severe_drawdown):
        return "failed_recovery"
    if future_close > future_ma20 and future_ma20_slope_state == "up" and future_close > future_ma60 and future_ret_pct > 0 and not severe_drawdown:
        return "uptrend_continuation"
    if (future_close < future_ma60 or future_close < future_ma100) and future_ma20_slope_state == "down" and future_ret_pct < 0 and severe_drawdown:
        return "downtrend_continuation"
    if (
        recovered_once
        and abs(future_ret_pct) <= RANGE_RET_ABS_THRESHOLD_PCT
        and future_ma60_slope_state in {"flat", "up"}
        and not severe_drawdown
        and future_range_width_pct is not None
        and future_range_width_pct <= RANGE_WIDTH_THRESHOLD_PCT
    ):
        return "range_reversion"
    if still_below_broken_ma and future_ret_pct < 0 and (future_ma20_slope_state == "down" or future_ma60_slope_state == "down"):
        return "downtrend_continuation"
    return "unresolved"


def _build_transition_events(frame: pd.DataFrame) -> pd.DataFrame:
    event_rows: list[dict[str, Any]] = []
    max_horizon = max(HORIZONS)
    for code, rows in frame.groupby("code", sort=False):
        rows = rows.reset_index(drop=True)
        closes = rows["c"].to_numpy()
        highs = rows["h"].to_numpy()
        lows = rows["l"].to_numpy()
        ymd_values = rows["ymd"].to_numpy()
        ma_values = {window: rows[f"ma{window}"].to_numpy() for window in MA_WINDOWS}
        break_values = {window: rows[f"break_ma{window}"].fillna(False).astype(bool).to_numpy() for window in MA_WINDOWS}
        for idx in range(len(rows)):
            base = rows.iloc[idx]
            for window in MA_WINDOWS:
                broken_ma_value = ma_values[window][idx]
                if not bool(break_values[window][idx]) or pd.isna(broken_ma_value):
                    continue
                end = min(len(rows), idx + max_horizon + 1)
                recovery_bars = None
                recovered_once_by_60 = False
                if idx + 1 < end:
                    recovered_positions = (closes[idx + 1 : end] >= ma_values[window][idx + 1 : end]).nonzero()[0]
                    if len(recovered_positions):
                        recovery_bars = int(recovered_positions[0] + 1)
                        recovered_once_by_60 = True
                for horizon in HORIZONS:
                    target_idx = idx + horizon
                    year = int(str(int(ymd_values[idx]))[:4])
                    if target_idx >= len(rows):
                        event_rows.append(
                            {
                                "code": code,
                                "event_date": int(ymd_values[idx]),
                                "event_year": year,
                                "target_ma": f"ma{window}",
                                "target_window": window,
                                "horizon": horizon,
                                "transition_state": "unresolved",
                                "unresolved_reason": "insufficient_future_bars",
                                "range_vs_trend": str(base["range_vs_trend"]),
                                "ma_alignment": str(base["ma_alignment"]),
                                "ma200_stack_bucket": str(base["ma200_stack_bucket"]),
                            }
                        )
                        continue
                    future_slice = slice(idx + 1, target_idx + 1)
                    event_close = float(closes[idx])
                    future_close = float(closes[target_idx])
                    ret_pct = (future_close / event_close - 1.0) * 100.0
                    max_drawdown_pct = (float(lows[future_slice].min()) / event_close - 1.0) * 100.0
                    severe_loss = ret_pct <= SEVERE_LOSS_THRESHOLD_PCT
                    recovered_once = recovery_bars is not None and recovery_bars <= horizon
                    still_below = future_close < float(ma_values[window][target_idx])
                    rebreak_after_recovery = None
                    if recovered_once:
                        rebreak_start = idx + int(recovery_bars) + 1
                        rebreak_end = min(target_idx + 1, rebreak_start + REBREAK_LOOKAHEAD_BARS)
                        rebreak_after_recovery = bool(rebreak_start < rebreak_end and (closes[rebreak_start:rebreak_end] < ma_values[window][rebreak_start:rebreak_end]).any())
                    range_low_start = max(0, idx - 20)
                    range_high = float(highs[range_low_start : idx + 1].max())
                    range_low = float(lows[range_low_start : idx + 1].min())
                    range_width_pct = (range_high / range_low - 1.0) * 100.0 if range_low > 0 else None
                    future_row = rows.iloc[target_idx]
                    transition_state = _transition_state(
                        recovered_once=recovered_once,
                        rebreak_after_recovery=rebreak_after_recovery,
                        still_below_broken_ma=still_below,
                        future_close=future_close,
                        future_ma20=float(future_row["ma20"]),
                        future_ma60=float(future_row["ma60"]),
                        future_ma100=float(future_row["ma100"]),
                        future_ma20_slope_state=None if pd.isna(future_row["ma20_slope_state"]) else str(future_row["ma20_slope_state"]),
                        future_ma60_slope_state=None if pd.isna(future_row["ma60_slope_state"]) else str(future_row["ma60_slope_state"]),
                        future_ret_pct=ret_pct,
                        max_drawdown_pct=max_drawdown_pct,
                        future_range_width_pct=range_width_pct,
                    )
                    event_rows.append(
                        {
                            "code": code,
                            "event_date": int(ymd_values[idx]),
                            "event_year": year,
                            "target_ma": f"ma{window}",
                            "target_window": window,
                            "horizon": horizon,
                            "transition_state": transition_state,
                            "unresolved_reason": "" if transition_state != "unresolved" else "conflicting_state",
                            "close": event_close,
                            "future_close": future_close,
                            "ret_horizon_pct": ret_pct,
                            "max_drawdown_pct": max_drawdown_pct,
                            "severe_loss": severe_loss,
                            "recovered_once": recovered_once,
                            "recovered_once_by_60": recovered_once_by_60,
                            "recovery_bars": recovery_bars,
                            "rebreak_after_recovery": rebreak_after_recovery,
                            "still_below_broken_ma": still_below,
                            "range_vs_trend": str(base["range_vs_trend"]),
                            "ma_alignment": str(base["ma_alignment"]),
                            "ma200_stack_bucket": str(base["ma200_stack_bucket"]),
                            "monthly_above_ma20": bool(base["monthly_above_ma20"]) if pd.notna(base["monthly_above_ma20"]) else None,
                            "weekly_above_ma20": bool(base["weekly_above_ma20"]) if pd.notna(base["weekly_above_ma20"]) else None,
                            "weekly_above_ma60": bool(base["weekly_above_ma60"]) if pd.notna(base["weekly_above_ma60"]) else None,
                        }
                    )
    return pd.DataFrame(event_rows)


def _rate(series: pd.Series) -> float | None:
    valid = series.dropna()
    if valid.empty:
        return None
    return float(valid.mean())


def _mean(series: pd.Series) -> float | None:
    valid = pd.to_numeric(series, errors="coerce").dropna()
    if valid.empty:
        return None
    return float(valid.mean())


def _median(series: pd.Series) -> float | None:
    valid = pd.to_numeric(series, errors="coerce").dropna()
    if valid.empty:
        return None
    return float(valid.median())


def _metric_row(group: pd.DataFrame, *, target_ma: str, regime_axis: str, regime_value: str, horizon: int) -> dict[str, Any]:
    states = group["transition_state"]
    return {
        "target_ma": target_ma,
        "regime_axis": regime_axis,
        "regime_value": regime_value,
        "horizon": int(horizon),
        "event_count": int(len(group)),
        "unique_symbol_count": int(group["code"].nunique()),
        "uptrend_continuation_rate": float((states == "uptrend_continuation").mean()),
        "downtrend_continuation_rate": float((states == "downtrend_continuation").mean()),
        "range_reversion_rate": float((states == "range_reversion").mean()),
        "failed_recovery_rate": float((states == "failed_recovery").mean()),
        "unresolved_rate": float((states == "unresolved").mean()),
        "mean_ret_horizon": _mean(group["ret_horizon_pct"]),
        "median_ret_horizon": _median(group["ret_horizon_pct"]),
        "severe_loss_rate": _rate(group["severe_loss"]),
        "mean_max_drawdown": _mean(group["max_drawdown_pct"]),
        "median_max_drawdown": _median(group["max_drawdown_pct"]),
        "rebreak_rate_after_recovery": _rate(group["rebreak_after_recovery"]),
        "still_below_broken_ma_rate": _rate(group["still_below_broken_ma"]),
    }


def _summary(events: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    axes = ("range_vs_trend", "ma200_stack_bucket", "ma_alignment")
    for (target_ma, horizon), group in events.groupby(["target_ma", "horizon"], sort=False):
        rows.append(_metric_row(group, target_ma=target_ma, regime_axis="all", regime_value="all", horizon=int(horizon)))
        for axis in axes:
            for value, sub in group.groupby(axis, dropna=False):
                rows.append(_metric_row(sub, target_ma=target_ma, regime_axis=axis, regime_value=str(value), horizon=int(horizon)))
    return pd.DataFrame(rows).sort_values(["target_ma", "regime_axis", "regime_value", "horizon"], kind="stable")


def _contrast(summary: pd.DataFrame) -> dict[str, Any]:
    specs = [
        ("ma60", "range_vs_trend", "trend_down_candidate", "range_candidate"),
        ("ma60", "range_vs_trend", "trend_down_candidate", "uptrend_pullback_candidate"),
        ("ma100", "range_vs_trend", "trend_down_candidate", "range_candidate"),
        ("ma100", "range_vs_trend", "trend_down_candidate", "uptrend_pullback_candidate"),
        ("ma200", "ma200_stack_bucket", "bearish_stack", "non_bearish"),
        ("ma200", "ma200_stack_bucket", "bearish_stack", "mixed_stack"),
        ("ma20", "range_vs_trend", "uptrend_pullback_candidate", "trend_down_candidate"),
    ]
    comparisons: list[dict[str, Any]] = []
    for target_ma, axis, first, second in specs:
        for horizon in HORIZONS:
            subset = summary[(summary["target_ma"] == target_ma) & (summary["regime_axis"] == axis) & (summary["horizon"] == horizon)]
            first_row = subset[subset["regime_value"] == first]
            second_row = subset[subset["regime_value"] == second]
            payload: dict[str, Any] = {"target_ma": target_ma, "regime_axis": axis, "horizon": horizon, "first": first, "second": second}
            if first_row.empty or second_row.empty:
                payload["status"] = "missing_group"
            else:
                a = first_row.iloc[0].to_dict()
                b = second_row.iloc[0].to_dict()
                payload["status"] = "ready"
                payload["first_metrics"] = a
                payload["second_metrics"] = b
                payload["deltas"] = {
                    "downtrend_continuation_rate_delta": a["downtrend_continuation_rate"] - b["downtrend_continuation_rate"],
                    "failed_recovery_rate_delta": a["failed_recovery_rate"] - b["failed_recovery_rate"],
                    "uptrend_continuation_rate_delta": a["uptrend_continuation_rate"] - b["uptrend_continuation_rate"],
                    "severe_loss_rate_delta": a["severe_loss_rate"] - b["severe_loss_rate"],
                    "mean_max_drawdown_delta": a["mean_max_drawdown"] - b["mean_max_drawdown"],
                }
            comparisons.append(payload)
    return {"axis_id": AXIS_ID, "required_contrasts": comparisons}


def _yearly_stability(events: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for (target_ma, horizon, axis_value, year), group in events.groupby(["target_ma", "horizon", "range_vs_trend", "event_year"], sort=False):
        row = _metric_row(group, target_ma=target_ma, regime_axis="range_vs_trend", regime_value=str(axis_value), horizon=int(horizon))
        row["event_year"] = int(year)
        row["sample_status"] = "sufficient" if int(row["event_count"]) >= MIN_YEAR_SAMPLE else "insufficient_sample"
        rows.append(row)
    return pd.DataFrame(rows).sort_values(["target_ma", "regime_value", "horizon", "event_year"], kind="stable")


def _failed_recovery_summary(events: pd.DataFrame) -> pd.DataFrame:
    failed = events[events["transition_state"] == "failed_recovery"].copy()
    if failed.empty:
        return pd.DataFrame()
    rows: list[dict[str, Any]] = []
    for (target_ma, horizon, regime), group in failed.groupby(["target_ma", "horizon", "range_vs_trend"], sort=False):
        rows.append(
            {
                "target_ma": target_ma,
                "horizon": int(horizon),
                "range_vs_trend": regime,
                "failed_count": int(len(group)),
                "unique_symbol_count": int(group["code"].nunique()),
                "mean_ret_horizon": _mean(group["ret_horizon_pct"]),
                "median_ret_horizon": _median(group["ret_horizon_pct"]),
                "mean_max_drawdown": _mean(group["max_drawdown_pct"]),
                "rebreak_rate_after_recovery": _rate(group["rebreak_after_recovery"]),
                "still_below_broken_ma_rate": _rate(group["still_below_broken_ma"]),
            }
        )
    return pd.DataFrame(rows).sort_values(["target_ma", "range_vs_trend", "horizon"], kind="stable")


def _decision(contrast: dict[str, Any], yearly: pd.DataFrame) -> dict[str, Any]:
    bad_pick_reasons: list[dict[str, Any]] = []
    pullback_reasons: list[dict[str, Any]] = []
    for comp in contrast["required_contrasts"]:
        if comp.get("status") != "ready" or comp["horizon"] != 20:
            continue
        a = comp["first_metrics"]
        b = comp["second_metrics"]
        if int(a["event_count"]) < MIN_CONTRAST_SAMPLE or int(b["event_count"]) < MIN_CONTRAST_SAMPLE:
            continue
        deltas = comp["deltas"]
        if comp["first"] == "trend_down_candidate" and comp["second"] in {"range_candidate", "uptrend_pullback_candidate"}:
            if deltas["downtrend_continuation_rate_delta"] >= 0.02 or deltas["failed_recovery_rate_delta"] >= 0.03 or deltas["severe_loss_rate_delta"] >= 0.02:
                bad_pick_reasons.append(
                    {
                        "target_ma": comp["target_ma"],
                        "second": comp["second"],
                        "typed_reason": "trend_down_has_higher_bad_transition_risk",
                        "deltas": deltas,
                        "first_event_count": int(a["event_count"]),
                        "second_event_count": int(b["event_count"]),
                    }
                )
        if comp["first"] == "uptrend_pullback_candidate" and comp["second"] == "trend_down_candidate":
            if deltas["uptrend_continuation_rate_delta"] >= 0.03 and deltas["severe_loss_rate_delta"] <= 0:
                pullback_reasons.append(
                    {
                        "target_ma": comp["target_ma"],
                        "typed_reason": "uptrend_pullback_has_better_uptrend_transition",
                        "deltas": deltas,
                        "first_event_count": int(a["event_count"]),
                        "second_event_count": int(b["event_count"]),
                    }
                )
    stability_notes: list[dict[str, Any]] = []
    for target_ma in ("ma60", "ma100"):
        subset = yearly[
            (yearly["target_ma"] == target_ma)
            & (yearly["horizon"] == 20)
            & (yearly["regime_value"].isin(["trend_down_candidate", "range_candidate"]))
            & (yearly["sample_status"] == "sufficient")
        ]
        years = sorted(set(subset["event_year"].astype(int).tolist()))
        favorable = 0
        comparable = 0
        for year in years:
            t = subset[(subset["event_year"] == year) & (subset["regime_value"] == "trend_down_candidate")]
            r = subset[(subset["event_year"] == year) & (subset["regime_value"] == "range_candidate")]
            if t.empty or r.empty:
                continue
            comparable += 1
            if float(t.iloc[0]["failed_recovery_rate"]) >= float(r.iloc[0]["failed_recovery_rate"]) or float(t.iloc[0]["severe_loss_rate"]) >= float(r.iloc[0]["severe_loss_rate"]):
                favorable += 1
        stability_notes.append({"target_ma": target_ma, "comparable_years": comparable, "years_supporting_bad_pick_axis": favorable})
    stable = any(note["comparable_years"] >= 3 and note["years_supporting_bad_pick_axis"] >= max(2, note["comparable_years"] - 1) for note in stability_notes)
    if bad_pick_reasons and stable:
        decision = "keep_for_bad_pick_removal_next"
        reason = "transition_probabilities_and_yearly_stability_support_bad_pick_axis"
    elif pullback_reasons and not bad_pick_reasons:
        decision = "keep_for_buy_pullback_feature_next"
        reason = "uptrend_pullback_transition_advantage_without_bad_pick_axis"
    elif bad_pick_reasons or pullback_reasons:
        decision = "hold"
        reason = "direction_exists_but_yearly_stability_or_contrast_is_incomplete"
    else:
        decision = "drop"
        reason = "transition_rates_do_not_differ_meaningfully_by_regime"
    return {
        "axis_id": AXIS_ID,
        "candidate_local_decision": decision,
        "session_aggregate_decision": decision,
        "authoritative_rollup_decision": decision,
        "reason": reason,
        "bad_pick_reasons": bad_pick_reasons,
        "buy_pullback_reasons": pullback_reasons,
        "yearly_stability_notes": stability_notes,
        "non_scope": [
            "no MeeMee reflection",
            "no runtime DuckDB writes",
            "no ranking changes",
            "no publish",
            "no candidate generation changes",
            "no buy/sell rule promotion",
            "no volume condition",
            "no stock-specific correction",
            "no bad-pick removal implementation",
        ],
    }


def _transition_definition() -> dict[str, Any]:
    return {
        "axis_id": AXIS_ID,
        "base_event": {
            "same_as_previous_axis": PREVIOUS_AXIS_ID,
            "definition": "previous close >= target MA and current close < target MA",
            "ma_windows": list(MA_WINDOWS),
            "horizons": list(HORIZONS),
            "confirmed_bars_only": True,
        },
        "transition_states": {
            "uptrend_continuation": "future close > MA20, MA20 slope up, future close > MA60, ret_horizon > 0, no severe drawdown",
            "downtrend_continuation": "future close < MA60/MA100 with down slope and negative return plus severe drawdown, or still below broken MA with negative return and down slope",
            "range_reversion": "recovered above broken MA, near-flat return, MA60 not down, recent range width contained, no severe drawdown",
            "failed_recovery": "recovered once then re-broke same MA within horizon, or recovered but ret_horizon negative, or recovered but severe drawdown",
            "unresolved": "insufficient bars or conflicting state",
        },
        "thresholds": {
            "severe_loss_pct": SEVERE_LOSS_THRESHOLD_PCT,
            "severe_drawdown_pct": SEVERE_DRAWDOWN_THRESHOLD_PCT,
            "range_ret_abs_pct": RANGE_RET_ABS_THRESHOLD_PCT,
            "range_width_pct": RANGE_WIDTH_THRESHOLD_PCT,
            "rebreak_lookahead_bars": REBREAK_LOOKAHEAD_BARS,
            "thin_year_sample_lt": MIN_YEAR_SAMPLE,
        },
        "regime_splits": ["trend_down_candidate", "range_candidate", "uptrend_pullback_candidate", "bearish_stack", "mixed_stack", "non_bearish", "bullish_stack"],
    }


def run(args: argparse.Namespace) -> Path:
    out_dir = args.out_root / f"{_now_tag()}-{AXIS_ID.replace('_', '-')}"
    out_dir.mkdir(parents=True, exist_ok=False)
    db_path = Path(args.db_path) if args.db_path else resolve_runtime_stock_db_path()
    runtime_status = get_runtime_stock_db_status()
    rankings_freshness = get_rankings_freshness()
    db_contract = inspect_runtime_stock_db(runtime_db_path=db_path)
    with duckdb.connect(str(db_path), read_only=True) as conn:
        daily = _load_daily_bars(conn, start_ymd=args.start_ymd, end_ymd=args.end_ymd)
        monthly = _load_monthly(conn)
    featured = _add_daily_features(daily)
    weekly = _weekly_context(daily)
    regime_frame = _attach_regime_context(featured, monthly, weekly)
    events = _build_transition_events(regime_frame)
    summary = _summary(events)
    contrast = _contrast(summary)
    yearly = _yearly_stability(events)
    failed = _failed_recovery_summary(events)
    decision = _decision(contrast, yearly)

    previous_event_count = None
    previous_events_path = Path(args.previous_events)
    if previous_events_path.exists():
        previous_event_count = sum(1 for _ in previous_events_path.open(encoding="utf-8")) - 1
    base_event_count = int(events[events["horizon"] == max(HORIZONS)].shape[0])
    event_definition_consistent = previous_event_count == base_event_count if previous_event_count is not None else None

    input_audit = {
        "axis_id": AXIS_ID,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "db_path": str(db_path),
        "db_contract": db_contract,
        "runtime_status": runtime_status,
        "rankings_freshness": rankings_freshness,
        "start_ymd": args.start_ymd,
        "end_ymd": args.end_ymd,
        "confirmed_bars_only": True,
        "runtime_db_write": False,
        "meemee_reflection": False,
        "ranking_change": False,
        "publish": False,
        "daily_rows": int(len(daily)),
        "code_count": int(daily["code"].nunique()),
        "min_ymd": int(daily["ymd"].min()),
        "max_ymd": int(daily["ymd"].max()),
        "transition_event_rows": int(len(events)),
        "base_event_count": base_event_count,
        "previous_axis_event_count": previous_event_count,
        "event_definition_consistent_with_previous_axis": event_definition_consistent,
        "previous_events_path": str(previous_events_path),
        "artifact_contract": {
            "authoritative_json": ["input_audit.json", "transition_definition.json", "transition_contrast_summary.json", "research_decision.json"],
            "derived_csv": [
                "ma_break_transition_events.csv",
                "transition_probability_by_ma_regime_horizon.csv",
                "failed_recovery_summary.csv",
                "yearly_stability_summary.csv",
            ],
        },
    }

    _write_json(out_dir / "input_audit.json", input_audit)
    _write_json(out_dir / "transition_definition.json", _transition_definition())
    events.to_csv(out_dir / "ma_break_transition_events.csv", index=False, encoding="utf-8")
    summary.to_csv(out_dir / "transition_probability_by_ma_regime_horizon.csv", index=False, encoding="utf-8")
    _write_json(out_dir / "transition_contrast_summary.json", contrast)
    failed.to_csv(out_dir / "failed_recovery_summary.csv", index=False, encoding="utf-8")
    yearly.to_csv(out_dir / "yearly_stability_summary.csv", index=False, encoding="utf-8")
    _write_json(out_dir / "research_decision.json", decision)

    missing = [name for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json" and not (out_dir / name).exists()]
    complete = {
        "axis_id": AXIS_ID,
        "status": "complete" if not missing else "incomplete",
        "missing_artifacts": missing,
        "authoritative_result": str(out_dir / "research_decision.json"),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    _write_json(out_dir / "_ARTIFACT_COMPLETE.json", complete)
    if missing:
        raise RuntimeError(f"missing artifacts: {missing}")
    return out_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TRADEX read-only MA break regime transition probability diagnostic.")
    parser.add_argument("--db-path", default="", help="Optional stocks.duckdb path. Defaults to runtime DB contract.")
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--start-ymd", type=int, default=20200101)
    parser.add_argument("--end-ymd", type=int, default=None)
    parser.add_argument("--previous-events", type=Path, default=DEFAULT_PREVIOUS_EVENTS)
    return parser.parse_args()


def main() -> None:
    print(run(parse_args()))


if __name__ == "__main__":
    main()
