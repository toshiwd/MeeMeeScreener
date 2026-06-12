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


AXIS_ID = "ma_break_recovery_by_regime_v1"
PREVIOUS_AXIS_ID = "ma_break_recovery_interaction_v1"
DEFAULT_OUT_ROOT = Path("G:/Tradex/ma_break_recovery_by_regime_v1")
DEFAULT_PREVIOUS_EVENTS = Path(
    "G:/Tradex/ma_break_recovery_interaction_v1/20260603T083923Z-ma-break-recovery-interaction-v1/ma_break_events.csv"
)
MA_WINDOWS = (5, 7, 20, 60, 100, 200)
RECOVERY_HORIZONS = (3, 5, 10, 20, 60)
FORWARD_HORIZONS = (20, 60)
SEVERE_LOSS_THRESHOLD_PCT = -10.0
REBREAK_LOOKAHEAD_BARS = 10
REQUIRED_ARTIFACTS = (
    "input_audit.json",
    "regime_definition.json",
    "ma_break_events_by_regime.csv",
    "ma_recovery_summary_by_regime.json",
    "ma_recovery_by_ma_and_regime.csv",
    "downside_continuation_by_regime.csv",
    "regime_contrast_summary.json",
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
    frame["monthly_close_vs_ma20"] = frame["monthly_close"] / frame["monthly_ma20"] - 1.0
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
        ma_col = f"ma{window}"
        out[ma_col] = grouped["c"].transform(lambda s, w=window: s.rolling(w, min_periods=w).mean())
        out[f"prev_c_ma{window}"] = grouped["c"].shift(1)
        out[f"prev_ma{window}"] = grouped[ma_col].shift(1)
        out[f"below_ma{window}"] = out["c"] < out[ma_col]
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
    for horizon in FORWARD_HORIZONS:
        out[f"ret_{horizon}d_pct"] = (grouped["c"].shift(-horizon) / out["c"] - 1.0) * 100.0
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
    return weekly[
        [
            "code",
            "ymd",
            "weekly_above_ma20",
            "weekly_above_ma60",
            "weekly_ma20_slope_state",
            "weekly_ma60_slope_state",
            "weekly_ma20_slope_4w_pct",
            "weekly_ma60_slope_4w_pct",
        ]
    ].rename(columns={"ymd": "week_ymd"})


def _attach_regime_context(daily: pd.DataFrame, monthly: pd.DataFrame, weekly: pd.DataFrame) -> pd.DataFrame:
    out = daily.copy()
    out["event_dt"] = pd.to_datetime(out["ymd"].astype(str), format="%Y%m%d")
    if not monthly.empty:
        month_cols = [
            "code",
            "month_dt",
            "monthly_close",
            "monthly_ma20",
            "monthly_above_ma20",
            "monthly_ma20_slope_state",
            "monthly_ma20_slope_3m_pct",
        ]
        parts = []
        for code, rows in out.groupby("code", sort=False):
            m = monthly[monthly["code"] == code][month_cols].dropna(subset=["month_dt"]).sort_values("month_dt")
            if m.empty:
                chunk = rows.copy()
                for col in month_cols[2:]:
                    chunk[col] = None
                parts.append(chunk)
                continue
            parts.append(pd.merge_asof(rows.sort_values("event_dt"), m, left_on="event_dt", right_on="month_dt", by="code", direction="backward"))
        out = pd.concat(parts, ignore_index=True)
    else:
        out["monthly_above_ma20"] = None
        out["monthly_ma20_slope_state"] = None
        out["monthly_ma20_slope_3m_pct"] = None

    parts = []
    for code, rows in out.groupby("code", sort=False):
        w = weekly[weekly["code"] == code].copy().sort_values("week_ymd")
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
    out["monthly_regime"] = (
        "monthly_"
        + out["monthly_above_ma20"].map({True: "above_ma20", False: "below_ma20"}).fillna("unknown")
        + "_slope_"
        + out["monthly_ma20_slope_state"].fillna("unknown").astype(str)
    )
    out["weekly_regime"] = (
        "weekly_"
        + out["weekly_above_ma20"].map({True: "above_ma20", False: "below_ma20"}).fillna("unknown")
        + "_ma60_"
        + out["weekly_above_ma60"].map({True: "above", False: "below"}).fillna("unknown")
        + "_s20_"
        + out["weekly_ma20_slope_state"].fillna("unknown").astype(str)
        + "_s60_"
        + out["weekly_ma60_slope_state"].fillna("unknown").astype(str)
    )
    out["daily_regime"] = out["ma_alignment"] + "_s20_" + out["ma20_slope_state"].fillna("unknown").astype(str) + "_s60_" + out["ma60_slope_state"].fillna("unknown").astype(str)
    return out.sort_values(["code", "ymd"], kind="stable").reset_index(drop=True)


def _build_events(frame: pd.DataFrame) -> pd.DataFrame:
    event_rows: list[dict[str, Any]] = []
    max_horizon = max(max(RECOVERY_HORIZONS), max(FORWARD_HORIZONS))
    for code, rows in frame.groupby("code", sort=False):
        rows = rows.reset_index(drop=True)
        closes = rows["c"].to_numpy()
        lows = rows["l"].to_numpy()
        ma_values = {window: rows[f"ma{window}"].to_numpy() for window in MA_WINDOWS}
        break_values = {window: rows[f"break_ma{window}"].fillna(False).astype(bool).to_numpy() for window in MA_WINDOWS}
        ymd_values = rows["ymd"].to_numpy()
        for idx in range(len(rows)):
            for window in MA_WINDOWS:
                ma_value = ma_values[window][idx]
                if not bool(break_values[window][idx]) or pd.isna(ma_value):
                    continue
                end = min(len(rows), idx + max_horizon + 1)
                recovery_bars = None
                rebreak_after_recovery = None
                max_drawdown_after_break = None
                if idx + 1 < end:
                    future_close = closes[idx + 1 : end]
                    future_ma = ma_values[window][idx + 1 : end]
                    recovered_positions = (future_close >= future_ma).nonzero()[0]
                    if len(recovered_positions):
                        recovery_bars = int(recovered_positions[0] + 1)
                        rebreak_start = idx + recovery_bars + 1
                        rebreak_end = min(len(rows), rebreak_start + REBREAK_LOOKAHEAD_BARS)
                        if rebreak_start < rebreak_end:
                            rebreak_after_recovery = bool((closes[rebreak_start:rebreak_end] < ma_values[window][rebreak_start:rebreak_end]).any())
                    low_end = min(len(rows), idx + 61)
                    if idx + 1 < low_end:
                        max_drawdown_after_break = float((lows[idx + 1 : low_end].min() / closes[idx] - 1.0) * 100.0)
                row = rows.iloc[idx]
                event: dict[str, Any] = {
                    "code": code,
                    "event_date": int(ymd_values[idx]),
                    "target_ma": f"ma{window}",
                    "target_window": window,
                    "close": float(closes[idx]),
                    "target_ma_value": float(ma_value),
                    "target_ma_distance_pct": float((closes[idx] / ma_value - 1.0) * 100.0),
                    "recovery_bars": recovery_bars,
                    "rebreak_after_recovery_10d": rebreak_after_recovery,
                    "max_drawdown_after_break_pct": max_drawdown_after_break,
                    "ret20": None if idx + 20 >= len(rows) else float((closes[idx + 20] / closes[idx] - 1.0) * 100.0),
                    "ret60": None if idx + 60 >= len(rows) else float((closes[idx + 60] / closes[idx] - 1.0) * 100.0),
                    "severe_loss_20d": None if idx + 20 >= len(rows) else bool((closes[idx + 20] / closes[idx] - 1.0) * 100.0 <= SEVERE_LOSS_THRESHOLD_PCT),
                    "range_vs_trend": str(row["range_vs_trend"]),
                    "monthly_regime": str(row["monthly_regime"]),
                    "weekly_regime": str(row["weekly_regime"]),
                    "daily_regime": str(row["daily_regime"]),
                    "ma_alignment": str(row["ma_alignment"]),
                    "ma200_stack_bucket": str(row["ma200_stack_bucket"]),
                    "monthly_above_ma20": bool(row["monthly_above_ma20"]) if pd.notna(row["monthly_above_ma20"]) else None,
                    "monthly_ma20_slope_state": None if pd.isna(row["monthly_ma20_slope_state"]) else str(row["monthly_ma20_slope_state"]),
                    "weekly_above_ma20": bool(row["weekly_above_ma20"]) if pd.notna(row["weekly_above_ma20"]) else None,
                    "weekly_above_ma60": bool(row["weekly_above_ma60"]) if pd.notna(row["weekly_above_ma60"]) else None,
                    "weekly_ma20_slope_state": None if pd.isna(row["weekly_ma20_slope_state"]) else str(row["weekly_ma20_slope_state"]),
                    "weekly_ma60_slope_state": None if pd.isna(row["weekly_ma60_slope_state"]) else str(row["weekly_ma60_slope_state"]),
                    "ma20_slope_state": None if pd.isna(row["ma20_slope_state"]) else str(row["ma20_slope_state"]),
                    "ma60_slope_state": None if pd.isna(row["ma60_slope_state"]) else str(row["ma60_slope_state"]),
                    "ma100_slope_state": None if pd.isna(row["ma100_slope_state"]) else str(row["ma100_slope_state"]),
                }
                for other in MA_WINDOWS:
                    event[f"daily_below_ma{other}"] = bool(row[f"below_ma{other}"]) if pd.notna(row[f"below_ma{other}"]) else None
                for horizon in RECOVERY_HORIZONS:
                    event[f"recovered_within_{horizon}d"] = recovery_bars is not None and recovery_bars <= horizon
                event_rows.append(event)
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


def _summary_row(group: pd.DataFrame, *, target_ma: str, regime_axis: str, regime_value: str) -> dict[str, Any]:
    row: dict[str, Any] = {
        "target_ma": target_ma,
        "regime_axis": regime_axis,
        "regime_value": regime_value,
        "event_count": int(len(group)),
        "unique_symbol_count": int(group["code"].nunique()),
        "unrecovered_20d_rate": 1.0 - float(group["recovered_within_20d"].mean()),
        "unrecovered_60d_rate": 1.0 - float(group["recovered_within_60d"].mean()),
        "median_bars_to_recovery": _median(group["recovery_bars"]),
        "mean_ret20": _mean(group["ret20"]),
        "median_ret20": _median(group["ret20"]),
        "mean_ret60": _mean(group["ret60"]),
        "median_ret60": _median(group["ret60"]),
        "severe_loss_rate_20d": _rate(group["severe_loss_20d"]),
        "max_drawdown_after_break_mean": _mean(group["max_drawdown_after_break_pct"]),
        "max_drawdown_after_break_median": _median(group["max_drawdown_after_break_pct"]),
        "rebreak_after_recovery_10d_rate": _rate(group["rebreak_after_recovery_10d"]),
    }
    for horizon in RECOVERY_HORIZONS:
        row[f"recovery_rate_{horizon}d"] = _rate(group[f"recovered_within_{horizon}d"])
    return row


def _summary_by_regime(events: pd.DataFrame) -> pd.DataFrame:
    axes = ("range_vs_trend", "monthly_regime", "weekly_regime", "daily_regime", "ma_alignment", "ma200_stack_bucket")
    rows: list[dict[str, Any]] = []
    for target_ma, ma_group in events.groupby("target_ma", sort=False):
        rows.append(_summary_row(ma_group, target_ma=target_ma, regime_axis="all", regime_value="all"))
        for axis in axes:
            for value, group in ma_group.groupby(axis, dropna=False):
                rows.append(_summary_row(group, target_ma=target_ma, regime_axis=axis, regime_value=str(value)))
    return pd.DataFrame(rows).sort_values(["target_ma", "regime_axis", "regime_value"], kind="stable")


def _contrast(summary: pd.DataFrame) -> dict[str, Any]:
    comparisons = [
        ("ma60", "range_vs_trend", ["trend_down_candidate", "range_candidate", "uptrend_pullback_candidate"]),
        ("ma100", "range_vs_trend", ["trend_down_candidate", "range_candidate", "uptrend_pullback_candidate"]),
        ("ma200", "ma200_stack_bucket", ["bearish_stack", "mixed_stack", "non_bearish"]),
        ("ma20", "range_vs_trend", ["uptrend_pullback_candidate", "trend_down_candidate"]),
    ]
    out: list[dict[str, Any]] = []
    for target_ma, axis, values in comparisons:
        subset = summary[(summary["target_ma"] == target_ma) & (summary["regime_axis"] == axis)]
        rows = []
        for value in values:
            row = subset[subset["regime_value"] == value]
            if row.empty:
                rows.append({"regime_value": value, "missing": True})
            else:
                data = row.iloc[0].to_dict()
                rows.append({k: data.get(k) for k in ["regime_value", "event_count", "unique_symbol_count", "recovery_rate_20d", "unrecovered_20d_rate", "severe_loss_rate_20d", "mean_ret20", "max_drawdown_after_break_mean"]})
        deltas: dict[str, Any] = {}
        if len(rows) >= 2 and not rows[0].get("missing") and not rows[1].get("missing"):
            deltas["first_minus_second_recovery_rate_20d"] = rows[0]["recovery_rate_20d"] - rows[1]["recovery_rate_20d"]
            deltas["first_minus_second_unrecovered_20d_rate"] = rows[0]["unrecovered_20d_rate"] - rows[1]["unrecovered_20d_rate"]
            deltas["first_minus_second_severe_loss_rate_20d"] = rows[0]["severe_loss_rate_20d"] - rows[1]["severe_loss_rate_20d"]
        out.append({"target_ma": target_ma, "regime_axis": axis, "ordered_values": values, "rows": rows, "deltas": deltas})
    return {"axis_id": AXIS_ID, "required_comparisons": out}


def _decision(contrast: dict[str, Any], events: pd.DataFrame) -> dict[str, Any]:
    reasons: list[dict[str, Any]] = []
    for comp in contrast["required_comparisons"]:
        rows = {row.get("regime_value"): row for row in comp["rows"] if not row.get("missing")}
        if comp["target_ma"] in {"ma60", "ma100"}:
            trend = rows.get("trend_down_candidate")
            range_row = rows.get("range_candidate")
            if trend and range_row and trend["event_count"] >= 200 and range_row["event_count"] >= 200:
                recovery_delta = trend["recovery_rate_20d"] - range_row["recovery_rate_20d"]
                severe_delta = trend["severe_loss_rate_20d"] - range_row["severe_loss_rate_20d"]
                if recovery_delta <= -0.05 or severe_delta >= 0.02:
                    reasons.append(
                        {
                            "target_ma": comp["target_ma"],
                            "typed_reason": "trend_down_candidate_materially_worse_than_range_candidate",
                            "recovery_rate_20d_delta": recovery_delta,
                            "severe_loss_rate_20d_delta": severe_delta,
                            "trend_event_count": int(trend["event_count"]),
                            "range_event_count": int(range_row["event_count"]),
                        }
                    )
    if len(reasons) >= 2:
        decision = "keep_for_next"
        reason = "regime_split_materially_changes_ma60_ma100_recovery_risk"
    elif reasons:
        decision = "hold"
        reason = "one_required_regime_contrast_is_material_but_needs_stability_check"
    else:
        decision = "drop"
        reason = "required_regime_contrasts_not_material_under_current_definitions"
    return {
        "axis_id": AXIS_ID,
        "candidate_local_decision": decision,
        "session_aggregate_decision": decision,
        "authoritative_rollup_decision": decision,
        "reason": reason,
        "event_count": int(len(events)),
        "material_contrast_reasons": reasons,
        "non_scope": [
            "no MeeMee reflection",
            "no runtime DuckDB writes",
            "no ranking changes",
            "no publish",
            "no candidate generation changes",
            "no trade rule promotion",
            "no volume condition added",
            "no symbol-specific correction",
            "no bad-pick removal implementation",
        ],
    }


def _regime_definition() -> dict[str, Any]:
    return {
        "axis_id": AXIS_ID,
        "monthly_regime": {
            "monthly_above_ma20": "monthly close >= monthly MA20 from monthly_bars joined to monthly_ma",
            "monthly_ma20_slope_state": "3-month monthly MA20 pct change: down < -1%, flat -1%..1%, up > 1%",
            "monthly_box_position": "unavailable_not_silently_fallbacked: no runtime DuckDB box fields found in monthly_bars/monthly_ma",
        },
        "weekly_regime": {
            "source": "weekly close resampled from confirmed daily_bars using W-FRI",
            "weekly_above_ma20_ma60": "weekly close >= weekly rolling MA20/MA60",
            "weekly_slope_state": "4-week MA pct change: down < -0.5%, flat -0.5%..0.5%, up > 0.5%",
        },
        "daily_regime": {
            "ma_windows": list(MA_WINDOWS),
            "ma_alignment": {
                "bullish_stack": "MA20 > MA60 > MA100 > MA200",
                "bearish_stack": "MA20 < MA60 < MA100 < MA200",
                "mixed_stack": "otherwise",
            },
            "slope_state": "20-bar MA pct change: down < -0.5%, flat -0.5%..0.5%, up > 0.5%",
        },
        "range_vs_trend": {
            "trend_down_candidate": "close < MA60 and MA60 slope down, or close < MA100 and MA100 slope down, or MA20 < MA60 and MA60 slope down",
            "range_candidate": "abs(close vs MA60) <= 5%, abs(close vs MA100) <= 7.5%, MA60 slope abs <= 1%, MA100 slope abs <= 1%",
            "uptrend_pullback_candidate": "monthly or weekly regime up, daily close temporarily below MA20/60, and MA60 or MA100 slope not down",
            "precedence": "trend_down overrides uptrend_pullback, uptrend_pullback overrides range, otherwise other",
        },
        "event_definition_contract": {
            "break": "previous close >= target MA and current close < target MA",
            "recovery": "future close >= same target MA",
            "same_as_previous_axis": PREVIOUS_AXIS_ID,
        },
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
    events = _build_events(regime_frame)
    if events.empty:
        raise RuntimeError("no MA break events found")
    summary = _summary_by_regime(events)
    contrast = _contrast(summary)
    decision = _decision(contrast, events)
    downside = summary[summary["regime_axis"].isin(["range_vs_trend", "ma_alignment", "ma200_stack_bucket"])].copy()

    previous_event_count = None
    previous_events_path = Path(args.previous_events)
    if previous_events_path.exists():
        previous_event_count = sum(1 for _ in previous_events_path.open(encoding="utf-8")) - 1
    event_definition_consistent = previous_event_count == int(len(events)) if previous_event_count is not None else None

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
        "event_count": int(len(events)),
        "previous_axis_event_count": previous_event_count,
        "event_definition_consistent_with_previous_axis": event_definition_consistent,
        "previous_events_path": str(previous_events_path),
        "artifact_contract": {
            "authoritative_json": ["ma_recovery_summary_by_regime.json", "regime_contrast_summary.json", "research_decision.json", "input_audit.json", "regime_definition.json"],
            "derived_csv": ["ma_break_events_by_regime.csv", "ma_recovery_by_ma_and_regime.csv", "downside_continuation_by_regime.csv"],
        },
    }

    _write_json(out_dir / "input_audit.json", input_audit)
    _write_json(out_dir / "regime_definition.json", _regime_definition())
    events.to_csv(out_dir / "ma_break_events_by_regime.csv", index=False, encoding="utf-8")
    _write_json(out_dir / "ma_recovery_summary_by_regime.json", {"axis_id": AXIS_ID, "rows": summary.to_dict(orient="records")})
    summary.to_csv(out_dir / "ma_recovery_by_ma_and_regime.csv", index=False, encoding="utf-8")
    downside.to_csv(out_dir / "downside_continuation_by_regime.csv", index=False, encoding="utf-8")
    _write_json(out_dir / "regime_contrast_summary.json", contrast)
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
    parser = argparse.ArgumentParser(description="TRADEX read-only MA break recovery by regime diagnostic.")
    parser.add_argument("--db-path", default="", help="Optional stocks.duckdb path. Defaults to runtime DB contract.")
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--start-ymd", type=int, default=20200101)
    parser.add_argument("--end-ymd", type=int, default=None)
    parser.add_argument("--previous-events", type=Path, default=DEFAULT_PREVIOUS_EVENTS)
    return parser.parse_args()


def main() -> None:
    out_dir = run(parse_args())
    print(out_dir)


if __name__ == "__main__":
    main()
