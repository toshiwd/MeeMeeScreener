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


AXIS_ID = "ma_above_run_maturity_break_v1"
PREVIOUS_AXIS_ID = "ma_break_regime_transition_probability_v1"
DEFAULT_OUT_ROOT = Path("G:/Tradex/ma_above_run_maturity_break_v1")
DEFAULT_PREVIOUS_EVENTS = Path(
    "G:/Tradex/ma_break_regime_transition_probability_v1/20260603T093838Z-ma-break-regime-transition-probability-v1/ma_break_transition_events.csv"
)
MA_WINDOWS = (5, 7, 20, 60, 100, 200)
RUN_BUCKETS = (-1, 9, 19, 49, 79, 99, 149, 10_000_000)
RUN_BUCKET_LABELS = ("0-9", "10-19", "20-49", "50-79", "80-99", "100-149", "150+")
SEVERE_LOSS_THRESHOLD_PCT = -10.0
REBREAK_LOOKAHEAD_BARS = 10
MIN_CONTRAST_SAMPLE = 200
MIN_YEAR_SAMPLE = 50
REQUIRED_ARTIFACTS = (
    "input_audit.json",
    "maturity_definition.json",
    "ma_break_events_with_above_run.csv",
    "ma_break_outcome_by_run_bucket.csv",
    "maturity_contrast_summary.json",
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
    frame = conn.execute(
        f"""
        WITH normalized AS (
          SELECT
            CAST(code AS VARCHAR) AS code,
            {_date_expr("date")} AS ymd,
            CAST(o AS DOUBLE) AS o,
            CAST(h AS DOUBLE) AS h,
            CAST(l AS DOUBLE) AS l,
            CAST(c AS DOUBLE) AS c,
            lower(coalesce(source, '')) AS source
          FROM daily_bars
          WHERE o > 0 AND h > 0 AND l > 0 AND c > 0
            AND lower(coalesce(source, '')) IN ('pan', 'txt', 'confirmed')
        )
        SELECT code, ymd, o, h, l, c, source
        FROM normalized
        WHERE ymd >= ? {end_clause}
        ORDER BY code, ymd
        """,
        params,
    ).fetchdf()
    if frame.empty:
        raise RuntimeError("daily_bars query returned no rows")
    frame["code"] = frame["code"].astype(str)
    frame["ymd"] = pd.to_numeric(frame["ymd"], errors="coerce").astype("Int64")
    frame = frame.dropna(subset=["ymd", "c"]).copy()
    frame["ymd"] = frame["ymd"].astype(int)
    return frame.sort_values(["code", "ymd"], kind="stable").reset_index(drop=True)


def _load_monthly(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    frame = conn.execute(
        f"""
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
    ).fetchdf()
    if frame.empty:
        return frame
    frame["month_dt"] = pd.to_datetime(frame["month_ymd"].astype(str), format="%Y%m%d", errors="coerce")
    frame["monthly_above_ma20"] = frame["monthly_close"] >= frame["monthly_ma20"]
    frame["monthly_ma20_slope_3m_pct"] = frame.groupby("code")["monthly_ma20"].transform(lambda s: (s / s.shift(3) - 1.0) * 100.0)
    frame["monthly_ma20_slope_state"] = pd.cut(
        frame["monthly_ma20_slope_3m_pct"],
        [-float("inf"), -1.0, 1.0, float("inf")],
        labels=["down", "flat", "up"],
    ).astype("object")
    return frame


def _streak_true(cond: pd.Series) -> pd.Series:
    values = cond.fillna(False).astype(bool)
    groups = values.ne(values.shift()).cumsum()
    return values.groupby(groups).cumcount().add(1).where(values, 0)


def _add_daily_features(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    grouped = out.groupby("code", group_keys=False)
    for window in MA_WINDOWS:
        out[f"ma{window}"] = grouped["c"].transform(lambda s, w=window: s.rolling(w, min_periods=w).mean())
        out[f"prev_c_ma{window}"] = grouped["c"].shift(1)
        out[f"prev_ma{window}"] = grouped[f"ma{window}"].shift(1)
        out[f"above_ma{window}"] = out["c"] >= out[f"ma{window}"]
        out[f"below_ma{window}"] = out["c"] < out[f"ma{window}"]
        out[f"break_ma{window}"] = (out[f"prev_c_ma{window}"] >= out[f"prev_ma{window}"]) & out[f"below_ma{window}"]
        out[f"ma{window}_above_run"] = grouped[f"above_ma{window}"].transform(_streak_true)
        out[f"ma{window}_above_run_before_break"] = grouped[f"ma{window}_above_run"].shift(1).fillna(0).astype(int)
    for window in (20, 60, 100):
        out[f"ma{window}_slope_20d_pct"] = grouped[f"ma{window}"].transform(lambda s: (s / s.shift(20) - 1.0) * 100.0)
        out[f"ma{window}_slope_state"] = pd.cut(
            out[f"ma{window}_slope_20d_pct"],
            [-float("inf"), -0.5, 0.5, float("inf")],
            labels=["down", "flat", "up"],
        ).astype("object")
    out["ma_alignment"] = "mixed_stack"
    out.loc[(out["ma20"] > out["ma60"]) & (out["ma60"] > out["ma100"]) & (out["ma100"] > out["ma200"]), "ma_alignment"] = "bullish_stack"
    out.loc[(out["ma20"] < out["ma60"]) & (out["ma60"] < out["ma100"]) & (out["ma100"] < out["ma200"]), "ma_alignment"] = "bearish_stack"
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
    weekly["weekly_ma20_slope_4w_pct"] = grouped["weekly_ma20"].transform(lambda s: (s / s.shift(4) - 1.0) * 100.0)
    weekly["weekly_above_ma20"] = weekly["c"] >= weekly["weekly_ma20"]
    weekly["weekly_ma20_slope_state"] = pd.cut(
        weekly["weekly_ma20_slope_4w_pct"],
        [-float("inf"), -0.5, 0.5, float("inf")],
        labels=["down", "flat", "up"],
    ).astype("object")
    return weekly.rename(columns={"ymd": "week_ymd"})


def _attach_regime_context(daily: pd.DataFrame, monthly: pd.DataFrame, weekly: pd.DataFrame) -> pd.DataFrame:
    out = daily.copy()
    out["event_dt"] = pd.to_datetime(out["ymd"].astype(str), format="%Y%m%d")
    if not monthly.empty:
        parts = []
        month_cols = ["code", "month_dt", "monthly_above_ma20", "monthly_ma20_slope_state"]
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
    weekly_cols = ["code", "week_ymd", "weekly_above_ma20", "weekly_ma20_slope_state"]
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


def _run_bucket(value: int) -> str:
    for lo, hi, label in zip(RUN_BUCKETS[:-1], RUN_BUCKETS[1:], RUN_BUCKET_LABELS):
        if int(value) > lo and int(value) <= hi:
            return label
    return "unknown"


def _build_events(frame: pd.DataFrame) -> pd.DataFrame:
    rows_out: list[dict[str, Any]] = []
    for code, rows in frame.groupby("code", sort=False):
        rows = rows.reset_index(drop=True)
        closes = rows["c"].to_numpy()
        lows = rows["l"].to_numpy()
        ymds = rows["ymd"].to_numpy()
        ma_values = {window: rows[f"ma{window}"].to_numpy() for window in MA_WINDOWS}
        break_values = {window: rows[f"break_ma{window}"].fillna(False).astype(bool).to_numpy() for window in MA_WINDOWS}
        run_values = {window: rows[f"ma{window}_above_run_before_break"].fillna(0).astype(int).to_numpy() for window in MA_WINDOWS}
        for idx in range(len(rows)):
            base = rows.iloc[idx]
            for window in MA_WINDOWS:
                if not bool(break_values[window][idx]) or pd.isna(ma_values[window][idx]):
                    continue
                if idx + 20 >= len(rows):
                    ret20 = None
                    max_dd = None
                    still_below = None
                    severe = None
                else:
                    ret20 = float(closes[idx + 20] / closes[idx] - 1.0) * 100.0
                    max_dd = float(lows[idx + 1 : idx + 21].min() / closes[idx] - 1.0) * 100.0
                    still_below = bool(closes[idx + 20] < ma_values[window][idx + 20])
                    severe = ret20 <= SEVERE_LOSS_THRESHOLD_PCT
                recovery_bars = None
                rebreak_after_recovery = None
                end = min(len(rows), idx + 61)
                if idx + 1 < end:
                    recovered_positions = (closes[idx + 1 : end] >= ma_values[window][idx + 1 : end]).nonzero()[0]
                    if len(recovered_positions):
                        recovery_bars = int(recovered_positions[0] + 1)
                recovered_20d = recovery_bars is not None and recovery_bars <= 20
                failed_recovery = False
                if recovered_20d:
                    rebreak_start = idx + int(recovery_bars) + 1
                    rebreak_end = min(idx + 21, rebreak_start + REBREAK_LOOKAHEAD_BARS)
                    if rebreak_start < rebreak_end:
                        rebreak_after_recovery = bool((closes[rebreak_start:rebreak_end] < ma_values[window][rebreak_start:rebreak_end]).any())
                    failed_recovery = bool(rebreak_after_recovery) or (ret20 is not None and ret20 < 0) or (max_dd is not None and max_dd <= SEVERE_LOSS_THRESHOLD_PCT)
                target_run = int(run_values[window][idx])
                rec: dict[str, Any] = {
                    "code": code,
                    "event_date": int(ymds[idx]),
                    "event_year": int(str(int(ymds[idx]))[:4]),
                    "target_ma": f"ma{window}",
                    "target_window": int(window),
                    "close": float(closes[idx]),
                    "target_ma_value": float(ma_values[window][idx]),
                    "target_above_run_before_break": target_run,
                    "target_run_bucket": _run_bucket(target_run),
                    "range_vs_trend": str(base["range_vs_trend"]),
                    "ma_alignment": str(base["ma_alignment"]),
                    "regime_bucket": str(base["range_vs_trend"]),
                    "recovery_rate_20d_flag": recovered_20d,
                    "unrecovered_20d_flag": not recovered_20d,
                    "failed_recovery_20d": failed_recovery,
                    "rebreak_after_recovery": rebreak_after_recovery,
                    "ret20": ret20,
                    "severe_loss_20d": severe,
                    "max_drawdown_20d": max_dd,
                    "still_below_broken_ma_20d": still_below,
                }
                for other in MA_WINDOWS:
                    rec[f"ma{other}_above_run_before_break"] = int(run_values[other][idx])
                rows_out.append(rec)
    return pd.DataFrame(rows_out)


def _rate(series: pd.Series) -> float | None:
    valid = series.dropna()
    if valid.empty:
        return None
    return float(valid.astype(bool).mean())


def _mean(series: pd.Series) -> float | None:
    valid = pd.to_numeric(series, errors="coerce").dropna()
    return None if valid.empty else float(valid.mean())


def _median(series: pd.Series) -> float | None:
    valid = pd.to_numeric(series, errors="coerce").dropna()
    return None if valid.empty else float(valid.median())


def _summary_row(group: pd.DataFrame, *, target_ma: str, run_bucket: str, regime_axis: str, regime_value: str) -> dict[str, Any]:
    return {
        "target_ma": target_ma,
        "run_bucket": run_bucket,
        "regime_axis": regime_axis,
        "regime_value": regime_value,
        "event_count": int(len(group)),
        "unique_symbol_count": int(group["code"].nunique()),
        "recovery_rate_20d": _rate(group["recovery_rate_20d_flag"]),
        "unrecovered_20d_rate": _rate(group["unrecovered_20d_flag"]),
        "failed_recovery_rate_20d": _rate(group["failed_recovery_20d"]),
        "rebreak_rate_after_recovery": _rate(group["rebreak_after_recovery"]),
        "mean_ret20": _mean(group["ret20"]),
        "median_ret20": _median(group["ret20"]),
        "severe_loss_rate_20d": _rate(group["severe_loss_20d"]),
        "mean_max_drawdown_20d": _mean(group["max_drawdown_20d"]),
        "median_max_drawdown_20d": _median(group["max_drawdown_20d"]),
        "still_below_broken_ma_rate_20d": _rate(group["still_below_broken_ma_20d"]),
    }


def _summary(events: pd.DataFrame) -> pd.DataFrame:
    rows = []
    axes = [("all", "all"), ("range_vs_trend", None), ("ma_alignment", None)]
    for (target_ma, run_bucket), group in events.groupby(["target_ma", "target_run_bucket"], sort=False):
        rows.append(_summary_row(group, target_ma=str(target_ma), run_bucket=str(run_bucket), regime_axis="all", regime_value="all"))
        for value, sub in group.groupby("range_vs_trend", dropna=False):
            rows.append(_summary_row(sub, target_ma=str(target_ma), run_bucket=str(run_bucket), regime_axis="range_vs_trend", regime_value=str(value)))
        for value, sub in group.groupby("ma_alignment", dropna=False):
            rows.append(_summary_row(sub, target_ma=str(target_ma), run_bucket=str(run_bucket), regime_axis="ma_alignment", regime_value=str(value)))
    return pd.DataFrame(rows).sort_values(["target_ma", "regime_axis", "regime_value", "run_bucket"], kind="stable")


def _aggregate_condition(events: pd.DataFrame, *, target_ma: str, condition_name: str, mask: pd.Series) -> dict[str, Any]:
    group = events[(events["target_ma"] == target_ma) & mask].copy()
    row = _summary_row(group, target_ma=target_ma, run_bucket=condition_name, regime_axis="contrast_condition", regime_value=condition_name) if not group.empty else {
        "target_ma": target_ma,
        "run_bucket": condition_name,
        "regime_axis": "contrast_condition",
        "regime_value": condition_name,
        "event_count": 0,
        "unique_symbol_count": 0,
    }
    return row


def _contrast(events: pd.DataFrame) -> dict[str, Any]:
    specs = [
        ("ma60", "run_ge_50", lambda s: s >= 50, "run_lt_20", lambda s: s < 20),
        ("ma60", "run_ge_60", lambda s: s >= 60, "run_lt_20", lambda s: s < 20),
        ("ma100", "run_ge_80", lambda s: s >= 80, "run_lt_30", lambda s: s < 30),
        ("ma100", "run_ge_100", lambda s: s >= 100, "run_lt_30", lambda s: s < 30),
        ("ma200", "run_ge_120", lambda s: s >= 120, "run_lt_50", lambda s: s < 50),
    ]
    comparisons = []
    for target_ma, first_name, first_fn, second_name, second_fn in specs:
        target = events[events["target_ma"] == target_ma].copy()
        run = pd.to_numeric(target["target_above_run_before_break"], errors="coerce").fillna(0)
        first = _aggregate_condition(target, target_ma=target_ma, condition_name=first_name, mask=first_fn(run))
        second = _aggregate_condition(target, target_ma=target_ma, condition_name=second_name, mask=second_fn(run))
        status = "ready" if first.get("event_count", 0) >= MIN_CONTRAST_SAMPLE and second.get("event_count", 0) >= MIN_CONTRAST_SAMPLE else "insufficient_sample"
        deltas = {}
        for metric in ["severe_loss_rate_20d", "mean_max_drawdown_20d", "failed_recovery_rate_20d", "mean_ret20"]:
            if first.get(metric) is not None and second.get(metric) is not None:
                deltas[f"{metric}_delta"] = first[metric] - second[metric]
        comparisons.append({"target_ma": target_ma, "first": first_name, "second": second_name, "status": status, "first_metrics": first, "second_metrics": second, "deltas": deltas})
    return {"axis_id": AXIS_ID, "required_contrasts": comparisons}


def _yearly(events: pd.DataFrame) -> pd.DataFrame:
    rows = []
    conditions = [
        ("ma60", "run_ge_50", lambda s: s >= 50),
        ("ma60", "run_lt_20", lambda s: s < 20),
        ("ma100", "run_ge_80", lambda s: s >= 80),
        ("ma100", "run_lt_30", lambda s: s < 30),
        ("ma200", "run_ge_120", lambda s: s >= 120),
        ("ma200", "run_lt_50", lambda s: s < 50),
    ]
    for target_ma, condition, fn in conditions:
        target = events[events["target_ma"] == target_ma].copy()
        run = pd.to_numeric(target["target_above_run_before_break"], errors="coerce").fillna(0)
        subset = target[fn(run)]
        for year, group in subset.groupby("event_year", sort=True):
            row = _summary_row(group, target_ma=target_ma, run_bucket=condition, regime_axis="yearly_condition", regime_value=condition)
            row["event_year"] = int(year)
            row["sample_status"] = "sufficient" if row["event_count"] >= MIN_YEAR_SAMPLE else "insufficient_sample"
            rows.append(row)
    return pd.DataFrame(rows).sort_values(["target_ma", "run_bucket", "event_year"], kind="stable") if rows else pd.DataFrame()


def _decision(contrast: dict[str, Any], yearly: pd.DataFrame) -> dict[str, Any]:
    reasons = []
    guard_reasons = []
    for comp in contrast["required_contrasts"]:
        if comp["status"] != "ready":
            continue
        d = comp["deltas"]
        risk_worse = (
            d.get("severe_loss_rate_20d_delta", 0) >= 0.01
            or d.get("failed_recovery_rate_20d_delta", 0) >= 0.03
            or d.get("mean_max_drawdown_20d_delta", 0) <= -0.5
        )
        if not risk_worse:
            continue
        target_ma = comp["target_ma"]
        first = comp["first"]
        second = comp["second"]
        yf = yearly[(yearly["target_ma"] == target_ma) & (yearly["run_bucket"] == first) & (yearly["sample_status"] == "sufficient")]
        ys = yearly[(yearly["target_ma"] == target_ma) & (yearly["run_bucket"] == second) & (yearly["sample_status"] == "sufficient")]
        support_years = 0
        comparable_years = 0
        for year in sorted(set(yf["event_year"].astype(int)).intersection(set(ys["event_year"].astype(int)))):
            a = yf[yf["event_year"] == year].iloc[0]
            b = ys[ys["event_year"] == year].iloc[0]
            comparable_years += 1
            if (
                (a.get("severe_loss_rate_20d") or 0) >= (b.get("severe_loss_rate_20d") or 0)
                or (a.get("failed_recovery_rate_20d") or 0) >= (b.get("failed_recovery_rate_20d") or 0)
                or (a.get("mean_max_drawdown_20d") or 0) <= (b.get("mean_max_drawdown_20d") or 0)
            ):
                support_years += 1
        payload = {
            "target_ma": target_ma,
            "first": first,
            "second": second,
            "typed_reason": "mature_run_break_has_worse_risk_than_short_run_break",
            "deltas": d,
            "first_event_count": comp["first_metrics"].get("event_count"),
            "second_event_count": comp["second_metrics"].get("event_count"),
            "comparable_years": comparable_years,
            "support_years": support_years,
        }
        if target_ma in {"ma60", "ma100"} and comparable_years >= 3 and support_years >= max(2, comparable_years - 1):
            reasons.append(payload)
        else:
            guard_reasons.append(payload)
    if reasons:
        decision = "keep_for_bad_pick_pretest_next"
        reason = "mature_run_ma60_ma100_breaks_add_risk_separation_with_yearly_support"
    elif guard_reasons:
        decision = "keep_as_trend_exhaustion_guard"
        reason = "mature_run_breaks_worsen_risk_but_not_enough_for_bad_pick_pretest"
    else:
        decision = "drop"
        reason = "run_length_does_not_add_stable_separation"
    return {
        "axis_id": AXIS_ID,
        "candidate_local_decision": decision,
        "session_aggregate_decision": decision,
        "authoritative_rollup_decision": decision,
        "reason": reason,
        "bad_pick_pretest_reasons": reasons,
        "trend_exhaustion_guard_reasons": guard_reasons,
        "non_scope": [
            "no MeeMee reflection",
            "no runtime DuckDB writes",
            "no ranking change",
            "no publish",
            "no candidate generation change",
            "no buy/sell rule promotion",
            "no bad-pick removal implementation",
            "no volume condition",
            "no stock-specific correction",
        ],
    }


def _definition() -> dict[str, Any]:
    return {
        "axis_id": AXIS_ID,
        "base_event": {
            "same_as_previous_axis": PREVIOUS_AXIS_ID,
            "definition": "previous close >= target MA and current close < target MA",
            "confirmed_bars_only": True,
            "ma_windows": list(MA_WINDOWS),
        },
        "above_run_definition": "consecutive prior bars where close >= target MA, measured as of the bar before the MA break",
        "run_buckets": list(RUN_BUCKET_LABELS),
        "main_thresholds": {
            "ma60": [30, 50, 60],
            "ma100": [50, 80, 100],
            "ma200": [80, 120, 200],
        },
        "regime_splits": ["all", "trend_down_candidate", "range_candidate", "uptrend_pullback_candidate", "bullish_stack", "bearish_stack", "mixed_stack"],
        "outcome_horizon": "20 bars after break",
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
    weekly = _weekly_context(daily)
    featured = _add_daily_features(daily)
    regime_frame = _attach_regime_context(featured, monthly, weekly)
    events = _build_events(regime_frame)
    if events.empty:
        raise RuntimeError("no MA break events found")
    summary = _summary(events)
    contrast = _contrast(events)
    yearly = _yearly(events)
    decision = _decision(contrast, yearly)

    previous_event_count = None
    previous_events_path = Path(args.previous_events)
    if previous_events_path.exists():
        previous_event_count = int((sum(1 for _ in previous_events_path.open(encoding="utf-8")) - 1) / 4)
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
    }
    _write_json(out_dir / "input_audit.json", input_audit)
    _write_json(out_dir / "maturity_definition.json", _definition())
    events.to_csv(out_dir / "ma_break_events_with_above_run.csv", index=False, encoding="utf-8")
    summary.to_csv(out_dir / "ma_break_outcome_by_run_bucket.csv", index=False, encoding="utf-8")
    _write_json(out_dir / "maturity_contrast_summary.json", contrast)
    yearly.to_csv(out_dir / "yearly_stability_summary.csv", index=False, encoding="utf-8")
    _write_json(out_dir / "research_decision.json", decision)
    missing = [name for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json" and not (out_dir / name).exists()]
    _write_json(
        out_dir / "_ARTIFACT_COMPLETE.json",
        {
            "axis_id": AXIS_ID,
            "status": "complete" if not missing else "incomplete",
            "missing_artifacts": missing,
            "authoritative_result": str(out_dir / "research_decision.json"),
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        },
    )
    if missing:
        raise RuntimeError(f"missing artifacts: {missing}")
    return out_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TRADEX read-only MA-above run maturity break diagnostic.")
    parser.add_argument("--db-path", default="")
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--start-ymd", type=int, default=20200101)
    parser.add_argument("--end-ymd", type=int, default=None)
    parser.add_argument("--previous-events", type=Path, default=DEFAULT_PREVIOUS_EVENTS)
    return parser.parse_args()


def main() -> None:
    print(run(parse_args()))


if __name__ == "__main__":
    main()
