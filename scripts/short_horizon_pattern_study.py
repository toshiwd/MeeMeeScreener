from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
import sys
from typing import Any

import duckdb
import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.core.config import config
from scripts.month_end_shape_study import (
    ROUND_TRIP_COST_DEFAULT,
    _bucket_cnt100,
    _bucket_cnt60,
    _bucket_dist_ma20,
    _box_state,
    _calc_ma_count_up,
    _detect_body_box,
    _group_summary,
    _rolling_sma,
    _safe_float,
    _summary_from_returns,
    _trend_bucket,
)


@dataclass
class BuildResult:
    events: pd.DataFrame
    stats: dict[str, Any]


def _parse_horizons(raw: str) -> list[int]:
    out: list[int] = []
    for token in str(raw or "").split(","):
        token = token.strip()
        if not token:
            continue
        try:
            v = int(token)
        except ValueError:
            continue
        if v <= 0:
            continue
        out.append(v)
    uniq = sorted(set(out))
    return uniq if uniq else [3, 5, 7, 10, 12, 15, 20, 25, 30]


def _period_label(entry_year: int) -> str:
    if entry_year <= 2002:
        return "1994-2002"
    if entry_year <= 2011:
        return "2003-2011"
    if entry_year <= 2019:
        return "2012-2019"
    return "2020-2026"


def _build_events_with_horizons(con: duckdb.DuckDBPyConnection, horizons: list[int]) -> BuildResult:
    daily = con.execute(
        """
        SELECT
            b.code,
            b.date,
            CAST(b.c AS DOUBLE) AS close,
            CAST(m.ma20 AS DOUBLE) AS ma20,
            CAST(m.ma60 AS DOUBLE) AS ma60
        FROM daily_bars b
        LEFT JOIN daily_ma m ON m.code = b.code AND m.date = b.date
        ORDER BY b.code, b.date
        """
    ).fetchdf()
    monthly = con.execute(
        """
        SELECT
            code,
            month,
            CAST(o AS DOUBLE) AS o,
            CAST(h AS DOUBLE) AS h,
            CAST(l AS DOUBLE) AS l,
            CAST(c AS DOUBLE) AS c
        FROM monthly_bars
        ORDER BY code, month
        """
    ).fetchdf()

    daily["code"] = daily["code"].astype(str)
    daily["dt"] = pd.to_datetime(daily["date"], unit="s", utc=True).dt.tz_localize(None)
    daily["month"] = daily["dt"].dt.to_period("M")
    monthly["code"] = monthly["code"].astype(str)
    monthly["dt"] = pd.to_datetime(monthly["month"], unit="s", utc=True).dt.tz_localize(None)
    monthly["period"] = monthly["dt"].dt.to_period("M")

    daily_groups = {str(code): group.copy() for code, group in daily.groupby("code", sort=False)}
    monthly_groups = {str(code): group.copy() for code, group in monthly.groupby("code", sort=False)}
    code_list = sorted(daily_groups.keys())
    events: list[dict[str, Any]] = []
    skipped_codes = 0
    max_h = int(max(horizons))

    for code in code_list:
        d = daily_groups.get(code)
        if d.empty or len(d) < max(260, max_h + 40):
            skipped_codes += 1
            continue

        closes = d["close"].to_numpy(dtype=np.float64, copy=False)
        ma20 = d["ma20"].to_numpy(dtype=np.float64, copy=False)
        ma60 = d["ma60"].to_numpy(dtype=np.float64, copy=False)
        ma100 = _rolling_sma(closes, 100)
        cnt60 = _calc_ma_count_up(closes, _rolling_sma(closes, 60))
        cnt100 = _calc_ma_count_up(closes, ma100)

        month_to_indices: dict[pd.Period, list[int]] = {}
        month_seq = d["month"].tolist()
        for i, month_key in enumerate(month_seq):
            month_to_indices.setdefault(month_key, []).append(i)
        ordered_months = sorted(month_to_indices.keys())
        if len(ordered_months) < 2:
            skipped_codes += 1
            continue

        m = monthly_groups.get(code)
        m_box_by_period: dict[pd.Period, dict[str, Any] | None] = {}
        if not m.empty:
            m = m.sort_values("period")
            monthly_rows = list(
                zip(
                    m["month"].astype(int).tolist(),
                    m["o"].astype(float).tolist(),
                    m["h"].astype(float).tolist(),
                    m["l"].astype(float).tolist(),
                    m["c"].astype(float).tolist(),
                )
            )
            periods = m["period"].tolist()
            for j, period in enumerate(periods):
                m_box_by_period[period] = _detect_body_box(monthly_rows[: j + 1])

        for mi in range(len(ordered_months) - 1):
            month_key = ordered_months[mi]
            next_month = ordered_months[mi + 1]
            idxs = month_to_indices.get(month_key, [])
            next_idxs = month_to_indices.get(next_month, [])
            if not idxs or not next_idxs:
                continue

            prev_month = month_key - 1
            box = m_box_by_period.get(prev_month)
            box_months = int(box["months"]) if box and box.get("months") is not None else None
            box_wild = bool(box["wild"]) if box and box.get("wild") is not None else None
            box_range = _safe_float(box["range_pct"]) if box else None
            exit_1m_idx = next_idxs[-1]
            exit_1m_close = _safe_float(closes[exit_1m_idx]) if exit_1m_idx < len(closes) else None

            for offset in (2, 1, 0):
                if len(idxs) <= offset:
                    continue
                entry_idx = idxs[-(offset + 1)]
                entry_close = _safe_float(closes[entry_idx]) if entry_idx < len(closes) else None
                if entry_close is None or entry_close <= 0:
                    continue

                ma20_i = _safe_float(ma20[entry_idx])
                ma60_i = _safe_float(ma60[entry_idx])
                ma100_i = _safe_float(ma100[entry_idx])
                dist_ma20 = None
                if ma20_i is not None and ma20_i > 0:
                    dist_ma20 = (entry_close - ma20_i) / ma20_i
                trend = _trend_bucket(entry_close, ma20_i, ma60_i, ma100_i)
                state, box_pos = _box_state(entry_close, box)
                ret20 = None
                if entry_idx >= 20 and closes[entry_idx - 20] > 0:
                    ret20 = (entry_close / closes[entry_idx - 20]) - 1.0
                ret_1m = None
                if exit_1m_close is not None and exit_1m_close > 0:
                    ret_1m = (exit_1m_close / entry_close) - 1.0

                row: dict[str, Any] = {
                    "code": str(code),
                    "entry_dt": int(d.iloc[entry_idx]["date"]),
                    "entry_date": d.iloc[entry_idx]["dt"].date().isoformat(),
                    "entry_month": str(month_key),
                    "entry_offset": f"M-{3 - offset}",
                    "ret_1m": float(ret_1m) if ret_1m is not None else None,
                    "ret20": float(ret20) if ret20 is not None else None,
                    "entry_close": float(entry_close),
                    "ma20": ma20_i,
                    "ma60": ma60_i,
                    "ma100": ma100_i,
                    "dist_ma20": float(dist_ma20) if dist_ma20 is not None else None,
                    "dist_bucket": _bucket_dist_ma20(dist_ma20),
                    "trend_bucket": trend,
                    "cnt60_up": int(cnt60[entry_idx]),
                    "cnt100_up": int(cnt100[entry_idx]),
                    "cnt60_bucket": _bucket_cnt60(float(cnt60[entry_idx])),
                    "cnt100_bucket": _bucket_cnt100(float(cnt100[entry_idx])),
                    "box_state": state,
                    "box_pos": float(box_pos) if box_pos is not None else None,
                    "box_months": box_months,
                    "box_wild": box_wild,
                    "box_range_pct": box_range,
                }
                for h in horizons:
                    exit_idx = entry_idx + int(h)
                    key = f"ret_h{int(h)}"
                    if exit_idx >= len(closes):
                        row[key] = None
                        continue
                    exit_close = _safe_float(closes[exit_idx])
                    if exit_close is None or exit_close <= 0:
                        row[key] = None
                        continue
                    row[key] = float((exit_close / entry_close) - 1.0)
                events.append(row)

    out = pd.DataFrame(events)
    return BuildResult(
        events=out,
        stats={
            "codes_total": int(len(code_list)),
            "codes_skipped": int(skipped_codes),
            "events": int(len(events)),
        },
    )


def _rule_rows_for_short(df: pd.DataFrame, ret_col: str) -> list[dict[str, Any]]:
    masks: list[tuple[str, pd.Series]] = [
        ("all", pd.Series(True, index=df.index)),
        (
            "d1_weak_breakdown",
            (df["trend_bucket"].isin(["mixed", "na"]))
            & (df["box_state"] == "below_box")
            & (df["dist_bucket"] == "far_below")
            & (df["cnt60_up"] < 10)
            & (df["cnt100_up"] < 20),
        ),
        (
            "d2_mixed_breakdown_relaxed",
            (df["trend_bucket"].isin(["mixed", "na"]))
            & (df["box_state"] == "below_box")
            & (df["dist_bucket"] == "far_below"),
        ),
        (
            "d3_early_weak_below",
            (df["trend_bucket"].isin(["mixed", "na"]))
            & (df["dist_bucket"] == "below")
            & (df["cnt60_up"] < 10)
            & (df["cnt100_up"] < 20),
        ),
        (
            "trap_stackdown_far_below",
            (df["trend_bucket"] == "stack_down")
            & (df["dist_bucket"] == "far_below"),
        ),
        (
            "trap_overheat_up",
            (df["trend_bucket"].isin(["up", "stack_up"]))
            & (df["dist_bucket"] == "overheat"),
        ),
        (
            "late_breakout_cnt100_200",
            (df["box_state"] == "breakout_up")
            & (df["cnt100_up"] >= 200),
        ),
        (
            "late_breakout_cnt60_100",
            (df["box_state"] == "breakout_up")
            & (df["cnt60_up"] >= 100),
        ),
        (
            "na_below",
            (df["trend_bucket"] == "na")
            & (df["dist_bucket"] == "below"),
        ),
    ]
    out: list[dict[str, Any]] = []
    for name, mask in masks:
        subset = df[mask]
        arr = subset[ret_col].to_numpy(dtype=np.float64, copy=False)
        stats = _summary_from_returns(arr)
        out.append({"rule": name, **stats})
    return out


def _rule_rows_for_long(df: pd.DataFrame, ret_col: str) -> list[dict[str, Any]]:
    masks: list[tuple[str, pd.Series]] = [
        ("all", pd.Series(True, index=df.index)),
        (
            "l1_box_lower_rebound",
            (df["box_state"].isin(["below_box", "box_lower"]))
            & (df["trend_bucket"].isin(["down", "mixed", "na"]))
            & (df["dist_bucket"].isin(["far_below", "below"]))
            & (df["cnt60_up"] < 20)
            & (df["cnt100_up"] < 40),
        ),
        (
            "l2_box_mid_breakout_setup",
            (df["box_state"].isin(["box_lower", "box_mid"]))
            & (df["trend_bucket"].isin(["mixed", "up", "stack_up"]))
            & (df["dist_bucket"].isin(["near", "above"]))
            & (df["cnt60_up"] >= 5)
            & (df["cnt60_up"] < 120),
        ),
        (
            "l3_breakout_early_not_late",
            (df["box_state"] == "breakout_up")
            & (df["cnt60_up"] < 80)
            & (df["cnt100_up"] < 160)
            & (df["dist_bucket"] != "far_below"),
        ),
        (
            "l4_stack_up_continuation_moderate",
            (df["trend_bucket"] == "stack_up")
            & (df["dist_bucket"].isin(["near", "above"]))
            & (df["cnt60_up"] >= 20)
            & (df["cnt60_up"] < 120)
            & (df["cnt100_up"] < 220),
        ),
        (
            "trap_overheat_late_up",
            (df["trend_bucket"].isin(["up", "stack_up"]))
            & (df["dist_bucket"] == "overheat")
            & (df["cnt60_up"] >= 60),
        ),
        (
            "trap_breakout_too_late",
            (df["box_state"] == "breakout_up")
            & (df["cnt100_up"] >= 200),
        ),
        (
            "trap_below_box_weak",
            (df["box_state"] == "below_box")
            & (df["trend_bucket"].isin(["down", "stack_down"])),
        ),
    ]
    out: list[dict[str, Any]] = []
    for name, mask in masks:
        subset = df[mask]
        arr = subset[ret_col].to_numpy(dtype=np.float64, copy=False)
        stats = _summary_from_returns(arr)
        out.append({"rule": name, **stats})
    return out


def _overall_row(df: pd.DataFrame, ret_col: str, *, horizon: int) -> dict[str, Any]:
    arr = df[ret_col].to_numpy(dtype=np.float64, copy=False)
    row = {"horizon_days": int(horizon), **_summary_from_returns(arr)}
    entry_dt = pd.to_datetime(df["entry_date"], errors="coerce")
    years = entry_dt.dt.year.fillna(0).astype(int)
    period = years.apply(_period_label)
    p_rows: list[dict[str, Any]] = []
    for p, g in df.groupby(period):
        pa = g[ret_col].to_numpy(dtype=np.float64, copy=False)
        s = _summary_from_returns(pa)
        if int(s["n"]) < 200:
            continue
        p_rows.append(
            {
                "period": str(p),
                "n": int(s["n"]),
                "mean": s["mean"],
                "win_rate": s["win_rate"],
                "quality": s["quality"],
            }
        )
    pos = sum(1 for r in p_rows if (r["mean"] or 0.0) > 0.0)
    neg = sum(1 for r in p_rows if (r["mean"] or 0.0) < 0.0)
    row["period_stability"] = {
        "periods": p_rows,
        "positive_periods": int(pos),
        "negative_periods": int(neg),
        "stability_score": float((pos - neg) / max(1, len(p_rows))),
    }
    return row


def _rule_best_rows(rule_matrix: dict[str, list[dict[str, Any]]], min_n: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for rule_name, rows in rule_matrix.items():
        valid = [r for r in rows if r.get("n") is not None and int(r["n"]) >= int(min_n)]
        if not valid:
            continue
        best = max(valid, key=lambda r: (float(r.get("quality") or -999), float(r.get("mean") or -999)))
        out.append(
            {
                "rule": rule_name,
                "best_horizon_days": int(best["horizon_days"]),
                "best_n": int(best["n"]),
                "best_mean": best.get("mean"),
                "best_win_rate": best.get("win_rate"),
                "best_pf": best.get("pf"),
                "best_quality": best.get("quality"),
            }
        )
    out.sort(key=lambda r: (-(r.get("best_quality") or -999), -(r.get("best_mean") or -999)))
    return out


def _selection_rows(overall_rows: list[dict[str, Any]]) -> dict[str, Any]:
    best_by_mean = max(overall_rows, key=lambda r: float(r.get("mean") or -999)) if overall_rows else None
    best_by_quality = max(overall_rows, key=lambda r: float(r.get("quality") or -999)) if overall_rows else None
    practical = [
        r
        for r in overall_rows
        if (r.get("mean") is not None and float(r["mean"]) > 0.0)
        and (r.get("pf") is not None and float(r["pf"]) >= 1.0)
        and (r.get("win_rate") is not None and float(r["win_rate"]) >= 0.5)
    ]
    best_practical = max(practical, key=lambda r: float(r.get("quality") or -999)) if practical else None
    return {
        "best_horizon_by_mean": best_by_mean,
        "best_horizon_by_quality": best_by_quality,
        "best_horizon_practical": best_practical,
    }


def _update_pattern_matrix(
    matrix: dict[str, list[dict[str, Any]]],
    combo: pd.DataFrame,
    *,
    horizon: int,
    combo_cols: list[str],
) -> None:
    if combo.empty:
        return
    keep_cols = [*combo_cols, "n", "win_rate", "mean", "median", "std", "pf", "p10", "cvar10", "quality"]
    for _, row in combo[keep_cols].iterrows():
        key = "|".join(str(row[c]) for c in combo_cols)
        packed = {"horizon_days": int(horizon)}
        for c in keep_cols:
            packed[c] = row[c]
        matrix.setdefault(key, []).append(packed)


def _pattern_best_rows(
    pattern_matrix: dict[str, list[dict[str, Any]]],
    *,
    combo_cols: list[str],
    min_n: int,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for rows in pattern_matrix.values():
        valid = [r for r in rows if r.get("n") is not None and int(r["n"]) >= int(min_n)]
        if not valid:
            continue
        best = max(valid, key=lambda r: (float(r.get("quality") or -999), float(r.get("mean") or -999)))
        worst = min(valid, key=lambda r: (float(r.get("quality") or 999), float(r.get("mean") or 999)))
        row: dict[str, Any] = {}
        for c in combo_cols:
            row[c] = best.get(c)
        row.update(
            {
                "best_horizon_days": int(best["horizon_days"]),
                "best_n": int(best["n"]),
                "best_mean": best.get("mean"),
                "best_win_rate": best.get("win_rate"),
                "best_pf": best.get("pf"),
                "best_quality": best.get("quality"),
                "worst_horizon_days": int(worst["horizon_days"]),
                "worst_mean": worst.get("mean"),
                "worst_quality": worst.get("quality"),
            }
        )
        out.append(row)
    out.sort(
        key=lambda r: (
            -(r.get("best_quality") or -999),
            -(r.get("best_mean") or -999),
            (r.get("worst_quality") or 999),
        )
    )
    return out


def run_study(*, horizons: list[int], round_trip_cost: float, min_samples: int) -> dict[str, Any]:
    with duckdb.connect(str(config.DB_PATH)) as con:
        built = _build_events_with_horizons(con, horizons)
    events = built.events
    if events.empty:
        return {
            "meta": {
                **built.stats,
                "horizons": [int(v) for v in horizons],
                "round_trip_cost": float(round_trip_cost),
                "min_samples": int(min_samples),
            },
            "error": "no_events",
        }

    by_horizon_short: list[dict[str, Any]] = []
    by_horizon_long: list[dict[str, Any]] = []
    by_horizon_compare: list[dict[str, Any]] = []
    rule_matrix_short: dict[str, list[dict[str, Any]]] = {}
    rule_matrix_long: dict[str, list[dict[str, Any]]] = {}
    pattern_matrix_short: dict[str, list[dict[str, Any]]] = {}
    pattern_matrix_long: dict[str, list[dict[str, Any]]] = {}
    combo_cols = ["box_state", "trend_bucket", "dist_bucket", "cnt60_bucket", "cnt100_bucket", "entry_offset"]

    for h in horizons:
        ret_src = f"ret_h{int(h)}"
        if ret_src not in events.columns:
            continue
        subset = events[events[ret_src].notna()].copy()
        if subset.empty:
            continue
        ret_col_short = f"ret_short_h{int(h)}_net"
        ret_col_long = f"ret_long_h{int(h)}_net"
        subset[ret_col_short] = -subset[ret_src].astype(float) - float(round_trip_cost)
        subset[ret_col_long] = subset[ret_src].astype(float) - float(round_trip_cost)

        overall_short = _overall_row(subset, ret_col_short, horizon=int(h))
        overall_long = _overall_row(subset, ret_col_long, horizon=int(h))

        by_offset_short = _group_summary(subset, ["entry_offset"], ret_col_short, min_samples=max(200, min_samples // 4))
        by_box_short = _group_summary(subset, ["box_state"], ret_col_short, min_samples=max(200, min_samples // 4))
        by_offset_long = _group_summary(subset, ["entry_offset"], ret_col_long, min_samples=max(200, min_samples // 4))
        by_box_long = _group_summary(subset, ["box_state"], ret_col_long, min_samples=max(200, min_samples // 4))

        combo_short = _group_summary(subset, combo_cols, ret_col_short, min_samples=min_samples)
        combo_long = _group_summary(subset, combo_cols, ret_col_long, min_samples=min_samples)
        short_bottom = combo_short.sort_values(["quality", "mean"], ascending=[True, True]).reset_index(drop=True)
        long_bottom = combo_long.sort_values(["quality", "mean"], ascending=[True, True]).reset_index(drop=True)
        rules_short = _rule_rows_for_short(subset, ret_col_short)
        rules_long = _rule_rows_for_long(subset, ret_col_long)

        by_horizon_short.append(
            {
                "horizon_days": int(h),
                "overall_short": overall_short,
                "by_entry_offset_short": by_offset_short.to_dict(orient="records"),
                "by_box_state_short": by_box_short.to_dict(orient="records"),
                "top_short_patterns": combo_short.head(15).to_dict(orient="records"),
                "bottom_short_patterns": short_bottom.head(15).to_dict(orient="records"),
                "rule_rows_short": rules_short,
            }
        )
        by_horizon_long.append(
            {
                "horizon_days": int(h),
                "overall_long": overall_long,
                "by_entry_offset_long": by_offset_long.to_dict(orient="records"),
                "by_box_state_long": by_box_long.to_dict(orient="records"),
                "top_long_patterns": combo_long.head(15).to_dict(orient="records"),
                "bottom_long_patterns": long_bottom.head(15).to_dict(orient="records"),
                "rule_rows_long": rules_long,
            }
        )
        by_horizon_compare.append(
            {
                "horizon_days": int(h),
                "short_mean": overall_short.get("mean"),
                "long_mean": overall_long.get("mean"),
                "short_quality": overall_short.get("quality"),
                "long_quality": overall_long.get("quality"),
                "edge_long_minus_short": (
                    float(overall_long.get("mean") or 0.0) - float(overall_short.get("mean") or 0.0)
                ),
            }
        )
        for row in rules_short:
            name = str(row.get("rule"))
            rule_matrix_short.setdefault(name, []).append({"horizon_days": int(h), **row})
        for row in rules_long:
            name = str(row.get("rule"))
            rule_matrix_long.setdefault(name, []).append({"horizon_days": int(h), **row})

        _update_pattern_matrix(pattern_matrix_short, combo_short, horizon=int(h), combo_cols=combo_cols)
        _update_pattern_matrix(pattern_matrix_long, combo_long, horizon=int(h), combo_cols=combo_cols)

    rule_best_short = _rule_best_rows(rule_matrix_short, min_n=max(200, min_samples // 2))
    rule_best_long = _rule_best_rows(rule_matrix_long, min_n=max(200, min_samples // 2))
    pattern_best_short = _pattern_best_rows(
        pattern_matrix_short,
        combo_cols=combo_cols,
        min_n=max(300, min_samples // 2),
    )
    pattern_best_long = _pattern_best_rows(
        pattern_matrix_long,
        combo_cols=combo_cols,
        min_n=max(300, min_samples // 2),
    )

    overall_rows_short = [row["overall_short"] for row in by_horizon_short if isinstance(row.get("overall_short"), dict)]
    overall_rows_long = [row["overall_long"] for row in by_horizon_long if isinstance(row.get("overall_long"), dict)]
    selection_short = _selection_rows(overall_rows_short)
    selection_long = _selection_rows(overall_rows_long)

    entry_min = str(events["entry_date"].min())
    entry_max = str(events["entry_date"].max())

    short_best_mean_h = (
        int(selection_short["best_horizon_by_mean"]["horizon_days"])
        if selection_short.get("best_horizon_by_mean")
        else None
    )
    long_best_mean_h = (
        int(selection_long["best_horizon_by_mean"]["horizon_days"])
        if selection_long.get("best_horizon_by_mean")
        else None
    )
    short_best_quality_h = (
        int(selection_short["best_horizon_by_quality"]["horizon_days"])
        if selection_short.get("best_horizon_by_quality")
        else None
    )
    long_best_quality_h = (
        int(selection_long["best_horizon_by_quality"]["horizon_days"])
        if selection_long.get("best_horizon_by_quality")
        else None
    )

    return {
        "meta": {
            **built.stats,
            "horizons": [int(v) for v in horizons],
            "round_trip_cost": float(round_trip_cost),
            "min_samples": int(min_samples),
            "entry_date_min": entry_min,
            "entry_date_max": entry_max,
        },
        "selection": {
            # Backward-compatible (short side aliases)
            "best_horizon_by_mean": selection_short.get("best_horizon_by_mean"),
            "best_horizon_by_quality": selection_short.get("best_horizon_by_quality"),
            "best_horizon_practical": selection_short.get("best_horizon_practical"),
            "short": selection_short,
            "long": selection_long,
            "comparison": {
                "short_best_horizon_by_mean": short_best_mean_h,
                "long_best_horizon_by_mean": long_best_mean_h,
                "short_is_faster_by_mean": (
                    (short_best_mean_h is not None and long_best_mean_h is not None and short_best_mean_h < long_best_mean_h)
                ),
                "short_best_horizon_by_quality": short_best_quality_h,
                "long_best_horizon_by_quality": long_best_quality_h,
                "short_is_faster_by_quality": (
                    (
                        short_best_quality_h is not None
                        and long_best_quality_h is not None
                        and short_best_quality_h < long_best_quality_h
                    )
                ),
            },
        },
        "overall_by_horizon_short": overall_rows_short,
        "overall_by_horizon_long": overall_rows_long,
        "overall_by_horizon_compare": by_horizon_compare,
        "rule_best_horizon_short": rule_best_short[:50],
        "rule_best_horizon_long": rule_best_long[:50],
        "pattern_best_horizon_short": pattern_best_short[:80],
        "pattern_best_horizon_long": pattern_best_long[:80],
        "details_by_horizon_short": by_horizon_short,
        "details_by_horizon_long": by_horizon_long,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Month-end short holding horizon study")
    parser.add_argument("--horizons", type=str, default="3,5,7,10,12,15,20,25,30")
    parser.add_argument("--round-trip-cost", type=float, default=ROUND_TRIP_COST_DEFAULT)
    parser.add_argument("--min-samples", type=int, default=1200)
    parser.add_argument("--output", type=str, default="tmp/short_horizon_pattern_study.json")
    args = parser.parse_args()

    horizons = _parse_horizons(args.horizons)
    result = run_study(
        horizons=horizons,
        round_trip_cost=float(args.round_trip_cost),
        min_samples=int(args.min_samples),
    )
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[short_horizon_pattern_study] wrote {out_path}")
    meta = result.get("meta", {})
    print(
        "[short_horizon_pattern_study] "
        f"events={meta.get('events')} "
        f"codes={meta.get('codes_total')} "
        f"range={meta.get('entry_date_min')}..{meta.get('entry_date_max')} "
        f"horizons={horizons}"
    )
    selection = result.get("selection", {})
    print("[short_horizon_pattern_study] short best_by_mean=", selection.get("short", {}).get("best_horizon_by_mean"))
    print("[short_horizon_pattern_study] short best_by_quality=", selection.get("short", {}).get("best_horizon_by_quality"))
    print("[short_horizon_pattern_study] short best_practical=", selection.get("short", {}).get("best_horizon_practical"))
    print("[short_horizon_pattern_study] long best_by_mean=", selection.get("long", {}).get("best_horizon_by_mean"))
    print("[short_horizon_pattern_study] long best_by_quality=", selection.get("long", {}).get("best_horizon_by_quality"))
    print("[short_horizon_pattern_study] long best_practical=", selection.get("long", {}).get("best_horizon_practical"))
    print("[short_horizon_pattern_study] comparison=", selection.get("comparison"))


if __name__ == "__main__":
    main()
