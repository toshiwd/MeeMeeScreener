from __future__ import annotations

import argparse
import json
import math
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


ROUND_TRIP_COST_DEFAULT = 0.002


def _safe_float(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(out):
        return None
    return out


def _rolling_sma(values: np.ndarray, period: int) -> np.ndarray:
    out = np.full(values.shape[0], np.nan, dtype=np.float64)
    if period <= 0 or values.size < period:
        return out
    csum = np.cumsum(values, dtype=np.float64)
    out[period - 1] = csum[period - 1] / float(period)
    for i in range(period, values.size):
        out[i] = (csum[i] - csum[i - period]) / float(period)
    return out


def _calc_ma_count_up(closes: np.ndarray, ma: np.ndarray) -> np.ndarray:
    # Match frontend-like count behavior: two opposite bars to fully reset side.
    up_count = 0
    down_count = 0
    pending: str | None = None
    out = np.zeros(closes.shape[0], dtype=np.int32)
    for i in range(closes.shape[0]):
        close = closes[i]
        avg = ma[i]
        if not np.isfinite(close) or not np.isfinite(avg):
            up_count = 0
            down_count = 0
            pending = None
            out[i] = 0
            continue
        side = "up" if close >= avg else "down"
        if side == "up":
            if up_count > 0:
                up_count += 1
                pending = None
            elif down_count > 0:
                if pending == "up":
                    up_count = 2
                    down_count = 0
                    pending = None
                else:
                    pending = "up"
            else:
                up_count = 1
                down_count = 0
                pending = None
        else:
            if down_count > 0:
                down_count += 1
                pending = None
            elif up_count > 0:
                if pending == "down":
                    down_count = 2
                    up_count = 0
                    pending = None
                else:
                    pending = "down"
            else:
                down_count = 1
                up_count = 0
                pending = None
        out[i] = int(up_count)
    return out


def _detect_body_box(monthly_rows: list[tuple[int, float, float, float, float]]) -> dict[str, Any] | None:
    min_months = 3
    max_months = 14
    max_range_pct = 0.2
    wild_wick_pct = 0.1
    if len(monthly_rows) < min_months:
        return None
    bars: list[dict[str, float]] = []
    for month, open_, high, low, close in monthly_rows:
        body_high = max(float(open_), float(close))
        body_low = min(float(open_), float(close))
        bars.append(
            {
                "time": float(month),
                "open": float(open_),
                "high": float(high),
                "low": float(low),
                "close": float(close),
                "body_high": float(body_high),
                "body_low": float(body_low),
            }
        )
    length_max = min(max_months, len(bars))
    for length in range(length_max, min_months - 1, -1):
        window = bars[-length:]
        upper = max(item["body_high"] for item in window)
        lower = min(item["body_low"] for item in window)
        base = max(abs(lower), 1e-9)
        range_pct = (upper - lower) / base
        if range_pct > max_range_pct:
            continue
        wild = False
        for item in window:
            if item["high"] > upper * (1.0 + wild_wick_pct) or item["low"] < lower * (1.0 - wild_wick_pct):
                wild = True
                break
        return {
            "start": int(window[0]["time"]),
            "end": int(window[-1]["time"]),
            "upper": float(upper),
            "lower": float(lower),
            "months": int(length),
            "range_pct": float(range_pct),
            "wild": bool(wild),
            "last_close": float(window[-1]["close"]),
        }
    return None


def _bucket_dist_ma20(dist: float | None) -> str:
    if dist is None or not math.isfinite(dist):
        return "na"
    if dist < -0.05:
        return "far_below"
    if dist < 0.0:
        return "below"
    if dist < 0.05:
        return "near"
    if dist < 0.12:
        return "extended"
    return "overheat"


def _bucket_cnt60(value: float | None) -> str:
    if value is None or not math.isfinite(value):
        return "na"
    v = int(value)
    if v < 10:
        return "0-9"
    if v < 30:
        return "10-29"
    if v < 60:
        return "30-59"
    if v < 100:
        return "60-99"
    return "100+"


def _bucket_cnt100(value: float | None) -> str:
    if value is None or not math.isfinite(value):
        return "na"
    v = int(value)
    if v < 20:
        return "0-19"
    if v < 50:
        return "20-49"
    if v < 100:
        return "50-99"
    if v < 200:
        return "100-199"
    return "200+"


def _trend_bucket(close: float, ma20: float | None, ma60: float | None, ma100: float | None) -> str:
    if ma20 is None or ma60 is None or ma100 is None:
        return "na"
    if close > ma20 > ma60 > ma100:
        return "stack_up"
    if close > ma20 > ma60:
        return "up"
    if close < ma20 < ma60 < ma100:
        return "stack_down"
    if close < ma20 < ma60:
        return "down"
    return "mixed"


def _box_state(entry_close: float, box: dict[str, Any] | None) -> tuple[str, float | None]:
    if not box:
        return "no_box", None
    lower = _safe_float(box.get("lower"))
    upper = _safe_float(box.get("upper"))
    if lower is None or upper is None or upper <= lower:
        return "no_box", None
    pos = (entry_close - lower) / (upper - lower)
    if pos < 0.0:
        return "below_box", pos
    if pos <= 0.25:
        return "box_lower", pos
    if pos <= 0.75:
        return "box_mid", pos
    if pos <= 1.0:
        return "box_upper", pos
    return "breakout_up", pos


def _summary_from_returns(rets: np.ndarray) -> dict[str, Any]:
    if rets.size == 0:
        return {
            "n": 0,
            "win_rate": None,
            "mean": None,
            "median": None,
            "std": None,
            "pf": None,
            "p10": None,
            "cvar10": None,
            "quality": None,
        }
    wins = rets > 0
    mean = float(np.mean(rets))
    median = float(np.median(rets))
    std = float(np.std(rets, ddof=0))
    pos_sum = float(np.sum(rets[rets > 0])) if np.any(rets > 0) else 0.0
    neg_sum = float(np.sum(rets[rets < 0])) if np.any(rets < 0) else 0.0
    pf = None
    if neg_sum < 0:
        pf = float(pos_sum / abs(neg_sum))
    p10 = float(np.quantile(rets, 0.10))
    cvar10 = float(np.mean(rets[rets <= p10])) if np.any(rets <= p10) else p10
    sharpe_like = float(mean / std) if std > 1e-12 else 0.0
    quality = float(sharpe_like * math.sqrt(max(1.0, min(2000.0, float(rets.size))) / 2000.0))
    return {
        "n": int(rets.size),
        "win_rate": float(np.mean(wins)),
        "mean": mean,
        "median": median,
        "std": std,
        "pf": pf,
        "p10": p10,
        "cvar10": cvar10,
        "quality": quality,
    }


def _group_summary(df: pd.DataFrame, group_cols: list[str], ret_col: str, min_samples: int) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for keys, g in df.groupby(group_cols, dropna=False):
        arr = g[ret_col].to_numpy(dtype=np.float64, copy=False)
        s = _summary_from_returns(arr)
        if int(s["n"]) < min_samples:
            continue
        row = {}
        if len(group_cols) == 1:
            row[group_cols[0]] = keys
        else:
            for i, c in enumerate(group_cols):
                row[c] = keys[i]
        row.update(s)
        rows.append(row)
    if not rows:
        return pd.DataFrame(columns=[*group_cols, "n", "win_rate", "mean", "median", "std", "pf", "p10", "cvar10", "quality"])
    out = pd.DataFrame(rows)
    out = out.sort_values(["quality", "mean", "win_rate"], ascending=[False, False, False]).reset_index(drop=True)
    return out


@dataclass
class BuildResult:
    events: pd.DataFrame
    stats: dict[str, Any]


def _build_events(con: duckdb.DuckDBPyConnection) -> BuildResult:
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
    for code in code_list:
        d = daily_groups.get(code)
        if d.empty or len(d) < 260:
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
            if len(idxs) < 1 or len(next_idxs) < 1:
                continue
            exit_idx = next_idxs[-1]
            exit_close = _safe_float(closes[exit_idx])
            if exit_close is None or exit_close <= 0:
                continue
            prev_month = month_key - 1
            box = m_box_by_period.get(prev_month)
            box_months = int(box["months"]) if box and box.get("months") is not None else None
            box_wild = bool(box["wild"]) if box and box.get("wild") is not None else None
            box_range = _safe_float(box["range_pct"]) if box else None
            for offset in (2, 1, 0):
                if len(idxs) <= offset:
                    continue
                entry_idx = idxs[-(offset + 1)]
                if entry_idx >= exit_idx:
                    continue
                entry_close = _safe_float(closes[entry_idx])
                if entry_close is None or entry_close <= 0:
                    continue
                ret_1m = (exit_close / entry_close) - 1.0
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
                events.append(
                    {
                        "code": str(code),
                        "entry_dt": int(d.iloc[entry_idx]["date"]),
                        "entry_date": d.iloc[entry_idx]["dt"].date().isoformat(),
                        "entry_month": str(month_key),
                        "entry_offset": f"M-{3 - offset}",
                        "exit_dt": int(d.iloc[exit_idx]["date"]),
                        "exit_date": d.iloc[exit_idx]["dt"].date().isoformat(),
                        "exit_month": str(next_month),
                        "ret_1m": float(ret_1m),
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
                )

    out = pd.DataFrame(events)
    return BuildResult(
        events=out,
        stats={
            "codes_total": int(len(code_list)),
            "codes_skipped": int(skipped_codes),
            "events": int(len(events)),
        },
    )


def _rule_table(df: pd.DataFrame, round_trip_cost: float) -> list[dict[str, Any]]:
    rows: list[tuple[str, pd.Series]] = []
    rows.append(("all", pd.Series(True, index=df.index)))
    rows.append(("trend_stack_up", df["trend_bucket"] == "stack_up"))
    rows.append(("trend_up_any", df["trend_bucket"].isin(["stack_up", "up"])))
    rows.append(("box_lower", df["box_state"] == "box_lower"))
    rows.append(("box_lower_4m_plus", (df["box_state"] == "box_lower") & (df["box_months"].fillna(0) >= 4)))
    rows.append(
        (
            "box_lower_with_up_trend",
            (df["box_state"] == "box_lower") & (df["trend_bucket"].isin(["stack_up", "up"])),
        )
    )
    rows.append(
        (
            "box_lower_not_overheat",
            (df["box_state"] == "box_lower")
            & (df["dist_bucket"].isin(["below", "near"]))
            & (df["cnt60_up"] < 60),
        )
    )
    rows.append(("breakout_up", df["box_state"] == "breakout_up"))
    rows.append(
        (
            "breakout_stack_up_moderate_streak",
            (df["box_state"] == "breakout_up")
            & (df["trend_bucket"] == "stack_up")
            & (df["cnt60_up"] >= 30)
            & (df["cnt60_up"] < 100),
        )
    )
    rows.append(
        (
            "deep_rebound_candidate",
            (df["trend_bucket"] == "stack_down")
            & (df["dist_bucket"] == "far_below")
            & (df["cnt60_up"] < 10),
        )
    )
    rows.append(
        (
            "likely_failure_buy",
            (df["box_state"].isin(["below_box", "no_box"]))
            & (df["trend_bucket"].isin(["na", "mixed"]))
            & (df["dist_bucket"] == "below")
            & (df["cnt60_up"] < 10),
        )
    )
    rows.append(("overheat", (df["dist_bucket"] == "overheat") | (df["cnt60_up"] >= 100) | (df["cnt100_up"] >= 200)))
    rows.append(
        (
            "balanced_trend",
            (df["trend_bucket"].isin(["stack_up", "up"]))
            & (df["dist_bucket"].isin(["near", "extended"]))
            & (df["cnt60_up"] >= 10)
            & (df["cnt60_up"] < 100),
        )
    )

    out: list[dict[str, Any]] = []
    for name, mask in rows:
        subset = df[mask]
        long_rets = subset["ret_long_net"].to_numpy(dtype=np.float64, copy=False)
        short_rets = subset["ret_short_net"].to_numpy(dtype=np.float64, copy=False)
        s_long = _summary_from_returns(long_rets)
        s_short = _summary_from_returns(short_rets)
        out.append(
            {
                "rule": name,
                "n": int(len(subset)),
                "long": s_long,
                "short": s_short,
                "cost_assumed_round_trip": float(round_trip_cost),
            }
        )
    return out


def run_analysis(round_trip_cost: float, min_samples: int) -> dict[str, Any]:
    with duckdb.connect(str(config.DB_PATH)) as con:
        built = _build_events(con)
    events = built.events
    if events.empty:
        return {
            "meta": {
                **built.stats,
                "round_trip_cost": float(round_trip_cost),
                "min_samples": int(min_samples),
            },
            "error": "no_events",
        }

    events["ret_long_net"] = events["ret_1m"] - float(round_trip_cost)
    events["ret_short_net"] = -events["ret_1m"] - float(round_trip_cost)
    overall_long = _summary_from_returns(events["ret_long_net"].to_numpy(dtype=np.float64))
    overall_short = _summary_from_returns(events["ret_short_net"].to_numpy(dtype=np.float64))

    by_offset_long = _group_summary(events, ["entry_offset"], "ret_long_net", min_samples=max(1, min_samples // 4))
    by_box_long = _group_summary(events, ["box_state"], "ret_long_net", min_samples=max(1, min_samples // 4))
    by_cnt60_long = _group_summary(events, ["cnt60_bucket"], "ret_long_net", min_samples=max(1, min_samples // 4))
    by_cnt100_long = _group_summary(events, ["cnt100_bucket"], "ret_long_net", min_samples=max(1, min_samples // 4))
    by_dist_long = _group_summary(events, ["dist_bucket"], "ret_long_net", min_samples=max(1, min_samples // 4))
    by_trend_long = _group_summary(events, ["trend_bucket"], "ret_long_net", min_samples=max(1, min_samples // 4))

    combo_cols = ["box_state", "trend_bucket", "dist_bucket", "cnt60_bucket", "cnt100_bucket", "entry_offset"]
    long_combo = _group_summary(events, combo_cols, "ret_long_net", min_samples=min_samples)
    short_combo = _group_summary(events, combo_cols, "ret_short_net", min_samples=min_samples)
    worst_long_combo = long_combo.sort_values(["quality", "mean"], ascending=[True, True]).reset_index(drop=True)

    top_long = long_combo.head(15).to_dict(orient="records")
    top_short = short_combo.head(15).to_dict(orient="records")
    bottom_long = worst_long_combo.head(15).to_dict(orient="records")
    rule_table = _rule_table(events, round_trip_cost)

    return {
        "meta": {
            **built.stats,
            "round_trip_cost": float(round_trip_cost),
            "min_samples": int(min_samples),
            "date_min": str(events["entry_date"].min()),
            "date_max": str(events["entry_date"].max()),
        },
        "overall": {
            "long": overall_long,
            "short": overall_short,
        },
        "slices": {
            "by_entry_offset_long": by_offset_long.to_dict(orient="records"),
            "by_box_state_long": by_box_long.to_dict(orient="records"),
            "by_cnt60_long": by_cnt60_long.to_dict(orient="records"),
            "by_cnt100_long": by_cnt100_long.to_dict(orient="records"),
            "by_dist_long": by_dist_long.to_dict(orient="records"),
            "by_trend_long": by_trend_long.to_dict(orient="records"),
        },
        "patterns": {
            "top_long": top_long,
            "bottom_long": bottom_long,
            "top_short": top_short,
        },
        "rules": rule_table,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Month-end (last 3 trading days) entry shape study")
    parser.add_argument("--output", type=Path, default=Path("tmp/month_end_shape_study.json"))
    parser.add_argument("--round-trip-cost", type=float, default=ROUND_TRIP_COST_DEFAULT)
    parser.add_argument("--min-samples", type=int, default=120)
    args = parser.parse_args()

    payload = run_analysis(
        round_trip_cost=float(args.round_trip_cost),
        min_samples=max(30, int(args.min_samples)),
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[ok] wrote {args.output}")
    if "error" in payload:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 1
    print(json.dumps(payload["meta"], ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
