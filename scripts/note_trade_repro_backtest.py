from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd


ROUND_TRIP_COST = 0.002
PATH_HORIZONS = [3, 5, 10, 20]
PATTERN_LENGTHS = [2, 3, 4]


def _summary_from_returns(values: pd.Series) -> dict[str, Any]:
    arr = values.dropna().to_numpy(dtype=np.float64, copy=False)
    if arr.size == 0:
        return {"n": 0, "mean": None, "median": None, "win_rate": None, "profit_factor": None, "sum": None}
    gains = arr[arr > 0.0].sum()
    losses = -arr[arr < 0.0].sum()
    if losses <= 0.0:
        profit_factor = None if gains <= 0.0 else float("inf")
    else:
        profit_factor = float(gains / losses)
    return {
        "n": int(arr.size),
        "mean": float(arr.mean()),
        "median": float(np.median(arr)),
        "win_rate": float(np.mean(arr > 0.0)),
        "profit_factor": profit_factor,
        "sum": float(arr.sum()),
    }


def _detect_body_box(monthly_rows: list[tuple[pd.Period, float, float, float, float]]) -> dict[str, float] | None:
    if len(monthly_rows) < 3:
        return None
    bars: list[dict[str, float]] = []
    for _, open_, high, low, close in monthly_rows:
        body_high = max(float(open_), float(close))
        body_low = min(float(open_), float(close))
        bars.append(
            {
                "high": float(high),
                "low": float(low),
                "body_high": float(body_high),
                "body_low": float(body_low),
            }
        )
    for length in range(min(14, len(bars)), 2, -1):
        window = bars[-length:]
        upper = max(item["body_high"] for item in window)
        lower = min(item["body_low"] for item in window)
        base = max(abs(lower), 1e-9)
        range_pct = (upper - lower) / base
        if range_pct > 0.2:
            continue
        wild = False
        for item in window:
            if item["high"] > upper * 1.1 or item["low"] < lower * 0.9:
                wild = True
                break
        return {
            "upper": float(upper),
            "lower": float(lower),
            "months": float(length),
            "range_pct": float(range_pct),
            "wild": 1.0 if wild else 0.0,
        }
    return None


def _resolve_default_db_paths() -> list[Path]:
    candidates = [
        Path(".local/meemee/research_db/stocks_research_20160226_20191231.duckdb"),
        Path(".local/meemee/research_db/stocks_research_20200101_20221231.duckdb"),
        Path(".local/meemee/research_db/stocks_research_20230101_20260226.duckdb"),
        Path("data/stocks.duckdb"),
    ]
    existing = [candidate for candidate in candidates if candidate.exists()]
    if existing:
        return existing
    raise FileNotFoundError("stocks.duckdb not found. Pass --db-path.")


def _load_daily_frame(db_paths: list[Path]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for db_path in db_paths:
        with duckdb.connect(str(db_path), read_only=True) as con:
            df = con.execute(
                """
                SELECT
                  b.code,
                  b.date,
                  b.o,
                  b.h,
                  b.l,
                  b.c,
                  b.v,
                  m.ma7,
                  m.ma20,
                  m.ma60
                FROM daily_bars b
                LEFT JOIN daily_ma m
                  ON m.code = b.code AND m.date = b.date
                ORDER BY b.code, b.date
                """
            ).df()
        if not df.empty:
            frames.append(df)
    if not frames:
        raise RuntimeError(f"daily_bars empty: {[str(path) for path in db_paths]}")
    df = pd.concat(frames, ignore_index=True)
    df = df.sort_values(["code", "date"]).drop_duplicates(["code", "date"], keep="last").reset_index(drop=True)
    df["code"] = df["code"].astype(str)
    df["dt"] = pd.to_datetime(df["date"], unit="s", utc=True).dt.tz_localize(None)
    df["month"] = df["dt"].dt.to_period("M")
    df["week_end"] = (df["dt"] + pd.to_timedelta((4 - df["dt"].dt.weekday) % 7, unit="D")).dt.normalize()
    return df


def _consecutive_count(close: pd.Series, avg: pd.Series, below: bool) -> pd.Series:
    values = close.to_numpy(dtype=np.float64, copy=False)
    means = avg.to_numpy(dtype=np.float64, copy=False)
    out = np.zeros(len(close), dtype=np.int32)
    streak = 0
    for idx, (price, ref) in enumerate(zip(values, means)):
        if not np.isfinite(price) or not np.isfinite(ref):
            streak = 0
        else:
            cond = price < ref if below else price > ref
            streak = streak + 1 if cond else 0
        out[idx] = streak
    return pd.Series(out, index=close.index)


def _bucket_count(value: Any, cuts: list[int], labels: list[str]) -> str:
    if value is None or not np.isfinite(value):
        return "na"
    val = int(value)
    for limit, label in zip(cuts, labels):
        if val <= limit:
            return label
    return labels[-1]


def _build_monthly_premise_map(daily: pd.DataFrame) -> pd.DataFrame:
    monthly = (
        daily.sort_values(["code", "dt"])
        .groupby(["code", "month"], as_index=False)
        .agg(o=("o", "first"), h=("h", "max"), l=("l", "min"), c=("c", "last"))
    )
    rows: list[dict[str, Any]] = []
    for code, group in monthly.groupby("code", sort=False):
        g = group.sort_values("month").reset_index(drop=True)
        closes = g["c"].to_numpy(dtype=np.float64)
        opens = g["o"].to_numpy(dtype=np.float64)
        for idx in range(len(g)):
            hist_rows = list(g.loc[:idx, ["month", "o", "h", "l", "c"]].itertuples(index=False, name=None))
            box = _detect_body_box(hist_rows)
            close_now = float(closes[idx])
            open_now = float(opens[idx])
            prev6 = closes[max(0, idx - 6) : idx]
            prev3 = closes[max(0, idx - 3) : idx]
            premise_label = "other"
            box_zone = "na"
            if box and box["upper"] > box["lower"]:
                pos = (close_now - box["lower"]) / (box["upper"] - box["lower"])
                if pos <= 0.25:
                    box_zone = "lower"
                elif pos < 0.75:
                    box_zone = "mid"
                elif pos <= 1.0:
                    box_zone = "upper"
                else:
                    box_zone = "breakout"
                if pos >= 0.75:
                    premise_label = "top_box_reversal"
                elif 0.25 <= pos < 0.75 and box["months"] >= 4:
                    premise_label = "sideways"
            if prev6.size >= 3:
                if (
                    close_now > float(np.max(prev6)) * 1.01
                    and close_now > open_now
                    and close_now > float(np.mean(prev6))
                ):
                    premise_label = "up_init"
            if prev3.size >= 3:
                monthly_ret = (close_now / max(open_now, 1e-9)) - 1.0
                expected_range = (float(np.max(prev3)) / max(float(np.min(prev3)), 1e-9)) - 1.0
                if abs(monthly_ret) < 0.02 and expected_range < 0.12 and idx >= 4:
                    premise_label = "unexpected_stagnation"
            rows.append({"code": str(code), "month": g.loc[idx, "month"], "premise_label": premise_label, "box_zone": box_zone})
    premise = pd.DataFrame(rows)
    premise["apply_month"] = premise["month"] + 1
    return premise[["code", "apply_month", "premise_label", "box_zone"]]


def _build_weekly_context_map(daily: pd.DataFrame) -> pd.DataFrame:
    weekly = (
        daily.sort_values(["code", "dt"])
        .groupby(["code", "week_end"], as_index=False)
        .agg(o=("o", "first"), h=("h", "max"), l=("l", "min"), c=("c", "last"), v=("v", "sum"))
    )
    rows: list[pd.DataFrame] = []
    for _, group in weekly.groupby("code", sort=False):
        g = group.sort_values("week_end").reset_index(drop=True)
        g["wk_ma20"] = g["c"].rolling(20, min_periods=10).mean()
        g["wk_ma20_slope3"] = g["wk_ma20"] - g["wk_ma20"].shift(3)
        g["week_slope"] = np.where(
            g["wk_ma20_slope3"] > 0.0,
            "up",
            np.where(g["wk_ma20_slope3"] < 0.0, "down", "flat"),
        )
        g["week_lower_high"] = (g["h"] < g["h"].shift(1)) & (g["h"].shift(1) < g["h"].shift(2))
        prev8_low = g["l"].shift(1).rolling(8, min_periods=3).min()
        g["week_near_prev_low"] = prev8_low.notna() & (g["l"] <= prev8_low * 1.03)
        rows.append(g[["code", "week_end", "week_slope", "week_lower_high", "week_near_prev_low"]])
    return pd.concat(rows, ignore_index=True) if rows else weekly.iloc[0:0]


def _add_daily_coordinates(daily: pd.DataFrame) -> pd.DataFrame:
    daily = daily.copy()
    daily["prev_o"] = daily.groupby("code", sort=False)["o"].shift(1)
    daily["prev_h"] = daily.groupby("code", sort=False)["h"].shift(1)
    daily["prev_l"] = daily.groupby("code", sort=False)["l"].shift(1)
    daily["prev_c"] = daily.groupby("code", sort=False)["c"].shift(1)
    daily["prev_v"] = daily.groupby("code", sort=False)["v"].shift(1)
    daily["range"] = daily["h"] - daily["l"]
    daily["body"] = (daily["c"] - daily["o"]).abs()
    daily["lower_wick"] = np.minimum(daily["o"], daily["c"]) - daily["l"]
    daily["upper_wick"] = daily["h"] - np.maximum(daily["o"], daily["c"])
    prev_close = daily.groupby("code", sort=False)["c"].shift(1)
    tr1 = daily["h"] - daily["l"]
    tr2 = (daily["h"] - prev_close).abs()
    tr3 = (daily["l"] - prev_close).abs()
    daily["tr"] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    daily["atr20"] = daily.groupby("code", sort=False)["tr"].transform(lambda s: s.rolling(20, min_periods=10).mean())
    daily["vol20"] = daily.groupby("code", sort=False)["v"].transform(lambda s: s.rolling(20, min_periods=10).mean())
    cnt7_parts = [_consecutive_count(group["c"], group["ma7"], below=True) for _, group in daily.groupby("code", sort=False)]
    cnt20_parts = [_consecutive_count(group["c"], group["ma20"], below=True) for _, group in daily.groupby("code", sort=False)]
    daily["cnt7_down"] = pd.concat(cnt7_parts).sort_index()
    daily["cnt20_down"] = pd.concat(cnt20_parts).sort_index()
    daily["day_pos_ma20"] = np.where(daily["c"] >= daily["ma20"], "above20", "below20")
    daily["day_pos_ma60"] = np.where(daily["c"] >= daily["ma60"], "above60", "below60")
    daily["cnt7_bucket"] = daily["cnt7_down"].map(lambda v: _bucket_count(v, [0, 3, 6], ["0", "1_3", "4_6", "7p"]))
    daily["cnt20_bucket"] = daily["cnt20_down"].map(lambda v: _bucket_count(v, [0, 3, 8], ["0", "1_3", "4_8", "9p"]))
    daily["atr_bucket"] = np.where(
        daily["atr20"].isna(),
        "na",
        np.where(daily["tr"] >= daily["atr20"] * 1.5, "high", np.where(daily["tr"] <= daily["atr20"] * 0.7, "low", "mid")),
    )
    daily["vol_bucket"] = np.where(
        daily["vol20"].isna(),
        "na",
        np.where(daily["v"] >= daily["vol20"] * 1.5, "surge", np.where(daily["v"] <= daily["vol20"] * 0.7, "dry", "mid")),
    )
    return daily


def _add_pattern_columns(daily: pd.DataFrame) -> pd.DataFrame:
    daily = daily.copy()
    body_ratio = np.where(daily["range"] > 0.0, daily["body"] / daily["range"], 0.0)
    dir_tag = np.where(body_ratio <= 0.2, "X", np.where(daily["c"] >= daily["o"], "U", "D"))
    size_tag = np.where(body_ratio >= 0.6, "L", np.where(body_ratio >= 0.3, "M", "S"))
    wick_tag = np.where(
        (daily["lower_wick"] >= np.maximum(daily["body"], 1e-9) * 1.2) & (daily["lower_wick"] > daily["upper_wick"] * 1.1),
        "WL",
        np.where(
            (daily["upper_wick"] >= np.maximum(daily["body"], 1e-9) * 1.2) & (daily["upper_wick"] > daily["lower_wick"] * 1.1),
            "WU",
            np.where(
                (daily["lower_wick"] >= np.maximum(daily["body"], 1e-9)) & (daily["upper_wick"] >= np.maximum(daily["body"], 1e-9)),
                "WB",
                "N",
            ),
        ),
    )
    gap_tag = np.where(
        daily["prev_h"].notna() & (daily["o"] > daily["prev_h"] * 1.005),
        "GU",
        np.where(daily["prev_l"].notna() & (daily["o"] < daily["prev_l"] * 0.995), "GD", "NG"),
    )
    break_tag = np.where(
        daily["prev_h"].notna() & (daily["h"] > daily["prev_h"] * 1.005),
        "HB",
        np.where(daily["prev_l"].notna() & (daily["l"] < daily["prev_l"] * 0.995), "LB", "IN"),
    )
    daily["bar_tag"] = pd.Series(dir_tag, index=daily.index).str.cat(pd.Series(size_tag, index=daily.index), sep="")
    daily["bar_tag"] = daily["bar_tag"].str.cat(pd.Series(wick_tag, index=daily.index), sep="-")
    daily["bar_tag"] = daily["bar_tag"].str.cat(pd.Series(gap_tag, index=daily.index), sep="-")
    daily["bar_tag"] = daily["bar_tag"].str.cat(pd.Series(break_tag, index=daily.index), sep="-")
    for length in PATTERN_LENGTHS:
        pieces = [daily.groupby("code", sort=False)["bar_tag"].shift(step) for step in range(length - 1, -1, -1)]
        pattern = pieces[0].astype("string")
        for part in pieces[1:]:
            pattern = pattern.str.cat(part.astype("string"), sep=">")
        daily[f"pattern_{length}"] = pattern
    return daily


def _add_forward_path_metrics(daily: pd.DataFrame, study_mask: pd.Series | None = None) -> pd.DataFrame:
    daily = daily.copy()
    daily["entry_next_open"] = daily.groupby("code", sort=False)["o"].shift(-1)
    grouped = daily.groupby("code", sort=False)
    for horizon in PATH_HORIZONS:
        exit_close = grouped["c"].shift(-(horizon + 1))
        daily[f"ret_long_{horizon}d"] = (exit_close / daily["entry_next_open"]) - 1.0 - ROUND_TRIP_COST
        high_shifts = [grouped["h"].shift(-step) for step in range(1, horizon + 1)]
        low_shifts = [grouped["l"].shift(-step) for step in range(1, horizon + 1)]
        future_high = pd.concat(high_shifts, axis=1).max(axis=1)
        future_low = pd.concat(low_shifts, axis=1).min(axis=1)
        daily[f"mfe_{horizon}d"] = (future_high / daily["entry_next_open"]) - 1.0
        daily[f"mae_{horizon}d"] = (future_low / daily["entry_next_open"]) - 1.0
    daily["hit_up5_before_dn5_20d"] = False
    daily["hit_dn5_before_up5_20d"] = False
    for _, group in daily.groupby("code", sort=False):
        highs = group["h"].to_numpy(dtype=np.float64, copy=False)
        lows = group["l"].to_numpy(dtype=np.float64, copy=False)
        opens = group["o"].to_numpy(dtype=np.float64, copy=False)
        hit_up = np.zeros(len(group), dtype=bool)
        hit_dn = np.zeros(len(group), dtype=bool)
        local_mask = None
        if study_mask is not None:
            local_mask = study_mask.loc[group.index].to_numpy(dtype=bool, copy=False)
        for idx in range(len(group)):
            if local_mask is not None and not local_mask[idx]:
                continue
            if idx + 1 >= len(group) or not np.isfinite(opens[idx + 1]):
                continue
            entry = opens[idx + 1]
            up_thr = entry * 1.05
            dn_thr = entry * 0.95
            up_idx = None
            dn_idx = None
            end = min(len(group), idx + 21)
            for future_idx in range(idx + 1, end):
                if up_idx is None and highs[future_idx] >= up_thr:
                    up_idx = future_idx
                if dn_idx is None and lows[future_idx] <= dn_thr:
                    dn_idx = future_idx
                if up_idx is not None and dn_idx is not None:
                    break
            if up_idx is not None and (dn_idx is None or up_idx < dn_idx):
                hit_up[idx] = True
            if dn_idx is not None and (up_idx is None or dn_idx < up_idx):
                hit_dn[idx] = True
        daily.loc[group.index, "hit_up5_before_dn5_20d"] = hit_up
        daily.loc[group.index, "hit_dn5_before_up5_20d"] = hit_dn
    return daily


def _aggregate_pattern_study(frame: pd.DataFrame, pattern_len: int, min_samples: int) -> list[dict[str, Any]]:
    pattern_col = f"pattern_{pattern_len}"
    work = frame.loc[frame[pattern_col].notna() & frame["ret_long_10d"].notna()].copy()
    baseline = (
        work.groupby("regime_key", as_index=False)
        .agg(
            regime_n=("ret_long_10d", "size"),
            regime_mean=("ret_long_10d", "mean"),
            regime_win_rate=("ret_long_10d", lambda s: float(np.mean(s > 0.0))),
        )
    )
    grouped = (
        work.groupby(["regime_key", pattern_col], as_index=False)
        .agg(
            n=("ret_long_10d", "size"),
            mean_ret_3d=("ret_long_3d", "mean"),
            mean_ret_5d=("ret_long_5d", "mean"),
            mean_ret_10d=("ret_long_10d", "mean"),
            mean_ret_20d=("ret_long_20d", "mean"),
            win_rate_10d=("ret_long_10d", lambda s: float(np.mean(s > 0.0))),
            mfe_20d=("mfe_20d", "mean"),
            mae_20d=("mae_20d", "mean"),
            up5_before_dn5_20d=("hit_up5_before_dn5_20d", "mean"),
            dn5_before_up5_20d=("hit_dn5_before_up5_20d", "mean"),
        )
    )
    merged = grouped.merge(baseline, on="regime_key", how="left")
    merged["delta_mean_10d_vs_regime"] = merged["mean_ret_10d"] - merged["regime_mean"]
    merged["pattern_len"] = pattern_len
    merged = merged.loc[merged["n"] >= int(min_samples)]
    merged = merged.sort_values(["delta_mean_10d_vs_regime", "mean_ret_10d", "n"], ascending=[False, False, False])
    return merged.head(30).to_dict(orient="records")


def _build_markdown_report(result: dict[str, Any]) -> str:
    lines = [
        "# Regime x Pattern x Path Study",
        "",
        "## Setup",
        "",
        f"- DBs: `{', '.join(result['meta']['db_paths'])}`",
        f"- Codes: `{result['meta']['codes']}`",
        f"- Date range: `{result['meta']['date_min']}` to `{result['meta']['date_max']}`",
        f"- Round-trip cost: `{result['meta']['round_trip_cost']:.3f}`",
        f"- Min samples per regime-pattern: `{result['meta']['min_samples']}`",
        "",
    ]
    for pattern_len in PATTERN_LENGTHS:
        rows = result["pattern_study"].get(f"pattern_{pattern_len}", [])
        lines.extend([f"## Top Pattern {pattern_len}", "", "| regime_key | pattern | n | ret10d | delta_vs_regime | win10d | mfe20d | mae20d | up5_before_dn5 |", "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"])
        for row in rows[:10]:
            lines.append(
                "| "
                + f"{row['regime_key']} | {row[f'pattern_{pattern_len}']} | {int(row['n'])} | "
                + f"{row['mean_ret_10d']:.4f} | {row['delta_mean_10d_vs_regime']:.4f} | {row['win_rate_10d']:.3f} | "
                + f"{row['mfe_20d']:.4f} | {row['mae_20d']:.4f} | {row['up5_before_dn5_20d']:.3f} |"
            )
        lines.append("")
    lines.extend(
        [
            "## Notes",
            "",
            "- `delta_vs_regime` is the key metric. Positive means the pattern beat the same regime baseline.",
            "- `mfe20d` and `mae20d` show path quality. A pattern can have positive ret10d but poor path if `mae20d` is too deep.",
            "- `up5_before_dn5_20d` is a simple path score for bottoming-style entries.",
        ]
    )
    return "\n".join(lines) + "\n"


def run_backtest(db_paths: list[Path], min_samples: int) -> dict[str, Any]:
    daily = _load_daily_frame(db_paths)
    premise_map = _build_monthly_premise_map(daily)
    weekly_map = _build_weekly_context_map(daily)
    daily = daily.merge(premise_map, how="left", left_on=["code", "month"], right_on=["code", "apply_month"])
    daily = daily.merge(weekly_map, how="left", on=["code", "week_end"])
    daily["premise_label"] = daily["premise_label"].fillna("other")
    daily["box_zone"] = daily["box_zone"].fillna("na")
    daily["week_slope"] = daily["week_slope"].fillna("na")
    daily["week_lower_high"] = daily["week_lower_high"].fillna(False).astype(bool)
    daily["week_near_prev_low"] = daily["week_near_prev_low"].fillna(False).astype(bool)
    daily = _add_daily_coordinates(daily)
    daily = _add_pattern_columns(daily)
    study_mask = (
        daily["premise_label"].isin(["up_init", "top_box_reversal", "sideways", "unexpected_stagnation"])
        & ((daily["day_pos_ma20"] == "below20") | (daily["cnt20_down"] >= 1))
    )
    daily = _add_forward_path_metrics(daily, study_mask=study_mask)
    week_structure = np.where(daily["week_near_prev_low"], "support", np.where(daily["week_lower_high"], "lowerhigh", "neutral"))
    daily["regime_key"] = pd.Series(daily["premise_label"], index=daily.index)
    daily["regime_key"] = daily["regime_key"].str.cat(pd.Series("wk_" + daily["week_slope"].astype(str), index=daily.index), sep="|")
    daily["regime_key"] = daily["regime_key"].str.cat(pd.Series(week_structure, index=daily.index), sep="|")
    daily["regime_key"] = daily["regime_key"].str.cat(daily["day_pos_ma20"].astype(str), sep="|")
    daily["regime_key"] = daily["regime_key"].str.cat(daily["day_pos_ma60"].astype(str), sep="|")
    daily["regime_key"] = daily["regime_key"].str.cat(pd.Series("7d_" + daily["cnt7_bucket"].astype(str), index=daily.index), sep="|")
    daily["regime_key"] = daily["regime_key"].str.cat(pd.Series("20d_" + daily["cnt20_bucket"].astype(str), index=daily.index), sep="|")
    daily["regime_key"] = daily["regime_key"].str.cat(pd.Series("atr_" + daily["atr_bucket"].astype(str), index=daily.index), sep="|")
    daily["regime_key"] = daily["regime_key"].str.cat(pd.Series("vol_" + daily["vol_bucket"].astype(str), index=daily.index), sep="|")
    daily["regime_key"] = daily["regime_key"].str.cat(pd.Series("box_" + daily["box_zone"].astype(str), index=daily.index), sep="|")
    daily = daily.loc[study_mask].copy()
    pattern_study = {
        f"pattern_{pattern_len}": _aggregate_pattern_study(daily, pattern_len=pattern_len, min_samples=min_samples)
        for pattern_len in PATTERN_LENGTHS
    }
    return {
        "meta": {
            "db_paths": [str(path) for path in db_paths],
            "codes": int(daily["code"].nunique()),
            "date_min": str(daily["dt"].min().date()),
            "date_max": str(daily["dt"].max().date()),
            "round_trip_cost": ROUND_TRIP_COST,
            "min_samples": int(min_samples),
            "study_rows": int(len(daily)),
        },
        "pattern_study": pattern_study,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Regime x pattern x path backtest for note-style studies")
    parser.add_argument("--db-path", type=Path, action="append", default=None, help="stocks.duckdb path; repeatable")
    parser.add_argument("--min-samples", type=int, default=80)
    parser.add_argument("--output-json", type=Path, default=Path("tmp/note_trade_repro_backtest.json"))
    parser.add_argument("--output-md", type=Path, default=Path("tmp/note_trade_repro_backtest.md"))
    args = parser.parse_args()

    db_paths = args.db_path or _resolve_default_db_paths()
    result = run_backtest(db_paths, min_samples=max(20, int(args.min_samples)))
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_md.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    args.output_md.write_text(_build_markdown_report(result), encoding="utf-8")
    print(f"[ok] wrote {args.output_json}")
    print(f"[ok] wrote {args.output_md}")
    print(json.dumps(result["meta"], ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
