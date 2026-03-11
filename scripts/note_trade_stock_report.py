from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd


TARGETS = [
    {"code": "5541", "name": "大平洋金属", "start": "2025-09-01", "end": "2025-12-31"},
    {"code": "2317", "name": "システナ", "start": "2025-12-01", "end": "2026-02-10"},
    {"code": "2531", "name": "宝HD", "start": "2025-12-01", "end": "2025-12-31"},
    {"code": "9697", "name": "カプコン", "start": "2025-09-01", "end": "2025-11-30"},
]


def _resolve_default_db_paths() -> list[Path]:
    candidates = [
        Path(".local/meemee/research_db/stocks_research_20160226_20191231.duckdb"),
        Path(".local/meemee/research_db/stocks_research_20200101_20221231.duckdb"),
        Path(".local/meemee/research_db/stocks_research_20230101_20260226.duckdb"),
    ]
    existing = [candidate for candidate in candidates if candidate.exists()]
    if existing:
        return existing
    raise FileNotFoundError("stocks.duckdb not found. Pass --db-path.")


def _load_daily_frame(db_paths: list[Path], codes: list[str]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for db_path in db_paths:
        with duckdb.connect(str(db_path), read_only=True) as con:
            query = """
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
                WHERE b.code IN ({})
                ORDER BY b.code, b.date
            """.format(",".join("?" for _ in codes))
            df = con.execute(query, codes).df()
        if not df.empty:
            frames.append(df)
    if not frames:
        raise RuntimeError("daily_bars empty")
    df = pd.concat(frames, ignore_index=True)
    df = df.sort_values(["code", "date"]).drop_duplicates(["code", "date"], keep="last").reset_index(drop=True)
    df["code"] = df["code"].astype(str)
    df["dt"] = pd.to_datetime(df["date"], unit="s", utc=True).dt.tz_localize(None)
    df["month"] = df["dt"].dt.to_period("M")
    df["week_end"] = (df["dt"] + pd.to_timedelta((4 - df["dt"].dt.weekday) % 7, unit="D")).dt.normalize()
    return df


def _detect_body_box(monthly_rows: list[tuple[pd.Period, float, float, float, float]]) -> dict[str, float] | None:
    if len(monthly_rows) < 3:
        return None
    bars: list[dict[str, float]] = []
    for _, open_, high, low, close in monthly_rows:
        body_high = max(float(open_), float(close))
        body_low = min(float(open_), float(close))
        bars.append({"high": float(high), "low": float(low), "body_high": body_high, "body_low": body_low})
    for length in range(min(14, len(bars)), 2, -1):
        window = bars[-length:]
        upper = max(item["body_high"] for item in window)
        lower = min(item["body_low"] for item in window)
        if (upper - lower) / max(abs(lower), 1e-9) > 0.2:
            continue
        return {"upper": upper, "lower": lower, "months": float(length)}
    return None


def _build_monthly_context(daily: pd.DataFrame) -> pd.DataFrame:
    monthly = (
        daily.sort_values(["code", "dt"])
        .groupby(["code", "month"], as_index=False)
        .agg(o=("o", "first"), h=("h", "max"), l=("l", "min"), c=("c", "last"))
    )
    rows: list[dict[str, object]] = []
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
            label = "other"
            box_pos = "na"
            if box and box["upper"] > box["lower"]:
                pos = (close_now - box["lower"]) / (box["upper"] - box["lower"])
                box_pos = "lower" if pos <= 0.25 else ("mid" if pos < 0.75 else "upper")
                if pos >= 0.75:
                    label = "top_box_reversal"
                elif 0.25 <= pos < 0.75 and box["months"] >= 4:
                    label = "sideways"
            if prev6.size >= 3 and close_now > float(np.max(prev6)) * 1.01 and close_now > open_now:
                label = "up_init"
            rows.append({"code": code, "month": g.loc[idx, "month"], "monthly_label": label, "monthly_box_pos": box_pos})
    out = pd.DataFrame(rows)
    out["apply_month"] = out["month"] + 1
    return out[["code", "apply_month", "monthly_label", "monthly_box_pos"]]


def _build_weekly_context(daily: pd.DataFrame) -> pd.DataFrame:
    weekly = (
        daily.sort_values(["code", "dt"])
        .groupby(["code", "week_end"], as_index=False)
        .agg(o=("o", "first"), h=("h", "max"), l=("l", "min"), c=("c", "last"), v=("v", "sum"))
    )
    rows: list[pd.DataFrame] = []
    for _, group in weekly.groupby("code", sort=False):
        g = group.sort_values("week_end").reset_index(drop=True)
        g["wk_ma20"] = g["c"].rolling(20, min_periods=10).mean()
        slope = g["wk_ma20"] - g["wk_ma20"].shift(3)
        g["weekly_slope"] = np.where(slope > 0.0, "up", np.where(slope < 0.0, "down", "flat"))
        g["weekly_lower_high"] = (g["h"] < g["h"].shift(1)) & (g["h"].shift(1) < g["h"].shift(2))
        prev8_low = g["l"].shift(1).rolling(8, min_periods=3).min()
        g["weekly_near_prev_low"] = prev8_low.notna() & (g["l"] <= prev8_low * 1.03)
        rows.append(g[["code", "week_end", "weekly_slope", "weekly_lower_high", "weekly_near_prev_low"]])
    return pd.concat(rows, ignore_index=True)


def _consecutive_count(close: pd.Series, avg: pd.Series) -> pd.Series:
    values = close.to_numpy(dtype=np.float64, copy=False)
    means = avg.to_numpy(dtype=np.float64, copy=False)
    out = np.zeros(len(close), dtype=np.int32)
    streak = 0
    for idx, (price, ref) in enumerate(zip(values, means)):
        if not np.isfinite(price) or not np.isfinite(ref):
            streak = 0
        elif price < ref:
            streak += 1
        else:
            streak = 0
        out[idx] = streak
    return pd.Series(out, index=close.index)


def _tag_daily_bars(daily: pd.DataFrame) -> pd.DataFrame:
    daily = daily.copy()
    daily["prev_h"] = daily.groupby("code", sort=False)["h"].shift(1)
    daily["prev_l"] = daily.groupby("code", sort=False)["l"].shift(1)
    daily["prev_c"] = daily.groupby("code", sort=False)["c"].shift(1)
    daily["range"] = daily["h"] - daily["l"]
    daily["body"] = (daily["c"] - daily["o"]).abs()
    daily["lower_wick"] = np.minimum(daily["o"], daily["c"]) - daily["l"]
    daily["upper_wick"] = daily["h"] - np.maximum(daily["o"], daily["c"])
    daily["cnt7_down"] = pd.concat(
        [_consecutive_count(group["c"], group["ma7"]) for _, group in daily.groupby("code", sort=False)]
    ).sort_index()
    daily["cnt20_down"] = pd.concat(
        [_consecutive_count(group["c"], group["ma20"]) for _, group in daily.groupby("code", sort=False)]
    ).sort_index()
    daily["vol20"] = daily.groupby("code", sort=False)["v"].transform(lambda s: s.rolling(20, min_periods=10).mean())
    body_ratio = np.where(daily["range"] > 0.0, daily["body"] / daily["range"], 0.0)
    direction = np.where(body_ratio <= 0.2, "X", np.where(daily["c"] >= daily["o"], "U", "D"))
    size = np.where(body_ratio >= 0.6, "L", np.where(body_ratio >= 0.3, "M", "S"))
    wick = np.where(
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
    gap = np.where(
        daily["prev_h"].notna() & (daily["o"] > daily["prev_h"] * 1.005),
        "GU",
        np.where(daily["prev_l"].notna() & (daily["o"] < daily["prev_l"] * 0.995), "GD", "NG"),
    )
    break_pos = np.where(
        daily["prev_h"].notna() & (daily["h"] > daily["prev_h"] * 1.005),
        "HB",
        np.where(daily["prev_l"].notna() & (daily["l"] < daily["prev_l"] * 0.995), "LB", "IN"),
    )
    daily["bar_tag"] = pd.Series(direction, index=daily.index).str.cat(pd.Series(size, index=daily.index), sep="")
    daily["bar_tag"] = daily["bar_tag"].str.cat(pd.Series(wick, index=daily.index), sep="-")
    daily["bar_tag"] = daily["bar_tag"].str.cat(pd.Series(gap, index=daily.index), sep="-")
    daily["bar_tag"] = daily["bar_tag"].str.cat(pd.Series(break_pos, index=daily.index), sep="-")
    daily["pattern_3"] = (
        daily.groupby("code", sort=False)["bar_tag"].shift(2).astype("string")
        .str.cat(daily.groupby("code", sort=False)["bar_tag"].shift(1).astype("string"), sep=">")
        .str.cat(daily["bar_tag"].astype("string"), sep=">")
    )
    daily["day_pos_ma20"] = np.where(daily["c"] >= daily["ma20"], "above20", "below20")
    daily["day_pos_ma60"] = np.where(daily["c"] >= daily["ma60"], "above60", "below60")
    daily["vol_state"] = np.where(
        daily["vol20"].isna(),
        "na",
        np.where(daily["v"] >= daily["vol20"] * 1.5, "surge", np.where(daily["v"] <= daily["vol20"] * 0.7, "dry", "mid")),
    )
    return daily


def _build_stock_report(df: pd.DataFrame, target: dict[str, str]) -> str:
    code = target["code"]
    name = target["name"]
    start = pd.Timestamp(target["start"])
    end = pd.Timestamp(target["end"])
    window = df.loc[(df["code"] == code) & (df["dt"] >= start) & (df["dt"] <= end)].copy()
    if window.empty:
        return f"## {name} {code}\n\n該当データなし\n"
    monthly_summary = (
        window.groupby(["month", "monthly_label", "monthly_box_pos"], as_index=False)
        .agg(start=("dt", "min"), end=("dt", "max"), close=("c", "last"))
    )
    weekly_summary = (
        window.groupby(["week_end", "weekly_slope", "weekly_lower_high", "weekly_near_prev_low"], as_index=False)
        .agg(close=("c", "last"))
        .tail(8)
    )
    daily_table = window[
        [
            "dt",
            "c",
            "ma7",
            "ma20",
            "ma60",
            "day_pos_ma20",
            "day_pos_ma60",
            "cnt7_down",
            "cnt20_down",
            "vol_state",
            "bar_tag",
            "pattern_3",
            "monthly_label",
            "weekly_slope",
            "weekly_lower_high",
            "weekly_near_prev_low",
        ]
    ].tail(25)
    lines = [f"## {name} {code}", "", f"- 期間: `{target['start']}` to `{target['end']}`", ""]
    lines.append("### 月足環境")
    for _, row in monthly_summary.iterrows():
        lines.append(
            f"- {row['month']}: {row['monthly_label']} / box={row['monthly_box_pos']} / close={row['close']:.2f}"
        )
    lines.append("")
    lines.append("### 週足の流れ")
    for _, row in weekly_summary.iterrows():
        week_note = "support" if row["weekly_near_prev_low"] else ("lower-high" if row["weekly_lower_high"] else "neutral")
        lines.append(f"- {row['week_end'].date()}: slope={row['weekly_slope']} / {week_note} / close={row['close']:.2f}")
    lines.append("")
    lines.append("### 直近25営業日")
    lines.append("")
    lines.append("| date | close | ma7 | ma20 | ma60 | pos20 | pos60 | 7down | 20down | vol | bar | pattern3 | monthly | weekly |")
    lines.append("| --- | ---: | ---: | ---: | ---: | --- | --- | ---: | ---: | --- | --- | --- | --- | --- |")
    for _, row in daily_table.iterrows():
        pattern3 = row["pattern_3"] if pd.notna(row["pattern_3"]) else ""
        weekly = f"{row['weekly_slope']}/{'LH' if row['weekly_lower_high'] else '-'}{'SUP' if row['weekly_near_prev_low'] else ''}"
        lines.append(
            "| "
            + f"{row['dt'].date()} | {row['c']:.2f} | {row['ma7']:.2f} | {row['ma20']:.2f} | {row['ma60']:.2f} | "
            + f"{row['day_pos_ma20']} | {row['day_pos_ma60']} | {int(row['cnt7_down'])} | {int(row['cnt20_down'])} | "
            + f"{row['vol_state']} | {row['bar_tag']} | {pattern3} | {row['monthly_label']} | {weekly} |"
        )
    lines.append("")
    lines.append("### 観察ポイント")
    latest = daily_table.tail(5)
    for _, row in latest.iterrows():
        points: list[str] = []
        if row["day_pos_ma20"] == "below20" and row["day_pos_ma60"] == "above60":
            points.append("上位足は崩れていないが日足は20MA下")
        if int(row["cnt7_down"]) >= 3:
            points.append(f"7下が{int(row['cnt7_down'])}本")
        if int(row["cnt20_down"]) >= 3:
            points.append(f"20下が{int(row['cnt20_down'])}本")
        if row["vol_state"] == "surge":
            points.append("出来高急増")
        if not points:
            points.append("位置関係の変化は限定的")
        lines.append(f"- {row['dt'].date()}: {' / '.join(points)} / {row['bar_tag']}")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Detailed stock-by-stock MA/candle/regime report for note targets")
    parser.add_argument("--db-path", type=Path, action="append", default=None)
    parser.add_argument("--output", type=Path, default=Path("tmp/note_trade_stock_report.md"))
    args = parser.parse_args()

    db_paths = args.db_path or _resolve_default_db_paths()
    codes = [target["code"] for target in TARGETS]
    daily = _load_daily_frame(db_paths, codes)
    monthly = _build_monthly_context(daily)
    weekly = _build_weekly_context(daily)
    daily = daily.merge(monthly, how="left", left_on=["code", "month"], right_on=["code", "apply_month"])
    daily = daily.merge(weekly, how="left", on=["code", "week_end"])
    daily["monthly_label"] = daily["monthly_label"].fillna("other")
    daily["monthly_box_pos"] = daily["monthly_box_pos"].fillna("na")
    daily["weekly_slope"] = daily["weekly_slope"].fillna("na")
    daily["weekly_lower_high"] = daily["weekly_lower_high"].fillna(False).astype(bool)
    daily["weekly_near_prev_low"] = daily["weekly_near_prev_low"].fillna(False).astype(bool)
    daily = _tag_daily_bars(daily)

    parts = ["# note銘柄 個別調査", ""]
    for target in TARGETS:
        parts.append(_build_stock_report(daily, target))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text("\n".join(parts), encoding="utf-8")
    print(f"[ok] wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
