from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.services.codex_bridge_service import get_rankings_freshness, get_runtime_stock_db_status
from shared.runtime_stock_db_contract import resolve_runtime_stock_db_path


SOURCE_SIGNALS = Path("G:/Tradex/monthly_box_upper_shelf_breakout_v1/signals.csv")
DEFAULT_OUT_DIR = Path("G:/Tradex/box_breakout_post_candle_entry_timing_v1")


@dataclass(frozen=True)
class EntryRow:
    code: str
    name: str
    shelf_entry_date: str
    breakout_confirm_date: str
    entry_rule: str
    entry_date: str
    entry_close: float
    breakout_close: float
    entry_vs_breakout_close_pct: float
    candle_body_ratio: float
    close_pos_in_range: float
    pullback_from_breakout_high_pct: float
    ma7: float
    ma20: float
    ma60: float
    close_vs_ma7_pct: float
    close_vs_ma20_pct: float
    max_high_20d_pct: float | None
    max_high_60d_pct: float | None
    min_low_20d_pct: float | None
    min_low_60d_pct: float | None
    return_20d_pct: float | None
    return_60d_pct: float | None
    win_60d: bool
    success_60d: bool
    severe_drawdown_60d: bool


def _epoch_to_iso_expr(column: str) -> str:
    return f"CAST(strftime(to_timestamp({column}), '%Y-%m-%d') AS VARCHAR)"


def _load_daily(conn: duckdb.DuckDBPyConnection, *, start_year: int, end_date: str) -> pd.DataFrame:
    query = f"""
    SELECT
      b.code,
      COALESCE(im.name, sm.name, t.name, '') AS name,
      b.date AS date_epoch,
      {_epoch_to_iso_expr("b.date")} AS date,
      b.o, b.h, b.l, b.c, b.v,
      COALESCE(b.source, '') AS source
    FROM daily_bars b
    LEFT JOIN industry_master im ON b.code = im.code
    LEFT JOIN stock_meta sm ON b.code = sm.code
    LEFT JOIN tickers t ON b.code = t.code
    WHERE b.o IS NOT NULL AND b.h IS NOT NULL AND b.l IS NOT NULL AND b.c IS NOT NULL
      AND b.o > 0 AND b.h > 0 AND b.l > 0 AND b.c > 0
      AND COALESCE(b.source, '') <> 'yahoo'
      AND CAST(strftime(to_timestamp(b.date), '%Y') AS INTEGER) >= ?
      AND {_epoch_to_iso_expr("b.date")} <= ?
      AND COALESCE(im.market_code, '') NOT LIKE '%ETF%'
      AND COALESCE(im.market_code, '') NOT LIKE '%REIT%'
    ORDER BY b.code, b.date
    """
    return conn.execute(query, [start_year, end_date]).fetchdf()


def _add_features(daily: pd.DataFrame) -> pd.DataFrame:
    out = daily.copy()
    grouped = out.groupby("code", group_keys=False)
    for period in (7, 20, 60):
        out[f"ma{period}"] = grouped["c"].transform(lambda s, p=period: s.rolling(p, min_periods=p).mean())
    rng = (out["h"] - out["l"]).replace(0, float("nan"))
    out["body_ratio"] = ((out["c"] - out["o"]).abs() / rng).astype(float)
    out["close_pos_in_range"] = ((out["c"] - out["l"]) / rng).astype(float)
    out["is_green"] = out["c"] > out["o"]
    return out


def _future_pct(rows: pd.DataFrame, idx: int, column: str, horizon: int, base: float) -> float | None:
    target = idx + horizon
    if target >= len(rows):
        return None
    value = rows.iloc[target][column]
    if pd.isna(value):
        return None
    return (float(value) / base - 1.0) * 100.0


def _row_for_rule(rows: pd.DataFrame, breakout_idx: int, rule: str) -> pd.Series | None:
    breakout = rows.iloc[breakout_idx]
    window = rows.iloc[breakout_idx : breakout_idx + 11].copy()
    if window.empty:
        return None
    breakout_close = float(breakout["c"])
    breakout_high = float(breakout["h"])
    if rule == "breakout_close":
        return breakout
    if rule == "next_green_close_above_breakout_close":
        cand = window.iloc[1:]
        cand = cand[(cand["is_green"]) & (cand["c"] >= breakout_close)]
        return None if cand.empty else cand.iloc[0]
    if rule == "first_red_or_small_body_hold_above_ma7":
        cand = window.iloc[1:]
        cand = cand[
            ((~cand["is_green"]) | (cand["body_ratio"] <= 0.35))
            & (cand["c"] >= cand["ma7"])
            & (cand["c"] >= breakout_close * 0.97)
        ]
        return None if cand.empty else cand.iloc[0]
    if rule == "pullback_2to6pct_hold_above_ma20":
        cand = window.iloc[1:]
        pullback = (cand["c"] / breakout_high - 1.0) * 100.0
        cand = cand[(pullback <= -2.0) & (pullback >= -6.0) & (cand["c"] >= cand["ma20"])]
        return None if cand.empty else cand.iloc[0]
    if rule == "ma7_retest_hold":
        cand = window.iloc[1:]
        near_ma7 = (cand["c"] / cand["ma7"] - 1.0).abs() * 100.0
        cand = cand[(near_ma7 <= 2.0) & (cand["c"] >= cand["ma20"])]
        return None if cand.empty else cand.iloc[0]
    raise ValueError(f"unknown rule: {rule}")


def _build_rows(daily: pd.DataFrame, source: pd.DataFrame) -> list[EntryRow]:
    rules = [
        "breakout_close",
        "next_green_close_above_breakout_close",
        "first_red_or_small_body_hold_above_ma7",
        "pullback_2to6pct_hold_above_ma20",
        "ma7_retest_hold",
    ]
    by_code = {str(code): rows.reset_index(drop=True) for code, rows in daily.groupby("code", sort=False)}
    out: list[EntryRow] = []
    base = source[source["breakout_confirm_date"].notna()].copy()
    for _, signal in base.iterrows():
        code = str(signal["code"])
        rows = by_code.get(code)
        if rows is None:
            continue
        matches = rows.index[rows["date"].eq(str(signal["breakout_confirm_date"]))].tolist()
        if not matches:
            continue
        breakout_idx = int(matches[0])
        breakout = rows.iloc[breakout_idx]
        breakout_close = float(breakout["c"])
        breakout_high = float(breakout["h"])
        for rule in rules:
            entry = _row_for_rule(rows, breakout_idx, rule)
            if entry is None:
                continue
            entry_idx = int(entry.name)
            entry_close = float(entry["c"])
            future = rows.iloc[entry_idx + 1 : entry_idx + 61]
            if future.empty:
                continue
            max_high_20 = float(future.iloc[:20]["h"].max())
            max_high_60 = float(future["h"].max())
            min_low_20 = float(future.iloc[:20]["l"].min())
            min_low_60 = float(future["l"].min())
            ret20 = _future_pct(rows, entry_idx, "c", 20, entry_close)
            ret60 = _future_pct(rows, entry_idx, "c", 60, entry_close)
            max60 = (max_high_60 / entry_close - 1.0) * 100.0
            min60 = (min_low_60 / entry_close - 1.0) * 100.0
            out.append(
                EntryRow(
                    code=code,
                    name=str(signal["name"]),
                    shelf_entry_date=str(signal["shelf_entry_date"]),
                    breakout_confirm_date=str(signal["breakout_confirm_date"]),
                    entry_rule=rule,
                    entry_date=str(entry["date"]),
                    entry_close=entry_close,
                    breakout_close=breakout_close,
                    entry_vs_breakout_close_pct=round((entry_close / breakout_close - 1.0) * 100.0, 4),
                    candle_body_ratio=round(float(entry["body_ratio"]), 4),
                    close_pos_in_range=round(float(entry["close_pos_in_range"]), 4),
                    pullback_from_breakout_high_pct=round((entry_close / breakout_high - 1.0) * 100.0, 4),
                    ma7=float(entry["ma7"]),
                    ma20=float(entry["ma20"]),
                    ma60=float(entry["ma60"]),
                    close_vs_ma7_pct=round((entry_close / float(entry["ma7"]) - 1.0) * 100.0, 4),
                    close_vs_ma20_pct=round((entry_close / float(entry["ma20"]) - 1.0) * 100.0, 4),
                    max_high_20d_pct=round((max_high_20 / entry_close - 1.0) * 100.0, 4),
                    max_high_60d_pct=round(max60, 4),
                    min_low_20d_pct=round((min_low_20 / entry_close - 1.0) * 100.0, 4),
                    min_low_60d_pct=round(min60, 4),
                    return_20d_pct=None if ret20 is None else round(ret20, 4),
                    return_60d_pct=None if ret60 is None else round(ret60, 4),
                    win_60d=bool(ret60 is not None and ret60 > 0),
                    success_60d=bool(max60 >= 8.0 and min60 > -8.0),
                    severe_drawdown_60d=bool(min60 <= -8.0),
                )
            )
    return out


def _summarize(rows: list[EntryRow]) -> dict[str, Any]:
    df = pd.DataFrame([asdict(row) for row in rows])
    if df.empty:
        return {"signal_count": 0, "judgment": "hold", "reason": "no_entry_rows"}
    summaries: dict[str, Any] = {}
    for rule, group in df.groupby("entry_rule"):
        complete = group[group["return_60d_pct"].notna()]
        if complete.empty:
            summaries[str(rule)] = {"count": int(len(group)), "complete_60d_count": 0}
            continue
        summaries[str(rule)] = {
            "count": int(len(group)),
            "complete_60d_count": int(len(complete)),
            "win_20d_rate": float((complete["return_20d_pct"] > 0).mean()),
            "win_60d_rate": float(complete["win_60d"].mean()),
            "success_60d_rate": float(complete["success_60d"].mean()),
            "severe_drawdown_60d_rate": float(complete["severe_drawdown_60d"].mean()),
            "median_return_20d_pct": float(complete["return_20d_pct"].median()),
            "median_return_60d_pct": float(complete["return_60d_pct"].median()),
            "median_max_high_60d_pct": float(complete["max_high_60d_pct"].median()),
            "median_min_low_60d_pct": float(complete["min_low_60d_pct"].median()),
            "median_entry_vs_breakout_close_pct": float(complete["entry_vs_breakout_close_pct"].median()),
        }
    ranked = sorted(
        summaries.items(),
        key=lambda item: (
            item[1].get("win_60d_rate", 0),
            item[1].get("median_return_60d_pct", -999),
            -item[1].get("severe_drawdown_60d_rate", 1),
        ),
        reverse=True,
    )
    best_rule = ranked[0][0] if ranked else None
    best = summaries.get(best_rule or "", {})
    if best.get("win_60d_rate", 0) >= 0.70 and best.get("median_return_60d_pct", 0) >= 4 and best.get("severe_drawdown_60d_rate", 1) <= 0.16:
        judgment = "keep"
        reason = "post_breakout_entry_rule_passes_gate"
    elif best.get("win_60d_rate", 0) >= 0.62 and best.get("median_return_60d_pct", 0) > 2:
        judgment = "hold"
        reason = "best_rule_positive_but_not_teppan_gate"
    else:
        judgment = "drop"
        reason = "no_post_breakout_rule_passes_basic_gate"
    return {
        "entry_rule_summaries": summaries,
        "best_entry_rule": best_rule,
        "judgment": judgment,
        "reason": reason,
    }


def _write(out_dir: Path, payload: dict[str, Any], rows: list[EntryRow]) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "post_breakout_entry_compare.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    dict_rows = [asdict(row) for row in rows]
    with (out_dir / "post_breakout_entry_signals.jsonl").open("w", encoding="utf-8", newline="\n") as fh:
        for row in dict_rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    if dict_rows:
        with (out_dir / "post_breakout_entry_signals.csv").open("w", encoding="utf-8-sig", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(dict_rows[0].keys()))
            writer.writeheader()
            writer.writerows(dict_rows)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--start-year", type=int, default=2016)
    parser.add_argument("--end-date", default="2026-05-19")
    args = parser.parse_args()

    runtime_status = get_runtime_stock_db_status()
    rankings_freshness = get_rankings_freshness(tf="D", which="latest", direction="up", mode="trade", risk_mode="balanced", limit=20)
    source = pd.read_csv(SOURCE_SIGNALS)
    db_path = resolve_runtime_stock_db_path()
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        daily = _add_features(_load_daily(conn, start_year=args.start_year, end_date=args.end_date))
    finally:
        conn.close()
    rows = _build_rows(daily, source)
    summary = _summarize(rows)
    payload: dict[str, Any] = {
        "artifact_name": "box_breakout_post_candle_entry_timing_v1",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_db_path": str(db_path),
        "authoritative_input": str(SOURCE_SIGNALS),
        "runtime_stock_db_status": runtime_status,
        "rankings_freshness": rankings_freshness,
        "fixed_evaluation_conditions": {
            "scope": "TRADEX-only post-breakout candle entry timing validation",
            "parent_axis": "monthly_box_upper_shelf_breakout_confirmed",
            "entry_rules": [
                "breakout_close",
                "next_green_close_above_breakout_close",
                "first_red_or_small_body_hold_above_ma7",
                "pullback_2to6pct_hold_above_ma20",
                "ma7_retest_hold",
            ],
            "success": "60d max high >= +8% and 60d min low > -8%",
            "silent_fallback_used": False,
            "meemee_reflectable": False,
        },
        "summary": summary,
        "sample_recent_rows": sorted([asdict(row) for row in rows], key=lambda item: item["entry_date"], reverse=True)[:30],
    }
    _write(args.out_dir, payload, rows)
    print(json.dumps({"out_dir": str(args.out_dir), "summary": summary}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
