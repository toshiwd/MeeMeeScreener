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


DEFAULT_OUT_DIR = Path("G:/Tradex/monthly_box_upper_shelf_breakout_v1")


@dataclass(frozen=True)
class SignalRow:
    code: str
    name: str
    shelf_entry_date: str
    breakout_confirm_date: str | None
    box_start_month: str
    box_end_month: str
    shelf_month: str
    entry_close: float
    box_upper: float
    box_lower: float
    box_range_pct: float
    shelf_close_vs_box_upper_pct: float
    monthly_close: float
    monthly_ma20: float
    monthly_ma20_cross_recent: bool
    ma7: float
    ma20: float
    ma60: float
    ma100: float
    ma200: float
    ma_order_state: str
    consecutive_green_10d_after_breakout: int | None
    max_high_20d_pct: float | None
    max_high_60d_pct: float | None
    max_high_120d_pct: float | None
    min_low_20d_pct: float | None
    min_low_60d_pct: float | None
    return_20d_pct: float | None
    return_60d_pct: float | None
    return_120d_pct: float | None
    success_60d: bool
    severe_drawdown_60d: bool


def _epoch_to_iso_expr(column: str) -> str:
    return f"CAST(strftime(to_timestamp({column}), '%Y-%m-%d') AS VARCHAR)"


def _epoch_to_month_expr(column: str) -> str:
    return f"CAST(strftime(to_timestamp({column}), '%Y-%m') AS VARCHAR)"


def _load_daily(conn: duckdb.DuckDBPyConnection, *, start_year: int, end_date: str) -> pd.DataFrame:
    query = f"""
    SELECT
      b.code,
      COALESCE(im.name, sm.name, t.name, '') AS name,
      b.date AS date_epoch,
      {_epoch_to_iso_expr("b.date")} AS date,
      {_epoch_to_month_expr("b.date")} AS month_key,
      b.o, b.h, b.l, b.c, b.v,
      COALESCE(b.source, '') AS source,
      im.market_code
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


def _load_monthly(conn: duckdb.DuckDBPyConnection, *, start_year: int, end_date: str) -> pd.DataFrame:
    query = f"""
    SELECT
      mb.code AS code,
      COALESCE(im.name, sm.name, t.name, '') AS name,
      mb.month AS month_epoch,
      {_epoch_to_month_expr("mb.month")} AS month_key,
      mb.o, mb.h, mb.l, mb.c, mb.v,
      mm.ma20 AS monthly_ma20,
      LAG(mm.ma20) OVER (PARTITION BY mb.code ORDER BY mb.month) AS prev_monthly_ma20,
      LAG(mb.c) OVER (PARTITION BY mb.code ORDER BY mb.month) AS prev_monthly_close,
      im.market_code
    FROM monthly_bars mb
    LEFT JOIN monthly_ma mm ON mb.code = mm.code AND mb.month = mm.month
    LEFT JOIN industry_master im ON mb.code = im.code
    LEFT JOIN stock_meta sm ON mb.code = sm.code
    LEFT JOIN tickers t ON mb.code = t.code
    WHERE mb.o IS NOT NULL AND mb.h IS NOT NULL AND mb.l IS NOT NULL AND mb.c IS NOT NULL
      AND mb.o > 0 AND mb.h > 0 AND mb.l > 0 AND mb.c > 0
      AND CAST(strftime(to_timestamp(mb.month), '%Y') AS INTEGER) >= ?
      AND CAST(strftime(to_timestamp(mb.month), '%Y-%m-%d') AS VARCHAR) <= ?
      AND COALESCE(im.market_code, '') NOT LIKE '%ETF%'
      AND COALESCE(im.market_code, '') NOT LIKE '%REIT%'
    ORDER BY mb.code, mb.month
    """
    return conn.execute(query, [start_year - 3, end_date]).fetchdf()


def _add_daily_ma(daily: pd.DataFrame) -> pd.DataFrame:
    out = daily.copy()
    grouped = out.groupby("code", group_keys=False)
    for period in (7, 20, 60, 100, 200):
        out[f"ma{period}"] = grouped["c"].transform(lambda s, p=period: s.rolling(p, min_periods=p).mean())
    out["is_green"] = out["c"] > out["o"]
    return out


def _monthly_candidates(monthly: pd.DataFrame) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for code, rows in monthly.groupby("code", sort=False):
        rows = rows.reset_index(drop=True)
        for shelf_idx in range(28, len(rows)):
            shelf = rows.iloc[shelf_idx]
            if pd.isna(shelf["monthly_ma20"]) or pd.isna(shelf["prev_monthly_ma20"]):
                continue
            box = rows.iloc[shelf_idx - 8 : shelf_idx]
            if len(box) != 8:
                continue
            box_upper = float(box["h"].max())
            box_lower = float(box["l"].min())
            if box_lower <= 0:
                continue
            box_range_pct = (box_upper / box_lower - 1.0) * 100.0
            if box_range_pct > 42.0:
                continue
            close_span_pct = (float(box["c"].max()) / float(box["c"].min()) - 1.0) * 100.0
            if close_span_pct > 30.0:
                continue
            shelf_close = float(shelf["c"])
            shelf_low = float(shelf["l"])
            if not (box_upper * 0.94 <= shelf_close <= box_upper * 1.10):
                continue
            if shelf_low < box_upper * 0.88:
                continue
            monthly_close = float(shelf["c"])
            monthly_ma20 = float(shelf["monthly_ma20"])
            prev_close = float(shelf["prev_monthly_close"]) if not pd.isna(shelf["prev_monthly_close"]) else None
            prev_ma20 = float(shelf["prev_monthly_ma20"])
            if monthly_close < monthly_ma20:
                continue
            cross_recent = bool(prev_close is not None and prev_close <= prev_ma20 and monthly_close > monthly_ma20)
            candidates.append(
                {
                    "code": str(code),
                    "name": str(shelf["name"] or ""),
                    "shelf_month": str(shelf["month_key"]),
                    "box_start_month": str(box.iloc[0]["month_key"]),
                    "box_end_month": str(box.iloc[-1]["month_key"]),
                    "box_upper": box_upper,
                    "box_lower": box_lower,
                    "box_range_pct": round(box_range_pct, 4),
                    "shelf_close_vs_box_upper_pct": round((shelf_close / box_upper - 1.0) * 100.0, 4),
                    "monthly_close": monthly_close,
                    "monthly_ma20": monthly_ma20,
                    "monthly_ma20_cross_recent": cross_recent,
                }
            )
    return candidates


def _future_pct(rows: pd.DataFrame, idx: int, column: str, horizon: int, base: float) -> float | None:
    target = idx + horizon
    if target >= len(rows):
        return None
    value = rows.iloc[target][column]
    if pd.isna(value):
        return None
    return (float(value) / base - 1.0) * 100.0


def _max_consecutive_green(rows: pd.DataFrame, start_idx: int, window: int = 10) -> int:
    max_run = 0
    run = 0
    for value in rows.iloc[start_idx : start_idx + window]["is_green"].tolist():
        if bool(value):
            run += 1
            max_run = max(max_run, run)
        else:
            run = 0
    return max_run


def _build_signals(daily: pd.DataFrame, monthly_candidates: list[dict[str, Any]]) -> list[SignalRow]:
    by_code = {str(code): rows.reset_index(drop=True) for code, rows in daily.groupby("code", sort=False)}
    signals: list[SignalRow] = []
    for candidate in monthly_candidates:
        code = candidate["code"]
        rows = by_code.get(code)
        if rows is None or rows.empty:
            continue
        shelf_rows = rows[rows["month_key"] == candidate["shelf_month"]]
        if shelf_rows.empty:
            continue
        box_upper = float(candidate["box_upper"])
        entry_candidates = shelf_rows[
            (shelf_rows["c"] >= box_upper * 0.97)
            & (shelf_rows["c"] <= box_upper * 1.10)
            & (shelf_rows["ma20"] > shelf_rows["ma60"])
            & (shelf_rows["ma60"] >= shelf_rows["ma100"] * 0.95)
        ]
        if entry_candidates.empty:
            continue
        entry = entry_candidates.iloc[0]
        entry_idx = int(entry.name)
        if entry_idx < 220:
            continue
        ma_values = [entry[f"ma{period}"] for period in (7, 20, 60, 100, 200)]
        if any(pd.isna(value) for value in ma_values):
            continue
        ma_order = bool(entry["ma7"] > entry["ma20"] > entry["ma60"] > entry["ma100"] > entry["ma200"])
        near_order = bool(entry["ma7"] > entry["ma20"] > entry["ma60"] and entry["ma100"] >= entry["ma200"] * 0.92)
        if not (ma_order or near_order):
            continue
        future = rows.iloc[entry_idx + 1 : entry_idx + 121]
        if future.empty:
            continue
        breakout = future[
            (future["c"] >= box_upper * 1.05)
            & (future["ma7"] > future["ma20"])
            & (future["ma20"] > future["ma60"])
        ]
        breakout_date = None
        green_run = None
        if not breakout.empty:
            breakout_row = breakout.iloc[0]
            breakout_date = str(breakout_row["date"])
            breakout_idx = int(breakout_row.name)
            green_run = _max_consecutive_green(rows, breakout_idx, window=10)
            if green_run < 2:
                continue
        entry_close = float(entry["c"])
        max_high_20 = float(future.iloc[:20]["h"].max()) if len(future) >= 1 else None
        max_high_60 = float(future.iloc[:60]["h"].max()) if len(future) >= 1 else None
        max_high_120 = float(future["h"].max()) if len(future) >= 1 else None
        min_low_20 = float(future.iloc[:20]["l"].min()) if len(future) >= 1 else None
        min_low_60 = float(future.iloc[:60]["l"].min()) if len(future) >= 1 else None
        return_20 = _future_pct(rows, entry_idx, "c", 20, entry_close)
        return_60 = _future_pct(rows, entry_idx, "c", 60, entry_close)
        return_120 = _future_pct(rows, entry_idx, "c", 120, entry_close)
        max_high_60_pct = None if max_high_60 is None else (max_high_60 / entry_close - 1.0) * 100.0
        min_low_60_pct = None if min_low_60 is None else (min_low_60 / entry_close - 1.0) * 100.0
        signals.append(
            SignalRow(
                code=code,
                name=str(candidate["name"]),
                shelf_entry_date=str(entry["date"]),
                breakout_confirm_date=breakout_date,
                box_start_month=str(candidate["box_start_month"]),
                box_end_month=str(candidate["box_end_month"]),
                shelf_month=str(candidate["shelf_month"]),
                entry_close=entry_close,
                box_upper=round(box_upper, 4),
                box_lower=round(float(candidate["box_lower"]), 4),
                box_range_pct=float(candidate["box_range_pct"]),
                shelf_close_vs_box_upper_pct=float(candidate["shelf_close_vs_box_upper_pct"]),
                monthly_close=float(candidate["monthly_close"]),
                monthly_ma20=float(candidate["monthly_ma20"]),
                monthly_ma20_cross_recent=bool(candidate["monthly_ma20_cross_recent"]),
                ma7=float(entry["ma7"]),
                ma20=float(entry["ma20"]),
                ma60=float(entry["ma60"]),
                ma100=float(entry["ma100"]),
                ma200=float(entry["ma200"]),
                ma_order_state="strict_pampaka" if ma_order else "near_pampaka",
                consecutive_green_10d_after_breakout=green_run,
                max_high_20d_pct=None if max_high_20 is None else round((max_high_20 / entry_close - 1.0) * 100.0, 4),
                max_high_60d_pct=None if max_high_60 is None else round(max_high_60_pct, 4),
                max_high_120d_pct=None if max_high_120 is None else round((max_high_120 / entry_close - 1.0) * 100.0, 4),
                min_low_20d_pct=None if min_low_20 is None else round((min_low_20 / entry_close - 1.0) * 100.0, 4),
                min_low_60d_pct=None if min_low_60 is None else round(min_low_60_pct, 4),
                return_20d_pct=None if return_20 is None else round(return_20, 4),
                return_60d_pct=None if return_60 is None else round(return_60, 4),
                return_120d_pct=None if return_120 is None else round(return_120, 4),
                success_60d=bool(max_high_60_pct is not None and max_high_60_pct >= 12.0 and (min_low_60_pct is None or min_low_60_pct > -10.0)),
                severe_drawdown_60d=bool(min_low_60_pct is not None and min_low_60_pct <= -10.0),
            )
        )
    return signals


def _summarize(signals: list[SignalRow]) -> dict[str, Any]:
    df = pd.DataFrame([asdict(signal) for signal in signals])
    if df.empty:
        return {"signal_count": 0, "judgment": "hold", "reason": "no_matching_signals"}
    complete = df[df["return_60d_pct"].notna()].copy()
    if complete.empty:
        return {"signal_count": int(len(df)), "complete_60d_count": 0, "judgment": "hold", "reason": "no_complete_forward_window"}
    success_rate = float(complete["success_60d"].mean())
    severe_rate = float(complete["severe_drawdown_60d"].mean())
    median_60 = float(complete["return_60d_pct"].median())
    win_20 = float((complete["return_20d_pct"] > 0).mean())
    win_60 = float((complete["return_60d_pct"] > 0).mean())
    win_120 = float((complete["return_120d_pct"] > 0).mean()) if complete["return_120d_pct"].notna().any() else None

    def _slice_stats(slice_df: pd.DataFrame) -> dict[str, Any]:
        if slice_df.empty:
            return {"count": 0}
        return {
            "count": int(len(slice_df)),
            "win_60d_rate": float((slice_df["return_60d_pct"] > 0).mean()),
            "success_60d_rate": float(slice_df["success_60d"].mean()),
            "severe_drawdown_60d_rate": float(slice_df["severe_drawdown_60d"].mean()),
            "median_return_60d_pct": float(slice_df["return_60d_pct"].median()),
            "median_max_high_60d_pct": float(slice_df["max_high_60d_pct"].median()),
        }

    breakout_confirmed = complete[complete["breakout_confirm_date"].notna()]
    strict_pampaka = complete[complete["ma_order_state"] == "strict_pampaka"]
    strict_breakout = complete[(complete["ma_order_state"] == "strict_pampaka") & complete["breakout_confirm_date"].notna()]

    if success_rate >= 0.52 and median_60 > 3.0 and severe_rate <= 0.28:
        judgment = "keep"
        reason = "success_rate_median_return_and_drawdown_pass"
    elif win_60 >= 0.58 and median_60 > 1.0 and severe_rate <= 0.25:
        judgment = "hold"
        reason = "positive_win_rate_but_success_threshold_not_met"
    elif success_rate < 0.38 or median_60 <= 0:
        judgment = "drop"
        reason = "success_rate_or_median_return_fail"
    else:
        judgment = "hold"
        reason = "mixed_or_needs_filter"
    return {
        "signal_count": int(len(df)),
        "complete_60d_count": int(len(complete)),
        "unique_symbol_count": int(df["code"].nunique()),
        "breakout_confirmed_rate": float(complete["breakout_confirm_date"].notna().mean()),
        "strict_pampaka_rate": float((complete["ma_order_state"] == "strict_pampaka").mean()),
        "success_60d_rate": success_rate,
        "win_20d_rate": win_20,
        "win_60d_rate": win_60,
        "win_120d_rate": win_120,
        "severe_drawdown_60d_rate": severe_rate,
        "median_return_20d_pct": float(complete["return_20d_pct"].median()),
        "median_return_60d_pct": median_60,
        "median_return_120d_pct": float(complete["return_120d_pct"].median()) if complete["return_120d_pct"].notna().any() else None,
        "mean_return_60d_pct": float(complete["return_60d_pct"].mean()),
        "median_max_high_60d_pct": float(complete["max_high_60d_pct"].median()),
        "slices": {
            "breakout_confirmed": _slice_stats(breakout_confirmed),
            "strict_pampaka": _slice_stats(strict_pampaka),
            "strict_pampaka_and_breakout_confirmed": _slice_stats(strict_breakout),
        },
        "judgment": judgment,
        "reason": reason,
    }


def _write_outputs(out_dir: Path, payload: dict[str, Any], signals: list[SignalRow]) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    rows = [asdict(signal) for signal in signals]
    (out_dir / "compare.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    with (out_dir / "signals.jsonl").open("w", encoding="utf-8", newline="\n") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    if rows:
        with (out_dir / "signals.csv").open("w", encoding="utf-8-sig", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
    else:
        (out_dir / "signals.csv").write_text("", encoding="utf-8-sig")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--start-year", type=int, default=2016)
    parser.add_argument("--end-date", default="2026-05-19")
    args = parser.parse_args()

    runtime_status = get_runtime_stock_db_status()
    rankings_freshness = get_rankings_freshness(tf="D", which="latest", direction="up", mode="trade", risk_mode="balanced", limit=20)
    db_path = resolve_runtime_stock_db_path()
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        daily = _add_daily_ma(_load_daily(conn, start_year=args.start_year, end_date=args.end_date))
        monthly = _load_monthly(conn, start_year=args.start_year, end_date=args.end_date)
    finally:
        conn.close()
    monthly_candidates = _monthly_candidates(monthly)
    signals = _build_signals(daily, monthly_candidates)
    summary = _summarize(signals)
    payload: dict[str, Any] = {
        "artifact_name": "monthly_box_upper_shelf_breakout_v1",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_db_path": str(db_path),
        "runtime_stock_db_status": runtime_status,
        "rankings_freshness": rankings_freshness,
        "fixed_evaluation_conditions": {
            "scope": "TRADEX-only historical shape validation",
            "start_year": args.start_year,
            "end_date": args.end_date,
            "confirmed_only": True,
            "universe": "runtime_stock_db daily_bars/monthly_bars excluding ETF/REIT market labels",
            "signal": "8-month monthly box, shelf month at box upper, monthly close above MA20, daily MA near/strict pampaka, and later box-upper breakout with >=2 consecutive green days when available",
            "entry": "first shelf-month daily close within 97%-110% of box upper with daily MA20 above MA60",
            "success": "60d max high >= +12% and 60d min low > -10%",
            "cost_slippage": "not_applied",
            "silent_fallback_used": False,
        },
        "current_champion": "none",
        "current_challenger": "monthly_box_upper_shelf_breakout_v1",
        "monthly_candidate_count": len(monthly_candidates),
        "summary": summary,
        "sample_signals_top_recent": sorted([asdict(signal) for signal in signals], key=lambda row: row["shelf_entry_date"], reverse=True)[:20],
    }
    _write_outputs(args.out_dir, payload, signals)
    print(json.dumps({"out_dir": str(args.out_dir), "summary": summary}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
