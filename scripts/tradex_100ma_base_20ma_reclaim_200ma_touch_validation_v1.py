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


DEFAULT_OUT_DIR = Path("G:/Tradex/100ma_base_20ma_reclaim_200ma_touch_v1")


@dataclass(frozen=True)
class SignalRow:
    code: str
    name: str
    signal_date: str
    probe_date: str | None
    full_entry_date: str
    probe_entry_close: float | None
    full_entry_open: float
    full_entry_close: float
    ma20: float
    ma100: float
    ma200: float
    monthly_close: float | None
    monthly_ma20: float | None
    below_ma100_count_100: int
    ma100_slope_20d_pct: float
    gap_up_pct: float
    recent_stop_date: str | None
    recent_stop_body_ratio: float | None
    target_ma200_pct: float
    max_high_30d_pct: float | None
    max_high_60d_pct: float | None
    min_low_20d_pct: float | None
    touched_ma200_30d: bool
    touched_ma200_60d: bool
    return_10d_pct: float | None
    return_20d_pct: float | None
    return_30d_pct: float | None
    return_60d_pct: float | None
    source_latest: str


def _epoch_to_iso_expr(column_name: str) -> str:
    return f"CAST(strftime(to_timestamp({column_name}), '%Y-%m-%d') AS VARCHAR)"


def _load_bars(conn: duckdb.DuckDBPyConnection, *, start_year: int, end_date: str, confirmed_only: bool) -> pd.DataFrame:
    source_filter = "AND COALESCE(source, '') <> 'yahoo'" if confirmed_only else ""
    query = f"""
    SELECT
        b.code,
        COALESCE(im.name, sm.name, t.name, '') AS name,
        b.date AS date_epoch,
        {_epoch_to_iso_expr("b.date")} AS date,
        b.o, b.h, b.l, b.c, b.v,
        COALESCE(b.source, '') AS source,
        im.market_code
    FROM daily_bars b
    LEFT JOIN industry_master im USING (code)
    LEFT JOIN stock_meta sm USING (code)
    LEFT JOIN tickers t USING (code)
    WHERE b.o IS NOT NULL AND b.h IS NOT NULL AND b.l IS NOT NULL AND b.c IS NOT NULL
      AND b.o > 0 AND b.h > 0 AND b.l > 0 AND b.c > 0
      AND CAST(strftime(to_timestamp(b.date), '%Y') AS INTEGER) >= ?
      AND {_epoch_to_iso_expr("b.date")} <= ?
      AND COALESCE(im.market_code, '') NOT LIKE '%ETF%'
      AND COALESCE(im.market_code, '') NOT LIKE '%REIT%'
      {source_filter}
    ORDER BY b.code, b.date
    """
    return conn.execute(query, [start_year, end_date]).fetchdf()


def _load_monthly(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    query = f"""
    SELECT
        mb.code,
        mb.month AS month_epoch,
        CAST(strftime(to_timestamp(mb.month), '%Y-%m') AS VARCHAR) AS month_key,
        mb.c AS monthly_close,
        mm.ma20 AS monthly_ma20
    FROM monthly_bars mb
    LEFT JOIN monthly_ma mm
      ON mb.code = mm.code AND mb.month = mm.month
    WHERE mb.c IS NOT NULL AND mb.c > 0
    ORDER BY mb.code, mb.month
    """
    return conn.execute(query).fetchdf()


def _add_daily_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    grouped = out.groupby("code", group_keys=False)
    out["ma20"] = grouped["c"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    out["ma100"] = grouped["c"].transform(lambda s: s.rolling(100, min_periods=100).mean())
    out["ma200"] = grouped["c"].transform(lambda s: s.rolling(200, min_periods=200).mean())
    out["prev_c"] = grouped["c"].shift(1)
    out["prev_h"] = grouped["h"].shift(1)
    out["prev_ma20"] = grouped["ma20"].shift(1)
    out["prev_ma100_20"] = grouped["ma100"].shift(20)
    out["below_ma100"] = (out["c"] < out["ma100"]).astype("float")
    out["below_ma100_count_100"] = grouped["below_ma100"].transform(lambda s: s.rolling(100, min_periods=100).sum())
    candle_range = (out["h"] - out["l"]).where((out["h"] - out["l"]) > 0)
    out["body_ratio"] = (out["c"] - out["o"]).abs() / candle_range
    out["rolling_low_20"] = grouped["l"].transform(lambda s: s.rolling(20, min_periods=20).min())
    return out


def _attach_monthly_context(daily: pd.DataFrame, monthly: pd.DataFrame) -> pd.DataFrame:
    d = daily.copy()
    d["month_key"] = pd.to_datetime(d["date"]).dt.to_period("M").astype(str)
    month_map = monthly.set_index(["code", "month_key"])[["monthly_close", "monthly_ma20"]]
    joined = d.join(month_map, on=["code", "month_key"])
    return joined


def _future_value(rows: pd.DataFrame, idx: int, column: str, horizon: int) -> float | None:
    target = idx + horizon
    if target >= len(rows):
        return None
    value = rows.iloc[target][column]
    if pd.isna(value):
        return None
    return float(value)


def _recent_stop(rows: pd.DataFrame, idx: int, lookback: int = 20) -> tuple[str | None, float | None]:
    start = max(0, idx - lookback)
    window = rows.iloc[start:idx]
    if window.empty:
        return None, None
    candidates = window[
        (window["body_ratio"] <= 0.28)
        & (window["l"] <= window["rolling_low_20"] * 1.03)
        & (window["c"] >= window["l"] * 1.01)
    ]
    if candidates.empty:
        return None, None
    row = candidates.iloc[-1]
    return str(row["date"]), float(row["body_ratio"])


def _probe_date(rows: pd.DataFrame, idx: int, lookback: int = 20) -> tuple[str | None, float | None]:
    start = max(0, idx - lookback)
    window = rows.iloc[start:idx]
    candidates = window[(window["c"] > window["ma20"]) & (window["prev_c"] <= window["prev_ma20"])]
    if candidates.empty:
        return None, None
    row = candidates.iloc[0]
    return str(row["date"]), float(row["c"])


def _probe_row(rows: pd.DataFrame, idx: int, lookback: int = 25) -> pd.Series | None:
    start = max(0, idx - lookback)
    window = rows.iloc[start:idx]
    candidates = window[
        (window["c"] > window["ma20"])
        & (window["prev_c"] <= window["prev_ma20"])
        & (window["c"] < window["ma100"])
        & (window["ma20"] < window["ma100"])
    ]
    if candidates.empty:
        return None
    return candidates.iloc[0]


def _build_signals(df: pd.DataFrame) -> list[SignalRow]:
    signals: list[SignalRow] = []
    for code, rows in df.groupby("code", sort=False):
        rows = rows.reset_index(drop=True)
        for idx, row in rows.iterrows():
            if idx < 220:
                continue
            required = ["ma20", "ma100", "ma200", "prev_c", "prev_ma20", "prev_h", "monthly_close", "monthly_ma20"]
            if any(pd.isna(row[col]) for col in required):
                continue
            if not (row["c"] <= row["ma100"] * 1.05 and row["ma20"] < row["ma100"]):
                continue
            if not (row["ma100"] < row["ma200"] and row["c"] < row["ma200"]):
                continue
            if not (row["monthly_close"] < row["monthly_ma20"]):
                continue
            below_count = int(row["below_ma100_count_100"])
            if below_count < 80:
                continue
            ma100_slope_20d_pct = (float(row["ma100"]) / float(row["prev_ma100_20"]) - 1.0) * 100.0
            if ma100_slope_20d_pct > 2.0:
                continue
            gap_up_pct = (float(row["o"]) / float(row["prev_h"]) - 1.0) * 100.0
            if gap_up_pct < 1.0:
                continue
            probe = _probe_row(rows, idx, lookback=25)
            if probe is None:
                continue
            stop_date, stop_body = _recent_stop(rows, idx, lookback=20)
            if stop_date is None:
                continue
            probe_date, probe_close = str(probe["date"]), float(probe["c"])
            future = rows.iloc[idx + 1 : idx + 61]
            max_high_30 = float(future.iloc[:30]["h"].max()) if len(future) >= 1 else None
            max_high_60 = float(future["h"].max()) if len(future) >= 1 else None
            min_low_20 = float(future.iloc[:20]["l"].min()) if len(future) >= 1 else None
            entry_close = float(row["c"])
            ret = lambda days: None if (value := _future_value(rows, idx, "c", days)) is None else (value / entry_close - 1.0) * 100.0
            target_ma200_pct = (float(row["ma200"]) / entry_close - 1.0) * 100.0
            signals.append(
                SignalRow(
                    code=str(code),
                    name=str(row["name"] or ""),
                    signal_date=str(row["date"]),
                    probe_date=probe_date,
                    full_entry_date=str(row["date"]),
                    probe_entry_close=probe_close,
                    full_entry_open=float(row["o"]),
                    full_entry_close=entry_close,
                    ma20=float(row["ma20"]),
                    ma100=float(row["ma100"]),
                    ma200=float(row["ma200"]),
                    monthly_close=float(row["monthly_close"]) if not pd.isna(row["monthly_close"]) else None,
                    monthly_ma20=float(row["monthly_ma20"]) if not pd.isna(row["monthly_ma20"]) else None,
                    below_ma100_count_100=below_count,
                    ma100_slope_20d_pct=round(ma100_slope_20d_pct, 4),
                    gap_up_pct=round(gap_up_pct, 4),
                    recent_stop_date=stop_date,
                    recent_stop_body_ratio=round(float(stop_body), 4) if stop_body is not None else None,
                    target_ma200_pct=round(target_ma200_pct, 4),
                    max_high_30d_pct=None if max_high_30 is None else round((max_high_30 / entry_close - 1.0) * 100.0, 4),
                    max_high_60d_pct=None if max_high_60 is None else round((max_high_60 / entry_close - 1.0) * 100.0, 4),
                    min_low_20d_pct=None if min_low_20 is None else round((min_low_20 / entry_close - 1.0) * 100.0, 4),
                    touched_ma200_30d=bool(max_high_30 is not None and max_high_30 >= float(row["ma200"])),
                    touched_ma200_60d=bool(max_high_60 is not None and max_high_60 >= float(row["ma200"])),
                    return_10d_pct=None if ret(10) is None else round(float(ret(10)), 4),
                    return_20d_pct=None if ret(20) is None else round(float(ret(20)), 4),
                    return_30d_pct=None if ret(30) is None else round(float(ret(30)), 4),
                    return_60d_pct=None if ret(60) is None else round(float(ret(60)), 4),
                    source_latest=str(row["source"] or ""),
                )
            )
    return signals


def _summarize(signals: list[SignalRow]) -> dict[str, Any]:
    rows = [asdict(signal) for signal in signals]
    df = pd.DataFrame(rows)
    if df.empty:
        return {
            "signal_count": 0,
            "judgment": "hold",
            "reason": "no_matching_signals_under_fixed_shape_contract",
        }
    complete_60 = df[df["return_60d_pct"].notna()].copy()
    touched_60_rate = float(complete_60["touched_ma200_60d"].mean()) if not complete_60.empty else None
    touched_30_rate = float(complete_60["touched_ma200_30d"].mean()) if not complete_60.empty else None
    median_20 = float(complete_60["return_20d_pct"].median()) if not complete_60.empty else None
    median_60 = float(complete_60["return_60d_pct"].median()) if not complete_60.empty else None
    severe_drawdown_rate = float((complete_60["min_low_20d_pct"] <= -8.0).mean()) if not complete_60.empty else None
    if touched_60_rate is not None and touched_60_rate >= 0.45 and (median_20 or 0) > 0 and (severe_drawdown_rate or 1) <= 0.35:
        judgment = "keep"
        reason = "ma200_touch_rate_and_median_forward_return_positive"
    elif touched_60_rate is not None and touched_60_rate < 0.25:
        judgment = "drop"
        reason = "ma200_touch_rate_too_low"
    else:
        judgment = "hold"
        reason = "mixed_or_sample_limited"
    return {
            "signal_count": int(len(df)),
        "complete_60d_count": int(len(complete_60)),
        "unique_symbol_count": int(df["code"].nunique()),
        "touched_ma200_30d_rate": touched_30_rate,
        "touched_ma200_60d_rate": touched_60_rate,
        "median_return_20d_pct": median_20,
        "median_return_60d_pct": median_60,
        "mean_return_20d_pct": float(complete_60["return_20d_pct"].mean()) if not complete_60.empty else None,
        "mean_return_60d_pct": float(complete_60["return_60d_pct"].mean()) if not complete_60.empty else None,
        "severe_drawdown_20d_rate_le_minus8pct": severe_drawdown_rate,
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
    parser.add_argument("--end-date", default="2026-05-18")
    parser.add_argument("--include-yahoo", action="store_true")
    args = parser.parse_args()

    runtime_status = get_runtime_stock_db_status()
    rankings_freshness = get_rankings_freshness(tf="D", which="latest", direction="up", mode="trade", risk_mode="balanced", limit=20)
    db_path = resolve_runtime_stock_db_path()
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        daily = _load_bars(conn, start_year=args.start_year, end_date=args.end_date, confirmed_only=not args.include_yahoo)
        monthly = _load_monthly(conn)
    finally:
        conn.close()
    featured = _attach_monthly_context(_add_daily_features(daily), monthly)
    signals = _build_signals(featured)
    summary = _summarize(signals)
    payload: dict[str, Any] = {
        "artifact_name": "100ma_base_20ma_reclaim_200ma_touch_v1",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_db_path": str(db_path),
        "runtime_stock_db_status": runtime_status,
        "rankings_freshness": rankings_freshness,
        "fixed_evaluation_conditions": {
            "scope": "TRADEX-only historical shape validation",
            "start_year": args.start_year,
            "end_date": args.end_date,
            "confirmed_only": not args.include_yahoo,
            "universe": "runtime_stock_db daily_bars excluding ETF/REIT market labels",
            "signal": "probe 20MA reclaim by close below 100MA, followed within 25 sessions by >=1pct gap-up full entry near/below 100MA and below 200MA/monthly MA20",
            "base_condition": ">=80 of prior 100 closes below MA100 and MA100 20d slope <= +2pct",
            "stop_candle_condition": "recent 20d small body candle near rolling 20d low",
            "target": "future high touches signal-date MA200 within 30d/60d",
            "cost_slippage": "not_applied",
            "silent_fallback_used": False,
        },
        "current_champion": "none",
        "current_challenger": "100ma_base_20ma_reclaim_200ma_touch_v1",
        "summary": summary,
        "sample_signals_top_recent": sorted([asdict(signal) for signal in signals], key=lambda row: row["signal_date"], reverse=True)[:20],
    }
    _write_outputs(args.out_dir, payload, signals)
    print(json.dumps({"out_dir": str(args.out_dir), "summary": summary}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
