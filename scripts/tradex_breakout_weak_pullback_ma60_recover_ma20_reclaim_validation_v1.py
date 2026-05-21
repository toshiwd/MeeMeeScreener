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


DEFAULT_OUT_DIR = Path("G:/Tradex/breakout_weak_pullback_ma60_recover_ma20_reclaim_v1")


@dataclass(frozen=True)
class SignalRow:
    code: str
    name: str
    range_breakout_date: str
    ma60_break_date: str
    ma60_recover_date: str
    ma20_reclaim_date: str
    entry_close: float
    range_upper: float
    range_lower: float
    range_width_pct: float
    breakout_close_pct_above_range: float
    ma60_break_close: float
    ma60_recover_close: float
    ma20_reclaim_close: float
    ma7: float
    ma20: float
    ma60: float
    ma100: float
    ma200: float
    max_high_20d_pct: float | None
    max_high_60d_pct: float | None
    max_high_120d_pct: float | None
    min_low_20d_pct: float | None
    min_low_60d_pct: float | None
    return_20d_pct: float | None
    return_60d_pct: float | None
    return_120d_pct: float | None
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


def _add_features(daily: pd.DataFrame) -> pd.DataFrame:
    out = daily.copy()
    grouped = out.groupby("code", group_keys=False)
    for period in (7, 20, 60, 100, 200):
        out[f"ma{period}"] = grouped["c"].transform(lambda s, p=period: s.rolling(p, min_periods=p).mean())
    out["prev_c"] = grouped["c"].shift(1)
    out["prev_ma20"] = grouped["ma20"].shift(1)
    out["prev_ma60"] = grouped["ma60"].shift(1)
    out["range_upper_90"] = grouped["h"].transform(lambda s: s.shift(1).rolling(90, min_periods=70).max())
    out["range_lower_90"] = grouped["l"].transform(lambda s: s.shift(1).rolling(90, min_periods=70).min())
    out["range_close_span_90"] = grouped["c"].transform(
        lambda s: (s.shift(1).rolling(90, min_periods=70).max() / s.shift(1).rolling(90, min_periods=70).min() - 1.0) * 100.0
    )
    out["breakout"] = (
        (out["c"] >= out["range_upper_90"] * 1.05)
        & (out["range_close_span_90"] <= 22.0)
        & (out["ma20"] >= out["ma60"] * 0.98)
        & (out["c"] > out["ma100"])
    )
    return out


def _future_pct(rows: pd.DataFrame, idx: int, column: str, horizon: int, base: float) -> float | None:
    target = idx + horizon
    if target >= len(rows):
        return None
    value = rows.iloc[target][column]
    if pd.isna(value):
        return None
    return (float(value) / base - 1.0) * 100.0


def _build_signals(df: pd.DataFrame) -> list[SignalRow]:
    signals: list[SignalRow] = []
    for code, rows in df.groupby("code", sort=False):
        rows = rows.reset_index(drop=True)
        breakout_indices = rows.index[rows["breakout"].fillna(False)].tolist()
        for breakout_idx in breakout_indices:
            if breakout_idx < 220:
                continue
            breakout = rows.iloc[breakout_idx]
            if any(pd.isna(breakout[col]) for col in ["ma20", "ma60", "ma100", "ma200", "range_upper_90", "range_lower_90"]):
                continue
            pull_window = rows.iloc[breakout_idx + 5 : breakout_idx + 45]
            ma60_breaks = pull_window[(pull_window["c"] < pull_window["ma60"]) & (pull_window["prev_c"] >= pull_window["prev_ma60"])]
            if ma60_breaks.empty:
                continue
            ma60_break = ma60_breaks.iloc[0]
            ma60_break_idx = int(ma60_break.name)
            recover_window = rows.iloc[ma60_break_idx + 1 : ma60_break_idx + 4]
            recovers = recover_window[recover_window["c"] > recover_window["ma60"]]
            if recovers.empty:
                continue
            recover = recovers.iloc[0]
            recover_idx = int(recover.name)
            reclaim_window = rows.iloc[recover_idx : recover_idx + 21]
            reclaims = reclaim_window[(reclaim_window["c"] > reclaim_window["ma20"]) & (reclaim_window["prev_c"] <= reclaim_window["prev_ma20"])]
            if reclaims.empty:
                continue
            reclaim = reclaims.iloc[0]
            entry_idx = int(reclaim.name)
            if any(pd.isna(reclaim[col]) for col in ["ma7", "ma20", "ma60", "ma100", "ma200"]):
                continue
            if not (reclaim["ma20"] > reclaim["ma60"] > reclaim["ma100"] > reclaim["ma200"] * 0.95):
                continue
            entry_close = float(reclaim["c"])
            future = rows.iloc[entry_idx + 1 : entry_idx + 121]
            if future.empty:
                continue
            max_high_20 = float(future.iloc[:20]["h"].max())
            max_high_60 = float(future.iloc[:60]["h"].max())
            max_high_120 = float(future["h"].max())
            min_low_20 = float(future.iloc[:20]["l"].min())
            min_low_60 = float(future.iloc[:60]["l"].min())
            ret20 = _future_pct(rows, entry_idx, "c", 20, entry_close)
            ret60 = _future_pct(rows, entry_idx, "c", 60, entry_close)
            ret120 = _future_pct(rows, entry_idx, "c", 120, entry_close)
            min_low_60_pct = (min_low_60 / entry_close - 1.0) * 100.0
            max_high_60_pct = (max_high_60 / entry_close - 1.0) * 100.0
            signals.append(
                SignalRow(
                    code=str(code),
                    name=str(reclaim["name"] or ""),
                    range_breakout_date=str(breakout["date"]),
                    ma60_break_date=str(ma60_break["date"]),
                    ma60_recover_date=str(recover["date"]),
                    ma20_reclaim_date=str(reclaim["date"]),
                    entry_close=entry_close,
                    range_upper=round(float(breakout["range_upper_90"]), 4),
                    range_lower=round(float(breakout["range_lower_90"]), 4),
                    range_width_pct=round((float(breakout["range_upper_90"]) / float(breakout["range_lower_90"]) - 1.0) * 100.0, 4),
                    breakout_close_pct_above_range=round((float(breakout["c"]) / float(breakout["range_upper_90"]) - 1.0) * 100.0, 4),
                    ma60_break_close=float(ma60_break["c"]),
                    ma60_recover_close=float(recover["c"]),
                    ma20_reclaim_close=entry_close,
                    ma7=float(reclaim["ma7"]),
                    ma20=float(reclaim["ma20"]),
                    ma60=float(reclaim["ma60"]),
                    ma100=float(reclaim["ma100"]),
                    ma200=float(reclaim["ma200"]),
                    max_high_20d_pct=round((max_high_20 / entry_close - 1.0) * 100.0, 4),
                    max_high_60d_pct=round(max_high_60_pct, 4),
                    max_high_120d_pct=round((max_high_120 / entry_close - 1.0) * 100.0, 4),
                    min_low_20d_pct=round((min_low_20 / entry_close - 1.0) * 100.0, 4),
                    min_low_60d_pct=round(min_low_60_pct, 4),
                    return_20d_pct=None if ret20 is None else round(ret20, 4),
                    return_60d_pct=None if ret60 is None else round(ret60, 4),
                    return_120d_pct=None if ret120 is None else round(ret120, 4),
                    win_60d=bool(ret60 is not None and ret60 > 0),
                    success_60d=bool(max_high_60_pct >= 10.0 and min_low_60_pct > -8.0),
                    severe_drawdown_60d=bool(min_low_60_pct <= -8.0),
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
    win60 = float(complete["win_60d"].mean())
    success = float(complete["success_60d"].mean())
    severe = float(complete["severe_drawdown_60d"].mean())
    med60 = float(complete["return_60d_pct"].median())
    if win60 >= 0.62 and med60 > 3.0 and severe <= 0.22:
        judgment = "keep"
        reason = "win_rate_median_return_and_drawdown_pass"
    elif win60 < 0.55 or med60 <= 0:
        judgment = "drop"
        reason = "win_rate_or_median_return_fail"
    else:
        judgment = "hold"
        reason = "positive_but_needs_position_sizing_or_filter"
    return {
        "signal_count": int(len(df)),
        "complete_60d_count": int(len(complete)),
        "unique_symbol_count": int(df["code"].nunique()),
        "win_20d_rate": float((complete["return_20d_pct"] > 0).mean()),
        "win_60d_rate": win60,
        "win_120d_rate": float((complete["return_120d_pct"] > 0).mean()) if complete["return_120d_pct"].notna().any() else None,
        "success_60d_rate": success,
        "severe_drawdown_60d_rate": severe,
        "median_return_20d_pct": float(complete["return_20d_pct"].median()),
        "median_return_60d_pct": med60,
        "median_return_120d_pct": float(complete["return_120d_pct"].median()) if complete["return_120d_pct"].notna().any() else None,
        "mean_return_60d_pct": float(complete["return_60d_pct"].mean()),
        "median_max_high_60d_pct": float(complete["max_high_60d_pct"].median()),
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
        daily = _add_features(_load_daily(conn, start_year=args.start_year, end_date=args.end_date))
    finally:
        conn.close()
    signals = _build_signals(daily)
    summary = _summarize(signals)
    payload: dict[str, Any] = {
        "artifact_name": "breakout_weak_pullback_ma60_recover_ma20_reclaim_v1",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_db_path": str(db_path),
        "runtime_stock_db_status": runtime_status,
        "rankings_freshness": rankings_freshness,
        "fixed_evaluation_conditions": {
            "scope": "TRADEX-only historical shape validation",
            "start_year": args.start_year,
            "end_date": args.end_date,
            "confirmed_only": True,
            "universe": "runtime_stock_db daily_bars excluding ETF/REIT market labels",
            "signal": "90-session range breakout, weak pullback breaks MA60, MA60 recovered within 3 sessions, then MA20 reclaimed within 20 sessions",
            "entry": "MA20 reclaim close after fast MA60 recovery",
            "success": "60d max high >= +10% and 60d min low > -8%",
            "cost_slippage": "not_applied",
            "silent_fallback_used": False,
        },
        "current_champion": "none",
        "current_challenger": "breakout_weak_pullback_ma60_recover_ma20_reclaim_v1",
        "summary": summary,
        "sample_signals_top_recent": sorted([asdict(signal) for signal in signals], key=lambda row: row["ma20_reclaim_date"], reverse=True)[:20],
    }
    _write_outputs(args.out_dir, payload, signals)
    print(json.dumps({"out_dir": str(args.out_dir), "summary": summary}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
