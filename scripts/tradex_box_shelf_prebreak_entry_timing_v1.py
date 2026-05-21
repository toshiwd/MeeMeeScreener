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
DEFAULT_OUT_DIR = Path("G:/Tradex/box_shelf_prebreak_entry_timing_v1")


@dataclass(frozen=True)
class EntryRow:
    code: str
    name: str
    shelf_month: str
    breakout_confirm_date: str
    entry_rule: str
    entry_date: str
    entry_close: float
    box_upper: float
    entry_vs_box_upper_pct: float
    days_to_breakout: int
    candle_body_ratio: float
    close_pos_in_range: float
    volume_vs_20d: float | None
    ma7: float
    ma20: float
    ma60: float
    close_vs_ma7_pct: float
    close_vs_ma20_pct: float
    max_high_to_breakout_pct: float | None
    min_low_to_breakout_pct: float | None
    max_high_60d_pct: float | None
    min_low_60d_pct: float | None
    return_20d_pct: float | None
    return_60d_pct: float | None
    return_120d_pct: float | None
    win_60d: bool
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
    out["vol20"] = grouped["v"].transform(lambda s: s.rolling(20, min_periods=10).mean())
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


def _select_entry(shelf_rows: pd.DataFrame, box_upper: float, rule: str) -> pd.Series | None:
    base = shelf_rows[
        (shelf_rows["c"] >= box_upper * 0.96)
        & (shelf_rows["c"] <= box_upper * 1.05)
        & (shelf_rows["c"] >= shelf_rows["ma20"] * 0.99)
        & (shelf_rows["ma20"] >= shelf_rows["ma60"] * 0.98)
    ].copy()
    if base.empty:
        return None
    if rule == "first_upper_shelf_close":
        cand = base
    elif rule == "small_body_upper_shelf":
        cand = base[(base["body_ratio"] <= 0.35) & (base["close_pos_in_range"] >= 0.35)]
    elif rule == "green_close_upper_half":
        cand = base[(base["is_green"]) & (base["close_pos_in_range"] >= 0.55)]
    elif rule == "ma7_or_ma20_support_hold":
        near_ma = (
            ((base["c"] / base["ma7"] - 1.0).abs() * 100.0 <= 2.0)
            | ((base["c"] / base["ma20"] - 1.0).abs() * 100.0 <= 2.0)
        )
        cand = base[near_ma & (base["c"] >= base["ma20"])]
    elif rule == "quiet_volume_small_body":
        cand = base[(base["body_ratio"] <= 0.40) & (base["v"] <= base["vol20"] * 0.85)]
    elif rule == "last_support_before_breakout":
        cand = base[
            (
                ((base["c"] / base["ma7"] - 1.0).abs() * 100.0 <= 2.5)
                | ((base["c"] / base["ma20"] - 1.0).abs() * 100.0 <= 2.5)
                | (base["body_ratio"] <= 0.35)
            )
            & (base["close_pos_in_range"] >= 0.30)
        ]
        return None if cand.empty else cand.iloc[-1]
    else:
        raise ValueError(f"unknown rule: {rule}")
    return None if cand.empty else cand.iloc[0]


def _build_rows(daily: pd.DataFrame, source: pd.DataFrame) -> list[EntryRow]:
    rules = [
        "first_upper_shelf_close",
        "small_body_upper_shelf",
        "green_close_upper_half",
        "ma7_or_ma20_support_hold",
        "quiet_volume_small_body",
        "last_support_before_breakout",
    ]
    by_code = {str(code): rows.reset_index(drop=True) for code, rows in daily.groupby("code", sort=False)}
    out: list[EntryRow] = []
    signals = source[source["breakout_confirm_date"].notna()].copy()
    for _, signal in signals.iterrows():
        code = str(signal["code"])
        rows = by_code.get(code)
        if rows is None:
            continue
        breakout_matches = rows.index[rows["date"].eq(str(signal["breakout_confirm_date"]))].tolist()
        if not breakout_matches:
            continue
        breakout_idx = int(breakout_matches[0])
        shelf_rows = rows[(rows["month_key"].eq(str(signal["shelf_month"]))) & (rows.index < breakout_idx)].copy()
        if shelf_rows.empty:
            continue
        box_upper = float(signal["box_upper"])
        for rule in rules:
            entry = _select_entry(shelf_rows, box_upper, rule)
            if entry is None:
                continue
            entry_idx = int(entry.name)
            entry_close = float(entry["c"])
            future = rows.iloc[entry_idx + 1 : entry_idx + 121]
            to_breakout = rows.iloc[entry_idx + 1 : breakout_idx + 1]
            if future.empty or to_breakout.empty:
                continue
            max_high_60 = float(future.iloc[:60]["h"].max())
            min_low_60 = float(future.iloc[:60]["l"].min())
            ret20 = _future_pct(rows, entry_idx, "c", 20, entry_close)
            ret60 = _future_pct(rows, entry_idx, "c", 60, entry_close)
            ret120 = _future_pct(rows, entry_idx, "c", 120, entry_close)
            max60 = (max_high_60 / entry_close - 1.0) * 100.0
            min60 = (min_low_60 / entry_close - 1.0) * 100.0
            days_to_breakout = (
                pd.Timestamp(str(signal["breakout_confirm_date"])) - pd.Timestamp(str(entry["date"]))
            ).days
            out.append(
                EntryRow(
                    code=code,
                    name=str(signal["name"]),
                    shelf_month=str(signal["shelf_month"]),
                    breakout_confirm_date=str(signal["breakout_confirm_date"]),
                    entry_rule=rule,
                    entry_date=str(entry["date"]),
                    entry_close=entry_close,
                    box_upper=box_upper,
                    entry_vs_box_upper_pct=round((entry_close / box_upper - 1.0) * 100.0, 4),
                    days_to_breakout=int(days_to_breakout),
                    candle_body_ratio=round(float(entry["body_ratio"]), 4),
                    close_pos_in_range=round(float(entry["close_pos_in_range"]), 4),
                    volume_vs_20d=None if pd.isna(entry["vol20"]) or float(entry["vol20"]) == 0 else round(float(entry["v"]) / float(entry["vol20"]), 4),
                    ma7=float(entry["ma7"]),
                    ma20=float(entry["ma20"]),
                    ma60=float(entry["ma60"]),
                    close_vs_ma7_pct=round((entry_close / float(entry["ma7"]) - 1.0) * 100.0, 4),
                    close_vs_ma20_pct=round((entry_close / float(entry["ma20"]) - 1.0) * 100.0, 4),
                    max_high_to_breakout_pct=round((float(to_breakout["h"].max()) / entry_close - 1.0) * 100.0, 4),
                    min_low_to_breakout_pct=round((float(to_breakout["l"].min()) / entry_close - 1.0) * 100.0, 4),
                    max_high_60d_pct=round(max60, 4),
                    min_low_60d_pct=round(min60, 4),
                    return_20d_pct=None if ret20 is None else round(ret20, 4),
                    return_60d_pct=None if ret60 is None else round(ret60, 4),
                    return_120d_pct=None if ret120 is None else round(ret120, 4),
                    win_60d=bool(ret60 is not None and ret60 > 0),
                    success_60d=bool(max60 >= 10.0 and min60 > -8.0),
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
        complete = group[group["return_60d_pct"].notna()].copy()
        if complete.empty:
            summaries[str(rule)] = {"count": int(len(group)), "complete_60d_count": 0}
            continue
        summaries[str(rule)] = {
            "count": int(len(group)),
            "complete_60d_count": int(len(complete)),
            "win_20d_rate": float((complete["return_20d_pct"] > 0).mean()),
            "win_60d_rate": float(complete["win_60d"].mean()),
            "win_120d_rate": float((complete["return_120d_pct"] > 0).mean()) if complete["return_120d_pct"].notna().any() else None,
            "success_60d_rate": float(complete["success_60d"].mean()),
            "severe_drawdown_60d_rate": float(complete["severe_drawdown_60d"].mean()),
            "median_return_20d_pct": float(complete["return_20d_pct"].median()),
            "median_return_60d_pct": float(complete["return_60d_pct"].median()),
            "median_return_120d_pct": float(complete["return_120d_pct"].median()) if complete["return_120d_pct"].notna().any() else None,
            "median_max_high_60d_pct": float(complete["max_high_60d_pct"].median()),
            "median_min_low_60d_pct": float(complete["min_low_60d_pct"].median()),
            "median_days_to_breakout": float(complete["days_to_breakout"].median()),
            "median_entry_vs_box_upper_pct": float(complete["entry_vs_box_upper_pct"].median()),
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
    if best.get("win_60d_rate", 0) >= 0.70 and best.get("median_return_60d_pct", 0) >= 5 and best.get("severe_drawdown_60d_rate", 1) <= 0.15:
        judgment = "keep"
        reason = "prebreak_shelf_entry_rule_passes_teppan_gate"
    elif best.get("win_60d_rate", 0) >= 0.62 and best.get("median_return_60d_pct", 0) > 3:
        judgment = "hold"
        reason = "positive_prebreak_entry_but_needs_filter"
    else:
        judgment = "drop"
        reason = "no_prebreak_entry_rule_passes_basic_gate"
    return {"entry_rule_summaries": summaries, "best_entry_rule": best_rule, "judgment": judgment, "reason": reason}


def _write(out_dir: Path, payload: dict[str, Any], rows: list[EntryRow]) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "prebreak_entry_compare.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    dict_rows = [asdict(row) for row in rows]
    with (out_dir / "prebreak_entry_signals.jsonl").open("w", encoding="utf-8", newline="\n") as fh:
        for row in dict_rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    if dict_rows:
        with (out_dir / "prebreak_entry_signals.csv").open("w", encoding="utf-8-sig", newline="") as fh:
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
        "artifact_name": "box_shelf_prebreak_entry_timing_v1",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_db_path": str(db_path),
        "authoritative_input": str(SOURCE_SIGNALS),
        "runtime_stock_db_status": runtime_status,
        "rankings_freshness": rankings_freshness,
        "fixed_evaluation_conditions": {
            "scope": "TRADEX-only pre-breakout shelf entry timing validation",
            "parent_axis": "monthly_box_upper_shelf_breakout_confirmed",
            "entry_rules": [
                "first_upper_shelf_close",
                "small_body_upper_shelf",
                "green_close_upper_half",
                "ma7_or_ma20_support_hold",
                "quiet_volume_small_body",
                "last_support_before_breakout",
            ],
            "success": "60d max high >= +10% and 60d min low > -8%",
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
