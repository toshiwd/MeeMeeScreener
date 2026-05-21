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


SOURCE_SIGNALS = Path("G:/Tradex/box_shelf_prebreak_entry_timing_v1/prebreak_entry_signals.csv")
DEFAULT_OUT_DIR = Path("G:/Tradex/box_shelf_pullback_exit_line_visibility_v1")


@dataclass(frozen=True)
class LineRow:
    code: str
    name: str
    entry_date: str
    breakout_confirm_date: str
    entry_close: float
    box_upper: float
    ma7_at_entry: float
    ma20_at_entry: float
    ma60_at_entry: float
    pre_entry_20d_low: float
    max_pullback_to_breakout_pct: float
    max_pullback_20d_pct: float
    max_pullback_60d_pct: float
    return_60d_pct: float | None
    win_60d: bool
    touched_ma7_20d: bool
    touched_ma20_20d: bool
    broke_entry_low_20d: bool
    broke_pre_entry_20d_low_60d: bool
    broke_box_upper_after_breakout_60d: bool
    ma20_break_before_breakout: bool
    exit_line_candidate: str
    line_price: float
    line_distance_pct: float


def _epoch_to_iso_expr(column: str) -> str:
    return f"CAST(strftime(to_timestamp({column}), '%Y-%m-%d') AS VARCHAR)"


def _load_daily(conn: duckdb.DuckDBPyConnection, *, start_year: int, end_date: str) -> pd.DataFrame:
    query = f"""
    SELECT b.code, b.date AS date_epoch, {_epoch_to_iso_expr("b.date")} AS date,
           b.o, b.h, b.l, b.c, b.v, COALESCE(b.source, '') AS source
    FROM daily_bars b
    LEFT JOIN industry_master im ON b.code = im.code
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


def _add_ma(daily: pd.DataFrame) -> pd.DataFrame:
    out = daily.copy()
    grouped = out.groupby("code", group_keys=False)
    for period in (7, 20, 60):
        out[f"ma{period}"] = grouped["c"].transform(lambda s, p=period: s.rolling(p, min_periods=p).mean())
    out["pre20_low"] = grouped["l"].transform(lambda s: s.shift(1).rolling(20, min_periods=10).min())
    return out


def _build_rows(daily: pd.DataFrame, source: pd.DataFrame) -> list[LineRow]:
    first = source[source["entry_rule"].eq("first_upper_shelf_close")].copy()
    by_code = {str(code): rows.reset_index(drop=True) for code, rows in daily.groupby("code", sort=False)}
    out: list[LineRow] = []
    for _, signal in first.iterrows():
        code = str(signal["code"])
        rows = by_code.get(code)
        if rows is None:
            continue
        entry_matches = rows.index[rows["date"].eq(str(signal["entry_date"]))].tolist()
        breakout_matches = rows.index[rows["date"].eq(str(signal["breakout_confirm_date"]))].tolist()
        if not entry_matches or not breakout_matches:
            continue
        entry_idx = int(entry_matches[0])
        breakout_idx = int(breakout_matches[0])
        if breakout_idx <= entry_idx:
            continue
        entry = rows.iloc[entry_idx]
        entry_close = float(entry["c"])
        pre20_low = float(entry["pre20_low"]) if not pd.isna(entry["pre20_low"]) else float(entry["l"])
        to_breakout = rows.iloc[entry_idx + 1 : breakout_idx + 1]
        future20 = rows.iloc[entry_idx + 1 : entry_idx + 21]
        future60 = rows.iloc[entry_idx + 1 : entry_idx + 61]
        after_breakout60 = rows.iloc[breakout_idx + 1 : breakout_idx + 61]
        if to_breakout.empty or future20.empty or future60.empty:
            continue
        ret60 = None
        if entry_idx + 60 < len(rows):
            ret60 = (float(rows.iloc[entry_idx + 60]["c"]) / entry_close - 1.0) * 100.0
        box_upper = float(signal["box_upper"])
        line_candidates = {
            "entry_low_minus_1pct": float(entry["l"]) * 0.99,
            "ma20_minus_2pct": float(entry["ma20"]) * 0.98,
            "pre_entry_20d_low_minus_1pct": pre20_low * 0.99,
            "box_upper_minus_6pct": box_upper * 0.94,
        }
        # A practical default: the tighter of MA20-2% and pre-entry low-1%, but not above entry close.
        practical_name = "practical_max_of_ma20_minus2_or_pre20low_minus1"
        practical_price = min(entry_close, max(line_candidates["ma20_minus_2pct"], line_candidates["pre_entry_20d_low_minus_1pct"]))
        out.append(
            LineRow(
                code=code,
                name=str(signal["name"]),
                entry_date=str(signal["entry_date"]),
                breakout_confirm_date=str(signal["breakout_confirm_date"]),
                entry_close=entry_close,
                box_upper=box_upper,
                ma7_at_entry=float(entry["ma7"]),
                ma20_at_entry=float(entry["ma20"]),
                ma60_at_entry=float(entry["ma60"]),
                pre_entry_20d_low=pre20_low,
                max_pullback_to_breakout_pct=round((float(to_breakout["l"].min()) / entry_close - 1.0) * 100.0, 4),
                max_pullback_20d_pct=round((float(future20["l"].min()) / entry_close - 1.0) * 100.0, 4),
                max_pullback_60d_pct=round((float(future60["l"].min()) / entry_close - 1.0) * 100.0, 4),
                return_60d_pct=None if ret60 is None else round(ret60, 4),
                win_60d=bool(ret60 is not None and ret60 > 0),
                touched_ma7_20d=bool((future20["l"] <= future20["ma7"]).any()),
                touched_ma20_20d=bool((future20["l"] <= future20["ma20"]).any()),
                broke_entry_low_20d=bool((future20["l"] < float(entry["l"])).any()),
                broke_pre_entry_20d_low_60d=bool((future60["l"] < pre20_low).any()),
                broke_box_upper_after_breakout_60d=bool(not after_breakout60.empty and (after_breakout60["c"] < box_upper).any()),
                ma20_break_before_breakout=bool((to_breakout["c"] < to_breakout["ma20"]).any()),
                exit_line_candidate=practical_name,
                line_price=round(practical_price, 4),
                line_distance_pct=round((practical_price / entry_close - 1.0) * 100.0, 4),
            )
        )
    return out


def _rate(df: pd.DataFrame, mask: pd.Series) -> float:
    return float(mask.mean()) if len(df) else 0.0


def _summarize(rows: list[LineRow]) -> dict[str, Any]:
    df = pd.DataFrame([asdict(row) for row in rows])
    complete = df[df["return_60d_pct"].notna()].copy()
    if complete.empty:
        return {"row_count": int(len(df)), "complete_60d_count": 0, "judgment": "hold"}
    winners = complete[complete["win_60d"]].copy()
    losers = complete[~complete["win_60d"]].copy()
    def pullback_quantiles(frame: pd.DataFrame) -> dict[str, float | None]:
        if frame.empty:
            return {"p50": None, "p75": None, "p90": None}
        s = frame["max_pullback_60d_pct"]
        return {"p50": float(s.quantile(0.50)), "p75": float(s.quantile(0.25)), "p90": float(s.quantile(0.10))}
    return {
        "row_count": int(len(df)),
        "complete_60d_count": int(len(complete)),
        "win_60d_rate": float(complete["win_60d"].mean()),
        "median_return_60d_pct": float(complete["return_60d_pct"].median()),
        "pullback_60d_quantiles_all": pullback_quantiles(complete),
        "pullback_60d_quantiles_winners": pullback_quantiles(winners),
        "pullback_60d_quantiles_losers": pullback_quantiles(losers),
        "touch_rates_all": {
            "touched_ma7_20d": _rate(complete, complete["touched_ma7_20d"]),
            "touched_ma20_20d": _rate(complete, complete["touched_ma20_20d"]),
            "broke_entry_low_20d": _rate(complete, complete["broke_entry_low_20d"]),
            "broke_pre_entry_20d_low_60d": _rate(complete, complete["broke_pre_entry_20d_low_60d"]),
            "broke_box_upper_after_breakout_60d": _rate(complete, complete["broke_box_upper_after_breakout_60d"]),
            "ma20_break_before_breakout": _rate(complete, complete["ma20_break_before_breakout"]),
        },
        "touch_rates_winners": {
            "touched_ma7_20d": _rate(winners, winners["touched_ma7_20d"]),
            "touched_ma20_20d": _rate(winners, winners["touched_ma20_20d"]),
            "broke_entry_low_20d": _rate(winners, winners["broke_entry_low_20d"]),
            "broke_pre_entry_20d_low_60d": _rate(winners, winners["broke_pre_entry_20d_low_60d"]),
            "broke_box_upper_after_breakout_60d": _rate(winners, winners["broke_box_upper_after_breakout_60d"]),
            "ma20_break_before_breakout": _rate(winners, winners["ma20_break_before_breakout"]),
        },
        "touch_rates_losers": {
            "touched_ma7_20d": _rate(losers, losers["touched_ma7_20d"]),
            "touched_ma20_20d": _rate(losers, losers["touched_ma20_20d"]),
            "broke_entry_low_20d": _rate(losers, losers["broke_entry_low_20d"]),
            "broke_pre_entry_20d_low_60d": _rate(losers, losers["broke_pre_entry_20d_low_60d"]),
            "broke_box_upper_after_breakout_60d": _rate(losers, losers["broke_box_upper_after_breakout_60d"]),
            "ma20_break_before_breakout": _rate(losers, losers["ma20_break_before_breakout"]),
        },
        "recommended_visibility": {
            "押し目目安": "entryから-3%〜-5%は通常の揺れ。勝ちサンプルでも60日内p75近辺は約この帯になりやすい。",
            "警戒ライン": "entry後20日内にentry足安値を割る、またはブレイク前に終値でMA20割れ。",
            "撤退候補": "pre_entry_20d_low-1% と MA20-2% の高い方。深く置くならbox_upper-6%。",
            "ブレイク後確認": "ブレイク後にbox_upperを終値で割り返す場合は失敗寄りとして可視化。",
        },
        "judgment": "visibility_needed_not_filter_drop",
    }


def _write(out_dir: Path, payload: dict[str, Any], rows: list[LineRow]) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "pullback_exit_line_visibility.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    dict_rows = [asdict(row) for row in rows]
    with (out_dir / "pullback_exit_line_ledger.jsonl").open("w", encoding="utf-8", newline="\n") as fh:
        for row in dict_rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    if dict_rows:
        with (out_dir / "pullback_exit_line_ledger.csv").open("w", encoding="utf-8-sig", newline="") as fh:
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
        daily = _add_ma(_load_daily(conn, start_year=args.start_year, end_date=args.end_date))
    finally:
        conn.close()
    rows = _build_rows(daily, source)
    summary = _summarize(rows)
    payload: dict[str, Any] = {
        "artifact_name": "box_shelf_pullback_exit_line_visibility_v1",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_db_path": str(db_path),
        "authoritative_input": str(SOURCE_SIGNALS),
        "runtime_stock_db_status": runtime_status,
        "rankings_freshness": rankings_freshness,
        "fixed_evaluation_conditions": {
            "scope": "TRADEX-only pullback and exit-line visibility for first_upper_shelf_close",
            "parent_axis": "monthly_box_upper_shelf_breakout_confirmed / first_upper_shelf_close",
            "line_candidates": ["entry low", "MA20", "pre-entry 20d low", "box upper", "box upper after breakout"],
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
