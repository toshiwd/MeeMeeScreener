from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path
from typing import Any

import duckdb

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from shared.runtime_stock_db_contract import resolve_runtime_stock_db_path
from tradex_short_shape_bad_avoidance_probe_v1 import _daily_rows


AXIS_ID = "ma_resistance_shape_probe_v1"
DEFAULT_DB_PATH = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\ma_resistance_shape_probe_v1")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _ma(values: list[float], end_index: int, period: int) -> float | None:
    start = end_index - period + 1
    if start < 0:
        return None
    return sum(values[start : end_index + 1]) / period


def _timing_class(*, ret5: float, mae5: float, mfe5: float, mae10: float) -> str:
    if mae5 <= -0.035 and mfe5 <= 0.025 and ret5 <= -0.015:
        return "entry_now"
    if mfe5 >= 0.045 or ret5 >= 0.025:
        return "too_early_or_wrong"
    if mae10 <= -0.045 and mfe5 <= 0.035:
        return "watch_next"
    return "no_edge"


def _class(row: dict[str, Any]) -> str:
    return str(row.get("purpose_outcome_class") or "")


def _bucket(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"n": 0}
    return {
        "n": len(rows),
        "entry_now_rate": sum(1 for row in rows if _class(row) == "entry_now") / len(rows),
        "watch_next_rate": sum(1 for row in rows if _class(row) == "watch_next") / len(rows),
        "wrong_rate": sum(1 for row in rows if _class(row) == "too_early_or_wrong") / len(rows),
        "no_edge_rate": sum(1 for row in rows if _class(row) == "no_edge") / len(rows),
        "avg_ret5": sum(float(row["ret5"]) for row in rows) / len(rows),
        "avg_MAE5": sum(float(row["MAE5"]) for row in rows) / len(rows),
        "avg_MFE5": sum(float(row["MFE5"]) for row in rows) / len(rows),
        "avg_ret10": sum(float(row["ret10"]) for row in rows) / len(rows),
        "avg_MAE10": sum(float(row["MAE10"]) for row in rows) / len(rows),
        "avg_MFE10": sum(float(row["MFE10"]) for row in rows) / len(rows),
    }


def _scan_events(db_path: Path, *, start_ymd: int, max_rows: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    conn = duckdb.connect(str(db_path), read_only=True)
    codes = [str(row[0]) for row in conn.execute("SELECT DISTINCT code FROM daily_bars ORDER BY code").fetchall()]
    try:
        for code in codes:
            if len(rows) >= max_rows:
                break
            bars = _daily_rows(conn, code)
            if len(bars) < 320:
                continue
            closes = [float(row[4]) for row in bars]
            highs = [float(row[2]) for row in bars]
            lows = [float(row[3]) for row in bars]
            volumes = [float(row[5] or 0) for row in bars]
            last_taken = -999
            for index in range(90, len(bars) - 10):
                as_of = int(bars[index][0])
                if as_of < start_ymd or index - last_taken < 7:
                    continue
                ma7 = _ma(closes, index, 7)
                ma20 = _ma(closes, index, 20)
                ma60 = _ma(closes, index, 60)
                ma7_prev3 = _ma(closes, index - 3, 7)
                ma20_prev5 = _ma(closes, index - 5, 20)
                vol20_prev = _ma(volumes, index - 1, 20)
                if None in {ma7, ma20, ma60, ma7_prev3, ma20_prev5} or not vol20_prev:
                    continue
                open_ = float(bars[index][1])
                high = float(bars[index][2])
                low = float(bars[index][3])
                close = float(bars[index][4])
                candle_range = high - low
                if candle_range <= 0:
                    continue
                high20_prev = max(highs[index - 20 : index])
                high60_prev = max(highs[index - 60 : index])
                low60 = min(lows[index - 59 : index + 1])
                high60 = max(highs[index - 59 : index + 1])
                tags = {
                    "below_7_20": close < ma7 and close < ma20,
                    "ma7_resistance": high >= ma7 * 0.995 and close < ma7 and close < open_,
                    "ma20_overhead": close < ma20 and high < ma20 * 1.01,
                    "failed_reclaim_ma20": closes[index - 1] < ma20 and high >= ma20 * 0.985 and close < ma20,
                    "ma7_turning_down": ma7 < ma7_prev3,
                    "ma20_flattening": ma20 / ma20_prev5 - 1.0 <= 0.005,
                    "compressed_under_ma7_20": close < ma7 and close < ma20 and abs(ma7 / ma20 - 1.0) <= 0.025,
                    "above_60_downside_room": close / ma60 - 1.0 >= 0.03,
                    "recent_high_zone": max(highs[index - 15 : index + 1]) >= high60_prev * 0.97,
                    "not_deep_from_high20": close / high20_prev - 1.0 >= -0.06,
                    "bear_or_upper_wick": close < open_ or (high - max(open_, close)) / candle_range >= 0.20,
                }
                if not (
                    tags["below_7_20"]
                    and tags["above_60_downside_room"]
                    and tags["recent_high_zone"]
                    and (tags["ma7_resistance"] or tags["ma20_overhead"] or tags["failed_reclaim_ma20"])
                ):
                    continue
                future5 = bars[index + 1 : index + 6]
                future10 = bars[index + 1 : index + 11]
                ret5 = float(bars[index + 5][4]) / close - 1.0
                ret10 = float(bars[index + 10][4]) / close - 1.0
                mae5 = min(float(row[3]) for row in future5) / close - 1.0
                mfe5 = max(float(row[2]) for row in future5) / close - 1.0
                mae10 = min(float(row[3]) for row in future10) / close - 1.0
                mfe10 = max(float(row[2]) for row in future10) / close - 1.0
                rows.append(
                    {
                        "sample_key": f"{code}:{as_of}",
                        "code": code,
                        "as_of": as_of,
                        "purpose_outcome_class": _timing_class(ret5=ret5, mae5=mae5, mfe5=mfe5, mae10=mae10),
                        "ret5": round(ret5, 8),
                        "ret10": round(ret10, 8),
                        "MAE5": round(mae5, 8),
                        "MFE5": round(mfe5, 8),
                        "MAE10": round(mae10, 8),
                        "MFE10": round(mfe10, 8),
                        "close_vs_ma7": round(close / ma7 - 1.0, 8),
                        "close_vs_ma20": round(close / ma20 - 1.0, 8),
                        "close_vs_ma60": round(close / ma60 - 1.0, 8),
                        "ma7_vs_ma20": round(ma7 / ma20 - 1.0, 8),
                        "ma20_vs_ma60": round(ma20 / ma60 - 1.0, 8),
                        "ma7_slope3": round(ma7 / ma7_prev3 - 1.0, 8),
                        "ma20_slope5": round(ma20 / ma20_prev5 - 1.0, 8),
                        "close_vs_prev_high20": round(close / high20_prev - 1.0, 8),
                        "range_pos60": round((close - low60) / (high60 - low60), 8) if high60 > low60 else None,
                        "volume_ratio20": round(volumes[index] / vol20_prev, 8),
                        "tags": tags,
                    }
                )
                last_taken = index
                if len(rows) >= max_rows:
                    break
    finally:
        conn.close()
    return rows


def _tag_summary(rows: list[dict[str, Any]], *, train_until: int) -> list[dict[str, Any]]:
    if not rows:
        return []
    tag_names = sorted(rows[0]["tags"].keys())
    summaries = []
    for size in (1, 2, 3):
        for combo in combinations(tag_names, size):
            selected = [row for row in rows if all(row["tags"].get(tag) for tag in combo)]
            train = [row for row in selected if int(row["as_of"]) <= train_until]
            test = [row for row in selected if int(row["as_of"]) > train_until]
            if len(train) < 30 or len(test) < 20:
                continue
            summaries.append({"tags": combo, "train": _bucket(train), "test": _bucket(test)})
    summaries.sort(key=lambda row: (row["test"]["wrong_rate"], -row["test"]["entry_now_rate"], -row["test"]["n"]))
    return summaries


def run(*, db_path: Path, output_root: Path, start_ymd: int, max_rows: int, train_until: int) -> Path:
    rows = _scan_events(db_path, start_ymd=start_ymd, max_rows=max_rows)
    train = [row for row in rows if int(row["as_of"]) <= train_until]
    test = [row for row in rows if int(row["as_of"]) > train_until]
    tag_summaries = _tag_summary(rows, train_until=train_until)
    test_base = _bucket(test)
    keep = [
        row for row in tag_summaries
        if row["test"]["n"] >= 30
        and row["test"]["wrong_rate"] <= test_base["wrong_rate"] - 0.05
        and row["test"]["avg_MFE5"] <= test_base["avg_MFE5"] - 0.005
    ][:10]
    report = {
        "schema_version": AXIS_ID,
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "db_path": str(db_path),
        "fixed_evaluation_conditions": {
            "source": "confirmed non-yahoo daily_bars",
            "start_ymd": start_ymd,
            "max_rows": max_rows,
            "train_until": train_until,
            "test_after": train_until,
            "base_entry": "below_7_20 + above_60_downside_room + recent_high_zone + one MA resistance tag",
        },
        "sample_counts": {"all": len(rows), "train": len(train), "test": len(test)},
        "train_baseline": _bucket(train),
        "test_baseline": test_base,
        "top_tag_summaries": tag_summaries[:20],
        "keep_tags": keep,
        "decision": {
            "candidate_local_decision": "keep_ma_resistance_watch_filter" if keep else "drop_ma_resistance_shape_current_form",
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": "MA resistance tags reduced OOS wrong-rate/MFE enough for watch filtering" if keep else "MA resistance tags did not clear OOS gates",
        },
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
    }
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    _write_json(output_dir / "ma_resistance_shape_probe.json", report)
    _write_json(output_root / "latest_ma_resistance_shape_probe.json", {"run_root": str(output_dir), **report})
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--start-ymd", type=int, default=20150101)
    parser.add_argument("--max-rows", type=int, default=6000)
    parser.add_argument("--train-until", type=int, default=20201231)
    args = parser.parse_args()
    print(run(
        db_path=args.db_path or resolve_runtime_stock_db_path(),
        output_root=args.output_root,
        start_ymd=args.start_ymd,
        max_rows=args.max_rows,
        train_until=args.train_until,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
