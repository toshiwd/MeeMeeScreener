from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path
from typing import Any

import duckdb
import numpy as np

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from shared.runtime_stock_db_contract import resolve_runtime_stock_db_path
from tradex_short_shape_bad_avoidance_probe_v1 import _daily_rows, _numeric_features_for
from tradex_short_shape_numeric_rule_probe_v1 import FEATURE_NAMES, _apply_rule


AXIS_ID = "macnica_like_return_sell_probe_v1"
DEFAULT_DB_PATH = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\macnica_like_return_sell_probe_v1")


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


def _class(row: dict[str, Any]) -> str:
    return str(row.get("purpose_outcome_class") or "")


def _timing_class(*, ret5: float, mae5: float, mfe5: float, mae10: float) -> str:
    if mae5 <= -0.035 and mfe5 <= 0.025 and ret5 <= -0.015:
        return "entry_now"
    if mfe5 >= 0.045 or ret5 >= 0.025:
        return "too_early_or_wrong"
    if mae10 <= -0.045 and mfe5 <= 0.035:
        return "watch_next"
    return "no_edge"


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
                if as_of < start_ymd or index - last_taken < 10:
                    continue
                ma7 = _ma(closes, index, 7)
                ma20 = _ma(closes, index, 20)
                ma60 = _ma(closes, index, 60)
                ma20_prev5 = _ma(closes, index - 5, 20)
                vol20_prev = _ma(volumes, index - 1, 20)
                if ma7 is None or ma20 is None or ma60 is None or ma20_prev5 is None or not vol20_prev:
                    continue
                open_ = float(bars[index][1])
                high = float(bars[index][2])
                low = float(bars[index][3])
                close = float(bars[index][4])
                candle_range = high - low
                if candle_range <= 0:
                    continue
                high60_prev = max(highs[index - 60 : index])
                high20_prev = max(highs[index - 20 : index])
                low60 = min(lows[index - 59 : index + 1])
                high60 = max(highs[index - 59 : index + 1])
                high_zone_recent = max(highs[index - 15 : index + 1]) >= high60_prev * 0.97
                below_ma20 = close < ma20
                failed_to_retake_ma20 = high < ma20 * 1.01
                still_above_ma60 = close / ma60 - 1.0 >= 0.03
                ma_stack_extended = ma20 / ma60 - 1.0 >= 0.04
                recent_break = closes[index - 1] < ma20 or closes[index - 2] < ma20
                bearish_or_upper = close < open_ or (high - max(open_, close)) / candle_range >= 0.20
                if not (
                    high_zone_recent
                    and below_ma20
                    and failed_to_retake_ma20
                    and still_above_ma60
                    and ma_stack_extended
                    and recent_break
                    and bearish_or_upper
                ):
                    continue
                future5 = bars[index + 1 : index + 6]
                future10 = bars[index + 1 : index + 11]
                ret3 = float(bars[index + 3][4]) / close - 1.0
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
                        "ret3": round(ret3, 8),
                        "ret5": round(ret5, 8),
                        "ret10": round(ret10, 8),
                        "MAE5": round(mae5, 8),
                        "MFE5": round(mfe5, 8),
                        "MAE10": round(mae10, 8),
                        "MFE10": round(mfe10, 8),
                        "ret20": round(ret10, 8),
                        "MAE20": round(mae10, 8),
                        "MFE20": round(mfe10, 8),
                        "setup_family": "return_sell_below_ma20_after_high_zone",
                        "close_vs_ma20": round(close / ma20 - 1.0, 8),
                        "close_vs_ma60": round(close / ma60 - 1.0, 8),
                        "ma20_vs_ma60": round(ma20 / ma60 - 1.0, 8),
                        "ma20_slope5": round(ma20 / ma20_prev5 - 1.0, 8),
                        "range_pos60": round((close - low60) / (high60 - low60), 8) if high60 > low60 else None,
                        "close_vs_prev_high20": round(close / high20_prev - 1.0, 8),
                        "volume_ratio20": round(volumes[index] / vol20_prev, 8),
                    }
                )
                last_taken = index
                if len(rows) >= max_rows:
                    break
    finally:
        conn.close()
    return rows


def _candidate_clauses(x: np.ndarray) -> list[dict[str, Any]]:
    clauses: list[dict[str, Any]] = []
    for index, name in enumerate(FEATURE_NAMES):
        values = x[:, index]
        for q in (0.2, 0.35, 0.5, 0.65, 0.8):
            threshold = float(np.quantile(values, q))
            clauses.append({"feature": name, "op": "<=", "threshold": threshold})
            clauses.append({"feature": name, "op": ">=", "threshold": threshold})
    return clauses


def _summarize(x: np.ndarray, rows: list[dict[str, Any]], clauses: list[dict[str, Any]]) -> dict[str, Any]:
    mask = _apply_rule(x, clauses)
    selected = [row for row, keep in zip(rows, mask) if bool(keep)]
    return {"clauses": clauses, **_bucket(selected)}


def run(*, db_path: Path, output_root: Path, start_ymd: int, max_rows: int, train_until: int) -> Path:
    events_raw = _scan_events(db_path, start_ymd=start_ymd, max_rows=max_rows)
    x_all, events = _numeric_features_for(events_raw, db_path)
    train_idx = [i for i, row in enumerate(events) if int(row["as_of"]) <= train_until]
    test_idx = [i for i, row in enumerate(events) if int(row["as_of"]) > train_until]
    x_train = x_all[train_idx]
    train_rows = [events[i] for i in train_idx]
    x_test = x_all[test_idx]
    test_rows = [events[i] for i in test_idx]
    train_base = _bucket(train_rows)
    test_base = _bucket(test_rows)
    clauses = _candidate_clauses(x_train) if len(train_rows) else []
    rule_specs = [[clause] for clause in clauses]
    for left, right in combinations(clauses, 2):
        if left["feature"] != right["feature"]:
            rule_specs.append([left, right])
    train_survivors = []
    for rule in rule_specs:
        summary = _summarize(x_train, train_rows, rule)
        if summary["n"] < 25:
            continue
        if summary["entry_now_rate"] < train_base["entry_now_rate"] + 0.06:
            continue
        if summary["wrong_rate"] > train_base["wrong_rate"] - 0.03:
            continue
        train_survivors.append(summary)
    train_survivors.sort(key=lambda row: (-row["entry_now_rate"], row["wrong_rate"], -row["n"]))
    tested = [{"train": summary, "test": _summarize(x_test, test_rows, summary["clauses"])} for summary in train_survivors[:30]]
    low_wrong_train = []
    for rule in rule_specs:
        summary = _summarize(x_train, train_rows, rule)
        if summary["n"] < 25:
            continue
        if summary["wrong_rate"] > train_base["wrong_rate"] - 0.10:
            continue
        if summary["avg_MFE5"] > train_base["avg_MFE5"] - 0.006:
            continue
        low_wrong_train.append(summary)
    low_wrong_train.sort(key=lambda row: (row["wrong_rate"], row["avg_MFE5"], -row["n"]))
    low_wrong_tested = [
        {"train": summary, "test": _summarize(x_test, test_rows, summary["clauses"])}
        for summary in low_wrong_train[:30]
    ]
    low_wrong_keep = [
        row for row in low_wrong_tested
        if row["test"].get("n", 0) >= 20
        and row["test"].get("wrong_rate", 1) <= test_base["wrong_rate"] - 0.08
        and row["test"].get("avg_MFE5", 1) <= test_base["avg_MFE5"] - 0.006
    ]
    keep = [
        row for row in tested
        if row["test"].get("n", 0) >= 20
        and row["test"].get("entry_now_rate", 0) >= test_base["entry_now_rate"] + 0.04
        and row["test"].get("wrong_rate", 1) <= test_base["wrong_rate"] - 0.02
        and row["test"].get("avg_ret5", 1) < test_base["avg_ret5"]
    ]
    shallow_rows = [row for row in events if float(row.get("close_vs_prev_high20") or -1.0) >= -0.0575]
    shallow_train_rows = [row for row in shallow_rows if int(row["as_of"]) <= train_until]
    shallow_test_rows = [row for row in shallow_rows if int(row["as_of"]) > train_until]
    shallow_baseline = {
        "train": _bucket(shallow_train_rows),
        "test": _bucket(shallow_test_rows),
        "condition": "close_vs_prev_high20 >= -0.0575",
    }
    report = {
        "schema_version": AXIS_ID,
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "db_path": str(db_path),
        "fixed_evaluation_conditions": {
            "source": "confirmed non-yahoo daily_bars",
            "setup_family": "return_sell_below_ma20_after_high_zone",
            "start_ymd": start_ymd,
            "max_rows": max_rows,
            "train_until": train_until,
            "test_after": train_until,
        },
        "sample_counts": {"all": len(events), "train": len(train_rows), "test": len(test_rows)},
        "train_baseline": train_base,
        "test_baseline": test_base,
        "candidate_rule_count": len(rule_specs),
        "train_survivor_count": len(train_survivors),
        "tested_count": len(tested),
        "top_tested_rules": tested[:10],
        "keep_rules": keep[:10],
        "low_wrong_diagnostic": {
            "train_survivor_count": len(low_wrong_train),
            "tested_count": len(low_wrong_tested),
            "top_tested_rules": low_wrong_tested[:10],
            "keep_rules": low_wrong_keep[:10],
        },
        "shallow_decline_diagnostic": shallow_baseline,
        "decision": {
            "candidate_local_decision": (
                "keep_macnica_like_branch_for_current_scan"
                if keep
                else "hold_macnica_like_shallow_decline_watch"
                if shallow_baseline["test"].get("n", 0) >= 50
                and shallow_baseline["test"].get("wrong_rate", 1) <= test_base["wrong_rate"] - 0.05
                else "hold_macnica_like_low_wrong_filter_only"
                if low_wrong_keep
                else "drop_macnica_like_branch_current_form"
            ),
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": (
                "macnica-like return-sell branch cleared temporal OOS gates"
                if keep
                else "shallow decline subset reduced wrong-rate enough for watch-only research"
                if shallow_baseline["test"].get("n", 0) >= 50
                and shallow_baseline["test"].get("wrong_rate", 1) <= test_base["wrong_rate"] - 0.05
                else "macnica-like branch did not improve entry rate but low-wrong filter survived"
                if low_wrong_keep
                else "macnica-like branch did not clear temporal OOS gates"
            ),
        },
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
    }
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    _write_json(output_dir / "macnica_like_return_sell_probe.json", report)
    _write_json(output_root / "latest_macnica_like_return_sell_probe.json", {"run_root": str(output_dir), **report})
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--start-ymd", type=int, default=20150101)
    parser.add_argument("--max-rows", type=int, default=5000)
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
