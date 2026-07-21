from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from shared.runtime_stock_db_contract import resolve_runtime_stock_db_path
from tradex_short_shape_bad_avoidance_probe_v1 import _bucket, _daily_rows, _numeric_features_for
from tradex_short_shape_numeric_rule_probe_v1 import _apply_rule


AXIS_ID = "short_shape_numeric_rule_oos_v1"
DEFAULT_RULE_PATH = Path(r"G:\Tradex\short_shape_numeric_rule_probe_v1\latest_numeric_rule_probe.json")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_shape_numeric_rule_oos_v1")


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


def _scan_events(db_path: Path, *, start_ymd: int, max_rows: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    conn = duckdb.connect(str(db_path), read_only=True)
    codes = [str(row[0]) for row in conn.execute("SELECT DISTINCT code FROM daily_bars ORDER BY code").fetchall()]
    try:
        for code in codes:
            if len(rows) >= max_rows:
                break
            bars = _daily_rows(conn, code)
            if len(bars) < 280:
                continue
            closes = [float(row[4]) for row in bars]
            highs = [float(row[2]) for row in bars]
            lows = [float(row[3]) for row in bars]
            volumes = [float(row[5] or 0) for row in bars]
            last_taken = -999
            for index in range(80, len(bars) - 20):
                as_of = int(bars[index][0])
                if as_of < start_ymd:
                    continue
                if index - last_taken < 20:
                    continue
                ma20 = _ma(closes, index, 20)
                ma60 = _ma(closes, index, 60)
                vol20_prev = _ma(volumes, index - 1, 20)
                if ma20 is None or ma60 is None or not vol20_prev:
                    continue
                open_ = float(bars[index][1])
                high = float(bars[index][2])
                low = float(bars[index][3])
                close = float(bars[index][4])
                range_ = high - low
                if range_ <= 0:
                    continue
                upper_wick_ratio = (high - max(open_, close)) / range_
                previous_high20 = max(highs[index - 20 : index])
                high_zone_wick = close > previous_high20 and upper_wick_ratio >= 0.25 and volumes[index] / vol20_prev < 1.8
                ma_bear_pullback20 = close < ma20 and high >= ma20 and ma20 < ma60 and close < open_
                if not high_zone_wick and not ma_bear_pullback20:
                    continue
                future = bars[index + 1 : index + 21]
                ret20 = float(future[-1][4]) / close - 1.0
                mae20 = min(float(row[3]) for row in future) / close - 1.0
                mfe20 = max(float(row[2]) for row in future) / close - 1.0
                if ret20 <= -0.08 and mae20 <= -0.10:
                    outcome_class = "good_short_shape"
                elif ret20 >= 0.06 or mfe20 >= 0.08:
                    outcome_class = "bad_short_shape"
                else:
                    outcome_class = "neutral_shape"
                rows.append({
                    "sample_key": f"{code}:{as_of}",
                    "code": code,
                    "as_of": as_of,
                    "purpose_outcome_class": outcome_class,
                    "ret20": round(ret20, 8),
                    "MAE20": round(mae20, 8),
                    "MFE20": round(mfe20, 8),
                })
                last_taken = index
                if len(rows) >= max_rows:
                    break
    finally:
        conn.close()
    return rows


def run(*, rule_path: Path, output_root: Path, db_path: Path, start_ymd: int, max_rows: int) -> Path:
    rule_report = json.loads(rule_path.read_text(encoding="utf-8"))
    keep_rules = rule_report.get("keep_rules", [])
    events = _scan_events(db_path, start_ymd=start_ymd, max_rows=max_rows)
    x, rows = _numeric_features_for(events, db_path)
    baseline = _bucket(rows)
    evaluations = []
    for rule in keep_rules:
        clauses = rule["holdout"]["clauses"]
        mask = _apply_rule(x, clauses)
        selected = [row for row, keep in zip(rows, mask) if bool(keep)]
        evaluations.append({"clauses": clauses, "oos": _bucket(selected), "prior_train": rule["train"], "prior_holdout": rule["holdout"]})
    keep = [
        row for row in evaluations
        if row["oos"].get("n", 0) >= 30
        and row["oos"].get("bad_rate", 1) <= baseline["bad_rate"] - 0.05
        and row["oos"].get("good_rate", 0) >= baseline["good_rate"]
    ]
    report = {
        "schema_version": AXIS_ID,
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "source_rule_path": str(rule_path),
        "fixed_evaluation_conditions": {
            "oos_source": "confirmed non-yahoo daily_bars setup-family events",
            "start_ymd": start_ymd,
            "max_rows": max_rows,
            "feature_contract": "same numeric structure features and train-selected rules",
            "promotion_gate": "oos n>=30, bad_rate at least 5 points below baseline, good_rate >= baseline",
        },
        "oos_baseline": baseline,
        "evaluations": evaluations,
        "keep_rules": keep,
        "decision": {
            "candidate_local_decision": "keep_numeric_rule_after_large_oos" if keep else "drop_numeric_rule_after_large_oos",
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": "train-selected rule survived larger OOS gate" if keep else "train-selected rule did not survive larger OOS gate",
        },
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
    }
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    _write_json(output_dir / "numeric_rule_oos.json", report)
    _write_json(output_root / "latest_numeric_rule_oos.json", {"run_root": str(output_dir), **report})
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rule-path", type=Path, default=DEFAULT_RULE_PATH)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--db-path", type=Path, default=None)
    parser.add_argument("--start-ymd", type=int, default=20190101)
    parser.add_argument("--max-rows", type=int, default=1000)
    args = parser.parse_args()
    print(run(
        rule_path=args.rule_path,
        output_root=args.output_root,
        db_path=args.db_path or resolve_runtime_stock_db_path(),
        start_ymd=args.start_ymd,
        max_rows=args.max_rows,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
