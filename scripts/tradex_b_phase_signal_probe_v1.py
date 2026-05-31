from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any

import duckdb

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.runtime_stock_db_contract import inspect_runtime_stock_db, resolve_runtime_stock_db_path
from tools.debug.trade_shape_classifier import classify_shape_from_bars


DEFAULT_OUTPUT_ROOT = Path("G:/Tradex/b_phase_signal_probe_v1")


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _round(value: float | None, digits: int = 6) -> float | None:
    return None if value is None else round(value, digits)


def _metrics(values: list[float]) -> dict[str, Any]:
    return {
        "count": len(values),
        "mean_ret20": _round(mean(values) if values else None),
        "median_ret20": _round(median(values) if values else None),
        "hit_rate_20": _round(sum(1 for value in values if value > 0) / len(values) if values else None),
        "bad_loser_rate_20": _round(sum(1 for value in values if value <= -0.05) / len(values) if values else None),
        "severe_loser_rate_20": _round(sum(1 for value in values if value <= -0.10) / len(values) if values else None),
    }


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _load_daily(con: duckdb.DuckDBPyConnection, *, start_dt: int, end_dt: int) -> dict[str, list[dict[str, Any]]]:
    rows = con.execute(
        """
        WITH normalized AS (
            SELECT
                code,
                CASE
                    WHEN length(CAST(abs(date) AS VARCHAR)) = 8
                        THEN CAST(date AS INTEGER)
                    ELSE CAST(strftime(to_timestamp(CAST(date AS BIGINT)), '%Y%m%d') AS INTEGER)
                END AS ymd,
                o, h, l, c, v
            FROM daily_bars
        )
        SELECT code, ymd, o, h, l, c, v
        FROM normalized
        WHERE ymd BETWEEN ? AND ?
        ORDER BY code, ymd
        """,
        [start_dt - 20000, end_dt + 10000],
    ).fetchall()
    by_code: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for code, ymd, open_, high, low, close, volume in rows:
        if open_ is None or high is None or low is None or close is None:
            continue
        by_code[str(code)].append(
            {"ymd": int(ymd), "o": float(open_), "h": float(high), "l": float(low), "c": float(close), "v": float(volume or 0)}
        )
    return {code: bars for code, bars in by_code.items() if len(bars) >= 181}


def _decision(by_timing: dict[str, dict[str, Any]], baseline: dict[str, Any]) -> dict[str, Any]:
    keep: list[str] = []
    hold: list[str] = []
    drop: list[str] = []
    baseline_mean = baseline.get("mean_ret20") or 0.0
    baseline_bad = baseline.get("bad_loser_rate_20") or 0.0
    for timing, metrics in sorted(by_timing.items()):
        count = metrics.get("count") or 0
        mean_delta = (metrics.get("mean_ret20") or 0.0) - baseline_mean
        bad_delta = (metrics.get("bad_loser_rate_20") or 0.0) - baseline_bad
        if count < 30:
            hold.append(timing)
        elif mean_delta > 0.01 and bad_delta <= 0.02:
            keep.append(timing)
        elif timing.endswith("wait"):
            drop.append(timing)
        elif mean_delta > 0.0:
            hold.append(timing)
        else:
            drop.append(timing)
    if keep:
        return {"judgment": "hold", "reason_type": "has_keep_subsignal_but_requires_followup", "keep_entry_timing": keep, "hold_entry_timing": hold, "drop_entry_timing": drop}
    if hold:
        return {"judgment": "hold", "reason_type": "weak_or_sparse_subsignals", "keep_entry_timing": keep, "hold_entry_timing": hold, "drop_entry_timing": drop}
    return {"judgment": "drop", "reason_type": "no_useful_b_phase_subsignal", "keep_entry_timing": keep, "hold_entry_timing": hold, "drop_entry_timing": drop}


def run_probe(*, db_path: Path, output_root: Path, start_dt: int, end_dt: int, max_codes: int | None = None) -> dict[str, Any]:
    output_dir = output_root / f"{_now_tag()}-b_phase_signal_probe_v1"
    output_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        by_code = _load_daily(con, start_dt=start_dt, end_dt=end_dt)
    finally:
        con.close()
    if max_codes is not None:
        by_code = dict(list(sorted(by_code.items()))[:max_codes])

    events: list[dict[str, Any]] = []
    universe_by_date: dict[int, list[float]] = defaultdict(list)
    for code, bars in by_code.items():
        for index in range(160, len(bars) - 20):
            ymd = int(bars[index]["ymd"])
            if ymd < start_dt or ymd > end_dt:
                continue
            close = float(bars[index]["c"])
            ret20 = float(bars[index + 20]["c"]) / close - 1.0 if close > 0 else None
            if ret20 is None:
                continue
            universe_by_date[ymd].append(ret20)
            shape = classify_shape_from_bars([[row["ymd"], row["o"], row["h"], row["l"], row["c"], row["v"]] for row in bars[index - 159 : index + 1]])
            if not shape.get("is_b_phase_setup"):
                continue
            events.append(
                {
                    "dt": ymd,
                    "code": code,
                    "shape_intent": shape.get("shape_intent"),
                    "entry_timing": shape.get("entry_timing"),
                    "forward_return_20": ret20,
                    "metrics": shape.get("metrics"),
                    "reasons": shape.get("reasons"),
                }
            )

    by_shape_values: dict[str, list[float]] = defaultdict(list)
    by_timing_values: dict[str, list[float]] = defaultdict(list)
    baseline_values: list[float] = []
    for event in events:
        by_shape_values[str(event["shape_intent"])].append(float(event["forward_return_20"]))
        by_timing_values[str(event["entry_timing"])].append(float(event["forward_return_20"]))
        same_date = universe_by_date.get(int(event["dt"]), [])
        if same_date:
            baseline_values.append(mean(same_date))

    by_timing = {key: _metrics(values) for key, values in sorted(by_timing_values.items())}
    result_decision = _decision(by_timing, _metrics(baseline_values))
    result = {
        "schema_version": "tradex_b_phase_signal_probe_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "authoritative_result": True,
        "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {
            "source_table": "daily_bars",
            "start_dt": start_dt,
            "end_dt": end_dt,
            "signal": "b_phase_*",
            "forward_return": "close_t_plus_20 / close_t - 1",
            "baseline": "same_date_universe_mean_ret20",
            "cost_slippage": "flat_zero_cost",
        },
        "scope": {
            "tradex_only": True,
            "meemee_ranking_changed": False,
            "meemee_ui_changed": False,
            "runtime_db_written": False,
            "silent_fallback_used": False,
        },
        "runtime_stock_db_status": inspect_runtime_stock_db(runtime_db_path=db_path),
        "coverage": {"code_count": len(by_code), "event_count": len(events), "event_date_count": len({event["dt"] for event in events})},
        "compare": {
            "events": _metrics([float(event["forward_return_20"]) for event in events]),
            "same_date_baseline": _metrics(baseline_values),
            "by_shape_intent": {key: _metrics(values) for key, values in sorted(by_shape_values.items())},
            "by_entry_timing": by_timing,
        },
        "authoritative_rollup_decision": result_decision["judgment"],
        "reason_type": result_decision["reason_type"],
        "candidate_local_decision": result_decision,
        "meemee_reflectable": False,
        "remaining_risks": [
            "event study, not topK ranking replay",
            "same-date baseline is aggregate universe mean, not matched liquidity/rank control",
            "single-period in-sample probe",
        ],
    }
    compare_path = output_dir / "b_phase_signal_probe_compare.json"
    decision_path = output_dir / "b_phase_signal_probe_decision.json"
    ledger_path = output_dir / "b_phase_signal_probe_ledger.jsonl"
    result["artifacts"] = {
        "output_dir": str(output_dir),
        "compare_json": str(compare_path),
        "decision_json": str(decision_path),
        "ledger_jsonl": str(ledger_path),
        "artifact_complete": str(output_dir / "_ARTIFACT_COMPLETE.json"),
    }
    _write_json(compare_path, result)
    _write_json(decision_path, {k: result[k] for k in ("schema_version", "authoritative_rollup_decision", "reason_type", "candidate_local_decision", "meemee_reflectable", "remaining_risks", "artifacts")})
    with ledger_path.open("w", encoding="utf-8") as fh:
        for event in events:
            fh.write(json.dumps(event, ensure_ascii=False, default=str) + "\n")
    _write_json(output_dir / "_ARTIFACT_COMPLETE.json", {"complete": True, **result["artifacts"]})
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=resolve_runtime_stock_db_path())
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--start-dt", type=int, default=20260301)
    parser.add_argument("--end-dt", type=int, default=20260430)
    parser.add_argument("--max-codes", type=int, default=None)
    args = parser.parse_args()
    result = run_probe(db_path=args.db_path, output_root=args.output_root, start_dt=args.start_dt, end_dt=args.end_dt, max_codes=args.max_codes)
    print(json.dumps(result["artifacts"], ensure_ascii=False, indent=2))
    print(json.dumps(result["coverage"], ensure_ascii=False, indent=2))
    print(json.dumps({"decision": result["authoritative_rollup_decision"], "reason_type": result["reason_type"], "candidate_local_decision": result["candidate_local_decision"], "by_entry_timing": result["compare"]["by_entry_timing"], "baseline": result["compare"]["same_date_baseline"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
