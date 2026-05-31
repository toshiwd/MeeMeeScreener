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

from scripts.tradex_market_scene_signal_probe_v1 import _load_daily
from scripts.tradex_short_scene_visual_topk_probe_v1 import BAD_LOSER_THRESHOLD, KEEP_KEYS, SEVERE_LOSER_THRESHOLD, _load_signal_rows
from scripts.tradex_visual_ai_entry_benchmark_v1 import _visual_features_from_ohlc, _visual_review
from shared.runtime_stock_db_contract import inspect_runtime_stock_db, resolve_runtime_stock_db_path
from tools.debug.trade_shape_classifier import classify_shape_from_bars


DEFAULT_OUTPUT_ROOT = Path("G:/Tradex/short_scene_visual_candidate_gap_v1")


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _round(value: float | None, digits: int = 6) -> float | None:
    return None if value is None else round(value, digits)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _to_visual_bars(bars: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {"ymd": row["ymd"], "open": row["o"], "high": row["h"], "low": row["l"], "close": row["c"], "volume": row["v"]}
        for row in bars
    ]


def _metrics(values: list[float]) -> dict[str, Any]:
    return {
        "count": len(values),
        "mean_short_ret20": _round(mean(values) if values else None),
        "median_short_ret20": _round(median(values) if values else None),
        "hit_rate_20": _round(sum(1 for value in values if value > 0) / len(values) if values else None),
        "bad_loser_rate_20": _round(sum(1 for value in values if value <= BAD_LOSER_THRESHOLD) / len(values) if values else None),
        "severe_loser_rate_20": _round(sum(1 for value in values if value <= SEVERE_LOSER_THRESHOLD) / len(values) if values else None),
    }


def _key(shape: dict[str, Any], visual: dict[str, Any]) -> str:
    return f"{shape.get('market_scene')}|{shape.get('action_bias')}|{shape.get('trade_side')}|{visual.get('decision')}"


def _decision(outside_metrics: dict[str, Any], champion_metrics: dict[str, Any], outside_count: int, same_date_coverage: int) -> dict[str, Any]:
    if outside_count == 0:
        return {"judgment": "drop", "reason_type": "no_candidate_gap_events"}
    if same_date_coverage < 5:
        return {"judgment": "hold", "reason_type": "insufficient_same_date_breadth"}
    mean_delta = (outside_metrics.get("mean_short_ret20") or 0.0) - (champion_metrics.get("mean_short_ret20") or 0.0)
    bad_delta = (outside_metrics.get("bad_loser_rate_20") or 0.0) - (champion_metrics.get("bad_loser_rate_20") or 0.0)
    if outside_count >= 30 and mean_delta > 0.01 and bad_delta <= 0.02:
        return {"judgment": "hold", "reason_type": "candidate_generation_gap_positive_requires_replay"}
    if mean_delta > 0 and bad_delta <= 0.05:
        return {"judgment": "hold", "reason_type": "candidate_generation_gap_weak_positive"}
    return {"judgment": "drop", "reason_type": "candidate_gap_not_better_than_sell_pool"}


def run_probe(*, db_path: Path, output_root: Path, start_dt: int, end_dt: int, max_codes: int | None = None) -> dict[str, Any]:
    output_dir = output_root / f"{_now_tag()}-short_scene_visual_candidate_gap_v1"
    output_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        sell_rows = _load_signal_rows(con, start_dt=start_dt, end_dt=end_dt)
        by_code = _load_daily(con, start_dt=start_dt, end_dt=end_dt, history=160, forward=20)
    finally:
        con.close()
    if max_codes is not None:
        by_code = dict(list(sorted(by_code.items()))[:max_codes])

    sell_codes_by_date: dict[int, set[str]] = defaultdict(set)
    sell_short_returns: list[float] = []
    for row in sell_rows:
        sell_codes_by_date[int(row["dt"])].add(str(row["code"]))
        if row.get("forward_return_20") is not None:
            sell_short_returns.append(float(row["forward_return_20"]))

    outside_events: list[dict[str, Any]] = []
    inside_events: list[dict[str, Any]] = []
    key_counts: dict[str, int] = defaultdict(int)
    for code, bars in by_code.items():
        for index in range(160, len(bars) - 20):
            ymd = int(bars[index]["ymd"])
            if ymd < start_dt or ymd > end_dt or ymd not in sell_codes_by_date:
                continue
            close = float(bars[index]["c"])
            if close <= 0:
                continue
            window = bars[index - 159 : index + 1]
            shape = classify_shape_from_bars([[row["ymd"], row["o"], row["h"], row["l"], row["c"], row["v"]] for row in window])
            visual = _visual_review(_visual_features_from_ohlc(_to_visual_bars(window[-60:])))
            key = _key(shape, visual)
            if key not in KEEP_KEYS:
                continue
            short_ret20 = -(float(bars[index + 20]["c"]) / close - 1.0)
            event = {
                "dt": ymd,
                "code": code,
                "scene_visual_key": key,
                "short_forward_return_20": short_ret20,
                "in_existing_sell_pool": code in sell_codes_by_date[ymd],
                "shape_intent": shape.get("shape_intent"),
                "entry_timing": shape.get("entry_timing"),
                "visual_decision": visual.get("decision"),
            }
            key_counts[key] += 1
            if event["in_existing_sell_pool"]:
                inside_events.append(event)
            else:
                outside_events.append(event)

    outside_values = [float(event["short_forward_return_20"]) for event in outside_events]
    inside_values = [float(event["short_forward_return_20"]) for event in inside_events]
    outside_metrics = _metrics(outside_values)
    champion_metrics = _metrics(sell_short_returns)
    same_date_coverage = len({event["dt"] for event in outside_events})
    decision = _decision(outside_metrics, champion_metrics, len(outside_events), same_date_coverage)
    result = {
        "schema_version": "tradex_short_scene_visual_candidate_gap_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "authoritative_result": True,
        "research_phase": "branching_generation",
        "fixed_evaluation_conditions": {
            "source_table": "daily_bars + signal_decision_daily",
            "start_dt": start_dt,
            "end_dt": end_dt,
            "same_dates_as_sell_candidate_pool": True,
            "signal_keys": sorted(KEEP_KEYS),
            "forward_return": "short_return=-(close_t_plus_20/close_t-1)",
            "cost_slippage": "flat_zero_cost",
        },
        "scope": {
            "tradex_only": True,
            "meemee_ranking_changed": False,
            "meemee_ui_changed": False,
            "runtime_db_written": False,
            "silent_fallback_used": False,
            "research_fallback_used": False,
        },
        "runtime_stock_db_status": inspect_runtime_stock_db(runtime_db_path=db_path),
        "coverage": {
            "code_count": len(by_code),
            "sell_candidate_rows": len(sell_rows),
            "sell_candidate_date_count": len(sell_codes_by_date),
            "outside_gap_event_count": len(outside_events),
            "inside_sell_pool_event_count": len(inside_events),
            "outside_gap_date_count": same_date_coverage,
            "scene_visual_key_counts": dict(sorted(key_counts.items())),
        },
        "compare": {
            "existing_sell_pool": champion_metrics,
            "inside_sell_pool_matching_signal": _metrics(inside_values),
            "outside_gap_events": outside_metrics,
            "delta_vs_existing_sell_pool": {
                "mean_short_ret20": _round((outside_metrics.get("mean_short_ret20") or 0.0) - (champion_metrics.get("mean_short_ret20") or 0.0)),
                "bad_loser_rate_20": _round((outside_metrics.get("bad_loser_rate_20") or 0.0) - (champion_metrics.get("bad_loser_rate_20") or 0.0)),
            },
        },
        "authoritative_rollup_decision": decision["judgment"],
        "reason_type": decision["reason_type"],
        "candidate_generation_challenger_created": False,
        "meemee_reflectable": False,
        "remaining_risks": [
            "candidate gap event study, not additive candidate replay",
            "OHLC visual proxy is not pixel screenshot analysis",
            "single-period in-sample probe",
        ],
    }
    compare_path = output_dir / "short_scene_visual_candidate_gap_compare.json"
    decision_path = output_dir / "short_scene_visual_candidate_gap_decision.json"
    ledger_path = output_dir / "short_scene_visual_candidate_gap_ledger.jsonl"
    result["artifacts"] = {
        "output_dir": str(output_dir),
        "compare_json": str(compare_path),
        "decision_json": str(decision_path),
        "ledger_jsonl": str(ledger_path),
        "artifact_complete": str(output_dir / "_ARTIFACT_COMPLETE.json"),
    }
    _write_json(compare_path, result)
    _write_json(decision_path, {k: result[k] for k in ("schema_version", "authoritative_rollup_decision", "reason_type", "candidate_generation_challenger_created", "meemee_reflectable", "remaining_risks", "artifacts")})
    with ledger_path.open("w", encoding="utf-8") as fh:
        for event in outside_events:
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
    print(json.dumps({"decision": result["authoritative_rollup_decision"], "reason_type": result["reason_type"], "compare": result["compare"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
