from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.tradex_market_scene_signal_probe_v1 import (
    DEFAULT_OUTPUT_ROOT as SCENE_OUTPUT_ROOT,
    _judge_groups,
    _load_daily,
    _metrics,
    _now_tag,
    _write_json,
)
from scripts.tradex_visual_ai_entry_benchmark_v1 import _visual_features_from_ohlc, _visual_review
from shared.runtime_stock_db_contract import inspect_runtime_stock_db, resolve_runtime_stock_db_path
from tools.debug.trade_shape_classifier import classify_shape_from_bars

from collections import defaultdict
from datetime import datetime, timezone
from statistics import mean

import duckdb


DEFAULT_OUTPUT_ROOT = SCENE_OUTPUT_ROOT.parent / "market_scene_visual_proxy_probe_v1"


def _to_visual_bars(bars: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "ymd": row["ymd"],
            "open": row["o"],
            "high": row["h"],
            "low": row["l"],
            "close": row["c"],
            "volume": row["v"],
        }
        for row in bars
    ]


def run_probe(*, db_path: Path, output_root: Path, start_dt: int, end_dt: int, max_codes: int | None = None) -> dict[str, Any]:
    output_dir = output_root / f"{_now_tag()}-market_scene_visual_proxy_probe_v1"
    output_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        by_code = _load_daily(con, start_dt=start_dt, end_dt=end_dt, history=160, forward=20)
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
            if close <= 0:
                continue
            long_ret20 = float(bars[index + 20]["c"]) / close - 1.0
            short_ret20 = -long_ret20
            universe_by_date[ymd].append(long_ret20)
            window = bars[index - 159 : index + 1]
            shape = classify_shape_from_bars([[row["ymd"], row["o"], row["h"], row["l"], row["c"], row["v"]] for row in window])
            if shape.get("market_scene") in {None, "neutral_or_unclassified"}:
                continue
            visual_features = _visual_features_from_ohlc(_to_visual_bars(window[-60:]))
            visual_review = _visual_review(visual_features)
            events.append(
                {
                    "dt": ymd,
                    "code": code,
                    "market_scene": shape.get("market_scene"),
                    "trade_side": shape.get("trade_side"),
                    "action_bias": shape.get("action_bias"),
                    "shape_intent": shape.get("shape_intent"),
                    "entry_timing": shape.get("entry_timing"),
                    "visual_decision": visual_review.get("decision"),
                    "visual_entry_method": visual_review.get("entry_method"),
                    "visual_reasons": visual_review.get("reasons"),
                    "long_forward_return_20": long_ret20,
                    "short_forward_return_20": short_ret20,
                }
            )

    long_groups: dict[str, list[float]] = defaultdict(list)
    short_groups: dict[str, list[float]] = defaultdict(list)
    baseline_long_values: list[float] = []
    for event in events:
        key = f"{event['market_scene']}|{event['action_bias']}|{event['trade_side']}|{event['visual_decision']}"
        long_groups[key].append(float(event["long_forward_return_20"]))
        short_groups[key].append(float(event["short_forward_return_20"]))
        same_date = universe_by_date.get(int(event["dt"]), [])
        if same_date:
            baseline_long_values.append(mean(same_date))

    long_by_group = {key: _metrics(values) for key, values in sorted(long_groups.items())}
    short_by_group = {key: _metrics(values) for key, values in sorted(short_groups.items())}
    baseline_long = _metrics(baseline_long_values)
    baseline_short = _metrics([-value for value in baseline_long_values])
    long_decision = _judge_groups(long_by_group, baseline_long, side="long")
    short_decision = _judge_groups(short_by_group, baseline_short, side="short")
    result = {
        "schema_version": "tradex_market_scene_visual_proxy_probe_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "authoritative_result": True,
        "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {
            "source_table": "daily_bars",
            "start_dt": start_dt,
            "end_dt": end_dt,
            "signal": "market_scene/action_bias + OHLC visual proxy decision",
            "forward_return": "close_t_plus_20 / close_t - 1; short_return=-long_return",
            "baseline": "same_date_universe_mean_ret20",
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
            "event_count": len(events),
            "event_date_count": len({event["dt"] for event in events}),
            "visual_decision_counts": dict(sorted((key, sum(1 for event in events if event["visual_decision"] == key)) for key in {event["visual_decision"] for event in events})),
        },
        "compare": {
            "same_date_long_baseline": baseline_long,
            "same_date_short_baseline": baseline_short,
            "long_by_scene_action_side_visual": long_by_group,
            "short_by_scene_action_side_visual": short_by_group,
        },
        "candidate_local_decision": {"long": long_decision, "short": short_decision},
        "authoritative_rollup_decision": "hold" if long_decision["judgment"] == "hold" or short_decision["judgment"] == "hold" else "drop",
        "reason_type": "visual_scene_subsignals_require_followup" if long_decision["judgment"] == "hold" or short_decision["judgment"] == "hold" else "no_visual_scene_edge",
        "meemee_reflectable": False,
        "remaining_risks": [
            "OHLC visual proxy is not pixel screenshot analysis",
            "event study, not topK ranking replay",
            "same-date baseline is aggregate universe mean, not matched liquidity/rank control",
            "single-period in-sample probe",
        ],
    }
    compare_path = output_dir / "market_scene_visual_proxy_probe_compare.json"
    decision_path = output_dir / "market_scene_visual_proxy_probe_decision.json"
    ledger_path = output_dir / "market_scene_visual_proxy_probe_ledger.jsonl"
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
    print(json.dumps({"decision": result["authoritative_rollup_decision"], "reason_type": result["reason_type"], "candidate_local_decision": result["candidate_local_decision"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
