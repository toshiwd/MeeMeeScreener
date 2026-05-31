from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.tradex_market_scene_signal_probe_v1 import _load_daily
from scripts.tradex_short_scene_visual_additive_a_phase_v1 import BASE_SIGNAL_KEY, _build_outside_candidates
from scripts.tradex_short_scene_visual_additive_candidate_v1 import TOP_K_VALUES, _decision, _select_one_per_date, _topk_compare
from scripts.tradex_short_scene_visual_candidate_gap_v1 import _write_json
from scripts.tradex_short_scene_visual_topk_probe_v1 import _load_signal_rows
from shared.runtime_stock_db_contract import inspect_runtime_stock_db, resolve_runtime_stock_db_path


DEFAULT_OUTPUT_ROOT = Path("G:/Tradex/short_scene_visual_additive_a_phase_slope_floor_v1")
MA20_SLOPE_10_FLOOR = -0.01


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _apply_slope_floor(events: list[dict[str, Any]], *, floor: float = MA20_SLOPE_10_FLOOR) -> list[dict[str, Any]]:
    kept: list[dict[str, Any]] = []
    for event in events:
        slope = event.get("ma20_slope_10")
        if slope is None:
            continue
        if float(slope) >= floor:
            kept.append(event)
    return kept


def run_probe(*, db_path: Path, output_root: Path, start_dt: int, end_dt: int, max_codes: int | None = None) -> dict[str, Any]:
    output_dir = output_root / f"{_now_tag()}-short_scene_visual_additive_a_phase_slope_floor_v1"
    output_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        sell_rows = _load_signal_rows(con, start_dt=start_dt, end_dt=end_dt)
        by_code = _load_daily(con, start_dt=start_dt, end_dt=end_dt, history=160, forward=20)
    finally:
        con.close()
    if max_codes is not None:
        by_code = dict(list(sorted(by_code.items()))[:max_codes])

    groups: dict[int, list[dict[str, Any]]] = defaultdict(list)
    sell_codes_by_date: dict[int, set[str]] = defaultdict(set)
    for row in sell_rows:
        groups[int(row["dt"])].append({**row, "additive_candidate": False})
        sell_codes_by_date[int(row["dt"])].add(str(row["code"]))

    outside_candidates = _build_outside_candidates(by_code, sell_codes_by_date=sell_codes_by_date, start_dt=start_dt, end_dt=end_dt)
    slope_floor_candidates = _apply_slope_floor(outside_candidates)
    selected_by_date = _select_one_per_date(slope_floor_candidates)
    compare = {f"top{topk}": _topk_compare(groups, selected_by_date, topk=topk) for topk in TOP_K_VALUES}
    coverage = {
        "sell_candidate_rows": len(sell_rows),
        "sell_candidate_date_count": len(groups),
        "outside_additive_candidate_count": len(outside_candidates),
        "slope_floor_candidate_count": len(slope_floor_candidates),
        "outside_additive_candidate_date_count": len({int(row["dt"]) for row in outside_candidates}),
        "slope_floor_candidate_date_count": len({int(row["dt"]) for row in slope_floor_candidates}),
        "selected_additive_candidate_count": len(selected_by_date),
        "selected_additive_date_count": len(selected_by_date),
        "ma20_slope_10_floor": MA20_SLOPE_10_FLOOR,
        "base_signal_key": BASE_SIGNAL_KEY,
    }
    decision = _decision(compare, coverage)
    result = {
        "schema_version": "tradex_short_scene_visual_additive_a_phase_slope_floor_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "authoritative_result": True,
        "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {
            "source_table": "signal_decision_daily + daily_bars outside-gap scan",
            "start_dt": start_dt,
            "end_dt": end_dt,
            "side": "sell",
            "entry_qualified_only": True,
            "same_dates_as_sell_candidate_pool": True,
            "same_top_k": list(TOP_K_VALUES),
            "same_cost_slippage": "flat_zero_cost",
            "changed_axis": "a_phase_ma20_slope_10_floor",
            "selection_rule": "filter ma20_slope_10 >= -0.01, then at most one outside-gap candidate per sell-candidate date ranked by lower ma20_slope_10",
            "additive_score_policy": "selected candidate receives date max tradePriorityScore + 0.0001 for fixed branching stress-test",
            "signal_key": BASE_SIGNAL_KEY,
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
        "coverage": coverage,
        "compare": compare,
        "observed_branching": {
            "changed_top5_members_count": compare["top5"]["changed_member_count_total"],
            "changed_top10_members_count": compare["top10"]["changed_member_count_total"],
            "changed_rank_count": compare["top5"]["changed_member_count_total"],
            "selection_divergence_reason": "outside_gap_a_phase_candidate_added_only_when_ma20_slope_10_not_too_steep",
        },
        "authoritative_rollup_decision": decision["judgment"],
        "reason_type": decision["reason_type"],
        "candidate_generation_challenger_created": len(selected_by_date) > 0,
        "meemee_reflectable": False,
        "remaining_risks": [
            "additive candidates do not have champion-native score or rank",
            "additive_score_policy is a fixed stress-test, not a production scoring implementation",
            "OHLC visual proxy is not pixel screenshot analysis",
            "single-period in-sample probe",
            "ma20_slope_10_floor was selected from same-period diagnostics and needs out-of-sample validation",
        ],
    }
    compare_path = output_dir / "short_scene_visual_additive_a_phase_slope_floor_compare.json"
    decision_path = output_dir / "short_scene_visual_additive_a_phase_slope_floor_decision.json"
    ledger_path = output_dir / "short_scene_visual_additive_a_phase_slope_floor_ledger.jsonl"
    result["artifacts"] = {
        "output_dir": str(output_dir),
        "compare_json": str(compare_path),
        "decision_json": str(decision_path),
        "ledger_jsonl": str(ledger_path),
        "artifact_complete": str(output_dir / "_ARTIFACT_COMPLETE.json"),
    }
    _write_json(compare_path, result)
    _write_json(
        decision_path,
        {k: result[k] for k in ("schema_version", "authoritative_rollup_decision", "reason_type", "candidate_generation_challenger_created", "meemee_reflectable", "remaining_risks", "artifacts")},
    )
    with ledger_path.open("w", encoding="utf-8") as fh:
        slope_floor_codes = {(int(row["dt"]), str(row["code"])) for row in slope_floor_candidates}
        selected_codes = {(dt, str(row["code"])) for dt, row in selected_by_date.items()}
        for event in outside_candidates:
            key = (int(event["dt"]), str(event["code"]))
            row = {
                **event,
                "passes_ma20_slope_10_floor": key in slope_floor_codes,
                "selected_additive_candidate": key in selected_codes,
            }
            fh.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
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
    print(json.dumps({"decision": result["authoritative_rollup_decision"], "reason_type": result["reason_type"], "observed_branching": result["observed_branching"], "top5": result["compare"]["top5"], "top10": result["compare"]["top10"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
