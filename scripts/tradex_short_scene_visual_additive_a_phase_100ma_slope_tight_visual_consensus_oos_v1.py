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
from scripts.tradex_short_scene_visual_additive_a_phase_100ma_slope_tight_oos_v1 import REQUIRED_SHAPE_INTENT
from scripts.tradex_short_scene_visual_additive_a_phase_slope_floor_oos_v1 import (
    IN_SAMPLE_END_DT,
    IN_SAMPLE_START_DT,
    _build_slope_floor_candidates,
    _month_rows,
    _oos_decision,
    _subset_groups,
    _subset_selected,
)
from scripts.tradex_short_scene_visual_additive_a_phase_slope_floor_tight_oos_v1 import MA20_SLOPE_10_FLOOR_TIGHT
from scripts.tradex_short_scene_visual_additive_a_phase_v1 import BASE_SIGNAL_KEY
from scripts.tradex_short_scene_visual_additive_candidate_v1 import TOP_K_VALUES, _select_one_per_date, _topk_compare
from scripts.tradex_short_scene_visual_candidate_gap_v1 import _write_json
from scripts.tradex_short_scene_visual_screenshot_gate_v1 import SHORT_ALLOW
from scripts.tradex_short_scene_visual_topk_probe_v1 import _load_signal_rows
from shared.runtime_stock_db_contract import inspect_runtime_stock_db, resolve_runtime_stock_db_path


DEFAULT_OUTPUT_ROOT = Path("G:/Tradex/a_phase_visual_consensus_oos_v1")


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _short_visual_consensus_decision(event: dict[str, Any]) -> str:
    if event.get("trade_side") != "short":
        return "reject"
    if event.get("shape_intent") != REQUIRED_SHAPE_INTENT:
        return "reject"
    if event.get("visual_decision") not in {"pullback_probe_candidate", *SHORT_ALLOW}:
        return "reject"
    slope = event.get("ma20_slope_10")
    if slope is None or float(slope) < MA20_SLOPE_10_FLOOR_TIGHT:
        return "reject"
    return "pass"


def _apply_visual_consensus(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for event in events:
        gate = _short_visual_consensus_decision(event)
        enriched = {
            **event,
            "screenshot_proxy_gate": gate,
            "screenshot_proxy_gate_reason": (
                "short_trade_side_shape_and_visual_proxy_consensus"
                if gate == "pass"
                else "short_visual_consensus_rejected"
            ),
        }
        if gate == "pass":
            filtered.append(enriched)
    return filtered


def run_probe(*, db_path: Path, output_root: Path, start_dt: int, end_dt: int, max_codes: int | None = None) -> dict[str, Any]:
    output_dir = output_root / f"{_now_tag()}-a_phase_visual_consensus_oos_v1"
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

    slope_floor_candidates = _build_slope_floor_candidates(
        by_code,
        sell_codes_by_date=sell_codes_by_date,
        start_dt=start_dt,
        end_dt=end_dt,
        floor=MA20_SLOPE_10_FLOOR_TIGHT,
    )
    shape_filtered_candidates = [event for event in slope_floor_candidates if event.get("shape_intent") == REQUIRED_SHAPE_INTENT]
    consensus_candidates = _apply_visual_consensus(shape_filtered_candidates)
    selected_by_date = _select_one_per_date(consensus_candidates)

    oos_start_dt = start_dt
    oos_end_dt = min(end_dt, IN_SAMPLE_START_DT - 1)
    oos_groups = _subset_groups(groups, oos_start_dt, oos_end_dt)
    oos_selected = _subset_selected(selected_by_date, oos_start_dt, oos_end_dt)
    in_sample_groups = _subset_groups(groups, IN_SAMPLE_START_DT, min(end_dt, IN_SAMPLE_END_DT))
    in_sample_selected = _subset_selected(selected_by_date, IN_SAMPLE_START_DT, min(end_dt, IN_SAMPLE_END_DT))
    oos_compare = {f"top{topk}": _topk_compare(oos_groups, oos_selected, topk=topk) for topk in TOP_K_VALUES}
    in_sample_compare = {f"top{topk}": _topk_compare(in_sample_groups, in_sample_selected, topk=topk) for topk in TOP_K_VALUES}
    monthly = _month_rows(groups, selected_by_date)
    oos_monthly = [row for row in monthly if row["month"] < "202603"]
    active_oos_months = [row for row in oos_monthly if (row.get("changed_top5_members_count") or 0) > 0]
    positive_oos_months = [row for row in active_oos_months if (row.get("top5_delta_mean") or 0.0) > 0.0]
    coverage = {
        "sell_candidate_rows": len(sell_rows),
        "sell_candidate_date_count": len(groups),
        "outside_additive_candidate_count": None,
        "outside_additive_candidate_count_computed": False,
        "slope_floor_candidate_count": len(slope_floor_candidates),
        "shape_filtered_candidate_count": len(shape_filtered_candidates),
        "visual_consensus_candidate_count": len(consensus_candidates),
        "visual_consensus_reject_count": len(shape_filtered_candidates) - len(consensus_candidates),
        "selected_additive_candidate_count": len(selected_by_date),
        "selected_additive_date_count": len(selected_by_date),
        "oos_sell_candidate_rows": sum(len(rows) for rows in oos_groups.values()),
        "oos_sell_candidate_date_count": len(oos_groups),
        "oos_selected_additive_date_count": len(oos_selected),
        "oos_active_month_count": len(active_oos_months),
        "oos_positive_active_month_count": len(positive_oos_months),
        "oos_positive_active_month_rate": round(len(positive_oos_months) / len(active_oos_months), 6) if active_oos_months else None,
        "in_sample_selected_additive_date_count": len(in_sample_selected),
        "ma20_slope_10_floor": MA20_SLOPE_10_FLOOR_TIGHT,
        "required_shape_intent": REQUIRED_SHAPE_INTENT,
        "base_signal_key": BASE_SIGNAL_KEY,
        "short_visual_allow": sorted(SHORT_ALLOW),
    }
    decision = _oos_decision(oos_compare, monthly, coverage)
    result = {
        "schema_version": "tradex_short_scene_visual_additive_a_phase_100ma_slope_tight_visual_consensus_oos_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "authoritative_result": True,
        "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {
            "source_table": "signal_decision_daily + daily_bars outside-gap scan",
            "start_dt": start_dt,
            "end_dt": end_dt,
            "oos_start_dt": oos_start_dt,
            "oos_end_dt": oos_end_dt,
            "in_sample_keep_period": [IN_SAMPLE_START_DT, IN_SAMPLE_END_DT],
            "side": "sell",
            "entry_qualified_only": True,
            "same_dates_as_sell_candidate_pool": True,
            "same_top_k": list(TOP_K_VALUES),
            "same_cost_slippage": "flat_zero_cost",
            "changed_axis": "a_phase_100ma_slope_tight_plus_short_visual_consensus",
            "selection_rule": "previous keep rule plus short visual consensus proxy; at most one outside-gap candidate per sell-candidate date ranked by lower ma20_slope_10",
            "screenshot_feedback_source": "current screenshot gate rejected 4384 because screenshot short review was watch while long review was probe_candidate",
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
        "compare": {"oos": oos_compare, "in_sample_reference": in_sample_compare},
        "monthly_stability": {
            "rows": monthly,
            "oos_active_month_count": len(active_oos_months),
            "oos_positive_active_month_count": len(positive_oos_months),
            "oos_positive_active_month_rate": coverage["oos_positive_active_month_rate"],
        },
        "observed_branching": {
            "changed_top5_members_count": oos_compare["top5"]["changed_member_count_total"],
            "changed_top10_members_count": oos_compare["top10"]["changed_member_count_total"],
            "changed_rank_count": oos_compare["top5"]["changed_member_count_total"],
            "selection_divergence_reason": "historical_oos_outside_gap_a_phase_candidate_added_only_when_100ma_rejection_slope_tight_and_short_visual_consensus_pass",
        },
        "authoritative_rollup_decision": decision["judgment"],
        "reason_type": decision["reason_type"],
        "candidate_generation_challenger_created": len(oos_selected) > 0,
        "meemee_reflectable": False,
        "remaining_risks": [
            "visual consensus is OHLC proxy informed by a current screenshot rejection, not historical pixel screenshots",
            "historical OOS is pre-selection holdout, not forward live paper replay",
            "additive candidates do not have champion-native score or rank",
            "full pre-floor outside candidate count is not computed in this OOS performance path",
        ],
    }
    compare_path = output_dir / "compare.json"
    decision_path = output_dir / "decision.json"
    monthly_path = output_dir / "monthly.json"
    ledger_path = output_dir / "ledger.jsonl"
    result["artifacts"] = {
        "output_dir": str(output_dir),
        "compare_json": str(compare_path),
        "decision_json": str(decision_path),
        "monthly_json": str(monthly_path),
        "ledger_jsonl": str(ledger_path),
        "artifact_complete": str(output_dir / "_ARTIFACT_COMPLETE.json"),
    }
    _write_json(compare_path, result)
    _write_json(
        decision_path,
        {k: result[k] for k in ("schema_version", "authoritative_rollup_decision", "reason_type", "candidate_generation_challenger_created", "meemee_reflectable", "remaining_risks", "artifacts")},
    )
    _write_json(monthly_path, result["monthly_stability"])
    selected_codes = {(dt, str(row["code"])) for dt, row in selected_by_date.items()}
    with ledger_path.open("w", encoding="utf-8") as fh:
        for event in shape_filtered_candidates:
            key = (int(event["dt"]), str(event["code"]))
            gate = _short_visual_consensus_decision(event)
            row = {
                **event,
                "passes_ma20_slope_10_floor": True,
                "passes_100ma_rejection_filter": event.get("shape_intent") == REQUIRED_SHAPE_INTENT,
                "screenshot_proxy_gate": gate,
                "selected_additive_candidate": key in selected_codes,
                "validation_block": "in_sample_reference" if IN_SAMPLE_START_DT <= int(event["dt"]) <= IN_SAMPLE_END_DT else "historical_oos",
            }
            fh.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
    _write_json(output_dir / "_ARTIFACT_COMPLETE.json", {"complete": True, **result["artifacts"]})
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=resolve_runtime_stock_db_path())
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--start-dt", type=int, default=20250101)
    parser.add_argument("--end-dt", type=int, default=20260331)
    parser.add_argument("--max-codes", type=int, default=None)
    args = parser.parse_args()
    result = run_probe(db_path=args.db_path, output_root=args.output_root, start_dt=args.start_dt, end_dt=args.end_dt, max_codes=args.max_codes)
    print(json.dumps(result["artifacts"], ensure_ascii=False, indent=2))
    print(json.dumps(result["coverage"], ensure_ascii=False, indent=2))
    print(json.dumps({"decision": result["authoritative_rollup_decision"], "reason_type": result["reason_type"], "observed_branching": result["observed_branching"], "oos_top5": result["compare"]["oos"]["top5"], "oos_top10": result["compare"]["oos"]["top10"], "monthly_stability": {k: v for k, v in result["monthly_stability"].items() if k != "rows"}}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
