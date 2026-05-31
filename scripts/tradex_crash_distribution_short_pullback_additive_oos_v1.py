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
from scripts.tradex_short_scene_visual_additive_a_phase_slope_floor_oos_v1 import (
    IN_SAMPLE_END_DT,
    IN_SAMPLE_START_DT,
    _month_rows,
    _subset_groups,
    _subset_selected,
)
from scripts.tradex_short_scene_visual_additive_candidate_v1 import TOP_K_VALUES, _ma20_slope_10, _topk_compare
from scripts.tradex_short_scene_visual_candidate_gap_v1 import _key, _to_visual_bars, _write_json
from scripts.tradex_short_scene_visual_topk_probe_v1 import _load_signal_rows
from scripts.tradex_visual_ai_entry_benchmark_v1 import _visual_features_from_ohlc, _visual_review
from shared.runtime_stock_db_contract import inspect_runtime_stock_db, resolve_runtime_stock_db_path
from tools.debug.trade_shape_classifier import classify_shape_from_bars


DEFAULT_OUTPUT_ROOT = Path("G:/Tradex/crash_distribution_short_pullback_additive_oos_v1")
BASE_SIGNAL_KEY = "crash_or_distribution_phase|wait_for_short_trigger|watch|pullback_probe_candidate"
TARGET_MARKET_SCENE = "crash_or_distribution_phase"
TARGET_ACTION_BIAS = "wait_for_short_trigger"
TARGET_TRADE_SIDE = "watch"
TARGET_VISUAL_DECISION = "pullback_probe_candidate"


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _round(value: float | None, digits: int = 6) -> float | None:
    return None if value is None else round(value, digits)


def _decision(compare: dict[str, Any], monthly: list[dict[str, Any]], coverage: dict[str, Any]) -> dict[str, str]:
    top5 = compare["top5"]
    top10 = compare["top10"]
    top5_delta = top5["additive_delta"]
    top10_delta = top10["additive_delta"]
    changed = int(top5.get("changed_member_count_total") or 0)
    active_months = [row for row in monthly if row["month"] < "202603" and (row.get("changed_top5_members_count") or 0) > 0]
    positive_rate = (
        sum(1 for row in active_months if (row.get("top5_delta_mean") or 0.0) > 0.0) / len(active_months)
        if active_months
        else 0.0
    )
    top5_mean = top5_delta.get("forward_return_20_mean") or 0.0
    top10_mean = top10_delta.get("forward_return_20_mean") or 0.0
    top5_bad = top5_delta.get("bad_loser_rate_20") or 0.0
    top5_severe = top5_delta.get("severe_loser_rate_20") or 0.0
    if coverage["oos_selected_additive_date_count"] < 10 or changed < 20:
        return {"judgment": "hold", "reason_type": "insufficient_oos_branching_or_breadth"}
    if top5_mean > 0.0 and top10_mean >= -0.001 and top5_bad <= 0.0 and top5_severe <= 0.0 and positive_rate >= 0.5:
        return {"judgment": "keep_for_next_probe", "reason_type": "oos_top5_improves_top10_stable_adverse_not_worse"}
    if top5_mean > 0.0 and top5_bad <= 0.0 and top5_severe <= 0.0:
        return {"judgment": "hold", "reason_type": "top5_improves_but_top10_or_month_stability_insufficient"}
    if top5_mean > 0.0:
        return {"judgment": "drop", "reason_type": "top5_improves_but_adverse_move_worsens"}
    return {"judgment": "drop", "reason_type": "top5_primary_metric_not_improved"}


def _select_one_per_date(events: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    selected: dict[int, dict[str, Any]] = {}
    for event in events:
        dt = int(event["dt"])
        current = selected.get(dt)
        event_rank = (
            float(event.get("ma20_slope_10") or 9.0),
            -float(event.get("latest_price_position_pct") or 0.0),
            str(event["code"]),
        )
        if current is None:
            selected[dt] = event
            continue
        current_rank = (
            float(current.get("ma20_slope_10") or 9.0),
            -float(current.get("latest_price_position_pct") or 0.0),
            str(current["code"]),
        )
        if event_rank < current_rank:
            selected[dt] = event
    return selected


def _build_candidates(
    by_code: dict[str, list[dict[str, Any]]],
    *,
    sell_codes_by_date: dict[int, set[str]],
    start_dt: int,
    end_dt: int,
) -> list[dict[str, Any]]:
    sell_dates = set(sell_codes_by_date)
    events: list[dict[str, Any]] = []
    for code, bars in by_code.items():
        for index, bar in enumerate(bars):
            if index < 160 or index >= len(bars) - 20:
                continue
            ymd = int(bar["ymd"])
            if ymd < start_dt or ymd > end_dt or ymd not in sell_dates or code in sell_codes_by_date[ymd]:
                continue
            close = float(bar["c"])
            if close <= 0:
                continue
            window = bars[index - 159 : index + 1]
            visual_features = _visual_features_from_ohlc(_to_visual_bars(window[-60:]))
            visual = _visual_review(visual_features)
            if visual.get("decision") != TARGET_VISUAL_DECISION:
                continue
            shape = classify_shape_from_bars([[row["ymd"], row["o"], row["h"], row["l"], row["c"], row["v"]] for row in window])
            signal_key = _key(shape, visual)
            if signal_key != BASE_SIGNAL_KEY:
                continue
            events.append(
                {
                    "dt": ymd,
                    "code": code,
                    "name": code,
                    "side": "sell",
                    "entry_qualified": True,
                    "setup_type": "crash_distribution_short_pullback_additive_oos_v1",
                    "forward_return_20": -(float(bars[index + 20]["c"]) / close - 1.0),
                    "scene_visual_key": signal_key,
                    "market_scene": shape.get("market_scene"),
                    "trade_side": shape.get("trade_side"),
                    "action_bias": shape.get("action_bias"),
                    "shape_intent": shape.get("shape_intent"),
                    "entry_timing": shape.get("entry_timing"),
                    "visual_decision": visual.get("decision"),
                    "visual_entry_method": visual.get("entry_method"),
                    "ma20_slope_10": _round(_ma20_slope_10(window)),
                    "latest_price_position_pct": visual_features.get("latest_price_position_pct"),
                    "recent_high_drawdown": visual_features.get("recent_high_drawdown"),
                    "in_existing_sell_pool": False,
                }
            )
    return events


def run_probe(*, db_path: Path, output_root: Path, start_dt: int, end_dt: int, max_codes: int | None = None) -> dict[str, Any]:
    output_dir = output_root / f"{_now_tag()}-crash_distribution_short_pullback_additive_oos_v1"
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

    candidates = _build_candidates(by_code, sell_codes_by_date=sell_codes_by_date, start_dt=start_dt, end_dt=end_dt)
    selected_by_date = _select_one_per_date(candidates)
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
        "outside_additive_candidate_count": len(candidates),
        "outside_additive_candidate_date_count": len({int(row["dt"]) for row in candidates}),
        "selected_additive_candidate_count": len(selected_by_date),
        "selected_additive_date_count": len(selected_by_date),
        "oos_sell_candidate_rows": sum(len(rows) for rows in oos_groups.values()),
        "oos_sell_candidate_date_count": len(oos_groups),
        "oos_selected_additive_date_count": len(oos_selected),
        "oos_active_month_count": len(active_oos_months),
        "oos_positive_active_month_count": len(positive_oos_months),
        "oos_positive_active_month_rate": round(len(positive_oos_months) / len(active_oos_months), 6) if active_oos_months else None,
        "in_sample_selected_additive_date_count": len(in_sample_selected),
        "base_signal_key": BASE_SIGNAL_KEY,
    }
    decision = _decision(oos_compare, monthly, coverage)
    result = {
        "schema_version": "tradex_crash_distribution_short_pullback_additive_oos_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "authoritative_result": True,
        "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {
            "source_table": "signal_decision_daily + daily_bars outside-gap scan",
            "start_dt": start_dt,
            "end_dt": end_dt,
            "oos_start_dt": oos_start_dt,
            "oos_end_dt": oos_end_dt,
            "in_sample_probe_period": [IN_SAMPLE_START_DT, IN_SAMPLE_END_DT],
            "side": "sell",
            "entry_qualified_only": True,
            "same_dates_as_sell_candidate_pool": True,
            "same_top_k": list(TOP_K_VALUES),
            "same_cost_slippage": "flat_zero_cost",
            "same_regime_condition": BASE_SIGNAL_KEY,
            "same_artifact_detail_level": "full_json_with_ledger",
            "changed_axis": "crash_or_distribution_short_pullback_probe_candidate",
            "selection_rule": "scene_visual_key == crash_or_distribution_phase|wait_for_short_trigger|watch|pullback_probe_candidate; at most one outside-gap candidate per sell-candidate date ranked by lower ma20_slope_10 then higher latest_price_position_pct",
            "additive_score_policy": "selected candidate receives date max tradePriorityScore + 0.0001 for fixed branching stress-test",
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
            "selection_divergence_reason": "historical_oos_outside_gap_crash_distribution_short_pullback_candidate_added_to_sell_pool",
        },
        "authoritative_rollup_decision": decision["judgment"],
        "reason_type": decision["reason_type"],
        "candidate_generation_challenger_created": len(oos_selected) > 0,
        "meemee_reflectable": False,
        "remaining_risks": [
            "additive candidates do not have champion-native score or rank",
            "additive_score_policy is a fixed stress-test, not a production scoring implementation",
            "OHLC visual proxy is not pixel screenshot analysis",
            "historical OOS is pre-selection holdout, not forward live paper replay",
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
    _write_json(decision_path, {k: result[k] for k in ("schema_version", "authoritative_rollup_decision", "reason_type", "candidate_generation_challenger_created", "meemee_reflectable", "remaining_risks", "artifacts")})
    _write_json(monthly_path, result["monthly_stability"])
    selected_codes = {(dt, str(row["code"])) for dt, row in selected_by_date.items()}
    with ledger_path.open("w", encoding="utf-8") as fh:
        for event in candidates:
            key = (int(event["dt"]), str(event["code"]))
            fh.write(json.dumps({**event, "selected_additive_candidate": key in selected_codes}, ensure_ascii=False, default=str) + "\n")
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
