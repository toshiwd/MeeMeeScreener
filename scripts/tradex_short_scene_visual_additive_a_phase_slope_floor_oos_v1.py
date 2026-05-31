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
from scripts.tradex_short_scene_visual_additive_a_phase_v1 import BASE_SIGNAL_KEY
from scripts.tradex_short_scene_visual_additive_a_phase_slope_floor_v1 import MA20_SLOPE_10_FLOOR
from scripts.tradex_short_scene_visual_additive_candidate_v1 import TOP_K_VALUES, _ma20_slope_10, _select_one_per_date, _topk_compare
from scripts.tradex_short_scene_visual_candidate_gap_v1 import _key, _to_visual_bars, _write_json
from scripts.tradex_short_scene_visual_topk_probe_v1 import _load_signal_rows
from scripts.tradex_visual_ai_entry_benchmark_v1 import _visual_features_from_ohlc, _visual_review
from shared.runtime_stock_db_contract import inspect_runtime_stock_db, resolve_runtime_stock_db_path
from tools.debug.trade_shape_classifier import classify_shape_from_bars


DEFAULT_OUTPUT_ROOT = Path("G:/Tradex/short_scene_visual_additive_a_phase_slope_floor_oos_v1")
IN_SAMPLE_START_DT = 20260301
IN_SAMPLE_END_DT = 20260331


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _month(dt: int) -> str:
    return str(dt)[:6]


def _build_slope_floor_candidates(
    by_code: dict[str, list[dict[str, Any]]],
    *,
    sell_codes_by_date: dict[int, set[str]],
    start_dt: int,
    end_dt: int,
    floor: float = MA20_SLOPE_10_FLOOR,
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
            slope = _ma20_slope_10(window)
            if slope is None or slope < floor:
                continue
            visual = _visual_review(_visual_features_from_ohlc(_to_visual_bars(window[-60:])))
            if visual.get("decision") != "pullback_probe_candidate":
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
                    "setup_type": "short_scene_visual_additive_a_phase_slope_floor_oos_v1",
                    "forward_return_20": -(float(bars[index + 20]["c"]) / close - 1.0),
                    "scene_visual_key": signal_key,
                    "market_scene": shape.get("market_scene"),
                    "trade_side": shape.get("trade_side"),
                    "action_bias": shape.get("action_bias"),
                    "shape_intent": shape.get("shape_intent"),
                    "entry_timing": shape.get("entry_timing"),
                    "visual_decision": visual.get("decision"),
                    "visual_entry_method": visual.get("entry_method"),
                    "ma20_slope_10": slope,
                    "in_existing_sell_pool": False,
                }
            )
    return events


def _subset_groups(groups: dict[int, list[dict[str, Any]]], start_dt: int, end_dt: int) -> dict[int, list[dict[str, Any]]]:
    return {dt: rows for dt, rows in groups.items() if start_dt <= dt <= end_dt}


def _subset_selected(selected_by_date: dict[int, dict[str, Any]], start_dt: int, end_dt: int) -> dict[int, dict[str, Any]]:
    return {dt: row for dt, row in selected_by_date.items() if start_dt <= dt <= end_dt}


def _month_rows(groups: dict[int, list[dict[str, Any]]], selected_by_date: dict[int, dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    months = sorted({_month(dt) for dt in groups})
    for month in months:
        month_dates = [dt for dt in groups if _month(dt) == month]
        if not month_dates:
            continue
        start_dt = min(month_dates)
        end_dt = max(month_dates)
        month_groups = _subset_groups(groups, start_dt, end_dt)
        month_selected = _subset_selected(selected_by_date, start_dt, end_dt)
        top5 = _topk_compare(month_groups, month_selected, topk=5)
        rows.append(
            {
                "month": month,
                "sell_candidate_rows": sum(len(v) for v in month_groups.values()),
                "sell_candidate_date_count": len(month_groups),
                "selected_additive_date_count": len(month_selected),
                "changed_top5_members_count": top5["changed_member_count_total"],
                "top5_delta_mean": top5["additive_delta"]["forward_return_20_mean"],
                "top5_delta_bad_loser_rate": top5["additive_delta"]["bad_loser_rate_20"],
                "top5_delta_severe_loser_rate": top5["additive_delta"]["severe_loser_rate_20"],
                "top5_compare": top5,
            }
        )
    return rows


def _oos_decision(oos_compare: dict[str, Any], month_rows: list[dict[str, Any]], coverage: dict[str, Any]) -> dict[str, str]:
    if coverage["oos_selected_additive_date_count"] < 10:
        return {"judgment": "hold", "reason_type": "insufficient_oos_additive_breadth"}
    top5 = oos_compare["top5"]
    top10 = oos_compare["top10"]
    top5_delta = top5["additive_delta"]
    top10_delta = top10["additive_delta"]
    changed = top5.get("changed_member_count_total") or 0
    positive_rows = [row for row in month_rows if row["month"] < "202603" and (row.get("changed_top5_members_count") or 0) > 0]
    positive_rate = (
        sum(1 for row in positive_rows if (row.get("top5_delta_mean") or 0.0) > 0.0) / len(positive_rows)
        if positive_rows
        else 0.0
    )
    if (
        changed >= 10
        and (top5_delta.get("forward_return_20_mean") or 0.0) >= 0.002
        and (top5_delta.get("bad_loser_rate_20") or 0.0) <= 0.0
        and (top5_delta.get("severe_loser_rate_20") or 0.0) <= 0.0
        and (top10_delta.get("forward_return_20_mean") or 0.0) >= -0.001
        and positive_rate >= 0.55
    ):
        return {"judgment": "keep", "reason_type": "oos_top5_improves_without_loser_damage"}
    if (top5_delta.get("forward_return_20_mean") or 0.0) > 0.0 and (top5_delta.get("bad_loser_rate_20") or 0.0) <= 0.0:
        return {"judgment": "hold", "reason_type": "oos_positive_but_stability_or_top10_not_enough"}
    return {"judgment": "drop", "reason_type": "oos_validation_failed"}


def run_probe(*, db_path: Path, output_root: Path, start_dt: int, end_dt: int, max_codes: int | None = None) -> dict[str, Any]:
    output_dir = output_root / f"{_now_tag()}-short_scene_visual_additive_a_phase_slope_floor_oos_v1"
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

    slope_floor_candidates = _build_slope_floor_candidates(by_code, sell_codes_by_date=sell_codes_by_date, start_dt=start_dt, end_dt=end_dt)
    selected_by_date = _select_one_per_date(slope_floor_candidates)

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
    positive_oos_months = [row for row in oos_monthly if (row.get("changed_top5_members_count") or 0) > 0 and (row.get("top5_delta_mean") or 0.0) > 0.0]
    active_oos_months = [row for row in oos_monthly if (row.get("changed_top5_members_count") or 0) > 0]
    coverage = {
        "sell_candidate_rows": len(sell_rows),
        "sell_candidate_date_count": len(groups),
        "outside_additive_candidate_count": None,
        "outside_additive_candidate_count_computed": False,
        "slope_floor_candidate_count": len(slope_floor_candidates),
        "selected_additive_candidate_count": len(selected_by_date),
        "selected_additive_date_count": len(selected_by_date),
        "oos_sell_candidate_rows": sum(len(rows) for rows in oos_groups.values()),
        "oos_sell_candidate_date_count": len(oos_groups),
        "oos_selected_additive_date_count": len(oos_selected),
        "oos_active_month_count": len(active_oos_months),
        "oos_positive_active_month_count": len(positive_oos_months),
        "oos_positive_active_month_rate": round(len(positive_oos_months) / len(active_oos_months), 6) if active_oos_months else None,
        "in_sample_selected_additive_date_count": len(in_sample_selected),
        "ma20_slope_10_floor": MA20_SLOPE_10_FLOOR,
        "base_signal_key": BASE_SIGNAL_KEY,
    }
    decision = _oos_decision(oos_compare, monthly, coverage)
    result = {
        "schema_version": "tradex_short_scene_visual_additive_a_phase_slope_floor_oos_v1",
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
            "changed_axis": "validation_only_no_threshold_change",
            "selection_rule": "filter ma20_slope_10 >= -0.01, then at most one outside-gap candidate per sell-candidate date ranked by lower ma20_slope_10",
            "performance_note": "OOS run applies fixed slope floor before expensive shape classification; full pre-floor outside candidate count is intentionally not computed",
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
        "compare": {
            "oos": oos_compare,
            "in_sample_reference": in_sample_compare,
        },
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
            "selection_divergence_reason": "historical_oos_outside_gap_a_phase_candidate_added_only_when_ma20_slope_10_not_too_steep",
        },
        "authoritative_rollup_decision": decision["judgment"],
        "reason_type": decision["reason_type"],
        "candidate_generation_challenger_created": len(oos_selected) > 0,
        "meemee_reflectable": False,
        "remaining_risks": [
            "historical OOS is pre-selection holdout, not forward live paper replay",
            "additive candidates do not have champion-native score or rank",
        "additive_score_policy is a fixed stress-test, not a production scoring implementation",
        "full pre-floor outside candidate count is not computed in this OOS performance path",
        "OHLC visual proxy is not pixel screenshot analysis",
        ],
    }
    compare_path = output_dir / "short_scene_visual_additive_a_phase_slope_floor_oos_compare.json"
    decision_path = output_dir / "short_scene_visual_additive_a_phase_slope_floor_oos_decision.json"
    monthly_path = output_dir / "short_scene_visual_additive_a_phase_slope_floor_oos_monthly.json"
    ledger_path = output_dir / "short_scene_visual_additive_a_phase_slope_floor_oos_ledger.jsonl"
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
    with ledger_path.open("w", encoding="utf-8") as fh:
        selected_codes = {(dt, str(row["code"])) for dt, row in selected_by_date.items()}
        for event in slope_floor_candidates:
            key = (int(event["dt"]), str(event["code"]))
            row = {
                **event,
                "passes_ma20_slope_10_floor": True,
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
    parser.add_argument("--start-dt", type=int, default=20240301)
    parser.add_argument("--end-dt", type=int, default=20260331)
    parser.add_argument("--max-codes", type=int, default=None)
    args = parser.parse_args()
    result = run_probe(db_path=args.db_path, output_root=args.output_root, start_dt=args.start_dt, end_dt=args.end_dt, max_codes=args.max_codes)
    print(json.dumps(result["artifacts"], ensure_ascii=False, indent=2))
    print(json.dumps(result["coverage"], ensure_ascii=False, indent=2))
    print(json.dumps({"decision": result["authoritative_rollup_decision"], "reason_type": result["reason_type"], "observed_branching": result["observed_branching"], "oos_top5": result["compare"]["oos"]["top5"], "oos_top10": result["compare"]["oos"]["top10"], "monthly_stability": {k: v for k, v in result["monthly_stability"].items() if k != "rows"}}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
