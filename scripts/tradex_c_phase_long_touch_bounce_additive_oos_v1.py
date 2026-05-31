from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
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
    _oos_decision,
    _subset_groups,
    _subset_selected,
)
from scripts.tradex_short_scene_visual_additive_candidate_v1 import TOP_K_VALUES, _ma20_slope_10, _topk_compare
from scripts.tradex_short_scene_visual_candidate_gap_v1 import _to_visual_bars, _write_json
from scripts.tradex_visual_ai_entry_benchmark_v1 import _load_signal_rows as _load_side_signal_rows
from scripts.tradex_visual_ai_entry_benchmark_v1 import _visual_features_from_ohlc, _visual_review
from shared.runtime_stock_db_contract import inspect_runtime_stock_db, resolve_runtime_stock_db_path
from tools.debug.trade_shape_classifier import classify_shape_from_bars


DEFAULT_OUTPUT_ROOT = Path("G:/Tradex/c_phase_long_touch_bounce_additive_oos_v1")
REQUIRED_SHAPE_INTENT = "c_phase_uptrend_20ma_touch_bounce"
BASE_SIGNAL_KEY = "uptrend_c_phase|hold_or_add_long|long|pullback_probe_candidate"


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _round(value: float | None, digits: int = 6) -> float | None:
    return None if value is None else round(value, digits)


def _select_one_per_date(events: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    selected: dict[int, dict[str, Any]] = {}
    decision_rank = {
        "pullback_probe_candidate": 0,
        "keep_probe_candidate": 1,
    }
    for event in events:
        dt = int(event["dt"])
        current = selected.get(dt)
        if current is None:
            selected[dt] = event
            continue
        current_rank = (
            decision_rank.get(str(current.get("visual_decision")), 9),
            abs(float(current.get("ma20_distance") or 9.0)),
            -float(current.get("ma20_slope_10") or -9.0),
            str(current["code"]),
        )
        event_rank = (
            decision_rank.get(str(event.get("visual_decision")), 9),
            abs(float(event.get("ma20_distance") or 9.0)),
            -float(event.get("ma20_slope_10") or -9.0),
            str(event["code"]),
        )
        if event_rank < current_rank:
            selected[dt] = event
    return selected


def _cheap_c_phase_touch_bounce_prefilter(window: list[dict[str, Any]]) -> bool:
    if len(window) < 61:
        return False
    closes = [float(row["c"]) for row in window]
    latest_low = float(window[-1]["l"])
    latest_high = float(window[-1]["h"])
    close = closes[-1]
    ma5 = mean(closes[-5:])
    ma20 = mean(closes[-20:])
    ma60 = mean(closes[-60:])
    prior_ma5 = mean(closes[-6:-1])
    prior_ma20 = mean(closes[-21:-1])
    prior_ma60 = mean(closes[-61:-1])
    latest_20_return = close / closes[-21] - 1.0 if closes[-21] else 0.0
    c_phase_context = ma5 > ma20 > ma60 and latest_20_return >= 0.04
    c_phase_prior_context = prior_ma5 > prior_ma20 > prior_ma60
    touched20 = latest_low <= ma20 <= latest_high
    return bool((c_phase_context or c_phase_prior_context) and touched20 and close >= ma20)


def _build_c_phase_long_candidates(
    by_code: dict[str, list[dict[str, Any]]],
    *,
    buy_codes_by_date: dict[int, set[str]],
    start_dt: int,
    end_dt: int,
) -> list[dict[str, Any]]:
    buy_dates = set(buy_codes_by_date)
    events: list[dict[str, Any]] = []
    for code, bars in by_code.items():
        for index, bar in enumerate(bars):
            if index < 160 or index >= len(bars) - 20:
                continue
            ymd = int(bar["ymd"])
            if ymd < start_dt or ymd > end_dt or ymd not in buy_dates or code in buy_codes_by_date[ymd]:
                continue
            close = float(bar["c"])
            if close <= 0:
                continue
            window = bars[index - 159 : index + 1]
            if not _cheap_c_phase_touch_bounce_prefilter(window):
                continue
            shape = classify_shape_from_bars([[row["ymd"], row["o"], row["h"], row["l"], row["c"], row["v"]] for row in window])
            if shape.get("shape_intent") != REQUIRED_SHAPE_INTENT:
                continue
            visual_features = _visual_features_from_ohlc(_to_visual_bars(window[-60:]))
            visual = _visual_review(visual_features)
            if visual.get("decision") != "pullback_probe_candidate":
                continue
            events.append(
                {
                    "dt": ymd,
                    "code": code,
                    "name": code,
                    "side": "buy",
                    "entry_qualified": True,
                    "setup_type": "c_phase_long_touch_bounce_additive_oos_v1",
                    "forward_return_20": float(bars[index + 20]["c"]) / close - 1.0,
                    "scene_visual_key": BASE_SIGNAL_KEY,
                    "market_scene": shape.get("market_scene"),
                    "trade_side": shape.get("trade_side"),
                    "action_bias": shape.get("action_bias"),
                    "shape_intent": shape.get("shape_intent"),
                    "entry_timing": shape.get("entry_timing"),
                    "visual_decision": visual.get("decision"),
                    "visual_entry_method": visual.get("entry_method"),
                    "ma20_slope_10": _round(_ma20_slope_10(window)),
                    "ma20_distance": visual_features.get("ma20_distance"),
                    "latest_price_position_pct": visual_features.get("latest_price_position_pct"),
                    "in_existing_buy_pool": False,
                }
            )
    return events


def run_probe(*, db_path: Path, output_root: Path, start_dt: int, end_dt: int, max_codes: int | None = None) -> dict[str, Any]:
    output_dir = output_root / f"{_now_tag()}-c_phase_long_touch_bounce_additive_oos_v1"
    output_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        buy_rows = _load_side_signal_rows(con, start_dt=start_dt, end_dt=end_dt, side="buy")
        by_code = _load_daily(con, start_dt=start_dt, end_dt=end_dt, history=160, forward=20)
    finally:
        con.close()
    if max_codes is not None:
        by_code = dict(list(sorted(by_code.items()))[:max_codes])

    groups: dict[int, list[dict[str, Any]]] = defaultdict(list)
    buy_codes_by_date: dict[int, set[str]] = defaultdict(set)
    for row in buy_rows:
        groups[int(row["dt"])].append({**row, "additive_candidate": False})
        buy_codes_by_date[int(row["dt"])].add(str(row["code"]))

    candidates = _build_c_phase_long_candidates(by_code, buy_codes_by_date=buy_codes_by_date, start_dt=start_dt, end_dt=end_dt)
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
        "buy_candidate_rows": len(buy_rows),
        "buy_candidate_date_count": len(groups),
        "outside_additive_candidate_count": len(candidates),
        "outside_additive_candidate_date_count": len({int(row["dt"]) for row in candidates}),
        "selected_additive_candidate_count": len(selected_by_date),
        "selected_additive_date_count": len(selected_by_date),
        "oos_buy_candidate_rows": sum(len(rows) for rows in oos_groups.values()),
        "oos_buy_candidate_date_count": len(oos_groups),
        "oos_selected_additive_date_count": len(oos_selected),
        "oos_active_month_count": len(active_oos_months),
        "oos_positive_active_month_count": len(positive_oos_months),
        "oos_positive_active_month_rate": round(len(positive_oos_months) / len(active_oos_months), 6) if active_oos_months else None,
        "in_sample_selected_additive_date_count": len(in_sample_selected),
        "required_shape_intent": REQUIRED_SHAPE_INTENT,
        "base_signal_key": BASE_SIGNAL_KEY,
    }
    decision = _oos_decision(oos_compare, monthly, coverage)
    result = {
        "schema_version": "tradex_c_phase_long_touch_bounce_additive_oos_v1",
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
            "side": "buy",
            "entry_qualified_only": True,
            "same_dates_as_buy_candidate_pool": True,
            "same_top_k": list(TOP_K_VALUES),
            "same_cost_slippage": "flat_zero_cost",
            "changed_axis": "c_phase_long_20ma_touch_bounce_outside_gap_candidate",
            "selection_rule": "shape_intent == c_phase_uptrend_20ma_touch_bounce and visual_decision == pullback_probe_candidate; at most one outside-gap candidate per buy-candidate date ranked by closer ma20_distance then higher ma20_slope_10",
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
            "selection_divergence_reason": "historical_oos_outside_gap_c_phase_long_touch_bounce_candidate_added_to_buy_pool",
        },
        "authoritative_rollup_decision": decision["judgment"],
        "reason_type": decision["reason_type"],
        "candidate_generation_challenger_created": len(oos_selected) > 0,
        "meemee_reflectable": False,
        "remaining_risks": [
            "historical OOS is pre-selection holdout, not forward live paper replay",
            "additive candidates do not have champion-native score or rank",
            "additive_score_policy is a fixed stress-test, not a production scoring implementation",
            "OHLC visual proxy is not pixel screenshot analysis",
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
