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
from scripts.tradex_short_scene_visual_candidate_gap_v1 import _key, _to_visual_bars, _write_json
from scripts.tradex_short_scene_visual_topk_probe_v1 import BAD_LOSER_THRESHOLD, KEEP_KEYS, SEVERE_LOSER_THRESHOLD, _load_signal_rows
from scripts.tradex_visual_ai_entry_benchmark_v1 import _visual_features_from_ohlc, _visual_review
from shared.runtime_stock_db_contract import inspect_runtime_stock_db, resolve_runtime_stock_db_path
from tools.debug.trade_shape_classifier import classify_shape_from_bars


DEFAULT_OUTPUT_ROOT = Path("G:/Tradex/short_scene_visual_additive_candidate_v1")
BASE_SIGNAL_KEY = "distribution_or_failed_retest|sell_failed_high_retest_after_7ma_break|short|keep_probe_candidate"
TOP_K_VALUES = (5, 10)


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _round(value: float | None, digits: int = 6) -> float | None:
    return None if value is None else round(value, digits)


def _metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    values = [float(row["forward_return_20"]) for row in rows if row.get("forward_return_20") is not None]
    return {
        "count": len(rows),
        "unique_codes": len({str(row["code"]) for row in rows}),
        "forward_return_20_mean": _round(mean(values) if values else None),
        "forward_return_20_median": _round(median(values) if values else None),
        "hit_rate_20": _round(sum(1 for value in values if value > 0) / len(values) if values else None),
        "bad_loser_rate_20": _round(sum(1 for value in values if value <= BAD_LOSER_THRESHOLD) / len(values) if values else None),
        "severe_loser_rate_20": _round(sum(1 for value in values if value <= SEVERE_LOSER_THRESHOLD) / len(values) if values else None),
    }


def _delta(target: dict[str, Any], baseline: dict[str, Any]) -> dict[str, Any]:
    fields = ("forward_return_20_mean", "forward_return_20_median", "hit_rate_20", "bad_loser_rate_20", "severe_loser_rate_20")
    return {field: _round((target.get(field) or 0.0) - (baseline.get(field) or 0.0)) for field in fields}


def _ma20_slope_10(window: list[dict[str, Any]]) -> float | None:
    closes = [float(row["c"]) for row in window if row.get("c") is not None]
    if len(closes) < 30:
        return None
    current = mean(closes[-20:])
    previous = mean(closes[-30:-10])
    if previous <= 0:
        return None
    return current / previous - 1.0


def _select_one_per_date(events: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    selected: dict[int, dict[str, Any]] = {}
    for event in events:
        dt = int(event["dt"])
        current = selected.get(dt)
        if current is None:
            selected[dt] = event
            continue
        current_slope = current.get("ma20_slope_10")
        event_slope = event.get("ma20_slope_10")
        current_rank = float(current_slope) if current_slope is not None else 1e9
        event_rank = float(event_slope) if event_slope is not None else 1e9
        if (event_rank, str(event["code"])) < (current_rank, str(current["code"])):
            selected[dt] = event
    return selected


def _topk_compare(groups: dict[int, list[dict[str, Any]]], selected_by_date: dict[int, dict[str, Any]], *, topk: int) -> dict[str, Any]:
    champion_rows: list[dict[str, Any]] = []
    challenger_rows: list[dict[str, Any]] = []
    changed_counts: list[int] = []
    added_selected_rows = 0
    evaluated_dates = 0
    skipped_dates_without_topk = 0
    for dt, rows in sorted(groups.items()):
        if len(rows) < topk:
            skipped_dates_without_topk += 1
            continue
        evaluated_dates += 1
        champion = sorted(rows, key=lambda row: (-float(row["tradePriorityScore"]), float(row.get("finalRank") or 1e9), str(row["code"])))[:topk]
        augmented = list(rows)
        selected = selected_by_date.get(dt)
        if selected is not None:
            max_score = max(float(row["tradePriorityScore"]) for row in rows)
            augmented.append({**selected, "tradePriorityScore": max_score + 0.0001, "finalRank": 0, "additive_candidate": True})
        challenger = sorted(augmented, key=lambda row: (-float(row["tradePriorityScore"]), float(row.get("finalRank") or 1e9), str(row["code"])))[:topk]
        champion_set = {str(row["code"]) for row in champion}
        challenger_set = {str(row["code"]) for row in challenger}
        changed_counts.append(len(champion_set.symmetric_difference(challenger_set)))
        added_selected_rows += sum(1 for row in challenger if row.get("additive_candidate") is True)
        champion_rows.extend(champion)
        challenger_rows.extend(challenger)
    champion = _metrics(champion_rows)
    challenger = _metrics(challenger_rows)
    return {
        "topk": topk,
        "evaluated_dates": evaluated_dates,
        "skipped_dates_without_topk": skipped_dates_without_topk,
        "champion": champion,
        "additive_challenger": challenger,
        "additive_delta": _delta(challenger, champion),
        "changed_member_count_total": int(sum(changed_counts)),
        "changed_member_count_mean": _round(mean([float(v) for v in changed_counts]) if changed_counts else None),
        "selected_additive_rows_in_topk": added_selected_rows,
    }


def _decision(compare: dict[str, Any], coverage: dict[str, Any]) -> dict[str, str]:
    top5 = compare["top5"]
    top10 = compare["top10"]
    if coverage["selected_additive_candidate_count"] == 0:
        return {"judgment": "drop", "reason_type": "no_additive_candidates_selected"}
    if (top5.get("changed_member_count_total") or 0) == 0:
        return {"judgment": "drop", "reason_type": "no_material_top5_branching"}
    top5_delta = top5["additive_delta"]
    top10_delta = top10["additive_delta"]
    mean_delta = top5_delta.get("forward_return_20_mean") or 0.0
    bad_delta = top5_delta.get("bad_loser_rate_20") or 0.0
    severe_delta = top5_delta.get("severe_loser_rate_20") or 0.0
    top10_mean_delta = top10_delta.get("forward_return_20_mean") or 0.0
    if (
        coverage["selected_additive_date_count"] >= 10
        and mean_delta >= 0.003
        and bad_delta <= 0.0
        and severe_delta <= 0.0
        and top10_mean_delta >= -0.001
    ):
        return {"judgment": "keep", "reason_type": "additive_candidate_improves_top5_without_loser_damage"}
    if mean_delta > 0 and bad_delta <= 0.0:
        return {"judgment": "hold", "reason_type": "weak_top5_gain_requires_broader_validation"}
    if mean_delta > 0:
        return {"judgment": "hold", "reason_type": "top5_gain_with_loser_damage"}
    return {"judgment": "drop", "reason_type": "additive_candidate_hurts_top5"}


def _build_outside_candidates(
    by_code: dict[str, list[dict[str, Any]]],
    *,
    sell_codes_by_date: dict[int, set[str]],
    start_dt: int,
    end_dt: int,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for code, bars in by_code.items():
        for index in range(160, len(bars) - 20):
            ymd = int(bars[index]["ymd"])
            if ymd < start_dt or ymd > end_dt or ymd not in sell_codes_by_date or code in sell_codes_by_date[ymd]:
                continue
            close = float(bars[index]["c"])
            if close <= 0:
                continue
            window = bars[index - 159 : index + 1]
            shape = classify_shape_from_bars([[row["ymd"], row["o"], row["h"], row["l"], row["c"], row["v"]] for row in window])
            visual = _visual_review(_visual_features_from_ohlc(_to_visual_bars(window[-60:])))
            key = _key(shape, visual)
            if key != BASE_SIGNAL_KEY or key not in KEEP_KEYS:
                continue
            events.append(
                {
                    "dt": ymd,
                    "code": code,
                    "name": code,
                    "side": "sell",
                    "entry_qualified": True,
                    "setup_type": "short_scene_visual_additive_candidate_v1",
                    "forward_return_20": -(float(bars[index + 20]["c"]) / close - 1.0),
                    "scene_visual_key": key,
                    "market_scene": shape.get("market_scene"),
                    "trade_side": shape.get("trade_side"),
                    "action_bias": shape.get("action_bias"),
                    "shape_intent": shape.get("shape_intent"),
                    "entry_timing": shape.get("entry_timing"),
                    "visual_decision": visual.get("decision"),
                    "visual_entry_method": visual.get("entry_method"),
                    "ma20_slope_10": _round(_ma20_slope_10(window)),
                    "in_existing_sell_pool": False,
                }
            )
    return events


def run_probe(*, db_path: Path, output_root: Path, start_dt: int, end_dt: int, max_codes: int | None = None) -> dict[str, Any]:
    output_dir = output_root / f"{_now_tag()}-short_scene_visual_additive_candidate_v1"
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
    selected_by_date = _select_one_per_date(outside_candidates)
    compare = {f"top{topk}": _topk_compare(groups, selected_by_date, topk=topk) for topk in TOP_K_VALUES}
    coverage = {
        "sell_candidate_rows": len(sell_rows),
        "sell_candidate_date_count": len(groups),
        "outside_additive_candidate_count": len(outside_candidates),
        "outside_additive_candidate_date_count": len({int(row["dt"]) for row in outside_candidates}),
        "selected_additive_candidate_count": len(selected_by_date),
        "selected_additive_date_count": len(selected_by_date),
        "base_signal_key": BASE_SIGNAL_KEY,
    }
    decision = _decision(compare, coverage)
    result = {
        "schema_version": "tradex_short_scene_visual_additive_candidate_v1",
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
            "selection_rule": "at most one outside-gap candidate per sell-candidate date, ranked by lower ma20_slope_10",
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
            "selection_divergence_reason": "outside_gap_additive_candidate_forced_into_topk_when_available",
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
        ],
    }
    compare_path = output_dir / "short_scene_visual_additive_candidate_compare.json"
    decision_path = output_dir / "short_scene_visual_additive_candidate_decision.json"
    ledger_path = output_dir / "short_scene_visual_additive_candidate_ledger.jsonl"
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
        for event in outside_candidates:
            event = {**event, "selected_additive_candidate": int(event["dt"]) in selected_by_date and selected_by_date[int(event["dt"])]["code"] == event["code"]}
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
    print(json.dumps({"decision": result["authoritative_rollup_decision"], "reason_type": result["reason_type"], "observed_branching": result["observed_branching"], "top5": result["compare"]["top5"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
