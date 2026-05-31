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
    _subset_groups,
    _subset_selected,
)
from scripts.tradex_short_scene_visual_additive_candidate_v1 import TOP_K_VALUES, _topk_compare
from scripts.tradex_short_scene_visual_candidate_gap_v1 import _write_json
from scripts.tradex_visual_ai_entry_benchmark_v1 import _load_signal_rows as _load_side_signal_rows
from shared.runtime_stock_db_contract import inspect_runtime_stock_db, resolve_runtime_stock_db_path


DEFAULT_OUTPUT_ROOT = Path("G:/Tradex/strict_hanarekojima_sell_exhaustion_v1")
PATTERN_LABEL = "strict_hanarekojima_sell_exhaustion_v1"
ENTRY_CONVENTIONS = ("signal_day_close_entry", "next_close_above_signal_high_entry", "ma5_reclaim_entry")
MIN_GAP_DOWN_PCT = 0.002
MIN_WICK_RATIO = 0.025
MAX_WICK_TO_BODY = 2.5
MIN_BODY_TO_RANGE = 0.14
MAX_CONFIRMATION_DAYS = 10


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _round(value: float | None, digits: int = 6) -> float | None:
    return None if value is None else round(value, digits)


def _slope(current: float, previous: float) -> float | None:
    if previous <= 0:
        return None
    return current / previous - 1.0


def _ma(closes: list[float], end_exclusive: int, window: int) -> float | None:
    if end_exclusive < window:
        return None
    return mean(closes[end_exclusive - window : end_exclusive])


def _candle_features(row: dict[str, Any]) -> dict[str, Any]:
    open_ = float(row["o"])
    high = float(row["h"])
    low = float(row["l"])
    close = float(row["c"])
    span = high - low
    body = abs(close - open_)
    if min(open_, high, low, close) <= 0 or span <= 0:
        return {"valid": False}
    upper = high - max(open_, close)
    lower = min(open_, close) - low
    return {
        "valid": True,
        "bullish": close > open_,
        "body_to_range": body / span,
        "upper_wick_ratio": upper / span,
        "lower_wick_ratio": lower / span,
        "upper_wick_to_body": upper / body if body > 0 else 99.0,
        "lower_wick_to_body": lower / body if body > 0 else 99.0,
    }


def _wick_guard(candle: dict[str, Any]) -> bool:
    return bool(
        candle.get("valid")
        and candle.get("bullish")
        and candle["upper_wick_ratio"] >= MIN_WICK_RATIO
        and candle["lower_wick_ratio"] >= MIN_WICK_RATIO
        and candle["upper_wick_to_body"] <= MAX_WICK_TO_BODY
        and candle["lower_wick_to_body"] <= MAX_WICK_TO_BODY
        and candle["body_to_range"] >= MIN_BODY_TO_RANGE
    )


def _strict_features(window: list[dict[str, Any]]) -> dict[str, Any]:
    if len(window) < 32:
        return {"confirmed": False, "reason": "insufficient_history"}
    closes = [float(row["c"]) for row in window]
    day1 = window[-2]
    day2 = window[-1]
    before = window[-3]
    prior_ma5 = _ma(closes, len(closes) - 2, 5)
    prior_ma20 = _ma(closes, len(closes) - 2, 20)
    prev_ma5 = _ma(closes, len(closes) - 7, 5)
    prev_ma20 = _ma(closes, len(closes) - 12, 20)
    if None in {prior_ma5, prior_ma20, prev_ma5, prev_ma20}:
        return {"confirmed": False, "reason": "insufficient_ma_history"}
    ma5_slope = _slope(float(prior_ma5), float(prev_ma5))
    ma20_slope = _slope(float(prior_ma20), float(prev_ma20))
    prior_20_return = closes[-3] / closes[-23] - 1.0 if closes[-23] > 0 else None
    ma_down = bool(ma5_slope is not None and ma20_slope is not None and ma5_slope < 0 and ma20_slope < 0)
    ma_stack_down = bool(float(prior_ma5) < float(prior_ma20))
    downtrend_context = bool(ma_down and ma_stack_down and prior_20_return is not None and prior_20_return < 0)

    day1_candle = _candle_features(day1)
    day2_candle = _candle_features(day2)
    day1_open = float(day1["o"])
    day1_high = float(day1["h"])
    day1_low = float(day1["l"])
    day1_close = float(day1["c"])
    day2_open = float(day2["o"])
    day2_high = float(day2["h"])
    day2_low = float(day2["l"])
    day2_close = float(day2["c"])
    before_low = float(before["l"])
    before_close = float(before["c"])
    day1_gap_down = day1_high < before_low * (1.0 - MIN_GAP_DOWN_PCT) or day1_open < before_close * (1.0 - MIN_GAP_DOWN_PCT)
    day2_open_below_day1_low = day2_open < day1_low
    day2_lower_close_or_lower_range = day2_close < day1_close or (day1_high > day1_low and (day2_close - day1_low) / (day1_high - day1_low) <= 0.4)
    below_ma = day2_close < float(prior_ma5) and day2_close < float(prior_ma20)
    matched = bool(
        downtrend_context
        and below_ma
        and day1_gap_down
        and _wick_guard(day1_candle)
        and day2_open_below_day1_low
        and _wick_guard(day2_candle)
        and day2_lower_close_or_lower_range
    )
    return {
        "confirmed": True,
        "matched": matched,
        "downtrend_context": downtrend_context,
        "ma_down": ma_down,
        "ma_stack_down": ma_stack_down,
        "below_ma": below_ma,
        "day1_gap_down": day1_gap_down,
        "day1_wick_guard": _wick_guard(day1_candle),
        "day2_open_below_day1_low": day2_open_below_day1_low,
        "day2_lower_close_or_lower_range": day2_lower_close_or_lower_range,
        "day2_wick_guard": _wick_guard(day2_candle),
        "prior_ma5": _round(float(prior_ma5)),
        "prior_ma20": _round(float(prior_ma20)),
        "ma5_slope_5": _round(ma5_slope),
        "ma20_slope_10": _round(ma20_slope),
        "prior_20_return": _round(prior_20_return),
        "signal_high": _round(day2_high),
        "signal_close": _round(day2_close),
        "day1_open": _round(day1_open),
        "day1_high": _round(day1_high),
        "day1_low": _round(day1_low),
        "day1_close": _round(day1_close),
        "day2_open": _round(day2_open),
        "day2_high": _round(day2_high),
        "day2_low": _round(day2_low),
        "day2_close": _round(day2_close),
        **{f"day1_{key}": _round(value) if isinstance(value, float) else value for key, value in day1_candle.items() if key != "valid"},
        **{f"day2_{key}": _round(value) if isinstance(value, float) else value for key, value in day2_candle.items() if key != "valid"},
    }


def _entry_index(bars: list[dict[str, Any]], signal_index: int, convention: str, signal_high: float) -> int | None:
    if convention == "signal_day_close_entry":
        return signal_index
    for index in range(signal_index + 1, min(len(bars) - 20, signal_index + MAX_CONFIRMATION_DAYS) + 1):
        close = float(bars[index]["c"])
        if convention == "next_close_above_signal_high_entry" and close > signal_high:
            return index
        if convention == "ma5_reclaim_entry":
            closes = [float(row["c"]) for row in bars[: index + 1]]
            ma5 = mean(closes[-5:]) if len(closes) >= 5 else None
            prior_ma5 = mean(closes[-6:-1]) if len(closes) >= 6 else None
            prior_close = closes[-2] if len(closes) >= 2 else None
            if ma5 is not None and prior_ma5 is not None and prior_close is not None and prior_close <= prior_ma5 and close > ma5:
                return index
    return None


def _select_one_per_date(events: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    selected: dict[int, dict[str, Any]] = {}
    for event in events:
        dt = int(event["dt"])
        current = selected.get(dt)
        event_rank = (
            float(event.get("prior_20_return") or 9.0),
            -float(event.get("day2_body_to_range") or 0.0),
            abs(float(event.get("day2_upper_wick_ratio") or 0.0) - float(event.get("day2_lower_wick_ratio") or 0.0)),
            str(event["code"]),
        )
        current_rank = (
            float(current.get("prior_20_return") or 9.0),
            -float(current.get("day2_body_to_range") or 0.0),
            abs(float(current.get("day2_upper_wick_ratio") or 0.0) - float(current.get("day2_lower_wick_ratio") or 0.0)),
            str(current["code"]),
        ) if current is not None else None
        if current is None or event_rank < current_rank:
            selected[dt] = event
    return selected


def _build_candidates(
    by_code: dict[str, list[dict[str, Any]]],
    *,
    buy_codes_by_date: dict[int, set[str]],
    start_dt: int,
    end_dt: int,
) -> dict[str, list[dict[str, Any]]]:
    buy_dates = set(buy_codes_by_date)
    events_by_convention: dict[str, list[dict[str, Any]]] = {key: [] for key in ENTRY_CONVENTIONS}
    for code, bars in by_code.items():
        for signal_index, signal_bar in enumerate(bars):
            if signal_index < 35 or signal_index >= len(bars) - 20:
                continue
            signal_dt = int(signal_bar["ymd"])
            if signal_dt < start_dt or signal_dt > end_dt:
                continue
            features = _strict_features(bars[signal_index - 35 : signal_index + 1])
            if not features.get("matched"):
                continue
            signal_high = float(signal_bar["h"])
            for convention in ENTRY_CONVENTIONS:
                entry_index = _entry_index(bars, signal_index, convention, signal_high)
                if entry_index is None or entry_index >= len(bars) - 20:
                    continue
                entry_bar = bars[entry_index]
                entry_dt = int(entry_bar["ymd"])
                if entry_dt < start_dt or entry_dt > end_dt or entry_dt not in buy_dates or code in buy_codes_by_date[entry_dt]:
                    continue
                entry_close = float(entry_bar["c"])
                if entry_close <= 0:
                    continue
                events_by_convention[convention].append(
                    {
                        "dt": entry_dt,
                        "signal_dt": signal_dt,
                        "code": code,
                        "name": code,
                        "side": "buy",
                        "entry_qualified": True,
                        "setup_type": f"{PATTERN_LABEL}:{convention}",
                        "entry_convention": convention,
                        "forward_return_20": float(bars[entry_index + 20]["c"]) / entry_close - 1.0,
                        "scene_visual_key": PATTERN_LABEL,
                        "market_scene": "downtrend_sell_exhaustion",
                        "trade_side": "long",
                        "action_bias": "buy_strict_hanarekojima_sell_exhaustion",
                        "shape_intent": PATTERN_LABEL,
                        "entry_timing": convention,
                        "entry_close": _round(entry_close),
                        "entry_delay_days": entry_index - signal_index,
                        "in_existing_buy_pool": False,
                        **{key: value for key, value in features.items() if key not in {"confirmed", "matched"}},
                    }
                )
    return events_by_convention


def _convention_decision(compare: dict[str, Any], coverage: dict[str, Any]) -> dict[str, str]:
    top5_delta = compare["top5"]["additive_delta"]
    top10_delta = compare["top10"]["additive_delta"]
    changed = int(compare["top5"].get("changed_member_count_total") or 0)
    if coverage["oos_selected_additive_date_count"] < 10 or changed < 20:
        return {"judgment": "hold", "reason_type": "insufficient_oos_branching_or_breadth"}
    mean_delta = top5_delta.get("forward_return_20_mean") or 0.0
    hit_delta = top5_delta.get("hit_rate_20") or 0.0
    bad_delta = top5_delta.get("bad_loser_rate_20") or 0.0
    severe_delta = top5_delta.get("severe_loser_rate_20") or 0.0
    top10_mean = top10_delta.get("forward_return_20_mean") or 0.0
    if mean_delta > 0 and hit_delta >= 0 and bad_delta <= 0 and severe_delta <= 0 and top10_mean >= -0.001:
        return {"judgment": "keep_for_next_probe", "reason_type": "top5_mean_hit_risk_passes_and_top10_not_damaged"}
    if mean_delta > 0 and (hit_delta < 0 or bad_delta > 0 or severe_delta > 0):
        return {"judgment": "drop", "reason_type": "top5_mean_improves_but_hit_or_adverse_worsens"}
    return {"judgment": "drop", "reason_type": "top5_primary_metric_not_improved"}


def _rollup_decision(per_entry: dict[str, Any]) -> dict[str, str]:
    b = per_entry["next_close_above_signal_high_entry"]["decision"]["judgment"]
    c = per_entry["ma5_reclaim_entry"]["decision"]["judgment"]
    a = per_entry["signal_day_close_entry"]["decision"]["judgment"]
    if b == "keep_for_next_probe" or c == "keep_for_next_probe":
        return {"judgment": "keep_confirmation_entry", "reason_type": "confirmation_entry_passes_while_broad_route_remains_drop"}
    if all(row["decision"]["judgment"] == "drop" for row in per_entry.values()):
        return {"judgment": "drop", "reason_type": "all_entry_conventions_fail"}
    if a == "drop":
        return {"judgment": "hold", "reason_type": "signal_close_entry_drops_but_confirmation_entries_not_decisive"}
    return {"judgment": "hold", "reason_type": "insufficient_or_mixed_entry_convention_evidence"}


def _feature_decomposition(events: list[dict[str, Any]]) -> dict[str, Any]:
    if not events:
        return {"count": 0}
    winners = [row for row in events if float(row["forward_return_20"]) > 0]
    losers = [row for row in events if float(row["forward_return_20"]) <= 0]

    def avg(rows: list[dict[str, Any]], key: str) -> float | None:
        values = [float(row[key]) for row in rows if row.get(key) is not None]
        return _round(mean(values) if values else None)

    keys = ("prior_20_return", "day2_body_to_range", "day2_upper_wick_to_body", "day2_lower_wick_to_body", "entry_delay_days")
    return {
        "count": len(events),
        "success_count": len(winners),
        "failure_count": len(losers),
        "success_feature_means": {key: avg(winners, key) for key in keys},
        "failure_feature_means": {key: avg(losers, key) for key in keys},
    }


def run_probe(*, db_path: Path, output_root: Path, start_dt: int, end_dt: int, max_codes: int | None = None) -> dict[str, Any]:
    output_dir = output_root / f"{_now_tag()}-strict_hanarekojima_sell_exhaustion_v1"
    output_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        buy_rows = _load_side_signal_rows(con, start_dt=start_dt, end_dt=end_dt, side="buy")
        by_code = _load_daily(con, start_dt=start_dt, end_dt=end_dt, history=45, forward=35)
    finally:
        con.close()
    if max_codes is not None:
        by_code = dict(list(sorted(by_code.items()))[:max_codes])

    groups: dict[int, list[dict[str, Any]]] = defaultdict(list)
    buy_codes_by_date: dict[int, set[str]] = defaultdict(set)
    for row in buy_rows:
        groups[int(row["dt"])].append({**row, "additive_candidate": False})
        buy_codes_by_date[int(row["dt"])].add(str(row["code"]))

    events_by_convention = _build_candidates(by_code, buy_codes_by_date=buy_codes_by_date, start_dt=start_dt, end_dt=end_dt)
    oos_start_dt = start_dt
    oos_end_dt = min(end_dt, IN_SAMPLE_START_DT - 1)
    oos_groups = _subset_groups(groups, oos_start_dt, oos_end_dt)
    in_sample_groups = _subset_groups(groups, IN_SAMPLE_START_DT, min(end_dt, IN_SAMPLE_END_DT))
    per_entry: dict[str, Any] = {}
    for convention, candidates in events_by_convention.items():
        selected_by_date = _select_one_per_date(candidates)
        oos_selected = _subset_selected(selected_by_date, oos_start_dt, oos_end_dt)
        in_sample_selected = _subset_selected(selected_by_date, IN_SAMPLE_START_DT, min(end_dt, IN_SAMPLE_END_DT))
        oos_compare = {f"top{topk}": _topk_compare(oos_groups, oos_selected, topk=topk) for topk in TOP_K_VALUES}
        in_sample_compare = {f"top{topk}": _topk_compare(in_sample_groups, in_sample_selected, topk=topk) for topk in TOP_K_VALUES}
        monthly = _month_rows(groups, selected_by_date)
        coverage = {
            "outside_additive_candidate_count": len(candidates),
            "outside_additive_candidate_date_count": len({int(row["dt"]) for row in candidates}),
            "selected_additive_candidate_count": len(selected_by_date),
            "selected_additive_date_count": len(selected_by_date),
            "oos_selected_additive_date_count": len(oos_selected),
            "in_sample_selected_additive_date_count": len(in_sample_selected),
        }
        per_entry[convention] = {
            "coverage": coverage,
            "compare": {"oos": oos_compare, "in_sample_reference": in_sample_compare},
            "observed_branching": {
                "changed_top5_members_count": oos_compare["top5"]["changed_member_count_total"],
                "changed_top10_members_count": oos_compare["top10"]["changed_member_count_total"],
                "changed_rank_count": oos_compare["top5"]["changed_member_count_total"],
                "selection_divergence_reason": f"strict_hanarekojima_sell_exhaustion_{convention}_added_to_buy_pool",
            },
            "decision": _convention_decision(oos_compare, coverage),
            "monthly_stability": {"rows": monthly},
            "success_failure_feature_decomposition": _feature_decomposition([row for row in candidates if int(row["dt"]) <= oos_end_dt]),
        }
    rollup = _rollup_decision(per_entry)
    result = {
        "schema_version": "tradex_strict_hanarekojima_sell_exhaustion_v1",
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
            "time_frame": "daily_only",
            "entry_qualified_only": True,
            "same_dates_as_buy_candidate_pool": True,
            "same_top_k": list(TOP_K_VALUES),
            "same_cost_slippage": "flat_zero_cost",
            "same_regime_condition": PATTERN_LABEL,
            "same_artifact_detail_level": "full_json_with_ledger_and_entry_convention_compare",
            "changed_axis": PATTERN_LABEL,
            "entry_conventions": list(ENTRY_CONVENTIONS),
            "selection_rule": "MA5 slope < 0, MA20 slope < 0, MA5 < MA20, downtrend context, day1 gap-down bullish candle with bounded upper/lower wicks, day2 opens below day1 low, bullish bounded-wick candle, and closes below day1 close or lower day1 range",
            "additive_score_policy": "selected candidate receives date max tradePriorityScore + 0.0001 for fixed branching stress-test",
        },
        "scope": {
            "tradex_only": True,
            "meemee_ranking_changed": False,
            "meemee_ui_changed": False,
            "runtime_db_written": False,
            "champion_scoring_changed": False,
            "broad_hanarekojima_drop_rescued": False,
            "silent_fallback_used": False,
            "research_fallback_used": False,
        },
        "runtime_stock_db_status": inspect_runtime_stock_db(runtime_db_path=db_path),
        "buy_pool_coverage": {
            "buy_candidate_rows": len(buy_rows),
            "buy_candidate_date_count": len(groups),
            "oos_buy_candidate_rows": sum(len(rows) for rows in oos_groups.values()),
            "oos_buy_candidate_date_count": len(oos_groups),
        },
        "per_entry_convention": per_entry,
        "authoritative_rollup_decision": rollup["judgment"],
        "reason_type": rollup["reason_type"],
        "candidate_generation_challenger_created": any(row["coverage"]["oos_selected_additive_date_count"] > 0 for row in per_entry.values()),
        "meemee_reflectable": False,
        "remaining_risks": [
            "additive candidates do not have champion-native score or rank",
            "additive_score_policy is a fixed stress-test, not a production scoring implementation",
            "strict OHLC translation may still differ from human screenshot interpretation",
            "confirmation entries require same-date buy-pool availability for fixed comparison",
        ],
    }
    compare_path = output_dir / "compare.json"
    decision_path = output_dir / "decision.json"
    ledger_path = output_dir / "ledger.jsonl"
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
        for convention, candidates in events_by_convention.items():
            selected_codes = {(dt, str(row["code"])) for dt, row in _select_one_per_date(candidates).items()}
            for event in candidates:
                key = (int(event["dt"]), str(event["code"]))
                fh.write(json.dumps({**event, "entry_convention": convention, "selected_additive_candidate": key in selected_codes}, ensure_ascii=False, default=str) + "\n")
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
    summary = {
        "artifacts": result["artifacts"],
        "decision": result["authoritative_rollup_decision"],
        "reason_type": result["reason_type"],
        "per_entry": {
            key: {
                "decision": value["decision"],
                "coverage": value["coverage"],
                "observed_branching": value["observed_branching"],
                "top5_delta": value["compare"]["oos"]["top5"]["additive_delta"],
                "top10_delta": value["compare"]["oos"]["top10"]["additive_delta"],
            }
            for key, value in result["per_entry_convention"].items()
        },
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
