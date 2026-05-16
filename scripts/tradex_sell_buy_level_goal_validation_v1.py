from __future__ import annotations

import argparse
import json
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


REPO_ROOT = Path(__file__).resolve().parent.parent
BUY_R11_GATE = REPO_ROOT / "artifacts" / "research_inventory" / "buy_surface_operational_validation_r11_gate_decision.json"
SHORT_COMPARE = REPO_ROOT / "artifacts" / "research_inventory" / "entry_precision_short_trend_compare.json"
SHORT_WIDE = REPO_ROOT / "artifacts" / "research_inventory" / "entry_precision_short_trend_wide_stability.json"
SHORT_REGIME = REPO_ROOT / "artifacts" / "research_inventory" / "entry_precision_short_trend_regime_map.json"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\sell_ranking_buy_level_goal_v1")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        parsed = float(value)
    except Exception:
        return None
    return parsed if parsed == parsed else None


def _mean(values: list[float]) -> float | None:
    return float(statistics.fmean(values)) if values else None


def _median(values: list[float]) -> float | None:
    return float(statistics.median(values)) if values else None


def _summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    short_rets = [_safe_float(row.get("short_ret_20")) for row in rows]
    short_rets = [value for value in short_rets if value is not None]
    mae = [_safe_float(row.get("mae20")) for row in rows]
    mae = [value for value in mae if value is not None]
    mfe = [_safe_float(row.get("mfe20")) for row in rows]
    mfe = [value for value in mfe if value is not None]
    return {
        "count": len(rows),
        "hit_rate": None if not short_rets else sum(1 for value in short_rets if value > 0.0) / len(short_rets),
        "mean_ret20": _mean(short_rets),
        "median_ret20": _median(short_rets),
        "severe_loser_rate": None if not short_rets else sum(1 for value in short_rets if value <= -0.05) / len(short_rets),
        "immediate_reverse_rate": None if not rows else sum(1 for row in rows if (_safe_float(row.get("short_ret_5")) or 0.0) <= 0.0) / len(rows),
        "mean_mae20": _mean(mae),
        "median_mae20": _median(mae),
        "mean_mfe20": _mean(mfe),
        "median_mfe20": _median(mfe),
    }


def _reason_codes(row: dict[str, Any]) -> list[str]:
    # Keep this independent from older scripts so the artifact records exactly
    # the fixed predicates used by this buy-level goal validation.
    reasons: list[str] = []
    weekly = _safe_float(row.get("weeklyBreakoutDownProb"))
    monthly = _safe_float(row.get("monthlyBreakoutDownProb"))
    range_prob = _safe_float(row.get("monthlyRangeProb"))
    range_pos = _safe_float(row.get("monthlyRangePos"))
    close_pos = _safe_float(row.get("close_pos"))
    trend_strict = row.get("trendDownStrict")
    ma20_slope = _safe_float(row.get("ma20_slope"))
    ma60_slope = _safe_float(row.get("ma60_slope"))
    short_ret_5 = _safe_float(row.get("short_ret_5"))
    short_ret_10 = _safe_float(row.get("short_ret_10"))

    if weekly is not None and monthly is not None and weekly >= 0.72 and monthly < 0.72:
        reasons.append("daily_trigger_but_monthly_not_aligned")
    if range_prob is not None and range_pos is not None and range_prob >= 0.20 and 0.30 <= range_pos <= 0.70:
        reasons.append("range_middle_short_without_edge")
    if close_pos is not None and 0.30 <= close_pos <= 0.70:
        reasons.append("range_middle_short_without_edge")
    if short_ret_5 is not None and short_ret_10 is not None and short_ret_5 <= 0.0 and short_ret_10 <= 0.0:
        reasons.append("failed_followthrough_after_break")
    if trend_strict is not True or (ma20_slope is not None and ma20_slope >= 0.0) or (ma60_slope is not None and ma60_slope >= 0.0):
        reasons.append("weak_downtrend_structure")
    return list(dict.fromkeys(reasons))


def _monthly(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_month: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        value = _safe_float(row.get("short_ret_20"))
        if value is None:
            continue
        by_month[str(row.get("ymd"))].append(value)
    medians = {month: float(statistics.median(values)) for month, values in sorted(by_month.items()) if values}
    return {
        "months_with_selection": len(medians),
        "positive_months": sum(1 for value in medians.values() if value > 0.0),
        "negative_months": sum(1 for value in medians.values() if value < 0.0),
        "monthly_median_ret20": medians,
    }


def _by_regime(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[str(row.get("marketRegime") or "unknown")].append(row)
    out: list[dict[str, Any]] = []
    for regime, regime_rows in sorted(groups.items()):
        out.append({"regime": regime, **_summary(regime_rows)})
    return out


def _branching(baseline_rows: list[dict[str, Any]], challenger_rows: list[dict[str, Any]]) -> dict[str, Any]:
    baseline_by_month: dict[int, list[dict[str, Any]]] = defaultdict(list)
    challenger_by_month: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in baseline_rows:
        baseline_by_month[int(row["ymd"])].append(row)
    for row in challenger_rows:
        challenger_by_month[int(row["ymd"])].append(row)

    changed_top5 = 0
    changed_top10 = 0
    changed_rank = 0
    bad_removed = 0
    severe_removed = 0
    monthly_rows: list[dict[str, Any]] = []

    for ymd, month_rows in sorted(baseline_by_month.items()):
        base_codes = [str(row["code"]) for row in month_rows]
        chal_rows = challenger_by_month.get(ymd, [])
        chal_codes = [str(row["code"]) for row in chal_rows]
        changed_top5 += len(set(base_codes[:5]).symmetric_difference(set(chal_codes[:5])))
        changed_top10 += len(set(base_codes[:10]).symmetric_difference(set(chal_codes[:10])))
        for code in set(base_codes[:20]).intersection(chal_codes[:20]):
            changed_rank += abs(base_codes.index(code) - chal_codes.index(code))
        removed = [row for row in month_rows if str(row["code"]) not in set(chal_codes)]
        bad_removed += sum(1 for row in removed if (_safe_float(row.get("short_ret_20")) or 0.0) <= 0.0)
        severe_removed += sum(1 for row in removed if (_safe_float(row.get("short_ret_20")) or 0.0) <= -0.05)
        monthly_rows.append(
            {
                "ymd": ymd,
                "baseline_top5": base_codes[:5],
                "challenger_top5": chal_codes[:5],
                "baseline_top10": base_codes[:10],
                "challenger_top10": chal_codes[:10],
                "removed_codes": [str(row["code"]) for row in removed],
            }
        )
    return {
        "changed_top5_members_count": changed_top5,
        "changed_top10_members_count": changed_top10,
        "changed_rank_count": changed_rank,
        "bad_pick_removal_count": bad_removed,
        "severe_loser_removal_count": severe_removed,
        "selection_divergence_reason": "meaningful_branching_observed" if changed_top5 or changed_top10 or changed_rank else "no_meaningful_branching",
        "monthly_rows": monthly_rows,
    }


def _deltas(baseline: dict[str, Any], challenger: dict[str, Any]) -> dict[str, Any]:
    keys = ["hit_rate", "mean_ret20", "median_ret20", "severe_loser_rate", "immediate_reverse_rate", "mean_mae20"]
    out: dict[str, Any] = {}
    for key in keys:
        left = baseline.get(key)
        right = challenger.get(key)
        out[f"{key}_delta"] = None if left is None or right is None else float(right) - float(left)
    return out


def _evaluate_label_candidate(
    baseline_rows: list[dict[str, Any]],
    *,
    candidate_id: str,
    exclude_predicate: Callable[[dict[str, Any]], bool],
) -> dict[str, Any]:
    challenger_rows = [dict(row) for row in baseline_rows if not exclude_predicate(row)]
    base_summary = _summary(baseline_rows)
    chal_summary = _summary(challenger_rows)
    branch = _branching(baseline_rows, challenger_rows)
    monthly = _monthly(challenger_rows)
    regime = _by_regime(challenger_rows)
    deltas = _deltas(base_summary, chal_summary)

    buy_level_blockers: list[str] = []
    if branch["changed_top5_members_count"] <= 0 or branch["changed_top10_members_count"] <= 0:
        buy_level_blockers.append("no_topk_branching")
    if (deltas.get("mean_ret20_delta") or 0.0) <= 0.0:
        buy_level_blockers.append("mean_ret20_not_improved")
    if (deltas.get("hit_rate_delta") or 0.0) < 0.0:
        buy_level_blockers.append("hit_rate_worse")
    if branch["bad_pick_removal_count"] <= 0:
        buy_level_blockers.append("bad_pick_removal_absent")
    if deltas.get("severe_loser_rate_delta") is not None and float(deltas["severe_loser_rate_delta"]) > 0.0:
        buy_level_blockers.append("severe_loser_rate_worse")
    if monthly["positive_months"] <= monthly["negative_months"]:
        buy_level_blockers.append("monthly_stability_not_positive")
    bad_regimes = [row for row in regime if (row.get("count") or 0) >= 3 and (row.get("mean_ret20") or 0.0) < 0.0]
    if bad_regimes:
        buy_level_blockers.append("regime_negative_mean_ret20")
    if chal_summary["count"] < 12:
        buy_level_blockers.append("sample_thin")

    preliminary_buy_level_gate_passed = not buy_level_blockers
    if preliminary_buy_level_gate_passed:
        buy_level_blockers.append("artifact_only_replay_no_new_candidate_refill")
        buy_level_blockers.append("dedicated_wide_window_rerun_missing")
        buy_level_blockers.append("dedicated_regime_stability_rerun_missing")
        decision = "hold_for_dedicated_rerun"
    elif (
        branch["bad_pick_removal_count"] > 0
        and (deltas.get("median_ret20_delta") or 0.0) > 0.0
        and (deltas.get("hit_rate_delta") or 0.0) >= 0.0
    ):
        decision = "hold_for_more_validation"
    else:
        decision = "drop_signal_not_buy_level_equivalent"

    return {
        "candidate_id": candidate_id,
        "baseline": base_summary,
        "challenger": chal_summary,
        "delta": {**deltas, **branch},
        "monthly_stability": monthly,
        "regime_stability": regime,
        "preliminary_buy_level_gate_passed": preliminary_buy_level_gate_passed,
        "buy_level_blockers": buy_level_blockers,
        "candidate_local_decision": decision,
        "research_fallback": False,
        "silent_fallback_used": False,
    }


def run(*, output_root: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    buy_gate = _load_json(BUY_R11_GATE)
    short_compare = _load_json(SHORT_COMPARE)
    short_wide = _load_json(SHORT_WIDE)
    short_regime = _load_json(SHORT_REGIME)
    current_sell = (short_compare.get("variants") or {}).get("short_trend_alignment_v1") or {}
    baseline_rows = [dict(row) for row in current_sell.get("baseline_rows") or []]
    if not baseline_rows:
        raise RuntimeError("short_trend_alignment_v1 baseline_rows not found")
    for row in baseline_rows:
        row["reason_codes_v1"] = _reason_codes(row)

    run_dir = output_root / f"{_utc_stamp()}-sell-ranking-buy-level-goal-v1"
    contract = {
        "schema_version": "tradex_sell_ranking_buy_level_goal_contract_v1",
        "generated_at": _utc_now(),
        "research_phase": "effectiveness_judgment",
        "axis": "sell_ranking_buy_level_goal_v1",
        "champion_id": "current_rule_trade_gate_baseline",
        "reference_sell_candidate": "short_trend_alignment_v1",
        "buy_reference_axis": "buy_surface_operational_validation_r11_r1_defensive",
        "fixed_evaluation_conditions": {
            "same_universe": True,
            "same_period": True,
            "same_top_k": True,
            "same_regime": True,
            "same_cost": True,
            "same_artifact_detail_level": True,
            "long_short_separated": True,
        },
        "no_silent_fallback": True,
        "silent_fallback_used": False,
        "research_fallback": False,
        "meemee_reflection": False,
        "production_ranking_changed": False,
        "blocked_actions": [
            "modify_meemee_ui",
            "modify_production_ranking",
            "publish_to_meemee",
            "change_buy_logic",
            "combine_with_ma_sell_probe",
        ],
        "source_artifacts": {
            "buy_r11_gate": str(BUY_R11_GATE),
            "short_compare": str(SHORT_COMPARE),
            "short_wide_stability": str(SHORT_WIDE),
            "short_regime_map": str(SHORT_REGIME),
        },
    }

    candidates = [
        _evaluate_label_candidate(
            baseline_rows,
            candidate_id="sell_failed_followthrough_after_break_demotion_v1",
            exclude_predicate=lambda row: "failed_followthrough_after_break" in row.get("reason_codes_v1", []),
        ),
        _evaluate_label_candidate(
            baseline_rows,
            candidate_id="sell_daily_trigger_but_monthly_not_aligned_demotion_v1",
            exclude_predicate=lambda row: "daily_trigger_but_monthly_not_aligned" in row.get("reason_codes_v1", []),
        ),
        _evaluate_label_candidate(
            baseline_rows,
            candidate_id="sell_range_middle_short_without_edge_demotion_v1",
            exclude_predicate=lambda row: "range_middle_short_without_edge" in row.get("reason_codes_v1", []),
        ),
    ]

    current_sell_summary = {
        "candidate_id": "short_trend_alignment_v1",
        "local_reference_summary": current_sell,
        "wide_summary": short_wide.get("wide_summary"),
        "wide_stability": short_wide.get("wide_stability"),
        "regime_map": short_regime.get("regime_map"),
        "buy_level_blockers": [
            "wide_window_mean_ret20_worse_than_baseline" if ((short_wide.get("wide_summary") or {}).get("delta") or {}).get("mean_ret20_delta", 0) < 0 else None,
            "wide_window_hit_rate_worse_than_baseline" if ((short_wide.get("wide_summary") or {}).get("delta") or {}).get("hit_rate_delta", 0) < 0 else None,
            "regime_map_contains_failure_regime" if any(str(row.get("regime_label")) == "failure_regime" for row in short_regime.get("regime_map") or []) else None,
        ],
    }
    current_sell_summary["buy_level_blockers"] = [item for item in current_sell_summary["buy_level_blockers"] if item]

    keep_candidates = [row for row in candidates if row["candidate_local_decision"] == "keep_as_buy_level_equivalent_research_candidate"]
    hold_candidates = [
        row
        for row in candidates
        if row["candidate_local_decision"] in {"hold_for_more_validation", "hold_for_dedicated_rerun"}
    ]
    if keep_candidates:
        final_decision = "keep_as_buy_level_equivalent_research_candidate"
        final_reason = "at_least_one_single_axis_sell_candidate_met_buy_level_contract"
    elif hold_candidates:
        final_decision = "hold_for_dedicated_rerun"
        final_reason = "sell_candidates_improved_precision_metrics_but_buy_level_requires_dedicated_refill_wide_and_regime_reruns"
    else:
        final_decision = "drop_signal_not_buy_level_equivalent"
        final_reason = "no_single_axis_sell_candidate_met_buy_level_or_hold_conditions"

    decision = {
        "schema_version": "tradex_sell_ranking_buy_level_goal_decision_v1",
        "generated_at": _utc_now(),
        "authoritative_rollup_decision": final_decision,
        "session_aggregate_decision": final_decision,
        "decision_reason": final_reason,
        "candidate_local_decisions": {
            row["candidate_id"]: row["candidate_local_decision"] for row in candidates
        },
        "buy_reference_metrics": {
            "authoritative_decision": buy_gate.get("authoritative_decision"),
            "promote_ready": (buy_gate.get("metrics") or {}).get("promote_ready", buy_gate.get("promote_ready")),
            "changed_top5_members_count": (buy_gate.get("metrics") or {}).get("changed_top5_members_count"),
            "changed_top10_members_count": (buy_gate.get("metrics") or {}).get("changed_top10_members_count"),
            "top5_uplift": (buy_gate.get("metrics") or {}).get("top5_uplift"),
            "top10_uplift": (buy_gate.get("metrics") or {}).get("top10_uplift"),
            "bad_pick_removal": (buy_gate.get("metrics") or {}).get("bad_pick_removal"),
        },
        "current_sell_summary": current_sell_summary,
        "no_silent_fallback": True,
        "silent_fallback_used": False,
        "research_fallback": False,
        "meemee_reflection": False,
        "production_ranking_changed": False,
        "remaining_risks": [
            "artifact_only_replay_no_new_candidate_refill",
            "sample_size_small",
            "wide_window_and_regime_failures_remain_until_revalidated",
        ],
    }

    compare = {
        "schema_version": "tradex_sell_ranking_buy_level_goal_compare_v1",
        "generated_at": _utc_now(),
        "baseline_row_count": len(baseline_rows),
        "candidate_results": candidates,
    }

    by_month = {
        "schema_version": "tradex_sell_ranking_buy_level_goal_by_month_v1",
        "generated_at": _utc_now(),
        "candidates": {row["candidate_id"]: row["monthly_stability"] for row in candidates},
    }
    by_regime = {
        "schema_version": "tradex_sell_ranking_buy_level_goal_by_regime_v1",
        "generated_at": _utc_now(),
        "candidates": {row["candidate_id"]: row["regime_stability"] for row in candidates},
    }
    reason_inventory = {
        "schema_version": "tradex_sell_ranking_reason_inventory_v1",
        "generated_at": _utc_now(),
        "reason_counts": dict(Counter(code for row in baseline_rows for code in row.get("reason_codes_v1", []))),
    }

    paths = {
        "contract": run_dir / "sell_ranking_buy_level_goal_contract.json",
        "compare": run_dir / "sell_ranking_buy_level_goal_compare.json",
        "by_month": run_dir / "sell_ranking_buy_level_goal_by_month.json",
        "by_regime": run_dir / "sell_ranking_buy_level_goal_by_regime.json",
        "reason_inventory": run_dir / "sell_ranking_reason_inventory.json",
        "decision": run_dir / "sell_ranking_buy_level_goal_decision.json",
        "complete": run_dir / "_ARTIFACT_COMPLETE.json",
    }
    _write_json(paths["contract"], contract)
    _write_json(paths["compare"], compare)
    _write_json(paths["by_month"], by_month)
    _write_json(paths["by_regime"], by_regime)
    _write_json(paths["reason_inventory"], reason_inventory)
    _write_json(paths["decision"], decision)
    _write_json(
        paths["complete"],
        {
            "schema_version": "tradex_sell_ranking_buy_level_goal_complete_v1",
            "generated_at": _utc_now(),
            "status": "complete",
            "artifact_refs": {key: str(path) for key, path in paths.items() if key != "complete"},
            "authoritative_decision": str(paths["decision"]),
            "silent_fallback_used": False,
            "research_fallback": False,
        },
    )
    return {
        "ok": True,
        "output_dir": str(run_dir),
        "authoritative_decision": final_decision,
        "decision_reason": final_reason,
        "artifact_refs": {key: str(path) for key, path in paths.items()},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate whether sell ranking reached buy R11-level evidence.")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args()
    result = run(output_root=Path(args.output_root))
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
