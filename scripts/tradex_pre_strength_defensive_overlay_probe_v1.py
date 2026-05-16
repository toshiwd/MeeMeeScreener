from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts import tradex_pre_strength_selected_family_candidate_generation_probe_v1 as probe_mod


AXIS_ID = "pre_strength_defensive_overlay_probe_v1"
DEFAULT_RUN_ID = "20260514T180000Z-pre-strength-defensive-overlay-probe-v1"
DEFAULT_OUTPUT_PARENT = Path(r"G:\Tradex\pre_strength_defensive_overlay_probe_v1")
DEFAULT_RISK_ROOT = Path(r"G:\Tradex\selected_family_risk_decomposition_v1\20260514T170000Z-selected-family-risk-decomposition-v1")

REQUIRED_OUTPUTS = [
    "evaluation_contract.json",
    "run_manifest.json",
    "source_artifact_refs.json",
    "defensive_overlay_contract.json",
    "overlay_variant_leaderboard.json",
    "top5_candidate_pool_report.json",
    "bad_pick_removal_report.json",
    "winner_suppression_report.json",
    "severe_loss_reduction_report.json",
    "human_selectable_day_report.json",
    "guardrail_report.json",
    "next_axis_recommendation.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
]

SAFE_FAMILY_GROUP = "pre_base_to_strength"
LABEL_COLUMNS = {"ret20_fwd", "mfe20", "mae20", "win20", "severe_loss20", "entry_next_open"}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--risk-root", type=Path, default=DEFAULT_RISK_ROOT)
    parser.add_argument("--output-parent", type=Path, default=DEFAULT_OUTPUT_PARENT)
    parser.add_argument("--run-id", default=DEFAULT_RUN_ID)
    args = parser.parse_args()
    run_pre_strength_defensive_overlay_probe_v1(risk_root=args.risk_root, output_parent=args.output_parent, run_id=args.run_id)
    return 0


def run_pre_strength_defensive_overlay_probe_v1(
    *,
    risk_root: Path = DEFAULT_RISK_ROOT,
    output_parent: Path = DEFAULT_OUTPUT_PARENT,
    run_id: str = DEFAULT_RUN_ID,
) -> dict[str, Any]:
    output_root = output_parent / run_id
    output_root.mkdir(parents=True, exist_ok=True)
    risk_root = risk_root.resolve()
    risk_decision = _read_json(risk_root / "research_decision.json")
    source_refs = _read_json(risk_root / "source_artifact_refs.json")
    probe_root = Path(source_refs["probe_root"])
    validation_root = Path(source_refs["validation_root"])
    pre_strength_root = Path(source_refs["pre_strength_root"])
    validation_report = _read_json(validation_root / "family_validation_report.json")
    events = probe_mod._prepare_events(_read_jsonl_frame(pre_strength_root / "pre_strength_event_ledger.jsonl"))
    safe_conditions = _safe_conditions(validation_report)
    variants = _overlay_variants()
    baseline = _select_topk(events, "baseline_previous_best", 5, lambda frame: frame["event_strength_score"])
    baseline3 = _select_topk(events, "baseline_previous_best", 3, lambda frame: frame["event_strength_score"])
    baseline10 = _select_topk(events, "baseline_previous_best", 10, lambda frame: frame["event_strength_score"])
    rows = []
    top5_rows = []
    guardrail_rows = []
    bad_rows = []
    winner_rows = []
    severe_rows = []
    human_rows = []
    for variant in variants:
        scored = _score_overlay(events, safe_conditions, variant)
        selected5 = _select_topk(scored, variant["variant_id"], 5, lambda frame: frame["overlay_score"])
        selected3 = _select_topk(scored, variant["variant_id"], 3, lambda frame: frame["overlay_score"])
        selected10 = _select_topk(scored, variant["variant_id"], 10, lambda frame: frame["overlay_score"])
        top5 = _top5_metrics(selected5, baseline, selected10, baseline10, events, variant["variant_id"])
        guard = _guardrail_metrics(selected3, baseline3, events, variant["variant_id"])
        bad = _bad_pick_report(selected5, baseline, variant["variant_id"])
        winner = _winner_suppression_report(selected5, baseline, variant["variant_id"])
        severe = _severe_loss_report(selected5, baseline, variant["variant_id"])
        human = _human_report(selected5, baseline, variant["variant_id"])
        rows.append(_leaderboard_row(variant, top5, guard, bad, winner, severe, human))
        top5_rows.append(top5)
        guardrail_rows.append(guard)
        bad_rows.append(bad)
        winner_rows.append(winner)
        severe_rows.append(severe)
        human_rows.append(human)

    leaderboard = {"schema_version": "tradex_pre_strength_defensive_overlay_variant_leaderboard_v1", "axis_id": AXIS_ID, "rows": rows}
    top5_report = {"schema_version": "tradex_pre_strength_defensive_overlay_top5_candidate_pool_report_v1", "axis_id": AXIS_ID, "rows": top5_rows}
    bad_report = {"schema_version": "tradex_pre_strength_defensive_overlay_bad_pick_removal_report_v1", "axis_id": AXIS_ID, "rows": bad_rows}
    winner_report = {"schema_version": "tradex_pre_strength_defensive_overlay_winner_suppression_report_v1", "axis_id": AXIS_ID, "rows": winner_rows}
    severe_report = {"schema_version": "tradex_pre_strength_defensive_overlay_severe_loss_reduction_report_v1", "axis_id": AXIS_ID, "rows": severe_rows}
    human_report = {"schema_version": "tradex_pre_strength_defensive_overlay_human_selectable_day_report_v1", "axis_id": AXIS_ID, "rows": human_rows}
    guardrail_report = {"schema_version": "tradex_pre_strength_defensive_overlay_guardrail_report_v1", "axis_id": AXIS_ID, "rows": guardrail_rows}
    decision = _decision(rows)
    next_axis = _next_axis(decision)
    research = _research_decision(decision, risk_decision)
    artifacts = {
        "evaluation_contract.json": _evaluation_contract(risk_root, risk_decision),
        "run_manifest.json": _run_manifest(output_root, risk_root),
        "source_artifact_refs.json": _source_refs(risk_root, probe_root, validation_root, pre_strength_root),
        "defensive_overlay_contract.json": _overlay_contract(safe_conditions, variants),
        "overlay_variant_leaderboard.json": leaderboard,
        "top5_candidate_pool_report.json": top5_report,
        "bad_pick_removal_report.json": bad_report,
        "winner_suppression_report.json": winner_report,
        "severe_loss_reduction_report.json": severe_report,
        "human_selectable_day_report.json": human_report,
        "guardrail_report.json": guardrail_report,
        "next_axis_recommendation.json": next_axis,
        "research_decision.json": research,
    }
    for name, payload in artifacts.items():
        _write_json(output_root / name, payload)
    complete = _artifact_complete(output_root, research)
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    return {
        "output_root": str(output_root),
        "overlay_variant_leaderboard": leaderboard,
        "top5_candidate_pool_report": top5_report,
        "bad_pick_removal_report": bad_report,
        "winner_suppression_report": winner_report,
        "severe_loss_reduction_report": severe_report,
        "human_selectable_day_report": human_report,
        "guardrail_report": guardrail_report,
        "next_axis_recommendation": next_axis,
        "research_decision": research,
        "artifact_complete": complete,
    }


def _overlay_variants() -> list[dict[str, Any]]:
    return [
        {"variant_id": "baseline_previous_best", "kind": "baseline", "safe_boost": 0.0, "unsafe_penalty": 0.0, "risky_penalty": 0.0},
        {"variant_id": "defensive_overlay_soft_penalty_v1", "kind": "soft_penalty", "safe_boost": 0.0, "unsafe_penalty": 1.25, "risky_penalty": 0.0},
        {"variant_id": "defensive_overlay_bad_pick_guard_v1", "kind": "bad_pick_guard", "safe_boost": 0.0, "unsafe_penalty": 0.0, "risky_penalty": 2.25},
        {"variant_id": "defensive_overlay_soft_boost_safe_v1", "kind": "soft_boost_safe", "safe_boost": 1.25, "unsafe_penalty": 0.0, "risky_penalty": 0.0},
        {"variant_id": "combined_defensive_overlay_v1", "kind": "combined", "safe_boost": 0.75, "unsafe_penalty": 0.75, "risky_penalty": 2.00},
    ]


def _score_overlay(events: pd.DataFrame, safe_conditions: Mapping[str, str], variant: Mapping[str, Any]) -> pd.DataFrame:
    out = events.copy()
    safe = _condition_mask(out, safe_conditions)
    risky = _risk_mask(out)
    out["overlay_safe_match"] = safe
    risky_outside_safe = risky & ~safe
    out["overlay_risky_match"] = risky_outside_safe
    out["overlay_score"] = pd.to_numeric(out["event_strength_score"], errors="coerce").fillna(0.0)
    out["overlay_score"] += safe.astype(float) * float(variant["safe_boost"])
    out["overlay_score"] -= (~safe).astype(float) * float(variant["unsafe_penalty"])
    out["overlay_score"] -= risky_outside_safe.astype(float) * float(variant["risky_penalty"])
    return out


def _select_topk(frame: pd.DataFrame, variant_id: str, k: int, score_fn: Any) -> pd.DataFrame:
    scored = frame.copy()
    scored["_selection_score"] = score_fn(scored)
    rows = []
    for _date, day in scored.groupby("event_date", sort=True):
        selected = day.sort_values(["_selection_score", "event_strength_score", "code"], ascending=[False, False, True]).head(k).copy()
        selected["variant_id"] = variant_id
        selected["selection_rank"] = range(1, len(selected) + 1)
        rows.append(selected)
    return pd.concat(rows, ignore_index=True) if rows else scored.head(0).copy()


def _top5_metrics(selected: pd.DataFrame, baseline: pd.DataFrame, selected10: pd.DataFrame, baseline10: pd.DataFrame, all_events: pd.DataFrame, variant_id: str) -> dict[str, Any]:
    selected_keys = _keys(selected)
    baseline_keys = _keys(baseline)
    all_big = int(all_events["future_big_winner"].sum())
    all_top10 = int(all_events["is_future_top10_by_ret20"].sum())
    return {
        "variant_id": variant_id,
        "top5_avg_ret20": _mean(selected["ret20_fwd"]),
        "top5_win_rate20": _rate(selected["win20"].astype(bool).sum(), len(selected)),
        "top5_severe_loss_rate20": _rate(selected["severe_loss20"].astype(bool).sum(), len(selected)),
        "top5_bad_pick_count": probe_mod._bad_pick_count(selected),
        "top5_big_winner_capture_rate": _rate(selected["future_big_winner"].sum(), all_big),
        "top5_future_top10_capture_rate": _rate(selected["is_future_top10_by_ret20"].sum(), all_top10),
        "human_selectable_day_rate": _human_day_rate(selected),
        "top5_candidate_diversity": _mean(selected.groupby("event_date")["code"].nunique()),
        "top5_changed_members_count_vs_baseline": len(selected_keys.symmetric_difference(baseline_keys)),
        "top10_changed_members_count_vs_baseline": len(_keys(selected10).symmetric_difference(_keys(baseline10))),
        "candidate_added_count": len(selected_keys - baseline_keys),
        "baseline": {
            "top5_avg_ret20": _mean(baseline["ret20_fwd"]),
            "top5_win_rate20": _rate(baseline["win20"].astype(bool).sum(), len(baseline)),
            "top5_severe_loss_rate20": _rate(baseline["severe_loss20"].astype(bool).sum(), len(baseline)),
            "top5_bad_pick_count": probe_mod._bad_pick_count(baseline),
            "top5_big_winner_capture_rate": _rate(baseline["future_big_winner"].sum(), all_big),
            "top5_future_top10_capture_rate": _rate(baseline["is_future_top10_by_ret20"].sum(), all_top10),
            "human_selectable_day_rate": _human_day_rate(baseline),
        },
    }


def _guardrail_metrics(selected: pd.DataFrame, baseline: pd.DataFrame, all_events: pd.DataFrame, variant_id: str) -> dict[str, Any]:
    oracle = probe_mod._oracle_topk_by_date(all_events, 3)
    return {
        "variant_id": variant_id,
        "top3_avg_ret20": _mean(selected["ret20_fwd"]),
        "top3_severe_loss_rate20": _rate(selected["severe_loss20"].astype(bool).sum(), len(selected)),
        "oracle_top3_gap": (_mean(oracle["ret20_fwd"]) or 0.0) - (_mean(selected["ret20_fwd"]) or 0.0),
        "selected_nonwinner_when_winner_available": probe_mod._nonwinner_when_winner_available(selected, all_events),
        "baseline": {
            "top3_avg_ret20": _mean(baseline["ret20_fwd"]),
            "top3_severe_loss_rate20": _rate(baseline["severe_loss20"].astype(bool).sum(), len(baseline)),
            "oracle_top3_gap": (_mean(oracle["ret20_fwd"]) or 0.0) - (_mean(baseline["ret20_fwd"]) or 0.0),
            "selected_nonwinner_when_winner_available": probe_mod._nonwinner_when_winner_available(baseline, all_events),
        },
    }


def _bad_pick_report(selected: pd.DataFrame, baseline: pd.DataFrame, variant_id: str) -> dict[str, Any]:
    selected_keys = _keys(selected)
    baseline_keys = _keys(baseline)
    removed = baseline[~baseline["event_member_key"].isin(selected_keys)]
    added = selected[~selected["event_member_key"].isin(baseline_keys)]
    bad_removed = _bad_frame(removed)
    bad_added = _bad_frame(added)
    return {
        "variant_id": variant_id,
        "bad_pick_removed_count": int(len(bad_removed)),
        "bad_pick_added_count": int(len(bad_added)),
        "net_bad_pick_removed_count": int(len(bad_removed) - len(bad_added)),
        "risky_candidate_demoted_count": int(removed.get("overlay_risky_match", pd.Series(False, index=removed.index)).astype(bool).sum()) if len(removed) else 0,
        "safe_candidate_promoted_count": int(added.get("overlay_safe_match", pd.Series(False, index=added.index)).astype(bool).sum()) if len(added) else 0,
        "removed_examples": _examples(bad_removed),
    }


def _winner_suppression_report(selected: pd.DataFrame, baseline: pd.DataFrame, variant_id: str) -> dict[str, Any]:
    selected_keys = _keys(selected)
    baseline_keys = _keys(baseline)
    suppressed = baseline[baseline["win20"].astype(bool) & ~baseline["event_member_key"].isin(selected_keys)]
    big_suppressed = suppressed[suppressed["future_big_winner"].astype(bool)] if len(suppressed) else suppressed
    return {
        "variant_id": variant_id,
        "winner_suppressed_count": int(len(suppressed)),
        "big_winner_suppressed_count": int(len(big_suppressed)),
        "winner_added_count": int((selected[~selected["event_member_key"].isin(baseline_keys)]["win20"].astype(bool)).sum()) if len(selected) else 0,
        "suppressed_examples": _examples(big_suppressed),
    }


def _severe_loss_report(selected: pd.DataFrame, baseline: pd.DataFrame, variant_id: str) -> dict[str, Any]:
    selected_keys = _keys(selected)
    removed = baseline[baseline["severe_loss20"].astype(bool) & ~baseline["event_member_key"].isin(selected_keys)]
    added = selected[selected["severe_loss20"].astype(bool) & ~selected["event_member_key"].isin(_keys(baseline))]
    return {
        "variant_id": variant_id,
        "severe_loss_reduced_count": int(len(removed)),
        "severe_loss_added_count": int(len(added)),
        "net_severe_loss_reduced_count": int(len(removed) - len(added)),
        "removed_examples": _examples(removed),
    }


def _human_report(selected: pd.DataFrame, baseline: pd.DataFrame, variant_id: str) -> dict[str, Any]:
    return {
        "variant_id": variant_id,
        "human_selectable_day_rate": _human_day_rate(selected),
        "baseline_human_selectable_day_rate": _human_day_rate(baseline),
        "human_selectable_day_rate_delta_vs_baseline": _human_day_rate(selected) - _human_day_rate(baseline),
        "days_with_3plus_usable": _human_day_count(selected),
        "baseline_days_with_3plus_usable": _human_day_count(baseline),
    }


def _leaderboard_row(variant: Mapping[str, Any], top5: Mapping[str, Any], guard: Mapping[str, Any], bad: Mapping[str, Any], winner: Mapping[str, Any], severe: Mapping[str, Any], human: Mapping[str, Any]) -> dict[str, Any]:
    base = top5["baseline"]
    guard_base = guard["baseline"]
    return {
        "variant_id": variant["variant_id"],
        "variant_kind": variant["kind"],
        "top5_avg_ret20_delta_vs_baseline": (top5["top5_avg_ret20"] or 0.0) - (base["top5_avg_ret20"] or 0.0),
        "top5_win_rate20_delta_vs_baseline": top5["top5_win_rate20"] - base["top5_win_rate20"],
        "top5_severe_loss_rate_delta_vs_baseline": top5["top5_severe_loss_rate20"] - base["top5_severe_loss_rate20"],
        "top5_bad_pick_count_delta_vs_baseline": int(top5["top5_bad_pick_count"]) - int(base["top5_bad_pick_count"]),
        "top5_big_winner_capture_rate_delta_vs_baseline": top5["top5_big_winner_capture_rate"] - base["top5_big_winner_capture_rate"],
        "top5_future_top10_capture_rate_delta_vs_baseline": top5["top5_future_top10_capture_rate"] - base["top5_future_top10_capture_rate"],
        "human_selectable_day_rate_delta_vs_baseline": human["human_selectable_day_rate_delta_vs_baseline"],
        "top5_candidate_diversity": top5["top5_candidate_diversity"],
        "top5_changed_members_count_vs_baseline": top5["top5_changed_members_count_vs_baseline"],
        "top3_avg_ret20_delta_vs_baseline": (guard["top3_avg_ret20"] or 0.0) - (guard_base["top3_avg_ret20"] or 0.0),
        "top3_severe_loss_rate_delta_vs_baseline": guard["top3_severe_loss_rate20"] - guard_base["top3_severe_loss_rate20"],
        "oracle_top3_gap_delta_vs_baseline": guard["oracle_top3_gap"] - guard_base["oracle_top3_gap"],
        "selected_nonwinner_when_winner_available_delta_vs_baseline": guard["selected_nonwinner_when_winner_available"] - guard_base["selected_nonwinner_when_winner_available"],
        "severe_loss_reduced_count": severe["net_severe_loss_reduced_count"],
        "bad_pick_removed_count": bad["net_bad_pick_removed_count"],
        "winner_suppressed_count": winner["winner_suppressed_count"],
        "big_winner_suppressed_count": winner["big_winner_suppressed_count"],
        "safe_candidate_promoted_count": bad["safe_candidate_promoted_count"],
        "risky_candidate_demoted_count": bad["risky_candidate_demoted_count"],
        "net_tradeoff_removed_bad_pick_vs_lost_winner": bad["net_bad_pick_removed_count"] - winner["winner_suppressed_count"],
    }


def _decision(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    variant_decisions = []
    for row in rows:
        if row["variant_id"] == "baseline_previous_best":
            continue
        severe_ok = row["top5_severe_loss_rate_delta_vs_baseline"] < 0.0
        bad_ok = row["top5_bad_pick_count_delta_vs_baseline"] < 0
        avg_ok = row["top5_avg_ret20_delta_vs_baseline"] >= 0.0
        big_ok = row["top5_big_winner_capture_rate_delta_vs_baseline"] >= -0.02
        top10_ok = row["top5_future_top10_capture_rate_delta_vs_baseline"] >= -0.02
        human_ok = row["human_selectable_day_rate_delta_vs_baseline"] >= 0.0
        top3_ok = row["top3_avg_ret20_delta_vs_baseline"] >= -0.01 and row["top3_severe_loss_rate_delta_vs_baseline"] <= 0.02
        if severe_ok and bad_ok and avg_ok and big_ok and top10_ok and human_ok and top3_ok:
            decision = "defensive_overlay_keep_candidate"
            reason = "bad_pick_and_severe_loss_reduced_without_material_capture_loss"
        elif severe_ok and bad_ok and avg_ok and top3_ok:
            decision = "defensive_overlay_hold"
            reason = "risk_reduced_but_capture_or_human_selectable_guardrail_needs_tuning"
        else:
            decision = "defensive_overlay_drop"
            reason = "overlay_failed_to_reduce_risk_without_quality_or_capture_damage"
        variant_decisions.append({"variant_id": row["variant_id"], "decision": decision, "decision_reason": reason, "metrics": row})
    keep = [row for row in variant_decisions if row["decision"] == "defensive_overlay_keep_candidate"]
    hold = [row for row in variant_decisions if row["decision"] == "defensive_overlay_hold"]
    if keep:
        decision = "defensive_overlay_keep_candidate"
        reason = "at_least_one_overlay_variant_reduces_bad_picks_without_material_capture_loss"
        best = keep[0]["variant_id"]
    elif hold:
        decision = "defensive_overlay_hold"
        reason = "overlay_reduces_bad_picks_or_severe_loss_but_needs_strength_or_family_specific_tuning"
        best = hold[0]["variant_id"]
    else:
        decision = "defensive_overlay_drop"
        reason = "no_overlay_variant_passed_defensive_value_gate"
        best = variant_decisions[0]["variant_id"] if variant_decisions else None
    return {"decision": decision, "decision_reason": reason, "best_variant_id": best, "variant_decisions": variant_decisions}


def _next_axis(decision: Mapping[str, Any]) -> dict[str, Any]:
    if decision["decision"] == "defensive_overlay_keep_candidate":
        next_axis = "starter_entry_candidate_pretest_v1"
    elif decision["decision"] == "defensive_overlay_hold":
        next_axis = "defensive_overlay_strength_sweep_v1"
    else:
        next_axis = "pattern_family_portfolio_refresh_v1"
    return {
        "schema_version": "tradex_pre_strength_defensive_overlay_next_axis_recommendation_v1",
        "axis_id": AXIS_ID,
        "decision": decision["decision"],
        "next": next_axis,
        "activation_allowed": False,
        "meemee_reflection_allowed": False,
    }


def _research_decision(decision: Mapping[str, Any], risk_decision: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "tradex_pre_strength_defensive_overlay_probe_research_decision_v1",
        "axis_id": AXIS_ID,
        "decision": decision["decision"],
        "decision_reason": decision["decision_reason"],
        "best_variant_id": decision["best_variant_id"],
        "variant_decisions": decision["variant_decisions"],
        "source_risk_decomposition_decision": risk_decision.get("decision"),
        "activation_allowed": False,
        "production_ranking_changed": False,
        "meemee_reflectable": False,
        "publish_created": False,
        "candidate_scoring_created": False,
        "ranking_objective_created": False,
        "threshold_policy_created": False,
        "silent_fallback_used": False,
        "generated_at_utc": _utc_now(),
    }


def _safe_conditions(validation_report: Mapping[str, Any]) -> dict[str, str]:
    for row in validation_report.get("rows") or []:
        if row.get("family_group") == SAFE_FAMILY_GROUP or SAFE_FAMILY_GROUP in str(row.get("family_id")):
            conditions = dict(row.get("conditions") or {})
            if set(conditions).intersection(LABEL_COLUMNS):
                raise ValueError("future label condition found in safe overlay conditions")
            return conditions
    raise RuntimeError("safe family conditions not found")


def _condition_mask(frame: pd.DataFrame, conditions: Mapping[str, str]) -> pd.Series:
    mask = pd.Series(True, index=frame.index)
    for key, value in conditions.items():
        if key in LABEL_COLUMNS:
            raise ValueError(f"future label condition is forbidden: {key}")
        mask &= frame[key].astype(str).eq(str(value)) if key in frame.columns else False
    return mask


def _risk_mask(frame: pd.DataFrame) -> pd.Series:
    risk = pd.Series(False, index=frame.index)
    if "pre_wick_warning_state" in frame.columns:
        risk |= frame["pre_wick_warning_state"].astype(str).isin({"pre_upper_wick_or_failed_push"})
    if "monthly_prior_state" in frame.columns:
        risk |= frame["monthly_prior_state"].astype(str).isin({"monthly_prior_down_or_drawdown", "monthly_prior_downtrend"})
    if "weekly_prior_state" in frame.columns:
        risk |= frame["weekly_prior_state"].astype(str).isin({"weekly_prior_mixed", "weekly_prior_downtrend"})
    if "event_daily_ret20_state" in frame.columns:
        risk |= frame["event_daily_ret20_state"].astype(str).isin({"daily20_down"})
    return risk


def _bad_frame(frame: pd.DataFrame) -> pd.DataFrame:
    return frame[(~frame["win20"].astype(bool)) | frame["severe_loss20"].astype(bool)].copy() if len(frame) else frame.copy()


def _keys(frame: pd.DataFrame) -> set[str]:
    return set(frame["event_member_key"].astype(str)) if len(frame) else set()


def _human_day_rate(frame: pd.DataFrame) -> float:
    total = 0
    good = 0
    for _date, day in frame.groupby("event_date", sort=True):
        total += 1
        if int(day["human_selectable"].astype(bool).sum()) >= 3:
            good += 1
    return _rate(good, total)


def _human_day_count(frame: pd.DataFrame) -> int:
    return sum(1 for _date, day in frame.groupby("event_date", sort=True) if int(day["human_selectable"].astype(bool).sum()) >= 3)


def _examples(frame: pd.DataFrame, limit: int = 20) -> list[dict[str, Any]]:
    columns = ["code", "event_date", "event_strength_score", "ret20_fwd", "win20", "severe_loss20", "future_big_winner", "overlay_safe_match", "overlay_risky_match"]
    if frame.empty:
        return []
    return frame.sort_values(["ret20_fwd", "event_strength_score"], ascending=[True, False])[[c for c in columns if c in frame.columns]].head(limit).to_dict("records")


def _evaluation_contract(risk_root: Path, risk_decision: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "tradex_pre_strength_defensive_overlay_evaluation_contract_v1",
        "axis_id": AXIS_ID,
        "boundary": "TRADEX-only",
        "source_risk_root": str(risk_root),
        "source_risk_decision": risk_decision.get("decision"),
        "primary_metric_scope": "top5_candidate_pool_quality",
        "overlay_role": "defensive_risk_modifier_not_candidate_generator",
        "future_labels_used_for_overlay_scoring": False,
        "future_labels_used_for_evaluation": True,
        "silent_fallback_used": False,
    }


def _run_manifest(output_root: Path, risk_root: Path) -> dict[str, Any]:
    return {"schema_version": "tradex_pre_strength_defensive_overlay_run_manifest_v1", "axis_id": AXIS_ID, "run_id": output_root.name, "source_risk_root": str(risk_root), "generated_at_utc": _utc_now(), "silent_fallback_used": False}


def _source_refs(risk_root: Path, probe_root: Path, validation_root: Path, pre_strength_root: Path) -> dict[str, Any]:
    return {"schema_version": "tradex_pre_strength_defensive_overlay_source_refs_v1", "axis_id": AXIS_ID, "risk_root": str(risk_root), "probe_root": str(probe_root), "validation_root": str(validation_root), "pre_strength_root": str(pre_strength_root)}


def _overlay_contract(safe_conditions: Mapping[str, str], variants: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": "tradex_pre_strength_defensive_overlay_contract_v1",
        "axis_id": AXIS_ID,
        "safe_family_group": SAFE_FAMILY_GROUP,
        "safe_conditions": dict(safe_conditions),
        "risk_condition_source": "observable_pre_event_and_event_state_only",
        "variants": list(variants),
        "hard_filter_used": False,
        "candidate_generator_created": False,
        "future_label_inputs_used_for_overlay": False,
        "not_changed": _not_changed(),
        "silent_fallback_used": False,
    }


def _artifact_complete(output_root: Path, decision: Mapping[str, Any]) -> dict[str, Any]:
    presence = {name: (output_root / name).exists() for name in REQUIRED_OUTPUTS if name != "_ARTIFACT_COMPLETE.json"}
    presence["_ARTIFACT_COMPLETE.json"] = True
    return {"schema_version": "tradex_pre_strength_defensive_overlay_artifact_complete_v1", "axis_id": AXIS_ID, "decision": decision.get("decision"), "complete": all(presence.values()), "required_outputs": REQUIRED_OUTPUTS, "present_outputs": presence, "output_root": str(output_root), "silent_fallback_used": False}


def _not_changed() -> list[str]:
    return ["candidate_generation_role", "starter_entry_pretest", "hard_filter_policy", "MeeMee_runtime", "active_ranking", "display_score", "runtime_DuckDB", "production_registry", "teppan_watch_policy", "threshold_no_trade", "image_fusion", "sell_side", "exit_optimization", "cost_slippage_liquidity"]


def _mean(values: Any) -> float | None:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    if numeric.empty:
        return None
    return float(numeric.mean())


def _rate(count: Any, total: Any) -> float:
    total_value = float(total or 0)
    return 0.0 if total_value == 0.0 else float(count or 0) / total_value


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    return data if isinstance(data, dict) else {}


def _read_jsonl_frame(path: Path) -> pd.DataFrame:
    rows = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return pd.DataFrame(rows)


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(dict(payload)), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
