from __future__ import annotations

import argparse
import json
import math
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts import tradex_pre_strength_selected_family_candidate_generation_probe_v1 as probe_mod


AXIS_ID = "selected_family_risk_decomposition_v1"
DEFAULT_RUN_ID = "20260514T170000Z-selected-family-risk-decomposition-v1"
DEFAULT_OUTPUT_PARENT = Path(r"G:\Tradex\selected_family_risk_decomposition_v1")
DEFAULT_PROBE_ROOT = Path(
    r"G:\Tradex\pre_strength_selected_family_candidate_generation_probe_v1"
    r"\20260514T160000Z-pre-strength-selected-family-candidate-generation-probe-v1"
)

REQUIRED_OUTPUTS = [
    "evaluation_contract.json",
    "run_manifest.json",
    "source_artifact_refs.json",
    "risk_decomposition_contract.json",
    "big_winner_capture_loss_report.json",
    "future_top10_capture_loss_report.json",
    "family_contribution_report.json",
    "variant_tradeoff_report.json",
    "human_selectable_day_diagnosis.json",
    "lost_winner_source_report.json",
    "next_design_recommendation.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--probe-root", type=Path, default=DEFAULT_PROBE_ROOT)
    parser.add_argument("--output-parent", type=Path, default=DEFAULT_OUTPUT_PARENT)
    parser.add_argument("--run-id", default=DEFAULT_RUN_ID)
    args = parser.parse_args()
    run_selected_family_risk_decomposition_v1(
        probe_root=args.probe_root,
        output_parent=args.output_parent,
        run_id=args.run_id,
    )
    return 0


def run_selected_family_risk_decomposition_v1(
    *,
    probe_root: Path = DEFAULT_PROBE_ROOT,
    output_parent: Path = DEFAULT_OUTPUT_PARENT,
    run_id: str = DEFAULT_RUN_ID,
) -> dict[str, Any]:
    output_root = output_parent / run_id
    output_root.mkdir(parents=True, exist_ok=True)
    probe_root = probe_root.resolve()
    probe_contract = _read_json(probe_root / "probe_contract.json")
    probe_decision = _read_json(probe_root / "candidate_generation_probe_decision.json")
    baseline_comparison = _read_json(probe_root / "baseline_comparison_report.json")
    variants_payload = _read_json(probe_root / "candidate_generation_variants.json")
    validation_root = Path(probe_contract["source_validation_root"])
    validation_report = _read_json(validation_root / "family_validation_report.json")
    source_root = Path(probe_contract["source_pre_strength_root"])
    events = probe_mod._prepare_events(_read_jsonl_frame(source_root / "pre_strength_event_ledger.jsonl"))
    variants = variants_payload.get("variants") or []
    validation_rows = validation_report.get("rows") or []

    evaluation_contract = _evaluation_contract(probe_root, probe_contract, probe_decision)
    run_manifest = _run_manifest(output_root, probe_root, source_root)
    refs = _source_refs(probe_root, validation_root, source_root)
    risk_contract = _risk_contract(probe_contract)
    variant_contexts = [_variant_context(events, variant, validation_rows) for variant in variants]
    big_loss = _capture_loss_report(variant_contexts, loss_kind="big_winner")
    future_top10_loss = _capture_loss_report(variant_contexts, loss_kind="future_top10")
    contribution = _family_contribution_report(variant_contexts)
    tradeoff = _variant_tradeoff_report(variant_contexts, baseline_comparison)
    human = _human_selectable_day_diagnosis(variant_contexts)
    lost_source = _lost_winner_source_report(variant_contexts, validation_rows)
    recommendation = _next_design_recommendation(big_loss, future_top10_loss, contribution, tradeoff, human, lost_source)
    research = _research_decision(recommendation, probe_decision)

    payloads = {
        "evaluation_contract.json": evaluation_contract,
        "run_manifest.json": run_manifest,
        "source_artifact_refs.json": refs,
        "risk_decomposition_contract.json": risk_contract,
        "big_winner_capture_loss_report.json": big_loss,
        "future_top10_capture_loss_report.json": future_top10_loss,
        "family_contribution_report.json": contribution,
        "variant_tradeoff_report.json": tradeoff,
        "human_selectable_day_diagnosis.json": human,
        "lost_winner_source_report.json": lost_source,
        "next_design_recommendation.json": recommendation,
        "research_decision.json": research,
    }
    for name, payload in payloads.items():
        _write_json(output_root / name, payload)
    complete = _artifact_complete(output_root, research)
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    return {
        "output_root": str(output_root),
        "evaluation_contract": evaluation_contract,
        "big_winner_capture_loss_report": big_loss,
        "future_top10_capture_loss_report": future_top10_loss,
        "family_contribution_report": contribution,
        "variant_tradeoff_report": tradeoff,
        "human_selectable_day_diagnosis": human,
        "lost_winner_source_report": lost_source,
        "next_design_recommendation": recommendation,
        "research_decision": research,
        "artifact_complete": complete,
    }


def _variant_context(events: pd.DataFrame, variant: Mapping[str, Any], validation_rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    family_masks = {family_id: probe_mod._condition_mask(events, conditions) for family_id, conditions in variant["conditions_by_family"].items()}
    variant_mask = pd.Series(False, index=events.index)
    for mask in family_masks.values():
        variant_mask |= mask
    variant_events = events.loc[variant_mask].copy()
    active_dates = sorted(variant_events["event_date"].astype(str).unique().tolist())
    eval_events = events[events["event_date"].astype(str).isin(active_dates)].copy()
    baseline_top5 = probe_mod._topk_by_date(eval_events, probe_mod.TOP5_K, "baseline_top5")
    variant_top5 = probe_mod._topk_by_date(variant_events, probe_mod.TOP5_K, str(variant["variant_id"]))
    baseline_top10 = probe_mod._topk_by_date(eval_events, probe_mod.TOP10_K, "baseline_top10")
    variant_top10 = probe_mod._topk_by_date(variant_events, probe_mod.TOP10_K, str(variant["variant_id"]))
    family_ids_by_key = _family_ids_by_event_key(events, family_masks)
    dropped_masks = _dropped_family_masks(events, validation_rows)
    return {
        "variant": variant,
        "family_masks": family_masks,
        "family_ids_by_key": family_ids_by_key,
        "dropped_masks": dropped_masks,
        "variant_events": variant_events,
        "eval_events": eval_events,
        "active_dates": active_dates,
        "baseline_top5": baseline_top5,
        "variant_top5": variant_top5,
        "baseline_top10": baseline_top10,
        "variant_top10": variant_top10,
    }


def _capture_loss_report(contexts: Sequence[Mapping[str, Any]], *, loss_kind: str) -> dict[str, Any]:
    rows = []
    for ctx in contexts:
        baseline = ctx["baseline_top5"] if loss_kind == "big_winner" else ctx["baseline_top10"]
        variant = ctx["variant_top5"] if loss_kind == "big_winner" else ctx["variant_top10"]
        flag = "future_big_winner" if loss_kind == "big_winner" else "is_future_top10_by_ret20"
        lost = baseline[baseline[flag].astype(bool) & ~baseline["event_member_key"].isin(set(variant["event_member_key"].astype(str)))].copy()
        excluded = _excluded_or_underranked(lost, ctx["variant_events"], variant)
        rows.append(
            {
                "variant_id": ctx["variant"]["variant_id"],
                f"lost_{loss_kind}_count": int(len(lost)),
                "excluded_by_family_definition_count": excluded["excluded"],
                "inside_family_but_under_ranked_count": excluded["under_ranked"],
                "lost_overlap_with_baseline_top5_count": int(lost["event_member_key"].isin(set(ctx["baseline_top5"]["event_member_key"].astype(str))).sum())
                if len(lost)
                else 0,
                "lost_pattern_tag_counts": _pattern_tag_counts(lost),
                "lost_examples": _example_rows(lost),
            }
        )
    return {
        "schema_version": f"tradex_selected_family_{loss_kind}_capture_loss_report_v1",
        "axis_id": AXIS_ID,
        "loss_kind": loss_kind,
        "rows": rows,
    }


def _family_contribution_report(contexts: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    rows = []
    for ctx in contexts:
        selected_keys = set(ctx["variant_top5"]["event_member_key"].astype(str))
        for family_id, mask in ctx["family_masks"].items():
            candidates = ctx["eval_events"].loc[mask.reindex(ctx["eval_events"].index, fill_value=False)].copy()
            selected = ctx["variant_top5"][ctx["variant_top5"]["event_member_key"].astype(str).isin(set(candidates["event_member_key"].astype(str)))]
            rows.append(
                {
                    "variant_id": ctx["variant"]["variant_id"],
                    "family_id": family_id,
                    "candidate_count": int(len(candidates)),
                    "selected_count": int(len(selected)),
                    "avg_ret20": _mean(selected["ret20_fwd"]),
                    "win_rate20": _rate(selected["win20"].astype(bool).sum(), len(selected)),
                    "severe_loss_rate20": _rate(selected["severe_loss20"].astype(bool).sum(), len(selected)),
                    "big_winner_capture": _rate(selected["future_big_winner"].sum(), ctx["eval_events"]["future_big_winner"].sum()),
                    "future_top10_capture": _rate(selected["is_future_top10_by_ret20"].sum(), ctx["eval_events"]["is_future_top10_by_ret20"].sum()),
                    "bad_pick_count": probe_mod._bad_pick_count(selected),
                    "human_selectable_day_rate": _human_selectable_day_rate(selected),
                    "diversity_contribution": _rate(len(set(selected["event_member_key"].astype(str))), len(selected_keys)),
                }
            )
    return {"schema_version": "tradex_selected_family_contribution_report_v1", "axis_id": AXIS_ID, "rows": rows}


def _variant_tradeoff_report(contexts: Sequence[Mapping[str, Any]], baseline_comparison: Mapping[str, Any]) -> dict[str, Any]:
    comparison_by_variant = {row["variant_id"]: row for row in baseline_comparison.get("rows") or []}
    rows = []
    for ctx in contexts:
        variant_id = ctx["variant"]["variant_id"]
        comparison = comparison_by_variant.get(variant_id, {})
        rows.append(
            {
                "variant_id": variant_id,
                "profile": _variant_profile(variant_id),
                "improves_safety": comparison.get("top5_severe_loss_rate_delta_vs_baseline", 0.0) < 0
                and comparison.get("top5_bad_pick_count_delta_vs_baseline", 0) < 0,
                "improves_average_quality": comparison.get("top5_avg_ret20_delta_vs_baseline", 0.0) > 0,
                "loses_winner_capture": comparison.get("top5_big_winner_capture_rate_delta_vs_baseline", 0.0) < 0,
                "loses_future_top10_capture": comparison.get("top5_future_top10_capture_rate_delta_vs_baseline", 0.0) < 0,
                "loses_human_selectable_day_rate": comparison.get("human_selectable_day_rate_delta_vs_baseline", 0.0) < 0,
                "comparison": comparison,
            }
        )
    return {"schema_version": "tradex_selected_family_variant_tradeoff_report_v1", "axis_id": AXIS_ID, "rows": rows}


def _human_selectable_day_diagnosis(contexts: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    rows = []
    for ctx in contexts:
        day_buckets = Counter()
        weak_count = 0
        safe_low_upside = 0
        high_upside_risky = 0
        for _date, day in ctx["variant_top5"].groupby("event_date", sort=True):
            usable = int((day["human_selectable"].astype(bool)).sum())
            bucket = "3plus" if usable >= 3 else str(usable)
            day_buckets[bucket] += 1
            weak_count += int((~day["win20"].astype(bool) | day["severe_loss20"].astype(bool)).sum())
            safe_low_upside += int((day["ret20_fwd"].between(0.0, 0.03, inclusive="left") & ~day["severe_loss20"].astype(bool)).sum())
            high_upside_risky += int((day["ret20_fwd"].ge(0.10) & day["severe_loss20"].astype(bool)).sum())
        total_days = sum(day_buckets.values())
        rows.append(
            {
                "variant_id": ctx["variant"]["variant_id"],
                "evaluation_day_count": total_days,
                "days_with_0_usable": day_buckets["0"],
                "days_with_1_usable": day_buckets["1"],
                "days_with_2_usable": day_buckets["2"],
                "days_with_3plus_usable": day_buckets["3plus"],
                "human_selectable_day_rate": _rate(day_buckets["3plus"], total_days),
                "top5_candidate_diversity": _mean(ctx["variant_top5"].groupby("event_date")["code"].nunique()),
                "top5_weak_candidate_count": weak_count,
                "top5_safe_but_low_upside_count": safe_low_upside,
                "top5_high_upside_but_risky_count": high_upside_risky,
                "loss_cause": _human_loss_cause(day_buckets, weak_count, safe_low_upside),
            }
        )
    return {"schema_version": "tradex_human_selectable_day_diagnosis_v1", "axis_id": AXIS_ID, "rows": rows}


def _lost_winner_source_report(contexts: Sequence[Mapping[str, Any]], validation_rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    rows = []
    dropped = [row for row in validation_rows if str(row.get("validation_decision", "")).startswith("drop")]
    for ctx in contexts:
        baseline = ctx["baseline_top5"]
        variant = ctx["variant_top5"]
        lost = baseline[baseline["future_big_winner"].astype(bool) & ~baseline["event_member_key"].isin(set(variant["event_member_key"].astype(str)))].copy()
        dropped_counts = {}
        for row in dropped:
            family_id = str(row["family_id"])
            mask = probe_mod._condition_mask(lost, row.get("conditions") or {}) if len(lost) else pd.Series(dtype=bool)
            dropped_counts[family_id] = int(mask.sum()) if len(lost) else 0
        rows.append(
            {
                "variant_id": ctx["variant"]["variant_id"],
                "lost_big_winner_count": int(len(lost)),
                "lost_big_winners_matching_dropped_family_count": int(sum(dropped_counts.values())),
                "dropped_family_match_counts": dropped_counts,
                "mostly_in_dropped_pre_reclaim_accumulation": bool(len(lost) and sum(dropped_counts.values()) / len(lost) >= 0.35),
                "dominant_lost_pattern_tags": _pattern_tag_counts(lost)[:20],
            }
        )
    return {"schema_version": "tradex_lost_winner_source_report_v1", "axis_id": AXIS_ID, "rows": rows}


def _next_design_recommendation(
    big_loss: Mapping[str, Any],
    top10_loss: Mapping[str, Any],
    contribution: Mapping[str, Any],
    tradeoff: Mapping[str, Any],
    human: Mapping[str, Any],
    lost_source: Mapping[str, Any],
) -> dict[str, Any]:
    trade_rows = tradeoff.get("rows") or []
    contrib_rows = contribution.get("rows") or []
    lost_rows = lost_source.get("rows") or []
    base_safe = any(row["profile"] == "safety_profile" and row["improves_safety"] for row in trade_rows)
    upside_family = any(row["profile"] == "upside_profile" and row["improves_average_quality"] for row in trade_rows)
    combined_bad_capture = any(row["variant_id"] == "combined_keep_families" and row["loses_winner_capture"] for row in trade_rows)
    dropped_winner_share = max((_rate(row["lost_big_winners_matching_dropped_family_count"], row["lost_big_winner_count"]) for row in lost_rows), default=0.0)
    human_structural = all((row.get("human_selectable_day_rate") or 0.0) < 0.20 for row in human.get("rows") or [])
    if base_safe and upside_family and not human_structural:
        decision = "family_blend_probe_ready"
        next_axis = "safe_plus_upside_family_blend_probe_v1"
        reason = "safe_family_and_upside_family_roles_are_separable"
    elif base_safe and dropped_winner_share >= 0.25:
        decision = "relax_family_definition_ready"
        next_axis = "pre_base_to_strength_relaxed_definition_probe_v1"
        reason = "lost_winners_cluster_near_dropped_or_adjacent_family_conditions"
    elif base_safe and combined_bad_capture:
        decision = "defensive_filter_only"
        next_axis = "pre_strength_defensive_overlay_probe_v1"
        reason = "safety_improves_but_candidate_generation_capture_loss_remains_structural"
    else:
        decision = "drop_pre_strength_family_group"
        next_axis = "pattern_family_portfolio_refresh_v1"
        reason = "lost_winners_or_human_selectable_gap_not_recoverable_in_current_family_group"
    return {
        "schema_version": "tradex_selected_family_next_design_recommendation_v1",
        "axis_id": AXIS_ID,
        "decision": decision,
        "decision_reason": reason,
        "next": next_axis,
        "safe_family_core_detected": base_safe,
        "upside_family_detected": upside_family,
        "combined_variant_loses_capture": combined_bad_capture,
        "lost_winner_dropped_family_max_share": dropped_winner_share,
        "human_selectable_gap_structural": human_structural,
        "activation_allowed": False,
        "meemee_reflection_allowed": False,
    }


def _research_decision(recommendation: Mapping[str, Any], probe_decision: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "tradex_selected_family_risk_decomposition_research_decision_v1",
        "axis_id": AXIS_ID,
        "decision": recommendation["decision"],
        "decision_reason": recommendation["decision_reason"],
        "next": recommendation["next"],
        "source_probe_decision": probe_decision.get("decision"),
        "diagnosis_only": True,
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


def _excluded_or_underranked(lost: pd.DataFrame, variant_events: pd.DataFrame, variant_selected: pd.DataFrame) -> dict[str, int]:
    variant_event_keys = set(variant_events["event_member_key"].astype(str)) if len(variant_events) else set()
    variant_selected_keys = set(variant_selected["event_member_key"].astype(str)) if len(variant_selected) else set()
    excluded = 0
    under_ranked = 0
    for key in lost["event_member_key"].astype(str).tolist() if len(lost) else []:
        if key not in variant_event_keys:
            excluded += 1
        elif key not in variant_selected_keys:
            under_ranked += 1
    return {"excluded": excluded, "under_ranked": under_ranked}


def _dropped_family_masks(events: pd.DataFrame, validation_rows: Sequence[Mapping[str, Any]]) -> dict[str, pd.Series]:
    masks = {}
    for row in validation_rows:
        if str(row.get("validation_decision", "")).startswith("drop"):
            masks[str(row["family_id"])] = probe_mod._condition_mask(events, row.get("conditions") or {})
    return masks


def _family_ids_by_event_key(events: pd.DataFrame, family_masks: Mapping[str, pd.Series]) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for family_id, mask in family_masks.items():
        for key in events.loc[mask, "event_member_key"].astype(str):
            out.setdefault(key, []).append(family_id)
    return out


def _pattern_tag_counts(frame: pd.DataFrame) -> list[dict[str, Any]]:
    columns = [
        "pre_ma20_path_state",
        "pre_ret20_state",
        "pre_ret5_state",
        "weekly_prior_state",
        "monthly_prior_state",
        "event_daily_ret20_state",
        "event_daily_candle_state",
    ]
    counter: Counter[str] = Counter()
    for _idx, row in frame.iterrows() if len(frame) else []:
        for column in columns:
            if column in row:
                counter[f"{column}={row[column]}"] += 1
    return [{"tag": tag, "count": count} for tag, count in counter.most_common()]


def _example_rows(frame: pd.DataFrame, limit: int = 20) -> list[dict[str, Any]]:
    columns = [
        "code",
        "event_date",
        "event_month",
        "event_strength_score",
        "ret20_fwd",
        "future_big_winner",
        "is_future_top10_by_ret20",
        "severe_loss20",
        "win20",
        "pre_ma20_path_state",
        "weekly_prior_state",
        "monthly_prior_state",
    ]
    if frame.empty:
        return []
    return frame.sort_values(["ret20_fwd", "event_strength_score"], ascending=[False, False])[[c for c in columns if c in frame.columns]].head(limit).to_dict("records")


def _human_selectable_day_rate(selected: pd.DataFrame) -> float:
    if selected.empty:
        return 0.0
    good_days = 0
    total = 0
    for _date, day in selected.groupby("event_date", sort=True):
        total += 1
        if int(day["human_selectable"].astype(bool).sum()) >= 3:
            good_days += 1
    return _rate(good_days, total)


def _human_loss_cause(day_buckets: Counter[str], weak_count: int, safe_low_upside: int) -> str:
    if day_buckets["0"] + day_buckets["1"] > day_buckets["2"] + day_buckets["3plus"]:
        return "too_few_usable_candidates_per_day"
    if weak_count > safe_low_upside:
        return "weak_or_risky_candidates_in_top5"
    return "safe_but_low_upside_candidates_limit_breadth"


def _variant_profile(variant_id: str) -> str:
    if "pre_base_to_strength" in variant_id:
        return "safety_profile"
    if "pre_to_event_confirmation" in variant_id:
        return "upside_profile"
    return "blend_profile"


def _evaluation_contract(probe_root: Path, probe_contract: Mapping[str, Any], probe_decision: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "tradex_selected_family_risk_decomposition_evaluation_contract_v1",
        "axis_id": AXIS_ID,
        "boundary": "TRADEX-only",
        "source_probe_root": str(probe_root),
        "source_probe_decision": probe_decision.get("decision"),
        "evaluation_role": "diagnosis_only",
        "fixed_conditions": {
            "source_pre_strength_root": probe_contract.get("source_pre_strength_root"),
            "evaluation_date_policy": probe_contract.get("evaluation_date_policy"),
            "baseline_policy": probe_contract.get("baseline_policy"),
            "variant_policy": probe_contract.get("variant_policy"),
        },
        "future_label_policy": probe_contract.get("future_label_policy"),
        "not_changed": _not_changed(),
        "silent_fallback_used": False,
    }


def _run_manifest(output_root: Path, probe_root: Path, source_root: Path) -> dict[str, Any]:
    return {
        "schema_version": "tradex_selected_family_risk_decomposition_run_manifest_v1",
        "axis_id": AXIS_ID,
        "run_id": output_root.name,
        "generated_at_utc": _utc_now(),
        "input_artifacts": {
            "source_probe_root": str(probe_root),
            "source_pre_strength_root": str(source_root),
        },
        "output_root": str(output_root),
        "silent_fallback_used": False,
    }


def _source_refs(probe_root: Path, validation_root: Path, source_root: Path) -> dict[str, Any]:
    return {
        "schema_version": "tradex_selected_family_risk_decomposition_source_artifact_refs_v1",
        "axis_id": AXIS_ID,
        "probe_root": str(probe_root),
        "validation_root": str(validation_root),
        "pre_strength_root": str(source_root),
        "required_inputs": [
            str(probe_root / "candidate_generation_probe_decision.json"),
            str(probe_root / "baseline_comparison_report.json"),
            str(probe_root / "candidate_generation_variants.json"),
            str(validation_root / "family_validation_report.json"),
            str(source_root / "pre_strength_event_ledger.jsonl"),
        ],
    }


def _risk_contract(probe_contract: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "tradex_selected_family_risk_decomposition_contract_v1",
        "axis_id": AXIS_ID,
        "diagnosis_questions": [
            "which_family_loses_big_winners",
            "lost_winners_excluded_or_under_ranked",
            "lost_winners_in_dropped_pre_reclaim_accumulation",
            "upside_family_risk_tradeoff",
            "safe_family_core_role",
            "human_selectable_day_loss_cause",
            "next_design_recommendation",
        ],
        "future_labels_used_for_candidate_generation": False,
        "future_labels_used_for_diagnosis": True,
        "source_probe_contract": probe_contract,
        "silent_fallback_used": False,
    }


def _artifact_complete(output_root: Path, decision: Mapping[str, Any]) -> dict[str, Any]:
    presence = {name: (output_root / name).exists() for name in REQUIRED_OUTPUTS if name != "_ARTIFACT_COMPLETE.json"}
    presence["_ARTIFACT_COMPLETE.json"] = True
    return {
        "schema_version": "tradex_selected_family_risk_decomposition_artifact_complete_v1",
        "axis_id": AXIS_ID,
        "decision": decision.get("decision"),
        "complete": all(presence.values()),
        "required_outputs": REQUIRED_OUTPUTS,
        "present_outputs": presence,
        "output_root": str(output_root),
        "silent_fallback_used": False,
    }


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


def _not_changed() -> list[str]:
    return [
        "MeeMee_runtime",
        "active_ranking",
        "display_score",
        "runtime_DuckDB",
        "production_registry",
        "teppan_watch_policy",
        "threshold_no_trade",
        "image_fusion",
        "sell_side",
        "exit_optimization",
        "cost_slippage_liquidity",
        "starter_entry_pretest",
    ]


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
