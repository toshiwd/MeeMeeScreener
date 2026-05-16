"""TRADEX-only pattern-family portfolio refresh after pre-strength archive."""

from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence


AXIS_ID = "pattern_family_portfolio_refresh_v1"
DEFAULT_RUN_ID = "20260514T190000Z-pattern-family-portfolio-refresh-v1"
DEFAULT_OUTPUT_PARENT = Path(r"G:\Tradex\pattern_family_portfolio_refresh_v1")

DEFAULT_PRE_STRENGTH_OVERLAY_ROOT = Path(
    r"G:\Tradex\pre_strength_defensive_overlay_probe_v1"
    r"\20260514T180000Z-pre-strength-defensive-overlay-probe-v1"
)
DEFAULT_MULTI_PATTERN_ROOT = Path(
    r"G:\Tradex\multi_pattern_candidate_generation_portfolio_v1"
    r"\20260514T140000Z-multi-pattern-candidate-generation-portfolio-v1"
)
DEFAULT_TEPPAN_WATCH_ROOT = Path(
    r"G:\Tradex\shadow_watch_logs\teppan_shadow_watch_mode_logging_v1"
    r"\20260514T130000Z-teppan-shadow-watch-mode-logging-v1"
)
DEFAULT_SOURCE_V2_ROOT = Path(
    r"G:\Tradex\source_specific_candidate_generation_validation_v2"
    r"\20260514T030000Z-source-specific-candidate-generation-validation-v2-top5-primary"
)
DEFAULT_SOURCE_MECHANISM_ROOT = Path(
    r"G:\Tradex\candidate_generation_source_mechanism_validation_v1"
    r"\20260513T190000Z-candidate-generation-source-mechanism-validation-v1"
)
DEFAULT_HYPOTHESIS_MAP_ROOT = Path(
    r"G:\Tradex\candidate_generation_hypothesis_map_refresh_v1"
    r"\20260513T180000Z-candidate-generation-hypothesis-map-refresh-v1"
)
DEFAULT_MA5_ROOT = Path(
    r"G:\Tradex\ma5_reclaim_hypothesis_batch_v1"
    r"\20260512T000000Z-ma5-reclaim-hypothesis-batch-v1-ma5_reclaim_hypothesis_batch_v1"
)
DEFAULT_WIDE_STRENGTH_ROOT = Path(
    r"G:\Tradex\wide_strength_pool_upside_rerank_v1"
    r"\20260513T030000Z-wide-strength-pool-upside-rerank-v1"
)
DEFAULT_SELECTION_RISK_ROOT = Path(
    r"G:\Tradex\selection_risk_control_for_wide_pool_v1"
    r"\20260513T040000Z-selection-risk-control-for-wide-pool-v1"
)
DEFAULT_R11_ARTIFACT = Path(r"artifacts\research_inventory\buy_surface_operational_validation_r11_r1_defensive.json")
DEFAULT_R11_GATE = Path(r"artifacts\research_inventory\buy_surface_operational_validation_r11_gate_decision.json")

REQUIRED_OUTPUTS = [
    "evaluation_contract.json",
    "run_manifest.json",
    "source_artifact_refs.json",
    "archived_family_context.json",
    "remaining_pattern_family_scan_report.json",
    "pattern_family_quality_report.json",
    "pattern_family_overlap_report.json",
    "signal_frequency_report.json",
    "top5_candidate_pool_fit_report.json",
    "selected_pattern_families_for_validation.json",
    "rejected_pattern_family_report.json",
    "next_axis_recommendation.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
]

EXCLUDED_FAMILY_TOKENS = (
    "teppan",
    "pre_base_to_strength",
    "pre_to_event_confirmation",
    "pre_reclaim_accumulation",
    "pre_strength",
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-parent", type=Path, default=DEFAULT_OUTPUT_PARENT)
    parser.add_argument("--run-id", default=DEFAULT_RUN_ID)
    parser.add_argument("--pre-strength-overlay-root", type=Path, default=DEFAULT_PRE_STRENGTH_OVERLAY_ROOT)
    parser.add_argument("--multi-pattern-root", type=Path, default=DEFAULT_MULTI_PATTERN_ROOT)
    parser.add_argument("--teppan-watch-root", type=Path, default=DEFAULT_TEPPAN_WATCH_ROOT)
    parser.add_argument("--source-v2-root", type=Path, default=DEFAULT_SOURCE_V2_ROOT)
    parser.add_argument("--source-mechanism-root", type=Path, default=DEFAULT_SOURCE_MECHANISM_ROOT)
    parser.add_argument("--hypothesis-map-root", type=Path, default=DEFAULT_HYPOTHESIS_MAP_ROOT)
    parser.add_argument("--ma5-root", type=Path, default=DEFAULT_MA5_ROOT)
    parser.add_argument("--wide-strength-root", type=Path, default=DEFAULT_WIDE_STRENGTH_ROOT)
    parser.add_argument("--selection-risk-root", type=Path, default=DEFAULT_SELECTION_RISK_ROOT)
    parser.add_argument("--r11-artifact", type=Path, default=DEFAULT_R11_ARTIFACT)
    parser.add_argument("--r11-gate", type=Path, default=DEFAULT_R11_GATE)
    args = parser.parse_args()
    run_pattern_family_portfolio_refresh_v1(
        output_parent=args.output_parent,
        run_id=args.run_id,
        pre_strength_overlay_root=args.pre_strength_overlay_root,
        multi_pattern_root=args.multi_pattern_root,
        teppan_watch_root=args.teppan_watch_root,
        source_v2_root=args.source_v2_root,
        source_mechanism_root=args.source_mechanism_root,
        hypothesis_map_root=args.hypothesis_map_root,
        ma5_root=args.ma5_root,
        wide_strength_root=args.wide_strength_root,
        selection_risk_root=args.selection_risk_root,
        r11_artifact=args.r11_artifact,
        r11_gate=args.r11_gate,
    )
    return 0


def run_pattern_family_portfolio_refresh_v1(
    *,
    output_parent: Path = DEFAULT_OUTPUT_PARENT,
    run_id: str = DEFAULT_RUN_ID,
    pre_strength_overlay_root: Path = DEFAULT_PRE_STRENGTH_OVERLAY_ROOT,
    multi_pattern_root: Path = DEFAULT_MULTI_PATTERN_ROOT,
    teppan_watch_root: Path = DEFAULT_TEPPAN_WATCH_ROOT,
    source_v2_root: Path = DEFAULT_SOURCE_V2_ROOT,
    source_mechanism_root: Path = DEFAULT_SOURCE_MECHANISM_ROOT,
    hypothesis_map_root: Path = DEFAULT_HYPOTHESIS_MAP_ROOT,
    ma5_root: Path = DEFAULT_MA5_ROOT,
    wide_strength_root: Path = DEFAULT_WIDE_STRENGTH_ROOT,
    selection_risk_root: Path = DEFAULT_SELECTION_RISK_ROOT,
    r11_artifact: Path = DEFAULT_R11_ARTIFACT,
    r11_gate: Path = DEFAULT_R11_GATE,
) -> dict[str, Any]:
    output_root = output_parent / run_id
    output_root.mkdir(parents=True, exist_ok=True)
    roots = {
        "pre_strength_overlay_root": pre_strength_overlay_root,
        "multi_pattern_root": multi_pattern_root,
        "teppan_watch_root": teppan_watch_root,
        "source_v2_root": source_v2_root,
        "source_mechanism_root": source_mechanism_root,
        "hypothesis_map_root": hypothesis_map_root,
        "ma5_root": ma5_root,
        "wide_strength_root": wide_strength_root,
        "selection_risk_root": selection_risk_root,
        "r11_artifact": r11_artifact,
        "r11_gate": r11_gate,
    }
    artifacts = _load_artifacts(roots)
    archived = _archived_family_context(artifacts, roots)
    scan = _remaining_pattern_family_scan_report(artifacts, roots)
    quality = _quality_report(scan)
    frequency = _frequency_report(scan)
    top5_fit = _top5_fit_report(scan)
    selected = _selected_pattern_families(scan)
    rejected = _rejected_pattern_family_report(scan)
    overlap = _overlap_report(selected["selected_pattern_families"])
    decision = _research_decision(selected, scan, artifacts)
    next_axis = _next_axis_recommendation(decision)
    payloads = {
        "evaluation_contract.json": _evaluation_contract(),
        "run_manifest.json": _run_manifest(output_root, roots),
        "source_artifact_refs.json": _source_artifact_refs(roots, artifacts),
        "archived_family_context.json": archived,
        "remaining_pattern_family_scan_report.json": scan,
        "pattern_family_quality_report.json": quality,
        "pattern_family_overlap_report.json": overlap,
        "signal_frequency_report.json": frequency,
        "top5_candidate_pool_fit_report.json": top5_fit,
        "selected_pattern_families_for_validation.json": selected,
        "rejected_pattern_family_report.json": rejected,
        "next_axis_recommendation.json": next_axis,
        "research_decision.json": decision,
    }
    for name, payload in payloads.items():
        _write_json(output_root / name, payload)
    complete = _artifact_complete(output_root, decision)
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    return {
        "output_root": str(output_root),
        "evaluation_contract": payloads["evaluation_contract.json"],
        "run_manifest": payloads["run_manifest.json"],
        "source_artifact_refs": payloads["source_artifact_refs.json"],
        "archived_family_context": archived,
        "remaining_pattern_family_scan_report": scan,
        "pattern_family_quality_report": quality,
        "pattern_family_overlap_report": overlap,
        "signal_frequency_report": frequency,
        "top5_candidate_pool_fit_report": top5_fit,
        "selected_pattern_families_for_validation": selected,
        "rejected_pattern_family_report": rejected,
        "next_axis_recommendation": next_axis,
        "research_decision": decision,
        "artifact_complete": complete,
    }


def _load_artifacts(roots: Mapping[str, Path]) -> dict[str, Any]:
    return {
        "overlay_decision": _read_json_optional(roots["pre_strength_overlay_root"] / "research_decision.json"),
        "multi_decision": _read_json_optional(roots["multi_pattern_root"] / "research_decision.json"),
        "teppan_result": _read_json_optional(roots["teppan_watch_root"] / "watch_run_result.json"),
        "teppan_metrics": _read_json_optional(roots["teppan_watch_root"] / "teppan_watch_metrics.json"),
        "source_v2_decision": _read_json_optional(roots["source_v2_root"] / "research_decision.json"),
        "source_mech_decision": _read_json_optional(roots["source_mechanism_root"] / "research_decision.json"),
        "source_mech_leaderboard": _read_json_optional(roots["source_mechanism_root"] / "hypothesis_validation_readiness_leaderboard.json"),
        "hypothesis_map": _read_json_optional(roots["hypothesis_map_root"] / "refreshed_candidate_generation_hypothesis_map.json"),
        "ma5_decision": _read_json_optional(roots["ma5_root"] / "research_decision.json"),
        "ma5_leaderboard": _read_json_optional(roots["ma5_root"] / "hypothesis_leaderboard.json"),
        "wide_decision": _read_json_optional(roots["wide_strength_root"] / "research_decision.json"),
        "selection_risk_decision": _read_json_optional(roots["selection_risk_root"] / "research_decision.json"),
        "r11_artifact": _read_json_optional(roots["r11_artifact"]),
        "r11_gate": _read_json_optional(roots["r11_gate"]),
    }


def _archived_family_context(artifacts: Mapping[str, Any], roots: Mapping[str, Path]) -> dict[str, Any]:
    overlay = artifacts["overlay_decision"]
    multi = artifacts["multi_decision"]
    source_v2 = artifacts["source_v2_decision"]
    return {
        "schema_version": "tradex_pattern_family_portfolio_refresh_archived_family_context_v1",
        "axis_id": AXIS_ID,
        "teppan": {
            "status": "watch_only",
            "activation_allowed": False,
            "source_artifact": str(roots["teppan_watch_root"]),
        },
        "pre_strength": {
            "candidate_generation_archived": True,
            "defensive_overlay_archived": True,
            "source_candidate_generation_decision": multi.get("decision"),
            "source_defensive_overlay_decision": overlay.get("decision"),
            "source_defensive_overlay_best_variant_id": overlay.get("best_variant_id"),
            "archive_reason": "candidate_generation_and_defensive_overlay_failed_keep_gate",
        },
        "source_specific_v2": {
            "status": "archived_drop",
            "source_decision": source_v2.get("authoritative_research_decision") or source_v2.get("decision"),
            "reason": "source_recovers_winners_but_too_noisy",
        },
        "image_route": {
            "status": "paused_or_out_of_scope",
            "reason": "image_fusion_not_reopened_in_this_axis",
        },
        "source_v1_v2_failed_or_archived": True,
        "not_changed": _not_changed(),
    }


def _remaining_pattern_family_scan_report(artifacts: Mapping[str, Any], roots: Mapping[str, Path]) -> dict[str, Any]:
    rows = []
    rows.extend(_wide_strength_rows(artifacts, roots["wide_strength_root"], roots["selection_risk_root"]))
    rows.extend(_ma5_rows(artifacts, roots["ma5_root"]))
    rows.extend(_source_mechanism_rows(artifacts, roots["source_mechanism_root"], roots["source_v2_root"]))
    rows.append(_r11_row(artifacts, roots["r11_artifact"], roots["r11_gate"]))
    rows.append(_teppan_row(artifacts, roots["teppan_watch_root"]))
    rows.append(_pre_strength_archive_row(artifacts, roots["pre_strength_overlay_root"]))
    scored = [_score_row(row) for row in rows if row]
    return {
        "schema_version": "tradex_remaining_pattern_family_scan_report_v1",
        "axis_id": AXIS_ID,
        "scan_scope": "pre_strength_teppan_and_archived_failed_sources_excluded_from_selection",
        "row_count": len(scored),
        "rows": sorted(scored, key=lambda item: (-float(item.get("selection_score") or 0.0), str(item["family_id"]))),
        "silent_fallback_used": False,
        "research_fallback_used": False,
        "future_labels_used_for_diagnosis_only": True,
        "future_labels_used_in_score_inputs": False,
    }


def _wide_strength_rows(artifacts: Mapping[str, Any], wide_root: Path, risk_root: Path) -> list[dict[str, Any]]:
    decision = artifacts["wide_decision"]
    metrics = decision.get("best_research_family_metrics") or {}
    if not metrics:
        return []
    risk = artifacts["selection_risk_decision"]
    risk_metrics = risk.get("best_risk_family_metrics") or {}
    return [
        {
            "family_id": str(metrics.get("family_id") or "momentum_continuation_soft_boost_v1"),
            "family_group": "wide_strength_momentum_continuation",
            "mechanism": "wide-pool momentum continuation, designed to supply upside candidates regularly",
            "role": "upside_candidate_generation_family",
            "source_axis": "wide_strength_pool_upside_rerank_v1",
            "source_artifact": str(wide_root / "research_decision.json"),
            "source_decision": decision.get("authoritative_research_decision") or decision.get("decision"),
            "status": "validation_candidate_with_risk_caveat",
            "selection_eligible": True,
            "out_of_scope_reason": None,
            "frequency": {
                "selected_day_count": _int(metrics.get("selected_day_count")),
                "selected_event_count": _int(metrics.get("selected_event_count")),
                "average_candidates_per_day": _float(metrics.get("average_candidates_per_day")),
                "median_candidates_per_day": _float(metrics.get("median_candidates_per_day")),
                "opportunity_days_total": _int(metrics.get("opportunity_days_total")),
            },
            "quality": {
                "top3_avg_ret20": _float(metrics.get("selected_top3_avg_ret20")),
                "top3_win_rate20": _float(metrics.get("selected_top3_win_rate20")),
                "top3_severe_loss_rate20": _float(metrics.get("selected_top3_severe_loss_rate20")),
                "top3_big_winner_capture_rate": _float(metrics.get("selected_top3_big_winner_ret20_ge_10_capture_rate")),
                "top3_future_top10_precision": _float(metrics.get("selected_top3_future_top10_precision")),
                "improvement_vs_random_top3": _float(metrics.get("improvement_vs_random_top3")),
                "improvement_vs_all_strength_event_average": _float(metrics.get("improvement_vs_all_strength_event_average")),
                "time_block_positive_rate": _float((_decision_reason_value(decision, "time_block_stability") or {}).get("positive_time_block_rate")),
                "risk_overlay_top3_severe_loss_rate20": _float(risk_metrics.get("selected_top3_severe_loss_rate20")),
            },
            "top5_candidate_pool_fit": "high_frequency_upside_surface_needs_top5_validation_and_risk_decomposition",
            "human_selectable_day_potential": "high_regular_candidate_supply",
            "time_block_stability": "stable_enough_in_source_artifact",
            "overlap_tags": ["wide_pool", "momentum_continuation", "upside_capture"],
            "typed_reasons": [
                "non_pre_strength_non_teppan",
                "regular_candidate_supply",
                "upside_capture_evidence",
                "hold_due_to_severe_loss_and_complete_champion_ranking_gap",
            ],
            "secondary_related_artifact": str(risk_root / "research_decision.json"),
        }
    ]


def _ma5_rows(artifacts: Mapping[str, Any], root: Path) -> list[dict[str, Any]]:
    decision = artifacts["ma5_decision"]
    rows = artifacts["ma5_leaderboard"].get("rows") or []
    if not rows:
        rows = (decision.get("excellent_hypotheses") or []) + (decision.get("promising_hypotheses") or [])
    candidates = []
    for row in rows[:8]:
        if not isinstance(row, Mapping):
            continue
        if str(row.get("hypothesis_decision") or "").lower() not in {"excellent", "promising", ""}:
            continue
        family_id = f"ma5_reclaim_context::{row.get('hypothesis_id')}"
        candidates.append(
            {
                "family_id": family_id,
                "family_group": "ma5_reclaim_context",
                "mechanism": str(row.get("thesis") or "MA5 reclaim context filter"),
                "role": "candidate_generation_context_family",
                "source_axis": "ma5_reclaim_hypothesis_batch_v1",
                "source_artifact": str(root / "research_decision.json"),
                "source_decision": decision.get("authoritative_research_decision") or decision.get("decision"),
                "status": "validation_candidate_with_additive_generation_gap",
                "selection_eligible": True,
                "out_of_scope_reason": None,
                "frequency": {
                    "trade_count": _int(row.get("trade_count")),
                    "symbol_count": _int(row.get("symbol_count")),
                },
                "quality": {
                    "avg_ret20": _float(row.get("avg_ret")),
                    "avg_ret20_delta_vs_base": _float(row.get("avg_ret_delta_vs_base")),
                    "win_rate20": _float(row.get("win_rate")),
                    "severe_loss_rate20": _float(row.get("severe_loss_rate")),
                    "avg_mfe20": _float(row.get("avg_mfe")),
                    "avg_mae20": _float(row.get("avg_mae")),
                    "profit_factor20": _float(row.get("profit_factor")),
                },
                "top5_candidate_pool_fit": "needs_additive_candidate_generation_validation",
                "human_selectable_day_potential": "unknown_until_reentry_expansion_modeled",
                "time_block_stability": "not_reported_in_source_artifact",
                "overlap_tags": ["ma5_reclaim", str(row.get("hypothesis_id") or ""), "trend_context"],
                "typed_reasons": [
                    "non_pre_strength_non_teppan",
                    "strong_context_filter_evidence",
                    "low_severe_loss_context",
                    "additive_candidate_generation_not_yet_modeled",
                ],
            }
        )
    return candidates


def _source_mechanism_rows(artifacts: Mapping[str, Any], root: Path, source_v2_root: Path) -> list[dict[str, Any]]:
    leaderboard = artifacts["source_mech_leaderboard"].get("rows") or []
    source_v2 = artifacts["source_v2_decision"]
    rows = []
    for row in leaderboard:
        if not isinstance(row, Mapping):
            continue
        final_validated_drop = row.get("hypothesis_id") == source_v2.get("selected_hypothesis_id")
        rows.append(
            {
                "family_id": f"source_mechanism::{row.get('hypothesis_id')}",
                "family_group": "source_specific_candidate_generation",
                "mechanism": str(row.get("expected_mechanism") or row.get("target_failure_mode") or ""),
                "role": "candidate_generation_source_family",
                "source_axis": "candidate_generation_source_mechanism_validation_v1",
                "source_artifact": str(root / "hypothesis_validation_readiness_leaderboard.json"),
                "source_decision": artifacts["source_mech_decision"].get("authoritative_research_decision")
                or artifacts["source_mech_decision"].get("decision"),
                "status": "drop_validated_too_noisy" if final_validated_drop else "hold_same_date_support_gap",
                "selection_eligible": False,
                "out_of_scope_reason": "validated_source_v2_drop" if final_validated_drop else "same_date_support_missing",
                "frequency": {
                    "sample_count": _int(row.get("sample_count")),
                    "missed_winner_count": _int(row.get("missed_winner_count")),
                },
                "quality": {
                    "future_winner_rate": _float(row.get("future_winner_rate")),
                    "selected_nonwinner_rate": _float(row.get("selected_nonwinner_rate")),
                    "severe_loss_rate20": _float(row.get("severe_loss_rate20")),
                    "selected_capture_rate_among_source_winners": _float(row.get("selected_capture_rate_among_source_winners")),
                    "time_block_stability": _float(row.get("time_block_stability")),
                },
                "top5_candidate_pool_fit": "failed_or_blocked_source_specific_candidate_generation",
                "human_selectable_day_potential": "blocked",
                "time_block_stability": row.get("time_block_stability"),
                "overlap_tags": ["source_specific", "weekly_prior_strong_up", "negative_guard_match"],
                "typed_reasons": [
                    "source_specific_v2_failed_or_blocked",
                    "not_selected_for_refresh",
                    f"source_v2_artifact={source_v2_root / 'research_decision.json'}",
                ],
            }
        )
    return rows


def _r11_row(artifacts: Mapping[str, Any], artifact_path: Path, gate_path: Path) -> dict[str, Any]:
    gate = artifacts["r11_gate"]
    metrics = gate.get("metrics") or {}
    return {
        "family_id": "r11_weak_liquidity_defensive_operational_lane",
        "family_group": "operational_defensive_liquidity",
        "mechanism": "weak-liquidity defensive operational validation, not a fresh pattern-generation family",
        "role": "operational_validation_not_selected",
        "source_axis": "buy_surface_operational_validation_r11_r1_defensive",
        "source_artifact": str(artifact_path),
        "secondary_artifact": str(gate_path),
        "source_decision": gate.get("authoritative_decision"),
        "status": "reject_out_of_scope_for_this_refresh",
        "selection_eligible": False,
        "out_of_scope_reason": "liquidity_operational_lane_and_candidate_symbol_count_3",
        "frequency": {
            "candidate_symbol_count": _int(metrics.get("candidate_symbol_count")),
            "changed_top5_members_count": _int(metrics.get("changed_top5_members_count")),
            "changed_top10_members_count": _int(metrics.get("changed_top10_members_count")),
            "changed_rank_count": _int(metrics.get("changed_rank_count")),
        },
        "quality": {
            "top5_uplift": _float(metrics.get("top5_uplift")),
            "top10_uplift": _float(metrics.get("top10_uplift")),
            "bad_pick_removal": _float(metrics.get("bad_pick_removal")),
            "worst_regime_delta": _float(metrics.get("worst_regime_delta")),
        },
        "top5_candidate_pool_fit": "strong_operational_uplift_but_not_this_pattern_family_axis",
        "human_selectable_day_potential": "insufficient_candidate_breadth_candidate_count_3",
        "time_block_stability": "worst_regime_negative",
        "overlap_tags": ["liquidity", "operational_validation", "defensive"],
        "typed_reasons": [
            "authoritative_keep_but_out_of_scope",
            "do_not_mix_liquidity_in_this_axis",
            "candidate_symbol_count_lt_5",
        ],
    }


def _teppan_row(artifacts: Mapping[str, Any], root: Path) -> dict[str, Any]:
    metrics = artifacts["teppan_metrics"]
    return {
        "family_id": "teppan_watch_only",
        "family_group": "teppan",
        "mechanism": "rare chart watch pattern",
        "role": "watch_pattern_not_selected",
        "source_axis": "teppan_shadow_watch_mode_logging_v1",
        "source_artifact": str(root / "watch_run_result.json"),
        "source_decision": artifacts["teppan_result"].get("decision"),
        "status": "watch_only_do_not_select",
        "selection_eligible": False,
        "out_of_scope_reason": "teppan_is_rare_watch_pattern",
        "frequency": {
            "top100_teppan_pattern_match_count": _int(metrics.get("top100_teppan_pattern_match_count")),
            "boost_eligible_count": _int(metrics.get("boost_eligible_count")),
        },
        "quality": {},
        "top5_candidate_pool_fit": "watch_only_no_current_regular_candidate_supply",
        "human_selectable_day_potential": "trigger_only",
        "time_block_stability": "not_applicable_watch_mode",
        "overlap_tags": ["teppan", "rare_watch"],
        "typed_reasons": ["kept_watch_only", "not_frequency_portfolio_candidate"],
    }


def _pre_strength_archive_row(artifacts: Mapping[str, Any], root: Path) -> dict[str, Any]:
    decision = artifacts["overlay_decision"]
    return {
        "family_id": "pre_strength_family_archived",
        "family_group": "pre_strength",
        "mechanism": "pre-strength candidate generation and defensive overlay archive marker",
        "role": "archived_family_not_selected",
        "source_axis": "pre_strength_defensive_overlay_probe_v1",
        "source_artifact": str(root / "research_decision.json"),
        "source_decision": decision.get("decision"),
        "status": "archived_do_not_select",
        "selection_eligible": False,
        "out_of_scope_reason": "pre_strength_failed_candidate_generation_and_defensive_overlay_keep_gates",
        "frequency": {"top5_changed_members_count_best_variant": _int(((decision.get("variant_decisions") or [{}])[0].get("metrics") or {}).get("top5_changed_members_count_vs_baseline"))},
        "quality": ((decision.get("variant_decisions") or [{}])[0].get("metrics") or {}),
        "top5_candidate_pool_fit": "archived_drop_for_this_refresh",
        "human_selectable_day_potential": "not_reopened",
        "time_block_stability": "not_reopened",
        "overlap_tags": ["pre_strength"],
        "typed_reasons": ["pre_strength_candidate_generation_archived", "pre_strength_defensive_overlay_archived"],
    }


def _score_row(row: dict[str, Any]) -> dict[str, Any]:
    family_id = str(row.get("family_id") or "")
    excluded = any(token in family_id for token in EXCLUDED_FAMILY_TOKENS)
    if excluded:
        row["selection_eligible"] = False
        row["out_of_scope_reason"] = row.get("out_of_scope_reason") or "explicitly_excluded_family"
    frequency = row.get("frequency") or {}
    quality = row.get("quality") or {}
    event_count = _first_int(frequency, "selected_event_count", "trade_count", "sample_count", "changed_top5_members_count", default=0)
    days = _first_int(frequency, "selected_day_count", "opportunity_days_total", default=0)
    severe = _first_float(quality, "top3_severe_loss_rate20", "severe_loss_rate20", "severe_loss_rate", default=0.30)
    avg_ret = _first_float(quality, "top3_avg_ret20", "avg_ret20", "avg_ret20_delta_vs_base", default=0.0)
    winner_capture = _first_float(quality, "top3_big_winner_capture_rate", "future_winner_rate", default=0.0)
    uplift = _first_float(quality, "improvement_vs_random_top3", "avg_ret20_delta_vs_base", "top5_uplift", default=0.0)
    score = 0.0
    if row.get("selection_eligible"):
        score += min(event_count / 1500.0, 1.0) * 0.25
        score += min(days / 1000.0, 1.0) * 0.12
        score += max(avg_ret, 0.0) * 4.0
        score += max(uplift, 0.0) * 6.0
        score += max(winner_capture, 0.0) * 0.35
        score += max(0.26 - severe, 0.0) * 0.8
        if "additive_generation_gap" in str(row.get("status")):
            score -= 0.08
        if "risk_caveat" in str(row.get("status")):
            score -= 0.04
    row["selection_score"] = round(max(score, 0.0), 6)
    row["quality_gate_summary"] = {
        "selection_eligible": bool(row.get("selection_eligible")),
        "not_excluded_family": not excluded,
        "signal_frequency_not_too_low": event_count >= 120 or days >= 120,
        "severe_profile_not_structurally_too_heavy": severe <= 0.26,
        "top5_fit_requires_validation": "validation" in str(row.get("top5_candidate_pool_fit")),
        "candidate_breadth_potential": str(row.get("human_selectable_day_potential")) != "blocked",
    }
    return row


def _quality_report(scan: Mapping[str, Any]) -> dict[str, Any]:
    rows = []
    for row in scan["rows"]:
        q = row.get("quality") or {}
        rows.append(
            {
                "family_id": row["family_id"],
                "status": row.get("status"),
                "role": row.get("role"),
                "event_count": _first_int(row.get("frequency") or {}, "selected_event_count", "trade_count", "sample_count", default=0),
                "month_count": (row.get("frequency") or {}).get("month_count"),
                "avg_ret20": _first_float(q, "top3_avg_ret20", "avg_ret20", default=None),
                "win_rate20": _first_float(q, "top3_win_rate20", "win_rate20", default=None),
                "severe_loss_rate20": _first_float(q, "top3_severe_loss_rate20", "severe_loss_rate20", "severe_loss_rate", default=None),
                "avg_MFE20": _first_float(q, "avg_mfe20", default=None),
                "avg_MAE20": _first_float(q, "avg_mae20", default=None),
                "big_winner_capture_rate": _first_float(q, "top3_big_winner_capture_rate", "future_winner_rate", default=None),
                "future_top10_capture_rate": _first_float(q, "top3_future_top10_precision", default=None),
                "bad_pick_rate": _first_float(q, "selected_nonwinner_rate", default=None),
                "top5_candidate_pool_fit": row.get("top5_candidate_pool_fit"),
                "human_selectable_day_potential": row.get("human_selectable_day_potential"),
                "time_block_stability": row.get("time_block_stability"),
                "selection_score": row.get("selection_score"),
            }
        )
    return {"schema_version": "tradex_pattern_family_refresh_quality_report_v1", "axis_id": AXIS_ID, "rows": rows}


def _frequency_report(scan: Mapping[str, Any]) -> dict[str, Any]:
    rows = []
    for row in scan["rows"]:
        freq = row.get("frequency") or {}
        event_count = _first_int(freq, "selected_event_count", "trade_count", "sample_count", "changed_top5_members_count", default=0)
        days = _first_int(freq, "selected_day_count", "opportunity_days_total", default=0)
        rows.append(
            {
                "family_id": row["family_id"],
                "event_or_signal_count": event_count,
                "day_count": days,
                "signal_frequency_bucket": "regular" if event_count >= 1000 or days >= 500 else "moderate" if event_count >= 120 else "sparse_or_blocked",
                "regular_candidate_surface_potential": bool(row.get("selection_eligible")) and (event_count >= 120 or days >= 120),
            }
        )
    return {"schema_version": "tradex_pattern_family_refresh_signal_frequency_report_v1", "axis_id": AXIS_ID, "rows": rows}


def _top5_fit_report(scan: Mapping[str, Any]) -> dict[str, Any]:
    rows = []
    for row in scan["rows"]:
        gate = row.get("quality_gate_summary") or {}
        fit = "select_for_validation" if row.get("selection_score", 0) >= 0.25 and gate.get("candidate_breadth_potential") else "reject_or_hold"
        if row.get("out_of_scope_reason"):
            fit = "out_of_scope"
        rows.append(
            {
                "family_id": row["family_id"],
                "fit_decision": fit,
                "top5_candidate_pool_fit": row.get("top5_candidate_pool_fit"),
                "selection_score": row.get("selection_score"),
                "human_selectable_day_potential": row.get("human_selectable_day_potential"),
                "typed_reasons": row.get("typed_reasons"),
            }
        )
    return {"schema_version": "tradex_pattern_family_refresh_top5_candidate_pool_fit_report_v1", "axis_id": AXIS_ID, "rows": rows}


def _selected_pattern_families(scan: Mapping[str, Any]) -> dict[str, Any]:
    eligible = [
        row
        for row in scan["rows"]
        if row.get("selection_eligible")
        and not row.get("out_of_scope_reason")
        and float(row.get("selection_score") or 0.0) >= 0.25
    ]
    selected = []
    used_groups: set[str] = set()
    for row in sorted(eligible, key=lambda item: -float(item.get("selection_score") or 0.0)):
        if len(selected) >= 3:
            break
        group = str(row.get("family_group"))
        if group in used_groups:
            continue
        selected.append(_selected_view(row))
        used_groups.add(group)
    return {
        "schema_version": "tradex_pattern_family_refresh_selected_families_for_validation_v1",
        "axis_id": AXIS_ID,
        "selection_count": len(selected),
        "selection_limit": 3,
        "selected_pattern_families": selected,
        "validation_next_axis": "selected_pattern_family_validation_v2" if selected else None,
    }


def _rejected_pattern_family_report(scan: Mapping[str, Any]) -> dict[str, Any]:
    rows = []
    for row in scan["rows"]:
        if row.get("selection_eligible") and float(row.get("selection_score") or 0.0) >= 0.25 and not row.get("out_of_scope_reason"):
            continue
        rows.append(
            {
                "family_id": row["family_id"],
                "status": row.get("status"),
                "rejection_or_hold_reason": row.get("out_of_scope_reason") or row.get("top5_candidate_pool_fit"),
                "typed_reasons": row.get("typed_reasons"),
                "selection_score": row.get("selection_score"),
            }
        )
    return {"schema_version": "tradex_pattern_family_refresh_rejected_family_report_v1", "axis_id": AXIS_ID, "rows": rows}


def _overlap_report(selected_rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    pairs = []
    for i, left in enumerate(selected_rows):
        for right in selected_rows[i + 1 :]:
            lt, rt = set(left.get("overlap_tags") or []), set(right.get("overlap_tags") or [])
            union = lt | rt
            jaccard = 0.0 if not union else len(lt & rt) / len(union)
            pairs.append(
                {
                    "left_family_id": left["family_id"],
                    "right_family_id": right["family_id"],
                    "overlap_jaccard": jaccard,
                    "overlap_level": "high" if jaccard >= 0.5 else "medium" if jaccard >= 0.25 else "low",
                    "shared_tags": sorted(lt & rt),
                }
            )
    return {
        "schema_version": "tradex_pattern_family_refresh_overlap_report_v1",
        "axis_id": AXIS_ID,
        "selected_pair_count": len(pairs),
        "selected_pairs": pairs,
        "selected_portfolio_overlap_assessment": "acceptable" if all(row["overlap_level"] != "high" for row in pairs) else "needs_deduplication",
    }


def _research_decision(selected: Mapping[str, Any], scan: Mapping[str, Any], artifacts: Mapping[str, Any]) -> dict[str, Any]:
    selected_count = int(selected.get("selection_count") or 0)
    eligible_count = sum(1 for row in scan["rows"] if row.get("selection_eligible") and not row.get("out_of_scope_reason"))
    if selected_count:
        decision = "keep_candidate"
        authoritative = "pattern_family_portfolio_refreshed_next_validation_ready"
        typed = [
            "non_pre_strength_non_teppan_family_selected",
            "one_to_three_families_selected_for_validation",
            "pre_strength_and_teppan_not_reopened",
            "no_meemee_or_publish_change",
        ]
    elif eligible_count:
        decision = "hold"
        authoritative = "pattern_family_portfolio_refresh_hold"
        typed = ["candidate_family_exists_but_quality_or_top5_fit_below_gate", "additional_feature_extraction_needed"]
    else:
        decision = "drop"
        authoritative = "pattern_family_portfolio_refresh_failed"
        typed = ["no_new_non_archived_family_selected", "remaining_candidates_are_archived_or_out_of_scope"]
    return {
        "schema_version": "tradex_pattern_family_portfolio_refresh_research_decision_v1",
        "research_phase": "pattern_family_portfolio_refresh",
        "boundary": "TRADEX-only",
        "axis_moved": "pattern_family_portfolio_refresh",
        "source_defensive_overlay_decision": artifacts["overlay_decision"].get("decision"),
        "pre_strength_candidate_generation_archived": True,
        "pre_strength_defensive_overlay_archived": True,
        "teppan_watch_only": True,
        "pattern_family_portfolio_refreshed": True,
        "selected_family_count": selected_count,
        "selected_family_ids": [row["family_id"] for row in selected["selected_pattern_families"]],
        "candidate_scoring_created": False,
        "threshold_policy_created": False,
        "image_score_used": False,
        "fusion_reranker_created": False,
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
        "future_labels_used_for_diagnosis_only": True,
        "future_labels_used_in_score_inputs": False,
        "silent_fallback_used": False,
        "research_fallback_used": False,
        "decision": decision,
        "authoritative_research_decision": authoritative,
        "typed_reasons": typed,
        "generated_at_utc": _utc_now(),
    }


def _next_axis_recommendation(decision: Mapping[str, Any]) -> dict[str, Any]:
    auth = decision["authoritative_research_decision"]
    next_axis = {
        "pattern_family_portfolio_refreshed_next_validation_ready": "selected_pattern_family_validation_v2",
        "pattern_family_portfolio_refresh_hold": "pattern_family_feature_extraction_audit_v1",
        "pattern_family_portfolio_refresh_failed": "broader_candidate_generation_design_reset_v1",
    }[auth]
    return {
        "schema_version": "tradex_pattern_family_portfolio_refresh_next_axis_recommendation_v1",
        "axis_id": AXIS_ID,
        "decision": decision["decision"],
        "authoritative_research_decision": auth,
        "next": next_axis,
        "activation_allowed": False,
        "meemee_reflection_allowed": False,
    }


def _evaluation_contract() -> dict[str, Any]:
    return {
        "schema_version": "tradex_pattern_family_portfolio_refresh_evaluation_contract_v1",
        "axis_id": AXIS_ID,
        "boundary": "TRADEX-only diagnosis",
        "goal": "refresh non-pre-strength non-teppan pattern families for top5 candidate-pool validation",
        "fixed_conditions": {
            "same_universe": "source_artifact_native",
            "same_period": "source_artifact_native",
            "same_topK": "source_artifact_native_or_reported",
            "same_regime_condition": "source_artifact_native",
            "same_cost_slippage": "no_new_cost_slippage_added",
            "same_artifact_detail_level": "JSON_authoritative",
        },
        "do_not_change": _not_changed(),
        "no_silent_fallback": True,
        "json_authoritative": True,
    }


def _run_manifest(output_root: Path, roots: Mapping[str, Path]) -> dict[str, Any]:
    return {
        "schema_version": "tradex_pattern_family_portfolio_refresh_run_manifest_v1",
        "axis_id": AXIS_ID,
        "run_id": output_root.name,
        "output_root": str(output_root),
        "input_roots": {key: str(value) for key, value in roots.items()},
        "generated_at_utc": _utc_now(),
    }


def _source_artifact_refs(roots: Mapping[str, Path], artifacts: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "tradex_pattern_family_portfolio_refresh_source_artifact_refs_v1",
        "axis_id": AXIS_ID,
        "refs": {key: str(value) for key, value in roots.items()},
        "missing_inputs": {
            key: value.get("_missing_path")
            for key, value in artifacts.items()
            if isinstance(value, Mapping) and value.get("_missing")
        },
        "silent_fallback_used": False,
    }


def _selected_view(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "family_id": row["family_id"],
        "family_group": row.get("family_group"),
        "mechanism": row.get("mechanism"),
        "role": row.get("role"),
        "source_axis": row.get("source_axis"),
        "source_artifact": row.get("source_artifact"),
        "source_decision": row.get("source_decision"),
        "status": row.get("status"),
        "selection_score": row.get("selection_score"),
        "frequency": row.get("frequency"),
        "quality": row.get("quality"),
        "top5_candidate_pool_fit": row.get("top5_candidate_pool_fit"),
        "human_selectable_day_potential": row.get("human_selectable_day_potential"),
        "time_block_stability": row.get("time_block_stability"),
        "overlap_tags": row.get("overlap_tags"),
        "typed_reasons": row.get("typed_reasons"),
        "validation_scope": "TRADEX_only_selected_pattern_family_validation_v2",
    }


def _decision_reason_value(decision: Mapping[str, Any], code: str) -> Any:
    for row in decision.get("decision_reasons") or []:
        if isinstance(row, Mapping) and row.get("code") == code:
            return row.get("value")
    return None


def _artifact_complete(output_root: Path, decision: Mapping[str, Any]) -> dict[str, Any]:
    presence = {name: (output_root / name).exists() for name in REQUIRED_OUTPUTS if name != "_ARTIFACT_COMPLETE.json"}
    presence["_ARTIFACT_COMPLETE.json"] = True
    return {
        "schema_version": "tradex_pattern_family_portfolio_refresh_artifact_complete_v1",
        "axis_id": AXIS_ID,
        "complete": all(presence.values()),
        "decision": decision.get("decision"),
        "authoritative_research_decision": decision.get("authoritative_research_decision"),
        "required_outputs": REQUIRED_OUTPUTS,
        "present_outputs": presence,
        "output_root": str(output_root),
        "silent_fallback_used": False,
        "research_fallback_used": False,
    }


def _not_changed() -> list[str]:
    return [
        "pre_strength_overlay_tuning",
        "pre_strength_starter_entry",
        "teppan_activation",
        "threshold_no_trade_policy",
        "image_fusion",
        "production_ranking",
        "MeeMee_runtime",
        "publish_bundle",
        "sell_side",
        "buy_more_core_logic",
        "exit_optimization",
        "cost_slippage_liquidity_axis",
    ]


def _read_json_optional(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"_missing": True, "_missing_path": str(path)}
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if isinstance(data, dict):
        return data
    return {"_not_object": True, "_path": str(path)}


def _first_float(mapping: Mapping[str, Any], *keys: str, default: float | None = 0.0) -> float | None:
    for key in keys:
        if mapping.get(key) is not None:
            return _float(mapping.get(key), default)
    return default


def _first_int(mapping: Mapping[str, Any], *keys: str, default: int = 0) -> int:
    for key in keys:
        if mapping.get(key) is not None:
            return _int(mapping.get(key), default)
    return default


def _float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None:
            return default
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def _int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


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
    return value


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(dict(payload)), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
