"""TRADEX-only multi-pattern candidate-generation portfolio design."""

from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd


AXIS_ID = "multi_pattern_candidate_generation_portfolio_v1"
DEFAULT_RUN_ID = "20260514T140000Z-multi-pattern-candidate-generation-portfolio-v1"
DEFAULT_OUTPUT_PARENT = Path(r"G:\Tradex\multi_pattern_candidate_generation_portfolio_v1")

DEFAULT_PRE_STRENGTH_ROOT = Path(r"G:\Tradex\pre_strength_pattern_mining_v1\20260513T000000Z-pre-strength-pattern-mining-v1")
DEFAULT_GUARD_ROOT = Path(r"G:\Tradex\pre_strength_guard_validation_v1\20260513T010000Z-pre-strength-guard-validation-v1")
DEFAULT_SOURCE_V2_ROOT = Path(
    r"G:\Tradex\source_specific_candidate_generation_validation_v2"
    r"\20260514T030000Z-source-specific-candidate-generation-validation-v2-top5-primary"
)
DEFAULT_IIZUKA_PHASE3_ROOT = Path(r"G:\Tradex\iizuka_signal_expectancy_v1_phase3_candidate_generation\20260509T073130Z-180617")
DEFAULT_IIZUKA_PHASE3B_ROOT = Path(
    r"G:\Tradex\iizuka_signal_expectancy_v1_phase3b_candidate_generation_challenger\20260509T074051Z-432727"
)
DEFAULT_MA5_ROOT = Path(
    r"G:\Tradex\ma5_reclaim_hypothesis_batch_v1"
    r"\20260512T000000Z-ma5-reclaim-hypothesis-batch-v1-ma5_reclaim_hypothesis_batch_v1"
)
DEFAULT_SWING_ROOT = Path(r"G:\Tradex\swing_quality_selection_v1\20260512T000000Z-swing-quality-selection-v1-swing_quality_selection_v1")
DEFAULT_RELATIVE_ROOT = Path(r"G:\Tradex\relative_strength_family_final_decision\20260511T085417Z-relative_strength_family_final_decision")
DEFAULT_TEPPAN_WATCH_ROOT = Path(
    r"G:\Tradex\shadow_watch_logs\teppan_shadow_watch_mode_logging_v1"
    r"\20260514T130000Z-teppan-shadow-watch-mode-logging-v1"
)

REQUIRED_OUTPUTS = [
    "pattern_family_inventory.json",
    "pattern_family_quality_report.json",
    "pattern_family_overlap_report.json",
    "signal_frequency_report.json",
    "top5_candidate_pool_fit_report.json",
    "selected_pattern_families_for_validation.json",
    "next_axis_recommendation.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-parent", type=Path, default=DEFAULT_OUTPUT_PARENT)
    parser.add_argument("--run-id", default=DEFAULT_RUN_ID)
    parser.add_argument("--pre-strength-root", type=Path, default=DEFAULT_PRE_STRENGTH_ROOT)
    parser.add_argument("--guard-root", type=Path, default=DEFAULT_GUARD_ROOT)
    parser.add_argument("--source-v2-root", type=Path, default=DEFAULT_SOURCE_V2_ROOT)
    parser.add_argument("--iizuka-phase3-root", type=Path, default=DEFAULT_IIZUKA_PHASE3_ROOT)
    parser.add_argument("--iizuka-phase3b-root", type=Path, default=DEFAULT_IIZUKA_PHASE3B_ROOT)
    parser.add_argument("--ma5-root", type=Path, default=DEFAULT_MA5_ROOT)
    parser.add_argument("--swing-root", type=Path, default=DEFAULT_SWING_ROOT)
    parser.add_argument("--relative-root", type=Path, default=DEFAULT_RELATIVE_ROOT)
    parser.add_argument("--teppan-watch-root", type=Path, default=DEFAULT_TEPPAN_WATCH_ROOT)
    args = parser.parse_args()
    run_multi_pattern_candidate_generation_portfolio_v1(
        output_parent=args.output_parent,
        run_id=args.run_id,
        pre_strength_root=args.pre_strength_root,
        guard_root=args.guard_root,
        source_v2_root=args.source_v2_root,
        iizuka_phase3_root=args.iizuka_phase3_root,
        iizuka_phase3b_root=args.iizuka_phase3b_root,
        ma5_root=args.ma5_root,
        swing_root=args.swing_root,
        relative_root=args.relative_root,
        teppan_watch_root=args.teppan_watch_root,
    )
    return 0


def run_multi_pattern_candidate_generation_portfolio_v1(
    *,
    output_parent: Path = DEFAULT_OUTPUT_PARENT,
    run_id: str = DEFAULT_RUN_ID,
    pre_strength_root: Path = DEFAULT_PRE_STRENGTH_ROOT,
    guard_root: Path = DEFAULT_GUARD_ROOT,
    source_v2_root: Path = DEFAULT_SOURCE_V2_ROOT,
    iizuka_phase3_root: Path = DEFAULT_IIZUKA_PHASE3_ROOT,
    iizuka_phase3b_root: Path = DEFAULT_IIZUKA_PHASE3B_ROOT,
    ma5_root: Path = DEFAULT_MA5_ROOT,
    swing_root: Path = DEFAULT_SWING_ROOT,
    relative_root: Path = DEFAULT_RELATIVE_ROOT,
    teppan_watch_root: Path = DEFAULT_TEPPAN_WATCH_ROOT,
) -> dict[str, Any]:
    output_root = output_parent / run_id
    output_root.mkdir(parents=True, exist_ok=True)
    roots = {
        "pre_strength": pre_strength_root,
        "guard": guard_root,
        "source_v2": source_v2_root,
        "iizuka_phase3": iizuka_phase3_root,
        "iizuka_phase3b": iizuka_phase3b_root,
        "ma5": ma5_root,
        "swing": swing_root,
        "relative": relative_root,
        "teppan_watch": teppan_watch_root,
    }
    artifacts = _load_artifacts(roots)
    inventory = _build_inventory(artifacts, roots)
    quality = _quality_report(inventory)
    frequency = _frequency_report(inventory)
    top5_fit = _top5_fit_report(inventory)
    selected = _select_families(inventory)
    overlap = _overlap_report(inventory, selected)
    decision, reason = _decision(selected, inventory)
    next_axis = _next_axis(decision)
    research_decision = {
        "schema_version": "tradex_multi_pattern_candidate_generation_portfolio_research_decision_v1",
        "axis_id": AXIS_ID,
        "decision": decision,
        "decision_reason": reason,
        "selected_family_count": len(selected["selected_pattern_families"]),
        "selected_family_ids": [row["family_id"] for row in selected["selected_pattern_families"]],
        "teppan_status": "watch_only_low_frequency_pattern",
        "activation_allowed": False,
        "production_ranking_changed": False,
        "meemee_reflectable": False,
        "publish_created": False,
        "candidate_scoring_created": False,
        "boost_value_changed": False,
        "loss_guard_changed": False,
        "pattern_definitions_changed": False,
        "generated_at_utc": _utc_now(),
    }

    _write_json(output_root / "pattern_family_inventory.json", inventory)
    _write_json(output_root / "pattern_family_quality_report.json", quality)
    _write_json(output_root / "pattern_family_overlap_report.json", overlap)
    _write_json(output_root / "signal_frequency_report.json", frequency)
    _write_json(output_root / "top5_candidate_pool_fit_report.json", top5_fit)
    _write_json(output_root / "selected_pattern_families_for_validation.json", selected)
    _write_json(output_root / "next_axis_recommendation.json", next_axis)
    _write_json(output_root / "research_decision.json", research_decision)
    complete = _artifact_complete(output_root, research_decision)
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    return {
        "output_root": str(output_root),
        "research_decision": research_decision,
        "pattern_family_inventory": inventory,
        "pattern_family_quality_report": quality,
        "pattern_family_overlap_report": overlap,
        "signal_frequency_report": frequency,
        "top5_candidate_pool_fit_report": top5_fit,
        "selected_pattern_families_for_validation": selected,
        "next_axis_recommendation": next_axis,
        "artifact_complete": complete,
    }


def _load_artifacts(roots: Mapping[str, Path]) -> dict[str, Any]:
    return {
        "pre_strength_decision": _read_json_optional(roots["pre_strength"] / "research_decision.json"),
        "pre_strength_leaderboard": _read_json_optional(roots["pre_strength"] / "pattern_leaderboard.json"),
        "guard_decision": _read_json_optional(roots["guard"] / "research_decision.json"),
        "guard_leaderboard": _read_json_optional(roots["guard"] / "guard_leaderboard.json"),
        "source_v2_decision": _read_json_optional(roots["source_v2"] / "research_decision.json"),
        "source_v2_top5": _read_json_optional(roots["source_v2"] / "top5_candidate_pool_report.json"),
        "source_v2_noise": _read_json_optional(roots["source_v2"] / "source_noise_report.json"),
        "iizuka_phase3": _read_json_optional(roots["iizuka_phase3"] / "phase3_decision.json"),
        "iizuka_phase3b": _read_json_optional(roots["iizuka_phase3b"] / "phase3b_decision.json"),
        "ma5_decision": _read_json_optional(roots["ma5"] / "research_decision.json"),
        "ma5_leaderboard": _read_json_optional(roots["ma5"] / "hypothesis_leaderboard.json"),
        "swing_leaderboard": _read_json_optional(roots["swing"] / "family_leaderboard.json"),
        "relative_decision": _read_json_optional(roots["relative"] / "relative_strength_family_final_decision.json"),
        "teppan_watch": _read_json_optional(roots["teppan_watch"] / "watch_run_result.json"),
        "teppan_metrics": _read_json_optional(roots["teppan_watch"] / "teppan_watch_metrics.json"),
    }


def _build_inventory(artifacts: Mapping[str, Any], roots: Mapping[str, Path]) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    rows.extend(_pre_strength_rows(artifacts, roots["pre_strength"]))
    rows.extend(_guard_rows(artifacts, roots["guard"]))
    rows.append(_source_v2_row(artifacts, roots["source_v2"]))
    rows.append(_iizuka_row(artifacts, roots["iizuka_phase3"], roots["iizuka_phase3b"]))
    rows.extend(_ma5_rows(artifacts, roots["ma5"]))
    rows.append(_swing_row(artifacts, roots["swing"]))
    rows.append(_relative_row(artifacts, roots["relative"]))
    rows.append(_teppan_row(artifacts, roots["teppan_watch"]))
    rows = [_score_row(row) for row in rows if row]
    return {
        "schema_version": "tradex_pattern_family_inventory_v1",
        "axis_id": AXIS_ID,
        "source_roots": {key: str(value) for key, value in roots.items()},
        "family_count": len(rows),
        "rows": sorted(rows, key=lambda row: (-float(row.get("portfolio_priority_score") or 0), str(row["family_id"]))),
        "not_changed": _not_changed(),
    }


def _pre_strength_rows(artifacts: Mapping[str, Any], root: Path) -> list[dict[str, Any]]:
    leaderboard = artifacts.get("pre_strength_leaderboard") or {}
    rows = []
    for row in (leaderboard.get("rows") or [])[:12]:
        if not isinstance(row, Mapping):
            continue
        decision = str(row.get("pattern_decision") or "")
        if not decision.startswith("high_"):
            continue
        family_id = f"pre_strength::{row.get('family_id')}::{row.get('pattern_key')}"
        rows.append(
            {
                "family_id": family_id,
                "family_group": str(row.get("family_id") or "pre_strength"),
                "display_name": _pre_strength_name(row),
                "mechanism": _pre_strength_mechanism(row),
                "source_axis": "pre_strength_pattern_mining_v1",
                "source_artifact": str(root / "pattern_leaderboard.json"),
                "status": "validation_candidate",
                "role": "candidate_generation_pattern",
                "point_in_time_status": "pattern_keys_use_pre_event_current_observable_features",
                "frequency": {
                    "event_count": _int(row.get("event_count")),
                    "month_count": _int(row.get("month_count")),
                    "symbol_count": _int(row.get("symbol_count")),
                },
                "quality": {
                    "avg_ret20": _float(row.get("avg_ret20")),
                    "median_ret20": _float(row.get("median_ret20")),
                    "win_rate20": _float(row.get("win_rate20")),
                    "profit_factor20": _float(row.get("profit_factor20")),
                    "severe_loss_rate20": _float(row.get("severe_loss_rate20")),
                    "avg_mfe20": _float(row.get("avg_mfe20")),
                    "avg_mae20": _float(row.get("avg_mae20")),
                    "positive_month_rate20": _float(row.get("positive_month_rate20")),
                },
                "top5_candidate_pool_fit": "candidate_generation_validation_needed",
                "overlap_tags": _tags_from_mapping(row.get("pattern_features") or {}),
                "selection_notes": ["teppan_independent", "direct_candidate_generation_surface"],
            }
        )
    return rows


def _guard_rows(artifacts: Mapping[str, Any], root: Path) -> list[dict[str, Any]]:
    leaderboard = artifacts.get("guard_leaderboard") or {}
    rows = []
    for row in leaderboard.get("rows") or []:
        if not isinstance(row, Mapping) or row.get("guard_id") != "safe_full":
            continue
        rows.append(
            {
                "family_id": "defensive_safe_full_guard::safe_full",
                "family_group": "defensive_winner_low_severe_loss",
                "display_name": "defensive safe-full low severe-loss candidate surface",
                "mechanism": "low severe-loss guard surface for human-selectable top5 pool",
                "source_axis": "pre_strength_guard_validation_v1",
                "source_artifact": str(root / "guard_leaderboard.json"),
                "status": "validation_candidate_with_stability_caveat",
                "role": "candidate_generation_guard_surface",
                "point_in_time_status": "past_only_guard_features_reported",
                "frequency": {
                    "event_count": _int(row.get("n")),
                    "opportunity_days_count": _int(row.get("opportunity_days_count")),
                    "coverage_rate": _float(row.get("coverage_rate")),
                    "events_per_day_mean": _float((row.get("events_per_day_distribution") or {}).get("mean")),
                },
                "quality": {
                    "avg_ret20": _float(row.get("avg_ret20")),
                    "median_ret20": _float(row.get("median_ret20")),
                    "win_rate20": _float(row.get("win_rate20")),
                    "profit_factor20": None,
                    "severe_loss_rate20": _float(row.get("severe_loss_rate20")),
                    "avg_mfe20": _float(row.get("avg_MFE20")),
                    "avg_mae20": _float(row.get("avg_MAE20")),
                    "severe_loss_improvement_rate": _float(row.get("severe_loss_improvement_rate_vs_all_strength")),
                },
                "top5_candidate_pool_fit": "needs_topk_rotation_validation",
                "overlap_tags": ["guard_id=safe_full", "defensive_low_severe_loss"],
                "selection_notes": ["low_severe_loss", "human_review_pool_guard_candidate"],
            }
        )
    return rows


def _source_v2_row(artifacts: Mapping[str, Any], root: Path) -> dict[str, Any]:
    decision = artifacts.get("source_v2_decision") or {}
    noise = artifacts.get("source_v2_noise") or {}
    return {
        "family_id": "archived_source_specific_candidate_generation_v2",
        "family_group": "source_specific_candidate_generation",
        "display_name": "source-specific pre-MA20 near weekly strong-up source",
        "mechanism": "source_not_selected_due_to_max3_overfill and label-mismatch recovery",
        "source_axis": "source_specific_candidate_generation_validation_v2",
        "source_artifact": str(root / "research_decision.json"),
        "status": "drop_do_not_select",
        "role": "rejected_candidate_generation_family",
        "point_in_time_status": "same_date_support_not_faked_but_per_source_support_unavailable",
        "frequency": {"recovered_missed_winner_count": _int(noise.get("recovered_missed_winner_count"))},
        "quality": {
            "severe_loser_added_per_recovered_winner": _float(noise.get("severe_loser_added_per_recovered_winner")),
            "nonwinner_added_per_recovered_winner": _float(noise.get("nonwinner_added_per_recovered_winner")),
        },
        "top5_candidate_pool_fit": "failed_top5_quality_despite_branching",
        "overlap_tags": ["pre_ma20_near", "pre_ma60_near_or_above", "weekly_prior_strong_up", "negative_guard_match=True"],
        "selection_notes": [
            "drop",
            str(decision.get("authoritative_research_decision") or decision.get("decision")),
            "too_noisy",
        ],
    }


def _iizuka_row(artifacts: Mapping[str, Any], phase3_root: Path, phase3b_root: Path) -> dict[str, Any]:
    phase3 = artifacts.get("iizuka_phase3") or {}
    phase3b = artifacts.get("iizuka_phase3b") or {}
    return {
        "family_id": "iizuka_monthly_C_pullback_end_reclaim7",
        "family_group": "pullback_recovery_reclaim",
        "display_name": "monthly-C pullback-end reclaim7",
        "mechanism": "monthly C regime pullback end, MA7 reclaim while above MA20",
        "source_axis": "iizuka_signal_expectancy_phase3_candidate_generation",
        "source_artifact": str(phase3_root / "phase3_decision.json"),
        "secondary_artifact": str(phase3b_root / "phase3b_decision.json"),
        "status": "hold_requires_risk_filter_before_portfolio_validation",
        "role": "candidate_generation_pattern",
        "point_in_time_status": "contract_defined_no_lookahead_signal",
        "frequency": {
            "signal_only_count": _int(phase3.get("signal_only_count")),
            "added_signal_candidate_count": _int(phase3b.get("added_signal_candidate_count")),
            "additive_candidate_count": _int(phase3b.get("additive_candidate_count")),
        },
        "quality": {
            "ret20_mean_delta": _float(phase3b.get("ret20_mean_delta")),
            "ret20_median_delta": _float(phase3b.get("ret20_median_delta")),
            "win_rate20_delta": _float(phase3b.get("win_rate20_delta")),
            "severe_loser_delta": _float(phase3b.get("severe_loser_delta")),
        },
        "top5_candidate_pool_fit": "blocked_rank_readiness_and_risk_filter_required",
        "overlap_tags": ["monthly_C", "pullback_end", "reclaim7", "above_MA20"],
        "selection_notes": ["good_mechanism", "risk_filter_required_before_candidate_generation"],
    }


def _ma5_rows(artifacts: Mapping[str, Any], root: Path) -> list[dict[str, Any]]:
    leaderboard = artifacts.get("ma5_leaderboard") or {}
    rows = []
    for row in (leaderboard.get("rows") or [])[:4]:
        if not isinstance(row, Mapping):
            continue
        rows.append(
            {
                "family_id": f"ma5_reclaim_filter::{row.get('hypothesis_id')}",
                "family_group": "ma_reclaim_context_filter",
                "display_name": str(row.get("thesis") or row.get("hypothesis_id")),
                "mechanism": "MA5 reclaim contextual filter; not yet additive candidate generation",
                "source_axis": "ma5_reclaim_hypothesis_batch_v1",
                "source_artifact": str(root / "hypothesis_leaderboard.json"),
                "status": "hold_not_candidate_generation_yet",
                "role": "context_filter_not_generation_surface",
                "point_in_time_status": "past_price_ma_context",
                "frequency": {"trade_count": _int(row.get("trade_count")), "symbol_count": _int(row.get("symbol_count"))},
                "quality": {
                    "avg_ret": _float(row.get("avg_ret")),
                    "avg_ret_delta_vs_base": _float(row.get("avg_ret_delta_vs_base")),
                    "profit_factor": _float(row.get("profit_factor")),
                    "severe_loss_rate": _float(row.get("severe_loss_rate")),
                },
                "top5_candidate_pool_fit": "not_additive_candidate_generation_reentry_unmodeled",
                "overlap_tags": ["ma5_reclaim", str(row.get("hypothesis_id") or "")],
                "selection_notes": ["context_filter_only", "do_not_treat_as_generation_without_reentry_model"],
            }
        )
    return rows


def _swing_row(artifacts: Mapping[str, Any], root: Path) -> dict[str, Any]:
    data = artifacts.get("swing_leaderboard") or {}
    candidate = ((data.get("candidate_rows") or [{}])[0]) if isinstance(data.get("candidate_rows"), list) else {}
    return {
        "family_id": "swing_quality_selection_v1",
        "family_group": "swing_quality",
        "display_name": "swing quality rerank",
        "mechanism": "swing-quality rerank changed topK but harmed top5 and severe loss",
        "source_axis": "swing_quality_selection_v1",
        "source_artifact": str(root / "family_leaderboard.json"),
        "status": "drop_do_not_select",
        "role": "rejected_rerank_family",
        "frequency": {"changed_top5_members_count": _int(candidate.get("changed_top5_members_count"))},
        "quality": {
            "top5_hold_end_return_20d_delta": _float(candidate.get("top5_hold_end_return_20d_delta")),
            "top5_severe_loss_rate_delta": _float(candidate.get("top5_severe_loss_rate_delta")),
        },
        "top5_candidate_pool_fit": "failed_top5_improved_and_severe_loss_not_worse",
        "overlap_tags": ["swing_quality"],
        "selection_notes": ["drop", "same_condition_failed_top5_quality"],
    }


def _relative_row(artifacts: Mapping[str, Any], root: Path) -> dict[str, Any]:
    data = artifacts.get("relative_decision") or {}
    metrics = (data.get("key_metrics") or {}).get("reranker") or {}
    return {
        "family_id": "relative_strength_family",
        "family_group": "relative_strength",
        "display_name": "relative strength persistence",
        "mechanism": "relative strength persistence rerank/veto",
        "source_axis": "relative_strength_family_final_decision",
        "source_artifact": str(root / "relative_strength_family_final_decision.json"),
        "status": "drop_do_not_select",
        "role": "rejected_rerank_family",
        "frequency": {"changed_top5_members_count": _int(metrics.get("changed_top5_members_count"))},
        "quality": {
            "top5_return_delta": _float(metrics.get("top5_return_delta")),
            "top5_severe_loser_rate_delta": _float(metrics.get("top5_severe_loser_rate_delta")),
        },
        "top5_candidate_pool_fit": "failed_top5_expectancy",
        "overlap_tags": ["relative_strength"],
        "selection_notes": ["drop", str(data.get("final_decision") or "")],
    }


def _teppan_row(artifacts: Mapping[str, Any], root: Path) -> dict[str, Any]:
    metrics = artifacts.get("teppan_metrics") or {}
    return {
        "family_id": "teppan_watch_only",
        "family_group": "teppan",
        "display_name": "teppan rare high-quality watch pattern",
        "mechanism": "rare high-quality chart pattern, manual review only when lit",
        "source_axis": "teppan_shadow_watch_mode_logging_v1",
        "source_artifact": str(root / "watch_run_result.json"),
        "status": "watch_only_do_not_select_for_frequency_portfolio",
        "role": "watch_pattern",
        "frequency": {
            "latest_top100_pattern_match_count": _int(metrics.get("top100_teppan_pattern_match_count")),
            "latest_boost_eligible_count": _int(metrics.get("boost_eligible_count")),
        },
        "quality": {},
        "top5_candidate_pool_fit": "rare_watch_only_currently_no_live_candidate_value",
        "overlap_tags": ["teppan", "rare_watch"],
        "selection_notes": ["watch_only", "activation_blocked"],
    }


def _score_row(row: dict[str, Any]) -> dict[str, Any]:
    quality = row.get("quality") or {}
    frequency = row.get("frequency") or {}
    severe = _first_float(quality, "severe_loss_rate20", "severe_loss_rate", default=0.25)
    win = _first_float(quality, "win_rate20", "win_rate", default=0.5)
    avg_ret = _first_float(quality, "avg_ret20", "avg_ret", default=0.0)
    pf = _first_float(quality, "profit_factor20", "profit_factor", default=1.0)
    count = _first_int(frequency, "event_count", "trade_count", "signal_only_count", "opportunity_days_count", default=0)
    status = str(row.get("status") or "")
    if status.startswith("drop") or "watch_only" in status or status.startswith("hold_not_candidate"):
        score = 0.0
    else:
        score = 0.0
        score += min(count / 300.0, 1.0) * 0.25
        score += max(avg_ret, 0.0) * 4.0
        score += max(win - 0.5, 0.0) * 1.5
        score += max(pf - 1.0, 0.0) * 0.2
        score += max(0.25 - severe, 0.0) * 1.4
        if row.get("role") == "candidate_generation_pattern":
            score += 0.15
        if "stability_caveat" in status:
            score -= 0.08
        if "risk_filter" in status:
            score -= 0.12
    row["portfolio_priority_score"] = round(max(score, 0.0), 6)
    row["quality_gate_summary"] = {
        "frequency_not_too_low": count >= 120,
        "severe_loss_acceptable": severe <= 0.25,
        "winner_rate_or_return_positive": win >= 0.55 or avg_ret > 0.01,
        "top5_candidate_pool_validation_needed": "validation_needed" in str(row.get("top5_candidate_pool_fit")),
    }
    return row


def _quality_report(inventory: Mapping[str, Any]) -> dict[str, Any]:
    rows = inventory["rows"]
    return {
        "schema_version": "tradex_pattern_family_quality_report_v1",
        "axis_id": AXIS_ID,
        "selected_quality_candidates": [
            _quality_view(row) for row in rows if row.get("status") in {"validation_candidate", "validation_candidate_with_stability_caveat"}
        ],
        "rejected_or_hold_families": [
            {
                "family_id": row["family_id"],
                "status": row.get("status"),
                "reason": row.get("top5_candidate_pool_fit"),
                "selection_notes": row.get("selection_notes"),
            }
            for row in rows
            if row.get("status") not in {"validation_candidate", "validation_candidate_with_stability_caveat"}
        ],
    }


def _overlap_report(inventory: Mapping[str, Any], selected: Mapping[str, Any] | None = None) -> dict[str, Any]:
    candidates = [row for row in inventory["rows"] if row.get("portfolio_priority_score", 0) > 0]
    pairs = _overlap_pairs(candidates)
    selected_ids = {row["family_id"] for row in (selected or {}).get("selected_pattern_families", [])}
    selected_rows = [row for row in candidates if row["family_id"] in selected_ids]
    selected_pairs = _overlap_pairs(selected_rows)
    return {
        "schema_version": "tradex_pattern_family_overlap_report_v1",
        "axis_id": AXIS_ID,
        "inventory_pair_count": len(pairs),
        "inventory_pairs": pairs,
        "inventory_overlap_assessment": "acceptable" if all(row["overlap_level"] != "high" for row in pairs) else "needs_deduplication",
        "selected_pair_count": len(selected_pairs),
        "selected_pairs": selected_pairs,
        "selected_portfolio_overlap_assessment": (
            "acceptable_low_overlap" if all(row["overlap_level"] == "low" for row in selected_pairs) else "needs_manual_deduplication"
        ),
    }


def _overlap_pairs(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    pairs = []
    for left, right in combinations(rows, 2):
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
    return pairs


def _frequency_report(inventory: Mapping[str, Any]) -> dict[str, Any]:
    rows = []
    for row in inventory["rows"]:
        frequency = row.get("frequency") or {}
        event_count = _first_int(frequency, "event_count", "trade_count", "signal_only_count", "opportunity_days_count", default=0)
        month_count = _int(frequency.get("month_count"))
        rows.append(
            {
                "family_id": row["family_id"],
                "status": row.get("status"),
                "event_or_signal_count": event_count,
                "month_count": month_count,
                "frequency_bucket": "regular" if event_count >= 250 else "moderate" if event_count >= 120 else "rare_or_unproven",
                "suitable_for_regular_candidate_supply": row.get("portfolio_priority_score", 0) > 0 and event_count >= 120,
            }
        )
    return {"schema_version": "tradex_signal_frequency_report_v1", "axis_id": AXIS_ID, "rows": rows}


def _top5_fit_report(inventory: Mapping[str, Any]) -> dict[str, Any]:
    rows = []
    for row in inventory["rows"]:
        score = float(row.get("portfolio_priority_score") or 0.0)
        rows.append(
            {
                "family_id": row["family_id"],
                "top5_candidate_pool_fit": row.get("top5_candidate_pool_fit"),
                "portfolio_priority_score": score,
                "fit_decision": "select_for_validation" if score > 0.35 else "hold_or_reject",
                "reason": row.get("selection_notes"),
            }
        )
    return {"schema_version": "tradex_top5_candidate_pool_fit_report_v1", "axis_id": AXIS_ID, "rows": rows}


def _select_families(inventory: Mapping[str, Any]) -> dict[str, Any]:
    eligible = [
        row
        for row in inventory["rows"]
        if row.get("status") in {"validation_candidate", "validation_candidate_with_stability_caveat"}
        and float(row.get("portfolio_priority_score") or 0.0) > 0.35
    ]
    selected: list[dict[str, Any]] = []
    used_groups: set[str] = set()
    for row in sorted(eligible, key=lambda item: -float(item.get("portfolio_priority_score") or 0.0)):
        if len(selected) >= 3:
            break
        group = str(row.get("family_group"))
        if group in used_groups and len(selected) >= 1:
            continue
        selected.append(_selected_view(row))
        used_groups.add(group)
    return {
        "schema_version": "tradex_selected_pattern_families_for_validation_v1",
        "axis_id": AXIS_ID,
        "selected_pattern_families": selected,
        "selection_count": len(selected),
        "selection_limit": 3,
        "validation_next_axis": "selected_pattern_family_validation_v1" if selected else None,
        "teppan_policy": "keep_watch_only_not_selected_for_frequency_portfolio",
    }


def _decision(selected: Mapping[str, Any], inventory: Mapping[str, Any]) -> tuple[str, str]:
    count = int(selected.get("selection_count") or 0)
    if count >= 1:
        return "pattern_family_portfolio_ready", "one_to_three_independent_validation_families_selected_with_teppan_watch_only"
    if any(row.get("status") in {"validation_candidate", "validation_candidate_with_stability_caveat"} for row in inventory["rows"]):
        return "hold", "candidate_families_exist_but_frequency_or_quality_is_below_selection_threshold"
    return "drop_redesign", "no_usable_non_teppan_pattern_family_found"


def _next_axis(decision: str) -> dict[str, Any]:
    next_axis = {
        "pattern_family_portfolio_ready": "selected_pattern_family_validation_v1",
        "hold": "pattern_family_feature_extraction_audit_v1",
        "drop_redesign": "broader_candidate_generation_design_reset_v1",
    }.get(decision, "pattern_family_feature_extraction_audit_v1")
    return {
        "schema_version": "tradex_pattern_family_portfolio_next_axis_recommendation_v1",
        "decision": decision,
        "next": next_axis,
        "activation_allowed": False,
        "meemee_reflection_allowed": False,
    }


def _pre_strength_name(row: Mapping[str, Any]) -> str:
    family = str(row.get("family_id") or "")
    features = row.get("pattern_features") or {}
    if "volume_expansion" in str(features):
        return f"{family} volume-led candidate pattern"
    if "reclaim" in str(features):
        return f"{family} reclaim/recovery candidate pattern"
    return f"{family} pre-strength candidate pattern"


def _pre_strength_mechanism(row: Mapping[str, Any]) -> str:
    family = str(row.get("family_id") or "")
    if family == "pre_reclaim_accumulation":
        return "pre-strength accumulation with MA context, compression/volume, and weekly prior trend"
    if family == "pre_to_event_confirmation":
        return "pre-strength setup confirmed by event-day candle/ret20 transition"
    if family == "pre_candle_quality":
        return "pre-event candle quality and wick/volume context before strength"
    return "pre-strength observable pattern before forward outcome window"


def _quality_view(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "family_id": row["family_id"],
        "display_name": row.get("display_name"),
        "status": row.get("status"),
        "portfolio_priority_score": row.get("portfolio_priority_score"),
        "frequency": row.get("frequency"),
        "quality": row.get("quality"),
        "top5_candidate_pool_fit": row.get("top5_candidate_pool_fit"),
        "quality_gate_summary": row.get("quality_gate_summary"),
    }


def _selected_view(row: Mapping[str, Any]) -> dict[str, Any]:
    out = _quality_view(row)
    out["mechanism"] = row.get("mechanism")
    out["source_axis"] = row.get("source_axis")
    out["source_artifact"] = row.get("source_artifact")
    out["point_in_time_status"] = row.get("point_in_time_status")
    out["validation_scope"] = "TRADEX_only_fixed_condition_top5_candidate_pool_validation"
    return out


def _tags_from_mapping(mapping: Mapping[str, Any]) -> list[str]:
    return [f"{key}={value}" for key, value in sorted(mapping.items())]


def _first_float(mapping: Mapping[str, Any], *keys: str, default: float = 0.0) -> float:
    for key in keys:
        if mapping.get(key) is not None:
            return _float(mapping.get(key), default)
    return default


def _first_int(mapping: Mapping[str, Any], *keys: str, default: int = 0) -> int:
    for key in keys:
        if mapping.get(key) is not None:
            return _int(mapping.get(key), default)
    return default


def _read_json_optional(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"_missing": True, "_path": str(path)}
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    return data if isinstance(data, dict) else {"_not_object": True, "_path": str(path)}


def _artifact_complete(output_root: Path, decision: Mapping[str, Any]) -> dict[str, Any]:
    presence = {name: (output_root / name).exists() for name in REQUIRED_OUTPUTS if name != "_ARTIFACT_COMPLETE.json"}
    presence["_ARTIFACT_COMPLETE.json"] = True
    return {
        "schema_version": "tradex_multi_pattern_candidate_generation_portfolio_artifact_complete_v1",
        "axis_id": AXIS_ID,
        "decision": decision.get("decision"),
        "complete": all(presence.values()),
        "required_outputs": REQUIRED_OUTPUTS,
        "present_outputs": presence,
        "output_root": str(output_root),
        "silent_fallback_used": False,
    }


def _not_changed() -> list[str]:
    return [
        "teppan_activation",
        "teppan_boost_value",
        "teppan_loss_guard",
        "teppan_pattern_definitions",
        "production_ranking",
        "MeeMee_runtime",
        "frontend_backend_ui_api",
        "publish_registry",
        "threshold_no_trade_policy",
        "image_fusion",
    ]


def _float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None or pd.isna(value):
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
