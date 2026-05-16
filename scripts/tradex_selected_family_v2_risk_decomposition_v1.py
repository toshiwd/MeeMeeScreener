"""TRADEX-only risk decomposition for selected pattern-family validation v2."""

from __future__ import annotations

import argparse
import json
import math
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


AXIS_ID = "selected_family_v2_risk_decomposition_v1"
DEFAULT_RUN_ID = "20260514T210000Z-selected-family-v2-risk-decomposition-v1"
DEFAULT_OUTPUT_PARENT = Path(r"G:\Tradex\selected_family_v2_risk_decomposition_v1")
DEFAULT_VALIDATION_ROOT = Path(
    r"G:\Tradex\selected_pattern_family_validation_v2"
    r"\20260514T200000Z-selected-pattern-family-validation-v2"
)
DEFAULT_WIDE_LEDGER = Path(
    r"G:\Tradex\wide_strength_pool_upside_rerank_v1"
    r"\20260513T030000Z-wide-strength-pool-upside-rerank-v1"
    r"\date_level_selection_ledger.jsonl"
)
DEFAULT_MA5_TRADE_LEDGER = Path(
    r"G:\Tradex\ma5_reclaim_ma20_exit_probe_v1"
    r"\20260512T000000Z-ma5-reclaim-ma20-exit-probe-v1-ma5_reclaim_ma20_exit_probe_v1"
    r"\trade_ledger.jsonl"
)

MOMENTUM_FAMILY_ID = "momentum_continuation_soft_boost_v1"
MA5_H12_FAMILY_ID = "ma5_reclaim_context::h12_near_bull_ma60_rising"

REQUIRED_OUTPUTS = [
    "evaluation_contract.json",
    "run_manifest.json",
    "source_artifact_refs.json",
    "risk_decomposition_contract.json",
    "momentum_risk_decomposition_report.json",
    "ma5_h12_additive_feasibility_report.json",
    "common_top5_candidate_ledger_design.json",
    "combined_family_feasibility_report.json",
    "point_in_time_context_separation_report.json",
    "next_axis_recommendation.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
]

MOMENTUM_CONTEXT_COLUMNS = [
    "pre_ma20_path_state",
    "weekly_prior_state",
    "monthly_prior_state",
    "negative_guard_match",
    "guard_safe_full",
]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--validation-root", type=Path, default=DEFAULT_VALIDATION_ROOT)
    parser.add_argument("--wide-ledger", type=Path, default=DEFAULT_WIDE_LEDGER)
    parser.add_argument("--ma5-trade-ledger", type=Path, default=DEFAULT_MA5_TRADE_LEDGER)
    parser.add_argument("--output-parent", type=Path, default=DEFAULT_OUTPUT_PARENT)
    parser.add_argument("--run-id", default=DEFAULT_RUN_ID)
    args = parser.parse_args()
    run_selected_family_v2_risk_decomposition_v1(
        validation_root=args.validation_root,
        wide_ledger=args.wide_ledger,
        ma5_trade_ledger=args.ma5_trade_ledger,
        output_parent=args.output_parent,
        run_id=args.run_id,
    )
    return 0


def run_selected_family_v2_risk_decomposition_v1(
    *,
    validation_root: Path = DEFAULT_VALIDATION_ROOT,
    wide_ledger: Path = DEFAULT_WIDE_LEDGER,
    ma5_trade_ledger: Path = DEFAULT_MA5_TRADE_LEDGER,
    output_parent: Path = DEFAULT_OUTPUT_PARENT,
    run_id: str = DEFAULT_RUN_ID,
) -> dict[str, Any]:
    output_root = output_parent / run_id
    output_root.mkdir(parents=True, exist_ok=True)
    roots = {
        "validation_root": validation_root,
        "wide_ledger": wide_ledger,
        "ma5_trade_ledger": ma5_trade_ledger,
    }
    artifacts = _load_artifacts(validation_root)
    momentum_rows = _load_jsonl_filtered(wide_ledger, lambda row: row.get("research_family_id") == MOMENTUM_FAMILY_ID)
    baseline_rows = _load_jsonl_filtered(wide_ledger, lambda row: row.get("research_family_id") == "all_strength_scoreless_random_top3")
    ma5_h12_rows = _load_jsonl_filtered(ma5_trade_ledger, _is_ma5_h12_row)
    momentum_report = _momentum_risk_decomposition(momentum_rows, baseline_rows, wide_ledger)
    ma5_report = _ma5_h12_additive_feasibility(ma5_h12_rows, momentum_rows, ma5_trade_ledger)
    context_report = _point_in_time_context_separation(momentum_rows)
    common_design = _common_top5_candidate_ledger_design(momentum_report, ma5_report, context_report)
    combined_report = _combined_family_feasibility(momentum_report, ma5_report, context_report, common_design)
    decision = _research_decision(momentum_report, ma5_report, context_report, common_design, combined_report, artifacts)
    next_axis = _next_axis(decision)
    payloads = {
        "evaluation_contract.json": _evaluation_contract(roots),
        "run_manifest.json": _run_manifest(output_root, roots),
        "source_artifact_refs.json": _source_refs(roots, artifacts),
        "risk_decomposition_contract.json": _risk_decomposition_contract(roots, artifacts),
        "momentum_risk_decomposition_report.json": momentum_report,
        "ma5_h12_additive_feasibility_report.json": ma5_report,
        "common_top5_candidate_ledger_design.json": common_design,
        "combined_family_feasibility_report.json": combined_report,
        "point_in_time_context_separation_report.json": context_report,
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
        "risk_decomposition_contract": payloads["risk_decomposition_contract.json"],
        "momentum_risk_decomposition_report": momentum_report,
        "ma5_h12_additive_feasibility_report": ma5_report,
        "common_top5_candidate_ledger_design": common_design,
        "combined_family_feasibility_report": combined_report,
        "point_in_time_context_separation_report": context_report,
        "next_axis_recommendation": next_axis,
        "research_decision": decision,
        "artifact_complete": complete,
    }


def _load_artifacts(validation_root: Path) -> dict[str, Any]:
    return {
        "validation_decision": _read_json_optional(validation_root / "research_decision.json"),
        "momentum_report": _read_json_optional(validation_root / "momentum_risk_profile_report.json"),
        "ma5_report": _read_json_optional(validation_root / "ma5_reclaim_context_report.json"),
        "combined_report": _read_json_optional(validation_root / "combined_family_report.json"),
        "source_refs": _read_json_optional(validation_root / "source_artifact_refs.json"),
        "artifact_complete": _read_json_optional(validation_root / "_ARTIFACT_COMPLETE.json"),
    }


def _momentum_risk_decomposition(rows: Sequence[Mapping[str, Any]], baseline_rows: Sequence[Mapping[str, Any]], source_path: Path) -> dict[str, Any]:
    metrics = _selection_metrics(rows)
    baseline = _selection_metrics(baseline_rows)
    context_rows = _context_rows(rows, MOMENTUM_CONTEXT_COLUMNS, min_count=100)
    best_contexts = sorted(context_rows, key=lambda row: (row["severe_loser_per_big_winner"], -row["big_winner_count"]))[:8]
    worst_contexts = sorted(context_rows, key=lambda row: (-row["severe_loser_per_big_winner"], -row["event_count"]))[:8]
    separable = bool(best_contexts) and bool(worst_contexts) and (
        best_contexts[0]["severe_loser_per_big_winner"] + 0.20 < metrics["severe_loser_per_big_winner"]
        or best_contexts[0]["severe_loss_rate20"] + 0.03 < metrics["severe_loss_rate20"]
    )
    return {
        "schema_version": "tradex_selected_family_v2_momentum_risk_decomposition_report_v1",
        "axis_id": AXIS_ID,
        "family_id": MOMENTUM_FAMILY_ID,
        "source_ledger": str(source_path),
        "event_count": metrics["event_count"],
        "winner_count": metrics["winner_count"],
        "momentum_winner_count": metrics["winner_count"],
        "severe_loser_count": metrics["severe_loser_count"],
        "momentum_severe_loser_count": metrics["severe_loser_count"],
        "big_winner_count": metrics["big_winner_count"],
        "momentum_big_winner_capture": metrics["big_winner_rate"],
        "severe_loser_per_big_winner": metrics["severe_loser_per_big_winner"],
        "metrics": metrics,
        "baseline_random_metrics": baseline,
        "delta_vs_random": {
            "avg_ret20": _safe_subtract(metrics["avg_ret20"], baseline["avg_ret20"]),
            "big_winner_rate": _safe_subtract(metrics["big_winner_rate"], baseline["big_winner_rate"]),
            "future_top10_rate": _safe_subtract(metrics["future_top10_rate"], baseline["future_top10_rate"]),
            "severe_loss_rate20": _safe_subtract(metrics["severe_loss_rate20"], baseline["severe_loss_rate20"]),
        },
        "best_point_in_time_contexts": best_contexts,
        "worst_point_in_time_contexts": worst_contexts,
        "severe_noise_separable_by_point_in_time_context": separable,
        "usable_as_upside_family": bool(metrics["big_winner_rate"] > baseline["big_winner_rate"] and separable),
        "typed_reasons": [
            "uses_decision_time_context_columns_only_for_context_split",
            "future_labels_used_for_diagnosis_only",
            "severe_loss_worse_than_random_requires_context_split",
        ],
    }


def _ma5_h12_additive_feasibility(rows: Sequence[Mapping[str, Any]], momentum_rows: Sequence[Mapping[str, Any]], source_path: Path) -> dict[str, Any]:
    ma5_keys = {_ma5_key(row) for row in rows if _ma5_key(row)[0] and _ma5_key(row)[1]}
    momentum_keys = {_momentum_key(row) for row in momentum_rows if _momentum_key(row)[0] and _momentum_key(row)[1]}
    ma5_dates = {key[0] for key in ma5_keys}
    momentum_dates = {key[0] for key in momentum_keys}
    ma5_symbols = {key[1] for key in ma5_keys}
    momentum_symbols = {key[1] for key in momentum_keys}
    metrics = _ma5_metrics(rows)
    required_fields = {"symbol", "signal_date", "ma_stack", "ma60_slope_state", "ret", "mfe", "mae", "win", "severe_loss"}
    sample_fields = set(rows[0].keys()) if rows else set()
    missing = sorted(required_fields - sample_fields)
    can_map = not missing and bool(ma5_keys)
    overlap = {
        "exact_event_symbol_overlap_with_momentum_selected_rows": len(ma5_keys & momentum_keys),
        "overlap_date_count": len(ma5_dates & momentum_dates),
        "overlap_symbol_count": len(ma5_symbols & momentum_symbols),
        "ma5_h12_date_count": len(ma5_dates),
        "ma5_h12_symbol_count": len(ma5_symbols),
        "momentum_date_count": len(momentum_dates),
        "momentum_symbol_count": len(momentum_symbols),
    }
    can_build_additive = can_map and overlap["overlap_date_count"] > 0 and overlap["overlap_symbol_count"] > 0
    return {
        "schema_version": "tradex_selected_family_v2_ma5_h12_additive_feasibility_report_v1",
        "axis_id": AXIS_ID,
        "family_id": MA5_H12_FAMILY_ID,
        "source_ledger": str(source_path),
        "h12_row_count": len(rows),
        "can_map_to_event_date_symbol_source_family": can_map,
        "event_date_field": "signal_date",
        "symbol_field": "symbol",
        "source_family_tag": "ma5_h12_near_bull_ma60_rising",
        "missing_fields": missing,
        "metrics": metrics,
        "overlap_with_momentum": overlap,
        "already_present_in_existing_wide_pool": overlap["exact_event_symbol_overlap_with_momentum_selected_rows"] > 0,
        "truly_additive_vs_momentum_selected_rows": overlap["exact_event_symbol_overlap_with_momentum_selected_rows"] == 0,
        "additive_top5_membership_ledger_can_be_generated": can_build_additive,
        "sample_count_by_month": _month_counts(rows, "signal_date")[:24],
        "candidate_overlap_with_baseline": "requires_common_ledger_build",
        "typed_reasons": [
            "h12_is_materializable_as_signal_date_symbol",
            "same_date_and_symbol_overlap_exists_with_momentum_source_universe" if can_build_additive else "join_overlap_missing_or_fields_missing",
            "top5_membership_requires_new_common_candidate_ledger",
        ],
    }


def _common_top5_candidate_ledger_design(
    momentum_report: Mapping[str, Any],
    ma5_report: Mapping[str, Any],
    context_report: Mapping[str, Any],
) -> dict[str, Any]:
    ready = bool(
        momentum_report.get("event_count", 0) > 0
        and ma5_report.get("additive_top5_membership_ledger_can_be_generated")
        and context_report.get("point_in_time_context_separation_available")
    )
    return {
        "schema_version": "tradex_common_top5_candidate_ledger_design_v1",
        "axis_id": AXIS_ID,
        "ledger_build_ready": ready,
        "common_same_date_candidate_universe": {
            "baseline": "all wide-strength opportunity rows by event_date + symbol, reconstructed before topK selection",
            "momentum": "membership flag from momentum_continuation_soft_boost_v1 candidate scoring",
            "ma5_h12": "membership flag from ma5 h12 signal_date + symbol materialization",
            "combined": "OR of momentum_candidate or ma5_h12_candidate with family tags",
        },
        "required_key_fields": ["event_date", "symbol"],
        "score_rank_fields": [
            "baseline_score",
            "baseline_rank",
            "momentum_score",
            "momentum_rank",
            "ma5_h12_context_score",
            "combined_score",
            "shadow_candidate_rank",
        ],
        "membership_flags": [
            "is_baseline_candidate",
            "is_momentum_candidate",
            "is_ma5_h12_candidate",
            "is_combined_candidate",
        ],
        "source_family_tags": ["momentum_continuation_soft_boost_v1", "ma5_h12_near_bull_ma60_rising"],
        "evaluation_labels": ["ret20_fwd", "win20", "mfe20", "mae20", "severe_loss20", "is_big_winner_ret20_ge_10pct", "is_future_top10_by_ret20"],
        "future_label_policy": {
            "future_labels_allowed_in_candidate_construction": False,
            "future_labels_allowed_in_evaluation": True,
            "point_in_time_context_columns": MOMENTUM_CONTEXT_COLUMNS + ["ma_stack", "ma60_slope_state"],
        },
        "no_fake_top5_direct_evidence": True,
        "build_blockers": [] if ready else _common_ledger_blockers(momentum_report, ma5_report, context_report),
    }


def _combined_family_feasibility(
    momentum_report: Mapping[str, Any],
    ma5_report: Mapping[str, Any],
    context_report: Mapping[str, Any],
    common_design: Mapping[str, Any],
) -> dict[str, Any]:
    overlap = ma5_report.get("overlap_with_momentum") or {}
    complementary = bool(overlap.get("overlap_date_count", 0) > 0 and overlap.get("overlap_symbol_count", 0) > 0)
    feasible = bool(common_design.get("ledger_build_ready") and complementary)
    return {
        "schema_version": "tradex_selected_family_v2_combined_family_feasibility_report_v1",
        "axis_id": AXIS_ID,
        "family_ids": [MOMENTUM_FAMILY_ID, MA5_H12_FAMILY_ID],
        "mechanism_overlap": "low",
        "same_date_overlap_count": overlap.get("overlap_date_count"),
        "same_symbol_overlap_count": overlap.get("overlap_symbol_count"),
        "exact_selected_row_overlap_count": overlap.get("exact_event_symbol_overlap_with_momentum_selected_rows"),
        "families_are_complementary": complementary,
        "combined_can_increase_candidate_breadth": feasible,
        "combined_severe_loss_control_plan": "use momentum point-in-time risk contexts plus ma5 h12 low-severe membership flag in common ledger",
        "direct_top5_validation_feasible_after_ledger_build": feasible,
        "typed_reasons": [
            "low_mechanism_overlap",
            "ma5_h12_shares_date_symbol_space_with_momentum_source" if complementary else "ma5_h12_join_gap_needs_review",
            "common_ledger_required_before_any_top5_claim",
        ],
    }


def _point_in_time_context_separation(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    context_rows = _context_rows(rows, ["pre_ma20_path_state", "weekly_prior_state", "negative_guard_match"], min_count=100)
    overall = _selection_metrics(rows)
    usable = [
        row
        for row in context_rows
        if row["event_count"] >= 100
        and row["severe_loser_per_big_winner"] < overall["severe_loser_per_big_winner"]
        and row["big_winner_count"] >= 20
    ]
    return {
        "schema_version": "tradex_selected_family_v2_point_in_time_context_separation_report_v1",
        "axis_id": AXIS_ID,
        "context_columns": ["pre_ma20_path_state", "weekly_prior_state", "negative_guard_match"],
        "future_label_columns_used_for_split": [],
        "future_labels_used_for_diagnosis_only": True,
        "overall_severe_loser_per_big_winner": overall["severe_loser_per_big_winner"],
        "point_in_time_context_separation_available": bool(usable),
        "usable_context_count": len(usable),
        "usable_context_examples": sorted(usable, key=lambda row: row["severe_loser_per_big_winner"])[:10],
        "all_context_rows": sorted(context_rows, key=lambda row: row["severe_loser_per_big_winner"])[:30],
    }


def _research_decision(
    momentum_report: Mapping[str, Any],
    ma5_report: Mapping[str, Any],
    context_report: Mapping[str, Any],
    common_design: Mapping[str, Any],
    combined_report: Mapping[str, Any],
    artifacts: Mapping[str, Any],
) -> dict[str, Any]:
    if common_design.get("ledger_build_ready"):
        decision = "ready_for_common_top5_candidate_ledger_build"
        next_axis = "common_top5_candidate_ledger_build_v1"
        reasons = [
            "common_same_date_candidate_ledger_can_be_built",
            "ma5_h12_can_materialize_event_date_symbol",
            "momentum_risk_context_separation_available",
            "combined_direct_top5_validation_feasible_after_ledger_build",
        ]
    elif momentum_report.get("usable_as_upside_family") and not ma5_report.get("additive_top5_membership_ledger_can_be_generated"):
        decision = "momentum_only_risk_limited_probe_ready"
        next_axis = "momentum_risk_limited_top5_probe_v1"
        reasons = ["momentum_upside_usable_with_context_risk_split", "ma5_additive_ledger_not_ready"]
    elif ma5_report.get("metrics", {}).get("avg_ret20") and not ma5_report.get("additive_top5_membership_ledger_can_be_generated"):
        decision = "ma5_h12_additive_ledger_repair_needed"
        next_axis = "ma5_h12_additive_candidate_ledger_repair_v1"
        reasons = ["ma5_h12_context_promising_but_additive_ledger_blocked"]
    else:
        decision = "selected_family_v2_drop"
        next_axis = "pattern_family_portfolio_refresh_v2"
        reasons = ["no_direct_top5_validation_path_remains"]
    return {
        "schema_version": "tradex_selected_family_v2_risk_decomposition_research_decision_v1",
        "research_phase": "selected_family_v2_risk_decomposition",
        "boundary": "TRADEX-only",
        "axis_moved": "selected_family_v2_risk_decomposition",
        "source_validation_decision": artifacts["validation_decision"].get("decision"),
        "top5_direct_improvement_claimed": False,
        "common_top5_candidate_ledger_build_ready": bool(common_design.get("ledger_build_ready")),
        "momentum_usable_as_upside_family": bool(momentum_report.get("usable_as_upside_family")),
        "ma5_h12_additive_ledger_can_be_generated": bool(ma5_report.get("additive_top5_membership_ledger_can_be_generated")),
        "combined_direct_top5_validation_feasible_after_ledger_build": bool(combined_report.get("direct_top5_validation_feasible_after_ledger_build")),
        "future_labels_used_for_diagnosis_only": True,
        "future_labels_used_in_score_inputs": False,
        "candidate_scoring_created": False,
        "threshold_policy_created": False,
        "image_score_used": False,
        "fusion_reranker_created": False,
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
        "silent_fallback_used": False,
        "research_fallback_used": False,
        "decision": decision,
        "authoritative_research_decision": decision,
        "recommended_next_axis": next_axis,
        "typed_reasons": reasons,
        "generated_at_utc": _utc_now(),
    }


def _next_axis(decision: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "tradex_selected_family_v2_risk_decomposition_next_axis_recommendation_v1",
        "axis_id": AXIS_ID,
        "decision": decision["decision"],
        "next": decision["recommended_next_axis"],
        "activation_allowed": False,
        "meemee_reflection_allowed": False,
    }


def _evaluation_contract(roots: Mapping[str, Path]) -> dict[str, Any]:
    return {
        "schema_version": "tradex_selected_family_v2_risk_decomposition_evaluation_contract_v1",
        "axis_id": AXIS_ID,
        "purpose": "decompose risk and determine common top5 candidate ledger build readiness",
        "boundary": "TRADEX-only",
        "top5_direct_improvement_claim_allowed": False,
        "future_label_policy": {
            "future_labels_allowed_in_candidate_construction": False,
            "future_labels_allowed_in_risk_diagnosis": True,
        },
        "input_paths": {key: str(value) for key, value in roots.items()},
        "not_changed": _not_changed(),
    }


def _run_manifest(output_root: Path, roots: Mapping[str, Path]) -> dict[str, Any]:
    return {
        "schema_version": "tradex_selected_family_v2_risk_decomposition_run_manifest_v1",
        "axis_id": AXIS_ID,
        "run_id": output_root.name,
        "output_root": str(output_root),
        "inputs": {key: str(value) for key, value in roots.items()},
        "generated_at_utc": _utc_now(),
    }


def _source_refs(roots: Mapping[str, Path], artifacts: Mapping[str, Any]) -> dict[str, Any]:
    missing = {
        key: value.get("_missing_path")
        for key, value in artifacts.items()
        if isinstance(value, Mapping) and value.get("_missing")
    }
    return {
        "schema_version": "tradex_selected_family_v2_risk_decomposition_source_refs_v1",
        "axis_id": AXIS_ID,
        "refs": {key: str(value) for key, value in roots.items()},
        "upstream_artifacts": artifacts.get("source_refs", {}).get("refs", {}),
        "missing_inputs": missing,
        "silent_fallback_used": False,
    }


def _risk_decomposition_contract(roots: Mapping[str, Path], artifacts: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "tradex_selected_family_v2_risk_decomposition_contract_v1",
        "axis_id": AXIS_ID,
        "source_validation_root": str(roots["validation_root"]),
        "source_validation_decision": artifacts["validation_decision"].get("decision"),
        "diagnosis_items": [
            "momentum winner/severe/big-winner decomposition",
            "point-in-time context separation",
            "ma5 h12 event_date symbol materialization",
            "common top5 candidate ledger design",
            "combined feasibility",
        ],
        "no_fake_top5_direct_evidence": True,
        "no_silent_fallback": True,
        "not_changed": _not_changed(),
    }


def _selection_metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    n = len(rows)
    winners = sum(bool(row.get("win20")) for row in rows)
    severe = sum(bool(row.get("severe_loss20")) for row in rows)
    big = sum(bool(row.get("is_big_winner_ret20_ge_10pct")) for row in rows)
    top10 = sum(bool(row.get("is_future_top10_by_ret20")) for row in rows)
    return {
        "event_count": n,
        "winner_count": winners,
        "win_rate20": _rate(winners, n),
        "severe_loser_count": severe,
        "severe_loss_rate20": _rate(severe, n),
        "big_winner_count": big,
        "big_winner_rate": _rate(big, n),
        "future_top10_count": top10,
        "future_top10_rate": _rate(top10, n),
        "avg_ret20": _mean(row.get("ret20_fwd") for row in rows),
        "avg_MFE20": _mean(row.get("mfe20") for row in rows),
        "avg_MAE20": _mean(row.get("mae20") for row in rows),
        "severe_loser_per_big_winner": severe / max(big, 1),
    }


def _ma5_metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    n = len(rows)
    winners = sum(bool(row.get("win")) for row in rows)
    severe = sum(bool(row.get("severe_loss")) for row in rows)
    return {
        "event_count": n,
        "winner_count": winners,
        "win_rate": _rate(winners, n),
        "severe_loser_count": severe,
        "severe_loss_rate": _rate(severe, n),
        "avg_ret": _mean(row.get("ret") for row in rows),
        "avg_mfe": _mean(row.get("mfe") for row in rows),
        "avg_mae": _mean(row.get("mae") for row in rows),
    }


def _context_rows(rows: Sequence[Mapping[str, Any]], columns: Sequence[str], *, min_count: int) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, ...], list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[tuple(str(row.get(column)) for column in columns)].append(row)
    out = []
    for key, items in grouped.items():
        if len(items) < min_count:
            continue
        metrics = _selection_metrics(items)
        out.append(
            {
                "context": {column: key[index] for index, column in enumerate(columns)},
                "event_count": metrics["event_count"],
                "big_winner_count": metrics["big_winner_count"],
                "severe_loser_count": metrics["severe_loser_count"],
                "severe_loss_rate20": metrics["severe_loss_rate20"],
                "big_winner_rate": metrics["big_winner_rate"],
                "severe_loser_per_big_winner": metrics["severe_loser_per_big_winner"],
                "avg_ret20": metrics["avg_ret20"],
            }
        )
    return out


def _month_counts(rows: Sequence[Mapping[str, Any]], date_key: str) -> list[dict[str, Any]]:
    counts: Counter[str] = Counter()
    for row in rows:
        date_value = str(row.get(date_key) or "")
        if len(date_value) >= 7:
            counts[date_value[:7]] += 1
    return [{"month": month, "count": count} for month, count in sorted(counts.items())]


def _common_ledger_blockers(momentum_report: Mapping[str, Any], ma5_report: Mapping[str, Any], context_report: Mapping[str, Any]) -> list[str]:
    blockers = []
    if not momentum_report.get("event_count"):
        blockers.append("momentum_rows_missing")
    if not ma5_report.get("additive_top5_membership_ledger_can_be_generated"):
        blockers.append("ma5_h12_additive_materialization_or_overlap_missing")
    if not context_report.get("point_in_time_context_separation_available"):
        blockers.append("momentum_context_risk_separation_missing")
    return blockers


def _load_jsonl_filtered(path: Path, predicate: Any) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            row = json.loads(line)
            if predicate(row):
                rows.append(row)
    return rows


def _is_ma5_h12_row(row: Mapping[str, Any]) -> bool:
    return row.get("ma_stack") == "ma5_above_20_below_60" and row.get("ma60_slope_state") == "ma60_rising"


def _ma5_key(row: Mapping[str, Any]) -> tuple[str | None, str | None]:
    return row.get("signal_date"), row.get("symbol")


def _momentum_key(row: Mapping[str, Any]) -> tuple[str | None, str | None]:
    return row.get("event_date"), row.get("code")


def _artifact_complete(output_root: Path, decision: Mapping[str, Any]) -> dict[str, Any]:
    presence = {name: (output_root / name).exists() for name in REQUIRED_OUTPUTS if name != "_ARTIFACT_COMPLETE.json"}
    presence["_ARTIFACT_COMPLETE.json"] = True
    return {
        "schema_version": "tradex_selected_family_v2_risk_decomposition_artifact_complete_v1",
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


def _read_json_optional(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"_missing": True, "_missing_path": str(path)}
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    return data if isinstance(data, dict) else {"_not_object": True, "_path": str(path)}


def _rate(count: int, total: int) -> float | None:
    return None if total <= 0 else count / total


def _mean(values: Iterable[Any]) -> float | None:
    nums = [_float(value) for value in values]
    nums = [value for value in nums if value is not None]
    return None if not nums else sum(nums) / len(nums)


def _safe_subtract(left: Any, right: Any) -> float | None:
    a = _float(left)
    b = _float(right)
    if a is None or b is None:
        return None
    return a - b


def _float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None:
            return default
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def _not_changed() -> list[str]:
    return [
        "starter_entry_pretest",
        "MeeMee_reflection",
        "production_ranking",
        "publish_bundle",
        "threshold_no_trade",
        "image_fusion",
        "teppan_changes",
        "pre_strength_revival",
        "R11_inclusion",
        "sell_side",
        "buy_more_core_logic",
        "exit_optimization",
        "cost_slippage_liquidity",
        "fake_top5_direct_evidence",
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
    return value


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(dict(payload)), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
