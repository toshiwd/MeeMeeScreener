"""TRADEX-only validation for refreshed non-pre-strength pattern families."""

from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path
from typing import Any, Mapping, Sequence


AXIS_ID = "selected_pattern_family_validation_v2"
DEFAULT_RUN_ID = "20260514T200000Z-selected-pattern-family-validation-v2"
DEFAULT_OUTPUT_PARENT = Path(r"G:\Tradex\selected_pattern_family_validation_v2")
DEFAULT_PORTFOLIO_ROOT = Path(
    r"G:\Tradex\pattern_family_portfolio_refresh_v1"
    r"\20260514T190000Z-pattern-family-portfolio-refresh-v1"
)
DEFAULT_WIDE_STRENGTH_ROOT = Path(
    r"G:\Tradex\wide_strength_pool_upside_rerank_v1"
    r"\20260513T030000Z-wide-strength-pool-upside-rerank-v1"
)
DEFAULT_MA5_ROOT = Path(
    r"G:\Tradex\ma5_reclaim_hypothesis_batch_v1"
    r"\20260512T000000Z-ma5-reclaim-hypothesis-batch-v1-ma5_reclaim_hypothesis_batch_v1"
)
DEFAULT_MA5_TRADE_LEDGER = Path(
    r"G:\Tradex\ma5_reclaim_ma20_exit_probe_v1"
    r"\20260512T000000Z-ma5-reclaim-ma20-exit-probe-v1-ma5_reclaim_ma20_exit_probe_v1"
    r"\trade_ledger.jsonl"
)

REQUIRED_OUTPUTS = [
    "evaluation_contract.json",
    "run_manifest.json",
    "source_artifact_refs.json",
    "selected_family_validation_contract_v2.json",
    "family_variant_leaderboard.json",
    "top5_candidate_pool_report.json",
    "family_contribution_report.json",
    "family_overlap_report.json",
    "momentum_risk_profile_report.json",
    "ma5_reclaim_context_report.json",
    "combined_family_report.json",
    "guardrail_report.json",
    "human_selectable_day_report.json",
    "next_axis_recommendation.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--portfolio-root", type=Path, default=DEFAULT_PORTFOLIO_ROOT)
    parser.add_argument("--wide-strength-root", type=Path, default=DEFAULT_WIDE_STRENGTH_ROOT)
    parser.add_argument("--ma5-root", type=Path, default=DEFAULT_MA5_ROOT)
    parser.add_argument("--ma5-trade-ledger", type=Path, default=DEFAULT_MA5_TRADE_LEDGER)
    parser.add_argument("--output-parent", type=Path, default=DEFAULT_OUTPUT_PARENT)
    parser.add_argument("--run-id", default=DEFAULT_RUN_ID)
    args = parser.parse_args()
    run_selected_pattern_family_validation_v2(
        portfolio_root=args.portfolio_root,
        wide_strength_root=args.wide_strength_root,
        ma5_root=args.ma5_root,
        ma5_trade_ledger=args.ma5_trade_ledger,
        output_parent=args.output_parent,
        run_id=args.run_id,
    )
    return 0


def run_selected_pattern_family_validation_v2(
    *,
    portfolio_root: Path = DEFAULT_PORTFOLIO_ROOT,
    wide_strength_root: Path = DEFAULT_WIDE_STRENGTH_ROOT,
    ma5_root: Path = DEFAULT_MA5_ROOT,
    ma5_trade_ledger: Path = DEFAULT_MA5_TRADE_LEDGER,
    output_parent: Path = DEFAULT_OUTPUT_PARENT,
    run_id: str = DEFAULT_RUN_ID,
) -> dict[str, Any]:
    output_root = output_parent / run_id
    output_root.mkdir(parents=True, exist_ok=True)
    roots = {
        "portfolio_root": portfolio_root,
        "wide_strength_root": wide_strength_root,
        "ma5_root": ma5_root,
        "ma5_trade_ledger": ma5_trade_ledger,
    }
    artifacts = _load_artifacts(roots)
    selected = artifacts["selected_families"].get("selected_pattern_families") or []
    contract = _validation_contract(roots, artifacts, selected)
    momentum = _momentum_report(artifacts, roots["wide_strength_root"])
    ma5 = _ma5_report(artifacts, roots["ma5_root"], roots["ma5_trade_ledger"])
    combined = _combined_report(momentum, ma5)
    leaderboard = _variant_leaderboard(momentum, ma5, combined)
    top5 = _top5_candidate_pool_report(momentum, ma5, combined)
    contribution = _family_contribution_report(momentum, ma5, combined)
    overlap = _family_overlap_report(selected)
    guardrail = _guardrail_report(momentum, ma5, combined)
    human = _human_selectable_day_report(momentum, ma5, combined)
    decision = _research_decision(leaderboard, top5, guardrail, artifacts)
    next_axis = _next_axis(decision)
    payloads = {
        "evaluation_contract.json": _evaluation_contract(roots),
        "run_manifest.json": _run_manifest(output_root, roots),
        "source_artifact_refs.json": _source_refs(roots, artifacts),
        "selected_family_validation_contract_v2.json": contract,
        "family_variant_leaderboard.json": leaderboard,
        "top5_candidate_pool_report.json": top5,
        "family_contribution_report.json": contribution,
        "family_overlap_report.json": overlap,
        "momentum_risk_profile_report.json": momentum,
        "ma5_reclaim_context_report.json": ma5,
        "combined_family_report.json": combined,
        "guardrail_report.json": guardrail,
        "human_selectable_day_report.json": human,
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
        "selected_family_validation_contract_v2": contract,
        "family_variant_leaderboard": leaderboard,
        "top5_candidate_pool_report": top5,
        "family_contribution_report": contribution,
        "family_overlap_report": overlap,
        "momentum_risk_profile_report": momentum,
        "ma5_reclaim_context_report": ma5,
        "combined_family_report": combined,
        "guardrail_report": guardrail,
        "human_selectable_day_report": human,
        "next_axis_recommendation": next_axis,
        "research_decision": decision,
        "artifact_complete": complete,
    }


def _load_artifacts(roots: Mapping[str, Path]) -> dict[str, Any]:
    return {
        "portfolio_decision": _read_json_optional(roots["portfolio_root"] / "research_decision.json"),
        "selected_families": _read_json_optional(roots["portfolio_root"] / "selected_pattern_families_for_validation.json"),
        "portfolio_complete": _read_json_optional(roots["portfolio_root"] / "_ARTIFACT_COMPLETE.json"),
        "wide_decision": _read_json_optional(roots["wide_strength_root"] / "research_decision.json"),
        "wide_leaderboard": _read_json_optional(roots["wide_strength_root"] / "score_leaderboard.json"),
        "wide_feature_audit": _read_json_optional(roots["wide_strength_root"] / "feature_availability_audit.json"),
        "ma5_decision": _read_json_optional(roots["ma5_root"] / "research_decision.json"),
        "ma5_leaderboard": _read_json_optional(roots["ma5_root"] / "hypothesis_leaderboard.json"),
        "ma5_feature_audit": _read_json_optional(roots["ma5_root"] / "feature_availability_audit.json"),
        "ma5_trade_ledger_sample": _read_trade_ledger_summary(roots["ma5_trade_ledger"]),
    }


def _momentum_report(artifacts: Mapping[str, Any], root: Path) -> dict[str, Any]:
    rows = artifacts["wide_leaderboard"].get("rows") or []
    by_id = {row.get("family_id"): row for row in rows if isinstance(row, Mapping)}
    momentum = by_id.get("momentum_continuation_soft_boost_v1") or {}
    random = by_id.get("all_strength_scoreless_random_top3") or {}
    oracle = by_id.get("all_strength_oracle_top3") or {}
    deltas = {
        "top3_avg_ret20_delta_vs_random": _delta(momentum, random, "selected_top3_avg_ret20"),
        "top3_big_winner_capture_delta_vs_random": _delta(momentum, random, "selected_top3_big_winner_ret20_ge_10_capture_rate"),
        "top3_future_top10_precision_delta_vs_random": _delta(momentum, random, "selected_top3_future_top10_precision"),
        "top3_severe_loss_rate_delta_vs_random": _delta(momentum, random, "selected_top3_severe_loss_rate20"),
        "selected_nonwinner_when_winner_available_delta_vs_random": _delta(momentum, random, "selected_nonwinner_when_winner_available_rate"),
    }
    risk_flags = []
    if _float(momentum.get("selected_top3_severe_loss_rate20"), 1.0) > _float(random.get("selected_top3_severe_loss_rate20"), 0.0):
        risk_flags.append("severe_loss_worse_than_random")
    if _float(momentum.get("selected_nonwinner_when_winner_available_rate"), 1.0) > _float(random.get("selected_nonwinner_when_winner_available_rate"), 0.0):
        risk_flags.append("nonwinner_when_winner_available_worse_than_random")
    if not momentum:
        risk_flags.append("source_metrics_missing")
    verdict = "hold_risk_decomposition_required" if risk_flags else "keep_candidate_proxy_only"
    return {
        "schema_version": "tradex_selected_pattern_family_v2_momentum_risk_profile_report_v1",
        "axis_id": AXIS_ID,
        "family_id": "momentum_continuation_soft_boost_v1",
        "source_artifact": str(root / "score_leaderboard.json"),
        "source_observability": "top3_selection_ledger_only",
        "top5_direct_observable": False,
        "metrics": _metrics_subset(momentum),
        "baseline_random_metrics": _metrics_subset(random),
        "oracle_metrics": _metrics_subset(oracle),
        "delta_vs_random": deltas,
        "risk_flags": risk_flags,
        "family_verdict": verdict,
        "typed_reasons": [
            "upside_capture_evidence_exists",
            "top5_membership_not_directly_observed_in_source",
            "risk_decomposition_needed_before_candidate_pool_keep",
        ],
    }


def _ma5_report(artifacts: Mapping[str, Any], root: Path, trade_ledger: Path) -> dict[str, Any]:
    rows = artifacts["ma5_leaderboard"].get("rows") or []
    if not rows:
        decision = artifacts["ma5_decision"]
        rows = (decision.get("excellent_hypotheses") or []) + (decision.get("promising_hypotheses") or [])
    by_id = {row.get("hypothesis_id"): row for row in rows if isinstance(row, Mapping)}
    h12 = by_id.get("h12_near_bull_ma60_rising") or {}
    base = by_id.get("h00_base_all") or {}
    deltas = {
        "avg_ret_delta_vs_base": _delta(h12, base, "avg_ret"),
        "win_rate_delta_vs_base": _delta(h12, base, "win_rate"),
        "severe_loss_rate_delta_vs_base": _delta(h12, base, "severe_loss_rate"),
        "profit_factor_delta_vs_base": _delta(h12, base, "profit_factor"),
    }
    blockers = []
    if not artifacts["ma5_trade_ledger_sample"].get("has_symbol_date_rows"):
        blockers.append("source_trade_ledger_missing_or_empty")
    if not h12:
        blockers.append("h12_metrics_missing")
    if h12.get("reentry_expansion_modelled") or h12.get("reentry_expansion_modeled"):
        pass
    else:
        blockers.append("reentry_expansion_not_modeled")
    blockers.append("no_top5_membership_ledger_for_additive_candidate_generation")
    verdict = "hold_additive_candidate_generation_gap" if blockers else "keep_candidate"
    return {
        "schema_version": "tradex_selected_pattern_family_v2_ma5_reclaim_context_report_v1",
        "axis_id": AXIS_ID,
        "family_id": "ma5_reclaim_context::h12_near_bull_ma60_rising",
        "source_artifact": str(root / "hypothesis_leaderboard.json"),
        "source_trade_ledger": str(trade_ledger),
        "source_observability": "aggregate_hypothesis_metrics_plus_trade_ledger_no_top5_membership",
        "top5_direct_observable": False,
        "metrics": {
            "trade_count": _int(h12.get("trade_count")),
            "symbol_count": _int(h12.get("symbol_count")),
            "avg_ret20": _float(h12.get("avg_ret")),
            "win_rate20": _float(h12.get("win_rate")),
            "severe_loss_rate20": _float(h12.get("severe_loss_rate")),
            "avg_MFE20": _float(h12.get("avg_mfe")),
            "avg_MAE20": _float(h12.get("avg_mae")),
            "profit_factor20": _float(h12.get("profit_factor")),
        },
        "baseline_h00_metrics": {
            "trade_count": _int(base.get("trade_count")),
            "avg_ret20": _float(base.get("avg_ret")),
            "win_rate20": _float(base.get("win_rate")),
            "severe_loss_rate20": _float(base.get("severe_loss_rate")),
            "profit_factor20": _float(base.get("profit_factor")),
        },
        "delta_vs_h00_base_all": deltas,
        "trade_ledger_summary": artifacts["ma5_trade_ledger_sample"],
        "blockers": blockers,
        "family_verdict": verdict,
        "typed_reasons": [
            "context_filter_quality_positive",
            "low_absolute_severe_loss",
            "additive_candidate_generation_not_proven",
            "top5_pool_membership_not_observed",
        ],
    }


def _combined_report(momentum: Mapping[str, Any], ma5: Mapping[str, Any]) -> dict[str, Any]:
    blockers = []
    if not momentum.get("top5_direct_observable"):
        blockers.append("momentum_top5_direct_membership_missing")
    if not ma5.get("top5_direct_observable"):
        blockers.append("ma5_top5_direct_membership_missing")
    blockers.append("no_common_same_date_candidate_score_space_between_sources")
    return {
        "schema_version": "tradex_selected_pattern_family_v2_combined_family_report_v1",
        "axis_id": AXIS_ID,
        "variant_id": "combined_selected_families",
        "family_ids": [momentum.get("family_id"), ma5.get("family_id")],
        "combined_evaluable": False,
        "combined_top5_direct_observable": False,
        "combination_blockers": blockers,
        "overlap_assessment": "mechanism_overlap_low_but_runtime_score_space_not_shared",
        "family_verdict": "hold_combined_requires_common_candidate_ledger",
        "typed_reasons": [
            "families_are_mechanistically_complementary",
            "combined_top5_cannot_be_claimed_without_common_ledger",
            "no_silent_fallback",
        ],
    }


def _variant_leaderboard(momentum: Mapping[str, Any], ma5: Mapping[str, Any], combined: Mapping[str, Any]) -> dict[str, Any]:
    rows = [
        _variant_row(
            "momentum_continuation_soft_boost_v1_only",
            "momentum_continuation_soft_boost_v1",
            momentum.get("family_verdict"),
            momentum.get("metrics") or {},
            momentum.get("delta_vs_random") or {},
            momentum.get("top5_direct_observable"),
        ),
        _variant_row(
            "ma5_reclaim_context_h12_only",
            "ma5_reclaim_context::h12_near_bull_ma60_rising",
            ma5.get("family_verdict"),
            ma5.get("metrics") or {},
            ma5.get("delta_vs_h00_base_all") or {},
            ma5.get("top5_direct_observable"),
        ),
        {
            "variant_id": "combined_selected_families",
            "family_id": "combined_selected_families",
            "variant_decision": combined.get("family_verdict"),
            "top5_direct_observable": False,
            "primary_blockers": combined.get("combination_blockers"),
            "rank": 3,
        },
    ]
    return {"schema_version": "tradex_selected_pattern_family_v2_variant_leaderboard_v1", "axis_id": AXIS_ID, "rows": rows}


def _top5_candidate_pool_report(momentum: Mapping[str, Any], ma5: Mapping[str, Any], combined: Mapping[str, Any]) -> dict[str, Any]:
    rows = []
    rows.append(
        {
            "variant_id": "momentum_continuation_soft_boost_v1_only",
            "top5_observability_status": "source_top3_only_top5_not_directly_observable",
            "top5_avg_ret20": None,
            "top5_win_rate20": None,
            "top5_big_winner_capture_rate": None,
            "top5_future_top10_capture_rate": None,
            "top5_severe_loss_rate20": None,
            "top5_bad_pick_count": None,
            "top5_candidate_diversity": None,
            "human_selectable_day_rate": None,
            "proxy_top3_metrics": momentum.get("metrics"),
            "decision": "hold_proxy_not_top5",
        }
    )
    rows.append(
        {
            "variant_id": "ma5_reclaim_context_h12_only",
            "top5_observability_status": "aggregate_trade_filter_only_no_top5_membership",
            "top5_avg_ret20": None,
            "top5_win_rate20": None,
            "top5_big_winner_capture_rate": None,
            "top5_future_top10_capture_rate": None,
            "top5_severe_loss_rate20": None,
            "top5_bad_pick_count": None,
            "top5_candidate_diversity": None,
            "human_selectable_day_rate": None,
            "aggregate_context_metrics": ma5.get("metrics"),
            "decision": "hold_additive_generation_gap",
        }
    )
    rows.append(
        {
            "variant_id": "combined_selected_families",
            "top5_observability_status": "not_evaluable_without_common_candidate_ledger",
            "top5_avg_ret20": None,
            "top5_win_rate20": None,
            "top5_big_winner_capture_rate": None,
            "top5_future_top10_capture_rate": None,
            "top5_severe_loss_rate20": None,
            "top5_bad_pick_count": None,
            "top5_candidate_diversity": None,
            "human_selectable_day_rate": None,
            "decision": combined.get("family_verdict"),
        }
    )
    return {
        "schema_version": "tradex_selected_pattern_family_v2_top5_candidate_pool_report_v1",
        "axis_id": AXIS_ID,
        "primary_metric_scope": "top5_candidate_pool_quality",
        "top5_direct_comparison_available": False,
        "no_silent_fallback": True,
        "rows": rows,
    }


def _family_contribution_report(momentum: Mapping[str, Any], ma5: Mapping[str, Any], combined: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "tradex_selected_pattern_family_v2_family_contribution_report_v1",
        "axis_id": AXIS_ID,
        "rows": [
            {"family_id": momentum.get("family_id"), "contribution_role": "upside_candidate_supply", "verdict": momentum.get("family_verdict")},
            {"family_id": ma5.get("family_id"), "contribution_role": "low_severe_context_filter", "verdict": ma5.get("family_verdict")},
            {"family_id": "combined_selected_families", "contribution_role": "breadth_complementarity_unproven", "verdict": combined.get("family_verdict")},
        ],
    }


def _family_overlap_report(selected: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    pairs = []
    for left, right in combinations(selected, 2):
        lt, rt = set(left.get("overlap_tags") or []), set(right.get("overlap_tags") or [])
        union = lt | rt
        jaccard = 0.0 if not union else len(lt & rt) / len(union)
        pairs.append(
            {
                "left_family_id": left.get("family_id"),
                "right_family_id": right.get("family_id"),
                "overlap_jaccard": jaccard,
                "overlap_level": "high" if jaccard >= 0.5 else "medium" if jaccard >= 0.25 else "low",
                "shared_tags": sorted(lt & rt),
            }
        )
    return {
        "schema_version": "tradex_selected_pattern_family_v2_overlap_report_v1",
        "axis_id": AXIS_ID,
        "pair_count": len(pairs),
        "pairs": pairs,
        "overlap_assessment": "acceptable_low_overlap" if all(row["overlap_level"] == "low" for row in pairs) else "needs_deduplication",
    }


def _guardrail_report(momentum: Mapping[str, Any], ma5: Mapping[str, Any], combined: Mapping[str, Any]) -> dict[str, Any]:
    m = momentum.get("metrics") or {}
    oracle = momentum.get("oracle_metrics") or {}
    return {
        "schema_version": "tradex_selected_pattern_family_v2_guardrail_report_v1",
        "axis_id": AXIS_ID,
        "rows": [
            {
                "variant_id": "momentum_continuation_soft_boost_v1_only",
                "top3_avg_ret20": m.get("selected_top3_avg_ret20"),
                "top3_severe_loss_rate20": m.get("selected_top3_severe_loss_rate20"),
                "oracle_top3_gap": _safe_subtract(oracle.get("selected_top3_avg_ret20"), m.get("selected_top3_avg_ret20")),
                "selected_nonwinner_when_winner_available": m.get("selected_nonwinner_when_winner_available_rate"),
                "guardrail_status": momentum.get("family_verdict"),
            },
            {
                "variant_id": "ma5_reclaim_context_h12_only",
                "top3_avg_ret20": None,
                "top3_severe_loss_rate20": None,
                "oracle_top3_gap": None,
                "selected_nonwinner_when_winner_available": None,
                "guardrail_status": "not_observable_no_top3_membership",
            },
            {
                "variant_id": "combined_selected_families",
                "top3_avg_ret20": None,
                "top3_severe_loss_rate20": None,
                "oracle_top3_gap": None,
                "selected_nonwinner_when_winner_available": None,
                "guardrail_status": combined.get("family_verdict"),
            },
        ],
    }


def _human_selectable_day_report(momentum: Mapping[str, Any], ma5: Mapping[str, Any], combined: Mapping[str, Any]) -> dict[str, Any]:
    m = momentum.get("metrics") or {}
    return {
        "schema_version": "tradex_selected_pattern_family_v2_human_selectable_day_report_v1",
        "axis_id": AXIS_ID,
        "rows": [
            {
                "variant_id": "momentum_continuation_soft_boost_v1_only",
                "human_selectable_day_rate": None,
                "proxy_selected_on_opportunity_days": m.get("selected_on_opportunity_days"),
                "proxy_average_candidates_per_day": m.get("average_candidates_per_day"),
                "status": "proxy_only_source_top3_not_top5",
            },
            {
                "variant_id": "ma5_reclaim_context_h12_only",
                "human_selectable_day_rate": None,
                "proxy_trade_count": (ma5.get("metrics") or {}).get("trade_count"),
                "status": "not_observable_without_candidate_pool_generation",
            },
            {
                "variant_id": "combined_selected_families",
                "human_selectable_day_rate": None,
                "status": combined.get("family_verdict"),
            },
        ],
    }


def _research_decision(
    leaderboard: Mapping[str, Any],
    top5: Mapping[str, Any],
    guardrail: Mapping[str, Any],
    artifacts: Mapping[str, Any],
) -> dict[str, Any]:
    rows = leaderboard.get("rows") or []
    keep_rows = [row for row in rows if row.get("variant_decision") == "keep_candidate"]
    hold_rows = [row for row in rows if str(row.get("variant_decision") or "").startswith("hold")]
    if keep_rows and top5.get("top5_direct_comparison_available"):
        decision = "keep_candidate"
        auth = "selected_pattern_family_validation_v2_keep_candidate"
        next_axis = "starter_entry_candidate_pretest_v1"
        reasons = ["top5_direct_validation_passed"]
    elif hold_rows:
        decision = "hold"
        auth = "selected_pattern_family_validation_v2_hold"
        next_axis = "selected_family_v2_risk_decomposition_v1"
        reasons = [
            "evidence_exists_but_top5_direct_comparison_unavailable",
            "momentum_requires_risk_decomposition",
            "ma5_requires_additive_candidate_generation_ledger",
            "combined_requires_common_candidate_score_space",
        ]
    else:
        decision = "drop"
        auth = "selected_pattern_family_validation_v2_drop"
        next_axis = "pattern_family_portfolio_refresh_v2"
        reasons = ["no_family_has_enough_evidence_for_candidate_pool_validation"]
    return {
        "schema_version": "tradex_selected_pattern_family_validation_v2_research_decision_v1",
        "research_phase": "selected_pattern_family_validation_v2",
        "boundary": "TRADEX-only",
        "source_portfolio_decision": artifacts["portfolio_decision"].get("decision"),
        "source_selected_family_count": artifacts["selected_families"].get("selection_count"),
        "decision": decision,
        "authoritative_research_decision": auth,
        "recommended_next_axis": next_axis,
        "keep_variant_count": len(keep_rows),
        "hold_variant_count": len(hold_rows),
        "drop_variant_count": len(rows) - len(keep_rows) - len(hold_rows),
        "top5_direct_comparison_available": bool(top5.get("top5_direct_comparison_available")),
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
        "typed_reasons": reasons,
        "generated_at_utc": _utc_now(),
    }


def _next_axis(decision: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "tradex_selected_pattern_family_validation_v2_next_axis_recommendation_v1",
        "axis_id": AXIS_ID,
        "decision": decision["decision"],
        "authoritative_research_decision": decision["authoritative_research_decision"],
        "next": decision["recommended_next_axis"],
        "activation_allowed": False,
        "meemee_reflection_allowed": False,
    }


def _validation_contract(roots: Mapping[str, Path], artifacts: Mapping[str, Any], selected: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": "tradex_selected_pattern_family_validation_v2_contract_v1",
        "axis_id": AXIS_ID,
        "boundary": "TRADEX-only validation",
        "selected_family_ids": [row.get("family_id") for row in selected],
        "variants": [
            "momentum_continuation_soft_boost_v1_only",
            "ma5_reclaim_context_h12_only",
            "combined_selected_families",
        ],
        "input_observability": {
            "momentum_source": "source top3 leaderboard/selection ledger, not full top5 membership",
            "ma5_source": "aggregate hypothesis metrics plus base trade ledger, not additive top5 membership",
            "combined_source": "no shared same-date score space",
        },
        "future_label_policy": {
            "future_labels_used_for_diagnosis_only": True,
            "future_labels_used_in_score_inputs": False,
        },
        "not_changed": _not_changed(),
        "silent_fallback_used": False,
    }


def _evaluation_contract(roots: Mapping[str, Path]) -> dict[str, Any]:
    return {
        "schema_version": "tradex_selected_pattern_family_validation_v2_evaluation_contract_v1",
        "axis_id": AXIS_ID,
        "purpose": "validate refreshed families for top5-ish human max3 buy candidate pool",
        "fixed_conditions": {
            "same_universe": "source_artifact_native; no cross-source fabricated merge",
            "same_period": "source_artifact_native",
            "same_topK": "not directly shared; top5 direct unavailable is reported as blocker",
            "same_regime_condition": "source_artifact_native",
            "same_cost_slippage": "no_new_cost_slippage_added",
            "same_artifact_detail_level": "JSON_authoritative",
        },
        "primary_evaluation": "top5_candidate_pool_quality",
        "top5_direct_fallback_allowed": False,
        "do_not_change": _not_changed(),
    }


def _run_manifest(output_root: Path, roots: Mapping[str, Path]) -> dict[str, Any]:
    return {
        "schema_version": "tradex_selected_pattern_family_validation_v2_run_manifest_v1",
        "axis_id": AXIS_ID,
        "run_id": output_root.name,
        "output_root": str(output_root),
        "inputs": {key: str(value) for key, value in roots.items()},
        "generated_at_utc": _utc_now(),
    }


def _source_refs(roots: Mapping[str, Path], artifacts: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "tradex_selected_pattern_family_validation_v2_source_refs_v1",
        "axis_id": AXIS_ID,
        "refs": {key: str(value) for key, value in roots.items()},
        "missing_inputs": {
            key: value.get("_missing_path")
            for key, value in artifacts.items()
            if isinstance(value, Mapping) and value.get("_missing")
        },
        "silent_fallback_used": False,
    }


def _variant_row(
    variant_id: str,
    family_id: str,
    verdict: Any,
    metrics: Mapping[str, Any],
    deltas: Mapping[str, Any],
    top5_direct_observable: Any,
) -> dict[str, Any]:
    return {
        "variant_id": variant_id,
        "family_id": family_id,
        "variant_decision": verdict,
        "top5_direct_observable": bool(top5_direct_observable),
        "metrics": metrics,
        "deltas": deltas,
    }


def _metrics_subset(row: Mapping[str, Any]) -> dict[str, Any]:
    keys = [
        "average_candidates_per_day",
        "selected_day_count",
        "selected_event_count",
        "selected_on_opportunity_days",
        "selected_top3_avg_ret20",
        "selected_top3_win_rate20",
        "selected_top3_big_winner_ret20_ge_10_capture_rate",
        "selected_top3_future_top10_precision",
        "selected_top3_severe_loss_rate20",
        "selected_nonwinner_when_winner_available_rate",
    ]
    return {key: _float(row.get(key)) for key in keys if key in row}


def _read_trade_ledger_summary(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "exists": False, "has_symbol_date_rows": False}
    line_count = 0
    symbols = set()
    dates = set()
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            line_count += 1
            if line_count <= 5000:
                row = json.loads(line)
                symbols.add(row.get("symbol"))
                dates.add(row.get("entry_date") or row.get("signal_date"))
    return {
        "path": str(path),
        "exists": True,
        "line_count": line_count,
        "sample_symbol_count": len(symbols),
        "sample_date_count": len(dates),
        "has_symbol_date_rows": line_count > 0,
    }


def _artifact_complete(output_root: Path, decision: Mapping[str, Any]) -> dict[str, Any]:
    presence = {name: (output_root / name).exists() for name in REQUIRED_OUTPUTS if name != "_ARTIFACT_COMPLETE.json"}
    presence["_ARTIFACT_COMPLETE.json"] = True
    return {
        "schema_version": "tradex_selected_pattern_family_validation_v2_artifact_complete_v1",
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


def _delta(left: Mapping[str, Any], right: Mapping[str, Any], key: str) -> float | None:
    return _safe_subtract(_float(left.get(key)), _float(right.get(key)))


def _safe_subtract(left: Any, right: Any) -> float | None:
    a = _float(left)
    b = _float(right)
    if a is None or b is None:
        return None
    return a - b


def _read_json_optional(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"_missing": True, "_missing_path": str(path)}
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    return data if isinstance(data, dict) else {"_not_object": True, "_path": str(path)}


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


def _not_changed() -> list[str]:
    return [
        "MeeMee_runtime",
        "active_ranking",
        "display_score",
        "runtime_DuckDB",
        "production_registry",
        "publish_bundle",
        "frontend_backend_UI_API",
        "teppan_watch_policy",
        "pre_strength_family",
        "R11_liquidity_operational_lane",
        "threshold_no_trade",
        "image_fusion",
        "sell_side",
        "buy_more_core_logic",
        "exit_optimization",
        "cost_slippage_liquidity_axis",
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
