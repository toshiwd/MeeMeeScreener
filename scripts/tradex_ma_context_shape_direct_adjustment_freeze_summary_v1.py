from __future__ import annotations

import argparse
import json
import math
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\research_freeze_summaries\ma_context_shape_direct_adjustment_line")
SCHEMA_VERSION = "tradex_ma_context_shape_direct_adjustment_line_freeze_summary_v1"

BRANCH_SPECS: list[dict[str, Any]] = [
    {
        "branch_name": "ma_state_family_bad_pick_pruner_v1",
        "branch_kind": "ranking_pruner",
        "analysis_only": False,
        "session_path": Path(r"G:\Tradex\ma_state_family_bad_pick_pruner_v1\20260429T071723Z-2a858f13"),
        "decision_file": "ma_state_family_bad_pick_pruner_v1_decision.json",
        "compare_file": "ma_state_family_bad_pick_pruner_v1_compare.json",
        "why_not_promoted": "Top5/Top10 contamination improved vs champion, but forward_ret_20d and path_value_score_v1 both worsened; Top20 did not move.",
    },
    {
        "branch_name": "ma_state_family_bad_pick_pruner_v1_1_narrow_penalty",
        "branch_kind": "ranking_pruner",
        "analysis_only": False,
        "session_path": Path(r"G:\Tradex\ma_state_family_bad_pick_pruner_v1_1_narrow_penalty\20260429T072508Z-bf85d2f9"),
        "decision_file": "ma_state_family_bad_pick_pruner_v1_1_decision.json",
        "compare_file": "ma_state_family_bad_pick_pruner_v1_1_compare.json",
        "why_not_promoted": "Narrowing the penalty reduced coverage but still did not recover path quality; top5 contamination improvement shrank and Top20 remained flat.",
    },
    {
        "branch_name": "ma_state_family_regime_only_bad_pick_pruner_v1",
        "branch_kind": "ranking_pruner",
        "analysis_only": False,
        "session_path": Path(r"G:\Tradex\ma_state_family_regime_only_bad_pick_pruner_v1\20260429T074051Z-0d1f3df7"),
        "decision_file": "ma_state_family_regime_only_bad_pick_pruner_v1_decision.json",
        "compare_file": "ma_state_family_regime_only_bad_pick_pruner_v1_compare.json",
        "why_not_promoted": "Regime gating improved contamination vs champion, but path/return remained mixed and the signal was sparse/context-specific.",
    },
    {
        "branch_name": "ma_state_family_risk_on_trend_bad_pick_pruner_v1",
        "branch_kind": "ranking_pruner",
        "analysis_only": False,
        "session_path": Path(r"G:\Tradex\ma_state_family_risk_on_trend_bad_pick_pruner_v1\20260429T082010Z-30b13edd"),
        "decision_file": "ma_state_family_risk_on_trend_bad_pick_pruner_v1_decision.json",
        "compare_file": "ma_state_family_risk_on_trend_bad_pick_pruner_v1_compare.json",
        "why_not_promoted": "Isolating C:risk_on_trend preserved the contamination signal, but top5/top10 path quality stayed mixed and Top20 still did not move.",
    },
    {
        "branch_name": "ma_state_family_high_value_boost_v1",
        "branch_kind": "ranking_boost",
        "analysis_only": False,
        "session_path": Path(r"G:\Tradex\ma_state_family_high_value_boost_v1\20260429T084326Z-2ae0f0de"),
        "decision_file": "ma_state_family_high_value_boost_v1_decision.json",
        "compare_file": "ma_state_family_high_value_boost_v1_compare.json",
        "why_not_promoted": "A global +0.06 boost improved Top10 slightly but degraded Top5 path quality and left Top20 flat, so it was dropped.",
    },
    {
        "branch_name": "multi_timeframe_context_gated_high_value_boost_v1",
        "branch_kind": "ranking_boost",
        "analysis_only": False,
        "session_path": Path(r"G:\Tradex\multi_timeframe_context_gated_high_value_boost_v1\20260429T094730Z-7e1acdee"),
        "decision_file": "multi_timeframe_context_gated_high_value_boost_v1_decision.json",
        "compare_file": "multi_timeframe_context_gated_high_value_boost_v1_compare.json",
        "why_not_promoted": "Higher-timeframe gating narrowed the surface, but Top5 still regressed and bottom15 contamination remained mixed.",
    },
    {
        "branch_name": "conditional_high_value_candle_shape_modifier_v1",
        "branch_kind": "analysis_only",
        "analysis_only": True,
        "session_path": Path(r"G:\Tradex\conditional_high_value_candle_shape_modifier_v1\20260429T105018Z-26bc381e"),
        "decision_file": "conditional_high_value_candle_shape_modifier_v1_decision.json",
        "compare_file": None,
        "summary_file": "conditional_shape_value_summary.json",
        "classification_file": "conditional_shape_modifier_classification.json",
        "shape_vs_base_file": "shape_vs_base_slice_comparison.json",
        "why_not_promoted": "Analysis-only branch; it produced keep-grade separation evidence, but it was not a ranking challenger by design.",
    },
    {
        "branch_name": "context_gated_candle_shape_modifier_boost_prune_v1",
        "branch_kind": "ranking_boost_prune",
        "analysis_only": False,
        "session_path": Path(r"G:\Tradex\context_gated_candle_shape_modifier_boost_prune_v1\20260429T133803Z-2abd2f69"),
        "decision_file": "context_gated_candle_shape_modifier_boost_prune_v1_decision.json",
        "compare_file": "context_gated_candle_shape_modifier_boost_prune_v1_compare.json",
        "why_not_promoted": "Shape gating recovered some drag versus the prior context-gated boost, but Top5 still regressed versus champion and bottom15 contamination worsened on Top10.",
    },
]

REUSABLE_SIGNAL_BLUEPRINTS: list[dict[str, Any]] = [
    {
        "signal_name": "daily_ma_state_family_value_map",
        "status": "research-only",
        "not_production_ranking_input": True,
        "not_meemee_reflectable": True,
        "recommended_next_use": "candidate_generation_pre_filtering",
        "evidence_branches": [
            "ma_state_family_bad_pick_pruner_v1",
            "ma_state_family_bad_pick_pruner_v1_1_narrow_penalty",
            "ma_state_family_regime_only_bad_pick_pruner_v1",
            "ma_state_family_risk_on_trend_bad_pick_pruner_v1",
            "ma_state_family_high_value_boost_v1",
        ],
        "why_reusable": "The daily state map consistently contains signal, but same-condition top-K score adjustment failed to beat the champion.",
    },
    {
        "signal_name": "multi_timeframe_conditional_separation",
        "status": "research-only",
        "not_production_ranking_input": True,
        "not_meemee_reflectable": True,
        "recommended_next_use": "candidate_generation_pre_filtering",
        "evidence_branches": [
            "multi_timeframe_context_gated_high_value_boost_v1",
            "context_gated_candle_shape_modifier_boost_prune_v1",
        ],
        "why_reusable": "Monthly × weekly × daily conditioning improved separation over the global surface, but not enough for keep-grade ranking improvement.",
    },
    {
        "signal_name": "conditional_high_value_candle_shape_separation",
        "status": "research-only",
        "not_production_ranking_input": True,
        "not_meemee_reflectable": True,
        "recommended_next_use": "candidate_generation_pre_filtering_or_explanation",
        "evidence_branches": [
            "conditional_high_value_candle_shape_modifier_v1",
            "context_gated_candle_shape_modifier_boost_prune_v1",
        ],
        "why_reusable": "Within conditional_high_value, several candle-shape modifiers showed stable positive lift and no sparse/unstable collapse.",
    },
    {
        "signal_name": "bad_pick_contamination_diagnostics",
        "status": "research-only",
        "not_production_ranking_input": True,
        "not_meemee_reflectable": True,
        "recommended_next_use": "candidate_generation_gating_diagnostics",
        "evidence_branches": [
            "ma_state_family_bad_pick_pruner_v1",
            "ma_state_family_bad_pick_pruner_v1_1_narrow_penalty",
            "ma_state_family_regime_only_bad_pick_pruner_v1",
            "ma_state_family_risk_on_trend_bad_pick_pruner_v1",
        ],
        "why_reusable": "Bad-pick pruning reduced contamination, but the path/return drag shows the diagnostics are more useful as analysis signals than as direct ranking inputs.",
    },
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"missing required source artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    return path


def _safe_float(value: Any, fallback: float | None = None) -> float | None:
    if value is None:
        return fallback
    try:
        out = float(value)
    except Exception:
        return fallback
    return out if math.isfinite(out) else fallback


def _safe_int(value: Any, fallback: int | None = None) -> int | None:
    if value is None:
        return fallback
    try:
        return int(value)
    except Exception:
        return fallback


def _make_session_id() -> str:
    return f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"


def _safe_path(value: str | Path | None, default: Path) -> Path:
    if value and str(value).strip():
        return Path(str(value)).expanduser().resolve()
    return default.resolve()


def _resolve_output_root(output_root: str | Path | None) -> Path:
    return _safe_path(output_root, DEFAULT_OUTPUT_ROOT)


def _lookup_topk_section(selection_only: dict[str, Any], topk: int) -> dict[str, Any]:
    for key in (str(topk), topk):
        section = selection_only.get(key)
        if isinstance(section, dict):
            return section
    return {}


def _topk_effect_from_compare(compare: dict[str, Any], topk: int) -> dict[str, Any] | None:
    champion_vs_challenger = compare.get("champion_vs_challenger") or {}
    selection_only = champion_vs_challenger.get("selection_only") or {}
    section = _lookup_topk_section(selection_only, topk)
    if not section:
        return None
    delta = section.get("delta") or {}
    challenger = (section.get("selection_only") or {}).get("challenger") or {}
    champion = (section.get("selection_only") or {}).get("champion") or {}
    branch_metrics = compare.get("topk_summary") or champion_vs_challenger.get("branching_metrics") or {}
    topk_overlap_key = f"top{topk}_overlap_ratio"
    changed_members_key = f"changed_top{topk}_members_count"
    return {
        "delta": {
            key: delta.get(key)
            for key in [
                "mean_forward_ret_20d",
                "median_forward_ret_20d",
                "mean_path_value_score_v1",
                "median_path_value_score_v1",
                "bad_pick_family_contamination_rate",
                "bottom15_contamination_rate",
                "regime_bad_pick_contamination_rate",
                "top15_capture_rate",
                "top15_contamination_rate",
                "win_rate",
            ]
            if key in delta
        },
        "selection_only": {
            "challenger": {
                "selected_count": challenger.get("selected_count"),
                "selected_anchor_count": challenger.get("selected_anchor_count"),
                "mean_forward_ret_20d": challenger.get("mean_forward_ret_20d"),
                "median_forward_ret_20d": challenger.get("median_forward_ret_20d"),
                "mean_path_value_score_v1": challenger.get("mean_path_value_score_v1"),
                "median_path_value_score_v1": challenger.get("median_path_value_score_v1"),
                "bad_pick_family_contamination_rate": challenger.get("bad_pick_family_contamination_rate"),
                "bottom15_contamination_rate": challenger.get("bottom15_contamination_rate"),
                "top15_capture_rate": challenger.get("top15_capture_rate"),
                "win_rate": challenger.get("win_rate"),
            },
            "champion": {
                "selected_count": champion.get("selected_count"),
                "selected_anchor_count": champion.get("selected_anchor_count"),
                "mean_forward_ret_20d": champion.get("mean_forward_ret_20d"),
                "median_forward_ret_20d": champion.get("median_forward_ret_20d"),
                "mean_path_value_score_v1": champion.get("mean_path_value_score_v1"),
                "median_path_value_score_v1": champion.get("median_path_value_score_v1"),
                "bad_pick_family_contamination_rate": champion.get("bad_pick_family_contamination_rate"),
                "bottom15_contamination_rate": champion.get("bottom15_contamination_rate"),
                "top15_capture_rate": champion.get("top15_capture_rate"),
                "win_rate": champion.get("win_rate"),
            },
        },
        "branching_metrics": {
            "changed_members_count": branch_metrics.get(changed_members_key),
            "overlap_ratio": branch_metrics.get(topk_overlap_key),
            "selection_divergence_reason": branch_metrics.get("selection_divergence_reason"),
            "turnover_proxy": branch_metrics.get("turnover_proxy"),
        },
    }


def _topk_effect_from_shape_analysis(summary: dict[str, Any], comparison: dict[str, Any]) -> dict[str, Any]:
    rows = list(comparison.get("shape_vs_base_slice_rows") or [])
    sorted_rows = sorted(rows, key=lambda row: _safe_float(row.get("delta_mean_path_value_score_v1"), 0.0) or 0.0, reverse=True)
    positive_examples = [
        {
            "candle_shape_modifier": row.get("candle_shape_modifier"),
            "delta_mean_path_value_score_v1": row.get("delta_mean_path_value_score_v1"),
            "delta_mean_forward_ret_20d": row.get("delta_mean_forward_ret_20d"),
            "bottom15_rate": row.get("bottom15_rate"),
            "sample_count": row.get("sample_count"),
            "shape_classification": row.get("shape_classification"),
        }
        for row in sorted_rows[:3]
    ]
    negative_examples = [
        {
            "candle_shape_modifier": row.get("candle_shape_modifier"),
            "delta_mean_path_value_score_v1": row.get("delta_mean_path_value_score_v1"),
            "delta_mean_forward_ret_20d": row.get("delta_mean_forward_ret_20d"),
            "bottom15_rate": row.get("bottom15_rate"),
            "sample_count": row.get("sample_count"),
            "shape_classification": row.get("shape_classification"),
        }
        for row in sorted(rows, key=lambda row: _safe_float(row.get("delta_mean_path_value_score_v1"), 0.0) or 0.0)[:3]
    ]
    return {
        "analysis_only": True,
        "conditional_high_value_gate_count": summary.get("conditional_high_value_gate_count"),
        "conditional_high_value_row_count": summary.get("conditional_high_value_row_count"),
        "shape_bucket_count": summary.get("shape_bucket_count"),
        "shape_bucket_counts": summary.get("shape_bucket_counts"),
        "shape_positive_modifier_count": len((comparison.get("shape_vs_base_slice_rows") or [])),
        "shape_class_counts": comparison.get("shape_vs_base_slice_rows") and {
            "shape_positive_modifier": _safe_int(summary.get("shape_bucket_counts", {}).get("bear_large")) is not None,
        },
        "positive_examples": positive_examples,
        "negative_examples": negative_examples,
    }


def _load_branch_record(branch_spec: dict[str, Any]) -> dict[str, Any]:
    session_path: Path = branch_spec["session_path"]
    decision = _load_json(session_path / branch_spec["decision_file"])
    compare = _load_json(session_path / branch_spec["compare_file"]) if branch_spec.get("compare_file") else None

    record: dict[str, Any] = {
        "branch_name": branch_spec["branch_name"],
        "branch_kind": branch_spec["branch_kind"],
        "analysis_only": bool(branch_spec.get("analysis_only")),
        "source_session_id": session_path.name,
        "source_session_path": str(session_path),
        "source_artifacts": {
            "decision": str(session_path / branch_spec["decision_file"]),
            "compare": str(session_path / branch_spec["compare_file"]) if branch_spec.get("compare_file") else None,
        },
        "decision": decision.get("recommendation") or decision.get("authoritative_rollup_decision"),
        "authoritative_rollup_decision": decision.get("authoritative_rollup_decision") or decision.get("recommendation"),
        "typed_reasons": decision.get("typed_reasons") or [],
        "retain_as_analysis_evidence": True,
        "why_not_promoted": branch_spec["why_not_promoted"],
    }

    if compare is not None:
        branching_metrics = compare.get("topk_summary") or (compare.get("champion_vs_challenger") or {}).get("branching_metrics") or {}
        record["branching_metrics"] = {
            "changed_top5_members_count": branching_metrics.get("changed_top5_members_count"),
            "changed_top10_members_count": branching_metrics.get("changed_top10_members_count"),
            "changed_top20_members_count": branching_metrics.get("changed_top20_members_count"),
            "changed_rank_count": branching_metrics.get("changed_rank_count"),
            "selection_divergence_reason": branching_metrics.get("selection_divergence_reason"),
            "top5_overlap_ratio": branching_metrics.get("top5_overlap_ratio"),
            "top10_overlap_ratio": branching_metrics.get("top10_overlap_ratio"),
            "top20_overlap_ratio": branching_metrics.get("top20_overlap_ratio"),
            "turnover_proxy": branching_metrics.get("turnover_proxy"),
            "best_month_delta_mean_forward_ret_20d": branching_metrics.get("best_month_delta_mean_forward_ret_20d"),
            "worst_month_delta_mean_forward_ret_20d": branching_metrics.get("worst_month_delta_mean_forward_ret_20d"),
            "monthly_wins": branching_metrics.get("monthly_wins"),
            "monthly_losses": branching_metrics.get("monthly_losses"),
            "monthly_flats": branching_metrics.get("monthly_flats"),
            "zero_pass_months": branching_metrics.get("zero_pass_months"),
        }
        record["topk_effects"] = {str(topk): _topk_effect_from_compare(compare, topk) for topk in (5, 10, 20)}
    else:
        summary = _load_json(session_path / branch_spec["summary_file"])
        classification = _load_json(session_path / branch_spec["classification_file"])
        shape_vs_base = _load_json(session_path / branch_spec["shape_vs_base_file"])
        decision_summary = {
            "shape_bucket_count": decision.get("shape_bucket_count"),
            "shape_positive_modifier_count": decision.get("shape_positive_modifier_count"),
            "shape_context_dependent_count": decision.get("shape_context_dependent_count"),
            "shape_negative_modifier_count": decision.get("shape_negative_modifier_count"),
            "shape_sparse_or_unstable_count": decision.get("shape_sparse_or_unstable_count"),
            "shape_neutral_count": decision.get("shape_neutral_count"),
            "conditional_high_value_row_count": decision.get("conditional_high_value_row_count"),
            "conditional_high_value_gate_count": decision.get("conditional_high_value_gate_count"),
            "no_lookahead_inherited": decision.get("no_lookahead_inherited"),
            "monthly_context_no_lookahead": decision.get("monthly_context_no_lookahead"),
            "weekly_context_no_lookahead": decision.get("weekly_context_no_lookahead"),
        }
        record["analysis_summary"] = {
            **decision_summary,
            "shape_class_counts": classification.get("shape_class_counts"),
            "top_positive_shape_modifiers": _top_shape_rows(shape_vs_base, reverse=True),
            "top_negative_shape_modifiers": _top_shape_rows(shape_vs_base, reverse=False),
            "reference_context_gated_boost_session_id": summary.get("reference_context_gated_boost_session_id"),
            "source_thresholds": summary.get("source_thresholds"),
        }
        record["topk_effects"] = {
            "5": None,
            "10": None,
            "20": None,
        }

    return record


def _top_shape_rows(shape_vs_base: dict[str, Any], *, reverse: bool) -> list[dict[str, Any]]:
    rows = list(shape_vs_base.get("shape_vs_base_slice_rows") or [])
    sorted_rows = sorted(rows, key=lambda row: _safe_float(row.get("delta_mean_path_value_score_v1"), 0.0) or 0.0, reverse=reverse)
    picked = sorted_rows[:3]
    return [
        {
            "candle_shape_modifier": row.get("candle_shape_modifier"),
            "shape_classification": row.get("shape_classification"),
            "delta_mean_forward_ret_20d": row.get("delta_mean_forward_ret_20d"),
            "delta_mean_path_value_score_v1": row.get("delta_mean_path_value_score_v1"),
            "delta_bottom15_rate": row.get("delta_bottom15_rate"),
            "top15_rate": row.get("top15_rate"),
            "bottom15_rate": row.get("bottom15_rate"),
            "sample_count": row.get("sample_count"),
            "unique_symbol_count": row.get("unique_symbol_count"),
            "month_count": row.get("month_count"),
            "positive_month_rate": row.get("positive_month_rate"),
        }
        for row in picked
    ]


def _build_lineage_summary(branches: list[dict[str, Any]]) -> dict[str, Any]:
    decision_counts: dict[str, int] = {"keep": 0, "hold": 0, "drop": 0, "analysis_only_keep": 0}
    for branch in branches:
        decision = str(branch.get("decision") or "").strip()
        if branch.get("analysis_only"):
            if decision == "keep":
                decision_counts["analysis_only_keep"] += 1
            continue
        if decision in decision_counts:
            decision_counts[decision] += 1

    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "freeze_line_name": "ma_context_shape_direct_adjustment_line",
        "decision": "freeze_direct_ranking_adjustment",
        "decision_reason": "analysis_signal_exists_but_same_condition_topk_improvement_failed",
        "same_condition_contract": True,
        "branches_reviewed": len(branches),
        "ranking_challenger_count": sum(1 for branch in branches if not branch.get("analysis_only")),
        "analysis_only_count": sum(1 for branch in branches if branch.get("analysis_only")),
        "branch_outcome_counts": decision_counts,
        "branches": branches,
    }


def _build_freeze_decision(lineage_summary: dict[str, Any]) -> dict[str, Any]:
    branches = lineage_summary.get("branches") or []
    reuse_signal_count = len(REUSABLE_SIGNAL_BLUEPRINTS)
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "freeze_line_name": lineage_summary.get("freeze_line_name"),
        "decision": "freeze_direct_ranking_adjustment",
        "candidate_local_decision": "freeze_direct_ranking_adjustment",
        "session_aggregate_decision": "freeze_direct_ranking_adjustment",
        "authoritative_rollup_decision": "freeze_direct_ranking_adjustment",
        "decision_reason": "analysis_signal_exists_but_same_condition_topk_improvement_failed",
        "same_condition_contract": True,
        "not_meemee_reflectable": True,
        "production_reflection_allowed": False,
        "branches_reviewed": len(branches),
        "ranking_challenger_count": lineage_summary.get("ranking_challenger_count"),
        "analysis_only_count": lineage_summary.get("analysis_only_count"),
        "branch_outcome_counts": lineage_summary.get("branch_outcome_counts"),
        "retained_analysis_signal_count": reuse_signal_count,
        "frozen_axes": [
            "ma_state_family",
            "multi_timeframe_context",
            "candle_shape_modifier",
        ],
        "next_allowed_action": "test a different axis such as candidate-generation pre-filtering or explanation/similarity-chart research",
        "typed_reasons": [
            "analysis_signal_exists_but_same_condition_topk_improvement_failed",
        ],
    }


def _build_remaining_reusable_signals(branches: list[dict[str, Any]]) -> dict[str, Any]:
    branch_names = {branch["branch_name"] for branch in branches}
    signals: list[dict[str, Any]] = []
    for signal in REUSABLE_SIGNAL_BLUEPRINTS:
        evidence_branches = [name for name in signal["evidence_branches"] if name in branch_names]
        signals.append(
            {
                "signal_name": signal["signal_name"],
                "status": signal["status"],
                "not_production_ranking_input": signal["not_production_ranking_input"],
                "not_meemee_reflectable": signal["not_meemee_reflectable"],
                "recommended_next_use": signal["recommended_next_use"],
                "evidence_branches": evidence_branches,
                "why_reusable": signal["why_reusable"],
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "signals": signals,
    }


def _build_next_axis_recommendation() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "recommended_axis": "candidate_generation_pre_filtering_with_context_shape_signals",
        "why": "Direct same-condition top-K score adjustment repeatedly failed, but the signal stack remains useful as a gate before the ranking boundary.",
        "do_not_continue": "Further retuning of the same MA/context/shape direct ranking-adjustment line.",
        "alternative": "Use the signals for explanation/similarity-chart research if candidate-generation work is not ready.",
    }


def build_freeze_artifacts() -> dict[str, Any]:
    branches = [_load_branch_record(spec) for spec in BRANCH_SPECS]
    lineage_summary = _build_lineage_summary(branches)
    freeze_decision = _build_freeze_decision(lineage_summary)
    remaining_reusable_signals = _build_remaining_reusable_signals(branches)
    next_axis_recommendation = _build_next_axis_recommendation()
    return {
        "lineage_summary": lineage_summary,
        "freeze_decision": freeze_decision,
        "remaining_reusable_signals": remaining_reusable_signals,
        "next_axis_recommendation": next_axis_recommendation,
    }


def write_freeze_artifacts(*, output_root: Path, session_id: str | None = None) -> Path:
    artifacts = build_freeze_artifacts()
    final_session_id = session_id or _make_session_id()
    session_root = output_root / final_session_id
    session_root.mkdir(parents=True, exist_ok=False)
    _write_json(session_root / "lineage_summary.json", artifacts["lineage_summary"])
    _write_json(session_root / "freeze_decision.json", artifacts["freeze_decision"])
    _write_json(session_root / "remaining_reusable_signals.json", artifacts["remaining_reusable_signals"])
    _write_json(session_root / "next_axis_recommendation.json", artifacts["next_axis_recommendation"])
    _write_json(
        session_root / "_ARTIFACT_COMPLETE.json",
        {
            "artifact_complete": True,
            "generated_at": _utc_now(),
            "schema_version": SCHEMA_VERSION,
            "session_root": str(session_root),
            "files": [
                "lineage_summary.json",
                "freeze_decision.json",
                "remaining_reusable_signals.json",
                "next_axis_recommendation.json",
                "_ARTIFACT_COMPLETE.json",
            ],
        },
    )
    return session_root


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Freeze the direct MA/context/shape ranking-adjustment line.")
    parser.add_argument(
        "--output-root",
        default=str(DEFAULT_OUTPUT_ROOT),
        help="Directory that will receive the freeze summary session.",
    )
    parser.add_argument(
        "--session-id",
        default=None,
        help="Optional fixed session id for deterministic tests.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    output_root = _resolve_output_root(args.output_root)
    write_freeze_artifacts(output_root=output_root, session_id=args.session_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
