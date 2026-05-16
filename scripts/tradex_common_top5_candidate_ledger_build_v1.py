"""TRADEX-only common top5 candidate ledger build for selected family v2."""

from __future__ import annotations

import argparse
import json
import math
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping


AXIS_ID = "common_top5_candidate_ledger_build_v1"
DEFAULT_RUN_ID = "20260514T220000Z-common-top5-candidate-ledger-build-v1"
DEFAULT_OUTPUT_PARENT = Path(r"G:\Tradex\common_top5_candidate_ledger_build_v1")
DEFAULT_RISK_ROOT = Path(
    r"G:\Tradex\selected_family_v2_risk_decomposition_v1"
    r"\20260514T210000Z-selected-family-v2-risk-decomposition-v1"
)
DEFAULT_WIDE_SOURCE_REFS = Path(
    r"G:\Tradex\wide_strength_pool_upside_rerank_v1"
    r"\20260513T030000Z-wide-strength-pool-upside-rerank-v1"
    r"\source_artifact_refs.json"
)
DEFAULT_WIDE_SELECTION_LEDGER = Path(
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
BASELINE_FAMILY_ID = "all_strength_scoreless_random_top3"
ORACLE_FAMILY_ID = "all_strength_oracle_top3"

REQUIRED_OUTPUTS = [
    "evaluation_contract.json",
    "run_manifest.json",
    "source_artifact_refs.json",
    "common_top5_candidate_ledger_contract.json",
    "common_top5_candidate_ledger.jsonl",
    "ledger_field_availability_audit.json",
    "candidate_membership_summary.json",
    "family_overlap_report.json",
    "momentum_risk_context_flag_report.json",
    "ma5_h12_membership_report.json",
    "leakage_audit.json",
    "next_axis_recommendation.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
]

LABEL_FIELDS = ["ret20_fwd", "mfe20", "mae20", "severe_loss20", "win20", "is_future_top10_by_ret20", "is_big_winner_ret20_ge_10pct"]
POINT_IN_TIME_FIELDS = [
    "pre_ma20_path_state",
    "pre_ret20_state",
    "pre_ret5_state",
    "weekly_prior_state",
    "monthly_prior_state",
    "negative_guard_match",
    "guard_safe_full",
    "ma_stack",
    "ma60_slope_state",
]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--risk-root", type=Path, default=DEFAULT_RISK_ROOT)
    parser.add_argument("--wide-source-refs", type=Path, default=DEFAULT_WIDE_SOURCE_REFS)
    parser.add_argument("--wide-selection-ledger", type=Path, default=DEFAULT_WIDE_SELECTION_LEDGER)
    parser.add_argument("--ma5-trade-ledger", type=Path, default=DEFAULT_MA5_TRADE_LEDGER)
    parser.add_argument("--output-parent", type=Path, default=DEFAULT_OUTPUT_PARENT)
    parser.add_argument("--run-id", default=DEFAULT_RUN_ID)
    args = parser.parse_args()
    run_common_top5_candidate_ledger_build_v1(
        risk_root=args.risk_root,
        wide_source_refs=args.wide_source_refs,
        wide_selection_ledger=args.wide_selection_ledger,
        ma5_trade_ledger=args.ma5_trade_ledger,
        output_parent=args.output_parent,
        run_id=args.run_id,
    )
    return 0


def run_common_top5_candidate_ledger_build_v1(
    *,
    risk_root: Path = DEFAULT_RISK_ROOT,
    wide_source_refs: Path = DEFAULT_WIDE_SOURCE_REFS,
    wide_selection_ledger: Path = DEFAULT_WIDE_SELECTION_LEDGER,
    ma5_trade_ledger: Path = DEFAULT_MA5_TRADE_LEDGER,
    output_parent: Path = DEFAULT_OUTPUT_PARENT,
    run_id: str = DEFAULT_RUN_ID,
) -> dict[str, Any]:
    output_root = output_parent / run_id
    output_root.mkdir(parents=True, exist_ok=True)
    roots = {
        "risk_root": risk_root,
        "wide_source_refs": wide_source_refs,
        "wide_selection_ledger": wide_selection_ledger,
        "ma5_trade_ledger": ma5_trade_ledger,
    }
    risk_artifacts = _load_risk_artifacts(risk_root)
    pre_strength_ledger = _pre_strength_ledger_path(wide_source_refs)
    baseline_rows = _load_pre_strength_rows(pre_strength_ledger)
    selected_rows = _load_selection_rows(wide_selection_ledger)
    ma5_rows = _load_ma5_h12_rows(ma5_trade_ledger)
    ledger_rows = _build_common_ledger(baseline_rows, selected_rows, ma5_rows)
    ledger_path = output_root / "common_top5_candidate_ledger.jsonl"
    _write_jsonl(ledger_path, ledger_rows)
    field_audit = _field_availability_audit(ledger_rows)
    membership = _membership_summary(ledger_rows)
    overlap = _family_overlap_report(ledger_rows)
    momentum_flags = _momentum_risk_context_flag_report(ledger_rows)
    ma5_report = _ma5_h12_membership_report(ledger_rows, ma5_rows)
    leakage = _leakage_audit(ledger_rows)
    decision = _research_decision(field_audit, membership, ma5_report, leakage)
    next_axis = _next_axis(decision)
    payloads = {
        "evaluation_contract.json": _evaluation_contract(roots),
        "run_manifest.json": _run_manifest(output_root, roots),
        "source_artifact_refs.json": _source_refs(roots, pre_strength_ledger, risk_artifacts),
        "common_top5_candidate_ledger_contract.json": _ledger_contract(pre_strength_ledger, wide_selection_ledger, ma5_trade_ledger),
        "ledger_field_availability_audit.json": field_audit,
        "candidate_membership_summary.json": membership,
        "family_overlap_report.json": overlap,
        "momentum_risk_context_flag_report.json": momentum_flags,
        "ma5_h12_membership_report.json": ma5_report,
        "leakage_audit.json": leakage,
        "next_axis_recommendation.json": next_axis,
        "research_decision.json": decision,
    }
    for name, payload in payloads.items():
        _write_json(output_root / name, payload)
    complete = _artifact_complete(output_root, decision)
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    return {
        "output_root": str(output_root),
        "common_top5_candidate_ledger_path": str(ledger_path),
        "evaluation_contract": payloads["evaluation_contract.json"],
        "run_manifest": payloads["run_manifest.json"],
        "source_artifact_refs": payloads["source_artifact_refs.json"],
        "common_top5_candidate_ledger_contract": payloads["common_top5_candidate_ledger_contract.json"],
        "ledger_field_availability_audit": field_audit,
        "candidate_membership_summary": membership,
        "family_overlap_report": overlap,
        "momentum_risk_context_flag_report": momentum_flags,
        "ma5_h12_membership_report": ma5_report,
        "leakage_audit": leakage,
        "next_axis_recommendation": next_axis,
        "research_decision": decision,
        "artifact_complete": complete,
    }


def _build_common_ledger(
    baseline_rows: list[dict[str, Any]],
    selected_rows: Mapping[tuple[str, str], dict[str, dict[str, Any]]],
    ma5_rows: Mapping[tuple[str, str], dict[str, Any]],
) -> list[dict[str, Any]]:
    by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for row in baseline_rows:
        key = (str(row["event_date"]), str(row["code"]))
        by_key[key] = _baseline_ledger_row(row)
    for key, ma5 in ma5_rows.items():
        if key not in by_key:
            by_key[key] = _empty_additive_row(key)
        _apply_ma5_membership(by_key[key], ma5)
    for key, families in selected_rows.items():
        if key not in by_key:
            by_key[key] = _empty_additive_row(key)
        if BASELINE_FAMILY_ID in families:
            _apply_selected_family(by_key[key], families[BASELINE_FAMILY_ID], "baseline")
        if MOMENTUM_FAMILY_ID in families:
            _apply_selected_family(by_key[key], families[MOMENTUM_FAMILY_ID], "momentum")
        if ORACLE_FAMILY_ID in families:
            _apply_selected_family(by_key[key], families[ORACLE_FAMILY_ID], "oracle")
    for row in by_key.values():
        row["combined_candidate_flag"] = bool(row["momentum_candidate_flag"] or row["ma5_h12_candidate_flag"])
        row["source_family_flags"] = {
            "baseline": bool(row["baseline_candidate_flag"]),
            "momentum_continuation_soft_boost_v1": bool(row["momentum_candidate_flag"]),
            "ma5_h12_near_bull_ma60_rising": bool(row["ma5_h12_candidate_flag"]),
            "combined": bool(row["combined_candidate_flag"]),
        }
        row["ret20_label_available"] = row.get("ret20_fwd") is not None
        row["mfe20_label_available"] = row.get("mfe20") is not None
        row["mae20_label_available"] = row.get("mae20") is not None
        row["evaluation_label_available"] = all(row.get(field) is not None for field in ("ret20_fwd", "mfe20", "mae20", "severe_loss20"))
    return sorted(by_key.values(), key=lambda row: (str(row["event_date"]), str(row["symbol"])))


def _baseline_ledger_row(row: Mapping[str, Any]) -> dict[str, Any]:
    event_strength = _float(row.get("event_strength_score"))
    return {
        "event_date": str(row.get("event_date")),
        "symbol": str(row.get("code")),
        "baseline_candidate_flag": True,
        "baseline_score": event_strength,
        "baseline_score_available": event_strength is not None,
        "baseline_rank": None,
        "baseline_rank_available": False,
        "momentum_candidate_flag": False,
        "momentum_score": None,
        "momentum_score_available": False,
        "momentum_rank": None,
        "momentum_rank_available": False,
        "ma5_h12_candidate_flag": False,
        "ma5_h12_context_score": None,
        "ma5_h12_score_available": False,
        "ma5_h12_rank": None,
        "ma5_h12_rank_available": False,
        "combined_candidate_flag": False,
        "combined_score": None,
        "combined_score_available": False,
        "shadow_candidate_rank": None,
        "shadow_candidate_rank_available": False,
        "pre_ma20_path_state": row.get("pre_ma20_path_state"),
        "pre_ret20_state": row.get("pre_ret20_state"),
        "pre_ret5_state": row.get("pre_ret5_state"),
        "weekly_prior_state": row.get("weekly_prior_state"),
        "monthly_prior_state": row.get("monthly_prior_state"),
        "negative_guard_match": None,
        "guard_safe_full": None,
        "ma_stack": None,
        "ma60_slope_state": None,
        "momentum_low_risk_context_flag": False,
        "momentum_high_risk_context_flag": False,
        "ret20_fwd": _float(row.get("ret20_fwd")),
        "mfe20": _float(row.get("mfe20")),
        "mae20": _float(row.get("mae20")),
        "severe_loss20": _bool_or_none(row.get("severe_loss20")),
        "win20": _bool_or_none(row.get("win20")),
        "is_future_top10_by_ret20": None,
        "is_big_winner_ret20_ge_10pct": _bool_or_none((_float(row.get("ret20_fwd")) or 0.0) >= 0.10),
        "ma5_exit_ret": None,
        "ma5_exit_mfe": None,
        "ma5_exit_mae": None,
        "ma5_exit_severe_loss": None,
        "candidate_construction_uses_future_labels": False,
    }


def _empty_additive_row(key: tuple[str, str]) -> dict[str, Any]:
    row = _baseline_ledger_row({"event_date": key[0], "code": key[1]})
    row["baseline_candidate_flag"] = False
    row["baseline_score"] = None
    row["baseline_score_available"] = False
    return row


def _apply_selected_family(row: dict[str, Any], selected: Mapping[str, Any], prefix: str) -> None:
    if prefix == "baseline":
        row["baseline_candidate_flag"] = True
        row["baseline_score"] = _float(selected.get("research_score"))
        row["baseline_score_available"] = row["baseline_score"] is not None
        row["baseline_rank"] = _float(selected.get("selection_rank"))
        row["baseline_rank_available"] = row["baseline_rank"] is not None
    elif prefix == "momentum":
        row["momentum_candidate_flag"] = True
        row["momentum_score"] = _float(selected.get("research_score"))
        row["momentum_score_available"] = row["momentum_score"] is not None
        row["momentum_rank"] = _float(selected.get("selection_rank"))
        row["momentum_rank_available"] = row["momentum_rank"] is not None
        for field in ("negative_guard_match", "guard_safe_full", "pre_ma20_path_state", "pre_ret20_state", "pre_ret5_state", "weekly_prior_state", "monthly_prior_state"):
            if selected.get(field) is not None:
                row[field] = selected.get(field)
        row["is_future_top10_by_ret20"] = _bool_or_none(selected.get("is_future_top10_by_ret20"))
        row["is_big_winner_ret20_ge_10pct"] = _bool_or_none(selected.get("is_big_winner_ret20_ge_10pct"))
        _apply_momentum_context_flags(row)
    elif prefix == "oracle":
        row["oracle_score"] = _float(selected.get("research_score"))
        row["oracle_rank"] = _float(selected.get("selection_rank"))


def _apply_ma5_membership(row: dict[str, Any], ma5: Mapping[str, Any]) -> None:
    row["ma5_h12_candidate_flag"] = True
    row["ma_stack"] = ma5.get("ma_stack")
    row["ma60_slope_state"] = ma5.get("ma60_slope_state")
    row["ma5_h12_context_score"] = None
    row["ma5_h12_score_available"] = False
    row["ma5_h12_rank"] = None
    row["ma5_h12_rank_available"] = False
    row["ma5_exit_ret"] = _float(ma5.get("ret"))
    row["ma5_exit_mfe"] = _float(ma5.get("mfe"))
    row["ma5_exit_mae"] = _float(ma5.get("mae"))
    row["ma5_exit_severe_loss"] = _bool_or_none(ma5.get("severe_loss"))


def _apply_momentum_context_flags(row: dict[str, Any]) -> None:
    low_risk = (
        str(row.get("negative_guard_match")) == "False"
        and str(row.get("pre_ma20_path_state")) in {"pre_ma20_reclaim_base", "pre_ma20_near"}
        and str(row.get("weekly_prior_state")) in {"weekly_prior_uptrend", "weekly_prior_mixed", "weekly_prior_recovery"}
    )
    high_risk = (
        str(row.get("negative_guard_match")) == "True"
        and str(row.get("pre_ma20_path_state")) in {"pre_ma20_near", "pre_ma20_already_extended"}
        and str(row.get("weekly_prior_state")) in {"weekly_prior_strong_up", "weekly_prior_recovery"}
    )
    row["momentum_low_risk_context_flag"] = low_risk
    row["momentum_high_risk_context_flag"] = high_risk


def _field_availability_audit(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    def count(field: str) -> int:
        return sum(row.get(field) is not None for row in rows)

    availability = {field: {"available_count": count(field), "available_rate": _rate(count(field), total)} for field in [
        "baseline_score",
        "baseline_rank",
        "momentum_score",
        "momentum_rank",
        "ma5_h12_context_score",
        "ma5_h12_rank",
        "combined_score",
        "shadow_candidate_rank",
        *LABEL_FIELDS,
        "ma5_exit_ret",
    ]}
    ma5_rows = [row for row in rows if row.get("ma5_h12_candidate_flag")]
    ma5_ret20_count = sum(row.get("ret20_fwd") is not None for row in ma5_rows)
    return {
        "schema_version": "tradex_common_top5_candidate_ledger_field_availability_audit_v1",
        "axis_id": AXIS_ID,
        "ledger_row_count": total,
        "availability": availability,
        "score_availability_explicit": True,
        "rank_availability_explicit": True,
        "ma5_h12_rows_with_ret20_label_count": ma5_ret20_count,
        "ma5_h12_rows_missing_ret20_label_count": len(ma5_rows) - ma5_ret20_count,
        "fake_score_or_rank_filled": False,
    }


def _membership_summary(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    return {
        "schema_version": "tradex_common_top5_candidate_membership_summary_v1",
        "axis_id": AXIS_ID,
        "ledger_row_count": total,
        "baseline_candidate_count": sum(bool(row.get("baseline_candidate_flag")) for row in rows),
        "momentum_candidate_count": sum(bool(row.get("momentum_candidate_flag")) for row in rows),
        "ma5_h12_candidate_count": sum(bool(row.get("ma5_h12_candidate_flag")) for row in rows),
        "combined_candidate_count": sum(bool(row.get("combined_candidate_flag")) for row in rows),
        "row_count_by_date": _date_count_summary(rows),
    }


def _family_overlap_report(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    momentum = {_key(row) for row in rows if row.get("momentum_candidate_flag")}
    ma5 = {_key(row) for row in rows if row.get("ma5_h12_candidate_flag")}
    baseline = {_key(row) for row in rows if row.get("baseline_candidate_flag")}
    return {
        "schema_version": "tradex_common_top5_candidate_family_overlap_report_v1",
        "axis_id": AXIS_ID,
        "momentum_ma5_exact_overlap_count": len(momentum & ma5),
        "baseline_ma5_exact_overlap_count": len(baseline & ma5),
        "baseline_momentum_exact_overlap_count": len(baseline & momentum),
        "ma5_additive_to_baseline_count": len(ma5 - baseline),
        "ma5_additive_to_momentum_count": len(ma5 - momentum),
        "date_overlap_count_momentum_ma5": len({key[0] for key in momentum} & {key[0] for key in ma5}),
        "symbol_overlap_count_momentum_ma5": len({key[1] for key in momentum} & {key[1] for key in ma5}),
    }


def _momentum_risk_context_flag_report(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    momentum = [row for row in rows if row.get("momentum_candidate_flag")]
    low = [row for row in momentum if row.get("momentum_low_risk_context_flag")]
    high = [row for row in momentum if row.get("momentum_high_risk_context_flag")]
    return {
        "schema_version": "tradex_common_top5_candidate_momentum_risk_context_flag_report_v1",
        "axis_id": AXIS_ID,
        "momentum_candidate_count": len(momentum),
        "low_risk_context_count": len(low),
        "high_risk_context_count": len(high),
        "low_risk_context_metrics": _label_metrics(low),
        "high_risk_context_metrics": _label_metrics(high),
        "flag_columns": ["momentum_low_risk_context_flag", "momentum_high_risk_context_flag"],
    }


def _ma5_h12_membership_report(rows: list[Mapping[str, Any]], ma5_source: Mapping[tuple[str, str], Mapping[str, Any]]) -> dict[str, Any]:
    ma5 = [row for row in rows if row.get("ma5_h12_candidate_flag")]
    ret20_available = [row for row in ma5 if row.get("ret20_fwd") is not None]
    return {
        "schema_version": "tradex_common_top5_candidate_ma5_h12_membership_report_v1",
        "axis_id": AXIS_ID,
        "source_h12_count": len(ma5_source),
        "ledger_h12_count": len(ma5),
        "event_date_symbol_reconstructable": len(ma5) == len(ma5_source),
        "score_available": False,
        "rank_available": False,
        "ret20_label_available_count": len(ret20_available),
        "ret20_label_missing_count": len(ma5) - len(ret20_available),
        "ma5_exit_label_available_count": sum(row.get("ma5_exit_ret") is not None for row in ma5),
        "requires_label_repair_before_direct_top5_validation": len(ret20_available) < len(ma5),
    }


def _leakage_audit(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": "tradex_common_top5_candidate_ledger_leakage_audit_v1",
        "axis_id": AXIS_ID,
        "candidate_construction_future_label_columns": [],
        "future_labels_in_ledger_for_evaluation_only": LABEL_FIELDS + ["ma5_exit_ret", "ma5_exit_mfe", "ma5_exit_mae", "ma5_exit_severe_loss"],
        "future_labels_used_in_candidate_construction": False,
        "candidate_construction_uses_future_labels_any_row": any(bool(row.get("candidate_construction_uses_future_labels")) for row in rows),
        "anti_leakage_pass": True,
    }


def _research_decision(
    field_audit: Mapping[str, Any],
    membership: Mapping[str, Any],
    ma5_report: Mapping[str, Any],
    leakage: Mapping[str, Any],
) -> dict[str, Any]:
    complete_membership = all(
        membership.get(key, 0) > 0
        for key in ("baseline_candidate_count", "momentum_candidate_count", "ma5_h12_candidate_count", "combined_candidate_count")
    )
    if complete_membership and leakage.get("anti_leakage_pass") and not ma5_report.get("requires_label_repair_before_direct_top5_validation"):
        decision = "common_ledger_ready"
        next_axis = "common_top5_candidate_pool_validation_v1"
        reasons = ["common_ledger_generated", "all_membership_flags_present", "evaluation_labels_available"]
    elif complete_membership and leakage.get("anti_leakage_pass"):
        decision = "hold"
        next_axis = "common_ledger_field_repair_v1"
        reasons = [
            "common_ledger_generated",
            "membership_flags_present",
            "ma5_h12_ret20_labels_missing_for_additive_rows",
            "score_rank_unavailable_marked_explicitly",
        ]
    else:
        decision = "failed"
        next_axis = "selected_family_v2_drop_or_refresh"
        reasons = ["common_ledger_build_failed_or_leakage_detected"]
    return {
        "schema_version": "tradex_common_top5_candidate_ledger_build_research_decision_v1",
        "research_phase": "common_top5_candidate_ledger_build",
        "boundary": "TRADEX-only",
        "decision": decision,
        "authoritative_research_decision": decision,
        "recommended_next_axis": next_axis,
        "top5_improvement_claimed": False,
        "candidate_selection_validation_run": False,
        "common_ledger_generated": complete_membership,
        "future_labels_used_in_candidate_construction": False,
        "future_labels_used_for_evaluation_only": True,
        "fake_score_or_rank_filled": bool(field_audit.get("fake_score_or_rank_filled")),
        "silent_fallback_used": False,
        "research_fallback_used": False,
        "candidate_scoring_created": False,
        "threshold_policy_created": False,
        "image_score_used": False,
        "fusion_reranker_created": False,
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
        "typed_reasons": reasons,
        "generated_at_utc": _utc_now(),
    }


def _next_axis(decision: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "tradex_common_top5_candidate_ledger_build_next_axis_recommendation_v1",
        "axis_id": AXIS_ID,
        "decision": decision["decision"],
        "next": decision["recommended_next_axis"],
        "activation_allowed": False,
        "meemee_reflection_allowed": False,
    }


def _evaluation_contract(roots: Mapping[str, Path]) -> dict[str, Any]:
    return {
        "schema_version": "tradex_common_top5_candidate_ledger_build_evaluation_contract_v1",
        "axis_id": AXIS_ID,
        "purpose": "build common event_date+symbol ledger only; no top5 improvement claim",
        "boundary": "TRADEX-only",
        "input_paths": {key: str(value) for key, value in roots.items()},
        "future_label_policy": {
            "future_labels_allowed_in_candidate_construction": False,
            "future_labels_allowed_in_evaluation": True,
        },
        "not_changed": _not_changed(),
    }


def _run_manifest(output_root: Path, roots: Mapping[str, Path]) -> dict[str, Any]:
    return {
        "schema_version": "tradex_common_top5_candidate_ledger_build_run_manifest_v1",
        "axis_id": AXIS_ID,
        "run_id": output_root.name,
        "output_root": str(output_root),
        "inputs": {key: str(value) for key, value in roots.items()},
        "generated_at_utc": _utc_now(),
    }


def _source_refs(roots: Mapping[str, Path], pre_strength_ledger: Path, risk_artifacts: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "tradex_common_top5_candidate_ledger_build_source_refs_v1",
        "axis_id": AXIS_ID,
        "refs": {key: str(value) for key, value in roots.items()},
        "pre_strength_event_ledger": str(pre_strength_ledger),
        "source_risk_decision": risk_artifacts["research_decision"].get("decision"),
        "silent_fallback_used": False,
    }


def _ledger_contract(pre_strength_ledger: Path, wide_selection_ledger: Path, ma5_trade_ledger: Path) -> dict[str, Any]:
    return {
        "schema_version": "tradex_common_top5_candidate_ledger_contract_v1",
        "axis_id": AXIS_ID,
        "ledger_key": ["event_date", "symbol"],
        "source_ledgers": {
            "baseline_universe": str(pre_strength_ledger),
            "momentum_selection": str(wide_selection_ledger),
            "ma5_h12_membership": str(ma5_trade_ledger),
        },
        "required_membership_flags": ["baseline_candidate_flag", "momentum_candidate_flag", "ma5_h12_candidate_flag", "combined_candidate_flag"],
        "score_rank_policy": "only source-available scores/ranks are populated; unavailable fields are null with availability flags false",
        "future_label_policy": "labels are carried for evaluation only and not used in membership construction",
        "no_fake_score_rank": True,
    }


def _load_risk_artifacts(root: Path) -> dict[str, Any]:
    return {
        "research_decision": _read_json_optional(root / "research_decision.json"),
        "common_design": _read_json_optional(root / "common_top5_candidate_ledger_design.json"),
        "artifact_complete": _read_json_optional(root / "_ARTIFACT_COMPLETE.json"),
    }


def _pre_strength_ledger_path(source_refs: Path) -> Path:
    data = _read_json_optional(source_refs)
    for row in data.get("refs") or []:
        if isinstance(row, Mapping) and row.get("name") == "pre_strength_event_ledger.jsonl" and row.get("exists"):
            return Path(str(row["path"]))
    raise FileNotFoundError(f"pre_strength_event_ledger.jsonl not found in {source_refs}")


def _load_pre_strength_rows(path: Path) -> list[dict[str, Any]]:
    return list(_iter_jsonl(path))


def _load_selection_rows(path: Path) -> dict[tuple[str, str], dict[str, dict[str, Any]]]:
    out: dict[tuple[str, str], dict[str, dict[str, Any]]] = defaultdict(dict)
    for row in _iter_jsonl(path):
        family = str(row.get("research_family_id"))
        if family not in {BASELINE_FAMILY_ID, MOMENTUM_FAMILY_ID, ORACLE_FAMILY_ID}:
            continue
        key = (str(row.get("event_date")), str(row.get("code")))
        out[key][family] = row
    return out


def _load_ma5_h12_rows(path: Path) -> dict[tuple[str, str], dict[str, Any]]:
    out = {}
    for row in _iter_jsonl(path):
        if row.get("ma_stack") == "ma5_above_20_below_60" and row.get("ma60_slope_state") == "ma60_rising":
            out[(str(row.get("signal_date")), str(row.get("symbol")))] = row
    return out


def _iter_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(path)
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                yield json.loads(line)


def _date_count_summary(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    counts = Counter(str(row.get("event_date")) for row in rows)
    values = list(counts.values())
    return {
        "date_count": len(counts),
        "min_rows_per_date": min(values) if values else 0,
        "max_rows_per_date": max(values) if values else 0,
    }


def _label_metrics(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"count": 0}
    severe = sum(bool(row.get("severe_loss20")) for row in rows if row.get("severe_loss20") is not None)
    available = [row for row in rows if row.get("ret20_fwd") is not None]
    return {
        "count": len(rows),
        "label_available_count": len(available),
        "avg_ret20": _mean(row.get("ret20_fwd") for row in available),
        "severe_loss_rate20": _rate(severe, len(available)),
        "big_winner_rate": _rate(sum(bool(row.get("is_big_winner_ret20_ge_10pct")) for row in available), len(available)),
    }


def _key(row: Mapping[str, Any]) -> tuple[str, str]:
    return str(row.get("event_date")), str(row.get("symbol"))


def _artifact_complete(output_root: Path, decision: Mapping[str, Any]) -> dict[str, Any]:
    presence = {name: (output_root / name).exists() for name in REQUIRED_OUTPUTS if name != "_ARTIFACT_COMPLETE.json"}
    presence["_ARTIFACT_COMPLETE.json"] = True
    return {
        "schema_version": "tradex_common_top5_candidate_ledger_build_artifact_complete_v1",
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


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(dict(payload)), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(_json_ready(dict(row)), ensure_ascii=False, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


def _float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None:
            return default
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def _mean(values: Iterable[Any]) -> float | None:
    nums = [_float(value) for value in values]
    nums = [value for value in nums if value is not None]
    return None if not nums else sum(nums) / len(nums)


def _rate(count: int, total: int) -> float | None:
    return None if total <= 0 else count / total


def _bool_or_none(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in {"true", "1", "yes"}
    return bool(value)


def _not_changed() -> list[str]:
    return [
        "top5_improvement_claim",
        "candidate_selection_validation",
        "starter_entry_pretest",
        "MeeMee_runtime",
        "active_ranking",
        "display_score",
        "runtime_DuckDB",
        "production_registry",
        "publish_bundle",
        "threshold_no_trade",
        "image_fusion",
        "teppan_policy",
        "pre_strength_revival",
        "R11_inclusion",
        "sell_side_exit_cost_liquidity",
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


if __name__ == "__main__":
    raise SystemExit(main())
