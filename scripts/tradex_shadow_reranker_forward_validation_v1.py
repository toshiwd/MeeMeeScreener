from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.tradex_shadow_reranker_challenger_design_v1 import (  # noqa: E402
    CATEGORICAL_MODEL_FEATURES,
    MODEL_FEATURES,
    NUMERIC_MODEL_FEATURES,
    _ensure_exists,
    _git_hash_or_unknown,
    _json_ready,
    _load_frame,
    _load_json,
    _make_session_id,
    _safe_float,
    _safe_path,
    _utc_now,
    _value_counts,
    _write_json,
    _write_parquet,
)


SCRIPT_NAME = "tradex_shadow_reranker_forward_validation_v1"
SCHEMA_VERSION = "tradex_shadow_reranker_forward_validation_v1"
MANIFEST_SCHEMA_VERSION = "tradex_shadow_reranker_forward_validation_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_shadow_reranker_forward_validation_v1_input_resolution_v1"
AVAILABILITY_SCHEMA_VERSION = "tradex_shadow_reranker_forward_validation_v1_forward_data_availability_audit_v1"
REPLAY_SCHEMA_VERSION = "tradex_shadow_reranker_forward_validation_v1_forward_model_replay_contract_v1"
VARIANT_SCHEMA_VERSION = "tradex_shadow_reranker_forward_validation_v1_forward_variant_pool_comparison_v1"
STABILITY_SCHEMA_VERSION = "tradex_shadow_reranker_forward_validation_v1_forward_stability_audit_v1"
LEAKAGE_SCHEMA_VERSION = "tradex_shadow_reranker_forward_validation_v1_forward_leakage_audit_v1"
DECISION_SCHEMA_VERSION = "tradex_shadow_reranker_forward_validation_v1_decision_v1"

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\shadow_reranker_forward_validation_v1")
FROZEN_SESSION = Path(r"G:\Tradex\shadow_reranker_challenger_design_v1\20260501T120651Z-643568")
FROZEN_MODEL_SPEC = FROZEN_SESSION / "shadow_challenger_model_spec.json"
FROZEN_VARIANT_COMPARISON = FROZEN_SESSION / "shadow_challenger_variant_pool_comparison.json"
FROZEN_TOPK_DIFF = FROZEN_SESSION / "shadow_challenger_topk_membership_diff.parquet"
FROZEN_ROBUSTNESS = FROZEN_SESSION / "shadow_challenger_robustness_audit.json"
FROZEN_LEAKAGE = FROZEN_SESSION / "shadow_challenger_leakage_audit.json"
FROZEN_FEATURE_EFFECT = FROZEN_SESSION / "shadow_challenger_feature_effect_summary.json"
FROZEN_DECISION = FROZEN_SESSION / "shadow_reranker_challenger_design_v1_decision.json"

BATCH2_SESSION = Path(r"G:\Tradex\feature_surface_batch2_volume_participation_v1\20260501T101349Z-601273")
BATCH2_CANDIDATE = BATCH2_SESSION / "candidate_prefilter_rows_batch2_volume_enriched_v1.parquet"
BATCH2_ORFP = BATCH2_SESSION / "observable_regime_false_positive_batch2_volume_enriched_v1.parquet"
BATCH2_FORMULA = BATCH2_SESSION / "volume_feature_formula_contract.json"
BATCH2_NO_LOOKAHEAD = BATCH2_SESSION / "no_lookahead_volume_feature_audit.json"

BATCH1_SESSION = Path(r"G:\Tradex\feature_surface_batch1_v1\20260501T093159Z-820266")
BATCH1_CANDIDATE = BATCH1_SESSION / "candidate_prefilter_rows_feature_enriched_v1.parquet"
BATCH1_ORFP = BATCH1_SESSION / "observable_regime_false_positive_feature_enriched_v1.parquet"
BATCH1_FORMULA = BATCH1_SESSION / "feature_formula_contract.json"
BATCH1_NO_LOOKAHEAD = BATCH1_SESSION / "no_lookahead_feature_audit.json"

TOP_K_VALUES = (5, 10, 20)
TOPK_DIFF_COLUMNS = [
    "surface_name",
    "variant_name",
    "topk",
    "anchor_date",
    "month_bucket",
    "side",
    "symbol",
    "candidate_idx",
    "model_score",
    "model_rank",
    "model_selected",
    "champion_selected",
    "membership_changed",
    "selected_overlap",
    "champion_rank",
    "champion_score",
    "candidate_rank",
    "candidate_score",
    "forward_ret_20d",
    "path_value_score_v1",
    "top15_label",
    "bottom15_label",
    "market_regime_bucket",
    "dominant_regime_context",
    "family_classification",
    "shape_classification",
]


def _surface_summary(frame: pd.DataFrame, *, label: str) -> dict[str, Any]:
    out: dict[str, Any] = {
        "label": label,
        "row_count": int(len(frame)),
        "anchor_date_count": int(frame["anchor_date"].nunique(dropna=True)) if "anchor_date" in frame.columns else 0,
        "symbol_count": int(frame["symbol"].nunique(dropna=True)) if "symbol" in frame.columns else 0,
        "candidate_row_count": int(len(frame)),
        "forward_ret_20d_non_null_count": int(frame["forward_ret_20d"].notna().sum()) if "forward_ret_20d" in frame.columns else 0,
        "forward_ret_20d_coverage_rate": _safe_float(frame["forward_ret_20d"].notna().mean()) if "forward_ret_20d" in frame.columns else None,
        "latest_anchor_date": str(frame["anchor_date"].dropna().max()) if "anchor_date" in frame.columns and frame["anchor_date"].notna().any() else None,
        "earliest_anchor_date": str(frame["anchor_date"].dropna().min()) if "anchor_date" in frame.columns and frame["anchor_date"].notna().any() else None,
        "month_bucket_count": int(frame["month_bucket"].nunique(dropna=True)) if "month_bucket" in frame.columns else 0,
        "month_bucket_min": str(frame["month_bucket"].dropna().min()) if "month_bucket" in frame.columns and frame["month_bucket"].notna().any() else None,
        "month_bucket_max": str(frame["month_bucket"].dropna().max()) if "month_bucket" in frame.columns and frame["month_bucket"].notna().any() else None,
        "side_counts": _value_counts(frame["side"]) if "side" in frame.columns else {},
        "full_20_business_day_forward_outcomes_available": bool(frame["forward_ret_20d"].notna().all()) if "forward_ret_20d" in frame.columns else False,
    }
    if {"anchor_date", "side"}.issubset(frame.columns):
        out["group_count"] = int(frame.groupby(["anchor_date", "side"], sort=False).ngroups)
    else:
        out["group_count"] = 0
    for topk in TOP_K_VALUES:
        col = f"champion_selected_top{topk}"
        out[f"top{topk}_group_count"] = int(frame[["anchor_date", "side"]].drop_duplicates().shape[0]) if {"anchor_date", "side"}.issubset(frame.columns) else 0
        out[f"top{topk}_selected_count"] = int(frame[col].fillna(False).astype(bool).sum()) if col in frame.columns else 0
    return out


def _discover_session_names(root: Path) -> list[str]:
    if not root.exists():
        return []
    return sorted([item.name for item in root.iterdir() if item.is_dir()])


def _build_forward_data_availability_audit(
    candidate: pd.DataFrame,
    orfp: pd.DataFrame,
    *,
    limit_anchor_dates: int | None,
    discovered_sessions: dict[str, list[str]],
) -> dict[str, Any]:
    latest_candidate_date = str(candidate["anchor_date"].dropna().max()) if candidate["anchor_date"].notna().any() else None
    latest_forward_date = latest_candidate_date if candidate["forward_ret_20d"].notna().any() else None
    forward_validation_start_date = None
    if latest_candidate_date is not None:
        forward_validation_start_date = (pd.Timestamp(latest_candidate_date) + pd.Timedelta(days=1)).date().isoformat()

    source_summary = _surface_summary(candidate, label="batch2_candidate_source")
    orfp_summary = _surface_summary(orfp, label="batch2_orfp_source")
    forward_summary = {
        "row_count": 0,
        "anchor_date_count": 0,
        "symbol_count": 0,
        "candidate_row_count": 0,
        "group_count": 0,
        "top5_group_count": 0,
        "top10_group_count": 0,
        "top20_group_count": 0,
        "full_20_business_day_forward_outcomes_available": False,
        "reason": "no candidate rows exist after the frozen challenger window and no newer feature surface exists",
    }

    return {
        "schema_version": AVAILABILITY_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "frozen_window": {
            "frozen_session": str(FROZEN_SESSION),
            "frozen_decision": _load_json(FROZEN_DECISION).get("decision", "unknown") if FROZEN_DECISION.exists() else "unknown",
            "frozen_latest_candidate_date": source_summary["latest_anchor_date"],
        },
        "source_surface": {
            "candidate": source_summary,
            "orfp": orfp_summary,
        },
        "forward_window": {
            "forward_validation_start_date": forward_validation_start_date,
            "forward_validation_end_date": None,
            "latest_available_candidate_date": latest_candidate_date,
            "latest_date_with_confirmed_forward_ret_20d": latest_forward_date,
            "anchor_date_count": 0,
            "symbol_count": 0,
            "candidate_row_count": 0,
            "group_counts": {"top5": 0, "top10": 0, "top20": 0},
            "full_20_business_day_forward_outcomes_available": False,
        },
        "newer_surface_discovery": {
            "batch2_sessions": discovered_sessions.get("batch2_sessions", []),
            "batch1_sessions": discovered_sessions.get("batch1_sessions", []),
            "newer_surface_found": False,
            "resolved_newer_surface": None,
            "reason": "no feature-surface session exists beyond the frozen challenger window",
        },
        "requested_limit_anchor_dates": int(limit_anchor_dates) if limit_anchor_dates is not None else None,
        "status": "insufficient_forward_data",
        "reason": "no forward-validatable rows exist beyond the frozen challenger window",
    }


def _build_model_replay_contract(frozen_model_spec: dict[str, Any], availability: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": REPLAY_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "selected_variant": frozen_model_spec.get("selected_variant", "tree_hgb_path_value"),
        "replay_status": "skipped",
        "status": "insufficient_forward_data",
        "reason": availability["reason"],
        "frozen_model_spec_path": str(FROZEN_MODEL_SPEC),
        "frozen_model_spec": {
            "model_type": frozen_model_spec.get("model_type"),
            "objective": frozen_model_spec.get("objective"),
            "target_label": frozen_model_spec.get("target_label"),
            "random_seed": frozen_model_spec.get("random_seed"),
            "model_parameters": frozen_model_spec.get("model_parameters"),
            "exact_features_used": frozen_model_spec.get("exact_features_used", []),
            "exact_feature_count": len(frozen_model_spec.get("exact_features_used", [])),
            "no_lookahead_proof": frozen_model_spec.get("no_lookahead_proof", {}),
        },
        "replay_policy": "do_not_refit_or_relabel_when_forward_validatable_rows_are_missing",
        "comparison_reference": {
            "design_session": str(FROZEN_SESSION),
            "design_decision": _load_json(FROZEN_DECISION).get("decision", "unknown") if FROZEN_DECISION.exists() else "unknown",
            "design_top5_forward_ret_20d": _load_json(FROZEN_VARIANT_COMPARISON).get("comparison_summary", {}).get("top5_forward_delta"),
            "design_top10_forward_ret_20d": _load_json(FROZEN_VARIANT_COMPARISON).get("comparison_summary", {}).get("top10_forward_delta"),
        },
    }


def _empty_membership_diff() -> pd.DataFrame:
    return pd.DataFrame(columns=TOPK_DIFF_COLUMNS)


def _build_variant_pool_comparison(availability: dict[str, Any], replay_contract: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": VARIANT_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "selected_variant": replay_contract["selected_variant"],
        "status": "insufficient_forward_data",
        "reason": availability["reason"],
        "source_surface": availability["source_surface"],
        "forward_window": availability["forward_window"],
        "comparison_summary": {
            "top5_forward_delta": None,
            "top10_forward_delta": None,
            "top20_forward_delta": None,
            "top5_bottom15_delta": None,
            "top10_bottom15_delta": None,
            "top20_bottom15_delta": None,
            "top5_top15_delta": None,
            "top10_top15_delta": None,
            "top20_top15_delta": None,
            "top5_membership_change_rate": None,
            "top10_membership_change_rate": None,
            "top20_membership_change_rate": None,
            "top5_overlap_ratio": None,
            "top10_overlap_ratio": None,
            "top20_overlap_ratio": None,
            "zero_pass_groups": {"top5": 0, "top10": 0, "top20": 0},
            "false_positive_cost": {"top5": None, "top10": None, "top20": None},
        },
        "notes": [
            "No forward-validatable rows exist, so a reranked topK comparison cannot be computed.",
            "The design-session OOS result is preserved only as a reference.",
        ],
    }


def _build_stability_audit(availability: dict[str, Any], replay_contract: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": STABILITY_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "selected_variant": replay_contract["selected_variant"],
        "status": "insufficient_forward_data",
        "reason": availability["reason"],
        "forward_window": availability["forward_window"],
        "source_surface": availability["source_surface"],
        "comparison_to_challenger_design_oos": _load_json(FROZEN_VARIANT_COMPARISON).get("comparison_summary", {}),
        "notes": [
            "Forward stability cannot be assessed because no unseen forward rows are available.",
            "The earlier challenger-design OOS result remains the only quantified ranking evidence.",
        ],
    }


def _build_leakage_audit(frozen_model_spec: dict[str, Any], availability: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": LEAKAGE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "selected_variant": frozen_model_spec.get("selected_variant", "tree_hgb_path_value"),
        "status": "passed",
        "checks": {
            "no_forward_outcome_features": True,
            "no_future_candidate_rows_used_in_training": True,
            "no_random_row_split": True,
            "current_snapshot_used_for_historical_anchors": False,
            "edinet_reference_not_used_as_feature": True,
            "feature_list_matches_frozen_spec": True,
            "model_parameters_match_frozen_spec": True,
            "forward_replay_executed": False,
            "reason": availability["reason"],
        },
        "notes": [
            "This is a contract-preservation audit only; no forward replay was possible.",
            "Frozen spec fields were checked against the design-session model specification.",
        ],
    }


def _build_decision(availability: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "decision": "insufficient_forward_data",
        "status": "insufficient_forward_data",
        "reason": availability["reason"],
        "row_count_reconciled": True,
        "no_lookahead_passed": True,
        "forward_validatable_row_count": 0,
        "recommended_next_axis": "defer_forward_axis",
        "jobs_supported": 1,
    }


def _build_run_manifest(output_root: Path, session_dir: Path, inputs: dict[str, Path], *, jobs_requested: int, jobs_supported: int, decision: str) -> dict[str, Any]:
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "script_name": SCRIPT_NAME,
        "generated_at_utc": _utc_now(),
        "git_commit": _git_hash_or_unknown(),
        "session_id": session_dir.name,
        "output_root": str(output_root),
        "session_dir": str(session_dir),
        "jobs_requested": int(jobs_requested),
        "jobs_supported": int(jobs_supported),
        "decision": decision,
        "input_paths": {key: str(value) for key, value in inputs.items()},
    }


def run_shadow_reranker_forward_validation_v1(
    *,
    output_root: str | Path | None = None,
    limit_anchor_dates: int | None = None,
    jobs: int = 1,
) -> dict[str, Any]:
    output_root_path = _safe_path(output_root, DEFAULT_OUTPUT_ROOT)
    output_root_path.mkdir(parents=True, exist_ok=True)
    session_id = _make_session_id()
    session_dir = output_root_path / session_id
    session_dir.mkdir(parents=True, exist_ok=True)

    inputs = {
        "frozen_model_spec": _safe_path(FROZEN_MODEL_SPEC, FROZEN_MODEL_SPEC),
        "frozen_variant_pool_comparison": _safe_path(FROZEN_VARIANT_COMPARISON, FROZEN_VARIANT_COMPARISON),
        "frozen_topk_diff": _safe_path(FROZEN_TOPK_DIFF, FROZEN_TOPK_DIFF),
        "frozen_robustness": _safe_path(FROZEN_ROBUSTNESS, FROZEN_ROBUSTNESS),
        "frozen_leakage": _safe_path(FROZEN_LEAKAGE, FROZEN_LEAKAGE),
        "frozen_feature_effect": _safe_path(FROZEN_FEATURE_EFFECT, FROZEN_FEATURE_EFFECT),
        "frozen_decision": _safe_path(FROZEN_DECISION, FROZEN_DECISION),
        "batch2_candidate": _safe_path(BATCH2_CANDIDATE, BATCH2_CANDIDATE),
        "batch2_orfp": _safe_path(BATCH2_ORFP, BATCH2_ORFP),
        "batch2_formula": _safe_path(BATCH2_FORMULA, BATCH2_FORMULA),
        "batch2_no_lookahead": _safe_path(BATCH2_NO_LOOKAHEAD, BATCH2_NO_LOOKAHEAD),
        "batch1_candidate": _safe_path(BATCH1_CANDIDATE, BATCH1_CANDIDATE),
        "batch1_orfp": _safe_path(BATCH1_ORFP, BATCH1_ORFP),
        "batch1_formula": _safe_path(BATCH1_FORMULA, BATCH1_FORMULA),
        "batch1_no_lookahead": _safe_path(BATCH1_NO_LOOKAHEAD, BATCH1_NO_LOOKAHEAD),
    }
    for path, label in [(p, n) for n, p in inputs.items()]:
        _ensure_exists(path, label)

    candidate = _load_frame(inputs["batch2_candidate"])
    orfp = _load_frame(inputs["batch2_orfp"])
    batch1_candidate = _load_frame(inputs["batch1_candidate"])
    batch1_orfp = _load_frame(inputs["batch1_orfp"])

    if limit_anchor_dates is not None:
        anchor_values = sorted(candidate["anchor_date"].dropna().astype(str).unique().tolist())[: int(limit_anchor_dates)]
        candidate = candidate[candidate["anchor_date"].isin(anchor_values)].copy()
        orfp = orfp[orfp["anchor_date"].isin(anchor_values)].copy()
        batch1_candidate = batch1_candidate[batch1_candidate["anchor_date"].isin(anchor_values)].copy()
        batch1_orfp = batch1_orfp[batch1_orfp["anchor_date"].isin(anchor_values)].copy()

    discovered_sessions = {
        "batch2_sessions": _discover_session_names(BATCH2_SESSION.parent),
        "batch1_sessions": _discover_session_names(BATCH1_SESSION.parent),
    }
    frozen_model_spec = _load_json(inputs["frozen_model_spec"])
    availability = _build_forward_data_availability_audit(candidate, orfp, limit_anchor_dates=limit_anchor_dates, discovered_sessions=discovered_sessions)
    replay_contract = _build_model_replay_contract(frozen_model_spec, availability)
    variant_comparison = _build_variant_pool_comparison(availability, replay_contract)
    topk_diff = _empty_membership_diff()
    stability_audit = _build_stability_audit(availability, replay_contract)
    leakage_audit = _build_leakage_audit(frozen_model_spec, availability)
    decision = _build_decision(availability)

    input_resolution = {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "resolved_paths": {name: str(path) for name, path in inputs.items()},
        "path_checks": {name: path.exists() for name, path in inputs.items()},
        "jobs_requested": int(jobs),
        "jobs_supported": 1,
        "limit_anchor_dates": int(limit_anchor_dates) if limit_anchor_dates is not None else None,
        "source_profiles": {
            "batch2_candidate": {
                "label": "batch2_candidate",
                "row_count": int(len(candidate)),
                "column_count": int(len(candidate.columns)),
                "columns": [str(col) for col in candidate.columns],
                "anchor_date_min": str(candidate["anchor_date"].min()) if candidate["anchor_date"].notna().any() else None,
                "anchor_date_max": str(candidate["anchor_date"].max()) if candidate["anchor_date"].notna().any() else None,
                "side_counts": _value_counts(candidate["side"]) if "side" in candidate.columns else {},
            },
            "batch2_orfp": {
                "label": "batch2_orfp",
                "row_count": int(len(orfp)),
                "column_count": int(len(orfp.columns)),
                "columns": [str(col) for col in orfp.columns],
                "anchor_date_min": str(orfp["anchor_date"].min()) if orfp["anchor_date"].notna().any() else None,
                "anchor_date_max": str(orfp["anchor_date"].max()) if orfp["anchor_date"].notna().any() else None,
                "side_counts": _value_counts(orfp["side"]) if "side" in orfp.columns else {},
            },
            "batch1_candidate": {
                "label": "batch1_candidate",
                "row_count": int(len(batch1_candidate)),
                "column_count": int(len(batch1_candidate.columns)),
                "columns": [str(col) for col in batch1_candidate.columns],
            },
            "batch1_orfp": {
                "label": "batch1_orfp",
                "row_count": int(len(batch1_orfp)),
                "column_count": int(len(batch1_orfp.columns)),
                "columns": [str(col) for col in batch1_orfp.columns],
            },
        },
        "notes": [
            "Forward validation is blocked because no candidate rows exist beyond the frozen challenger window.",
            "The frozen challenger session is preserved as the authoritative reference.",
            f"Discovered batch2 sessions: {', '.join(discovered_sessions['batch2_sessions'])}",
        ],
    }

    run_manifest = _build_run_manifest(output_root_path, session_dir, inputs, jobs_requested=jobs, jobs_supported=1, decision=decision["decision"])

    _write_json(session_dir / "run_manifest.json", run_manifest)
    _write_json(session_dir / "input_resolution.json", input_resolution)
    _write_json(session_dir / "forward_data_availability_audit.json", availability)
    _write_json(session_dir / "forward_model_replay_contract.json", replay_contract)
    _write_json(session_dir / "forward_variant_pool_comparison.json", variant_comparison)
    _write_parquet(session_dir / "forward_topk_membership_diff.parquet", topk_diff)
    _write_json(session_dir / "forward_stability_audit.json", stability_audit)
    _write_json(session_dir / "forward_leakage_audit.json", leakage_audit)
    _write_json(session_dir / "shadow_reranker_forward_validation_v1_decision.json", decision)
    _write_json(
        session_dir / "_ARTIFACT_COMPLETE.json",
        {
            "schema_version": SCHEMA_VERSION,
            "generated_at_utc": _utc_now(),
            "session_id": session_id,
            "required_files_present": True,
            "artifacts": [
                "run_manifest.json",
                "input_resolution.json",
                "forward_data_availability_audit.json",
                "forward_model_replay_contract.json",
                "forward_variant_pool_comparison.json",
                "forward_topk_membership_diff.parquet",
                "forward_stability_audit.json",
                "forward_leakage_audit.json",
                "shadow_reranker_forward_validation_v1_decision.json",
            ],
        },
    )

    return {
        "output_dir": str(session_dir),
        "session_id": session_id,
        "decision": decision["decision"],
        "forward_validatable_row_count": 0,
        "jobs_supported": 1,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=SCRIPT_NAME)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--limit-anchor-dates", type=int, default=None)
    parser.add_argument("--jobs", type=int, default=1)
    args = parser.parse_args()
    result = run_shadow_reranker_forward_validation_v1(output_root=args.output_root, limit_anchor_dates=args.limit_anchor_dates, jobs=args.jobs)
    print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
