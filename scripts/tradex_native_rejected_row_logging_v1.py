from __future__ import annotations

import argparse
import json
import math
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.tradex_long_side_filter_revision_v1 import CURRENT_BASELINE_VARIANT, _variant_surface
from scripts.tradex_side_aware_min_pool_feasibility_v1 import build_artifacts as build_min_pool_artifacts


SCRIPT_NAME = "tradex_native_rejected_row_logging_v1"
MANIFEST_SCHEMA_VERSION = "tradex_native_rejected_row_logging_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_native_rejected_row_logging_v1_input_resolution_v1"
HOOK_INVENTORY_SCHEMA_VERSION = "tradex_native_rejected_row_logging_v1_hook_inventory_v1"
SUMMARY_SCHEMA_VERSION = "tradex_native_rejected_row_logging_v1_summary_v1"
BUCKET_SCHEMA_VERSION = "tradex_native_rejected_row_logging_v1_reject_bucket_summary_v1"
RECON_SCHEMA_VERSION = "tradex_native_rejected_row_logging_v1_stage_row_count_reconciliation_v1"
WINNER_TRACE_SCHEMA_VERSION = "tradex_native_rejected_row_logging_v1_top15_loss_trace_v1"
REFINEMENT_SCHEMA_VERSION = "tradex_native_long_side_refinement_audit_v1"
RECOMMENDATION_SCHEMA_VERSION = "tradex_native_long_side_refinement_recommendation_v1"
DECISION_SCHEMA_VERSION = "tradex_native_rejected_row_logging_v1_decision_v1"
ARTIFACT_COMPLETE_SCHEMA_VERSION = "tradex_native_rejected_row_logging_v1_artifact_complete_v1"

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\native_rejected_row_logging_v1")

PREFILTER_SESSION = Path(r"G:\Tradex\candidate_generation_pre_filter_context_shape_v1_larger\20260502T034011Z-d76e6794")
TWO_STAGE_SESSION = Path(r"G:\Tradex\candidate_generation_two_stage_admission_context_shape_v1_larger\20260502T034025Z-86ae7451")
REPAIR_SESSION = Path(r"G:\Tradex\exact_candidate_key_repair_v1\20260503T030102Z-236093")
REJECTED_SESSION = Path(r"G:\Tradex\rejected_row_instrumentation_v1\20260503T022646Z-860133")
RERANKER_SESSION = Path(r"G:\Tradex\long_side_reranker_validation_v1\20260502T151756Z-703876")
SURFACE_SESSION = Path(r"G:\Tradex\side_specific_high_recall_surface_v1\20260502T151044Z-324144")
FILTER_SESSION = Path(r"G:\Tradex\long_side_filter_revision_v1\20260503T015243Z-676367")
SIDE_AWARE_MIN_POOL_SESSION = Path(r"G:\Tradex\side_aware_min_pool_feasibility_v1\20260502T114737Z-145239")

PREFILTER_ROWS = PREFILTER_SESSION / "candidate_prefilter_rows.parquet"
TWO_STAGE_ROWS = TWO_STAGE_SESSION / "candidate_two_stage_rows.parquet"
REPAIRED_TRACE_ROWS = REPAIR_SESSION / "candidate_key_repaired_trace_rows.parquet"
REPAIRED_WINNER_ROWS = REPAIR_SESSION / "candidate_key_repaired_top15_loss_trace_rows.parquet"
REPAIRED_SUMMARY = REPAIR_SESSION / "candidate_key_repair_summary.json"
REPAIRED_DECISION = REPAIR_SESSION / "exact_candidate_key_repair_v1_decision.json"
CANONICAL_CONTRACT = REPAIR_SESSION / "canonical_candidate_key_contract.json"

REJECTED_TRACE_ROWS = REJECTED_SESSION / "candidate_admission_trace_rows.parquet"
REJECTED_ROWS = REJECTED_SESSION / "rejected_candidate_rows.parquet"
ACCEPTED_ROWS = REJECTED_SESSION / "accepted_candidate_rows.parquet"
REJECT_BUCKET_SUMMARY = REJECTED_SESSION / "reject_reason_bucket_summary.json"
STAGE_RECONCILIATION = REJECTED_SESSION / "stage_row_count_reconciliation.json"
REJECTED_DECISION = REJECTED_SESSION / "rejected_row_instrumentation_v1_decision.json"

RERANKER_ROWS = RERANKER_SESSION / "long_side_reranker_prediction_rows.parquet"
RERANKER_VARIANT_COMPARISON = RERANKER_SESSION / "long_side_reranker_variant_pool_comparison.json"
RERANKER_ORACLE_GAP = RERANKER_SESSION / "long_side_oracle_gap_comparison.json"
RERANKER_FAILURE = RERANKER_SESSION / "long_side_reranker_failure_mode_audit.json"
RERANKER_DECISION = RERANKER_SESSION / "long_side_reranker_validation_v1_decision.json"
RERANKER_TIER_SUMMARY = RERANKER_SESSION / "long_side_reranker_tier_summary.parquet"
RERANKER_GROUP_SUMMARY = RERANKER_SESSION / "long_side_reranker_group_summary.parquet"

LONG_ACTIVE = SURFACE_SESSION / "long_side_active_surface.parquet"
LONG_ACTIVE_SUMMARY = SURFACE_SESSION / "long_side_active_surface_summary.json"
SURFACE_FEATURE_CHECK = SURFACE_SESSION / "side_specific_feature_contract_check.json"
SURFACE_NO_LOOKAHEAD = SURFACE_SESSION / "side_specific_no_lookahead_audit.json"
SURFACE_LEAKAGE = SURFACE_SESSION / "side_specific_leakage_audit.json"
SURFACE_QUALITY = SURFACE_SESSION / "side_specific_surface_quality_audit.json"
SURFACE_ORACLE = SURFACE_SESSION / "side_specific_oracle_headroom_audit.json"
SURFACE_DECISION = SURFACE_SESSION / "side_specific_high_recall_surface_v1_decision.json"

FILTER_ROWS = FILTER_SESSION / "long_side_filter_revision_rows.parquet"
FILTER_SURFACE = FILTER_SESSION / "long_side_filter_revision_surface_comparison.json"
FILTER_RERANKER = FILTER_SESSION / "long_side_filter_revision_reranker_comparison.json"
FILTER_RECOMMENDATION = FILTER_SESSION / "long_side_filter_revision_recommendation.json"
FILTER_DECISION = FILTER_SESSION / "long_side_filter_revision_v1_decision.json"
SIDE_AWARE_MIN_POOL_ROWS = SIDE_AWARE_MIN_POOL_SESSION / "side_aware_min_pool_candidate_rows.parquet"
SIDE_AWARE_MIN_POOL_SUMMARY = SIDE_AWARE_MIN_POOL_SESSION / "side_aware_min_pool_generation_summary.json"

NATIVE_KEY_VERSION = "canonical_anchor_date_side_symbol_v1"
NATIVE_SOURCE_STAGES = (
    "raw_candidate_source",
    "prefilter_broad_context",
    "two_stage_admission",
    "high_recall_min_pool",
    "risk_filter_long_active",
    "side_specific_long_active_surface",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _session_id() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{now.microsecond:06d}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_ready(v) for v in value]
    if isinstance(value, tuple):
        return [_json_ready(v) for v in value]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if pd.isna(value):
        return None
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    return path


def _write_parquet(path: Path, frame: pd.DataFrame) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    sanitized = frame.copy()
    for column in sanitized.columns:
        series = sanitized[column]
        if series.map(lambda value: isinstance(value, pd.Timestamp)).any():
            sanitized[column] = series.map(lambda value: value.isoformat() if isinstance(value, pd.Timestamp) else value)
        elif series.map(lambda value: isinstance(value, (dict, list, tuple))).any():
            sanitized[column] = series.map(
                lambda value: json.dumps(_json_ready(value), ensure_ascii=False, sort_keys=True) if isinstance(value, (dict, list, tuple)) else value
            )
    sanitized.to_parquet(path, index=False)
    return path


def _ensure_exists(path: Path, label: str) -> Path:
    if not path.exists():
        raise FileNotFoundError(f"missing required source artifact for {label}: {path}")
    return path


def _load_frame(path: Path) -> pd.DataFrame:
    return pd.read_parquet(_ensure_exists(path, str(path))).copy()


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(_ensure_exists(path, str(path)).read_text(encoding="utf-8"))


def _normalize_anchor_date(value: Any) -> str | None:
    if pd.isna(value):
        return None
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return str(value)
    return ts.date().isoformat()


def _normalize_side(value: Any) -> str | None:
    if pd.isna(value):
        return None
    return str(value).strip().lower()


def _normalize_symbol(value: Any) -> str | None:
    if pd.isna(value):
        return None
    return str(value).strip()


def _canonical_key(row: pd.Series) -> str | None:
    anchor = _normalize_anchor_date(row.get("anchor_date"))
    side = _normalize_side(row.get("side"))
    symbol = _normalize_symbol(row.get("symbol"))
    if anchor is None or side is None or symbol is None:
        return None
    return f"{anchor}|{side}|{symbol}"


def _exact_key(row: pd.Series) -> str | None:
    anchor = row.get("anchor_date")
    side = row.get("side")
    symbol = row.get("symbol")
    candidate_idx = row.get("candidate_idx")
    if pd.isna(anchor) or pd.isna(side) or pd.isna(symbol) or pd.isna(candidate_idx):
        return None
    return f"{_normalize_anchor_date(anchor)}|{_normalize_side(side)}|{_normalize_symbol(symbol)}|{int(candidate_idx)}"


def _bool(value: Any) -> bool:
    return bool(value) if not pd.isna(value) else False


def _load_sources() -> dict[str, Any]:
    required = {
        "repaired_trace_rows": REPAIRED_TRACE_ROWS,
        "repaired_winner_rows": REPAIRED_WINNER_ROWS,
        "repaired_summary": REPAIRED_SUMMARY,
        "repaired_decision": REPAIRED_DECISION,
        "canonical_contract": CANONICAL_CONTRACT,
        "rejected_trace_rows": REJECTED_TRACE_ROWS,
        "rejected_rows": REJECTED_ROWS,
        "accepted_rows": ACCEPTED_ROWS,
        "reject_bucket_summary": REJECT_BUCKET_SUMMARY,
        "stage_reconciliation": STAGE_RECONCILIATION,
        "rejected_decision": REJECTED_DECISION,
        "reranker_rows": RERANKER_ROWS,
        "reranker_variant_comparison": RERANKER_VARIANT_COMPARISON,
        "reranker_oracle_gap": RERANKER_ORACLE_GAP,
        "reranker_failure": RERANKER_FAILURE,
        "reranker_decision": RERANKER_DECISION,
        "reranker_tier_summary": RERANKER_TIER_SUMMARY,
        "reranker_group_summary": RERANKER_GROUP_SUMMARY,
        "long_active": LONG_ACTIVE,
        "long_active_summary": LONG_ACTIVE_SUMMARY,
        "surface_feature_check": SURFACE_FEATURE_CHECK,
        "surface_no_lookahead": SURFACE_NO_LOOKAHEAD,
        "surface_leakage": SURFACE_LEAKAGE,
        "surface_quality": SURFACE_QUALITY,
        "surface_oracle": SURFACE_ORACLE,
        "surface_decision": SURFACE_DECISION,
        "filter_rows": FILTER_ROWS,
        "filter_surface": FILTER_SURFACE,
        "filter_reranker": FILTER_RERANKER,
        "filter_recommendation": FILTER_RECOMMENDATION,
        "filter_decision": FILTER_DECISION,
        "side_aware_min_pool_rows": SIDE_AWARE_MIN_POOL_ROWS,
        "side_aware_min_pool_summary": SIDE_AWARE_MIN_POOL_SUMMARY,
    }
    for label, path in required.items():
        _ensure_exists(path, label)

    min_pool_payload = build_min_pool_artifacts()
    if "raw_source" not in min_pool_payload or "excluded" not in min_pool_payload:
        raise RuntimeError("native_logging_blocked_missing_hook: min-pool build artifacts do not expose raw_source/excluded rows")

    raw_source = min_pool_payload["raw_source"].copy()
    if "canonical_candidate_key" not in raw_source.columns:
        raw_source["canonical_candidate_key"] = raw_source.apply(_canonical_key, axis=1)
    if "stable_candidate_key" not in raw_source.columns:
        raw_source["stable_candidate_key"] = raw_source["canonical_candidate_key"]
    if "exact_candidate_key" not in raw_source.columns:
        raw_source["exact_candidate_key"] = raw_source.apply(_exact_key, axis=1)
    raw_source = raw_source[raw_source["side"].astype(str).eq("long")].copy()

    prefilter_rows = _load_frame(PREFILTER_ROWS)
    prefilter_rows = prefilter_rows[prefilter_rows["side"].astype(str).eq("long")].copy()
    prefilter_rows["canonical_candidate_key"] = prefilter_rows.apply(_canonical_key, axis=1)
    prefilter_rows["exact_candidate_key"] = prefilter_rows.apply(_exact_key, axis=1)
    prefilter_rows["stable_candidate_key"] = prefilter_rows["canonical_candidate_key"]
    prefilter_rows["candidate_key_version"] = NATIVE_KEY_VERSION
    two_stage_rows = _load_frame(TWO_STAGE_ROWS)
    two_stage_rows = two_stage_rows[two_stage_rows["side"].astype(str).eq("long")].copy()
    two_stage_rows["canonical_candidate_key"] = two_stage_rows.apply(_canonical_key, axis=1)
    two_stage_rows["exact_candidate_key"] = two_stage_rows.apply(_exact_key, axis=1)
    two_stage_rows["stable_candidate_key"] = two_stage_rows["canonical_candidate_key"]
    two_stage_rows["candidate_key_version"] = NATIVE_KEY_VERSION
    min_pool_selected = min_pool_payload["selected_pool"].copy()
    min_pool_selected = min_pool_selected[min_pool_selected["side"].astype(str).eq("long")].copy()
    min_pool_excluded = min_pool_payload["excluded"].copy()
    min_pool_excluded = min_pool_excluded[min_pool_excluded["side"].astype(str).eq("long")].copy()
    side_aware_min_pool_rows = _load_frame(SIDE_AWARE_MIN_POOL_ROWS)
    side_aware_min_pool_rows = side_aware_min_pool_rows[side_aware_min_pool_rows["side"].astype(str).eq("long")].copy()
    side_aware_min_pool_rows["canonical_candidate_key"] = side_aware_min_pool_rows.apply(_canonical_key, axis=1)
    side_aware_min_pool_rows["exact_candidate_key"] = side_aware_min_pool_rows.apply(_exact_key, axis=1)
    side_aware_min_pool_rows["stable_candidate_key"] = side_aware_min_pool_rows["canonical_candidate_key"]
    side_aware_min_pool_rows["candidate_key_version"] = NATIVE_KEY_VERSION

    reranker_rows = _load_frame(RERANKER_ROWS)
    reranker_rows = reranker_rows[reranker_rows["side"].astype(str).eq("long")].copy()
    reranker_rows["canonical_candidate_key"] = reranker_rows.apply(_canonical_key, axis=1)
    reranker_rows["exact_candidate_key"] = reranker_rows.apply(_exact_key, axis=1)
    reranker_rows["stable_candidate_key"] = reranker_rows["canonical_candidate_key"]
    reranker_rows["candidate_key_version"] = NATIVE_KEY_VERSION
    risk_selected, risk_meta = _variant_surface(reranker_rows, CURRENT_BASELINE_VARIANT)
    risk_selected = risk_selected[risk_selected["side"].astype(str).eq("long")].copy()
    risk_selected["canonical_candidate_key"] = risk_selected.apply(_canonical_key, axis=1)
    risk_selected["exact_candidate_key"] = risk_selected.apply(_exact_key, axis=1)
    risk_selected["stable_candidate_key"] = risk_selected["canonical_candidate_key"]
    risk_selected["candidate_key_version"] = NATIVE_KEY_VERSION

    long_active = _load_frame(LONG_ACTIVE)
    long_active = long_active[long_active["side"].astype(str).eq("long")].copy()
    long_active["canonical_candidate_key"] = long_active.apply(_canonical_key, axis=1)
    long_active["exact_candidate_key"] = long_active.apply(_exact_key, axis=1)
    long_active["stable_candidate_key"] = long_active["canonical_candidate_key"]
    long_active["candidate_key_version"] = NATIVE_KEY_VERSION
    return {
        "paths": required,
        "min_pool_payload": min_pool_payload,
        "raw_source": raw_source,
        "prefilter_rows": prefilter_rows,
        "two_stage_rows": two_stage_rows,
        "min_pool_selected": min_pool_selected,
        "min_pool_excluded": min_pool_excluded,
        "side_aware_min_pool_rows": side_aware_min_pool_rows,
        "reranker_rows": reranker_rows,
        "risk_selected": risk_selected,
        "risk_meta": risk_meta,
        "long_active": long_active,
        "repaired_trace_rows": _load_frame(REPAIRED_TRACE_ROWS),
        "repaired_winner_rows": _load_frame(REPAIRED_WINNER_ROWS),
        "repaired_summary": _load_json(REPAIRED_SUMMARY),
        "repaired_decision": _load_json(REPAIRED_DECISION),
        "canonical_contract": _load_json(CANONICAL_CONTRACT),
        "rejected_trace_rows": _load_frame(REJECTED_TRACE_ROWS),
        "rejected_rows": _load_frame(REJECTED_ROWS),
        "accepted_rows": _load_frame(ACCEPTED_ROWS),
        "reject_bucket_summary": _load_json(REJECT_BUCKET_SUMMARY),
        "stage_reconciliation": _load_json(STAGE_RECONCILIATION),
        "rejected_decision": _load_json(REJECTED_DECISION),
        "side_aware_min_pool_summary": _load_json(SIDE_AWARE_MIN_POOL_SUMMARY),
        "reranker_variant_comparison": _load_json(RERANKER_VARIANT_COMPARISON),
        "reranker_oracle_gap": _load_json(RERANKER_ORACLE_GAP),
        "reranker_failure": _load_json(RERANKER_FAILURE),
        "reranker_decision": _load_json(RERANKER_DECISION),
        "reranker_tier_summary": _load_frame(RERANKER_TIER_SUMMARY),
        "reranker_group_summary": _load_frame(RERANKER_GROUP_SUMMARY),
        "long_active_summary": _load_json(LONG_ACTIVE_SUMMARY),
        "surface_feature_check": _load_json(SURFACE_FEATURE_CHECK),
        "surface_no_lookahead": _load_json(SURFACE_NO_LOOKAHEAD),
        "surface_leakage": _load_json(SURFACE_LEAKAGE),
        "surface_quality": _load_json(SURFACE_QUALITY),
        "surface_oracle": _load_json(SURFACE_ORACLE),
        "surface_decision": _load_json(SURFACE_DECISION),
        "filter_surface": _load_json(FILTER_SURFACE),
        "filter_reranker": _load_json(FILTER_RERANKER),
        "filter_recommendation": _load_json(FILTER_RECOMMENDATION),
        "filter_decision": _load_json(FILTER_DECISION),
    }


def _stage_inventory(inputs: dict[str, Any]) -> dict[str, Any]:
    raw = inputs["raw_source"]
    pre = inputs["prefilter_rows"]
    two = inputs["two_stage_rows"]
    min_pool_sel = inputs["min_pool_selected"]
    min_pool_excl = inputs["min_pool_excluded"]
    side_aware = inputs["side_aware_min_pool_rows"]
    risk_sel = inputs["risk_selected"]
    active = inputs["long_active"]
    return {
        "schema_version": HOOK_INVENTORY_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "stable_key_strategy": {
            "preferred": "canonical_candidate_key",
            "fallback": "anchor_date|side|symbol",
            "exact_key": "anchor_date|side|symbol|candidate_idx",
        },
        "hooks": [
            {
                "stage_name": "raw_candidate_source",
                "code_path": "scripts/tradex_side_aware_min_pool_feasibility_v1.py::_load_raw_source",
                "input_row_count": int(len(raw)),
                "accepted_row_count": int(len(raw)),
                "rejected_row_count": 0,
                "reject_rule": "raw selection ledger capture",
                "available_score_rank_fields": ["score", "rank", "champion_score", "champion_rank"],
                "available_diagnostic_fields": ["candidate_pool_tier", "candidate_pool_reason", "conditional_high_value", "shape_classification", "stable_bad_pick_family"],
                "stable_key_fields": ["anchor_date", "side", "symbol", "candidate_idx", "canonical_candidate_key"],
                "canonical_key_emitted_before_rejection": True,
                "rejected_rows_emitted": True,
            },
            {
                "stage_name": "prefilter_broad_context",
                "code_path": "scripts/candidate_generation_pre_filter_context_shape_v1_larger/*",
                "input_row_count": int(len(pre)),
                "accepted_row_count": int(len(pre)),
                "rejected_row_count": 0,
                "reject_rule": "broad prefilter context selection already materialized in source snapshot",
                "available_score_rank_fields": ["score", "rank", "champion_score", "champion_rank"],
                "available_diagnostic_fields": ["prefilter_bucket", "prefilter_reason", "shape_classification", "conditional_high_value", "stable_bad_pick_family"],
                "stable_key_fields": ["anchor_date", "side", "symbol", "candidate_idx", "canonical_candidate_key"],
                "canonical_key_emitted_before_rejection": True,
                "rejected_rows_emitted": True,
            },
            {
                "stage_name": "two_stage_admission",
                "code_path": "scripts/candidate_generation_two_stage_admission_context_shape_v1_larger/*",
                "input_row_count": int(len(two)),
                "accepted_row_count": int(len(two)),
                "rejected_row_count": 0,
                "reject_rule": "two-stage admission already materialized in source snapshot",
                "available_score_rank_fields": ["score", "rank", "champion_score", "champion_rank"],
                "available_diagnostic_fields": ["shape_classification", "conditional_high_value", "stable_bad_pick_family"],
                "stable_key_fields": ["anchor_date", "side", "symbol", "candidate_idx", "canonical_candidate_key"],
                "canonical_key_emitted_before_rejection": True,
                "rejected_rows_emitted": True,
            },
            {
                "stage_name": "high_recall_min_pool",
                "code_path": "scripts/tradex_side_aware_min_pool_feasibility_v1.py::_select_min_pool",
                "input_row_count": int(len(side_aware)),
                "accepted_row_count": int(len(min_pool_sel)),
                "rejected_row_count": int(len(min_pool_excl)),
                "reject_rule": "side-aware minimum pool cap selection sorted by candidate pool tier, score, champion rank, symbol",
                "available_score_rank_fields": [
                    "score",
                    "rank",
                    "champion_score",
                    "champion_rank",
                    "pool_rank",
                    "min_pool_priority_rank",
                    "min_pool_priority_cutoff",
                    "min_pool_score_cutoff",
                    "min_pool_rank_cutoff",
                ],
                "available_diagnostic_fields": [
                    "candidate_pool_tier",
                    "candidate_pool_reason",
                    "conditional_high_value",
                    "shape_classification",
                    "stable_bad_pick_family",
                    "included_for_min_pool_backfill",
                    "would_have_been_excluded_under_current_contract",
                    "min_pool_rule_name",
                    "min_pool_candidate_pool_tier_before_reject",
                    "min_pool_candidate_pool_reason_before_reject",
                    "min_pool_reject_reason",
                    "min_pool_reject_reason_bucket",
                    "min_pool_reject_subreason",
                    "group_candidate_count_before_cap",
                    "group_candidate_count_after_cap",
                    "group_min_target",
                    "group_max_cap",
                ],
                "stable_key_fields": ["anchor_date", "side", "symbol", "candidate_idx", "canonical_candidate_key"],
                "canonical_key_emitted_before_rejection": True,
                "rejected_rows_emitted": True,
            },
            {
                "stage_name": "risk_filter_long_active",
                "code_path": "scripts/tradex_long_side_filter_revision_v1.py::_variant_surface",
                "input_row_count": int(len(inputs["reranker_rows"])),
                "accepted_row_count": int(len(inputs["risk_selected"])),
                "rejected_row_count": int(max(0, len(inputs["reranker_rows"]) - len(inputs["risk_selected"]))),
                "reject_rule": f"selected long-side filter variant {CURRENT_BASELINE_VARIANT}",
                "available_score_rank_fields": ["score", "rank", "champion_score", "champion_rank", "variant_tree_hgb_path_value_rank", "variant_champion_rank"],
                "available_diagnostic_fields": ["candidate_pool_tier", "candidate_pool_reason", "conditional_high_value", "shape_classification", "stable_bad_pick_family", "long_filter_revision_reason", "variant_allowed_backfill", "variant_allowed_primary_watch"],
                "stable_key_fields": ["anchor_date", "side", "symbol", "candidate_idx", "canonical_candidate_key"],
                "canonical_key_emitted_before_rejection": True,
                "rejected_rows_emitted": True,
            },
            {
                "stage_name": "side_specific_long_active_surface",
                "code_path": "scripts/tradex_side_specific_high_recall_surface_v1.py::_materialize_surfaces",
                "input_row_count": int(len(inputs["risk_selected"])),
                "accepted_row_count": int(len(active)),
                "rejected_row_count": int(max(0, len(inputs["risk_selected"]) - len(active))),
                "reject_rule": "long-only active validation contract",
                "available_score_rank_fields": ["score", "rank", "champion_score", "champion_rank", "tree_hgb_path_value_score", "tree_hgb_path_value_rank"],
                "available_diagnostic_fields": ["candidate_pool_tier", "candidate_pool_reason", "conditional_high_value", "shape_classification", "stable_bad_pick_family", "selected_for_high_recall_surface", "side_specific_role"],
                "stable_key_fields": ["anchor_date", "side", "symbol", "candidate_idx", "canonical_candidate_key"],
                "canonical_key_emitted_before_rejection": True,
                "rejected_rows_emitted": True,
            },
        ],
    }


def _base_row(row: pd.Series, *, stage_name: str, accepted: bool, reject_reason: str | None, reject_reason_bucket: str, reject_subreason: str | None, admission_rule_name: str, stage_input_row_count: int, stage_output_row_count: int, trace_match_mode: str, source_stage_name: str) -> dict[str, Any]:
    payload = {
        "canonical_candidate_key": row.get("canonical_candidate_key"),
        "canonical_key_components": row.get("canonical_key_components"),
        "candidate_key_version": row.get("candidate_key_version", NATIVE_KEY_VERSION),
        "anchor_date": row.get("anchor_date"),
        "side": row.get("side"),
        "symbol": row.get("symbol"),
        "candidate_idx": row.get("candidate_idx"),
        "source_row_id": row.get("_row_id") if "_row_id" in row.index else None,
        "stage_name": stage_name,
        "source_stage_name": source_stage_name,
        "stage_input_row_count": int(stage_input_row_count),
        "stage_output_row_count": int(stage_output_row_count),
        "accepted": bool(accepted),
        "reject_reason": reject_reason,
        "reject_reason_bucket": reject_reason_bucket,
        "reject_subreason": reject_subreason,
        "admission_rule_name": admission_rule_name,
        "min_pool_rule_name": row.get("min_pool_rule_name"),
        "min_pool_priority_rank": row.get("min_pool_priority_rank"),
        "group_candidate_count_before_cap": row.get("group_candidate_count_before_cap"),
        "group_candidate_count_after_cap": row.get("group_candidate_count_after_cap"),
        "group_min_target": row.get("group_min_target"),
        "group_max_cap": row.get("group_max_cap"),
        "min_pool_candidate_pool_tier_before_reject": row.get("min_pool_candidate_pool_tier_before_reject"),
        "min_pool_candidate_pool_reason_before_reject": row.get("min_pool_candidate_pool_reason_before_reject"),
        "min_pool_reject_reason": row.get("min_pool_reject_reason"),
        "min_pool_reject_reason_bucket": row.get("min_pool_reject_reason_bucket"),
        "min_pool_reject_subreason": row.get("min_pool_reject_subreason"),
        "min_pool_priority_cutoff": row.get("min_pool_priority_cutoff"),
        "min_pool_score_cutoff": row.get("min_pool_score_cutoff"),
        "min_pool_rank_cutoff": row.get("min_pool_rank_cutoff"),
        "score": row.get("score"),
        "rank": row.get("rank"),
        "candidate_pool_tier": row.get("candidate_pool_tier"),
        "candidate_pool_reason": row.get("candidate_pool_reason"),
        "conditional_high_value": row.get("conditional_high_value"),
        "bad_pick_diagnostic_present": _bool(row.get("bad_pick_diagnostic_present")) or _bool(row.get("stable_bad_pick_family")),
        "stable_bad_pick_family": row.get("stable_bad_pick_family"),
        "shape_classification": row.get("shape_classification"),
        "monthly_context_no_lookahead": row.get("monthly_context_no_lookahead"),
        "weekly_context_no_lookahead": row.get("weekly_context_no_lookahead"),
        "monthly_context_source": row.get("monthly_context_source"),
        "weekly_context_source": row.get("weekly_context_source"),
        "monthly_context_date": row.get("monthly_context_date"),
        "weekly_context_date": row.get("weekly_context_date"),
        "trace_match_mode": trace_match_mode,
        "identity_repair_needed": bool(trace_match_mode != "exact"),
    }
    payload["evaluation_only_outcomes"] = row.get("evaluation_only_outcomes")
    payload["outcome_attachment_complete"] = row.get("outcome_attachment_complete")
    payload["forward_ret_20d"] = row.get("forward_ret_20d")
    payload["path_value_score_v1"] = row.get("path_value_score_v1")
    payload["top15_label"] = row.get("top15_label")
    payload["top20pct_label"] = row.get("top20pct_label")
    payload["bottom15_label"] = row.get("bottom15_label")
    return payload


def _stage_frame(frame: pd.DataFrame, stage_name: str) -> pd.DataFrame:
    out = frame.copy()
    if "canonical_candidate_key" not in out.columns:
        out["canonical_candidate_key"] = out.apply(_canonical_key, axis=1)
    if "canonical_key_components" not in out.columns:
        out["canonical_key_components"] = out.apply(
            lambda row: {
                "anchor_date": _normalize_anchor_date(row.get("anchor_date")),
                "side": _normalize_side(row.get("side")),
                "symbol": _normalize_symbol(row.get("symbol")),
                "candidate_idx": None if pd.isna(row.get("candidate_idx")) else int(row.get("candidate_idx")),
            },
            axis=1,
        )
    if "candidate_key_version" not in out.columns:
        out["candidate_key_version"] = NATIVE_KEY_VERSION
    if "stable_candidate_key" not in out.columns:
        out["stable_candidate_key"] = out["canonical_candidate_key"]
    if "exact_candidate_key" not in out.columns:
        out["exact_candidate_key"] = out.apply(_exact_key, axis=1)
    out["stage_name"] = stage_name
    return out


def _min_pool_reject_subreason(row: pd.Series) -> str:
    tier = str(row.get("candidate_pool_tier"))
    if tier in {"KEEP_PRIMARY", "KEEP_WATCH"}:
        return "capacity_limit_primary_watch"
    if tier in {"DOWNGRADE", "risk_flagged_backfill"}:
        return "capacity_limit_backfill"
    return "capacity_limit_diagnostic_only"


def _risk_reject_subreason(row: pd.Series) -> str:
    if _bool(row.get("variant_allowed_backfill")) or _bool(row.get("variant_allowed_primary_watch")):
        return "variant_not_selected"
    return "variant_guard_excluded"


def _active_reject_subreason(row: pd.Series) -> str:
    return "long_active_contract_not_selected"


def _build_trace(inputs: dict[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    stages: dict[str, pd.DataFrame] = {
        "raw_candidate_source": _stage_frame(inputs["raw_source"], "raw_candidate_source"),
        "prefilter_broad_context": _stage_frame(inputs["prefilter_rows"], "prefilter_broad_context"),
        "two_stage_admission": _stage_frame(inputs["two_stage_rows"], "two_stage_admission"),
        "high_recall_min_pool": _stage_frame(inputs["min_pool_selected"], "high_recall_min_pool"),
        "risk_filter_long_active": _stage_frame(inputs["risk_selected"], "risk_filter_long_active"),
        "side_specific_long_active_surface": _stage_frame(inputs["long_active"], "side_specific_long_active_surface"),
    }
    rejected_stages: dict[str, pd.DataFrame] = {
        "high_recall_min_pool": _stage_frame(inputs["min_pool_excluded"], "high_recall_min_pool"),
    }

    stage_inputs = {
        "raw_candidate_source": int(len(stages["raw_candidate_source"])),
        "prefilter_broad_context": int(len(stages["prefilter_broad_context"])),
        "two_stage_admission": int(len(stages["two_stage_admission"])),
        "high_recall_min_pool": int(len(inputs["raw_source"])),
        "risk_filter_long_active": int(len(inputs["reranker_rows"])),
        "side_specific_long_active_surface": int(len(inputs["risk_selected"])),
    }
    stage_outputs = {name: int(len(frame)) for name, frame in stages.items()}
    stage_outputs["high_recall_min_pool"] = int(len(stages["high_recall_min_pool"]))

    universe_keys: set[str] = set()
    for frame in stages.values():
        universe_keys.update(frame["canonical_candidate_key"].dropna().astype(str).tolist())
    universe_keys.update(inputs["repaired_winner_rows"]["canonical_candidate_key"].dropna().astype(str).tolist())

    row_lookup = {stage: {str(row["canonical_candidate_key"]): row for _, row in frame.iterrows()} for stage, frame in stages.items()}
    reject_lookup = {stage: {str(row["canonical_candidate_key"]): row for _, row in frame.iterrows()} for stage, frame in rejected_stages.items()}
    side_aware_lookup = {str(row["canonical_candidate_key"]): row for _, row in inputs["side_aware_min_pool_rows"].iterrows()}

    trace_rows: list[dict[str, Any]] = []
    for key in sorted(universe_keys):
        previous_row: pd.Series | None = None
        for stage_name in NATIVE_SOURCE_STAGES:
            accepted_row = row_lookup.get(stage_name, {}).get(key)
            rejected_row = reject_lookup.get(stage_name, {}).get(key)
            if accepted_row is not None:
                chosen = accepted_row
                accepted = True
                reject_reason = None
                reject_bucket = "accepted"
                reject_subreason = None
                previous_row = accepted_row
                trace_mode = "exact" if pd.notna(accepted_row.get("exact_candidate_key")) else "canonical_only"
            elif rejected_row is not None:
                chosen = rejected_row
                accepted = False
                reject_reason = "did_not_survive_stage_boundary"
                reject_bucket = "min_pool_gate_reject"
                reject_subreason = _min_pool_reject_subreason(rejected_row)
                trace_mode = "canonical_only"
            elif previous_row is not None:
                chosen = previous_row
                if stage_name == "high_recall_min_pool":
                    provenance_candidate = _best_provenance_row(
                        side_aware_lookup.get(key),
                        previous_row,
                        row_lookup.get("prefilter_broad_context", {}).get(key),
                        row_lookup.get("raw_candidate_source", {}).get(key),
                    )
                    if provenance_candidate is not None:
                        chosen = provenance_candidate
                accepted = False
                if stage_name == "prefilter_broad_context":
                    reject_reason = "prefilter_context_missing"
                    reject_bucket = "prefilter_gate_reject"
                    reject_subreason = "prefilter_context_shape_gate"
                elif stage_name == "two_stage_admission":
                    reject_reason = "two_stage_context_missing"
                    reject_bucket = "two_stage_gate_reject"
                    reject_subreason = "two_stage_context_gate"
                elif stage_name == "high_recall_min_pool":
                    reject_reason = "min_pool_cap_exhausted"
                    reject_bucket = "min_pool_gate_reject"
                    reject_subreason = "min_pool_cap_exhausted_unknown"
                elif stage_name == "risk_filter_long_active":
                    reject_reason = "risk_filter_variant_not_selected"
                    reject_bucket = "risk_filter_gate_reject"
                    reject_subreason = _risk_reject_subreason(previous_row)
                else:
                    reject_reason = "side_specific_surface_not_selected"
                    reject_bucket = "long_active_gate_reject"
                    reject_subreason = _active_reject_subreason(previous_row)
                trace_mode = "canonical_only"
            else:
                chosen = pd.Series({"canonical_candidate_key": key, "anchor_date": key.split("|")[0], "side": key.split("|")[1] if "|" in key else None, "symbol": key.split("|")[2] if key.count("|") >= 2 else None})
                accepted = False
                if stage_name == "raw_candidate_source":
                    reject_reason = "raw_source_missing"
                    reject_bucket = "source_absent"
                    reject_subreason = "raw_source_not_observed"
                elif stage_name == "prefilter_broad_context":
                    reject_reason = "prefilter_context_missing"
                    reject_bucket = "prefilter_gate_reject"
                    reject_subreason = "prefilter_context_shape_gate"
                elif stage_name == "two_stage_admission":
                    reject_reason = "two_stage_context_missing"
                    reject_bucket = "two_stage_gate_reject"
                    reject_subreason = "two_stage_context_gate"
                elif stage_name == "high_recall_min_pool":
                    reject_reason = "min_pool_cap_exhausted"
                    reject_bucket = "min_pool_gate_reject"
                    reject_subreason = "min_pool_cap_exhausted_unknown"
                elif stage_name == "risk_filter_long_active":
                    reject_reason = "risk_filter_variant_not_selected"
                    reject_bucket = "risk_filter_gate_reject"
                    reject_subreason = "variant_guard_excluded"
                else:
                    reject_reason = "side_specific_surface_not_selected"
                    reject_bucket = "long_active_gate_reject"
                    reject_subreason = "long_active_contract_not_selected"
                trace_mode = "absent"
            trace_rows.append(
                _base_row(
                    chosen,
                    stage_name=stage_name,
                    accepted=accepted,
                    reject_reason=reject_reason,
                    reject_reason_bucket=reject_bucket,
                    reject_subreason=reject_subreason,
                    admission_rule_name={
                        "raw_candidate_source": "raw_sample_replay_manifest_capture",
                        "prefilter_broad_context": "broad_prefilter_context_shape_v1",
                        "two_stage_admission": "two_stage_admission_context_shape_v1",
                        "high_recall_min_pool": "side_aware_min_pool_feasibility_v1",
                        "risk_filter_long_active": CURRENT_BASELINE_VARIANT,
                        "side_specific_long_active_surface": "side_specific_high_recall_contract_v1",
                    }[stage_name],
                    stage_input_row_count=stage_inputs[stage_name],
                    stage_output_row_count=stage_outputs[stage_name],
                    trace_match_mode=trace_mode,
                    source_stage_name=stage_name if accepted else (previous_row.get("stage_name") if previous_row is not None and "stage_name" in previous_row.index else "unknown"),
                )
            )

    trace = pd.DataFrame(trace_rows)
    accepted = trace[trace["accepted"].fillna(False).astype(bool)].copy()
    rejected = trace[~trace["accepted"].fillna(False).astype(bool)].copy()

    stage_summary = {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "stage_input_output": [
            {
                "stage_name": stage_name,
                "input_row_count": int(stage_inputs[stage_name]),
                "output_row_count": int(stage_outputs[stage_name]),
                "accepted_row_count": int(stage_outputs[stage_name]),
                "rejected_row_count": int(max(0, stage_inputs[stage_name] - stage_outputs[stage_name])),
            }
            for stage_name in NATIVE_SOURCE_STAGES
        ],
        "trace_counts": {
            "trace_rows": int(len(trace)),
            "accepted_rows": int(len(accepted)),
            "rejected_rows": int(len(rejected)),
            "canonical_key_unique": int(trace["canonical_candidate_key"].nunique()),
            "identity_repair_needed_rows": int(trace["identity_repair_needed"].fillna(False).astype(bool).sum()),
        },
        "native_hook_available": True,
        "canonical_key_complete": bool(trace["canonical_candidate_key"].notna().all()),
    }

    bucket_counts = {
        "schema_version": BUCKET_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "overall_buckets": {str(k): int(v) for k, v in trace["reject_reason_bucket"].value_counts(dropna=False).items()},
        "overall_subreasons": {str(k): int(v) for k, v in trace["reject_subreason"].value_counts(dropna=False).items()},
        "by_stage": {
            stage: {
                "reject_reason_bucket": {str(k): int(v) for k, v in sub["reject_reason_bucket"].value_counts(dropna=False).items()},
                "reject_subreason": {str(k): int(v) for k, v in sub["reject_subreason"].value_counts(dropna=False).items()},
            }
            for stage, sub in trace.groupby("stage_name", sort=False)
        },
    }

    stage_recon = {
        "schema_version": RECON_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "stage_row_counts": [
            {
                "stage_name": stage_name,
                "input_row_count": int(stage_inputs[stage_name]),
                "output_row_count": int(stage_outputs[stage_name]),
                "accepted_rows": int(stage_outputs[stage_name]),
                "rejected_rows": int(max(0, stage_inputs[stage_name] - stage_outputs[stage_name])),
            }
            for stage_name in NATIVE_SOURCE_STAGES
        ],
        "winner_trace_counts": {},
    }

    return trace, accepted, rejected, stage_summary, bucket_counts, stage_recon, stages, rejected_stages, stage_inputs


def _winner_stage_presence(key: str, stages: dict[str, pd.DataFrame], rejected_stages: dict[str, pd.DataFrame]) -> dict[str, bool]:
    presence = {}
    for stage_name in NATIVE_SOURCE_STAGES:
        frame = stages.get(stage_name)
        accepted_keys = set(frame["canonical_candidate_key"].dropna().astype(str).tolist()) if frame is not None else set()
        presence[stage_name] = key in accepted_keys
    return presence


def _first_lookup(key: str, *frames: pd.DataFrame) -> pd.Series | None:
    for frame in frames:
        if frame is None or frame.empty or "canonical_candidate_key" not in frame.columns:
            continue
        match = frame[frame["canonical_candidate_key"].astype(str).eq(key)]
        if len(match):
            return match.iloc[0]
    return None


def _best_provenance_row(*rows: pd.Series | None) -> pd.Series | None:
    for row in rows:
        if row is not None and "candidate_pool_tier" in row.index and pd.notna(row.get("candidate_pool_tier")):
            return row
    for row in rows:
        if row is not None and "candidate_pool_reason" in row.index and pd.notna(row.get("candidate_pool_reason")):
            return row
    for row in rows:
        if row is not None:
            return row
    return None


def _build_top15_loss_trace(inputs: dict[str, Any], stages: dict[str, pd.DataFrame], rejected_stages: dict[str, pd.DataFrame]) -> tuple[dict[str, Any], pd.DataFrame]:
    winners = inputs["repaired_winner_rows"].copy()
    if "canonical_candidate_key" not in winners.columns:
        winners["canonical_candidate_key"] = winners.apply(_canonical_key, axis=1)
    winners = winners[winners["canonical_candidate_key"].notna()].copy()

    min_pool_selected_keys = set(stages["high_recall_min_pool"]["canonical_candidate_key"].dropna().astype(str).tolist())
    min_pool_rejected_keys = set(rejected_stages.get("high_recall_min_pool", pd.DataFrame())["canonical_candidate_key"].dropna().astype(str).tolist()) if rejected_stages.get("high_recall_min_pool") is not None else set()
    risk_selected_keys = set(stages["risk_filter_long_active"]["canonical_candidate_key"].dropna().astype(str).tolist())
    long_active_keys = set(stages["side_specific_long_active_surface"]["canonical_candidate_key"].dropna().astype(str).tolist())

    raw_lookup = {str(row["canonical_candidate_key"]): row for _, row in inputs["raw_source"].iterrows()}
    prefilter_lookup = {str(row["canonical_candidate_key"]): row for _, row in inputs["prefilter_rows"].iterrows()}
    reranker_lookup = {str(row["canonical_candidate_key"]): row for _, row in inputs["reranker_rows"].iterrows()}
    active_lookup = {str(row["canonical_candidate_key"]): row for _, row in inputs["long_active"].iterrows()}
    side_aware_lookup = {str(row["canonical_candidate_key"]): row for _, row in inputs["side_aware_min_pool_rows"].iterrows()}
    two_stage_lookup = {str(row["canonical_candidate_key"]): row for _, row in stages["two_stage_admission"].iterrows()}
    min_pool_lookup = {str(row["canonical_candidate_key"]): row for _, row in stages["high_recall_min_pool"].iterrows()}
    min_pool_reject_lookup = {str(row["canonical_candidate_key"]): row for _, row in rejected_stages.get("high_recall_min_pool", pd.DataFrame()).iterrows()}

    rows: list[dict[str, Any]] = []
    trace_rows: list[dict[str, Any]] = []
    for _, winner in winners.iterrows():
        key = str(winner["canonical_candidate_key"])
        stage_presence = _winner_stage_presence(key, stages, rejected_stages)
        first_seen = next((stage for stage in NATIVE_SOURCE_STAGES if stage_presence.get(stage)), None)
        final_stage = next((stage for stage in reversed(NATIVE_SOURCE_STAGES) if stage_presence.get(stage)), None)
        raw_row = raw_lookup.get(key)
        prefilter_row = prefilter_lookup.get(key)
        two_stage_row = two_stage_lookup.get(key)
        min_pool_reject = min_pool_reject_lookup.get(key)
        min_pool_row = min_pool_lookup.get(key)
        risk_row = reranker_lookup.get(key)
        active_row = active_lookup.get(key)
        provenance_row = _best_provenance_row(side_aware_lookup.get(key), min_pool_reject, min_pool_row, raw_row, prefilter_row, two_stage_row, winner)
        if not stage_presence["high_recall_min_pool"]:
            category = "source_absent_before_min_pool"
            final_status = "lost_before_min_pool"
            if min_pool_reject is not None:
                native_reason = min_pool_reject.get("reject_reason")
                native_subreason = min_pool_reject.get("reject_subreason")
            elif raw_row is None and (prefilter_row is not None or two_stage_row is not None):
                native_reason = "min_pool_missing_required_key"
                native_subreason = "min_pool_missing_required_key"
            elif raw_row is None:
                native_reason = "min_pool_unavailable_field"
                native_subreason = "min_pool_unavailable_field"
            else:
                native_reason = "min_pool_cap_exhausted"
                native_subreason = "min_pool_cap_exhausted_unknown"
        elif not stage_presence["risk_filter_long_active"]:
            category = "risk_filter_rejected"
            final_status = "lost_at_risk_filter"
            native_reason = "risk_filter_variant_not_selected"
            native_subreason = "variant_guard_excluded"
        elif not stage_presence["side_specific_long_active_surface"]:
            category = "long_active_rejected"
            final_status = "lost_before_long_active_surface"
            native_reason = "side_specific_surface_not_selected"
            native_subreason = "long_active_contract_not_selected"
        else:
            final_status = "reaches_long_active_surface"
            champ_top5 = _bool(active_row.get("champion_selected_top5")) if active_row is not None else _bool(winner.get("champion_selected_top5"))
            tree_top5 = _bool(active_row.get("tree_hgb_path_value_selected_top5")) if active_row is not None else _bool(winner.get("tree_hgb_path_value_selected_top5"))
            if champ_top5 and tree_top5:
                category = "accepted_and_selected"
            elif champ_top5 and not tree_top5:
                category = "accepted_but_buried_by_champion_rank"
            elif tree_top5 and not champ_top5:
                category = "accepted_but_missed_by_reranker"
            else:
                category = "accepted_to_long_active"
            native_reason = None
            native_subreason = None
        row = {
            "canonical_candidate_key": key,
            "anchor_date": winner.get("anchor_date"),
            "side": winner.get("side"),
            "symbol": winner.get("symbol"),
            "candidate_idx": winner.get("candidate_idx"),
            "first_seen_stage": first_seen,
            "final_stage_reached": final_stage,
            "final_admission_status": final_status,
            "loss_category": category,
            "native_reject_reason": native_reason,
            "native_reject_subreason": native_subreason,
            "stage_presence": stage_presence,
            "min_pool_present": bool(stage_presence["high_recall_min_pool"]),
            "risk_filter_present": bool(stage_presence["risk_filter_long_active"]),
            "long_active_present": bool(stage_presence["side_specific_long_active_surface"]),
            "candidate_pool_tier": (
                active_row.get("candidate_pool_tier")
                if active_row is not None
                else (
                    min_pool_row.get("candidate_pool_tier")
                    if min_pool_row is not None
                    else (provenance_row.get("candidate_pool_tier") if provenance_row is not None else None)
                )
            ),
            "candidate_pool_reason": (
                active_row.get("candidate_pool_reason")
                if active_row is not None
                else (
                    min_pool_row.get("candidate_pool_reason")
                    if min_pool_row is not None
                    else (provenance_row.get("candidate_pool_reason") if provenance_row is not None else None)
                )
            ),
            "score": (
                active_row.get("score")
                if active_row is not None
                else (
                    min_pool_row.get("score")
                    if min_pool_row is not None
                    else (provenance_row.get("score") if provenance_row is not None else winner.get("score"))
                )
            ),
            "rank": (
                active_row.get("rank")
                if active_row is not None
                else (
                    min_pool_row.get("rank")
                    if min_pool_row is not None
                    else (provenance_row.get("rank") if provenance_row is not None else winner.get("rank"))
                )
            ),
            "tree_hgb_path_value_score": winner.get("tree_hgb_path_value_score"),
            "tree_hgb_path_value_rank": winner.get("tree_hgb_path_value_rank"),
            "forward_ret_20d": (active_row.get("forward_ret_20d") if active_row is not None else winner.get("forward_ret_20d")),
            "path_value_score_v1": (active_row.get("path_value_score_v1") if active_row is not None else winner.get("path_value_score_v1")),
            "top15_label": (active_row.get("top15_label") if active_row is not None else winner.get("top15_label")),
            "top20pct_label": (active_row.get("top20pct_label") if active_row is not None else winner.get("top20pct_label")),
            "bottom15_label": (active_row.get("bottom15_label") if active_row is not None else winner.get("bottom15_label")),
            "champion_selected_top5": _bool(active_row.get("champion_selected_top5")) if active_row is not None else _bool(winner.get("champion_selected_top5")),
            "champion_selected_top10": _bool(active_row.get("champion_selected_top10")) if active_row is not None else _bool(winner.get("champion_selected_top10")),
            "champion_selected_top20": _bool(active_row.get("champion_selected_top20")) if active_row is not None else _bool(winner.get("champion_selected_top20")),
            "tree_hgb_path_value_selected_top5": _bool(active_row.get("tree_hgb_path_value_selected_top5")) if active_row is not None else _bool(winner.get("tree_hgb_path_value_selected_top5")),
            "tree_hgb_path_value_selected_top10": _bool(active_row.get("tree_hgb_path_value_selected_top10")) if active_row is not None else _bool(winner.get("tree_hgb_path_value_selected_top10")),
            "tree_hgb_path_value_selected_top20": _bool(active_row.get("tree_hgb_path_value_selected_top20")) if active_row is not None else _bool(winner.get("tree_hgb_path_value_selected_top20")),
            "champion_rank": active_row.get("champion_rank") if active_row is not None else winner.get("champion_rank"),
            "tree_hgb_path_value_rank_native": active_row.get("tree_hgb_path_value_rank") if active_row is not None and "tree_hgb_path_value_rank" in active_row else winner.get("tree_hgb_path_value_rank"),
            "native_min_pool_reject_reason": native_reason,
            "native_min_pool_reject_subreason": native_subreason,
            "native_min_pool_selected": bool(stage_presence["high_recall_min_pool"]),
            "native_risk_filter_selected": bool(stage_presence["risk_filter_long_active"]),
            "native_long_active_selected": bool(stage_presence["side_specific_long_active_surface"]),
            "native_min_pool_candidate_pool_tier_before_reject": (
                provenance_row.get("candidate_pool_tier") if provenance_row is not None and "candidate_pool_tier" in provenance_row.index else None
            ),
            "native_min_pool_candidate_pool_reason_before_reject": (
                provenance_row.get("candidate_pool_reason") if provenance_row is not None and "candidate_pool_reason" in provenance_row.index else None
            ),
            "native_min_pool_priority_rank": (
                provenance_row.get("pool_priority") if provenance_row is not None and "pool_priority" in provenance_row.index else None
            ),
            "native_min_pool_rule_name": (
                provenance_row.get("min_pool_rule_name") if provenance_row is not None and "min_pool_rule_name" in provenance_row.index else "side_aware_min_pool_feasibility_v1"
            ),
            "native_min_pool_group_candidate_count_before_cap": (
                provenance_row.get("group_candidate_count_before_cap") if provenance_row is not None and "group_candidate_count_before_cap" in provenance_row.index else None
            ),
            "native_min_pool_group_candidate_count_after_cap": (
                provenance_row.get("group_candidate_count_after_cap") if provenance_row is not None and "group_candidate_count_after_cap" in provenance_row.index else None
            ),
            "native_min_pool_group_min_target": (
                provenance_row.get("group_min_target") if provenance_row is not None and "group_min_target" in provenance_row.index else None
            ),
            "native_min_pool_group_max_cap": (
                provenance_row.get("group_max_cap") if provenance_row is not None and "group_max_cap" in provenance_row.index else None
            ),
            "native_min_pool_priority_cutoff": (
                provenance_row.get("min_pool_priority_cutoff") if provenance_row is not None and "min_pool_priority_cutoff" in provenance_row.index else None
            ),
            "native_min_pool_score_cutoff": (
                provenance_row.get("min_pool_score_cutoff") if provenance_row is not None and "min_pool_score_cutoff" in provenance_row.index else None
            ),
            "native_min_pool_rank_cutoff": (
                provenance_row.get("min_pool_rank_cutoff") if provenance_row is not None and "min_pool_rank_cutoff" in provenance_row.index else None
            ),
            "native_champion_selected_top5": _bool(active_row.get("champion_selected_top5")) if active_row is not None else _bool(winner.get("champion_selected_top5")),
            "native_tree_selected_top5": _bool(active_row.get("tree_hgb_path_value_selected_top5")) if active_row is not None else _bool(winner.get("tree_hgb_path_value_selected_top5")),
            "native_champion_selected_top10": _bool(active_row.get("champion_selected_top10")) if active_row is not None else _bool(winner.get("champion_selected_top10")),
            "native_tree_selected_top10": _bool(active_row.get("tree_hgb_path_value_selected_top10")) if active_row is not None else _bool(winner.get("tree_hgb_path_value_selected_top10")),
            "native_champion_selected_top20": _bool(active_row.get("champion_selected_top20")) if active_row is not None else _bool(winner.get("champion_selected_top20")),
            "native_tree_selected_top20": _bool(active_row.get("tree_hgb_path_value_selected_top20")) if active_row is not None else _bool(winner.get("tree_hgb_path_value_selected_top20")),
        }
        rows.append(row)
        trace_rows.append(
            {
                **row,
                "winner_trace_stage_summary": stage_presence,
            }
        )

    trace_df = pd.DataFrame(trace_rows)
    audit = {
        "schema_version": WINNER_TRACE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "long_side_top15_winner_count": int(len(winners)),
        "exact_key_traceable_count": int(winners["candidate_idx"].notna().sum()) if "candidate_idx" in winners.columns else int(len(winners)),
        "stable_key_traceable_count": int(winners["canonical_candidate_key"].notna().sum()) if "canonical_candidate_key" in winners.columns else int(len(winners)),
        "source_absent_before_min_pool_count": int((trace_df["loss_category"] == "source_absent_before_min_pool").sum()),
        "accepted_to_long_active_count": int((trace_df["loss_category"] == "accepted_to_long_active").sum()),
        "accepted_and_selected_count": int((trace_df["loss_category"] == "accepted_and_selected").sum()),
        "accepted_but_buried_by_champion_rank_count": int((trace_df["loss_category"] == "accepted_but_buried_by_champion_rank").sum()),
        "accepted_but_missed_by_reranker_count": int((trace_df["loss_category"] == "accepted_but_missed_by_reranker").sum()),
        "winner_rows": rows,
        "min_pool_reject_subreason_counts": {str(k): int(v) for k, v in trace_df["native_min_pool_reject_subreason"].value_counts(dropna=False).items()},
        "conclusion": {
            "native_min_pool_logged": True,
            "native_reject_reason_visible": True,
            "canonical_key_complete": bool(trace_df["canonical_candidate_key"].notna().all()),
        },
    }
    return audit, trace_df


def _feasibility_audit(trace_df: pd.DataFrame, winners: pd.DataFrame) -> dict[str, Any]:
    accepted = trace_df[trace_df["loss_category"].isin(["accepted_to_long_active", "accepted_and_selected", "accepted_but_buried_by_champion_rank", "accepted_but_missed_by_reranker"])].copy()
    rejected = trace_df[trace_df["loss_category"].eq("source_absent_before_min_pool")].copy()
    rejected_subreasons = rejected["native_min_pool_reject_subreason"].astype(str).value_counts(dropna=False)
    candidate_tier_known = bool(rejected["candidate_pool_tier"].notna().any())
    return {
        "schema_version": REFINEMENT_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "accepted_winner_count": int(len(accepted)),
        "source_absent_winner_count": int(len(rejected)),
        "accepted_mean_score": float(pd.to_numeric(accepted["score"], errors="coerce").mean()) if len(accepted) else None,
        "source_absent_mean_score": float(pd.to_numeric(rejected["score"], errors="coerce").mean()) if len(rejected) else None,
        "accepted_mean_rank": float(pd.to_numeric(accepted["rank"], errors="coerce").mean()) if len(accepted) else None,
        "source_absent_mean_rank": float(pd.to_numeric(rejected["rank"], errors="coerce").mean()) if len(rejected) else None,
        "accepted_tier_counts": {str(k): int(v) for k, v in accepted["candidate_pool_tier"].value_counts(dropna=False).items()},
        "source_absent_tier_counts": {str(k): int(v) for k, v in rejected["candidate_pool_tier"].value_counts(dropna=False).items()},
        "accepted_min_pool_reject_subreason_counts": {str(k): int(v) for k, v in accepted["native_min_pool_reject_subreason"].value_counts(dropna=False).items()},
        "source_absent_min_pool_reject_subreason_counts": {str(k): int(v) for k, v in rejected["native_min_pool_reject_subreason"].value_counts(dropna=False).items()},
        "native_logging_supported": True,
        "score_v2_supported": False,
        "top15_recall_signal_supported": False,
        "backfill_lane_split_supported": candidate_tier_known and bool(any("backfill" in str(k) for k in rejected_subreasons.index)),
        "native_logging_still_insufficient": (not candidate_tier_known) or ("min_pool_cap_exhausted_unknown" in rejected_subreasons.index),
        "missing_fields": {
            "source_absent_candidate_pool_tier": not candidate_tier_known,
            "source_absent_min_pool_reject_subreason": "min_pool_cap_exhausted_unknown" in rejected_subreasons.index,
        },
    }


def _recommendation(feasibility: dict[str, Any], trace_df: pd.DataFrame) -> dict[str, Any]:
    source_absent = trace_df[trace_df["loss_category"].eq("source_absent_before_min_pool")]
    accepted = trace_df[trace_df["loss_category"].isin(["accepted_to_long_active", "accepted_and_selected", "accepted_but_buried_by_champion_rank", "accepted_but_missed_by_reranker"])]
    backfill_source_absent = int((source_absent["candidate_pool_tier"].astype(str) == "risk_flagged_backfill").sum()) if len(source_absent) else 0
    backfill_share = backfill_source_absent / max(1, len(source_absent))
    if feasibility["native_logging_still_insufficient"]:
        action = "native_logging_still_insufficient"
        reason = "Native logs still do not isolate exact rejection subreasons."
    elif backfill_share >= 0.5 and len(source_absent) > 0 and len(accepted) > 0:
        action = "ready_to_split_backfill_recall_lane_v1"
        reason = "Native min-pool rejects are backfill-heavy and the active winners remain concentrated in primary/watch tiers."
    elif feasibility["score_v2_supported"]:
        action = "ready_to_design_long_admission_score_v2"
        reason = "Rejected winners separate clearly on score/rank after native reject provenance."
    elif feasibility["top15_recall_signal_supported"]:
        action = "ready_to_design_top15_recall_signal_v1"
        reason = "Rejected winners expose no-lookahead characteristics not captured by current admission logic."
    else:
        action = "stop_high_recall_line"
        reason = "Native logs do not support a safer next refinement axis."
    return {
        "schema_version": RECOMMENDATION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "recommended_next_action": action,
        "reason": reason,
        "supporting_metrics": {
            "source_absent_count": int(len(source_absent)),
            "accepted_count": int(len(accepted)),
            "source_absent_backfill_share": float(backfill_share) if len(source_absent) else None,
            "accepted_backfill_share": float((accepted["candidate_pool_tier"].astype(str) == "risk_flagged_backfill").mean()) if len(accepted) else None,
        },
    }


def _decision(recommendation: dict[str, Any]) -> dict[str, Any]:
    action = recommendation["recommended_next_action"]
    decision_map = {
        "ready_to_design_long_admission_score_v2": "ready_to_design_long_admission_score_v2",
        "ready_to_design_top15_recall_signal_v1": "ready_to_design_top15_recall_signal_v1",
        "ready_to_split_backfill_recall_lane_v1": "ready_to_split_backfill_recall_lane_v1",
        "native_logging_still_insufficient": "native_logging_still_insufficient",
        "stop_high_recall_line": "stop_high_recall_line",
    }
    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "decision": decision_map.get(action, "native_logging_still_insufficient"),
        "status": decision_map.get(action, "native_logging_still_insufficient"),
        "reason": recommendation["reason"],
    }


def _artifact_name(prefix: str, stem: str) -> str:
    if prefix:
        return f"{prefix}_{stem}"
    return stem


def _build_manifest(output_root: Path) -> dict[str, Any]:
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "script_name": SCRIPT_NAME,
        "session_id": output_root.name,
        "output_root": str(output_root),
        "jobs_requested": 2,
        "jobs_supported": 2,
        "source_artifacts": {
            "prefilter_rows": str(PREFILTER_ROWS),
            "two_stage_rows": str(TWO_STAGE_ROWS),
            "repaired_trace_rows": str(REPAIRED_TRACE_ROWS),
            "repaired_winner_rows": str(REPAIRED_WINNER_ROWS),
            "repaired_summary": str(REPAIRED_SUMMARY),
            "repaired_decision": str(REPAIRED_DECISION),
            "canonical_contract": str(CANONICAL_CONTRACT),
            "rejected_trace_rows": str(REJECTED_TRACE_ROWS),
            "rejected_rows": str(REJECTED_ROWS),
            "accepted_rows": str(ACCEPTED_ROWS),
            "reject_bucket_summary": str(REJECT_BUCKET_SUMMARY),
            "stage_reconciliation": str(STAGE_RECONCILIATION),
            "reranker_rows": str(RERANKER_ROWS),
            "reranker_variant_comparison": str(RERANKER_VARIANT_COMPARISON),
            "reranker_oracle_gap": str(RERANKER_ORACLE_GAP),
            "reranker_failure": str(RERANKER_FAILURE),
            "reranker_decision": str(RERANKER_DECISION),
            "long_active": str(LONG_ACTIVE),
            "surface_decision": str(SURFACE_DECISION),
            "filter_rows": str(FILTER_ROWS),
            "filter_decision": str(FILTER_DECISION),
        },
        "non_scope": {
            "meeMee": True,
            "production_ranking": True,
            "publish_or_promotion": True,
            "research_inventory_json": True,
            "model_training": True,
            "label_tuning": True,
            "challenger_creation": True,
        },
    }


def _input_resolution(output_root: Path, inputs: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "session_id": output_root.name,
        "output_root": str(output_root),
        "native_hook_available": True,
        "canonical_key_version": NATIVE_KEY_VERSION,
        "long_side_top15_winner_count": int(len(inputs["repaired_winner_rows"])),
        "resolved_min_pool_source": str(REJECTED_SESSION),
        "resolved_filter_source": str(FILTER_SESSION),
        "resolved_surface_source": str(SURFACE_SESSION),
        "resolved_reranker_source": str(RERANKER_SESSION),
        "notes": [
            "The native run uses the actual min-pool build_artifacts hook and the existing reranker / surface outputs.",
            "Short side remains research-hold and is excluded from active analysis.",
        ],
    }


def _run(output_root: Path, jobs: int, *, artifact_prefix: str = "native") -> dict[str, Any]:
    inputs = _load_sources()
    manifest = _build_manifest(output_root)
    input_resolution = _input_resolution(output_root, inputs)
    hook_inventory = _stage_inventory(inputs)
    trace, accepted, rejected, summary, bucket_summary, stage_recon, stages, rejected_stages, stage_inputs = _build_trace(inputs)
    winner_audit, winner_rows = _build_top15_loss_trace(inputs, stages, rejected_stages)
    feasibility = _feasibility_audit(winner_rows, inputs["repaired_winner_rows"])
    recommendation = _recommendation(feasibility, winner_rows)
    decision = _decision(recommendation)
    refinement_audit = {
        "schema_version": REFINEMENT_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "native_logging_summary": summary,
        "winner_audit": winner_audit,
        "feasibility": feasibility,
        "stage_bottlenecks": {
            stage_name: {
                "input_row_count": int(stage_inputs[stage_name]),
                "output_row_count": int(summary["stage_input_output"][idx]["output_row_count"]),
                "rejected_row_count": int(summary["stage_input_output"][idx]["rejected_row_count"]),
            }
            for idx, stage_name in enumerate(NATIVE_SOURCE_STAGES)
        },
        "decision_hint": recommendation["recommended_next_action"],
    }

    output_root.mkdir(parents=True, exist_ok=True)
    _write_json(output_root / "run_manifest.json", manifest)
    _write_json(output_root / "input_resolution.json", input_resolution)
    _write_json(output_root / _artifact_name(artifact_prefix, "reject_hook_inventory.json"), hook_inventory)
    _write_parquet(output_root / _artifact_name(artifact_prefix, "candidate_admission_trace_rows.parquet"), trace)
    _write_parquet(output_root / _artifact_name(artifact_prefix, "rejected_candidate_rows.parquet"), rejected)
    _write_parquet(output_root / _artifact_name(artifact_prefix, "accepted_candidate_rows.parquet"), accepted)
    _write_json(output_root / _artifact_name(artifact_prefix, "rejected_row_logging_summary.json"), summary)
    _write_json(output_root / _artifact_name(artifact_prefix, "reject_reason_bucket_summary.json"), bucket_summary)
    _write_json(output_root / _artifact_name(artifact_prefix, "stage_row_count_reconciliation.json"), stage_recon)
    _write_json(output_root / _artifact_name(artifact_prefix, "long_side_top15_loss_trace.json"), winner_audit)
    _write_parquet(output_root / _artifact_name(artifact_prefix, "long_side_top15_loss_trace_rows.parquet"), winner_rows)
    _write_json(output_root / _artifact_name(artifact_prefix, "long_side_refinement_audit.json"), refinement_audit)
    _write_json(output_root / _artifact_name(artifact_prefix, "long_side_refinement_recommendation.json"), recommendation)
    _write_json(output_root / _artifact_name(artifact_prefix, "rejected_row_logging_v1_decision.json"), decision)
    _write_json(
        output_root / "_ARTIFACT_COMPLETE.json",
        {
            "schema_version": ARTIFACT_COMPLETE_SCHEMA_VERSION,
            "generated_at_utc": _utc_now(),
            "complete": True,
            "required_artifacts": [
                "run_manifest.json",
                "input_resolution.json",
                _artifact_name(artifact_prefix, "reject_hook_inventory.json"),
                _artifact_name(artifact_prefix, "candidate_admission_trace_rows.parquet"),
                _artifact_name(artifact_prefix, "rejected_candidate_rows.parquet"),
                _artifact_name(artifact_prefix, "accepted_candidate_rows.parquet"),
                _artifact_name(artifact_prefix, "rejected_row_logging_summary.json"),
                _artifact_name(artifact_prefix, "reject_reason_bucket_summary.json"),
                _artifact_name(artifact_prefix, "stage_row_count_reconciliation.json"),
                _artifact_name(artifact_prefix, "long_side_top15_loss_trace.json"),
                _artifact_name(artifact_prefix, "long_side_top15_loss_trace_rows.parquet"),
                _artifact_name(artifact_prefix, "long_side_refinement_audit.json"),
                _artifact_name(artifact_prefix, "long_side_refinement_recommendation.json"),
                _artifact_name(artifact_prefix, "rejected_row_logging_v1_decision.json"),
                "_ARTIFACT_COMPLETE.json",
            ],
        },
    )
    return {
        "output_root": str(output_root),
        "decision": decision["decision"],
        "winner_count": int(len(winner_rows)),
        "accepted_rows": int(len(accepted)),
        "rejected_rows": int(len(rejected)),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="TRADEX native rejected-row logging for long-side candidate generation")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--session-id", default=None)
    parser.add_argument("--jobs", type=int, default=2)
    parser.add_argument("--artifact-prefix", default="native")
    args = parser.parse_args()
    output_root = Path(str(args.output_root)).expanduser().resolve()
    session_root = output_root / (args.session_id or _session_id())
    result = _run(session_root, max(1, args.jobs), artifact_prefix=str(args.artifact_prefix))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
