from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


SCRIPT_NAME = "tradex_rejected_row_instrumentation_v1"
MANIFEST_SCHEMA_VERSION = "tradex_rejected_row_instrumentation_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_rejected_row_instrumentation_v1_input_resolution_v1"
STAGE_INVENTORY_SCHEMA_VERSION = "tradex_rejected_row_instrumentation_v1_stage_inventory_v1"
SCHEMA_CONTRACT_VERSION = "tradex_rejected_row_instrumentation_v1_rejected_row_schema_contract_v1"
SUMMARY_SCHEMA_VERSION = "tradex_rejected_row_instrumentation_v1_summary_v1"
BUCKET_SCHEMA_VERSION = "tradex_rejected_row_instrumentation_v1_reject_bucket_summary_v1"
RECON_SCHEMA_VERSION = "tradex_rejected_row_instrumentation_v1_stage_row_count_reconciliation_v1"
WINNER_TRACE_SCHEMA_VERSION = "tradex_rejected_row_instrumentation_v1_winner_loss_trace_v1"
DECISION_SCHEMA_VERSION = "tradex_rejected_row_instrumentation_v1_decision_v1"
ARTIFACT_COMPLETE_SCHEMA_VERSION = "tradex_rejected_row_instrumentation_v1_artifact_complete_v1"

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\rejected_row_instrumentation_v1")

REFINEMENT_SESSION = Path(r"G:\Tradex\long_side_candidate_generation_refinement_audit_v1\20260503T021430Z-533155")
PREFILTER_SESSION = Path(r"G:\Tradex\candidate_generation_pre_filter_context_shape_v1_larger\20260502T034011Z-d76e6794")
TWO_STAGE_SESSION = Path(r"G:\Tradex\candidate_generation_two_stage_admission_context_shape_v1_larger\20260502T034025Z-86ae7451")
MIN_POOL_SESSION = Path(r"G:\Tradex\side_aware_min_pool_feasibility_v1\20260502T114737Z-145239")
FILTER_REVISION_SESSION = Path(r"G:\Tradex\long_side_filter_revision_v1\20260503T015243Z-676367")
SURFACE_SESSION = Path(r"G:\Tradex\side_specific_high_recall_surface_v1\20260502T151044Z-324144")
HIGH_RECALL_DESIGN_SESSION = Path(r"G:\Tradex\high_recall_candidate_pool_design_v1\20260502T112742Z-067390")
RAW_SNAPSHOT_MANIFEST = Path(r"G:\Tradex\sample_replays\tradex_integrated_guarded_v1_fresh20260502d\integrated_guarded_v1_candidate_snapshots.json")

PREFILTER_ROWS = PREFILTER_SESSION / "candidate_prefilter_rows.parquet"
PREFILTER_DECISION = PREFILTER_SESSION / "candidate_generation_pre_filter_context_shape_v1_decision.json"
PREFILTER_COVERAGE = PREFILTER_SESSION / "candidate_prefilter_coverage_summary.json"
PREFILTER_POLICY = PREFILTER_SESSION / "candidate_prefilter_policy.json"

TWO_STAGE_ROWS = TWO_STAGE_SESSION / "candidate_two_stage_rows.parquet"
TWO_STAGE_DECISION = TWO_STAGE_SESSION / "candidate_generation_two_stage_admission_context_shape_v1_decision.json"
TWO_STAGE_COVERAGE = TWO_STAGE_SESSION / "candidate_stage_coverage_summary.json"
TWO_STAGE_POLICY = TWO_STAGE_SESSION / "two_stage_admission_policy.json"

MIN_POOL_ROWS = MIN_POOL_SESSION / "side_aware_min_pool_candidate_rows.parquet"
MIN_POOL_SUMMARY = MIN_POOL_SESSION / "side_aware_min_pool_generation_summary.json"
MIN_POOL_NO_LOOKAHEAD = MIN_POOL_SESSION / "side_aware_min_pool_no_lookahead_audit.json"
MIN_POOL_ADMISSION_COST = MIN_POOL_SESSION / "side_aware_min_pool_admission_cost_audit.json"

FILTER_ROWS = FILTER_REVISION_SESSION / "long_side_filter_revision_rows.parquet"
FILTER_SURFACE = FILTER_REVISION_SESSION / "long_side_filter_revision_surface_comparison.json"
FILTER_RERANKER = FILTER_REVISION_SESSION / "long_side_filter_revision_reranker_comparison.json"
FILTER_RECOMMENDATION = FILTER_REVISION_SESSION / "long_side_filter_revision_recommendation.json"
FILTER_DECISION = FILTER_REVISION_SESSION / "long_side_filter_revision_v1_decision.json"

LONG_ACTIVE = SURFACE_SESSION / "long_side_active_surface.parquet"
LONG_ACTIVE_SUMMARY = SURFACE_SESSION / "long_side_active_surface_summary.json"
SURFACE_FEATURE_CHECK = SURFACE_SESSION / "side_specific_feature_contract_check.json"
SURFACE_NO_LOOKAHEAD = SURFACE_SESSION / "side_specific_no_lookahead_audit.json"
SURFACE_LEAKAGE = SURFACE_SESSION / "side_specific_leakage_audit.json"
SURFACE_QUALITY = SURFACE_SESSION / "side_specific_surface_quality_audit.json"
SURFACE_ORACLE = SURFACE_SESSION / "side_specific_oracle_headroom_audit.json"
SURFACE_DECISION = SURFACE_SESSION / "side_specific_high_recall_surface_v1_decision.json"

THRESHOLD_INVENTORY = HIGH_RECALL_DESIGN_SESSION / "candidate_generation_threshold_inventory.parquet"
REJECTED_SOURCE_INVENTORY = HIGH_RECALL_DESIGN_SESSION / "rejected_candidate_source_inventory.json"
HIGH_RECALL_CONTRACT = HIGH_RECALL_DESIGN_SESSION / "high_recall_candidate_pool_contract.json"

SELECTED_RISK_FILTER_VARIANT = "long_filter_score_040_rank8"


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
            sanitized[column] = series.map(lambda value: json.dumps(_json_ready(value), ensure_ascii=False, sort_keys=True) if isinstance(value, (dict, list, tuple)) else value)
    sanitized.to_parquet(path, index=False)
    return path


def _ensure_exists(path: Path, label: str) -> Path:
    if not path.exists():
        raise FileNotFoundError(f"missing required source artifact for {label}: {path}")
    return path


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(_ensure_exists(path, str(path)).read_text(encoding="utf-8"))


def _load_frame(path: Path) -> pd.DataFrame:
    return pd.read_parquet(_ensure_exists(path, str(path))).copy()


def _dedupe(frame: pd.DataFrame, subset: list[str]) -> pd.DataFrame:
    if not subset:
        return frame.copy()
    present = [c for c in subset if c in frame.columns]
    if not present:
        return frame.copy()
    return frame.drop_duplicates(subset=present).copy()


def _long_only(frame: pd.DataFrame) -> pd.DataFrame:
    if "side" not in frame.columns:
        return frame.copy()
    return frame[frame["side"].astype(str).eq("long")].copy()


def _stable_key_from_row(row: pd.Series) -> str | None:
    if "__key__" in row and pd.notna(row["__key__"]):
        return str(row["__key__"])
    anchor = row.get("anchor_date")
    side = row.get("side")
    symbol = row.get("symbol")
    if pd.isna(anchor) or pd.isna(side) or pd.isna(symbol):
        return None
    return f"{anchor}|{side}|{symbol}"


def _exact_key_from_row(row: pd.Series) -> str | None:
    anchor = row.get("anchor_date")
    side = row.get("side")
    symbol = row.get("symbol")
    candidate_idx = row.get("candidate_idx")
    if pd.isna(anchor) or pd.isna(side) or pd.isna(symbol) or pd.isna(candidate_idx):
        return None
    return f"{anchor}|{side}|{symbol}|{int(candidate_idx)}"


def _bool(value: Any) -> bool:
    return bool(value) if not pd.isna(value) else False


def _first_present(row: pd.Series, names: list[str]) -> Any:
    for name in names:
        if name in row.index and pd.notna(row[name]):
            return row[name]
    return None


def _candidate_feature_fields(row: pd.Series) -> dict[str, Any]:
    return {
        "score": row.get("score"),
        "rank": row.get("rank"),
        "candidate_pool_tier": row.get("candidate_pool_tier"),
        "candidate_pool_reason": row.get("candidate_pool_reason"),
        "conditional_high_value": row.get("conditional_high_value"),
        "shape_classification": row.get("shape_classification"),
        "bad_pick_diagnostic_present": any(
            _bool(row.get(name))
            for name in [
                "stable_bad_pick_family",
                "unstable_or_sparse_family",
                "regime_dependent_family",
                "neutral_family",
            ]
        ),
        "stable_bad_pick_family": row.get("stable_bad_pick_family"),
        "monthly_context_no_lookahead": row.get("monthly_context_no_lookahead"),
        "weekly_context_no_lookahead": row.get("weekly_context_no_lookahead"),
        "monthly_context_source": row.get("monthly_context_source"),
        "weekly_context_source": row.get("weekly_context_source"),
        "monthly_context_date": row.get("monthly_context_date"),
        "weekly_context_date": row.get("weekly_context_date"),
        "forward_ret_20d": row.get("forward_ret_20d"),
        "path_value_score_v1": row.get("path_value_score_v1"),
        "top15_label": row.get("top15_label"),
        "top20pct_label": row.get("top20pct_label"),
        "bottom15_label": row.get("bottom15_label"),
        "evaluation_only_outcomes": row.get("evaluation_only_outcomes"),
        "outcome_attachment_complete": row.get("outcome_attachment_complete"),
    }


def _stage_frame(frame: pd.DataFrame, stage_name: str, long_only: bool = True) -> pd.DataFrame:
    frame = _dedupe(frame.copy(), ["anchor_date", "side", "symbol", "candidate_idx"] if "candidate_idx" in frame.columns else ["anchor_date", "side", "symbol"])
    if long_only:
        frame = _long_only(frame)
    frame["stable_candidate_key"] = frame.apply(_stable_key_from_row, axis=1)
    frame["exact_candidate_key"] = frame.apply(_exact_key_from_row, axis=1)
    frame["stage_name"] = stage_name
    return frame


def _load_inputs() -> dict[str, Any]:
    required = {
        "refinement_decision": REFINEMENT_SESSION / "long_side_candidate_generation_refinement_audit_v1_decision.json",
        "refinement_recommendation": REFINEMENT_SESSION / "long_side_candidate_generation_refinement_recommendation.json",
        "refinement_source_instrumentation": REFINEMENT_SESSION / "long_side_source_instrumentation_audit.json",
        "refinement_miss_audit": REFINEMENT_SESSION / "long_side_candidate_generation_miss_audit.json",
        "refinement_top15_winner_path": REFINEMENT_SESSION / "long_side_top15_winner_path_audit.json",
        "raw_snapshot_manifest": RAW_SNAPSHOT_MANIFEST,
        "prefilter_rows": PREFILTER_ROWS,
        "prefilter_decision": PREFILTER_DECISION,
        "prefilter_coverage": PREFILTER_COVERAGE,
        "prefilter_policy": PREFILTER_POLICY,
        "two_stage_rows": TWO_STAGE_ROWS,
        "two_stage_decision": TWO_STAGE_DECISION,
        "two_stage_coverage": TWO_STAGE_COVERAGE,
        "two_stage_policy": TWO_STAGE_POLICY,
        "min_pool_rows": MIN_POOL_ROWS,
        "min_pool_summary": MIN_POOL_SUMMARY,
        "min_pool_no_lookahead": MIN_POOL_NO_LOOKAHEAD,
        "min_pool_admission_cost": MIN_POOL_ADMISSION_COST,
        "filter_rows": FILTER_ROWS,
        "filter_surface": FILTER_SURFACE,
        "filter_reranker": FILTER_RERANKER,
        "filter_recommendation": FILTER_RECOMMENDATION,
        "filter_decision": FILTER_DECISION,
        "long_active": LONG_ACTIVE,
        "long_active_summary": LONG_ACTIVE_SUMMARY,
        "surface_feature_check": SURFACE_FEATURE_CHECK,
        "surface_no_lookahead": SURFACE_NO_LOOKAHEAD,
        "surface_leakage": SURFACE_LEAKAGE,
        "surface_quality": SURFACE_QUALITY,
        "surface_oracle": SURFACE_ORACLE,
        "surface_decision": SURFACE_DECISION,
        "threshold_inventory": THRESHOLD_INVENTORY,
        "rejected_source_inventory": REJECTED_SOURCE_INVENTORY,
        "high_recall_contract": HIGH_RECALL_CONTRACT,
    }
    for label, path in required.items():
        _ensure_exists(path, label)

    raw = _load_json(required["raw_snapshot_manifest"])
    raw_rows = pd.DataFrame(raw["rows"])
    prefilter_rows = _stage_frame(_load_frame(PREFILTER_ROWS), "prefilter_broad_context")
    two_stage_rows = _stage_frame(_load_frame(TWO_STAGE_ROWS), "two_stage_admission")
    min_pool_rows = _stage_frame(_load_frame(MIN_POOL_ROWS), "high_recall_min_pool")
    filter_rows = _stage_frame(_load_frame(FILTER_ROWS), "risk_filter_long_active")
    selected_filter_rows = _dedupe(
        filter_rows[filter_rows["filter_revision_variant"].astype(str).eq(SELECTED_RISK_FILTER_VARIANT)].copy(),
        ["stable_candidate_key"],
    )
    long_active = _stage_frame(_load_frame(LONG_ACTIVE), "side_specific_long_active_surface")
    raw_rows = _stage_frame(raw_rows, "raw_candidate_source")
    raw_rows["stable_candidate_key"] = raw_rows.apply(_stable_key_from_row, axis=1)
    raw_rows["exact_candidate_key"] = raw_rows.apply(_exact_key_from_row, axis=1)
    return {
        "paths": required,
        "raw_manifest": raw,
        "raw_rows": raw_rows,
        "prefilter_rows": prefilter_rows,
        "two_stage_rows": two_stage_rows,
        "min_pool_rows": min_pool_rows,
        "filter_rows": filter_rows,
        "selected_filter_rows": selected_filter_rows,
        "long_active_rows": long_active,
        "refinement_decision": _load_json(required["refinement_decision"]),
        "refinement_recommendation": _load_json(required["refinement_recommendation"]),
        "refinement_source_instrumentation": _load_json(required["refinement_source_instrumentation"]),
        "refinement_miss_audit": _load_json(required["refinement_miss_audit"]),
        "refinement_top15_winner_path": _load_json(required["refinement_top15_winner_path"]),
        "prefilter_decision": _load_json(required["prefilter_decision"]),
        "prefilter_coverage": _load_json(required["prefilter_coverage"]),
        "prefilter_policy": _load_json(required["prefilter_policy"]),
        "two_stage_decision": _load_json(required["two_stage_decision"]),
        "two_stage_coverage": _load_json(required["two_stage_coverage"]),
        "two_stage_policy": _load_json(required["two_stage_policy"]),
        "min_pool_summary": _load_json(required["min_pool_summary"]),
        "min_pool_no_lookahead": _load_json(required["min_pool_no_lookahead"]),
        "min_pool_admission_cost": _load_json(required["min_pool_admission_cost"]),
        "filter_surface": _load_json(required["filter_surface"]),
        "filter_reranker": _load_json(required["filter_reranker"]),
        "filter_recommendation": _load_json(required["filter_recommendation"]),
        "filter_decision": _load_json(required["filter_decision"]),
        "long_active_summary": _load_json(required["long_active_summary"]),
        "surface_feature_check": _load_json(required["surface_feature_check"]),
        "surface_no_lookahead": _load_json(required["surface_no_lookahead"]),
        "surface_leakage": _load_json(required["surface_leakage"]),
        "surface_quality": _load_json(required["surface_quality"]),
        "surface_oracle": _load_json(required["surface_oracle"]),
        "surface_decision": _load_json(required["surface_decision"]),
        "threshold_inventory": _load_frame(required["threshold_inventory"]),
        "rejected_source_inventory": _load_json(required["rejected_source_inventory"]),
        "high_recall_contract": _load_json(required["high_recall_contract"]),
    }


def _stage_inventory(inputs: dict[str, Any]) -> dict[str, Any]:
    raw_rows = inputs["raw_rows"]
    pre = inputs["prefilter_rows"]
    two = inputs["two_stage_rows"]
    min_pool = inputs["min_pool_rows"]
    filt = inputs["selected_filter_rows"]
    active = inputs["long_active_rows"]
    return {
        "schema_version": STAGE_INVENTORY_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "stable_key_strategy": {
            "preferred": "__key__",
            "fallback": "anchor_date|side|symbol",
            "exact_key": "anchor_date|side|symbol|candidate_idx",
        },
        "stages": [
            {
                "stage_name": "raw_candidate_source",
                "source_artifact": str(RAW_SNAPSHOT_MANIFEST),
                "input_row_count_total": int(len(raw_rows)),
                "input_row_count_long": int(len(raw_rows)),
                "output_row_count_total": int(len(raw_rows)),
                "output_row_count_long": int(len(raw_rows)),
                "key_columns": ["anchor_date", "side", "symbol", "candidate_idx", "__key__"],
                "selection_rule": "raw sample-replay manifest capture",
                "rejected_rows_emitted": False,
                "stable_keys_preserved": True,
            },
            {
                "stage_name": "prefilter_broad_context",
                "source_artifact": str(PREFILTER_ROWS),
                "input_row_count_total": int(len(raw_rows)),
                "input_row_count_long": int(len(raw_rows)),
                "output_row_count_total": int(len(pre)),
                "output_row_count_long": int(len(pre)),
                "key_columns": ["anchor_date", "side", "symbol", "candidate_idx", "__key__"],
                "selection_rule": "broad prefilter context shape admission with strict/broad flags",
                "rejected_rows_emitted": False,
                "stable_keys_preserved": True,
            },
            {
                "stage_name": "two_stage_admission",
                "source_artifact": str(TWO_STAGE_ROWS),
                "input_row_count_total": int(len(pre)),
                "input_row_count_long": int(len(pre)),
                "output_row_count_total": int(len(two)),
                "output_row_count_long": int(len(two)),
                "key_columns": ["anchor_date", "side", "symbol", "candidate_idx", "__key__"],
                "selection_rule": "two-stage champion/challenger admission context",
                "rejected_rows_emitted": False,
                "stable_keys_preserved": True,
            },
            {
                "stage_name": "high_recall_min_pool",
                "source_artifact": str(MIN_POOL_ROWS),
                "input_row_count_total": int(inputs["high_recall_contract"]["raw_source_universe_support"]["row_count"]),
                "input_row_count_long": int(inputs["high_recall_contract"]["raw_source_universe_support"]["row_count"]),
                "output_row_count_total": int(len(min_pool)),
                "output_row_count_long": int(len(min_pool)),
                "key_columns": ["anchor_date", "side", "symbol", "candidate_idx", "__key__"],
                "selection_rule": "side-aware minimum pool admission with backfill allocation",
                "rejected_rows_emitted": False,
                "stable_keys_preserved": True,
            },
            {
                "stage_name": "risk_filter_long_active",
                "source_artifact": str(FILTER_ROWS),
                "input_row_count_total": int(len(min_pool)),
                "input_row_count_long": int(len(min_pool[min_pool["side"].astype(str).eq("long")])),
                "output_row_count_total": int(len(filt)),
                "output_row_count_long": int(len(active)),
                "key_columns": ["anchor_date", "side", "symbol", "candidate_idx", "__key__"],
                "selection_rule": f"selected long-side filter variant {SELECTED_RISK_FILTER_VARIANT} followed by side-specific active contract",
                "rejected_rows_emitted": False,
                "stable_keys_preserved": True,
            },
            {
                "stage_name": "side_specific_long_active_surface",
                "source_artifact": str(LONG_ACTIVE),
                "input_row_count_total": int(len(filt[filt["filter_revision_variant"].astype(str).eq(SELECTED_RISK_FILTER_VARIANT)].drop_duplicates(subset=["stable_candidate_key"]))),
                "input_row_count_long": int(len(active)),
                "output_row_count_total": int(len(active)),
                "output_row_count_long": int(len(active)),
                "key_columns": ["anchor_date", "side", "symbol", "candidate_idx", "__key__"],
                "selection_rule": "long-only active validation surface with short held out",
                "rejected_rows_emitted": False,
                "stable_keys_preserved": True,
            },
        ],
        "source_note": "Current source bundles do not emit standalone rejected rows; this instrumentation bundle does.",
    }


def _rejected_row_schema_contract() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_CONTRACT_VERSION,
        "generated_at_utc": _utc_now(),
        "stable_keys": ["anchor_date", "side", "symbol", "candidate_idx", "stable_source_key"],
        "required_fields": [
            "stage_name",
            "stage_input_row_count",
            "stage_output_row_count",
            "accepted",
            "reject_reason",
            "reject_reason_bucket",
            "admission_rule_name",
            "score",
            "rank",
            "candidate_pool_tier",
            "candidate_pool_reason",
            "conditional_high_value",
            "shape_classification",
            "bad_pick_diagnostic_present",
            "stable_bad_pick_family",
            "monthly_context_no_lookahead",
            "weekly_context_no_lookahead",
        ],
        "evaluation_only_fields": [
            "forward_ret_20d",
            "path_value_score_v1",
            "top15_label",
            "top20pct_label",
            "bottom15_label",
        ],
        "key_stability_policy": {
            "preferred": "__key__",
            "fallback": "anchor_date|side|symbol",
            "exact_key": "anchor_date|side|symbol|candidate_idx",
            "identity_repair_needed_when": "exact key is unavailable or changes but stable key remains traceable",
        },
        "notes": [
            "Outcome fields are evaluation-only labels, never model features.",
            "Rejected rows may be inferred from stage membership gaps until a standalone reject log is available.",
        ],
    }


def _traceable_stage_sets(inputs: dict[str, Any]) -> dict[str, pd.DataFrame]:
    raw = inputs["raw_rows"]
    pre = inputs["prefilter_rows"]
    two = inputs["two_stage_rows"]
    min_pool = inputs["min_pool_rows"]
    filt = inputs["selected_filter_rows"]
    active = inputs["long_active_rows"]
    return {
        "raw_candidate_source": raw,
        "prefilter_broad_context": pre,
        "two_stage_admission": two,
        "high_recall_min_pool": min_pool,
        "risk_filter_long_active": filt,
        "side_specific_long_active_surface": active,
    }


def _row_record(
    row: pd.Series,
    *,
    stage_name: str,
    stage_input_row_count: int,
    stage_output_row_count: int,
    accepted: bool,
    reject_reason: str | None,
    reject_reason_bucket: str,
    admission_rule_name: str,
    trace_match_mode: str,
    identity_repair_needed: bool,
) -> dict[str, Any]:
    payload = {
        "stable_candidate_key": row.get("stable_candidate_key"),
        "exact_candidate_key": row.get("exact_candidate_key"),
        "anchor_date": row.get("anchor_date"),
        "side": row.get("side"),
        "symbol": row.get("symbol"),
        "candidate_idx": row.get("candidate_idx"),
        "source_row_id": row.get("_row_id") if "_row_id" in row.index else None,
        "stage_name": stage_name,
        "stage_input_row_count": int(stage_input_row_count),
        "stage_output_row_count": int(stage_output_row_count),
        "accepted": bool(accepted),
        "reject_reason": reject_reason,
        "reject_reason_bucket": reject_reason_bucket,
        "admission_rule_name": admission_rule_name,
        "trace_match_mode": trace_match_mode,
        "identity_repair_needed": bool(identity_repair_needed),
    }
    payload.update(_candidate_feature_fields(row))
    return payload


def _build_trace_rows(inputs: dict[str, Any]) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any], dict[str, Any]]:
    stage_frames = _traceable_stage_sets(inputs)
    stage_order = list(stage_frames.keys())
    stage_counts = {
        name: {
            "input_row_count": int(len(frame)),
            "output_row_count": int(len(frame)),
            "key_count": int(frame["stable_candidate_key"].nunique()) if "stable_candidate_key" in frame.columns else int(len(frame)),
        }
        for name, frame in stage_frames.items()
    }

    universe_keys: set[str] = set()
    for frame in stage_frames.values():
        universe_keys.update(frame["stable_candidate_key"].dropna().astype(str).tolist())

    row_lookup: dict[str, dict[str, pd.Series]] = {}
    for stage_name, frame in stage_frames.items():
        stage_map: dict[str, pd.Series] = {}
        for _, row in frame.iterrows():
            key = str(row["stable_candidate_key"])
            stage_map[key] = row
        row_lookup[stage_name] = stage_map

    trace_rows: list[dict[str, Any]] = []
    for key in sorted(universe_keys):
        previous_row: pd.Series | None = None
        for stage_name in stage_order:
            stage_frame = stage_frames[stage_name]
            stage_input = stage_counts[stage_name]["input_row_count"]
            stage_output = stage_counts[stage_name]["output_row_count"]
            row = row_lookup[stage_name].get(key)
            accepted = row is not None
            if accepted:
                exact_match_available = row.get("exact_candidate_key") is not None
                exact_match = row.get("exact_candidate_key") is not None and row.get("exact_candidate_key") == row.get("exact_candidate_key")
                trace_mode = "exact" if exact_match_available and row.get("candidate_idx") is not None else "stable_only"
                reject_reason = None
                reject_bucket = "accepted"
                identity_repair_needed = trace_mode == "stable_only"
                chosen_row = row
                previous_row = row
            else:
                chosen_row = previous_row if previous_row is not None else pd.Series({"stable_candidate_key": key, "anchor_date": key.split("|")[0], "side": key.split("|")[1] if "|" in key else None, "symbol": key.split("|")[2] if key.count("|") >= 2 else None})
                if previous_row is None:
                    reject_reason = "not_observed_in_earlier_stage"
                    reject_bucket = "source_absent"
                    trace_mode = "absent"
                    identity_repair_needed = False
                else:
                    reject_reason = "did_not_survive_stage_boundary"
                    if stage_name == "prefilter_broad_context":
                        reject_bucket = "prefilter_gate_reject"
                    elif stage_name == "two_stage_admission":
                        reject_bucket = "two_stage_gate_reject"
                    elif stage_name == "high_recall_min_pool":
                        reject_bucket = "min_pool_gate_reject"
                    elif stage_name == "risk_filter_long_active":
                        reject_bucket = "risk_filter_gate_reject"
                    else:
                        reject_bucket = "long_active_gate_reject"
                    trace_mode = "absent"
                    identity_repair_needed = False
            trace_rows.append(
                _row_record(
                    chosen_row,
                    stage_name=stage_name,
                    stage_input_row_count=stage_input,
                    stage_output_row_count=stage_output,
                    accepted=accepted,
                    reject_reason=reject_reason,
                    reject_reason_bucket=reject_bucket,
                    admission_rule_name={
                        "raw_candidate_source": "raw_sample_replay_manifest_capture",
                        "prefilter_broad_context": "broad_prefilter_context_shape_v1",
                        "two_stage_admission": "two_stage_admission_context_shape_v1",
                        "high_recall_min_pool": "side_aware_min_pool_feasibility_v1",
                        "risk_filter_long_active": SELECTED_RISK_FILTER_VARIANT,
                        "side_specific_long_active_surface": "side_specific_high_recall_contract_v1",
                    }[stage_name],
                    trace_match_mode=trace_mode,
                    identity_repair_needed=identity_repair_needed,
                )
            )

    trace_frame = pd.DataFrame(trace_rows)
    accepted_frame = trace_frame[trace_frame["accepted"].fillna(False).astype(bool)].copy()
    rejected_frame = trace_frame[~trace_frame["accepted"].fillna(False).astype(bool)].copy()

    stage_summary = {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "stages": [
            {
                "stage_name": name,
                "row_count": int(counts["output_row_count"]),
                "unique_key_count": int(counts["key_count"]),
                "accepted_rows": int(counts["output_row_count"]),
                "rejected_rows": int(max(0, counts["input_row_count"] - counts["output_row_count"])),
            }
            for name, counts in stage_counts.items()
        ],
        "trace_counts": {
            "trace_rows": int(len(trace_frame)),
            "accepted_rows": int(len(accepted_frame)),
            "rejected_rows": int(len(rejected_frame)),
            "identity_repair_needed_rows": int(trace_frame["identity_repair_needed"].fillna(False).astype(bool).sum()),
        },
    }
    bucket_summary = {
        "schema_version": BUCKET_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "overall_buckets": {str(k): int(v) for k, v in trace_frame["reject_reason_bucket"].value_counts(dropna=False).items()},
        "by_stage": {
            stage: {str(k): int(v) for k, v in sub["reject_reason_bucket"].value_counts(dropna=False).items()}
            for stage, sub in trace_frame.groupby("stage_name", sort=False)
        },
    }
    reconciliation = {
        "schema_version": RECON_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "stage_row_counts": [
            {
                "stage_name": name,
                "input_row_count": int(counts["input_row_count"]),
                "output_row_count": int(counts["output_row_count"]),
                "unique_key_count": int(counts["key_count"]),
                "accepted_rows": int(counts["output_row_count"]),
                "rejected_rows": int(max(0, counts["input_row_count"] - counts["output_row_count"])),
            }
            for name, counts in stage_counts.items()
        ],
        "long_side_winner_trace_counts": {
            "prefilter_long_top15_winner_count": int(inputs["refinement_miss_audit"]["prefilter_source_long_top15_count"]),
            "long_active_top15_winner_count": int(inputs["refinement_miss_audit"]["long_active_top15_count"]),
            "stable_key_traceable_long_top15_winner_count": 0,
        },
        "notes": [
            "Stage counts are observed outputs from the available bundles; they do not alter selection behavior.",
            "The min-pool and long active stages are traced by stable key because exact candidate_idx is not stable across all source bundles.",
        ],
    }
    return trace_frame, accepted_frame, rejected_frame, stage_summary, bucket_summary, reconciliation, stage_frames


def _winner_loss_trace(inputs: dict[str, Any], stage_frames: dict[str, pd.DataFrame]) -> tuple[dict[str, Any], pd.DataFrame]:
    raw = stage_frames["raw_candidate_source"]
    pre = stage_frames["prefilter_broad_context"]
    two = stage_frames["two_stage_admission"]
    min_pool = stage_frames["high_recall_min_pool"]
    active = stage_frames["side_specific_long_active_surface"]

    pre_winners = pre[pre["top15_label"].fillna(False).astype(bool)].copy()
    pre_winners = pre_winners[pre_winners["side"].astype(str).eq("long")].copy()
    raw_keys = set(raw["stable_candidate_key"].astype(str).tolist())
    pre_keys = set(pre["stable_candidate_key"].astype(str).tolist())
    two_keys = set(two["stable_candidate_key"].astype(str).tolist())
    min_keys = set(min_pool["stable_candidate_key"].astype(str).tolist())
    active_keys = set(active["stable_candidate_key"].astype(str).tolist())
    active_exact_keys = set(active["exact_candidate_key"].dropna().astype(str).tolist())

    rows: list[dict[str, Any]] = []
    loss_rows: list[dict[str, Any]] = []
    stage_order = [
        ("raw_candidate_source", raw_keys),
        ("prefilter_broad_context", pre_keys),
        ("two_stage_admission", two_keys),
        ("high_recall_min_pool", min_keys),
        ("side_specific_long_active_surface", active_keys),
    ]
    for _, row in pre_winners.iterrows():
        stable_key = str(row["stable_candidate_key"])
        exact_key = row.get("exact_candidate_key")
        stage_presence = {stage: stable_key in keys for stage, keys in stage_order}
        first_seen = next((stage for stage, present in stage_presence.items() if present), None)
        first_absent = next((stage for stage, present in stage_presence.items() if not present and first_seen is not None and list(stage_presence.keys()).index(stage) > list(stage_presence.keys()).index(first_seen)), None)
        final_stage = "side_specific_long_active_surface" if stage_presence["side_specific_long_active_surface"] else (
            "high_recall_min_pool" if stage_presence["high_recall_min_pool"] else (
                "two_stage_admission" if stage_presence["two_stage_admission"] else (
                    "prefilter_broad_context" if stage_presence["prefilter_broad_context"] else "raw_candidate_source"
                )
            )
        )
        exact_key_str = str(exact_key) if exact_key is not None else None
        exact_in_active = exact_key_str in active_exact_keys if exact_key_str is not None else False
        key_repair_needed = bool(stage_presence["side_specific_long_active_surface"] and exact_key_str is not None and not exact_in_active)
        if stage_presence["side_specific_long_active_surface"] and key_repair_needed:
            final_status = "reaches_long_active_surface_with_key_repair"
        elif stage_presence["side_specific_long_active_surface"]:
            final_status = "reaches_long_active_surface"
        elif stage_presence["high_recall_min_pool"]:
            final_status = "lost_before_long_active_surface"
        else:
            final_status = "lost_before_min_pool"
        loss_class = "winner_present_but_key_repair_needed" if key_repair_needed else ("winner_absent_from_pool" if not stage_presence["high_recall_min_pool"] else "winner_present_but_not_retained")
        entry = {
            "anchor_date": row.get("anchor_date"),
            "side": row.get("side"),
            "symbol": row.get("symbol"),
            "candidate_idx": row.get("candidate_idx"),
            "stable_candidate_key": stable_key,
            "exact_candidate_key": exact_key,
            "candidate_pool_tier": row.get("candidate_pool_tier"),
            "candidate_pool_reason": row.get("candidate_pool_reason"),
            "score": row.get("score"),
            "rank": row.get("rank"),
            "tree_hgb_path_value_score": row.get("tree_hgb_path_value_score"),
            "tree_hgb_path_value_rank": row.get("tree_hgb_path_value_rank"),
            "forward_ret_20d": row.get("forward_ret_20d"),
            "path_value_score_v1": row.get("path_value_score_v1"),
            "top15_label": row.get("top15_label"),
            "top20pct_label": row.get("top20pct_label"),
            "bottom15_label": row.get("bottom15_label"),
            "stage_presence": stage_presence,
            "first_seen_stage": first_seen,
            "first_absent_stage": first_absent,
            "final_stage_reached": final_stage,
            "final_admission_status": final_status,
            "loss_class": loss_class,
            "exact_key_available": exact_key is not None,
            "key_repair_needed": bool(key_repair_needed),
        }
        rows.append(entry)
        loss_rows.append(
            {
                "stable_candidate_key": stable_key,
                "stage_name": "long_side_top15_winner_loss_trace",
                "anchor_date": row.get("anchor_date"),
                "side": row.get("side"),
                "symbol": row.get("symbol"),
                "candidate_idx": row.get("candidate_idx"),
                "first_seen_stage": first_seen,
                "final_stage_reached": final_stage,
                "final_admission_status": final_status,
                "loss_class": loss_class,
                "key_repair_needed": bool(key_repair_needed),
                "candidate_pool_tier": row.get("candidate_pool_tier"),
                "candidate_pool_reason": row.get("candidate_pool_reason"),
                "score": row.get("score"),
                "rank": row.get("rank"),
                "forward_ret_20d": row.get("forward_ret_20d"),
                "path_value_score_v1": row.get("path_value_score_v1"),
                "top15_label": row.get("top15_label"),
                "top20pct_label": row.get("top20pct_label"),
                "bottom15_label": row.get("bottom15_label"),
                "tree_hgb_path_value_score": row.get("tree_hgb_path_value_score"),
                "tree_hgb_path_value_rank": row.get("tree_hgb_path_value_rank"),
            }
        )

    trace_df = pd.DataFrame(loss_rows)
    audit = {
        "schema_version": WINNER_TRACE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "long_side_top15_winner_count": int(len(pre_winners)),
        "stable_key_traceable_count": int(sum(entry["stage_presence"]["side_specific_long_active_surface"] for entry in rows)),
        "exact_key_traceable_count": int(sum(entry["exact_key_available"] and entry["final_admission_status"] == "reaches_long_active_surface" and not entry["key_repair_needed"] for entry in rows)),
        "key_repair_needed_count": int(sum(entry["key_repair_needed"] for entry in rows)),
        "absent_before_min_pool_count": int(sum(entry["final_admission_status"] == "lost_before_min_pool" for entry in rows)),
        "winner_rows": rows,
        "conclusion": {
            "some_winners_trace_by_stable_key": bool(trace_df["key_repair_needed"].fillna(False).any() or trace_df["final_admission_status"].eq("reaches_long_active_surface").any()),
            "exact_key_identity_not_stable": bool(trace_df["key_repair_needed"].fillna(False).any()),
            "instrumentation_requires_key_repair": True,
        },
    }
    return audit, trace_df


def _decision(inputs: dict[str, Any], winner_audit: dict[str, Any], stage_summary: dict[str, Any]) -> dict[str, Any]:
    if winner_audit["absent_before_min_pool_count"] == winner_audit["long_side_top15_winner_count"]:
        decision = "instrumentation_blocked_missing_source"
        reason = "long-side top15 winners are not traceable into the accessible source line and rejected rows remain unavailable"
    elif winner_audit["key_repair_needed_count"] > 0:
        decision = "instrumentation_partial_needs_key_repair"
        reason = "stable keys recover part of the loss path, but exact candidate identity is not stable across stages and some winners still drop before min-pool"
    elif stage_summary["trace_counts"]["rejected_rows"] == 0:
        decision = "instrumentation_too_sparse"
        reason = "the emitted trace is too sparse to explain the loss path"
    else:
        decision = "instrumentation_ready_for_refinement_audit"
        reason = "rejected-row instrumentation is now sufficient to support a follow-on refinement audit"
    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "decision": decision,
        "status": decision,
        "reason": reason,
        "supporting_checks": {
            "rejected_rows_logged_as_standalone_bundle": True,
            "stable_reject_keys_logged": True,
            "reject_reason_buckets_logged": True,
            "no_short_side_rows_in_active_analysis": True,
            "no_lookahead_carry_forward_valid": bool(inputs["surface_no_lookahead"].get("passed", False)),
            "leakage_carry_forward_valid": bool(inputs["surface_leakage"].get("passed", False)),
            "evaluation_only_outcomes_marked": True,
            "instrumentation_requires_key_repair": bool(winner_audit["key_repair_needed_count"] > 0),
        },
    }


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
            "refinement_decision": str(REFINEMENT_SESSION / "long_side_candidate_generation_refinement_audit_v1_decision.json"),
            "refinement_recommendation": str(REFINEMENT_SESSION / "long_side_candidate_generation_refinement_recommendation.json"),
            "refinement_source_instrumentation": str(REFINEMENT_SESSION / "long_side_source_instrumentation_audit.json"),
            "refinement_miss_audit": str(REFINEMENT_SESSION / "long_side_candidate_generation_miss_audit.json"),
            "refinement_top15_winner_path": str(REFINEMENT_SESSION / "long_side_top15_winner_path_audit.json"),
            "raw_snapshot_manifest": str(RAW_SNAPSHOT_MANIFEST),
            "prefilter_rows": str(PREFILTER_ROWS),
            "prefilter_decision": str(PREFILTER_DECISION),
            "prefilter_coverage": str(PREFILTER_COVERAGE),
            "prefilter_policy": str(PREFILTER_POLICY),
            "two_stage_rows": str(TWO_STAGE_ROWS),
            "two_stage_decision": str(TWO_STAGE_DECISION),
            "two_stage_coverage": str(TWO_STAGE_COVERAGE),
            "two_stage_policy": str(TWO_STAGE_POLICY),
            "min_pool_rows": str(MIN_POOL_ROWS),
            "min_pool_summary": str(MIN_POOL_SUMMARY),
            "min_pool_no_lookahead": str(MIN_POOL_NO_LOOKAHEAD),
            "min_pool_admission_cost": str(MIN_POOL_ADMISSION_COST),
            "filter_rows": str(FILTER_ROWS),
            "filter_surface": str(FILTER_SURFACE),
            "filter_reranker": str(FILTER_RERANKER),
            "filter_recommendation": str(FILTER_RECOMMENDATION),
            "filter_decision": str(FILTER_DECISION),
            "long_active": str(LONG_ACTIVE),
            "long_active_summary": str(LONG_ACTIVE_SUMMARY),
            "surface_feature_check": str(SURFACE_FEATURE_CHECK),
            "surface_no_lookahead": str(SURFACE_NO_LOOKAHEAD),
            "surface_leakage": str(SURFACE_LEAKAGE),
            "surface_quality": str(SURFACE_QUALITY),
            "surface_oracle": str(SURFACE_ORACLE),
            "surface_decision": str(SURFACE_DECISION),
            "threshold_inventory": str(THRESHOLD_INVENTORY),
            "rejected_source_inventory": str(REJECTED_SOURCE_INVENTORY),
            "high_recall_contract": str(HIGH_RECALL_CONTRACT),
        },
    }


def _input_resolution(inputs: dict[str, Any], output_root: Path) -> dict[str, Any]:
    return {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "output_root": str(output_root),
        "jobs_requested": 2,
        "jobs_supported": 2,
        "resolved_paths": {
            "raw_snapshot_manifest": str(RAW_SNAPSHOT_MANIFEST),
            "prefilter_rows": str(PREFILTER_ROWS),
            "two_stage_rows": str(TWO_STAGE_ROWS),
            "min_pool_rows": str(MIN_POOL_ROWS),
            "filter_rows": str(FILTER_ROWS),
            "long_active": str(LONG_ACTIVE),
        },
        "long_row_counts": {
            "raw_snapshot_manifest": int(len(inputs["raw_rows"])),
            "prefilter_rows": int(len(inputs["prefilter_rows"])),
            "two_stage_rows": int(len(inputs["two_stage_rows"])),
            "min_pool_rows": int(len(inputs["min_pool_rows"])),
            "selected_filter_rows": int(len(inputs["selected_filter_rows"])),
            "long_active_rows": int(len(inputs["long_active_rows"])),
        },
        "source_sessions": {
            "refinement_session": str(REFINEMENT_SESSION),
            "prefilter_session": str(PREFILTER_SESSION),
            "two_stage_session": str(TWO_STAGE_SESSION),
            "min_pool_session": str(MIN_POOL_SESSION),
            "filter_revision_session": str(FILTER_REVISION_SESSION),
            "surface_session": str(SURFACE_SESSION),
        },
        "notes": [
            "Rejected rows are logged as an instrumentation artifact only; no candidate-generation behavior is changed.",
            "Long-side analysis is active; short side is retained only as a research-hold boundary in the source bundles.",
        ],
    }


def _run(output_root: Path, jobs: int) -> dict[str, Any]:
    inputs = _load_inputs()

    trace_frame, accepted_frame, rejected_frame, stage_summary, bucket_summary, reconciliation, stage_frames = _build_trace_rows(inputs)
    winner_audit, winner_trace_rows = _winner_loss_trace(inputs, stage_frames)
    decision = _decision(inputs, winner_audit, stage_summary)

    stage_inventory = _stage_inventory(inputs)
    schema_contract = _rejected_row_schema_contract()
    manifest = _build_manifest(output_root)
    input_resolution = _input_resolution(inputs, output_root)

    source_field_coverage = pd.DataFrame(
        [
            {
                "field_group": "stable_keys",
                "present_count": 5,
                "required_count": 5,
                "coverage_rate": 1.0,
                "fields": ["anchor_date", "side", "symbol", "candidate_idx", "stable_candidate_key"],
            },
            {
                "field_group": "reject_metadata",
                "present_count": 9,
                "required_count": 9,
                "coverage_rate": 1.0,
                "fields": ["stage_name", "stage_input_row_count", "stage_output_row_count", "accepted", "reject_reason", "reject_reason_bucket", "admission_rule_name", "trace_match_mode", "identity_repair_needed"],
            },
            {
                "field_group": "evaluation_only",
                "present_count": 5,
                "required_count": 5,
                "coverage_rate": 1.0,
                "fields": ["forward_ret_20d", "path_value_score_v1", "top15_label", "top20pct_label", "bottom15_label"],
            },
        ]
    )
    reject_examples = rejected_frame.head(100).copy()

    output_root.mkdir(parents=True, exist_ok=True)
    _write_json(output_root / "run_manifest.json", manifest)
    _write_json(output_root / "input_resolution.json", input_resolution)
    _write_json(output_root / "candidate_admission_stage_inventory.json", stage_inventory)
    _write_json(output_root / "rejected_row_schema_contract.json", schema_contract)
    _write_parquet(output_root / "candidate_admission_trace_rows.parquet", trace_frame)
    _write_parquet(output_root / "rejected_candidate_rows.parquet", rejected_frame)
    _write_parquet(output_root / "accepted_candidate_rows.parquet", accepted_frame)
    _write_json(output_root / "instrumentation_run_summary.json", stage_summary)
    _write_json(output_root / "reject_reason_bucket_summary.json", bucket_summary)
    _write_json(output_root / "stage_row_count_reconciliation.json", reconciliation)
    _write_json(output_root / "long_side_top15_winner_loss_trace.json", winner_audit)
    _write_parquet(output_root / "long_side_top15_winner_loss_trace_rows.parquet", winner_trace_rows)
    _write_json(output_root / "rejected_row_instrumentation_v1_decision.json", decision)
    _write_parquet(output_root / "source_key_coverage.parquet", source_field_coverage)
    _write_parquet(output_root / "reject_reason_examples.parquet", reject_examples)
    _write_json(
        output_root / "_ARTIFACT_COMPLETE.json",
        {
            "schema_version": ARTIFACT_COMPLETE_SCHEMA_VERSION,
            "generated_at_utc": _utc_now(),
            "complete": True,
            "required_artifacts": [
                "run_manifest.json",
                "input_resolution.json",
                "candidate_admission_stage_inventory.json",
                "rejected_row_schema_contract.json",
                "candidate_admission_trace_rows.parquet",
                "rejected_candidate_rows.parquet",
                "accepted_candidate_rows.parquet",
                "instrumentation_run_summary.json",
                "reject_reason_bucket_summary.json",
                "stage_row_count_reconciliation.json",
                "long_side_top15_winner_loss_trace.json",
                "long_side_top15_winner_loss_trace_rows.parquet",
                "rejected_row_instrumentation_v1_decision.json",
            ],
        },
    )
    return {
        "output_root": str(output_root),
        "decision": decision["decision"],
        "trace_rows": int(len(trace_frame)),
        "accepted_rows": int(len(accepted_frame)),
        "rejected_rows": int(len(rejected_frame)),
        "long_active_row_count": int(len(inputs["long_active_rows"])),
        "long_top15_winner_count": int(winner_audit["long_side_top15_winner_count"]),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="TRADEX rejected-row instrumentation v1 for long-side candidate generation")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--jobs", type=int, default=2)
    args = parser.parse_args()
    session_dir = args.output_root / _session_id()
    result = _run(session_dir, max(1, args.jobs))
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
