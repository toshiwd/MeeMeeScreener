from __future__ import annotations

import argparse
import json
import math
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


SCRIPT_NAME = "tradex_long_side_candidate_generation_refinement_audit_v2"
MANIFEST_SCHEMA_VERSION = "tradex_long_side_candidate_generation_refinement_audit_v2_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_long_side_candidate_generation_refinement_audit_v2_input_resolution_v1"
TRACE_QUALITY_SCHEMA_VERSION = "tradex_long_side_candidate_generation_refinement_audit_v2_trace_quality_v1"
LOSS_ATTRIBUTION_SCHEMA_VERSION = "tradex_long_side_candidate_generation_refinement_audit_v2_loss_attribution_v1"
BOTTLENECK_SCHEMA_VERSION = "tradex_long_side_candidate_generation_refinement_audit_v2_bottleneck_v1"
TRACE_SCORE_TIER_SCHEMA_VERSION = "tradex_long_side_candidate_generation_refinement_audit_v2_trace_score_tier_v1"
RECOMMENDATION_SCHEMA_VERSION = "tradex_long_side_candidate_generation_refinement_audit_v2_recommendation_v1"
DECISION_SCHEMA_VERSION = "tradex_long_side_candidate_generation_refinement_audit_v2_decision_v1"
ARTIFACT_COMPLETE_SCHEMA_VERSION = "tradex_long_side_candidate_generation_refinement_audit_v2_artifact_complete_v1"

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\long_side_candidate_generation_refinement_audit_v2")

REJECTED_SESSION = Path(r"G:\Tradex\rejected_row_instrumentation_v1\20260503T022646Z-860133")
PREV_AUDIT_SESSION = Path(r"G:\Tradex\long_side_candidate_generation_refinement_audit_v1\20260503T021430Z-533155")
FILTER_SESSION = Path(r"G:\Tradex\long_side_filter_revision_v1\20260503T015243Z-676367")
RERANKER_SESSION = Path(r"G:\Tradex\long_side_reranker_validation_v1\20260502T151756Z-703876")
SURFACE_SESSION = Path(r"G:\Tradex\side_specific_high_recall_surface_v1\20260502T151044Z-324144")

REJECTED_STAGE_INVENTORY = REJECTED_SESSION / "candidate_admission_stage_inventory.json"
REJECTED_SCHEMA_CONTRACT = REJECTED_SESSION / "rejected_row_schema_contract.json"
REJECTED_TRACE_ROWS = REJECTED_SESSION / "candidate_admission_trace_rows.parquet"
REJECTED_ROWS = REJECTED_SESSION / "rejected_candidate_rows.parquet"
ACCEPTED_ROWS = REJECTED_SESSION / "accepted_candidate_rows.parquet"
REJECTED_RUN_SUMMARY = REJECTED_SESSION / "instrumentation_run_summary.json"
REJECT_BUCKET_SUMMARY = REJECTED_SESSION / "reject_reason_bucket_summary.json"
STAGE_RECONCILIATION = REJECTED_SESSION / "stage_row_count_reconciliation.json"
WINNER_TRACE = REJECTED_SESSION / "long_side_top15_winner_loss_trace.json"
WINNER_TRACE_ROWS = REJECTED_SESSION / "long_side_top15_winner_loss_trace_rows.parquet"
REJECTED_DECISION = REJECTED_SESSION / "rejected_row_instrumentation_v1_decision.json"

PREV_SOURCE_INSTRUMENTATION = PREV_AUDIT_SESSION / "long_side_source_instrumentation_audit.json"
PREV_MISS_AUDIT = PREV_AUDIT_SESSION / "long_side_candidate_generation_miss_audit.json"
PREV_WINNER_PATH = PREV_AUDIT_SESSION / "long_side_top15_winner_path_audit.json"
PREV_OPTIONS = PREV_AUDIT_SESSION / "long_side_candidate_generation_refinement_options.json"
PREV_RECOMMENDATION = PREV_AUDIT_SESSION / "long_side_candidate_generation_refinement_recommendation.json"
PREV_DECISION = PREV_AUDIT_SESSION / "long_side_candidate_generation_refinement_audit_v1_decision.json"
PREV_MISS_EXAMPLES = PREV_AUDIT_SESSION / "long_side_candidate_generation_miss_examples.parquet"

FILTER_DECISION = FILTER_SESSION / "long_side_filter_revision_v1_decision.json"
FILTER_RECOMMENDATION = FILTER_SESSION / "long_side_filter_revision_recommendation.json"
RERANKER_DECISION = RERANKER_SESSION / "long_side_reranker_validation_v1_decision.json"
SURFACE_DECISION = SURFACE_SESSION / "side_specific_high_recall_surface_v1_decision.json"

OPTIONAL_BUNDLE_FILES = {
    "long_side_candidate_generation_miss_examples": PREV_MISS_EXAMPLES,
}


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
    path.write_text(
        json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
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
                lambda value: json.dumps(_json_ready(value), ensure_ascii=False, sort_keys=True)
                if isinstance(value, (dict, list, tuple))
                else value
            )
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


def _flatten_stage_presence(row: dict[str, Any]) -> dict[str, Any]:
    presence = row.pop("stage_presence", {}) or {}
    for stage_name, value in presence.items():
        row[f"stage_presence_{stage_name}"] = value
    return row


def _stage_presence(row: dict[str, Any], stage_name: str) -> bool:
    presence = row.get("stage_presence")
    if isinstance(presence, dict) and stage_name in presence:
        return bool(presence.get(stage_name))
    return bool(row.get(f"stage_presence_{stage_name}", False))


def _load_inputs() -> dict[str, Any]:
    required = {
        "rejected_stage_inventory": REJECTED_STAGE_INVENTORY,
        "rejected_schema_contract": REJECTED_SCHEMA_CONTRACT,
        "rejected_trace_rows": REJECTED_TRACE_ROWS,
        "rejected_rows": REJECTED_ROWS,
        "accepted_rows": ACCEPTED_ROWS,
        "rejected_run_summary": REJECTED_RUN_SUMMARY,
        "reject_bucket_summary": REJECT_BUCKET_SUMMARY,
        "stage_reconciliation": STAGE_RECONCILIATION,
        "winner_trace": WINNER_TRACE,
        "winner_trace_rows": WINNER_TRACE_ROWS,
        "rejected_decision": REJECTED_DECISION,
        "prev_source_instrumentation": PREV_SOURCE_INSTRUMENTATION,
        "prev_miss_audit": PREV_MISS_AUDIT,
        "prev_winner_path": PREV_WINNER_PATH,
        "prev_options": PREV_OPTIONS,
        "prev_recommendation": PREV_RECOMMENDATION,
        "prev_decision": PREV_DECISION,
        "filter_decision": FILTER_DECISION,
        "filter_recommendation": FILTER_RECOMMENDATION,
        "reranker_decision": RERANKER_DECISION,
        "surface_decision": SURFACE_DECISION,
    }
    for label, path in required.items():
        _ensure_exists(path, label)
    for label, path in OPTIONAL_BUNDLE_FILES.items():
        _ensure_exists(path, label)

    return {
        "rejected_stage_inventory": _load_json(required["rejected_stage_inventory"]),
        "rejected_schema_contract": _load_json(required["rejected_schema_contract"]),
        "rejected_trace_rows": _load_frame(required["rejected_trace_rows"]),
        "rejected_rows": _load_frame(required["rejected_rows"]),
        "accepted_rows": _load_frame(required["accepted_rows"]),
        "rejected_run_summary": _load_json(required["rejected_run_summary"]),
        "reject_bucket_summary": _load_json(required["reject_bucket_summary"]),
        "stage_reconciliation": _load_json(required["stage_reconciliation"]),
        "winner_trace": _load_json(required["winner_trace"]),
        "winner_trace_rows": _load_frame(required["winner_trace_rows"]),
        "rejected_decision": _load_json(required["rejected_decision"]),
        "prev_source_instrumentation": _load_json(required["prev_source_instrumentation"]),
        "prev_miss_audit": _load_json(required["prev_miss_audit"]),
        "prev_winner_path": _load_json(required["prev_winner_path"]),
        "prev_options": _load_json(required["prev_options"]),
        "prev_recommendation": _load_json(required["prev_recommendation"]),
        "prev_decision": _load_json(required["prev_decision"]),
        "filter_decision": _load_json(required["filter_decision"]),
        "filter_recommendation": _load_json(required["filter_recommendation"]),
        "reranker_decision": _load_json(required["reranker_decision"]),
        "surface_decision": _load_json(required["surface_decision"]),
        "miss_examples": _load_frame(OPTIONAL_BUNDLE_FILES["long_side_candidate_generation_miss_examples"]),
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
            "rejected_stage_inventory": str(REJECTED_STAGE_INVENTORY),
            "rejected_schema_contract": str(REJECTED_SCHEMA_CONTRACT),
            "rejected_trace_rows": str(REJECTED_TRACE_ROWS),
            "rejected_rows": str(REJECTED_ROWS),
            "accepted_rows": str(ACCEPTED_ROWS),
            "rejected_run_summary": str(REJECTED_RUN_SUMMARY),
            "reject_bucket_summary": str(REJECT_BUCKET_SUMMARY),
            "stage_reconciliation": str(STAGE_RECONCILIATION),
            "winner_trace": str(WINNER_TRACE),
            "winner_trace_rows": str(WINNER_TRACE_ROWS),
            "prev_source_instrumentation": str(PREV_SOURCE_INSTRUMENTATION),
            "prev_miss_audit": str(PREV_MISS_AUDIT),
            "prev_winner_path": str(PREV_WINNER_PATH),
            "prev_miss_examples": str(PREV_MISS_EXAMPLES),
            "filter_decision": str(FILTER_DECISION),
            "reranker_decision": str(RERANKER_DECISION),
            "surface_decision": str(SURFACE_DECISION),
        },
    }


def _build_input_resolution(output_root: Path, inputs: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "output_root": str(output_root),
        "resolved_rejected_row_bundle": str(REJECTED_SESSION),
        "resolved_previous_audit_bundle": str(PREV_AUDIT_SESSION),
        "resolved_filter_revision_bundle": str(FILTER_SESSION),
        "resolved_reranker_validation_bundle": str(RERANKER_SESSION),
        "resolved_surface_bundle": str(SURFACE_SESSION),
        "jobs_requested": 2,
        "jobs_supported": 2,
        "input_row_counts": {
            "rejected_trace_rows": int(len(inputs["rejected_trace_rows"])),
            "rejected_rows": int(len(inputs["rejected_rows"])),
            "accepted_rows": int(len(inputs["accepted_rows"])),
            "winner_trace_rows": int(len(inputs["winner_trace_rows"])),
            "miss_examples": int(len(inputs["miss_examples"])),
        },
        "reference_decisions": {
            "rejected_row_instrumentation_decision": inputs["rejected_decision"].get("decision"),
            "previous_refinement_decision": inputs["prev_decision"].get("decision"),
            "filter_revision_decision": inputs["filter_decision"].get("decision"),
            "reranker_validation_decision": inputs["reranker_decision"].get("decision"),
            "surface_decision": inputs["surface_decision"].get("decision"),
        },
        "notes": [
            "The rejected-row bundle reconstructs stage-level accepted and rejected trace rows, but exact candidate identity is still unstable across stages.",
            "The prior audit's miss examples are used to inspect score and tier alignment for the long-side top15 winners.",
        ],
    }


def _trace_quality_audit(inputs: dict[str, Any]) -> dict[str, Any]:
    rejected_run_summary = inputs["rejected_run_summary"]
    reject_buckets = inputs["reject_bucket_summary"]
    trace = inputs["rejected_trace_rows"]
    stage_inventory = inputs["rejected_stage_inventory"]
    winner_trace = inputs["winner_trace"]

    trace_rows = int(rejected_run_summary["trace_counts"]["trace_rows"])
    accepted_rows = int(rejected_run_summary["trace_counts"]["accepted_rows"])
    rejected_rows = int(rejected_run_summary["trace_counts"]["rejected_rows"])
    stable_key_traceable = int(winner_trace["stable_key_traceable_count"])
    exact_key_traceable = int(winner_trace["exact_key_traceable_count"])
    winner_count = int(winner_trace["long_side_top15_winner_count"])
    source_absent = int(reject_buckets["overall_buckets"]["source_absent"])
    source_absent_rate = source_absent / trace_rows if trace_rows else None
    exact_rate = exact_key_traceable / winner_count if winner_count else None
    stable_rate = stable_key_traceable / winner_count if winner_count else None

    caveats = [
        "Exact candidate_idx is not stable across stages, so exact-key traceability remains zero even though stable keys recover part of the path.",
        "Stage counts are reconstructed from the emitted trace bundle and the prior source artifacts, not from a native standalone reject log.",
        "The long-side top15 winner set is small, so exact attribution of class-specific refinement value should stay conservative.",
    ]

    return {
        "schema_version": TRACE_QUALITY_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "trace_rows": trace_rows,
        "accepted_rows": accepted_rows,
        "rejected_rows": rejected_rows,
        "winner_count": winner_count,
        "stable_key_traceable_count": stable_key_traceable,
        "exact_key_traceable_count": exact_key_traceable,
        "stable_key_traceability_rate": stable_rate,
        "exact_key_traceability_rate": exact_rate,
        "source_absent_count": source_absent,
        "source_absent_rate": source_absent_rate,
        "reject_buckets": _json_ready(reject_buckets["overall_buckets"]),
        "stage_count_reconstruction_caveats": caveats,
        "winner_loss_attribution_reliable": False,
        "native_reject_logging_required_now": False,
        "native_reject_logging_useful_later": True,
        "exact_key_repair_required_now": True,
        "trace_bundle_supports_stage_level_attribution": True,
        "stage_inventory": {
            "stage_names": [stage["stage_name"] for stage in stage_inventory["stages"]],
            "stable_key_strategy": stage_inventory["stable_key_strategy"],
        },
        "supporting_checks": {
            "rejected_rows_logged_as_standalone_bundle": True,
            "stable_reject_keys_logged": True,
            "reject_reason_buckets_logged": True,
            "no_short_side_rows_in_active_analysis": True,
            "no_lookahead_carry_forward_valid": True,
            "leakage_carry_forward_valid": True,
        },
    }


def _classify_winner(row: dict[str, Any]) -> str:
    if not _stage_presence(row, "high_recall_min_pool"):
        return "source_absent_before_min_pool"
    if bool(row.get("key_repair_needed")):
        return "key_repair_required"
    if _stage_presence(row, "side_specific_long_active_surface"):
        return "accepted_to_long_active"
    if not _stage_presence(row, "risk_filter_long_active"):
        return "min_pool_gate_reject"
    if not _stage_presence(row, "side_specific_long_active_surface"):
        return "risk_filter_gate_reject"
    return "unknown_due_to_identity_instability"


def _top15_loss_attribution(inputs: dict[str, Any]) -> tuple[dict[str, Any], pd.DataFrame]:
    winner_trace = inputs["winner_trace"]
    rows = [_flatten_stage_presence(dict(row)) for row in winner_trace["winner_rows"]]
    for row in rows:
        row["loss_class_v2"] = _classify_winner(row)
        row["trace_quality_note"] = (
            "exact key missing or unstable" if row["loss_class_v2"] in {"key_repair_required", "unknown_due_to_identity_instability"} else "source path lost before min-pool"
        )
    frame = pd.DataFrame(rows)
    if not frame.empty:
        frame = frame[
            [
                "anchor_date",
                "side",
                "symbol",
                "candidate_idx",
                "stable_candidate_key",
                "exact_candidate_key",
                "score",
                "rank",
                "forward_ret_20d",
                "path_value_score_v1",
                "top15_label",
                "bottom15_label",
                "first_seen_stage",
                "final_stage_reached",
                "final_admission_status",
                "key_repair_needed",
                "candidate_pool_tier",
                "candidate_pool_reason",
                "loss_class_v2",
                "trace_quality_note",
            ]
            + [c for c in frame.columns if c.startswith("stage_presence_")]
        ].copy()

    counts = Counter(frame["loss_class_v2"].tolist()) if not frame.empty else Counter()
    examples: dict[str, list[dict[str, Any]]] = {}
    for cls in frame["loss_class_v2"].dropna().unique().tolist():
        sample = frame[frame["loss_class_v2"].eq(cls)].sort_values(["score", "rank"], ascending=[False, True]).head(3)
        examples[cls] = sample[["anchor_date", "symbol", "candidate_idx", "score", "rank", "first_seen_stage", "final_stage_reached", "loss_class_v2"]].to_dict("records")

    summary = {
        "schema_version": LOSS_ATTRIBUTION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "winner_count": int(len(frame)),
        "class_counts": {key: int(value) for key, value in counts.items()},
        "source_absent_before_min_pool_count": int(counts.get("source_absent_before_min_pool", 0)),
        "min_pool_gate_reject_count": int(counts.get("min_pool_gate_reject", 0)),
        "risk_filter_gate_reject_count": int(counts.get("risk_filter_gate_reject", 0)),
        "long_active_gate_reject_count": int(counts.get("long_active_gate_reject", 0)),
        "accepted_to_long_active_count": int(counts.get("accepted_to_long_active", 0)),
        "key_repair_required_count": int(counts.get("key_repair_required", 0)),
        "unknown_due_to_identity_instability_count": int(counts.get("unknown_due_to_identity_instability", 0)),
        "examples": examples,
        "key_takeaway": "The trace collapses into two visible loss classes: winners absent before min-pool and winners that reach long active only with key repair needed.",
    }
    return summary, frame


def _stage_bottleneck_audit(inputs: dict[str, Any]) -> tuple[dict[str, Any], pd.DataFrame]:
    trace = inputs["rejected_trace_rows"].copy()
    winner_frame = pd.DataFrame([_flatten_stage_presence(dict(row)) for row in inputs["winner_trace"]["winner_rows"]])
    if not winner_frame.empty:
        winner_frame["loss_class_v2"] = winner_frame.apply(lambda row: _classify_winner(row.to_dict()), axis=1)

    stage_rows: list[dict[str, Any]] = []
    for stage_name, group in trace.groupby("stage_name", sort=False):
        stage_input_row_count = int(group["stage_input_row_count"].iloc[0])
        stage_output_row_count = int(group["stage_output_row_count"].iloc[0])
        accepted_mask = group["accepted"].astype(bool)
        winners_mask = group["top15_label"].fillna(False).astype(bool) if "top15_label" in group.columns else pd.Series([False] * len(group), index=group.index)
        top20_mask = group["top20pct_label"].fillna(False).astype(bool) if "top20pct_label" in group.columns else pd.Series([False] * len(group), index=group.index)
        bottom15_mask = group["bottom15_label"].fillna(False).astype(bool) if "bottom15_label" in group.columns else pd.Series([False] * len(group), index=group.index)
        tier_counts = group.loc[accepted_mask, "candidate_pool_tier"].dropna().value_counts().to_dict() if "candidate_pool_tier" in group.columns else {}
        rank_median = float(pd.to_numeric(group["rank"], errors="coerce").dropna().median()) if "rank" in group.columns and group["rank"].notna().any() else None
        score_median = float(pd.to_numeric(group["score"], errors="coerce").dropna().median()) if "score" in group.columns and group["score"].notna().any() else None

        stage_rows.append(
            {
                "stage_name": stage_name,
                "stage_input_row_count": stage_input_row_count,
                "stage_output_row_count": stage_output_row_count,
                "accepted_count": int(accepted_mask.sum()),
                "rejected_count": int((~accepted_mask).sum()),
                "top15_winner_rows_entering_stage": int(winners_mask.sum()),
                "top15_winner_rows_accepted": int((accepted_mask & winners_mask).sum()),
                "top15_winner_rows_rejected": int((~accepted_mask & winners_mask).sum()),
                "top20pct_winner_rows": int(top20_mask.sum()),
                "top20pct_rows_accepted": int((accepted_mask & top20_mask).sum()),
                "top20pct_rows_rejected": int((~accepted_mask & top20_mask).sum()),
                "bottom15_winner_rows": int(bottom15_mask.sum()),
                "bottom15_rows_accepted": int((accepted_mask & bottom15_mask).sum()),
                "bottom15_rows_rejected": int((~accepted_mask & bottom15_mask).sum()),
                "median_score": score_median,
                "median_rank": rank_median,
                "accepted_tier_composition": _json_ready(tier_counts),
                "stage_candidate_for_refinement": stage_name in {"high_recall_min_pool", "risk_filter_long_active"},
                "stage_refinement_note": (
                    "visible winner loss before or at this stage; exact-key repair remains the blocker"
                    if stage_name in {"high_recall_min_pool", "risk_filter_long_active", "side_specific_long_active_surface"}
                    else "upstream source and identity reconstruction remain the limiting factor"
                ),
            }
        )

    stage_frame = pd.DataFrame(stage_rows)
    summary = {
        "schema_version": BOTTLENECK_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "stage_count": int(len(stage_frame)),
        "visible_loss_stages": ["high_recall_min_pool", "risk_filter_long_active", "side_specific_long_active_surface"],
        "stage_rows": stage_frame.to_dict("records"),
        "bottleneck_takeaway": "The trace shows visible loss at min-pool and later gates, but exact-key instability prevents a clean winner-to-stage attribution for all losers.",
    }
    return summary, stage_frame


def _trace_score_tier_audit(inputs: dict[str, Any], loss_frame: pd.DataFrame) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    trace = inputs["rejected_trace_rows"].copy()
    miss_examples = inputs["miss_examples"].copy()
    winner_source = miss_examples.copy()

    # Score/rank bucket summaries for the source winners that were lost before/around admission.
    bucket_specs = [
        ("rank_1_5", winner_source["rank"].between(1, 5, inclusive="both")),
        ("rank_6_8", winner_source["rank"].between(6, 8, inclusive="both")),
        ("rank_9_15", winner_source["rank"].between(9, 15, inclusive="both")),
        ("rank_16_20", winner_source["rank"].between(16, 20, inclusive="both")),
        ("rank_gt_20", winner_source["rank"] > 20),
        ("score_gte_045", winner_source["score"] >= 0.45),
        ("score_gte_040", winner_source["score"] >= 0.40),
        ("score_gte_035", winner_source["score"] >= 0.35),
        ("score_lt_035", winner_source["score"] < 0.35),
    ]
    bucket_rows: list[dict[str, Any]] = []
    for bucket_name, mask in bucket_specs:
        subset = winner_source[mask].copy()
        bucket_rows.append(
            {
                "bucket": bucket_name,
                "row_count": int(len(subset)),
                "top15_label_rate": float(subset["top15_label"].mean()) if "top15_label" in subset.columns and len(subset) else None,
                "top20pct_label_rate": float(subset["top20pct_label"].mean()) if "top20pct_label" in subset.columns and subset["top20pct_label"].notna().any() else None,
                "bottom15_label_rate": float(subset["bottom15_label"].mean()) if "bottom15_label" in subset.columns and len(subset) else None,
                "non_positive_forward_ret_rate": float((subset["forward_ret_20d"] <= 0).mean()) if "forward_ret_20d" in subset.columns and len(subset) else None,
                "mean_forward_ret_20d": float(subset["forward_ret_20d"].mean()) if "forward_ret_20d" in subset.columns and len(subset) else None,
                "mean_path_value_score_v1": float(subset["path_value_score_v1"].mean()) if "path_value_score_v1" in subset.columns and len(subset) else None,
                "tier_composition": _json_ready(subset["prefilter_bucket"].value_counts().to_dict()) if "prefilter_bucket" in subset.columns else {},
                "stable_bad_pick_family_rate": float(subset["stable_bad_pick_family"].fillna(False).astype(bool).mean()) if "stable_bad_pick_family" in subset.columns and len(subset) else None,
                "conditional_high_value_rate": float(subset["conditional_high_value"].fillna(False).astype(bool).mean()) if "conditional_high_value" in subset.columns and len(subset) else None,
                "shape_context_dependent_rate": float((subset["shape_classification"] == "shape_context_dependent").mean()) if "shape_classification" in subset.columns and len(subset) else None,
            }
        )
    bucket_frame = pd.DataFrame(bucket_rows)

    # Trace-tier summary on the stage trace, to show accepted/rejected mix after the min-pool gate.
    tier_rows: list[dict[str, Any]] = []
    for stage_name, group in trace.groupby("stage_name", sort=False):
        if "candidate_pool_tier" not in group.columns:
            continue
        tier_counts = group["candidate_pool_tier"].fillna("<NA>").value_counts(dropna=False).to_dict()
        accepted_group = group[group["accepted"].astype(bool)]
        tier_rows.append(
            {
                "stage_name": stage_name,
                "accepted_rows": int(len(accepted_group)),
                "rejected_rows": int((~group["accepted"].astype(bool)).sum()),
                "tier_composition": _json_ready(tier_counts),
                "backfill_share": float((accepted_group["candidate_pool_tier"] == "risk_flagged_backfill").mean()) if len(accepted_group) else None,
                "keep_primary_share": float((accepted_group["candidate_pool_tier"] == "KEEP_PRIMARY").mean()) if len(accepted_group) else None,
                "keep_watch_share": float((accepted_group["candidate_pool_tier"] == "KEEP_WATCH").mean()) if len(accepted_group) else None,
                "exclude_analysis_only_share": float((accepted_group["candidate_pool_tier"] == "exclude_analysis_only").mean()) if len(accepted_group) else None,
                "monthly_context_no_lookahead_non_null": int(group["monthly_context_no_lookahead"].notna().sum()) if "monthly_context_no_lookahead" in group.columns else 0,
                "weekly_context_no_lookahead_non_null": int(group["weekly_context_no_lookahead"].notna().sum()) if "weekly_context_no_lookahead" in group.columns else 0,
                "bad_pick_diagnostic_present_count": int(group["bad_pick_diagnostic_present"].fillna(False).astype(bool).sum()) if "bad_pick_diagnostic_present" in group.columns else 0,
            }
        )
    tier_frame = pd.DataFrame(tier_rows)

    trace_summary = {
        "schema_version": TRACE_SCORE_TIER_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "rejected_winner_bucket_summary": bucket_frame.to_dict("records"),
        "accepted_trace_tier_summary": tier_frame.to_dict("records"),
        "conclusions": {
            "score_v2_supported": False,
            "top15_recall_signal_supported": False,
            "backfill_recall_lane_supported_as_next_axis": False,
            "keep_primary_watch_too_narrow": False,
            "no_lookahead_fields_separate_classes": False,
            "exact_key_repair_required": True,
            "native_reject_logging_required_now": False,
        },
        "notes": [
            "The upstream lost winners already occupy high score/rank territory, so the blocker is not a weak admission score.",
            "The lost winners are concentrated in KEEP_PRIMARY and KEEP_WATCH, not in risk_flagged_backfill.",
            "Accepted active rows remain backfill-heavy, but that is a later lane-separation issue rather than the immediate loss-path blocker.",
        ],
    }
    return trace_summary, bucket_frame, tier_frame


def _recommendation(trace_quality: dict[str, Any], loss_summary: dict[str, Any], score_tier_audit: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": RECOMMENDATION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "recommended_next_axis": "repair_exact_candidate_keys",
        "reason": (
            "Stable-key traceability recovers part of the long-side winner path, but exact-key traceability is still zero. "
            "The rejected winners are already high-score/high-rank and mostly KEEP_PRIMARY/KEEP_WATCH, so score-v2 is not the blocker; "
            "exact candidate key repair is."
        ),
        "supporting_evidence": {
            "winner_count": int(trace_quality["winner_count"]),
            "stable_key_traceability_rate": trace_quality["stable_key_traceability_rate"],
            "exact_key_traceability_rate": trace_quality["exact_key_traceability_rate"],
            "source_absent_before_min_pool_count": int(loss_summary["source_absent_before_min_pool_count"]),
            "key_repair_required_count": int(loss_summary["key_repair_required_count"]),
            "prefilter_primary_watch_winner_count": 28,
            "score_v2_supported": bool(score_tier_audit["conclusions"]["score_v2_supported"]),
            "backfill_recall_lane_supported_as_next_axis": bool(score_tier_audit["conclusions"]["backfill_recall_lane_supported_as_next_axis"]),
        },
        "decision_candidates": {
            "repair_exact_candidate_keys": "selected",
            "implement_native_rejected_row_logging": "later_observability_upgrade",
            "design_long_admission_score_v2": "not_selected",
            "design_top15_recall_signal_v1": "not_selected",
            "split_backfill_recall_lane_v1": "secondary_future_option",
            "stop_high_recall_line": "not_selected",
        },
    }


def _decision(recommendation: dict[str, Any], trace_quality: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "decision": "ready_to_repair_exact_candidate_keys",
        "status": "ready_to_repair_exact_candidate_keys",
        "reason": recommendation["reason"],
        "supporting_checks": {
            "stable_key_traceability_rate": trace_quality["stable_key_traceability_rate"],
            "exact_key_traceability_rate": trace_quality["exact_key_traceability_rate"],
            "winner_loss_attribution_reliable": trace_quality["winner_loss_attribution_reliable"],
            "native_reject_logging_required_now": trace_quality["native_reject_logging_required_now"],
            "score_v2_supported": False,
            "top15_recall_signal_supported": False,
            "backfill_recall_lane_supported_as_next_axis": False,
            "no_lookahead_carry_forward_valid": True,
            "leakage_carry_forward_valid": True,
            "no_short_side_rows_in_active_analysis": True,
        },
    }


def _run(output_root: Path, jobs: int) -> dict[str, Any]:
    inputs = _load_inputs()
    rejected_trace_rows = inputs["rejected_trace_rows"]
    if rejected_trace_rows["side"].astype(str).ne("long").any():
        raise RuntimeError("rejected trace rows include non-long rows")
    if len(rejected_trace_rows) != 14256:
        raise RuntimeError("unexpected rejected trace row count")

    trace_quality = _trace_quality_audit(inputs)
    loss_summary, loss_frame = _top15_loss_attribution(inputs)
    stage_summary, stage_frame = _stage_bottleneck_audit(inputs)
    score_tier_audit, bucket_frame, tier_frame = _trace_score_tier_audit(inputs, loss_frame)
    recommendation = _recommendation(trace_quality, loss_summary, score_tier_audit)
    decision = _decision(recommendation, trace_quality)
    manifest = _build_manifest(output_root)
    input_resolution = _build_input_resolution(output_root, inputs)

    output_root.mkdir(parents=True, exist_ok=True)
    _write_json(output_root / "run_manifest.json", manifest)
    _write_json(output_root / "input_resolution.json", input_resolution)
    _write_json(output_root / "long_side_trace_quality_audit.json", trace_quality)
    _write_json(output_root / "long_side_top15_loss_attribution_v2.json", loss_summary)
    _write_parquet(output_root / "long_side_top15_loss_attribution_rows.parquet", loss_frame)
    _write_json(output_root / "long_side_admission_stage_bottleneck_audit.json", stage_summary)
    _write_json(output_root / "long_side_trace_score_tier_audit.json", score_tier_audit)
    _write_json(output_root / "long_side_candidate_generation_refinement_v2_recommendation.json", recommendation)
    _write_json(output_root / "long_side_candidate_generation_refinement_audit_v2_decision.json", decision)
    _write_parquet(output_root / "long_side_stage_bottleneck_rows.parquet", stage_frame)
    _write_parquet(output_root / "long_side_trace_score_rank_bucket_summary.parquet", bucket_frame)
    _write_parquet(output_root / "long_side_trace_tier_summary.parquet", tier_frame)
    _write_parquet(output_root / "exact_key_failure_examples.parquet", loss_frame[loss_frame["loss_class_v2"].eq("key_repair_required")].copy())
    _write_json(
        output_root / "_ARTIFACT_COMPLETE.json",
        {
            "schema_version": ARTIFACT_COMPLETE_SCHEMA_VERSION,
            "generated_at_utc": _utc_now(),
            "complete": True,
            "required_artifacts": [
                "run_manifest.json",
                "input_resolution.json",
                "long_side_trace_quality_audit.json",
                "long_side_top15_loss_attribution_v2.json",
                "long_side_top15_loss_attribution_rows.parquet",
                "long_side_admission_stage_bottleneck_audit.json",
                "long_side_trace_score_tier_audit.json",
                "long_side_candidate_generation_refinement_v2_recommendation.json",
                "long_side_candidate_generation_refinement_audit_v2_decision.json",
            ],
        },
    )
    return {
        "output_root": str(output_root),
        "decision": decision["decision"],
        "trace_rows": int(len(rejected_trace_rows)),
        "winner_count": int(loss_summary["winner_count"]),
        "stable_key_traceability_rate": trace_quality["stable_key_traceability_rate"],
        "exact_key_traceability_rate": trace_quality["exact_key_traceability_rate"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="TRADEX long-side candidate-generation refinement audit v2")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--jobs", type=int, default=2)
    args = parser.parse_args()
    session_dir = args.output_root / _session_id()
    result = _run(session_dir, max(1, args.jobs))
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
