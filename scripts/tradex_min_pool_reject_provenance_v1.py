from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from shutil import copy2
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.tradex_native_rejected_row_logging_v1 import _run as run_native_bundle


SCRIPT_NAME = "tradex_min_pool_reject_provenance_v1"
SCHEMA_VERSION = "tradex_min_pool_reject_provenance_v1_schema_v1"
DECISION_LOGIC_VERSION = "tradex_min_pool_reject_provenance_v1_decision_logic_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\min_pool_reject_provenance_v1")


def _session_id() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{now.microsecond:06d}"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, float) and (value != value or value in (float("inf"), float("-inf"))):
        return None
    return value


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"missing required source artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    return path


def _copy_artifact(source: Path, target: Path) -> Path:
    if not source.exists():
        raise FileNotFoundError(f"missing required source artifact: {source}")
    target.parent.mkdir(parents=True, exist_ok=True)
    copy2(source, target)
    return target


def _decision_logic_inventory(bundle_root: Path) -> dict[str, Any]:
    hook_inventory = _load_json(bundle_root / "min_pool_reject_hook_inventory.json")
    hooks = hook_inventory.get("hooks", [])
    min_pool_hook = next((hook for hook in hooks if hook.get("stage_name") == "high_recall_min_pool"), {})
    return {
        "schema_version": DECISION_LOGIC_VERSION,
        "generated_at_utc": _utc_now(),
        "script_name": SCRIPT_NAME,
        "bundle_root": str(bundle_root),
        "decision_logic": {
            "stage_name": "high_recall_min_pool",
            "cap_rule": "long cap 40 / short cap 10",
            "tier_priority_rule": "KEEP_PRIMARY < KEEP_WATCH < DOWNGRADE < risk_flagged_backfill < exclude_analysis_only",
            "score_rank_guard": "pool_priority asc, score desc, champion_rank asc, symbol asc",
            "side_specific_min_max_rule": "min_target 20 on long / 5 on short; cap 40 on long / 10 on short",
            "dedupe_rule": "duplicate_key_excluded rows are rejected before min-pool admission",
            "group_size_rule": "group_candidate_count_before_cap and group_candidate_count_after_cap track cap pressure",
            "missing_key_rule": "missing anchor_date, side, symbol, or candidate_idx maps to min_pool_missing_required_key",
            "unavailable_field_rule": "rows that cannot be classified beyond the cap boundary fall back to min_pool_unclassified_reject or min_pool_cap_exhausted_by_rank",
        },
        "min_pool_hook": min_pool_hook,
        "source_hook_inventory": hook_inventory,
    }


def _provenance_schema_contract(bundle_root: Path) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "bundle_root": str(bundle_root),
        "stable_keys": [
            "canonical_candidate_key",
            "anchor_date",
            "side",
            "symbol",
            "candidate_idx",
            "source_row_id",
        ],
        "required_fields": [
            "stage_name",
            "accepted",
            "reject_reason",
            "reject_reason_bucket",
            "reject_subreason",
            "min_pool_rule_name",
            "min_pool_priority_rank",
            "min_pool_candidate_pool_tier_before_reject",
            "min_pool_candidate_pool_reason_before_reject",
            "score",
            "rank",
            "pool_rank",
            "group_candidate_count_before_cap",
            "group_candidate_count_after_cap",
            "group_min_target",
            "group_max_cap",
            "included_for_min_pool_backfill",
            "would_have_been_excluded_under_current_contract",
        ],
        "evaluation_only_fields": [
            "forward_ret_20d",
            "path_value_score_v1",
            "top15_label",
            "top20pct_label",
            "bottom15_label",
        ],
        "notes": [
            "This schema is tracing-only; it does not alter selection behavior.",
            "Outcome fields are evaluation-only.",
        ],
    }


def _materialize_exact_artifacts(bundle_root: Path) -> None:
    mapping = {
        "min_pool_reject_hook_inventory.json": "min_pool_decision_logic_inventory.json",
        "min_pool_candidate_admission_trace_rows.parquet": "min_pool_provenance_trace_rows.parquet",
        "min_pool_rejected_candidate_rows.parquet": "min_pool_rejected_rows.parquet",
        "min_pool_accepted_candidate_rows.parquet": "min_pool_accepted_rows.parquet",
        "min_pool_rejected_row_logging_summary.json": "min_pool_provenance_logging_summary.json",
        "min_pool_reject_reason_bucket_summary.json": "min_pool_reject_subreason_summary.json",
        "min_pool_long_side_refinement_audit.json": "min_pool_refinement_audit.json",
        "min_pool_long_side_refinement_recommendation.json": "min_pool_refinement_recommendation.json",
        "min_pool_rejected_row_logging_v1_decision.json": "min_pool_reject_provenance_v1_decision.json",
    }
    for source_name, target_name in mapping.items():
        _copy_artifact(bundle_root / source_name, bundle_root / target_name)

    _write_json(bundle_root / "min_pool_decision_logic_inventory.json", _decision_logic_inventory(bundle_root))
    _write_json(bundle_root / "min_pool_reject_provenance_schema.json", _provenance_schema_contract(bundle_root))

    audit = _load_json(bundle_root / "min_pool_refinement_audit.json")
    recommendation = _load_json(bundle_root / "min_pool_refinement_recommendation.json")
    decision = _load_json(bundle_root / "min_pool_reject_provenance_v1_decision.json")
    mapped_action = recommendation.get("recommended_next_action")
    if mapped_action == "native_logging_still_insufficient":
        mapped_action = "min_pool_provenance_still_insufficient"
        recommendation["recommended_next_action"] = mapped_action
        recommendation["reason"] = "Exact min-pool tier-before-reject provenance is still missing for source-absent winners."
        audit["decision_hint"] = mapped_action
        decision["decision"] = mapped_action
        decision["status"] = mapped_action
        decision["reason"] = recommendation["reason"]
        _write_json(bundle_root / "min_pool_refinement_audit.json", audit)
        _write_json(bundle_root / "min_pool_refinement_recommendation.json", recommendation)
        _write_json(bundle_root / "min_pool_reject_provenance_v1_decision.json", decision)

    required_artifacts = [
        "run_manifest.json",
        "input_resolution.json",
        "min_pool_decision_logic_inventory.json",
        "min_pool_reject_provenance_schema.json",
        "min_pool_provenance_trace_rows.parquet",
        "min_pool_rejected_rows.parquet",
        "min_pool_accepted_rows.parquet",
        "min_pool_provenance_logging_summary.json",
        "min_pool_reject_subreason_summary.json",
        "min_pool_stage_row_count_reconciliation.json",
        "min_pool_long_side_top15_loss_trace.json",
        "min_pool_long_side_top15_loss_trace_rows.parquet",
        "min_pool_refinement_audit.json",
        "min_pool_refinement_recommendation.json",
        "min_pool_reject_provenance_v1_decision.json",
        "_ARTIFACT_COMPLETE.json",
    ]
    _write_json(
        bundle_root / "_ARTIFACT_COMPLETE.json",
        {
            "schema_version": "tradex_min_pool_reject_provenance_v1_artifact_complete_v1",
            "generated_at_utc": _utc_now(),
            "complete": True,
            "required_artifacts": required_artifacts,
        },
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="TRADEX min-pool reject provenance for long-side candidate generation")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--session-id", default=None)
    parser.add_argument("--jobs", type=int, default=2)
    args = parser.parse_args()
    output_root = Path(str(args.output_root)).expanduser().resolve()
    session_root = output_root / (args.session_id or _session_id())
    result = run_native_bundle(session_root, max(1, args.jobs), artifact_prefix="min_pool")
    _materialize_exact_artifacts(session_root)
    print(json.dumps({"output_root": str(session_root), "decision": result["decision"]}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
