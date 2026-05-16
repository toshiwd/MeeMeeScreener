from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import tradex_iizuka_signal_expectancy_v1 as base
from scripts import tradex_side_aware_min_pool_feasibility_v1 as min_pool

SCRIPT_NAME = "tradex_point_in_time_candidate_pool_phase2b_v1"
SCHEMA_VERSION = "tradex_point_in_time_candidate_pool_phase2b_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\point_in_time_candidate_pool_contract_v1_phase2b")
CONTRACT_PATH = REPO_ROOT / "docs" / "contracts" / "tradex_point_in_time_candidate_pool_contract_v1.json"
REQUIRED_ARTIFACTS = [
    "run_manifest.json",
    "input_resolution.json",
    "generation_path_inventory.json",
    "point_in_time_instrumentation_plan.json",
    "smoke_candidate_pool_rows.parquet",
    "smoke_contract_validation.json",
    "phase2b_decision.json",
    "_ARTIFACT_COMPLETE.json",
]
REQUIRED_SMOKE_FIELDS = [
    "as_of_date",
    "candidate_date",
    "feature_cutoff_date",
    "symbol",
    "universe_membership",
    "candidate_pool_membership",
    "prefilter_pass",
    "prefilter_reject_reason",
    "champion_score",
    "champion_rank",
    "top5_membership",
    "top10_membership",
    "top20_membership",
    "top50_membership",
    "score_source",
    "feature_source",
    "source_artifact_lineage",
    "no_future_label_used",
    "no_lookahead_status",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _session_id() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{now.microsecond:06d}"


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    base._write_json(path, payload)


def _write_parquet(path: Path, frame: pd.DataFrame) -> None:
    base._write_parquet(path, frame)


def _safe_path(value: str | Path | None, default: Path) -> Path:
    return base._safe_path(value, default)


def _validate(frame: pd.DataFrame) -> dict[str, Any]:
    missing = [field for field in REQUIRED_SMOKE_FIELDS if field not in frame.columns]
    no_future_values = frame["no_future_label_used"].fillna(False).astype(bool) if "no_future_label_used" in frame.columns else pd.Series(dtype=bool)
    no_lookahead_status = frame["no_lookahead_status"].fillna("unknown").astype(str).value_counts().to_dict() if "no_lookahead_status" in frame.columns else {}
    return {
        "schema_version": f"{SCHEMA_VERSION}_smoke_contract_validation_v1",
        "generated_at_utc": _utc_now(),
        "required_fields": REQUIRED_SMOKE_FIELDS,
        "missing_fields": missing,
        "required_field_validation_pass": len(missing) == 0,
        "no_future_label_used_all_true": bool(no_future_values.all()) if len(no_future_values) else False,
        "no_lookahead_status_distribution": no_lookahead_status,
        "full_no_lookahead_verified": False,
        "validation_pass": len(missing) == 0 and bool(no_future_values.all()) if len(no_future_values) else False,
        "warning": "full no-lookahead remains partial because raw selection ledger context flags are only available on overlap-enriched rows",
    }


def run_phase2b(*, output_root: Path, smoke_limit: int = 250) -> dict[str, Any]:
    session_root = output_root / _session_id()
    session_root.mkdir(parents=True, exist_ok=True)
    payload = min_pool.build_artifacts()
    rows = payload["selected_pool"].head(smoke_limit).copy()
    validation = _validate(rows)
    inventory = {
        "schema_version": f"{SCHEMA_VERSION}_generation_path_inventory_v1",
        "generated_at_utc": _utc_now(),
        "selected_generation_path": "scripts/tradex_side_aware_min_pool_feasibility_v1.py::build_artifacts",
        "entrypoint": "python scripts/tradex_side_aware_min_pool_feasibility_v1.py --output-root <G:\\Tradex path>",
        "downstream_native_path": "scripts/tradex_min_pool_reject_provenance_v1.py uses build_artifacts() through tradex_native_rejected_row_logging_v1",
        "input_data_sources": payload["input_resolution"]["used_files"],
        "output_artifacts": [
            "side_aware_min_pool_candidate_rows.parquet",
            "side_aware_min_pool_generation_summary.json",
            "side_aware_min_pool_no_lookahead_audit.json",
        ],
        "computes_candidate_membership": True,
        "computes_champion_score_rank": True,
        "has_as_of_date_candidate_date": True,
        "can_determine_feature_cutoff_date": True,
        "uses_future_labels_during_scoring": False,
        "future_labels_downstream_evaluation_only": True,
    }
    plan = {
        "schema_version": f"{SCHEMA_VERSION}_instrumentation_plan_v1",
        "generated_at_utc": _utc_now(),
        "instrumentation_added": True,
        "scoring_behavior_changed": False,
        "fields_added": REQUIRED_SMOKE_FIELDS,
        "no_future_label_used_policy": "true only for score/rank/candidate construction; evaluation outcome fields remain separate",
        "remaining_gap": "full no-lookahead proof is still partial for raw selection ledger rows without monthly/weekly context flags",
    }
    if validation["validation_pass"]:
        decision = "smoke_ready"
        reason = "bounded smoke candidate pool rows include required point-in-time contract fields without scoring behavior changes"
    else:
        decision = "instrumentation_ready"
        reason = "instrumentation added but smoke validation has missing fields"
    decision_payload = {
        "schema_version": f"{SCHEMA_VERSION}_decision_v1",
        "generated_at_utc": _utc_now(),
        "authoritative_decision": decision,
        "decision_reason": reason,
        "selected_generation_path": inventory["selected_generation_path"],
        "instrumentation_added": True,
        "scoring_behavior_changed": False,
        "smoke_artifact_generated": True,
        "contract_validation_pass": validation["validation_pass"],
        "missing_fields": validation["missing_fields"],
        "meemee_changed": False,
        "production_ranking_changed": False,
        "publish_changed": False,
        "ranking_challenger_created": False,
    }
    artifacts = {
        "run_manifest.json": {"schema_version": f"{SCHEMA_VERSION}_manifest_v1", "generated_at_utc": _utc_now(), "script_name": SCRIPT_NAME, "session_root": str(session_root), "boundary": "TRADEX-only"},
        "input_resolution.json": {"schema_version": f"{SCHEMA_VERSION}_input_resolution_v1", "generated_at_utc": _utc_now(), "contract_path": str(CONTRACT_PATH), "smoke_limit": smoke_limit, "selected_pool_rows": int(len(payload["selected_pool"]))},
        "generation_path_inventory.json": inventory,
        "point_in_time_instrumentation_plan.json": plan,
        "smoke_contract_validation.json": validation,
        "phase2b_decision.json": decision_payload,
    }
    for name, body in artifacts.items():
        _write_json(session_root / name, body)
    _write_parquet(session_root / "smoke_candidate_pool_rows.parquet", rows)
    complete = {"schema_version": f"{SCHEMA_VERSION}_artifact_complete_v1", "generated_at_utc": _utc_now(), "session_root": str(session_root), "required_artifacts": REQUIRED_ARTIFACTS, "all_present": all((session_root / artifact).exists() for artifact in REQUIRED_ARTIFACTS if artifact != "_ARTIFACT_COMPLETE.json")}
    _write_json(session_root / "_ARTIFACT_COMPLETE.json", complete)
    return {"session_root": str(session_root), "decision": decision}


def main() -> None:
    parser = argparse.ArgumentParser(description="TRADEX point-in-time candidate pool Phase 2b instrumentation smoke")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--smoke-limit", type=int, default=250)
    args = parser.parse_args()
    result = run_phase2b(output_root=_safe_path(args.output_root, DEFAULT_OUTPUT_ROOT), smoke_limit=args.smoke_limit)
    print(json.dumps(base._json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
