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

SCRIPT_NAME = "tradex_point_in_time_candidate_pool_phase2c_v1"
SCHEMA_VERSION = "tradex_point_in_time_candidate_pool_phase2c_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\point_in_time_candidate_pool_contract_v1_phase2c")
CONTRACT_PATH = REPO_ROOT / "docs" / "contracts" / "tradex_point_in_time_candidate_pool_contract_v1.json"
GENERATION_PATH = "scripts/tradex_side_aware_min_pool_feasibility_v1.py::build_artifacts"
REQUIRED_ARTIFACTS = [
    "run_manifest.json",
    "input_resolution.json",
    "full_generation_manifest.json",
    "full_candidate_pool_rows.parquet",
    "point_in_time_contract_validation.json",
    "full_no_lookahead_audit.json",
    "phase2c_point_in_time_pool_decision.json",
    "_ARTIFACT_COMPLETE.json",
]
REQUIRED_FIELDS = [
    "as_of_date",
    "candidate_date",
    "feature_cutoff_date",
    "symbol",
    "universe_membership",
    "candidate_pool_membership",
    "prefilter_pass",
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
EVALUATION_LABEL_FIELDS = [
    "forward_ret_5d",
    "forward_ret_10d",
    "forward_ret_20d",
    "path_value_score_v1",
    "mfe_20d",
    "mae_20d",
    "ret20",
    "ret63",
    "mae63",
    "proxy_top15_label",
    "proxy_bottom15_label",
    "proxy_top20pct_label",
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


def _load_contract() -> dict[str, Any]:
    return json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))


def _safe_path(value: str | Path | None, default: Path) -> Path:
    return base._safe_path(value, default)


def _validate_contract(rows: pd.DataFrame) -> dict[str, Any]:
    missing = [field for field in REQUIRED_FIELDS if field not in rows.columns]
    null_required = {}
    for field in REQUIRED_FIELDS:
        if field in rows.columns:
            null_required[field] = int(rows[field].isna().sum())
    eval_present = [field for field in EVALUATION_LABEL_FIELDS if field in rows.columns]
    no_future_all_true = bool(rows["no_future_label_used"].fillna(False).astype(bool).all()) if "no_future_label_used" in rows.columns else False
    construction_sources = {
        "score_source_values": rows["score_source"].fillna("unknown").astype(str).value_counts().to_dict() if "score_source" in rows.columns else {},
        "feature_source_values": rows["feature_source"].fillna("unknown").astype(str).value_counts().to_dict() if "feature_source" in rows.columns else {},
        "evaluation_fields_present": eval_present,
        "evaluation_fields_separated_all_true": bool(rows["evaluation_fields_separated"].fillna(False).astype(bool).all()) if "evaluation_fields_separated" in rows.columns else False,
    }
    return {
        "schema_version": f"{SCHEMA_VERSION}_contract_validation_v1",
        "generated_at_utc": _utc_now(),
        "contract_path": str(CONTRACT_PATH),
        "row_count": int(len(rows)),
        "required_fields": REQUIRED_FIELDS,
        "missing_fields": missing,
        "null_required_field_counts": null_required,
        "required_fields_present": len(missing) == 0,
        "no_future_label_used_all_true": no_future_all_true,
        "construction_sources": construction_sources,
        "validation_pass": len(missing) == 0 and no_future_all_true and construction_sources["evaluation_fields_separated_all_true"],
    }


def _audit_no_lookahead(rows: pd.DataFrame, source_no_lookahead: dict[str, Any]) -> dict[str, Any]:
    feature_cutoff = pd.to_datetime(rows.get("feature_cutoff_date"), errors="coerce")
    as_of = pd.to_datetime(rows.get("as_of_date"), errors="coerce")
    candidate = pd.to_datetime(rows.get("candidate_date"), errors="coerce")
    cutoff_le_asof = bool((feature_cutoff <= as_of).fillna(False).all()) if len(rows) else False
    cutoff_le_candidate = bool((feature_cutoff <= candidate).fillna(False).all()) if len(rows) else False
    no_future_all_true = bool(rows["no_future_label_used"].fillna(False).astype(bool).all()) if "no_future_label_used" in rows.columns else False
    explicit_status = bool(rows["no_lookahead_status"].notna().all()) if "no_lookahead_status" in rows.columns else False
    separated = bool(rows["evaluation_fields_separated"].fillna(False).astype(bool).all()) if "evaluation_fields_separated" in rows.columns else False
    full_verified = bool(source_no_lookahead.get("full_pool_verified") is True)
    pass_result = cutoff_le_asof and cutoff_le_candidate and no_future_all_true and explicit_status and separated and full_verified
    return {
        "schema_version": f"{SCHEMA_VERSION}_full_no_lookahead_audit_v1",
        "generated_at_utc": _utc_now(),
        "feature_cutoff_date_lte_as_of_date": cutoff_le_asof,
        "feature_cutoff_date_lte_candidate_date": cutoff_le_candidate,
        "no_future_label_used_all_true": no_future_all_true,
        "evaluation_labels_separated_from_construction_fields": separated,
        "no_lookahead_status_explicit_all_rows": explicit_status,
        "source_full_pool_verified": full_verified,
        "full_no_lookahead_verified": pass_result,
        "source_no_lookahead_audit": source_no_lookahead,
    }


def run_phase2c(*, output_root: Path) -> dict[str, Any]:
    session_root = output_root / _session_id()
    session_root.mkdir(parents=True, exist_ok=True)
    contract = _load_contract()
    payload = min_pool.build_artifacts()
    rows = payload["selected_pool"].copy()
    generation_manifest = {
        "schema_version": f"{SCHEMA_VERSION}_full_generation_manifest_v1",
        "generated_at_utc": _utc_now(),
        "selected_generation_path": GENERATION_PATH,
        "scoring_behavior_changed": False,
        "full_run": True,
        "research_fallback": False,
        "row_count": int(len(rows)),
        "source_resolution": payload["generation_summary"].get("source_resolution"),
        "source_counts": payload["generation_summary"].get("source_counts"),
        "point_in_time_candidate_pool_contract": payload["generation_summary"].get("point_in_time_candidate_pool_contract"),
    }
    validation = _validate_contract(rows)
    no_lookahead = _audit_no_lookahead(rows, payload["no_lookahead_audit"])
    if validation["validation_pass"] and no_lookahead["full_no_lookahead_verified"]:
        decision = "ready"
        reason = "full_candidate_pool_satisfies_contract_and_full_no_lookahead"
    elif validation["validation_pass"] and not no_lookahead["full_no_lookahead_verified"]:
        decision = "reconstructable_with_contract_gap"
        reason = "contract_fields_present_but_full_pool_no_lookahead_is_partial"
    elif validation["missing_fields"]:
        decision = "blocked_missing_sources"
        reason = "required_contract_fields_missing"
    else:
        decision = "blocked_future_label_risk"
        reason = "future_label_exclusion_not_proven"
    decision_payload = {
        "schema_version": f"{SCHEMA_VERSION}_decision_v1",
        "generated_at_utc": _utc_now(),
        "authoritative_decision": decision,
        "decision_reason": reason,
        "selected_generation_path": GENERATION_PATH,
        "full_run": True,
        "research_fallback": False,
        "row_count": int(len(rows)),
        "contract_validation_pass": validation["validation_pass"],
        "full_no_lookahead_verified": no_lookahead["full_no_lookahead_verified"],
        "missing_fields": validation["missing_fields"],
        "future_phase2d_ranking_exposure_retest_allowed": decision == "ready",
        "meemee_changed": False,
        "production_ranking_changed": False,
        "publish_changed": False,
        "ranking_challenger_created": False,
        "scoring_behavior_changed": False,
    }
    artifacts = {
        "run_manifest.json": {
            "schema_version": f"{SCHEMA_VERSION}_manifest_v1",
            "generated_at_utc": _utc_now(),
            "script_name": SCRIPT_NAME,
            "session_root": str(session_root),
            "boundary": "TRADEX-only",
            "meemee_changed": False,
            "production_ranking_changed": False,
            "publish_changed": False,
            "ranking_challenger_created": False,
        },
        "input_resolution.json": {
            "schema_version": f"{SCHEMA_VERSION}_input_resolution_v1",
            "generated_at_utc": _utc_now(),
            "contract_path": str(CONTRACT_PATH),
            "selected_generation_path": GENERATION_PATH,
            "source_roots": payload["input_resolution"].get("source_roots"),
            "used_files": payload["input_resolution"].get("used_files"),
        },
        "full_generation_manifest.json": generation_manifest,
        "point_in_time_contract_validation.json": validation,
        "full_no_lookahead_audit.json": no_lookahead,
        "phase2c_point_in_time_pool_decision.json": decision_payload,
    }
    for name, body in artifacts.items():
        _write_json(session_root / name, body)
    _write_json(session_root / "point_in_time_candidate_pool_contract_snapshot.json", contract)
    _write_parquet(session_root / "full_candidate_pool_rows.parquet", rows)
    complete = {
        "schema_version": f"{SCHEMA_VERSION}_artifact_complete_v1",
        "generated_at_utc": _utc_now(),
        "session_root": str(session_root),
        "required_artifacts": REQUIRED_ARTIFACTS,
        "all_present": all((session_root / artifact).exists() for artifact in REQUIRED_ARTIFACTS if artifact != "_ARTIFACT_COMPLETE.json"),
    }
    _write_json(session_root / "_ARTIFACT_COMPLETE.json", complete)
    return {"session_root": str(session_root), "decision": decision, "row_count": int(len(rows))}


def main() -> None:
    parser = argparse.ArgumentParser(description="TRADEX point-in-time candidate pool Phase 2c full validation")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args()
    result = run_phase2c(output_root=_safe_path(args.output_root, DEFAULT_OUTPUT_ROOT))
    print(json.dumps(base._json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
