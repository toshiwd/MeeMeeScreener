from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

SCRIPT_NAME = "tradex_shadow_reranker_forward_readiness_v1"
SCHEMA_VERSION = "tradex_shadow_reranker_forward_readiness_v1"
MANIFEST_SCHEMA_VERSION = "tradex_shadow_reranker_forward_readiness_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_shadow_reranker_forward_readiness_v1_input_resolution_v1"
SURFACE_DISCOVERY_SCHEMA_VERSION = "tradex_shadow_reranker_forward_readiness_v1_surface_discovery_v1"
OUTCOME_AVAILABILITY_SCHEMA_VERSION = "tradex_shadow_reranker_forward_readiness_v1_forward_outcome_availability_v1"
FEATURE_CONTRACT_SCHEMA_VERSION = "tradex_shadow_reranker_forward_readiness_v1_frozen_feature_contract_check_v1"
DECISION_SCHEMA_VERSION = "tradex_shadow_reranker_forward_readiness_v1_decision_v1"

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\shadow_reranker_forward_readiness_v1")
DEFER_SESSION = Path(r"G:\Tradex\research_defer_summaries\shadow_reranker_forward_validation\20260501T140925Z-567657")
DEFER_DECISION = DEFER_SESSION / "defer_decision.json"
FROZEN_SUMMARY = DEFER_SESSION / "frozen_shadow_challenger_summary.json"
FORWARD_GAP = DEFER_SESSION / "forward_data_gap_summary.json"
REOPEN_CONDITIONS = DEFER_SESSION / "reopen_conditions.json"
FROZEN_FORWARD_WINDOW_END = "2026-01-19"

FROZEN_MODEL_SPEC = Path(r"G:\Tradex\shadow_reranker_challenger_design_v1\20260501T120651Z-643568\shadow_challenger_model_spec.json")


@dataclass(frozen=True)
class SurfaceInfo:
    family: str
    session_dir: Path
    candidate_file: Path
    orfp_file: Path | None
    audit_file: Path | None
    row_count: int
    anchor_date_min: str | None
    anchor_date_max: str | None
    symbol_count: int
    forward_non_null_count: int
    forward_available: bool
    no_lookahead_status: str
    no_lookahead_passed: bool


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _make_session_id() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{now.microsecond:06d}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, set):
        return [_json_ready(item) for item in sorted(value, key=str)]
    if isinstance(value, float) and (value != value or value in (float("inf"), float("-inf"))):
        return None
    if hasattr(value, "__class__") and value.__class__.__name__ == "NAType":
        return None
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    return path


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"missing required source artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _ensure_exists(path: Path, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"missing required artifact for {label}: {path}")


def _safe_path(value: str | Path | None, default: Path) -> Path:
    if value is None or str(value).strip() == "":
        return default.resolve()
    return Path(str(value)).expanduser().resolve()


def _git_hash_or_unknown() -> str:
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        token = (completed.stdout or completed.stderr or "").strip()
        return token or "unknown"
    except Exception:  # pragma: no cover - best effort metadata
        return "unknown"


def _audit_passes(audit: dict[str, Any]) -> bool:
    status = str(audit.get("status", "")).lower()
    if status in {"pass", "passed", "ready_for_evaluation", "ready_for_time_split_evaluation"}:
        return True
    if status in {"fail", "failed", "missing", "error"}:
        return False
    for key in ("candidate_surface", "orfp_surface"):
        surface = audit.get(key)
        if not isinstance(surface, dict):
            continue
        if surface.get("feature_future_fields_used") or surface.get("future_outcome_fields_used"):
            return False
        if int(surface.get("source_date_future_violation_count", 0) or 0) > 0:
            return False
        per_field = surface.get("per_field")
        if isinstance(per_field, dict):
            for field_payload in per_field.values():
                if isinstance(field_payload, dict) and int(field_payload.get("future_violation_count", 0) or 0) > 0:
                    return False
    return "candidate_surface" in audit or "orfp_surface" in audit


def _discover_surface_info(session_dir: Path, family: str) -> SurfaceInfo | None:
    candidate_files = sorted(session_dir.glob("*candidate*enriched*.parquet"))
    if not candidate_files:
        return None
    candidate_file = candidate_files[0]
    orfp_files = sorted(session_dir.glob("*observable_regime_false_positive*enriched*.parquet"))
    audit_files = sorted(session_dir.glob("no_lookahead*.json"))
    audit_file = audit_files[0] if audit_files else None
    df = pd.read_parquet(candidate_file, columns=["anchor_date", "symbol", "forward_ret_20d"])
    no_lookahead_status = "missing"
    no_lookahead_passed = False
    if audit_file is not None:
        audit = _load_json(audit_file)
        no_lookahead_status = str(audit.get("status", "missing"))
        no_lookahead_passed = _audit_passes(audit)
    return SurfaceInfo(
        family=family,
        session_dir=session_dir,
        candidate_file=candidate_file,
        orfp_file=orfp_files[0] if orfp_files else None,
        audit_file=audit_file,
        row_count=int(len(df)),
        anchor_date_min=str(df["anchor_date"].min()) if df["anchor_date"].notna().any() else None,
        anchor_date_max=str(df["anchor_date"].max()) if df["anchor_date"].notna().any() else None,
        symbol_count=int(df["symbol"].nunique(dropna=True)),
        forward_non_null_count=int(df["forward_ret_20d"].notna().sum()),
        forward_available=bool(df["forward_ret_20d"].notna().all()),
        no_lookahead_status=no_lookahead_status,
        no_lookahead_passed=no_lookahead_passed,
    )


def _discover_surfaces(tradex_root: Path) -> list[SurfaceInfo]:
    surfaces: list[SurfaceInfo] = []
    for family_dir in sorted(tradex_root.glob("feature_surface_*")):
        if not family_dir.is_dir():
            continue
        for session_dir in sorted(item for item in family_dir.iterdir() if item.is_dir()):
            surface = _discover_surface_info(session_dir, family_dir.name)
            if surface is not None:
                surfaces.append(surface)
    return surfaces


def _load_frozen_features() -> list[str]:
    spec = _load_json(FROZEN_MODEL_SPEC)
    return list(spec.get("exact_features_used", []))


def _build_surface_discovery_summary(surfaces: list[SurfaceInfo], frozen_end: str) -> dict[str, Any]:
    discovered = []
    for surface in surfaces:
        discovered.append(
            {
                "family": surface.family,
                "session_dir": str(surface.session_dir),
                "candidate_file": str(surface.candidate_file),
                "orfp_file": str(surface.orfp_file) if surface.orfp_file is not None else None,
                "audit_file": str(surface.audit_file) if surface.audit_file is not None else None,
                "row_count": surface.row_count,
                "anchor_date_min": surface.anchor_date_min,
                "anchor_date_max": surface.anchor_date_max,
                "symbol_count": surface.symbol_count,
                "forward_available": surface.forward_available,
                "no_lookahead_status": surface.no_lookahead_status,
                "no_lookahead_passed": surface.no_lookahead_passed,
            }
        )
    newest = None
    if surfaces:
        newest = max(
            surfaces,
            key=lambda item: (
                item.anchor_date_max or "",
                item.session_dir.name,
                item.family,
            ),
        )
    newest_payload = None
    if newest is not None:
        newest_payload = {
            "family": newest.family,
            "session_dir": str(newest.session_dir),
            "candidate_file": str(newest.candidate_file),
            "anchor_date_max": newest.anchor_date_max,
            "anchor_date_min": newest.anchor_date_min,
            "row_count": newest.row_count,
            "symbol_count": newest.symbol_count,
            "forward_available": newest.forward_available,
            "no_lookahead_status": newest.no_lookahead_status,
            "no_lookahead_passed": newest.no_lookahead_passed,
        }
    max_candidate_date = newest.anchor_date_max if newest is not None else None
    newer_surface_found = bool(max_candidate_date and max_candidate_date > frozen_end)
    return {
        "schema_version": SURFACE_DISCOVERY_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "frozen_forward_window_end": frozen_end,
        "candidate_surface_count": len(surfaces),
        "discovered_surfaces": discovered,
        "newest_surface": newest_payload,
        "max_candidate_date": max_candidate_date,
        "newer_surface_found": newer_surface_found,
        "newer_surface_exists_beyond_frozen_window": newer_surface_found,
        "all_candidate_surfaces_with_forward_outcomes": all(surface.forward_available for surface in surfaces) if surfaces else False,
        "all_candidate_surfaces_with_no_lookahead_pass": all(surface.no_lookahead_passed for surface in surfaces) if surfaces else False,
    }


def _build_forward_outcome_availability(surface_discovery: dict[str, Any]) -> dict[str, Any]:
    newest = surface_discovery.get("newest_surface") or {}
    return {
        "schema_version": OUTCOME_AVAILABILITY_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "newest_surface": newest,
        "latest_available_candidate_date": surface_discovery.get("max_candidate_date"),
        "frozen_forward_window_end": surface_discovery.get("frozen_forward_window_end"),
        "full_20_business_day_forward_outcomes_available": bool(newest.get("forward_available")),
        "forward_outcomes_cover_newest_surface": bool(newest.get("forward_available")),
        "forward_outcomes_cover_all_candidate_surfaces": bool(surface_discovery.get("all_candidate_surfaces_with_forward_outcomes")),
        "forward_validation_boundary_reached": not bool(surface_discovery.get("newer_surface_exists_beyond_frozen_window")),
        "reason": (
            "newer candidate surface exists and forward outcomes need validation"
            if surface_discovery.get("newer_surface_exists_beyond_frozen_window")
            else "no candidate / feature surface exists beyond the frozen challenger window"
        ),
    }


def _build_feature_contract_check(frozen_features: list[str], newest_surface: dict[str, Any]) -> dict[str, Any]:
    candidate_cols = _load_candidate_columns(Path(newest_surface["candidate_file"]))
    orfp_cols = None
    if newest_surface.get("orfp_file"):
        orfp_cols = _load_candidate_columns(Path(newest_surface["orfp_file"]))
    missing_candidate = [feature for feature in frozen_features if feature not in candidate_cols]
    missing_orfp = [feature for feature in frozen_features if orfp_cols is not None and feature not in orfp_cols]
    status = "pass" if not missing_candidate and not missing_orfp else "fail"
    return {
        "schema_version": FEATURE_CONTRACT_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "selected_surface": newest_surface,
        "frozen_feature_count": len(frozen_features),
        "candidate_column_count": len(candidate_cols),
        "orfp_column_count": len(orfp_cols) if orfp_cols is not None else None,
        "missing_candidate_features": missing_candidate,
        "missing_orfp_features": missing_orfp,
        "status": status,
        "feature_contract_matches": status == "pass",
    }


def _load_candidate_columns(path: Path) -> list[str]:
    frame = pd.read_parquet(path, columns=None)
    return [str(column) for column in frame.columns]


def _build_decision(surface_discovery: dict[str, Any], feature_contract: dict[str, Any], forward_outcomes: dict[str, Any], no_lookahead_passed: bool) -> dict[str, Any]:
    newest = surface_discovery.get("newest_surface") or {}
    max_date = surface_discovery.get("max_candidate_date")
    if not no_lookahead_passed:
        decision = "no_lookahead_audit_missing"
    elif not feature_contract.get("feature_contract_matches"):
        decision = "feature_contract_mismatch"
    elif not max_date or str(max_date) <= FROZEN_FORWARD_WINDOW_END:
        decision = "waiting_for_new_candidate_surface"
    elif not forward_outcomes.get("full_20_business_day_forward_outcomes_available"):
        decision = "waiting_for_more_forward_outcomes"
    else:
        decision = "ready_to_run_forward_validation"
    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "decision": decision,
        "status": decision,
        "frozen_challenger": "tree_hgb_path_value",
        "frozen_forward_window_end": FROZEN_FORWARD_WINDOW_END,
        "newest_surface": newest,
        "max_candidate_date": max_date,
        "full_20_business_day_forward_outcomes_available": forward_outcomes.get("full_20_business_day_forward_outcomes_available"),
        "feature_contract_matches": feature_contract.get("feature_contract_matches"),
        "no_lookahead_passed": no_lookahead_passed,
        "reason": _decision_reason(decision),
        "jobs_supported": 1,
    }


def _decision_reason(decision: str) -> str:
    reasons = {
        "ready_to_run_forward_validation": "new candidate surface exists beyond the frozen window and the frozen contract can be replayed",
        "waiting_for_more_forward_outcomes": "a newer candidate surface exists but full 20-business-day forward outcomes are not yet available",
        "waiting_for_new_candidate_surface": "no candidate / feature surface exists beyond the frozen challenger window",
        "feature_contract_mismatch": "the newest candidate surface is missing one or more frozen model features",
        "no_lookahead_audit_missing": "the newest candidate surface does not have a passing no-lookahead audit",
    }
    return reasons[decision]


def _build_run_manifest(output_root: Path, session_dir: Path, inputs: dict[str, Path]) -> dict[str, Any]:
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "script_name": SCRIPT_NAME,
        "generated_at_utc": _utc_now(),
        "git_commit": _git_hash_or_unknown(),
        "session_id": session_dir.name,
        "output_root": str(output_root),
        "session_dir": str(session_dir),
        "decision_axis": "shadow_reranker_forward_readiness",
        "input_paths": {key: str(path) for key, path in inputs.items()},
    }


def _build_input_resolution(inputs: dict[str, Path]) -> dict[str, Any]:
    return {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "resolved_paths": {key: str(path) for key, path in inputs.items()},
        "path_checks": {key: path.exists() for key, path in inputs.items()},
        "notes": [
            "This checker is read-only and does not replay forward validation.",
            "It only inspects the frozen defer summary and candidate / feature surfaces.",
        ],
    }


def run_forward_readiness_checker(output_root: str | Path | None = None) -> dict[str, Any]:
    output_root_path = _safe_path(output_root, DEFAULT_OUTPUT_ROOT)
    output_root_path.mkdir(parents=True, exist_ok=True)
    session_id = _make_session_id()
    session_dir = output_root_path / session_id
    session_dir.mkdir(parents=True, exist_ok=True)

    inputs = {
        "defer_decision": _safe_path(DEFER_DECISION, DEFER_DECISION),
        "frozen_shadow_challenger_summary": _safe_path(FROZEN_SUMMARY, FROZEN_SUMMARY),
        "forward_data_gap_summary": _safe_path(FORWARD_GAP, FORWARD_GAP),
        "reopen_conditions": _safe_path(REOPEN_CONDITIONS, REOPEN_CONDITIONS),
        "frozen_model_spec": _safe_path(FROZEN_MODEL_SPEC, FROZEN_MODEL_SPEC),
    }
    for label, path in inputs.items():
        _ensure_exists(path, label)

    frozen_features = _load_frozen_features()
    surfaces = _discover_surfaces(Path(r"G:\Tradex"))
    surface_discovery = _build_surface_discovery_summary(surfaces, FROZEN_FORWARD_WINDOW_END)
    forward_outcomes = _build_forward_outcome_availability(surface_discovery)
    newest = surface_discovery.get("newest_surface")
    if newest is None:
        feature_contract = {
            "schema_version": FEATURE_CONTRACT_SCHEMA_VERSION,
            "generated_at_utc": _utc_now(),
            "status": "no_surface_found",
            "feature_contract_matches": False,
            "missing_candidate_features": frozen_features,
            "missing_orfp_features": frozen_features,
            "frozen_feature_count": len(frozen_features),
            "candidate_column_count": 0,
            "orfp_column_count": None,
            "selected_surface": None,
        }
    else:
        feature_contract = _build_feature_contract_check(frozen_features, newest)

    no_lookahead_passed = bool(newest and newest.get("no_lookahead_passed"))
    decision = _build_decision(surface_discovery, feature_contract, forward_outcomes, no_lookahead_passed)

    run_manifest = _build_run_manifest(output_root_path, session_dir, inputs)
    input_resolution = _build_input_resolution(inputs)

    _write_json(session_dir / "run_manifest.json", run_manifest)
    _write_json(session_dir / "input_resolution.json", input_resolution)
    _write_json(session_dir / "surface_discovery_summary.json", surface_discovery)
    _write_json(session_dir / "forward_outcome_availability.json", forward_outcomes)
    _write_json(session_dir / "frozen_feature_contract_check.json", feature_contract)
    _write_json(session_dir / "forward_readiness_decision.json", decision)
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
                "surface_discovery_summary.json",
                "forward_outcome_availability.json",
                "frozen_feature_contract_check.json",
                "forward_readiness_decision.json",
                "_ARTIFACT_COMPLETE.json",
            ],
        },
    )

    return {
        "output_dir": str(session_dir),
        "session_id": session_id,
        "decision": decision["decision"],
        "status": decision["status"],
        "newest_max_candidate_date": surface_discovery.get("max_candidate_date"),
        "jobs_supported": 1,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=SCRIPT_NAME)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    result = run_forward_readiness_checker(args.output_root)
    print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
