from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

SCRIPT_NAME = "tradex_shadow_reranker_forward_defer_summary_v1"
SCHEMA_VERSION = "tradex_shadow_reranker_forward_defer_summary_v1"
MANIFEST_SCHEMA_VERSION = "tradex_shadow_reranker_forward_defer_summary_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_shadow_reranker_forward_defer_summary_v1_input_resolution_v1"
DEFER_DECISION_SCHEMA_VERSION = "tradex_shadow_reranker_forward_defer_summary_v1_defer_decision_v1"
FROZEN_SUMMARY_SCHEMA_VERSION = "tradex_shadow_reranker_forward_defer_summary_v1_frozen_shadow_challenger_summary_v1"
GAP_SUMMARY_SCHEMA_VERSION = "tradex_shadow_reranker_forward_defer_summary_v1_forward_data_gap_summary_v1"
REOPEN_CONDITIONS_SCHEMA_VERSION = "tradex_shadow_reranker_forward_defer_summary_v1_reopen_conditions_v1"

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\research_defer_summaries\shadow_reranker_forward_validation")
FROZEN_SESSION = Path(r"G:\Tradex\shadow_reranker_challenger_design_v1\20260501T120651Z-643568")
FROZEN_MODEL_SPEC = FROZEN_SESSION / "shadow_challenger_model_spec.json"
FROZEN_VARIANT_COMPARISON = FROZEN_SESSION / "shadow_challenger_variant_pool_comparison.json"
FROZEN_ROBUSTNESS = FROZEN_SESSION / "shadow_challenger_robustness_audit.json"
FROZEN_LEAKAGE = FROZEN_SESSION / "shadow_challenger_leakage_audit.json"
FROZEN_DECISION = FROZEN_SESSION / "shadow_reranker_challenger_design_v1_decision.json"

FORWARD_SESSION = Path(r"G:\Tradex\shadow_reranker_forward_validation_v1\20260501T135029Z-615008")
FORWARD_AVAILABILITY = FORWARD_SESSION / "forward_data_availability_audit.json"
FORWARD_REPLAY = FORWARD_SESSION / "forward_model_replay_contract.json"
FORWARD_VARIANTS = FORWARD_SESSION / "forward_variant_pool_comparison.json"
FORWARD_STABILITY = FORWARD_SESSION / "forward_stability_audit.json"
FORWARD_LEAKAGE = FORWARD_SESSION / "forward_leakage_audit.json"
FORWARD_DECISION = FORWARD_SESSION / "shadow_reranker_forward_validation_v1_decision.json"

REQUIRED_OUTPUT_NAMES = [
    "run_manifest.json",
    "input_resolution.json",
    "defer_decision.json",
    "frozen_shadow_challenger_summary.json",
    "forward_data_gap_summary.json",
    "reopen_conditions.json",
    "_ARTIFACT_COMPLETE.json",
]


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
    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            return None
    if hasattr(value, "__class__") and value.__class__.__name__ == "NAType":
        return None
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
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


def _feature_list_hash(features: list[str]) -> str:
    payload = json.dumps(features, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _build_input_resolution(inputs: dict[str, Path]) -> dict[str, Any]:
    return {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "resolved_paths": {name: str(path) for name, path in inputs.items()},
        "path_checks": {name: path.exists() for name, path in inputs.items()},
        "source_sessions": {
            "frozen_challenger_design": {
                "session_dir": str(FROZEN_SESSION),
                "exists": FROZEN_SESSION.exists(),
            },
            "forward_validation_stop_path": {
                "session_dir": str(FORWARD_SESSION),
                "exists": FORWARD_SESSION.exists(),
            },
        },
        "notes": [
            "This is a defer bookkeeping run only.",
            "No forward data is replayed or revalidated here.",
        ],
    }


def _build_frozen_shadow_challenger_summary() -> dict[str, Any]:
    model_spec = _load_json(FROZEN_MODEL_SPEC)
    variant = _load_json(FROZEN_VARIANT_COMPARISON)
    robustness = _load_json(FROZEN_ROBUSTNESS)
    leakage = _load_json(FROZEN_LEAKAGE)
    decision = _load_json(FROZEN_DECISION)
    features = list(model_spec.get("exact_features_used", []))
    comparison = dict(variant.get("comparison_summary", {}))
    train_metrics = dict(robustness.get("train_metrics", {}))
    validation_metrics = dict(robustness.get("validation_metrics", {}))
    test_metrics = dict(robustness.get("test_metrics", {}))
    return {
        "schema_version": FROZEN_SUMMARY_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "frozen_session": str(FROZEN_SESSION),
        "frozen_challenger": model_spec.get("selected_variant", "tree_hgb_path_value"),
        "model_type": model_spec.get("model_type"),
        "target_label": model_spec.get("target_label"),
        "feature_count": int(len(features)),
        "frozen_feature_list_hash": {
            "algorithm": "sha256",
            "value": _feature_list_hash(features),
        },
        "model_parameters": model_spec.get("model_parameters", {}),
        "challenger_design_performance_summary": {
            "decision": decision.get("decision"),
            "reason": decision.get("reason"),
            "top5_forward_delta": comparison.get("top5_forward_delta"),
            "top10_forward_delta": comparison.get("top10_forward_delta"),
            "top5_bottom15_delta": comparison.get("top5_bottom15_delta"),
            "top10_bottom15_delta": comparison.get("top10_bottom15_delta"),
            "top5_membership_change_rate": comparison.get("top5_membership_change_rate"),
            "top10_membership_change_rate": comparison.get("top10_membership_change_rate"),
            "top20_membership_change_rate": comparison.get("top20_membership_change_rate"),
            "top20_overlap_ratio": comparison.get("top20_overlap_ratio"),
            "zero_pass_groups": comparison.get("zero_pass_groups", {}),
            "train_metrics": train_metrics,
            "validation_metrics": validation_metrics,
            "test_metrics": test_metrics,
        },
        "leakage_audit_status": leakage.get("status"),
        "time_split_audit_status": model_spec.get("split_contract", {}).get("status"),
        "known_risks": [
            "weak global OOS Spearman",
            "top-K-local signal",
            "top20 unchanged",
            "shadow-only result",
        ],
        "source_artifacts": {
            "model_spec": str(FROZEN_MODEL_SPEC),
            "variant_pool_comparison": str(FROZEN_VARIANT_COMPARISON),
            "robustness_audit": str(FROZEN_ROBUSTNESS),
            "leakage_audit": str(FROZEN_LEAKAGE),
            "design_decision": str(FROZEN_DECISION),
        },
    }


def _build_forward_data_gap_summary() -> dict[str, Any]:
    availability = _load_json(FORWARD_AVAILABILITY)
    replay = _load_json(FORWARD_REPLAY)
    variants = _load_json(FORWARD_VARIANTS)
    stability = _load_json(FORWARD_STABILITY)
    leakage = _load_json(FORWARD_LEAKAGE)
    decision = _load_json(FORWARD_DECISION)
    window = dict(availability.get("forward_window", {}))
    source_surface = availability.get("source_surface", {})
    candidate_surface = dict(source_surface.get("candidate", {}))
    return {
        "schema_version": GAP_SUMMARY_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "latest_available_candidate_date": window.get("latest_available_candidate_date"),
        "latest_date_with_confirmed_forward_ret_20d": window.get("latest_date_with_confirmed_forward_ret_20d"),
        "forward_validation_start_date": window.get("forward_validation_start_date"),
        "forward_validation_end_date": window.get("forward_validation_end_date"),
        "candidate_row_count": int(window.get("candidate_row_count") or 0),
        "anchor_date_count": int(window.get("anchor_date_count") or 0),
        "symbol_count": int(window.get("symbol_count") or 0),
        "group_counts": dict(window.get("group_counts", {})),
        "reason_validation_could_not_run": availability.get("reason"),
        "required_minimum_data_for_next_attempt": {
            "new_surface_required": "a new candidate / feature surface beyond 2026-01-19",
            "full_forward_outcomes_required": "full 20-business-day forward outcomes for the new surface",
            "no_lookahead_required": "the no-lookahead feature audit must pass on the new surface",
            "corpus_boundary_required": "the new validation rows must not be part of the prior challenger-design corpus",
        },
        "blocking_status": {
            "forward_replay_status": replay.get("replay_status"),
            "forward_variants_status": variants.get("status"),
            "forward_stability_status": stability.get("status"),
            "forward_leakage_status": leakage.get("status"),
            "forward_decision": decision.get("decision"),
        },
        "source_surface": {
            "candidate_row_count": candidate_surface.get("row_count"),
            "candidate_anchor_date_min": candidate_surface.get("earliest_anchor_date"),
            "candidate_anchor_date_max": candidate_surface.get("latest_anchor_date"),
            "full_20_business_day_forward_outcomes_available": candidate_surface.get("full_20_business_day_forward_outcomes_available"),
        },
    }


def _build_reopen_conditions() -> dict[str, Any]:
    return {
        "schema_version": REOPEN_CONDITIONS_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "reopen_when": [
            "a new candidate / feature surface exists beyond 2026-01-19",
            "full 20-business-day forward outcomes are available for that new surface",
            "the frozen model spec can be applied unchanged",
            "the no-lookahead feature audit passes",
            "the new validation rows are not part of the prior challenger-design corpus",
        ],
        "do_not_reopen_for": [
            "pseudo-forward split inside the old corpus",
            "hyperparameter retuning",
            "feature changes",
            "manual cherry-picked symbols",
            "MeeMee reflection",
        ],
        "frozen_challenger": "tree_hgb_path_value",
        "frozen_forward_window_end": "2026-01-19",
        "reopen_gate": "waiting_for_new_forward_surface",
    }


def _build_defer_decision() -> dict[str, Any]:
    return {
        "schema_version": DEFER_DECISION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "decision": "defer_shadow_reranker_forward_validation",
        "status": "waiting_for_forward_surface",
        "frozen_challenger": "tree_hgb_path_value",
        "promote_ready": False,
        "meemee_reflectable": False,
        "reason": "no_forward_validatable_rows_exist_beyond_frozen_challenger_window",
        "forward_validation_session": str(FORWARD_SESSION),
        "frozen_challenger_session": str(FROZEN_SESSION),
    }


def _build_run_manifest(output_root: Path, session_dir: Path, inputs: dict[str, Path]) -> dict[str, Any]:
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "script_name": SCRIPT_NAME,
        "generated_at_utc": _utc_now(),
        "git_commit": _git_hash_or_unknown(),
        "session_id": session_dir.name,
        "output_root": str(output_root),
        "session_dir": str(session_dir),
        "decision": "defer_shadow_reranker_forward_validation",
        "input_paths": {name: str(path) for name, path in inputs.items()},
        "notes": [
            "This run only records a formal defer decision and reopen conditions.",
            "No model replay or forward validation is performed here.",
        ],
    }


def build_defer_summary(
    output_root: str | Path | None = None,
    *,
    session_id: str | None = None,
) -> dict[str, Any]:
    output_root_path = _safe_path(output_root, DEFAULT_OUTPUT_ROOT)
    output_root_path.mkdir(parents=True, exist_ok=True)
    session_name = session_id or _make_session_id()
    session_dir = output_root_path / session_name
    session_dir.mkdir(parents=True, exist_ok=True)

    inputs = {
        "frozen_model_spec": _safe_path(FROZEN_MODEL_SPEC, FROZEN_MODEL_SPEC),
        "frozen_variant_pool_comparison": _safe_path(FROZEN_VARIANT_COMPARISON, FROZEN_VARIANT_COMPARISON),
        "frozen_robustness": _safe_path(FROZEN_ROBUSTNESS, FROZEN_ROBUSTNESS),
        "frozen_leakage": _safe_path(FROZEN_LEAKAGE, FROZEN_LEAKAGE),
        "frozen_decision": _safe_path(FROZEN_DECISION, FROZEN_DECISION),
        "forward_data_availability": _safe_path(FORWARD_AVAILABILITY, FORWARD_AVAILABILITY),
        "forward_model_replay_contract": _safe_path(FORWARD_REPLAY, FORWARD_REPLAY),
        "forward_variant_pool_comparison": _safe_path(FORWARD_VARIANTS, FORWARD_VARIANTS),
        "forward_stability_audit": _safe_path(FORWARD_STABILITY, FORWARD_STABILITY),
        "forward_leakage_audit": _safe_path(FORWARD_LEAKAGE, FORWARD_LEAKAGE),
        "forward_decision": _safe_path(FORWARD_DECISION, FORWARD_DECISION),
    }
    for label, path in inputs.items():
        _ensure_exists(path, label)

    input_resolution = _build_input_resolution(inputs)
    frozen_summary = _build_frozen_shadow_challenger_summary()
    forward_gap = _build_forward_data_gap_summary()
    reopen_conditions = _build_reopen_conditions()
    defer_decision = _build_defer_decision()
    run_manifest = _build_run_manifest(output_root_path, session_dir, inputs)

    _write_json(session_dir / "run_manifest.json", run_manifest)
    _write_json(session_dir / "input_resolution.json", input_resolution)
    _write_json(session_dir / "defer_decision.json", defer_decision)
    _write_json(session_dir / "frozen_shadow_challenger_summary.json", frozen_summary)
    _write_json(session_dir / "forward_data_gap_summary.json", forward_gap)
    _write_json(session_dir / "reopen_conditions.json", reopen_conditions)
    _write_json(
        session_dir / "_ARTIFACT_COMPLETE.json",
        {
            "schema_version": SCHEMA_VERSION,
            "generated_at_utc": _utc_now(),
            "session_id": session_name,
            "required_files_present": True,
            "artifacts": REQUIRED_OUTPUT_NAMES,
        },
    )

    return {
        "output_dir": str(session_dir),
        "session_id": session_name,
        "decision": defer_decision["decision"],
        "status": defer_decision["status"],
        "forward_candidate_row_count": 0,
        "jobs_supported": 1,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=SCRIPT_NAME)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--session-id", type=str, default=None)
    args = parser.parse_args()
    result = build_defer_summary(args.output_root, session_id=args.session_id)
    print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
