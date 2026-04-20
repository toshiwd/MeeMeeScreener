from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.services import tradex_research_os_store as os_store
from app.backend.services.toredex.toredex_hash import hash_payload
from external_analysis.runtime.source_snapshot import create_source_snapshot
from shared.tradex_storage import tradex_scratch_path
from scripts.action_precision_replay import (
    _next_month,
    _resolve_db_path,
    _snapshot_horizon_contract,
)
ARTIFACT_DIR = REPO_ROOT / "artifacts" / "research_inventory"
CURRENT_THRESHOLDS_ARTIFACT = ARTIFACT_DIR / "action_precision_thresholds.json"
CURRENT_TOO_LATE_RULES_ARTIFACT = ARTIFACT_DIR / "action_precision_long_too_late_rules.json"
CURRENT_WEAK_RULES_ARTIFACT = ARTIFACT_DIR / "action_precision_long_weak_direction_rules.json"
CURRENT_TOO_LATE_FORWARD_CONFIRM_ARTIFACT = ARTIFACT_DIR / "action_precision_long_too_late_forward_confirm.json"
CURRENT_WEAK_FORWARD_CONFIRM_ARTIFACT = ARTIFACT_DIR / "action_precision_long_weak_direction_forward_confirm.json"
CURRENT_TOO_LATE_COMPARE_ARTIFACT = ARTIFACT_DIR / "action_precision_long_too_late_compare.json"
CURRENT_WEAK_COMPARE_ARTIFACT = ARTIFACT_DIR / "action_precision_long_weak_direction_compare.json"
CURRENT_TOO_LATE_CANDIDATES_ARTIFACT = ARTIFACT_DIR / "action_precision_long_too_late_candidates.json"
CURRENT_WEAK_CANDIDATES_ARTIFACT = ARTIFACT_DIR / "action_precision_long_weak_direction_candidates.json"
CURRENT_TOO_LATE_AUTHORITATIVE_ARTIFACT = ARTIFACT_DIR / "authoritative_decision.long_too_late_forward_confirm.json"
CURRENT_WEAK_AUTHORITATIVE_ARTIFACT = ARTIFACT_DIR / "authoritative_decision.long_weak_direction_forward_confirm.json"
READINESS_ARTIFACT = ARTIFACT_DIR / "action_precision_forward_readiness.json"
ORCHESTRATION_ARTIFACT = ARTIFACT_DIR / "action_precision_forward_orchestration.json"
FROZEN_MANIFEST_ARTIFACT = ARTIFACT_DIR / "action_precision_frozen_manifest.json"
REPORT_TEMPLATE_ARTIFACT = ARTIFACT_DIR / "action_precision_forward_report_template.json"
FORWARD_RUNS_ROOT = tradex_scratch_path("action_precision_forward_control", "runs").resolve()
REPLAY_SCRIPT = REPO_ROOT / "scripts" / "action_precision_replay.py"

TOO_LATE_STAGE = "long_too_late_block"
WEAK_STAGE = "long_weak_direction_block"
COMBINED_STAGE = "combined_long_block"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _generated_token() -> str:
    return _utc_now().strftime("%Y%m%dT%H%M%S%fZ")


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    return os_store.write_json(path, payload)


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"missing artifact: {path}")
    return os_store.read_json_object_strict(path, artifact_name=path.name)


def _ensure_unique_run_dir(root: Path, stem: str) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    token = _generated_token()
    candidate = root / f"{token}_{stem}"
    suffix = 1
    while candidate.exists():
        candidate = root / f"{token}_{stem}_{suffix:02d}"
        suffix += 1
    candidate.mkdir(parents=True, exist_ok=False)
    return candidate


def _current_validation_month_end() -> int:
    thresholds = _read_json(CURRENT_THRESHOLDS_ARTIFACT)
    split_contract = thresholds.get("split_contract") if isinstance(thresholds.get("split_contract"), dict) else {}
    months = split_contract.get("validation_months") if isinstance(split_contract.get("validation_months"), list) else []
    if not months:
        raise RuntimeError(f"validation months missing in {CURRENT_THRESHOLDS_ARTIFACT}")
    return int(months[-1])


def _current_validation_months() -> list[int]:
    thresholds = _read_json(CURRENT_THRESHOLDS_ARTIFACT)
    split_contract = thresholds.get("split_contract") if isinstance(thresholds.get("split_contract"), dict) else {}
    months = split_contract.get("validation_months") if isinstance(split_contract.get("validation_months"), list) else []
    return [int(month) for month in months]


def _list_unseen_complete_months(current_validation_month_end: int | None, last_fully_confirmable_month: int | None) -> list[int]:
    if current_validation_month_end is None or last_fully_confirmable_month is None:
        return []
    if int(last_fully_confirmable_month) <= int(current_validation_month_end):
        return []
    months: list[int] = []
    month = _next_month(int(current_validation_month_end))
    while month <= int(last_fully_confirmable_month):
        months.append(int(month))
        month = _next_month(int(month))
    return months


def _snapshot_with_horizon(*, db_path: str | None) -> dict[str, Any]:
    resolved_db = _resolve_db_path(db_path)
    snapshot_payload = create_source_snapshot(source_db_path=str(resolved_db), label="action_precision_forward_control")
    snapshot_db_path = Path(str(snapshot_payload["snapshot_db_path"])).expanduser().resolve()
    with duckdb.connect(str(snapshot_db_path), read_only=True) as conn:
        horizon = _snapshot_horizon_contract(conn)
    return {
        "source_db_path": str(resolved_db),
        "snapshot_payload": snapshot_payload,
        "snapshot_db_path": str(snapshot_db_path),
        **horizon,
    }


def build_readiness(*, db_path: str | None = None) -> dict[str, Any]:
    current_validation_months = _current_validation_months()
    current_validation_month_end = current_validation_months[-1] if current_validation_months else None
    horizon = _snapshot_with_horizon(db_path=db_path)
    unseen_months = _list_unseen_complete_months(current_validation_month_end, horizon["last_fully_confirmable_month"])
    ready = bool(unseen_months)
    return {
        "schema_version": "tradex_action_precision_forward_readiness_v1",
        "generated_at": _utc_now().isoformat(),
        "source_db_path": horizon["source_db_path"],
        "snapshot_db_path": horizon["snapshot_db_path"],
        "snapshot_payload": horizon["snapshot_payload"],
        "snapshot_max_trade_date": horizon["snapshot_max_trade_date"],
        "replay_lookback_start_date": horizon["replay_lookback_start_date"],
        "last_fully_confirmable_month": horizon["last_fully_confirmable_month"],
        "current_validation_months": current_validation_months,
        "current_validation_month_end": current_validation_month_end,
        "new_complete_unseen_month_exists": ready,
        "new_complete_unseen_months": unseen_months,
        "decision": "keep" if ready else "hold_needs_more_time",
        "decision_reason": (
            f"complete unseen month(s) available: {', '.join(str(month) for month in unseen_months)}"
            if ready
            else "no_complete_month_available_after_validation"
        ),
    }


def _threshold_contract_hash() -> str:
    thresholds = _read_json(CURRENT_THRESHOLDS_ARTIFACT)
    contract_view = {
        "signal_replay_contract": thresholds.get("signal_replay_contract"),
        "thresholds": thresholds.get("thresholds"),
        "split_contract": thresholds.get("split_contract"),
        "long_revision_axis": thresholds.get("long_revision_axis"),
        "short_revision_axis": thresholds.get("short_revision_axis"),
    }
    return hash_payload(contract_view)


def _load_revision_artifact(path: Path, *, fallback: dict[str, Any] | None = None) -> dict[str, Any]:
    if path.exists():
        return _read_json(path)
    return dict(fallback or {})


def build_frozen_manifest(*, readiness: dict[str, Any]) -> dict[str, Any]:
    too_late_rules = _load_revision_artifact(CURRENT_TOO_LATE_RULES_ARTIFACT)
    weak_rules = _load_revision_artifact(CURRENT_WEAK_RULES_ARTIFACT)
    thresholds = _read_json(CURRENT_THRESHOLDS_ARTIFACT)
    threshold_hash = _threshold_contract_hash()
    return {
        "schema_version": "tradex_action_precision_frozen_manifest_v1",
        "generated_at": _utc_now().isoformat(),
        "source_db_path": readiness["source_db_path"],
        "snapshot_db_path": readiness["snapshot_db_path"],
        "snapshot_max_trade_date": readiness["snapshot_max_trade_date"],
        "replay_lookback_start_date": readiness["replay_lookback_start_date"],
        "last_fully_confirmable_month": readiness["last_fully_confirmable_month"],
        "current_validation_month_end": readiness["current_validation_month_end"],
        "threshold_contract_hash": threshold_hash,
        "current_threshold_artifact": str(CURRENT_THRESHOLDS_ARTIFACT),
        "allowed_next_action": "defer_and_wait_for_complete_forward_block"
        if not readiness["new_complete_unseen_month_exists"]
        else "forward_confirm_frozen_revision",
        "frozen_revisions": [
            {
                "revision_name": "long_weak_direction_1_cluster_block",
                "status": "accepted_frozen",
                "allowed_next_action": "forward_confirm_frozen_revision",
                "exact_cluster_list": list(weak_rules.get("selected_block_clusters") or []),
                "threshold_contract_hash": threshold_hash,
                "source_artifact_paths": [
                    str(CURRENT_WEAK_CANDIDATES_ARTIFACT),
                    str(CURRENT_WEAK_RULES_ARTIFACT),
                    str(CURRENT_WEAK_COMPARE_ARTIFACT),
                    str(CURRENT_WEAK_FORWARD_CONFIRM_ARTIFACT),
                    str(CURRENT_WEAK_AUTHORITATIVE_ARTIFACT),
                ],
            },
            {
                "revision_name": "long_too_late_2_cluster_block",
                "status": "pending_frozen",
                "allowed_next_action": "forward_confirm_frozen_revision",
                "exact_cluster_list": list(too_late_rules.get("selected_block_clusters") or []),
                "threshold_contract_hash": threshold_hash,
                "source_artifact_paths": [
                    str(CURRENT_TOO_LATE_CANDIDATES_ARTIFACT),
                    str(CURRENT_TOO_LATE_RULES_ARTIFACT),
                    str(CURRENT_TOO_LATE_COMPARE_ARTIFACT),
                    str(CURRENT_TOO_LATE_FORWARD_CONFIRM_ARTIFACT),
                    str(CURRENT_TOO_LATE_AUTHORITATIVE_ARTIFACT),
                ],
            },
        ],
        "threshold_contract": thresholds.get("thresholds"),
    }


def build_forward_report_template() -> dict[str, Any]:
    metric_keys = [
        "buy_signal_count",
        "buy_precision_strong",
        "buy_mfe_20_mean",
        "buy_mfe_20_median",
        "buy_mae_20_mean",
        "buy_too_late_rate",
        "buy_on_time_rate",
        "buy_timing_score_mean",
        "coverage_loss_pct",
        "precision_gain",
        "mfe_delta",
        "mae_delta",
        "timing_score_delta",
    ]
    return {
        "schema_version": "tradex_action_precision_forward_report_template_v1",
        "generated_at": _utc_now().isoformat(),
        "stage_order": [TOO_LATE_STAGE, WEAK_STAGE, COMBINED_STAGE],
        "sections": {
            TOO_LATE_STAGE: {
                "baseline_metrics": {key: None for key in metric_keys},
                "block_metrics": {key: None for key in metric_keys},
                "decision": None,
            },
            WEAK_STAGE: {
                "baseline_metrics": {key: None for key in metric_keys},
                "block_metrics": {key: None for key in metric_keys},
                "decision": None,
            },
            COMBINED_STAGE: {
                "baseline_metrics": {key: None for key in metric_keys},
                "block_metrics": {key: None for key in metric_keys},
                "decision": None,
                "note": "combined long remains gated until both frozen singles confirm on a complete unseen month",
            },
        },
        "required_artifacts": {
            TOO_LATE_STAGE: "action_precision_long_too_late_forward_confirm.json",
            WEAK_STAGE: "action_precision_long_weak_direction_forward_confirm.json",
            COMBINED_STAGE: "authoritative_decision.long_forward_confirm_bundle.json",
        },
    }


def _run_replay_stage(*, db_path: str, output_dir: Path) -> dict[str, Any]:
    cmd = [
        sys.executable,
        str(REPLAY_SCRIPT),
        "--db-path",
        str(db_path),
        "--output-dir",
        str(output_dir),
    ]
    completed = subprocess.run(cmd, check=True, capture_output=True, text=True)
    return {
        "command": cmd,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
        "returncode": completed.returncode,
    }


def _read_stage_payload(output_dir: Path, filename: str) -> dict[str, Any]:
    return _read_json(output_dir / filename)


def _stage_summary(stage_name: str, output_dir: Path, payload: dict[str, Any], *, artifact_name: str) -> dict[str, Any]:
    return {
        "stage": stage_name,
        "run_dir": str(output_dir),
        "artifact_path": str(output_dir / artifact_name),
        "decision_artifact_path": str(output_dir / ("authoritative_decision.long_too_late_forward_confirm.json" if stage_name == TOO_LATE_STAGE else "authoritative_decision.long_weak_direction_forward_confirm.json" if stage_name == WEAK_STAGE else "authoritative_decision.long_forward_confirm_bundle.json")),
        "decision": payload.get("decision"),
        "decision_reason": payload.get("decision_reason"),
    }


def _orchestration_from_readiness(readiness: dict[str, Any]) -> dict[str, Any]:
    if not readiness["new_complete_unseen_month_exists"]:
        return {
            "schema_version": "tradex_action_precision_forward_orchestration_v1",
            "generated_at": _utc_now().isoformat(),
            "status": "hold_needs_more_time",
            "decision": "hold_needs_more_time",
            "decision_reason": "no_complete_month_available_after_validation",
            "readiness_artifact_path": str(READINESS_ARTIFACT),
            "frozen_manifest_artifact_path": str(FROZEN_MANIFEST_ARTIFACT),
            "current_validation_month_end": readiness["current_validation_month_end"],
            "new_complete_unseen_month_exists": False,
            "combined_long_unlocked": False,
            "stages": [],
            "per_run_artifacts": [],
        }

    run_root = _ensure_unique_run_dir(FORWARD_RUNS_ROOT, "forward_confirm_bundle")
    stages: list[dict[str, Any]] = []
    per_run_artifacts: list[dict[str, Any]] = []

    too_late_dir = _ensure_unique_run_dir(run_root, TOO_LATE_STAGE)
    _run_replay_stage(db_path=readiness["source_db_path"], output_dir=too_late_dir)
    too_late_payload = _read_stage_payload(too_late_dir, "action_precision_long_too_late_forward_confirm.json")
    stages.append(_stage_summary(TOO_LATE_STAGE, too_late_dir, too_late_payload, artifact_name="action_precision_long_too_late_forward_confirm.json"))
    per_run_artifacts.append(
        {
            "stage": TOO_LATE_STAGE,
            "run_dir": str(too_late_dir),
            "forward_confirm_artifact": str(too_late_dir / "action_precision_long_too_late_forward_confirm.json"),
            "authoritative_decision_artifact": str(too_late_dir / "authoritative_decision.long_too_late_forward_confirm.json"),
        }
    )
    if not str(too_late_payload.get("decision") or "").startswith("keep"):
        return {
            "schema_version": "tradex_action_precision_forward_orchestration_v1",
            "generated_at": _utc_now().isoformat(),
            "status": "hold_needs_more_time",
            "decision": "hold_needs_more_time",
            "decision_reason": "long too-late forward confirm did not keep; combined long not unlocked",
            "readiness_artifact_path": str(READINESS_ARTIFACT),
            "frozen_manifest_artifact_path": str(FROZEN_MANIFEST_ARTIFACT),
            "current_validation_month_end": readiness["current_validation_month_end"],
            "new_complete_unseen_month_exists": True,
            "combined_long_unlocked": False,
            "stages": stages,
            "per_run_artifacts": per_run_artifacts,
            "run_root": str(run_root),
        }

    weak_dir = _ensure_unique_run_dir(run_root, WEAK_STAGE)
    _run_replay_stage(db_path=readiness["source_db_path"], output_dir=weak_dir)
    weak_payload = _read_stage_payload(weak_dir, "action_precision_long_weak_direction_forward_confirm.json")
    stages.append(_stage_summary(WEAK_STAGE, weak_dir, weak_payload, artifact_name="action_precision_long_weak_direction_forward_confirm.json"))
    per_run_artifacts.append(
        {
            "stage": WEAK_STAGE,
            "run_dir": str(weak_dir),
            "forward_confirm_artifact": str(weak_dir / "action_precision_long_weak_direction_forward_confirm.json"),
            "authoritative_decision_artifact": str(weak_dir / "authoritative_decision.long_weak_direction_forward_confirm.json"),
        }
    )
    if not str(weak_payload.get("decision") or "").startswith("keep"):
        return {
            "schema_version": "tradex_action_precision_forward_orchestration_v1",
            "generated_at": _utc_now().isoformat(),
            "status": "hold_needs_more_time",
            "decision": "hold_needs_more_time",
            "decision_reason": "long weak-direction forward confirm did not keep; combined long not unlocked",
            "readiness_artifact_path": str(READINESS_ARTIFACT),
            "frozen_manifest_artifact_path": str(FROZEN_MANIFEST_ARTIFACT),
            "current_validation_month_end": readiness["current_validation_month_end"],
            "new_complete_unseen_month_exists": True,
            "combined_long_unlocked": False,
            "stages": stages,
            "per_run_artifacts": per_run_artifacts,
            "run_root": str(run_root),
        }

    combined_dir = _ensure_unique_run_dir(run_root, COMBINED_STAGE)
    combined_summary = {
        "schema_version": "tradex_action_precision_long_forward_confirm_bundle_v1",
        "generated_at": _utc_now().isoformat(),
        "readiness_period": {
            "snapshot_max_trade_date": readiness["snapshot_max_trade_date"],
            "replay_lookback_start_date": readiness["replay_lookback_start_date"],
            "last_fully_confirmable_month": readiness["last_fully_confirmable_month"],
            "current_validation_month_end": readiness["current_validation_month_end"],
            "new_complete_unseen_months": readiness["new_complete_unseen_months"],
        },
        "too_late_forward_confirm": too_late_payload,
        "weak_direction_forward_confirm": weak_payload,
        "combined_long_unlocked": True,
        "combined_long_status": "prepare_combined_long_revision",
        "combined_long_run_dir": str(combined_dir),
        "combined_long_note": "both frozen singles confirmed; combined-long testing is now unlocked",
    }
    _write_json(combined_dir / "authoritative_decision.long_forward_confirm_bundle.json", combined_summary)
    stages.append(
        {
            "stage": COMBINED_STAGE,
            "run_dir": str(combined_dir),
            "artifact_path": str(combined_dir / "authoritative_decision.long_forward_confirm_bundle.json"),
            "decision_artifact_path": str(combined_dir / "authoritative_decision.long_forward_confirm_bundle.json"),
            "decision": "prepare_combined_long_revision",
            "decision_reason": "both singles confirmed; combined long is unlocked",
        }
    )
    per_run_artifacts.append(
        {
            "stage": COMBINED_STAGE,
            "run_dir": str(combined_dir),
            "authoritative_decision_artifact": str(combined_dir / "authoritative_decision.long_forward_confirm_bundle.json"),
        }
    )
    return {
        "schema_version": "tradex_action_precision_forward_orchestration_v1",
        "generated_at": _utc_now().isoformat(),
        "status": "ready",
        "decision": "keep",
        "decision_reason": "both frozen singles confirmed; combined long unlocked",
        "readiness_artifact_path": str(READINESS_ARTIFACT),
        "frozen_manifest_artifact_path": str(FROZEN_MANIFEST_ARTIFACT),
        "current_validation_month_end": readiness["current_validation_month_end"],
        "new_complete_unseen_month_exists": True,
        "combined_long_unlocked": True,
        "combined_long_bundle_artifact_path": str(combined_dir / "authoritative_decision.long_forward_confirm_bundle.json"),
        "stages": stages,
        "per_run_artifacts": per_run_artifacts,
        "run_root": str(run_root),
    }


def build_orchestration(*, db_path: str | None = None) -> dict[str, Any]:
    readiness = build_readiness(db_path=db_path)
    return _orchestration_from_readiness(readiness)


def build_and_write_control_artifacts(*, db_path: str | None = None) -> dict[str, Any]:
    readiness = build_readiness(db_path=db_path)
    manifest = build_frozen_manifest(readiness=readiness)
    template = build_forward_report_template()
    orchestration = _orchestration_from_readiness(readiness)
    _write_json(READINESS_ARTIFACT, readiness)
    _write_json(FROZEN_MANIFEST_ARTIFACT, manifest)
    _write_json(ORCHESTRATION_ARTIFACT, orchestration)
    _write_json(REPORT_TEMPLATE_ARTIFACT, template)
    return {
        "readiness": readiness,
        "manifest": manifest,
        "orchestration": orchestration,
        "template": template,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="TRADEX forward-confirm readiness and orchestration control.")
    sub = parser.add_subparsers(dest="command", required=True)

    readiness_parser = sub.add_parser("readiness", help="Refresh the source snapshot and emit readiness JSON.")
    readiness_parser.add_argument("--db-path", default="", help="Path to the authoritative source DB")

    orchestration_parser = sub.add_parser("orchestrate", help="Run or stage the frozen long forward-confirm bundle.")
    orchestration_parser.add_argument("--db-path", default="", help="Path to the authoritative source DB")

    manifest_parser = sub.add_parser("manifest", help="Write the frozen-rule manifest from current artifacts.")
    manifest_parser.add_argument("--db-path", default="", help="Path to the authoritative source DB")

    template_parser = sub.add_parser("template", help="Write the compare-report template for the next forward run.")
    template_parser.add_argument("--db-path", default="", help="Path to the authoritative source DB")

    args = parser.parse_args()
    db_path = args.db_path or None

    if args.command == "readiness":
        payload = build_readiness(db_path=db_path)
        _write_json(READINESS_ARTIFACT, payload)
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return
    if args.command == "manifest":
        readiness = build_readiness(db_path=db_path)
        payload = build_frozen_manifest(readiness=readiness)
        _write_json(FROZEN_MANIFEST_ARTIFACT, payload)
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return
    if args.command == "template":
        payload = build_forward_report_template()
        _write_json(REPORT_TEMPLATE_ARTIFACT, payload)
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return
    if args.command == "orchestrate":
        payload = build_and_write_control_artifacts(db_path=db_path)
        print(json.dumps(payload["orchestration"], ensure_ascii=False, indent=2))
        return
    raise ValueError(f"unknown command: {args.command}")


if __name__ == "__main__":
    main()
