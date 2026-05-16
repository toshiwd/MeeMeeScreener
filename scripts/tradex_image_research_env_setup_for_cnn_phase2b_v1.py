from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.backend.services import tradex_research_contracts as contracts
from scripts import tradex_image_cnn_baseline_phase2b as phase2b_mod


AXIS_ID = "image_research_env_setup_for_cnn_phase2b_v1"
SCHEMA_PREFIX = "tradex_image_research_env_setup_for_cnn_phase2b_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\image_research_env_setup_for_cnn_phase2b_v1")
DEFAULT_ENV_DIR = Path(r"G:\Tradex\envs\image-cnn-phase2b")
DEFAULT_PHASE0_1_RUN_ID = "20260513T080000Z-image-assisted-rerank-phase0-1"
DEFAULT_PHASE2_RUN_ID = "20260513T090000Z-image-only-classifier-baseline-phase2"
DEFAULT_PHASE2B_RERUN_ID = "20260513T110000Z-image-cnn-baseline-phase2b-torch"
DEFAULT_PHASE0_1_ROOT = Path(r"G:\Tradex\image_assisted_rerank_phase0_1")
DEFAULT_PHASE2_ROOT = Path(r"G:\Tradex\image_only_classifier_baseline_phase2")
DEFAULT_PHASE2B_OUTPUT_ROOT = Path(r"G:\Tradex\image_cnn_baseline_phase2b")
RESEARCH_PYTHON_PACKAGES = ("numpy", "pandas", "pillow", "scikit-learn", "duckdb")

REQUIRED_ARTIFACTS = (
    "evaluation_contract.json",
    "run_manifest.json",
    "source_artifact_refs.json",
    "environment_setup_contract.json",
    "dependency_audit_before.json",
    "isolation_audit.json",
    "install_plan.json",
    "install_log.jsonl",
    "dependency_audit_after.json",
    "acceptance_criteria_audit.json",
    "rerun_command_contract.json",
    "phase2b_rerun_report.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{AXIS_ID}"


def _json_ready(value: Any) -> Any:
    return phase2b_mod._json_ready(value)


def _json_text(payload: Any) -> str:
    return phase2b_mod._json_text(payload)


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    return phase2b_mod._write_json(path, payload)


def _write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> Path:
    return phase2b_mod._write_jsonl(path, rows)


def _load_json(path: Path) -> dict[str, Any]:
    return phase2b_mod._load_json(path)


def _stable_hash(payload: Any) -> str:
    return phase2b_mod._stable_hash(payload)


def _safe_path(value: str | Path | None, default: Path) -> Path:
    return phase2b_mod._safe_path(value, default)


def _run_dir(root: str | Path, run_id: str, default_root: Path) -> Path:
    return phase2b_mod._run_dir(root, run_id, default_root)


def _file_hash(path: Path) -> str | None:
    return phase2b_mod._file_hash(path)


def _bool_arg(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_python(env_dir: Path) -> Path:
    return env_dir / "Scripts" / "python.exe"


def _run_process(command: list[str], *, cwd: Path, timeout_seconds: int = 1800) -> dict[str, Any]:
    started = _utc_now()
    try:
        completed = subprocess.run(
            command,
            cwd=str(cwd),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout_seconds,
        )
        return {
            "started_at": started,
            "finished_at": _utc_now(),
            "command": command,
            "returncode": int(completed.returncode),
            "stdout_tail": completed.stdout[-4000:],
            "stderr_tail": completed.stderr[-4000:],
            "timed_out": False,
        }
    except subprocess.TimeoutExpired as exc:
        return {
            "started_at": started,
            "finished_at": _utc_now(),
            "command": command,
            "returncode": None,
            "stdout_tail": (exc.stdout or "")[-4000:] if isinstance(exc.stdout, str) else "",
            "stderr_tail": (exc.stderr or "")[-4000:] if isinstance(exc.stderr, str) else "",
            "timed_out": True,
        }


def audit_python_environment(python_exe: Path) -> dict[str, Any]:
    if not python_exe.exists():
        return {
            "python_exe": str(python_exe),
            "python_exists": False,
            "torch_available": False,
            "torchvision_available": False,
            "cuda_available": False,
            "audit_error": "python_exe_missing",
        }
    code = (
        "import importlib.util,json,sys\n"
        "payload={'python_exe':sys.executable,'python_version':sys.version,'python_prefix':sys.prefix}\n"
        "for name in ['torch','torchvision','numpy','pandas','PIL','sklearn','duckdb']:\n"
        " spec=importlib.util.find_spec(name); payload[name+'_available']=bool(spec); payload[name+'_version']=None\n"
        " if spec:\n"
        "  mod=__import__(name); payload[name+'_version']=getattr(mod,'__version__',None)\n"
        "payload['cuda_available']=False\n"
        "if payload.get('torch_available'):\n"
        " import torch; payload['cuda_available']=bool(torch.cuda.is_available()); payload['cuda_device_count']=int(torch.cuda.device_count())\n"
        "print(json.dumps(payload, sort_keys=True))\n"
    )
    result = _run_process([str(python_exe), "-c", code], cwd=Path.cwd(), timeout_seconds=120)
    payload: dict[str, Any] = {
        "python_exe": str(python_exe),
        "python_exists": True,
        "audit_returncode": result["returncode"],
        "audit_timed_out": result["timed_out"],
        "audit_stdout_tail": result["stdout_tail"],
        "audit_stderr_tail": result["stderr_tail"],
    }
    if result["returncode"] == 0:
        try:
            payload.update(json.loads(str(result["stdout_tail"]).strip().splitlines()[-1]))
        except Exception as exc:
            payload["audit_parse_error"] = str(exc)
    payload.setdefault("torch_available", False)
    payload.setdefault("torchvision_available", False)
    payload.setdefault("cuda_available", False)
    return payload


def _repo_dependency_hashes(repo_root: Path) -> dict[str, str | None]:
    names = [
        "requirements.txt",
        "requirements-dev.txt",
        "pyproject.toml",
        "setup.py",
        "setup.cfg",
        "package.json",
        "uv.lock",
        "poetry.lock",
    ]
    return {name: _file_hash(repo_root / name) for name in names if (repo_root / name).exists()}


def _source_refs(phase0_1_run_id: str, phase2_run_id: str, phase0_1_root: Path, phase2_root: Path) -> dict[str, Any]:
    phase0_dir = phase0_1_root / phase0_1_run_id
    phase2_dir = phase2_root / phase2_run_id
    return {
        "schema_version": f"{SCHEMA_PREFIX}_source_artifact_refs_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "refs": [
            {
                "axis_id": "image_assisted_rerank_phase0_1",
                "run_id": phase0_1_run_id,
                "path": str(phase0_dir),
                "exists": phase0_dir.exists(),
                "file_hashes": {
                    "research_decision.json": _file_hash(phase0_dir / "research_decision.json"),
                    "phase2_readiness_report.json": _file_hash(phase0_dir / "phase2_readiness_report.json"),
                    "_ARTIFACT_COMPLETE.json": _file_hash(phase0_dir / "_ARTIFACT_COMPLETE.json"),
                },
            },
            {
                "axis_id": "image_only_classifier_baseline_phase2",
                "run_id": phase2_run_id,
                "path": str(phase2_dir),
                "exists": phase2_dir.exists(),
                "file_hashes": {
                    "research_decision.json": _file_hash(phase2_dir / "research_decision.json"),
                    "phase3_readiness_report.json": _file_hash(phase2_dir / "phase3_readiness_report.json"),
                    "_ARTIFACT_COMPLETE.json": _file_hash(phase2_dir / "_ARTIFACT_COMPLETE.json"),
                },
            },
        ],
    }


def _validate_source_decisions(phase0_1_root: Path, phase0_1_run_id: str, phase2_root: Path, phase2_run_id: str) -> dict[str, Any]:
    phase0_dir = phase0_1_root / phase0_1_run_id
    phase2_dir = phase2_root / phase2_run_id
    phase0_decision = _load_json(phase0_dir / "research_decision.json")
    phase0_readiness = _load_json(phase0_dir / "phase2_readiness_report.json")
    phase2_decision = _load_json(phase2_dir / "research_decision.json")
    phase2_readiness = _load_json(phase2_dir / "phase3_readiness_report.json")
    if phase0_decision.get("authoritative_research_decision") != "image_assisted_phase0_1_ready_for_phase2":
        raise RuntimeError("Phase0/1 source is not ready_for_phase2")
    if phase0_readiness.get("ready_for_phase2") is not True:
        raise RuntimeError("Phase0/1 readiness report is not ready_for_phase2")
    if phase2_decision.get("authoritative_research_decision") != "image_only_classifier_phase2_failed":
        raise RuntimeError("Phase2 source is not image_only_classifier_phase2_failed")
    if phase2_readiness.get("ready_for_fusion") is not False:
        raise RuntimeError("Phase2 source unexpectedly allows fusion")
    return {
        "source_phase0_1_decision": phase0_decision.get("authoritative_research_decision"),
        "source_phase0_1_ready_for_phase2": phase0_readiness.get("ready_for_phase2"),
        "source_phase2_decision": phase2_decision.get("authoritative_research_decision"),
        "source_phase2_ready_for_fusion": phase2_readiness.get("ready_for_fusion"),
    }


def run_image_research_env_setup_for_cnn_phase2b_v1(
    *,
    run_id: str | None = None,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    env_dir: str | Path = DEFAULT_ENV_DIR,
    source_image_phase0_1_run_id: str = DEFAULT_PHASE0_1_RUN_ID,
    source_image_phase2_run_id: str = DEFAULT_PHASE2_RUN_ID,
    source_image_phase0_1_root: str | Path = DEFAULT_PHASE0_1_ROOT,
    source_image_phase2_root: str | Path = DEFAULT_PHASE2_ROOT,
    phase2b_output_root: str | Path = DEFAULT_PHASE2B_OUTPUT_ROOT,
    phase2b_rerun_id: str = DEFAULT_PHASE2B_RERUN_ID,
    create_env: bool = True,
    install_torch: bool = True,
    run_phase2b_if_ready: bool = True,
    repo_root: str | Path | None = None,
) -> dict[str, Any]:
    run_id = run_id or _default_run_id()
    repo = _safe_path(repo_root, Path.cwd()) if repo_root else Path.cwd().resolve()
    output_dir = _run_dir(output_root, run_id, DEFAULT_OUTPUT_ROOT)
    output_dir.mkdir(parents=True, exist_ok=True)
    env_path = _safe_path(env_dir, DEFAULT_ENV_DIR)
    phase0_root = _safe_path(source_image_phase0_1_root, DEFAULT_PHASE0_1_ROOT)
    phase2_root = _safe_path(source_image_phase2_root, DEFAULT_PHASE2_ROOT)
    phase2b_root = _safe_path(phase2b_output_root, DEFAULT_PHASE2B_OUTPUT_ROOT)
    source_status = _validate_source_decisions(phase0_root, source_image_phase0_1_run_id, phase2_root, source_image_phase2_run_id)
    before_hashes = _repo_dependency_hashes(repo)
    current_python_audit = audit_python_environment(Path(sys.executable))
    env_python = _env_python(env_path)
    dependency_before = {
        "schema_version": f"{SCHEMA_PREFIX}_dependency_audit_before_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "current_python": current_python_audit,
        "target_env_python": audit_python_environment(env_python),
    }
    install_log: list[dict[str, Any]] = []
    if create_env and not env_python.exists():
        env_path.parent.mkdir(parents=True, exist_ok=True)
        install_log.append(_run_process([str(Path(sys.executable)), "-m", "venv", str(env_path)], cwd=repo, timeout_seconds=600))
    if install_torch and env_python.exists():
        install_log.append(_run_process([str(env_python), "-m", "pip", "install", "--upgrade", "pip"], cwd=repo, timeout_seconds=900))
        install_log.append(
            _run_process(
                [str(env_python), "-m", "pip", "install", *RESEARCH_PYTHON_PACKAGES],
                cwd=repo,
                timeout_seconds=1800,
            )
        )
        install_log.append(
            _run_process(
                [str(env_python), "-m", "pip", "install", "torch", "torchvision", "--index-url", "https://download.pytorch.org/whl/cpu"],
                cwd=repo,
                timeout_seconds=3600,
            )
        )
    dependency_after = {
        "schema_version": f"{SCHEMA_PREFIX}_dependency_audit_after_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "current_python": audit_python_environment(Path(sys.executable)),
        "target_env_python": audit_python_environment(env_python),
    }
    after_hashes = _repo_dependency_hashes(repo)
    target = dependency_after["target_env_python"]
    research_deps_ready = all(
        bool(target.get(key))
        for key in ("numpy_available", "pandas_available", "PIL_available", "sklearn_available", "duckdb_available")
    )
    env_ready = bool(target.get("python_exists") and target.get("torch_available") and target.get("torchvision_available") and research_deps_ready)
    isolation_audit = {
        "schema_version": f"{SCHEMA_PREFIX}_isolation_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "env_dir": str(env_path),
        "env_under_g_tradex": str(env_path).lower().startswith(str(Path(r"G:\Tradex")).lower()),
        "repo_root": str(repo),
        "production_dependency_hashes_before": before_hashes,
        "production_dependency_hashes_after": after_hashes,
        "production_dependency_changed": before_hashes != after_hashes,
        "meemee_runtime_changed": False,
        "production_runtime_requirements_modified": False,
        "meemee_runtime_dependencies_modified": False,
    }
    install_plan = {
        "schema_version": f"{SCHEMA_PREFIX}_install_plan_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "create_env": create_env,
        "install_torch": install_torch,
        "target_env_dir": str(env_path),
        "torch_install_command": [str(env_python), "-m", "pip", "install", "torch", "torchvision", "--index-url", "https://download.pytorch.org/whl/cpu"],
        "research_dependency_install_command": [str(env_python), "-m", "pip", "install", *RESEARCH_PYTHON_PACKAGES],
        "required_research_python_packages": list(RESEARCH_PYTHON_PACKAGES),
        "production_dependency_changed": False,
        "meemee_runtime_changed": False,
        "sklearn_fallback_allowed": False,
    }
    phase2b_command = [
        str(env_python),
        str(repo / "scripts" / "tradex_image_cnn_baseline_phase2b.py"),
        "--source-image-phase0-1-run-id",
        source_image_phase0_1_run_id,
        "--source-image-phase2-run-id",
        source_image_phase2_run_id,
        "--run-id",
        phase2b_rerun_id,
    ]
    rerun_report: dict[str, Any] = {
        "schema_version": f"{SCHEMA_PREFIX}_phase2b_rerun_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "phase2b_rerun_attempted": False,
        "phase2b_rerun_command": phase2b_command,
        "phase2b_rerun_id": phase2b_rerun_id,
        "phase2b_output_dir": str(phase2b_root / phase2b_rerun_id),
        "phase2b_rerun_returncode": None,
        "phase2b_existing_complete_reused": False,
        "phase2b_authoritative_research_decision": None,
        "phase2b_ready_for_fusion": None,
    }
    existing_complete = phase2b_root / phase2b_rerun_id / "_ARTIFACT_COMPLETE.json"
    existing_decision = phase2b_root / phase2b_rerun_id / "research_decision.json"
    existing_readiness = phase2b_root / phase2b_rerun_id / "phase3_readiness_report.json"
    if env_ready and existing_complete.exists() and existing_decision.exists() and existing_readiness.exists():
        complete_json = _load_json(existing_complete)
        decision_json = _load_json(existing_decision)
        readiness_json = _load_json(existing_readiness)
        rerun_report.update(
            {
                "phase2b_rerun_attempted": False,
                "phase2b_existing_complete_reused": complete_json.get("complete") is True,
                "phase2b_rerun_returncode": 0,
                "phase2b_authoritative_research_decision": decision_json.get("authoritative_research_decision"),
                "phase2b_decision": decision_json.get("decision"),
                "phase2b_ready_for_fusion": readiness_json.get("ready_for_fusion"),
            }
        )
    elif env_ready and run_phase2b_if_ready:
        result = _run_process(phase2b_command, cwd=repo, timeout_seconds=7200)
        rerun_report.update(
            {
                "phase2b_rerun_attempted": True,
                "phase2b_rerun_returncode": result["returncode"],
                "phase2b_rerun_timed_out": result["timed_out"],
                "phase2b_rerun_stdout_tail": result["stdout_tail"],
                "phase2b_rerun_stderr_tail": result["stderr_tail"],
            }
        )
        decision_path = phase2b_root / phase2b_rerun_id / "research_decision.json"
        readiness_path = phase2b_root / phase2b_rerun_id / "phase3_readiness_report.json"
        if decision_path.exists():
            decision_json = _load_json(decision_path)
            rerun_report["phase2b_authoritative_research_decision"] = decision_json.get("authoritative_research_decision")
            rerun_report["phase2b_decision"] = decision_json.get("decision")
        if readiness_path.exists():
            readiness_json = _load_json(readiness_path)
            rerun_report["phase2b_ready_for_fusion"] = readiness_json.get("ready_for_fusion")
    acceptance = {
        "schema_version": f"{SCHEMA_PREFIX}_acceptance_criteria_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "isolated_research_env_separated": bool(isolation_audit["env_under_g_tradex"] and not isolation_audit["production_dependency_changed"]),
        "torch_torchvision_cuda_recorded": True,
        "research_python_dependencies_ready": research_deps_ready,
        "production_dependency_changed": bool(isolation_audit["production_dependency_changed"]),
        "meemee_runtime_changed": False,
        "sklearn_fallback_used": False,
        "silent_fallback_used": False,
        "research_fallback_used": False,
        "phase2b_rerun_command_artifact_created": True,
        "env_ready": env_ready,
        "cnn_experiment_attempted_only_if_env_ready": bool((not rerun_report["phase2b_rerun_attempted"]) or env_ready),
        "phase2b_existing_complete_reused": bool(rerun_report.get("phase2b_existing_complete_reused")),
        "blocked_hold_if_env_not_ready": not env_ready,
    }
    rerun_succeeded = bool(
        (rerun_report.get("phase2b_rerun_attempted") or rerun_report.get("phase2b_existing_complete_reused"))
        and rerun_report.get("phase2b_rerun_returncode") == 0
        and rerun_report.get("phase2b_authoritative_research_decision")
    )
    if env_ready and rerun_succeeded:
        decision = "keep_candidate" if rerun_report.get("phase2b_ready_for_fusion") is True else "hold"
        authoritative = "image_research_env_ready_and_phase2b_rerun_completed"
        typed_reasons = ["isolated_torch_env_ready", "research_python_deps_ready", "phase2b_rerun_completed_or_reused"]
    elif env_ready and rerun_report.get("phase2b_rerun_attempted"):
        decision = "hold"
        authoritative = "image_research_env_ready_phase2b_rerun_failed"
        typed_reasons = ["isolated_torch_env_ready", "research_python_deps_ready", "phase2b_rerun_failed", "fusion_phase3_remains_blocked"]
    elif env_ready:
        decision = "keep_candidate"
        authoritative = "image_research_env_ready_for_phase2b_rerun"
        typed_reasons = ["isolated_torch_env_ready", "phase2b_rerun_command_created"]
    else:
        decision = "hold"
        authoritative = "image_research_env_setup_hold"
        typed_reasons = ["isolated_torch_env_not_ready", "phase2b_rerun_not_attempted", "fusion_phase3_remains_blocked"]
    evaluation_contract = {
        "schema_version": f"{SCHEMA_PREFIX}_evaluation_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary": "TRADEX-only",
        "purpose": "prepare isolated torch/torchvision research environment for image_cnn_baseline_phase2b",
        "final_research_goal": "starter_entry_candidate_for_10m_buy_only_max3_symbols_around_20_trading_days",
        "fusion_reranker_created": False,
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
    }
    evaluation_contract["contract_hash"] = _stable_hash(evaluation_contract)
    setup_contract = {
        "schema_version": f"{SCHEMA_PREFIX}_environment_setup_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "target_env_dir": str(env_path),
        "target_env_python": str(env_python),
        "isolation_policy": "G:/Tradex research environment only; do not modify MeeMee runtime or production dependencies",
        "sklearn_fallback_allowed": False,
        "torch_required_for_phase2b_training": True,
        "torchvision_required_for_resnet18": True,
        "phase2b_rerun_command": phase2b_command,
    }
    setup_contract["contract_hash"] = _stable_hash(setup_contract)
    source_refs = _source_refs(source_image_phase0_1_run_id, source_image_phase2_run_id, phase0_root, phase2_root)
    run_manifest = contracts.build_run_manifest(
        session_id=run_id,
        seed=20260513,
        random_seed=20260513,
        input_artifacts=source_refs["refs"],
        asof=_utc_now(),
        config={"axis_id": AXIS_ID, "env_dir": str(env_path), "install_torch": install_torch, "run_phase2b_if_ready": run_phase2b_if_ready},
        universe=[],
        period={"source_phase0_1_run_id": source_image_phase0_1_run_id, "source_phase2_run_id": source_image_phase2_run_id},
        horizon="environment setup",
        artifact_detail_level=contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        fallback_status=contracts.TRADEX_FALLBACK_STATUS_AUTHORITATIVE,
        cost_model=contracts.TRADEX_DEFAULT_COST_MODEL,
    )
    research_decision = {
        "schema_version": f"{SCHEMA_PREFIX}_research_decision_v1",
        "generated_at": _utc_now(),
        "research_phase": AXIS_ID,
        "boundary": "TRADEX-only",
        "axis_moved": "image_research_env_setup_for_cnn_phase2b",
        "source_phase2_decision": source_status["source_phase2_decision"],
        "isolated_research_env_created_or_validated": env_python.exists(),
        "env_ready_for_image_cnn_phase2b": env_ready,
        "torch_available": bool(target.get("torch_available")),
        "torchvision_available": bool(target.get("torchvision_available")),
        "research_python_dependencies_ready": research_deps_ready,
        "cuda_available": bool(target.get("cuda_available")),
        "production_dependency_changed": bool(isolation_audit["production_dependency_changed"]),
        "meemee_runtime_changed": False,
        "sklearn_fallback_used": False,
        "phase2b_rerun_attempted": bool(rerun_report["phase2b_rerun_attempted"]),
        "phase2b_existing_complete_reused": bool(rerun_report.get("phase2b_existing_complete_reused")),
        "phase2b_ready_for_fusion": rerun_report.get("phase2b_ready_for_fusion"),
        "fusion_reranker_created": False,
        "candidate_scoring_created": False,
        "threshold_policy_created": False,
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
        "safe_full_used_as_hard_filter": False,
        "negative_guard_used_as_hard_veto": False,
        "yolo_used": False,
        "llm_used": False,
        "silent_fallback_used": False,
        "research_fallback_used": False,
        "decision": decision,
        "authoritative_research_decision": authoritative,
        "typed_reasons": typed_reasons,
    }
    complete = {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "run_id": run_id,
        "complete": True,
        "required_artifacts": list(REQUIRED_ARTIFACTS),
        "missing_artifacts": [],
        "artifact_detail_level": contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        "silent_fallback_used": False,
        "research_fallback_used": False,
        "decision": decision,
        "authoritative_research_decision": authoritative,
    }
    artifacts = {
        "evaluation_contract.json": evaluation_contract,
        "run_manifest.json": run_manifest,
        "source_artifact_refs.json": source_refs,
        "environment_setup_contract.json": setup_contract,
        "dependency_audit_before.json": dependency_before,
        "isolation_audit.json": isolation_audit,
        "install_plan.json": install_plan,
        "dependency_audit_after.json": dependency_after,
        "acceptance_criteria_audit.json": acceptance,
        "rerun_command_contract.json": {
            "schema_version": f"{SCHEMA_PREFIX}_rerun_command_contract_v1",
            "generated_at": _utc_now(),
            "axis_id": AXIS_ID,
            "phase2b_rerun_command": phase2b_command,
            "phase2b_rerun_id": phase2b_rerun_id,
            "phase2b_output_dir": str(phase2b_root / phase2b_rerun_id),
        },
        "phase2b_rerun_report.json": rerun_report,
        "research_decision.json": research_decision,
        "_ARTIFACT_COMPLETE.json": complete,
    }
    _write_jsonl(output_dir / "install_log.jsonl", install_log)
    for filename, payload in artifacts.items():
        _write_json(output_dir / filename, payload)
    missing = [name for name in REQUIRED_ARTIFACTS if not (output_dir / name).exists()]
    if missing:
        raise RuntimeError(f"artifact write incomplete: {missing}")
    return {
        "output_dir": str(output_dir),
        "run_id": run_id,
        "decision": decision,
        "authoritative_research_decision": authoritative,
        "env_ready": env_ready,
        "torch_available": bool(target.get("torch_available")),
        "torchvision_available": bool(target.get("torchvision_available")),
        "research_python_dependencies_ready": research_deps_ready,
        "cuda_available": bool(target.get("cuda_available")),
        "production_dependency_changed": bool(isolation_audit["production_dependency_changed"]),
        "meemee_runtime_changed": False,
        "sklearn_fallback_used": False,
        "phase2b_rerun_attempted": bool(rerun_report["phase2b_rerun_attempted"]),
        "phase2b_existing_complete_reused": bool(rerun_report.get("phase2b_existing_complete_reused")),
        "phase2b_ready_for_fusion": rerun_report.get("phase2b_ready_for_fusion"),
        "silent_fallback_used": False,
        "research_fallback_used": False,
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="TRADEX isolated torch research env setup for CNN Phase2b")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--env-dir", default=str(DEFAULT_ENV_DIR))
    parser.add_argument("--source-image-phase0-1-run-id", default=DEFAULT_PHASE0_1_RUN_ID)
    parser.add_argument("--source-image-phase2-run-id", default=DEFAULT_PHASE2_RUN_ID)
    parser.add_argument("--source-image-phase0-1-root", default=str(DEFAULT_PHASE0_1_ROOT))
    parser.add_argument("--source-image-phase2-root", default=str(DEFAULT_PHASE2_ROOT))
    parser.add_argument("--phase2b-output-root", default=str(DEFAULT_PHASE2B_OUTPUT_ROOT))
    parser.add_argument("--phase2b-rerun-id", default=DEFAULT_PHASE2B_RERUN_ID)
    parser.add_argument("--create-env", default="true")
    parser.add_argument("--install-torch", default="true")
    parser.add_argument("--run-phase2b-if-ready", default="true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    result = run_image_research_env_setup_for_cnn_phase2b_v1(
        run_id=args.run_id,
        output_root=args.output_root,
        env_dir=args.env_dir,
        source_image_phase0_1_run_id=args.source_image_phase0_1_run_id,
        source_image_phase2_run_id=args.source_image_phase2_run_id,
        source_image_phase0_1_root=args.source_image_phase0_1_root,
        source_image_phase2_root=args.source_image_phase2_root,
        phase2b_output_root=args.phase2b_output_root,
        phase2b_rerun_id=args.phase2b_rerun_id,
        create_env=_bool_arg(args.create_env),
        install_torch=_bool_arg(args.install_torch),
        run_phase2b_if_ready=_bool_arg(args.run_phase2b_if_ready),
    )
    print(_json_text(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
