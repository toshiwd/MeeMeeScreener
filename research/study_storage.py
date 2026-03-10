from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from research.config import ResearchConfig
from research.storage import ResearchPaths, now_utc_iso, read_json, write_csv, write_json


DATASET_META_FILE = "dataset_meta.json"
MANIFEST_FILE = "manifest.json"
TRIAL_STATE_FILE = "trial_state.json"
SEARCH_TRACE_FILE = "search_trace.csv"
OOS_METRICS_FILE = "oos_metrics.csv"
DIST_HORIZON_FILE = "distribution_by_horizon.csv"
DIST_CLUSTER_FILE = "distribution_by_cluster.csv"
DIST_REGIME_FILE = "distribution_by_regime.csv"
BAD_HYPOTHESES_FILE = "bad_hypotheses_summary.csv"
TOP_HYPOTHESES_FILE = "top_hypotheses.json"
ADOPTED_HYPOTHESES_FILE = "adopted_hypotheses.json"


def _combo_key(timeframe: str, family: str) -> str:
    return f"{timeframe}::{family}"


def study_paths(paths: ResearchPaths, study_id: str) -> dict[str, Path]:
    root = paths.study_dir(study_id)
    return {
        "root": root,
        "datasets": root / "datasets",
        "fold_artifacts": root / "fold_artifacts",
        "manifest": root / MANIFEST_FILE,
        "dataset_meta": root / DATASET_META_FILE,
        "trial_state": root / TRIAL_STATE_FILE,
        "search_trace": root / SEARCH_TRACE_FILE,
        "oos_metrics": root / OOS_METRICS_FILE,
        "dist_horizon": root / DIST_HORIZON_FILE,
        "dist_cluster": root / DIST_CLUSTER_FILE,
        "dist_regime": root / DIST_REGIME_FILE,
        "bad_hypotheses": root / BAD_HYPOTHESES_FILE,
        "top_hypotheses": root / TOP_HYPOTHESES_FILE,
        "adopted_hypotheses": root / ADOPTED_HYPOTHESES_FILE,
    }


def ensure_study_dirs(paths: ResearchPaths, study_id: str) -> dict[str, Path]:
    spaths = study_paths(paths, study_id)
    spaths["root"].mkdir(parents=True, exist_ok=True)
    spaths["datasets"].mkdir(parents=True, exist_ok=True)
    spaths["fold_artifacts"].mkdir(parents=True, exist_ok=True)
    return spaths


def init_study_manifest(
    paths: ResearchPaths,
    config: ResearchConfig,
    snapshot_id: str,
    study_id: str | None = None,
    *,
    timeframes: list[str] | tuple[str, ...] | None = None,
    families: list[str] | tuple[str, ...] | None = None,
    resume: bool = False,
) -> dict[str, Any]:
    resolved_study_id = study_id or paths.next_study_id(snapshot_id=snapshot_id)
    spaths = ensure_study_dirs(paths, resolved_study_id)
    manifest_path = spaths["manifest"]
    if manifest_path.exists():
        return read_json(manifest_path)

    manifest = {
        "study_id": resolved_study_id,
        "snapshot_id": snapshot_id,
        "created_at": now_utc_iso(),
        "updated_at": now_utc_iso(),
        "status": "initialized",
        "resume_enabled": bool(resume),
        "timeframes": list(timeframes or config.study.timeframes),
        "families": list(families or config.study.families),
        "config": {
            "split": {
                "train_years": int(config.split.train_years),
                "valid_months": int(config.split.valid_months),
                "test_months": int(config.split.test_months),
            },
            "study": {
                "trials_per_family": dict(config.study.trials_per_family),
                "refinement_trials_per_family": dict(config.study.refinement_trials_per_family),
                "retention_gates": {
                    "min_profit_factor": float(config.study.retention_gates.min_profit_factor),
                    "min_positive_window_ratio": float(config.study.retention_gates.min_positive_window_ratio),
                    "max_worst_drawdown": float(config.study.retention_gates.max_worst_drawdown),
                    "min_samples": int(config.study.retention_gates.min_samples),
                    "top_hypotheses_per_combo": int(config.study.retention_gates.top_hypotheses_per_combo),
                },
                "adoption_gates": {
                    "min_oos_return": float(config.study.adoption_gates.min_oos_return),
                    "min_pf": float(config.study.adoption_gates.min_pf),
                    "min_positive_window_ratio": float(config.study.adoption_gates.min_positive_window_ratio),
                    "max_worst_drawdown": float(config.study.adoption_gates.max_worst_drawdown),
                    "min_stability": float(config.study.adoption_gates.min_stability),
                    "min_cluster_consistency": float(config.study.adoption_gates.min_cluster_consistency),
                    "min_fold_months": int(config.study.adoption_gates.min_fold_months),
                },
                "seed_weights": deepcopy(config.study.seed_weights),
                "negation_penalties": list(config.study.negation_penalties),
                "selection_cutoffs": list(config.study.selection_cutoffs),
                "top_refinement_parents": int(config.study.top_refinement_parents),
                "random_seed": int(config.study.random_seed),
            },
        },
    }
    write_json(manifest_path, manifest)
    return manifest


def update_study_manifest(paths: ResearchPaths, study_id: str, patch: dict[str, Any]) -> dict[str, Any]:
    spaths = ensure_study_dirs(paths, study_id)
    manifest = read_json(spaths["manifest"])
    manifest.update(patch)
    manifest["updated_at"] = now_utc_iso()
    write_json(spaths["manifest"], manifest)
    return manifest


def _initial_combo_state() -> dict[str, Any]:
    return {
        "base_completed_ids": [],
        "refine_completed_ids": [],
        "queued_refinements": [],
        "seen_param_hashes": [],
        "best_trial_ids": [],
        "status": "pending",
        "updated_at": now_utc_iso(),
    }


def init_trial_state(
    paths: ResearchPaths,
    study_id: str,
    *,
    timeframes: list[str] | tuple[str, ...],
    families: list[str] | tuple[str, ...],
) -> dict[str, Any]:
    spaths = ensure_study_dirs(paths, study_id)
    state_path = spaths["trial_state"]
    if state_path.exists():
        return read_json(state_path)

    combos = {
        _combo_key(timeframe, family): _initial_combo_state()
        for timeframe in timeframes
        for family in families
    }
    state = {
        "study_id": study_id,
        "created_at": now_utc_iso(),
        "updated_at": now_utc_iso(),
        "combos": combos,
    }
    write_json(state_path, state)
    return state


def load_trial_state(paths: ResearchPaths, study_id: str) -> dict[str, Any]:
    return read_json(study_paths(paths, study_id)["trial_state"])


def save_trial_state(paths: ResearchPaths, study_id: str, state: dict[str, Any]) -> None:
    state["updated_at"] = now_utc_iso()
    write_json(study_paths(paths, study_id)["trial_state"], state)


def set_combo_state(
    state: dict[str, Any],
    timeframe: str,
    family: str,
    patch: dict[str, Any],
) -> dict[str, Any]:
    combos = state.setdefault("combos", {})
    key = _combo_key(timeframe, family)
    combo = combos.setdefault(key, _initial_combo_state())
    combo.update(patch)
    combo["updated_at"] = now_utc_iso()
    combos[key] = combo
    return combo


def dataset_path(paths: ResearchPaths, study_id: str, timeframe: str) -> Path:
    return study_paths(paths, study_id)["datasets"] / f"events_{timeframe}.csv"


def fold_artifact_dir(paths: ResearchPaths, study_id: str, trial_id: str) -> Path:
    path = study_paths(paths, study_id)["fold_artifacts"] / trial_id
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_dataset_meta(paths: ResearchPaths, study_id: str, payload: dict[str, Any]) -> None:
    write_json(study_paths(paths, study_id)["dataset_meta"], payload)


def load_dataset_meta(paths: ResearchPaths, study_id: str) -> dict[str, Any]:
    return read_json(study_paths(paths, study_id)["dataset_meta"])


def write_frame(paths: ResearchPaths, study_id: str, key: str, frame: pd.DataFrame) -> None:
    spaths = study_paths(paths, study_id)
    mapping = {
        "search_trace": spaths["search_trace"],
        "oos_metrics": spaths["oos_metrics"],
        "distribution_by_horizon": spaths["dist_horizon"],
        "distribution_by_cluster": spaths["dist_cluster"],
        "distribution_by_regime": spaths["dist_regime"],
        "bad_hypotheses_summary": spaths["bad_hypotheses"],
    }
    if key not in mapping:
        raise KeyError(f"unsupported study frame key: {key}")
    write_csv(mapping[key], frame)


def write_json_payload(paths: ResearchPaths, study_id: str, key: str, payload: dict[str, Any]) -> None:
    spaths = study_paths(paths, study_id)
    mapping = {
        "top_hypotheses": spaths["top_hypotheses"],
        "adopted_hypotheses": spaths["adopted_hypotheses"],
    }
    if key not in mapping:
        raise KeyError(f"unsupported study json key: {key}")
    write_json(mapping[key], payload)


def find_latest_resume_study(paths: ResearchPaths, snapshot_id: str | None = None) -> str | None:
    candidates: list[tuple[datetime, str]] = []
    for manifest_path in paths.studies_root.glob("*/manifest.json"):
        try:
            manifest = read_json(manifest_path)
        except Exception:
            continue
        if snapshot_id and str(manifest.get("snapshot_id")) != str(snapshot_id):
            continue
        status = str(manifest.get("status") or "")
        if status == "completed":
            continue
        created_raw = str(manifest.get("created_at") or "")
        try:
            created_at = datetime.strptime(created_raw, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        except Exception:
            continue
        candidates.append((created_at, str(manifest.get("study_id") or "")))
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0], reverse=True)
    study_id = candidates[0][1].strip()
    return study_id or None
