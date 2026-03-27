from __future__ import annotations

import os
from pathlib import Path

from app.backend.services.tradex_experiment_store import resolve_tradex_root


def resolve_image_rerank_root() -> Path:
    raw = os.getenv("MEEMEE_IMAGE_RERANK_ROOT", "").strip()
    if raw:
        root = Path(raw).expanduser().resolve()
    else:
        root = (resolve_tradex_root() / "image_rerank").resolve()
    root.mkdir(parents=True, exist_ok=True)
    return root


def image_rerank_runs_root() -> Path:
    root = resolve_image_rerank_root() / "runs"
    root.mkdir(parents=True, exist_ok=True)
    return root


def image_rerank_run_dir(run_id: str) -> Path:
    path = image_rerank_runs_root() / str(run_id).strip()
    path.mkdir(parents=True, exist_ok=True)
    return path


def image_rerank_run_inputs_dir(run_id: str) -> Path:
    path = image_rerank_run_dir(run_id) / "inputs"
    path.mkdir(parents=True, exist_ok=True)
    return path


def image_rerank_run_manifests_dir(run_id: str) -> Path:
    path = image_rerank_run_dir(run_id) / "manifests"
    path.mkdir(parents=True, exist_ok=True)
    return path


def image_rerank_run_outputs_dir(run_id: str) -> Path:
    path = image_rerank_run_dir(run_id) / "outputs"
    path.mkdir(parents=True, exist_ok=True)
    return path


def image_rerank_run_renders_dir(run_id: str) -> Path:
    path = image_rerank_run_dir(run_id) / "renders"
    path.mkdir(parents=True, exist_ok=True)
    return path


def image_rerank_run_models_dir(run_id: str) -> Path:
    path = image_rerank_run_dir(run_id) / "models"
    path.mkdir(parents=True, exist_ok=True)
    return path
