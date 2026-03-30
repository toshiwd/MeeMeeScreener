from __future__ import annotations

import os
from pathlib import Path

from app.backend.services.tradex_experiment_store import resolve_tradex_root


def resolve_event_image_dataset_root() -> Path:
    raw = os.getenv("MEEMEE_EVENT_IMAGE_DATASET_ROOT", "").strip()
    if raw:
        root = Path(raw).expanduser().resolve()
    else:
        root = (resolve_tradex_root() / "event_image_dataset").resolve()
    root.mkdir(parents=True, exist_ok=True)
    return root


def event_image_datasets_root() -> Path:
    root = resolve_event_image_dataset_root() / "datasets"
    root.mkdir(parents=True, exist_ok=True)
    return root


def event_image_dataset_dir(dataset_id: str) -> Path:
    path = event_image_datasets_root() / str(dataset_id).strip()
    path.mkdir(parents=True, exist_ok=True)
    return path

