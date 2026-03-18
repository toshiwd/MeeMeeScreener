from __future__ import annotations

import json
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.core.config import config as core_config
from external_analysis.contracts.paths import resolve_source_db_path

SNAPSHOT_KEEP_LATEST = 2
SNAPSHOT_COPY_RETRIES = 10
SNAPSHOT_COPY_WAIT_SEC = 0.25


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _default_snapshot_root() -> Path:
    return (core_config.DATA_DIR / "external_analysis" / "source_snapshots").expanduser().resolve()


def _wal_path(db_path: Path) -> Path:
    return Path(f"{db_path}.wal")


def _snapshot_name(*, label: str) -> str:
    safe = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in str(label or "analysis"))
    timestamp = _utcnow().strftime("%Y%m%dT%H%M%S%fZ")
    return f"{safe}_{timestamp}"


def _copy_file_with_retry(*, source: Path, target: Path) -> None:
    last_error: Exception | None = None
    for _ in range(SNAPSHOT_COPY_RETRIES):
        try:
            shutil.copy2(str(source), str(target))
            return
        except Exception as exc:  # pragma: no cover - exercised via final retry path
            last_error = exc
            time.sleep(SNAPSHOT_COPY_WAIT_SEC)
    if last_error is not None:
        raise last_error


def _cleanup_old_snapshots(*, snapshot_root: Path, keep_latest: int) -> None:
    keep = max(1, int(keep_latest))
    metadata_files = sorted(snapshot_root.glob("*.json"), key=lambda path: path.stat().st_mtime, reverse=True)
    stale_metadata = metadata_files[keep:]
    for metadata_path in stale_metadata:
        try:
            payload = json.loads(metadata_path.read_text(encoding="utf-8"))
        except Exception:
            payload = {}
        snapshot_db_raw = str(payload.get("snapshot_db_path") or "").strip()
        snapshot_wal_raw = str(payload.get("snapshot_wal_path") or "").strip()
        snapshot_db = Path(snapshot_db_raw) if snapshot_db_raw else None
        snapshot_wal = Path(snapshot_wal_raw) if snapshot_wal_raw else None
        for candidate in (snapshot_db, snapshot_wal, metadata_path):
            if candidate is not None and candidate.exists() and candidate != snapshot_root:
                candidate.unlink(missing_ok=True)


def create_source_snapshot(
    *,
    source_db_path: str | None = None,
    snapshot_root: str | None = None,
    label: str = "analysis",
    keep_latest: int = SNAPSHOT_KEEP_LATEST,
) -> dict[str, Any]:
    resolved_source = resolve_source_db_path(source_db_path)
    if not resolved_source.exists():
        raise FileNotFoundError(f"source_db_not_found:{resolved_source}")
    root = Path(str(snapshot_root)).expanduser().resolve() if snapshot_root else _default_snapshot_root()
    root.mkdir(parents=True, exist_ok=True)
    snapshot_id = _snapshot_name(label=label)
    snapshot_db_path = root / f"{snapshot_id}.duckdb"
    snapshot_wal_path = root / f"{snapshot_id}.duckdb.wal"

    _copy_file_with_retry(source=resolved_source, target=snapshot_db_path)
    source_wal = _wal_path(resolved_source)
    wal_copied = False
    if source_wal.exists():
        _copy_file_with_retry(source=source_wal, target=snapshot_wal_path)
        wal_copied = True

    metadata = {
        "snapshot_id": snapshot_id,
        "label": label,
        "created_at": _utcnow().isoformat(),
        "source_db_path": str(resolved_source),
        "snapshot_db_path": str(snapshot_db_path),
        "source_wal_path": str(source_wal) if source_wal.exists() else None,
        "snapshot_wal_path": str(snapshot_wal_path) if wal_copied else None,
        "source_size": resolved_source.stat().st_size,
        "snapshot_size": snapshot_db_path.stat().st_size,
        "wal_copied": wal_copied,
    }
    metadata_path = root / f"{snapshot_id}.json"
    metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    _cleanup_old_snapshots(snapshot_root=root, keep_latest=keep_latest)
    return metadata
