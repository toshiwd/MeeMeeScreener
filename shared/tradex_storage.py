from __future__ import annotations

from datetime import datetime, timedelta, timezone
from functools import lru_cache
import os
import shutil
from pathlib import Path


DEFAULT_TRADEX_ROOT = Path(r"G:\Tradex")
DEFAULT_SCRATCH_RETENTION_DAYS = 14
_LAST_SCRATCH_CLEANUP_AT: datetime | None = None


def _resolve_path(value: str | Path) -> Path:
    return Path(str(value)).expanduser().resolve()


@lru_cache(maxsize=1)
def resolve_tradex_root() -> Path:
    raw = str(os.getenv("MEEMEE_TRADEX_ROOT") or "").strip()
    if raw:
        return _resolve_path(raw)
    return _resolve_path(DEFAULT_TRADEX_ROOT)


def tradex_scratch_root() -> Path:
    return resolve_tradex_root() / "scratch"


def tradex_keep_root() -> Path:
    return resolve_tradex_root() / "keep"


def tradex_logs_root() -> Path:
    return resolve_tradex_root() / "logs"


def tradex_db_root() -> Path:
    return resolve_tradex_root() / "db"


def tradex_research_home() -> Path:
    return tradex_scratch_root() / "research"


def tradex_research_sessions_root() -> Path:
    return tradex_scratch_root() / "research_sessions"


def tradex_research_families_root() -> Path:
    return tradex_scratch_root() / "research_families"


def tradex_research_keep_root() -> Path:
    return tradex_keep_root() / "research"


def tradex_path(*parts: str, root: Path | None = None) -> Path:
    base = root or resolve_tradex_root()
    return base.joinpath(*parts)


def tradex_scratch_path(*parts: str) -> Path:
    return tradex_scratch_root().joinpath(*parts)


def tradex_keep_path(*parts: str) -> Path:
    return tradex_keep_root().joinpath(*parts)


def tradex_logs_path(*parts: str) -> Path:
    return tradex_logs_root().joinpath(*parts)


def tradex_db_path(*parts: str) -> Path:
    return tradex_db_root().joinpath(*parts)


def _is_older_than(path: Path, *, cutoff: datetime) -> bool:
    try:
        modified = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    except OSError:
        return False
    return modified < cutoff


def _remove_path(path: Path) -> None:
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path, ignore_errors=True)
        return
    try:
        path.unlink()
    except OSError:
        pass


def _safe_mtime(path: Path) -> float:
    try:
        return path.stat().st_mtime
    except OSError:
        return 0.0


def _cleanup_child_entries(root: Path, *, cutoff: datetime, keep_latest: int | None = None) -> dict[str, int]:
    if not root.exists():
        return {"removed": 0, "skipped": 0}
    entries = sorted(root.iterdir(), key=_safe_mtime, reverse=True)
    candidates = entries if keep_latest is None else entries[max(0, int(keep_latest)) :]
    removed = 0
    skipped = 0
    for entry in candidates:
        if entry.name.startswith("."):
            skipped += 1
            continue
        if not _is_older_than(entry, cutoff=cutoff):
            skipped += 1
            continue
        _remove_path(entry)
        removed += 1
    return {"removed": removed, "skipped": skipped}


def cleanup_tradex_scratch(*, retention_days: int = DEFAULT_SCRATCH_RETENTION_DAYS, force: bool = False) -> dict[str, int]:
    """
    Remove stale scratch outputs under G:\\Tradex\\scratch.

    Kept outputs must live under G:\\Tradex\\keep and are never touched here.
    """
    global _LAST_SCRATCH_CLEANUP_AT

    now = datetime.now(timezone.utc)
    if (not force) and _LAST_SCRATCH_CLEANUP_AT is not None:
        if now - _LAST_SCRATCH_CLEANUP_AT < timedelta(hours=6):
            return {"removed": 0, "skipped": 0}

    root = tradex_scratch_root()
    root.mkdir(parents=True, exist_ok=True)
    cutoff = now - timedelta(days=max(1, int(retention_days)))
    removed = 0
    skipped = 0
    managed_roots = (
        tradex_research_home(),
        tradex_research_sessions_root(),
        tradex_research_families_root(),
        tradex_scratch_path("temp"),
        tradex_scratch_path("source_snapshots"),
    )
    managed_names = {path.name for path in managed_roots}
    for managed_root in managed_roots:
        managed_root.mkdir(parents=True, exist_ok=True)
        result = _cleanup_child_entries(managed_root, cutoff=cutoff)
        removed += result["removed"]
        skipped += result["skipped"]
    for entry in list(root.iterdir()):
        if not entry.exists():
            continue
        if entry.name.startswith(".") or entry.name in managed_names:
            skipped += 1
            continue
        if not _is_older_than(entry, cutoff=cutoff):
            skipped += 1
            continue
        _remove_path(entry)
        removed += 1
    _LAST_SCRATCH_CLEANUP_AT = now
    return {"removed": removed, "skipped": skipped}


def ensure_tradex_layout(*, retention_days: int = DEFAULT_SCRATCH_RETENTION_DAYS) -> dict[str, int]:
    for path in (
        tradex_scratch_root(),
        tradex_keep_root(),
        tradex_logs_root(),
        tradex_db_root(),
        tradex_research_home(),
        tradex_research_sessions_root(),
        tradex_research_families_root(),
        tradex_research_keep_root(),
        tradex_scratch_path("temp"),
        tradex_scratch_path("source_snapshots"),
    ):
        path.mkdir(parents=True, exist_ok=True)
    return cleanup_tradex_scratch(retention_days=retention_days)
