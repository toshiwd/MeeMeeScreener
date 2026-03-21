from __future__ import annotations

import contextlib
import json
import os
import tempfile
import time
from pathlib import Path
from typing import Any, Iterator


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_TRADEX_ROOT = REPO_ROOT / ".local" / "meemee" / "tradex"
DEFAULT_CONFIG_ROOT = REPO_ROOT / "config" / "tradex"


def resolve_tradex_root() -> Path:
    raw = os.getenv("MEEMEE_TRADEX_ROOT", "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return DEFAULT_TRADEX_ROOT.resolve()


def resolve_tradex_config_root() -> Path:
    raw = os.getenv("MEEMEE_TRADEX_CONFIG_ROOT", "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return DEFAULT_CONFIG_ROOT.resolve()


def tradex_families_root() -> Path:
    root = resolve_tradex_root() / "families"
    root.mkdir(parents=True, exist_ok=True)
    return root


def family_dir(family_id: str) -> Path:
    return tradex_families_root() / str(family_id)


def family_file(family_id: str) -> Path:
    return family_dir(family_id) / "family.json"


def baseline_lock_file(family_id: str) -> Path:
    return family_dir(family_id) / "baseline.lock.json"


def family_compare_file(family_id: str) -> Path:
    return family_dir(family_id) / "compare.json"


def runs_dir(family_id: str) -> Path:
    return family_dir(family_id) / "runs"


def run_dir(family_id: str, run_id: str) -> Path:
    return runs_dir(family_id) / str(run_id)


def run_file(family_id: str, run_id: str) -> Path:
    return run_dir(family_id, run_id) / "run.json"


def run_detail_file(family_id: str, run_id: str, code: str) -> Path:
    return run_dir(family_id, run_id) / "detail" / f"{str(code).strip()}.json"


def run_adopt_file(family_id: str, run_id: str) -> Path:
    return run_dir(family_id, run_id) / "adopt.json"


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def read_json(path: Path, default: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = _read_json(path)
    if payload is None:
        return dict(default or {})
    return payload


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True, default=str)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
        return path
    except Exception:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass
        raise


@contextlib.contextmanager
def acquire_lock(lock_path: Path, *, timeout_sec: float = 30.0, poll_sec: float = 0.1) -> Iterator[Path]:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    token = f"{os.getpid()}:{int(time.time())}"
    deadline = time.time() + max(0.1, float(timeout_sec))
    while True:
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            if time.time() >= deadline:
                raise TimeoutError(f"lock acquisition timed out: {lock_path}")
            time.sleep(max(0.01, float(poll_sec)))
            continue
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(token)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            yield lock_path
        finally:
            try:
                if lock_path.exists():
                    lock_path.unlink()
            except Exception:
                pass
        return


def write_json(path: Path, payload: dict[str, Any]) -> Path:
    return _atomic_write_json(path, payload)


def list_family_ids() -> list[str]:
    root = tradex_families_root()
    if not root.exists():
        return []
    out: list[str] = []
    for entry in root.iterdir():
        if not entry.is_dir():
            continue
        if (entry / "family.json").exists():
            out.append(entry.name)
    return sorted(out)


def family_lock_path(family_id: str) -> Path:
    return family_dir(family_id).parent / f".{family_id}.lock"


def find_family_id_by_run_id(run_id: str) -> str | None:
    target = str(run_id).strip()
    if not target:
        return None
    for family_id in list_family_ids():
        family = read_json(family_file(family_id))
        runs = family.get("run_ids") if isinstance(family.get("run_ids"), list) else []
        if target in {str(item) for item in runs}:
            return family_id
        baseline_run_id = str(family.get("baseline_run_id") or "").strip()
        if baseline_run_id == target:
            return family_id
        family_runs_dir = runs_dir(family_id)
        if family_runs_dir.exists():
            for entry in family_runs_dir.iterdir():
                if entry.is_dir() and (entry / "run.json").exists() and entry.name == target:
                    return family_id
    return None


def load_family(family_id: str) -> dict[str, Any] | None:
    return _read_json(family_file(family_id))


def load_run(family_id: str, run_id: str) -> dict[str, Any] | None:
    return _read_json(run_file(family_id, run_id))


def load_run_any(run_id: str) -> tuple[str | None, dict[str, Any] | None]:
    family_id = find_family_id_by_run_id(run_id)
    if not family_id:
        return None, None
    return family_id, load_run(family_id, run_id)
