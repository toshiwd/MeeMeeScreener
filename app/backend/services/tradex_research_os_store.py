from __future__ import annotations

import contextlib
import json
import os
import tempfile
import time
from pathlib import Path
from typing import Any, Iterator

from shared.tradex_storage import tradex_keep_path


class JsonReadError(ValueError):
    pass


class JsonFileMissingError(FileNotFoundError):
    pass


class JsonParseError(JsonReadError):
    pass


class JsonShapeError(JsonReadError):
    pass


def research_os_root() -> Path:
    root = tradex_keep_path("research_os")
    root.mkdir(parents=True, exist_ok=True)
    return root


def hypotheses_root() -> Path:
    root = research_os_root() / "hypotheses"
    root.mkdir(parents=True, exist_ok=True)
    return root


def hypothesis_file(hypothesis_id: str) -> Path:
    return hypotheses_root() / f"{str(hypothesis_id).strip()}.json"


def experiments_root() -> Path:
    root = research_os_root() / "experiments"
    root.mkdir(parents=True, exist_ok=True)
    return root


def experiment_dir(experiment_id: str) -> Path:
    path = experiments_root() / str(experiment_id).strip()
    path.mkdir(parents=True, exist_ok=True)
    return path


def experiment_manifest_file(experiment_id: str) -> Path:
    return experiment_dir(experiment_id) / "experiment_manifest.json"


def judge_input_file(experiment_id: str) -> Path:
    return experiment_dir(experiment_id) / "judge_input.json"


def judge_decision_file(experiment_id: str) -> Path:
    return experiment_dir(experiment_id) / "judge_decision.json"


def authoritative_decision_file(experiment_id: str) -> Path:
    return experiment_dir(experiment_id) / "authoritative_decision.json"


def observation_snapshot_file(experiment_id: str) -> Path:
    return experiment_dir(experiment_id) / "observation_snapshot.json"


def strategy_judgement_file(experiment_id: str) -> Path:
    return experiment_dir(experiment_id) / "strategy_judgement.json"


def teacher_evaluation_row_file(experiment_id: str) -> Path:
    return experiment_dir(experiment_id) / "teacher_evaluation_row.json"


def preflight_report_file(experiment_id: str) -> Path:
    return experiment_dir(experiment_id) / "preflight_report.json"


def memory_root() -> Path:
    root = research_os_root() / "memory"
    root.mkdir(parents=True, exist_ok=True)
    return root


def memory_file(hypothesis_id: str) -> Path:
    return memory_root() / f"{str(hypothesis_id).strip()}.json"


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def read_json(path: Path, default: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = _read_json(path)
    if payload is None:
        return dict(default or {})
    return payload if isinstance(payload, dict) else dict(default or {})


def read_json_object_strict(path: Path, *, artifact_name: str = "json file") -> dict[str, Any]:
    if not path.exists():
        raise JsonFileMissingError(f"{artifact_name} missing: {path}")
    try:
        raw = path.read_text(encoding="utf-8")
    except UnicodeDecodeError as exc:
        raise JsonParseError(f"{artifact_name} must be valid UTF-8: {path}") from exc
    except OSError as exc:
        raise JsonReadError(f"{artifact_name} unreadable: {path}") from exc
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise JsonParseError(f"{artifact_name} parse error at line {exc.lineno} column {exc.colno}: {path}") from exc
    if not isinstance(payload, dict):
        raise JsonShapeError(f"{artifact_name} root must be a JSON object: {path}")
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
