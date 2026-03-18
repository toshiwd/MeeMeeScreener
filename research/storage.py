from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
from typing import Any

import pandas as pd


_ASOF_FILE_RE = re.compile(r".*_(\d{4}-\d{2}-\d{2})\.csv$")
_DEFAULT_RESEARCH_HOME_NAME = "MeeMeeResearch"
_DEFAULT_MEEMEE_HOME_NAME = "MeeMeeScreener"


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_date(value: str) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts.normalize()


def ymd(value: pd.Timestamp | datetime | str) -> str:
    if isinstance(value, str):
        return parse_date(value).strftime("%Y-%m-%d")
    ts = pd.Timestamp(value)
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts.normalize().strftime("%Y-%m-%d")


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON must be object: {path}")
    return payload


def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    frame = pd.read_csv(path)
    for col in ("code", "sector33_code", "sector33_name", "snapshot_id", "study_id"):
        if col not in frame.columns:
            continue
        def _normalize(value: Any) -> Any:
            if pd.isna(value):
                return value
            if isinstance(value, float) and float(value).is_integer():
                return str(int(value))
            return str(value).strip()
        frame[col] = frame[col].map(_normalize)
    return frame


def write_csv(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False, encoding="utf-8")


def git_commit(repo_root: Path) -> str:
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=str(repo_root),
            stderr=subprocess.DEVNULL,
        )
        return out.decode("utf-8").strip()
    except Exception:
        return "unknown"


def _local_appdata_root() -> Path:
    return Path(os.getenv("LOCALAPPDATA") or str(Path.home())).expanduser().resolve()


def default_research_home() -> Path:
    raw = str(os.getenv("MEEMEE_RESEARCH_HOME") or "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return (_local_appdata_root() / _DEFAULT_RESEARCH_HOME_NAME).resolve()


def default_research_bridge_root(research_home: Path) -> Path:
    raw = str(os.getenv("MEEMEE_RESEARCH_BRIDGE_DIR") or "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return (research_home / "bridge").resolve()


def default_source_db_path() -> Path:
    for env_name in ("MEEMEE_SOURCE_DB", "STOCKS_DB_PATH"):
        raw = str(os.getenv(env_name) or "").strip()
        if raw:
            return Path(raw).expanduser().resolve()
    return (_local_appdata_root() / _DEFAULT_MEEMEE_HOME_NAME / "data" / "stocks.duckdb").resolve()


@dataclass(frozen=True)
class ResearchPaths:
    repo_root: Path
    research_home: Path
    workspace_root: Path
    published_root: Path

    @classmethod
    def build(
        cls,
        repo_root: Path | None = None,
        research_home: Path | None = None,
        workspace_root: Path | None = None,
        published_root: Path | None = None,
    ) -> "ResearchPaths":
        root = (repo_root or Path(__file__).resolve().parents[1]).resolve()
        resolved_home = (research_home or default_research_home()).resolve()
        workspace = (workspace_root or (resolved_home / "workspace")).resolve()
        published = (published_root or (resolved_home / "legacy_published")).resolve()
        paths = cls(repo_root=root, research_home=resolved_home, workspace_root=workspace, published_root=published)
        paths.ensure_base_dirs()
        return paths

    def ensure_base_dirs(self) -> None:
        self.research_home.mkdir(parents=True, exist_ok=True)
        self.workspace_root.mkdir(parents=True, exist_ok=True)
        self.snapshots_root.mkdir(parents=True, exist_ok=True)
        self.cache_root.mkdir(parents=True, exist_ok=True)
        self.runs_root.mkdir(parents=True, exist_ok=True)
        self.studies_root.mkdir(parents=True, exist_ok=True)
        self.evaluations_root.mkdir(parents=True, exist_ok=True)
        self.state_root.mkdir(parents=True, exist_ok=True)
        self.logs_root.mkdir(parents=True, exist_ok=True)
        self.mirror_root.mkdir(parents=True, exist_ok=True)
        self.bridge_root.mkdir(parents=True, exist_ok=True)
        self.published_root.mkdir(parents=True, exist_ok=True)

    @property
    def snapshots_root(self) -> Path:
        return self.workspace_root / "snapshots"

    @property
    def cache_root(self) -> Path:
        return self.workspace_root / "cache"

    @property
    def runs_root(self) -> Path:
        return self.workspace_root / "runs"

    @property
    def studies_root(self) -> Path:
        return self.workspace_root / "studies"

    @property
    def evaluations_root(self) -> Path:
        return self.workspace_root / "evaluations"

    @property
    def state_root(self) -> Path:
        return self.workspace_root / "state"

    @property
    def logs_root(self) -> Path:
        return self.research_home / "logs"

    @property
    def mirror_root(self) -> Path:
        return self.research_home / "mirror"

    @property
    def current_mirror_dir(self) -> Path:
        return self.mirror_root / "current"

    @property
    def current_mirror_db(self) -> Path:
        return self.current_mirror_dir / "source.duckdb"

    @property
    def current_mirror_manifest(self) -> Path:
        return self.current_mirror_dir / "mirror_manifest.json"

    @property
    def bridge_root(self) -> Path:
        return default_research_bridge_root(self.research_home)

    @property
    def bridge_latest_dir(self) -> Path:
        return self.bridge_root / "latest"

    @property
    def bridge_history_root(self) -> Path:
        return self.bridge_root / "history"

    def snapshot_dir(self, snapshot_id: str) -> Path:
        return self.snapshots_root / str(snapshot_id)

    def run_dir(self, run_id: str) -> Path:
        return self.runs_root / str(run_id)

    def study_dir(self, study_id: str) -> Path:
        return self.studies_root / str(study_id)

    def cache_dir(
        self,
        data_snapshot_id: str,
        feature_version: str,
        label_version: str,
        params_hash: str,
    ) -> Path:
        key = f"{data_snapshot_id}__{feature_version}__{label_version}__{params_hash}"
        return self.cache_root / key

    def next_study_id(self, snapshot_id: str | None = None) -> str:
        prefix = f"study_{snapshot_id}_" if snapshot_id else "study_"
        return datetime.now(timezone.utc).strftime(prefix + "%Y%m%d%H%M%S")

    @property
    def latest_snapshot_pointer(self) -> Path:
        return self.state_root / "latest_snapshot.txt"

    def get_latest_snapshot_id(self) -> str:
        if not self.latest_snapshot_pointer.exists():
            raise FileNotFoundError("latest snapshot pointer is missing")
        snapshot_id = self.latest_snapshot_pointer.read_text(encoding="utf-8").strip()
        if not snapshot_id:
            raise ValueError("latest snapshot pointer is empty")
        return snapshot_id

    def set_latest_snapshot_id(self, snapshot_id: str) -> None:
        self.latest_snapshot_pointer.parent.mkdir(parents=True, exist_ok=True)
        self.latest_snapshot_pointer.write_text(str(snapshot_id).strip(), encoding="utf-8")

    def next_publish_version_name(self) -> str:
        max_id = 0
        for child in self.published_root.glob("published_v*"):
            if not child.is_dir():
                continue
            suffix = child.name.replace("published_v", "").strip()
            if suffix.isdigit():
                max_id = max(max_id, int(suffix))
        return f"published_v{max_id + 1:03d}"

    @property
    def latest_published_dir(self) -> Path:
        return self.published_root / "latest"

    def replace_dir_atomically(self, source_dir: Path, target_dir: Path) -> None:
        if not source_dir.exists() or not source_dir.is_dir():
            raise FileNotFoundError(f"source publish dir missing: {source_dir}")
        target_dir.parent.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
        backup = target_dir.parent / f".{target_dir.name}_backup_{stamp}"
        if target_dir.exists():
            if backup.exists():
                shutil.rmtree(backup, ignore_errors=True)
            target_dir.replace(backup)
        source_dir.replace(target_dir)
        if backup.exists():
            shutil.rmtree(backup, ignore_errors=True)

    def replace_latest_atomically(self, source_dir: Path) -> None:
        self.replace_dir_atomically(source_dir, self.latest_published_dir)


def extract_asof_from_file(path: Path) -> str | None:
    match = _ASOF_FILE_RE.match(path.name)
    if not match:
        return None
    return match.group(1)


def ensure_clean_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True, exist_ok=True)
