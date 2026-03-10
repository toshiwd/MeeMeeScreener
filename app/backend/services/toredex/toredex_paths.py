from __future__ import annotations

import os
from pathlib import Path


_REPO_ROOT = Path(__file__).resolve().parents[3]


def _to_abs_path(value: str | Path) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return (_REPO_ROOT / path).resolve()


def resolve_runs_root(config_runs_dir: str | None) -> Path:
    if config_runs_dir:
        return _to_abs_path(config_runs_dir)

    env_runs_dir = os.getenv("TOREDEX_RUNS_DIR", "").strip()
    if env_runs_dir:
        return _to_abs_path(env_runs_dir)

    data_dir = os.getenv("MEEMEE_DATA_DIR", "").strip()
    if data_dir:
        return Path(data_dir).resolve() / "runs"

    return (_REPO_ROOT / ".local" / "meemee" / "runs").resolve()


def logical_daily_dir(season_id: str, as_of_iso: str) -> str:
    return f"runs/{season_id}/daily/{as_of_iso}"


def real_daily_dir(runs_root: Path, season_id: str, as_of_iso: str) -> Path:
    return (runs_root / season_id / "daily" / as_of_iso).resolve()


def logical_monthly_dir(season_id: str) -> str:
    return f"runs/{season_id}/monthly"


def real_monthly_dir(runs_root: Path, season_id: str) -> Path:
    return (runs_root / season_id / "monthly").resolve()


def ensure_daily_paths(runs_root: Path, season_id: str, as_of_iso: str) -> dict[str, str]:
    real_dir = real_daily_dir(runs_root, season_id, as_of_iso)
    real_dir.mkdir(parents=True, exist_ok=True)
    logical_dir = logical_daily_dir(season_id, as_of_iso)
    return {
        "logical_dir": logical_dir,
        "real_dir": str(real_dir),
        "snapshot": str(real_dir / "snapshot.json"),
        "decision": str(real_dir / "decision.json"),
        "metrics": str(real_dir / "metrics.json"),
        "ledger_after": str(real_dir / "ledger_after.json"),
        "narrative": str(real_dir / "narrative.md"),
    }


def ensure_monthly_paths(runs_root: Path, season_id: str) -> dict[str, str]:
    real_dir = real_monthly_dir(runs_root, season_id)
    real_dir.mkdir(parents=True, exist_ok=True)
    logical_dir = logical_monthly_dir(season_id)
    return {
        "logical_dir": logical_dir,
        "real_dir": str(real_dir),
        "summary": str(real_dir / "summary.md"),
        "kpi": str(real_dir / "kpi.json"),
    }
