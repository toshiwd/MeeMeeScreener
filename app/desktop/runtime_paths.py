from __future__ import annotations

from pathlib import Path
import os
import sys


def is_frozen() -> bool:
    return bool(getattr(sys, "frozen", False))


def base_path() -> Path:
    if is_frozen():
        return Path(getattr(sys, "_MEIPASS", Path(sys.executable).parent)).resolve()
    return Path(__file__).resolve().parents[2]


def resolve_path(*parts: str) -> str:
    return str(base_path().joinpath(*parts))


def local_app_dir(app_name: str = "MeeMeeScreener") -> Path:
    base = os.getenv("LOCALAPPDATA")
    if base:
        return Path(base) / app_name
    return Path.home() / ".local" / app_name
