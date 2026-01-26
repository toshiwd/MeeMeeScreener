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


def is_portable_mode() -> bool:
    """Check if portable mode is enabled by looking for portable.txt"""
    if is_frozen():
        # Check in the same folder as the executable
        exe_dir = Path(sys.executable).parent
        portable_marker = exe_dir / "portable.txt"
        return portable_marker.exists()
    else:
        # In development mode, check in the repo root
        portable_marker = base_path() / "portable.txt"
        return portable_marker.exists()


def local_app_dir(app_name: str = "MeeMeeScreener") -> Path:
    """
    Get the application data directory.
    
    In portable mode (when portable.txt exists):
        Returns: <exe_folder>/data
    
    In normal mode:
        Returns: %LOCALAPPDATA%/MeeMeeScreener
    """
    if is_portable_mode():
        if is_frozen():
            # Portable mode: use data folder next to executable
            return Path(sys.executable).parent / "data"
        else:
            # Development portable mode: use data folder in repo root
            return base_path() / "data"
    else:
        # Normal mode: use %LOCALAPPDATA%
        base = os.getenv("LOCALAPPDATA")
        if base:
            return Path(base) / app_name
        return Path.home() / ".local" / app_name
