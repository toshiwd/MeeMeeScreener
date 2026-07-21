from __future__ import annotations

from pathlib import Path
from typing import Any


def _path_ready(path: str | None) -> bool:
    return bool(path and Path(path).expanduser().exists())


def probe_daily_research_prepared_environment(
    *,
    source_db_path: str | None = None,
    export_db_path: str | None = None,
    label_db_path: str | None = None,
) -> dict[str, Any]:
    checks = {
        "source_db": _path_ready(source_db_path),
        "export_db": _path_ready(export_db_path),
        "label_db": _path_ready(label_db_path),
    }
    missing = [name for name, ready in checks.items() if not ready]
    return {
        "prepared": not missing,
        "checks": checks,
        "missing": missing,
        "latest_trade_date": None,
    }
