from __future__ import annotations

import os
from pathlib import Path


def resolve_analysis_db_path() -> str:
    try:
        from app.core.config import config

        return str(config.DB_PATH)
    except Exception:
        pass

    env_path = str(os.getenv("STOCKS_DB_PATH") or "").strip()
    if env_path:
        return str(Path(env_path).expanduser().resolve(strict=False))

    data_dir = str(os.getenv("MEEMEE_DATA_DIR") or "").strip()
    if data_dir:
        candidate = Path(data_dir).expanduser().resolve(strict=False) / "stocks.duckdb"
        return str(candidate)

    raise RuntimeError(
        "Legacy analysis DB path is not configured. Set STOCKS_DB_PATH or MEEMEE_DATA_DIR before running this script."
    )
