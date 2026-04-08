from __future__ import annotations

import os
from pathlib import Path

from app.core.config import config as core_config
from shared.tradex_storage import tradex_db_path


def resolve_result_db_path(db_path: str | None = None) -> Path:
    if db_path and str(db_path).strip():
        return Path(str(db_path)).expanduser().resolve()
    raw = str(os.getenv("MEEMEE_RESULT_DB_PATH") or "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return tradex_db_path("result.duckdb").resolve()


def resolve_source_db_path(db_path: str | None = None) -> Path:
    if db_path and str(db_path).strip():
        return Path(str(db_path)).expanduser().resolve()
    raw = str(os.getenv("MEEMEE_SOURCE_DB") or os.getenv("STOCKS_DB_PATH") or "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return tradex_db_path("stocks.duckdb").resolve()


def resolve_export_db_path(db_path: str | None = None) -> Path:
    if db_path and str(db_path).strip():
        return Path(str(db_path)).expanduser().resolve()
    return tradex_db_path("export.duckdb").resolve()


def resolve_label_db_path(db_path: str | None = None) -> Path:
    if db_path and str(db_path).strip():
        return Path(str(db_path)).expanduser().resolve()
    return tradex_db_path("label.duckdb").resolve()


def resolve_ops_db_path(db_path: str | None = None) -> Path:
    if db_path and str(db_path).strip():
        return Path(str(db_path)).expanduser().resolve()
    return tradex_db_path("ops.duckdb").resolve()


def resolve_similarity_db_path(db_path: str | None = None) -> Path:
    if db_path and str(db_path).strip():
        return Path(str(db_path)).expanduser().resolve()
    return tradex_db_path("similarity.duckdb").resolve()
