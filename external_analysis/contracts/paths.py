from __future__ import annotations

from pathlib import Path

from app.core.config import config as core_config


def resolve_result_db_path(db_path: str | None = None) -> Path:
    if db_path and str(db_path).strip():
        return Path(str(db_path)).expanduser().resolve()
    return Path(core_config.RESULT_DB_PATH).expanduser().resolve()


def resolve_source_db_path(db_path: str | None = None) -> Path:
    if db_path and str(db_path).strip():
        return Path(str(db_path)).expanduser().resolve()
    return Path(core_config.DB_PATH).expanduser().resolve()


def resolve_export_db_path(db_path: str | None = None) -> Path:
    if db_path and str(db_path).strip():
        return Path(str(db_path)).expanduser().resolve()
    return (core_config.DATA_DIR / "external_analysis" / "export.duckdb").expanduser().resolve()


def resolve_label_db_path(db_path: str | None = None) -> Path:
    if db_path and str(db_path).strip():
        return Path(str(db_path)).expanduser().resolve()
    return (core_config.DATA_DIR / "external_analysis" / "label.duckdb").expanduser().resolve()


def resolve_ops_db_path(db_path: str | None = None) -> Path:
    if db_path and str(db_path).strip():
        return Path(str(db_path)).expanduser().resolve()
    return (core_config.DATA_DIR / "external_analysis" / "ops.duckdb").expanduser().resolve()


def resolve_similarity_db_path(db_path: str | None = None) -> Path:
    if db_path and str(db_path).strip():
        return Path(str(db_path)).expanduser().resolve()
    return (core_config.DATA_DIR / "external_analysis" / "similarity.duckdb").expanduser().resolve()
