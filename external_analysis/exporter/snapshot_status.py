from __future__ import annotations

from pathlib import Path

from external_analysis.exporter.diff_export import (
    _resolve_snapshot_progress_path,
    run_diff_export,
)


def resolve_snapshot_progress_path(export_db_path: str | Path) -> Path:
    return _resolve_snapshot_progress_path(export_db_path)


def build_export_snapshot(source_db_path: str | None = None, export_db_path: str | None = None) -> dict:
    return run_diff_export(source_db_path=source_db_path, export_db_path=export_db_path)
