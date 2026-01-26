from __future__ import annotations

"""
Thin wrapper around legacy trade CSV ingest so API/services do not depend on `app.backend`.
"""

from app.backend.import_positions import process_import_rakuten, process_import_sbi

__all__ = ["process_import_rakuten", "process_import_sbi"]

