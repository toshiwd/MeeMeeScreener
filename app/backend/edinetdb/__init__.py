from __future__ import annotations

from pathlib import Path

from external_analysis._pyc_importer import install_bytecode_finder

install_bytecode_finder("app.backend.edinetdb", Path(__file__).resolve().parent)
