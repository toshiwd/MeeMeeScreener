from __future__ import annotations

from pathlib import Path

from external_analysis._pyc_importer import install_bytecode_finder

install_bytecode_finder("external_analysis.event_image_dataset", Path(__file__).resolve().parent)
