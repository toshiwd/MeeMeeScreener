from __future__ import annotations

import importlib


def test_external_analysis_main_import_smoke() -> None:
    module = importlib.import_module("external_analysis.__main__")
    assert callable(getattr(module, "main", None))
