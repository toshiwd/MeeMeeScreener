from __future__ import annotations

import sys
from importlib import util
from pathlib import Path

_MODULE_PATH = Path(__file__).resolve().parent / "data" / "yahoo_provisional.py"
_SPEC = util.spec_from_file_location("app.backend.services.data.yahoo_provisional", _MODULE_PATH)
if _SPEC is None or _SPEC.loader is None:  # pragma: no cover - defensive compatibility guard
    raise ImportError(f"Unable to load Yahoo provisional implementation from {_MODULE_PATH}")
_impl = util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_impl)

sys.modules[__name__] = _impl
sys.modules["app.backend.services.data.yahoo_provisional"] = _impl

