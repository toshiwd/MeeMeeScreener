from __future__ import annotations

import importlib
import sys as _sys
from pathlib import Path
from types import ModuleType

_PKG = __name__
_PKG_PATH = Path(__file__).resolve().parent
_MODULE_TARGETS = {
    "ml_config": "app.backend.services.ml.ml_config",
    "ml_service": "app.backend.services.ml.ml_service",
    "edinet_rank_features": "app.backend.services.ml.edinet_rank_features",
    "ranking_analysis_quality": "app.backend.services.ml.ranking_analysis_quality",
    "rankings_cache": "app.backend.services.ml.rankings_cache",
}

__all__ = sorted(_MODULE_TARGETS.keys())


def _load_target(name: str) -> ModuleType:
    alias_key = f"{_PKG}.{name}"
    existing = _sys.modules.get(alias_key)
    if isinstance(existing, _LazyModule):
        _sys.modules.pop(alias_key, None)
    module_path = _PKG_PATH / f"{name}.py"
    if module_path.exists():
        module = importlib.import_module(alias_key)
    else:
        target = _MODULE_TARGETS[name]
        module = importlib.import_module(target)
    _sys.modules[alias_key] = module
    globals()[name] = module
    return module


class _LazyModule(ModuleType):
    def __init__(self, alias: str):
        super().__init__(f"{_PKG}.{alias}")
        self.__dict__["_alias"] = alias

    def _load(self) -> ModuleType:
        alias = self.__dict__["_alias"]
        module = _load_target(alias)
        self.__dict__.update(module.__dict__)
        return module

    def __getattr__(self, item):
        module = self._load()
        return getattr(module, item)

    def __setattr__(self, key, value):
        if key == "_alias":
            self.__dict__[key] = value
            return
        module = self._load()
        setattr(module, key, value)
        self.__dict__[key] = value

    def __delattr__(self, item):
        if item == "_alias":
            raise AttributeError(item)
        module = self._load()
        if hasattr(module, item):
            delattr(module, item)
        self.__dict__.pop(item, None)


def __getattr__(name: str) -> ModuleType:
    if name not in _MODULE_TARGETS:
        raise AttributeError(name)
    return _load_target(name)


for _name in __all__:
    _proxy = _LazyModule(_name)
    globals()[_name] = _proxy
    _sys.modules.setdefault(f"{_PKG}.{_name}", _proxy)

del _name, _proxy
