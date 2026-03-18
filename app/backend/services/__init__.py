"""Backward-compatible lazy re-exports for app.backend.services modules.

This package keeps old import paths working while avoiding eager imports of
heavy analysis modules at package import time.
"""

from __future__ import annotations

import importlib
import sys as _sys
from pathlib import Path
from types import ModuleType

_PKG = __name__
_PKG_PATH = Path(__file__).resolve().parent

_MODULE_TARGETS = {
    "toredex_config": "app.backend.services.toredex.toredex_config",
    "toredex_execution": "app.backend.services.toredex.toredex_execution",
    "toredex_hash": "app.backend.services.toredex.toredex_hash",
    "toredex_models": "app.backend.services.toredex.toredex_models",
    "toredex_paths": "app.backend.services.toredex.toredex_paths",
    "toredex_policy": "app.backend.services.toredex.toredex_policy",
    "toredex_replay": "app.backend.services.toredex.toredex_replay",
    "toredex_repository": "app.backend.services.toredex.toredex_repository",
    "toredex_runner": "app.backend.services.toredex.toredex_runner",
    "toredex_self_improve": "app.backend.services.toredex.toredex_self_improve",
    "toredex_simulation_service": "app.backend.services.toredex.toredex_simulation_service",
    "toredex_snapshot_service": "app.backend.services.toredex.toredex_snapshot_service",
    "ml_config": "app.backend.services.ml.ml_config",
    "ml_service": "app.backend.services.ml.ml_service",
    "rankings_cache": "app.backend.services.ml.rankings_cache",
    "ranking_analysis_quality": "app.backend.services.ml.ranking_analysis_quality",
    "edinet_rank_features": "app.backend.services.ml.edinet_rank_features",
    "analysis_backfill_service": "app.backend.services.analysis.analysis_backfill_service",
    "analysis_decision": "app.backend.services.analysis.analysis_decision",
    "sell_analysis_accumulator": "app.backend.services.analysis.sell_analysis_accumulator",
    "strategy_backtest_service": "app.backend.services.analysis.strategy_backtest_service",
    "swing_expectancy_service": "app.backend.services.analysis.swing_expectancy_service",
    "swing_plan_service": "app.backend.services.analysis.swing_plan_service",
    "bar_aggregation": "app.backend.services.data.bar_aggregation",
    "events": "app.backend.services.data.events",
    "tdnet_mcp_import": "app.backend.services.data.tdnet_mcp_import",
    "txt_update": "app.backend.services.data.txt_update",
    "yahoo_daily_ingest": "app.backend.services.data.yahoo_daily_ingest",
    "yahoo_provisional": "app.backend.services.data.yahoo_provisional",
    "screener_snapshot_service": "app.backend.services.screener_snapshot_service",
    "static_assets": "app.backend.services.static_assets",
    "system_status": "app.backend.services.system_status",
    "watchlist": "app.backend.services.watchlist",
    "jpx_calendar": "app.backend.services.jpx_calendar",
    "legacy_analysis_control": "app.backend.services.legacy_analysis_control",
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

    def __dir__(self):
        module = self._load()
        return sorted(set(dir(type(self)) + list(module.__dict__.keys())))


def __getattr__(name: str) -> ModuleType:
    if name not in _MODULE_TARGETS:
        raise AttributeError(name)
    return _load_target(name)


for _name in __all__:
    _proxy = _LazyModule(_name)
    globals()[_name] = _proxy
    _sys.modules.setdefault(f"{_PKG}.{_name}", _proxy)

del _name, _proxy
