from __future__ import annotations

import importlib as _importlib
import sys as _sys
from types import ModuleType as _ModuleType

_MOVED_MODULES = {
    "analysis_backfill_service": "analysis.analysis_backfill_service",
    "analysis_decision": "analysis.analysis_decision",
    "bar_aggregation": "data.bar_aggregation",
    "edinet_rank_features": "ml.edinet_rank_features",
    "events": "data.events",
    "ml_config": "ml.ml_config",
    "ml_service": "ml.ml_service",
    "ranking_analysis_quality": "ml.ranking_analysis_quality",
    "rankings_cache": "ml.rankings_cache",
    "sell_analysis_accumulator": "analysis.sell_analysis_accumulator",
    "strategy_backtest_service": "analysis.strategy_backtest_service",
    "swing_expectancy_service": "analysis.swing_expectancy_service",
    "swing_plan_service": "analysis.swing_plan_service",
    "tdnet_mcp_import": "data.tdnet_mcp_import",
    "toredex_config": "toredex.toredex_config",
    "toredex_execution": "toredex.toredex_execution",
    "toredex_hash": "toredex.toredex_hash",
    "toredex_models": "toredex.toredex_models",
    "toredex_paths": "toredex.toredex_paths",
    "toredex_policy": "toredex.toredex_policy",
    "toredex_replay": "toredex.toredex_replay",
    "toredex_repository": "toredex.toredex_repository",
    "toredex_runner": "toredex.toredex_runner",
    "toredex_self_improve": "toredex.toredex_self_improve",
    "toredex_simulation_service": "toredex.toredex_simulation_service",
    "toredex_snapshot_service": "toredex.toredex_snapshot_service",
    "txt_update": "data.txt_update",
    "yahoo_daily_ingest": "data.yahoo_daily_ingest",
    "yahoo_provisional": "data.yahoo_provisional",
}
_ROOT_MODULES = ("jpx_calendar", "static_assets", "system_status", "watchlist")
_EXPORT_SOURCES = ("analysis", "data", "ml", "toredex", *_ROOT_MODULES)


class _MovedModuleAlias(_ModuleType):
    def __init__(self, alias_key: str, target_key: str, export_name: str) -> None:
        super().__init__(alias_key)
        self.__dict__["_alias_key"] = alias_key
        self.__dict__["_target_key"] = target_key
        self.__dict__["_export_name"] = export_name
        self.__dict__["_loaded_module"] = None

    def _load(self) -> _ModuleType:
        module = self.__dict__["_loaded_module"]
        if module is None:
            module = _importlib.import_module(self.__dict__["_target_key"])
            self.__dict__["_loaded_module"] = module
            self.__dict__.update(module.__dict__)
            globals()[self.__dict__["_export_name"]] = module
            _sys.modules[self.__dict__["_alias_key"]] = module
        return module

    def __getattr__(self, name: str):
        return getattr(self._load(), name)

    def __dir__(self) -> list[str]:
        return sorted(set(super().__dir__()) | set(dir(self._load())))


def _load_export_source(name: str) -> _ModuleType:
    return _importlib.import_module(f"{__name__}.{name}")


for _name, _target in _MOVED_MODULES.items():
    _alias_key = f"{__name__}.{_name}"
    _alias = _MovedModuleAlias(_alias_key, f"{__name__}.{_target}", _name)
    globals()[_name] = _alias
    _sys.modules.setdefault(_alias_key, _alias)


def __getattr__(name: str):
    if name in _ROOT_MODULES:
        module = _load_export_source(name)
        globals()[name] = module
        return module

    for source_name in _EXPORT_SOURCES:
        source = _load_export_source(source_name)
        if hasattr(source, name):
            value = getattr(source, name)
            globals()[name] = value
            return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    names = set(globals())
    for source_name in _EXPORT_SOURCES:
        try:
            names.update(dir(_load_export_source(source_name)))
        except Exception:
            continue
    return sorted(name for name in names if not name.startswith("_"))


del _name
del _target
del _alias
del _alias_key
