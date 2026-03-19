from __future__ import annotations

import importlib
import sys as _sys
from pathlib import Path
from types import ModuleType

_PKG = __name__
_PKG_PATH = Path(__file__).resolve().parent
_MODULE_TARGETS = {
    "analysis_adapter": "external_analysis.runtime.analysis_adapter",
    "daily_research": "external_analysis.runtime.daily_research",
    "decision_parts": "external_analysis.runtime.decision_parts",
    "historical_replay": "external_analysis.runtime.historical_replay",
    "incremental_cache": "external_analysis.runtime.incremental_cache",
    "input_normalization": "external_analysis.runtime.input_normalization",
    "load_control": "external_analysis.runtime.load_control",
    "nightly_pipeline": "external_analysis.runtime.nightly_pipeline",
    "nightly_similarity_challenger_pipeline": "external_analysis.runtime.nightly_similarity_challenger_pipeline",
    "nightly_similarity_pipeline": "external_analysis.runtime.nightly_similarity_pipeline",
    "orchestrator": "external_analysis.runtime.orchestrator",
    "output_assembler": "external_analysis.runtime.output_assembler",
    "promotion_decision": "external_analysis.runtime.promotion_decision",
    "review_build": "external_analysis.runtime.review_build",
    "review_summary": "external_analysis.runtime.review_summary",
    "rolling_comparison": "external_analysis.runtime.rolling_comparison",
    "score_context": "external_analysis.runtime.score_context",
    "score_axes": "external_analysis.runtime.score_axes",
    "score_aggregate": "external_analysis.runtime.score_aggregate",
    "score_finalize": "external_analysis.runtime.score_finalize",
    "source_snapshot": "external_analysis.runtime.source_snapshot",
}
_ATTR_TARGETS = {
    "build_candidate_comparison_payloads": ("decision_parts", "build_candidate_comparison_payloads"),
    "build_tradex_analysis_payload": ("analysis_adapter", "build_tradex_analysis_payload"),
    "build_tradex_score_reasons": ("score_finalize", "build_tradex_score_reasons"),
    "finalize_tradex_score_output": ("score_finalize", "finalize_tradex_score_output"),
    "normalize_candidate_comparison_payloads": ("decision_parts", "normalize_candidate_comparison_payloads"),
    "normalize_override_state_payload": ("decision_parts", "normalize_override_state_payload"),
    "normalize_publish_readiness_payload": ("decision_parts", "normalize_publish_readiness_payload"),
    "normalize_tradex_analysis_input": ("input_normalization", "normalize_tradex_analysis_input"),
    "prepare_tradex_score_context": ("score_context", "prepare_tradex_score_context"),
    "score_trend_direction": ("score_axes", "score_trend_direction"),
    "score_turning_momentum": ("score_axes", "score_turning_momentum"),
    "score_ev_upside_downside": ("score_axes", "score_ev_upside_downside"),
    "score_short_bias_penalty": ("score_axes", "score_short_bias_penalty"),
    "aggregate_tradex_score_decision": ("score_aggregate", "aggregate_tradex_score_decision"),
    "assemble_tradex_analysis_output": ("output_assembler", "assemble_tradex_analysis_output"),
    "run_tradex_analysis": ("orchestrator", "run_tradex_analysis"),
    "NormalizedTradexAnalysisInput": ("input_normalization", "NormalizedTradexAnalysisInput"),
}

__all__ = sorted(set(_MODULE_TARGETS) | set(_ATTR_TARGETS))


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


def __getattr__(name: str):
    if name in _MODULE_TARGETS:
        return _load_target(name)
    if name in _ATTR_TARGETS:
        module_name, attr_name = _ATTR_TARGETS[name]
        module = _load_target(module_name)
        value = getattr(module, attr_name)
        globals()[name] = value
        return value
    raise AttributeError(name)


for _name in _MODULE_TARGETS:
    _proxy = _LazyModule(_name)
    globals()[_name] = _proxy
    _sys.modules.setdefault(f"{_PKG}.{_name}", _proxy)

del _name, _proxy
