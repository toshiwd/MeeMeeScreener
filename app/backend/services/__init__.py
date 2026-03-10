"""Backward-compatible re-exports for app.backend.services sub-packages.

After reorganisation the service modules live under sub-packages
(toredex/, ml/, analysis/, data/) but callers still import from
``app.backend.services.<module>``.  This __init__ registers every moved
module in sys.modules so that the old import paths keep working.

Static imports are used (instead of importlib.import_module) so that
PyInstaller can trace all dependencies at build time.
"""
from __future__ import annotations

import sys as _sys

# ── sub-package imports (static, so PyInstaller can trace them) ──────
from app.backend.services.toredex import (  # noqa: F401
    toredex_config,
    toredex_execution,
    toredex_hash,
    toredex_models,
    toredex_paths,
    toredex_policy,
    toredex_replay,
    toredex_repository,
    toredex_runner,
    toredex_self_improve,
    toredex_simulation_service,
    toredex_snapshot_service,
)

from app.backend.services.ml import (  # noqa: F401
    ml_config,
    ml_service,
    rankings_cache,
    ranking_analysis_quality,
    edinet_rank_features,
)

from app.backend.services.analysis import (  # noqa: F401
    analysis_backfill_service,
    analysis_decision,
    sell_analysis_accumulator,
    strategy_backtest_service,
    swing_expectancy_service,
    swing_plan_service,
)

from app.backend.services.data import (  # noqa: F401
    bar_aggregation,
    events,
    tdnet_mcp_import,
    txt_update,
    yahoo_daily_ingest,
    yahoo_provisional,
)

# ── register old-style module paths in sys.modules ───────────────────
_PKG = __name__  # "app.backend.services"

_MOVED = {
    "toredex_config": toredex_config,
    "toredex_execution": toredex_execution,
    "toredex_hash": toredex_hash,
    "toredex_models": toredex_models,
    "toredex_paths": toredex_paths,
    "toredex_policy": toredex_policy,
    "toredex_replay": toredex_replay,
    "toredex_repository": toredex_repository,
    "toredex_runner": toredex_runner,
    "toredex_self_improve": toredex_self_improve,
    "toredex_simulation_service": toredex_simulation_service,
    "toredex_snapshot_service": toredex_snapshot_service,
    "ml_config": ml_config,
    "ml_service": ml_service,
    "rankings_cache": rankings_cache,
    "ranking_analysis_quality": ranking_analysis_quality,
    "edinet_rank_features": edinet_rank_features,
    "analysis_backfill_service": analysis_backfill_service,
    "analysis_decision": analysis_decision,
    "sell_analysis_accumulator": sell_analysis_accumulator,
    "strategy_backtest_service": strategy_backtest_service,
    "swing_expectancy_service": swing_expectancy_service,
    "swing_plan_service": swing_plan_service,
    "bar_aggregation": bar_aggregation,
    "events": events,
    "tdnet_mcp_import": tdnet_mcp_import,
    "txt_update": txt_update,
    "yahoo_daily_ingest": yahoo_daily_ingest,
    "yahoo_provisional": yahoo_provisional,
}

for _name, _mod in _MOVED.items():
    _sys.modules.setdefault(f"{_PKG}.{_name}", _mod)

del _name, _mod
