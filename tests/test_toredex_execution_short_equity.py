from __future__ import annotations

from datetime import date
from typing import Any

from app.backend.services.toredex_config import ToredexConfig
from app.backend.services.toredex_execution import execute_live_decision


class _RepoStub:
    def __init__(self) -> None:
        self._positions: list[dict[str, Any]] = []
        self._metrics: list[dict[str, Any]] = []
        self._season = {"initial_cash": 10_000_000}
        self.trades: list[dict[str, Any]] = []

    def get_positions(self, season_id: str) -> list[dict[str, Any]]:
        _ = season_id
        return [dict(p) for p in self._positions]

    def get_season(self, season_id: str) -> dict[str, Any] | None:
        _ = season_id
        return dict(self._season)

    def get_latest_metrics(self, season_id: str, *, before_or_equal: date | None = None) -> dict[str, Any] | None:
        _ = season_id
        rows = self._metrics
        if before_or_equal is not None:
            rows = [m for m in self._metrics if m.get("asOf") is not None and m["asOf"] <= before_or_equal]
        if not rows:
            return None
        return dict(rows[-1])

    def get_close_map(self, *, as_of: date, tickers: list[str]) -> dict[str, float]:
        _ = (as_of, tickers)
        return {}

    def save_trades(self, trades: list[dict[str, Any]]) -> None:
        self.trades.extend(trades)

    def replace_positions(self, season_id: str, positions: list[dict[str, Any]]) -> None:
        _ = season_id
        self._positions = [dict(p) for p in positions]

    def save_daily_metrics(self, metric: dict[str, Any]) -> None:
        self._metrics.append(dict(metric))


def _config() -> ToredexConfig:
    return ToredexConfig(
        data={
            "policyVersion": "toredex.v8-test",
            "mode": "LIVE",
            "operatingMode": "champion",
            "initialCash": 10_000_000,
            "maxPerTicker": 10_000_000,
            "maxHoldings": 3,
            "unitOptions": [2, 3, 5],
            "sides": {"longEnabled": True, "shortEnabled": True},
            "thresholds": {
                "goal20Pct": 20.0,
                "goal30Pct": 30.0,
                "gameOverPct": -20.0,
            },
            "stageRules": {"goal20Pct": 20.0, "goal30Pct": 30.0},
            "costModel": {
                "feesBps": 0.0,
                "slippageBps": 0.0,
                "slippageLiquidityFactorBps": 0.0,
                "borrowShortBpsAnnual": 0.0,
                "sensitivityBps": [],
            },
            "portfolioConstraints": {
                "grossUnitsCap": 10,
                "maxNetUnits": 2,
                "maxUnitsPerTicker": 2,
                "maxPerSector": 2,
                "minLiquidity20d": 0.0,
                "shortBlacklist": [],
            },
        },
        config_hash="test",
    )


def test_short_open_does_not_artificially_inflate_equity_or_drawdown() -> None:
    repo = _RepoStub()
    config = _config()
    season_id = "test_short_equity"

    snapshot_open = {
        "rankings": {
            "buy": [],
            "sell": [{"ticker": "9984", "close": 100.0}],
        }
    }
    decision_open = {
        "actions": [
            {"ticker": "9984", "side": "SHORT", "deltaUnits": 2, "reasonId": "E_NEW_TOP1_GATE_OK"}
        ]
    }

    open_result = execute_live_decision(
        repo=repo,
        season_id=season_id,
        as_of=date(2026, 2, 18),
        snapshot=snapshot_open,
        decision=decision_open,
        config=config,
    )
    open_metric = open_result["metrics"]
    assert open_metric["equity"] == 10_000_000.0
    assert open_metric["max_drawdown_pct"] == 0.0
    assert open_metric["short_units"] == 2

    snapshot_close = {
        "rankings": {
            "buy": [],
            "sell": [{"ticker": "9984", "close": 101.0}],
        }
    }
    decision_close = {
        "actions": [
            {"ticker": "9984", "side": "SHORT", "deltaUnits": -2, "reasonId": "X_EXIT_EV_DROP"}
        ]
    }

    close_result = execute_live_decision(
        repo=repo,
        season_id=season_id,
        as_of=date(2026, 2, 19),
        snapshot=snapshot_close,
        decision=decision_close,
        config=config,
    )
    close_metric = close_result["metrics"]
    assert close_metric["equity"] == 9_980_000.0
    assert -1.0 < float(close_metric["max_drawdown_pct"]) < 0.0
    assert close_metric["short_units"] == 0
