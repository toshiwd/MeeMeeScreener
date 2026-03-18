from __future__ import annotations

from app.backend.services.toredex_config import ToredexConfig, load_toredex_config
from app.backend.services.toredex_hash import hash_payload
from app.backend.services.toredex_paths import resolve_runs_root
from app.backend.services.toredex_policy import build_decision
from app.backend.services.toredex import toredex_snapshot_service
from app.backend.services.toredex_runner import _evaluate_risk_gate


def test_toredex_hash_is_deterministic_and_ignores_runtime_fields() -> None:
    left = {
        "asOf": "2025-01-10",
        "actions": [{"ticker": "1111", "deltaUnits": 2}],
        "createdAt": "2025-01-10T10:00:00Z",
    }
    right = {
        "createdAt": "2025-01-10T10:01:00Z",
        "actions": [{"ticker": "1111", "deltaUnits": 2}],
        "asOf": "2025-01-10",
    }
    assert hash_payload(left) == hash_payload(right)


def test_toredex_runs_root_priority(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("TOREDEX_RUNS_DIR", str(tmp_path / "env_runs"))
    monkeypatch.setenv("MEEMEE_DATA_DIR", str(tmp_path / "data"))

    from_cfg = resolve_runs_root(str(tmp_path / "cfg_runs"))
    assert from_cfg == (tmp_path / "cfg_runs").resolve()

    from_env = resolve_runs_root(None)
    assert from_env == (tmp_path / "env_runs").resolve()

    monkeypatch.delenv("TOREDEX_RUNS_DIR", raising=False)
    from_data = resolve_runs_root(None)
    assert from_data == (tmp_path / "data" / "runs").resolve()


def test_toredex_policy_decision_is_deterministic() -> None:
    config = ToredexConfig(
        data={
            "policyVersion": "toredex.v1",
            "mode": "LIVE",
            "topN": 50,
            "runsDir": None,
            "initialCash": 10_000_000,
            "maxPerTicker": 10_000_000,
            "maxHoldings": 3,
            "unitOptions": [2, 3, 5],
            "rankingMode": "hybrid",
            "sides": {"longEnabled": True, "shortEnabled": False},
            "thresholds": {
                "entryMinUpProb": 0.55,
                "entryMinEv": 0.0,
                "exitMinUpProb": 0.45,
                "exitMinEv": -0.01,
                "revRiskWarn": 0.55,
                "revRiskHigh": 0.65,
                "switchMinEvGap": 0.03,
                "cutLossWarnPct": -8.0,
                "cutLossHardPct": -10.0,
                "gameOverPct": -20.0,
                "takeProfitHintPct": 10.0,
            },
            "stageRules": {"goal20Pct": 20.0, "goal30Pct": 30.0},
        },
        config_hash="x",
    )
    snapshot = {
        "asOf": "2025-01-10",
        "seasonId": "s1",
        "policyVersion": "toredex.v1",
        "positions": [],
        "rankings": {
            "buy": [
                {
                    "ticker": "1111",
                    "ev": 0.1,
                    "upProb": 0.7,
                    "revRisk": 0.2,
                    "entryScore": 0.9,
                    "gate": {"ok": True, "reason": "ENTRY_OK"},
                }
            ],
            "sell": [],
        },
        "meta": {"noFutureLeakOk": True},
    }

    left = build_decision(snapshot=snapshot, config=config, prev_metrics=None)
    right = build_decision(snapshot=snapshot, config=config, prev_metrics=None)
    assert left == right
    assert left["actions"]
    assert left["actions"][0]["deltaUnits"] in {2, 3, 5}


def test_toredex_snapshot_exposes_timeframe_signals(monkeypatch) -> None:
    config = ToredexConfig(
        data={
            "policyVersion": "toredex.v1",
            "mode": "LIVE",
            "topN": 50,
            "runsDir": None,
            "initialCash": 10_000_000,
            "maxPerTicker": 10_000_000,
            "maxHoldings": 3,
            "unitOptions": [2, 3, 5],
            "rankingMode": "hybrid",
            "sides": {"longEnabled": True, "shortEnabled": False},
            "thresholds": {
                "entryMinUpProb": 0.55,
                "entryMinEv": 0.0,
                "exitMinUpProb": 0.45,
                "exitMinEv": -0.01,
                "revRiskWarn": 0.55,
                "revRiskHigh": 0.65,
                "switchMinEvGap": 0.03,
                "cutLossWarnPct": -8.0,
                "cutLossHardPct": -10.0,
                "gameOverPct": -20.0,
                "takeProfitHintPct": 10.0,
            },
            "stageRules": {"goal20Pct": 20.0, "goal30Pct": 30.0},
        },
        config_hash="x",
    )

    def fake_get_rankings_asof(*args, **_kwargs):
        direction = args[2]
        if direction == "up":
            return {
                "pred_dt": 20250110,
                "items": [
                    {
                        "code": "1111",
                        "asOf": "2025-01-10",
                        "mlEv20Net": 0.12,
                        "mlPUp": 0.53,
                        "weeklyBreakoutUpProb": 0.71,
                        "weeklyBreakoutDownProb": 0.18,
                        "weeklyRangeProb": 0.11,
                        "monthlyBreakoutUpProb": 0.68,
                        "monthlyBreakoutDownProb": 0.09,
                        "monthlyRangeProb": 0.22,
                        "monthlyBoxState": "tight",
                        "monthlyBoxPos": 0.28,
                        "monthlyBoxMonths": 5,
                        "monthlyBoxRangePct": 12.3,
                        "monthlyBoxWild": False,
                        "weeklyRegimeAligned": True,
                        "monthlyRegimeAligned": True,
                        "entryQualified": True,
                        "entryScore": 0.8,
                        "sector": "X",
                        "trendUp": True,
                        "trendDown": False,
                    }
                ],
            }
        return {"pred_dt": 20250110, "items": []}

    monkeypatch.setattr(toredex_snapshot_service.rankings_cache, "get_rankings_asof", fake_get_rankings_asof)
    snapshot = toredex_snapshot_service.build_snapshot(
        season_id="s1",
        as_of=_parse_date("2025-01-10"),
        config=config,
        positions=[],
    )

    item = snapshot["rankings"]["buy"][0]
    assert item["timeframeSignals"]["frameState"] == "BULLISH"
    assert item["timeframeSignals"]["weeklyBreakoutUpProb"] == 0.71
    assert item["timeframeSignals"]["monthlyBreakoutUpProb"] == 0.68


def test_toredex_policy_uses_timeframe_signals_for_entry_gate() -> None:
    config = ToredexConfig(
        data={
            "policyVersion": "toredex.v1",
            "mode": "LIVE",
            "topN": 50,
            "runsDir": None,
            "initialCash": 10_000_000,
            "maxPerTicker": 10_000_000,
            "maxHoldings": 3,
            "unitOptions": [2, 3, 5],
            "rankingMode": "hybrid",
            "sides": {"longEnabled": True, "shortEnabled": False},
            "thresholds": {
                "entryMinUpProb": 0.55,
                "entryMinEv": 0.0,
                "exitMinUpProb": 0.45,
                "exitMinEv": -0.01,
                "revRiskWarn": 0.55,
                "revRiskHigh": 0.65,
                "switchMinEvGap": 0.03,
                "cutLossWarnPct": -8.0,
                "cutLossHardPct": -10.0,
                "gameOverPct": -20.0,
                "takeProfitHintPct": 10.0,
            },
            "stageRules": {"goal20Pct": 20.0, "goal30Pct": 30.0},
        },
        config_hash="x",
    )
    snapshot = {
        "asOf": "2025-01-10",
        "seasonId": "s1",
        "policyVersion": "toredex.v1",
        "positions": [],
        "rankings": {
            "buy": [
                {
                    "ticker": "1111",
                    "ev": 0.05,
                    "upProb": 0.53,
                    "revRisk": 0.20,
                    "entryScore": 0.8,
                    "gate": {"ok": True, "reason": "ENTRY_OK"},
                    "timeframeSignals": {
                        "weeklyBreakoutUpProb": 0.71,
                        "weeklyBreakoutDownProb": 0.18,
                        "monthlyBreakoutUpProb": 0.68,
                        "monthlyBreakoutDownProb": 0.09,
                        "frameState": "BULLISH",
                    },
                }
            ],
            "sell": [],
        },
        "meta": {"noFutureLeakOk": True},
    }

    decision = build_decision(snapshot=snapshot, config=config, prev_metrics=None)
    assert decision["actions"]
    assert decision["actions"][0]["reasonId"] == "E_NEW_TOP1_GATE_OK"
    assert decision["signals"]["buy"][0]["frameSignals"]["frameState"] == "BULLISH"
    assert decision["signals"]["buy"][0]["upProb"] == 0.71


def test_toredex_policy_rebalances_exposure_after_risk_cut() -> None:
    config = ToredexConfig(
        data={
            "policyVersion": "toredex.v1",
            "mode": "LIVE",
            "topN": 50,
            "runsDir": None,
            "initialCash": 10_000_000,
            "maxPerTicker": 10_000_000,
            "maxHoldings": 3,
            "unitOptions": [2, 3, 5],
            "rankingMode": "hybrid",
            "sides": {"longEnabled": True, "shortEnabled": True},
            "thresholds": {
                "entryMinUpProb": 0.55,
                "entryMinEv": 0.0,
                "entryMaxRevRisk": 0.7,
                "addMinUpProb": 0.6,
                "addMinEv": 0.03,
                "addMaxRevRisk": 0.45,
                "addMinPnlPct": -1.0,
                "addMaxPnlPct": 12.0,
                "maxNewEntriesPerDay": 2.0,
                "newEntryMaxRank": 10.0,
                "exitMinUpProb": 0.5,
                "exitMinEv": -0.01,
                "revRiskWarn": 0.55,
                "revRiskHigh": 0.65,
                "switchMinEvGap": 0.05,
                "cutLossWarnPct": -5.5,
                "cutLossHardPct": -9.0,
                "gameOverPct": -20.0,
                "takeProfitHintPct": 10.0,
                "exitIfUnranked": 0.0,
                "exitGateNgMinHoldingDays": 10.0,
                "exitGateNgMinPnlPct": 0.0,
            },
            "stageRules": {"goal20Pct": 20.0, "goal30Pct": 30.0},
            "portfolioConstraints": {
                "grossUnitsCap": 10,
                "maxNetUnits": 2,
                "maxUnitsPerTicker": 2,
                "maxPerSector": 2,
                "minLiquidity20d": 0.0,
                "shortBlacklist": [],
            },
        },
        config_hash="x",
    )
    snapshot = {
        "asOf": "2003-06-11",
        "seasonId": "toredex_multiframe_20260318",
        "policyVersion": "toredex.v1",
        "positions": [
            {"ticker": "1001", "side": "SHORT", "units": 2, "avgPrice": 8375.0, "stage": "PROBE", "openedAt": "2003-05-29", "holdingDays": 9, "pnlPct": -6.0},
            {"ticker": "1306", "side": "LONG", "units": 2, "avgPrice": 835.0, "stage": "PROBE", "openedAt": "2003-05-28", "holdingDays": 10, "pnlPct": 13.0},
            {"ticker": "1308", "side": "LONG", "units": 2, "avgPrice": 879.0, "stage": "PROBE", "openedAt": "2003-06-06", "holdingDays": 3, "pnlPct": 13.0},
        ],
        "rankings": {
            "buy": [
                {
                    "ticker": "1306",
                    "ev": 0.1,
                    "upProb": 0.8,
                    "revRisk": 0.2,
                    "entryScore": 0.9,
                    "gate": {"ok": True, "reason": "ENTRY_OK"},
                    "liquidity20d": 100_000_000.0,
                },
                {
                    "ticker": "1308",
                    "ev": 0.09,
                    "upProb": 0.79,
                    "revRisk": 0.2,
                    "entryScore": 0.85,
                    "gate": {"ok": True, "reason": "ENTRY_OK"},
                    "liquidity20d": 100_000_000.0,
                },
            ],
            "sell": [
                {
                    "ticker": "1001",
                    "ev": 0.08,
                    "upProb": 0.78,
                    "revRisk": 0.2,
                    "entryScore": 0.88,
                    "gate": {"ok": True, "reason": "ENTRY_OK"},
                    "shortable": True,
                    "liquidity20d": 100_000_000.0,
                }
            ],
        },
        "meta": {"noFutureLeakOk": True},
    }

    decision = build_decision(snapshot=snapshot, config=config, prev_metrics={"equity": 10_253_235.073254}, mode="BACKTEST")
    assert decision["checks"]["exposureOk"] is True
    assert any(action["reasonId"] == "R_CUT_LOSS_WARN" for action in decision["actions"])
    assert any(action["reasonId"] == "R_EXPOSURE_TRIM" for action in decision["actions"])


def test_toredex_policy_keeps_profitable_position_on_early_gate_ng() -> None:
    config = ToredexConfig(
        data={
            "policyVersion": "toredex.v9-test",
            "mode": "LIVE",
            "topN": 50,
            "maxPerTicker": 10_000_000,
            "maxHoldings": 3,
            "unitOptions": [2, 3, 5],
            "sides": {"longEnabled": True, "shortEnabled": False},
            "thresholds": {
                "entryMinUpProb": 0.55,
                "entryMinEv": 0.0,
                "exitMinUpProb": 0.45,
                "exitMinEv": -0.01,
                "revRiskWarn": 0.55,
                "revRiskHigh": 0.65,
                "cutLossWarnPct": -8.0,
                "cutLossHardPct": -10.0,
                "takeProfitHintPct": 10.0,
                "exitIfUnranked": 1.0,
                "exitGateNgMinHoldingDays": 10.0,
                "exitGateNgMinPnlPct": 0.0,
            },
            "stageRules": {"goal20Pct": 20.0, "goal30Pct": 30.0},
        },
        config_hash="x",
    )
    snapshot = {
        "asOf": "2025-01-10",
        "seasonId": "s1",
        "policyVersion": "toredex.v9-test",
        "positions": [
            {
                "ticker": "1111",
                "side": "LONG",
                "units": 2,
                "avgPrice": 1000.0,
                "pnlPct": 3.0,
                "holdingDays": 4,
            }
        ],
        "rankings": {
            "buy": [
                {
                    "ticker": "1111",
                    "ev": 0.04,
                    "upProb": 0.58,
                    "revRisk": 0.40,
                    "entryScore": 0.8,
                    "gate": {"ok": False, "reason": "ENTRY_NG"},
                }
            ],
            "sell": [],
        },
        "meta": {"noFutureLeakOk": True},
    }
    decision = build_decision(snapshot=snapshot, config=config, prev_metrics=None)
    assert decision["actions"] == []


def test_toredex_policy_exits_gate_ng_after_min_holding_days() -> None:
    config = ToredexConfig(
        data={
            "policyVersion": "toredex.v9-test",
            "mode": "LIVE",
            "topN": 50,
            "maxPerTicker": 10_000_000,
            "maxHoldings": 3,
            "unitOptions": [2, 3, 5],
            "sides": {"longEnabled": True, "shortEnabled": False},
            "thresholds": {
                "entryMinUpProb": 0.55,
                "entryMinEv": 0.0,
                "exitMinUpProb": 0.45,
                "exitMinEv": -0.01,
                "revRiskWarn": 0.55,
                "revRiskHigh": 0.65,
                "cutLossWarnPct": -8.0,
                "cutLossHardPct": -10.0,
                "takeProfitHintPct": 10.0,
                "exitIfUnranked": 1.0,
                "exitGateNgMinHoldingDays": 10.0,
                "exitGateNgMinPnlPct": 0.0,
            },
            "stageRules": {"goal20Pct": 20.0, "goal30Pct": 30.0},
        },
        config_hash="x",
    )
    snapshot = {
        "asOf": "2025-01-10",
        "seasonId": "s1",
        "policyVersion": "toredex.v9-test",
        "positions": [
            {
                "ticker": "1111",
                "side": "LONG",
                "units": 2,
                "avgPrice": 1000.0,
                "pnlPct": 2.0,
                "holdingDays": 12,
            }
        ],
        "rankings": {
            "buy": [
                {
                    "ticker": "1111",
                    "ev": -0.05,
                    "upProb": 0.40,
                    "revRisk": 0.40,
                    "entryScore": 0.8,
                    "gate": {"ok": False, "reason": "ENTRY_NG"},
                }
            ],
            "sell": [],
        },
        "meta": {"noFutureLeakOk": True},
    }
    decision = build_decision(snapshot=snapshot, config=config, prev_metrics=None)
    assert len(decision["actions"]) == 1
    assert decision["actions"][0]["ticker"] == "1111"
    assert decision["actions"][0]["reasonId"] == "X_EXIT_GATE_NG"
    assert decision["actions"][0]["deltaUnits"] == -2


def test_toredex_policy_keeps_gate_ng_when_signal_still_strong_after_min_days() -> None:
    config = ToredexConfig(
        data={
            "policyVersion": "toredex.v9-test",
            "mode": "LIVE",
            "topN": 50,
            "maxPerTicker": 10_000_000,
            "maxHoldings": 3,
            "unitOptions": [2, 3, 5],
            "sides": {"longEnabled": True, "shortEnabled": False},
            "thresholds": {
                "entryMinUpProb": 0.55,
                "entryMinEv": 0.0,
                "exitMinUpProb": 0.45,
                "exitMinEv": -0.01,
                "revRiskWarn": 0.55,
                "revRiskHigh": 0.65,
                "cutLossWarnPct": -8.0,
                "cutLossHardPct": -10.0,
                "takeProfitHintPct": 10.0,
                "exitIfUnranked": 1.0,
                "exitGateNgMinHoldingDays": 10.0,
                "exitGateNgMinPnlPct": 0.0,
            },
            "stageRules": {"goal20Pct": 20.0, "goal30Pct": 30.0},
        },
        config_hash="x",
    )
    snapshot = {
        "asOf": "2025-01-10",
        "seasonId": "s1",
        "policyVersion": "toredex.v9-test",
        "positions": [
            {
                "ticker": "1111",
                "side": "LONG",
                "units": 2,
                "avgPrice": 1000.0,
                "pnlPct": 2.0,
                "holdingDays": 12,
            }
        ],
        "rankings": {
            "buy": [
                {
                    "ticker": "1111",
                    "ev": 0.04,
                    "upProb": 0.58,
                    "revRisk": 0.40,
                    "entryScore": 0.8,
                    "gate": {"ok": False, "reason": "ENTRY_NG"},
                }
            ],
            "sell": [],
        },
        "meta": {"noFutureLeakOk": True},
    }
    decision = build_decision(snapshot=snapshot, config=config, prev_metrics=None)
    assert decision["actions"] == []


def test_toredex_config_override_parses_cost_and_constraints() -> None:
    cfg = load_toredex_config(
        override={
            "operatingMode": "challenger",
            "costModel": {
                "feesBps": 10,
                "slippageBps": 2,
                "slippageLiquidityFactorBps": 3,
                "borrowShortBpsAnnual": 100,
                "sensitivityBps": [5, 10, 15],
            },
            "portfolioConstraints": {
                "grossUnitsCap": 10,
                "maxNetUnits": 2,
                "maxUnitsPerTicker": 2,
                "maxPerSector": 2,
                "minLiquidity20d": 100000000,
            },
        }
    )
    assert cfg.operating_mode == "challenger"
    assert cfg.cost_model["feesBps"] == 10.0
    assert cfg.cost_model["slippageBps"] == 2.0
    assert cfg.cost_model["borrowShortBpsAnnual"] == 100.0
    assert cfg.portfolio_constraints["maxUnitsPerTicker"] == 2
    assert cfg.portfolio_constraints["minLiquidity20d"] == 100000000.0


def test_toredex_policy_blocks_entry_when_net_exposure_limit_is_zero() -> None:
    config = ToredexConfig(
        data={
            "policyVersion": "toredex.v9-test",
            "mode": "LIVE",
            "operatingMode": "challenger",
            "topN": 50,
            "runsDir": None,
            "initialCash": 10_000_000,
            "maxPerTicker": 10_000_000,
            "maxHoldings": 5,
            "unitOptions": [2, 3, 5],
            "rankingMode": "ml",
            "sides": {"longEnabled": True, "shortEnabled": True},
            "thresholds": {
                "entryMinUpProb": 0.55,
                "entryMinEv": 0.0,
                "entryMaxRevRisk": 0.55,
                "exitMinUpProb": 0.45,
                "exitMinEv": -0.01,
                "revRiskWarn": 0.55,
                "revRiskHigh": 0.65,
                "switchMinEvGap": 0.03,
                "cutLossWarnPct": -8.0,
                "cutLossHardPct": -10.0,
                "gameOverPct": -20.0,
                "takeProfitHintPct": 10.0,
                "maxNewEntriesPerDay": 1.0,
                "newEntryMaxRank": 1.0,
            },
            "stageRules": {"goal20Pct": 20.0, "goal30Pct": 30.0},
            "portfolioConstraints": {
                "grossUnitsCap": 10,
                "maxNetUnits": 0,
                "maxUnitsPerTicker": 2,
                "maxPerSector": 2,
                "minLiquidity20d": 0.0,
                "shortBlacklist": [],
            },
        },
        config_hash="x",
    )
    snapshot = {
        "asOf": "2025-01-10",
        "seasonId": "s1",
        "policyVersion": "toredex.v9-test",
        "positions": [],
        "rankings": {
            "buy": [
                {
                    "ticker": "1111",
                    "ev": 0.1,
                    "upProb": 0.7,
                    "revRisk": 0.2,
                    "entryScore": 0.9,
                    "sector": "A",
                    "gate": {"ok": True, "reason": "ENTRY_OK"},
                }
            ],
            "sell": [
                {
                    "ticker": "2222",
                    "ev": 0.1,
                    "upProb": 0.7,
                    "revRisk": 0.2,
                    "entryScore": 0.9,
                    "sector": "B",
                    "shortable": True,
                    "gate": {"ok": True, "reason": "ENTRY_OK"},
                }
            ],
        },
        "meta": {"noFutureLeakOk": True},
    }
    decision = build_decision(snapshot=snapshot, config=config, prev_metrics=None)
    assert decision["actions"] == []


class _RiskGateRepoStub:
    def get_worst_month_return_pct(self, *_args, **_kwargs):
        return -9.0

    def get_max_turnover_pct_per_month(self, *_args, **_kwargs):
        return 200.0

    def get_max_abs_net_units(self, *_args, **_kwargs):
        return 1.0


def test_risk_gate_fails_when_worst_month_is_below_threshold() -> None:
    cfg = load_toredex_config(
        override={
            "operatingMode": "champion",
            "riskGates": {
                "champion": {
                    "enabled": True,
                    "maxDrawdownPct": -8.0,
                    "worstMonthPct": -8.0,
                    "maxTurnoverPctPerMonth": 300.0,
                    "maxNetExposureUnits": 2.0,
                }
            },
        }
    )
    passed, reason, details = _evaluate_risk_gate(
        repo=_RiskGateRepoStub(),
        cfg=cfg,
        season_id="s1",
        as_of_date=_parse_date("2025-01-10"),
        metric={"max_drawdown_pct": -5.0, "net_units": 1},
    )
    assert passed is False
    assert "WORST_MONTH" in reason
    assert details["pass"] is False


def _parse_date(text: str):
    import datetime as _dt
    return _dt.date.fromisoformat(text)
