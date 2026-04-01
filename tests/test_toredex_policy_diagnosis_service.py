from __future__ import annotations

import json
from datetime import date

from app.backend.services.analysis import toredex_policy_diagnosis_service


def test_run_toredex_policy_diagnosis_selects_holdings_axis(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(tmp_path))
    monkeypatch.setattr(
        toredex_policy_diagnosis_service,
        "_default_period",
        lambda end_date=None: (date(2024, 1, 1), date(2024, 2, 29)),
    )
    monkeypatch.setattr(
        toredex_policy_diagnosis_service,
        "_latest_trade_date",
        lambda: date(2024, 2, 29),
    )

    class _Cfg:
        def __init__(self, config_hash: str):
            self.config_hash = config_hash

    def _fake_load_toredex_config(*, override=None, path=None):
        variant = "base_current"
        if isinstance(override, dict):
            if override.get("maxHoldings") == 1:
                variant = "holdings_1"
            elif override.get("maxHoldings") == 2:
                variant = "holdings_2"
            elif isinstance(override.get("thresholds"), dict):
                variant = "turnover_tight"
            elif isinstance(override.get("riskGates"), dict) and isinstance(override["riskGates"].get("champion"), dict) and override["riskGates"]["champion"].get("enabled") is False:
                variant = "gate_disabled_for_diagnosis"
        return _Cfg(config_hash=f"hash-{variant}")

    def _fake_run_backtest(*, season_id, start_date, end_date, dry_run=False, config_override=None):
        variant = "base_current"
        if isinstance(config_override, dict):
            if config_override.get("maxHoldings") == 1:
                variant = "holdings_1"
            elif config_override.get("maxHoldings") == 2:
                variant = "holdings_2"
            elif isinstance(config_override.get("thresholds"), dict):
                variant = "turnover_tight"
            elif isinstance(config_override.get("riskGates"), dict) and isinstance(config_override["riskGates"].get("champion"), dict) and config_override["riskGates"]["champion"].get("enabled") is False:
                variant = "gate_disabled_for_diagnosis"
        payloads = {
            "base_current": {
                "final_metrics": {"equity": 80.0, "net_cum_return_pct": -20.0, "max_drawdown_pct": -21.0},
                "rollup": {"worst_month_pct": -11.0, "max_turnover_pct_per_month": 200.0},
                "risk_gate": {"pass": False, "reason": "MAX_DD"},
                "reason_counts": {"R_CUT_LOSS_WARN": 8, "X_EXIT_GATE_NG": 2},
            },
            "holdings_1": {
                "final_metrics": {"equity": 96.0, "net_cum_return_pct": -4.0, "max_drawdown_pct": -6.0},
                "rollup": {"worst_month_pct": -3.0, "max_turnover_pct_per_month": 120.0},
                "risk_gate": {"pass": True, "reason": ""},
                "reason_counts": {"R_CUT_LOSS_WARN": 3, "X_EXIT_GATE_NG": 1},
            },
            "holdings_2": {
                "final_metrics": {"equity": 90.0, "net_cum_return_pct": -10.0, "max_drawdown_pct": -12.0},
                "rollup": {"worst_month_pct": -6.0, "max_turnover_pct_per_month": 150.0},
                "risk_gate": {"pass": False, "reason": "MAX_DD"},
                "reason_counts": {"R_CUT_LOSS_WARN": 5, "X_EXIT_GATE_NG": 1},
            },
            "turnover_tight": {
                "final_metrics": {"equity": 85.0, "net_cum_return_pct": -15.0, "max_drawdown_pct": -16.0},
                "rollup": {"worst_month_pct": -8.0, "max_turnover_pct_per_month": 90.0},
                "risk_gate": {"pass": False, "reason": "MAX_DD"},
                "reason_counts": {"R_CUT_LOSS_WARN": 6, "X_EXIT_GATE_NG": 1},
            },
            "gate_disabled_for_diagnosis": {
                "final_metrics": {"equity": 82.0, "net_cum_return_pct": -18.0, "max_drawdown_pct": -20.0},
                "rollup": {"worst_month_pct": -10.0, "max_turnover_pct_per_month": 205.0},
                "risk_gate": {"pass": True, "reason": ""},
                "reason_counts": {"R_CUT_LOSS_WARN": 8, "X_EXIT_GATE_NG": 1},
            },
        }
        row = payloads[variant]
        return {
            "season_id": season_id,
            "processed_days": 40,
            **row,
        }

    monkeypatch.setattr(toredex_policy_diagnosis_service.toredex_config, "load_toredex_config", _fake_load_toredex_config)
    monkeypatch.setattr(toredex_policy_diagnosis_service.toredex_runner, "run_backtest", _fake_run_backtest)
    monkeypatch.setattr(
        toredex_policy_diagnosis_service,
        "_query_holdings_count_max",
        lambda season_id: 1 if "holdings_1" in season_id else 2 if "holdings_2" in season_id else 3,
    )

    result = toredex_policy_diagnosis_service.run_toredex_policy_diagnosis()

    json_path = tmp_path / "reports" / "toredex_policy_diagnosis" / result["run_id"] / "toredex_policy_diagnosis.json"
    assert json_path.exists()
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert result["primary_failure_axis"] == "holdings"
    assert payload["primary_failure_axis"] == "holdings"
    assert len(payload["variant_results"]) == 5
