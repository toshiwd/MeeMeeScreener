from __future__ import annotations

import pandas as pd

from scripts import tradex_position_management_policy_pretest_v1 as mod


def _bars(closes: list[float]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "code": "1001",
                "bar_date": 20250101 + idx,
                "open": close,
                "high": close + 1,
                "low": close - 1,
                "close": close,
                "volume": 1000,
            }
            for idx, close in enumerate(closes)
        ]
    )


def _entry() -> pd.Series:
    return pd.Series(
        {
            "event_id": "event_1",
            "as_of_date": 20250120,
            "code": "1001",
            "entry_reference_close": 100.0,
            "ma20": 95.0,
            "atr14": 2.0,
            "recent_swing_low": 90.0,
        }
    )


def test_build_bar_features_adds_ma_and_atr_columns() -> None:
    features = mod.build_bar_features(_bars([100 + i for i in range(40)]))
    assert pd.notna(features.iloc[-1]["ma7"])
    assert pd.notna(features.iloc[-1]["ma20"])
    assert pd.notna(features.iloc[-1]["atr14"])


def test_policy_a_exits_on_invalidation_or_ma20() -> None:
    window = pd.DataFrame(
        [
            {"code": "1001", "bar_date": 20250120, "close": 100.0, "ma7": 99.0, "ma20": 95.0},
            {"code": "1001", "bar_date": 20250121, "close": 94.0, "ma7": 98.0, "ma20": 96.0},
        ]
    )
    trade, ledger = mod.simulate_event(_entry(), window, "policy_a_loss_control")
    assert trade["exit_day"] == 1
    assert trade["exit_reason"] == "close_below_invalidation_or_ma20"
    assert ledger[-1]["action"] == "exit"


def test_policy_b_adds_only_after_confirmation() -> None:
    window = pd.DataFrame(
        [
            {"code": "1001", "bar_date": 20250120, "close": 100.0, "ma7": 99.0, "ma20": 95.0},
            {"code": "1001", "bar_date": 20250121, "close": 102.0, "ma7": 100.0, "ma20": 96.0},
            {"code": "1001", "bar_date": 20250122, "close": 104.0, "ma7": 101.0, "ma20": 97.0},
        ]
    )
    trade, ledger = mod.simulate_event(_entry(), window, "policy_b_confirmation_add")
    assert any(row["action"] == "add_2_units" for row in ledger)
    assert trade["final_units"] == 3.0


def test_aggregate_metrics_reports_bad_and_profit_factor() -> None:
    trades = pd.DataFrame(
        [
            {"policy": "x", "as_of_date": 1, "code": "a", "cost_adjusted_return": 0.12, "max_drawdown": -0.02, "holding_days": 20, "average_gross_exposure": 1, "turnover": 1, "return_per_unit_exposure": 0.12, "initial_unit_return": 0.12, "add_unit_return": 0, "hedge_return": 0},
            {"policy": "x", "as_of_date": 2, "code": "b", "cost_adjusted_return": -0.06, "max_drawdown": -0.08, "holding_days": 8, "average_gross_exposure": 1, "turnover": 1, "return_per_unit_exposure": -0.06, "initial_unit_return": -0.06, "add_unit_return": 0, "hedge_return": 0},
        ]
    )
    metrics = mod.aggregate_metrics(trades)
    assert metrics["trade_count"] == 2
    assert metrics["bad_rate_lt_minus_5pct"] == 0.5
    assert metrics["profit_factor"] == 2.0


def test_decide_keeps_policy_when_return_and_risk_improve() -> None:
    comparison = {
        "metrics_by_policy": {
            "baseline_hold20": {"cost_adjusted_return": 0.01, "bad_rate_lt_minus_5pct": 0.2, "severe_rate_lt_minus_10pct": 0.1, "average_max_drawdown": -0.08, "return_per_unit_exposure": 0.01},
            "policy_a_loss_control": {"trade_count": 120, "date_count": 80, "cost_adjusted_return": 0.03, "bad_rate_lt_minus_5pct": 0.1, "severe_rate_lt_minus_10pct": 0.05, "average_max_drawdown": -0.04, "average_gross_exposure": 1.0, "return_per_unit_exposure": 0.03},
            "policy_b_confirmation_add": {"trade_count": 120, "date_count": 80, "cost_adjusted_return": 0.0, "bad_rate_lt_minus_5pct": 0.2, "severe_rate_lt_minus_10pct": 0.1, "average_max_drawdown": -0.08, "average_gross_exposure": 1.0, "return_per_unit_exposure": 0.0},
            "policy_c_hedged_scale": {"trade_count": 120, "date_count": 80, "cost_adjusted_return": 0.0, "bad_rate_lt_minus_5pct": 0.2, "severe_rate_lt_minus_10pct": 0.1, "average_max_drawdown": -0.08, "average_gross_exposure": 1.0, "return_per_unit_exposure": 0.0},
        },
        "policy_vs_baseline_hold20": {
            "policy_a_loss_control": {"delta_cost_adjusted_return": 0.02, "delta_average_max_drawdown": 0.04, "delta_return_per_unit_exposure": 0.02},
            "policy_b_confirmation_add": {"delta_cost_adjusted_return": -0.01, "delta_average_max_drawdown": 0.0, "delta_return_per_unit_exposure": -0.01},
            "policy_c_hedged_scale": {"delta_cost_adjusted_return": -0.01, "delta_average_max_drawdown": 0.0, "delta_return_per_unit_exposure": -0.01},
        },
    }
    decision, decision_class, _, best = mod.decide(comparison, {"replayable_event_count": 120}, {"no_lookahead_pass": True})
    assert decision == "position_policy_keep_for_portfolio_replay"
    assert decision_class == "KEEP"
    assert best == "policy_a_loss_control"
