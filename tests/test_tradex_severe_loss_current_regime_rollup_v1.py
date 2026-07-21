from scripts.tradex_severe_loss_current_regime_rollup_v1 import current_regime_gates


def test_current_regime_gates_require_all_fixed_thresholds():
    source = {
        "baseline_fixed_interleave": {"shadow": {
            "daily_profit_factor": 1.7, "calendar_expectancy": .005,
            "signals_per_week": 3.0, "cvar10": -.051, "max_drawdown_equal_weight": -.26,
        }},
        "challenger_severe_loss_classifier": {"shadow": {
            "daily_profit_factor": 2.8, "calendar_expectancy": .010,
            "signals_per_week": 3.0, "cvar10": -.049, "max_drawdown_equal_weight": -.13,
        }},
        "branching": {"summary": {"shadow": {"changed_day_rate": .84}}},
    }
    assert all(current_regime_gates(source).values())
    source["branching"]["summary"]["shadow"]["changed_day_rate"] = .19
    gates = current_regime_gates(source)
    assert not gates["branch_ge_20pct"]
    assert not all(gates.values())
