from __future__ import annotations

import pandas as pd

from scripts.tradex_high_zone_reversal_confirmation_v1 import _decision, _tail_cases, choose_confirmation


def _row(**changes):
    values = {"c": 100.0, "h": 105.0}
    for i in range(1, 4):
        values.update({f"o{i}": 101.0, f"h{i}": 103.0, f"l{i}": 96.0, f"c{i}": 98.0})
    values.update(changes)
    return pd.Series(values)


def test_balanced_confirmation_enters_on_composite_weakness():
    result = choose_confirmation(_row(), "balanced_reversal_3d")
    assert result["state"] == "entry"
    assert result["entry_offset"] == 1


def test_balanced_confirmation_rejects_high_continuation():
    result = choose_confirmation(_row(h1=112.0), "balanced_reversal_3d")
    assert result["state"] != "entry"


def test_strict_confirmation_rejects_close_above_signal():
    result = choose_confirmation(_row(c1=101.0, o1=102.0, h1=103.0, l1=99.0), "strict_reversal_3d")
    assert result["entry_offset"] == 2


def test_decision_requires_absolute_tail_gates():
    h10 = {"mean": 0.03, "profit_factor": 1.4, "loss_le_minus10_rate": 0.12, "worst_mae": -0.40}
    candidate = {"entry_count": 40, "entry_rate": 0.4, "missed_drop_rate": 0.1, "h10": h10}
    baseline = {"h10": {"loss_le_minus10_rate": 0.20}}
    decision, checks = _decision(candidate, baseline)
    assert decision == "hold"
    assert checks["loss10_rate_at_most_10pct"] is False


def test_tail_cases_returns_only_large_losses():
    ledger = pd.DataFrame([
        {"policy": "p", "state": "entry", "ret10": -0.2, "mae10": -0.3, "code": "1", "signal_ymd": 20240101, "entry_offset": 1},
        {"policy": "p", "state": "entry", "ret10": 0.1, "mae10": -0.1, "code": "2", "signal_ymd": 20240102, "entry_offset": 1},
    ])
    assert [item["code"] for item in _tail_cases(ledger, "p")] == ["1"]
