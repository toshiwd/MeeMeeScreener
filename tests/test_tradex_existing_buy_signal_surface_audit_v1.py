from __future__ import annotations

import pandas as pd

from scripts import tradex_existing_buy_signal_surface_audit_v1 as mod


def _rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"as_of_date": 20260101, "code": "1001", "side": "buy", "entry_qualified": True, "setup_type": "breakout", "ret20": 0.1},
            {"as_of_date": 20260102, "code": "1002", "side": "buy", "entry_qualified": False, "setup_type": "reject", "ret20": -0.1},
        ]
    )


def test_group_metrics_includes_entry_qualified() -> None:
    metrics = mod.group_metrics(_rows())
    assert metrics["all_buy_side_rows"]["sample_count"] == 2
    assert metrics["entry_qualified_true"]["sample_count"] == 1
    assert "setup_type=breakout" in metrics


def test_buyability_gate_requires_support() -> None:
    gate = mod.buyability_gate(mod.group_metrics(_rows()))
    assert gate["any_buyability_gate_pass"] is False
    assert gate["thresholds"]["sample_count_min"] == 1000


def test_decide_blocks_empty_rows() -> None:
    decision, decision_class, reasons = mod.decide({"any_buyability_gate_pass": False}, pd.DataFrame())
    assert decision == "blocked_no_existing_buy_signal_rows"
    assert decision_class == "BLOCKED"
    assert reasons


def test_decide_keep_when_group_passes() -> None:
    decision, decision_class, reasons = mod.decide({"any_buyability_gate_pass": True}, _rows())
    assert decision == "existing_buy_signal_surface_keep_for_next_validation"
    assert decision_class == "KEEP"
    assert reasons
