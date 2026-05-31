from __future__ import annotations

import pandas as pd

from scripts import tradex_current_buyable_invalidation_variant_compare_v1 as mod


def _rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "as_of_date": 20260520,
                "code": "1001",
                "entry_reference_close": 100.0,
                "ma20": 95.0,
                "recent_swing_low": 90.0,
                "atr14": 4.0,
                "ret20": 0.12,
                "ret5": 0.02,
                "period_bucket": "2026H1",
            }
        ]
    )


def _bars() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"code": "1001", "bar_date": 20260520, "close": 100.0},
            {"code": "1001", "bar_date": 20260521, "close": 93.0},
            {"code": "1001", "bar_date": 20260522, "close": 97.0},
        ]
    )


def test_add_variant_levels_creates_fixed_stop_levels() -> None:
    rows = mod.add_variant_levels(_rows())
    row = rows.iloc[0]
    assert row["stop_ma20"] == 95.0
    assert row["stop_recent_swing_low"] == 90.0
    assert row["stop_atr1"] == 96.0
    assert row["stop_atr1_5"] == 94.0
    assert row["stop_atr2"] == 92.0


def test_attach_hits_marks_hits_by_variant() -> None:
    rows = mod.attach_hits(_rows(), _bars())
    row = rows.iloc[0]
    assert bool(row["stop_ma20_hit_20d"]) is True
    assert bool(row["stop_atr1_hit_20d"]) is True
    assert bool(row["stop_atr2_hit_20d"]) is False


def test_metric_payload_reports_stopped_winner_rate() -> None:
    rows = mod.attach_hits(_rows(), _bars())
    metrics = mod.metric_payload(rows, "stop_atr1")
    assert metrics["sample_count"] == 1
    assert metrics["invalidation_hit_20d_rate"] == 1.0
    assert metrics["stopped_winner_rate"] == 1.0


def test_decide_keep_when_variant_exists() -> None:
    decision, decision_class, reasons = mod.decide("stop_atr2", {"no_lookahead_pass": True})
    assert decision == "invalidation_contract_variant_ready_for_forward_tracking"
    assert decision_class == "KEEP"
    assert "stop_atr2_passed_historical_stop_operability_gate" in reasons
