from __future__ import annotations

from scripts.tradex_negative_selection_avoidance_v1 import _flag_breakdown


def test_breakdown_after_failed_high_flag_requires_all_rule_blocks() -> None:
    row = {
        "context_available": True,
        "distance_from_20d_high_pct": -0.05,
        "runup_20d": 0.12,
        "close_vs_ma7_pct": -0.01,
        "close_vs_ma20_pct": 0.02,
        "ma7_slope_5d": 0.01,
        "drawdown_10d": -0.04,
        "drawdown_20d": -0.02,
        "close_position_in_range": 0.7,
    }

    assert _flag_breakdown(row)

    row["distance_from_20d_high_pct"] = -0.01
    assert not _flag_breakdown(row)
