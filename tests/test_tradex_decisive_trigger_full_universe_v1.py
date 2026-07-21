from __future__ import annotations

import pandas as pd

from scripts import tradex_decisive_trigger_full_universe_v1 as mod


def test_future_extreme_excludes_signal_bar_and_requires_full_window() -> None:
    values = pd.Series(range(1, 26), dtype=float)
    future_max = mod._future_extreme(values, 20, "max")
    future_min = mod._future_extreme(values, 20, "min")
    assert future_max.iloc[0] == 21
    assert future_min.iloc[0] == 2
    assert pd.isna(future_max.iloc[5])


def test_summary_and_decision_use_completed_outcomes_only() -> None:
    rows = []
    for index in range(110):
        rows.append(
            {
                "code": f"{1000 + index}",
                "outcome_complete20": index < 100,
                "directional_ret5": 0.01,
                "directional_ret10": 0.02,
                "directional_ret20": 0.06 if index < 100 else None,
                "directional_mfe20": 0.10,
                "directional_adverse20": -0.03,
            }
        )
    summary = mod.summarize(pd.DataFrame(rows))
    decision, gates = mod.decision_for(summary)
    assert summary["event_count"] == 110
    assert summary["complete20_count"] == 100
    assert summary["incomplete_recent_count"] == 10
    assert summary["directional_ret20_trim5_mean_pct"] == 6.0
    assert summary["directional_ret20_symbol_equal_mean_pct"] == 6.0
    assert decision == "keep_review_only"
    assert all(gates.values())


def test_tiny_group_trimmed_mean_is_null_not_nan() -> None:
    rows = pd.DataFrame(
        [
            {"code": "1", "outcome_complete20": True, "directional_ret5": 0.1, "directional_ret10": 0.1, "directional_ret20": -0.5, "directional_mfe20": 0.1, "directional_adverse20": -0.1},
            {"code": "2", "outcome_complete20": True, "directional_ret5": 0.1, "directional_ret10": 0.1, "directional_ret20": 0.5, "directional_mfe20": 0.1, "directional_adverse20": -0.1},
        ]
    )
    assert mod.summarize(rows)["directional_ret20_trim5_mean_pct"] is None
