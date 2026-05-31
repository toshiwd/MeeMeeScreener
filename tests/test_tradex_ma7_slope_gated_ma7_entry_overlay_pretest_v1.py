from __future__ import annotations

import pandas as pd

from scripts.tradex_ma7_slope_gated_ma7_entry_overlay_pretest_v1 import decide, summarize


def test_summarize_applies_ma7_overlay_only_to_gate_hits() -> None:
    rows = pd.DataFrame(
        [
            _row("A", True, -0.10, 0.05, True, False),
            _row("B", False, 0.10, -0.05, False, True),
        ]
    )

    summary, gate, no_trigger, loser, winner, _stability, _comparison = summarize(rows, {("2024", 5): 0.0, ("2024", 10): 0.0, ("2024", 20): 0.0}, {("2024", 5): 0.0, ("2024", 10): 0.0, ("2024", 20): 0.0})

    assert abs(summary["2024"]["top5"]["ma7_slope_gated_ma7_entry_overlay"]["mean_ret20"] - 0.075) < 1e-12
    assert gate.iloc[0]["gate_hit_count"] == 1
    assert no_trigger.iloc[0]["gated_no_trigger_count"] == 0
    assert loser.iloc[0]["gated_loser_count"] == 1
    assert winner.iloc[0]["gated_winner_count"] == 0


def test_decide_inconclusive_when_ma7_slope_coverage_low() -> None:
    decision = decide({}, {"ma7_slope_coverage": 0.5})

    assert decision["research_decision"] == "inconclusive"


def _row(code: str, gate: bool, base_ret: float, ma7_ret: float, loser: bool, winner: bool) -> dict[str, object]:
    return {
        "decision_ymd": 20240105,
        "code": code,
        "year": 2024,
        "baseline_rank_recalc": 1,
        "ma7_slope_gate_hit": gate,
        "ma7_triggered": True,
        "ma7_path_available": True,
        "baseline_ret20_from_t": base_ret,
        "delayed_ret20_from_t5": ma7_ret,
        "delayed_loser20": ma7_ret <= -0.05,
        "delayed_winner20_abs": ma7_ret >= 0.05,
        "baseline_mae20": min(base_ret, 0),
        "baseline_mfe20": max(base_ret, 0),
        "delayed_mae20": min(ma7_ret, 0),
        "delayed_mfe20": max(ma7_ret, 0),
        "ma7_ret20_including_no_entry_as_0": ma7_ret,
        "ma7_mae20": min(ma7_ret, 0),
        "ma7_mfe20": max(ma7_ret, 0),
        "gated_ret20": ma7_ret if gate else base_ret,
        "gated_mae20": min(ma7_ret if gate else base_ret, 0),
        "gated_mfe20": max(ma7_ret if gate else base_ret, 0),
        "gated_delta_vs_baseline": (ma7_ret if gate else base_ret) - base_ret,
        "gated_loser20": (ma7_ret if gate else base_ret) <= -0.05,
        "gated_winner20": (ma7_ret if gate else base_ret) >= 0.05,
        "gated_no_entry": False,
        "gated_no_trigger_classification": "not_gated_or_entered",
        "no_trigger_classification": "entered",
        "loser20": loser,
        "winner20": winner,
    }
