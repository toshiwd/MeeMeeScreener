from __future__ import annotations

import pandas as pd

from scripts.tradex_dist_ma20_conditional_t5_wait_pretest_v1 import decide, summarize


def test_summarize_uses_delay_only_for_high_dist_ma20_rows() -> None:
    rows = pd.DataFrame(
        [
            _row("A", True, -0.10, 0.05, True, False),
            _row("B", False, 0.10, -0.05, False, True),
        ]
    )

    summary, loser, winner, coverage, _stability, _comp = summarize(rows, {("2024", 5): 0.01, ("2024", 10): 0.01, ("2024", 20): 0.01})

    assert abs(summary["2024"]["top5"]["dist_ma20_top_quartile_conditional_t5_wait"]["mean_ret20"] - 0.075) < 1e-12
    assert not loser.empty
    assert not winner.empty
    assert coverage.iloc[0]["dist_ma20_pct_coverage"] == 1.0


def test_decide_inconclusive_when_dist_coverage_low() -> None:
    decision = decide({}, {"path_coverage": 1.0, "dist_ma20_pct_coverage": 0.1})

    assert decision["research_decision"] == "inconclusive"


def _row(code: str, wait: bool, base_ret: float, delayed_ret: float, loser: bool, winner: bool) -> dict[str, object]:
    return {
        "decision_ymd": 20240105,
        "code": code,
        "year": 2024,
        "baseline_rank_recalc": 1,
        "path_available": True,
        "dist_ma20_pct_num": 0.1 if wait else 0.0,
        "conditional_wait_applied": wait,
        "baseline_ret20_from_t": base_ret,
        "delayed_ret20_from_t5": delayed_ret,
        "delayed_loser20": delayed_ret <= -0.05,
        "delayed_winner20_abs": delayed_ret >= 0.05,
        "baseline_mae20": min(base_ret, 0),
        "conditional_mae20": min(delayed_ret if wait else base_ret, 0),
        "delayed_mae20": min(delayed_ret, 0),
        "baseline_mfe20": max(base_ret, 0),
        "conditional_mfe20": max(delayed_ret if wait else base_ret, 0),
        "delayed_mfe20": max(delayed_ret, 0),
        "conditional_ret20": delayed_ret if wait else base_ret,
        "conditional_delta_vs_baseline": (delayed_ret if wait else base_ret) - base_ret,
        "conditional_delta_vs_universal": (delayed_ret if wait else base_ret) - delayed_ret,
        "conditional_loser20": (delayed_ret if wait else base_ret) <= -0.05,
        "conditional_winner20": (delayed_ret if wait else base_ret) >= 0.05,
        "loser20": loser,
        "winner20": winner,
    }
