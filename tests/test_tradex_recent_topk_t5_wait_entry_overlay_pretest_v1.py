from __future__ import annotations

import pandas as pd

from scripts.tradex_recent_topk_t5_wait_entry_overlay_pretest_v1 import decide, summarize


def test_summarize_reports_loser_repair_and_winner_damage() -> None:
    rows = pd.DataFrame(
        [
            _row("A", -0.10, -0.02, True, False),
            _row("B", 0.10, 0.05, False, True),
            _row("C", 0.00, 0.01, False, False),
        ]
    )

    summary, loser, winner, _stability, _no_entry = summarize(rows)

    assert "2024" in summary
    assert loser.iloc[0]["delayed_improve_rate"] == 1.0
    assert winner.iloc[0]["delayed_harms_winner_rate"] == 1.0


def test_decide_inconclusive_when_path_coverage_low() -> None:
    decision = decide({}, 0.5)

    assert decision["research_decision"] == "inconclusive"


def _row(code: str, base_ret: float, delayed_ret: float, loser: bool, winner: bool) -> dict[str, object]:
    return {
        "decision_ymd": 20240105,
        "code": code,
        "year": 2024,
        "baseline_rank_recalc": 1,
        "path_available": True,
        "baseline_ret20_from_t": base_ret,
        "delayed_ret20_from_t5": delayed_ret,
        "delta_ret20": delayed_ret - base_ret,
        "baseline_mae20": min(base_ret, 0),
        "delayed_mae20": min(delayed_ret, 0),
        "baseline_mfe20": max(base_ret, 0),
        "delayed_mfe20": max(delayed_ret, 0),
        "loser20": loser,
        "winner20": winner,
        "delayed_loser20": delayed_ret <= -0.05,
        "delayed_winner20_abs": delayed_ret >= 0.05,
    }
