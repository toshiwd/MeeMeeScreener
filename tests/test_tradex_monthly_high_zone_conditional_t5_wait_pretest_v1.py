from __future__ import annotations

import pandas as pd

from scripts.tradex_monthly_high_zone_conditional_t5_wait_pretest_v1 import decide, summarize


def test_summarize_uses_delay_only_for_high_zone_rows() -> None:
    rows = pd.DataFrame(
        [
            _row("A", True, -0.10, 0.05, True, False),
            _row("B", False, 0.10, -0.05, False, True),
        ]
    )
    rows["conditional_wait_applied"] = rows["monthly_high_zone_bool"]
    rows["conditional_ret20"] = rows["baseline_ret20_from_t"]
    rows.loc[rows["conditional_wait_applied"], "conditional_ret20"] = rows.loc[rows["conditional_wait_applied"], "delayed_ret20_from_t5"]
    rows["conditional_mae20"] = rows["baseline_mae20"]
    rows["conditional_mfe20"] = rows["baseline_mfe20"]
    rows["conditional_loser20"] = rows["conditional_ret20"] <= -0.05
    rows["conditional_winner20"] = rows["conditional_ret20"] >= 0.05
    rows["conditional_delta_vs_baseline"] = rows["conditional_ret20"] - rows["baseline_ret20_from_t"]
    rows["conditional_delta_vs_universal"] = rows["conditional_ret20"] - rows["delayed_ret20_from_t5"]
    rows["monthly_high_zone_missing"] = False

    summary, loser, winner, coverage, _stability, _comp = summarize(rows)

    assert abs(summary["2024"]["top5"]["conditional_high_zone_t5_wait"]["mean_ret20"] - 0.075) < 1e-12
    assert not loser.empty
    assert not winner.empty
    assert coverage.iloc[0]["monthly_high_zone_coverage"] == 1.0


def test_decide_inconclusive_when_high_zone_coverage_low() -> None:
    decision = decide({}, {"path_coverage": 1.0, "monthly_high_zone_coverage": 0.1})

    assert decision["research_decision"] == "inconclusive"


def _row(code: str, high_zone: bool, base_ret: float, delayed_ret: float, loser: bool, winner: bool) -> dict[str, object]:
    return {
        "decision_ymd": 20240105,
        "code": code,
        "year": 2024,
        "baseline_rank_recalc": 1,
        "path_available": True,
        "monthly_high_zone_bool": high_zone,
        "baseline_ret20_from_t": base_ret,
        "delayed_ret20_from_t5": delayed_ret,
        "delayed_loser20": delayed_ret <= -0.05,
        "delayed_winner20_abs": delayed_ret >= 0.05,
        "baseline_mae20": min(base_ret, 0),
        "delayed_mae20": min(delayed_ret, 0),
        "baseline_mfe20": max(base_ret, 0),
        "delayed_mfe20": max(delayed_ret, 0),
        "loser20": loser,
        "winner20": winner,
    }
