from __future__ import annotations

import pandas as pd

from scripts.tradex_ma7_pullback_reclaim_entry_overlay_pretest_v1 import _find_ma7_trigger, decide, summarize


def test_find_ma7_trigger_requires_pullback_then_reclaim_when_above_ma7() -> None:
    frame = pd.DataFrame(
        {
            "close": [11, 12, 9, 10.5, 11, 11, 11, 11, 11, 11, 11],
            "ma7": [10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10],
        }
    )

    pos, reason = _find_ma7_trigger(frame, 0)

    assert pos == 3
    assert reason == "pullback_then_reclaim"


def test_find_ma7_trigger_from_below_waits_for_reclaim() -> None:
    frame = pd.DataFrame(
        {
            "close": [9, 9.5, 10.1, 10.5, 11, 11, 11, 11, 11, 11, 11],
            "ma7": [10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10],
        }
    )

    pos, reason = _find_ma7_trigger(frame, 0)

    assert pos == 2
    assert reason == "reclaim_from_below"


def test_summarize_counts_no_trigger_as_zero_for_primary_compare() -> None:
    rows = pd.DataFrame([_row("A", True, -0.10, 0.05, True, False), _row("B", False, 0.10, 0.0, False, True)])

    summary, trigger, loser, winner, no_trigger, _stability, _comparison = summarize(rows, {("2024", 5): 0.0, ("2024", 10): 0.0, ("2024", 20): 0.0})

    assert abs(summary["2024"]["top5"]["ma7_pullback_reclaim_no_entry_as_0"]["mean_ret20"] - 0.025) < 1e-12
    assert trigger.iloc[0]["trigger_count"] == 1
    assert loser.iloc[0]["skipped_loser_rate"] == 0.0
    assert winner.iloc[0]["no_trigger_missed_winner_rate"] == 1.0
    assert no_trigger.iloc[0]["missed_good_candidate_count"] == 1


def test_decide_inconclusive_when_trigger_rate_too_low() -> None:
    decision = decide({}, {"ma7_trigger_rate": 0.01, "ma7_entry_path_coverage_of_triggers": 1.0})

    assert decision["research_decision"] == "inconclusive"


def _row(code: str, triggered: bool, baseline_ret: float, ma7_ret: float, loser: bool, winner: bool) -> dict[str, object]:
    return {
        "decision_ymd": 20240105,
        "code": code,
        "year": 2024,
        "baseline_rank_recalc": 1,
        "ma7_triggered": triggered,
        "ma7_path_available": triggered,
        "baseline_ret20_from_t": baseline_ret,
        "delayed_ret20_from_t5": baseline_ret,
        "delayed_loser20": baseline_ret <= -0.05,
        "delayed_winner20_abs": baseline_ret >= 0.05,
        "baseline_mae20": min(baseline_ret, 0),
        "baseline_mfe20": max(baseline_ret, 0),
        "delayed_mae20": min(baseline_ret, 0),
        "delayed_mfe20": max(baseline_ret, 0),
        "ma7_ret20_from_entry": ma7_ret if triggered else None,
        "ma7_ret20_including_no_entry_as_0": ma7_ret if triggered else 0.0,
        "ma7_loser20": ma7_ret <= -0.05 if triggered else pd.NA,
        "ma7_winner20": ma7_ret >= 0.05 if triggered else pd.NA,
        "ma7_mae20": min(ma7_ret, 0) if triggered else None,
        "ma7_mfe20": max(ma7_ret, 0) if triggered else None,
        "ma7_delta_vs_baseline": (ma7_ret if triggered else 0.0) - baseline_ret,
        "loser20": loser,
        "winner20": winner,
        "no_trigger_classification": "entered" if triggered else ("skipped_bad_candidate" if loser else "missed_good_candidate" if winner else "neutral_no_entry"),
    }
