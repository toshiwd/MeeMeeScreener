from __future__ import annotations

import pandas as pd

from scripts.tradex_ma7_no_trigger_decomposition_v1 import decide, fallback_candidates, no_trigger_outcome_summary


def test_no_trigger_outcome_summary_splits_missed_skipped_neutral() -> None:
    rows = pd.DataFrame(
        [
            _row(2024, 1, "missed_good_candidate", 0.10),
            _row(2024, 2, "skipped_bad_candidate", -0.10),
            _row(2024, 3, "neutral_no_entry", 0.01),
            _row(2024, 4, "entered", 0.02, triggered=True),
        ]
    )

    summary = no_trigger_outcome_summary(rows)
    top5 = summary[(summary["year"] == 2024) & (summary["topk"] == 5)].iloc[0]

    assert top5["no_trigger_count"] == 3
    assert top5["missed_good_count"] == 1
    assert top5["skipped_bad_count"] == 1
    assert top5["neutral_count"] == 1


def test_fallback_candidates_selects_stable_effect_axis() -> None:
    decomp = pd.DataFrame(
        [
            {"year": 2024, "topk": 10, "feature": "ma7_slope", "effect_size": 0.4, "missed_good_n": 100},
            {"year": 2025, "topk": 10, "feature": "ma7_slope", "effect_size": 0.3, "missed_good_n": 100},
            {"year": 2026, "topk": 10, "feature": "ma7_slope", "effect_size": 0.2, "missed_good_n": 50},
        ]
    )

    candidates = fallback_candidates(decomp, pd.DataFrame())

    assert candidates[0]["recommended_next"] == "pretest"
    assert "ma7_slope" in candidates[0]["axis_name"]


def test_decide_drops_when_missed_good_dominates_without_axis() -> None:
    outcome = pd.DataFrame(
        [
            {"topk": 10, "missed_good_rate": 0.5, "skipped_bad_rate": 0.3},
            {"topk": 10, "missed_good_rate": 0.6, "skipped_bad_rate": 0.2},
        ]
    )

    decision = decide(outcome, [], {"largest_year_share": 0.5, "largest_code_share": 0.01})

    assert decision["research_decision"] == "drop_ma7_event_overlay"


def _row(year: int, rank: int, cls: str, ret20: float, triggered: bool = False) -> dict[str, object]:
    return {
        "year": year,
        "baseline_rank_recalc": rank,
        "ma7_triggered": triggered,
        "no_trigger_classification": cls,
        "baseline_ret20_from_t": ret20,
    }
