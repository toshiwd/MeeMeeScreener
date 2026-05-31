from __future__ import annotations

import pandas as pd

from scripts import tradex_starter_ready_negative_tag_pretest_v1 as mod


def test_has_lookahead_dependency_detects_ret20_bucket_parts() -> None:
    assert mod.has_lookahead_dependency("breakout|no_trigger_hit|flat_or_negative") is True
    assert mod.has_lookahead_dependency("breakout|no_trigger_hit|monthly_supportive_text") is False


def test_prepare_rows_applies_signatures_to_all_rows() -> None:
    rows = pd.DataFrame(
        [
            {
                "research_candidate_source_family": "breakout_retest_source",
                "ret20": -0.01,
                "trigger_hit": False,
                "invalidation_hit": False,
                "reason_summary": "monthly context is supportive",
                "decision_date": 20240131,
                "code": "A",
            },
            {
                "research_candidate_source_family": "breakout_retest_source",
                "ret20": 0.05,
                "trigger_hit": True,
                "invalidation_hit": False,
                "reason_summary": "monthly context is supportive",
                "decision_date": 20240131,
                "code": "B",
            },
        ]
    )
    signature = "breakout|no_trigger_hit|no_invalidation_hit|flat_or_negative|monthly_supportive_text"

    prepared = mod.prepare_rows(rows, [signature])

    assert prepared["negative_tag_hit"].tolist() == [True, False]


def test_decide_blocks_missing_columns_for_lookahead_signatures() -> None:
    rows = pd.DataFrame([{"negative_tag_hit": True}])
    missing = {"blocked_missing_columns": True}

    assert mod.decide(rows, ["x"], missing, {}, {}) == "blocked_missing_columns"


def test_compare_marks_small_samples_not_allowed() -> None:
    left = pd.DataFrame([{"ret20": -0.01, "decision_date": 20240131, "code": "A"}])
    right = pd.DataFrame([{"ret20": 0.01, "decision_date": 20240131, "code": "B"}])

    result = mod.compare(left, right)

    assert result["mean_ret20_delta_tagged_minus_untagged"] < 0
    assert result["sample_allows_comparison"] is False
