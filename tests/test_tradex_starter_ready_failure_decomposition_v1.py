from __future__ import annotations

import pandas as pd

from scripts import tradex_starter_ready_failure_decomposition_v1 as mod


def test_ret20_bucket_boundaries() -> None:
    assert mod.ret20_bucket(0.01) == "good"
    assert mod.ret20_bucket(0.0) == "flat_or_negative"
    assert mod.ret20_bucket(-0.05) == "bad"
    assert mod.ret20_bucket(-0.10) == "severe"


def test_pattern_type_from_family() -> None:
    assert mod.pattern_type("pullback_reclaim_source") == "pullback"
    assert mod.pattern_type("breakout_retest_source") == "breakout"
    assert mod.pattern_type("early_trend_source") == "early_trend"


def test_negative_tags_are_diagnostic_only() -> None:
    rows = pd.DataFrame(
        [
            {
                "manual_judgment": "starter_ready",
                "decision_date": 20240131,
                "code": "A",
                "ret20": -0.01,
                "trigger_hit": False,
                "invalidation_hit": True,
                "research_candidate_source_family": "pullback_reclaim_source",
                "pattern_type": "pullback",
                "ret20_bucket": "flat_or_negative",
                "reason_summary": "monthly context is supportive",
            },
            {
                "manual_judgment": "starter_ready",
                "decision_date": 20240229,
                "code": "B",
                "ret20": -0.02,
                "trigger_hit": False,
                "invalidation_hit": True,
                "research_candidate_source_family": "pullback_reclaim_source",
                "pattern_type": "pullback",
                "ret20_bucket": "flat_or_negative",
                "reason_summary": "monthly context is supportive",
            },
        ]
    )
    rows["failure_signature"] = rows.apply(mod.failure_signature, axis=1)

    tags = mod.build_negative_tags(rows, rows)

    assert tags
    assert tags[0]["active_gate"] is False
    assert tags[0]["status"] == "diagnostic_candidate_only"


def test_decide_negative_tag_candidate_found() -> None:
    starter_ready = pd.DataFrame([{"code": str(i)} for i in range(10)])
    starter_failures = pd.DataFrame([{"code": "1"}])

    assert mod.decide(starter_ready, starter_failures, {}, [{"tag": "x"}]) == "negative_tag_candidate_found"
