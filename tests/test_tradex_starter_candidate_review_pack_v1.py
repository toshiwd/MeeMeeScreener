from __future__ import annotations

import json

import pandas as pd

from scripts import tradex_starter_candidate_review_pack_v1 as mod


def test_classify_starter_requires_low_risk_good_context() -> None:
    row = pd.Series(
        {
            "research_candidate_source_family": "pullback_reclaim_source",
            "research_risk_tags_json": json.dumps(["no_shadow_risk_tag"]),
            "research_setup_tags_json": json.dumps(["pullback_candidate"]),
            "selected_loser": False,
            "immediate_adverse_entry": False,
            "next_open_available": True,
            "entry_allowed_by_score": True,
            "baseline_score": 10,
        }
    )

    action, reasons, score = mod._classify(row)

    assert action == "starter"
    assert "pullback_reclaim_context" in reasons
    assert score > 10


def test_classify_overextension_high_risk_waits() -> None:
    row = pd.Series(
        {
            "research_candidate_source_family": "overextension_risk_source",
            "research_risk_tags_json": json.dumps(["ma20_overextension_risk", "ma60_overextension_risk"]),
            "research_setup_tags_json": json.dumps(["overextension_candidate"]),
            "selected_loser": False,
            "immediate_adverse_entry": False,
            "next_open_available": True,
            "entry_allowed_by_score": True,
            "baseline_score": 12,
        }
    )

    action, reasons, _ = mod._classify(row)

    assert action == "wait"
    assert "high_risk_flags" in reasons


def test_classify_selected_loser_avoids() -> None:
    row = pd.Series(
        {
            "research_candidate_source_family": "early_trend_source",
            "research_risk_tags_json": json.dumps(["no_shadow_risk_tag"]),
            "research_setup_tags_json": json.dumps(["early_trend_candidate"]),
            "selected_loser": True,
            "immediate_adverse_entry": False,
            "next_open_available": True,
            "entry_allowed_by_score": True,
            "baseline_score": 11,
        }
    )

    action, reasons, _ = mod._classify(row)

    assert action == "avoid"
    assert "severe_risk_flags" in reasons
