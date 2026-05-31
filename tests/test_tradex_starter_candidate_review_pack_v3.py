from __future__ import annotations

import json

import pandas as pd

from scripts import tradex_starter_candidate_review_pack_v3 as mod


def test_family_rules_include_pullback_promotion_and_avoid() -> None:
    rules = mod.family_rules()["pullback_reclaim_source"]

    assert any("MA7/MA20 reclaim" in item for item in rules["starter_promotion_conditions"])
    assert any("below MA20" in item for item in rules["avoid_conditions"])


def test_enrich_rows_adds_manual_review_fields() -> None:
    v2 = pd.DataFrame(
        [
            {
                "review_rank": 1,
                "decision_date": 20260508,
                "code": "2802",
                "candidate_action_class": "watch",
                "baseline_rank": 2,
                "baseline_score": 17,
                "research_candidate_source_family": "pullback_reclaim_source",
                "risk_flags": "no_shadow_risk_tag",
                "classification_reason": "no_keep_gated_artifact|entry_liquidity_or_data_coverage_gap",
                "daily_bar_source": "confirmed",
                "feature_freshness_status": "fresh",
            }
        ]
    )
    surface = pd.DataFrame(
        [
            {
                "code": "2802",
                "family_assignment_reason_json": json.dumps({"primary_rule": "pullback_reclaim_family"}),
                "daily_bar_source": "confirmed",
                "daily_bar_max_date": 20260508,
                "feature_source_max_date": 20260508,
                "feature_freshness_status": "fresh",
                "provisional_used": False,
            }
        ]
    )

    out = mod.enrich_rows(v2, surface)

    assert out.loc[0, "confidence_level"] == "watch"
    assert out.loc[0, "validation_status"] == "manual_review_only"
    assert "entry/liquidity confirmation is incomplete" in out.loc[0, "primary_watch_reason"]
    assert "MA7/MA20 reclaim" in out.loc[0, "starter_promotion_conditions"]


def test_cards_have_required_fields() -> None:
    rows = pd.DataFrame(
        [
            {
                "code": "1942",
                "decision_date": 20260508,
                "candidate_action_class": "watch",
                "research_candidate_source_family": "breakout_retest_source",
                "baseline_rank": 1,
                "baseline_score": 17,
                "data_source": "confirmed",
                "feature_freshness_status": "fresh",
                "family_assignment_reason": json.dumps({"primary_rule": "breakout_retest_family"}),
                "primary_watch_reason": "watch reason",
                "risk_flags": "no_shadow_risk_tag",
                "starter_promotion_conditions": json.dumps(["retest hold"]),
                "wait_conditions": json.dumps(["wait"]),
                "avoid_conditions": json.dumps(["failed breakout"]),
                "manual_chart_checkpoints": json.dumps(["check retest"]),
                "confidence_level": "watch",
            }
        ]
    )

    card = mod._cards(rows, limit=1)[0]

    assert card["code"] == "1942"
    assert card["starter_promotion_conditions"] == ["retest hold"]
    assert card["validation_status"] == "not_validated_challenger; manual_review_only"
