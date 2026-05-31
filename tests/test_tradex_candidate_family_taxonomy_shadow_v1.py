from __future__ import annotations

import json

import pandas as pd

from scripts import tradex_candidate_family_taxonomy_shadow_v1 as mod


def test_tag_rows_uses_research_names_not_candidate_source() -> None:
    rows = pd.DataFrame(
        [
            {
                "decision_date": 20240101,
                "code": "1000",
                "baseline_rank": 1,
                "ma7_gt_ma20_gt_ma60": True,
                "above20_streak": 12,
                "above60_streak": 65,
                "days_since_ma20_reclaim": 20,
                "monthly_high_zone_proxy": True,
                "monthly_box_breakout_proxy": False,
                "monthly_box_inside_proxy": False,
                "weekly_monthly_uptrend_proxy": True,
                "dist_ma20_pct": 0.10,
                "dist_ma60_pct": 0.20,
                "ma7_slope": 0.05,
                "realized_vol20": 0.03,
                "upper_wick_ratio": 0.50,
                "failed_high_update": True,
                "large_bearish_candle": False,
            },
            {
                "decision_date": 20240101,
                "code": "2000",
                "baseline_rank": 2,
                "ma7_gt_ma20_gt_ma60": False,
                "above20_streak": 3,
                "above60_streak": 0,
                "days_since_ma20_reclaim": 1,
                "monthly_high_zone_proxy": False,
                "monthly_box_breakout_proxy": False,
                "monthly_box_inside_proxy": True,
                "weekly_monthly_uptrend_proxy": False,
                "dist_ma20_pct": 0.01,
                "dist_ma60_pct": 0.02,
                "ma7_slope": 0.01,
                "realized_vol20": 0.01,
                "upper_wick_ratio": 0.10,
                "failed_high_update": False,
                "large_bearish_candle": False,
            },
        ]
    )
    tagged = mod.tag_rows(rows)
    assert "research_setup_tags_json" in tagged
    assert "candidate_source" not in tagged.columns
    setup = json.loads(tagged.loc[0, "research_setup_tags_json"])
    risk = json.loads(tagged.loc[0, "research_risk_tags_json"])
    regime = json.loads(tagged.loc[0, "research_regime_tags_json"])
    assert "mature_trend_candidate" in setup
    assert "upper_wick_risk" in risk
    assert "monthly_high_zone" in regime


def test_summarize_by_tag_detects_loser_overrepresentation() -> None:
    rows = pd.DataFrame(
        [
            {
                "decision_date": 20240101,
                "year": 2024,
                "code": "1000",
                "baseline_rank": 1,
                "ret20": -0.10,
                "selected_loser": True,
                "selected_winner": False,
                "selected_non_loser": False,
                "research_setup_tags_json": json.dumps(["overextension_candidate"]),
                "research_risk_tags_json": json.dumps(["ma20_overextension_risk"]),
                "research_regime_tags_json": json.dumps(["monthly_high_zone"]),
            },
            {
                "decision_date": 20240101,
                "year": 2024,
                "code": "2000",
                "baseline_rank": 2,
                "ret20": 0.10,
                "selected_loser": False,
                "selected_winner": True,
                "selected_non_loser": True,
                "research_setup_tags_json": json.dumps(["trend_continuation_candidate"]),
                "research_risk_tags_json": json.dumps(["no_shadow_risk_tag"]),
                "research_regime_tags_json": json.dumps(["monthly_high_zone"]),
            },
        ]
    )
    summary = mod.summarize_by_tag(rows)
    over = summary[(summary["period"] == "2024_2026_combined") & (summary["tag"] == "overextension_candidate")].iloc[0]
    assert over["selected_loser_rate"] == 1.0
    assert over["loser_minus_winner_spread"] == 1.0


def test_decide_requests_source_split_for_broad_shadow_family() -> None:
    coverage = {"tags_usable_for_selected_loser_audit": True}
    recs = [{"loser_minus_winner_spread": 0.06, "winner_damage_risk": "high"}]
    decision = mod.decide(pd.DataFrame(), recs, coverage)
    assert decision["research_decision"] == "candidate_source_split_needed"
