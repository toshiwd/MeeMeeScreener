from __future__ import annotations

import pandas as pd

from scripts import tradex_starter_entry_actionability_score_v1 as mod


def test_expand_tags_creates_research_tag_booleans() -> None:
    rows = pd.DataFrame(
        [
            {
                "research_setup_tags_json": '["pullback_candidate"]',
                "research_risk_tags_json": '["high_volatility_risk"]',
                "research_regime_tags_json": '["monthly_high_zone"]',
            }
        ]
    )
    out = mod.expand_tags(rows)
    assert bool(out.loc[0, "setup:pullback_candidate"])
    assert bool(out.loc[0, "risk:high_volatility_risk"])
    assert bool(out.loc[0, "regime:monthly_high_zone"])


def test_comparison_summary_detects_actionability_improvement() -> None:
    rows = pd.DataFrame(
        [
            {"validation_period": "2024", "decision_date": 20240101, "code": "A", "baseline_rank": 1, "actionability_rank": 2, "ret20": -0.1, "starter_good": False, "starter_bad": True, "selected_loser": True, "selected_winner": False, "immediate_adverse_entry": True, "mae20": -0.12, "mfe20": 0.01, "same_date_ret20_rank_pct": 0.1},
            {"validation_period": "2024", "decision_date": 20240101, "code": "B", "baseline_rank": 2, "actionability_rank": 1, "ret20": 0.1, "starter_good": True, "starter_bad": False, "selected_loser": False, "selected_winner": True, "immediate_adverse_entry": False, "mae20": -0.01, "mfe20": 0.12, "same_date_ret20_rank_pct": 0.9},
        ]
    )
    summary = mod.comparison_summary(rows)
    row = summary[(summary["period"] == "2024") & (summary["topk"] == 5)].iloc[0]
    assert row["baseline_mean_ret20"] == row["actionability_mean_ret20"]
    assert row["baseline_starter_good_rate"] == row["actionability_starter_good_rate"]


def test_decide_keep_when_return_and_label_gates_pass() -> None:
    comp = pd.DataFrame(
        [
            {"period": "2024_2026_combined", "topk": 10, "delta_mean_ret20": 0.01, "delta_starter_good_rate": 0.1, "delta_starter_bad_rate": -0.1},
            {"period": "2024", "topk": 10, "delta_mean_ret20": 0.0, "delta_starter_good_rate": 0.1, "delta_starter_bad_rate": -0.1},
            {"period": "2025", "topk": 10, "delta_mean_ret20": 0.0, "delta_starter_good_rate": 0.1, "delta_starter_bad_rate": -0.1},
            {"period": "2026_label_safe", "topk": 10, "delta_mean_ret20": 0.0, "delta_starter_good_rate": 0.1, "delta_starter_bad_rate": -0.1},
        ]
    )
    repl = pd.DataFrame([{"period": "2024_2026_combined", "topk": 10, "added_minus_removed_ret20": 0.02}])
    decision = mod.decide(comp, repl, {"interpretable": True})
    assert decision["research_decision"] == "keep_for_formal_challenger_compare"


def test_feature_columns_include_tag_booleans() -> None:
    rows = pd.DataFrame(columns=["baseline_score", "setup:x", "risk:y"])
    numeric, bools = mod.feature_columns(rows)
    assert "baseline_score" in numeric
    assert "setup:x" in bools
    assert "risk:y" in bools
