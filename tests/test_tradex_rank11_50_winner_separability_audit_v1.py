from __future__ import annotations

import pandas as pd

from scripts import tradex_rank11_50_winner_separability_audit_v1 as mod


def test_feature_contract_marks_outcomes_and_features() -> None:
    contract = mod.feature_contract(["baseline_rank", "ret20"])
    assert contract["fields"]["baseline_rank"]["classification"] == "point_in_time_feature"
    assert contract["fields"]["ret20"]["classification"] == "outcome_only"
    assert contract["fields"]["ret20_derived_terms"]["classification"] == "forbidden_future_leak"


def test_ranking_gradient_buckets_rank11_50() -> None:
    rows = pd.DataFrame(
        [
            {"baseline_rank": 11, "decision_date": 20240101, "code": "A", "ret20": 0.2, "winner_label": True, "bad_label": False, "severe_label": False},
            {"baseline_rank": 25, "decision_date": 20240101, "code": "B", "ret20": -0.1, "winner_label": False, "bad_label": True, "severe_label": True},
            {"baseline_rank": 40, "decision_date": 20240102, "code": "C", "ret20": 0.0, "winner_label": False, "bad_label": False, "severe_label": False},
        ]
    )
    grad = mod.ranking_gradient(rows)
    assert grad["rank_11_20"]["winner_rate"] == 1.0
    assert grad["rank_21_30"]["severe_rate"] == 1.0


def test_decide_supports_learned_lift_when_oos_gates_pass() -> None:
    probe = {"auc": 0.6}
    lift = {
        "promote_1": {
            "promoted_minus_displaced_ret20": 0.02,
            "OOS_top10_delta_mean_ret20": 0.01,
            "OOS_top10_delta_bad_pick_rate": 0.0,
            "OOS_top10_delta_severe_loss_rate": 0.0,
            "accidental_promotion_bad_rate": 0.1,
        }
    }
    decision, _, best = mod.decide(probe, lift, {"rank_11_20": {"mean_ret20": 0.01}, "rank_31_50": {"mean_ret20": 0.02}})
    assert decision == "separability_supports_learned_lift_pretest"
    assert best == "promote_1"


def test_decide_feature_signal_too_weak_when_auc_low() -> None:
    probe = {"auc": 0.5}
    lift = {
        "promote_1": {
            "promoted_minus_displaced_ret20": -0.01,
            "OOS_top10_delta_mean_ret20": -0.01,
            "OOS_top10_delta_bad_pick_rate": 0.0,
            "OOS_top10_delta_severe_loss_rate": 0.0,
            "accidental_promotion_bad_rate": 0.1,
        }
    }
    decision, _, _ = mod.decide(probe, lift, {"rank_11_20": {"mean_ret20": 0.01}, "rank_31_50": {"mean_ret20": 0.02}})
    assert decision == "feature_signal_too_weak_rebuild_candidate_generation"


def test_decide_gradient_rebuild_when_oos_return_positive_but_risk_worsens() -> None:
    probe = {"auc": 0.62}
    lift = {
        "promote_1": {
            "promoted_minus_displaced_ret20": 0.02,
            "OOS_top10_delta_mean_ret20": 0.01,
            "OOS_top10_delta_bad_pick_rate": 0.03,
            "OOS_top10_delta_severe_loss_rate": 0.02,
            "accidental_promotion_bad_rate": 0.35,
        }
    }
    gradient = {
        "rank_11_20": {"mean_ret20": 0.0130},
        "rank_21_30": {"mean_ret20": 0.0140},
        "rank_31_50": {"mean_ret20": 0.0131},
    }
    decision, _, _ = mod.decide(probe, lift, gradient)
    assert decision == "ranking_gradient_too_weak_rebuild_candidate_generation"
