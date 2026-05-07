from __future__ import annotations

from scripts.tradex_high_recall_reranker_validation_v1 import _build_input_validation, _decision_from_results, _resolve_input_bundle


def test_decision_from_results_uses_flat_topk_metrics() -> None:
    comparison = {
        "models": {
            "tree_hgb_path_value": {
                "topk": {
                    "top5": {
                        "mean_forward_ret_20d": 1.0,
                        "champion_mean_forward_ret_20d": 0.5,
                        "mean_path_value_score_v1": 2.0,
                        "champion_mean_path_value_score_v1": 1.0,
                        "top20pct_capture_rate": 0.3,
                        "champion_top20pct_capture_rate": 0.2,
                        "bottom15_contamination_rate": 0.0,
                        "champion_bottom15_contamination_rate": 0.1,
                        "membership_changed_count": 4,
                    },
                    "top10": {
                        "mean_forward_ret_20d": 0.9,
                        "champion_mean_forward_ret_20d": 0.4,
                        "mean_path_value_score_v1": 1.5,
                        "champion_mean_path_value_score_v1": 0.9,
                        "top20pct_capture_rate": 0.25,
                        "champion_top20pct_capture_rate": 0.2,
                        "bottom15_contamination_rate": 0.0,
                        "champion_bottom15_contamination_rate": 0.1,
                        "membership_changed_count": 2,
                    },
                    "top20": {
                        "mean_forward_ret_20d": 0.8,
                        "champion_mean_forward_ret_20d": 0.8,
                        "mean_path_value_score_v1": 1.2,
                        "champion_mean_path_value_score_v1": 1.2,
                        "top20pct_capture_rate": 0.2,
                        "champion_top20pct_capture_rate": 0.2,
                        "bottom15_contamination_rate": 0.0,
                        "champion_bottom15_contamination_rate": 0.0,
                        "membership_changed_count": 0,
                    },
                }
            }
        }
    }
    failure_mode = {
        "best_model_name": "tree_hgb_path_value",
        "noisy_tier_dominance": True,
        "long_side_only_improvement": True,
    }

    decision = _decision_from_results(failure_mode, comparison)

    assert decision["decision"] == "hold_needs_high_recall_filter_revision"
    assert decision["improvement_checks"]["top5_forward_ret_improved"] is True
    assert decision["improvement_checks"]["top10_forward_ret_improved"] is True
    assert decision["improvement_checks"]["membership_nontrivial"] is True


def test_surface_input_validation_confirms_no_lookahead_and_leakage() -> None:
    bundle = _resolve_input_bundle()
    validation = _build_input_validation(bundle["frame"])
    assert validation["row_count"] == 1329
    assert validation["group_count"] == 267
    assert validation["all_33_frozen_features_present"] is True
    assert validation["no_lookahead_passed"] is True
    assert validation["leakage_passed"] is True
    assert validation["prediction_ready_file_missing_resolved_via_candidate_rows"] is True
