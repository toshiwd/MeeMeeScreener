from __future__ import annotations

from scripts.tradex_risk_flag_filter_before_high_recall_surface_v1 import build_artifacts


def test_risk_flag_filter_prefers_combined_conservative() -> None:
    payload = build_artifacts()

    manifest = payload["manifest"]
    input_resolution = payload["input_resolution"]
    risk_inventory = payload["risk_flag_inventory"]
    variant_contracts = payload["variant_contracts"]
    comparison = payload["variant_comparison"]
    recommendation = payload["recommendation"]
    decision = payload["decision"]
    variant_rows = payload["variant_rows"]

    assert manifest["script_name"] == "tradex_risk_flag_filter_before_high_recall_surface_v1"
    assert input_resolution["resolved_raw_candidate_source"].endswith("integrated_guarded_v1_selection_only_ledger.json")
    assert risk_inventory["candidate_pool_tier"]["present"] is True
    assert risk_inventory["candidate_pool_tier"]["non_null_count"] == comparison["comparison"]["unfiltered_side_aware_min_pool"]["row_count"]
    assert len(variant_contracts["variants"]) == 6
    assert variant_rows["variant_name"].nunique() == 6
    assert comparison["comparison"]["current_accumulated_pool"]["row_count"] == 488
    assert comparison["comparison"]["unfiltered_side_aware_min_pool"]["row_count"] == 2475
    assert comparison["variants"]["filter_combined_conservative"]["row_count"] > comparison["comparison"]["current_accumulated_pool"]["row_count"]
    assert comparison["variants"]["filter_combined_conservative"]["short_row_count"] >= comparison["comparison"]["current_accumulated_pool"]["short_row_count"]
    assert comparison["variants"]["filter_combined_conservative"]["retained_non_positive_forward_return_rows"] < comparison["variants"]["filter_exclude_analysis_only_off"]["retained_non_positive_forward_return_rows"]
    assert recommendation["recommended_filter"] == "filter_combined_conservative"
    assert decision["decision"] == "ready_to_build_feature_complete_surface_with_filter"
    assert decision["authoritative_rollup_decision"] == "ready_to_build_feature_complete_surface_with_filter"
