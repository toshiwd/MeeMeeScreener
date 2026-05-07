from __future__ import annotations

from scripts.tradex_side_aware_min_pool_feasibility_v1 import build_artifacts


def test_side_aware_min_pool_feasibility_prefers_risk_filter_before_surface_build() -> None:
    payload = build_artifacts()

    manifest = payload["manifest"]
    input_resolution = payload["input_resolution"]
    generation = payload["generation_summary"]
    no_lookahead = payload["no_lookahead_audit"]
    breadth = payload["breadth_comparison"]
    winner = payload["winner_audit"]
    oracle = payload["oracle_headroom"]
    admission = payload["admission_cost"]
    decision = payload["decision"]
    selected = payload["selected_pool"]

    assert manifest["script_name"] == "tradex_side_aware_min_pool_feasibility_v1"
    assert input_resolution["resolved_raw_candidate_source"].endswith("integrated_guarded_v1_selection_only_ledger.json")
    assert generation["contract_name"] == "side_aware_minimum_pool_size_v1"
    assert generation["selected_counts"]["row_count"] > 2000
    assert generation["selected_counts"]["group_count"] >= 200
    assert no_lookahead["verified_no_lookahead_pass"] is True
    assert no_lookahead["full_pool_verified"] is False
    assert breadth["comparison"]["side_aware_min_pool"]["overall"]["row_count"] > breadth["comparison"]["current_accumulated_pool"]["overall"]["row_count"]
    assert winner["side_aware_min_pool"]["row_count"] == len(selected)
    assert oracle["breadth_headroom"]["5"]["oracle_minus_champion_mean_forward_ret_20d"] is not None
    assert admission["added_candidates_count"] > 1000
    assert decision["decision"] == "needs_risk_flag_filter_before_surface_build"
    assert decision["authoritative_rollup_decision"] == "needs_risk_flag_filter_before_surface_build"
