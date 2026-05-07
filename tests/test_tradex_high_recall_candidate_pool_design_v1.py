from __future__ import annotations

from scripts.tradex_high_recall_candidate_pool_design_v1 import (
    ACCUMULATED_SESSION,
    CURRENT_BROAD_PREFILTER_SESSION,
    CURRENT_BROAD_TWO_STAGE_SESSION,
    CURRENT_REPAIR_PREFILTER_SESSION,
    RAW_CANDIDATE_SNAPSHOT_UNIVERSE,
    REDESIGN_AUDIT_SESSION,
    build_artifacts,
)


def test_high_recall_candidate_pool_design_prefers_side_aware_min_pool() -> None:
    payload = build_artifacts(
        broad_prefilter_session=CURRENT_BROAD_PREFILTER_SESSION,
        broad_two_stage_session=CURRENT_BROAD_TWO_STAGE_SESSION,
        repair_prefilter_session=CURRENT_REPAIR_PREFILTER_SESSION,
        raw_candidate_universe=RAW_CANDIDATE_SNAPSHOT_UNIVERSE,
        accumulated_surface=ACCUMULATED_SESSION / "accumulated_forward_prediction_rows.parquet",
        current_breadth_audit=REDESIGN_AUDIT_SESSION / "candidate_pool_breadth_audit.json",
        current_oracle_headroom=REDESIGN_AUDIT_SESSION / "candidate_pool_oracle_headroom_audit.json",
        current_admission_audit=REDESIGN_AUDIT_SESSION / "candidate_admission_failure_audit.json",
    )

    inventory = payload["current_candidate_generation_contract_inventory"]
    options = payload["high_recall_candidate_pool_design_options"]["options"]
    contract = payload["high_recall_candidate_pool_contract"]
    feasibility = payload["high_recall_candidate_pool_feasibility_estimate"]
    recommendation = payload["high_recall_candidate_pool_recommendation"] if "high_recall_candidate_pool_recommendation" in payload else None
    decision = payload["high_recall_candidate_pool_design_v1_decision"]

    assert inventory["inventory_judgment"]["primary_blocker"] == "side_aware_under_supply"
    assert options[0]["option_name"] == "side_aware_candidate_admission_caps"
    assert contract["contract_name"] == "side_aware_minimum_pool_size_v1"
    assert contract["implementation_style"] == "two_stage_high_recall_then_rerank"
    assert contract["side_specific_caps"] == {"long": 40, "short": 10}
    assert feasibility["compatibility"]["no_lookahead_safe"] is True
    assert feasibility["observability_limits"]["rejected_rows_outside_snapshot_not_logged"] is True
    assert decision["decision"] == "ready_to_design_side_aware_min_pool"
    assert decision["authoritative_rollup_decision"] == "ready_to_design_side_aware_min_pool"
    assert recommendation is None or recommendation["recommended_axis"] == "side_aware_candidate_admission_caps"
