from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.tradex_monthly_weekly_daily_misalignment_audit_v1 import (
    _build_pattern,
    _build_timeframe_context_inventory,
    run_monthly_weekly_daily_misalignment_audit_v1,
)


SOURCE_ROWS_PARQUET = Path(
    r"G:\Tradex\candidate_generation_pre_filter_context_shape_v1\20260429T145332Z-7bd554ac\candidate_prefilter_rows.parquet"
)
BAD_PICK_SESSION = Path(r"G:\Tradex\bad_pick_root_cause_audit\20260429T155546Z-2053e5e4")
POLICY_LEDGER = Path(r"G:\Tradex\sample_replays\tradex_integrated_guarded_v1_stress200\integrated_guarded_v1_policy_trade_ledger.json")


def test_pattern_builder_and_inventory_cover_expected_context_fields() -> None:
    frame = pd.DataFrame(
        {
            "monthly_context": ["monthly_overextended", "monthly_range"],
            "weekly_context": ["weekly_overextended", "weekly_range"],
            "daily_main_state_ctx": ["daily_reversal_up_candidate", None],
            "shape_classification": ["shape_context_dependent", "shape_missing"],
            "monthly_context_no_lookahead": [True, None],
            "weekly_context_no_lookahead": [True, None],
            "conditional_high_value": [True, False],
            "dist_ma20_pct": [0.05, 0.01],
            "dist_ma60_pct": [0.07, 0.02],
            "forward_ret_20d": [0.1, -0.1],
            "path_value_score_v1": [0.2, -0.2],
            "vol_ratio5_20": [1.1, 0.7],
            "liquidity20d": [1000.0, None],
        }
    )
    assert _build_pattern(frame.iloc[0]) == "monthly_overextended|weekly_overextended|daily_reversal_up_candidate|shape_context_dependent"
    inventory = _build_timeframe_context_inventory(frame)
    inventory_rows = inventory["field_inventory"]
    by_field = {row["field"]: row for row in inventory_rows}
    assert by_field["monthly_context"]["availability"] == "confirmed usable"
    assert by_field["daily_main_state_ctx"]["availability"] == "partial overlay"
    assert by_field["event_flag"]["availability"] == "unavailable"
    assert by_field["dist_ma20_pct"]["availability"] == "proxy only"
    assert inventory["schema_version"].startswith("tradex_monthly_weekly_daily_misalignment_audit_v1")


def test_misalignment_audit_smoke_run_writes_required_artifacts(tmp_path: Path) -> None:
    output_root = tmp_path / "monthly_weekly_daily_misalignment_audit_v1"
    result = run_monthly_weekly_daily_misalignment_audit_v1(
        source_rows_parquet=SOURCE_ROWS_PARQUET,
        bad_pick_session=BAD_PICK_SESSION,
        policy_ledger_path=POLICY_LEDGER,
        output_root=output_root,
        limit_anchor_dates=2,
        jobs=2,
    )
    session_dir = Path(result["session_dir"])
    assert session_dir.exists()
    required = [
        "run_manifest.json",
        "input_resolution.json",
        "timeframe_context_inventory.json",
        "misalignment_cohort_summary.json",
        "timeframe_combination_contrast_summary.json",
        "misalignment_pairwise_boundary.parquet",
        "misalignment_regime_breakdown.json",
        "misalignment_failure_patterns.json",
        "misalignment_challenger_hypotheses.json",
        "monthly_weekly_daily_misalignment_audit_v1_decision.json",
        "_ARTIFACT_COMPLETE.json",
    ]
    for file_name in required:
        assert (session_dir / file_name).exists(), file_name

    manifest = json.loads((session_dir / "run_manifest.json").read_text(encoding="utf-8"))
    input_resolution = json.loads((session_dir / "input_resolution.json").read_text(encoding="utf-8"))
    cohort = json.loads((session_dir / "misalignment_cohort_summary.json").read_text(encoding="utf-8"))
    contrast = json.loads((session_dir / "timeframe_combination_contrast_summary.json").read_text(encoding="utf-8"))
    decision = json.loads((session_dir / "monthly_weekly_daily_misalignment_audit_v1_decision.json").read_text(encoding="utf-8"))
    artifact = json.loads((session_dir / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))

    assert manifest["schema_version"] == "tradex_monthly_weekly_daily_misalignment_audit_v1_manifest_v1"
    assert input_resolution["authoritative_for_audit"] is True
    assert cohort["explicit_misalignment_count"] >= 0
    assert contrast["schema_version"] == "tradex_monthly_weekly_daily_misalignment_audit_v1_timeframe_combination_contrast_summary_v1"
    assert decision["decision"] in {"ready_for_single_axis_challenger_design", "needs_more_input_data", "insufficient_signal", "explanation_only"}
    assert artifact["parse_status"]["run_manifest"] is True
    assert artifact["row_reconciliation"]["no_silent_row_drops"] is True

    pairwise = pd.read_parquet(session_dir / "misalignment_pairwise_boundary.parquet")
    assert len(pairwise) >= 0
    if len(pairwise):
        assert {"near_miss_joined", "monthly_alignment_same", "weekly_alignment_same", "daily_alignment_same"}.issubset(set(pairwise.columns))
