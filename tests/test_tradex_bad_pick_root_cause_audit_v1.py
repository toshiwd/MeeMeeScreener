from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.tradex_bad_pick_root_cause_audit_v1 import (
    _build_boundary_summary,
    _classify_root_cause,
    run_bad_pick_root_cause_audit_v1,
)


SOURCE_ROWS_PARQUET = Path(
    r"G:\Tradex\candidate_generation_pre_filter_context_shape_v1\20260429T145332Z-7bd554ac\candidate_prefilter_rows.parquet"
)
CANDIDATE_SNAPSHOT = Path(
    r"G:\Tradex\sample_replays\tradex_integrated_guarded_v1_stress200\integrated_guarded_v1_candidate_snapshots.json"
)
SELECTION_LEDGER = Path(
    r"G:\Tradex\sample_replays\tradex_integrated_guarded_v1_stress200\integrated_guarded_v1_selection_only_ledger.json"
)


def test_root_cause_classifier_covers_expected_cases() -> None:
    late_row = pd.Series(
        {
            "score": 0.75,
            "forward_ret_20d": -0.12,
            "path_value_score_v1": -0.2,
            "top15_label": False,
            "bottom15_label": True,
            "monthly_context": "monthly_overextended",
            "weekly_context": "weekly_overextended",
            "daily_main_state_ctx": "daily_reversal_up_candidate",
            "dist_ma20_pct": 0.06,
            "dist_ma60_pct": 0.08,
            "gap_pct": 0.01,
            "body_ratio": 0.8,
            "upper_wick_ratio": 0.1,
            "lower_wick_ratio": 0.0,
            "vol_ratio5_20": 1.1,
            "family_classification": "regime_dependent_family",
            "stable_bad_pick_family": False,
            "stable_high_value_family": False,
            "shape_joined": True,
            "conditional_high_value": True,
            "dominant_regime_context": "C:risk_on_trend",
            "market_regime_bucket": "risk_on",
            "shape_classification": "shape_positive_modifier",
            "candle_shape_modifier": "gap_down_bear",
        }
    )
    late = _classify_root_cause(late_row)
    assert late["root_cause_code"] == "late_entry_after_extended_rise"
    assert late["confidence"] == "high"

    overweight_row = pd.Series(
        {
            "score": 0.68,
            "forward_ret_20d": -0.02,
            "path_value_score_v1": -0.05,
            "top15_label": False,
            "bottom15_label": True,
            "monthly_context": "monthly_overextended",
            "weekly_context": "weekly_overextended",
            "daily_main_state_ctx": "daily_reversal_up_candidate",
            "dist_ma20_pct": 0.03,
            "dist_ma60_pct": 0.04,
            "gap_pct": 0.0,
            "body_ratio": 0.7,
            "upper_wick_ratio": 0.12,
            "lower_wick_ratio": 0.02,
            "vol_ratio5_20": 1.0,
            "family_classification": "stable_bad_pick_family",
            "stable_bad_pick_family": True,
            "stable_high_value_family": False,
            "shape_joined": True,
            "conditional_high_value": False,
            "dominant_regime_context": "C:risk_on_trend",
            "market_regime_bucket": "risk_on",
            "shape_classification": "shape_context_dependent",
            "candle_shape_modifier": "upper_wick_then_bear",
        }
    )
    overweight = _classify_root_cause(overweight_row)
    assert overweight["root_cause_code"] == "score_component_overweight"
    assert overweight["confidence"] == "high"

    unknown_row = pd.Series({"score": 0.1, "forward_ret_20d": 0.01, "path_value_score_v1": 0.02})
    unknown = _classify_root_cause(unknown_row)
    assert unknown["root_cause_code"] == "unknown_or_insufficient_data"
    assert unknown["confidence"] == "low"


def test_boundary_summary_uses_immediate_near_miss_from_rank_boundary() -> None:
    frame = pd.DataFrame(
        [
            {
                "anchor_date": "2024-03-15",
                "side": "long",
                "symbol": "A",
                "champion_rank": 5,
                "score": 0.90,
                "forward_ret_20d": -0.10,
                "path_value_score_v1": -0.20,
                "shape_classification": "shape_positive_modifier",
            },
            {
                "anchor_date": "2024-03-15",
                "side": "long",
                "symbol": "B",
                "champion_rank": 6,
                "score": 0.88,
                "forward_ret_20d": 0.05,
                "path_value_score_v1": 0.03,
                "shape_classification": "shape_context_dependent",
            },
            {
                "anchor_date": "2024-03-15",
                "side": "long",
                "symbol": "C",
                "champion_rank": 7,
                "score": 0.87,
                "forward_ret_20d": 0.03,
                "path_value_score_v1": 0.01,
                "shape_classification": "shape_positive_modifier",
            },
        ]
    )
    row = frame.iloc[0]
    summary = _build_boundary_summary(frame, row)
    assert summary["boundary_candidate_count"] == 2
    assert summary["best_near_miss_rank"] == 6
    assert summary["score_gap"] == 0.020000000000000018
    assert summary["forward_ret_20d_gap"] == -0.15000000000000002
    assert summary["path_value_gap"] == -0.23


def test_bad_pick_root_cause_audit_smoke_run_writes_required_artifacts(tmp_path: Path) -> None:
    output_root = tmp_path / "bad_pick_root_cause_audit"
    result = run_bad_pick_root_cause_audit_v1(
        source_rows_parquet=SOURCE_ROWS_PARQUET,
        candidate_snapshot_path=CANDIDATE_SNAPSHOT,
        selection_ledger_path=SELECTION_LEDGER,
        output_root=output_root,
        limit_anchor_dates=2,
        jobs=2,
    )

    session_dir = Path(result["session_dir"])
    assert session_dir.exists()
    required_files = (
        "run_manifest.json",
        "input_resolution.json",
        "bad_pick_cohort_summary.json",
        "bad_pick_cases.parquet",
        "boundary_near_miss_comparison.parquet",
        "root_cause_taxonomy_summary.json",
        "feature_contrast_summary.json",
        "veto_hypothesis_backlog.json",
        "audit_decision.json",
        "_ARTIFACT_COMPLETE.json",
    )
    for file_name in required_files:
        assert (session_dir / file_name).exists()

    manifest = json.loads((session_dir / "run_manifest.json").read_text(encoding="utf-8"))
    input_resolution = json.loads((session_dir / "input_resolution.json").read_text(encoding="utf-8"))
    cohort = json.loads((session_dir / "bad_pick_cohort_summary.json").read_text(encoding="utf-8"))
    root_summary = json.loads((session_dir / "root_cause_taxonomy_summary.json").read_text(encoding="utf-8"))
    feature_contrast = json.loads((session_dir / "feature_contrast_summary.json").read_text(encoding="utf-8"))
    decision = json.loads((session_dir / "audit_decision.json").read_text(encoding="utf-8"))
    artifact = json.loads((session_dir / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))

    assert manifest["schema_version"] == "tradex_bad_pick_root_cause_audit_v1_manifest_v1"
    assert manifest["same_condition_contract"]["candidate_universe"] == "integrated_guarded_v1 champion top20 audit surface"
    assert input_resolution["selected_candidate_source"]["kind"] == "prefilter_rows_parquet"
    assert input_resolution["authoritative_for_audit"] is True
    assert cohort["top5_bad_pick_count"] >= 0
    assert cohort["top10_bad_pick_count"] >= cohort["top5_bad_pick_count"]
    assert root_summary["bad_pick_count"] == cohort["bad_pick_count"]
    assert feature_contrast["top5_summary"]["bad_count"] >= 0
    assert decision["decision"] in {"ready_for_veto_candidate_design", "needs_more_input_data", "insufficient_signal"}
    assert artifact["parse_status"]["run_manifest"] is True
    assert artifact["row_reconciliation"]["selected_rows_have_feature_coverage"] is True

    bad_cases = pd.read_parquet(session_dir / "bad_pick_cases.parquet")
    boundary = pd.read_parquet(session_dir / "boundary_near_miss_comparison.parquet")
    assert not bad_cases.empty
    assert not boundary.empty
    assert {"root_cause_code", "root_cause_confidence", "evidence_fields_used", "best_near_miss_rank"}.issubset(set(bad_cases.columns))
    assert {"score_gap", "forward_ret_20d_gap", "path_value_gap", "boundary_candidate_count"}.issubset(set(boundary.columns))
