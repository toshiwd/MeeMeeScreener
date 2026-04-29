from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.tradex_multi_timeframe_conditional_state_value_v1 import (
    _classify_conditional_group,
    run_multi_timeframe_conditional_state_value_v1,
)


SOURCE_FAMILY_SESSION = Path(r"G:\Tradex\ma_position_path_research_family_filter\20260429T062945Z-87844c56")


def test_conditional_group_classifier_handles_high_value_and_bad_pick() -> None:
    high_value_row = pd.Series(
        {
            "sample_count": 140,
            "unique_symbol_count": 40,
            "month_count": 12,
            "mean_path_value_score_v1": 0.052,
            "median_path_value_score_v1": 0.011,
            "plus5_before_minus5_rate": 0.51,
            "minus5_before_plus5_rate": 0.31,
            "bottom15_rate": 0.12,
            "positive_month_rate": 0.61,
            "regime_count": 2,
            "regime_consistency_score": 0.82,
            "score_spread": 0.03,
        }
    )
    bad_pick_row = pd.Series(
        {
            "sample_count": 120,
            "unique_symbol_count": 34,
            "month_count": 10,
            "mean_path_value_score_v1": -0.012,
            "median_path_value_score_v1": -0.004,
            "plus5_before_minus5_rate": 0.32,
            "minus5_before_plus5_rate": 0.51,
            "bottom15_rate": 0.17,
            "positive_month_rate": 0.42,
            "regime_count": 2,
            "regime_consistency_score": 0.64,
            "score_spread": 0.04,
        }
    )

    thresholds = {
        "min_sample_count": 100,
        "min_unique_symbol_count": 20,
        "min_month_count": 8,
        "baseline_mean_path_value_score_v1": 0.02,
        "baseline_median_path_value_score_v1": 0.0,
        "baseline_plus5_before_minus5_rate": 0.44,
        "baseline_minus5_before_plus5_rate": 0.42,
        "baseline_bottom15_rate": 0.15,
        "baseline_top15_rate": 0.15,
    }

    assert _classify_conditional_group(high_value_row, thresholds=thresholds) == "conditional_high_value"
    assert _classify_conditional_group(bad_pick_row, thresholds=thresholds) == "conditional_bad_pick"


def test_multi_timeframe_conditional_state_value_smoke_run(tmp_path: Path) -> None:
    output_root = tmp_path / "multi_timeframe_conditional"
    result = run_multi_timeframe_conditional_state_value_v1(
        source_family_session=SOURCE_FAMILY_SESSION,
        output_root=output_root,
        limit_codes=2,
    )

    session_dir = Path(result["session_dir"])
    assert session_dir.exists()

    required_files = (
        "run_manifest.json",
        "context_definition.json",
        "conditional_state_value_summary.json",
        "conditional_state_value_by_monthly.json",
        "conditional_state_value_by_weekly.json",
        "conditional_state_classification.json",
        "global_vs_conditional_comparison.json",
        "multi_timeframe_conditional_state_value_v1_decision.json",
        "conditional_state_rows.parquet",
        "_ARTIFACT_COMPLETE.json",
    )
    for file_name in required_files:
        assert (session_dir / file_name).exists()

    manifest = json.loads((session_dir / "run_manifest.json").read_text(encoding="utf-8"))
    context_definition = json.loads((session_dir / "context_definition.json").read_text(encoding="utf-8"))
    summary = json.loads((session_dir / "conditional_state_value_summary.json").read_text(encoding="utf-8"))
    monthly_payload = json.loads((session_dir / "conditional_state_value_by_monthly.json").read_text(encoding="utf-8"))
    weekly_payload = json.loads((session_dir / "conditional_state_value_by_weekly.json").read_text(encoding="utf-8"))
    classification = json.loads((session_dir / "conditional_state_classification.json").read_text(encoding="utf-8"))
    comparison = json.loads((session_dir / "global_vs_conditional_comparison.json").read_text(encoding="utf-8"))
    decision = json.loads((session_dir / "multi_timeframe_conditional_state_value_v1_decision.json").read_text(encoding="utf-8"))

    assert manifest["schema_version"] == "tradex_multi_timeframe_conditional_state_value_v1_manifest_v1"
    assert manifest["no_lookahead_inherited"] is True
    assert manifest["monthly_context_no_lookahead"] is True
    assert manifest["weekly_context_no_lookahead"] is True
    assert manifest["source_family_session_id"] == "20260429T062945Z-87844c56"
    assert summary["row_count"] > 0
    assert summary["code_count"] > 0
    assert summary["monthly_context_count"] > 0
    assert summary["weekly_context_count"] > 0
    assert context_definition["confirmed_vs_provisional_fields"]["confirmed_monthly_fields"] == ["monthly_bars", "monthly_ma"]
    assert context_definition["confirmed_vs_provisional_fields"]["provisional_weekly_fields"] == [
        "weekly_bars_derived_from_daily_bars",
        "weekly_ma_derived_from_daily_bars",
    ]
    assert monthly_payload["monthly_contexts"]
    assert weekly_payload["weekly_contexts"]
    assert comparison["monthly_context_level"]["total_groups"] >= 0
    assert comparison["weekly_context_level"]["total_groups"] >= 0
    assert comparison["triple_level"]["total_groups"] >= 0
    assert decision["recommendation"] in {"keep", "hold", "drop"}
    assert decision["source_family_session_id"] == "20260429T062945Z-87844c56"

    parquet = pd.read_parquet(session_dir / "conditional_state_rows.parquet")
    assert not parquet.empty
    assert {"state_family_id", "monthly_context", "weekly_context", "path_value_score_v1"}.issubset(set(parquet.columns))

