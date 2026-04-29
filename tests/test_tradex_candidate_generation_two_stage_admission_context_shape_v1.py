from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.tradex_candidate_generation_two_stage_admission_context_shape_v1 import (
    _build_context_comparison,
    _group_rows_for_comparison,
    _select_pool,
    write_artifacts,
)


PREFILTER_SESSION = Path(r"G:\Tradex\candidate_generation_pre_filter_context_shape_v1\20260429T145332Z-7bd554ac")
CANDIDATE_INPUT_DIR = Path(r"G:\Tradex\sample_replays\tradex_integrated_guarded_v1_stress200")
FAMILY_SESSION = Path(r"G:\Tradex\ma_position_path_research_family_filter\20260429T062945Z-87844c56")
CONTEXT_SESSION = Path(r"G:\Tradex\multi_timeframe_conditional_state_value_v1\20260429T091138Z-7d26cb7c")
SHAPE_SESSION = Path(r"G:\Tradex\conditional_high_value_candle_shape_modifier_v1\20260429T105018Z-26bc381e")
FREEZE_SESSION = Path(r"G:\Tradex\research_freeze_summaries\ma_context_shape_direct_adjustment_line\20260429T143302Z-8f34ef9d")


def test_two_stage_backfill_prefers_primary_then_watch() -> None:
    frame = pd.DataFrame(
        [
            {"anchor_date": "2024-01-01", "side": "long", "symbol": "AAA", "score": 9.0, "rank": 1, "candidate_idx": 10, "admission_stage": "PRIMARY"},
            {"anchor_date": "2024-01-01", "side": "long", "symbol": "AAB", "score": 8.0, "rank": 2, "candidate_idx": 11, "admission_stage": "WATCH"},
            {"anchor_date": "2024-01-01", "side": "long", "symbol": "AAC", "score": 7.0, "rank": 3, "candidate_idx": 12, "admission_stage": "WATCH"},
            {"anchor_date": "2024-01-01", "side": "short", "symbol": "BAA", "score": 6.0, "rank": 1, "candidate_idx": 13, "admission_stage": "PRIMARY"},
            {"anchor_date": "2024-01-01", "side": "short", "symbol": "BAB", "score": 5.0, "rank": 2, "candidate_idx": 14, "admission_stage": "PRIMARY"},
            {"anchor_date": "2024-01-01", "side": "short", "symbol": "BAC", "score": 4.0, "rank": 3, "candidate_idx": 15, "admission_stage": "WATCH"},
        ]
    )

    flags, group_rows = _select_pool(frame, top_k=2, pool_mode="primary_watch_backfill")
    selected = frame.loc[flags, ["anchor_date", "side", "symbol", "admission_stage"]].sort_values(["anchor_date", "side", "symbol"]).reset_index(drop=True)

    assert selected.to_dict("records") == [
        {"anchor_date": "2024-01-01", "side": "long", "symbol": "AAA", "admission_stage": "PRIMARY"},
        {"anchor_date": "2024-01-01", "side": "long", "symbol": "AAB", "admission_stage": "WATCH"},
        {"anchor_date": "2024-01-01", "side": "short", "symbol": "BAA", "admission_stage": "PRIMARY"},
        {"anchor_date": "2024-01-01", "side": "short", "symbol": "BAB", "admission_stage": "PRIMARY"},
    ]

    rows = {(row["anchor_date"], row["side"]): row for row in group_rows.to_dict("records")}
    assert rows[("2024-01-01", "long")]["primary_selected_count"] == 1
    assert rows[("2024-01-01", "long")]["watch_selected_count"] == 1
    assert rows[("2024-01-01", "long")]["backfill_group"] == 1
    assert rows[("2024-01-01", "short")]["primary_selected_count"] == 2
    assert rows[("2024-01-01", "short")]["watch_selected_count"] == 0


def test_group_rows_use_matching_top_k_for_original_and_challenger() -> None:
    frame = pd.DataFrame(
        [
            {
                "monthly_context": "monthly_uptrend",
                "weekly_context": "weekly_pullback",
                "dominant_regime_context": "C:risk_on_trend",
                "anchor_date": "2024-01-01",
                "side": "long",
                "symbol": "AAA",
                "forward_ret_20d": 0.10,
                "forward_ret_10d": 0.08,
                "path_value_score_v1": 0.50,
                "mfe_20d": 0.9,
                "mae_20d": 0.1,
                "family_classification": "stable_high_value_family",
                "stable_bad_pick_family": False,
                "original_selected_top5": True,
                "original_selected_top20": False,
                "primary_only_selected_top5": False,
                "primary_only_selected_top20": True,
            },
            {
                "monthly_context": "monthly_uptrend",
                "weekly_context": "weekly_pullback",
                "dominant_regime_context": "C:risk_on_trend",
                "anchor_date": "2024-01-01",
                "side": "long",
                "symbol": "AAB",
                "forward_ret_20d": 0.20,
                "forward_ret_10d": 0.16,
                "path_value_score_v1": 0.80,
                "mfe_20d": 1.1,
                "mae_20d": 0.2,
                "family_classification": "stable_high_value_family",
                "stable_bad_pick_family": False,
                "original_selected_top5": False,
                "original_selected_top20": True,
                "primary_only_selected_top5": True,
                "primary_only_selected_top20": False,
            },
        ]
    )

    rows_top5 = _group_rows_for_comparison(
        frame=frame,
        selected_prefix="primary_only",
        original_top_k=5,
        selected_top_k=5,
        group_cols=["monthly_context"],
        bottom15_threshold=-1.0,
        top15_threshold=0.0,
    )
    rows_top20 = _group_rows_for_comparison(
        frame=frame,
        selected_prefix="primary_only",
        original_top_k=20,
        selected_top_k=20,
        group_cols=["monthly_context"],
        bottom15_threshold=-1.0,
        top15_threshold=0.0,
    )

    assert rows_top5[0]["original"]["mean_path_value_score_v1"] != rows_top20[0]["original"]["mean_path_value_score_v1"]
    assert rows_top5[0]["challenger"]["mean_path_value_score_v1"] != rows_top20[0]["challenger"]["mean_path_value_score_v1"]


def test_two_stage_smoke_run_writes_required_artifacts(tmp_path: Path) -> None:
    output_root = tmp_path / "candidate_generation_two_stage_admission_context_shape"
    session_dir = write_artifacts(
        output_root=output_root,
        prefilter_session=PREFILTER_SESSION,
        candidate_input_dir=CANDIDATE_INPUT_DIR,
        family_session=FAMILY_SESSION,
        context_session=CONTEXT_SESSION,
        shape_session=SHAPE_SESSION,
        freeze_session=FREEZE_SESSION,
        limit_anchor_dates=2,
    )

    assert session_dir.exists()
    required_files = (
        "run_manifest.json",
        "two_stage_admission_policy.json",
        "candidate_stage_coverage_summary.json",
        "candidate_pool_comparison.json",
        "monthly_comparison.json",
        "context_comparison.json",
        "topk_membership_diff.parquet",
        "candidate_two_stage_rows.parquet",
        "candidate_generation_two_stage_admission_context_shape_v1_decision.json",
        "_ARTIFACT_COMPLETE.json",
    )
    for file_name in required_files:
        assert (session_dir / file_name).exists()

    manifest = json.loads((session_dir / "run_manifest.json").read_text(encoding="utf-8"))
    policy = json.loads((session_dir / "two_stage_admission_policy.json").read_text(encoding="utf-8"))
    coverage = json.loads((session_dir / "candidate_stage_coverage_summary.json").read_text(encoding="utf-8"))
    compare = json.loads((session_dir / "candidate_pool_comparison.json").read_text(encoding="utf-8"))
    context = json.loads((session_dir / "context_comparison.json").read_text(encoding="utf-8"))
    decision = json.loads((session_dir / "candidate_generation_two_stage_admission_context_shape_v1_decision.json").read_text(encoding="utf-8"))

    assert manifest["schema_version"] == "tradex_candidate_generation_two_stage_admission_context_shape_v1_manifest_v1"
    assert policy["schema_version"] == "tradex_candidate_generation_two_stage_admission_context_shape_v1_policy_v1"
    assert coverage["no_lookahead_inherited"] is True
    assert coverage["backfill_by_topk"]["5"]["zero_pass_group_count"] == 0
    assert compare["same_condition_contract"]["two_stage_admission"] is True
    assert set(context["topk"].keys()) == {"5", "10", "20"}
    assert decision["authoritative_rollup_decision"] in {"keep", "hold", "drop"}

    parquet = pd.read_parquet(session_dir / "candidate_two_stage_rows.parquet")
    assert not parquet.empty
    assert {"admission_stage", "prefilter_bucket", "monthly_context", "weekly_context"}.issubset(set(parquet.columns))
    assert parquet["admission_stage"].isin({"PRIMARY", "WATCH", "DOWNGRADE", "EXCLUDE"}).all()
