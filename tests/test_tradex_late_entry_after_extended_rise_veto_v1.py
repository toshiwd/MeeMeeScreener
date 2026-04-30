from __future__ import annotations

from pathlib import Path

import pandas as pd

from scripts.tradex_late_entry_after_extended_rise_veto_v1 import (
    _apply_variant_ranking,
    _late_entry_veto_mask,
    build_artifacts,
)


def test_late_entry_veto_mask_matches_narrow_point_in_time_condition() -> None:
    frame = pd.DataFrame(
        [
            {
                "side": "long",
                "dominant_regime_context": "C:risk_on_trend",
                "monthly_context": "monthly_overextended",
                "weekly_context": "weekly_overextended",
                "monthly_context_no_lookahead": True,
                "weekly_context_no_lookahead": True,
                "daily_main_state_ctx": "daily_reversal_up_candidate",
                "dist_ma20_pct": 0.05,
                "dist_ma60_pct": 0.01,
            },
            {
                "side": "long",
                "dominant_regime_context": "C:risk_on_trend",
                "monthly_context": "monthly_range",
                "weekly_context": "weekly_overextended",
                "monthly_context_no_lookahead": True,
                "weekly_context_no_lookahead": True,
                "daily_main_state_ctx": "daily_reversal_up_candidate",
                "dist_ma20_pct": 0.05,
                "dist_ma60_pct": 0.01,
            },
        ]
    )
    mask = _late_entry_veto_mask(frame)
    assert mask.tolist() == [True, False]


def test_variant_ranking_drop_and_deprioritize() -> None:
    frame = pd.DataFrame(
        [
            {
                "anchor_date": "2026-01-01",
                "side": "long",
                "symbol": "A",
                "candidate_rank": 1,
                "score": 0.9,
                "champion_selected_top5": True,
                "champion_selected_top10": True,
                "champion_selected_top20": True,
                "late_entry_veto_flag": False,
            },
            {
                "anchor_date": "2026-01-01",
                "side": "long",
                "symbol": "B",
                "candidate_rank": 2,
                "score": 0.8,
                "champion_selected_top5": True,
                "champion_selected_top10": True,
                "champion_selected_top20": True,
                "late_entry_veto_flag": True,
            },
            {
                "anchor_date": "2026-01-01",
                "side": "long",
                "symbol": "C",
                "candidate_rank": 3,
                "score": 0.7,
                "champion_selected_top5": True,
                "champion_selected_top10": True,
                "champion_selected_top20": True,
                "late_entry_veto_flag": False,
            },
            {
                "anchor_date": "2026-01-01",
                "side": "long",
                "symbol": "D",
                "candidate_rank": 4,
                "score": 0.6,
                "champion_selected_top5": True,
                "champion_selected_top10": True,
                "champion_selected_top20": True,
                "late_entry_veto_flag": False,
            },
            {
                "anchor_date": "2026-01-01",
                "side": "long",
                "symbol": "E",
                "candidate_rank": 5,
                "score": 0.5,
                "champion_selected_top5": True,
                "champion_selected_top10": True,
                "champion_selected_top20": True,
                "late_entry_veto_flag": False,
            },
            {
                "anchor_date": "2026-01-01",
                "side": "long",
                "symbol": "F",
                "candidate_rank": 6,
                "score": 0.4,
                "champion_selected_top5": True,
                "champion_selected_top10": True,
                "champion_selected_top20": True,
                "late_entry_veto_flag": False,
            },
        ]
    )
    dropped = _apply_variant_ranking(frame, variant_name="late_entry_veto_drop", mode="drop", veto_col="late_entry_veto_flag")
    deprioritized = _apply_variant_ranking(frame, variant_name="late_entry_veto_deprioritize", mode="deprioritize", veto_col="late_entry_veto_flag")

    assert not bool(dropped.loc[dropped["symbol"] == "B", "late_entry_veto_drop_selected_top5"].iloc[0])
    assert bool(dropped.loc[dropped["symbol"] == "A", "late_entry_veto_drop_selected_top5"].iloc[0])
    assert bool(dropped.loc[dropped["symbol"] == "C", "late_entry_veto_drop_selected_top5"].iloc[0])

    assert bool(deprioritized.loc[deprioritized["symbol"] == "A", "late_entry_veto_deprioritize_selected_top5"].iloc[0])
    assert not bool(deprioritized.loc[deprioritized["symbol"] == "B", "late_entry_veto_deprioritize_selected_top5"].iloc[0])
    assert bool(deprioritized.loc[deprioritized["symbol"] == "F", "late_entry_veto_deprioritize_selected_top5"].iloc[0])


def test_build_artifacts_smoke_contract(tmp_path: Path) -> None:
    result = build_artifacts(
        candidate_input_dir=Path(r"G:\Tradex\candidate_generation_pre_filter_context_shape_v1\20260429T145332Z-7bd554ac"),
        audit_session=Path(r"G:\Tradex\bad_pick_root_cause_audit\20260429T155546Z-2053e5e4"),
        boundary_session=Path(r"G:\Tradex\bad_pick_root_cause_audit\20260429T155546Z-2053e5e4"),
        policy_ledger_path=Path(r"G:\Tradex\sample_replays\tradex_integrated_guarded_v1_stress200\integrated_guarded_v1_policy_trade_ledger.json"),
        output_root=tmp_path,
        limit_anchor_dates=1,
    )
    session_dir = Path(result["session_dir"])
    assert session_dir.exists()
    required = [
        "run_manifest.json",
        "input_resolution.json",
        "veto_policy.json",
        "candidate_veto_rows.parquet",
        "veto_pool_comparison.json",
        "monthly_comparison.json",
        "context_comparison.json",
        "topk_membership_diff.parquet",
        "veto_precision_recall_summary.json",
        "late_entry_after_extended_rise_veto_v1_decision.json",
        "_ARTIFACT_COMPLETE.json",
    ]
    for name in required:
        assert (session_dir / name).exists()
    assert result["decision"]["decision"] in {"keep", "hold", "drop"}
    assert pd.read_parquet(session_dir / "candidate_veto_rows.parquet").shape[0] > 0
    assert pd.read_parquet(session_dir / "topk_membership_diff.parquet").shape[0] >= 0
