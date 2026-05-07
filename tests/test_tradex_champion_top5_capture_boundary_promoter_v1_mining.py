from __future__ import annotations

from pathlib import Path

import pandas as pd

from scripts.tradex_champion_top5_capture_boundary_promoter_v1_mining import _build_mining_outputs


def _build_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "anchor_date": ["2025-01-03"] * 20,
            "side": ["long"] * 20,
            "month_bucket": ["2025-01"] * 20,
            "symbol": [f"S{i:02d}" for i in range(20)],
            "score": [100 - i for i in range(20)],
            "champion_rank": list(range(1, 21)),
            "forward_ret_20d": [1, 2, 3, 4, 5, 20, 19, 18, 17, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            "champion_selected_top20": [True] * 20,
            "monthly_context": ["monthly_range"] * 20,
            "weekly_context": ["weekly_range"] * 20,
            "shape_classification": ["shape_positive_modifier"] * 20,
            "family_regime_context": ["C:risk_on_range"] * 20,
            "gap_pct": [0.01] * 20,
            "vol_ratio5_20": [1.2] * 20,
            "candle_body_ratio": [0.4] * 20,
            "path_value_score_v1": [0.2] * 20,
        }
    )


def test_mining_outputs_stay_mining_only_and_classify_top5_boundary_opportunity(tmp_path: Path) -> None:
    payload = _build_mining_outputs(_build_frame(), output_root=tmp_path, source_rows_parquet=Path(r"G:\Tradex\source.parquet"))

    mining_contract = payload["artifact_paths"]["mining_contract.json"]
    next_design = payload["next_candidate_design"]
    monthly = payload["monthly_capture_baseline_summary"]
    missed_inventory = payload["missed_top5_winner_inventory"]
    opportunity = payload["rank6_20_promotion_opportunity_summary"]
    feature_contrast = payload["feature_contrast_summary"]

    assert Path(mining_contract).exists()
    assert next_design["decision"] == "proceed_to_candidate"
    assert next_design["candidate_id"] == "champion_top5_capture_boundary_promoter_v1"
    assert payload["artifact_paths"]["mining_contract.json"]
    assert payload["artifact_paths"]["next_candidate_design.json"]
    assert payload["artifact_paths"]["report.md"]
    assert payload["artifact_paths"]["_ARTIFACT_COMPLETE.json"]
    assert "adjusted_rank" not in next_design
    assert "realized_top5_label" not in feature_contrast["groups"]["champion_top5_miss_but_rank6_20"]["features"]
    assert "realized_top5_label" not in feature_contrast["groups"]["champion_top5_false_positive"]["features"]
    assert missed_inventory["record_count"] == 5
    assert monthly["months_evaluated"] == 1
    assert monthly["missed_realized_top5_winners_in_rank6_20_total"] == 5
    assert monthly["missed_realized_top5_winners_outside_top20_total"] == 0
    assert opportunity["broad_or_concentrated"] == "concentrated"
    assert opportunity["rank6_20_opportunity_rate"] == 1.0
    assert feature_contrast["groups"]["champion_top5_miss_outside_top20"]["count"] == 0

    for record in missed_inventory["records"]:
        assert record["rank6_20_pool_label"] is True
        assert record["realized_top5_label"] is True
        assert "adjusted_rank" not in record
