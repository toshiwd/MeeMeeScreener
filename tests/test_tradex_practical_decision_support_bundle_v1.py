from __future__ import annotations

import json

import pandas as pd

from scripts import tradex_practical_decision_support_bundle_v1 as mod


def _row(as_of: int, code: str, *, positive: bool) -> dict:
    row = {name: 0.01 for name in mod.base.FEATURES}
    row.update({name: False for name in mod.ADDITIONAL_FEATURES})
    row.update({
        "as_of_date": as_of, "code": code,
        "winner_ret20_gt_10pct": positive,
        "bad_ret20_lt_minus_5pct": not positive,
        "severe_ret20_lt_minus_10pct": not positive,
        "ret20": 0.2 if positive else -0.1,
        "recent_high_distance_pct": -0.1,
        "volume_vs_20d_avg": 1.0,
        "weekly_supportive_flag": positive,
        "monthly_supportive_flag": positive,
        "high_upside_reserve_reference_match": positive,
    })
    return row


def test_bundle_selects_features_and_emits_review_board(tmp_path):
    rows = []
    for year in [2021, 2022, 2023, 2024]:
        for index in range(20):
            rows.append(_row(year * 10000 + 101 + index, f"{year}-{index}", positive=index % 2 == 0))
    source = tmp_path / "source.parquet"
    pd.DataFrame(rows).to_parquet(source, index=False)
    output = mod.run(source_path=source, output_root=tmp_path / "out")
    audit = json.loads((output / "feature_selection_audit.json").read_text())
    metrics = json.loads((output / "decision_support_metrics.json").read_text())
    decision = json.loads((output / "research_decision.json").read_text())
    board = pd.read_csv(output / "latest_review_board.csv")

    assert audit["selected_feature_set"] in {"baseline", "expanded"}
    assert metrics["latest_board_row_count"] == 1
    assert set(board["review_bucket"]).issubset({"Starter", "Watch", "Wait", "Avoid"})
    assert decision["decision_class"] == "READY_REVIEW_ONLY"
    assert decision["current_recommendation_allowed"] is False
    assert decision["runtime_db_write"] is False
