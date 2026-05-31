from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.tradex_above20_streak_recent_long_quality_pretest_v1 import (
    REQUIRED_ARTIFACTS,
    above20_bucket,
    decide,
    load_and_score,
    run,
    score_boost,
)


def test_above20_bucket_and_fixed_boost_policy() -> None:
    assert above20_bucket(0) == "bucket_0"
    assert above20_bucket(5) == "bucket_1"
    assert above20_bucket(10) == "bucket_2"
    assert above20_bucket(20) == "bucket_3"
    assert above20_bucket(40) == "bucket_4"
    assert score_boost("bucket_0") == 0
    assert score_boost("bucket_2") == 1
    assert score_boost("bucket_3") == 2
    assert score_boost("bucket_4") == 1


def test_load_and_score_reranks_within_same_decision_date(tmp_path: Path) -> None:
    input_root = tmp_path
    rows = pd.DataFrame(
        [
            {"decision_ymd": 20240101, "code": "A", "candidate_rank": 1, "selection_score": 10, "selected_for_buy": True, "source_year": 2024, "year": 2024, "period_bucket": "recent_2024_2026", "above20_streak": 0, "days_since_ma20_reclaim": 0, "ret20": 0.01, "ret40": 0.01, "mae20": -0.01, "mfe20": 0.02, "max_drawdown_20": -0.01},
            {"decision_ymd": 20240101, "code": "B", "candidate_rank": 2, "selection_score": 9, "selected_for_buy": True, "source_year": 2024, "year": 2024, "period_bucket": "recent_2024_2026", "above20_streak": 25, "days_since_ma20_reclaim": 10, "ret20": 0.04, "ret40": 0.05, "mae20": -0.01, "mfe20": 0.06, "max_drawdown_20": -0.01},
        ]
    )
    rows.to_csv(input_root / "candidate_rows_with_features.csv", index=False)

    scored = load_and_score(input_root)

    b = scored[scored["code"] == "B"].iloc[0]
    assert b["challenger_score"] == 11
    assert b["challenger_rank"] == 1


def test_decide_drops_without_meaningful_recent_improvement() -> None:
    empty_metric = {
        "baseline": {"mean_ret20": 0.02, "median_ret20": 0.02, "severe_loss_rate_ret20_lte_minus_5pct": 0.1},
        "challenger": {"mean_ret20": 0.019, "median_ret20": 0.02, "severe_loss_rate_ret20_lte_minus_5pct": 0.1},
        "delta_mean_ret20": -0.001,
        "delta_median_ret20": 0.0,
        "delta_severe_loss_rate": 0.0,
        "changed_top5_members_count": 1,
        "changed_top10_members_count": 1,
        "changed_top20_members_count": 1,
        "added_minus_removed_ret20": -0.01,
    }
    summary = {
        "2024_2025_combined": {"top5": empty_metric, "top10": empty_metric, "top20": empty_metric},
        "2019_2023_combined": {"top5": empty_metric, "top10": empty_metric, "top20": empty_metric},
    }

    decision = decide(summary)

    assert decision["research_decision"] == "drop"


def test_run_writes_required_artifacts(tmp_path: Path) -> None:
    input_root = tmp_path / "input"
    output_root = tmp_path / "output"
    input_root.mkdir()
    rows = []
    for year in [2024, 2025, 2023]:
        for day in [1, 2]:
            ymd = int(f"{year}010{day}")
            for idx in range(1, 8):
                rows.append(
                    {
                        "decision_ymd": ymd,
                        "code": str(1000 + idx),
                        "candidate_rank": idx,
                        "selection_score": 20 - idx,
                        "selected_for_buy": idx <= 3,
                        "source_year": year,
                        "year": year,
                        "period_bucket": "recent_2024_2026" if year >= 2024 else "pre_recent_2019_2023",
                        "above20_streak": idx * 5,
                        "days_since_ma20_reclaim": idx,
                        "ret20": 0.01 * idx,
                        "ret40": 0.01 * idx,
                        "mae20": -0.01,
                        "mfe20": 0.02,
                        "max_drawdown_20": -0.01,
                    }
                )
    pd.DataFrame(rows).to_csv(input_root / "candidate_rows_with_features.csv", index=False)
    (input_root / "pattern_shift_matrix.csv").write_text("feature\nabove20_streak\n", encoding="utf-8")
    (input_root / "next_challenger_candidates.json").write_text(json.dumps({"recommended_single_axis": "above20_streak_recent_long_quality_pretest"}), encoding="utf-8")
    (input_root / "no_lookahead_audit.json").write_text(json.dumps({"audit_result": "pass"}), encoding="utf-8")

    result = run(input_root=input_root, output_root=output_root)

    out = Path(result["output_dir"])
    assert all((out / artifact).exists() for artifact in REQUIRED_ARTIFACTS)
    assert json.loads((out / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))["artifact_complete"] is True
