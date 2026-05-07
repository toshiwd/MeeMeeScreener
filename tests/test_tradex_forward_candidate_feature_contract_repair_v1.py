from __future__ import annotations

import tempfile
from pathlib import Path

import duckdb
import pandas as pd

from scripts.tradex_forward_candidate_feature_contract_repair_v1 import (
    MODEL_FROZEN_FEATURES,
    _decision_from_result,
    _month_to_date_monthly_fallback,
    run_repair,
)


def test_decision_from_result_uses_allowed_labels() -> None:
    incomplete = {"feature_contract_complete": False}
    comparison = {"summary": {"branching_happened": False}}
    assert _decision_from_result(incomplete, comparison) == "candidate_feature_contract_incomplete"

    complete = {"feature_contract_complete": True, "score_delta_non_zero_count": 0, "scores_identical": True}
    assert _decision_from_result(complete, comparison) == "model_rescoring_valid_but_no_branching"

    branching = {"feature_contract_complete": True, "score_delta_non_zero_count": 3, "scores_identical": False, "candidate_row_count": 53}
    assert _decision_from_result(branching, {"summary": {"branching_happened": True}}) == "needs_full_surface_generation_after_repair"

    larger_branching = {"feature_contract_complete": True, "score_delta_non_zero_count": 3, "scores_identical": False, "candidate_row_count": 128}
    assert _decision_from_result(larger_branching, {"summary": {"branching_happened": True}}) == "candidate_feature_contract_repaired"


def test_run_repair_builds_feature_complete_candidate_surface() -> None:
    base = Path(r"G:\Tradex")
    with tempfile.TemporaryDirectory(dir=base) as tmpdir:
        tmp_path = Path(tmpdir)
        result = run_repair(
            output_root=tmp_path / "repair",
            feature_complete_root=tmp_path / "complete",
            jobs=1,
        )

        repair_dir = Path(result["repair_dir"])
        feature_complete_dir = Path(result["feature_complete_dir"])
        feature_complete_path = feature_complete_dir / "candidate_prefilter_rows_batch2_volume_feature_complete_v1.parquet"
        rescoring_path = repair_dir / "candidate_complete_rescoring_rows.parquet"
        summary_path = repair_dir / "candidate_complete_rescoring_summary.json"
        decision_path = repair_dir / "forward_candidate_feature_contract_repair_v1_decision.json"
        no_lookahead_path = repair_dir / "candidate_feature_no_lookahead_audit.json"

        assert result["candidate_row_count"] == 53
        assert result["feature_contract_complete"] is True
        assert result["no_lookahead_passed"] is True
        assert result["decision"] in {
            "candidate_feature_contract_repaired",
            "model_rescoring_valid_but_no_branching",
            "needs_full_surface_generation_after_repair",
        }

        for path in [
            feature_complete_path,
            rescoring_path,
            summary_path,
            decision_path,
            no_lookahead_path,
        ]:
            assert path.exists(), path

        feature_complete = pd.read_parquet(feature_complete_path)
        assert len(feature_complete) == 53
        for feature in MODEL_FROZEN_FEATURES:
            assert feature in feature_complete.columns
            assert feature_complete[feature].notna().all(), feature

        rescored = pd.read_parquet(rescoring_path)
        assert len(rescored) == 53
        assert "tree_hgb_path_value_score" in rescored.columns
        assert "champion_original_score" in rescored.columns
        assert "effective_rank_score" in rescored.columns
        assert rescored["tree_hgb_path_value_score"].notna().all()
        assert rescored["effective_rank_score"].equals(rescored["tree_hgb_path_value_score"])


def test_month_to_date_monthly_fallback_fills_missing_monthly_ohlc() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "fallback.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute(
            """
            CREATE TABLE daily_bars (
                code VARCHAR,
                date BIGINT,
                o DOUBLE,
                h DOUBLE,
                l DOUBLE,
                c DOUBLE,
                v BIGINT
            )
            """
        )
        conn.executemany(
            "INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                ("8572", 1721779200, 100.0, 104.0, 99.0, 103.0, 1000),
                ("8572", 1721865600, 103.0, 105.0, 101.0, 104.0, 1100),
                ("8572", 1721779200 + 86400 * 2, 104.0, 108.0, 102.0, 107.0, 1200),
            ],
        )
        frame = pd.DataFrame(
            [
                {
                    "_row_id": 0,
                    "symbol": "8572",
                    "anchor_dt": 1721779200 + 86400 * 2,
                }
            ]
        )

        fallback = _month_to_date_monthly_fallback(frame, conn)

        assert len(fallback) == 1
        assert fallback.loc[0, "monthly_o_fallback"] == 100.0
        assert fallback.loc[0, "monthly_h_fallback"] == 108.0
        assert fallback.loc[0, "monthly_l_fallback"] == 99.0
        assert fallback.loc[0, "monthly_c_fallback"] == 107.0
        conn.close()
