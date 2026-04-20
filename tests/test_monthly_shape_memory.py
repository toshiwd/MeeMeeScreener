from __future__ import annotations

import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pandas as pd

from external_analysis import monthly_shape_memory as msm


def _seed_daily_bars(db_path: Path) -> None:
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE daily_bars (
                code TEXT,
                date INTEGER,
                o DOUBLE,
                h DOUBLE,
                l DOUBLE,
                c DOUBLE,
                v BIGINT
            )
            """
        )
        start = datetime(2021, 1, 4, tzinfo=timezone.utc)
        codes = [f"{1300 + idx}" for idx in range(12)]
        base_trends = {
            code: (-0.0018 + (idx * 0.0004))
            for idx, code in enumerate(codes)
        }
        for month_idx in range(18):
            month_start = (pd.Timestamp(start) + pd.DateOffset(months=month_idx)).to_pydatetime()
            month_end = (pd.Timestamp(month_start) + pd.offsets.MonthEnd(0)).to_pydatetime()
            current = month_start
            month_days: list[datetime] = []
            while current <= month_end:
                if current.weekday() < 5:
                    month_days.append(current)
                current += timedelta(days=1)
            for code_idx, code in enumerate(codes):
                base_price = 100.0 + (code_idx * 7.5)
                trend = base_trends[code] + math.sin((month_idx + 1) / 3.0) * 0.002
                price = base_price * (1.0 + month_idx * 0.008 + code_idx * 0.003)
                for day_idx, day in enumerate(month_days):
                    day_bias = trend + math.sin((day_idx + code_idx) / 4.0) * 0.0015
                    open_price = price * (1.0 - 0.001 + (day_idx % 3) * 0.0002)
                    close_price = price * (1.0 + day_bias)
                    high_price = max(open_price, close_price) * 1.01
                    low_price = min(open_price, close_price) * 0.99
                    volume = int(1_000_000 + (code_idx * 75_000) + month_idx * 18_000 + day_idx * 2_000)
                    conn.execute(
                        "INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?)",
                        [
                            code,
                            int(day.strftime("%Y%m%d")),
                            float(open_price),
                            float(high_price),
                            float(low_price),
                            float(close_price),
                            volume,
                        ],
                    )
                    price = close_price
    finally:
        conn.close()


def test_monthly_shape_memory_pipeline_writes_authoritative_artifacts(tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    _seed_daily_bars(source_db)

    result = msm.run_monthly_shape_memory_research(
        source_db_path=str(source_db),
        top_k=10,
        candidate_pool_k=12,
        analog_k=4,
        memory_lookback_months=6,
        rolling_window_months=6,
        start_month=202101,
    )

    summary_path = Path(result["summary_path"])
    split_path = Path(result["split_contract_path"])
    branch_path = Path(result["branch_eval_path"])
    compare_path = Path(result["compare_path"])
    decision_path = Path(result["decision_path"])
    samples_path = Path(result["samples_path"])
    labels_path = Path(result["labels_path"])
    similarity_path = Path(result["similarity_path"])
    hierarchical_dictionary_path = Path(result["hierarchical_dictionary_path"])
    hierarchical_rules_path = Path(result["hierarchical_rules_path"])
    hierarchical_priority_path = Path(result["hierarchical_priority_path"])
    hierarchical_rows_path = Path(result["hierarchical_rows_path"])
    hierarchical_summary_path = Path(result["hierarchical_summary_path"])
    hierarchical_score_summary_path = Path(result["hierarchical_score_summary_path"])
    hierarchical_effect_by_state_path = Path(result["hierarchical_effect_by_state_path"])
    hierarchical_effect_by_regime_path = Path(result["hierarchical_effect_by_regime_path"])
    hierarchical_ablation_compare_path = Path(result["hierarchical_ablation_compare_path"])
    hierarchical_weekly_diversity_path = Path(result["hierarchical_weekly_diversity_path"])
    hierarchical_regime_gate_rules_path = Path(result["hierarchical_regime_gate_rules_path"])
    hierarchical_regime_gate_compare_path = Path(result["hierarchical_regime_gate_compare_path"])
    hierarchical_regime_gate_effect_by_regime_path = Path(result["hierarchical_regime_gate_effect_by_regime_path"])
    authoritative_regime_gate_decision_path = Path(result["authoritative_regime_gate_decision_path"])
    hierarchical_labels_keep_rerank_drop_path = Path(result["authoritative_decision_hierarchical_labels_keep_rerank_drop_path"])
    hierarchical_allowed_uses_path = Path(result["hierarchical_label_allowed_uses_path"])
    hierarchical_state_failure_map_path = Path(result["hierarchical_state_failure_map_path"])
    hierarchical_state_winner_loser_decomposition_path = Path(result["hierarchical_state_winner_loser_decomposition_path"])
    hierarchical_rerank_do_not_continue_path = Path(result["hierarchical_rerank_do_not_continue_path"])
    hierarchical_state_rank_impact_table_path = Path(result["hierarchical_state_rank_impact_table_path"])
    boundary_winner_promotion_rules_path = Path(result["boundary_winner_promotion_rules_path"])
    boundary_winner_promotion_compare_path = Path(result["boundary_winner_promotion_compare_path"])
    boundary_winner_promotion_effect_by_regime_path = Path(result["boundary_winner_promotion_effect_by_regime_path"])
    authoritative_boundary_winner_promotion_decision_path = Path(result["authoritative_boundary_winner_promotion_decision_path"])
    boundary_winner_promotion_drop_path = Path(result["boundary_winner_promotion_drop_path"])
    lightweight_boundary_challenger_candidates_path = Path(result["lightweight_boundary_challenger_candidates_path"])
    lightweight_boundary_challenger_rules_path = Path(result["lightweight_boundary_challenger_rules_path"])
    lightweight_boundary_challenger_compare_path = Path(result["lightweight_boundary_challenger_compare_path"])
    lightweight_boundary_challenger_effect_by_regime_path = Path(result["lightweight_boundary_challenger_effect_by_regime_path"])
    authoritative_lightweight_boundary_challenger_decision_path = Path(result["authoritative_lightweight_boundary_challenger_decision_path"])

    for path in (
        summary_path,
        split_path,
        branch_path,
        compare_path,
        decision_path,
        samples_path,
        labels_path,
        similarity_path,
        hierarchical_dictionary_path,
        hierarchical_rules_path,
        hierarchical_priority_path,
        hierarchical_rows_path,
        hierarchical_summary_path,
        hierarchical_score_summary_path,
        hierarchical_effect_by_state_path,
        hierarchical_effect_by_regime_path,
        hierarchical_ablation_compare_path,
        hierarchical_weekly_diversity_path,
        hierarchical_regime_gate_rules_path,
        hierarchical_regime_gate_compare_path,
        hierarchical_regime_gate_effect_by_regime_path,
        authoritative_regime_gate_decision_path,
        hierarchical_labels_keep_rerank_drop_path,
        hierarchical_allowed_uses_path,
        hierarchical_state_failure_map_path,
        hierarchical_state_winner_loser_decomposition_path,
        hierarchical_rerank_do_not_continue_path,
        hierarchical_state_rank_impact_table_path,
        boundary_winner_promotion_rules_path,
        boundary_winner_promotion_compare_path,
        boundary_winner_promotion_effect_by_regime_path,
        authoritative_boundary_winner_promotion_decision_path,
        boundary_winner_promotion_drop_path,
        lightweight_boundary_challenger_candidates_path,
        lightweight_boundary_challenger_rules_path,
        lightweight_boundary_challenger_compare_path,
        lightweight_boundary_challenger_effect_by_regime_path,
        authoritative_lightweight_boundary_challenger_decision_path,
    ):
        assert path.exists(), path

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    split_contract = json.loads(split_path.read_text(encoding="utf-8"))
    branch_eval = json.loads(branch_path.read_text(encoding="utf-8"))
    compare = json.loads(compare_path.read_text(encoding="utf-8"))
    decision = json.loads(decision_path.read_text(encoding="utf-8"))
    hierarchical_dictionary = json.loads(hierarchical_dictionary_path.read_text(encoding="utf-8"))
    hierarchical_rules = json.loads(hierarchical_rules_path.read_text(encoding="utf-8"))
    hierarchical_priority = json.loads(hierarchical_priority_path.read_text(encoding="utf-8"))
    hierarchical_summary = json.loads(hierarchical_summary_path.read_text(encoding="utf-8"))
    hierarchical_score_summary = json.loads(hierarchical_score_summary_path.read_text(encoding="utf-8"))
    hierarchical_effect_by_state = json.loads(hierarchical_effect_by_state_path.read_text(encoding="utf-8"))
    hierarchical_effect_by_regime = json.loads(hierarchical_effect_by_regime_path.read_text(encoding="utf-8"))
    hierarchical_ablation_compare = json.loads(hierarchical_ablation_compare_path.read_text(encoding="utf-8"))
    hierarchical_weekly_diversity = json.loads(hierarchical_weekly_diversity_path.read_text(encoding="utf-8"))
    hierarchical_regime_gate_rules = json.loads(hierarchical_regime_gate_rules_path.read_text(encoding="utf-8"))
    hierarchical_regime_gate_compare = json.loads(hierarchical_regime_gate_compare_path.read_text(encoding="utf-8"))
    hierarchical_regime_gate_effect_by_regime = json.loads(hierarchical_regime_gate_effect_by_regime_path.read_text(encoding="utf-8"))
    authoritative_regime_gate_decision = json.loads(authoritative_regime_gate_decision_path.read_text(encoding="utf-8"))
    hierarchical_labels_keep_rerank_drop = json.loads(hierarchical_labels_keep_rerank_drop_path.read_text(encoding="utf-8"))
    hierarchical_allowed_uses = json.loads(hierarchical_allowed_uses_path.read_text(encoding="utf-8"))
    hierarchical_state_failure_map = json.loads(hierarchical_state_failure_map_path.read_text(encoding="utf-8"))
    hierarchical_state_winner_loser_decomposition = json.loads(hierarchical_state_winner_loser_decomposition_path.read_text(encoding="utf-8"))
    hierarchical_rerank_do_not_continue = json.loads(hierarchical_rerank_do_not_continue_path.read_text(encoding="utf-8"))
    hierarchical_state_rank_impact_table = json.loads(hierarchical_state_rank_impact_table_path.read_text(encoding="utf-8"))
    boundary_winner_promotion_rules = json.loads(boundary_winner_promotion_rules_path.read_text(encoding="utf-8"))
    boundary_winner_promotion_compare = json.loads(boundary_winner_promotion_compare_path.read_text(encoding="utf-8"))
    boundary_winner_promotion_effect_by_regime = json.loads(boundary_winner_promotion_effect_by_regime_path.read_text(encoding="utf-8"))
    authoritative_boundary_winner_promotion_decision = json.loads(authoritative_boundary_winner_promotion_decision_path.read_text(encoding="utf-8"))
    boundary_winner_promotion_drop = json.loads(boundary_winner_promotion_drop_path.read_text(encoding="utf-8"))
    lightweight_boundary_challenger_rules = json.loads(lightweight_boundary_challenger_rules_path.read_text(encoding="utf-8"))
    lightweight_boundary_challenger_compare = json.loads(lightweight_boundary_challenger_compare_path.read_text(encoding="utf-8"))
    lightweight_boundary_challenger_effect_by_regime = json.loads(lightweight_boundary_challenger_effect_by_regime_path.read_text(encoding="utf-8"))
    authoritative_lightweight_boundary_challenger_decision = json.loads(authoritative_lightweight_boundary_challenger_decision_path.read_text(encoding="utf-8"))

    assert summary["schema_version"] == msm.MONTHLY_SHAPE_MEMORY_SCHEMA_VERSION
    assert split_contract["schema_version"] == msm.MONTHLY_SHAPE_MEMORY_SPLIT_SCHEMA_VERSION
    assert split_contract["leakage_check_status"] == "pass"
    assert branch_eval["schema_version"] == msm.MONTHLY_SHAPE_MEMORY_BRANCH_SCHEMA_VERSION
    assert "expanding" in branch_eval["modes"]
    assert "rolling" in branch_eval["modes"]
    assert "champion_only" in branch_eval["modes"]["expanding"]["branch_metrics"]
    assert "champion_plus_shape_rerank" in branch_eval["modes"]["expanding"]["branch_metrics"]
    assert compare["schema_version"] == msm.MONTHLY_SHAPE_MEMORY_COMPARE_SCHEMA_VERSION
    assert "expanding" in compare["modes"]
    assert "rolling" in compare["modes"]
    assert "boundary_bucket_effect" in compare
    assert "winner_promotion_delta" in compare
    assert "candidate_pool_top10_capture" in compare
    assert decision["schema_version"] == msm.MONTHLY_SHAPE_MEMORY_DECISION_SCHEMA_VERSION
    assert decision["candidate_id"] == "monthly_shape_memory_v1"
    assert "failure_mode_typed" in decision
    assert "next_action_typed" in decision
    assert "branch_contribution_summary" in decision
    assert hierarchical_dictionary["schema_version"] == "tradex_monthly_shape_memory_hierarchical_label_v1"
    assert hierarchical_rules["schema_version"] == "tradex_monthly_shape_memory_hierarchical_label_v1"
    assert hierarchical_priority["schema_version"] == "tradex_monthly_shape_memory_hierarchical_label_v1"
    assert hierarchical_summary["schema_version"] == "tradex_monthly_shape_memory_hierarchical_label_v1"
    assert hierarchical_summary["row_count"] > 0
    assert hierarchical_summary["symbol_count"] > 0
    assert hierarchical_summary["month_count"] > 0
    assert hierarchical_ablation_compare["schema_version"] == "tradex_monthly_shape_memory_hierarchical_label_v1"
    assert set(hierarchical_ablation_compare["modes"].keys()) == {"expanding", "rolling"}
    assert set(hierarchical_ablation_compare["modes"]["expanding"]["variants"].keys()) == {"A", "B", "C", "D", "E", "F", "G"}
    assert summary["coverage_rows"] > 0
    assert summary["coverage_months"] >= 6
    assert compare["month_count"] >= 6
    assert decision["leakage_check_status"] == "pass"

    conn = duckdb.connect(":memory:")
    try:
        samples_cols = {desc[0] for desc in conn.execute(f"SELECT * FROM read_parquet('{samples_path}') LIMIT 0").description}
        labels_cols = {desc[0] for desc in conn.execute(f"SELECT * FROM read_parquet('{labels_path}') LIMIT 0").description}
        hierarchical_cols = {desc[0] for desc in conn.execute(f"SELECT * FROM read_parquet('{hierarchical_rows_path}') LIMIT 0").description}
        assert "sample_id" in samples_cols
        assert "img_e_00" in samples_cols
        assert "is_next_top10" in labels_cols
        assert "cohort_label" in labels_cols
        assert "monthly_main_state" in hierarchical_cols
        assert "weekly_main_state" in hierarchical_cols
        assert "daily_main_state" in hierarchical_cols
        assert "change_day_score" in hierarchical_cols
        assert "winner_promotion_score" in hierarchical_cols
        assert "loser_removal_score" in hierarchical_cols
        sample_frame = conn.execute(f"SELECT * FROM read_parquet('{samples_path}')").fetchdf()
        hierarchical_frame = conn.execute(f"SELECT * FROM read_parquet('{hierarchical_rows_path}')").fetchdf()
    finally:
        conn.close()

    leakage = msm.validate_no_future_month_leakage(sample_frame)
    assert leakage["status"] == "pass"
    score_columns = [col for col in hierarchical_frame.columns if col.endswith("_score") or col.endswith("_subtotal")]
    for column in score_columns:
        series = pd.to_numeric(hierarchical_frame[column], errors="coerce")
        assert float(series.min()) >= 0.0
        assert float(series.max()) <= 100.0
    assert hierarchical_score_summary["monthly_environment_score"]["max"] <= 100.0
    assert hierarchical_effect_by_state["monthly_main_state"]
    assert hierarchical_effect_by_regime
    assert hierarchical_weekly_diversity["before"]["label_count"] >= 1
    assert hierarchical_weekly_diversity["after"]["label_count"] >= 1
    assert hierarchical_regime_gate_rules["schema_version"] == "tradex_monthly_shape_memory_hierarchical_label_v1"
    assert set(hierarchical_regime_gate_compare["variants"].keys()) == {
        "champion_only",
        "champion_plus_hierarchical_rerank_ungated",
        "champion_plus_hierarchical_rerank_regime_gated",
    }
    assert authoritative_regime_gate_decision["decision"] in {
        "keep_regime_gated_variant",
        "hold_regime_gated_variant",
        "drop_regime_gated_variant",
    }
    assert hierarchical_regime_gate_effect_by_regime["regimes"]
    assert hierarchical_labels_keep_rerank_drop["hierarchical_label_layer_decision"] == "keep_infrastructure"
    assert hierarchical_labels_keep_rerank_drop["hierarchical_regime_gated_rerank_decision"] == "drop_axis"
    assert hierarchical_allowed_uses["allowed_use_cases"]
    assert hierarchical_allowed_uses["disallowed_use_cases"]
    assert hierarchical_state_failure_map["state_combination_rows"]
    assert hierarchical_state_winner_loser_decomposition["monthly_state_rows"]
    assert hierarchical_rerank_do_not_continue["decision"] == "drop_axis"
    assert hierarchical_state_rank_impact_table["table_rows"]
    assert boundary_winner_promotion_rules["schema_version"] == "tradex_monthly_shape_memory_boundary_winner_promotion_v1"
    assert boundary_winner_promotion_rules["candidate_rank_window"] == [6, 30]
    assert boundary_winner_promotion_rules["top5_freeze_enabled"] is True
    assert set(boundary_winner_promotion_compare["variants"].keys()) == {
        "champion_only",
        "boundary_bonus_only",
        "boundary_penalty_only",
        "boundary_bonus_plus_penalty",
        "boundary_bonus_plus_penalty_strict_window",
    }
    assert boundary_winner_promotion_compare["variants"]["boundary_bonus_plus_penalty"]["monthly_rows"]
    sample_boundary_row = boundary_winner_promotion_compare["variants"]["boundary_bonus_plus_penalty"]["monthly_rows"][0]["monthly_rows"][0]
    assert "boundary_winner_bonus" in sample_boundary_row
    assert "boundary_loser_penalty" in sample_boundary_row
    assert "boundary_action_type" in sample_boundary_row
    assert boundary_winner_promotion_compare["best_variant"] in boundary_winner_promotion_compare["variants"]
    assert boundary_winner_promotion_compare["churn_acceptable"] in {True, False}
    assert boundary_winner_promotion_effect_by_regime["regimes"]
    assert authoritative_boundary_winner_promotion_decision["decision"] in {
        "keep_boundary_winner_promotion_challenger",
        "hold_boundary_winner_promotion_challenger",
        "drop_boundary_winner_promotion_challenger",
    }
    assert "full_snapshot_oos_top10_uplift" in authoritative_boundary_winner_promotion_decision
    assert "winner_promotion_improved" in authoritative_boundary_winner_promotion_decision
    assert "loser_removal_improved" in authoritative_boundary_winner_promotion_decision
    assert "boundary_improved" in authoritative_boundary_winner_promotion_decision
    assert "churn_acceptable" in authoritative_boundary_winner_promotion_decision
    assert "recommended_next_use" in authoritative_boundary_winner_promotion_decision
    assert authoritative_boundary_winner_promotion_decision["churn_acceptable"] in {True, False}
    assert boundary_winner_promotion_drop["decision"] == "drop_boundary_winner_promotion_challenger"
    assert boundary_winner_promotion_drop["reason"] == "no_oos_top10_uplift_and_no_winner_promotion"
    assert boundary_winner_promotion_drop["supersedes"] == "authoritative_decision.boundary_winner_promotion.json"
    assert lightweight_boundary_challenger_rules["schema_version"] == "tradex_monthly_shape_memory_lightweight_boundary_challenger_v1"
    assert lightweight_boundary_challenger_rules["candidate_rank_window"] == [6, 20]
    assert lightweight_boundary_challenger_rules["strict_candidate_rank_window"] == [8, 20]
    assert set(lightweight_boundary_challenger_compare["variants"].keys()) == {
        "champion_only",
        "lightweight_boundary_challenger",
        "lightweight_boundary_challenger_strict",
    }
    assert lightweight_boundary_challenger_compare["best_variant"] in lightweight_boundary_challenger_compare["variants"]
    assert lightweight_boundary_challenger_compare["candidate_dataset_summary"]["row_count"] >= 0
    assert lightweight_boundary_challenger_compare["decision"] in {
        "keep_lightweight_boundary_challenger",
        "hold_lightweight_boundary_challenger",
        "drop_lightweight_boundary_challenger",
    }
    assert "recommended_next_use" in authoritative_lightweight_boundary_challenger_decision
    assert lightweight_boundary_challenger_effect_by_regime["regimes"] or lightweight_boundary_challenger_compare["decision"] == "drop_lightweight_boundary_challenger"
    assert authoritative_lightweight_boundary_challenger_decision["decision"] in {
        "keep_lightweight_boundary_challenger",
        "hold_lightweight_boundary_challenger",
        "drop_lightweight_boundary_challenger",
    }
    assert "recommended_next_use" in authoritative_lightweight_boundary_challenger_decision
    assert authoritative_lightweight_boundary_challenger_decision["churn_acceptable"] in {True, False}

    conn = duckdb.connect(":memory:")
    try:
        lightweight_candidates_cols = {desc[0] for desc in conn.execute(f"SELECT * FROM read_parquet('{lightweight_boundary_challenger_candidates_path}') LIMIT 0").description}
        assert "champion_rank" in lightweight_candidates_cols
        assert "lightweight_primary_score" in lightweight_candidates_cols
        assert "lightweight_support_score" in lightweight_candidates_cols
        assert "lightweight_candidate_score" in lightweight_candidates_cols
        assert "should_enter_top10_next_month" in lightweight_candidates_cols
        lightweight_candidates = conn.execute(f"SELECT * FROM read_parquet('{lightweight_boundary_challenger_candidates_path}')").fetchdf()
    finally:
        conn.close()

    assert lightweight_candidates.shape[0] >= 0


def test_monthly_shape_memory_suffix_separates_production_artifacts(tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    _seed_daily_bars(source_db)

    result = msm.run_monthly_shape_memory_research(
        source_db_path=str(source_db),
        top_k=10,
        candidate_pool_k=12,
        analog_k=4,
        memory_lookback_months=6,
        rolling_window_months=6,
        start_month=202101,
        artifact_suffix="production",
    )

    assert Path(result["summary_path"]).name.endswith(".production.json")
    assert Path(result["split_contract_path"]).name.endswith(".production.json")
    assert Path(result["branch_eval_path"]).name.endswith(".production.json")
    assert Path(result["compare_path"]).name.endswith(".production.json")
    assert Path(result["decision_path"]).name.endswith(".production.json")
    assert Path(result["samples_path"]).name.endswith(".production.parquet")
    assert Path(result["labels_path"]).name.endswith(".production.parquet")
    assert Path(result["similarity_path"]).name.endswith(".production.parquet")
    assert Path(result["hierarchical_rows_path"]).name.endswith(".production.parquet")
