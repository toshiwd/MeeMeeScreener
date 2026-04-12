from __future__ import annotations

import duckdb

from external_analysis.results.result_schema import ALL_RESULT_TABLES, ensure_result_db


def test_ensure_result_db_creates_all_tables(tmp_path) -> None:
    db_path = tmp_path / "result.duckdb"
    payload = ensure_result_db(str(db_path))
    assert payload["ok"] is True
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main' ORDER BY table_name"
        ).fetchall()
    finally:
        conn.close()
    names = {str(row[0]) for row in rows}
    for table_name in ALL_RESULT_TABLES:
        assert table_name in names


def test_public_empty_schema_has_expected_columns_and_publish_id(tmp_path) -> None:
    db_path = tmp_path / "result.duckdb"
    ensure_result_db(str(db_path))
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        expected = {
            "publish_pointer": {"pointer_name": "VARCHAR", "publish_id": "VARCHAR", "as_of_date": "DATE"},
            "publish_manifest": {
                "publish_id": "VARCHAR",
                "as_of_date": "DATE",
                "published_at": "TIMESTAMP",
                "logic_id": "VARCHAR",
                "logic_version": "VARCHAR",
                "logic_family": "VARCHAR",
                "default_logic_pointer": "VARCHAR",
                "logic_artifact_uri": "VARCHAR",
                "logic_artifact_checksum": "VARCHAR",
            },
            "candidate_daily": {"publish_id": "VARCHAR", "as_of_date": "DATE", "code": "VARCHAR"},
            "candidate_component_scores": {"publish_id": "VARCHAR", "as_of_date": "DATE", "code": "VARCHAR"},
            "forecast_surface_daily": {
                "publish_id": "VARCHAR",
                "as_of_date": "DATE",
                "code": "VARCHAR",
                "side": "VARCHAR",
                "action_state": "VARCHAR",
                "direction_prob": "DOUBLE",
                "expected_ret_5": "DOUBLE",
                "expected_ret_10": "DOUBLE",
                "expected_ret_20": "DOUBLE",
                "expected_mfe_20": "DOUBLE",
                "expected_mae_20": "DOUBLE",
                "invalidation_price": "DOUBLE",
                "setup_tags": "JSON",
                "reason_codes": "JSON",
                "opportunity_score": "DOUBLE",
                "freshness_state": "VARCHAR",
                "created_at": "TIMESTAMP",
            },
            "forecast_surface_evaluation_runs": {
                "run_id": "VARCHAR",
                "scope_type": "VARCHAR",
                "publish_id": "VARCHAR",
                "as_of_date": "DATE",
                "model_version": "VARCHAR",
                "top_k": "INTEGER",
                "fold_count": "INTEGER",
                "daily_count": "INTEGER",
                "horizon_count": "INTEGER",
                "top_long_mean_ret20_net": "DOUBLE",
                "top_short_mean_ret20_net": "DOUBLE",
                "top_combined_mean_ret20_net": "DOUBLE",
                "worst_regime_combined_mean_ret20_net": "DOUBLE",
                "primary_gate_reason": "VARCHAR",
                "gate_failures_json": "JSON",
                "calibration_method_long": "VARCHAR",
                "calibration_method_short": "VARCHAR",
                "ready_streak": "INTEGER",
                "recent_ready_count_20": "INTEGER",
                "readiness_pass": "BOOLEAN",
                "gate_reason": "VARCHAR",
            },
            "forecast_surface_evaluation_folds": {
                "run_id": "VARCHAR",
                "scope_type": "VARCHAR",
                "publish_id": "VARCHAR",
                "as_of_date": "DATE",
                "regime_tag": "VARCHAR",
                "side": "VARCHAR",
                "horizon_days": "INTEGER",
                "top_k": "INTEGER",
                "sample_count": "INTEGER",
                "top_mean_ret_net": "DOUBLE",
                "top_mean_prob": "DOUBLE",
                "top_positive_rate": "DOUBLE",
                "candidate_mean_ret_net": "DOUBLE",
                "signal_mean_ret_net": "DOUBLE",
            },
            "forecast_surface_runs": {
                "publish_id": "VARCHAR",
                "as_of_date": "DATE",
                "model_version": "VARCHAR",
                "universe_code_count": "INTEGER",
                "expected_row_count": "INTEGER",
                "actual_row_count": "INTEGER",
                "missing_row_count": "INTEGER",
                "coverage_ratio": "DOUBLE",
                "feature_frame_version": "VARCHAR",
                "market_opportunity_score_enabled": "BOOLEAN",
                "personal_fit_score_enabled": "BOOLEAN",
                "side_counts_json": "JSON",
                "action_counts_json": "JSON",
                "source_context_presence_json": "JSON",
                "alerts_json": "JSON",
                "created_at": "TIMESTAMP",
            },
            "nightly_candidate_metrics": {"run_id": "VARCHAR", "publish_id": "VARCHAR", "as_of_date": "DATE", "baseline_version": "VARCHAR"},
            "publish_candidate_bundle": {
                "candidate_id": "VARCHAR",
                "logic_key": "VARCHAR",
                "logic_id": "VARCHAR",
                "logic_version": "VARCHAR",
                "logic_family": "VARCHAR",
                "candidate_status": "VARCHAR",
                "validation_state": "VARCHAR",
            },
            "publish_candidate_audit": {
                "event_id": "VARCHAR",
                "candidate_id": "VARCHAR",
                "logic_key": "VARCHAR",
                "action": "VARCHAR",
                "previous_status": "VARCHAR",
                "new_status": "VARCHAR",
            },
            "publish_maintenance_state": {
                "maintenance_name": "VARCHAR",
                "schema_version": "VARCHAR",
                "candidate_backfill_last_run": "TIMESTAMP",
                "snapshot_sweep_last_run": "TIMESTAMP",
                "non_promotable_legacy_count": "BIGINT",
                "maintenance_degraded": "BOOLEAN",
                "updated_at": "TIMESTAMP",
            },
            "state_eval_daily": {
                "publish_id": "VARCHAR",
                "as_of_date": "DATE",
                "code": "VARCHAR",
                "side": "VARCHAR",
                "holding_band": "VARCHAR",
                "strategy_tags": "JSON",
                "decision_3way": "VARCHAR",
                "machine_action_state": "VARCHAR",
                "human_readable_judgement": "VARCHAR",
                "buy_score": "DOUBLE",
                "environment_score": "DOUBLE",
                "trend_score": "DOUBLE",
                "trigger_score": "DOUBLE",
                "risk_score": "DOUBLE",
                "invalidation_price": "DOUBLE",
                "invalidation_reason_code": "VARCHAR",
                "reason_text_top3": "JSON",
            },
            "forecast_surface_daily": {
                "publish_id": "VARCHAR",
                "as_of_date": "DATE",
                "code": "VARCHAR",
                "side": "VARCHAR",
                "action_state": "VARCHAR",
                "direction_prob": "DOUBLE",
                "expected_ret_5": "DOUBLE",
                "expected_ret_10": "DOUBLE",
                "expected_ret_20": "DOUBLE",
                "expected_mfe_20": "DOUBLE",
                "expected_mae_20": "DOUBLE",
                "invalidation_price": "DOUBLE",
                "setup_tags": "JSON",
                "reason_codes": "JSON",
                "market_opportunity_score": "DOUBLE",
                "personal_fit_score": "DOUBLE",
                "opportunity_score": "DOUBLE",
                "freshness_state": "VARCHAR",
                "created_at": "TIMESTAMP",
            },
            "similar_cases_daily": {"publish_id": "VARCHAR", "as_of_date": "DATE", "code": "VARCHAR"},
            "similar_case_paths": {"publish_id": "VARCHAR", "as_of_date": "DATE", "code": "VARCHAR"},
            "regime_daily": {"publish_id": "VARCHAR", "as_of_date": "DATE", "regime_tag": "VARCHAR"},
        }
        for table_name, columns in expected.items():
            rows = conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
            by_name = {str(row[1]): str(row[2]).upper() for row in rows}
            for column_name, column_type in columns.items():
                assert by_name[column_name] == column_type
    finally:
        conn.close()
