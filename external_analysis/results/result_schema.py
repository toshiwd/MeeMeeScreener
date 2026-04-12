from __future__ import annotations

from typing import Any

import duckdb

from shared.contracts.analysis_bridge import (
    ALLOWED_PUBLIC_TABLES,
    DEGRADE_REASON_HARD_STALE,
    DEGRADE_REASON_MANIFEST_MISMATCH,
    DEGRADE_REASON_NO_PUBLISH,
    DEGRADE_REASON_POINTER_CORRUPTION,
    DEGRADE_REASON_RESULT_DB_MISSING,
    DEGRADE_REASON_REGIME_ROW_CORRUPTION,
    DEGRADE_REASON_SCHEMA_MISMATCH,
    DEGRADE_REASON_WARNING_STALE,
    LATEST_POINTER_NAME,
    allowed_public_columns,
    is_allowed_public_table,
)
from external_analysis.contracts.paths import resolve_result_db_path

SCHEMA_VERSION = "phase1-v1"
CONTRACT_VERSION = "phase1-v1"
POINTER_NAME_LATEST_SUCCESSFUL = LATEST_POINTER_NAME
PUBLIC_RESULT_TABLES: tuple[str, ...] = ALLOWED_PUBLIC_TABLES
INTERNAL_RESULT_TABLES: tuple[str, ...] = (
    "publish_runs",
    "candidate_component_scores",
    "forecast_surface_daily",
    "forecast_surface_runs",
    "forecast_surface_evaluation_runs",
    "forecast_surface_evaluation_folds",
    "nightly_candidate_metrics",
    "publish_registry_state",
    "publish_registry_audit",
    "publish_candidate_bundle",
    "publish_candidate_audit",
    "publish_maintenance_state",
)
ALL_RESULT_TABLES: tuple[str, ...] = PUBLIC_RESULT_TABLES + INTERNAL_RESULT_TABLES
ALLOWED_FRESHNESS_STATES: tuple[str, ...] = ("fresh", "warning", "hard")


def connect_result_db(db_path: str | None = None, *, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    resolved = resolve_result_db_path(db_path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(resolved), read_only=read_only)


def ensure_result_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS publish_pointer (
            pointer_name TEXT PRIMARY KEY,
            publish_id TEXT NOT NULL,
            as_of_date DATE NOT NULL,
            published_at TIMESTAMP NOT NULL,
            schema_version TEXT NOT NULL,
            contract_version TEXT NOT NULL,
            freshness_state TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS publish_runs (
            publish_id TEXT PRIMARY KEY,
            as_of_date DATE NOT NULL,
            contract_version TEXT NOT NULL,
            schema_version TEXT NOT NULL,
            status TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL,
            published_at TIMESTAMP,
            validation_summary JSON,
            row_counts JSON
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS publish_manifest (
            publish_id TEXT PRIMARY KEY,
            as_of_date DATE NOT NULL,
            schema_version TEXT NOT NULL,
            contract_version TEXT NOT NULL,
            status TEXT NOT NULL,
            published_at TIMESTAMP NOT NULL,
            freshness_state TEXT NOT NULL,
            degrade_ready BOOLEAN NOT NULL,
            table_row_counts JSON NOT NULL,
            logic_id TEXT,
            logic_version TEXT,
            logic_family TEXT,
            default_logic_pointer TEXT,
            bootstrap_champion BOOLEAN NOT NULL DEFAULT FALSE,
            logic_artifact_uri TEXT,
            logic_artifact_checksum TEXT,
            logic_manifest_json JSON
        )
        """
    )
    conn.execute("ALTER TABLE publish_manifest ADD COLUMN IF NOT EXISTS logic_id TEXT")
    conn.execute("ALTER TABLE publish_manifest ADD COLUMN IF NOT EXISTS logic_version TEXT")
    conn.execute("ALTER TABLE publish_manifest ADD COLUMN IF NOT EXISTS logic_family TEXT")
    conn.execute("ALTER TABLE publish_manifest ADD COLUMN IF NOT EXISTS default_logic_pointer TEXT")
    conn.execute("ALTER TABLE publish_manifest ADD COLUMN IF NOT EXISTS bootstrap_champion BOOLEAN")
    conn.execute("ALTER TABLE publish_manifest ADD COLUMN IF NOT EXISTS logic_artifact_uri TEXT")
    conn.execute("ALTER TABLE publish_manifest ADD COLUMN IF NOT EXISTS logic_artifact_checksum TEXT")
    conn.execute("ALTER TABLE publish_manifest ADD COLUMN IF NOT EXISTS logic_manifest_json JSON")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS publish_registry_state (
            registry_name TEXT PRIMARY KEY,
            schema_version TEXT NOT NULL,
            registry_version BIGINT NOT NULL,
            source_of_truth TEXT NOT NULL,
            source_revision TEXT,
            updated_at TIMESTAMP NOT NULL,
            last_sync_at TIMESTAMP,
            champion_logic_key TEXT,
            challenger_logic_key TEXT,
            challengers_json JSON NOT NULL,
            default_logic_pointer TEXT,
            previous_stable_champion_logic_key TEXT,
            bootstrap_rule TEXT NOT NULL,
            retired_logic_keys JSON NOT NULL,
            demoted_logic_keys JSON NOT NULL,
            registry_state_json JSON NOT NULL,
            registry_checksum TEXT,
            degraded BOOLEAN NOT NULL,
            sync_state TEXT NOT NULL,
            sync_message TEXT
        )
        """
    )
    conn.execute("ALTER TABLE publish_registry_state ADD COLUMN IF NOT EXISTS challengers_json JSON")
    conn.execute("ALTER TABLE publish_registry_state ADD COLUMN IF NOT EXISTS previous_stable_champion_logic_key TEXT")
    conn.execute("ALTER TABLE publish_registry_state ADD COLUMN IF NOT EXISTS bootstrap_rule TEXT")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS publish_registry_audit (
            event_id TEXT PRIMARY KEY,
            registry_name TEXT NOT NULL,
            action TEXT NOT NULL,
            previous_logic_key TEXT,
            new_logic_key TEXT,
            logic_id TEXT,
            logic_version TEXT,
            logic_family TEXT,
            artifact_uri TEXT,
            artifact_checksum TEXT,
            source TEXT NOT NULL,
            reason TEXT,
            actor TEXT,
            registry_version BIGINT,
            created_at TIMESTAMP NOT NULL,
            details_json JSON NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS publish_candidate_bundle (
            candidate_id TEXT PRIMARY KEY,
            logic_key TEXT NOT NULL,
            logic_id TEXT NOT NULL,
            logic_version TEXT NOT NULL,
            logic_family TEXT NOT NULL,
            source_publish_id TEXT,
            bundle_schema_version TEXT NOT NULL,
            candidate_status TEXT NOT NULL,
            validation_state TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL,
            approved_at TIMESTAMP,
            rejected_at TIMESTAMP,
            promoted_at TIMESTAMP,
            retired_at TIMESTAMP,
            published_logic_artifact JSON NOT NULL,
            published_logic_manifest JSON NOT NULL,
            validation_summary JSON NOT NULL,
            published_ranking_snapshot JSON,
            bundle_checksum TEXT NOT NULL,
            notes JSON NOT NULL,
            metadata JSON NOT NULL
        )
        """
    )
    conn.execute("ALTER TABLE publish_candidate_bundle ADD COLUMN IF NOT EXISTS source_publish_id TEXT")
    conn.execute("ALTER TABLE publish_candidate_bundle ADD COLUMN IF NOT EXISTS bundle_schema_version TEXT")
    conn.execute("ALTER TABLE publish_candidate_bundle ADD COLUMN IF NOT EXISTS candidate_status TEXT")
    conn.execute("ALTER TABLE publish_candidate_bundle ADD COLUMN IF NOT EXISTS validation_state TEXT")
    conn.execute("ALTER TABLE publish_candidate_bundle ADD COLUMN IF NOT EXISTS approved_at TIMESTAMP")
    conn.execute("ALTER TABLE publish_candidate_bundle ADD COLUMN IF NOT EXISTS rejected_at TIMESTAMP")
    conn.execute("ALTER TABLE publish_candidate_bundle ADD COLUMN IF NOT EXISTS promoted_at TIMESTAMP")
    conn.execute("ALTER TABLE publish_candidate_bundle ADD COLUMN IF NOT EXISTS retired_at TIMESTAMP")
    conn.execute("ALTER TABLE publish_candidate_bundle ADD COLUMN IF NOT EXISTS notes JSON")
    conn.execute("ALTER TABLE publish_candidate_bundle ADD COLUMN IF NOT EXISTS metadata JSON")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS publish_candidate_audit (
            event_id TEXT PRIMARY KEY,
            candidate_id TEXT NOT NULL,
            logic_key TEXT NOT NULL,
            action TEXT NOT NULL,
            previous_status TEXT,
            new_status TEXT,
            source TEXT NOT NULL,
            reason TEXT,
            actor TEXT,
            queue_order_before INTEGER,
            queue_order_after INTEGER,
            changed_at TIMESTAMP NOT NULL,
            details_json JSON NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS publish_maintenance_state (
            maintenance_name TEXT PRIMARY KEY,
            schema_version TEXT NOT NULL,
            candidate_backfill_last_run TIMESTAMP,
            candidate_backfill_summary JSON,
            snapshot_sweep_last_run TIMESTAMP,
            snapshot_sweep_summary JSON,
            non_promotable_legacy_count BIGINT NOT NULL DEFAULT 0,
            maintenance_degraded BOOLEAN NOT NULL,
            updated_at TIMESTAMP NOT NULL,
            details_json JSON NOT NULL
        )
        """
    )
    conn.execute("ALTER TABLE publish_maintenance_state ADD COLUMN IF NOT EXISTS non_promotable_legacy_count BIGINT DEFAULT 0")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS candidate_daily (
            publish_id TEXT NOT NULL,
            as_of_date DATE NOT NULL,
            code TEXT NOT NULL,
            side TEXT NOT NULL,
            rank_position INTEGER NOT NULL,
            candidate_score DOUBLE,
            expected_horizon_days INTEGER,
            primary_reason_codes JSON,
            regime_tag TEXT,
            freshness_state TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS candidate_component_scores (
            publish_id TEXT NOT NULL,
            as_of_date DATE NOT NULL,
            code TEXT NOT NULL,
            side TEXT NOT NULL,
            retrieval_score DOUBLE,
            ranking_score DOUBLE,
            risk_penalty DOUBLE,
            regime_adjustment DOUBLE,
            reason_codes JSON
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS forecast_surface_daily (
            publish_id TEXT NOT NULL,
            as_of_date DATE NOT NULL,
            code TEXT NOT NULL,
            side TEXT NOT NULL,
            action_state TEXT NOT NULL,
            direction_prob DOUBLE,
            expected_ret_5 DOUBLE,
            expected_ret_10 DOUBLE,
            expected_ret_20 DOUBLE,
            expected_mfe_20 DOUBLE,
            expected_mae_20 DOUBLE,
            invalidation_price DOUBLE,
            setup_tags JSON,
            reason_codes JSON,
            market_opportunity_score DOUBLE,
            personal_fit_score DOUBLE,
            opportunity_score DOUBLE,
            freshness_state TEXT,
            created_at TIMESTAMP NOT NULL
        )
        """
    )
    conn.execute("ALTER TABLE forecast_surface_daily ADD COLUMN IF NOT EXISTS market_opportunity_score DOUBLE")
    conn.execute("ALTER TABLE forecast_surface_daily ADD COLUMN IF NOT EXISTS personal_fit_score DOUBLE")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS forecast_surface_runs (
            publish_id TEXT PRIMARY KEY,
            as_of_date DATE NOT NULL,
            model_version TEXT NOT NULL,
            universe_code_count INTEGER NOT NULL,
            expected_row_count INTEGER NOT NULL,
            actual_row_count INTEGER NOT NULL,
            missing_row_count INTEGER NOT NULL,
            coverage_ratio DOUBLE NOT NULL,
            feature_frame_version TEXT,
            market_opportunity_score_enabled BOOLEAN NOT NULL DEFAULT FALSE,
            personal_fit_score_enabled BOOLEAN NOT NULL DEFAULT FALSE,
            side_counts_json JSON NOT NULL,
            action_counts_json JSON NOT NULL,
            source_context_presence_json JSON NOT NULL,
            alerts_json JSON NOT NULL,
            created_at TIMESTAMP NOT NULL
        )
        """
    )
    conn.execute("ALTER TABLE forecast_surface_runs ADD COLUMN IF NOT EXISTS feature_frame_version TEXT")
    conn.execute("ALTER TABLE forecast_surface_runs ADD COLUMN IF NOT EXISTS market_opportunity_score_enabled BOOLEAN DEFAULT FALSE")
    conn.execute("ALTER TABLE forecast_surface_runs ADD COLUMN IF NOT EXISTS personal_fit_score_enabled BOOLEAN DEFAULT FALSE")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS forecast_surface_evaluation_runs (
            run_id TEXT PRIMARY KEY,
            scope_type TEXT NOT NULL,
            publish_id TEXT,
            as_of_date DATE,
            model_version TEXT NOT NULL,
            top_k INTEGER NOT NULL,
            fold_count INTEGER NOT NULL,
            daily_count INTEGER NOT NULL,
            horizon_count INTEGER NOT NULL,
            top_long_mean_ret20_net DOUBLE,
            top_short_mean_ret20_net DOUBLE,
            top_combined_mean_ret20_net DOUBLE,
            candidate_long_mean_ret20_net DOUBLE,
            candidate_short_mean_ret20_net DOUBLE,
            candidate_combined_mean_ret20_net DOUBLE,
            signal_long_mean_ret20_net DOUBLE,
            signal_short_mean_ret20_net DOUBLE,
            direction_brier_long DOUBLE,
            direction_brier_short DOUBLE,
            calibration_gap_long DOUBLE,
            calibration_gap_short DOUBLE,
            top_k_uplift DOUBLE,
            worst_regime_combined_mean_ret20_net DOUBLE,
            min_folds INTEGER,
            min_daily_count INTEGER,
            primary_gate_reason TEXT,
            gate_failures_json JSON,
            calibration_method_long TEXT,
            calibration_method_short TEXT,
            ready_streak INTEGER DEFAULT 0,
            recent_ready_count_20 INTEGER DEFAULT 0,
            regime_breakdown_json JSON NOT NULL,
            fold_metrics_json JSON NOT NULL,
            readiness_pass BOOLEAN NOT NULL,
            gate_reason TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL
        )
        """
    )
    conn.execute("ALTER TABLE forecast_surface_evaluation_runs ADD COLUMN IF NOT EXISTS worst_regime_combined_mean_ret20_net DOUBLE")
    conn.execute("ALTER TABLE forecast_surface_evaluation_runs ADD COLUMN IF NOT EXISTS primary_gate_reason TEXT")
    conn.execute("ALTER TABLE forecast_surface_evaluation_runs ADD COLUMN IF NOT EXISTS gate_failures_json JSON")
    conn.execute("ALTER TABLE forecast_surface_evaluation_runs ADD COLUMN IF NOT EXISTS calibration_method_long TEXT")
    conn.execute("ALTER TABLE forecast_surface_evaluation_runs ADD COLUMN IF NOT EXISTS calibration_method_short TEXT")
    conn.execute("ALTER TABLE forecast_surface_evaluation_runs ADD COLUMN IF NOT EXISTS ready_streak INTEGER DEFAULT 0")
    conn.execute("ALTER TABLE forecast_surface_evaluation_runs ADD COLUMN IF NOT EXISTS recent_ready_count_20 INTEGER DEFAULT 0")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS forecast_surface_evaluation_folds (
            run_id TEXT NOT NULL,
            scope_type TEXT NOT NULL,
            publish_id TEXT,
            as_of_date DATE NOT NULL,
            regime_tag TEXT,
            side TEXT NOT NULL,
            horizon_days INTEGER NOT NULL,
            top_k INTEGER NOT NULL,
            sample_count INTEGER NOT NULL,
            top_mean_ret_net DOUBLE,
            top_mean_mfe_net DOUBLE,
            top_mean_mae_net DOUBLE,
            top_win_rate DOUBLE,
            top_mean_prob DOUBLE,
            top_positive_rate DOUBLE,
            top_brier DOUBLE,
            top_calibration_gap DOUBLE,
            candidate_mean_ret_net DOUBLE,
            candidate_win_rate DOUBLE,
            signal_mean_ret_net DOUBLE,
            signal_sample_count INTEGER,
            created_at TIMESTAMP NOT NULL
        )
        """
    )
    conn.execute("ALTER TABLE forecast_surface_evaluation_folds ADD COLUMN IF NOT EXISTS top_mean_prob DOUBLE")
    conn.execute("ALTER TABLE forecast_surface_evaluation_folds ADD COLUMN IF NOT EXISTS top_positive_rate DOUBLE")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS nightly_candidate_metrics (
            run_id TEXT PRIMARY KEY,
            publish_id TEXT NOT NULL,
            as_of_date DATE NOT NULL,
            model_key TEXT NOT NULL,
            baseline_version TEXT NOT NULL,
            label_policy_version TEXT,
            feature_version TEXT,
            universe_count INTEGER NOT NULL,
            candidate_count_long INTEGER NOT NULL,
            candidate_count_short INTEGER NOT NULL,
            recall_at_20 DOUBLE,
            recall_at_10 DOUBLE,
            monthly_top5_capture DOUBLE,
            avg_ret_20_top20 DOUBLE,
            avg_mfe_20_top20 DOUBLE,
            avg_mae_20_top20 DOUBLE,
            max_drawdown_proxy DOUBLE,
            turnover_proxy DOUBLE,
            regime_breakdown_json JSON NOT NULL,
            created_at TIMESTAMP NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS state_eval_daily (
            publish_id TEXT NOT NULL,
            as_of_date DATE NOT NULL,
            code TEXT NOT NULL,
            state_action TEXT,
            side TEXT,
            holding_band TEXT,
            strategy_tags JSON,
            decision_3way TEXT,
            confidence DOUBLE,
            machine_action_state TEXT,
            human_readable_judgement TEXT,
            buy_score DOUBLE,
            environment_score DOUBLE,
            trend_score DOUBLE,
            trigger_score DOUBLE,
            risk_score DOUBLE,
            invalidation_price DOUBLE,
            invalidation_reason_code TEXT,
            reason_codes JSON,
            reason_text_top3 JSON,
            freshness_state TEXT
        )
        """
    )
    conn.execute("ALTER TABLE state_eval_daily ADD COLUMN IF NOT EXISTS side TEXT")
    conn.execute("ALTER TABLE state_eval_daily ADD COLUMN IF NOT EXISTS holding_band TEXT")
    conn.execute("ALTER TABLE state_eval_daily ADD COLUMN IF NOT EXISTS strategy_tags JSON")
    conn.execute("ALTER TABLE state_eval_daily ADD COLUMN IF NOT EXISTS decision_3way TEXT")
    conn.execute("ALTER TABLE state_eval_daily ADD COLUMN IF NOT EXISTS machine_action_state TEXT")
    conn.execute("ALTER TABLE state_eval_daily ADD COLUMN IF NOT EXISTS human_readable_judgement TEXT")
    conn.execute("ALTER TABLE state_eval_daily ADD COLUMN IF NOT EXISTS buy_score DOUBLE")
    conn.execute("ALTER TABLE state_eval_daily ADD COLUMN IF NOT EXISTS environment_score DOUBLE")
    conn.execute("ALTER TABLE state_eval_daily ADD COLUMN IF NOT EXISTS trend_score DOUBLE")
    conn.execute("ALTER TABLE state_eval_daily ADD COLUMN IF NOT EXISTS trigger_score DOUBLE")
    conn.execute("ALTER TABLE state_eval_daily ADD COLUMN IF NOT EXISTS risk_score DOUBLE")
    conn.execute("ALTER TABLE state_eval_daily ADD COLUMN IF NOT EXISTS invalidation_price DOUBLE")
    conn.execute("ALTER TABLE state_eval_daily ADD COLUMN IF NOT EXISTS invalidation_reason_code TEXT")
    conn.execute("ALTER TABLE state_eval_daily ADD COLUMN IF NOT EXISTS reason_codes JSON")
    conn.execute("ALTER TABLE state_eval_daily ADD COLUMN IF NOT EXISTS reason_text_top3 JSON")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS similar_cases_daily (
            publish_id TEXT NOT NULL,
            as_of_date DATE NOT NULL,
            code TEXT NOT NULL,
            query_type TEXT,
            query_anchor_type TEXT,
            neighbor_rank INTEGER,
            case_id TEXT,
            neighbor_code TEXT,
            neighbor_anchor_date DATE,
            case_type TEXT,
            outcome_class TEXT,
            success_flag BOOLEAN,
            similarity_score DOUBLE,
            reason_codes JSON
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS similar_case_paths (
            publish_id TEXT NOT NULL,
            as_of_date DATE NOT NULL,
            code TEXT NOT NULL,
            case_id TEXT NOT NULL,
            rel_day INTEGER NOT NULL,
            path_return_norm DOUBLE,
            path_volume_norm DOUBLE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS regime_daily (
            publish_id TEXT NOT NULL,
            as_of_date DATE NOT NULL,
            regime_tag TEXT NOT NULL,
            regime_score DOUBLE,
            breadth_score DOUBLE,
            volatility_state TEXT
        )
        """
    )


def ensure_result_db(db_path: str | None = None) -> dict[str, Any]:
    conn = connect_result_db(db_path=db_path, read_only=False)
    try:
        ensure_result_schema(conn)
        conn.execute("CHECKPOINT")
        return {
            "ok": True,
            "db_path": str(resolve_result_db_path(db_path)),
            "schema_version": SCHEMA_VERSION,
            "contract_version": CONTRACT_VERSION,
            "tables": list(ALL_RESULT_TABLES),
        }
    finally:
        conn.close()
