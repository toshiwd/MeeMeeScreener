from __future__ import annotations

from typing import Any

import duckdb

from external_analysis.contracts.paths import resolve_ops_db_path


OPS_TABLES: tuple[str, ...] = (
    "external_job_runs",
    "external_job_quarantine",
    "external_work_items",
    "external_trade_teacher_profiles",
    "external_state_eval_shadow_runs",
    "external_state_eval_readiness",
    "external_state_eval_failure_samples",
    "external_state_eval_tag_rollups",
    "external_state_eval_daily_summaries",
    "external_replay_runs",
    "external_replay_days",
    "external_replay_summaries",
    "external_replay_readiness",
    "external_metric_daily_summaries",
    "external_comparison_rollups",
    "external_promotion_readiness",
    "external_promotion_decisions",
    "external_review_artifacts",
)


def connect_ops_db(db_path: str | None = None) -> duckdb.DuckDBPyConnection:
    resolved = resolve_ops_db_path(db_path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(resolved), read_only=False)


def ensure_ops_schema(conn: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS external_job_runs (
            job_id TEXT PRIMARY KEY,
            job_type TEXT NOT NULL,
            status TEXT NOT NULL,
            as_of_date DATE,
            publish_id TEXT,
            attempt INTEGER NOT NULL DEFAULT 1,
            created_at TIMESTAMP NOT NULL,
            started_at TIMESTAMP,
            finished_at TIMESTAMP,
            checkpoint_uri TEXT,
            error_class TEXT,
            details_json JSON
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS external_job_quarantine (
            quarantine_id TEXT PRIMARY KEY,
            job_type TEXT NOT NULL,
            as_of_date DATE,
            publish_id TEXT,
            attempt_count INTEGER NOT NULL,
            reason TEXT NOT NULL,
            payload_json JSON NOT NULL,
            created_at TIMESTAMP NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS external_work_items (
            work_id TEXT PRIMARY KEY,
            work_type TEXT NOT NULL,
            scope_type TEXT NOT NULL,
            scope_id TEXT NOT NULL,
            status TEXT NOT NULL,
            depends_on_json JSON NOT NULL,
            payload_json JSON NOT NULL,
            created_at TIMESTAMP NOT NULL,
            started_at TIMESTAMP,
            finished_at TIMESTAMP,
            error_class TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS external_trade_teacher_profiles (
            profile_id TEXT PRIMARY KEY,
            as_of_date DATE NOT NULL,
            code TEXT NOT NULL,
            side TEXT NOT NULL,
            holding_band TEXT,
            strategy_tags JSON,
            trade_count INTEGER NOT NULL,
            alignment_score DOUBLE,
            position_bias DOUBLE,
            summary_json JSON NOT NULL,
            created_at TIMESTAMP NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS external_state_eval_shadow_runs (
            shadow_id TEXT PRIMARY KEY,
            publish_id TEXT NOT NULL,
            as_of_date DATE NOT NULL,
            code TEXT NOT NULL,
            side TEXT NOT NULL,
            holding_band TEXT,
            strategy_tags JSON,
            champion_decision TEXT NOT NULL,
            challenger_decision TEXT NOT NULL,
            champion_confidence DOUBLE,
            challenger_confidence DOUBLE,
            expected_return DOUBLE,
            adverse_move DOUBLE,
            teacher_alignment DOUBLE,
            label_available BOOLEAN NOT NULL,
            summary_json JSON NOT NULL,
            created_at TIMESTAMP NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS external_state_eval_readiness (
            readiness_id TEXT PRIMARY KEY,
            publish_id TEXT NOT NULL,
            as_of_date DATE NOT NULL,
            champion_version TEXT NOT NULL,
            challenger_version TEXT NOT NULL,
            sample_count INTEGER NOT NULL DEFAULT 0,
            expectancy_delta DOUBLE,
            improved_expectancy BOOLEAN NOT NULL,
            mae_non_worse BOOLEAN NOT NULL,
            adverse_move_non_worse BOOLEAN NOT NULL,
            stable_window BOOLEAN NOT NULL,
            alignment_ok BOOLEAN NOT NULL,
            readiness_pass BOOLEAN NOT NULL,
            reason_codes JSON,
            summary_json JSON NOT NULL,
            created_at TIMESTAMP NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS external_state_eval_failure_samples (
            sample_id TEXT PRIMARY KEY,
            publish_id TEXT NOT NULL,
            as_of_date DATE NOT NULL,
            code TEXT NOT NULL,
            side TEXT NOT NULL,
            holding_band TEXT NOT NULL,
            strategy_tags JSON NOT NULL,
            bucket_type TEXT NOT NULL,
            expected_return DOUBLE,
            adverse_move DOUBLE,
            reason_codes JSON NOT NULL,
            summary_json JSON NOT NULL,
            created_at TIMESTAMP NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS external_state_eval_tag_rollups (
            rollup_id TEXT PRIMARY KEY,
            publish_id TEXT NOT NULL,
            as_of_date DATE NOT NULL,
            side TEXT NOT NULL,
            holding_band TEXT NOT NULL,
            strategy_tag TEXT NOT NULL,
            observation_count INTEGER NOT NULL,
            labeled_count INTEGER NOT NULL,
            enter_count INTEGER NOT NULL,
            wait_count INTEGER NOT NULL,
            skip_count INTEGER NOT NULL,
            expectancy_mean DOUBLE,
            adverse_mean DOUBLE,
            large_loss_rate DOUBLE,
            win_rate DOUBLE,
            teacher_alignment_mean DOUBLE,
            failure_count INTEGER NOT NULL,
            readiness_hint TEXT NOT NULL,
            latest_failure_examples JSON NOT NULL,
            worst_failure_examples JSON NOT NULL,
            summary_json JSON NOT NULL,
            created_at TIMESTAMP NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS external_state_eval_daily_summaries (
            summary_id TEXT PRIMARY KEY,
            publish_id TEXT NOT NULL,
            as_of_date DATE NOT NULL,
            side_scope TEXT NOT NULL,
            top_strategy_tag TEXT,
            top_strategy_expectancy DOUBLE,
            top_candle_tag TEXT,
            top_candle_expectancy DOUBLE,
            risk_watch_tag TEXT,
            risk_watch_loss_rate DOUBLE,
            sample_watch_tag TEXT,
            sample_watch_labeled_count INTEGER,
            promotion_ready BOOLEAN,
            promotion_sample_count INTEGER,
            summary_json JSON NOT NULL,
            created_at TIMESTAMP NOT NULL
        )
        """
    )
    conn.execute("ALTER TABLE external_trade_teacher_profiles ADD COLUMN IF NOT EXISTS holding_band TEXT")
    conn.execute("ALTER TABLE external_trade_teacher_profiles ADD COLUMN IF NOT EXISTS strategy_tags JSON")
    conn.execute("ALTER TABLE external_state_eval_shadow_runs ADD COLUMN IF NOT EXISTS holding_band TEXT")
    conn.execute("ALTER TABLE external_state_eval_shadow_runs ADD COLUMN IF NOT EXISTS strategy_tags JSON")
    conn.execute("ALTER TABLE external_state_eval_readiness ADD COLUMN IF NOT EXISTS sample_count INTEGER DEFAULT 0")
    conn.execute("ALTER TABLE external_state_eval_readiness ADD COLUMN IF NOT EXISTS expectancy_delta DOUBLE")
    conn.execute("ALTER TABLE external_state_eval_readiness ADD COLUMN IF NOT EXISTS reason_codes JSON")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS external_replay_runs (
            replay_id TEXT PRIMARY KEY,
            job_type TEXT NOT NULL,
            status TEXT NOT NULL,
            start_as_of_date DATE NOT NULL,
            end_as_of_date DATE NOT NULL,
            max_days INTEGER,
            universe_filter TEXT,
            universe_limit INTEGER,
            created_at TIMESTAMP NOT NULL,
            started_at TIMESTAMP,
            finished_at TIMESTAMP,
            last_completed_as_of_date DATE,
            error_class TEXT,
            details_json JSON
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS external_replay_days (
            replay_id TEXT NOT NULL,
            as_of_date DATE NOT NULL,
            status TEXT NOT NULL,
            attempt INTEGER NOT NULL DEFAULT 1,
            publish_id TEXT,
            started_at TIMESTAMP,
            finished_at TIMESTAMP,
            error_class TEXT,
            details_json JSON,
            PRIMARY KEY (replay_id, as_of_date)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS external_replay_summaries (
            summary_id TEXT PRIMARY KEY,
            replay_id TEXT NOT NULL,
            start_as_of_date DATE NOT NULL,
            end_as_of_date DATE NOT NULL,
            total_days INTEGER NOT NULL,
            success_days INTEGER NOT NULL,
            failed_days INTEGER NOT NULL,
            skipped_days INTEGER NOT NULL,
            summary_json JSON NOT NULL,
            created_at TIMESTAMP NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS external_replay_readiness (
            readiness_id TEXT PRIMARY KEY,
            replay_id TEXT NOT NULL,
            window_size INTEGER NOT NULL,
            start_as_of_date DATE NOT NULL,
            end_as_of_date DATE NOT NULL,
            run_count INTEGER NOT NULL,
            champion_version TEXT NOT NULL,
            challenger_version TEXT NOT NULL,
            overlap_at_k_mean DOUBLE,
            success_hit_rate_at_k_mean DOUBLE,
            failure_hit_rate_at_k_mean DOUBLE,
            big_drop_hit_rate_at_k_mean DOUBLE,
            avg_similarity_score_mean DOUBLE,
            recall_at_20_mean DOUBLE,
            recall_at_10_mean DOUBLE,
            monthly_top5_capture_mean DOUBLE,
            avg_ret_20_top20_mean DOUBLE,
            readiness_pass BOOLEAN NOT NULL,
            summary_json JSON NOT NULL,
            created_at TIMESTAMP NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS external_metric_daily_summaries (
            summary_id TEXT PRIMARY KEY,
            scope_type TEXT NOT NULL,
            scope_id TEXT NOT NULL,
            as_of_date DATE NOT NULL,
            publish_id TEXT NOT NULL,
            source_kind TEXT NOT NULL,
            summary_json JSON NOT NULL,
            created_at TIMESTAMP NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS external_comparison_rollups (
            rollup_id TEXT PRIMARY KEY,
            scope_type TEXT NOT NULL,
            scope_id TEXT NOT NULL,
            window_size INTEGER NOT NULL,
            start_as_of_date DATE NOT NULL,
            end_as_of_date DATE NOT NULL,
            run_count INTEGER NOT NULL,
            champion_version TEXT NOT NULL,
            challenger_version TEXT NOT NULL,
            overlap_at_k_mean DOUBLE,
            success_hit_rate_at_k_mean DOUBLE,
            failure_hit_rate_at_k_mean DOUBLE,
            big_drop_hit_rate_at_k_mean DOUBLE,
            avg_similarity_score_mean DOUBLE,
            recall_at_20_mean DOUBLE,
            recall_at_10_mean DOUBLE,
            monthly_top5_capture_mean DOUBLE,
            avg_ret_20_top20_mean DOUBLE,
            success_delta_vs_champion DOUBLE,
            failure_delta_vs_champion DOUBLE,
            big_drop_delta_vs_champion DOUBLE,
            summary_json JSON NOT NULL,
            created_at TIMESTAMP NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS external_promotion_readiness (
            readiness_id TEXT PRIMARY KEY,
            scope_type TEXT NOT NULL,
            scope_id TEXT NOT NULL,
            window_size INTEGER NOT NULL,
            end_as_of_date DATE NOT NULL,
            run_count INTEGER NOT NULL,
            champion_version TEXT NOT NULL,
            challenger_version TEXT NOT NULL,
            readiness_pass BOOLEAN NOT NULL,
            reason_codes JSON NOT NULL,
            summary_json JSON NOT NULL,
            created_at TIMESTAMP NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS external_promotion_decisions (
            decision_id TEXT PRIMARY KEY,
            publish_id TEXT NOT NULL,
            as_of_date DATE NOT NULL,
            champion_version TEXT,
            challenger_version TEXT,
            decision TEXT NOT NULL,
            note TEXT,
            actor TEXT,
            summary_json JSON NOT NULL,
            created_at TIMESTAMP NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS external_review_artifacts (
            review_id TEXT PRIMARY KEY,
            review_kind TEXT NOT NULL,
            latest_end_as_of_date DATE,
            replay_scope_id TEXT,
            nightly_scope_id TEXT,
            combined_scope_id TEXT,
            combined_readiness_20 BOOLEAN,
            combined_readiness_40 BOOLEAN,
            combined_readiness_60 BOOLEAN,
            recent_run_limit INTEGER NOT NULL,
            recent_failure_rate DOUBLE,
            recent_quarantine_count INTEGER NOT NULL,
            top_reason_codes_json JSON NOT NULL,
            replay_summary_json JSON NOT NULL,
            nightly_summary_json JSON NOT NULL,
            combined_summary_json JSON NOT NULL,
            summary_json JSON NOT NULL,
            created_at TIMESTAMP NOT NULL
        )
        """
    )
    return {"ok": True}


def ensure_ops_db(db_path: str | None = None) -> dict[str, Any]:
    conn = connect_ops_db(db_path)
    try:
        ensure_ops_schema(conn)
        conn.execute("CHECKPOINT")
        return {"ok": True, "db_path": str(resolve_ops_db_path(db_path)), "tables": list(OPS_TABLES)}
    finally:
        conn.close()
