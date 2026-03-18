from __future__ import annotations

from typing import Any

import duckdb

from external_analysis.contracts.paths import resolve_similarity_db_path

SIMILARITY_TABLES: tuple[str, ...] = (
    "case_library",
    "case_window_bars",
    "case_embedding_store",
    "case_generation_runs",
    "similarity_shadow_template_rows",
    "similarity_shadow_cases",
    "similarity_quality_metrics",
    "similarity_promotion_reviews",
    "similarity_nightly_summaries",
    "similarity_generation_manifest",
)


def connect_similarity_db(db_path: str | None = None) -> duckdb.DuckDBPyConnection:
    resolved = resolve_similarity_db_path(db_path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(resolved), read_only=False)


def _column_exists(conn: duckdb.DuckDBPyConnection, table_name: str, column_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'main' AND table_name = ? AND column_name = ?
        LIMIT 1
        """,
        [table_name, column_name],
    ).fetchone()
    return bool(row)


def ensure_similarity_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS case_library (
            case_id TEXT PRIMARY KEY,
            query_source TEXT NOT NULL,
            case_type TEXT NOT NULL,
            anchor_type TEXT,
            code TEXT NOT NULL,
            anchor_date INTEGER NOT NULL,
            asof_start_date INTEGER NOT NULL,
            asof_end_date INTEGER NOT NULL,
            outcome_class TEXT NOT NULL,
            success_flag BOOLEAN NOT NULL,
            failure_reason TEXT,
            trade_side TEXT,
            setup_family TEXT,
            break_direction TEXT,
            future_path_signature TEXT NOT NULL,
            embedding_version TEXT NOT NULL,
            source_snapshot_id TEXT NOT NULL,
            generation_run_id TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS case_window_bars (
            case_id TEXT NOT NULL,
            rel_day INTEGER NOT NULL,
            trade_date INTEGER NOT NULL,
            close_norm DOUBLE,
            volume_norm DOUBLE,
            ma20_gap DOUBLE,
            generation_run_id TEXT NOT NULL,
            PRIMARY KEY (case_id, rel_day)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS case_embedding_store (
            case_id TEXT NOT NULL,
            embedding_version TEXT NOT NULL,
            embedding_role TEXT NOT NULL DEFAULT 'champion',
            vector_json JSON NOT NULL,
            generated_at TIMESTAMP NOT NULL,
            PRIMARY KEY (case_id, embedding_version)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS case_generation_runs (
            run_id TEXT PRIMARY KEY,
            kind TEXT NOT NULL,
            status TEXT NOT NULL,
            as_of_date DATE NOT NULL,
            publish_id TEXT,
            started_at TIMESTAMP NOT NULL,
            finished_at TIMESTAMP,
            summary_json JSON NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS similarity_shadow_template_rows (
            template_key TEXT NOT NULL,
            run_id TEXT NOT NULL,
            query_case_id TEXT NOT NULL,
            query_code TEXT NOT NULL,
            embedding_version TEXT NOT NULL,
            neighbor_rank INTEGER NOT NULL,
            case_id TEXT NOT NULL,
            neighbor_code TEXT NOT NULL,
            outcome_class TEXT NOT NULL,
            success_flag BOOLEAN NOT NULL,
            similarity_score DOUBLE,
            created_at TIMESTAMP NOT NULL,
            PRIMARY KEY (template_key, query_case_id, neighbor_rank)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS similarity_shadow_cases (
            run_id TEXT NOT NULL,
            publish_id TEXT NOT NULL,
            as_of_date DATE NOT NULL,
            query_case_id TEXT NOT NULL,
            query_code TEXT NOT NULL,
            embedding_version TEXT NOT NULL,
            neighbor_rank INTEGER NOT NULL,
            case_id TEXT NOT NULL,
            neighbor_code TEXT NOT NULL,
            outcome_class TEXT NOT NULL,
            success_flag BOOLEAN NOT NULL,
            similarity_score DOUBLE,
            created_at TIMESTAMP NOT NULL,
            PRIMARY KEY (run_id, query_case_id, neighbor_rank)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS similarity_quality_metrics (
            run_id TEXT PRIMARY KEY,
            publish_id TEXT NOT NULL,
            as_of_date DATE NOT NULL,
            engine_role TEXT NOT NULL DEFAULT 'champion',
            baseline_version TEXT NOT NULL DEFAULT 'deterministic_similarity_v1',
            embedding_version TEXT NOT NULL,
            comparison_target_version TEXT NOT NULL DEFAULT 'deterministic_similarity_v1',
            top_k INTEGER NOT NULL,
            case_count INTEGER NOT NULL,
            success_count INTEGER NOT NULL,
            failure_count INTEGER NOT NULL,
            big_drop_count INTEGER NOT NULL,
            query_count INTEGER NOT NULL,
            returned_case_count INTEGER NOT NULL,
            returned_path_count INTEGER NOT NULL,
            avg_similarity_score DOUBLE,
            overlap_at_k DOUBLE,
            success_hit_rate_at_k DOUBLE,
            failure_hit_rate_at_k DOUBLE,
            big_drop_hit_rate_at_k DOUBLE,
            scope_type TEXT,
            scope_id TEXT,
            producer_work_id TEXT,
            created_at TIMESTAMP NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS similarity_promotion_reviews (
            review_id TEXT PRIMARY KEY,
            publish_id TEXT NOT NULL,
            as_of_date DATE NOT NULL,
            champion_version TEXT NOT NULL,
            challenger_version TEXT NOT NULL,
            required_streak INTEGER NOT NULL,
            observed_streak INTEGER NOT NULL,
            overlap_at_k DOUBLE,
            success_hit_rate_at_k DOUBLE,
            champion_success_hit_rate_at_k DOUBLE,
            big_drop_hit_rate_at_k DOUBLE,
            champion_big_drop_hit_rate_at_k DOUBLE,
            pass_gate BOOLEAN NOT NULL,
            reason_codes JSON NOT NULL,
            created_at TIMESTAMP NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS similarity_nightly_summaries (
            summary_id TEXT PRIMARY KEY,
            publish_id TEXT NOT NULL,
            as_of_date DATE NOT NULL,
            champion_version TEXT NOT NULL,
            challenger_version TEXT NOT NULL,
            summary_json JSON NOT NULL,
            created_at TIMESTAMP NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS similarity_generation_manifest (
            generation_key TEXT PRIMARY KEY,
            source_signature TEXT NOT NULL,
            dependency_version TEXT NOT NULL,
            cache_state TEXT NOT NULL,
            dirty_ranges_json JSON NOT NULL,
            row_count INTEGER NOT NULL,
            generation_run_id TEXT NOT NULL,
            updated_at TIMESTAMP NOT NULL
        )
        """
    )
    if not _column_exists(conn, "case_embedding_store", "embedding_role"):
        conn.execute("ALTER TABLE case_embedding_store ADD COLUMN embedding_role TEXT DEFAULT 'champion'")
        conn.execute("UPDATE case_embedding_store SET embedding_role = 'champion' WHERE embedding_role IS NULL")
    if not _column_exists(conn, "case_library", "trade_side"):
        conn.execute("ALTER TABLE case_library ADD COLUMN trade_side TEXT")
    if not _column_exists(conn, "case_library", "setup_family"):
        conn.execute("ALTER TABLE case_library ADD COLUMN setup_family TEXT")
    if not _column_exists(conn, "case_library", "break_direction"):
        conn.execute("ALTER TABLE case_library ADD COLUMN break_direction TEXT")
    if not _column_exists(conn, "similarity_quality_metrics", "engine_role"):
        conn.execute("ALTER TABLE similarity_quality_metrics ADD COLUMN engine_role TEXT DEFAULT 'champion'")
        conn.execute("UPDATE similarity_quality_metrics SET engine_role = 'champion' WHERE engine_role IS NULL")
    if not _column_exists(conn, "similarity_quality_metrics", "baseline_version"):
        conn.execute("ALTER TABLE similarity_quality_metrics ADD COLUMN baseline_version TEXT DEFAULT 'deterministic_similarity_v1'")
        conn.execute("UPDATE similarity_quality_metrics SET baseline_version = 'deterministic_similarity_v1' WHERE baseline_version IS NULL")
    if not _column_exists(conn, "similarity_quality_metrics", "comparison_target_version"):
        conn.execute("ALTER TABLE similarity_quality_metrics ADD COLUMN comparison_target_version TEXT")
        conn.execute("UPDATE similarity_quality_metrics SET comparison_target_version = 'deterministic_similarity_v1' WHERE comparison_target_version IS NULL")
    if not _column_exists(conn, "similarity_quality_metrics", "overlap_at_k"):
        conn.execute("ALTER TABLE similarity_quality_metrics ADD COLUMN overlap_at_k DOUBLE")
    if not _column_exists(conn, "similarity_quality_metrics", "success_hit_rate_at_k"):
        conn.execute("ALTER TABLE similarity_quality_metrics ADD COLUMN success_hit_rate_at_k DOUBLE")
    if not _column_exists(conn, "similarity_quality_metrics", "failure_hit_rate_at_k"):
        conn.execute("ALTER TABLE similarity_quality_metrics ADD COLUMN failure_hit_rate_at_k DOUBLE")
    if not _column_exists(conn, "similarity_quality_metrics", "big_drop_hit_rate_at_k"):
        conn.execute("ALTER TABLE similarity_quality_metrics ADD COLUMN big_drop_hit_rate_at_k DOUBLE")
    if not _column_exists(conn, "similarity_quality_metrics", "scope_type"):
        conn.execute("ALTER TABLE similarity_quality_metrics ADD COLUMN scope_type TEXT")
    if not _column_exists(conn, "similarity_quality_metrics", "scope_id"):
        conn.execute("ALTER TABLE similarity_quality_metrics ADD COLUMN scope_id TEXT")
    if not _column_exists(conn, "similarity_quality_metrics", "producer_work_id"):
        conn.execute("ALTER TABLE similarity_quality_metrics ADD COLUMN producer_work_id TEXT")
    conn.execute(
        """
        UPDATE similarity_quality_metrics
        SET scope_type = CASE
                WHEN publish_id LIKE 'replay_%' THEN 'replay'
                ELSE 'nightly'
            END
        WHERE scope_type IS NULL
        """
    )
    conn.execute(
        """
        UPDATE similarity_quality_metrics
        SET scope_id = CASE
                WHEN publish_id LIKE 'replay_%' THEN regexp_extract(publish_id, '^replay_(.*)_\\d{4}-\\d{2}-\\d{2}$', 1)
                ELSE 'nightly'
            END
        WHERE scope_id IS NULL
        """
    )


def ensure_similarity_db(db_path: str | None = None) -> dict[str, Any]:
    conn = connect_similarity_db(db_path)
    try:
        ensure_similarity_schema(conn)
        conn.execute("CHECKPOINT")
        return {"ok": True, "db_path": str(resolve_similarity_db_path(db_path)), "tables": list(SIMILARITY_TABLES)}
    finally:
        conn.close()
