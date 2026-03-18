import duckdb

from app.db.schema import ensure_legacy_analysis_schema, ensure_schema


def _table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema = 'main' AND table_name = ?",
        [table_name],
    ).fetchone()
    return bool(row)


def test_ensure_schema_skips_legacy_analysis_tables_when_disabled(monkeypatch):
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "1")
    conn = duckdb.connect(":memory:")
    try:
        ensure_schema(conn)

        assert _table_exists(conn, "daily_bars")
        assert _table_exists(conn, "stock_meta")
        assert _table_exists(conn, "earnings_planned")
        assert _table_exists(conn, "ex_rights")
        assert _table_exists(conn, "feature_snapshot_daily")

        assert not _table_exists(conn, "label_20d")
        assert not _table_exists(conn, "phase_pred_daily")
        assert not _table_exists(conn, "ml_feature_daily")
        assert not _table_exists(conn, "ml_label_20d")
        assert not _table_exists(conn, "ml_pred_20d")
        assert not _table_exists(conn, "ml_model_registry")
        assert not _table_exists(conn, "ranking_analysis_quality_daily")
        assert not _table_exists(conn, "swing_setup_stats_daily")
    finally:
        conn.close()


def test_ensure_schema_keeps_legacy_analysis_tables_when_enabled(monkeypatch):
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "0")
    conn = duckdb.connect(":memory:")
    try:
        ensure_schema(conn)
        assert _table_exists(conn, "stock_meta")
        assert _table_exists(conn, "earnings_planned")
        assert _table_exists(conn, "ex_rights")
        assert not _table_exists(conn, "ml_feature_daily")
        assert not _table_exists(conn, "ml_pred_20d")
        assert not _table_exists(conn, "ml_model_registry")
        assert not _table_exists(conn, "ranking_analysis_quality_daily")
        assert not _table_exists(conn, "swing_setup_stats_daily")

        ensure_legacy_analysis_schema(conn)
        assert _table_exists(conn, "ml_feature_daily")
        assert _table_exists(conn, "ml_pred_20d")
        assert _table_exists(conn, "ml_model_registry")
    finally:
        conn.close()
