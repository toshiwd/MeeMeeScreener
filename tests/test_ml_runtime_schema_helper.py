import duckdb

from app.backend.services.ml.legacy_schema_runtime import ensure_ml_runtime_schema


def test_ensure_ml_runtime_schema_skips_legacy_daily_tables_when_disabled():
    with duckdb.connect(":memory:") as conn:
        ensure_ml_runtime_schema(conn, legacy_schema_enabled=False)
        monthly = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'ml_monthly_label'"
        ).fetchone()
        legacy = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'ml_feature_daily'"
        ).fetchone()
    assert int(monthly[0]) == 1
    assert int(legacy[0]) == 0


def test_ensure_ml_runtime_schema_creates_legacy_daily_tables_when_enabled():
    with duckdb.connect(":memory:") as conn:
        ensure_ml_runtime_schema(conn, legacy_schema_enabled=True)
        legacy = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'ml_feature_daily'"
        ).fetchone()
        audit = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'ml_training_audit'"
        ).fetchone()
    assert int(legacy[0]) == 1
    assert int(audit[0]) == 1
