import duckdb

from app.backend.services.ml import ml_service


def test_ensure_ml_schema_skips_legacy_tables_when_legacy_analysis_disabled(monkeypatch):
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "1")
    with duckdb.connect(":memory:") as conn:
        ml_service._ensure_ml_schema(conn)
        monthly = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'ml_monthly_label'"
        ).fetchone()
        legacy = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'ml_feature_daily'"
        ).fetchone()
    assert int(monthly[0]) == 1
    assert int(legacy[0]) == 0


def test_ensure_ml_schema_creates_legacy_tables_when_legacy_analysis_enabled(monkeypatch):
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "0")
    with duckdb.connect(":memory:") as conn:
        ml_service._ensure_ml_schema(conn)
        legacy = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'ml_feature_daily'"
        ).fetchone()
    assert int(legacy[0]) == 1
