from __future__ import annotations

import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.backend.services.analysis import analysis_backfill_service


class _FakeRows:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _FakeConn:
    def execute(self, query: str, params=None):
        sql = " ".join(str(query).split())
        args = list(params or [])
        if "SELECT MAX(date) FROM daily_bars WHERE" in sql:
            assert args == [20260311]
            return _FakeRows([(20260311,)])
        if "SELECT DISTINCT CASE WHEN dt >= 1000000000" in sql and "FROM ml_feature_daily" in sql and "LIMIT ?" in sql:
            assert args == [20260311, 130]
            return _FakeRows([(20260310,), (20260309,)])
        if "SELECT DISTINCT CASE WHEN dt >= 1000000000" in sql and "FROM ml_feature_daily" in sql and "BETWEEN ? AND ?" in sql:
            assert args == [20260310, 20260311]
            return _FakeRows([(20260310,)])
        raise AssertionError(f"Unexpected query: {sql} params={args}")


def test_resolve_anchor_dt_prefers_latest_daily_bar_date() -> None:
    assert analysis_backfill_service._resolve_anchor_dt(_FakeConn(), 20260311) == 20260311


def test_resolve_target_dates_appends_provisional_anchor() -> None:
    values = analysis_backfill_service._resolve_target_dates(
        _FakeConn(),
        lookback_days=130,
        anchor_dt=20260311,
    )
    values = analysis_backfill_service._append_anchor_date(values, 20260311)
    assert values == [20260309, 20260310, 20260311]


def test_resolve_target_dates_in_range_appends_bounds() -> None:
    values = analysis_backfill_service._resolve_target_dates_in_range(
        _FakeConn(),
        start_dt=20260310,
        end_dt=20260311,
    )
    assert values == [20260310, 20260311]


def test_inspect_analysis_backfill_coverage_uses_explicit_legacy_schema(monkeypatch) -> None:
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "0")
    called = {"schema": 0}

    class _CoverageConn:
        def execute(self, query: str, params=None):
            sql = " ".join(str(query).split())
            if "SELECT COUNT(*) FROM ml_feature_daily" in sql:
                return _FakeRows([(1,)])
            raise AssertionError(f"Unexpected query: {sql}")

    class _ConnCtx:
        def __enter__(self):
            return _CoverageConn()

        def __exit__(self, exc_type, exc, tb):
            return None

    monkeypatch.setattr(analysis_backfill_service, "get_conn", lambda: _ConnCtx())
    monkeypatch.setattr(
        analysis_backfill_service,
        "ensure_legacy_analysis_schema",
        lambda conn: called.__setitem__("schema", called["schema"] + 1),
    )
    monkeypatch.setattr(
        analysis_backfill_service,
        "_resolve_analysis_cache_coverage",
        lambda *args, **kwargs: {
            "anchor_dt": None,
            "start_dt": None,
            "end_dt": None,
            "target_dates": [],
            "active_ml_model_version": None,
            "sell_calc_version": analysis_backfill_service.SELL_ANALYSIS_CALC_VERSION,
            "missing_ml_dates": [],
            "missing_sell_dates": [],
            "missing_phase_dates": [],
            "force_recompute": False,
        },
    )

    result = analysis_backfill_service.inspect_analysis_backfill_coverage()

    assert called["schema"] == 1
    assert result["covered"] is True
