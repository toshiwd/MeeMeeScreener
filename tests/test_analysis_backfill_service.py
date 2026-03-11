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
        if "SELECT DISTINCT dt FROM ml_feature_daily WHERE dt <= ?" in sql:
            assert args == [20260311, 130]
            return _FakeRows([(20260310,), (20260309,)])
        if "SELECT DISTINCT dt FROM ml_feature_daily WHERE" in sql and "BETWEEN ? AND ?" in sql:
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
