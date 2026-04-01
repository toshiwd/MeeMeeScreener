from __future__ import annotations

from datetime import datetime, timezone

from app.services import screener_engine


class _FakeResult:
    def __init__(self, rows: list[tuple]) -> None:
        self._rows = rows

    def fetchall(self) -> list[tuple]:
        return self._rows


class _FakeConn:
    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def execute(self, sql: str, params=None) -> _FakeResult:
        if "SELECT DISTINCT code FROM daily_bars" in sql:
            return _FakeResult([("1111",)])
        if "FROM stock_meta" in sql:
            return _FakeResult([("1111", "Alpha", "WATCH", None, "", None, None, None)])
        if "FROM daily_bars" in sql:
            return _FakeResult([])
        if "FROM monthly_bars" in sql:
            return _FakeResult([])
        if "FROM earnings_planned" in sql:
            return _FakeResult([])
        if "FROM ex_rights" in sql:
            return _FakeResult([])
        raise AssertionError(f"unexpected SQL: {sql}")


def test_build_screener_rows_keeps_display_score_empty_when_only_diagnostic_scores_exist(monkeypatch) -> None:
    monkeypatch.setattr(screener_engine, "get_conn", lambda: _FakeConn())
    monkeypatch.setattr(screener_engine, "jst_now", lambda: datetime(2026, 3, 30, tzinfo=timezone.utc))
    monkeypatch.setattr(screener_engine, "_build_name_map_from_txt", lambda: {})
    monkeypatch.setattr(
        screener_engine,
        "_compute_screener_metrics",
        lambda *_args, **_kwargs: {
            "buyStateScore": 91.0,
            "scores": {"upScore": 88.0, "downScore": 77.0},
            "statusLabel": "WATCH",
            "reasons": [],
        },
    )

    rows = screener_engine._build_screener_rows()  # type: ignore[attr-defined]

    assert len(rows) == 1
    row = rows[0]
    assert row["score"] is None
    assert row["displayScore"] is None
    assert row["displayScoreSource"] == "none"
    assert row["buyStateScore"] == 91.0
