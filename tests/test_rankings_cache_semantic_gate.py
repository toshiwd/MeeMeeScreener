from __future__ import annotations

from datetime import datetime, timezone

from app.backend.services.ml import rankings_cache


def test_analysis_provisional_merge_is_blocked_by_semantic_gate(monkeypatch) -> None:
    today_key = int(datetime.now(timezone.utc).strftime("%Y%m%d"))
    daily_map = {"0001": [(today_key, 100.0, 110.0, 90.0, 105.0, 1000.0)]}
    provisional_row = (today_key, 101.0, 111.0, 91.0, 106.0, 1200.0)

    monkeypatch.setattr(rankings_cache, "_analysis_provisional_enabled", lambda: True)
    monkeypatch.setattr(
        "app.backend.services.data.yahoo_provisional.get_provisional_daily_rows_from_spark",
        lambda codes: {"0001": provisional_row},
    )

    def _fail_merge(*_args, **_kwargs):
        raise AssertionError("semantic gate should block provisional merge before merge helper is called")

    monkeypatch.setattr(
        "app.backend.services.data.yahoo_provisional.merge_daily_rows_with_provisional",
        _fail_merge,
    )
    monkeypatch.setattr(
        "app.backend.services.data.yahoo_provisional.normalize_date_key",
        lambda value: int(value) if value is not None else None,
    )

    result = rankings_cache._merge_analysis_provisional_rows(daily_map, ["0001"])

    assert result == daily_map

