from __future__ import annotations

from datetime import datetime, timezone

from app.backend.services.ml import rankings_cache


def test_get_rankings_reuses_result_cache_after_cold_refresh(monkeypatch):
    refreshed_at = datetime(2026, 5, 13, 3, 0, tzinfo=timezone.utc)
    build_calls = {"count": 0}

    monkeypatch.setattr(rankings_cache, "_CACHE", {})
    monkeypatch.setattr(rankings_cache, "_LAST_UPDATED", None)
    monkeypatch.setattr(rankings_cache, "_LAST_DB_MTIME", None)
    monkeypatch.setattr(rankings_cache, "_LAST_CACHE_DAILY_ASOF_INT", None)
    monkeypatch.setattr(rankings_cache, "_LAST_CACHE_PAN_DAILY_ASOF_INT", None)
    monkeypatch.setattr(rankings_cache, "_LAST_REFRESH_SIGNATURE", None)
    monkeypatch.setattr(rankings_cache, "_RESULT_CACHE", {})
    monkeypatch.setattr(rankings_cache, "_RESULT_CACHE_GENERATION", 0)
    monkeypatch.setattr(rankings_cache, "_RESULT_REFRESH_IN_PROGRESS", {})
    monkeypatch.setattr(rankings_cache, "_RESULT_REFRESH_LAST_ERROR", {})
    monkeypatch.setattr(rankings_cache, "is_legacy_analysis_disabled", lambda: True)
    monkeypatch.setattr(rankings_cache, "_is_edinet_bonus_enabled", lambda: False)
    monkeypatch.setattr(rankings_cache, "_current_jst_ymd_int", lambda: 20260513)

    def fake_ensure_cache_fresh_stale_ok(*, key):
        with rankings_cache._LOCK:  # type: ignore[attr-defined]
            if rankings_cache._LAST_UPDATED is None:  # type: ignore[attr-defined]
                rankings_cache._CACHE = {key: [{"code": "1111", "asOf": "2026-05-12"}]}  # type: ignore[attr-defined]
                rankings_cache._LAST_UPDATED = refreshed_at  # type: ignore[attr-defined]
                rankings_cache._LAST_CACHE_DAILY_ASOF_INT = 20260512  # type: ignore[attr-defined]
                rankings_cache._LAST_CACHE_PAN_DAILY_ASOF_INT = 20260512  # type: ignore[attr-defined]
                rankings_cache._LAST_REFRESH_SIGNATURE = ("pan", 20260512)  # type: ignore[attr-defined]
                rankings_cache._RESULT_CACHE_GENERATION += 1  # type: ignore[attr-defined]

    def fake_build_rankings_response(*args, cache_generation, **kwargs):
        build_calls["count"] += 1
        return {
            "tf": args[0],
            "which": args[1],
            "dir": args[2],
            "mode": kwargs["mode"],
            "risk_mode": kwargs["risk_mode"],
            "cache_generation": cache_generation,
            "last_updated": refreshed_at.isoformat(),
            "items": [{"code": "1111", "asOf": "2026-05-12"}],
        }

    monkeypatch.setattr(rankings_cache, "_ensure_cache_fresh_stale_ok", fake_ensure_cache_fresh_stale_ok)
    monkeypatch.setattr(rankings_cache, "_build_rankings_response", fake_build_rankings_response)

    first = rankings_cache.get_rankings("D", "latest", "up", 50, mode="trade", risk_mode="balanced")
    second = rankings_cache.get_rankings("D", "latest", "up", 50, mode="trade", risk_mode="balanced")

    assert first["items"][0]["code"] == "1111"
    assert second["items"][0]["code"] == "1111"
    assert build_calls["count"] == 1
