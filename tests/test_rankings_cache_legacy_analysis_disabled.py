from __future__ import annotations

from app.backend.services.ml import rankings_cache


def test_build_rankings_response_falls_back_to_rule_when_legacy_analysis_disabled(monkeypatch):
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "1")

    source_items = [{"code": "1301", "asOf": "2026-03-13"}]

    monkeypatch.setattr(rankings_cache, "_load_live_cache_items", lambda cache_key: (source_items, None))
    monkeypatch.setattr(
        rankings_cache,
        "_decorate_rule_items_with_entry_gate",
        lambda items, direction, risk_mode: [dict(item, entryQualified=False) for item in items],
    )
    monkeypatch.setattr(
        rankings_cache,
        "_fallback_down_ml_items_when_empty",
        lambda **kwargs: (kwargs["out_items"], kwargs["pred_dt"], kwargs["model_version"]),
    )
    monkeypatch.setattr(rankings_cache, "_attach_quality_flags", lambda items, mode, direction, now_ymd=None: items)
    monkeypatch.setattr(rankings_cache, "_attach_swing_fields", lambda items, direction: items)

    def _raise(*args, **kwargs):
        raise AssertionError("ml ranking path should not be called")

    monkeypatch.setattr(rankings_cache, "_call_apply_ml_mode", _raise)
    monkeypatch.setattr(rankings_cache, "_call_apply_monthly_ml_mode", _raise)

    payload = rankings_cache._build_rankings_response(  # type: ignore[attr-defined]
        "D",
        "latest",
        "up",
        10,
        mode="hybrid",
        risk_mode="balanced",
        cache_generation=1,
    )

    assert payload["mode"] == "rule"
    assert payload["items"] == [{"code": "1301", "asOf": "2026-03-13", "entryQualified": False}]


def test_fetch_recent_asof_dates_returns_empty_when_legacy_analysis_disabled(monkeypatch):
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "1")
    assert rankings_cache._fetch_recent_asof_dates(as_of_int=20260313, lookback_days=20) == []  # type: ignore[attr-defined]
