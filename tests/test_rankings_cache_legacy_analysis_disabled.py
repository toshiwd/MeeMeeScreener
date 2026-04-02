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


def test_build_rankings_response_keeps_trade_mode_when_legacy_analysis_disabled(monkeypatch):
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "1")

    source_items = [{"code": "2301", "asOf": "2026-03-13"}]

    monkeypatch.setattr(rankings_cache, "_load_live_cache_items", lambda cache_key: (source_items, None))
    monkeypatch.setattr(
        rankings_cache,
        "_decorate_rule_items_with_entry_gate",
        lambda items, direction, risk_mode: [
            {
                "code": "2301",
                "asOf": "2026-03-13",
                "entryQualified": True,
                "setupType": "breakout",
                "monthlyBoxState": "box_upper",
            }
        ],
    )
    monkeypatch.setattr(rankings_cache, "_fallback_down_ml_items_when_empty", lambda **kwargs: (kwargs["out_items"], kwargs["pred_dt"], kwargs["model_version"]))
    monkeypatch.setattr(rankings_cache, "_attach_quality_flags", lambda items, mode, direction, now_ymd=None: items)
    monkeypatch.setattr(rankings_cache, "_attach_swing_fields", lambda items, direction: items)
    monkeypatch.setattr(rankings_cache, "_call_apply_ml_mode", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("trade should not use ml path when legacy analysis is disabled")))

    payload = rankings_cache._build_rankings_response(  # type: ignore[attr-defined]
        "D",
        "latest",
        "up",
        10,
        mode="trade",
        risk_mode="balanced",
        cache_generation=1,
    )

    assert payload["mode"] == "trade"
    assert payload["legacy_analysis_disabled"] is True
    assert payload["candidate_source"] == "current_features"
    assert [item["code"] for item in payload["items"]] == ["2301"]


def test_get_rankings_asof_uses_rule_gate_when_trade_and_legacy_analysis_disabled(monkeypatch):
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "1")

    monkeypatch.setattr(rankings_cache, "_ensure_cache_fresh_stale_ok", lambda key: None)
    monkeypatch.setattr(
        rankings_cache,
        "_get_asof_base_cache",
        lambda as_of_int: {
            ("D", "latest", "up"): [{"code": "4301", "asOf": "2026-03-13"}],
        },
    )
    monkeypatch.setattr(
        rankings_cache,
        "_decorate_rule_items_with_entry_gate",
        lambda items, direction, risk_mode: [
            {
                "code": "4301",
                "asOf": "2026-03-13",
                "entryQualified": True,
                "setupType": "breakout",
                "monthlyBoxState": "box_upper",
            }
        ],
    )
    monkeypatch.setattr(rankings_cache, "_fallback_down_ml_items_when_empty", lambda **kwargs: (kwargs["out_items"], kwargs["pred_dt"], kwargs["model_version"]))
    monkeypatch.setattr(rankings_cache, "_attach_quality_flags", lambda items, mode, direction, now_ymd=None: items)
    monkeypatch.setattr(rankings_cache, "_attach_swing_fields", lambda items, direction: items)
    monkeypatch.setattr(rankings_cache, "_call_apply_ml_mode", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("trade asof should not use ml path when legacy analysis is disabled")))

    payload = rankings_cache.get_rankings_asof(
        "D",
        "latest",
        "up",
        10,
        as_of="2026-03-13",
        mode="trade",
        risk_mode="balanced",
    )

    assert payload["mode"] == "trade"
    assert payload["legacy_analysis_disabled"] is True
    assert payload["candidate_source"] == "current_features"
    assert [item["code"] for item in payload["items"]] == ["4301"]


def test_fetch_recent_asof_dates_uses_daily_bars_when_legacy_analysis_disabled(monkeypatch):
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "1")

    class _FakeConn:
        def __init__(self):
            self.executed: list[tuple[str, list[int]]] = []

        def execute(self, query, params):
            self.executed.append((query, list(params)))

            class _FakeResult:
                def fetchall(self_inner):
                    return [(20260313,), (20260312,), (20260311,)]

            return _FakeResult()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    fake_conn = _FakeConn()
    monkeypatch.setattr(rankings_cache, "get_conn", lambda: fake_conn)

    result = rankings_cache._fetch_recent_asof_dates(as_of_int=20260313, lookback_days=20)  # type: ignore[attr-defined]

    assert result == [20260313, 20260312, 20260311]
    assert fake_conn.executed
    assert "FROM daily_bars" in fake_conn.executed[0][0]
