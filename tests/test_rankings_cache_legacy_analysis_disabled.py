from __future__ import annotations

from datetime import datetime, timezone

from app.backend.services.ml import rankings_cache


def test_build_rankings_response_falls_back_to_rule_when_legacy_analysis_disabled(monkeypatch):
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "1")

    source_items = [{"code": "1301", "asOf": "2026-03-13"}]

    monkeypatch.setattr(rankings_cache, "_load_live_cache_items", lambda cache_key, **kwargs: (source_items, None, None))
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

    monkeypatch.setattr(rankings_cache, "_load_live_cache_items", lambda cache_key, **kwargs: (source_items, None, None))
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


def test_build_rankings_response_exposes_explicit_trade_buckets(monkeypatch):
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "1")

    source_items = [
        {"code": "2301", "asOf": "2026-03-13", "setupType": "breakout", "monthlyBoxState": "box_upper"},
        {"code": "9999", "asOf": "2026-03-13", "setupType": "watch", "monthlyBoxState": "box_mid"},
    ]

    monkeypatch.setattr(rankings_cache, "_load_live_cache_items", lambda cache_key, **kwargs: (source_items, None, None))
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
                "tradeEntryClass": "box_upper_breakout",
            },
            {
                "code": "9999",
                "asOf": "2026-03-13",
                "entryQualified": False,
                "setupType": "watch",
                "monthlyBoxState": "box_mid",
            },
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
    assert [item["code"] for item in payload["items"]] == ["2301"]
    assert [item["code"] for item in payload["actionable_buy_candidates"]] == ["2301"]
    assert payload["actionable_short_candidates"] == []
    assert [item["code"] for item in payload["caution_watch_candidates"]] == ["9999"]


def test_get_trade_direction_summary_uses_explicit_buckets(monkeypatch):
    def _fake_get_rankings(tf, which, direction, limit, *, mode, risk_mode, include_provisional=False):
        if direction == "up":
            return {
                "candidate_source": "current_features",
                "legacy_analysis_disabled": True,
                "freshness_state": "fresh",
                "freshness_days": 1,
                "snapshot_as_of": "2026-04-20",
                "stale": False,
                "actionable_buy_candidates": [
                    {"code": "2301", "tradePriorityScore": 0.9, "tradePriorityProfitScore": 0.1, "tradePriorityHitScore": 0.2}
                ],
                "actionable_short_candidates": [],
                "caution_watch_candidates": [
                    {"code": "9999", "tradePriorityScore": 0.1, "tradePriorityProfitScore": -0.1, "tradePriorityHitScore": 0.0}
                ],
                "items": [{"code": "2301", "tradePriorityScore": 0.9, "tradePriorityProfitScore": 0.1, "tradePriorityHitScore": 0.2}],
            }
        return {
            "candidate_source": "current_features",
            "legacy_analysis_disabled": True,
            "freshness_state": "fresh",
            "freshness_days": 1,
            "snapshot_as_of": "2026-04-20",
            "stale": False,
            "actionable_buy_candidates": [],
            "actionable_short_candidates": [],
            "caution_watch_candidates": [],
            "items": [],
        }

    monkeypatch.setattr(rankings_cache, "get_rankings", _fake_get_rankings)

    summary = rankings_cache.get_trade_direction_summary("D", "latest", 10, risk_mode="balanced", top_n=5)

    assert summary["actionable_buy"]["count"] == 1
    assert summary["actionable_short"]["count"] == 0
    assert summary["caution_watch"]["count"] == 1
    assert summary["buy"]["count"] == 1
    assert summary["sell"]["count"] == 0


def test_get_rankings_session_bundle_separates_confirmed_and_provisional(monkeypatch):
    def _fake_get_rankings(tf, which, direction, limit, *, mode, risk_mode, include_provisional=False):
        base = {
            "snapshot_as_of": "2026-04-20",
            "actionable_buy_candidates": [{"code": "1301"}],
            "actionable_short_candidates": [],
            "caution_watch_candidates": [{"code": "7203"}],
            "items": [{"code": "1301"}, {"code": "7203"}],
        }
        if include_provisional:
            return {
                **base,
                "snapshot_as_of": "2026-04-21",
                "provisional_snapshot_as_of": "2026-04-21",
                "provisional_source": "yahoo_intraday_unconfirmed_source",
                "provisional_freshness_state": "fresh",
                "is_provisional": True,
                "provisional_fetched_at": "2026-04-21T12:34:56+09:00",
                "actionable_buy_candidates": [{"code": "1301", "is_provisional": True, "confirmed_rank": 1, "provisional_rank": 1, "rank_delta": 0}],
                "actionable_short_candidates": [],
                "caution_watch_candidates": [{"code": "7203", "is_provisional": True, "confirmed_rank": 2, "provisional_rank": 2, "rank_delta": 0}],
                "items": [{"code": "1301", "is_provisional": True, "confirmed_rank": 1, "provisional_rank": 1, "rank_delta": 0}],
            }
        return {
            **base,
            "confirmed_snapshot_as_of": "2026-04-20",
            "confirmed_actionable_buy_candidates": [{"code": "1301"}],
            "confirmed_actionable_short_candidates": [],
            "confirmed_caution_watch_candidates": [{"code": "7203"}],
            "is_provisional": False,
        }

    monkeypatch.setattr(rankings_cache, "get_rankings", _fake_get_rankings)

    bundle = rankings_cache.get_rankings_session_bundle("D", "latest", "up", 10, mode="trade", risk_mode="balanced")

    assert bundle["confirmed_snapshot_as_of"] == "2026-04-20"
    assert bundle["provisional_snapshot_as_of"] == "2026-04-21"
    assert bundle["provisional_source"] == "yahoo_intraday_unconfirmed_source"
    assert bundle["is_provisional"] is True
    assert [item["code"] for item in bundle["confirmed_actionable_buy_candidates"]] == ["1301"]
    assert [item["code"] for item in bundle["provisional_actionable_buy_candidates"]] == ["1301"]
    assert bundle["provisional_actionable_short_candidates"] == []
    assert [item["code"] for item in bundle["confirmed_caution_watch_candidates"]] == ["7203"]
    assert [item["code"] for item in bundle["provisional_caution_watch_candidates"]] == ["7203"]
    assert bundle["provisional_items"][0]["rank_delta"] == 0


def test_rankings_session_bundle_marks_unavailable_provisional_explicitly(monkeypatch):
    def _fake_get_rankings(tf, which, direction, limit, *, mode, risk_mode, include_provisional=False):
        if include_provisional:
            return {
                "snapshot_as_of": "2026-04-20",
                "provisional_snapshot_as_of": None,
                "provisional_source": None,
                "provisional_freshness_state": "unavailable",
                "is_provisional": False,
                "provisional_fetched_at": "2026-04-21T12:34:56+09:00",
                "actionable_buy_candidates": [],
                "actionable_short_candidates": [],
                "caution_watch_candidates": [],
                "items": [],
            }
        return {
            "snapshot_as_of": "2026-04-20",
            "confirmed_snapshot_as_of": "2026-04-20",
            "confirmed_actionable_buy_candidates": [{"code": "1301"}],
            "confirmed_actionable_short_candidates": [],
            "confirmed_caution_watch_candidates": [],
            "actionable_buy_candidates": [{"code": "1301"}],
            "actionable_short_candidates": [],
            "caution_watch_candidates": [],
            "items": [{"code": "1301"}],
            "is_provisional": False,
        }

    monkeypatch.setattr(rankings_cache, "get_rankings", _fake_get_rankings)

    bundle = rankings_cache.get_rankings_session_bundle("D", "latest", "up", 10, mode="trade", risk_mode="balanced")

    assert bundle["provisional_freshness_state"] == "unavailable"
    assert bundle["is_provisional"] is False
    assert bundle["provisional_actionable_buy_candidates"] == []
    assert bundle["provisional_actionable_short_candidates"] == []
    assert bundle["provisional_caution_watch_candidates"] == []


def test_provisional_result_cache_key_is_distinct(monkeypatch):
    confirmed_variant = rankings_cache._current_result_cache_variant(include_provisional=False)  # type: ignore[attr-defined]
    provisional_variant = rankings_cache._current_result_cache_variant(include_provisional=True)  # type: ignore[attr-defined]
    assert confirmed_variant != provisional_variant
    assert confirmed_variant[-2] is False
    assert provisional_variant[-2] is True
    assert provisional_variant[-1]


def test_load_analysis_provisional_overlay_reports_partial_coverage(monkeypatch):
    import app.backend.services.data.yahoo_provisional as yp

    monkeypatch.setattr(rankings_cache, "_PROVISIONAL_MIN_COVERAGE_RATIO", 0.6)
    monkeypatch.setattr(rankings_cache, "_PROVISIONAL_ALLOW_PARTIAL", True)

    sample_row = (20260421, 100.0, 101.0, 99.0, 100.5, 123.0)
    monkeypatch.setattr(
        yp,
        "get_provisional_daily_rows_from_spark",
        lambda codes, *, prefer_chart_ohlc=False: {code: sample_row for code in codes[:2]},
    )

    provisional_map, meta = rankings_cache._load_analysis_provisional_overlay(["1111", "2222", "3333"])  # type: ignore[attr-defined]

    assert list(provisional_map.keys()) == ["1111", "2222"]
    assert meta["provisional_freshness_state"] == "partial"
    assert meta["is_provisional"] is True
    assert meta["provisional_requested_symbols"] == 3
    assert meta["provisional_covered_symbols"] == 2
    assert meta["provisional_missing_symbols"] == 1
    assert meta["provisional_complete_ohlcv_symbols"] == 2
    assert meta["provisional_same_day_symbols"] == 2
    assert meta["provisional_same_day_ratio"] == 1.0
    assert meta["provisional_coverage_ratio"] == 0.666667
    assert meta["provisional_snapshot_as_of"] == "2026-04-21"
    assert meta["provisional_missing_reason_summary"] == {"fetch_none": 1}


def test_get_rankings_session_bundle_preserves_partial_item_flags(monkeypatch):
    def _fake_get_rankings(tf, which, direction, limit, *, mode, risk_mode, include_provisional=False):
        base = {
            "snapshot_as_of": "2026-04-20",
            "actionable_buy_candidates": [{"code": "1301"}],
            "actionable_short_candidates": [],
            "caution_watch_candidates": [{"code": "7203"}],
            "items": [{"code": "1301"}, {"code": "7203"}],
        }
        if include_provisional:
            return {
                **base,
                "snapshot_as_of": "2026-04-21",
                "provisional_snapshot_as_of": "2026-04-21",
                "provisional_source": "yahoo_intraday_unconfirmed_source",
                "provisional_freshness_state": "partial",
                "is_provisional": True,
                "provisional_fetched_at": "2026-04-21T12:34:56+09:00",
                "provisional_requested_symbols": 2,
                "provisional_covered_symbols": 1,
                "provisional_complete_ohlcv_symbols": 1,
                "provisional_same_day_symbols": 1,
                "provisional_missing_symbols": 1,
                "provisional_missing_reason_summary": {"fetch_none": 1},
                "provisional_coverage_ratio": 0.5,
                "provisional_same_day_ratio": 0.5,
                "provisional_min_coverage_ratio": 0.95,
                "provisional_min_same_day_ratio": 0.95,
                "provisional_allow_partial": True,
                "provisional_render_mode": "practical_partial",
                "actionable_buy_candidates": [
                    {"code": "1301", "asOf": "2026-04-21", "is_provisional": True, "confirmed_rank": 1, "provisional_rank": 1, "rank_delta": 0}
                ],
                "actionable_short_candidates": [],
                "caution_watch_candidates": [
                    {"code": "7203", "asOf": "2026-04-20", "is_provisional": False, "confirmed_rank": 2, "provisional_rank": 2, "rank_delta": 0}
                ],
                "items": [
                    {"code": "1301", "asOf": "2026-04-21", "is_provisional": True, "confirmed_rank": 1, "provisional_rank": 1, "rank_delta": 0},
                    {"code": "7203", "asOf": "2026-04-20", "is_provisional": False, "confirmed_rank": 2, "provisional_rank": 2, "rank_delta": 0},
                ],
            }
        return {
            **base,
            "confirmed_snapshot_as_of": "2026-04-20",
            "confirmed_actionable_buy_candidates": [{"code": "1301"}],
            "confirmed_actionable_short_candidates": [],
            "confirmed_caution_watch_candidates": [{"code": "7203"}],
            "is_provisional": False,
        }

    monkeypatch.setattr(rankings_cache, "get_rankings", _fake_get_rankings)

    bundle = rankings_cache.get_rankings_session_bundle("D", "latest", "up", 10, mode="trade", risk_mode="balanced")

    assert bundle["provisional_freshness_state"] == "partial"
    assert bundle["is_provisional"] is True
    assert bundle["provisional_requested_symbols"] == 2
    assert bundle["provisional_covered_symbols"] == 1
    assert bundle["provisional_missing_symbols"] == 1
    assert bundle["provisional_coverage_ratio"] == 0.5
    assert bundle["provisional_same_day_ratio"] == 0.5
    assert bundle["provisional_actionable_buy_candidates"][0]["is_provisional"] is True
    assert bundle["provisional_caution_watch_candidates"][0]["is_provisional"] is False
    assert bundle["provisional_items"][0]["is_provisional"] is True
    assert bundle["provisional_items"][1]["is_provisional"] is False


def test_mark_provisional_items_by_snapshot_uses_covered_codes():
    items = [{"code": "1301"}, {"code": "7203"}]
    marked = rankings_cache._mark_provisional_items_by_snapshot(  # type: ignore[attr-defined]
        items,
        provisional_snapshot_as_of="2026-04-21",
        provisional_covered_codes={"1301"},
    )

    assert marked[0]["is_provisional"] is True
    assert marked[1]["is_provisional"] is False


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


def test_get_rankings_marks_stale_when_latest_snapshot_is_old(monkeypatch):
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "1")
    monkeypatch.setattr(rankings_cache, "_ensure_cache_fresh_stale_ok", lambda key: None)
    monkeypatch.setattr(rankings_cache, "is_legacy_analysis_disabled", lambda: True)
    monkeypatch.setattr(rankings_cache, "_current_jst_ymd_int", lambda: 20260421)
    monkeypatch.setattr(
        rankings_cache,
        "_decorate_rule_items_with_entry_gate",
        lambda items, direction, risk_mode: list(items),
    )
    monkeypatch.setattr(
        rankings_cache,
        "_fallback_down_ml_items_when_empty",
        lambda **kwargs: (kwargs["out_items"], kwargs["pred_dt"], kwargs["model_version"]),
    )
    monkeypatch.setattr(rankings_cache, "_attach_quality_flags", lambda items, mode, direction, now_ymd=None: items)
    monkeypatch.setattr(rankings_cache, "_attach_swing_fields", lambda items, direction: items)
    rankings_cache._RESULT_CACHE = {}  # type: ignore[attr-defined]
    rankings_cache._RESULT_CACHE_GENERATION = 0  # type: ignore[attr-defined]
    rankings_cache._CACHE = {  # type: ignore[attr-defined]
        ("D", "latest", "up"): [{"code": "1111", "asOf": "2026-03-19"}],
    }
    rankings_cache._LAST_UPDATED = datetime(2026, 4, 2, tzinfo=timezone.utc)  # type: ignore[attr-defined]
    rankings_cache._LAST_CACHE_DAILY_ASOF_INT = 20260319  # type: ignore[attr-defined]
    rankings_cache._LAST_CACHE_PAN_DAILY_ASOF_INT = 20260319  # type: ignore[attr-defined]
    rankings_cache._LAST_REFRESH_SIGNATURE = ("pan", 20260319)  # type: ignore[attr-defined]

    # The trade-mode path can filter out stub rows after scoring; rule mode keeps the
    # stub visible while still exercising the same freshness metadata path.
    payload = rankings_cache.get_rankings("D", "latest", "up", 10, mode="rule")

    assert payload["freshness_state"] == "stale"
    assert payload["stale"] is True
    assert payload["current_candidate_available"] is False
    assert payload["snapshot_as_of"] == "2026-03-19"
    assert payload["freshness_days"] == 33
    assert payload["items"][0]["code"] == "1111"


def test_cache_needs_refresh_detects_new_signature(monkeypatch):
    rankings_cache._CACHE = {  # type: ignore[attr-defined]
        ("D", "latest", "up"): [{"code": "OLD", "asOf": "2026-03-19"}],
    }
    rankings_cache._LAST_UPDATED = datetime(2026, 4, 2, tzinfo=timezone.utc)  # type: ignore[attr-defined]
    rankings_cache._LAST_DB_MTIME = 1.0  # type: ignore[attr-defined]
    rankings_cache._LAST_CACHE_DAILY_ASOF_INT = 20260319  # type: ignore[attr-defined]
    rankings_cache._LAST_CACHE_PAN_DAILY_ASOF_INT = 20260319  # type: ignore[attr-defined]
    rankings_cache._LAST_REFRESH_SIGNATURE = ("pan", 20260319)  # type: ignore[attr-defined]

    monkeypatch.setattr(rankings_cache, "_db_mtime", lambda: 2.0)
    monkeypatch.setattr(rankings_cache, "_resolve_refresh_signature", lambda: (("pan", 20260320), 2.0, 20260320))
    assert rankings_cache._cache_needs_refresh() is True  # type: ignore[attr-defined]
