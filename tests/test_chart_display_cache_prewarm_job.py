from __future__ import annotations

from app.backend.core import chart_display_cache_prewarm_job as mod


def test_collect_chart_display_prewarm_codes_prioritizes_explicit_holdings_favorites_then_ranking(monkeypatch):
    monkeypatch.setenv("MEEMEE_CHART_DISPLAY_PREWARM_CODES", "1111, 2222")
    monkeypatch.setattr(mod, "load_holdings_codes", lambda _db_path: ["2222", "3333"])
    monkeypatch.setattr(mod, "load_favorites_codes", lambda: ["4444"])
    monkeypatch.setattr(mod, "load_ranking_codes", lambda _db_path, _limit: ["5555", "3333"])

    assert mod.collect_chart_display_prewarm_codes(max_codes=5) == ["1111", "2222", "3333", "4444", "5555"]


def test_prewarm_chart_display_cache_skips_when_operator_mutation_is_active(monkeypatch):
    monkeypatch.setattr(mod, "is_operator_mutation_active", lambda: True)

    result = mod.prewarm_chart_display_cache_for_codes(["1111", "2222"], source="test")

    assert result["skipped"] is True
    assert result["reason"] == "operator_mutation_active"
    assert result["warmed"] == 0


def test_prewarm_chart_display_cache_calls_batch_bars_v3(monkeypatch):
    calls = []

    monkeypatch.setattr(mod, "is_operator_mutation_active", lambda: False)
    monkeypatch.setattr(mod, "get_stock_repo", lambda: object())

    def fake_batch_bars_v3(payload, repo):
        calls.append((payload, repo))
        return {
            "items": {payload.codes[0]: {"daily": {"bars": []}}},
            "meta": {
                "display_cache": {
                    "hit": False,
                    "stale": False,
                    "cache_id": f"cache-{payload.codes[0]}",
                }
            },
        }

    monkeypatch.setattr(mod, "batch_bars_v3", fake_batch_bars_v3)

    result = mod.prewarm_chart_display_cache_for_codes(["1111", "1111", "2222"], source="test", per_code_delay_ms=0)

    assert result["requested"] == 2
    assert result["warmed"] == 2
    assert result["failed"] == 0
    assert [call[0].codes for call in calls] == [["1111"], ["2222"]]
    assert calls[0][0].timeframes == ["daily", "weekly", "monthly"]
    assert result["items"][0]["display_cache"]["cache_id"] == "cache-1111"
