from __future__ import annotations

from datetime import datetime, timezone

from app.backend.api.routers import rankings as rankings_router
from app.backend.services import rankings_cache


def test_tradable_rank_filter_excludes_watch_and_fallback() -> None:
    items = [
        {
            "code": "1111",
            "entryQualified": True,
            "entryQualifiedByFallback": False,
            "setupType": "breakout",
        },
        {
            "code": "2222",
            "entryQualified": True,
            "entryQualifiedByFallback": False,
            "setupType": "watch",
        },
        {
            "code": "3333",
            "entryQualified": True,
            "entryQualifiedByFallback": True,
            "setupType": "breakout",
        },
    ]

    filtered = rankings_cache._filter_tradable_rank_items(items, direction="up")  # type: ignore[attr-defined]

    assert [item["code"] for item in filtered] == ["1111"]


def test_rankings_router_defaults_to_trade_mode(monkeypatch) -> None:
    captured: list[str] = []

    def _fake_get_rankings(tf, which, direction, limit, *, mode, risk_mode):
        captured.append(str(mode))
        return {"items": [], "mode": mode, "tf": tf, "which": which, "dir": direction, "limit": limit}

    def _fake_get_last_qualified_trace(tf, which, direction, limit, *, mode, risk_mode, lookback_days, recent_hits, as_of):
        captured.append(str(mode))
        return {"items": [], "mode": mode, "tf": tf, "which": which, "dir": direction, "limit": limit}

    def _fake_get_trade_direction_summary(tf, which, limit, *, risk_mode, top_n):
        return {"tf": tf, "which": which, "limit": limit, "risk_mode": risk_mode, "top_n": top_n}

    def _fake_get_trade_code_qualification_summary(tf, which, code, *, risk_mode, lookback_days, recent_hits, as_of, limit):
        return {"tf": tf, "which": which, "code": code, "risk_mode": risk_mode, "lookback_days": lookback_days, "recent_hits": recent_hits, "as_of": as_of, "limit": limit}

    monkeypatch.setattr(rankings_router.rankings_cache, "get_rankings", _fake_get_rankings)
    monkeypatch.setattr(rankings_router.rankings_cache, "get_last_qualified_trace", _fake_get_last_qualified_trace)
    monkeypatch.setattr(rankings_router.rankings_cache, "get_trade_direction_summary", _fake_get_trade_direction_summary)
    monkeypatch.setattr(rankings_router.rankings_cache, "get_trade_code_qualification_summary", _fake_get_trade_code_qualification_summary)

    assert rankings_router.get_rankings(tf="D", which="latest", dir="up", mode="trade", risk_mode="balanced", limit=50)[
        "mode"
    ] == "trade"
    assert (
        rankings_router.get_rankings_multi(which="latest", dir="up", mode="trade", risk_mode="balanced", limit=50)[
            "mode"
        ]
        == "trade"
    )
    assert (
        rankings_router.get_rankings_last_qualified_trace(
            tf="D",
            which="latest",
            dir="up",
            mode="trade",
            risk_mode="balanced",
            limit=50,
            lookback_days=260,
            recent_hits=10,
            as_of=None,
        )["mode"]
        == "trade"
    )
    assert rankings_router.get_rankings_trade_summary(tf="D", which="latest", risk_mode="balanced", limit=50, top_n=5)[
        "top_n"
    ] == 5
    assert rankings_router.get_rankings_code_qualified_trace(
        code="7203",
        tf="D",
        which="latest",
        risk_mode="balanced",
        lookback_days=260,
        recent_hits=10,
        as_of=None,
        limit=200,
    )["code"] == "7203"
    assert captured == ["trade", "trade", "trade", "trade", "trade"]


def test_trade_mode_build_response_filters_non_tradable(monkeypatch) -> None:
    now = datetime.now(timezone.utc)
    rankings_cache._RESULT_CACHE = {}  # type: ignore[attr-defined]
    rankings_cache._RESULT_CACHE_GENERATION = 0  # type: ignore[attr-defined]
    rankings_cache._CACHE = {  # type: ignore[attr-defined]
        ("D", "latest", "up"): [
            {"code": "1111", "changePct": 0.2, "asOf": "2024-02-29"},
            {"code": "2222", "changePct": 0.3, "asOf": "2024-02-29"},
            {"code": "3333", "changePct": 0.4, "asOf": "2024-02-29"},
        ],
    }
    rankings_cache._LAST_UPDATED = now  # type: ignore[attr-defined]

    def _fake_apply_ml_mode(items, *, direction, mode, limit, risk_mode):
        _ = (direction, mode, limit, risk_mode)
        return (
            [
                {
                    "code": "1111",
                    "entryQualified": True,
                    "setupType": "breakout",
                    "monthlyBoxState": "box_upper",
                    "changePct": 0.2,
                },
                {
                    "code": "2222",
                    "entryQualified": True,
                    "setupType": "watch",
                    "monthlyBoxState": "no_box",
                    "changePct": 0.3,
                },
                {
                    "code": "3333",
                    "entryQualified": True,
                    "setupType": "breakout",
                    "entryQualifiedByFallback": True,
                    "monthlyBoxState": "box_upper",
                    "changePct": 0.4,
                },
            ],
            20240229,
            "model_v1",
        )

    monkeypatch.setattr(rankings_cache, "_call_apply_ml_mode", _fake_apply_ml_mode)
    monkeypatch.setattr(rankings_cache, "is_legacy_analysis_disabled", lambda: False)
    result = rankings_cache._build_rankings_response(  # type: ignore[attr-defined]
        "D",
        "latest",
        "up",
        50,
        mode="trade",
        risk_mode="balanced",
        cache_generation=0,
    )

    assert result["mode"] == "trade"
    assert [item["code"] for item in result["items"]] == ["1111"]


def test_trade_mode_build_response_returns_only_strict_tradable_items(monkeypatch) -> None:
    now = datetime.now(timezone.utc)
    rankings_cache._RESULT_CACHE = {}  # type: ignore[attr-defined]
    rankings_cache._RESULT_CACHE_GENERATION = 0  # type: ignore[attr-defined]
    rankings_cache._CACHE = {  # type: ignore[attr-defined]
        ("D", "latest", "up"): [
            {
                "code": "1111",
                "changePct": 0.2,
                "asOf": "2024-02-29",
                "swingScore": 0.10,
                "swingLongScore": 0.20,
                "weeklyBreakoutUpProb": 0.25,
                "monthlyBreakoutUpProb": 0.30,
            },
            {
                "code": "2222",
                "changePct": 0.3,
                "asOf": "2024-02-29",
                "swingScore": 0.90,
                "swingLongScore": 0.80,
                "weeklyBreakoutUpProb": 0.85,
                "monthlyBreakoutUpProb": 0.88,
            },
        ],
    }
    rankings_cache._LAST_UPDATED = now  # type: ignore[attr-defined]

    def _fake_apply_ml_mode(items, *, direction, mode, limit, risk_mode):
        _ = (items, direction, mode, limit, risk_mode)
        return (
            [
                {
                    "code": "1111",
                    "entryQualified": False,
                    "setupType": "watch",
                    "monthlyBoxState": "no_box",
                    "changePct": 0.2,
                    "close": 100.0,
                    "mlPUp": 0.24,
                    "mlEv20Net": -0.03,
                },
                {
                    "code": "2222",
                    "entryQualified": True,
                    "entryQualifiedByFallback": False,
                    "setupType": "breakout",
                    "monthlyBoxState": "box_upper",
                    "changePct": 0.3,
                    "close": 100.0,
                    "mlPUp": 0.78,
                    "mlEv20Net": 0.05,
                },
            ],
            None,
            None,
        )

    def _fake_build_swing_plan(
        *,
        code,
        as_of_ymd,
        close,
        p_up,
        p_down,
        p_turn_up,
        p_turn_down,
        ev20_net,
        long_setup_type,
        short_setup_type,
        playbook_bonus_long,
        playbook_bonus_short,
        short_score,
        atr_pct,
        liquidity20d,
        decision_tone,
        hold_days_long,
        hold_days_short,
    ):
        _ = (
            code,
            as_of_ymd,
            close,
            p_down,
            p_turn_up,
            p_turn_down,
            ev20_net,
            long_setup_type,
            short_setup_type,
            playbook_bonus_long,
            playbook_bonus_short,
            short_score,
            atr_pct,
            liquidity20d,
            decision_tone,
            hold_days_long,
            hold_days_short,
        )
        score = float(p_up or 0.0)
        return {
            "diagnostics": {
                "long": {
                    "score": score,
                    "qualified": True,
                    "reasons": [f"score={score:.2f}"],
                },
                "short": {
                    "score": 1.0 - score,
                    "qualified": False,
                    "reasons": [],
                },
            },
            "plan": None,
        }

    monkeypatch.setattr(rankings_cache, "_call_apply_ml_mode", _fake_apply_ml_mode)
    monkeypatch.setattr(rankings_cache.swing_plan_service, "build_swing_plan", _fake_build_swing_plan)
    monkeypatch.setattr(rankings_cache, "is_legacy_analysis_disabled", lambda: False)
    result = rankings_cache._build_rankings_response(  # type: ignore[attr-defined]
        "D",
        "latest",
        "up",
        50,
        mode="trade",
        risk_mode="balanced",
        cache_generation=0,
    )

    assert result["mode"] == "trade"
    assert [item["code"] for item in result["items"]] == ["2222"]
    assert all("tradePriorityScore" in item for item in result["items"])
