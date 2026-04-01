from __future__ import annotations

from app.backend.api.routers import ticker as ticker_router


def test_build_research_prior_summary_includes_decision_signal_fields(monkeypatch) -> None:
    snapshot = {
        "run_id": "decision_signal_prior_20260331_close",
        "strategy_id": "meemee_decision_signal_prior_v1",
        "up": {
            "asof": "2026-03-31",
            "codes": ["1301"],
            "rank_map": {"1301": 1},
            "signal_strength_map": {"1301": 0.84},
            "promotion_stage_map": {"1301": "weighted"},
            "decision_reason_map": {"1301": ["月足box文脈"]},
            "risk_watch_map": {"1301": ["値幅荒い"]},
            "provisional_map": {"1301": False},
            "hypothesis_family_map": {"1301": "monthly_box_breakout"},
            "bonus_map": {"1301": 0.018},
        },
        "down": {"asof": None, "codes": [], "rank_map": {}},
    }
    monkeypatch.setattr(ticker_router.rankings_cache, "_load_research_prior_snapshot", lambda: snapshot)

    summary = ticker_router._build_research_prior_summary("1301")

    assert summary is not None
    assert summary["runId"] == "decision_signal_prior_20260331_close"
    assert summary["up"]["signalStrength"] == 0.84
    assert summary["up"]["promotionStage"] == "weighted"
    assert summary["up"]["decisionReasons"] == ["月足box文脈"]
    assert summary["up"]["riskWatch"] == ["値幅荒い"]
    assert summary["up"]["hypothesisFamily"] == "monthly_box_breakout"
