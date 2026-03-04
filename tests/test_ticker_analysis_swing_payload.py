from __future__ import annotations

import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.backend.api.routers import ticker


class _StubRepo:
    def get_ml_analysis_pred(self, code: str, asof_dt):
        return (
            20260304,
            0.60,   # p_up
            0.40,   # p_down
            0.58,   # p_up_5
            0.57,   # p_up_10
            0.55,   # p_turn_up
            0.45,   # p_turn_down
            0.44,   # p_turn_down_5
            0.43,   # p_turn_down_10
            0.42,   # p_turn_down_20
            None,
            None,
            0.018,  # ret_pred20
            0.014,  # ev20
            0.012,  # ev20_net
            0.008,  # ev5_net
            0.010,  # ev10_net
            "vtest",
        )

    def get_daily_bars(self, code: str, limit: int, asof_dt):
        return [
            (20260228, 100.0, 102.0, 99.0, 101.0, 200_000),
            (20260303, 101.0, 103.0, 100.0, 102.0, 210_000),
            (20260304, 102.0, 104.0, 101.0, 103.0, 220_000),
        ]

    def get_monthly_bars(self, code: str, limit: int, asof_dt):
        return [
            (20251231, 90.0, 105.0, 88.0, 100.0, 3_000_000),
            (20260131, 100.0, 108.0, 98.0, 104.0, 3_200_000),
        ]

    def get_buy_stage_precision(self, code: str, asof_dt, lookback_bars: int, horizon: int):
        return None

    def get_sell_analysis_snapshot(self, code: str, asof_dt):
        return None


def test_ticker_analysis_includes_swing_payload(monkeypatch) -> None:
    monkeypatch.setattr(
        ticker.swing_expectancy_service,
        "compute_atr_pct_and_liquidity20d",
        lambda rows: (0.02, 80_000_000.0),
    )
    monkeypatch.setattr(
        ticker.swing_expectancy_service,
        "refresh_swing_setup_stats",
        lambda **kwargs: {"ok": True},
    )
    monkeypatch.setattr(
        ticker,
        "_build_research_prior_summary",
        lambda _code: None,
    )
    monkeypatch.setattr(
        ticker,
        "_build_edinet_summary",
        lambda _code, _asof_dt: None,
    )
    monkeypatch.setattr(
        ticker.swing_plan_service,
        "build_swing_plan",
        lambda **kwargs: {
            "plan": {
                "code": "1301",
                "side": "long",
                "score": 0.71,
                "horizonDays": 20,
                "entry": 103.0,
                "stop": 99.7,
                "tp1": 106.9,
                "tp2": 109.6,
                "timeStopDays": 20,
                "reasons": ["LONG gate=PASS"],
            },
            "diagnostics": {
                "edge": 0.72,
                "risk": 0.28,
                "setupExpectancy": {"setupType": "breakout", "samples": 120},
                "regimeFit": 1.0,
                "atrPct": 0.02,
                "liquidity20d": 80_000_000.0,
            },
        },
    )

    out = ticker.get_analysis_pred(code="1301", risk_mode="balanced", repo=_StubRepo())
    item = out.get("item") or {}
    assert item.get("swingPlan") is not None
    assert item.get("swingDiagnostics") is not None
    assert item["swingPlan"]["side"] == "long"
    assert float(item["swingPlan"]["score"]) >= 0.62
    assert "edge" in item["swingDiagnostics"]
