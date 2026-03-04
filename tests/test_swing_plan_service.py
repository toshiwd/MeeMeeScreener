from __future__ import annotations

import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.backend.services import swing_plan_service


def _fake_expectancy(*, side: str, setup_type: str | None, horizon_days: int, as_of_ymd: int | None):
    if side == "long":
        return {
            "asOfYmd": as_of_ymd,
            "side": "long",
            "setupType": setup_type or "breakout",
            "horizonDays": horizon_days,
            "samples": 240,
            "winRate": 0.58,
            "meanRet": 0.022,
            "shrunkMeanRet": 0.020,
            "p25Ret": -0.015,
            "p10Ret": -0.030,
            "maxAdverse": -0.050,
            "sideMeanRet": 0.015,
        }
    return {
        "asOfYmd": as_of_ymd,
        "side": "short",
        "setupType": setup_type or "breakdown",
        "horizonDays": horizon_days,
        "samples": 240,
        "winRate": 0.57,
        "meanRet": 0.020,
        "shrunkMeanRet": 0.018,
        "p25Ret": -0.014,
        "p10Ret": -0.028,
        "maxAdverse": -0.048,
        "sideMeanRet": 0.014,
    }


def test_evaluate_candidates_long_gate(monkeypatch) -> None:
    monkeypatch.setattr(
        swing_plan_service.swing_expectancy_service,
        "resolve_setup_expectancy",
        _fake_expectancy,
    )
    out = swing_plan_service.evaluate_swing_candidates(
        as_of_ymd=20260304,
        p_up=0.62,
        p_down=0.38,
        p_turn_up=0.60,
        p_turn_down=0.40,
        ev20_net=0.012,
        long_setup_type="breakout",
        short_setup_type="watch",
        playbook_bonus_long=0.01,
        playbook_bonus_short=0.0,
        short_score=70.0,
        atr_pct=0.018,
        liquidity20d=120_000_000.0,
    )
    assert out["selectedSide"] == "long"
    assert out["long"]["qualified"] is True
    assert float(out["long"]["score"]) >= 0.62
    assert out["short"]["qualified"] is False


def test_build_swing_plan_short(monkeypatch) -> None:
    monkeypatch.setattr(
        swing_plan_service.swing_expectancy_service,
        "resolve_setup_expectancy",
        _fake_expectancy,
    )
    out = swing_plan_service.build_swing_plan(
        code="1301",
        as_of_ymd=20260304,
        close=100.0,
        p_up=0.35,
        p_down=0.65,
        p_turn_up=0.30,
        p_turn_down=0.62,
        ev20_net=-0.012,
        long_setup_type="watch",
        short_setup_type="breakdown",
        playbook_bonus_long=0.0,
        playbook_bonus_short=0.01,
        short_score=85.0,
        atr_pct=0.02,
        liquidity20d=130_000_000.0,
        decision_tone="down",
        hold_days_long=18,
        hold_days_short=22,
    )
    plan = out.get("plan")
    assert isinstance(plan, dict)
    assert plan["side"] == "short"
    assert float(plan["stop"]) > float(plan["entry"])
    assert float(plan["tp1"]) < float(plan["entry"])
    assert float(plan["tp2"]) < float(plan["tp1"])
    assert 10 <= int(plan["timeStopDays"]) <= 25
    diagnostics = out.get("diagnostics") or {}
    assert diagnostics.get("edge") is not None
    assert diagnostics.get("risk") is not None

