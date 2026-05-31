from __future__ import annotations

import pandas as pd

from scripts import tradex_starter_candidate_chart_review_pack_v1 as mod


def test_daily_context_detects_ma_and_candle_state() -> None:
    dates = pd.date_range("2026-01-01", periods=80, freq="B")
    rows = []
    for i, dt in enumerate(dates):
        price = 100 + i
        rows.append({"code": "A", "date": int(dt.timestamp()), "ymd": int(dt.strftime("%Y%m%d")), "dt": dt, "o": price - 1, "h": price + 1, "l": price - 2, "c": price, "v": 1000 + i, "source": "pan"})
    bars = pd.DataFrame(rows)

    out = mod.daily_context_for("A", bars, int(dates[-1].strftime("%Y%m%d")))

    assert out["close_above_ma7"] is True
    assert out["close_above_ma20"] is True
    assert out["data_freshness_status"] == "fresh"


def test_judge_pullback_starter_ready_when_reclaim_confirmed() -> None:
    row = pd.Series({"research_candidate_source_family": "pullback_reclaim_source"})
    daily = {
        "close": 105.0,
        "ma7": 102.0,
        "ma20": 100.0,
        "ma20_slope": 0.001,
        "dist_ma20_pct": 0.05,
        "dist_ma60_pct": 0.08,
        "failed_high": False,
        "large_bearish_candle": False,
        "upper_wick_ratio": 0.1,
        "volume_ma20_ratio": 1.0,
        "data_freshness_status": "fresh",
    }
    weekly = {"trend_direction": "up"}
    monthly = {"trend_direction": "up"}

    judgment = mod.judge_candidate(row, daily, weekly, monthly)

    assert judgment["manual_judgment"] == "starter_ready"
    assert "above MA7/MA20" in judgment["reason_summary"]


def test_judge_overextension_avoid_when_too_extended() -> None:
    row = pd.Series({"research_candidate_source_family": "overextension_risk_source"})
    daily = {
        "close": 150.0,
        "ma7": 140.0,
        "ma20": 100.0,
        "ma20_slope": 0.01,
        "dist_ma20_pct": 0.30,
        "dist_ma60_pct": 0.45,
        "failed_high": False,
        "large_bearish_candle": False,
        "upper_wick_ratio": 0.2,
        "volume_ma20_ratio": 1.0,
        "data_freshness_status": "fresh",
    }

    judgment = mod.judge_candidate(row, daily, {"trend_direction": "up"}, {"trend_direction": "up"})

    assert judgment["manual_judgment"] == "avoid"
    assert "overextension risk" in judgment["reason_summary"]
