from __future__ import annotations

import json

import pandas as pd

from scripts import tradex_position_lifecycle_state_machine_v1 as mod


def test_lifecycle_states_separate_entry_and_held_review(monkeypatch, tmp_path):
    surface = pd.DataFrame([
        {"code": "1001", "as_of_date": 20240101, "avoid_level": "none", "review_bucket": "Wait", "volatility_compression_breakout_preparation_candidate": True, "constructive_pullback_support_bullish_confirmation_reference_match": False, "early_trend_reclaim_controlled_extension_candidate": False, "monthly_weekly_supportive_daily_confirmation_candidate": False, "failed_high_flag": False, "close_vs_ma20_pct": 0.01, "downside_risk_probability_20d": 0.3, "weekly_supportive_flag": True, "ret5": 0.02, "ret20": 0.1, "bad_ret20_lt_minus_5pct": False, "severe_ret20_lt_minus_10pct": False, "winner_ret20_gt_10pct": False},
        {"code": "1001", "as_of_date": 20240102, "avoid_level": "avoid", "review_bucket": "Avoid", "volatility_compression_breakout_preparation_candidate": False, "constructive_pullback_support_bullish_confirmation_reference_match": False, "early_trend_reclaim_controlled_extension_candidate": False, "monthly_weekly_supportive_daily_confirmation_candidate": False, "failed_high_flag": True, "close_vs_ma20_pct": -0.08, "downside_risk_probability_20d": 0.8, "weekly_supportive_flag": False, "ret5": -0.1, "ret20": -0.2, "bad_ret20_lt_minus_5pct": True, "severe_ret20_lt_minus_10pct": True, "winner_ret20_gt_10pct": False},
    ])
    path = tmp_path / "surface.parquet"
    surface.to_parquet(path, index=False)
    bars = pd.DataFrame([
        {"code": "1001", "as_of_date": 20240101, "high": 100.0, "low": 95.0, "close": 98.0},
        {"code": "1001", "as_of_date": 20240102, "high": 99.0, "low": 90.0, "close": 91.0},
        {"code": "1001", "as_of_date": 20240103, "high": 95.0, "low": 88.0, "close": 90.0},
    ])
    monkeypatch.setattr(mod, "_bars", lambda _: bars)

    output = mod.run(surface_path=path, db_path=tmp_path / "unused.duckdb", output_root=tmp_path / "out")
    replay = pd.read_parquet(output / "position_lifecycle_replay.parquet")
    metrics = json.loads((output / "position_lifecycle_metrics.json").read_text())
    decision = json.loads((output / "research_decision.json").read_text())

    assert replay["entry_state"].tolist() == ["Accumulate", "Avoid"]
    assert replay["held_position_review_state"].tolist() == ["Hold", "ExitReview"]
    assert replay["held_transition"].tolist() == ["INITIAL->Hold", "Hold->ExitReview"]
    assert metrics["contract"]["automatic_trade_action"] is False
    assert decision["decision_class"] == "READY_REVIEW_ONLY"
    assert decision["runtime_db_write"] is False
