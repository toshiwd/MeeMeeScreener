from __future__ import annotations

import json

import pandas as pd

from scripts import tradex_position_lifecycle_multiyear_momentum_regime_audit_v1 as mod


def test_multiyear_audit_keeps_states_fixed_and_adds_point_in_time_regime(tmp_path):
    rows = []
    for year in [2021, 2026]:
        for index in range(10):
            starter = index < 7
            rows.append({
                "as_of_date": year * 10000 + 101,
                "code": f"{year}-{index}",
                "close_above_ma20": True,
                "close_above_ma60": True,
                "monthly_weekly_supportive_daily_confirmation_candidate": starter,
                "early_trend_reclaim_controlled_extension_candidate": False,
                "volatility_compression_breakout_preparation_candidate": False,
                "constructive_pullback_support_bullish_confirmation_reference_match": False,
                "close_vs_ma20_pct": 0.01,
                "failed_high_flag": False,
                "weekly_supportive_flag": True,
                "ret5": 0.01,
                "ret20": 0.02,
                "winner_ret20_gt_10pct": False,
                "bad_ret20_lt_minus_5pct": False,
                "severe_ret20_lt_minus_10pct": False,
            })
    source = tmp_path / "source.parquet"
    pd.DataFrame(rows).to_parquet(source, index=False)
    output = mod.run(source_path=source, output_root=tmp_path / "out")
    audit = json.loads((output / "position_lifecycle_multiyear_regime_audit.json").read_text())
    decision = json.loads((output / "research_decision.json").read_text())

    assert audit["contract"]["state_thresholds_fixed"] is True
    assert audit["contract"]["market_regime_point_in_time_only"] is True
    assert audit["daily_regime_distribution"]["broad_momentum"] == 2
    assert decision["state_thresholds_changed"] is False
    assert decision["runtime_db_write"] is False
