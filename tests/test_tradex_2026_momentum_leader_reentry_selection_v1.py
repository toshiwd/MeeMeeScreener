from __future__ import annotations

import json

import pandas as pd

from scripts import tradex_2026_momentum_leader_reentry_selection_v1 as mod


def test_momentum_leader_reentry_uses_same_day_strength_without_theme_names(tmp_path):
    rows = []
    for year in [2025, 2026]:
        for index in range(100):
            leader = index >= 85
            reentry = leader and index < 95
            rows.append({
                "as_of_date": year * 10000 + 101,
                "code": f"{year}-{index}",
                "market_momentum_regime": "broad_momentum",
                "close_vs_ma20_pct": 0.04 if reentry else (0.15 if leader else 0.0),
                "close_vs_ma60_pct": 0.2 if leader else 0.0,
                "weekly_close_vs_ma20_pct": 0.2 if leader else 0.0,
                "monthly_close_vs_ma20_pct": 0.3 if leader else 0.0,
                "ma7_slope_5d": 0.02,
                "ma20_slope_10d": 0.02,
                "volume_vs_20d_avg": 1.0,
                "recent_high_distance_pct": -0.03,
                "bearish_body_flag": False,
                "failed_high_flag": False,
                "ret5": 0.02 if reentry else -0.01,
                "ret20": 0.12 if reentry else -0.08,
                "winner_ret20_gt_10pct": reentry,
                "bad_ret20_lt_minus_5pct": not reentry,
                "severe_ret20_lt_minus_10pct": False,
            })
    source = tmp_path / "source.parquet"
    pd.DataFrame(rows).to_parquet(source, index=False)
    output = mod.run(source_path=source, output_root=tmp_path / "out")
    compare = json.loads((output / "momentum_leader_reentry_compare.json").read_text())
    decision = json.loads((output / "research_decision.json").read_text())

    assert compare["contract"]["theme_name_used"] is False
    assert compare["contract"]["point_in_time_features_only"] is True
    assert decision["runtime_db_write"] is False
    assert decision["automatic_trade_action"] is False
