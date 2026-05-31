from __future__ import annotations

import pandas as pd

from scripts import tradex_daily_feature_freshness_contract_v1 as mod


def test_build_surface_rows_records_source_contract_fields() -> None:
    rows = pd.DataFrame(
        [
            {
                "decision_date": 20260508,
                "code": "A",
                "baseline_score": 10,
                "dist_ma20_top_quartile": False,
                "dist_ma60_top_quartile": False,
                "ma7_slope_top_quartile": False,
                "upper_wick_ratio": 0.1,
                "failed_high_update": False,
                "large_bearish_candle": False,
                "dist_ma7_pct": 0.01,
                "dist_ma20_pct": 0.02,
                "above20_streak": 10,
                "above60_streak": 5,
                "days_since_ma20_reclaim": 3,
                "days_since_ma60_reclaim": 50,
                "ma20_slope": 0.01,
                "ma60_slope": 0.0,
                "monthly_high_zone_proxy": False,
                "monthly_box_breakout_proxy": False,
                "monthly_box_inside_proxy": False,
                "daily_bar_source": "confirmed",
                "daily_bar_max_date": 20260508,
                "feature_source_max_date": 20260508,
                "feature_freshness_status": "fresh",
                "provisional_used": False,
            }
        ]
    )

    out = mod.build_surface_rows(rows)

    assert out.loc[0, "research_candidate_source_family"] == "pullback_reclaim_source"
    assert out.loc[0, "daily_bar_source"] == "confirmed"
    assert bool(out.loc[0, "current_review_no_lookahead_mode"]) is True


def test_select_daily_source_blocks_without_covering_runtime(monkeypatch) -> None:
    monkeypatch.setattr(
        mod,
        "_runtime_db_candidates",
        lambda: [mod.Path("missing.duckdb")],
    )

    result = mod.select_daily_source(20260508)

    assert result["decision"] == "blocked"
    assert result["selected"] is None
