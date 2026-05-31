from __future__ import annotations

import pandas as pd

from scripts import tradex_asof_positive_selection_score_v1 as mod


def _rows() -> pd.DataFrame:
    rows = []
    for date in [20250101, 20250102]:
        for i in range(1, 6):
            rows.append(
                {
                    "as_of_date": date,
                    "code": f"10{i}",
                    "source_db": "dummy.duckdb",
                    "source_bar_status": "confirmed",
                    "source_lineage": "test",
                    "close_vs_ma7_pct": i / 100,
                    "close_vs_ma20_pct": i / 100,
                    "close_vs_ma60_pct": i / 100,
                    "ma7_slope_5d": i / 100,
                    "ma20_slope_10d": i / 100,
                    "ma60_slope_20d": i / 100,
                    "close_above_ma7": i >= 3,
                    "close_above_ma20": i >= 3,
                    "close_above_ma60": i >= 4,
                    "ma7_above_ma20": i >= 3,
                    "ma20_above_ma60": i >= 4,
                    "body_ratio": i / 10,
                    "upper_wick_ratio": (6 - i) / 10,
                    "lower_wick_ratio": i / 10,
                    "bullish_body_flag": i >= 4,
                    "bearish_body_flag": i == 1,
                    "failed_high_flag": i == 1,
                    "recent_high_distance_pct": -i / 100,
                    "recent_low_distance_pct": i / 100,
                    "gap_up_flag": False,
                    "gap_down_flag": i == 1,
                    "volume_vs_20d_avg": 1 + i / 10,
                    "atr14_pct": (6 - i) / 100,
                    "realized_vol20": (6 - i) / 100,
                    "weekly_close_vs_ma7_pct": i / 100,
                    "weekly_close_vs_ma20_pct": i / 100,
                    "weekly_ma7_slope": i / 100,
                    "weekly_ma20_slope": i / 100,
                    "weekly_supportive_flag": i >= 3,
                    "weekly_failed_high_flag": i == 1,
                    "monthly_close_vs_ma7_pct": i / 100,
                    "monthly_close_vs_ma20_pct": i / 100,
                    "monthly_ma7_slope": i / 100,
                    "monthly_ma20_slope": i / 100,
                    "monthly_supportive_flag": i >= 3,
                    "monthly_box_position": i / 10,
                    "monthly_box_width_pct": 0.2,
                    "monthly_box_month_count": 6,
                    "high_upside_contained_reserve_family_v2": False,
                    "constructive_pullback_confirmation_family_v2": i == 4,
                    "volatility_compression_pre_breakout_family_v2": i == 5,
                    "ret5": 0.01,
                    "ret20": 0.02 * i,
                    "winner_ret20_gt_10pct": False,
                    "bad_ret20_lt_minus_5pct": False,
                    "severe_ret20_lt_minus_10pct": False,
                }
            )
    return pd.DataFrame(rows)


def test_forbidden_live_feature_violations_detects_future_terms() -> None:
    assert mod.forbidden_live_feature_violations(["close_vs_ma20_pct", "ret20"]) == ["ret20"]


def test_score_bucket_thresholds() -> None:
    assert mod.score_bucket(0.995) == "top_1pct"
    assert mod.score_bucket(0.975) == "top_3pct"
    assert mod.score_bucket(0.955) == "top_5pct"
    assert mod.score_bucket(0.91) == "top_10pct"
    assert mod.score_bucket(0.81) == "top_20pct"
    assert mod.score_bucket(0.5) == "remaining"


def test_build_score_ranks_best_context_first_by_date() -> None:
    rows = _rows()
    live, missing = mod.validate_live_features(rows)
    assert not missing
    scored = mod.build_score(rows, live)
    top = scored.sort_values(["as_of_date", "score_rank_by_date"]).groupby("as_of_date").head(1)
    assert set(top["code"]) == {"105"}
    assert scored["asof_positive_selection_score_v1"].notna().all()
    assert scored["live_feature_available_flag"].all()


def test_bucket_metrics_counts_rows() -> None:
    rows = _rows()
    scored = mod.build_score(rows, mod.validate_live_features(rows)[0])
    metrics = mod.bucket_metrics(scored)
    assert sum(bucket["sample_count"] for bucket in metrics.values()) == len(rows)
    assert metrics["top_20pct"]["sample_count"] > 0


def test_decide_blocks_on_missing_required_features() -> None:
    decision, cls, reasons = mod.decide(
        {"top_5pct": {}, "remaining": {}},
        {"missing_required_live_features": ["x"]},
        True,
    )
    assert decision == "blocked_missing_point_in_time_features"
    assert cls == "BLOCKED"
    assert reasons


def test_feature_contract_separates_offline_outcomes() -> None:
    contract = mod.feature_contract({"axis_id": "source"}, ["close_vs_ma20_pct"], [])
    assert contract["fields"]["ret20"]["classification"] == "offline_outcome_only"
    assert contract["fields"]["close_vs_ma20_pct"]["classification"] == "point_in_time_feature"
    assert contract["fields"]["ret20_derived_tags"]["classification"] == "forbidden_future_leak"
