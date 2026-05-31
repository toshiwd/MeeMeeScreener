from __future__ import annotations

import pandas as pd

from scripts import tradex_family_definition_v2_source_rows as mod


def _rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "as_of_date": 20250101,
                "code": "1001",
                "source_db_path": "db",
                "close_vs_ma7_pct": 0.01,
                "close_vs_ma20_pct": 0.02,
                "close_vs_ma60_pct": 0.08,
                "ma7_slope_5d": 0.01,
                "ma20_slope_10d": 0.01,
                "ma60_slope_20d": 0.01,
                "close_above_ma7": True,
                "close_above_ma20": True,
                "close_above_ma60": True,
                "ma7_above_ma20": True,
                "ma20_above_ma60": True,
                "body_ratio": 0.5,
                "upper_wick_ratio": 0.1,
                "lower_wick_ratio": 0.3,
                "bullish_body_flag": True,
                "bearish_body_flag": False,
                "failed_high_flag": False,
                "recent_high_distance_pct": -0.04,
                "recent_low_distance_pct": 0.2,
                "gap_up_flag": False,
                "gap_down_flag": False,
                "volume_vs_20d_avg": 1.2,
                "atr14_pct": 0.025,
                "realized_vol20": 0.018,
                "weekly_close_vs_ma7_pct": 0.01,
                "weekly_close_vs_ma20_pct": 0.02,
                "weekly_ma7_slope": 0.01,
                "weekly_ma20_slope": 0.01,
                "weekly_supportive_flag": True,
                "weekly_failed_high_flag": False,
                "monthly_close_vs_ma7_pct": 0.01,
                "monthly_close_vs_ma20_pct": 0.02,
                "monthly_ma7_slope": 0.01,
                "monthly_ma20_slope": 0.01,
                "monthly_supportive_flag": True,
                "monthly_box_position": 0.5,
                "monthly_box_width_pct": 0.2,
                "monthly_box_month_count": 6,
                "ret5": 0.01,
                "ret20": 0.12,
                "winner_ret20_gt_10pct": True,
                "bad_ret20_lt_minus_5pct": False,
                "severe_ret20_lt_minus_10pct": False,
            }
        ]
    )


def test_add_v2_flags_generates_boolean_flags() -> None:
    rows = mod.add_v2_flags(_rows())
    for flag in mod.V2_FLAGS:
        assert flag in rows
        assert rows[flag].dtype == bool
    assert rows["high_upside_contained_reserve_family_v2"].iloc[0]


def test_required_missing_detects_missing_columns() -> None:
    missing = mod.required_missing(pd.DataFrame({"as_of_date": [20250101]}))
    assert "ret20" in missing
    assert "close_vs_ma20_pct" in missing


def test_feature_contract_separates_outcomes() -> None:
    rows = mod.add_v2_flags(_rows()).rename(columns={"source_db_path": "source_db"})
    rows["source_bar_status"] = "confirmed"
    rows["source_lineage"] = "x"
    contract = mod.feature_contract(rows)
    assert contract["fields"]["as_of_date"]["classification"] == "identifier"
    assert contract["fields"]["ret20"]["classification"] == "offline_outcome_only"
    assert contract["fields"]["high_upside_contained_reserve_family_v2"]["classification"] == "point_in_time_feature"
    assert contract["fields"]["source_db"]["classification"] == "source_metadata"


def test_flag_counts_and_overlap() -> None:
    rows = mod.add_v2_flags(_rows()).rename(columns={"source_db_path": "source_db"})
    rows["source_bar_status"] = "confirmed"
    rows["source_lineage"] = "x"
    counts = mod.flag_counts(rows)
    overlap = mod.overlap_matrix(rows)
    assert counts["rows_per_family"]["high_upside_contained_reserve_family_v2"] == 1
    assert overlap["high_upside_contained_reserve_family_v2"]["high_upside_contained_reserve_family_v2"]["overlap_count"] == 1


def test_decide_ready_when_rows_and_flags_exist() -> None:
    rows = mod.add_v2_flags(_rows())
    decision, reasons = mod.decide(rows, [], True)
    assert decision == "family_v2_source_rows_ready_for_evaluation"
    assert reasons
