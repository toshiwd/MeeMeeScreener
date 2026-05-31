from __future__ import annotations

import pandas as pd

from scripts import tradex_pattern_family_source_rows_v1 as mod


def _daily_rows() -> pd.DataFrame:
    rows = []
    dates = pd.bdate_range("2023-01-02", periods=520)
    for i, dt in enumerate(dates):
        close = 100 + i * 0.2
        rows.append(
            {
                "code": "1001",
                "as_of_date": int(dt.strftime("%Y%m%d")),
                "open": close - 0.5,
                "high": close + 1.0,
                "low": close - 1.0,
                "close": close,
                "volume": 1000 + i,
                "ma7": close - 1.0,
                "ma20": close - 2.0,
                "ma60": close - 3.0,
                "bar_source": "pan",
            }
        )
    return pd.DataFrame(rows)


def test_add_daily_features_uses_past_windows_and_future_outcomes_separately() -> None:
    featured = mod.add_daily_features(_daily_rows())
    assert "close_vs_ma20_pct" in featured
    assert "ret20" in featured
    assert pd.isna(featured.loc[0, "ma60_slope_20d"])
    assert featured.loc[30, "ret20"] > 0


def test_apply_exclusions_requires_lookback_and_outcome_horizon() -> None:
    featured = mod.attach_period_features(mod.add_daily_features(_daily_rows()))
    eligible = mod.apply_exclusions(featured)
    assert len(eligible) < len(featured)
    assert eligible["ret20"].notna().all()
    assert eligible["monthly_close_vs_ma20_pct"].notna().all()


def test_family_flags_are_boolean_and_non_outcome_based() -> None:
    featured = mod.attach_period_features(mod.add_daily_features(_daily_rows()))
    eligible = mod.add_family_flags(mod.apply_exclusions(featured))
    for col in mod.FAMILY_FLAG_COLUMNS:
        assert col in eligible
        assert eligible[col].dtype == bool


def test_feature_contract_classifies_offline_outcomes() -> None:
    rows = pd.DataFrame({"as_of_date": [20250101], "code": ["1001"], "ret20": [0.1], "close_vs_ma20_pct": [0.02], "source_lineage": ["x"]})
    contract = mod.feature_contract(rows)
    assert contract["fields"]["as_of_date"]["classification"] == "identifier"
    assert contract["fields"]["ret20"]["classification"] == "offline_outcome_only"
    assert contract["fields"]["close_vs_ma20_pct"]["classification"] == "point_in_time_feature"
    assert contract["fields"]["source_lineage"]["classification"] == "source_metadata"
    assert contract["fields"]["ret20_derived_tags"]["classification"] == "forbidden_future_leak"


def test_decide_ready_when_core_features_and_families_exist() -> None:
    rows = pd.DataFrame({flag: [flag == mod.FAMILY_FLAG_COLUMNS[0]] for flag in mod.FAMILY_FLAG_COLUMNS})
    coverage = {"core_feature_non_null_rate": {"a": 1.0, "b": 0.95}}
    decision, reasons = mod.decide(rows, coverage, {})
    assert decision == "pattern_family_source_rows_ready_for_candidate_evaluation"
    assert reasons
