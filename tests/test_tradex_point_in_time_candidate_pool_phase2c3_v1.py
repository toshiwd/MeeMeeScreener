from __future__ import annotations

import pandas as pd

from scripts.tradex_side_aware_min_pool_feasibility_v1 import (
    _attach_context_lineage_fields,
    _attach_full_context_lineage,
    _normalize_date_key,
)


def test_full_context_lineage_join_fills_missing_dates_without_changing_score_fields() -> None:
    rows = pd.DataFrame(
        [
            {
                "anchor_date": "2024-03-05",
                "symbol": "1234",
                "champion_score": 0.42,
                "champion_rank": 3,
            }
        ]
    )
    context = pd.DataFrame(
        [
            {
                "__context_key__": "2024-03-05|1234",
                "monthly_context_date": "2024-02-29",
                "monthly_context_source": "confirmed_monthly_bars_monthly_ma",
                "monthly_context_no_lookahead": True,
                "weekly_context_date": "2024-03-01",
                "weekly_context_source": "provisional_weekly_from_daily_bars_daily_ma",
                "weekly_context_no_lookahead": True,
            }
        ]
    )

    enriched = _attach_full_context_lineage(rows, context)

    assert enriched.loc[0, "champion_score"] == 0.42
    assert enriched.loc[0, "champion_rank"] == 3
    assert pd.Timestamp(enriched.loc[0, "monthly_context_date"]) == pd.Timestamp("2024-02-29")
    assert pd.Timestamp(enriched.loc[0, "weekly_context_date"]) == pd.Timestamp("2024-03-01")
    assert bool(enriched.loc[0, "context_shape_full_lineage_joined"]) is True


def test_normalize_date_key_handles_integer_yyyymmdd() -> None:
    assert _normalize_date_key(20240305) == "2024-03-05"


def test_context_lineage_fields_validate_completed_weekly_monthly_sources() -> None:
    rows = pd.DataFrame(
        [
            {
                "as_of_date": "2024-03-05",
                "candidate_date": "2024-03-05",
                "feature_cutoff_date": "2024-03-05",
                "monthly_context_date": "2024-02-29",
                "monthly_context_source": "confirmed_monthly_bars_monthly_ma",
                "monthly_context_no_lookahead": True,
                "weekly_context_date": "2024-03-01",
                "weekly_context_source": "provisional_weekly_from_daily_bars_daily_ma",
                "weekly_context_no_lookahead": True,
            }
        ]
    )

    enriched = _attach_context_lineage_fields(rows)

    assert bool(enriched.loc[0, "daily_no_lookahead_valid"]) is True
    assert bool(enriched.loc[0, "weekly_no_lookahead_valid"]) is True
    assert bool(enriched.loc[0, "monthly_no_lookahead_valid"]) is True
    assert bool(enriched.loc[0, "context_no_lookahead_valid"]) is True
    assert enriched.loc[0, "context_no_lookahead_status"] == "valid"


def test_full_context_lineage_uses_nearest_prior_without_future_row() -> None:
    rows = pd.DataFrame(
        [
            {
                "anchor_date": "2024-03-05",
                "symbol": "1234",
                "champion_score": 0.42,
                "champion_rank": 3,
            }
        ]
    )
    context = pd.DataFrame(
        [
            {
                "__context_key__": "2024-03-01|1234",
                "symbol": "1234",
                "context_trade_date": pd.Timestamp("2024-03-01"),
                "monthly_context_date": "2024-02-29",
                "monthly_context_source": "confirmed_monthly_bars_monthly_ma",
                "monthly_context_no_lookahead": True,
                "weekly_context_date": "2024-03-01",
                "weekly_context_source": "provisional_weekly_from_daily_bars_daily_ma",
                "weekly_context_no_lookahead": True,
            },
            {
                "__context_key__": "2024-03-06|1234",
                "symbol": "1234",
                "context_trade_date": pd.Timestamp("2024-03-06"),
                "monthly_context_date": "2024-03-31",
                "monthly_context_source": "future_monthly",
                "monthly_context_no_lookahead": True,
                "weekly_context_date": "2024-03-08",
                "weekly_context_source": "future_weekly",
                "weekly_context_no_lookahead": True,
            },
        ]
    )

    enriched = _attach_full_context_lineage(rows, context)

    assert pd.Timestamp(enriched.loc[0, "monthly_context_date"]) == pd.Timestamp("2024-02-29")
    assert pd.Timestamp(enriched.loc[0, "weekly_context_date"]) == pd.Timestamp("2024-03-01")
    assert enriched.loc[0, "context_lineage_match_policy"] == "nearest_prior_or_same_symbol_trade_date"
