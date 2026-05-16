from __future__ import annotations

import pandas as pd

from scripts.tradex_point_in_time_candidate_pool_phase2c2_v1 import enrich_context_lineage


def test_context_lineage_marks_missing_weekly_monthly_unverifiable() -> None:
    rows = pd.DataFrame(
        [
            {
                "symbol": "1234",
                "candidate_date": "2024-03-05",
                "as_of_date": "2024-03-05",
                "feature_cutoff_date": "2024-03-05",
            }
        ]
    )

    enriched = enrich_context_lineage(rows)

    assert bool(enriched.loc[0, "daily_no_lookahead_valid"]) is True
    assert bool(enriched.loc[0, "weekly_no_lookahead_valid"]) is False
    assert bool(enriched.loc[0, "monthly_no_lookahead_valid"]) is False
    assert bool(enriched.loc[0, "context_no_lookahead_valid"]) is False
    assert enriched.loc[0, "context_no_lookahead_status"] == "unverifiable"
    assert "missing_weekly_context_source_date" in enriched.loc[0, "context_no_lookahead_failure_reason"]
    assert "missing_monthly_context_source_date" in enriched.loc[0, "context_no_lookahead_failure_reason"]


def test_context_lineage_prefers_accumulated_context_when_available() -> None:
    rows = pd.DataFrame(
        [
            {
                "symbol": "1234",
                "candidate_date": "2024-03-05",
                "as_of_date": "2024-03-05",
                "feature_cutoff_date": "2024-03-05",
                "weekly_context_no_lookahead": True,
                "weekly_context_date": "2024-02-23",
                "weekly_context_source": "broad_weekly",
                "monthly_context_no_lookahead": True,
                "monthly_context_date": "2024-01-31",
                "monthly_context_source": "broad_monthly",
                "weekly_context_no_lookahead_acc": True,
                "weekly_context_date_acc": "2024-03-01",
                "weekly_context_source_acc": "acc_weekly",
                "monthly_context_no_lookahead_acc": True,
                "monthly_context_date_acc": "2024-02-29",
                "monthly_context_source_acc": "acc_monthly",
            }
        ]
    )

    enriched = enrich_context_lineage(rows)

    assert bool(enriched.loc[0, "context_no_lookahead_valid"]) is True
    assert enriched.loc[0, "context_no_lookahead_status"] == "valid"
    assert pd.Timestamp(enriched.loc[0, "weekly_context_source_date"]) == pd.Timestamp("2024-03-01")
    assert pd.Timestamp(enriched.loc[0, "monthly_context_source_date"]) == pd.Timestamp("2024-02-29")
    assert "weekly=acc_weekly" in enriched.loc[0, "context_lineage_source"]
    assert "monthly=acc_monthly" in enriched.loc[0, "context_lineage_source"]
