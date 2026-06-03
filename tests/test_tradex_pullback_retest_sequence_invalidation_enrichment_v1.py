from __future__ import annotations

import json

import duckdb
import pandas as pd

from scripts.tradex_pullback_retest_sequence_invalidation_enrichment_v1 import enrich


def test_enrich_materializes_absolute_invalidation_from_read_only_daily_bars(tmp_path):
    sequence_root = tmp_path / "sequence"
    sequence_root.mkdir()
    pd.DataFrame(
        [
            {
                "code": "1001",
                "candidate_as_of": 20250103,
                "pullback_start_as_of": 20250101,
                "pullback_low_as_of": 20250101,
                "reclaim_as_of": 20250102,
                "retest_as_of": 20250103,
                "confirmation_as_of": 20250103,
                "confirmation_reason": "retest_bullish_body",
                "invalidation_distance_pct_as_of_confirmation": 0.1,
                "invalidation_price_as_of_confirmation": None,
            }
        ]
    ).to_parquet(sequence_root / "pullback_retest_sequence_events.parquet", index=False)

    db_path = tmp_path / "stocks.duckdb"
    connection = duckdb.connect(str(db_path))
    connection.execute(
        """
        CREATE TABLE daily_bars (
            code VARCHAR,
            date INTEGER,
            o DOUBLE,
            h DOUBLE,
            l DOUBLE,
            c DOUBLE,
            v BIGINT,
            source VARCHAR
        )
        """
    )
    connection.execute(
        """
        INSERT INTO daily_bars VALUES
            ('1001', 20250101, 95, 96, 90, 94, 1000, 'pan'),
            ('1001', 20250102, 96, 99, 93, 98, 1100, 'pan'),
            ('1001', 20250103, 99, 102, 97, 100, 1200, 'pan')
        """
    )
    connection.close()

    output_root = tmp_path / "output"
    run_root = enrich(sequence_root=sequence_root, db_path=db_path, output_root=output_root)
    audit = json.loads((run_root / "invalidation_enrichment_audit.json").read_text())
    decision = json.loads((run_root / "research_decision.json").read_text())
    events = pd.read_parquet(run_root / "pullback_retest_sequence_events_enriched.parquet")

    assert audit["event_count"] == 1
    assert audit["absolute_invalidation_price_materialized"] is True
    assert audit["confirmation_close_materialized"] is True
    assert audit["fixed_condition_pretest_eligible_count"] == 1
    assert audit["missing_confirmation_close_handling"] == "explicit_exclusion_no_fallback"
    assert decision["decision_class"] == "READY"
    assert events.loc[0, "invalidation_price_as_of_confirmation"] == 90
    assert events.loc[0, "confirmation_close"] == 100
    assert events.loc[0, "invalidation_source_bar_count"] == 3
    assert bool(events.loc[0, "fixed_condition_pretest_eligible"]) is True
