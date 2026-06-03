from __future__ import annotations

import json

import pandas as pd

from scripts.tradex_pullback_retest_confirmation_source_pretest_v1 import run_pretest


def test_pretest_compares_events_with_same_day_source_rows_without_mutation(tmp_path):
    enriched_root = tmp_path / "enriched"
    enriched_root.mkdir()
    pd.DataFrame(
        [
            {"code": "1001", "confirmation_as_of": 20250103, "fixed_condition_pretest_eligible": True},
            {"code": "1002", "confirmation_as_of": 20250103, "fixed_condition_pretest_eligible": False},
        ]
    ).to_parquet(enriched_root / "pullback_retest_sequence_events_enriched.parquet", index=False)
    source_parquet = tmp_path / "source.parquet"
    pd.DataFrame(
        [
            {
                "as_of_date": 20250103,
                "code": "1001",
                "ret20": 0.2,
                "winner_ret20_gt_10pct": True,
                "bad_ret20_lt_minus_5pct": False,
                "severe_ret20_lt_minus_10pct": False,
            },
            {
                "as_of_date": 20250103,
                "code": "1002",
                "ret20": -0.1,
                "winner_ret20_gt_10pct": False,
                "bad_ret20_lt_minus_5pct": True,
                "severe_ret20_lt_minus_10pct": False,
            },
        ]
    ).to_parquet(source_parquet, index=False)

    output_root = tmp_path / "output"
    run_root = run_pretest(enriched_root=enriched_root, source_parquet=source_parquet, output_root=output_root)
    compare = json.loads((run_root / "compare.json").read_text())
    decision = json.loads((run_root / "research_decision.json").read_text())

    assert compare["event"]["row_count"] == 1
    assert compare["same_day_baseline"]["row_count"] == 2
    assert compare["event"]["ret20_mean_delta_vs_same_day_baseline"] == 0.15000000000000002
    assert decision["decision_class"] == "DROP"
    assert decision["candidate_generation_changed"] is False
    assert decision["runtime_db_write"] is False
