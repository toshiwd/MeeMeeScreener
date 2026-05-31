from __future__ import annotations

import pandas as pd

from scripts import tradex_starter_entry_role_backfill_v1 as mod


def test_coverage_reports_year_path_counts() -> None:
    rows = pd.DataFrame(
        [
            {"year": 2019, "decision_date": 20190101, "ret20": 0.1, "mae20": -0.01, "path20_available": True, "ret5": 0.01, "mae5": -0.01, "ma7_slope": 0.1},
            {"year": 2019, "decision_date": 20191231, "ret20": None, "mae20": None, "path20_available": False, "ret5": None, "mae5": None, "ma7_slope": None},
        ]
    )
    year_cov, path_cov = mod.coverage(rows)
    assert year_cov.iloc[0]["total_candidate_rows"] == 2
    assert year_cov.iloc[0]["rows_with_ret20"] == 1
    assert path_cov.iloc[0]["missing_path20_rows"] == 1


def test_snapshot_paths_include_2019_and_2026() -> None:
    paths = mod.snapshot_paths()
    assert 2019 in paths
    assert 2026 in paths
