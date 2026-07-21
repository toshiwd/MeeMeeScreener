from __future__ import annotations

import pandas as pd

from scripts.tradex_sideways_blind_reveal_compare_v1 import compare


def test_compare_keeps_borderline_out_of_binary_metrics(tmp_path) -> None:
    human = pd.DataFrame([
        {"case_id": "SW001", "code": "1000", "ymd": 20200101, "sideways_decision": "SIDEWAYS", "confidence": "HIGH"},
        {"case_id": "SW002", "code": "1001", "ymd": 20200102, "sideways_decision": "NOT_SIDEWAYS", "confidence": "HIGH"},
        {"case_id": "SW003", "code": "1002", "ymd": 20200103, "sideways_decision": "BORDERLINE", "confidence": "LOW"},
    ])
    sealed = pd.DataFrame([
        {"case_id": "SW001", "code": "1000", "ymd": 20200101, "sideways_state": True, "sample_group": "DETECTOR_POSITIVE", "year": 2020, "outcome_joined": False},
        {"case_id": "SW002", "code": "1001", "ymd": 20200102, "sideways_state": False, "sample_group": "RANDOM_NEGATIVE", "year": 2020, "outcome_joined": False},
        {"case_id": "SW003", "code": "1002", "ymd": 20200103, "sideways_state": False, "sample_group": "NEAR_BOUNDARY_NEGATIVE", "year": 2020, "outcome_joined": False},
    ])
    human_path, sealed_path = tmp_path / "human.parquet", tmp_path / "sealed.parquet"
    human.to_parquet(human_path, index=False)
    sealed.to_parquet(sealed_path, index=False)
    result = compare(human_path, sealed_path, tmp_path / "output")
    assert result["metrics"]["decided_rows"] == 2
    assert result["metrics"]["borderline_rows"] == 1
    assert result["metrics"]["accuracy"] == 1.0
    assert result["fixed_conditions"]["outcomes_loaded"] is False
