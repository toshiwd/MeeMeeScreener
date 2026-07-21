from __future__ import annotations

import pandas as pd
import pytest

from scripts.tradex_sideways_blind_freeze_v1 import validate_annotations


def test_validate_annotations_accepts_complete_three_way_contract() -> None:
    board = pd.DataFrame({"case_id": ["SW001"], "code": ["1000"], "ymd": [20250101]})
    payload = {"annotations": [{"case_id": "SW001", "code": "1000", "ymd": 20250101, "sideways_decision": "BORDERLINE", "confidence": "LOW", "reviewer_note": ""}]}
    result = validate_annotations(board, payload)
    assert result.iloc[0].sideways_decision == "BORDERLINE"


def test_validate_annotations_rejects_missing_decision() -> None:
    board = pd.DataFrame({"case_id": ["SW001"], "code": ["1000"], "ymd": [20250101]})
    payload = {"annotations": [{"case_id": "SW001", "code": "1000", "ymd": 20250101, "sideways_decision": "", "confidence": "HIGH"}]}
    with pytest.raises(RuntimeError, match="invalid sideways"):
        validate_annotations(board, payload)
