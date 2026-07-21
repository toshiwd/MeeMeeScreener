from __future__ import annotations

import pytest

from scripts.tradex_sideways_blind_review_server_v1 import validate_payload


def annotation(case_id: str) -> dict[str, str]:
    return {
        "case_id": case_id,
        "sideways_decision": "SIDEWAYS",
        "confidence": "HIGH",
    }


def test_validate_payload_accepts_complete_blind_annotations() -> None:
    payload = {
        "schema_version": "tradex_sideways_blind_annotation_v1",
        "annotations": [annotation("SW001"), annotation("SW002")],
    }
    assert validate_payload(payload, expected_count=2) is payload


def test_validate_payload_rejects_missing_or_invalid_answers() -> None:
    payload = {
        "schema_version": "tradex_sideways_blind_annotation_v1",
        "annotations": [annotation("SW001"), annotation("SW002")],
    }
    payload["annotations"][1]["confidence"] = ""
    with pytest.raises(ValueError, match="invalid confidence"):
        validate_payload(payload, expected_count=2)
