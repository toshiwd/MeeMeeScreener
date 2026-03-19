from __future__ import annotations

from external_analysis.runtime.decision_parts import (
    build_candidate_comparison_payloads,
    normalize_candidate_comparison_payloads,
    normalize_override_state_payload,
    normalize_publish_readiness_payload,
)


def test_candidate_comparison_payloads_drop_unknown_fields() -> None:
    payloads = build_candidate_comparison_payloads(
        (
            {"key": "up", "label": "buy", "tone": "up", "score": 0.81, "debug_note": "drop-me"},
            {"key": "down", "label": "sell", "tone": "down", "score": 0.07},
        ),
        comparison_scope="decision_scenarios",
        baseline_key="up",
    )

    assert len(payloads) == 2
    assert payloads[0]["candidate_key"] == "up"
    assert payloads[0]["comparison_scope"] == "decision_scenarios"
    assert "debug_note" not in payloads[0]


def test_candidate_comparison_payloads_normalize_unknown_fields_from_result_payload() -> None:
    payloads = normalize_candidate_comparison_payloads(
        (
            {
                "candidate_key": "logic_a:v2",
                "baseline_key": "logic_a:v1",
                "comparison_scope": "external_result",
                "score": 0.74,
                "score_delta": 0.03,
                "rank": 1,
                "reasons": ["validation_ok"],
                "publish_ready": True,
                "debug_note": "drop-me",
            },
        )
    )

    assert len(payloads) == 1
    assert payloads[0]["candidate_key"] == "logic_a:v2"
    assert payloads[0]["comparison_scope"] == "external_result"
    assert "debug_note" not in payloads[0]


def test_publish_readiness_and_override_state_normalization_are_stable() -> None:
    readiness = normalize_publish_readiness_payload(
        {
            "ready": True,
            "status": "approved",
            "reasons": ["validation_ok"],
            "candidate_key": "logic_family_a:v2",
            "approved": True,
            "debug_note": "drop-me",
        }
    )
    override_state = normalize_override_state_payload(
        {
            "present": True,
            "source": "operator_override",
            "logic_key": "logic_family_a:v4",
            "logic_version": "v4",
            "reason": "operator pin",
            "debug_note": "drop-me",
        }
    )

    assert readiness == {
        "ready": True,
        "status": "approved",
        "reasons": ["validation_ok"],
        "candidate_key": "logic_family_a:v2",
        "approved": True,
    }
    assert override_state == {
        "present": True,
        "source": "operator_override",
        "logic_key": "logic_family_a:v4",
        "logic_version": "v4",
        "reason": "operator pin",
    }
