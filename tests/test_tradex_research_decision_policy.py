from __future__ import annotations

from pathlib import Path

from app.backend.services import tradex_research_decision_policy as decision_policy


def _base_judge_input(*, sample_count: int = 12, branching: bool = True) -> dict[str, object]:
    return {
        "schema_version": "tradex_research_os_judge_input_v1",
        "experiment_id": "exp_test",
        "comparison_scope": {"family_id": "family-a", "target_method_family": "regime-aware"},
        "changed_top5_members_count": 2 if branching else 0,
        "changed_top10_members_count": 3 if branching else 0,
        "changed_rank_count": 4 if branching else 0,
        "top5_boundary_score_gap": 0.12 if branching else 0.0,
        "top10_boundary_score_gap": 0.08 if branching else 0.0,
        "selection_divergence_reason": "top5_member_replacement" if branching else "no_meaningful_branching",
        "available_sample_count": sample_count,
        "available_session_count": 1,
        "summary_metrics": {
            "family_decision": "keep",
            "authoritative_rollup_decision": "keep",
            "candidate_local_decision": "keep",
            "session_aggregate_decision": "keep",
            "promote_ready": True,
            "insufficient_samples": False,
            "topk_branching_block_reason": "",
            "meaningful_topk_branching_possible": branching,
        },
    }


def _base_context(*, family_decision: str = "keep", authoritative_rollup_decision: str = "keep", candidate_local_decision: str = "keep", best_candidate_decision: str = "keep", topk_block_reason: str = "", branching: bool = True) -> dict[str, object]:
    return {
        "session_id": "session-a",
        "family_id": "family-a",
        "method_family": "regime-aware",
        "family_compare_state": "present",
        "family_compare_path": "G:/Tradex/scratch/research_families/family-a/compare.json",
        "family_compare_hash": "compare-hash",
        "family_decision": family_decision,
        "authoritative_rollup_decision": authoritative_rollup_decision,
        "best_candidate_decision": best_candidate_decision,
        "candidate_local_decision": candidate_local_decision,
        "promote_ready": family_decision == "keep",
        "insufficient_samples": False,
        "meaningful_topk_branching_possible": branching,
        "topk_branching_block_reason": topk_block_reason,
        "decision_reasons": [{"code": "candidate_keep_present", "status": family_decision}],
        "selection_divergence_reason": "top5_member_replacement" if branching else "no_meaningful_branching",
        "candidate_method_id": "regime_aware_v1",
    }


def _provisional_decision(*, decision: str = "hold", blocking_unknowns: list[str] | None = None) -> dict[str, object]:
    return {
        "schema_version": "tradex_research_os_judge_decision_v1",
        "experiment_id": "exp_test",
        "decision": decision,
        "typed_reason": {
            "code": "candidate_keep_present" if decision == "keep" else "family_decision",
            "source_artifact": "family_compare",
            "source_field": "decision_reasons[0]",
            "detail": {},
        },
        "confidence": 0.5,
        "next_action": "manual_review" if decision == "keep" else "collect_more_evidence",
        "blocking_unknowns": list(blocking_unknowns or []),
        "decided_at": "2025-01-31T00:00:00+09:00",
        "decision_policy_version": "provisional",
    }


def _evaluate(*, judge_input: dict[str, object], context: dict[str, object], provisional_decision: dict[str, object]) -> dict[str, object]:
    return decision_policy.evaluate_authoritative_decision(
        experiment_id=str(judge_input["experiment_id"]),
        hypothesis_id="hypothesis-regime-aware-v1",
        method_family="regime-aware",
        judge_input=judge_input,
        authoritative_context=context,
        provisional_decision=provisional_decision,
        decided_at="2025-01-31T01:00:00+09:00",
        policy=decision_policy.load_decision_policy(),
    )


def test_policy_document_matches_json_policy() -> None:
    policy = decision_policy.load_decision_policy()
    doc_path = Path(__file__).resolve().parents[1] / "docs" / "architecture" / "TRADEX_DECISION_POLICY.md"
    doc_text = doc_path.read_text(encoding="utf-8")
    assert policy["decision_policy_version"] in doc_text
    for rule_id in policy["decision_order"]:
        assert rule_id in doc_text or rule_id.replace("_", " ") in doc_text


def test_hard_blocker_drop() -> None:
    judge_input = _base_judge_input()
    context = _base_context(topk_block_reason="effective_universe_too_small_for_topk")
    provisional = _provisional_decision(blocking_unknowns=["effective_universe_too_small_for_topk"])
    decision = _evaluate(judge_input=judge_input, context=context, provisional_decision=provisional)
    assert decision["decision"] == "drop"
    assert "effective_universe_too_small_for_topk" in decision["blocking_reasons"]


def test_insufficient_sample_hold() -> None:
    judge_input = _base_judge_input(sample_count=0)
    judge_input["summary_metrics"] = dict(judge_input["summary_metrics"], insufficient_samples=True)
    context = _base_context()
    provisional = _provisional_decision()
    decision = _evaluate(judge_input=judge_input, context=context, provisional_decision=provisional)
    assert decision["decision"] == "hold"
    assert "insufficient_sample" in decision["blocking_reasons"]


def test_no_branching_hold() -> None:
    judge_input = _base_judge_input(branching=False)
    context = _base_context(branching=False)
    provisional = _provisional_decision()
    decision = _evaluate(judge_input=judge_input, context=context, provisional_decision=provisional)
    assert decision["decision"] == "hold"
    assert decision["blocking_reasons"]


def test_branching_affirmative_keep() -> None:
    judge_input = _base_judge_input()
    context = _base_context(family_decision="keep", authoritative_rollup_decision="keep", candidate_local_decision="keep", best_candidate_decision="keep")
    provisional = _provisional_decision()
    decision = _evaluate(judge_input=judge_input, context=context, provisional_decision=provisional)
    assert decision["decision"] == "keep"
    assert decision["blocking_reasons"] == []


def test_branching_negative_drop() -> None:
    judge_input = _base_judge_input()
    context = _base_context(family_decision="drop", authoritative_rollup_decision="drop", candidate_local_decision="drop", best_candidate_decision="drop")
    provisional = _provisional_decision()
    decision = _evaluate(judge_input=judge_input, context=context, provisional_decision=provisional)
    assert decision["decision"] == "drop"
    assert decision["blocking_reasons"] == ["family_compare_negative"]


def test_missing_or_ambiguous_metrics_hold() -> None:
    judge_input = _base_judge_input()
    judge_input["summary_metrics"] = {}
    judge_input["selection_divergence_reason"] = ""
    judge_input["changed_rank_count"] = None
    context = _base_context(family_decision="", authoritative_rollup_decision="", candidate_local_decision="", best_candidate_decision="")
    provisional = _provisional_decision()
    decision = _evaluate(judge_input=judge_input, context=context, provisional_decision=provisional)
    assert decision["decision"] == "hold"
    assert decision["blocking_reasons"]
