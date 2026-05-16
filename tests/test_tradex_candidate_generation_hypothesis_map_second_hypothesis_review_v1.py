from __future__ import annotations

import json
from pathlib import Path

from scripts import tradex_candidate_generation_hypothesis_map_second_hypothesis_review_v1 as mod


FIRST_SOURCE = "pre_ma20_path_state=base|pre_ma60_context_state=near|weekly_prior_state=up|negative_guard_match=False"
SECOND_SOURCE = "pre_ma20_path_state=base|pre_ma60_context_state=near|weekly_prior_state=up|negative_guard_match=True"


def _status() -> dict[str, object]:
    return {
        "missed_winner": {
            "research_decision.json": {
                "authoritative_research_decision": "missed_winner_source_hypothesis_ready",
                "silent_fallback_used": False,
                "research_fallback_used": False,
            },
            "candidate_generation_hypothesis_map.json": {
                "hypotheses": [
                    {
                        "hypothesis_id": "source_specific_candidate_generation_v1",
                        "source_family": FIRST_SOURCE,
                        "evidence": {
                            "sample_count": 1000,
                            "missed_winner_count": 100,
                            "future_winner_rate": 0.17,
                            "severe_loss_rate20": 0.13,
                            "time_block_stability": 1.0,
                        },
                        "target_failure_mode": "source_under_ranked",
                        "testable_next_axis": "source_specific_candidate_generation_validation_v1",
                    },
                    {
                        "hypothesis_id": "source_specific_candidate_generation_v2",
                        "source_family": SECOND_SOURCE,
                        "evidence": {
                            "sample_count": 700,
                            "missed_winner_count": 90,
                            "future_winner_rate": 0.26,
                            "severe_loss_rate20": 0.215,
                            "time_block_stability": 1.0,
                            "same_date_under_ranked_rate": 0.29,
                        },
                        "target_failure_mode": "source_under_ranked",
                        "testable_next_axis": "source_specific_candidate_generation_validation_v1",
                    },
                ]
            },
            "event_source_quality_leaderboard.json": {"rows": []},
            "selected_nonwinner_source_decomposition.json": {"rows": []},
        },
        "source_validation": {
            "research_decision.json": {"authoritative_research_decision": "source_specific_candidate_generation_drop"},
            "validation_outcome_classification.json": {"validation_outcome": "source_under_ranked_but_unusable"},
            "baseline_comparison_report.json": {"selected_top3_avg_ret20_delta_vs_previous_best": -0.001},
        },
        "applicability": {
            "research_decision.json": {"authoritative_research_decision": "source_applicability_hold"},
            "source_archive_or_refine_decision.json": {"classification": "source_applicability_hold"},
            "overfit_risk_report.json": {"overfit_risk_high": True},
            "point_in_time_applicability_proxy_report.json": {"point_in_time_proxy_found": False},
        },
        "root_cause": {
            "research_decision.json": {"authoritative_research_decision": "root_cause_identified_next_axis_ready"},
            "candidate_generation_hypothesis_map.json": {"hypotheses": []},
        },
    }


def test_second_hypothesis_review_detects_same_failure_risk() -> None:
    status = _status()
    first_context = mod.build_first_hypothesis_archive_context(status)
    profile = mod.build_second_hypothesis_profile(status)
    distinctness = mod.build_hypothesis_distinctness_audit(status)
    quality = mod.build_second_hypothesis_quality_precheck(profile)
    risk = mod.build_second_hypothesis_failure_mode_risk_report(
        distinctness=distinctness,
        quality=quality,
        first_context=first_context,
        profile=profile,
    )
    decision_class = mod._decision_class(distinctness, quality, risk)

    assert first_context["first_hypothesis_rescue_stopped"] is True
    assert profile["sample_count"] == 700
    assert distinctness["overlap_count"] == 0
    assert distinctness["negative_guard_distinguishes_second"] is True
    assert quality["quality_precheck_passed"] is True
    assert risk["same_failure_mode_risk"] == "high"
    assert decision_class == "second_hypothesis_drop_or_skip"


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")


def _make_source_tree(root: Path, decision: dict[str, object], extras: dict[str, dict[str, object]]) -> None:
    _write_json(root / "_ARTIFACT_COMPLETE.json", {"complete": True, "silent_fallback_used": False, "research_fallback_used": False})
    _write_json(root / "research_decision.json", {"silent_fallback_used": False, "research_fallback_used": False, **decision})
    for name, payload in extras.items():
        _write_json(root / name, payload)


def test_second_hypothesis_review_run_writes_required_artifacts(tmp_path: Path) -> None:
    status = _status()
    missed = tmp_path / "missed" / "missed-run"
    validation = tmp_path / "validation" / "validation-run"
    applicability = tmp_path / "applicability" / "applicability-run"
    root = tmp_path / "root" / "root-run"

    _make_source_tree(
        missed,
        status["missed_winner"]["research_decision.json"],
        {
            "candidate_generation_hypothesis_map.json": status["missed_winner"]["candidate_generation_hypothesis_map.json"],
            "event_source_quality_leaderboard.json": status["missed_winner"]["event_source_quality_leaderboard.json"],
            "selected_nonwinner_source_decomposition.json": status["missed_winner"]["selected_nonwinner_source_decomposition.json"],
        },
    )
    _make_source_tree(
        validation,
        status["source_validation"]["research_decision.json"],
        {
            "validation_outcome_classification.json": status["source_validation"]["validation_outcome_classification.json"],
            "baseline_comparison_report.json": status["source_validation"]["baseline_comparison_report.json"],
        },
    )
    _make_source_tree(
        applicability,
        status["applicability"]["research_decision.json"],
        {
            "source_archive_or_refine_decision.json": status["applicability"]["source_archive_or_refine_decision.json"],
            "overfit_risk_report.json": status["applicability"]["overfit_risk_report.json"],
            "point_in_time_applicability_proxy_report.json": status["applicability"]["point_in_time_applicability_proxy_report.json"],
        },
    )
    _make_source_tree(
        root,
        status["root_cause"]["research_decision.json"],
        {"candidate_generation_hypothesis_map.json": status["root_cause"]["candidate_generation_hypothesis_map.json"]},
    )

    result = mod.run_candidate_generation_hypothesis_map_second_hypothesis_review_v1(
        source_missed_winner_run_id="missed-run",
        source_validation_run_id="validation-run",
        source_applicability_run_id="applicability-run",
        source_root_cause_run_id="root-run",
        missed_winner_root=tmp_path / "missed",
        source_validation_root=tmp_path / "validation",
        applicability_root=tmp_path / "applicability",
        root_cause_root=tmp_path / "root",
        output_root=tmp_path / "out",
        run_id="second-review-smoke",
    )
    output_dir = Path(result["output_dir"])
    for artifact in mod.REQUIRED_ARTIFACTS:
        assert (output_dir / artifact).exists(), artifact

    complete = json.loads((output_dir / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    decision = json.loads((output_dir / "research_decision.json").read_text(encoding="utf-8"))

    assert complete["complete"] is True
    assert decision["first_hypothesis_rescue_stopped"] is True
    assert decision["second_hypothesis_review_created"] is True
    assert decision["candidate_generation_challenger_created"] is False
    assert decision["candidate_scoring_created"] is False
    assert decision["production_ranking_changed"] is False
    assert decision["meemee_reflectable"] is False
    assert result["candidate_generation_challenger_created"] is False
