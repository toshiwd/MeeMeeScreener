from __future__ import annotations

import json
from pathlib import Path

from scripts import tradex_candidate_generation_hypothesis_map_refresh_v1 as mod


FIRST_SOURCE = mod.FIRST_ARCHIVED_SOURCE
SECOND_SOURCE = mod.SECOND_DROPPED_SOURCE
REFRESH_SOURCE = (
    "pre_ma20_path_state=pre_ma20_near|"
    "pre_ma60_context_state=pre_ma60_near_or_above|"
    "weekly_prior_state=weekly_prior_strong_up|"
    "negative_guard_match=True"
)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")


def _make_source_tree(root: Path, decision: dict[str, object], extras: dict[str, dict[str, object]], complete_extra: dict[str, object] | None = None) -> None:
    complete = {"complete": True, "silent_fallback_used": False, "research_fallback_used": False}
    if complete_extra:
        complete.update(complete_extra)
    _write_json(root / "_ARTIFACT_COMPLETE.json", complete)
    _write_json(root / "research_decision.json", {"silent_fallback_used": False, "research_fallback_used": False, **decision})
    for name, payload in extras.items():
        _write_json(root / name, payload)


def _quality_row(source: str, **overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "event_source": source,
        "sample_count": 600,
        "day_count": 320,
        "future_winner_count": 130,
        "future_winner_rate": 0.22,
        "missed_winner_count": 80,
        "missed_winner_rate": 0.62,
        "selected_capture_rate_among_source_winners": 0.22,
        "selected_nonwinner_count": 120,
        "selected_nonwinner_rate": 0.55,
        "selected_top3_count": 220,
        "severe_loser_count": 110,
        "severe_loss_rate20": 0.18,
        "avg_MFE20": 0.07,
        "avg_ret20": 0.01,
        "median_ret20": 0.004,
        "win_rate20": 0.52,
        "negative_guard_rate": 1.0,
        "safe_full_rate": 0.0,
    }
    row.update(overrides)
    return row


def _source_status() -> dict[str, object]:
    return {
        "second_review": {
            "research_decision.json": {
                "authoritative_research_decision": "second_hypothesis_drop",
                "decision_classification": "second_hypothesis_drop_or_skip",
                "silent_fallback_used": False,
                "research_fallback_used": False,
            },
            "hypothesis_distinctness_audit.json": {
                "first_source_family": FIRST_SOURCE,
                "second_source_family": SECOND_SOURCE,
                "shared_tag_rate": 0.75,
                "shared_tags": {
                    "pre_ma20_path_state": "pre_ma20_reclaim_base",
                    "pre_ma60_context_state": "pre_ma60_near_or_above",
                    "weekly_prior_state": "weekly_prior_uptrend",
                },
            },
            "second_hypothesis_failure_mode_risk_report.json": {"same_failure_mode_risk": "high"},
            "second_hypothesis_profile.json": {"severe_loss_rate20": 0.215, "selected_nonwinner_rate": 0.5346},
            "first_hypothesis_archive_context.json": {"point_in_time_proxy_found": False},
        },
        "applicability": {
            "research_decision.json": {"authoritative_research_decision": "source_applicability_hold"},
            "source_archive_or_refine_decision.json": {"classification": "source_applicability_hold"},
            "point_in_time_applicability_proxy_report.json": {"point_in_time_proxy_found": False},
        },
        "validation": {
            "research_decision.json": {"authoritative_research_decision": "source_specific_candidate_generation_drop"},
            "validation_outcome_classification.json": {"validation_outcome": "source_under_ranked_but_unusable"},
            "baseline_comparison_report.json": {"selected_top3_avg_ret20_delta_vs_previous_best": -0.01},
        },
        "missed_winner": {
            "research_decision.json": {"authoritative_research_decision": "missed_winner_source_hypothesis_ready"},
            "candidate_generation_hypothesis_map.json": {"hypotheses": []},
            "event_source_quality_leaderboard.json": {
                "rows": [
                    _quality_row(FIRST_SOURCE, missed_winner_count=403, severe_loss_rate20=0.134, selected_nonwinner_rate=0.586),
                    _quality_row(SECOND_SOURCE, missed_winner_count=183, severe_loss_rate20=0.215, selected_nonwinner_rate=0.534),
                    _quality_row(REFRESH_SOURCE),
                ]
            },
            "missed_winner_source_decomposition.json": {"rows": []},
            "selected_nonwinner_source_decomposition.json": {"rows": []},
            "same_date_source_miss_report.json": {
                "winner_available_day_count": 100,
                "winner_source_present_but_under_ranked_rate": 0.30,
                "source_mismatch_explains_miss_rate": 0.20,
            },
            "time_block_source_stability.json": {
                "rows": [
                    {
                        "event_source": REFRESH_SOURCE,
                        "time_block": "2024",
                        "sample_count": 50,
                        "future_winner_rate": 0.24,
                        "severe_loss_rate20": 0.16,
                    },
                    {
                        "event_source": REFRESH_SOURCE,
                        "time_block": "2025",
                        "sample_count": 50,
                        "future_winner_rate": 0.22,
                        "severe_loss_rate20": 0.18,
                    },
                ]
            },
            "max3_source_structure_report.json": {
                "forced_top3_overfill_day_rate": 0.75,
                "source_mix_overfill_rows": [
                    {
                        "source_mix": REFRESH_SOURCE,
                        "day_count": 100,
                        "overfill_day_count": 80,
                        "overfill_day_rate": 0.80,
                        "avg_selected_top3_ret20": 0.02,
                    }
                ],
            },
        },
        "root_cause": {
            "research_decision.json": {"authoritative_research_decision": "root_cause_identified_next_axis_ready"},
            "candidate_generation_hypothesis_map.json": {"hypotheses": []},
            "failure_mode_classification.json": {"primary_failure_mode": "candidate_generation_gap"},
            "max3_deployment_fit_report.json": {"max3_overfill": True},
        },
        "wide": {
            "research_decision.json": {"authoritative_research_decision": "wide_strength_pool_upside_rerank_hold"},
            "missed_winner_selection_report.json": {"missed_winner_count": 10},
            "ranking_coverage_audit.json": {"coverage": "limited"},
        },
    }


def test_refresh_builds_distinct_hypothesis_without_creating_challenger() -> None:
    status = _source_status()
    archive = mod.build_archived_source_failure_summary(status)
    scan = mod.build_remaining_source_scan_report(status)
    diversity = mod.build_mechanism_diversity_report(scan)
    refreshed = mod.build_refreshed_candidate_generation_hypothesis_map(scan)
    decision_class = mod._decision_class(refreshed)
    decision = mod.build_research_decision(refreshed_map=refreshed, decision_class=decision_class, artifact_complete=True)

    assert archive["first_hypothesis_archived"] is True
    assert archive["second_hypothesis_dropped"] is True
    assert scan["remaining_count"] == 1
    assert diversity["mechanism_buckets"]["source_not_selected_due_to_max3_overfill"]["candidate_count"] == 1
    assert refreshed["hypothesis_count"] == 1
    assert refreshed["hypotheses"][0]["source_family"] == REFRESH_SOURCE
    assert refreshed["hypotheses"][0]["risk_profile"]["same_failure_risk"] == "low"
    assert decision["authoritative_research_decision"] == "hypothesis_map_refreshed_next_validation_ready"
    assert decision["candidate_generation_challenger_created"] is False
    assert decision["candidate_scoring_created"] is False
    assert decision["threshold_policy_created"] is False
    assert decision["production_ranking_changed"] is False
    assert decision["meemee_reflectable"] is False


def test_refresh_run_writes_required_artifacts(tmp_path: Path) -> None:
    status = _source_status()
    roots = {
        "second": tmp_path / "second" / "second-run",
        "applicability": tmp_path / "applicability" / "applicability-run",
        "validation": tmp_path / "validation" / "validation-run",
        "missed": tmp_path / "missed" / "missed-run",
        "root": tmp_path / "root" / "root-run",
        "wide": tmp_path / "wide" / "wide-run",
    }
    _make_source_tree(
        roots["second"],
        status["second_review"]["research_decision.json"],
        {
            "hypothesis_distinctness_audit.json": status["second_review"]["hypothesis_distinctness_audit.json"],
            "second_hypothesis_failure_mode_risk_report.json": status["second_review"]["second_hypothesis_failure_mode_risk_report.json"],
            "second_hypothesis_profile.json": status["second_review"]["second_hypothesis_profile.json"],
            "first_hypothesis_archive_context.json": status["second_review"]["first_hypothesis_archive_context.json"],
        },
    )
    _make_source_tree(
        roots["applicability"],
        status["applicability"]["research_decision.json"],
        {
            "source_archive_or_refine_decision.json": status["applicability"]["source_archive_or_refine_decision.json"],
            "point_in_time_applicability_proxy_report.json": status["applicability"]["point_in_time_applicability_proxy_report.json"],
        },
    )
    _make_source_tree(
        roots["validation"],
        status["validation"]["research_decision.json"],
        {
            "validation_outcome_classification.json": status["validation"]["validation_outcome_classification.json"],
            "baseline_comparison_report.json": status["validation"]["baseline_comparison_report.json"],
        },
    )
    _make_source_tree(
        roots["missed"],
        status["missed_winner"]["research_decision.json"],
        {
            "candidate_generation_hypothesis_map.json": status["missed_winner"]["candidate_generation_hypothesis_map.json"],
            "event_source_quality_leaderboard.json": status["missed_winner"]["event_source_quality_leaderboard.json"],
            "missed_winner_source_decomposition.json": status["missed_winner"]["missed_winner_source_decomposition.json"],
            "selected_nonwinner_source_decomposition.json": status["missed_winner"]["selected_nonwinner_source_decomposition.json"],
            "same_date_source_miss_report.json": status["missed_winner"]["same_date_source_miss_report.json"],
            "time_block_source_stability.json": status["missed_winner"]["time_block_source_stability.json"],
            "max3_source_structure_report.json": status["missed_winner"]["max3_source_structure_report.json"],
        },
    )
    _make_source_tree(
        roots["root"],
        status["root_cause"]["research_decision.json"],
        {
            "candidate_generation_hypothesis_map.json": status["root_cause"]["candidate_generation_hypothesis_map.json"],
            "failure_mode_classification.json": status["root_cause"]["failure_mode_classification.json"],
            "max3_deployment_fit_report.json": status["root_cause"]["max3_deployment_fit_report.json"],
        },
    )
    _make_source_tree(
        roots["wide"],
        status["wide"]["research_decision.json"],
        {
            "missed_winner_selection_report.json": status["wide"]["missed_winner_selection_report.json"],
            "ranking_coverage_audit.json": status["wide"]["ranking_coverage_audit.json"],
        },
    )

    result = mod.run_candidate_generation_hypothesis_map_refresh_v1(
        source_second_hypothesis_review_run_id="second-run",
        source_applicability_run_id="applicability-run",
        source_validation_run_id="validation-run",
        source_missed_winner_run_id="missed-run",
        source_root_cause_run_id="root-run",
        source_wide_run_id="wide-run",
        second_review_root=tmp_path / "second",
        applicability_root=tmp_path / "applicability",
        validation_root=tmp_path / "validation",
        missed_winner_root=tmp_path / "missed",
        root_cause_root=tmp_path / "root",
        wide_root=tmp_path / "wide",
        output_root=tmp_path / "out",
        run_id="refresh-smoke",
    )
    output_dir = Path(result["output_dir"])
    for artifact in mod.REQUIRED_ARTIFACTS:
        assert (output_dir / artifact).exists(), artifact

    complete = json.loads((output_dir / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    decision = json.loads((output_dir / "research_decision.json").read_text(encoding="utf-8"))

    assert complete["complete"] is True
    assert decision["first_hypothesis_archived"] is True
    assert decision["second_hypothesis_dropped"] is True
    assert decision["refreshed_hypothesis_count"] == 1
    assert decision["authoritative_research_decision"] == "hypothesis_map_refreshed_next_validation_ready"
    assert decision["candidate_generation_challenger_created"] is False
    assert decision["future_labels_used_in_score_inputs"] is False
    assert result["meemee_reflectable"] is False
