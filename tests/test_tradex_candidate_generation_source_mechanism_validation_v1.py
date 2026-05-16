from __future__ import annotations

import json
from pathlib import Path

from scripts import tradex_candidate_generation_hypothesis_map_refresh_v1 as refresh_mod
from scripts import tradex_candidate_generation_source_mechanism_validation_v1 as mod


HYPOTHESIS_A = (
    "pre_ma20_path_state=pre_ma20_near|"
    "pre_ma60_context_state=pre_ma60_near_or_above|"
    "weekly_prior_state=weekly_prior_strong_up|"
    "negative_guard_match=True"
)
HYPOTHESIS_B = (
    "pre_ma20_path_state=pre_ma20_already_extended|"
    "pre_ma60_context_state=pre_ma60_near_or_above|"
    "weekly_prior_state=weekly_prior_strong_up|"
    "negative_guard_match=True"
)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")


def _make_source_tree(root: Path, decision: dict[str, object], extras: dict[str, dict[str, object]], *, jsonl: dict[str, list[dict[str, object]]] | None = None) -> None:
    _write_json(root / "_ARTIFACT_COMPLETE.json", {"complete": True, "silent_fallback_used": False, "research_fallback_used": False})
    _write_json(root / "research_decision.json", {"silent_fallback_used": False, "research_fallback_used": False, **decision})
    for name, payload in extras.items():
        _write_json(root / name, payload)
    for name, rows in (jsonl or {}).items():
        _write_jsonl(root / name, rows)


def _source_status() -> dict[str, object]:
    hypotheses = [
        {
            "hypothesis_id": "candidate_generation_map_refresh_hypothesis_1",
            "source_family": HYPOTHESIS_A,
            "expected_mechanism": "source_not_selected_due_to_max3_overfill",
            "target_failure_mode": "source_not_selected_due_to_max3_overfill",
            "risk_profile": {
                "future_winner_rate": 0.23,
                "missed_winner_count": 92,
                "same_failure_risk": "low",
                "selected_nonwinner_rate": 0.61,
                "severe_loss_rate20": 0.24,
            },
        },
        {
            "hypothesis_id": "candidate_generation_map_refresh_hypothesis_2",
            "source_family": HYPOTHESIS_B,
            "expected_mechanism": "source_label_mismatch_high_MFE_low_ret20",
            "target_failure_mode": "source_label_mismatch_high_MFE_low_ret20",
            "risk_profile": {
                "future_winner_rate": 0.27,
                "missed_winner_count": 35,
                "same_failure_risk": "low",
                "selected_nonwinner_rate": 0.58,
                "severe_loss_rate20": 0.25,
            },
        },
    ]
    return {
        "hypothesis_refresh": {
            "research_decision.json": {
                "authoritative_research_decision": "hypothesis_map_refreshed_next_validation_ready",
                "refreshed_hypothesis_count": 2,
                "candidate_generation_challenger_created": False,
            },
            "refreshed_candidate_generation_hypothesis_map.json": {"hypotheses": hypotheses, "hypothesis_count": 2},
            "remaining_source_scan_report.json": {
                "rows": [
                    {
                        "event_source": HYPOTHESIS_A,
                        "sample_count": 1000,
                        "missed_winner_count": 92,
                        "future_winner_rate": 0.23,
                        "severe_loss_rate20": 0.24,
                        "selected_nonwinner_rate": 0.61,
                        "time_block_stability": 0.82,
                        "same_date_source_miss_support": None,
                        "same_date_source_miss_support_available": False,
                        "max3_structure_fit": 0.71,
                        "selected_capture_rate_among_source_winners": 0.30,
                        "mechanisms": ["source_not_selected_due_to_max3_overfill"],
                    },
                    {
                        "event_source": HYPOTHESIS_B,
                        "sample_count": 650,
                        "missed_winner_count": 35,
                        "future_winner_rate": 0.27,
                        "severe_loss_rate20": 0.25,
                        "selected_nonwinner_rate": 0.58,
                        "time_block_stability": 0.82,
                        "same_date_source_miss_support": None,
                        "same_date_source_miss_support_available": False,
                        "max3_structure_fit": 0.42,
                        "selected_capture_rate_among_source_winners": 0.31,
                        "mechanisms": ["source_label_mismatch_high_MFE_low_ret20"],
                    },
                ],
                "same_date_source_miss_report_summary": {"per_source_rows_available": False},
            },
            "archived_source_failure_summary.json": {"first_hypothesis_archived": True, "second_hypothesis_dropped": True},
        },
        "second_review": {
            "research_decision.json": {"authoritative_research_decision": "second_hypothesis_drop"},
            "hypothesis_distinctness_audit.json": {"shared_tag_rate": 0.75},
        },
        "applicability": {
            "research_decision.json": {"authoritative_research_decision": "source_applicability_hold"},
            "point_in_time_applicability_proxy_report.json": {"point_in_time_proxy_found": False},
        },
        "validation": {
            "research_decision.json": {"authoritative_research_decision": "source_specific_candidate_generation_drop"},
            "validation_outcome_classification.json": {"validation_outcome": "drop"},
        },
        "missed_winner": {
            "research_decision.json": {"authoritative_research_decision": "missed_winner_source_hypothesis_ready"},
            "same_date_source_miss_report.json": {
                "winner_available_day_count": 100,
                "winner_source_present_but_under_ranked_rate": 0.29,
                "source_mismatch_explains_miss_rate": 0.60,
            },
            "time_block_source_stability.json": {"rows": []},
            "max3_source_structure_report.json": {"source_mix_overfill_rows": []},
        },
        "root_cause": {
            "research_decision.json": {"authoritative_research_decision": "root_cause_identified_next_axis_ready"},
            "failure_mode_classification.json": {"primary_failure_mode": "candidate_generation_gap"},
        },
        "wide": {
            "research_decision.json": {"authoritative_research_decision": "wide_strength_pool_upside_rerank_hold"},
            "ranking_coverage_audit.json": {"coverage": "limited"},
        },
    }


def test_source_mechanism_validation_selects_one_without_creating_challenger(tmp_path: Path) -> None:
    status = _source_status()
    distinct = mod.build_archived_source_distinctness_audit(status)
    mechanism = mod.build_hypothesis_source_mechanism_report(status)
    same_date = mod.build_per_source_same_date_support_report(status=status, wide_dir=tmp_path)
    leaderboard = mod.build_hypothesis_validation_readiness_leaderboard(mechanism_report=mechanism, distinctness=distinct, same_date_report=same_date)
    selection = mod.build_selected_next_validation_target(leaderboard)
    decision_class = mod._decision_class(selection, leaderboard)
    decision = mod.build_research_decision(selection=selection, same_date=same_date, decision_class=decision_class, artifact_complete=True)

    assert distinct["rows"][0]["meaningfully_distinct_from_archived_sources"] is True
    assert same_date["per_source_same_date_support_available"] is False
    assert selection["selected"] is True
    assert selection["selected_hypothesis_id"] == "candidate_generation_map_refresh_hypothesis_1"
    assert selection["selected_next_axis"] == "source_specific_candidate_generation_validation_v2"
    assert "per_source_same_date_support_missing_but_not_faked" in selection["reason"]
    assert decision["authoritative_research_decision"] == "source_mechanism_validation_next_target_ready"
    assert decision["candidate_generation_challenger_created"] is False
    assert decision["candidate_scoring_created"] is False
    assert decision["threshold_policy_created"] is False
    assert decision["production_ranking_changed"] is False
    assert decision["meemee_reflectable"] is False


def test_source_mechanism_validation_run_writes_required_artifacts(tmp_path: Path) -> None:
    status = _source_status()
    roots = {
        "refresh": tmp_path / "refresh" / "refresh-run",
        "second": tmp_path / "second" / "second-run",
        "applicability": tmp_path / "applicability" / "applicability-run",
        "validation": tmp_path / "validation" / "validation-run",
        "missed": tmp_path / "missed" / "missed-run",
        "root": tmp_path / "root" / "root-run",
        "wide": tmp_path / "wide" / "wide-run",
    }
    _make_source_tree(
        roots["refresh"],
        status["hypothesis_refresh"]["research_decision.json"],
        {
            "refreshed_candidate_generation_hypothesis_map.json": status["hypothesis_refresh"]["refreshed_candidate_generation_hypothesis_map.json"],
            "remaining_source_scan_report.json": status["hypothesis_refresh"]["remaining_source_scan_report.json"],
            "archived_source_failure_summary.json": status["hypothesis_refresh"]["archived_source_failure_summary.json"],
        },
    )
    _make_source_tree(
        roots["second"],
        status["second_review"]["research_decision.json"],
        {"hypothesis_distinctness_audit.json": status["second_review"]["hypothesis_distinctness_audit.json"]},
    )
    _make_source_tree(
        roots["applicability"],
        status["applicability"]["research_decision.json"],
        {"point_in_time_applicability_proxy_report.json": status["applicability"]["point_in_time_applicability_proxy_report.json"]},
    )
    _make_source_tree(
        roots["validation"],
        status["validation"]["research_decision.json"],
        {"validation_outcome_classification.json": status["validation"]["validation_outcome_classification.json"]},
    )
    _make_source_tree(
        roots["missed"],
        status["missed_winner"]["research_decision.json"],
        {
            "same_date_source_miss_report.json": status["missed_winner"]["same_date_source_miss_report.json"],
            "time_block_source_stability.json": status["missed_winner"]["time_block_source_stability.json"],
            "max3_source_structure_report.json": status["missed_winner"]["max3_source_structure_report.json"],
        },
    )
    _make_source_tree(
        roots["root"],
        status["root_cause"]["research_decision.json"],
        {"failure_mode_classification.json": status["root_cause"]["failure_mode_classification.json"]},
    )
    _make_source_tree(
        roots["wide"],
        status["wide"]["research_decision.json"],
        {"ranking_coverage_audit.json": status["wide"]["ranking_coverage_audit.json"]},
        jsonl={
            "date_level_selection_ledger.jsonl": [
                {
                    "event_date": "2026-01-01",
                    "pre_ma20_path_state": "pre_ma20_near",
                    "weekly_prior_state": "weekly_prior_strong_up",
                    "negative_guard_match": True,
                }
            ]
        },
    )

    result = mod.run_candidate_generation_source_mechanism_validation_v1(
        source_hypothesis_refresh_run_id="refresh-run",
        source_second_hypothesis_review_run_id="second-run",
        source_applicability_run_id="applicability-run",
        source_validation_run_id="validation-run",
        source_missed_winner_run_id="missed-run",
        source_root_cause_run_id="root-run",
        source_wide_run_id="wide-run",
        hypothesis_refresh_root=tmp_path / "refresh",
        second_review_root=tmp_path / "second",
        applicability_root=tmp_path / "applicability",
        validation_root=tmp_path / "validation",
        missed_winner_root=tmp_path / "missed",
        root_cause_root=tmp_path / "root",
        wide_root=tmp_path / "wide",
        output_root=tmp_path / "out",
        run_id="mechanism-smoke",
    )
    output_dir = Path(result["output_dir"])
    for artifact in mod.REQUIRED_ARTIFACTS:
        assert (output_dir / artifact).exists(), artifact

    complete = json.loads((output_dir / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    selection = json.loads((output_dir / "selected_next_validation_target.json").read_text(encoding="utf-8"))
    decision = json.loads((output_dir / "research_decision.json").read_text(encoding="utf-8"))

    assert complete["complete"] is True
    assert selection["selected"] is True
    assert selection["selected_hypothesis_id"] == "candidate_generation_map_refresh_hypothesis_1"
    assert decision["selected_next_validation_target_created"] is True
    assert decision["per_source_same_date_support_available"] is False
    assert decision["future_labels_used_in_score_inputs"] is False
    assert result["candidate_generation_challenger_created"] is False


def test_archived_sources_are_not_selected() -> None:
    status = _source_status()
    status["hypothesis_refresh"]["refreshed_candidate_generation_hypothesis_map.json"]["hypotheses"][0]["source_family"] = refresh_mod.FIRST_ARCHIVED_SOURCE
    distinct = mod.build_archived_source_distinctness_audit(status)
    assert distinct["rows"][0]["meaningfully_distinct_from_archived_sources"] is False
