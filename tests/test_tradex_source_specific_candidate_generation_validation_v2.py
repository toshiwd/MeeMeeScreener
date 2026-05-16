from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts import tradex_source_specific_candidate_generation_validation_v2 as mod


SOURCE = (
    "pre_ma20_path_state=pre_ma20_near|"
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


def _complete_tree(root: Path, decision: dict[str, object], extras: dict[str, dict[str, object]], jsonl: dict[str, list[dict[str, object]]] | None = None) -> None:
    _write_json(root / "_ARTIFACT_COMPLETE.json", {"complete": True, "silent_fallback_used": False, "research_fallback_used": False})
    _write_json(root / "research_decision.json", {"silent_fallback_used": False, "research_fallback_used": False, **decision})
    for name, payload in extras.items():
        _write_json(root / name, payload)
    for name, rows in (jsonl or {}).items():
        _write_jsonl(root / name, rows)


def _frame() -> pd.DataFrame:
    rows = []
    for day_idx, event_date in enumerate(["2024-01-02", "2024-01-03", "2024-01-04"]):
        rows.extend(
            [
                {
                    "event_date": event_date,
                    "event_ymd": int(event_date.replace("-", "")),
                    "code": f"A{day_idx}",
                    "source_family": SOURCE,
                    "pre_ma20_path_state": "pre_ma20_near",
                    "pre_ma60_context_state": "pre_ma60_near_or_above",
                    "weekly_prior_state": "weekly_prior_strong_up",
                    "negative_guard_match": "True",
                    "selection_rank": 6.0,
                    "selected_by_previous_best_top3": False,
                    "selected_by_previous_best_top5": True,
                    "ret20_fwd": 0.16,
                    "mfe20": 0.20,
                    "mae20": -0.03,
                    "win20": True,
                    "severe_loss20": False,
                    "is_future_top10_by_ret20": True,
                    "is_big_winner_ret20_ge_10pct": True,
                    "is_big_winner_MFE20_ge_15pct": True,
                    "time_block": "2024",
                    "ret20_rank_pct_by_date": 0.1,
                },
                {
                    "event_date": event_date,
                    "event_ymd": int(event_date.replace("-", "")),
                    "code": f"B{day_idx}",
                    "source_family": "other",
                    "pre_ma20_path_state": "pre_ma20_reclaim_base",
                    "pre_ma60_context_state": "pre_ma60_near_or_above",
                    "weekly_prior_state": "weekly_prior_uptrend",
                    "negative_guard_match": "False",
                    "selection_rank": 1.0,
                    "selected_by_previous_best_top3": True,
                    "selected_by_previous_best_top5": True,
                    "ret20_fwd": -0.01,
                    "mfe20": 0.03,
                    "mae20": -0.09,
                    "win20": False,
                    "severe_loss20": False,
                    "is_future_top10_by_ret20": False,
                    "is_big_winner_ret20_ge_10pct": False,
                    "is_big_winner_MFE20_ge_15pct": False,
                    "time_block": "2024",
                    "ret20_rank_pct_by_date": 0.8,
                },
                {
                    "event_date": event_date,
                    "event_ymd": int(event_date.replace("-", "")),
                    "code": f"C{day_idx}",
                    "source_family": "other",
                    "pre_ma20_path_state": "pre_ma20_reclaim_base",
                    "pre_ma60_context_state": "pre_ma60_near_or_above",
                    "weekly_prior_state": "weekly_prior_uptrend",
                    "negative_guard_match": "False",
                    "selection_rank": 2.0,
                    "selected_by_previous_best_top3": True,
                    "selected_by_previous_best_top5": True,
                    "ret20_fwd": 0.02,
                    "mfe20": 0.04,
                    "mae20": -0.04,
                    "win20": True,
                    "severe_loss20": False,
                    "is_future_top10_by_ret20": False,
                    "is_big_winner_ret20_ge_10pct": False,
                    "is_big_winner_MFE20_ge_15pct": False,
                    "time_block": "2024",
                    "ret20_rank_pct_by_date": 0.5,
                },
                {
                    "event_date": event_date,
                    "event_ymd": int(event_date.replace("-", "")),
                    "code": f"D{day_idx}",
                    "source_family": "other",
                    "pre_ma20_path_state": "pre_ma20_reclaim_base",
                    "pre_ma60_context_state": "pre_ma60_near_or_above",
                    "weekly_prior_state": "weekly_prior_uptrend",
                    "negative_guard_match": "False",
                    "selection_rank": 3.0,
                    "selected_by_previous_best_top3": True,
                    "selected_by_previous_best_top5": True,
                    "ret20_fwd": 0.00,
                    "mfe20": 0.02,
                    "mae20": -0.05,
                    "win20": False,
                    "severe_loss20": False,
                    "is_future_top10_by_ret20": False,
                    "is_big_winner_ret20_ge_10pct": False,
                    "is_big_winner_MFE20_ge_15pct": False,
                    "time_block": "2024",
                    "ret20_rank_pct_by_date": 0.6,
                },
                {
                    "event_date": event_date,
                    "event_ymd": int(event_date.replace("-", "")),
                    "code": f"E{day_idx}",
                    "source_family": "other",
                    "pre_ma20_path_state": "pre_ma20_reclaim_base",
                    "pre_ma60_context_state": "pre_ma60_near_or_above",
                    "weekly_prior_state": "weekly_prior_uptrend",
                    "negative_guard_match": "False",
                    "selection_rank": 4.0,
                    "selected_by_previous_best_top3": False,
                    "selected_by_previous_best_top5": True,
                    "ret20_fwd": -0.02,
                    "mfe20": 0.02,
                    "mae20": -0.06,
                    "win20": False,
                    "severe_loss20": False,
                    "is_future_top10_by_ret20": False,
                    "is_big_winner_ret20_ge_10pct": False,
                    "is_big_winner_MFE20_ge_15pct": False,
                    "time_block": "2024",
                    "ret20_rank_pct_by_date": 0.7,
                },
                {
                    "event_date": event_date,
                    "event_ymd": int(event_date.replace("-", "")),
                    "code": f"F{day_idx}",
                    "source_family": "other",
                    "pre_ma20_path_state": "pre_ma20_reclaim_base",
                    "pre_ma60_context_state": "pre_ma60_near_or_above",
                    "weekly_prior_state": "weekly_prior_uptrend",
                    "negative_guard_match": "False",
                    "selection_rank": 5.0,
                    "selected_by_previous_best_top3": False,
                    "selected_by_previous_best_top5": True,
                    "ret20_fwd": -0.03,
                    "mfe20": 0.01,
                    "mae20": -0.07,
                    "win20": False,
                    "severe_loss20": False,
                    "is_future_top10_by_ret20": False,
                    "is_big_winner_ret20_ge_10pct": False,
                    "is_big_winner_MFE20_ge_15pct": False,
                    "time_block": "2024",
                    "ret20_rank_pct_by_date": 0.9,
                },
            ]
        )
    return pd.DataFrame(rows)


def test_validation_v2_metrics_keep_boundary_flags() -> None:
    frame = mod._source_frame(_frame(), SOURCE)
    primary = mod._selected_from_ledger(mod.build_selection_ledger(frame, max_source_slots=1, family_id=mod.PRIMARY_FAMILY_ID))
    diagnostic = mod._selected_from_ledger(mod.build_selection_ledger(frame, max_source_slots=2, family_id=mod.DIAGNOSTIC_FAMILY_ID))
    top3 = mod.build_top3_selection_report(frame, primary, diagnostic)
    top5 = mod.build_top5_candidate_pool_report(frame)
    recovery = mod.build_source_recovery_report(frame, primary)
    noise = mod.build_source_noise_report(recovery)
    timeblock = mod.build_time_block_source_validation(frame, primary)
    comparison = mod.build_baseline_comparison_report(top3, top5, recovery, noise, timeblock)
    branching = mod.build_branching_report(top3)
    context = {"source_definition_applicable_point_in_time": True}
    same_date = {"same_date_support_not_faked": True, "per_source_same_date_support_available": False}
    outcome = mod.build_validation_outcome_classification(
        overlap=mod.build_source_overlap_audit(frame),
        comparison=comparison,
        recovery=recovery,
        noise=noise,
        timeblock=timeblock,
        context=context,
        same_date=same_date,
    )
    decision = mod.build_research_decision(
        comparison=comparison,
        recovery=recovery,
        noise=noise,
        branching=branching,
        timeblock=timeblock,
        context=context,
        same_date=same_date,
        outcome=outcome,
        artifact_complete=True,
        selected_hypothesis_id="candidate_generation_map_refresh_hypothesis_1",
    )

    assert top3["changed_top3_members_count_vs_previous_best"] > 0
    assert top5["changed_top5_members_count_vs_previous_best"] > 0
    assert top5["evaluation_role"] == "primary_top5_candidate_pool_quality"
    assert comparison["forced_top3_is_primary"] is False
    assert comparison["top5_avg_ret20_delta_vs_previous_best"] > 0
    assert comparison["top5_big_winner_capture_rate_delta_vs_previous_best"] > 0
    assert recovery["recovered_missed_winner_count"] > 0
    assert decision["candidate_generation_challenger_created"] is True
    assert decision["primary_metric_scope"] == "top5_candidate_pool_quality"
    assert decision["final_max3_selection_owner"] == "human_user"
    assert decision["candidate_scoring_created"] is False
    assert decision["threshold_policy_created"] is False
    assert decision["production_ranking_changed"] is False
    assert decision["meemee_reflectable"] is False
    assert decision["same_date_support_not_faked"] is True


def test_validation_v2_run_writes_required_artifacts(tmp_path: Path, monkeypatch) -> None:
    roots = {
        "mechanism": tmp_path / "mechanism" / "mechanism-run",
        "refresh": tmp_path / "refresh" / "refresh-run",
        "second": tmp_path / "second" / "second-run",
        "missed": tmp_path / "missed" / "missed-run",
        "root": tmp_path / "root" / "root-run",
        "wide": tmp_path / "wide" / "wide-run",
        "pattern": tmp_path / "pattern" / "pattern-run",
        "upside": tmp_path / "upside" / "upside-run",
        "feature": tmp_path / "feature" / "feature-run",
    }
    _complete_tree(
        roots["mechanism"],
        {"authoritative_research_decision": "source_mechanism_validation_next_target_ready"},
        {
            "selected_next_validation_target.json": {
                "selected": True,
                "selected_hypothesis_id": "candidate_generation_map_refresh_hypothesis_1",
                "selected_next_axis": "source_specific_candidate_generation_validation_v2",
                "selected_source_family": SOURCE,
            },
            "per_source_same_date_support_report.json": {
                "per_source_same_date_support_available": False,
                "missing_required_source_fields": ["pre_ma60_context_state"],
                "aggregate_same_date_support_only": {"winner_source_present_but_under_ranked_rate": 0.29},
            },
        },
    )
    _complete_tree(
        roots["refresh"],
        {"authoritative_research_decision": "hypothesis_map_refreshed_next_validation_ready"},
        {
            "refreshed_candidate_generation_hypothesis_map.json": {
                "hypotheses": [
                    {
                        "hypothesis_id": "candidate_generation_map_refresh_hypothesis_1",
                        "source_family": SOURCE,
                        "expected_mechanism": "source_not_selected_due_to_max3_overfill",
                    }
                ]
            }
        },
    )
    _complete_tree(roots["second"], {"authoritative_research_decision": "second_hypothesis_drop"}, {})
    _complete_tree(
        roots["missed"],
        {"authoritative_research_decision": "missed_winner_source_hypothesis_ready"},
        {
            "source_artifact_refs.json": {
                "source_roots": {
                    "pattern": str(roots["pattern"]),
                    "upside": str(roots["upside"]),
                    "feature_diagnosis": str(roots["feature"]),
                },
                "refs": [],
            }
        },
    )
    _complete_tree(roots["root"], {"authoritative_research_decision": "root_cause_identified_next_axis_ready"}, {})
    _complete_tree(
        roots["wide"],
        {"authoritative_research_decision": "wide_strength_pool_upside_rerank_hold"},
        {},
        jsonl={"date_level_selection_ledger.jsonl": [{"event_date": "2024-01-02", "pre_ma20_path_state": "pre_ma20_near"}]},
    )
    for key in ("pattern", "upside", "feature"):
        _write_json(roots[key] / "_ARTIFACT_COMPLETE.json", {"complete": True, "silent_fallback_used": False, "research_fallback_used": False})

    monkeypatch.setattr(mod, "load_validation_frame", lambda **_kwargs: _frame())
    result = mod.run_source_specific_candidate_generation_validation_v2(
        source_mechanism_validation_run_id="mechanism-run",
        source_hypothesis_refresh_run_id="refresh-run",
        source_second_hypothesis_review_run_id="second-run",
        source_missed_winner_run_id="missed-run",
        source_root_cause_run_id="root-run",
        source_wide_run_id="wide-run",
        mechanism_validation_root=tmp_path / "mechanism",
        hypothesis_refresh_root=tmp_path / "refresh",
        second_review_root=tmp_path / "second",
        missed_winner_root=tmp_path / "missed",
        root_cause_root=tmp_path / "root",
        wide_root=tmp_path / "wide",
        output_root=tmp_path / "out",
        run_id="validation-v2-smoke",
    )
    output_dir = Path(result["output_dir"])
    for artifact in mod.REQUIRED_ARTIFACTS:
        assert (output_dir / artifact).exists(), artifact
    complete = json.loads((output_dir / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    decision = json.loads((output_dir / "research_decision.json").read_text(encoding="utf-8"))
    limitation = json.loads((output_dir / "same_date_support_limitation_report.json").read_text(encoding="utf-8"))
    top5 = json.loads((output_dir / "top5_candidate_pool_report.json").read_text(encoding="utf-8"))

    assert complete["complete"] is True
    assert decision["selected_hypothesis_id"] == "candidate_generation_map_refresh_hypothesis_1"
    assert decision["forced_top3_is_primary"] is False
    assert decision["primary_metric_scope"] == "top5_candidate_pool_quality"
    assert decision["final_max3_selection_owner"] == "human_user"
    assert top5["evaluation_role"] == "primary_top5_candidate_pool_quality"
    assert top5["candidate_pool_size_target"] == 5
    assert decision["candidate_generation_challenger_created"] is True
    assert decision["candidate_scoring_created"] is False
    assert decision["future_labels_used_in_candidate_generation"] is False
    assert limitation["per_source_same_date_support_available"] is False
    assert limitation["same_date_support_not_faked"] is True
