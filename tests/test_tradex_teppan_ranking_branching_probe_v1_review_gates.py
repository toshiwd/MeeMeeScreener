from __future__ import annotations

import json
from pathlib import Path

from scripts import tradex_teppan_ranking_branching_probe_v1_review_gates as gates


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _source_root(tmp_path: Path) -> Path:
    root = tmp_path / "source"
    pattern_dir = tmp_path / "pattern" / "run"
    guard_dir = tmp_path / "guard" / "run"
    source_db = tmp_path / "stocks.duckdb"
    pattern_dir.mkdir(parents=True)
    guard_dir.mkdir(parents=True)
    source_db.write_text("stub", encoding="utf-8")
    same_condition = {
        "schema_version": "tradex_research_contract_v1",
        "top_k": 10,
        "regime": "all",
        "period": [{"start_date": "20250101", "end_date": "20250131", "label": "test"}],
        "cost_model": {"mode": "flat_zero_cost"},
        "artifact_detail_level": "authoritative_full",
        "fallback_status": "authoritative",
        "feature_family": "boundary_feature",
        "contract_hash": "same-condition",
    }
    compare = {
        "schema_version": "tradex_teppan_ranking_branching_probe_v1_compare_v1",
        "same_condition_contract": same_condition,
        "ranking_compare": {
            "top5": {"delta": {"avg_ret20": 0.01, "median_ret20": 0.005, "severe_loss_rate20": -0.01}},
            "top10": {"delta": {"avg_ret20": 0.02, "median_ret20": 0.006, "severe_loss_rate20": -0.02}},
        },
        "silent_fallback_used": False,
    }
    branching = {
        "changed_top5_members_count": 2,
        "changed_top10_members_count": 2,
        "changed_rank_count": 8,
        "selection_divergence_reason": "top5_member_swap",
        "future_labels_used_in_selection": False,
        "silent_fallback_used": False,
    }
    coverage = {
        "complete_champion_ranking_available": True,
        "complete_top20_decision_set_rate": 1.0,
        "complete_top10_decision_set_rate": 1.0,
        "complete_top5_decision_set_rate": 1.0,
    }
    decision = {
        "decision": "keep",
        "candidate_local_decision": "keep",
        "session_aggregate_decision": "keep",
        "authoritative_research_decision": "teppan_ranking_branching_keep_candidate",
        "typed_reason": "same_condition_branching_helped_and_coverage_complete",
        "candidate_scoring_created": True,
        "production_ranking_changed": False,
        "meemee_reflectable": False,
        "publish_bundle_created": False,
        "silent_fallback_used": False,
    }
    evaluation_contract = {
        "schema_version": "tradex_teppan_ranking_branching_probe_v1_evaluation_contract_v1",
        "same_condition_contract": same_condition,
        "ranking_adjustment": {
            "mode": "static_teppan_guarded_soft_boost",
            "boost_value": 0.04,
            "eligible_side": "long",
            "eligible_champion_rank_min": 6,
            "eligible_champion_rank_max": 20,
            "direction": "up",
            "rank_limit": 20,
        },
        "future_label_policy": {
            "future_labels_used_in_selection": False,
            "forward_ret_20d_used_for_evaluation_only": True,
        },
        "silent_fallback_used": False,
    }
    source_refs = {
        "source_mode": "runtime-ranking",
        "source_db": str(source_db),
        "source_rows_parquet": str(tmp_path / "unused.parquet"),
        "pattern_dir": str(pattern_dir),
        "guard_dir": str(guard_dir),
        "silent_fallback_used": False,
    }
    complete = {"complete": True, "silent_fallback_used": False}
    run_manifest = {"schema_version": "test", "asof": "20250131"}
    for name, payload in {
        "evaluation_contract.json": evaluation_contract,
        "run_manifest.json": run_manifest,
        "source_artifact_refs.json": source_refs,
        "ranking_coverage_audit.json": coverage,
        "branching_probe.json": branching,
        "compare.json": compare,
        "research_decision.json": decision,
        "_ARTIFACT_COMPLETE.json": complete,
    }.items():
        _write_json(root / name, payload)
    (root / "selected_event_ledger.jsonl").write_text('{"forward_ret_20d":0.01}\n', encoding="utf-8")
    return root


def _replay_stub(source_root: Path):
    return {"output_dir": str(source_root)}


def test_publish_review_gate_passes_and_preserves_meemee_boundary(tmp_path: Path) -> None:
    source_root = _source_root(tmp_path)
    payload = gates.publish_review_outputs(
        source_root=source_root,
        output_root=tmp_path / "review",
        replay_runner=lambda _temp_root: _replay_stub(source_root),
    )

    assert payload["decision"] == "pass_to_manual_review"
    assert payload["publish_review_decision"]["decision_reason"] == "source_ready_for_manual_review"
    assert payload["publish_review_decision"]["production_ranking_changed"] is False
    assert payload["publish_review_decision"]["meemee_reflectable_now"] is False
    assert payload["publish_review_decision"]["no_meemee_mutation"] is True
    assert payload["source_artifact_integrity"]["compare_decision_keep"] is True
    assert payload["feature_availability_audit"]["pass"] is True
    assert payload["reproducibility_audit"]["matches_within_tolerance"] is True
    assert payload["anti_leakage_recheck"]["pass"] is True
    assert "forward_ret_20d" in payload["ranking_adjustment_contract"]["forbidden_inputs"]
    assert "selected_event_ledger.jsonl" in payload["meemee_exposure_assessment"]["forbidden_meemee_exposure"]
    assert payload["shadow_publish_bundle_manifest"]["bundle_status"] == "complete"

    for name in gates.GATE_REQUIRED_ARTIFACTS:
        assert (Path(payload["output_root"]) / name).exists(), name
    for name in [
        "published_logic_artifact.json",
        "published_logic_manifest.json",
        "validation_summary.json",
        "source_artifact_refs.json",
        "ranking_adjustment_contract.json",
        "meemee_exposure_assessment.json",
        "bundle_manifest.json",
    ]:
        assert (Path(payload["output_root"]) / "shadow_publish_bundle" / name).exists(), name


def test_publish_review_gate_blocks_when_source_is_not_keep(tmp_path: Path) -> None:
    source_root = _source_root(tmp_path)
    decision_path = source_root / "research_decision.json"
    decision = json.loads(decision_path.read_text(encoding="utf-8"))
    decision["decision"] = "hold"
    decision["candidate_local_decision"] = "hold"
    decision_path.write_text(json.dumps(decision, ensure_ascii=False), encoding="utf-8")

    payload = gates.publish_review_outputs(
        source_root=source_root,
        output_root=tmp_path / "review",
        replay_runner=lambda _temp_root: _replay_stub(source_root),
    )

    assert payload["decision"] == "blocked"
    assert "source_decision_not_keep" in payload["publish_review_decision"]["blockers"]
    assert payload["shadow_publish_bundle_manifest"]["bundle_status"] == "blocked_draft"
