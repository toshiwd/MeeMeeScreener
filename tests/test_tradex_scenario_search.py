from __future__ import annotations

import json
import sys
from pathlib import Path

from app.backend.tools import tradex_research_runner as runner


def _seed_record(*, candidate_id: str, population_kind: str, method_family: str, feature_family: str, decision: str, score: float, parent_candidate_ids: list[str] | None = None) -> dict[str, object]:
    parent_candidate_ids = parent_candidate_ids or []
    signal_bias = "sell" if population_kind == "short_expert_population" else "balanced" if population_kind == "regime_selector_population" else "buy"
    ranking_target = {
        "long_expert_population": "topk_uplift",
        "short_expert_population": "bad_pick_removal",
        "regime_selector_population": "regime_balance",
    }[population_kind]
    holding_days = 10 if population_kind == "short_expert_population" else 20
    return {
        "candidate_id": candidate_id,
        "generation_index": 0,
        "population_kind": population_kind,
        "family_id": f"seed-{population_kind}",
        "family_title": population_kind,
        "method_family": method_family,
        "method_id": f"{candidate_id}-method",
        "method_title": f"{population_kind} method",
        "method_thesis": f"{population_kind} thesis",
        "feature_family": feature_family,
        "decision": decision,
        "sample_validity": True,
        "fallback_status": "authoritative",
        "artifact_detail_level": "authoritative",
        "ret20_source_mode": runner.tradex.TRADEX_RET20_SOURCE_MODE_PRECOMPUTED,
        "ret20_source_mode_reason": "",
        "same_condition_contract_hash": f"contract-{population_kind}",
        "same_condition_contract": {"contract_hash": f"contract-{population_kind}"},
        "scenario_definition": {
            "direction": "short" if population_kind == "short_expert_population" else "long",
            "entry_family": method_family,
            "filter_family": feature_family,
            "ranking_target": ranking_target,
            "holding_days": holding_days,
            "exit_family": "risk_exit" if population_kind == "short_expert_population" else "time_stop",
            "stop_type": "hard" if population_kind == "short_expert_population" else "trailing",
            "regime_gate": "trend_short" if population_kind == "short_expert_population" else "trend_long",
            "liquidity_gate": "tight" if population_kind == "short_expert_population" else "balanced",
            "weight_profile": "bad_pick_heavy" if population_kind == "short_expert_population" else "regime_heavy" if population_kind == "regime_selector_population" else "boundary_heavy",
            "mutation_parent_ids": parent_candidate_ids,
            "generation_index": 0,
            "population_kind": population_kind,
        },
        "evaluation_metrics": {
            "sample_count": 18,
            "promote_ready": decision == "keep",
            "top5_ret20_mean": score / 100.0,
            "top10_ret20_mean": score / 120.0,
            "monthly_capture_mean": 0.15,
            "worst_regime_ret20_mean": 0.04,
            "dd": 0.03,
            "turnover": 0.10,
            "liquidity_fail_rate": 0.02,
            "changed_top5_members_count": 1,
            "changed_top10_members_count": 1,
            "changed_rank_count": 1,
            "top5_boundary_score_gap": 0.01,
            "top10_boundary_score_gap": 0.01,
            "selection_divergence_reason": "branchable",
        },
        "branching_metrics": {
            "meaningful_topk_branching_possible": True,
            "topk_branching_block_reason": "",
            "top_k": 5,
            "effective_universe_count": 20,
            "candidate_in_scope_before_build_count": 18,
            "candidate_in_scope_after_build_count": 16,
            "candidate_removed_by_scope_boundary_count": 2,
            "scope_filter_applied_stage": "final",
            "key_normalization_mode": "normalized",
        },
        "regime_summary": {
            "regime_tag": "trend_long" if population_kind != "short_expert_population" else "trend_short",
            "long_horizon_regime_score": 0.2,
            "recent_adaptation_score": 0.1,
        },
        "failure_reason": "",
        "parent_candidate_ids": parent_candidate_ids,
        "source_kind": "seed",
        "score": score,
        "metrics": {"sample_count": 18},
        "comparison": {
            "challenger_top5_ret20_mean": score / 100.0,
            "challenger_top10_ret20_mean": score / 120.0,
            "challenger_monthly_capture_mean": 0.15,
            "challenger_worst_regime_ret20_mean": 0.04,
            "challenger_dd": 0.03,
            "challenger_turnover": 0.10,
            "challenger_liquidity_fail_rate": 0.02,
        },
    }


def _seed_memory() -> dict[str, object]:
    return {
        "schema_version": runner.SCENARIO_SEARCH_MEMORY_SCHEMA_VERSION,
        "generated_at": "2026-04-12T00:00:00Z",
        "source_artifact_refs": [{"session_id": "seed-1"}],
        "record_count": 3,
        "population_summary": {
            "long_expert_population": {"count": 1, "keep": 1, "hold": 0, "drop": 0},
            "short_expert_population": {"count": 1, "keep": 0, "hold": 1, "drop": 0},
            "regime_selector_population": {"count": 1, "keep": 0, "hold": 0, "drop": 1},
        },
        "records": [
            _seed_record(
                candidate_id="seed-long",
                population_kind="long_expert_population",
                method_family="long_breakout",
                feature_family="boundary_feature",
                decision="keep",
                score=63.0,
            ),
            _seed_record(
                candidate_id="seed-short",
                population_kind="short_expert_population",
                method_family="short_downtrend",
                feature_family="bad_pick_removal",
                decision="hold",
                score=54.0,
            ),
            _seed_record(
                candidate_id="seed-regime",
                population_kind="regime_selector_population",
                method_family="regime_router",
                feature_family="regime_adjustment",
                decision="drop",
                score=41.0,
            ),
        ],
    }


def _fake_research_session(*, session_id: str, random_seed: int, family_specs=None, **kwargs) -> dict[str, object]:
    del kwargs
    family_specs = tuple(family_specs or ())
    session_dir = runner._session_dir(session_id)
    leaderboard_path = runner._session_family_leaderboard_file(session_id)
    compare_path = runner._session_compare_file(session_id)

    family_results: list[dict[str, object]] = []
    family_summary: list[dict[str, object]] = []
    for index, family_spec in enumerate(family_specs):
        candidate_results: list[dict[str, object]] = []
        for cand_index, candidate_spec in enumerate(family_spec.candidates):
            plan = dict(candidate_spec.plan_overrides)
            candidate_id = f"{session_id}-{candidate_spec.method_family}-{index}-{cand_index}"
            candidate_result = {
                "plan_id": candidate_spec.method_id,
                "method_signature_hash": candidate_id,
                "candidate_method": {
                    "method_family": candidate_spec.method_family,
                    "method_id": candidate_spec.method_id,
                    "method_title": candidate_spec.method_title,
                    "method_thesis": candidate_spec.method_thesis,
                    "feature_family": candidate_spec.feature_family,
                },
                "diagnostics": {"candidate_effective_config": plan},
                "comparison": {
                    "challenger_top5_ret20_mean": 0.09 + index * 0.01 + cand_index * 0.002 + (random_seed % 3) * 0.001,
                    "challenger_top10_ret20_mean": 0.07 + index * 0.01 + cand_index * 0.002 + (random_seed % 3) * 0.001,
                    "challenger_monthly_capture_mean": 0.16,
                    "challenger_worst_regime_ret20_mean": 0.04,
                    "challenger_dd": 0.02 + index * 0.005,
                    "challenger_turnover": 0.09,
                    "challenger_liquidity_fail_rate": 0.01,
                    "changed_top5_members_count": 1 if cand_index > 0 else 0,
                    "changed_top10_members_count": 1,
                    "changed_rank_count": 1,
                },
                "selection_compare": {
                    "challenger_top5_ret20_mean": 0.09 + index * 0.01 + cand_index * 0.002 + (random_seed % 3) * 0.001,
                    "challenger_top10_ret20_mean": 0.07 + index * 0.01 + cand_index * 0.002 + (random_seed % 3) * 0.001,
                    "changed_top5_members_count": 1 if cand_index > 0 else 0,
                    "changed_top10_members_count": 1,
                    "changed_rank_count": 1,
                    "top5_boundary_score_gap": 0.01 if cand_index > 0 else 0.0,
                    "top10_boundary_score_gap": 0.01,
                    "selection_divergence_reason": "branchable" if cand_index > 0 else "rank_shuffle_only",
                },
                "evaluation_summary": {
                    "sample_count": 16 + index + cand_index,
                    "regime_tag": plan.get("regime_gate", "unknown"),
                },
                "candidate_local_decision": "keep" if index == 0 and cand_index == 0 else "hold",
                "promote_ready": index == 0 and cand_index == 0,
                "fallback_status": "authoritative",
                "artifact_detail_level": "authoritative",
                "ret20_source_mode": runner.tradex.TRADEX_RET20_SOURCE_MODE_PRECOMPUTED,
                "ret20_source_mode_reason": "",
                "same_condition_contract": {"contract_hash": f"contract-{candidate_spec.method_family}"},
                "meaningful_topk_branching_possible": True,
                "top_k": int(plan.get("top_k") or 5),
                "effective_universe_count": 20,
                "candidate_in_scope_before_build_count": 18,
                "candidate_in_scope_after_build_count": 16,
                "candidate_removed_by_scope_boundary_count": 2,
                "scope_filter_applied_stage": "final",
                "key_normalization_mode": "normalized",
                "long_horizon_regime_score": 0.2,
                "recent_adaptation_score": 0.1,
            }
            candidate_results.append(candidate_result)
        family_result = {
            "family_id": f"family-{family_spec.method_family}",
            "method_family": family_spec.method_family,
            "family_title": family_spec.family_title,
            "family_thesis": family_spec.family_thesis,
            "candidate_count": len(family_spec.candidates),
            "candidate_order": [spec.method_id for spec in family_spec.candidates],
            "compare_path": str(compare_path),
            "compare": {"candidate_results": candidate_results},
            "candidate_results": candidate_results,
            "best_candidate": max(candidate_results, key=lambda item: (float(item["comparison"]["challenger_top5_ret20_mean"]), int(item["evaluation_summary"]["sample_count"]))),
            "promote_ready": any(bool(item["promote_ready"]) for item in candidate_results),
            "promote_reasons": [{"code": "test"}],
            "best_method_title": max(candidate_results, key=lambda item: (float(item["comparison"]["challenger_top5_ret20_mean"]), int(item["evaluation_summary"]["sample_count"])))["candidate_method"]["method_title"],
            "best_method_thesis": max(candidate_results, key=lambda item: (float(item["comparison"]["challenger_top5_ret20_mean"]), int(item["evaluation_summary"]["sample_count"])))["candidate_method"]["method_thesis"],
        }
        family_results.append(family_result)
        family_summary.append(
            {
                "method_family": family_spec.method_family,
                "population_kind": family_spec.candidates[0].population_kind,
                "family_id": family_result["family_id"],
                "family_title": family_result["family_title"],
                "family_thesis": family_result["family_thesis"],
                "compare_path": str(compare_path),
                "family_leaderboard_path": str(leaderboard_path),
                "candidate_count": len(family_spec.candidates),
                "candidate_order": [spec.method_id for spec in family_spec.candidates],
                "decision": family_result["best_candidate"]["candidate_local_decision"],
                "sample_count": family_result["best_candidate"]["evaluation_summary"]["sample_count"],
                "best_candidate_method_id": family_result["best_candidate"]["candidate_method"]["method_id"],
                "best_candidate_method_title": family_result["best_candidate"]["candidate_method"]["method_title"],
                "best_candidate_method_thesis": family_result["best_candidate"]["candidate_method"]["method_thesis"],
                "best_candidate_feature_family": family_result["best_candidate"]["candidate_method"]["feature_family"],
                "best_candidate_promote_ready": family_result["best_candidate"]["promote_ready"],
                "best_candidate_signal_bias": family_result["best_candidate"]["diagnostics"]["candidate_effective_config"].get("signal_bias"),
                "best_candidate_regime_tag": family_result["best_candidate"]["evaluation_summary"]["regime_tag"],
                "best_candidate_method_signature_hash": family_result["best_candidate"]["method_signature_hash"],
                "same_condition_contract_hash": family_result["best_candidate"]["same_condition_contract"]["contract_hash"],
                "latest_fallback_status": "authoritative",
                "ret20_source_mode": runner.tradex.TRADEX_RET20_SOURCE_MODE_PRECOMPUTED,
                "ret20_source_mode_reason": "",
                "artifact_detail_level": "authoritative",
                "insufficient_samples": False,
                "topk_branching_block_reason": "",
                "selection_divergence_reason": "branchable",
                "top_k": int(family_result["best_candidate"]["top_k"] or 5),
                "effective_universe_count": 20,
                "candidate_in_scope_before_build_count": 18,
                "candidate_in_scope_after_build_count": 16,
                "candidate_removed_by_scope_boundary_count": 2,
                "scope_filter_applied_stage": "final",
                "key_normalization_mode": "normalized",
                "meaningful_topk_branching_possible": True,
                "avg_top5_ret20_mean_delta": family_result["best_candidate"]["comparison"]["challenger_top5_ret20_mean"],
                "avg_top10_ret20_mean_delta": family_result["best_candidate"]["comparison"]["challenger_top10_ret20_mean"],
                "avg_monthly_capture_delta": family_result["best_candidate"]["comparison"]["challenger_monthly_capture_mean"],
                "avg_worst_regime_delta": family_result["best_candidate"]["comparison"]["challenger_worst_regime_ret20_mean"],
                "avg_dd_delta": family_result["best_candidate"]["comparison"]["challenger_dd"],
                "avg_turnover_delta": family_result["best_candidate"]["comparison"]["challenger_turnover"],
                "avg_liquidity_fail_delta": family_result["best_candidate"]["comparison"]["challenger_liquidity_fail_rate"],
                "avg_changed_top5_members_count": family_result["best_candidate"]["comparison"]["changed_top5_members_count"],
                "avg_changed_top10_members_count": family_result["best_candidate"]["comparison"]["changed_top10_members_count"],
                "avg_changed_rank_count": family_result["best_candidate"]["comparison"]["changed_rank_count"],
                "avg_top5_boundary_score_gap": 0.01,
                "avg_top10_boundary_score_gap": 0.01,
                "selection_divergence_reason": "branchable",
                "sample_validity": True,
                "score": family_result["best_candidate"]["comparison"]["challenger_top5_ret20_mean"] * 100.0,
            }
        )

    leaderboard = {"family_summary": family_summary}
    leaderboard_path.write_text(json.dumps(leaderboard, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return {
        "status": "complete",
        "session_id": session_id,
        "session_scope_id": session_id,
        "random_seed": random_seed,
        "manifest_hash": f"manifest-{session_id}",
        "family_results": family_results,
        "best_result": family_results[0]["best_candidate"] if family_results else {},
        "eval_window_mode": "standard",
        "ret20_source_mode": runner.tradex.TRADEX_RET20_SOURCE_MODE_PRECOMPUTED,
    }


def test_tradex_scenario_search_reuses_seed_memory_and_writes_artifacts(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(tmp_path / "tradex-root"))
    monkeypatch.setattr(runner, "_scenario_seed_memory_from_sessions", _seed_memory)
    monkeypatch.setattr(runner, "run_tradex_research_session", _fake_research_session)

    result = runner.run_tradex_scenario_search(
        search_id="scenario-search",
        generations=2,
        random_seed=7,
        universe_size=20,
        max_candidates_per_family=2,
        max_parents_per_population=2,
    )

    assert result["status"] == "complete"
    assert result["generation_count"] >= 1
    assert result["seed_memory"]["record_count"] == 3
    assert {row["population_kind"] for row in result["seed_memory"]["records"]} == {
        "long_expert_population",
        "short_expert_population",
        "regime_selector_population",
    }
    assert result["candidate_population"]["populations"]
    assert {row["population_kind"] for row in result["candidate_population"]["populations"]} == {
        "long_expert_population",
        "regime_selector_population",
        "short_expert_population",
    }
    assert result["lineage"]["lineage_rows"]
    assert any(row["parent_candidate_ids"] for row in result["lineage"]["lineage_rows"])
    assert result["keep_drop_hold_rollup"]["overview"]["keep_count"] >= 1
    assert result["keep_drop_hold_rollup"]["overview"]["hold_count"] >= 1
    assert result["keep_drop_hold_rollup"]["overview"]["drop_count"] >= 1
    assert result["regime_selector_eval"]["overview"]["candidate_count"] >= 1
    assert result["champion_vs_challenger_summary"]["comparison"]["score_delta"] >= 0.0
    assert result["practical_topk_eval"]["status"] == "complete"
    assert len(result["practical_topk_eval"]["candidates"]) >= 4

    for key in (
        "memory_path",
        "candidate_population_path",
        "generation_summary_path",
        "lineage_path",
        "keep_drop_hold_rollup_path",
        "regime_selector_eval_path",
        "champion_vs_challenger_summary_path",
        "practical_topk_eval_path",
    ):
        assert Path(result[key]).exists()

    memory = json.loads(Path(result["memory_path"]).read_text(encoding="utf-8"))
    assert memory["schema_version"] == runner.SCENARIO_SEARCH_MEMORY_SCHEMA_VERSION
    assert memory["record_count"] == result["memory"]["record_count"]


def test_tradex_scenario_search_uses_branchable_top_k() -> None:
    spec = runner.ScenarioSpec(
        direction="long",
        entry_family="long_breakout",
        filter_family="boundary_feature",
        ranking_target="topk_uplift",
        holding_days=20,
        exit_family="time_stop",
        stop_type="trailing",
        regime_gate="trend_long",
        liquidity_gate="balanced",
        weight_profile="boundary_heavy",
        population_kind="long_expert_population",
    )

    plan = runner._scenario_plan_overrides(spec, search_id="scenario-search", parent_candidate_ids=[])
    champion_plan = runner._build_champion_plan(top_k=int(plan["top_k"]))

    assert plan["top_k"] == runner.SCENARIO_SEARCH_TOP_K
    assert champion_plan["top_k"] == runner.SCENARIO_SEARCH_TOP_K


def test_tradex_scenario_search_branching_threshold_tracks_top_k() -> None:
    branchable_record = {
        "branching_metrics": {"top_k": 2, "effective_universe_count": 4},
        "same_condition_contract": {"top_k": 2, "universe": ["a", "b", "c", "d"]},
    }
    blocked_record = {
        "branching_metrics": {"top_k": 5, "effective_universe_count": 4},
        "same_condition_contract": {"top_k": 5, "universe": ["a", "b", "c", "d"]},
    }

    assert runner._scenario_universe_too_small_for_branching(branchable_record) is False
    assert runner._scenario_universe_too_small_for_branching(blocked_record) is True


def test_tradex_scenario_wide_topk_family_specs_use_distinct_family_ids() -> None:
    champion_record = _seed_record(
        candidate_id="champion-seed",
        population_kind="short_expert_population",
        method_family="bad-pick-prune",
        feature_family="bad_pick_removal",
        decision="hold",
        score=61.0,
    )
    challenger_record = _seed_record(
        candidate_id="challenger-seed",
        population_kind="long_expert_population",
        method_family="long_breakout",
        feature_family="boundary_feature",
        decision="keep",
        score=71.0,
    )

    family_specs = runner._scenario_wide_topk_family_specs(
        "scenario-search",
        champion_record,
        challenger_record,
        top_k=5,
    )
    family_ids = [runner._session_family_id("scenario-search", spec.method_family) for spec in family_specs]

    assert len(family_specs) == 2
    assert len(set(family_ids)) == len(family_ids)
    assert len({spec.method_family for spec in family_specs}) == len(family_specs)
    assert all(len(spec.candidates) == 3 for spec in family_specs)


def test_tradex_scenario_search_preserves_parent_lineage_from_index() -> None:
    candidate_result = {
        "plan_id": "cand-1",
        "candidate_method": {
            "method_family": "long_breakout",
            "method_id": "cand-1",
            "method_title": "Candidate 1",
            "method_thesis": "Candidate 1 thesis",
            "feature_family": "boundary_feature",
        },
        "diagnostics": {
            "candidate_effective_config": {
                "population_kind": "long_expert_population",
                "ranking_target": "topk_uplift",
                "holding_days": 20,
                "weight_profile": "boundary_heavy",
                "entry_family": "long_breakout",
                "filter_family": "boundary_feature",
                "exit_family": "time_stop",
                "stop_type": "trailing",
                "regime_gate": "trend_long",
                "liquidity_gate": "balanced",
            }
        },
        "candidate_local_decision": "keep",
        "promote_ready": True,
        "fallback_status": "authoritative",
        "artifact_detail_level": "authoritative",
        "ret20_source_mode": runner.tradex.TRADEX_RET20_SOURCE_MODE_PRECOMPUTED,
        "ret20_source_mode_reason": "",
        "same_condition_contract": {"contract_hash": "contract-a", "universe": ["a", "b", "c"], "top_k": 2},
        "meaningful_topk_branching_possible": True,
        "top_k": 2,
        "effective_universe_count": 3,
        "candidate_in_scope_before_build_count": 1,
        "candidate_in_scope_after_build_count": 1,
        "candidate_removed_by_scope_boundary_count": 0,
        "scope_filter_applied_stage": "final",
        "key_normalization_mode": "normalized",
        "long_horizon_regime_score": 0.2,
        "recent_adaptation_score": 0.1,
        "comparison": {"challenger_top5_ret20_mean": 0.1, "challenger_top10_ret20_mean": 0.08},
        "evaluation_summary": {"sample_count": 12, "regime_tag": "trend_long"},
    }
    family_result = {
        "family_id": "family-a",
        "family_title": "Family A",
        "family_thesis": "Family A thesis",
        "compare_path": "compare.json",
        "family_leaderboard_path": "family_leaderboard.json",
    }
    records = runner._scenario_records_from_session(
        search_id="search-a",
        session_id="session-a",
        session_state={"family_results": [{"method_family": "long_breakout", "compare": {"candidate_results": [candidate_result]}}]},
        family_leaderboard={"family_summary": [{"method_family": "long_breakout"}]},
        generation_index=0,
        source_kind="evaluated_generation",
        mutation_parent_ids_by_method_id={"cand-1": ("parent-a", "parent-b")},
    )

    assert records[0]["parent_candidate_ids"] == ["parent-a", "parent-b"]


def test_tradex_scenario_candidate_spec_keeps_parent_ids() -> None:
    spec = runner.ScenarioSpec(
        direction="long",
        entry_family="long_breakout",
        filter_family="boundary_feature",
        ranking_target="topk_uplift",
        holding_days=20,
        exit_family="time_stop",
        stop_type="trailing",
        regime_gate="trend_long",
        liquidity_gate="balanced",
        weight_profile="boundary_heavy",
        mutation_parent_ids=("parent-a", "parent-b"),
        generation_index=1,
        population_kind="long_expert_population",
    )

    candidate = runner._scenario_candidate_spec("search-a", spec)

    assert candidate.mutation_parent_ids == ("parent-a", "parent-b")
    assert candidate.generation_index == 1


def test_tradex_scenario_spec_round_trips_from_scenario_definition() -> None:
    record = {
        "population_kind": "long_expert_population",
        "scenario_definition": {
            "direction": "long",
            "entry_family": "long_pullback",
            "filter_family": "boundary_feature",
            "ranking_target": "stability",
            "holding_days": 40,
            "exit_family": "regime_switch",
            "stop_type": "adaptive",
            "regime_gate": "top_warning",
            "liquidity_gate": "tight",
            "weight_profile": "boundary_heavy",
            "signal_bias": "buy",
            "minimum_confidence": 0.82,
            "minimum_ready_rate": 0.74,
            "bad_pick_penalty_scale": 1.5,
            "playbook_up_score_bonus": 0.08,
            "playbook_down_score_bonus": 0.01,
            "mutation_parent_ids": ["parent-x"],
            "generation_index": 4,
            "population_kind": "long_expert_population",
        },
    }

    spec = runner._scenario_spec_from_record(record, generation_index=0)

    assert spec.entry_family == "long_pullback"
    assert spec.filter_family == "boundary_feature"
    assert spec.ranking_target == "stability"
    assert spec.holding_days == 40
    assert spec.exit_family == "regime_switch"
    assert spec.stop_type == "adaptive"
    assert spec.regime_gate == "top_warning"
    assert spec.liquidity_gate == "tight"
    assert spec.weight_profile == "boundary_heavy"
    assert spec.signal_bias == "buy"
    assert spec.minimum_confidence == 0.82
    assert spec.minimum_ready_rate == 0.74
    assert spec.bad_pick_penalty_scale == 1.5
    assert spec.playbook_up_score_bonus == 0.08
    assert spec.playbook_down_score_bonus == 0.01
    assert spec.mutation_parent_ids == ("parent-x",)
    assert spec.generation_index == 4


def test_tradex_scenario_plan_overrides_keep_selection_sensitive_fields() -> None:
    spec = runner.ScenarioSpec(
        direction="short",
        entry_family="short_downtrend",
        filter_family="bad_pick_removal",
        ranking_target="bad_pick_removal",
        holding_days=10,
        exit_family="risk_exit",
        stop_type="hard",
        regime_gate="trend_short",
        liquidity_gate="tight",
        weight_profile="bad_pick_heavy",
        signal_bias="balanced",
        minimum_confidence=0.47,
        minimum_ready_rate=0.33,
        bad_pick_penalty_scale=3.5,
        playbook_up_score_bonus=0.01,
        playbook_down_score_bonus=0.05,
        generation_index=2,
        population_kind="short_expert_population",
    )

    plan = runner._scenario_plan_overrides(spec, search_id="search-a", parent_candidate_ids=["parent-a"])

    assert plan["signal_bias"] == "balanced"
    assert plan["minimum_confidence"] == 0.47
    assert plan["minimum_ready_rate"] == 0.33
    assert plan["bad_pick_penalty_scale"] == 3.5
    assert plan["playbook_up_score_bonus"] == 0.01
    assert plan["playbook_down_score_bonus"] == 0.05


def test_tradex_scenario_parent_priority_prefers_branchy_records() -> None:
    branchy = {
        "decision": "keep",
        "candidate_id": "branchy",
        "score": 10.0,
        "evaluation_metrics": {
            "changed_rank_count": 2,
            "changed_top5_members_count": 1,
            "changed_top10_members_count": 1,
            "top5_boundary_score_gap": 0.01,
            "top10_boundary_score_gap": 0.01,
        },
        "branching_metrics": {"meaningful_topk_branching_possible": True},
    }
    flat = {
        "decision": "keep",
        "candidate_id": "flat",
        "score": 20.0,
        "evaluation_metrics": {
            "changed_rank_count": 0,
            "changed_top5_members_count": 0,
            "changed_top10_members_count": 0,
            "top5_boundary_score_gap": 0.0,
            "top10_boundary_score_gap": 0.0,
        },
        "branching_metrics": {"meaningful_topk_branching_possible": True},
    }

    assert runner._scenario_parent_priority(branchy) < runner._scenario_parent_priority(flat)


def test_tradex_scenario_search_cli_smoke(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(tmp_path / "tradex-root"))
    monkeypatch.setattr(runner, "_scenario_seed_memory_from_sessions", _seed_memory)
    monkeypatch.setattr(runner, "run_tradex_research_session", _fake_research_session)

    argv = [
        "tradex_research_runner",
        "--session-id",
        "scenario-search-cli",
        "--random-seed",
        "7",
        "--scenario-search",
        "--scenario-search-generations",
        "1",
        "--scenario-search-max-parents-per-population",
        "2",
        "--scenario-search-scope-id",
        "scope-a",
    ]
    monkeypatch.setattr(sys, "argv", argv)

    assert runner.main() == 0
