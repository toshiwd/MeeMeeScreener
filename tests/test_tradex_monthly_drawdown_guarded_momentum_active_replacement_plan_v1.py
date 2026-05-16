from __future__ import annotations

import argparse
import json
from pathlib import Path

from scripts import tradex_monthly_drawdown_guarded_momentum_active_replacement_plan_v1 as mod


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _make_source_roots(tmp_path: Path, *, entry_decision: str = "entry_timing_confirmation_hold") -> dict[str, Path]:
    top5 = tmp_path / "top5"
    pretest = tmp_path / "pretest"
    manual = tmp_path / "manual"
    entry = tmp_path / "entry"

    _write_json(
        top5 / "research_decision.json",
        {
            "authoritative_research_decision": "monthly_drawdown_guarded_momentum_top5_gate_keep_candidate",
            "decision": "keep_candidate",
            "top5_candidate_pool_clearly_better_than_baseline": True,
            "best_variant_id": "monthly_drawdown_guarded_momentum_m+0.02_l-0.02_h-0.02_md-0.005",
        },
    )
    _write_json(
        top5 / "gate_pass_fail_report.json",
        {
            "mandatory_gates": [
                "top5_avg_ret20_improved",
                "top5_big_winner_capture_improved",
                "top5_future_top10_capture_improved",
            ],
            "best_variant_gate_results": {
                "top5_avg_ret20_improved": True,
                "top5_big_winner_capture_improved": True,
                "top5_future_top10_capture_improved": True,
            },
        },
    )
    _write_json(
        top5 / "strict_gate_leaderboard.json",
        {
            "best_variant": {
                "variant_id": "monthly_drawdown_guarded_momentum_m+0.02_l-0.02_h-0.02_md-0.005",
                "spec": {"momentum_weight": 0.02},
            }
        },
    )
    _write_json(
        pretest / "research_decision.json",
        {"authoritative_research_decision": "starter_entry_pretest_keep", "decision": "keep_candidate"},
    )
    _write_json(
        pretest / "starter_entry_leaderboard.json",
        {
            "all_pretest_gates_pass": True,
            "starter_entry_variant": {
                "variant_id": "monthly_drawdown_guarded_momentum_starter_entry",
                "deltas_vs_baseline": {
                    "top5_avg_ret20_delta_vs_baseline": 0.001,
                    "top5_big_winner_capture_delta_vs_baseline": 0.002,
                    "top5_future_top10_capture_delta_vs_baseline": 0.003,
                    "top5_severe_loss_rate_delta_vs_baseline": -0.001,
                    "top5_bad_pick_count_delta_vs_baseline": -3,
                },
                "metrics": {"top5_avg_ret20": 0.02},
                "guardrail": {"top3_severe_loss_rate_delta_vs_baseline": -0.001},
                "candidate_source_mix": {"candidate_source_mix": {"max_family_share": 0.52}},
            },
        },
    )
    _write_json(
        manual / "research_decision.json",
        {"authoritative_research_decision": "manual_review_pack_ready", "decision": "keep_candidate"},
    )
    _write_json(
        entry / "research_decision.json",
        {"authoritative_research_decision": entry_decision, "decision": "hold" if entry_decision.endswith("_hold") else "keep_candidate"},
    )
    _write_json(
        entry / "confirmed_candidate_metrics.json",
        {
            "all_starter_candidates": {"severe_loss_rate20": 0.216},
            "entry_timing_confirmed_candidates": {"severe_loss_rate20": 0.227},
            "confirmed_rate": 0.71,
        },
    )
    return {"top5": top5, "pretest": pretest, "manual": manual, "entry": entry}


def _run(tmp_path: Path, *, entry_decision: str = "entry_timing_confirmation_hold") -> Path:
    roots = _make_source_roots(tmp_path, entry_decision=entry_decision)
    args = argparse.Namespace(
        top5_gate_root=roots["top5"],
        starter_pretest_root=roots["pretest"],
        manual_review_pack_root=roots["manual"],
        entry_timing_root=roots["entry"],
        output_parent=tmp_path / "out",
        run_id="active-replacement-plan-run",
    )
    return mod.run(args)


def test_active_replacement_plan_requires_live_dry_run_when_entry_timing_is_hold(tmp_path: Path) -> None:
    output = _run(tmp_path)

    decision = _read_json(output / "research_decision.json")
    plan = _read_json(output / "active_replacement_plan.json")
    blocked = _read_json(output / "blocked_or_approval_report.json")
    complete = _read_json(output / "_ARTIFACT_COMPLETE.json")

    assert decision["authoritative_research_decision"] == "active_replacement_plan_ready_for_live_dry_run"
    assert decision["replacement_direction_approved"] is True
    assert decision["immediate_active_replacement_allowed"] is False
    assert plan["live_dry_run_required_before_activation"] is True
    assert "entry_timing_confirmation_hold" in blocked["blocking_items"]
    assert complete["complete"] is True
    assert complete["artifacts"]["_ARTIFACT_COMPLETE.json"]["exists"] is True


def test_active_replacement_plan_no_mutation_and_contract_files_are_reported(tmp_path: Path) -> None:
    output = _run(tmp_path)

    mutation = _read_json(output / "no_mutation_audit.json")
    impact = _read_json(output / "runtime_contract_impact_report.json")
    changes = _read_json(output / "implementation_change_list.json")

    assert mutation["no_mutation_pass"] is True
    assert mutation["production_ranking_changed"] is False
    assert mutation["runtime_duckdb_written"] is False
    assert impact["runtime_contract_files_exist"] is True
    assert changes["this_run_changes_runtime_code"] is False


def test_active_replacement_plan_allows_next_implementation_only_when_entry_timing_ready(tmp_path: Path) -> None:
    output = _run(tmp_path, entry_decision="entry_timing_confirmation_keep")

    decision = _read_json(output / "research_decision.json")
    next_axis = _read_json(output / "next_axis_recommendation.json")

    assert decision["authoritative_research_decision"] == "active_replacement_implementation_plan_ready"
    assert decision["immediate_active_replacement_allowed"] is True
    assert next_axis["next"] == "monthly_drawdown_guarded_momentum_active_replacement_implementation_v1"
