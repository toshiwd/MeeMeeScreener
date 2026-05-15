from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.backend.services import tradex_readonly_reflection_service as service
from scripts import tradex_sell_failed_followthrough_family_freeze_v1 as freeze
from scripts import tradex_sell_failed_followthrough_meemee_readonly_reflection_v1 as reflection


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _manifest() -> dict:
    return {
        "candidate_name": service.CLEAN_CANDIDATE,
        "candidate_version": "v1",
        "source_run_root": "G:/Tradex/source",
        "source_decision_artifact": "G:/Tradex/source/no_lookahead_clean_decision.json",
        "source_compare_artifact": "G:/Tradex/source/no_lookahead_clean_compare.json",
        "source_contract_artifact": "G:/Tradex/source/no_lookahead_clean_contract.json",
        "reflectability_decision_artifact": "G:/Tradex/source/meemee_reflectability_decision.json",
        "decision": "meemee_reflectable_candidate",
        "side": "sell",
        "display_level": "read_only_research_candidate",
        "no_lookahead_pass": True,
        "production_ranking_changed": False,
        "active_ranking_changed": False,
        "publish_run": False,
        "old_candidate_status": "lookahead_contaminated_excluded",
        "allowed_meemee_usage": ["show not active ranking", "show research candidate"],
        "forbidden_meemee_usage": [
            "do not use for production ranking",
            "do not change active ranking",
            f"do not use old lookahead-contaminated candidate {service.OLD_LOOKAHEAD_CANDIDATE}",
        ],
        "key_metrics": {
            "mean_ret20_delta": 0.0103,
            "hit_rate_delta": 0.1429,
            "added_bad_pick": 6,
            "bad_pick_removal": 8,
        },
        "remaining_risks": ["added_bad_pick remains visible"],
    }


def test_readonly_reflection_manifest_parses_clean_candidate_only(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _write_json(run_dir / service.MANIFEST_NAME, _manifest())

    snapshot = service.build_readonly_reflection_snapshot(run_dir, strict=True)

    assert snapshot["available"] is True
    item = snapshot["items"][0]
    assert item["candidate_name"] == service.CLEAN_CANDIDATE
    assert service.OLD_LOOKAHEAD_CANDIDATE not in item["candidate_name"]
    assert item["old_candidate_status"] == "lookahead_contaminated_excluded"
    assert item["display_level"] == "read_only_research_candidate"
    assert item["visible_warning"] == "This is not active ranking and not a live sell signal."
    assert item["production_ranking_changed"] is False
    assert item["active_ranking_changed"] is False
    assert item["publish_run"] is False


def test_readonly_reflection_missing_manifest_fails_loudly(tmp_path: Path) -> None:
    with pytest.raises(service.ReadonlyReflectionError):
        service.load_readonly_reflection_manifest(tmp_path / "missing")


def test_readonly_reflection_drop_manifest_supersedes_positive_candidate(tmp_path: Path) -> None:
    run_dir = tmp_path / "freeze"
    _write_json(
        run_dir / service.DROP_MANIFEST_NAME,
        {
            "candidate_name": service.CLEAN_CANDIDATE,
            "candidate_version": "v1",
            "family_status": "dropped",
            "decision": "drop_after_multiyear_replay",
            "side": "sell",
            "display_level": "read_only_research_candidate",
            "meemee_readonly_status": "dropped_after_multiyear_replay",
            "shadow_trade_candidate": False,
            "no_lookahead_pass": True,
            "production_ranking_changed": False,
            "active_ranking_changed": False,
            "active_champion_changed": False,
            "publish_run": False,
            "live_sell_signal_added": False,
            "old_candidate_status": "lookahead_contaminated_excluded",
            "source_run_root": "G:/Tradex/drop",
            "supersedes_readonly_reflection_root": "G:/Tradex/readonly",
            "authoritative_drop_root": "G:/Tradex/drop",
            "supersession_reason": "multi_year_portfolio_replay_failed",
            "drop_reason": "all_fixed_exit_variants_failed_capital_curve_gates",
            "visible_warning": "Not shadow trade eligible. Not active ranking. Not a live sell signal.",
            "allowed_meemee_usage": ["show not active ranking", "show dropped after multi-year portfolio replay"],
            "forbidden_meemee_usage": [
                "do not use for production ranking",
                "do not change active ranking",
                f"do not use old lookahead-contaminated candidate {service.OLD_LOOKAHEAD_CANDIDATE}",
            ],
            "key_metrics": {"shadow_trade_candidate": False},
            "remaining_risks": ["previous positive state superseded"],
            "full_period_result_summary": [{"exit_variant": "fixed_horizon_20d_exit", "total_return": -0.4, "max_drawdown": -0.46}],
            "max_drawdown_summary": [{"exit_variant": "fixed_horizon_20d_exit", "max_drawdown": -0.46}],
            "added_bad_pick_impact": {"fixed_horizon_20d_added_bad_pick_pnl": -1490242.0},
        },
    )

    snapshot = service.build_readonly_reflection_snapshot(run_dir, strict=True)

    item = snapshot["items"][0]
    assert item["decision"] == "drop_after_multiyear_replay"
    assert item["family_status"] == "dropped"
    assert item["meemee_readonly_status"] == "dropped_after_multiyear_replay"
    assert item["shadow_trade_candidate"] is False
    assert item["visible_warning"].startswith("Not shadow trade eligible")
    assert item["supersession_reason"] == "multi_year_portfolio_replay_failed"


def test_readonly_reflection_script_writes_ready_artifacts(tmp_path: Path) -> None:
    source = tmp_path / "source"
    _write_json(
        source / "meemee_reflectability_decision.json",
        {
            "decision": "meemee_reflectable_candidate",
            "no_lookahead_pass": True,
            "metrics": {
                "added_severe_loser": 0,
                "added_bad_pick": 6,
                "mean_ret20_delta": 0.010328334681733489,
                "severe_loser_rate_delta": 0.0,
            },
        },
    )
    _write_json(
        source / "no_lookahead_clean_compare.json",
        {
            "delta": {"hit_rate_delta": 0.1428571428571429, "bad_pick_removal_count": 8},
            "monthly_stability": {"positive_months": 5, "negative_months": 0},
        },
    )
    _write_json(
        source / "no_lookahead_clean_contract.json",
        {
            "fixed_evaluation_conditions": {"refill_liquidity20d_min": 1_000_000.0},
            "selection_input_contract": "selection_input_contract.json",
        },
    )
    _write_json(source / "no_lookahead_selector_guard.json", {"no_lookahead_pass": True})

    result = reflection.run(source_run_root=source, output_root=tmp_path / "out")

    assert result["decision"] == "meemee_readonly_reflection_ready"
    assert result["meemee_readonly_reflection_ready"] is True
    run_dir = Path(result["output_dir"])
    decision = json.loads((run_dir / "meemee_readonly_reflection_decision.json").read_text(encoding="utf-8"))
    guardrail = json.loads((run_dir / "guardrail_check.json").read_text(encoding="utf-8"))
    exclusion = json.loads((run_dir / "old_candidate_exclusion_check.json").read_text(encoding="utf-8"))
    ui_check = json.loads((run_dir / "ui_or_api_reflection_check.json").read_text(encoding="utf-8"))
    assert decision["production_ranking_changed"] is False
    assert decision["active_ranking_changed"] is False
    assert decision["publish_run"] is False
    assert guardrail["guardrail_pass"] is True
    assert exclusion["old_candidate_excluded"] is True
    assert ui_check["missing_artifact_fails_loudly"] is True


def test_family_freeze_script_writes_drop_status_artifacts(tmp_path: Path) -> None:
    drop_root = tmp_path / "drop"
    readonly_root = tmp_path / "readonly"
    readonly_root.mkdir()
    _write_json(
        drop_root / "final_shadow_trade_decision.json",
        {
            "decision": "drop_after_multiyear_replay",
            "shadow_trade_candidate": False,
            "blockers": ["no_fixed_exit_variant_survived"],
        },
    )
    _write_json(
        drop_root / "data_availability_report.json",
        {"actual_period": {"start_ymd": 20190101, "end_ymd": 20251231}},
    )
    _write_json(
        drop_root / "exit_variant_comparison.json",
        {
            "variants": [
                {
                    "exit_variant": "fixed_horizon_20d_exit",
                    "challenger": {
                        "total_return": -0.40036171463684067,
                        "max_drawdown": -0.4639479465754378,
                        "severe_loser_count": 34,
                        "bad_pick_count": 71,
                        "number_of_trades": 121,
                    },
                }
            ]
        },
    )
    _write_json(
        drop_root / "added_bad_pick_decomposition.json",
        {
            "multiyear_added_bad_pick_count": 76,
            "added_bad_pick_impact": {
                "fixed_horizon_20d_added_bad_pick_pnl": -1490242.0,
                "stop5_added_bad_pick_pnl": -1667389.92,
            },
        },
    )
    _write_json(drop_root / "severe_loser_audit.json", {"variant_counts": [{"exit_variant": "fixed", "severe_loser_count": 34}]})

    result = freeze.run(drop_root=drop_root, readonly_root=readonly_root, output_root=tmp_path / "out")

    assert result["decision"] == "family_frozen_drop_status_reflected"
    run_dir = Path(result["output_dir"])
    manifest = json.loads((run_dir / service.DROP_MANIFEST_NAME).read_text(encoding="utf-8"))
    complete = json.loads((run_dir / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    assert manifest["decision"] == "drop_after_multiyear_replay"
    assert manifest["shadow_trade_candidate"] is False
    assert manifest["meemee_readonly_status"] == "dropped_after_multiyear_replay"
    assert complete["readonly_snapshot_after_freeze"]["items"][0]["decision"] == "drop_after_multiyear_replay"
