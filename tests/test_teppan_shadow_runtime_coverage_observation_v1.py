from __future__ import annotations

import json
from pathlib import Path

from scripts import teppan_shadow_runtime_coverage_observation_v1 as mod


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _plan_root(tmp_path: Path) -> Path:
    root = tmp_path / "plan"
    _write_json(
        root / "shadow_integration_plan.json",
        {
            "decision": "approve_shadow_integration_implementation",
            "candidate_id": "teppan_ranking_branching_probe_v1",
            "active_runtime_ranking_change_allowed": False,
            "runtime_duckdb_write_allowed": False,
            "production_publish_registration_allowed": False,
            "frontend_or_backend_ui_change_allowed": False,
            "static_soft_boost_value": 0.04,
        },
    )
    _write_json(
        root / "feature_materialization_plan.json",
        {
            "decision": "ready",
            "materialized_features": [{"feature": "teppan_pattern_match"}, {"feature": "teppan_guard_pass"}],
        },
    )
    _write_json(
        root / "rank_storage_contract.json",
        {"record_shape": {"original_rank_is_recoverable": True, "adjusted_rank_is_separate": True}},
    )
    _write_json(root / "rollback_plan.json", {"runtime_db_rollback_required": False})
    _write_json(root / "_ARTIFACT_COMPLETE.json", {"complete": True})
    return root


def _active_rows() -> list[dict[str, object]]:
    rows = []
    for rank in range(1, 21):
        rows.append(
            {
                "anchor_date": "2026-05-13",
                "symbol": f"10{rank:02d}",
                "name": f"name-{rank}",
                "side": "long",
                "champion_rank": rank,
                "champion_score": 1.0 - (rank * 0.01),
                "display_score": 1.0 - (rank * 0.01),
                "signal_state": "wait",
                "entry_qualified": False,
                "setup_type": "reject",
                "status": "active",
            }
        )
    return rows


def _tags() -> list[dict[str, object]]:
    return [
        {
            "symbol": "1006",
            "anchor_ymd": 20260513,
            "teppan_pattern_match": True,
            "teppan_guard_pass": True,
            "best_pattern_family": "family",
            "best_pattern_key": "pass",
            "best_pattern_decision": "high_return_candidate",
            "best_teppan_score": 0.8,
            "matched_pattern_count": 2,
            "guard_block_reason": "",
        },
        {
            "symbol": "1012",
            "anchor_ymd": 20260513,
            "teppan_pattern_match": True,
            "teppan_guard_pass": False,
            "best_pattern_family": "family",
            "best_pattern_key": "blocked",
            "best_pattern_decision": "high_return_candidate",
            "best_teppan_score": 0.7,
            "matched_pattern_count": 1,
            "guard_block_reason": "composite_downside_risk",
        },
    ]


def test_coverage_observation_reports_topk_coverage_and_promotion(tmp_path: Path) -> None:
    payload = mod.run_teppan_shadow_runtime_coverage_observation_v1(
        plan_root=_plan_root(tmp_path),
        output_parent=tmp_path / "out",
        run_id="coverage",
        runtime_status={"selected_runtime_db_path": str(tmp_path / "stocks.duckdb")},
        rankings_freshness={"freshness_state": "fresh", "stale": False},
        active_rows=_active_rows(),
        teppan_tags=_tags(),
    )

    result = payload["coverage_observation_result"]
    assert result["decision"] == "observe_shadow_promotion_potential"
    assert result["coverage_summary"]["top20"]["teppan_pattern_match_count"] == 2
    assert result["coverage_summary"]["top20"]["loss_guard_blocked_count"] == 1
    assert result["coverage_summary"]["top20"]["boosted_candidate_count"] == 1
    assert result["boost_promotion_potential"]["would_enter_top5"][0]["symbol"] == "1006"
    assert result["topk_comparison"]["top5"]["added_by_shadow"][0]["symbol"] == "1006"
    assert result["topk_comparison"]["top5"]["removed_from_active"][0]["symbol"] == "1005"
    assert result["no_mutation_audit"]["no_mutation_pass"] is True
    assert payload["artifact_complete"]["complete"] is True
    for name in mod.REQUIRED_OUTPUTS:
        assert (Path(payload["output_root"]) / name).exists(), name


def test_coverage_observation_holds_when_no_live_teppan_coverage(tmp_path: Path) -> None:
    payload = mod.run_teppan_shadow_runtime_coverage_observation_v1(
        plan_root=_plan_root(tmp_path),
        output_parent=tmp_path / "out",
        run_id="coverage",
        runtime_status={"selected_runtime_db_path": str(tmp_path / "stocks.duckdb")},
        rankings_freshness={"freshness_state": "fresh", "stale": False},
        active_rows=_active_rows(),
        teppan_tags=[],
    )

    result = payload["coverage_observation_result"]
    assert result["decision"] == "hold_no_live_teppan_coverage"
    assert result["coverage_summary"]["all_observed"]["teppan_pattern_match_count"] == 0
    assert result["boost_promotion_potential"]["boosted_candidate_count"] == 0
