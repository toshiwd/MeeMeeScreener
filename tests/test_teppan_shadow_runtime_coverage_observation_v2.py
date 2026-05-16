from __future__ import annotations

import json
from pathlib import Path

from scripts import teppan_shadow_runtime_coverage_observation_v2 as mod


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _plan_root(tmp_path: Path) -> Path:
    root = tmp_path / "plan"
    _write_json(
        root / "shadow_integration_plan.json",
        {
            "decision": "approve_shadow_integration_implementation",
            "source_candidate_id": "static_teppan_guarded_soft_boost_v1",
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
    _write_json(root / "rank_storage_contract.json", {"record_shape": {"original_rank_is_recoverable": True, "adjusted_rank_is_separate": True}})
    _write_json(root / "rollback_plan.json", {"runtime_db_rollback_required": False})
    _write_json(root / "_ARTIFACT_COMPLETE.json", {"complete": True})
    return root


def _materialization_fix_root(tmp_path: Path) -> Path:
    root = tmp_path / "materialization_fix"
    _write_json(root / "research_decision.json", {"decision": "live_safe_materialization_ready"})
    return root


def _active_rows() -> list[dict[str, object]]:
    rows = []
    for rank in range(1, 21):
        rows.append(
            {
                "anchor_date": "2026-05-13",
                "anchor_ymd": 20260513,
                "symbol": f"10{rank:02d}",
                "name": f"name-{rank}",
                "side": "long",
                "champion_rank": rank,
                "runtime_rank": rank,
                "champion_score": 1.0 - (rank * 0.01),
                "display_score": 1.0 - (rank * 0.01),
                "signal_state": "wait",
                "entry_qualified": False,
                "setup_type": "reject",
                "status": "active",
            }
        )
    return rows


def _materialized_rows() -> list[dict[str, object]]:
    rows = []
    for active in _active_rows():
        symbol = str(active["symbol"])
        pattern = symbol in {"1006", "1012"}
        guard = symbol == "1006"
        rows.append(
            {
                **active,
                "active_rank": active["champion_rank"],
                "teppan_pattern_match": pattern,
                "teppan_guard_pass": guard,
                "loss_guard_pass": guard or not pattern,
                "loss_guard_blocked": pattern and not guard,
                "guard_block_reason": "composite_downside_risk" if pattern and not guard else "" if pattern else "no_teppan_pattern_match",
                "matched_pattern_count": 1 if pattern else 0,
                "best_pattern_family": "family" if pattern else None,
                "best_pattern_key": "key" if pattern else None,
                "best_pattern_decision": "high_return_candidate" if pattern else None,
                "best_teppan_score": 0.8 if pattern else None,
                "future_label_inputs_used": False,
                "signal_features": {"daily_ma_stack": "daily_bull_stack_5_20_60"},
            }
        )
    return rows


def test_v2_reports_shadow_topk_diff_human_review_and_complete_artifacts(tmp_path: Path) -> None:
    payload = mod.run_teppan_shadow_runtime_coverage_observation_v2(
        plan_root=_plan_root(tmp_path),
        materialization_fix_root=_materialization_fix_root(tmp_path),
        output_parent=tmp_path / "out",
        run_id="coverage-v2",
        runtime_status={"selected_runtime_db_path": str(tmp_path / "stocks.duckdb")},
        rankings_freshness={"freshness_state": "fresh", "stale": False},
        active_rows=_active_rows(),
        materialized_rows=_materialized_rows(),
    )

    result = payload["coverage_observation_v2_result"]
    assert result["decision"] == "shadow_coverage_v2_pass"
    assert result["added_by_shadow_top5"][0]["symbol"] == "1006"
    assert result["removed_from_active_top5"][0]["symbol"] == "1005"
    assert result["coverage_summary"]["latest"]["top20"]["teppan_pattern_match_count"] == 2
    assert result["coverage_summary"]["latest"]["top20"]["loss_guard_blocked_count"] == 1
    assert result["human_review_candidate_list"][0]["symbol"] == "1006"
    assert result["materialization_readback"]["materialization_fix_decision"] == "live_safe_materialization_ready"
    assert result["no_mutation_audit"]["no_mutation_pass"] is True
    assert payload["artifact_complete"]["complete"] is True
    for name in mod.REQUIRED_OUTPUTS:
        assert (Path(payload["output_root"]) / name).exists(), name


def test_v2_holds_for_sparse_live_coverage_without_latest_topk_diff(tmp_path: Path) -> None:
    rows = _active_rows()
    materialized = _materialized_rows()
    for row in materialized:
        row["teppan_pattern_match"] = str(row["symbol"]) == "1020"
        row["teppan_guard_pass"] = str(row["symbol"]) == "1020"
        row["loss_guard_pass"] = True
        row["loss_guard_blocked"] = False

    payload = mod.run_teppan_shadow_runtime_coverage_observation_v2(
        plan_root=_plan_root(tmp_path),
        materialization_fix_root=_materialization_fix_root(tmp_path),
        output_parent=tmp_path / "out",
        run_id="coverage-v2",
        runtime_status={"selected_runtime_db_path": str(tmp_path / "stocks.duckdb")},
        rankings_freshness={"freshness_state": "fresh", "stale": False},
        active_rows=rows,
        materialized_rows=materialized,
    )

    result = payload["coverage_observation_v2_result"]
    assert result["decision"] == "hold_for_sparse_live_coverage"
    assert result["coverage_summary"]["recent"]["all_observed"]["teppan_pattern_match_count"] == 1
    assert result["added_by_shadow_top5"] == []


def test_v2_drops_when_no_recent_coverage(tmp_path: Path) -> None:
    materialized = _materialized_rows()
    for row in materialized:
        row["teppan_pattern_match"] = False
        row["teppan_guard_pass"] = False
        row["loss_guard_pass"] = True
        row["loss_guard_blocked"] = False

    payload = mod.run_teppan_shadow_runtime_coverage_observation_v2(
        plan_root=_plan_root(tmp_path),
        materialization_fix_root=_materialization_fix_root(tmp_path),
        output_parent=tmp_path / "out",
        run_id="coverage-v2",
        runtime_status={"selected_runtime_db_path": str(tmp_path / "stocks.duckdb")},
        rankings_freshness={"freshness_state": "fresh", "stale": False},
        active_rows=_active_rows(),
        materialized_rows=materialized,
    )

    result = payload["coverage_observation_v2_result"]
    assert result["decision"] == "drop_shadow_live_value"
    assert result["coverage_summary"]["recent"]["all_observed"]["teppan_pattern_match_count"] == 0
