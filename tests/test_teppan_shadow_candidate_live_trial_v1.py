from __future__ import annotations

import json
from pathlib import Path

from scripts import teppan_shadow_candidate_live_trial_v1 as mod


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
            "materialized_features": [
                {"feature": "teppan_pattern_match"},
                {"feature": "teppan_guard_pass"},
            ],
        },
    )
    _write_json(
        root / "rank_storage_contract.json",
        {
            "record_shape": {
                "original_rank_is_recoverable": True,
                "adjusted_rank_is_separate": True,
            }
        },
    )
    _write_json(root / "rollback_plan.json", {"runtime_db_rollback_required": False})
    _write_json(root / "_ARTIFACT_COMPLETE.json", {"complete": True})
    return root


def _ranking_payload() -> dict[str, object]:
    items = []
    for idx, score in enumerate([1.00, 0.995, 0.990, 0.985, 0.980, 0.970], start=1):
        code = f"10{idx:02d}"
        items.append(
            {
                "code": code,
                "name": f"name-{idx}",
                "asOf": "2026-05-13",
                "entryScore": score,
                "setupType": "breakout",
                "tradeEntryClass": "box_upper_breakout",
                "tradeDecisionReasons": ["weekly breakout"],
                "tradeRiskWatch": [],
            }
        )
    return {"snapshot_as_of": "2026-05-13", "items": items}


def _tags() -> list[dict[str, object]]:
    return [
        {
            "symbol": "1006",
            "anchor_ymd": 20260513,
            "teppan_pattern_match": True,
            "teppan_guard_pass": True,
            "best_pattern_family": "higher_frame_confirmed_daily",
            "best_pattern_key": "synthetic",
            "best_pattern_decision": "high_return_candidate",
            "best_teppan_score": 0.8,
            "matched_pattern_count": 1,
            "guard_block_reason": "",
        },
        {
            "symbol": "1005",
            "anchor_ymd": 20260513,
            "teppan_pattern_match": True,
            "teppan_guard_pass": False,
            "best_pattern_family": "higher_frame_confirmed_daily",
            "best_pattern_key": "blocked",
            "best_pattern_decision": "high_return_candidate",
            "best_teppan_score": 0.6,
            "matched_pattern_count": 1,
            "guard_block_reason": "composite_downside_risk",
        },
    ]


def test_live_trial_generates_shadow_top5_and_human_review_artifacts(tmp_path: Path) -> None:
    payload = mod.run_teppan_shadow_candidate_live_trial_v1(
        plan_root=_plan_root(tmp_path),
        output_parent=tmp_path / "out",
        run_id="trial",
        ranking_payload=_ranking_payload(),
        runtime_status={
            "selected_runtime_db_path": str(tmp_path / "stocks.duckdb"),
            "freshness_state": "fresh",
            "stale": False,
        },
        rankings_freshness={"freshness_state": "fresh", "stale": False, "current_candidate_available": True},
        teppan_tags=_tags(),
    )

    output_root = Path(payload["output_root"])
    result = payload["live_trial_result"]
    assert result["decision"] == "live_trial_ready_for_human_review"
    assert [row["symbol"] for row in result["active_top5"]] == ["1001", "1002", "1003", "1004", "1005"]
    assert "1006" in [row["symbol"] for row in result["shadow_top5"]]
    assert result["added_by_shadow"][0]["symbol"] == "1006"
    assert result["removed_from_active"][0]["symbol"] == "1005"
    assert result["boosted_candidates"][0]["symbol"] == "1006"
    assert result["boosted_candidates"][0]["active_score_source"] == "rank_order_surrogate_no_displayScore"
    assert result["loss_guard_blocked_candidates"][0]["symbol"] == "1005"
    assert result["no_mutation_audit"]["no_mutation_pass"] is True
    assert payload["artifact_complete"]["complete"] is True
    assert payload["artifact_complete"]["present_outputs"]["_ARTIFACT_COMPLETE.json"] is True
    for name in mod.REQUIRED_OUTPUTS:
        assert (output_root / name).exists(), name


def test_live_trial_records_no_added_shadow_candidate_when_guard_blocks(tmp_path: Path) -> None:
    blocked_tags = [{**row, "teppan_guard_pass": False, "guard_block_reason": "composite_downside_risk"} for row in _tags()]
    payload = mod.run_teppan_shadow_candidate_live_trial_v1(
        plan_root=_plan_root(tmp_path),
        output_parent=tmp_path / "out",
        run_id="trial",
        ranking_payload=_ranking_payload(),
        runtime_status={"selected_runtime_db_path": str(tmp_path / "stocks.duckdb")},
        rankings_freshness={"freshness_state": "fresh", "stale": False, "current_candidate_available": True},
        teppan_tags=blocked_tags,
    )

    result = payload["live_trial_result"]
    assert result["added_by_shadow"] == []
    assert result["boosted_candidates"] == []
    assert result["shadow_top5_reason_summary"]["added_by_shadow_count"] == 0
    assert result["loss_guard_blocked_candidates"]
