from __future__ import annotations

import json
from pathlib import Path

from app.backend.services.teppan_shadow_integration_adapter import (
    compute_teppan_shadow_adjusted_ranking,
    load_teppan_shadow_plan,
)
from scripts import tradex_teppan_shadow_integration_implementation_v1 as impl


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _plan_root(tmp_path: Path) -> Path:
    root = tmp_path / "plan"
    probe = tmp_path / "probe"
    _write_json(
        root / "shadow_integration_plan.json",
        {
            "decision": "approve_shadow_integration_implementation",
            "candidate_id": "teppan_ranking_branching_probe_v1",
            "source_roots": {"branching_probe_root": str(probe)},
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


def _rows() -> list[dict[str, object]]:
    return [
        {
            "anchor_date": "2026-05-14",
            "symbol": "1111",
            "side": "long",
            "champion_rank": 5,
            "champion_score": 1.00,
            "teppan_pattern_match": True,
            "teppan_guard_pass": True,
        },
        {
            "anchor_date": "2026-05-14",
            "symbol": "2222",
            "side": "long",
            "champion_rank": 6,
            "champion_score": 0.99,
            "teppan_pattern_match": True,
            "teppan_guard_pass": True,
        },
        {
            "anchor_date": "2026-05-14",
            "symbol": "3333",
            "side": "long",
            "champion_rank": 7,
            "champion_score": 0.98,
            "teppan_pattern_match": True,
            "teppan_guard_pass": False,
        },
        {
            "anchor_date": "2026-05-14",
            "symbol": "4444",
            "side": "long",
            "champion_rank": 8,
            "champion_score": 0.97,
            "teppan_pattern_match": False,
            "teppan_guard_pass": True,
        },
    ]


def test_shadow_adapter_applies_boost_only_in_shadow_context(tmp_path: Path) -> None:
    plan = load_teppan_shadow_plan(_plan_root(tmp_path))
    active_rows = _rows()
    original_rows = [dict(row) for row in active_rows]

    payload = compute_teppan_shadow_adjusted_ranking(active_rows, active_rows, plan)

    assert active_rows == original_rows
    by_symbol = {row["symbol"]: row for row in payload["shadow_rows"]}
    assert by_symbol["1111"]["teppan_guarded_boost_applied"] is False
    assert by_symbol["1111"]["shadow_decision_reason"] == "outside_rank_6_20_shadow_pool"
    assert by_symbol["2222"]["teppan_guarded_boost_applied"] is True
    assert by_symbol["2222"]["shadow_adjusted_score"] == 1.03
    assert by_symbol["2222"]["active_rank"] == 6
    assert by_symbol["2222"]["active_display_score"] == 0.99
    assert by_symbol["2222"]["original_rank"] == 6
    assert by_symbol["2222"]["shadow_adjusted_rank"] == 1
    assert by_symbol["3333"]["shadow_decision_reason"] == "teppan_guard_blocked"
    assert by_symbol["4444"]["shadow_decision_reason"] == "teppan_pattern_match_false"
    assert payload["audit"]["active_ranking_invariance_pass"] is True
    assert payload["audit"]["runtime_duckdb_write_attempted"] is False
    assert payload["audit"]["production_registry_write_attempted"] is False
    assert payload["audit"]["adjusted_rank_separate"] is True


def test_shadow_adapter_missing_features_preserves_order_without_silent_fallback(tmp_path: Path) -> None:
    plan = load_teppan_shadow_plan(_plan_root(tmp_path))
    active_rows = [_rows()[1]]

    payload = compute_teppan_shadow_adjusted_ranking(active_rows, [], plan)

    row = payload["shadow_rows"][0]
    assert row["teppan_guarded_boost_applied"] is False
    assert row["shadow_adjusted_score"] == row["original_score"]
    assert row["shadow_decision_reason"] == "missing_teppan_pattern_match_no_silent_fallback"
    assert payload["summary"]["missing_feature_row_count"] == 1


def test_shadow_implementation_script_writes_required_artifacts(tmp_path: Path) -> None:
    plan_root = _plan_root(tmp_path)
    ledger_path = tmp_path / "probe" / "selected_event_ledger.jsonl"
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    ledger_path.write_text("\n".join(json.dumps(row, ensure_ascii=False) for row in _rows()) + "\n", encoding="utf-8")

    payload = impl.run_teppan_shadow_integration_implementation_v1(
        plan_root=plan_root,
        output_parent=tmp_path / "out",
        run_id="implementation",
        ledger_path=ledger_path,
    )

    output_root = Path(payload["output_root"])
    assert payload["implementation_result"]["decision"] == "shadow_implementation_ready"
    assert payload["acceptance_result"]["decision"] == "shadow_implementation_ready"
    assert payload["no_mutation_audit"]["no_mutation_pass"] is True
    assert payload["no_mutation_audit"]["runtime_db_accessed"] is False
    assert payload["artifact_complete"]["complete"] is True
    assert payload["artifact_complete"]["present_outputs"]["_ARTIFACT_COMPLETE.json"] is True
    for name in impl.REQUIRED_OUTPUTS:
        assert (output_root / name).exists(), name
