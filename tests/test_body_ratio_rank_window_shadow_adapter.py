from __future__ import annotations

import json
from pathlib import Path

from app.backend.services.body_ratio_rank_window_shadow_adapter import (
    compute_body_ratio_rank_window_shadow_ranking,
    load_body_ratio_rank_window_plan,
)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _plan_root(tmp_path: Path) -> Path:
    root = tmp_path / "plan"
    _write_json(
        root / "shadow_integration_plan.json",
        {
            "decision": "approve_shadow_dry_run_implementation_plan",
            "candidate_id": "body_ratio_rank_window_6_20_v1",
            "active_runtime_selection_change": "not_allowed",
            "production_registry_change": "not_allowed",
            "runtime_db_write": "not_allowed",
            "adapter_contract": {"rank_window": [6, 20], "body_ratio_min": 0.30},
        },
    )
    _write_json(root / "feature_materialization_plan.json", {"decision": "ready_for_dry_run_verification"})
    _write_json(
        root / "rank_storage_contract.json",
        {
            "record_shape": {"adjusted_rank_is_separate": True},
            "persistence_contract": {"runtime_db_write": False},
        },
    )
    _write_json(root / "rollback_plan.json", {"runtime_db_rollback_required": False})
    _write_json(root / "_ARTIFACT_COMPLETE.json", {"complete": True})
    return root


def _rows() -> list[dict[str, object]]:
    return [
        {"anchor_date": "2026-06-02", "symbol": "0997", "side": "long", "rank": 1, "display_score": 0.99, "body_ratio": 0.05},
        {"anchor_date": "2026-06-02", "symbol": "0998", "side": "long", "rank": 2, "display_score": 0.98, "body_ratio": 0.05},
        {"anchor_date": "2026-06-02", "symbol": "0999", "side": "long", "rank": 3, "display_score": 0.97, "body_ratio": 0.05},
        {"anchor_date": "2026-06-02", "symbol": "1000", "side": "long", "rank": 4, "display_score": 0.96, "body_ratio": 0.05},
        {"anchor_date": "2026-06-02", "symbol": "1001", "side": "long", "rank": 5, "display_score": 0.95, "body_ratio": 0.05},
        {"anchor_date": "2026-06-02", "symbol": "1002", "side": "long", "rank": 6, "display_score": 0.90, "body_ratio": 0.10},
        {"anchor_date": "2026-06-02", "symbol": "1003", "side": "long", "rank": 7, "display_score": 0.89, "body_ratio": 0.55},
        {"anchor_date": "2026-06-02", "symbol": "1004", "side": "long", "rank": 8, "display_score": 0.88, "body_ratio": 0.20},
        {"anchor_date": "2026-06-02", "symbol": "1005", "side": "long", "rank": 9, "display_score": 0.87, "body_ratio": 0.45},
    ]


def test_body_ratio_shadow_adapter_reorders_only_rank_window(tmp_path: Path) -> None:
    plan = load_body_ratio_rank_window_plan(_plan_root(tmp_path))
    active_rows = _rows()
    original_rows = [dict(row) for row in active_rows]

    payload = compute_body_ratio_rank_window_shadow_ranking(active_rows, plan=plan)

    assert active_rows == original_rows
    by_symbol = {row["symbol"]: row for row in payload["shadow_rows"]}
    assert by_symbol["1001"]["shadow_adjusted_rank"] == 5
    assert by_symbol["1001"]["shadow_decision_reason"] == "outside_rank_window_before_no_change"
    assert by_symbol["1003"]["shadow_adjusted_rank"] == 6
    assert by_symbol["1003"]["body_ratio_gate_passed"] is True
    assert by_symbol["1005"]["shadow_adjusted_rank"] == 7
    assert by_symbol["1002"]["shadow_adjusted_rank"] == 8
    assert by_symbol["1002"]["shadow_decision_reason"] == "body_ratio_rank_window_demoted"
    assert payload["summary"]["changed_rank_count"] == 4
    assert payload["audit"]["active_ranking_invariance_pass"] is True
    assert payload["audit"]["runtime_duckdb_write_attempted"] is False
    assert payload["audit"]["production_registry_write_attempted"] is False
    assert payload["audit"]["adjusted_rank_separate"] is True


def test_body_ratio_shadow_adapter_missing_feature_preserves_original_order(tmp_path: Path) -> None:
    plan = load_body_ratio_rank_window_plan(_plan_root(tmp_path))
    rows = [
        {"anchor_date": "2026-06-02", "symbol": "1001", "side": "long", "rank": 6, "display_score": 0.90},
        {"anchor_date": "2026-06-02", "symbol": "1002", "side": "long", "rank": 7, "display_score": 0.89, "body_ratio": 0.20},
    ]

    payload = compute_body_ratio_rank_window_shadow_ranking(rows, plan=plan)

    by_symbol = {row["symbol"]: row for row in payload["shadow_rows"]}
    assert by_symbol["1001"]["shadow_adjusted_rank"] == 1
    assert by_symbol["1001"]["shadow_decision_reason"] == "missing_body_ratio_no_silent_fallback"
    assert by_symbol["1002"]["shadow_adjusted_rank"] == 2
    assert payload["summary"]["missing_feature_row_count"] == 1
