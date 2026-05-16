from __future__ import annotations

import json
from pathlib import Path

from scripts import teppan_shadow_watch_mode_logging_v1 as mod


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _shadow_plan_root(tmp_path: Path) -> Path:
    root = tmp_path / "shadow_plan"
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
    _write_json(root / "feature_materialization_plan.json", {"decision": "ready", "materialized_features": ["teppan_pattern_match", "teppan_guard_pass"]})
    _write_json(root / "rank_storage_contract.json", {"record_shape": {"original_rank_is_recoverable": True, "adjusted_rank_is_separate": True}})
    _write_json(root / "rollback_plan.json", {"runtime_db_rollback_required": False})
    _write_json(root / "_ARTIFACT_COMPLETE.json", {"complete": True})
    return root


def _watch_plan_root(tmp_path: Path, *, decision: str = "watch_mode_ready", activation_allowed: bool = False) -> Path:
    root = tmp_path / "watch_plan"
    _write_json(root / "research_decision.json", {"decision": decision, "activation_allowed": activation_allowed})
    _write_json(root / "watch_mode_logging_plan.json", {"mode": "watch_only_inactive_shadow"})
    _write_json(root / "watch_trigger_conditions.json", {"human_review_trigger_if_any": []})
    _write_json(root / "human_review_trigger_contract.json", {"activation_allowed_after_trigger": False})
    _write_json(root / "no_activation_policy.json", {"activation_allowed": activation_allowed})
    _write_json(root / "_ARTIFACT_COMPLETE.json", {"complete": True})
    return root


def _active_rows() -> list[dict[str, object]]:
    return [
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
        }
        for rank in range(1, 21)
    ]


def _materialized_rows(match_symbols: set[str], blocked_symbols: set[str] | None = None) -> list[dict[str, object]]:
    blocked_symbols = blocked_symbols or set()
    rows = []
    for active in _active_rows():
        symbol = str(active["symbol"])
        pattern = symbol in match_symbols or symbol in blocked_symbols
        guard = symbol in match_symbols
        rows.append(
            {
                **active,
                "active_rank": active["champion_rank"],
                "teppan_pattern_match": pattern,
                "teppan_guard_pass": guard,
                "loss_guard_pass": guard or not pattern,
                "loss_guard_blocked": symbol in blocked_symbols,
                "guard_block_reason": "composite_downside_risk" if symbol in blocked_symbols else "" if pattern else "no_teppan_pattern_match",
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


def test_watch_logger_triggers_manual_review_for_top5_addition(tmp_path: Path) -> None:
    payload = mod.run_teppan_shadow_watch_mode_logging_v1(
        watch_plan_root=_watch_plan_root(tmp_path),
        plan_root=_shadow_plan_root(tmp_path),
        output_parent=tmp_path / "out",
        run_id="watch",
        runtime_status={"selected_runtime_db_path": str(tmp_path / "stocks.duckdb")},
        rankings_freshness={"freshness_state": "fresh", "stale": False},
        active_rows=_active_rows(),
        materialized_rows=_materialized_rows({"1006"}, {"1012"}),
    )

    result = payload["watch_run_result"]
    metrics = payload["teppan_watch_metrics"]
    trigger = payload["human_review_trigger_report"]
    assert result["decision"] == "manual_review_triggered"
    assert trigger["manual_review_triggered"] is True
    assert metrics["added_by_shadow_top5"] == 1
    assert metrics["boost_eligible_count"] == 1
    assert metrics["loss_guard_blocked_count"] == 1
    assert result["activation_allowed"] is False
    assert payload["artifact_complete"]["complete"] is True
    for name in mod.REQUIRED_OUTPUTS:
        assert (Path(payload["output_root"]) / name).exists(), name


def test_watch_logger_continues_watch_only_without_trigger(tmp_path: Path) -> None:
    payload = mod.run_teppan_shadow_watch_mode_logging_v1(
        watch_plan_root=_watch_plan_root(tmp_path),
        plan_root=_shadow_plan_root(tmp_path),
        output_parent=tmp_path / "out",
        run_id="watch",
        runtime_status={"selected_runtime_db_path": str(tmp_path / "stocks.duckdb")},
        rankings_freshness={"freshness_state": "fresh", "stale": False},
        active_rows=_active_rows(),
        materialized_rows=_materialized_rows(set()),
    )

    result = payload["watch_run_result"]
    metrics = payload["teppan_watch_metrics"]
    assert result["decision"] == "continue_watch_only"
    assert result["logger_status"] == "watch_logger_ready"
    assert metrics["manual_review_triggered"] is False
    assert metrics["human_review_candidate_count"] == 0
    assert metrics["no_mutation_pass"] is True


def test_watch_logger_rejects_activation_allowed_plan(tmp_path: Path) -> None:
    try:
        mod.run_teppan_shadow_watch_mode_logging_v1(
            watch_plan_root=_watch_plan_root(tmp_path, activation_allowed=True),
            plan_root=_shadow_plan_root(tmp_path),
            output_parent=tmp_path / "out",
            run_id="watch",
            runtime_status={"selected_runtime_db_path": str(tmp_path / "stocks.duckdb")},
            rankings_freshness={"freshness_state": "fresh", "stale": False},
            active_rows=_active_rows(),
            materialized_rows=_materialized_rows(set()),
        )
    except ValueError as exc:
        assert "watch_plan_activation_not_forbidden" in str(exc)
    else:
        raise AssertionError("expected activation-allowed watch plan to be rejected")
