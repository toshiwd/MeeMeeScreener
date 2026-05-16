from __future__ import annotations

import json
from pathlib import Path

from scripts import teppan_shadow_recent_days_replay_v1 as mod


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


def _active_rows(dates: tuple[str, ...] = ("2026-05-12", "2026-05-13")) -> list[dict[str, object]]:
    rows = []
    for date in dates:
        ymd = int(date.replace("-", ""))
        for rank in range(1, 21):
            rows.append(
                {
                    "anchor_date": date,
                    "anchor_ymd": ymd,
                    "symbol": f"{date[-2:]}{rank:02d}",
                    "name": f"name-{date}-{rank}",
                    "side": "long",
                    "champion_rank": rank,
                    "runtime_rank": rank,
                    "champion_score": 1.0 - (rank * 0.01),
                    "display_score": 1.0 - (rank * 0.01),
                }
            )
    return rows


def _materialized_rows(active_rows: list[dict[str, object]], *, match_symbols: set[str], blocked_symbols: set[str] | None = None) -> list[dict[str, object]]:
    blocked_symbols = blocked_symbols or set()
    rows = []
    for active in active_rows:
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


def test_recent_days_replay_passes_when_multiple_dates_create_shadow_additions(tmp_path: Path) -> None:
    active = _active_rows()
    materialized = _materialized_rows(active, match_symbols={"1206", "1306"}, blocked_symbols={"1312"})
    payload = mod.run_teppan_shadow_recent_days_replay_v1(
        plan_root=_plan_root(tmp_path),
        output_parent=tmp_path / "out",
        run_id="replay",
        runtime_status={"selected_runtime_db_path": str(tmp_path / "stocks.duckdb")},
        rankings_freshness={"freshness_state": "fresh", "stale": False},
        active_rows=active,
        materialized_rows=materialized,
    )

    result = payload["replay_result"]
    metrics = result["required_metrics"]
    assert result["decision"] == "recent_days_replay_pass"
    assert metrics["replay_date_count"] == 2
    assert metrics["added_by_shadow_top5_count"] == 2
    assert metrics["total_loss_guard_blocked_count"] == 1
    assert result["human_review_candidate_count"] >= 2
    assert payload["artifact_complete"]["complete"] is True
    for name in mod.REQUIRED_OUTPUTS:
        assert (Path(payload["output_root"]) / name).exists(), name


def test_recent_days_replay_holds_for_watch_mode_when_coverage_does_not_enter_topk(tmp_path: Path) -> None:
    active = _active_rows(("2026-05-13",))
    materialized = _materialized_rows(active, match_symbols={"1320"})
    payload = mod.run_teppan_shadow_recent_days_replay_v1(
        plan_root=_plan_root(tmp_path),
        output_parent=tmp_path / "out",
        run_id="replay",
        runtime_status={"selected_runtime_db_path": str(tmp_path / "stocks.duckdb")},
        rankings_freshness={"freshness_state": "fresh", "stale": False},
        active_rows=active,
        materialized_rows=materialized,
    )

    result = payload["replay_result"]
    assert result["decision"] == "hold_for_watch_mode"
    assert result["required_metrics"]["dates_with_teppan_pattern_match_top20"] == ["2026-05-13"]
    assert result["required_metrics"]["added_by_shadow_top5_count"] == 0


def test_recent_days_replay_drops_when_no_coverage_or_candidates(tmp_path: Path) -> None:
    active = _active_rows(("2026-05-13",))
    materialized = _materialized_rows(active, match_symbols=set())
    payload = mod.run_teppan_shadow_recent_days_replay_v1(
        plan_root=_plan_root(tmp_path),
        output_parent=tmp_path / "out",
        run_id="replay",
        runtime_status={"selected_runtime_db_path": str(tmp_path / "stocks.duckdb")},
        rankings_freshness={"freshness_state": "fresh", "stale": False},
        active_rows=active,
        materialized_rows=materialized,
    )

    result = payload["replay_result"]
    assert result["decision"] == "drop_shadow_live_value"
    assert result["required_metrics"]["dates_with_teppan_pattern_match_top100"] == []
    assert result["human_review_candidate_count"] == 0
