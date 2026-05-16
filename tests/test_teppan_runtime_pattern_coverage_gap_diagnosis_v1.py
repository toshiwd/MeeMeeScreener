from __future__ import annotations

import json
from pathlib import Path

from scripts import teppan_runtime_pattern_coverage_gap_diagnosis_v1 as mod


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _plan_root(tmp_path: Path) -> Path:
    root = tmp_path / "plan"
    _write_json(
        root / "shadow_integration_plan.json",
        {
            "decision": "approve_shadow_integration_implementation",
            "active_runtime_ranking_change_allowed": False,
            "runtime_duckdb_write_allowed": False,
            "production_publish_registration_allowed": False,
            "frontend_or_backend_ui_change_allowed": False,
            "static_soft_boost_value": 0.04,
        },
    )
    _write_json(
        root / "feature_materialization_plan.json",
        {"decision": "ready", "materialized_features": [{"feature": "teppan_pattern_match"}, {"feature": "teppan_guard_pass"}]},
    )
    _write_json(root / "rank_storage_contract.json", {"record_shape": {"original_rank_is_recoverable": True, "adjusted_rank_is_separate": True}})
    _write_json(root / "rollback_plan.json", {"runtime_db_rollback_required": False})
    _write_json(root / "_ARTIFACT_COMPLETE.json", {"complete": True})
    return root


def _pattern_root(tmp_path: Path) -> Path:
    root = tmp_path / "pattern"
    _write_json(
        root / "teppan_candidates.json",
        {
            "candidates": [
                {
                    "pattern_family": "multi_tf_trend_core",
                    "pattern_key": "daily_ma_stack=approved_daily|weekly_trend_state=approved_weekly|monthly_trend_state=approved_monthly",
                    "pattern_decision": "high_return_candidate",
                    "teppan_score": 1.0,
                    "pattern_features": {
                        "daily_ma_stack": "approved_daily",
                        "weekly_trend_state": "approved_weekly",
                        "monthly_trend_state": "approved_monthly",
                    },
                }
            ]
        },
    )
    return root


def _active_rows() -> list[dict[str, object]]:
    return [
        {
            "anchor_date": "2026-05-13",
            "anchor_ymd": 20260513,
            "symbol": "1001",
            "name": "one",
            "side": "long",
            "champion_rank": 1,
            "champion_score": 1.0,
            "display_score": 1.0,
        },
        {
            "anchor_date": "2026-05-13",
            "anchor_ymd": 20260513,
            "symbol": "1006",
            "name": "six",
            "side": "long",
            "champion_rank": 6,
            "champion_score": 0.95,
            "display_score": 0.95,
        },
    ]


def _anchor(symbol: str, *, approved: bool) -> dict[str, object]:
    value = "approved" if approved else "other"
    return {
        "symbol": symbol,
        "anchor_date": "2026-05-13",
        "anchor_ymd": 20260513,
        "daily_ma_stack": f"{value}_daily",
        "weekly_trend_state": f"{value}_weekly",
        "monthly_trend_state": f"{value}_monthly",
    }


def _false_tags() -> list[dict[str, object]]:
    return [
        {
            "symbol": "1001",
            "anchor_ymd": 20260513,
            "teppan_pattern_match": False,
            "teppan_guard_pass": False,
            "matched_pattern_count": 0,
            "guard_block_reason": "no_teppan_pattern_match",
        },
        {
            "symbol": "1006",
            "anchor_ymd": 20260513,
            "teppan_pattern_match": False,
            "teppan_guard_pass": False,
            "matched_pattern_count": 0,
            "guard_block_reason": "no_teppan_pattern_match",
        },
    ]


def test_gap_diagnosis_confirms_no_current_runtime_exact_match(tmp_path: Path) -> None:
    payload = mod.run_teppan_runtime_pattern_coverage_gap_diagnosis_v1(
        plan_root=_plan_root(tmp_path),
        pattern_root=_pattern_root(tmp_path),
        output_parent=tmp_path / "out",
        run_id="diagnosis",
        runtime_status={"selected_runtime_db_path": str(tmp_path / "stocks.duckdb")},
        ranking_field_audit={"pass": True, "missing_required_fields": []},
        active_rows=_active_rows(),
        anchor_features=[_anchor("1001", approved=False), _anchor("1006", approved=False)],
        teppan_tags=_false_tags(),
    )

    result = payload["coverage_gap_diagnosis_result"]
    assert result["decision"] == "gap_confirmed_no_current_runtime_exact_pattern_match"
    assert result["runtime_coverage_by_topk_and_date"]["by_topk"]["top100"]["teppan_pattern_match_count"] == 0
    assert result["materialization_false_diagnostic"]["materialization_impl_bug_suspected"] is False
    assert result["materialization_false_diagnostic"]["diagnosis"] == "materialization_matches_independent_exact_match_zero"
    assert result["pattern_condition_pass_rates"]["feature_condition_pass_rates"]["daily_ma_stack"]["approved_value_pass_count"] == 0
    assert result["no_mutation_audit"]["no_mutation_pass"] is True
    assert payload["artifact_complete"]["complete"] is True
    for name in mod.REQUIRED_OUTPUTS:
        assert (Path(payload["output_root"]) / name).exists(), name


def test_gap_diagnosis_flags_materialization_false_mismatch(tmp_path: Path) -> None:
    payload = mod.run_teppan_runtime_pattern_coverage_gap_diagnosis_v1(
        plan_root=_plan_root(tmp_path),
        pattern_root=_pattern_root(tmp_path),
        output_parent=tmp_path / "out",
        run_id="diagnosis",
        runtime_status={"selected_runtime_db_path": str(tmp_path / "stocks.duckdb")},
        ranking_field_audit={"pass": True, "missing_required_fields": []},
        active_rows=_active_rows(),
        anchor_features=[_anchor("1001", approved=False), _anchor("1006", approved=True)],
        teppan_tags=_false_tags(),
    )

    result = payload["coverage_gap_diagnosis_result"]
    assert result["decision"] == "hold_materialization_impl_gap"
    assert result["materialization_false_diagnostic"]["materialization_impl_bug_suspected"] is True
    assert result["materialization_false_diagnostic"]["mismatch_examples"][0]["symbol"] == "1006"
