from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts import candidate_selection_quality_decomposition_v1 as mod


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")


def _make_fixture(tmp_path: Path) -> Path:
    root = tmp_path / "gate"
    run = root / "subruns" / "2025-baseline-portfolio_agent_replay_v1"
    entry = root / "entry_confirmation_pretest_v1"
    run.mkdir(parents=True)
    entry.mkdir(parents=True)
    pd.DataFrame([{"year": 2025, "run_dir": str(run), "total_return": 0.5, "benchmark_return": 0.2, "max_drawdown": -0.1}]).to_csv(root / "yearly_results.csv", index=False)
    components_strong = json.dumps(
        [
            {"feature": "daily_candle_state", "value": "daily_strong_bull"},
            {"feature": "daily_volume_state", "value": "daily_volume_expansion"},
            {"feature": "weekly_trend_state", "value": "weekly_uptrend"},
        ]
    )
    components_weak = json.dumps(
        [
            {"feature": "daily_candle_state", "value": "daily_upper_wick"},
            {"feature": "daily_volume_state", "value": "daily_volume_normal"},
            {"feature": "weekly_trend_state", "value": "weekly_uptrend"},
        ]
    )
    pd.DataFrame(
        [
            {"decision_ymd": 20250104, "code": "1001", "candidate_rank": 1, "selection_score": 15, "entry_allowed_by_score": True, "downside_guard_blocked": False, "selected_for_buy": True, "reject_reason": "", "score_components_json": components_strong},
            {"decision_ymd": 20250104, "code": "1002", "candidate_rank": 2, "selection_score": 12, "entry_allowed_by_score": True, "downside_guard_blocked": False, "selected_for_buy": True, "reject_reason": "", "score_components_json": components_weak},
            {"decision_ymd": 20250104, "code": "1003", "candidate_rank": 4, "selection_score": 14, "entry_allowed_by_score": True, "downside_guard_blocked": False, "selected_for_buy": False, "reject_reason": "max_positions_full", "score_components_json": components_strong},
        ]
    ).to_csv(run / "daily_candidate_snapshot.csv", index=False)
    pd.DataFrame(
        [
            {"decision_ymd": 20250104, "code": "1001", "was_selected": True, "post_ret_20": 0.15, "mae_20": -0.01, "mfe_20": 0.20},
            {"decision_ymd": 20250104, "code": "1002", "was_selected": True, "post_ret_20": -0.08, "mae_20": -0.12, "mfe_20": 0.01},
            {"decision_ymd": 20250104, "code": "1003", "was_selected": False, "post_ret_20": 0.20, "mae_20": -0.01, "mfe_20": 0.25},
        ]
    ).to_csv(run / "post_run_outcome_labels.csv", index=False)
    pd.DataFrame([{"decision_ymd": 20250104, "code": "1003", "candidate_rank": 4, "selection_score": 14, "reject_reason": "max_positions_full"}]).to_csv(run / "rejected_candidates.csv", index=False)
    pd.DataFrame(
        [
            {"year": 2025, "original_decision_ymd": 20250104, "confirmation_ymd": 20250105, "code": "1001", "original_rank": 1, "original_score": 15, "confirmation_rank": 20, "confirmation_score": 10, "cancel_reason": "rank_or_score_deteriorated", "decision_ymd": 20250104, "was_selected": True, "diagnostic_only": True, "post_ret_20": 0.15, "mae_20": -0.01, "mfe_20": 0.20, "entry_confirmation_outcome_class": "missed_winner_due_to_confirmation", "avoided_loss_estimate": 0.0, "missed_profit_estimate": 0.15},
            {"year": 2025, "original_decision_ymd": 20250104, "confirmation_ymd": 20250105, "code": "1002", "original_rank": 2, "original_score": 12, "confirmation_rank": 30, "confirmation_score": 8, "cancel_reason": "rank_or_score_deteriorated", "decision_ymd": 20250104, "was_selected": True, "diagnostic_only": True, "post_ret_20": -0.08, "mae_20": -0.12, "mfe_20": 0.01, "entry_confirmation_outcome_class": "avoided_bad_entry", "avoided_loss_estimate": 0.08, "missed_profit_estimate": 0.0},
        ]
    ).to_csv(entry / "entry_confirmation_outcome_analysis.csv", index=False)
    pd.DataFrame([{"year": 2025, "original_decision_ymd": 20250104, "confirmation_ymd": 20250105, "code": "1001"}]).to_csv(entry / "cancelled_entries.csv", index=False)
    pd.DataFrame([{"year": 2025, "original_decision_ymd": 20250104, "confirmation_ymd": 20250105, "code": "9999"}]).to_csv(entry / "confirmed_entries.csv", index=False)
    _write_json(entry / "_ARTIFACT_COMPLETE.json", {"decision": "drop_due_to_profit_damage"})
    _write_json(entry / "goal_gate_summary.json", {"return_damage_2025": -0.30})
    _write_json(entry / "no_lookahead_audit.json", {"audit_result": "pass"})
    _write_json(entry / "selection_feature_manifest.json", {"audit_result": "pass"})
    pd.DataFrame([{"year": 2025}]).to_csv(entry / "yearly_results_baseline_vs_entry_confirmation.csv", index=False)
    return root


def test_candidate_selection_quality_decomposition_outputs(tmp_path: Path) -> None:
    root = _make_fixture(tmp_path)

    result = mod.run_decomposition(root)

    out = root / "candidate_selection_quality_decomposition_v1"
    assert result["complete"] is True
    for artifact in mod.REQUIRED_ARTIFACTS:
        assert (out / artifact).exists(), artifact
    complete = json.loads((out / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    assert complete["required_artifacts_all_present"] is True
    assert complete["replay_rerun"] is False
    assert complete["rule_changed"] is False
    assert complete["threshold_sweep"] is False

    missed = pd.read_csv(out / "missed_winner_due_to_confirmation_cases.csv")
    assert "daily_volume_state" in missed.columns
    assert len(missed) == 1
    alternatives = pd.read_csv(out / "same_day_candidate_alternatives.csv")
    assert len(alternatives) == 1

    decision = json.loads((out / "next_axis_decision.json").read_text(encoding="utf-8"))
    assert decision["decision"] in mod.DECISIONS
    assert decision["decision_count"] == 1
