from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts import tradex_profit_loss_policy_axis_v1 as mod


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_profit_loss_policy_axis_keeps_reduce_when_return_and_risk_improve(tmp_path: Path) -> None:
    source = tmp_path / "historical_operational_replay_rows.csv"
    pd.DataFrame(
        [
            {"as_of_date": 20260501, "period_bucket": "2026H1", "code": "1001", "ret5": -0.04, "ret20": -0.14, "invalidation_hit_20d": True},
            {"as_of_date": 20260501, "period_bucket": "2026H1", "code": "1002", "ret5": 0.05, "ret20": 0.18, "invalidation_hit_20d": False},
            {"as_of_date": 20260502, "period_bucket": "2026H2", "code": "1003", "ret5": -0.049, "ret20": -0.12, "invalidation_hit_20d": True},
            {"as_of_date": 20260502, "period_bucket": "2026H2", "code": "1004", "ret5": 0.02, "ret20": 0.03, "invalidation_hit_20d": False},
        ]
    ).to_csv(source, index=False)

    result = mod.run_profit_loss_policy_axis_v1(source, tmp_path / "out")

    assert result["decision"] == "keep_deterioration_reduce_for_forward_replay"
    assert result["observed_branching"]["changed_top5_members_count"] == 0
    assert result["fixed_evaluation_conditions"]["same_entry_selector"] is True
    assert result["metrics"]["deterioration_reduce"]["mean_return"] > result["metrics"]["baseline_hold20"]["mean_return"]
    assert result["metrics"]["deterioration_reduce"]["severe_loss_rate_le_minus_10pct"] == 0.0

    out = tmp_path / "out"
    for artifact in mod.REQUIRED_OUTPUTS:
        assert (out / artifact).exists(), artifact
    complete = _read_json(out / "_ARTIFACT_COMPLETE.json")
    assert complete["complete"] is True
    audit = _read_json(out / "no_lookahead_audit.json")
    assert audit["audit_result"] == "pass"
    assert audit["runtime_db_write"] is False
    assert audit["meemee_reflection"] is False

    rows = pd.read_csv(out / "policy_axis_rows.csv")
    assert rows["deterioration_reduce_trigger"].astype(bool).sum() == 2
    assert set(rows["policy_note"]) == {"ret5_is_observable_day5_proxy_no_entry_selector_change"}


def test_profit_loss_policy_axis_drops_when_profit_damage_or_risk_not_jointly_better(tmp_path: Path) -> None:
    source = tmp_path / "historical_operational_replay_rows.csv"
    pd.DataFrame(
        [
            {"as_of_date": 20260501, "code": "2001", "ret5": -0.06, "ret20": 0.16, "invalidation_hit_20d": False},
            {"as_of_date": 20260501, "code": "2002", "ret5": 0.01, "ret20": 0.04, "invalidation_hit_20d": False},
            {"as_of_date": 20260502, "code": "2003", "ret5": -0.04, "ret20": 0.12, "invalidation_hit_20d": False},
            {"as_of_date": 20260502, "code": "2004", "ret5": 0.02, "ret20": -0.02, "invalidation_hit_20d": False},
        ]
    ).to_csv(source, index=False)

    result = mod.run_profit_loss_policy_axis_v1(source, tmp_path / "out")

    assert result["decision"] == "drop_policy_axis_no_joint_return_risk_edge"
    assert result["decision_class"] == "DROP"
    assert result["deltas_vs_baseline"]["deterioration_reduce"]["mean_return"] < 0


def test_profit_loss_policy_axis_blocks_missing_required_columns(tmp_path: Path) -> None:
    source = tmp_path / "bad_rows.csv"
    pd.DataFrame([{"as_of_date": 20260501, "code": "3001", "ret20": 0.05}]).to_csv(source, index=False)

    result = mod.run_profit_loss_policy_axis_v1(source, tmp_path / "out")

    assert result["decision"] == "blocked_missing_required_columns"
    complete = _read_json(tmp_path / "out" / "_ARTIFACT_COMPLETE.json")
    assert complete["complete"] is False
    assert "ret5" in result["missing_required_columns"]


def test_profit_loss_policy_axis_keeps_day5_shock_partial_reduce_when_jointly_better(tmp_path: Path) -> None:
    source = tmp_path / "historical_operational_replay_rows.csv"
    pd.DataFrame(
        [
            {"as_of_date": 20260501, "period_bucket": "2026H1", "code": "4001", "ret5": -0.06, "ret20": -0.12, "invalidation_hit_20d": False},
            {"as_of_date": 20260501, "period_bucket": "2026H1", "code": "4002", "ret5": 0.04, "ret20": 0.10, "invalidation_hit_20d": False},
            {"as_of_date": 20260502, "period_bucket": "2026H2", "code": "4003", "ret5": -0.055, "ret20": -0.11, "invalidation_hit_20d": False},
            {"as_of_date": 20260502, "period_bucket": "2026H2", "code": "4004", "ret5": 0.02, "ret20": 0.06, "invalidation_hit_20d": False},
        ]
    ).to_csv(source, index=False)

    result = mod.run_profit_loss_policy_axis_v1(source, tmp_path / "out")

    assert result["decision"] == "keep_day5_shock_partial_reduce_for_forward_replay"
    assert result["decision_class"] == "KEEP"
    shock = result["metrics"]["day5_shock_partial_reduce"]
    baseline = result["metrics"]["baseline_hold20"]
    assert shock["mean_return"] > baseline["mean_return"]
    assert shock["severe_loss_rate_le_minus_10pct"] < baseline["severe_loss_rate_le_minus_10pct"]
    assert result["fixed_evaluation_conditions"]["day5_shock_threshold"] == mod.DAY5_SHOCK_THRESHOLD
    assert result["fixed_evaluation_conditions"]["day5_shock_trim_ratio"] == mod.DAY5_SHOCK_TRIM_RATIO

    stability = _read_json(tmp_path / "out" / "policy_axis_period_stability.json")
    assert stability["period_column"] == "period_bucket"
    assert stability["trigger_period_count"] == 2
    assert stability["severe_not_worse_period_count"] == 2
    assert stability["dd_not_worse_period_count"] == 2
    assert result["period_stability_summary"]["stable_enough_for_forward_replay"] is True

    trigger_audit = pd.read_csv(tmp_path / "out" / "policy_axis_trigger_audit.csv")
    assert len(trigger_audit) == 2
    assert set(trigger_audit["trigger_classification"]) == {"saved_severe_loss"}
    assert result["trigger_audit_summary"]["saved_severe_loss_count"] == 2
    assert result["trigger_audit_summary"]["false_reduce_winner_count"] == 0


def test_profit_loss_policy_axis_promotes_near_high_shock_policy_when_available(tmp_path: Path) -> None:
    source = tmp_path / "historical_operational_replay_rows.csv"
    pd.DataFrame(
        [
            {"as_of_date": 20250101, "period_bucket": "train", "code": "6001", "ret5": -0.06, "ret20": -0.12, "recent_high_distance_pct": 0.0},
            {"as_of_date": 20250102, "period_bucket": "train", "code": "6002", "ret5": 0.01, "ret20": 0.08, "recent_high_distance_pct": 0.02},
            {"as_of_date": 20260101, "period_bucket": "holdout", "code": "6003", "ret5": -0.055, "ret20": -0.11, "recent_high_distance_pct": 0.0},
            {"as_of_date": 20260102, "period_bucket": "holdout", "code": "6004", "ret5": 0.03, "ret20": 0.06, "recent_high_distance_pct": 0.02},
        ]
    ).to_csv(source, index=False)

    result = mod.run_profit_loss_policy_axis_v1(source, tmp_path / "out", holdout_cutoff_ymd=20251231)

    assert result["decision"] == "keep_day5_shock_near_high_partial_reduce_for_forward_replay"
    near_high = result["metrics"]["day5_shock_near_high_partial_reduce"]
    baseline = result["metrics"]["baseline_hold20"]
    assert near_high["mean_return"] > baseline["mean_return"]
    assert near_high["severe_loss_rate_le_minus_10pct"] < baseline["severe_loss_rate_le_minus_10pct"]
    assert result["fixed_evaluation_conditions"]["recent_high_distance_max_for_keep"] == mod.RECENT_HIGH_DISTANCE_MAX_FOR_KEEP
    robustness = _read_json(tmp_path / "out" / "policy_axis_cutoff_robustness.json")
    assert robustness["policy"] == "day5_shock_near_or_below_recent_high_trim50"
    promotion = _read_json(tmp_path / "out" / "policy_axis_promotion_readiness.json")
    assert promotion["promotable_for_operational_review"] is True
    assert promotion["meemee_reflection"] is False
    review = _read_json(tmp_path / "out" / "policy_axis_operator_review_pack.json")
    assert review["review_decision"] == "review_candidate_keep"
    assert review["next_action"] == "manual_operator_review_only"
    gate = _read_json(tmp_path / "out" / "policy_axis_forward_gate.json")
    assert gate["gate_decision"] == "forward_gate_ready_for_manual_review"
    assert gate["next_required_action"] == "manual_operator_review"
    assert gate["blockers"] == []


def test_profit_loss_policy_axis_blocks_when_ret20_outcomes_are_not_ready(tmp_path: Path) -> None:
    source = tmp_path / "forward_rows.csv"
    pd.DataFrame(
        [
            {"as_of_date": 20260520, "period_bucket": "forward", "code": "8086", "ret5": -0.007, "ret20": None},
            {"as_of_date": 20260520, "period_bucket": "forward", "code": "9831", "ret5": 0.008, "ret20": None},
        ]
    ).to_csv(source, index=False)

    result = mod.run_profit_loss_policy_axis_v1(source, tmp_path / "out")

    assert result["decision"] == "blocked_insufficient_outcome_coverage"
    assert result["decision_class"] == "BLOCKED"
    assert result["outcome_coverage"]["ret20_ready_count"] == 0
    complete = _read_json(tmp_path / "out" / "_ARTIFACT_COMPLETE.json")
    assert complete["complete"] is False
    assert complete["decision"] == "blocked_insufficient_outcome_coverage"
    robustness = _read_json(tmp_path / "out" / "policy_axis_cutoff_robustness.json")
    assert robustness["robustness_decision"] == "blocked_insufficient_outcome_coverage"
    promotion = _read_json(tmp_path / "out" / "policy_axis_promotion_readiness.json")
    assert promotion["promotable_for_operational_review"] is False
    assert "forward_ret20_not_fully_mature" in promotion["blockers"]
    review = _read_json(tmp_path / "out" / "policy_axis_operator_review_pack.json")
    assert review["review_decision"] == "blocked_wait_for_forward_maturity"
    assert review["next_action"] == "rerun_after_forward_ret20_ready"
    gate = _read_json(tmp_path / "out" / "policy_axis_forward_gate.json")
    assert gate["gate_decision"] == "forward_gate_blocked"
    assert gate["next_required_action"] == "rerun_same_condition_after_forward_ret20_ready"
    assert "forward_ret20_not_fully_mature" in gate["blockers"]


def test_profit_loss_policy_axis_emits_holdout_validation(tmp_path: Path) -> None:
    source = tmp_path / "historical_operational_replay_rows.csv"
    pd.DataFrame(
        [
            {
                "as_of_date": 20250101,
                "period_bucket": "train",
                "code": "5001",
                "ret5": -0.06,
                "ret20": -0.12,
                "variant_b_volatility_extension_clean": True,
                "recent_high_distance_pct": 0.0,
            },
            {
                "as_of_date": 20250102,
                "period_bucket": "train",
                "code": "5002",
                "ret5": 0.01,
                "ret20": 0.08,
                "variant_b_volatility_extension_clean": True,
                "recent_high_distance_pct": 0.02,
            },
            {
                "as_of_date": 20260101,
                "period_bucket": "holdout",
                "code": "5003",
                "ret5": -0.055,
                "ret20": -0.11,
                "variant_b_volatility_extension_clean": True,
                "recent_high_distance_pct": 0.0,
            },
            {
                "as_of_date": 20260102,
                "period_bucket": "holdout",
                "code": "5004",
                "ret5": 0.03,
                "ret20": 0.06,
                "variant_b_volatility_extension_clean": True,
                "recent_high_distance_pct": 0.02,
            },
        ]
    ).to_csv(source, index=False)

    result = mod.run_profit_loss_policy_axis_v1(source, tmp_path / "out", holdout_cutoff_ymd=20251231)

    holdout = _read_json(tmp_path / "out" / "policy_axis_holdout_validation.json")
    assert holdout["enabled"] is True
    assert holdout["cutoff_ymd"] == 20251231
    assert holdout["holdout"]["joint_return_risk_pass"] is True
    assert holdout["holdout_decision"] == "holdout_pass"
    assert result["holdout_validation_summary"]["holdout_joint_return_risk_pass"] is True
    grid = pd.read_csv(tmp_path / "out" / "policy_axis_candidate_grid.csv")
    assert {
        "day5_shock_all_trim50",
        "day5_shock_extension_clean_trim50",
        "day5_shock_near_or_below_recent_high_trim50",
    }.issubset(set(grid["variant"]))
    assert "day5_shock_all_trim50" in result["candidate_grid_summary"]["variants_with_holdout_pass"]
    robustness = _read_json(tmp_path / "out" / "policy_axis_cutoff_robustness.json")
    assert robustness["cutoff_count"] >= 1
    assert "robustness_decision" in robustness
    promotion = _read_json(tmp_path / "out" / "policy_axis_promotion_readiness.json")
    assert promotion["runtime_db_write"] is False
