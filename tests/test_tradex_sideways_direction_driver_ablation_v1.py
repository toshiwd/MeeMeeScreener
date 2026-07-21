from __future__ import annotations

from scripts import tradex_sideways_direction_driver_ablation_v1 as subject


def _result(ba: float, auc: float, n: int = 2000) -> dict:
    return {
        "validation": {"balanced_accuracy": ba, "roc_auc": auc, "sample_count": n},
        "test": {"balanced_accuracy": ba, "roc_auc": auc, "sample_count": n},
    }


def test_feature_groups_keep_price_initial_as_explicit_baseline() -> None:
    assert subject.FEATURE_GROUPS["price_initial_only"] == ("move_since_start_2",)
    assert "move_since_start_2" not in subject.FEATURE_GROUPS["prestart_context_only"]
    assert "move_since_start_2" in subject.FEATURE_GROUPS["price_plus_all_context"]


def test_incremental_gate_requires_one_point_on_both_metrics_and_splits() -> None:
    baseline = _result(0.60, 0.65)
    assert subject.incremental_gate(_result(0.611, 0.661), baseline)["pass"]
    candidate = _result(0.611, 0.661)
    candidate["test"]["roc_auc"] = 0.659
    assert not subject.incremental_gate(candidate, baseline)["pass"]


def test_incremental_gate_requires_minimum_samples() -> None:
    baseline = _result(0.60, 0.65)
    assert not subject.incremental_gate(_result(0.62, 0.67, n=999), baseline)["pass"]
