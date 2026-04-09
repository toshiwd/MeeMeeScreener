from __future__ import annotations

import pytest

from app.backend.services import tradex_research_trader_label_policy as label_policy


def _benchmark_row(
    *,
    complete_horizon: bool = True,
    teacher_horizon_bars: int = 20,
    future_bar_count: int = 20,
    return_close_basis: float | None = 0.05,
    return_next_open_basis: float | None = 0.02,
    mfe: float | None = 0.12,
    mae: float | None = -0.03,
) -> dict[str, object]:
    return {
        "complete_horizon": complete_horizon,
        "teacher_horizon_bars": teacher_horizon_bars,
        "future_bar_count": future_bar_count,
        "return_close_basis": return_close_basis,
        "return_next_open_basis": return_next_open_basis,
        "max_favorable_excursion_close_basis": mfe,
        "max_adverse_excursion_close_basis": mae,
    }


def test_apply_trader_label_policy_positive_good_outcome() -> None:
    labels = label_policy.apply_trader_label_policy(_benchmark_row())

    assert labels["close_positive_20"] is True
    assert labels["next_open_positive_20"] is True
    assert labels["mfe_ge_10pct_20"] is True
    assert labels["mae_worse_than_7pct_20"] is False
    assert labels["judgement_outcome_class"] == "good"
    assert labels["label_policy_version"] == "v1"


def test_apply_trader_label_policy_negative_bad_outcome() -> None:
    labels = label_policy.apply_trader_label_policy(
        _benchmark_row(return_close_basis=-0.02, return_next_open_basis=-0.01, mfe=0.04, mae=-0.09)
    )

    assert labels["close_positive_20"] is False
    assert labels["next_open_positive_20"] is False
    assert labels["mfe_ge_10pct_20"] is False
    assert labels["mae_worse_than_7pct_20"] is True
    assert labels["judgement_outcome_class"] == "bad"


def test_apply_trader_label_policy_incomplete_horizon_returns_null_labels() -> None:
    labels = label_policy.apply_trader_label_policy(_benchmark_row(complete_horizon=False, future_bar_count=8))

    assert labels["close_positive_20"] is None
    assert labels["next_open_positive_20"] is None
    assert labels["mfe_ge_10pct_20"] is None
    assert labels["mae_worse_than_7pct_20"] is None
    assert labels["judgement_outcome_class"] == "incomplete"


def test_apply_trader_label_policy_rejects_missing_required_label_inputs() -> None:
    with pytest.raises(ValueError, match="label inputs incomplete"):
        label_policy.apply_trader_label_policy(_benchmark_row(return_close_basis=None))
