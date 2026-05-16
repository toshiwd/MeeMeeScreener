from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts import candidate_family_failure_map_v1 as mod


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")


def _components(**values: str) -> str:
    return json.dumps([{"feature": key, "value": value} for key, value in values.items()], ensure_ascii=False)


def _subrun(root: Path, year: int) -> None:
    root.mkdir(parents=True, exist_ok=True)
    rows = []
    outcomes = []
    for idx in range(8):
        ymd = year * 10000 + 101 + idx
        code = str(7000 + idx)
        family_a = idx < 4
        selected = idx in {0, 1, 4, 5}
        if family_a:
            components = _components(
                daily_ret20_state="daily20_strong_up",
                daily_candle_state="daily_strong_bull",
                daily_volume_state="daily_volume_expansion",
                daily_sequence_state="daily_sequence_up",
                daily_ma_stack="daily_bull_stack_5_20_60",
                daily_ma60_slope_state="daily_ma60_rising",
                weekly_trend_state="weekly_uptrend",
                weekly_ret4_state="weekly4_up",
                monthly_trend_state="monthly_uptrend",
                monthly_ret6_state="monthly6_up",
            )
            post_ret = 0.08 if selected else 0.04
            mae = -0.03
        else:
            components = _components(
                daily_ret20_state="daily20_down",
                daily_candle_state="daily_bearish",
                daily_volume_state="daily_volume_expansion",
                daily_sequence_state="daily_sequence_mixed",
                daily_ma_stack="daily_mixed_stack",
                daily_ma60_slope_state="daily_ma60_flat",
                weekly_trend_state="weekly_downtrend",
                weekly_ret4_state="weekly4_down",
                monthly_trend_state="monthly_downtrend",
                monthly_ret6_state="monthly6_down",
            )
            post_ret = -0.09 if selected else -0.04
            mae = -0.16
        rows.append(
            {
                "decision_ymd": ymd,
                "code": code,
                "candidate_rank": idx + 1,
                "selection_score": 18 - idx,
                "selected_for_buy": selected,
                "score_components_json": components,
            }
        )
        outcomes.append({"decision_ymd": ymd, "code": code, "post_ret_20": post_ret, "mae_20": mae, "mfe_20": max(post_ret, 0.01)})
    pd.DataFrame(rows).to_csv(root / "daily_candidate_snapshot.csv", index=False)
    pd.DataFrame(outcomes).to_csv(root / "post_run_outcome_labels.csv", index=False)


def test_candidate_family_map_writes_required_artifacts_and_one_decision(tmp_path: Path) -> None:
    gate = tmp_path / "gate"
    run_2024 = gate / "2024-run"
    run_2025 = gate / "2025-run"
    _subrun(run_2024, 2024)
    _subrun(run_2025, 2025)
    pd.DataFrame(
        [
            {"year": 2024, "run_dir": str(run_2024)},
            {"year": 2025, "run_dir": str(run_2025)},
        ]
    ).to_csv(gate / "yearly_results.csv", index=False)
    (gate / "baseline_regime_failure_decomposition_v1").mkdir(parents=True)
    pd.DataFrame(
        [
            {"year": 2024, "month": 202401, "regime_bucket": "benchmark_up", "portfolio_return": 0.1, "benchmark_return": 0.05, "excess_return": 0.05},
            {"year": 2025, "month": 202501, "regime_bucket": "benchmark_down", "portfolio_return": -0.1, "benchmark_return": -0.05, "excess_return": -0.05},
        ]
    ).to_csv(gate / "baseline_regime_failure_decomposition_v1" / "monthly_failure_decomposition.csv", index=False)

    result = mod.run_family_map(gate)
    out = Path(result["output_root"])

    assert result["decision"] in mod.DECISIONS
    for artifact in mod.REQUIRED_ARTIFACTS:
        assert (out / artifact).exists(), artifact
    complete = json.loads((out / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    assert complete["required_artifacts_all_present"] is True
    assert complete["silent_fallback_used"] is False
    assert complete["candidate_generation_changed"] is False
    decision = json.loads((out / "next_axis_decision.json").read_text(encoding="utf-8"))
    assert decision["decision_count"] == 1
    assert decision["policy_promotion_allowed"] is False
    mapped = pd.read_csv(out / "candidate_family_map.csv")
    assert "breakout_continuation" in set(mapped["candidate_family"])
    assert "weak_reversal" in set(mapped["candidate_family"])


def test_family_classification_ignores_future_outcome_columns() -> None:
    base = pd.Series(
        {
            "selection_score": 18,
            "score_components_json": _components(
                daily_ret20_state="daily20_strong_up",
                daily_candle_state="daily_strong_bull",
                daily_volume_state="daily_volume_expansion",
                daily_sequence_state="daily_sequence_up",
                daily_ma_stack="daily_bull_stack_5_20_60",
                daily_ma60_slope_state="daily_ma60_rising",
                weekly_trend_state="weekly_uptrend",
                weekly_ret4_state="weekly4_up",
                monthly_trend_state="monthly_uptrend",
                monthly_ret6_state="monthly6_up",
            ),
        }
    )
    with_future = base.copy()
    with_future["post_ret_20"] = -0.9
    with_future["mae_20"] = -0.9
    with_future["mfe_20"] = 0.9
    assert mod._family_from_components(base) == mod._family_from_components(with_future)
