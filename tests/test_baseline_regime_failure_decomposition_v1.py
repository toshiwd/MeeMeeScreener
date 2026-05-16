from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts import baseline_regime_failure_decomposition_v1 as mod


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")


def _make_run(root: Path, year: int) -> None:
    root.mkdir(parents=True)
    pd.DataFrame(
        [
            {"ymd": year * 10000 + 101, "cash": 10_000_000, "positions_market_value": 0, "equity": 10_000_000, "market_benchmark_equity": 10_000_000, "open_position_count": 0},
            {"ymd": year * 10000 + 131, "cash": 500_000, "positions_market_value": 8_500_000, "equity": 9_000_000, "market_benchmark_equity": 10_500_000, "open_position_count": 3},
        ]
    ).to_csv(root / "equity_curve.csv", index=False)
    pd.DataFrame(
        [
            {"decision_ymd": year * 10000 + 101, "code": "1001", "candidate_rank": 1, "selection_score": 12, "selected_for_buy": True},
            {"decision_ymd": year * 10000 + 101, "code": "1002", "candidate_rank": 2, "selection_score": 11, "selected_for_buy": False},
        ]
    ).to_csv(root / "daily_candidate_snapshot.csv", index=False)
    pd.DataFrame(
        [
            {"decision_ymd": year * 10000 + 101, "code": "1001", "post_ret_20": -0.10},
            {"decision_ymd": year * 10000 + 101, "code": "1002", "post_ret_20": 0.10},
        ]
    ).to_csv(root / "post_run_outcome_labels.csv", index=False)
    pd.DataFrame(
        [
            {"action": "buy", "order_status": "filled", "realized_pnl": None, "realized_return": None},
            {"action": "stop", "order_status": "filled", "realized_pnl": -100_000, "realized_return": -0.10},
        ]
    ).to_csv(root / "orders_ledger.csv", index=False)


def test_baseline_regime_failure_decomposition_outputs(tmp_path: Path) -> None:
    gate = tmp_path / "gate"
    run = gate / "subruns" / "2019-baseline-portfolio_agent_replay_v1"
    _make_run(run, 2019)
    pd.DataFrame(
        [
            {
                "year": 2019,
                "run_dir": str(run),
                "final_equity": 9_000_000,
                "total_return": -0.10,
                "benchmark_return": 0.05,
                "excess_return": -0.15,
                "max_drawdown": -0.10,
                "order_count": 2,
                "win_rate": 0,
                "stop_count": 1,
                "exit_count": 0,
                "primary_failure_mode": "missed_winner",
            }
        ]
    ).to_csv(gate / "yearly_results.csv", index=False)

    result = mod.run_decomposition(gate)

    out = gate / "baseline_regime_failure_decomposition_v1"
    assert result["complete"] is True
    for artifact in mod.REQUIRED_ARTIFACTS:
        assert (out / artifact).exists(), artifact
    complete = json.loads((out / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    assert complete["required_artifacts_all_present"] is True
    assert complete["policy_changed"] is False
    decision = json.loads((out / "next_axis_decision.json").read_text(encoding="utf-8"))
    assert decision["decision_count"] == 1
    yearly = pd.read_csv(out / "yearly_failure_decomposition.csv")
    assert yearly.iloc[0]["bought_vs_rejected_gap"] < 0
