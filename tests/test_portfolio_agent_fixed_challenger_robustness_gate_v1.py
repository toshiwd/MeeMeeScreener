from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts import portfolio_agent_fixed_challenger_robustness_gate_v1 as mod


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")


def _fake_subrun(root: Path, year: int, total_return: float, max_drawdown: float, benchmark_return: float) -> None:
    root.mkdir(parents=True, exist_ok=True)
    final_equity = 10_000_000.0 * (1.0 + total_return)
    pd.DataFrame(
        [
            {"ymd": year * 10000 + 101, "equity": 10_000_000.0},
            {"ymd": year * 10000 + 630, "equity": 10_000_000.0 * (1.0 + max_drawdown)},
            {"ymd": year * 10000 + 1230, "equity": final_equity},
        ]
    ).to_csv(root / "equity_curve.csv", index=False)
    pd.DataFrame(
        [
            {"action": "buy"},
            {"action": "stop"},
            {"action": "exit"},
        ]
    ).to_csv(root / "orders_ledger.csv", index=False)
    _write_json(
        root / "failure_diagnosis_summary.json",
        {
            "primary_failure_mode": "missed_winner",
            "secondary_risks": ["missed_winner"],
            "benchmark": {"benchmark_status": "available", "benchmark_code": "1306", "market_benchmark_total_return": benchmark_return},
            "metrics": {
                "final_equity": final_equity,
                "total_return": total_return,
                "max_drawdown": max_drawdown,
                "win_rate": 0.25,
                "missed_winner_count": 1,
                "bought_weak_candidate_count": 1,
                "profit_factor": 0.5,
                "cost_return_drag": 0.01,
            },
        },
    )
    _write_json(
        root / "_ARTIFACT_COMPLETE.json",
        {
            "required_artifacts_all_present": True,
            "critical_logs_non_empty": True,
            "no_lookahead_audit": "pass",
            "accounting_reconciliation": {"status": "pass"},
        },
    )


def test_robustness_gate_outputs_drop_for_severe_year(tmp_path: Path, monkeypatch) -> None:
    years = [2019, 2020, 2021]
    monkeypatch.setattr(mod, "PERIODS", tuple((year, year * 10000 + 101, (year + 1) * 10000 + 101) for year in years))

    def fake_run_c1506_fixed_replay(**kwargs):
        run_id = kwargs["run_id"]
        year = int(str(run_id).split("-")[0])
        out = Path(kwargs["output_root"]) / f"{run_id}-portfolio_agent_replay_v1"
        if year == 2021:
            _fake_subrun(out, year, -0.52, -0.54, 0.11)
        else:
            _fake_subrun(out, year, 0.12, -0.10, 0.08)
        return {"output_dir": str(out), "final_equity": 1.0, "total_return": 0.0}

    monkeypatch.setattr(mod.c1506, "run_c1506_fixed_replay", fake_run_c1506_fixed_replay)

    result = mod.run_robustness_gate(output_root=tmp_path, gate_id="test-gate")

    out = tmp_path / "test-gate"
    assert result["decision"] == "drop_due_to_severe_drawdown"
    for artifact in mod.REQUIRED_ARTIFACTS:
        assert (out / artifact).exists(), artifact
    decision = json.loads((out / "robustness_decision.json").read_text(encoding="utf-8"))
    assert decision["decision_count"] == 1
    assert decision["policy_promotion_allowed"] is False
    yearly = pd.read_csv(out / "yearly_results.csv")
    assert len(yearly) == 3
