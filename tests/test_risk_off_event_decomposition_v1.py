from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts import risk_off_event_decomposition_v1 as mod


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")


def _make_fixture(tmp_path: Path) -> Path:
    gate = tmp_path / "baseline_gate"
    risk = gate / "risk_off_cash_control_pretest_v1"
    risk.mkdir(parents=True)
    _write_json(
        risk / "risk_off_cash_control_summary.json",
        {
            "decision": "hold_due_to_upside_damage",
            "reason_type": "drawdown_improved_but_2024_2025_upside_damaged",
            "robustness_root": str(gate),
        },
    )
    pd.DataFrame(
        [
            {"year": 2024, "event_id": "2024-risk-off-001", "event_type": "risk_off", "decision_ymd": 20240103, "execution_ymd": 20240104, "trailing_dd": -0.08, "open_position_count": 3, "trim_ratio": 0.5, "lookback_days": 20},
            {"year": 2024, "event_id": "2024-risk-off-001", "event_type": "risk_on", "decision_ymd": 20240108, "trailing_dd": -0.02, "risk_off_days": 3, "release_reason": "drawdown_recovered"},
            {"year": 2021, "event_id": "2021-risk-off-001", "event_type": "risk_off", "decision_ymd": 20210103, "execution_ymd": 20210104, "trailing_dd": -0.09, "open_position_count": 3, "trim_ratio": 0.5, "lookback_days": 20},
            {"year": 2021, "event_id": "2021-risk-off-001", "event_type": "risk_on", "decision_ymd": 20210108, "trailing_dd": -0.02, "risk_off_days": 3, "release_reason": "drawdown_recovered"},
        ]
    ).to_csv(risk / "risk_off_events.csv", index=False)
    pd.DataFrame(
        [
            {"year": 2024, "ymd": 20240103, "cash": 1_000_000, "positions_market_value": 9_000_000, "equity": 10_000_000, "baseline_equity": 10_000_000, "market_benchmark_equity": 10_000_000, "risk_off_active": False},
            {"year": 2024, "ymd": 20240104, "cash": 5_000_000, "positions_market_value": 5_000_000, "equity": 10_000_000, "baseline_equity": 10_000_000, "market_benchmark_equity": 10_000_000, "risk_off_active": True},
            {"year": 2024, "ymd": 20240105, "cash": 5_000_000, "positions_market_value": 5_200_000, "equity": 10_200_000, "baseline_equity": 11_000_000, "market_benchmark_equity": 10_300_000, "risk_off_active": True},
            {"year": 2024, "ymd": 20240108, "cash": 5_000_000, "positions_market_value": 5_400_000, "equity": 10_400_000, "baseline_equity": 12_000_000, "market_benchmark_equity": 10_600_000, "risk_off_active": True},
            {"year": 2021, "ymd": 20210103, "cash": 1_000_000, "positions_market_value": 9_000_000, "equity": 10_000_000, "baseline_equity": 10_000_000, "market_benchmark_equity": 10_000_000, "risk_off_active": False},
            {"year": 2021, "ymd": 20210104, "cash": 5_000_000, "positions_market_value": 5_000_000, "equity": 10_000_000, "baseline_equity": 10_000_000, "market_benchmark_equity": 10_000_000, "risk_off_active": True},
            {"year": 2021, "ymd": 20210105, "cash": 5_000_000, "positions_market_value": 4_900_000, "equity": 9_900_000, "baseline_equity": 9_000_000, "market_benchmark_equity": 9_700_000, "risk_off_active": True},
            {"year": 2021, "ymd": 20210108, "cash": 5_000_000, "positions_market_value": 4_800_000, "equity": 9_800_000, "baseline_equity": 8_000_000, "market_benchmark_equity": 9_600_000, "risk_off_active": True},
        ]
    ).to_csv(risk / "risk_off_equity_curve_by_year.csv", index=False)
    pd.DataFrame(
        [
            {"year": 2024, "order_id": "trim1", "decision_ymd": 20240103, "execution_ymd": 20240104, "action": "risk_off_trim", "code": "1001", "order_status": "filled", "notional": 500, "realized_pnl": -10, "event_id": "2024-risk-off-001"},
            {"year": 2024, "order_id": "blocked1", "decision_ymd": 20240104, "execution_ymd": 20240105, "action": "buy", "code": "1002", "order_status": "unfilled", "unfilled_reason": "risk_off_or_position_limit"},
            {"year": 2021, "order_id": "trim2", "decision_ymd": 20210103, "execution_ymd": 20210104, "action": "risk_off_trim", "code": "1001", "order_status": "filled", "notional": 500, "realized_pnl": -10, "event_id": "2021-risk-off-001"},
        ]
    ).to_csv(risk / "risk_off_orders_ledger.csv", index=False)
    for year in (2021, 2024):
        run = gate / "subruns" / f"{year}-baseline-portfolio_agent_replay_v1"
        run.mkdir(parents=True)
        pd.DataFrame(
            [
                {"decision_ymd": year * 10000 + 104, "code": "1001", "candidate_rank": 1, "selection_score": 10, "selected_for_buy": True},
                {"decision_ymd": year * 10000 + 104, "code": "1002", "candidate_rank": 2, "selection_score": 9, "selected_for_buy": False},
            ]
        ).to_csv(run / "daily_candidate_snapshot.csv", index=False)
        pd.DataFrame(
            [
                {"decision_ymd": year * 10000 + 104, "code": "1001", "post_ret_20": 0.02, "mfe_20": 0.04},
                {"decision_ymd": year * 10000 + 104, "code": "1002", "post_ret_20": 0.12, "mfe_20": 0.20},
            ]
        ).to_csv(run / "post_run_outcome_labels.csv", index=False)
    return risk


def test_risk_off_event_decomposition_outputs_required_artifacts(tmp_path: Path) -> None:
    risk = _make_fixture(tmp_path)

    result = mod.run_event_decomposition(risk)

    out = risk / "risk_off_event_decomposition_v1"
    assert result["complete"] is True
    for artifact in mod.REQUIRED_ARTIFACTS:
        assert (out / artifact).exists(), artifact
    complete = json.loads((out / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    assert complete["required_artifacts_all_present"] is True
    assert complete["rule_changed"] is False
    assert complete["threshold_changed"] is False
    assert complete["sweep_used"] is False

    cases = pd.read_csv(out / "risk_off_event_cases.csv")
    assert len(cases) == 2
    classes = set(cases["event_class"])
    assert "recovery_year_damage" in classes
    assert "saved_loss" in classes

    candidates = pd.read_csv(out / "risk_off_candidate_context.csv")
    assert candidates["candidate_context_status"].eq("available").all()
    assert candidates["missed_big_winner_top10_count"].sum() >= 2

    decision = json.loads((out / "next_axis_decision.json").read_text(encoding="utf-8"))
    assert decision["decision"] in mod.DECISIONS
    assert decision["decision_count"] == 1
