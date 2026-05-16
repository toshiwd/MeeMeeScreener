from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts import portfolio_agent_replay_diagnosis_v1 as diag


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")


def _make_run_root(tmp_path: Path) -> Path:
    root = tmp_path / "run"
    root.mkdir()
    _write_json(
        root / "run_config.json",
        {
            "portfolio": {"initial_cash_jpy": 10_000_000.0},
            "period": {"start_ymd": 20250401, "end_ymd": 20250408},
        },
    )
    _write_json(
        root / "failure_diagnosis_summary.json",
        {
            "metrics": {"initial_cash": 10_000_000.0, "final_equity": 10_800_000.0},
            "benchmark": {"benchmark_status": "available", "benchmark_code": "1306"},
        },
    )
    _write_json(root / "no_lookahead_audit.json", {"audit_result": "pass"})
    _write_json(root / "selection_feature_manifest.json", {"audit_result": "pass"})
    _write_json(root / "_ARTIFACT_COMPLETE.json", {"complete": True, "required_artifacts_all_present": True, "accounting_reconciliation": {"status": "pass"}})
    pd.DataFrame(
        [
            {"ymd": 20250401, "cash": 10_000_000.0, "positions_market_value": 0.0, "equity": 10_000_000.0, "market_benchmark_equity": 10_000_000.0, "benchmark_status": "available", "open_position_count": 0},
            {"ymd": 20250402, "cash": 6_990_000.0, "positions_market_value": 3_200_000.0, "equity": 10_190_000.0, "market_benchmark_equity": 10_100_000.0, "benchmark_status": "available", "open_position_count": 1},
            {"ymd": 20250408, "cash": 10_800_000.0, "positions_market_value": 0.0, "equity": 10_800_000.0, "market_benchmark_equity": 10_200_000.0, "benchmark_status": "available", "open_position_count": 0},
        ]
    ).to_csv(root / "equity_curve.csv", index=False)
    pd.DataFrame(
        [
            {"order_id": "o1", "decision_ymd": 20250401, "execution_ymd": 20250402, "action": "buy", "code": "7001", "order_status": "filled", "execution_price": 1000.0, "shares": 3000, "notional": 3_000_000.0, "cost_amount": 9_000.0, "position_id": "p1"},
            {"order_id": "o2", "decision_ymd": 20250407, "execution_ymd": 20250408, "action": "exit", "code": "7001", "order_status": "filled", "execution_price": 1270.0, "shares": 3000, "notional": 3_810_000.0, "cost_amount": 11_430.0, "position_id": "p1", "realized_pnl": 789_570.0, "realized_return": 0.262},
        ]
    ).to_csv(root / "orders_ledger.csv", index=False)
    pd.DataFrame(
        [
            {"ymd": 20250402, "position_id": "p1", "code": "7001", "shares": 3000, "entry_ymd": 20250402, "entry_price": 1000.0, "close_price": 1040.0, "market_value": 3_120_000.0, "cost_basis": 3_009_000.0, "unrealized_pnl": 111_000.0, "holding_days": 1},
            {"ymd": 20250407, "position_id": "p1", "code": "7001", "shares": 3000, "entry_ymd": 20250402, "entry_price": 1000.0, "close_price": 1260.0, "market_value": 3_780_000.0, "cost_basis": 3_009_000.0, "unrealized_pnl": 771_000.0, "holding_days": 4},
        ]
    ).to_csv(root / "positions_ledger.csv", index=False)
    pd.DataFrame(
        [
            {"decision_ymd": 20250401, "code": "7001", "candidate_rank": 1, "selection_score": 14, "selected_for_buy": True, "score_components_json": "[]"},
            {"decision_ymd": 20250401, "code": "7002", "candidate_rank": 2, "selection_score": 13, "selected_for_buy": False, "score_components_json": "[]"},
        ]
    ).to_csv(root / "daily_candidate_snapshot.csv", index=False)
    pd.DataFrame(
        [
            {"decision_ymd": 20250401, "code": "7002", "candidate_rank": 2, "selection_score": 13, "reject_reason": "max_positions_full"},
        ]
    ).to_csv(root / "rejected_candidates.csv", index=False)
    pd.DataFrame(
        [
            {"decision_ymd": 20250401, "code": "7001", "was_selected": True, "diagnostic_only": True, "post_ret_20": 0.02, "mae_20": -0.01, "mfe_20": 0.05},
            {"decision_ymd": 20250401, "code": "7002", "was_selected": False, "diagnostic_only": True, "post_ret_20": 0.18, "mae_20": -0.02, "mfe_20": 0.22},
        ]
    ).to_csv(root / "post_run_outcome_labels.csv", index=False)
    _write_jsonl(root / "daily_action_ledger.jsonl", [{"decision_ymd": 20250401, "action": "buy"}, {"decision_ymd": 20250401, "action": "reject"}])
    return root


def test_diagnosis_emits_required_artifacts_and_single_axis(tmp_path: Path) -> None:
    root = _make_run_root(tmp_path)

    result = diag.run_portfolio_agent_replay_diagnosis_v1(run_root=root)

    assert result["complete"] is True
    assert result["selected_next_axis"] in diag.NEXT_AXIS_CANDIDATES
    out = root / "diagnosis_v1"
    for artifact in diag.OUTPUT_ARTIFACTS:
        assert (out / artifact).exists(), artifact
    complete = json.loads((out / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    assert complete["next_axis_selected_count"] == 1
    assert complete["rerun_performed"] is False
    assert complete["conditions_changed"] is False
    summary = json.loads((out / "replay_diagnosis_summary.json").read_text(encoding="utf-8"))
    assert summary["benchmark_comparison"]["excess_return"] > 0.0
    missed = pd.read_csv(out / "missed_winner_cases.csv")
    assert len(missed) == 1
    replay_complete = json.loads((root / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    assert replay_complete.get("axis_id") != diag.AXIS_ID
