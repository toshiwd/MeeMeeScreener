from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts import weak_buy_volume_normal_veto_pretest_v1 as mod


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")


def _components(volume: str) -> str:
    return json.dumps([{"feature": "daily_volume_state", "value": volume, "points": 0}], ensure_ascii=False)


def _make_run(tmp_path: Path) -> Path:
    root = tmp_path / "run"
    root.mkdir()
    (root / "diagnosis_v1").mkdir()
    (root / "bought_weak_candidate_decomposition_v1").mkdir()
    (root / "trade_reflection_audit_v1").mkdir()
    _write_json(root / "failure_diagnosis_summary.json", {"metrics": {"final_equity": 1_100_000.0}})
    _write_json(root / "selection_feature_manifest.json", {"audit_result": "pass"})
    _write_json(root / "no_lookahead_audit.json", {"audit_result": "pass"})
    _write_json(
        root / "bought_weak_candidate_decomposition_v1" / "weak_buy_veto_candidate_rules.json",
        {
            "candidate_rules": [
                {
                    "rule_id": "feature_1",
                    "description": "daily_volume_state == daily_volume_normal",
                    "rule": {"feature": "daily_volume_state", "op": "eq", "value": "daily_volume_normal"},
                }
            ]
        },
    )
    _write_json(
        root / "trade_reflection_audit_v1" / "reflection_priority_decision.json",
        {"selected_next_axis": "bought_weak_candidate"},
    )
    pd.DataFrame(
        [
            {"decision_ymd": 20250401, "code": "7001", "candidate_rank": 1, "selection_score": 15, "entry_allowed_by_score": True, "downside_guard_blocked": False, "selected_for_buy": True, "next_execution_ymd": 20250402, "close": 1000.0, "next_open_available": True, "score_components_json": _components("daily_volume_normal")},
            {"decision_ymd": 20250401, "code": "7002", "candidate_rank": 2, "selection_score": 14, "entry_allowed_by_score": True, "downside_guard_blocked": False, "selected_for_buy": False, "next_execution_ymd": 20250402, "close": 500.0, "next_open_available": True, "score_components_json": _components("daily_volume_expansion")},
            {"decision_ymd": 20250403, "code": "7003", "candidate_rank": 1, "selection_score": 15, "entry_allowed_by_score": True, "downside_guard_blocked": False, "selected_for_buy": True, "next_execution_ymd": 20250404, "close": 1000.0, "next_open_available": True, "score_components_json": _components("daily_volume_expansion")},
        ]
    ).to_csv(root / "daily_candidate_snapshot.csv", index=False)
    pd.DataFrame(
        [
            {"order_id": "o1", "decision_ymd": 20250401, "execution_ymd": 20250402, "action": "buy", "code": "7001", "order_status": "filled", "execution_price": 1010.0, "shares": 100, "notional": 101000.0, "cost_amount": 303.0, "position_id": "p1", "reason_type": "entry_score_passed"},
            {"order_id": "o2", "decision_ymd": 20250405, "execution_ymd": 20250406, "action": "stop", "code": "7001", "order_status": "filled", "execution_price": 900.0, "shares": 100, "notional": 90000.0, "cost_amount": 270.0, "position_id": "p1", "reason_type": "stop_loss", "realized_pnl": -11573.0, "realized_return": -0.114},
            {"order_id": "o3", "decision_ymd": 20250403, "execution_ymd": 20250404, "action": "buy", "code": "7003", "order_status": "filled", "execution_price": 1000.0, "shares": 100, "notional": 100000.0, "cost_amount": 300.0, "position_id": "p2", "reason_type": "entry_score_passed"},
            {"order_id": "o4", "decision_ymd": 20250408, "execution_ymd": 20250409, "action": "exit", "code": "7003", "order_status": "filled", "execution_price": 1100.0, "shares": 100, "notional": 110000.0, "cost_amount": 330.0, "position_id": "p2", "reason_type": "profit_target", "realized_pnl": 9370.0, "realized_return": 0.093},
        ]
    ).to_csv(root / "orders_ledger.csv", index=False)
    pd.DataFrame(
        [
            {"ymd": 20250402, "position_id": "p1", "code": "7001", "entry_ymd": 20250402, "close_price": 1000.0, "market_value": 100000.0, "cost_basis": 101303.0, "unrealized_pnl": -1303.0, "holding_days": 1},
            {"ymd": 20250404, "position_id": "p2", "code": "7003", "entry_ymd": 20250404, "close_price": 1000.0, "market_value": 100000.0, "cost_basis": 100300.0, "unrealized_pnl": -300.0, "holding_days": 1},
        ]
    ).to_csv(root / "positions_ledger.csv", index=False)
    pd.DataFrame(
        [
            {"ymd": 20250401, "equity": 1_000_000.0},
            {"ymd": 20250406, "equity": 988_427.0},
            {"ymd": 20250409, "equity": 1_097_797.0},
        ]
    ).to_csv(root / "equity_curve.csv", index=False)
    pd.DataFrame(
        [
            {"decision_ymd": 20250401, "code": "7001", "was_selected": True, "diagnostic_only": True, "post_ret_20": -0.10, "mae_20": -0.12, "mfe_20": 0.01, "outcome_bucket": "loser"},
            {"decision_ymd": 20250401, "code": "7002", "was_selected": False, "diagnostic_only": True, "post_ret_20": 0.10, "mae_20": -0.02, "mfe_20": 0.12, "outcome_bucket": "winner"},
            {"decision_ymd": 20250403, "code": "7003", "was_selected": True, "diagnostic_only": True, "post_ret_20": 0.10, "mae_20": -0.02, "mfe_20": 0.12, "outcome_bucket": "winner"},
        ]
    ).to_csv(root / "post_run_outcome_labels.csv", index=False)
    pd.DataFrame(
        [
            {"decision_ymd": 20250401, "code": "7001", "baseline_contribution": -11573.0},
        ]
    ).to_csv(root / "bought_weak_candidate_cases.csv", index=False)
    return root


def test_volume_normal_veto_pretest_outputs_required_artifacts(tmp_path: Path) -> None:
    root = _make_run(tmp_path)

    result = mod.run_weak_buy_volume_normal_veto_pretest_v1(root)

    out = root / "weak_buy_volume_normal_veto_pretest_v1"
    assert result["complete"] is True
    for artifact in mod.OUTPUT_ARTIFACTS:
        assert (out / artifact).exists(), artifact
    complete = json.loads((out / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    assert complete["single_axis_only"] is True
    assert complete["replay_rerun"] is False
    assert complete["post_run_outcome_used_for_selection"] is False
    assert complete["research_fallback_recorded"] is True
    manifest = json.loads((out / "selection_feature_manifest.json").read_text(encoding="utf-8"))
    assert manifest["audit_result"] == "pass"
    assert "post_ret_20" in manifest["selection_forbidden_columns"]
    audit = json.loads((out / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    assert audit["post_run_outcomes_used_for_selection"] is False
    vetoed = pd.read_csv(out / "vetoed_baseline_buys.csv")
    assert len(vetoed) == 1
    replacements = pd.read_csv(out / "replacement_candidates.csv")
    assert replacements.iloc[0]["replacement_code"] == 7002
    comparison = json.loads((out / "baseline_vs_veto_comparison.json").read_text(encoding="utf-8"))
    assert comparison["metrics"]["vetoed_buy_count"] == 1
    assert comparison["metrics"]["weak_buy_vetoed_count"] == 1
    decision = json.loads((out / "next_axis_decision.json").read_text(encoding="utf-8"))
    assert decision["decision"] in mod.DECISIONS
    assert decision["decision_count"] == 1
