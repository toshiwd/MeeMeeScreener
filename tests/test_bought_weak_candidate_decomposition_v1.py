from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts import bought_weak_candidate_decomposition_v1 as mod


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")


def _components(daily_candle: str = "daily_upper_wick_warning", volume: str = "daily_volume_dry") -> str:
    rows = [
        {"feature": "daily_ma_stack", "value": "daily_bull_stack_5_20_60", "points": 3},
        {"feature": "daily_ma60_slope_state", "value": "daily_ma60_rising", "points": 2},
        {"feature": "daily_ret20_state", "value": "daily20_strong_up", "points": 2},
        {"feature": "daily_candle_state", "value": daily_candle, "points": -1 if daily_candle == "daily_upper_wick_warning" else 2},
        {"feature": "daily_volume_state", "value": volume, "points": -1 if volume == "daily_volume_dry" else 1},
        {"feature": "daily_sequence_state", "value": "daily_sequence_bullish", "points": 1},
        {"feature": "weekly_trend_state", "value": "weekly_uptrend", "points": 2},
        {"feature": "weekly_ret4_state", "value": "weekly4_up", "points": 1},
        {"feature": "monthly_trend_state", "value": "monthly_uptrend", "points": 2},
        {"feature": "monthly_ret6_state", "value": "monthly6_up", "points": 1},
    ]
    return json.dumps(rows, ensure_ascii=False)


def _make_run(tmp_path: Path) -> Path:
    root = tmp_path / "run"
    diag = root / "diagnosis_v1"
    diag.mkdir(parents=True)
    _write_json(root / "failure_diagnosis_summary.json", {"primary_failure_mode": "profitable_but_with_risks"})
    _write_json(root / "selection_feature_manifest.json", {"audit_result": "pass"})
    _write_json(root / "no_lookahead_audit.json", {"audit_result": "pass"})
    _write_jsonl(root / "daily_action_ledger.jsonl", [{"decision_ymd": 20250401, "action": "buy"}, {"decision_ymd": 20250401, "action": "reject"}])
    pd.DataFrame(
        [
            {"decision_ymd": 20250401, "code": "7001", "candidate_rank": 1, "selection_score": 10, "selected_for_buy": True, "entry_allowed_by_score": True, "downside_guard_blocked": False, "next_open_available": True, "score_components_json": _components()},
            {"decision_ymd": 20250401, "code": "7002", "candidate_rank": 2, "selection_score": 14, "selected_for_buy": False, "entry_allowed_by_score": True, "downside_guard_blocked": False, "next_open_available": True, "score_components_json": _components("daily_strong_bull", "daily_volume_expansion")},
            {"decision_ymd": 20250402, "code": "7003", "candidate_rank": 1, "selection_score": 15, "selected_for_buy": True, "entry_allowed_by_score": True, "downside_guard_blocked": False, "next_open_available": True, "score_components_json": _components("daily_strong_bull", "daily_volume_expansion")},
        ]
    ).to_csv(root / "daily_candidate_snapshot.csv", index=False)
    pd.DataFrame(
        [
            {"decision_ymd": 20250401, "code": "7002", "candidate_rank": 2, "selection_score": 14, "reject_reason": "max_positions_full"},
        ]
    ).to_csv(root / "rejected_candidates.csv", index=False)
    pd.DataFrame(
        [
            {"decision_ymd": 20250401, "code": "7001", "was_selected": True, "diagnostic_only": True, "post_ret_5": -0.04, "post_ret_20": -0.05, "mae_20": -0.08, "mfe_20": 0.01},
            {"decision_ymd": 20250401, "code": "7002", "was_selected": False, "diagnostic_only": True, "post_ret_5": 0.02, "post_ret_20": 0.18, "mae_20": -0.01, "mfe_20": 0.20},
            {"decision_ymd": 20250402, "code": "7003", "was_selected": True, "diagnostic_only": True, "post_ret_5": 0.03, "post_ret_20": 0.12, "mae_20": -0.01, "mfe_20": 0.15},
        ]
    ).to_csv(root / "post_run_outcome_labels.csv", index=False)
    pd.DataFrame(
        [
            {"order_id": "o1", "decision_ymd": 20250401, "execution_ymd": 20250402, "action": "buy", "code": "7001", "order_status": "filled", "execution_price": 1000.0, "shares": 1000, "notional": 1_000_000.0, "cost_amount": 3000.0, "position_id": "p1"},
        ]
    ).to_csv(root / "orders_ledger.csv", index=False)
    pd.DataFrame(
        [
            {"ymd": 20250402, "position_id": "p1", "code": "7001", "entry_ymd": 20250402, "unrealized_pnl": -50_000.0, "cost_basis": 1_003_000.0, "holding_days": 1},
            {"ymd": 20250408, "position_id": "p1", "code": "7001", "entry_ymd": 20250402, "unrealized_pnl": -80_000.0, "cost_basis": 1_003_000.0, "holding_days": 5},
        ]
    ).to_csv(root / "positions_ledger.csv", index=False)
    pd.DataFrame(
        [
            {"decision_ymd": 20250401, "code": "7001", "was_selected": True, "diagnostic_only": True, "post_ret_5": -0.04, "post_ret_20": -0.05, "mae_20": -0.08, "mfe_20": 0.01, "best_rejected_code": "7002", "best_rejected_post_ret_20": 0.18, "underperformance_vs_best_rejected": 0.23, "candidate_rank": 1, "selection_score": 10, "score_components_json": _components(), "weakness_visible_at_decision_time": True},
        ]
    ).to_csv(diag / "bought_weak_candidate_cases.csv", index=False)
    pd.DataFrame([{"code": "7001", "net_contribution": -50_000.0}, {"code": "7003", "net_contribution": 120_000.0}]).to_csv(diag / "trade_contribution.csv", index=False)
    return root


def test_decomposition_outputs_artifacts_and_pretest_plan(tmp_path: Path) -> None:
    root = _make_run(tmp_path)

    result = mod.run_bought_weak_candidate_decomposition_v1(run_root=root)

    out = root / "bought_weak_candidate_decomposition_v1"
    assert result["complete"] is True
    for artifact in mod.OUTPUT_ARTIFACTS:
        assert (out / artifact).exists(), artifact
    complete = json.loads((out / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    assert complete["replay_rerun"] is False
    assert complete["rule_changed"] is False
    assert complete["selected_veto_candidate_count"] <= 3
    plan = json.loads((out / "next_veto_pretest_plan.json").read_text(encoding="utf-8"))
    assert plan["pretest_policy"]["actual_next_axis_count"] == 1
    enriched = pd.read_csv(out / "weak_buy_cases_enriched.csv")
    assert enriched.iloc[0]["entry_timing_bucket"] == "immediate_adverse_within_5d"
    alternatives = pd.read_csv(out / "weak_buy_same_day_alternatives.csv")
    assert len(alternatives) == 1
