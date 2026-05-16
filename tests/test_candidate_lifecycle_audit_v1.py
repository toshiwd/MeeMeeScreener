from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts import candidate_lifecycle_audit_v1 as mod


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")


def _components(candle: str = "daily_strong_bull", volume: str = "daily_volume_normal", stack: str = "daily_bull_stack_5_20_60") -> str:
    return json.dumps(
        [
            {"feature": "daily_ma_stack", "value": stack, "points": 3},
            {"feature": "daily_ma60_slope_state", "value": "daily_ma60_rising", "points": 2},
            {"feature": "daily_ret20_state", "value": "daily20_strong_up", "points": 2},
            {"feature": "daily_candle_state", "value": candle, "points": 2 if candle == "daily_strong_bull" else -2},
            {"feature": "daily_volume_state", "value": volume, "points": 1},
            {"feature": "daily_sequence_state", "value": "daily_sequence_bullish", "points": 1},
            {"feature": "weekly_trend_state", "value": "weekly_uptrend", "points": 2},
            {"feature": "weekly_ret4_state", "value": "weekly4_up", "points": 1},
            {"feature": "monthly_trend_state", "value": "monthly_uptrend", "points": 2},
            {"feature": "monthly_ret6_state", "value": "monthly6_up", "points": 1},
        ],
        ensure_ascii=False,
    )


def _make_run(tmp_path: Path) -> Path:
    root = tmp_path / "run"
    decomp = root / "bought_weak_candidate_decomposition_v1"
    decomp.mkdir(parents=True)
    _write_jsonl(
        root / "daily_action_ledger.jsonl",
        [{"decision_ymd": 20250401, "action": "buy", "code": "7001"}],
    )
    pd.DataFrame(
        [
            {"order_id": "o1", "decision_ymd": 20250401, "execution_ymd": 20250402, "action": "buy", "code": "7001", "order_status": "filled", "execution_price": 1000.0, "shares": 100, "notional": 100000.0, "cost_amount": 300.0, "position_id": "p1", "reason_type": "entry_score_passed"},
            {"order_id": "o2", "decision_ymd": 20250408, "execution_ymd": 20250409, "action": "stop", "code": "7001", "order_status": "filled", "execution_price": 900.0, "shares": 100, "notional": 90000.0, "cost_amount": 270.0, "position_id": "p1", "reason_type": "stop_loss", "realized_pnl": -10570.0, "realized_return": -0.105},
        ]
    ).to_csv(root / "orders_ledger.csv", index=False)
    pd.DataFrame(
        [
            {"ymd": 20250402, "position_id": "p1", "code": "7001", "shares": 100, "entry_ymd": 20250402, "entry_price": 1000.0, "close_price": 1000.0, "market_value": 100000.0, "cost_basis": 100300.0, "unrealized_pnl": -300.0, "holding_days": 1},
            {"ymd": 20250403, "position_id": "p1", "code": "7001", "shares": 100, "entry_ymd": 20250402, "entry_price": 1000.0, "close_price": 930.0, "market_value": 93000.0, "cost_basis": 100300.0, "unrealized_pnl": -7300.0, "holding_days": 2},
            {"ymd": 20250404, "position_id": "p1", "code": "7001", "shares": 100, "entry_ymd": 20250402, "entry_price": 1000.0, "close_price": 970.0, "market_value": 97000.0, "cost_basis": 100300.0, "unrealized_pnl": -3300.0, "holding_days": 3},
            {"ymd": 20250407, "position_id": "p1", "code": "7001", "shares": 100, "entry_ymd": 20250402, "entry_price": 1000.0, "close_price": 1020.0, "market_value": 102000.0, "cost_basis": 100300.0, "unrealized_pnl": 1700.0, "holding_days": 4},
        ]
    ).to_csv(root / "positions_ledger.csv", index=False)
    pd.DataFrame(
        [
            {"ymd": 20250401, "equity": 1_000_000.0},
            {"ymd": 20250409, "equity": 989_430.0},
        ]
    ).to_csv(root / "equity_curve.csv", index=False)
    pd.DataFrame(
        [
            {"decision_ymd": 20250401, "code": "7001", "candidate_rank": 1, "selection_score": 15, "entry_allowed_by_score": True, "downside_guard_blocked": False, "selected_for_buy": True, "next_execution_ymd": 20250402, "close": 1000.0, "next_open_available": True, "score_components_json": _components()},
            {"decision_ymd": 20250402, "code": "7001", "candidate_rank": 1, "selection_score": 15, "entry_allowed_by_score": True, "downside_guard_blocked": False, "selected_for_buy": False, "next_execution_ymd": 20250403, "close": 1000.0, "next_open_available": True, "score_components_json": _components()},
            {"decision_ymd": 20250403, "code": "7001", "candidate_rank": 2, "selection_score": 13, "entry_allowed_by_score": True, "downside_guard_blocked": False, "selected_for_buy": False, "next_execution_ymd": 20250404, "close": 930.0, "next_open_available": True, "score_components_json": _components("daily_strong_bear", "daily_volume_expansion")},
            {"decision_ymd": 20250404, "code": "7001", "candidate_rank": 8, "selection_score": 12, "entry_allowed_by_score": True, "downside_guard_blocked": False, "selected_for_buy": False, "next_execution_ymd": 20250407, "close": 970.0, "next_open_available": True, "score_components_json": _components("daily_lower_wick_bull")},
        ]
    ).to_csv(root / "daily_candidate_snapshot.csv", index=False)
    weak = pd.DataFrame(
        [
            {"decision_ymd": 20250401, "code": "7001", "was_selected": True, "diagnostic_only": True, "post_ret_20": -0.05, "mae_20": -0.10, "mfe_20": 0.03, "outcome_bucket": "loser", "candidate_rank": 1, "selection_score": 15, "score_components_json": _components(), "weakness_visible_at_decision_time": False},
        ]
    )
    weak.to_csv(root / "bought_weak_candidate_cases.csv", index=False)
    enriched = weak.copy()
    enriched["order_id"] = "o1"
    enriched["position_id"] = "p1"
    enriched["entry_execution_ymd"] = 20250402
    enriched["execution_price"] = 1000.0
    enriched["shares"] = 100
    enriched["notional"] = 100000.0
    enriched["cost_amount"] = 300.0
    enriched["entry_timing_bucket"] = "delayed_breakdown_after_initial_hold"
    enriched["reason_codes"] = "daily_candle_state=daily_strong_bull"
    enriched.to_csv(decomp / "weak_buy_cases_enriched.csv", index=False)
    pd.DataFrame(
        [
            {"decision_ymd": 20250401, "code": "7001", "was_selected": True, "diagnostic_only": True, "post_ret_20": -0.05, "mae_20": -0.10, "mfe_20": 0.03, "outcome_bucket": "loser"},
        ]
    ).to_csv(root / "post_run_outcome_labels.csv", index=False)
    pd.DataFrame([{"code": "7001", "net_contribution": -10570.0}]).to_csv(root / "trade_contribution.csv", index=False)
    return root


def test_candidate_lifecycle_audit_outputs_invalidation_artifacts(tmp_path: Path) -> None:
    root = _make_run(tmp_path)

    result = mod.run_candidate_lifecycle_audit_v1(root)

    out = root / "candidate_lifecycle_audit_v1"
    assert result["complete"] is True
    for artifact in mod.OUTPUT_ARTIFACTS:
        assert (out / artifact).exists(), artifact
    complete = json.loads((out / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    assert complete["replay_rerun"] is False
    assert complete["rule_changed"] is False
    assert complete["daily_volume_normal_veto_revived"] is False
    lifecycle = pd.read_csv(out / "bought_candidate_lifecycle.csv")
    assert lifecycle.iloc[0]["first_invalidation_type"] == "big_bearish_candle"
    invalidations = pd.read_csv(out / "invalidation_candle_cases.csv")
    assert invalidations.iloc[0]["estimate_status"] == "next_holding_close_proxy_not_next_open"
    escape = pd.read_csv(out / "buy_to_escape_transition_cases.csv")
    assert len(escape) == 1
    recovery = pd.read_csv(out / "false_invalidation_recovery_cases.csv")
    assert len(recovery) == 1
    decision = json.loads((out / "lifecycle_next_axis_decision.json").read_text(encoding="utf-8"))
    assert decision["selected_next_axis"] in mod.NEXT_AXIS_CANDIDATES
    assert decision["selected_next_axis_count"] == 1
