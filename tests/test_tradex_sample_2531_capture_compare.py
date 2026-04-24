from __future__ import annotations

import json

import pandas as pd

from scripts.tradex_sample_2531_capture_compare import (
    ChallengerState,
    _apply_challenger_action,
    _challenger_select_action,
    _compute_interval_metrics,
    _policy_aggregate_summary,
)


def _context(
    *,
    score_long: float,
    score_short: float,
    market_regime: str,
    daily_state: str,
    continuation: bool = False,
) -> dict[str, object]:
    return {
        "score_long": score_long,
        "score_short": score_short,
        "marketRegime": market_regime,
        "daily_main_state_ctx": daily_state,
        "reclaim60": continuation,
        "v60Core": continuation,
        "morningStar": continuation,
        "bullMarubozu": continuation,
        "shootingStarLike": False,
        "bearMarubozu": False,
        "decision_index": 0,
    }


def _snapshot(
    *,
    score_long: float,
    score_short: float,
    market_regime: str,
    daily_state: str,
) -> str:
    payload = {
        "basis_payload": {"marketRegime": market_regime},
        "derived_context": {
            "score_long": score_long,
            "score_short": score_short,
            "daily_main_state_ctx": daily_state,
        },
        "policy": {"action_gate": {"reason": "test_gate"}},
    }
    return json.dumps(payload, ensure_ascii=False)


def test_challenger_reentry_partial_take_and_confirmation_add_sequence() -> None:
    state = ChallengerState()

    action, meta = _challenger_select_action(state, _context(score_long=5.0, score_short=0.0, market_regime="risk_on", daily_state="daily_up_mid"), decision_index=0)
    assert action == "long_entry"
    _apply_challenger_action(state, action, 100.0, decision_date=20260105, execution_date=20260106, decision_index=0)

    action, meta = _challenger_select_action(
        state,
        _context(score_long=8.0, score_short=0.0, market_regime="risk_on", daily_state="daily_up_mid", continuation=True),
        decision_index=1,
    )
    assert action == "long_add"
    _apply_challenger_action(state, action, 102.0, decision_date=20260106, execution_date=20260107, decision_index=1)

    action, meta = _challenger_select_action(
        state,
        _context(score_long=3.0, score_short=0.0, market_regime="risk_on", daily_state="daily_reversal_up_candidate", continuation=True),
        decision_index=2,
    )
    assert action == "partial_take_long"
    _apply_challenger_action(state, action, 103.0, decision_date=20260107, execution_date=20260108, decision_index=2)

    action, meta = _challenger_select_action(
        state,
        _context(score_long=-1.0, score_short=0.0, market_regime="risk_on", daily_state="daily_reversal_up_candidate"),
        decision_index=3,
    )
    assert action == "long_exit"
    _apply_challenger_action(state, action, 99.0, decision_date=20260108, execution_date=20260109, decision_index=3)

    action, meta = _challenger_select_action(
        state,
        _context(score_long=5.0, score_short=0.0, market_regime="risk_on", daily_state="daily_up_mid"),
        decision_index=4,
    )
    assert action == "long_entry"
    assert meta["reason"] == "reentry_after_full_exit"
    _apply_challenger_action(state, action, 101.0, decision_date=20260109, execution_date=20260110, decision_index=4)

    action, meta = _challenger_select_action(
        state,
        _context(score_long=7.0, score_short=0.0, market_regime="risk_on", daily_state="daily_up_mid", continuation=True),
        decision_index=5,
    )
    assert action == "long_add"
    assert meta["reason"] == "confirmed_continuation"

    for decision_index in range(6, 14):
        action, meta = _challenger_select_action(
            state,
            _context(score_long=6.0, score_short=0.0, market_regime="risk_on", daily_state="daily_up_mid"),
            decision_index=decision_index,
        )
    assert action == "long_exit"
    assert meta["reason"] == "reentry_hold_time_stop"


def test_metric_helpers_compute_capture_drawdown_profit_factor_and_reentry_counts() -> None:
    ledger = pd.DataFrame(
        [
            {
                "dt": 20260105,
                "date": "2026-01-05",
                "selected_action": "long_entry",
                "previous_position": "0-0",
                "next_position": "0-2",
                "execution_price": 100.0,
                "realized_pnl": 0.0,
                "unrealized_pnl": 2.0,
                "equity_curve": 0.0,
                "daily_micro_snapshot": _snapshot(score_long=5.0, score_short=0.0, market_regime="risk_on", daily_state="daily_up_mid"),
            },
            {
                "dt": 20260106,
                "date": "2026-01-06",
                "selected_action": "long_add",
                "previous_position": "0-2",
                "next_position": "0-5",
                "execution_price": 101.0,
                "realized_pnl": 0.0,
                "unrealized_pnl": 4.0,
                "equity_curve": 4.0,
                "daily_micro_snapshot": _snapshot(score_long=8.0, score_short=0.0, market_regime="risk_on", daily_state="daily_up_mid"),
            },
            {
                "dt": 20260107,
                "date": "2026-01-07",
                "selected_action": "partial_take_long",
                "previous_position": "0-5",
                "next_position": "0-2",
                "execution_price": 102.0,
                "realized_pnl": 100.0,
                "unrealized_pnl": 3.0,
                "equity_curve": 3.0,
                "daily_micro_snapshot": _snapshot(score_long=1.0, score_short=0.0, market_regime="risk_on", daily_state="daily_reversal_up_candidate"),
            },
            {
                "dt": 20260108,
                "date": "2026-01-08",
                "selected_action": "long_exit",
                "previous_position": "0-2",
                "next_position": "0-0",
                "execution_price": 100.0,
                "realized_pnl": 200.0,
                "unrealized_pnl": 0.0,
                "equity_curve": 8.0,
                "daily_micro_snapshot": _snapshot(score_long=-1.0, score_short=0.0, market_regime="risk_on", daily_state="daily_reversal_up_candidate"),
            },
            {
                "dt": 20260109,
                "date": "2026-01-09",
                "selected_action": "stay",
                "previous_position": "0-0",
                "next_position": "0-0",
                "execution_price": 100.0,
                "realized_pnl": 200.0,
                "unrealized_pnl": 0.0,
                "equity_curve": 8.0,
                "daily_micro_snapshot": _snapshot(score_long=5.0, score_short=0.0, market_regime="risk_on", daily_state="daily_up_mid"),
            },
            {
                "dt": 20260110,
                "date": "2026-01-10",
                "selected_action": "stay",
                "previous_position": "0-0",
                "next_position": "0-0",
                "execution_price": 100.0,
                "realized_pnl": 200.0,
                "unrealized_pnl": 0.0,
                "equity_curve": 8.0,
                "daily_micro_snapshot": _snapshot(score_long=2.0, score_short=0.0, market_regime="neutral", daily_state="daily_reversal_up_candidate"),
            },
        ]
    )
    bars = pd.DataFrame(
        [
            {"dt": 20260105, "c": 100.0},
            {"dt": 20260106, "c": 105.0},
            {"dt": 20260107, "c": 110.0},
            {"dt": 20260108, "c": 108.0},
            {"dt": 20260109, "c": 111.0},
            {"dt": 20260110, "c": 112.0},
        ]
    )
    roundtrips = [
        {"entry_type": "initial", "realized_pnl": 300.0, "exit_decision_date": 20260108},
        {"entry_type": "reentry", "realized_pnl": -100.0, "exit_decision_date": 20260110},
    ]

    aggregate = _policy_aggregate_summary(
        ledger_frame=ledger,
        roundtrips=roundtrips,
        policy_id="tradex_sample_2531_capture_v2",
        symbol="2531",
        start_date="2026-01-05",
        end_date="2026-01-10",
    )
    interval_metrics = _compute_interval_metrics(
        ledger_frame=ledger,
        bars_frame=bars,
        start_date="2026-01-05",
        end_date="2026-01-10",
        first_full_exit_dt=20260108,
    )

    assert aggregate["trade_count"] == 2
    assert aggregate["reentry_count"] == 1
    assert aggregate["total_days_in_position"] == 3
    assert round(aggregate["exposure_ratio_in_window"], 3) == 0.5
    assert aggregate["profit_factor"] == 3.0
    assert aggregate["avg_pnl_per_trade"] == 100.0
    assert round(aggregate["max_drawdown"], 3) == -1.0

    assert round(interval_metrics["upside_capture_ratio"], 3) == 0.5
    assert round(interval_metrics["missed_upside_ratio"], 3) == 0.5
    assert interval_metrics["idle_up_days_after_exit"] == 2
    assert interval_metrics["premature_exit_count"] == 1
    assert interval_metrics["failed_reentry_count"] == 1
