from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from external_analysis.policy_replay.simulator import (
    _feature_snapshot,
    build_replay_change_log,
    build_replay_window,
    normalize_replay_run_config,
    normalize_cost_model,
)


def _business_days(start: date, count: int) -> list[date]:
    days: list[date] = []
    current = start
    while len(days) < count:
        if current.weekday() < 5:
            days.append(current)
        current += timedelta(days=1)
    return days


def _rows_from_prices(prices: list[float], start: date) -> list[tuple[int, float, float, float, float, int]]:
    rows = []
    for current, price in zip(_business_days(start, len(prices)), prices, strict=True):
        ymd = int(current.strftime("%Y%m%d"))
        rows.append((ymd, price, price * 1.01, price * 0.99, price, 1000))
    return rows


def _rows_from_open_close(series: list[tuple[float, float]], start: date) -> list[tuple[int, float, float, float, float, int]]:
    rows = []
    for current, (open_price, close_price) in zip(_business_days(start, len(series)), series, strict=True):
        ymd = int(current.strftime("%Y%m%d"))
        high = max(open_price, close_price) * 1.01
        low = min(open_price, close_price) * 0.99
        rows.append((ymd, open_price, high, low, close_price, 1000))
    return rows


def _epoch_rows_from_prices(prices: list[float], start: date) -> list[tuple[int, float, float, float, float, int]]:
    rows = []
    for current, price in zip(_business_days(start, len(prices)), prices, strict=True):
        epoch = int(datetime.combine(current, datetime.min.time(), tzinfo=timezone.utc).timestamp())
        rows.append((epoch, price, price * 1.01, price * 0.99, price, 1000))
    return rows


class StubRepo:
    def __init__(self, data: dict[str, list[tuple[int, float, float, float, float, int]]]):
        self.data = data

    def get_daily_bars_batch(self, codes, limit=420, asof_dt=None):  # noqa: ANN001
        return {code: [row for row in self.data.get(code, []) if asof_dt is None or row[0] <= asof_dt] for code in codes}


def test_feature_snapshot_ignores_future_rows():
    current = date(2026, 1, 16)
    base_rows = _rows_from_prices([100, 101, 102, 103, 104, 105, 106, 107, 108, 109], date(2026, 1, 5))
    future_rows = _rows_from_prices([500, 600, 700], date(2026, 1, 19))
    snap_base = _feature_snapshot("1111", base_rows, current)
    snap_future = _feature_snapshot("1111", base_rows + future_rows, current)
    assert snap_base is not None
    assert snap_future is not None
    assert snap_base["close_price"] == snap_future["close_price"]
    assert snap_base["daily_return_1d"] == snap_future["daily_return_1d"]
    assert snap_base["daily_return_5d"] == snap_future["daily_return_5d"]
    assert snap_base["daily_return_20d"] == snap_future["daily_return_20d"]
    assert snap_base["weekly_return_1w"] == snap_future["weekly_return_1w"]
    assert snap_base["monthly_return_1m"] == snap_future["monthly_return_1m"]


def test_feature_snapshot_ignores_future_rows_with_epoch_seconds():
    current = date(2026, 1, 16)
    base_rows = _epoch_rows_from_prices([100, 101, 102, 103, 104, 105, 106, 107, 108, 109], date(2026, 1, 5))
    future_rows = _epoch_rows_from_prices([500, 600, 700], date(2026, 1, 19))
    snap_base = _feature_snapshot("1111", base_rows, current)
    snap_future = _feature_snapshot("1111", base_rows + future_rows, current)
    assert snap_base is not None
    assert snap_future is not None
    assert snap_base["close_price"] == snap_future["close_price"]
    assert snap_base["daily_return_1d"] == snap_future["daily_return_1d"]
    assert snap_base["weekly_return_1w"] == snap_future["weekly_return_1w"]
    assert snap_base["monthly_return_1m"] == snap_future["monthly_return_1m"]


def test_replay_window_is_deterministic_and_records_required_artifacts():
    days = _business_days(date(2026, 1, 5), 30)
    up = [100 + i * 2 for i in range(len(days))]
    down = [150 - i * 2 for i in range(len(days))]
    flat = [100 for _ in range(len(days))]
    repo = StubRepo(
        {
            "1111": _rows_from_prices(up, date(2026, 1, 5)),
            "2222": _rows_from_prices(down, date(2026, 1, 5)),
            "1306": _rows_from_prices(flat, date(2026, 1, 5)),
        }
    )
    payload = {
        "window_start_date": "2026-01-05",
        "universe": ["1111", "2222"],
        "market_benchmark_symbol": "1306",
        "entry_threshold": 0.01,
        "add_threshold": 0.02,
        "exit_threshold": -0.01,
        "partial_take_threshold": 0.01,
        "stop_loss_threshold": -0.5,
        "addon_units": [2, 3, 5],
        "unit_scale": 10,
    }
    first = build_replay_window(repo, payload)
    second = build_replay_window(repo, payload)
    assert first["window"]["window_summary"] == second["window"]["window_summary"]
    assert first["window"]["trade_ledger"] == second["window"]["trade_ledger"]
    assert first["window"]["positions_timeline"] == second["window"]["positions_timeline"]
    assert first["multiwindow_leaderboard"] == second["multiwindow_leaderboard"]
    summary = first["window"]["window_summary"]
    assert summary["selection_rule_signatures"]["selection_rule_signature"]
    assert summary["weekly_trade_count_map"]
    assert summary["weeks_with_no_trade"] >= 0
    assert summary["forced_activity_events_count"] >= 0
    change_log = build_replay_change_log(first["window"]["run_config"])
    assert change_log and {"change_id", "policy_id", "previous_rule_signature", "new_rule_signature", "reason_code", "reason_text"} <= set(change_log[0])


def test_replay_window_caps_add_ons_and_uses_sell_buy_notation():
    repo = StubRepo(
        {
            "1111": _rows_from_prices([100, 102, 104, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124, 126, 128], date(2026, 1, 5)),
            "2222": _rows_from_prices([120, 118, 116, 114, 112, 110, 108, 106, 104, 102, 100, 98, 96, 94, 92], date(2026, 1, 5)),
            "1306": _rows_from_prices([100 for _ in range(15)], date(2026, 1, 5)),
        }
    )
    result = build_replay_window(
        repo,
        {
            "window_start_date": "2026-01-05",
            "universe": ["1111", "2222"],
            "market_benchmark_symbol": "1306",
            "entry_threshold": 0.01,
            "add_threshold": 0.02,
            "exit_threshold": -0.01,
            "partial_take_threshold": 0.01,
            "stop_loss_threshold": -0.5,
            "addon_units": [2, 3, 5],
            "unit_scale": 10,
        },
    )
    window = result["window"]
    actions_by_symbol: dict[str, list[str]] = {}
    for row in window["trade_ledger"]:
        actions_by_symbol.setdefault(row["symbol"], []).append(row["action_taken"])
        assert "-" in row["position_notation_before"]
        assert "-" in row["position_notation_after"]
    for actions in actions_by_symbol.values():
        build_streak = 0
        max_streak = 0
        for action in actions:
            if action in {"enter_long", "enter_short", "add_on"}:
                build_streak += 1
                max_streak = max(max_streak, build_streak)
            elif action in {"full_exit", "forced_exit", "invalidated", "partial_take_1", "partial_take_2"}:
                build_streak = 0
        assert max_streak <= 3
    for row in window["positions_timeline"]:
        notation = row["position_notation"]
        assert notation == "0-0" or notation.count("-") == 1


def test_multiwindow_identical_conditions_stay_identical():
    repo = StubRepo(
        {
            "1111": _rows_from_prices([100, 101, 102, 103, 104, 105, 106, 107, 108, 109], date(2026, 1, 5)),
            "2222": _rows_from_prices([100, 99, 98, 97, 96, 95, 94, 93, 92, 91], date(2026, 1, 5)),
            "1306": _rows_from_prices([100 for _ in range(10)], date(2026, 1, 5)),
        }
    )
    result = build_replay_window(
        repo,
        {
            "window_start_dates": ["2026-01-05", "2026-01-05"],
            "universe": ["1111", "2222"],
            "market_benchmark_symbol": "1306",
            "entry_threshold": 0.01,
            "add_threshold": 0.02,
            "exit_threshold": -0.01,
            "partial_take_threshold": 0.01,
            "stop_loss_threshold": -0.5,
            "addon_units": [2, 3, 5],
            "unit_scale": 10,
        },
    )
    leaderboard = result["multiwindow_leaderboard"]
    assert len(leaderboard["rows"]) == 2
    assert leaderboard["rows"][0]["final_score"] == leaderboard["rows"][1]["final_score"]
    assert leaderboard["rows"][0]["window_start_date"] == leaderboard["rows"][1]["window_start_date"]


def test_cost_model_normalization_defaults_to_provisional_nonzero_costs():
    model = normalize_cost_model(None)
    assert model["enabled"] is True
    assert model["commission_bps"] > 0
    assert model["slippage_bps"] > 0
    assert model["status"] == "provisional_placeholder"

    supplied = normalize_replay_run_config(
        {
            "window_start_date": "2026-01-05",
            "universe": ["1111"],
            "cost_model": {"enabled": True, "commission_bps": 7.0, "slippage_bps": 3.0, "tax_or_fee_bps": 1.0, "min_fee": 10.0},
            "execution_convention": "next_session_open",
        }
    )
    assert supplied["cost_model"]["commission_bps"] == 7.0
    assert supplied["cost_model"]["slippage_bps"] == 3.0
    assert supplied["cost_model"]["tax_or_fee_bps"] == 1.0
    assert supplied["cost_model"]["min_fee"] == 10.0
    assert supplied["execution_model"]["active_execution_model"] == "next_session_open"
    assert supplied["execution_model"]["next_session_open"]["supported"] is True
    assert supplied["execution_model"]["next_session_open"]["status"] == "supported"


def test_normalize_replay_run_config_preserves_explicit_zero_thresholds():
    supplied = normalize_replay_run_config(
        {
            "window_start_date": "2026-01-05",
            "universe": ["1111"],
            "initial_capital_jpy": 1000,
            "gross_exposure_cap_jpy": 1000,
            "entry_threshold": 0.0,
            "add_threshold": 0.0,
            "partial_take_threshold": 0.0,
            "exit_threshold": 0.0,
            "stop_loss_threshold": 0.0,
            "policy": {
                "entry_threshold": 0.0,
                "add_threshold": 0.0,
                "partial_take_threshold": 0.0,
                "exit_threshold": 0.0,
                "stop_loss_threshold": 0.0,
            },
        }
    )

    assert supplied["entry_threshold"] == 0.0
    assert supplied["add_threshold"] == 0.0
    assert supplied["partial_take_threshold"] == 0.0
    assert supplied["exit_threshold"] == 0.0
    assert supplied["stop_loss_threshold"] == 0.0
    assert supplied["policy_rules"]["entry_rule"]["entry_threshold"] == 0.0
    assert supplied["policy_rules"]["add_rule"]["add_threshold"] == 0.0
    assert supplied["policy_rules"]["partial_take_rule"]["partial_take_threshold"] == 0.0
    assert supplied["policy_rules"]["full_exit_rule"]["exit_threshold"] == 0.0
    assert supplied["policy_rules"]["full_exit_rule"]["stop_loss_threshold"] == 0.0


def test_normalize_replay_run_config_preserves_entry_gate_timing_override_fields():
    supplied = normalize_replay_run_config(
        {
            "window_start_date": "2026-01-05",
            "universe": ["1111"],
            "execution_convention": "next_session_open",
            "action_policy": {
                "mode": "long_entry_cash_gate_entry_signal_relax_v1",
                "enabled": True,
                "entry_gate": {
                    "enabled": True,
                    "timing_override_enabled": True,
                    "timing_override_reason_codes": ["timing_block"],
                    "timing_override_rank_max": 11,
                    "timing_override_score_min": 0.05,
                },
            },
        }
    )

    gate = supplied["action_policy"]["entry_gate"]
    assert gate["timing_override_enabled"] is True
    assert gate["timing_override_reason_codes"] == ["timing_block"]
    assert gate["timing_override_rank_max"] == 11
    assert gate["timing_override_score_min"] == 0.05
    assert supplied["execution_model"]["next_session_open"]["status"] == "supported"


def test_next_session_open_fill_uses_next_trading_session_open_and_records_execution_metadata():
    repo = StubRepo(
        {
            "1111": _rows_from_open_close([(100.0, 101.0), (110.0, 120.0), (130.0, 140.0)], date(2026, 1, 5)),
            "1306": _rows_from_open_close([(100.0, 100.0), (100.0, 100.0), (100.0, 100.0)], date(2026, 1, 5)),
        }
    )
    result = build_replay_window(
        repo,
        {
            "window_start_date": "2026-01-05",
            "universe": ["1111"],
            "market_benchmark_symbol": "1306",
            "entry_threshold": -0.01,
            "add_threshold": 0.02,
            "exit_threshold": -0.01,
            "partial_take_threshold": 0.01,
            "stop_loss_threshold": -0.5,
            "addon_units": [1, 1, 1],
            "unit_scale": 1,
            "initial_capital_jpy": 1000,
            "gross_exposure_cap_jpy": 1000,
            "execution_convention": "next_session_open",
            "cost_model": {"enabled": True, "commission_bps": 10.0, "slippage_bps": 5.0, "tax_or_fee_bps": 0.0, "min_fee": 0.0},
        },
    )
    trade = next(row for row in result["window"]["trade_ledger"] if row["symbol"] == "1111" and row["action_taken"] == "enter_long")
    assert trade["execution_timing"] == "next_session_open"
    assert trade["decision_date"] == "2026-01-05"
    assert trade["fill_date"] == "2026-01-06"
    assert trade["price_provenance"] == "next_session_open_price"
    assert trade["order_status"] == "filled"
    assert trade["execution_price"] == 110.0
    assert trade["no_lookahead_check"]["decision_before_fill"] is True
    assert trade["no_lookahead_check"]["status"] == "pass"
    assert trade["cash_after"] == pytest.approx(1000.0 - 110.0 - 0.165)
    ledger_row = next(row for row in result["window"]["portfolio_daily_action_ledger"] if row["symbol"] == "1111" and row["action"] == "buy" and row["order_status"] == "filled")
    assert ledger_row["decision_date"] == "2026-01-05"
    assert ledger_row["fill_date"] == "2026-01-06"
    assert ledger_row["execution_model"] == "next_session_open"
    assert ledger_row["execution_timing"] == "next_session_open"
    assert ledger_row["execution_price"] == 110.0
    assert ledger_row["price_provenance"] == "next_session_open_price"
    assert ledger_row["no_lookahead_check"]["decision_before_fill"] is True
    assert ledger_row["cost_amount"] > 0


def test_next_session_open_missing_next_open_data_emits_unfilled_status():
    repo = StubRepo(
        {
            "1111": _rows_from_open_close([(100.0, 101.0)], date(2026, 1, 5)),
            "1306": _rows_from_open_close([(100.0, 100.0)], date(2026, 1, 5)),
        }
    )
    result = build_replay_window(
        repo,
        {
            "window_start_date": "2026-01-05",
            "universe": ["1111"],
            "market_benchmark_symbol": "1306",
            "entry_threshold": -0.01,
            "add_threshold": 0.02,
            "exit_threshold": -0.01,
            "partial_take_threshold": 0.01,
            "stop_loss_threshold": -0.5,
            "addon_units": [1, 1, 1],
            "unit_scale": 1,
            "initial_capital_jpy": 1000,
            "gross_exposure_cap_jpy": 1000,
            "execution_convention": "next_session_open",
            "cost_model": {"enabled": True, "commission_bps": 10.0, "slippage_bps": 5.0, "tax_or_fee_bps": 0.0, "min_fee": 0.0},
        },
    )
    trade = next(row for row in result["window"]["trade_ledger"] if row["symbol"] == "1111")
    assert trade["order_status"] == "unfilled"
    assert trade["execution_price"] is None
    assert trade["fill_date"] is None
    assert trade["price_provenance"] is None
    assert trade["unfilled_reason"] == "insufficient_execution_data"
    assert trade["no_lookahead_check"]["status"] == "unknown"


def test_replay_window_emits_portfolio_daily_action_ledger_required_schema():
    repo = StubRepo(
        {
            "1111": _rows_from_prices([100, 103, 106, 109, 112, 115, 118, 121, 124, 127], date(2026, 1, 5)),
            "1306": _rows_from_prices([100 for _ in range(10)], date(2026, 1, 5)),
        }
    )
    result = build_replay_window(
        repo,
        {
            "window_start_date": "2026-01-05",
            "universe": ["1111"],
            "market_benchmark_symbol": "1306",
            "entry_threshold": 0.01,
            "add_threshold": 0.02,
            "exit_threshold": -0.01,
            "partial_take_threshold": 0.01,
            "stop_loss_threshold": -0.5,
            "addon_units": [2, 3, 5],
            "unit_scale": 10,
            "cost_model": {"enabled": True, "commission_bps": 10.0, "slippage_bps": 5.0, "tax_or_fee_bps": 0.0, "min_fee": 0.0},
        },
    )
    ledger = result["window"]["portfolio_daily_action_ledger"]
    assert ledger
    required = {
        "date",
        "symbol",
        "action",
        "side",
        "reason_codes",
        "cash",
        "gross_exposure",
        "net_exposure",
        "position_qty",
        "position_value",
        "realized_pnl",
        "unrealized_pnl",
        "daily_pnl",
        "cumulative_pnl",
        "drawdown",
        "cost_amount",
        "slippage_amount",
        "execution_price",
        "execution_timing",
        "execution_model",
        "price_provenance",
        "data_asof",
        "no_lookahead_check",
        "decision_date",
        "fill_date",
        "order_status",
        "unfilled_reason",
        "notes",
    }
    assert required <= set(ledger[0])
    assert any(row["cost_amount"] > 0 for row in ledger)
    assert all("capability_flags" in row for row in ledger)
