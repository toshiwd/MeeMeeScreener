from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from external_analysis.policy_replay.simulator import (
    _feature_snapshot,
    build_replay_change_log,
    build_replay_window,
    normalize_replay_run_config,
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
