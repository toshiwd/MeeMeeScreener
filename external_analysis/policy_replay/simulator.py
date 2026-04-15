from __future__ import annotations

import hashlib
import json
from calendar import monthrange
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from statistics import median
from typing import Any

from app.backend.infra.duckdb.stock_repo import StockRepository

REPLAY_SCHEMA_VERSION = "tradex_policy_replay_v1"
DEFAULT_INITIAL_CAPITAL_JPY = 10_000_000.0
DEFAULT_GROSS_CAP_JPY = 10_000_000.0
DEFAULT_MARKET_BENCHMARK_SYMBOL = "1306"


def _text(value: Any, fallback: str = "") -> str:
    if value is None:
        return fallback
    if isinstance(value, str):
        value = value.strip()
        return value or fallback
    value = str(value).strip()
    return value or fallback


def _num(value: Any, fallback: float = 0.0) -> float:
    if value is None:
        return float(fallback)
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return float(fallback)
    return parsed if parsed == parsed else float(fallback)


def _stable_hash(payload: Any) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _iso(value: date) -> str:
    return value.isoformat()


def _parse_date(value: Any) -> date:
    text = _text(value)
    if len(text) == 8 and text.isdigit():
        return date(int(text[:4]), int(text[4:6]), int(text[6:8]))
    return date.fromisoformat(text[:10])


def _add_months(value: date, months: int) -> date:
    year = value.year + (value.month - 1 + months) // 12
    month = (value.month - 1 + months) % 12 + 1
    days = [31, 29 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    return date(year, month, min(value.day, days[month - 1]))


def _bar_date(value: Any) -> date:
    raw = int(value)
    # Support both YYYYMMDD and epoch-based bar dates.
    if 10_000_101 <= raw <= 99_991_231:
        return date(raw // 10_000, (raw // 100) % 100, raw % 100)
    if raw >= 1_000_000_000_000:
        return datetime.fromtimestamp(raw / 1_000.0, tz=timezone.utc).date()
    return datetime.fromtimestamp(raw, tz=timezone.utc).date()


def _week_key(value: date) -> str:
    iso = value.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"


def _rule_signature(payload: dict[str, Any]) -> str:
    return _stable_hash(payload)


def _rule_block(payload: dict[str, Any], *, name: str, fallback: dict[str, Any] | None = None) -> dict[str, Any]:
    block = payload.get(name)
    if isinstance(block, dict):
        return dict(block)
    return dict(fallback or {})


def _normalize_policy_rule_blocks(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    policy = dict(payload.get("policy") or {})
    capital = dict(payload.get("capital") or {})
    scoring = dict(payload.get("scoring") or {})
    selection_rule = _rule_block(
        payload,
        name="selection_rule",
        fallback={
            "policy_id": _text(payload.get("policy_id") or policy.get("policy_id") or "tradex_policy_replay_v1"),
            "policy_version": _text(payload.get("policy_version") or policy.get("policy_version") or "v1"),
            "window_months": int(payload.get("window_months") or 3),
            "weekly_activity_required": bool(payload.get("weekly_activity_required", True)),
            "execution_convention": _text(payload.get("execution_convention") or "close_close_research_convention"),
            "weights": dict(scoring.get("weights") or payload.get("weights") or {}),
            "selection_rule_change_log": list(payload.get("selection_rule_change_log") or []),
        },
    )
    entry_rule = _rule_block(
        payload,
        name="entry_rule",
        fallback={
            "entry_threshold": _num(policy.get("entry_threshold") or payload.get("entry_threshold") or 0.55),
            "direction_mode": "both",
        },
    )
    add_rule = _rule_block(
        payload,
        name="add_rule",
        fallback={
            "add_threshold": _num(policy.get("add_threshold") or payload.get("add_threshold") or 0.80),
            "addon_units": [int(v) for v in (payload.get("addon_units") or [2, 3, 5])],
        },
    )
    partial_take_rule = _rule_block(
        payload,
        name="partial_take_rule",
        fallback={
            "partial_take_threshold": _num(policy.get("partial_take_threshold") or payload.get("partial_take_threshold") or 0.08),
        },
    )
    full_exit_rule = _rule_block(
        payload,
        name="full_exit_rule",
        fallback={
            "exit_threshold": _num(policy.get("exit_threshold") or payload.get("exit_threshold") or -0.10),
            "stop_loss_threshold": _num(policy.get("stop_loss_threshold") or payload.get("stop_loss_threshold") or -0.06),
        },
    )
    sizing_rule = _rule_block(
        payload,
        name="sizing_rule",
        fallback={
            "initial_capital_jpy": _num(capital.get("initial_capital_jpy") or payload.get("initial_capital_jpy") or DEFAULT_INITIAL_CAPITAL_JPY),
            "gross_exposure_cap_jpy": _num(capital.get("gross_exposure_cap_jpy") or payload.get("gross_exposure_cap_jpy") or DEFAULT_GROSS_CAP_JPY),
            "unit_scale": int(payload.get("unit_scale") or 100),
            "short_cash_reusable": bool(payload.get("short_cash_reusable", False)),
        },
    )
    return {
        "selection_rule": selection_rule,
        "entry_rule": entry_rule,
        "add_rule": add_rule,
        "partial_take_rule": partial_take_rule,
        "full_exit_rule": full_exit_rule,
        "sizing_rule": sizing_rule,
    }


def _avg(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def _med(values: list[float]) -> float | None:
    return float(median(values)) if values else None


def _notation(short_units: int, long_units: int) -> str:
    return f"{max(0, short_units)}-{max(0, long_units)}"


def normalize_replay_run_config(payload: dict[str, Any]) -> dict[str, Any]:
    policy = dict(payload.get("policy") or {})
    capital = dict(payload.get("capital") or {})
    scoring = dict(payload.get("scoring") or {})
    rule_blocks = _normalize_policy_rule_blocks(payload)
    run_config = {
        "schema_version": REPLAY_SCHEMA_VERSION,
        "policy_id": _text(payload.get("policy_id") or policy.get("policy_id") or "tradex_policy_replay_v1"),
        "policy_version": _text(payload.get("policy_version") or policy.get("policy_version") or "v1"),
        "window_start_date": _text(payload.get("window_start_date")),
        "window_start_dates": [_text(item) for item in payload.get("window_start_dates") or [] if _text(item)],
        "window_months": int(payload.get("window_months") or 3),
        "universe": [_text(item) for item in payload.get("universe") or [] if _text(item)],
        "market_benchmark_symbol": _text(payload.get("market_benchmark_symbol") or DEFAULT_MARKET_BENCHMARK_SYMBOL),
        "initial_capital_jpy": _num(capital.get("initial_capital_jpy") or payload.get("initial_capital_jpy") or DEFAULT_INITIAL_CAPITAL_JPY),
        "gross_exposure_cap_jpy": _num(capital.get("gross_exposure_cap_jpy") or payload.get("gross_exposure_cap_jpy") or DEFAULT_GROSS_CAP_JPY),
        "unit_scale": int(payload.get("unit_scale") or 100),
        "addon_units": [int(v) for v in (payload.get("addon_units") or [2, 3, 5])],
        "entry_threshold": _num(policy.get("entry_threshold") or payload.get("entry_threshold") or 0.55),
        "exit_threshold": _num(policy.get("exit_threshold") or payload.get("exit_threshold") or -0.10),
        "add_threshold": _num(policy.get("add_threshold") or payload.get("add_threshold") or 0.80),
        "partial_take_threshold": _num(policy.get("partial_take_threshold") or payload.get("partial_take_threshold") or 0.08),
        "stop_loss_threshold": _num(policy.get("stop_loss_threshold") or payload.get("stop_loss_threshold") or -0.06),
        "weekly_activity_required": bool(payload.get("weekly_activity_required", True)),
        "short_cash_reusable": bool(payload.get("short_cash_reusable", False)),
        "execution_convention": _text(payload.get("execution_convention") or "close_close_research_convention"),
        "weights": dict(scoring.get("weights") or payload.get("weights") or {}),
        "selection_rule_change_log": list(payload.get("selection_rule_change_log") or []),
        "policy_rules": rule_blocks,
    }
    if not run_config["window_start_dates"] and run_config["window_start_date"]:
        run_config["window_start_dates"] = [run_config["window_start_date"]]
    if not run_config["universe"]:
        raise ValueError("universe is required")
    if not run_config["weights"]:
        run_config["weights"] = {
            "total_return": 0.08,
            "excess_vs_universe": 0.22,
            "exposure_adjusted_excess": 0.16,
            "median_window_excess": 0.16,
            "worst_window_excess": 0.10,
            "max_drawdown": -0.10,
            "turnover": -0.08,
            "concentration": -0.05,
            "weekly_activity": -0.08,
            "long_hold": 0.10,
            "premature_exit": -0.07,
        }
    selection_rule_signature = _rule_signature(run_config["policy_rules"]["selection_rule"])
    entry_rule_signature = _rule_signature(run_config["policy_rules"]["entry_rule"])
    add_rule_signature = _rule_signature(run_config["policy_rules"]["add_rule"])
    partial_take_rule_signature = _rule_signature(run_config["policy_rules"]["partial_take_rule"])
    full_exit_rule_signature = _rule_signature(run_config["policy_rules"]["full_exit_rule"])
    sizing_rule_signature = _rule_signature(run_config["policy_rules"]["sizing_rule"])
    run_config["selection_rule_signatures"] = {
        "selection_rule_signature": selection_rule_signature,
        "entry_rule_signature": entry_rule_signature,
        "add_rule_signature": add_rule_signature,
        "partial_take_rule_signature": partial_take_rule_signature,
        "full_exit_rule_signature": full_exit_rule_signature,
        "exit_rule_signature": full_exit_rule_signature,
        "sizing_rule_signature": sizing_rule_signature,
    }
    run_config["policy_family_signature"] = _stable_hash(
        {
            "policy_id": run_config["policy_id"],
            "policy_version": run_config["policy_version"],
            "selection_rule_signatures": run_config["selection_rule_signatures"],
            "universe": run_config["universe"],
            "window_start_dates": run_config["window_start_dates"],
            "window_months": run_config["window_months"],
            "market_benchmark_symbol": run_config["market_benchmark_symbol"],
            "capital": {
                "initial_capital_jpy": run_config["initial_capital_jpy"],
                "gross_exposure_cap_jpy": run_config["gross_exposure_cap_jpy"],
                "short_cash_reusable": run_config["short_cash_reusable"],
            },
            "execution_convention": run_config["execution_convention"],
        }
    )
    return run_config


def build_replay_change_log(run_config: dict[str, Any]) -> list[dict[str, Any]]:
    current = dict(run_config["selection_rule_signatures"])
    raw_items = list(run_config.get("selection_rule_change_log") or [])
    if not raw_items:
        raw_items = [{}]
    previous = "initial"
    records: list[dict[str, Any]] = []
    for index, raw in enumerate(raw_items, start=1):
        item = dict(raw or {})
        item.setdefault("change_id", f"change_{index}")
        item.setdefault("policy_id", run_config["policy_id"])
        item.setdefault("previous_rule_signature", previous)
        item.setdefault("new_rule_signature", current["selection_rule_signature"])
        item.setdefault(
            "changed_fields",
            [
                "selection_rule_signature",
                "entry_rule_signature",
                "add_rule_signature",
                "partial_take_rule_signature",
                "full_exit_rule_signature",
                "sizing_rule_signature",
            ],
        )
        item.setdefault("reason_code", "initial_policy" if index == 1 else "policy_update")
        item.setdefault("reason_text", "initial policy loaded for replay")
        item.setdefault("expected_effect", "repeatable point-in-time policy simulation")
        item.setdefault("author_or_source", "system")
        item.setdefault("timestamp_or_run_id", run_config["window_start_date"] or "suite")
        previous = _text(item["new_rule_signature"]) or previous
        records.append(item)
    return records


def _extract_series_map(repo: StockRepository, symbols: list[str], *, asof_dt: int, limit: int = 420) -> dict[str, list[tuple[Any, ...]]]:
    rows = repo.get_daily_bars_batch(symbols, limit=limit, asof_dt=asof_dt)
    return {symbol: list(value or []) for symbol, value in rows.items()}


def _series_dates(rows: list[tuple[Any, ...]]) -> list[date]:
    return [_bar_date(row[0]) for row in rows if len(row) >= 5]


def _feature_snapshot(symbol: str, rows: list[tuple[Any, ...]], current: date) -> dict[str, Any] | None:
    current_rows = [row for row in rows if _bar_date(row[0]) <= current]
    if not current_rows:
        return None
    completed_weekly_rows: list[tuple[Any, ...]] = []
    completed_monthly_rows: list[tuple[Any, ...]] = []
    for row in current_rows:
        bar_date = _bar_date(row[0])
        if bar_date.weekday() < 4 or bar_date == current:
            completed_weekly_rows.append(row)
        if bar_date.day < monthrange(bar_date.year, bar_date.month)[1] or bar_date == current:
            completed_monthly_rows.append(row)
    closes = [_num(row[4]) for row in current_rows]
    highs = [_num(row[2]) for row in current_rows]
    lows = [_num(row[3]) for row in current_rows]
    close = closes[-1]
    prev_close = closes[-2] if len(closes) >= 2 else None
    weekly_ret = None
    monthly_ret = None
    if len(completed_weekly_rows) >= 2:
        completed_weekly_closes = [_num(row[4]) for row in completed_weekly_rows if _bar_date(row[0]).weekday() == 4]
        if len(completed_weekly_closes) >= 2:
            weekly_ret = completed_weekly_closes[-1] / completed_weekly_closes[-2] - 1.0
    if len(completed_monthly_rows) >= 2:
        completed_monthly_closes = [_num(row[4]) for row in completed_monthly_rows if _bar_date(row[0]).day == monthrange(_bar_date(row[0]).year, _bar_date(row[0]).month)[1]]
        if len(completed_monthly_closes) >= 2:
            monthly_ret = completed_monthly_closes[-1] / completed_monthly_closes[-2] - 1.0
    return {
        "symbol": symbol,
        "feature_state_id": _stable_hash(
            {
                "symbol": symbol,
                "date": _iso(current),
                "close": close,
                "ret1": None if prev_close is None else round(close / prev_close - 1.0, 8),
                "ret5": round(close / closes[-6] - 1.0, 8) if len(closes) >= 6 else None,
                "ret20": round(close / closes[-21] - 1.0, 8) if len(closes) >= 21 else None,
                "weekly": weekly_ret,
                "monthly": monthly_ret,
            }
        ),
        "close_price": close,
        "daily_return_1d": None if prev_close is None else close / prev_close - 1.0,
        "daily_return_5d": close / closes[-6] - 1.0 if len(closes) >= 6 else None,
        "daily_return_20d": close / closes[-21] - 1.0 if len(closes) >= 21 else None,
        "weekly_return_1w": weekly_ret,
        "monthly_return_1m": monthly_ret,
        "ma20": _avg(closes[-20:]) if len(closes) >= 20 else _avg(closes),
        "ma60": _avg(closes[-60:]) if len(closes) >= 60 else _avg(closes),
        "high_20d": max(highs[-20:]) if len(highs) >= 20 else max(highs),
        "low_20d": min(lows[-20:]) if len(lows) >= 20 else min(lows),
    }


def prepare_replay_window_context(repo: StockRepository, run_config: dict[str, Any], window_start: date) -> dict[str, Any]:
    window_end = _add_months(window_start, int(run_config["window_months"])) - timedelta(days=1)
    symbols = list(dict.fromkeys([*run_config["universe"], run_config["market_benchmark_symbol"]]))
    asof_dt = int(datetime.combine(window_end, datetime.min.time(), tzinfo=timezone.utc).timestamp())
    series_map = _extract_series_map(repo, symbols, asof_dt=asof_dt)
    dates = sorted({d for rows in series_map.values() for d in _series_dates(rows) if window_start <= d <= window_end})
    if not dates:
        raise ValueError("no trading dates available in replay window")
    feature_grid: dict[str, dict[str, dict[str, Any] | None]] = {}
    benchmark_market: list[dict[str, Any]] = []
    benchmark_universe: list[dict[str, Any]] = []
    for current in dates:
        date_key = _iso(current)
        per_symbol: dict[str, dict[str, Any] | None] = {}
        for symbol in symbols:
            per_symbol[symbol] = _feature_snapshot(symbol, series_map.get(symbol) or [], current)
        feature_grid[date_key] = per_symbol
        benchmark_feature = per_symbol.get(run_config["market_benchmark_symbol"])
        if benchmark_feature:
            benchmark_market.append(
                {
                    "date": date_key,
                    "symbol": run_config["market_benchmark_symbol"],
                    "close_price": benchmark_feature["close_price"],
                    "feature_state_id": benchmark_feature["feature_state_id"],
                }
            )
        returns = [float(feature["daily_return_1d"]) for symbol, feature in per_symbol.items() if symbol in run_config["universe"] and feature is not None and feature.get("daily_return_1d") is not None]
        benchmark_universe.append({"date": date_key, "daily_return": _avg(returns), "universe_size": len(returns)})
    return {
        "window_start": _iso(window_start),
        "window_end": _iso(window_end),
        "asof_dt": asof_dt,
        "symbols": symbols,
        "series_map": series_map,
        "dates": dates,
        "feature_grid": feature_grid,
        "benchmark_market": benchmark_market,
        "benchmark_universe": benchmark_universe,
    }


def _context_feature(context: dict[str, Any], current: date, symbol: str) -> dict[str, Any] | None:
    feature_grid = context.get("feature_grid")
    if not isinstance(feature_grid, dict):
        return None
    current_key = _iso(current)
    current_features = feature_grid.get(current_key)
    if not isinstance(current_features, dict):
        return None
    feature = current_features.get(symbol)
    return feature if isinstance(feature, dict) or feature is None else None


def _signal_score(feature: dict[str, Any]) -> float:
    close = _num(feature["close_price"])
    ma20 = _num(feature.get("ma20")) or close
    ma60 = _num(feature.get("ma60")) or close
    return (
        0.10 * _num(feature.get("daily_return_1d"))
        + 0.22 * _num(feature.get("daily_return_5d"))
        + 0.30 * _num(feature.get("daily_return_20d"))
        + 0.18 * _num(feature.get("weekly_return_1w"))
        + 0.14 * _num(feature.get("monthly_return_1m"))
        + 0.03 * (close / ma20 - 1.0)
        + 0.03 * (close / ma60 - 1.0)
    )


def _new_position(*, symbol: str, side: str, units: int, unit_scale: int, price: float, entry_date: date, position_id: str) -> dict[str, Any]:
    return {
        "position_id": position_id,
        "symbol": symbol,
        "side": side,
        "logical_units": int(units),
        "unit_scale": int(unit_scale),
        "avg_entry_price": float(price),
        "add_count": 0,
        "realized_pnl_cum": 0.0,
        "entry_date": entry_date,
        "closed": False,
        "exit_state": None,
        "exit_reason_code": None,
        "exit_date": None,
        "first_partial_exit_day": None,
        "holding_sessions": 0,
        "reserved_short_proceeds": 0.0,
    }


def _position_state(position: dict[str, Any] | None) -> str:
    if not position:
        return "flat"
    if position.get("closed"):
        return _text(position.get("exit_state") or "full_exit")
    add_count = int(position.get("add_count") or 0)
    if add_count >= 2:
        return "fully_built"
    if add_count == 1:
        return "entered_2"
    return "entered_1"


def _position_notation(position: dict[str, Any] | None) -> str:
    if not position or position.get("closed"):
        return "0-0"
    short_units = int(position["logical_units"]) if position["side"] == "short" else 0
    long_units = int(position["logical_units"]) if position["side"] == "long" else 0
    return _notation(short_units, long_units)


def _position_units(position: dict[str, Any] | None) -> tuple[int, int]:
    if not position or position.get("closed"):
        return 0, 0
    return (
        int(position["logical_units"]) if position["side"] == "short" else 0,
        int(position["logical_units"]) if position["side"] == "long" else 0,
    )


def _position_market_value(position: dict[str, Any] | None, close_price: float | None) -> float:
    if not position or position.get("closed") or close_price is None:
        return 0.0
    return float(int(position["logical_units"]) * int(position["unit_scale"]) * float(close_price))


def _position_unrealized(position: dict[str, Any] | None, close_price: float | None) -> float:
    if not position or position.get("closed") or close_price is None:
        return 0.0
    signed = 1.0 if position["side"] == "long" else -1.0
    return (float(close_price) - float(position["avg_entry_price"])) * int(position["logical_units"]) * int(position["unit_scale"]) * signed


def _build_trade_event(
    *,
    date_value: date,
    symbol: str,
    state_from: str,
    state_to: str,
    notation_before: str,
    notation_after: str,
    action_taken: str,
    trigger_reason_code: str,
    trigger_reason_text: str,
    units_changed: int,
    close_price: float,
    avg_entry_price_after: float,
    realized_pnl_delta: float,
    unrealized_pnl_after: float,
    cash_after: float,
    equity_after: float,
    position_id: str,
    feature_state_id: str | None,
    holding_days_open: int,
) -> dict[str, Any]:
    return {
        "date": _iso(date_value),
        "symbol": symbol,
        "state_from": state_from,
        "state_to": state_to,
        "position_notation_before": notation_before,
        "position_notation_after": notation_after,
        "action_taken": action_taken,
        "trigger_reason_code": trigger_reason_code,
        "trigger_reason_text": trigger_reason_text,
        "units_changed": int(units_changed),
        "close_price": float(close_price),
        "avg_entry_price_after": float(avg_entry_price_after),
        "realized_pnl_delta": float(realized_pnl_delta),
        "unrealized_pnl_after": float(unrealized_pnl_after),
        "cash_after": float(cash_after),
        "equity_after": float(equity_after),
        "position_id": position_id,
        "feature_state_id": feature_state_id,
        "holding_days_open": int(holding_days_open),
    }


def _simulate_window(
    repo: StockRepository,
    run_config: dict[str, Any],
    window_start: date,
    *,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if context is None:
        context = prepare_replay_window_context(repo, run_config, window_start)
    window_end = _parse_date(context["window_end"])
    dates = list(context["dates"])

    positions: dict[str, dict[str, Any]] = {}
    cash = float(run_config["initial_capital_jpy"])
    reserved_short = 0.0
    ledger: list[dict[str, Any]] = []
    selection_snapshots: list[dict[str, Any]] = []
    feature_snapshots: list[dict[str, Any]] = []
    timeline: list[dict[str, Any]] = []
    equity_curve: list[dict[str, Any]] = []
    benchmark_market: list[dict[str, Any]] = list(context["benchmark_market"])
    benchmark_universe: list[dict[str, Any]] = list(context["benchmark_universe"])
    weekly_trade_count_map: dict[str, int] = defaultdict(int)
    trade_durations: list[int] = []
    partial_days: list[int] = []
    premature_exit_count = 0
    forced_activity_events_count = 0
    realized_by_symbol: dict[str, float] = defaultdict(float)
    last_week_key: str | None = None
    last_week_has_trade = False
    peak_equity = cash

    for current in dates:
        week_key = _week_key(current)
        if last_week_key is None:
            last_week_key = week_key
        elif week_key != last_week_key:
            weekly_trade_count_map.setdefault(last_week_key, 0)
            if not last_week_has_trade and run_config["weekly_activity_required"]:
                forced_activity_events_count += 1
                candidate = None
                candidate_score = 0.0
                for symbol in run_config["universe"]:
                    feature = _context_feature(context, current, symbol)
                    if not feature:
                        continue
                    score = _signal_score(feature)
                    if candidate is None or abs(score) > abs(candidate_score) or (abs(score) == abs(candidate_score) and symbol < candidate["symbol"]):
                        candidate = {"symbol": symbol, "feature": feature, "score": score}
                        candidate_score = score
                if candidate and candidate["symbol"] not in positions:
                    side = "long" if candidate["score"] >= 0 else "short"
                    pos = _new_position(
                        symbol=candidate["symbol"],
                        side=side,
                        units=int(run_config["addon_units"][0]),
                        unit_scale=int(run_config["unit_scale"]),
                        price=_num(candidate["feature"]["close_price"]),
                        entry_date=current,
                        position_id=_stable_hash({"symbol": candidate["symbol"], "date": _iso(current), "forced": True})[:16],
                    )
                    notional = _position_market_value(pos, pos["avg_entry_price"])
                    if side == "long" and cash >= notional and notional <= run_config["gross_exposure_cap_jpy"]:
                        cash -= notional
                        positions[candidate["symbol"]] = pos
                        ledger.append(_build_trade_event(
                            date_value=current,
                            symbol=candidate["symbol"],
                            state_from="flat",
                            state_to=_position_state(pos),
                            notation_before="0-0",
                            notation_after=_position_notation(pos),
                            action_taken="forced_entry",
                            trigger_reason_code="weekly_activity_forced_trade",
                            trigger_reason_text="forced portfolio weekly activity trade",
                            units_changed=pos["logical_units"],
                            close_price=pos["avg_entry_price"],
                            avg_entry_price_after=pos["avg_entry_price"],
                            realized_pnl_delta=0.0,
                            unrealized_pnl_after=0.0,
                            cash_after=cash,
                            equity_after=cash,
                            position_id=pos["position_id"],
                            feature_state_id=candidate["feature"]["feature_state_id"],
                            holding_days_open=0,
                        ))
                        weekly_trade_count_map[last_week_key] += 1
                    elif side == "short" and notional <= run_config["gross_exposure_cap_jpy"]:
                        cash += notional
                        reserved_short += notional
                        pos["reserved_short_proceeds"] = notional
                        positions[candidate["symbol"]] = pos
                        ledger.append(_build_trade_event(
                            date_value=current,
                            symbol=candidate["symbol"],
                            state_from="flat",
                            state_to=_position_state(pos),
                            notation_before="0-0",
                            notation_after=_position_notation(pos),
                            action_taken="forced_entry",
                            trigger_reason_code="weekly_activity_forced_trade",
                            trigger_reason_text="forced portfolio weekly activity trade",
                            units_changed=pos["logical_units"],
                            close_price=pos["avg_entry_price"],
                            avg_entry_price_after=pos["avg_entry_price"],
                            realized_pnl_delta=0.0,
                            unrealized_pnl_after=0.0,
                            cash_after=cash,
                            equity_after=cash,
                            position_id=pos["position_id"],
                            feature_state_id=candidate["feature"]["feature_state_id"],
                            holding_days_open=0,
                        ))
                        weekly_trade_count_map[last_week_key] += 1
            last_week_key = week_key
            last_week_has_trade = False

        feature_map: dict[str, dict[str, Any]] = {}
        score_map: dict[str, float] = {}
        for symbol in run_config["universe"]:
            feature = _context_feature(context, current, symbol)
            if not feature:
                continue
            feature_map[symbol] = feature
            score_map[symbol] = _signal_score(feature)
            feature_snapshots.append({"date": _iso(current), "symbol": symbol, **feature})
        selection_snapshots.append({
            "date": _iso(current),
            "candidates": [{"symbol": sym, "score": score_map[sym], "feature_state_id": feature_map[sym]["feature_state_id"]} for sym in sorted(feature_map, key=lambda item: (-abs(score_map[item]), item))],
        })
        daily_actions: dict[str, dict[str, Any]] = {}

        for symbol in run_config["universe"]:
            feature = feature_map.get(symbol)
            if not feature:
                continue
            score = score_map[symbol]
            close_price = _num(feature["close_price"])
            position = positions.get(symbol)
            if position:
                holding_days = (current - position["entry_date"]).days + 1
                position["holding_sessions"] = holding_days
                profit_ratio = _position_unrealized(position, close_price) / max(1.0, _position_market_value(position, position["avg_entry_price"]))
                exit_signal = (position["side"] == "long" and score <= run_config["exit_threshold"]) or (position["side"] == "short" and score >= -run_config["exit_threshold"])
                stop_loss = profit_ratio <= run_config["stop_loss_threshold"]
                action = "hold"
                reason_code = "hold"
                reason_text = "no rule fired"
                units = 0
                if current == window_end:
                    action, reason_code, reason_text = "forced_exit", "window_end_flatten", "forced window flatten"
                    units = int(position["logical_units"])
                elif stop_loss:
                    action, reason_code, reason_text = "invalidated", "stop_loss", "stop loss hit"
                    units = int(position["logical_units"])
                elif exit_signal:
                    action, reason_code, reason_text = "full_exit", "opposite_signal", "opposite signal triggered exit"
                    units = int(position["logical_units"])
                elif position["logical_units"] > 0 and position["add_count"] >= 2 and profit_ratio >= run_config["partial_take_threshold"]:
                    action = "partial_take_1" if position["first_partial_exit_day"] is None else "partial_take_2"
                    reason_code, reason_text = "profit_take", "rule-based profit taking"
                    units = 3 if action == "partial_take_1" else 4
                    units = min(units, int(position["logical_units"]))
                elif position["add_count"] < 2 and ((position["side"] == "long" and score >= run_config["add_threshold"]) or (position["side"] == "short" and score <= -run_config["add_threshold"])):
                    action, reason_code, reason_text = "add_on", "trend_add", "trend strengthening add-on"
                    units = int(run_config["addon_units"][position["add_count"]])

                if action != "hold":
                    before_state = _position_state(position)
                    before_notation = _position_notation(position)
                    if action == "add_on":
                        next_units = int(position["logical_units"]) + units
                        next_notional = next_units * int(position["unit_scale"]) * close_price
                        if next_notional <= run_config["gross_exposure_cap_jpy"]:
                            total_notional_before = int(position["logical_units"]) * int(position["unit_scale"]) * float(position["avg_entry_price"])
                            position["avg_entry_price"] = (total_notional_before + units * int(position["unit_scale"]) * close_price) / max(1, next_units)
                            position["logical_units"] = next_units
                            position["add_count"] = int(position["add_count"]) + 1
                            cash_delta = units * int(position["unit_scale"]) * close_price
                            if position["side"] == "long":
                                cash -= cash_delta
                            else:
                                cash += cash_delta
                                reserved_short += cash_delta
                                position["reserved_short_proceeds"] = float(position["reserved_short_proceeds"]) + cash_delta
                            daily_actions[symbol] = {"action_taken": action, "trigger_reason_code": reason_code, "trigger_reason_text": reason_text, "units_changed": units, "realized_pnl_delta": 0.0}
                            last_week_has_trade = True
                            weekly_trade_count_map[week_key] += 1
                            ledger.append(_build_trade_event(
                                date_value=current,
                                symbol=symbol,
                                state_from=before_state,
                                state_to=_position_state(position),
                                notation_before=before_notation,
                                notation_after=_position_notation(position),
                                action_taken=action,
                                trigger_reason_code=reason_code,
                                trigger_reason_text=reason_text,
                                units_changed=units,
                                close_price=close_price,
                                avg_entry_price_after=position["avg_entry_price"],
                                realized_pnl_delta=0.0,
                                unrealized_pnl_after=_position_unrealized(position, close_price),
                                cash_after=cash,
                                equity_after=0.0,
                                position_id=position["position_id"],
                                feature_state_id=feature["feature_state_id"],
                                holding_days_open=holding_days,
                            ))
                    else:
                        if position["side"] == "long":
                            realized = (close_price - float(position["avg_entry_price"])) * units * int(position["unit_scale"])
                            cash += units * int(position["unit_scale"]) * close_price
                        else:
                            realized = (float(position["avg_entry_price"]) - close_price) * units * int(position["unit_scale"])
                            cash -= units * int(position["unit_scale"]) * close_price
                            reserved_short = max(0.0, reserved_short - float(position.get("reserved_short_proceeds") or 0.0))
                            position["reserved_short_proceeds"] = 0.0
                        position["realized_pnl_cum"] = float(position["realized_pnl_cum"]) + realized
                        realized_by_symbol[symbol] += realized
                        if action in {"partial_take_1", "partial_take_2"}:
                            position["logical_units"] = max(0, int(position["logical_units"]) - units)
                            if position["first_partial_exit_day"] is None:
                                position["first_partial_exit_day"] = holding_days
                            position["exit_state"] = action
                            partial_days.append(holding_days)
                        else:
                            position["logical_units"] = 0
                            position["closed"] = True
                            position["exit_state"] = action
                            position["exit_reason_code"] = reason_code
                            position["exit_date"] = current
                            trade_durations.append(holding_days)
                            if holding_days < 20 and realized > 0:
                                premature_exit_count += 1
                            positions.pop(symbol, None)
                        daily_actions[symbol] = {"action_taken": action, "trigger_reason_code": reason_code, "trigger_reason_text": reason_text, "units_changed": units, "realized_pnl_delta": realized}
                        last_week_has_trade = True
                        weekly_trade_count_map[week_key] += 1
                        ledger.append(_build_trade_event(
                            date_value=current,
                            symbol=symbol,
                            state_from=before_state,
                            state_to=_position_state(position),
                            notation_before=before_notation,
                            notation_after=_position_notation(position),
                            action_taken=action,
                            trigger_reason_code=reason_code,
                            trigger_reason_text=reason_text,
                            units_changed=units,
                            close_price=close_price,
                            avg_entry_price_after=float(position["avg_entry_price"]),
                            realized_pnl_delta=realized,
                            unrealized_pnl_after=_position_unrealized(position if symbol in positions else None, close_price),
                            cash_after=cash,
                            equity_after=0.0,
                            position_id=position["position_id"],
                            feature_state_id=feature["feature_state_id"],
                            holding_days_open=holding_days,
                        ))
            else:
                if abs(score) >= run_config["entry_threshold"]:
                    side = "long" if score >= 0 else "short"
                    units = int(run_config["addon_units"][0])
                    pos = _new_position(
                        symbol=symbol,
                        side=side,
                        units=units,
                        unit_scale=int(run_config["unit_scale"]),
                        price=close_price,
                        entry_date=current,
                        position_id=_stable_hash({"symbol": symbol, "date": _iso(current), "side": side})[:16],
                    )
                    notional = _position_market_value(pos, pos["avg_entry_price"])
                    if notional <= run_config["gross_exposure_cap_jpy"]:
                        if side == "long" and cash >= notional:
                            cash -= notional
                            positions[symbol] = pos
                            daily_actions[symbol] = {"action_taken": "enter_long", "trigger_reason_code": "entry_signal", "trigger_reason_text": "long entry signal", "units_changed": units, "realized_pnl_delta": 0.0}
                            last_week_has_trade = True
                            weekly_trade_count_map[week_key] += 1
                            ledger.append(_build_trade_event(
                                date_value=current,
                                symbol=symbol,
                                state_from="flat",
                                state_to=_position_state(pos),
                                notation_before="0-0",
                                notation_after=_position_notation(pos),
                                action_taken="enter_long",
                                trigger_reason_code="entry_signal",
                                trigger_reason_text="long entry signal",
                                units_changed=units,
                                close_price=close_price,
                                avg_entry_price_after=close_price,
                                realized_pnl_delta=0.0,
                                unrealized_pnl_after=0.0,
                                cash_after=cash,
                                equity_after=0.0,
                                position_id=pos["position_id"],
                                feature_state_id=feature["feature_state_id"],
                                holding_days_open=0,
                            ))

        total_unrealized = 0.0
        total_gross = 0.0
        total_net = 0.0
        for symbol in run_config["universe"]:
            pos = positions.get(symbol)
            feature = feature_map.get(symbol)
            close_price = _num(feature["close_price"]) if feature else None
            unrealized = _position_unrealized(pos, close_price)
            market_value = _position_market_value(pos, close_price)
            total_unrealized += unrealized
            total_gross += market_value
            if pos:
                total_net += market_value if pos["side"] == "long" else -market_value
            timeline.append({
                "date": _iso(current),
                "symbol": symbol,
                "position_notation": _position_notation(pos),
                "long_units": _position_units(pos)[1],
                "short_units": _position_units(pos)[0],
                "avg_entry_price": None if not pos else float(pos["avg_entry_price"]),
                "close_price": close_price,
                "market_value": market_value,
                "unrealized_pnl": unrealized,
                "realized_pnl_cum": float(realized_by_symbol.get(symbol, 0.0)),
                "holding_days_open": 0 if not pos else (current - pos["entry_date"]).days + 1,
                "cash_after": cash,
                "equity_after": 0.0,
                "action_taken": daily_actions.get(symbol, {}).get("action_taken", "hold"),
                "trigger_reason_code": daily_actions.get(symbol, {}).get("trigger_reason_code", "hold"),
                "trigger_reason_text": daily_actions.get(symbol, {}).get("trigger_reason_text", "no rule fired"),
                "feature_state_id": None if not feature else feature["feature_state_id"],
            })
        equity = cash + total_unrealized
        peak_equity = max(peak_equity, equity)
        drawdown = 0.0 if peak_equity <= 0 else equity / peak_equity - 1.0
        for row in timeline[-len(run_config["universe"]):]:
            row["equity_after"] = equity
        equity_curve.append({
            "date": _iso(current),
            "cash": cash,
            "equity": equity,
            "gross_exposure": total_gross,
            "net_exposure": total_net,
            "reserved_short_proceeds": reserved_short,
            "drawdown": drawdown,
        })
    portfolio_return = equity_curve[-1]["equity"] / run_config["initial_capital_jpy"] - 1.0
    if last_week_key is not None:
        weekly_trade_count_map.setdefault(last_week_key, 0)
    market_return = 0.0
    if len(benchmark_market) >= 2 and benchmark_market[0]["close_price"]:
        market_return = benchmark_market[-1]["close_price"] / benchmark_market[0]["close_price"] - 1.0
    universe_equity = run_config["initial_capital_jpy"]
    universe_curve: list[dict[str, Any]] = []
    for row in benchmark_universe:
        if row["daily_return"] is not None:
            universe_equity *= 1.0 + float(row["daily_return"])
        universe_curve.append({"date": row["date"], "equity": universe_equity, **row})
    universe_return = universe_equity / run_config["initial_capital_jpy"] - 1.0 if universe_curve else 0.0
    excess_vs_market = portfolio_return - market_return
    excess_vs_universe = portfolio_return - universe_return
    avg_net_exposure = _avg([abs(float(item["net_exposure"])) / run_config["initial_capital_jpy"] for item in equity_curve]) or 0.0
    avg_gross_exposure = _avg([float(item["gross_exposure"]) / run_config["initial_capital_jpy"] for item in equity_curve]) or 0.0
    exposure_adjusted_excess = excess_vs_universe / max(0.25, avg_gross_exposure or 0.25)
    max_drawdown = min((float(item["drawdown"]) for item in equity_curve), default=0.0)
    turnover = sum(float(item["gross_exposure"]) for item in equity_curve) / max(1.0, run_config["initial_capital_jpy"] * len(equity_curve))
    avg_holding_days = _avg(trade_durations) or 0.0
    median_holding_days = _med(trade_durations) or 0.0
    pct_trades_over_20d = (sum(1 for item in trade_durations if item > 20) / len(trade_durations)) if trade_durations else 0.0
    avg_days_to_first_partial_exit = _avg(partial_days)
    weeks_with_no_trade = sum(1 for count in weekly_trade_count_map.values() if count == 0)
    weekly_activity_pass_rate = 1.0 - (weeks_with_no_trade / max(1, len(weekly_trade_count_map)))
    concentration_penalty = _avg([abs(float(row["market_value"])) / max(1.0, run_config["initial_capital_jpy"]) for row in timeline]) or 0.0
    long_hold_bonus = max(0.0, (avg_holding_days - 20.0) / 20.0)
    weights = run_config["weights"]
    final_score = (
        weights["total_return"] * portfolio_return
        + weights["excess_vs_universe"] * excess_vs_universe
        + weights["exposure_adjusted_excess"] * exposure_adjusted_excess
        + weights["median_window_excess"] * excess_vs_universe
        + weights["worst_window_excess"] * min(excess_vs_universe, excess_vs_market)
        + weights["max_drawdown"] * abs(max_drawdown)
        + weights["turnover"] * turnover
        + weights["concentration"] * concentration_penalty
        + weights["weekly_activity"] * (1.0 - weekly_activity_pass_rate)
        + weights["long_hold"] * long_hold_bonus
        + weights["premature_exit"] * premature_exit_count
    )
    return {
        "schema_version": REPLAY_SCHEMA_VERSION,
        "window_start_date": _iso(window_start),
        "window_end_date": _iso(window_end),
        "run_config": run_config,
        "selection_rule_change_log": build_replay_change_log(run_config),
        "daily_selection_snapshot": selection_snapshots,
        "feature_snapshot": feature_snapshots,
        "positions_timeline": timeline,
        "trade_ledger": ledger,
        "daily_equity_curve": equity_curve,
        "benchmark_market": {"schema_version": REPLAY_SCHEMA_VERSION, "symbol": run_config["market_benchmark_symbol"], "series": benchmark_market},
        "benchmark_universe": {"schema_version": REPLAY_SCHEMA_VERSION, "mode": "equal_weight", "series": benchmark_universe, "equity_curve": universe_curve},
        "relative_performance": {
            "schema_version": REPLAY_SCHEMA_VERSION,
            "portfolio_return_3m": portfolio_return,
            "benchmark_market_return_3m": market_return,
            "benchmark_universe_return_3m": universe_return,
            "excess_vs_market": excess_vs_market,
            "excess_vs_universe": excess_vs_universe,
            "average_net_exposure": avg_net_exposure,
            "daily_net_exposure_series": [{"date": row["date"], "net_exposure": row["net_exposure"], "gross_exposure": row["gross_exposure"]} for row in equity_curve],
            "exposure_adjusted_excess": exposure_adjusted_excess,
            "median_window_excess": excess_vs_universe,
            "worst_window_excess": min(excess_vs_universe, excess_vs_market),
        },
        "window_summary": {
            "schema_version": REPLAY_SCHEMA_VERSION,
            "policy_id": run_config["policy_id"],
            "policy_version": run_config["policy_version"],
            "window_start_date": _iso(window_start),
            "window_end_date": _iso(window_end),
            "window_months": int(run_config["window_months"]),
            "portfolio_return_3m": portfolio_return,
            "benchmark_market_return_3m": market_return,
            "benchmark_universe_return_3m": universe_return,
            "excess_vs_market": excess_vs_market,
            "excess_vs_universe": excess_vs_universe,
            "average_net_exposure": avg_net_exposure,
            "avg_holding_days": avg_holding_days,
            "median_holding_days": median_holding_days,
            "pct_trades_over_20d": pct_trades_over_20d,
            "turnover": turnover,
            "premature_exit_count": premature_exit_count,
            "avg_days_to_first_partial_exit": avg_days_to_first_partial_exit,
            "weekly_trade_count_map": dict(weekly_trade_count_map),
            "weeks_with_no_trade": weeks_with_no_trade,
            "weekly_activity_pass_rate": weekly_activity_pass_rate,
            "forced_activity_events_count": forced_activity_events_count,
            "max_drawdown": max_drawdown,
            "concentration_penalty": concentration_penalty,
            "long_hold_bonus": long_hold_bonus,
            "premature_exit_penalty": premature_exit_count,
            "final_score": final_score,
            "selection_rule_signatures": run_config["selection_rule_signatures"],
            "last_change_reason_code": _text((run_config.get("selection_rule_change_log") or [{}])[-1].get("reason_code")),
            "last_change_reason_text": _text((run_config.get("selection_rule_change_log") or [{}])[-1].get("reason_text")),
        },
    }


def build_replay_window(repo: StockRepository, payload: dict[str, Any]) -> dict[str, Any]:
    run_config = normalize_replay_run_config(payload)
    if not run_config["window_start_dates"]:
        raise ValueError("window_start_date is required")
    context_cache: dict[str, dict[str, Any]] = {}
    def _window_context(item: str) -> dict[str, Any]:
        key = _text(item)
        cached = context_cache.get(key)
        if cached is not None:
            return cached
        prepared = prepare_replay_window_context(repo, run_config, _parse_date(key))
        context_cache[key] = prepared
        return prepared
    if len(run_config["window_start_dates"]) > 1:
        windows = [_simulate_window(repo, run_config, _parse_date(item), context=_window_context(item)) for item in run_config["window_start_dates"]]
        leaderboard_rows = [
            {
                "window_start_date": item["window_summary"]["window_start_date"],
                "window_end_date": item["window_summary"]["window_end_date"],
                "final_score": item["window_summary"]["final_score"],
                "portfolio_return_3m": item["window_summary"]["portfolio_return_3m"],
                "excess_vs_universe": item["window_summary"]["excess_vs_universe"],
                "exposure_adjusted_excess": item["relative_performance"]["exposure_adjusted_excess"],
                "median_window_excess": item["relative_performance"]["median_window_excess"],
                "worst_window_excess": item["relative_performance"]["worst_window_excess"],
                "weekly_activity_pass_rate": item["window_summary"]["weekly_activity_pass_rate"],
            }
            for item in windows
        ]
        leaderboard_rows.sort(key=lambda row: (-float(row["final_score"]), row["window_start_date"]))
        return {
            "schema_version": REPLAY_SCHEMA_VERSION,
            "run_config": run_config,
            "windows": windows,
            "multiwindow_leaderboard": {
                "schema_version": REPLAY_SCHEMA_VERSION,
                "policy_id": run_config["policy_id"],
                "policy_version": run_config["policy_version"],
                "selection_rule_signatures": run_config["selection_rule_signatures"],
                "rows": leaderboard_rows,
                "median_window_excess": _med([float(row["excess_vs_universe"]) for row in leaderboard_rows]) or 0.0,
                "worst_window_excess": min((float(row["excess_vs_universe"]) for row in leaderboard_rows), default=0.0),
            },
        }
    first_window_start = run_config["window_start_dates"][0]
    window = _simulate_window(repo, run_config, _parse_date(first_window_start), context=_window_context(first_window_start))
    return {
        "schema_version": REPLAY_SCHEMA_VERSION,
        "run_config": run_config,
        "window": window,
        "multiwindow_leaderboard": {
            "schema_version": REPLAY_SCHEMA_VERSION,
            "policy_id": run_config["policy_id"],
            "policy_version": run_config["policy_version"],
            "selection_rule_signatures": run_config["selection_rule_signatures"],
            "rows": [
                {
                    "window_start_date": window["window_summary"]["window_start_date"],
                    "window_end_date": window["window_summary"]["window_end_date"],
                    "final_score": window["window_summary"]["final_score"],
                    "portfolio_return_3m": window["window_summary"]["portfolio_return_3m"],
                    "excess_vs_universe": window["window_summary"]["excess_vs_universe"],
                    "exposure_adjusted_excess": window["relative_performance"]["exposure_adjusted_excess"],
                    "median_window_excess": window["relative_performance"]["median_window_excess"],
                    "worst_window_excess": window["relative_performance"]["worst_window_excess"],
                    "weekly_activity_pass_rate": window["window_summary"]["weekly_activity_pass_rate"],
                }
            ],
            "median_window_excess": window["relative_performance"]["median_window_excess"],
            "worst_window_excess": window["relative_performance"]["worst_window_excess"],
        },
    }


def build_replay_suite(repo: StockRepository, payload: dict[str, Any]) -> dict[str, Any]:
    return build_replay_window(repo, payload)


def persist_replay_window(_: dict[str, Any], __: Any) -> None:
    raise NotImplementedError("backend service persists replay artifacts")


def persist_replay_suite(_: dict[str, Any], __: Any) -> None:
    raise NotImplementedError("backend service persists replay artifacts")
