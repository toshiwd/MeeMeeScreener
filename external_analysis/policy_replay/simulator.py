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
DEFAULT_COST_MODEL = {
    "schema_version": "tradex_daily_action_cost_model_v1",
    "enabled": True,
    "commission_bps": 5.0,
    "slippage_bps": 5.0,
    "tax_or_fee_bps": 0.0,
    "min_fee": 0.0,
    "status": "provisional_placeholder",
    "notes": "Conservative placeholder for research replay only; replace with broker-specific assumptions before candidate adoption.",
}
LEDGER_ACTION_MAP = {
    "enter_long": "buy",
    "enter_short": "hedge",
    "forced_entry": "buy",
    "add_on": "add",
    "partial_take_1": "take_profit",
    "partial_take_2": "take_profit",
    "full_exit": "exit",
    "forced_exit": "exit",
    "invalidated": "exit",
    "hold": "hold",
}


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


def _first_defined(*values: Any, default: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return default


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


def normalize_cost_model(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    raw = dict(payload or {})
    model = dict(DEFAULT_COST_MODEL)
    aliases = {
        "transaction_cost_bps": "commission_bps",
        "fee_bps": "commission_bps",
        "fees_bps": "commission_bps",
    }
    for source, target in aliases.items():
        if source in raw and target not in raw:
            raw[target] = raw[source]
    for key in ("schema_version", "status", "notes"):
        if raw.get(key) is not None:
            model[key] = raw[key]
    for key in ("commission_bps", "slippage_bps", "tax_or_fee_bps", "min_fee"):
        if raw.get(key) is not None:
            model[key] = max(0.0, _num(raw.get(key)))
    if raw.get("enabled") is not None:
        model["enabled"] = bool(raw.get("enabled"))
    if not payload:
        model["input_source"] = "default_provisional_placeholder"
    else:
        model["input_source"] = "caller_supplied"
    return model


def _execution_model(execution_convention: str) -> dict[str, Any]:
    requested = _text(execution_convention, fallback="close_close_research_convention")
    next_open_requested = requested in {"next_session_open", "next_trading_day_open", "next_session_open_after_signal_date"}
    return {
        "schema_version": "tradex_daily_action_execution_model_v1",
        "requested_execution_convention": requested,
        "active_execution_model": "next_session_open" if next_open_requested else "close_to_close_baseline",
        "close_to_close_baseline": {
            "supported": True,
            "status": "supported",
            "optimism_warning": "Current policy replay fills at current close and should be treated as baseline, not realistic execution proof.",
        },
        "next_session_open": {
            "supported": True,
            "status": "supported",
            "requested": next_open_requested,
            "missing_capabilities": [],
            "fill_timing": "decision_date -> next trading session open",
        },
        "capability_flags": {
            "close_to_close_baseline_supported": True,
            "next_session_open_supported": True,
            "cost_slippage_supported": True,
            "portfolio_daily_action_ledger_supported": True,
        },
    }


def _trade_costs(notional: float, cost_model: dict[str, Any]) -> tuple[float, float, float]:
    if not bool(cost_model.get("enabled", True)):
        return 0.0, 0.0, 0.0
    base = abs(float(notional))
    commission = max(float(cost_model.get("min_fee") or 0.0), base * _num(cost_model.get("commission_bps")) / 10_000.0)
    slippage = base * _num(cost_model.get("slippage_bps")) / 10_000.0
    tax_or_fee = base * _num(cost_model.get("tax_or_fee_bps")) / 10_000.0
    return commission + slippage + tax_or_fee, slippage, commission + tax_or_fee


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
            "entry_threshold": _num(_first_defined(policy.get("entry_threshold"), payload.get("entry_threshold"), default=0.55)),
            "direction_mode": "both",
        },
    )
    add_rule = _rule_block(
        payload,
        name="add_rule",
        fallback={
            "add_threshold": _num(_first_defined(policy.get("add_threshold"), payload.get("add_threshold"), default=0.80)),
            "addon_units": [int(v) for v in (payload.get("addon_units") or [2, 3, 5])],
        },
    )
    partial_take_rule = _rule_block(
        payload,
        name="partial_take_rule",
        fallback={
            "partial_take_threshold": _num(_first_defined(policy.get("partial_take_threshold"), payload.get("partial_take_threshold"), default=0.08)),
        },
    )
    full_exit_rule = _rule_block(
        payload,
        name="full_exit_rule",
        fallback={
            "exit_threshold": _num(_first_defined(policy.get("exit_threshold"), payload.get("exit_threshold"), default=-0.10)),
            "stop_loss_threshold": _num(_first_defined(policy.get("stop_loss_threshold"), payload.get("stop_loss_threshold"), default=-0.06)),
        },
    )
    sizing_rule = _rule_block(
        payload,
        name="sizing_rule",
        fallback={
            "initial_capital_jpy": _num(_first_defined(capital.get("initial_capital_jpy"), payload.get("initial_capital_jpy"), default=DEFAULT_INITIAL_CAPITAL_JPY)),
            "gross_exposure_cap_jpy": _num(_first_defined(capital.get("gross_exposure_cap_jpy"), payload.get("gross_exposure_cap_jpy"), default=DEFAULT_GROSS_CAP_JPY)),
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


def _normalize_action_policy(payload: dict[str, Any]) -> dict[str, Any]:
    raw = dict(payload.get("action_policy") or {})
    entry_gate = dict(raw.get("entry_gate") or {})
    mode = _text(raw.get("mode") or payload.get("action_policy_mode") or "legacy")
    action_policy = {
        "schema_version": "tradex_daily_action_policy_v1",
        "mode": mode,
        "enabled": bool(raw.get("enabled", mode != "legacy")),
        "allow_long_entries": bool(raw.get("allow_long_entries", True)),
        "allow_short_entries": bool(raw.get("allow_short_entries", True)),
        "allow_position_management": bool(raw.get("allow_position_management", True)),
        "allow_weekly_activity": bool(raw.get("allow_weekly_activity", payload.get("weekly_activity_required", True))),
        "entry_gate": {
            "enabled": bool(entry_gate.get("enabled", False)),
            "market_ret20_min": _num(_first_defined(entry_gate.get("market_ret20_min"), default=-0.01)),
            "breadth_above_ma20_max": _num(_first_defined(entry_gate.get("breadth_above_ma20_max"), default=0.50)),
            "liquidity20d_min": _num(_first_defined(entry_gate.get("liquidity20d_min"), default=50_000_000.0)),
            "turnover_z20_min": _num(_first_defined(entry_gate.get("turnover_z20_min"), default=0.0)),
            "diff20_pct_max": _num(_first_defined(entry_gate.get("diff20_pct_max"), default=0.01)),
            "candle_upper_wick_ratio_max": _num(_first_defined(entry_gate.get("candle_upper_wick_ratio_max"), default=0.30)),
            "timing_override_enabled": bool(entry_gate.get("timing_override_enabled", False)),
            "timing_override_reason_codes": [str(item) for item in (entry_gate.get("timing_override_reason_codes") or ["timing_block"]) if _text(item)],
            "timing_override_rank_max": (
                int(entry_gate["timing_override_rank_max"])
                if entry_gate.get("timing_override_rank_max") is not None
                else None
            ),
            "timing_override_score_min": (
                float(entry_gate["timing_override_score_min"])
                if entry_gate.get("timing_override_score_min") is not None
                else None
            ),
        },
        "notes": list(raw.get("notes") or []),
    }
    if not action_policy["enabled"]:
        action_policy["allow_long_entries"] = bool(raw.get("allow_long_entries", True))
        action_policy["allow_short_entries"] = bool(raw.get("allow_short_entries", True))
        action_policy["allow_position_management"] = bool(raw.get("allow_position_management", True))
    return action_policy


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
    action_policy = _normalize_action_policy(payload)
    cost_model = normalize_cost_model(payload.get("cost_model") if isinstance(payload.get("cost_model"), dict) else None)
    execution_model = _execution_model(_text(payload.get("execution_convention") or "close_close_research_convention"))
    run_config = {
        "schema_version": REPLAY_SCHEMA_VERSION,
        "policy_id": _text(payload.get("policy_id") or policy.get("policy_id") or "tradex_policy_replay_v1"),
        "policy_version": _text(payload.get("policy_version") or policy.get("policy_version") or "v1"),
        "window_start_date": _text(payload.get("window_start_date")),
        "decision_window_start_date": _text(payload.get("decision_window_start_date") or payload.get("window_start_date")),
        "decision_window_end_date": _text(
            payload.get("decision_window_end_date")
            or payload.get("decision_window_start_date")
            or payload.get("window_start_date")
        ),
        "execution_buffer_days": max(1, int(_first_defined(payload.get("execution_buffer_days"), default=1))),
        "outcome_buffer_days": max(
            1,
            int(
                _first_defined(
                    payload.get("outcome_buffer_days"),
                    default=max(20, int(payload.get("window_months") or 3) * 21),
                )
            ),
        ),
        "window_start_dates": [_text(item) for item in payload.get("window_start_dates") or [] if _text(item)],
        "window_months": int(payload.get("window_months") or 3),
        "universe": [_text(item) for item in payload.get("universe") or [] if _text(item)],
        "market_benchmark_symbol": _text(payload.get("market_benchmark_symbol") or DEFAULT_MARKET_BENCHMARK_SYMBOL),
        "initial_capital_jpy": _num(_first_defined(capital.get("initial_capital_jpy"), payload.get("initial_capital_jpy"), default=DEFAULT_INITIAL_CAPITAL_JPY)),
        "gross_exposure_cap_jpy": _num(_first_defined(capital.get("gross_exposure_cap_jpy"), payload.get("gross_exposure_cap_jpy"), default=DEFAULT_GROSS_CAP_JPY)),
        "unit_scale": int(payload.get("unit_scale") or 100),
        "addon_units": [int(v) for v in (payload.get("addon_units") or [2, 3, 5])],
        "entry_threshold": _num(_first_defined(policy.get("entry_threshold"), payload.get("entry_threshold"), default=0.55)),
        "exit_threshold": _num(_first_defined(policy.get("exit_threshold"), payload.get("exit_threshold"), default=-0.10)),
        "add_threshold": _num(_first_defined(policy.get("add_threshold"), payload.get("add_threshold"), default=0.80)),
        "partial_take_threshold": _num(_first_defined(policy.get("partial_take_threshold"), payload.get("partial_take_threshold"), default=0.08)),
        "stop_loss_threshold": _num(_first_defined(policy.get("stop_loss_threshold"), payload.get("stop_loss_threshold"), default=-0.06)),
        "weekly_activity_required": bool(payload.get("weekly_activity_required", True)),
        "short_cash_reusable": bool(payload.get("short_cash_reusable", False)),
        "execution_convention": _text(payload.get("execution_convention") or "close_close_research_convention"),
        "execution_model": execution_model,
        "cost_model": cost_model,
        "action_policy": action_policy,
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
    action_policy_signature = _stable_hash(run_config["action_policy"]) if run_config["action_policy"].get("mode") != "legacy" or run_config["action_policy"].get("enabled") else ""
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
            "action_policy_signature": action_policy_signature,
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
            "execution_model": run_config["execution_model"],
            "cost_model": run_config["cost_model"],
        }
    )
    run_config["action_policy_signature"] = action_policy_signature
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


def _series_row_by_date(rows: list[tuple[Any, ...]], current: date) -> tuple[Any, ...] | None:
    for row in rows:
        if len(row) >= 5 and _bar_date(row[0]) == current:
            return row
    return None


def _series_open_price(rows: list[tuple[Any, ...]], current: date) -> float | None:
    row = _series_row_by_date(rows, current)
    if row is None or len(row) < 2:
        return None
    return _num(row[1]) if row[1] is not None else None


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
    opens = [_num(row[1]) for row in current_rows]
    highs = [_num(row[2]) for row in current_rows]
    lows = [_num(row[3]) for row in current_rows]
    volumes = [_num(row[5]) if len(row) >= 6 else 0.0 for row in current_rows]
    close = closes[-1]
    prev_close = closes[-2] if len(closes) >= 2 else None
    open_price = opens[-1]
    high_price = highs[-1]
    low_price = lows[-1]
    current_turnover = close * max(0.0, volumes[-1])
    turnover_series = [float(_num(c) * max(0.0, _num(v))) for c, v in zip(closes, volumes, strict=True)]
    liquidity20d = sum(turnover_series[-20:]) if turnover_series else None
    turnover_z20 = None
    if len(turnover_series) >= 21:
        recent = turnover_series[-21:-1]
        current_turnover = turnover_series[-1]
        mean_recent = _avg(recent)
        if mean_recent is not None:
            variance = sum((item - mean_recent) ** 2 for item in recent) / len(recent) if recent else 0.0
            std_recent = variance ** 0.5
            turnover_z20 = None if std_recent == 0 else (current_turnover - mean_recent) / std_recent
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
    ma20_value = _avg(closes[-20:]) if len(closes) >= 20 else _avg(closes)
    diff20_pct = None if ma20_value in {None, 0} else close / ma20_value - 1.0
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
        "open_price": open_price,
        "high_price": high_price,
        "low_price": low_price,
        "daily_return_1d": None if prev_close is None else close / prev_close - 1.0,
        "daily_return_5d": close / closes[-6] - 1.0 if len(closes) >= 6 else None,
        "daily_return_20d": close / closes[-21] - 1.0 if len(closes) >= 21 else None,
        "weekly_return_1w": weekly_ret,
        "monthly_return_1m": monthly_ret,
        "ma20": ma20_value,
        "ma60": _avg(closes[-60:]) if len(closes) >= 60 else _avg(closes),
        "high_20d": max(highs[-20:]) if len(highs) >= 20 else max(highs),
        "low_20d": min(lows[-20:]) if len(lows) >= 20 else min(lows),
        "diff20_pct": diff20_pct,
        "candle_upper_wick_ratio": None if high_price <= low_price else max(0.0, high_price - max(open_price, close)) / max(1e-9, high_price - low_price),
        "turnover20": liquidity20d,
        "liquidity20d": liquidity20d,
        "turnover_z20": turnover_z20,
    }


def prepare_replay_window_context(repo: StockRepository, run_config: dict[str, Any], window_start: date) -> dict[str, Any]:
    window_end = _add_months(window_start, int(run_config["window_months"])) - timedelta(days=1)
    symbols = list(dict.fromkeys([*run_config["universe"], run_config["market_benchmark_symbol"]]))
    asof_dt = int(datetime.combine(window_end, datetime.min.time(), tzinfo=timezone.utc).timestamp())
    series_map = _extract_series_map(repo, symbols, asof_dt=asof_dt)
    series_lookup = {
        symbol: {
            _bar_date(row[0]): row
            for row in rows
            if len(row) >= 5
        }
        for symbol, rows in series_map.items()
    }
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
    benchmark_close_by_date = {item["date"]: _num(item.get("close_price")) for item in benchmark_market if item.get("close_price") is not None}
    market_context_by_date: dict[str, dict[str, Any]] = {}
    for index, current in enumerate(dates):
        date_key = _iso(current)
        current_features = feature_grid.get(date_key) or {}
        breadth_total = 0
        breadth_above_ma20 = 0
        for symbol in run_config["universe"]:
            feature = current_features.get(symbol)
            if not feature or feature.get("ma20") is None:
                continue
            breadth_total += 1
            if _num(feature.get("close_price")) > _num(feature.get("ma20")):
                breadth_above_ma20 += 1
        breadth_ratio = None if breadth_total == 0 else breadth_above_ma20 / breadth_total
        market_ret20 = None
        if index >= 20:
            prev_key = _iso(dates[index - 20])
            prev_close = benchmark_close_by_date.get(prev_key)
            current_close = benchmark_close_by_date.get(date_key)
            if prev_close not in {None, 0} and current_close is not None:
                market_ret20 = current_close / prev_close - 1.0
        market_context_by_date[date_key] = {
            "market_ret20": market_ret20,
            "breadth_above_ma20": breadth_ratio,
            "regime_block": bool(
                market_ret20 is not None
                and breadth_ratio is not None
                and market_ret20 < -0.01
                and breadth_ratio <= 0.50
            ),
        }
    return {
        "window_start": _iso(window_start),
        "window_end": _iso(window_end),
        "asof_dt": asof_dt,
        "symbols": symbols,
        "series_map": series_map,
        "series_lookup": series_lookup,
        "dates": dates,
        "feature_grid": feature_grid,
        "market_context_by_date": market_context_by_date,
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


def _signal_score(feature: dict[str, Any], selection_weights: dict[str, Any] | None = None) -> float:
    close = _num(feature["close_price"])
    ma20 = _num(feature.get("ma20")) or close
    ma60 = _num(feature.get("ma60")) or close
    score = (
        0.10 * _num(feature.get("daily_return_1d"))
        + 0.22 * _num(feature.get("daily_return_5d"))
        + 0.30 * _num(feature.get("daily_return_20d"))
        + 0.18 * _num(feature.get("weekly_return_1w"))
        + 0.14 * _num(feature.get("monthly_return_1m"))
        + 0.03 * (close / ma20 - 1.0)
        + 0.03 * (close / ma60 - 1.0)
    )
    weights = dict(selection_weights or {})
    if weights:
        score += 0.05 * abs(float(weights.get("excess_vs_universe") or 0.0)) * _num(feature.get("monthly_return_1m"))
        score += 0.04 * abs(float(weights.get("exposure_adjusted_excess") or 0.0)) * _num(feature.get("daily_return_20d"))
        score += 0.03 * abs(float(weights.get("median_window_excess") or 0.0)) * _num(feature.get("weekly_return_1w"))
        score += 0.03 * abs(float(weights.get("worst_window_excess") or 0.0)) * min(_num(feature.get("daily_return_1d")), _num(feature.get("daily_return_5d")))
        score += 0.04 * abs(float(weights.get("long_hold") or 0.0)) * (close / ma60 - 1.0)
        score += 0.02 * abs(float(weights.get("premature_exit") or 0.0)) * (-abs(_num(feature.get("daily_return_1d"))))
        score += 0.02 * abs(float(weights.get("turnover") or 0.0)) * (-abs(_num(feature.get("daily_return_5d"))))
        score += 0.01 * abs(float(weights.get("weekly_activity") or 0.0)) * _num(feature.get("weekly_return_1w"))
    return score


def _entry_gate_decision(
    feature: dict[str, Any],
    market_context: dict[str, Any] | None,
    action_policy: dict[str, Any] | None,
    *,
    score: float | None = None,
    candidate_rank: int | None = None,
) -> tuple[bool, str, str, list[str]]:
    policy = dict(action_policy or {})
    gate = dict(policy.get("entry_gate") or {})
    if not bool(gate.get("enabled", False)):
        return True, "entry_gate_pass", "entry gate disabled", []
    reason_codes: list[str] = []
    market_ret20 = None if market_context is None else market_context.get("market_ret20")
    breadth_above_ma20 = None if market_context is None else market_context.get("breadth_above_ma20")
    liquidity20d = feature.get("liquidity20d")
    turnover_z20 = feature.get("turnover_z20")
    diff20_pct = feature.get("diff20_pct")
    candle_upper_wick_ratio = feature.get("candle_upper_wick_ratio")
    if market_ret20 is not None and breadth_above_ma20 is not None:
        if float(market_ret20) < float(gate.get("market_ret20_min", -0.01)) and float(breadth_above_ma20) <= float(gate.get("breadth_above_ma20_max", 0.5)):
            reason_codes.append("regime_block")
    if liquidity20d is not None and turnover_z20 is not None:
        if float(liquidity20d) < float(gate.get("liquidity20d_min", 50_000_000.0)) and float(turnover_z20) < float(gate.get("turnover_z20_min", 0.0)):
            reason_codes.append("cost_turnover_block")
    if diff20_pct is not None and candle_upper_wick_ratio is not None:
        if float(candle_upper_wick_ratio) > float(gate.get("candle_upper_wick_ratio_max", 0.30)) and float(diff20_pct) > float(gate.get("diff20_pct_max", 0.01)):
            reason_codes.append("timing_block")
    if not reason_codes:
        return True, "entry_gate_pass", "entry gate passed", []
    if (
        gate.get("timing_override_enabled")
        and reason_codes == ["timing_block"]
        and score is not None
        and candidate_rank is not None
    ):
        timing_reason_codes = [str(item) for item in gate.get("timing_override_reason_codes") or ["timing_block"]]
        timing_rank_max = gate.get("timing_override_rank_max")
        timing_score_min = gate.get("timing_override_score_min")
        if (
            (timing_rank_max is None or int(candidate_rank) <= int(timing_rank_max))
            and (timing_score_min is None or float(score) >= float(timing_score_min))
        ):
            return True, "timing_override_pass", "timing block relaxed by strong same-day entry signal", list(dict.fromkeys([*timing_reason_codes, "entry_signal_relax"]))
    primary = "regime_block" if "regime_block" in reason_codes else ("cost_turnover_block" if "cost_turnover_block" in reason_codes else "timing_block")
    text_map = {
        "regime_block": "market regime deteriorating; stay in cash",
        "cost_turnover_block": "weak liquidity continuation; stay in cash",
        "timing_block": "late or stretched entry; stay in cash",
    }
    return False, primary, text_map.get(primary, "entry gate blocked"), reason_codes


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
        "cost_pnl_cum": 0.0,
        "slippage_pnl_cum": 0.0,
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
    decision_date: date | None = None,
    fill_date: date | None = None,
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
    cost_amount: float = 0.0,
    slippage_amount: float = 0.0,
    execution_timing: str = "close_to_close_baseline",
    execution_model: str | None = None,
    price_provenance: str | None = None,
    order_status: str = "filled",
    unfilled_reason: str | None = None,
    execution_price: float | None = None,
    no_lookahead_check: dict[str, Any] | None = None,
    notes: list[str] | None = None,
) -> dict[str, Any]:
    decision_date = decision_date or date_value
    if fill_date is None and order_status == "unfilled":
        effective_fill_date: date | None = None
    else:
        effective_fill_date = fill_date or date_value
    fill_date = effective_fill_date
    execution_model = execution_model or execution_timing
    if price_provenance is None:
        price_provenance = None if fill_date is None and order_status == "unfilled" else ("same_day_close_price" if execution_timing == "close_to_close_baseline" else "next_session_open_price")
    if no_lookahead_check is None:
        no_lookahead_check = {
            "decision_asof_date": _iso(decision_date),
            "fill_price_date": None if fill_date is None else _iso(fill_date),
            "decision_before_fill": None if fill_date is None else decision_date <= fill_date,
            "status": "pass" if fill_date is not None and decision_date <= fill_date else ("unknown" if fill_date is None else "fail"),
            "notes": [],
        }
    return {
        "date": _iso(fill_date or decision_date),
        "decision_date": _iso(decision_date),
        "fill_date": None if fill_date is None else _iso(fill_date),
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
        "cost_amount": float(cost_amount),
        "slippage_amount": float(slippage_amount),
        "execution_price": None if execution_price is None else float(execution_price),
        "execution_timing": execution_timing,
        "execution_model": execution_model,
        "price_provenance": price_provenance,
        "order_status": order_status,
        "unfilled_reason": unfilled_reason,
        "no_lookahead_check": no_lookahead_check,
        "notes": list(notes or []),
        "position_id": position_id,
        "feature_state_id": feature_state_id,
        "holding_days_open": int(holding_days_open),
    }


def _ledger_action(action_taken: str, side: str | None) -> str:
    action = LEDGER_ACTION_MAP.get(action_taken, action_taken)
    if action_taken == "forced_entry" and side == "short":
        return "hedge"
    return action


def _build_portfolio_daily_action_rows(
    *,
    current: date,
    run_config: dict[str, Any],
    symbols: list[str],
    feature_map: dict[str, dict[str, Any]],
    positions: dict[str, dict[str, Any]],
    daily_actions: dict[str, dict[str, Any]],
    cash: float,
    total_gross: float,
    total_net: float,
    equity: float,
    prior_equity: float,
    drawdown: float,
    realized_by_symbol: dict[str, float],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    cumulative_pnl = equity - float(run_config["initial_capital_jpy"])
    daily_pnl = equity - prior_equity
    execution_model = dict(run_config.get("execution_model") or {})
    execution_timing = _text(execution_model.get("active_execution_model"), fallback="close_to_close_baseline")
    capability_flags = dict(execution_model.get("capability_flags") or {})
    for symbol in symbols:
        feature = feature_map.get(symbol)
        position = positions.get(symbol)
        action_info = dict(daily_actions.get(symbol) or {})
        action_taken = _text(action_info.get("action_taken"), fallback="hold")
        side = _text(
            action_info.get("side") if action_info.get("side") is not None else (position.get("side") if position else None),
            fallback="cash" if action_taken in {"hold", "stay_cash"} else "long",
        )
        close_price = _num(feature.get("close_price")) if feature else None
        market_value = _position_market_value(position, close_price)
        unrealized = _position_unrealized(position, close_price)
        cost_amount = float(action_info.get("cost_amount") or 0.0)
        slippage_amount = float(action_info.get("slippage_amount") or 0.0)
        decision_date = _text(action_info.get("decision_date"), fallback=_iso(current))
        fill_date = _text(action_info.get("fill_date"), fallback=_iso(current))
        order_status = _text(action_info.get("order_status"), fallback="not_applicable" if action_taken in {"hold", "stay_cash"} else "filled")
        price_provenance = action_info.get("price_provenance")
        execution_price = action_info.get("execution_price")
        rows.append(
            {
                "schema_version": REPLAY_SCHEMA_VERSION,
                "date": _text(action_info.get("date"), fallback=_iso(current)),
                "decision_date": decision_date,
                "fill_date": fill_date,
                "symbol": symbol,
                "action": _ledger_action(action_taken, side),
                "source_action_taken": action_taken,
                "side": side,
                "reason_codes": list(action_info.get("reason_codes") or ([_text(action_info.get("trigger_reason_code"), fallback="hold")] if _text(action_info.get("trigger_reason_code")) else [])),
                "cash": float(cash),
                "gross_exposure": float(total_gross),
                "net_exposure": float(total_net),
                "position_qty": None if not position else int(position["logical_units"]) * int(position["unit_scale"]),
                "position_value": float(market_value),
                "realized_pnl": float(realized_by_symbol.get(symbol, 0.0)),
                "unrealized_pnl": float(unrealized),
                "daily_pnl": float(daily_pnl),
                "cumulative_pnl": float(cumulative_pnl),
                "drawdown": float(drawdown),
                "cost_amount": cost_amount,
                "slippage_amount": slippage_amount,
                "execution_price": execution_price if execution_price is not None else (None if action_taken == "hold" else close_price),
                "execution_timing": execution_timing,
                "execution_model": _text(action_info.get("execution_model"), fallback=execution_timing),
                "price_provenance": price_provenance,
                "data_asof": _text(action_info.get("data_asof"), fallback=_iso(current)),
                "no_lookahead_check": {
                    **dict(action_info.get("no_lookahead_check") or {}),
                    "feature_state_id": None if not feature else feature.get("feature_state_id"),
                },
                "decision_date": decision_date,
                "fill_date": fill_date,
                "order_status": order_status,
                "unfilled_reason": action_info.get("unfilled_reason"),
                "capability_flags": capability_flags,
                "notes": list(action_info.get("notes") or ([] if execution_timing == "close_to_close_baseline" else ["next_session_open_execution"])) ,
            }
        )
    if not rows:
        rows.append(
            {
                "schema_version": REPLAY_SCHEMA_VERSION,
                "date": _iso(current),
                "decision_date": _iso(current),
                "fill_date": _iso(current),
                "symbol": None,
                "action": "stay_cash",
                "source_action_taken": "stay_cash",
                "side": "cash",
                "reason_codes": ["no_candidate_features"],
                "cash": float(cash),
                "gross_exposure": float(total_gross),
                "net_exposure": float(total_net),
                "position_qty": None,
                "position_value": None,
                "realized_pnl": None,
                "unrealized_pnl": None,
                "daily_pnl": float(daily_pnl),
                "cumulative_pnl": float(cumulative_pnl),
                "drawdown": float(drawdown),
                "cost_amount": 0.0,
                "slippage_amount": 0.0,
                "execution_price": None,
                "execution_timing": execution_timing,
                "execution_model": execution_timing,
                "price_provenance": None,
                "data_asof": _iso(current),
                "no_lookahead_check": {"status": "confirmed", "feature_date_lte_decision_date": True},
                "decision_date": _iso(current),
                "fill_date": _iso(current),
                "order_status": "not_applicable",
                "unfilled_reason": None,
                "capability_flags": capability_flags,
                "notes": ["no symbol feature rows available for this date"],
            }
        )
    return rows


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
    portfolio_daily_action_ledger: list[dict[str, Any]] = []
    benchmark_market: list[dict[str, Any]] = list(context["benchmark_market"])
    benchmark_universe: list[dict[str, Any]] = list(context["benchmark_universe"])
    selection_weights = dict((run_config.get("policy_rules") or {}).get("selection_rule") or {}).get("weights") or {}
    weekly_trade_count_map: dict[str, int] = defaultdict(int)
    trade_durations: list[int] = []
    partial_days: list[int] = []
    premature_exit_count = 0
    forced_activity_events_count = 0
    realized_by_symbol: dict[str, float] = defaultdict(float)
    last_week_key: str | None = None
    last_week_has_trade = False
    peak_equity = cash
    prior_equity = cash
    cost_model = dict(run_config.get("cost_model") or DEFAULT_COST_MODEL)
    execution_timing = _text((run_config.get("execution_model") or {}).get("active_execution_model"), fallback="close_to_close_baseline")
    execution_model_config = dict(run_config.get("execution_model") or {})
    decision_window_start = _parse_date(_text(run_config.get("decision_window_start_date")) or _iso(window_start))
    decision_window_end = _parse_date(
        _text(run_config.get("decision_window_end_date"))
        or _text(run_config.get("decision_window_start_date"))
        or _iso(window_start)
    )
    decision_dates = {current for current in dates if decision_window_start <= current <= decision_window_end}
    if not decision_dates:
        decision_dates = set(dates)
    action_policy = dict(run_config.get("action_policy") or {})
    action_policy_active = bool(action_policy.get("enabled", False)) and _text(action_policy.get("mode"), fallback="legacy") != "legacy"
    allow_long_entries = bool(action_policy.get("allow_long_entries", True))
    allow_short_entries = bool(action_policy.get("allow_short_entries", True))
    allow_position_management = bool(action_policy.get("allow_position_management", True))
    allow_weekly_activity = bool(action_policy.get("allow_weekly_activity", run_config["weekly_activity_required"]))
    series_lookup = dict(context.get("series_lookup") or {})
    date_index_map = {current_date: index for index, current_date in enumerate(dates)}
    pending_orders: list[dict[str, Any]] = []

    def _next_session_fill_date(decision_date: date) -> date | None:
        index = date_index_map.get(decision_date)
        if index is None:
            return None
        next_index = index + 1
        if next_index >= len(dates):
            return None
        return dates[next_index]

    def _open_price_for(symbol: str, fill_date: date) -> float | None:
        symbol_lookup = series_lookup.get(symbol)
        if not isinstance(symbol_lookup, dict):
            return None
        row = symbol_lookup.get(fill_date)
        if row is None or len(row) < 2:
            return None
        return _num(row[1]) if row[1] is not None else None

    def _make_no_lookahead_check(decision_date: date | None, fill_date: date | None, *, status: str, notes: list[str] | None = None) -> dict[str, Any]:
        decision_text = _iso(decision_date) if decision_date is not None else None
        fill_text = _iso(fill_date) if fill_date is not None else None
        decision_before_fill = None if decision_date is None or fill_date is None else decision_date <= fill_date
        return {
            "decision_asof_date": decision_text,
            "fill_price_date": fill_text,
            "decision_before_fill": decision_before_fill,
            "status": status,
            "notes": list(notes or []),
        }

    def _record_filled_order(
        *,
        symbol: str,
        decision_date: date,
        fill_date: date,
        action_taken: str,
        trigger_reason_code: str,
        trigger_reason_text: str,
        units: int,
        side: str,
        fill_price: float,
        price_provenance: str,
        order_status: str,
        feature_state_id: str | None,
        notes: list[str] | None = None,
    ) -> None:
        nonlocal cash, reserved_short, last_week_has_trade, weekly_trade_count_map, premature_exit_count, trade_durations, partial_days
        position = positions.get(symbol)
        order_side = side if side else (_text(position.get("side")) if position else "long")
        action_label = _ledger_action(action_taken, order_side)
        current_state = _position_state(position)
        current_notation = _position_notation(position)
        current_holding_days = 0 if position is None else int((fill_date - position["entry_date"]).days) + 1
        if position is None and action_taken in {"add_on", "partial_take_1", "partial_take_2", "full_exit", "forced_exit", "invalidated"}:
            reason = "no_open_position_for_order"
            daily_actions[symbol] = {
                "action_taken": action_taken,
                "side": order_side,
                "trigger_reason_code": trigger_reason_code,
                "trigger_reason_text": trigger_reason_text,
                "units_changed": units,
                "realized_pnl_delta": 0.0,
                "cost_amount": 0.0,
                "slippage_amount": 0.0,
                "decision_date": _iso(decision_date),
                "fill_date": None,
                "execution_model": execution_timing,
                "execution_timing": execution_timing,
                "execution_price": None,
                "price_provenance": None,
                "order_status": "unfilled",
                "unfilled_reason": reason,
                "data_asof": _iso(decision_date),
                "no_lookahead_check": _make_no_lookahead_check(decision_date, None, status="unknown", notes=[reason]),
                "notes": list(notes or [reason]),
            }
            ledger.append(
                _build_trade_event(
                    decision_date=decision_date,
                    fill_date=None,
                    date_value=decision_date,
                    symbol=symbol,
                    state_from=current_state,
                    state_to=current_state,
                    notation_before=current_notation,
                    notation_after=current_notation,
                    action_taken=action_taken,
                    trigger_reason_code=trigger_reason_code,
                    trigger_reason_text=trigger_reason_text,
                    units_changed=units,
                    close_price=fill_price,
                    avg_entry_price_after=float(position["avg_entry_price"]) if position else fill_price,
                    realized_pnl_delta=0.0,
                    unrealized_pnl_after=_position_unrealized(position, fill_price),
                    cash_after=cash,
                    equity_after=cash,
                    position_id=_stable_hash({"symbol": symbol, "decision_date": _iso(decision_date), "fill_date": _iso(fill_date), "unfilled": True})[:16],
                    feature_state_id=feature_state_id,
                    holding_days_open=0,
                    cost_amount=0.0,
                    slippage_amount=0.0,
                    execution_timing=execution_timing,
                    execution_model=execution_timing,
                    execution_price=None,
                    price_provenance=None,
                    order_status="unfilled",
                    unfilled_reason=reason,
                    no_lookahead_check=_make_no_lookahead_check(decision_date, None, status="unknown", notes=[reason]),
                    notes=list(notes or [reason]),
                )
            )
            return

        if action_taken in {"enter_long", "enter_short", "forced_entry"}:
            if position is not None:
                reason = "position_already_open"
                daily_actions[symbol] = {
                    "action_taken": action_label,
                    "side": order_side,
                    "trigger_reason_code": trigger_reason_code,
                    "trigger_reason_text": trigger_reason_text,
                    "units_changed": units,
                    "realized_pnl_delta": 0.0,
                    "cost_amount": 0.0,
                    "slippage_amount": 0.0,
                    "decision_date": _iso(decision_date),
                    "fill_date": None,
                    "execution_model": execution_timing,
                    "execution_timing": execution_timing,
                    "execution_price": None,
                    "price_provenance": None,
                    "order_status": "unfilled",
                    "unfilled_reason": reason,
                    "data_asof": _iso(decision_date),
                    "no_lookahead_check": _make_no_lookahead_check(decision_date, None, status="unknown", notes=[reason]),
                    "notes": list(notes or [reason]),
                }
                ledger.append(
                    _build_trade_event(
                        decision_date=decision_date,
                        fill_date=None,
                        date_value=decision_date,
                        symbol=symbol,
                        state_from=current_state,
                        state_to=current_state,
                        notation_before=current_notation,
                        notation_after=current_notation,
                        action_taken=action_taken,
                        trigger_reason_code=trigger_reason_code,
                        trigger_reason_text=trigger_reason_text,
                        units_changed=units,
                        close_price=fill_price,
                        avg_entry_price_after=float(position["avg_entry_price"]),
                        realized_pnl_delta=0.0,
                        unrealized_pnl_after=_position_unrealized(position, fill_price),
                        cash_after=cash,
                        equity_after=cash + _position_unrealized(position, fill_price),
                        position_id=position["position_id"],
                        feature_state_id=feature_state_id,
                        holding_days_open=current_holding_days,
                        execution_timing=execution_timing,
                        execution_model=execution_timing,
                        execution_price=None,
                        price_provenance=None,
                        order_status="unfilled",
                        unfilled_reason=reason,
                        no_lookahead_check=_make_no_lookahead_check(decision_date, None, status="unknown", notes=[reason]),
                        notes=list(notes or [reason]),
                    )
                )
                return
            notional = units * int(run_config["unit_scale"]) * fill_price
            cost_amount, slippage_amount, _ = _trade_costs(notional, cost_model)
            if order_side == "long":
                if cash < notional + cost_amount or notional > run_config["gross_exposure_cap_jpy"]:
                    reason = "insufficient_cash_or_exposure_cap"
                    daily_actions[symbol] = {
                        "action_taken": action_label,
                        "side": order_side,
                        "trigger_reason_code": trigger_reason_code,
                        "trigger_reason_text": trigger_reason_text,
                        "units_changed": units,
                        "realized_pnl_delta": 0.0,
                        "cost_amount": 0.0,
                        "slippage_amount": 0.0,
                        "decision_date": _iso(decision_date),
                        "fill_date": None,
                        "execution_model": execution_timing,
                        "execution_timing": execution_timing,
                        "execution_price": None,
                        "price_provenance": None,
                        "order_status": "unfilled",
                        "unfilled_reason": reason,
                        "data_asof": _iso(decision_date),
                        "no_lookahead_check": _make_no_lookahead_check(decision_date, None, status="unknown", notes=[reason]),
                        "notes": list(notes or [reason]),
                    }
                    ledger.append(
                        _build_trade_event(
                            decision_date=decision_date,
                            fill_date=None,
                            date_value=decision_date,
                            symbol=symbol,
                            state_from="flat",
                            state_to="flat",
                            notation_before="0-0",
                            notation_after="0-0",
                            action_taken=action_taken,
                            trigger_reason_code=trigger_reason_code,
                            trigger_reason_text=trigger_reason_text,
                            units_changed=units,
                            close_price=fill_price,
                            avg_entry_price_after=fill_price,
                            realized_pnl_delta=0.0,
                            unrealized_pnl_after=0.0,
                            cash_after=cash,
                            equity_after=cash,
                            position_id=_stable_hash({"symbol": symbol, "decision_date": _iso(decision_date), "fill_date": _iso(fill_date), "unfilled": True})[:16],
                            feature_state_id=feature_state_id,
                            holding_days_open=0,
                            cost_amount=0.0,
                            slippage_amount=0.0,
                            execution_timing=execution_timing,
                            execution_model=execution_timing,
                            execution_price=None,
                            price_provenance=None,
                            order_status="unfilled",
                            unfilled_reason=reason,
                            no_lookahead_check=_make_no_lookahead_check(decision_date, None, status="unknown", notes=[reason]),
                            notes=list(notes or [reason]),
                        )
                    )
                    return
                pos = _new_position(
                    symbol=symbol,
                    side=order_side,
                    units=units,
                    unit_scale=int(run_config["unit_scale"]),
                    price=fill_price,
                    entry_date=fill_date,
                    position_id=_stable_hash({"symbol": symbol, "decision_date": _iso(decision_date), "fill_date": _iso(fill_date), "side": order_side})[:16],
                )
                cash -= notional + cost_amount
                pos["cost_pnl_cum"] = float(pos.get("cost_pnl_cum") or 0.0) + cost_amount
                pos["slippage_pnl_cum"] = float(pos.get("slippage_pnl_cum") or 0.0) + slippage_amount
                positions[symbol] = pos
            else:
                if notional > run_config["gross_exposure_cap_jpy"]:
                    reason = "gross_exposure_cap_breached"
                    daily_actions[symbol] = {
                        "action_taken": action_label,
                        "side": order_side,
                        "trigger_reason_code": trigger_reason_code,
                        "trigger_reason_text": trigger_reason_text,
                        "units_changed": units,
                        "realized_pnl_delta": 0.0,
                        "cost_amount": 0.0,
                        "slippage_amount": 0.0,
                        "decision_date": _iso(decision_date),
                        "fill_date": None,
                        "execution_model": execution_timing,
                        "execution_timing": execution_timing,
                        "execution_price": None,
                        "price_provenance": None,
                        "order_status": "unfilled",
                        "unfilled_reason": reason,
                        "data_asof": _iso(decision_date),
                        "no_lookahead_check": _make_no_lookahead_check(decision_date, None, status="unknown", notes=[reason]),
                        "notes": list(notes or [reason]),
                    }
                    ledger.append(
                        _build_trade_event(
                            decision_date=decision_date,
                            fill_date=None,
                            date_value=decision_date,
                            symbol=symbol,
                            state_from="flat",
                            state_to="flat",
                            notation_before="0-0",
                            notation_after="0-0",
                            action_taken=action_taken,
                            trigger_reason_code=trigger_reason_code,
                            trigger_reason_text=trigger_reason_text,
                            units_changed=units,
                            close_price=fill_price,
                            avg_entry_price_after=fill_price,
                            realized_pnl_delta=0.0,
                            unrealized_pnl_after=0.0,
                            cash_after=cash,
                            equity_after=cash,
                            position_id=_stable_hash({"symbol": symbol, "decision_date": _iso(decision_date), "fill_date": _iso(fill_date), "unfilled": True})[:16],
                            feature_state_id=feature_state_id,
                            holding_days_open=0,
                            cost_amount=0.0,
                            slippage_amount=0.0,
                            execution_timing=execution_timing,
                            execution_model=execution_timing,
                            execution_price=None,
                            price_provenance=None,
                            order_status="unfilled",
                            unfilled_reason=reason,
                            no_lookahead_check=_make_no_lookahead_check(decision_date, None, status="unknown", notes=[reason]),
                            notes=list(notes or [reason]),
                        )
                    )
                    return
                pos = _new_position(
                    symbol=symbol,
                    side=order_side,
                    units=units,
                    unit_scale=int(run_config["unit_scale"]),
                    price=fill_price,
                    entry_date=fill_date,
                    position_id=_stable_hash({"symbol": symbol, "decision_date": _iso(decision_date), "fill_date": _iso(fill_date), "side": order_side})[:16],
                )
                cash += notional - cost_amount
                reserved_short += notional
                pos["reserved_short_proceeds"] = notional
                pos["cost_pnl_cum"] = float(pos.get("cost_pnl_cum") or 0.0) + cost_amount
                pos["slippage_pnl_cum"] = float(pos.get("slippage_pnl_cum") or 0.0) + slippage_amount
                positions[symbol] = pos

            realized_delta = 0.0
            position_after = positions.get(symbol)
            daily_actions[symbol] = {
                "action_taken": action_label,
                "side": order_side,
                "trigger_reason_code": trigger_reason_code,
                "trigger_reason_text": trigger_reason_text,
                "units_changed": units,
                "realized_pnl_delta": realized_delta,
                "cost_amount": cost_amount,
                "slippage_amount": slippage_amount,
                "decision_date": _iso(decision_date),
                "fill_date": _iso(fill_date),
                "execution_model": execution_timing,
                "execution_timing": execution_timing,
                "execution_price": fill_price,
                "price_provenance": price_provenance,
                "order_status": order_status,
                "unfilled_reason": None,
                "data_asof": _iso(decision_date),
                "no_lookahead_check": _make_no_lookahead_check(decision_date, fill_date, status="pass", notes=list(notes or [])),
                "notes": list(notes or []),
            }
            last_week_has_trade = True
            weekly_trade_count_map[_week_key(fill_date)] += 1
            ledger.append(
                _build_trade_event(
                    decision_date=decision_date,
                    fill_date=fill_date,
                    date_value=fill_date,
                    symbol=symbol,
                    state_from="flat",
                    state_to=_position_state(position_after),
                    notation_before="0-0",
                    notation_after=_position_notation(position_after),
                    action_taken=action_taken,
                    trigger_reason_code=trigger_reason_code,
                    trigger_reason_text=trigger_reason_text,
                    units_changed=units,
                    close_price=fill_price,
                    avg_entry_price_after=float(position_after["avg_entry_price"]) if position_after else fill_price,
                    realized_pnl_delta=realized_delta,
                    unrealized_pnl_after=_position_unrealized(position_after, fill_price),
                    cash_after=cash,
                    equity_after=cash + _position_unrealized(position_after, fill_price),
                    position_id=position_after["position_id"] if position_after else _stable_hash({"symbol": symbol, "decision_date": _iso(decision_date), "fill_date": _iso(fill_date)})[:16],
                    feature_state_id=feature_state_id,
                    holding_days_open=0 if position_after is None else int((fill_date - position_after["entry_date"]).days),
                    cost_amount=cost_amount,
                    slippage_amount=slippage_amount,
                    execution_timing=execution_timing,
                    execution_model=execution_timing,
                    execution_price=fill_price,
                    price_provenance=price_provenance,
                    order_status=order_status,
                    unfilled_reason=None,
                    no_lookahead_check=_make_no_lookahead_check(decision_date, fill_date, status="pass", notes=list(notes or [])),
                    notes=list(notes or []),
                )
            )
            return

        elif action_taken == "add_on":
            if position is None:
                return
            next_units = int(position["logical_units"]) + units
            next_notional = next_units * int(position["unit_scale"]) * fill_price
            if next_notional > run_config["gross_exposure_cap_jpy"]:
                reason = "gross_exposure_cap_breached"
                daily_actions[symbol] = {
                    "action_taken": action_label,
                    "side": order_side,
                    "trigger_reason_code": trigger_reason_code,
                    "trigger_reason_text": trigger_reason_text,
                    "units_changed": units,
                    "realized_pnl_delta": 0.0,
                    "cost_amount": 0.0,
                    "slippage_amount": 0.0,
                    "decision_date": _iso(decision_date),
                    "fill_date": None,
                    "execution_model": execution_timing,
                    "execution_timing": execution_timing,
                    "execution_price": None,
                    "price_provenance": None,
                    "order_status": "unfilled",
                    "unfilled_reason": reason,
                    "data_asof": _iso(decision_date),
                    "no_lookahead_check": _make_no_lookahead_check(decision_date, None, status="unknown", notes=[reason]),
                    "notes": list(notes or [reason]),
                }
                ledger.append(
                    _build_trade_event(
                        decision_date=decision_date,
                        fill_date=None,
                        date_value=decision_date,
                        symbol=symbol,
                        state_from=current_state,
                        state_to=current_state,
                        notation_before=current_notation,
                        notation_after=current_notation,
                        action_taken=action_taken,
                        trigger_reason_code=trigger_reason_code,
                        trigger_reason_text=trigger_reason_text,
                        units_changed=units,
                        close_price=fill_price,
                        avg_entry_price_after=float(position["avg_entry_price"]),
                        realized_pnl_delta=0.0,
                        unrealized_pnl_after=_position_unrealized(position, fill_price),
                        cash_after=cash,
                        equity_after=cash + _position_unrealized(position, fill_price),
                        position_id=position["position_id"],
                        feature_state_id=feature_state_id,
                        holding_days_open=current_holding_days,
                        execution_timing=execution_timing,
                        execution_model=execution_timing,
                        execution_price=None,
                        price_provenance=None,
                        order_status="unfilled",
                        unfilled_reason=reason,
                        no_lookahead_check=_make_no_lookahead_check(decision_date, None, status="unknown", notes=[reason]),
                        notes=list(notes or [reason]),
                    )
                )
                return
            total_notional_before = int(position["logical_units"]) * int(position["unit_scale"]) * float(position["avg_entry_price"])
            position["avg_entry_price"] = (total_notional_before + units * int(position["unit_scale"]) * fill_price) / max(1, next_units)
            position["logical_units"] = next_units
            position["add_count"] = int(position["add_count"]) + 1
            cash_delta = units * int(position["unit_scale"]) * fill_price
            cost_amount, slippage_amount, _ = _trade_costs(cash_delta, cost_model)
            if order_side == "long":
                cash -= cash_delta + cost_amount
            else:
                cash += cash_delta - cost_amount
                reserved_short += cash_delta
                position["reserved_short_proceeds"] = float(position["reserved_short_proceeds"]) + cash_delta
            position["cost_pnl_cum"] = float(position.get("cost_pnl_cum") or 0.0) + cost_amount
            position["slippage_pnl_cum"] = float(position.get("slippage_pnl_cum") or 0.0) + slippage_amount
            realized_delta = 0.0
            position_after = position
            daily_actions[symbol] = {
                "action_taken": action_label,
                "side": order_side,
                "trigger_reason_code": trigger_reason_code,
                "trigger_reason_text": trigger_reason_text,
                "units_changed": units,
                "realized_pnl_delta": realized_delta,
                "cost_amount": cost_amount,
                "slippage_amount": slippage_amount,
                "decision_date": _iso(decision_date),
                "fill_date": _iso(fill_date),
                "execution_model": execution_timing,
                "execution_timing": execution_timing,
                "execution_price": fill_price,
                "price_provenance": price_provenance,
                "order_status": order_status,
                "unfilled_reason": None,
                "data_asof": _iso(decision_date),
                "no_lookahead_check": _make_no_lookahead_check(decision_date, fill_date, status="pass", notes=list(notes or [])),
                "notes": list(notes or []),
            }
            last_week_has_trade = True
            weekly_trade_count_map[_week_key(fill_date)] += 1
            ledger.append(
                _build_trade_event(
                    decision_date=decision_date,
                    fill_date=fill_date,
                    date_value=fill_date,
                    symbol=symbol,
                    state_from=current_state,
                    state_to=_position_state(position_after),
                    notation_before=current_notation,
                    notation_after=_position_notation(position_after),
                    action_taken=action_taken,
                    trigger_reason_code=trigger_reason_code,
                    trigger_reason_text=trigger_reason_text,
                    units_changed=units,
                    close_price=fill_price,
                    avg_entry_price_after=float(position_after["avg_entry_price"]),
                    realized_pnl_delta=realized_delta,
                    unrealized_pnl_after=_position_unrealized(position_after, fill_price),
                    cash_after=cash,
                    equity_after=cash + _position_unrealized(position_after, fill_price),
                    position_id=position_after["position_id"],
                    feature_state_id=feature_state_id,
                    holding_days_open=current_holding_days,
                    cost_amount=cost_amount,
                    slippage_amount=slippage_amount,
                    execution_timing=execution_timing,
                    execution_model=execution_timing,
                    execution_price=fill_price,
                    price_provenance=price_provenance,
                    order_status=order_status,
                    unfilled_reason=None,
                    no_lookahead_check=_make_no_lookahead_check(decision_date, fill_date, status="pass", notes=list(notes or [])),
                    notes=list(notes or []),
                )
            )
            return

        elif action_taken in {"partial_take_1", "partial_take_2", "full_exit", "forced_exit", "invalidated"}:
            if position is None:
                return
            trade_notional = units * int(position["unit_scale"]) * fill_price
            cost_amount, slippage_amount, _ = _trade_costs(trade_notional, cost_model)
            if order_side == "long":
                realized_delta = (fill_price - float(position["avg_entry_price"])) * units * int(position["unit_scale"]) - cost_amount
                cash += trade_notional - cost_amount
            else:
                realized_delta = (float(position["avg_entry_price"]) - fill_price) * units * int(position["unit_scale"]) - cost_amount
                cash -= trade_notional + cost_amount
                reserved_short = max(0.0, reserved_short - float(position.get("reserved_short_proceeds") or 0.0))
                position["reserved_short_proceeds"] = 0.0
            position["cost_pnl_cum"] = float(position.get("cost_pnl_cum") or 0.0) + cost_amount
            position["slippage_pnl_cum"] = float(position.get("slippage_pnl_cum") or 0.0) + slippage_amount
            position["realized_pnl_cum"] = float(position["realized_pnl_cum"]) + realized_delta
            realized_by_symbol[symbol] += realized_delta
            if action_taken in {"partial_take_1", "partial_take_2"}:
                position["logical_units"] = max(0, int(position["logical_units"]) - units)
                if position["first_partial_exit_day"] is None:
                    position["first_partial_exit_day"] = current_holding_days
                position["exit_state"] = action_taken
                partial_days.append(current_holding_days)
                position_after = position
            else:
                position["logical_units"] = 0
                position["closed"] = True
                position["exit_state"] = action_taken
                position["exit_reason_code"] = trigger_reason_code
                position["exit_date"] = fill_date
                trade_durations.append(current_holding_days)
                if current_holding_days < 20 and realized_delta > 0:
                    premature_exit_count += 1
                positions.pop(symbol, None)
                position_after = None
            daily_actions[symbol] = {
                "action_taken": action_label,
                "side": order_side,
                "trigger_reason_code": trigger_reason_code,
                "trigger_reason_text": trigger_reason_text,
                "units_changed": units,
                "realized_pnl_delta": realized_delta,
                "cost_amount": cost_amount,
                "slippage_amount": slippage_amount,
                "decision_date": _iso(decision_date),
                "fill_date": _iso(fill_date),
                "execution_model": execution_timing,
                "execution_timing": execution_timing,
                "execution_price": fill_price,
                "price_provenance": price_provenance,
                "order_status": order_status,
                "unfilled_reason": None,
                "data_asof": _iso(decision_date),
                "no_lookahead_check": _make_no_lookahead_check(decision_date, fill_date, status="pass", notes=list(notes or [])),
                "notes": list(notes or []),
            }
            last_week_has_trade = True
            weekly_trade_count_map[_week_key(fill_date)] += 1
            ledger.append(
                _build_trade_event(
                    decision_date=decision_date,
                    fill_date=fill_date,
                    date_value=fill_date,
                    symbol=symbol,
                    state_from=current_state,
                    state_to=state_to,
                    notation_before=current_notation,
                    notation_after=notation_after,
                    action_taken=action_taken,
                    trigger_reason_code=trigger_reason_code,
                    trigger_reason_text=trigger_reason_text,
                    units_changed=units,
                    close_price=fill_price,
                    avg_entry_price_after=avg_entry_after,
                    realized_pnl_delta=realized_delta,
                    unrealized_pnl_after=unrealized_after,
                    cash_after=cash,
                    equity_after=cash + unrealized_after,
                    position_id=position_after["position_id"] if position_after else _stable_hash({"symbol": symbol, "decision_date": _iso(decision_date), "fill_date": _iso(fill_date)})[:16],
                    feature_state_id=feature_state_id,
                    holding_days_open=current_holding_days,
                    cost_amount=cost_amount,
                    slippage_amount=slippage_amount,
                    execution_timing=execution_timing,
                    execution_model=execution_timing,
                    execution_price=fill_price,
                    price_provenance=price_provenance,
                    order_status=order_status,
                    unfilled_reason=None,
                    no_lookahead_check=_make_no_lookahead_check(decision_date, fill_date, status="pass", notes=list(notes or [])),
                    notes=list(notes or []),
                )
            )
            return

        else:
            realized_delta = 0.0
            daily_actions[symbol] = {
                "action_taken": action_label,
                "side": order_side,
                "trigger_reason_code": trigger_reason_code,
                "trigger_reason_text": trigger_reason_text,
                "units_changed": units,
                "realized_pnl_delta": realized_delta,
                "cost_amount": cost_amount,
                "slippage_amount": slippage_amount,
                "decision_date": _iso(decision_date),
                "fill_date": _iso(fill_date),
                "execution_model": execution_timing,
                "execution_timing": execution_timing,
                "execution_price": fill_price,
                "price_provenance": price_provenance,
                "order_status": order_status,
                "unfilled_reason": None,
                "data_asof": _iso(decision_date),
                "no_lookahead_check": _make_no_lookahead_check(decision_date, fill_date, status="pass", notes=list(notes or [])),
                "notes": list(notes or []),
            }
            last_week_has_trade = True
            weekly_trade_count_map[_week_key(fill_date)] += 1
            position_after = positions.get(symbol)
            state_to = _position_state(position_after)
            notation_after = _position_notation(position_after)
            avg_entry_after = float(position_after["avg_entry_price"]) if position_after else fill_price
            unrealized_after = _position_unrealized(position_after, fill_price)
            if action_taken in {"partial_take_1", "partial_take_2"} and position_after is not None:
                realized_delta = 0.0
            ledger.append(
                _build_trade_event(
                    decision_date=decision_date,
                    fill_date=fill_date,
                    date_value=fill_date,
                    symbol=symbol,
                    state_from=current_state,
                    state_to=state_to,
                    notation_before=current_notation,
                    notation_after=notation_after,
                    action_taken=action_taken,
                    trigger_reason_code=trigger_reason_code,
                    trigger_reason_text=trigger_reason_text,
                    units_changed=units,
                    close_price=fill_price,
                    avg_entry_price_after=avg_entry_after,
                    realized_pnl_delta=realized_delta,
                    unrealized_pnl_after=unrealized_after,
                    cash_after=cash,
                    equity_after=cash + unrealized_after,
                    position_id=position_after["position_id"] if position_after else _stable_hash({"symbol": symbol, "decision_date": _iso(decision_date), "fill_date": _iso(fill_date)})[:16],
                    feature_state_id=feature_state_id,
                    holding_days_open=current_holding_days,
                    cost_amount=cost_amount,
                    slippage_amount=slippage_amount,
                    execution_timing=execution_timing,
                    execution_model=execution_timing,
                    execution_price=fill_price,
                    price_provenance=price_provenance,
                    order_status=order_status,
                    unfilled_reason=None,
                    no_lookahead_check=_make_no_lookahead_check(decision_date, fill_date, status="pass", notes=list(notes or [])),
                    notes=list(notes or []),
                )
            )

    for current in dates:
        daily_actions: dict[str, dict[str, Any]] = {}
        due_orders = [item for item in pending_orders if item["fill_date"] == current]
        for order in due_orders:
            pending_orders.remove(order)
            fill_price = _open_price_for(order["symbol"], current)
            if fill_price is None:
                reason = "next_session_open_price_unavailable"
                daily_actions[order["symbol"]] = {
                    "action_taken": order["action_taken"],
                    "side": order["side"],
                    "trigger_reason_code": order["trigger_reason_code"],
                    "trigger_reason_text": order["trigger_reason_text"],
                    "units_changed": order["units"],
                    "realized_pnl_delta": 0.0,
                    "cost_amount": 0.0,
                    "slippage_amount": 0.0,
                    "decision_date": _iso(order["decision_date"]),
                    "fill_date": None,
                    "execution_model": execution_timing,
                    "execution_timing": execution_timing,
                    "execution_price": None,
                    "price_provenance": None,
                    "order_status": "unfilled",
                    "unfilled_reason": reason,
                    "data_asof": _iso(order["decision_date"]),
                    "no_lookahead_check": _make_no_lookahead_check(order["decision_date"], None, status="unknown", notes=[reason]),
                    "notes": [reason],
                }
                ledger.append(
                    _build_trade_event(
                        decision_date=order["decision_date"],
                        fill_date=None,
                        date_value=order["decision_date"],
                        symbol=order["symbol"],
                        state_from=order.get("state_from", "flat"),
                        state_to=order.get("state_from", "flat"),
                        notation_before=order.get("notation_before", "0-0"),
                        notation_after=order.get("notation_before", "0-0"),
                        action_taken=order["action_taken"],
                        trigger_reason_code=order["trigger_reason_code"],
                        trigger_reason_text=order["trigger_reason_text"],
                        units_changed=order["units"],
                        close_price=0.0,
                        avg_entry_price_after=0.0,
                        realized_pnl_delta=0.0,
                        unrealized_pnl_after=0.0,
                        cash_after=cash,
                        equity_after=cash,
                        position_id=order.get("position_id", _stable_hash({"symbol": order["symbol"], "decision_date": _iso(order["decision_date"]), "unfilled": True})[:16]),
                        feature_state_id=order.get("feature_state_id"),
                        holding_days_open=order.get("holding_days_open", 0),
                        execution_timing=execution_timing,
                        execution_model=execution_timing,
                        price_provenance=None,
                        order_status="unfilled",
                        unfilled_reason=reason,
                        no_lookahead_check=_make_no_lookahead_check(order["decision_date"], None, status="unknown", notes=[reason]),
                        notes=[reason],
                    )
                )
                continue
            _record_filled_order(
                symbol=order["symbol"],
                decision_date=order["decision_date"],
                fill_date=current,
                action_taken=order["action_taken"],
                trigger_reason_code=order["trigger_reason_code"],
                trigger_reason_text=order["trigger_reason_text"],
                units=order["units"],
                side=order["side"],
                fill_price=fill_price,
                price_provenance="next_session_open_price",
                order_status="filled",
                feature_state_id=order.get("feature_state_id"),
                notes=list(order.get("notes") or []),
            )

        week_key = _week_key(current)
        if last_week_key is None:
            last_week_key = week_key
        elif week_key != last_week_key:
            weekly_trade_count_map.setdefault(last_week_key, 0)
            if not last_week_has_trade and run_config["weekly_activity_required"] and allow_weekly_activity:
                forced_activity_events_count += 1
                candidate = None
                candidate_score = 0.0
                for symbol in run_config["universe"]:
                    feature = _context_feature(context, current, symbol)
                    if not feature:
                        continue
                    score = _signal_score(feature, selection_weights)
                    if candidate is None or abs(score) > abs(candidate_score) or (abs(score) == abs(candidate_score) and symbol < candidate["symbol"]):
                        candidate = {"symbol": symbol, "feature": feature, "score": score}
                        candidate_score = score
                if candidate and candidate["symbol"] not in positions:
                    side = "long" if candidate["score"] >= 0 else "short"
                    if action_policy_active and side == "short" and not allow_short_entries:
                        daily_actions[candidate["symbol"]] = {
                            "action_taken": "stay_cash",
                            "side": "cash",
                            "trigger_reason_code": "short_blocked",
                            "trigger_reason_text": "short entry blocked by action policy",
                            "reason_codes": ["short_blocked"],
                            "units_changed": 0,
                            "realized_pnl_delta": 0.0,
                            "cost_amount": 0.0,
                            "slippage_amount": 0.0,
                            "decision_date": _iso(current),
                            "fill_date": None,
                            "execution_model": execution_timing,
                            "execution_timing": execution_timing,
                            "execution_price": None,
                            "price_provenance": None,
                            "order_status": "not_applicable",
                            "unfilled_reason": "short_entries_disabled",
                            "data_asof": _iso(current),
                            "no_lookahead_check": _make_no_lookahead_check(current, None, status="pass", notes=["short_entries_disabled"]),
                            "notes": ["short_entries_disabled"],
                        }
                        continue
                    if execution_timing == "next_session_open":
                        fill_date = _next_session_fill_date(current)
                        if fill_date is None:
                            reason = "insufficient_execution_data"
                            daily_actions[candidate["symbol"]] = {
                                "action_taken": "forced_entry",
                                "side": side,
                                "trigger_reason_code": "weekly_activity_forced_trade",
                                "trigger_reason_text": "forced portfolio weekly activity trade",
                                "units_changed": int(run_config["addon_units"][0]),
                                "realized_pnl_delta": 0.0,
                                "cost_amount": 0.0,
                                "slippage_amount": 0.0,
                                "decision_date": _iso(current),
                                "fill_date": None,
                                "execution_model": execution_timing,
                                "execution_timing": execution_timing,
                                "execution_price": None,
                                "price_provenance": None,
                                "order_status": "unfilled",
                                "unfilled_reason": reason,
                                "data_asof": _iso(current),
                                "no_lookahead_check": _make_no_lookahead_check(current, None, status="unknown", notes=[reason]),
                                "notes": [reason],
                            }
                            ledger.append(
                                _build_trade_event(
                                    decision_date=current,
                                    fill_date=None,
                                    date_value=current,
                                    symbol=candidate["symbol"],
                                    state_from="flat",
                                    state_to="flat",
                                    notation_before="0-0",
                                    notation_after="0-0",
                                    action_taken="forced_entry",
                                    trigger_reason_code="weekly_activity_forced_trade",
                                    trigger_reason_text="forced portfolio weekly activity trade",
                                    units_changed=int(run_config["addon_units"][0]),
                                    close_price=_num(candidate["feature"]["close_price"]),
                                    avg_entry_price_after=_num(candidate["feature"]["close_price"]),
                                    realized_pnl_delta=0.0,
                                    unrealized_pnl_after=0.0,
                                    cash_after=cash,
                                    equity_after=cash,
                                    position_id=_stable_hash({"symbol": candidate["symbol"], "decision_date": _iso(current), "unfilled": True})[:16],
                                    feature_state_id=candidate["feature"]["feature_state_id"],
                                    holding_days_open=0,
                                    execution_timing=execution_timing,
                                    execution_model=execution_timing,
                                    execution_price=None,
                                    price_provenance=None,
                                    order_status="unfilled",
                                    unfilled_reason=reason,
                                    no_lookahead_check=_make_no_lookahead_check(current, None, status="unknown", notes=[reason]),
                                    notes=[reason],
                                )
                            )
                        else:
                            pending_orders.append(
                                {
                                    "symbol": candidate["symbol"],
                                    "decision_date": current,
                                    "fill_date": fill_date,
                                    "action_taken": "forced_entry",
                                    "trigger_reason_code": "weekly_activity_forced_trade",
                                    "trigger_reason_text": "forced portfolio weekly activity trade",
                                    "units": int(run_config["addon_units"][0]),
                                    "side": side,
                                    "state_from": "flat",
                                    "notation_before": "0-0",
                                    "position_id": _stable_hash({"symbol": candidate["symbol"], "decision_date": _iso(current), "forced": True})[:16],
                                    "feature_state_id": candidate["feature"]["feature_state_id"],
                                    "holding_days_open": 0,
                                    "notes": [f"scheduled_for_{_iso(fill_date)}"],
                                }
                            )
                            weekly_trade_count_map.setdefault(last_week_key, 0)
                        continue
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
                    cost_amount, slippage_amount, _ = _trade_costs(notional, cost_model)
                    if side == "long" and cash >= notional + cost_amount and notional <= run_config["gross_exposure_cap_jpy"]:
                        cash -= notional + cost_amount
                        pos["cost_pnl_cum"] = float(pos.get("cost_pnl_cum") or 0.0) + cost_amount
                        pos["slippage_pnl_cum"] = float(pos.get("slippage_pnl_cum") or 0.0) + slippage_amount
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
                            cost_amount=cost_amount,
                            slippage_amount=slippage_amount,
                            execution_timing=execution_timing,
                        ))
                        weekly_trade_count_map[last_week_key] += 1
                    elif side == "short" and notional <= run_config["gross_exposure_cap_jpy"]:
                        cash += notional - cost_amount
                        reserved_short += notional
                        pos["reserved_short_proceeds"] = notional
                        pos["cost_pnl_cum"] = float(pos.get("cost_pnl_cum") or 0.0) + cost_amount
                        pos["slippage_pnl_cum"] = float(pos.get("slippage_pnl_cum") or 0.0) + slippage_amount
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
                            cost_amount=cost_amount,
                            slippage_amount=slippage_amount,
                            execution_timing=execution_timing,
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
            score_map[symbol] = _signal_score(feature, selection_weights)
            feature_snapshots.append({"date": _iso(current), "symbol": symbol, **feature})
        selection_snapshots.append({
            "date": _iso(current),
            "candidates": [{"symbol": sym, "score": score_map[sym], "feature_state_id": feature_map[sym]["feature_state_id"]} for sym in sorted(feature_map, key=lambda item: (-abs(score_map[item]), item))],
        })
        for event in ledger:
            if event.get("date") == _iso(current) and event.get("action_taken") == "forced_entry":
                daily_actions.setdefault(
                    _text(event.get("symbol")),
                    {
                        "action_taken": event.get("action_taken"),
                        "side": "short" if str(event.get("position_notation_after", "")).split("-", 1)[0] != "0" else "long",
                        "trigger_reason_code": event.get("trigger_reason_code"),
                        "trigger_reason_text": event.get("trigger_reason_text"),
                        "units_changed": event.get("units_changed"),
                        "realized_pnl_delta": event.get("realized_pnl_delta"),
                        "cost_amount": event.get("cost_amount"),
                        "slippage_amount": event.get("slippage_amount"),
                    },
                )

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
                if current == window_end and not (action_policy_active and not allow_position_management):
                    action, reason_code, reason_text = "forced_exit", "window_end_flatten", "forced window flatten"
                    units = int(position["logical_units"])
                elif action_policy_active and not allow_position_management:
                    action, reason_code, reason_text = "hold", "position_management_disabled", "position management disabled by action policy"
                    units = 0
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
                    if execution_timing == "next_session_open":
                        fill_date = _next_session_fill_date(current)
                        order_side = position["side"] if position else side
                        if fill_date is None:
                            reason = "insufficient_execution_data"
                            daily_actions[symbol] = {
                                "action_taken": action,
                                "side": order_side,
                                "trigger_reason_code": reason_code,
                                "trigger_reason_text": reason_text,
                                "units_changed": units,
                                "realized_pnl_delta": 0.0,
                                "cost_amount": 0.0,
                                "slippage_amount": 0.0,
                                "decision_date": _iso(current),
                                "fill_date": None,
                                "execution_model": execution_timing,
                                "execution_timing": execution_timing,
                                "execution_price": None,
                                "price_provenance": None,
                                "order_status": "unfilled",
                                "unfilled_reason": reason,
                                "data_asof": _iso(current),
                                "no_lookahead_check": _make_no_lookahead_check(current, None, status="unknown", notes=[reason]),
                                "notes": [reason],
                            }
                            ledger.append(
                                _build_trade_event(
                                    decision_date=current,
                                    fill_date=None,
                                    date_value=current,
                                    symbol=symbol,
                                    state_from=before_state,
                                    state_to=before_state,
                                    notation_before=before_notation,
                                    notation_after=before_notation,
                                    action_taken=action,
                                    trigger_reason_code=reason_code,
                                    trigger_reason_text=reason_text,
                                    units_changed=units,
                                    close_price=close_price,
                                    avg_entry_price_after=float(position["avg_entry_price"]) if position else close_price,
                                    realized_pnl_delta=0.0,
                                    unrealized_pnl_after=_position_unrealized(position, close_price),
                                    cash_after=cash,
                                    equity_after=0.0,
                                    position_id=None if position is None else position["position_id"],
                                    feature_state_id=feature["feature_state_id"],
                                    holding_days_open=holding_days if position else 0,
                                    cost_amount=0.0,
                                    slippage_amount=0.0,
                                    execution_timing=execution_timing,
                                    execution_model=execution_timing,
                                    execution_price=None,
                                    price_provenance=None,
                                    order_status="unfilled",
                                    unfilled_reason=reason,
                                    no_lookahead_check=_make_no_lookahead_check(current, None, status="unknown", notes=[reason]),
                                    notes=[reason],
                                )
                            )
                            continue
                        pending_orders.append(
                            {
                                "symbol": symbol,
                                "decision_date": current,
                                "fill_date": fill_date,
                                "action_taken": action,
                                "trigger_reason_code": reason_code,
                                "trigger_reason_text": reason_text,
                                "units": units,
                                "side": order_side,
                                "state_from": before_state,
                                "notation_before": before_notation,
                                "position_id": None if position is None else position["position_id"],
                                "feature_state_id": feature["feature_state_id"],
                                "holding_days_open": holding_days if position else 0,
                                "notes": [f"scheduled_for_{_iso(fill_date)}"],
                            }
                        )
                        continue
                    if action == "add_on":
                        next_units = int(position["logical_units"]) + units
                        next_notional = next_units * int(position["unit_scale"]) * close_price
                        if next_notional <= run_config["gross_exposure_cap_jpy"]:
                            total_notional_before = int(position["logical_units"]) * int(position["unit_scale"]) * float(position["avg_entry_price"])
                            position["avg_entry_price"] = (total_notional_before + units * int(position["unit_scale"]) * close_price) / max(1, next_units)
                            position["logical_units"] = next_units
                            position["add_count"] = int(position["add_count"]) + 1
                            cash_delta = units * int(position["unit_scale"]) * close_price
                            cost_amount, slippage_amount, _ = _trade_costs(cash_delta, cost_model)
                            if position["side"] == "long":
                                cash -= cash_delta + cost_amount
                            else:
                                cash += cash_delta - cost_amount
                                reserved_short += cash_delta
                                position["reserved_short_proceeds"] = float(position["reserved_short_proceeds"]) + cash_delta
                            position["cost_pnl_cum"] = float(position.get("cost_pnl_cum") or 0.0) + cost_amount
                            position["slippage_pnl_cum"] = float(position.get("slippage_pnl_cum") or 0.0) + slippage_amount
                            daily_actions[symbol] = {"action_taken": action, "side": position["side"], "trigger_reason_code": reason_code, "trigger_reason_text": reason_text, "units_changed": units, "realized_pnl_delta": 0.0, "cost_amount": cost_amount, "slippage_amount": slippage_amount}
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
                                cost_amount=cost_amount,
                                slippage_amount=slippage_amount,
                                execution_timing=execution_timing,
                            ))
                    else:
                        trade_notional = units * int(position["unit_scale"]) * close_price
                        cost_amount, slippage_amount, _ = _trade_costs(trade_notional, cost_model)
                        if position["side"] == "long":
                            realized = (close_price - float(position["avg_entry_price"])) * units * int(position["unit_scale"]) - cost_amount
                            cash += trade_notional - cost_amount
                        else:
                            realized = (float(position["avg_entry_price"]) - close_price) * units * int(position["unit_scale"]) - cost_amount
                            cash -= trade_notional + cost_amount
                            reserved_short = max(0.0, reserved_short - float(position.get("reserved_short_proceeds") or 0.0))
                            position["reserved_short_proceeds"] = 0.0
                        position["cost_pnl_cum"] = float(position.get("cost_pnl_cum") or 0.0) + cost_amount
                        position["slippage_pnl_cum"] = float(position.get("slippage_pnl_cum") or 0.0) + slippage_amount
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
                        daily_actions[symbol] = {"action_taken": action, "side": position["side"], "trigger_reason_code": reason_code, "trigger_reason_text": reason_text, "units_changed": units, "realized_pnl_delta": realized, "cost_amount": cost_amount, "slippage_amount": slippage_amount}
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
                            cost_amount=cost_amount,
                            slippage_amount=slippage_amount,
                            execution_timing=execution_timing,
                        ))
            else:
                if action_policy_active:
                    market_context = (context.get("market_context_by_date") or {}).get(_iso(current))
                    long_candidate_rank = None
                    if score_map:
                        long_candidates = sorted(
                            [item for item in score_map.items() if item[1] >= run_config["entry_threshold"]],
                            key=lambda item: (-item[1], item[0]),
                        )
                        rank_lookup = {symbol_key: index + 1 for index, (symbol_key, _) in enumerate(long_candidates)}
                        long_candidate_rank = rank_lookup.get(symbol)
                    gate_allow, gate_reason_code, gate_reason_text, gate_reason_codes = _entry_gate_decision(feature, market_context, action_policy, score=score, candidate_rank=long_candidate_rank)
                    if not allow_long_entries or score < run_config["entry_threshold"]:
                        daily_actions[symbol] = {
                            "action_taken": "stay_cash",
                            "side": "cash",
                            "trigger_reason_code": "long_entries_disabled" if not allow_long_entries else "entry_threshold_not_met",
                            "trigger_reason_text": "long entries disabled by action policy" if not allow_long_entries else "entry threshold not met",
                            "reason_codes": ["long_entries_disabled" if not allow_long_entries else "entry_threshold_not_met"],
                            "units_changed": 0,
                            "realized_pnl_delta": 0.0,
                            "cost_amount": 0.0,
                            "slippage_amount": 0.0,
                            "decision_date": _iso(current),
                            "fill_date": None,
                            "execution_model": execution_timing,
                            "execution_timing": execution_timing,
                            "execution_price": None,
                            "price_provenance": None,
                            "order_status": "not_applicable",
                            "unfilled_reason": None,
                            "data_asof": _iso(current),
                            "no_lookahead_check": _make_no_lookahead_check(current, None, status="pass", notes=["entry_threshold_not_met"]),
                            "notes": ["long_entries_disabled" if not allow_long_entries else "entry_threshold_not_met"],
                        }
                        continue
                    if not gate_allow:
                        daily_actions[symbol] = {
                            "action_taken": "stay_cash",
                            "side": "cash",
                            "trigger_reason_code": gate_reason_code,
                            "trigger_reason_text": gate_reason_text,
                            "reason_codes": list(gate_reason_codes or [gate_reason_code]),
                            "units_changed": 0,
                            "realized_pnl_delta": 0.0,
                            "cost_amount": 0.0,
                            "slippage_amount": 0.0,
                            "decision_date": _iso(current),
                            "fill_date": None,
                            "execution_model": execution_timing,
                            "execution_timing": execution_timing,
                            "execution_price": None,
                            "price_provenance": None,
                            "order_status": "not_applicable",
                            "unfilled_reason": None,
                            "data_asof": _iso(current),
                            "no_lookahead_check": _make_no_lookahead_check(current, None, status="pass", notes=list(gate_reason_codes or [gate_reason_code])),
                            "notes": [gate_reason_text],
                        }
                        continue
                    side = "long"
                else:
                    if abs(score) < run_config["entry_threshold"]:
                        continue
                    side = "long" if score >= 0 else "short"
                    if side == "short" and not allow_short_entries:
                        daily_actions[symbol] = {
                            "action_taken": "stay_cash",
                            "side": "cash",
                            "trigger_reason_code": "short_blocked",
                            "trigger_reason_text": "short entry blocked by policy",
                            "reason_codes": ["short_blocked"],
                            "units_changed": 0,
                            "realized_pnl_delta": 0.0,
                            "cost_amount": 0.0,
                            "slippage_amount": 0.0,
                            "decision_date": _iso(current),
                            "fill_date": None,
                            "execution_model": execution_timing,
                            "execution_timing": execution_timing,
                            "execution_price": None,
                            "price_provenance": None,
                            "order_status": "not_applicable",
                            "unfilled_reason": "short_entries_disabled",
                            "data_asof": _iso(current),
                            "no_lookahead_check": _make_no_lookahead_check(current, None, status="pass", notes=["short_entries_disabled"]),
                            "notes": ["short_entries_disabled"],
                        }
                        continue
                units = int(run_config["addon_units"][0])
                if execution_timing == "next_session_open":
                    fill_date = _next_session_fill_date(current)
                    if fill_date is None:
                        reason = "insufficient_execution_data"
                        daily_actions[symbol] = {
                            "action_taken": _ledger_action("enter_long" if side == "long" else "enter_short", side),
                            "side": side,
                            "trigger_reason_code": "entry_signal",
                            "trigger_reason_text": "long entry signal" if side == "long" else "short entry signal",
                            "reason_codes": ["entry_signal"],
                            "units_changed": units,
                            "realized_pnl_delta": 0.0,
                            "cost_amount": 0.0,
                            "slippage_amount": 0.0,
                            "decision_date": _iso(current),
                            "fill_date": None,
                            "execution_model": execution_timing,
                            "execution_timing": execution_timing,
                            "execution_price": None,
                            "price_provenance": None,
                            "order_status": "unfilled",
                            "unfilled_reason": reason,
                            "data_asof": _iso(current),
                            "no_lookahead_check": _make_no_lookahead_check(current, None, status="unknown", notes=[reason]),
                            "notes": [reason],
                        }
                        ledger.append(
                            _build_trade_event(
                                decision_date=current,
                                fill_date=None,
                                date_value=current,
                                symbol=symbol,
                                state_from="flat",
                                state_to="flat",
                                notation_before="0-0",
                                notation_after="0-0",
                                action_taken="enter_long" if side == "long" else "enter_short",
                                trigger_reason_code="entry_signal",
                                trigger_reason_text="long entry signal" if side == "long" else "short entry signal",
                                units_changed=units,
                                close_price=close_price,
                                avg_entry_price_after=close_price,
                                realized_pnl_delta=0.0,
                                unrealized_pnl_after=0.0,
                                cash_after=cash,
                                equity_after=cash,
                                position_id=_stable_hash({"symbol": symbol, "decision_date": _iso(current), "unfilled": True})[:16],
                                feature_state_id=feature["feature_state_id"],
                                holding_days_open=0,
                                execution_timing=execution_timing,
                                execution_model=execution_timing,
                                execution_price=None,
                                price_provenance=None,
                                order_status="unfilled",
                                unfilled_reason=reason,
                                no_lookahead_check=_make_no_lookahead_check(current, None, status="unknown", notes=[reason]),
                                notes=[reason],
                            )
                        )
                        continue
                    pending_orders.append(
                        {
                            "symbol": symbol,
                            "decision_date": current,
                            "fill_date": fill_date,
                            "action_taken": "enter_long" if side == "long" else "enter_short",
                            "trigger_reason_code": "entry_signal",
                            "trigger_reason_text": "long entry signal" if side == "long" else "short entry signal",
                            "units": units,
                            "side": side,
                            "state_from": "flat",
                            "notation_before": "0-0",
                            "position_id": _stable_hash({"symbol": symbol, "decision_date": _iso(current), "side": side})[:16],
                            "feature_state_id": feature["feature_state_id"],
                            "holding_days_open": 0,
                            "notes": [f"scheduled_for_{_iso(fill_date)}"],
                        }
                    )
                    continue
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
                cost_amount, slippage_amount, _ = _trade_costs(notional, cost_model)
                if notional <= run_config["gross_exposure_cap_jpy"]:
                    if side == "long" and cash >= notional + cost_amount:
                        cash -= notional + cost_amount
                        pos["cost_pnl_cum"] = float(pos.get("cost_pnl_cum") or 0.0) + cost_amount
                        pos["slippage_pnl_cum"] = float(pos.get("slippage_pnl_cum") or 0.0) + slippage_amount
                        positions[symbol] = pos
                        daily_actions[symbol] = {"action_taken": "enter_long", "side": "long", "trigger_reason_code": "entry_signal", "trigger_reason_text": "long entry signal", "reason_codes": ["entry_signal"], "units_changed": units, "realized_pnl_delta": 0.0, "cost_amount": cost_amount, "slippage_amount": slippage_amount}
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
                            cost_amount=cost_amount,
                            slippage_amount=slippage_amount,
                            execution_timing=execution_timing,
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
        portfolio_daily_action_ledger.extend(
            _build_portfolio_daily_action_rows(
                current=current,
                run_config=run_config,
                symbols=list(run_config["universe"]),
                feature_map=feature_map,
                positions=positions,
                daily_actions=daily_actions,
                cash=cash,
                total_gross=total_gross,
                total_net=total_net,
                equity=equity,
                prior_equity=prior_equity,
                drawdown=drawdown,
                realized_by_symbol=realized_by_symbol,
            )
        )
        prior_equity = equity
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
        "portfolio_daily_action_ledger": portfolio_daily_action_ledger,
        "daily_equity_curve": equity_curve,
        "benchmark_market": {"schema_version": REPLAY_SCHEMA_VERSION, "symbol": run_config["market_benchmark_symbol"], "series": benchmark_market},
        "benchmark_universe": {"schema_version": REPLAY_SCHEMA_VERSION, "mode": "equal_weight", "series": benchmark_universe, "equity_curve": universe_curve},
        "relative_performance": {
            "schema_version": REPLAY_SCHEMA_VERSION,
            "portfolio_return_3m": portfolio_return,
            "execution_model": run_config["execution_model"],
            "cost_model": run_config["cost_model"],
            "portfolio_daily_action_ledger_supported": True,
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
