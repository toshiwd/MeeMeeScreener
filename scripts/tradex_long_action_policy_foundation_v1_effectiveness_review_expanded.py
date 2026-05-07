from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
import sys
import uuid
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.infra.duckdb.stock_repo import StockRepository
from external_analysis.policy_replay.simulator import (
    _add_months,
    _bar_date,
    _iso,
    _parse_date,
    _simulate_window,
    _text,
    normalize_replay_run_config,
    prepare_replay_window_context,
)
from scripts.tradex_long_action_policy_foundation_v1 import (
    ACTION_POLICY_MODE_BASELINE,
    ACTION_POLICY_MODE_VARIANT,
    BASELINE_NAME,
    DEFAULT_WEIGHTS,
    ENTRY_GATE_VARIANT,
    EVALUATION_COST_MODEL,
    FAMILY_ID,
    VARIANT_NAME,
    _build_payload,
    _aggregate_scenario,
    _classify_regime,
    _count_actions,
    _resolve_repo,
    _resolve_universe,
)
from shared.runtime_stock_db_contract import inspect_runtime_stock_db, resolve_runtime_stock_db_path

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\research_sessions\long_action_policy_foundation_v1_effectiveness_review_expanded")
SCRIPT_NAME = "tradex_long_action_policy_foundation_v1_effectiveness_review_expanded"
EXPANSION_MODE = "research_fallback_monthly_anchor_expansion_limited"
DEFAULT_BENCHMARK_SYMBOL = "1306"
MAX_FUTURE_TRADING_DAYS = 126
MAX_WINDOW_SPECS = 4
MAX_ANALYSIS_UNIVERSE = 50


def _analysis_universe(repo: Any) -> list[str]:
    try:
        rows = repo.get_latest_params_for_screening()
        ordered = [str(row[0]).strip() for row in rows if row and str(row[0]).strip()]
    except Exception:
        ordered = []
    if not ordered:
        ordered = _resolve_universe(repo)
    ordered = list(dict.fromkeys(ordered))
    if DEFAULT_BENCHMARK_SYMBOL in ordered:
        ordered = [DEFAULT_BENCHMARK_SYMBOL, *[code for code in ordered if code != DEFAULT_BENCHMARK_SYMBOL]]
    else:
        ordered = [DEFAULT_BENCHMARK_SYMBOL, *ordered]
    if len(ordered) > MAX_ANALYSIS_UNIVERSE:
        ordered = [DEFAULT_BENCHMARK_SYMBOL, *[code for code in ordered if code != DEFAULT_BENCHMARK_SYMBOL][: MAX_ANALYSIS_UNIVERSE - 1]]
    return ordered


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _make_session_id() -> str:
    return f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, set):
        return [_json_ready(item) for item in sorted(value, key=str)]
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def _safe_git_output(args: list[str]) -> str:
    try:
        completed = subprocess.run(args, cwd=REPO_ROOT, capture_output=True, text=True, check=False)
        output = (completed.stdout or completed.stderr or "").strip()
        return output
    except Exception as exc:  # pragma: no cover - best-effort metadata
        return f"unavailable: {exc}"


def _safe_float(value: Any, default: float | None = None) -> float | None:
    if value is None:
        return default
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed == parsed else default


def _safe_int(value: Any, default: int | None = None) -> int | None:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


class _CachedRepo:
    def __init__(self, repo: Any) -> None:
        self._repo = repo
        self._all_codes: list[str] | None = None
        self._latest_params_cache: list[tuple[Any, ...]] | None = None
        self._daily_bars_cache: dict[tuple[str, int, Any], list[Any]] = {}

    def get_all_codes(self):  # noqa: ANN001
        if self._all_codes is None:
            self._all_codes = list(self._repo.get_all_codes())
        return list(self._all_codes)

    def get_latest_params_for_screening(self, codes=None):  # noqa: ANN001
        if codes is None:
            if self._latest_params_cache is None:
                self._latest_params_cache = list(self._repo.get_latest_params_for_screening())
            return list(self._latest_params_cache)
        return list(self._repo.get_latest_params_for_screening(codes))

    def get_daily_bars(self, code: str, limit: int = 400, asof_dt=None):  # noqa: ANN001
        key = (str(code), int(limit), asof_dt)
        if key not in self._daily_bars_cache:
            self._daily_bars_cache[key] = list(self._repo.get_daily_bars(str(code), limit=limit, asof_dt=asof_dt))
        return list(self._daily_bars_cache[key])

    def get_daily_bars_batch(self, codes, limit=420, asof_dt=None):  # noqa: ANN001
        return {str(code): self.get_daily_bars(str(code), limit=limit, asof_dt=asof_dt) for code in codes}


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    idx = (len(ordered) - 1) * percentile
    lo = math.floor(idx)
    hi = math.ceil(idx)
    if lo == hi:
        return ordered[int(idx)]
    return ordered[lo] * (hi - idx) + ordered[hi] * (idx - lo)


def _distribution(values: list[float]) -> dict[str, Any]:
    if not values:
        return {
            "count": 0,
            "mean": None,
            "median": None,
            "min": None,
            "max": None,
            "p10": None,
            "p90": None,
            "win_rate": None,
        }
    return {
        "count": len(values),
        "mean": sum(values) / len(values),
        "median": median(values),
        "min": min(values),
        "max": max(values),
        "p10": _percentile(values, 0.10),
        "p90": _percentile(values, 0.90),
        "win_rate": sum(1 for value in values if value > 0.0) / len(values),
    }


def _drawdown_duration(curve: list[dict[str, Any]]) -> dict[str, Any]:
    runs: list[int] = []
    current = 0
    for row in curve:
        drawdown = _safe_float(row.get("drawdown"), 0.0) or 0.0
        if drawdown < 0:
            current += 1
        elif current:
            runs.append(current)
            current = 0
    if current:
        runs.append(current)
    return {
        "count": len(runs),
        "max_duration_days": max(runs) if runs else 0,
        "mean_duration_days": (sum(runs) / len(runs)) if runs else 0.0,
    }


def _month_key(value: str) -> str:
    return value[:7]


def _week_key(value: str) -> str:
    current = _parse_date(value)
    iso = current.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"


def _get_series_dates(repo: Any, symbol: str) -> list[date]:
    rows = repo.get_daily_bars(symbol, limit=8000)
    return [_bar_date(row[0]) for row in rows if row]


def _first_trading_day_by_month(repo: Any, symbol: str = DEFAULT_BENCHMARK_SYMBOL) -> list[date]:
    dates = _get_series_dates(repo, symbol)
    seen: set[tuple[int, int]] = set()
    monthly: list[date] = []
    for current in dates:
        key = (current.year, current.month)
        if key in seen:
            continue
        seen.add(key)
        monthly.append(current)
    return monthly


def _build_base_payload(
    *,
    universe: list[str],
    window_start_date: str,
    window_months: int,
    policy_version: str,
    action_policy: dict[str, Any],
) -> dict[str, Any]:
    return _build_payload(
        universe=universe,
        window_start_date=window_start_date,
        window_months=window_months,
        policy_version=policy_version,
        action_policy=action_policy,
    )


def _make_action_policy(*, mode: str, entry_gate_enabled: bool, notes: list[str]) -> dict[str, Any]:
    return {
        "schema_version": "tradex_daily_action_policy_v1",
        "mode": mode,
        "enabled": True,
        "allow_long_entries": True,
        "allow_short_entries": False,
        "allow_position_management": False,
        "allow_weekly_activity": False,
        "entry_gate": {"enabled": entry_gate_enabled},
        "notes": notes,
    }


def _classify_window_label(regime: str) -> str:
    if regime == "uptrend":
        return "up"
    if regime == "downtrend":
        return "down"
    return "flat"


def _select_window_months(regime: str) -> int:
    return 3 if regime == "uptrend" else 1


def _fallback_market_context(current: date, dates: list[date], close_by_date: dict[str, float]) -> dict[str, Any]:
    index_by_date = {item: idx for idx, item in enumerate(dates)}
    idx = index_by_date.get(current)
    current_close = close_by_date.get(_iso(current))
    market_ret20 = None
    if idx is not None:
        lookback = idx - 20 if idx >= 20 else idx - 5
        if lookback >= 0:
            prev_date = dates[lookback]
            prev_close = close_by_date.get(_iso(prev_date))
            if prev_close not in {None, 0} and current_close not in {None, 0}:
                market_ret20 = current_close / float(prev_close) - 1.0
    if market_ret20 is None:
        market_ret20 = 0.0
    if market_ret20 > 0.01:
        breadth = 0.70
    elif market_ret20 < -0.01:
        breadth = 0.30
    else:
        breadth = 0.50
    return {
        "market_ret20": market_ret20,
        "breadth_above_ma20": breadth,
        "regime_block": market_ret20 < -0.01 and breadth <= 0.50,
        "source": "benchmark_return_proxy",
    }


def _make_window_specs(repo: Any, universe: list[str]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    dates = _get_series_dates(repo, DEFAULT_BENCHMARK_SYMBOL)
    if not dates:
        raise ValueError("benchmark series is unavailable")
    index_by_date = {current: idx for idx, current in enumerate(dates)}
    benchmark_close_by_date = {
        _iso(_bar_date(row[0])): _safe_float(row[4])
        for row in repo.get_daily_bars(DEFAULT_BENCHMARK_SYMBOL, limit=8000)
        if row and _safe_float(row[4]) is not None
    }
    fallback_payload = _build_base_payload(
        universe=universe,
        window_start_date=_iso(dates[0]),
        window_months=1,
        policy_version=BASELINE_NAME,
        action_policy=_make_action_policy(
            mode=ACTION_POLICY_MODE_BASELINE,
            entry_gate_enabled=False,
            notes=["classification-only baseline"],
        ),
    )
    fallback_run_config = normalize_replay_run_config(fallback_payload)
    window_specs: list[dict[str, Any]] = []
    excluded: list[dict[str, Any]] = []
    for current in _first_trading_day_by_month(repo, DEFAULT_BENCHMARK_SYMBOL):
        idx = index_by_date.get(current)
        if idx is None:
            continue
        if idx + MAX_FUTURE_TRADING_DAYS >= len(dates):
            excluded.append(
                {
                    "window_start_date": _iso(current),
                    "reason": "insufficient_outcome_data",
                }
            )
            continue
        run_config = dict(fallback_run_config)
        run_config["window_start_date"] = _iso(current)
        run_config["window_start_dates"] = [_iso(current)]
        run_config["decision_window_start_date"] = _iso(current)
        run_config["decision_window_end_date"] = _iso(current)
        run_config["execution_buffer_days"] = 1
        run_config["outcome_buffer_days"] = 63
        try:
            context = prepare_replay_window_context(repo, run_config, current)
            market_context = (context.get("market_context_by_date") or {}).get(_iso(current))
        except ValueError:
            market_context = None
        if not market_context or _classify_regime(market_context) == "unknown":
            market_context = _fallback_market_context(current, dates, benchmark_close_by_date)
        regime = _classify_regime(market_context)
        if regime == "unknown":
            excluded.append(
                {
                    "window_start_date": _iso(current),
                    "reason": "unknown_regime",
                }
            )
            continue
        label = _classify_window_label(regime)
        window_months = _select_window_months(regime)
        window_specs.append(
            {
                "window_id": f"{label}_{current:%Y%m%d}",
                "label": label,
                "regime": regime,
                "window_start_date": _iso(current),
                "window_end_date": _iso(_add_months(current, window_months) - timedelta(days=1)),
                "window_months": window_months,
                "decision_window_end_date": _iso(current),
                "execution_buffer_days": 1,
                "outcome_buffer_days": 63 if window_months == 3 else 21,
            }
        )
    total_eligible_count = len(window_specs)
    if len(window_specs) > MAX_WINDOW_SPECS:
        selected_indices = sorted(
            {
                round(index * (len(window_specs) - 1) / (MAX_WINDOW_SPECS - 1))
                for index in range(MAX_WINDOW_SPECS)
            }
        )
        window_specs = [window_specs[index] for index in selected_indices]
        excluded.append(
            {
                "reason": "research_fallback_window_cap",
                "total_eligible_count": total_eligible_count,
                "kept_count": len(window_specs),
                "cap": MAX_WINDOW_SPECS,
            }
        )
    return window_specs, {
        "first_trading_date": _iso(dates[0]),
        "last_trading_date": _iso(dates[-1]),
        "candidate_month_count": len(_first_trading_day_by_month(repo, DEFAULT_BENCHMARK_SYMBOL)),
        "eligible_window_count": total_eligible_count,
        "selected_window_count": len(window_specs),
        "analysis_universe_size": len(universe),
        "source_universe_size": len(source_universe) if "source_universe" in locals() else len(universe),
        "excluded_candidates": excluded,
    }


def _window_run_config(template: dict[str, Any], window_spec: dict[str, Any]) -> dict[str, Any]:
    run_config = deepcopy(template)
    run_config["window_start_date"] = window_spec["window_start_date"]
    run_config["window_months"] = int(window_spec["window_months"])
    run_config["window_start_dates"] = [window_spec["window_start_date"]]
    run_config["decision_window_start_date"] = window_spec["window_start_date"]
    run_config["decision_window_end_date"] = window_spec.get("decision_window_end_date") or window_spec["window_start_date"]
    run_config["execution_buffer_days"] = int(window_spec.get("execution_buffer_days") or 1)
    run_config["outcome_buffer_days"] = int(window_spec.get("outcome_buffer_days") or max(20, int(window_spec["window_months"]) * 21))
    return run_config


def _simulate_window_pair(
    *,
    repo: Any,
    baseline_template: dict[str, Any],
    variant_template: dict[str, Any],
    window_spec: dict[str, Any],
) -> dict[str, Any]:
    start_date = _parse_date(window_spec["window_start_date"])
    baseline_run_config = _window_run_config(baseline_template, window_spec)
    variant_run_config = _window_run_config(variant_template, window_spec)
    try:
        baseline_context = prepare_replay_window_context(repo, baseline_run_config, start_date)
        variant_context = prepare_replay_window_context(repo, variant_run_config, start_date)
    except ValueError as exc:
        return {
            "window_spec": window_spec,
            "excluded": True,
            "excluded_reason": str(exc),
            "baseline": None,
            "variant": None,
        }
    baseline_result = _simulate_window(repo, baseline_run_config, start_date, context=baseline_context)
    variant_result = _simulate_window(repo, variant_run_config, start_date, context=variant_context)
    for scenario, result in (("baseline", baseline_result), (VARIANT_NAME, variant_result)):
        result["scenario"] = scenario
        result["window_id"] = window_spec["window_id"]
        result["window_label"] = window_spec["label"]
        result["window_start_date"] = window_spec["window_start_date"]
        result["window_end_date"] = window_spec["window_end_date"]
    return {
        "window_spec": window_spec,
        "baseline": baseline_result,
        "variant": variant_result,
    }


def _window_summary(result: dict[str, Any]) -> dict[str, Any]:
    summary = dict(result.get("window_summary") or {})
    curve = list(result.get("daily_equity_curve") or [])
    equity_values = [_safe_float(row.get("equity"), 0.0) or 0.0 for row in curve]
    daily_pnl = [equity_values[idx] - equity_values[idx - 1] for idx in range(1, len(equity_values))] if len(equity_values) >= 2 else []
    drawdown = _drawdown_duration(curve)
    return {
        "window_id": result.get("window_id"),
        "window_label": result.get("window_label"),
        "window_start_date": result.get("window_start_date"),
        "window_end_date": result.get("window_end_date"),
        "portfolio_return_3m": summary.get("portfolio_return_3m"),
        "max_drawdown": summary.get("max_drawdown"),
        "turnover": summary.get("turnover"),
        "avg_holding_days": summary.get("avg_holding_days"),
        "filled_buy_count": sum(
            1
            for row in result.get("portfolio_daily_action_ledger") or []
            if row.get("action") == "buy" and row.get("order_status") == "filled"
        ),
        "unfilled_buy_count": sum(
            1
            for row in result.get("portfolio_daily_action_ledger") or []
            if row.get("action") == "buy" and row.get("order_status") == "unfilled"
        ),
        "action_counts": _count_actions(result.get("portfolio_daily_action_ledger") or []),
        "daily_pnl_distribution": _distribution(daily_pnl),
        "drawdown_duration": drawdown,
        "gross_exposure_mean": mean([_safe_float(row.get("gross_exposure"), 0.0) or 0.0 for row in curve]) if curve else 0.0,
        "net_exposure_mean": mean([_safe_float(row.get("net_exposure"), 0.0) or 0.0 for row in curve]) if curve else 0.0,
    }


def _build_price_lookup(repo: Any, symbols: list[str]) -> dict[str, dict[str, Any]]:
    batch = repo.get_daily_bars_batch(symbols, limit=8000)
    price_lookup: dict[str, dict[str, Any]] = {}
    for symbol, rows in batch.items():
        ordered: list[tuple[str, float]] = []
        for row in rows:
            if not row:
                continue
            current = _bar_date(row[0])
            close = _safe_float(row[4])
            if close is None:
                continue
            ordered.append((current.isoformat(), close))
        index_by_date = {item[0]: idx for idx, item in enumerate(ordered)}
        price_lookup[str(symbol)] = {"ordered": ordered, "index_by_date": index_by_date}
    return price_lookup


def _forward_return(
    price_lookup: dict[str, dict[str, Any]],
    *,
    symbol: str,
    anchor_date: str,
    horizon: int,
    entry_price: float | None = None,
) -> float | None:
    series = price_lookup.get(str(symbol))
    if not series:
        return None
    ordered = series["ordered"]
    index_by_date = series["index_by_date"]
    idx = index_by_date.get(anchor_date)
    if idx is None:
        return None
    target_idx = idx + horizon
    if target_idx >= len(ordered):
        return None
    base = entry_price if entry_price not in (None, 0) else ordered[idx][1]
    if base in (None, 0):
        return None
    return float(ordered[target_idx][1] / float(base) - 1.0)


def _build_skip_case_rows(
    *,
    window_payload: dict[str, Any],
    price_lookup: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    baseline_rows = window_payload["baseline"].get("portfolio_daily_action_ledger") or []
    variant_rows = window_payload["variant"].get("portfolio_daily_action_ledger") or []
    baseline_by_key = {(row["date"], row["symbol"]): row for row in baseline_rows}
    variant_by_key = {(row["date"], row["symbol"]): row for row in variant_rows}
    baseline_selection = {
        item["date"]: list(item.get("candidates") or [])
        for item in window_payload["baseline"].get("daily_selection_snapshot") or []
    }
    skip_rows: list[dict[str, Any]] = []
    for key in sorted(set(baseline_by_key).intersection(variant_by_key)):
        baseline = baseline_by_key[key]
        variant = variant_by_key[key]
        if baseline.get("action") != "buy" or variant.get("action") != "stay_cash":
            continue
        decision_date = str(baseline.get("decision_date") or baseline.get("date") or "")
        symbol = str(baseline.get("symbol") or "")
        candidates = baseline_selection.get(decision_date, [])
        candidate_rank = next((index + 1 for index, item in enumerate(candidates) if str(item.get("symbol")) == symbol), None)
        candidate_score = next((float(item.get("score")) for item in candidates if str(item.get("symbol")) == symbol and item.get("score") is not None), None)
        top_score = float(candidates[0].get("score")) if candidates and candidates[0].get("score") is not None else None
        ret_5 = _forward_return(price_lookup, symbol=symbol, anchor_date=decision_date, horizon=5)
        ret_10 = _forward_return(price_lookup, symbol=symbol, anchor_date=decision_date, horizon=10)
        ret_20 = _forward_return(price_lookup, symbol=symbol, anchor_date=decision_date, horizon=20)
        skip_class = "skipped_neutral_buy"
        if ret_20 is not None:
            if ret_20 > 0:
                skip_class = "skipped_good_buy"
            elif ret_20 < 0:
                skip_class = "skipped_bad_buy"
        later_buy_date = None
        later_buy_delay_days = None
        later_buy_forward_ret_20d = None
        later_buy_delay_cost_20d = None
        later_buy_action = None
        for later in sorted(variant_rows, key=lambda row: (row["date"], row["symbol"])):
            if later.get("symbol") != symbol:
                continue
            if str(later.get("date")) <= decision_date:
                continue
            if later.get("action") != "buy":
                continue
            later_buy_date = str(later.get("date"))
            later_buy_delay_days = (date.fromisoformat(later_buy_date) - date.fromisoformat(decision_date)).days
            later_buy_forward_ret_20d = _forward_return(price_lookup, symbol=symbol, anchor_date=later_buy_date, horizon=20, entry_price=_safe_float(later.get("execution_price")))
            if ret_20 is not None and later_buy_forward_ret_20d is not None:
                later_buy_delay_cost_20d = later_buy_forward_ret_20d - ret_20
            later_buy_action = later.get("action")
            break
        skip_rows.append(
            {
                "window_id": baseline.get("window_id"),
                "window_label": baseline.get("window_label"),
                "window_start_date": baseline.get("window_start_date"),
                "window_end_date": baseline.get("window_end_date"),
                "date": baseline.get("date"),
                "decision_date": decision_date,
                "symbol": symbol,
                "baseline_action": baseline.get("action"),
                "variant_action": variant.get("action"),
                "baseline_order_status": baseline.get("order_status"),
                "variant_order_status": variant.get("order_status"),
                "baseline_reason_codes": baseline.get("reason_codes"),
                "variant_reason_codes": variant.get("reason_codes"),
                "baseline_execution_price": baseline.get("execution_price"),
                "variant_execution_price": variant.get("execution_price"),
                "baseline_cash": baseline.get("cash"),
                "variant_cash": variant.get("cash"),
                "baseline_position_value": baseline.get("position_value"),
                "variant_position_value": variant.get("position_value"),
                "baseline_position_qty": baseline.get("position_qty"),
                "variant_position_qty": variant.get("position_qty"),
                "baseline_score": candidate_score,
                "baseline_rank": candidate_rank,
                "top_candidate_score": top_score,
                "market_regime": baseline.get("window_label"),
                "month_key": _month_key(decision_date),
                "week_key": _week_key(decision_date),
                "ret_5": ret_5,
                "ret_10": ret_10,
                "ret_20": ret_20,
                "forward_ret_20d": ret_20,
                "path_value_score_v1": None,
                "skip_class": skip_class,
                "reason_codes_key": "|".join(map(str, baseline.get("reason_codes") or [])),
                "later_buy_date": later_buy_date,
                "later_buy_delay_days": later_buy_delay_days,
                "later_buy_forward_ret_20d": later_buy_forward_ret_20d,
                "later_buy_delay_cost_20d": later_buy_delay_cost_20d,
                "later_buy_action": later_buy_action,
                "later_buy_within_window": later_buy_date is not None,
                "baseline_filled": baseline.get("order_status") == "filled",
                "variant_filled": variant.get("order_status") == "filled",
            }
        )
    return skip_rows


def _group_case_summary(rows: list[dict[str, Any]], *, positive_class: str, negative_class: str) -> tuple[dict[str, Any], dict[str, Any]]:
    positives = [row for row in rows if row.get("skip_class") == positive_class]
    negatives = [row for row in rows if row.get("skip_class") == negative_class]

    def _mean_or_none(values: list[float]) -> float | None:
        return mean(values) if values else None

    def _case_stats(items: list[dict[str, Any]]) -> dict[str, Any]:
        ret20 = [float(row["ret_20"]) for row in items if row.get("ret_20") is not None]
        return {
            "count": len(items),
            "ret_20": _distribution(ret20),
            "later_buy_rate": (sum(1 for row in items if row.get("later_buy_within_window")) / len(items)) if items else None,
            "mean_candidate_score": _mean_or_none([float(row["baseline_score"]) for row in items if row.get("baseline_score") is not None]),
            "mean_candidate_rank": _mean_or_none([float(row["baseline_rank"]) for row in items if row.get("baseline_rank") is not None]),
            "mean_delay_days": _mean_or_none([float(row["later_buy_delay_days"]) for row in items if row.get("later_buy_delay_days") is not None]),
            "mean_delay_cost_20d": _mean_or_none([float(row["later_buy_delay_cost_20d"]) for row in items if row.get("later_buy_delay_cost_20d") is not None]),
            "top_groups": [],
        }

    def _top_groups(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        bucket: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
        for row in items:
            key = (
                str(row.get("window_label") or "unknown"),
                str(row.get("reason_codes_key") or ""),
                str(row.get("month_key") or ""),
            )
            bucket[key].append(row)
        grouped = []
        for (regime, reason_key, month_key), rows_for_key in bucket.items():
            ret20 = [float(row["ret_20"]) for row in rows_for_key if row.get("ret_20") is not None]
            grouped.append(
                {
                    "regime": regime,
                    "reason_codes_key": reason_key,
                    "month_key": month_key,
                    "count": len(rows_for_key),
                    "ret_20": _distribution(ret20),
                    "mean_candidate_score": mean([float(row["baseline_score"]) for row in rows_for_key if row.get("baseline_score") is not None]) if any(row.get("baseline_score") is not None for row in rows_for_key) else None,
                    "later_buy_rate": (sum(1 for row in rows_for_key if row.get("later_buy_within_window")) / len(rows_for_key)) if rows_for_key else None,
                    "examples": [
                        {
                            "window_id": row.get("window_id"),
                            "date": row.get("date"),
                            "symbol": row.get("symbol"),
                            "ret_20": row.get("ret_20"),
                            "baseline_rank": row.get("baseline_rank"),
                            "baseline_score": row.get("baseline_score"),
                            "later_buy_delay_cost_20d": row.get("later_buy_delay_cost_20d"),
                        }
                        for row in rows_for_key[:5]
                    ],
                }
            )
        grouped.sort(key=lambda item: (-item["count"], -(item["ret_20"]["mean"] or 0.0), item["regime"], item["reason_codes_key"]))
        return grouped[:10]

    positive_summary = _case_stats(positives)
    negative_summary = _case_stats(negatives)
    positive_summary["top_groups"] = _top_groups(positives)
    negative_summary["top_groups"] = _top_groups(negatives)
    return positive_summary, negative_summary


def build_expanded_effectiveness_review(
    repo: Any,
    output_root: Path,
    *,
    window_specs: list[dict[str, Any]] | None = None,
    jobs: int = 2,
) -> dict[str, Any]:
    repo = _CachedRepo(repo)
    source_universe = _resolve_universe(repo)
    if not source_universe:
        raise ValueError("missing candidate universe from stock repository")
    universe = _analysis_universe(repo)

    if window_specs is None:
        window_specs, input_resolution = _make_window_specs(repo, universe)
    else:
        input_resolution = {
            "first_trading_date": None,
            "last_trading_date": None,
            "candidate_month_count": len(window_specs),
            "excluded_candidates": [],
            "analysis_universe_size": len(universe),
            "source_universe_size": len(source_universe),
        }

    if not window_specs:
        raise ValueError("no eligible windows available for expanded review")

    baseline_template = normalize_replay_run_config(
        _build_base_payload(
            universe=universe,
            window_start_date=window_specs[0]["window_start_date"],
            window_months=int(window_specs[0]["window_months"]),
            policy_version=BASELINE_NAME,
            action_policy=_make_action_policy(
                mode=ACTION_POLICY_MODE_BASELINE,
                entry_gate_enabled=False,
                notes=["baseline long-only entry/cash control without the gate"],
            ),
        )
    )
    variant_template = normalize_replay_run_config(
        _build_base_payload(
            universe=universe,
            window_start_date=window_specs[0]["window_start_date"],
            window_months=int(window_specs[0]["window_months"]),
            policy_version=VARIANT_NAME,
            action_policy=_make_action_policy(
                mode=ACTION_POLICY_MODE_VARIANT,
                entry_gate_enabled=True,
                notes=["gate long entries using regime mismatch, liquidity continuation, and stretched-entry diagnostics"],
            ),
        )
    )

    if jobs and jobs > 1:
        with ThreadPoolExecutor(max_workers=jobs) as executor:
            window_payloads = list(executor.map(lambda spec: _simulate_window_pair(repo=repo, baseline_template=baseline_template, variant_template=variant_template, window_spec=spec), window_specs))
    else:
        window_payloads = [
            _simulate_window_pair(repo=repo, baseline_template=baseline_template, variant_template=variant_template, window_spec=spec)
            for spec in window_specs
        ]

    excluded_window_payloads = [payload for payload in window_payloads if payload.get("excluded")]
    window_payloads = [payload for payload in window_payloads if not payload.get("excluded")]
    if not window_payloads:
        raise ValueError(
            "no replay windows could be simulated",
        )

    baseline_results = [payload["baseline"] for payload in window_payloads]
    variant_results = [payload["variant"] for payload in window_payloads]
    baseline_window_summaries = [_window_summary(result) for result in baseline_results]
    variant_window_summaries = [_window_summary(result) for result in variant_results]
    baseline_aggregate = _aggregate_scenario(
        [
            {
                "window_summary": result.get("window_summary"),
                "relative_performance": result.get("relative_performance"),
                "daily_equity_curve": result.get("daily_equity_curve"),
                "trade_ledger": result.get("trade_ledger"),
            }
            for result in baseline_results
        ]
    )
    variant_aggregate = _aggregate_scenario(
        [
            {
                "window_summary": result.get("window_summary"),
                "relative_performance": result.get("relative_performance"),
                "daily_equity_curve": result.get("daily_equity_curve"),
                "trade_ledger": result.get("trade_ledger"),
            }
            for result in variant_results
        ]
    )

    daily_pnl_baseline: list[float] = []
    daily_pnl_variant: list[float] = []
    drawdown_duration_baseline: list[int] = []
    drawdown_duration_variant: list[int] = []
    gross_exposure_baseline: list[float] = []
    gross_exposure_variant: list[float] = []
    net_exposure_baseline: list[float] = []
    net_exposure_variant: list[float] = []
    window_months_seen: set[str] = set()
    month_coverage: set[str] = set()
    symbol_coverage: set[str] = set()
    branch_transition_counts: Counter[tuple[str | None, str | None]] = Counter()
    state_diff_counts: Counter[str] = Counter()
    baseline_row_count = 0
    variant_row_count = 0
    baseline_filled_buy_count = 0
    variant_filled_buy_count = 0
    baseline_skipped_buy_count = 0
    variant_skipped_buy_count = 0
    skipped_rows: list[dict[str, Any]] = []
    window_coverage_rows: list[dict[str, Any]] = []
    window_regime_rows: list[dict[str, Any]] = []
    skipped_reason_counts: Counter[str] = Counter()

    for payload in window_payloads:
        window_spec = payload["window_spec"]
        window_months_seen.add(str(window_spec["window_months"]))
        month_coverage.add(_month_key(window_spec["window_start_date"]))
        baseline = payload["baseline"]
        variant = payload["variant"]
        base_rows = list(baseline.get("portfolio_daily_action_ledger") or [])
        var_rows = list(variant.get("portfolio_daily_action_ledger") or [])
        base_map = {(row["date"], row["symbol"]): row for row in base_rows}
        var_map = {(row["date"], row["symbol"]): row for row in var_rows}
        common_keys = sorted(set(base_map).intersection(var_map))
        for key in common_keys:
            b = base_map[key]
            v = var_map[key]
            branch_transition_counts[(b.get("action"), v.get("action"))] += 1
            for field in ["cash", "gross_exposure", "net_exposure", "position_qty", "position_value", "realized_pnl", "unrealized_pnl", "daily_pnl", "cumulative_pnl", "drawdown", "cost_amount", "slippage_amount", "execution_price"]:
                if b.get(field) != v.get(field):
                    state_diff_counts[field] += 1
            symbol_coverage.add(str(b.get("symbol")))
            symbol_coverage.add(str(v.get("symbol")))
        baseline_row_count += len(base_rows)
        variant_row_count += len(var_rows)
        baseline_filled_buy_count += sum(1 for row in base_rows if row.get("action") == "buy" and row.get("order_status") == "filled")
        variant_filled_buy_count += sum(1 for row in var_rows if row.get("action") == "buy" and row.get("order_status") == "filled")
        baseline_skipped_buy_count += sum(1 for row in base_rows if row.get("action") == "buy" and row.get("order_status") == "unfilled")
        variant_skipped_buy_count += sum(1 for row in var_rows if row.get("action") == "buy" and row.get("order_status") == "unfilled")
        if len(baseline.get("daily_equity_curve") or []) >= 2:
            baseline_curve = baseline["daily_equity_curve"]
            variant_curve = variant["daily_equity_curve"]
            for curve, pnl_bucket, dd_bucket, gross_bucket, net_bucket in [
                (baseline_curve, daily_pnl_baseline, drawdown_duration_baseline, gross_exposure_baseline, net_exposure_baseline),
                (variant_curve, daily_pnl_variant, drawdown_duration_variant, gross_exposure_variant, net_exposure_variant),
            ]:
                equities = [_safe_float(row.get("equity"), 0.0) or 0.0 for row in curve]
                if len(equities) >= 2:
                    pnl_bucket.extend(equities[idx] - equities[idx - 1] for idx in range(1, len(equities)))
                dd_stats = _drawdown_duration(curve)
                dd_bucket.append(int(dd_stats["max_duration_days"]))
                gross_bucket.extend([_safe_float(row.get("gross_exposure"), 0.0) or 0.0 for row in curve])
                net_bucket.extend([_safe_float(row.get("net_exposure"), 0.0) or 0.0 for row in curve])
        window_coverage_rows.append(
            {
                "window_id": window_spec["window_id"],
                "window_label": window_spec["label"],
                "window_start_date": window_spec["window_start_date"],
                "window_end_date": window_spec["window_end_date"],
                "baseline_rows": len(base_rows),
                "variant_rows": len(var_rows),
                "baseline_filled_buy_count": sum(1 for row in base_rows if row.get("action") == "buy" and row.get("order_status") == "filled"),
                "variant_filled_buy_count": sum(1 for row in var_rows if row.get("action") == "buy" and row.get("order_status") == "filled"),
                "branch_transition_counts": {f"{a}->{b}": c for (a, b), c in Counter((base_map[k].get("action"), var_map[k].get("action")) for k in common_keys).items()},
                "baseline_action_counts": _count_actions(base_rows),
                "variant_action_counts": _count_actions(var_rows),
            }
        )
        window_regime_rows.append(
            {
                "window_id": window_spec["window_id"],
                "window_label": window_spec["label"],
                "regime": window_spec["regime"],
                "window_months": window_spec["window_months"],
                "baseline_aggregate": _window_summary(baseline),
                "variant_aggregate": _window_summary(variant),
                "delta": {
                    "portfolio_return_3m": _safe_float(variant.get("window_summary", {}).get("portfolio_return_3m"), 0.0) - _safe_float(baseline.get("window_summary", {}).get("portfolio_return_3m"), 0.0),
                    "max_drawdown": _safe_float(variant.get("window_summary", {}).get("max_drawdown"), 0.0) - _safe_float(baseline.get("window_summary", {}).get("max_drawdown"), 0.0),
                    "turnover": _safe_float(variant.get("window_summary", {}).get("turnover"), 0.0) - _safe_float(baseline.get("window_summary", {}).get("turnover"), 0.0),
                },
            }
        )
    all_symbols = sorted(set(universe))
    price_lookup = _build_price_lookup(repo, all_symbols)
    skipped_rows = []
    for payload in window_payloads:
        skipped_rows.extend(_build_skip_case_rows(window_payload=payload, price_lookup=price_lookup))
    for row in skipped_rows:
        skipped_reason_counts[str(row.get("reason_codes_key") or "")] += 1

    baseline_aggregate = _aggregate_scenario(
        [
            {
                "window_summary": result.get("window_summary"),
                "relative_performance": result.get("relative_performance"),
                "daily_equity_curve": result.get("daily_equity_curve"),
                "trade_ledger": result.get("trade_ledger"),
            }
            for result in baseline_results
        ]
    )
    variant_aggregate = _aggregate_scenario(
        [
            {
                "window_summary": result.get("window_summary"),
                "relative_performance": result.get("relative_performance"),
                "daily_equity_curve": result.get("daily_equity_curve"),
                "trade_ledger": result.get("trade_ledger"),
            }
            for result in variant_results
        ]
    )

    # Roll up skipped-buy decomposition.
    def _bucket(rows: list[dict[str, Any]]) -> dict[str, Any]:
        return {
            "count": len(rows),
            "ret_5": _distribution([float(row["ret_5"]) for row in rows if row.get("ret_5") is not None]),
            "ret_10": _distribution([float(row["ret_10"]) for row in rows if row.get("ret_10") is not None]),
            "ret_20": _distribution([float(row["ret_20"]) for row in rows if row.get("ret_20") is not None]),
            "path_value_score_v1_mean": mean([float(row["path_value_score_v1"]) for row in rows if row.get("path_value_score_v1") is not None]) if rows and any(row.get("path_value_score_v1") is not None for row in rows) else None,
            "by_regime": {
                regime: {
                    "count": len(group),
                    "ret_20": _distribution([float(row["ret_20"]) for row in group if row.get("ret_20") is not None]),
                }
                for regime, group in _group_by(rows, lambda row: str(row.get("window_label") or "unknown")).items()
            },
            "by_month": {
                month: {
                    "count": len(group),
                    "ret_20": _distribution([float(row["ret_20"]) for row in group if row.get("ret_20") is not None]),
                }
                for month, group in _group_by(rows, lambda row: str(row.get("month_key") or "unknown")).items()
            },
            "by_reason_codes": {
                reason: {
                    "count": len(group),
                    "ret_20": _distribution([float(row["ret_20"]) for row in group if row.get("ret_20") is not None]),
                }
                for reason, group in _group_by(rows, lambda row: str(row.get("reason_codes_key") or "")).items()
            },
        }

    def _group_by(rows: list[dict[str, Any]], key_fn):
        buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            buckets[key_fn(row)].append(row)
        return buckets

    good_rows = [row for row in skipped_rows if row.get("skip_class") == "skipped_good_buy"]
    bad_rows = [row for row in skipped_rows if row.get("skip_class") == "skipped_bad_buy"]
    neutral_rows = [row for row in skipped_rows if row.get("skip_class") == "skipped_neutral_buy"]

    skipped_buy_bucket_decomposition = {
        "schema_version": "tradex_long_action_policy_foundation_v1_effectiveness_review_expanded",
        "bucket_row_count": len(skipped_rows),
        "skip_class_counts": {
            "skipped_good_buy": len(good_rows),
            "skipped_bad_buy": len(bad_rows),
            "skipped_neutral_buy": len(neutral_rows),
        },
        "overall": _bucket(skipped_rows),
        "good_bucket": _bucket(good_rows),
        "bad_bucket": _bucket(bad_rows),
        "neutral_bucket": _bucket(neutral_rows),
    }

    false_negative_summary, true_positive_summary = _group_case_summary(
        skipped_rows,
        positive_class="skipped_good_buy",
        negative_class="skipped_bad_buy",
    )

    # Build symbol and monthly summaries.
    symbol_summary = []
    for symbol, rows in sorted(_group_by(skipped_rows, lambda row: str(row.get("symbol") or "")).items()):
        ret20 = [float(row["ret_20"]) for row in rows if row.get("ret_20") is not None]
        symbol_summary.append(
            {
                "symbol": symbol,
                "count": len(rows),
                "skipped_good_buy_count": sum(1 for row in rows if row.get("skip_class") == "skipped_good_buy"),
                "skipped_bad_buy_count": sum(1 for row in rows if row.get("skip_class") == "skipped_bad_buy"),
                "ret_20": _distribution(ret20),
                "mean_baseline_rank": mean([float(row["baseline_rank"]) for row in rows if row.get("baseline_rank") is not None]) if any(row.get("baseline_rank") is not None for row in rows) else None,
                "mean_baseline_score": mean([float(row["baseline_score"]) for row in rows if row.get("baseline_score") is not None]) if any(row.get("baseline_score") is not None for row in rows) else None,
                "mean_delay_cost_20d": mean([float(row["later_buy_delay_cost_20d"]) for row in rows if row.get("later_buy_delay_cost_20d") is not None]) if any(row.get("later_buy_delay_cost_20d") is not None for row in rows) else None,
            }
        )
    symbol_summary.sort(key=lambda item: (-item["count"], item["symbol"]))

    monthly_summary = []
    for month, rows in sorted(_group_by(skipped_rows, lambda row: str(row.get("month_key") or "")).items()):
        ret20 = [float(row["ret_20"]) for row in rows if row.get("ret_20") is not None]
        monthly_summary.append(
            {
                "month_key": month,
                "count": len(rows),
                "skipped_good_buy_count": sum(1 for row in rows if row.get("skip_class") == "skipped_good_buy"),
                "skipped_bad_buy_count": sum(1 for row in rows if row.get("skip_class") == "skipped_bad_buy"),
                "ret_20": _distribution(ret20),
            }
        )

    entry_delay_rows = [row for row in skipped_rows if row.get("later_buy_within_window") and row.get("skip_class") == "skipped_good_buy"]
    entry_delay_cost_summary = {
        "count": len(entry_delay_rows),
        "mean_delay_days": mean([float(row["later_buy_delay_days"]) for row in entry_delay_rows if row.get("later_buy_delay_days") is not None]) if any(row.get("later_buy_delay_days") is not None for row in entry_delay_rows) else None,
        "mean_delay_cost_20d": mean([float(row["later_buy_delay_cost_20d"]) for row in entry_delay_rows if row.get("later_buy_delay_cost_20d") is not None]) if any(row.get("later_buy_delay_cost_20d") is not None for row in entry_delay_rows) else None,
        "median_delay_cost_20d": median([float(row["later_buy_delay_cost_20d"]) for row in entry_delay_rows if row.get("later_buy_delay_cost_20d") is not None]) if entry_delay_rows and any(row.get("later_buy_delay_cost_20d") is not None for row in entry_delay_rows) else None,
        "later_buy_rate": (len(entry_delay_rows) / len(good_rows)) if good_rows else None,
    }

    window_coverage_summary = {
        "schema_version": "tradex_long_action_policy_foundation_v1_effectiveness_review_expanded",
        "expansion_mode": EXPANSION_MODE,
        "window_count": len(window_specs),
        "window_months_seen": sorted(window_months_seen),
        "date_range": {
            "first_window_start_date": min(spec["window_start_date"] for spec in window_specs),
            "last_window_end_date": max(spec["window_end_date"] for spec in window_specs),
        },
        "regime_coverage": dict(Counter(spec["label"] for spec in window_specs)),
        "month_coverage": sorted(month_coverage),
        "symbol_coverage": {
            "universe_size": len(universe),
            "symbols_seen_in_actions": len(symbol_coverage),
        },
        "baseline_row_count": baseline_row_count,
        "variant_row_count": variant_row_count,
        "baseline_filled_buy_count": baseline_filled_buy_count,
        "variant_filled_buy_count": variant_filled_buy_count,
        "baseline_skipped_buy_count": baseline_skipped_buy_count,
        "variant_skipped_buy_count": variant_skipped_buy_count,
        "branch_transition_counts": {f"{a}->{b}": c for (a, b), c in branch_transition_counts.items()},
        "state_diff_counts": dict(state_diff_counts),
        "excluded_candidates": input_resolution.get("excluded_candidates", []),
    }

    comparison_delta = {
        "net_return_mean": float(variant_aggregate["net_return_mean"] or 0.0) - float(baseline_aggregate["net_return_mean"] or 0.0),
        "turnover_adjusted_return": float(variant_aggregate["turnover_adjusted_return"] or 0.0) - float(baseline_aggregate["turnover_adjusted_return"] or 0.0),
        "opportunity_cost_adjusted_return": float(variant_aggregate["opportunity_cost_adjusted_return"] or 0.0) - float(baseline_aggregate["opportunity_cost_adjusted_return"] or 0.0),
        "max_drawdown_worst": float(variant_aggregate["max_drawdown_worst"] or 0.0) - float(baseline_aggregate["max_drawdown_worst"] or 0.0),
    }
    daily_pnl_distribution = {
        "baseline": _distribution(daily_pnl_baseline),
        "variant": _distribution(daily_pnl_variant),
        "delta_mean": (sum(daily_pnl_variant) / len(daily_pnl_variant) if daily_pnl_variant else 0.0) - (sum(daily_pnl_baseline) / len(daily_pnl_baseline) if daily_pnl_baseline else 0.0),
    }
    portfolio_economic_comparison = {
        "schema_version": "tradex_long_action_policy_foundation_v1_effectiveness_review_expanded",
        "baseline_name": BASELINE_NAME,
        "variant_name": VARIANT_NAME,
        "window_count": len(window_specs),
        "baseline_aggregate": baseline_aggregate,
        "variant_aggregate": variant_aggregate,
        "comparison_delta": comparison_delta,
        "daily_pnl_distribution": daily_pnl_distribution,
        "drawdown_duration": {
            "baseline": _distribution([float(value) for value in drawdown_duration_baseline if value is not None]),
            "variant": _distribution([float(value) for value in drawdown_duration_variant if value is not None]),
        },
        "exposure_metrics": {
            "baseline": {
                "gross_exposure_mean": mean(gross_exposure_baseline) if gross_exposure_baseline else 0.0,
                "net_exposure_mean": mean(net_exposure_baseline) if net_exposure_baseline else 0.0,
            },
            "variant": {
                "gross_exposure_mean": mean(gross_exposure_variant) if gross_exposure_variant else 0.0,
                "net_exposure_mean": mean(net_exposure_variant) if net_exposure_variant else 0.0,
            },
        },
        "cost_slippage": {
            "baseline": {
                "cost_amount_total": sum(float(row.get("cost_amount") or 0.0) for result in baseline_results for row in result.get("portfolio_daily_action_ledger") or []),
                "slippage_amount_total": sum(float(row.get("slippage_amount") or 0.0) for result in baseline_results for row in result.get("portfolio_daily_action_ledger") or []),
            },
            "variant": {
                "cost_amount_total": sum(float(row.get("cost_amount") or 0.0) for result in variant_results for row in result.get("portfolio_daily_action_ledger") or []),
                "slippage_amount_total": sum(float(row.get("slippage_amount") or 0.0) for result in variant_results for row in result.get("portfolio_daily_action_ledger") or []),
            },
        },
        "action_counts": {
            "baseline": dict(Counter(row.get("action") for result in baseline_results for row in result.get("portfolio_daily_action_ledger") or [])),
            VARIANT_NAME: dict(Counter(row.get("action") for result in variant_results for row in result.get("portfolio_daily_action_ledger") or [])),
        },
        "filled_buy_counts": {
            "baseline": baseline_filled_buy_count,
            VARIANT_NAME: variant_filled_buy_count,
        },
        "skipped_buy_counts": {
            "baseline": baseline_skipped_buy_count,
            VARIANT_NAME: variant_skipped_buy_count,
        },
        "window_summaries": window_coverage_rows,
    }

    # Drawdown attribution based on per-window drawdown gap and branch behavior.
    drawdown_attribution_rows = []
    worse_drawdown_windows = []
    for payload in window_payloads:
        baseline = payload["baseline"]
        variant = payload["variant"]
        baseline_summary = baseline.get("window_summary") or {}
        variant_summary = variant.get("window_summary") or {}
        gap = _safe_float(variant_summary.get("max_drawdown"), 0.0) - _safe_float(baseline_summary.get("max_drawdown"), 0.0)
        if gap is not None and gap > 0:
            worse_drawdown_windows.append(
                {
                    "window_id": payload["window_spec"]["window_id"],
                    "window_label": payload["window_spec"]["label"],
                    "window_start_date": payload["window_spec"]["window_start_date"],
                    "window_end_date": payload["window_spec"]["window_end_date"],
                    "max_drawdown_gap": gap,
                }
            )
        base_rows = {(row["date"], row["symbol"]): row for row in baseline.get("portfolio_daily_action_ledger") or []}
        var_rows = {(row["date"], row["symbol"]): row for row in variant.get("portfolio_daily_action_ledger") or []}
        common_keys = sorted(set(base_rows).intersection(var_rows))
        for key in common_keys:
            b = base_rows[key]
            v = var_rows[key]
            if b.get("cash") != v.get("cash") or b.get("position_value") != v.get("position_value"):
                drawdown_attribution_rows.append(
                    {
                        "window_id": payload["window_spec"]["window_id"],
                        "date": b.get("date"),
                        "symbol": b.get("symbol"),
                        "cash_delta": _safe_float(v.get("cash"), 0.0) - _safe_float(b.get("cash"), 0.0),
                        "position_value_delta": _safe_float(v.get("position_value"), 0.0) - _safe_float(b.get("position_value"), 0.0),
                        "drawdown_delta": _safe_float(v.get("drawdown"), 0.0) - _safe_float(b.get("drawdown"), 0.0),
                        "baseline_action": b.get("action"),
                        "variant_action": v.get("action"),
                    }
                )
    drawdown_attribution_summary = {
        "schema_version": "tradex_long_action_policy_foundation_v1_effectiveness_review_expanded",
        "worse_drawdown_windows": worse_drawdown_windows,
        "top_symbol_contributors": [
            {
                "window_id": row["window_id"],
                "date": row["date"],
                "symbol": row["symbol"],
                "cash_delta": row["cash_delta"],
                "position_value_delta": row["position_value_delta"],
                "drawdown_delta": row["drawdown_delta"],
            }
            for row in sorted(drawdown_attribution_rows, key=lambda row: abs(row["drawdown_delta"]), reverse=True)[:50]
        ],
        "cause_breakdown": {
            "missed_profitable_buys": len(good_rows),
            "true_positive_skips": len(bad_rows),
            "delay_cost_entries": len(entry_delay_rows),
            "cost_slippage_rows": sum(1 for result in baseline_results for row in result.get("portfolio_daily_action_ledger") or [] if float(row.get("cost_amount") or 0.0) > 0 or float(row.get("slippage_amount") or 0.0) > 0),
        },
        "notes": [
            "drawdown attribution is based on branch-linked ledger deltas and window-level drawdown gaps",
            "this is diagnostic, not a separate policy decision",
        ],
    }

    regime_effectiveness_summary = {
        "schema_version": "tradex_long_action_policy_foundation_v1_effectiveness_review_expanded",
        "window_coverage": {
            "window_count": len(window_specs),
            "regime_counts": dict(Counter(spec["label"] for spec in window_specs)),
            "month_coverage": sorted(month_coverage),
        },
        "portfolio_comparison": {
            "baseline_aggregate": baseline_aggregate,
            "variant_aggregate": variant_aggregate,
            "comparison_delta": comparison_delta,
        },
        "regime_breakdown": [
            {
                "window_id": row["window_id"],
                "window_label": row["window_label"],
                "regime": row["regime"],
                "window_months": row["window_months"],
                "baseline": row["baseline_aggregate"],
                "variant": row["variant_aggregate"],
                "delta": row["delta"],
            }
            for row in window_regime_rows
        ],
        "interpretation": (
            "expanded windows show whether the gate is consistent across more monthly anchors; "
            "keep requires broad improvement without materially worse drawdown"
        ),
    }

    good_bucket_mean = skipped_buy_bucket_decomposition["good_bucket"]["ret_20"]["mean"]
    bad_bucket_mean = skipped_buy_bucket_decomposition["bad_bucket"]["ret_20"]["mean"]
    good_bucket_win_rate = skipped_buy_bucket_decomposition["good_bucket"]["ret_20"]["win_rate"]
    bad_bucket_win_rate = skipped_buy_bucket_decomposition["bad_bucket"]["ret_20"]["win_rate"]

    decision = "hold_needs_more_windows"
    if (
        comparison_delta["net_return_mean"] <= 0
        and comparison_delta["turnover_adjusted_return"] <= 0
        and comparison_delta["opportunity_cost_adjusted_return"] <= 0
        and comparison_delta["max_drawdown_worst"] > -0.05
    ):
        decision = "drop"
    elif (
        good_bucket_mean is not None
        and good_bucket_mean > 0
        and good_bucket_win_rate is not None
        and good_bucket_win_rate >= 0.95
        and bad_bucket_mean is not None
        and bad_bucket_mean < 0
        and bad_bucket_win_rate == 0.0
        and comparison_delta["net_return_mean"] > 0
        and comparison_delta["turnover_adjusted_return"] > 0
    ):
        decision = "needs_gate_redesign_before_more_replay"
    elif len(good_rows) > len(bad_rows) * 2 and comparison_delta["net_return_mean"] > 0 and comparison_delta["turnover_adjusted_return"] > 0 and comparison_delta["max_drawdown_worst"] <= 0:
        decision = "keep_for_refinement"
    elif len(good_rows) > len(bad_rows) and comparison_delta["net_return_mean"] > 0 and comparison_delta["turnover_adjusted_return"] > 0 and comparison_delta["max_drawdown_worst"] > 0:
        decision = "needs_gate_redesign_before_more_replay"
    elif len(window_specs) < 8:
        decision = "hold_needs_more_windows"

    expanded_effectiveness_decision = {
        "schema_version": "tradex_long_action_policy_foundation_v1_effectiveness_review_expanded",
        "decision": decision,
        "reason": (
            "branching is real and economically active, but the gate is skipping a clearly profitable bucket; "
            "more replay of the same gate is lower value than redesigning the entry gate"
            if decision == "needs_gate_redesign_before_more_replay"
            else (
                "branching is real and economically active, but the skipped-good bucket remains material "
                "and drawdown is not yet clean enough to move to keep"
                if decision != "drop"
                else "expanded windows show no durable portfolio advantage and the skipped-good bucket remains material"
            )
        ),
        "window_count": len(window_specs),
        "authoritative_artifacts": {
            "portfolio_economic_comparison": "portfolio_economic_comparison.json",
            "skipped_buy_bucket_decomposition": "skipped_buy_bucket_decomposition.json",
            "drawdown_attribution_summary": "drawdown_attribution_summary.json",
        },
    }

    session_id = _make_session_id()
    session_dir = output_root / session_id
    session_dir.mkdir(parents=True, exist_ok=False)

    run_manifest = {
        "schema_version": "tradex_long_action_policy_foundation_v1_effectiveness_review_expanded",
        "family_id": FAMILY_ID,
        "script_name": SCRIPT_NAME,
        "session_id": session_id,
        "output_dir": str(session_dir),
        "expansion_mode": EXPANSION_MODE,
        "window_count": len(window_specs),
        "baseline_name": BASELINE_NAME,
        "variant_name": VARIANT_NAME,
        "comparison_artifacts": [
            "portfolio_economic_comparison.json",
            "branch_effect_audit.json",
            "skipped_buy_bucket_decomposition.json",
            "false_negative_skip_summary.json",
            "true_positive_skip_summary.json",
            "drawdown_attribution_summary.json",
            "regime_effectiveness_summary.json",
        ],
        "diagnostic_artifacts": [
            "symbol_level_effectiveness_summary.json",
            "monthly_effectiveness_summary.json",
            "entry_delay_cost_summary.json",
        ],
        "policy_locked": True,
        "policy_thresholds_changed": False,
        "mee_mee_reflection_approved": False,
        "research_fallback": True,
    }
    input_resolution = {
        "schema_version": "tradex_long_action_policy_foundation_v1_effectiveness_review_expanded",
        "source_db_path": str(resolve_runtime_stock_db_path()),
        "source_db_contract": inspect_runtime_stock_db(),
        "repo_class": type(repo).__name__,
        "universe_size": len(universe),
        "universe_source": "StockRepository.get_latest_params_for_screening() with fallback to get_all_codes()",
        "research_inventory_artifact": str(REPO_ROOT / "artifacts" / "research_inventory" / "research_inventory.json"),
        "foundation_artifact_directory": str(Path(r"G:\Tradex\research_sessions\long_action_policy_foundation_v1\20260430T092731Z-333b4a7b")),
        "window_generation_mode": EXPANSION_MODE,
        "window_generation_notes": [
            "first trading day of each month",
            "uptrend windows use 3 months; down/flat windows use 1 month",
            "candidate windows beyond the future-data cutoff are excluded explicitly",
        ],
        "excluded_candidates": input_resolution.get("excluded_candidates", []),
    }

    skipped_rows_df = pd.DataFrame(skipped_rows)
    if not skipped_rows_df.empty:
        skipped_rows_df.to_parquet(session_dir / "skipped_buy_cases.parquet", index=False)
    else:
        pd.DataFrame(columns=["window_id", "date", "symbol"]).to_parquet(session_dir / "skipped_buy_cases.parquet", index=False)

    artifacts = {
        "run_manifest.json": run_manifest,
        "input_resolution.json": input_resolution,
        "window_coverage_summary.json": window_coverage_summary,
        "branch_effect_audit.json": {
            "schema_version": "tradex_long_action_policy_foundation_v1_effectiveness_review_expanded",
            "baseline_action_counts": dict(Counter(row.get("action") for result in baseline_results for row in result.get("portfolio_daily_action_ledger") or [])),
            "variant_action_counts": dict(Counter(row.get("action") for result in variant_results for row in result.get("portfolio_daily_action_ledger") or [])),
            "branch_row_count": sum(c for (ba, va), c in branch_transition_counts.items() if ba != va),
            "action_transition_counts": {f"{a}->{b}": c for (a, b), c in branch_transition_counts.items()},
            "rows_with_state_diffs": dict(state_diff_counts),
            "filled_buy_counts": {
                "baseline": baseline_filled_buy_count,
                VARIANT_NAME: variant_filled_buy_count,
            },
            "unfilled_buy_counts": {
                "baseline": baseline_skipped_buy_count,
                VARIANT_NAME: variant_skipped_buy_count,
            },
            "branch_affects_fills_positions_capital": bool(any(state_diff_counts.get(field, 0) > 0 for field in ["cash", "gross_exposure", "net_exposure", "position_qty", "position_value"])),
        },
        "portfolio_economic_comparison.json": portfolio_economic_comparison,
        "skipped_buy_bucket_decomposition.json": skipped_buy_bucket_decomposition,
        "false_negative_skip_summary.json": false_negative_summary,
        "true_positive_skip_summary.json": true_positive_summary,
        "drawdown_attribution_summary.json": drawdown_attribution_summary,
        "regime_effectiveness_summary.json": regime_effectiveness_summary,
        "expanded_effectiveness_decision.json": expanded_effectiveness_decision,
        "entry_delay_cost_summary.json": entry_delay_cost_summary,
        "symbol_level_effectiveness_summary.json": {
            "schema_version": "tradex_long_action_policy_foundation_v1_effectiveness_review_expanded",
            "symbols": symbol_summary[:200],
            "symbol_count": len(symbol_summary),
        },
        "monthly_effectiveness_summary.json": {
            "schema_version": "tradex_long_action_policy_foundation_v1_effectiveness_review_expanded",
            "months": monthly_summary,
            "month_count": len(monthly_summary),
        },
    }

    for filename, payload in artifacts.items():
        _write_json(session_dir / filename, payload)

    complete = {
        "schema_version": "tradex_long_action_policy_foundation_v1_effectiveness_review_expanded",
        "family_id": FAMILY_ID,
        "generated_at_utc": _utc_now(),
        "session_id": session_id,
        "output_dir": str(session_dir),
        "artifact_list": [
            "run_manifest.json",
            "input_resolution.json",
            "window_coverage_summary.json",
            "branch_effect_audit.json",
            "portfolio_economic_comparison.json",
            "skipped_buy_bucket_decomposition.json",
            "skipped_buy_cases.parquet",
            "false_negative_skip_summary.json",
            "true_positive_skip_summary.json",
            "drawdown_attribution_summary.json",
            "regime_effectiveness_summary.json",
            "expanded_effectiveness_decision.json",
            "_ARTIFACT_COMPLETE.json",
            "effectiveness_review_summary.md",
            "symbol_level_effectiveness_summary.json",
            "monthly_effectiveness_summary.json",
            "entry_delay_cost_summary.json",
        ],
        "commands_run": [
            f"python {SCRIPT_NAME}.py --output-root {output_root} --jobs {jobs}",
        ],
        "verification_status": "generated",
        "git_status_short": _safe_git_output(["git", "status", "--short"]),
        "git_diff_name_only": _safe_git_output(["git", "diff", "--name-only"]),
        "notes": [
            "TRADEX-only expanded effectiveness review",
            "window expansion is a research-fallback monthly-anchor expansion",
        ],
    }
    _write_json(session_dir / "_ARTIFACT_COMPLETE.json", complete)

    summary_md = f"""# Expanded Effectiveness Review Summary

- Review directory: `{session_dir}`
- Expansion mode: `{EXPANSION_MODE}`
- Decision: `{expanded_effectiveness_decision['decision']}`

## Readout
- Branching is real and propagates into fills, positions, cash, exposure, and pnl.
- The gate still skips a positive bucket.
- Expanded windows are broad enough to judge refinement vs redesign.
"""
    (session_dir / "effectiveness_review_summary.md").write_text(summary_md, encoding="utf-8")

    return {
        "session_id": session_id,
        "output_dir": str(session_dir),
        "decision": expanded_effectiveness_decision["decision"],
        "artifacts": {key: str(session_dir / key) for key in artifacts},
        "complete": str(session_dir / "_ARTIFACT_COMPLETE.json"),
    }


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="TRADEX long-action policy expanded effectiveness review")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT, help="Root output directory for the review session")
    parser.add_argument("--db-path", type=Path, default=None, help="Path to the stock repository DuckDB database")
    parser.add_argument("--jobs", type=int, default=2, help="Parallel window jobs to use when simulating windows")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    repo = _resolve_repo(args.db_path)
    result = build_expanded_effectiveness_review(repo, args.output_root, jobs=max(1, int(args.jobs or 1)))
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
