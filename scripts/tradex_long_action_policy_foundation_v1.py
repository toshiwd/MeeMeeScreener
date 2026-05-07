from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import uuid
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.infra.duckdb.stock_repo import StockRepository
from external_analysis.policy_replay.simulator import (
    DEFAULT_COST_MODEL,
    _avg,
    _iso,
    _num,
    _parse_date,
    _simulate_window,
    _stable_hash,
    _text,
    normalize_replay_run_config,
    prepare_replay_window_context,
)

DEFAULT_DB_PATH = Path(
    os.getenv("STOCKS_DB_PATH")
    or os.getenv("TRADEX_SNAPSHOT_DB_PATH")
    or r"C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\research_sessions\long_action_policy_foundation_v1")
SCRIPT_NAME = "tradex_long_action_policy_foundation_v1"

FAMILY_ID = "long_action_policy_foundation_v1"
BASELINE_NAME = "long_entry_cash_gate_baseline_v1"
VARIANT_NAME = "long_entry_cash_gate_v1"
ACTION_POLICY_MODE_BASELINE = "long_entry_cash_gate_baseline_v1"
ACTION_POLICY_MODE_VARIANT = "long_entry_cash_gate_v1"

WINDOW_SPECS_DEFAULT: list[dict[str, Any]] = [
    {
        "window_id": "flat_20230803",
        "label": "flat",
        "window_start_date": "2023-08-03",
        "window_months": 1,
    },
    {
        "window_id": "down_20250331",
        "label": "down",
        "window_start_date": "2025-03-31",
        "window_months": 1,
    },
    {
        "window_id": "up_20250626",
        "label": "up",
        "window_start_date": "2025-06-26",
        "window_months": 3,
    },
]

EVALUATION_COST_MODEL = {
    **DEFAULT_COST_MODEL,
    "schema_version": "tradex_daily_action_cost_model_v1",
    "enabled": True,
    "commission_bps": 5.0,
    "slippage_bps": 5.0,
    "tax_or_fee_bps": 0.0,
    "min_fee": 0.0,
    "status": "provisional_placeholder",
}

DEFAULT_WEIGHTS = {
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

ENTRY_GATE_VARIANT = {
    "enabled": True,
    "market_ret20_min": -0.01,
    "breadth_above_ma20_max": 0.50,
    "liquidity20d_min": 438_345.4875,
    "turnover_z20_min": 0.0,
    "diff20_pct_max": 0.01,
    "candle_upper_wick_ratio_max": 0.30,
}


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


def _resolve_repo(db_path: Path | None = None) -> StockRepository:
    path = Path(db_path or DEFAULT_DB_PATH)
    return StockRepository(str(path))


def _resolve_universe(repo: Any) -> list[str]:
    codes: list[str] = []
    try:
        rows = repo.get_latest_params_for_screening()
        codes = [str(row[0]).strip() for row in rows if row and str(row[0]).strip()]
    except Exception:
        codes = []
    if not codes:
        try:
            codes = [str(code).strip() for code in repo.get_all_codes() if str(code).strip()]
        except Exception:
            codes = []
    return sorted(dict.fromkeys(codes))


def _build_payload(
    *,
    universe: list[str],
    window_start_date: str,
    window_months: int,
    policy_version: str,
    action_policy: dict[str, Any],
) -> dict[str, Any]:
    return {
        "policy_id": FAMILY_ID,
        "policy_version": policy_version,
        "window_start_date": window_start_date,
        "window_months": int(window_months),
        "universe": list(universe),
        "market_benchmark_symbol": "1306",
        "initial_capital_jpy": 10_000_000,
        "gross_exposure_cap_jpy": 10_000_000,
        "execution_convention": "next_session_open",
        "decision_window_start_date": window_start_date,
        "decision_window_end_date": window_start_date,
        "execution_buffer_days": 1,
        "outcome_buffer_days": max(20, int(window_months) * 21),
        "cost_model": dict(EVALUATION_COST_MODEL),
        "action_policy": dict(action_policy),
        "weekly_activity_required": False,
        "short_cash_reusable": False,
        "weights": dict(DEFAULT_WEIGHTS),
        "entry_threshold": 0.04,
        "exit_threshold": -0.10,
        "add_threshold": 0.80,
        "partial_take_threshold": 0.08,
        "stop_loss_threshold": -0.06,
        "selection_rule_change_log": [
            {
                "reason_code": "initial_policy",
                "reason_text": "initial long-only entry gate foundation",
                "expected_effect": "compare baseline long entry against a long-entry gate under next-session-open execution",
                "author_or_source": "system",
                "timestamp_or_run_id": "foundation",
            }
        ],
    }


def _slice_context_to_window(context: dict[str, Any], end_date: date) -> dict[str, Any]:
    end_key = _iso(end_date)
    filtered_dates = [current for current in context["dates"] if current <= end_date]
    if not filtered_dates:
        raise ValueError(f"no trading dates available through {end_key}")
    feature_grid = {
        date_key: feature_map
        for date_key, feature_map in dict(context.get("feature_grid") or {}).items()
        if _parse_date(date_key) <= end_date
    }
    benchmark_market = [row for row in list(context.get("benchmark_market") or []) if _parse_date(row["date"]) <= end_date]
    benchmark_universe = [row for row in list(context.get("benchmark_universe") or []) if _parse_date(row["date"]) <= end_date]
    market_context_by_date = {
        date_key: item
        for date_key, item in dict(context.get("market_context_by_date") or {}).items()
        if _parse_date(date_key) <= end_date
    }
    sliced = dict(context)
    sliced["dates"] = filtered_dates
    sliced["window_end"] = end_key
    sliced["feature_grid"] = feature_grid
    sliced["benchmark_market"] = benchmark_market
    sliced["benchmark_universe"] = benchmark_universe
    sliced["market_context_by_date"] = market_context_by_date
    return sliced


def _classify_regime(market_context: dict[str, Any] | None) -> str:
    if not market_context:
        return "unknown"
    market_ret20 = market_context.get("market_ret20")
    breadth = market_context.get("breadth_above_ma20")
    if market_ret20 is None or breadth is None:
        return "unknown"
    market_ret20 = float(market_ret20)
    breadth = float(breadth)
    if market_ret20 < -0.01 and breadth <= 0.50:
        return "downtrend"
    if market_ret20 > 0.01 and breadth >= 0.50:
        return "uptrend"
    return "sideways"


def _count_actions(rows: list[dict[str, Any]]) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for row in rows:
        counter[str(row.get("action") or row.get("source_action_taken") or "hold")] += 1
    return dict(sorted(counter.items()))


def _profit_factor(trade_rows: list[dict[str, Any]]) -> float | None:
    gains = 0.0
    losses = 0.0
    for row in trade_rows:
        pnl = row.get("realized_pnl_delta")
        if pnl is None:
            continue
        pnl = float(pnl)
        if pnl > 0:
            gains += pnl
        elif pnl < 0:
            losses += abs(pnl)
    if losses == 0.0:
        return None if gains == 0.0 else float("inf")
    return gains / losses


def _monthly_returns(daily_curve: list[dict[str, Any]]) -> list[float]:
    by_month: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in daily_curve:
        by_month[str(row["date"])[:7]].append(row)
    returns: list[float] = []
    for month_key, rows in sorted(by_month.items()):
        if len(rows) < 2:
            continue
        first = float(rows[0]["equity"])
        last = float(rows[-1]["equity"])
        if first > 0:
            returns.append(last / first - 1.0)
    return returns


def _aggregate_scenario(windows: list[dict[str, Any]]) -> dict[str, Any]:
    if not windows:
        return {
            "window_count": 0,
            "ending_equity_mean": None,
            "net_return_mean": None,
            "max_drawdown_worst": None,
            "worst_month_return": None,
            "profit_factor": None,
            "turnover_adjusted_return": None,
            "monthly_positive_rate": None,
            "return_per_unit_drawdown": None,
            "average_holding_period": None,
            "exposure_utilization": None,
            "opportunity_cost_adjusted_return": None,
        }
    portfolio_returns = [float(window["window_summary"]["portfolio_return_3m"]) for window in windows]
    max_drawdown = [float(window["window_summary"]["max_drawdown"]) for window in windows]
    holding_days = [float(window["window_summary"]["avg_holding_days"]) for window in windows]
    gross_exposure = [float(window["relative_performance"]["average_net_exposure"]) for window in windows]
    turn = [float(window["window_summary"]["turnover"]) for window in windows]
    excess = [float(window["relative_performance"]["excess_vs_universe"]) for window in windows]
    monthly_returns = [value for window in windows for value in _monthly_returns(window["daily_equity_curve"])]
    return {
        "window_count": len(windows),
        "ending_equity_mean": mean([10_000_000.0 * (1.0 + value) for value in portfolio_returns]),
        "net_return_mean": mean(portfolio_returns),
        "max_drawdown_worst": min(max_drawdown),
        "worst_month_return": min(monthly_returns) if monthly_returns else None,
        "profit_factor": _profit_factor([trade for window in windows for trade in window["trade_ledger"]]),
        "turnover_adjusted_return": mean(excess) / max(0.25, mean(turn) if turn else 0.25),
        "monthly_positive_rate": None if not monthly_returns else sum(1 for value in monthly_returns if value > 0) / len(monthly_returns),
        "return_per_unit_drawdown": None if not max_drawdown or min(max_drawdown) == 0 else mean(portfolio_returns) / abs(min(max_drawdown)),
        "average_holding_period": mean(holding_days),
        "exposure_utilization": mean(gross_exposure),
        "opportunity_cost_adjusted_return": mean(excess),
    }


def _build_diagnostics(
    *,
    windows: list[dict[str, Any]],
    scenario_name: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if not windows:
        return (
            {
                "schema_version": "tradex_long_action_policy_foundation_v1",
                "scenario": scenario_name,
                "would_rotate_count": 0,
                "stronger_candidate_count": 0,
                "estimated_opportunity_gap_mean": None,
                "estimated_opportunity_gap_median": None,
                "executed_rotation": False,
                "notes": ["no rows available"],
            },
            {
                "schema_version": "tradex_long_action_policy_foundation_v1",
                "scenario": scenario_name,
                "hedge_pressure_count": 0,
                "regime_deterioration_flags": 0,
                "executed_hedge": False,
                "notes": ["no rows available"],
            },
        )
    would_rotate = 0
    stronger_candidates = 0
    gaps: list[float] = []
    hedge_pressure = 0
    regime_flags = 0
    for window in windows:
        action_rows = list(window["portfolio_daily_action_ledger"])
        selection_rows = list(window["daily_selection_snapshot"])
        if not action_rows or not selection_rows:
            continue
        for snapshot in selection_rows:
            candidates = list(snapshot.get("candidates") or [])
            if not candidates:
                continue
            top_candidate = candidates[0]
            held_rows = [
                item
                for item in action_rows
                if item.get("date") == snapshot.get("date")
                and float(item.get("position_qty") or 0.0) > 0
                and item.get("side") == "long"
            ]
            held_symbol = held_rows[0]["symbol"] if held_rows else None
            held_score = next((float(item.get("score") or 0.0) for item in candidates if item.get("symbol") == held_symbol), None)
            if held_symbol and held_symbol != top_candidate["symbol"]:
                best_score = float(top_candidate.get("score") or 0.0)
                if held_score is None or best_score > held_score:
                    would_rotate += 1
                    stronger_candidates += 1
                    gaps.append(best_score - float(held_score or 0.0))
        for item in window["regime_split_rows"]:
            if item.get("regime") == "downtrend" and float(item.get("long_exposure_days") or 0.0) > 0:
                hedge_pressure += 1
            if item.get("regime") == "downtrend":
                regime_flags += 1
    return (
        {
            "schema_version": "tradex_long_action_policy_foundation_v1",
            "scenario": scenario_name,
            "would_rotate_count": would_rotate,
            "stronger_candidate_count": stronger_candidates,
            "estimated_opportunity_gap_mean": mean(gaps) if gaps else None,
            "estimated_opportunity_gap_median": median(gaps) if gaps else None,
            "executed_rotation": False,
            "notes": ["rotation is diagnostic only in this pass"],
        },
        {
            "schema_version": "tradex_long_action_policy_foundation_v1",
            "scenario": scenario_name,
            "hedge_pressure_count": hedge_pressure,
            "regime_deterioration_flags": regime_flags,
            "executed_hedge": False,
            "notes": ["hedge is diagnostic only in this pass"],
        },
    )


def _window_months_for(start_date: str, end_date: str) -> int:
    start = _parse_date(start_date)
    end = _parse_date(end_date)
    months = (end.year - start.year) * 12 + (end.month - start.month)
    return max(1, months + 1)


def _run_scenario_window(
    *,
    repo: Any,
    universe: list[str],
    window_spec: dict[str, Any],
    run_config: dict[str, Any],
    scenario_name: str,
) -> dict[str, Any]:
    start_date = _parse_date(window_spec["window_start_date"])
    context = prepare_replay_window_context(repo, run_config, start_date)
    end_date = _parse_date(window_spec.get("window_end_date") or context["window_end"])
    context = _slice_context_to_window(context, end_date)
    result = _simulate_window(repo, run_config, start_date, context=context)
    market_context_by_date = dict(context.get("market_context_by_date") or {})
    regime_rows: list[dict[str, Any]] = []
    regime_counts: Counter[str] = Counter()
    for row in result["daily_equity_curve"]:
        regime_label = _classify_regime(market_context_by_date.get(row["date"]))
        regime_counts[regime_label] += 1
        regime_rows.append(
            {
                "date": row["date"],
                "regime": regime_label,
                "market_ret20": None if market_context_by_date.get(row["date"]) is None else market_context_by_date[row["date"]].get("market_ret20"),
                "breadth_above_ma20": None if market_context_by_date.get(row["date"]) is None else market_context_by_date[row["date"]].get("breadth_above_ma20"),
                "long_exposure_days": sum(1 for item in result["portfolio_daily_action_ledger"] if item["date"] == row["date"] and item.get("side") == "long" and float(item.get("position_qty") or 0.0) > 0),
            }
        )
    result["regime_split_rows"] = regime_rows
    result["regime_split_counts"] = dict(regime_counts)
    result["scenario"] = scenario_name
    result["window_id"] = window_spec["window_id"]
    result["window_label"] = window_spec["label"]
    result["window_start_date"] = window_spec["window_start_date"]
    result["window_end_date"] = _iso(end_date)
    return result


def build_long_action_policy_foundation(
    repo: Any,
    output_root: Path,
    *,
    window_specs: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    window_specs = list(window_specs or WINDOW_SPECS_DEFAULT)
    universe = _resolve_universe(repo)
    if not universe:
        raise ValueError("missing candidate universe from stock repository")
    if "1306" not in universe:
        universe = ["1306", *[code for code in universe if code != "1306"]]

    baseline_payload = _build_payload(
        universe=universe,
        window_start_date=window_specs[0]["window_start_date"],
        window_months=int(window_specs[0]["window_months"]),
        policy_version=BASELINE_NAME,
        action_policy={
            "schema_version": "tradex_daily_action_policy_v1",
            "mode": ACTION_POLICY_MODE_BASELINE,
            "enabled": True,
            "allow_long_entries": True,
            "allow_short_entries": False,
            "allow_position_management": False,
            "allow_weekly_activity": False,
            "entry_gate": {"enabled": False},
            "notes": ["baseline long-only entry/cash control without the gate"],
        },
    )
    variant_payload = _build_payload(
        universe=universe,
        window_start_date=window_specs[0]["window_start_date"],
        window_months=int(window_specs[0]["window_months"]),
        policy_version=VARIANT_NAME,
        action_policy={
            "schema_version": "tradex_daily_action_policy_v1",
            "mode": ACTION_POLICY_MODE_VARIANT,
            "enabled": True,
            "allow_long_entries": True,
            "allow_short_entries": False,
            "allow_position_management": False,
            "allow_weekly_activity": False,
            "entry_gate": dict(ENTRY_GATE_VARIANT),
            "notes": [
                "gate long entries using regime mismatch, liquidity continuation, and stretched-entry diagnostics",
            ],
        },
    )
    baseline_run_config = normalize_replay_run_config(baseline_payload)
    variant_run_config = normalize_replay_run_config(variant_payload)

    baseline_windows: list[dict[str, Any]] = []
    variant_windows: list[dict[str, Any]] = []
    for spec in window_specs:
        payload_baseline = dict(baseline_run_config)
        payload_baseline["window_start_date"] = spec["window_start_date"]
        payload_baseline["window_months"] = int(spec["window_months"])
        payload_baseline["window_start_dates"] = [spec["window_start_date"]]
        payload_baseline["decision_window_start_date"] = spec["window_start_date"]
        payload_baseline["decision_window_end_date"] = spec.get("decision_window_end_date") or spec["window_start_date"]
        payload_baseline["execution_buffer_days"] = int(spec.get("execution_buffer_days") or 1)
        payload_baseline["outcome_buffer_days"] = int(spec.get("outcome_buffer_days") or max(20, int(spec["window_months"]) * 21))
        payload_variant = dict(variant_run_config)
        payload_variant["window_start_date"] = spec["window_start_date"]
        payload_variant["window_months"] = int(spec["window_months"])
        payload_variant["window_start_dates"] = [spec["window_start_date"]]
        payload_variant["decision_window_start_date"] = spec["window_start_date"]
        payload_variant["decision_window_end_date"] = spec.get("decision_window_end_date") or spec["window_start_date"]
        payload_variant["execution_buffer_days"] = int(spec.get("execution_buffer_days") or 1)
        payload_variant["outcome_buffer_days"] = int(spec.get("outcome_buffer_days") or max(20, int(spec["window_months"]) * 21))
        baseline_windows.append(_run_scenario_window(repo=repo, universe=universe, window_spec=spec, run_config=payload_baseline, scenario_name="baseline"))
        variant_windows.append(_run_scenario_window(repo=repo, universe=universe, window_spec=spec, run_config=payload_variant, scenario_name=VARIANT_NAME))

    baseline_daily_rows = [
        {**row, "scenario": "baseline", "window_id": window["window_id"], "window_label": window["window_label"]}
        for window in baseline_windows
        for row in window["portfolio_daily_action_ledger"]
    ]
    variant_daily_rows = [
        {**row, "scenario": VARIANT_NAME, "window_id": window["window_id"], "window_label": window["window_label"]}
        for window in variant_windows
        for row in window["portfolio_daily_action_ledger"]
    ]
    all_daily_rows = baseline_daily_rows + variant_daily_rows

    baseline_aggregate = _aggregate_scenario(baseline_windows)
    variant_aggregate = _aggregate_scenario(variant_windows)

    comparison_windows = []
    for baseline_window, variant_window in zip(baseline_windows, variant_windows, strict=True):
        comparison_windows.append(
            {
                "window_id": baseline_window["window_id"],
                "window_label": baseline_window["window_label"],
                "window_start_date": baseline_window["window_start_date"],
                "window_end_date": baseline_window["window_end_date"],
                "baseline": {
                    "portfolio_return_3m": baseline_window["window_summary"]["portfolio_return_3m"],
                    "max_drawdown": baseline_window["window_summary"]["max_drawdown"],
                    "turnover": baseline_window["window_summary"]["turnover"],
                    "avg_holding_days": baseline_window["window_summary"]["avg_holding_days"],
                    "action_counts": _count_actions(baseline_window["portfolio_daily_action_ledger"]),
                },
                "variant": {
                    "portfolio_return_3m": variant_window["window_summary"]["portfolio_return_3m"],
                    "max_drawdown": variant_window["window_summary"]["max_drawdown"],
                    "turnover": variant_window["window_summary"]["turnover"],
                    "avg_holding_days": variant_window["window_summary"]["avg_holding_days"],
                    "action_counts": _count_actions(variant_window["portfolio_daily_action_ledger"]),
                },
                "delta": {
                    "portfolio_return_3m": float(variant_window["window_summary"]["portfolio_return_3m"]) - float(baseline_window["window_summary"]["portfolio_return_3m"]),
                    "max_drawdown": float(variant_window["window_summary"]["max_drawdown"]) - float(baseline_window["window_summary"]["max_drawdown"]),
                    "turnover": float(variant_window["window_summary"]["turnover"]) - float(baseline_window["window_summary"]["turnover"]),
                    "avg_holding_days": float(variant_window["window_summary"]["avg_holding_days"]) - float(baseline_window["window_summary"]["avg_holding_days"]),
                },
            }
        )

    baseline_diag, baseline_hedge = _build_diagnostics(windows=baseline_windows, scenario_name="baseline")
    variant_diag, variant_hedge = _build_diagnostics(windows=variant_windows, scenario_name=VARIANT_NAME)
    opportunity_cost_summary = {
        "schema_version": "tradex_long_action_policy_foundation_v1",
        "scenario": VARIANT_NAME,
        "baseline_compare_reference": BASELINE_NAME,
        "would_rotate_count": variant_diag["would_rotate_count"],
        "stronger_candidate_count": variant_diag["stronger_candidate_count"],
        "estimated_opportunity_gap_mean": variant_diag["estimated_opportunity_gap_mean"],
        "estimated_opportunity_gap_median": variant_diag["estimated_opportunity_gap_median"],
        "no_executed_rotation": True,
        "notes": ["rotation remains diagnostic-only in this pass"],
    }
    hedge_pressure_summary = {
        "schema_version": "tradex_long_action_policy_foundation_v1",
        "scenario": VARIANT_NAME,
        "baseline_compare_reference": BASELINE_NAME,
        "hedge_pressure_count": variant_hedge["hedge_pressure_count"],
        "regime_deterioration_flags": variant_hedge["regime_deterioration_flags"],
        "no_executed_hedge": True,
        "notes": ["hedge remains diagnostic-only in this pass"],
    }

    portfolio_replay_summary = {
        "schema_version": "tradex_long_action_policy_foundation_v1",
        "family_id": FAMILY_ID,
        "baseline_name": BASELINE_NAME,
        "variant_name": VARIANT_NAME,
        "execution_convention": "next_session_open",
        "cost_model": dict(EVALUATION_COST_MODEL),
        "window_count": len(window_specs),
        "comparison_windows": comparison_windows,
        "baseline_aggregate": baseline_aggregate,
        "variant_aggregate": variant_aggregate,
        "comparison_delta": {
            "net_return_mean": float(variant_aggregate["net_return_mean"] or 0.0) - float(baseline_aggregate["net_return_mean"] or 0.0),
            "max_drawdown_worst": float(variant_aggregate["max_drawdown_worst"] or 0.0) - float(baseline_aggregate["max_drawdown_worst"] or 0.0),
            "turnover_adjusted_return": float(variant_aggregate["turnover_adjusted_return"] or 0.0) - float(baseline_aggregate["turnover_adjusted_return"] or 0.0),
            "opportunity_cost_adjusted_return": float(variant_aggregate["opportunity_cost_adjusted_return"] or 0.0) - float(baseline_aggregate["opportunity_cost_adjusted_return"] or 0.0),
        },
        "action_counts": {
            "baseline": _count_actions(baseline_daily_rows),
            VARIANT_NAME: _count_actions(variant_daily_rows),
        },
        "notes": [
            "baseline is the long-only entry/cash control without the gate",
            "variant adds the long_entry_cash_gate_v1 entry gate only",
        ],
    }

    regime_split_summary = {
        "schema_version": "tradex_long_action_policy_foundation_v1",
        "family_id": FAMILY_ID,
        "regime_counts": {
            "baseline": dict(Counter(row["regime"] for window in baseline_windows for row in window["regime_split_rows"])),
            VARIANT_NAME: dict(Counter(row["regime"] for window in variant_windows for row in window["regime_split_rows"])),
        },
        "window_regime_counts": [
            {
                "window_id": window["window_id"],
                "window_label": window["window_label"],
                "scenario": "baseline",
                "regime_counts": dict(Counter(row["regime"] for row in window["regime_split_rows"])),
            }
            for window in baseline_windows
        ]
        + [
            {
                "window_id": window["window_id"],
                "window_label": window["window_label"],
                "scenario": VARIANT_NAME,
                "regime_counts": dict(Counter(row["regime"] for row in window["regime_split_rows"])),
            }
            for window in variant_windows
        ],
        "notes": ["regime labels are derived from the market context available at decision time"],
    }

    daily_action_ledger = {
        "schema_version": "tradex_long_action_policy_foundation_v1",
        "items": all_daily_rows,
    }
    policy_candidate_manifest = {
        "schema_version": "tradex_long_action_policy_foundation_v1",
        "family_id": FAMILY_ID,
        "baseline_name": BASELINE_NAME,
        "variant_name": VARIANT_NAME,
        "selected_candidate_family": "long_entry_cash_gate_v1",
        "diagnostic_only_families": [
            "rotation_opportunity_cost_diagnostic_v1",
            "hedge_pressure_diagnostic_v1",
        ],
        "allowed_actions": ["buy", "stay_cash", "hold"],
        "replay_continuity_actions": ["forced_exit"],
        "diagnostic_only_actions": ["would_rotate", "hedge_pressure", "regime_block", "timing_block", "liquidity_block", "cost_turnover_block"],
        "forbidden_actions": ["add", "reduce", "take_profit", "rotate", "hedge", "short"],
        "input_artifacts": [
            "artifacts/research_inventory/research_inventory.json",
            "artifacts/research_inventory/buy_judgment_effectiveness_audit.json",
            "artifacts/research_inventory/buy_judgment_revision_r1_weak_liquidity_gate.json",
            "artifacts/research_inventory/buy_judgment_revision_r2_regime_mismatch_gate.json",
            "artifacts/research_inventory/buy_judgment_policy_selection_r5_default_surface_choice.json",
            "G:/Tradex/research_sessions/tradex_daily_action_policy_engine_v1/20260430T063606Z/*.json",
        ],
        "reason_codes": ["entry_signal", "entry_threshold_not_met", "long_entries_disabled", "short_blocked", "regime_block", "cost_turnover_block", "timing_block"],
        "implementation_status": "bounded_long_entry_cash_gate_v1",
        "execution_convention": "next_session_open",
        "cost_model_enabled": True,
        "no_meemee_reflection_approved": False,
    }
    evaluation_contract = {
        "schema_version": "tradex_long_action_policy_foundation_v1",
        "family_id": FAMILY_ID,
        "initial_capital_jpy": 10_000_000,
        "execution_convention": "next_session_open",
        "execution_model": {
            "close_to_close_baseline": {"status": "supported"},
            "next_session_open": {"status": "supported"},
        },
        "cost_slippage_model": dict(EVALUATION_COST_MODEL),
        "no_lookahead_rule": {
            "decision_inputs_only_use_data_available_as_of_decision_date": True,
            "fill_price_may_use_next_session_open_only": True,
            "no_silent_fallback_to_close_to_close": True,
        },
        "universe_source": "StockRepository.get_latest_params_for_screening() with fallback to get_all_codes()",
        "universe_count": len(universe),
        "window_specs": window_specs,
        "window_contract": [
            {
                "window_id": spec["window_id"],
                "label": spec["label"],
                "decision_window_start_date": spec["window_start_date"],
                "decision_window_end_date": spec.get("decision_window_end_date") or spec["window_start_date"],
                "execution_buffer_days": int(spec.get("execution_buffer_days") or 1),
                "outcome_buffer_days": int(spec.get("outcome_buffer_days") or max(20, int(spec["window_months"]) * 21)),
                "decision_window": "decision_date only",
                "execution_data_window": "decision_date plus execution buffer",
                "outcome_data_window": "decision_date plus evaluation horizon",
            }
            for spec in window_specs
        ],
        "liquidity_filter": {
            "enabled": True,
            "liquidity20d_min": ENTRY_GATE_VARIANT["liquidity20d_min"],
            "turnover_z20_min": ENTRY_GATE_VARIANT["turnover_z20_min"],
            "notes": ["applies only to the gate variant"],
        },
        "metrics": {
            "primary": [
                "ending_equity",
                "net_return",
                "max_drawdown",
                "worst_month_return",
                "profit_factor",
                "turnover_adjusted_return",
                "monthly_positive_rate",
                "return_per_unit_drawdown",
                "average_holding_period",
                "exposure_utilization",
                "opportunity_cost_adjusted_return",
            ],
            "secondary": [
                "top5_ret_20",
                "top10_ret_20",
                "monthly_top5_capture",
                "recall@20",
                "win_rate",
                "mean_ret_20",
                "median_ret_20",
            ],
        },
        "pass_fail_rules": {
            "ret20_alone_not_sufficient": True,
            "must_use_next_session_open": True,
            "must_include_cost_slippage": True,
            "must_report_drawdown": True,
            "must_report_regime_split": True,
            "no_meemee_reflection_approved": True,
        },
    }
    risk_audit = {
        "schema_version": "tradex_long_action_policy_foundation_v1",
        "family_id": FAMILY_ID,
        "lookahead_risk": {
            "status": "controlled",
            "notes": ["decision inputs are as-of decision date; fills use next-session-open only"],
        },
        "overfitting_risk": {
            "status": "guarded",
            "notes": ["baseline plus one explainable gate variant; no threshold grid; three fixed windows"],
        },
        "cost_slippage_risk": {
            "status": "controlled",
            "notes": ["nonzero provisional cost model is enabled for both baseline and variant"],
        },
        "liquidity_risk": {
            "status": "controlled",
            "notes": ["gate variant includes liquidity20d and turnover_z20 blocks"],
        },
        "event_news_blindness": {
            "status": "unmodeled",
            "notes": ["this pass does not ingest event/news features"],
        },
        "concentration_risk": {
            "status": "partially_observed",
            "notes": ["portfolio-level replay is available, but the action model remains long-entry focused"],
        },
        "stale_or_empty_event_table": {
            "status": "not_applicable",
            "notes": ["no event table is used in this first pass"],
        },
        "zero_cost_artifact_warning": {
            "status": "avoided",
            "notes": ["cost/slippage are enabled; zero-cost-only comparison is not used"],
        },
    }
    final_decision = {
        "schema_version": "tradex_long_action_policy_foundation_v1",
        "final_status": "implementation_done",
        "reason": "first long-action policy-family pass executed under next-session-open with cost/slippage and diagnostic-only rotation/hedge outputs",
        "notes": ["no adoption decision is made in this implementation pass"],
    }

    session_id = _make_session_id()
    session_dir = output_root / session_id
    session_dir.mkdir(parents=True, exist_ok=False)

    artifacts = {
        "policy_candidate_manifest.json": policy_candidate_manifest,
        "evaluation_contract.json": evaluation_contract,
        "portfolio_replay_summary.json": portfolio_replay_summary,
        "daily_action_ledger.json": daily_action_ledger,
        "regime_split_summary.json": regime_split_summary,
        "cost_slippage_summary.json": {
            "schema_version": "tradex_long_action_policy_foundation_v1",
            "family_id": FAMILY_ID,
            "baseline": {
                "cost_model": dict(EVALUATION_COST_MODEL),
                "total_cost_amount": sum(float(row["cost_amount"] or 0.0) for row in baseline_daily_rows),
                "total_slippage_amount": sum(float(row["slippage_amount"] or 0.0) for row in baseline_daily_rows),
            },
            "variant": {
                "cost_model": dict(EVALUATION_COST_MODEL),
                "total_cost_amount": sum(float(row["cost_amount"] or 0.0) for row in variant_daily_rows),
                "total_slippage_amount": sum(float(row["slippage_amount"] or 0.0) for row in variant_daily_rows),
            },
            "notes": ["cost and slippage are enabled in both scenarios"],
        },
        "opportunity_cost_summary.json": opportunity_cost_summary,
        "hedge_pressure_summary.json": hedge_pressure_summary,
        "risk_audit.json": risk_audit,
        "final_decision.json": final_decision,
    }

    commands_run = [
        f"python {SCRIPT_NAME}.py --output-root {output_root}",
        "git status --short",
        "git diff --name-only",
    ]
    git_status_short = _safe_git_output(["git", "status", "--short"])
    git_diff_name_only = _safe_git_output(["git", "diff", "--name-only"])

    for filename, payload in artifacts.items():
        _write_json(session_dir / filename, payload)

    complete = {
        "schema_version": "tradex_long_action_policy_foundation_v1",
        "family_id": FAMILY_ID,
        "generated_at": _utc_now(),
        "session_id": session_id,
        "output_dir": str(session_dir),
        "artifact_list": list(artifacts.keys()) + ["_ARTIFACT_COMPLETE.json"],
        "verification_status": "generated",
        "commands_run": commands_run,
        "git_status_short": git_status_short,
        "git_diff_name_only": git_diff_name_only,
        "final_decision": final_decision["final_status"],
        "notes": [
            "TRADEX-only first bounded policy-family pass",
            "no MeeMee production surfaces were changed by this script",
        ],
    }
    _write_json(session_dir / "_ARTIFACT_COMPLETE.json", complete)
    return {
        "session_id": session_id,
        "output_dir": str(session_dir),
        "artifacts": {key: str(session_dir / key) for key in artifacts},
        "complete": str(session_dir / "_ARTIFACT_COMPLETE.json"),
    }


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="TRADEX long-action policy foundation runner")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT, help="Root output directory for the research session")
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH, help="Path to the stock repository DuckDB database")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    repo = _resolve_repo(args.db_path)
    result = build_long_action_policy_foundation(repo, args.output_root)
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
