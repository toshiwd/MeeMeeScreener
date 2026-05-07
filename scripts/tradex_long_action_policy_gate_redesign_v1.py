from __future__ import annotations

import argparse
import ast
import json
import subprocess
import sys
from collections import Counter, defaultdict
from collections.abc import Iterable
from datetime import date, datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from external_analysis.policy_replay.simulator import _parse_date
from scripts.tradex_long_action_policy_foundation_v1 import (
    ACTION_POLICY_MODE_BASELINE,
    ACTION_POLICY_MODE_VARIANT,
    BASELINE_NAME,
    DEFAULT_WEIGHTS,
    EVALUATION_COST_MODEL,
    FAMILY_ID,
    ENTRY_GATE_VARIANT,
    WINDOW_SPECS_DEFAULT,
    _aggregate_scenario,
    _build_diagnostics,
    _build_payload,
    _count_actions,
    _make_session_id,
    _resolve_repo,
    _resolve_universe,
    _run_scenario_window,
    normalize_replay_run_config,
    _write_json,
)

SCRIPT_NAME = "tradex_long_action_policy_gate_redesign_v1"
FAMILY_ID_REDESIGN = "long_action_policy_gate_redesign_v1"
CURRENT_GATE_NAME = "long_entry_cash_gate_v1"
REDESIGN_NAME = "long_entry_cash_gate_entry_signal_relax_v1"
REDESIGN_GATE = {
    **ENTRY_GATE_VARIANT,
    "timing_override_enabled": True,
    "timing_override_reason_codes": ["timing_block"],
    "timing_override_rank_max": 11,
    "timing_override_score_min": 0.05,
}
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\research_sessions\long_action_policy_foundation_v1_gate_redesign")
DEFAULT_EXPANDED_REVIEW_DIR = Path(
    r"G:\Tradex\research_sessions\long_action_policy_foundation_v1_effectiveness_review_expanded\20260430T201556Z-9d9865fd"
)
DEFAULT_FOUNDATION_REVIEW_DIR = Path(
    r"G:\Tradex\research_sessions\long_action_policy_foundation_v1_effectiveness_review\20260430T135939Z-83ef381c"
)
DEFAULT_SKIPPED_CASES_PATH = DEFAULT_EXPANDED_REVIEW_DIR / "skipped_buy_cases.parquet"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def _safe_git_output(args: list[str]) -> str:
    try:
        completed = subprocess.run(args, cwd=REPO_ROOT, capture_output=True, text=True, check=False)
        output = (completed.stdout or completed.stderr or "").strip()
        return output
    except Exception as exc:  # pragma: no cover - best effort metadata
        return f"unavailable: {exc}"


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _resolve_path(path: Path, fallback: Path | None = None) -> Path:
    if path.exists():
        return path
    if fallback is not None and fallback.exists():
        return fallback
    return path


def _build_entry_gate(mode: str, *, timing_override: bool = False) -> dict[str, Any]:
    gate = {
        "enabled": True,
        "market_ret20_min": ENTRY_GATE_VARIANT["market_ret20_min"],
        "breadth_above_ma20_max": ENTRY_GATE_VARIANT["breadth_above_ma20_max"],
        "liquidity20d_min": ENTRY_GATE_VARIANT["liquidity20d_min"],
        "turnover_z20_min": ENTRY_GATE_VARIANT["turnover_z20_min"],
        "diff20_pct_max": ENTRY_GATE_VARIANT["diff20_pct_max"],
        "candle_upper_wick_ratio_max": ENTRY_GATE_VARIANT["candle_upper_wick_ratio_max"],
    }
    if timing_override:
        gate.update(
            {
                "timing_override_enabled": True,
                "timing_override_reason_codes": ["timing_block"],
                "timing_override_rank_max": 11,
                "timing_override_score_min": 0.05,
            }
        )
    return gate


def _build_payload_for_family(
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


def _load_skipped_cases(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"missing skipped-buy analysis parquet: {path}")
    frame = pd.read_parquet(path)
    if frame.empty:
        return frame
    for column in ("date", "decision_date", "window_start_date", "window_end_date", "month_key", "week_key", "window_label", "window_id", "symbol", "skip_class", "reason_codes_key"):
        if column in frame.columns:
            frame[column] = frame[column].astype("string")
    return frame


def _safe_list(values: Any) -> list[str]:
    if values is None:
        return []
    if isinstance(values, list):
        return [str(item) for item in values if str(item)]
    if isinstance(values, tuple):
        return [str(item) for item in values if str(item)]
    if isinstance(values, Iterable) and not isinstance(values, (str, bytes)):
        return [str(item) for item in list(values) if str(item)]
    if isinstance(values, str):
        try:
            parsed = ast.literal_eval(values)
        except Exception:
            parsed = None
        if isinstance(parsed, (list, tuple)):
            return [str(item) for item in parsed if str(item)]
        return [item.strip() for item in values.split(",") if item.strip()]
    return [str(values)]


def _feature_availability(frame: pd.DataFrame) -> dict[str, Any]:
    columns = list(frame.columns)
    forbidden_outcome = [
        "ret_5",
        "ret_10",
        "ret_20",
        "forward_ret_20d",
        "path_value_score_v1",
        "later_buy_forward_ret_20d",
        "later_buy_delay_cost_20d",
        "later_buy_delay_days",
    ]
    return {
        "schema_version": "tradex_long_action_policy_gate_redesign_v1",
        "source_parquet": str(DEFAULT_SKIPPED_CASES_PATH),
        "available_columns": columns,
        "action_fields": [column for column in ["baseline_action", "variant_action", "baseline_order_status", "variant_order_status"] if column in columns],
        "baseline_action_fields": [column for column in ["baseline_action", "baseline_order_status", "baseline_reason_codes", "baseline_position_qty", "baseline_position_value", "baseline_cash"] if column in columns],
        "variant_action_fields": [column for column in ["variant_action", "variant_order_status", "variant_reason_codes", "variant_position_qty", "variant_position_value", "variant_cash"] if column in columns],
        "reason_code_fields": [column for column in ["baseline_reason_codes", "variant_reason_codes", "reason_codes_key"] if column in columns],
        "rank_fields": [column for column in ["baseline_rank", "baseline_score", "top_candidate_score"] if column in columns],
        "regime_fields": [column for column in ["market_regime"] if column in columns],
        "date_month_window_fields": [column for column in ["date", "decision_date", "window_id", "window_label", "window_start_date", "window_end_date", "month_key", "week_key"] if column in columns],
        "symbol_fields": [column for column in ["symbol"] if column in columns],
        "entry_signal_fields": [column for column in ["baseline_score", "top_candidate_score", "reason_codes_key"] if column in columns],
        "position_state_fields": [column for column in ["baseline_position_qty", "variant_position_qty", "baseline_position_value", "variant_position_value", "baseline_cash", "variant_cash"] if column in columns],
        "no_lookahead_safe_fields": [column for column in ["date", "decision_date", "window_id", "window_label", "window_start_date", "window_end_date", "symbol", "baseline_rank", "baseline_score", "top_candidate_score", "reason_codes_key", "variant_reason_codes"] if column in columns],
        "forbidden_outcome_fields": [column for column in forbidden_outcome if column in columns],
        "notes": [
            "same-day rank and score are no-lookahead safe because they are formed from the decision-date snapshot",
            "outcome columns are excluded from the redesign rule",
        ],
    }


def _policy_spec() -> dict[str, Any]:
    return {
        "schema_version": "tradex_long_action_policy_gate_redesign_v1",
        "family_id": FAMILY_ID_REDESIGN,
        "baseline_name": BASELINE_NAME,
        "current_gate_name": CURRENT_GATE_NAME,
        "variant_name": REDESIGN_NAME,
        "rule_name": "long_entry_cash_gate_entry_signal_relax_v1",
        "rule": {
            "mode": "narrow_timing_relax_only",
            "restore_when": [
                "baseline_action == buy",
                "current_gate_action in {stay_cash, hold}",
                "current_gate_reason_codes == [timing_block]",
                "baseline_rank <= 11",
                "baseline_score >= 0.05",
            ],
            "preserve_blockers": [
                "regime_block",
                "cost_turnover_block",
            ],
            "no_lookahead_safe_inputs": [
                "baseline_action",
                "variant_action",
                "baseline_rank",
                "baseline_score",
                "reason_codes_key",
                "variant_reason_codes",
                "date",
                "decision_date",
                "window_id",
                "window_label",
                "symbol",
            ],
            "excluded_inputs": [
                "ret_5",
                "ret_10",
                "ret_20",
                "forward_ret_20d",
                "path_value_score_v1",
                "later_buy_forward_ret_20d",
                "later_buy_delay_cost_20d",
                "later_buy_delay_days",
            ],
            "overfit_guardrail": "the rule is fixed across all windows and does not condition on month, symbol, or regime pocket; it only relaxes the timing block for same-day high-confidence entries",
        },
        "timing_override": {
            "enabled": True,
            "reason_codes": ["timing_block"],
            "rank_max": 11,
            "score_min": 0.05,
        },
        "why_safe": [
            "candidate_rank and baseline_score are derived from same-day score map information available at decision time",
            "the override is only applied when the gate would otherwise block on timing alone",
            "future-return columns are not consulted in the rule",
        ],
        "why_narrower_than_current_gate": [
            "the current gate can block timing, regime, and liquidity/turnover combinations",
            "the redesign only relaxes the timing block when same-day signal strength is high enough",
        ],
    }


def _scenario_payload(
    *,
    universe: list[str],
    window_spec: dict[str, Any],
    policy_version: str,
    action_policy: dict[str, Any],
) -> dict[str, Any]:
    return _build_payload_for_family(
        universe=universe,
        window_start_date=str(window_spec["window_start_date"]),
        window_months=int(window_spec["window_months"]),
        policy_version=policy_version,
        action_policy=action_policy,
    )


def _run_family_windows(repo: Any, universe: list[str], window_specs: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    baseline_windows: list[dict[str, Any]] = []
    current_windows: list[dict[str, Any]] = []
    redesign_windows: list[dict[str, Any]] = []

    baseline_policy = {
        "schema_version": "tradex_daily_action_policy_v1",
        "mode": ACTION_POLICY_MODE_BASELINE,
        "enabled": True,
        "allow_long_entries": True,
        "allow_short_entries": False,
        "allow_position_management": False,
        "allow_weekly_activity": False,
        "entry_gate": {"enabled": False},
        "notes": ["baseline long-only entry/cash control without the gate"],
    }
    current_policy = {
        "schema_version": "tradex_daily_action_policy_v1",
        "mode": ACTION_POLICY_MODE_VARIANT,
        "enabled": True,
        "allow_long_entries": True,
        "allow_short_entries": False,
        "allow_position_management": False,
        "allow_weekly_activity": False,
        "entry_gate": dict(ENTRY_GATE_VARIANT),
        "notes": ["current gate under review"],
    }
    redesign_policy = {
        "schema_version": "tradex_daily_action_policy_v1",
        "mode": "long_entry_cash_gate_entry_signal_relax_v1",
        "enabled": True,
        "allow_long_entries": True,
        "allow_short_entries": False,
        "allow_position_management": False,
        "allow_weekly_activity": False,
        "entry_gate": dict(REDESIGN_GATE),
        "notes": ["narrow timing relaxer for strong same-day entry signal"],
    }

    baseline_run_config = None
    current_run_config = None
    redesign_run_config = None

    for idx, spec in enumerate(window_specs):
        if baseline_run_config is None:
            baseline_run_config = normalize_replay_run_config(
                _scenario_payload(universe=universe, window_spec=spec, policy_version=BASELINE_NAME, action_policy=baseline_policy)
            )
            current_run_config = normalize_replay_run_config(
                _scenario_payload(universe=universe, window_spec=spec, policy_version=CURRENT_GATE_NAME, action_policy=current_policy)
            )
            redesign_run_config = normalize_replay_run_config(
                _scenario_payload(universe=universe, window_spec=spec, policy_version=REDESIGN_NAME, action_policy=redesign_policy)
            )

        assert baseline_run_config is not None and current_run_config is not None and redesign_run_config is not None
        for payload in (baseline_run_config, current_run_config, redesign_run_config):
            payload["window_start_date"] = str(spec["window_start_date"])
            payload["window_months"] = int(spec["window_months"])
            payload["window_start_dates"] = [str(spec["window_start_date"])]
            payload["decision_window_start_date"] = str(spec["window_start_date"])
            payload["decision_window_end_date"] = str(spec.get("decision_window_end_date") or spec["window_start_date"])
            payload["execution_buffer_days"] = int(spec.get("execution_buffer_days") or 1)
            payload["outcome_buffer_days"] = int(spec.get("outcome_buffer_days") or max(20, int(spec["window_months"]) * 21))

        baseline_windows.append(_run_scenario_window(repo=repo, universe=universe, window_spec=spec, run_config=dict(baseline_run_config), scenario_name="baseline"))
        current_windows.append(_run_scenario_window(repo=repo, universe=universe, window_spec=spec, run_config=dict(current_run_config), scenario_name=CURRENT_GATE_NAME))
        redesign_windows.append(_run_scenario_window(repo=repo, universe=universe, window_spec=spec, run_config=dict(redesign_run_config), scenario_name=REDESIGN_NAME))

    return {
        "baseline": baseline_windows,
        CURRENT_GATE_NAME: current_windows,
        REDESIGN_NAME: redesign_windows,
    }


def _equity_series(window: dict[str, Any]) -> list[dict[str, Any]]:
    return list(window.get("daily_equity_curve") or [])


def _aggregate_family(windows: list[dict[str, Any]]) -> dict[str, Any]:
    aggregate = _aggregate_scenario(windows)
    window_returns = [float(window["window_summary"]["portfolio_return_3m"]) for window in windows]
    daily_pnl_values = [float(row.get("daily_pnl") or 0.0) for window in windows for row in window["portfolio_daily_action_ledger"]]
    gross_exposures = [float(row.get("gross_exposure") or 0.0) for window in windows for row in window["portfolio_daily_action_ledger"]]
    net_exposures = [float(row.get("net_exposure") or 0.0) for window in windows for row in window["portfolio_daily_action_ledger"]]
    drawdown_values = [float(row.get("drawdown") or 0.0) for window in windows for row in window["daily_equity_curve"] if row.get("drawdown") is not None]
    drawdown_runs: list[int] = []
    current_run = 0
    for window in windows:
        for row in window["daily_equity_curve"]:
            drawdown = float(row.get("drawdown") or 0.0)
            if drawdown < 0:
                current_run += 1
            elif current_run:
                drawdown_runs.append(current_run)
                current_run = 0
        if current_run:
            drawdown_runs.append(current_run)
            current_run = 0
    total_buys = sum(1 for window in windows for row in window["portfolio_daily_action_ledger"] if row.get("action") == "buy")
    filled_buys = sum(1 for window in windows for row in window["portfolio_daily_action_ledger"] if row.get("action") == "buy" and row.get("order_status") == "filled")
    skipped_buys = sum(1 for window in windows for row in window["portfolio_daily_action_ledger"] if row.get("action") == "stay_cash" and row.get("side") == "cash")
    total_cost = sum(float(row.get("cost_amount") or 0.0) for window in windows for row in window["portfolio_daily_action_ledger"])
    total_slippage = sum(float(row.get("slippage_amount") or 0.0) for window in windows for row in window["portfolio_daily_action_ledger"])
    return {
        **aggregate,
        "filled_buys": filled_buys,
        "buy_attempts": total_buys,
        "skipped_buy_actions": skipped_buys,
        "total_cost_amount": total_cost,
        "total_slippage_amount": total_slippage,
        "net_return_median": median(window_returns) if window_returns else None,
        "gross_exposure_mean": mean(gross_exposures) if gross_exposures else None,
        "net_exposure_mean": mean(net_exposures) if net_exposures else None,
        "daily_pnl_distribution": {
            "count": len(daily_pnl_values),
            "mean": mean(daily_pnl_values) if daily_pnl_values else None,
            "median": median(daily_pnl_values) if daily_pnl_values else None,
            "min": min(daily_pnl_values) if daily_pnl_values else None,
            "max": max(daily_pnl_values) if daily_pnl_values else None,
            "win_rate": (sum(1 for value in daily_pnl_values if value > 0.0) / len(daily_pnl_values)) if daily_pnl_values else None,
        },
        "drawdown_duration_days": {
            "count": len(drawdown_runs),
            "max_duration_days": max(drawdown_runs) if drawdown_runs else 0,
            "mean_duration_days": (sum(drawdown_runs) / len(drawdown_runs)) if drawdown_runs else 0.0,
        },
        "action_counts": _count_actions([row for window in windows for row in window["portfolio_daily_action_ledger"]]),
    }


def _aligned_rows(scenarios: dict[str, list[dict[str, Any]]]) -> dict[tuple[str, str, str], dict[str, dict[str, Any]]]:
    paired: dict[tuple[str, str, str], dict[str, dict[str, Any]]] = {}
    for scenario_name, windows in scenarios.items():
        for window in windows:
            for row in window["portfolio_daily_action_ledger"]:
                key = (str(row.get("window_id")), str(row.get("date")), str(row.get("symbol")))
                paired.setdefault(key, {})[scenario_name] = row
    return paired


def _action_transition_matrix(rows_a: list[dict[str, Any]], rows_b: list[dict[str, Any]]) -> dict[str, int]:
    matrix: Counter[str] = Counter()
    lookup_b = {(row.get("window_id"), row.get("date"), row.get("symbol")): row for row in rows_b}
    for row in rows_a:
        key = (row.get("window_id"), row.get("date"), row.get("symbol"))
        other = lookup_b.get(key)
        if other is None:
            continue
        matrix[f'{row.get("action")}->{other.get("action")}'] += 1
    return dict(sorted(matrix.items()))


def _branch_audit(scenarios: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    baseline_rows = [row for window in scenarios["baseline"] for row in window["portfolio_daily_action_ledger"]]
    current_rows = [row for window in scenarios[CURRENT_GATE_NAME] for row in window["portfolio_daily_action_ledger"]]
    redesign_rows = [row for window in scenarios[REDESIGN_NAME] for row in window["portfolio_daily_action_ledger"]]
    paired = _aligned_rows(scenarios)

    def _diff_counts(left_name: str, right_name: str) -> dict[str, int]:
        counts = Counter()
        for pair in paired.values():
            left = pair.get(left_name)
            right = pair.get(right_name)
            if left is None or right is None:
                continue
            for key in ("action", "order_status", "cash", "gross_exposure", "net_exposure", "position_qty", "position_value", "realized_pnl", "unrealized_pnl", "daily_pnl", "cumulative_pnl", "drawdown", "cost_amount", "slippage_amount", "execution_price"):
                if str(left.get(key)) != str(right.get(key)):
                    counts[key] += 1
        return dict(sorted(counts.items()))

    baseline_vs_current = _diff_counts("baseline", CURRENT_GATE_NAME)
    current_vs_redesign = _diff_counts(CURRENT_GATE_NAME, REDESIGN_NAME)
    baseline_vs_redesign = _diff_counts("baseline", REDESIGN_NAME)
    return {
        "schema_version": "tradex_long_action_policy_gate_redesign_v1",
        "baseline_vs_current": {
            "row_count": len(baseline_rows),
            "diff_counts": baseline_vs_current,
            "transition_counts": _action_transition_matrix(baseline_rows, current_rows),
        },
        "current_vs_redesign": {
            "row_count": len(current_rows),
            "diff_counts": current_vs_redesign,
            "transition_counts": _action_transition_matrix(current_rows, redesign_rows),
        },
        "baseline_vs_redesign": {
            "row_count": len(baseline_rows),
            "diff_counts": baseline_vs_redesign,
            "transition_counts": _action_transition_matrix(baseline_rows, redesign_rows),
        },
        "branch_effect_present": bool(baseline_vs_current or current_vs_redesign or baseline_vs_redesign),
        "notes": ["branch effect is measured on aligned scenario rows at the portfolio action ledger level"],
    }


def _scenario_monthly_summary(scenarios: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for scenario_name, windows in scenarios.items():
        for window in windows:
            month_key = str(window["window_start_date"])[:7]
            rows.append(
                {
                    "scenario": scenario_name,
                    "window_id": window["window_id"],
                    "window_label": window["window_label"],
                    "month_key": month_key,
                    "net_return": float(window["window_summary"]["portfolio_return_3m"]),
                    "turnover": float(window["window_summary"]["turnover"]),
                    "max_drawdown": float(window["window_summary"]["max_drawdown"]),
                    "filled_buys": sum(1 for row in window["portfolio_daily_action_ledger"] if row.get("action") == "buy" and row.get("order_status") == "filled"),
                    "skipped_buys": sum(1 for row in window["portfolio_daily_action_ledger"] if row.get("action") == "stay_cash" and row.get("side") == "cash"),
                }
            )
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[row["month_key"]].append(row)
    return {
        "schema_version": "tradex_long_action_policy_gate_redesign_v1",
        "rows": rows,
        "by_month": {
            month: {
                "scenario_count": len(items),
                "net_return_mean": mean(item["net_return"] for item in items),
                "turnover_mean": mean(item["turnover"] for item in items),
                "max_drawdown_worst": min(item["max_drawdown"] for item in items),
            }
            for month, items in sorted(grouped.items())
        },
    }


def _scenario_regime_summary(scenarios: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for scenario_name, windows in scenarios.items():
        for window in windows:
            regime_counts = Counter(row["regime"] for row in window.get("regime_split_rows") or [])
            rows.append(
                {
                    "scenario": scenario_name,
                    "window_id": window["window_id"],
                    "window_label": window["window_label"],
                    "regime_counts": dict(regime_counts),
                }
            )
    return {
        "schema_version": "tradex_long_action_policy_gate_redesign_v1",
        "rows": rows,
        "by_scenario": {
            scenario_name: {
                "window_count": len(windows),
                "regime_counts": dict(Counter(row["regime"] for window in windows for row in window.get("regime_split_rows") or [])),
            }
            for scenario_name, windows in scenarios.items()
        },
    }


def _drawdown_attribution(scenarios: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    paired = _aligned_rows(scenarios)
    worst_rows: list[dict[str, Any]] = []
    for key, pair in paired.items():
        baseline = pair.get("baseline")
        redesign = pair.get(REDESIGN_NAME)
        if baseline is None or redesign is None:
            continue
        b_dd = float(baseline.get("drawdown") or 0.0)
        r_dd = float(redesign.get("drawdown") or 0.0)
        if r_dd < b_dd:
            worst_rows.append(
                {
                    "window_id": key[0],
                    "date": key[1],
                    "symbol": key[2],
                    "baseline_drawdown": b_dd,
                    "redesign_drawdown": r_dd,
                    "drawdown_gap": r_dd - b_dd,
                    "baseline_action": baseline.get("action"),
                    "redesign_action": redesign.get("action"),
                    "baseline_order_status": baseline.get("order_status"),
                    "redesign_order_status": redesign.get("order_status"),
                }
            )
    top_symbols = Counter(row["symbol"] for row in worst_rows)
    by_reason = Counter()
    for row in worst_rows:
        pair = paired[(row["window_id"], row["date"], row["symbol"])]
        current = pair.get(CURRENT_GATE_NAME) or {}
        reasons = current.get("reason_codes") or []
        for reason in reasons:
            by_reason[str(reason)] += 1
    return {
        "schema_version": "tradex_long_action_policy_gate_redesign_v1",
        "worse_drawdown_row_count": len(worst_rows),
        "worse_drawdown_windows": sorted({(row["window_id"], row["date"][:7]) for row in worst_rows}),
        "top_symbols": dict(top_symbols.most_common(10)),
        "cause_breakdown": dict(sorted(by_reason.items())),
        "notes": ["drawdown attribution compares baseline to redesign on aligned ledger rows"],
    }


def _restoration_summary(cases: pd.DataFrame) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    if cases.empty:
        empty = {
            "schema_version": "tradex_long_action_policy_gate_redesign_v1",
            "total_skipped_buy": 0,
            "restored_good_buy": 0,
            "restored_bad_buy": 0,
            "remaining_skipped_good_buy": 0,
            "remaining_skipped_bad_buy": 0,
            "false_negative_skip_reduction": 0.0,
            "true_positive_skip_retention": 0.0,
            "entry_delay_cost_mean": None,
            "entry_delay_cost_median": None,
        }
        return empty, cases, cases

    def _restore_mask(frame: pd.DataFrame) -> pd.Series:
        return (
            (frame["baseline_action"] == "buy")
            & (frame["variant_action"].isin(["stay_cash", "hold"]))
            & (frame["reason_codes_key"] == "entry_signal")
            & (frame["variant_reason_codes"].map(lambda values: _safe_list(values) == ["timing_block"]))
            & (frame["baseline_rank"].fillna(10_000).astype(float) <= 11)
            & (frame["baseline_score"].fillna(0.0).astype(float) >= 0.05)
        )

    skipped = cases.loc[(cases["baseline_action"] == "buy") & (cases["variant_action"].isin(["stay_cash", "hold"]))].copy()
    restored = skipped.loc[_restore_mask(skipped)].copy()
    remaining = skipped.loc[~_restore_mask(skipped)].copy()
    restored_good = restored.loc[restored["skip_class"] == "skipped_good_buy"].copy()
    restored_bad = restored.loc[restored["skip_class"] == "skipped_bad_buy"].copy()
    remaining_good = remaining.loc[remaining["skip_class"] == "skipped_good_buy"].copy()
    remaining_bad = remaining.loc[remaining["skip_class"] == "skipped_bad_buy"].copy()
    delay_cost_series = restored["later_buy_delay_cost_20d"].dropna().astype(float) if "later_buy_delay_cost_20d" in restored.columns else pd.Series(dtype=float)

    summary = {
        "schema_version": "tradex_long_action_policy_gate_redesign_v1",
        "total_skipped_buy": int(len(skipped)),
        "restored_buy_count": int(len(restored)),
        "restored_good_buy": int(len(restored_good)),
        "restored_bad_buy": int(len(restored_bad)),
        "remaining_skipped_buy_count": int(len(remaining)),
        "remaining_skipped_good_buy": int(len(remaining_good)),
        "remaining_skipped_bad_buy": int(len(remaining_bad)),
        "restored_good_ret_20_mean": float(restored_good["ret_20"].mean()) if not restored_good.empty else None,
        "restored_bad_ret_20_mean": float(restored_bad["ret_20"].mean()) if not restored_bad.empty else None,
        "remaining_skipped_good_ret_20_mean": float(remaining_good["ret_20"].mean()) if not remaining_good.empty else None,
        "remaining_skipped_bad_ret_20_mean": float(remaining_bad["ret_20"].mean()) if not remaining_bad.empty else None,
        "false_negative_skip_reduction": (len(restored_good) / len(skipped.loc[skipped["skip_class"] == "skipped_good_buy"])) if len(skipped.loc[skipped["skip_class"] == "skipped_good_buy"]) else 0.0,
        "true_positive_skip_retention": (len(remaining_bad) / len(skipped.loc[skipped["skip_class"] == "skipped_bad_buy"])) if len(skipped.loc[skipped["skip_class"] == "skipped_bad_buy"]) else 0.0,
        "entry_delay_cost_mean": float(delay_cost_series.mean()) if not delay_cost_series.empty else None,
        "entry_delay_cost_median": float(delay_cost_series.median()) if not delay_cost_series.empty else None,
        "by_reason_code": {
            key: {
                "count": int(len(group)),
                "restored_good_buy": int((group["skip_class"] == "skipped_good_buy").sum()),
                "restored_bad_buy": int((group["skip_class"] == "skipped_bad_buy").sum()),
            }
            for key, group in restored.groupby("reason_codes_key", dropna=False)
        },
        "notes": [
            "restoration uses same-day rank and score only; no future-return field is used in the rule",
            "skipped good/bad labels are evaluation-only outcome tags from the prior review artifact",
        ],
    }
    return summary, restored, remaining


def _portfolio_comparison(scenarios: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    comparisons = {}
    aggregates = {}
    for scenario_name, windows in scenarios.items():
        aggregates[scenario_name] = _aggregate_family(windows)
    baseline = aggregates["baseline"]
    current = aggregates[CURRENT_GATE_NAME]
    redesign = aggregates[REDESIGN_NAME]
    comparisons["baseline_vs_current"] = {
        "net_return_mean": float(current["net_return_mean"] or 0.0) - float(baseline["net_return_mean"] or 0.0),
        "turnover_adjusted_return": float(current["turnover_adjusted_return"] or 0.0) - float(baseline["turnover_adjusted_return"] or 0.0),
        "opportunity_cost_adjusted_return": float(current["opportunity_cost_adjusted_return"] or 0.0) - float(baseline["opportunity_cost_adjusted_return"] or 0.0),
        "max_drawdown_worst": float(current["max_drawdown_worst"] or 0.0) - float(baseline["max_drawdown_worst"] or 0.0),
    }
    comparisons["baseline_vs_redesign"] = {
        "net_return_mean": float(redesign["net_return_mean"] or 0.0) - float(baseline["net_return_mean"] or 0.0),
        "turnover_adjusted_return": float(redesign["turnover_adjusted_return"] or 0.0) - float(baseline["turnover_adjusted_return"] or 0.0),
        "opportunity_cost_adjusted_return": float(redesign["opportunity_cost_adjusted_return"] or 0.0) - float(baseline["opportunity_cost_adjusted_return"] or 0.0),
        "max_drawdown_worst": float(redesign["max_drawdown_worst"] or 0.0) - float(baseline["max_drawdown_worst"] or 0.0),
    }
    comparisons["current_vs_redesign"] = {
        "net_return_mean": float(redesign["net_return_mean"] or 0.0) - float(current["net_return_mean"] or 0.0),
        "turnover_adjusted_return": float(redesign["turnover_adjusted_return"] or 0.0) - float(current["turnover_adjusted_return"] or 0.0),
        "opportunity_cost_adjusted_return": float(redesign["opportunity_cost_adjusted_return"] or 0.0) - float(current["opportunity_cost_adjusted_return"] or 0.0),
        "max_drawdown_worst": float(redesign["max_drawdown_worst"] or 0.0) - float(current["max_drawdown_worst"] or 0.0),
    }
    return {
        "schema_version": "tradex_long_action_policy_gate_redesign_v1",
        "aggregates": aggregates,
        "pairwise_delta": comparisons,
    }


def _decision(payload: dict[str, Any], restoration: dict[str, Any], comparison: dict[str, Any]) -> dict[str, Any]:
    baseline = comparison["aggregates"]["baseline"]
    current = comparison["aggregates"][CURRENT_GATE_NAME]
    redesign = comparison["aggregates"][REDESIGN_NAME]
    net_delta_vs_current = float(redesign["net_return_mean"] or 0.0) - float(current["net_return_mean"] or 0.0)
    turnover_delta_vs_current = float(redesign["turnover_adjusted_return"] or 0.0) - float(current["turnover_adjusted_return"] or 0.0)
    dd_delta_vs_current = float(redesign["max_drawdown_worst"] or 0.0) - float(current["max_drawdown_worst"] or 0.0)
    good_restoration = int(restoration["restored_good_buy"])
    bad_restoration = int(restoration["restored_bad_buy"])
    if net_delta_vs_current > 0 and turnover_delta_vs_current > 0 and dd_delta_vs_current >= -0.01 and good_restoration >= bad_restoration:
        decision = "keep_for_larger_window_validation"
        reason = "narrow timing relaxer improves return and turnover-adjusted return while preserving more good skips than bad restores"
    elif good_restoration > 0 and bad_restoration <= good_restoration and dd_delta_vs_current > -0.05:
        decision = "hold_needs_larger_window_validation"
        reason = "timing relaxer restores good skipped buys but the sample is still too small for adoption"
    elif bad_restoration > good_restoration or dd_delta_vs_current < -0.05:
        decision = "drop"
        reason = "timing relaxer restores too many bad skips or materially worsens drawdown"
    else:
        decision = "needs_second_redesign"
        reason = "the timing relaxer addresses part of the false-negative issue but a narrower refinement is still needed"
    return {
        "schema_version": "tradex_long_action_policy_gate_redesign_v1",
        "final_status": decision,
        "reason": reason,
        "decision_inputs": {
            "baseline_net_return_mean": baseline["net_return_mean"],
            "current_net_return_mean": current["net_return_mean"],
            "redesign_net_return_mean": redesign["net_return_mean"],
            "baseline_turnover_adjusted_return": baseline["turnover_adjusted_return"],
            "current_turnover_adjusted_return": current["turnover_adjusted_return"],
            "redesign_turnover_adjusted_return": redesign["turnover_adjusted_return"],
            "baseline_max_drawdown_worst": baseline["max_drawdown_worst"],
            "current_max_drawdown_worst": current["max_drawdown_worst"],
            "redesign_max_drawdown_worst": redesign["max_drawdown_worst"],
            "restored_good_buy": good_restoration,
            "restored_bad_buy": bad_restoration,
        },
        "notes": ["adoption is not approved here; this file only decides whether to continue redesign validation"],
    }


def build_gate_redesign_review(
    repo: Any,
    output_root: Path,
    *,
    expanded_review_dir: Path = DEFAULT_EXPANDED_REVIEW_DIR,
    foundation_review_dir: Path = DEFAULT_FOUNDATION_REVIEW_DIR,
    skipped_cases_path: Path = DEFAULT_SKIPPED_CASES_PATH,
    window_specs: list[dict[str, Any]] | None = None,
    jobs: int = 1,
) -> dict[str, Any]:
    window_specs = list(window_specs or WINDOW_SPECS_DEFAULT)
    universe = _resolve_universe(repo)
    if not universe:
        raise ValueError("missing candidate universe from stock repository")
    if "1306" not in universe:
        universe = ["1306", *[code for code in universe if code != "1306"]]

    expanded_review_dir = _resolve_path(expanded_review_dir, fallback=expanded_review_dir)
    foundation_review_dir = _resolve_path(foundation_review_dir, fallback=foundation_review_dir)
    skipped_cases_path = _resolve_path(skipped_cases_path, fallback=skipped_cases_path)
    skipped_cases = _load_skipped_cases(skipped_cases_path)

    scenarios = _run_family_windows(repo, universe, window_specs)
    baseline_windows = scenarios["baseline"]
    current_windows = scenarios[CURRENT_GATE_NAME]
    redesign_windows = scenarios[REDESIGN_NAME]

    branch_effect_audit = _branch_audit(scenarios)
    portfolio_comparison = _portfolio_comparison(scenarios)
    restoration_summary, restored_cases, remaining_cases = _restoration_summary(skipped_cases)
    monthly_summary = _scenario_monthly_summary(scenarios)
    regime_summary = _scenario_regime_summary(scenarios)
    drawdown_summary = _drawdown_attribution(scenarios)

    comparison = portfolio_comparison
    decision = _decision(
        {
            "expanded_review_dir": str(expanded_review_dir),
            "foundation_review_dir": str(foundation_review_dir),
            "skipped_cases_path": str(skipped_cases_path),
        },
        restoration_summary,
        comparison,
    )

    feature_availability = _feature_availability(skipped_cases)
    policy_spec = _policy_spec()

    scenario_rows = [
        {**row, "scenario": "baseline", "window_id": window["window_id"], "window_label": window["window_label"]}
        for window in baseline_windows
        for row in window["portfolio_daily_action_ledger"]
    ] + [
        {**row, "scenario": CURRENT_GATE_NAME, "window_id": window["window_id"], "window_label": window["window_label"]}
        for window in current_windows
        for row in window["portfolio_daily_action_ledger"]
    ] + [
        {**row, "scenario": REDESIGN_NAME, "window_id": window["window_id"], "window_label": window["window_label"]}
        for window in redesign_windows
        for row in window["portfolio_daily_action_ledger"]
    ]

    run_manifest = {
        "schema_version": "tradex_long_action_policy_gate_redesign_v1",
        "family_id": FAMILY_ID_REDESIGN,
        "baseline_name": BASELINE_NAME,
        "current_gate_name": CURRENT_GATE_NAME,
        "variant_name": REDESIGN_NAME,
        "window_count": len(window_specs),
        "window_specs": window_specs,
        "expanded_review_dir": str(expanded_review_dir),
        "foundation_review_dir": str(foundation_review_dir),
        "skipped_cases_path": str(skipped_cases_path),
        "jobs_requested": int(jobs),
        "jobs_supported": 1,
        "research_fallback": True,
        "notes": [
            "actual comparison runs on the fixed foundation windows",
            "expanded review inputs are used for feature availability and redesign spec only",
            "parallel jobs are not enabled in this runner; jobs are recorded explicitly",
        ],
    }

    input_resolution = {
        "schema_version": "tradex_long_action_policy_gate_redesign_v1",
        "expanded_review_dir": str(expanded_review_dir),
        "foundation_review_dir": str(foundation_review_dir),
        "skipped_cases_path": str(skipped_cases_path),
        "expanded_review_found": expanded_review_dir.exists(),
        "foundation_review_found": foundation_review_dir.exists(),
        "skipped_cases_found": skipped_cases_path.exists(),
        "window_source": "WINDOW_SPECS_DEFAULT from long_action_policy_foundation_v1",
        "analysis_source": "expanded review skipped_buy_cases.parquet",
        "comparison_mode": "same fixed foundation windows with expanded-review-assisted redesign",
        "notes": ["no silent fallback; missing paths are surfaced explicitly"],
    }

    cost_slippage_summary = {
        "schema_version": "tradex_long_action_policy_gate_redesign_v1",
        "cost_model": dict(EVALUATION_COST_MODEL),
        "baseline_total_cost_amount": sum(float(row.get("cost_amount") or 0.0) for window in baseline_windows for row in window["portfolio_daily_action_ledger"]),
        "baseline_total_slippage_amount": sum(float(row.get("slippage_amount") or 0.0) for window in baseline_windows for row in window["portfolio_daily_action_ledger"]),
        "current_total_cost_amount": sum(float(row.get("cost_amount") or 0.0) for window in current_windows for row in window["portfolio_daily_action_ledger"]),
        "current_total_slippage_amount": sum(float(row.get("slippage_amount") or 0.0) for window in current_windows for row in window["portfolio_daily_action_ledger"]),
        "redesign_total_cost_amount": sum(float(row.get("cost_amount") or 0.0) for window in redesign_windows for row in window["portfolio_daily_action_ledger"]),
        "redesign_total_slippage_amount": sum(float(row.get("slippage_amount") or 0.0) for window in redesign_windows for row in window["portfolio_daily_action_ledger"]),
        "notes": ["cost/slippage remains enabled in all scenarios"],
    }

    opportunity_cost_summary = {
        "schema_version": "tradex_long_action_policy_gate_redesign_v1",
        "would_rotate_count": 0,
        "stronger_candidate_count": 0,
        "no_executed_rotation": True,
        "notes": ["rotation remains diagnostic-only and unchanged"],
    }
    hedge_pressure_summary = {
        "schema_version": "tradex_long_action_policy_gate_redesign_v1",
        "hedge_pressure_count": 0,
        "regime_deterioration_flags": 0,
        "no_executed_hedge": True,
        "notes": ["hedge remains diagnostic-only and unchanged"],
    }

    artifact_dir = output_root / _make_session_id()
    artifact_dir.mkdir(parents=True, exist_ok=False)

    artifact_payloads = {
        "run_manifest.json": run_manifest,
        "input_resolution.json": input_resolution,
        "gate_redesign_feature_availability.json": feature_availability,
        "gate_redesign_policy_spec.json": policy_spec,
        "branch_effect_audit.json": branch_effect_audit,
        "portfolio_economic_comparison.json": portfolio_comparison,
        "skipped_buy_restoration_summary.json": restoration_summary,
        "entry_delay_cost_summary.json": {
            "schema_version": "tradex_long_action_policy_gate_redesign_v1",
            "entry_delay_cost_mean": restoration_summary.get("entry_delay_cost_mean"),
            "entry_delay_cost_median": restoration_summary.get("entry_delay_cost_median"),
            "notes": ["computed from expanded skipped-buy cases using later_buy_delay_cost_20d as an evaluation proxy"],
        },
        "monthly_effectiveness_summary.json": monthly_summary,
        "regime_effectiveness_summary.json": regime_summary,
        "drawdown_attribution_summary.json": drawdown_summary,
        "cost_slippage_summary.json": cost_slippage_summary,
        "opportunity_cost_summary.json": opportunity_cost_summary,
        "hedge_pressure_summary.json": hedge_pressure_summary,
        "gate_redesign_decision.json": decision,
    }

    _write_json(artifact_dir / "run_manifest.json", run_manifest)
    _write_json(artifact_dir / "input_resolution.json", input_resolution)
    _write_json(artifact_dir / "gate_redesign_feature_availability.json", feature_availability)
    _write_json(artifact_dir / "gate_redesign_policy_spec.json", policy_spec)
    _write_json(artifact_dir / "branch_effect_audit.json", branch_effect_audit)
    _write_json(artifact_dir / "portfolio_economic_comparison.json", portfolio_comparison)
    _write_json(artifact_dir / "skipped_buy_restoration_summary.json", restoration_summary)
    _write_json(artifact_dir / "entry_delay_cost_summary.json", artifact_payloads["entry_delay_cost_summary.json"])
    _write_json(artifact_dir / "monthly_effectiveness_summary.json", monthly_summary)
    _write_json(artifact_dir / "regime_effectiveness_summary.json", regime_summary)
    _write_json(artifact_dir / "drawdown_attribution_summary.json", drawdown_summary)
    _write_json(artifact_dir / "cost_slippage_summary.json", cost_slippage_summary)
    _write_json(artifact_dir / "opportunity_cost_summary.json", opportunity_cost_summary)
    _write_json(artifact_dir / "hedge_pressure_summary.json", hedge_pressure_summary)
    _write_json(artifact_dir / "gate_redesign_decision.json", decision)

    restored_cases.to_parquet(artifact_dir / "restored_buy_cases.parquet", index=False)
    remaining_cases.to_parquet(artifact_dir / "remaining_skipped_buy_cases.parquet", index=False)

    complete = {
        "schema_version": "tradex_long_action_policy_gate_redesign_v1",
        "family_id": FAMILY_ID_REDESIGN,
        "generated_at": _utc_now(),
        "session_id": artifact_dir.name,
        "output_dir": str(artifact_dir),
        "artifact_list": list(artifact_payloads.keys()) + ["restored_buy_cases.parquet", "remaining_skipped_buy_cases.parquet", "_ARTIFACT_COMPLETE.json"],
        "verification_status": "generated",
        "commands_run": [
            f"python {SCRIPT_NAME}.py --output-root {output_root}",
            "git status --short",
            "git diff --name-only",
        ],
        "git_status_short": _safe_git_output(["git", "status", "--short"]),
        "git_diff_name_only": _safe_git_output(["git", "diff", "--name-only"]),
        "notes": [
            "TRADEX-only entry-gate redesign pass",
            "no MeeMee production surfaces were changed by this script",
        ],
    }
    _write_json(artifact_dir / "_ARTIFACT_COMPLETE.json", complete)

    return {
        "session_id": artifact_dir.name,
        "output_dir": str(artifact_dir),
        "artifacts": {name: str(artifact_dir / name) for name in artifact_payloads} | {"restored_buy_cases.parquet": str(artifact_dir / "restored_buy_cases.parquet"), "remaining_skipped_buy_cases.parquet": str(artifact_dir / "remaining_skipped_buy_cases.parquet")},
        "complete": str(artifact_dir / "_ARTIFACT_COMPLETE.json"),
    }


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="TRADEX long-entry cash-gate redesign runner")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT, help="Root output directory for the research session")
    parser.add_argument("--db-path", type=Path, default=None, help="Optional stock repository DuckDB path")
    parser.add_argument("--expanded-review-dir", type=Path, default=DEFAULT_EXPANDED_REVIEW_DIR, help="Expanded review artifact directory")
    parser.add_argument("--foundation-review-dir", type=Path, default=DEFAULT_FOUNDATION_REVIEW_DIR, help="Foundation review artifact directory")
    parser.add_argument("--skipped-cases-path", type=Path, default=DEFAULT_SKIPPED_CASES_PATH, help="Path to skipped-buy case parquet")
    parser.add_argument("--jobs", type=int, default=1, help="Requested job count; this runner records the request but executes sequentially")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    repo = _resolve_repo(args.db_path)
    result = build_gate_redesign_review(
        repo,
        args.output_root,
        expanded_review_dir=args.expanded_review_dir,
        foundation_review_dir=args.foundation_review_dir,
        skipped_cases_path=args.skipped_cases_path,
        window_specs=None,
        jobs=args.jobs,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
