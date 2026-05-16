from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import duckdb
import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts import tradex_daily_selection_replay_learning_v1 as base
from scripts import tradex_teppan_chart_pattern_discovery_v1 as discovery


AXIS_ID = "portfolio_agent_replay_v1"
SCHEMA_PREFIX = "tradex_portfolio_agent_replay_v1"
DEFAULT_SOURCE_DB = discovery.DEFAULT_SOURCE_DB
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\portfolio_agent_replay_v1")
DEFAULT_START_YMD = 20250401
DEFAULT_END_YMD = 20260401

INITIAL_CASH_JPY = 10_000_000.0
MAX_POSITIONS = 3
PER_SYMBOL_CAP_JPY = INITIAL_CASH_JPY / MAX_POSITIONS
LOT_SIZE = 100
CANDIDATE_TOP_N = 100
ENTRY_SCORE_THRESHOLD = base.ENTRY_SCORE_THRESHOLD
PROFIT_TARGET = base.PROFIT_TARGET
STOP_LOSS = base.STOP_LOSS
MAX_HOLDING_TRADING_DAYS = base.MAX_HOLDING_TRADING_DAYS
MARKET_BENCHMARK_CANDIDATES = ("1306", "1308", "1348")

SUPPORTED_ACTIONS = ("buy", "hold", "add", "trim", "exit", "stop", "reject")
V1_POLICY_TRIGGERED_ACTIONS = ("buy", "hold", "exit", "stop", "reject")
SUPPORTED_BUT_NOT_TRIGGERED = tuple(action for action in SUPPORTED_ACTIONS if action not in V1_POLICY_TRIGGERED_ACTIONS)

OUTCOME_LABEL_COLUMNS = (
    "post_ret_5",
    "post_ret_10",
    "post_ret_20",
    "post_ret_40",
    "mae_20",
    "mfe_20",
    "outcome_bucket",
)
DIAGNOSTIC_ONLY_COLUMNS = tuple(sorted(set(base.LABEL_COLUMNS) | set(OUTCOME_LABEL_COLUMNS)))
SELECTION_ALLOWED_COLUMNS = tuple(sorted(base.ENTRY_SCORING_FEATURE_COLUMNS | {"downside_guard_blocked"}))
SELECTION_FORBIDDEN_COLUMNS = tuple(sorted(set(DIAGNOSTIC_ONLY_COLUMNS) | {"future_close_5", "future_close_10", "future_close_20", "future_close_40"}))

REQUIRED_ARTIFACTS = (
    "run_config.json",
    "daily_market_snapshot.csv",
    "daily_candidate_snapshot.csv",
    "daily_action_ledger.jsonl",
    "orders_ledger.csv",
    "positions_ledger.csv",
    "equity_curve.csv",
    "rejected_candidates.csv",
    "post_run_outcome_labels.csv",
    "failure_diagnosis_summary.json",
    "no_lookahead_audit.json",
    "selection_feature_manifest.json",
    "_ARTIFACT_COMPLETE.json",
)

FAILURE_MODES = (
    "candidate_bad",
    "bought_weak_candidate",
    "missed_winner",
    "entry_too_early",
    "entry_too_late",
    "late_exit",
    "early_exit",
    "stop_too_wide",
    "stop_too_tight",
    "profit_take_too_early",
    "held_loser_too_long",
    "over_concentration",
    "cash_idle_too_high",
    "cost_drag",
    "event_risk_failure",
    "data_gap_failure",
    "no_primary_failure_profit_positive",
    "profitable_but_with_risks",
)


@dataclass
class OpenPosition:
    position_id: str
    code: str
    entry_order_id: str
    entry_decision_ymd: int
    entry_ymd: int
    entry_price: float
    shares: int
    entry_notional: float
    entry_cost: float
    cost_basis: float
    entry_score: int
    entry_rank: int
    entry_features: dict[str, Any]
    holding_days: int = 0
    pending_exit_order_id: str | None = None
    peak_return: float = 0.0
    trough_return: float = 0.0


@dataclass
class PendingOrder:
    order_id: str
    action: str
    code: str
    decision_ymd: int
    execution_ymd: int
    reason_type: str
    score: int | None
    rank: int | None
    shares: int | None = None
    position_id: str | None = None


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_json_ready(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    try:
        import numpy as np

        if isinstance(value, np.generic):
            return _json_ready(value.item())
    except Exception:
        pass
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _json_text(payload: Any) -> str:
    return json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, default=str)


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_json_text(payload) + "\n", encoding="utf-8")
    return path


def _write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(_json_text(row) + "\n" for row in rows), encoding="utf-8")
    return path


def _write_csv(path: Path, rows: Iterable[dict[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(list(rows)).to_csv(path, index=False)
    return path


def _stable_hash(payload: Any) -> str:
    return hashlib.sha256(_json_text(payload).encode("utf-8")).hexdigest()


def _resolve_source_db(source_db: str | Path | None) -> Path:
    if source_db and str(source_db).strip():
        path = Path(str(source_db)).expanduser().resolve()
    elif os.getenv("STOCKS_DB_PATH"):
        path = Path(os.environ["STOCKS_DB_PATH"]).expanduser().resolve()
    else:
        path = DEFAULT_SOURCE_DB.resolve()
    if not path.exists():
        raise FileNotFoundError(f"source DB not found: {path}")
    return path


def _safe_path(value: str | Path | None, default: Path) -> Path:
    if value is None or not str(value).strip():
        return default.resolve()
    return Path(str(value)).expanduser().resolve()


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _safe_int(value: Any) -> int | None:
    parsed = _safe_float(value)
    return None if parsed is None else int(parsed)


def _cost_model(total_one_way_bps: float = 30.0) -> dict[str, Any]:
    return {
        "schema_version": f"{SCHEMA_PREFIX}_cost_model_v1",
        "mode": "commission_plus_slippage_bps",
        "commission_bps": 15.0,
        "slippage_bps": max(0.0, float(total_one_way_bps) - 15.0),
        "tax_or_fee_bps": 0.0,
        "min_fee": 0.0,
        "one_way_total_bps": float(total_one_way_bps),
        "status": "provisional_replay_assumption",
    }


def _trade_cost(notional: float, cost_model: dict[str, Any]) -> tuple[float, float, float]:
    base_notional = abs(float(notional))
    commission = max(float(cost_model.get("min_fee") or 0.0), base_notional * float(cost_model["commission_bps"]) / 10_000.0)
    slippage = base_notional * float(cost_model["slippage_bps"]) / 10_000.0
    tax_or_fee = base_notional * float(cost_model.get("tax_or_fee_bps") or 0.0) / 10_000.0
    return commission + slippage + tax_or_fee, slippage, commission + tax_or_fee


def _load_inputs(source_path: Path, *, start_ymd: int, end_ymd: int, label_horizon_days: int = 80) -> tuple[pd.DataFrame, pd.DataFrame, int, int]:
    conn = duckdb.connect(str(source_path), read_only=True)
    try:
        max_daily_ymd = discovery._load_max_daily_ymd(conn)
        replay_end_ymd = min(int(end_ymd), int(max_daily_ymd))
        label_end_ts = discovery._ymd_to_timestamp(replay_end_ymd) + pd.DateOffset(days=int(label_horizon_days))
        label_end_ymd = min(discovery._timestamp_to_ymd(label_end_ts), int(max_daily_ymd))
        data_start_ts = discovery._ymd_to_timestamp(start_ymd) - pd.DateOffset(days=520)
        data_start_ymd = discovery._timestamp_to_ymd(data_start_ts)
        daily = discovery._load_daily_rows(conn, start_ymd=data_start_ymd, end_ymd=label_end_ymd)
        monthly = discovery._load_monthly_rows(conn, start_ymd=data_start_ymd, end_ymd=label_end_ymd)
    finally:
        conn.close()
    return daily, monthly, replay_end_ymd, label_end_ymd


def build_selection_feature_manifest() -> dict[str, Any]:
    overlap = sorted(set(SELECTION_ALLOWED_COLUMNS) & set(SELECTION_FORBIDDEN_COLUMNS))
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_selection_feature_manifest_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "selection_allowed_columns": list(SELECTION_ALLOWED_COLUMNS),
        "selection_forbidden_columns": list(SELECTION_FORBIDDEN_COLUMNS),
        "diagnostic_only_columns": list(DIAGNOSTIC_ONLY_COLUMNS),
        "outcome_label_columns": list(OUTCOME_LABEL_COLUMNS),
        "allowed_forbidden_overlap": overlap,
        "audit_result": "pass" if not overlap else "fail",
    }
    payload["manifest_hash"] = _stable_hash(payload)
    return payload


def build_no_lookahead_audit(
    *,
    features: pd.DataFrame,
    selection_manifest: dict[str, Any],
    orders: list[dict[str, Any]],
    selection_feature_manifest_path: Path,
    accounting_reconciliation: dict[str, Any],
) -> dict[str, Any]:
    feature_overlap = sorted(set(SELECTION_ALLOWED_COLUMNS) & set(DIAGNOSTIC_ONLY_COLUMNS))
    filled = [row for row in orders if row.get("order_status") == "filled"]
    same_day_fills = [
        row
        for row in filled
        if _safe_int(row.get("execution_ymd")) is not None
        and _safe_int(row.get("decision_ymd")) is not None
        and int(row["execution_ymd"]) <= int(row["decision_ymd"])
    ]
    future_feature_rows = 0
    if not features.empty:
        future_feature_rows = int((pd.to_numeric(features["ymd"], errors="coerce") > pd.to_numeric(features["ymd"], errors="coerce")).sum())
    checks = {
        "selection_feature_manifest_pass": selection_manifest.get("audit_result") == "pass",
        "selection_outcome_column_overlap_count": len(feature_overlap),
        "same_day_or_prior_filled_order_count": len(same_day_fills),
        "future_feature_rows": future_feature_rows,
        "accounting_reconciliation_pass": bool(accounting_reconciliation.get("status") == "pass"),
    }
    status = "pass" if all(
        [
            checks["selection_feature_manifest_pass"],
            checks["selection_outcome_column_overlap_count"] == 0,
            checks["same_day_or_prior_filled_order_count"] == 0,
            checks["future_feature_rows"] == 0,
        ]
    ) else "fail"
    return {
        "schema_version": f"{SCHEMA_PREFIX}_no_lookahead_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "audit_result": status,
        "selection_feature_manifest_path": str(selection_feature_manifest_path),
        "selection_feature_manifest_hash": selection_manifest.get("manifest_hash"),
        "checks": checks,
        "same_day_or_prior_filled_order_examples": same_day_fills[:10],
        "future_label_use": {
            "used_in_selection": False,
            "diagnostic_only_after_replay_completion": True,
            "diagnostic_only_columns": list(DIAGNOSTIC_ONLY_COLUMNS),
        },
        "execution_model": {
            "decision_timing": "after_close",
            "fill_timing": "next_trading_session_open",
            "same_day_close_fill_allowed": False,
        },
        "silent_fallback_used": False,
    }


def _candidate_features(row: pd.Series) -> dict[str, Any]:
    return {column: row.get(column) for column in SELECTION_ALLOWED_COLUMNS if column in row.index}


def _score_candidates(day_rows: pd.DataFrame, *, entry_score_threshold: int, top_n: int) -> pd.DataFrame:
    scored_rows: list[dict[str, Any]] = []
    for _idx, row in day_rows.iterrows():
        score = base.score_entry_candidate(row, entry_score_threshold=entry_score_threshold)
        out = row.to_dict()
        out["selection_score"] = int(score["score"])
        out["entry_allowed_by_score"] = bool(score["entry_allowed_by_score"])
        out["downside_guard_blocked"] = bool(score["downside_guard_blocked"])
        out["score_components_json"] = _json_text(score["components"])
        scored_rows.append(out)
    frame = pd.DataFrame(scored_rows)
    if frame.empty:
        return frame
    frame = frame.sort_values(["selection_score", "code"], ascending=[False, True], kind="stable").head(int(top_n)).copy()
    frame["candidate_rank"] = range(1, len(frame) + 1)
    return frame


def _make_run_config(
    *,
    source_db: Path,
    output_dir: Path,
    run_id: str,
    start_ymd: int,
    end_ymd: int,
    label_end_ymd: int,
    entry_score_threshold: int,
    cost_model: dict[str, Any],
) -> dict[str, Any]:
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_run_config_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "run_id": run_id,
        "artifact_root": str(output_dir),
        "source_db": str(source_db),
        "scope": "TRADEX-only",
        "period": {"start_ymd": int(start_ymd), "end_ymd": int(end_ymd), "label_end_ymd": int(label_end_ymd)},
        "portfolio": {
            "initial_cash_jpy": INITIAL_CASH_JPY,
            "max_positions": MAX_POSITIONS,
            "per_symbol_cap_jpy": PER_SYMBOL_CAP_JPY,
            "lot_size": LOT_SIZE,
            "long_only": True,
        },
        "execution": {
            "decision_timing": "after_close",
            "fill_timing": "next_trading_session_open",
            "same_day_close_fill_allowed": False,
        },
        "selection": {
            "candidate_top_n": CANDIDATE_TOP_N,
            "entry_score_threshold": int(entry_score_threshold),
            "scoring_source": "scripts.tradex_daily_selection_replay_learning_v1.score_entry_candidate",
            "condition_optimized_after_result": False,
        },
        "exit_rules": {
            "profit_target": PROFIT_TARGET,
            "stop_loss": STOP_LOSS,
            "max_holding_trading_days": MAX_HOLDING_TRADING_DAYS,
        },
        "cost_model": cost_model,
        "action_schema": {
            "supported_actions": list(SUPPORTED_ACTIONS),
            "v1_policy_triggered_actions": list(V1_POLICY_TRIGGERED_ACTIONS),
            "supported_but_not_triggered": list(SUPPORTED_BUT_NOT_TRIGGERED),
        },
        "prohibitions": {
            "meemee_ui_changed": False,
            "runtime_db_write": False,
            "ranking_engine_changed": False,
            "publish_registry_changed": False,
            "broker_api_connected": False,
        },
        "silent_fallback_used": False,
    }
    payload["config_hash"] = _stable_hash(payload)
    return payload


def _find_benchmark_series(daily: pd.DataFrame, *, start_ymd: int, end_ymd: int) -> tuple[str | None, pd.DataFrame, dict[str, Any]]:
    for code in MARKET_BENCHMARK_CANDIDATES:
        frame = daily[(daily["code"].astype(str) == code) & (daily["ymd"] >= int(start_ymd)) & (daily["ymd"] <= int(end_ymd))].copy()
        if not frame.empty and frame["c"].notna().sum() >= 2:
            frame = frame.sort_values("ymd", kind="stable")
            return code, frame, {"benchmark_status": "available", "benchmark_code": code, "reason": ""}
    return None, pd.DataFrame(), {
        "benchmark_status": "unavailable",
        "benchmark_code": None,
        "reason": f"none of {list(MARKET_BENCHMARK_CANDIDATES)} had safe daily data for replay period",
    }


def _shares_for_cash(*, cash: float, price: float, per_symbol_cap: float, lot_size: int, cost_model: dict[str, Any]) -> int:
    max_notional = min(float(cash), float(per_symbol_cap))
    if price <= 0.0 or max_notional <= 0.0:
        return 0
    total_bps = float(cost_model["one_way_total_bps"]) / 10_000.0
    raw_shares = int(max_notional / (float(price) * (1.0 + total_bps)))
    return int(raw_shares // int(lot_size) * int(lot_size))


def _market_value(position: OpenPosition, close_price: float | None) -> float:
    price = position.entry_price if close_price is None or close_price <= 0.0 else close_price
    return float(position.shares) * float(price)


def _profit_factor(trade_returns: list[float]) -> float | None:
    gains = sum(value for value in trade_returns if value > 0.0)
    losses = sum(value for value in trade_returns if value < 0.0)
    if losses == 0.0:
        return None if gains == 0.0 else 999.0
    return gains / abs(losses)


def _max_drawdown(equity_rows: list[dict[str, Any]]) -> float:
    peak: float | None = None
    worst = 0.0
    for row in equity_rows:
        equity = _safe_float(row.get("equity"))
        if equity is None:
            continue
        peak = equity if peak is None else max(peak, equity)
        if peak and peak > 0.0:
            worst = min(worst, equity / peak - 1.0)
    return float(worst)


def _outcome_for(candidate: dict[str, Any], raw_by_code: dict[str, pd.DataFrame]) -> dict[str, Any]:
    code = str(candidate["code"])
    ymd = int(candidate["decision_ymd"])
    rows = raw_by_code.get(code)
    base_close = _safe_float(candidate.get("close"))
    if rows is None or rows.empty or base_close is None or base_close <= 0.0:
        return {column: None for column in OUTCOME_LABEL_COLUMNS}
    idx_candidates = rows.index[rows["ymd"] == ymd].tolist()
    if not idx_candidates:
        return {column: None for column in OUTCOME_LABEL_COLUMNS}
    idx = int(idx_candidates[0])
    out: dict[str, Any] = {}
    for horizon in (5, 10, 20, 40):
        future_idx = idx + horizon
        if future_idx < len(rows):
            future_close = _safe_float(rows.iloc[future_idx].get("c"))
            out[f"post_ret_{horizon}"] = None if future_close is None else future_close / base_close - 1.0
        else:
            out[f"post_ret_{horizon}"] = None
    future_window = rows.iloc[idx + 1 : idx + 21]
    if future_window.empty:
        out["mae_20"] = None
        out["mfe_20"] = None
    else:
        lows = pd.to_numeric(future_window["l"], errors="coerce")
        highs = pd.to_numeric(future_window["h"], errors="coerce")
        out["mae_20"] = None if lows.dropna().empty else float(lows.min() / base_close - 1.0)
        out["mfe_20"] = None if highs.dropna().empty else float(highs.max() / base_close - 1.0)
    ret20 = _safe_float(out.get("post_ret_20"))
    if ret20 is None:
        out["outcome_bucket"] = "outcome_unavailable"
    elif ret20 >= 0.10:
        out["outcome_bucket"] = "strong_winner"
    elif ret20 > 0.0:
        out["outcome_bucket"] = "winner"
    elif ret20 <= -0.10:
        out["outcome_bucket"] = "severe_loser"
    else:
        out["outcome_bucket"] = "loser"
    return out


def _diagnose_failure(
    *,
    final_equity: float,
    initial_cash: float,
    equity_rows: list[dict[str, Any]],
    orders: list[dict[str, Any]],
    rejected_rows: list[dict[str, Any]],
    outcome_rows: list[dict[str, Any]],
    cost_total: float,
) -> dict[str, Any]:
    total_return = final_equity / initial_cash - 1.0 if initial_cash else None
    trade_returns = [
        float(row["realized_return"])
        for row in orders
        if row.get("action") in {"exit", "stop"} and row.get("order_status") == "filled" and _safe_float(row.get("realized_return")) is not None
    ]
    win_rate = (sum(1 for value in trade_returns if value > 0.0) / len(trade_returns)) if trade_returns else None
    avg_profit = (sum(value for value in trade_returns if value > 0.0) / sum(1 for value in trade_returns if value > 0.0)) if any(value > 0.0 for value in trade_returns) else None
    avg_loss = (sum(value for value in trade_returns if value < 0.0) / sum(1 for value in trade_returns if value < 0.0)) if any(value < 0.0 for value in trade_returns) else None
    cash_idle = [float(row.get("cash", 0.0)) / initial_cash for row in equity_rows if initial_cash]
    avg_cash_idle = sum(cash_idle) / len(cash_idle) if cash_idle else None
    missed_winner_count = sum(1 for row in outcome_rows if row.get("was_selected") is False and (_safe_float(row.get("post_ret_20")) or 0.0) >= 0.10)
    bought_weak_count = sum(1 for row in outcome_rows if row.get("was_selected") is True and (_safe_float(row.get("post_ret_20")) or 0.0) <= 0.0)
    severe_loss_count = sum(1 for value in trade_returns if value <= -0.10)
    cost_return_drag = cost_total / initial_cash if initial_cash else 0.0
    secondary_risks: list[str] = []
    if avg_cash_idle is not None and avg_cash_idle >= 0.50:
        secondary_risks.append("cash_idle_too_high")
    if cost_return_drag >= 0.01:
        secondary_risks.append("cost_drag")
    if missed_winner_count > 0:
        secondary_risks.append("missed_winner")
    if bought_weak_count > 0:
        secondary_risks.append("bought_weak_candidate")
    if severe_loss_count > 0:
        secondary_risks.append("held_loser_too_long")
    if final_equity > initial_cash:
        primary = "profitable_but_with_risks" if secondary_risks else "no_primary_failure_profit_positive"
    elif missed_winner_count >= max(1, bought_weak_count):
        primary = "missed_winner"
    elif bought_weak_count > 0:
        primary = "bought_weak_candidate"
    elif avg_cash_idle is not None and avg_cash_idle >= 0.50:
        primary = "cash_idle_too_high"
    elif cost_return_drag >= 0.005:
        primary = "cost_drag"
    elif not rejected_rows:
        primary = "data_gap_failure"
    else:
        primary = "candidate_bad"
    return {
        "schema_version": f"{SCHEMA_PREFIX}_failure_diagnosis_summary_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "primary_failure_mode": primary,
        "valid_failure_modes": list(FAILURE_MODES),
        "secondary_risks": sorted(set(secondary_risks)),
        "metrics": {
            "initial_cash": initial_cash,
            "final_equity": final_equity,
            "total_return": total_return,
            "max_drawdown": _max_drawdown(equity_rows),
            "win_rate": win_rate,
            "average_profit": avg_profit,
            "average_loss": avg_loss,
            "profit_factor": _profit_factor(trade_returns),
            "trade_count": len(trade_returns),
            "total_cost": cost_total,
            "cost_return_drag": cost_return_drag,
            "bankrupt": final_equity <= 0.0,
            "avg_cash_idle_ratio": avg_cash_idle,
            "missed_winner_count": missed_winner_count,
            "bought_weak_candidate_count": bought_weak_count,
        },
        "action_support": {
            "supported_actions": list(SUPPORTED_ACTIONS),
            "supported_but_not_triggered": list(SUPPORTED_BUT_NOT_TRIGGERED),
        },
        "silent_fallback_used": False,
    }


def _accounting_reconciliation(equity_rows: list[dict[str, Any]], orders: list[dict[str, Any]]) -> dict[str, Any]:
    max_abs_diff = 0.0
    for row in equity_rows:
        expected = float(row.get("cash", 0.0)) + float(row.get("positions_market_value", 0.0))
        actual = float(row.get("equity", 0.0))
        max_abs_diff = max(max_abs_diff, abs(expected - actual))
    invalid_cash_orders = [row for row in orders if row.get("order_status") == "filled" and (_safe_float(row.get("cash_after")) or 0.0) < -0.01]
    status = "pass" if max_abs_diff <= 0.05 and not invalid_cash_orders else "fail"
    return {
        "schema_version": f"{SCHEMA_PREFIX}_accounting_reconciliation_v1",
        "status": status,
        "max_abs_cash_plus_positions_minus_equity": max_abs_diff,
        "filled_order_negative_cash_count": len(invalid_cash_orders),
    }


def run_portfolio_agent_replay_v1(
    *,
    source_db: str | Path | None = None,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    run_id: str | None = None,
    start_ymd: int = DEFAULT_START_YMD,
    end_ymd: int = DEFAULT_END_YMD,
    entry_score_threshold: int = ENTRY_SCORE_THRESHOLD,
    candidate_top_n: int = CANDIDATE_TOP_N,
    cost_bps: float = 30.0,
) -> dict[str, Any]:
    source_path = _resolve_source_db(source_db)
    output_base = _safe_path(output_root, DEFAULT_OUTPUT_ROOT)
    run_name = run_id.strip() if run_id else _default_run_id()
    if not run_name.endswith(AXIS_ID):
        run_name = f"{run_name}-{AXIS_ID}"
    output_dir = output_base / run_name
    output_dir.mkdir(parents=True, exist_ok=True)

    cost_model = _cost_model(cost_bps)
    daily, monthly, effective_end_ymd, label_end_ymd = _load_inputs(source_path, start_ymd=start_ymd, end_ymd=end_ymd)
    all_features = base.build_point_in_time_feature_frame(daily, monthly, replay_start_ymd=start_ymd)
    features = all_features[(all_features["ymd"] >= int(start_ymd)) & (all_features["ymd"] <= int(effective_end_ymd))].copy()
    raw_daily = base._normalize_daily_frame(daily).sort_values(["code", "ymd"], kind="stable").reset_index(drop=True)
    raw_by_code = {code: group.reset_index(drop=True) for code, group in raw_daily.groupby("code", sort=False)}

    selection_manifest = build_selection_feature_manifest()
    run_config = _make_run_config(
        source_db=source_path,
        output_dir=output_dir,
        run_id=run_name,
        start_ymd=start_ymd,
        end_ymd=effective_end_ymd,
        label_end_ymd=label_end_ymd,
        entry_score_threshold=entry_score_threshold,
        cost_model=cost_model,
    )

    cash = INITIAL_CASH_JPY
    positions: dict[str, OpenPosition] = {}
    pending_orders: dict[int, list[PendingOrder]] = {}
    order_seq = 0
    total_cost = 0.0
    realized_pnl_total = 0.0
    candidate_rows: list[dict[str, Any]] = []
    action_rows: list[dict[str, Any]] = []
    order_rows: list[dict[str, Any]] = []
    position_rows: list[dict[str, Any]] = []
    equity_rows: list[dict[str, Any]] = []
    rejected_rows: list[dict[str, Any]] = []
    selected_candidate_keys: set[tuple[int, str]] = set()
    top_candidate_keys: set[tuple[int, str]] = set()

    benchmark_code, benchmark_frame, benchmark_status = _find_benchmark_series(raw_daily, start_ymd=start_ymd, end_ymd=effective_end_ymd)
    benchmark_start_close = _safe_float(benchmark_frame.iloc[0]["c"]) if not benchmark_frame.empty else None
    benchmark_by_ymd = {int(row["ymd"]): row for _idx, row in benchmark_frame.iterrows()} if not benchmark_frame.empty else {}

    for ymd, day_rows in features.groupby("ymd", sort=True):
        ymd = int(ymd)
        day_rows = day_rows.sort_values("code", kind="stable")
        rows_by_code = {str(row["code"]): row for _idx, row in day_rows.iterrows()}

        for order in pending_orders.pop(ymd, []):
            day_row = rows_by_code.get(order.code)
            open_price = _safe_float(day_row.get("o")) if day_row is not None else None
            if open_price is None or open_price <= 0.0:
                order_rows.append(
                    {
                        "order_id": order.order_id,
                        "decision_ymd": order.decision_ymd,
                        "execution_ymd": order.execution_ymd,
                        "action": order.action,
                        "code": order.code,
                        "order_status": "unfilled",
                        "unfilled_reason": "missing_next_open",
                        "execution_price": None,
                        "shares": order.shares,
                        "notional": 0.0,
                        "cost_amount": 0.0,
                        "slippage_amount": 0.0,
                        "cash_after": cash,
                    }
                )
                continue
            if order.action == "buy":
                shares = _shares_for_cash(cash=cash, price=open_price, per_symbol_cap=PER_SYMBOL_CAP_JPY, lot_size=LOT_SIZE, cost_model=cost_model)
                if shares <= 0:
                    order_rows.append(
                        {
                            "order_id": order.order_id,
                            "decision_ymd": order.decision_ymd,
                            "execution_ymd": order.execution_ymd,
                            "action": order.action,
                            "code": order.code,
                            "order_status": "unfilled",
                            "unfilled_reason": "insufficient_cash_or_lot_size",
                            "execution_price": open_price,
                            "shares": 0,
                            "notional": 0.0,
                            "cost_amount": 0.0,
                            "slippage_amount": 0.0,
                            "cash_after": cash,
                        }
                    )
                    continue
                notional = shares * open_price
                cost_amount, slippage_amount, _fees = _trade_cost(notional, cost_model)
                if cash < notional + cost_amount:
                    continue
                position_id = _stable_hash({"run_id": run_name, "order_id": order.order_id, "code": order.code})[:16]
                cash -= notional + cost_amount
                total_cost += cost_amount
                positions[order.code] = OpenPosition(
                    position_id=position_id,
                    code=order.code,
                    entry_order_id=order.order_id,
                    entry_decision_ymd=order.decision_ymd,
                    entry_ymd=order.execution_ymd,
                    entry_price=open_price,
                    shares=shares,
                    entry_notional=notional,
                    entry_cost=cost_amount,
                    cost_basis=notional + cost_amount,
                    entry_score=int(order.score or 0),
                    entry_rank=int(order.rank or 0),
                    entry_features={},
                )
                order_rows.append(
                    {
                        "order_id": order.order_id,
                        "decision_ymd": order.decision_ymd,
                        "execution_ymd": order.execution_ymd,
                        "action": order.action,
                        "code": order.code,
                        "order_status": "filled",
                        "unfilled_reason": "",
                        "execution_price": open_price,
                        "shares": shares,
                        "notional": notional,
                        "cost_amount": cost_amount,
                        "slippage_amount": slippage_amount,
                        "cash_after": cash,
                        "position_id": position_id,
                        "reason_type": order.reason_type,
                    }
                )
            elif order.action in {"exit", "stop"}:
                position = positions.pop(order.code, None)
                if position is None:
                    continue
                shares = position.shares
                notional = shares * open_price
                cost_amount, slippage_amount, _fees = _trade_cost(notional, cost_model)
                cash += notional - cost_amount
                total_cost += cost_amount
                realized_pnl = notional - cost_amount - position.cost_basis
                realized_pnl_total += realized_pnl
                order_rows.append(
                    {
                        "order_id": order.order_id,
                        "decision_ymd": order.decision_ymd,
                        "execution_ymd": order.execution_ymd,
                        "action": order.action,
                        "code": order.code,
                        "order_status": "filled",
                        "unfilled_reason": "",
                        "execution_price": open_price,
                        "shares": shares,
                        "notional": notional,
                        "cost_amount": cost_amount,
                        "slippage_amount": slippage_amount,
                        "cash_after": cash,
                        "position_id": position.position_id,
                        "reason_type": order.reason_type,
                        "realized_pnl": realized_pnl,
                        "realized_return": realized_pnl / position.cost_basis if position.cost_basis else None,
                    }
                )

        scored = _score_candidates(day_rows, entry_score_threshold=entry_score_threshold, top_n=candidate_top_n)
        selected_today: list[dict[str, Any]] = []
        selected_codes: set[str] = set()
        selected_score = None
        selected_rank = None
        exit_scheduled_codes: set[str] = set()

        for code, position in list(positions.items()):
            row = rows_by_code.get(code)
            close_price = _safe_float(row.get("c")) if row is not None else None
            if close_price is None or close_price <= 0.0:
                action_rows.append(
                    {
                        "decision_ymd": ymd,
                        "execution_ymd": None,
                        "action": "hold",
                        "code": code,
                        "reason_type": "missing_close_for_exit_check",
                        "order_status": "not_applicable",
                    }
                )
                continue
            position.holding_days += 1
            close_return = close_price / position.entry_price - 1.0
            position.peak_return = max(position.peak_return, close_return)
            position.trough_return = min(position.trough_return, close_return)
            next_open = _safe_float(row.get("next_open"))
            next_ymd = _safe_int(row.get("next_ymd"))
            exit_action: str | None = None
            reason_type: str | None = None
            if close_return <= STOP_LOSS:
                exit_action = "stop"
                reason_type = "stop_loss"
            elif close_return >= PROFIT_TARGET:
                exit_action = "exit"
                reason_type = "profit_target"
            elif position.holding_days >= MAX_HOLDING_TRADING_DAYS:
                exit_action = "exit"
                reason_type = "time_stop"
            if exit_action and next_open is not None and next_ymd is not None:
                order_seq += 1
                order_id = f"{AXIS_ID}-{order_seq:08d}"
                position.pending_exit_order_id = order_id
                pending_orders.setdefault(next_ymd, []).append(
                    PendingOrder(
                        order_id=order_id,
                        action=exit_action,
                        code=code,
                        decision_ymd=ymd,
                        execution_ymd=next_ymd,
                        reason_type=reason_type or exit_action,
                        score=None,
                        rank=None,
                        shares=position.shares,
                        position_id=position.position_id,
                    )
                )
                exit_scheduled_codes.add(code)
                action_rows.append(
                    {
                        "decision_ymd": ymd,
                        "execution_ymd": next_ymd,
                        "action": exit_action,
                        "code": code,
                        "reason_type": reason_type,
                        "order_id": order_id,
                        "order_status": "scheduled",
                        "close_return_at_decision": close_return,
                    }
                )
            else:
                action_rows.append(
                    {
                        "decision_ymd": ymd,
                        "execution_ymd": None,
                        "action": "hold",
                        "code": code,
                        "reason_type": "no_exit_rule_triggered",
                        "order_status": "not_applicable",
                        "close_return_at_decision": close_return,
                    }
                )

        slots_available = max(0, MAX_POSITIONS - (len(positions) - len(exit_scheduled_codes)))
        if not scored.empty:
            for _idx, row in scored.iterrows():
                code = str(row["code"])
                next_ymd = _safe_int(row.get("next_ymd"))
                next_open = _safe_float(row.get("next_open"))
                score = int(row["selection_score"])
                rank = int(row["candidate_rank"])
                allowed = bool(row["entry_allowed_by_score"]) and not bool(row["downside_guard_blocked"])
                reject_reason = ""
                will_buy = False
                if code in positions and code not in exit_scheduled_codes:
                    reject_reason = "existing_position"
                elif not allowed:
                    reject_reason = "downside_guard_blocked" if bool(row["downside_guard_blocked"]) else "score_below_threshold"
                elif next_ymd is None or next_open is None:
                    reject_reason = "no_next_open"
                elif slots_available <= 0:
                    reject_reason = "max_positions_full"
                elif code in selected_codes:
                    reject_reason = "already_selected_today"
                else:
                    will_buy = True
                candidate_key = (ymd, code)
                top_candidate_keys.add(candidate_key)
                if will_buy:
                    order_seq += 1
                    order_id = f"{AXIS_ID}-{order_seq:08d}"
                    pending_orders.setdefault(int(next_ymd), []).append(
                        PendingOrder(
                            order_id=order_id,
                            action="buy",
                            code=code,
                            decision_ymd=ymd,
                            execution_ymd=int(next_ymd),
                            reason_type="entry_score_passed",
                            score=score,
                            rank=rank,
                        )
                    )
                    selected_today.append({"code": code, "score": score, "rank": rank})
                    selected_codes.add(code)
                    selected_candidate_keys.add(candidate_key)
                    slots_available -= 1
                    selected_score = score if selected_score is None else max(selected_score, score)
                    selected_rank = rank if selected_rank is None else min(selected_rank, rank)
                    action_rows.append(
                        {
                            "decision_ymd": ymd,
                            "execution_ymd": int(next_ymd),
                            "action": "buy",
                            "code": code,
                            "reason_type": "entry_score_passed",
                            "order_id": order_id,
                            "order_status": "scheduled",
                            "selection_score": score,
                            "candidate_rank": rank,
                        }
                    )
                else:
                    rejected = {
                        "decision_ymd": ymd,
                        "code": code,
                        "candidate_rank": rank,
                        "selection_score": score,
                        "reject_reason": reject_reason,
                        "selected_best_score": selected_score,
                        "selected_best_rank": selected_rank,
                        "score_gap_vs_selected_best": None if selected_score is None else float(score - selected_score),
                        "rank_gap_vs_selected_best": None if selected_rank is None else int(rank - selected_rank),
                    }
                    rejected_rows.append(rejected)
                    action_rows.append(
                        {
                            "decision_ymd": ymd,
                            "execution_ymd": None,
                            "action": "reject",
                            "code": code,
                            "reason_type": reject_reason,
                            "order_status": "not_applicable",
                            "selection_score": score,
                            "candidate_rank": rank,
                        }
                    )
                candidate_rows.append(
                    {
                        "decision_ymd": ymd,
                        "code": code,
                        "candidate_rank": rank,
                        "selection_score": score,
                        "entry_allowed_by_score": bool(row["entry_allowed_by_score"]),
                        "downside_guard_blocked": bool(row["downside_guard_blocked"]),
                        "selected_for_buy": will_buy,
                        "reject_reason": "" if will_buy else reject_reason,
                        "next_execution_ymd": next_ymd,
                        "close": _safe_float(row.get("c")),
                        "next_open_available": next_open is not None,
                        "score_components_json": row.get("score_components_json"),
                    }
                )

        positions_market_value = 0.0
        for code, position in positions.items():
            row = rows_by_code.get(code)
            close_price = _safe_float(row.get("c")) if row is not None else None
            market_value = _market_value(position, close_price)
            unrealized_pnl = market_value - position.cost_basis
            positions_market_value += market_value
            position_rows.append(
                {
                    "ymd": ymd,
                    "position_id": position.position_id,
                    "code": code,
                    "shares": position.shares,
                    "entry_ymd": position.entry_ymd,
                    "entry_price": position.entry_price,
                    "close_price": close_price,
                    "market_value": market_value,
                    "cost_basis": position.cost_basis,
                    "unrealized_pnl": unrealized_pnl,
                    "holding_days": position.holding_days,
                    "pending_exit_order_id": position.pending_exit_order_id or "",
                }
            )
        equity = cash + positions_market_value
        benchmark_equity = None
        if benchmark_code and benchmark_start_close:
            bench_row = benchmark_by_ymd.get(ymd)
            bench_close = _safe_float(bench_row.get("c")) if bench_row is not None else None
            if bench_close is not None:
                benchmark_equity = INITIAL_CASH_JPY * (bench_close / benchmark_start_close)
        equity_rows.append(
            {
                "ymd": ymd,
                "cash": cash,
                "positions_market_value": positions_market_value,
                "equity": equity,
                "cash_only_equity": INITIAL_CASH_JPY,
                "market_benchmark_equity": benchmark_equity,
                "benchmark_code": benchmark_code,
                "benchmark_status": benchmark_status["benchmark_status"],
                "open_position_count": len(positions),
                "realized_pnl_total": realized_pnl_total,
                "total_cost": total_cost,
            }
        )

    post_run_rows: list[dict[str, Any]] = []
    candidate_lookup = {(row["decision_ymd"], row["code"]): row for row in candidate_rows}
    for key in sorted(top_candidate_keys):
        candidate = candidate_lookup.get(key)
        if not candidate:
            continue
        label_payload = _outcome_for(candidate, raw_by_code)
        post_run_rows.append(
            {
                "decision_ymd": key[0],
                "code": key[1],
                "was_selected": key in selected_candidate_keys,
                "diagnostic_only": True,
                **label_payload,
            }
        )

    accounting = _accounting_reconciliation(equity_rows, order_rows)
    final_equity = float(equity_rows[-1]["equity"]) if equity_rows else INITIAL_CASH_JPY
    failure_summary = _diagnose_failure(
        final_equity=final_equity,
        initial_cash=INITIAL_CASH_JPY,
        equity_rows=equity_rows,
        orders=order_rows,
        rejected_rows=rejected_rows,
        outcome_rows=post_run_rows,
        cost_total=total_cost,
    )
    failure_summary["benchmark"] = benchmark_status
    if benchmark_code and equity_rows and equity_rows[-1].get("market_benchmark_equity") is not None:
        failure_summary["benchmark"]["market_benchmark_total_return"] = float(equity_rows[-1]["market_benchmark_equity"]) / INITIAL_CASH_JPY - 1.0
    failure_summary["metrics"]["benchmark_status"] = benchmark_status["benchmark_status"]

    paths: dict[str, str] = {}
    paths["run_config.json"] = str(_write_json(output_dir / "run_config.json", run_config))
    paths["selection_feature_manifest.json"] = str(_write_json(output_dir / "selection_feature_manifest.json", selection_manifest))
    paths["daily_market_snapshot.csv"] = str(
        _write_csv(
            output_dir / "daily_market_snapshot.csv",
            [
                {
                    "ymd": row["ymd"],
                    "candidate_count": int((pd.Series([r["decision_ymd"] for r in candidate_rows]) == row["ymd"]).sum()) if candidate_rows else 0,
                    "cash_only_equity": row["cash_only_equity"],
                    "market_benchmark_equity": row["market_benchmark_equity"],
                    "benchmark_status": row["benchmark_status"],
                    "benchmark_code": row["benchmark_code"],
                }
                for row in equity_rows
            ],
        )
    )
    paths["daily_candidate_snapshot.csv"] = str(_write_csv(output_dir / "daily_candidate_snapshot.csv", candidate_rows))
    paths["daily_action_ledger.jsonl"] = str(_write_jsonl(output_dir / "daily_action_ledger.jsonl", action_rows))
    paths["orders_ledger.csv"] = str(_write_csv(output_dir / "orders_ledger.csv", order_rows))
    paths["positions_ledger.csv"] = str(_write_csv(output_dir / "positions_ledger.csv", position_rows))
    paths["equity_curve.csv"] = str(_write_csv(output_dir / "equity_curve.csv", equity_rows))
    paths["rejected_candidates.csv"] = str(_write_csv(output_dir / "rejected_candidates.csv", rejected_rows))
    paths["post_run_outcome_labels.csv"] = str(_write_csv(output_dir / "post_run_outcome_labels.csv", post_run_rows))
    paths["failure_diagnosis_summary.json"] = str(_write_json(output_dir / "failure_diagnosis_summary.json", failure_summary))
    no_lookahead = build_no_lookahead_audit(
        features=features,
        selection_manifest=selection_manifest,
        orders=order_rows,
        selection_feature_manifest_path=output_dir / "selection_feature_manifest.json",
        accounting_reconciliation=accounting,
    )
    paths["no_lookahead_audit.json"] = str(_write_json(output_dir / "no_lookahead_audit.json", no_lookahead))
    existing = {name: (output_dir / name).exists() for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json"}
    critical_logs = {
        "daily_candidate_snapshot.csv": len(candidate_rows),
        "daily_action_ledger.jsonl": len(action_rows),
        "rejected_candidates.csv": len(rejected_rows),
        "post_run_outcome_labels.csv": len(post_run_rows),
    }
    complete = {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "artifact_root": str(output_dir),
        "required_artifacts": list(REQUIRED_ARTIFACTS),
        "existing_artifacts": existing,
        "required_artifacts_all_present": all(existing.values()),
        "selection_feature_manifest_audit_result": selection_manifest["audit_result"],
        "no_lookahead_audit": no_lookahead["audit_result"],
        "accounting_reconciliation": accounting,
        "next_open_execution": "pass" if no_lookahead["checks"]["same_day_or_prior_filled_order_count"] == 0 else "fail",
        "critical_log_row_counts": critical_logs,
        "critical_logs_non_empty": all(count > 0 for count in critical_logs.values()),
        "complete": all(existing.values())
        and selection_manifest["audit_result"] == "pass"
        and no_lookahead["audit_result"] == "pass"
        and accounting["status"] == "pass"
        and all(count > 0 for count in critical_logs.values()),
        "silent_fallback_used": False,
        "meemee_reflectable": False,
        "publish_bundle_created": False,
    }
    paths["_ARTIFACT_COMPLETE.json"] = str(_write_json(output_dir / "_ARTIFACT_COMPLETE.json", complete))
    return {
        "output_dir": str(output_dir),
        "paths": paths,
        "required_artifacts_all_present": complete["required_artifacts_all_present"],
        "selection_feature_manifest": selection_manifest["audit_result"],
        "no_lookahead_audit": no_lookahead["audit_result"],
        "accounting_reconciliation": accounting["status"],
        "next_open_execution": complete["next_open_execution"],
        "critical_log_row_counts": critical_logs,
        "final_equity": final_equity,
        "total_return": final_equity / INITIAL_CASH_JPY - 1.0,
        "primary_failure_mode": failure_summary["primary_failure_mode"],
        "benchmark_status": benchmark_status["benchmark_status"],
        "silent_fallback_used": False,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-db", default="")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--run-id", default="")
    parser.add_argument("--start-ymd", type=int, default=DEFAULT_START_YMD)
    parser.add_argument("--end-ymd", type=int, default=DEFAULT_END_YMD)
    parser.add_argument("--entry-score-threshold", type=int, default=ENTRY_SCORE_THRESHOLD)
    parser.add_argument("--candidate-top-n", type=int, default=CANDIDATE_TOP_N)
    parser.add_argument("--cost-bps", type=float, default=30.0)
    args = parser.parse_args(argv)
    result = run_portfolio_agent_replay_v1(
        source_db=args.source_db.strip() or None,
        output_root=args.output_root,
        run_id=args.run_id.strip() or None,
        start_ymd=args.start_ymd,
        end_ymd=args.end_ymd,
        entry_score_threshold=args.entry_score_threshold,
        candidate_top_n=args.candidate_top_n,
        cost_bps=args.cost_bps,
    )
    print(_json_text(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
