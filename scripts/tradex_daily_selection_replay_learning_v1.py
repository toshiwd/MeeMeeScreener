from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import duckdb
import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.backend.services import tradex_research_contracts as contracts
from scripts import tradex_teppan_chart_pattern_discovery_v1 as discovery
from scripts import tradex_teppan_loss_guard_v1 as loss_guard


AXIS_ID = "daily_selection_replay_learning_v1"
SCHEMA_PREFIX = "tradex_daily_selection_replay_learning_v1"
DEFAULT_SOURCE_DB = discovery.DEFAULT_SOURCE_DB
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\daily_selection_replay_learning_v1")
DEFAULT_START_YMD = 20260101

INITIAL_CAPITAL_JPY = 10_000_000.0
MAX_POSITIONS = 5
PER_POSITION_CAPITAL_JPY = 1_000_000.0
ENTRY_SCORE_THRESHOLD = 10
PROFIT_TARGET = 0.08
STOP_LOSS = -0.06
MAX_HOLDING_TRADING_DAYS = 20
SEVERE_LOSS_THRESHOLD = -0.10
LEARNING_MODE_NONE = "none"
LEARNING_MODE_LOSS_FINGERPRINT_GUARD = "loss_fingerprint_guard"
LEARNING_FINGERPRINT_FIELDS = (
    "daily_ma_stack",
    "daily_candle_state",
    "weekly_ret4_state",
    "monthly_ret6_state",
)

LABEL_COLUMNS = set(discovery.LABEL_COLUMNS) | {
    "ret20_fwd",
    "ret40_fwd",
    "mfe20",
    "mae20",
    "win20",
    "win40",
    "severe_loss20",
    "future_close_20",
    "future_close_40",
    "future_high_20",
    "future_low_20",
}
ENTRY_SCORING_FEATURE_COLUMNS = frozenset(discovery.SIGNAL_FEATURE_COLUMNS)

REQUIRED_ARTIFACTS = (
    "evaluation_contract.json",
    "run_manifest.json",
    "feature_availability_audit.json",
    "daily_decision_ledger.jsonl",
    "trade_ledger.jsonl",
    "failure_analysis.json",
    "replay_summary.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)


@dataclass
class Position:
    trade_id: str
    code: str
    entry_decision_ymd: int
    entry_ymd: int
    entry_price: float
    quantity: float
    capital: float
    entry_score: int
    entry_score_components: list[dict[str, Any]]
    entry_features: dict[str, str]
    holding_days: int = 0
    pending_exit: dict[str, Any] | None = None


@dataclass
class ReplayState:
    cash: float
    positions: dict[str, Position] = field(default_factory=dict)
    pending_entries: dict[int, list[dict[str, Any]]] = field(default_factory=dict)
    pending_exits: dict[int, list[dict[str, Any]]] = field(default_factory=dict)
    trades: list[dict[str, Any]] = field(default_factory=list)
    daily_rows: list[dict[str, Any]] = field(default_factory=list)
    equity_curve: list[dict[str, Any]] = field(default_factory=list)
    entry_failures: dict[str, int] = field(default_factory=dict)
    exit_failures: dict[str, int] = field(default_factory=dict)
    loss_fingerprint_counts: dict[str, int] = field(default_factory=dict)
    trade_seq: int = 0


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


def _stable_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(_json_text(payload).encode("utf-8")).hexdigest()


def _safe_path(value: str | Path | None, default: Path) -> Path:
    if value is None or not str(value).strip():
        return default.resolve()
    return Path(str(value)).expanduser().resolve()


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


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _inc(counter: dict[str, int], key: str) -> None:
    counter[key] = int(counter.get(key, 0)) + 1


def _profit_factor(returns: list[float]) -> float | None:
    gains = sum(value for value in returns if value > 0.0)
    losses = sum(value for value in returns if value < 0.0)
    if losses == 0.0:
        return None if gains == 0.0 else 999.0
    return float(gains / abs(losses))


def _entry_features(row: pd.Series | dict[str, Any]) -> dict[str, str]:
    return {column: str(row.get(column, f"{column}_unknown")) for column in sorted(ENTRY_SCORING_FEATURE_COLUMNS)}


def _loss_fingerprint(features: dict[str, str]) -> str:
    return "|".join(f"{column}={features.get(column, f'{column}_unknown')}" for column in LEARNING_FINGERPRINT_FIELDS)


def _max_drawdown(equity_curve: list[dict[str, Any]]) -> float | None:
    peak: float | None = None
    worst = 0.0
    for row in equity_curve:
        equity = _safe_float(row.get("equity_close"))
        if equity is None or equity <= 0.0:
            continue
        peak = equity if peak is None else max(peak, equity)
        if peak:
            worst = min(worst, equity / peak - 1.0)
    return worst


def _normalize_daily_frame(daily: pd.DataFrame) -> pd.DataFrame:
    frame = daily.copy()
    if "ymd" not in frame.columns:
        if "date" not in frame.columns:
            raise ValueError("daily frame requires date or ymd")
        frame["ymd"] = pd.to_numeric(frame["date"], errors="coerce").astype("Int64")
    if "date" not in frame.columns or not pd.api.types.is_datetime64_any_dtype(frame["date"]):
        frame["date"] = pd.to_datetime(frame["ymd"].astype(str), format="%Y%m%d")
    frame["code"] = frame["code"].astype(str)
    return frame


def build_point_in_time_feature_frame(daily: pd.DataFrame, monthly: pd.DataFrame, *, replay_start_ymd: int) -> pd.DataFrame:
    frame = _normalize_daily_frame(daily).sort_values(["code", "date"], kind="stable").copy()
    grouped = frame.groupby("code", sort=False)
    frame["history_days"] = grouped.cumcount() + 1
    frame["ma5"] = grouped["c"].transform(lambda s: s.rolling(5, min_periods=5).mean())
    if "ma20" not in frame.columns or frame["ma20"].isna().all():
        frame["ma20"] = grouped["c"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    if "ma60" not in frame.columns or frame["ma60"].isna().all():
        frame["ma60"] = grouped["c"].transform(lambda s: s.rolling(60, min_periods=60).mean())
    frame["ret20"] = grouped["c"].transform(lambda s: s / s.shift(20) - 1.0)
    frame["ma60_slope_20d"] = grouped["ma60"].transform(lambda s: s / s.shift(20) - 1.0)
    vol5 = grouped["v"].transform(lambda s: s.rolling(5, min_periods=5).mean())
    vol20 = grouped["v"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    frame["vol_ratio5_20"] = discovery._safe_div(vol5, vol20)
    frame["next_open"] = grouped["o"].shift(-1)
    frame["next_ymd"] = grouped["ymd"].shift(-1)

    frame["daily_ma_stack"] = "daily_stack_mixed"
    frame.loc[(frame["ma5"] > frame["ma20"]) & (frame["ma20"] > frame["ma60"]), "daily_ma_stack"] = "daily_bull_stack_5_20_60"
    frame.loc[(frame["ma5"] > frame["ma20"]) & (frame["ma20"] <= frame["ma60"]), "daily_ma_stack"] = "daily_near_bull_5_over_20_under_60"
    frame.loc[(frame["ma5"] <= frame["ma20"]) & (frame["ma20"] > frame["ma60"]), "daily_ma_stack"] = "daily_pullback_20_over_60"
    frame.loc[(frame["ma5"] < frame["ma20"]) & (frame["ma20"] < frame["ma60"]), "daily_ma_stack"] = "daily_bear_stack_5_20_60"
    frame["daily_ma60_slope_state"] = "daily_ma60_flat"
    frame.loc[frame["ma60_slope_20d"] >= 0.02, "daily_ma60_slope_state"] = "daily_ma60_rising"
    frame.loc[frame["ma60_slope_20d"] <= -0.02, "daily_ma60_slope_state"] = "daily_ma60_falling"
    frame["daily_ret20_state"] = discovery._bucket_return(
        frame["ret20"],
        strong_down=-0.08,
        down=-0.03,
        up=0.03,
        strong_up=0.08,
        prefix="daily20",
    )
    frame["daily_candle_state"] = discovery._candle_state(frame["o"], frame["h"], frame["l"], frame["c"], prefix="daily")
    frame["daily_volume_state"] = "daily_volume_normal"
    frame.loc[frame["vol_ratio5_20"] >= 1.6, "daily_volume_state"] = "daily_volume_expansion"
    frame.loc[frame["vol_ratio5_20"] <= 0.7, "daily_volume_state"] = "daily_volume_dry"
    strong_bull = frame["daily_candle_state"].isin({"daily_strong_bull", "daily_lower_wick_bull"})
    weak_bear = frame["daily_candle_state"].isin({"daily_strong_bear", "daily_upper_wick_warning"})
    frame["strong_bull_count_5"] = strong_bull.astype(float).groupby(frame["code"], sort=False).transform(lambda s: s.rolling(5, min_periods=5).sum())
    frame["weak_bear_count_5"] = weak_bear.astype(float).groupby(frame["code"], sort=False).transform(lambda s: s.rolling(5, min_periods=5).sum())
    frame["daily_sequence_state"] = "daily_sequence_mixed"
    frame.loc[(frame["strong_bull_count_5"] >= 2) & (frame["weak_bear_count_5"] <= 1), "daily_sequence_state"] = "daily_sequence_bullish"
    frame.loc[frame["weak_bear_count_5"] >= 2, "daily_sequence_state"] = "daily_sequence_warning"
    frame["anchor_month"] = frame["date"].dt.to_period("M").astype(str)
    frame["week_key"] = frame["date"].dt.to_period("W-FRI").astype(str)
    frame["month_key"] = frame["date"].dt.to_period("M").astype(str)

    monthly_work = monthly.copy()
    if "month_key" not in monthly_work.columns:
        if "month_date" not in monthly_work.columns:
            monthly_work["month_date"] = pd.to_datetime(monthly_work["ymd"].astype(str), format="%Y%m%d")
        monthly_work["month_key"] = monthly_work["month_date"].dt.to_period("M")
    weekly_features = discovery.build_weekly_feature_frame(frame)
    monthly_features = discovery.build_monthly_feature_frame(monthly_work)
    frame = frame.merge(weekly_features, left_on=["code", "week_key"], right_on=["code", "effective_week_key"], how="left")
    frame = frame.merge(monthly_features, left_on=["code", "month_key"], right_on=["code", "effective_month_key"], how="left")
    for column in ENTRY_SCORING_FEATURE_COLUMNS:
        if column in frame.columns:
            frame[column] = frame[column].fillna(f"{column}_unknown").astype(str)

    eligible = frame[
        (frame["ymd"] >= int(replay_start_ymd))
        & (frame["history_days"] >= discovery.MIN_HISTORY_DAYS)
        & frame["o"].notna()
        & frame["c"].notna()
    ].copy()
    guard_mask = loss_guard._composite_downside_risk(eligible).fillna(False).astype(bool) if not eligible.empty else pd.Series(dtype=bool)
    eligible["downside_guard_blocked"] = guard_mask.to_numpy() if len(guard_mask) else False
    return eligible.sort_values(["ymd", "code"], kind="stable").reset_index(drop=True)


def score_entry_candidate(row: pd.Series | dict[str, Any], *, entry_score_threshold: int = ENTRY_SCORE_THRESHOLD) -> dict[str, Any]:
    get = row.get
    components: list[dict[str, Any]] = []

    def add(name: str, value: Any, points: int) -> None:
        components.append({"feature": name, "value": value, "points": int(points)})

    daily_stack = str(get("daily_ma_stack", ""))
    if daily_stack == "daily_bull_stack_5_20_60":
        add("daily_ma_stack", daily_stack, 3)
    elif daily_stack == "daily_pullback_20_over_60":
        add("daily_ma_stack", daily_stack, 2)
    elif daily_stack == "daily_near_bull_5_over_20_under_60":
        add("daily_ma_stack", daily_stack, 1)
    elif daily_stack == "daily_bear_stack_5_20_60":
        add("daily_ma_stack", daily_stack, -3)

    ma60 = str(get("daily_ma60_slope_state", ""))
    add("daily_ma60_slope_state", ma60, 2 if ma60 == "daily_ma60_rising" else -2 if ma60 == "daily_ma60_falling" else 0)

    ret20 = str(get("daily_ret20_state", ""))
    if ret20 == "daily20_strong_up":
        add("daily_ret20_state", ret20, 2)
    elif ret20 == "daily20_up":
        add("daily_ret20_state", ret20, 1)
    elif ret20 in {"daily20_down", "daily20_strong_down"}:
        add("daily_ret20_state", ret20, -2)

    candle = str(get("daily_candle_state", ""))
    if candle in {"daily_strong_bull", "daily_lower_wick_bull"}:
        add("daily_candle_state", candle, 2)
    elif candle == "daily_upper_wick_warning":
        add("daily_candle_state", candle, -1)
    elif candle == "daily_strong_bear":
        add("daily_candle_state", candle, -2)

    volume = str(get("daily_volume_state", ""))
    add("daily_volume_state", volume, 1 if volume == "daily_volume_expansion" else -1 if volume == "daily_volume_dry" else 0)

    sequence = str(get("daily_sequence_state", ""))
    add("daily_sequence_state", sequence, 1 if sequence == "daily_sequence_bullish" else -1 if sequence == "daily_sequence_warning" else 0)

    weekly_trend = str(get("weekly_trend_state", ""))
    if weekly_trend == "weekly_uptrend":
        add("weekly_trend_state", weekly_trend, 2)
    elif weekly_trend == "weekly_recovery":
        add("weekly_trend_state", weekly_trend, 1)
    elif weekly_trend == "weekly_downtrend":
        add("weekly_trend_state", weekly_trend, -2)

    weekly_ret4 = str(get("weekly_ret4_state", ""))
    if weekly_ret4 in {"weekly4_up", "weekly4_strong_up"}:
        add("weekly_ret4_state", weekly_ret4, 1)
    elif weekly_ret4 in {"weekly4_down", "weekly4_strong_down"}:
        add("weekly_ret4_state", weekly_ret4, -1)

    monthly_trend = str(get("monthly_trend_state", ""))
    if monthly_trend == "monthly_uptrend":
        add("monthly_trend_state", monthly_trend, 2)
    elif monthly_trend == "monthly_recovery":
        add("monthly_trend_state", monthly_trend, 1)
    elif monthly_trend == "monthly_downtrend":
        add("monthly_trend_state", monthly_trend, -2)

    monthly_ret6 = str(get("monthly_ret6_state", ""))
    if monthly_ret6 in {"monthly6_up", "monthly6_strong_up"}:
        add("monthly_ret6_state", monthly_ret6, 1)
    elif monthly_ret6 in {"monthly6_down", "monthly6_strong_down"}:
        add("monthly_ret6_state", monthly_ret6, -1)

    blocked = bool(get("downside_guard_blocked", False))
    raw_score = int(sum(item["points"] for item in components))
    return {
        "score": raw_score,
        "entry_allowed_by_score": raw_score >= int(entry_score_threshold),
        "entry_score_threshold": int(entry_score_threshold),
        "downside_guard_blocked": blocked,
        "components": components,
        "scoring_feature_columns": sorted(ENTRY_SCORING_FEATURE_COLUMNS),
    }


def build_feature_availability_audit(features: pd.DataFrame) -> dict[str, Any]:
    overlap = sorted(ENTRY_SCORING_FEATURE_COLUMNS & LABEL_COLUMNS)
    return {
        "schema_version": f"{SCHEMA_PREFIX}_feature_availability_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "feature_rows": int(len(features)),
        "symbol_count": int(features["code"].nunique()) if not features.empty else 0,
        "decision_day_count": int(features["ymd"].nunique()) if not features.empty else 0,
        "entry_scoring_feature_columns": sorted(ENTRY_SCORING_FEATURE_COLUMNS),
        "label_columns_excluded_from_scoring": sorted(LABEL_COLUMNS),
        "signal_label_overlap": overlap,
        "used_future_labels_in_scoring": bool(overlap),
        "weekly_source": "derived_from_daily_bars_previous_completed_week",
        "monthly_source": "monthly_bars_previous_completed_month",
        "downside_guard_source": loss_guard.PRIMARY_GUARD_ID,
        "no_lookahead_pass": not overlap,
        "silent_fallback_used": False,
    }


def _execute_pending_exits(
    state: ReplayState,
    ymd: int,
    rows_by_code: dict[str, pd.Series],
    *,
    learning_mode: str,
) -> None:
    for event in state.pending_exits.pop(int(ymd), []):
        position = state.positions.pop(str(event["code"]), None)
        if position is None:
            continue
        day_row = rows_by_code.get(position.code)
        exit_price = _safe_float(day_row.get("o")) if day_row is not None else None
        if exit_price is None or exit_price <= 0.0:
            state.positions[position.code] = position
            _inc(state.exit_failures, "missing_exit_open")
            continue
        proceeds = position.quantity * exit_price
        pnl = proceeds - position.capital
        trade_return = exit_price / position.entry_price - 1.0
        state.cash += proceeds
        if learning_mode == LEARNING_MODE_LOSS_FINGERPRINT_GUARD and trade_return < 0.0:
            fingerprint = _loss_fingerprint(position.entry_features)
            state.loss_fingerprint_counts[fingerprint] = int(state.loss_fingerprint_counts.get(fingerprint, 0)) + 1
        state.trades.append(
            {
                "schema_version": f"{SCHEMA_PREFIX}_trade_ledger_row_v1",
                "trade_id": position.trade_id,
                "code": position.code,
                "entry_decision_ymd": position.entry_decision_ymd,
                "entry_ymd": position.entry_ymd,
                "exit_signal_ymd": int(event["exit_signal_ymd"]),
                "exit_ymd": int(ymd),
                "entry_price": position.entry_price,
                "exit_price": exit_price,
                "quantity": position.quantity,
                "capital": position.capital,
                "pnl_jpy": pnl,
                "return": trade_return,
                "holding_trading_days": int(event["holding_trading_days"]),
                "exit_reason": event["exit_reason"],
                "severe_loss_trade": trade_return <= SEVERE_LOSS_THRESHOLD,
                "entry_score": position.entry_score,
                "entry_score_components": position.entry_score_components,
                "entry_features": position.entry_features,
                "loss_fingerprint": _loss_fingerprint(position.entry_features),
                "silent_fallback_used": False,
            }
        )


def _execute_pending_entries(state: ReplayState, ymd: int, rows_by_code: dict[str, pd.Series]) -> list[str]:
    executed: list[str] = []
    for event in state.pending_entries.pop(int(ymd), []):
        code = str(event["code"])
        if code in state.positions:
            _inc(state.entry_failures, "position_already_open_at_fill")
            continue
        if len(state.positions) >= MAX_POSITIONS:
            _inc(state.entry_failures, "max_positions_full_at_fill")
            continue
        day_row = rows_by_code.get(code)
        entry_price = _safe_float(day_row.get("o")) if day_row is not None else None
        if entry_price is None or entry_price <= 0.0:
            _inc(state.entry_failures, "missing_entry_open")
            continue
        capital = min(PER_POSITION_CAPITAL_JPY, state.cash)
        if capital <= 0.0:
            _inc(state.entry_failures, "insufficient_cash_at_fill")
            continue
        state.trade_seq += 1
        trade_id = f"{AXIS_ID}-{state.trade_seq:06d}"
        state.cash -= capital
        state.positions[code] = Position(
            trade_id=trade_id,
            code=code,
            entry_decision_ymd=int(event["decision_ymd"]),
            entry_ymd=int(ymd),
            entry_price=entry_price,
            quantity=capital / entry_price,
            capital=capital,
            entry_score=int(event["score"]),
            entry_score_components=list(event["score_components"]),
            entry_features=dict(event["entry_features"]),
        )
        executed.append(code)
    return executed


def _schedule_exits(
    state: ReplayState,
    decision_ymd: int,
    day_rows: pd.DataFrame,
    *,
    profit_target: float,
    stop_loss: float,
    max_holding_trading_days: int,
) -> list[dict[str, Any]]:
    scheduled: list[dict[str, Any]] = []
    row_by_code = {str(row["code"]): row for _idx, row in day_rows.iterrows()}
    for code, position in list(state.positions.items()):
        if position.pending_exit is not None:
            continue
        row = row_by_code.get(code)
        if row is None:
            _inc(state.exit_failures, "missing_position_close")
            continue
        close = _safe_float(row.get("c"))
        next_open = _safe_float(row.get("next_open"))
        next_ymd = _safe_float(row.get("next_ymd"))
        if close is None:
            _inc(state.exit_failures, "missing_position_close")
            continue
        position.holding_days += 1
        close_return = close / position.entry_price - 1.0
        exit_reason: str | None = None
        if close_return >= float(profit_target):
            exit_reason = "profit_target"
        elif close_return <= float(stop_loss):
            exit_reason = "stop_loss"
        elif position.holding_days >= int(max_holding_trading_days):
            exit_reason = "time_stop"
        if exit_reason is None:
            continue
        if next_open is None or next_ymd is None:
            _inc(state.exit_failures, f"{exit_reason}_no_next_open")
            continue
        event = {
            "code": code,
            "exit_signal_ymd": int(decision_ymd),
            "execution_ymd": int(next_ymd),
            "exit_reason": exit_reason,
            "close_return_at_signal": close_return,
            "holding_trading_days": position.holding_days,
        }
        position.pending_exit = event
        state.pending_exits.setdefault(int(next_ymd), []).append(event)
        scheduled.append(event)
    return scheduled


def _schedule_entries(
    state: ReplayState,
    decision_ymd: int,
    day_rows: pd.DataFrame,
    *,
    entry_score_threshold: int,
    learning_mode: str,
) -> tuple[list[dict[str, Any]], list[str]]:
    candidates: list[dict[str, Any]] = []
    failures: list[str] = []
    projected_open = len(state.positions) - sum(1 for pos in state.positions.values() if pos.pending_exit is not None)
    for _idx, row in day_rows.iterrows():
        code = str(row["code"])
        score = score_entry_candidate(row, entry_score_threshold=entry_score_threshold)
        reason: str | None = None
        if code in state.positions:
            reason = "existing_position"
        elif score["downside_guard_blocked"]:
            reason = "downside_guard_blocked"
        elif not score["entry_allowed_by_score"]:
            reason = "score_below_threshold"
        elif learning_mode == LEARNING_MODE_LOSS_FINGERPRINT_GUARD:
            fingerprint = _loss_fingerprint(_entry_features(row))
            if int(state.loss_fingerprint_counts.get(fingerprint, 0)) > 0:
                reason = "loss_fingerprint_blocked"
        if reason is None and (_safe_float(row.get("next_open")) is None or _safe_float(row.get("next_ymd")) is None):
            reason = "no_next_open"
        if reason is not None:
            _inc(state.entry_failures, reason)
            failures.append(reason)
            continue
        candidates.append(
            {
                "code": code,
                "decision_ymd": int(decision_ymd),
                "execution_ymd": int(row["next_ymd"]),
                "score": int(score["score"]),
                "score_components": score["components"],
                "entry_features": _entry_features(row),
            }
        )
    candidates.sort(key=lambda item: (-int(item["score"]), str(item["code"])))
    slots = max(0, MAX_POSITIONS - projected_open)
    scheduled: list[dict[str, Any]] = []
    for candidate in candidates:
        if slots <= 0:
            _inc(state.entry_failures, "max_positions_full")
            failures.append("max_positions_full")
            continue
        if state.cash <= 0.0:
            _inc(state.entry_failures, "insufficient_cash")
            failures.append("insufficient_cash")
            continue
        state.pending_entries.setdefault(int(candidate["execution_ymd"]), []).append(candidate)
        scheduled.append(candidate)
        slots -= 1
    return scheduled, failures


def _mark_to_market_equity(state: ReplayState, day_rows: pd.DataFrame) -> float:
    row_by_code = {str(row["code"]): row for _idx, row in day_rows.iterrows()}
    open_value = 0.0
    for position in state.positions.values():
        row = row_by_code.get(position.code)
        close = _safe_float(row.get("c")) if row is not None else None
        price = close if close is not None and close > 0.0 else position.entry_price
        open_value += position.quantity * price
    return float(state.cash + open_value)


def replay_daily_selection(
    features: pd.DataFrame,
    *,
    initial_capital: float = INITIAL_CAPITAL_JPY,
    entry_score_threshold: int = ENTRY_SCORE_THRESHOLD,
    learning_mode: str = LEARNING_MODE_NONE,
    profit_target: float = PROFIT_TARGET,
    stop_loss: float = STOP_LOSS,
    max_holding_trading_days: int = MAX_HOLDING_TRADING_DAYS,
) -> dict[str, Any]:
    state = ReplayState(cash=float(initial_capital))
    if features.empty:
        return {
            "daily_decision_rows": [],
            "trade_rows": [],
            "equity_curve": [],
            "failure_analysis": {
                "entry_failure_counts": {},
                "exit_failure_counts": {},
                "open_position_count": 0,
                "silent_fallback_used": False,
            },
        }

    for ymd, day_rows in features.groupby("ymd", sort=True):
        day_rows = day_rows.sort_values("code", kind="stable")
        rows_by_code = {str(row["code"]): row for _idx, row in day_rows.iterrows()}
        _execute_pending_exits(state, int(ymd), rows_by_code, learning_mode=learning_mode)
        filled_entries = _execute_pending_entries(state, int(ymd), rows_by_code)
        scheduled_exits = _schedule_exits(
            state,
            int(ymd),
            day_rows,
            profit_target=float(profit_target),
            stop_loss=float(stop_loss),
            max_holding_trading_days=int(max_holding_trading_days),
        )
        scheduled_entries, failures = _schedule_entries(
            state,
            int(ymd),
            day_rows,
            entry_score_threshold=int(entry_score_threshold),
            learning_mode=learning_mode,
        )
        equity = _mark_to_market_equity(state, day_rows)
        state.equity_curve.append({"ymd": int(ymd), "equity_close": equity, "cash_close": state.cash})
        state.daily_rows.append(
            {
                "schema_version": f"{SCHEMA_PREFIX}_daily_decision_row_v1",
                "decision_ymd": int(ymd),
                "candidate_count": int(len(day_rows)),
                "filled_entry_symbols": filled_entries,
                "scheduled_entry_symbols": [item["code"] for item in scheduled_entries],
                "scheduled_exit_symbols": [item["code"] for item in scheduled_exits],
                "held_symbols_after_close": sorted(state.positions.keys()),
                "open_position_count_after_close": len(state.positions),
                "cash_after_close": state.cash,
                "equity_after_close": equity,
                "entry_failure_reasons_seen": sorted(set(failures)),
                "learning_mode": learning_mode,
                "loss_fingerprint_memory_size": len(state.loss_fingerprint_counts),
                "silent_fallback_used": False,
            }
        )

    failure_analysis = {
        "schema_version": f"{SCHEMA_PREFIX}_failure_analysis_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "entry_failure_counts": dict(sorted(state.entry_failures.items())),
        "exit_failure_counts": dict(sorted(state.exit_failures.items())),
        "loss_fingerprint_counts": dict(sorted(state.loss_fingerprint_counts.items())),
        "learning_mode": learning_mode,
        "open_position_count": len(state.positions),
        "open_positions": [
            {
                "trade_id": position.trade_id,
                "code": position.code,
                "entry_ymd": position.entry_ymd,
                "holding_days": position.holding_days,
                "pending_exit": position.pending_exit,
            }
            for position in state.positions.values()
        ],
        "silent_fallback_used": False,
    }
    return {
        "daily_decision_rows": state.daily_rows,
        "trade_rows": state.trades,
        "equity_curve": state.equity_curve,
        "failure_analysis": failure_analysis,
    }


def build_replay_summary(replay: dict[str, Any], *, initial_capital: float) -> dict[str, Any]:
    trades = list(replay["trade_rows"])
    returns = [float(row["return"]) for row in trades if _safe_float(row.get("return")) is not None]
    pnl = [float(row["pnl_jpy"]) for row in trades if _safe_float(row.get("pnl_jpy")) is not None]
    closed_count = len(returns)
    realized_return = sum(pnl) / float(initial_capital) if initial_capital else None
    summary = {
        "schema_version": f"{SCHEMA_PREFIX}_replay_summary_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "initial_capital_jpy": float(initial_capital),
        "closed_trade_count": closed_count,
        "realized_pnl_jpy": sum(pnl),
        "realized_portfolio_return": realized_return,
        "win_rate": (sum(1 for value in returns if value > 0.0) / closed_count) if closed_count else None,
        "profit_factor": _profit_factor(returns),
        "max_drawdown": _max_drawdown(list(replay["equity_curve"])),
        "severe_loss_trade_rate": (sum(1 for value in returns if value <= SEVERE_LOSS_THRESHOLD) / closed_count) if closed_count else None,
        "exit_reason_counts": {
            reason: sum(1 for row in trades if row.get("exit_reason") == reason)
            for reason in sorted({str(row.get("exit_reason")) for row in trades})
        },
        "open_position_count": int(replay["failure_analysis"]["open_position_count"]),
        "learning_mode": replay["failure_analysis"].get("learning_mode", LEARNING_MODE_NONE),
        "loss_fingerprint_memory_size": len(replay["failure_analysis"].get("loss_fingerprint_counts") or {}),
        "silent_fallback_used": False,
    }
    return summary


def apply_decision_policy(summary: dict[str, Any], feature_audit: dict[str, Any]) -> dict[str, Any]:
    closed_trade_count = int(summary.get("closed_trade_count") or 0)
    realized_return = _safe_float(summary.get("realized_portfolio_return")) or 0.0
    win_rate = _safe_float(summary.get("win_rate")) or 0.0
    profit_factor = _safe_float(summary.get("profit_factor")) or 0.0
    max_drawdown = _safe_float(summary.get("max_drawdown"))
    severe_rate = _safe_float(summary.get("severe_loss_trade_rate")) or 0.0
    no_lookahead_pass = bool(feature_audit.get("no_lookahead_pass")) and not bool(feature_audit.get("used_future_labels_in_scoring"))
    silent_fallback_used = bool(summary.get("silent_fallback_used")) or bool(feature_audit.get("silent_fallback_used"))
    gates = {
        "realized_portfolio_return_gt_0": realized_return > 0.0,
        "win_rate_gte_0_55": win_rate >= 0.55,
        "profit_factor_gte_1_25": profit_factor >= 1.25,
        "max_drawdown_gte_minus_0_12": (max_drawdown is not None and max_drawdown >= -0.12),
        "severe_loss_trade_rate_lte_0_10": severe_rate <= 0.10,
        "min_10_closed_trades": closed_trade_count >= 10,
        "no_silent_fallback": not silent_fallback_used,
        "no_lookahead_pass": no_lookahead_pass,
    }
    risk_gate_names = (
        "win_rate_gte_0_55",
        "profit_factor_gte_1_25",
        "max_drawdown_gte_minus_0_12",
        "severe_loss_trade_rate_lte_0_10",
    )
    failed_risk_gates = [name for name in risk_gate_names if not gates[name]]
    if all(gates.values()):
        decision = "keep"
        reason = "winning_state"
        winning_state = True
    elif gates["realized_portfolio_return_gt_0"] and gates["no_silent_fallback"] and gates["no_lookahead_pass"] and (
        not gates["min_10_closed_trades"] or len(failed_risk_gates) == 1
    ):
        decision = "hold"
        reason = "positive_but_insufficient_sample_or_one_risk_gate_failed"
        winning_state = False
    else:
        decision = "drop"
        reason = "decision_policy_failed"
        winning_state = False
    return {
        "decision": decision,
        "candidate_local_decision": decision,
        "session_aggregate_decision": decision,
        "authoritative_research_decision": decision,
        "authoritative_reason": reason,
        "winning_state_achieved": winning_state,
        "decision_gates": gates,
        "failed_risk_gates": failed_risk_gates,
        "silent_fallback_used": silent_fallback_used,
        "no_lookahead_pass": no_lookahead_pass,
    }


def build_evaluation_contract(
    *,
    source_db: Path,
    start_ymd: int,
    end_ymd: int,
    entry_score_threshold: int,
    learning_mode: str,
    profit_target: float,
    stop_loss: float,
    max_holding_trading_days: int,
) -> dict[str, Any]:
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_evaluation_contract_v1",
        "axis_id": AXIS_ID,
        "boundary": "TRADEX-only",
        "research_phase": "daily point-in-time replay learning",
        "source_db": str(source_db),
        "start_ymd": int(start_ymd),
        "end_ymd": int(end_ymd),
        "universe": "all daily_bars symbols where source=PAN in source DB",
        "portfolio_assumptions": {
            "initial_capital_jpy": INITIAL_CAPITAL_JPY,
            "max_positions": MAX_POSITIONS,
            "per_position_capital_jpy": PER_POSITION_CAPITAL_JPY,
            "fill_policy": "next_session_open",
            "same_open_turnover_cash_reuse": False,
            "cost_model": contracts.TRADEX_DEFAULT_COST_MODEL,
        },
        "entry_policy": {
            "side": "long_only",
            "entry_score_threshold": int(entry_score_threshold),
            "weights": "fixed_predeclared_no_learning_no_tuning",
            "scoring_feature_columns": sorted(ENTRY_SCORING_FEATURE_COLUMNS),
            "downside_guard_id": loss_guard.PRIMARY_GUARD_ID,
        },
        "learning_policy": {
            "learning_mode": learning_mode,
            "online_only": learning_mode == LEARNING_MODE_LOSS_FINGERPRINT_GUARD,
            "fingerprint_fields": list(LEARNING_FINGERPRINT_FIELDS),
            "updates_from": "closed_losing_trades_only_after_exit_execution",
            "future_labels_used_for_learning_updates": False,
        },
        "exit_rules": {
            "profit_target": float(profit_target),
            "stop_loss": float(stop_loss),
            "max_holding_trading_days": int(max_holding_trading_days),
            "decision_price": "close",
            "execution_price": "next_session_open_if_available",
        },
        "future_label_policy": {
            "future_labels_used_for_entry_scoring": False,
            "future_labels_used_for_selection": False,
            "future_prices_used_for_execution_and_evaluation": True,
        },
        "same_condition_controls": {
            "same_universe_source": "runtime snapshot daily_bars PAN source",
            "same_period": True,
            "same_top_k": MAX_POSITIONS,
            "same_regime_condition": "all_available_pan_daily_bars_from_start_ymd",
            "same_cost_slippage": contracts.TRADEX_DEFAULT_COST_MODEL,
            "artifact_detail_level": contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        },
        "meemee_reflection_allowed": False,
        "publish_bundle_allowed": False,
        "silent_fallback_used": False,
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def _artifact_complete(output_dir: Path, paths: dict[str, str], decision: dict[str, Any]) -> dict[str, Any]:
    existing = {name: Path(path).exists() for name, path in paths.items()}
    required_existing = {name: (output_dir / name).exists() for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json"}
    return {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "artifact_root": str(output_dir),
        "required_artifacts": list(REQUIRED_ARTIFACTS),
        "existing_artifacts": {**existing, **required_existing},
        "complete": all(existing.values()) and all(required_existing.values()),
        "candidate_local_decision": decision["candidate_local_decision"],
        "session_aggregate_decision": decision["session_aggregate_decision"],
        "authoritative_research_decision": decision["authoritative_research_decision"],
        "silent_fallback_used": False,
        "meemee_reflectable": False,
        "publish_bundle_created": False,
    }


def _load_inputs(source_path: Path, *, start_ymd: int, end_ymd: int | None) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    conn = duckdb.connect(str(source_path), read_only=True)
    try:
        max_daily_ymd = discovery._load_max_daily_ymd(conn)
        effective_end_ymd = min(int(end_ymd), int(max_daily_ymd)) if end_ymd else int(max_daily_ymd)
        data_start_ts = discovery._ymd_to_timestamp(start_ymd) - pd.DateOffset(days=520)
        data_start_ymd = discovery._timestamp_to_ymd(data_start_ts)
        daily = discovery._load_daily_rows(conn, start_ymd=data_start_ymd, end_ymd=effective_end_ymd)
        monthly = discovery._load_monthly_rows(conn, start_ymd=data_start_ymd, end_ymd=effective_end_ymd)
    finally:
        conn.close()
    return daily, monthly, effective_end_ymd


def run_daily_selection_replay_learning_v1(
    *,
    source_db: str | Path | None = None,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    run_id: str | None = None,
    start_ymd: int = DEFAULT_START_YMD,
    end_ymd: int | None = None,
    entry_score_threshold: int = ENTRY_SCORE_THRESHOLD,
    learning_mode: str = LEARNING_MODE_NONE,
    profit_target: float = PROFIT_TARGET,
    stop_loss: float = STOP_LOSS,
    max_holding_trading_days: int = MAX_HOLDING_TRADING_DAYS,
) -> dict[str, Any]:
    source_path = _resolve_source_db(source_db)
    output_base = _safe_path(output_root, DEFAULT_OUTPUT_ROOT)
    run_name = run_id.strip() if run_id else _default_run_id()
    if not run_name.endswith(AXIS_ID):
        run_name = f"{run_name}-{AXIS_ID}"
    output_dir = output_base / run_name
    output_dir.mkdir(parents=True, exist_ok=True)

    daily, monthly, effective_end_ymd = _load_inputs(source_path, start_ymd=start_ymd, end_ymd=end_ymd)
    features = build_point_in_time_feature_frame(daily, monthly, replay_start_ymd=start_ymd)
    feature_audit = build_feature_availability_audit(features)
    replay = replay_daily_selection(
        features,
        initial_capital=INITIAL_CAPITAL_JPY,
        entry_score_threshold=int(entry_score_threshold),
        learning_mode=learning_mode,
        profit_target=float(profit_target),
        stop_loss=float(stop_loss),
        max_holding_trading_days=int(max_holding_trading_days),
    )
    summary = build_replay_summary(replay, initial_capital=INITIAL_CAPITAL_JPY)
    policy = apply_decision_policy(summary, feature_audit)
    evaluation_contract = build_evaluation_contract(
        source_db=source_path,
        start_ymd=start_ymd,
        end_ymd=effective_end_ymd,
        entry_score_threshold=int(entry_score_threshold),
        learning_mode=learning_mode,
        profit_target=float(profit_target),
        stop_loss=float(stop_loss),
        max_holding_trading_days=int(max_holding_trading_days),
    )
    universe = sorted(features["code"].astype(str).unique().tolist()) if not features.empty else []
    run_manifest = contracts.build_run_manifest(
        session_id=run_name,
        seed=0,
        random_seed=0,
        input_artifacts=[
            {"name": "source_db", "path": str(source_path)},
            {"name": "evaluation_contract", "contract_hash": evaluation_contract["contract_hash"]},
        ],
        asof=str(effective_end_ymd),
        config={
            "axis_id": AXIS_ID,
            "start_ymd": int(start_ymd),
            "end_ymd": int(effective_end_ymd),
            "entry_score_threshold": int(entry_score_threshold),
            "learning_mode": learning_mode,
            "profit_target": float(profit_target),
            "stop_loss": float(stop_loss),
            "max_holding_trading_days": int(max_holding_trading_days),
            "max_positions": MAX_POSITIONS,
            "candidate_scoring": "fixed_predeclared_long_only_swing_policy",
            "silent_fallback_used": False,
        },
        universe=universe,
        period={"start_date": str(start_ymd), "end_date": str(effective_end_ymd), "label": "daily_selection_replay"},
        horizon="sequential_replay_profit_stop_time_exit",
        artifact_detail_level=contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        fallback_status=contracts.TRADEX_FALLBACK_STATUS_AUTHORITATIVE,
        cost_model=contracts.TRADEX_DEFAULT_COST_MODEL,
    )
    contracts.validate_run_manifest(run_manifest)
    research_decision = {
        "schema_version": f"{SCHEMA_PREFIX}_research_decision_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "artifact_root": str(output_dir),
        **policy,
        "decision_reasons": [{"code": policy["authoritative_reason"], "value": True}],
        "metrics": {
            "realized_portfolio_return": summary["realized_portfolio_return"],
            "win_rate": summary["win_rate"],
            "profit_factor": summary["profit_factor"],
            "max_drawdown": summary["max_drawdown"],
            "severe_loss_trade_rate": summary["severe_loss_trade_rate"],
            "closed_trade_count": summary["closed_trade_count"],
            "learning_mode": learning_mode,
            "profit_target": float(profit_target),
            "stop_loss": float(stop_loss),
            "max_holding_trading_days": int(max_holding_trading_days),
        },
        "meemee_reflectable": False,
        "publish_bundle_created": False,
    }

    paths: dict[str, str] = {}
    for name, payload in {
        "evaluation_contract.json": evaluation_contract,
        "run_manifest.json": run_manifest,
        "feature_availability_audit.json": feature_audit,
        "failure_analysis.json": replay["failure_analysis"],
        "replay_summary.json": summary,
        "research_decision.json": research_decision,
    }.items():
        paths[name] = str(_write_json(output_dir / name, payload))
    paths["daily_decision_ledger.jsonl"] = str(_write_jsonl(output_dir / "daily_decision_ledger.jsonl", replay["daily_decision_rows"]))
    paths["trade_ledger.jsonl"] = str(_write_jsonl(output_dir / "trade_ledger.jsonl", replay["trade_rows"]))
    complete = _artifact_complete(output_dir, paths, research_decision)
    paths["_ARTIFACT_COMPLETE.json"] = str(_write_json(output_dir / "_ARTIFACT_COMPLETE.json", complete))

    return {
        "output_dir": str(output_dir),
        "paths": paths,
        "candidate_local_decision": research_decision["candidate_local_decision"],
        "session_aggregate_decision": research_decision["session_aggregate_decision"],
        "authoritative_research_decision": research_decision["authoritative_research_decision"],
        "metrics": research_decision["metrics"],
        "silent_fallback_used": False,
        "no_lookahead_pass": policy["no_lookahead_pass"],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-db", default="")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--run-id", default="")
    parser.add_argument("--start-ymd", type=int, default=DEFAULT_START_YMD)
    parser.add_argument("--end-ymd", type=int, default=0)
    parser.add_argument("--entry-score-threshold", type=int, default=ENTRY_SCORE_THRESHOLD)
    parser.add_argument("--learning-mode", choices=[LEARNING_MODE_NONE, LEARNING_MODE_LOSS_FINGERPRINT_GUARD], default=LEARNING_MODE_NONE)
    parser.add_argument("--profit-target", type=float, default=PROFIT_TARGET)
    parser.add_argument("--stop-loss", type=float, default=STOP_LOSS)
    parser.add_argument("--max-holding-trading-days", type=int, default=MAX_HOLDING_TRADING_DAYS)
    args = parser.parse_args(argv)
    result = run_daily_selection_replay_learning_v1(
        source_db=args.source_db.strip() or None,
        output_root=args.output_root,
        run_id=args.run_id.strip() or None,
        start_ymd=args.start_ymd,
        end_ymd=args.end_ymd or None,
        entry_score_threshold=args.entry_score_threshold,
        learning_mode=args.learning_mode,
        profit_target=args.profit_target,
        stop_loss=args.stop_loss,
        max_holding_trading_days=args.max_holding_trading_days,
    )
    print(_json_text(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
