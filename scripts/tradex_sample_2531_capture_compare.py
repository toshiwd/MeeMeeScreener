from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.tradex_sample_replay_2531 import (  # noqa: E402
    ADD_BUY_UNITS,
    BASIS_VERSION,
    DECISION_LOGIC_VERSION,
    ENTRY_BUY_UNITS,
    MAX_BUY_UNITS,
    SHARES_PER_BUY_UNIT,
    _basis_context,
    _json_text as _champion_json_text,
    _load_source_frames as _load_champion_source_frames,
    _max_drawdown_from_marks,
    _position_text,
    _safe_float,
    _safe_int,
    _write_json as _champion_write_json,
    _write_parquet as _champion_write_parquet,
    simulate_sample_replay,
)

DEFAULT_SOURCE_DB_PATH = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb")
DEFAULT_OUTPUT_DIR = Path(r"G:\Tradex\sample_replays\tradex_sample_2531_capture_compare")
DEFAULT_SYMBOL = "2531"
DEFAULT_START_DATE = "2026-01-01"
DEFAULT_END_DATE = "2026-03-31"
DEFAULT_FREEZE_DATE = "2025-12-31"
CAPTURE_OUTPUT_PREFIX = "tradex_sample_2531"
MAX_REENTRY_OPPORTUNITIES = 1
PARTIAL_TAKE_TARGET_UNITS = 2
REENTRY_ENTRY_UNITS = 2
REENTRY_ADD_UNITS = 3
CAPTURE_LOOKAHEAD_DAYS = 15
PREMATURE_EXIT_HORIZON_DAYS = 3
PREMATURE_EXIT_EXTENSION_THRESHOLD = 0.02
REENTRY_TIME_STOP_DAYS = 9
PARTIAL_TAKE_COOL_OFF_MIN_SCORE = 2.0
PARTIAL_TAKE_COOL_OFF_MAX_SCORE = 4.5


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, pd.Series):
        return _json_ready(value.to_dict())
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if value is None:
        return None
    if isinstance(value, (pd.NA.__class__,)):  # pragma: no cover - defensive for pandas scalar NA
        return None
    return value


def _json_text(payload: Any) -> str:
    return json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, default=str)


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_json_text(payload) + "\n", encoding="utf-8")
    return path


def _write_parquet(path: Path, frame: pd.DataFrame) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        frame.to_parquet(path, index=False)
    except Exception:
        conn = duckdb.connect()
        try:
            conn.register("ledger_frame", frame)
            conn.execute(f"COPY ledger_frame TO '{path.as_posix()}' (FORMAT PARQUET)")
        finally:
            conn.close()
    return path


def _ymd_to_date_text(value: int | str) -> str:
    text = str(int(value))
    return f"{text[0:4]}-{text[4:6]}-{text[6:8]}"


def _date_text_to_ymd(value: str) -> int:
    return int(str(value).replace("-", ""))


def _add_calendar_days(value: str, days: int) -> str:
    base = datetime.strptime(value, "%Y-%m-%d").date()
    return (base + timedelta(days=days)).isoformat()


def _roundtrip_id(symbol: str, decision_dt: int, sequence: int) -> str:
    return f"{symbol}_{decision_dt}_{sequence}"


def _safe_text(value: Any, fallback: str = "unknown") -> str:
    if value is None:
        return fallback
    text = str(value).strip()
    return text or fallback


def _sample_name(symbol: str) -> str:
    return f"tradex_sample_{symbol}"


def _champion_policy_id(symbol: str) -> str:
    return f"tradex_sample_{symbol}_frozen_v1"


def _challenger_policy_id(symbol: str) -> str:
    return f"tradex_sample_{symbol}_capture_v2"


def _load_replay_frames(
    *,
    source_db_path: Path,
    symbol: str,
    start_date: str,
    end_date: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    bar_end_date = _add_calendar_days(end_date, CAPTURE_LOOKAHEAD_DAYS)
    conn = duckdb.connect(str(source_db_path), read_only=True)
    try:
        basis_frame = conn.execute(
            """
            SELECT
                b.dt,
                b.code,
                b.basis_version,
                b.name,
                b.source_rank_buy,
                b.source_rank_sell,
                b.basis_payload_json,
                b.source_as_of,
                b.basis_source,
                d.reason_snapshot_json,
                d.score_snapshot_json,
                d.rank_snapshot_json,
                d.entry_qualified,
                d.setup_type
            FROM signal_basis_daily b
            LEFT JOIN signal_decision_daily d
              ON b.dt = d.dt
             AND b.code = d.code
             AND d.side = 'buy'
             AND d.logic_version = ?
             AND d.basis_version = ?
            WHERE b.code = ?
              AND b.dt BETWEEN ? AND ?
            ORDER BY b.dt
            """,
            [
                DECISION_LOGIC_VERSION,
                BASIS_VERSION,
                symbol,
                _date_text_to_ymd(start_date),
                _date_text_to_ymd(end_date),
            ],
        ).fetchdf()
        bars_frame = conn.execute(
            """
            SELECT
                CAST(strftime(to_timestamp(CAST(date AS BIGINT)), '%Y%m%d') AS INTEGER) AS dt,
                code,
                o,
                h,
                l,
                c,
                v
            FROM daily_bars
            WHERE code = ?
              AND CAST(strftime(to_timestamp(CAST(date AS BIGINT)), '%Y%m%d') AS INTEGER)
                  BETWEEN ? AND ?
            ORDER BY dt
            """,
            [symbol, _date_text_to_ymd(start_date), _date_text_to_ymd(bar_end_date)],
        ).fetchdf()
    finally:
        conn.close()

    if basis_frame.empty:
        raise RuntimeError(f"no basis rows found for symbol={symbol}")
    if bars_frame.empty:
        raise RuntimeError(f"no bar rows found for symbol={symbol}")

    basis_frame = basis_frame.copy()
    basis_frame["dt"] = pd.to_numeric(basis_frame["dt"], errors="coerce").astype("Int64")
    bars_frame = bars_frame.copy()
    bars_frame["dt"] = pd.to_numeric(bars_frame["dt"], errors="coerce").astype("Int64")
    return basis_frame, bars_frame


def _entry_gate(context: dict[str, Any]) -> bool:
    return (
        _safe_float(context.get("score_long")) >= 5.0
        and _safe_float(context.get("score_short")) <= 0.0
        and str(context.get("marketRegime")) in {"risk_on", "neutral"}
    )


def _bullish_recovery_gate(context: dict[str, Any]) -> bool:
    return str(context.get("daily_main_state_ctx")) in {"daily_reversal_up_candidate", "daily_up_mid"}


def _reentry_gate(context: dict[str, Any]) -> bool:
    return _entry_gate(context) and _bullish_recovery_gate(context)


def _continuation_gate(context: dict[str, Any]) -> bool:
    return (
        _safe_float(context.get("score_long")) >= 7.0
        and _safe_float(context.get("score_short")) <= 0.0
        and str(context.get("marketRegime")) == "risk_on"
        and any(bool(context.get(key)) for key in ("reclaim60", "v60Core", "morningStar", "bullMarubozu"))
    )


def _hard_exit_gate(context: dict[str, Any]) -> tuple[bool, str]:
    score_long = _safe_float(context.get("score_long"))
    score_short = _safe_float(context.get("score_short"))
    market_regime = str(context.get("marketRegime"))
    if score_short >= 3.0:
        return True, "short_pressure"
    if market_regime == "risk_off" and any(bool(context.get(key)) for key in ("shootingStarLike", "bearMarubozu")):
        return True, "risk_off_reversal"
    return False, "hold"


def _partial_take_gate(context: dict[str, Any], *, days_held: int) -> bool:
    score_long = _safe_float(context.get("score_long"))
    market_regime = str(context.get("marketRegime"))
    daily_state = str(context.get("daily_main_state_ctx"))
    if score_long is None:
        return False
    if not (PARTIAL_TAKE_COOL_OFF_MIN_SCORE <= score_long <= PARTIAL_TAKE_COOL_OFF_MAX_SCORE):
        return False
    if market_regime not in {"risk_on", "neutral"}:
        return False
    if days_held < 2:
        return False
    return daily_state in {"daily_reversal_up_candidate", "daily_up_mid"}


@dataclass
class ChallengerState:
    long_units: int = 0
    avg_price: float = 0.0
    realized_pnl: float = 0.0
    entered_once: bool = False
    exited_once: bool = False
    reentry_used: bool = False
    current_cycle_added_once: bool = False
    current_cycle_partial_taken_once: bool = False
    open_trade_start_index: int | None = None
    open_trade_entry_date: int | None = None
    open_trade_entry_fill_date: int | None = None
    open_trade_entry_price: float | None = None
    open_trade_entry_type: str | None = None
    cycle_start_realized_pnl: float = 0.0

    def start_cycle(self, *, decision_dt: int, execution_dt: int, entry_price: float, entry_type: str) -> None:
        self.entered_once = True
        self.current_cycle_added_once = False
        self.current_cycle_partial_taken_once = False
        self.open_trade_entry_date = decision_dt
        self.open_trade_entry_fill_date = execution_dt
        self.open_trade_entry_price = entry_price
        self.open_trade_entry_type = entry_type
        self.cycle_start_realized_pnl = self.realized_pnl


def _challenger_select_action(
    state: ChallengerState,
    context: dict[str, Any],
    *,
    decision_index: int,
) -> tuple[str, dict[str, Any]]:
    days_held = 0
    if state.open_trade_start_index is not None:
        days_held = max(0, decision_index - int(state.open_trade_start_index))
    hard_exit, hard_exit_reason = _hard_exit_gate(context)
    if state.long_units > 0:
        if (
            state.open_trade_entry_type == "reentry"
            and days_held >= REENTRY_TIME_STOP_DAYS
            and state.long_units > 0
        ):
            return "long_exit", {
                "gate": "reentry_time_stop",
                "reason": "reentry_hold_time_stop",
                "days_held": days_held,
            }
        if hard_exit and state.long_units <= PARTIAL_TAKE_TARGET_UNITS:
            return "long_exit", {"gate": "exit", "reason": hard_exit_reason, "days_held": days_held}
        if state.long_units == PARTIAL_TAKE_TARGET_UNITS:
            if _safe_float(context.get("score_long")) <= 0.0 or (days_held >= 10 and _safe_float(context.get("score_long")) < 5.0):
                return "long_exit", {"gate": "exit", "reason": "soft_exit_after_reduced_size", "days_held": days_held}
            if not state.current_cycle_added_once and _continuation_gate(context):
                return "long_add", {
                    "gate": "continuation_add",
                "reason": "confirmed_continuation",
                "days_held": days_held,
            }
            return "stay", {"gate": "hold", "reason": "continue_reduced_position", "days_held": days_held}
        if (
            state.long_units == MAX_BUY_UNITS
            and not state.current_cycle_partial_taken_once
            and _partial_take_gate(context, days_held=days_held)
        ):
            return "partial_take_long", {
                "gate": "partial_take",
                "reason": "capture_density_before_full_exit",
                "days_held": days_held,
            }
        if hard_exit and state.long_units == MAX_BUY_UNITS:
            return "long_exit", {"gate": "exit", "reason": hard_exit_reason, "days_held": days_held}
        if state.long_units == MAX_BUY_UNITS and _safe_float(context.get("score_long")) <= 0.0:
            return "partial_take_long", {
                "gate": "partial_take",
                "reason": "weak_long_score_before_full_exit",
                "days_held": days_held,
            }
        return "stay", {"gate": "hold", "reason": "continue_position", "days_held": days_held}
    if state.exited_once and not state.reentry_used and _reentry_gate(context):
        return "long_entry", {"gate": "reentry", "reason": "reentry_after_full_exit", "entry_type": "reentry"}
    if not state.entered_once and _entry_gate(context):
        return "long_entry", {"gate": "entry", "reason": "initial_entry", "entry_type": "initial"}
    return "stay", {"gate": "no_trade", "reason": "frozen_hold"}


def _apply_challenger_action(
    state: ChallengerState,
    action: str,
    execution_price: float,
    *,
    decision_date: int,
    execution_date: int,
    decision_index: int,
) -> dict[str, Any]:
    before_units = state.long_units
    before_avg = state.avg_price
    event: dict[str, Any] = {
        "action": action,
        "decision_date": decision_date,
        "decision_index": decision_index,
        "execution_date": execution_date,
        "execution_price": execution_price,
        "before_units": before_units,
        "after_units": before_units,
        "before_avg_price": before_avg,
        "after_avg_price": before_avg,
        "realized_pnl_delta": 0.0,
        "entry_kind": None,
    }
    if action == "long_entry" and state.long_units == 0:
        entry_kind = "reentry" if state.exited_once else "initial"
        state.start_cycle(
            decision_dt=decision_date,
            execution_dt=execution_date,
            entry_price=execution_price,
            entry_type=entry_kind,
        )
        state.long_units = REENTRY_ENTRY_UNITS
        state.avg_price = execution_price
        state.open_trade_start_index = decision_index
        event["after_units"] = state.long_units
        event["after_avg_price"] = state.avg_price
        event["entry_kind"] = entry_kind
        event["entry_fill"] = True
        if entry_kind == "reentry":
            state.reentry_used = True
        return event
    if action == "long_add" and state.long_units > 0:
        new_units = state.long_units + REENTRY_ADD_UNITS
        new_avg = (state.avg_price * state.long_units + execution_price * REENTRY_ADD_UNITS) / new_units
        state.long_units = new_units
        state.avg_price = new_avg
        state.current_cycle_added_once = True
        event["after_units"] = state.long_units
        event["after_avg_price"] = state.avg_price
        event["add_fill"] = True
        return event
    if action == "partial_take_long" and state.long_units > PARTIAL_TAKE_TARGET_UNITS:
        sold_units = state.long_units - PARTIAL_TAKE_TARGET_UNITS
        realized_delta = (execution_price - state.avg_price) * sold_units * SHARES_PER_BUY_UNIT
        state.realized_pnl += realized_delta
        state.long_units = PARTIAL_TAKE_TARGET_UNITS
        state.current_cycle_partial_taken_once = True
        event["after_units"] = state.long_units
        event["realized_pnl_delta"] = realized_delta
        event["partial_take_fill"] = True
        return event
    if action == "long_exit" and state.long_units > 0:
        realized_delta = (execution_price - state.avg_price) * state.long_units * SHARES_PER_BUY_UNIT
        state.realized_pnl += realized_delta
        state.long_units = 0
        state.avg_price = 0.0
        state.exited_once = True
        state.open_trade_start_index = None
        state.open_trade_entry_date = None
        state.open_trade_entry_fill_date = None
        state.open_trade_entry_price = None
        state.open_trade_entry_type = None
        event["after_units"] = state.long_units
        event["after_avg_price"] = state.avg_price
        event["realized_pnl_delta"] = realized_delta
        event["exit_fill"] = True
        return event
    return event


def _max_drawdown_from_equity(values: list[float]) -> float:
    if not values:
        return 0.0
    peak = values[0]
    worst_drawdown = 0.0
    for value in values:
        peak = max(peak, value)
        worst_drawdown = min(worst_drawdown, value - peak)
    return float(worst_drawdown)


def _build_action_reason_payload(context: dict[str, Any]) -> dict[str, list[str]]:
    payload = context.get("reason_codes")
    if isinstance(payload, dict):
        positive = [str(value) for value in payload.get("positive") or []]
        negative = [str(value) for value in payload.get("negative") or []]
        return {"positive": positive, "negative": negative}
    return {"positive": [], "negative": []}


def _simulate_challenger_replay(
    *,
    source_db_path: Path,
    basis_frame: pd.DataFrame,
    bars_frame: pd.DataFrame,
    symbol: str,
    start_date: str,
    end_date: str,
) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any]]:
    basis_frame = basis_frame.copy()
    bars_frame = bars_frame.copy()
    basis_frame["dt"] = pd.to_numeric(basis_frame["dt"], errors="coerce").astype("Int64")
    bars_frame["dt"] = pd.to_numeric(bars_frame["dt"], errors="coerce").astype("Int64")

    decision_start = _date_text_to_ymd(start_date)
    decision_end = _date_text_to_ymd(end_date)
    basis_frame = basis_frame.loc[(basis_frame["dt"] >= decision_start) & (basis_frame["dt"] <= decision_end)].copy()
    basis_frame = basis_frame.sort_values("dt").reset_index(drop=True)
    if basis_frame.empty:
        raise RuntimeError("basis frame is empty after date filtering")

    bar_frame = bars_frame.loc[bars_frame["dt"].notna()].copy().sort_values("dt").reset_index(drop=True)
    bar_frame = bar_frame.drop_duplicates(subset=["dt"], keep="last")
    bar_lookup = bar_frame.set_index("dt").to_dict(orient="index")
    trading_days = [int(value) for value in basis_frame["dt"].tolist()]

    state = ChallengerState()
    ledger_rows: list[dict[str, Any]] = []
    roundtrips: list[dict[str, Any]] = []
    current_roundtrip: dict[str, Any] | None = None
    roundtrip_sequence = 0
    first_full_exit_index: int | None = None

    for index, decision_dt in enumerate(trading_days):
        row = basis_frame.loc[basis_frame["dt"] == decision_dt].iloc[0]
        context = _basis_context(row)
        context["decision_index"] = index
        action, action_meta = _challenger_select_action(state, context, decision_index=index)
        next_trading_day = trading_days[index + 1] if index + 1 < len(trading_days) else None
        execution_dt = next_trading_day
        if execution_dt is None:
            future_bars = sorted(int(value) for value in bar_lookup.keys() if int(value) > decision_dt)
            if not future_bars:
                raise RuntimeError(f"missing execution bar after {decision_dt}")
            execution_dt = future_bars[0]
        execution_bar = bar_lookup.get(execution_dt)
        if execution_bar is None:
            raise RuntimeError(f"missing execution bar for {execution_dt}")

        previous_position = _position_text(state.long_units)
        if action == "long_entry":
            state.open_trade_start_index = index
        event = _apply_challenger_action(
            state,
            action,
            _safe_float(execution_bar.get("o")),
            decision_date=decision_dt,
            execution_date=execution_dt,
            decision_index=index,
        )
        next_position = _position_text(state.long_units)
        unrealized_pnl = 0.0
        if state.long_units > 0:
            unrealized_pnl = (float(execution_bar.get("c")) - state.avg_price) * state.long_units * SHARES_PER_BUY_UNIT
        equity_curve_value = float(state.realized_pnl + unrealized_pnl)

        state_notes: list[str] = [
            f"decision_basis_date={_ymd_to_date_text(decision_dt)}",
            f"execution_date={_ymd_to_date_text(execution_dt)}",
        ]
        if action == "stay":
            state_notes.append("research_fallback=held_by_capture_policy")
        elif action in {"long_entry", "long_add", "long_exit", "partial_take_long"}:
            state_notes.append("research_fallback=deterministic_replay_adapter")

        reason_payload = _build_action_reason_payload(context)
        regime_context = {
            "marketRegime": context.get("marketRegime"),
            "marketRiskOn": bool(context.get("marketRiskOn")),
            "marketRiskOff": bool(context.get("marketRiskOff")),
            "monthlyBoxPos": _safe_float(context.get("monthlyBoxPos")),
            "weeklyBreakoutUpProb": _safe_float(context.get("weeklyBreakoutUpProb")),
            "weeklyBreakoutDownProb": _safe_float(context.get("weeklyBreakoutDownProb")),
            "monthlyBreakoutUpProb": _safe_float(context.get("monthlyBreakoutUpProb")),
            "monthlyBreakoutDownProb": _safe_float(context.get("monthlyBreakoutDownProb")),
        }
        micro_snapshot = {
            "decision_date": decision_dt,
            "execution_date": execution_dt,
            "basis_version": row.get("basis_version"),
            "basis_source": row.get("basis_source"),
            "source_as_of": _safe_int(row.get("source_as_of"), 0) or None,
            "basis_payload": json.loads(str(row.get("basis_payload_json") or "{}")),
            "derived_context": {
                "join_mode": context.get("join_mode"),
                "monthly_main_state_ctx": context.get("monthly_main_state_ctx"),
                "weekly_main_state_ctx": context.get("weekly_main_state_ctx"),
                "daily_main_state_ctx": context.get("daily_main_state_ctx"),
                "score_long": _safe_float(context.get("score_long")),
                "score_short": _safe_float(context.get("score_short")),
                "entry_qualified": bool(row.get("entry_qualified")),
                "setup_type": row.get("setup_type"),
            },
            "policy": {
                "policy_id": _challenger_policy_id(symbol),
                "mode": "research-fallback",
                "action_gate": action_meta,
            },
        }

        if action == "long_entry":
            roundtrip_sequence += 1
            current_roundtrip = {
                "roundtrip_id": _roundtrip_id(symbol, decision_dt, roundtrip_sequence),
                "entry_kind": event.get("entry_kind"),
                "entry_decision_date": decision_dt,
                "entry_index": index,
                "entry_execution_date": execution_dt,
                "entry_fill_price": _safe_float(execution_bar.get("o")),
                "entry_reason": action_meta.get("reason"),
                "units": state.long_units,
                "adds": 0,
                "partial_takes": 0,
                "exit_decision_date": None,
                "exit_execution_date": None,
                "exit_fill_price": None,
                "exit_reason": None,
                "actions": [action],
                "action_details": [
                    {
                        "action": action,
                        "decision_date": decision_dt,
                        "execution_date": execution_dt,
                        "reason": action_meta.get("reason"),
                    }
                ],
                "equity_curve": [equity_curve_value],
                "realized_pnl": 0.0,
                "cycle_start_realized_pnl": float(state.cycle_start_realized_pnl),
                "entry_type": event.get("entry_kind"),
            }
        elif action == "long_add" and current_roundtrip is not None:
            current_roundtrip["adds"] = int(current_roundtrip.get("adds") or 0) + 1
            current_roundtrip["units"] = state.long_units
            current_roundtrip["actions"].append(action)
            current_roundtrip.setdefault("action_details", []).append(
                {
                    "action": action,
                    "decision_date": decision_dt,
                    "execution_date": execution_dt,
                    "reason": action_meta.get("reason"),
                }
            )
            current_roundtrip["equity_curve"].append(equity_curve_value)
        elif action == "partial_take_long" and current_roundtrip is not None:
            current_roundtrip["partial_takes"] = int(current_roundtrip.get("partial_takes") or 0) + 1
            current_roundtrip["units"] = state.long_units
            current_roundtrip["actions"].append(action)
            current_roundtrip.setdefault("action_details", []).append(
                {
                    "action": action,
                    "decision_date": decision_dt,
                    "execution_date": execution_dt,
                    "reason": action_meta.get("reason"),
                }
            )
            current_roundtrip["equity_curve"].append(equity_curve_value)
        elif action == "long_exit" and current_roundtrip is not None:
            current_roundtrip["exit_decision_date"] = decision_dt
            current_roundtrip["exit_index"] = index
            current_roundtrip["exit_execution_date"] = execution_dt
            current_roundtrip["exit_fill_price"] = _safe_float(execution_bar.get("o"))
            current_roundtrip["exit_reason"] = action_meta.get("reason")
            current_roundtrip["actions"].append(action)
            current_roundtrip.setdefault("action_details", []).append(
                {
                    "action": action,
                    "decision_date": decision_dt,
                    "execution_date": execution_dt,
                    "reason": action_meta.get("reason"),
                }
            )
            current_roundtrip["equity_curve"].append(equity_curve_value)
            current_roundtrip["realized_pnl"] = float(state.realized_pnl - float(current_roundtrip.get("cycle_start_realized_pnl") or 0.0))
            current_roundtrip["max_equity"] = float(max(current_roundtrip["equity_curve"]))
            current_roundtrip["min_equity"] = float(min(current_roundtrip["equity_curve"]))
            current_roundtrip["max_drawdown"] = float(_max_drawdown_from_equity(list(current_roundtrip["equity_curve"])))
            current_roundtrip["holding_days"] = int(current_roundtrip["exit_index"] - current_roundtrip["entry_index"])
            roundtrips.append(current_roundtrip)
            current_roundtrip = None
            first_full_exit_index = index if first_full_exit_index is None else first_full_exit_index

        ledger_rows.append(
            {
                "dt": decision_dt,
                "date": _ymd_to_date_text(decision_dt),
                "symbol": str(row.get("code") or symbol),
                "previous_position": previous_position,
                "selected_action": action,
                "next_position": next_position,
                "execution_price": _safe_float(execution_bar.get("o")),
                "reason_codes": _json_text(reason_payload),
                "regime_context": _json_text(regime_context),
                "daily_micro_snapshot": _json_text(micro_snapshot),
                "realized_pnl": float(state.realized_pnl),
                "unrealized_pnl": float(unrealized_pnl),
                "equity_curve": float(equity_curve_value),
                "invalidation_state": _json_text(
                    {
                        "state": "flat" if state.long_units == 0 else "long_active",
                        "trigger": action_meta.get("reason"),
                        "days_held": 0 if current_roundtrip is None else int(index - int(current_roundtrip.get("entry_index") or index)),
                        "reentry_allowed": not state.reentry_used,
                        "first_full_exit_seen": first_full_exit_index is not None,
                    }
                ),
                "notes_if_any": "; ".join(state_notes),
            }
        )

    if current_roundtrip is not None:
        current_roundtrip["realized_pnl"] = float(state.realized_pnl - float(current_roundtrip.get("cycle_start_realized_pnl") or 0.0))
        current_roundtrip["max_equity"] = float(max(current_roundtrip["equity_curve"]))
        current_roundtrip["min_equity"] = float(min(current_roundtrip["equity_curve"]))
        current_roundtrip["max_drawdown"] = float(_max_drawdown_from_equity(list(current_roundtrip["equity_curve"])))
        current_roundtrip["holding_days"] = int(len(current_roundtrip["equity_curve"]) - 1)
        roundtrips.append(current_roundtrip)

    ledger_frame = pd.DataFrame(ledger_rows)
    aggregate = _policy_aggregate_summary(
        ledger_frame=ledger_frame,
        roundtrips=roundtrips,
        policy_id=_challenger_policy_id(symbol),
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
    )
    config = {
        "policy_id": _challenger_policy_id(symbol),
        "symbol": symbol,
        "name": "Takara Holdings",
        "freeze_date": DEFAULT_FREEZE_DATE,
        "period": {"start": start_date, "end": end_date},
        "source_db_path": str(source_db_path),
        "policy_mode": "research-fallback",
        "policy_scope": [
            "long_entry",
            "long_add",
            "partial_take_long",
            "long_exit",
            "stay",
        ],
        "non_scope": [
            "MeeMee UI changes",
            "publish promotion",
            "portfolio allocation",
            "short-side family",
            "hedge_long",
        ],
        "execution_model": "decision on confirmed daily basis row; execution at next trading day open",
        "policy_contract": {
            "initial_entry_units": ENTRY_BUY_UNITS,
            "partial_take_units": PARTIAL_TAKE_TARGET_UNITS,
            "continuation_add_units": REENTRY_ADD_UNITS,
            "max_units": MAX_BUY_UNITS,
            "shares_per_buy_unit": SHARES_PER_BUY_UNIT,
            "reentry_limit": MAX_REENTRY_OPPORTUNITIES,
        },
        "generated_at": _utc_now(),
    }
    return ledger_frame, config, {"aggregate": aggregate, "roundtrips": roundtrips}


def _max_close_to_close_positive_move(
    *,
    current_close: float,
    future_closes: list[float],
    horizon_days: int,
    threshold: float,
) -> tuple[float, bool]:
    horizon = future_closes[:horizon_days]
    if not horizon:
        return 0.0, False
    max_extension = max(0.0, max(horizon) - current_close)
    return max_extension, bool(current_close > 0.0 and (max(horizon) / current_close - 1.0) >= threshold)


def _policy_aggregate_summary(
    *,
    ledger_frame: pd.DataFrame,
    roundtrips: list[dict[str, Any]],
    policy_id: str,
    symbol: str,
    start_date: str,
    end_date: str,
) -> dict[str, Any]:
    completed_roundtrips = [row for row in roundtrips if row.get("exit_decision_date") is not None]
    gross_gains = sum(max(0.0, float(row.get("realized_pnl") or 0.0)) for row in completed_roundtrips)
    gross_losses = sum(abs(min(0.0, float(row.get("realized_pnl") or 0.0))) for row in completed_roundtrips)
    profit_factor = None if gross_losses <= 0.0 else float(gross_gains / gross_losses)
    trade_count = len(completed_roundtrips)
    reentry_count = sum(1 for row in completed_roundtrips if str(row.get("entry_type")) == "reentry")
    total_days_in_position = int(sum(1 for value in ledger_frame["next_position"].astype(str).tolist() if value != "0-0"))
    exposure_ratio_in_window = float(total_days_in_position / max(1, int(len(ledger_frame))))
    equity_curve = [float(value) for value in ledger_frame["equity_curve"].tolist()]
    max_drawdown = float(_max_drawdown_from_equity(equity_curve))
    avg_pnl_per_trade = None if trade_count <= 0 else float(sum(float(row.get("realized_pnl") or 0.0) for row in completed_roundtrips) / trade_count)
    decision_day_count = int(len(ledger_frame))
    available_interval_count = max(0, decision_day_count - 1)
    return {
        "policy_id": policy_id,
        "symbol": symbol,
        "period": {"start": start_date, "end": end_date},
        "trade_count": trade_count,
        "reentry_count": reentry_count,
        "total_days_in_position": total_days_in_position,
        "exposure_ratio_in_window": exposure_ratio_in_window,
        "avg_pnl_per_trade": avg_pnl_per_trade,
        "max_drawdown": max_drawdown,
        "profit_factor": profit_factor,
        "gross_gains": float(gross_gains),
        "gross_losses": float(gross_losses),
        "decision_day_count": decision_day_count,
        "available_interval_count": available_interval_count,
        "completed_roundtrips": completed_roundtrips,
        "completed_roundtrip_count": trade_count,
        "total_realized_pnl": float(sum(float(row.get("realized_pnl") or 0.0) for row in completed_roundtrips)),
    }


def _build_interval_frame(ledger_frame: pd.DataFrame, bars_frame: pd.DataFrame) -> pd.DataFrame:
    bars = bars_frame.copy().sort_values("dt").reset_index(drop=True)
    bars = bars.drop_duplicates(subset=["dt"], keep="last")
    close_map = {int(row["dt"]): float(row["c"]) for _, row in bars.iterrows() if pd.notna(row["dt"])}
    trading_days = [int(value) for value in ledger_frame["dt"].tolist()]
    rows: list[dict[str, Any]] = []
    for idx, decision_dt in enumerate(trading_days):
        if decision_dt not in close_map:
            continue
        current_close = close_map[decision_dt]
        future_index = idx + 1
        if future_index >= len(trading_days):
            future_candidates = [value for value in sorted(close_map) if value > decision_dt]
            if not future_candidates:
                continue
            next_dt = future_candidates[0]
        else:
            next_dt = trading_days[future_index]
        if next_dt not in close_map:
            future_candidates = [value for value in sorted(close_map) if value > decision_dt]
            if not future_candidates:
                continue
            next_dt = future_candidates[0]
        next_close = close_map[next_dt]
        row = ledger_frame.loc[ledger_frame["dt"] == decision_dt].iloc[0]
        units = int(str(row["next_position"]).split("-")[1]) if "-" in str(row["next_position"]) else 0
        exposure_fraction = units / float(MAX_BUY_UNITS)
        positive_close_move = max(0.0, next_close - current_close)
        rows.append(
            {
                "dt": decision_dt,
                "date": _ymd_to_date_text(decision_dt),
                "next_dt": next_dt,
                "next_date": _ymd_to_date_text(next_dt),
                "current_close": current_close,
                "next_close": next_close,
                "close_to_close_return": (next_close / current_close - 1.0) if current_close else None,
                "positive_close_move": positive_close_move,
                "exposure_fraction": exposure_fraction,
                "captured_positive_move": positive_close_move * exposure_fraction,
                "selected_action": row["selected_action"],
                "next_position": row["next_position"],
                "realized_pnl": float(row["realized_pnl"]),
                "equity_curve": float(row["equity_curve"]),
            }
        )
    return pd.DataFrame(rows)


def _build_missed_capture_report(
    *,
    champion_ledger: pd.DataFrame,
    challenger_ledger: pd.DataFrame,
    bars_frame: pd.DataFrame,
    symbol: str,
    start_date: str,
    end_date: str,
) -> dict[str, Any]:
    champion_intervals = _build_interval_frame(champion_ledger, bars_frame)
    challenger_intervals = _build_interval_frame(challenger_ledger, bars_frame)
    if champion_intervals.empty or challenger_intervals.empty:
        return {
            "schema_version": "tradex_sample_2531_missed_capture_report_v1",
            "generated_at": _utc_now(),
            "summary": {"note": "insufficient interval data"},
        }

    merged = champion_intervals[["dt", "date", "next_date", "current_close", "next_close", "positive_close_move", "exposure_fraction", "captured_positive_move"]].merge(
        challenger_intervals[["dt", "exposure_fraction", "captured_positive_move", "selected_action"]],
        on="dt",
        how="inner",
        suffixes=("_champion", "_challenger"),
    )
    merged["capture_gap"] = merged["captured_positive_move_challenger"] - merged["captured_positive_move_champion"]
    merged["flat_gap"] = merged["positive_close_move"] - merged["captured_positive_move_champion"]
    positive_move_total = float(merged["positive_close_move"].sum())
    champion_captured = float(merged["captured_positive_move_champion"].sum())
    challenger_captured = float(merged["captured_positive_move_challenger"].sum())
    champion_capture_ratio = None if positive_move_total <= 0.0 else float(champion_captured / positive_move_total)
    challenger_capture_ratio = None if positive_move_total <= 0.0 else float(challenger_captured / positive_move_total)

    first_exit_row = challenger_ledger.loc[challenger_ledger["selected_action"] == "long_exit"].head(1)
    post_exit_flat_days = 0
    if not first_exit_row.empty:
        first_exit_dt = int(first_exit_row.iloc[0]["dt"])
        post_exit_flat_days = int(
            sum(
                1
                for _, row in challenger_intervals.iterrows()
                if int(row["dt"]) >= first_exit_dt and float(row["positive_close_move"]) > 0.0 and str(row["next_position"]) == "0-0"
            )
        )

    worst_missed_rows = merged.sort_values("capture_gap", ascending=False).head(10)
    top_missed_windows = [
        {
            "dt": int(row["dt"]),
            "date": row["date"],
            "next_date": row["next_date"],
            "positive_close_move": float(row["positive_close_move"]),
            "champion_capture_fraction": float(row["exposure_fraction_champion"]),
            "challenger_capture_fraction": float(row["exposure_fraction_challenger"]),
            "capture_gap": float(row["capture_gap"]),
        }
        for _, row in worst_missed_rows.iterrows()
    ]
    return {
        "schema_version": "tradex_sample_2531_missed_capture_report_v1",
        "generated_at": _utc_now(),
        "authoritative_sources": {
            "champion_ledger": "current sample replay policy",
            "challenger_ledger": f"{_sample_name(symbol)}_policy_v2_daily_ledger.parquet",
            "bars_source": "daily_bars from frozen source db",
        },
        "window": {"start": start_date, "end": end_date},
        "summary": {
            "positive_close_to_close_move_total": positive_move_total,
            "champion_captured_positive_move": champion_captured,
            "challenger_captured_positive_move": challenger_captured,
            "champion_capture_ratio": champion_capture_ratio,
            "challenger_capture_ratio": challenger_capture_ratio,
            "capture_ratio_delta": None if champion_capture_ratio is None or challenger_capture_ratio is None else float(challenger_capture_ratio - champion_capture_ratio),
            "idle_up_days_after_first_full_exit": post_exit_flat_days,
        },
        "top_missed_windows": top_missed_windows,
    }


def _build_reentry_reason_rollup(
    *,
    challenger_ledger: pd.DataFrame,
    challenger_roundtrips: list[dict[str, Any]],
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    taken_reentry_reasons: dict[str, int] = {}
    partial_take_reasons: dict[str, int] = {}
    full_exit_reasons: dict[str, int] = {}
    failed_reentry_reasons: dict[str, int] = {}

    first_full_exit_dt = challenger_ledger.loc[challenger_ledger["selected_action"] == "long_exit", "dt"].min()
    for _, row in challenger_ledger.iterrows():
        action = str(row["selected_action"])
        reason_payload = json.loads(str(row["daily_micro_snapshot"]))
        action_gate = reason_payload.get("policy", {}).get("action_gate") if isinstance(reason_payload, dict) else {}
        reason = _safe_text(action_gate.get("reason"), "unknown")
        if action == "long_entry" and reason == "reentry_after_full_exit":
            taken_reentry_reasons[reason] = taken_reentry_reasons.get(reason, 0) + 1
        elif action == "partial_take_long":
            partial_take_reasons[reason] = partial_take_reasons.get(reason, 0) + 1
        elif action == "long_exit":
            full_exit_reasons[reason] = full_exit_reasons.get(reason, 0) + 1

    if first_full_exit_dt is not None:
        if pd.isna(first_full_exit_dt):
            first_full_exit_dt = None
    if first_full_exit_dt is not None:
        for _, row in challenger_ledger.iterrows():
            if int(row["dt"]) <= int(first_full_exit_dt):
                continue
            if str(row["previous_position"]) != "0-0":
                continue
            action = str(row["selected_action"])
            if action == "stay":
                snapshot = json.loads(str(row["daily_micro_snapshot"]))
                gate = snapshot.get("policy", {}).get("action_gate", {}) if isinstance(snapshot, dict) else {}
                score_long = _safe_float(snapshot.get("derived_context", {}).get("score_long"))
                score_short = _safe_float(snapshot.get("derived_context", {}).get("score_short"))
                market_regime = _safe_text(snapshot.get("basis_payload", {}).get("marketRegime"), "unknown")
                recovery = _safe_text(snapshot.get("derived_context", {}).get("daily_main_state_ctx"), "unknown")
                if (
                    score_long is not None
                    and score_long >= 5.0
                    and (score_short is None or score_short <= 0.0)
                    and market_regime in {"risk_on", "neutral"}
                    and recovery in {"daily_reversal_up_candidate", "daily_up_mid"}
                ):
                    if len([rt for rt in challenger_roundtrips if rt.get("entry_type") == "reentry"]) >= MAX_REENTRY_OPPORTUNITIES:
                        failed_reentry_reasons["reentry_limit_reached"] = failed_reentry_reasons.get("reentry_limit_reached", 0) + 1
                    else:
                        failed_reentry_reasons["gate_met_but_not_taken"] = failed_reentry_reasons.get("gate_met_but_not_taken", 0) + 1

    for reason, count in sorted(taken_reentry_reasons.items()):
        rows.append(
            {
                "reason": reason,
                "category": "taken_reentry",
                "count": int(count),
            }
        )
    for reason, count in sorted(partial_take_reasons.items()):
        rows.append(
            {
                "reason": reason,
                "category": "partial_take",
                "count": int(count),
            }
        )
    for reason, count in sorted(full_exit_reasons.items()):
        rows.append(
            {
                "reason": reason,
                "category": "full_exit",
                "count": int(count),
            }
        )
    for reason, count in sorted(failed_reentry_reasons.items()):
        rows.append(
            {
                "reason": reason,
                "category": "failed_reentry",
                "count": int(count),
            }
        )
    return {
        "schema_version": "tradex_sample_2531_reentry_reason_rollup_v1",
        "generated_at": _utc_now(),
        "summary": {
            "taken_reentry_count": int(sum(taken_reentry_reasons.values())),
            "partial_take_count": int(sum(partial_take_reasons.values())),
            "full_exit_count": int(sum(full_exit_reasons.values())),
            "failed_reentry_count": int(sum(failed_reentry_reasons.values())),
        },
        "rows": rows,
    }


def _compute_interval_metrics(
    *,
    ledger_frame: pd.DataFrame,
    bars_frame: pd.DataFrame,
    start_date: str,
    end_date: str,
    first_full_exit_dt: int | None,
) -> dict[str, Any]:
    intervals = _build_interval_frame(ledger_frame, bars_frame)
    if intervals.empty:
        return {
            "upside_capture_ratio": None,
            "missed_upside_ratio": None,
            "idle_up_days_after_exit": 0,
            "premature_exit_count": 0,
            "failed_reentry_count": 0,
            "positive_move_total": 0.0,
            "positive_move_captured": 0.0,
            "interval_count": 0,
        }

    positive_move_total = float(intervals["positive_close_move"].sum())
    positive_move_captured = float(intervals["captured_positive_move"].sum())
    upside_capture_ratio = None if positive_move_total <= 0.0 else float(positive_move_captured / positive_move_total)
    missed_upside_ratio = None if upside_capture_ratio is None else float(1.0 - upside_capture_ratio)
    idle_up_days_after_exit = 0
    if first_full_exit_dt is not None:
        idle_up_days_after_exit = int(
            sum(
                1
                for _, row in intervals.iterrows()
                if int(row["dt"]) >= int(first_full_exit_dt) and float(row["positive_close_move"]) > 0.0 and float(row["exposure_fraction"]) <= 0.0
            )
        )

    premature_exit_count = 0
    close_map = {int(row["dt"]): float(row["c"]) for _, row in bars_frame.sort_values("dt").drop_duplicates(subset=["dt"], keep="last").iterrows()}
    ledger_rows = ledger_frame.sort_values("dt").reset_index(drop=True)
    for idx, row in ledger_rows.iterrows():
        if str(row["selected_action"]) != "long_exit":
            continue
        exit_dt = int(row["dt"])
        exit_price = _safe_float(row["execution_price"])
        future_dates = [value for value in sorted(close_map) if value > exit_dt]
        future_closes = [close_map[value] for value in future_dates]
        _, extended = _max_close_to_close_positive_move(
            current_close=exit_price,
            future_closes=future_closes,
            horizon_days=PREMATURE_EXIT_HORIZON_DAYS,
            threshold=PREMATURE_EXIT_EXTENSION_THRESHOLD,
        )
        if extended:
            premature_exit_count += 1

    failed_reentry_count = 0
    if first_full_exit_dt is not None:
        reentry_used = False
        for _, row in ledger_rows.iterrows():
            if int(row["dt"]) <= int(first_full_exit_dt):
                continue
            if str(row["previous_position"]) != "0-0" or str(row["next_position"]) != "0-0":
                continue
            snapshot = json.loads(str(row["daily_micro_snapshot"]))
            gate = snapshot.get("policy", {}).get("action_gate", {}) if isinstance(snapshot, dict) else {}
            derived = snapshot.get("derived_context", {}) if isinstance(snapshot, dict) else {}
            score_long = _safe_float(derived.get("score_long"))
            score_short = _safe_float(derived.get("score_short"))
            market_regime = _safe_text(snapshot.get("basis_payload", {}).get("marketRegime"), "unknown") if isinstance(snapshot, dict) else "unknown"
            recovery = _safe_text(derived.get("daily_main_state_ctx"), "unknown") if isinstance(snapshot, dict) else "unknown"
            if (
                score_long is not None
                and score_long >= 5.0
                and (score_short is None or score_short <= 0.0)
                and market_regime in {"risk_on", "neutral"}
                and recovery in {"daily_reversal_up_candidate", "daily_up_mid"}
            ):
                if str(row["selected_action"]) != "long_entry" or reentry_used:
                    failed_reentry_count += 1
                else:
                    reentry_used = True

    return {
        "upside_capture_ratio": upside_capture_ratio,
        "missed_upside_ratio": missed_upside_ratio,
        "idle_up_days_after_exit": idle_up_days_after_exit,
        "premature_exit_count": premature_exit_count,
        "failed_reentry_count": failed_reentry_count,
        "positive_move_total": positive_move_total,
        "positive_move_captured": positive_move_captured,
        "interval_count": int(len(intervals)),
    }


def _build_roundtrip_summary(
    *,
    config: dict[str, Any],
    ledger_frame: pd.DataFrame,
    roundtrip_payload: dict[str, Any],
    bars_frame: pd.DataFrame,
    start_date: str,
    end_date: str,
    generated_at: str,
) -> dict[str, Any]:
    roundtrips = roundtrip_payload["roundtrips"]
    aggregate = roundtrip_payload["aggregate"]
    first_full_exit_row = ledger_frame.loc[ledger_frame["selected_action"] == "long_exit"].head(1)
    first_full_exit_dt = int(first_full_exit_row.iloc[0]["dt"]) if not first_full_exit_row.empty else None
    interval_metrics = _compute_interval_metrics(
        ledger_frame=ledger_frame,
        bars_frame=bars_frame,
        start_date=start_date,
        end_date=end_date,
        first_full_exit_dt=first_full_exit_dt,
    )
    total_realized_pnl = float(sum(float(row.get("realized_pnl") or 0.0) for row in roundtrips if row.get("exit_decision_date") is not None))
    worst_drawdown = float(min([float(row.get("max_drawdown") or 0.0) for row in roundtrips], default=0.0))
    summary_rows: list[dict[str, Any]] = []
    for roundtrip in roundtrips:
        summary_rows.append(
            {
                "roundtrip_id": roundtrip.get("roundtrip_id"),
                "entry_type": roundtrip.get("entry_type"),
                "entry_decision_date": _ymd_to_date_text(int(roundtrip["entry_decision_date"])),
                "entry_execution_date": _ymd_to_date_text(int(roundtrip["entry_execution_date"])),
                "exit_decision_date": _ymd_to_date_text(int(roundtrip["exit_decision_date"])) if roundtrip.get("exit_decision_date") else None,
                "exit_execution_date": _ymd_to_date_text(int(roundtrip["exit_execution_date"])) if roundtrip.get("exit_execution_date") else None,
                "adds": int(roundtrip.get("adds") or 0),
                "partial_takes": int(roundtrip.get("partial_takes") or 0),
                "realized_pnl": float(roundtrip.get("realized_pnl") or 0.0),
                "max_drawdown": float(roundtrip.get("max_drawdown") or 0.0),
                "holding_days": int(roundtrip.get("holding_days") or 0),
                "exit_reason": roundtrip.get("exit_reason"),
                "entry_reason": roundtrip.get("entry_reason"),
                "entry_kind": roundtrip.get("entry_type"),
            }
        )
    return {
        "schema_version": "tradex_sample_2531_policy_v2_roundtrip_summary_v1",
        "policy_id": config["policy_id"],
        "symbol": config["symbol"],
        "period": config["period"],
        "metric_definitions": {
            "trade_count": "count of completed full-exit roundtrips",
            "reentry_count": "count of completed roundtrips whose entry_type == reentry",
            "total_days_in_position": "count of decision intervals where next_position is not 0-0",
            "exposure_ratio_in_window": "mean(next_position_units / max_units) across decision intervals",
            "upside_capture_ratio": "sum(max(next_close - current_close, 0) * exposure_fraction) / sum(max(next_close - current_close, 0))",
            "missed_upside_ratio": "1 - upside_capture_ratio",
            "idle_up_days_after_exit": "positive close-to-close days after the first full exit while flat",
            "avg_pnl_per_trade": "total realized pnl from completed roundtrips / trade_count",
            "max_drawdown": "max peak-to-trough drawdown from daily realized+unrealized equity",
            "profit_factor": "gross gains / gross losses on completed realized roundtrips; null if no gross losses",
            "premature_exit_count": "full exits followed by >=2% close extension within 3 trading days",
            "failed_reentry_count": "reentry opportunities that met the challenger gate but were not taken",
        },
        "aggregate": {
            **aggregate,
            **interval_metrics,
            "total_realized_pnl": total_realized_pnl,
            "max_drawdown": float(_max_drawdown_from_equity([float(value) for value in ledger_frame["equity_curve"].tolist()])),
            "avg_pnl_per_trade": aggregate.get("avg_pnl_per_trade"),
            "trade_count": aggregate.get("trade_count"),
            "reentry_count": aggregate.get("reentry_count"),
            "total_days_in_position": aggregate.get("total_days_in_position"),
            "exposure_ratio_in_window": aggregate.get("exposure_ratio_in_window"),
        },
        "roundtrips": summary_rows,
        "generated_at": generated_at,
    }


def _build_compare_payload(
    *,
    champion_payload: dict[str, Any],
    challenger_payload: dict[str, Any],
    champion_metrics: dict[str, Any],
    challenger_metrics: dict[str, Any],
    symbol: str,
    challenger_reference: Path,
    start_date: str,
    end_date: str,
    source_db_path: Path,
) -> dict[str, Any]:
    metric_names = [
        "trade_count",
        "reentry_count",
        "total_days_in_position",
        "exposure_ratio_in_window",
        "upside_capture_ratio",
        "missed_upside_ratio",
        "idle_up_days_after_exit",
        "avg_pnl_per_trade",
        "max_drawdown",
        "profit_factor",
        "premature_exit_count",
        "failed_reentry_count",
    ]
    delta = {
        name: (
            None
            if champion_metrics.get(name) is None or challenger_metrics.get(name) is None
            else float(challenger_metrics[name] - champion_metrics[name])
        )
        for name in metric_names
        if isinstance(champion_metrics.get(name), (int, float)) or isinstance(challenger_metrics.get(name), (int, float))
    }
    changed_action_day_count = int((champion_payload["ledger"]["selected_action"] != challenger_payload["ledger"]["selected_action"]).sum())
    changed_entry_day_count = int(
        (
            (champion_payload["ledger"]["selected_action"] == "long_entry")
            ^ (challenger_payload["ledger"]["selected_action"] == "long_entry")
        ).sum()
    )
    changed_exit_day_count = int(
        (
            (champion_payload["ledger"]["selected_action"] == "long_exit")
            ^ (challenger_payload["ledger"]["selected_action"] == "long_exit")
        ).sum()
    )
    changed_partial_take_day_count = int(
        (
            (champion_payload["ledger"]["selected_action"] == "partial_take_long")
            ^ (challenger_payload["ledger"]["selected_action"] == "partial_take_long")
        ).sum()
    )
    changed_add_day_count = int(
        (
            (champion_payload["ledger"]["selected_action"] == "long_add")
            ^ (challenger_payload["ledger"]["selected_action"] == "long_add")
        ).sum()
    )
    first_changed_dates = challenger_payload["ledger"].loc[
        champion_payload["ledger"]["selected_action"] != challenger_payload["ledger"]["selected_action"], "date"
    ].head(10).tolist()
    compare_payload = {
        "schema_version": "tradex_sample_2531_capture_compare_v1",
        "generated_at": _utc_now(),
        "authoritative_sources": {
            "source_db_path": str(source_db_path),
            "champion_reference": str(Path(rf"G:\Tradex\sample_replays\{_sample_name(symbol)}")),
            "challenger_reference": str(challenger_reference),
        },
        "same_condition_contract": {
            "same_symbol": True,
            "same_period": True,
            "same_source_db": True,
            "same_execution_assumption": "next_trading_day_open",
            "same_no_in_period_learning_rule": True,
            "same_artifact_detail_level": True,
            "single_axis": "long-side re-entry / capture density",
            "no_short_side_expansion": True,
            "no_meemee_product_changes": True,
        },
        "policy_contract": {
            "champion_policy_id": champion_metrics["policy_id"],
            "challenger_policy_id": challenger_metrics["policy_id"],
            "challenger_policy_version": "v2",
            "challenger_entry_units": REENTRY_ENTRY_UNITS,
            "challenger_partial_take_units": PARTIAL_TAKE_TARGET_UNITS,
            "challenger_add_units": REENTRY_ADD_UNITS,
            "challenger_reentry_limit": MAX_REENTRY_OPPORTUNITIES,
        },
        "metric_definitions": challenger_payload["summary"]["metric_definitions"],
        "champion": champion_metrics,
        "challenger": challenger_metrics,
        "delta": delta,
        "observed_branching": {
            "changed_action_day_count": changed_action_day_count,
            "changed_entry_day_count": changed_entry_day_count,
            "changed_exit_day_count": changed_exit_day_count,
            "changed_partial_take_day_count": changed_partial_take_day_count,
            "changed_add_day_count": changed_add_day_count,
            "first_changed_dates": first_changed_dates,
            "selection_divergence_reason": "long-side re-entry / partial-take / re-add capture-density axis",
        },
        "window": {"start": start_date, "end": end_date},
        "artifact_paths": {
            "challenger_daily_ledger": f"{_sample_name(symbol)}_policy_v2_daily_ledger.parquet",
            "challenger_roundtrip_summary": f"{_sample_name(symbol)}_policy_v2_roundtrip_summary.json",
            "missed_capture_report": f"{_sample_name(symbol)}_missed_capture_report.json",
            "reentry_reason_rollup": f"{_sample_name(symbol)}_reentry_reason_rollup.json",
            "decision": f"{_sample_name(symbol)}_capture_keep_drop_hold_decision.json",
        },
    }
    return compare_payload


def _decision_from_compare(compare_payload: dict[str, Any]) -> dict[str, Any]:
    champion = compare_payload["champion"]
    challenger = compare_payload["challenger"]
    delta = compare_payload["delta"]
    reasons: list[str] = []
    decision = "hold"

    if (challenger.get("upside_capture_ratio") or 0.0) > (champion.get("upside_capture_ratio") or 0.0) + 0.03:
        reasons.append("upside_capture_ratio_improved")
    if (challenger.get("idle_up_days_after_exit") or 0) < (champion.get("idle_up_days_after_exit") or 0):
        reasons.append("idle_up_days_after_exit_reduced")
    if (challenger.get("failed_reentry_count") or 0) < (champion.get("failed_reentry_count") or 0):
        reasons.append("failed_reentry_count_reduced")
    if (challenger.get("premature_exit_count") or 0) < (champion.get("premature_exit_count") or 0):
        reasons.append("premature_exit_count_reduced")

    champion_dd = abs(float(champion.get("max_drawdown") or 0.0))
    challenger_dd = abs(float(challenger.get("max_drawdown") or 0.0))
    drawdown_regression = challenger_dd > champion_dd * 1.25 if champion_dd > 0.0 else challenger_dd > 0.0
    capture_regression = (challenger.get("upside_capture_ratio") or 0.0) < (champion.get("upside_capture_ratio") or 0.0) - 0.01
    pnl_regression = (challenger.get("avg_pnl_per_trade") or 0.0) < (champion.get("avg_pnl_per_trade") or 0.0) - 0.01

    if not capture_regression and not drawdown_regression and not pnl_regression and reasons:
        decision = "keep"
    elif capture_regression or drawdown_regression:
        decision = "drop"
        reasons.append("material_metric_regression")
    else:
        reasons.append("insufficient_separation_for_keep")

    return {
        "schema_version": "tradex_sample_2531_capture_keep_drop_hold_decision_v1",
        "generated_at": _utc_now(),
        "candidate_local_decision": decision,
        "session_aggregate_decision": decision,
        "authoritative_rollup_decision": decision,
        "decision_reasons": reasons,
        "authoritative_sources": {
            "compare_json": f"{_sample_name(compare_payload['champion']['symbol'])}_capture_compare.json",
            "missed_capture_report": f"{_sample_name(compare_payload['champion']['symbol'])}_missed_capture_report.json",
            "reentry_reason_rollup": f"{_sample_name(compare_payload['champion']['symbol'])}_reentry_reason_rollup.json",
            "challenger_roundtrip_summary": f"{_sample_name(compare_payload['champion']['symbol'])}_policy_v2_roundtrip_summary.json",
        },
        "compare_metrics": {
            "champion": {key: compare_payload["champion"].get(key) for key in compare_payload["metric_definitions"].keys()},
            "challenger": {key: compare_payload["challenger"].get(key) for key in compare_payload["metric_definitions"].keys()},
            "delta": delta,
        },
        "same_condition_contract": compare_payload["same_condition_contract"],
        "remaining_risks": [
            "reentry coverage may remain sparse if the frozen window has only one confirmed recovery sequence",
            "profit_factor can be null when the completed-trade sample has no losses",
            "premature_exit_count uses a fixed 3-trading-day extension horizon and a 2% threshold",
        ],
    }


def run_compare(
    *,
    source_db_path: Path = DEFAULT_SOURCE_DB_PATH,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    symbol: str = DEFAULT_SYMBOL,
    start_date: str = DEFAULT_START_DATE,
    end_date: str = DEFAULT_END_DATE,
    freeze_date: str = DEFAULT_FREEZE_DATE,
) -> dict[str, Any]:
    basis_frame, bars_frame = _load_replay_frames(
        source_db_path=source_db_path,
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
    )

    champion_ledger, champion_config, champion_roundtrip_payload = simulate_sample_replay(
        basis_frame=basis_frame,
        bars_frame=bars_frame,
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        source_db_path=source_db_path,
    )

    challenger_ledger, challenger_config, challenger_roundtrip_payload = _simulate_challenger_replay(
        source_db_path=source_db_path,
        basis_frame=basis_frame,
        bars_frame=bars_frame,
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
    )

    champion_ledger_for_metrics = champion_ledger.copy()
    champion_ledger_for_metrics["dt"] = pd.to_numeric(champion_ledger_for_metrics["date"].str.replace("-", "", regex=False), errors="coerce").astype("Int64")
    champion_ledger_for_metrics["equity_curve"] = pd.to_numeric(champion_ledger_for_metrics["realized_pnl"], errors="coerce").fillna(0.0) + pd.to_numeric(
        champion_ledger_for_metrics["unrealized_pnl"], errors="coerce"
    ).fillna(0.0)
    champion_roundtrips = []
    for row in champion_roundtrip_payload["roundtrips"]:
        champion_roundtrips.append({**row, "entry_type": "initial"})
    champion_metrics = _policy_aggregate_summary(
        ledger_frame=champion_ledger_for_metrics,
        roundtrips=champion_roundtrips,
        policy_id=champion_config["policy_id"],
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
    )
    champion_first_exit_row = champion_ledger_for_metrics.loc[champion_ledger_for_metrics["selected_action"] == "long_exit"].head(1)
    champion_first_exit_dt = int(champion_first_exit_row.iloc[0]["dt"]) if not champion_first_exit_row.empty else None
    champion_interval_metrics = _compute_interval_metrics(
        ledger_frame=champion_ledger_for_metrics,
        bars_frame=bars_frame,
        start_date=start_date,
        end_date=end_date,
        first_full_exit_dt=champion_first_exit_dt,
    )
    champion_metrics.update(champion_interval_metrics)

    challenger_metrics = _policy_aggregate_summary(
        ledger_frame=challenger_ledger,
        roundtrips=challenger_roundtrip_payload["roundtrips"],
        policy_id=challenger_config["policy_id"],
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
    )
    challenger_metrics.update(
        _compute_interval_metrics(
            ledger_frame=challenger_ledger,
            bars_frame=bars_frame,
            start_date=start_date,
            end_date=end_date,
            first_full_exit_dt=int(challenger_ledger.loc[challenger_ledger["selected_action"] == "long_exit", "dt"].min())
            if not challenger_ledger.loc[challenger_ledger["selected_action"] == "long_exit"].empty
            else None,
        )
    )

    champion_compare_metrics = {key: value for key, value in champion_metrics.items() if key != "completed_roundtrips"}
    challenger_compare_metrics = {key: value for key, value in challenger_metrics.items() if key != "completed_roundtrips"}
    champion_payload = {"ledger": champion_ledger, "config": champion_config, "roundtrips": champion_roundtrip_payload["roundtrips"], "summary": champion_roundtrip_payload["aggregate"]}
    challenger_summary = _build_roundtrip_summary(
        config=challenger_config,
        ledger_frame=challenger_ledger,
        roundtrip_payload=challenger_roundtrip_payload,
        bars_frame=bars_frame,
        start_date=start_date,
        end_date=end_date,
        generated_at=_utc_now(),
    )
    challenger_payload = {
        "ledger": challenger_ledger,
        "config": challenger_config,
        "roundtrips": challenger_roundtrip_payload["roundtrips"],
        "summary": challenger_summary,
    }

    compare_payload = _build_compare_payload(
        champion_payload=champion_payload,
        challenger_payload=challenger_payload,
        champion_metrics=champion_compare_metrics,
        challenger_metrics=challenger_compare_metrics,
        symbol=symbol,
        challenger_reference=output_dir,
        start_date=start_date,
        end_date=end_date,
        source_db_path=source_db_path,
    )
    missed_capture_report = _build_missed_capture_report(
        champion_ledger=champion_ledger_for_metrics,
        challenger_ledger=challenger_ledger,
        bars_frame=bars_frame,
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
    )
    reentry_reason_rollup = _build_reentry_reason_rollup(
        challenger_ledger=challenger_ledger,
        challenger_roundtrips=challenger_roundtrip_payload["roundtrips"],
    )
    decision_payload = _decision_from_compare(compare_payload)

    output_dir.mkdir(parents=True, exist_ok=True)
    sample_name = _sample_name(symbol)
    paths = {
        "compare_json": _write_json(output_dir / f"{sample_name}_capture_compare.json", compare_payload),
        "missed_capture_report": _write_json(output_dir / f"{sample_name}_missed_capture_report.json", missed_capture_report),
        "reentry_reason_rollup": _write_json(output_dir / f"{sample_name}_reentry_reason_rollup.json", reentry_reason_rollup),
        "policy_v2_daily_ledger": _write_parquet(output_dir / f"{sample_name}_policy_v2_daily_ledger.parquet", challenger_ledger),
        "policy_v2_roundtrip_summary": _write_json(output_dir / f"{sample_name}_policy_v2_roundtrip_summary.json", challenger_summary),
        "decision": _write_json(output_dir / f"{sample_name}_capture_keep_drop_hold_decision.json", decision_payload),
    }
    return {
        "ok": True,
        "output_dir": str(output_dir),
        "paths": {key: str(value) for key, value in paths.items()},
        "compare": compare_payload,
        "decision": decision_payload,
        "missed_capture_report": missed_capture_report,
        "reentry_reason_rollup": reentry_reason_rollup,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build the TRADEX 2531 capture compare replay.")
    parser.add_argument("--source-db-path", default=str(DEFAULT_SOURCE_DB_PATH))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--start-date", default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", default=DEFAULT_END_DATE)
    parser.add_argument("--freeze-date", default=DEFAULT_FREEZE_DATE)
    args = parser.parse_args(argv)

    payload = run_compare(
        source_db_path=Path(args.source_db_path).expanduser().resolve(),
        output_dir=Path(args.output_dir).expanduser().resolve(),
        symbol=str(args.symbol),
        start_date=str(args.start_date),
        end_date=str(args.end_date),
        freeze_date=str(args.freeze_date),
    )
    print(_json_text(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
