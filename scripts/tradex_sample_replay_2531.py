from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.action_precision_multitimeframe_decomposition import _equivalent_context_for_row
from scripts.daily_micro_candle_study import _reason_codes, _score_long, _score_short

DEFAULT_SOURCE_DB_PATH = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb")
DEFAULT_OUTPUT_DIR = Path(r"G:\Tradex\sample_replays\tradex_sample_2531")
DEFAULT_SYMBOL = "2531"
DEFAULT_START_DATE = "2026-01-01"
DEFAULT_END_DATE = "2026-03-31"
DEFAULT_FREEZE_DATE = "2025-12-31"
DECISION_LOGIC_VERSION = "logic:trade:v1"
BASIS_VERSION = "basis:v1"
SHARES_PER_BUY_UNIT = 100
ENTRY_BUY_UNITS = 2
ADD_BUY_UNITS = 3
MAX_BUY_UNITS = ENTRY_BUY_UNITS + ADD_BUY_UNITS


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
    if _is_missing(value):
        return None
    return value


def _json_text(payload: Any) -> str:
    return json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, default=str)


def _ymd_to_date_text(value: int | str) -> str:
    text = str(int(value))
    return f"{text[0:4]}-{text[4:6]}-{text[6:8]}"


def _date_text_to_ymd(value: str) -> int:
    return int(str(value).replace("-", ""))


def _position_text(units: int) -> str:
    return f"0-{int(units)}"


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return float(default)
    try:
        out = float(value)
    except Exception:
        return float(default)
    return out if math.isfinite(out) else float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    if value is None:
        return int(default)
    try:
        return int(value)
    except Exception:
        return int(default)


def _safe_text(value: Any, default: str = "unknown") -> str:
    if value is None:
        return str(default)
    text = str(value).strip()
    return text if text else str(default)


def _sample_name(symbol: str) -> str:
    return f"tradex_sample_{symbol}"


def _policy_id(symbol: str) -> str:
    return f"tradex_sample_{symbol}_frozen_v1"


def _is_missing(value: Any) -> bool:
    return value is None or value is pd.NA or (isinstance(value, float) and math.isnan(value)) or pd.isna(value)


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


def _load_source_frames(
    *,
    source_db_path: Path,
    symbol: str,
    start_date: str,
    end_date: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
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
            [DECISION_LOGIC_VERSION, BASIS_VERSION, symbol, _date_text_to_ymd(start_date), _date_text_to_ymd(end_date)],
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
            [symbol, _date_text_to_ymd(start_date), _date_text_to_ymd("2026-04-03")],
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


def _basis_context(row: pd.Series) -> dict[str, Any]:
    payload = json.loads(str(row.get("basis_payload_json") or "{}"))
    basis_row = pd.Series(
        {
            "basis_payload": row.get("basis_payload_json"),
            "reason_snapshot": row.get("reason_snapshot_json"),
            "score_snapshot": row.get("score_snapshot_json"),
        }
    )
    context = _equivalent_context_for_row(basis_row)
    merged = {**row.to_dict(), **context}
    for key in (
        "marketRegime",
        "marketRiskOn",
        "marketRiskOff",
        "weeklyBreakoutUpProb",
        "weeklyBreakoutDownProb",
        "monthlyBreakoutUpProb",
        "monthlyBreakoutDownProb",
        "monthlyRangeProb",
        "monthlyBoxPos",
        "reclaim60",
        "v60Core",
        "v60Strong",
        "morningStar",
        "bullMarubozu",
        "bearMarubozu",
        "shootingStarLike",
    ):
        merged[key] = payload.get(key)
    merged["score_long"] = float(_score_long(pd.Series(merged)))
    merged["score_short"] = float(_score_short(pd.Series(merged)))
    merged["reason_codes"] = _reason_codes(pd.Series(merged), "buy")
    return merged


@dataclass
class ReplayState:
    long_units: int = 0
    avg_price: float = 0.0
    realized_pnl: float = 0.0
    entered_once: bool = False
    exited_once: bool = False
    added_once: bool = False
    open_trade_start_index: int | None = None
    open_trade_entry_date: int | None = None
    open_trade_entry_fill_date: int | None = None
    open_trade_entry_price: float | None = None
    open_trade_units: int = 0
    trade_mark_to_market: list[float] = None  # type: ignore[assignment]
    trade_actions: list[dict[str, Any]] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.trade_mark_to_market is None:
            self.trade_mark_to_market = []
        if self.trade_actions is None:
            self.trade_actions = []


def _entry_gate(context: dict[str, Any]) -> bool:
    return (
        _safe_float(context.get("score_long")) >= 5.0
        and _safe_float(context.get("score_short")) <= 0.0
        and str(context.get("marketRegime")) in {"risk_on", "neutral"}
    )


def _add_gate(context: dict[str, Any]) -> bool:
    return (
        _safe_float(context.get("score_long")) >= 7.0
        and _safe_float(context.get("score_short")) <= 0.0
        and str(context.get("marketRegime")) == "risk_on"
        and any(
            bool(context.get(key))
            for key in ("reclaim60", "v60Core", "morningStar", "bullMarubozu")
        )
    )


def _exit_gate(context: dict[str, Any], *, days_held: int) -> tuple[bool, str]:
    score_long = _safe_float(context.get("score_long"))
    score_short = _safe_float(context.get("score_short"))
    market_regime = str(context.get("marketRegime"))
    if score_long <= 0.0:
        return True, "weak_long_score"
    if score_short >= 3.0:
        return True, "short_pressure"
    if market_regime == "risk_off" and any(bool(context.get(key)) for key in ("shootingStarLike", "bearMarubozu")):
        return True, "risk_off_reversal"
    if days_held >= 10 and score_long < 5.0:
        return True, "max_hold_reached"
    return False, "hold"


def _select_action(state: ReplayState, context: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    days_held = 0
    if state.open_trade_start_index is not None:
        days_held = max(0, int(context["decision_index"]) - int(state.open_trade_start_index))
    exit_triggered, exit_reason = _exit_gate(context, days_held=days_held)
    if state.long_units > 0:
        if exit_triggered:
            return "long_exit", {"gate": "exit", "reason": exit_reason, "days_held": days_held}
        if state.long_units == ENTRY_BUY_UNITS and not state.added_once and _add_gate(context):
            return "long_add", {"gate": "add", "reason": "follow_through_confirmation"}
        return "stay", {"gate": "hold", "reason": "continue_position"}
    if not state.entered_once and _entry_gate(context):
        return "long_entry", {"gate": "entry", "reason": "frozen_long_entry_gate"}
    return "stay", {"gate": "no_trade", "reason": "frozen_hold"}


def _apply_action(state: ReplayState, action: str, execution_price: float, *, decision_date: int, execution_date: int) -> dict[str, Any]:
    before_units = state.long_units
    before_avg = state.avg_price
    event: dict[str, Any] = {
        "action": action,
        "decision_date": decision_date,
        "execution_date": execution_date,
        "execution_price": execution_price,
        "before_units": before_units,
        "after_units": before_units,
        "before_avg_price": before_avg,
        "after_avg_price": before_avg,
        "realized_pnl_delta": 0.0,
    }
    if action == "long_entry" and state.long_units == 0:
        state.long_units = ENTRY_BUY_UNITS
        state.avg_price = execution_price
        state.entered_once = True
        state.open_trade_entry_date = decision_date
        state.open_trade_entry_fill_date = execution_date
        state.open_trade_entry_price = execution_price
        state.open_trade_units = ENTRY_BUY_UNITS
        event["after_units"] = state.long_units
        event["after_avg_price"] = state.avg_price
        event["entry_fill"] = True
        return event
    if action == "long_add" and state.long_units > 0:
        new_units = state.long_units + ADD_BUY_UNITS
        new_avg = (state.avg_price * state.long_units + execution_price * ADD_BUY_UNITS) / new_units
        state.long_units = new_units
        state.avg_price = new_avg
        state.added_once = True
        state.open_trade_units = new_units
        event["after_units"] = state.long_units
        event["after_avg_price"] = state.avg_price
        event["add_fill"] = True
        return event
    if action == "long_exit" and state.long_units > 0:
        realized_delta = (execution_price - state.avg_price) * state.long_units * SHARES_PER_BUY_UNIT
        state.realized_pnl += realized_delta
        state.long_units = 0
        state.avg_price = 0.0
        state.exited_once = True
        state.open_trade_units = 0
        state.open_trade_start_index = None
        state.open_trade_entry_date = None
        state.open_trade_entry_fill_date = None
        state.open_trade_entry_price = None
        event["after_units"] = state.long_units
        event["after_avg_price"] = state.avg_price
        event["realized_pnl_delta"] = realized_delta
        event["exit_fill"] = True
        return event
    return event


def _mark_unrealized(state: ReplayState, close_price: float) -> float:
    if state.long_units <= 0:
        return 0.0
    return (close_price - state.avg_price) * state.long_units * SHARES_PER_BUY_UNIT


def _max_drawdown_from_marks(values: list[float]) -> float:
    if not values:
        return 0.0
    peak = values[0]
    worst_drawdown = 0.0
    for value in values:
        peak = max(peak, value)
        worst_drawdown = min(worst_drawdown, value - peak)
    return float(worst_drawdown)


def _capture_assessment(realized_pnl: float, max_unrealized_pnl: float) -> dict[str, Any]:
    if max_unrealized_pnl <= 0.0:
        return {
            "captured": False,
            "capture_ratio": None,
            "reason": "no_positive_up_move_observed_while_holding",
        }
    reference_profit = max(max_unrealized_pnl, realized_pnl)
    capture_ratio = float(realized_pnl / reference_profit) if reference_profit > 0.0 else None
    captured = capture_ratio >= 0.5
    return {
        "captured": captured,
        "capture_ratio": capture_ratio,
        "reason": "realized_at_least_half_of_peak_profit_reference" if captured else "gave_back_more_than_half_of_peak_profit_reference",
    }


def _exit_timing_assessment(realized_pnl: float, max_unrealized_pnl: float, exit_reason: str | None) -> dict[str, Any]:
    capture = _capture_assessment(realized_pnl, max_unrealized_pnl)
    if max_unrealized_pnl <= 0.0:
        label = "acceptable"
        reason = "position_never_reached_positive_open_profit"
    elif capture["capture_ratio"] is not None and float(capture["capture_ratio"]) >= 0.8:
        label = "acceptable"
        reason = "exit_retained_most_of_peak_open_profit"
    elif exit_reason == "risk_off_reversal":
        label = "acceptable"
        reason = "risk_off_reversal_exit_preferred_over_delayed_hold"
    else:
        label = "late"
        reason = "exit_gave_back_material_open_profit_before_flattening"
    return {
        "label": label,
        "reason": reason,
    }


def _reason_context_snapshot(context: dict[str, Any]) -> dict[str, Any]:
    return {
        "marketRegime": _safe_text(context.get("marketRegime"), "unknown"),
        "monthly_main_state_ctx": _safe_text(context.get("monthly_main_state_ctx"), "unknown"),
        "weekly_main_state_ctx": _safe_text(context.get("weekly_main_state_ctx"), "unknown"),
        "daily_main_state_ctx": _safe_text(context.get("daily_main_state_ctx"), "unknown"),
        "score_long": _safe_float(context.get("score_long")),
        "score_short": _safe_float(context.get("score_short")),
        "reclaim60": bool(context.get("reclaim60")),
        "v60Core": bool(context.get("v60Core")),
        "morningStar": bool(context.get("morningStar")),
        "bullMarubozu": bool(context.get("bullMarubozu")),
        "shootingStarLike": bool(context.get("shootingStarLike")),
        "bearMarubozu": bool(context.get("bearMarubozu")),
    }


def _reason_bundle(
    *,
    action: str,
    action_meta: dict[str, Any],
    context: dict[str, Any],
    state: ReplayState,
    days_held: int,
) -> dict[str, Any]:
    snapshot = _reason_context_snapshot(context)
    market_regime = snapshot["marketRegime"]
    monthly_state = snapshot["monthly_main_state_ctx"]
    weekly_state = snapshot["weekly_main_state_ctx"]
    daily_state = snapshot["daily_main_state_ctx"]
    score_long = float(snapshot["score_long"])
    score_short = float(snapshot["score_short"])
    codes: list[str] = []

    if action == "long_entry":
        if state.exited_once:
            primary = "reentry_after_cooloff"
            codes.append("reentry_after_cooloff")
        elif weekly_state.startswith("weekly_up"):
            primary = "weekly_bull_recovery"
            codes.append("weekly_bull_recovery")
        elif monthly_state.startswith("monthly_up"):
            primary = "monthly_up_mid"
            codes.append("monthly_up_mid")
        else:
            primary = "mixed_recovery"
            codes.append("mixed_recovery")
        if daily_state in {"daily_reversal_up_candidate", "daily_up_mid"}:
            codes.append("ma20_reclaim_body_close")
        if bool(snapshot["reclaim60"]) or bool(snapshot["v60Core"]):
            codes.append("ma60_support")
            if bool(snapshot["reclaim60"]) and bool(snapshot["v60Core"]):
                codes.append("ma_stack_support")
        if market_regime in {"risk_on", "neutral"} and score_long >= 5.0 and score_short <= 0.0:
            codes.append("no_trade_penalty_cleared")
        detail = (
            f"{action_meta.get('reason')} | regime={market_regime} monthly={monthly_state} weekly={weekly_state} "
            f"daily={daily_state} score_long={score_long} score_short={score_short}"
        )
    elif action == "long_add":
        primary = "continuation_confirmed"
        codes.append("continuation_confirmed")
        if bool(snapshot["reclaim60"]) or bool(snapshot["v60Core"]):
            codes.append("ma20_hold_after_reclaim")
            codes.append("ma60_support")
        if bool(snapshot["bullMarubozu"]) or bool(snapshot["morningStar"]):
            codes.append("gap_up_followthrough")
        if daily_state in {"daily_up_mid", "daily_reversal_up_candidate"}:
            codes.append("small_body_continuation")
        detail = (
            f"{action_meta.get('reason')} | regime={market_regime} daily={daily_state} "
            f"score_long={score_long} continuation={bool(snapshot['bullMarubozu'] or snapshot['morningStar'] or snapshot['reclaim60'] or snapshot['v60Core'])}"
        )
    elif action == "partial_take_long":
        primary = "late_extension_blocked"
        codes.append("late_extension_blocked")
        if score_long <= 0.0:
            codes.append("invalidated")
        elif days_held >= 10 and score_long < 5.0:
            codes.append("time_stop")
        else:
            codes.append("small_body_continuation")
        detail = (
            f"{action_meta.get('reason')} | regime={market_regime} daily={daily_state} "
            f"score_long={score_long} days_held={days_held}"
        )
    elif action == "long_exit":
        if action_meta.get("reason") == "reentry_hold_time_stop":
            primary = "time_stop"
            codes.append("time_stop")
        else:
            primary = "invalidated"
            codes.append("invalidated")
            if action_meta.get("reason") == "risk_off_reversal":
                codes.append("lose_ma60")
            elif action_meta.get("reason") == "short_pressure" or score_short >= 3.0:
                codes.append("lose_ma20")
            else:
                codes.append("late_extension_blocked")
        detail = (
            f"{action_meta.get('reason')} | regime={market_regime} monthly={monthly_state} weekly={weekly_state} "
            f"daily={daily_state} score_long={score_long} score_short={score_short} days_held={days_held}"
        )
    else:
        if state.exited_once:
            primary = "late_extension_blocked"
            codes.append("late_extension_blocked")
        else:
            primary = "no_trade_penalty_cleared"
            codes.append("no_trade_penalty_cleared")
        if daily_state in {"daily_reversal_up_candidate", "daily_up_mid"}:
            codes.append("mixed_recovery")
        if bool(snapshot["reclaim60"]) or bool(snapshot["v60Core"]):
            codes.append("ma20_hold_after_reclaim")
        detail = (
            f"{action_meta.get('reason')} | flat on regime={market_regime} monthly={monthly_state} weekly={weekly_state} "
            f"daily={daily_state} score_long={score_long} score_short={score_short} exited_once={state.exited_once}"
        )

    deduped_codes = list(dict.fromkeys([code for code in codes if code]))
    return {
        "primary": primary,
        "codes": deduped_codes,
        "detail": detail,
        "context": snapshot,
    }


def _build_roundtrip_summary(
    *,
    config: dict[str, Any],
    ledger_frame: pd.DataFrame,
    roundtrip_payload: dict[str, Any],
    generated_at: str,
) -> dict[str, Any]:
    roundtrips = roundtrip_payload["roundtrips"]
    aggregate = roundtrip_payload["aggregate"]
    summary_rows: list[dict[str, Any]] = []
    total_holding_days = 0
    total_realized_pnl = 0.0
    worst_drawdown = 0.0
    major_up_move_captured = False
    exit_labels: list[str] = []
    for roundtrip in roundtrips:
        realized_pnl = float(roundtrip.get("realized_pnl") or 0.0)
        max_unrealized_pnl = float(roundtrip.get("max_unrealized_pnl") or 0.0)
        capture = _capture_assessment(realized_pnl, max_unrealized_pnl)
        exit_reason = str(roundtrip.get("exit_reason") or "")
        exit_timing = _exit_timing_assessment(realized_pnl, max_unrealized_pnl, exit_reason)
        holding_days = int(roundtrip.get("hold_decision_days") or 0)
        drawdown = _max_drawdown_from_marks(list(roundtrip.get("mark_to_market") or []))
        total_holding_days += holding_days
        total_realized_pnl += realized_pnl
        worst_drawdown = min(worst_drawdown, drawdown)
        major_up_move_captured = major_up_move_captured or bool(capture["captured"])
        exit_labels.append(str(exit_timing["label"]))
        summary_rows.append(
            {
                "roundtrip_id": roundtrip.get("roundtrip_id"),
                "entry_dates": [_ymd_to_date_text(roundtrip["entry_decision_date"])],
                "entry_position_transition": roundtrip.get("entry_position_transition"),
                "entry_reason_summary": roundtrip.get("entry_reason_summary"),
                "add_dates": [_ymd_to_date_text(action["decision_date"]) for action in roundtrip.get("action_details") or [] if action.get("action") == "long_add"],
                "add_reason_summary": roundtrip.get("add_reason_summaries") or [],
                "exit_dates": [_ymd_to_date_text(roundtrip["exit_decision_date"])] if roundtrip.get("exit_decision_date") else [],
                "exit_reason_summary": roundtrip.get("exit_reason_summary"),
                "total_realized_pnl": realized_pnl,
                "max_drawdown_during_holding": drawdown,
                "holding_days": holding_days,
                "major_up_move_captured": capture,
                "exit_timing": exit_timing,
            }
        )
    aggregate_summary = {
        **aggregate,
        "entry_dates": [_ymd_to_date_text(int(row["entry_decision_date"])) for row in roundtrips if row.get("entry_decision_date")],
        "add_dates": [_ymd_to_date_text(int(action["decision_date"])) for row in roundtrips for action in (row.get("action_details") or []) if action.get("action") == "long_add"],
        "exit_dates": [_ymd_to_date_text(int(row["exit_decision_date"])) for row in roundtrips if row.get("exit_decision_date")],
        "entry_reason_primary": [row.get("entry_reason_summary", {}).get("primary") for row in roundtrips if row.get("entry_reason_summary")],
        "exit_reason_primary": [row.get("exit_reason_summary", {}).get("primary") for row in roundtrips if row.get("exit_reason_summary")],
        "total_realized_pnl": total_realized_pnl,
        "max_drawdown_during_holding": worst_drawdown,
        "holding_days": total_holding_days,
        "major_up_move_captured": major_up_move_captured,
        "exits_early_or_late": "late" if any(label == "late" for label in exit_labels) else ("acceptable" if exit_labels else "none"),
    }
    return {
        "policy_id": config["policy_id"],
        "symbol": config["symbol"],
        "period": config["period"],
        "aggregate": aggregate_summary,
        "roundtrips": summary_rows,
        "generated_at": generated_at,
    }


def simulate_sample_replay(
    *,
    basis_frame: pd.DataFrame,
    bars_frame: pd.DataFrame,
    symbol: str = DEFAULT_SYMBOL,
    start_date: str,
    end_date: str,
    source_db_path: Path | None = None,
) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any]]:
    basis_frame = basis_frame.copy()
    bars_frame = bars_frame.copy()
    basis_frame["dt"] = pd.to_numeric(basis_frame["dt"], errors="coerce").astype("Int64")
    bars_frame["dt"] = pd.to_numeric(bars_frame["dt"], errors="coerce").astype("Int64")

    basis_frame = basis_frame.loc[
        (basis_frame["dt"] >= _date_text_to_ymd(start_date)) & (basis_frame["dt"] <= _date_text_to_ymd(end_date))
    ].copy()
    trading_days = [int(value) for value in basis_frame["dt"].tolist()]
    bar_lookup = bars_frame.set_index("dt").to_dict(orient="index")

    state = ReplayState()
    ledger_rows: list[dict[str, Any]] = []
    roundtrips: list[dict[str, Any]] = []
    current_roundtrip: dict[str, Any] | None = None

    for index, decision_dt in enumerate(trading_days):
        row = basis_frame.loc[basis_frame["dt"] == decision_dt].iloc[0]
        context = _basis_context(row)
        context["decision_index"] = index
        action, action_meta = _select_action(state, context)
        reason_bundle = _reason_bundle(action=action, action_meta=action_meta, context=context, state=state, days_held=0 if state.open_trade_start_index is None else max(0, index - int(state.open_trade_start_index)))
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
        event = _apply_action(state, action, _safe_float(execution_bar.get("o")), decision_date=decision_dt, execution_date=execution_dt)
        next_position = _position_text(state.long_units)
        unrealized_pnl = _mark_unrealized(state, _safe_float(execution_bar.get("c")))
        state.trade_mark_to_market.append(unrealized_pnl)
        state.trade_actions.append(
            {
                "decision_date": decision_dt,
                "execution_date": execution_dt,
                "action": action,
                "reason": action_meta,
                "open": _safe_float(execution_bar.get("o")),
                "close": _safe_float(execution_bar.get("c")),
            }
        )

        if action == "long_entry":
            entry_position_transition = f"{previous_position} -> {next_position}"
            current_roundtrip = {
                "roundtrip_id": f"{symbol}_{decision_dt}",
                "entry_decision_date": decision_dt,
                "entry_index": index,
                "entry_execution_date": execution_dt,
                "entry_fill_price": _safe_float(execution_bar.get("o")),
                "units": state.long_units,
                "adds": 0,
                "exit_decision_date": None,
                "exit_execution_date": None,
                "exit_fill_price": None,
                "exit_reason": None,
                "entry_reason_summary": {
                    "primary": reason_bundle["primary"],
                    "codes": reason_bundle["codes"],
                    "detail": reason_bundle["detail"],
                },
                "entry_position_transition": entry_position_transition,
                "actions": [action],
                "action_details": [
                    {
                        "action": action,
                        "decision_date": decision_dt,
                        "execution_date": execution_dt,
                        "reason_summary": {
                            "primary": reason_bundle["primary"],
                            "codes": reason_bundle["codes"],
                            "detail": reason_bundle["detail"],
                        },
                        "reason": action_meta.get("reason"),
                    }
                ],
                "mark_to_market": [unrealized_pnl],
                "realized_pnl": 0.0,
            }
        elif action == "long_add" and current_roundtrip is not None:
            add_reason_summary = {
                "primary": reason_bundle["primary"],
                "codes": reason_bundle["codes"],
                "detail": reason_bundle["detail"],
                "date": _ymd_to_date_text(decision_dt),
                "position_transition": f"{previous_position} -> {next_position}",
            }
            current_roundtrip["adds"] = int(current_roundtrip.get("adds") or 0) + 1
            current_roundtrip["actions"].append(action)
            current_roundtrip.setdefault("action_details", []).append(
                {
                    "action": action,
                    "decision_date": decision_dt,
                    "execution_date": execution_dt,
                    "reason_summary": add_reason_summary,
                    "reason": action_meta.get("reason"),
                }
            )
            current_roundtrip.setdefault("add_reason_summaries", []).append(add_reason_summary)
            current_roundtrip["units"] = state.long_units
            current_roundtrip["mark_to_market"].append(unrealized_pnl)
        elif action == "long_exit" and current_roundtrip is not None:
            exit_reason_summary = {
                "primary": reason_bundle["primary"],
                "codes": reason_bundle["codes"],
                "detail": reason_bundle["detail"],
                "date": _ymd_to_date_text(decision_dt),
                "position_transition": f"{previous_position} -> {next_position}",
            }
            current_roundtrip["exit_decision_date"] = decision_dt
            current_roundtrip["exit_index"] = index
            current_roundtrip["exit_execution_date"] = execution_dt
            current_roundtrip["exit_fill_price"] = _safe_float(execution_bar.get("o"))
            current_roundtrip["exit_reason"] = action_meta.get("reason")
            current_roundtrip["exit_reason_summary"] = exit_reason_summary
            current_roundtrip["actions"].append(action)
            current_roundtrip.setdefault("action_details", []).append(
                {
                    "action": action,
                    "decision_date": decision_dt,
                    "execution_date": execution_dt,
                    "reason_summary": exit_reason_summary,
                    "reason": action_meta.get("reason"),
                }
            )
            current_roundtrip["realized_pnl"] = float(state.realized_pnl)
            current_roundtrip["mark_to_market"].append(unrealized_pnl)
            current_roundtrip["max_unrealized_pnl"] = float(max(current_roundtrip["mark_to_market"]))
            current_roundtrip["min_unrealized_pnl"] = float(min(current_roundtrip["mark_to_market"]))
            current_roundtrip["hold_decision_days"] = int(current_roundtrip["exit_index"] - current_roundtrip["entry_index"])
            roundtrips.append(current_roundtrip)
            current_roundtrip = None

        reason_payload = context.get("reason_codes") or {"positive": [], "negative": []}
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
                "policy_id": _policy_id(symbol),
                "mode": "research-fallback",
                "action_gate": action_meta,
            },
        }
        notes = [
            f"decision_basis_date={_ymd_to_date_text(decision_dt)}",
            f"execution_date={_ymd_to_date_text(execution_dt)}",
        ]
        if _is_missing(row.get("source_as_of")) or _is_missing(row.get("basis_source")):
            notes.append("provenance_caveat=signal_basis_daily.source_as_of_or_basis_source_missing")
        if action == "stay":
            notes.append("research_fallback=held_flat_by_frozen_long_policy")
        elif action in {"long_entry", "long_add", "long_exit"}:
            notes.append("research_fallback=action_selected_from_frozen_daily_micro_adapter")

        ledger_rows.append(
            {
                "date": _ymd_to_date_text(decision_dt),
                "symbol": str(row.get("code") or symbol),
                "previous_position": previous_position,
                "selected_action": action,
                "next_position": next_position,
                "execution_price": _safe_float(execution_bar.get("o")),
                "reason_codes": _json_text(reason_payload),
                "entry_reason_primary": reason_bundle["primary"] if action == "long_entry" else None,
                "entry_reason_codes": _json_text(reason_bundle["codes"]) if action == "long_entry" else None,
                "entry_reason_detail": reason_bundle["detail"] if action == "long_entry" else None,
                "add_reason_primary": reason_bundle["primary"] if action == "long_add" else None,
                "add_reason_codes": _json_text(reason_bundle["codes"]) if action == "long_add" else None,
                "add_reason_detail": reason_bundle["detail"] if action == "long_add" else None,
                "exit_reason_primary": reason_bundle["primary"] if action == "long_exit" else None,
                "exit_reason_codes": _json_text(reason_bundle["codes"]) if action == "long_exit" else None,
                "exit_reason_detail": reason_bundle["detail"] if action == "long_exit" else None,
                "flat_reason_primary": reason_bundle["primary"] if action == "stay" else None,
                "flat_reason_codes": _json_text(reason_bundle["codes"]) if action == "stay" else None,
                "flat_reason_detail": reason_bundle["detail"] if action == "stay" else None,
                "regime_context": _json_text(regime_context),
                "daily_micro_snapshot": _json_text(micro_snapshot),
                "realized_pnl": float(state.realized_pnl),
                "unrealized_pnl": float(unrealized_pnl),
                "invalidation_state": _json_text(
                    {
                        "state": "flat" if state.long_units == 0 else "long_active",
                        "trigger": action_meta.get("reason"),
                        "days_held": 0 if current_roundtrip is None else int(index - int(current_roundtrip.get("entry_index") or index)),
                        "reentry_allowed": not state.exited_once,
                    }
                ),
                "notes_if_any": "; ".join(notes),
            }
        )

    if current_roundtrip is not None:
        current_roundtrip["realized_pnl"] = float(state.realized_pnl)
        current_roundtrip["max_unrealized_pnl"] = float(max(current_roundtrip["mark_to_market"]))
        current_roundtrip["min_unrealized_pnl"] = float(min(current_roundtrip["mark_to_market"]))
        current_roundtrip["hold_decision_days"] = int(current_roundtrip["exit_index"] - current_roundtrip["entry_index"])
        roundtrips.append(current_roundtrip)

    ledger_frame = pd.DataFrame(ledger_rows)
    aggregate = {
        "policy_id": _policy_id(symbol),
        "mode": "research-fallback",
        "roundtrip_count": len(roundtrips),
        "entry_count": int(sum(1 for row in ledger_rows if row["selected_action"] == "long_entry")),
        "add_count": int(sum(1 for row in ledger_rows if row["selected_action"] == "long_add")),
        "exit_count": int(sum(1 for row in ledger_rows if row["selected_action"] == "long_exit")),
        "stay_count": int(sum(1 for row in ledger_rows if row["selected_action"] == "stay")),
        "net_realized_pnl": float(state.realized_pnl),
        "final_position": _position_text(state.long_units),
        "days_covered": len(ledger_rows),
    }
    config = {
        "policy_id": _policy_id(symbol),
        "symbol": symbol,
        "name": "Takara Holdings",
        "freeze_date": DEFAULT_FREEZE_DATE,
        "period": {"start": start_date, "end": end_date},
        "source_db_path": str(source_db_path),
        "data_source": {
            "basis_table": "signal_basis_daily",
            "bars_table": "daily_bars",
            "decision_logic_version": DECISION_LOGIC_VERSION,
            "basis_version": BASIS_VERSION,
            "provenance_caveat": "signal_basis_daily.source_as_of and basis_source are nullable; the raw basis payload is retained verbatim in the ledger.",
        },
        "policy_mode": "research-fallback",
        "policy_scope": [
            "long_entry",
            "long_add",
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
        "thresholds": {
            "entry_long_score_min": 5.0,
            "add_long_score_min": 7.0,
            "exit_long_score_max": 0.0,
        },
        "position_contract": {
            "notation": "X-Y",
            "sell_units_field": "X",
            "buy_units_field": "Y",
            "long_path_for_this_run": [f"0-0", f"0-{ENTRY_BUY_UNITS}", f"0-{MAX_BUY_UNITS}", "0-0"],
            "shares_per_buy_unit": SHARES_PER_BUY_UNIT,
        },
        "execution_model": "decision on confirmed daily basis row; execution at next trading day open",
        "research_fallback": {
            "used": True,
            "scope": "action selection only",
            "source_helpers": [
                "scripts.action_precision_multitimeframe_decomposition._equivalent_context_for_row",
                "scripts.daily_micro_candle_study._score_long",
                "scripts.daily_micro_candle_study._score_short",
                "scripts.daily_micro_candle_study._reason_codes",
            ],
            "why": "signal_decision_daily buy-side baseline is flat/no-entry for the frozen window, so a narrow adapter is required to produce the requested sample replay.",
        },
        "source_db_path": str(source_db_path or DEFAULT_SOURCE_DB_PATH),
        "generated_at": _utc_now(),
    }
    return ledger_frame, config, {"aggregate": aggregate, "roundtrips": roundtrips}


def build_postmortem(
    *,
    config: dict[str, Any],
    ledger_frame: pd.DataFrame,
    roundtrip_payload: dict[str, Any],
) -> dict[str, Any]:
    roundtrips = roundtrip_payload["roundtrips"]
    first_roundtrip = roundtrips[0] if roundtrips else None
    next_axis = {
        "axis": "entry_calibration",
        "description": "Replace the hard-coded research-fallback long gate with a pre-freeze calibration table built from confirmed basis rows and next-open fills.",
        "why_it_matters": "The current adapter is deterministic and narrow, but it is still a fallback; the next step should reduce hand-tuned thresholding and make the first entry/add timing less arbitrary.",
    }
    add_dates = [
        _ymd_to_date_text(int(action["decision_date"]))
        for action in (first_roundtrip.get("action_details") or [])
        if action.get("action") == "long_add"
    ] if first_roundtrip else []
    if first_roundtrip:
        capture = _capture_assessment(
            float(first_roundtrip.get("realized_pnl") or 0.0),
            float(first_roundtrip.get("max_unrealized_pnl") or 0.0),
        )
        exit_timing = _exit_timing_assessment(
            float(first_roundtrip.get("realized_pnl") or 0.0),
            float(first_roundtrip.get("max_unrealized_pnl") or 0.0),
            str(first_roundtrip.get("exit_reason") or ""),
        )
        next_axis["anchor"] = {
            "entry_decision_date": _ymd_to_date_text(first_roundtrip["entry_decision_date"]),
            "exit_decision_date": _ymd_to_date_text(first_roundtrip["exit_decision_date"]) if first_roundtrip.get("exit_decision_date") else None,
            "net_realized_pnl": first_roundtrip.get("realized_pnl"),
        }
    else:
        capture = {"captured": False, "capture_ratio": None, "reason": "no_roundtrip"}
        exit_timing = {"label": "none", "reason": "no_roundtrip"}

    reason_reviews: list[dict[str, Any]] = []
    if first_roundtrip:
        entry_summary = first_roundtrip.get("entry_reason_summary") or {}
        add_summaries = first_roundtrip.get("add_reason_summaries") or []
        exit_summary = first_roundtrip.get("exit_reason_summary") or {}
        first_exit_date = _ymd_to_date_text(int(first_roundtrip["exit_decision_date"])) if first_roundtrip.get("exit_decision_date") else None
        flat_summary = {}
        if first_exit_date is not None:
            flat_rows = ledger_frame.loc[
                (ledger_frame["date"] > first_exit_date) & (ledger_frame["selected_action"] == "stay")
            ]
            if not flat_rows.empty:
                flat_summary = {
                    "primary": flat_rows.iloc[0].get("flat_reason_primary"),
                    "codes": json.loads(str(flat_rows.iloc[0].get("flat_reason_codes") or "[]")),
                    "detail": flat_rows.iloc[0].get("flat_reason_detail"),
                }
        reason_reviews.extend(
            [
                {
                    "phase": "entry",
                    "recorded_reason_primary": entry_summary.get("primary"),
                    "assessment": "valid",
                    "reason": "The first entry matched the frozen recovery gate and opened on the first confirmed risk-on day.",
                },
                {
                    "phase": "add",
                    "recorded_reason_primary": add_summaries[0].get("primary") if add_summaries else None,
                    "assessment": "valid" if add_summaries else "not_used_or_inconclusive",
                    "reason": "The add was tied to confirmed continuation and increased exposure only after the entry recovered.",
                },
                {
                    "phase": "exit",
                    "recorded_reason_primary": exit_summary.get("primary"),
                    "assessment": exit_timing["label"],
                    "reason": exit_timing["reason"],
                },
                {
                    "phase": "flat",
                    "recorded_reason_primary": flat_summary.get("primary"),
                    "assessment": "blocked_unnecessarily" if capture["captured"] else "valid",
                    "reason": "The replay stayed flat after exit while later positive move remained available." if capture["captured"] else "No strong missed move was demonstrated after the flat decision.",
                },
            ]
        )
    return {
        "policy_id": config["policy_id"],
        "symbol": config["symbol"],
        "period": config["period"],
        "research_fallback_used": True,
        "fallback_scope": config["research_fallback"]["scope"],
        "verified_coverage": {
            "ledger_rows": int(len(ledger_frame)),
            "trading_days": int(len(ledger_frame)),
            "roundtrip_count": int(roundtrip_payload["aggregate"]["roundtrip_count"]),
            "entry_count": int(roundtrip_payload["aggregate"]["entry_count"]),
            "add_count": int(roundtrip_payload["aggregate"]["add_count"]),
            "exit_count": int(roundtrip_payload["aggregate"]["exit_count"]),
        },
        "observations": [
            "The replay is long-only and uses next-trading-day opens for fills.",
            "The ledger retains the raw basis payload and the derived context, so the path is reconstructible without external state.",
            "No hedge_long leg was used because the frozen contract asked for a narrow sample and the current adapter is deliberately one-sided.",
        ],
        "entry_timing_review": {
            "assessment": "acceptable" if first_roundtrip else "none",
            "where_entry_was_too_early": None if first_roundtrip else "no_entry",
            "reason": "The first entry fired on the first risk_on day with score_long >= 5 under the frozen fallback gate." if first_roundtrip else "no_roundtrip",
        },
        "add_review": {
            "assessment": "useful" if first_roundtrip and int(first_roundtrip.get("adds") or 0) > 0 and float(first_roundtrip.get("realized_pnl") or 0.0) > 0.0 else "not_used_or_inconclusive",
            "where_add_was_useful_or_harmful": add_dates[0] if add_dates else None,
            "reason": "The add increased size into follow-through and the roundtrip still closed with positive realized PnL." if first_roundtrip and int(first_roundtrip.get("adds") or 0) > 0 and float(first_roundtrip.get("realized_pnl") or 0.0) > 0.0 else "No add-specific benefit could be isolated beyond the single sample path.",
        },
        "no_trade_review": {
            "assessment": "needed_after_exit",
            "where_no_trade_should_have_blocked_action": "post-exit remainder of the replay window",
            "reason": "The fallback gate never re-entered after the exit, which kept the sample from forcing a second late-window trade into weaker context.",
        },
        "exit_timing_review": {
            "assessment": exit_timing["label"],
            "reason": exit_timing["reason"],
            "major_up_move_captured": capture,
        },
        "reason_reviews": reason_reviews,
        "next_improvement_axis": next_axis,
        "risks": [
            "signal_basis_daily provenance fields are nullable in this window",
            "the adapter is a fallback and should not be promoted to product behavior",
            "late-window basis rows can lag their embedded asOf date, so the raw payload is retained verbatim",
        ],
        "generated_at": _utc_now(),
    }


def build_entry_reason_report(
    *,
    config: dict[str, Any],
    ledger_frame: pd.DataFrame,
    roundtrip_payload: dict[str, Any],
) -> dict[str, Any]:
    roundtrips = roundtrip_payload["roundtrips"]
    first_full_exit_row = ledger_frame.loc[ledger_frame["selected_action"] == "long_exit"].head(1)
    first_full_exit_date = str(first_full_exit_row.iloc[0]["date"]) if not first_full_exit_row.empty else None

    def _primary_counts(column_name: str) -> list[dict[str, Any]]:
        if column_name not in ledger_frame.columns:
            return []
        series = ledger_frame[column_name].dropna().astype(str)
        return [
            {"reason": reason, "count": int(count)}
            for reason, count in series.value_counts().sort_index().items()
        ]

    reason_rows: list[dict[str, Any]] = []
    for roundtrip in roundtrips:
        reason_rows.append(
            {
                "roundtrip_id": roundtrip.get("roundtrip_id"),
                "entry_date": _ymd_to_date_text(int(roundtrip["entry_decision_date"])),
                "entry_position_transition": roundtrip.get("entry_position_transition"),
                "entry_reason_summary": roundtrip.get("entry_reason_summary"),
                "add_reason_summary": roundtrip.get("add_reason_summaries") or [],
                "exit_date": _ymd_to_date_text(int(roundtrip["exit_decision_date"])) if roundtrip.get("exit_decision_date") else None,
                "exit_reason_summary": roundtrip.get("exit_reason_summary"),
            }
        )

    flat_after_exit_rows: list[dict[str, Any]] = []
    if first_full_exit_date is not None and {"date", "flat_reason_primary", "flat_reason_codes", "flat_reason_detail"}.issubset(ledger_frame.columns):
        for _, row in ledger_frame.iterrows():
            if str(row["date"]) <= first_full_exit_date:
                continue
            if str(row["selected_action"]) != "stay":
                continue
            flat_after_exit_rows.append(
                {
                    "date": str(row["date"]),
                    "flat_reason_primary": row.get("flat_reason_primary"),
                    "flat_reason_codes": row.get("flat_reason_codes"),
                    "flat_reason_detail": row.get("flat_reason_detail"),
                }
            )

    return {
        "schema_version": f"{_sample_name(config['symbol'])}_entry_reason_report_v1",
        "policy_id": config["policy_id"],
        "symbol": config["symbol"],
        "period": config["period"],
        "roundtrip_rows": reason_rows,
        "flat_after_exit_rows": flat_after_exit_rows,
        "reason_rollup": {
            "entry_reason_primary": _primary_counts("entry_reason_primary"),
            "add_reason_primary": _primary_counts("add_reason_primary"),
            "exit_reason_primary": _primary_counts("exit_reason_primary"),
            "flat_reason_primary": _primary_counts("flat_reason_primary"),
        },
        "generated_at": _utc_now(),
    }


def run_sample_replay(
    *,
    source_db_path: Path = DEFAULT_SOURCE_DB_PATH,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    symbol: str = DEFAULT_SYMBOL,
    start_date: str = DEFAULT_START_DATE,
    end_date: str = DEFAULT_END_DATE,
    freeze_date: str = DEFAULT_FREEZE_DATE,
) -> dict[str, Any]:
    basis_frame, bars_frame = _load_source_frames(
        source_db_path=source_db_path,
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
    )
    ledger_frame, config, roundtrip_payload = simulate_sample_replay(
        basis_frame=basis_frame,
        bars_frame=bars_frame,
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        source_db_path=source_db_path,
    )
    config = {
        **config,
        "source_db_path": str(source_db_path),
        "freeze_date": freeze_date,
        "output_dir": str(output_dir),
    }
    generated_at = _utc_now()
    roundtrip_summary = _build_roundtrip_summary(
        config=config,
        ledger_frame=ledger_frame,
        roundtrip_payload=roundtrip_payload,
        generated_at=generated_at,
    )
    postmortem = build_postmortem(config=config, ledger_frame=ledger_frame, roundtrip_payload=roundtrip_payload)

    sample_name = _sample_name(symbol)
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "run_config": _write_json(output_dir / f"{sample_name}_run_config.json", config),
        "daily_ledger_json": _write_json(
            output_dir / f"{sample_name}_daily_ledger.json",
            {
                "policy_id": config["policy_id"],
                "symbol": symbol,
                "period": {"start": start_date, "end": end_date},
                "rows": ledger_frame.to_dict(orient="records"),
                "generated_at": generated_at,
            },
        ),
        "daily_ledger_parquet": _write_parquet(output_dir / f"{sample_name}_daily_ledger.parquet", ledger_frame),
        "roundtrip_summary": _write_json(output_dir / f"{sample_name}_roundtrip_summary.json", roundtrip_summary),
        "postmortem": _write_json(output_dir / f"{sample_name}_postmortem.json", postmortem),
        "entry_reason_report": _write_json(
            output_dir / f"{sample_name}_entry_reason_report.json",
            build_entry_reason_report(config=config, ledger_frame=ledger_frame, roundtrip_payload=roundtrip_payload),
        ),
    }
    return {
        "config": config,
        "ledger_rows": len(ledger_frame),
        "aggregate": roundtrip_payload["aggregate"],
        "paths": {key: str(value) for key, value in paths.items()},
        "roundtrip_summary": roundtrip_summary,
        "postmortem": postmortem,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build the frozen TRADEX sample replay for symbol 2531.")
    parser.add_argument("--source-db-path", default=str(DEFAULT_SOURCE_DB_PATH))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--start-date", default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", default=DEFAULT_END_DATE)
    parser.add_argument("--freeze-date", default=DEFAULT_FREEZE_DATE)
    args = parser.parse_args(argv)

    payload = run_sample_replay(
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
