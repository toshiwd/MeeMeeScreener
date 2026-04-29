from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.tradex_chart_first_replay import (  # noqa: E402
    ChartState,
    DEFAULT_SOURCE_DB_PATH,
    ENTRY_LONG_UNITS,
    CONFIRMED_LONG_UNITS,
    FULL_LONG_UNITS,
    HEAVY_HEDGE_UNITS,
    LIGHT_HEDGE_UNITS,
    SHARES_PER_UNIT,
    _apply_state_transition,
    _date_text_to_ymd,
    _desired_targets,
    _fill_long_entry,
    _fill_long_exit,
    _fill_short_cover,
    _fill_short_entry,
    _json_text,
    _load_source_frames,
    _mark_unrealized,
    _prepare_chart_frame,
    _safe_float,
    _ymd_to_date_text,
)

ACTION_SPACE = (
    "watch",
    "trial_buy",
    "long_add",
    "hedge_add",
    "long_reduce",
    "hedge_reduce",
    "stop_add",
    "exit_all",
)

FAILED_FOLLOWTHROUGH_CANDIDATE = "failed_followthrough_time_stop_v1"
HARD_INVALIDATION_CANDIDATE = "hard_invalidation_exit_v1"
HARD_INVALIDATION_SEVERITY_CANDIDATE = "hard_invalidation_exit_severity_v2_loss_side_override"
HARD_INVALIDATION_PROFIT_PRESERVATION_GUARD_CANDIDATE = "hard_invalidation_profit_preservation_guard_v1"
HARD_INVALIDATION_NON_EXIT_LATE_EXTENSION_HEDGE_CANDIDATE = "hard_invalidation_non_exit_late_extension_hedge_v1"
PROFIT_TAKE_REENTRY_CANDIDATE = "profit_take_reentry_delay_v1"
BAD_REENTRY_AFTER_PROFIT_TAKE_CANDIDATE = "bad_reentry_after_profit_take_v1"
STACK_CANDIDATE = "iizuka_kept_axis_stack_v1"
TRACE_SCHEMA_VERSION = "tradex_iizuka_trade_learning_loop_action_trace_v2"
HARD_INVALIDATION_ACTIVE_CANDIDATES = {
    HARD_INVALIDATION_CANDIDATE,
    HARD_INVALIDATION_SEVERITY_CANDIDATE,
    HARD_INVALIDATION_PROFIT_PRESERVATION_GUARD_CANDIDATE,
    HARD_INVALIDATION_NON_EXIT_LATE_EXTENSION_HEDGE_CANDIDATE,
}
STACKED_AXIS_CANDIDATES = (
    FAILED_FOLLOWTHROUGH_CANDIDATE,
    HARD_INVALIDATION_CANDIDATE,
    BAD_REENTRY_AFTER_PROFIT_TAKE_CANDIDATE,
)

ERROR_LABELS = (
    "valid_read",
    "reasonable_but_failed",
    "too_early_entry",
    "too_early_add",
    "over_accumulation_before_confirmation",
    "failed_breakout_misread",
    "bad_pullback_misread",
    "late_time_stop",
    "missed_reduce",
    "missed_exit",
    "hedge_correct_but_long_too_large",
    "false_exit_on_valid_pullback",
    "blocked_valid_winner",
    "success_pattern_preserved",
    "hard_invalidation_exit_applicable",
    "profit_take_reasonable",
    "profit_take_too_early",
    "profit_take_too_late",
    "missed_reentry",
    "valid_reentry",
    "bad_reentry_blocked",
    "false_reentry",
    "opportunity_loss_after_profit_take",
    "bad_reentry_after_profit_take",
    "bad_reentry_after_profit_take_applicable",
    "reentry_blocked",
    "false_reentry_avoided",
)

CASE_SPECS = (
    {
        "symbol": "2317",
        "name": "Systena 2317",
        "role": "failure_control",
        "applicable_patch_ids": [FAILED_FOLLOWTHROUGH_CANDIDATE],
        "start_date": "2025-12-02",
        "end_date": "2026-02-04",
        "trade_start_date": "2025-12-02",
        "expected_themes": [
            "failed line escape",
            "no follow-through",
            "range stall",
            "over-accumulation before confirmation",
            "time-stop ignored",
            "MA cluster breakdown",
            "exit correct but late",
        ],
    },
    {
        "symbol": "9697",
        "name": "Capcom 9697",
        "role": "failure_control",
        "applicable_patch_ids": [HARD_INVALIDATION_CANDIDATE],
        "start_date": "2025-09-19",
        "end_date": "2025-11-21",
        "trade_start_date": "2025-09-19",
        "expected_themes": [
            "breakout-like attempt",
            "no follow-through",
            "sideways / koma cluster",
            "continued long bias after failed attempt",
            "late drawdown",
        ],
    },
    {
        "symbol": "2531",
        "name": "宝HD2531",
        "role": "capture_control",
        "applicable_patch_ids": [BAD_REENTRY_AFTER_PROFIT_TAKE_CANDIDATE],
        "start_date": "2026-01-01",
        "end_date": "2026-03-31",
        "trade_start_date": "2026-01-01",
        "expected_themes": [
            "profit-taking / re-entry timing",
            "bad re-entry after profit-taking",
            "blocked re-entry after weak rebound",
            "not a hard invalidation long-failure case",
        ],
    },
    {
        "symbol": "5541",
        "name": "Taiheiyo Kinzoku 5541",
        "role": "success_control",
        "applicable_patch_ids": [],
        "start_date": "2025-10-10",
        "end_date": "2026-01-30",
        "trade_start_date": "2025-10-10",
        "expected_themes": [
            "line escape",
            "follow-through",
            "MA expansion",
            "valid pullback",
            "old resistance becoming support",
            "reacceleration",
        ],
    },
)

PRE_CONFIRMATION_ADD_CAP_UNITS = 3
TIME_STOP_WARNING_TRADING_DAYS = 20
HARD_TIME_STOP_TRADING_DAYS = 40
MAX_BASELINE_LONG_UNITS = 20
FOLLOWTHROUGH_WINDOW_MIN_DAYS = 3
FOLLOWTHROUGH_WINDOW_MAX_DAYS = 5
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\scratch\research_sessions\tradex_iizuka_trade_learning_loop")
DEFAULT_REPO_MIRROR_ROOT = REPO_ROOT / "artifacts" / "research_inventory" / "tradex_iizuka_trade_learning_loop"
NO_LOOKAHEAD_ASSERTION = (
    "decision uses only the current row and prior state; "
    "critic labels are assigned after replay outcome is known"
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _case_manifest_entry(case_spec: dict[str, Any]) -> dict[str, Any]:
    entry = {
        "symbol": str(case_spec["symbol"]),
        "name": str(case_spec["name"]),
        "role": str(case_spec["role"]),
        "window": {
            "start_date": str(case_spec["start_date"]),
            "end_date": str(case_spec["end_date"]),
            "trade_start_date": str(case_spec["trade_start_date"]),
        },
        "expected_themes": list(case_spec["expected_themes"]),
    }
    if "applicable_patch_ids" in case_spec:
        entry["applicable_patch_ids"] = list(case_spec["applicable_patch_ids"])
    return entry


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
    if hasattr(value, "isoformat") and not isinstance(value, (str, bytes)):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return path


def _position_text(sell_units: int, buy_units: int) -> str:
    return f"{int(sell_units)}-{int(buy_units)}"


def _position_market_value(buy_units: int, sell_units: int, price: float) -> float:
    return float((int(buy_units) - int(sell_units)) * float(price) * SHARES_PER_UNIT)


def _trace_row_hash(row: dict[str, Any]) -> str:
    payload = {key: value for key, value in row.items() if key != "trace_row_hash"}
    return hashlib.sha256(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _confirmation_signal(row: pd.Series) -> bool:
    return bool(row.get("breakout10")) or (
        bool(row.get("breakout5"))
        and bool(row.get("bull_stack"))
        and _safe_float(row.get("dist_ma20_pct")) >= 0.05
    )


def _hard_invalidation_signal(row: pd.Series, *, days_open: int, confirmed: bool) -> bool:
    if confirmed:
        return bool(row.get("lose_ma20")) and (bool(row.get("lose_ma60")) or bool(row.get("exhaustion")) or bool(row.get("failed_breakout5")))
    return bool(row.get("lose_ma20")) and (bool(row.get("lose_ma60")) or bool(row.get("exhaustion")) or bool(row.get("failed_breakout5")))


def _hard_invalidation_warning(row: pd.Series) -> bool:
    return bool(row.get("lose_ma20")) and (bool(row.get("lose_ma60")) or bool(row.get("exhaustion")) or bool(row.get("failed_breakout5")))


def _hard_invalidation_confirmed(
    row: pd.Series,
    *,
    tracker: HardInvalidationState,
    days_since_warning: int,
) -> bool:
    if not tracker.active or tracker.trigger_index is None:
        return False
    if days_since_warning < 3:
        return False
    if bool(row.get("reclaim_ma20")) or bool(row.get("support_wick")):
        return False
    return bool(row.get("lose_ma20")) or bool(row.get("lose_ma60")) or bool(row.get("bear_stack")) or bool(row.get("failed_breakout5"))


def _hard_invalidation_severity_plan(
    row: pd.Series,
    *,
    state: ChartState,
    days_since_warning: int,
    confirmed: bool,
) -> dict[str, Any]:
    close = _safe_float(row.get("c"))
    unrealized_pnl = (close - float(state.avg_buy_price)) * int(state.buy_units) * SHARES_PER_UNIT if state.buy_units > 0 else 0.0
    path_pnl = float(state.realized_pnl) + float(unrealized_pnl)
    profitable = path_pnl > 0
    loss_side = path_pnl <= 0
    near_flat = abs(path_pnl) <= max(1.0, abs(float(state.realized_pnl)) * 0.1)
    ma7_below_ma20 = bool(row.get("ma7_below_ma20"))
    lose_ma20 = bool(row.get("lose_ma20"))
    lose_ma60 = bool(row.get("lose_ma60"))
    bear_stack = bool(row.get("bear_stack"))
    failed_breakout = bool(row.get("failed_breakout5"))
    support_wick = bool(row.get("support_wick"))
    reclaim_ma20 = bool(row.get("reclaim_ma20"))
    gap_down = _safe_float(row.get("gap_pct")) < -0.02
    failed_rebound = (bear_stack or failed_breakout or gap_down) and not reclaim_ma20
    structural_breakdown = bool(lose_ma20 and lose_ma60 and ma7_below_ma20 and failed_rebound and not support_wick)
    warning_breakdown = bool(days_since_warning >= 3 and confirmed and (lose_ma20 or lose_ma60 or bear_stack or failed_breakout))
    loss_side_override_applied = bool(
        confirmed
        and int(state.buy_units) >= CONFIRMED_LONG_UNITS
        and (loss_side or near_flat)
        and (lose_ma20 or lose_ma60)
        and (bear_stack or failed_breakout or gap_down or not reclaim_ma20)
        and not support_wick
    )
    high_severity_condition_met = bool(structural_breakdown or warning_breakdown or loss_side_override_applied)
    profit_protection_guard_applied = bool(profitable and high_severity_condition_met and not loss_side_override_applied)

    if loss_side_override_applied:
        return {
            "action_type": "exit_all",
            "target_buy_units": 0,
            "severity_level": "exit_all",
            "reduction_intensity": None,
            "special_reason": "hard_invalidation_exit_v2_loss_side_override",
            "high_severity_condition_met": True,
            "loss_side_override_applied": True,
            "profit_protection_guard_applied": False,
        }

    if profitable or loss_side:
        if structural_breakdown:
            if int(state.buy_units) >= FULL_LONG_UNITS + 2:
                target_buy_units = max(CONFIRMED_LONG_UNITS, int(state.buy_units) - 3)
                reduction_intensity = "medium"
            elif int(state.buy_units) >= CONFIRMED_LONG_UNITS + 2:
                target_buy_units = max(CONFIRMED_LONG_UNITS, int(state.buy_units) - 2)
                reduction_intensity = "light"
            else:
                target_buy_units = max(ENTRY_LONG_UNITS, int(state.buy_units) - 1)
                reduction_intensity = "light"
            severity_level = "partial_exit" if int(target_buy_units) <= CONFIRMED_LONG_UNITS + 1 else "long_reduce"
        elif warning_breakdown or not reclaim_ma20 or (lose_ma20 and lose_ma60):
            if int(state.buy_units) >= FULL_LONG_UNITS:
                target_buy_units = max(CONFIRMED_LONG_UNITS + 1, int(state.buy_units) - 2)
                reduction_intensity = "medium"
            else:
                target_buy_units = max(ENTRY_LONG_UNITS, int(state.buy_units) - 1)
                reduction_intensity = "light"
            severity_level = "long_reduce"
        else:
            target_buy_units = max(ENTRY_LONG_UNITS, int(state.buy_units) - 1)
            reduction_intensity = "light"
            severity_level = "long_reduce"
        return {
            "action_type": "long_reduce",
            "target_buy_units": int(target_buy_units),
            "severity_level": severity_level,
            "reduction_intensity": reduction_intensity,
            "special_reason": "partial_exit_due_to_hard_invalidation"
            if int(target_buy_units) <= CONFIRMED_LONG_UNITS
            else "hard_invalidation_long_reduce",
            "high_severity_condition_met": high_severity_condition_met,
            "loss_side_override_applied": False,
            "profit_protection_guard_applied": profit_protection_guard_applied,
        }
    return {
        "action_type": "long_reduce",
        "target_buy_units": max(ENTRY_LONG_UNITS, int(state.buy_units) - 1),
        "severity_level": "long_reduce",
        "reduction_intensity": "light",
        "special_reason": "hard_invalidation_long_reduce",
        "high_severity_condition_met": high_severity_condition_met,
        "loss_side_override_applied": False,
        "profit_protection_guard_applied": profit_protection_guard_applied,
    }


def _hard_invalidation_profit_preservation_guard_plan(
    row: pd.Series,
    *,
    state: ChartState,
    days_since_warning: int,
    confirmed: bool,
) -> dict[str, Any]:
    severity_plan = _hard_invalidation_severity_plan(
        row,
        state=state,
        days_since_warning=days_since_warning,
        confirmed=confirmed,
    )
    close = _safe_float(row.get("c"))
    unrealized_pnl = (close - float(state.avg_buy_price)) * int(state.buy_units) * SHARES_PER_UNIT if state.buy_units > 0 else 0.0
    path_pnl = float(state.realized_pnl) + float(unrealized_pnl)
    profitable = path_pnl > 0
    near_flat = abs(path_pnl) <= max(1.0, abs(float(state.realized_pnl)) * 0.1)
    daily_ctx = str(row.get("daily_main_state_ctx") or "")
    monthly_ctx = str(row.get("monthly_main_state_ctx") or "")
    constructive_regime = bool(
        bool(row.get("reclaim_ma20"))
        or bool(row.get("support_wick"))
        or bool(row.get("bull_stack"))
        or daily_ctx in {"daily_reversal_up_candidate", "daily_up_mid"}
        or monthly_ctx in {"monthly_up_top_warning", "monthly_range_mid"}
    )
    weak_confirmation = bool(
        days_since_warning <= 4
        and not bool(row.get("lose_ma60"))
        and not bool(row.get("bear_stack"))
        and not bool(row.get("failed_breakout5"))
    )
    should_soften = bool(severity_plan["action_type"] == "exit_all" and (profitable or near_flat) and constructive_regime and weak_confirmation)
    if not should_soften:
        plan = dict(severity_plan)
        plan["profit_preservation_guard_applied"] = False
        plan["profit_preservation_guard_reason"] = None
        return plan

    target_buy_units = max(ENTRY_LONG_UNITS, int(state.buy_units) - 1)
    if profitable and int(state.buy_units) > CONFIRMED_LONG_UNITS:
        target_buy_units = max(CONFIRMED_LONG_UNITS, target_buy_units)

    return {
        "action_type": "long_reduce",
        "target_buy_units": int(target_buy_units),
        "severity_level": "long_reduce",
        "reduction_intensity": "light",
        "special_reason": "hard_invalidation_profit_preservation_guard_long_reduce",
        "high_severity_condition_met": bool(severity_plan["high_severity_condition_met"]),
        "loss_side_override_applied": False,
        "profit_protection_guard_applied": bool(severity_plan["profit_protection_guard_applied"]),
        "profit_preservation_guard_applied": True,
        "profit_preservation_guard_reason": "profitable_constructive_continuation" if profitable else "near_flat_constructive_continuation",
    }


def _hard_invalidation_non_exit_late_extension_hedge_plan(
    row: pd.Series,
    *,
    state: ChartState,
    days_since_warning: int,
    confirmed: bool,
) -> dict[str, Any]:
    severity_plan = _hard_invalidation_severity_plan(
        row,
        state=state,
        days_since_warning=days_since_warning,
        confirmed=confirmed,
    )
    if str(severity_plan["action_type"]) != "long_reduce":
        plan = dict(severity_plan)
        plan["late_extension_hedge_condition_activated"] = False
        plan["late_extension_hedge_condition_reason"] = None
        return plan

    close = _safe_float(row.get("c"))
    ma20 = _safe_float(row.get("ma20"))
    ma60 = _safe_float(row.get("ma60"))
    extended_strength = bool(row.get("bull_stack")) and close >= ma20 and close >= ma60 and _safe_float(row.get("dist_ma20_pct")) >= 0.05
    no_existing_hedge = int(state.sell_units) == 0
    if not bool(extended_strength and no_existing_hedge):
        plan = dict(severity_plan)
        plan["late_extension_hedge_condition_activated"] = False
        plan["late_extension_hedge_condition_reason"] = None
        return plan

    return {
        "action_type": "hedge_add",
        "target_buy_units": int(state.buy_units),
        "severity_level": "hedge_add",
        "reduction_intensity": None,
        "special_reason": "late_extension_blocked",
        "high_severity_condition_met": bool(severity_plan["high_severity_condition_met"]),
        "loss_side_override_applied": False,
        "profit_protection_guard_applied": False,
        "late_extension_hedge_condition_activated": True,
        "late_extension_hedge_condition_reason": "bull_stack_and_extension_without_existing_hedge",
    }


@dataclass
class FollowThroughState:
    active: bool = False
    attempt_index: int | None = None
    attempt_dt: int | None = None
    attempt_close: float = 0.0
    attempt_high: float = 0.0
    attempt_midpoint: float = 0.0
    attempt_key_line: float = 0.0
    confirmed: bool = False
    failed: bool = False
    stop_active: bool = False
    first_failed_dt: int | None = None
    first_warning_dt: int | None = None
    first_stop_dt: int | None = None
    time_decay_score: int = 0


@dataclass
class HardInvalidationState:
    active: bool = False
    trigger_index: int | None = None
    trigger_dt: int | None = None
    warning_close: float = 0.0
    first_action_dt: int | None = None
    exit_forced: bool = False
    severity_action_type: str | None = None
    severity_target_buy_units: int | None = None
    severity_level: str | None = None
    reduction_intensity: str | None = None
    high_severity_condition_met: bool = False
    loss_side_override_applied: bool = False
    profit_protection_guard_applied: bool = False
    profit_preservation_guard_applied: bool = False
    profit_preservation_guard_reason: str | None = None
    late_extension_hedge_condition_activated: bool = False
    late_extension_hedge_condition_reason: str | None = None
    first_action_type: str | None = None
    first_action_target_buy_units: int | None = None
    first_action_severity_level: str | None = None
    first_action_severity_action_type: str | None = None
    first_action_reduction_intensity: str | None = None
    time_decay_score: int = 0


@dataclass
class ProfitTakeReentryState:
    active: bool = False
    profit_taken: bool = False
    reentry_active: bool = False
    reentry_confirmed: bool = False
    first_profit_take_candidate_dt: int | None = None
    first_profit_take_dt: int | None = None
    first_reentry_candidate_dt: int | None = None
    first_missed_reentry_dt: int | None = None
    first_reentry_confirmation_dt: int | None = None
    first_reentry_action_dt: int | None = None
    first_profit_take_action_dt: int | None = None
    opportunity_loss_score: float = 0.0
    risk_materially_increased: bool = False


@dataclass
class BadReentryState:
    active: bool = False
    profit_taken: bool = False
    reentry_block_active: bool = False
    first_profit_take_candidate_dt: int | None = None
    first_profit_take_dt: int | None = None
    first_bad_reentry_candidate_dt: int | None = None
    first_reentry_block_dt: int | None = None
    false_reentry_avoided: bool = False
    opportunity_loss_score: float = 0.0
    risk_reduced: bool = False


def _followthrough_attempt_candidate(row: pd.Series) -> bool:
    return bool(
        bool(row.get("breakout10"))
        or (bool(row.get("breakout5")) and bool(row.get("bull_stack")))
        or (bool(row.get("reclaim_ma20")) and bool(row.get("support_wick")))
    )


def _basis_payload(row: pd.Series) -> dict[str, Any]:
    basis = row.get("basis_payload")
    return basis if isinstance(basis, dict) else {}


def _profit_take_candidate(row: pd.Series, *, state: ChartState) -> bool:
    if state.buy_units <= 0 or state.avg_buy_price <= 0:
        return False
    basis = _basis_payload(row)
    upper_wick = _safe_float(basis.get("candleUpperWickRatio"))
    daily_ctx = str(row.get("daily_main_state_ctx") or "")
    monthly_ctx = str(row.get("monthly_main_state_ctx") or "")
    above_ma20_streak = int(_safe_float(row.get("above_ma20_streak")))
    return bool(
        _safe_float(row.get("dist_ma20_pct")) >= 0.02
        and upper_wick >= 0.20
        and bool(row.get("support_wick"))
        and bool(row.get("exhaustion"))
        and above_ma20_streak >= 20
        and daily_ctx == "daily_reversal_up_candidate"
        and monthly_ctx in {"monthly_up_top_warning", "monthly_range_mid"}
    )


def _reentry_candidate(row: pd.Series, *, state: ChartState, tracker: ProfitTakeReentryState) -> bool:
    if tracker is None or not tracker.profit_taken or state.buy_units > CONFIRMED_LONG_UNITS:
        return False
    basis = _basis_payload(row)
    close = _safe_float(row.get("c"))
    ma20 = _safe_float(row.get("ma20"))
    ma60 = _safe_float(row.get("ma60"))
    daily_ctx = str(row.get("daily_main_state_ctx") or "")
    trend_reclaim = bool(basis.get("reclaim60")) or bool(row.get("reclaim_ma20")) or bool(row.get("support_wick"))
    reversal_ctx = daily_ctx in {"daily_reversal_up_candidate", "daily_up_mid"}
    stable_below = close >= ma60 * 0.98 and close >= ma20 * 0.96
    return bool(trend_reclaim and reversal_ctx and stable_below and not bool(row.get("failed_breakout5")) and not bool(row.get("lose_ma60")))


def _reentry_confirmation(row: pd.Series, *, tracker: ProfitTakeReentryState) -> bool:
    if tracker is None or not tracker.reentry_active:
        return False
    basis = _basis_payload(row)
    close = _safe_float(row.get("c"))
    ma20 = _safe_float(row.get("ma20"))
    ma60 = _safe_float(row.get("ma60"))
    return bool(
        bool(basis.get("reclaim60"))
        or bool(basis.get("v60Core"))
        or bool(row.get("breakout5"))
        or bool(row.get("breakout10"))
        or (close >= ma20 and close >= ma60 and bool(row.get("support_wick")))
    )


def _bad_reentry_candidate(row: pd.Series, *, tracker: BadReentryState) -> bool:
    if tracker is None or not tracker.profit_taken:
        return False
    basis = _basis_payload(row)
    daily_ctx = str(row.get("daily_main_state_ctx") or "")
    monthly_ctx = str(row.get("monthly_main_state_ctx") or "")
    weak_reclaim = not bool(row.get("reclaim_ma20")) and not bool(basis.get("reclaim60"))
    weak_breakout = not bool(row.get("breakout5")) and not bool(row.get("breakout10"))
    ambiguous_support = bool(row.get("support_wick")) or bool(row.get("exhaustion"))
    extended_after_exit = _safe_float(row.get("dist_ma20_pct")) < 0.03 or _safe_float(row.get("dist_ma20_pct")) <= 0.0
    return bool(
        weak_reclaim
        and weak_breakout
        and ambiguous_support
        and daily_ctx in {"daily_reversal_up_candidate", "daily_up_mid"}
        and monthly_ctx in {"monthly_up_top_warning", "monthly_range_mid"}
        and extended_after_exit
    )


def _strong_reentry_signal(row: pd.Series) -> bool:
    basis = _basis_payload(row)
    return bool(
        bool(row.get("breakout5"))
        or bool(row.get("breakout10"))
        or bool(row.get("reclaim_ma20"))
        or bool(basis.get("reclaim60"))
    )


def _followthrough_key_line(row: pd.Series) -> float:
    return float(max(_safe_float(row.get("ma20")), _safe_float(row.get("prev_5_high"))))


def _followthrough_confirmed(row: pd.Series, *, tracker: FollowThroughState) -> bool:
    if not tracker.active or tracker.attempt_index is None:
        return False
    higher_high = _safe_float(row.get("h")) > tracker.attempt_high
    higher_close = _safe_float(row.get("c")) > tracker.attempt_close
    above_key_line = _safe_float(row.get("c")) >= tracker.attempt_key_line
    ma7_rising = _safe_float(row.get("ma7_slope")) >= 0 or _safe_float(row.get("ma7")) >= tracker.attempt_key_line
    midpoint_held = _safe_float(row.get("c")) >= tracker.attempt_midpoint
    return bool((higher_high or higher_close) and above_key_line and ma7_rising and midpoint_held)


def _followthrough_failed(row: pd.Series, *, tracker: FollowThroughState, days_since_attempt: int) -> bool:
    if not tracker.active or tracker.confirmed:
        return False
    if days_since_attempt < FOLLOWTHROUGH_WINDOW_MIN_DAYS:
        return False
    higher_high = _safe_float(row.get("h")) > tracker.attempt_high
    higher_close = _safe_float(row.get("c")) > tracker.attempt_close
    no_progress = not higher_high and not higher_close
    returned_to_range = _safe_float(row.get("c")) <= tracker.attempt_key_line or _safe_float(row.get("c")) <= tracker.attempt_midpoint
    lost_ma7 = _safe_float(row.get("c")) < _safe_float(row.get("ma7"))
    bearish_shift = bool(row.get("failed_breakout5")) or bool(row.get("breakdown5")) or bool(row.get("bear_stack")) or (_safe_float(row.get("gap_pct")) < -0.02)
    if no_progress and (returned_to_range or lost_ma7 or bearish_shift):
        return True
    if days_since_attempt >= FOLLOWTHROUGH_WINDOW_MAX_DAYS and no_progress and (returned_to_range or lost_ma7):
        return True
    return False


def _aggressive_long_target(state: ChartState, row: pd.Series, *, confirmed: bool) -> int:
    lose60 = bool(row.get("lose_ma60"))
    lose20 = bool(row.get("lose_ma20"))
    bear_stack = bool(row.get("bear_stack"))
    exhaustion = bool(row.get("exhaustion"))
    bullish_core = (
        bool(row.get("reclaim_ma20"))
        or bool(row.get("support_wick"))
        or bool(row.get("bull_stack"))
        or bool(row.get("breakout5"))
        or bool(row.get("breakout10"))
        or _safe_float(row.get("c")) >= _safe_float(row.get("ma20"))
    )
    if lose60 or (lose20 and bear_stack):
        return 0
    if state.buy_units == 0:
        if bullish_core:
            return 1 if not confirmed else min(2, MAX_BASELINE_LONG_UNITS)
        return 0
    if not confirmed:
        if bullish_core:
            if bool(row.get("breakout10")) or bool(row.get("bull_stack")):
                step = 3
            elif bool(row.get("breakout5")) or bool(row.get("reclaim_ma20")) or bool(row.get("support_wick")):
                step = 2
            else:
                step = 1
            return min(MAX_BASELINE_LONG_UNITS, state.buy_units + step)
        if exhaustion:
            return max(1, state.buy_units - 1)
        return state.buy_units
    if bullish_core:
        if bool(row.get("breakout10")):
            step = 4
        elif bool(row.get("breakout5")):
            step = 2
        else:
            step = 1
        return min(MAX_BASELINE_LONG_UNITS, state.buy_units + step)
    if exhaustion:
        return max(1, state.buy_units - 1)
    return state.buy_units


def _confidence_for_action(action_type: str, *, confirmed: bool, blocked_by_cap: bool = False) -> float:
    if action_type == "watch":
        return 0.20
    if action_type == "trial_buy":
        return 0.32 if not confirmed else 0.55
    if action_type == "long_add":
        return 0.42 if not confirmed else 0.70
    if action_type == "hedge_add":
        return 0.60 if confirmed else 0.48
    if action_type == "long_reduce":
        return 0.66 if confirmed else 0.52
    if action_type == "hedge_reduce":
        return 0.61 if confirmed else 0.50
    if action_type == "stop_add":
        return 0.78 if blocked_by_cap else 0.55
    if action_type == "exit_all":
        return 0.82 if confirmed else 0.63
    return 0.40


def _active_thesis(row: pd.Series, *, confirmed: bool) -> str:
    if confirmed:
        return "line escape / follow-through / support retest continuation"
    if bool(row.get("reclaim_ma20")) or bool(row.get("support_wick")):
        return "probe for line escape before confirmation"
    if bool(row.get("exhaustion")):
        return "watch for failure after extension"
    return "range stall / no conviction yet"


def _evidence_for(row: pd.Series) -> list[str]:
    items = [
        f"close={_safe_float(row.get('c')):.2f}",
        f"ma20={_safe_float(row.get('ma20')):.2f}",
        f"ma60={_safe_float(row.get('ma60')):.2f}",
        f"dist_ma20_pct={_safe_float(row.get('dist_ma20_pct')):.4f}",
        f"breakout5={bool(row.get('breakout5'))}",
        f"breakout10={bool(row.get('breakout10'))}",
        f"reclaim_ma20={bool(row.get('reclaim_ma20'))}",
        f"support_wick={bool(row.get('support_wick'))}",
        f"bull_stack={bool(row.get('bull_stack'))}",
    ]
    return items


def _evidence_against(row: pd.Series) -> list[str]:
    items = [
        f"lose_ma20={bool(row.get('lose_ma20'))}",
        f"lose_ma60={bool(row.get('lose_ma60'))}",
        f"breakdown5={bool(row.get('breakdown5'))}",
        f"failed_breakout5={bool(row.get('failed_breakout5'))}",
        f"exhaustion={bool(row.get('exhaustion'))}",
        f"bear_stack={bool(row.get('bear_stack'))}",
    ]
    return items


def _reason_for(
    action_type: str,
    *,
    confirmed: bool,
    blocked_by_cap: bool,
    row: pd.Series,
    base_reason_map: dict[str, Any],
    forced_exit_reason: str | None = None,
    special_reason: str | None = None,
) -> str:
    if special_reason is not None:
        return special_reason
    if action_type == "watch":
        return str((base_reason_map.get("flat") or {}).get("primary") or "no_trade_penalty_cleared")
    if action_type == "trial_buy":
        return str((base_reason_map.get("entry") or {}).get("primary") or "ma20_reclaim_body_close")
    if action_type == "long_add":
        return str((base_reason_map.get("add") or {}).get("primary") or "continuation_confirmed")
    if action_type == "hedge_add":
        return str((base_reason_map.get("hedge") or {}).get("primary") or "late_extension_blocked")
    if action_type == "long_reduce":
        return str((base_reason_map.get("trim") or base_reason_map.get("exit") or {}).get("primary") or "lose_ma20")
    if action_type == "hedge_reduce":
        return str((base_reason_map.get("cover") or base_reason_map.get("flat") or {}).get("primary") or "reentry_after_cooloff")
    if action_type == "stop_add":
        if blocked_by_cap:
            return "failed_followthrough_time_stop_v1"
        return str((base_reason_map.get("add") or {}).get("primary") or "followthrough_stop_not_required")
    if action_type == "exit_all":
        if forced_exit_reason is not None:
            return forced_exit_reason
        if bool(row.get("lose_ma60")):
            return "lose_ma60"
        if bool(row.get("lose_ma20")) and bool(row.get("bear_stack")):
            return "lose_ma20"
        if not confirmed:
            return "time_stop"
        return str((base_reason_map.get("exit") or {}).get("primary") or "time_stop")
    return "no_trade_penalty_cleared"


def _plan_day_actions(
    *,
    state: ChartState,
    row: pd.Series,
    base_buy_target: int,
    base_sell_target: int,
    base_reason_map: dict[str, Any],
    mode: str,
    confirmed: bool,
) -> tuple[list[dict[str, Any]], bool]:
    current_long = int(state.buy_units)
    current_hedge = int(state.sell_units)
    blocked_by_cap = False

    if current_long > 0 and base_buy_target == 0:
        return (
            [
                {
                    "action_type": "exit_all",
                    "long_delta": -current_long,
                    "hedge_delta": -current_hedge,
                    "blocked_by_cap": False,
                }
            ],
            False,
        )

    if current_long == 0 and current_hedge == 0 and base_buy_target <= 0:
        return ([{"action_type": "watch", "long_delta": 0, "hedge_delta": 0, "blocked_by_cap": False}], False)

    effective_buy_target = int(base_buy_target)

    actions: list[dict[str, Any]] = []
    working_long = current_long

    if effective_buy_target < current_long:
        reduce_units = current_long - effective_buy_target
        while reduce_units > 0:
            step = min(ENTRY_LONG_UNITS, reduce_units)
            actions.append(
                {
                    "action_type": "long_reduce",
                    "long_delta": -step,
                    "hedge_delta": 0,
                    "blocked_by_cap": False,
                }
            )
            working_long -= step
            reduce_units -= step
    elif working_long == 0 and effective_buy_target > 0:
        actions.append({"action_type": "trial_buy", "long_delta": 1, "hedge_delta": 0, "blocked_by_cap": False})
        working_long = 1
        remaining = effective_buy_target - 1
        while remaining > 0:
            step = min(CONFIRMED_LONG_UNITS - ENTRY_LONG_UNITS, remaining)
            actions.append(
                {
                    "action_type": "long_add",
                    "long_delta": step,
                    "hedge_delta": 0,
                    "blocked_by_cap": False,
                }
            )
            working_long += step
            remaining -= step
    elif working_long > 0 and effective_buy_target > working_long:
        remaining = effective_buy_target - working_long
        while remaining > 0:
            step = min(CONFIRMED_LONG_UNITS - ENTRY_LONG_UNITS, remaining)
            actions.append(
                {
                    "action_type": "long_add",
                    "long_delta": step,
                    "hedge_delta": 0,
                    "blocked_by_cap": False,
                }
            )
            working_long += step
            remaining -= step

    if working_long <= 0:
        if actions:
            return actions, blocked_by_cap
        return ([{"action_type": "watch", "long_delta": 0, "hedge_delta": 0, "blocked_by_cap": False}], blocked_by_cap)

    hedge_target = min(max(0, int(base_sell_target)), working_long)
    if hedge_target < current_hedge:
        reduce_units = current_hedge - hedge_target
        while reduce_units > 0:
            step = min(LIGHT_HEDGE_UNITS if working_long < CONFIRMED_LONG_UNITS else HEAVY_HEDGE_UNITS, reduce_units)
            actions.append(
                {
                    "action_type": "hedge_reduce",
                    "long_delta": 0,
                    "hedge_delta": -step,
                    "blocked_by_cap": False,
                }
            )
            current_hedge -= step
            reduce_units -= step
    elif hedge_target > current_hedge:
        add_units = hedge_target - current_hedge
        while add_units > 0:
            step = min(LIGHT_HEDGE_UNITS if working_long < CONFIRMED_LONG_UNITS else HEAVY_HEDGE_UNITS, add_units)
            actions.append(
                {
                    "action_type": "hedge_add",
                    "long_delta": 0,
                    "hedge_delta": step,
                    "blocked_by_cap": False,
                }
            )
            current_hedge += step
            add_units -= step

    if not actions:
        actions.append({"action_type": "watch", "long_delta": 0, "hedge_delta": 0, "blocked_by_cap": False})
    return actions, blocked_by_cap


def _apply_action(state: ChartState, *, action_type: str, long_delta: int, hedge_delta: int, execution_price: float) -> None:
    if action_type == "trial_buy" and long_delta > 0:
        _fill_long_entry(state, units=long_delta, price=execution_price)
        return
    if action_type == "long_add" and long_delta > 0:
        _fill_long_entry(state, units=long_delta, price=execution_price)
        return
    if action_type == "long_reduce" and long_delta < 0:
        _fill_long_exit(state, units=abs(long_delta), price=execution_price)
        return
    if action_type == "hedge_add" and hedge_delta > 0:
        _fill_short_entry(state, units=hedge_delta, price=execution_price)
        return
    if action_type == "hedge_reduce" and hedge_delta < 0:
        _fill_short_cover(state, units=abs(hedge_delta), price=execution_price)
        return
    if action_type == "exit_all":
        _apply_state_transition(state=state, target_buy_units=0, target_sell_units=0, execution_price=execution_price)


def _run_case(
    *,
    case_spec: dict[str, Any],
    source_db_path: Path,
    mode: str,
    candidate_name: str = FAILED_FOLLOWTHROUGH_CANDIDATE,
    session_id: str | None = None,
) -> dict[str, Any]:
    symbol = str(case_spec["symbol"])
    start_date = str(case_spec["start_date"])
    end_date = str(case_spec["end_date"])
    trade_start_date = str(case_spec["trade_start_date"])
    bars_frame, basis_frame = _load_source_frames(
        source_db_path=source_db_path,
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
    )
    frame = _prepare_chart_frame(bars_frame, basis_frame)
    frame = frame.loc[(frame["dt"] >= _date_text_to_ymd(start_date)) & (frame["dt"] <= _date_text_to_ymd(end_date))].copy()
    frame = frame.reset_index(drop=True)
    frame["ma7"] = frame["c"].rolling(7, min_periods=1).mean()
    frame["ma7_slope"] = frame["ma7"].diff()
    if frame.empty:
        raise RuntimeError(f"no chart rows remained for symbol={symbol} window={start_date}..{end_date}")

    trading_days = [int(value) for value in frame["dt"].tolist()]
    bar_lookup = bars_frame.set_index("dt").to_dict(orient="index")
    source_end_dt = max(int(value) for value in bar_lookup.keys())
    state = ChartState()
    followthrough = FollowThroughState()
    hard_invalidation = HardInvalidationState()
    profit_take_reentry = ProfitTakeReentryState()
    bad_reentry = BadReentryState()
    action_trace: list[dict[str, Any]] = []
    current_cycle: dict[str, Any] | None = None
    first_entry_date: str | None = None
    first_confirmation_date: str | None = None
    first_time_stop_warning_date: str | None = None
    first_hard_invalidation_date: str | None = None
    first_hard_invalidation_exit_date: str | None = None
    first_hard_invalidation_action_date: str | None = None
    first_add_stop_date: str | None = None
    first_failed_followthrough_date: str | None = None
    first_profit_take_candidate_date: str | None = None
    first_profit_take_date: str | None = None
    first_bad_reentry_candidate_date: str | None = None
    first_reentry_block_date: str | None = None
    first_reentry_candidate_date: str | None = None
    first_missed_reentry_date: str | None = None
    first_reentry_confirmation_date: str | None = None
    bad_reentry_event_log: list[dict[str, Any]] = []
    max_long_pre_failed_followthrough_stop = 0
    max_gross_pre_failed_followthrough_stop = 0
    add_stop_count = 0
    action_counts = {action: 0 for action in ACTION_SPACE}
    trade_start_ymd = _date_text_to_ymd(trade_start_date)
    active_patch_ids = set(case_spec.get("applicable_patch_ids") or [])
    stack_mode = candidate_name in {STACK_CANDIDATE, HARD_INVALIDATION_SEVERITY_CANDIDATE}
    followthrough_rules_active = candidate_name in {FAILED_FOLLOWTHROUGH_CANDIDATE, STACK_CANDIDATE, HARD_INVALIDATION_SEVERITY_CANDIDATE}
    hard_invalidation_rules_active = candidate_name in HARD_INVALIDATION_ACTIVE_CANDIDATES or candidate_name == STACK_CANDIDATE
    profit_take_rules_active = candidate_name == PROFIT_TAKE_REENTRY_CANDIDATE and symbol == "2531"
    bad_reentry_rules_active = candidate_name in {BAD_REENTRY_AFTER_PROFIT_TAKE_CANDIDATE, STACK_CANDIDATE, HARD_INVALIDATION_SEVERITY_CANDIDATE} and case_spec["role"] != "success_control"

    def _build_action_trace_row(
        *,
        action_type: str,
        before_position: str,
        after_position: str,
        reason: str,
        special_reason: str | None,
        forced_exit_reason: str | None,
        evidence_for: list[str],
        evidence_against: list[str],
        risk_warning: str,
        confidence: float,
        active_thesis: str,
        before_buy_units: int,
        before_sell_units: int,
        after_buy_units: int,
        after_sell_units: int,
        realized_before: float,
        realized_after: float,
        execution_price: float,
        severity_level: str | None,
        severity_action_type: str | None,
        reduction_intensity: str | None,
        severity_target_buy_units: int | None,
        profit_protection_guard_applied: bool | None,
        profit_preservation_guard_applied: bool | None,
        profit_preservation_guard_reason: str | None,
        late_extension_hedge_condition_activated: bool | None,
        late_extension_hedge_condition_reason: str | None,
    ) -> dict[str, Any]:
        close_price = _safe_float(execution_bar.get("c"))
        unrealized_after = float(_mark_unrealized(state, close_price=close_price))
        source_as_of = row.get("source_as_of")
        if source_as_of is None or (isinstance(source_as_of, float) and math.isnan(source_as_of)) or pd.isna(source_as_of):
            as_of = _ymd_to_date_text(decision_dt)
        else:
            try:
                source_as_of_ymd = int(float(source_as_of))
            except Exception:
                source_as_of_ymd = decision_dt
            as_of = _ymd_to_date_text(source_as_of_ymd)
        trace_row = {
            "symbol": symbol,
            "date": _ymd_to_date_text(decision_dt),
            "as_of": as_of,
            "session_id": session_id,
            "candidate_id": candidate_name,
            "trace_schema_version": TRACE_SCHEMA_VERSION,
            "action_type": action_type,
            "action_reason": reason,
            "action_source": str(forced_exit_reason or special_reason or candidate_name),
            "position_before": before_position,
            "position_after": after_position,
            "target_position_after": after_position,
            "buy_units_before": int(before_buy_units),
            "sell_units_before": int(before_sell_units),
            "buy_units_after": int(after_buy_units),
            "sell_units_after": int(after_sell_units),
            "net_units_before": int(before_buy_units - before_sell_units),
            "net_units_after": int(after_buy_units - after_sell_units),
            "gross_units_before": int(before_buy_units + before_sell_units),
            "gross_units_after": int(after_buy_units + after_sell_units),
            "close": close_price,
            "next_open_if_used": float(execution_price),
            "mark_price": close_price,
            "position_market_value_before": _position_market_value(before_buy_units, before_sell_units, close_price),
            "position_market_value_after": _position_market_value(after_buy_units, after_sell_units, close_price),
            "realized_pnl_day": float(realized_after - realized_before),
            "unrealized_pnl_day": unrealized_after,
            "cumulative_realized_pnl": float(realized_after),
            "cumulative_unrealized_pnl": unrealized_after,
            "equity_curve_value": float(realized_after + unrealized_after),
            "pnl_path_available": True,
            "data_source": str(source_db_path),
            "input_bar_date": _ymd_to_date_text(decision_dt),
            "decision_uses_future_data": False,
            "reason": reason,
            "special_reason": special_reason,
            "forced_exit_reason": forced_exit_reason,
            "severity_level": severity_level,
            "severity_action_type": severity_action_type,
            "reduction_intensity": reduction_intensity,
            "severity_target_buy_units": severity_target_buy_units,
            "profit_protection_guard_applied": profit_protection_guard_applied,
            "profit_preservation_guard_applied": profit_preservation_guard_applied,
            "profit_preservation_guard_reason": profit_preservation_guard_reason,
            "late_extension_hedge_condition_activated": late_extension_hedge_condition_activated,
            "late_extension_hedge_condition_reason": late_extension_hedge_condition_reason,
            "evidence_for": evidence_for,
            "evidence_against": evidence_against,
            "risk_warning": risk_warning,
            "confidence": confidence,
            "active_thesis": active_thesis,
            "no_lookahead_assertion": NO_LOOKAHEAD_ASSERTION,
        }
        trace_row["trace_row_hash"] = _trace_row_hash(trace_row)
        return trace_row

    for index, decision_dt in enumerate(trading_days):
        if decision_dt < trade_start_ymd:
            continue
        row = frame.loc[frame["dt"] == decision_dt].iloc[0]
        next_trading_day = trading_days[index + 1] if index + 1 < len(trading_days) else None
        execution_dt = next_trading_day if next_trading_day is not None else decision_dt
        terminal_source_end = execution_dt == decision_dt and decision_dt == source_end_dt
        execution_bar = bar_lookup.get(execution_dt)
        if execution_bar is None:
            if terminal_source_end:
                execution_bar = row
            else:
                raise RuntimeError(f"missing execution bar for symbol={symbol} execution_dt={execution_dt}")

        row_confirmation = _confirmation_signal(row)

        base_buy_target, base_sell_target, base_reason_map = _desired_targets(
            state,
            row,
            end_date=end_date,
            policy_variant="baseline",
            policy_context={"rank_bucket": "anchor_loop", "rollout_variant": ""},
        )
        base_buy_target = max(0, int(base_buy_target))
        base_sell_target = max(0, int(base_sell_target))
        if state.buy_units == 0 and state.sell_units == 0 and base_sell_target > 0:
            base_sell_target = 0

        aggressive_long_target = _aggressive_long_target(state, row, confirmed=bool(row_confirmation or followthrough.confirmed))
        if aggressive_long_target > base_buy_target:
            base_buy_target = aggressive_long_target

        if row_confirmation and first_confirmation_date is None:
            first_confirmation_date = _ymd_to_date_text(decision_dt)
        if row_confirmation:
            followthrough.confirmed = True

        confirmed = bool(row_confirmation or followthrough.confirmed)
        profit_take_signal = False
        reentry_signal = False
        reentry_confirmation_signal = False
        bad_reentry_signal = False
        strong_reentry_signal = False
        if profit_take_rules_active:
            profit_take_signal = _profit_take_candidate(row, state=state)
            if profit_take_signal and profit_take_reentry.first_profit_take_candidate_dt is None:
                profit_take_reentry.first_profit_take_candidate_dt = decision_dt
                if first_profit_take_candidate_date is None:
                    first_profit_take_candidate_date = _ymd_to_date_text(decision_dt)
            reentry_signal = _reentry_candidate(row, state=state, tracker=profit_take_reentry)
            if reentry_signal and profit_take_reentry.first_reentry_candidate_dt is None:
                profit_take_reentry.first_reentry_candidate_dt = decision_dt
                if first_reentry_candidate_date is None:
                    first_reentry_candidate_date = _ymd_to_date_text(decision_dt)
            reentry_confirmation_signal = _reentry_confirmation(row, tracker=profit_take_reentry)
            if reentry_confirmation_signal and first_reentry_confirmation_date is None:
                first_reentry_confirmation_date = _ymd_to_date_text(decision_dt)
        if bad_reentry_rules_active:
            if _profit_take_candidate(row, state=state) and bad_reentry.first_profit_take_candidate_dt is None:
                bad_reentry.first_profit_take_candidate_dt = decision_dt
                if first_profit_take_candidate_date is None:
                    first_profit_take_candidate_date = _ymd_to_date_text(decision_dt)
            bad_reentry_signal = _bad_reentry_candidate(row, tracker=bad_reentry)
            if bad_reentry_signal and bad_reentry.first_bad_reentry_candidate_dt is None:
                bad_reentry.first_bad_reentry_candidate_dt = decision_dt
                if first_bad_reentry_candidate_date is None:
                    first_bad_reentry_candidate_date = _ymd_to_date_text(decision_dt)
            strong_reentry_signal = _strong_reentry_signal(row)
        if mode == "corrected" and profit_take_rules_active:
            if profit_take_signal and state.buy_units > CONFIRMED_LONG_UNITS:
                base_buy_target = min(base_buy_target, CONFIRMED_LONG_UNITS)
                profit_take_reentry.active = True
            if profit_take_reentry.profit_taken and state.buy_units <= CONFIRMED_LONG_UNITS and reentry_signal:
                base_buy_target = max(base_buy_target, 5)
                profit_take_reentry.reentry_active = True
            if profit_take_reentry.reentry_active and reentry_confirmation_signal:
                base_buy_target = max(base_buy_target, 5)
                profit_take_reentry.reentry_confirmed = True
        if mode == "corrected" and bad_reentry_rules_active:
            if profit_take_signal:
                bad_reentry.active = True
                bad_reentry.profit_taken = True
                if bad_reentry.first_profit_take_dt is None:
                    bad_reentry.first_profit_take_dt = decision_dt
                if first_profit_take_date is None:
                    first_profit_take_date = _ymd_to_date_text(decision_dt)
                if state.buy_units > CONFIRMED_LONG_UNITS:
                    base_buy_target = min(base_buy_target, CONFIRMED_LONG_UNITS)
            if bad_reentry.profit_taken and bad_reentry_signal and not strong_reentry_signal:
                bad_reentry.reentry_block_active = True
                if first_reentry_block_date is None:
                    first_reentry_block_date = _ymd_to_date_text(decision_dt)
                if bad_reentry.first_reentry_block_dt is None:
                    bad_reentry.first_reentry_block_dt = decision_dt
            if bad_reentry.reentry_block_active and strong_reentry_signal:
                bad_reentry.reentry_block_active = False

        if state.buy_units > 0:
            days_open = index - int(current_cycle["entry_index"]) if current_cycle is not None else 0
            if first_time_stop_warning_date is None and not confirmed and days_open >= TIME_STOP_WARNING_TRADING_DAYS and (followthrough_rules_active or hard_invalidation_rules_active):
                first_time_stop_warning_date = _ymd_to_date_text(decision_dt)
            if first_hard_invalidation_date is None and hard_invalidation_rules_active and _hard_invalidation_signal(row, days_open=days_open, confirmed=confirmed):
                first_hard_invalidation_date = _ymd_to_date_text(decision_dt)
            if (
                mode == "corrected"
                and candidate_name == HARD_INVALIDATION_SEVERITY_CANDIDATE
                and hard_invalidation.active
                and hard_invalidation.severity_target_buy_units is not None
                and not hard_invalidation.exit_forced
            ):
                base_buy_target = min(base_buy_target, int(hard_invalidation.severity_target_buy_units))

        planned_actions, blocked_by_cap = _plan_day_actions(
            state=state,
            row=row,
            base_buy_target=base_buy_target,
            base_sell_target=base_sell_target if base_buy_target > 0 or state.buy_units > 0 else 0,
            base_reason_map=base_reason_map,
            mode=mode,
            confirmed=confirmed,
        )

        if mode == "corrected" and bad_reentry_rules_active:
            bad_reentry_block_applied = False
            if bad_reentry.reentry_block_active and not strong_reentry_signal:
                transformed: list[dict[str, Any]] = []
                add_blocked = False
                for planned in planned_actions:
                    if planned["action_type"] in {"trial_buy", "long_add"}:
                        add_blocked = True
                        continue
                    transformed.append(planned)
                if add_blocked or bad_reentry_signal:
                    transformed.insert(
                        0,
                        {
                            "action_type": "stop_add",
                            "long_delta": 0,
                            "hedge_delta": 0,
                            "blocked_by_cap": True,
                            "special_reason": "reentry_blocked",
                        },
                    )
                    bad_reentry_block_applied = True
                    bad_reentry_event_log.append(
                        {
                            "date": _ymd_to_date_text(decision_dt),
                            "event_type": "bad_reentry_stop_add",
                            "symbol": symbol,
                            "reason": "stop_add inserted to block weak re-entry after profit take",
                            "action_type": "stop_add",
                        }
                    )
                planned_actions = transformed or [
                    {
                        "action_type": "stop_add",
                        "long_delta": 0,
                        "hedge_delta": 0,
                        "blocked_by_cap": True,
                        "special_reason": "reentry_blocked",
                    }
                ]
            if bad_reentry_block_applied:
                if first_reentry_block_date is None:
                    first_reentry_block_date = _ymd_to_date_text(decision_dt)
                if bad_reentry.first_reentry_block_dt is None:
                    bad_reentry.first_reentry_block_dt = decision_dt
                bad_reentry.false_reentry_avoided = True
                bad_reentry.risk_reduced = True
                bad_reentry_event_log.append(
                    {
                        "date": _ymd_to_date_text(decision_dt),
                        "event_type": "reentry_blocked",
                        "symbol": symbol,
                        "reason": "re-entry blocked after profit take because rebound lacked confirmation",
                        "action_type": "stop_add",
                    }
                )

        if current_cycle is None and (state.buy_units > 0 or state.sell_units > 0):
            current_cycle = {
                "symbol": symbol,
                "entry_index": index,
                "entry_decision_date": decision_dt,
                "entry_execution_date": execution_dt,
            }
        elif current_cycle is None and planned_actions and planned_actions[0]["action_type"] in {"trial_buy", "long_add"}:
            current_cycle = {
                "symbol": symbol,
                "entry_index": index,
                "entry_decision_date": decision_dt,
                "entry_execution_date": execution_dt,
            }
            if first_entry_date is None:
                first_entry_date = _ymd_to_date_text(decision_dt)

        if current_cycle is None and any(action["action_type"] in {"trial_buy", "long_add", "hedge_add"} for action in planned_actions):
            current_cycle = {
                "symbol": symbol,
                "entry_index": index,
                "entry_decision_date": decision_dt,
                "entry_execution_date": execution_dt,
            }
            if first_entry_date is None:
                first_entry_date = _ymd_to_date_text(decision_dt)

        if mode == "corrected" and followthrough_rules_active:
            would_open_or_add = any(action["action_type"] in {"trial_buy", "long_add"} for action in planned_actions)
            if not followthrough.active and not followthrough.stop_active and not followthrough.confirmed and (state.buy_units > 0 or would_open_or_add):
                if _followthrough_attempt_candidate(row):
                    followthrough.active = True
                    followthrough.attempt_index = index
                    followthrough.attempt_dt = decision_dt
                    followthrough.attempt_close = _safe_float(row.get("c"))
                    followthrough.attempt_high = _safe_float(row.get("h"))
                    followthrough.attempt_midpoint = (_safe_float(row.get("o")) + _safe_float(row.get("c"))) / 2.0
                    followthrough.attempt_key_line = _followthrough_key_line(row)
            if followthrough.active and followthrough.attempt_index is not None and not followthrough.confirmed and not followthrough.failed:
                days_since_attempt = index - int(followthrough.attempt_index)
                if first_time_stop_warning_date is None and days_since_attempt >= FOLLOWTHROUGH_WINDOW_MIN_DAYS:
                    first_time_stop_warning_date = _ymd_to_date_text(decision_dt)
                    followthrough.first_warning_dt = decision_dt
                if _followthrough_confirmed(row, tracker=followthrough):
                    followthrough.confirmed = True
                    followthrough.active = False
                    if first_confirmation_date is None:
                        first_confirmation_date = _ymd_to_date_text(decision_dt)
                elif _followthrough_failed(row, tracker=followthrough, days_since_attempt=days_since_attempt):
                    followthrough.failed = True
                    followthrough.stop_active = True
                    followthrough.active = False
                    followthrough.first_failed_dt = decision_dt
                    followthrough.first_stop_dt = decision_dt
                    followthrough.time_decay_score += max(1, days_since_attempt)
                    if first_failed_followthrough_date is None:
                        first_failed_followthrough_date = _ymd_to_date_text(decision_dt)
                    if first_add_stop_date is None:
                        first_add_stop_date = _ymd_to_date_text(decision_dt)
                    if first_time_stop_warning_date is None:
                        first_time_stop_warning_date = _ymd_to_date_text(decision_dt)

        if mode == "corrected" and followthrough_rules_active and followthrough.stop_active:
            blocked_add_today = base_buy_target > state.buy_units or any(action["action_type"] in {"trial_buy", "long_add"} for action in planned_actions)
            if blocked_add_today:
                transformed: list[dict[str, Any]] = []
                add_blocked = False
                for planned in planned_actions:
                    if planned["action_type"] in {"trial_buy", "long_add"}:
                        add_blocked = True
                        continue
                    transformed.append(planned)
                if add_blocked:
                    transformed.insert(
                        0,
                        {
                            "action_type": "stop_add",
                            "long_delta": 0,
                            "hedge_delta": 0,
                            "blocked_by_cap": True,
                        },
                    )
                    if first_add_stop_date is None:
                        first_add_stop_date = _ymd_to_date_text(decision_dt)
                planned_actions = transformed or [
                    {
                        "action_type": "stop_add",
                        "long_delta": 0,
                        "hedge_delta": 0,
                        "blocked_by_cap": True,
                    }
                ]

        if mode == "corrected" and hard_invalidation_rules_active and state.buy_units > 0:
            if not hard_invalidation.active and confirmed and _hard_invalidation_warning(row):
                hard_invalidation.active = True
                hard_invalidation.trigger_index = index
                hard_invalidation.trigger_dt = decision_dt
                hard_invalidation.warning_close = _safe_float(row.get("c"))
                if first_hard_invalidation_date is None:
                    first_hard_invalidation_date = _ymd_to_date_text(decision_dt)
            if hard_invalidation.active and hard_invalidation.trigger_index is not None:
                days_since_warning = index - int(hard_invalidation.trigger_index)
                if days_since_warning >= 1 and bool(row.get("reclaim_ma20")) and bool(row.get("support_wick")):
                    hard_invalidation.active = False
                    hard_invalidation.exit_forced = False
                    hard_invalidation.severity_target_buy_units = None
                    hard_invalidation.severity_level = None
                    hard_invalidation.reduction_intensity = None
                elif _hard_invalidation_confirmed(row, tracker=hard_invalidation, days_since_warning=days_since_warning):
                    hard_invalidation.first_action_dt = decision_dt
                    hard_invalidation.time_decay_score += max(1, days_since_warning)
                    if candidate_name == HARD_INVALIDATION_SEVERITY_CANDIDATE:
                        severity_plan = _hard_invalidation_severity_plan(
                            row,
                            state=state,
                            days_since_warning=days_since_warning,
                            confirmed=confirmed,
                        )
                    elif candidate_name == HARD_INVALIDATION_PROFIT_PRESERVATION_GUARD_CANDIDATE:
                        severity_plan = _hard_invalidation_profit_preservation_guard_plan(
                            row,
                            state=state,
                            days_since_warning=days_since_warning,
                            confirmed=confirmed,
                        )
                    elif candidate_name == HARD_INVALIDATION_NON_EXIT_LATE_EXTENSION_HEDGE_CANDIDATE:
                        severity_plan = _hard_invalidation_non_exit_late_extension_hedge_plan(
                            row,
                            state=state,
                            days_since_warning=days_since_warning,
                            confirmed=confirmed,
                        )
                    else:
                        severity_plan = None
                    if severity_plan is not None:
                        hard_invalidation.severity_target_buy_units = int(severity_plan["target_buy_units"])
                        hard_invalidation.severity_level = str(severity_plan["severity_level"])
                        hard_invalidation.severity_action_type = str(severity_plan["action_type"])
                        hard_invalidation.reduction_intensity = severity_plan.get("reduction_intensity")
                        hard_invalidation.high_severity_condition_met = bool(severity_plan.get("high_severity_condition_met"))
                        hard_invalidation.loss_side_override_applied = bool(
                            severity_plan.get("loss_side_override_applied")
                        )
                        hard_invalidation.profit_protection_guard_applied = bool(
                            severity_plan.get("profit_protection_guard_applied")
                        )
                        hard_invalidation.profit_preservation_guard_applied = bool(
                            severity_plan.get("profit_preservation_guard_applied")
                        )
                        hard_invalidation.profit_preservation_guard_reason = (
                            str(severity_plan.get("profit_preservation_guard_reason"))
                            if severity_plan.get("profit_preservation_guard_reason") is not None
                            else None
                        )
                        hard_invalidation.late_extension_hedge_condition_activated = bool(
                            severity_plan.get("late_extension_hedge_condition_activated")
                        )
                        hard_invalidation.late_extension_hedge_condition_reason = (
                            str(severity_plan.get("late_extension_hedge_condition_reason"))
                            if severity_plan.get("late_extension_hedge_condition_reason") is not None
                            else None
                        )
                        hard_invalidation.first_action_type = str(severity_plan["action_type"])
                        hard_invalidation.first_action_target_buy_units = int(severity_plan["target_buy_units"])
                        hard_invalidation.first_action_severity_level = str(severity_plan["severity_level"])
                        hard_invalidation.first_action_severity_action_type = str(severity_plan["action_type"])
                        hard_invalidation.first_action_reduction_intensity = severity_plan.get("reduction_intensity")
                        if first_hard_invalidation_action_date is None:
                            first_hard_invalidation_action_date = _ymd_to_date_text(decision_dt)
                        if str(severity_plan["action_type"]) == "exit_all":
                            hard_invalidation.exit_forced = True
                            if first_hard_invalidation_exit_date is None:
                                first_hard_invalidation_exit_date = _ymd_to_date_text(decision_dt)
                            planned_actions = [
                                {
                                    "action_type": "exit_all",
                                    "long_delta": -state.buy_units,
                                    "hedge_delta": -state.sell_units,
                                    "blocked_by_cap": False,
                                    "forced_exit_reason": candidate_name,
                                    "severity_level": hard_invalidation.severity_level,
                                    "severity_action_type": hard_invalidation.severity_action_type,
                                    "reduction_intensity": hard_invalidation.reduction_intensity,
                                    "high_severity_condition_met": hard_invalidation.high_severity_condition_met,
                                    "loss_side_override_applied": hard_invalidation.loss_side_override_applied,
                                    "profit_protection_guard_applied": hard_invalidation.profit_protection_guard_applied,
                                    "profit_preservation_guard_applied": hard_invalidation.profit_preservation_guard_applied,
                                    "profit_preservation_guard_reason": hard_invalidation.profit_preservation_guard_reason,
                                    "late_extension_hedge_condition_activated": hard_invalidation.late_extension_hedge_condition_activated,
                                    "late_extension_hedge_condition_reason": hard_invalidation.late_extension_hedge_condition_reason,
                                }
                            ]
                        elif str(severity_plan["action_type"]) == "hedge_add":
                            hard_invalidation.exit_forced = False
                            hedge_delta = (
                                HEAVY_HEDGE_UNITS
                                if int(state.buy_units) >= CONFIRMED_LONG_UNITS
                                else LIGHT_HEDGE_UNITS
                            )
                            planned_actions = [
                                {
                                    "action_type": "hedge_add",
                                    "long_delta": 0,
                                    "hedge_delta": int(hedge_delta),
                                    "blocked_by_cap": False,
                                    "forced_exit_reason": candidate_name,
                                    "special_reason": "late_extension_blocked",
                                    "severity_level": hard_invalidation.severity_level,
                                    "severity_action_type": hard_invalidation.severity_action_type,
                                    "reduction_intensity": hard_invalidation.reduction_intensity,
                                    "severity_target_buy_units": int(hard_invalidation.severity_target_buy_units),
                                    "high_severity_condition_met": hard_invalidation.high_severity_condition_met,
                                    "loss_side_override_applied": hard_invalidation.loss_side_override_applied,
                                    "profit_protection_guard_applied": hard_invalidation.profit_protection_guard_applied,
                                    "profit_preservation_guard_applied": hard_invalidation.profit_preservation_guard_applied,
                                    "profit_preservation_guard_reason": hard_invalidation.profit_preservation_guard_reason,
                                    "late_extension_hedge_condition_activated": hard_invalidation.late_extension_hedge_condition_activated,
                                    "late_extension_hedge_condition_reason": hard_invalidation.late_extension_hedge_condition_reason,
                                }
                            ]
                        else:
                            hard_invalidation.exit_forced = False
                            planned_actions = [
                                {
                                    "action_type": "long_reduce",
                                    "long_delta": int(hard_invalidation.severity_target_buy_units) - state.buy_units,
                                    "hedge_delta": 0,
                                    "blocked_by_cap": False,
                                    "forced_exit_reason": candidate_name,
                                    "special_reason": (
                                        "hard_invalidation_profit_preservation_guard_long_reduce"
                                        if hard_invalidation.profit_preservation_guard_applied
                                        else (
                                            "partial_exit_due_to_hard_invalidation"
                                            if hard_invalidation.severity_level == "partial_exit"
                                            else "hard_invalidation_long_reduce"
                                        )
                                    ),
                                    "severity_level": hard_invalidation.severity_level,
                                    "severity_action_type": hard_invalidation.severity_action_type,
                                    "reduction_intensity": hard_invalidation.reduction_intensity,
                                    "severity_target_buy_units": int(hard_invalidation.severity_target_buy_units),
                                    "high_severity_condition_met": hard_invalidation.high_severity_condition_met,
                                    "loss_side_override_applied": hard_invalidation.loss_side_override_applied,
                                    "profit_protection_guard_applied": hard_invalidation.profit_protection_guard_applied,
                                    "profit_preservation_guard_applied": hard_invalidation.profit_preservation_guard_applied,
                                    "profit_preservation_guard_reason": hard_invalidation.profit_preservation_guard_reason,
                                    "late_extension_hedge_condition_activated": hard_invalidation.late_extension_hedge_condition_activated,
                                    "late_extension_hedge_condition_reason": hard_invalidation.late_extension_hedge_condition_reason,
                                }
                            ]
                    else:
                        hard_invalidation.exit_forced = True
                        if first_hard_invalidation_exit_date is None:
                            first_hard_invalidation_exit_date = _ymd_to_date_text(decision_dt)
            if hard_invalidation.exit_forced:
                planned_actions = [
                    {
                        "action_type": "exit_all",
                        "long_delta": -state.buy_units,
                        "hedge_delta": -state.sell_units,
                        "blocked_by_cap": False,
                        "forced_exit_reason": candidate_name,
                        "severity_level": hard_invalidation.severity_level,
                        "severity_action_type": hard_invalidation.severity_action_type,
                        "reduction_intensity": hard_invalidation.reduction_intensity,
                        "high_severity_condition_met": hard_invalidation.high_severity_condition_met,
                        "loss_side_override_applied": hard_invalidation.loss_side_override_applied,
                        "profit_protection_guard_applied": hard_invalidation.profit_protection_guard_applied,
                        "profit_preservation_guard_applied": hard_invalidation.profit_preservation_guard_applied,
                        "profit_preservation_guard_reason": hard_invalidation.profit_preservation_guard_reason,
                    }
                ]

        if mode == "corrected" and profit_take_rules_active:
            if profit_take_signal and state.buy_units > CONFIRMED_LONG_UNITS:
                would_open_or_add = any(action["action_type"] in {"trial_buy", "long_add"} for action in planned_actions)
                if not would_open_or_add or state.buy_units > CONFIRMED_LONG_UNITS:
                    transformed: list[dict[str, Any]] = []
                    trimmed = False
                    target_core = CONFIRMED_LONG_UNITS
                    for planned in planned_actions:
                        if planned["action_type"] in {"long_add", "trial_buy"} and state.buy_units > target_core:
                            trimmed = True
                            continue
                        transformed.append(planned)
                    if trimmed:
                        transformed.insert(
                            0,
                            {
                                "action_type": "long_reduce",
                                "long_delta": -(state.buy_units - target_core),
                                "hedge_delta": 0,
                                "blocked_by_cap": False,
                            },
                        )
                        profit_take_reentry.active = True
                        if first_profit_take_date is None:
                            first_profit_take_date = _ymd_to_date_text(decision_dt)
                        if profit_take_reentry.first_profit_take_action_dt is None:
                            profit_take_reentry.first_profit_take_action_dt = decision_dt
                    planned_actions = transformed or planned_actions
            if profit_take_reentry.profit_taken and state.buy_units == 0 and reentry_signal:
                open_actions = any(action["action_type"] in {"trial_buy", "long_add"} for action in planned_actions)
                if not open_actions:
                    planned_actions = [
                        {
                            "action_type": "trial_buy",
                            "long_delta": ENTRY_LONG_UNITS,
                            "hedge_delta": 0,
                            "blocked_by_cap": False,
                        },
                        {
                            "action_type": "long_add",
                            "long_delta": CONFIRMED_LONG_UNITS - ENTRY_LONG_UNITS,
                            "hedge_delta": 0,
                            "blocked_by_cap": False,
                        },
                    ]
            if profit_take_reentry.reentry_active and reentry_confirmation_signal and state.buy_units > 0:
                if not any(action["action_type"] == "long_add" for action in planned_actions):
                    planned_actions = planned_actions + [
                        {
                            "action_type": "long_add",
                            "long_delta": 3,
                            "hedge_delta": 0,
                            "blocked_by_cap": False,
                        }
                    ]

        day_long_units_before = int(state.buy_units)
        day_sell_units_before = int(state.sell_units)
        execution_price = _safe_float(execution_bar.get("o"))
        day_actions: list[dict[str, Any]] = []
        for planned in planned_actions:
            action_type = str(planned["action_type"])
            forced_exit_reason = planned.get("forced_exit_reason")
            planned_special_reason = planned.get("special_reason")
            before_buy_units = int(state.buy_units)
            before_sell_units = int(state.sell_units)
            before_position = _position_text(before_sell_units, before_buy_units)
            realized_before = float(state.realized_pnl)
            _apply_action(
                state,
                action_type=action_type,
                long_delta=int(planned["long_delta"]),
                hedge_delta=int(planned["hedge_delta"]),
                execution_price=execution_price,
            )
            after_buy_units = int(state.buy_units)
            after_sell_units = int(state.sell_units)
            after_position = _position_text(after_sell_units, after_buy_units)
            realized_after = float(state.realized_pnl)
            if action_type == "trial_buy" and first_entry_date is None:
                first_entry_date = _ymd_to_date_text(decision_dt)
            if action_type == "stop_add":
                add_stop_count += 1
                if first_add_stop_date is None:
                    first_add_stop_date = _ymd_to_date_text(decision_dt)
            if mode == "corrected":
                if not followthrough.stop_active:
                    max_long_pre_failed_followthrough_stop = max(max_long_pre_failed_followthrough_stop, int(state.buy_units))
                    max_gross_pre_failed_followthrough_stop = max(max_gross_pre_failed_followthrough_stop, int(state.buy_units + state.sell_units))
            elif not confirmed:
                max_long_pre_failed_followthrough_stop = max(max_long_pre_failed_followthrough_stop, int(state.buy_units))
                max_gross_pre_failed_followthrough_stop = max(max_gross_pre_failed_followthrough_stop, int(state.buy_units + state.sell_units))
            action_counts[action_type] += 1
            special_reason: str | None = str(planned_special_reason) if planned_special_reason is not None else None
            if profit_take_rules_active:
                if action_type == "long_reduce" and profit_take_signal:
                    special_reason = "profit_take_reasonable"
                elif action_type in {"trial_buy", "long_add"} and reentry_signal:
                    special_reason = "valid_reentry" if not profit_take_reentry.reentry_confirmed else "valid_reentry"
            day_actions.append(
                _build_action_trace_row(
                    action_type=action_type,
                    before_position=before_position,
                    after_position=after_position,
                    reason=_reason_for(
                        action_type,
                        confirmed=confirmed,
                        blocked_by_cap=bool(planned.get("blocked_by_cap")),
                        row=row,
                        base_reason_map=base_reason_map,
                        forced_exit_reason=str(forced_exit_reason) if forced_exit_reason is not None else None,
                        special_reason=special_reason,
                    ),
                    special_reason=special_reason,
                    forced_exit_reason=str(forced_exit_reason) if forced_exit_reason is not None else None,
                    evidence_for=_evidence_for(row),
                    evidence_against=_evidence_against(row),
                    risk_warning=(
                        "hard-invalidation exit active"
                        if forced_exit_reason is not None
                        else (
                            "re-entry block active"
                            if special_reason == "reentry_blocked"
                            else (
                                "failed-followthrough time stop active"
                                if action_type == "stop_add"
                                else (
                                    "failed-followthrough time stop warning"
                                    if not confirmed and state.buy_units > 0 and action_type in {"trial_buy", "long_add", "watch"}
                                    else "structure weakening"
                                    if action_type in {"long_reduce", "exit_all"}
                                    else "continuation needs follow-through"
                                )
                            )
                        )
                    ),
                    confidence=_confidence_for_action(action_type, confirmed=confirmed, blocked_by_cap=bool(planned.get("blocked_by_cap"))),
                    active_thesis=_active_thesis(row, confirmed=confirmed),
                    before_buy_units=before_buy_units,
                    before_sell_units=before_sell_units,
                    after_buy_units=after_buy_units,
                    after_sell_units=after_sell_units,
                    realized_before=realized_before,
                    realized_after=realized_after,
                    execution_price=execution_price,
                    severity_level=planned.get("severity_level"),
                    severity_action_type=planned.get("severity_action_type"),
                    reduction_intensity=planned.get("reduction_intensity"),
                    severity_target_buy_units=planned.get("severity_target_buy_units"),
                    profit_protection_guard_applied=planned.get("profit_protection_guard_applied"),
                    profit_preservation_guard_applied=planned.get("profit_preservation_guard_applied"),
                    profit_preservation_guard_reason=planned.get("profit_preservation_guard_reason"),
                    late_extension_hedge_condition_activated=planned.get("late_extension_hedge_condition_activated"),
                    late_extension_hedge_condition_reason=planned.get("late_extension_hedge_condition_reason"),
                )
            )

        day_long_units_after = int(state.buy_units)
        if profit_take_rules_active:
            if profit_take_signal and day_long_units_before > day_long_units_after:
                profit_take_reentry.profit_taken = True
                if first_profit_take_date is None:
                    first_profit_take_date = _ymd_to_date_text(decision_dt)
                if profit_take_reentry.first_profit_take_dt is None:
                    profit_take_reentry.first_profit_take_dt = decision_dt
                if bad_reentry_rules_active:
                    bad_reentry.profit_taken = True
                    if bad_reentry.first_profit_take_dt is None:
                        bad_reentry.first_profit_take_dt = decision_dt
            if reentry_signal and day_long_units_before <= CONFIRMED_LONG_UNITS and day_long_units_after > day_long_units_before:
                profit_take_reentry.reentry_active = True
                if profit_take_reentry.first_reentry_action_dt is None:
                    profit_take_reentry.first_reentry_action_dt = decision_dt
            if reentry_confirmation_signal and day_long_units_after > day_long_units_before and day_long_units_after > 0:
                profit_take_reentry.reentry_confirmed = True
                if profit_take_reentry.first_reentry_confirmation_dt is None:
                    profit_take_reentry.first_reentry_confirmation_dt = decision_dt
            if reentry_signal and day_long_units_before <= CONFIRMED_LONG_UNITS and day_long_units_after <= day_long_units_before and first_missed_reentry_date is None:
                first_missed_reentry_date = _ymd_to_date_text(decision_dt)
                profit_take_reentry.risk_materially_increased = True
        if bad_reentry_rules_active and action_type in {"long_reduce", "exit_all"} and first_profit_take_candidate_date is not None:
            bad_reentry.profit_taken = True
            bad_reentry.active = True
            if bad_reentry.first_profit_take_dt is None:
                bad_reentry.first_profit_take_dt = decision_dt
            if first_profit_take_date is None:
                first_profit_take_date = _ymd_to_date_text(decision_dt)
            bad_reentry_event_log.append(
                {
                    "date": _ymd_to_date_text(decision_dt),
                    "event_type": "bad_reentry_candidate",
                    "symbol": symbol,
                    "reason": "profit take after extension and exhaustion created a later bad re-entry window",
                    "action_type": action_type,
                }
            )

        if not day_actions:
            before_buy_units = int(state.buy_units)
            before_sell_units = int(state.sell_units)
            before_position = _position_text(before_sell_units, before_buy_units)
            realized_before = float(state.realized_pnl)
            day_actions.append(
                _build_action_trace_row(
                    action_type="watch",
                    before_position=before_position,
                    after_position=before_position,
                    reason=_reason_for("watch", confirmed=confirmed, blocked_by_cap=False, row=row, base_reason_map=base_reason_map),
                    special_reason=None,
                    forced_exit_reason=None,
                    evidence_for=_evidence_for(row),
                    evidence_against=_evidence_against(row),
                    risk_warning="no conviction yet",
                    confidence=_confidence_for_action("watch", confirmed=confirmed),
                    active_thesis=_active_thesis(row, confirmed=confirmed),
                    before_buy_units=before_buy_units,
                    before_sell_units=before_sell_units,
                    after_buy_units=before_buy_units,
                    after_sell_units=before_sell_units,
                    realized_before=realized_before,
                    realized_after=realized_before,
                    execution_price=execution_price,
                    severity_level=None,
                    severity_action_type=None,
                    reduction_intensity=None,
                    severity_target_buy_units=None,
                    profit_protection_guard_applied=None,
                    profit_preservation_guard_applied=None,
                    profit_preservation_guard_reason=None,
                    late_extension_hedge_condition_activated=None,
                    late_extension_hedge_condition_reason=None,
                )
            )
            action_counts["watch"] += 1

        if state.buy_units > 0 and current_cycle is not None:
            current_cycle["entry_index"] = int(current_cycle.get("entry_index") or index)
            if first_entry_date is None:
                first_entry_date = _ymd_to_date_text(int(current_cycle["entry_decision_date"]))

        if current_cycle is not None and state.buy_units == 0 and state.sell_units == 0:
            current_cycle = None

        if current_cycle is None and (state.buy_units > 0 or state.sell_units > 0):
            current_cycle = {
                "symbol": symbol,
                "entry_index": index,
                "entry_decision_date": decision_dt,
                "entry_execution_date": execution_dt,
            }
            if first_entry_date is None:
                first_entry_date = _ymd_to_date_text(decision_dt)

        unrealized_pnl = _mark_unrealized(state, close_price=_safe_float(execution_bar.get("c")))
        if current_cycle is not None:
            current_cycle.setdefault("mark_to_market", []).append(float(unrealized_pnl))
            current_cycle["realized_pnl"] = float(state.realized_pnl)
            current_cycle["max_unrealized_pnl"] = float(max(current_cycle["mark_to_market"]))
            current_cycle["min_unrealized_pnl"] = float(min(current_cycle["mark_to_market"]))

        if terminal_source_end and (state.buy_units > 0 or state.sell_units > 0):
            before_buy_units = int(state.buy_units)
            before_sell_units = int(state.sell_units)
            before_position = _position_text(before_sell_units, before_buy_units)
            realized_before = float(state.realized_pnl)
            _apply_state_transition(state=state, target_buy_units=0, target_sell_units=0, execution_price=execution_price)
            after_buy_units = int(state.buy_units)
            after_sell_units = int(state.sell_units)
            after_position = _position_text(after_sell_units, after_buy_units)
            realized_after = float(state.realized_pnl)
            action_counts["exit_all"] += 1
            day_actions.append(
                _build_action_trace_row(
                    action_type="exit_all",
                    before_position=before_position,
                    after_position=after_position,
                    reason="time_stop",
                    special_reason=None,
                    forced_exit_reason=None,
                    evidence_for=_evidence_for(row),
                    evidence_against=_evidence_against(row),
                    risk_warning="source coverage end forced close",
                    confidence=_confidence_for_action("exit_all", confirmed=confirmed),
                    active_thesis=_active_thesis(row, confirmed=confirmed),
                    before_buy_units=before_buy_units,
                    before_sell_units=before_sell_units,
                    after_buy_units=after_buy_units,
                    after_sell_units=after_sell_units,
                    realized_before=realized_before,
                    realized_after=realized_after,
                    execution_price=execution_price,
                    severity_level=None,
                    severity_action_type=None,
                    reduction_intensity=None,
                    severity_target_buy_units=None,
                    profit_protection_guard_applied=None,
                    profit_preservation_guard_applied=None,
                    profit_preservation_guard_reason=None,
                    late_extension_hedge_condition_activated=None,
                    late_extension_hedge_condition_reason=None,
                )
            )
            if current_cycle is not None:
                current_cycle.setdefault("mark_to_market", []).append(float(_mark_unrealized(state, close_price=_safe_float(execution_bar.get("c")))))
                current_cycle["realized_pnl"] = float(state.realized_pnl)

        if current_cycle is not None and state.buy_units == 0 and state.sell_units == 0:
            current_cycle["exit_decision_date"] = decision_dt
            current_cycle["exit_execution_date"] = execution_dt
            current_cycle["exit_reason"] = day_actions[-1]["reason"] if day_actions else None
            current_cycle["exit_index"] = index

        action_trace.extend(day_actions)

    final_total_pnl = float(state.realized_pnl + _mark_unrealized(state, close_price=_safe_float(frame.iloc[-1]["c"])))
    hard_invalidation_event_rows = [
        row
        for row in action_trace
        if str(row.get("reason")) in {
            "hard_invalidation_exit_v1",
            "hard_invalidation_exit_all",
            "hard_invalidation_exit_severity_v2_loss_side_override",
            HARD_INVALIDATION_PROFIT_PRESERVATION_GUARD_CANDIDATE,
            "hard_invalidation_profit_preservation_guard_long_reduce",
            "late_extension_blocked",
            "hard_invalidation_long_reduce",
            "partial_exit_due_to_hard_invalidation",
        }
        or str(row.get("forced_exit_reason")) in {
            "hard_invalidation_exit_v1",
            "hard_invalidation_exit_severity_v2_loss_side_override",
            "hard_invalidation_exit_all",
            "hard_invalidation_profit_preservation_guard_v1",
        }
    ]
    hard_invalidation_event_row = None
    if first_hard_invalidation_action_date is not None:
        hard_invalidation_event_row = next(
            (
                row
                for row in hard_invalidation_event_rows
                if str(row.get("date")) == str(first_hard_invalidation_action_date)
            ),
            None,
        )
    if hard_invalidation_event_row is None:
        if candidate_name == HARD_INVALIDATION_SEVERITY_CANDIDATE:
            hard_invalidation_event_row = next(
                (row for row in hard_invalidation_event_rows if str(row.get("action_type")) == "exit_all"),
                hard_invalidation_event_rows[0] if hard_invalidation_event_rows else None,
            )
        elif candidate_name == HARD_INVALIDATION_NON_EXIT_LATE_EXTENSION_HEDGE_CANDIDATE:
            hard_invalidation_event_row = next(
                (row for row in hard_invalidation_event_rows if str(row.get("action_type")) == "hedge_add"),
                hard_invalidation_event_rows[0] if hard_invalidation_event_rows else None,
            )
        else:
            hard_invalidation_event_row = hard_invalidation_event_rows[0] if hard_invalidation_event_rows else None
    hard_invalidation_event_target_buy_units: int | None = None
    if hard_invalidation_event_row is not None:
        target_position_after = str(hard_invalidation_event_row.get("target_position_after") or "")
        if "-" in target_position_after:
            try:
                hard_invalidation_event_target_buy_units = int(target_position_after.split("-", 1)[1])
            except ValueError:
                hard_invalidation_event_target_buy_units = None
    hard_invalidation_summary_level = (
        str(hard_invalidation_event_row.get("severity_level"))
        if hard_invalidation_event_row is not None and hard_invalidation_event_row.get("severity_level") is not None
        else hard_invalidation.first_action_severity_level
        if hard_invalidation.first_action_severity_level is not None
        else hard_invalidation.severity_level
    )
    hard_invalidation_summary_action_type = (
        str(hard_invalidation_event_row.get("severity_action_type"))
        if hard_invalidation_event_row is not None and hard_invalidation_event_row.get("severity_action_type") is not None
        else hard_invalidation.first_action_severity_action_type
        if hard_invalidation.first_action_severity_action_type is not None
        else hard_invalidation.severity_action_type
    )
    hard_invalidation_summary_reduction_intensity = (
        hard_invalidation_event_row.get("reduction_intensity")
        if hard_invalidation_event_row is not None and hard_invalidation_event_row.get("reduction_intensity") is not None
        else hard_invalidation.first_action_reduction_intensity
        if hard_invalidation.first_action_reduction_intensity is not None
        else hard_invalidation.reduction_intensity
    )
    hard_invalidation_summary_target_buy_units = (
        hard_invalidation_event_target_buy_units
        if hard_invalidation_event_target_buy_units is not None
        else hard_invalidation.first_action_target_buy_units
        if hard_invalidation.first_action_target_buy_units is not None
        else hard_invalidation.severity_target_buy_units
    )
    hard_invalidation_summary_high_severity_condition_met = (
        bool(hard_invalidation_event_row.get("high_severity_condition_met"))
        if hard_invalidation_event_row is not None and hard_invalidation_event_row.get("high_severity_condition_met") is not None
        else bool(hard_invalidation.high_severity_condition_met)
    )
    hard_invalidation_summary_profit_protection_guard_applied = (
        bool(hard_invalidation_event_row.get("profit_protection_guard_applied"))
        if hard_invalidation_event_row is not None and hard_invalidation_event_row.get("profit_protection_guard_applied") is not None
        else bool(hard_invalidation.profit_protection_guard_applied)
    )
    hard_invalidation_summary_profit_preservation_guard_applied = (
        bool(hard_invalidation_event_row.get("profit_preservation_guard_applied"))
        if hard_invalidation_event_row is not None and hard_invalidation_event_row.get("profit_preservation_guard_applied") is not None
        else bool(hard_invalidation.profit_preservation_guard_applied)
    )
    hard_invalidation_summary_profit_preservation_guard_reason = (
        str(hard_invalidation_event_row.get("profit_preservation_guard_reason"))
        if hard_invalidation_event_row is not None and hard_invalidation_event_row.get("profit_preservation_guard_reason") is not None
        else hard_invalidation.profit_preservation_guard_reason
    )
    hard_invalidation_summary_late_extension_hedge_condition_activated = (
        bool(hard_invalidation_event_row.get("late_extension_hedge_condition_activated"))
        if hard_invalidation_event_row is not None and hard_invalidation_event_row.get("late_extension_hedge_condition_activated") is not None
        else bool(hard_invalidation.late_extension_hedge_condition_activated)
    )
    hard_invalidation_summary_late_extension_hedge_condition_reason = (
        str(hard_invalidation_event_row.get("late_extension_hedge_condition_reason"))
        if hard_invalidation_event_row is not None and hard_invalidation_event_row.get("late_extension_hedge_condition_reason") is not None
        else hard_invalidation.late_extension_hedge_condition_reason
    )
    if state.buy_units > 0 and current_cycle is not None and first_hard_invalidation_date is None:
        first_hard_invalidation_date = _ymd_to_date_text(int(frame.iloc[-1]["dt"]))

    return {
        "case": {
            "symbol": symbol,
            "name": case_spec["name"],
            "role": case_spec["role"],
            "start_date": start_date,
            "end_date": end_date,
            "trade_start_date": trade_start_date,
            "expected_themes": list(case_spec["expected_themes"]),
        },
        "mode": mode,
        "position_contract": {
            "notation": "X-Y",
            "sell_units": "X",
            "buy_units": "Y",
            "shares_per_unit": SHARES_PER_UNIT,
            "interpretation": "hedge/short units are recorded on the sell side; long units are recorded on the buy side",
        },
        "summary": {
            "final_position": _position_text(state.sell_units, state.buy_units),
            "total_realized_pnl": float(state.realized_pnl),
            "total_unrealized_pnl": float(_mark_unrealized(state, close_price=_safe_float(frame.iloc[-1]["c"]))),
            "total_pnl": float(final_total_pnl),
            "action_counts": action_counts,
            "first_entry_date": first_entry_date,
            "first_confirmation_date": first_confirmation_date,
            "first_time_stop_warning_date": first_time_stop_warning_date,
            "first_hard_invalidation_date": first_hard_invalidation_date,
            "first_hard_invalidation_exit_date": first_hard_invalidation_exit_date,
            "first_hard_invalidation_action_date": first_hard_invalidation_action_date,
            "hard_invalidation_severity_level": hard_invalidation_summary_level,
            "hard_invalidation_severity_action_type": hard_invalidation_summary_action_type,
            "hard_invalidation_reduction_intensity": hard_invalidation_summary_reduction_intensity,
            "hard_invalidation_severity_target_buy_units": hard_invalidation_summary_target_buy_units,
            "hard_invalidation_high_severity_condition_met": hard_invalidation_summary_high_severity_condition_met,
            "hard_invalidation_loss_side_override_applied": bool(hard_invalidation.loss_side_override_applied),
            "hard_invalidation_profit_protection_guard_applied": hard_invalidation_summary_profit_protection_guard_applied,
            "hard_invalidation_profit_preservation_guard_applied": hard_invalidation_summary_profit_preservation_guard_applied,
            "hard_invalidation_profit_preservation_guard_reason": hard_invalidation_summary_profit_preservation_guard_reason,
            "hard_invalidation_late_extension_hedge_condition_activated": hard_invalidation_summary_late_extension_hedge_condition_activated,
            "hard_invalidation_late_extension_hedge_condition_reason": hard_invalidation_summary_late_extension_hedge_condition_reason,
            "hard_invalidation_contract_violation": bool(
                hard_invalidation_summary_level is not None
                and hard_invalidation_summary_action_type is not None
                and hard_invalidation_summary_target_buy_units is not None
                and (
                    (
                        hard_invalidation_summary_level == "exit_all"
                        and (
                            hard_invalidation_summary_action_type != "exit_all"
                            or int(hard_invalidation_summary_target_buy_units) != 0
                        )
                    )
                    or (
                        hard_invalidation_summary_level in {"long_reduce", "partial_exit"}
                        and (
                            hard_invalidation_summary_action_type != "long_reduce"
                            or int(hard_invalidation_summary_target_buy_units) <= 0
                        )
                    )
                )
            ),
            "first_add_stop_date": first_add_stop_date,
            "first_failed_followthrough_date": first_failed_followthrough_date,
            "first_profit_take_candidate_date": first_profit_take_candidate_date,
            "first_profit_take_date": first_profit_take_date,
            "first_reentry_candidate_date": first_reentry_candidate_date,
            "first_missed_reentry_date": first_missed_reentry_date,
            "first_reentry_confirmation_date": first_reentry_confirmation_date,
            "first_bad_reentry_candidate_date": _ymd_to_date_text(int(bad_reentry.first_bad_reentry_candidate_dt)) if bad_reentry.first_bad_reentry_candidate_dt is not None else None,
            "first_reentry_block_date": _ymd_to_date_text(int(bad_reentry.first_reentry_block_dt)) if bad_reentry.first_reentry_block_dt is not None else None,
            "false_reentry": bool(profit_take_reentry.risk_materially_increased and profit_take_reentry.reentry_active and not profit_take_reentry.reentry_confirmed),
            "risk_materially_increased": bool(profit_take_reentry.risk_materially_increased),
            "opportunity_loss_score": float(profit_take_reentry.opportunity_loss_score),
            "bad_reentry_after_profit_take_applicable": bool(bad_reentry_rules_active),
            "false_reentry_avoided": bool(bad_reentry.false_reentry_avoided),
            "reentry_risk_reduced": bool(bad_reentry.risk_reduced),
            "bad_reentry_event_log": list(bad_reentry_event_log),
            "max_long_exposure_before_failed_followthrough_stop": int(max_long_pre_failed_followthrough_stop),
            "max_gross_exposure_before_failed_followthrough_stop": int(max_gross_pre_failed_followthrough_stop),
            "add_stop_count": int(add_stop_count),
            "confirmed": bool(first_confirmation_date is not None),
            "failed_followthrough_stop_active": bool(followthrough.stop_active),
            "hard_invalidation_stop_active": bool(hard_invalidation.exit_forced),
            "time_decay_score": int(followthrough.time_decay_score),
            "hard_invalidation_time_decay_score": int(hard_invalidation.time_decay_score),
            "bad_reentry_time_decay_score": int(bad_reentry.opportunity_loss_score),
            "no_lookahead_assertion": NO_LOOKAHEAD_ASSERTION,
        },
        "daily_action_trace": action_trace,
        "generated_at": _utc_now(),
    }


def _classify_case(result: dict[str, Any], *, mode: str, candidate_name: str = FAILED_FOLLOWTHROUGH_CANDIDATE) -> dict[str, Any]:
    summary = result["summary"]
    case = result["case"]
    total_pnl = float(summary["total_pnl"])
    max_long = int(summary["max_long_exposure_before_failed_followthrough_stop"])
    labels: list[str] = []
    stack_mode = candidate_name in {STACK_CANDIDATE, HARD_INVALIDATION_SEVERITY_CANDIDATE}
    hard_invalidation_mode = candidate_name in HARD_INVALIDATION_ACTIVE_CANDIDATES or stack_mode

    if case["role"] == "failure_control":
        if summary["first_failed_followthrough_date"] is not None:
            labels.append("failed_breakout_misread")
        if summary["first_time_stop_warning_date"] is not None:
            labels.append("late_time_stop")
        if max_long >= 7:
            labels.append("over_accumulation_before_confirmation")
        if total_pnl < 0:
            labels.append("reasonable_but_failed")
        if hard_invalidation_mode and summary["first_hard_invalidation_date"] is not None:
            labels.append("hard_invalidation_exit_applicable")
            if mode == "corrected" and summary["first_hard_invalidation_exit_date"] is None and candidate_name != HARD_INVALIDATION_SEVERITY_CANDIDATE:
                labels.append("missed_exit")
        if summary["failed_followthrough_stop_active"] and mode == "corrected" and total_pnl < 0:
            labels.append("missed_exit")
    elif case["role"] == "capture_control":
        if candidate_name == BAD_REENTRY_AFTER_PROFIT_TAKE_CANDIDATE or stack_mode:
            if summary.get("bad_reentry_after_profit_take_applicable"):
                labels.append("bad_reentry_after_profit_take_applicable")
            if summary.get("first_profit_take_date") is not None:
                labels.append("profit_take_reasonable")
            if summary.get("first_bad_reentry_candidate_date") is not None:
                labels.append("bad_reentry_after_profit_take")
            if mode == "corrected" and summary.get("first_reentry_block_date") is not None:
                labels.append("reentry_blocked")
                labels.append("false_reentry_avoided")
            if summary.get("false_reentry"):
                labels.append("false_reentry")
            if summary.get("reentry_risk_reduced"):
                labels.append("false_reentry_avoided")
            if total_pnl > 0 and summary.get("first_reentry_confirmation_date") is not None and not summary.get("false_reentry"):
                labels.append("success_pattern_preserved")
        else:
            if summary.get("first_profit_take_date") is not None:
                labels.append("profit_take_reasonable")
            elif mode == "baseline":
                labels.append("profit_take_too_late")
            if summary.get("first_reentry_candidate_date") is not None and summary.get("first_reentry_confirmation_date") is not None:
                labels.append("valid_reentry")
            if mode == "baseline" and summary.get("first_missed_reentry_date") is not None:
                labels.append("missed_reentry")
                labels.append("opportunity_loss_after_profit_take")
            if mode == "corrected" and summary.get("first_missed_reentry_date") is not None:
                labels.append("bad_reentry_blocked")
            if summary.get("false_reentry"):
                labels.append("false_reentry")
            if total_pnl > 0 or summary.get("first_reentry_confirmation_date") is not None:
                labels.append("success_pattern_preserved")
    elif case["role"] == "mining_candidate":
        axis_hit = False
        if summary.get("first_hard_invalidation_date") is not None:
            labels.append("hard_invalidation_exit_applicable")
            axis_hit = True
            if mode == "corrected" and summary.get("first_hard_invalidation_exit_date") is None:
                labels.append("missed_exit")
        if summary.get("first_failed_followthrough_date") is not None or summary.get("failed_followthrough_stop_active"):
            labels.append("failed_breakout_misread")
            axis_hit = True
            if summary.get("first_time_stop_warning_date") is not None:
                labels.append("late_time_stop")
            if mode == "corrected" and total_pnl < 0:
                labels.append("missed_exit")
        if summary.get("first_bad_reentry_candidate_date") is not None or summary.get("first_reentry_block_date") is not None or summary.get("false_reentry_avoided"):
            labels.append("bad_reentry_after_profit_take_applicable")
            axis_hit = True
            if summary.get("first_profit_take_date") is not None:
                labels.append("profit_take_reasonable")
            if summary.get("first_bad_reentry_candidate_date") is not None:
                labels.append("bad_reentry_after_profit_take")
            if mode == "corrected" and summary.get("first_reentry_block_date") is not None:
                labels.append("reentry_blocked")
                labels.append("false_reentry_avoided")
            if summary.get("false_reentry"):
                labels.append("false_reentry")
            if summary.get("reentry_risk_reduced"):
                labels.append("false_reentry_avoided")
            if total_pnl > 0 and summary.get("first_reentry_confirmation_date") is not None and not summary.get("false_reentry"):
                labels.append("success_pattern_preserved")
        if not axis_hit:
            labels.append("valid_read")
    else:
        labels.append("valid_read")
        labels.append("success_pattern_preserved")
        if total_pnl < 0:
            labels.append("blocked_valid_winner")
        if (candidate_name in {HARD_INVALIDATION_CANDIDATE, HARD_INVALIDATION_SEVERITY_CANDIDATE} or stack_mode) and summary["first_hard_invalidation_exit_date"] is not None:
            labels.append("false_exit_on_valid_pullback")
        if summary["failed_followthrough_stop_active"] and mode == "corrected" and total_pnl > 0:
            labels.append("hedge_correct_but_long_too_large")

    # de-dup and preserve order
    seen: set[str] = set()
    ordered = []
    for label in labels:
        if label in seen:
            continue
        seen.add(label)
        ordered.append(label)
    if not ordered:
        ordered = ["valid_read"]
    return {
        "case": case,
        "mode": mode,
        "labels": ordered,
        "label_counts": {label: ordered.count(label) for label in ordered},
        "interpretation": (
            "failure_control should reduce over-accumulation and late exits"
            if case["role"] == "failure_control"
            else "success_control should preserve line escape and follow-through"
        ),
        "total_pnl": total_pnl,
    }


def _compare_case(
    *,
    baseline: dict[str, Any],
    corrected: dict[str, Any],
) -> dict[str, Any]:
    b = baseline["summary"]
    c = corrected["summary"]
    baseline_loss = max(0.0, -float(b["total_pnl"]))
    corrected_loss = max(0.0, -float(c["total_pnl"]))
    baseline_profit = max(0.0, float(b["total_pnl"]))
    corrected_profit = max(0.0, float(c["total_pnl"]))
    taiheiyo_checks = {
        "line_escape_remains_valid": bool(c["first_confirmation_date"] is not None),
        "follow_through_remains_valid": bool(c["first_failed_followthrough_date"] is None and c["first_confirmation_date"] is not None),
        "add_path_preserved_after_followthrough": bool(c["first_add_stop_date"] is None or corrected_profit >= baseline_profit * 0.9),
        "valid_pullback_preserved": bool(c["first_confirmation_date"] is not None and corrected_profit >= baseline_profit * 0.9),
        "profit_materially_damaged": bool(baseline_profit > 0 and corrected_profit < baseline_profit * 0.9),
        "false_winner_block": bool(baseline["case"]["role"] == "success_control" and corrected_profit < baseline_profit * 0.9),
    }
    capture_checks = {
        "profit_take_reasonable": bool(c["first_profit_take_date"] is not None),
        "valid_reentry": bool(c["first_reentry_confirmation_date"] is not None),
        "missed_reentry": bool(b["first_missed_reentry_date"] is not None),
        "false_reentry": bool(c["false_reentry"]),
        "opportunity_loss_after_profit_take": float(max(0.0, baseline_loss - corrected_loss)),
        "risk_materially_increased": bool(c["risk_materially_increased"]),
        "success_pattern_preserved": bool(c["first_reentry_confirmation_date"] is not None and not c["false_reentry"]),
    }
    bad_reentry_checks = {
        "bad_reentry_after_profit_take_applicable": bool(c.get("bad_reentry_after_profit_take_applicable")),
        "profit_take_reasonable": bool(c["first_profit_take_date"] is not None),
        "bad_reentry_after_profit_take": bool(c.get("first_bad_reentry_candidate_date") is not None),
        "reentry_blocked": bool(c.get("first_reentry_block_date") is not None),
        "false_reentry_avoided": bool(c.get("false_reentry_avoided")),
        "reentry_risk_reduced": bool(c.get("false_reentry_avoided")),
        "false_block": bool(c.get("first_reentry_block_date") is not None and not c.get("false_reentry_avoided")),
        "opportunity_loss_after_profit_take": float(max(0.0, baseline_loss - corrected_loss)) if c.get("first_reentry_block_date") is None else 0.0,
        "success_pattern_preserved": bool(c.get("first_reentry_confirmation_date") is not None and not c.get("false_reentry") and corrected_profit > 0),
    }
    return {
        "symbol": baseline["case"]["symbol"],
        "name": baseline["case"]["name"],
        "role": baseline["case"]["role"],
        "baseline": {
            "total_pnl": float(b["total_pnl"]),
            "estimated_loss": float(baseline_loss),
            "first_add_stop_date": b["first_add_stop_date"],
            "first_failed_followthrough_date": b["first_failed_followthrough_date"],
            "first_time_stop_warning_date": b["first_time_stop_warning_date"],
            "first_hard_invalidation_date": b["first_hard_invalidation_date"],
            "first_hard_invalidation_exit_date": b["first_hard_invalidation_exit_date"],
            "first_profit_take_candidate_date": b.get("first_profit_take_candidate_date"),
            "first_profit_take_date": b.get("first_profit_take_date"),
            "first_reentry_candidate_date": b.get("first_reentry_candidate_date"),
            "first_missed_reentry_date": b.get("first_missed_reentry_date"),
            "first_reentry_confirmation_date": b.get("first_reentry_confirmation_date"),
            "first_bad_reentry_candidate_date": b.get("first_bad_reentry_candidate_date"),
            "first_reentry_block_date": b.get("first_reentry_block_date"),
            "max_long_exposure_before_failed_followthrough_stop": int(b["max_long_exposure_before_failed_followthrough_stop"]),
            "max_gross_exposure_before_failed_followthrough_stop": int(b["max_gross_exposure_before_failed_followthrough_stop"]),
            "labels": list(baseline["labels"]),
        },
        "corrected": {
            "total_pnl": float(c["total_pnl"]),
            "estimated_loss": float(corrected_loss),
            "first_add_stop_date": c["first_add_stop_date"],
            "first_failed_followthrough_date": c["first_failed_followthrough_date"],
            "first_time_stop_warning_date": c["first_time_stop_warning_date"],
            "first_hard_invalidation_date": c["first_hard_invalidation_date"],
            "first_hard_invalidation_exit_date": c["first_hard_invalidation_exit_date"],
            "first_profit_take_candidate_date": c.get("first_profit_take_candidate_date"),
            "first_profit_take_date": c.get("first_profit_take_date"),
            "first_reentry_candidate_date": c.get("first_reentry_candidate_date"),
            "first_missed_reentry_date": c.get("first_missed_reentry_date"),
            "first_reentry_confirmation_date": c.get("first_reentry_confirmation_date"),
            "first_bad_reentry_candidate_date": c.get("first_bad_reentry_candidate_date"),
            "first_reentry_block_date": c.get("first_reentry_block_date"),
            "max_long_exposure_before_failed_followthrough_stop": int(c["max_long_exposure_before_failed_followthrough_stop"]),
            "max_gross_exposure_before_failed_followthrough_stop": int(c["max_gross_exposure_before_failed_followthrough_stop"]),
            "labels": list(corrected["labels"]),
        },
        "comparison": {
            "estimated_loss_before_correction": float(baseline_loss),
            "estimated_loss_after_correction": float(corrected_loss),
            "avoidable_loss_estimate": float(max(0.0, baseline_loss - corrected_loss)),
            "profit_before_correction": float(baseline_profit),
            "profit_after_correction": float(corrected_profit),
            "profit_delta": float(corrected_profit - baseline_profit),
            "damage_reduced": bool(corrected_loss < baseline_loss),
            "material_profit_damage": bool(
                baseline_profit > 0 and corrected_profit < baseline_profit * 0.9
            ),
            "false_winner_block": bool(
                baseline["case"]["role"] == "success_control" and corrected_profit < baseline_profit * 0.9
            ),
            "hard_invalidation_first_actionable_signal": bool(
                corrected["summary"]["first_hard_invalidation_date"] is not None
                and corrected["summary"]["first_hard_invalidation_exit_date"] is not None
                and corrected["summary"]["first_hard_invalidation_date"] == corrected["summary"]["first_hard_invalidation_exit_date"]
            ),
            "preservation_checks": taiheiyo_checks,
            "capture_checks": capture_checks if baseline["case"]["role"] == "capture_control" else None,
            "bad_reentry_checks": bad_reentry_checks if baseline["case"]["role"] == "capture_control" else None,
        },
    }


def _build_case_result_artifact(
    *,
    baseline: dict[str, Any],
    corrected: dict[str, Any],
    mode: str,
    output_root: Path,
) -> dict[str, Any]:
    compare = _compare_case(baseline=baseline, corrected=corrected)
    case_name = f"{baseline['case']['symbol']}_{baseline['case']['name'].replace(' ', '_')}"
    case_dir = output_root / case_name
    case_dir.mkdir(parents=True, exist_ok=True)
    baseline_trace_path = _write_json(case_dir / f"{baseline['case']['symbol']}_baseline_trace.json", {"rows": baseline["daily_action_trace"]})
    corrected_trace_path = _write_json(case_dir / f"{baseline['case']['symbol']}_corrected_trace.json", {"rows": corrected["daily_action_trace"]})
    return {
        "case": baseline["case"],
        "trace_artifacts": {
            "baseline_trace_path": str(baseline_trace_path),
            "corrected_trace_path": str(corrected_trace_path),
        },
        "baseline": {
            "summary": baseline["summary"],
            "labels": baseline["labels"],
        },
        "corrected": {
            "summary": corrected["summary"],
            "labels": corrected["labels"],
        },
        "compare": compare,
        "mode": mode,
    }


def _build_kept_axis_stack_artifacts(
    *,
    case_artifacts: list[dict[str, Any]],
    case_comparisons: list[dict[str, Any]],
    decision: str,
    decision_reason: str,
) -> dict[str, Any]:
    case_artifact_by_symbol = {artifact["case"]["symbol"]: artifact for artifact in case_artifacts}
    case_compare_by_symbol = {artifact["symbol"]: artifact for artifact in case_comparisons}
    systena_compare = case_compare_by_symbol["2317"]["comparison"]
    hard_invalidation_compare = case_compare_by_symbol["9697"]["comparison"]
    bad_reentry_compare = case_compare_by_symbol["2531"]["comparison"]
    taiheiyo_compare = case_compare_by_symbol["5541"]["comparison"]

    axis_applicability = {
        "2317": [FAILED_FOLLOWTHROUGH_CANDIDATE],
        "9697": [HARD_INVALIDATION_CANDIDATE],
        "2531": [BAD_REENTRY_AFTER_PROFIT_TAKE_CANDIDATE],
        "5541": [],
    }

    stack_conflicts: list[str] = []
    if not bool(systena_compare["damage_reduced"]):
        stack_conflicts.append("failed_followthrough_damage_not_reduced")
    if not bool(hard_invalidation_compare["damage_reduced"]):
        stack_conflicts.append("hard_invalidation_damage_not_reduced")
    if not (bool(bad_reentry_compare["damage_reduced"]) or bool(bad_reentry_compare.get("reentry_risk_reduced"))):
        stack_conflicts.append("bad_reentry_risk_not_reduced")
    if bool(taiheiyo_compare["material_profit_damage"]) or bool(taiheiyo_compare["false_winner_block"]):
        stack_conflicts.append("winner_false_block")

    anchor_results = {
        "schema_version": "tradex_iizuka_kept_axis_stack_anchor_results_v1",
        "stack_name": STACK_CANDIDATE,
        "cases": [
            {
                "symbol": symbol,
                "case_label": case_artifact_by_symbol[symbol]["case"]["role"],
                "applicable_axes": list(axis_applicability.get(symbol, [])),
                "baseline": case_artifact_by_symbol[symbol]["baseline"],
                "corrected": case_artifact_by_symbol[symbol]["corrected"],
                "comparison": case_compare_by_symbol[symbol],
                "trace_artifacts": case_artifact_by_symbol[symbol]["trace_artifacts"],
            }
            for symbol in ["2317", "9697", "2531", "5541"]
        ],
    }

    precedence = {
        "schema_version": "tradex_iizuka_kept_axis_stack_precedence_v1",
        "stack_name": STACK_CANDIDATE,
        "precedence_order": [
            {
                "priority": 1,
                "axis_name": HARD_INVALIDATION_CANDIDATE,
                "when_active": "force long_reduce or exit_all and prohibit long_add",
            },
            {
                "priority": 2,
                "axis_name": FAILED_FOLLOWTHROUGH_CANDIDATE,
                "when_active": "set add_stop after a failed breakout / follow-through window",
            },
            {
                "priority": 3,
                "axis_name": BAD_REENTRY_AFTER_PROFIT_TAKE_CANDIDATE,
                "when_active": "block weak re-entry after profit taking and emit reentry_blocked",
            },
        ],
        "winner_preservation": [
            "preserve valid line escape winner state",
            "preserve follow-through confirmation when support continues to hold",
            "preserve valid pullback / support retest behavior on 5541-style winners",
        ],
    }

    interaction_summary = {
        "schema_version": "tradex_iizuka_kept_axis_stack_interaction_summary_v1",
        "stack_name": STACK_CANDIDATE,
        "stack_coexistence": not stack_conflicts,
        "anchor_outcomes": {
            symbol: {
                "damage_reduced": bool(case_compare_by_symbol[symbol]["comparison"]["damage_reduced"]),
                "material_profit_damage": bool(case_compare_by_symbol[symbol]["comparison"]["material_profit_damage"]),
                "false_winner_block": bool(case_compare_by_symbol[symbol]["comparison"]["false_winner_block"]),
                "preservation_checks": case_compare_by_symbol[symbol]["comparison"].get("preservation_checks"),
                "bad_reentry_checks": case_compare_by_symbol[symbol]["comparison"].get("bad_reentry_checks"),
            }
            for symbol in ["2317", "9697", "2531", "5541"]
        },
        "axis_conflicts_detected": list(stack_conflicts),
        "precedence_applied": list(precedence["precedence_order"]),
        "decision": decision,
        "decision_reason": decision_reason,
    }

    stack_spec = {
        "schema_version": "tradex_iizuka_kept_axis_stack_spec_v1",
        "stack_name": STACK_CANDIDATE,
        "scope": "TRADEX research only",
        "included_axes": list(STACKED_AXIS_CANDIDATES),
        "precedence": list(precedence["precedence_order"]),
        "anchor_set": [artifact["case"] for artifact in case_artifacts],
        "decision_rules": {
            "keep": "all three kept axes preserve their prior improvements and 5541 remains preserved",
            "hold": "one anchor is inconclusive or interaction needs clarification",
            "drop": "stacking causes a regression or a winner false-block",
        },
        "no_lookahead": {
            "actor": "current row and prior state only",
            "critic": "post-replay outcomes only",
        },
    }

    manifest = {
        "schema_version": "tradex_iizuka_kept_axis_stack_manifest_v1",
        "stack_name": STACK_CANDIDATE,
        "anchor_cases": [
            {
                "symbol": artifact["case"]["symbol"],
                "name": artifact["case"]["name"],
                "role": artifact["case"]["role"],
                "window": {
                    "start_date": artifact["case"]["start_date"],
                    "end_date": artifact["case"]["end_date"],
                    "trade_start_date": artifact["case"]["trade_start_date"],
                },
                "applicable_axes": list(axis_applicability.get(artifact["case"]["symbol"], [])),
                "expected_themes": list(artifact["case"]["expected_themes"]),
            }
            for artifact in case_artifacts
        ],
        "generated_at": _utc_now(),
    }

    error_report = {
        "schema_version": "tradex_iizuka_kept_axis_stack_error_report_v1",
        "stack_name": STACK_CANDIDATE,
        "case_reports": [
            {
                "symbol": artifact["case"]["symbol"],
                "name": artifact["case"]["name"],
                "role": artifact["case"]["role"],
                "baseline_labels": artifact["baseline"]["labels"],
                "corrected_labels": artifact["corrected"]["labels"],
                "baseline_summary": artifact["baseline"]["summary"],
                "corrected_summary": artifact["corrected"]["summary"],
                "compare": artifact["compare"],
            }
            for artifact in case_artifacts
        ],
    }

    decision_artifact = {
        "schema_version": "tradex_iizuka_kept_axis_stack_decision_v1",
        "stack_name": STACK_CANDIDATE,
        "decision": decision,
        "decision_reason": decision_reason,
        "candidate_local_decision": decision,
        "authoritative_rollup_decision": decision,
        "session_aggregate_decision": decision,
        "precedence": list(precedence["precedence_order"]),
        "result_basis": {
            "systena": case_compare_by_symbol["2317"],
            "hard_invalidation_anchor": case_compare_by_symbol["9697"],
            "bad_reentry_anchor": case_compare_by_symbol["2531"],
            "taiheiyo": case_compare_by_symbol["5541"],
        },
        "generated_at": _utc_now(),
    }

    return {
        "spec": stack_spec,
        "manifest": manifest,
        "precedence": precedence,
        "anchor_results": anchor_results,
        "interaction_summary": interaction_summary,
        "error_report": error_report,
        "decision": decision_artifact,
    }


def _build_error_taxonomy() -> dict[str, Any]:
    descriptions = {
        "valid_read": "The replay read the chart correctly and the action matched the observed structure.",
        "reasonable_but_failed": "The interpretation was defensible but the market still failed.",
        "too_early_entry": "The agent entered before confirmation and before the move proved itself.",
        "too_early_add": "The agent added size before confirmation or follow-through.",
        "over_accumulation_before_confirmation": "Size became too large before line escape / follow-through confirmation.",
        "failed_breakout_misread": "A breakout-like move was treated as durable when follow-through never arrived.",
        "bad_pullback_misread": "A pullback or stall was read as healthy when it invalidated the thesis.",
        "late_time_stop": "The agent held past the intended time stop and exited late.",
        "missed_reduce": "The agent should have reduced but kept the position too large.",
        "missed_exit": "The agent should have exited but kept risk open.",
        "hedge_correct_but_long_too_large": "Hedging was directionally right but the long side remained oversized.",
        "false_exit_on_valid_pullback": "The agent exited a winner during a valid pullback or retest.",
        "blocked_valid_winner": "The correction blocked a valid winner pattern.",
        "success_pattern_preserved": "The correction preserved the winner pattern and did not suppress the known good case.",
        "hard_invalidation_exit_applicable": "Hard invalidation appeared and the correction should exit or reduce instead of holding or adding.",
        "profit_take_reasonable": "Profit taking was justified by extension, exhaustion, and profit-state maturity.",
        "profit_take_too_early": "The position was trimmed before the trend had matured enough.",
        "profit_take_too_late": "Profit taking happened after the move was already mostly gone.",
        "missed_reentry": "A valid re-entry window appeared after profit taking, but the agent stayed flat or under-sized.",
        "valid_reentry": "The agent re-entered on a valid pullback or revalidation signal.",
        "bad_reentry_blocked": "The correction blocked a valid re-entry window.",
        "false_reentry": "The agent re-entered into a setup that later invalidated.",
        "opportunity_loss_after_profit_take": "Taking profit was fine, but failing to re-enter left upside on the table.",
        "bad_reentry_after_profit_take": "Re-entry after profit taking was weak, premature, or unsupported and should be blocked.",
        "bad_reentry_after_profit_take_applicable": "The anchor is applicable to the bad re-entry after profit-taking patch family.",
        "reentry_blocked": "A weak re-entry attempt was blocked after profit taking because the rebound did not revalidate the trend.",
        "false_reentry_avoided": "Blocking the re-entry avoided a later invalid or low-quality re-entry path.",
    }
    return {
        "schema_version": "tradex_iizuka_trade_learning_loop_error_taxonomy_v1",
        "labels": [
            {"label": label, "description": descriptions[label]}
            for label in ERROR_LABELS
        ],
    }


def _build_replay_spec(*, source_db_path: Path, output_root: Path, current_candidate_name: str) -> dict[str, Any]:
    return {
        "schema_version": "tradex_iizuka_trade_learning_loop_replay_spec_v1",
        "scope": "TRADEX research only",
        "purpose": "Replay Iizuka-style trading one day at a time, classify mistakes, and test one narrow rule correction.",
        "non_scope": [
            "MeeMee UI",
            "production ranking",
            "publish registry mutation",
            "publish candidate creation",
            "promotion flow",
            "live trading",
            "external order execution",
            "broad ranking experiments",
        ],
        "evaluation_conditions": {
            "same_universe": True,
            "same_period": True,
            "same_top_k": False,
            "same_regime": True,
            "same_cost_slippage": True,
            "same_artifact_detail_level": True,
            "no_lookahead": True,
        },
        "action_space": list(ACTION_SPACE),
        "current_candidate_name": current_candidate_name,
        "preserved_candidate_names": [
            FAILED_FOLLOWTHROUGH_CANDIDATE,
            HARD_INVALIDATION_CANDIDATE,
            BAD_REENTRY_AFTER_PROFIT_TAKE_CANDIDATE,
        ],
        "frozen_candidate_names": [
            PROFIT_TAKE_REENTRY_CANDIDATE,
        ],
        "stacked_candidate_names": list(STACKED_AXIS_CANDIDATES) if current_candidate_name in {STACK_CANDIDATE, HARD_INVALIDATION_SEVERITY_CANDIDATE} else [],
        "frozen_case_set": [_case_manifest_entry(case_spec) for case_spec in CASE_SPECS],
        "action_trace_schema": {
            "schema_version": TRACE_SCHEMA_VERSION,
            "fields": [
                "symbol",
                "date",
                "as_of",
                "session_id",
                "candidate_id",
                "trace_schema_version",
                "action_type",
                "action_reason",
                "action_source",
                "position_before",
                "position_after",
                "target_position_after",
                "buy_units_before",
                "sell_units_before",
                "buy_units_after",
                "sell_units_after",
                "net_units_before",
                "net_units_after",
                "gross_units_before",
                "gross_units_after",
                "close",
                "next_open_if_used",
                "mark_price",
                "position_market_value_before",
                "position_market_value_after",
                "realized_pnl_day",
                "unrealized_pnl_day",
                "cumulative_realized_pnl",
                "cumulative_unrealized_pnl",
                "equity_curve_value",
                "pnl_path_available",
                "data_source",
                "input_bar_date",
                "decision_uses_future_data",
                "trace_row_hash",
                "reason",
                "special_reason",
                "forced_exit_reason",
                "severity_level",
                "severity_action_type",
                "reduction_intensity",
                "severity_target_buy_units",
                "profit_protection_guard_applied",
                "profit_preservation_guard_applied",
                "profit_preservation_guard_reason",
                "evidence_for",
                "evidence_against",
                "risk_warning",
                "confidence",
                "active_thesis",
                "no_lookahead_assertion",
            ]
        },
        "position_constraints": {
            "notation": "X-Y",
            "sell_units": "X",
            "buy_units": "Y",
            "hedge_units_may_not_exceed_long_units": True,
            "failed_followthrough_window_days": {
                "min_days": FOLLOWTHROUGH_WINDOW_MIN_DAYS,
                "max_days": FOLLOWTHROUGH_WINDOW_MAX_DAYS,
            },
            "long_full_units": FULL_LONG_UNITS,
            "confirm_units": CONFIRMED_LONG_UNITS,
        },
        "critic_error_labels": list(ERROR_LABELS),
        "rule_patch_schema": {
            "candidate_name": current_candidate_name,
            "preserved_candidate_names": [
                FAILED_FOLLOWTHROUGH_CANDIDATE,
                HARD_INVALIDATION_CANDIDATE,
                BAD_REENTRY_AFTER_PROFIT_TAKE_CANDIDATE,
            ],
            "frozen_candidate_names": [
                PROFIT_TAKE_REENTRY_CANDIDATE,
            ],
            "intent": (
                "combine the three kept axes with explicit precedence and preserve all prior gains"
                if current_candidate_name in {STACK_CANDIDATE, HARD_INVALIDATION_SEVERITY_CANDIDATE}
                else (
                    "soften hard invalidation on profitable continuation states without changing loss-side protection"
                    if current_candidate_name == HARD_INVALIDATION_PROFIT_PRESERVATION_GUARD_CANDIDATE
                    else (
                "block bad re-entry after profit-taking when the rebound lacks confirmation"
                if current_candidate_name == BAD_REENTRY_AFTER_PROFIT_TAKE_CANDIDATE
                else (
                    "trim after extended profit and re-enter only when the pullback revalidates the trend"
                    if current_candidate_name == PROFIT_TAKE_REENTRY_CANDIDATE
                    else (
                        "allow early staged adds, then stop further adds only when the breakout attempt fails to follow through"
                        if current_candidate_name == FAILED_FOLLOWTHROUGH_CANDIDATE
                        else "reduce or exit only when hard invalidation appears"
                    )
                )
                    )
                )
            ),
            "narrowness": "single-axis only",
        },
        "artifact_names": [
            "tradex_iizuka_trade_learning_loop_replay_spec.json",
            "tradex_iizuka_trade_learning_loop_action_space.json",
            "tradex_iizuka_trade_learning_loop_state_rule.json",
            "tradex_iizuka_trade_learning_loop_position_constraints.json",
            "tradex_iizuka_trade_learning_loop_anchor_manifest.json",
            "tradex_iizuka_trade_learning_loop_systena_replay_result.json",
            "tradex_iizuka_trade_learning_loop_taiheiyo_replay_result.json",
            "tradex_iizuka_trade_learning_loop_2531_replay_result.json",
            "tradex_iizuka_trade_learning_loop_error_taxonomy.json",
            "tradex_iizuka_trade_learning_loop_error_report.json",
            "tradex_iizuka_trade_learning_loop_rule_patch_candidates.json",
            "tradex_iizuka_trade_learning_loop_before_after_patch_summary.json",
            "tradex_iizuka_trade_learning_loop_hard_invalidation_summary.json",
            "tradex_iizuka_trade_learning_loop_bad_reentry_summary.json",
            "tradex_iizuka_trade_learning_loop_decision.json",
        ],
        "stack_artifact_names": (
            [
                "tradex_iizuka_kept_axis_stack_spec.json",
                "tradex_iizuka_kept_axis_stack_manifest.json",
                "tradex_iizuka_kept_axis_stack_precedence.json",
                "tradex_iizuka_kept_axis_stack_anchor_results.json",
                "tradex_iizuka_kept_axis_stack_interaction_summary.json",
                "tradex_iizuka_kept_axis_stack_error_report.json",
                "tradex_iizuka_kept_axis_stack_decision.json",
            ]
            if current_candidate_name in {STACK_CANDIDATE, HARD_INVALIDATION_SEVERITY_CANDIDATE}
            else []
        ),
        "source_db_path": str(source_db_path),
        "output_root": str(output_root),
        "generated_at": _utc_now(),
    }


def _build_action_space() -> dict[str, Any]:
    return {
        "schema_version": "tradex_iizuka_trade_learning_loop_action_space_v1",
        "actions": [
            {"action_type": "watch", "meaning": "observe only; no position change"},
            {"action_type": "trial_buy", "meaning": "initial long probe from flat"},
            {"action_type": "long_add", "meaning": "increase long size"},
            {"action_type": "hedge_add", "meaning": "increase hedge / short-side protection"},
            {"action_type": "long_reduce", "meaning": "trim long size"},
            {"action_type": "hedge_reduce", "meaning": "reduce hedge / short-side protection"},
            {"action_type": "stop_add", "meaning": "block more long adds after a failed follow-through"},
            {"action_type": "exit_all", "meaning": "flatten the position"},
        ],
    }


def _build_state_rule(*, current_candidate_name: str) -> dict[str, Any]:
    stack_mode = current_candidate_name in {STACK_CANDIDATE, HARD_INVALIDATION_SEVERITY_CANDIDATE}
    state_rule = {
        "schema_version": "tradex_iizuka_trade_learning_loop_state_rule_v1",
        "confirmation_rule": {
            "line_escape_confirmed_when": [
                "breakout10",
                "breakout5 and bull_stack and dist_ma20_pct >= 0.05",
            ],
            "follow_through_required_for_full_size": True,
        },
        "failed_followthrough_time_stop_v1": {
            "active": True,
            "window_days": {
                "min": FOLLOWTHROUGH_WINDOW_MIN_DAYS,
                "max": FOLLOWTHROUGH_WINDOW_MAX_DAYS,
            },
            "applies_to": "long accumulation only after a failed breakout window",
            "preserves": ["trial_buy probe", "valid pullback", "reacceleration after confirmation"],
        },
        "hard_invalidation_exit_v1": {
            "active": True,
            "trigger_conditions": [
                "close below MA20 with bearish stack after bullish thesis",
                "close below MA60 after prior bullish setup",
                "breakout candle midpoint lost with bearish continuation",
                "signal candle low lost after warning",
                "failed rebound after hard invalidation warning",
            ],
            "action_effect": [
                "force long_reduce or exit_all",
                "prohibit further long_add",
                "allow hedge_add",
            ],
            "preserves": ["valid pullback / support retest", "line escape winner state when support holds"],
        },
        "hard_invalidation_exit_severity_v2_loss_side_override": {
            "active": current_candidate_name == HARD_INVALIDATION_SEVERITY_CANDIDATE,
            "severity_levels": ["exit_all", "long_reduce", "partial_exit"],
            "severity_policy": [
                "exit_all only when the setup is already loss-side or decisively broken",
                "long_reduce when hard invalidation appears but the path is not fully dead",
                "partial_exit is encoded as long_reduce with reduction intensity metadata",
            ],
            "action_effect": [
                "force long_reduce or exit_all depending on severity",
                "prohibit further long_add while the severity gate is active",
                "allow hedge_add",
            ],
            "preserves": ["valid pullback / support retest", "line escape winner state when support holds"],
        },
        "hard_invalidation_profit_preservation_guard_v1": {
            "active": current_candidate_name == HARD_INVALIDATION_PROFIT_PRESERVATION_GUARD_CANDIDATE,
            "guard_policy": [
                "soften exit_all to long_reduce when the path remains profitable or near-profitable",
                "keep exit_all on decisive breakdowns",
                "preserve the same trigger-date evidence contract as v2",
            ],
            "guard_signals": [
                "path is profitable or near-flat at the trigger date",
                "higher-timeframe or regime context remains constructive",
                "price still respects support or reclaim structure",
                "breakdown remains weak relative to the v2 exit-all path",
            ],
            "action_effect": [
                "force long_reduce instead of exit_all when guard conditions are met",
                "preserve loss-side hard invalidation exits on decisive breakdowns",
                "allow hedge_add",
            ],
            "preserves": ["valid pullback / support retest", "line escape winner state when support holds"],
        },
        "hard_invalidation_non_exit_late_extension_hedge_v1": {
            "active": current_candidate_name == HARD_INVALIDATION_NON_EXIT_LATE_EXTENSION_HEDGE_CANDIDATE,
            "guard_policy": [
                "replace long_reduce with hedge_add only when the late-extension path remains constructive",
                "keep exit_all behavior unchanged",
                "require the same trigger-date evidence contract as the repaired baseline",
            ],
            "guard_signals": [
                "current hard-invalidation path would otherwise trim to long_reduce",
                "bull_stack remains true at the trigger date",
                "close is above MA20 and MA60",
                "dist_ma20_pct is at least 0.05",
                "no existing hedge exposure is open",
            ],
            "action_effect": [
                "force hedge_add instead of long_reduce when the late-extension hedge condition is met",
                "leave exit_all decisions unchanged",
                "allow hedge_add",
            ],
            "preserves": ["valid pullback / support retest", "line escape winner state when support holds"],
        },
        "profit_take_reentry_delay_v1": {
            "active": True,
            "profit_take_conditions": [
                "large favorable move after entry",
                "distance from MA20 extended",
                "upper wick and exhaustion after extended move",
                "daily reversal-up candidate with monthly top warning",
            ],
            "reentry_conditions": [
                "profit already taken",
                "pullback after profit take with support holding",
                "reclaim of MA60 or MA20 support",
                "no hard invalidation or failed follow-through",
            ],
            "action_effect": [
                "allow long_reduce for profit taking",
                "allow trial_buy or long_add for re-entry",
                "preserve valid pullback and support retest",
            ],
        },
        "bad_reentry_after_profit_take_v1": {
            "active": True,
            "block_conditions": [
                "profit-taking already happened after extension or exhaustion",
                "rebound lacks follow-through or MA reclaim",
                "support retest is weak or ambiguous",
                "next 3-5 days do not confirm the rebound",
            ],
            "action_effect": [
                "block trial_buy or long_add re-entry attempts",
                "emit stop_add when a weak re-entry attempt appears",
                "preserve valid re-entry when a strong reclaim appears",
            ],
            "preserves": ["valid 5541-style re-entry", "winner pattern when support and follow-through revalidate"],
        },
        "no_lookahead": {
            "actor_inputs": [
                "current row only",
                "prior state only",
            ],
            "critic_inputs": [
                "replay outcomes after the run",
            ],
        },
    }
    if stack_mode:
        state_rule["iizuka_kept_axis_stack_v1"] = {
            "active": True,
            "stacked_axes": list(STACKED_AXIS_CANDIDATES),
            "precedence": [
                HARD_INVALIDATION_CANDIDATE,
                FAILED_FOLLOWTHROUGH_CANDIDATE,
                BAD_REENTRY_AFTER_PROFIT_TAKE_CANDIDATE,
            ],
            "interaction_policy": [
                "hard invalidation dominates when support breaks decisively",
                "failed follow-through stops new adds after a failed breakout attempt",
                "bad re-entry blocks weak post-profit re-entries without suppressing valid winners",
            ],
        }
    return state_rule


def _build_position_constraints() -> dict[str, Any]:
    return {
        "schema_version": "tradex_iizuka_trade_learning_loop_position_constraints_v1",
        "notation": "X-Y",
        "sell_units": "X",
        "buy_units": "Y",
        "long_full_units": FULL_LONG_UNITS,
        "trial_buy_units": ENTRY_LONG_UNITS,
        "confirmed_long_units": CONFIRMED_LONG_UNITS,
        "light_hedge_units": LIGHT_HEDGE_UNITS,
        "heavy_hedge_units": HEAVY_HEDGE_UNITS,
        "failed_followthrough_window_days": {
            "min": FOLLOWTHROUGH_WINDOW_MIN_DAYS,
            "max": FOLLOWTHROUGH_WINDOW_MAX_DAYS,
        },
        "profit_take_reentry_window_days": {
            "min": 2,
            "max": 5,
        },
        "bad_reentry_block_window_days": {
            "min": 2,
            "max": 5,
        },
        "hedge_must_not_exceed_long": True,
        "pure_short_entries_disallowed": True,
    }


def run_learning_loop(
    *,
    source_db_path: Path = DEFAULT_SOURCE_DB_PATH,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    mirror_repo_inventory: bool = False,
    candidate_name: str = BAD_REENTRY_AFTER_PROFIT_TAKE_CANDIDATE,
) -> dict[str, Any]:
    run_stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    session_root = output_root / run_stamp
    session_root.mkdir(parents=True, exist_ok=True)

    replay_spec = _build_replay_spec(source_db_path=source_db_path, output_root=session_root, current_candidate_name=candidate_name)
    action_space = _build_action_space()
    state_rule = _build_state_rule(current_candidate_name=candidate_name)
    position_constraints = _build_position_constraints()
    error_taxonomy = _build_error_taxonomy()

    case_artifacts: list[dict[str, Any]] = []
    baseline_results: dict[str, dict[str, Any]] = {}
    corrected_results: dict[str, dict[str, Any]] = {}
    baseline_labels: dict[str, list[str]] = {}
    corrected_labels: dict[str, list[str]] = {}

    for case_spec in CASE_SPECS:
        baseline = _run_case(case_spec=case_spec, source_db_path=source_db_path, mode="baseline", candidate_name=candidate_name, session_id=run_stamp)
        corrected = _run_case(case_spec=case_spec, source_db_path=source_db_path, mode="corrected", candidate_name=candidate_name, session_id=run_stamp)
        baseline_labels[case_spec["symbol"]] = _classify_case(baseline, mode="baseline", candidate_name=candidate_name)["labels"]
        corrected_labels[case_spec["symbol"]] = _classify_case(corrected, mode="corrected", candidate_name=candidate_name)["labels"]

        baseline = {**baseline, "labels": baseline_labels[case_spec["symbol"]]}
        corrected = {**corrected, "labels": corrected_labels[case_spec["symbol"]]}
        baseline_results[case_spec["symbol"]] = baseline
        corrected_results[case_spec["symbol"]] = corrected
        case_artifacts.append(
            _build_case_result_artifact(
                baseline=baseline,
                corrected=corrected,
                mode="baseline_vs_corrected",
                output_root=session_root,
            )
        )

    case_comparisons = [artifact["compare"] for artifact in case_artifacts]
    systena_compare = next(item for item in case_comparisons if item["symbol"] == "2317")
    taiheiyo_compare = next(item for item in case_comparisons if item["symbol"] == "5541")
    invalidation_anchor_compare = next(item for item in case_comparisons if item["symbol"] == "9697")
    bad_reentry_anchor_compare = next(item for item in case_comparisons if item["symbol"] == "2531")
    bad_reentry_case_artifact = next(artifact for artifact in case_artifacts if artifact["case"]["symbol"] == "2531")

    invalidation_anchor_baseline_loss = float(invalidation_anchor_compare["comparison"]["estimated_loss_before_correction"])
    invalidation_anchor_corrected_loss = float(invalidation_anchor_compare["comparison"]["estimated_loss_after_correction"])
    invalidation_anchor_loss_delta = float(invalidation_anchor_corrected_loss - invalidation_anchor_baseline_loss)
    invalidation_anchor_improved = bool(invalidation_anchor_compare["comparison"]["damage_reduced"])
    invalidation_anchor_inconclusive = bool(
        not invalidation_anchor_improved
        and invalidation_anchor_loss_delta <= max(1.0, invalidation_anchor_baseline_loss * 0.05)
    )
    bad_reentry_anchor_baseline_loss = float(bad_reentry_anchor_compare["comparison"]["estimated_loss_before_correction"])
    bad_reentry_anchor_corrected_loss = float(bad_reentry_anchor_compare["comparison"]["estimated_loss_after_correction"])
    bad_reentry_anchor_loss_delta = float(bad_reentry_anchor_corrected_loss - bad_reentry_anchor_baseline_loss)
    bad_reentry_checks = bad_reentry_anchor_compare["comparison"].get("bad_reentry_checks") or {}
    bad_reentry_anchor_improved = bool(bad_reentry_anchor_compare["comparison"]["damage_reduced"])
    bad_reentry_anchor_risk_reduced = bool(bad_reentry_checks.get("reentry_risk_reduced"))
    bad_reentry_anchor_false_block = bool(bad_reentry_checks.get("false_block"))
    bad_reentry_anchor_inconclusive = bool(
        not bad_reentry_anchor_improved
        and not bad_reentry_anchor_risk_reduced
        and bad_reentry_anchor_loss_delta <= max(1.0, bad_reentry_anchor_baseline_loss * 0.05)
    )

    decision_reason = "insufficient_evidence"
    decision = "hold"
    if taiheiyo_compare["comparison"]["material_profit_damage"] or taiheiyo_compare["comparison"]["false_winner_block"]:
        decision = "drop"
        decision_reason = "The correction blocked the winner or materially damaged Taiheiyo"
    elif systena_compare["comparison"]["material_profit_damage"]:
        decision = "drop"
        decision_reason = "Systena was materially worsened"
    elif invalidation_anchor_compare["comparison"]["material_profit_damage"]:
        decision = "drop"
        decision_reason = "The hard invalidation control was materially worsened"
    elif bad_reentry_anchor_false_block:
        decision = "drop"
        decision_reason = "The bad re-entry control falsely blocked a valid winner path"
    elif bad_reentry_anchor_improved or bad_reentry_anchor_risk_reduced:
        decision = "keep"
        decision_reason = "Systena, hard invalidation, and Taiheiyo controls passed, and the bad re-entry anchor improved or risk was reduced"
    elif bad_reentry_anchor_inconclusive:
        decision = "hold"
        decision_reason = "Controls passed, but the bad re-entry anchor evidence is inconclusive"
    else:
        decision = "drop"
        decision_reason = "The bad re-entry control did not improve the negative anchor"

    error_report = {
        "schema_version": "tradex_iizuka_trade_learning_loop_error_report_v1",
        "case_reports": [
            {
                "symbol": artifact["case"]["symbol"],
                "name": artifact["case"]["name"],
                "role": artifact["case"]["role"],
                "baseline_labels": artifact["baseline"]["labels"],
                "corrected_labels": artifact["corrected"]["labels"],
                "baseline_summary": artifact["baseline"]["summary"],
                "corrected_summary": artifact["corrected"]["summary"],
                "compare": artifact["compare"],
            }
            for artifact in case_artifacts
        ],
    }

    rule_patch_candidates = {
        "schema_version": "tradex_iizuka_trade_learning_loop_rule_patch_candidates_v1",
        "candidates": [
            {
                "candidate_name": FAILED_FOLLOWTHROUGH_CANDIDATE,
                "status": "preserved",
                "single_axis": "failed breakout / line escape time stop",
                "window_days": {
                    "min": FOLLOWTHROUGH_WINDOW_MIN_DAYS,
                    "max": FOLLOWTHROUGH_WINDOW_MAX_DAYS,
                },
                "intent": "allow early staged adds, then stop further adds only when the breakout attempt fails to follow through",
                "applies_to": ["long_add", "trial_buy"],
                "does_not_change": [
                    "ranking",
                    "MeeMee UI",
                    "live trading",
                    "publish flows",
                    "broad parameter sweep",
                ],
            },
            {
                "candidate_name": HARD_INVALIDATION_CANDIDATE,
                "status": "preserved",
                "single_axis": "hard invalidation exit",
                "intent": "reduce or exit when support breaks decisively and the bullish thesis is invalidated",
                "applies_to": ["long_reduce", "exit_all"],
                "does_not_change": [
                    "ranking",
                    "MeeMee UI",
                    "live trading",
                    "publish flows",
                    "broad parameter sweep",
                ],
            },
            {
                "candidate_name": HARD_INVALIDATION_SEVERITY_CANDIDATE,
                "status": "current_candidate",
                "single_axis": "hard invalidation exit severity",
                "intent": "tier hard invalidation into exit_all, long_reduce, and partial_exit metadata so profitable baselines are reduced rather than flattened",
                "applies_to": ["long_reduce", "exit_all"],
                "does_not_change": [
                    "ranking",
                    "MeeMee UI",
                    "live trading",
                    "publish flows",
                    "broad parameter sweep",
                ],
            },
            {
                "candidate_name": PROFIT_TAKE_REENTRY_CANDIDATE,
                "status": "frozen_drop",
                "single_axis": "profit-taking and delayed re-entry timing",
                "intent": "trim after extended profit, then re-enter only when the pullback revalidates the trend",
                "applies_to": ["long_reduce", "trial_buy", "long_add"],
                "does_not_change": [
                    "ranking",
                    "MeeMee UI",
                    "live trading",
                    "publish flows",
                    "broad parameter sweep",
                ],
            },
            {
                "candidate_name": BAD_REENTRY_AFTER_PROFIT_TAKE_CANDIDATE,
                "status": "current_candidate",
                "single_axis": "bad re-entry after profit taking",
                "intent": "block re-entry after profit-taking when the rebound lacks confirmation",
                "applies_to": ["stop_add", "trial_buy", "long_add"],
                "does_not_change": [
                    "ranking",
                    "MeeMee UI",
                    "live trading",
                    "publish flows",
                    "broad parameter sweep",
                ],
            },
            {
                "candidate_name": STACK_CANDIDATE,
                "status": "stack_validation",
                "single_axis": "kept-axis stack integration",
                "intent": "combine the three kept axes under explicit precedence without regressing any control",
                "applies_to": [
                    "stop_add",
                    "trial_buy",
                    "long_add",
                    "long_reduce",
                    "exit_all",
                ],
                "does_not_change": [
                    "ranking",
                    "MeeMee UI",
                    "live trading",
                    "publish flows",
                    "broad parameter sweep",
                ],
            }
        ],
    }

    before_after_patch_summary = {
        "schema_version": "tradex_iizuka_trade_learning_loop_before_after_patch_summary_v1",
        "summary_rows": case_comparisons,
        "bad_reentry_anchor_compare": bad_reentry_anchor_compare,
    }

    hard_invalidation_summary = {
        "schema_version": "tradex_iizuka_trade_learning_loop_hard_invalidation_summary_v1",
        "candidate_name": HARD_INVALIDATION_CANDIDATE,
        "systena": systena_compare,
        "taiheiyo": taiheiyo_compare,
        "hard_invalidation_anchor": invalidation_anchor_compare,
        "decision": decision,
        "decision_reason": decision_reason,
        "generated_at": _utc_now(),
    }

    bad_reentry_summary = {
        "schema_version": "tradex_iizuka_trade_learning_loop_bad_reentry_summary_v1",
        "candidate_name": BAD_REENTRY_AFTER_PROFIT_TAKE_CANDIDATE,
        "systena_control": systena_compare,
        "hard_invalidation_control": invalidation_anchor_compare,
        "taiheiyo_control": taiheiyo_compare,
        "bad_reentry_anchor": bad_reentry_anchor_compare,
        "event_surface": {
            "first_bad_reentry_candidate_date": bad_reentry_case_artifact["corrected"]["summary"].get("first_bad_reentry_candidate_date"),
            "first_reentry_block_date": bad_reentry_case_artifact["corrected"]["summary"].get("first_reentry_block_date"),
            "reentry_blocked": bad_reentry_case_artifact["corrected"]["summary"].get("first_reentry_block_date"),
            "bad_reentry_stop_add": bad_reentry_case_artifact["corrected"]["summary"].get("first_add_stop_date"),
            "event_log": list(bad_reentry_case_artifact["corrected"]["summary"].get("bad_reentry_event_log") or []),
        },
        "decision": decision,
        "decision_reason": decision_reason,
        "generated_at": _utc_now(),
    }

    decision_artifact = {
        "schema_version": "tradex_iizuka_trade_learning_loop_decision_v1",
        "decision": decision,
        "decision_reason": decision_reason,
        "candidate_local_decision": decision,
        "authoritative_rollup_decision": decision,
        "session_aggregate_decision": decision,
        "result_basis": {
            "systena": systena_compare,
            "taiheiyo": taiheiyo_compare,
            "hard_invalidation_anchor": invalidation_anchor_compare,
            "bad_reentry_anchor": bad_reentry_anchor_compare,
        },
        "generated_at": _utc_now(),
    }

    stack_artifacts: dict[str, Any] = {}
    if candidate_name == STACK_CANDIDATE:
        stack_artifacts = _build_kept_axis_stack_artifacts(
            case_artifacts=case_artifacts,
            case_comparisons=case_comparisons,
            decision=decision,
            decision_reason=decision_reason,
        )

    artifacts = {
        "replay_spec": _write_json(session_root / "tradex_iizuka_trade_learning_loop_replay_spec.json", replay_spec),
        "action_space": _write_json(session_root / "tradex_iizuka_trade_learning_loop_action_space.json", action_space),
        "state_rule": _write_json(session_root / "tradex_iizuka_trade_learning_loop_state_rule.json", state_rule),
        "position_constraints": _write_json(session_root / "tradex_iizuka_trade_learning_loop_position_constraints.json", position_constraints),
        "systena_replay_result": _write_json(
            session_root / "tradex_iizuka_trade_learning_loop_systena_replay_result.json",
            next(artifact for artifact in case_artifacts if artifact["case"]["symbol"] == "2317"),
        ),
        "taiheiyo_replay_result": _write_json(
            session_root / "tradex_iizuka_trade_learning_loop_taiheiyo_replay_result.json",
            next(artifact for artifact in case_artifacts if artifact["case"]["symbol"] == "5541"),
        ),
        "2531_replay_result": _write_json(
            session_root / "tradex_iizuka_trade_learning_loop_2531_replay_result.json",
            next(artifact for artifact in case_artifacts if artifact["case"]["symbol"] == "2531"),
        ),
        "error_taxonomy": _write_json(session_root / "tradex_iizuka_trade_learning_loop_error_taxonomy.json", error_taxonomy),
        "error_report": _write_json(session_root / "tradex_iizuka_trade_learning_loop_error_report.json", error_report),
        "rule_patch_candidates": _write_json(session_root / "tradex_iizuka_trade_learning_loop_rule_patch_candidates.json", rule_patch_candidates),
        "before_after_patch_summary": _write_json(session_root / "tradex_iizuka_trade_learning_loop_before_after_patch_summary.json", before_after_patch_summary),
        "anchor_manifest": _write_json(
            session_root / "tradex_iizuka_trade_learning_loop_anchor_manifest.json",
            {
                "schema_version": "tradex_iizuka_trade_learning_loop_anchor_manifest_v1",
                "candidate_name": candidate_name,
                "preserved_candidate_names": [
                    FAILED_FOLLOWTHROUGH_CANDIDATE,
                    HARD_INVALIDATION_CANDIDATE,
                ],
                "frozen_candidate_names": [
                    PROFIT_TAKE_REENTRY_CANDIDATE,
                ],
                "controls": {
                    "failure_controls": [
                        _case_manifest_entry(case_spec)
                        for case_spec in CASE_SPECS
                        if case_spec["role"] == "failure_control"
                    ],
                    "success_controls": [
                        _case_manifest_entry(case_spec)
                        for case_spec in CASE_SPECS
                        if case_spec["role"] == "success_control"
                    ],
                },
                "cases": [_case_manifest_entry(case_spec) for case_spec in CASE_SPECS],
                "generated_at": _utc_now(),
            },
        ),
        "hard_invalidation_summary": _write_json(session_root / "tradex_iizuka_trade_learning_loop_hard_invalidation_summary.json", hard_invalidation_summary),
        "bad_reentry_summary": _write_json(session_root / "tradex_iizuka_trade_learning_loop_bad_reentry_summary.json", bad_reentry_summary),
        "decision": _write_json(session_root / "tradex_iizuka_trade_learning_loop_decision.json", decision_artifact),
    }

    if stack_artifacts:
        artifacts.update(
            {
                "stack_spec": _write_json(session_root / "tradex_iizuka_kept_axis_stack_spec.json", stack_artifacts["spec"]),
                "stack_manifest": _write_json(session_root / "tradex_iizuka_kept_axis_stack_manifest.json", stack_artifacts["manifest"]),
                "stack_precedence": _write_json(session_root / "tradex_iizuka_kept_axis_stack_precedence.json", stack_artifacts["precedence"]),
                "stack_anchor_results": _write_json(session_root / "tradex_iizuka_kept_axis_stack_anchor_results.json", stack_artifacts["anchor_results"]),
                "stack_interaction_summary": _write_json(session_root / "tradex_iizuka_kept_axis_stack_interaction_summary.json", stack_artifacts["interaction_summary"]),
                "stack_error_report": _write_json(session_root / "tradex_iizuka_kept_axis_stack_error_report.json", stack_artifacts["error_report"]),
                "stack_decision": _write_json(session_root / "tradex_iizuka_kept_axis_stack_decision.json", stack_artifacts["decision"]),
            }
        )

    if mirror_repo_inventory:
        DEFAULT_REPO_MIRROR_ROOT.mkdir(parents=True, exist_ok=True)
        _write_json(DEFAULT_REPO_MIRROR_ROOT / "tradex_iizuka_trade_learning_loop_decision.json", decision_artifact)
        _write_json(DEFAULT_REPO_MIRROR_ROOT / "tradex_iizuka_trade_learning_loop_before_after_patch_summary.json", before_after_patch_summary)

    return {
        "session_root": str(session_root),
        "artifacts": {name: str(path) for name, path in artifacts.items()},
        "decision": decision_artifact,
        "case_comparisons": case_comparisons,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the TRADEX Iizuka trade learning loop v1.")
    parser.add_argument("--source-db-path", default=str(DEFAULT_SOURCE_DB_PATH))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--mirror-research-inventory", action="store_true")
    parser.add_argument(
        "--candidate-name",
        default=BAD_REENTRY_AFTER_PROFIT_TAKE_CANDIDATE,
        choices=[
            FAILED_FOLLOWTHROUGH_CANDIDATE,
            HARD_INVALIDATION_CANDIDATE,
            HARD_INVALIDATION_SEVERITY_CANDIDATE,
            HARD_INVALIDATION_PROFIT_PRESERVATION_GUARD_CANDIDATE,
            PROFIT_TAKE_REENTRY_CANDIDATE,
            BAD_REENTRY_AFTER_PROFIT_TAKE_CANDIDATE,
            STACK_CANDIDATE,
        ],
    )
    args = parser.parse_args(argv)
    payload = run_learning_loop(
        source_db_path=Path(args.source_db_path).expanduser().resolve(),
        output_root=Path(args.output_root).expanduser().resolve(),
        mirror_repo_inventory=bool(args.mirror_research_inventory),
        candidate_name=str(args.candidate_name),
    )
    print(_json_text(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
