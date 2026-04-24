from __future__ import annotations

import argparse
import json
import math
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.action_precision_multitimeframe_decomposition import _equivalent_context_for_row  # noqa: E402

DEFAULT_SOURCE_DB_PATH = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb")
DEFAULT_OUTPUT_DIR = Path(r"G:\Tradex\sample_replays\tradex_chart_first_5541")
DEFAULT_SYMBOL = "5541"
DEFAULT_START_DATE = "2025-10-10"
DEFAULT_END_DATE = "2026-01-30"
DEFAULT_FREEZE_DATE = "2025-10-09"
SHARES_PER_UNIT = 100
ENTRY_LONG_UNITS = 1
CONFIRMED_LONG_UNITS = 3
FULL_LONG_UNITS = 5
LIGHT_HEDGE_UNITS = 1
HEAVY_HEDGE_UNITS = 2
SHORT_ENTRY_UNITS = 2
SHORT_ADD_UNITS = 2
MAX_SHORT_UNITS = 4


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_missing(value: Any) -> bool:
    return value is None or value is pd.NA or (isinstance(value, float) and math.isnan(value)) or pd.isna(value)


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


def _parse_json_dict(value: Any) -> dict[str, Any]:
    if value is None or _is_missing(value):
        return {}
    if isinstance(value, dict):
        return dict(value)
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "<na>"}:
        return {}
    try:
        parsed = json.loads(text)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _position_text(sell_units: int, buy_units: int) -> str:
    return f"{int(sell_units)}-{int(buy_units)}"


def _sample_name(symbol: str) -> str:
    return f"tradex_chart_first_{symbol}"


def _policy_id(symbol: str) -> str:
    return f"tradex_chart_first_{symbol}_chart_policy_v1"


def _load_source_frames(
    *,
    source_db_path: Path,
    symbol: str,
    start_date: str,
    end_date: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    conn = duckdb.connect(str(source_db_path), read_only=True)
    try:
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
            ORDER BY dt
            """,
            [symbol],
        ).fetchdf()
        basis_frame = conn.execute(
            """
            SELECT
                dt,
                code,
                basis_version,
                name,
                source_rank_buy,
                source_rank_sell,
                basis_payload_json,
                source_as_of,
                pred_dt,
                model_version,
                basis_source,
                source_hash,
                payload_schema_version
            FROM signal_basis_daily
            WHERE code = ?
              AND dt BETWEEN ? AND ?
            ORDER BY dt
            """,
            [symbol, _date_text_to_ymd(start_date), _date_text_to_ymd(end_date)],
        ).fetchdf()
    finally:
        conn.close()

    if bars_frame.empty:
        raise RuntimeError(f"no daily_bars rows found for symbol={symbol}")
    if basis_frame.empty:
        raise RuntimeError(f"no signal_basis_daily rows found for symbol={symbol}")

    bars_frame = bars_frame.copy()
    bars_frame["dt"] = pd.to_numeric(bars_frame["dt"], errors="coerce").astype("Int64")
    basis_frame = basis_frame.copy()
    basis_frame["dt"] = pd.to_numeric(basis_frame["dt"], errors="coerce").astype("Int64")
    return bars_frame, basis_frame


def _rolling_streak(values: pd.Series, *, above: bool) -> pd.Series:
    streak = 0
    out: list[int] = []
    for value, ref in zip(values["c"].to_numpy(dtype=float, copy=False), values["ma20"].to_numpy(dtype=float, copy=False)):
        if not math.isfinite(value) or not math.isfinite(ref):
            streak = 0
        else:
            if (value > ref) if above else (value < ref):
                streak += 1
            else:
                streak = 0
        out.append(streak)
    return pd.Series(out, index=values.index, dtype="Int64")


def _prepare_chart_frame(bars_frame: pd.DataFrame, basis_frame: pd.DataFrame) -> pd.DataFrame:
    frame = bars_frame.copy().sort_values("dt").reset_index(drop=True)
    for col in ("o", "h", "l", "c", "v"):
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
    frame["ma20"] = frame["c"].rolling(20, min_periods=1).mean()
    frame["ma60"] = frame["c"].rolling(60, min_periods=1).mean()
    frame["prev_c"] = frame["c"].shift(1)
    frame["prev_o"] = frame["o"].shift(1)
    frame["prev_h"] = frame["h"].shift(1)
    frame["prev_l"] = frame["l"].shift(1)
    frame["prev_ma20"] = frame["ma20"].shift(1)
    frame["prev_ma60"] = frame["ma60"].shift(1)
    frame["prev_3_high"] = frame["h"].shift(1).rolling(3, min_periods=1).max()
    frame["prev_5_high"] = frame["h"].shift(1).rolling(5, min_periods=1).max()
    frame["prev_10_high"] = frame["h"].shift(1).rolling(10, min_periods=1).max()
    frame["prev_3_low"] = frame["l"].shift(1).rolling(3, min_periods=1).min()
    frame["prev_5_low"] = frame["l"].shift(1).rolling(5, min_periods=1).min()
    frame["prev_10_low"] = frame["l"].shift(1).rolling(10, min_periods=1).min()
    frame["range"] = (frame["h"] - frame["l"]).replace(0, pd.NA)
    frame["body"] = (frame["c"] - frame["o"]).abs()
    frame["body_ratio"] = frame["body"] / frame["range"]
    frame["upper_wick_ratio"] = (frame["h"] - frame[["o", "c"]].max(axis=1)) / frame["range"]
    frame["lower_wick_ratio"] = (frame[["o", "c"]].min(axis=1) - frame["l"]) / frame["range"]
    frame["gap_pct"] = (frame["o"] - frame["prev_c"]) / frame["prev_c"]
    frame["dist_ma20_pct"] = (frame["c"] - frame["ma20"]) / frame["ma20"]
    frame["dist_ma60_pct"] = (frame["c"] - frame["ma60"]) / frame["ma60"]
    frame["ma20_slope"] = frame["ma20"].diff()
    frame["ma60_slope"] = frame["ma60"].diff()
    frame["above_ma20_streak"] = _rolling_streak(frame, above=True)
    frame["below_ma20_streak"] = _rolling_streak(frame, above=False)
    frame["reclaim_ma20"] = (frame["prev_c"] <= frame["prev_ma20"]) & (frame["c"] > frame["ma20"])
    frame["lose_ma20"] = (frame["prev_c"] >= frame["prev_ma20"]) & (frame["c"] < frame["ma20"])
    frame["lose_ma60"] = (frame["prev_c"] >= frame["prev_ma60"]) & (frame["c"] < frame["ma60"])
    frame["breakout5"] = (frame["c"] > frame["prev_5_high"]) & (frame["c"] > frame["ma20"])
    frame["breakout10"] = (frame["c"] > frame["prev_10_high"]) & (frame["c"] > frame["ma20"])
    frame["breakdown5"] = (frame["c"] < frame["prev_5_low"]) & (frame["c"] < frame["ma20"])
    frame["failed_breakout5"] = (frame["h"] > frame["prev_5_high"]) & (frame["c"] <= frame["prev_5_high"])
    frame["bull_stack"] = (frame["c"] > frame["ma20"]) & (frame["ma20"] > frame["ma60"]) & (frame["ma20_slope"] > 0) & (frame["ma60_slope"] >= 0)
    frame["bear_stack"] = (frame["c"] < frame["ma20"]) & (frame["ma20"] < frame["ma60"]) & (frame["ma20_slope"] <= 0) & (frame["ma60_slope"] <= 0)
    frame["support_wick"] = (frame["lower_wick_ratio"] >= 0.20) & (frame["c"] >= frame["o"])
    frame["exhaustion"] = (frame["upper_wick_ratio"] >= 0.25) | (frame["body_ratio"] <= 0.45) | frame["failed_breakout5"]
    frame["candle_sequence_3"] = frame["c"].diff().rolling(3, min_periods=1).apply(
        lambda values: float(sum(1 for value in values if value > 0)) if len(values) else 0.0,
        raw=False,
    )
    frame = frame.merge(basis_frame, on=["dt", "code"], how="left", suffixes=("", "_basis"))
    basis_context_rows: list[dict[str, Any]] = []
    for _, row in frame.iterrows():
        basis_row = pd.Series(
            {
                "basis_payload": row.get("basis_payload_json"),
            }
        )
        context = _equivalent_context_for_row(basis_row)
        basis_context_rows.append(
            {
                "marketRegime": context.get("marketRegime"),
                "monthly_main_state_ctx": context.get("monthly_main_state_ctx"),
                "weekly_main_state_ctx": context.get("weekly_main_state_ctx"),
                "daily_main_state_ctx": context.get("daily_main_state_ctx"),
                "regime_tag": context.get("regime_tag"),
                "hierarchical_native_regime_tag": context.get("hierarchical_native_regime_tag"),
            }
        )
    basis_context = pd.DataFrame(basis_context_rows)
    basis_context["dt"] = frame["dt"]
    basis_context["code"] = frame["code"]
    frame = frame.merge(basis_context, on=["dt", "code"], how="left")
    frame["basis_payload"] = frame["basis_payload_json"].map(_parse_json_dict)
    return frame


def _higher_frame_bias(row: pd.Series) -> tuple[bool, bool]:
    monthly = str(row.get("monthly_main_state_ctx") or "unknown")
    weekly = str(row.get("weekly_main_state_ctx") or "unknown")
    daily = str(row.get("daily_main_state_ctx") or "unknown")
    bull = monthly in {"monthly_up_mid", "monthly_up_top_warning"} or weekly in {"weekly_up_early", "weekly_up_mid", "weekly_up_late"} or daily in {"daily_up_mid", "daily_reversal_up_candidate"}
    bear = monthly in {"monthly_down_mid", "monthly_down_bottom_warning"} or weekly in {"weekly_down_early", "weekly_down_mid", "weekly_down_bottom_warning"} or daily in {"daily_down_mid", "daily_down_bottom_warning"}
    return bull, bear


def _sequence_label(row: pd.Series) -> str:
    if not math.isfinite(_safe_float(row.get("prev_c"), float("nan"))):
        return "unknown"
    changes = []
    for key in ("prev_3_low", "prev_5_low", "prev_10_low"):
        changes.append("up" if _safe_float(row.get("c")) >= _safe_float(row.get(key)) else "down")
    return "-".join(changes)


def _reason_bundle(
    *,
    action: str,
    row: pd.Series,
    target_buy_units: int,
    target_sell_units: int,
    current_buy_units: int,
    current_sell_units: int,
    window_end: bool,
) -> dict[str, Any]:
    close = _safe_float(row.get("c"))
    ma20 = _safe_float(row.get("ma20"))
    ma60 = _safe_float(row.get("ma60"))
    dist_ma20_pct = _safe_float(row.get("dist_ma20_pct"))
    dist_ma60_pct = _safe_float(row.get("dist_ma60_pct"))
    upper_wick = _safe_float(row.get("upper_wick_ratio"))
    lower_wick = _safe_float(row.get("lower_wick_ratio"))
    body_ratio = _safe_float(row.get("body_ratio"))
    gap_pct = _safe_float(row.get("gap_pct"))
    sequence = _sequence_label(row)
    monthly = str(row.get("monthly_main_state_ctx") or "unknown")
    weekly = str(row.get("weekly_main_state_ctx") or "unknown")
    daily = str(row.get("daily_main_state_ctx") or "unknown")
    codes: list[str] = []
    primary = "no_trade_penalty_cleared"
    if action in {"long_entry", "long_add"}:
        if action == "long_entry" and bool(row.get("reclaim_ma20")):
            primary = "ma20_reclaim_body_close"
            codes.extend(["ma20_reclaim_body_close", "ma60_support"])
            if bool(row.get("support_wick")):
                codes.append("lower_wick_support")
        elif action == "long_entry":
            primary = "reentry_after_cooloff"
            codes.extend(["reentry_after_cooloff", "ma60_support"])
            if bool(row.get("support_wick")):
                codes.append("lower_wick_support")
        else:
            primary = "continuation_confirmed"
            codes.extend(["continuation_confirmed", "ma20_hold_after_reclaim", "ma_stack_support"])
            if bool(row.get("breakout5")) or bool(row.get("breakout10")):
                codes.append("gap_up_followthrough" if gap_pct > 0 else "continuation_confirmed")
        if monthly in {"monthly_up_mid", "monthly_up_top_warning"}:
            codes.append("monthly_up_mid")
        if weekly in {"weekly_bull", "weekly_up_early", "weekly_up_mid", "weekly_up_late"}:
            codes.append("weekly_bull_recovery")
        detail = (
            f"close={close:.2f} ma20={ma20:.2f} ma60={ma60:.2f} dist_ma20_pct={dist_ma20_pct:.4f} "
            f"body_ratio={body_ratio:.3f} lower_wick={lower_wick:.3f} upper_wick={upper_wick:.3f} "
            f"sequence={sequence} monthly={monthly} weekly={weekly} daily={daily}"
        )
    elif action in {"hedge_open", "hedge_add"}:
        primary = "late_extension_blocked" if (dist_ma20_pct >= 0.08 or bool(row.get("exhaustion"))) else "partial_take_on_extension"
        codes.extend(["late_extension_blocked", "trend_up_intact", "ma20_hold_after_reclaim"])
        if bool(row.get("exhaustion")):
            codes.append("small_body_continuation")
        detail = (
            f"hedge_units={target_sell_units} core_units={target_buy_units} "
            f"dist_ma20_pct={dist_ma20_pct:.4f} dist_ma60_pct={dist_ma60_pct:.4f} "
            f"upper_wick={upper_wick:.3f} lower_wick={lower_wick:.3f} body_ratio={body_ratio:.3f} "
            f"sequence={sequence} monthly={monthly} weekly={weekly} daily={daily}"
        )
    elif action in {"long_trim", "long_exit"}:
        if window_end:
            primary = "time_stop"
            codes.append("time_stop")
        elif bool(row.get("lose_ma60")):
            primary = "lose_ma60"
            codes.extend(["lose_ma60", "invalidated"])
        elif bool(row.get("lose_ma20")):
            primary = "lose_ma20"
            codes.extend(["lose_ma20", "invalidated"])
        else:
            primary = "invalidated"
            codes.append("invalidated")
        detail = (
            f"close={close:.2f} ma20={ma20:.2f} ma60={ma60:.2f} dist_ma20_pct={dist_ma20_pct:.4f} "
            f"sequence={sequence} monthly={monthly} weekly={weekly} daily={daily}"
        )
    elif action in {"short_entry", "short_add"}:
        if action == "short_entry" and bool(row.get("lose_ma60")):
            primary = "lose_ma60"
            codes.extend(["lose_ma60", "invalidated"])
        else:
            primary = "invalidated"
            codes.extend(["lose_ma20", "lose_ma60", "invalidated"])
        detail = (
            f"close={close:.2f} ma20={ma20:.2f} ma60={ma60:.2f} dist_ma20_pct={dist_ma20_pct:.4f} "
            f"upper_wick={upper_wick:.3f} body_ratio={body_ratio:.3f} sequence={sequence} monthly={monthly} weekly={weekly} daily={daily}"
        )
    elif action in {"short_cover", "hedge_close", "hedge_reduce"}:
        if bool(row.get("reclaim_ma20")) or close > ma20:
            primary = "ma20_reclaim_body_close"
            codes.extend(["ma20_reclaim_body_close", "weekly_bull_recovery"])
        else:
            primary = "reentry_after_cooloff"
            codes.append("reentry_after_cooloff")
        detail = (
            f"close={close:.2f} ma20={ma20:.2f} ma60={ma60:.2f} dist_ma20_pct={dist_ma20_pct:.4f} "
            f"sequence={sequence} monthly={monthly} weekly={weekly} daily={daily}"
        )
    else:
        if target_buy_units == current_buy_units and target_sell_units == current_sell_units:
            primary = "no_trade_penalty_cleared"
            codes.append("no_trade_penalty_cleared")
            if bool(row.get("exhaustion")) and target_buy_units > 0:
                codes.append("late_extension_blocked")
        else:
            primary = "reentry_after_cooloff"
            codes.append("reentry_after_cooloff")
        detail = (
            f"flat_on_close={close:.2f} ma20={ma20:.2f} ma60={ma60:.2f} dist_ma20_pct={dist_ma20_pct:.4f} "
            f"dist_ma60_pct={dist_ma60_pct:.4f} sequence={sequence} monthly={monthly} weekly={weekly} daily={daily}"
        )
    deduped_codes = list(dict.fromkeys([code for code in codes if code]))
    return {
        "primary": primary,
        "codes": deduped_codes,
        "detail": detail,
        "context": {
            "close": close,
            "ma20": ma20,
            "ma60": ma60,
            "dist_ma20_pct": dist_ma20_pct,
            "dist_ma60_pct": dist_ma60_pct,
            "gap_pct": gap_pct,
            "body_ratio": body_ratio,
            "upper_wick_ratio": upper_wick,
            "lower_wick_ratio": lower_wick,
            "sequence": sequence,
            "monthly_main_state_ctx": monthly,
            "weekly_main_state_ctx": weekly,
            "daily_main_state_ctx": daily,
        },
    }


@dataclass
class ChartState:
    buy_units: int = 0
    sell_units: int = 0
    avg_buy_price: float = 0.0
    avg_sell_price: float = 0.0
    realized_pnl: float = 0.0
    cycle_side: str = "flat"
    cycle_entry_decision_date: int | None = None
    cycle_entry_execution_date: int | None = None
    cycle_entry_fill_price: float | None = None
    cycle_entry_position_transition: str | None = None
    cycle_reason_summary: dict[str, Any] | None = None
    cycle_actions: list[str] | None = None
    cycle_action_details: list[dict[str, Any]] | None = None
    cycle_mark_to_market: list[float] | None = None
    cycle_realized_pnl: float = 0.0
    cycle_max_unrealized_pnl: float = 0.0
    cycle_min_unrealized_pnl: float = 0.0
    cycle_hedge_reason_summaries: list[dict[str, Any]] | None = None

    def __post_init__(self) -> None:
        if self.cycle_actions is None:
            self.cycle_actions = []
        if self.cycle_action_details is None:
            self.cycle_action_details = []
        if self.cycle_mark_to_market is None:
            self.cycle_mark_to_market = []
        if self.cycle_hedge_reason_summaries is None:
            self.cycle_hedge_reason_summaries = []


def _fill_long_entry(state: ChartState, *, units: int, price: float) -> None:
    if units <= 0:
        return
    total_cost = state.avg_buy_price * state.buy_units + price * units
    state.buy_units += units
    state.avg_buy_price = total_cost / max(state.buy_units, 1)


def _fill_long_exit(state: ChartState, *, units: int, price: float) -> None:
    if units <= 0:
        return
    units = min(units, state.buy_units)
    state.realized_pnl += (price - state.avg_buy_price) * units * SHARES_PER_UNIT
    state.buy_units -= units
    if state.buy_units <= 0:
        state.buy_units = 0
        state.avg_buy_price = 0.0


def _fill_short_entry(state: ChartState, *, units: int, price: float) -> None:
    if units <= 0:
        return
    total_credit = state.avg_sell_price * state.sell_units + price * units
    state.sell_units += units
    state.avg_sell_price = total_credit / max(state.sell_units, 1)


def _fill_short_cover(state: ChartState, *, units: int, price: float) -> None:
    if units <= 0:
        return
    units = min(units, state.sell_units)
    state.realized_pnl += (state.avg_sell_price - price) * units * SHARES_PER_UNIT
    state.sell_units -= units
    if state.sell_units <= 0:
        state.sell_units = 0
        state.avg_sell_price = 0.0


def _mark_unrealized(state: ChartState, *, close_price: float) -> float:
    long_mark = (close_price - state.avg_buy_price) * state.buy_units * SHARES_PER_UNIT if state.buy_units > 0 else 0.0
    short_mark = (state.avg_sell_price - close_price) * state.sell_units * SHARES_PER_UNIT if state.sell_units > 0 else 0.0
    return float(long_mark + short_mark)


def _hold_reason_bundle(row: pd.Series, *, window_end: bool) -> dict[str, Any]:
    close = _safe_float(row.get("c"))
    ma20 = _safe_float(row.get("ma20"))
    ma60 = _safe_float(row.get("ma60"))
    monthly = str(row.get("monthly_main_state_ctx") or "unknown")
    weekly = str(row.get("weekly_main_state_ctx") or "unknown")
    daily = str(row.get("daily_main_state_ctx") or "unknown")
    primary = "no_trade_penalty_cleared"
    codes = ["no_trade_penalty_cleared"]
    if window_end:
        primary = "time_stop"
        codes = ["time_stop"]
    elif bool(row.get("exhaustion")):
        primary = "late_extension_blocked"
        codes = ["late_extension_blocked", "trend_up_intact"]
    detail = (
        f"close={close:.2f} ma20={ma20:.2f} ma60={ma60:.2f} "
        f"monthly={monthly} weekly={weekly} daily={daily} "
        f"dist_ma20_pct={_safe_float(row.get('dist_ma20_pct')):.4f} sequence={_sequence_label(row)}"
    )
    return {"primary": primary, "codes": codes, "detail": detail}


def _desired_targets(state: ChartState, row: pd.Series, *, end_date: str) -> tuple[int, int, dict[str, Any]]:
    decision_dt = int(row.get("dt"))
    is_window_end = _ymd_to_date_text(decision_dt) == end_date
    close = _safe_float(row.get("c"))
    ma20 = _safe_float(row.get("ma20"))
    ma60 = _safe_float(row.get("ma60"))
    dist_ma20_pct = _safe_float(row.get("dist_ma20_pct"))
    upper_wick = _safe_float(row.get("upper_wick_ratio"))
    lower_wick = _safe_float(row.get("lower_wick_ratio"))
    bull_bias, bear_bias = _higher_frame_bias(row)
    reclaim = bool(row.get("reclaim_ma20"))
    lose20 = bool(row.get("lose_ma20"))
    lose60 = bool(row.get("lose_ma60"))
    breakout5 = bool(row.get("breakout5"))
    breakout10 = bool(row.get("breakout10"))
    breakdown5 = bool(row.get("breakdown5"))
    exhaustion = bool(row.get("exhaustion"))
    support_wick = bool(row.get("support_wick"))
    bull_stack = bool(row.get("bull_stack"))
    bear_stack = bool(row.get("bear_stack"))
    daily = str(row.get("daily_main_state_ctx") or "unknown")

    target_buy = state.buy_units
    target_sell = state.sell_units
    reason_map: dict[str, Any] = {}

    if is_window_end and (state.buy_units > 0 or state.sell_units > 0):
        target_buy = 0
        target_sell = 0
        reason_map["exit"] = {
            "primary": "time_stop",
            "codes": ["time_stop"],
            "detail": f"window_end={end_date} forced_close_of_open_position",
        }
        return target_buy, target_sell, reason_map

    if state.buy_units == 0 and state.sell_units == 0:
        if reclaim and (bull_stack or support_wick or bull_bias):
            target_buy = 1 if close < ma20 else 2
            reason_map["entry"] = {
                "primary": "ma20_reclaim_body_close" if close > ma20 else "reentry_after_cooloff",
                "codes": ["ma20_reclaim_body_close", "ma60_support", "lower_wick_support"] if close > ma20 else ["reentry_after_cooloff", "ma60_support", "lower_wick_support"],
                "detail": f"probe_long close={close:.2f} ma20={ma20:.2f} ma60={ma60:.2f} lower_wick={lower_wick:.3f} monthly={row.get('monthly_main_state_ctx')} weekly={row.get('weekly_main_state_ctx')} daily={daily}",
            }
        elif bear_stack and (lose60 or breakdown5 or bear_bias):
            target_sell = SHORT_ENTRY_UNITS
            reason_map["entry"] = {
                "primary": "lose_ma60" if lose60 else "invalidated",
                "codes": ["lose_ma60", "lose_ma20", "invalidated"],
                "detail": f"probe_short close={close:.2f} ma20={ma20:.2f} ma60={ma60:.2f} breakdown5={breakdown5} monthly={row.get('monthly_main_state_ctx')} weekly={row.get('weekly_main_state_ctx')} daily={daily}",
            }
        elif support_wick and bull_bias and close > ma60:
            target_buy = 1
            reason_map["entry"] = {
                "primary": "reentry_after_cooloff",
                "codes": ["reentry_after_cooloff", "ma60_support", "lower_wick_support"],
                "detail": f"support_probe close={close:.2f} ma20={ma20:.2f} ma60={ma60:.2f} body_ratio={_safe_float(row.get('body_ratio')):.3f}",
            }
        else:
            reason_map["flat"] = _hold_reason_bundle(row, window_end=False)
        return target_buy, target_sell, reason_map

    if state.buy_units > 0:
        if lose60 or (lose20 and bear_stack):
            target_buy = 0
            target_sell = 0
            reason_map["exit"] = {
                "primary": "lose_ma60" if lose60 else "lose_ma20",
                "codes": ["lose_ma60", "invalidated"] if lose60 else ["lose_ma20", "invalidated"],
                "detail": f"long_exit close={close:.2f} ma20={ma20:.2f} ma60={ma60:.2f} dist_ma20_pct={dist_ma20_pct:.4f} daily={daily}",
            }
            return target_buy, target_sell, reason_map

        if lose20 and not bull_stack:
            target_buy = max(ENTRY_LONG_UNITS, state.buy_units - 2)
            reason_map["trim"] = {
                "primary": "lose_ma20",
                "codes": ["lose_ma20", "ma20_hold_after_reclaim"],
                "detail": f"trim_long close={close:.2f} ma20={ma20:.2f} ma60={ma60:.2f} dist_ma20_pct={dist_ma20_pct:.4f}",
            }
        elif breakout10 or (breakout5 and bull_stack and state.buy_units < FULL_LONG_UNITS):
            target_buy = FULL_LONG_UNITS if breakout10 or dist_ma20_pct >= 0.08 else min(FULL_LONG_UNITS, state.buy_units + CONFIRMED_LONG_UNITS)
            reason_map["add"] = {
                "primary": "continuation_confirmed",
                "codes": ["continuation_confirmed", "ma20_hold_after_reclaim", "ma_stack_support", "gap_up_followthrough" if _safe_float(row.get("gap_pct")) > 0 else "continuation_confirmed"],
                "detail": f"long_add close={close:.2f} ma20={ma20:.2f} ma60={ma60:.2f} breakout5={breakout5} breakout10={breakout10} sequence={_sequence_label(row)}",
            }

        hedge_target = 0
        if dist_ma20_pct >= 0.12 or (str(row.get("monthly_main_state_ctx")) == "monthly_up_top_warning" and dist_ma20_pct >= 0.08):
            hedge_target = HEAVY_HEDGE_UNITS if state.buy_units >= CONFIRMED_LONG_UNITS else LIGHT_HEDGE_UNITS
        elif dist_ma20_pct >= 0.06 and exhaustion:
            hedge_target = LIGHT_HEDGE_UNITS if state.buy_units <= CONFIRMED_LONG_UNITS else HEAVY_HEDGE_UNITS
        elif bool(row.get("failed_breakout5")) and dist_ma20_pct >= 0.04:
            hedge_target = LIGHT_HEDGE_UNITS
        target_sell = min(hedge_target, max(0, target_buy))
        if target_sell > 0 and "hedge" not in reason_map:
            reason_map["hedge"] = {
                "primary": "late_extension_blocked" if hedge_target >= HEAVY_HEDGE_UNITS else "partial_take_on_extension",
                "codes": ["late_extension_blocked", "trend_up_intact", "ma20_hold_after_reclaim", "small_body_continuation" if exhaustion else "continuation_confirmed"],
                "detail": f"hedge target={target_sell} close={close:.2f} ma20={ma20:.2f} ma60={ma60:.2f} dist_ma20_pct={dist_ma20_pct:.4f} upper_wick={upper_wick:.3f} lower_wick={lower_wick:.3f} body_ratio={_safe_float(row.get('body_ratio')):.3f}",
            }
        return target_buy, target_sell, reason_map

    # short active
    if reclaim and (bull_stack or support_wick or bull_bias):
        target_sell = 0
        reason_map["cover"] = {
            "primary": "ma20_reclaim_body_close",
            "codes": ["ma20_reclaim_body_close", "weekly_bull_recovery", "lower_wick_support"],
            "detail": f"short_cover close={close:.2f} ma20={ma20:.2f} ma60={ma60:.2f} daily={daily}",
        }
        if close > ma20 and bull_stack:
            target_buy = 1 if dist_ma20_pct < 0.05 else 2
            reason_map["entry"] = {
                "primary": "reentry_after_cooloff",
                "codes": ["reentry_after_cooloff", "ma60_support", "continuation_confirmed"],
                "detail": f"long_reentry_after_short_cover close={close:.2f} ma20={ma20:.2f} ma60={ma60:.2f}",
            }
        return target_buy, target_sell, reason_map

    if bear_stack and (breakdown5 or lose60 or bear_bias):
        target_sell = min(MAX_SHORT_UNITS, state.sell_units + SHORT_ADD_UNITS if state.sell_units > 0 else SHORT_ENTRY_UNITS)
        reason_map["add"] = {
            "primary": "lose_ma60" if lose60 else "invalidated",
            "codes": ["lose_ma60", "lose_ma20", "invalidated"],
            "detail": f"short_add close={close:.2f} ma20={ma20:.2f} ma60={ma60:.2f} breakdown5={breakdown5} upper_wick={upper_wick:.3f} lower_wick={lower_wick:.3f} daily={daily}",
        }
        return target_buy, target_sell, reason_map

    if close > ma20 and support_wick:
        target_sell = 0
        reason_map["cover"] = {
            "primary": "reentry_after_cooloff",
            "codes": ["reentry_after_cooloff", "ma20_hold_after_reclaim"],
            "detail": f"short_recover close={close:.2f} ma20={ma20:.2f} ma60={ma60:.2f}",
        }
        return target_buy, target_sell, reason_map

    reason_map["flat"] = _hold_reason_bundle(row, window_end=False)
    return target_buy, target_sell, reason_map


def _selected_actions_from_deltas(
    *,
    prev_buy_units: int,
    prev_sell_units: int,
    next_buy_units: int,
    next_sell_units: int,
) -> list[str]:
    actions: list[str] = []
    if next_buy_units > prev_buy_units:
        actions.append("long_entry" if prev_buy_units == 0 else "long_add")
    elif next_buy_units < prev_buy_units:
        actions.append("long_exit" if next_buy_units == 0 else "long_trim")
    if next_sell_units > prev_sell_units:
        if prev_buy_units > 0:
            actions.append("hedge_open" if prev_sell_units == 0 else "hedge_add")
        else:
            actions.append("short_entry" if prev_sell_units == 0 else "short_add")
    elif next_sell_units < prev_sell_units:
        if next_sell_units == 0:
            actions.append("hedge_close" if next_buy_units > 0 else "short_cover")
        else:
            actions.append("hedge_reduce" if next_buy_units > 0 else "short_cover")
    if not actions:
        actions.append("stay")
    return actions


def _apply_state_transition(
    *,
    state: ChartState,
    target_buy_units: int,
    target_sell_units: int,
    execution_price: float,
) -> None:
    buy_delta = target_buy_units - state.buy_units
    sell_delta = target_sell_units - state.sell_units
    if buy_delta > 0:
        _fill_long_entry(state, units=buy_delta, price=execution_price)
    elif buy_delta < 0:
        _fill_long_exit(state, units=abs(buy_delta), price=execution_price)
    if sell_delta > 0:
        _fill_short_entry(state, units=sell_delta, price=execution_price)
    elif sell_delta < 0:
        _fill_short_cover(state, units=abs(sell_delta), price=execution_price)


def _start_cycle_if_needed(
    *,
    state: ChartState,
    current_cycle: dict[str, Any] | None,
    decision_dt: int,
    execution_dt: int,
    action_labels: list[str],
    reason_map: dict[str, Any],
    prev_position: str,
    next_position: str,
) -> dict[str, Any] | None:
    new_cycle_started = False
    cycle_side = "flat"
    if state.buy_units > 0 and state.sell_units == 0:
        cycle_side = "long"
    elif state.sell_units > 0 and state.buy_units == 0:
        cycle_side = "short"
    elif state.buy_units > 0 and state.sell_units > 0:
        cycle_side = "long_hedged"

    if current_cycle is None and cycle_side != "flat":
        new_cycle_started = True
    elif current_cycle is not None and cycle_side == "flat":
        current_cycle["exit_decision_date"] = decision_dt
        current_cycle["exit_execution_date"] = execution_dt
        current_cycle["exit_fill_price"] = None
        current_cycle["exit_reason_summary"] = None
        current_cycle["hold_decision_days"] = int(current_cycle.get("exit_index") or 0) - int(current_cycle.get("entry_index") or 0)
        return None
    elif current_cycle is not None and current_cycle.get("side") != cycle_side and cycle_side != "flat":
        current_cycle["exit_decision_date"] = decision_dt
        current_cycle["exit_execution_date"] = execution_dt
        current_cycle["exit_reason_summary"] = None
        current_cycle["hold_decision_days"] = int(current_cycle.get("exit_index") or 0) - int(current_cycle.get("entry_index") or 0)
        return {
            "roundtrip_id": f"{state.cycle_side}_{decision_dt}",
            "side": cycle_side,
            "entry_decision_date": decision_dt,
            "entry_execution_date": execution_dt,
            "entry_fill_price": None,
            "entry_position_transition": prev_position + " -> " + next_position,
            "entry_reason_summary": None,
            "actions": list(action_labels),
            "action_details": [],
            "hedge_reason_summaries": [],
            "mark_to_market": [],
            "realized_pnl": 0.0,
            "max_unrealized_pnl": 0.0,
            "min_unrealized_pnl": 0.0,
            "adds": 0,
            "entry_index": 0,
        }
    if new_cycle_started:
        return {
            "roundtrip_id": f"{state.cycle_side}_{decision_dt}",
            "side": cycle_side,
            "entry_decision_date": decision_dt,
            "entry_execution_date": execution_dt,
            "entry_fill_price": None,
            "entry_position_transition": prev_position + " -> " + next_position,
            "entry_reason_summary": None,
            "actions": list(action_labels),
            "action_details": [],
            "hedge_reason_summaries": [],
            "mark_to_market": [],
            "realized_pnl": 0.0,
            "max_unrealized_pnl": 0.0,
            "min_unrealized_pnl": 0.0,
            "adds": 0,
            "entry_index": 0,
        }
    return current_cycle


def simulate_chart_first_replay(
    *,
    bars_frame: pd.DataFrame,
    basis_frame: pd.DataFrame,
    symbol: str = DEFAULT_SYMBOL,
    start_date: str = DEFAULT_START_DATE,
    end_date: str = DEFAULT_END_DATE,
    trade_start_date: str | None = None,
    source_db_path: Path | None = None,
) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any]]:
    frame = _prepare_chart_frame(bars_frame, basis_frame)
    frame = frame.loc[(frame["dt"] >= _date_text_to_ymd(start_date)) & (frame["dt"] <= _date_text_to_ymd(end_date))].copy()
    frame = frame.reset_index(drop=True)
    if frame.empty:
        raise RuntimeError("no chart rows remained after applying the replay window")

    trade_start_ymd = _date_text_to_ymd(trade_start_date or start_date)
    trading_days = [int(value) for value in frame["dt"].tolist()]
    bar_lookup = bars_frame.set_index("dt").to_dict(orient="index")
    source_end_dt = max(int(value) for value in bar_lookup.keys())
    state = ChartState()
    ledger_rows: list[dict[str, Any]] = []
    roundtrips: list[dict[str, Any]] = []
    current_cycle: dict[str, Any] | None = None
    accumulated_trade_count = 0

    for index, decision_dt in enumerate(trading_days):
        row = frame.loc[frame["dt"] == decision_dt].iloc[0]
        if decision_dt < trade_start_ymd:
            continue
        next_trading_day = trading_days[index + 1] if index + 1 < len(trading_days) else None
        execution_dt = next_trading_day
        terminal_source_end = False
        if execution_dt is None:
            future_dates = sorted(int(value) for value in bar_lookup.keys() if int(value) > decision_dt)
            if future_dates:
                execution_dt = future_dates[0]
            else:
                terminal_source_end = True
                execution_dt = decision_dt
        execution_bar = bar_lookup.get(execution_dt)
        if execution_bar is None:
            if terminal_source_end and decision_dt == source_end_dt:
                execution_bar = row
            else:
                raise RuntimeError(f"missing execution bar for {execution_dt}")

        prev_position = _position_text(state.sell_units, state.buy_units)
        target_buy_units, target_sell_units, reason_map = _desired_targets(state, row, end_date=end_date)
        if terminal_source_end and decision_dt == source_end_dt:
            if state.buy_units > 0 or state.sell_units > 0:
                target_buy_units = 0
                target_sell_units = 0
                reason_map = {
                    "exit": {
                        "primary": "time_stop",
                        "codes": ["time_stop", "research_fallback"],
                        "detail": f"source_coverage_end={_ymd_to_date_text(source_end_dt)} forced_close_of_open_position",
                    }
                }
            else:
                target_buy_units = 0
                target_sell_units = 0
                reason_map = {
                    "flat": {
                        "primary": "research_fallback_source_coverage_end",
                        "codes": ["research_fallback", "source_coverage_end"],
                        "detail": f"source_coverage_end={_ymd_to_date_text(source_end_dt)} no_future_execution_bar_available",
                    }
                }
        selected_actions = _selected_actions_from_deltas(
            prev_buy_units=state.buy_units,
            prev_sell_units=state.sell_units,
            next_buy_units=target_buy_units,
            next_sell_units=target_sell_units,
        )
        if selected_actions == ["stay"]:
            target_buy_units = state.buy_units
            target_sell_units = state.sell_units

        if current_cycle is not None and (target_buy_units == 0 and target_sell_units == 0):
            current_cycle["exit_decision_date"] = decision_dt
            current_cycle["exit_execution_date"] = execution_dt
            current_cycle["exit_fill_price"] = _safe_float(execution_bar.get("o"))
            current_cycle["exit_reason_summary"] = None
            current_cycle["exit_index"] = index
            current_cycle["hold_decision_days"] = int(current_cycle["exit_index"] - int(current_cycle.get("entry_index") or index))
        elif current_cycle is None and (target_buy_units > 0 or target_sell_units > 0):
            cycle_side = "long" if target_buy_units > 0 else "short"
            current_cycle = {
                "roundtrip_id": f"{symbol}_{decision_dt}_{cycle_side}",
                "side": cycle_side,
                "entry_decision_date": decision_dt,
                "entry_index": index,
                "entry_execution_date": execution_dt,
                "entry_fill_price": _safe_float(execution_bar.get("o")),
                "entry_position_transition": f"{prev_position} -> {_position_text(target_sell_units, target_buy_units)}",
                "entry_reason_summary": reason_map.get("entry") or reason_map.get("cover") or reason_map.get("flat"),
                "actions": list(selected_actions),
                "action_details": [],
                "hedge_reason_summaries": [],
                "mark_to_market": [],
                "realized_pnl": 0.0,
                "max_unrealized_pnl": 0.0,
                "min_unrealized_pnl": 0.0,
                "adds": 0,
            }

        _apply_state_transition(state=state, target_buy_units=target_buy_units, target_sell_units=target_sell_units, execution_price=_safe_float(execution_bar.get("o")))
        next_position = _position_text(state.sell_units, state.buy_units)
        unrealized_pnl = _mark_unrealized(state, close_price=_safe_float(execution_bar.get("c")))
        state.cycle_side = "long" if state.buy_units > 0 else ("short" if state.sell_units > 0 else "flat")
        state.cycle_mark_to_market = [unrealized_pnl]
        reason_bundle_for_action = {
            "entry": reason_map.get("entry"),
            "add": reason_map.get("add"),
            "hedge": reason_map.get("hedge"),
            "trim": reason_map.get("trim"),
            "exit": reason_map.get("exit"),
            "cover": reason_map.get("cover"),
            "flat": reason_map.get("flat"),
        }
        action_meta: list[dict[str, Any]] = []
        for action in selected_actions:
            bundle = reason_bundle_for_action.get("entry") if action in {"long_entry", "short_entry"} else None
            if action == "long_add":
                bundle = reason_bundle_for_action.get("add")
            elif action in {"hedge_open", "hedge_add"}:
                bundle = reason_bundle_for_action.get("hedge")
            elif action == "long_trim":
                bundle = reason_bundle_for_action.get("trim")
            elif action == "long_exit":
                bundle = reason_bundle_for_action.get("exit")
            elif action in {"short_cover", "hedge_close", "hedge_reduce"}:
                bundle = reason_bundle_for_action.get("cover") or reason_bundle_for_action.get("flat")
            elif action == "stay":
                bundle = reason_bundle_for_action.get("flat")
            if bundle is None:
                bundle = reason_bundle_for_action.get("flat") or reason_bundle_for_action.get("entry") or reason_bundle_for_action.get("add") or reason_bundle_for_action.get("hedge")
            action_meta.append(
                {
                    "action": action,
                    "decision_date": decision_dt,
                    "execution_date": execution_dt,
                    "reason_summary": bundle,
                    "reason": bundle.get("primary") if isinstance(bundle, dict) else None,
                }
            )
        if current_cycle is not None:
            current_cycle.setdefault("action_details", []).extend(action_meta)
            current_cycle.setdefault("actions", []).extend(selected_actions)
            if reason_map.get("hedge"):
                current_cycle.setdefault("hedge_reason_summaries", []).append(reason_map["hedge"])
            if reason_map.get("add"):
                current_cycle["adds"] = int(current_cycle.get("adds") or 0) + 1
            current_cycle["mark_to_market"].append(unrealized_pnl)
            current_cycle["realized_pnl"] = float(state.realized_pnl)
            current_cycle["max_unrealized_pnl"] = float(max(current_cycle["mark_to_market"]))
            current_cycle["min_unrealized_pnl"] = float(min(current_cycle["mark_to_market"]))

        if current_cycle is not None and state.buy_units == 0 and state.sell_units == 0:
            current_cycle["exit_decision_date"] = decision_dt
            current_cycle["exit_index"] = index
            current_cycle["exit_execution_date"] = execution_dt
            current_cycle["exit_fill_price"] = _safe_float(execution_bar.get("o"))
            exit_bundle = reason_map.get("exit") or reason_map.get("cover") or reason_map.get("flat")
            current_cycle["exit_reason_summary"] = exit_bundle
            current_cycle["realized_pnl"] = float(state.realized_pnl)
            current_cycle["mark_to_market"].append(unrealized_pnl)
            current_cycle["max_unrealized_pnl"] = float(max(current_cycle["mark_to_market"]))
            current_cycle["min_unrealized_pnl"] = float(min(current_cycle["mark_to_market"]))
            current_cycle["hold_decision_days"] = int(current_cycle["exit_index"] - int(current_cycle.get("entry_index") or index))
            roundtrips.append(current_cycle)
            current_cycle = None

        chart_context = {
            "close": _safe_float(row.get("c")),
            "open": _safe_float(row.get("o")),
            "high": _safe_float(row.get("h")),
            "low": _safe_float(row.get("l")),
            "ma20": _safe_float(row.get("ma20")),
            "ma60": _safe_float(row.get("ma60")),
            "gap_pct": _safe_float(row.get("gap_pct")),
            "dist_ma20_pct": _safe_float(row.get("dist_ma20_pct")),
            "dist_ma60_pct": _safe_float(row.get("dist_ma60_pct")),
            "body_ratio": _safe_float(row.get("body_ratio")),
            "upper_wick_ratio": _safe_float(row.get("upper_wick_ratio")),
            "lower_wick_ratio": _safe_float(row.get("lower_wick_ratio")),
            "reclaim_ma20": bool(row.get("reclaim_ma20")),
            "lose_ma20": bool(row.get("lose_ma20")),
            "lose_ma60": bool(row.get("lose_ma60")),
            "breakout5": bool(row.get("breakout5")),
            "breakout10": bool(row.get("breakout10")),
            "breakdown5": bool(row.get("breakdown5")),
            "failed_breakout5": bool(row.get("failed_breakout5")),
            "bull_stack": bool(row.get("bull_stack")),
            "bear_stack": bool(row.get("bear_stack")),
            "support_wick": bool(row.get("support_wick")),
            "exhaustion": bool(row.get("exhaustion")),
            "sequence_3": _sequence_label(row),
            "monthly_main_state_ctx": row.get("monthly_main_state_ctx"),
            "weekly_main_state_ctx": row.get("weekly_main_state_ctx"),
            "daily_main_state_ctx": row.get("daily_main_state_ctx"),
            "regime_tag": row.get("regime_tag"),
        }
        target_context = {
            "target_buy_units": target_buy_units,
            "target_sell_units": target_sell_units,
            "buy_delta_units": int(target_buy_units - state.buy_units),
            "sell_delta_units": int(target_sell_units - state.sell_units),
            "selected_actions": selected_actions,
            "reason_map": reason_map,
        }
        daily_micro_snapshot = {
            "decision_date": decision_dt,
            "execution_date": execution_dt,
            "basis_version": row.get("basis_version"),
            "basis_source": row.get("basis_source"),
            "source_as_of": _safe_int(row.get("source_as_of"), 0) or None,
            "basis_payload": row.get("basis_payload"),
            "derived_context": {
                "monthly_main_state_ctx": row.get("monthly_main_state_ctx"),
                "weekly_main_state_ctx": row.get("weekly_main_state_ctx"),
                "daily_main_state_ctx": row.get("daily_main_state_ctx"),
                "regime_tag": row.get("regime_tag"),
                "hierarchical_native_regime_tag": row.get("hierarchical_native_regime_tag"),
            },
            "policy": {
                "policy_id": _policy_id(symbol),
                "mode": "chart-first",
                "chart_primary": True,
                "decision_context": target_context,
            },
        }
        notes = [
            f"decision_basis_date={_ymd_to_date_text(decision_dt)}",
            f"execution_date={_ymd_to_date_text(execution_dt)}",
        ]
        if _is_missing(row.get("source_as_of")) or _is_missing(row.get("basis_source")):
            notes.append("provenance_caveat=signal_basis_daily.source_as_of_or_basis_source_missing")
        if terminal_source_end and decision_dt == source_end_dt:
            notes.append("research_fallback=source_coverage_end_used_for_final_day")
        if selected_actions == ["stay"]:
            notes.append("research_fallback=held_by_chart_first_no_trade_gate")
        else:
            notes.append("chart_first=confirmed_ohlc_ma_gap_wick_sequence")
        ledger_rows.append(
            {
                "date": _ymd_to_date_text(decision_dt),
                "symbol": str(row.get("code") or symbol),
                "previous_position": prev_position,
                "selected_action": ";".join(selected_actions),
                "selected_actions": _json_text(selected_actions),
                "next_position": next_position,
                "execution_price": _safe_float(execution_bar.get("o")),
                "target_buy_units": target_buy_units,
                "target_sell_units": target_sell_units,
                "buy_delta_units": int(target_buy_units - state.buy_units),
                "sell_delta_units": int(target_sell_units - state.sell_units),
                "entry_reason_primary": reason_map.get("entry", {}).get("primary") if reason_map.get("entry") else None,
                "entry_reason_codes": _json_text(reason_map.get("entry", {}).get("codes")) if reason_map.get("entry") else None,
                "entry_reason_detail": reason_map.get("entry", {}).get("detail") if reason_map.get("entry") else None,
                "add_reason_primary": reason_map.get("add", {}).get("primary") if reason_map.get("add") else None,
                "add_reason_codes": _json_text(reason_map.get("add", {}).get("codes")) if reason_map.get("add") else None,
                "add_reason_detail": reason_map.get("add", {}).get("detail") if reason_map.get("add") else None,
                "hedge_reason_primary": reason_map.get("hedge", {}).get("primary") if reason_map.get("hedge") else None,
                "hedge_reason_codes": _json_text(reason_map.get("hedge", {}).get("codes")) if reason_map.get("hedge") else None,
                "hedge_reason_detail": reason_map.get("hedge", {}).get("detail") if reason_map.get("hedge") else None,
                "trim_reason_primary": reason_map.get("trim", {}).get("primary") if reason_map.get("trim") else None,
                "trim_reason_codes": _json_text(reason_map.get("trim", {}).get("codes")) if reason_map.get("trim") else None,
                "trim_reason_detail": reason_map.get("trim", {}).get("detail") if reason_map.get("trim") else None,
                "exit_reason_primary": reason_map.get("exit", {}).get("primary") if reason_map.get("exit") else None,
                "exit_reason_codes": _json_text(reason_map.get("exit", {}).get("codes")) if reason_map.get("exit") else None,
                "exit_reason_detail": reason_map.get("exit", {}).get("detail") if reason_map.get("exit") else None,
                "cover_reason_primary": reason_map.get("cover", {}).get("primary") if reason_map.get("cover") else None,
                "cover_reason_codes": _json_text(reason_map.get("cover", {}).get("codes")) if reason_map.get("cover") else None,
                "cover_reason_detail": reason_map.get("cover", {}).get("detail") if reason_map.get("cover") else None,
                "flat_reason_primary": reason_map.get("flat", {}).get("primary") if reason_map.get("flat") else None,
                "flat_reason_codes": _json_text(reason_map.get("flat", {}).get("codes")) if reason_map.get("flat") else None,
                "flat_reason_detail": reason_map.get("flat", {}).get("detail") if reason_map.get("flat") else None,
                "chart_context": _json_text(chart_context),
                "daily_micro_snapshot": _json_text(daily_micro_snapshot),
                "realized_pnl": float(state.realized_pnl),
                "unrealized_pnl": float(unrealized_pnl),
                "invalidation_state": _json_text(
                    {
                        "state": "flat" if state.buy_units == 0 and state.sell_units == 0 else ("long" if state.buy_units > 0 else "short"),
                        "trigger": reason_map.get("exit", reason_map.get("cover", reason_map.get("entry", reason_map.get("flat", {})))).get("primary")
                        if isinstance(reason_map.get("exit", reason_map.get("cover", reason_map.get("entry", reason_map.get("flat", {})))), dict)
                        else None,
                        "long_units": int(state.buy_units),
                        "sell_units": int(state.sell_units),
                    }
                ),
                "notes_if_any": "; ".join(notes),
            }
        )

        if current_cycle is not None:
            if reason_map.get("entry") and current_cycle.get("entry_reason_summary") is None:
                current_cycle["entry_reason_summary"] = reason_map["entry"]
            if reason_map.get("add"):
                current_cycle.setdefault("add_reason_summaries", []).append(reason_map["add"])
            if reason_map.get("hedge"):
                current_cycle.setdefault("hedge_reason_summaries", []).append(reason_map["hedge"])
            if reason_map.get("exit"):
                current_cycle["exit_reason_summary"] = reason_map["exit"]
            if reason_map.get("cover"):
                current_cycle["exit_reason_summary"] = reason_map["cover"]
            current_cycle["realized_pnl"] = float(state.realized_pnl)
            current_cycle["mark_to_market"].append(unrealized_pnl)
            current_cycle["max_unrealized_pnl"] = float(max(current_cycle["mark_to_market"]))
            current_cycle["min_unrealized_pnl"] = float(min(current_cycle["mark_to_market"]))

        if state.buy_units == 0 and state.sell_units == 0 and current_cycle is not None:
            current_cycle["exit_decision_date"] = decision_dt
            current_cycle["exit_index"] = index
            current_cycle["exit_execution_date"] = execution_dt
            current_cycle["exit_fill_price"] = _safe_float(execution_bar.get("o"))
            current_cycle["realized_pnl"] = float(state.realized_pnl)
            current_cycle["hold_decision_days"] = int(current_cycle["exit_index"] - int(current_cycle.get("entry_index") or index))
            roundtrips.append(current_cycle)
            current_cycle = None

    if current_cycle is not None:
        current_cycle["realized_pnl"] = float(state.realized_pnl)
        current_cycle["max_unrealized_pnl"] = float(max(current_cycle["mark_to_market"])) if current_cycle["mark_to_market"] else 0.0
        current_cycle["min_unrealized_pnl"] = float(min(current_cycle["mark_to_market"])) if current_cycle["mark_to_market"] else 0.0
        current_cycle["hold_decision_days"] = int(current_cycle.get("exit_index") or len(trading_days) - 1) - int(current_cycle.get("entry_index") or 0)
        roundtrips.append(current_cycle)

    ledger_frame = pd.DataFrame(ledger_rows)
    aggregate = {
        "policy_id": _policy_id(symbol),
        "mode": "chart-first",
        "roundtrip_count": int(len(roundtrips)),
        "entry_count": int(sum(1 for row in ledger_rows if str(row["selected_action"]).startswith("long_entry") or str(row["selected_action"]).startswith("short_entry"))),
        "add_count": int(sum(1 for row in ledger_rows if "long_add" in str(row["selected_action"]) or "short_add" in str(row["selected_action"]))),
        "hedge_count": int(sum(1 for row in ledger_rows if "hedge_open" in str(row["selected_action"]) or "hedge_add" in str(row["selected_action"]) or "hedge_reduce" in str(row["selected_action"]) or "hedge_close" in str(row["selected_action"]))),
        "exit_count": int(sum(1 for row in ledger_rows if "long_exit" in str(row["selected_action"]) or "short_cover" in str(row["selected_action"]))),
        "stay_count": int(sum(1 for row in ledger_rows if str(row["selected_action"]) == "stay")),
        "net_realized_pnl": float(state.realized_pnl),
        "final_position": _position_text(state.sell_units, state.buy_units),
        "days_covered": int(len(ledger_rows)),
    }
    config = {
        "policy_id": _policy_id(symbol),
        "symbol": symbol,
        "name": symbol,
        "freeze_date": DEFAULT_FREEZE_DATE,
        "period": {"start": start_date, "end": end_date},
        "trade_start_date": _ymd_to_date_text(trade_start_ymd),
        "requested_end_date": end_date,
        "source_coverage_end_date": _ymd_to_date_text(source_end_dt),
        "effective_end_date": min(end_date, _ymd_to_date_text(source_end_dt)),
        "source_db_path": str(source_db_path or DEFAULT_SOURCE_DB_PATH),
        "data_source": {
            "bars_table": "daily_bars",
            "basis_table": "signal_basis_daily",
            "execution_model": "decision on confirmed daily OHLC; execution at next trading day open",
            "primary_inputs": ["confirmed_ohlc", "ma20", "ma60", "gap", "wick", "body", "candle_sequence", "higher_frame_regime"],
            "secondary_inputs": ["basis_payload_context", "daily_micro_proxy_only_as_secondary_context"],
        },
        "policy_mode": "chart-first",
        "policy_scope": [
            "long_entry",
            "long_add",
            "long_trim",
            "long_exit",
            "short_entry",
            "short_add",
            "short_cover",
            "hedge_open",
            "hedge_add",
            "hedge_reduce",
            "hedge_close",
            "stay",
        ],
        "non_scope": [
            "MeeMee UI changes",
            "portfolio logic",
            "all-symbol replay",
            "in-period learning",
            "proxy-only daily_micro scoring",
        ],
        "position_contract": {
            "notation": "X-Y",
            "sell_units_field": "X",
            "buy_units_field": "Y",
            "shares_per_unit": SHARES_PER_UNIT,
            "interpretation": "sell units represent shorts or hedges; buy units represent long units",
        },
        "thresholds": {
            "entry_long_units": ENTRY_LONG_UNITS,
            "confirmed_long_units": CONFIRMED_LONG_UNITS,
            "full_long_units": FULL_LONG_UNITS,
            "light_hedge_units": LIGHT_HEDGE_UNITS,
            "heavy_hedge_units": HEAVY_HEDGE_UNITS,
            "short_entry_units": SHORT_ENTRY_UNITS,
            "short_add_units": SHORT_ADD_UNITS,
            "max_short_units": MAX_SHORT_UNITS,
        },
        "window_end_force_exit": True,
        "research_fallback": {
            "used": True,
            "scope": "reason classification only when chart signal family is not cleanly available",
            "why": "The run is chart-first, but a few context labels are still borrowed from the existing basis payload so the ledger stays explicit rather than silently dropping context.",
        },
        "generated_at": _utc_now(),
    }
    return ledger_frame, config, {"aggregate": aggregate, "roundtrips": roundtrips}


def _capture_assessment(realized_pnl: float, max_unrealized_pnl: float) -> dict[str, Any]:
    if max_unrealized_pnl <= 0:
        return {"captured": False, "capture_ratio": None, "reason": "no_positive_excursion"}
    ratio = realized_pnl / max_unrealized_pnl
    return {
        "captured": realized_pnl >= 0.7 * max_unrealized_pnl,
        "capture_ratio": float(ratio),
        "reason": "realized_vs_max_unrealized_ratio",
    }


def _exit_timing_assessment(realized_pnl: float, max_unrealized_pnl: float, exit_reason: str | None) -> dict[str, Any]:
    if exit_reason == "time_stop":
        return {"label": "acceptable", "reason": "The final exit was forced by the fixed window end, not an in-window structural failure."}
    if max_unrealized_pnl <= 0:
        return {"label": "none", "reason": "No positive excursion was recorded."}
    if realized_pnl >= 0.9 * max_unrealized_pnl:
        return {"label": "acceptable", "reason": "The exit captured most of the realized run."}
    if realized_pnl >= 0.5 * max_unrealized_pnl:
        return {"label": "early", "reason": "The exit left some upside on the table but did not destroy the trade."}
    return {"label": "late", "reason": "The exit lagged the move badly relative to the best excursion."}


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
    capture_ratio_sum = 0.0
    capture_count = 0
    exit_labels: list[str] = []
    for roundtrip in roundtrips:
        realized_pnl = float(roundtrip.get("realized_pnl") or 0.0)
        max_unrealized_pnl = float(roundtrip.get("max_unrealized_pnl") or 0.0)
        capture = _capture_assessment(realized_pnl, max_unrealized_pnl)
        exit_reason = str(roundtrip.get("exit_reason_summary", {}).get("primary") or "")
        exit_timing = _exit_timing_assessment(realized_pnl, max_unrealized_pnl, exit_reason)
        holding_days = int(roundtrip.get("hold_decision_days") or 0)
        drawdown = float(min(0.0, min(roundtrip.get("mark_to_market") or [0.0])))
        total_holding_days += holding_days
        total_realized_pnl += realized_pnl
        worst_drawdown = min(worst_drawdown, drawdown)
        if capture["capture_ratio"] is not None:
            capture_ratio_sum += float(capture["capture_ratio"])
            capture_count += 1
        exit_labels.append(str(exit_timing["label"]))
        summary_rows.append(
            {
                "roundtrip_id": roundtrip.get("roundtrip_id"),
                "side": roundtrip.get("side"),
                "entry_dates": [_ymd_to_date_text(roundtrip["entry_decision_date"])],
                "entry_position_transition": roundtrip.get("entry_position_transition"),
                "entry_reason_summary": roundtrip.get("entry_reason_summary"),
                "hedge_reason_summaries": roundtrip.get("hedge_reason_summaries") or [],
                "add_dates": [_ymd_to_date_text(action["decision_date"]) for action in roundtrip.get("action_details") or [] if action.get("action") in {"long_add", "short_add"}],
                "add_reason_summary": [action.get("reason_summary") for action in roundtrip.get("action_details") or [] if action.get("action") in {"long_add", "short_add"}],
                "exit_dates": [_ymd_to_date_text(roundtrip["exit_decision_date"])] if roundtrip.get("exit_decision_date") else [],
                "exit_reason_summary": roundtrip.get("exit_reason_summary"),
                "total_realized_pnl": realized_pnl,
                "max_drawdown_during_holding": drawdown,
                "holding_days": holding_days,
                "major_move_captured": capture,
                "exit_timing": exit_timing,
            }
        )
    aggregate_summary = {
        **aggregate,
        "entry_dates": [_ymd_to_date_text(int(row["entry_decision_date"])) for row in roundtrips if row.get("entry_decision_date")],
        "exit_dates": [_ymd_to_date_text(int(row["exit_decision_date"])) for row in roundtrips if row.get("exit_decision_date")],
        "entry_reason_primary": [row.get("entry_reason_summary", {}).get("primary") for row in roundtrips if row.get("entry_reason_summary")],
        "exit_reason_primary": [row.get("exit_reason_summary", {}).get("primary") for row in roundtrips if row.get("exit_reason_summary")],
        "total_realized_pnl": total_realized_pnl,
        "max_drawdown_during_holding": worst_drawdown,
        "holding_days": total_holding_days,
        "average_capture_ratio": float(capture_ratio_sum / max(capture_count, 1)),
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


def _build_entry_reason_report(
    *,
    config: dict[str, Any],
    ledger_frame: pd.DataFrame,
    roundtrip_payload: dict[str, Any],
) -> dict[str, Any]:
    def _primary_counts(column_name: str) -> list[dict[str, Any]]:
        if column_name not in ledger_frame.columns:
            return []
        series = ledger_frame[column_name].dropna().astype(str)
        return [
            {"reason": reason, "count": int(count)}
            for reason, count in series.value_counts().sort_index().items()
        ]

    rows: list[dict[str, Any]] = []
    for roundtrip in roundtrip_payload["roundtrips"]:
        rows.append(
            {
                "roundtrip_id": roundtrip.get("roundtrip_id"),
                "side": roundtrip.get("side"),
                "entry_date": _ymd_to_date_text(int(roundtrip["entry_decision_date"])),
                "entry_position_transition": roundtrip.get("entry_position_transition"),
                "entry_reason_summary": roundtrip.get("entry_reason_summary"),
                "hedge_reason_summaries": roundtrip.get("hedge_reason_summaries") or [],
                "add_reason_summary": [action.get("reason_summary") for action in roundtrip.get("action_details") or [] if action.get("action") in {"long_add", "short_add"}],
                "exit_date": _ymd_to_date_text(int(roundtrip["exit_decision_date"])) if roundtrip.get("exit_decision_date") else None,
                "exit_reason_summary": roundtrip.get("exit_reason_summary"),
            }
        )
    return {
        "schema_version": f"{_sample_name(config['symbol'])}_entry_reason_report_v1",
        "policy_id": config["policy_id"],
        "symbol": config["symbol"],
        "period": config["period"],
        "roundtrip_rows": rows,
        "reason_rollup": {
            "entry_reason_primary": _primary_counts("entry_reason_primary"),
            "add_reason_primary": _primary_counts("add_reason_primary"),
            "hedge_reason_primary": _primary_counts("hedge_reason_primary"),
            "exit_reason_primary": _primary_counts("exit_reason_primary"),
            "cover_reason_primary": _primary_counts("cover_reason_primary"),
            "flat_reason_primary": _primary_counts("flat_reason_primary"),
        },
        "generated_at": _utc_now(),
    }


def _build_postmortem(
    *,
    config: dict[str, Any],
    ledger_frame: pd.DataFrame,
    roundtrip_payload: dict[str, Any],
) -> dict[str, Any]:
    roundtrips = roundtrip_payload["roundtrips"]
    first_roundtrip = roundtrips[0] if roundtrips else None
    reason_reviews: list[dict[str, Any]] = []
    if first_roundtrip:
        first_exit_date = _ymd_to_date_text(int(first_roundtrip["exit_decision_date"])) if first_roundtrip.get("exit_decision_date") else None
        flat_after_exit = {}
        if first_exit_date is not None:
            flat_rows = ledger_frame.loc[
                (ledger_frame["date"] > first_exit_date) & (ledger_frame["selected_action"] == "stay")
            ]
            if not flat_rows.empty:
                flat_after_exit = {
                    "primary": flat_rows.iloc[0].get("flat_reason_primary"),
                    "codes": json.loads(str(flat_rows.iloc[0].get("flat_reason_codes") or "[]")),
                    "detail": flat_rows.iloc[0].get("flat_reason_detail"),
                }
        entry_summary = first_roundtrip.get("entry_reason_summary") or {}
        hedge_summaries = first_roundtrip.get("hedge_reason_summaries") or []
        exit_summary = first_roundtrip.get("exit_reason_summary") or {}
        reason_reviews.extend(
            [
                {
                    "phase": "entry",
                    "recorded_reason_primary": entry_summary.get("primary"),
                    "assessment": "valid",
                    "reason": "The first bullish decision was driven by confirmed OHLC support and higher-frame recovery context.",
                },
                {
                    "phase": "hedge",
                    "recorded_reason_primary": hedge_summaries[0].get("primary") if hedge_summaries else None,
                    "assessment": "valid" if hedge_summaries else "not_used_or_inconclusive",
                    "reason": "The hedge was sized from extension and wick exhaustion; the output should show whether the ratio was too heavy or too light.",
                },
                {
                    "phase": "exit",
                    "recorded_reason_primary": exit_summary.get("primary"),
                    "assessment": "acceptable" if exit_summary.get("primary") in {"lose_ma60", "time_stop"} else "review",
                    "reason": "Exit should be structural or window-end based; anything else needs review.",
                },
                {
                    "phase": "flat",
                    "recorded_reason_primary": flat_after_exit.get("primary"),
                    "assessment": "blocked_unnecessarily" if flat_after_exit else "valid",
                    "reason": "Flat decisions after the first exit should be judged against whether the later move was still tradable.",
                },
            ]
        )
    return {
        "policy_id": config["policy_id"],
        "symbol": config["symbol"],
        "period": config["period"],
        "research_fallback_used": True,
        "coverage": {
            "ledger_rows": int(len(ledger_frame)),
            "roundtrip_count": int(roundtrip_payload["aggregate"]["roundtrip_count"]),
            "entry_count": int(roundtrip_payload["aggregate"]["entry_count"]),
            "hedge_count": int(roundtrip_payload["aggregate"]["hedge_count"]),
            "exit_count": int(roundtrip_payload["aggregate"]["exit_count"]),
        },
        "entry_timing_review": {
            "assessment": "acceptable" if first_roundtrip else "none",
            "reason": "The first chart-first entry should be explainable from reclaim/support structure and higher-frame context." if first_roundtrip else "no_roundtrip",
        },
        "hedge_review": {
            "assessment": "review",
            "reason": "The first pass intentionally uses rule-based hedge sizing; the main question is whether the hedge ratio was calibrated too tightly or too loosely.",
        },
        "exit_timing_review": {
            "assessment": "acceptable" if any(reason.get("assessment") == "acceptable" for reason in reason_reviews if reason.get("phase") == "exit") else "review",
            "reason": "Window-end exits are acceptable; structural exits should be validated against the chart state.",
        },
        "reason_reviews": reason_reviews,
        "next_improvement_axis": {
            "axis": "hedge_ratio_calibration",
            "description": "Tighten the mapping from extension/exhaustion to hedge quantity so the hedge reduces downside without suppressing the rest of the trend.",
            "why_it_matters": "The first pass should make hedge sizing explainable, then the next pass can refine the 25% / 40% / 50% hedge thresholds if they are too aggressive.",
        },
        "risks": [
            "The chart-first rules are deterministic but still intentionally narrow.",
            "The run borrows higher-frame regime context from the existing basis payload, so that context should stay secondary to OHLC / MA / wick / sequence evidence.",
            "A final forced exit at the window end is an explicit study convention, not a product rule.",
        ],
        "generated_at": _utc_now(),
    }


def run_chart_first_replay(
    *,
    source_db_path: Path = DEFAULT_SOURCE_DB_PATH,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    symbol: str = DEFAULT_SYMBOL,
    start_date: str = DEFAULT_START_DATE,
    end_date: str = DEFAULT_END_DATE,
    freeze_date: str = DEFAULT_FREEZE_DATE,
    trade_start_date: str | None = None,
) -> dict[str, Any]:
    bars_frame, basis_frame = _load_source_frames(
        source_db_path=source_db_path,
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
    )
    ledger_frame, config, roundtrip_payload = simulate_chart_first_replay(
        bars_frame=bars_frame,
        basis_frame=basis_frame,
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        trade_start_date=trade_start_date,
        source_db_path=source_db_path,
    )
    config = {
        **config,
        "freeze_date": freeze_date,
        "output_dir": str(output_dir),
        "source_db_path": str(source_db_path),
    }
    generated_at = _utc_now()
    roundtrip_summary = _build_roundtrip_summary(
        config=config,
        ledger_frame=ledger_frame,
        roundtrip_payload=roundtrip_payload,
        generated_at=generated_at,
    )
    postmortem = _build_postmortem(config=config, ledger_frame=ledger_frame, roundtrip_payload=roundtrip_payload)
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
            _build_entry_reason_report(config=config, ledger_frame=ledger_frame, roundtrip_payload=roundtrip_payload),
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
    parser = argparse.ArgumentParser(description="Build a chart-first TRADEX replay study.")
    parser.add_argument("--source-db-path", default=str(DEFAULT_SOURCE_DB_PATH))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--start-date", default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", default=DEFAULT_END_DATE)
    parser.add_argument("--freeze-date", default=DEFAULT_FREEZE_DATE)
    args = parser.parse_args(argv)
    payload = run_chart_first_replay(
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
