from __future__ import annotations

import bisect
import json
import math
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from typing import Any, Callable

import numpy as np
import pandas as pd

from app.backend.core.legacy_analysis_control import is_legacy_analysis_disabled
from app.db.session import get_conn


LIQ_COST_TURNOVER_LOW = 50_000_000.0
LIQ_COST_TURNOVER_MID = 200_000_000.0
LIQ_SLIPPAGE_BPS_LOW = 14.0
LIQ_SLIPPAGE_BPS_MID = 7.0
LIQ_SLIPPAGE_BPS_HIGH = 2.0
LIQ_SLIPPAGE_BPS_UNKNOWN = 18.0
SHORT_BORROW_BPS_20D = 6.0


@dataclass(frozen=True)
class StrategyBacktestConfig:
    max_positions: int = 3
    # Long entry default is now a 3-step split (initial/add1/add2 = 1/1/1).
    initial_units: int = 1
    add1_units: int = 1
    add2_units: int = 1
    hedge_units: int = 1
    min_hedge_ratio: float = 0.2
    cost_bps: float = 20.0
    min_history_bars: int = 220
    prefer_net_short_ratio: float = 2.0
    event_lookback_days: int = 2
    event_lookahead_days: int = 1
    min_long_score: float = 1.0
    min_short_score: float = 1.0
    max_new_entries_per_day: int = 3
    max_new_entries_per_month: int | None = None
    allowed_sides: str = "both"  # both | long | short
    require_decision_for_long: bool = False
    allow_decision_only_long_entries: bool = False
    require_ma_bull_stack_long: bool = False
    max_dist_ma20_long: float | None = None
    min_volume_ratio_long: float = 0.0
    max_atr_pct_long: float | None = None
    min_ml_p_up_long: float | None = None
    allowed_long_setups: tuple[str, ...] | None = None
    allowed_short_setups: tuple[str, ...] | None = None
    use_regime_filter: bool = False
    regime_breadth_lookback_days: int = 20
    regime_long_min_breadth_above60: float = 0.52
    regime_short_max_breadth_above60: float = 0.48
    range_bias_width_min: float = 0.08
    range_bias_long_pos_min: float = 0.60
    range_bias_short_pos_max: float = 0.40
    ma20_count20_min_long: int = 12
    ma20_count20_min_short: int = 12
    ma60_count60_min_long: int = 30
    ma60_count60_min_short: int = 30

    @property
    def cost_rate(self) -> float:
        return float(self.cost_bps) / 10_000.0


@dataclass
class OpenPosition:
    code: str
    side: str  # long | short
    units: int
    entry_price: float
    entry_dt: int
    add_step: int
    half_taken: bool
    is_hedge: bool
    sector33_code: str | None
    entry_score: float
    setup_id: str


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table_name],
    ).fetchone()
    return bool(row and row[0])


def _safe_float(value: object) -> float | None:
    try:
        f = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if not math.isfinite(f):
        return None
    return f


def _liquidity_slippage_bps(turnover20: float | None) -> float:
    turnover = _safe_float(turnover20)
    if turnover is None:
        return float(LIQ_SLIPPAGE_BPS_UNKNOWN)
    if turnover < float(LIQ_COST_TURNOVER_LOW):
        return float(LIQ_SLIPPAGE_BPS_LOW)
    if turnover < float(LIQ_COST_TURNOVER_MID):
        return float(LIQ_SLIPPAGE_BPS_MID)
    return float(LIQ_SLIPPAGE_BPS_HIGH)


def _trade_cost_rate(*, base_cost_rate: float, turnover20: float | None, side: str) -> float:
    slippage_rate = _liquidity_slippage_bps(turnover20) / 10_000.0
    borrow_rate = (SHORT_BORROW_BPS_20D / 10_000.0) if str(side) == "short" else 0.0
    return float(base_cost_rate) + float(slippage_rate) + float(borrow_rate)


def _safe_int(value: object) -> int | None:
    try:
        if value is None:
            return None
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _dt_to_date(value: object) -> date | None:
    v = _safe_int(value)
    if v is None:
        return None
    if 19_000_101 <= v <= 21_001_231:
        y = v // 10_000
        m = (v // 100) % 100
        d = v % 100
        try:
            return date(y, m, d)
        except ValueError:
            return None
    ts = v
    if ts > 10_000_000_000:
        ts //= 1000
    try:
        return datetime.fromtimestamp(ts, tz=timezone.utc).date()
    except Exception:
        return None


def _parse_date_text(value: object) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _build_streak(series: pd.Series) -> pd.Series:
    arr = series.fillna(False).to_numpy(dtype=np.bool_)
    out = np.zeros(arr.shape[0], dtype=np.int32)
    streak = 0
    for i, v in enumerate(arr):
        if v:
            streak += 1
        else:
            streak = 0
        out[i] = streak
    return pd.Series(out, index=series.index)


def _position_return(side: str, entry_price: float, price: float) -> float:
    if entry_price <= 0:
        return 0.0
    if side == "short":
        return (entry_price - price) / entry_price
    return (price - entry_price) / entry_price


def _close_units(
    position: OpenPosition,
    *,
    exit_price: float,
    qty: int,
    dt: int,
    dt_date: date | None,
    reason: str,
    base_cost_rate: float,
    turnover20: float | None,
) -> tuple[dict[str, Any], float]:
    quantity = max(1, min(int(qty), int(position.units)))
    gross = _position_return(position.side, float(position.entry_price), float(exit_price))
    cost_rate = _trade_cost_rate(
        base_cost_rate=base_cost_rate,
        turnover20=turnover20,
        side=position.side,
    )
    net = float(gross) - float(cost_rate)
    position.units = int(position.units) - int(quantity)
    event = {
        "code": position.code,
        "side": position.side,
        "qty": int(quantity),
        "sector33_code": position.sector33_code,
        "is_hedge": bool(position.is_hedge),
        "entry_dt": int(position.entry_dt),
        "exit_dt": int(dt),
        "entry_date": _dt_to_date(position.entry_dt).isoformat() if _dt_to_date(position.entry_dt) else None,
        "exit_date": dt_date.isoformat() if dt_date else None,
        "entry_price": float(position.entry_price),
        "exit_price": float(exit_price),
        "ret_gross": float(gross),
        "ret_net": float(net),
        "reason": str(reason),
        "setup_id": str(position.setup_id),
    }
    return event, float(net) * float(quantity)


def _build_trade_group_breakdown(
    trades_df: pd.DataFrame,
    *,
    group_col: str,
    unknown_label: str = "unknown",
) -> dict[str, dict[str, Any]]:
    if trades_df.empty or group_col not in trades_df.columns:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for group_key, group in trades_df.groupby(group_col, dropna=False):
        label = str(group_key).strip() if group_key is not None and str(group_key).strip() else unknown_label
        trades = int(len(group))
        wins = int((group["ret_net"] > 0).sum()) if "ret_net" in group.columns else 0
        avg_ret = float(group["ret_net"].mean()) if "ret_net" in group.columns and trades > 0 else 0.0
        ret_net_sum = (
            float((group["ret_net"] * group["qty"]).sum())
            if {"ret_net", "qty"}.issubset(group.columns)
            else 0.0
        )
        pos_ret_sum = float(group.loc[group["ret_net"] > 0, "ret_net"].sum()) if "ret_net" in group.columns else 0.0
        neg_ret_sum = float(group.loc[group["ret_net"] < 0, "ret_net"].sum()) if "ret_net" in group.columns else 0.0
        profit_factor = (pos_ret_sum / abs(neg_ret_sum)) if neg_ret_sum < 0 else None
        out[label] = {
            "count": int(trades),
            "trades": int(trades),
            "wins": int(wins),
            "losses": int(max(0, trades - wins)),
            "win_rate": float(wins / trades) if trades > 0 else 0.0,
            "avg_ret_net": float(avg_ret),
            "ret_net_sum": float(ret_net_sum),
            "sum_ret_net": float(ret_net_sum),
            "pos_ret_sum": float(pos_ret_sum),
            "neg_ret_sum": float(neg_ret_sum),
            "profit_factor": float(profit_factor) if profit_factor is not None else None,
        }
    return out


def _entry_setup_id(row: dict[str, Any], side: str) -> str:
    if side == "long":
        if bool(row.get("buy_p2")):
            return "long_breakout_p2"
        if bool(row.get("buy_p1")):
            return "long_reversal_p1"
        if bool(row.get("buy_p3")):
            return "long_pullback_p3"
        if bool(row.get("decision_up")):
            return "long_decision_up"
        return "long_entry"

    if bool(row.get("sell_p3")):
        return "short_crash_top_p3"
    if bool(row.get("sell_p4")):
        return "short_downtrend_p4"
    if bool(row.get("sell_p1")):
        return "short_failed_high_p1"
    if bool(row.get("sell_p2")):
        return "short_box_fail_p2"
    if bool(row.get("sell_p5")):
        return "short_ma20_break_p5"
    if bool(row.get("decision_down")):
        return "short_decision_down"
    return "short_entry"


def _add_units(position: OpenPosition, add_qty: int, price: float) -> None:
    q = max(0, int(add_qty))
    if q <= 0:
        return
    total = int(position.units) + q
    if total <= 0:
        return
    position.entry_price = (float(position.entry_price) * float(position.units) + float(price) * float(q)) / float(total)
    position.units = int(total)
    position.add_step = int(position.add_step) + 1


def _ensure_backtest_schema(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS strategy_backtest_runs (
            run_id TEXT PRIMARY KEY,
            started_at TIMESTAMP,
            finished_at TIMESTAMP,
            status TEXT,
            start_dt INTEGER,
            end_dt INTEGER,
            max_codes INTEGER,
            config_json TEXT,
            metrics_json TEXT,
            note TEXT
        );
        """
    )


def _load_market_frame(
    conn,
    *,
    start_dt: int | None,
    end_dt: int | None,
    max_codes: int | None,
) -> pd.DataFrame:
    has_sector = _table_exists(conn, "industry_master")
    has_daily_ma = _table_exists(conn, "daily_ma")
    has_ml_pred = (not is_legacy_analysis_disabled()) and _table_exists(conn, "ml_pred_20d")
    ma_select = (
        "m.ma7 AS ma7, m.ma20 AS ma20, m.ma60 AS ma60"
        if has_daily_ma
        else "NULL AS ma7, NULL AS ma20, NULL AS ma60"
    )
    ma_join = "LEFT JOIN daily_ma m ON m.code = b.code AND m.date = b.date" if has_daily_ma else ""
    sector_select = "im.sector33_code AS sector33_code" if has_sector else "NULL AS sector33_code"
    sector_join = "LEFT JOIN industry_master im ON im.code = b.code" if has_sector else ""
    ml_select = "mp.p_up AS ml_p_up" if has_ml_pred else "NULL AS ml_p_up"
    ml_join = "LEFT JOIN ml_pred_20d mp ON mp.code = b.code AND mp.dt = b.date" if has_ml_pred else ""

    where: list[str] = []
    params: list[object] = []
    if start_dt is not None:
        where.append("b.date >= ?")
        params.append(int(start_dt))
    if end_dt is not None:
        where.append("b.date <= ?")
        params.append(int(end_dt))
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""

    if max_codes is not None and int(max_codes) > 0:
        limit_n = int(max_codes)
        and_filters = f"AND {' AND '.join(where)}" if where else ""
        sql = f"""
            WITH latest AS (
                SELECT MAX(date) AS dt FROM daily_bars
            ),
            universe AS (
                SELECT b.code
                FROM daily_bars b
                JOIN latest l ON b.date = l.dt
                ORDER BY COALESCE(b.v, 0) DESC, b.code ASC
                LIMIT ?
            )
            SELECT
                b.date AS dt,
                b.code AS code,
                b.o AS o,
                b.h AS h,
                b.l AS l,
                b.c AS c,
                b.v AS v,
                {ma_select},
                {ml_select},
                {sector_select}
            FROM daily_bars b
            {ma_join}
            {ml_join}
            {sector_join}
            WHERE b.code IN (SELECT code FROM universe)
            {and_filters}
            ORDER BY b.code ASC, b.date ASC
        """
        return conn.execute(sql, [limit_n, *params]).df()

    sql = f"""
        SELECT
            b.date AS dt,
            b.code AS code,
            b.o AS o,
            b.h AS h,
            b.l AS l,
            b.c AS c,
            b.v AS v,
            {ma_select},
            {ml_select},
            {sector_select}
        FROM daily_bars b
        {ma_join}
        {ml_join}
        {sector_join}
        {where_sql}
        ORDER BY b.code ASC, b.date ASC
    """
    return conn.execute(sql, params).df()


def _prepare_feature_frame(df: pd.DataFrame, cfg: StrategyBacktestConfig) -> pd.DataFrame:
    if df.empty:
        return df
    frame = df.copy()
    frame = frame.sort_values(["code", "dt"], kind="stable").reset_index(drop=True)
    g = frame.groupby("code", sort=False, group_keys=False)
    range_width_min = max(0.0, float(cfg.range_bias_width_min))
    range_pos_long_min = min(1.0, max(0.0, float(cfg.range_bias_long_pos_min)))
    range_pos_short_max = min(1.0, max(0.0, float(cfg.range_bias_short_pos_max)))
    ma20_count20_min_long = max(1, int(cfg.ma20_count20_min_long))
    ma20_count20_min_short = max(1, int(cfg.ma20_count20_min_short))
    ma60_count60_min_long = max(1, int(cfg.ma60_count60_min_long))
    ma60_count60_min_short = max(1, int(cfg.ma60_count60_min_short))

    # MAs: daily_ma values are preferred; missing values are backfilled from raw close rolling.
    for period in (7, 20, 60):
        calc = g["c"].transform(lambda s, p=period: s.rolling(p, min_periods=p).mean())
        col = f"ma{period}"
        frame[col] = frame[col].where(frame[col].notna(), calc)

    frame["ma100"] = g["c"].transform(lambda s: s.rolling(100, min_periods=100).mean())
    frame["ma200"] = g["c"].transform(lambda s: s.rolling(200, min_periods=200).mean())
    frame["vol_ma20"] = g["v"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    frame["turnover"] = pd.to_numeric(frame["c"], errors="coerce") * pd.to_numeric(frame["v"], errors="coerce")
    frame["turnover20"] = g["turnover"].transform(lambda s: s.rolling(20, min_periods=20).mean())

    frame["prev_open"] = g["o"].shift(1)
    frame["prev_close"] = g["c"].shift(1)
    frame["prev_high"] = g["h"].shift(1)
    frame["prev_low"] = g["l"].shift(1)
    frame["prev_ma7"] = g["ma7"].shift(1)
    frame["prev_ma20"] = g["ma20"].shift(1)

    tr1 = (frame["h"] - frame["l"]).abs()
    tr2 = (frame["h"] - frame["prev_close"]).abs()
    tr3 = (frame["l"] - frame["prev_close"]).abs()
    frame["tr"] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    frame["atr14"] = g["tr"].transform(lambda s: s.rolling(14, min_periods=14).mean())

    shifted_h = g["h"].shift(1)
    shifted_l = g["l"].shift(1)
    frame["prev20_high"] = shifted_h.groupby(frame["code"]).transform(
        lambda s: s.rolling(20, min_periods=20).max()
    )
    frame["prev20_low"] = shifted_l.groupby(frame["code"]).transform(
        lambda s: s.rolling(20, min_periods=20).min()
    )
    range20_width = frame["prev20_high"] - frame["prev20_low"]
    frame["range20_width"] = range20_width
    frame["range20_width_pct"] = range20_width / frame["ma20"]
    frame["range20_pos"] = (frame["c"] - frame["prev20_low"]) / range20_width.replace(0, np.nan)
    frame["range20_pct"] = (
        frame["h"].groupby(frame["code"]).transform(lambda s: s.rolling(20, min_periods=20).max())
        - frame["l"].groupby(frame["code"]).transform(lambda s: s.rolling(20, min_periods=20).min())
    ) / frame["ma20"]

    frame["ma20_slope5"] = frame["ma20"] - g["ma20"].shift(5)

    body = (frame["c"] - frame["o"]).abs()
    lower_wick = (np.minimum(frame["c"], frame["o"]) - frame["l"]).clip(lower=0)
    upper_wick = (frame["h"] - np.maximum(frame["c"], frame["o"])).clip(lower=0)
    frame["body"] = body
    frame["lower_wick"] = lower_wick
    frame["upper_wick"] = upper_wick

    frame["below7"] = frame["c"] < frame["ma7"]
    frame["below20"] = frame["c"] < frame["ma20"]
    frame["below60"] = frame["c"] < frame["ma60"]
    frame["above7"] = frame["c"] > frame["ma7"]
    frame["above20"] = frame["c"] > frame["ma20"]
    frame["above60"] = frame["c"] > frame["ma60"]

    for col in ("below7", "below20", "below60", "above7", "above20", "above60"):
        frame[f"{col}_streak"] = g[col].transform(_build_streak)

    frame["above20_count20"] = g["above20"].transform(lambda s: s.rolling(20, min_periods=1).sum())
    frame["below20_count20"] = g["below20"].transform(lambda s: s.rolling(20, min_periods=1).sum())
    frame["above60_count60"] = g["above60"].transform(lambda s: s.rolling(60, min_periods=1).sum())
    frame["below60_count60"] = g["below60"].transform(lambda s: s.rolling(60, min_periods=1).sum())

    frame["touch20"] = ((frame["l"] <= frame["ma20"]) & (frame["h"] >= frame["ma20"])).fillna(False)
    frame["touch20_20"] = g["touch20"].transform(lambda s: s.rolling(20, min_periods=1).sum())
    frame["ma20_band"] = (frame["c"] - frame["ma20"]).abs() / frame["ma20"] <= 0.03
    frame["ma20_band_20"] = g["ma20_band"].transform(lambda s: s.rolling(20, min_periods=1).sum())
    frame["sideways_10_20"] = (frame["ma20_band_20"] >= 10) & (frame["range20_pct"] <= 0.18)
    frame["range_bias_long"] = (
        (frame["range20_pos"] >= range_pos_long_min) & (frame["range20_width_pct"] >= range_width_min)
    ).fillna(False)
    frame["range_bias_short"] = (
        (frame["range20_pos"] <= range_pos_short_max) & (frame["range20_width_pct"] >= range_width_min)
    ).fillna(False)
    frame["ma_up_persist_long"] = (
        (frame["above20_count20"] >= ma20_count20_min_long) & (frame["above60_count60"] >= ma60_count60_min_long)
    ).fillna(False)
    frame["ma_up_persist_short"] = (
        (frame["below20_count20"] >= ma20_count20_min_short)
        & (frame["below60_count60"] >= ma60_count60_min_short)
    ).fillna(False)

    # B/G/M (down)
    frame["flag_bear_big"] = (
        (frame["c"] < frame["o"])
        & (frame["body"] >= 0.8 * frame["atr14"])
        & (frame["lower_wick"] <= 0.25 * frame["body"].replace(0, np.nan))
    ).fillna(False)
    frame["flag_gap_down"] = (frame["prev_close"] - frame["o"] >= 0.5 * frame["atr14"]).fillna(False)
    frame["flag_m_down"] = frame["c"] < frame["ma7"]
    frame["bgm_down_score"] = (
        frame["flag_bear_big"].astype(int)
        + frame["flag_gap_down"].astype(int)
        + frame["flag_m_down"].astype(int)
    )
    frame["decision_down"] = (
        (frame["flag_bear_big"] & frame["flag_gap_down"]) | (frame["bgm_down_score"] >= 2)
    ).fillna(False)

    # Up-side symmetrical cues.
    frame["flag_bull_big"] = (
        (frame["c"] > frame["o"])
        & (frame["body"] >= 0.8 * frame["atr14"])
        & (frame["upper_wick"] <= 0.25 * frame["body"].replace(0, np.nan))
    ).fillna(False)
    frame["flag_gap_up"] = (frame["o"] - frame["prev_close"] >= 0.5 * frame["atr14"]).fillna(False)
    frame["flag_m_up"] = frame["c"] > frame["ma7"]
    frame["bgm_up_score"] = (
        frame["flag_bull_big"].astype(int)
        + frame["flag_gap_up"].astype(int)
        + frame["flag_m_up"].astype(int)
    )
    frame["decision_up"] = ((frame["flag_bull_big"] & frame["flag_gap_up"]) | (frame["bgm_up_score"] >= 2)).fillna(False)

    frame["hammer_like"] = (
        (frame["lower_wick"] >= 1.2 * frame["body"].replace(0, np.nan))
        & (frame["c"] > frame["o"])
        & (frame["upper_wick"] <= frame["body"] * 1.2)
    ).fillna(False)
    frame["bull_engulf"] = (
        (frame["c"] > frame["o"])
        & (frame["prev_close"] < frame["prev_open"])
        & (frame["c"] >= frame["prev_open"])
        & (frame["o"] <= frame["prev_close"])
    ).fillna(False)
    frame["reversal_up"] = frame["hammer_like"] | frame["bull_engulf"]

    frame["retest_fail_short"] = (
        (frame["prev_close"] >= frame["prev_ma7"] * 0.995)
        & (frame["prev_close"] <= frame["prev_ma20"] * 1.01)
        & (frame["c"] < frame["o"])
        & (frame["c"] < frame["ma7"])
    ).fillna(False)
    frame["retest_success_long"] = (
        (frame["prev_close"] <= frame["prev_ma20"] * 1.01)
        & (frame["c"] > frame["o"])
        & (frame["c"] > frame["ma20"])
    ).fillna(False)

    # Sell patterns (Phase 1 approximation).
    frame["sell_p1"] = (
        (frame["c"] < frame["ma60"])
        & (frame["above7_streak"] >= 7)
        & (frame["c"] >= frame["prev20_high"] * 0.995)
        & (frame["c"] < frame["prev_low"])
        & (frame["below7_streak"] >= 2)
    ).fillna(False)
    frame["sell_p2"] = (
        (frame["c"] < frame["ma60"])
        & frame["sideways_10_20"]
        & (frame["c"] >= frame["prev20_high"] * 0.995)
        & (frame["c"] < frame["o"])
        & (frame["c"] < frame["ma7"])
    ).fillna(False)
    frame["sell_p3"] = (
        (frame["c"] > frame["ma60"])
        & (frame["touch20_20"] >= 3)
        & ((frame["below20_streak"] >= 2) | (frame["flag_bear_big"] & (frame["c"] < frame["prev_low"])))
    ).fillna(False)
    frame["sell_p4"] = (
        (frame["c"] < frame["ma60"])
        & (frame["below20_streak"] >= 3)
        & frame["retest_fail_short"]
    ).fillna(False)
    frame["sell_p5"] = (
        (frame["c"] < frame["ma60"])
        & (frame["above20_streak"] >= 10)
        & (frame["below20_streak"] >= 2)
    ).fillna(False)

    # Buy patterns (Phase 1 approximation).
    dist60 = (frame["c"] - frame["ma60"]).abs() / frame["ma60"]
    dist100 = (frame["c"] - frame["ma100"]).abs() / frame["ma100"]
    dist200 = (frame["c"] - frame["ma200"]).abs() / frame["ma200"]
    near_support = (dist60 <= 0.02) | (dist100 <= 0.02) | (dist200 <= 0.02)
    frame["buy_p1"] = (
        near_support
        & frame["reversal_up"]
        & ((frame["c"] > frame["ma7"]) | (frame["c"] > frame["ma20"]))
    ).fillna(False)
    frame["buy_p2"] = (
        (frame["c"] > frame["prev20_high"])
        & (frame["c"] > frame["ma20"])
        & ((frame["v"] >= 1.2 * frame["vol_ma20"]) | frame["vol_ma20"].isna())
    ).fillna(False)
    frame["buy_p3"] = (
        (frame["ma20_slope5"] > 0)
        & (frame["c"] > frame["ma20"])
        & (
            frame["below7_streak"].between(1, 2)
            | ((frame["prev_close"] < frame["prev_ma7"]) & (frame["c"] > frame["ma7"]))
        )
    ).fillna(False)

    frame["short_score"] = (
        frame["sell_p1"].astype(int) * 4
        + frame["sell_p2"].astype(int) * 3
        + frame["sell_p3"].astype(int) * 3
        + frame["sell_p4"].astype(int) * 2
        + frame["sell_p5"].astype(int) * 2
        + frame["decision_down"].astype(int)
        + frame["range_bias_short"].astype(int)
        + frame["ma_up_persist_short"].astype(int)
    )
    frame["long_score"] = (
        frame["buy_p1"].astype(int) * 4
        + frame["buy_p2"].astype(int) * 3
        + frame["buy_p3"].astype(int) * 2
        + frame["decision_up"].astype(int)
        + frame["range_bias_long"].astype(int)
        + frame["ma_up_persist_long"].astype(int)
    )

    frame["entry_short"] = (
        frame["sell_p1"] | frame["sell_p2"] | frame["sell_p3"] | frame["sell_p4"] | frame["sell_p5"]
    ) & frame["decision_down"]
    frame["entry_long"] = frame["buy_p1"] | frame["buy_p2"] | frame["buy_p3"]

    frame["dt_date"] = frame["dt"].apply(_dt_to_date)
    frame["signal_ready"] = frame["atr14"].notna() & frame["ma20"].notna() & frame["ma60"].notna()
    return frame


def _load_event_rows(conn) -> tuple[list[tuple[str, date]], list[str]]:
    rows: list[tuple[str, date]] = []
    notes: list[str] = []

    for table_name, date_col in (("earnings_planned", "planned_date"), ("ex_rights", "ex_date")):
        if not _table_exists(conn, table_name):
            notes.append(f"{table_name}:table_missing")
            continue
        try:
            q = f"SELECT code, {date_col} FROM {table_name}"
            fetched = conn.execute(q).fetchall()
        except Exception as exc:
            notes.append(f"{table_name}:query_failed:{exc}")
            continue
        valid = 0
        for code, raw_dt in fetched:
            code_text = str(code).strip() if code is not None else ""
            if not code_text:
                continue
            d = _parse_date_text(raw_dt)
            if d is None:
                continue
            rows.append((code_text, d))
            valid += 1
        notes.append(f"{table_name}:rows={valid}")
    return rows, notes


def _build_event_block_set(
    frame: pd.DataFrame,
    event_rows: list[tuple[str, date]],
    *,
    lookback_days: int,
    lookahead_days: int,
) -> set[tuple[str, date]]:
    if frame.empty or not event_rows:
        return set()
    code_dates: dict[str, list[date]] = {}
    for code, g in frame.groupby("code", sort=False):
        dates = [d for d in g["dt_date"].tolist() if isinstance(d, date)]
        if not dates:
            continue
        code_dates[str(code)] = sorted(set(dates))

    blocked: set[tuple[str, date]] = set()
    lb = max(0, int(lookback_days))
    la = max(0, int(lookahead_days))
    for code, event_d in event_rows:
        dates = code_dates.get(code)
        if not dates:
            continue
        idx = bisect.bisect_left(dates, event_d)
        if idx >= len(dates):
            idx = len(dates) - 1
        elif dates[idx] != event_d and idx > 0:
            prev_idx = idx - 1
            cur_delta = abs((dates[idx] - event_d).days)
            prev_delta = abs((dates[prev_idx] - event_d).days)
            if prev_delta <= cur_delta:
                idx = prev_idx
        start = max(0, idx - lb)
        end = min(len(dates), idx + la + 1)
        for i in range(start, end):
            blocked.add((code, dates[i]))
    return blocked


def _select_worst_open_code(
    open_positions: dict[str, OpenPosition],
    day_map: dict[str, dict[str, Any]],
) -> str | None:
    worst_code: str | None = None
    worst_score: float | None = None
    for code, pos in open_positions.items():
        row = day_map.get(code)
        score = 0.0
        if row:
            score = float(row["long_score"] if pos.side == "long" else row["short_score"])
        if worst_code is None or score < float(worst_score or 0.0):
            worst_code = code
            worst_score = score
    return worst_code


def _simulate(
    frame: pd.DataFrame,
    cfg: StrategyBacktestConfig,
    event_block_set: set[tuple[str, date]],
    *,
    include_trade_events: bool = False,
) -> dict[str, Any]:
    open_positions: dict[str, OpenPosition] = {}
    trade_events: list[dict[str, Any]] = []
    daily_rows: list[dict[str, Any]] = []
    monthly_new_entries: dict[str, int] = {}

    cum_realized = 0.0
    latest_price: dict[str, float] = {}
    latest_turnover20: dict[str, float | None] = {}
    latest_dt: int | None = None
    latest_dt_date: date | None = None

    grouped = frame.groupby("dt", sort=True)
    allow_long_setup_set = (
        {str(s).strip() for s in cfg.allowed_long_setups if str(s).strip()}
        if cfg.allowed_long_setups
        else None
    )
    allow_short_setup_set = (
        {str(s).strip() for s in cfg.allowed_short_setups if str(s).strip()}
        if cfg.allowed_short_setups
        else None
    )
    breadth_history: list[float] = []
    for dt_value, day_df in grouped:
        dt = int(dt_value)
        records = day_df.to_dict(orient="records")
        day_map: dict[str, dict[str, Any]] = {str(r["code"]): r for r in records}
        latest_dt = dt
        latest_dt_date = records[0].get("dt_date") if records else None
        for r in records:
            latest_price[str(r["code"])] = float(r["c"])
            latest_turnover20[str(r["code"])] = _safe_float(r.get("turnover20"))

        day_realized = 0.0
        month_key = latest_dt_date.strftime("%Y-%m") if isinstance(latest_dt_date, date) else None
        monthly_limit = (
            int(cfg.max_new_entries_per_month) if cfg.max_new_entries_per_month is not None else None
        )
        breadth_raw = None
        ready_rows = [r for r in records if bool(r.get("signal_ready"))]
        breadth_base = ready_rows if ready_rows else records
        if breadth_base:
            breadth_raw = float(sum(1 for r in breadth_base if bool(r.get("above60"))) / len(breadth_base))
            breadth_history.append(float(breadth_raw))
        lb_days = max(1, int(cfg.regime_breadth_lookback_days))
        breadth_smoothed = (
            float(sum(breadth_history[-lb_days:]) / len(breadth_history[-lb_days:])) if breadth_history else None
        )
        regime_allow_long = True
        regime_allow_short = True
        if bool(cfg.use_regime_filter) and breadth_smoothed is not None:
            regime_allow_long = float(breadth_smoothed) >= float(cfg.regime_long_min_breadth_above60)
            regime_allow_short = float(breadth_smoothed) <= float(cfg.regime_short_max_breadth_above60)

        # 1) Manage existing positions: stop / take-profit / add.
        for code in list(open_positions.keys()):
            pos = open_positions[code]
            row = day_map.get(code)
            if row is None:
                continue
            price = float(row["c"])
            dt_date = row.get("dt_date")
            ma7 = _safe_float(row.get("ma7"))
            ma20 = _safe_float(row.get("ma20"))
            atr14 = _safe_float(row.get("atr14"))
            prev_low = _safe_float(row.get("prev_low"))
            prev_high = _safe_float(row.get("prev_high"))

            ret_now = _position_return(pos.side, float(pos.entry_price), float(price))

            should_exit = False
            exit_reason = ""
            if pos.side == "short":
                if int(row.get("above7_streak") or 0) >= 2:
                    should_exit = True
                    exit_reason = "stop_short_7ma_reclaim"
                elif ma20 is not None and price > ma20 and bool(row.get("decision_up")):
                    should_exit = True
                    exit_reason = "stop_short_ma20_retake"
            else:
                if int(row.get("below7_streak") or 0) >= 2:
                    should_exit = True
                    exit_reason = "stop_long_7ma_break"
                elif ma20 is not None and price < ma20 and bool(row.get("decision_down")):
                    should_exit = True
                    exit_reason = "stop_long_ma20_break"

            if should_exit:
                ev, pnl = _close_units(
                    pos,
                    exit_price=price,
                    qty=int(pos.units),
                    dt=dt,
                    dt_date=dt_date,
                    reason=exit_reason,
                    base_cost_rate=cfg.cost_rate,
                    turnover20=_safe_float(row.get("turnover20")),
                )
                trade_events.append(ev)
                day_realized += float(pnl)
                del open_positions[code]
                continue

            atr_ret = (atr14 / price) if (atr14 is not None and price > 0) else None
            if (not pos.half_taken) and (atr_ret is not None) and (ret_now >= atr_ret):
                qty = max(1, int(round(pos.units * 0.5)))
                ev, pnl = _close_units(
                    pos,
                    exit_price=price,
                    qty=qty,
                    dt=dt,
                    dt_date=dt_date,
                    reason="take_profit_half",
                    base_cost_rate=cfg.cost_rate,
                    turnover20=_safe_float(row.get("turnover20")),
                )
                trade_events.append(ev)
                day_realized += float(pnl)
                pos.half_taken = True
                if pos.units <= 0:
                    del open_positions[code]
                    continue

            if pos.half_taken:
                if pos.side == "short" and ma7 is not None and price >= ma7:
                    ev, pnl = _close_units(
                        pos,
                        exit_price=price,
                        qty=int(pos.units),
                        dt=dt,
                        dt_date=dt_date,
                        reason="take_profit_short_7ma_touch",
                        base_cost_rate=cfg.cost_rate,
                        turnover20=_safe_float(row.get("turnover20")),
                    )
                    trade_events.append(ev)
                    day_realized += float(pnl)
                    del open_positions[code]
                    continue
                if pos.side == "long" and ma7 is not None and price <= ma7:
                    ev, pnl = _close_units(
                        pos,
                        exit_price=price,
                        qty=int(pos.units),
                        dt=dt,
                        dt_date=dt_date,
                        reason="take_profit_long_7ma_break",
                        base_cost_rate=cfg.cost_rate,
                        turnover20=_safe_float(row.get("turnover20")),
                    )
                    trade_events.append(ev)
                    day_realized += float(pnl)
                    del open_positions[code]
                    continue

            if not pos.is_hedge and pos.add_step < 2:
                if pos.side == "short":
                    cond_add1 = bool(row.get("decision_down")) or (prev_low is not None and price < prev_low)
                    cond_add2 = bool(row.get("retest_fail_short")) or bool(row.get("sell_p5"))
                    if pos.add_step == 0 and cond_add1:
                        _add_units(pos, cfg.add1_units, price)
                    elif pos.add_step == 1 and cond_add2:
                        _add_units(pos, cfg.add2_units, price)
                else:
                    cond_add1 = bool(row.get("buy_p2")) or (prev_high is not None and price > prev_high)
                    cond_add2 = bool(row.get("retest_success_long")) or (
                        prev_high is not None and price > prev_high * 1.01
                    )
                    if pos.add_step == 0 and cond_add1:
                        _add_units(pos, cfg.add1_units, price)
                    elif pos.add_step == 1 and cond_add2:
                        _add_units(pos, cfg.add2_units, price)

        # 2) Entry candidates.
        open_codes = set(open_positions.keys())
        short_sectors = {
            str(p.sector33_code)
            for p in open_positions.values()
            if p.side == "short" and p.sector33_code is not None and str(p.sector33_code)
        }
        long_candidates: list[dict[str, Any]] = []
        short_candidates: list[dict[str, Any]] = []

        for row in records:
            code = str(row["code"])
            if code in open_codes:
                continue
            if not bool(row.get("signal_ready")):
                continue
            dt_date = row.get("dt_date")
            if isinstance(dt_date, date) and (code, dt_date) in event_block_set:
                continue

            long_score = float(row.get("long_score") or 0.0)
            short_score = float(row.get("short_score") or 0.0)
            c_price = _safe_float(row.get("c"))
            ma20_v = _safe_float(row.get("ma20"))
            ma60_v = _safe_float(row.get("ma60"))
            vol_v = _safe_float(row.get("v"))
            vol_ma20_v = _safe_float(row.get("vol_ma20"))
            atr14_v = _safe_float(row.get("atr14"))
            ml_p_up_v = _safe_float(row.get("ml_p_up"))

            dist_ma20 = None
            if c_price is not None and ma20_v is not None and ma20_v > 0:
                dist_ma20 = (float(c_price) - float(ma20_v)) / float(ma20_v)
            vol_ratio = None
            if vol_v is not None and vol_ma20_v is not None and vol_ma20_v > 0:
                vol_ratio = float(vol_v) / float(vol_ma20_v)
            atr_pct = None
            if atr14_v is not None and c_price is not None and c_price > 0:
                atr_pct = float(atr14_v) / float(c_price)
            allow_long = str(cfg.allowed_sides).lower() in ("both", "long") and bool(regime_allow_long)
            allow_short = str(cfg.allowed_sides).lower() in ("both", "short") and bool(regime_allow_short)

            long_extra_ok = True
            if cfg.require_ma_bull_stack_long:
                long_extra_ok = long_extra_ok and (
                    c_price is not None
                    and ma20_v is not None
                    and ma60_v is not None
                    and float(c_price) > float(ma20_v)
                    and float(ma20_v) > float(ma60_v)
                )
            if cfg.max_dist_ma20_long is not None:
                long_extra_ok = long_extra_ok and (
                    dist_ma20 is not None and float(dist_ma20) <= float(cfg.max_dist_ma20_long)
                )
            if float(cfg.min_volume_ratio_long) > 0:
                long_extra_ok = long_extra_ok and (
                    vol_ratio is not None and float(vol_ratio) >= float(cfg.min_volume_ratio_long)
                )
            if cfg.max_atr_pct_long is not None:
                long_extra_ok = long_extra_ok and (
                    atr_pct is not None and float(atr_pct) <= float(cfg.max_atr_pct_long)
                )
            if cfg.min_ml_p_up_long is not None:
                long_extra_ok = long_extra_ok and (
                    ml_p_up_v is not None and float(ml_p_up_v) >= float(cfg.min_ml_p_up_long)
                )

            if (
                allow_long
                and (
                    bool(row.get("entry_long"))
                    or (
                        bool(cfg.allow_decision_only_long_entries)
                        and bool(row.get("decision_up"))
                    )
                )
                and long_score >= float(cfg.min_long_score)
                and (not cfg.require_decision_for_long or bool(row.get("decision_up")))
                and long_extra_ok
            ):
                setup_id = _entry_setup_id(row, "long")
                if allow_long_setup_set is not None and setup_id not in allow_long_setup_set:
                    continue
                long_candidates.append(
                    {
                        "side": "long",
                        "score": long_score,
                        "row": row,
                        "setup_id": setup_id,
                    }
                )
            if allow_short and bool(row.get("entry_short")) and short_score >= float(cfg.min_short_score):
                sector = row.get("sector33_code")
                sector_text = str(sector) if sector is not None else None
                if sector_text and sector_text in short_sectors:
                    continue
                setup_id = _entry_setup_id(row, "short")
                if allow_short_setup_set is not None and setup_id not in allow_short_setup_set:
                    continue
                short_candidates.append(
                    {
                        "side": "short",
                        "score": short_score,
                        "row": row,
                        "setup_id": setup_id,
                    }
                )

        long_units = sum(p.units for p in open_positions.values() if p.side == "long")
        short_units = sum(p.units for p in open_positions.values() if p.side == "short")
        short_needs_bonus = short_units < cfg.prefer_net_short_ratio * max(1, long_units)
        long_needs_bonus = short_units > cfg.prefer_net_short_ratio * max(1, max(long_units, 1))

        combined = []
        for c in long_candidates:
            score = float(c["score"]) + (0.25 if long_needs_bonus else 0.0)
            combined.append((score, c))
        for c in short_candidates:
            score = float(c["score"]) + (0.25 if short_needs_bonus else 0.0)
            combined.append((score, c))
        combined.sort(key=lambda x: (-float(x[0]), str(x[1]["row"]["code"])))

        def _open_candidate(candidate: dict[str, Any], *, is_hedge: bool, units: int) -> bool:
            nonlocal open_codes
            row = candidate["row"]
            code = str(row["code"])
            if code in open_positions:
                return False
            if len(open_positions) >= cfg.max_positions:
                return False
            if (not is_hedge) and monthly_limit is not None and month_key is not None:
                used = int(monthly_new_entries.get(month_key, 0))
                if used >= monthly_limit:
                    return False
            side = str(candidate["side"])
            if side == "short":
                sector = row.get("sector33_code")
                sector_text = str(sector) if sector is not None else None
                if sector_text and any(
                    p.side == "short" and str(p.sector33_code or "") == sector_text
                    for p in open_positions.values()
                ):
                    return False
            open_positions[code] = OpenPosition(
                code=code,
                side=side,
                units=max(1, int(units)),
                entry_price=float(row["c"]),
                entry_dt=dt,
                add_step=0 if not is_hedge else -1,
                half_taken=False,
                is_hedge=bool(is_hedge),
                sector33_code=str(row.get("sector33_code")) if row.get("sector33_code") is not None else None,
                entry_score=float(candidate["score"]),
                setup_id=str(candidate.get("setup_id") or _entry_setup_id(row, side)),
            )
            if (not is_hedge) and month_key is not None:
                monthly_new_entries[month_key] = int(monthly_new_entries.get(month_key, 0)) + 1
            open_codes = set(open_positions.keys())
            return True

        slots = max(0, cfg.max_positions - len(open_positions))
        slots = min(int(slots), int(max(0, cfg.max_new_entries_per_day)))
        used_codes: set[str] = set()
        for _, cand in combined:
            if slots <= 0:
                break
            if monthly_limit is not None and month_key is not None:
                if int(monthly_new_entries.get(month_key, 0)) >= monthly_limit:
                    break
            code = str(cand["row"]["code"])
            if code in used_codes:
                continue
            if _open_candidate(cand, is_hedge=False, units=cfg.initial_units):
                used_codes.add(code)
                slots -= 1

        # Rotation when already full and stronger candidate appears.
        if len(open_positions) >= cfg.max_positions and combined:
            best_score, best_cand = combined[0]
            best_code = str(best_cand["row"]["code"])
            if best_code not in open_positions:
                worst_code = _select_worst_open_code(open_positions, day_map)
                if worst_code is not None:
                    worst_row = day_map.get(worst_code)
                    worst_signal = 0.0
                    if worst_row:
                        side = open_positions[worst_code].side
                        worst_signal = float(worst_row["long_score"] if side == "long" else worst_row["short_score"])
                    if float(best_score) >= float(worst_signal) + 1.0:
                        if (
                            monthly_limit is not None
                            and month_key is not None
                            and int(monthly_new_entries.get(month_key, 0)) >= monthly_limit
                        ):
                            continue
                        worst_pos = open_positions[worst_code]
                        if worst_row:
                            ev, pnl = _close_units(
                                worst_pos,
                                exit_price=float(worst_row["c"]),
                                qty=int(worst_pos.units),
                                dt=dt,
                                dt_date=worst_row.get("dt_date"),
                                reason="rotation",
                                base_cost_rate=cfg.cost_rate,
                                turnover20=_safe_float(worst_row.get("turnover20")),
                            )
                            trade_events.append(ev)
                            day_realized += float(pnl)
                            del open_positions[worst_code]
                            _open_candidate(best_cand, is_hedge=False, units=cfg.initial_units)

        # 3) Hedge minimum 1/5.
        long_units = sum(p.units for p in open_positions.values() if p.side == "long")
        short_units = sum(p.units for p in open_positions.values() if p.side == "short")

        need_short = int(math.ceil(float(long_units) * float(cfg.min_hedge_ratio))) if long_units > 0 else 0
        need_long = int(math.ceil(float(short_units) * float(cfg.min_hedge_ratio))) if short_units > 0 else 0
        slots = max(0, cfg.max_positions - len(open_positions))

        if short_units < need_short and slots > 0:
            for cand in sorted(short_candidates, key=lambda x: (-float(x["score"]), str(x["row"]["code"]))):
                if slots <= 0 or short_units >= need_short:
                    break
                code = str(cand["row"]["code"])
                if code in open_positions:
                    continue
                if _open_candidate(cand, is_hedge=True, units=cfg.hedge_units):
                    slots -= 1
                    short_units += cfg.hedge_units

        if long_units < need_long and slots > 0:
            for cand in sorted(long_candidates, key=lambda x: (-float(x["score"]), str(x["row"]["code"]))):
                if slots <= 0 or long_units >= need_long:
                    break
                code = str(cand["row"]["code"])
                if code in open_positions:
                    continue
                if _open_candidate(cand, is_hedge=True, units=cfg.hedge_units):
                    slots -= 1
                    long_units += cfg.hedge_units

        # 4) Daily mark-to-market.
        unrealized = 0.0
        for code, pos in open_positions.items():
            row = day_map.get(code)
            if row is None:
                continue
            price = float(row["c"])
            r = _position_return(pos.side, float(pos.entry_price), price)
            unrealized += float(r) * float(pos.units)

        cum_realized += float(day_realized)
        equity = float(cum_realized + unrealized)
        daily_rows.append(
            {
                "dt": int(dt),
                "date": latest_dt_date.isoformat() if isinstance(latest_dt_date, date) else None,
                "realized_unit_pnl": float(day_realized),
                "equity_unit": float(equity),
                "open_positions": int(len(open_positions)),
                "open_units_long": int(sum(p.units for p in open_positions.values() if p.side == "long")),
                "open_units_short": int(sum(p.units for p in open_positions.values() if p.side == "short")),
                "regime_breadth_above60": float(breadth_smoothed) if breadth_smoothed is not None else None,
            }
        )

    # Force close remaining positions on the last available close.
    if latest_dt is not None:
        for code in list(open_positions.keys()):
            pos = open_positions[code]
            if code not in latest_price:
                continue
            ev, pnl = _close_units(
                pos,
                exit_price=float(latest_price[code]),
                qty=int(pos.units),
                dt=int(latest_dt),
                dt_date=latest_dt_date,
                reason="forced_close_last_day",
                base_cost_rate=cfg.cost_rate,
                turnover20=latest_turnover20.get(code),
            )
            trade_events.append(ev)
            cum_realized += float(pnl)
            if daily_rows:
                daily_rows[-1]["realized_unit_pnl"] = float(daily_rows[-1]["realized_unit_pnl"]) + float(pnl)
                daily_rows[-1]["equity_unit"] = float(daily_rows[-1]["equity_unit"]) + float(pnl)
            del open_positions[code]

    trades_df = pd.DataFrame(trade_events)
    daily_df = pd.DataFrame(daily_rows)

    if not daily_df.empty:
        eq = daily_df["equity_unit"].astype(float)
        dd = eq - eq.cummax()
        max_dd = float(dd.min())
        daily_win_rate = float((daily_df["realized_unit_pnl"].astype(float) > 0).mean())
        monthly_df = daily_df.copy()
        monthly_df["month"] = pd.to_datetime(monthly_df["date"], errors="coerce").dt.strftime("%Y-%m")
        monthly_rows = (
            monthly_df.dropna(subset=["month"])
            .groupby("month", as_index=False)["realized_unit_pnl"]
            .sum()
            .sort_values("month")
        )
        monthly_payload = [
            {
                "month": str(r["month"]),
                "realized_unit_pnl": float(r["realized_unit_pnl"]),
            }
            for _, r in monthly_rows.iterrows()
        ]
        yearly_daily_rows = (
            monthly_df.assign(year=pd.to_datetime(monthly_df["date"], errors="coerce").dt.strftime("%Y"))
            .dropna(subset=["year"])
            .groupby("year", as_index=False)["realized_unit_pnl"]
            .sum()
            .sort_values("year")
        )
        yearly_daily_payload = [
            {"year": str(r["year"]), "realized_unit_pnl": float(r["realized_unit_pnl"])}
            for _, r in yearly_daily_rows.iterrows()
        ]
    else:
        max_dd = 0.0
        daily_win_rate = 0.0
        monthly_payload = []
        yearly_daily_payload = []

    if trades_df.empty:
        trade_count = 0
        win_rate = 0.0
        avg_ret = 0.0
        pos_sum = 0.0
        neg_sum = 0.0
        unit_turnover = 0.0
        total_realized = float(cum_realized)
        side_breakdown: dict[str, dict[str, Any]] = {}
        setup_breakdown: dict[str, dict[str, Any]] = {}
        code_breakdown: dict[str, dict[str, Any]] = {}
        sector_breakdown: dict[str, dict[str, Any]] = {}
        hedge_breakdown: dict[str, dict[str, Any]] = {}
        yearly_trade_payload: list[dict[str, Any]] = []
    else:
        trades_df["ret_net"] = trades_df["ret_net"].astype(float)
        trades_df["qty"] = trades_df["qty"].astype(float)
        trades_df["is_hedge"] = trades_df["is_hedge"].astype(bool)
        trades_df["hedge_bucket"] = trades_df["is_hedge"].map(lambda v: "hedge" if bool(v) else "core")
        trade_count = int(len(trades_df))
        win_rate = float((trades_df["ret_net"] > 0).mean())
        avg_ret = float(trades_df["ret_net"].mean())
        pos_sum = float(trades_df.loc[trades_df["ret_net"] > 0, "ret_net"].sum())
        neg_sum = float(trades_df.loc[trades_df["ret_net"] < 0, "ret_net"].sum())
        unit_turnover = float(trades_df["qty"].sum())
        total_realized = float((trades_df["ret_net"] * trades_df["qty"]).sum())
        side_breakdown = _build_trade_group_breakdown(trades_df, group_col="side", unknown_label="unknown")
        setup_breakdown = _build_trade_group_breakdown(trades_df, group_col="setup_id", unknown_label="unknown")
        code_breakdown = _build_trade_group_breakdown(trades_df, group_col="code", unknown_label="unknown")
        sector_breakdown = _build_trade_group_breakdown(
            trades_df,
            group_col="sector33_code",
            unknown_label="unknown",
        )
        hedge_breakdown = _build_trade_group_breakdown(
            trades_df,
            group_col="hedge_bucket",
            unknown_label="unknown",
        )
        yearly_trade_payload = []
        if "exit_date" in trades_df.columns:
            trade_year_df = trades_df.copy()
            trade_year_df["year"] = pd.to_datetime(trade_year_df["exit_date"], errors="coerce").dt.strftime("%Y")
            trade_year_df = trade_year_df.dropna(subset=["year"])
            for year, g in trade_year_df.groupby("year"):
                pos_sum_y = float(g.loc[g["ret_net"] > 0, "ret_net"].sum())
                neg_sum_y = float(g.loc[g["ret_net"] < 0, "ret_net"].sum())
                pf_y = (pos_sum_y / abs(neg_sum_y)) if neg_sum_y < 0 else None
                yearly_trade_payload.append(
                    {
                        "year": str(year),
                        "trade_events": int(len(g)),
                        "win_rate": float((g["ret_net"] > 0).mean()),
                        "avg_ret_net": float(g["ret_net"].mean()),
                        "profit_factor": float(pf_y) if pf_y is not None else None,
                        "sum_ret_net": float((g["ret_net"] * g["qty"]).sum()),
                    }
                )
            yearly_trade_payload.sort(key=lambda x: str(x["year"]))

    profit_factor = (pos_sum / abs(neg_sum)) if neg_sum < 0 else None
    avg_ret_per_unit = (total_realized / unit_turnover) if unit_turnover > 0 else 0.0

    sample_trades = trade_events[-100:] if len(trade_events) > 100 else trade_events
    monthly_entry_payload = [
        {"month": str(month), "entries": int(cnt)}
        for month, cnt in sorted(monthly_new_entries.items(), key=lambda x: x[0])
    ]
    return {
        "metrics": {
            "days": int(len(daily_rows)),
            "trade_events": int(trade_count),
            "win_rate": float(win_rate),
            "avg_ret_net": float(avg_ret),
            "avg_ret_net_per_unit": float(avg_ret_per_unit),
            "profit_factor": float(profit_factor) if profit_factor is not None else None,
            "max_drawdown_unit": float(max_dd),
            "daily_win_rate": float(daily_win_rate),
                "total_realized_unit_pnl": float(total_realized),
                "final_equity_unit": float(daily_rows[-1]["equity_unit"]) if daily_rows else 0.0,
                "side_breakdown": side_breakdown,
                "setup_breakdown": setup_breakdown,
                "code_breakdown": code_breakdown,
                "sector_breakdown": sector_breakdown,
                "hedge_breakdown": hedge_breakdown,
            },
        "monthly": monthly_payload,
        "yearly_daily": yearly_daily_payload,
        "yearly_trades": yearly_trade_payload,
        "entry_monthly": monthly_entry_payload,
        "daily": daily_rows[-400:] if len(daily_rows) > 400 else daily_rows,
        "sample_trades": sample_trades,
        "trade_events": trade_events if include_trade_events else None,
    }


def _save_backtest_run(
    conn,
    *,
    run_id: str,
    started_at: datetime,
    finished_at: datetime,
    status: str,
    start_dt: int | None,
    end_dt: int | None,
    max_codes: int | None,
    config: StrategyBacktestConfig,
    result: dict[str, Any] | None,
    note: str | None,
) -> None:
    _ensure_backtest_schema(conn)
    metrics_payload = result or {}
    conn.execute(
        """
        INSERT INTO strategy_backtest_runs (
            run_id,
            started_at,
            finished_at,
            status,
            start_dt,
            end_dt,
            max_codes,
            config_json,
            metrics_json,
            note
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            started_at,
            finished_at,
            status,
            start_dt,
            end_dt,
            max_codes,
            json.dumps(asdict(config), ensure_ascii=False),
            json.dumps(metrics_payload, ensure_ascii=False),
            note,
        ],
    )


def run_strategy_backtest(
    *,
    start_dt: int | None = None,
    end_dt: int | None = None,
    max_codes: int | None = 500,
    dry_run: bool = False,
    config: StrategyBacktestConfig | None = None,
) -> dict[str, Any]:
    cfg = config or StrategyBacktestConfig()
    run_id = datetime.now(tz=timezone.utc).strftime("sbt_%Y%m%d%H%M%S_%f")
    started_at = datetime.now(tz=timezone.utc)

    with get_conn() as conn:
        _ensure_backtest_schema(conn)
        market = _load_market_frame(conn, start_dt=start_dt, end_dt=end_dt, max_codes=max_codes)
    if market.empty:
        raise RuntimeError("No daily_bars rows for backtest range")

    features = _prepare_feature_frame(market, cfg)
    features["bar_index"] = features.groupby("code", sort=False).cumcount() + 1
    features = features[
        (features["signal_ready"])
        & (features["bar_index"] >= int(max(1, cfg.min_history_bars)))
    ].copy()
    if features.empty:
        raise RuntimeError("No rows with enough history for signal computation")

    with get_conn() as conn:
        event_rows, event_notes = _load_event_rows(conn)
    event_block = _build_event_block_set(
        features,
        event_rows,
        lookback_days=cfg.event_lookback_days,
        lookahead_days=cfg.event_lookahead_days,
    )

    sim = _simulate(features, cfg, event_block)
    finished_at = datetime.now(tz=timezone.utc)
    result = {
        "run_id": run_id,
        "status": "success",
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "range": {
            "start_dt": int(features["dt"].min()),
            "end_dt": int(features["dt"].max()),
        },
        "dataset": {
            "rows": int(len(features)),
            "codes": int(features["code"].nunique()),
            "days": int(features["dt"].nunique()),
            "max_codes": int(max_codes) if max_codes is not None else None,
        },
        "event_filter": {
            "event_rows": int(len(event_rows)),
            "blocked_points": int(len(event_block)),
            "notes": event_notes,
        },
        "config": asdict(cfg),
        **sim,
    }

    if not dry_run:
        with get_conn() as conn:
            _save_backtest_run(
                conn,
                run_id=run_id,
                started_at=started_at,
                finished_at=finished_at,
                status="success",
                start_dt=start_dt,
                end_dt=end_dt,
                max_codes=max_codes,
                config=cfg,
                result=result,
                note=None,
            )
    result["dry_run"] = bool(dry_run)
    return result


def get_latest_strategy_backtest() -> dict[str, Any]:
    with get_conn() as conn:
        _ensure_backtest_schema(conn)
        row = conn.execute(
            """
            SELECT
                run_id,
                started_at,
                finished_at,
                status,
                start_dt,
                end_dt,
                max_codes,
                config_json,
                metrics_json,
                note
            FROM strategy_backtest_runs
            ORDER BY finished_at DESC
            LIMIT 1
            """
        ).fetchone()
    if not row:
        return {"has_run": False, "latest": None}

    try:
        config_json = json.loads(row[7]) if row[7] else {}
    except Exception:
        config_json = {}
    try:
        metrics_json = json.loads(row[8]) if row[8] else {}
    except Exception:
        metrics_json = {}
    return {
        "has_run": True,
        "latest": {
            "run_id": row[0],
            "started_at": row[1],
            "finished_at": row[2],
            "status": row[3],
            "start_dt": row[4],
            "end_dt": row[5],
            "max_codes": row[6],
            "config": config_json,
            "metrics": metrics_json,
            "note": row[9],
        },
    }


def _ensure_walkforward_schema(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS strategy_walkforward_runs (
            run_id TEXT PRIMARY KEY,
            started_at TIMESTAMP,
            finished_at TIMESTAMP,
            status TEXT,
            start_dt INTEGER,
            end_dt INTEGER,
            max_codes INTEGER,
            train_months INTEGER,
            test_months INTEGER,
            step_months INTEGER,
            config_json TEXT,
            report_json TEXT,
            note TEXT
        );
        """
    )


def _ensure_walkforward_gate_schema(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS strategy_walkforward_gate_reports (
            gate_id TEXT PRIMARY KEY,
            created_at TIMESTAMP,
            source_run_id TEXT,
            source_finished_at TIMESTAMP,
            status TEXT,
            thresholds_json TEXT,
            report_json TEXT,
            note TEXT
        );
        """
    )


def _ensure_walkforward_research_schema(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS strategy_walkforward_research_daily (
            snapshot_date INTEGER PRIMARY KEY,
            created_at TIMESTAMP,
            source_run_id TEXT,
            source_finished_at TIMESTAMP,
            report_json TEXT
        );
        """
    )


def _save_walkforward_gate_report(
    conn,
    *,
    gate_id: str,
    created_at: datetime,
    source_run_id: str,
    source_finished_at: datetime | None,
    status: str,
    thresholds: dict[str, Any],
    report: dict[str, Any],
    note: str | None,
) -> None:
    _ensure_walkforward_gate_schema(conn)
    conn.execute(
        """
        INSERT INTO strategy_walkforward_gate_reports (
            gate_id,
            created_at,
            source_run_id,
            source_finished_at,
            status,
            thresholds_json,
            report_json,
            note
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            gate_id,
            created_at,
            source_run_id,
            source_finished_at,
            status,
            json.dumps(thresholds, ensure_ascii=False),
            json.dumps(report, ensure_ascii=False),
            note,
        ],
    )


def _build_walkforward_gate_report(
    *,
    gate_id: str,
    created_at: datetime,
    source_run_id: str,
    source_finished_at: datetime | None,
    source_status: str | None,
    source_summary: dict[str, Any],
    source_windowing: dict[str, Any],
    min_oos_total_realized_unit_pnl: float,
    min_oos_mean_profit_factor: float,
    min_oos_positive_window_ratio: float,
    min_oos_worst_max_drawdown_unit: float,
    note: str | None,
) -> dict[str, Any]:
    oos_total = _safe_float(source_summary.get("oos_total_realized_unit_pnl"))
    oos_pf = _safe_float(source_summary.get("oos_mean_profit_factor"))
    oos_pos_ratio = _safe_float(source_summary.get("oos_positive_window_ratio"))
    oos_worst_dd = _safe_float(source_summary.get("oos_worst_max_drawdown_unit"))
    checks = {
        "oos_total_realized_unit_pnl": {
            "actual": oos_total,
            "threshold": float(min_oos_total_realized_unit_pnl),
            "pass": (oos_total is not None and oos_total >= float(min_oos_total_realized_unit_pnl)),
        },
        "oos_mean_profit_factor": {
            "actual": oos_pf,
            "threshold": float(min_oos_mean_profit_factor),
            "pass": (oos_pf is not None and oos_pf >= float(min_oos_mean_profit_factor)),
        },
        "oos_positive_window_ratio": {
            "actual": oos_pos_ratio,
            "threshold": float(min_oos_positive_window_ratio),
            "pass": (
                oos_pos_ratio is not None
                and oos_pos_ratio >= float(min_oos_positive_window_ratio)
            ),
        },
        "oos_worst_max_drawdown_unit": {
            "actual": oos_worst_dd,
            "threshold": float(min_oos_worst_max_drawdown_unit),
            "pass": (
                oos_worst_dd is not None
                and oos_worst_dd >= float(min_oos_worst_max_drawdown_unit)
            ),
        },
    }
    passed = all(bool(v.get("pass")) for v in checks.values())
    month_key = (
        source_finished_at.strftime("%Y-%m")
        if isinstance(source_finished_at, datetime)
        else datetime.now(tz=timezone.utc).strftime("%Y-%m")
    )
    return {
        "gate_id": gate_id,
        "created_at": created_at.isoformat(),
        "status": "pass" if passed else "fail",
        "passed": bool(passed),
        "month_key": month_key,
        "source": {
            "run_id": source_run_id,
            "finished_at": source_finished_at.isoformat() if isinstance(source_finished_at, datetime) else None,
            "status": source_status,
            "windowing": source_windowing,
            "summary": source_summary,
        },
        "thresholds": {
            "min_oos_total_realized_unit_pnl": float(min_oos_total_realized_unit_pnl),
            "min_oos_mean_profit_factor": float(min_oos_mean_profit_factor),
            "min_oos_positive_window_ratio": float(min_oos_positive_window_ratio),
            "min_oos_worst_max_drawdown_unit": float(min_oos_worst_max_drawdown_unit),
        },
        "checks": checks,
        "note": note,
    }


def _save_walkforward_run(
    conn,
    *,
    run_id: str,
    started_at: datetime,
    finished_at: datetime,
    status: str,
    start_dt: int | None,
    end_dt: int | None,
    max_codes: int | None,
    train_months: int,
    test_months: int,
    step_months: int,
    config: StrategyBacktestConfig,
    report: dict[str, Any],
    note: str | None,
) -> None:
    _ensure_walkforward_schema(conn)
    conn.execute(
        """
        INSERT INTO strategy_walkforward_runs (
            run_id,
            started_at,
            finished_at,
            status,
            start_dt,
            end_dt,
            max_codes,
            train_months,
            test_months,
            step_months,
            config_json,
            report_json,
            note
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            started_at,
            finished_at,
            status,
            start_dt,
            end_dt,
            max_codes,
            int(train_months),
            int(test_months),
            int(step_months),
            json.dumps(asdict(config), ensure_ascii=False),
            json.dumps(report, ensure_ascii=False),
            note,
        ],
    )


def _month_key_from_dt(dt_value: int) -> str | None:
    d = _dt_to_date(dt_value)
    if d is None:
        return None
    return d.strftime("%Y-%m")


def _build_month_segments(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame.empty or "dt" not in frame.columns:
        return []
    raw_dts = frame["dt"].dropna().tolist()
    dts = sorted({int(v) for v in raw_dts})
    segments: list[dict[str, Any]] = []
    for dt in dts:
        key = _month_key_from_dt(int(dt))
        if key is None:
            continue
        if segments and segments[-1]["month"] == key:
            segments[-1]["end_dt"] = int(dt)
            segments[-1]["days"] = int(segments[-1]["days"]) + 1
        else:
            segments.append(
                {
                    "month": key,
                    "start_dt": int(dt),
                    "end_dt": int(dt),
                    "days": 1,
                }
            )
    return segments


def _compact_metrics(payload: dict[str, Any]) -> dict[str, Any]:
    metrics = payload.get("metrics") or {}
    side_breakdown = metrics.get("side_breakdown") if isinstance(metrics.get("side_breakdown"), dict) else {}
    setup_breakdown = metrics.get("setup_breakdown") if isinstance(metrics.get("setup_breakdown"), dict) else {}
    code_breakdown = metrics.get("code_breakdown") if isinstance(metrics.get("code_breakdown"), dict) else {}
    sector_breakdown = metrics.get("sector_breakdown") if isinstance(metrics.get("sector_breakdown"), dict) else {}
    hedge_breakdown = metrics.get("hedge_breakdown") if isinstance(metrics.get("hedge_breakdown"), dict) else {}
    return {
        "days": int(metrics.get("days") or 0),
        "trade_events": int(metrics.get("trade_events") or 0),
        "win_rate": _safe_float(metrics.get("win_rate")),
        "avg_ret_net": _safe_float(metrics.get("avg_ret_net")),
        "profit_factor": _safe_float(metrics.get("profit_factor")),
        "max_drawdown_unit": _safe_float(metrics.get("max_drawdown_unit")),
        "total_realized_unit_pnl": _safe_float(metrics.get("total_realized_unit_pnl")),
        "final_equity_unit": _safe_float(metrics.get("final_equity_unit")),
        "side_breakdown": side_breakdown,
        "setup_breakdown": setup_breakdown,
        "code_breakdown": code_breakdown,
        "sector_breakdown": sector_breakdown,
        "hedge_breakdown": hedge_breakdown,
    }


def _aggregate_attribution_dimension(
    windows: list[dict[str, Any]],
    *,
    breakdown_key: str,
) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, float]] = {}
    for row in windows:
        if str(row.get("status")) != "success":
            continue
        metrics = (row.get("test") or {}).get("metrics") or {}
        breakdown = metrics.get(breakdown_key) if isinstance(metrics, dict) else None
        if not isinstance(breakdown, dict):
            continue
        for raw_key, raw_stats in breakdown.items():
            if not isinstance(raw_stats, dict):
                continue
            key = str(raw_key).strip() or "unknown"
            trades = int(raw_stats.get("trades") or raw_stats.get("count") or 0)
            if trades <= 0:
                continue
            wins_raw = raw_stats.get("wins")
            wins = int(wins_raw) if wins_raw is not None else int(round(float(raw_stats.get("win_rate") or 0.0) * trades))
            avg_ret = _safe_float(raw_stats.get("avg_ret_net")) or 0.0
            ret_net_sum = _safe_float(raw_stats.get("ret_net_sum"))
            if ret_net_sum is None:
                ret_net_sum = _safe_float(raw_stats.get("sum_ret_net")) or 0.0
            pos_ret_sum = _safe_float(raw_stats.get("pos_ret_sum")) or 0.0
            neg_ret_sum = _safe_float(raw_stats.get("neg_ret_sum")) or 0.0
            slot = grouped.setdefault(
                key,
                {
                    "trades": 0.0,
                    "wins": 0.0,
                    "ret_net_sum": 0.0,
                    "avg_ret_numer": 0.0,
                    "pos_ret_sum": 0.0,
                    "neg_ret_sum": 0.0,
                },
            )
            slot["trades"] += float(trades)
            slot["wins"] += float(max(0, min(trades, wins)))
            slot["ret_net_sum"] += float(ret_net_sum)
            slot["avg_ret_numer"] += float(avg_ret) * float(trades)
            slot["pos_ret_sum"] += float(pos_ret_sum)
            slot["neg_ret_sum"] += float(neg_ret_sum)

    rows: list[dict[str, Any]] = []
    for key, slot in grouped.items():
        trades = int(slot["trades"])
        wins = int(slot["wins"])
        avg_ret = (slot["avg_ret_numer"] / slot["trades"]) if slot["trades"] > 0 else 0.0
        neg_ret_sum = float(slot["neg_ret_sum"])
        pos_ret_sum = float(slot["pos_ret_sum"])
        profit_factor = (pos_ret_sum / abs(neg_ret_sum)) if neg_ret_sum < 0 else None
        rows.append(
            {
                "key": str(key),
                "trades": int(trades),
                "wins": int(wins),
                "win_rate": float(wins / trades) if trades > 0 else None,
                "ret_net_sum": float(slot["ret_net_sum"]),
                "avg_ret_net": float(avg_ret),
                "profit_factor": float(profit_factor) if profit_factor is not None else None,
            }
        )
    rows.sort(key=lambda item: (float(item.get("ret_net_sum") or 0.0), str(item.get("key") or "")), reverse=True)
    return rows


def _slice_top_bottom(rows: list[dict[str, Any]], *, limit: int = 8) -> dict[str, list[dict[str, Any]]]:
    safe_limit = max(1, int(limit))
    top = rows[:safe_limit]
    bottom_sorted = sorted(rows, key=lambda item: (float(item.get("ret_net_sum") or 0.0), str(item.get("key") or "")))
    bottom = bottom_sorted[:safe_limit]
    return {"top": top, "bottom": bottom}


def _build_walkforward_attribution(windows: list[dict[str, Any]]) -> dict[str, Any]:
    code_rows = _aggregate_attribution_dimension(windows, breakdown_key="code_breakdown")
    sector_rows = _aggregate_attribution_dimension(windows, breakdown_key="sector_breakdown")
    setup_rows = _aggregate_attribution_dimension(windows, breakdown_key="setup_breakdown")
    side_rows = _aggregate_attribution_dimension(windows, breakdown_key="side_breakdown")
    hedge_rows = _aggregate_attribution_dimension(windows, breakdown_key="hedge_breakdown")
    return {
        "code": {**_slice_top_bottom(code_rows), "rows": code_rows},
        "sector33_code": {**_slice_top_bottom(sector_rows), "rows": sector_rows},
        "setup_id": {**_slice_top_bottom(setup_rows), "rows": setup_rows},
        "setup": {**_slice_top_bottom(setup_rows), "rows": setup_rows},
        "side": {**_slice_top_bottom(side_rows), "rows": side_rows},
        "hedge": {**_slice_top_bottom(hedge_rows), "rows": hedge_rows},
    }


def _summarize_walkforward_windows(windows: list[dict[str, Any]]) -> dict[str, Any]:
    success_windows = [w for w in windows if str(w.get("status")) == "success"]
    executed = len(success_windows)
    failed = len([w for w in windows if str(w.get("status")) != "success"])

    total_trades = 0
    weighted_win_numer = 0.0
    total_realized = 0.0
    worst_dd: float | None = None
    pf_values: list[float] = []
    positive_windows = 0

    for row in success_windows:
        test_metrics = (row.get("test") or {}).get("metrics") or {}
        trade_events = int(test_metrics.get("trade_events") or 0)
        win_rate = _safe_float(test_metrics.get("win_rate"))
        total_realized_val = _safe_float(test_metrics.get("total_realized_unit_pnl")) or 0.0
        max_dd = _safe_float(test_metrics.get("max_drawdown_unit"))
        pf = _safe_float(test_metrics.get("profit_factor"))

        total_trades += trade_events
        if win_rate is not None and trade_events > 0:
            weighted_win_numer += float(win_rate) * float(trade_events)
        total_realized += float(total_realized_val)
        if total_realized_val > 0:
            positive_windows += 1
        if max_dd is not None:
            worst_dd = float(max_dd) if worst_dd is None else min(float(worst_dd), float(max_dd))
        if pf is not None:
            pf_values.append(float(pf))

    weighted_win_rate = (weighted_win_numer / float(total_trades)) if total_trades > 0 else None
    mean_pf = (sum(pf_values) / float(len(pf_values))) if pf_values else None
    positive_ratio = (float(positive_windows) / float(executed)) if executed > 0 else None
    return {
        "windows_total": int(len(windows)),
        "executed_windows": int(executed),
        "failed_windows": int(failed),
        "oos_trade_events": int(total_trades),
        "oos_weighted_win_rate": float(weighted_win_rate) if weighted_win_rate is not None else None,
        "oos_total_realized_unit_pnl": float(total_realized),
        "oos_worst_max_drawdown_unit": float(worst_dd) if worst_dd is not None else None,
        "oos_mean_profit_factor": float(mean_pf) if mean_pf is not None else None,
        "oos_positive_window_ratio": float(positive_ratio) if positive_ratio is not None else None,
    }


def run_strategy_walkforward(
    *,
    start_dt: int | None = None,
    end_dt: int | None = None,
    max_codes: int | None = 500,
    dry_run: bool = False,
    config: StrategyBacktestConfig | None = None,
    train_months: int = 24,
    test_months: int = 3,
    step_months: int = 1,
    min_windows: int = 1,
    max_windows: int | None = None,
    stop_on_oos_worst_max_drawdown_below: float | None = None,
    progress_cb: Callable[[int, str], None] | None = None,
) -> dict[str, Any]:
    def _notify(progress: int, message: str) -> None:
        if progress_cb is None:
            return
        progress_cb(max(0, min(100, int(progress))), str(message))

    cfg = config or StrategyBacktestConfig()
    train_months = max(1, int(train_months))
    test_months = max(1, int(test_months))
    step_months = max(1, int(step_months))
    min_windows = max(1, int(min_windows))
    max_windows = max(1, int(max_windows)) if max_windows is not None else None

    run_id = datetime.now(tz=timezone.utc).strftime("swf_%Y%m%d%H%M%S_%f")
    started_at = datetime.now(tz=timezone.utc)

    with get_conn() as conn:
        _ensure_backtest_schema(conn)
        market = _load_market_frame(conn, start_dt=start_dt, end_dt=end_dt, max_codes=max_codes)
    _notify(5, "Loading market data...")
    if market.empty:
        raise RuntimeError("No daily_bars rows for walkforward range")

    features = _prepare_feature_frame(market, cfg)
    _notify(15, "Preparing feature frame...")
    signal_ready_features = features[features["signal_ready"]].copy()
    features["bar_index"] = features.groupby("code", sort=False).cumcount() + 1
    features = features[
        (features["signal_ready"])
        & (features["bar_index"] >= int(max(1, cfg.min_history_bars)))
    ].copy()
    if features.empty and not signal_ready_features.empty:
        features = signal_ready_features
    if features.empty:
        raise RuntimeError("No rows with enough history for walkforward computation")

    segments = _build_month_segments(features)
    if len(segments) < (train_months + test_months):
        raise RuntimeError(
            f"Insufficient month segments for walkforward (segments={len(segments)}, "
            f"required={train_months + test_months})"
        )

    windows: list[dict[str, Any]] = []
    for pivot in range(train_months, len(segments) - test_months + 1, step_months):
        train_slice = segments[pivot - train_months : pivot]
        test_slice = segments[pivot : pivot + test_months]
        if not train_slice or not test_slice:
            continue
        windows.append(
            {
                "index": len(windows) + 1,
                "train_month_start": train_slice[0]["month"],
                "train_month_end": train_slice[-1]["month"],
                "test_month_start": test_slice[0]["month"],
                "test_month_end": test_slice[-1]["month"],
                "train_start_dt": int(train_slice[0]["start_dt"]),
                "train_end_dt": int(train_slice[-1]["end_dt"]),
                "test_start_dt": int(test_slice[0]["start_dt"]),
                "test_end_dt": int(test_slice[-1]["end_dt"]),
            }
        )

    if len(windows) < min_windows:
        raise RuntimeError(
            f"Walkforward windows below minimum (windows={len(windows)}, min_windows={min_windows})"
        )

    with get_conn() as conn:
        event_rows, event_notes = _load_event_rows(conn)
    _notify(20, f"Loaded event filters ({len(event_rows)} rows)...")

    windows_payload: list[dict[str, Any]] = []
    truncated = False
    truncated_reason: str | None = None
    for window in windows:
        if max_windows is not None and len(windows_payload) >= int(max_windows):
            truncated = True
            truncated_reason = "max_windows_reached"
            break
        train_start = int(window["train_start_dt"])
        train_end = int(window["train_end_dt"])
        test_start = int(window["test_start_dt"])
        test_end = int(window["test_end_dt"])

        train_frame = features[(features["dt"] >= train_start) & (features["dt"] <= train_end)].copy()
        test_frame = features[(features["dt"] >= test_start) & (features["dt"] <= test_end)].copy()

        payload: dict[str, Any] = {
            "index": int(window["index"]),
            "label": (
                f"{window['test_month_start']}..{window['test_month_end']} "
                f"(train {window['train_month_start']}..{window['train_month_end']})"
            ),
            "train": {
                "range": {"start_dt": train_start, "end_dt": train_end},
                "dataset": {
                    "rows": int(len(train_frame)),
                    "codes": int(train_frame["code"].nunique()) if not train_frame.empty else 0,
                    "days": int(train_frame["dt"].nunique()) if not train_frame.empty else 0,
                },
            },
            "test": {
                "range": {"start_dt": test_start, "end_dt": test_end},
                "dataset": {
                    "rows": int(len(test_frame)),
                    "codes": int(test_frame["code"].nunique()) if not test_frame.empty else 0,
                    "days": int(test_frame["dt"].nunique()) if not test_frame.empty else 0,
                },
            },
        }
        if train_frame.empty or test_frame.empty:
            payload["status"] = "skipped"
            payload["error"] = "insufficient_window_rows"
            windows_payload.append(payload)
            window_progress = 25 + int(65 * len(windows_payload) / max(1, len(windows)))
            _notify(
                window_progress,
                f"Walk-forward window {len(windows_payload)}/{len(windows)} skipped",
            )
            continue

        try:
            train_event_block = _build_event_block_set(
                train_frame,
                event_rows,
                lookback_days=cfg.event_lookback_days,
                lookahead_days=cfg.event_lookahead_days,
            )
            test_event_block = _build_event_block_set(
                test_frame,
                event_rows,
                lookback_days=cfg.event_lookback_days,
                lookahead_days=cfg.event_lookahead_days,
            )
            train_result = _simulate(train_frame, cfg, train_event_block)
            test_result = _simulate(test_frame, cfg, test_event_block)
            payload["status"] = "success"
            payload["train"]["metrics"] = _compact_metrics(train_result)
            payload["test"]["metrics"] = _compact_metrics(test_result)
            payload["event_filter"] = {
                "train_blocked_points": int(len(train_event_block)),
                "test_blocked_points": int(len(test_event_block)),
            }
        except Exception as exc:
            payload["status"] = "failed"
            payload["error"] = str(exc)
        windows_payload.append(payload)
        window_progress = 25 + int(65 * len(windows_payload) / max(1, len(windows)))
        _notify(
            window_progress,
            f"Walk-forward window {len(windows_payload)}/{len(windows)}",
        )
        if stop_on_oos_worst_max_drawdown_below is not None and str(payload.get("status")) == "success":
            test_metrics = (payload.get("test") or {}).get("metrics") or {}
            current_dd = _safe_float(test_metrics.get("max_drawdown_unit"))
            if (
                current_dd is not None
                and float(current_dd) < float(stop_on_oos_worst_max_drawdown_below)
            ):
                truncated = True
                truncated_reason = "oos_worst_max_drawdown_below_threshold"
                break

    summary = _summarize_walkforward_windows(windows_payload)
    _notify(94, "Summarizing walk-forward results...")
    attribution = _build_walkforward_attribution(windows_payload)
    finished_at = datetime.now(tz=timezone.utc)
    report = {
        "run_id": run_id,
        "status": "success",
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "range": {
            "start_dt": int(features["dt"].min()),
            "end_dt": int(features["dt"].max()),
        },
        "dataset": {
            "rows": int(len(features)),
            "codes": int(features["code"].nunique()),
            "days": int(features["dt"].nunique()),
            "max_codes": int(max_codes) if max_codes is not None else None,
        },
        "windowing": {
            "train_months": int(train_months),
            "test_months": int(test_months),
            "step_months": int(step_months),
            "min_windows": int(min_windows),
        },
        "event_filter": {
            "event_rows": int(len(event_rows)),
            "notes": event_notes,
        },
        "execution": {
            "requested_max_windows": int(max_windows) if max_windows is not None else None,
            "executed_windows": int(len(windows_payload)),
            "truncated": bool(truncated),
            "truncated_reason": truncated_reason,
            "stop_on_oos_worst_max_drawdown_below": (
                float(stop_on_oos_worst_max_drawdown_below)
                if stop_on_oos_worst_max_drawdown_below is not None
                else None
            ),
        },
        "config": asdict(cfg),
        "summary": summary,
        "attribution": attribution,
        "windows": windows_payload,
    }
    report["dry_run"] = bool(dry_run)

    if not dry_run and not is_legacy_analysis_disabled():
        _notify(98, "Saving walk-forward report...")
        with get_conn() as conn:
            _save_walkforward_run(
                conn,
                run_id=run_id,
                started_at=started_at,
                finished_at=finished_at,
                status="success",
                start_dt=start_dt,
                end_dt=end_dt,
                max_codes=max_codes,
                train_months=train_months,
                test_months=test_months,
                step_months=step_months,
                config=cfg,
                report=report,
                note=None,
            )
    _notify(100, "Walk-forward completed.")
    return report


def run_strategy_walkforward_gate(
    *,
    min_oos_total_realized_unit_pnl: float = 0.0,
    min_oos_mean_profit_factor: float = 1.05,
    min_oos_positive_window_ratio: float = 0.40,
    min_oos_worst_max_drawdown_unit: float = -0.12,
    dry_run: bool = False,
    note: str | None = None,
    source_run_id: str | None = None,
    source_finished_at: datetime | None = None,
    source_status: str | None = None,
    source_report: dict[str, Any] | None = None,
) -> dict[str, Any]:
    gate_id = datetime.now(tz=timezone.utc).strftime("swfg_%Y%m%d%H%M%S_%f")
    created_at = datetime.now(tz=timezone.utc)

    conn = None
    report_json = source_report if isinstance(source_report, dict) else None
    resolved_source_run_id = str(source_run_id or "")
    resolved_source_finished_at = source_finished_at
    resolved_source_status = str(source_status) if source_status is not None else None

    if report_json is None:
        with get_conn() as conn:
            _ensure_walkforward_schema(conn)
            row = conn.execute(
                """
                SELECT
                    run_id,
                    finished_at,
                    status,
                    report_json
                FROM strategy_walkforward_runs
                ORDER BY finished_at DESC
                LIMIT 1
                """
            ).fetchone()
            if not row:
                raise RuntimeError("No strategy_walkforward_runs found")

            resolved_source_run_id = str(row[0])
            resolved_source_finished_at = row[1]
            resolved_source_status = str(row[2]) if row[2] is not None else None
            try:
                report_json = json.loads(row[3]) if row[3] else {}
            except Exception:
                report_json = {}
    source_summary = (report_json.get("summary") or {}) if isinstance(report_json, dict) else {}
    source_windowing = (report_json.get("windowing") or {}) if isinstance(report_json, dict) else {}
    if not resolved_source_run_id:
        resolved_source_run_id = "unknown"
    report = _build_walkforward_gate_report(
        gate_id=gate_id,
        created_at=created_at,
        source_run_id=resolved_source_run_id,
        source_finished_at=resolved_source_finished_at,
        source_status=resolved_source_status,
        source_summary=source_summary,
        source_windowing=source_windowing,
        min_oos_total_realized_unit_pnl=float(min_oos_total_realized_unit_pnl),
        min_oos_mean_profit_factor=float(min_oos_mean_profit_factor),
        min_oos_positive_window_ratio=float(min_oos_positive_window_ratio),
        min_oos_worst_max_drawdown_unit=float(min_oos_worst_max_drawdown_unit),
        note=note,
    )
    if not dry_run and not is_legacy_analysis_disabled():
        with get_conn() as conn:
            _save_walkforward_gate_report(
                conn,
                gate_id=gate_id,
                created_at=created_at,
                source_run_id=resolved_source_run_id,
                source_finished_at=resolved_source_finished_at,
                status=str(report.get("status") or "fail"),
                thresholds=report.get("thresholds") or {},
                report=report,
                note=note,
            )
    report["dry_run"] = bool(dry_run)
    return report


def get_latest_strategy_walkforward() -> dict[str, Any]:
    if is_legacy_analysis_disabled():
        return {
            "has_run": False,
            "disabled_reason": "legacy_analysis_disabled",
            "latest": None,
        }
    with get_conn() as conn:
        _ensure_walkforward_schema(conn)
        row = conn.execute(
            """
            SELECT
                run_id,
                started_at,
                finished_at,
                status,
                start_dt,
                end_dt,
                max_codes,
                train_months,
                test_months,
                step_months,
                config_json,
                report_json,
                note
            FROM strategy_walkforward_runs
            ORDER BY finished_at DESC
            LIMIT 1
            """
        ).fetchone()
    if not row:
        return {"has_run": False, "latest": None}

    try:
        config_json = json.loads(row[10]) if row[10] else {}
    except Exception:
        config_json = {}
    try:
        report_json = json.loads(row[11]) if row[11] else {}
    except Exception:
        report_json = {}
    return {
        "has_run": True,
        "latest": {
            "run_id": row[0],
            "started_at": row[1],
            "finished_at": row[2],
            "status": row[3],
            "start_dt": row[4],
            "end_dt": row[5],
            "max_codes": row[6],
            "train_months": row[7],
            "test_months": row[8],
            "step_months": row[9],
            "config": config_json,
            "report": report_json,
            "note": row[12],
        },
    }


def get_latest_strategy_walkforward_gate() -> dict[str, Any]:
    if is_legacy_analysis_disabled():
        return {
            "has_run": False,
            "disabled_reason": "legacy_analysis_disabled",
            "latest": None,
        }
    with get_conn() as conn:
        _ensure_walkforward_gate_schema(conn)
        row = conn.execute(
            """
            SELECT
                gate_id,
                created_at,
                source_run_id,
                source_finished_at,
                status,
                thresholds_json,
                report_json,
                note
            FROM strategy_walkforward_gate_reports
            ORDER BY created_at DESC
            LIMIT 1
            """
        ).fetchone()
    if not row:
        return {"has_run": False, "latest": None}

    try:
        thresholds_json = json.loads(row[5]) if row[5] else {}
    except Exception:
        thresholds_json = {}
    try:
        report_json = json.loads(row[6]) if row[6] else {}
    except Exception:
        report_json = {}
    return {
        "has_run": True,
        "latest": {
            "gate_id": row[0],
            "created_at": row[1],
            "source_run_id": row[2],
            "source_finished_at": row[3],
            "status": row[4],
            "thresholds": thresholds_json,
            "report": report_json,
            "note": row[7],
        },
    }


def _extract_attribution_rows(report: dict[str, Any], key: str) -> list[dict[str, Any]]:
    attribution = report.get("attribution") if isinstance(report.get("attribution"), dict) else {}
    section = attribution.get(key) if isinstance(attribution, dict) else {}
    rows = section.get("rows") if isinstance(section, dict) else None
    if not isinstance(rows, list):
        return []
    return [item for item in rows if isinstance(item, dict)]


def _build_walkforward_research_snapshot(
    *,
    snapshot_date: int,
    source_run_id: str,
    source_finished_at: datetime | None,
    report: dict[str, Any],
) -> dict[str, Any]:
    setup_rows = _extract_attribution_rows(report, "setup")
    hedge_rows = _extract_attribution_rows(report, "hedge")
    window_rows = report.get("windows") if isinstance(report.get("windows"), list) else []

    rejected: dict[str, int] = {}
    for row in window_rows:
        if not isinstance(row, dict):
            continue
        if str(row.get("status")) == "success":
            continue
        reason = str(row.get("error") or row.get("status") or "unknown")
        rejected[reason] = int(rejected.get(reason, 0) + 1)

    core_ret = 0.0
    hedge_ret = 0.0
    for row in hedge_rows:
        key = str(row.get("key") or "").lower()
        ret_sum = _safe_float(row.get("ret_net_sum")) or 0.0
        if key == "hedge":
            hedge_ret += float(ret_sum)
        elif key == "core":
            core_ret += float(ret_sum)
    total_ret = float(core_ret + hedge_ret)
    hedge_ratio = (float(hedge_ret / total_ret) if total_ret != 0 else None)

    adopted_setups = [
        {
            "setup_id": str(row.get("key") or ""),
            "trades": int(row.get("trades") or 0),
            "ret_net_sum": float(_safe_float(row.get("ret_net_sum")) or 0.0),
            "win_rate": _safe_float(row.get("win_rate")),
            "profit_factor": _safe_float(row.get("profit_factor")),
        }
        for row in sorted(
            setup_rows,
            key=lambda item: float(_safe_float(item.get("ret_net_sum")) or 0.0),
            reverse=True,
        )[:8]
    ]

    rejected_reasons = [
        {"reason": reason, "count": int(count)}
        for reason, count in sorted(rejected.items(), key=lambda item: (-int(item[1]), str(item[0])))
    ]

    return {
        "snapshot_date": int(snapshot_date),
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
        "source_run_id": str(source_run_id),
        "source_finished_at": source_finished_at.isoformat() if isinstance(source_finished_at, datetime) else None,
        "summary": report.get("summary") if isinstance(report.get("summary"), dict) else {},
        "adopted_setups": adopted_setups,
        "rejected_reasons": rejected_reasons,
        "hedge_contribution": {
            "core_ret_net_sum": float(core_ret),
            "hedge_ret_net_sum": float(hedge_ret),
            "total_ret_net_sum": float(total_ret),
            "hedge_share": float(hedge_ratio) if hedge_ratio is not None else None,
        },
    }


def save_daily_walkforward_research_snapshot(*, snapshot_date: int | None = None) -> dict[str, Any]:
    snap_date = int(snapshot_date) if snapshot_date is not None else int(datetime.now(tz=timezone.utc).strftime("%Y%m%d"))
    if is_legacy_analysis_disabled():
        return {
            "saved": False,
            "snapshot_date": int(snap_date),
            "source_run_id": None,
            "reason": "legacy_analysis_disabled",
        }
    with get_conn() as conn:
        _ensure_walkforward_schema(conn)
        _ensure_walkforward_research_schema(conn)
        row = conn.execute(
            """
            SELECT
                run_id,
                finished_at,
                report_json
            FROM strategy_walkforward_runs
            ORDER BY finished_at DESC
            LIMIT 1
            """
        ).fetchone()
        if not row:
            return {
                "saved": False,
                "snapshot_date": int(snap_date),
                "source_run_id": None,
                "reason": "no_walkforward_run",
            }
        source_run_id = str(row[0] or "")
        source_finished_at = row[1] if isinstance(row[1], datetime) else None
        try:
            report_json = json.loads(row[2]) if row[2] else {}
        except Exception:
            report_json = {}
        snapshot = _build_walkforward_research_snapshot(
            snapshot_date=int(snap_date),
            source_run_id=source_run_id,
            source_finished_at=source_finished_at,
            report=report_json if isinstance(report_json, dict) else {},
        )
        conn.execute(
            """
            INSERT INTO strategy_walkforward_research_daily (
                snapshot_date,
                created_at,
                source_run_id,
                source_finished_at,
                report_json
            )
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(snapshot_date) DO UPDATE SET
                created_at = excluded.created_at,
                source_run_id = excluded.source_run_id,
                source_finished_at = excluded.source_finished_at,
                report_json = excluded.report_json
            """,
            [
                int(snap_date),
                datetime.now(tz=timezone.utc),
                source_run_id,
                source_finished_at,
                json.dumps(snapshot, ensure_ascii=False),
            ],
        )
    return {
        "saved": True,
        "snapshot_date": int(snap_date),
        "source_run_id": source_run_id,
        "report": snapshot,
    }


def get_latest_strategy_walkforward_research_snapshot() -> dict[str, Any]:
    if is_legacy_analysis_disabled():
        return {
            "has_snapshot": False,
            "disabled_reason": "legacy_analysis_disabled",
            "latest": None,
        }
    with get_conn() as conn:
        _ensure_walkforward_research_schema(conn)
        row = conn.execute(
            """
            SELECT
                snapshot_date,
                created_at,
                source_run_id,
                source_finished_at,
                report_json
            FROM strategy_walkforward_research_daily
            ORDER BY snapshot_date DESC
            LIMIT 1
            """
        ).fetchone()
    if not row:
        return {"has_snapshot": False, "latest": None}
    try:
        report_json = json.loads(row[4]) if row[4] else {}
    except Exception:
        report_json = {}
    return {
        "has_snapshot": True,
        "latest": {
            "snapshot_date": int(row[0]) if row[0] is not None else None,
            "created_at": row[1],
            "source_run_id": row[2],
            "source_finished_at": row[3],
            "report": report_json,
        },
    }


def _count_rows(conn, table_name: str) -> int:
    row = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    return int(row[0]) if row else 0


def _prune_table_to_latest(
    conn,
    *,
    table_name: str,
    pk_column: str,
    order_column: str,
    keep_latest: int,
) -> dict[str, int]:
    keep = max(0, int(keep_latest))
    before = _count_rows(conn, table_name)
    if before <= keep:
        return {"before": int(before), "after": int(before), "deleted": 0}
    conn.execute(
        f"""
        DELETE FROM {table_name}
        WHERE {pk_column} IN (
            SELECT {pk_column}
            FROM (
                SELECT
                    {pk_column},
                    ROW_NUMBER() OVER (
                        ORDER BY {order_column} DESC, {pk_column} DESC
                    ) AS rn
                FROM {table_name}
            ) ranked
            WHERE rn > ?
        )
        """,
        [keep],
    )
    after = _count_rows(conn, table_name)
    return {
        "before": int(before),
        "after": int(after),
        "deleted": int(max(0, before - after)),
    }


def prune_strategy_walkforward_history(
    *,
    keep_latest_runs: int = 160,
    keep_latest_gates: int = 160,
    keep_latest_snapshots: int = 45,
    reclaim_space: bool = False,
) -> dict[str, Any]:
    if is_legacy_analysis_disabled():
        return {
            "runs": {"before": 0, "after": 0, "deleted": 0},
            "gates": {"before": 0, "after": 0, "deleted": 0},
            "snapshots": {"before": 0, "after": 0, "deleted": 0},
            "deleted_total": 0,
            "reclaim_space_requested": bool(reclaim_space),
            "space_reclaimed": False,
            "skipped_reason": "legacy_analysis_disabled",
        }
    with get_conn() as conn:
        _ensure_walkforward_schema(conn)
        _ensure_walkforward_gate_schema(conn)
        _ensure_walkforward_research_schema(conn)
        result = {
            "runs": _prune_table_to_latest(
                conn,
                table_name="strategy_walkforward_runs",
                pk_column="run_id",
                order_column="finished_at",
                keep_latest=int(keep_latest_runs),
            ),
            "gates": _prune_table_to_latest(
                conn,
                table_name="strategy_walkforward_gate_reports",
                pk_column="gate_id",
                order_column="created_at",
                keep_latest=int(keep_latest_gates),
            ),
            "snapshots": _prune_table_to_latest(
                conn,
                table_name="strategy_walkforward_research_daily",
                pk_column="snapshot_date",
                order_column="snapshot_date",
                keep_latest=int(keep_latest_snapshots),
            ),
        }
        deleted_total = int(
            (result["runs"]["deleted"])
            + (result["gates"]["deleted"])
            + (result["snapshots"]["deleted"])
        )
        result["deleted_total"] = deleted_total
        result["reclaim_space_requested"] = bool(reclaim_space)
        result["space_reclaimed"] = False
        if deleted_total > 0:
            try:
                conn.execute("CHECKPOINT")
            except Exception:
                pass
            if reclaim_space:
                try:
                    conn.execute("VACUUM")
                    result["space_reclaimed"] = True
                except Exception:
                    result["space_reclaimed"] = False
        return result


def _ensure_market_regime_daily_schema(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS market_regime_daily (
            dt INTEGER PRIMARY KEY,
            regime_id TEXT NOT NULL,
            breadth_above_ma20 DOUBLE,
            breadth_above_ma60 DOUBLE,
            advancers_ratio DOUBLE,
            index_close_vs_ma20 DOUBLE,
            index_close_vs_ma60 DOUBLE,
            market_atr_pct DOUBLE,
            sector_dispersion DOUBLE,
            regime_score DOUBLE,
            label_version TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL
        );
        """
    )


def _ensure_future_pattern_daily_schema(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS future_pattern_daily (
            code TEXT NOT NULL,
            dt INTEGER NOT NULL,
            horizon INTEGER NOT NULL,
            pattern_id TEXT NOT NULL,
            ret_5_atr DOUBLE,
            ret_10_atr DOUBLE,
            ret_20_atr DOUBLE,
            mfe_20_atr DOUBLE,
            mae_20_atr DOUBLE,
            max_dd_20_atr DOUBLE,
            realized_vol_20 DOUBLE,
            label_version TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL,
            PRIMARY KEY (code, dt, horizon)
        );
        """
    )


def _delete_dt_range(
    conn,
    *,
    table_name: str,
    dt_column: str,
    start_dt: int | None,
    end_dt: int | None,
    extra_where_sql: str = "",
    extra_params: list[object] | None = None,
) -> None:
    clauses: list[str] = []
    params: list[object] = []
    if start_dt is not None:
        clauses.append(f"{dt_column} >= ?")
        params.append(int(start_dt))
    if end_dt is not None:
        clauses.append(f"{dt_column} <= ?")
        params.append(int(end_dt))
    if extra_where_sql:
        clauses.append(str(extra_where_sql))
        params.extend(list(extra_params or []))
    if not clauses:
        conn.execute(f"DELETE FROM {table_name}")
        return
    conn.execute(f"DELETE FROM {table_name} WHERE {' AND '.join(clauses)}", params)


def _classify_market_regime_row(
    row: pd.Series,
    *,
    high_vol_threshold: float,
    high_dispersion_threshold: float,
) -> tuple[str, float]:
    breadth20 = _safe_float(row.get("breadth_above_ma20"))
    breadth60 = _safe_float(row.get("breadth_above_ma60"))
    advancers = _safe_float(row.get("advancers_ratio"))
    idx20 = _safe_float(row.get("index_close_vs_ma20"))
    idx60 = _safe_float(row.get("index_close_vs_ma60"))
    atr_pct = _safe_float(row.get("market_atr_pct"))
    dispersion = _safe_float(row.get("sector_dispersion"))
    breadth_delta5 = _safe_float(row.get("breadth_delta5"))

    score = 0.0
    if breadth20 is not None:
        score += (float(breadth20) - 0.50) * 3.0
    if breadth60 is not None:
        score += (float(breadth60) - 0.50) * 3.5
    if advancers is not None:
        score += (float(advancers) - 0.50) * 2.0
    if idx20 is not None:
        score += float(idx20) * 8.0
    if idx60 is not None:
        score += float(idx60) * 6.0
    if atr_pct is not None:
        score -= max(0.0, float(atr_pct) - 0.03) * 18.0
    if breadth_delta5 is not None:
        score += float(breadth_delta5) * 2.5

    is_risk_on_trend = bool(
        breadth20 is not None
        and breadth60 is not None
        and idx20 is not None
        and idx60 is not None
        and atr_pct is not None
        and float(breadth20) >= 0.60
        and float(breadth60) >= 0.55
        and float(idx20) > 0.0
        and float(idx60) > 0.0
        and float(atr_pct) < float(high_vol_threshold)
    )
    is_risk_off_trend = bool(
        breadth20 is not None
        and breadth60 is not None
        and idx20 is not None
        and idx60 is not None
        and float(breadth20) <= 0.40
        and float(breadth60) <= 0.45
        and float(idx20) < 0.0
        and float(idx60) < 0.0
    )
    is_capitulation_rebound = bool(
        is_risk_off_trend
        and breadth_delta5 is not None
        and advancers is not None
        and float(breadth_delta5) >= 0.08
        and float(advancers) >= 0.55
    )
    is_high_vol_chaos = bool(
        atr_pct is not None
        and dispersion is not None
        and float(atr_pct) >= float(high_vol_threshold)
        and float(dispersion) >= float(high_dispersion_threshold)
    )
    is_risk_on_range = bool(
        breadth20 is not None
        and idx20 is not None
        and float(breadth20) >= 0.55
        and float(idx20) > 0.0
    )

    if is_capitulation_rebound:
        return "capitulation_rebound", float(score)
    if is_high_vol_chaos:
        return "high_vol_chaos", float(score)
    if is_risk_on_trend:
        return "risk_on_trend", float(score)
    if is_risk_on_range:
        return "risk_on_range", float(score)
    if is_risk_off_trend:
        return "risk_off_trend", float(score)
    return "neutral_range", float(score)


def build_market_regime_daily(
    *,
    start_dt: int | None = None,
    end_dt: int | None = None,
    label_version: str = "v1",
) -> dict[str, Any]:
    with get_conn() as conn:
        _ensure_market_regime_daily_schema(conn)
        has_sector = _table_exists(conn, "industry_master")
        sector_join = "LEFT JOIN industry_master im ON im.code = b.code" if has_sector else ""
        sector_select = "COALESCE(im.sector33_code, '__NA__') AS sector33_code" if has_sector else "'__NA__' AS sector33_code"
        params: list[object] = []
        where_sql = ""
        if start_dt is not None:
            where_sql += " AND d.dt >= ?"
            params.append(int(start_dt))
        if end_dt is not None:
            where_sql += " AND d.dt <= ?"
            params.append(int(end_dt))

        metrics_df = conn.execute(
            f"""
            WITH base0 AS (
                SELECT
                    b.date AS dt,
                    b.code AS code,
                    b.c AS close,
                    b.h AS h,
                    b.l AS l,
                    {sector_select},
                    LAG(b.c, 1) OVER (PARTITION BY b.code ORDER BY b.date) AS prev_close
                FROM daily_bars b
                {sector_join}
                WHERE b.c IS NOT NULL
            ),
            base1 AS (
                SELECT
                    dt,
                    code,
                    close,
                    sector33_code,
                    prev_close,
                    AVG(close) OVER (
                        PARTITION BY code
                        ORDER BY dt
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) AS ma20,
                    AVG(close) OVER (
                        PARTITION BY code
                        ORDER BY dt
                        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                    ) AS ma60,
                    GREATEST(
                        h - l,
                        ABS(h - COALESCE(prev_close, close)),
                        ABS(l - COALESCE(prev_close, close))
                    ) AS tr
                FROM base0
            ),
            base2 AS (
                SELECT
                    *,
                    AVG(tr) OVER (
                        PARTITION BY code
                        ORDER BY dt
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) AS atr20
                FROM base1
            ),
            daily_cross AS (
                SELECT
                    dt,
                    AVG(
                        CASE
                            WHEN ma20 IS NOT NULL AND ABS(ma20) > 1e-12 AND close > ma20 THEN 1.0
                            ELSE 0.0
                        END
                    ) AS breadth_above_ma20,
                    AVG(
                        CASE
                            WHEN ma60 IS NOT NULL AND ABS(ma60) > 1e-12 AND close > ma60 THEN 1.0
                            ELSE 0.0
                        END
                    ) AS breadth_above_ma60,
                    AVG(
                        CASE
                            WHEN prev_close IS NOT NULL AND ABS(prev_close) > 1e-12 AND close > prev_close THEN 1.0
                            ELSE 0.0
                        END
                    ) AS advancers_ratio,
                    AVG(
                        CASE
                            WHEN ma20 IS NOT NULL AND ABS(ma20) > 1e-12 THEN (close - ma20) / ma20
                            ELSE NULL
                        END
                    ) AS proxy_close_vs_ma20,
                    AVG(
                        CASE
                            WHEN ma60 IS NOT NULL AND ABS(ma60) > 1e-12 THEN (close - ma60) / ma60
                            ELSE NULL
                        END
                    ) AS proxy_close_vs_ma60,
                    AVG(
                        CASE
                            WHEN atr20 IS NOT NULL AND close > 0 THEN atr20 / close
                            ELSE NULL
                        END
                    ) AS fallback_atr_pct
                FROM base2
                GROUP BY dt
            ),
            sector_daily AS (
                SELECT
                    dt,
                    sector33_code,
                    AVG(
                        CASE
                            WHEN prev_close IS NOT NULL AND ABS(prev_close) > 1e-12 THEN (close - prev_close) / prev_close
                            ELSE NULL
                        END
                    ) AS sector_ret1
                FROM base2
                GROUP BY dt, sector33_code
            ),
            sector_disp AS (
                SELECT
                    dt,
                    STDDEV_SAMP(sector_ret1) AS sector_dispersion
                FROM sector_daily
                GROUP BY dt
            ),
            index_base0 AS (
                SELECT
                    b.date AS dt,
                    b.c AS close,
                    b.h AS h,
                    b.l AS l,
                    LAG(b.c, 1) OVER (ORDER BY b.date) AS prev_close
                FROM daily_bars b
                WHERE b.code = '1001' AND b.c IS NOT NULL
            ),
            index_base1 AS (
                SELECT
                    dt,
                    close,
                    AVG(close) OVER (
                        ORDER BY dt
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) AS ma20,
                    AVG(close) OVER (
                        ORDER BY dt
                        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                    ) AS ma60,
                    GREATEST(
                        h - l,
                        ABS(h - COALESCE(prev_close, close)),
                        ABS(l - COALESCE(prev_close, close))
                    ) AS tr
                FROM index_base0
            ),
            index_metrics AS (
                SELECT
                    dt,
                    CASE
                        WHEN ma20 IS NOT NULL AND ABS(ma20) > 1e-12 THEN (close - ma20) / ma20
                        ELSE NULL
                    END AS index_close_vs_ma20,
                    CASE
                        WHEN ma60 IS NOT NULL AND ABS(ma60) > 1e-12 THEN (close - ma60) / ma60
                        ELSE NULL
                    END AS index_close_vs_ma60,
                    CASE
                        WHEN close > 0 THEN
                            AVG(tr) OVER (
                                ORDER BY dt
                                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                            ) / close
                        ELSE NULL
                    END AS market_atr_pct
                FROM index_base1
            )
            SELECT
                d.dt,
                d.breadth_above_ma20,
                d.breadth_above_ma60,
                d.advancers_ratio,
                COALESCE(i.index_close_vs_ma20, d.proxy_close_vs_ma20) AS index_close_vs_ma20,
                COALESCE(i.index_close_vs_ma60, d.proxy_close_vs_ma60) AS index_close_vs_ma60,
                COALESCE(i.market_atr_pct, d.fallback_atr_pct) AS market_atr_pct,
                s.sector_dispersion
            FROM daily_cross d
            LEFT JOIN sector_disp s ON s.dt = d.dt
            LEFT JOIN index_metrics i ON i.dt = d.dt
            WHERE 1 = 1
            {where_sql}
            ORDER BY d.dt ASC
            """,
            params,
        ).df()

        if metrics_df.empty:
            return {
                "table": "market_regime_daily",
                "rows": 0,
                "label_version": str(label_version),
                "counts_by_regime": {},
            }

        metrics_df["breadth_delta5"] = metrics_df["breadth_above_ma20"].astype(float).diff(5)
        atr_series = metrics_df["market_atr_pct"].dropna()
        dispersion_series = metrics_df["sector_dispersion"].dropna()
        high_vol_threshold = float(max(0.035, atr_series.quantile(0.75))) if not atr_series.empty else 0.035
        high_dispersion_threshold = (
            float(max(0.012, dispersion_series.quantile(0.75)))
            if not dispersion_series.empty
            else 0.012
        )
        classifications = metrics_df.apply(
            lambda row: _classify_market_regime_row(
                row,
                high_vol_threshold=high_vol_threshold,
                high_dispersion_threshold=high_dispersion_threshold,
            ),
            axis=1,
        ).tolist()
        metrics_df["regime_id"] = [str(item[0]) for item in classifications]
        metrics_df["regime_score"] = [float(item[1]) for item in classifications]

        _delete_dt_range(
            conn,
            table_name="market_regime_daily",
            dt_column="dt",
            start_dt=start_dt,
            end_dt=end_dt,
        )
        created_at = datetime.now(tz=timezone.utc)
        rows = [
            [
                int(row.dt),
                str(row.regime_id),
                _safe_float(row.breadth_above_ma20),
                _safe_float(row.breadth_above_ma60),
                _safe_float(row.advancers_ratio),
                _safe_float(row.index_close_vs_ma20),
                _safe_float(row.index_close_vs_ma60),
                _safe_float(row.market_atr_pct),
                _safe_float(row.sector_dispersion),
                _safe_float(row.regime_score),
                str(label_version),
                created_at,
            ]
            for row in metrics_df.itertuples(index=False)
        ]
        conn.executemany(
            """
            INSERT INTO market_regime_daily (
                dt,
                regime_id,
                breadth_above_ma20,
                breadth_above_ma60,
                advancers_ratio,
                index_close_vs_ma20,
                index_close_vs_ma60,
                market_atr_pct,
                sector_dispersion,
                regime_score,
                label_version,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        counts_df = conn.execute(
            """
            SELECT regime_id, COUNT(*) AS n
            FROM market_regime_daily
            WHERE label_version = ?
              AND (? IS NULL OR dt >= ?)
              AND (? IS NULL OR dt <= ?)
            GROUP BY regime_id
            ORDER BY n DESC, regime_id ASC
            """,
            [str(label_version), start_dt, start_dt, end_dt, end_dt],
        ).df()
        return {
            "table": "market_regime_daily",
            "rows": int(len(rows)),
            "label_version": str(label_version),
            "dt_min": int(metrics_df["dt"].min()),
            "dt_max": int(metrics_df["dt"].max()),
            "high_vol_threshold": float(high_vol_threshold),
            "high_dispersion_threshold": float(high_dispersion_threshold),
            "counts_by_regime": {
                str(row["regime_id"]): int(row["n"])
                for _, row in counts_df.iterrows()
            },
        }


def build_future_pattern_daily(
    *,
    start_dt: int | None = None,
    end_dt: int | None = None,
    horizon: int = 20,
    label_version: str = "v1",
) -> dict[str, Any]:
    horizon_n = max(5, int(horizon))
    with get_conn() as conn:
        _ensure_future_pattern_daily_schema(conn)
        _delete_dt_range(
            conn,
            table_name="future_pattern_daily",
            dt_column="dt",
            start_dt=start_dt,
            end_dt=end_dt,
            extra_where_sql="horizon = ?",
            extra_params=[int(horizon_n)],
        )
        params: list[object] = [
            int(horizon_n),
            str(label_version),
        ]
        dt_where = ""
        if start_dt is not None:
            dt_where += " AND dt >= ?"
            params.append(int(start_dt))
        if end_dt is not None:
            dt_where += " AND dt <= ?"
            params.append(int(end_dt))

        conn.execute(
            f"""
            INSERT INTO future_pattern_daily (
                code,
                dt,
                horizon,
                pattern_id,
                ret_5_atr,
                ret_10_atr,
                ret_20_atr,
                mfe_20_atr,
                mae_20_atr,
                max_dd_20_atr,
                realized_vol_20,
                label_version,
                created_at
            )
            WITH base0 AS (
                SELECT
                    b.code AS code,
                    b.date AS dt,
                    b.o AS o,
                    b.h AS h,
                    b.l AS l,
                    b.c AS c,
                    LAG(b.c, 1) OVER (PARTITION BY b.code ORDER BY b.date) AS prev_close
                FROM daily_bars b
                WHERE b.c IS NOT NULL
            ),
            base1 AS (
                SELECT
                    *,
                    GREATEST(
                        h - l,
                        ABS(h - COALESCE(prev_close, c)),
                        ABS(l - COALESCE(prev_close, c))
                    ) AS tr,
                    CASE
                        WHEN prev_close IS NULL OR ABS(prev_close) <= 1e-12 OR c <= 0 THEN NULL
                        ELSE LN(c / prev_close)
                    END AS log_ret1
                FROM base0
            ),
            feature_base AS (
                SELECT
                    *,
                    AVG(tr) OVER (
                        PARTITION BY code
                        ORDER BY dt
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) AS atr20,
                    LEAD(c, 5) OVER (PARTITION BY code ORDER BY dt) AS close_f5,
                    LEAD(c, 10) OVER (PARTITION BY code ORDER BY dt) AS close_f10,
                    LEAD(c, 20) OVER (PARTITION BY code ORDER BY dt) AS close_f20,
                    MAX(h) OVER (
                        PARTITION BY code
                        ORDER BY dt
                        ROWS BETWEEN 1 FOLLOWING AND 20 FOLLOWING
                    ) AS future_high20,
                    MIN(l) OVER (
                        PARTITION BY code
                        ORDER BY dt
                        ROWS BETWEEN 1 FOLLOWING AND 20 FOLLOWING
                    ) AS future_low20,
                    MIN(c) OVER (
                        PARTITION BY code
                        ORDER BY dt
                        ROWS BETWEEN 1 FOLLOWING AND 20 FOLLOWING
                    ) AS future_close_min20,
                    STDDEV_SAMP(log_ret1) OVER (
                        PARTITION BY code
                        ORDER BY dt
                        ROWS BETWEEN 1 FOLLOWING AND 20 FOLLOWING
                    ) AS realized_vol_20
                FROM base1
            ),
            scored AS (
                SELECT
                    code,
                    dt,
                    c,
                    atr20,
                    CASE
                        WHEN close_f5 IS NULL OR atr20 IS NULL OR ABS(atr20) <= 1e-12 THEN NULL
                        ELSE (close_f5 - c) / atr20
                    END AS ret_5_atr,
                    CASE
                        WHEN close_f10 IS NULL OR atr20 IS NULL OR ABS(atr20) <= 1e-12 THEN NULL
                        ELSE (close_f10 - c) / atr20
                    END AS ret_10_atr,
                    CASE
                        WHEN close_f20 IS NULL OR atr20 IS NULL OR ABS(atr20) <= 1e-12 THEN NULL
                        ELSE (close_f20 - c) / atr20
                    END AS ret_20_atr,
                    CASE
                        WHEN future_high20 IS NULL OR atr20 IS NULL OR ABS(atr20) <= 1e-12 THEN NULL
                        ELSE (future_high20 - c) / atr20
                    END AS mfe_20_atr,
                    CASE
                        WHEN future_low20 IS NULL OR atr20 IS NULL OR ABS(atr20) <= 1e-12 THEN NULL
                        ELSE (future_low20 - c) / atr20
                    END AS mae_20_atr,
                    CASE
                        WHEN future_close_min20 IS NULL OR atr20 IS NULL OR ABS(atr20) <= 1e-12 THEN NULL
                        ELSE (future_close_min20 - c) / atr20
                    END AS max_dd_20_atr,
                    realized_vol_20
                FROM feature_base
            )
            SELECT
                code,
                dt,
                ? AS horizon,
                CASE
                    WHEN mae_20_atr IS NOT NULL AND mae_20_atr <= -2.5 THEN 'panic_down'
                    WHEN ret_5_atr IS NOT NULL AND ret_20_atr IS NOT NULL
                        AND ret_5_atr < -0.5 AND ret_20_atr >= 1.0 THEN 'mean_revert_up'
                    WHEN ret_20_atr IS NOT NULL AND mae_20_atr IS NOT NULL
                        AND ret_20_atr >= 1.5 AND mae_20_atr > -0.8 THEN 'trend_up'
                    WHEN mfe_20_atr IS NOT NULL AND ret_20_atr IS NOT NULL
                        AND mfe_20_atr >= 1.5 AND ret_20_atr >= 0.0 AND ret_20_atr < 0.7 THEN 'trend_up_then_fade'
                    WHEN mfe_20_atr IS NOT NULL AND ret_20_atr IS NOT NULL
                        AND mfe_20_atr >= 1.0 AND ret_20_atr <= -0.5 THEN 'breakout_fail'
                    WHEN ret_20_atr IS NOT NULL AND ret_20_atr <= -1.5 THEN 'trend_down'
                    WHEN ret_20_atr IS NOT NULL AND ABS(ret_20_atr) <= 0.5
                        AND COALESCE(realized_vol_20, 0.0) <= 0.018 THEN 'range_flat'
                    WHEN ret_20_atr IS NOT NULL AND ABS(ret_20_atr) <= 0.5 THEN 'range_volatile'
                    ELSE 'mixed'
                END AS pattern_id,
                ret_5_atr,
                ret_10_atr,
                ret_20_atr,
                mfe_20_atr,
                mae_20_atr,
                max_dd_20_atr,
                realized_vol_20,
                ? AS label_version,
                CURRENT_TIMESTAMP AS created_at
            FROM scored
            WHERE atr20 IS NOT NULL
              AND ABS(atr20) > 1e-12
              AND ret_20_atr IS NOT NULL
              {dt_where}
            """,
            params,
        )
        counts_df = conn.execute(
            """
            SELECT pattern_id, COUNT(*) AS n
            FROM future_pattern_daily
            WHERE horizon = ?
              AND label_version = ?
              AND (? IS NULL OR dt >= ?)
              AND (? IS NULL OR dt <= ?)
            GROUP BY pattern_id
            ORDER BY n DESC, pattern_id ASC
            """,
            [int(horizon_n), str(label_version), start_dt, start_dt, end_dt, end_dt],
        ).df()
        row = conn.execute(
            """
            SELECT COUNT(*), MIN(dt), MAX(dt)
            FROM future_pattern_daily
            WHERE horizon = ?
              AND label_version = ?
              AND (? IS NULL OR dt >= ?)
              AND (? IS NULL OR dt <= ?)
            """,
            [int(horizon_n), str(label_version), start_dt, start_dt, end_dt, end_dt],
        ).fetchone()
        return {
            "table": "future_pattern_daily",
            "rows": int(row[0]) if row and row[0] is not None else 0,
            "dt_min": int(row[1]) if row and row[1] is not None else None,
            "dt_max": int(row[2]) if row and row[2] is not None else None,
            "horizon": int(horizon_n),
            "label_version": str(label_version),
            "counts_by_pattern": {
                str(r["pattern_id"]): int(r["n"])
                for _, r in counts_df.iterrows()
            },
        }


def get_regime_router_foundation_summary(
    *,
    label_version: str = "v1",
    horizon: int = 20,
) -> dict[str, Any]:
    with get_conn() as conn:
        _ensure_market_regime_daily_schema(conn)
        _ensure_future_pattern_daily_schema(conn)
        regime_counts = conn.execute(
            """
            SELECT regime_id, COUNT(*) AS n
            FROM market_regime_daily
            WHERE label_version = ?
            GROUP BY regime_id
            ORDER BY n DESC, regime_id ASC
            """,
            [str(label_version)],
        ).df()
        pattern_counts = conn.execute(
            """
            SELECT pattern_id, COUNT(*) AS n
            FROM future_pattern_daily
            WHERE label_version = ?
              AND horizon = ?
            GROUP BY pattern_id
            ORDER BY n DESC, pattern_id ASC
            """,
            [str(label_version), int(horizon)],
        ).df()
        return {
            "label_version": str(label_version),
            "horizon": int(horizon),
            "market_regime_daily": {
                "counts_by_regime": {
                    str(row["regime_id"]): int(row["n"])
                    for _, row in regime_counts.iterrows()
                }
            },
            "future_pattern_daily": {
                "counts_by_pattern": {
                    str(row["pattern_id"]): int(row["n"])
                    for _, row in pattern_counts.iterrows()
                }
            },
        }


def _ensure_strategy_registry_schema(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS strategy_registry (
            strategy_id TEXT PRIMARY KEY,
            family TEXT NOT NULL,
            side TEXT NOT NULL,
            status TEXT NOT NULL,
            config_json TEXT NOT NULL,
            note TEXT,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
        );
        """
    )


def _ensure_strategy_conditional_stats_schema(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS strategy_conditional_stats (
            strategy_id TEXT NOT NULL,
            regime_id TEXT NOT NULL,
            pattern_id TEXT NOT NULL,
            side TEXT NOT NULL,
            horizon INTEGER NOT NULL,
            scope_key TEXT NOT NULL,
            trades INTEGER NOT NULL,
            wins INTEGER NOT NULL,
            win_rate DOUBLE,
            ret_net_sum DOUBLE,
            avg_ret_net DOUBLE,
            profit_factor DOUBLE,
            avg_mfe DOUBLE,
            avg_mae DOUBLE,
            worst_dd DOUBLE,
            stability_score DOUBLE,
            label_version TEXT NOT NULL,
            updated_at TIMESTAMP NOT NULL,
            PRIMARY KEY (
                strategy_id,
                regime_id,
                pattern_id,
                side,
                horizon,
                scope_key,
                label_version
            )
        );
        """
    )


def _strategy_config_from_payload(payload: dict[str, Any]) -> StrategyBacktestConfig:
    data = dict(payload or {})
    if "allowed_long_setups" in data and data["allowed_long_setups"] is not None:
        data["allowed_long_setups"] = tuple(str(v) for v in data["allowed_long_setups"])
    if "allowed_short_setups" in data and data["allowed_short_setups"] is not None:
        data["allowed_short_setups"] = tuple(str(v) for v in data["allowed_short_setups"])
    return StrategyBacktestConfig(**data)


def _default_strategy_registry_entries() -> list[dict[str, Any]]:
    return [
        {
            "strategy_id": "lb_p2_regime_loose_v1",
            "family": "long_breakout",
            "side": "long",
            "status": "challenger",
            "note": "Long breakout core with regime filter, loose entry gate.",
            "config": StrategyBacktestConfig(
                max_positions=1,
                initial_units=1,
                add1_units=0,
                add2_units=0,
                hedge_units=0,
                min_hedge_ratio=0.0,
                cost_bps=20.0,
                min_history_bars=220,
                prefer_net_short_ratio=2.0,
                event_lookback_days=2,
                event_lookahead_days=1,
                min_long_score=1.0,
                min_short_score=99.0,
                max_new_entries_per_day=1,
                max_new_entries_per_month=None,
                allowed_sides="long",
                require_decision_for_long=False,
                require_ma_bull_stack_long=False,
                max_dist_ma20_long=None,
                min_volume_ratio_long=0.0,
                max_atr_pct_long=None,
                min_ml_p_up_long=None,
                allowed_long_setups=("long_breakout_p2",),
                allowed_short_setups=None,
                use_regime_filter=True,
                regime_breadth_lookback_days=20,
                regime_long_min_breadth_above60=0.52,
                regime_short_max_breadth_above60=0.48,
                range_bias_width_min=0.08,
                range_bias_long_pos_min=0.60,
                range_bias_short_pos_max=0.40,
                ma20_count20_min_long=15,
                ma20_count20_min_short=12,
                ma60_count60_min_long=24,
                ma60_count60_min_short=30,
            ),
        },
        {
            "strategy_id": "lb_p2_regime_strict_v1",
            "family": "long_breakout",
            "side": "long",
            "status": "challenger",
            "note": "Long breakout strict decision + MA stack + volume filter.",
            "config": StrategyBacktestConfig(
                max_positions=1,
                initial_units=1,
                add1_units=0,
                add2_units=0,
                hedge_units=0,
                min_hedge_ratio=0.0,
                cost_bps=20.0,
                min_history_bars=220,
                prefer_net_short_ratio=2.0,
                event_lookback_days=2,
                event_lookahead_days=1,
                min_long_score=1.5,
                min_short_score=99.0,
                max_new_entries_per_day=1,
                max_new_entries_per_month=None,
                allowed_sides="long",
                require_decision_for_long=True,
                require_ma_bull_stack_long=True,
                max_dist_ma20_long=0.10,
                min_volume_ratio_long=1.1,
                max_atr_pct_long=0.08,
                min_ml_p_up_long=None,
                allowed_long_setups=("long_breakout_p2",),
                allowed_short_setups=None,
                use_regime_filter=True,
                regime_breadth_lookback_days=20,
                regime_long_min_breadth_above60=0.52,
                regime_short_max_breadth_above60=0.48,
                range_bias_width_min=0.08,
                range_bias_long_pos_min=0.60,
                range_bias_short_pos_max=0.40,
                ma20_count20_min_long=15,
                ma20_count20_min_short=12,
                ma60_count60_min_long=24,
                ma60_count60_min_short=30,
            ),
        },
        {
            "strategy_id": "lb_p2_regime_two_pos_v1",
            "family": "long_breakout",
            "side": "long",
            "status": "challenger",
            "note": "Long breakout strict regime variant with two simultaneous positions.",
            "config": StrategyBacktestConfig(
                max_positions=2,
                initial_units=1,
                add1_units=0,
                add2_units=0,
                hedge_units=0,
                min_hedge_ratio=0.0,
                cost_bps=20.0,
                min_history_bars=220,
                prefer_net_short_ratio=2.0,
                event_lookback_days=2,
                event_lookahead_days=1,
                min_long_score=1.0,
                min_short_score=99.0,
                max_new_entries_per_day=1,
                max_new_entries_per_month=None,
                allowed_sides="long",
                require_decision_for_long=True,
                require_ma_bull_stack_long=True,
                max_dist_ma20_long=0.12,
                min_volume_ratio_long=0.0,
                max_atr_pct_long=0.10,
                min_ml_p_up_long=None,
                allowed_long_setups=("long_breakout_p2",),
                allowed_short_setups=None,
                use_regime_filter=True,
                regime_breadth_lookback_days=20,
                regime_long_min_breadth_above60=0.52,
                regime_short_max_breadth_above60=0.48,
                range_bias_width_min=0.08,
                range_bias_long_pos_min=0.60,
                range_bias_short_pos_max=0.40,
                ma20_count20_min_long=15,
                ma20_count20_min_short=12,
                ma60_count60_min_long=24,
                ma60_count60_min_short=30,
            ),
        },
        {
            "strategy_id": "lb_p2_no_regime_v1",
            "family": "long_breakout",
            "side": "long",
            "status": "challenger",
            "note": "Long breakout without regime filter, broad universe behavior check.",
            "config": StrategyBacktestConfig(
                max_positions=1,
                initial_units=1,
                add1_units=0,
                add2_units=0,
                hedge_units=0,
                min_hedge_ratio=0.0,
                cost_bps=20.0,
                min_history_bars=220,
                prefer_net_short_ratio=2.0,
                event_lookback_days=2,
                event_lookahead_days=1,
                min_long_score=1.0,
                min_short_score=99.0,
                max_new_entries_per_day=1,
                max_new_entries_per_month=None,
                allowed_sides="long",
                require_decision_for_long=False,
                require_ma_bull_stack_long=False,
                max_dist_ma20_long=None,
                min_volume_ratio_long=0.0,
                max_atr_pct_long=None,
                min_ml_p_up_long=None,
                allowed_long_setups=("long_breakout_p2",),
                allowed_short_setups=None,
                use_regime_filter=False,
                regime_breadth_lookback_days=20,
                regime_long_min_breadth_above60=0.52,
                regime_short_max_breadth_above60=0.48,
                range_bias_width_min=0.08,
                range_bias_long_pos_min=0.55,
                range_bias_short_pos_max=0.40,
                ma20_count20_min_long=10,
                ma20_count20_min_short=12,
                ma60_count60_min_long=36,
                ma60_count60_min_short=30,
            ),
        },
        {
            "strategy_id": "lb_p2_decision_only_v1",
            "family": "long_breakout",
            "side": "long",
            "status": "challenger",
            "note": "Long breakout requiring decision confirmation but looser MA stack rules.",
            "config": StrategyBacktestConfig(
                max_positions=1,
                initial_units=1,
                add1_units=0,
                add2_units=0,
                hedge_units=0,
                min_hedge_ratio=0.0,
                cost_bps=20.0,
                min_history_bars=220,
                prefer_net_short_ratio=2.0,
                event_lookback_days=2,
                event_lookahead_days=1,
                min_long_score=1.5,
                min_short_score=99.0,
                max_new_entries_per_day=1,
                max_new_entries_per_month=None,
                allowed_sides="long",
                require_decision_for_long=True,
                require_ma_bull_stack_long=False,
                max_dist_ma20_long=0.10,
                min_volume_ratio_long=0.0,
                max_atr_pct_long=0.08,
                min_ml_p_up_long=None,
                allowed_long_setups=("long_breakout_p2",),
                allowed_short_setups=None,
                use_regime_filter=True,
                regime_breadth_lookback_days=20,
                regime_long_min_breadth_above60=0.52,
                regime_short_max_breadth_above60=0.48,
                range_bias_width_min=0.06,
                range_bias_long_pos_min=0.55,
                range_bias_short_pos_max=0.40,
                ma20_count20_min_long=10,
                ma20_count20_min_short=12,
                ma60_count60_min_long=36,
                ma60_count60_min_short=30,
            ),
        },
        {
            "strategy_id": "lr_p1_reversal_v1",
            "family": "long_reversal",
            "side": "long",
            "status": "challenger",
            "note": "Support reversal near major MA bands, intended for rebound regimes.",
            "config": StrategyBacktestConfig(
                max_positions=1,
                initial_units=1,
                add1_units=0,
                add2_units=0,
                hedge_units=0,
                min_hedge_ratio=0.0,
                cost_bps=20.0,
                min_history_bars=220,
                prefer_net_short_ratio=2.0,
                event_lookback_days=2,
                event_lookahead_days=1,
                min_long_score=1.0,
                min_short_score=99.0,
                max_new_entries_per_day=1,
                max_new_entries_per_month=None,
                allowed_sides="long",
                require_decision_for_long=False,
                require_ma_bull_stack_long=False,
                max_dist_ma20_long=0.10,
                min_volume_ratio_long=0.0,
                max_atr_pct_long=0.10,
                min_ml_p_up_long=None,
                allowed_long_setups=("long_reversal_p1",),
                allowed_short_setups=None,
                use_regime_filter=True,
                regime_breadth_lookback_days=20,
                regime_long_min_breadth_above60=0.46,
                regime_short_max_breadth_above60=0.48,
                range_bias_width_min=0.05,
                range_bias_long_pos_min=0.45,
                range_bias_short_pos_max=0.40,
                ma20_count20_min_long=8,
                ma20_count20_min_short=12,
                ma60_count60_min_long=18,
                ma60_count60_min_short=30,
            ),
        },
        {
            "strategy_id": "lp_p3_pullback_v1",
            "family": "long_pullback",
            "side": "long",
            "status": "challenger",
            "note": "Trend pullback continuation with decision confirmation and tighter ATR cap.",
            "config": StrategyBacktestConfig(
                max_positions=1,
                initial_units=1,
                add1_units=0,
                add2_units=0,
                hedge_units=0,
                min_hedge_ratio=0.0,
                cost_bps=20.0,
                min_history_bars=220,
                prefer_net_short_ratio=2.0,
                event_lookback_days=2,
                event_lookahead_days=1,
                min_long_score=1.0,
                min_short_score=99.0,
                max_new_entries_per_day=1,
                max_new_entries_per_month=None,
                allowed_sides="long",
                require_decision_for_long=True,
                require_ma_bull_stack_long=True,
                max_dist_ma20_long=0.08,
                min_volume_ratio_long=0.0,
                max_atr_pct_long=0.06,
                min_ml_p_up_long=None,
                allowed_long_setups=("long_pullback_p3",),
                allowed_short_setups=None,
                use_regime_filter=True,
                regime_breadth_lookback_days=20,
                regime_long_min_breadth_above60=0.50,
                regime_short_max_breadth_above60=0.48,
                range_bias_width_min=0.06,
                range_bias_long_pos_min=0.55,
                range_bias_short_pos_max=0.40,
                ma20_count20_min_long=12,
                ma20_count20_min_short=12,
                ma60_count60_min_long=24,
                ma60_count60_min_short=30,
            ),
        },
        {
            "strategy_id": "ld_decision_up_v1",
            "family": "long_decision",
            "side": "long",
            "status": "challenger",
            "note": "Decision-up event confirmation without breakout requirement.",
            "config": StrategyBacktestConfig(
                max_positions=1,
                initial_units=1,
                add1_units=0,
                add2_units=0,
                hedge_units=0,
                min_hedge_ratio=0.0,
                cost_bps=20.0,
                min_history_bars=220,
                prefer_net_short_ratio=2.0,
                event_lookback_days=2,
                event_lookahead_days=1,
                min_long_score=1.0,
                min_short_score=99.0,
                max_new_entries_per_day=1,
                max_new_entries_per_month=None,
                allowed_sides="long",
                require_decision_for_long=True,
                allow_decision_only_long_entries=True,
                require_ma_bull_stack_long=False,
                max_dist_ma20_long=0.12,
                min_volume_ratio_long=0.0,
                max_atr_pct_long=0.09,
                min_ml_p_up_long=None,
                allowed_long_setups=("long_decision_up",),
                allowed_short_setups=None,
                use_regime_filter=True,
                regime_breadth_lookback_days=20,
                regime_long_min_breadth_above60=0.48,
                regime_short_max_breadth_above60=0.48,
                range_bias_width_min=0.05,
                range_bias_long_pos_min=0.50,
                range_bias_short_pos_max=0.40,
                ma20_count20_min_long=8,
                ma20_count20_min_short=12,
                ma60_count60_min_long=18,
                ma60_count60_min_short=30,
            ),
        },
    ]


def seed_strategy_registry_defaults() -> dict[str, Any]:
    created_at = datetime.now(tz=timezone.utc)
    rows = _default_strategy_registry_entries()
    with get_conn() as conn:
        _ensure_strategy_registry_schema(conn)
        before = _count_rows(conn, "strategy_registry")
        for row in rows:
            conn.execute(
                """
                INSERT INTO strategy_registry (
                    strategy_id,
                    family,
                    side,
                    status,
                    config_json,
                    note,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(strategy_id) DO UPDATE SET
                    family = excluded.family,
                    side = excluded.side,
                    status = excluded.status,
                    config_json = excluded.config_json,
                    note = excluded.note,
                    updated_at = excluded.updated_at
                """,
                [
                    str(row["strategy_id"]),
                    str(row["family"]),
                    str(row["side"]),
                    str(row["status"]),
                    json.dumps(asdict(row["config"]), ensure_ascii=False),
                    row.get("note"),
                    created_at,
                    created_at,
                ],
            )
        after = _count_rows(conn, "strategy_registry")
    return {
        "table": "strategy_registry",
        "seeded": int(len(rows)),
        "rows_before": int(before),
        "rows_after": int(after),
    }


def _compute_stability_score(
    *,
    trades: int,
    win_rate: float | None,
    profit_factor: float | None,
    avg_ret_net: float | None,
    worst_dd: float | None,
) -> float:
    support = min(1.0, math.log1p(max(0, int(trades))) / math.log(51.0))
    pf_term = math.log(max(0.5, float(profit_factor or 0.5)))
    win_term = float((float(win_rate or 0.0) - 0.5) * 1.5)
    avg_term = float(avg_ret_net or 0.0) * 2.0
    dd_term = float(worst_dd or 0.0) * 0.5
    return float((pf_term + win_term + avg_term + dd_term) * support)


def build_strategy_conditional_stats(
    *,
    start_dt: int | None = None,
    end_dt: int | None = None,
    max_codes: int | None = 500,
    horizon: int = 20,
    label_version: str = "v1",
    scope_key: str | None = None,
    strategy_ids: list[str] | None = None,
) -> dict[str, Any]:
    horizon_n = max(5, int(horizon))
    scope = str(scope_key or f"top{int(max_codes) if max_codes is not None else 'all'}")
    seed_summary = seed_strategy_registry_defaults()

    with get_conn() as conn:
        _ensure_strategy_registry_schema(conn)
        _ensure_strategy_conditional_stats_schema(conn)
        _ensure_market_regime_daily_schema(conn)
        _ensure_future_pattern_daily_schema(conn)
        regime_row = conn.execute("SELECT COUNT(*) FROM market_regime_daily WHERE label_version = ?", [str(label_version)]).fetchone()
        pattern_row = conn.execute(
            "SELECT COUNT(*) FROM future_pattern_daily WHERE label_version = ? AND horizon = ?",
            [str(label_version), int(horizon_n)],
        ).fetchone()
        if not regime_row or int(regime_row[0] or 0) <= 0:
            raise RuntimeError("market_regime_daily is empty for the requested label_version")
        if not pattern_row or int(pattern_row[0] or 0) <= 0:
            raise RuntimeError("future_pattern_daily is empty for the requested label_version/horizon")

        select_sql = """
            SELECT strategy_id, family, side, status, config_json, note
            FROM strategy_registry
            WHERE status <> 'deprecated'
        """
        params: list[object] = []
        if strategy_ids:
            placeholders = ", ".join(["?"] * len(strategy_ids))
            select_sql += f" AND strategy_id IN ({placeholders})"
            params.extend([str(v) for v in strategy_ids])
        select_sql += " ORDER BY strategy_id ASC"
        strategy_rows = conn.execute(select_sql, params).fetchall()
        if not strategy_rows:
            raise RuntimeError("No active strategies found in strategy_registry")

        _delete_dt_range(
            conn,
            table_name="strategy_conditional_stats",
            dt_column="horizon",
            start_dt=None,
            end_dt=None,
            extra_where_sql="scope_key = ? AND label_version = ? AND horizon = ?",
            extra_params=[scope, str(label_version), int(horizon_n)],
        )

        market = _load_market_frame(conn, start_dt=start_dt, end_dt=end_dt, max_codes=max_codes)
        if market.empty:
            raise RuntimeError("No daily_bars rows for strategy conditional stats range")
        event_rows, _event_notes = _load_event_rows(conn)
        regime_df = conn.execute(
            """
            SELECT dt, regime_id
            FROM market_regime_daily
            WHERE label_version = ?
            """,
            [str(label_version)],
        ).df()
        pattern_df = conn.execute(
            """
            SELECT code, dt, pattern_id, mfe_20_atr, mae_20_atr, max_dd_20_atr
            FROM future_pattern_daily
            WHERE label_version = ?
              AND horizon = ?
            """,
            [str(label_version), int(horizon_n)],
        ).df()

    regime_lookup = regime_df.rename(columns={"dt": "entry_dt"})
    pattern_lookup = pattern_df.rename(columns={"dt": "entry_dt"})
    inserted_rows: list[list[object]] = []
    per_strategy_summary: list[dict[str, Any]] = []
    built_at = datetime.now(tz=timezone.utc)

    for strategy_row in strategy_rows:
        strategy_id = str(strategy_row[0])
        try:
            config_payload = json.loads(strategy_row[4]) if strategy_row[4] else {}
        except Exception:
            config_payload = {}
        cfg = _strategy_config_from_payload(config_payload if isinstance(config_payload, dict) else {})
        features = _prepare_feature_frame(market, cfg)
        features["bar_index"] = features.groupby("code", sort=False).cumcount() + 1
        features = features[
            (features["signal_ready"])
            & (features["bar_index"] >= int(max(1, cfg.min_history_bars)))
        ].copy()
        if features.empty:
            per_strategy_summary.append(
                {
                    "strategy_id": strategy_id,
                    "trade_events": 0,
                    "aggregate_rows": 0,
                    "status": "no_features",
                }
            )
            continue
        event_block = _build_event_block_set(
            features,
            event_rows,
            lookback_days=cfg.event_lookback_days,
            lookahead_days=cfg.event_lookahead_days,
        )
        result = _simulate(features, cfg, event_block, include_trade_events=True)
        trade_events = result.get("trade_events") if isinstance(result.get("trade_events"), list) else []
        if not trade_events:
            per_strategy_summary.append(
                {
                    "strategy_id": strategy_id,
                    "trade_events": 0,
                    "aggregate_rows": 0,
                    "status": "no_trades",
                }
            )
            continue
        trades_df = pd.DataFrame(trade_events)
        trades_df["entry_dt"] = trades_df["entry_dt"].astype(int)
        trades_df["ret_net"] = trades_df["ret_net"].astype(float)
        trades_df["qty"] = trades_df["qty"].astype(float)
        merged = trades_df.merge(regime_lookup, how="left", on="entry_dt")
        merged = merged.merge(pattern_lookup, how="left", on=["code", "entry_dt"])
        merged["regime_id"] = merged["regime_id"].fillna("unknown_regime")
        merged["pattern_id"] = merged["pattern_id"].fillna("unknown_pattern")

        aggregate_rows = 0
        for (regime_id, pattern_id, side), group in merged.groupby(
            ["regime_id", "pattern_id", "side"],
            dropna=False,
        ):
            trades = int(len(group))
            if trades <= 0:
                continue
            wins = int((group["ret_net"] > 0).sum())
            win_rate = float(wins / trades) if trades > 0 else None
            avg_ret_net = float(group["ret_net"].mean()) if trades > 0 else None
            ret_net_sum = float((group["ret_net"] * group["qty"]).sum()) if {"ret_net", "qty"}.issubset(group.columns) else 0.0
            pos_sum = float(group.loc[group["ret_net"] > 0, "ret_net"].sum())
            neg_sum = float(group.loc[group["ret_net"] < 0, "ret_net"].sum())
            profit_factor = (pos_sum / abs(neg_sum)) if neg_sum < 0 else None
            avg_mfe = float(group["mfe_20_atr"].mean()) if "mfe_20_atr" in group.columns and group["mfe_20_atr"].notna().any() else None
            avg_mae = float(group["mae_20_atr"].mean()) if "mae_20_atr" in group.columns and group["mae_20_atr"].notna().any() else None
            worst_dd = float(group["max_dd_20_atr"].min()) if "max_dd_20_atr" in group.columns and group["max_dd_20_atr"].notna().any() else None
            stability_score = _compute_stability_score(
                trades=trades,
                win_rate=win_rate,
                profit_factor=profit_factor,
                avg_ret_net=avg_ret_net,
                worst_dd=worst_dd,
            )
            inserted_rows.append(
                [
                    strategy_id,
                    str(regime_id),
                    str(pattern_id),
                    str(side),
                    int(horizon_n),
                    scope,
                    int(trades),
                    int(wins),
                    float(win_rate) if win_rate is not None else None,
                    float(ret_net_sum),
                    float(avg_ret_net) if avg_ret_net is not None else None,
                    float(profit_factor) if profit_factor is not None else None,
                    float(avg_mfe) if avg_mfe is not None else None,
                    float(avg_mae) if avg_mae is not None else None,
                    float(worst_dd) if worst_dd is not None else None,
                    float(stability_score),
                    str(label_version),
                    built_at,
                ]
            )
            aggregate_rows += 1
        per_strategy_summary.append(
            {
                "strategy_id": strategy_id,
                "trade_events": int(len(trades_df)),
                "aggregate_rows": int(aggregate_rows),
                "status": "ok",
            }
        )

    with get_conn() as conn:
        _ensure_strategy_conditional_stats_schema(conn)
        if inserted_rows:
            conn.executemany(
                """
                INSERT INTO strategy_conditional_stats (
                    strategy_id,
                    regime_id,
                    pattern_id,
                    side,
                    horizon,
                    scope_key,
                    trades,
                    wins,
                    win_rate,
                    ret_net_sum,
                    avg_ret_net,
                    profit_factor,
                    avg_mfe,
                    avg_mae,
                    worst_dd,
                    stability_score,
                    label_version,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                inserted_rows,
            )
        top_rows = conn.execute(
            """
            SELECT strategy_id, COUNT(*) AS row_count, SUM(trades) AS trade_count
            FROM strategy_conditional_stats
            WHERE scope_key = ?
              AND label_version = ?
              AND horizon = ?
            GROUP BY strategy_id
            ORDER BY trade_count DESC, strategy_id ASC
            """,
            [scope, str(label_version), int(horizon_n)],
        ).df()
    return {
        "seed_strategy_registry": seed_summary,
        "table": "strategy_conditional_stats",
        "rows": int(len(inserted_rows)),
        "scope_key": scope,
        "label_version": str(label_version),
        "horizon": int(horizon_n),
        "max_codes": int(max_codes) if max_codes is not None else None,
        "per_strategy": per_strategy_summary,
        "summary_by_strategy": [
            {
                "strategy_id": str(row["strategy_id"]),
                "row_count": int(row["row_count"]),
                "trade_count": int(row["trade_count"]),
            }
            for _, row in top_rows.iterrows()
        ],
    }


def _ensure_router_daily_candidates_schema(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS router_daily_candidates (
            dt INTEGER NOT NULL,
            code TEXT NOT NULL,
            sector33_code TEXT,
            regime_id TEXT NOT NULL,
            state_bucket TEXT NOT NULL,
            dominant_pattern_id TEXT,
            recommended_strategy_id TEXT,
            recommended_side TEXT NOT NULL,
            action TEXT NOT NULL,
            expected_return DOUBLE,
            expected_profit_factor DOUBLE,
            expected_win_rate DOUBLE,
            expected_worst_dd DOUBLE,
            expected_stability DOUBLE,
            router_score DOUBLE,
            confidence DOUBLE,
            trades_support INTEGER,
            rank_in_day INTEGER,
            scope_key TEXT NOT NULL,
            label_version TEXT NOT NULL,
            updated_at TIMESTAMP NOT NULL,
            reason_json TEXT,
            PRIMARY KEY (dt, code, scope_key, label_version)
        );
        """
    )


def _router_feature_config() -> StrategyBacktestConfig:
    return StrategyBacktestConfig(
        max_positions=1,
        initial_units=1,
        add1_units=0,
        add2_units=0,
        hedge_units=0,
        min_hedge_ratio=0.0,
        cost_bps=20.0,
        min_history_bars=220,
        prefer_net_short_ratio=2.0,
        event_lookback_days=2,
        event_lookahead_days=1,
        min_long_score=1.0,
        min_short_score=99.0,
        max_new_entries_per_day=1,
        max_new_entries_per_month=None,
        allowed_sides="long",
        require_decision_for_long=False,
        require_ma_bull_stack_long=False,
        max_dist_ma20_long=None,
        min_volume_ratio_long=0.0,
        max_atr_pct_long=None,
        min_ml_p_up_long=None,
        allowed_long_setups=("long_breakout_p2",),
        allowed_short_setups=None,
        use_regime_filter=False,
        regime_breadth_lookback_days=20,
        regime_long_min_breadth_above60=0.52,
        regime_short_max_breadth_above60=0.48,
        range_bias_width_min=0.06,
        range_bias_long_pos_min=0.55,
        range_bias_short_pos_max=0.40,
        ma20_count20_min_long=10,
        ma20_count20_min_short=12,
        ma60_count60_min_long=24,
        ma60_count60_min_short=30,
    )


def _weighted_metric_or_none(group: pd.DataFrame, value_col: str, weight_col: str = "trades") -> float | None:
    if value_col not in group.columns or weight_col not in group.columns:
        return None
    values = pd.to_numeric(group[value_col], errors="coerce")
    weights = pd.to_numeric(group[weight_col], errors="coerce").fillna(0.0)
    mask = values.notna() & weights.gt(0)
    if not bool(mask.any()):
        return None
    return float(np.average(values[mask], weights=weights[mask]))


def _aggregate_router_stats_frame(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=[
                *group_cols,
                "trades",
                "wins",
                "win_rate",
                "ret_net_sum",
                "avg_ret_net",
                "profit_factor",
                "avg_mfe",
                "avg_mae",
                "worst_dd",
                "stability_score",
            ]
        )
    rows: list[dict[str, Any]] = []
    for keys, group in df.groupby(group_cols, dropna=False):
        keys_tuple = keys if isinstance(keys, tuple) else (keys,)
        trades = int(pd.to_numeric(group["trades"], errors="coerce").fillna(0).sum())
        wins = int(pd.to_numeric(group["wins"], errors="coerce").fillna(0).sum()) if "wins" in group.columns else 0
        dd_values = pd.to_numeric(group["worst_dd"], errors="coerce").dropna() if "worst_dd" in group.columns else pd.Series(dtype=float)
        row = {
            **{col: keys_tuple[idx] for idx, col in enumerate(group_cols)},
            "trades": trades,
            "wins": wins,
            "win_rate": float(wins / trades) if trades > 0 else None,
            "ret_net_sum": float(pd.to_numeric(group["ret_net_sum"], errors="coerce").fillna(0.0).sum()),
            "avg_ret_net": _weighted_metric_or_none(group, "avg_ret_net"),
            "profit_factor": _weighted_metric_or_none(group, "profit_factor"),
            "avg_mfe": _weighted_metric_or_none(group, "avg_mfe"),
            "avg_mae": _weighted_metric_or_none(group, "avg_mae"),
            "worst_dd": float(dd_values.min()) if not dd_values.empty else None,
            "stability_score": _weighted_metric_or_none(group, "stability_score"),
        }
        rows.append(row)
    return pd.DataFrame(rows)


def _build_pattern_prob_lookup(base_counts: pd.DataFrame, key_cols: list[str]) -> dict[tuple[str, ...], dict[str, Any]]:
    lookup: dict[tuple[str, ...], dict[str, Any]] = {}
    if base_counts.empty:
        return lookup
    for keys, group in base_counts.groupby(key_cols, dropna=False):
        keys_tuple = keys if isinstance(keys, tuple) else (keys,)
        total = int(pd.to_numeric(group["count"], errors="coerce").fillna(0).sum())
        if total <= 0:
            continue
        lookup[tuple("" if v is None else str(v) for v in keys_tuple)] = {
            "support": total,
            "probs": {
                str(row["pattern_id"]): float(float(row["count"]) / float(total))
                for _, row in group.iterrows()
                if pd.notna(row["pattern_id"]) and float(row["count"]) > 0
            },
        }
    return lookup


def _resolve_pattern_probs(
    *,
    regime_id: str,
    state_bucket: str,
    exact_lookup: dict[tuple[str, ...], dict[str, Any]],
    regime_lookup: dict[tuple[str, ...], dict[str, Any]],
    state_lookup: dict[tuple[str, ...], dict[str, Any]],
    global_entry: dict[str, Any] | None,
    min_support: int,
) -> tuple[dict[str, float], str, int]:
    exact = exact_lookup.get((str(regime_id), str(state_bucket)))
    if exact and int(exact.get("support") or 0) >= int(min_support):
        return dict(exact.get("probs") or {}), "regime_state", int(exact.get("support") or 0)
    regime_only = regime_lookup.get((str(regime_id),))
    if regime_only and int(regime_only.get("support") or 0) >= int(min_support):
        return dict(regime_only.get("probs") or {}), "regime", int(regime_only.get("support") or 0)
    state_only = state_lookup.get((str(state_bucket),))
    if state_only and int(state_only.get("support") or 0) >= int(min_support):
        return dict(state_only.get("probs") or {}), "state", int(state_only.get("support") or 0)
    if global_entry:
        return dict(global_entry.get("probs") or {}), "global", int(global_entry.get("support") or 0)
    return {}, "empty", 0


def _resolve_strategy_stat(
    *,
    strategy_id: str,
    side: str,
    regime_id: str,
    pattern_id: str,
    exact_lookup: dict[tuple[str, ...], dict[str, Any]],
    regime_lookup: dict[tuple[str, ...], dict[str, Any]],
    strategy_lookup: dict[tuple[str, ...], dict[str, Any]],
) -> tuple[dict[str, Any] | None, str]:
    exact = exact_lookup.get((str(strategy_id), str(regime_id), str(pattern_id), str(side)))
    if exact and int(exact.get("trades") or 0) >= 5:
        return exact, "exact"
    regime_only = regime_lookup.get((str(strategy_id), str(regime_id), str(side)))
    if regime_only and int(regime_only.get("trades") or 0) >= 15:
        return regime_only, "regime"
    strategy_only = strategy_lookup.get((str(strategy_id), str(side)))
    if strategy_only and int(strategy_only.get("trades") or 0) >= 40:
        return strategy_only, "strategy"
    return None, "missing"


def _score_router_strategy(
    *,
    strategy_id: str,
    side: str,
    regime_id: str,
    pattern_probs: dict[str, float],
    candidate_bias: float,
    setup_bonus: float,
    exact_lookup: dict[tuple[str, ...], dict[str, Any]],
    regime_lookup: dict[tuple[str, ...], dict[str, Any]],
    strategy_lookup: dict[tuple[str, ...], dict[str, Any]],
) -> dict[str, Any]:
    expected_return = 0.0
    expected_pf_log = 0.0
    expected_win_rate = 0.0
    expected_worst_dd = 0.0
    expected_stability = 0.0
    weighted_support = 0.0
    source_hits = {"exact": 0, "regime": 0, "strategy": 0, "missing": 0}
    for pattern_id, prob in pattern_probs.items():
        weight = float(prob)
        if weight <= 0:
            continue
        stat_row, source_level = _resolve_strategy_stat(
            strategy_id=strategy_id,
            side=side,
            regime_id=regime_id,
            pattern_id=pattern_id,
            exact_lookup=exact_lookup,
            regime_lookup=regime_lookup,
            strategy_lookup=strategy_lookup,
        )
        source_hits[source_level] = source_hits.get(source_level, 0) + 1
        if not stat_row:
            continue
        expected_return += weight * float(stat_row.get("avg_ret_net") or 0.0)
        expected_pf_log += weight * math.log(max(0.5, float(stat_row.get("profit_factor") or 0.5)))
        expected_win_rate += weight * float(stat_row.get("win_rate") or 0.0)
        expected_worst_dd += weight * float(stat_row.get("worst_dd") or -1.5)
        expected_stability += weight * float(stat_row.get("stability_score") or 0.0)
        weighted_support += weight * float(stat_row.get("trades") or 0.0)
    support_term = min(1.0, math.log1p(max(0.0, weighted_support)) / math.log(101.0))
    raw_score = (
        (expected_return * 9.0)
        + (expected_pf_log * 0.9)
        + ((expected_win_rate - 0.5) * 1.6)
        + (expected_stability * 0.65)
        + (expected_worst_dd * 0.35)
    )
    dominant_source = max(source_hits.items(), key=lambda kv: kv[1])[0] if source_hits else "missing"
    fallback_penalty = {
        "exact": 0.0,
        "regime": 0.08,
        "strategy": 0.15,
        "missing": 0.20,
    }.get(dominant_source, 0.10)
    dd_penalty = max(0.0, (-1.2 - float(expected_worst_dd))) * 0.8 if expected_worst_dd < -1.2 else 0.0
    router_score = float(
        (raw_score * (0.55 + 0.45 * support_term))
        + float(candidate_bias)
        + float(setup_bonus)
        - float(fallback_penalty)
        - float(dd_penalty)
    )
    confidence = float(min(1.0, (0.5 * support_term) + (0.5 * max(pattern_probs.values(), default=0.0))))
    return {
        "strategy_id": str(strategy_id),
        "side": str(side),
        "expected_return": float(expected_return),
        "expected_profit_factor": float(math.exp(expected_pf_log)) if pattern_probs else 0.0,
        "expected_win_rate": float(expected_win_rate),
        "expected_worst_dd": float(expected_worst_dd),
        "expected_stability": float(expected_stability),
        "trades_support": int(round(weighted_support)),
        "router_score": float(router_score),
        "confidence": confidence,
        "dominant_source": dominant_source,
        "setup_bonus": float(setup_bonus),
    }


def build_router_daily_candidates(
    *,
    start_dt: int | None = None,
    end_dt: int | None = None,
    max_codes: int | None = 500,
    horizon: int = 20,
    label_version: str = "v1",
    scope_key: str | None = None,
    top_n_per_day: int = 25,
    min_pattern_support: int = 40,
    min_router_score: float = -0.25,
    candidate_long_score_min: float = 2.0,
) -> dict[str, Any]:
    horizon_n = max(5, int(horizon))
    scope = str(scope_key or f"top{int(max_codes) if max_codes is not None else 'all'}")

    with get_conn() as conn:
        _ensure_router_daily_candidates_schema(conn)
        _ensure_strategy_registry_schema(conn)
        _ensure_strategy_conditional_stats_schema(conn)
        _ensure_market_regime_daily_schema(conn)
        _ensure_future_pattern_daily_schema(conn)
        cond_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM strategy_conditional_stats
            WHERE scope_key = ?
              AND label_version = ?
              AND horizon = ?
            """,
            [scope, str(label_version), int(horizon_n)],
        ).fetchone()
        if not cond_count or int(cond_count[0] or 0) <= 0:
            raise RuntimeError("strategy_conditional_stats is empty for the requested scope/label_version/horizon")
        market = _load_market_frame(conn, start_dt=None, end_dt=end_dt, max_codes=max_codes)
        if market.empty:
            raise RuntimeError("No daily_bars rows for router candidate build")
        regime_df = conn.execute(
            """
            SELECT dt, regime_id
            FROM market_regime_daily
            WHERE label_version = ?
            """,
            [str(label_version)],
        ).df()
        pattern_df = conn.execute(
            """
            SELECT code, dt, pattern_id
            FROM future_pattern_daily
            WHERE label_version = ?
              AND horizon = ?
            """,
            [str(label_version), int(horizon_n)],
        ).df()
        cond_df = conn.execute(
            """
            SELECT
                strategy_id,
                regime_id,
                pattern_id,
                side,
                trades,
                wins,
                win_rate,
                ret_net_sum,
                avg_ret_net,
                profit_factor,
                avg_mfe,
                avg_mae,
                worst_dd,
                stability_score
            FROM strategy_conditional_stats
            WHERE scope_key = ?
              AND label_version = ?
              AND horizon = ?
            ORDER BY strategy_id ASC, regime_id ASC, pattern_id ASC
            """,
            [scope, str(label_version), int(horizon_n)],
        ).df()
        strategy_meta_df = conn.execute(
            """
            SELECT strategy_id, family, side, status, config_json
            FROM strategy_registry
            WHERE status <> 'deprecated'
            ORDER BY strategy_id ASC
            """
        ).df()

    cfg = _router_feature_config()
    features = _prepare_feature_frame(market, cfg)
    if features.empty:
        raise RuntimeError("No features available for router candidate build")
    features = features[(features["signal_ready"])].copy()
    if features.empty:
        raise RuntimeError("signal_ready features are empty for router candidate build")

    latest_dt = int(features["dt"].max())
    target_start = int(start_dt) if start_dt is not None else latest_dt
    target_end = int(end_dt) if end_dt is not None else target_start
    if target_end < target_start:
        raise ValueError("end_dt must be greater than or equal to start_dt")

    close_series = pd.to_numeric(features["c"], errors="coerce")
    atr_series = pd.to_numeric(features["atr14"], errors="coerce")
    vol_ma20_series = pd.to_numeric(features["vol_ma20"], errors="coerce")
    volume_series = pd.to_numeric(features["v"], errors="coerce")
    features["atr_pct_router"] = np.where(close_series.abs() > 1e-12, atr_series / close_series, np.nan)
    features["vol_ratio_router"] = np.where(vol_ma20_series.abs() > 1e-12, volume_series / vol_ma20_series, np.nan)

    setup_conditions = [
        features["buy_p2"].fillna(False),
        features["buy_p1"].fillna(False),
        features["buy_p3"].fillna(False),
        features["decision_up"].fillna(False) & features["ma_up_persist_long"].fillna(False),
        features["decision_up"].fillna(False),
    ]
    setup_choices = ["breakout", "support_reversal", "pullback_resume", "decision_trend", "decision_only"]
    features["setup_bucket"] = np.select(setup_conditions, setup_choices, default="neutral")
    features["current_setup_id"] = np.select(
        setup_conditions,
        ["long_breakout_p2", "long_reversal_p1", "long_pullback_p3", "long_decision_up", "long_decision_up"],
        default="long_entry",
    )
    trend_conditions = [
        features["above20"].fillna(False) & features["above60"].fillna(False) & (pd.to_numeric(features["ma20_slope5"], errors="coerce") > 0),
        features["above20"].fillna(False) & (pd.to_numeric(features["ma20_slope5"], errors="coerce") >= 0),
        features["range_bias_long"].fillna(False) | features["sideways_10_20"].fillna(False),
    ]
    trend_choices = ["trend", "transition", "range"]
    features["trend_bucket"] = np.select(trend_conditions, trend_choices, default="weak")
    vol_conditions = [
        features["atr_pct_router"].notna() & (features["atr_pct_router"] <= 0.025),
        features["atr_pct_router"].notna() & (features["atr_pct_router"] <= 0.05),
    ]
    vol_choices = ["lowvol", "midvol"]
    features["vol_bucket"] = np.select(vol_conditions, vol_choices, default="highvol")
    features.loc[features["atr_pct_router"].isna(), "vol_bucket"] = "unkvol"
    features["state_bucket"] = (
        features["setup_bucket"].astype(str)
        + "|"
        + features["trend_bucket"].astype(str)
        + "|"
        + features["vol_bucket"].astype(str)
    )
    features["router_candidate"] = (
        features["entry_long"].fillna(False)
        | features["decision_up"].fillna(False)
        | (pd.to_numeric(features["long_score"], errors="coerce").fillna(0.0) >= float(candidate_long_score_min))
    ) & features["above20"].fillna(False)
    features["candidate_bias"] = (
        features["buy_p2"].fillna(False).astype(float) * 0.35
        + features["buy_p1"].fillna(False).astype(float) * 0.18
        + features["buy_p3"].fillna(False).astype(float) * 0.14
        + features["decision_up"].fillna(False).astype(float) * 0.12
        + features["ma_up_persist_long"].fillna(False).astype(float) * 0.08
        + features["range_bias_long"].fillna(False).astype(float) * 0.05
        + (features["vol_ratio_router"].fillna(0.0) >= 1.2).astype(float) * 0.05
        - (features["atr_pct_router"].fillna(0.0) > 0.09).astype(float) * 0.10
        - (~features["above60"].fillna(False)).astype(float) * 0.08
    )

    regime_lookup_df = regime_df.copy()
    pattern_lookup_df = pattern_df.copy()
    hist = features[features["dt"] < int(target_start)].copy()
    hist = hist.merge(regime_lookup_df, how="left", on="dt")
    hist = hist.merge(pattern_lookup_df, how="left", on=["code", "dt"])
    hist["regime_id"] = hist["regime_id"].fillna("unknown_regime")
    hist = hist[hist["pattern_id"].notna()].copy()
    if hist.empty:
        raise RuntimeError("No historical labeled rows available before target_start for router priors")

    hist_counts = (
        hist.groupby(["regime_id", "state_bucket", "pattern_id"], dropna=False)
        .size()
        .reset_index(name="count")
    )
    regime_counts = (
        hist.groupby(["regime_id", "pattern_id"], dropna=False)
        .size()
        .reset_index(name="count")
    )
    state_counts = (
        hist.groupby(["state_bucket", "pattern_id"], dropna=False)
        .size()
        .reset_index(name="count")
    )
    global_counts = hist.groupby(["pattern_id"], dropna=False).size().reset_index(name="count")
    exact_pattern_lookup = _build_pattern_prob_lookup(hist_counts, ["regime_id", "state_bucket"])
    regime_pattern_lookup = _build_pattern_prob_lookup(regime_counts, ["regime_id"])
    state_pattern_lookup = _build_pattern_prob_lookup(state_counts, ["state_bucket"])
    global_pattern_lookup = _build_pattern_prob_lookup(global_counts.assign(bucket="global"), ["bucket"])
    global_entry = global_pattern_lookup.get(("global",))

    cond_long = cond_df[cond_df["side"].astype(str) == "long"].copy()
    if cond_long.empty:
        raise RuntimeError("No long-side rows found in strategy_conditional_stats")
    exact_stats_lookup = {
        (
            str(row["strategy_id"]),
            str(row["regime_id"]),
            str(row["pattern_id"]),
            str(row["side"]),
        ): row.to_dict()
        for _, row in cond_long.iterrows()
    }
    regime_stats_df = _aggregate_router_stats_frame(cond_long, ["strategy_id", "regime_id", "side"])
    strategy_stats_df = _aggregate_router_stats_frame(cond_long, ["strategy_id", "side"])
    regime_stats_lookup = {
        (str(row["strategy_id"]), str(row["regime_id"]), str(row["side"])): row.to_dict()
        for _, row in regime_stats_df.iterrows()
    }
    strategy_stats_lookup = {
        (str(row["strategy_id"]), str(row["side"])): row.to_dict()
        for _, row in strategy_stats_df.iterrows()
    }
    strategy_ids = sorted({str(v) for v in cond_long["strategy_id"].astype(str).tolist()})
    strategy_meta_by_id: dict[str, dict[str, Any]] = {}
    if not strategy_meta_df.empty:
        for _, row in strategy_meta_df.iterrows():
            payload_raw = row.get("config_json")
            try:
                payload = json.loads(payload_raw) if payload_raw else {}
            except Exception:
                payload = {}
            cfg_payload = payload if isinstance(payload, dict) else {}
            allowed_long_setups = cfg_payload.get("allowed_long_setups")
            strategy_meta_by_id[str(row["strategy_id"])] = {
                "family": str(row.get("family") or ""),
                "side": str(row.get("side") or ""),
                "allowed_long_setups": {
                    str(v) for v in (allowed_long_setups or []) if v is not None
                },
            }

    target = features[(features["dt"] >= int(target_start)) & (features["dt"] <= int(target_end)) & (features["router_candidate"])].copy()
    target = target.merge(regime_lookup_df, how="left", on="dt")
    target["regime_id"] = target["regime_id"].fillna("unknown_regime")
    if target.empty:
        with get_conn() as conn:
            _ensure_router_daily_candidates_schema(conn)
            _delete_dt_range(
                conn,
                table_name="router_daily_candidates",
                dt_column="dt",
                start_dt=int(target_start),
                end_dt=int(target_end),
                extra_where_sql="scope_key = ? AND label_version = ?",
                extra_params=[scope, str(label_version)],
            )
        return {
            "table": "router_daily_candidates",
            "rows": 0,
            "target_start_dt": int(target_start),
            "target_end_dt": int(target_end),
            "target_dates": 0,
            "scope_key": scope,
            "label_version": str(label_version),
            "horizon": int(horizon_n),
            "message": "No router candidates matched the current filters.",
        }

    inserted_rows: list[list[object]] = []
    rendered_rows: list[dict[str, Any]] = []
    built_at = datetime.now(tz=timezone.utc)
    keep_n = max(1, int(top_n_per_day))
    fallback_watch_n = min(5, keep_n)
    min_score = float(min_router_score)

    for dt_value, day_group in target.groupby("dt", sort=True):
        ranked_candidates: list[dict[str, Any]] = []
        for row in day_group.to_dict("records"):
            current_setup_id = str(row.get("current_setup_id") or "long_entry")
            pattern_probs, lookup_level, lookup_support = _resolve_pattern_probs(
                regime_id=str(row.get("regime_id") or "unknown_regime"),
                state_bucket=str(row.get("state_bucket") or "unknown_state"),
                exact_lookup=exact_pattern_lookup,
                regime_lookup=regime_pattern_lookup,
                state_lookup=state_pattern_lookup,
                global_entry=global_entry,
                min_support=int(min_pattern_support),
            )
            if not pattern_probs:
                continue
            scored = [
                _score_router_strategy(
                    strategy_id=strategy_id,
                    side="long",
                    regime_id=str(row.get("regime_id") or "unknown_regime"),
                    pattern_probs=pattern_probs,
                    candidate_bias=float(row.get("candidate_bias") or 0.0),
                    setup_bonus=(
                        0.18
                        if current_setup_id in strategy_meta_by_id.get(strategy_id, {}).get("allowed_long_setups", set())
                        else (
                            -0.12
                            if (
                                current_setup_id != "long_entry"
                                and bool(strategy_meta_by_id.get(strategy_id, {}).get("allowed_long_setups"))
                            )
                            else 0.0
                        )
                    ),
                    exact_lookup=exact_stats_lookup,
                    regime_lookup=regime_stats_lookup,
                    strategy_lookup=strategy_stats_lookup,
                )
                for strategy_id in strategy_ids
            ]
            scored = [entry for entry in scored if math.isfinite(float(entry.get("router_score") or 0.0))]
            if not scored:
                continue
            scored.sort(key=lambda item: (float(item["router_score"]), float(item["expected_return"])), reverse=True)
            best = scored[0]
            dominant_pattern_id = max(pattern_probs.items(), key=lambda kv: kv[1])[0]
            action = "watch"
            if (
                float(best["expected_return"]) > 0
                and float(best["expected_profit_factor"]) >= 1.02
                and float(best["expected_worst_dd"]) >= -1.2
                and float(best["confidence"]) >= 0.45
            ):
                action = "long"
            feature_flags = [
                name
                for name, enabled in [
                    ("buy_p2", bool(row.get("buy_p2"))),
                    ("buy_p1", bool(row.get("buy_p1"))),
                    ("buy_p3", bool(row.get("buy_p3"))),
                    ("decision_up", bool(row.get("decision_up"))),
                    ("ma_up_persist_long", bool(row.get("ma_up_persist_long"))),
                    ("range_bias_long", bool(row.get("range_bias_long"))),
                ]
                if enabled
            ]
            reason_payload = {
                "lookup_level": lookup_level,
                "lookup_support": int(lookup_support),
                "current_setup_id": current_setup_id,
                "threshold_passed": bool(float(best["router_score"]) >= min_score),
                "top_patterns": [
                    {"pattern_id": str(pattern_id), "prob": round(float(prob), 4)}
                    for pattern_id, prob in sorted(pattern_probs.items(), key=lambda kv: kv[1], reverse=True)[:3]
                ],
                "top_strategies": [
                    {
                        "strategy_id": str(item["strategy_id"]),
                        "router_score": round(float(item["router_score"]), 4),
                        "expected_return": round(float(item["expected_return"]), 4),
                        "setup_bonus": round(float(item.get("setup_bonus") or 0.0), 4),
                    }
                    for item in scored[:3]
                ],
                "feature_flags": feature_flags,
            }
            ranked_candidates.append(
                {
                    "dt": int(dt_value),
                    "code": str(row.get("code") or ""),
                    "sector33_code": row.get("sector33_code"),
                    "regime_id": str(row.get("regime_id") or "unknown_regime"),
                    "state_bucket": str(row.get("state_bucket") or "unknown_state"),
                    "dominant_pattern_id": str(dominant_pattern_id),
                    "recommended_strategy_id": str(best["strategy_id"]),
                    "recommended_side": "long",
                    "action": str(action),
                    "expected_return": float(best["expected_return"]),
                    "expected_profit_factor": float(best["expected_profit_factor"]),
                    "expected_win_rate": float(best["expected_win_rate"]),
                    "expected_worst_dd": float(best["expected_worst_dd"]),
                    "expected_stability": float(best["expected_stability"]),
                    "router_score": float(best["router_score"]),
                    "confidence": float(best["confidence"]),
                    "trades_support": int(best["trades_support"]),
                    "reason_json": json.dumps(reason_payload, ensure_ascii=False),
                    "threshold_passed": bool(float(best["router_score"]) >= min_score),
                }
            )
        ranked_candidates.sort(
            key=lambda item: (float(item["router_score"]), float(item["expected_return"]), float(item["confidence"])),
            reverse=True,
        )
        selected_candidates = [item for item in ranked_candidates if bool(item.get("threshold_passed"))]
        if not selected_candidates:
            selected_candidates = ranked_candidates[:fallback_watch_n]
        else:
            selected_candidates = selected_candidates[:keep_n]
        for rank_idx, item in enumerate(selected_candidates, start=1):
            item["rank_in_day"] = int(rank_idx)
            inserted_rows.append(
                [
                    int(item["dt"]),
                    str(item["code"]),
                    item.get("sector33_code"),
                    str(item["regime_id"]),
                    str(item["state_bucket"]),
                    str(item["dominant_pattern_id"]),
                    str(item["recommended_strategy_id"]),
                    str(item["recommended_side"]),
                    str(item["action"]),
                    float(item["expected_return"]),
                    float(item["expected_profit_factor"]),
                    float(item["expected_win_rate"]),
                    float(item["expected_worst_dd"]),
                    float(item["expected_stability"]),
                    float(item["router_score"]),
                    float(item["confidence"]),
                    int(item["trades_support"]),
                    int(item["rank_in_day"]),
                    scope,
                    str(label_version),
                    built_at,
                    str(item["reason_json"]),
                ]
            )
            rendered_rows.append(item)

    with get_conn() as conn:
        _ensure_router_daily_candidates_schema(conn)
        _delete_dt_range(
            conn,
            table_name="router_daily_candidates",
            dt_column="dt",
            start_dt=int(target_start),
            end_dt=int(target_end),
            extra_where_sql="scope_key = ? AND label_version = ?",
            extra_params=[scope, str(label_version)],
        )
        if inserted_rows:
            conn.executemany(
                """
                INSERT INTO router_daily_candidates (
                    dt,
                    code,
                    sector33_code,
                    regime_id,
                    state_bucket,
                    dominant_pattern_id,
                    recommended_strategy_id,
                    recommended_side,
                    action,
                    expected_return,
                    expected_profit_factor,
                    expected_win_rate,
                    expected_worst_dd,
                    expected_stability,
                    router_score,
                    confidence,
                    trades_support,
                    rank_in_day,
                    scope_key,
                    label_version,
                    updated_at,
                    reason_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                inserted_rows,
            )

    summary_rows = (
        pd.DataFrame(rendered_rows)
        if rendered_rows
        else pd.DataFrame(columns=["dt", "action", "code", "recommended_strategy_id", "router_score"])
    )
    action_breakdown = []
    top_by_dt = []
    if not summary_rows.empty:
        action_counts = summary_rows.groupby("action").size().sort_values(ascending=False)
        action_breakdown = [
            {"action": str(action), "rows": int(count)}
            for action, count in action_counts.items()
        ]
        for dt_value, group in summary_rows.groupby("dt", sort=True):
            best = group.sort_values(["router_score", "expected_return"], ascending=False).iloc[0]
            top_by_dt.append(
                {
                    "dt": int(dt_value),
                    "rows": int(len(group)),
                    "best_code": str(best["code"]),
                    "best_strategy_id": str(best["recommended_strategy_id"]),
                    "best_score": float(best["router_score"]),
                }
            )
    return {
        "table": "router_daily_candidates",
        "rows": int(len(inserted_rows)),
        "target_start_dt": int(target_start),
        "target_end_dt": int(target_end),
        "target_dates": int(summary_rows["dt"].nunique()) if not summary_rows.empty else 0,
        "scope_key": scope,
        "label_version": str(label_version),
        "horizon": int(horizon_n),
        "max_codes": int(max_codes) if max_codes is not None else None,
        "top_n_per_day": int(keep_n),
        "min_pattern_support": int(min_pattern_support),
        "action_breakdown": action_breakdown,
        "top_by_dt": top_by_dt[:10],
    }
