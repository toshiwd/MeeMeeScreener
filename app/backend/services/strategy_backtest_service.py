from __future__ import annotations

import bisect
import json
import math
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from typing import Any

import numpy as np
import pandas as pd

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
    has_ml_pred = _table_exists(conn, "ml_pred_20d")
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
                and bool(row.get("entry_long"))
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
) -> dict[str, Any]:
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
    if market.empty:
        raise RuntimeError("No daily_bars rows for walkforward range")

    features = _prepare_feature_frame(market, cfg)
    features["bar_index"] = features.groupby("code", sort=False).cumcount() + 1
    features = features[
        (features["signal_ready"])
        & (features["bar_index"] >= int(max(1, cfg.min_history_bars)))
    ].copy()
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

    if not dry_run:
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
    return report


def run_strategy_walkforward_gate(
    *,
    min_oos_total_realized_unit_pnl: float = 0.0,
    min_oos_mean_profit_factor: float = 1.05,
    min_oos_positive_window_ratio: float = 0.40,
    min_oos_worst_max_drawdown_unit: float = -0.12,
    dry_run: bool = False,
    note: str | None = None,
) -> dict[str, Any]:
    gate_id = datetime.now(tz=timezone.utc).strftime("swfg_%Y%m%d%H%M%S_%f")
    created_at = datetime.now(tz=timezone.utc)

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

        source_run_id = str(row[0])
        source_finished_at = row[1]
        source_status = str(row[2]) if row[2] is not None else None
        try:
            report_json = json.loads(row[3]) if row[3] else {}
        except Exception:
            report_json = {}
        source_summary = (report_json.get("summary") or {})
        source_windowing = (report_json.get("windowing") or {})
        report = _build_walkforward_gate_report(
            gate_id=gate_id,
            created_at=created_at,
            source_run_id=source_run_id,
            source_finished_at=source_finished_at,
            source_status=source_status,
            source_summary=source_summary,
            source_windowing=source_windowing,
            min_oos_total_realized_unit_pnl=float(min_oos_total_realized_unit_pnl),
            min_oos_mean_profit_factor=float(min_oos_mean_profit_factor),
            min_oos_positive_window_ratio=float(min_oos_positive_window_ratio),
            min_oos_worst_max_drawdown_unit=float(min_oos_worst_max_drawdown_unit),
            note=note,
        )
        if not dry_run:
            _save_walkforward_gate_report(
                conn,
                gate_id=gate_id,
                created_at=created_at,
                source_run_id=source_run_id,
                source_finished_at=source_finished_at,
                status=str(report.get("status") or "fail"),
                thresholds=report.get("thresholds") or {},
                report=report,
                note=note,
            )
    report["dry_run"] = bool(dry_run)
    return report


def get_latest_strategy_walkforward() -> dict[str, Any]:
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
