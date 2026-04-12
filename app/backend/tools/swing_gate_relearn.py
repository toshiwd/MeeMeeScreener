from __future__ import annotations

import argparse
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from app.backend.tools.weekly_top_gainers_study import _load_daily_frame
from app.db.session import get_conn

DEFAULT_LOOKBACK_DAYS = 365 * 30
DEFAULT_INITIAL_CAPITAL = 10_000_000.0
DEFAULT_TRAIN_END_YMD = 20161230
DEFAULT_COST = 0.001
DEFAULT_REPORT_DIR = Path("G:/Tradex/reports")


@dataclass(frozen=True)
class RelearnConfig:
    lookback_days: int = DEFAULT_LOOKBACK_DAYS
    initial_capital: float = DEFAULT_INITIAL_CAPITAL
    train_end_ymd: int = DEFAULT_TRAIN_END_YMD
    transaction_cost_rate: float = DEFAULT_COST
    report_dir: Path = DEFAULT_REPORT_DIR
    max_positions_buy: int = 5
    max_positions_sell: int = 3
    review_day_1: int = 3
    review_day_2: int = 5
    review_1_mfe_min: float = 0.01
    review_1_ret_min: float = 0.0
    review_2_mfe_min: float = 0.02
    review_2_ret_min: float = 0.0


@dataclass(frozen=True)
class CodeHistory:
    code: str
    ymds: np.ndarray
    opens: np.ndarray
    highs: np.ndarray
    lows: np.ndarray
    closes: np.ndarray


def _fmt_pct(value: Any) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value):.2%}"


def _fmt_num(value: Any, digits: int = 3) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value):.{digits}f}"


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_jsonable(v) for v in value]
    if isinstance(value, tuple):
        return [_jsonable(v) for v in value]
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if isinstance(value, np.generic):
        return value.item()
    return value


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table_name],
    ).fetchone()
    return bool(row and row[0])


def _build_histories(daily: pd.DataFrame) -> dict[str, CodeHistory]:
    out: dict[str, CodeHistory] = {}
    for code, group in daily.groupby("code", sort=False):
        g = group.sort_values("date_dt")
        out[str(code)] = CodeHistory(
            code=str(code),
            ymds=g["ymd"].astype(int).to_numpy(),
            opens=g["o"].astype(float).to_numpy(),
            highs=g["h"].astype(float).to_numpy(),
            lows=g["l"].astype(float).to_numpy(),
            closes=g["c"].astype(float).to_numpy(),
        )
    return out


def _next_entry_idx(history: CodeHistory, signal_ymd: int) -> int | None:
    idx = int(np.searchsorted(history.ymds, int(signal_ymd) + 1, side="left"))
    return None if idx >= history.ymds.size else idx


def _load_signal_frame(conn, *, start_ymd: int, end_ymd: int) -> pd.DataFrame:
    if not _table_exists(conn, "signal_decision_daily") or not _table_exists(conn, "ml_feature_daily"):
        return pd.DataFrame()
    frame = conn.execute(
        """
        SELECT
            s.dt AS signal_dt,
            s.code,
            s.side,
            s.setup_type,
            s.entry_qualified,
            s.forward_return_5,
            s.forward_return_10,
            s.forward_return_20,
            s.forward_return_30,
            s.max_favorable_30,
            s.max_adverse_30,
            CAST(json_extract(s.score_snapshot_json, '$.entryScore') AS DOUBLE) AS entry_score,
            f.close,
            f.ma7,
            f.ma20,
            f.ma60,
            f.candle_body_ratio AS body_ratio,
            f.candle_upper_wick_ratio AS upper_wick_ratio,
            f.candle_lower_wick_ratio AS lower_wick_ratio,
            r.regime_id
        FROM signal_decision_daily s
        LEFT JOIN ml_feature_daily f
            ON f.dt = epoch(strptime(cast(s.dt AS VARCHAR), '%Y%m%d'))::BIGINT
           AND f.code = s.code
        LEFT JOIN market_regime_daily r
            ON r.dt = s.dt
        WHERE s.entry_qualified = TRUE
          AND s.side IN ('buy', 'sell')
          AND s.dt BETWEEN ? AND ?
        ORDER BY s.dt ASC, s.code ASC
        """,
        [int(start_ymd), int(end_ymd)],
    ).df()
    if frame.empty:
        return frame
    frame["code"] = frame["code"].astype(str).str.strip()
    frame["side"] = frame["side"].astype(str).str.strip().str.lower()
    frame["setup_type"] = frame["setup_type"].astype(str).str.strip().str.lower()
    frame["signal_dt"] = pd.to_numeric(frame["signal_dt"], errors="coerce").astype("Int64")
    frame = frame.dropna(subset=["signal_dt", "code", "side"]).copy()
    frame["signal_dt"] = frame["signal_dt"].astype(int)
    for col in [
        "forward_return_5",
        "forward_return_10",
        "forward_return_20",
        "forward_return_30",
        "max_favorable_30",
        "max_adverse_30",
        "entry_score",
        "close",
        "ma7",
        "ma20",
        "ma60",
        "body_ratio",
        "upper_wick_ratio",
        "lower_wick_ratio",
    ]:
        if col in frame.columns:
            frame[col] = pd.to_numeric(frame[col], errors="coerce")
    return frame


def _augment_features(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["gap7"] = out["close"] / out["ma7"] - 1.0
    out["gap20"] = out["close"] / out["ma20"] - 1.0
    out["gap60"] = out["close"] / out["ma60"] - 1.0
    out["doji_like"] = (
        (out["body_ratio"] <= 0.09)
        & (out["upper_wick_ratio"] >= 0.10)
        & (out["lower_wick_ratio"] >= 0.10)
    )
    out["risk_off_proxy"] = out["regime_id"].isin(["risk_off_trend", "high_vol_chaos"])
    out["risk_on_proxy"] = out["regime_id"].isin(["risk_on_trend", "capitulation_rebound"])
    out["buy_strict"] = (
        (out["side"] == "buy")
        & (out["entry_score"] >= 0.75)
        & (out["setup_type"] == "breakout")
        & (out["gap7"] <= 0.05)
        & (out["gap20"] <= 0.05)
        & out["doji_like"]
    )
    out["buy_soft"] = (
        (out["side"] == "buy")
        & (out["entry_score"] >= 0.65)
        & (out["setup_type"] == "breakout")
        & (out["gap7"] <= 0.05)
        & (out["gap20"] <= 0.05)
    )
    out["sell_strict"] = (
        (out["side"] == "sell")
        & (out["entry_score"] >= 0.75)
        & (out["setup_type"] == "breakdown")
        & (out["gap20"] <= -0.05)
        & (out["gap60"] <= 0.0)
    )
    out["sell_soft"] = (
        (out["side"] == "sell")
        & (out["entry_score"] >= 0.65)
        & (out["gap60"] <= 0.0)
        & ((out["setup_type"] == "breakdown") | ((out["setup_type"] == "pressure") & (out["regime_id"] == "risk_off_trend")))
        & (out["gap20"] <= 0.0)
    )
    return out


def _soft_score(row: pd.Series, side: str) -> float:
    score = float(row.get("entry_score") or 0.0)
    regime_id = str(row.get("regime_id") or "").strip()
    gap7 = float(row.get("gap7") or 0.0)
    gap20 = float(row.get("gap20") or 0.0)
    gap60 = float(row.get("gap60") or 0.0)
    if side == "buy":
        if str(row.get("setup_type")) == "breakout":
            score += 0.30
        if bool(row.get("doji_like")):
            score += 0.20
        if gap7 <= 0.05:
            score += 0.10
        elif gap7 > 0.07:
            score -= 0.15
        if 0.0 <= gap20 <= 0.05:
            score += 0.20
        elif gap20 > 0.07:
            score -= 0.20
        score += {
            "risk_on_trend": 0.15,
            "capitulation_rebound": 0.10,
            "neutral_range": 0.05,
            "risk_off_trend": -0.15,
            "high_vol_chaos": -0.10,
        }.get(regime_id, 0.0)
    else:
        if str(row.get("setup_type")) == "breakdown":
            score += 0.35
        elif str(row.get("setup_type")) == "pressure" and regime_id == "risk_off_trend":
            score += 0.20
        if gap20 <= -0.05:
            score += 0.20
        elif gap20 <= 0.0:
            score += 0.10
        else:
            score -= 0.15
        if gap60 <= 0.0:
            score += 0.10
        else:
            score -= 0.20
        score += {
            "risk_off_trend": 0.20,
            "high_vol_chaos": 0.10,
            "neutral_range": -0.05,
            "risk_on_trend": -0.20,
            "capitulation_rebound": -0.10,
        }.get(regime_id, 0.0)
    return float(score)


def _simulate_trade(
    history: CodeHistory,
    entry_idx: int,
    *,
    side: str,
    tp: float,
    sl: float,
    max_hold_days: int,
    cost: float,
    review_day_1: int,
    review_day_2: int,
    review_1_mfe_min: float,
    review_1_ret_min: float,
    review_2_mfe_min: float,
    review_2_ret_min: float,
    early_review: bool,
) -> dict[str, Any] | None:
    if entry_idx < 0 or entry_idx >= history.ymds.size:
        return None
    entry_open = float(history.opens[entry_idx])
    if not np.isfinite(entry_open) or entry_open <= 0:
        return None
    side = str(side).lower()
    end_idx = min(entry_idx + max(1, int(max_hold_days)) - 1, history.ymds.size - 1)
    if side == "buy":
        tp_price = entry_open * (1.0 + float(tp))
        sl_price = entry_open * (1.0 - float(sl))
    else:
        tp_price = entry_open * (1.0 - float(tp))
        sl_price = entry_open * (1.0 + float(sl))

    exit_idx = end_idx
    exit_price = float(history.closes[end_idx])
    exit_reason = "max_hold"
    review_stage: int | None = None

    for idx in range(entry_idx, end_idx + 1):
        high = float(history.highs[idx])
        low = float(history.lows[idx])
        close = float(history.closes[idx])
        held = idx - entry_idx + 1
        if side == "buy":
            if low <= sl_price and high >= tp_price:
                exit_idx = idx
                exit_price = sl_price
                exit_reason = "stop_loss_first_when_both_hit"
                break
            if low <= sl_price:
                exit_idx = idx
                exit_price = sl_price
                exit_reason = "stop_loss"
                break
            if high >= tp_price:
                exit_idx = idx
                exit_price = tp_price
                exit_reason = "take_profit"
                break
            mfe = float(np.max(history.highs[entry_idx : idx + 1]) / entry_open - 1.0)
            ret = float(close / entry_open - 1.0)
        else:
            if high >= sl_price and low <= tp_price:
                exit_idx = idx
                exit_price = sl_price
                exit_reason = "stop_loss_first_when_both_hit"
                break
            if high >= sl_price:
                exit_idx = idx
                exit_price = sl_price
                exit_reason = "stop_loss"
                break
            if low <= tp_price:
                exit_idx = idx
                exit_price = tp_price
                exit_reason = "take_profit"
                break
            mfe = float(entry_open / np.min(history.lows[entry_idx : idx + 1]) - 1.0)
            ret = float(entry_open / close - 1.0)
        if early_review:
            if held >= review_day_1 and review_stage is None and mfe < review_1_mfe_min and ret <= review_1_ret_min:
                review_stage = review_day_1
            if held >= review_day_2 and review_stage is None and mfe < review_2_mfe_min and ret <= review_2_ret_min:
                review_stage = review_day_2
            if review_stage is not None:
                review_exit_idx = min(idx + 1, history.ymds.size - 1)
                exit_idx = review_exit_idx
                exit_price = float(history.opens[review_exit_idx] if review_exit_idx > idx else history.closes[idx])
                exit_reason = f"review_{review_stage}"
                break

    if side == "buy":
        entry_fill = entry_open * (1.0 + float(cost))
        exit_fill = exit_price * (1.0 - float(cost))
        net_ret = float(exit_fill / entry_fill - 1.0)
        gross_ret = float(exit_price / entry_open - 1.0)
    else:
        entry_fill = entry_open * (1.0 - float(cost))
        exit_fill = exit_price * (1.0 + float(cost))
        net_ret = float(entry_fill / exit_fill - 1.0)
        gross_ret = float(entry_open / exit_price - 1.0)
    return {
        "entry_ymd": int(history.ymds[entry_idx]),
        "exit_ymd": int(history.ymds[exit_idx]),
        "entry_open": float(entry_open),
        "entry_fill": float(entry_fill),
        "exit_fill": float(exit_fill),
        "net_ret": float(net_ret),
        "gross_ret": float(gross_ret),
        "hold_days": int(exit_idx - entry_idx + 1),
        "exit_reason": str(exit_reason),
        "review_stage": int(review_stage) if review_stage is not None else None,
    }


def _selection_summary(frame: pd.DataFrame, *, side: str, mask: pd.Series) -> dict[str, Any]:
    subset = frame[frame["side"] == side].copy()
    selected = subset[mask.loc[subset.index]].copy()
    excluded = subset[~mask.loc[subset.index]].copy()
    profit20 = subset["forward_return_20"] if side == "buy" else -subset["forward_return_20"]
    selected_profit20 = selected["forward_return_20"] if side == "buy" else -selected["forward_return_20"]
    excluded_profit20 = excluded["forward_return_20"] if side == "buy" else -excluded["forward_return_20"]
    excluded_winners = excluded[excluded_profit20 > 0].copy()
    rank_series = selected["rank_score"] if "rank_score" in selected.columns else selected["entry_score"]
    if side == "buy":
        reason_counts = {
            "score<0.75": int((excluded_winners["entry_score"] < 0.75).sum()),
            "gap7>+5%": int((excluded_winners["gap7"] > 0.05).sum()),
            "gap20>+5%": int((excluded_winners["gap20"] > 0.05).sum()),
            "not_doji_like": int((~excluded_winners["doji_like"]).sum()),
        }
    else:
        reason_counts = {
            "score<0.75": int((excluded_winners["entry_score"] < 0.75).sum()),
            "not_breakdown": int((excluded_winners["setup_type"] != "breakdown").sum()),
            "gap20>-5%": int((excluded_winners["gap20"] > -0.05).sum()),
            "gap60>0%": int((excluded_winners["gap60"] > 0.0).sum()),
        }
    return {
        "count": int(len(selected)),
        "total_count": int(len(subset)),
        "winner_total": int((profit20 > 0).sum()),
        "selected_winners": int((selected_profit20 > 0).sum()),
        "excluded_winners": int((excluded_profit20 > 0).sum()),
        "winner_capture_rate": float((selected_profit20 > 0).sum() / max(1, int((profit20 > 0).sum()))),
        "mean5": float((selected["forward_return_5"] if side == "buy" else -selected["forward_return_5"]).mean()) if len(selected) else None,
        "mean10": float((selected["forward_return_10"] if side == "buy" else -selected["forward_return_10"]).mean()) if len(selected) else None,
        "mean20": float(selected_profit20.mean()) if len(selected) else None,
        "win20": float((selected_profit20 > 0).mean()) if len(selected) else None,
        "median20": float(selected_profit20.median()) if len(selected) else None,
        "mfe30": float((selected["max_favorable_30"] if side == "buy" else -selected["max_adverse_30"]).mean()) if len(selected) else None,
        "mae30": float((selected["max_adverse_30"] if side == "buy" else -selected["max_favorable_30"]).mean()) if len(selected) else None,
        "avg_score": float(selected["entry_score"].mean()) if len(selected) else None,
        "avg_rank_score": float(rank_series.mean()) if len(selected) else None,
        "avg_gap20": float(selected["gap20"].mean()) if len(selected) else None,
        "avg_gap60": float(selected["gap60"].mean()) if len(selected) else None,
        "excluded_winner_reason_counts": reason_counts,
    }


def _run_book(
    frame: pd.DataFrame,
    histories: dict[str, CodeHistory],
    *,
    side: str,
    variant: str,
    config: RelearnConfig,
) -> dict[str, Any]:
    side_frame = frame[frame["side"] == side].copy()
    if side_frame.empty:
        return {"ok": False, "reason": "no_candidates", "variant": variant, "side": side}

    if variant == "strict":
        mask = side_frame[f"{side}_strict"].astype(bool)
        early_review = False
    else:
        mask = side_frame[f"{side}_soft"].astype(bool)
        early_review = variant == "soft_review"

    selected = side_frame.loc[mask].copy()
    if selected.empty:
        return {"ok": False, "reason": "no_selected", "variant": variant, "side": side}
    selected["rank_score"] = selected.apply(lambda row: _soft_score(row, side) if variant != "strict" else float(row.get("entry_score") or 0.0), axis=1)
    selected = selected.sort_values(["signal_dt", "rank_score", "entry_score", "code"], ascending=[True, False, False, True]).copy()

    plans: list[dict[str, Any]] = []
    cutoff = max(int(history.ymds[-1]) for history in histories.values() if history.ymds.size > 0)
    for _, row in selected.iterrows():
        history = histories.get(str(row["code"]))
        if history is None:
            continue
        entry_idx = _next_entry_idx(history, int(row["signal_dt"]))
        if entry_idx is None:
            continue
        trade = _simulate_trade(
            history,
            int(entry_idx),
            side=side,
            tp=0.15 if side == "buy" else 0.10,
            sl=0.05,
            max_hold_days=20 if side == "buy" else 10,
            cost=config.transaction_cost_rate,
            review_day_1=config.review_day_1,
            review_day_2=config.review_day_2,
            review_1_mfe_min=config.review_1_mfe_min,
            review_1_ret_min=config.review_1_ret_min,
            review_2_mfe_min=config.review_2_mfe_min,
            review_2_ret_min=config.review_2_ret_min,
            early_review=early_review,
        )
        if trade is None or trade["exit_ymd"] > cutoff:
            continue
        trade.update(
            {
                "code": str(row["code"]),
                "signal_dt": int(row["signal_dt"]),
                "entry_score": float(row.get("entry_score") or 0.0),
                "rank_score": float(row.get("rank_score") or 0.0),
                "setup_type": str(row.get("setup_type") or ""),
                "regime_id": str(row.get("regime_id") or ""),
                "gap7": float(row.get("gap7") or 0.0),
                "gap20": float(row.get("gap20") or 0.0),
                "gap60": float(row.get("gap60") or 0.0),
                "doji_like": bool(row.get("doji_like")),
            }
        )
        plans.append(trade)

    plans_by_entry: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for plan in plans:
        plans_by_entry[int(plan["entry_ymd"])].append(plan)

    trading_days = sorted(int(day) for day in pd.unique(np.concatenate([h.ymds for h in histories.values()])))
    trading_days = [day for day in trading_days if int(selected["signal_dt"].min()) <= day <= int(selected["signal_dt"].max())]
    cash = float(config.initial_capital)
    open_positions: dict[str, dict[str, Any]] = {}
    equity_curve: list[dict[str, Any]] = []
    realized: list[dict[str, Any]] = []

    def _position_value(plan: dict[str, Any], day: int) -> float:
        history = histories[str(plan["code"])]
        idx = int(np.searchsorted(history.ymds, int(day), side="right") - 1)
        if idx < 0:
            return 0.0
        current = float(history.closes[idx])
        entry_open = float(plan["entry_open"])
        if side == "buy":
            return float(plan["allocation"] * (current / entry_open))
        return float(plan["allocation"] * (entry_open / current))

    for day in trading_days:
        for code, pos in list(open_positions.items()):
            if int(pos["exit_ymd"]) == int(day):
                cash += float(pos["allocation"]) * (1.0 + float(pos["net_ret"]))
                realized.append(pos)
                open_positions.pop(code, None)
        todays = [plan for plan in plans_by_entry.get(int(day), []) if plan["code"] not in open_positions]
        todays.sort(key=lambda row: (float(row["rank_score"]), float(row["entry_score"]), str(row["code"])), reverse=True)
        slots = max(0, int(config.max_positions_buy if side == "buy" else config.max_positions_sell) - len(open_positions))
        selected_today = todays[:slots]
        if selected_today:
            allocation = float(cash / len(selected_today))
            for plan in selected_today:
                plan = dict(plan)
                plan["allocation"] = allocation
                open_positions[str(plan["code"])] = plan
                cash -= allocation
        for code, pos in list(open_positions.items()):
            if int(pos["exit_ymd"]) == int(day):
                cash += float(pos["allocation"]) * (1.0 + float(pos["net_ret"]))
                realized.append(pos)
                open_positions.pop(code, None)
        open_value = sum(_position_value(plan, int(day)) for plan in open_positions.values())
        equity_curve.append({"ymd": int(day), "cash": cash, "open_value": open_value, "equity": cash + open_value})

    eq = pd.Series([float(row["equity"]) for row in equity_curve], dtype=float)
    trade_returns = pd.Series([float(plan["net_ret"]) for plan in realized], dtype=float)
    month_counts: dict[str, int] = defaultdict(int)
    regime_counts: dict[str, dict[str, Any]] = defaultdict(lambda: {"count": 0, "wins": 0, "net_sum": 0.0})
    exit_reason_counts: dict[str, int] = defaultdict(int)
    for trade in realized:
        month_counts[str(trade["entry_ymd"])[:6]] += 1
        regime = str(trade.get("regime_id") or "unknown")
        regime_counts[regime]["count"] += 1
        regime_counts[regime]["wins"] += int(float(trade["net_ret"]) > 0.0)
        regime_counts[regime]["net_sum"] += float(trade["net_ret"])
        exit_reason_counts[str(trade.get("exit_reason") or "unknown")] += 1

    regime_rows = [
        {
            "regime_id": regime,
            "count": int(slot["count"]),
            "win_rate": float(slot["wins"] / slot["count"]) if slot["count"] else None,
            "mean_net_ret": float(slot["net_sum"] / slot["count"]) if slot["count"] else None,
        }
        for regime, slot in sorted(regime_counts.items(), key=lambda item: (-int(item[1]["count"]), item[0]))
    ]
    return {
        "ok": True,
        "variant": variant,
        "side": side,
        "selected_count": int(len(selected)),
        "winner_total": int((selected["forward_return_20"] > 0).sum() if side == "buy" else (-selected["forward_return_20"] > 0).sum()),
        "excluded_winners": int(((~mask) & (side_frame["forward_return_20"] > 0 if side == "buy" else -side_frame["forward_return_20"] > 0)).sum()),
        "trade_count": int(len(realized)),
        "win_rate": None if trade_returns.empty else float((trade_returns > 0).mean()),
        "avg_trade_net_ret": None if trade_returns.empty else float(trade_returns.mean()),
        "median_trade_net_ret": None if trade_returns.empty else float(trade_returns.median()),
        "monthly_trade_month_count": int(len(month_counts)),
        "avg_trades_per_active_month": float(len(realized) / max(1, len(month_counts))),
        "final_capital": float(eq.iloc[-1]) if not eq.empty else float(cash),
        "total_return": float((eq.iloc[-1] if not eq.empty else cash) / float(config.initial_capital) - 1.0),
        "annualized_return": None if eq.empty else float(((eq.iloc[-1] if not eq.empty else cash) / float(config.initial_capital)) ** (252.0 / len(eq)) - 1.0),
        "max_drawdown": float((eq / eq.cummax() - 1.0).min()) if not eq.empty else 0.0,
        "exit_reason_counts": dict(exit_reason_counts),
        "regime_breakdown": regime_rows,
    }


def run_swing_gate_relearn(*, config: RelearnConfig = RelearnConfig()) -> dict[str, Any]:
    with get_conn() as conn:
        if not _table_exists(conn, "daily_bars"):
            return {"ok": False, "reason": "daily_bars_missing"}
        daily = _load_daily_frame(conn, lookback_days=config.lookback_days)
        if daily.empty:
            return {"ok": False, "reason": "daily_frame_empty"}
        latest_ymd = int(daily["ymd"].max())
        cutoff_dt = datetime.strptime(str(latest_ymd), "%Y%m%d") - pd.Timedelta(days=max(1, int(config.lookback_days)))
        cutoff_ymd = int(cutoff_dt.strftime("%Y%m%d"))
        frame = _load_signal_frame(conn, start_ymd=cutoff_ymd, end_ymd=latest_ymd)
    if frame.empty:
        return {"ok": False, "reason": "signal_frame_empty"}
    frame = _augment_features(frame)
    histories = _build_histories(daily)

    variants = ("strict", "soft", "soft_review")
    results: dict[str, dict[str, Any]] = {"buy": {}, "sell": {}}
    for side in ("buy", "sell"):
        side_frame = frame[frame["side"] == side].copy()
        if side_frame.empty:
            continue
        for variant in variants:
            mask = side_frame[f"{side}_strict"].astype(bool) if variant == "strict" else side_frame[f"{side}_soft"].astype(bool)
            results[side][variant] = {
                "selection": _selection_summary(frame, side=side, mask=mask),
                "book": _run_book(frame, histories, side=side, variant=variant, config=config),
            }

    return {
        "ok": True,
        "as_of_ymd": latest_ymd,
        "period_start_ymd": int(frame["signal_dt"].min()),
        "period_end_ymd": int(frame["signal_dt"].max()),
        "train_end_ymd": int(config.train_end_ymd),
        "lookback_days": int(config.lookback_days),
        "initial_capital": float(config.initial_capital),
        "transaction_cost_rate": float(config.transaction_cost_rate),
        "variants": results,
        "missed_opportunity_delta": {
            side: {
                "strict_excluded_winners": int(results.get(side, {}).get("strict", {}).get("selection", {}).get("excluded_winners") or 0),
                "soft_excluded_winners": int(results.get(side, {}).get("soft", {}).get("selection", {}).get("excluded_winners") or 0),
                "winner_capture_delta": float(results.get(side, {}).get("soft", {}).get("selection", {}).get("winner_capture_rate") or 0.0)
                - float(results.get(side, {}).get("strict", {}).get("selection", {}).get("winner_capture_rate") or 0.0),
            }
            for side in ("buy", "sell")
        },
    }


def _write_markdown_report(result: dict[str, Any], output_path: Path) -> None:
    lines = [
        "# Swing Gate Relearn",
        "",
        f"- ok: `{result.get('ok')}`",
        f"- as_of_ymd: `{result.get('as_of_ymd')}`",
        f"- period_start_ymd: `{result.get('period_start_ymd')}`",
        f"- period_end_ymd: `{result.get('period_end_ymd')}`",
        f"- train_end_ymd: `{result.get('train_end_ymd')}`",
        "",
        "## Missed Opportunity Delta",
        "",
        "| side | strict_excluded_winners | soft_excluded_winners | winner_capture_delta |",
        "| --- | ---: | ---: | ---: |",
    ]
    for side in ("buy", "sell"):
        row = (result.get("missed_opportunity_delta") or {}).get(side) or {}
        lines.append(
            f"| {side} | {row.get('strict_excluded_winners')} | {row.get('soft_excluded_winners')} | {_fmt_pct(row.get('winner_capture_delta'))} |"
        )
    lines += [
        "",
        "## Variants",
        "",
        "| side | variant | count | mean5 | mean10 | mean20 | win20 | median20 | trade_count | max_drawdown | final_capital |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for side in ("buy", "sell"):
        for variant in ("strict", "soft", "soft_review"):
            payload = (result.get("variants") or {}).get(side, {}).get(variant) or {}
            selection = payload.get("selection") or {}
            book = payload.get("book") or {}
            lines.append(
                "| {side} | {variant} | {count} | {mean5} | {mean10} | {mean20} | {win20} | {median20} | {trades} | {dd} | {final} |".format(
                    side=side,
                    variant=variant,
                    count=selection.get("count"),
                    mean5=_fmt_pct(selection.get("mean5")),
                    mean10=_fmt_pct(selection.get("mean10")),
                    mean20=_fmt_pct(selection.get("mean20")),
                    win20=_fmt_pct(selection.get("win20")),
                    median20=_fmt_pct(selection.get("median20")),
                    trades=book.get("trade_count"),
                    dd=_fmt_pct(book.get("max_drawdown")),
                    final=_fmt_num(book.get("final_capital"), digits=0),
                )
            )
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_json_report(result: dict[str, Any], output_path: Path) -> None:
    output_path.write_text(json.dumps(_jsonable(result), ensure_ascii=False, indent=2), encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Relearn swing gate with soft ranking and early review")
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS)
    parser.add_argument("--initial-capital", type=float, default=DEFAULT_INITIAL_CAPITAL)
    parser.add_argument("--train-end-ymd", type=int, default=DEFAULT_TRAIN_END_YMD)
    parser.add_argument("--transaction-cost-rate", type=float, default=DEFAULT_COST)
    parser.add_argument("--max-positions-buy", type=int, default=5)
    parser.add_argument("--max-positions-sell", type=int, default=3)
    parser.add_argument("--review-day-1", type=int, default=3)
    parser.add_argument("--review-day-2", type=int, default=5)
    parser.add_argument("--review-1-mfe-min", type=float, default=0.01)
    parser.add_argument("--review-1-ret-min", type=float, default=0.0)
    parser.add_argument("--review-2-mfe-min", type=float, default=0.02)
    parser.add_argument("--review-2-ret-min", type=float, default=0.0)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
    parser.add_argument("--prefix", default="swing_gate_relearn")
    args = parser.parse_args(argv)
    result = run_swing_gate_relearn(
        config=RelearnConfig(
            lookback_days=int(args.lookback_days),
            initial_capital=float(args.initial_capital),
            train_end_ymd=int(args.train_end_ymd),
            transaction_cost_rate=float(args.transaction_cost_rate),
            report_dir=Path(args.report_dir),
            max_positions_buy=int(args.max_positions_buy),
            max_positions_sell=int(args.max_positions_sell),
            review_day_1=int(args.review_day_1),
            review_day_2=int(args.review_day_2),
            review_1_mfe_min=float(args.review_1_mfe_min),
            review_1_ret_min=float(args.review_1_ret_min),
            review_2_mfe_min=float(args.review_2_mfe_min),
            review_2_ret_min=float(args.review_2_ret_min),
        )
    )
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d%H%M%S")
    json_path = report_dir / f"{args.prefix}_{stamp}.json"
    md_path = report_dir / f"{args.prefix}_{stamp}.md"
    _write_json_report(result, json_path)
    _write_markdown_report(result, md_path)
    print(json.dumps({"ok": result.get("ok"), "json": str(json_path), "markdown": str(md_path)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
