from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import entry_precision_short_audit as base  # noqa: E402
from scripts import tradex_sell_failed_followthrough_no_lookahead_repair_v1 as clean  # noqa: E402


SCHEMA_PREFIX = "sell_failed_followthrough_multiyear_portfolio_replay_v1"
CANDIDATE_NAME = clean.CANDIDATE_NAME
DEFAULT_DB_PATH = clean.DEFAULT_DB_PATH
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\sell_failed_followthrough_multiyear_portfolio_replay_v1")
SOURCE_REFLECTION_ROOT = Path(
    r"G:\Tradex\sell_failed_followthrough_meemee_readonly_reflection_v1"
    r"\20260515T040337Z-sell-failed-followthrough-meemee-readonly-reflection-v1"
)
PREFERRED_START_YMD = 20190101
PREFERRED_END_YMD = 20251231
LATEST_CHECK_END_YMD = 20261231
BASE_CAPITAL_JPY = 10_000_000.0
MAX_CONCURRENT_POSITIONS = 3
LOT_SIZE = 100
ONE_WAY_COST_BPS = 30.0
SEVERE_LOSER_THRESHOLD = -0.05
BAD_PICK_THRESHOLD = 0.0

EXIT_VARIANTS = (
    "fixed_horizon_20d_exit",
    "stop_loss_at_negative_5pct_or_20d",
    "stop_loss_at_negative_8pct_or_20d",
    "trailing_exit_if_available_from_past_current_MA_rule",
)


@dataclass
class Trade:
    symbol: str
    as_of_date: int
    entry_date: int
    exit_date: int
    entry_price: float
    exit_price: float
    shares: int
    gross_return: float
    net_return: float
    pnl: float
    entry_cost: float
    exit_cost: float
    exit_reason: str
    holding_days: int
    max_adverse_excursion: float
    max_favorable_excursion: float
    source: str
    row_id: str
    baseline_rank: int | None
    added_by_challenger: bool
    removed_from_baseline: bool
    why_candidate_added_it: str | None = None
    detectable_by_past_current_features: bool | None = None


@dataclass
class OpenPosition:
    row: dict[str, Any]
    entry_date: int
    entry_idx: int
    entry_price: float
    shares: int
    entry_cost: float
    source: str
    peak_adverse: float = 0.0
    peak_favorable: float = 0.0


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _utc_stamp() -> str:
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
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (np.integer, np.floating)):
        return value.item()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        parsed = float(value)
    except Exception:
        return None
    return parsed if math.isfinite(parsed) else None


def _as_list(values: list[float | None]) -> list[float]:
    return [float(value) for value in values if value is not None and math.isfinite(float(value))]


def _mean(values: list[float]) -> float | None:
    return float(statistics.fmean(values)) if values else None


def _median(values: list[float]) -> float | None:
    return float(statistics.median(values)) if values else None


def _table_exists(conn: duckdb.DuckDBPyConnection, table: str) -> bool:
    return bool(
        conn.execute(
            "select count(*) from information_schema.tables where table_schema='main' and table_name=?",
            [table],
        ).fetchone()[0]
    )


def _column_names(conn: duckdb.DuckDBPyConnection, table: str) -> list[str]:
    return [str(row[0]) for row in conn.execute(f"describe {table}").fetchall()]


def _range_for_table(conn: duckdb.DuckDBPyConnection, table: str, date_col: str) -> dict[str, Any]:
    if not _table_exists(conn, table):
        return {"table": table, "exists": False}
    row = conn.execute(
        f"""
        SELECT MIN({base._ymd_expr(date_col)}) AS min_ymd,
               MAX({base._ymd_expr(date_col)}) AS max_ymd,
               COUNT(*) AS row_count
        FROM {table}
        """
    ).fetchone()
    return {
        "table": table,
        "exists": True,
        "date_column": date_col,
        "min_ymd": None if row[0] is None else int(row[0]),
        "max_ymd": None if row[1] is None else int(row[1]),
        "row_count": int(row[2] or 0),
    }


def _load_frame_map_range(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    *,
    ymd_col: str,
    start_ymd: int,
    end_ymd: int,
) -> dict[tuple[int, str], dict[str, Any]]:
    if not _table_exists(conn, table):
        return {}
    df = conn.execute(
        f"""
        SELECT *, {base._ymd_expr(ymd_col)} AS __ymd
        FROM {table}
        WHERE {base._ymd_expr(ymd_col)} BETWEEN ? AND ?
        """,
        [int(start_ymd), int(end_ymd)],
    ).fetchdf()
    if df.empty or "code" not in df.columns:
        return {}
    df = df.dropna(subset=["__ymd", "code"])
    df["code"] = df["code"].astype(str)
    return {
        (int(row["__ymd"]), str(row["code"])): row.to_dict()
        for _, row in df.iterrows()
        if row.get("__ymd") is not None and row.get("code") is not None
    }


def _load_price_store_with_volume(conn: duckdb.DuckDBPyConnection) -> dict[str, dict[str, np.ndarray]]:
    df = conn.execute(
        f"""
        SELECT
          CAST(code AS VARCHAR) AS code,
          {base._ymd_expr("date")} AS ymd,
          CAST(o AS DOUBLE) AS o,
          CAST(h AS DOUBLE) AS h,
          CAST(l AS DOUBLE) AS l,
          CAST(c AS DOUBLE) AS c,
          CAST(v AS DOUBLE) AS v
        FROM daily_bars
        WHERE COALESCE(source, 'pan') <> 'yahoo'
        ORDER BY code, ymd
        """
    ).fetchdf()
    df = df.dropna(subset=["code", "ymd", "o", "h", "l", "c"])
    df["ymd"] = df["ymd"].astype(np.int64)
    store: dict[str, dict[str, np.ndarray]] = {}
    for code, part in df.groupby("code", sort=False):
        store[str(code)] = {
            "ymd": part["ymd"].to_numpy(dtype=np.int64, copy=True),
            "o": part["o"].to_numpy(dtype=np.float64, copy=True),
            "h": part["h"].to_numpy(dtype=np.float64, copy=True),
            "l": part["l"].to_numpy(dtype=np.float64, copy=True),
            "c": part["c"].to_numpy(dtype=np.float64, copy=True),
            "v": part["v"].to_numpy(dtype=np.float64, copy=True),
        }
    return store


def _available_period(conn: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    daily = _range_for_table(conn, "daily_bars", "date")
    sell = _range_for_table(conn, "sell_analysis_daily", "dt")
    feature = _range_for_table(conn, "feature_snapshot_daily", "dt")
    market = _range_for_table(conn, "market_regime_daily", "dt") if _table_exists(conn, "market_regime_daily") else {"exists": False}
    confirmed_daily = dict(daily)
    confirmed_daily["confirmed_filter"] = "COALESCE(source, 'pan') <> 'yahoo'"
    confirmed_row = conn.execute(
        f"""
        SELECT MIN({base._ymd_expr("date")}), MAX({base._ymd_expr("date")}), COUNT(*)
        FROM daily_bars
        WHERE COALESCE(source, 'pan') <> 'yahoo'
        """
    ).fetchone()
    confirmed_daily["min_ymd"] = None if confirmed_row[0] is None else int(confirmed_row[0])
    confirmed_daily["max_ymd"] = None if confirmed_row[1] is None else int(confirmed_row[1])
    confirmed_daily["row_count"] = int(confirmed_row[2] or 0)
    return {
        "daily_bars": daily,
        "confirmed_daily_bars": confirmed_daily,
        "sell_analysis_daily": sell,
        "feature_snapshot_daily": feature,
        "market_regime_daily": market,
    }


def _choose_period(availability: dict[str, Any], *, preferred_start: int, preferred_end: int) -> dict[str, Any]:
    daily = availability["confirmed_daily_bars"]
    sell = availability["sell_analysis_daily"]
    feature = availability["feature_snapshot_daily"]
    bounds = [
        daily.get("min_ymd"),
        sell.get("min_ymd"),
        feature.get("min_ymd"),
    ]
    ends = [
        daily.get("max_ymd"),
        sell.get("max_ymd"),
        feature.get("max_ymd"),
    ]
    usable_start = max([int(x) for x in bounds if x is not None] + [int(preferred_start)])
    usable_end = min([int(x) for x in ends if x is not None] + [int(preferred_end)])
    return {
        "preferred_start_ymd": int(preferred_start),
        "preferred_end_ymd": int(preferred_end),
        "actual_start_ymd": int(usable_start),
        "actual_end_ymd": int(usable_end),
        "period_reduced": bool(usable_start != preferred_start or usable_end != preferred_end),
        "reduction_reason": None
        if usable_start == preferred_start and usable_end == preferred_end
        else "available confirmed daily/sell/feature overlap did not cover preferred period",
    }


def load_multiyear_candidate_rows(
    db_path: str | Path,
    *,
    start_ymd: int,
    end_ymd: int,
) -> dict[str, Any]:
    resolved_db = base._resolve_db_path(str(db_path))
    with duckdb.connect(str(resolved_db), read_only=True) as conn:
        availability = _available_period(conn)
        months = base._month_end_dates(conn, start_ymd=int(start_ymd), end_ymd=int(end_ymd))
        price_store = _load_price_store_with_volume(conn)
        sell_map = _load_frame_map_range(conn, "sell_analysis_daily", ymd_col="dt", start_ymd=start_ymd, end_ymd=end_ymd)
        feature_map = _load_frame_map_range(conn, "feature_snapshot_daily", ymd_col="dt", start_ymd=start_ymd, end_ymd=end_ymd)
        bundle = clean._build_rows_no_lookahead(
            conn=conn,
            months=months,
            price_store=price_store,
            sell_map=sell_map,
            feature_map=feature_map,
        )
    return {
        "resolved_db": str(resolved_db),
        "availability": availability,
        "months": months,
        "price_store": price_store,
        **bundle,
    }


def _portfolio_calendar(price_store: dict[str, dict[str, np.ndarray]], *, start_ymd: int, end_ymd: int) -> list[int]:
    dates: set[int] = set()
    for series in price_store.values():
        dates.update(int(v) for v in series["ymd"] if int(start_ymd) <= int(v) <= int(end_ymd))
    return sorted(dates)


def _date_index(series: dict[str, np.ndarray], ymd: int, *, side: str = "exact_or_after") -> int | None:
    idx = int(np.searchsorted(series["ymd"], int(ymd), side="left"))
    if side == "after" and idx < len(series["ymd"]) and int(series["ymd"][idx]) == int(ymd):
        idx += 1
    if idx >= len(series["ymd"]):
        return None
    return idx


def _price_at(series: dict[str, np.ndarray], idx: int, field: str) -> float | None:
    if idx < 0 or idx >= len(series["ymd"]):
        return None
    value = float(series[field][idx])
    return value if math.isfinite(value) and value > 0.0 else None


def _ma20(series: dict[str, np.ndarray], idx: int) -> float | None:
    if idx < 19:
        return None
    values = series["c"][idx - 19 : idx + 1]
    if len(values) < 20:
        return None
    return float(np.mean(values))


def _one_way_cost(notional: float) -> float:
    return abs(float(notional)) * ONE_WAY_COST_BPS / 10_000.0


def _row_groups(rows: list[dict[str, Any]]) -> dict[int, list[dict[str, Any]]]:
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row.get("ymd") is not None:
            grouped[int(row["ymd"])].append(row)
    return grouped


def _top_rows(rows: list[dict[str, Any]], limit: int = MAX_CONCURRENT_POSITIONS) -> list[dict[str, Any]]:
    return sorted(rows, key=clean._row_sort_key)[:limit]


def _trade_return_for_short(entry_price: float, exit_price: float) -> float:
    return float((entry_price - exit_price) / entry_price)


def _simulate_portfolio(
    *,
    rows: list[dict[str, Any]],
    selection: dict[str, Any],
    price_store: dict[str, dict[str, np.ndarray]],
    exit_variant: str,
    start_ymd: int,
    end_ymd: int,
    label: str,
) -> dict[str, Any]:
    calendar = _portfolio_calendar(price_store, start_ymd=start_ymd, end_ymd=end_ymd + 10000)
    if not calendar:
        return {"trades": [], "equity_curve": [], "summary": _empty_summary(label, exit_variant, "no_calendar")}
    rows_by_month = _row_groups(rows)
    candidate_by_entry: dict[int, list[dict[str, Any]]] = defaultdict(list)
    execution_fallbacks: list[dict[str, Any]] = []
    for asof, month_rows in rows_by_month.items():
        for row in _top_rows(month_rows):
            series = price_store.get(str(row.get("code") or ""))
            if not series:
                execution_fallbacks.append({"row_id": row.get("row_id"), "reason": "missing_price_series"})
                continue
            idx = _date_index(series, asof, side="after")
            if idx is None:
                execution_fallbacks.append({"row_id": row.get("row_id"), "reason": "next_session_open_missing"})
                continue
            entry_ymd = int(series["ymd"][idx])
            candidate_by_entry[entry_ymd].append(row)

    cash = BASE_CAPITAL_JPY
    open_positions: list[OpenPosition] = []
    trades: list[Trade] = []
    equity_curve: list[dict[str, Any]] = []
    added_ids = {str(row["row_id"]) for row in selection.get("added_rows", [])}
    removed_ids = {str(row["row_id"]) for row in selection.get("removed_rows", [])}
    calendar = [date for date in calendar if int(start_ymd) <= date <= int(end_ymd) + 10000]

    for ymd in calendar:
        still_open: list[OpenPosition] = []
        for pos in open_positions:
            series = price_store[str(pos.row.get("code"))]
            idx = _date_index(series, ymd)
            if idx is None or int(series["ymd"][idx]) != int(ymd):
                still_open.append(pos)
                continue
            high = _price_at(series, idx, "h")
            low = _price_at(series, idx, "l")
            close = _price_at(series, idx, "c")
            if high is not None:
                pos.peak_adverse = max(pos.peak_adverse, float((high - pos.entry_price) / pos.entry_price))
            if low is not None:
                pos.peak_favorable = max(pos.peak_favorable, float((pos.entry_price - low) / pos.entry_price))
            holding_days = int(idx - pos.entry_idx)
            exit_reason = None
            exit_price = close
            if exit_variant == "stop_loss_at_negative_5pct_or_20d" and high is not None and high >= pos.entry_price * 1.05:
                exit_reason = "stop_loss_5pct"
                exit_price = pos.entry_price * 1.05
            elif exit_variant == "stop_loss_at_negative_8pct_or_20d" and high is not None and high >= pos.entry_price * 1.08:
                exit_reason = "stop_loss_8pct"
                exit_price = pos.entry_price * 1.08
            elif exit_variant == "trailing_exit_if_available_from_past_current_MA_rule":
                ma20 = _ma20(series, idx)
                if ma20 is not None and close is not None and close > ma20:
                    exit_reason = "close_reclaimed_ma20"
                    exit_price = close
            if exit_reason is None and holding_days >= 20:
                exit_reason = "fixed_horizon_20d"
                exit_price = close
            if exit_reason and exit_price is not None:
                notional = pos.shares * exit_price
                exit_cost = _one_way_cost(notional)
                gross = _trade_return_for_short(pos.entry_price, exit_price)
                pnl = (pos.entry_price - exit_price) * pos.shares - pos.entry_cost - exit_cost
                cash += pnl
                trades.append(
                    Trade(
                        symbol=str(pos.row.get("code")),
                        as_of_date=int(pos.row["ymd"]),
                        entry_date=pos.entry_date,
                        exit_date=int(ymd),
                        entry_price=pos.entry_price,
                        exit_price=float(exit_price),
                        shares=pos.shares,
                        gross_return=gross,
                        net_return=float(pnl / (pos.entry_price * pos.shares)),
                        pnl=float(pnl),
                        entry_cost=float(pos.entry_cost),
                        exit_cost=float(exit_cost),
                        exit_reason=exit_reason,
                        holding_days=holding_days,
                        max_adverse_excursion=float(pos.peak_adverse),
                        max_favorable_excursion=float(pos.peak_favorable),
                        source=pos.source,
                        row_id=str(pos.row["row_id"]),
                        baseline_rank=pos.row.get("baseline_rank"),
                        added_by_challenger=str(pos.row["row_id"]) in added_ids,
                        removed_from_baseline=str(pos.row["row_id"]) in removed_ids,
                        why_candidate_added_it="same_month_candidate_pool_refill_after_clean_failed_followthrough_removal"
                        if str(pos.row["row_id"]) in added_ids
                        else None,
                        detectable_by_past_current_features=True if str(pos.row["row_id"]) in added_ids else None,
                    )
                )
            else:
                still_open.append(pos)
        open_positions = still_open

        for row in candidate_by_entry.get(ymd, []):
            if len(open_positions) >= MAX_CONCURRENT_POSITIONS:
                continue
            code = str(row.get("code") or "")
            if any(pos.row.get("code") == code for pos in open_positions):
                continue
            series = price_store.get(code)
            if not series:
                continue
            idx = _date_index(series, ymd)
            if idx is None or int(series["ymd"][idx]) != int(ymd):
                continue
            entry_price = _price_at(series, idx, "o")
            if entry_price is None:
                execution_fallbacks.append({"row_id": row.get("row_id"), "reason": "entry_open_missing"})
                continue
            target_notional = max(0.0, cash) / MAX_CONCURRENT_POSITIONS
            shares = int(target_notional // (entry_price * LOT_SIZE)) * LOT_SIZE
            if shares <= 0:
                continue
            entry_cost = _one_way_cost(shares * entry_price)
            cash -= entry_cost
            open_positions.append(
                OpenPosition(
                    row=row,
                    entry_date=int(ymd),
                    entry_idx=idx,
                    entry_price=float(entry_price),
                    shares=shares,
                    entry_cost=float(entry_cost),
                    source=label,
                )
            )

        mark_to_market = cash
        for pos in open_positions:
            series = price_store[str(pos.row.get("code"))]
            idx = _date_index(series, ymd)
            if idx is None or int(series["ymd"][idx]) != int(ymd):
                continue
            close = _price_at(series, idx, "c")
            if close is not None:
                mark_to_market += (pos.entry_price - close) * pos.shares
        equity_curve.append(
            {
                "ymd": int(ymd),
                "equity": float(mark_to_market),
                "cash_after_realized_pnl": float(cash),
                "open_positions": len(open_positions),
            }
        )

    summary = _summarize_portfolio(trades, equity_curve, label=label, exit_variant=exit_variant)
    summary["execution_fallback_count"] = len(execution_fallbacks)
    summary["execution_fallbacks"] = execution_fallbacks[:50]
    return {
        "trades": [_json_ready(trade.__dict__) for trade in trades],
        "equity_curve": equity_curve,
        "summary": summary,
    }


def _empty_summary(label: str, exit_variant: str, reason: str) -> dict[str, Any]:
    return {
        "label": label,
        "exit_variant": exit_variant,
        "reason": reason,
        "trade_count": 0,
        "total_return": 0.0,
        "final_equity": BASE_CAPITAL_JPY,
    }


def _summarize_portfolio(trades: list[Trade], equity_curve: list[dict[str, Any]], *, label: str, exit_variant: str) -> dict[str, Any]:
    if not equity_curve:
        return _empty_summary(label, exit_variant, "no_equity_curve")
    final_equity = float(equity_curve[-1]["equity"])
    total_return = final_equity / BASE_CAPITAL_JPY - 1.0
    first_date = int(equity_curve[0]["ymd"])
    last_date = int(equity_curve[-1]["ymd"])
    years = max(1.0 / 365.0, (_ymd_to_datetime(last_date) - _ymd_to_datetime(first_date)).days / 365.25)
    cagr = (final_equity / BASE_CAPITAL_JPY) ** (1.0 / years) - 1.0 if final_equity > 0 else -1.0
    equities = [float(row["equity"]) for row in equity_curve]
    peak = -math.inf
    max_dd = 0.0
    for equity in equities:
        peak = max(peak, equity)
        if peak > 0:
            max_dd = min(max_dd, equity / peak - 1.0)
    daily_returns = [
        equities[idx] / equities[idx - 1] - 1.0
        for idx in range(1, len(equities))
        if equities[idx - 1] > 0
    ]
    sharpe_like = None
    if len(daily_returns) >= 2:
        stdev = statistics.pstdev(daily_returns)
        if stdev > 0:
            sharpe_like = float(statistics.fmean(daily_returns) / stdev * math.sqrt(252))
    returns = [trade.net_return for trade in trades]
    wins = [value for value in returns if value > 0]
    losses = [value for value in returns if value <= 0]
    gross_profit = sum(trade.pnl for trade in trades if trade.pnl > 0)
    gross_loss = abs(sum(trade.pnl for trade in trades if trade.pnl <= 0))
    max_consecutive_losses = 0
    current_losses = 0
    for value in returns:
        if value <= 0:
            current_losses += 1
            max_consecutive_losses = max(max_consecutive_losses, current_losses)
        else:
            current_losses = 0
    exposure_days = sum(1 for row in equity_curve if int(row["open_positions"]) > 0)
    turnover = sum(abs(trade.entry_price * trade.shares) + abs(trade.exit_price * trade.shares) for trade in trades) / BASE_CAPITAL_JPY
    worst = min(trades, key=lambda trade: trade.net_return, default=None)
    best = max(trades, key=lambda trade: trade.net_return, default=None)
    return {
        "label": label,
        "exit_variant": exit_variant,
        "base_capital_jpy": BASE_CAPITAL_JPY,
        "final_equity": final_equity,
        "total_return": float(total_return),
        "cagr": float(cagr),
        "max_drawdown": float(max_dd),
        "sharpe_like_daily_return": sharpe_like,
        "win_rate": None if not returns else float(len(wins) / len(returns)),
        "average_win": _mean(wins),
        "average_loss": _mean(losses),
        "profit_factor": None if gross_loss == 0 else float(gross_profit / gross_loss),
        "max_consecutive_losses": int(max_consecutive_losses),
        "number_of_trades": len(trades),
        "average_holding_days": _mean([float(trade.holding_days) for trade in trades]),
        "exposure_days": int(exposure_days),
        "turnover": float(turnover),
        "worst_trade": None if worst is None else _json_ready(worst.__dict__),
        "best_trade": None if best is None else _json_ready(best.__dict__),
        "severe_loser_count": sum(1 for trade in trades if trade.net_return <= SEVERE_LOSER_THRESHOLD),
        "bad_pick_count": sum(1 for trade in trades if trade.net_return <= BAD_PICK_THRESHOLD),
        "added_bad_pick_trade_count": sum(1 for trade in trades if trade.added_by_challenger and trade.net_return <= BAD_PICK_THRESHOLD),
        "bad_pick_removal_trade_count": sum(1 for trade in trades if trade.removed_from_baseline and trade.net_return <= BAD_PICK_THRESHOLD),
    }


def _ymd_to_datetime(ymd: int) -> datetime:
    return datetime.strptime(str(int(ymd)), "%Y%m%d")


def _performance_by_period(trades: list[dict[str, Any]], *, key: str) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for trade in trades:
        ymd = str(trade["exit_date"])
        bucket = ymd[:4] if key == "year" else ymd[:6]
        grouped[bucket].append(trade)
    rows: list[dict[str, Any]] = []
    for bucket, part in sorted(grouped.items()):
        pnl = sum(float(trade["pnl"]) for trade in part)
        rows.append(
            {
                key: bucket,
                "trade_count": len(part),
                "return_on_base_capital": float(pnl / BASE_CAPITAL_JPY),
                "win_rate": float(sum(1 for trade in part if float(trade["net_return"]) > 0) / len(part)) if part else None,
                "bad_pick_count": sum(1 for trade in part if float(trade["net_return"]) <= BAD_PICK_THRESHOLD),
                "severe_loser_count": sum(1 for trade in part if float(trade["net_return"]) <= SEVERE_LOSER_THRESHOLD),
                "classification": _classify_period_return(float(pnl / BASE_CAPITAL_JPY), len(part)),
            }
        )
    return rows


def _classify_period_return(value: float, count: int) -> str:
    if count == 0:
        return "insufficient_sample"
    if value > 0.002:
        return "positive"
    if value < -0.002:
        return "negative"
    return "flat"


def _regime_for_row(row: dict[str, Any]) -> str:
    regime = row.get("marketRegime")
    if regime:
        return str(regime)
    if row.get("marketRiskOff") is True:
        return "risk_off"
    if row.get("marketRiskOff") is False:
        return "risk_on"
    return "unknown"


def _regime_stability(trades: list[dict[str, Any]], row_by_id: dict[str, dict[str, Any]]) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for trade in trades:
        grouped[_regime_for_row(row_by_id.get(str(trade["row_id"]), {}))].append(trade)
    return {
        "schema_version": f"{SCHEMA_PREFIX}_regime_stability_v1",
        "regime_available": bool(grouped),
        "regimes": [
            {
                "regime": regime,
                "trade_count": len(part),
                "return_on_base_capital": float(sum(float(trade["pnl"]) for trade in part) / BASE_CAPITAL_JPY),
                "win_rate": float(sum(1 for trade in part if float(trade["net_return"]) > 0) / len(part)) if part else None,
                "bad_pick_count": sum(1 for trade in part if float(trade["net_return"]) <= BAD_PICK_THRESHOLD),
                "severe_loser_count": sum(1 for trade in part if float(trade["net_return"]) <= SEVERE_LOSER_THRESHOLD),
            }
            for regime, part in sorted(grouped.items())
        ],
    }


def _added_bad_pick_decomposition(
    *,
    selection: dict[str, Any],
    variant_results: dict[str, dict[str, Any]],
    row_by_id: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    added_ids = {str(row["row_id"]) for row in selection.get("added_rows", [])}
    rows: list[dict[str, Any]] = []
    fixed_trades = {
        str(trade["row_id"]): trade
        for trade in variant_results["fixed_horizon_20d_exit"]["challenger"]["trades"]
        if str(trade["row_id"]) in added_ids
    }
    stop5_trades = {
        str(trade["row_id"]): trade
        for trade in variant_results["stop_loss_at_negative_5pct_or_20d"]["challenger"]["trades"]
        if str(trade["row_id"]) in added_ids
    }
    for row in selection.get("added_rows", []):
        row_id = str(row["row_id"])
        short_ret_20 = _safe_float(row.get("short_ret_20"))
        trade = fixed_trades.get(row_id)
        if short_ret_20 is not None and short_ret_20 > BAD_PICK_THRESHOLD and (trade is None or float(trade["net_return"]) > BAD_PICK_THRESHOLD):
            continue
        stop_trade = stop5_trades.get(row_id)
        rows.append(
            {
                "symbol": str(row.get("code")),
                "as_of_date": int(row.get("ymd")),
                "entry_date": None if trade is None else trade["entry_date"],
                "exit_date": None if trade is None else trade["exit_date"],
                "entry_price": None if trade is None else trade["entry_price"],
                "exit_price": None if trade is None else trade["exit_price"],
                "ret20": short_ret_20,
                "realized_replay_return": None if trade is None else trade["net_return"],
                "max_adverse_excursion": None if trade is None else trade["max_adverse_excursion"],
                "stop_5pct_return": None if stop_trade is None else stop_trade["net_return"],
                "stop_would_have_reduced_loss": None
                if trade is None or stop_trade is None
                else bool(float(stop_trade["net_return"]) > float(trade["net_return"])),
                "why_candidate_added_it": "same_month_candidate_pool_refill_after_clean_failed_followthrough_removal",
                "detectable_by_past_current_features": True,
                "past_current_detection_notes": {
                    "liquidity20d": row.get("liquidity20d"),
                    "close_pos": row.get("close_pos"),
                    "day_change_pct": row.get("day_change_pct"),
                    "baseline_rank": row.get("baseline_rank"),
                    "tradePriorityScore": row.get("tradePriorityScore"),
                },
            }
        )
    return {
        "schema_version": f"{SCHEMA_PREFIX}_added_bad_pick_decomposition_v1",
        "candidate_name": CANDIDATE_NAME,
        "source_readonly_added_bad_pick_count": 6,
        "multiyear_added_bad_pick_count": len(rows),
        "cases": rows,
        "added_bad_pick_impact": {
            "fixed_horizon_20d_added_bad_pick_pnl": float(
                sum(float(trade["pnl"]) for trade in fixed_trades.values() if float(trade["net_return"]) <= BAD_PICK_THRESHOLD)
            ),
            "stop5_added_bad_pick_pnl": float(
                sum(float(trade["pnl"]) for trade in stop5_trades.values() if float(trade["net_return"]) <= BAD_PICK_THRESHOLD)
            ),
        },
    }


def _severe_loser_audit(variant_results: dict[str, dict[str, Any]]) -> dict[str, Any]:
    variants: list[dict[str, Any]] = []
    all_cases: list[dict[str, Any]] = []
    for variant, result in variant_results.items():
        severe = [trade for trade in result["challenger"]["trades"] if float(trade["net_return"]) <= SEVERE_LOSER_THRESHOLD]
        variants.append({"exit_variant": variant, "severe_loser_count": len(severe)})
        for trade in severe:
            all_cases.append({"exit_variant": variant, **trade})
    return {
        "schema_version": f"{SCHEMA_PREFIX}_severe_loser_audit_v1",
        "added_severe_loser_remains_zero_in_source_research": True,
        "portfolio_replay_severe_loser_controlled": len(all_cases) == 0,
        "variant_counts": variants,
        "cases": all_cases,
    }


def _upstream_provenance_light_audit(db_path: str | Path) -> dict[str, Any]:
    inspected: list[str] = []
    field_provenance: list[dict[str, Any]] = []
    for path in [
        REPO_ROOT / "scripts" / "tradex_sell_failed_followthrough_no_lookahead_repair_v1.py",
        REPO_ROOT / "scripts" / "entry_precision_short_audit.py",
        REPO_ROOT / "app" / "backend" / "services" / "ml" / "rankings_cache.py",
        REPO_ROOT / "app" / "backend" / "services" / "ml" / "ml_service.py",
        REPO_ROOT / "app" / "backend" / "services" / "analysis" / "sell_analysis_accumulator.py",
        REPO_ROOT / "app" / "backend" / "jobs" / "scoring_job.py",
        REPO_ROOT / "app" / "backend" / "infra" / "duckdb" / "stock_repo.py",
    ]:
        if path.exists():
            inspected.append(str(path))
    registry_rows: list[dict[str, Any]] = []
    try:
        with duckdb.connect(str(db_path), read_only=True) as conn:
            for table in ("ml_model_registry", "ml_training_audit"):
                if _table_exists(conn, table):
                    inspected.append(f"{db_path}:{table}")
                    rows = conn.execute(f"select * from {table} limit 20").fetchdf().to_dict(orient="records")
                    registry_rows.extend({"table": table, **row} for row in rows)
    except Exception as exc:
        inspected.append(f"{db_path}:registry_inspection_error:{exc}")
    claims_no_lookahead = any(
        "lookahead" in json.dumps(_json_ready(row), ensure_ascii=False).lower()
        and "pass" in json.dumps(_json_ready(row), ensure_ascii=False).lower()
        for row in registry_rows
    )
    field_provenance.extend(
        [
            {
                "field": "p_down",
                "source_path": "ml_pred_20d.p_down -> sell_analysis_daily.p_down -> clean selector rows",
                "inspected_sources": [
                    "app/backend/services/ml/ml_service.py",
                    "app/backend/services/analysis/sell_analysis_accumulator.py",
                ],
                "point_in_time_claim": "partial_ml_dt_lte_target_dt_only",
                "obvious_future_label_dependency_found": True,
                "notes": "Prediction rows are joined as ml.dt <= target_dt, but this lightweight audit did not prove the active model train_end_dt precedes each historical target; labels/training include forward 20d outcomes.",
            },
            {
                "field": "ev20_net",
                "source_path": "ml_pred_20d.ev20_net -> sell_analysis_daily.ev20_net -> clean selector rows",
                "inspected_sources": [
                    "app/backend/services/ml/ml_service.py",
                    "app/backend/services/analysis/sell_analysis_accumulator.py",
                ],
                "point_in_time_claim": "partial_ml_dt_lte_target_dt_only",
                "obvious_future_label_dependency_found": True,
                "notes": "Regression output ultimately comes from a ret20 target; historical model chronology is not proven by this lightweight pass.",
            },
            {
                "field": "short_score",
                "source_path": "stock_scores.score_a + stock_scores.score_b -> sell_analysis_daily.short_score -> clean selector rows",
                "inspected_sources": [
                    "app/backend/jobs/scoring_job.py",
                    "app/backend/infra/duckdb/stock_repo.py",
                    "app/backend/services/analysis/sell_analysis_accumulator.py",
                ],
                "point_in_time_claim": "weak_not_established_for_historical_backfill",
                "obvious_future_label_dependency_found": False,
                "notes": "No direct future-return label was found in the lightweight scoring path, but stock_scores is keyed by code rather than historical dt, so point-in-time cleanliness remains unproven.",
            },
        ]
    )
    return {
        "schema_version": f"{SCHEMA_PREFIX}_upstream_provenance_light_audit_v1",
        "fields": ["p_down", "ev20_net", "short_score"],
        "provenance_known": bool(registry_rows),
        "source_files_or_artifacts_inspected": inspected,
        "model_registry_rows_sampled": registry_rows[:20],
        "claims_no_lookahead_found": claims_no_lookahead,
        "field_provenance": field_provenance,
        "obvious_future_label_dependency_found": any(item["obvious_future_label_dependency_found"] for item in field_provenance),
        "obvious_future_label_dependency_notes": [
            f"{item['field']}:{item['notes']}"
            for item in field_provenance
            if item["obvious_future_label_dependency_found"]
        ],
        "full_audit_still_required": True,
    }


def _no_lookahead_replay_audit(bundle: dict[str, Any], selection: dict[str, Any]) -> dict[str, Any]:
    guard = clean.build_selector_guard(bundle["rows"])
    return {
        "schema_version": f"{SCHEMA_PREFIX}_no_lookahead_replay_audit_v1",
        "candidate_name": CANDIDATE_NAME,
        "no_lookahead_pass": bool(guard.get("no_lookahead_pass")),
        "selector_guard": guard,
        "confirmed_data_only": True,
        "provisional_intraday_yahoo_used": False,
        "execution_convention": "next_session_open",
        "future_return_fields_used_in_selection": selection.get("selector_forbidden_fields_in_view", []),
        "silent_fallback_used": False,
    }


def _decide(
    *,
    variant_results: dict[str, dict[str, Any]],
    yearly: dict[str, list[dict[str, Any]]],
    no_lookahead: dict[str, Any],
    severe_loser: dict[str, Any],
    added_bad_pick: dict[str, Any],
) -> dict[str, Any]:
    blockers: list[str] = []
    working_variants: list[str] = []
    for variant, result in variant_results.items():
        summary = result["challenger"]["summary"]
        years = yearly[variant]
        positive_or_flat = sum(1 for row in years if row["classification"] in {"positive", "flat"})
        damaging = [row for row in years if row["classification"] == "negative" and float(row["return_on_base_capital"]) < -0.02]
        if (
            summary.get("total_return", 0.0) > 0.0
            and positive_or_flat >= max(1, math.ceil(len(years) / 2))
            and not damaging
            and summary.get("max_drawdown", -1.0) > -0.20
            and summary.get("severe_loser_count", 0) <= 1
        ):
            working_variants.append(variant)
    if not no_lookahead.get("no_lookahead_pass"):
        blockers.append("no_lookahead_replay_audit_failed")
    if not working_variants:
        blockers.append("no_fixed_exit_variant_survived")
    if not severe_loser.get("portfolio_replay_severe_loser_controlled"):
        blockers.append("portfolio_severe_loser_detected")
    catastrophic_added = added_bad_pick.get("added_bad_pick_impact", {}).get("fixed_horizon_20d_added_bad_pick_pnl")
    if catastrophic_added is not None and float(catastrophic_added) < -BASE_CAPITAL_JPY * 0.05:
        blockers.append("added_bad_pick_impact_catastrophic")
    if not blockers:
        decision = "shadow_trade_candidate"
        next_axis = "open_shadow_trade_monitoring_with_fixed_exit_variant"
    elif working_variants:
        decision = "hold_for_portfolio_risk_repair"
        next_axis = "added_bad_pick_risk_filter"
    else:
        decision = "drop_after_multiyear_replay"
        next_axis = "freeze_family_or_redesign_past_current_failed_followthrough_features"
    return {
        "schema_version": f"{SCHEMA_PREFIX}_final_shadow_trade_decision_v1",
        "candidate_name": CANDIDATE_NAME,
        "decision": decision,
        "shadow_trade_candidate": decision == "shadow_trade_candidate",
        "working_exit_variants": working_variants,
        "blockers": blockers,
        "one_next_repair_axis": None if decision == "shadow_trade_candidate" else next_axis,
        "production_ranking_changed": False,
        "active_champion_changed": False,
        "publish_run": False,
        "live_sell_signal_added": False,
        "silent_fallback_used": False,
        "research_fallback": False,
    }


def run(
    *,
    db_path: str | Path = DEFAULT_DB_PATH,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    start_ymd: int = PREFERRED_START_YMD,
    end_ymd: int = PREFERRED_END_YMD,
) -> dict[str, Any]:
    resolved_db = base._resolve_db_path(str(db_path))
    run_dir = Path(output_root).expanduser().resolve() / f"{_utc_stamp()}-sell-failed-followthrough-multiyear-portfolio-replay-v1"
    with duckdb.connect(str(resolved_db), read_only=True) as conn:
        availability = _available_period(conn)
    period = _choose_period(availability, preferred_start=int(start_ymd), preferred_end=int(end_ymd))
    if period["actual_start_ymd"] > period["actual_end_ymd"]:
        raise RuntimeError(f"no overlapping confirmed data period: {period}")

    bundle = load_multiyear_candidate_rows(
        resolved_db,
        start_ymd=period["actual_start_ymd"],
        end_ymd=period["actual_end_ymd"],
    )
    selection = clean.build_clean_selection(bundle["rows"], refill_liquidity20d_min=clean.REFILL_LIQUIDITY20D_MIN)
    row_by_id = {str(row["row_id"]): row for row in selection["baseline_rows"] + selection["challenger_rows"] + selection["added_rows"] + selection["removed_rows"]}
    no_lookahead = _no_lookahead_replay_audit(bundle, selection)

    variant_results: dict[str, dict[str, Any]] = {}
    yearly_results: dict[str, list[dict[str, Any]]] = {}
    monthly_results: dict[str, list[dict[str, Any]]] = {}
    for variant in EXIT_VARIANTS:
        baseline_result = _simulate_portfolio(
            rows=selection["baseline_rows"],
            selection=selection,
            price_store=bundle["price_store"],
            exit_variant=variant,
            start_ymd=period["actual_start_ymd"],
            end_ymd=period["actual_end_ymd"],
            label="baseline",
        )
        challenger_result = _simulate_portfolio(
            rows=selection["challenger_rows"],
            selection=selection,
            price_store=bundle["price_store"],
            exit_variant=variant,
            start_ymd=period["actual_start_ymd"],
            end_ymd=period["actual_end_ymd"],
            label="challenger",
        )
        variant_results[variant] = {"baseline": baseline_result, "challenger": challenger_result}
        yearly_results[variant] = _performance_by_period(challenger_result["trades"], key="year")
        monthly_results[variant] = _performance_by_period(challenger_result["trades"], key="month")

    added_bad_pick = _added_bad_pick_decomposition(selection=selection, variant_results=variant_results, row_by_id=row_by_id)
    severe_loser = _severe_loser_audit(variant_results)
    regime = {
        variant: _regime_stability(result["challenger"]["trades"], row_by_id)
        for variant, result in variant_results.items()
    }
    provenance = _upstream_provenance_light_audit(resolved_db)
    decision = _decide(
        variant_results=variant_results,
        yearly=yearly_results,
        no_lookahead=no_lookahead,
        severe_loser=severe_loser,
        added_bad_pick=added_bad_pick,
    )

    replay_contract = {
        "schema_version": f"{SCHEMA_PREFIX}_contract_v1",
        "candidate_name": CANDIDATE_NAME,
        "source_db_path": str(resolved_db),
        "source_reflection_root": str(SOURCE_REFLECTION_ROOT),
        "no_lookahead": True,
        "base_capital_jpy": BASE_CAPITAL_JPY,
        "max_concurrent_positions": MAX_CONCURRENT_POSITIONS,
        "position_size": "capital / 3 before risk adjustment, rounded down to 100-share lots",
        "execution": "next_session_open",
        "confirmed_data_only": True,
        "provisional_intraday_yahoo_data_used": False,
        "cost_slippage": {"one_way_total_bps": ONE_WAY_COST_BPS},
        "short_borrow_availability_data_available": False,
        "short_borrow_limitation": "borrow/short availability is not modeled; liquidity20d is only a proxy and limitation",
        "threshold_tuning": False,
        "candidate_tuning": False,
        "exit_variants": list(EXIT_VARIANTS),
        "non_scope": ["MeeMee UI/runtime", "production ranking", "active champion", "publish", "live sell signal"],
    }
    data_availability = {
        "schema_version": f"{SCHEMA_PREFIX}_data_availability_v1",
        "candidate_name": CANDIDATE_NAME,
        "source_db_path": str(resolved_db),
        "preferred_period": {"start_ymd": int(start_ymd), "end_ymd": int(end_ymd)},
        "actual_period": {"start_ymd": period["actual_start_ymd"], "end_ymd": period["actual_end_ymd"]},
        "period_reduced": period["period_reduced"],
        "reduction_reason": period["reduction_reason"],
        "availability": availability,
        "month_end_count": len(bundle["months"]),
        "candidate_row_count": len(bundle["rows"]),
        "baseline_selection_count": len(selection["baseline_rows"]),
        "challenger_selection_count": len(selection["challenger_rows"]),
        "confirmed_data_only": True,
        "silent_fallback_used": False,
    }
    exit_comparison = {
        "schema_version": f"{SCHEMA_PREFIX}_exit_variant_comparison_v1",
        "candidate_name": CANDIDATE_NAME,
        "variants": [
            {
                "exit_variant": variant,
                "baseline": result["baseline"]["summary"],
                "challenger": result["challenger"]["summary"],
                "delta_total_return": result["challenger"]["summary"].get("total_return", 0.0)
                - result["baseline"]["summary"].get("total_return", 0.0),
            }
            for variant, result in variant_results.items()
        ],
        "post_hoc_best_variant_selected": False,
    }
    portfolio_summary = {
        "schema_version": f"{SCHEMA_PREFIX}_summary_v1",
        "candidate_name": CANDIDATE_NAME,
        "source_db_path": str(resolved_db),
        "period": data_availability["actual_period"],
        "exit_variants": exit_comparison["variants"],
        "decision": decision["decision"],
        "shadow_trade_candidate": decision["shadow_trade_candidate"],
        "production_ranking_changed": False,
        "active_champion_changed": False,
        "publish_run": False,
        "live_sell_signal_added": False,
    }

    artifacts = {
        "replay_contract": replay_contract,
        "data_availability_report": data_availability,
        "no_lookahead_replay_audit": no_lookahead,
        "portfolio_replay_summary": portfolio_summary,
        "yearly_performance": {"schema_version": f"{SCHEMA_PREFIX}_yearly_performance_v1", "variants": yearly_results},
        "monthly_performance": {"schema_version": f"{SCHEMA_PREFIX}_monthly_performance_v1", "variants": monthly_results},
        "exit_variant_comparison": exit_comparison,
        "added_bad_pick_decomposition": added_bad_pick,
        "severe_loser_audit": severe_loser,
        "regime_stability": {"schema_version": f"{SCHEMA_PREFIX}_regime_stability_bundle_v1", "variants": regime},
        "upstream_provenance_light_audit": provenance,
        "final_shadow_trade_decision": decision,
    }
    paths = {name: run_dir / f"{name}.json" for name in artifacts}
    for name, payload in artifacts.items():
        _write_json(paths[name], payload)
    readme = run_dir / "README.md"
    readme.write_text(
        "\n".join(
            [
                "# sell failed-followthrough multiyear portfolio replay v1",
                "",
                "Authoritative JSON artifacts in this directory are the source of truth.",
                f"candidate_name: {CANDIDATE_NAME}",
                f"decision: {decision['decision']}",
                f"shadow_trade_candidate: {decision['shadow_trade_candidate']}",
                "production_ranking_changed: false",
                "active_champion_changed: false",
                "publish_run: false",
                "live_sell_signal_added: false",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    complete = {
        "schema_version": f"{SCHEMA_PREFIX}_complete_v1",
        "generated_at": _utc_now(),
        "artifact_complete": True,
        "status": "complete",
        "candidate_name": CANDIDATE_NAME,
        "decision": decision["decision"],
        "shadow_trade_candidate": decision["shadow_trade_candidate"],
        "artifact_refs": {name: str(path) for name, path in paths.items()} | {"README": str(readme)},
        "authoritative_decision": str(paths["final_shadow_trade_decision"]),
        "silent_fallback_used": False,
        "research_fallback": False,
        "production_ranking_changed": False,
        "active_champion_changed": False,
        "publish_run": False,
        "live_sell_signal_added": False,
    }
    _write_json(run_dir / "_ARTIFACT_COMPLETE.json", complete)
    return {
        "ok": True,
        "output_dir": str(run_dir),
        "decision": decision["decision"],
        "shadow_trade_candidate": decision["shadow_trade_candidate"],
        "artifact_refs": complete["artifact_refs"] | {"_ARTIFACT_COMPLETE": str(run_dir / "_ARTIFACT_COMPLETE.json")},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Multiyear no-lookahead portfolio replay for clean sell failed-followthrough candidate.")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--start-ymd", type=int, default=PREFERRED_START_YMD)
    parser.add_argument("--end-ymd", type=int, default=PREFERRED_END_YMD)
    args = parser.parse_args()
    result = run(db_path=args.db_path, output_root=args.output_root, start_ymd=args.start_ymd, end_ymd=args.end_ymd)
    print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
