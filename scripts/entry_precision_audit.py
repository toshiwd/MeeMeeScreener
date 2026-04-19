from __future__ import annotations

import argparse
import json
import math
import os
import sys
from collections import Counter, defaultdict
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import duckdb
import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.services.ml import rankings_cache as rc  # noqa: E402


DEFAULT_DB_PATH = Path(r"G:\Tradex\scratch\source_snapshots\tradex_research_snapshot_full_20230101_20260226.duckdb")
DEFAULT_OUTPUT_DIR = REPO_ROOT / "artifacts" / "research_inventory"
ROUND_TRIP_COST = 0.0
ENTRY_SCORE_EPS = 0.005


@dataclass(frozen=True)
class GateThresholds:
    min_entry_score_up: float
    min_entry_score_down: float
    min_weekly_up: float
    min_weekly_down: float
    min_monthly_up: float
    min_monthly_down: float
    min_prob_up: float
    min_prob_down: float


BASELINE_THRESHOLDS = GateThresholds(
    min_entry_score_up=float(getattr(rc, "_STRICT_RULE_TRADE_UP_MIN_ENTRY_SCORE")),
    min_entry_score_down=float(getattr(rc, "_STRICT_RULE_TRADE_DOWN_MIN_ENTRY_SCORE")),
    min_weekly_up=float(getattr(rc, "_STRICT_RULE_TRADE_UP_MIN_WEEKLY")),
    min_weekly_down=float(getattr(rc, "_STRICT_RULE_TRADE_DOWN_MIN_WEEKLY")),
    min_monthly_up=float(getattr(rc, "_STRICT_RULE_TRADE_UP_MIN_MONTHLY")),
    min_monthly_down=float(getattr(rc, "_STRICT_RULE_TRADE_DOWN_MIN_MONTHLY")),
    min_prob_up=float(getattr(rc, "_STRICT_RULE_TRADE_UP_MIN_PROB")),
    min_prob_down=float(getattr(rc, "_STRICT_RULE_TRADE_DOWN_MIN_PROB")),
)

CHALLENGER_THRESHOLDS = GateThresholds(
    min_entry_score_up=BASELINE_THRESHOLDS.min_entry_score_up + 0.03,
    min_entry_score_down=BASELINE_THRESHOLDS.min_entry_score_down + 0.03,
    min_weekly_up=BASELINE_THRESHOLDS.min_weekly_up + 0.03,
    min_weekly_down=BASELINE_THRESHOLDS.min_weekly_down + 0.03,
    min_monthly_up=BASELINE_THRESHOLDS.min_monthly_up + 0.03,
    min_monthly_down=BASELINE_THRESHOLDS.min_monthly_down + 0.03,
    min_prob_up=BASELINE_THRESHOLDS.min_prob_up + 0.03,
    min_prob_down=BASELINE_THRESHOLDS.min_prob_down + 0.03,
)


def _resolve_db_path(cli_value: str | None) -> Path:
    if cli_value:
        path = Path(cli_value).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"DB not found: {path}")
        return path
    env = os.getenv("TRADEX_SNAPSHOT_DB_PATH") or os.getenv("STOCKS_DB_PATH")
    if env:
        path = Path(env).expanduser().resolve()
        if path.exists():
            return path
    if DEFAULT_DB_PATH.exists():
        return DEFAULT_DB_PATH.resolve()
    raise FileNotFoundError("Could not resolve snapshot DB path. Pass --db-path or set TRADEX_SNAPSHOT_DB_PATH.")


def _ymd_expr(col: str) -> str:
    return f"""
        CASE
          WHEN TRY_CAST({col} AS BIGINT) BETWEEN 19000101 AND 20991231 THEN CAST({col} AS BIGINT)
          WHEN TRY_CAST({col} AS BIGINT) >= 1000000000000 THEN CAST(strftime(to_timestamp(CAST({col} AS BIGINT) / 1000), '%Y%m%d') AS INTEGER)
          WHEN TRY_CAST({col} AS BIGINT) BETWEEN 600000000 AND 5000000000 THEN CAST(strftime(to_timestamp(CAST({col} AS BIGINT)), '%Y%m%d') AS INTEGER)
          ELSE NULL
        END
    """


def _ymd_to_ts(ymd: int) -> str:
    return f"{int(str(int(ymd))[:4])}-{int(str(int(ymd))[4:6]):02d}-{int(str(int(ymd))[6:8]):02d}"


def _month_end_dates(conn: duckdb.DuckDBPyConnection, *, start_ymd: int, end_ymd: int) -> list[int]:
    rows = conn.execute(
        f"""
        WITH d AS (
          SELECT {_ymd_expr("date")} AS ymd
          FROM daily_bars
        ),
        m AS (
          SELECT (ymd / 100)::INT AS ym, MAX(ymd) AS asof_ymd
          FROM d
          WHERE ymd BETWEEN ? AND ?
          GROUP BY (ymd / 100)::INT
        )
        SELECT asof_ymd
        FROM m
        ORDER BY asof_ymd
        """,
        [int(start_ymd), int(end_ymd)],
    ).fetchall()
    return [int(row[0]) for row in rows if row and row[0] is not None]


def _load_regime_map(conn: duckdb.DuckDBPyConnection, dates: Iterable[int]) -> dict[int, str]:
    dates = [int(v) for v in dates]
    if not dates:
        return {}
    placeholders = ", ".join("?" for _ in dates)
    rows = conn.execute(
        f"""
        SELECT {_ymd_expr("dt")} AS ymd, CAST(regime_id AS VARCHAR) AS regime_id
        FROM market_regime_daily
        WHERE {_ymd_expr("dt")} IN ({placeholders})
        """,
        dates,
    ).fetchall()
    out: dict[int, str] = {}
    for ymd, regime_id in rows:
        if ymd is None or regime_id is None:
            continue
        out[int(ymd)] = str(regime_id)
    return out


def _load_price_store(conn: duckdb.DuckDBPyConnection) -> dict[str, dict[str, np.ndarray]]:
    df = conn.execute(
        f"""
        SELECT
          CAST(code AS VARCHAR) AS code,
          {_ymd_expr("date")} AS ymd,
          CAST(h AS DOUBLE) AS high,
          CAST(l AS DOUBLE) AS low,
          CAST(c AS DOUBLE) AS close
        FROM daily_bars
        WHERE COALESCE(source, 'pan') <> 'yahoo'
        ORDER BY code, ymd
        """
    ).fetchdf()
    if df.empty:
        return {}
    df = df.dropna(subset=["code", "ymd", "high", "low", "close"])
    df["ymd"] = df["ymd"].astype(np.int64)
    store: dict[str, dict[str, np.ndarray]] = {}
    for code, part in df.groupby("code", sort=False):
        store[str(code)] = {
            "ymd": part["ymd"].to_numpy(dtype=np.int64, copy=True),
            "high": part["high"].to_numpy(dtype=np.float64, copy=True),
            "low": part["low"].to_numpy(dtype=np.float64, copy=True),
            "close": part["close"].to_numpy(dtype=np.float64, copy=True),
        }
    return store


def _load_ma_store(conn: duckdb.DuckDBPyConnection) -> dict[tuple[str, int], tuple[float | None, float | None]]:
    df = conn.execute(
        f"""
        SELECT
          CAST(code AS VARCHAR) AS code,
          {_ymd_expr("date")} AS ymd,
          CAST(ma20 AS DOUBLE) AS ma20,
          CAST(ma60 AS DOUBLE) AS ma60
        FROM daily_ma
        ORDER BY code, ymd
        """
    ).fetchdf()
    out: dict[tuple[str, int], tuple[float | None, float | None]] = {}
    if df.empty:
        return out
    for row in df.itertuples(index=False):
        if row.code is None or row.ymd is None:
            continue
        out[(str(row.code), int(row.ymd))] = (
            float(row.ma20) if row.ma20 is not None and np.isfinite(float(row.ma20)) else None,
            float(row.ma60) if row.ma60 is not None and np.isfinite(float(row.ma60)) else None,
        )
    return out


def _load_candidate_cache(conn: duckdb.DuckDBPyConnection, ymd: int) -> dict[tuple[str, str, str], list[dict[str, Any]]]:
    return rc._build_cache_asof(conn, int(ymd))


def _copy_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [dict(item) for item in items]


@contextmanager
def _patched_gate(thresholds: GateThresholds):
    snapshot = {
        "_STRICT_RULE_TRADE_UP_MIN_ENTRY_SCORE": getattr(rc, "_STRICT_RULE_TRADE_UP_MIN_ENTRY_SCORE"),
        "_STRICT_RULE_TRADE_DOWN_MIN_ENTRY_SCORE": getattr(rc, "_STRICT_RULE_TRADE_DOWN_MIN_ENTRY_SCORE"),
        "_STRICT_RULE_TRADE_UP_MIN_WEEKLY": getattr(rc, "_STRICT_RULE_TRADE_UP_MIN_WEEKLY"),
        "_STRICT_RULE_TRADE_DOWN_MIN_WEEKLY": getattr(rc, "_STRICT_RULE_TRADE_DOWN_MIN_WEEKLY"),
        "_STRICT_RULE_TRADE_UP_MIN_MONTHLY": getattr(rc, "_STRICT_RULE_TRADE_UP_MIN_MONTHLY"),
        "_STRICT_RULE_TRADE_DOWN_MIN_MONTHLY": getattr(rc, "_STRICT_RULE_TRADE_DOWN_MIN_MONTHLY"),
        "_STRICT_RULE_TRADE_UP_MIN_PROB": getattr(rc, "_STRICT_RULE_TRADE_UP_MIN_PROB"),
        "_STRICT_RULE_TRADE_DOWN_MIN_PROB": getattr(rc, "_STRICT_RULE_TRADE_DOWN_MIN_PROB"),
    }
    try:
        rc._STRICT_RULE_TRADE_UP_MIN_ENTRY_SCORE = float(thresholds.min_entry_score_up)
        rc._STRICT_RULE_TRADE_DOWN_MIN_ENTRY_SCORE = float(thresholds.min_entry_score_down)
        rc._STRICT_RULE_TRADE_UP_MIN_WEEKLY = float(thresholds.min_weekly_up)
        rc._STRICT_RULE_TRADE_DOWN_MIN_WEEKLY = float(thresholds.min_weekly_down)
        rc._STRICT_RULE_TRADE_UP_MIN_MONTHLY = float(thresholds.min_monthly_up)
        rc._STRICT_RULE_TRADE_DOWN_MIN_MONTHLY = float(thresholds.min_monthly_down)
        rc._STRICT_RULE_TRADE_UP_MIN_PROB = float(thresholds.min_prob_up)
        rc._STRICT_RULE_TRADE_DOWN_MIN_PROB = float(thresholds.min_prob_down)
        yield
    finally:
        for key, value in snapshot.items():
            setattr(rc, key, value)


def _selected_items(
    items: list[dict[str, Any]],
    *,
    direction: str,
    thresholds: GateThresholds,
) -> list[dict[str, Any]]:
    decorated = _decorated_items(items, direction=direction, thresholds=thresholds)
    qualified = [dict(item) for item in decorated if bool(item.get("entryQualified"))]
    rc._apply_trade_priority_scores(qualified, direction=direction)
    qualified.sort(key=rc._trade_priority_sort_key)
    return qualified


def _decorated_items(
    items: list[dict[str, Any]],
    *,
    direction: str,
    thresholds: GateThresholds,
) -> list[dict[str, Any]]:
    with _patched_gate(thresholds):
        return rc._decorate_rule_items_with_entry_gate(_copy_items(items), direction=direction, risk_mode="balanced")


def _entry_score(item: dict[str, Any]) -> float:
    v = item.get("entryScore")
    try:
        return float(v)
    except Exception:
        return float("nan")


def _first_finite(*values: Any) -> float | None:
    for v in values:
        if isinstance(v, (int, float)) and math.isfinite(float(v)):
            return float(v)
    return None


def _side_adjusted_ret(direction: str, entry: float, exit_: float) -> float:
    raw = (exit_ / entry) - 1.0
    return float(raw if direction == "up" else -raw)


def _path_metrics(
    *,
    direction: str,
    code: str,
    asof_ymd: int,
    price_store: dict[str, dict[str, np.ndarray]],
    ma_store: dict[tuple[str, int], tuple[float | None, float | None]],
) -> dict[str, Any] | None:
    series = price_store.get(code)
    if not series:
        return None
    ymd = series["ymd"]
    idxs = np.where(ymd == int(asof_ymd))[0]
    if idxs.size == 0:
        return None
    idx = int(idxs[-1])
    if idx + 20 >= len(ymd):
        return None
    entry = float(series["close"][idx])
    future_5 = float(series["close"][idx + 5])
    future_10 = float(series["close"][idx + 10])
    future_20 = float(series["close"][idx + 20])
    lows_20 = series["low"][idx + 1 : idx + 21]
    highs_20 = series["high"][idx + 1 : idx + 21]
    if len(lows_20) < 20 or len(highs_20) < 20:
        return None
    if direction == "up":
        mae20 = float(max(0.0, (entry - float(np.nanmin(lows_20))) / entry))
        mfe20 = float(max(0.0, (float(np.nanmax(highs_20)) - entry) / entry))
        early_5 = float((float(np.nanmax(series["close"][idx + 1 : idx + 6])) / entry) - 1.0)
        early_10 = float((float(np.nanmax(series["close"][idx + 1 : idx + 11])) / entry) - 1.0)
    else:
        mae20 = float(max(0.0, (float(np.nanmax(highs_20)) - entry) / entry))
        mfe20 = float(max(0.0, (entry - float(np.nanmin(lows_20))) / entry))
        early_5 = float(-((float(np.nanmin(series["close"][idx + 1 : idx + 6])) / entry) - 1.0))
        early_10 = float(-((float(np.nanmin(series["close"][idx + 1 : idx + 11])) / entry) - 1.0))
    ret20 = _side_adjusted_ret(direction, entry, future_20)
    ret5 = _side_adjusted_ret(direction, entry, future_5)
    ret10 = _side_adjusted_ret(direction, entry, future_10)
    close_slice = series["close"][idx + 1 : idx + 21]
    if direction == "up":
        best_close = float(np.nanmax(close_slice) / entry - 1.0)
    else:
        best_close = float(-(np.nanmin(close_slice) / entry - 1.0))
    ma20, ma60 = ma_store.get((code, int(asof_ymd)), (None, None))
    dist_ma20 = float((entry / ma20) - 1.0) if ma20 is not None and ma20 != 0 else None
    dist_ma60 = float((entry / ma60) - 1.0) if ma60 is not None and ma60 != 0 else None
    return {
        "entry_close": entry,
        "ret5": ret5,
        "ret10": ret10,
        "ret20": ret20,
        "best_close_20": best_close,
        "mae20": mae20,
        "mfe20": mfe20,
        "early_confirm_5": early_5,
        "early_confirm_10": early_10,
        "flat_20": abs(ret20) <= 0.005,
        "immediate_reverse": ret5 <= 0.0,
        "dist_ma20": dist_ma20,
        "dist_ma60": dist_ma60,
    }


def _quantile_label(values: list[float], q: float) -> float | None:
    arr = np.array([v for v in values if isinstance(v, (int, float)) and math.isfinite(float(v))], dtype=np.float64)
    if arr.size == 0:
        return None
    return float(np.quantile(arr, q))


def _side_good_flags(metrics: dict[str, Any]) -> dict[str, bool]:
    ret20 = float(metrics["ret20"])
    mae20 = float(metrics["mae20"])
    early10 = float(metrics["early_confirm_10"])
    early5 = float(metrics["early_confirm_5"])
    return {
        "ret20_only": ret20 > 0.0,
        "ret20_mae_cap": ret20 > 0.0 and mae20 <= 0.03,
        "ret20_early_confirmation": ret20 > 0.0 and max(early5, early10) > 0.0,
        "risk_adjusted": (ret20 - mae20) > 0.0,
    }


def _family_score(metrics: dict[str, Any], family: str) -> float:
    ret20 = float(metrics["ret20"])
    mae20 = float(metrics["mae20"])
    early5 = float(metrics["early_confirm_5"])
    early10 = float(metrics["early_confirm_10"])
    if family == "ret20_only":
        return ret20
    if family == "ret20_mae_cap":
        return ret20 - max(0.0, mae20 - 0.03)
    if family == "ret20_early_confirmation":
        return ret20 + 0.35 * max(0.0, early5) + 0.20 * max(0.0, early10)
    if family == "risk_adjusted":
        return ret20 - mae20 + 0.10 * max(0.0, early10)
    raise KeyError(family)


def _monthly_slice_metrics(
    *,
    asof_ymd: int,
    direction: str,
    selected: list[dict[str, Any]],
    all_items: list[dict[str, Any]],
    price_store: dict[str, dict[str, np.ndarray]],
    ma_store: dict[tuple[str, int], tuple[float | None, float | None]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    selected_rows: list[dict[str, Any]] = []
    all_rows: list[dict[str, Any]] = []
    selected_codes = {str(item.get("code")) for item in selected}
    for item in all_items:
        code = str(item.get("code") or "")
        if not code:
            continue
        metrics = _path_metrics(
            direction=direction,
            code=code,
            asof_ymd=int(asof_ymd),
            price_store=price_store,
            ma_store=ma_store,
        )
        if metrics is None:
            continue
        base = {
            "asof_ymd": int(asof_ymd),
            "code": code,
            "side": direction,
            "selected": code in selected_codes,
            "entry_score": _entry_score(item),
            "entry_qualified": bool(item.get("entryQualified")),
            "trade_priority_score": float(item.get("tradePriorityScore") or 0.0) if item.get("tradePriorityScore") is not None else None,
            "trade_entry_class": item.get("tradeEntryClass"),
            "setup_type": item.get("setupType"),
            "trade_decision_reasons": item.get("tradeDecisionReasons") or [],
            "trade_risk_watch": item.get("tradeRiskWatch") or [],
            "market_regime": item.get("marketRegime"),
            "market_risk_off": bool(item.get("marketRiskOff")),
            "liquidity20d": float(item.get("liquidity20d")) if item.get("liquidity20d") is not None else None,
            "weekly_breakout_up_prob": float(item.get("weeklyBreakoutUpProb")) if item.get("weeklyBreakoutUpProb") is not None else None,
            "weekly_breakout_down_prob": float(item.get("weeklyBreakoutDownProb")) if item.get("weeklyBreakoutDownProb") is not None else None,
            "monthly_breakout_up_prob": float(item.get("monthlyBreakoutUpProb")) if item.get("monthlyBreakoutUpProb") is not None else None,
            "monthly_breakout_down_prob": float(item.get("monthlyBreakoutDownProb")) if item.get("monthlyBreakoutDownProb") is not None else None,
            "monthly_box_state": item.get("monthlyBoxState"),
            "monthly_box_months": float(item.get("monthlyBoxMonths")) if item.get("monthlyBoxMonths") is not None else None,
            "change_pct": float(item.get("changePct")) if item.get("changePct") is not None else None,
            "candle_body_ratio": float(item.get("candleBodyRatio")) if item.get("candleBodyRatio") is not None else None,
            "candle_upper_wick_ratio": float(item.get("candleUpperWickRatio")) if item.get("candleUpperWickRatio") is not None else None,
            "candle_lower_wick_ratio": float(item.get("candleLowerWickRatio")) if item.get("candleLowerWickRatio") is not None else None,
            **metrics,
        }
        all_rows.append(base)
        if code in selected_codes:
            selected_rows.append(base)
    return selected_rows, all_rows


def _summarize_rows(rows: list[dict[str, Any]], *, all_rows: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    if not rows:
        return {
            "count": 0,
            "coverage": None,
            "hit_rate": None,
            "mean_ret20": None,
            "median_ret20": None,
            "mean_mae20": None,
            "median_mae20": None,
            "mean_mfe20": None,
            "median_mfe20": None,
            "mean_early_confirm_5": None,
            "mean_early_confirm_10": None,
            "flat_rate": None,
            "immediate_reverse_rate": None,
            "false_positive_count": 0,
            "false_neutral_count": 0,
            "bad_pick_removal_count": 0,
        }
    ret20 = np.array([float(r["ret20"]) for r in rows], dtype=np.float64)
    mae20 = np.array([float(r["mae20"]) for r in rows], dtype=np.float64)
    mfe20 = np.array([float(r["mfe20"]) for r in rows], dtype=np.float64)
    early5 = np.array([float(r["early_confirm_5"]) for r in rows], dtype=np.float64)
    early10 = np.array([float(r["early_confirm_10"]) for r in rows], dtype=np.float64)
    hit_rate = float(np.mean(ret20 > 0.0))
    false_positive = int(np.sum(ret20 <= 0.0))
    flat = int(np.sum(np.abs(ret20) <= 0.005))
    immediate_reverse = int(np.sum(early5 <= 0.0))
    bad_pick = int(np.sum((ret20 <= 0.0) | ((np.abs(ret20) <= 0.005) & (mfe20 <= 0.02))))
    false_neutral = 0
    if all_rows is not None:
        selected_keys = {(r["asof_ymd"], r["side"], r["code"]) for r in rows}
        false_neutral = sum(
            1
            for row in all_rows
            if (row["asof_ymd"], row["side"], row["code"]) not in selected_keys
            and sum(1 for flag in _side_good_flags(row).values() if flag) >= 2
        )
    return {
        "count": int(len(rows)),
        "coverage": None,
        "hit_rate": hit_rate,
        "mean_ret20": float(np.mean(ret20)),
        "median_ret20": float(np.median(ret20)),
        "mean_mae20": float(np.mean(mae20)),
        "median_mae20": float(np.median(mae20)),
        "mean_mfe20": float(np.mean(mfe20)),
        "median_mfe20": float(np.median(mfe20)),
        "mean_early_confirm_5": float(np.mean(early5)),
        "mean_early_confirm_10": float(np.mean(early10)),
        "flat_rate": float(flat / len(rows)),
        "immediate_reverse_rate": float(immediate_reverse / len(rows)),
        "false_positive_count": false_positive,
        "false_neutral_count": int(false_neutral),
        "bad_pick_removal_count": bad_pick,
    }


def _group_rows(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for row in rows:
        value = row.get(key)
        if value is None or value == "":
            value = "unknown"
        counter[str(value)] += 1
    return dict(sorted(counter.items(), key=lambda kv: (-kv[1], kv[0])))


def _collect_topk_rows(
    *,
    selected_by_key: dict[tuple[int, str], list[dict[str, Any]]],
    row_map: dict[tuple[int, str, str], dict[str, Any]],
    k: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key, selected in selected_by_key.items():
        for item in selected[:k]:
            row = row_map.get((key[0], key[1], str(item.get("code") or "")))
            if row is not None:
                rows.append(row)
    return rows


def _bucket_reason(row: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    side = str(row["side"])
    ret20 = float(row["ret20"])
    mae20 = float(row["mae20"])
    mfe20 = float(row["mfe20"])
    early5 = float(row["early_confirm_5"])
    early10 = float(row["early_confirm_10"])
    entry_score = float(row.get("entry_score") or 0.0)
    if ret20 <= 0.0:
        reasons.append("false_positive")
    if abs(ret20) <= 0.005:
        reasons.append("flat")
    if early5 <= 0.0:
        reasons.append("immediate_reverse")
    if mae20 >= 0.03 and ret20 <= 0.01:
        reasons.append("strong_adverse_excursion")
    if mfe20 <= 0.02 and abs(ret20) <= 0.02:
        reasons.append("sideways_low_range")
    if entry_score >= 0.60 and ret20 <= 0.0:
        reasons.append("high_score_no_followthrough")
    if side == "up":
        if row.get("monthly_breakout_up_prob") is not None and float(row["monthly_breakout_up_prob"]) >= 0.8:
            reasons.append("breakout_blur")
        if row.get("monthly_box_state") in {"box_mid", "box_upper"}:
            reasons.append("range_middle")
        if row.get("change_pct") is not None and float(row["change_pct"]) >= 0.05:
            reasons.append("late_stretched_entry")
        if row.get("trade_risk_watch"):
            reasons.extend([f"risk_watch:{str(v)}" for v in row["trade_risk_watch"][:2]])
    else:
        if row.get("monthly_breakout_down_prob") is not None and float(row["monthly_breakout_down_prob"]) >= 0.8:
            reasons.append("breakdown_blur")
        if row.get("monthly_box_state") == "box_mid":
            reasons.append("range_middle")
        if row.get("change_pct") is not None and float(row["change_pct"]) <= -0.05:
            reasons.append("late_stretched_entry")
        if row.get("trade_risk_watch"):
            reasons.extend([f"risk_watch:{str(v)}" for v in row["trade_risk_watch"][:2]])
    if early10 > 0.0 and ret20 > 0.0:
        reasons.append("early_confirmation")
    return list(dict.fromkeys(reasons))


def _build_error_buckets(rows: list[dict[str, Any]], selected_rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_bucket: dict[str, list[dict[str, Any]]] = defaultdict(list)
    selected_keys = {(r["asof_ymd"], r["side"], r["code"]) for r in selected_rows}
    selected_map = {(
        r["asof_ymd"],
        r["side"],
        r["code"],
    ): r for r in selected_rows}
    for row in rows:
        key = (row["asof_ymd"], row["side"], row["code"])
        if key in selected_keys:
            if float(row["ret20"]) <= 0.0 or abs(float(row["ret20"])) <= 0.005:
                bucket = "bad_pick"
            elif float(row["mae20"]) >= 0.03 and float(row["ret20"]) <= 0.01:
                bucket = "bad_pick"
            else:
                continue
        else:
            if float(row["ret20"]) > 0.0 and float(row["mae20"]) <= 0.03 and max(float(row["early_confirm_5"]), float(row["early_confirm_10"])) > 0.0:
                bucket = "false_neutral"
            else:
                continue
        enriched = dict(row)
        enriched["bucket"] = bucket
        enriched["reason_codes"] = _bucket_reason({**row, "trade_risk_watch": row.get("trade_risk_watch") or []})
        enriched["selected"] = key in selected_keys
        if key in selected_map:
            enriched["selected_entry_score"] = selected_map[key].get("entry_score")
        by_bucket[bucket].append(enriched)

    def _summarize_bucket(bucket_rows: list[dict[str, Any]]) -> dict[str, Any]:
        if not bucket_rows:
            return {
                "count": 0,
                "by_side": {},
                "by_month": {},
                "by_regime": {},
                "representative_examples": [],
            }
        sample = sorted(
            bucket_rows,
            key=lambda r: (
                -abs(float(r["ret20"])),
                -float(r["entry_score"] or 0.0),
                r["asof_ymd"],
                r["code"],
            ),
        )[:5]
        return {
            "count": int(len(bucket_rows)),
            "by_side": _group_rows(bucket_rows, "side"),
            "by_month": _group_rows(bucket_rows, "asof_ymd"),
            "by_regime": _group_rows(bucket_rows, "market_regime"),
            "representative_examples": [
                {
                    "asof_ymd": int(row["asof_ymd"]),
                    "code": row["code"],
                    "side": row["side"],
                    "ret20": float(row["ret20"]),
                    "mae20": float(row["mae20"]),
                    "mfe20": float(row["mfe20"]),
                    "entry_score": float(row["entry_score"] or 0.0),
                    "reason_codes": row["reason_codes"],
                    "trade_entry_class": row.get("trade_entry_class"),
                    "setup_type": row.get("setup_type"),
                }
                for row in sample
            ],
        }

    return {bucket: _summarize_bucket(rows) for bucket, rows in sorted(by_bucket.items())}


def _family_results(rows: list[dict[str, Any]], *, top_k_values: tuple[int, ...] = (5, 10, 20)) -> dict[str, Any]:
    families = ("ret20_only", "ret20_mae_cap", "ret20_early_confirmation", "risk_adjusted")
    rows_by_group: dict[tuple[int, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        rows_by_group[(int(row["asof_ymd"]), str(row["side"]))].append(row)
    out: dict[str, Any] = {}
    for fam in families:
        topk_payload: dict[str, Any] = {}
        for top_k in top_k_values:
            fam_rows: list[dict[str, Any]] = []
            for (_, _side), group in rows_by_group.items():
                ranked = sorted(group, key=lambda r: (_family_score(r, fam), -float(r["ret20"])), reverse=True)
                fam_rows.extend(ranked[:top_k])
            if fam_rows:
                ret = np.array([float(r["ret20"]) for r in fam_rows], dtype=np.float64)
                mae = np.array([float(r["mae20"]) for r in fam_rows], dtype=np.float64)
                topk_payload[str(top_k)] = {
                    "top_k": int(top_k),
                    "count": int(len(fam_rows)),
                    "hit_rate": float(np.mean(ret > 0.0)),
                    "median_ret20": float(np.median(ret)),
                    "mean_ret20": float(np.mean(ret)),
                    "median_mae20": float(np.median(mae)),
                    "mean_mae20": float(np.mean(mae)),
                    "flat_rate": float(np.mean(np.abs(ret) <= 0.005)),
                    "immediate_reverse_rate": float(np.mean(np.array([float(r["ret5"]) for r in fam_rows], dtype=np.float64) <= 0.0)),
                    "representative_examples": [
                        {
                            "asof_ymd": int(r["asof_ymd"]),
                            "code": r["code"],
                            "side": r["side"],
                            "score": float(_family_score(r, fam)),
                            "ret20": float(r["ret20"]),
                            "mae20": float(r["mae20"]),
                            "early_confirm_10": float(r["early_confirm_10"]),
                        }
                        for r in fam_rows[:5]
                    ],
                }
            else:
                topk_payload[str(top_k)] = {
                    "top_k": int(top_k),
                    "count": 0,
                    "hit_rate": None,
                    "median_ret20": None,
                    "mean_ret20": None,
                    "median_mae20": None,
                    "mean_mae20": None,
                    "flat_rate": None,
                    "immediate_reverse_rate": None,
                    "representative_examples": [],
                }
        out[fam] = {
            "score_definition": fam,
            "metrics_by_top_k": topk_payload,
        }
    return out


def _coverage_curve(rows: list[dict[str, Any]], *, direction: str) -> dict[str, Any]:
    thresholds = [round(v, 2) for v in np.linspace(0.0, 1.0, 21)]
    out: list[dict[str, Any]] = []
    rows = [r for r in rows if str(r["side"]) == direction]
    total = len(rows)
    for th in thresholds:
        picked = [r for r in rows if float(r["entry_score"]) >= th]
        if picked:
            ret = np.array([float(r["ret20"]) for r in picked], dtype=np.float64)
            mae = np.array([float(r["mae20"]) for r in picked], dtype=np.float64)
            hit = float(np.mean(ret > 0.0))
            out.append(
                {
                    "threshold": float(th),
                    "coverage": float(len(picked) / total) if total else None,
                    "count": int(len(picked)),
                    "hit_rate": hit,
                    "median_ret20": float(np.median(ret)),
                    "mean_ret20": float(np.mean(ret)),
                    "median_mae20": float(np.median(mae)),
                }
            )
        else:
            out.append(
                {
                    "threshold": float(th),
                    "coverage": 0.0 if total else None,
                    "count": 0,
                    "hit_rate": None,
                    "median_ret20": None,
                    "mean_ret20": None,
                    "median_mae20": None,
                }
            )
    return {
        "direction": direction,
        "curve": out,
        "total_rows": int(total),
    }


def _compare_baseline_vs_challenger(
    *,
    months: list[int],
    directions: list[str],
    all_items_by_month: dict[tuple[int, str], list[dict[str, Any]]],
    selected_baseline_by_key: dict[tuple[int, str], list[dict[str, Any]]],
    selected_challenger_by_key: dict[tuple[int, str], list[dict[str, Any]]],
    all_rows_by_key: dict[tuple[int, str], list[dict[str, Any]]],
) -> dict[str, Any]:
    changed_top5 = 0
    changed_top10 = 0
    changed_top20 = 0
    changed_rank = 0
    bad_pick_removal = 0
    top5_ret_delta: list[float] = []
    top10_ret_delta: list[float] = []
    top20_ret_delta: list[float] = []
    divergence_reasons: Counter[str] = Counter()
    monthly_rows: list[dict[str, Any]] = []
    row_map: dict[tuple[int, str, str], dict[str, Any]] = {}
    for key, rows in all_rows_by_key.items():
        for row in rows:
            row_map[(int(row["asof_ymd"]), str(row["side"]), str(row["code"]))] = row

    for asof in months:
        for direction in directions:
            key = (int(asof), direction)
            base = selected_baseline_by_key.get(key, [])
            chal = selected_challenger_by_key.get(key, [])
            base_codes = [str(r["code"]) for r in base]
            chal_codes = [str(r["code"]) for r in chal]
            base_set_5 = set(base_codes[:5])
            chal_set_5 = set(chal_codes[:5])
            base_set_10 = set(base_codes[:10])
            chal_set_10 = set(chal_codes[:10])
            base_set_20 = set(base_codes[:20])
            chal_set_20 = set(chal_codes[:20])
            changed_top5 += len(base_set_5.symmetric_difference(chal_set_5))
            changed_top10 += len(base_set_10.symmetric_difference(chal_set_10))
            changed_top20 += len(base_set_20.symmetric_difference(chal_set_20))
            shared = set(base_codes[:20]).intersection(chal_codes[:20])
            for code in shared:
                changed_rank += abs(base_codes.index(code) - chal_codes.index(code))
            all_rows = all_rows_by_key.get(key, [])
            base_top5 = [r for r in all_rows if r["code"] in base_set_5]
            chal_top5 = [r for r in all_rows if r["code"] in chal_set_5]
            base_top10 = [r for r in all_rows if r["code"] in base_set_10]
            chal_top10 = [r for r in all_rows if r["code"] in chal_set_10]
            base_top20 = [r for r in all_rows if r["code"] in base_set_20]
            chal_top20 = [r for r in all_rows if r["code"] in chal_set_20]
            if base_top5 and chal_top5:
                top5_ret_delta.append(float(np.mean([r["ret20"] for r in chal_top5]) - np.mean([r["ret20"] for r in base_top5])))
            if base_top10 and chal_top10:
                top10_ret_delta.append(float(np.mean([r["ret20"] for r in chal_top10]) - np.mean([r["ret20"] for r in base_top10])))
            if base_top20 and chal_top20:
                top20_ret_delta.append(float(np.mean([r["ret20"] for r in chal_top20]) - np.mean([r["ret20"] for r in base_top20])))
            removed = [r for r in base if str(r["code"]) not in chal_set_20]
            bad_pick_removal += sum(1 for r in removed if float(r.get("ret20") or 0.0) <= 0.0 or abs(float(r.get("ret20") or 0.0)) <= 0.005)
            if len(chal) < len(base):
                divergence_reasons["challenger_gate_is_stricter"] += 1
            if base_set_20 != chal_set_20:
                divergence_reasons["top20_membership_changed"] += 1
            if len(chal) > 0 and len(base) > 0 and abs(len(chal) - len(base)) >= 3:
                divergence_reasons["coverage_changed_materially"] += 1
            monthly_rows.append(
                {
                    "asof_ymd": int(asof),
                    "side": direction,
                    "baseline_count": int(len(base)),
                    "challenger_count": int(len(chal)),
                    "baseline_top5": list(base_codes[:5]),
                    "challenger_top5": list(chal_codes[:5]),
                    "baseline_top10": list(base_codes[:10]),
                    "challenger_top10": list(chal_codes[:10]),
                    "baseline_top20": list(base_codes[:20]),
                    "challenger_top20": list(chal_codes[:20]),
                }
            )

    baseline_topk: dict[str, Any] = {}
    challenger_topk: dict[str, Any] = {}
    for k in (5, 10, 20):
        base_rows = _collect_topk_rows(selected_by_key=selected_baseline_by_key, row_map=row_map, k=k)
        chal_rows = _collect_topk_rows(selected_by_key=selected_challenger_by_key, row_map=row_map, k=k)
        baseline_topk[str(k)] = {
            "count": int(len(base_rows)),
            "overall": _summarize_rows(base_rows, all_rows=[row for rows in all_rows_by_key.values() for row in rows]),
        }
        challenger_topk[str(k)] = {
            "count": int(len(chal_rows)),
            "overall": _summarize_rows(chal_rows, all_rows=[row for rows in all_rows_by_key.values() for row in rows]),
        }

    return {
        "changed_top5_members_count": int(changed_top5),
        "changed_top10_members_count": int(changed_top10),
        "changed_top20_members_count": int(changed_top20),
        "changed_rank_count": int(changed_rank),
        "bad_pick_removal_count": int(bad_pick_removal),
        "selection_divergence_reason": ", ".join(f"{k}:{v}" for k, v in divergence_reasons.most_common()) or "no_meaningful_branching",
        "top5_uplift_mean": float(np.mean(top5_ret_delta)) if top5_ret_delta else None,
        "top10_uplift_mean": float(np.mean(top10_ret_delta)) if top10_ret_delta else None,
        "top20_uplift_mean": float(np.mean(top20_ret_delta)) if top20_ret_delta else None,
        "baseline_topk": baseline_topk,
        "challenger_topk": challenger_topk,
        "monthly_rows": monthly_rows,
    }


def _decision_from_results(baseline_eval: dict[str, Any], compare: dict[str, Any], family_results: dict[str, Any]) -> tuple[str, list[str]]:
    reasons: list[str] = []
    long_base = baseline_eval["by_side"].get("up", {})
    short_base = baseline_eval["by_side"].get("down", {})
    comp = compare
    top5_delta = comp.get("top5_uplift_mean")
    top10_delta = comp.get("top10_uplift_mean")
    family_best = max(
        (v.get("median_ret20") or -1e9) + (v.get("hit_rate") or 0.0)
        for v in family_results.values()
        if isinstance(v, dict)
    )
    if long_base.get("median_ret20") is not None and short_base.get("median_ret20") is not None:
        if float(long_base["median_ret20"]) > 0.0 and float(short_base["median_ret20"]) > 0.0:
            reasons.append("baseline_long_short_median_ret20_positive")
    if top5_delta is not None and top10_delta is not None and top5_delta >= 0.0 and top10_delta >= 0.0:
        reasons.append("coverage_tightening_did_not_hurt_topK_mean_return")
    if comp.get("changed_top5_members_count", 0) > 0:
        reasons.append("top5_branching_observed")
    if comp.get("bad_pick_removal_count", 0) > 0:
        reasons.append("bad_pick_removal_observed")
    if any((v.get("hit_rate") or 0.0) > 0.5 for v in family_results.values()):
        reasons.append("at_least_one_family_discriminates_above_coinflip")

    # Conservative default: keep only if branching is real and precision improves on both sides.
    long_hit = float(long_base.get("hit_rate") or 0.0)
    short_hit = float(short_base.get("hit_rate") or 0.0)
    long_med = float(long_base.get("median_ret20") or -1.0)
    short_med = float(short_base.get("median_ret20") or -1.0)
    if (
        comp.get("changed_top5_members_count", 0) > 0
        and comp.get("changed_top10_members_count", 0) > 0
        and long_hit >= 0.50
        and short_hit >= 0.50
        and long_med > 0.0
        and short_med > 0.0
    ):
        decision = "keep"
        reasons.append("precision_first_gate_branches_and_keeps_positive_median_ret20")
    elif comp.get("changed_top5_members_count", 0) == 0 and comp.get("changed_top10_members_count", 0) == 0:
        decision = "drop"
        reasons.append("no_material_branching")
    else:
        decision = "hold"
        reasons.append("sample_or_regime_stability_insufficient_for_keep")
    return decision, reasons


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_report_md(path: Path, payloads: dict[str, dict[str, Any]]) -> None:
    audit = payloads["audit"]
    baseline = payloads["baseline"]
    compare = payloads["compare"]
    decision = payloads["decision"]
    lines = [
        "# Entry Precision Audit",
        "",
        f"- session_id: `{audit['session_id']}`",
        f"- baseline_id: `{audit['baseline_id']}`",
        f"- challenger_id: `{audit['challenger_id']}`",
        f"- decision: `{decision['decision']}`",
        "",
        "## Current State",
        f"- confirmed: {', '.join(audit['confirmed'])}",
        f"- provisional: {', '.join(audit['provisional'])}",
        "",
        "## Baseline",
        f"- long hit rate: `{baseline['by_side']['up']['hit_rate']}`",
        f"- short hit rate: `{baseline['by_side']['down']['hit_rate']}`",
        f"- top5 changed members: `{compare['changed_top5_members_count']}`",
        f"- top10 changed members: `{compare['changed_top10_members_count']}`",
        "",
        "## Decision",
        f"- `{decision['decision']}`",
        f"- reasons: {', '.join(decision['decision_reasons'])}",
        "",
        "## Risks",
        f"- {', '.join(decision['remaining_risks'])}",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(args: argparse.Namespace) -> dict[str, Any]:
    db_path = _resolve_db_path(args.db_path or None)
    output_dir = Path(args.output_dir).expanduser().resolve() if args.output_dir else DEFAULT_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    session_id = f"entry-precision-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
    baseline_id = "current_rule_trade_gate_baseline"
    challenger_id = "precision_first_stricter_gate_v1"
    with duckdb.connect(str(db_path), read_only=True) as conn:
        months = _month_end_dates(conn, start_ymd=int(args.start_ymd), end_ymd=int(args.end_ymd))
        regime_map = _load_regime_map(conn, months)
        price_store = _load_price_store(conn)
        ma_store = _load_ma_store(conn)
        all_items_by_key: dict[tuple[int, str], list[dict[str, Any]]] = {}
        baseline_selected_by_key: dict[tuple[int, str], list[dict[str, Any]]] = {}
        challenger_selected_by_key: dict[tuple[int, str], list[dict[str, Any]]] = {}
        baseline_all_rows: list[dict[str, Any]] = []
        baseline_selected_rows: list[dict[str, Any]] = []
        challenger_selected_rows: list[dict[str, Any]] = []

        for idx, asof in enumerate(months, start=1):
            cache = _load_candidate_cache(conn, asof)
            for direction in ("up", "down"):
                raw_items = _copy_items(cache[("D", "latest", direction)])
                all_items_by_key[(int(asof), direction)] = raw_items
                baseline_decorated = _decorated_items(raw_items, direction=direction, thresholds=BASELINE_THRESHOLDS)
                challenger_decorated = _decorated_items(raw_items, direction=direction, thresholds=CHALLENGER_THRESHOLDS)
                baseline_selected = [dict(item) for item in baseline_decorated if bool(item.get("entryQualified"))]
                challenger_selected = [dict(item) for item in challenger_decorated if bool(item.get("entryQualified"))]
                rc._apply_trade_priority_scores(baseline_selected, direction=direction)
                rc._apply_trade_priority_scores(challenger_selected, direction=direction)
                baseline_selected.sort(key=rc._trade_priority_sort_key)
                challenger_selected.sort(key=rc._trade_priority_sort_key)
                baseline_selected_by_key[(int(asof), direction)] = baseline_selected
                challenger_selected_by_key[(int(asof), direction)] = challenger_selected
                baseline_rows, all_rows = _monthly_slice_metrics(
                    asof_ymd=int(asof),
                    direction=direction,
                    selected=baseline_selected,
                    all_items=baseline_decorated,
                    price_store=price_store,
                    ma_store=ma_store,
                )
                baseline_selected_rows.extend(baseline_rows)
                baseline_all_rows.extend(all_rows)
                challenger_rows, _ = _monthly_slice_metrics(
                    asof_ymd=int(asof),
                    direction=direction,
                    selected=challenger_selected,
                    all_items=challenger_decorated,
                    price_store=price_store,
                    ma_store=ma_store,
                )
                challenger_selected_rows.extend(challenger_rows)
            if idx % 12 == 0 or idx == len(months):
                print(f"[progress] {idx}/{len(months)} month-ends processed")

    all_row_map: dict[tuple[int, str, str], dict[str, Any]] = {
        (int(row["asof_ymd"]), str(row["side"]), str(row["code"])): row for row in baseline_all_rows
    }
    monthly_regime_rows = {}
    for asof in months:
        monthly_regime_rows[int(asof)] = regime_map.get(int(asof), "unknown")
    combined_all_rows = baseline_all_rows
    combined_selected_rows = baseline_selected_rows

    by_side = {
        "up": _summarize_rows(
            [r for r in combined_selected_rows if r["side"] == "up"],
            all_rows=[r for r in combined_all_rows if r["side"] == "up"],
        ),
        "down": _summarize_rows(
            [r for r in combined_selected_rows if r["side"] == "down"],
            all_rows=[r for r in combined_all_rows if r["side"] == "down"],
        ),
    }
    for side in ("up", "down"):
        all_count = sum(1 for r in combined_all_rows if r["side"] == side)
        sel_count = sum(1 for r in combined_selected_rows if r["side"] == side)
        by_side[side]["coverage"] = float(sel_count / all_count) if all_count else None
    overall = _summarize_rows(combined_selected_rows, all_rows=combined_all_rows)
    overall["coverage"] = float(len(combined_selected_rows) / len(combined_all_rows)) if combined_all_rows else None
    overall["by_month"] = _group_rows(combined_selected_rows, "asof_ymd")
    overall["by_regime"] = _group_rows(combined_selected_rows, "market_regime")

    topk_metrics: dict[str, Any] = {}
    for k in (5, 10, 20):
        top_rows = _collect_topk_rows(selected_by_key=baseline_selected_by_key, row_map=all_row_map, k=k)
        topk_metrics[str(k)] = {
            "count": int(len(top_rows)),
            "coverage": float(len(top_rows) / len(combined_all_rows)) if combined_all_rows else None,
            "overall": _summarize_rows(top_rows, all_rows=combined_all_rows),
            "by_side": {
                "up": _summarize_rows(
                    [r for r in top_rows if r["side"] == "up"],
                    all_rows=[r for r in combined_all_rows if r["side"] == "up"],
                ),
                "down": _summarize_rows(
                    [r for r in top_rows if r["side"] == "down"],
                    all_rows=[r for r in combined_all_rows if r["side"] == "down"],
                ),
            },
        }
    baseline_eval = {
        "session_id": session_id,
        "baseline_id": baseline_id,
        "challenger_id": challenger_id,
        "round_trip_cost": ROUND_TRIP_COST,
        "thresholds": {
            "baseline": BASELINE_THRESHOLDS.__dict__,
            "challenger": CHALLENGER_THRESHOLDS.__dict__,
        },
        "by_side": by_side,
        "topk": topk_metrics,
        "overall": overall,
        "monthly_regime": monthly_regime_rows,
        "sample_rows": combined_selected_rows[:20],
    }

    compare = _compare_baseline_vs_challenger(
        months=months,
        directions=["up", "down"],
        all_items_by_month=all_items_by_key,
        selected_baseline_by_key=baseline_selected_by_key,
        selected_challenger_by_key=challenger_selected_by_key,
        all_rows_by_key={
            key: _monthly_slice_metrics(
                asof_ymd=key[0],
                direction=key[1],
                selected=baseline_selected_by_key[key],
                all_items=all_items_by_key[key],
                price_store=price_store,
                ma_store=ma_store,
            )[1]
            for key in all_items_by_key
        },
    )

    error_buckets = _build_error_buckets(combined_all_rows, combined_selected_rows)
    feature_family_results = _family_results(combined_all_rows, top_k_values=(5, 10, 20))
    coverage_precision_curve = {
        "session_id": session_id,
        "baseline_id": baseline_id,
        "challenger_id": challenger_id,
        "round_trip_cost": ROUND_TRIP_COST,
        "curves": {
            "up": _coverage_curve(combined_all_rows, direction="up"),
            "down": _coverage_curve(combined_all_rows, direction="down"),
        },
    }

    decision, decision_reasons = _decision_from_results(baseline_eval, compare, feature_family_results)
    decision_payload = {
        "session_id": session_id,
        "baseline_id": baseline_id,
        "challenger_id": challenger_id,
        "decision": decision,
        "decision_reasons": decision_reasons,
        "confirmed": [
            "same_universe_fixed",
            "same_period_fixed",
            "same_top_k_fixed",
            "long_short_separated",
            "json_artifacts_are_authoritative",
        ],
        "provisional": [
            "early_confirmation_definition_is_proxy_based",
            "false_neutral_definition_is_consensus_proxy_based",
            "challenger_thresholds_are_a_single_stricter_gate_axis",
        ],
        "remaining_risks": [
            "monthly_oos_sample_is_small_for_regime_specific_conclusions",
            "bucket_reason_codes_are_heuristic_proxies_for_failure_analysis",
            "coverage_curve_sweeps_entry_score_only_and_does_not_retrain_the_gate",
            "no_model_retraining_was_performed",
        ],
    }

    audit_payload = {
        "schema_version": "tradex_entry_precision_audit_v1",
        "session_id": session_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "db_path": str(db_path),
        "output_dir": str(output_dir),
        "baseline_id": baseline_id,
        "challenger_id": challenger_id,
        "round_trip_cost": ROUND_TRIP_COST,
        "evaluation_contract": {
            "same_universe": True,
            "same_period": True,
            "same_top_k": True,
            "same_regime": True,
            "same_cost": True,
            "same_artifact_detail_level": True,
            "long_short_separated": True,
            "one_axis_only": True,
            "no_meemee_ui_change": True,
            "no_publish_or_autowire_change": True,
        },
        "confirmed": [
            "buy_sell_neutral_gate_exists_in_rankings_cache",
            "entry_score_is_distinct_from_entry_qualified",
            "trade_priority_is_stage_b_after_gate",
            "month_end_rolling_oos_is_fixed",
            "json_is_the_authoritative_artifact_layer",
        ],
        "provisional": [
            "exact_false_neutral_definition_is_a_research_proxy",
            "early_confirmation_and_mae_family_definitions_are_study heuristics",
            "challenger_is_stricter_but_not_retrained",
        ],
        "source_artifacts": [
            str(REPO_ROOT / "artifacts" / "research_inventory" / "buy_judgment_effectiveness_audit.json"),
            str(REPO_ROOT / "artifacts" / "research_inventory" / "buy_judgment_revision_r4_reclaim_quality_gate.json"),
            str(REPO_ROOT / "app" / "backend" / "services" / "ml" / "rankings_cache.py"),
        ],
        "audit_scope": {
            "timeframe": "D/latest",
            "directions": ["up", "down"],
            "months": len(months),
            "start_ymd": int(args.start_ymd),
            "end_ymd": int(args.end_ymd),
            "candidate_universe": int(sum(len(v) for v in all_items_by_key.values())),
            "regimes_seen": sorted(set(monthly_regime_rows.values())),
        },
        "stage_a_generation_path": {
            "module": "app.backend.services.ml.rankings_cache",
            "function": "_decorate_rule_items_with_entry_gate",
            "entry_qualified_field": "entryQualified",
            "entry_score_field": "entryScore",
            "neutral_behavior": "item_not_entryQualified is treated as neutral for stage-A coverage accounting",
        },
        "stage_b_generation_path": {
            "module": "app.backend.services.ml.rankings_cache",
            "function": "_apply_trade_priority_scores",
            "sort_key": "_trade_priority_sort_key",
            "long_short_separate": True,
        },
        "baseline_gate_thresholds": BASELINE_THRESHOLDS.__dict__,
        "challenger_gate_thresholds": CHALLENGER_THRESHOLDS.__dict__,
        "baseline_eval": baseline_eval,
        "error_buckets": error_buckets,
        "feature_family_results": feature_family_results,
        "coverage_precision_curve": coverage_precision_curve,
        "compare": compare,
    }

    _write_json(output_dir / "entry_precision_audit.json", audit_payload)
    _write_json(output_dir / "entry_precision_baseline_eval.json", baseline_eval)
    _write_json(output_dir / "entry_precision_error_buckets.json", {
        "session_id": session_id,
        "baseline_id": baseline_id,
        "challenger_id": challenger_id,
        "round_trip_cost": ROUND_TRIP_COST,
        "buckets": error_buckets,
    })
    _write_json(output_dir / "entry_precision_feature_family_results.json", {
        "session_id": session_id,
        "baseline_id": baseline_id,
        "challenger_id": challenger_id,
        "families": feature_family_results,
    })
    _write_json(output_dir / "entry_precision_coverage_precision_curve.json", coverage_precision_curve)
    _write_json(output_dir / "entry_precision_champion_vs_challenger.json", {
        "session_id": session_id,
        "baseline_id": baseline_id,
        "challenger_id": challenger_id,
        "baseline_eval": baseline_eval,
        "compare": compare,
    })
    _write_json(output_dir / "entry_precision_decision.json", decision_payload)
    _write_report_md(
        output_dir / "entry_precision_report.md",
        {
            "audit": audit_payload,
            "baseline": baseline_eval,
            "compare": compare,
            "decision": decision_payload,
        },
    )

    return {
        "ok": True,
        "session_id": session_id,
        "output_dir": str(output_dir),
        "db_path": str(db_path),
        "decision": decision,
        "baseline_id": baseline_id,
        "challenger_id": challenger_id,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Entry precision audit for TRADEX neutral suppression / readiness separation.")
    parser.add_argument("--db-path", default="", help="Path to authoritative snapshot DB")
    parser.add_argument("--start-ymd", type=int, default=20230101)
    parser.add_argument("--end-ymd", type=int, default=20260226)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args()
    result = run(args)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
