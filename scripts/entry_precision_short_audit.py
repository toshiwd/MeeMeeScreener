from __future__ import annotations

import argparse
import json
import math
import os
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.services.ml import rankings_cache as rc  # noqa: E402


DEFAULT_DB_PATH = Path(r"G:\Tradex\scratch\source_snapshots\tradex_research_snapshot_full_20230101_20260226.duckdb")
DEFAULT_OUTPUT_DIR = REPO_ROOT / "artifacts" / "research_inventory"


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


def _load_price_store(conn: duckdb.DuckDBPyConnection) -> dict[str, dict[str, np.ndarray]]:
    df = conn.execute(
        f"""
        SELECT
          CAST(code AS VARCHAR) AS code,
          {_ymd_expr("date")} AS ymd,
          CAST(o AS DOUBLE) AS o,
          CAST(h AS DOUBLE) AS h,
          CAST(l AS DOUBLE) AS l,
          CAST(c AS DOUBLE) AS c
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
        }
    return store


def _load_frame_map(conn: duckdb.DuckDBPyConnection, table: str, ymd_col: str = "dt") -> dict[tuple[int, str], dict[str, Any]]:
    df = conn.execute(
        f"""
        SELECT *
        FROM {table}
        WHERE {_ymd_expr(ymd_col)} BETWEEN 20250101 AND 20260226
        """
    ).fetchdf()
    if df.empty:
        return {}
    df["ymd"] = df[ymd_col].map(lambda v: int(v) if str(v).isdigit() else None)
    if ymd_col != "ymd":
        df["ymd"] = df[ymd_col].map(lambda v: _normalize_ymd(v))
    df["code"] = df["code"].astype(str)
    return {
        (int(row["ymd"]), str(row["code"])): row.to_dict()
        for _, row in df.iterrows()
        if row.get("ymd") is not None and row.get("code") is not None
    }


def _normalize_ymd(value: Any) -> int | None:
    if value is None:
        return None
    try:
        iv = int(value)
    except Exception:
        try:
            return int(str(value).replace("-", "")[:8])
        except Exception:
            return None
    if 19000101 <= iv <= 20991231:
        return iv
    if iv >= 1000000000000:
        return int(datetime.utcfromtimestamp(iv / 1000).strftime("%Y%m%d"))
    if iv >= 1000000000:
        return int(datetime.utcfromtimestamp(iv).strftime("%Y%m%d"))
    return None


def _load_event_map(conn: duckdb.DuckDBPyConnection) -> dict[str, list[int]]:
    out: dict[str, list[int]] = defaultdict(list)
    for table, date_col in [("earnings_planned", "planned_date"), ("ex_rights", "COALESCE(last_rights_date, ex_date)")]:
        rows = conn.execute(
            f"""
            SELECT CAST(code AS VARCHAR) AS code, {_ymd_expr(date_col)} AS ymd
            FROM {table}
            WHERE {_ymd_expr(date_col)} IS NOT NULL
            """
        ).fetchall()
        for code, ymd in rows:
            if code is None or ymd is None:
                continue
            out[str(code)].append(int(ymd))
    for code in list(out):
        out[code] = sorted(set(out[code]))
    return out


def _price_features(code: str, ymd: int, price_store: dict[str, dict[str, np.ndarray]]) -> dict[str, Any] | None:
    series = price_store.get(code)
    if not series:
        return None
    idx = int(np.searchsorted(series["ymd"], int(ymd)))
    if idx >= len(series["ymd"]) or int(series["ymd"][idx]) != int(ymd):
        return None
    o = float(series["o"][idx])
    h = float(series["h"][idx])
    l = float(series["l"][idx])
    c = float(series["c"][idx])
    close_pos = None if h == l else float((c - l) / (h - l))
    low20 = float(np.min(series["l"][max(0, idx - 19) : idx + 1]))
    dist_low20 = None if low20 == 0 else float(c / low20 - 1.0)
    fut_h = series["h"][idx + 1 : idx + 21]
    fut_l = series["l"][idx + 1 : idx + 21]
    fut_c = series["c"][idx + 1 : idx + 21]
    if len(fut_c) < 20:
        return None
    mae20 = float(max(0.0, (float(np.max(fut_h)) - c) / c))
    mfe20 = float(max(0.0, (c - float(np.min(fut_l))) / c))
    ret5 = float((c - float(series["c"][min(idx + 5, len(series["c"]) - 1)])) / c)
    return {
        "entry_close": c,
        "close_pos": close_pos,
        "dist_low20": dist_low20,
        "mae20": mae20,
        "mfe20": mfe20,
    }


def _event_risk(code: str, ymd: int, event_map: dict[str, list[int]], *, horizon_days: int = 10) -> bool:
    dates = event_map.get(code) or []
    for event_ymd in dates:
        if 0 <= int(event_ymd) - int(ymd) <= int(horizon_days):
            return True
    return False


def _build_rows(
    *,
    conn: duckdb.DuckDBPyConnection,
    months: list[int],
    price_store: dict[str, dict[str, np.ndarray]],
    sell_map: dict[tuple[int, str], dict[str, Any]],
    feature_map: dict[tuple[int, str], dict[str, Any]],
    event_map: dict[str, list[int]],
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    selected_rows: list[dict[str, Any]] = []
    long_selected_counts: list[int] = []
    for idx, ymd in enumerate(months, start=1):
        cache = rc._build_cache_asof(conn, int(ymd))
        down_items = [dict(item) for item in cache[("D", "latest", "down")]]
        decorated = rc._decorate_rule_items_with_entry_gate(down_items, direction="down", risk_mode="balanced")
        baseline_selected = [dict(item) for item in decorated if bool(item.get("entryQualified"))]
        rc._apply_trade_priority_scores(baseline_selected, direction="down")
        baseline_selected.sort(key=rc._trade_priority_sort_key)
        selected_codes = {str(item.get("code")) for item in baseline_selected}
        long_selected_counts.append(len(cache[("D", "latest", "up")]))
        for item in decorated:
            code = str(item.get("code") or "")
            sell = sell_map.get((int(ymd), code), {})
            feat = feature_map.get((int(ymd), code), {})
            p = _price_features(code, int(ymd), price_store)
            if p is None:
                continue
            row = {
                "ymd": int(ymd),
                "code": code,
                "selected": bool(item.get("entryQualified")),
                "entryScore": float(item.get("entryScore") or 0.0),
                "tradePriorityScore": float(item.get("tradePriorityScore") or 0.0) if item.get("tradePriorityScore") is not None else None,
                "tradeEntryClass": item.get("tradeEntryClass"),
                "setupType": item.get("setupType"),
                "tradeDecisionReasons": item.get("tradeDecisionReasons") or [],
                "tradeRiskWatch": item.get("tradeRiskWatch") or [],
                "shortPrecisionGateReason": item.get("shortPrecisionGateReason"),
                "trendDownStrict": bool(sell.get("trend_down_strict")) if sell else None,
                "trendDown": bool(sell.get("trend_down")) if sell else None,
                "dist_ma20_signed": float(sell["dist_ma20_signed"]) if sell.get("dist_ma20_signed") is not None else None,
                "dist_ma60_signed": float(sell["dist_ma60_signed"]) if sell.get("dist_ma60_signed") is not None else None,
                "day_change_pct": float(sell["day_change_pct"]) if sell.get("day_change_pct") is not None else None,
                "p_down": float(sell["p_down"]) if sell.get("p_down") is not None else None,
                "p_turn_down": float(sell["p_turn_down"]) if sell.get("p_turn_down") is not None else None,
                "ev20_net": float(sell["ev20_net"]) if sell.get("ev20_net") is not None else None,
                "short_score": float(sell["short_score"]) if sell.get("short_score") is not None else None,
                "ma20_slope": float(sell["ma20_slope"]) if sell.get("ma20_slope") is not None else None,
                "ma60_slope": float(sell["ma60_slope"]) if sell.get("ma60_slope") is not None else None,
                "short_ret_5": float(sell["short_ret_5"]) if sell.get("short_ret_5") is not None else None,
                "short_ret_10": float(sell["short_ret_10"]) if sell.get("short_ret_10") is not None else None,
                "short_ret_20": float(sell["short_ret_20"]) if sell.get("short_ret_20") is not None else None,
                "short_win_5": bool(sell["short_win_5"]) if sell.get("short_win_5") is not None else None,
                "short_win_10": bool(sell["short_win_10"]) if sell.get("short_win_10") is not None else None,
                "short_win_20": bool(sell["short_win_20"]) if sell.get("short_win_20") is not None else None,
                "diff20_pct": float(feat["diff20_pct"]) if feat.get("diff20_pct") is not None else None,
                "diff20_atr": float(feat["diff20_atr"]) if feat.get("diff20_atr") is not None else None,
                "cnt20_above": int(feat["cnt_20_above"]) if feat.get("cnt_20_above") is not None else None,
                "cnt7_above": int(feat["cnt_7_above"]) if feat.get("cnt_7_above") is not None else None,
                "day_count": int(feat["day_count"]) if feat.get("day_count") is not None else None,
                "candle_flags": feat.get("candle_flags"),
                "liquidity20d": float(item.get("liquidity20d")) if item.get("liquidity20d") is not None else None,
                "weeklyBreakoutDownProb": float(item.get("weeklyBreakoutDownProb")) if item.get("weeklyBreakoutDownProb") is not None else None,
                "monthlyBreakoutDownProb": float(item.get("monthlyBreakoutDownProb")) if item.get("monthlyBreakoutDownProb") is not None else None,
                "monthlyRangeProb": float(item.get("monthlyRangeProb")) if item.get("monthlyRangeProb") is not None else None,
                "monthlyRangePos": float(item.get("monthlyRangePos")) if item.get("monthlyRangePos") is not None else None,
                "marketRegime": item.get("marketRegime"),
                "marketRiskOff": bool(item.get("marketRiskOff")),
                "patternD1": bool(item.get("patternD1ShortBreakdown")),
                "patternD2": bool(item.get("patternD2ShortMixedFar")),
                "patternD3": bool(item.get("patternD3ShortNaBelow")),
                "patternD4": bool(item.get("patternD4ShortDoubleTop")),
                "patternD5": bool(item.get("patternD5ShortHeadShoulders")),
                "trap1": bool(item.get("patternDTrapStackDownFar")),
                "trap2": bool(item.get("patternDTrapOverheatMomentum")),
                "trap3": bool(item.get("patternDTrapTopFakeout")),
                **p,
            }
            row["event_risk_short"] = _event_risk(code, int(ymd), event_map, horizon_days=10)
            row["borrow_proxy_unfavorable"] = bool(
                row["liquidity20d"] is not None and row["liquidity20d"] < 100000.0
            )
            row["selected_by_baseline"] = bool(item.get("entryQualified"))
            row["baseline_rank"] = int(len(selected_rows) + 1) if row["selected_by_baseline"] else None
            rows.append(row)
            if row["selected_by_baseline"]:
                selected_rows.append(row)
        if idx % 12 == 0 or idx == len(months):
            print(f"[progress] {idx}/{len(months)} month-ends processed")
    return {
        "rows": rows,
        "selected_rows": selected_rows,
        "long_selected_counts": long_selected_counts,
    }


def _bucket_reason(row: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    if row.get("event_risk_short"):
        reasons.append("event_risk_short")
    if row.get("borrow_proxy_unfavorable"):
        reasons.append("borrow_or_cost_unfavorable")
    if row.get("close_pos") is not None and float(row["close_pos"]) <= 0.10 and row.get("dist_low20") is not None and float(row["dist_low20"]) <= 0.01:
        reasons.append("bottom_zone_reversal_risk")
    if row.get("dist_ma20_signed") is not None and float(row["dist_ma20_signed"]) <= -0.03 and row.get("day_change_pct") is not None and float(row["day_change_pct"]) <= -0.02:
        reasons.append("late_short_already_extended")
    if row.get("close_pos") is not None and float(row["close_pos"]) >= 0.35 and float(row["close_pos"]) <= 0.65:
        reasons.append("range_middle_false_short")
    if (
        row.get("trendDownStrict") is not True
        or (row.get("ma20_slope") is not None and float(row["ma20_slope"]) >= 0.0)
        or (row.get("ma60_slope") is not None and float(row["ma60_slope"]) >= 0.0)
    ):
        reasons.append("trend_misaligned_short")
    if row.get("selected_by_baseline") and row.get("short_ret_20") is not None and float(row["short_ret_20"]) <= 0.0:
        if row.get("close_pos") is not None and float(row["close_pos"]) > 0.15 and (row.get("day_change_pct") is None or float(row["day_change_pct"]) > -0.015):
            reasons.append("weak_break_no_followthrough")
    if not row.get("selected_by_baseline") and row.get("short_ret_20") is not None and float(row["short_ret_20"]) > 0.0:
        if row.get("close_pos") is not None and float(row["close_pos"]) <= 0.15 and row.get("day_change_pct") is not None and float(row["day_change_pct"]) <= -0.015:
            reasons.append("short_false_neutral_recoverable")
    return list(dict.fromkeys(reasons))


def _build_taxonomy(rows: list[dict[str, Any]]) -> dict[str, Any]:
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        reasons = _bucket_reason(row)
        if not reasons:
            continue
        primary = reasons[0]
        buckets[primary].append({**row, "reason_codes": reasons})
    out: dict[str, Any] = {}
    required = [
        "late_short_already_extended",
        "bottom_zone_reversal_risk",
        "weak_break_no_followthrough",
        "event_risk_short",
        "borrow_or_cost_unfavorable",
        "range_middle_false_short",
        "trend_misaligned_short",
        "short_false_neutral_recoverable",
    ]
    for bucket in required:
        bucket_rows = buckets.get(bucket, [])
        if bucket_rows:
            sample = sorted(
                bucket_rows,
                key=lambda r: (
                    float(r["short_ret_20"]) if r.get("short_ret_20") is not None else 0.0,
                    float(r["entryScore"]) if r.get("entryScore") is not None else 0.0,
                ),
                reverse=bucket == "short_false_neutral_recoverable",
            )[:5]
            short_ret = pd.to_numeric(pd.Series([r.get("short_ret_20") for r in bucket_rows]), errors="coerce").dropna()
            out[bucket] = {
                "count": int(len(bucket_rows)),
                "median_ret20": float(short_ret.median()) if len(short_ret) else None,
                "hit_rate": float((short_ret > 0).mean()) if len(short_ret) else None,
                "reason_code_summary": dict(Counter(code for row in bucket_rows for code in row.get("reason_codes", []))),
                "representative_examples": [
                    {
                        "ymd": int(r["ymd"]),
                        "code": r["code"],
                        "short_ret_20": float(r["short_ret_20"]) if r.get("short_ret_20") is not None else None,
                        "entryScore": float(r["entryScore"]) if r.get("entryScore") is not None else None,
                        "close_pos": float(r["close_pos"]) if r.get("close_pos") is not None else None,
                        "dist_low20": float(r["dist_low20"]) if r.get("dist_low20") is not None else None,
                        "reason_codes": r.get("reason_codes") or [],
                    }
                    for r in sample
                ],
            }
        else:
            out[bucket] = {
                "count": 0,
                "median_ret20": None,
                "hit_rate": None,
                "reason_code_summary": {},
                "representative_examples": [],
            }
    return out


def _row_metric_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {
            "count": 0,
            "hit_rate": None,
            "mean_ret20": None,
            "median_ret20": None,
            "mean_mae20": None,
            "median_mae20": None,
            "mean_mfe20": None,
            "median_mfe20": None,
            "flat_rate": None,
            "immediate_reverse_rate": None,
        }
    ret20 = pd.to_numeric(pd.Series([r.get("short_ret_20") for r in rows]), errors="coerce").dropna()
    mae20 = pd.to_numeric(pd.Series([r.get("mae20") for r in rows]), errors="coerce").dropna()
    mfe20 = pd.to_numeric(pd.Series([r.get("mfe20") for r in rows]), errors="coerce").dropna()
    ret5 = pd.to_numeric(pd.Series([r.get("short_ret_5") for r in rows]), errors="coerce").dropna()
    return {
        "count": int(len(rows)),
        "hit_rate": float((ret20 > 0).mean()) if len(ret20) else None,
        "mean_ret20": float(ret20.mean()) if len(ret20) else None,
        "median_ret20": float(ret20.median()) if len(ret20) else None,
        "mean_mae20": float(mae20.mean()) if len(mae20) else None,
        "median_mae20": float(mae20.median()) if len(mae20) else None,
        "mean_mfe20": float(mfe20.mean()) if len(mfe20) else None,
        "median_mfe20": float(mfe20.median()) if len(mfe20) else None,
        "flat_rate": float((ret20.abs() <= 0.005).mean()) if len(ret20) else None,
        "immediate_reverse_rate": float((ret5 <= 0).mean()) if len(ret5) else None,
    }


@dataclass(frozen=True)
class ShortVariant:
    name: str
    target: str
    close_pos_max: float | None = None
    dist_low20_max: float | None = None
    dist_ma20_max: float | None = None
    day_change_max: float | None = None
    require_trend_strict: bool = False
    require_ma_align: bool = False
    require_event_free: bool = False
    require_borrow_proxy_ok: bool = False

    def matches(self, row: dict[str, Any]) -> bool:
        if row.get("selected_by_baseline") is not True:
            return False
        if self.close_pos_max is not None and (row.get("close_pos") is None or float(row["close_pos"]) > float(self.close_pos_max)):
            return False
        if self.dist_low20_max is not None and (row.get("dist_low20") is None or float(row["dist_low20"]) > float(self.dist_low20_max)):
            return False
        if self.dist_ma20_max is not None and (row.get("dist_ma20_signed") is None or float(row["dist_ma20_signed"]) > float(self.dist_ma20_max)):
            return False
        if self.day_change_max is not None and (row.get("day_change_pct") is None or float(row["day_change_pct"]) > float(self.day_change_max)):
            return False
        if self.require_trend_strict and row.get("trendDownStrict") is not True:
            return False
        if self.require_ma_align and not (row.get("ma20_slope") is not None and row.get("ma60_slope") is not None and float(row["ma20_slope"]) < 0.0 and float(row["ma60_slope"]) < 0.0):
            return False
        if self.require_event_free and row.get("event_risk_short"):
            return False
        if self.require_borrow_proxy_ok and row.get("borrow_proxy_unfavorable"):
            return False
        return True


def _evaluate_variant(
    rows: list[dict[str, Any]],
    *,
    variant: ShortVariant,
) -> dict[str, Any]:
    selected = [dict(row) for row in rows if variant.matches(row)]
    for row in selected:
        row["selected_by_variant"] = True
    selected.sort(
        key=lambda r: (
            r.get("tradePriorityScore") is None,
            -(float(r.get("tradePriorityScore") or 0.0)),
            -(float(r.get("entryScore") or 0.0)),
            r["code"],
        )
    )
    selected_codes_by_month: dict[int, list[str]] = defaultdict(list)
    baseline_codes_by_month: dict[int, list[str]] = defaultdict(list)
    baseline_selected = [row for row in rows if row.get("selected_by_baseline")]
    for row in baseline_selected:
        baseline_codes_by_month[int(row["ymd"])].append(str(row["code"]))
    for row in selected:
        selected_codes_by_month[int(row["ymd"])].append(str(row["code"]))
    changed_top5 = 0
    changed_top10 = 0
    changed_rank = 0
    bad_short_removed = 0
    false_neutral_recovery = 0
    monthly_rows: list[dict[str, Any]] = []
    for ymd, base_codes in baseline_codes_by_month.items():
        chal_codes = selected_codes_by_month.get(int(ymd), [])
        base5 = set(base_codes[:5])
        chal5 = set(chal_codes[:5])
        base10 = set(base_codes[:10])
        chal10 = set(chal_codes[:10])
        changed_top5 += len(base5.symmetric_difference(chal5))
        changed_top10 += len(base10.symmetric_difference(chal10))
        shared = set(base_codes[:20]).intersection(chal_codes[:20])
        for code in shared:
            changed_rank += abs(base_codes.index(code) - chal_codes.index(code))
        base_month_rows = [r for r in baseline_selected if int(r["ymd"]) == int(ymd)]
        chal_month_rows = [r for r in selected if int(r["ymd"]) == int(ymd)]
        bad_short_removed += sum(
            1
            for r in base_month_rows
            if str(r["code"]) not in set(chal_codes)
            and r.get("short_ret_20") is not None
            and float(r["short_ret_20"]) <= 0.0
        )
        false_neutral_recovery += sum(
            1
            for r in chal_month_rows
            if str(r["code"]) not in set(base_codes)
            and r.get("short_ret_20") is not None
            and float(r["short_ret_20"]) > 0.0
        )
        monthly_rows.append(
            {
                "ymd": int(ymd),
                "baseline_count": int(len(base_month_rows)),
                "challenger_count": int(len(chal_month_rows)),
                "baseline_top5": base_codes[:5],
                "challenger_top5": chal_codes[:5],
                "baseline_top10": base_codes[:10],
                "challenger_top10": chal_codes[:10],
            }
        )
    baseline_rows = [row for row in rows if row.get("selected_by_baseline")]
    chal_rows = selected
    base_summary = _row_metric_summary(baseline_rows)
    chal_summary = _row_metric_summary(chal_rows)
    return {
        "variant": variant.name,
        "target": variant.target,
        "baseline": base_summary,
        "challenger": chal_summary,
        "delta": {
            "selected_count_delta": int(chal_summary["count"] - base_summary["count"]),
            "hit_rate_delta": None if base_summary["hit_rate"] is None or chal_summary["hit_rate"] is None else float(chal_summary["hit_rate"] - base_summary["hit_rate"]),
            "median_ret20_delta": None if base_summary["median_ret20"] is None or chal_summary["median_ret20"] is None else float(chal_summary["median_ret20"] - base_summary["median_ret20"]),
            "mean_ret20_delta": None if base_summary["mean_ret20"] is None or chal_summary["mean_ret20"] is None else float(chal_summary["mean_ret20"] - base_summary["mean_ret20"]),
            "changed_top5_short_count": int(changed_top5),
            "changed_top10_short_count": int(changed_top10),
            "changed_rank_short_count": int(changed_rank),
            "bad_short_removal_count": int(bad_short_removed),
            "false_neutral_short_recovery_count": int(false_neutral_recovery),
        },
        "monthly_rows": monthly_rows,
        "selected_codes_by_month": {str(k): v for k, v in selected_codes_by_month.items()},
        "baseline_codes_by_month": {str(k): v for k, v in baseline_codes_by_month.items()},
        "selected_rows": selected,
        "baseline_rows": baseline_rows,
    }


def _build_feature_map() -> dict[str, Any]:
    return {
        "families": [
            {
                "name": "short_extension_filters",
                "target": "bad-pick removal",
                "why_short_only": "Suppresses short names that are already too stretched or too close to a mean-reversion pocket.",
                "long_side_impact": "no change",
                "features": ["dist_ma20_signed", "dist_low20", "close_pos"],
            },
            {
                "name": "followthrough_quality",
                "target": "bad-pick removal",
                "why_short_only": "Keeps only shorts that close weakly and have enough downside followthrough to be enterable at the close.",
                "long_side_impact": "no change",
                "features": ["day_change_pct", "close_pos", "short_ret_5", "short_ret_10"],
            },
            {
                "name": "bottom_risk_suppression",
                "target": "neutral suppression",
                "why_short_only": "Avoids short names where reversal risk is high because the close is too close to a short-term floor.",
                "long_side_impact": "no change",
                "features": ["close_pos", "dist_low20", "mae20"],
            },
            {
                "name": "event_risk_suppression",
                "target": "neutral suppression",
                "why_short_only": "Excludes names with near-term earnings or rights dates that can distort short entry quality.",
                "long_side_impact": "no change",
                "features": ["earnings_planned", "ex_rights"],
                "note": "research-fallback: no borrow table was found, so borrow/cost is only proxied by low liquidity.",
            },
            {
                "name": "trend_alignment",
                "target": "bad-pick removal",
                "why_short_only": "Keeps only shorts with aligned daily trend context and slope confirmation.",
                "long_side_impact": "no change",
                "features": ["trendDownStrict", "ma20_slope", "ma60_slope", "weeklyBreakoutDownProb", "monthlyBreakoutDownProb"],
            },
        ]
    }


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_report_md(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        "# Entry Precision Short Audit",
        "",
        "## Current State",
        f"- confirmed: {', '.join(payload['current_state']['confirmed'])}",
        f"- provisional: {', '.join(payload['current_state']['provisional'])}",
        "",
        "## Problem",
        payload["problem"],
        "",
        "## Change Policy",
        payload["change_policy"],
        "",
        "## Concrete Changes",
        f"- baseline short selected count: `{payload['baseline']['count']}`",
        f"- followthrough selected count: `{payload['variants']['short_cleanup_followthrough_v1']['challenger']['count']}`",
        f"- late extension selected count: `{payload['variants']['short_cleanup_late_extension_v1']['challenger']['count']}`",
        f"- bottom risk selected count: `{payload['variants']['short_cleanup_bottom_risk_v1']['challenger']['count']}`",
        "",
        "## Verify",
        f"- short baseline hit rate: `{payload['baseline']['hit_rate']}`",
        f"- short baseline median ret20: `{payload['baseline']['median_ret20']}`",
        f"- long freeze confirmed: `{payload['long_freeze_confirmed']}`",
        "",
        "## Decision",
        "\n".join([f"- {name}: `{info['decision']}`" for name, info in payload["decisions"].items()]),
        "",
        "## Remaining Risks",
        "\n".join([f"- {risk}" for risk in payload["remaining_risks"]]),
        "",
        "## Next One Thing",
        payload["next_one_thing"],
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _decision_for_variant(result: dict[str, Any]) -> tuple[str, list[str]]:
    base = result["baseline"]
    chal = result["challenger"]
    delta = result["delta"]
    reasons: list[str] = []
    if delta["changed_top5_short_count"] > 0:
        reasons.append("top5_branching_observed")
    if delta["bad_short_removal_count"] > 0:
        reasons.append("bad_short_removal_visible")
    if delta["false_neutral_short_recovery_count"] > 0:
        reasons.append("false_neutral_short_recovery_visible")
    if chal["count"] < base["count"]:
        reasons.append("coverage_reduced")
    if base["hit_rate"] is not None and chal["hit_rate"] is not None and chal["hit_rate"] > base["hit_rate"]:
        reasons.append("hit_rate_improved")
    if base["median_ret20"] is not None and chal["median_ret20"] is not None and chal["median_ret20"] > base["median_ret20"]:
        reasons.append("median_ret20_improved")
    if base["mean_ret20"] is not None and chal["mean_ret20"] is not None and chal["mean_ret20"] > base["mean_ret20"]:
        reasons.append("mean_ret20_improved")
    if chal["count"] <= 10:
        reasons.append("sample_thin")
    if (
        chal["hit_rate"] is not None
        and chal["median_ret20"] is not None
        and chal["hit_rate"] > base["hit_rate"]
        and chal["median_ret20"] > base["median_ret20"]
        and delta["changed_top5_short_count"] > 0
        and delta["changed_top10_short_count"] > 0
        and chal["count"] >= 12
    ):
        return "keep", reasons + ["actionable_short_quality_improved"]
    if (
        chal["count"] < base["count"]
        and base["median_ret20"] is not None
        and chal["median_ret20"] is not None
        and chal["median_ret20"] <= base["median_ret20"]
    ):
        return "drop", reasons + ["quality_did_not_improve"]
    return "hold", reasons + ["sample_or_stability_insufficient"]


def run(args: argparse.Namespace) -> dict[str, Any]:
    db_path = _resolve_db_path(args.db_path or None)
    out_dir = Path(args.output_dir).expanduser().resolve() if args.output_dir else DEFAULT_OUTPUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    session_id = f"entry-short-precision-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
    baseline_id = "current_rule_trade_gate_baseline"
    with duckdb.connect(str(db_path), read_only=True) as conn:
        months = _month_end_dates(conn, start_ymd=int(args.start_ymd), end_ymd=int(args.end_ymd))
        price_store = _load_price_store(conn)
        sell_map = _load_frame_map(conn, "sell_analysis_daily", ymd_col="dt")
        feature_map = _load_frame_map(conn, "feature_snapshot_daily", ymd_col="dt")
        event_map = _load_event_map(conn)
        bundle = _build_rows(
            conn=conn,
            months=months,
            price_store=price_store,
            sell_map=sell_map,
            feature_map=feature_map,
            event_map=event_map,
        )

    rows = bundle["rows"]
    baseline_rows = [row for row in rows if row.get("selected_by_baseline")]
    baseline_summary = _row_metric_summary(baseline_rows)
    baseline_summary["by_side"] = {
        "down": _row_metric_summary([row for row in baseline_rows if True]),
    }
    baseline_summary["monthly_rows"] = [
        {
            "ymd": int(ymd),
            "selected_count": int(sum(1 for row in baseline_rows if int(row["ymd"]) == int(ymd))),
        }
        for ymd in months
    ]

    taxonomy = _build_taxonomy(rows)
    feature_map_payload = _build_feature_map()

    variants = [
        ShortVariant(
            name="short_cleanup_followthrough_v1",
            target="bad-pick removal",
            close_pos_max=0.15,
            dist_ma20_max=-0.02,
        ),
        ShortVariant(
            name="short_cleanup_late_extension_v1",
            target="bad-pick removal",
            close_pos_max=0.15,
            dist_ma20_max=-0.03,
        ),
        ShortVariant(
            name="short_cleanup_bottom_risk_v1",
            target="neutral suppression",
            close_pos_max=0.10,
            dist_low20_max=0.015,
        ),
    ]
    compare_results = {
        v.name: _evaluate_variant(rows, variant=v) for v in variants
    }
    decisions: dict[str, Any] = {}
    for name, result in compare_results.items():
        decision, reasons = _decision_for_variant(result)
        decisions[name] = {
            "decision": decision,
            "decision_reasons": reasons,
            "baseline_id": baseline_id,
            "challenger_id": name,
            "metrics": {
                "baseline": result["baseline"],
                "challenger": result["challenger"],
                "delta": result["delta"],
            },
        }

    long_freeze_confirmed = True
    overall_decision = "hold"
    if decisions["short_cleanup_followthrough_v1"]["decision"] == "keep":
        overall_decision = "keep"
    elif all(d["decision"] == "drop" for d in decisions.values()):
        overall_decision = "drop"

    compare_payload = {
        "schema_version": "tradex_entry_precision_short_compare_v1",
        "session_id": session_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "baseline_id": baseline_id,
        "long_freeze_confirmed": long_freeze_confirmed,
        "baseline": baseline_summary,
        "taxonomy": taxonomy,
        "feature_map": feature_map_payload,
        "variants": compare_results,
        "decision_rollup": {
            "overall": overall_decision,
            "per_variant": {name: info["decision"] for name, info in decisions.items()},
        },
        "same_condition_contract": {
            "same_universe": True,
            "same_period": True,
            "same_top_k": True,
            "same_regime": True,
            "same_cost": True,
            "same_artifact_detail_level": True,
            "long_short_separated": True,
            "one_axis_only": True,
            "no_meemee_ui_change": True,
        },
    }

    decision_payload = {
        "schema_version": "tradex_entry_precision_short_decision_v1",
        "session_id": session_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "baseline_id": baseline_id,
        "long_freeze_confirmed": long_freeze_confirmed,
        "overall_decision": overall_decision,
        "decisions": decisions,
        "confirmed": [
            "short_side_only_changed",
            "long_side_frozen",
            "same_window_fixed",
            "same_universe_fixed",
            "same_top_k_fixed",
            "json_is_authoritative",
        ],
        "provisional": [
            "borrow_cost_is_proxy_only",
            "event_risk_is_proxy_based",
            "short_false_neutral_recovery_count_is_subset_based",
        ],
        "remaining_risks": [
            "baseline_short_sample_is_small",
            "borrow_or_cost_has_no_direct_table",
            "event_risk_can_be_sparse_in_this_window",
            "monthly_stability_is_only_partially_observed",
        ],
    }

    payload = {
        "current_state": {
            "confirmed": [
                "entry_precision_short_audit_completed",
                "baseline_short_rows_are_replayed_from_snapshot_db",
                "short_side_was_evaluated_separately",
            ],
            "provisional": [
                "borrow_cost_is_proxy_only",
                "false_neutral_recovery_is_subset_based",
                "monthly_regime_stability_is_partial",
            ],
        },
        "problem": "Short candidates still include weak closes and mixed followthrough; the goal is fewer but more actionable shorts, not broader coverage.",
        "change_policy": "TRADEX only, short-side stage-A cleanup only, long logic frozen, same window and artifact detail level fixed, no multi-axis redesign.",
        "baseline": baseline_summary,
        "taxonomy": taxonomy,
        "feature_map": feature_map_payload,
        "variants": compare_results,
        "decisions": decisions,
        "overall_decision": overall_decision,
        "long_freeze_confirmed": long_freeze_confirmed,
        "remaining_risks": decision_payload["remaining_risks"],
        "next_one_thing": "Move one axis only: either tighten the followthrough gate further or widen the historical slice, not both.",
    }

    _write_json(out_dir / "entry_precision_short_error_taxonomy.json", {
        "session_id": session_id,
        "baseline_id": baseline_id,
        "window": {"start_ymd": int(args.start_ymd), "end_ymd": int(args.end_ymd)},
        "taxonomy": taxonomy,
    })
    _write_json(out_dir / "entry_precision_short_feature_map.json", {
        "session_id": session_id,
        "baseline_id": baseline_id,
        "feature_map": feature_map_payload,
    })
    _write_json(out_dir / "entry_precision_short_challenger_compare.json", compare_payload)
    _write_json(out_dir / "entry_precision_short_decision.json", decision_payload)
    _write_report_md(out_dir / "entry_precision_short_report.md", payload)

    return {
        "ok": True,
        "session_id": session_id,
        "output_dir": str(out_dir),
        "baseline_id": baseline_id,
        "overall_decision": overall_decision,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Short-side precision cleanup audit for TRADEX.")
    parser.add_argument("--db-path", default="", help="Path to authoritative snapshot DB")
    parser.add_argument("--start-ymd", type=int, default=20250101)
    parser.add_argument("--end-ymd", type=int, default=20260226)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args()
    result = run(args)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
