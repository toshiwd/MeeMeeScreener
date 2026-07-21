from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from shared.runtime_stock_db_contract import resolve_runtime_stock_db_path
from tradex_short_entry_timing_rule_probe_v1 import _scan_timing_events
from tradex_short_shape_bad_avoidance_probe_v1 import _daily_rows


AXIS_ID = "short_entry_shape_family_probe_v1"
DEFAULT_DB_PATH = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_entry_shape_family_probe_v1")
CANDIDATE_KEYS = [
    ("mixed_or_late", "ma_not_bearish_enough"),
    ("mixed_or_late", "ma_compressed_transition"),
    ("initial_break_from_high_zone", "ma_resistance_rejection"),
    ("upper_wick_stall_near_high", "full_bear_stack_late"),
    ("upper_wick_stall_near_high", "ma_compressed_transition"),
    ("initial_break_from_high_zone", "reclaim_failed_at_ma"),
    ("pullback_fail_near_ma7_20", "below_7_20_flat_or_down"),
]
INVALIDATED_STATUSES = {"invalidated_by_high_break", "hard_invalidated"}
STRONG_STATUSES = {"confirmed_downside_rejection"}
PROBE_STATUSES = {"intraday_low_break_only", "close_rejection_only"}
MIN_ACTIONABLE_RISK_WIDTH = 20.0
MAX_SHORTLIST_RISK_WIDTH_PCT = 0.08
MIN_SHORTLIST_VOLUME = 100


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _ma(values: list[float], end_index: int, period: int) -> float | None:
    start = end_index - period + 1
    if start < 0:
        return None
    return sum(values[start : end_index + 1]) / period


def _month_rows(bars: list[tuple[int, float, float, float, float, float]]) -> list[dict[str, Any]]:
    months: dict[int, list[tuple[int, float, float, float, float, float]]] = {}
    for row in bars:
        month = int(str(int(row[0]))[:6])
        months.setdefault(month, []).append(row)
    out = []
    for month, rows in sorted(months.items()):
        out.append(
            {
                "month": month,
                "open": float(rows[0][1]),
                "high": max(float(row[2]) for row in rows),
                "low": min(float(row[3]) for row in rows),
                "close": float(rows[-1][4]),
                "volume": sum(float(row[5] or 0) for row in rows),
            }
        )
    return out


def _monthly_breakdown_context(
    bars: list[tuple[int, float, float, float, float, float]],
    index: int,
    close: float,
) -> dict[str, Any]:
    months = _month_rows(bars[: index + 1])
    current_month = int(str(int(bars[index][0]))[:6])
    month_index = next((i for i, row in enumerate(months) if int(row["month"]) == current_month), None)
    if month_index is None or month_index < 3:
        return {"monthly_breakdown_tags": [], "monthly_breakdown_score_bonus": 0.0}
    month = months[month_index]
    prev_month = months[month_index - 1]
    prev2 = months[month_index - 2]
    month_range = float(month["high"]) - float(month["low"])
    if month_range <= 0:
        return {"monthly_breakdown_tags": [], "monthly_breakdown_score_bonus": 0.0}
    extension3 = float(month["high"]) / min(float(months[i]["low"]) for i in range(month_index - 3, month_index)) - 1.0
    upper_wick = (float(month["high"]) - max(float(month["open"]), float(month["close"]))) / month_range
    failed_new_high = float(month["high"]) > max(float(prev_month["high"]), float(prev2["high"])) and float(month["close"]) < float(prev_month["high"])
    mid_break = close < (float(month["high"]) + float(month["low"])) / 2
    prev_low_break = close < float(prev_month["low"])
    tags = []
    if extension3 >= 0.25:
        tags.append("monthly_extension_25pct")
    if upper_wick >= 0.35:
        tags.append("monthly_upper_wick")
    if failed_new_high:
        tags.append("monthly_failed_new_high")
    if mid_break:
        tags.append("monthly_mid_break")
    if prev_low_break:
        tags.append("monthly_prev_low_break")
    bonus = 0.0
    if "monthly_extension_25pct" in tags:
        bonus += 0.05
    if "monthly_upper_wick" in tags:
        bonus += 0.04
    if "monthly_failed_new_high" in tags:
        bonus += 0.04
    if "monthly_mid_break" in tags:
        bonus += 0.03
    if "monthly_prev_low_break" in tags:
        bonus -= 0.03
    return {
        "monthly_breakdown_tags": tags,
        "monthly_breakdown_score_bonus": round(bonus, 6),
        "monthly_breakdown_metrics": {
            "month_extension3": round(extension3, 8),
            "month_upper_wick": round(upper_wick, 8),
        },
    }


def _slope(values: list[float], end_index: int, period: int, lookback: int = 5) -> float | None:
    current = _ma(values, end_index, period)
    previous = _ma(values, end_index - lookback, period)
    if current is None or previous is None or previous == 0:
        return None
    return current / previous - 1.0


def _class(row: dict[str, Any]) -> str:
    return str(row.get("purpose_outcome_class") or "")


def _bucket(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"n": 0}
    return {
        "n": len(rows),
        "entry_now_rate": sum(1 for row in rows if _class(row) == "entry_now") / len(rows),
        "watch_next_rate": sum(1 for row in rows if _class(row) == "watch_next") / len(rows),
        "wrong_rate": sum(1 for row in rows if _class(row) == "too_early_or_wrong") / len(rows),
        "avg_ret5": sum(float(row["ret5"]) for row in rows) / len(rows),
        "avg_MAE5": sum(float(row["MAE5"]) for row in rows) / len(rows),
        "avg_MFE5": sum(float(row["MFE5"]) for row in rows) / len(rows),
    }


def _year_of(row: dict[str, Any]) -> int:
    return int(str(row["as_of"])[:4])


def _year_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    years = sorted({_year_of(row) for row in rows})
    return {
        str(year): _bucket([row for row in rows if _year_of(row) == year])
        for year in years
    }


def _code_concentration(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"unique_codes": 0, "top_code_share": None, "top_codes": []}
    counts: dict[str, int] = {}
    for row in rows:
        code = str(row["code"])
        counts[code] = counts.get(code, 0) + 1
    top_codes = sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:10]
    return {
        "unique_codes": len(counts),
        "top_code_share": top_codes[0][1] / len(rows),
        "top_codes": [{"code": code, "n": n} for code, n in top_codes],
    }


def _candidate_validation(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "summary": _bucket(rows),
        "year_summary": _year_summary(rows),
        "code_concentration": _code_concentration(rows),
        "historical_trigger_status_summary": {
            status: _bucket([row for row in rows if row.get("historical_trigger_recheck", {}).get("status") == status])
            for status in sorted({str(row.get("historical_trigger_recheck", {}).get("status")) for row in rows})
        },
        "examples": {
            "success": sorted(
                [row for row in rows if _class(row) == "entry_now"],
                key=lambda row: (float(row["ret5"]), float(row["MFE5"])),
            )[:12],
            "failure": sorted(
                [row for row in rows if _class(row) == "too_early_or_wrong"],
                key=lambda row: (-float(row["MFE5"]), -float(row["ret5"])),
            )[:12],
        },
    }


def _score_bucket(bucket: dict[str, Any]) -> dict[str, Any]:
    n = int(bucket.get("n") or 0)
    if n == 0:
        return {"score": None, "decision": "drop_no_sample", "reasons": ["no_sample"]}
    entry_rate = float(bucket.get("entry_now_rate") or 0)
    wrong_rate = float(bucket.get("wrong_rate") or 0)
    avg_ret5 = float(bucket.get("avg_ret5") or 0)
    avg_mae5 = float(bucket.get("avg_MAE5") or 0)
    avg_mfe5 = float(bucket.get("avg_MFE5") or 0)
    score = (-avg_ret5 * 100.0) + (entry_rate * 2.0) - (wrong_rate * 3.0) + (-avg_mae5 * 0.5) - (avg_mfe5 * 0.5)
    reasons: list[str] = []
    if n < 50:
        reasons.append("sample_lt_50")
    if wrong_rate > 0.15:
        reasons.append("wrong_rate_gt_15pct")
    if avg_ret5 >= 0:
        reasons.append("avg_ret5_not_negative")
    if avg_mfe5 > 0.015:
        reasons.append("avg_mfe5_gt_1_5pct")
    if entry_rate < 0.20:
        reasons.append("entry_rate_lt_20pct")
    decision = "keep_expectancy_candidate" if not reasons else "hold_expectancy_candidate"
    if "wrong_rate_gt_15pct" in reasons or "avg_ret5_not_negative" in reasons:
        decision = "drop_expectancy_candidate"
    if n < 20:
        decision = "drop_too_thin"
    return {"score": round(score, 6), "decision": decision, "reasons": reasons}


def _stability_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    yearly = _year_summary(rows)
    usable_years = []
    weak_years = []
    for year, bucket in yearly.items():
        if int(bucket.get("n") or 0) < 5:
            continue
        usable_years.append(year)
        if float(bucket.get("wrong_rate") or 0) > 0.25 or float(bucket.get("avg_ret5") or 0) >= 0:
            weak_years.append(year)
    return {
        "usable_year_count": len(usable_years),
        "weak_year_count": len(weak_years),
        "weak_years": weak_years,
        "year_summary": yearly,
    }


def _expectancy_leaderboard(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in rows:
        key = (
            str(row["shape_family"]),
            str(row["ma_shape_family"]),
            str(row.get("historical_trigger_recheck", {}).get("status") or ""),
        )
        grouped.setdefault(key, []).append(row)
    leaders = []
    for (shape_family, ma_shape_family, trigger_status), bucket_rows in grouped.items():
        bucket = _bucket(bucket_rows)
        score = _score_bucket(bucket)
        stability = _stability_summary(bucket_rows)
        leaders.append(
            {
                "candidate_key": f"{shape_family}__{ma_shape_family}",
                "shape_family": shape_family,
                "ma_shape_family": ma_shape_family,
                "trigger_status": trigger_status,
                "summary": bucket,
                "score": score,
                "stability": {
                    "usable_year_count": stability["usable_year_count"],
                    "weak_year_count": stability["weak_year_count"],
                    "weak_years": stability["weak_years"],
                },
            }
        )
    return sorted(
        leaders,
        key=lambda item: (
            item["score"]["decision"] != "keep_expectancy_candidate",
            -(item["score"]["score"] if item["score"]["score"] is not None else -999999),
            -int(item["summary"].get("n") or 0),
        ),
    )


def _freshness(conn: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            COALESCE(source, 'pan') AS source,
            max(
                CASE
                    WHEN length(CAST(abs(date) AS VARCHAR)) = 8 THEN CAST(date AS INTEGER)
                    ELSE CAST(strftime(to_timestamp(CAST(date AS BIGINT)), '%Y%m%d') AS INTEGER)
                END
            ) AS max_ymd,
            count(*) AS row_count
        FROM daily_bars
        GROUP BY COALESCE(source, 'pan')
        ORDER BY source
        """
    ).fetchall()
    return [{"source": source, "max_ymd": int(max_ymd), "row_count": int(row_count)} for source, max_ymd, row_count in rows]


def _family_for(bars: list[tuple[int, float, float, float, float, float]], as_of: int) -> dict[str, Any] | None:
    ymds = [int(row[0]) for row in bars]
    if as_of not in ymds:
        return None
    index = ymds.index(as_of)
    if index < 80:
        return None
    closes = [float(row[4]) for row in bars]
    highs = [float(row[2]) for row in bars]
    lows = [float(row[3]) for row in bars]
    open_ = float(bars[index][1])
    high = float(bars[index][2])
    low = float(bars[index][3])
    close = float(bars[index][4])
    candle_range = high - low
    if candle_range <= 0:
        return None
    ma7 = _ma(closes, index, 7)
    ma20 = _ma(closes, index, 20)
    ma60 = _ma(closes, index, 60)
    if ma7 is None or ma20 is None or ma60 is None:
        return None
    ma7_prev = _ma(closes, index - 5, 7)
    ma20_prev = _ma(closes, index - 5, 20)
    ma60_prev = _ma(closes, index - 5, 60)
    ma7_slope5 = _slope(closes, index, 7)
    ma20_slope5 = _slope(closes, index, 20)
    ma60_slope5 = _slope(closes, index, 60)
    if ma7_prev is None or ma20_prev is None or ma60_prev is None or ma7_slope5 is None or ma20_slope5 is None or ma60_slope5 is None:
        return None
    prev_closes = closes[index - 10 : index]
    high20_prev = max(highs[index - 20 : index])
    high10_prev = max(highs[index - 10 : index])
    low10_prev = min(lows[index - 10 : index])
    close_vs_high20 = close / high20_prev - 1.0
    close_vs_ma20 = close / ma20 - 1.0
    close_vs_ma60 = close / ma60 - 1.0
    upper_wick_ratio = (high - max(open_, close)) / candle_range
    red_count5 = sum(1 for offset in range(index - 4, index + 1) if float(bars[offset][4]) < float(bars[offset][1]))
    below_ma20_count5 = sum(1 for offset in range(index - 4, index + 1) if closes[offset] < (_ma(closes, offset, 20) or closes[offset]))
    first_break_ma20 = closes[index] < ma20 and sum(1 for offset in range(index - 5, index) if closes[offset] < (_ma(closes, offset, 20) or closes[offset])) <= 1
    pullback_fail = closes[index - 1] < (_ma(closes, index - 1, 20) or closes[index - 1]) and high >= ma7 * 0.995 and close < ma7
    upper_wick_stall = high >= high10_prev * 0.995 and upper_wick_ratio >= 0.30 and close < open_
    continuation_weak = below_ma20_count5 >= 4 and close < ma7 and close < ma20 and close_vs_high20 < -0.08
    shallow_high_break = close_vs_high20 >= -0.06
    ma_stack = "bull_7_20_60"
    if ma7 < ma20 < ma60:
        ma_stack = "bear_7_20_60"
    elif ma7 < ma20 and ma20 >= ma60:
        ma_stack = "short_term_bear_above60"
    elif ma7 >= ma20 and ma20 < ma60:
        ma_stack = "rebound_above20_below60"
    ma_compressed = abs(ma7 / ma20 - 1.0) <= 0.015 and abs(ma20 / ma60 - 1.0) <= 0.04
    ma7_turning_down = ma7_slope5 < -0.002
    ma20_turning_down = ma20_slope5 < -0.0015
    ma20_flat_or_down = ma20_slope5 <= 0.001
    ma7_below20_now = ma7 < ma20
    ma7_crossed_below20 = ma7 < ma20 and ma7_prev >= ma20_prev
    ma_reclaim_failure = (
        closes[index - 1] > (_ma(closes, index - 1, 7) or closes[index - 1])
        and close < ma7
        and high >= ma7 * 0.995
    )
    ma_resistance_touch = (
        (high >= ma7 * 0.995 and close < ma7)
        or (high >= ma20 * 0.995 and close < ma20)
    )
    ma_shape_tags = []
    if ma_stack != "bull_7_20_60":
        ma_shape_tags.append(ma_stack)
    if ma_compressed:
        ma_shape_tags.append("ma_compressed")
    if ma7_turning_down:
        ma_shape_tags.append("ma7_turning_down")
    if ma20_turning_down:
        ma_shape_tags.append("ma20_turning_down")
    if ma7_crossed_below20:
        ma_shape_tags.append("ma7_crossed_below20")
    if ma_reclaim_failure:
        ma_shape_tags.append("ma_reclaim_failure")
    if ma_resistance_touch:
        ma_shape_tags.append("ma_resistance_touch")
    if close < ma7 and close < ma20 and ma20_flat_or_down:
        ma_shape_family = "below_7_20_flat_or_down"
    elif ma_reclaim_failure and ma_resistance_touch:
        ma_shape_family = "reclaim_failed_at_ma"
    elif ma7_crossed_below20:
        ma_shape_family = "fresh_7_below20_cross"
    elif ma_stack == "bear_7_20_60":
        ma_shape_family = "full_bear_stack_late"
    elif ma_resistance_touch:
        ma_shape_family = "ma_resistance_rejection"
    elif ma_compressed:
        ma_shape_family = "ma_compressed_transition"
    else:
        ma_shape_family = "ma_not_bearish_enough"
    if first_break_ma20 and shallow_high_break:
        family = "initial_break_from_high_zone"
    elif pullback_fail and shallow_high_break:
        family = "pullback_fail_near_ma7_20"
    elif upper_wick_stall and shallow_high_break:
        family = "upper_wick_stall_near_high"
    elif continuation_weak:
        family = "continuation_weak_late"
    else:
        family = "mixed_or_late"
    source_bar = {
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": float(bars[index][5] or 0),
    }
    trigger_plan = _trigger_plan_for_bar(source_bar)
    historical_trigger_recheck = _historical_trigger_recheck(bars, index, trigger_plan)
    monthly_context = _monthly_breakdown_context(bars, index, close)
    return {
        "shape_family": family,
        "source_bar": source_bar,
        "trigger_plan": trigger_plan,
        "historical_trigger_recheck": historical_trigger_recheck,
        **monthly_context,
        "close_vs_high20": round(close_vs_high20, 8),
        "close_vs_ma20": round(close_vs_ma20, 8),
        "close_vs_ma60": round(close_vs_ma60, 8),
        "ma7_vs_ma20": round(ma7 / ma20 - 1.0, 8),
        "ma20_vs_ma60": round(ma20 / ma60 - 1.0, 8),
        "ma7_slope5": round(ma7_slope5, 8),
        "ma20_slope5": round(ma20_slope5, 8),
        "ma60_slope5": round(ma60_slope5, 8),
        "ma_stack": ma_stack,
        "ma_shape_family": ma_shape_family,
        "ma_shape_tags": ma_shape_tags,
        "upper_wick_ratio": round(upper_wick_ratio, 8),
        "red_count5": red_count5,
        "below_ma20_count5": below_ma20_count5,
        "range10_pos": round((close - low10_prev) / (high10_prev - low10_prev), 8) if high10_prev > low10_prev else None,
        "family_tags": {
            "first_break_ma20": first_break_ma20,
            "pullback_fail": pullback_fail,
            "upper_wick_stall": upper_wick_stall,
            "continuation_weak": continuation_weak,
            "shallow_high_break": shallow_high_break,
            "ma_compressed": ma_compressed,
            "ma7_turning_down": ma7_turning_down,
            "ma20_turning_down": ma20_turning_down,
            "ma7_below20_now": ma7_below20_now,
            "ma7_crossed_below20": ma7_crossed_below20,
            "ma_reclaim_failure": ma_reclaim_failure,
            "ma_resistance_touch": ma_resistance_touch,
        },
    }


def _current_candidate_rows(
    conn: duckdb.DuckDBPyConnection,
    *,
    confirmed_as_of: int,
    target_shape_family: str,
    target_ma_shape_family: str,
) -> list[dict[str, Any]]:
    codes = [
        str(row[0])
        for row in conn.execute(
            "SELECT DISTINCT code FROM daily_bars WHERE COALESCE(source, 'pan') <> 'yahoo' ORDER BY code"
        ).fetchall()
    ]
    names = {
        str(code): str(name or "")
        for code, name in conn.execute("SELECT code, name FROM tickers").fetchall()
    }
    rows: list[dict[str, Any]] = []
    for code in codes:
        bars = _daily_rows(conn, code)
        if len(bars) < 80:
            continue
        ymds = [int(row[0]) for row in bars]
        if confirmed_as_of not in ymds:
            continue
        family = _family_for(bars, confirmed_as_of)
        if family is None:
            continue
        if family["shape_family"] != target_shape_family or family["ma_shape_family"] != target_ma_shape_family:
            continue
        rows.append(
            {
                "code": code,
                "name": names.get(code, ""),
                "as_of": confirmed_as_of,
                **family,
            }
        )
    return sorted(
        rows,
        key=lambda row: (
            float(row.get("close_vs_high20") or 0),
            -float(row.get("upper_wick_ratio") or 0),
            str(row["code"]),
        ),
    )


def _bars_after_with_yahoo(conn: duckdb.DuckDBPyConnection, code: str, after_ymd: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        WITH normalized AS (
            SELECT
                CASE
                    WHEN length(CAST(abs(date) AS VARCHAR)) = 8 THEN CAST(date AS INTEGER)
                    ELSE CAST(strftime(to_timestamp(CAST(date AS BIGINT)), '%Y%m%d') AS INTEGER)
                END AS ymd,
                COALESCE(source, 'pan') AS source,
                o, h, l, c, v
            FROM daily_bars
            WHERE code = ?
              AND COALESCE(source, 'pan') IN ('pan', 'yahoo')
              AND o IS NOT NULL AND h IS NOT NULL AND l IS NOT NULL AND c IS NOT NULL
        ),
        ranked AS (
            SELECT *,
                   row_number() OVER (
                       PARTITION BY ymd
                       ORDER BY CASE WHEN source = 'yahoo' THEN 1 ELSE 0 END DESC
                   ) AS rn
            FROM normalized
        )
        SELECT ymd, source, o, h, l, c, v
        FROM ranked
        WHERE rn = 1 AND ymd > ?
        ORDER BY ymd
        """,
        [code, after_ymd],
    ).fetchall()
    return [
        {
            "ymd": int(ymd),
            "source": str(source),
            "open": float(open_),
            "high": float(high),
            "low": float(low),
            "close": float(close),
            "volume": float(volume or 0),
        }
        for ymd, source, open_, high, low, close, volume in rows
    ]


def _provisional_recheck(conn: duckdb.DuckDBPyConnection, row: dict[str, Any]) -> dict[str, Any]:
    future = _bars_after_with_yahoo(conn, str(row["code"]), int(row["as_of"]))
    if not future:
        return {
            "status": "waiting_next_bar",
            "reason": "no pan/yahoo bar after confirmed candidate date",
            "evaluated_bar": None,
        }
    bar = future[0]
    plan = row.get("trigger_plan") or _trigger_plan(row)
    candidate_high = float(plan["invalidate_if_high_breaks"])
    candidate_low = float(plan["entry_review_trigger_break_low"])
    close_below = float(plan["entry_review_trigger_close_below"])
    hard_invalid = float(plan["hard_invalidate_if_above"])
    close = float(bar["close"])
    if bar["high"] > hard_invalid:
        status = "hard_invalidated"
        reason = "next bar high exceeded hard invalidation level"
    elif bar["high"] > candidate_high:
        status = "invalidated_by_high_break"
        reason = "next bar high exceeded trigger invalidation level"
    elif bar["low"] < candidate_low and close <= close_below:
        status = "confirmed_downside_rejection"
        reason = "next bar broke trigger low and closed below trigger close threshold"
    elif bar["low"] < candidate_low:
        status = "intraday_low_break_only"
        reason = "next bar broke trigger low but close confirmation is missing"
    elif close <= close_below:
        status = "close_rejection_only"
        reason = "next bar closed below trigger close threshold without low break"
    else:
        status = "still_waiting"
        reason = "next bar did not invalidate or confirm downside rejection"
    return {
        "status": status,
        "reason": reason,
        "trigger_plan_used": plan,
        "evaluated_bar": bar,
    }


def _trigger_plan_for_bar(source_bar: dict[str, Any]) -> dict[str, Any]:
    high = float(source_bar["high"])
    low = float(source_bar["low"])
    close = float(source_bar["close"])
    candle_range = max(high - low, 0.0)
    close_below = min(close, low + candle_range * 0.35)
    return {
        "entry_review_trigger_break_low": low,
        "entry_review_trigger_close_below": round(close_below, 4),
        "invalidate_if_high_breaks": high,
        "hard_invalidate_if_above": round(high + candle_range * 0.5, 4),
        "risk_width_from_close_to_invalidate": round(max(high - close, 0.0), 4),
        "risk_width_from_trigger_close_to_invalidate": round(max(high - close_below, 0.0), 4),
        "trigger_basis": "candidate low break plus close rejection; invalidation on candidate high break",
    }


def _trigger_plan(row: dict[str, Any]) -> dict[str, Any]:
    return _trigger_plan_for_bar(row["source_bar"])


def _evaluate_bar_against_trigger(bar: dict[str, Any], plan: dict[str, Any]) -> dict[str, Any]:
    break_low = float(plan["entry_review_trigger_break_low"])
    close_below = float(plan["entry_review_trigger_close_below"])
    invalid_high = float(plan["invalidate_if_high_breaks"])
    hard_invalid = float(plan["hard_invalidate_if_above"])
    close = float(bar["close"])
    if float(bar["high"]) > hard_invalid:
        status = "hard_invalidated"
        reason = "next bar high exceeded hard invalidation level"
    elif float(bar["high"]) > invalid_high:
        status = "invalidated_by_high_break"
        reason = "next bar high exceeded trigger invalidation level"
    elif float(bar["low"]) < break_low and close <= close_below:
        status = "confirmed_downside_rejection"
        reason = "next bar broke trigger low and closed below trigger close threshold"
    elif float(bar["low"]) < break_low:
        status = "intraday_low_break_only"
        reason = "next bar broke trigger low but close confirmation is missing"
    elif close <= close_below:
        status = "close_rejection_only"
        reason = "next bar closed below trigger close threshold without low break"
    else:
        status = "still_waiting"
        reason = "next bar did not invalidate or confirm downside rejection"
    return {"status": status, "reason": reason, "trigger_plan_used": plan, "evaluated_bar": bar}


def _historical_trigger_recheck(
    bars: list[tuple[int, float, float, float, float, float]],
    index: int,
    plan: dict[str, Any],
) -> dict[str, Any]:
    if index + 1 >= len(bars):
        return {"status": "waiting_next_bar", "reason": "no next historical bar", "evaluated_bar": None}
    next_bar = bars[index + 1]
    bar = {
        "ymd": int(next_bar[0]),
        "source": "historical_confirmed",
        "open": float(next_bar[1]),
        "high": float(next_bar[2]),
        "low": float(next_bar[3]),
        "close": float(next_bar[4]),
        "volume": float(next_bar[5] or 0),
    }
    return _evaluate_bar_against_trigger(bar, plan)


def _tradability_exclusion_reasons(row: dict[str, Any]) -> list[str]:
    code = str(row.get("code") or "")
    name = str(row.get("name") or "")
    volume = float(row.get("source_bar", {}).get("volume") or 0)
    reasons: list[str] = []
    if name.startswith("NEXT ") or "ETF" in name.upper() or "ETN" in name.upper() or "NOMURA" in name.upper():
        reasons.append("fund_or_etf_like_name")
    if code.isdigit() and (1300 <= int(code) <= 1699 or 2500 <= int(code) <= 2599):
        reasons.append("fund_or_etf_like_code_band")
    if volume < 100:
        reasons.append("low_candidate_bar_volume_lt_100")
    return reasons


def _probe_quality_rejection_reasons(row: dict[str, Any]) -> list[str]:
    status = str(row.get("provisional_recheck", {}).get("status") or "")
    risk_width = float(row.get("trigger_plan", {}).get("risk_width_from_trigger_close_to_invalidate") or 0)
    reasons: list[str] = []
    if risk_width < MIN_ACTIONABLE_RISK_WIDTH:
        reasons.append("risk_width_too_small_for_actionable_probe")
    if status == "close_rejection_only":
        reasons.append("close_only_without_low_break")
    return reasons


def _shortlist_rejection_reasons(row: dict[str, Any]) -> list[str]:
    reasons = _tradability_exclusion_reasons(row)
    plan = row.get("trigger_plan", {})
    source_bar = row.get("source_bar", {})
    close = float(source_bar.get("close") or 0)
    risk_width = float(plan.get("risk_width_from_trigger_close_to_invalidate") or 0)
    risk_width_pct = risk_width / close if close else 999.0
    volume = float(source_bar.get("volume") or 0)
    code = str(row.get("code") or "")
    name = str(row.get("name") or "")
    if code in {"1001", "1002", "1003"} or "日経" in name or "TOPIX" in name.upper():
        reasons.append("index_like_code_or_name")
    if volume < MIN_SHORTLIST_VOLUME:
        reasons.append("low_candidate_bar_volume_for_shortlist")
    if risk_width_pct > MAX_SHORTLIST_RISK_WIDTH_PCT:
        reasons.append("risk_width_pct_gt_8pct")
    return reasons


def _shortlist_score(row: dict[str, Any]) -> float:
    plan = row.get("trigger_plan", {})
    source_bar = row.get("source_bar", {})
    validation = row.get("historical_same_status_stats", {})
    close = float(source_bar.get("close") or 0)
    risk_width = float(plan.get("risk_width_from_trigger_close_to_invalidate") or 0)
    risk_width_pct = risk_width / close if close else 1.0
    wrong_rate = float(validation.get("wrong_rate") or 0.25)
    avg_ret5 = float(validation.get("avg_ret5") or 0)
    avg_mfe5 = float(validation.get("avg_MFE5") or 0.03)
    monthly_bonus = float(row.get("monthly_breakdown_score_bonus") or 0.0)
    return round((-avg_ret5 * 100.0) - (wrong_rate * 2.0) - (avg_mfe5 * 0.75) - (risk_width_pct * 2.5) + monthly_bonus, 6)


def _build_shortlist(board: dict[str, Any], validation: dict[str, Any], limit: int = 5) -> dict[str, Any]:
    validation_by_code = {
        str(row["code"]): row
        for row in validation.get("rows", [])
        if row.get("candidate_local_decision") == "keep_actionable_probe"
    }
    eligible = []
    rejected = []
    for row in board.get("strong_rows", []):
        validation_row = validation_by_code.get(str(row["code"]))
        if not validation_row:
            rejected.append({**row, "shortlist_rejection_reasons": ["missing_keep_validation"]})
            continue
        enriched = {**row, **validation_row}
        reasons = _shortlist_rejection_reasons(enriched)
        if reasons:
            rejected.append({**enriched, "shortlist_rejection_reasons": reasons})
            continue
        eligible.append({**enriched, "shortlist_score": _shortlist_score(enriched)})
    shortlist = sorted(
        eligible,
        key=lambda row: (
            -float(row["shortlist_score"]),
            float(row.get("trigger_plan", {}).get("risk_width_from_trigger_close_to_invalidate") or 999999),
            str(row["code"]),
        ),
    )[:limit]
    return {
        "row_count": len(shortlist),
        "eligible_count": len(eligible),
        "rejected_count": len(rejected),
        "rows": shortlist,
        "rejected_rows": rejected,
        "decision": {
            "candidate_local_decision": "shortlist_present" if shortlist else "no_shortlist_candidate",
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": "strong rows filtered by tradability, risk width, and historical keep validation",
        },
    }


def _current_scan_payload(
    conn: duckdb.DuckDBPyConnection,
    *,
    confirmed_as_of: int,
    shape_family: str,
    ma_shape_family: str,
) -> dict[str, Any]:
    candidate_key = f"{shape_family}__{ma_shape_family}"
    current_candidates = _current_candidate_rows(
        conn,
        confirmed_as_of=confirmed_as_of,
        target_shape_family=shape_family,
        target_ma_shape_family=ma_shape_family,
    )
    for row in current_candidates:
        row["trigger_plan"] = _trigger_plan(row)
        row["provisional_recheck"] = _provisional_recheck(conn, row)
    active_rows = [
        row for row in current_candidates
        if row.get("provisional_recheck", {}).get("status") not in INVALIDATED_STATUSES
    ]
    tradable_active_rows: list[dict[str, Any]] = []
    excluded_active_rows: list[dict[str, Any]] = []
    for row in active_rows:
        exclusion_reasons = _tradability_exclusion_reasons(row)
        if exclusion_reasons:
            excluded_active_rows.append({**row, "exclusion_reasons": exclusion_reasons})
        else:
            tradable_active_rows.append(row)
    invalidated_rows = [
        row for row in current_candidates
        if row.get("provisional_recheck", {}).get("status") in INVALIDATED_STATUSES
    ]
    strong_rows = [
        row for row in tradable_active_rows
        if row.get("provisional_recheck", {}).get("status") in STRONG_STATUSES
    ]
    probe_rows = [
        row for row in tradable_active_rows
        if row.get("provisional_recheck", {}).get("status") in PROBE_STATUSES
    ]
    watch_rows = [
        row for row in tradable_active_rows
        if row.get("provisional_recheck", {}).get("status") not in STRONG_STATUSES
        and row.get("provisional_recheck", {}).get("status") not in PROBE_STATUSES
    ]
    return {
        "target_candidate_key": candidate_key,
        "shape_family": shape_family,
        "ma_shape_family": ma_shape_family,
        "row_count": len(current_candidates),
        "active_row_count": len(active_rows),
        "tradable_active_row_count": len(tradable_active_rows),
        "excluded_active_row_count": len(excluded_active_rows),
        "invalidated_row_count": len(invalidated_rows),
        "strong_row_count": len(strong_rows),
        "probe_row_count": len(probe_rows),
        "watch_row_count": len(watch_rows),
        "rows": current_candidates,
        "active_rows": active_rows,
        "tradable_active_rows": tradable_active_rows,
        "excluded_active_rows": excluded_active_rows,
        "invalidated_rows": invalidated_rows,
        "strong_rows": strong_rows,
        "probe_rows": probe_rows,
        "watch_rows": watch_rows,
        "decision": {
            "candidate_local_decision": "review_only_strong_current_candidates_present"
            if strong_rows
            else "review_only_probe_current_candidates_present"
            if probe_rows
            else "review_only_watch_current_candidates_present"
            if watch_rows
            else "no_active_current_candidate",
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": "latest provisional recheck confirmed downside rejection"
            if strong_rows
            else "latest provisional recheck has partial trigger only"
            if probe_rows
            else "candidate morphology remains active but trigger confirmation is still waiting"
            if watch_rows
            else "no current candidate remains after provisional high-break invalidation or research tradability filters",
        },
    }


def _current_board(current_scans: dict[str, dict[str, Any]]) -> dict[str, Any]:
    strong_rows: list[dict[str, Any]] = []
    probe_rows: list[dict[str, Any]] = []
    watch_rows: list[dict[str, Any]] = []
    excluded_rows: list[dict[str, Any]] = []
    invalidated_rows: list[dict[str, Any]] = []
    for candidate_key, scan in current_scans.items():
        for bucket_name, target in [
            ("strong_rows", strong_rows),
            ("probe_rows", probe_rows),
            ("watch_rows", watch_rows),
            ("excluded_active_rows", excluded_rows),
            ("invalidated_rows", invalidated_rows),
        ]:
            for row in scan.get(bucket_name, []):
                target.append({**row, "candidate_key": candidate_key})
    probe_rows = sorted(
        probe_rows,
        key=lambda row: (
            0 if row.get("provisional_recheck", {}).get("status") == "intraday_low_break_only" else 1,
            float(row.get("trigger_plan", {}).get("risk_width_from_trigger_close_to_invalidate") or 999999),
            str(row["code"]),
        ),
    )
    actionable_probe_rows: list[dict[str, Any]] = []
    low_quality_probe_rows: list[dict[str, Any]] = []
    for row in probe_rows:
        quality_reasons = _probe_quality_rejection_reasons(row)
        if quality_reasons:
            low_quality_probe_rows.append({**row, "probe_quality_rejection_reasons": quality_reasons})
        else:
            actionable_probe_rows.append(row)
    watch_rows = sorted(
        watch_rows,
        key=lambda row: (
            float(row.get("trigger_plan", {}).get("risk_width_from_trigger_close_to_invalidate") or 999999),
            str(row["code"]),
        ),
    )
    decision = "no_active_current_candidate"
    if strong_rows:
        decision = "strong_review_candidates_present"
    elif actionable_probe_rows:
        decision = "actionable_probe_review_candidates_present"
    elif watch_rows:
        decision = "watch_review_candidates_present"
    return {
        "strong_count": len(strong_rows),
        "probe_count": len(probe_rows),
        "actionable_probe_count": len(actionable_probe_rows),
        "low_quality_probe_count": len(low_quality_probe_rows),
        "watch_count": len(watch_rows),
        "excluded_count": len(excluded_rows),
        "invalidated_count": len(invalidated_rows),
        "strong_rows": strong_rows,
        "probe_rows": probe_rows,
        "actionable_probe_rows": actionable_probe_rows,
        "low_quality_probe_rows": low_quality_probe_rows,
        "watch_rows": watch_rows,
        "excluded_rows": excluded_rows,
        "invalidated_rows": invalidated_rows,
        "decision": {
            "candidate_local_decision": decision,
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": "unified review-only board from current candidate scans",
        },
    }


def _current_board_validation(board: dict[str, Any], candidate_validation: dict[str, Any]) -> dict[str, Any]:
    rows = []
    rows_to_validate = [
        *[{**row, "board_bucket": "strong"} for row in board.get("strong_rows", [])],
        *[{**row, "board_bucket": "actionable_probe"} for row in board.get("actionable_probe_rows", [])],
    ]
    for row in rows_to_validate:
        candidate_key = str(row["candidate_key"])
        status = str(row.get("provisional_recheck", {}).get("status") or "")
        status_stats = (
            candidate_validation
            .get(candidate_key, {})
            .get("historical_trigger_status_summary", {})
            .get(status, {"n": 0})
        )
        decision = "hold_insufficient_sample"
        reasons = []
        n = int(status_stats.get("n") or 0)
        wrong_rate = status_stats.get("wrong_rate")
        entry_now_rate = status_stats.get("entry_now_rate")
        avg_ret5 = status_stats.get("avg_ret5")
        if n < 20:
            reasons.append("historical_same_status_sample_lt_20")
        if wrong_rate is not None and float(wrong_rate) > 0.25:
            reasons.append("historical_same_status_wrong_rate_gt_25pct")
        if entry_now_rate is not None and float(entry_now_rate) < 0.18:
            reasons.append("historical_same_status_entry_rate_lt_18pct")
        if avg_ret5 is not None and float(avg_ret5) > 0:
            reasons.append("historical_same_status_avg_ret5_not_negative")
        if not reasons:
            decision = "keep_actionable_probe"
        elif "historical_same_status_sample_lt_20" in reasons:
            decision = "hold_actionable_probe"
        else:
            decision = "drop_actionable_probe"
        rows.append(
            {
                "code": row["code"],
                "name": row["name"],
                "candidate_key": candidate_key,
                "board_bucket": row["board_bucket"],
                "current_status": status,
                "historical_same_status_stats": status_stats,
                "candidate_local_decision": decision,
                "decision_reasons": reasons,
            }
        )
    aggregate_decision = "no_actionable_probe"
    if any(row["candidate_local_decision"] == "keep_actionable_probe" and row["board_bucket"] == "strong" for row in rows):
        aggregate_decision = "keep_strong_candidate"
    elif any(row["candidate_local_decision"] == "keep_actionable_probe" for row in rows):
        aggregate_decision = "keep_some_actionable_probe"
    elif any(row["candidate_local_decision"] == "hold_actionable_probe" for row in rows):
        aggregate_decision = "hold_actionable_probe_only"
    elif any(row["candidate_local_decision"] == "drop_actionable_probe" for row in rows):
        aggregate_decision = "drop_actionable_probe"
    return {
        "rows": rows,
        "decision": {
            "candidate_local_decision": aggregate_decision,
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": "actionable probe rows validated against historical same trigger status",
        },
    }


def run(*, db_path: Path, output_root: Path, start_ymd: int, max_rows: int, train_until: int) -> Path:
    events = _scan_timing_events(db_path, start_ymd=start_ymd, max_rows=max_rows)
    cache: dict[str, list[tuple[int, float, float, float, float, float]]] = {}
    enriched: list[dict[str, Any]] = []
    conn = duckdb.connect(str(db_path), read_only=True)
    freshness: list[dict[str, Any]] = []
    current_scans: dict[str, dict[str, Any]] = {}
    confirmed_as_of: int | None = None
    try:
        freshness = _freshness(conn)
        pan_dates = [row["max_ymd"] for row in freshness if row["source"] == "pan"]
        confirmed_as_of = max(pan_dates) if pan_dates else None
        for row in events:
            code = str(row["code"])
            if code not in cache:
                cache[code] = _daily_rows(conn, code)
            family = _family_for(cache[code], int(row["as_of"]))
            if family is None:
                continue
            enriched.append({**row, **family})
        if confirmed_as_of is not None:
            for shape_family, ma_shape_family in CANDIDATE_KEYS:
                candidate_key = f"{shape_family}__{ma_shape_family}"
                current_scans[candidate_key] = _current_scan_payload(
                    conn,
                    confirmed_as_of=confirmed_as_of,
                    shape_family=shape_family,
                    ma_shape_family=ma_shape_family,
                )
    finally:
        conn.close()
    train = [row for row in enriched if int(row["as_of"]) <= train_until]
    test = [row for row in enriched if int(row["as_of"]) > train_until]
    families = sorted({row["shape_family"] for row in enriched})
    family_summary = {
        family: {
            "train": _bucket([row for row in train if row["shape_family"] == family]),
            "test": _bucket([row for row in test if row["shape_family"] == family]),
        }
        for family in families
    }
    ma_families = sorted({row["ma_shape_family"] for row in enriched})
    ma_shape_summary = {
        family: {
            "train": _bucket([row for row in train if row["ma_shape_family"] == family]),
            "test": _bucket([row for row in test if row["ma_shape_family"] == family]),
        }
        for family in ma_families
    }
    shape_ma_cross_summary = {
        f"{shape_family}__{ma_family}": {
            "shape_family": shape_family,
            "ma_shape_family": ma_family,
            "train": _bucket([row for row in train if row["shape_family"] == shape_family and row["ma_shape_family"] == ma_family]),
            "test": _bucket([row for row in test if row["shape_family"] == shape_family and row["ma_shape_family"] == ma_family]),
        }
        for shape_family in families
        for ma_family in ma_families
        if any(row["shape_family"] == shape_family and row["ma_shape_family"] == ma_family for row in enriched)
    }
    candidate_validation = {}
    for shape_family, ma_shape_family in CANDIDATE_KEYS:
        candidate_key = f"{shape_family}__{ma_shape_family}"
        candidate_rows = [
            row for row in test
            if row["shape_family"] == shape_family and row["ma_shape_family"] == ma_shape_family
        ]
        candidate_validation[candidate_key] = _candidate_validation(candidate_rows)
    examples: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for family in families:
        family_rows = [row for row in test if row["shape_family"] == family]
        success = sorted(
            [row for row in family_rows if _class(row) == "entry_now"],
            key=lambda row: (float(row["ret5"]), float(row["MFE5"])),
        )[:8]
        failure = sorted(
            [row for row in family_rows if _class(row) == "too_early_or_wrong"],
            key=lambda row: (-float(row["MFE5"]), -float(row["ret5"])),
        )[:8]
        examples[family] = {"success": success, "failure": failure}
    current_board = _current_board(current_scans)
    current_board_validation = _current_board_validation(current_board, candidate_validation)
    current_shortlist = _build_shortlist(current_board, current_board_validation)
    expectancy_leaderboard = _expectancy_leaderboard(test)
    keep_expectancy_candidates = [
        row for row in expectancy_leaderboard
        if row["score"]["decision"] == "keep_expectancy_candidate"
    ]
    report = {
        "schema_version": AXIS_ID,
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "db_path": str(db_path),
        "runtime_freshness_by_source": freshness,
        "confirmed_as_of": confirmed_as_of,
        "confirmed_source_policy": "confirmed non-yahoo daily_bars only; yahoo provisional excluded from current scan",
        "fixed_evaluation_conditions": {
            "source": "confirmed non-yahoo daily_bars",
            "base_events": "short_entry_timing_rule_probe_v1 events",
            "start_ymd": start_ymd,
            "max_rows": max_rows,
            "train_until": train_until,
            "test_after": train_until,
        },
        "current_scans": current_scans,
        "current_board": current_board,
        "current_board_validation": current_board_validation,
        "current_shortlist": current_shortlist,
        "current_scan": current_scans.get("upper_wick_stall_near_high__full_bear_stack_late", {}),
        "sample_counts": {"all": len(enriched), "train": len(train), "test": len(test)},
        "overall": {"train": _bucket(train), "test": _bucket(test)},
        "family_summary": family_summary,
        "ma_shape_summary": ma_shape_summary,
        "shape_ma_cross_summary": shape_ma_cross_summary,
        "candidate_validation": candidate_validation,
        "expectancy_leaderboard": expectancy_leaderboard[:100],
        "keep_expectancy_candidates": keep_expectancy_candidates,
        "examples": examples,
        "decision": {
            "candidate_local_decision": "ma_shape_family_split_ready_for_comparison",
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": "events are split by pre-entry candle family and MA morphology so success/failure cases can be compared within tighter families",
        },
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
    }
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    _write_json(output_dir / "short_entry_shape_family_probe.json", report)
    _write_json(output_root / "latest_short_entry_shape_family_probe.json", {"run_root": str(output_dir), **report})
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--start-ymd", type=int, default=20150101)
    parser.add_argument("--max-rows", type=int, default=6000)
    parser.add_argument("--train-until", type=int, default=20201231)
    args = parser.parse_args()
    print(run(
        db_path=args.db_path or resolve_runtime_stock_db_path(),
        output_root=args.output_root,
        start_ymd=args.start_ymd,
        max_rows=args.max_rows,
        train_until=args.train_until,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
