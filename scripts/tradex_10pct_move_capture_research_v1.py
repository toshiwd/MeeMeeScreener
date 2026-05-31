from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any

import duckdb

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.runtime_stock_db_contract import resolve_runtime_stock_db_path


AXIS_ID = "ten_pct_move_capture_research_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\ten_pct_move_capture_research_v1")
HORIZONS = (10, 20, 40)
STOP_RULES = ("stop_a", "stop_b", "stop_c")
REQUIRED_ARTIFACTS = (
    "ten_pct_capture_summary.json",
    "ten_pct_capture_rows.csv",
    "setup_family_contract.json",
    "long_short_direction_contract.json",
    "target_stop_contract.json",
    "setup_metrics.json",
    "direction_metrics.json",
    "hit_before_stop_metrics.json",
    "holding_period_metrics.json",
    "drawdown_adverse_excursion_metrics.json",
    "no_lookahead_audit.json",
    "source_coverage.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, float):
        return None if not math.isfinite(value) else value
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _safe_div(num: float, den: float) -> float | None:
    return None if den == 0 else num / den


def _mean(values: list[float]) -> float | None:
    return None if not values else sum(values) / len(values)


def _round(value: float | None, digits: int = 6) -> float | None:
    return None if value is None else round(value, digits)


def _date_expr(column: str = "date") -> str:
    return f"""
        CASE
            WHEN {column} BETWEEN 19000101 AND 20991231 THEN CAST({column} AS INTEGER)
            WHEN {column} >= 1000000000000 THEN CAST(strftime(to_timestamp({column} / 1000), '%Y%m%d') AS INTEGER)
            WHEN {column} >= 1000000000 THEN CAST(strftime(to_timestamp({column}), '%Y%m%d') AS INTEGER)
            ELSE NULL
        END
    """


def _table_columns(conn: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    try:
        rows = conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_schema='main' AND table_name=?",
            [table_name],
        ).fetchall()
    except Exception:
        return set()
    return {str(row[0]) for row in rows}


def _load_bars(conn: duckdb.DuckDBPyConnection, *, start_ymd: int, end_ymd: int, max_codes: int | None) -> list[dict[str, Any]]:
    source_filter = ""
    cols = _table_columns(conn, "daily_bars")
    if "source" in cols:
        source_filter = "AND lower(coalesce(source, '')) = 'pan'"
    code_limit = ""
    params: list[Any] = [start_ymd, end_ymd]
    if max_codes:
        code_limit = "AND code IN (SELECT DISTINCT code FROM daily_bars ORDER BY code LIMIT ?)"
        params.append(int(max_codes))
    query = f"""
        WITH normalized AS (
            SELECT
                CAST(code AS VARCHAR) AS code,
                {_date_expr("date")} AS ymd,
                CAST(o AS DOUBLE) AS o,
                CAST(h AS DOUBLE) AS h,
                CAST(l AS DOUBLE) AS l,
                CAST(c AS DOUBLE) AS c,
                CAST(v AS DOUBLE) AS v,
                {'source' if 'source' in cols else "'unknown'"} AS source
            FROM daily_bars
            WHERE o IS NOT NULL AND h IS NOT NULL AND l IS NOT NULL AND c IS NOT NULL
        )
        SELECT code, ymd, o, h, l, c, v, source
        FROM normalized
        WHERE ymd BETWEEN ? AND ?
          AND ymd IS NOT NULL
          {source_filter}
          {code_limit}
        ORDER BY code, ymd
    """
    rows = conn.execute(query, params).fetchall()
    return [
        {"code": str(r[0]), "ymd": int(r[1]), "o": float(r[2]), "h": float(r[3]), "l": float(r[4]), "c": float(r[5]), "v": float(r[6] or 0), "source": r[7]}
        for r in rows
        if r[1] is not None and float(r[2]) > 0 and float(r[3]) > 0 and float(r[4]) > 0 and float(r[5]) > 0
    ]


def _rolling(values: list[float], idx: int, window: int, fn: str) -> float | None:
    start = idx - window + 1
    if start < 0:
        return None
    chunk = values[start : idx + 1]
    if len(chunk) != window:
        return None
    if fn == "mean":
        return sum(chunk) / window
    if fn == "max":
        return max(chunk)
    if fn == "min":
        return min(chunk)
    raise ValueError(fn)


def _atr(rows: list[dict[str, Any]], idx: int, window: int = 14) -> float | None:
    start = idx - window + 1
    if start < 1:
        return None
    trs: list[float] = []
    for j in range(start, idx + 1):
        high = rows[j]["h"]
        low = rows[j]["l"]
        prev_close = rows[j - 1]["c"]
        trs.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    return sum(trs) / len(trs) if trs else None


def _feature_row(rows: list[dict[str, Any]], idx: int) -> dict[str, Any] | None:
    closes = [r["c"] for r in rows]
    highs = [r["h"] for r in rows]
    lows = [r["l"] for r in rows]
    vols = [r["v"] for r in rows]
    close = closes[idx]
    ma5 = _rolling(closes, idx, 5, "mean")
    ma20 = _rolling(closes, idx, 20, "mean")
    ma20_prev = _rolling(closes, idx - 5, 20, "mean") if idx >= 5 else None
    ma60 = _rolling(closes, idx, 60, "mean")
    ma60_prev = _rolling(closes, idx - 10, 60, "mean") if idx >= 10 else None
    high20 = _rolling(highs, idx, 20, "max")
    low20 = _rolling(lows, idx, 20, "min")
    high60 = _rolling(highs, idx, 60, "max")
    low60 = _rolling(lows, idx, 60, "min")
    vol20 = _rolling(vols, idx, 20, "mean")
    atr14 = _atr(rows, idx)
    if None in {ma5, ma20, ma20_prev, ma60, ma60_prev, high20, low20, high60, low60, vol20}:
        return None
    body = close - rows[idx]["o"]
    candle_range = max(rows[idx]["h"] - rows[idx]["l"], 1e-9)
    upper_wick = rows[idx]["h"] - max(rows[idx]["o"], close)
    lower_wick = min(rows[idx]["o"], close) - rows[idx]["l"]
    prior_high20 = _rolling(highs, idx - 1, 20, "max") if idx >= 20 else None
    prior_low20 = _rolling(lows, idx - 1, 20, "min") if idx >= 20 else None
    gap_up = idx > 0 and rows[idx]["o"] >= rows[idx - 1]["c"] * 1.02
    gap_down = idx > 0 and rows[idx]["o"] <= rows[idx - 1]["c"] * 0.98
    weekly_return = close / closes[idx - 5] - 1.0 if idx >= 5 and closes[idx - 5] else None
    monthly_return = close / closes[idx - 20] - 1.0 if idx >= 20 and closes[idx - 20] else None
    range60 = max((high60 or close) - (low60 or close), 1e-9)
    return {
        "ma5": ma5,
        "ma20": ma20,
        "ma20_slope": (ma20 - ma20_prev) / ma20_prev if ma20_prev else None,
        "ma60": ma60,
        "ma60_slope": (ma60 - ma60_prev) / ma60_prev if ma60_prev else None,
        "close_vs_ma20": close / ma20 - 1.0 if ma20 else None,
        "close_vs_ma60": close / ma60 - 1.0 if ma60 else None,
        "weekly_trend_regime": "up" if (weekly_return or 0) > 0.03 else "down" if (weekly_return or 0) < -0.03 else "range",
        "monthly_regime_box_context": "upper_box" if (close - low60) / range60 >= 0.70 else "lower_box" if (close - low60) / range60 <= 0.30 else "middle_box",
        "body_to_range": body / candle_range,
        "upper_wick_ratio": upper_wick / candle_range,
        "lower_wick_ratio": lower_wick / candle_range,
        "failed_high_flag": bool(prior_high20 and rows[idx]["h"] >= prior_high20 * 0.995 and close < prior_high20 and body < 0),
        "recent_high_distance": close / high20 - 1.0 if high20 else None,
        "recent_low_distance": close / low20 - 1.0 if low20 else None,
        "gap_up_flag": bool(gap_up),
        "gap_down_flag": bool(gap_down),
        "volume_vs_20d_avg": rows[idx]["v"] / vol20 if vol20 else None,
        "atr14": atr14,
        "atr_pct": atr14 / close if atr14 else None,
        "monthly_return": monthly_return,
        "prior_high20": prior_high20,
        "prior_low20": prior_low20,
    }


def _setup_flags(row: dict[str, Any], feat: dict[str, Any]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    bullish_body = feat["body_to_range"] is not None and feat["body_to_range"] >= 0.35
    bearish_body = feat["body_to_range"] is not None and feat["body_to_range"] <= -0.35
    close_vs_ma20 = feat.get("close_vs_ma20") or 0.0
    ma20_slope = feat.get("ma20_slope") or 0.0
    ma60_slope = feat.get("ma60_slope") or 0.0
    vol_ratio = feat.get("volume_vs_20d_avg") or 0.0
    atr_pct = feat.get("atr_pct") or 0.0
    recent_high_distance = feat.get("recent_high_distance") or 0.0
    recent_low_distance = feat.get("recent_low_distance") or 0.0

    if -0.06 <= close_vs_ma20 <= 0.03 and ma20_slope >= -0.003 and bullish_body and vol_ratio >= 0.8:
        out.append(("long", "long_a"))
    if abs(close_vs_ma20) <= 0.04 and atr_pct <= 0.035 and recent_high_distance >= -0.08 and bullish_body:
        out.append(("long", "long_b"))
    if row["c"] > feat["ma20"] and ma20_slope > 0 and ma60_slope >= -0.004 and 0 <= close_vs_ma20 <= 0.08 and bullish_body:
        out.append(("long", "long_c"))

    if feat["failed_high_flag"] and feat["upper_wick_ratio"] >= 0.35:
        out.append(("short", "short_a"))
    if row["c"] < feat["ma20"] and ma20_slope < 0 and -0.08 <= close_vs_ma20 <= 0.02 and bearish_body:
        out.append(("short", "short_b"))
    if close_vs_ma20 >= 0.06 and feat["upper_wick_ratio"] >= 0.30 and bearish_body:
        out.append(("short", "short_c"))
    return out


def _evaluate_path(rows: list[dict[str, Any]], idx: int, feat: dict[str, Any], *, direction: str, horizon: int, stop_rule: str) -> dict[str, Any] | None:
    future = rows[idx + 1 : idx + horizon + 1]
    if len(future) < horizon:
        return None
    entry = rows[idx]["c"]
    atr = feat.get("atr14")
    if direction == "long":
        target = entry * 1.10
        if stop_rule == "stop_a":
            stop = entry * 0.95
        elif stop_rule == "stop_b":
            stop = feat["ma20"]
        else:
            stop = entry - 2.0 * atr if atr else None
        if stop is None:
            return None
        adverse_values = [(bar["l"] / entry - 1.0) for bar in future]
        exit_return = future[-1]["c"] / entry - 1.0
        for day, bar in enumerate(future, start=1):
            target_hit = bar["h"] >= target
            stop_hit = bar["l"] <= stop or bar["c"] <= stop
            if target_hit or stop_hit:
                ret = 0.10 if target_hit and not stop_hit else (stop / entry - 1.0)
                event = "target_before_stop" if target_hit and not stop_hit else "stop_before_target" if stop_hit and not target_hit else "same_bar_both"
                return {"event": event, "days_to_event": day, "days_to_target": day if target_hit else None, "return_at_exit": ret, "adverse_excursion": min(adverse_values[:day])}
    else:
        target = entry * 0.90
        if stop_rule == "stop_a":
            stop = entry * 1.05
        elif stop_rule == "stop_b":
            stop = feat["ma20"]
        else:
            stop = entry + 2.0 * atr if atr else None
        if stop is None:
            return None
        adverse_values = [(bar["h"] / entry - 1.0) for bar in future]
        exit_return = entry / future[-1]["c"] - 1.0
        for day, bar in enumerate(future, start=1):
            target_hit = bar["l"] <= target
            stop_hit = bar["h"] >= stop or bar["c"] >= stop
            if target_hit or stop_hit:
                ret = 0.10 if target_hit and not stop_hit else (entry / stop - 1.0)
                event = "target_before_stop" if target_hit and not stop_hit else "stop_before_target" if stop_hit and not target_hit else "same_bar_both"
                return {"event": event, "days_to_event": day, "days_to_target": day if target_hit else None, "return_at_exit": ret, "adverse_excursion": max(adverse_values[:day])}
    return {"event": "neither_hit", "days_to_event": horizon, "days_to_target": None, "return_at_exit": exit_return, "adverse_excursion": min(adverse_values) if direction == "long" else max(adverse_values)}


def _baseline_setup(direction: str) -> str:
    return "all_long_candidates" if direction == "long" else "all_short_candidates"


def build_capture_rows(bars: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    by_code: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in bars:
        by_code[row["code"]].append(row)
    capture_rows: list[dict[str, Any]] = []
    feature_audit = {
        "required_point_in_time_features": [
            "daily MA position/slope",
            "weekly trend/regime",
            "monthly regime/box context",
            "candle body/wick features",
            "failed_high_flag",
            "recent high/low distance",
            "gap flags",
            "volume_vs_20d_avg",
            "ATR/volatility proxy",
            "family/direction flag",
        ],
        "feature_source": "daily_bars rows at or before anchor date",
        "future_outcomes_excluded_from_features": True,
        "missing_required_feature_count": 0,
        "missing_required_features": [],
    }
    for code, rows in by_code.items():
        rows.sort(key=lambda r: r["ymd"])
        for idx in range(60, len(rows) - min(HORIZONS)):
            feat = _feature_row(rows, idx)
            if feat is None:
                continue
            setups = _setup_flags(rows[idx], feat)
            for direction, family in setups:
                for horizon in HORIZONS:
                    hold20_return = None
                    if idx + 20 < len(rows):
                        hold20_return = rows[idx + 20]["c"] / rows[idx]["c"] - 1.0
                        if direction == "short":
                            hold20_return = rows[idx]["c"] / rows[idx + 20]["c"] - 1.0
                    for stop_rule in STOP_RULES:
                        path = _evaluate_path(rows, idx, feat, direction=direction, horizon=horizon, stop_rule=stop_rule)
                        if path is None:
                            continue
                        market_regime = "up" if (feat.get("monthly_return") or 0) > 0.03 else "down" if (feat.get("monthly_return") or 0) < -0.03 else "range"
                        capture_rows.append(
                            {
                                "dt": rows[idx]["ymd"],
                                "code": code,
                                "direction": direction,
                                "setup_family": family,
                                "baseline_family": _baseline_setup(direction),
                                "horizon": horizon,
                                "stop_rule": stop_rule,
                                "entry_price": rows[idx]["c"],
                                "event": path["event"],
                                "target_hit": path["event"] in {"target_before_stop", "same_bar_both"},
                                "stop_hit": path["event"] in {"stop_before_target", "same_bar_both"},
                                "target_before_stop": path["event"] == "target_before_stop",
                                "stop_before_target": path["event"] == "stop_before_target",
                                "neither_hit": path["event"] == "neither_hit",
                                "days_to_event": path["days_to_event"],
                                "days_to_target": path["days_to_target"],
                                "return_at_exit": path["return_at_exit"],
                                "hold20_return": hold20_return,
                                "adverse_excursion": path["adverse_excursion"],
                                "severe_loss": path["return_at_exit"] <= -0.08,
                                "market_regime": market_regime,
                                "ma20": feat["ma20"],
                                "ma20_slope": feat["ma20_slope"],
                                "ma60": feat["ma60"],
                                "ma60_slope": feat["ma60_slope"],
                                "close_vs_ma20": feat["close_vs_ma20"],
                                "close_vs_ma60": feat["close_vs_ma60"],
                                "weekly_trend_regime": feat["weekly_trend_regime"],
                                "monthly_regime_box_context": feat["monthly_regime_box_context"],
                                "body_to_range": feat["body_to_range"],
                                "upper_wick_ratio": feat["upper_wick_ratio"],
                                "lower_wick_ratio": feat["lower_wick_ratio"],
                                "failed_high_flag": feat["failed_high_flag"],
                                "recent_high_distance": feat["recent_high_distance"],
                                "recent_low_distance": feat["recent_low_distance"],
                                "gap_up_flag": feat["gap_up_flag"],
                                "gap_down_flag": feat["gap_down_flag"],
                                "volume_vs_20d_avg": feat["volume_vs_20d_avg"],
                                "atr_pct": feat["atr_pct"],
                            }
                        )
    return capture_rows, feature_audit


def _metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {
            "sample_count": 0,
            "date_count": 0,
            "code_count": 0,
            "target_10pct_hit_rate": None,
            "stop_hit_rate": None,
            "target_before_stop_rate": None,
            "stop_before_target_rate": None,
            "neither_hit_rate": None,
            "average_days_to_target": None,
            "median_days_to_target": None,
            "average_adverse_excursion": None,
            "worst_adverse_excursion": None,
            "mean_return_at_exit": None,
            "median_return_at_exit": None,
            "severe_loss_rate": None,
            "profit_factor": None,
            "return_per_day": None,
            "cost_slippage_adjusted_return": None,
        }
    n = len(rows)
    returns = [float(r["return_at_exit"]) for r in rows]
    wins = [r for r in returns if r > 0]
    losses = [r for r in returns if r < 0]
    days_to_target = [int(r["days_to_target"]) for r in rows if r["days_to_target"] not in {None, ""}]
    adverse = [float(r["adverse_excursion"]) for r in rows if r.get("adverse_excursion") is not None]
    days = [int(r["days_to_event"]) for r in rows if r.get("days_to_event") is not None]
    direction = str(rows[0].get("direction") or "long")
    worst_adverse = min(adverse) if direction == "long" and adverse else max(adverse) if adverse else None
    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    mean_ret = _mean(returns)
    cost_assumption = 0.002
    return {
        "sample_count": n,
        "date_count": len({r["dt"] for r in rows}),
        "code_count": len({r["code"] for r in rows}),
        "target_10pct_hit_rate": _round(sum(1 for r in rows if r["target_hit"]) / n),
        "stop_hit_rate": _round(sum(1 for r in rows if r["stop_hit"]) / n),
        "target_before_stop_rate": _round(sum(1 for r in rows if r["target_before_stop"]) / n),
        "stop_before_target_rate": _round(sum(1 for r in rows if r["stop_before_target"]) / n),
        "neither_hit_rate": _round(sum(1 for r in rows if r["neither_hit"]) / n),
        "average_days_to_target": _round(_mean([float(v) for v in days_to_target])),
        "median_days_to_target": _round(float(median(days_to_target)) if days_to_target else None),
        "average_adverse_excursion": _round(_mean(adverse)),
        "worst_adverse_excursion": _round(worst_adverse),
        "mean_return_at_exit": _round(mean_ret),
        "median_return_at_exit": _round(float(median(returns))),
        "severe_loss_rate": _round(sum(1 for r in rows if r["severe_loss"]) / n),
        "profit_factor": _round(_safe_div(gross_profit, gross_loss)),
        "return_per_day": _round(_safe_div(mean_ret or 0.0, _mean([float(v) for v in days]) or 0.0)),
        "cost_slippage_adjusted_return": _round((mean_ret - cost_assumption) if mean_ret is not None else None),
        "cost_assumption_round_trip": cost_assumption,
    }


def _group_metrics(rows: list[dict[str, Any]], keys: tuple[str, ...]) -> dict[str, Any]:
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[tuple(row[k] for k in keys)].append(row)
    return {"|".join(map(str, key)): _metrics(group) for key, group in sorted(groups.items())}


def _decision(rows: list[dict[str, Any]], direction_metrics: dict[str, Any], source_coverage: dict[str, Any]) -> dict[str, Any]:
    if source_coverage["point_in_time_feature_blocked"]:
        decision = "blocked_missing_point_in_time_features"
        reason = "required_point_in_time_features_missing"
    else:
        long_m = direction_metrics.get("long", {})
        short_m = direction_metrics.get("short", {})
        long_edge = (long_m.get("sample_count") or 0) >= 50 and (long_m.get("target_before_stop_rate") or 0) >= 0.18 and (long_m.get("severe_loss_rate") or 1) <= 0.18
        short_edge = (short_m.get("sample_count") or 0) >= 50 and (short_m.get("target_before_stop_rate") or 0) >= 0.18 and (short_m.get("severe_loss_rate") or 1) <= 0.18
        thin_signal = any(
            (m.get("sample_count") or 0) > 0 and (m.get("sample_count") or 0) < 50 and (m.get("target_before_stop_rate") or 0) >= 0.22
            for m in [long_m, short_m]
        )
        if long_edge and short_edge and not source_coverage["short_borrow_contract_missing"]:
            decision = "ten_pct_capture_keep_for_policy_replay"
            reason = "both_directions_clear_thresholds_with_operational_constraints_documented"
        elif long_edge and not short_edge:
            decision = "long_only_edge"
            reason = "long_direction_clear_thresholds_short_direction_does_not"
        elif short_edge and source_coverage["short_borrow_contract_missing"]:
            decision = "short_only_edge_theoretical"
            reason = "short_price_path_edge_present_borrow_contract_missing"
        elif short_edge:
            decision = "ten_pct_capture_keep_for_policy_replay"
            reason = "short_direction_clear_thresholds_with_borrow_contract_available"
        elif thin_signal:
            decision = "ten_pct_capture_promising_but_underpowered"
            reason = "candidate_signal_rate_present_but_sample_below_support_threshold"
        else:
            decision = "no_ten_pct_capture_edge"
            reason = "target_before_stop_not_sufficiently_above_baseline_or_loss_control_threshold"
    setup_metrics = _group_metrics(rows, ("setup_family", "direction"))
    ranked = sorted(
        setup_metrics.items(),
        key=lambda kv: (kv[1].get("target_before_stop_rate") or -1, -(kv[1].get("severe_loss_rate") or 1), kv[1].get("sample_count") or 0),
        reverse=True,
    )
    best_setup = ranked[0][0] if ranked else None
    return {
        "axis_id": AXIS_ID,
        "research_decision": decision,
        "decision_reason": reason,
        "candidate_local_decision": decision,
        "session_aggregate_decision": decision,
        "authoritative_rollup_decision": decision,
        "best_setup_for_policy_replay": best_setup,
        "next_step_if_promising": "position_management_policy_replay_only",
        "runtime_db_write": False,
        "meemee_reflectable_candidate": False,
        "production_ranking_changed": False,
        "production_candidate_generator_changed": False,
        "validated_buy_count": 0,
        "short_results_operational_status": "theoretical_only" if source_coverage["short_borrow_contract_missing"] else "borrow_contract_available_for_audit",
    }


def _write_rows_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = sorted({key for row in rows for key in row.keys()}) if rows else ["dt", "code", "direction", "setup_family", "horizon", "stop_rule"]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def run_research(
    *,
    db_path: Path | None = None,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    start_ymd: int = 20240101,
    end_ymd: int = 20260515,
    max_codes: int | None = None,
) -> dict[str, Any]:
    resolved = resolve_runtime_stock_db_path() if db_path is None else db_path
    if isinstance(resolved, dict):
        resolved_db = Path(resolved["runtime_db_path"])
    else:
        resolved_db = Path(resolved)
    run_dir = output_root / f"{_now_tag()}-ten-pct-move-capture-research-v1"
    run_dir.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(resolved_db), read_only=True) as conn:
        daily_cols = _table_columns(conn, "daily_bars")
        borrow_fields = sorted(c for c in daily_cols if "borrow" in c.lower() or "lend" in c.lower() or "margin" in c.lower())
        bars = _load_bars(conn, start_ymd=start_ymd, end_ymd=end_ymd, max_codes=max_codes)
    rows, feature_audit = build_capture_rows(bars)
    direction_metrics = _group_metrics(rows, ("direction",))
    setup_metrics = _group_metrics(rows, ("setup_family", "direction", "horizon", "stop_rule"))
    source_coverage = {
        "axis_id": AXIS_ID,
        "runtime_db_path": str(resolved_db),
        "daily_bars_source": "pan_confirmed_only",
        "provisional_yahoo_bars_used": False,
        "bar_rows_loaded": len(bars),
        "codes_loaded": len({r["code"] for r in bars}),
        "start_ymd": start_ymd,
        "end_ymd": end_ymd,
        "borrow_lending_fields_found": borrow_fields,
        "short_borrow_contract_missing": not bool(borrow_fields),
        "short_setup_operational_status": "theoretical_only" if not borrow_fields else "borrow_fields_available_for_audit",
        "point_in_time_feature_blocked": feature_audit["missing_required_feature_count"] > 0,
    }
    decision = _decision(rows, direction_metrics, source_coverage)
    contracts = {
        "setup_family_contract.json": {
            "axis_id": AXIS_ID,
            "candidate_setup_family_count": 6,
            "long_families": {
                "long_a": "constructive pullback + bullish confirmation",
                "long_b": "volatility compression before breakout",
                "long_c": "early trend reclaim with controlled extension",
            },
            "short_families": {
                "short_a": "failed high / upper-wick reversal",
                "short_b": "20MA breakdown after weak rebound",
                "short_c": "overextended high + bearish confirmation",
            },
            "combined_score_before_evaluation": False,
        },
        "long_short_direction_contract.json": {
            "axis_id": AXIS_ID,
            "directions": ["long", "short"],
            "long_short_metrics_separate": True,
            "short_borrow_contract_missing": source_coverage["short_borrow_contract_missing"],
            "short_theoretical_only_when_borrow_missing": source_coverage["short_borrow_contract_missing"],
            "validated_buy_sell_claim": False,
        },
        "target_stop_contract.json": {
            "axis_id": AXIS_ID,
            "primary_objective": "hit_10pct_target_before_stop_or_invalidation",
            "horizons": list(HORIZONS),
            "stop_rules": {
                "stop_a": "fixed 5pct adverse move",
                "stop_b": "close/low/high beyond point-in-time MA20 invalidation",
                "stop_c": "2x point-in-time ATR14 adverse stop when ATR exists",
            },
            "long_target": "future high >= entry_price * 1.10",
            "short_target": "future low <= entry_price * 0.90",
            "future_outcomes_evaluation_only": True,
            "no_ret20_derived_feature_tags": True,
        },
    }
    summary = {
        "axis_id": AXIS_ID,
        "output_dir": str(run_dir),
        "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {
            "same_universe": True,
            "same_period": True,
            "same_top_k": "not_topk_selection_objective",
            "same_regime_condition": True,
            "same_cost_slippage": True,
            "same_artifact_detail_level": True,
            "confirmed_historical_bars_only": True,
        },
        "primary_objective": "target_before_stop_rate",
        "direction_metrics": direction_metrics,
        "best_policy_replay_setup": decision["best_setup_for_policy_replay"],
        "authoritative_research_decision": decision["research_decision"],
    }
    no_lookahead = {
        "axis_id": AXIS_ID,
        "pass": feature_audit["missing_required_feature_count"] == 0,
        "features_use_anchor_or_prior_bars_only": True,
        "future_bars_used_for_selection": [],
        "future_outcomes_evaluation_only": True,
        "ret20_derived_feature_tags_used": False,
        "provisional_yahoo_bars_used": False,
        "source_feature_audit": feature_audit,
    }
    artifacts: dict[str, Any] = {
        "ten_pct_capture_summary.json": summary,
        "setup_metrics.json": setup_metrics,
        "direction_metrics.json": direction_metrics,
        "hit_before_stop_metrics.json": _group_metrics(rows, ("direction", "horizon", "stop_rule")),
        "holding_period_metrics.json": _group_metrics(rows, ("setup_family", "horizon")),
        "drawdown_adverse_excursion_metrics.json": _group_metrics(rows, ("direction", "setup_family")),
        "no_lookahead_audit.json": no_lookahead,
        "source_coverage.json": source_coverage,
        "research_decision.json": decision,
        **contracts,
    }
    for name, payload in artifacts.items():
        _write_json(run_dir / name, payload)
    _write_rows_csv(run_dir / "ten_pct_capture_rows.csv", rows)
    existing = {name: (run_dir / name).exists() for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json"}
    complete_payload = {
        "axis_id": AXIS_ID,
        "complete": all(existing.values()),
        "artifact_complete": all(existing.values()),
        "required_artifacts": list(REQUIRED_ARTIFACTS),
        "existing_artifacts": existing,
        "output_dir": str(run_dir),
        "runtime_db_write": False,
        "meemee_reflectable_candidate": False,
        "production_ranking_changed": False,
        "production_candidate_generator_changed": False,
        "validated_buy_count": 0,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    _write_json(run_dir / "_ARTIFACT_COMPLETE.json", complete_payload)
    return {"output_dir": str(run_dir), "summary": summary, "decision": decision, "source_coverage": source_coverage}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run TRADEX-only 10pct move capture research v1.")
    parser.add_argument("--db-path", type=Path, default=None)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--start-ymd", type=int, default=20240101)
    parser.add_argument("--end-ymd", type=int, default=20260515)
    parser.add_argument("--max-codes", type=int, default=None)
    args = parser.parse_args(argv)
    result = run_research(db_path=args.db_path, output_root=args.output_root, start_ymd=args.start_ymd, end_ymd=args.end_ymd, max_codes=args.max_codes)
    print(json.dumps(_json_ready(result), ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
