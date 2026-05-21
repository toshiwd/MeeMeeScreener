from __future__ import annotations

import argparse
import json
import math
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.runtime_stock_db_contract import resolve_runtime_stock_db_path


DEFAULT_OUTPUT_ROOT = Path("G:/Tradex/failed_high_retest_sell_signal_v1")
HORIZONS = (5, 10, 20)
SEVERE_ADVERSE_UP_RET = 0.10
PATH_MAX_LOOKAHEAD = 20


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _ymd(value: Any) -> int | None:
    try:
        raw = int(value)
    except (TypeError, ValueError):
        return None
    if len(str(abs(raw))) == 8:
        return raw
    try:
        return int(datetime.fromtimestamp(raw, timezone.utc).strftime("%Y%m%d"))
    except (OverflowError, OSError, ValueError):
        return None


def _finite(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _mean(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2


def _round(value: float | None, digits: int = 8) -> float | None:
    return None if value is None else round(float(value), digits)


def _safe_rate(num: int, den: int) -> float | None:
    return None if den <= 0 else num / den


def _summarize(values: list[float]) -> dict[str, Any]:
    return {
        "count": len(values),
        "mean": _round(_mean(values)),
        "median": _round(_median(values)),
        "win_rate_short": _round(_safe_rate(sum(1 for value in values if value > 0), len(values))),
        "loss_rate_short": _round(_safe_rate(sum(1 for value in values if value < 0), len(values))),
    }


def _sma(history: list[dict[str, Any]], idx: int, period: int) -> float | None:
    if idx + 1 < period:
        return None
    values = [row["c"] for row in history[idx - period + 1 : idx + 1]]
    return _mean(values)


def _summarize_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {"row_count": len(rows)}
    for horizon in HORIZONS:
        key = f"short_ret_{horizon}"
        values = [float(row[key]) for row in rows if row.get(key) is not None]
        out[key] = _summarize(values)
    adverse = [
        float(row["future_up_ret_20"])
        for row in rows
        if row.get("future_up_ret_20") is not None
    ]
    out["severe_adverse_up_rate20"] = _round(
        _safe_rate(sum(1 for value in adverse if value >= SEVERE_ADVERSE_UP_RET), len(adverse))
    )
    return out


def _evaluate_short_path(
    history: list[dict[str, Any]],
    anchor_idx: int,
    *,
    profit_targets: tuple[float, ...] = (0.03, 0.05),
    max_lookahead: int = PATH_MAX_LOOKAHEAD,
) -> dict[str, Any]:
    entry = history[anchor_idx]["c"]
    if entry <= 0:
        return {}
    trough = entry
    best_drop = 0.0
    first_profit_hit: dict[str, int | None] = {f"profit_hit_{int(target * 100)}pct_day": None for target in profit_targets}
    exit_reason = "no_edge"
    exit_day = None
    max_adverse_up = 0.0
    for offset in range(1, max_lookahead + 1):
        idx = anchor_idx + offset
        if idx >= len(history):
            break
        bar = history[idx]
        trough = min(trough, bar["l"])
        best_drop = max(best_drop, (entry - trough) / entry)
        max_adverse_up = max(max_adverse_up, (bar["h"] / entry) - 1.0)
        for target in profit_targets:
            key = f"profit_hit_{int(target * 100)}pct_day"
            if first_profit_hit[key] is None and best_drop >= target:
                first_profit_hit[key] = offset
        ma7 = _sma(history, idx, 7)
        rebound_from_trough = (bar["c"] - trough) / max(entry - trough, 1e-9) if trough < entry else 0.0
        if best_drop >= 0.03 and (rebound_from_trough >= 0.5 or (ma7 is not None and bar["c"] > ma7)):
            exit_reason = "rebound_after_drop"
            exit_day = offset
            break
        if best_drop < 0.03 and max_adverse_up >= 0.05:
            exit_reason = "adverse_rebound_before_profit"
            exit_day = offset
            break
    if exit_day is None:
        if best_drop >= 0.03:
            exit_reason = "slow_bleed_or_profit_available"
        elif max_adverse_up >= 0.03:
            exit_reason = "adverse_without_profit"
    out: dict[str, Any] = {
        "path_best_drop_20": best_drop,
        "path_max_adverse_up_20": max_adverse_up,
        "path_exit_reason": exit_reason,
        "path_exit_day": exit_day,
    }
    out.update(first_profit_hit)
    return out


def _summarize_path(rows: list[dict[str, Any]]) -> dict[str, Any]:
    best_drops = [float(row["path_best_drop_20"]) for row in rows if row.get("path_best_drop_20") is not None]
    adverse = [float(row["path_max_adverse_up_20"]) for row in rows if row.get("path_max_adverse_up_20") is not None]
    reasons: dict[str, int] = defaultdict(int)
    for row in rows:
        reason = row.get("path_exit_reason")
        if reason:
            reasons[str(reason)] += 1
    return {
        "row_count": len(rows),
        "best_drop_20": _summarize(best_drops),
        "max_adverse_up_20": _summarize(adverse),
        "profit_hit_3pct_rate": _round(_safe_rate(sum(1 for row in rows if row.get("profit_hit_3pct_day") is not None), len(rows))),
        "profit_hit_5pct_rate": _round(_safe_rate(sum(1 for row in rows if row.get("profit_hit_5pct_day") is not None), len(rows))),
        "exit_reason_counts": dict(sorted(reasons.items())),
    }


def _signal_for_anchor(history: list[dict[str, Any]], anchor_idx: int) -> dict[str, Any] | None:
    if anchor_idx < 80:
        return None
    latest = history[anchor_idx]
    prev = history[anchor_idx - 1]
    recent = history[max(0, anchor_idx - 20) : anchor_idx]
    prior = history[: max(0, anchor_idx - 20)]
    if len(recent) < 5 or len(prior) < 40:
        return None
    prior_high = max(row["h"] for row in prior)
    recent_retest_high = max(row["h"] for row in recent)
    closes = [row["c"] for row in history[: anchor_idx + 1]]
    ma7 = _mean(closes[-7:])
    ma20 = _mean(closes[-20:])
    if prior_high <= 0 or ma7 is None or ma20 is None:
        return None
    attempted_prior_high = recent_retest_high >= prior_high * 0.88 and recent_retest_high < prior_high * 1.01
    rolled_over_from_retest = latest["c"] <= recent_retest_high * 0.94
    below_short_mas = latest["c"] < ma7 and latest["c"] < ma20
    still_falling = latest["c"] < prev["c"]
    if not (attempted_prior_high and rolled_over_from_retest and below_short_mas and still_falling):
        return None
    retest_ratio = recent_retest_high / prior_high
    rollover_ratio = latest["c"] / recent_retest_high
    return {
        "prior_high": prior_high,
        "recent_retest_high": recent_retest_high,
        "retest_ratio": retest_ratio,
        "rollover_ratio": rollover_ratio,
        "ma7": ma7,
        "ma20": ma20,
        "signal_score": (retest_ratio - 0.88) + (1.0 - rollover_ratio) + max(0.0, (ma20 - latest["c"]) / ma20),
    }


def _signal_for_anchor_rejection(
    history: list[dict[str, Any]],
    anchor_idx: int,
    *,
    prior_high_lookback_sessions: int,
    min_peak_age_sessions: int,
) -> dict[str, Any] | None:
    if anchor_idx < 80:
        return None
    anchor = history[anchor_idx]
    prior = history[max(0, anchor_idx - prior_high_lookback_sessions) : anchor_idx]
    if len(prior) < 40:
        return None
    prior_high = max(row["h"] for row in prior)
    prior_high_local_idx = max(range(len(prior)), key=lambda idx: prior[idx]["h"])
    prior_high_idx = max(0, anchor_idx - prior_high_lookback_sessions) + prior_high_local_idx
    peak_age_sessions = anchor_idx - prior_high_idx
    if prior_high <= 0 or anchor["h"] <= 0:
        return None
    if peak_age_sessions < min_peak_age_sessions:
        return None
    left = history[max(0, prior_high_idx - 10) : prior_high_idx]
    right = history[prior_high_idx + 1 : min(anchor_idx, prior_high_idx + 11)]
    if len(left) < 5 or len(right) < 5:
        return None
    left_high = max(row["h"] for row in left)
    right_high = max(row["h"] for row in right)
    peak_prominence = min((prior_high - left_high) / prior_high, (prior_high - right_high) / prior_high)
    if peak_prominence < 0.03:
        return None
    body = anchor["c"] - anchor["o"]
    range_size = anchor["h"] - anchor["l"]
    if range_size <= 0:
        return None
    retest_ratio = anchor["h"] / prior_high
    close_from_high = anchor["c"] / anchor["h"]
    close_pos = (anchor["c"] - anchor["l"]) / range_size
    bearish_body_ratio = (anchor["o"] - anchor["c"]) / range_size
    attempted_prior_high = retest_ratio >= 0.88 and retest_ratio < 1.01
    bearish_rejection = body < 0 and bearish_body_ratio >= 0.45 and close_pos <= 0.35 and close_from_high <= 0.93
    if not (attempted_prior_high and bearish_rejection):
        return None
    return {
        "prior_high": prior_high,
        "recent_retest_high": anchor["h"],
        "retest_ratio": retest_ratio,
        "rollover_ratio": close_from_high,
        "ma7": None,
        "ma20": None,
        "anchor_body_ratio": bearish_body_ratio,
        "anchor_close_pos": close_pos,
        "prior_peak_age_sessions": peak_age_sessions,
        "prior_peak_prominence": peak_prominence,
        "signal_score": (retest_ratio - 0.88) + (1.0 - close_from_high) + bearish_body_ratio + (0.35 - close_pos),
        "prior_high_lookback_sessions": prior_high_lookback_sessions,
    }


def _signal_for_mountain_retest(
    history: list[dict[str, Any]],
    anchor_idx: int,
    *,
    prior_high_lookback_sessions: int,
    min_peak_age_sessions: int,
    require_anchor_below_ma7: bool,
    require_next_day_no_full_reclaim: bool,
    confirmation_reclaim_days: int,
    max_ma20_slope_20: float | None,
    min_retest_ratio: float,
    max_anchor_drop_pct: float | None,
    min_prior_peak_runup: float,
    require_prior_peak_above_ma200: bool,
    min_prior_peak_local_prominence: float,
) -> dict[str, Any] | None:
    if anchor_idx < 80:
        return None
    anchor = history[anchor_idx]
    prior_start = max(0, anchor_idx - prior_high_lookback_sessions)
    prior = history[prior_start:anchor_idx]
    if len(prior) < 40:
        return None
    prior_high_local_idx = max(range(len(prior)), key=lambda idx: prior[idx]["h"])
    prior_high_idx = prior_start + prior_high_local_idx
    peak_age_sessions = anchor_idx - prior_high_idx
    if peak_age_sessions < min_peak_age_sessions:
        return None
    prior_high = history[prior_high_idx]["h"]
    if prior_high <= 0 or anchor["h"] <= 0:
        return None
    prior_peak_ma200 = _sma(history, prior_high_idx, 200)
    prior_peak_above_ma200 = prior_peak_ma200 is not None and prior_high > prior_peak_ma200
    if require_prior_peak_above_ma200 and not prior_peak_above_ma200:
        return None
    left_peak_window = history[max(0, prior_high_idx - 20) : prior_high_idx]
    right_peak_window = history[prior_high_idx + 1 : min(anchor_idx, prior_high_idx + 21)]
    if len(left_peak_window) < 5 or len(right_peak_window) < 5:
        return None
    left_peak_high = max(row["h"] for row in left_peak_window)
    right_peak_high = max(row["h"] for row in right_peak_window)
    prior_peak_local_prominence = min(
        (prior_high - left_peak_high) / prior_high,
        (prior_high - right_peak_high) / prior_high,
    )
    if prior_peak_local_prominence < min_prior_peak_local_prominence:
        return None

    before_peak = history[max(0, prior_high_idx - 60) : prior_high_idx]
    if len(before_peak) < 20:
        return None
    pre_peak_low = min(row["l"] for row in before_peak)
    prior_peak_runup = (prior_high - pre_peak_low) / pre_peak_low if pre_peak_low > 0 else None
    if prior_peak_runup is None or prior_peak_runup < min_prior_peak_runup:
        return None

    after_peak = history[prior_high_idx + 1 : anchor_idx]
    if len(after_peak) < 10:
        return None
    pullback_low = min(row["l"] for row in after_peak)
    pullback_depth = (prior_high - pullback_low) / prior_high
    if pullback_depth < 0.12:
        return None

    pre_anchor = history[max(prior_high_idx + 1, anchor_idx - 12) : anchor_idx]
    pre_anchor_high = max((row["h"] for row in pre_anchor), default=0.0)
    if pre_anchor_high >= prior_high * 1.01:
        return None

    body = anchor["c"] - anchor["o"]
    range_size = anchor["h"] - anchor["l"]
    if range_size <= 0:
        return None
    retest_ratio = anchor["h"] / prior_high
    close_from_high = anchor["c"] / anchor["h"]
    close_pos = (anchor["c"] - anchor["l"]) / range_size
    bearish_body_ratio = (anchor["o"] - anchor["c"]) / range_size
    anchor_drop_pct = (anchor["o"] - anchor["c"]) / anchor["o"] if anchor["o"] > 0 else None
    if max_anchor_drop_pct is not None and anchor_drop_pct is not None and anchor_drop_pct > max_anchor_drop_pct:
        return None
    attempted_prior_high = retest_ratio >= min_retest_ratio and retest_ratio < 1.01
    bearish_rejection = body < 0 and bearish_body_ratio >= 0.35 and close_pos <= 0.45 and close_from_high <= 0.95
    if not (attempted_prior_high and bearish_rejection):
        return None
    ma7 = _sma(history, anchor_idx, 7)
    ma20 = _sma(history, anchor_idx, 20)
    ma20_prev_20 = _sma(history, anchor_idx - 20, 20) if anchor_idx >= 20 else None
    ma20_slope_20 = None
    if ma20 is not None and ma20_prev_20 is not None and ma20_prev_20 > 0:
        ma20_slope_20 = (ma20 / ma20_prev_20) - 1.0
    if max_ma20_slope_20 is not None and ma20_slope_20 is not None and ma20_slope_20 > max_ma20_slope_20:
        return None
    anchor_below_ma7 = bool(ma7 is not None and anchor["c"] < ma7)
    if require_anchor_below_ma7 and not anchor_below_ma7:
        return None
    next_day_reclaim = None
    confirmation_reclaim = False
    confirmation_reclaim_day = None
    if anchor_idx + 1 < len(history):
        next_day = history[anchor_idx + 1]
        next_day_reclaim = bool(next_day["c"] >= anchor["o"] and next_day["c"] > next_day["o"])
    if confirmation_reclaim_days > 0:
        for offset in range(1, confirmation_reclaim_days + 1):
            if anchor_idx + offset >= len(history):
                break
            follow = history[anchor_idx + offset]
            if follow["c"] >= anchor["o"]:
                confirmation_reclaim = True
                confirmation_reclaim_day = offset
                break
        if require_next_day_no_full_reclaim and confirmation_reclaim:
            return None
    return {
        "prior_high": prior_high,
        "recent_retest_high": anchor["h"],
        "retest_ratio": retest_ratio,
        "rollover_ratio": close_from_high,
        "ma7": ma7,
        "ma20": ma20,
        "ma20_slope_20": ma20_slope_20,
        "max_ma20_slope_20": max_ma20_slope_20,
        "min_retest_ratio": min_retest_ratio,
        "anchor_below_ma7": anchor_below_ma7,
        "next_day_full_reclaim_bull": next_day_reclaim,
        "confirmation_reclaim": confirmation_reclaim,
        "confirmation_reclaim_day": confirmation_reclaim_day,
        "confirmation_reclaim_days": confirmation_reclaim_days,
        "anchor_body_ratio": bearish_body_ratio,
        "anchor_drop_pct": anchor_drop_pct,
        "anchor_close_pos": close_pos,
        "prior_peak_age_sessions": peak_age_sessions,
        "prior_peak_prominence": pullback_depth,
        "prior_peak_runup": prior_peak_runup,
        "min_prior_peak_runup": min_prior_peak_runup,
        "prior_peak_ma200": prior_peak_ma200,
        "prior_peak_above_ma200": prior_peak_above_ma200,
        "require_prior_peak_above_ma200": require_prior_peak_above_ma200,
        "prior_peak_local_prominence": prior_peak_local_prominence,
        "min_prior_peak_local_prominence": min_prior_peak_local_prominence,
        "pullback_depth_from_prior_peak": pullback_depth,
        "signal_score": (retest_ratio - min_retest_ratio) + (1.0 - close_from_high) + bearish_body_ratio + pullback_depth,
        "prior_high_lookback_sessions": prior_high_lookback_sessions,
    }


def _load_universe_codes(path: Path | None) -> set[str] | None:
    if path is None:
        return None
    codes: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        code = line.strip()
        if code:
            codes.add(code)
    return codes


def _load_bars(
    db_path: Path,
    *,
    start_dt: int,
    end_dt: int,
    universe_codes: set[str] | None,
) -> dict[str, list[dict[str, Any]]]:
    with duckdb.connect(str(db_path), read_only=True) as con:
        rows = con.execute(
            """
            SELECT code, date, o, h, l, c, v
            FROM daily_bars
            WHERE COALESCE(source, 'pan') = 'pan'
            ORDER BY code, date
            """
        ).fetchall()
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    warmup_start = start_dt - 10000
    for code, date_value, open_, high, low, close, volume in rows:
        ymd = _ymd(date_value)
        code_text = str(code)
        if universe_codes is not None and code_text not in universe_codes:
            continue
        o = _finite(open_)
        h = _finite(high)
        l = _finite(low)
        c = _finite(close)
        if ymd is None or o is None or h is None or l is None or c is None:
            continue
        if ymd < warmup_start or ymd > end_dt:
            continue
        grouped[code_text].append({"code": code_text, "dt": ymd, "o": o, "h": h, "l": l, "c": c, "v": _finite(volume)})
    return grouped


def _build_ledgers(
    grouped: dict[str, list[dict[str, Any]]],
    *,
    start_dt: int,
    end_dt: int,
    signal_mode: str,
    prior_high_lookback_sessions: int,
    min_peak_age_sessions: int,
    require_anchor_below_ma7: bool,
    require_next_day_no_full_reclaim: bool,
    confirmation_reclaim_days: int,
    max_ma20_slope_20: float | None,
    min_retest_ratio: float,
    max_anchor_drop_pct: float | None,
    min_prior_peak_runup: float,
    require_prior_peak_above_ma200: bool,
    min_prior_peak_local_prominence: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    signal_rows: list[dict[str, Any]] = []
    baseline_rows: list[dict[str, Any]] = []
    for code, bars in grouped.items():
        if len(bars) < 101:
            continue
        for idx, anchor in enumerate(bars):
            if anchor["dt"] < start_dt or anchor["dt"] > end_dt:
                continue
            if idx + max(HORIZONS) >= len(bars):
                continue
            row = {
                "dt": anchor["dt"],
                "month": anchor["dt"] // 100,
                "code": code,
                "close": anchor["c"],
            }
            for horizon in HORIZONS:
                future_close = bars[idx + horizon]["c"]
                up_ret = (future_close / anchor["c"]) - 1.0
                row[f"future_up_ret_{horizon}"] = up_ret
                row[f"short_ret_{horizon}"] = -up_ret
            baseline_rows.append(dict(row))
            signal = (
                _signal_for_anchor_rejection(
                    bars,
                    idx,
                    prior_high_lookback_sessions=prior_high_lookback_sessions,
                    min_peak_age_sessions=min_peak_age_sessions,
                )
                if signal_mode == "anchor_rejection"
                else _signal_for_mountain_retest(
                    bars,
                    idx,
                    prior_high_lookback_sessions=prior_high_lookback_sessions,
                    min_peak_age_sessions=min_peak_age_sessions,
                    require_anchor_below_ma7=require_anchor_below_ma7,
                    require_next_day_no_full_reclaim=require_next_day_no_full_reclaim,
                    confirmation_reclaim_days=confirmation_reclaim_days,
                    max_ma20_slope_20=max_ma20_slope_20,
                    min_retest_ratio=min_retest_ratio,
                    max_anchor_drop_pct=max_anchor_drop_pct,
                    min_prior_peak_runup=min_prior_peak_runup,
                    require_prior_peak_above_ma200=require_prior_peak_above_ma200,
                    min_prior_peak_local_prominence=min_prior_peak_local_prominence,
                )
                if signal_mode == "mountain_retest"
                else _signal_for_anchor(bars, idx)
            )
            if signal is not None:
                signal_rows.append({**row, **signal, **_evaluate_short_path(bars, idx)})
    return signal_rows, baseline_rows


def _same_date_delta(signal_rows: list[dict[str, Any]], baseline_rows: list[dict[str, Any]]) -> dict[str, Any]:
    baseline_by_date: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in baseline_rows:
        baseline_by_date[int(row["dt"])].append(row)
    signal_by_date: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in signal_rows:
        signal_by_date[int(row["dt"])].append(row)
    rows = []
    for dt, signals in sorted(signal_by_date.items()):
        base = baseline_by_date.get(dt, [])
        if not base:
            continue
        sig_ret = _mean([float(row["short_ret_20"]) for row in signals])
        base_ret = _mean([float(row["short_ret_20"]) for row in base])
        if sig_ret is None or base_ret is None:
            continue
        rows.append({"dt": dt, "signal_count": len(signals), "baseline_count": len(base), "short_ret20_delta": sig_ret - base_ret})
    deltas = [float(row["short_ret20_delta"]) for row in rows]
    return {
        "date_count": len(rows),
        "mean_same_date_short_ret20_delta": _round(_mean(deltas)),
        "median_same_date_short_ret20_delta": _round(_median(deltas)),
        "positive_date_rate": _round(_safe_rate(sum(1 for value in deltas if value > 0), len(deltas))),
        "sample": rows[-20:],
    }


def _topk_summary(signal_rows: list[dict[str, Any]], baseline_rows: list[dict[str, Any]], *, k: int) -> dict[str, Any]:
    signal_by_date: dict[int, list[dict[str, Any]]] = defaultdict(list)
    baseline_by_date: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in signal_rows:
        signal_by_date[int(row["dt"])].append(row)
    for row in baseline_rows:
        baseline_by_date[int(row["dt"])].append(row)
    selected: list[dict[str, Any]] = []
    baseline_same_dates: list[dict[str, Any]] = []
    for dt, rows in signal_by_date.items():
        ordered = sorted(rows, key=lambda row: (-float(row.get("signal_score") or 0.0), str(row["code"])))
        selected.extend(ordered[:k])
        baseline_same_dates.extend(baseline_by_date.get(dt, []))
    summary = _summarize_rows(selected)
    base_summary = _summarize_rows(baseline_same_dates)
    summary["same_date_baseline"] = base_summary
    sig_mean = summary["short_ret_20"]["mean"]
    base_mean = base_summary["short_ret_20"]["mean"]
    summary["short_ret20_delta_vs_same_date_universe"] = _round(
        None if sig_mean is None or base_mean is None else sig_mean - base_mean
    )
    return summary


def _month_summary(signal_rows: list[dict[str, Any]], baseline_rows: list[dict[str, Any]]) -> dict[str, Any]:
    sig_by_month: dict[int, list[dict[str, Any]]] = defaultdict(list)
    base_by_month: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in signal_rows:
        sig_by_month[int(row["month"])].append(row)
    for row in baseline_rows:
        base_by_month[int(row["month"])].append(row)
    rows = []
    for month, sig_rows in sorted(sig_by_month.items()):
        base_rows = base_by_month.get(month, [])
        sig_mean = _mean([float(row["short_ret_20"]) for row in sig_rows])
        base_mean = _mean([float(row["short_ret_20"]) for row in base_rows])
        rows.append({
            "month": month,
            "signal_count": len(sig_rows),
            "baseline_count": len(base_rows),
            "short_ret20_mean": _round(sig_mean),
            "baseline_short_ret20_mean": _round(base_mean),
            "delta": _round(None if sig_mean is None or base_mean is None else sig_mean - base_mean),
        })
    valid = [row for row in rows if row.get("delta") is not None and int(row["signal_count"]) >= 5]
    return {
        "month_count": len(rows),
        "months_with_at_least_5_signals": len(valid),
        "positive_month_rate_min5": _round(_safe_rate(sum(1 for row in valid if float(row["delta"]) > 0), len(valid))),
        "rows": rows,
    }


def _decision(compare: dict[str, Any]) -> dict[str, Any]:
    signal = compare["signal_summary"]
    top5 = compare["topk"]["top5"]
    same_date = compare["same_date_delta"]
    month = compare["month_summary"]
    count = int(signal.get("row_count") or 0)
    avg_delta = same_date.get("mean_same_date_short_ret20_delta")
    top5_delta = top5.get("short_ret20_delta_vs_same_date_universe")
    severe_signal = signal.get("severe_adverse_up_rate20")
    severe_base = compare["baseline_same_date_summary"].get("severe_adverse_up_rate20")
    severe_delta = None if severe_signal is None or severe_base is None else float(severe_signal) - float(severe_base)
    positive_month_rate = month.get("positive_month_rate_min5")

    keep = (
        count >= 100
        and avg_delta is not None and float(avg_delta) >= 0.01
        and top5_delta is not None and float(top5_delta) >= 0.01
        and severe_delta is not None and severe_delta <= 0.0
        and positive_month_rate is not None and float(positive_month_rate) >= 0.60
    )
    hold = (
        count >= 50
        and avg_delta is not None and float(avg_delta) > 0
        and positive_month_rate is not None and float(positive_month_rate) >= 0.45
    )
    if keep:
        judgment = "keep"
        reason = "same_date_topk_and_month_breadth_pass"
        ranking_integration_view = "eligible_for_shadow_rerank_design_only"
    elif hold:
        judgment = "hold"
        reason = "directional_edge_present_but_topk_or_risk_breadth_not_keep_grade"
        ranking_integration_view = "watchlist_or_soft_demote_candidate_only"
    else:
        judgment = "drop"
        reason = "same_date_edge_or_breadth_insufficient"
        ranking_integration_view = "do_not_integrate_into_ranking"
    return {
        "judgment": judgment,
        "reason": reason,
        "ranking_integration_view": ranking_integration_view,
        "candidate_local_decision": judgment,
        "session_aggregate_decision": judgment,
        "authoritative_rollup_decision": judgment,
        "meemee_reflectable": False,
        "production_ranking_changed": False,
        "publish_candidate_review_ready": False,
        "silent_fallback_used": False,
        "decision_inputs": {
            "signal_count": count,
            "mean_same_date_short_ret20_delta": avg_delta,
            "top5_short_ret20_delta_vs_same_date_universe": top5_delta,
            "severe_adverse_up_rate20_delta": _round(severe_delta),
            "positive_month_rate_min5": positive_month_rate,
        },
    }


def run_validation(
    *,
    db_path: Path | None,
    output_root: Path,
    start_dt: int,
    end_dt: int,
    run_id: str | None = None,
    signal_mode: str = "post_retest_rollover",
    prior_high_lookback_sessions: int = 252,
    min_peak_age_sessions: int = 20,
    require_anchor_below_ma7: bool = False,
    require_next_day_no_full_reclaim: bool = False,
    confirmation_reclaim_days: int = 1,
    max_ma20_slope_20: float | None = None,
    min_retest_ratio: float = 0.88,
    max_anchor_drop_pct: float | None = None,
    min_prior_peak_runup: float = 0.0,
    require_prior_peak_above_ma200: bool = False,
    min_prior_peak_local_prominence: float = 0.0,
    universe_codes_path: Path | None = None,
) -> dict[str, Any]:
    resolved_db = Path(db_path) if db_path is not None else Path(resolve_runtime_stock_db_path())
    run_dir = output_root / (run_id or f"{_now_tag()}-failed-high-retest-sell-signal-v1")
    run_dir.mkdir(parents=True, exist_ok=True)

    universe_codes = _load_universe_codes(universe_codes_path)
    grouped = _load_bars(resolved_db, start_dt=start_dt, end_dt=end_dt, universe_codes=universe_codes)
    signal_rows, baseline_rows = _build_ledgers(
        grouped,
        start_dt=start_dt,
        end_dt=end_dt,
        signal_mode=signal_mode,
        prior_high_lookback_sessions=prior_high_lookback_sessions,
        min_peak_age_sessions=min_peak_age_sessions,
        require_anchor_below_ma7=require_anchor_below_ma7,
        require_next_day_no_full_reclaim=require_next_day_no_full_reclaim,
        confirmation_reclaim_days=confirmation_reclaim_days,
        max_ma20_slope_20=max_ma20_slope_20,
        min_retest_ratio=min_retest_ratio,
        max_anchor_drop_pct=max_anchor_drop_pct,
        min_prior_peak_runup=min_prior_peak_runup,
        require_prior_peak_above_ma200=require_prior_peak_above_ma200,
        min_prior_peak_local_prominence=min_prior_peak_local_prominence,
    )
    signal_dates = {int(row["dt"]) for row in signal_rows}
    baseline_same_dates = [row for row in baseline_rows if int(row["dt"]) in signal_dates]

    compare = {
        "schema_version": "failed_high_retest_sell_signal_v1.compare.v1",
        "run_dir": str(run_dir),
        "db_path": str(resolved_db),
        "fixed_evaluation_conditions": {
            "research_axis": "failed_high_retest_sell_signal_v1",
            "signal_mode": signal_mode,
            "prior_high_lookback_sessions": prior_high_lookback_sessions,
            "min_peak_age_sessions": min_peak_age_sessions,
            "require_anchor_below_ma7": require_anchor_below_ma7,
            "require_next_day_no_full_reclaim": require_next_day_no_full_reclaim,
            "confirmation_reclaim_days": confirmation_reclaim_days,
            "max_ma20_slope_20": max_ma20_slope_20,
            "min_retest_ratio": min_retest_ratio,
            "max_anchor_drop_pct": max_anchor_drop_pct,
            "min_prior_peak_runup": min_prior_peak_runup,
            "require_prior_peak_above_ma200": require_prior_peak_above_ma200,
            "min_prior_peak_local_prominence": min_prior_peak_local_prominence,
            "universe": "all confirmed pan daily_bars symbols with enough history and future horizon",
            "universe_codes_path": str(universe_codes_path) if universe_codes_path is not None else None,
            "universe_code_count": len(universe_codes) if universe_codes is not None else None,
            "period": {"start_dt": start_dt, "end_dt": end_dt},
            "top_k": [5, 10],
            "regime_condition": "none",
            "cost_slippage": "not_applied_directional_signal_pretest",
            "artifact_detail_level": "compare_json_decision_json_signal_ledger_sample",
            "feature_lookahead": "features use bars up to anchor date only",
            "future_labels_used_for_evaluation_only": True,
        },
        "signal_definition": {
            "mode": signal_mode,
            "post_retest_rollover": {
                "prior_window": "all bars before recent 20 sessions",
                "recent_retest_window": "20 sessions before anchor, excluding anchor",
                "attempted_prior_high": "recent_retest_high >= prior_high * 0.88 and < prior_high * 1.01",
                "rollover": "anchor_close <= recent_retest_high * 0.94",
                "short_ma_break": "anchor_close < ma7 and anchor_close < ma20",
                "falling": "anchor_close < previous_close",
            },
            "anchor_rejection": {
                "prior_window": f"last {prior_high_lookback_sessions} sessions before anchor",
                "prior_peak_shape": "prior high must be at least min_peak_age_sessions old and at least 3 percent above both nearby 10-session left and right highs",
                "attempted_prior_high": "anchor_high >= prior_high * min_retest_ratio and < prior_high * 1.01",
                "bearish_rejection": "anchor is bearish, body/range >= 0.45, close in lower 35 percent of range, close <= 93 percent of high",
            },
            "mountain_retest": {
                "prior_window": f"last {prior_high_lookback_sessions} sessions before anchor",
                "prior_mountain": "highest high is at least min_peak_age_sessions old and followed by at least 12 percent pullback before anchor",
                "attempted_prior_high": "anchor_high >= prior_high * 0.88 and < prior_high * 1.01",
                "bearish_rejection": "anchor is bearish, body/range >= 0.35, close in lower 45 percent of range, close <= 95 percent of high",
                "pre_anchor_no_breakout": "last 12 pre-anchor sessions did not break prior_high by more than 1 percent",
                "confirmation_filters": {
                    "require_anchor_below_ma7": "anchor close must be below same-day 7MA when enabled",
                    "require_next_day_no_full_reclaim": "next confirmation_reclaim_days sessions must not close back above anchor open when enabled",
                    "max_ma20_slope_20": "exclude when same-day 20MA is rising more than this ratio versus 20 sessions earlier when set",
                    "max_anchor_drop_pct": "exclude when anchor open-to-close drop is larger than this ratio when set",
                    "min_prior_peak_runup": "prior high must be preceded by at least this ratio of run-up from the prior 60-session low when set",
                    "require_prior_peak_above_ma200": "prior high must be above same-day 200MA when enabled",
                    "min_prior_peak_local_prominence": "prior high must exceed both nearby 20-session left and right highs by at least this ratio",
                },
            },
        },
        "coverage": {
            "symbol_count": len(grouped),
            "baseline_row_count": len(baseline_rows),
            "signal_row_count": len(signal_rows),
            "signal_date_count": len(signal_dates),
            "baseline_same_date_row_count": len(baseline_same_dates),
        },
        "signal_summary": _summarize_rows(signal_rows),
        "path_summary": _summarize_path(signal_rows),
        "baseline_same_date_summary": _summarize_rows(baseline_same_dates),
        "same_date_delta": _same_date_delta(signal_rows, baseline_rows),
        "topk": {
            "top5": _topk_summary(signal_rows, baseline_rows, k=5),
            "top10": _topk_summary(signal_rows, baseline_rows, k=10),
        },
        "month_summary": _month_summary(signal_rows, baseline_rows),
        "non_scope": [
            "MeeMee UI/runtime changes",
            "production ranking changes",
            "TRADEX publish/champion changes",
            "live sell signal",
            "threshold tuning after validation",
        ],
    }
    decision = {
        "schema_version": "failed_high_retest_sell_signal_v1.decision.v1",
        **_decision(compare),
        "authoritative_compare": str(run_dir / "compare.json"),
    }
    compare["decision"] = decision

    ledger_sample = sorted(signal_rows, key=lambda row: (int(row["dt"]), str(row["code"])))[:500]
    (run_dir / "compare.json").write_text(json.dumps(compare, ensure_ascii=False, indent=2), encoding="utf-8")
    (run_dir / "research_decision.json").write_text(json.dumps(decision, ensure_ascii=False, indent=2), encoding="utf-8")
    (run_dir / "signal_ledger_sample.json").write_text(json.dumps(ledger_sample, ensure_ascii=False, indent=2), encoding="utf-8")
    return compare


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", default=None)
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--start-dt", type=int, default=20240101)
    parser.add_argument("--end-dt", type=int, default=20260519)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--signal-mode", choices=["post_retest_rollover", "anchor_rejection", "mountain_retest"], default="post_retest_rollover")
    parser.add_argument("--prior-high-lookback-sessions", type=int, default=252)
    parser.add_argument("--min-peak-age-sessions", type=int, default=20)
    parser.add_argument("--require-anchor-below-ma7", action="store_true")
    parser.add_argument("--require-next-day-no-full-reclaim", action="store_true")
    parser.add_argument("--confirmation-reclaim-days", type=int, default=1)
    parser.add_argument("--max-ma20-slope-20", type=float, default=None)
    parser.add_argument("--min-retest-ratio", type=float, default=0.88)
    parser.add_argument("--max-anchor-drop-pct", type=float, default=None)
    parser.add_argument("--min-prior-peak-runup", type=float, default=0.0)
    parser.add_argument("--require-prior-peak-above-ma200", action="store_true")
    parser.add_argument("--min-prior-peak-local-prominence", type=float, default=0.0)
    parser.add_argument("--universe-codes-path", default=None)
    args = parser.parse_args()
    result = run_validation(
        db_path=Path(args.db_path) if args.db_path else None,
        output_root=Path(args.output_root),
        start_dt=args.start_dt,
        end_dt=args.end_dt,
        run_id=args.run_id,
        signal_mode=args.signal_mode,
        prior_high_lookback_sessions=args.prior_high_lookback_sessions,
        min_peak_age_sessions=args.min_peak_age_sessions,
        require_anchor_below_ma7=bool(args.require_anchor_below_ma7),
        require_next_day_no_full_reclaim=bool(args.require_next_day_no_full_reclaim),
        confirmation_reclaim_days=max(0, int(args.confirmation_reclaim_days)),
        max_ma20_slope_20=args.max_ma20_slope_20,
        min_retest_ratio=float(args.min_retest_ratio),
        max_anchor_drop_pct=args.max_anchor_drop_pct,
        min_prior_peak_runup=float(args.min_prior_peak_runup),
        require_prior_peak_above_ma200=bool(args.require_prior_peak_above_ma200),
        min_prior_peak_local_prominence=float(args.min_prior_peak_local_prominence),
        universe_codes_path=Path(args.universe_codes_path) if args.universe_codes_path else None,
    )
    print(json.dumps({
        "run_dir": result["run_dir"],
        "coverage": result["coverage"],
        "decision": result["decision"],
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
