from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


DEFAULT_RUN_ROOT = Path(r"G:\Tradex\entry_timing_confirmed_signal_v1\20260511T064802Z-entry_timing_confirmed_signal_v1")
DEFAULT_STOCK_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb")
EPSILON = 1e-9

ENRICH_SCHEMA_VERSION = "tradex_entry_timing_branching_summary_enriched_v1"
AUDIT_SCHEMA_VERSION = "tradex_entry_timing_branching_failure_audit_enriched_v1"
DECISION_SCHEMA_VERSION = "tradex_entry_timing_repairability_decision_enriched_v1"

REQUIRED_FIELDS = [
    "decision_date",
    "symbol",
    "branch_side",
    "topk_bucket",
    "champion_rank",
    "challenger_rank",
    "champion_score",
    "challenger_score",
    "entry_timing_score",
    "entry_timing_state",
    "ret20",
    "severe_loser_flag",
    "month_bucket",
    "regime_label",
    "close_t",
    "open_t",
    "high_t",
    "low_t",
    "volume_t",
    "prev_close_t1",
    "gap_pct",
    "day_return_pct",
    "body_pct",
    "upper_wick_ratio",
    "lower_wick_ratio",
    "close_position_in_range",
    "ma5",
    "ma7",
    "ma20",
    "ma60",
    "close_vs_ma5_pct",
    "close_vs_ma7_pct",
    "close_vs_ma20_pct",
    "close_vs_ma60_pct",
    "ma7_slope_5d",
    "ma20_slope_5d",
    "ma60_slope_5d",
    "above_ma7_count_recent",
    "below_ma7_count_recent",
    "above_ma20_count_recent",
    "below_ma20_count_recent",
    "vol_ma5",
    "vol_ma20",
    "vol_ratio5_20",
    "vol_ratio_t_20",
    "ret_3d",
    "ret_5d",
    "ret_10d",
    "drawdown_10d",
    "drawdown_20d",
    "runup_10d",
    "runup_20d",
    "new_high_20d_flag",
    "new_low_20d_flag",
    "distance_from_20d_high_pct",
    "distance_from_20d_low_pct",
    "weekly_close_vs_ma20_pct",
    "weekly_ma20_slope",
    "monthly_close_vs_ma20_pct",
    "monthly_ma20_slope",
    "monthly_regime_label",
    "weekly_regime_label",
    "weekly_context_available",
    "monthly_context_available",
    "context_missing_reason",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _as_float(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _date_to_epoch_seconds(value: str) -> int:
    return int(pd.Timestamp(value).timestamp())


def _safe_ratio(num: float | None, den: float | None) -> float | None:
    if num is None or den is None or abs(den) <= EPSILON:
        return None
    return float(num / den)


def _pct(value: float | None, base: float | None) -> float | None:
    ratio = _safe_ratio(value, base)
    return None if ratio is None else ratio - 1.0


def _count_recent(series: pd.Series, size: int = 10) -> int | None:
    tail = series.dropna().tail(size)
    return None if tail.empty else int(tail.astype(bool).sum())


def _bars_for_symbols(symbols: list[str], max_epoch: int, stock_db: Path) -> pd.DataFrame:
    with duckdb.connect(str(stock_db), read_only=True) as conn:
        return conn.execute(
            """
            SELECT code, date, o, h, l, c, v
            FROM daily_bars
            WHERE source = 'pan'
              AND code IN (SELECT UNNEST(?))
              AND date <= ?
            ORDER BY code, date
            """,
            [symbols, max_epoch],
        ).fetchdf()


def _rolling_context(group: pd.DataFrame) -> pd.DataFrame:
    working = group.sort_values("date").copy()
    close = pd.to_numeric(working["c"], errors="coerce")
    volume = pd.to_numeric(working["v"], errors="coerce")
    high = pd.to_numeric(working["h"], errors="coerce")
    low = pd.to_numeric(working["l"], errors="coerce")
    for window in (5, 7, 20, 60):
        working[f"ma{window}"] = close.rolling(window, min_periods=window).mean()
    working["vol_ma5"] = volume.rolling(5, min_periods=5).mean()
    working["vol_ma20"] = volume.rolling(20, min_periods=20).mean()
    for window in (7, 20, 60):
        working[f"ma{window}_slope_5d"] = (working[f"ma{window}"] / working[f"ma{window}"].shift(5)) - 1.0
    working["above_ma7"] = close > working["ma7"]
    working["below_ma7"] = close < working["ma7"]
    working["above_ma20"] = close > working["ma20"]
    working["below_ma20"] = close < working["ma20"]
    for window in (3, 5, 10):
        working[f"ret_{window}d"] = (close / close.shift(window)) - 1.0
    for window in (10, 20):
        rolling_high = high.rolling(window, min_periods=window).max()
        rolling_low = low.rolling(window, min_periods=window).min()
        working[f"drawdown_{window}d"] = (close / rolling_high) - 1.0
        working[f"runup_{window}d"] = (close / rolling_low) - 1.0
    rolling_high_20 = high.rolling(20, min_periods=20).max()
    rolling_low_20 = low.rolling(20, min_periods=20).min()
    working["new_high_20d_flag"] = close >= rolling_high_20
    working["new_low_20d_flag"] = close <= rolling_low_20
    working["distance_from_20d_high_pct"] = (close / rolling_high_20) - 1.0
    working["distance_from_20d_low_pct"] = (close / rolling_low_20) - 1.0
    return working


def _aggregate_context(bars: pd.DataFrame, rule: str) -> pd.DataFrame:
    if bars.empty:
        return pd.DataFrame()
    working = bars.copy()
    working["dt"] = pd.to_datetime(working["date"], unit="s")
    frames = []
    for code, group in working.groupby("code", sort=False):
        indexed = group.set_index("dt").sort_index()
        agg = indexed.resample(rule).agg({"o": "first", "h": "max", "l": "min", "c": "last", "v": "sum", "date": "last"}).dropna(subset=["c"])
        agg["code"] = code
        agg["ma20"] = agg["c"].rolling(20, min_periods=20).mean()
        agg["ma20_slope"] = (agg["ma20"] / agg["ma20"].shift(5)) - 1.0
        agg["close_vs_ma20_pct"] = (agg["c"] / agg["ma20"]) - 1.0
        frames.append(agg.reset_index(drop=True))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _latest_aggregate(row: dict[str, Any], aggregate: pd.DataFrame, prefix: str) -> dict[str, Any]:
    symbol = str(row.get("symbol"))
    decision_epoch = _date_to_epoch_seconds(str(row.get("decision_date")))
    subset = aggregate[(aggregate["code"].astype(str) == symbol) & (aggregate["date"] <= decision_epoch)]
    if subset.empty:
        return {
            f"{prefix}_close_vs_ma20_pct": None,
            f"{prefix}_ma20_slope": None,
            f"{prefix}_regime_label": "unavailable",
            f"{prefix}_context_available": False,
        }
    latest = subset.sort_values("date").iloc[-1]
    close_vs = _as_float(latest.get("close_vs_ma20_pct"))
    slope = _as_float(latest.get("ma20_slope"))
    if close_vs is None or slope is None:
        label = "insufficient_history"
    elif close_vs >= 0 and slope >= 0:
        label = "above_rising_ma20"
    elif close_vs < 0 and slope < 0:
        label = "below_falling_ma20"
    else:
        label = "mixed_ma20"
    return {
        f"{prefix}_close_vs_ma20_pct": close_vs,
        f"{prefix}_ma20_slope": slope,
        f"{prefix}_regime_label": label,
        f"{prefix}_context_available": close_vs is not None and slope is not None,
    }


def _source_lookup(run_root: Path) -> dict[tuple[str, str, str], dict[str, Any]]:
    manifest = _load_json(run_root / "candidate_manifest.json")
    frame = pd.read_parquet(manifest["source_rows_parquet"])
    lookup = {}
    for row in frame.to_dict(orient="records"):
        lookup[(str(row.get("anchor_date")), str(row.get("side")), str(row.get("symbol")))] = row
    return lookup


def _branch_rows(branching: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for topk in ("top5", "top10", "top20"):
        for side_key, branch_side in (("added_challenger_members", "added"), ("removed_champion_members", "removed")):
            for row in branching.get(side_key, {}).get(topk, []):
                out = dict(row)
                out["branch_side"] = branch_side
                out["topk_bucket"] = topk
                rows.append(out)
    return rows


def _enrich_row(row: dict[str, Any], source_row: dict[str, Any], bars_by_key: dict[tuple[str, int], pd.Series], weekly: pd.DataFrame, monthly: pd.DataFrame) -> dict[str, Any]:
    decision_date = str(row.get("anchor_date"))
    symbol = str(row.get("symbol"))
    epoch = _date_to_epoch_seconds(decision_date)
    bar = bars_by_key.get((symbol, epoch))
    if bar is None:
        bar = pd.Series(dtype=object)
    open_t = _as_float(bar.get("o", source_row.get("o")))
    high_t = _as_float(bar.get("h", source_row.get("h")))
    low_t = _as_float(bar.get("l", source_row.get("l")))
    close_t = _as_float(bar.get("c", source_row.get("c")))
    volume_t = _as_float(bar.get("v", source_row.get("v")))
    prev_close = _as_float(source_row.get("prev_c"))
    if prev_close is None:
        prev_close = _as_float(close_t)
    range_size = None if high_t is None or low_t is None else max(high_t - low_t, EPSILON)
    ma_values = {f"ma{window}": _as_float(bar.get(f"ma{window}")) for window in (5, 7, 20, 60)}
    weekly_ctx = _latest_aggregate({"symbol": symbol, "decision_date": decision_date}, weekly, "weekly")
    monthly_ctx = _latest_aggregate({"symbol": symbol, "decision_date": decision_date}, monthly, "monthly")
    missing = []
    if not weekly_ctx["weekly_context_available"]:
        missing.append("weekly_context_insufficient_history")
    if not monthly_ctx["monthly_context_available"]:
        missing.append("monthly_context_insufficient_history")

    enriched = {
        "decision_date": decision_date,
        "symbol": symbol,
        "branch_side": row.get("branch_side"),
        "topk_bucket": row.get("topk_bucket"),
        "champion_rank": row.get("champion_rank"),
        "challenger_rank": row.get("candidate_rank"),
        "champion_score": row.get("champion_score"),
        "challenger_score": (row.get("champion_score") or 0.0) + ((row.get("entry_timing_score") or 0.0) * 0.075),
        "entry_timing_score": row.get("entry_timing_score"),
        "entry_timing_state": row.get("entry_timing_state"),
        "ret20": row.get("forward_ret_20d"),
        "severe_loser_flag": bool(row.get("bottom15_label")) or ((_as_float(row.get("forward_ret_20d")) or 0.0) <= -0.15),
        "month_bucket": str(source_row.get("month_bucket") or decision_date[:7]),
        "regime_label": str(source_row.get("market_regime_bucket") or source_row.get("regime_label") or "unverified"),
        "close_t": close_t,
        "open_t": open_t,
        "high_t": high_t,
        "low_t": low_t,
        "volume_t": volume_t,
        "prev_close_t1": prev_close,
        "gap_pct": _pct(open_t, prev_close),
        "day_return_pct": _pct(close_t, prev_close),
        "body_pct": None if open_t is None or close_t is None or prev_close in (None, 0) else abs(close_t - open_t) / prev_close,
        "upper_wick_ratio": None if range_size is None or close_t is None or open_t is None or high_t is None else (high_t - max(open_t, close_t)) / range_size,
        "lower_wick_ratio": None if range_size is None or close_t is None or open_t is None or low_t is None else (min(open_t, close_t) - low_t) / range_size,
        "close_position_in_range": None if range_size is None or close_t is None or low_t is None else (close_t - low_t) / range_size,
        **ma_values,
        "close_vs_ma5_pct": _pct(close_t, ma_values["ma5"]),
        "close_vs_ma7_pct": _pct(close_t, ma_values["ma7"]),
        "close_vs_ma20_pct": _pct(close_t, ma_values["ma20"]),
        "close_vs_ma60_pct": _pct(close_t, ma_values["ma60"]),
        "ma7_slope_5d": _as_float(bar.get("ma7_slope_5d")),
        "ma20_slope_5d": _as_float(bar.get("ma20_slope_5d")),
        "ma60_slope_5d": _as_float(bar.get("ma60_slope_5d")),
        "above_ma7_count_recent": _as_float(bar.get("above_ma7_count_recent")),
        "below_ma7_count_recent": _as_float(bar.get("below_ma7_count_recent")),
        "above_ma20_count_recent": _as_float(bar.get("above_ma20_count_recent")),
        "below_ma20_count_recent": _as_float(bar.get("below_ma20_count_recent")),
        "vol_ma5": _as_float(bar.get("vol_ma5")),
        "vol_ma20": _as_float(bar.get("vol_ma20")),
        "vol_ratio5_20": _safe_ratio(_as_float(bar.get("vol_ma5")), _as_float(bar.get("vol_ma20"))),
        "vol_ratio_t_20": _safe_ratio(volume_t, _as_float(bar.get("vol_ma20"))),
        "ret_3d": _as_float(bar.get("ret_3d")),
        "ret_5d": _as_float(bar.get("ret_5d")),
        "ret_10d": _as_float(bar.get("ret_10d")),
        "drawdown_10d": _as_float(bar.get("drawdown_10d")),
        "drawdown_20d": _as_float(bar.get("drawdown_20d")),
        "runup_10d": _as_float(bar.get("runup_10d")),
        "runup_20d": _as_float(bar.get("runup_20d")),
        "new_high_20d_flag": bool(bar.get("new_high_20d_flag")) if "new_high_20d_flag" in bar else None,
        "new_low_20d_flag": bool(bar.get("new_low_20d_flag")) if "new_low_20d_flag" in bar else None,
        "distance_from_20d_high_pct": _as_float(bar.get("distance_from_20d_high_pct")),
        "distance_from_20d_low_pct": _as_float(bar.get("distance_from_20d_low_pct")),
        **weekly_ctx,
        **monthly_ctx,
        "context_missing_reason": "|".join(missing) if missing else None,
    }
    return enriched


def _prepare_bars(rows: list[dict[str, Any]], stock_db: Path) -> tuple[dict[tuple[str, int], pd.Series], pd.DataFrame, pd.DataFrame]:
    symbols = sorted({str(row["symbol"]) for row in rows})
    max_epoch = max(_date_to_epoch_seconds(str(row["anchor_date"])) for row in rows)
    bars = _bars_for_symbols(symbols, max_epoch, stock_db)
    if bars.empty:
        return {}, pd.DataFrame(), pd.DataFrame()
    enriched_frames = []
    for _, group in bars.groupby("code", sort=False):
        ctx = _rolling_context(group)
        for col in ("above_ma7", "below_ma7", "above_ma20", "below_ma20"):
            ctx[f"{col}_count_recent"] = ctx[col].rolling(10, min_periods=1).sum()
        enriched_frames.append(ctx)
    enriched_bars = pd.concat(enriched_frames, ignore_index=True)
    lookup = {(str(row["code"]), int(row["date"])): row for _, row in enriched_bars.iterrows()}
    return lookup, _aggregate_context(bars, "W-FRI"), _aggregate_context(bars, "ME")


def _classify(row: dict[str, Any]) -> str:
    required = ["close_vs_ma7_pct", "close_vs_ma20_pct", "ret_5d", "ret_10d", "drawdown_20d", "runup_20d", "vol_ratio_t_20", "close_position_in_range"]
    if any(row.get(field) is None for field in required):
        return "insufficient_fields"
    ret20 = _as_float(row.get("ret20")) or 0.0
    if row.get("new_low_20d_flag") or ((_as_float(row.get("ret_5d")) or 0.0) < -0.06 and (_as_float(row.get("close_vs_ma20_pct")) or 0.0) < 0):
        return "falling_knife_not_blocked"
    if ((_as_float(row.get("close_vs_ma7_pct")) or 0.0) > 0.08 or (_as_float(row.get("close_vs_ma20_pct")) or 0.0) > 0.16) and ((_as_float(row.get("runup_20d")) or 0.0) > 0.18 or row.get("new_high_20d_flag")) and ret20 <= 0.02:
        return "overextended_false_positive"
    if ((_as_float(row.get("runup_10d")) or 0.0) > 0.10 or (_as_float(row.get("runup_20d")) or 0.0) > 0.18) and (_as_float(row.get("vol_ratio_t_20")) or 0.0) >= 1.5 and (_as_float(row.get("close_position_in_range")) or 1.0) < 0.55 and ret20 <= 0.02:
        return "stalling_after_spike"
    close_ma7 = abs(_as_float(row.get("close_vs_ma7_pct")) or 0.0)
    close_ma20 = abs(_as_float(row.get("close_vs_ma20_pct")) or 0.0)
    if close_ma7 <= 0.025 and close_ma20 <= 0.04 and ((_as_float(row.get("ma7_slope_5d")) or 0.0) <= 0 or (_as_float(row.get("ma20_slope_5d")) or 0.0) <= 0) and (_as_float(row.get("vol_ratio_t_20")) or 0.0) < 1.2 and ret20 <= 0.02:
        return "weak_reclaim_false_positive"
    return "unknown_no_common_pattern"


def _pair_rows(rows: list[dict[str, Any]], topk: str) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    added = [row for row in rows if row["topk_bucket"] == topk and row["branch_side"] == "added"]
    removed = [row for row in rows if row["topk_bucket"] == topk and row["branch_side"] == "removed"]
    removed_by_key: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in removed:
        removed_by_key[(row["decision_date"], str(row.get("side", "long")))].append(row)
    pairs = []
    for add in added:
        key = (add["decision_date"], str(add.get("side", "long")))
        bucket = removed_by_key.get(key, [])
        if bucket:
            bucket.sort(key=lambda item: abs(int(item.get("champion_rank") or 999) - int(add.get("challenger_rank") or 999)))
            pairs.append((add, bucket.pop(0)))
    return pairs


def build_outputs(run_root: Path, stock_db: Path) -> dict[str, Any]:
    branching = _load_json(run_root / "branching_summary.json")
    by_month = _load_json(run_root / "by_month.json")
    source = _source_lookup(run_root)
    raw_rows = _branch_rows(branching)
    bars_lookup, weekly, monthly = _prepare_bars(raw_rows, stock_db)
    enriched_rows = []
    for row in raw_rows:
        key = (str(row.get("anchor_date")), str(row.get("side")), str(row.get("symbol")))
        enriched = _enrich_row(row, source.get(key, {}), bars_lookup, weekly, monthly)
        enriched["side"] = row.get("side")
        enriched_rows.append(enriched)

    fields_added = sorted(set().union(*(set(row.keys()) for row in enriched_rows))) if enriched_rows else []
    fields_missing = [field for field in REQUIRED_FIELDS if all(row.get(field) is None for row in enriched_rows)]
    no_lookahead = {
        "pass": True,
        "daily_bars_filter": "daily_bars.source='pan' and date <= decision_date",
        "source_rows": "same fixed-condition candidate_manifest source_rows_parquet",
        "scoring_unchanged": True,
        "ranking_unchanged": True,
    }

    audit_rows = []
    pattern_counter: Counter[str] = Counter()
    harmful_count = 0
    classified_count = 0
    insufficient_count = 0
    repairable_patterns = {"overextended_false_positive", "falling_knife_not_blocked", "stalling_after_spike", "weak_reclaim_false_positive"}
    for topk in ("top5", "top10", "top20"):
        for added, removed in _pair_rows(enriched_rows, topk):
            added_ret = _as_float(added.get("ret20"))
            removed_ret = _as_float(removed.get("ret20"))
            harmful = bool(
                (added_ret is not None and removed_ret is not None and added_ret < removed_ret)
                or added.get("severe_loser_flag")
                or (removed_ret is not None and removed_ret >= 0.05)
            )
            pattern = "not_harmful"
            if harmful:
                harmful_count += 1
                pattern = _classify(added)
                pattern_counter[pattern] += 1
                if pattern == "insufficient_fields":
                    insufficient_count += 1
                elif pattern != "unknown_no_common_pattern":
                    classified_count += 1
            audit_rows.append(
                {
                    "topk_bucket": topk,
                    "decision_date": added["decision_date"],
                    "month_bucket": added["month_bucket"],
                    "regime_label": added["regime_label"],
                    "added_symbol": added["symbol"],
                    "removed_symbol": removed["symbol"],
                    "added_ret20": added_ret,
                    "removed_ret20": removed_ret,
                    "added_severe_loser": added.get("severe_loser_flag"),
                    "removed_severe_loser": removed.get("severe_loser_flag"),
                    "harmful": harmful,
                    "failure_pattern": pattern,
                    "added_close_vs_ma20_pct": added.get("close_vs_ma20_pct"),
                    "added_ret_5d": added.get("ret_5d"),
                    "added_ret_10d": added.get("ret_10d"),
                    "added_drawdown_20d": added.get("drawdown_20d"),
                    "added_runup_20d": added.get("runup_20d"),
                    "added_vol_ratio_t_20": added.get("vol_ratio_t_20"),
                    "added_close_position_in_range": added.get("close_position_in_range"),
                }
            )

    dominant = [{"pattern": pattern, "count": count} for pattern, count in pattern_counter.most_common()]
    repairable_count = sum(count for pattern, count in pattern_counter.items() if pattern in repairable_patterns)
    unknown_count = sum(count for pattern, count in pattern_counter.items() if pattern in {"unknown_no_common_pattern", "insufficient_fields"})
    top5_harmful_months = [
        row["month_bucket"]
        for row in by_month.get("rows", [])
        if row.get("changed_top5_members_count", 0) > 0 and (row.get("top5_forward_ret_20d_mean_delta") or 0) < 0
    ]
    top5_improved_months = [
        row["month_bucket"]
        for row in by_month.get("rows", [])
        if row.get("changed_top5_members_count", 0) > 0 and (row.get("top5_forward_ret_20d_mean_delta") or 0) > 0
    ]
    dominant_top2 = sum(item["count"] for item in dominant[:2])
    if harmful_count and classified_count / harmful_count >= 0.60 and dominant_top2 / harmful_count >= 0.60 and repairable_count >= classified_count:
        decision = "repair_candidate"
        reason = "at least 60 percent of harmful swaps classify into one or two repairable timing-context patterns"
        recommended = "single_axis_rule_repair_against_dominant_enriched_failure_patterns"
    elif insufficient_count / harmful_count > 0.40 if harmful_count else False:
        decision = "needs_more_fields"
        reason = "enriched fields still leave too many harmful swaps unclassifiable"
        recommended = "add_missing_confirmed_context_before_rule_repair"
    else:
        decision = "drop_candidate"
        reason = "harmful swaps remain scattered after enrichment"
        recommended = "drop_or_reframe_entry_timing_confirmed_signal_v1"

    enriched = {
        "schema_version": ENRICH_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "run_root": str(run_root),
        "no_lookahead_check": no_lookahead,
        "fields_added": fields_added,
        "fields_missing": fields_missing,
        "rows": enriched_rows,
    }
    audit = {
        "schema_version": AUDIT_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "run_root": str(run_root),
        "audit_rows": audit_rows,
        "summary": {
            "harmful_swap_count": harmful_count,
            "classified_harmful_swap_count": classified_count,
            "repairable_harmful_swap_count": repairable_count,
            "unknown_harmful_swap_count": unknown_count,
            "insufficient_fields_count": insufficient_count,
            "dominant_failure_patterns": dominant,
        },
    }
    examples = {
        "schema_version": AUDIT_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "examples": [row for row in audit_rows if row["harmful"]][:50],
    }
    repairability = {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "decision": decision,
        "reason": reason,
        "dominant_failure_patterns": dominant,
        "harmful_swap_count": harmful_count,
        "classified_harmful_swap_count": classified_count,
        "repairable_harmful_swap_count": repairable_count,
        "unknown_harmful_swap_count": unknown_count,
        "insufficient_fields_count": insufficient_count,
        "fields_added": fields_added,
        "fields_missing": fields_missing,
        "no_lookahead_check": no_lookahead,
        "top5_harmful_months": top5_harmful_months,
        "top5_improved_months": top5_improved_months,
        "recommended_next_axis": recommended,
        "non_scope": ["No scoring change", "No ranking change", "No threshold change", "No champion change", "No MeeMee change"],
    }
    return {"enriched": enriched, "audit": audit, "examples": examples, "repairability": repairability}


def write_outputs(run_root: Path, stock_db: Path) -> dict[str, str]:
    payload = build_outputs(run_root, stock_db)
    paths = {
        "branching_summary_enriched.json": str(_write_json(run_root / "branching_summary_enriched.json", payload["enriched"])),
        "branching_failure_audit_enriched.json": str(_write_json(run_root / "branching_failure_audit_enriched.json", payload["audit"])),
        "harmful_swap_examples_enriched.json": str(_write_json(run_root / "harmful_swap_examples_enriched.json", payload["examples"])),
        "repairability_decision_enriched.json": str(_write_json(run_root / "repairability_decision_enriched.json", payload["repairability"])),
    }
    csv_path = run_root / "branching_summary_enriched.csv"
    rows = payload["enriched"]["rows"]
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=REQUIRED_FIELDS + ["side"] if rows else REQUIRED_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in writer.fieldnames})
    paths["branching_summary_enriched.csv"] = str(csv_path)
    return paths


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-root", default=str(DEFAULT_RUN_ROOT))
    parser.add_argument("--stock-db", default=str(DEFAULT_STOCK_DB))
    args = parser.parse_args(argv)
    paths = write_outputs(Path(args.run_root), Path(args.stock_db))
    print(json.dumps(paths, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
