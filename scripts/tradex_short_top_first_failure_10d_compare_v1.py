from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any

import duckdb

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from shared.runtime_stock_db_contract import inspect_runtime_stock_db, resolve_runtime_stock_db_path
from scripts.tradex_short_collapse_shape_10d_compare_v1 import (
    _clean,
    _load_daily,
    _pct,
    _rolling_max,
    _rolling_mean,
    _rolling_min,
    _round,
)


AXIS_ID = "short_top_first_failure_10d_compare_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_top_first_failure_10d_compare_v1")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _features(
    bars: list[dict[str, Any]],
    index: int,
    closes: list[float],
    ma7s: list[float | None],
    ma20s: list[float | None],
    ma60s: list[float | None],
    vol20s: list[float | None],
    high20s: list[float | None],
    high60s: list[float | None],
    high120s: list[float | None],
    low60s: list[float | None],
) -> dict[str, float] | None:
    if index < 120:
        return None
    ma7 = ma7s[index]
    ma20 = ma20s[index]
    ma60 = ma60s[index]
    vol20 = vol20s[index]
    high20 = high20s[index]
    high60 = high60s[index]
    high120 = high120s[index]
    low60 = low60s[index]
    if None in {ma7, ma20, ma60, vol20, high20, high60, high120, low60} or not vol20:
        return None
    row = bars[index]
    prev = bars[index - 1]
    rng = row["h"] - row["l"]
    return {
        "ma7": float(ma7),
        "ma20": float(ma20),
        "ma60": float(ma60),
        "high20": float(high20),
        "high60": float(high60),
        "high120": float(high120),
        "low60": float(low60),
        "ret20": row["c"] / closes[index - 20] - 1,
        "ret60": row["c"] / closes[index - 60] - 1,
        "ret120": row["c"] / closes[index - 120] - 1,
        "volume_vs20": row["v"] / float(vol20),
        "close_pos": (row["c"] - row["l"]) / rng if rng > 0 else 0.5,
        "upper_wick": (row["h"] - max(row["o"], row["c"])) / rng if rng > 0 else 0.0,
        "lower_wick": (min(row["o"], row["c"]) - row["l"]) / rng if rng > 0 else 0.0,
        "body_ratio": abs(row["c"] - row["o"]) / rng if rng > 0 else 0.0,
        "range60_pos": (row["c"] - float(low60)) / (float(high60) - float(low60)) if float(high60) > float(low60) else 0.5,
        "dist_ma7": row["c"] / float(ma7) - 1,
        "dist_ma20": row["c"] / float(ma20) - 1,
        "dist_ma60": row["c"] / float(ma60) - 1,
        "prev_dist_ma7": prev["c"] / float(ma7s[index - 1] or ma7) - 1,
        "close_vs_high60": row["c"] / float(high60) - 1,
        "close_vs_high120": row["c"] / float(high120) - 1,
    }


def _red_count(bars: list[dict[str, Any]], index: int, length: int) -> int:
    return sum(1 for i in range(index - length + 1, index + 1) if i >= 0 and bars[i]["c"] < bars[i]["o"])


def _tags(bars: list[dict[str, Any]], index: int, feat: dict[str, float]) -> list[str]:
    row = bars[index]
    prev = bars[index - 1]
    tags = []
    high_zone = feat["range60_pos"] >= 0.70 and feat["ret60"] >= 0.10
    near_high = feat["close_vs_high60"] >= -0.08 or feat["close_vs_high120"] >= -0.10
    first_ma7_fail = row["c"] < feat["ma7"] and prev["c"] >= float(feat["ma7"]) * 0.995
    if high_zone and near_high and feat["upper_wick"] >= 0.35 and feat["close_pos"] <= 0.45 and row["c"] < row["o"]:
        tags.append("high_zone_upper_wick_first_failure")
    if high_zone and near_high and first_ma7_fail and feat["dist_ma20"] >= -0.03 and row["c"] < row["o"]:
        tags.append("high_zone_first_ma7_fail")
    if (
        feat["ret20"] >= 0.12
        and row["h"] >= feat["high60"] * 0.995
        and row["c"] < feat["high60"] * 0.985
        and feat["upper_wick"] >= 0.25
        and feat["volume_vs20"] <= 1.80
    ):
        tags.append("failed_breakout_low_volume")
    if high_zone and _red_count(bars, index, 3) >= 2 and feat["volume_vs20"] <= 1.50 and feat["dist_ma20"] >= -0.02:
        tags.append("distribution_three_red_high_zone")
    if (
        high_zone
        and near_high
        and feat["upper_wick"] >= 0.25
        and feat["close_pos"] <= 0.55
        and feat["volume_vs20"] <= 1.30
        and feat["dist_ma20"] >= 0
    ):
        tags.append("pre_break_weak_pressure_above_ma20")
    return tags


def _evaluate(bars: list[dict[str, Any]], index: int, tag: str, horizon: int) -> dict[str, Any] | None:
    if index + horizon >= len(bars):
        return None
    signal = bars[index]
    entry = signal["c"]
    future = bars[index + 1 : index + horizon + 1]
    lows = [row["l"] for row in future]
    highs = [row["h"] for row in future]
    closes = [row["c"] for row in future]
    stop_price = signal["h"] * 1.005
    target5_day = next((i + 1 for i, low in enumerate(lows) if low <= entry * 0.95), None)
    target8_day = next((i + 1 for i, low in enumerate(lows) if low <= entry * 0.92), None)
    stop_day = next((i + 1 for i, high in enumerate(highs) if high >= stop_price), None)
    first_event = "time"
    if target5_day is not None and (stop_day is None or target5_day <= stop_day):
        first_event = "target5"
    elif stop_day is not None:
        first_event = "stop_peak_high"
    return {
        "tag": tag,
        "signal_ymd": signal["ymd"],
        "entry_ymd": signal["ymd"],
        "entry_price": entry,
        "stop_pct": stop_price / entry - 1,
        "target5_hit_10d": target5_day is not None,
        "target8_hit_10d": target8_day is not None,
        "target5_first": first_event == "target5",
        "stop_first": first_event == "stop_peak_high",
        "first_event": first_event,
        "close10_short_ret": (entry - closes[-1]) / entry,
        "mfe10_short": (entry - min(lows)) / entry,
        "mae10_short": (entry - max(highs)) / entry,
    }


def _summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"event_count": 0}
    returns = [float(row["close10_short_ret"]) for row in rows]
    by_month: dict[str, list[float]] = defaultdict(list)
    by_year: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        ymd = str(row["entry_ymd"])
        by_month[ymd[:6]].append(float(row["close10_short_ret"]))
        by_year[ymd[:4]].append(float(row["close10_short_ret"]))
    usable_months = [values for values in by_month.values() if len(values) >= 5]
    usable_years = [values for values in by_year.values() if len(values) >= 10]
    return {
        "event_count": len(rows),
        "unique_code_count": len({row["code"] for row in rows}),
        "target5_hit_10d_rate": _round(sum(1 for row in rows if row["target5_hit_10d"]) / len(rows)),
        "target8_hit_10d_rate": _round(sum(1 for row in rows if row["target8_hit_10d"]) / len(rows)),
        "target5_first_rate": _round(sum(1 for row in rows if row["target5_first"]) / len(rows)),
        "stop_first_rate": _round(sum(1 for row in rows if row["stop_first"]) / len(rows)),
        "avg_stop_pct": _round(mean(float(row["stop_pct"]) for row in rows)),
        "close10_short_ret_mean": _round(mean(returns)),
        "close10_short_ret_median": _round(median(returns)),
        "close10_short_positive_rate": _round(sum(1 for value in returns if value > 0) / len(returns)),
        "positive_month_rate": _round(sum(1 for values in usable_months if mean(values) > 0) / len(usable_months)) if usable_months else None,
        "positive_year_rate": _round(sum(1 for values in usable_years if mean(values) > 0) / len(usable_years)) if usable_years else None,
        "usable_month_count": len(usable_months),
        "usable_year_count": len(usable_years),
        "close10_short_ret_p10": _round(_pct(returns, 0.10)),
        "close10_short_ret_p90": _round(_pct(returns, 0.90)),
        "mfe10_short_mean": _round(mean(float(row["mfe10_short"]) for row in rows)),
        "mae10_short_mean": _round(mean(float(row["mae10_short"]) for row in rows)),
    }


def _decision(summary: dict[str, Any]) -> tuple[str, str]:
    if summary.get("event_count", 0) < 300:
        return "drop", "insufficient_sample"
    positive = (summary.get("close10_short_ret_mean") or 0) > 0
    month_ok = (summary.get("positive_month_rate") or 0) >= 0.50
    year_ok = (summary.get("positive_year_rate") or 0) >= 0.60
    stop_ok = (summary.get("stop_first_rate") or 1) <= 0.45
    target_ok = (summary.get("target5_first_rate") or 0) >= 0.35
    tight_risk = (summary.get("avg_stop_pct") or 1) <= 0.055
    if positive and month_ok and year_ok and stop_ok and target_ok and tight_risk:
        return "keep", "positive_return_with_stability_target_first_and_tight_peak_stop"
    if positive and tight_risk and (month_ok or year_ok):
        return "hold", "positive_return_but_not_all_stability_or_target_gates"
    return "drop", "no_positive_10d_first_failure_edge"


def run(*, db_path: Path, output_root: Path, horizon: int, limit_codes: int | None) -> Path:
    db_status = inspect_runtime_stock_db(runtime_db_path=db_path)
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        daily = _load_daily(conn, limit_codes)
    finally:
        conn.close()
    events: list[dict[str, Any]] = []
    for code, bars in daily.items():
        closes = [row["c"] for row in bars]
        lows = [row["l"] for row in bars]
        highs = [row["h"] for row in bars]
        vols = [row["v"] for row in bars]
        ma7s = _rolling_mean(closes, 7)
        ma20s = _rolling_mean(closes, 20)
        ma60s = _rolling_mean(closes, 60)
        vol20s = _rolling_mean(vols, 20)
        high20s = _rolling_max(highs, 20)
        high60s = _rolling_max(highs, 60)
        high120s = _rolling_max(highs, 120)
        low60s = _rolling_min(lows, 60)
        for index in range(120, len(bars) - horizon):
            feat = _features(bars, index, closes, ma7s, ma20s, ma60s, vol20s, high20s, high60s, high120s, low60s)
            if feat is None:
                continue
            for tag in _tags(bars, index, feat):
                metrics = _evaluate(bars, index, tag, horizon)
                if metrics is None:
                    continue
                events.append(
                    {
                        "code": code,
                        **{key: _round(feat[key]) for key in ["ret20", "ret60", "ret120", "volume_vs20", "close_pos", "upper_wick", "range60_pos", "dist_ma7", "dist_ma20", "close_vs_high60"]},
                        **metrics,
                    }
                )
    rows = []
    for tag in sorted({row["tag"] for row in events}):
        tag_rows = [row for row in events if row["tag"] == tag]
        summary = _summarize(tag_rows)
        decision, reason = _decision(summary)
        rows.append({"pattern_tag": tag, "decision": decision, "reason": reason, **summary})
    rows.sort(key=lambda row: (row["decision"] == "keep", row["decision"] == "hold", row.get("close10_short_ret_mean") or -1), reverse=True)
    keep = [row for row in rows if row["decision"] == "keep"]
    hold = [row for row in rows if row["decision"] == "hold"]
    if keep:
        rollup_decision = "keep_top_first_failure_branch"
        reason = "at_least_one_first_failure_pattern_cleared_10d_short_edge_gates"
    elif hold:
        rollup_decision = "hold_top_first_failure_branch"
        reason = "at_least_one_first_failure_pattern_positive_but_not_trade_ready"
    else:
        rollup_decision = "drop_top_first_failure_branch"
        reason = "no_first_failure_pattern_cleared_10d_short_edge_gates"
    report = {
        "schema_version": f"{AXIS_ID}_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "db_path": str(db_path),
        "db_status": db_status,
        "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {
            "changed_axis": "top_first_failure_shape_only",
            "horizon": horizon,
            "entry": "signal_close",
            "stop_observation": "signal_high_plus_0.5pct",
            "confirmed_non_yahoo_daily_bars": True,
            "gross_only": True,
            "no_runtime_mutation": True,
            "no_meemee_reflection": True,
            "no_provisional_intraday_mix": True,
        },
        "pattern_definitions": {
            "high_zone_upper_wick_first_failure": "high-zone, near high, bearish upper wick weak close",
            "high_zone_first_ma7_fail": "high-zone first MA7 close failure before deep MA20 break",
            "failed_breakout_low_volume": "failed high update with upper wick and low follow-through volume",
            "distribution_three_red_high_zone": "high-zone two or more red candles with volume cooling",
            "pre_break_weak_pressure_above_ma20": "weak pressure while still above MA20, before breakdown chase",
        },
        "family_leaderboard": rows,
        "decision": {
            "candidate_local_decision": rollup_decision,
            "session_aggregate_decision": rollup_decision,
            "authoritative_rollup_decision": rollup_decision,
            "reason": reason,
        },
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
    }
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    _write_json(output_dir / "compare.json", report)
    _write_json(output_dir / "family_leaderboard.json", {"rows": rows})
    _write_json(output_dir / "session_leaderboard_rollup.json", {"decision": report["decision"], "family_leaderboard": rows})
    _write_csv(output_dir / "event_sample.csv", events[:1000])
    _write_json(output_root / "latest_compare.json", {"run_root": str(output_dir), **report})
    _write_json(
        output_dir / "_ARTIFACT_COMPLETE.json",
        {
            "all_present": all(
                (output_dir / name).exists()
                for name in ["compare.json", "family_leaderboard.json", "session_leaderboard_rollup.json", "event_sample.csv"]
            ),
            "authoritative_rollup_decision": rollup_decision,
            "run_root": str(output_dir),
        },
    )
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=None)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--horizon", type=int, default=10)
    parser.add_argument("--limit-codes", type=int, default=None)
    args = parser.parse_args()
    db_path = args.db_path or resolve_runtime_stock_db_path()
    print(run(db_path=db_path, output_root=args.output_root, horizon=args.horizon, limit_codes=args.limit_codes))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
