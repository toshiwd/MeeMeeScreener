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


AXIS_ID = "short_collapse_shape_10d_compare_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_collapse_shape_10d_compare_v1")


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


def _clean(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if hasattr(value, "item"):
        return _clean(value.item())
    return value


def _round(value: float | None, digits: int = 6) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return round(value, digits)


def _pct(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    idx = (len(ordered) - 1) * q
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return ordered[lo]
    return ordered[lo] * (hi - idx) + ordered[hi] * (idx - lo)


def _load_daily(conn: duckdb.DuckDBPyConnection, limit_codes: int | None) -> dict[str, list[dict[str, Any]]]:
    code_limit = f"LIMIT {int(limit_codes)}" if limit_codes else ""
    limit_join = "JOIN selected_codes s ON b.code = s.code" if limit_codes else ""
    selected_codes = f"selected_codes AS (SELECT DISTINCT code FROM daily_bars ORDER BY code {code_limit})," if limit_codes else ""
    rows = conn.execute(
        f"""
        WITH
        {selected_codes}
        normalized AS (
          SELECT
            b.code,
            CASE
              WHEN b.date > 30000000 THEN CAST(strftime(to_timestamp(b.date), '%Y%m%d') AS INTEGER)
              ELSE CAST(b.date AS INTEGER)
            END AS ymd,
            b.o, b.h, b.l, b.c, b.v
          FROM daily_bars b
          {limit_join}
          WHERE COALESCE(b.source, 'pan') <> 'yahoo'
            AND b.o > 0 AND b.h > 0 AND b.l > 0 AND b.c > 0
        )
        SELECT code, ymd, o, h, l, c, v
        FROM normalized
        ORDER BY code, ymd
        """
    ).fetchall()
    out: dict[str, list[dict[str, Any]]] = {}
    for code, ymd, o, h, l, c, v in rows:
        out.setdefault(str(code), []).append(
            {"ymd": int(ymd), "o": float(o), "h": float(h), "l": float(l), "c": float(c), "v": float(v or 0)}
        )
    return out


def _ma(values: list[float], end: int, period: int) -> float | None:
    start = end - period + 1
    if start < 0:
        return None
    return sum(values[start : end + 1]) / period


def _rolling_mean(values: list[float], period: int) -> list[float | None]:
    out: list[float | None] = [None] * len(values)
    total = 0.0
    for index, value in enumerate(values):
        total += value
        if index >= period:
            total -= values[index - period]
        if index >= period - 1:
            out[index] = total / period
    return out


def _rolling_min(values: list[float], period: int, *, exclude_current: bool = False) -> list[float | None]:
    out: list[float | None] = [None] * len(values)
    offset = 1 if exclude_current else 0
    for index in range(len(values)):
        end = index - offset + 1
        start = end - period
        if start >= 0:
            out[index] = min(values[start:end])
    return out


def _rolling_max(values: list[float], period: int) -> list[float | None]:
    out: list[float | None] = [None] * len(values)
    for index in range(period - 1, len(values)):
        out[index] = max(values[index - period + 1 : index + 1])
    return out


def _features(
    bars: list[dict[str, Any]],
    index: int,
    closes: list[float],
    ma20s: list[float | None],
    ma60s: list[float | None],
    vol20s: list[float | None],
    prior_low20s: list[float | None],
    high60s: list[float | None],
    low60s: list[float | None],
    high120s: list[float | None],
) -> dict[str, float] | None:
    ma20 = ma20s[index]
    ma60 = ma60s[index]
    vol20 = vol20s[index]
    if ma20 is None or ma60 is None or not vol20 or index < 120:
        return None
    row = bars[index]
    range_ = row["h"] - row["l"]
    high60 = high60s[index]
    low60 = low60s[index]
    high120 = high120s[index]
    low20_prior = prior_low20s[index]
    if high60 is None or low60 is None or high120 is None or low20_prior is None:
        return None
    ret20 = row["c"] / closes[index - 20] - 1
    ret60 = row["c"] / closes[index - 60] - 1
    return {
        "ma20": ma20,
        "ma60": ma60,
        "vol20": vol20,
        "prior_low20": low20_prior,
        "high60": high60,
        "low60": low60,
        "high120": high120,
        "ret20": ret20,
        "ret60": ret60,
        "volume_vs20": row["v"] / vol20,
        "close_pos": (row["c"] - row["l"]) / range_ if range_ > 0 else 0.5,
        "upper_wick": (row["h"] - max(row["o"], row["c"])) / range_ if range_ > 0 else 0.0,
        "body_ratio": abs(row["c"] - row["o"]) / range_ if range_ > 0 else 0.0,
        "range60_pos": (row["c"] - low60) / (high60 - low60) if high60 > low60 else 0.5,
        "dist_ma20": row["c"] / ma20 - 1,
        "dist_ma60": row["c"] / ma60 - 1,
        "drawdown_from_high120": row["c"] / high120 - 1,
    }


def _event_tags(row: dict[str, Any], feat: dict[str, float], next_row: dict[str, Any] | None) -> list[str]:
    tags = []
    bearish = row["c"] < row["o"]
    if (
        row["c"] < feat["prior_low20"]
        and feat["volume_vs20"] >= 2.0
        and feat["close_pos"] <= 0.25
        and feat["dist_ma20"] <= -0.04
    ):
        tags.append("support_break_volume")
    if (
        row["c"] < feat["prior_low20"]
        and feat["volume_vs20"] >= 3.0
        and feat["close_pos"] <= 0.10
        and feat["dist_ma20"] <= -0.10
    ):
        tags.append("support_break_capitulation")
    if (
        feat["ret20"] >= 0.20
        and feat["range60_pos"] >= 0.65
        and feat["volume_vs20"] >= 1.5
        and feat["close_pos"] <= 0.35
        and bearish
    ):
        tags.append("blowoff_weak_close")
    if next_row is not None:
        next_range = next_row["h"] - next_row["l"]
        next_close_pos = (next_row["c"] - next_row["l"]) / next_range if next_range > 0 else 0.5
        if (
            "blowoff_weak_close" in tags
            and next_row["h"] >= row["c"] * 0.98
            and next_row["c"] <= row["c"] * 1.01
            and next_row["c"] <= next_row["o"]
            and next_close_pos <= 0.45
        ):
            tags.append("blowoff_pullback_reject")
    if (
        row["c"] < feat["ma20"]
        and feat["ret20"] <= -0.08
        and feat["volume_vs20"] >= 1.5
        and feat["close_pos"] <= 0.30
        and feat["dist_ma60"] <= 0.03
    ):
        tags.append("downtrend_continuation_pressure")
    return tags


def _evaluate(bars: list[dict[str, Any]], signal_index: int, tag: str, horizon: int) -> dict[str, Any] | None:
    if signal_index + horizon >= len(bars):
        return None
    signal = bars[signal_index]
    entry_index = signal_index
    entry_price = signal["c"]
    if tag == "blowoff_pullback_reject":
        entry_index = signal_index + 1
        entry_price = bars[entry_index]["c"]
    if entry_price <= 0 or entry_index + horizon > len(bars) - 1:
        return None
    future = bars[entry_index + 1 : entry_index + horizon + 1]
    lows = [row["l"] for row in future]
    highs = [row["h"] for row in future]
    closes = [row["c"] for row in future]
    target5_day = next((i + 1 for i, low in enumerate(lows) if low <= entry_price * 0.95), None)
    target8_day = next((i + 1 for i, low in enumerate(lows) if low <= entry_price * 0.92), None)
    stop5_day = next((i + 1 for i, high in enumerate(highs) if high >= entry_price * 1.05), None)
    stop8_day = next((i + 1 for i, high in enumerate(highs) if high >= entry_price * 1.08), None)
    first_event = "time"
    first_day = horizon
    if target5_day is not None and (stop5_day is None or target5_day <= stop5_day):
        first_event = "target5"
        first_day = target5_day
    elif stop5_day is not None:
        first_event = "stop5"
        first_day = stop5_day
    return {
        "tag": tag,
        "signal_ymd": signal["ymd"],
        "entry_ymd": bars[entry_index]["ymd"],
        "entry_price": entry_price,
        "target5_hit_10d": target5_day is not None,
        "target8_hit_10d": target8_day is not None,
        "stop5_hit_10d": stop5_day is not None,
        "stop8_hit_10d": stop8_day is not None,
        "target5_first_rate_flag": first_event == "target5",
        "stop5_first_rate_flag": first_event == "stop5",
        "first_event": first_event,
        "first_event_day": first_day,
        "close10_short_ret": (entry_price - closes[-1]) / entry_price,
        "mfe10_short": (entry_price - min(lows)) / entry_price,
        "mae10_short": (entry_price - max(highs)) / entry_price,
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
        "target5_first_rate": _round(sum(1 for row in rows if row["target5_first_rate_flag"]) / len(rows)),
        "stop5_first_rate": _round(sum(1 for row in rows if row["stop5_first_rate_flag"]) / len(rows)),
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
    risk_ok = (summary.get("stop5_first_rate") or 1) <= 0.45
    target_ok = (summary.get("target5_first_rate") or 0) >= 0.35
    if positive and month_ok and year_ok and risk_ok and target_ok:
        return "keep", "positive_return_with_month_year_stability_and_target_first_gate"
    if positive and (month_ok or year_ok):
        return "hold", "positive_return_but_stability_or_risk_gate_not_complete"
    return "drop", "no_positive_10d_short_edge"


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
        ma20s = _rolling_mean(closes, 20)
        ma60s = _rolling_mean(closes, 60)
        vol20s = _rolling_mean(vols, 20)
        prior_low20s = _rolling_min(lows, 20, exclude_current=True)
        high60s = _rolling_max(highs, 60)
        low60s = _rolling_min(lows, 60)
        high120s = _rolling_max(highs, 120)
        for index in range(120, len(bars) - horizon - 1):
            feat = _features(bars, index, closes, ma20s, ma60s, vol20s, prior_low20s, high60s, low60s, high120s)
            if feat is None:
                continue
            tags = _event_tags(bars[index], feat, bars[index + 1] if index + 1 < len(bars) else None)
            for tag in tags:
                metrics = _evaluate(bars, index, tag, horizon)
                if metrics is None:
                    continue
                events.append(
                    {
                        "code": code,
                        **{key: _round(feat[key]) for key in ["ret20", "ret60", "volume_vs20", "close_pos", "range60_pos", "dist_ma20", "dist_ma60", "drawdown_from_high120"]},
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
        rollup_decision = "keep_collapse_shape_branch"
        reason = "at_least_one_collapse_shape_cleared_10d_short_edge_gates"
    elif hold:
        rollup_decision = "hold_collapse_shape_branch"
        reason = "at_least_one_collapse_shape_positive_but_not_trade_ready"
    else:
        rollup_decision = "drop_collapse_shape_branch"
        reason = "no_collapse_shape_cleared_10d_short_edge_gates"
    report = {
        "schema_version": f"{AXIS_ID}_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "db_path": str(db_path),
        "db_status": db_status,
        "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {
            "changed_axis": "collapse_shape_only",
            "horizon": horizon,
            "entry": "signal_close_except_blowoff_pullback_reject_next_close",
            "confirmed_non_yahoo_daily_bars": True,
            "gross_only": True,
            "no_runtime_mutation": True,
            "no_meemee_reflection": True,
            "no_provisional_intraday_mix": True,
        },
        "pattern_definitions": {
            "support_break_volume": "close < prior_low20, volume_vs20 >= 2.0, weak close, below MA20",
            "support_break_capitulation": "stricter support break with volume_vs20 >= 3.0 and close_pos <= 0.10",
            "blowoff_weak_close": "20d rise, high-zone, high volume, weak bearish close",
            "blowoff_pullback_reject": "blowoff weak close plus next-day failed rebound",
            "downtrend_continuation_pressure": "already falling below MA20 with volume pressure near/below MA60",
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
