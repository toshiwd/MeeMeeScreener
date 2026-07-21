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

from tradex_short_shape_bad_avoidance_probe_v1 import _daily_rows


AXIS_ID = "monthly_breakdown_condition_probe_v1"
DEFAULT_DB_PATH = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\monthly_breakdown_condition_probe_v1")


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


def _bucket(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"n": 0}
    return {
        "n": len(rows),
        "down5_rate": sum(1 for row in rows if float(row["ret5"]) <= -0.03) / len(rows),
        "down20_rate": sum(1 for row in rows if float(row["ret20"]) <= -0.08) / len(rows),
        "wrong5_rate": sum(1 for row in rows if float(row["MFE5"]) >= 0.03) / len(rows),
        "wrong20_rate": sum(1 for row in rows if float(row["MFE20"]) >= 0.05) / len(rows),
        "avg_ret5": sum(float(row["ret5"]) for row in rows) / len(rows),
        "avg_ret20": sum(float(row["ret20"]) for row in rows) / len(rows),
        "avg_MAE20": sum(float(row["MAE20"]) for row in rows) / len(rows),
        "avg_MFE20": sum(float(row["MFE20"]) for row in rows) / len(rows),
    }


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
                "first_ymd": int(rows[0][0]),
                "last_ymd": int(rows[-1][0]),
                "open": float(rows[0][1]),
                "high": max(float(row[2]) for row in rows),
                "low": min(float(row[3]) for row in rows),
                "close": float(rows[-1][4]),
                "volume": sum(float(row[5] or 0) for row in rows),
            }
        )
    return out


def _month_index(months: list[dict[str, Any]], ymd: int) -> int | None:
    ym = int(str(ymd)[:6])
    for i, row in enumerate(months):
        if int(row["month"]) == ym:
            return i
    return None


def _event_for(code: str, name: str, bars: list[tuple[int, float, float, float, float, float]], index: int) -> dict[str, Any] | None:
    if index < 260 or index + 20 >= len(bars):
        return None
    months = _month_rows(bars[: index + 1])
    mi = _month_index(months, int(bars[index][0]))
    if mi is None or mi < 8:
        return None

    closes = [float(row[4]) for row in bars]
    highs = [float(row[2]) for row in bars]
    lows = [float(row[3]) for row in bars]
    open_, high, low, close = map(float, bars[index][1:5])
    ma7 = _ma(closes, index, 7)
    ma20 = _ma(closes, index, 20)
    ma60 = _ma(closes, index, 60)
    if ma7 is None or ma20 is None or ma60 is None:
        return None

    m = months[mi]
    prev_m = months[mi - 1]
    prev2 = months[mi - 2]
    m_range = float(m["high"]) - float(m["low"])
    if m_range <= 0:
        return None
    month_upper_wick = (float(m["high"]) - max(float(m["open"]), float(m["close"]))) / m_range
    month_body_red = float(m["close"]) < float(m["open"])
    month_extension3 = float(m["high"]) / min(float(months[j]["low"]) for j in range(mi - 3, mi)) - 1.0
    month_failed_new_high = float(m["high"]) > max(float(prev_m["high"]), float(prev2["high"])) and float(m["close"]) < float(prev_m["high"])
    month_prev_low_break = close < float(prev_m["low"])
    month_mid_break = close < (float(m["high"]) + float(m["low"])) / 2
    day_below20 = close < ma20
    day_below60 = close < ma60
    day_ma7_below20 = ma7 < ma20
    recent_high20 = max(highs[index - 20 : index])
    close_vs_high20 = close / recent_high20 - 1.0
    high_zone = close_vs_high20 >= -0.12
    day_trigger_break = close < min(lows[index - 5 : index])
    trigger_plan = {
        "break_low": low,
        "close_below": min(close, low + (high - low) * 0.35),
        "invalid_high": high,
        "hard_invalid": high + (high - low) * 0.5,
    }
    next_bar = bars[index + 1]
    next_high = float(next_bar[2])
    next_low = float(next_bar[3])
    next_close = float(next_bar[4])
    if next_high > trigger_plan["hard_invalid"]:
        next_trigger_status = "hard_invalidated"
    elif next_high > trigger_plan["invalid_high"]:
        next_trigger_status = "invalidated_by_high_break"
    elif next_low < trigger_plan["break_low"] and next_close <= trigger_plan["close_below"]:
        next_trigger_status = "confirmed_downside_rejection"
    elif next_low < trigger_plan["break_low"]:
        next_trigger_status = "intraday_low_break_only"
    elif next_close <= trigger_plan["close_below"]:
        next_trigger_status = "close_rejection_only"
    else:
        next_trigger_status = "still_waiting"
    ret5 = closes[index + 5] / close - 1.0
    future20 = closes[index + 1 : index + 21]
    ret20 = closes[index + 20] / close - 1.0
    mae20 = min(future20) / close - 1.0
    mfe20 = max(future20) / close - 1.0
    future5 = closes[index + 1 : index + 6]
    mae5 = min(future5) / close - 1.0
    mfe5 = max(future5) / close - 1.0

    tags = []
    if month_extension3 >= 0.25:
        tags.append("monthly_extension_25pct")
    if month_upper_wick >= 0.35:
        tags.append("monthly_upper_wick")
    if month_body_red:
        tags.append("monthly_red_body")
    if month_failed_new_high:
        tags.append("monthly_failed_new_high")
    if month_prev_low_break:
        tags.append("monthly_prev_low_break")
    if month_mid_break:
        tags.append("monthly_mid_break")
    if day_below20:
        tags.append("daily_below20")
    if day_below60:
        tags.append("daily_below60")
    if day_ma7_below20:
        tags.append("daily_ma7_below20")
    if high_zone:
        tags.append("near_high_zone")
    if day_trigger_break:
        tags.append("daily_trigger_break")
    tags.append(f"next_{next_trigger_status}")

    if not {"monthly_extension_25pct", "near_high_zone", "daily_below20"}.issubset(set(tags)):
        return None
    if not (month_upper_wick >= 0.25 or month_failed_new_high or month_mid_break or day_trigger_break):
        return None

    return {
        "code": code,
        "name": name,
        "as_of": int(bars[index][0]),
        "month": int(m["month"]),
        "tags": tags,
        "next_trigger_status": next_trigger_status,
        "month_extension3": round(month_extension3, 8),
        "month_upper_wick": round(month_upper_wick, 8),
        "close_vs_high20": round(close_vs_high20, 8),
        "close_vs_ma20": round(close / ma20 - 1.0, 8),
        "close_vs_ma60": round(close / ma60 - 1.0, 8),
        "ret5": round(ret5, 8),
        "ret20": round(ret20, 8),
        "MAE5": round(mae5, 8),
        "MFE5": round(mfe5, 8),
        "MAE20": round(mae20, 8),
        "MFE20": round(mfe20, 8),
    }


def _combo_summary(events: list[dict[str, Any]], combos: list[tuple[str, ...]]) -> list[dict[str, Any]]:
    rows = []
    for combo in combos:
        selected = [event for event in events if set(combo).issubset(set(event["tags"]))]
        if len(selected) < 20:
            continue
        bucket = _bucket(selected)
        decision = "keep_monthly_breakdown_candidate"
        reasons = []
        if bucket["wrong20_rate"] > 0.18:
            reasons.append("wrong20_rate_gt_18pct")
        if bucket["avg_ret20"] >= -0.02:
            reasons.append("avg_ret20_not_below_minus_2pct")
        if bucket["down20_rate"] < 0.25:
            reasons.append("down20_rate_lt_25pct")
        if reasons:
            decision = "hold_or_drop_monthly_breakdown_candidate"
        rows.append(
            {
                "combo": list(combo),
                "summary": bucket,
                "decision": decision,
                "reasons": reasons,
                "examples": sorted(selected, key=lambda row: float(row["ret20"]))[:10],
            }
        )
    return sorted(
        rows,
        key=lambda row: (
            row["decision"] != "keep_monthly_breakdown_candidate",
            float(row["summary"]["avg_ret20"]),
            float(row["summary"]["wrong20_rate"]),
            -int(row["summary"]["n"]),
        ),
    )


def run(*, db_path: Path, output_root: Path, start_ymd: int, max_codes: int | None) -> Path:
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        codes = [
            (str(code), str(name or ""))
            for code, name in conn.execute("SELECT code, name FROM tickers ORDER BY code").fetchall()
            if str(code).isdigit() and int(str(code)) >= 1300
        ]
        if max_codes:
            codes = codes[:max_codes]
        events: list[dict[str, Any]] = []
        for code, name in codes:
            bars = _daily_rows(conn, code)
            if len(bars) < 320:
                continue
            ymds = [int(row[0]) for row in bars]
            last_index_by_month: dict[int, int] = {}
            for index, ymd in enumerate(ymds):
                last_index_by_month[int(str(ymd)[:6])] = index
            for index in sorted(last_index_by_month.values()):
                ymd = ymds[index]
                if ymd < start_ymd:
                    continue
                event = _event_for(code, name, bars, index)
                if event:
                    events.append(event)
    finally:
        conn.close()

    combos = [
        ("monthly_extension_25pct", "daily_below20", "near_high_zone"),
        ("monthly_extension_25pct", "monthly_upper_wick", "daily_below20", "near_high_zone"),
        ("monthly_extension_25pct", "monthly_failed_new_high", "daily_below20", "near_high_zone"),
        ("monthly_extension_25pct", "monthly_mid_break", "daily_below20", "near_high_zone"),
        ("monthly_extension_25pct", "daily_trigger_break", "daily_below20", "near_high_zone"),
        ("monthly_extension_25pct", "daily_below60", "daily_below20", "near_high_zone"),
        ("monthly_extension_25pct", "monthly_prev_low_break", "daily_below20", "near_high_zone"),
        ("monthly_extension_25pct", "monthly_upper_wick", "daily_ma7_below20", "daily_below20", "near_high_zone"),
        ("monthly_extension_25pct", "next_confirmed_downside_rejection", "daily_below20", "near_high_zone"),
        ("monthly_extension_25pct", "monthly_upper_wick", "next_confirmed_downside_rejection", "daily_below20", "near_high_zone"),
        ("monthly_extension_25pct", "monthly_failed_new_high", "next_confirmed_downside_rejection", "daily_below20", "near_high_zone"),
        ("monthly_extension_25pct", "monthly_mid_break", "next_confirmed_downside_rejection", "daily_below20", "near_high_zone"),
        ("monthly_extension_25pct", "daily_ma7_below20", "next_confirmed_downside_rejection", "daily_below20", "near_high_zone"),
        ("monthly_extension_25pct", "daily_below60", "next_confirmed_downside_rejection", "daily_below20", "near_high_zone"),
    ]
    combo_summary = _combo_summary(events, combos)
    report = {
        "schema_version": AXIS_ID,
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "db_path": str(db_path),
        "fixed_evaluation_conditions": {
            "source": "confirmed non-yahoo daily_bars",
            "start_ymd": start_ymd,
            "max_codes": max_codes,
        },
        "event_count": len(events),
        "overall": _bucket(events),
        "combo_summary": combo_summary,
        "decision": {
            "candidate_local_decision": "monthly_breakdown_conditions_ranked",
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": "monthly uptrend failure tags are compared on 20-day downside and wrong-side risk",
        },
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
    }
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    _write_json(output_dir / "monthly_breakdown_condition_probe.json", report)
    _write_json(output_root / "latest_monthly_breakdown_condition_probe.json", {"run_root": str(output_dir), **report})
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--start-ymd", type=int, default=20210101)
    parser.add_argument("--max-codes", type=int, default=None)
    args = parser.parse_args()
    print(run(db_path=args.db_path, output_root=args.output_root, start_ymd=args.start_ymd, max_codes=args.max_codes))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
