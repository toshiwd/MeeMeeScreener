from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any

import duckdb

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from shared.runtime_stock_db_contract import inspect_runtime_stock_db, resolve_runtime_stock_db_path


AXIS_ID = "decline_shape_event_mining_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\decline_shape_event_mining_v1")
MA_PERIODS = (7, 20, 60, 100, 200)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.write_text("".join(json.dumps(row, ensure_ascii=False, default=str) + "\n" for row in rows), encoding="utf-8")


def _round(value: float | None, digits: int = 6) -> float | None:
    return None if value is None or not math.isfinite(value) else round(value, digits)


def _safe_div(numerator: float, denominator: float) -> float | None:
    if denominator == 0 or not math.isfinite(denominator):
        return None
    return numerator / denominator


def _moving_average(values: list[float], period: int) -> list[float | None]:
    out: list[float | None] = []
    running = 0.0
    for index, value in enumerate(values):
        running += value
        if index >= period:
            running -= values[index - period]
        out.append(running / period if index + 1 >= period else None)
    return out


def _normalize_date_expr() -> str:
    return """
    CASE
        WHEN length(CAST(abs(date) AS VARCHAR)) = 8 THEN CAST(date AS INTEGER)
        ELSE CAST(strftime(to_timestamp(CAST(date AS BIGINT)), '%Y%m%d') AS INTEGER)
    END
    """


def _load_codes(
    conn: duckdb.DuckDBPyConnection,
    *,
    start_ymd: int,
    end_ymd: int,
    limit_codes: int | None,
    exclude_etf_like: bool,
) -> list[str]:
    rows = conn.execute(
        f"""
        SELECT b.code, COUNT(*) AS n, ANY_VALUE(t.name) AS name
        FROM (
            SELECT code, {_normalize_date_expr()} AS ymd
            FROM daily_bars
            WHERE COALESCE(source, 'pan') <> 'yahoo'
        ) b
        LEFT JOIN tickers t ON t.code = b.code
        WHERE b.ymd BETWEEN ? AND ?
          AND (? = FALSE OR (
              COALESCE(t.name, '') NOT ILIKE '%ETF%'
              AND COALESCE(t.name, '') NOT ILIKE '%ETN%'
              AND COALESCE(t.name, '') NOT ILIKE '%REIT%'
              AND COALESCE(t.name, '') NOT ILIKE '%インバ%'
              AND COALESCE(t.name, '') NOT ILIKE '%ベア%'
              AND COALESCE(t.name, '') NOT ILIKE '%ブル%'
          ))
        GROUP BY b.code
        HAVING COUNT(*) >= 260
        ORDER BY b.code
        """,
        [start_ymd, end_ymd, exclude_etf_like],
    ).fetchall()
    codes = [str(row[0]) for row in rows]
    return codes[:limit_codes] if limit_codes else codes


def _load_bars(conn: duckdb.DuckDBPyConnection, code: str, *, start_ymd: int, end_ymd: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        f"""
        WITH normalized AS (
            SELECT code, {_normalize_date_expr()} AS ymd, o, h, l, c, v
            FROM daily_bars
            WHERE code = ?
              AND COALESCE(source, 'pan') <> 'yahoo'
        )
        SELECT ymd, o, h, l, c, v
        FROM normalized
        WHERE ymd BETWEEN ? AND ?
          AND o IS NOT NULL AND h IS NOT NULL AND l IS NOT NULL AND c IS NOT NULL
        ORDER BY ymd
        """,
        [code, start_ymd, end_ymd],
    ).fetchall()
    bars = [
        {
            "date": int(ymd),
            "open": float(open_),
            "high": float(high),
            "low": float(low),
            "close": float(close),
            "volume": float(volume or 0),
        }
        for ymd, open_, high, low, close, volume in rows
    ]
    closes = [row["close"] for row in bars]
    volumes = [row["volume"] for row in bars]
    for period in MA_PERIODS:
        ma = _moving_average(closes, period)
        for row, value in zip(bars, ma):
            row[f"ma{period}"] = value
    vol20 = _moving_average(volumes, 20)
    for row, value in zip(bars, vol20):
        row["vol20"] = value
    return bars


def _candle_features(row: dict[str, Any]) -> dict[str, Any]:
    span = max(row["high"] - row["low"], 0.0)
    body = abs(row["close"] - row["open"])
    upper = row["high"] - max(row["open"], row["close"])
    lower = min(row["open"], row["close"]) - row["low"]
    return {
        "body_ratio": _round(_safe_div(body, span) or 0.0),
        "upper_wick_ratio": _round(_safe_div(upper, span) or 0.0),
        "lower_wick_ratio": _round(_safe_div(lower, span) or 0.0),
        "bearish": row["close"] < row["open"],
        "bullish": row["close"] > row["open"],
        "long_upper_wick": bool(span > 0 and upper / span >= 0.45),
        "long_lower_wick": bool(span > 0 and lower / span >= 0.45),
        "large_body": bool(span > 0 and body / span >= 0.65),
    }


def _feature_at(bars: list[dict[str, Any]], index: int) -> dict[str, Any]:
    row = bars[index]
    prev = bars[index - 1] if index > 0 else None
    closes = [bar["close"] for bar in bars[: index + 1]]
    highs = [bar["high"] for bar in bars[: index + 1]]
    lows = [bar["low"] for bar in bars[: index + 1]]
    candle = _candle_features(row)
    lookback20_low = min(lows[-21:-1]) if len(lows) >= 21 else None
    lookback20_high = max(highs[-21:-1]) if len(highs) >= 21 else None
    lookback60_high = max(highs[-61:-1]) if len(highs) >= 61 else None
    close = row["close"]
    out: dict[str, Any] = {
        **candle,
        "close_position_60": None,
        "ret5": None,
        "ret20": None,
        "volume_ratio20": None,
        "failed_high20": False,
        "break_support20_close": False,
        "break_support20_low": False,
        "near_high60": False,
        "ma_stack_bull": False,
        "ma_stack_bear": False,
    }
    if len(closes) >= 6:
        out["ret5"] = _round(close / closes[-6] - 1.0)
    if len(closes) >= 21:
        out["ret20"] = _round(close / closes[-21] - 1.0)
    if len(highs) >= 60:
        high60 = max(highs[-60:])
        low60 = min(lows[-60:])
        out["close_position_60"] = _round(_safe_div(close - low60, high60 - low60))
    if row.get("vol20"):
        out["volume_ratio20"] = _round(_safe_div(row["volume"], float(row["vol20"])))
    if lookback20_high:
        out["failed_high20"] = bool(row["high"] > lookback20_high and row["close"] < lookback20_high)
    if lookback20_low:
        out["break_support20_close"] = bool(row["close"] < lookback20_low)
        out["break_support20_low"] = bool(row["low"] < lookback20_low)
    if lookback60_high:
        out["near_high60"] = bool(close >= lookback60_high * 0.97)
    ma_values = {period: row.get(f"ma{period}") for period in MA_PERIODS}
    for period, ma in ma_values.items():
        out[f"dist_ma{period}"] = _round(close / ma - 1.0) if ma else None
        out[f"below_ma{period}"] = bool(ma and close < ma)
        out[f"cross_down_ma{period}"] = bool(
            prev and ma and prev.get(f"ma{period}") and prev["close"] >= float(prev[f"ma{period}"]) and close < ma
        )
        out[f"ma{period}_slope5"] = _round((ma / bars[index - 5][f"ma{period}"] - 1.0) if index >= 5 and ma and bars[index - 5].get(f"ma{period}") else None)
    if all(row.get(f"ma{period}") for period in (7, 20, 60)):
        out["ma_stack_bull"] = bool(row["ma7"] > row["ma20"] > row["ma60"])
        out["ma_stack_bear"] = bool(row["ma7"] < row["ma20"] < row["ma60"])
    return out


def _first_index(indices: list[int]) -> int | None:
    return indices[0] if indices else None


def _classify_event(pre: dict[str, Any], breakdown: dict[str, Any] | None) -> list[str]:
    tags: list[str] = []
    if pre.get("near_high60") and pre.get("long_upper_wick"):
        tags.append("high_zone_upper_wick")
    if pre.get("failed_high20"):
        tags.append("failed_high20")
    if pre.get("ma_stack_bull") and pre.get("cross_down_ma7"):
        tags.append("bull_stack_lost_ma7")
    if pre.get("below_ma20") and (pre.get("dist_ma20") or 0) < 0:
        tags.append("under_ma20_pressure")
    if breakdown:
        if breakdown.get("break_support20_close"):
            tags.append("close_break_support20")
        if breakdown.get("cross_down_ma20"):
            tags.append("cross_down_ma20")
        if breakdown.get("cross_down_ma60"):
            tags.append("cross_down_ma60")
        if breakdown.get("large_body") and breakdown.get("bearish"):
            tags.append("large_bear_breakdown")
        if breakdown.get("volume_ratio20") is not None and breakdown["volume_ratio20"] >= 1.5:
            tags.append("volume_expansion_break")
    return tags or ["unclassified"]


def _mine_code_events(
    *,
    code: str,
    bars: list[dict[str, Any]],
    forward_window: int,
    min_decline_pct: float,
    pre_window: int,
    cooldown: int,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    index = 220
    while index < len(bars) - forward_window:
        anchor = bars[index]
        future = bars[index + 1 : index + forward_window + 1]
        bottom_offset, bottom_bar = min(enumerate(future, start=1), key=lambda item: item[1]["low"])
        decline_pct = bottom_bar["low"] / anchor["close"] - 1.0
        if decline_pct > -abs(min_decline_pct):
            index += 1
            continue
        pre_start = max(0, index - pre_window + 1)
        pre_indices = list(range(pre_start, index + 1))
        pre_features = [(idx, _feature_at(bars, idx)) for idx in pre_indices]
        trigger_idx = _first_index([
            idx for idx, feat in pre_features
            if feat["failed_high20"] or feat["long_upper_wick"] or feat["cross_down_ma7"] or feat["cross_down_ma20"]
        ]) or index
        path_indices = list(range(trigger_idx, index + bottom_offset + 1))
        path_features = [(idx, _feature_at(bars, idx)) for idx in path_indices]
        breakdown_idx = _first_index([
            idx for idx, feat in path_features
            if feat["break_support20_close"] or feat["cross_down_ma20"] or feat["cross_down_ma60"]
        ])
        pre_feat = _feature_at(bars, trigger_idx)
        breakdown_feat = _feature_at(bars, breakdown_idx) if breakdown_idx is not None else None
        bottom_feat = _feature_at(bars, index + bottom_offset)
        tags = _classify_event(pre_feat, breakdown_feat)
        events.append({
            "event_id": f"{code}:{anchor['date']}:{bottom_bar['date']}",
            "code": code,
            "anchor_as_of": int(anchor["date"]),
            "trigger_as_of": int(bars[trigger_idx]["date"]),
            "breakdown_as_of": int(bars[breakdown_idx]["date"]) if breakdown_idx is not None else None,
            "bottom_as_of": int(bottom_bar["date"]),
            "days_trigger_to_bottom": int(index + bottom_offset - trigger_idx),
            "days_anchor_to_bottom": int(bottom_offset),
            "anchor_close": _round(anchor["close"]),
            "bottom_low": _round(bottom_bar["low"]),
            "decline_pct": _round(decline_pct),
            "rebound_10d_from_bottom": _round(
                bars[index + bottom_offset + 10]["close"] / bottom_bar["low"] - 1.0
                if index + bottom_offset + 10 < len(bars) else None
            ),
            "tags": tags,
            "primary_tag": tags[0],
            "trigger_features": pre_feat,
            "breakdown_features": breakdown_feat,
            "bottom_features": bottom_feat,
            "image_samples": [
                {"stage": "trigger", "code": code, "as_of": int(bars[trigger_idx]["date"])},
                *([{"stage": "breakdown", "code": code, "as_of": int(bars[breakdown_idx]["date"])}] if breakdown_idx is not None else []),
                {"stage": "bottom", "code": code, "as_of": int(bottom_bar["date"])},
            ],
        })
        index += max(cooldown, bottom_offset)
    return events


def _summary(events: list[dict[str, Any]]) -> dict[str, Any]:
    by_tag: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        by_tag[str(event["primary_tag"])].append(event)
    tag_summary: dict[str, Any] = {}
    for tag, rows in sorted(by_tag.items(), key=lambda item: (-len(item[1]), item[0])):
        declines = [float(row["decline_pct"]) for row in rows]
        rebounds = [float(row["rebound_10d_from_bottom"]) for row in rows if row.get("rebound_10d_from_bottom") is not None]
        tag_summary[tag] = {
            "count": len(rows),
            "decline_pct_mean": _round(mean(declines)),
            "decline_pct_median": _round(median(declines)),
            "rebound_10d_from_bottom_mean": _round(mean(rebounds)) if rebounds else None,
            "common_secondary_tags": dict(Counter(tag for row in rows for tag in row["tags"]).most_common(8)),
        }
    return {
        "event_count": len(events),
        "unique_code_count": len({row["code"] for row in events}),
        "tag_summary": tag_summary,
    }


def run(
    *,
    db_path: Path,
    output_root: Path,
    start_ymd: int,
    end_ymd: int,
    forward_window: int,
    min_decline_pct: float,
    pre_window: int,
    cooldown: int,
    limit_codes: int | None,
    max_events: int | None,
    exclude_etf_like: bool,
) -> Path:
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        codes = _load_codes(
            conn,
            start_ymd=start_ymd,
            end_ymd=end_ymd,
            limit_codes=limit_codes,
            exclude_etf_like=exclude_etf_like,
        )
        events: list[dict[str, Any]] = []
        for code in codes:
            bars = _load_bars(conn, code, start_ymd=start_ymd - 10000, end_ymd=end_ymd)
            if len(bars) < 260 + forward_window:
                continue
            events.extend(_mine_code_events(
                code=code,
                bars=bars,
                forward_window=forward_window,
                min_decline_pct=min_decline_pct,
                pre_window=pre_window,
                cooldown=cooldown,
            ))
            if max_events and len(events) >= max_events:
                events = events[:max_events]
                break
    finally:
        conn.close()
    events = sorted(events, key=lambda row: (row["anchor_as_of"], row["code"], row["bottom_as_of"]))
    _write_jsonl(output_dir / "decline_events.jsonl", events)
    image_plan = [
        {
            "event_id": event["event_id"],
            "stage": sample["stage"],
            "code": sample["code"],
            "as_of": sample["as_of"],
            "as_of_iso": f"{str(sample['as_of'])[:4]}-{str(sample['as_of'])[4:6]}-{str(sample['as_of'])[6:8]}",
            "primary_tag": event["primary_tag"],
            "decline_pct": event["decline_pct"],
        }
        for event in events
        for sample in event["image_samples"]
    ]
    _write_jsonl(output_dir / "image_sample_plan.jsonl", image_plan)
    with (output_dir / "decline_events_flat.csv").open("w", newline="", encoding="utf-8") as handle:
        fieldnames = [
            "event_id", "code", "anchor_as_of", "trigger_as_of", "breakdown_as_of", "bottom_as_of",
            "days_trigger_to_bottom", "decline_pct", "primary_tag", "tags",
        ]
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for event in events:
            writer.writerow({key: json.dumps(event[key], ensure_ascii=False) if key == "tags" else event.get(key) for key in fieldnames})
    summary = _summary(events)
    audit = {
        "schema_version": "tradex_decline_shape_event_mining_v1_audit",
        "generated_at": _utc_now(),
        "boundary_owner": "TRADEX",
        "axis_id": AXIS_ID,
        "db_path": str(db_path),
        "runtime_stock_db_status": inspect_runtime_stock_db(runtime_db_path=db_path),
        "fixed_event_definition": {
            "start_ymd": start_ymd,
            "end_ymd": end_ymd,
            "forward_window": forward_window,
            "min_decline_pct": -abs(min_decline_pct),
            "pre_window": pre_window,
            "cooldown": cooldown,
            "exclude_etf_like": exclude_etf_like,
        },
        "feature_family": [
            "candlestick_wicks_body",
            "MA7/20/60/100/200 distance and cross-down",
            "20-day support break",
            "failed 20-day high",
            "volume expansion",
        ],
        **summary,
        "image_sample_count": len(image_plan),
        "labels_used_in_image_rendering": False,
        "runtime_db_write": False,
        "production_ranking_changed": False,
        "judgment": "pass_decline_shape_event_dataset_pilot" if events else "hold_no_decline_events_found",
        "non_scope": ["model training", "MeeMee reflection", "production ranking mutation", "runtime DB write"],
    }
    _write_json(output_dir / "decline_shape_event_audit.json", audit)
    _write_json(output_root / "latest_decline_shape_event_audit.json", {"run_root": str(output_dir), **audit})
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=None)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--start-ymd", type=int, default=20240101)
    parser.add_argument("--end-ymd", type=int, default=20260629)
    parser.add_argument("--forward-window", type=int, default=30)
    parser.add_argument("--min-decline-pct", type=float, default=0.12)
    parser.add_argument("--pre-window", type=int, default=10)
    parser.add_argument("--cooldown", type=int, default=20)
    parser.add_argument("--limit-codes", type=int, default=None)
    parser.add_argument("--max-events", type=int, default=None)
    parser.add_argument("--include-etf-like", action="store_true")
    args = parser.parse_args()
    print(run(
        db_path=args.db_path or resolve_runtime_stock_db_path(),
        output_root=args.output_root,
        start_ymd=args.start_ymd,
        end_ymd=args.end_ymd,
        forward_window=args.forward_window,
        min_decline_pct=args.min_decline_pct,
        pre_window=args.pre_window,
        cooldown=args.cooldown,
        limit_codes=args.limit_codes,
        max_events=args.max_events,
        exclude_etf_like=not args.include_etf_like,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
