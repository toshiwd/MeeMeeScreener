from __future__ import annotations

import argparse
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


AXIS_ID = "short_setup_universe_backtest_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_setup_universe_backtest_v1")
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


def _ma(values: list[float], period: int) -> list[float | None]:
    out: list[float | None] = []
    total = 0.0
    for index, value in enumerate(values):
        total += value
        if index >= period:
            total -= values[index - period]
        out.append(total / period if index + 1 >= period else None)
    return out


def _date_expr() -> str:
    return """
    CASE
        WHEN length(CAST(abs(date) AS VARCHAR)) = 8 THEN CAST(date AS INTEGER)
        ELSE CAST(strftime(to_timestamp(CAST(date AS BIGINT)), '%Y%m%d') AS INTEGER)
    END
    """


def _load_codes(conn: duckdb.DuckDBPyConnection, *, start_ymd: int, end_ymd: int, limit_codes: int | None) -> list[str]:
    rows = conn.execute(
        f"""
        SELECT b.code, COUNT(*) AS n
        FROM (
            SELECT code, {_date_expr()} AS ymd
            FROM daily_bars
            WHERE COALESCE(source, 'pan') <> 'yahoo'
        ) b
        LEFT JOIN tickers t ON t.code = b.code
        WHERE b.ymd BETWEEN ? AND ?
          AND COALESCE(t.name, '') NOT ILIKE '%ETF%'
          AND COALESCE(t.name, '') NOT ILIKE '%ETN%'
          AND COALESCE(t.name, '') NOT ILIKE '%REIT%'
          AND COALESCE(t.name, '') NOT ILIKE '%インバ%'
          AND COALESCE(t.name, '') NOT ILIKE '%ベア%'
          AND COALESCE(t.name, '') NOT ILIKE '%ブル%'
        GROUP BY b.code
        HAVING COUNT(*) >= 260
        ORDER BY b.code
        """,
        [start_ymd, end_ymd],
    ).fetchall()
    codes = [str(row[0]) for row in rows]
    return codes[:limit_codes] if limit_codes else codes


def _load_bars(conn: duckdb.DuckDBPyConnection, code: str, *, start_ymd: int, end_ymd: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        f"""
        WITH normalized AS (
            SELECT code, {_date_expr()} AS ymd, o, h, l, c, v
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
        {"date": int(d), "open": float(o), "high": float(h), "low": float(l), "close": float(c), "volume": float(v or 0)}
        for d, o, h, l, c, v in rows
    ]
    closes = [bar["close"] for bar in bars]
    volumes = [bar["volume"] for bar in bars]
    for period in MA_PERIODS:
        for bar, value in zip(bars, _ma(closes, period)):
            bar[f"ma{period}"] = value
    for bar, value in zip(bars, _ma(volumes, 20)):
        bar["vol20"] = value
    return bars


def _features(bars: list[dict[str, Any]], index: int) -> dict[str, Any]:
    row = bars[index]
    prev = bars[index - 1] if index > 0 else None
    span = max(row["high"] - row["low"], 0.0)
    upper = row["high"] - max(row["open"], row["close"])
    lower = min(row["open"], row["close"]) - row["low"]
    body = abs(row["close"] - row["open"])
    high60 = max(bar["high"] for bar in bars[max(0, index - 59): index + 1])
    low60 = min(bar["low"] for bar in bars[max(0, index - 59): index + 1])
    prior_high20 = max(bar["high"] for bar in bars[index - 20:index]) if index >= 20 else None
    prior_low20 = min(bar["low"] for bar in bars[index - 20:index]) if index >= 20 else None
    close = row["close"]
    out: dict[str, Any] = {
        "upper_wick_ratio": upper / span if span > 0 else 0.0,
        "lower_wick_ratio": lower / span if span > 0 else 0.0,
        "body_ratio": body / span if span > 0 else 0.0,
        "bearish": row["close"] < row["open"],
        "close_position_60": (close - low60) / (high60 - low60) if high60 > low60 else 0.5,
        "failed_high20": bool(prior_high20 and row["high"] > prior_high20 and row["close"] < prior_high20),
        "break_support20_close": bool(prior_low20 and row["close"] < prior_low20),
        "break_support20_low": bool(prior_low20 and row["low"] < prior_low20),
        "near_high60": close >= high60 * 0.97,
        "volume_ratio20": row["volume"] / row["vol20"] if row.get("vol20") else None,
    }
    for period in MA_PERIODS:
        ma = row.get(f"ma{period}")
        prev_ma = prev.get(f"ma{period}") if prev else None
        out[f"dist_ma{period}"] = close / ma - 1.0 if ma else None
        out[f"below_ma{period}"] = bool(ma and close < ma)
        out[f"cross_down_ma{period}"] = bool(prev and ma and prev_ma and prev["close"] >= prev_ma and close < ma)
        out[f"ma{period}_slope5"] = (ma / bars[index - 5][f"ma{period}"] - 1.0) if index >= 5 and ma and bars[index - 5].get(f"ma{period}") else None
    out["ma_stack_bull"] = bool(row.get("ma7") and row.get("ma20") and row.get("ma60") and row["ma7"] > row["ma20"] > row["ma60"])
    return out


def _tags(feature: dict[str, Any]) -> list[str]:
    high_zone = feature["close_position_60"] >= 0.75 or feature["near_high60"]
    upper_wick = feature["upper_wick_ratio"] >= 0.40
    failed_high = feature["failed_high20"]
    lost_ma7 = feature["cross_down_ma7"] or feature["below_ma7"]
    lost_ma20 = feature["cross_down_ma20"] or feature["below_ma20"]
    support_break = feature["break_support20_close"] or feature["break_support20_low"]
    large_bear = feature["body_ratio"] >= 0.55 and feature["bearish"]
    volume = (feature.get("volume_ratio20") or 0.0) >= 1.5
    extended = (feature.get("dist_ma20") or 0.0) >= 0.08 or (feature.get("dist_ma60") or 0.0) >= 0.18
    ma20_pressure = feature["below_ma20"] and (feature.get("ma20_slope5") or 0.0) <= 0.0
    tags: list[str] = []
    if high_zone and upper_wick:
        tags.append("top_upper_wick")
    if high_zone and failed_high:
        tags.append("top_failed_high")
    if feature["ma_stack_bull"] and high_zone and lost_ma7:
        tags.append("bull_stack_lost_ma7")
    if high_zone and extended and (upper_wick or failed_high):
        tags.append("extended_high_rejection")
    if lost_ma20:
        tags.append("ma20_loss")
    if ma20_pressure:
        tags.append("ma20_pressure")
    if support_break:
        tags.append("support20_break")
    if lost_ma20 and large_bear and volume:
        tags.append("ma20_loss_large_bear_volume")
    if lost_ma20 and feature["below_ma60"]:
        tags.append("ma20_loss_to_ma60_under")
    return tags


def _forward_short_outcome(bars: list[dict[str, Any]], index: int, horizon: int) -> dict[str, Any] | None:
    if index + horizon >= len(bars):
        return None
    entry = bars[index]["close"]
    future = bars[index + 1:index + horizon + 1]
    low = min(bar["low"] for bar in future)
    high = max(bar["high"] for bar in future)
    close_h = bars[index + horizon]["close"]
    return {
        "ret_short": entry / close_h - 1.0,
        "mfe_short": entry / low - 1.0,
        "mae_short": entry / high - 1.0,
    }


def _summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"count": 0}
    ret = [float(row["ret20_short"]) for row in rows]
    mfe = [float(row["mfe20_short"]) for row in rows]
    mae = [float(row["mae20_short"]) for row in rows]
    return {
        "count": len(rows),
        "unique_code_count": len({row["code"] for row in rows}),
        "ret20_short_mean": _round(mean(ret)),
        "ret20_short_median": _round(median(ret)),
        "ret20_short_positive_rate": _round(sum(1 for value in ret if value > 0) / len(ret)),
        "mfe20_short_ge_8pct_rate": _round(sum(1 for value in mfe if value >= 0.08) / len(mfe)),
        "mfe20_short_ge_12pct_rate": _round(sum(1 for value in mfe if value >= 0.12) / len(mfe)),
        "mae20_short_le_minus5pct_rate": _round(sum(1 for value in mae if value <= -0.05) / len(mae)),
        "mae20_short_le_minus8pct_rate": _round(sum(1 for value in mae if value <= -0.08) / len(mae)),
    }


def run(*, db_path: Path, output_root: Path, start_ymd: int, end_ymd: int, limit_codes: int | None) -> Path:
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    rows: list[dict[str, Any]] = []
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        codes = _load_codes(conn, start_ymd=start_ymd, end_ymd=end_ymd, limit_codes=limit_codes)
        for code in codes:
            bars = _load_bars(conn, code, start_ymd=start_ymd - 10000, end_ymd=end_ymd)
            for index in range(220, len(bars) - 31):
                if not (start_ymd <= bars[index]["date"] <= end_ymd):
                    continue
                feature = _features(bars, index)
                tags = _tags(feature)
                if not tags:
                    continue
                outcome20 = _forward_short_outcome(bars, index, 20)
                outcome30 = _forward_short_outcome(bars, index, 30)
                if not outcome20 or not outcome30:
                    continue
                rows.append({
                    "code": code,
                    "as_of": int(bars[index]["date"]),
                    "tags": tags,
                    "close": _round(bars[index]["close"]),
                    "ret20_short": _round(outcome20["ret_short"]),
                    "mfe20_short": _round(outcome20["mfe_short"]),
                    "mae20_short": _round(outcome20["mae_short"]),
                    "ret30_short": _round(outcome30["ret_short"]),
                    "mfe30_short": _round(outcome30["mfe_short"]),
                    "mae30_short": _round(outcome30["mae_short"]),
                    "feature": {key: _round(value) if isinstance(value, float) else value for key, value in feature.items()},
                })
    finally:
        conn.close()

    by_tag: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        for tag in row["tags"]:
            by_tag[tag].append(row)
    tag_summary = {tag: _summary(items) for tag, items in sorted(by_tag.items(), key=lambda item: (-len(item[1]), item[0]))}
    combos: Counter[str] = Counter("|".join(row["tags"]) for row in rows)
    _write_jsonl(output_dir / "short_setup_universe_rows.jsonl", rows)
    _write_json(output_dir / "short_setup_universe_compare.json", {
        "schema_version": "tradex_short_setup_universe_compare_v1",
        "generated_at": _utc_now(),
        "tag_summary": tag_summary,
        "top_tag_combinations": dict(combos.most_common(50)),
    })
    audit = {
        "schema_version": "tradex_short_setup_universe_backtest_v1_audit",
        "generated_at": _utc_now(),
        "boundary_owner": "TRADEX",
        "axis_id": AXIS_ID,
        "db_path": str(db_path),
        "runtime_stock_db_status": inspect_runtime_stock_db(runtime_db_path=db_path),
        "start_ymd": start_ymd,
        "end_ymd": end_ymd,
        "tagged_row_count": len(rows),
        "tag_count": len(tag_summary),
        "tag_summary": tag_summary,
        "labels_used_in_image_rendering": False,
        "runtime_db_write": False,
        "production_ranking_changed": False,
        "judgment": "pass_universe_short_setup_backtest" if rows else "hold_no_tagged_rows",
        "non_scope": ["model training", "MeeMee reflection", "production ranking mutation"],
    }
    _write_json(output_dir / "short_setup_universe_audit.json", audit)
    _write_json(output_root / "latest_short_setup_universe_audit.json", {"run_root": str(output_dir), **audit})
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=None)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--start-ymd", type=int, default=20200101)
    parser.add_argument("--end-ymd", type=int, default=20260629)
    parser.add_argument("--limit-codes", type=int, default=None)
    args = parser.parse_args()
    print(run(
        db_path=args.db_path or resolve_runtime_stock_db_path(),
        output_root=args.output_root,
        start_ymd=args.start_ymd,
        end_ymd=args.end_ymd,
        limit_codes=args.limit_codes,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
