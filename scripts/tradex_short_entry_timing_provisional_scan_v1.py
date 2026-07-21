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

from tradex_short_entry_timing_current_scan_v1 import DEFAULT_DB_PATH, DEFAULT_OUTPUT_ROOT, RULES
from tradex_short_shape_numeric_rule_probe_v1 import FEATURE_NAMES, _apply_rule


AXIS_ID = "short_entry_timing_provisional_scan_v1"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _normalize_expr() -> str:
    return """
    CASE
        WHEN length(CAST(abs(date) AS VARCHAR)) = 8 THEN CAST(date AS INTEGER)
        ELSE CAST(strftime(to_timestamp(CAST(date AS BIGINT)), '%Y%m%d') AS INTEGER)
    END
    """


def _ma(values: list[float], end_index: int, period: int) -> float | None:
    start = end_index - period + 1
    if start < 0:
        return None
    return sum(values[start : end_index + 1]) / period


def _load_combined_bars(conn: duckdb.DuckDBPyConnection, code: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        f"""
        WITH normalized AS (
            SELECT
                {_normalize_expr()} AS ymd,
                COALESCE(source, 'pan') AS src,
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
                       ORDER BY CASE WHEN src = 'yahoo' THEN 1 ELSE 0 END DESC
                   ) AS rn
            FROM normalized
        )
        SELECT ymd, src, o, h, l, c, v
        FROM ranked
        WHERE rn = 1
        ORDER BY ymd
        """,
        [code],
    ).fetchall()
    return [
        {
            "date": int(ymd),
            "source": str(source),
            "open": float(open_),
            "high": float(high),
            "low": float(low),
            "close": float(close),
            "volume": float(volume or 0),
        }
        for ymd, source, open_, high, low, close, volume in rows
    ]


def _feature_row(bars: list[dict[str, Any]], index: int) -> dict[str, float] | None:
    if index < 60:
        return None
    closes = [row["close"] for row in bars]
    highs = [row["high"] for row in bars]
    lows = [row["low"] for row in bars]
    volumes = [row["volume"] for row in bars]
    current = bars[index]
    candle_range = current["high"] - current["low"]
    if candle_range <= 0:
        return None
    ma7 = _ma(closes, index, 7)
    ma20 = _ma(closes, index, 20)
    ma60 = _ma(closes, index, 60)
    ma20_prev5 = _ma(closes, index - 5, 20)
    vol20_prev = _ma(volumes, index - 1, 20)
    if ma7 is None or ma20 is None or ma60 is None or ma20_prev5 is None or not vol20_prev:
        return None
    low60 = min(lows[index - 59 : index + 1])
    high60 = max(highs[index - 59 : index + 1])
    high20_prev = max(highs[index - 20 : index])
    close = current["close"]
    open_ = current["open"]
    high = current["high"]
    low = current["low"]
    return {
        "close_vs_ma7": close / ma7 - 1.0,
        "close_vs_ma20": close / ma20 - 1.0,
        "close_vs_ma60": close / ma60 - 1.0,
        "ma20_vs_ma60": ma20 / ma60 - 1.0,
        "ma20_slope5": ma20 / ma20_prev5 - 1.0,
        "range_pos60": (close - low60) / (high60 - low60) if high60 > low60 else 0.5,
        "close_vs_prev_high20": close / high20_prev - 1.0 if high20_prev else 0.0,
        "body_ratio": abs(close - open_) / candle_range,
        "upper_wick_ratio": (high - max(open_, close)) / candle_range,
        "lower_wick_ratio": (min(open_, close) - low) / candle_range,
        "volume_ratio20": current["volume"] / vol20_prev,
        "close_below_ma7": 1.0 if close < ma7 else 0.0,
        "close_below_ma20": 1.0 if close < ma20 else 0.0,
        "high_touched_ma20_close_below": 1.0 if high >= ma20 and close < ma20 else 0.0,
        "bear_candle": 1.0 if close < open_ else 0.0,
    }


def _setup_family(bars: list[dict[str, Any]], index: int) -> str | None:
    if index < 60:
        return None
    closes = [row["close"] for row in bars]
    highs = [row["high"] for row in bars]
    volumes = [row["volume"] for row in bars]
    current = bars[index]
    ma20 = _ma(closes, index, 20)
    ma60 = _ma(closes, index, 60)
    vol20_prev = _ma(volumes, index - 1, 20)
    if ma20 is None or ma60 is None or not vol20_prev:
        return None
    candle_range = current["high"] - current["low"]
    if candle_range <= 0:
        return None
    upper_wick_ratio = (current["high"] - max(current["open"], current["close"])) / candle_range
    previous_high20 = max(highs[index - 20 : index])
    high_zone_wick = current["close"] > previous_high20 and upper_wick_ratio >= 0.25 and current["volume"] / vol20_prev < 1.8
    ma_bear_pullback20 = current["close"] < ma20 and current["high"] >= ma20 and ma20 < ma60 and current["close"] < current["open"]
    if high_zone_wick:
        return "high_zone_wick"
    if ma_bear_pullback20:
        return "ma_bear_pullback20"
    return None


def run(*, db_path: Path, output_root: Path) -> Path:
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        freshness = conn.execute(
            f"""
            SELECT COALESCE(source, 'pan') AS src, max({_normalize_expr()}) AS max_ymd, count(*) AS row_count
            FROM daily_bars
            GROUP BY COALESCE(source, 'pan')
            ORDER BY src
            """
        ).fetchall()
        provisional_as_of = max(int(max_ymd) for source, max_ymd, _ in freshness if source == "yahoo")
        codes = [str(row[0]) for row in conn.execute("SELECT DISTINCT code FROM daily_bars ORDER BY code").fetchall()]
        names = {str(code): str(name or "") for code, name in conn.execute("SELECT code, name FROM tickers").fetchall()}
        setup_rows: list[dict[str, Any]] = []
        feature_rows: list[list[float]] = []
        for code in codes:
            bars = _load_combined_bars(conn, code)
            if len(bars) < 280:
                continue
            dates = [row["date"] for row in bars]
            if provisional_as_of not in dates:
                continue
            index = dates.index(provisional_as_of)
            if bars[index]["source"] != "yahoo":
                continue
            setup = _setup_family(bars, index)
            features = _feature_row(bars, index)
            if setup is None or features is None:
                continue
            row = {
                "sample_key": f"{code}:{provisional_as_of}",
                "code": code,
                "name": names.get(code, ""),
                "as_of": provisional_as_of,
                "bar_source": "yahoo",
                "setup_family": setup,
                "numeric_features": features,
            }
            setup_rows.append(row)
            feature_rows.append([features[name] for name in FEATURE_NAMES])
    finally:
        conn.close()
    import numpy as np

    x = np.array(feature_rows)
    rule_outputs: list[dict[str, Any]] = []
    merged: dict[str, dict[str, Any]] = {}
    for rule in RULES:
        mask = _apply_rule(x, rule["clauses"]) if len(setup_rows) else []
        selected = [row for row, keep in zip(setup_rows, mask) if bool(keep)]
        for row in selected:
            merged.setdefault(
                row["sample_key"],
                {
                    **row,
                    "matched_rules": [],
                },
            )
            merged[row["sample_key"]]["matched_rules"].append(
                {
                    "rule_id": rule["rule_id"],
                    "review_strength": rule["review_strength"],
                    "oos_reference": rule["oos_reference"],
                }
            )
        rule_outputs.append(
            {
                "rule_id": rule["rule_id"],
                "review_strength": rule["review_strength"],
                "oos_reference": rule["oos_reference"],
                "selected_count": len(selected),
                "rows": selected,
            }
        )
    current_candidates = sorted(
        merged.values(),
        key=lambda row: (max(item["oos_reference"]["entry_now_rate"] for item in row["matched_rules"]), len(row["matched_rules"])),
        reverse=True,
    )
    report = {
        "schema_version": AXIS_ID,
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "db_path": str(db_path),
        "runtime_freshness_by_source": [{"source": source, "max_ymd": int(max_ymd), "row_count": int(row_count)} for source, max_ymd, row_count in freshness],
        "provisional_as_of": provisional_as_of,
        "source_policy": "yahoo provisional overlaid on confirmed pan history; review-only",
        "setup_event_count": len(setup_rows),
        "current_candidate_count": len(current_candidates),
        "rule_outputs": rule_outputs,
        "current_candidates": current_candidates,
        "decision": {
            "candidate_local_decision": "provisional_review_candidates_present" if current_candidates else "no_provisional_candidate",
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": "provisional bar matches timing-rule research axis" if current_candidates else "no provisional bar matched timing-rule research axis",
        },
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
    }
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    _write_json(output_dir / "provisional_entry_timing_candidates.json", report)
    _write_json(output_root / "latest_provisional_entry_timing_candidates.json", {"run_root": str(output_dir), **report})
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(run(db_path=args.db_path, output_root=args.output_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
