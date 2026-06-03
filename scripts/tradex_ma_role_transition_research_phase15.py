from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any

import duckdb


AXIS_ID = "ma_role_transition_research_phase15"
DEFAULT_DB_PATH = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\ma_role_transition_research_phase15")
MA_WINDOWS = (7, 20, 60, 100, 200)
MIN_SAMPLE_COUNT = 100


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _relation(left: float, right: float, *, band: float = 0.003) -> str:
    relative = left / right - 1.0
    if relative > band:
        return "above"
    if relative < -band:
        return "below"
    return "near"


def _slope(current: float, previous: float, *, band: float = 0.002) -> str:
    relative = current / previous - 1.0
    if relative > band:
        return "up"
    if relative < -band:
        return "down"
    return "flat"


def _candle_vs_ma(high: float, low: float, ma: float) -> str:
    if high <= ma:
        return "all_below"
    if low >= ma:
        return "all_above"
    width = high - low
    if width <= 0:
        return "straddle"
    above_share = (high - ma) / width
    if above_share >= 0.5:
        return "mostly_above"
    return "mostly_below"


def _candle_shape(open_: float, high: float, low: float, close: float) -> str:
    span = high - low
    if span <= 0:
        return "flat"
    body = abs(close - open_)
    upper = high - max(open_, close)
    lower = min(open_, close) - low
    body_share = body / span
    upper_share = upper / span
    lower_share = lower / span
    direction = "bull" if close > open_ else "bear" if close < open_ else "doji"
    if body_share <= 0.15 and upper_share >= 0.25 and lower_share >= 0.25:
        return "spinning_top"
    if body_share <= 0.25 and lower_share >= 0.45 and upper_share <= 0.20:
        return f"hammer_{direction}"
    if body_share <= 0.25 and upper_share >= 0.45 and lower_share <= 0.20:
        return f"inverted_hammer_{direction}"
    if upper_share >= 0.45 and upper >= body * 1.5:
        return f"long_upper_wick_{direction}"
    if lower_share >= 0.45 and lower >= body * 1.5:
        return f"long_lower_wick_{direction}"
    if body_share >= 0.70:
        return f"wide_body_{direction}"
    if body_share <= 0.20:
        return f"small_body_{direction}"
    return f"normal_{direction}"


def _three_candle_pattern(
    open2: float,
    high2: float,
    low2: float,
    close2: float,
    open1: float,
    high1: float,
    low1: float,
    close1: float,
    open0: float,
    high0: float,
    low0: float,
    close0: float,
) -> str:
    bullish = [close2 > open2, close1 > open1, close0 > open0]
    bearish = [close2 < open2, close1 < open1, close0 < open0]
    if all(bullish) and close2 < close1 < close0:
        return "three_white_soldiers"
    if all(bearish) and close2 > close1 > close0:
        return "three_black_crows"
    body1_high = max(open1, close1)
    body1_low = min(open1, close1)
    body0_high = max(open0, close0)
    body0_low = min(open0, close0)
    if close1 < open1 and close0 > open0 and body0_high >= body1_high and body0_low <= body1_low:
        return "bullish_engulfing"
    if close1 > open1 and close0 < open0 and body0_high >= body1_high and body0_low <= body1_low:
        return "bearish_engulfing"
    if body0_high <= body1_high and body0_low >= body1_low:
        return "inside_body"
    if high2 < high1 < high0 and low2 < low1 < low0:
        return "three_bar_rising"
    if high2 > high1 > high0 and low2 > low1 > low0:
        return "three_bar_falling"
    if close0 > close2:
        return "three_bar_up"
    if close0 < close2:
        return "three_bar_down"
    return "three_bar_flat"


def _environment(ma60: float, ma100: float, ma200: float) -> str:
    if ma60 > ma100 > ma200:
        return "bull_alignment"
    if ma60 < ma100 < ma200:
        return "bear_alignment"
    return "mixed_alignment"


def _state(row: dict[str, Any]) -> dict[str, str]:
    return {
        "entry_exit": "|".join(
            [
                f"candle_shape:{_candle_shape(row['open'], row['high'], row['low'], row['close'])}",
                "three_candle:"
                + _three_candle_pattern(
                    row["open_prev2"],
                    row["high_prev2"],
                    row["low_prev2"],
                    row["close_prev2"],
                    row["open_prev1"],
                    row["high_prev1"],
                    row["low_prev1"],
                    row["close_prev1"],
                    row["open"],
                    row["high"],
                    row["low"],
                    row["close"],
                ),
                f"close_ma7:{_relation(row['close'], row['ma7'])}",
                f"close_ma20:{_relation(row['close'], row['ma20'])}",
                f"candle_ma7:{_candle_vs_ma(row['high'], row['low'], row['ma7'])}",
                f"candle_ma20:{_candle_vs_ma(row['high'], row['low'], row['ma20'])}",
                f"ma7_ma20:{_relation(row['ma7'], row['ma20'])}",
                f"ma7_slope:{_slope(row['ma7'], row['ma7_prev5'])}",
                f"ma20_slope:{_slope(row['ma20'], row['ma20_prev5'])}",
            ]
        ),
        "trend": "|".join(
            [
                f"close_ma60:{_relation(row['close'], row['ma60'])}",
                f"candle_ma60:{_candle_vs_ma(row['high'], row['low'], row['ma60'])}",
                f"ma20_ma60:{_relation(row['ma20'], row['ma60'])}",
                f"ma60_slope:{_slope(row['ma60'], row['ma60_prev5'])}",
            ]
        ),
        "environment": "|".join(
            [
                f"alignment:{_environment(row['ma60'], row['ma100'], row['ma200'])}",
                f"candle_ma100:{_candle_vs_ma(row['high'], row['low'], row['ma100'])}",
                f"candle_ma200:{_candle_vs_ma(row['high'], row['low'], row['ma200'])}",
                f"ma100_slope:{_slope(row['ma100'], row['ma100_prev20'])}",
                f"ma200_slope:{_slope(row['ma200'], row['ma200_prev20'])}",
            ]
        ),
    }


def _load_rows(conn: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    result = conn.execute(
        """
        WITH normalized AS (
            SELECT
                CAST(code AS VARCHAR) AS code,
                CASE
                    WHEN date BETWEEN 19000101 AND 20991231 THEN CAST(date AS INTEGER)
                    WHEN date >= 1000000000000 THEN CAST(strftime(to_timestamp(date / 1000), '%Y%m%d') AS INTEGER)
                    WHEN date >= 1000000000 THEN CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)
                    ELSE NULL
                END AS ymd,
                CAST(o AS DOUBLE) AS open,
                CAST(h AS DOUBLE) AS high,
                CAST(l AS DOUBLE) AS low,
                CAST(c AS DOUBLE) AS close
            FROM daily_bars
            WHERE COALESCE(source, 'pan') <> 'yahoo'
        ),
        ma AS (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY code ORDER BY ymd) AS history_rows,
                AVG(close) OVER (PARTITION BY code ORDER BY ymd ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ma7,
                AVG(close) OVER (PARTITION BY code ORDER BY ymd ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20,
                AVG(close) OVER (PARTITION BY code ORDER BY ymd ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS ma60,
                AVG(close) OVER (PARTITION BY code ORDER BY ymd ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) AS ma100,
                AVG(close) OVER (PARTITION BY code ORDER BY ymd ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS ma200,
                LEAD(close, 1) OVER (PARTITION BY code ORDER BY ymd) AS close1,
                LEAD(close, 5) OVER (PARTITION BY code ORDER BY ymd) AS close5,
                LEAD(close, 10) OVER (PARTITION BY code ORDER BY ymd) AS close10,
                LEAD(close, 20) OVER (PARTITION BY code ORDER BY ymd) AS close20,
                LEAD(close, 60) OVER (PARTITION BY code ORDER BY ymd) AS close60,
                MAX(high) OVER (PARTITION BY code ORDER BY ymd ROWS BETWEEN 1 FOLLOWING AND 20 FOLLOWING) AS high20,
                MIN(low) OVER (PARTITION BY code ORDER BY ymd ROWS BETWEEN 1 FOLLOWING AND 20 FOLLOWING) AS low20,
                LAG(open, 1) OVER (PARTITION BY code ORDER BY ymd) AS open_prev1,
                LAG(high, 1) OVER (PARTITION BY code ORDER BY ymd) AS high_prev1,
                LAG(low, 1) OVER (PARTITION BY code ORDER BY ymd) AS low_prev1,
                LAG(close, 1) OVER (PARTITION BY code ORDER BY ymd) AS close_prev1,
                LAG(open, 2) OVER (PARTITION BY code ORDER BY ymd) AS open_prev2,
                LAG(high, 2) OVER (PARTITION BY code ORDER BY ymd) AS high_prev2,
                LAG(low, 2) OVER (PARTITION BY code ORDER BY ymd) AS low_prev2,
                LAG(close, 2) OVER (PARTITION BY code ORDER BY ymd) AS close_prev2
            FROM normalized
            WHERE ymd IS NOT NULL AND open IS NOT NULL AND close IS NOT NULL AND high IS NOT NULL AND low IS NOT NULL
        ),
        lagged AS (
            SELECT
                *,
                LAG(ma7, 5) OVER (PARTITION BY code ORDER BY ymd) AS ma7_prev5,
                LAG(ma20, 5) OVER (PARTITION BY code ORDER BY ymd) AS ma20_prev5,
                LAG(ma60, 5) OVER (PARTITION BY code ORDER BY ymd) AS ma60_prev5,
                LAG(ma100, 20) OVER (PARTITION BY code ORDER BY ymd) AS ma100_prev20,
                LAG(ma200, 20) OVER (PARTITION BY code ORDER BY ymd) AS ma200_prev20
            FROM ma
        )
        SELECT *
        FROM lagged
        WHERE history_rows >= 220
          AND close60 IS NOT NULL
          AND ma7_prev5 IS NOT NULL
          AND ma20_prev5 IS NOT NULL
          AND ma60_prev5 IS NOT NULL
          AND ma100_prev20 IS NOT NULL
          AND ma200_prev20 IS NOT NULL
          AND open_prev1 IS NOT NULL
          AND high_prev1 IS NOT NULL
          AND low_prev1 IS NOT NULL
          AND close_prev1 IS NOT NULL
          AND open_prev2 IS NOT NULL
          AND high_prev2 IS NOT NULL
          AND low_prev2 IS NOT NULL
          AND close_prev2 IS NOT NULL
        ORDER BY ymd, code
        """
    )
    columns = [item[0] for item in result.description]
    return [dict(zip(columns, row)) for row in result.fetchall()]


def _split_dates(rows: list[dict[str, Any]]) -> tuple[int, int]:
    dates = sorted({int(row["ymd"]) for row in rows})
    return dates[int(len(dates) * 0.60)], dates[int(len(dates) * 0.80)]


def _split(ymd: int, validation_start: int, test_start: int) -> str:
    if ymd < validation_start:
        return "train"
    if ymd < test_start:
        return "validation"
    return "test"


def _summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    def values(key: str) -> list[float]:
        return [float(row[key]) for row in rows]

    ret20 = values("ret20")
    return {
        "count": len(rows),
        "ret1_mean": mean(values("ret1")),
        "ret5_mean": mean(values("ret5")),
        "ret10_mean": mean(values("ret10")),
        "ret20_mean": mean(ret20),
        "ret20_median": median(ret20),
        "ret60_mean": mean(values("ret60")),
        "positive_ret20_rate": mean([value > 0 for value in ret20]),
        "winner_ret20_gt_10pct_rate": mean([value >= 0.10 for value in ret20]),
        "bad_ret20_lt_minus_5pct_rate": mean([value <= -0.05 for value in ret20]),
        "mfe20_mean": mean(values("mfe20")),
        "mae20_mean": mean(values("mae20")),
    }


def _aggregate(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(row["split"], row["entry_exit"], row["trend"], row["environment"])].append(row)
    result = []
    for (split, entry_exit, trend, environment), group in grouped.items():
        if len(group) < MIN_SAMPLE_COUNT:
            continue
        result.append(
            {
                "split": split,
                "entry_exit": entry_exit,
                "trend": trend,
                "environment": environment,
                **_summary(group),
            }
        )
    return sorted(result, key=lambda row: (row["split"], -row["count"], row["entry_exit"], row["trend"], row["environment"]))


def _stable_candidates(aggregates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], dict[str, dict[str, Any]]] = defaultdict(dict)
    for row in aggregates:
        grouped[(row["entry_exit"], row["trend"], row["environment"])][row["split"]] = row
    candidates = []
    for (entry_exit, trend, environment), by_split in grouped.items():
        if set(by_split) != {"train", "validation", "test"}:
            continue
        ret20 = {split: float(by_split[split]["ret20_mean"]) for split in ("train", "validation", "test")}
        candidates.append(
            {
                "entry_exit": entry_exit,
                "trend": trend,
                "environment": environment,
                "minimum_split_count": min(int(row["count"]) for row in by_split.values()),
                "ret20_mean_by_split": ret20,
                "minimum_ret20_mean": min(ret20.values()),
                "test_positive_ret20_rate": float(by_split["test"]["positive_ret20_rate"]),
                "test_bad_ret20_lt_minus_5pct_rate": float(by_split["test"]["bad_ret20_lt_minus_5pct_rate"]),
                "stable_positive_ret20": all(value > 0 for value in ret20.values()),
            }
        )
    return sorted(candidates, key=lambda row: (-row["minimum_ret20_mean"], -row["minimum_split_count"], row["entry_exit"]))


def run(*, db_path: Path, output_root: Path) -> Path:
    with duckdb.connect(str(db_path), read_only=True) as conn:
        raw = _load_rows(conn)
    validation_start, test_start = _split_dates(raw)
    rows = []
    for row in raw:
        states = _state(row)
        close = float(row["close"])
        rows.append(
            {
                "code": row["code"],
                "ymd": int(row["ymd"]),
                "split": _split(int(row["ymd"]), validation_start, test_start),
                **states,
                "ret1": float(row["close1"]) / close - 1.0,
                "ret5": float(row["close5"]) / close - 1.0,
                "ret10": float(row["close10"]) / close - 1.0,
                "ret20": float(row["close20"]) / close - 1.0,
                "ret60": float(row["close60"]) / close - 1.0,
                "mfe20": float(row["high20"]) / close - 1.0,
                "mae20": float(row["low20"]) / close - 1.0,
            }
        )
    aggregates = _aggregate(rows)
    candidates = _stable_candidates(aggregates)
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    contract = {
        "schema_version": "tradex_ma_role_transition_contract_v1",
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "bar_basis": "confirmed_non_yahoo_daily_bars_only",
        "ma_roles": {
            "entry_exit": [7, 20],
            "trend": [60],
            "environment": [100, 200],
        },
                "candle_position_features": {
            "basis": "daily high-low range share above each MA",
            "labels": ["all_above", "mostly_above", "straddle", "mostly_below", "all_below"],
            "mostly_above_definition": "at least half of the candle high-low range is above the MA",
        },
        "candle_shape_features": {
            "basis": "daily OHLC body and wick proportions",
            "labels_include": [
                "hammer",
                "inverted_hammer",
                "spinning_top",
                "long_upper_wick",
                "long_lower_wick",
                "wide_body",
                "small_body",
                "normal",
            ],
            "japanese_terms": {"hammer": "トンカチ", "spinning_top": "コマ"},
        },
        "three_candle_features": {
            "basis": "latest three confirmed daily candles",
            "labels_include": [
                "three_white_soldiers",
                "three_black_crows",
                "bullish_engulfing",
                "bearish_engulfing",
                "inside_body",
                "three_bar_rising",
                "three_bar_falling",
                "three_bar_up",
                "three_bar_down",
                "three_bar_flat",
            ],
        },
        "future_horizons": [1, 5, 10, 20, 60],
        "minimum_group_sample_count": MIN_SAMPLE_COUNT,
        "split_contract": {"train_lt": validation_start, "validation_lt": test_start, "test_gte": test_start},
    }
    _write_json(output_dir / "contract.json", contract)
    _write_json(
        output_dir / "ma_role_transition_compare.json",
        {
            "schema_version": "tradex_ma_role_transition_compare_v1",
            "generated_at": _utc_now(),
            "axis_id": AXIS_ID,
            "row_count": len(rows),
            "qualified_state_count": len(aggregates),
            "states": aggregates,
        },
    )
    _write_json(
        output_dir / "stable_state_leaderboard.json",
        {
            "schema_version": "tradex_ma_role_transition_stable_state_leaderboard_v1",
            "generated_at": _utc_now(),
            "axis_id": AXIS_ID,
            "candidate_count": len(candidates),
            "stable_positive_ret20_candidate_count": sum(row["stable_positive_ret20"] for row in candidates),
            "states": candidates,
        },
    )
    decision = {
        "schema_version": "tradex_ma_role_transition_decision_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "authoritative_rollup_decision": "hold_for_state_transition_review",
        "reason_type": "descriptive_daily_ma_state_transition_baseline_generated",
        "row_count": len(rows),
        "qualified_state_count": len(aggregates),
        "cross_split_candidate_count": len(candidates),
        "stable_positive_ret20_candidate_count": sum(row["stable_positive_ret20"] for row in candidates),
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
        "meemee_reflectable": False,
        "automatic_trade_action": False,
        "non_scope": ["MeeMee reflection", "production ranking", "automatic trade action", "validated buy claim", "validated sell claim"],
    }
    _write_json(output_dir / "research_decision.json", decision)
    _write_json(output_root / "latest_research_decision.json", {"run_root": str(output_dir), **decision})
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
