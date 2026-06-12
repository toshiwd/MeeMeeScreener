from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.tradex_pre_crash_shape_pattern_discovery_v1 import CRASH_THRESHOLD_20, END_YMD, MIN_HISTORY, START_YMD, _classify_shape
from shared.runtime_stock_db_contract import inspect_runtime_stock_db, resolve_runtime_stock_db_path


AXIS_ID = "pre_crash_shape_false_positive_escape_v1"
DEFAULT_OUTPUT_ROOT = Path("G:/Tradex/pre_crash_shape_false_positive_escape_v1")
TYPICAL_PATTERNS = {
    "support_grind_then_flush",
    "lower_high_rollover",
    "downtrend_pause_second_leg",
    "failed_new_high_reversal",
    "rise_flat_breakdown",
    "long_range_support_break",
    "parabolic_exhaustion_distribution",
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{AXIS_ID}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    try:
        import numpy as np

        if isinstance(value, np.generic):
            return _json_ready(value.item())
    except Exception:
        pass
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(_json_ready(row), ensure_ascii=False, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )


def _load_daily(db_path: Path, code_limit: int | None) -> pd.DataFrame:
    limit_clause = ""
    params: list[Any] = [START_YMD, END_YMD]
    if code_limit:
        limit_clause = "AND code IN (SELECT DISTINCT code FROM daily_bars ORDER BY code LIMIT ?)"
        params.append(int(code_limit))
    sql = f"""
        WITH bars AS (
          SELECT
            code::VARCHAR AS code,
            CASE
              WHEN date > 1000000000 THEN CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)
              ELSE CAST(date AS INTEGER)
            END AS ymd,
            CAST(o AS DOUBLE) AS o,
            CAST(h AS DOUBLE) AS h,
            CAST(l AS DOUBLE) AS l,
            CAST(c AS DOUBLE) AS c,
            CAST(v AS DOUBLE) AS v,
            lower(coalesce(source, '')) AS source
          FROM daily_bars
          WHERE date IS NOT NULL
            AND o IS NOT NULL AND h IS NOT NULL AND l IS NOT NULL AND c IS NOT NULL
            AND lower(coalesce(source, '')) = 'pan'
        )
        SELECT code, ymd, o, h, l, c, v
        FROM bars
        WHERE ymd BETWEEN ? AND ?
        {limit_clause}
        ORDER BY code, ymd
    """
    df = duckdb.connect(str(db_path), read_only=True).execute(sql, params).df()
    if df.empty:
        raise RuntimeError("daily_bars query returned no confirmed pan rows")
    return df


def _escape_candle(after: pd.DataFrame, signal_close: float, signal_high: float, ma5: float | None) -> dict[str, Any]:
    best: dict[str, Any] = {
        "escape_bullish_candle_found": False,
        "escape_day_offset": None,
        "escape_ymd": None,
        "escape_type": None,
        "escape_close_above_signal_high": False,
        "escape_close_above_ma5": False,
        "escape_large_body": False,
        "escape_volume_ratio": None,
    }
    if after.empty:
        return best
    vol20 = after["v"].replace(0, float("nan")).astype(float).rolling(20).mean()
    for offset, (_, row) in enumerate(after.head(5).iterrows(), start=1):
        open_ = float(row["o"])
        high = float(row["h"])
        low = float(row["l"])
        close = float(row["c"])
        span = high - low
        if span <= 0 or open_ <= 0 or signal_close <= 0:
            continue
        body_pct = abs(close - open_) / open_
        close_pos = (close - low) / span
        vol_ratio = None
        if offset - 1 < len(vol20) and not pd.isna(vol20.iloc[offset - 1]) and vol20.iloc[offset - 1] > 0:
            vol_ratio = float(row["v"] / vol20.iloc[offset - 1])
        close_above_signal_high = close > signal_high
        close_above_ma5 = ma5 is not None and close > ma5
        bullish = close > open_
        large_body = body_pct >= 0.025
        reclaim = close >= signal_close * 1.03
        if bullish and close_pos >= 0.65 and (close_above_signal_high or close_above_ma5 or large_body or reclaim):
            if close_above_signal_high:
                escape_type = "bullish_reclaim_signal_high"
            elif close_above_ma5:
                escape_type = "bullish_reclaim_ma5"
            elif reclaim:
                escape_type = "bullish_reclaim_signal_close_3pct"
            else:
                escape_type = "large_bullish_denial"
            best.update(
                {
                    "escape_bullish_candle_found": True,
                    "escape_day_offset": offset,
                    "escape_ymd": int(row["ymd"]),
                    "escape_type": escape_type,
                    "escape_close_above_signal_high": close_above_signal_high,
                    "escape_close_above_ma5": close_above_ma5,
                    "escape_large_body": large_body,
                    "escape_volume_ratio": vol_ratio,
                }
            )
            return best
    return best


def _ma(values: pd.Series, end_exclusive: int, window: int) -> float | None:
    if end_exclusive < window:
        return None
    return float(values.iloc[end_exclusive - window : end_exclusive].mean())


def _add_shape_features(g: pd.DataFrame) -> pd.DataFrame:
    out = g.sort_values("ymd").reset_index(drop=True).copy()
    c = out["c"].astype(float)
    h = out["h"].astype(float)
    l = out["l"].astype(float)
    o = out["o"].astype(float)
    v = out["v"].replace(0, float("nan")).astype(float)
    early_first = c.shift(79)
    early_last = c.shift(40)
    mid_first = c.shift(39)
    mid_last = c.shift(20)
    late_first = c.shift(19)
    out["ret_80_40"] = early_last / early_first - 1.0
    out["ret_40_20"] = mid_last / mid_first - 1.0
    out["ret_20_0"] = c / late_first - 1.0
    out["ret_60_0"] = c / c.shift(59) - 1.0
    high_40_20 = h.shift(20).rolling(20).max()
    low_40_20 = l.shift(20).rolling(20).min()
    out["range_40_20"] = high_40_20 / low_40_20 - 1.0
    out["range_20_0"] = h.rolling(20).max() / l.rolling(20).min() - 1.0
    prior_high = h.shift(20).rolling(60).max()
    prior_low = l.shift(20).rolling(60).min()
    late_high = h.rolling(20).max()
    out["dist_prior_80_high"] = c / prior_high - 1.0
    out["dist_prior_80_low"] = c / prior_low - 1.0
    out["late_high_break"] = late_high / prior_high - 1.0
    out["last_vol_ratio"] = v / v.rolling(20).mean()
    span = (h - l).replace(0, float("nan"))
    close_pos = (c - l) / span
    red = (c < o).astype(float)
    weak = (close_pos <= 0.35).astype(float)
    out["red_cluster_10"] = red.rolling(10).sum()
    out["weak_close_cluster_10"] = weak.rolling(10).sum()
    out["ma5"] = c.rolling(5).mean()
    out["future_min20"] = l.shift(-1).rolling(20).min().shift(-19) / c - 1.0
    out["future_ret20"] = c.shift(-20) / c - 1.0
    return out


def _feature_payload(row: pd.Series) -> dict[str, float | None]:
    keys = [
        "ret_80_40",
        "ret_40_20",
        "ret_20_0",
        "ret_60_0",
        "range_40_20",
        "range_20_0",
        "dist_prior_80_high",
        "dist_prior_80_low",
        "late_high_break",
        "last_vol_ratio",
        "red_cluster_10",
        "weak_close_cluster_10",
    ]
    payload: dict[str, float | None] = {}
    for key in keys:
        value = row.get(key)
        payload[key] = None if pd.isna(value) else float(value)
    return payload


def _build_pattern_events(daily: pd.DataFrame) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for code, group in daily.groupby("code", sort=False):
        g = _add_shape_features(group)
        for idx in range(MIN_HISTORY, len(g) - 20):
            current = g.iloc[idx]
            features = _feature_payload(current)
            pattern = _classify_shape(features)
            if pattern not in TYPICAL_PATTERNS:
                continue
            fmin = current["future_min20"]
            fret = current["future_ret20"]
            if pd.isna(fmin) or pd.isna(fret):
                continue
            signal_close = float(current["c"])
            signal_high = float(current["h"])
            ma5 = None if pd.isna(current["ma5"]) else float(current["ma5"])
            after = g.iloc[idx + 1 : idx + 21]
            escape = _escape_candle(after, signal_close, signal_high, ma5)
            rows.append(
                {
                    "code": str(code),
                    "ymd": int(g.iloc[idx]["ymd"]),
                    "month": int(g.iloc[idx]["ymd"]) // 100,
                    "pattern": pattern,
                    "crash20": bool(float(fmin) <= CRASH_THRESHOLD_20),
                    "non_crash20": bool(float(fmin) > CRASH_THRESHOLD_20),
                    "future_min20": float(fmin),
                    "future_ret20": float(fret),
                    **features,
                    **escape,
                }
            )
    meta = {
        "typical_pattern_event_count": len(rows),
        "symbol_count": len({row["code"] for row in rows}),
        "month_count": len({row["month"] for row in rows}),
        "crash_threshold_20": CRASH_THRESHOLD_20,
    }
    return rows, meta


def _rate(series: pd.Series) -> float:
    return float(series.mean()) if len(series) else 0.0


def _summaries(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    df = pd.DataFrame(rows)
    if df.empty:
        return [], [], {}
    pattern_rows: list[dict[str, Any]] = []
    feature_rows: list[dict[str, Any]] = []
    for pattern, group in df.groupby("pattern"):
        crash = group[group["crash20"]]
        non = group[group["non_crash20"]]
        escape_crash = _rate(crash["escape_bullish_candle_found"]) if len(crash) else None
        escape_non = _rate(non["escape_bullish_candle_found"]) if len(non) else None
        pattern_rows.append(
            {
                "pattern": pattern,
                "n": int(len(group)),
                "crash_n": int(len(crash)),
                "non_crash_n": int(len(non)),
                "crash_rate": _rate(group["crash20"]),
                "mean_future_min20_crash": float(crash["future_min20"].mean()) if len(crash) else None,
                "mean_future_min20_non_crash": float(non["future_min20"].mean()) if len(non) else None,
                "escape_rate_crash": escape_crash,
                "escape_rate_non_crash": escape_non,
                "escape_rate_delta_non_minus_crash": None if escape_crash is None or escape_non is None else escape_non - escape_crash,
                "median_ret_20_0_crash": float(crash["ret_20_0"].median()) if len(crash) else None,
                "median_ret_20_0_non_crash": float(non["ret_20_0"].median()) if len(non) else None,
                "median_last_vol_ratio_crash": float(crash["last_vol_ratio"].median()) if len(crash) else None,
                "median_last_vol_ratio_non_crash": float(non["last_vol_ratio"].median()) if len(non) else None,
                "median_red_cluster_10_crash": float(crash["red_cluster_10"].median()) if len(crash) else None,
                "median_red_cluster_10_non_crash": float(non["red_cluster_10"].median()) if len(non) else None,
            }
        )
    feature_cols = [
        "ret_80_40",
        "ret_40_20",
        "ret_20_0",
        "ret_60_0",
        "range_40_20",
        "range_20_0",
        "dist_prior_80_high",
        "dist_prior_80_low",
        "late_high_break",
        "last_vol_ratio",
        "red_cluster_10",
        "weak_close_cluster_10",
    ]
    for col in feature_cols:
        crash = df[df["crash20"]][col].dropna()
        non = df[df["non_crash20"]][col].dropna()
        feature_rows.append(
            {
                "feature": col,
                "median_crash": float(crash.median()) if len(crash) else None,
                "median_non_crash": float(non.median()) if len(non) else None,
                "mean_crash": float(crash.mean()) if len(crash) else None,
                "mean_non_crash": float(non.mean()) if len(non) else None,
                "median_non_minus_crash": None if not len(crash) or not len(non) else float(non.median() - crash.median()),
            }
        )
    overall = {
        "n": int(len(df)),
        "crash_n": int(df["crash20"].sum()),
        "non_crash_n": int(df["non_crash20"].sum()),
        "crash_rate": _rate(df["crash20"]),
        "escape_rate_crash": _rate(df[df["crash20"]]["escape_bullish_candle_found"]),
        "escape_rate_non_crash": _rate(df[df["non_crash20"]]["escape_bullish_candle_found"]),
    }
    overall["escape_rate_delta_non_minus_crash"] = overall["escape_rate_non_crash"] - overall["escape_rate_crash"]
    pattern_rows.sort(key=lambda item: item["n"], reverse=True)
    feature_rows.sort(
        key=lambda item: 0.0 if item["median_non_minus_crash"] is None else abs(float(item["median_non_minus_crash"])),
        reverse=True,
    )
    return pattern_rows, feature_rows, overall


def run(db_path: Path, output_root: Path, code_limit: int | None) -> Path:
    run_dir = output_root / _run_id()
    runtime_status = inspect_runtime_stock_db(runtime_db_path=db_path)
    daily = _load_daily(db_path, code_limit)
    rows, meta = _build_pattern_events(daily)
    pattern_summary, feature_contrast, overall = _summaries(rows)
    df = pd.DataFrame(rows)
    false_examples: list[dict[str, Any]] = []
    escape_examples: list[dict[str, Any]] = []
    if not df.empty:
        false_examples = (
            df[df["non_crash20"]]
            .sort_values(["future_min20", "future_ret20"], ascending=[False, False])
            .head(200)
            .to_dict(orient="records")
        )
        escape_examples = (
            df[df["escape_bullish_candle_found"]]
            .sort_values(["non_crash20", "future_min20"], ascending=[False, False])
            .head(200)
            .to_dict(orient="records")
        )
    decision = {
        "authoritative_decision": "escape_candle_candidate_found" if overall.get("escape_rate_delta_non_minus_crash", 0) > 0 else "escape_candle_not_discriminative",
        "reason": "compare typical-pattern false positives against crash cases and test bullish denial candles",
        "overall": overall,
        "promotion_allowed": False,
        "meemee_reflection": False,
        "production_ranking_change": False,
        "runtime_db_write": False,
    }
    contract = {
        "axis_id": AXIS_ID,
        "research_phase": "comparison_stabilization",
        "fixed_evaluation_conditions": {
            "universe": "runtime_stock_db daily_bars confirmed PAN rows",
            "period": f"{START_YMD}-{END_YMD}",
            "pattern_source": "same classifier as pre_crash_shape_pattern_discovery_v1",
            "false_positive_definition": f"typical pattern matched and future_min20 > {CRASH_THRESHOLD_20}",
            "crash_definition": f"typical pattern matched and future_min20 <= {CRASH_THRESHOLD_20}",
            "escape_candle_window": "first 5 sessions after pattern signal",
            "escape_candle_definition": "bullish close in upper 35pct of range plus signal-high reclaim, ma5 reclaim, 3pct close reclaim, or large bullish body",
            "borrow_lending": "not_available_short_side_theoretical_only",
            "cost_slippage": "not_applied_diagnostic_only",
            "no_lookahead": "pattern features use rows at or before signal date; escape uses post-signal bars for exit diagnostics only",
        },
        "non_scope": [
            "no MeeMee reflection",
            "no runtime DB writes",
            "no production ranking or candidate-generator mutation",
            "no validated short entry claim",
            "no threshold tuning after seeing results",
        ],
    }
    _write_json(run_dir / "evaluation_contract.json", contract)
    _write_json(
        run_dir / "run_manifest.json",
        {
            "axis_id": AXIS_ID,
            "generated_at": _utc_now(),
            "db_path": str(db_path),
            "output_dir": str(run_dir),
            "code_limit": code_limit,
            "runtime_status": runtime_status,
            "raw_rows": int(len(daily)),
            "event_meta": meta,
        },
    )
    _write_json(run_dir / "pattern_false_positive_summary.json", {"overall": overall, "patterns": pattern_summary})
    _write_json(run_dir / "feature_contrast_summary.json", {"features": feature_contrast})
    _write_jsonl(run_dir / "false_positive_examples.jsonl", false_examples)
    _write_jsonl(run_dir / "escape_candle_examples.jsonl", escape_examples)
    _write_json(run_dir / "research_decision.json", decision)
    _write_json(
        run_dir / "_ARTIFACT_COMPLETE.json",
        {
            "status": "complete",
            "axis_id": AXIS_ID,
            "generated_at": _utc_now(),
            "required_files": [
                "evaluation_contract.json",
                "run_manifest.json",
                "pattern_false_positive_summary.json",
                "feature_contrast_summary.json",
                "false_positive_examples.jsonl",
                "escape_candle_examples.jsonl",
                "research_decision.json",
                "_ARTIFACT_COMPLETE.json",
            ],
        },
    )
    return run_dir


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=resolve_runtime_stock_db_path())
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--code-limit", type=int, default=None)
    args = parser.parse_args()
    print(run(args.db_path, args.output_root, args.code_limit))


if __name__ == "__main__":
    main()
