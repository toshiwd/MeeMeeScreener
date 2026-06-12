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

from shared.runtime_stock_db_contract import inspect_runtime_stock_db, resolve_runtime_stock_db_path


AXIS_ID = "pre_crash_short_signal_discovery_v1"
DEFAULT_OUTPUT_ROOT = Path("G:/Tradex/pre_crash_short_signal_discovery_v1")
START_YMD = 20200101
END_YMD = 20991231
MIN_HISTORY = 220
MIN_EVENTS = 120
MIN_SYMBOLS = 25
MIN_MONTHS = 12
SEVERE_DROP_20 = -0.10
SEVERE_DROP_40 = -0.15


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


def _add_features(group: pd.DataFrame) -> pd.DataFrame:
    g = group.sort_values("ymd").copy()
    c = g["c"]
    h = g["h"]
    l = g["l"]
    o = g["o"]
    v = g["v"].replace(0, float("nan")).astype(float)
    for w in (5, 10, 20, 60, 100, 200):
        g[f"ma{w}"] = c.rolling(w).mean()
    g["ret1"] = c.pct_change(1)
    g["ret5"] = c.pct_change(5)
    g["ret10"] = c.pct_change(10)
    g["ret20"] = c.pct_change(20)
    g["ret60"] = c.pct_change(60)
    g["high20"] = h.rolling(20).max()
    g["high60"] = h.rolling(60).max()
    g["low20"] = l.rolling(20).min()
    g["vol20"] = v.rolling(20).mean()
    g["vol_ratio"] = v / g["vol20"]
    rng = (h - l).replace(0, pd.NA)
    g["body_to_range"] = (c - o).abs() / rng
    g["upper_wick_to_range"] = (h - pd.concat([o, c], axis=1).max(axis=1)) / rng
    g["lower_wick_to_range"] = (pd.concat([o, c], axis=1).min(axis=1) - l) / rng
    g["close_pos"] = (c - l) / rng
    g["ma20_slope_5"] = g["ma20"] / g["ma20"].shift(5) - 1.0
    g["ma60_slope_10"] = g["ma60"] / g["ma60"].shift(10) - 1.0
    g["dist_ma20"] = c / g["ma20"] - 1.0
    g["dist_high20"] = c / g["high20"].shift(1) - 1.0
    g["dist_high60"] = c / g["high60"].shift(1) - 1.0
    g["range10"] = (h.rolling(10).max() / l.rolling(10).min() - 1.0)
    g["future_ret20"] = c.shift(-20) / c - 1.0
    g["future_ret40"] = c.shift(-40) / c - 1.0
    g["future_min20"] = l.shift(-1).rolling(20).min().shift(-19) / c - 1.0
    g["future_min40"] = l.shift(-1).rolling(40).min().shift(-39) / c - 1.0
    g["month"] = (g["ymd"] // 100).astype(int)
    return g


def _event_flags(df: pd.DataFrame) -> dict[str, pd.Series]:
    red = df["c"] < df["o"]
    ma_stack_down = (df["ma20"] < df["ma60"]) & (df["ma60"] < df["ma100"])
    price_below_ma20 = df["c"] < df["ma20"]
    near_high = df["dist_high20"] >= -0.04
    extended_high = df["ret20"] >= 0.12
    heavy_volume = df["vol_ratio"] >= 1.6
    high_wick = df["upper_wick_to_range"] >= 0.35
    weak_close = df["close_pos"] <= 0.35
    ma20_rollover = (df["ma20_slope_5"] < 0) & (df["ma20_slope_5"].shift(5) >= 0)
    failed_high = near_high & high_wick & weak_close & heavy_volume
    exhaustion_reversal = extended_high & red & high_wick & weak_close & (df["vol_ratio"] >= 1.3)
    distribution_cluster = (
        (red & (df["vol_ratio"] >= 1.2) & (df["close_pos"] <= 0.45)).rolling(5).sum() >= 3
    )
    support_break_after_range = (df["range10"].shift(1) <= 0.08) & (df["c"] < df["low20"].shift(1)) & heavy_volume
    weak_retest_ma20 = (
        (df["dist_ma20"] <= 0.02)
        & (df["dist_ma20"] >= -0.03)
        & price_below_ma20
        & (df["ma20_slope_5"] <= 0)
        & red
        & weak_close
    )
    gap_down_followthrough = (
        (df["o"] / df["c"].shift(1) - 1.0 <= -0.025)
        & red
        & weak_close
        & (df["c"] < df["ma20"])
    )
    late_chase_already_broken = (
        ma_stack_down
        & (df["ret20"] <= -0.12)
        & (df["dist_ma20"] <= -0.08)
        & (df["c"] <= df["low20"].shift(1) * 1.01)
    )
    return {
        "failed_high_distribution": failed_high,
        "exhaustion_reversal_after_20d_rise": exhaustion_reversal,
        "distribution_cluster_5d": distribution_cluster,
        "ma20_rollover_from_flat_up": ma20_rollover & (df["ret20"] >= -0.04),
        "support_break_after_tight_range": support_break_after_range,
        "weak_retest_under_ma20": weak_retest_ma20,
        "gap_down_followthrough": gap_down_followthrough,
        "late_chase_already_broken": late_chase_already_broken,
        "failed_high_plus_distribution_cluster": failed_high & distribution_cluster,
        "rollover_plus_distribution_cluster": ma20_rollover & distribution_cluster,
    }


def _summarize_event(name: str, rows: pd.DataFrame, baseline: dict[str, Any]) -> dict[str, Any]:
    n = int(len(rows))
    symbols = int(rows["code"].nunique()) if n else 0
    months = int(rows["month"].nunique()) if n else 0
    severe20 = float((rows["future_min20"] <= SEVERE_DROP_20).mean()) if n else None
    severe40 = float((rows["future_min40"] <= SEVERE_DROP_40).mean()) if n else None
    mean_ret20 = float(rows["future_ret20"].mean()) if n else None
    mean_ret40 = float(rows["future_ret40"].mean()) if n else None
    median_min20 = float(rows["future_min20"].median()) if n else None
    lift20 = None if severe20 is None else severe20 - float(baseline["severe20_rate"])
    pass_size = n >= MIN_EVENTS and symbols >= MIN_SYMBOLS and months >= MIN_MONTHS
    directional = bool(
        pass_size
        and lift20 is not None
        and lift20 > 0.02
        and mean_ret20 is not None
        and mean_ret20 < float(baseline["mean_ret20"]) - 0.002
    )
    return {
        "signal": name,
        "n": n,
        "symbols": symbols,
        "months": months,
        "mean_ret20": mean_ret20,
        "mean_ret40": mean_ret40,
        "median_future_min20": median_min20,
        "severe20_rate": severe20,
        "severe40_rate": severe40,
        "severe20_lift_vs_baseline": lift20,
        "sample_gate_pass": pass_size,
        "directional_candidate": directional,
        "decision": "hold_for_challenger_pretest" if directional else "drop_or_diagnostic_only",
    }


def run(db_path: Path, output_root: Path, code_limit: int | None) -> Path:
    run_dir = output_root / _run_id()
    runtime_status = inspect_runtime_stock_db(runtime_db_path=db_path)
    raw = _load_daily(db_path, code_limit)
    features = raw.groupby("code", group_keys=False).apply(_add_features).reset_index(drop=True)
    eligible = features.groupby("code").cumcount() >= MIN_HISTORY
    scored = features[eligible].dropna(subset=["future_ret20", "future_min20", "future_ret40", "future_min40"]).copy()
    baseline = {
        "n": int(len(scored)),
        "symbols": int(scored["code"].nunique()),
        "months": int(scored["month"].nunique()),
        "mean_ret20": float(scored["future_ret20"].mean()),
        "mean_ret40": float(scored["future_ret40"].mean()),
        "severe20_rate": float((scored["future_min20"] <= SEVERE_DROP_20).mean()),
        "severe40_rate": float((scored["future_min40"] <= SEVERE_DROP_40).mean()),
    }
    flags = _event_flags(scored)
    leaderboard = []
    examples: list[dict[str, Any]] = []
    for name, mask in flags.items():
        rows = scored[mask.fillna(False)].copy()
        summary = _summarize_event(name, rows, baseline)
        leaderboard.append(summary)
        if not rows.empty:
            sample = rows.sort_values("future_min20").head(20)
            for _, row in sample.iterrows():
                examples.append(
                    {
                        "signal": name,
                        "code": str(row["code"]),
                        "ymd": int(row["ymd"]),
                        "future_ret20": float(row["future_ret20"]),
                        "future_min20": float(row["future_min20"]),
                        "ret20": float(row["ret20"]) if pd.notna(row["ret20"]) else None,
                        "dist_ma20": float(row["dist_ma20"]) if pd.notna(row["dist_ma20"]) else None,
                        "vol_ratio": float(row["vol_ratio"]) if pd.notna(row["vol_ratio"]) else None,
                        "close_pos": float(row["close_pos"]) if pd.notna(row["close_pos"]) else None,
                    }
                )
    leaderboard.sort(
        key=lambda item: (
            bool(item["directional_candidate"]),
            float(item["severe20_lift_vs_baseline"] or -999),
            -float(item["mean_ret20"] or 999),
            int(item["n"]),
        ),
        reverse=True,
    )
    candidate_signals = [row for row in leaderboard if row["directional_candidate"]]
    decision = {
        "authoritative_decision": "hold_for_challenger_pretest" if candidate_signals else "no_promotable_signal_found",
        "reason": (
            "one_or_more_pre_crash_signals_passed_size_and_directional_gates"
            if candidate_signals
            else "no_signal_met_sample_and_directional_gates"
        ),
        "candidate_signals": [row["signal"] for row in candidate_signals],
        "promotion_allowed": False,
        "meemee_reflection": False,
        "production_ranking_change": False,
        "runtime_db_write": False,
    }
    contract = {
        "axis_id": AXIS_ID,
        "research_phase": "branching_generation",
        "fixed_evaluation_conditions": {
            "universe": "runtime_stock_db daily_bars confirmed PAN rows",
            "period": f"{START_YMD}-{END_YMD}",
            "min_history": MIN_HISTORY,
            "labels": {
                "future_ret20": "close t+20 / close t - 1",
                "future_min20": "minimum low over next 20 sessions / close t - 1",
                "severe20": f"future_min20 <= {SEVERE_DROP_20}",
                "severe40": f"future_min40 <= {SEVERE_DROP_40}",
            },
            "cost_slippage": "not_applied_discovery_only",
            "borrow_lending": "not_available_short_side_theoretical_only",
            "no_lookahead": "features use rows at or before signal date; future fields labels only",
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
            "raw_rows": int(len(raw)),
            "scored_rows": int(len(scored)),
        },
    )
    _write_json(run_dir / "baseline_outcome.json", baseline)
    _write_json(run_dir / "signal_leaderboard.json", {"baseline": baseline, "signals": leaderboard})
    _write_jsonl(run_dir / "worst_examples_by_signal.jsonl", examples)
    _write_json(run_dir / "research_decision.json", decision)
    complete = {
        "status": "complete",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "required_files": [
            "evaluation_contract.json",
            "run_manifest.json",
            "baseline_outcome.json",
            "signal_leaderboard.json",
            "worst_examples_by_signal.jsonl",
            "research_decision.json",
            "_ARTIFACT_COMPLETE.json",
        ],
    }
    _write_json(run_dir / "_ARTIFACT_COMPLETE.json", complete)
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
