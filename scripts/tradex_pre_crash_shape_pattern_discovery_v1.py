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


AXIS_ID = "pre_crash_shape_pattern_discovery_v1"
DEFAULT_OUTPUT_ROOT = Path("G:/Tradex/pre_crash_shape_pattern_discovery_v1")
START_YMD = 20200101
END_YMD = 20991231
MIN_HISTORY = 140
CRASH_THRESHOLD_20 = -0.15
MIN_PATTERN_EVENTS = 80
MIN_PATTERN_SYMBOLS = 20
MIN_PATTERN_MONTHS = 8


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


def _safe_ret(last: float, first: float) -> float | None:
    if first <= 0:
        return None
    return last / first - 1.0


def _segment_features(window: pd.DataFrame) -> dict[str, float | None]:
    close = window["c"].astype(float).tolist()
    high = window["h"].astype(float).tolist()
    low = window["l"].astype(float).tolist()
    vol = window["v"].replace(0, float("nan")).astype(float)
    if len(close) < 80:
        return {}
    early = close[-80:-40]
    mid = close[-40:-20]
    late = close[-20:]
    late_high = max(high[-20:])
    prior_high = max(high[-80:-20])
    prior_low = min(low[-80:-20])
    range_mid = max(high[-40:-20]) / min(low[-40:-20]) - 1.0 if min(low[-40:-20]) > 0 else None
    range_late = max(high[-20:]) / min(low[-20:]) - 1.0 if min(low[-20:]) > 0 else None
    vol20 = vol.rolling(20).mean()
    last_vol_ratio = float(vol.iloc[-1] / vol20.iloc[-1]) if vol20.iloc[-1] and not pd.isna(vol20.iloc[-1]) else None
    red_cluster = 0
    weak_close_cluster = 0
    for _, row in window.tail(10).iterrows():
        span = float(row["h"] - row["l"])
        if span <= 0:
            continue
        close_pos = float((row["c"] - row["l"]) / span)
        if row["c"] < row["o"]:
            red_cluster += 1
        if close_pos <= 0.35:
            weak_close_cluster += 1
    return {
        "ret_80_40": _safe_ret(early[-1], early[0]),
        "ret_40_20": _safe_ret(mid[-1], mid[0]),
        "ret_20_0": _safe_ret(late[-1], late[0]),
        "ret_60_0": _safe_ret(close[-1], close[-60]),
        "range_40_20": range_mid,
        "range_20_0": range_late,
        "dist_prior_80_high": close[-1] / prior_high - 1.0 if prior_high > 0 else None,
        "dist_prior_80_low": close[-1] / prior_low - 1.0 if prior_low > 0 else None,
        "late_high_break": late_high / prior_high - 1.0 if prior_high > 0 else None,
        "last_vol_ratio": last_vol_ratio,
        "red_cluster_10": float(red_cluster),
        "weak_close_cluster_10": float(weak_close_cluster),
    }


def _classify_shape(f: dict[str, float | None]) -> str:
    r1 = f.get("ret_80_40")
    r2 = f.get("ret_40_20")
    r3 = f.get("ret_20_0")
    range_mid = f.get("range_40_20")
    dist_high = f.get("dist_prior_80_high")
    dist_low = f.get("dist_prior_80_low")
    late_high_break = f.get("late_high_break")
    vol = f.get("last_vol_ratio")
    red = f.get("red_cluster_10") or 0
    weak = f.get("weak_close_cluster_10") or 0
    if None in {r1, r2, r3, range_mid, dist_high, dist_low, late_high_break}:
        return "unclassified_insufficient_features"
    if r1 >= 0.12 and abs(r2) <= 0.05 and range_mid <= 0.16 and r3 <= -0.04:
        return "rise_flat_breakdown"
    if r1 >= 0.10 and late_high_break >= 0.00 and r3 <= -0.03 and weak >= 4:
        return "failed_new_high_reversal"
    if r1 >= 0.08 and r2 >= 0.04 and r3 >= -0.02 and (vol or 0) >= 1.5 and weak >= 3:
        return "parabolic_exhaustion_distribution"
    if r1 <= -0.06 and abs(r2) <= 0.05 and r3 <= -0.04 and red >= 5:
        return "downtrend_pause_second_leg"
    if abs(r1) <= 0.05 and abs(r2) <= 0.05 and range_mid <= 0.12 and r3 <= -0.05:
        return "long_range_support_break"
    if r1 >= 0.05 and r2 <= -0.04 and r3 <= -0.04 and dist_high <= -0.08:
        return "lower_high_rollover"
    if dist_low <= 0.06 and r3 <= -0.05 and red >= 5:
        return "support_grind_then_flush"
    return "other_pre_crash_shape"


def _build_events(daily: pd.DataFrame) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for code, group in daily.groupby("code", sort=False):
        g = group.sort_values("ymd").reset_index(drop=True)
        closes = g["c"].astype(float)
        lows = g["l"].astype(float)
        future_min20 = lows.shift(-1).rolling(20).min().shift(-19) / closes - 1.0
        for idx in range(MIN_HISTORY, len(g) - 20):
            if pd.isna(future_min20.iloc[idx]) or float(future_min20.iloc[idx]) > CRASH_THRESHOLD_20:
                continue
            window = g.iloc[idx - 80 : idx + 1]
            f = _segment_features(window)
            pattern = _classify_shape(f)
            events.append(
                {
                    "code": str(code),
                    "ymd": int(g.iloc[idx]["ymd"]),
                    "month": int(g.iloc[idx]["ymd"]) // 100,
                    "pattern": pattern,
                    "future_min20": float(future_min20.iloc[idx]),
                    "future_ret20": float(closes.iloc[idx + 20] / closes.iloc[idx] - 1.0),
                    **f,
                }
            )
    meta = {
        "crash_threshold_20": CRASH_THRESHOLD_20,
        "event_count": len(events),
        "symbol_count": len({row["code"] for row in events}),
        "month_count": len({row["month"] for row in events}),
    }
    return events, meta


def _summarize(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not events:
        return []
    df = pd.DataFrame(events)
    rows = []
    total = len(df)
    for pattern, group in df.groupby("pattern"):
        n = int(len(group))
        symbols = int(group["code"].nunique())
        months = int(group["month"].nunique())
        sample_gate = n >= MIN_PATTERN_EVENTS and symbols >= MIN_PATTERN_SYMBOLS and months >= MIN_PATTERN_MONTHS
        rows.append(
            {
                "pattern": str(pattern),
                "n": n,
                "share_of_crash_events": n / total,
                "symbols": symbols,
                "months": months,
                "mean_future_min20": float(group["future_min20"].mean()),
                "median_future_min20": float(group["future_min20"].median()),
                "mean_future_ret20": float(group["future_ret20"].mean()),
                "sample_gate_pass": sample_gate,
                "decision": "typical_pattern" if sample_gate else "thin_diagnostic_only",
            }
        )
    rows.sort(key=lambda item: (item["sample_gate_pass"], item["n"]), reverse=True)
    return rows


def run(db_path: Path, output_root: Path, code_limit: int | None) -> Path:
    run_dir = output_root / _run_id()
    runtime_status = inspect_runtime_stock_db(runtime_db_path=db_path)
    daily = _load_daily(db_path, code_limit)
    events, event_meta = _build_events(daily)
    leaderboard = _summarize(events)
    typical = [row for row in leaderboard if row["decision"] == "typical_pattern"]
    examples = []
    if events:
        df = pd.DataFrame(events)
        for pattern in [row["pattern"] for row in leaderboard]:
            sample = df[df["pattern"] == pattern].sort_values("future_min20").head(15)
            examples.extend(sample.to_dict(orient="records"))
    decision = {
        "authoritative_decision": "typical_patterns_found" if typical else "no_stable_typical_patterns_found",
        "typical_patterns": [row["pattern"] for row in typical],
        "pattern_count": len(leaderboard),
        "promotion_allowed": False,
        "meemee_reflection": False,
        "production_ranking_change": False,
        "runtime_db_write": False,
        "reason": "shape_patterns_group_crash_events_only_not_entry_edge",
    }
    contract = {
        "axis_id": AXIS_ID,
        "research_phase": "branching_generation",
        "fixed_evaluation_conditions": {
            "universe": "runtime_stock_db daily_bars confirmed PAN rows",
            "period": f"{START_YMD}-{END_YMD}",
            "event_definition": f"future_min20 <= {CRASH_THRESHOLD_20}",
            "shape_window": "80 sessions ending at signal date, segmented into 80-40, 40-20, and 20-0 session blocks",
            "no_lookahead": "shape features use rows at or before signal date; future_min20 only selects crash events",
            "cost_slippage": "not_applied_pattern_discovery_only",
            "borrow_lending": "not_available_short_side_theoretical_only",
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
            "event_meta": event_meta,
        },
    )
    _write_json(run_dir / "pattern_leaderboard.json", {"event_meta": event_meta, "patterns": leaderboard})
    _write_jsonl(run_dir / "pattern_examples.jsonl", examples)
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
                "pattern_leaderboard.json",
                "pattern_examples.jsonl",
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
