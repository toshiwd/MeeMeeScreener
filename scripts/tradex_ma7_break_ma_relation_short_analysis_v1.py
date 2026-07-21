from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


AXIS_ID = "tradex_ma7_break_ma_relation_short_analysis_v1"
DEFAULT_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\ma7_break_ma_relation_short_analysis_v1")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _clean(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if hasattr(value, "item"):
        return _clean(value.item())
    return value


def _pct(values: list[float], q: float) -> float | None:
    if not values:
        return None
    values = sorted(values)
    idx = (len(values) - 1) * q
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return values[lo]
    return values[lo] * (hi - idx) + values[hi] * (idx - lo)


EVENT_SQL = r"""
WITH normalized AS (
  SELECT
    code,
    date,
    CASE
      WHEN date > 30000000 THEN CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)
      ELSE CAST(date AS INTEGER)
    END AS ymd,
    o, h, l, c, v, source
  FROM daily_bars
  WHERE o > 0 AND h > 0 AND l > 0 AND c > 0
),
base AS (
  SELECT
    *,
    avg(c) OVER w7 AS ma7,
    avg(c) OVER w20 AS ma20,
    avg(c) OVER w60 AS ma60,
    avg(v) OVER w20 AS vol20,
    lag(c, 1) OVER w AS prev_c,
    avg(c) OVER w7_prev AS prev_ma7,
    avg(c) OVER w20_prev AS prev_ma20,
    avg(c) OVER w60_prev AS prev_ma60,
    max(h) OVER (PARTITION BY code ORDER BY ymd ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS prior_high20,
    min(l) OVER (PARTITION BY code ORDER BY ymd ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS prior_low20,
    c / lag(c, 5) OVER w - 1 AS ret5,
    c / lag(c, 20) OVER w - 1 AS ret20,
    c / lag(c, 60) OVER w - 1 AS ret60,
    lead(h, 1) OVER w AS f1_h,
    lead(l, 1) OVER w AS f1_l,
    lead(c, 1) OVER w AS f1_c,
    lead(h, 2) OVER w AS f2_h,
    lead(l, 2) OVER w AS f2_l,
    lead(c, 2) OVER w AS f2_c,
    lead(h, 3) OVER w AS f3_h,
    lead(l, 3) OVER w AS f3_l,
    lead(c, 3) OVER w AS f3_c,
    lead(h, 4) OVER w AS f4_h,
    lead(l, 4) OVER w AS f4_l,
    lead(c, 4) OVER w AS f4_c,
    lead(h, 5) OVER w AS f5_h,
    lead(l, 5) OVER w AS f5_l,
    lead(c, 5) OVER w AS f5_c
  FROM normalized
  WINDOW
    w AS (PARTITION BY code ORDER BY ymd),
    w7 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
    w20 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
    w60 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 59 PRECEDING AND CURRENT ROW),
    w7_prev AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING),
    w20_prev AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING),
    w60_prev AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING)
),
features AS (
  SELECT
    *,
    CASE WHEN h > l THEN (c - l) / (h - l) ELSE NULL END AS close_pos,
    CASE WHEN h > l THEN (h - greatest(o, c)) / (h - l) ELSE NULL END AS upper_wick,
    CASE WHEN vol20 > 0 THEN v / vol20 ELSE NULL END AS volume_vs20,
    c / ma7 - 1 AS dist_ma7,
    c / ma20 - 1 AS dist_ma20,
    c / ma60 - 1 AS dist_ma60,
    ma7 / ma20 - 1 AS ma7_vs_ma20,
    ma20 / ma60 - 1 AS ma20_vs_ma60,
    c / prior_high20 - 1 AS dist_high20,
    c / prior_low20 - 1 AS dist_low20
  FROM base
  WHERE ma7 IS NOT NULL AND ma20 IS NOT NULL AND ma60 IS NOT NULL
    AND prev_ma7 IS NOT NULL AND prev_ma20 IS NOT NULL AND prev_ma60 IS NOT NULL
    AND f5_c IS NOT NULL
)
SELECT *
FROM features
WHERE prev_c >= prev_ma7 AND c < ma7
ORDER BY ymd DESC, code
"""


def _simulate(row: dict[str, Any], *, tp: float, sl: float) -> dict[str, Any]:
    entry = float(row["c"])
    for i in range(1, 6):
        high = row.get(f"f{i}_h")
        low = row.get(f"f{i}_l")
        close = row.get(f"f{i}_c")
        if high is None or low is None or close is None:
            continue
        if float(high) >= entry * (1 + sl):
            return {"ret": -sl, "exit_reason": "stop", "bars_to_exit": i}
        if float(low) <= entry * (1 - tp):
            return {"ret": tp, "exit_reason": "take_profit", "bars_to_exit": i}
        if i == 5:
            return {"ret": (entry - float(close)) / entry, "exit_reason": "time", "bars_to_exit": i}
    return {"ret": None, "exit_reason": "missing_path", "bars_to_exit": None}


def _bucket(row: dict[str, Any]) -> str:
    c = float(row["c"])
    ma20 = float(row["ma20"])
    ma60 = float(row["ma60"])
    ma7 = float(row["ma7"])
    if c >= ma20 and ma7 >= ma20 and ma20 >= ma60:
        return "above_ma20_bull_stack"
    if c < ma20 and ma7 >= ma20 and ma20 >= ma60:
        return "first_ma20_under_bull_stack"
    if c < ma20 and ma7 < ma20 and ma20 >= ma60:
        return "under_ma20_ma20_above_ma60"
    if c < ma20 and ma20 < ma60:
        return "under_ma20_bear_stack"
    if c >= ma20 and ma20 < ma60:
        return "above_ma20_in_bear_stack"
    return "mixed"


def _pattern_flags(row: dict[str, Any]) -> list[str]:
    flags = []
    if (row.get("ret20") or 0) >= 0.10:
        flags.append("ret20_up10")
    if (row.get("ret60") or 0) >= 0.30:
        flags.append("ret60_up30")
    if (row.get("close_pos") or 0) <= 0.25:
        flags.append("weak_close")
    if (row.get("upper_wick") or 0) >= 0.35:
        flags.append("upper_wick")
    if (row.get("volume_vs20") or 0) >= 1.5:
        flags.append("volume15")
    if (row.get("dist_high20") or -1) >= -0.08:
        flags.append("near_high20")
    if (row.get("dist_ma20") or 0) <= -0.03:
        flags.append("under_ma20_3")
    return flags


def _summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    rows = [row for row in rows if row.get("ret") is not None]
    rets = [float(row["ret"]) for row in rows]
    return {
        "n": len(rows),
        "avg_ret": sum(rets) / len(rets) if rets else None,
        "median_ret": _pct(rets, 0.5),
        "positive_rate": sum(1 for ret in rets if ret > 0) / len(rets) if rets else None,
        "tp_rate": sum(1 for row in rows if row["exit_reason"] == "take_profit") / len(rows) if rows else None,
        "stop_rate": sum(1 for row in rows if row["exit_reason"] == "stop") / len(rows) if rows else None,
        "time_exit_rate": sum(1 for row in rows if row["exit_reason"] == "time") / len(rows) if rows else None,
        "avg_bars_to_exit": sum(float(row["bars_to_exit"]) for row in rows) / len(rows) if rows else None,
    }


def _eval_slice(rows: list[dict[str, Any]], *, name: str, tp: float, sl: float) -> dict[str, Any] | None:
    evaluated = []
    for row in rows:
        out = dict(row)
        out.update(_simulate(out, tp=tp, sl=sl))
        evaluated.append(out)
    if len(evaluated) < 200:
        return None
    recent = [row for row in evaluated if int(str(row["ymd"])[:4]) >= 2021]
    if len(recent) < 80:
        return None
    return {
        "slice": name,
        "tp": tp,
        "sl": sl,
        "overall": _summarize(evaluated),
        "recent_2021_plus": _summarize(recent),
    }


def _recent_current(rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    fields = [
        "code", "ymd", "c", "ma7", "ma20", "ma60", "bucket", "ret5", "ret20", "ret60",
        "close_pos", "upper_wick", "volume_vs20", "dist_ma20", "ma7_vs_ma20", "ma20_vs_ma60",
    ]
    return [{key: _clean(row.get(key)) for key in fields} for row in rows[:limit]]


def run(*, db_path: Path, output_root: Path, recent_limit: int) -> Path:
    run_dir = output_root / f"{_tag()}-{AXIS_ID}"
    run_dir.mkdir(parents=True, exist_ok=False)
    with duckdb.connect(str(db_path), read_only=True) as conn:
        raw_rows = conn.execute(EVENT_SQL).fetchdf().to_dict("records")

    rows = []
    for raw in raw_rows:
        row = {key: _clean(value) for key, value in raw.items()}
        row["bucket"] = _bucket(row)
        row["flags"] = _pattern_flags(row)
        rows.append(row)

    rules: list[tuple[str, list[dict[str, Any]]]] = []
    rules.append(("all_ma7_break", rows))
    for bucket in sorted({row["bucket"] for row in rows}):
        rules.append((bucket, [row for row in rows if row["bucket"] == bucket]))
    flag_sets = [
        ("weak_close", lambda row: "weak_close" in row["flags"]),
        ("upper_wick", lambda row: "upper_wick" in row["flags"]),
        ("volume15", lambda row: "volume15" in row["flags"]),
        ("ret60_up30", lambda row: "ret60_up30" in row["flags"]),
        ("ret60_up30_weak_close", lambda row: "ret60_up30" in row["flags"] and "weak_close" in row["flags"]),
        ("ret60_up30_near_high20", lambda row: "ret60_up30" in row["flags"] and "near_high20" in row["flags"]),
        ("first_ma20_under_bull_stack_weak", lambda row: row["bucket"] == "first_ma20_under_bull_stack" and "weak_close" in row["flags"]),
        ("above_ma20_bull_stack_weak", lambda row: row["bucket"] == "above_ma20_bull_stack" and "weak_close" in row["flags"]),
        ("under_ma20_bear_stack_weak", lambda row: row["bucket"] == "under_ma20_bear_stack" and "weak_close" in row["flags"]),
        ("under_ma20_ma20_above_ma60_weak", lambda row: row["bucket"] == "under_ma20_ma20_above_ma60" and "weak_close" in row["flags"]),
    ]
    for name, fn in flag_sets:
        rules.append((name, [row for row in rows if fn(row)]))

    evaluated = []
    for name, slice_rows in rules:
        for tp, sl in [(0.03, 0.05), (0.05, 0.05), (0.05, 0.08), (0.08, 0.08)]:
            result = _eval_slice(slice_rows, name=name, tp=tp, sl=sl)
            if result:
                evaluated.append(result)

    best_by_recent_positive = sorted(
        evaluated,
        key=lambda row: (
            row["recent_2021_plus"].get("positive_rate") or 0,
            row["recent_2021_plus"].get("avg_ret") or -999,
            row["overall"].get("positive_rate") or 0,
        ),
        reverse=True,
    )[:40]
    passing = [
        row
        for row in evaluated
        if (row["recent_2021_plus"].get("positive_rate") or 0) >= 0.60
        and (row["overall"].get("positive_rate") or 0) >= 0.55
        and (row["recent_2021_plus"].get("avg_ret") or -999) > 0
    ]

    latest_ymd = max((int(row["ymd"]) for row in rows), default=None)
    latest_rows = [row for row in rows if int(row["ymd"]) == latest_ymd] if latest_ymd else []
    summary = {
        "schema_version": f"{AXIS_ID}_summary_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "research_phase": "effectiveness_judgment",
        "db_path": str(db_path),
        "fixed_evaluation_conditions": {
            "event": "close crosses below MA7 after previous close was at/above previous MA7",
            "entry": "signal day close",
            "exit_rules": "short TP/SL over next 5 bars; same-bar ambiguity uses stop-before-target",
            "focus": "relationship between MA7 break and MA20/MA60 position/order",
        },
        "event_count": len(rows),
        "latest_ymd": latest_ymd,
        "latest_signal_count": len(latest_rows),
        "latest_signals": _recent_current(latest_rows, recent_limit),
        "best_by_recent_positive_rate": best_by_recent_positive,
        "passing_60pct_gate": passing,
        "decision": {
            "candidate_local_decision": "keep_for_further_entry_timing_review" if passing else "drop_close_entry_ma7_break",
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": (
                "some MA7-break slices passed the 60pct recent gate"
                if passing
                else "MA7 break close-entry slices did not pass the 60pct gate; likely need retest/failure entry rather than immediate close-entry"
            ),
        },
        "artifacts": {
            "summary_json": str(run_dir / "ma7_break_ma_relation_short_analysis_summary.json"),
        },
        "runtime_db_write": False,
        "meemee_unchanged": True,
        "production_ranking_changed": False,
        "silent_fallback_used": False,
    }
    _write_json(run_dir / "ma7_break_ma_relation_short_analysis_summary.json", summary)
    _write_json(output_root / "latest_ma7_break_ma_relation_short_analysis_summary.json", {"run_root": str(run_dir), **summary})
    return run_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--recent-limit", type=int, default=80)
    args = parser.parse_args()
    print(run(db_path=args.db, output_root=args.output_root, recent_limit=args.recent_limit))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
