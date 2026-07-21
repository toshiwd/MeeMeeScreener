from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


AXIS_ID = "tradex_ma7_break_prior20_count_analysis_v1"
DEFAULT_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\ma7_break_prior20_count_analysis_v1")


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
ma_base AS (
  SELECT
    *,
    avg(c) OVER w7 AS ma7,
    avg(c) OVER w20 AS ma20,
    avg(c) OVER w60 AS ma60,
    avg(v) OVER w20 AS vol20,
    lag(c, 1) OVER w AS prev_c,
    avg(c) OVER w7_prev AS prev_ma7,
    c / lag(c, 20) OVER w - 1 AS ret20,
    c / lag(c, 60) OVER w - 1 AS ret60,
    max(h) OVER (PARTITION BY code ORDER BY ymd ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS prior_high20,
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
    w7_prev AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING)
),
flags AS (
  SELECT
    *,
    CASE WHEN prev_c >= prev_ma7 AND c < ma7 THEN 1 ELSE 0 END AS ma7_break_event,
    CASE WHEN c < ma7 THEN 1 ELSE 0 END AS close_under_ma7
  FROM ma_base
  WHERE ma7 IS NOT NULL AND ma20 IS NOT NULL AND ma60 IS NOT NULL AND prev_ma7 IS NOT NULL
),
features AS (
  SELECT
    *,
    sum(ma7_break_event) OVER (PARTITION BY code ORDER BY ymd ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS prior20_ma7_break_count,
    sum(close_under_ma7) OVER (PARTITION BY code ORDER BY ymd ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS prior20_under_ma7_count,
    CASE WHEN h > l THEN (c - l) / (h - l) ELSE NULL END AS close_pos,
    CASE WHEN h > l THEN (h - greatest(o, c)) / (h - l) ELSE NULL END AS upper_wick,
    CASE WHEN vol20 > 0 THEN v / vol20 ELSE NULL END AS volume_vs20,
    c / ma20 - 1 AS dist_ma20,
    ma7 / ma20 - 1 AS ma7_vs_ma20,
    ma20 / ma60 - 1 AS ma20_vs_ma60,
    c / prior_high20 - 1 AS dist_high20
  FROM flags
)
SELECT *
FROM features
WHERE ma7_break_event = 1 AND f5_c IS NOT NULL
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
    }


def _count_bucket(value: Any) -> str:
    count = int(value or 0)
    if count == 0:
        return "0_first_break"
    if count == 1:
        return "1_prior_break"
    if count <= 3:
        return "2_3_prior_breaks"
    if count <= 6:
        return "4_6_prior_breaks"
    return "7plus_prior_breaks"


def _under_bucket(value: Any) -> str:
    count = int(value or 0)
    if count <= 2:
        return "under_ma7_0_2"
    if count <= 5:
        return "under_ma7_3_5"
    if count <= 10:
        return "under_ma7_6_10"
    return "under_ma7_11plus"


def _ma_bucket(row: dict[str, Any]) -> str:
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
    return "mixed"


def run(*, db_path: Path, output_root: Path) -> Path:
    run_dir = output_root / f"{_tag()}-{AXIS_ID}"
    run_dir.mkdir(parents=True, exist_ok=False)
    with duckdb.connect(str(db_path), read_only=True) as conn:
        raw_rows = conn.execute(EVENT_SQL).fetchdf().to_dict("records")

    rows = []
    for raw in raw_rows:
        row = {key: _clean(value) for key, value in raw.items()}
        row["prior20_break_bucket"] = _count_bucket(row.get("prior20_ma7_break_count"))
        row["prior20_under_bucket"] = _under_bucket(row.get("prior20_under_ma7_count"))
        row["ma_relation_bucket"] = _ma_bucket(row)
        row["strong_rise_near_high"] = bool((row.get("ret60") or 0) >= 0.30 and (row.get("dist_high20") or -1) >= -0.08)
        rows.append(row)

    evaluated: list[dict[str, Any]] = []
    for tp, sl in [(0.03, 0.05), (0.05, 0.08)]:
        sim_rows = []
        for row in rows:
            out = dict(row)
            out.update(_simulate(out, tp=tp, sl=sl))
            sim_rows.append(out)
        group_defs: list[tuple[str, list[dict[str, Any]]]] = []
        for bucket in sorted({row["prior20_break_bucket"] for row in sim_rows}):
            group_defs.append((f"break_count::{bucket}", [row for row in sim_rows if row["prior20_break_bucket"] == bucket]))
        for bucket in sorted({row["prior20_under_bucket"] for row in sim_rows}):
            group_defs.append((f"under_count::{bucket}", [row for row in sim_rows if row["prior20_under_bucket"] == bucket]))
        for ma_bucket in sorted({row["ma_relation_bucket"] for row in sim_rows}):
            for break_bucket in sorted({row["prior20_break_bucket"] for row in sim_rows}):
                group_defs.append((
                    f"ma::{ma_bucket}|break_count::{break_bucket}",
                    [row for row in sim_rows if row["ma_relation_bucket"] == ma_bucket and row["prior20_break_bucket"] == break_bucket],
                ))
        group_defs.append(("strong_rise_near_high|first_break", [row for row in sim_rows if row["strong_rise_near_high"] and row["prior20_break_bucket"] == "0_first_break"]))
        group_defs.append(("strong_rise_near_high|2_3_prior_breaks", [row for row in sim_rows if row["strong_rise_near_high"] and row["prior20_break_bucket"] == "2_3_prior_breaks"]))

        for name, group in group_defs:
            if len(group) < 200:
                continue
            recent = [row for row in group if int(str(row["ymd"])[:4]) >= 2021]
            if len(recent) < 80:
                continue
            evaluated.append({
                "slice": name,
                "tp": tp,
                "sl": sl,
                "overall": _summarize(group),
                "recent_2021_plus": _summarize(recent),
            })

    best = sorted(
        evaluated,
        key=lambda row: (
            row["recent_2021_plus"].get("positive_rate") or 0,
            row["recent_2021_plus"].get("avg_ret") or -999,
            row["overall"].get("positive_rate") or 0,
        ),
        reverse=True,
    )[:60]
    passing = [
        row for row in evaluated
        if (row["recent_2021_plus"].get("positive_rate") or 0) >= 0.60
        and (row["overall"].get("positive_rate") or 0) >= 0.55
        and (row["recent_2021_plus"].get("avg_ret") or -999) > 0
    ]

    summary = {
        "schema_version": f"{AXIS_ID}_summary_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "research_phase": "effectiveness_judgment",
        "db_path": str(db_path),
        "fixed_evaluation_conditions": {
            "event": "close crosses below MA7 after previous close was at/above previous MA7",
            "new_axis": "number of MA7 break events and closes under MA7 in the prior 20 bars",
            "entry": "signal day close",
            "exit": "short TP/SL over next 5 bars; stop-before-target ambiguity",
        },
        "event_count": len(rows),
        "best_by_recent_positive_rate": best,
        "passing_60pct_gate": passing,
        "decision": {
            "candidate_local_decision": "keep_prior20_count_axis" if passing else "hold_prior20_count_axis_no_60pct_close_entry",
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": "prior 20 break count was evaluated as a conditioning axis for MA7 break shorts",
        },
        "artifacts": {"summary_json": str(run_dir / "ma7_break_prior20_count_analysis_summary.json")},
        "runtime_db_write": False,
        "meemee_unchanged": True,
        "production_ranking_changed": False,
    }
    _write_json(run_dir / "ma7_break_prior20_count_analysis_summary.json", summary)
    _write_json(output_root / "latest_ma7_break_prior20_count_analysis_summary.json", {"run_root": str(run_dir), **summary})
    return run_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(run(db_path=args.db, output_root=args.output_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
