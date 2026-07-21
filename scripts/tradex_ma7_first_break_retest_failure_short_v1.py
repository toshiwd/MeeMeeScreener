from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


AXIS_ID = "tradex_ma7_first_break_retest_failure_short_v1"
DEFAULT_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\ma7_first_break_retest_failure_short_v1")


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
    c / lag(c, 20) OVER w - 1 AS ret20,
    c / lag(c, 60) OVER w - 1 AS ret60,
    max(h) OVER (PARTITION BY code ORDER BY ymd ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS prior_high20,
    lead(ymd, 1) OVER w AS r1_ymd,
    lead(o, 1) OVER w AS r1_o,
    lead(h, 1) OVER w AS r1_h,
    lead(l, 1) OVER w AS r1_l,
    lead(c, 1) OVER w AS r1_c,
    avg(c) OVER w7_f1 AS r1_ma7,
    lead(ymd, 2) OVER w AS r2_ymd,
    lead(o, 2) OVER w AS r2_o,
    lead(h, 2) OVER w AS r2_h,
    lead(l, 2) OVER w AS r2_l,
    lead(c, 2) OVER w AS r2_c,
    avg(c) OVER w7_f2 AS r2_ma7,
    lead(ymd, 3) OVER w AS r3_ymd,
    lead(o, 3) OVER w AS r3_o,
    lead(h, 3) OVER w AS r3_h,
    lead(l, 3) OVER w AS r3_l,
    lead(c, 3) OVER w AS r3_c,
    avg(c) OVER w7_f3 AS r3_ma7,
    lead(h, 4) OVER w AS f1_h,
    lead(l, 4) OVER w AS f1_l,
    lead(c, 4) OVER w AS f1_c,
    lead(h, 5) OVER w AS f2_h,
    lead(l, 5) OVER w AS f2_l,
    lead(c, 5) OVER w AS f2_c,
    lead(h, 6) OVER w AS f3_h,
    lead(l, 6) OVER w AS f3_l,
    lead(c, 6) OVER w AS f3_c,
    lead(h, 7) OVER w AS f4_h,
    lead(l, 7) OVER w AS f4_l,
    lead(c, 7) OVER w AS f4_c,
    lead(h, 8) OVER w AS f5_h,
    lead(l, 8) OVER w AS f5_l,
    lead(c, 8) OVER w AS f5_c
  FROM normalized
  WINDOW
    w AS (PARTITION BY code ORDER BY ymd),
    w7 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
    w20 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
    w60 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 59 PRECEDING AND CURRENT ROW),
    w7_prev AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING),
    w7_f1 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 5 PRECEDING AND 1 FOLLOWING),
    w7_f2 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 4 PRECEDING AND 2 FOLLOWING),
    w7_f3 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING)
),
flags AS (
  SELECT
    *,
    CASE WHEN prev_c >= prev_ma7 AND c < ma7 THEN 1 ELSE 0 END AS ma7_break_event,
    CASE WHEN c < ma7 THEN 1 ELSE 0 END AS close_under_ma7
  FROM base
  WHERE ma7 IS NOT NULL AND ma20 IS NOT NULL AND ma60 IS NOT NULL AND prev_ma7 IS NOT NULL
),
features AS (
  SELECT
    *,
    sum(ma7_break_event) OVER (PARTITION BY code ORDER BY ymd ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS prior20_ma7_break_count,
    CASE WHEN vol20 > 0 THEN v / vol20 ELSE NULL END AS volume_vs20,
    c / ma20 - 1 AS dist_ma20,
    ma7 / ma20 - 1 AS ma7_vs_ma20,
    ma20 / ma60 - 1 AS ma20_vs_ma60,
    c / prior_high20 - 1 AS dist_high20
  FROM flags
)
SELECT *
FROM features
WHERE ma7_break_event = 1
  AND prior20_ma7_break_count = 0
  AND f5_c IS NOT NULL
ORDER BY ymd DESC, code
"""


def _bar_close_pos(row: dict[str, Any], prefix: str) -> float | None:
    h = row.get(f"{prefix}_h")
    l = row.get(f"{prefix}_l")
    c = row.get(f"{prefix}_c")
    if h is None or l is None or c is None or float(h) <= float(l):
        return None
    return (float(c) - float(l)) / (float(h) - float(l))


def _bar_upper_wick(row: dict[str, Any], prefix: str) -> float | None:
    h = row.get(f"{prefix}_h")
    l = row.get(f"{prefix}_l")
    o = row.get(f"{prefix}_o")
    c = row.get(f"{prefix}_c")
    if h is None or l is None or o is None or c is None or float(h) <= float(l):
        return None
    return (float(h) - max(float(o), float(c))) / (float(h) - float(l))


def _entry_signal(row: dict[str, Any], *, max_day: int, retest_band: float, close_pos_max: float, upper_wick_min: float | None) -> dict[str, Any] | None:
    for day in range(1, max_day + 1):
        p = f"r{day}"
        high = row.get(f"{p}_h")
        close = row.get(f"{p}_c")
        ma7 = row.get(f"{p}_ma7")
        ymd = row.get(f"{p}_ymd")
        if high is None or close is None or ma7 is None:
            continue
        retested = float(high) >= float(ma7) * (1 - retest_band)
        weak_close = (_bar_close_pos(row, p) or 999) <= close_pos_max
        under_ma7_close = float(close) < float(ma7)
        wick_ok = True if upper_wick_min is None else ((_bar_upper_wick(row, p) or 0) >= upper_wick_min)
        if retested and weak_close and under_ma7_close and wick_ok:
            return {
                "entry_day": day,
                "entry_ymd": int(ymd),
                "entry_price": float(close),
                "entry_close_pos": _bar_close_pos(row, p),
                "entry_upper_wick": _bar_upper_wick(row, p),
                "entry_ma7": float(ma7),
            }
    return None


def _simulate_from_entry(row: dict[str, Any], entry: dict[str, Any], *, tp: float, sl: float) -> dict[str, Any]:
    entry_price = float(entry["entry_price"])
    start = int(entry["entry_day"]) + 1
    # The query provides forward bars as f1..f5 from after r3, so use raw r/f path conservatively.
    path: list[tuple[float, float, float]] = []
    for day in range(start, 4):
        p = f"r{day}"
        if row.get(f"{p}_h") is not None:
            path.append((float(row[f"{p}_h"]), float(row[f"{p}_l"]), float(row[f"{p}_c"])))
    for i in range(1, 6):
        if row.get(f"f{i}_h") is not None:
            path.append((float(row[f"f{i}_h"]), float(row[f"f{i}_l"]), float(row[f"f{i}_c"])))
    path = path[:5]
    for i, (high, low, close) in enumerate(path, start=1):
        if high >= entry_price * (1 + sl):
            return {"ret": -sl, "exit_reason": "stop", "bars_to_exit": i}
        if low <= entry_price * (1 - tp):
            return {"ret": tp, "exit_reason": "take_profit", "bars_to_exit": i}
        if i == len(path):
            return {"ret": (entry_price - close) / entry_price, "exit_reason": "time", "bars_to_exit": i}
    return {"ret": None, "exit_reason": "missing_path", "bars_to_exit": None}


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


def run(*, db_path: Path, output_root: Path) -> Path:
    run_dir = output_root / f"{_tag()}-{AXIS_ID}"
    run_dir.mkdir(parents=True, exist_ok=False)
    with duckdb.connect(str(db_path), read_only=True) as conn:
        raw_rows = conn.execute(EVENT_SQL).fetchdf().to_dict("records")
    rows = [{key: _clean(value) for key, value in raw.items()} for raw in raw_rows]
    for row in rows:
        row["ma_relation_bucket"] = _ma_bucket(row)
        row["strong_rise_near_high"] = bool((row.get("ret60") or 0) >= 0.30 and (row.get("dist_high20") or -1) >= -0.08)

    evaluated = []
    configs = [
        {"name": "retest_3d_weak35", "max_day": 3, "retest_band": 0.005, "close_pos_max": 0.35, "upper_wick_min": None},
        {"name": "retest_3d_weak25", "max_day": 3, "retest_band": 0.005, "close_pos_max": 0.25, "upper_wick_min": None},
        {"name": "retest_3d_upper_wick35_weak45", "max_day": 3, "retest_band": 0.005, "close_pos_max": 0.45, "upper_wick_min": 0.35},
        {"name": "tight_retest_3d_weak35", "max_day": 3, "retest_band": 0.0, "close_pos_max": 0.35, "upper_wick_min": None},
    ]
    for cfg in configs:
        entries = []
        for row in rows:
            entry = _entry_signal(row, max_day=cfg["max_day"], retest_band=cfg["retest_band"], close_pos_max=cfg["close_pos_max"], upper_wick_min=cfg["upper_wick_min"])
            if entry:
                enriched = dict(row)
                enriched.update(entry)
                entries.append(enriched)
        slices = [("all", entries)]
        for bucket in sorted({row["ma_relation_bucket"] for row in entries}):
            slices.append((f"ma::{bucket}", [row for row in entries if row["ma_relation_bucket"] == bucket]))
        slices.append(("strong_rise_near_high", [row for row in entries if row["strong_rise_near_high"]]))
        for name, group in slices:
            if len(group) < 200:
                continue
            for tp, sl in [(0.03, 0.05), (0.05, 0.08), (0.08, 0.08)]:
                sim_rows = []
                for row in group:
                    out = dict(row)
                    out.update(_simulate_from_entry(out, out, tp=tp, sl=sl))
                    sim_rows.append(out)
                recent = [row for row in sim_rows if int(str(row["ymd"])[:4]) >= 2021]
                if len(recent) < 80:
                    continue
                evaluated.append({
                    "slice": f"{cfg['name']}|{name}",
                    "tp": tp,
                    "sl": sl,
                    "overall": _summarize(sim_rows),
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
    )[:50]
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
        "fixed_evaluation_conditions": {
            "base_event": "first MA7 break in prior 20 bars",
            "entry": "1-3 bars later retest near MA7, close back under MA7 with weak close or upper wick",
            "exit": "short TP/SL over next 5 bars; stop-before-target ambiguity",
        },
        "base_event_count": len(rows),
        "best_by_recent_positive_rate": best,
        "passing_60pct_gate": passing,
        "decision": {
            "candidate_local_decision": "keep_for_selection_candidate_review" if passing else "hold_no_60pct_retest_failure",
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": "first MA7 break retest-failure entry was evaluated",
        },
        "artifacts": {"summary_json": str(run_dir / "ma7_first_break_retest_failure_short_summary.json")},
        "runtime_db_write": False,
        "meemee_unchanged": True,
        "production_ranking_changed": False,
    }
    _write_json(run_dir / "ma7_first_break_retest_failure_short_summary.json", summary)
    _write_json(output_root / "latest_ma7_first_break_retest_failure_short_summary.json", {"run_root": str(run_dir), **summary})
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
