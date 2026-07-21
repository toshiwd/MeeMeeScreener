from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


AXIS_ID = "tradex_short_ma_break_length_compare_v1"
DEFAULT_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_ma_break_length_compare_v1")


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


FEATURE_SQL = r"""
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
    avg(c) OVER w3 AS ma3,
    avg(c) OVER w5 AS ma5,
    avg(c) OVER w7 AS ma7,
    avg(c) OVER w10 AS ma10,
    avg(c) OVER w20 AS ma20,
    avg(c) OVER w60 AS ma60,
    avg(v) OVER w20 AS vol20,
    lag(c, 1) OVER w AS prev_c,
    avg(c) OVER w3_prev AS prev_ma3,
    avg(c) OVER w5_prev AS prev_ma5,
    avg(c) OVER w7_prev AS prev_ma7,
    avg(c) OVER w10_prev AS prev_ma10,
    avg(c) OVER w20_prev AS prev_ma20,
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
    w3 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),
    w5 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 4 PRECEDING AND CURRENT ROW),
    w7 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
    w10 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 9 PRECEDING AND CURRENT ROW),
    w20 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
    w60 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 59 PRECEDING AND CURRENT ROW),
    w3_prev AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING),
    w5_prev AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING),
    w7_prev AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING),
    w10_prev AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING),
    w20_prev AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING)
),
flags AS (
  SELECT
    *,
    CASE WHEN prev_c >= prev_ma3 AND c < ma3 THEN 1 ELSE 0 END AS break3,
    CASE WHEN prev_c >= prev_ma5 AND c < ma5 THEN 1 ELSE 0 END AS break5,
    CASE WHEN prev_c >= prev_ma7 AND c < ma7 THEN 1 ELSE 0 END AS break7,
    CASE WHEN prev_c >= prev_ma10 AND c < ma10 THEN 1 ELSE 0 END AS break10,
    CASE WHEN prev_c >= prev_ma20 AND c < ma20 THEN 1 ELSE 0 END AS break20,
    CASE WHEN c < ma3 THEN 1 ELSE 0 END AS under3,
    CASE WHEN c < ma5 THEN 1 ELSE 0 END AS under5,
    CASE WHEN c < ma7 THEN 1 ELSE 0 END AS under7,
    CASE WHEN c < ma10 THEN 1 ELSE 0 END AS under10,
    CASE WHEN c < ma20 THEN 1 ELSE 0 END AS under20
  FROM base
  WHERE ma20 IS NOT NULL AND ma60 IS NOT NULL AND prev_ma20 IS NOT NULL AND f5_c IS NOT NULL
),
features AS (
  SELECT
    *,
    sum(break3) OVER (PARTITION BY code ORDER BY ymd ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS prior20_break3,
    sum(break5) OVER (PARTITION BY code ORDER BY ymd ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS prior20_break5,
    sum(break7) OVER (PARTITION BY code ORDER BY ymd ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS prior20_break7,
    sum(break10) OVER (PARTITION BY code ORDER BY ymd ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS prior20_break10,
    sum(break20) OVER (PARTITION BY code ORDER BY ymd ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS prior20_break20,
    CASE WHEN h > l THEN (c - l) / (h - l) ELSE NULL END AS close_pos,
    CASE WHEN h > l THEN (h - greatest(o, c)) / (h - l) ELSE NULL END AS upper_wick,
    CASE WHEN vol20 > 0 THEN v / vol20 ELSE NULL END AS volume_vs20,
    c / ma20 - 1 AS dist_ma20,
    ma20 / ma60 - 1 AS ma20_vs_ma60,
    c / prior_high20 - 1 AS dist_high20
  FROM flags
)
SELECT * FROM features
WHERE break3 = 1 OR break5 = 1 OR break7 = 1 OR break10 = 1 OR break20 = 1
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


def _eval(rows: list[dict[str, Any]], *, name: str, tp: float, sl: float) -> dict[str, Any] | None:
    if len(rows) < 500:
        return None
    out = []
    for row in rows:
        item = dict(row)
        item.update(_simulate(item, tp=tp, sl=sl))
        out.append(item)
    recent = [row for row in out if int(str(row["ymd"])[:4]) >= 2021]
    if len(recent) < 100:
        return None
    return {"slice": name, "tp": tp, "sl": sl, "overall": _summarize(out), "recent_2021_plus": _summarize(recent)}


def run(*, db_path: Path, output_root: Path) -> Path:
    run_dir = output_root / f"{_tag()}-{AXIS_ID}"
    run_dir.mkdir(parents=True, exist_ok=False)
    with duckdb.connect(str(db_path), read_only=True) as conn:
        raw = conn.execute(FEATURE_SQL).fetchdf().to_dict("records")
    rows = [{key: _clean(value) for key, value in row.items()} for row in raw]

    lengths = [3, 5, 7, 10, 20]
    slices: list[tuple[str, list[dict[str, Any]]]] = []
    for n in lengths:
        slices.append((f"ma{n}_break_all", [r for r in rows if r.get(f"break{n}") == 1]))
        slices.append((f"ma{n}_first_break", [r for r in rows if r.get(f"break{n}") == 1 and int(r.get(f"prior20_break{n}") or 0) == 0]))
        slices.append((f"ma{n}_break_weak_close", [r for r in rows if r.get(f"break{n}") == 1 and (r.get("close_pos") or 9) <= 0.25]))
        slices.append((f"ma{n}_first_break_weak_close", [r for r in rows if r.get(f"break{n}") == 1 and int(r.get(f"prior20_break{n}") or 0) == 0 and (r.get("close_pos") or 9) <= 0.25]))
        slices.append((f"ma{n}_first_break_strong_rise_near_high", [
            r for r in rows
            if r.get(f"break{n}") == 1
            and int(r.get(f"prior20_break{n}") or 0) == 0
            and (r.get("ret60") or 0) >= 0.30
            and (r.get("dist_high20") or -1) >= -0.08
        ]))

    evaluated = []
    for name, group in slices:
        for tp, sl in [(0.03, 0.05), (0.05, 0.08), (0.08, 0.08)]:
            result = _eval(group, name=name, tp=tp, sl=sl)
            if result:
                evaluated.append(result)

    best = sorted(
        evaluated,
        key=lambda row: (
            row["recent_2021_plus"].get("positive_rate") or 0,
            row["recent_2021_plus"].get("avg_ret") or -999,
            row["overall"].get("positive_rate") or 0,
        ),
        reverse=True,
    )[:60]
    best_by_ma = {}
    for n in lengths:
        ma_rows = [row for row in evaluated if row["slice"].startswith(f"ma{n}_")]
        best_by_ma[f"ma{n}"] = sorted(
            ma_rows,
            key=lambda row: (
                row["recent_2021_plus"].get("positive_rate") or 0,
                row["recent_2021_plus"].get("avg_ret") or -999,
            ),
            reverse=True,
        )[:10]
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
            "ma_lengths": lengths,
            "event": "previous close at/above MA, current close below MA",
            "entry": "signal day close",
            "exit": "short TP/SL over next 5 bars; stop-before-target ambiguity",
            "common_filters": ["all", "first_break_in_prior20", "weak_close", "first_break_strong_rise_near_high"],
        },
        "best_by_recent_positive_rate": best,
        "best_by_ma": best_by_ma,
        "passing_60pct_gate": passing,
        "decision": {
            "candidate_local_decision": "keep_best_ma_break_for_review" if passing else "hold_no_ma_break_60pct_close_entry",
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": "short MA break lengths were compared under fixed close-entry short rules",
        },
        "artifacts": {"summary_json": str(run_dir / "short_ma_break_length_compare_summary.json")},
        "runtime_db_write": False,
        "meemee_unchanged": True,
        "production_ranking_changed": False,
    }
    _write_json(run_dir / "short_ma_break_length_compare_summary.json", summary)
    _write_json(output_root / "latest_short_ma_break_length_compare_summary.json", {"run_root": str(run_dir), **summary})
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
