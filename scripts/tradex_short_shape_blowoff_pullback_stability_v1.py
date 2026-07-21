from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


AXIS_ID = "tradex_short_shape_blowoff_pullback_stability_v1"
DEFAULT_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_shape_blowoff_pullback_stability_v1")


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
    return value


def _clean_row(row: dict[str, Any]) -> dict[str, Any]:
    return {key: _clean(value) for key, value in row.items()}


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
  WHERE o IS NOT NULL AND h IS NOT NULL AND l IS NOT NULL AND c IS NOT NULL
    AND o > 0 AND h > 0 AND l > 0 AND c > 0
),
base AS (
  SELECT
    *,
    lead(ymd, 1) OVER w AS e_ymd,
    lead(o, 1) OVER w AS e_o,
    lead(h, 1) OVER w AS e_h,
    lead(l, 1) OVER w AS e_l,
    lead(c, 1) OVER w AS e_c,
    lead(h, 2) OVER w AS f1_h,
    lead(l, 2) OVER w AS f1_l,
    lead(c, 2) OVER w AS f1_c,
    lead(h, 3) OVER w AS f2_h,
    lead(l, 3) OVER w AS f2_l,
    lead(c, 3) OVER w AS f2_c,
    lead(h, 4) OVER w AS f3_h,
    lead(l, 4) OVER w AS f3_l,
    lead(c, 4) OVER w AS f3_c,
    lead(h, 5) OVER w AS f4_h,
    lead(l, 5) OVER w AS f4_l,
    lead(c, 5) OVER w AS f4_c,
    lead(h, 6) OVER w AS f5_h,
    lead(l, 6) OVER w AS f5_l,
    lead(c, 6) OVER w AS f5_c,
    avg(c) OVER w7 AS ma7,
    avg(c) OVER w20 AS ma20,
    avg(c) OVER w60 AS ma60,
    avg(v) OVER w20 AS vol20,
    min(l) OVER w60 AS low60,
    max(h) OVER w60 AS high60,
    c / lag(c, 20) OVER w - 1 AS ret20,
    c / lag(c, 60) OVER w - 1 AS ret60
  FROM normalized
  WINDOW
    w AS (PARTITION BY code ORDER BY ymd),
    w7 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
    w20 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
    w60 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
),
feat AS (
  SELECT
    *,
    CASE WHEN h > l THEN (c - l) / (h - l) ELSE NULL END AS close_pos,
    CASE WHEN h > l THEN (h - greatest(o, c)) / (h - l) ELSE NULL END AS upper_wick,
    CASE WHEN high60 > low60 THEN (c - low60) / (high60 - low60) ELSE NULL END AS range60_close_pos,
    CASE WHEN ma7 > 0 THEN c / ma7 - 1 ELSE NULL END AS dist_ma7,
    CASE WHEN ma20 > 0 THEN c / ma20 - 1 ELSE NULL END AS dist_ma20,
    CASE WHEN vol20 > 0 THEN v / vol20 ELSE NULL END AS volume_vs20,
    CASE WHEN e_h > e_l THEN (e_c - e_l) / (e_h - e_l) ELSE NULL END AS entry_close_pos
  FROM base
  WHERE ma20 IS NOT NULL AND ma60 IS NOT NULL AND vol20 IS NOT NULL
    AND e_ymd IS NOT NULL AND f5_c IS NOT NULL
    AND datediff('day', strptime(CAST(ymd AS VARCHAR), '%Y%m%d'), strptime(CAST(e_ymd AS VARCHAR), '%Y%m%d')) BETWEEN 1 AND 10
),
signals AS (
  SELECT
    *,
    e_c AS entry_price,
    e_ymd AS entry_ymd,
    e_o / c - 1 AS entry_gap_pct
  FROM feat
  WHERE ret20 >= 0.20
    AND volume_vs20 >= 1.50
    AND close_pos <= 0.35
    AND range60_close_pos >= 0.65
    AND e_h >= c * 0.98
    AND e_c <= c * 1.01
    AND e_c <= e_o
    AND entry_close_pos <= 0.45
),
paths AS (
  SELECT
    *,
    least(f1_l, f2_l, f3_l, f4_l, f5_l) / entry_price - 1 AS mae5,
    greatest(f1_h, f2_h, f3_h, f4_h, f5_h) / entry_price - 1 AS mfe5,
    f5_c / entry_price - 1 AS long_ret5
  FROM signals
)
SELECT * FROM paths
ORDER BY ymd DESC, code
"""


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


def _simulate(row: dict[str, Any], *, tp: float, sl: float) -> dict[str, Any]:
    entry = float(row["entry_price"])
    for i in range(1, 6):
        high = row.get(f"f{i}_h")
        low = row.get(f"f{i}_l")
        close = row.get(f"f{i}_c")
        if high is None or low is None or close is None:
            continue
        high = float(high)
        low = float(low)
        close = float(close)
        stop_hit = high >= entry * (1 + sl)
        tp_hit = low <= entry * (1 - tp)
        if stop_hit:
            return {"ret": -sl, "exit_reason": "stop", "bars_to_exit": i}
        if tp_hit:
            return {"ret": tp, "exit_reason": "take_profit", "bars_to_exit": i}
        if i == 5:
            return {"ret": (entry - close) / entry, "exit_reason": "time", "bars_to_exit": i}
    return {"ret": None, "exit_reason": "missing_path", "bars_to_exit": None}


def _summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    valid = [row for row in rows if row.get("ret") is not None]
    rets = [float(row["ret"]) for row in valid]
    mae = [float(row["mae5"]) for row in valid if row.get("mae5") is not None]
    mfe = [float(row["mfe5"]) for row in valid if row.get("mfe5") is not None]
    return {
        "n": len(valid),
        "avg_ret": sum(rets) / len(rets) if rets else None,
        "median_ret": _pct(rets, 0.5),
        "positive_rate": sum(1 for value in rets if value > 0) / len(rets) if rets else None,
        "tp_rate": sum(1 for row in valid if row["exit_reason"] == "take_profit") / len(valid) if valid else None,
        "stop_rate": sum(1 for row in valid if row["exit_reason"] == "stop") / len(valid) if valid else None,
        "time_exit_rate": sum(1 for row in valid if row["exit_reason"] == "time") / len(valid) if valid else None,
        "avg_bars_to_exit": sum(float(row["bars_to_exit"]) for row in valid) / len(valid) if valid else None,
        "mae5_median": _pct(mae, 0.5),
        "mfe5_median": _pct(mfe, 0.5),
    }


def _by_year(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    years = sorted({int(str(row["ymd"])[:4]) for row in rows})
    out = []
    for year in years:
        group = [row for row in rows if int(str(row["ymd"])[:4]) == year]
        out.append({"year": year, **_summarize(group)})
    return out


def _recent_events(rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    fields = [
        "code", "ymd", "entry_ymd", "entry_price", "ret", "exit_reason", "bars_to_exit",
        "mae5", "mfe5", "ret20", "ret60", "volume_vs20", "close_pos", "entry_close_pos",
        "entry_gap_pct",
    ]
    return [{key: _clean(row.get(key)) for key in fields} for row in rows[:limit]]


def run(*, db_path: Path, output_root: Path, recent_limit: int) -> Path:
    run_dir = output_root / f"{_tag()}-{AXIS_ID}"
    run_dir.mkdir(parents=True, exist_ok=False)
    with duckdb.connect(str(db_path), read_only=True) as conn:
        raw_rows = conn.execute(EVENT_SQL).fetchdf().to_dict("records")

    rows = []
    for raw in raw_rows:
        row = _clean_row(raw)
        row.update(_simulate(row, tp=0.08, sl=0.08))
        rows.append(row)

    by_year = _by_year(rows)
    recent_years = [row for row in by_year if row["year"] >= 2021]
    latest_as_of = max((int(row["ymd"]) for row in rows), default=None)
    latest_entry = max((int(row["entry_ymd"]) for row in rows), default=None)

    decision = "hold_for_selection_candidate_review"
    reason = "overall edge is positive and recent-year sample exists; visual and live-board review still required"
    if _summarize(rows)["n"] < 1000 or _summarize(rows)["avg_ret"] is None or _summarize(rows)["avg_ret"] <= 0:
        decision = "drop"
        reason = "overall sample or average return failed the fixed rule gate"
    elif recent_years and sum(1 for row in recent_years if (row.get("avg_ret") or 0) > 0) < max(1, math.ceil(len(recent_years) * 0.5)):
        decision = "hold"
        reason = "overall edge survives, but recent-year stability is not strong enough for direct promotion"

    summary = {
        "schema_version": f"{AXIS_ID}_summary_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "db_path": str(db_path),
        "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {
            "shape": "blowoff_weak_close",
            "entry": "next_pullback_reject",
            "shape_definition": {
                "ret20": ">= 0.20",
                "volume_vs20": ">= 1.50",
                "signal_close_pos": "<= 0.35",
                "range60_close_pos": ">= 0.65",
            },
            "entry_definition": {
                "entry_day_high": ">= signal_close * 0.98",
                "entry_day_close": "<= signal_close * 1.01",
                "entry_day_close_vs_open": "<= 0",
                "entry_day_close_pos": "<= 0.45",
                "entry_price": "entry_day_close",
            },
            "exit_rule": "short TP8/SL8 over next 5 bars; same-bar ambiguity uses stop-before-target",
            "universe": "confirmed daily_bars rows with valid 60-day history and five forward bars",
        },
        "overall": _summarize(rows),
        "by_year": by_year,
        "recent_years": recent_years,
        "latest_signal": {
            "latest_as_of_ymd": latest_as_of,
            "latest_entry_ymd": latest_entry,
            "recent_events": _recent_events(rows, recent_limit),
        },
        "decision": {
            "candidate_local_decision": decision,
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": reason,
        },
        "artifacts": {
            "summary_json": str(run_dir / "short_shape_blowoff_pullback_stability_summary.json"),
        },
        "runtime_db_write": False,
        "meemee_unchanged": True,
        "production_ranking_changed": False,
        "silent_fallback_used": False,
        "remaining_risks": [
            "entry trigger is daily-bar approximated, not intraday order-book execution",
            "visual MeeMee screenshot review is not yet attached to this stability artifact",
            "cost and borrow constraints are not modeled",
        ],
    }
    _write_json(run_dir / "short_shape_blowoff_pullback_stability_summary.json", summary)
    _write_json(output_root / "latest_short_shape_blowoff_pullback_stability_summary.json", {"run_root": str(run_dir), **summary})
    return run_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--recent-limit", type=int, default=40)
    args = parser.parse_args()
    print(run(db_path=args.db, output_root=args.output_root, recent_limit=args.recent_limit))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
