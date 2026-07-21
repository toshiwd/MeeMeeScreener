from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


AXIS_ID = "tradex_short_shape_support_break_capitulation_stability_v1"
DEFAULT_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_shape_support_break_capitulation_stability_v1")


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
    avg(v) OVER w20 AS vol20,
    avg(c) OVER w20 AS ma20,
    avg(c) OVER w60 AS ma60,
    min(l) OVER (PARTITION BY code ORDER BY ymd ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS prior_low20,
    c / lag(c, 20) OVER w - 1 AS ret20,
    c / lag(c, 60) OVER w - 1 AS ret60,
    lead(ymd, 1) OVER w AS entry_ymd,
    lead(l, 1) OVER w AS entry_day_low,
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
    lead(c, 6) OVER w AS f5_c
  FROM normalized
  WINDOW
    w AS (PARTITION BY code ORDER BY ymd),
    w20 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
    w60 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
),
features AS (
  SELECT
    *,
    CASE WHEN vol20 > 0 THEN v / vol20 ELSE NULL END AS volume_vs20,
    CASE WHEN h > l THEN (c - l) / (h - l) ELSE NULL END AS close_pos,
    c / ma20 - 1 AS dist_ma20,
    c / ma60 - 1 AS dist_ma60
  FROM base
  WHERE ma20 IS NOT NULL AND ma60 IS NOT NULL
    AND entry_ymd IS NOT NULL AND f5_c IS NOT NULL
    AND datediff('day', strptime(CAST(ymd AS VARCHAR), '%Y%m%d'), strptime(CAST(entry_ymd AS VARCHAR), '%Y%m%d')) BETWEEN 1 AND 10
)
SELECT
  *,
  l AS entry_price,
  least(f1_l, f2_l, f3_l, f4_l, f5_l) / l - 1 AS mae5,
  greatest(f1_h, f2_h, f3_h, f4_h, f5_h) / l - 1 AS mfe5
FROM features
WHERE c < prior_low20
  AND entry_day_low <= l
  AND volume_vs20 >= 3.0
  AND close_pos <= 0.10
  AND dist_ma20 <= -0.10
ORDER BY ymd DESC, code
"""


def _simulate(row: dict[str, Any], *, tp: float = 0.10, sl: float = 0.08) -> dict[str, Any]:
    entry = float(row["entry_price"])
    for i in range(1, 6):
        high = float(row[f"f{i}_h"])
        low = float(row[f"f{i}_l"])
        close = float(row[f"f{i}_c"])
        if high >= entry * (1 + sl):
            return {"ret": -sl, "exit_reason": "stop", "bars_to_exit": i}
        if low <= entry * (1 - tp):
            return {"ret": tp, "exit_reason": "take_profit", "bars_to_exit": i}
        if i == 5:
            return {"ret": (entry - close) / entry, "exit_reason": "time", "bars_to_exit": i}
    return {"ret": None, "exit_reason": "missing_path", "bars_to_exit": None}


def _summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    rows = [row for row in rows if row.get("ret") is not None]
    rets = [float(row["ret"]) for row in rows]
    mae = [float(row["mae5"]) for row in rows if row.get("mae5") is not None]
    mfe = [float(row["mfe5"]) for row in rows if row.get("mfe5") is not None]
    return {
        "n": len(rows),
        "avg_ret": sum(rets) / len(rets) if rets else None,
        "median_ret": _pct(rets, 0.5),
        "positive_rate": sum(1 for value in rets if value > 0) / len(rets) if rets else None,
        "tp_rate": sum(1 for row in rows if row["exit_reason"] == "take_profit") / len(rows) if rows else None,
        "stop_rate": sum(1 for row in rows if row["exit_reason"] == "stop") / len(rows) if rows else None,
        "time_exit_rate": sum(1 for row in rows if row["exit_reason"] == "time") / len(rows) if rows else None,
        "avg_bars_to_exit": sum(float(row["bars_to_exit"]) for row in rows) / len(rows) if rows else None,
        "mae5_median": _pct(mae, 0.5),
        "mfe5_median": _pct(mfe, 0.5),
    }


def _by_year(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for year in sorted({int(str(row["ymd"])[:4]) for row in rows}):
        group = [row for row in rows if int(str(row["ymd"])[:4]) == year]
        out.append({"year": year, **_summarize(group)})
    return out


def _recent(rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    fields = [
        "code", "ymd", "entry_ymd", "entry_price", "ret", "exit_reason", "bars_to_exit",
        "mae5", "mfe5", "volume_vs20", "close_pos", "dist_ma20", "ret20", "ret60",
    ]
    return [{key: _clean(row.get(key)) for key in fields} for row in rows[:limit]]


def run(*, db_path: Path, output_root: Path, recent_limit: int) -> Path:
    run_dir = output_root / f"{_tag()}-{AXIS_ID}"
    run_dir.mkdir(parents=True, exist_ok=False)
    with duckdb.connect(str(db_path), read_only=True) as conn:
        raw_rows = conn.execute(EVENT_SQL).fetchdf().to_dict("records")

    rows: list[dict[str, Any]] = []
    for raw in raw_rows:
        row = {key: _clean(value) for key, value in raw.items()}
        row.update(_simulate(row))
        rows.append(row)

    by_year = _by_year(rows)
    recent_years = [row for row in by_year if row["year"] >= 2021]
    recent_positive_years = sum(1 for row in recent_years if (row.get("avg_ret") or 0) > 0)
    decision = "keep_for_selection_candidate_review"
    reason = "fixed-condition candidate has positive all-period and 2021+ averages, with enough recent sample for review-only selection tagging"
    if _summarize(rows)["n"] < 500:
        decision = "hold"
        reason = "edge is positive but sample is too small for keep"
    elif recent_positive_years < 4:
        decision = "hold"
        reason = "edge is positive but recent year stability is mixed"

    summary = {
        "schema_version": f"{AXIS_ID}_summary_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "research_phase": "effectiveness_judgment",
        "db_path": str(db_path),
        "fixed_evaluation_conditions": {
            "shape": "support_break_capitulation",
            "entry": "next_break_signal_low",
            "shape_definition": {
                "support_break": "signal close < prior 20-bar low",
                "entry_trigger": "next trading day low <= signal low",
                "volume_vs20": ">= 3.0",
                "signal_close_pos": "<= 0.10",
                "dist_ma20": "<= -0.10",
            },
            "exit_rule": "short TP10/SL8 over next 5 bars; same-bar ambiguity uses stop-before-target",
            "universe": "confirmed daily_bars rows with valid 60-day history and five forward bars",
        },
        "overall": _summarize(rows),
        "by_year": by_year,
        "recent_years": recent_years,
        "latest_signal": {
            "latest_as_of_ymd": max((int(row["ymd"]) for row in rows), default=None),
            "latest_entry_ymd": max((int(row["entry_ymd"]) for row in rows), default=None),
            "recent_events": _recent(rows, recent_limit),
        },
        "decision": {
            "candidate_local_decision": decision,
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": reason,
        },
        "artifacts": {
            "summary_json": str(run_dir / "short_shape_support_break_capitulation_stability_summary.json"),
        },
        "runtime_db_write": False,
        "meemee_unchanged": True,
        "production_ranking_changed": False,
        "silent_fallback_used": False,
        "remaining_risks": [
            "entry trigger is daily-bar approximated and not an intraday execution replay",
            "short borrow, fees, and slippage are not modeled",
            "candidate is a review-only selection tag, not a standalone trade signal",
        ],
    }
    _write_json(run_dir / "short_shape_support_break_capitulation_stability_summary.json", summary)
    _write_json(output_root / "latest_short_shape_support_break_capitulation_stability_summary.json", {"run_root": str(run_dir), **summary})
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
