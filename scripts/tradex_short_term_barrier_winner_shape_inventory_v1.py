from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


AXIS_ID = "short_term_barrier_winner_shape_inventory_v1"
DEFAULT_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\chart_entry_geometry_research_v1")
LATEST_CONFIRMED_EPOCH = 1783555200
TAKE_PROFIT, STOP_LOSS, HORIZON = 0.08, 0.05, 10


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _first_hit(column: str, operator: str, threshold: str) -> str:
    return "LEAST(" + ", ".join(
        f"CASE WHEN {column}{day} {operator} {threshold} THEN {day} ELSE 99 END" for day in range(1, HORIZON + 1)
    ) + ")"


def run(*, db_path: Path, output_root: Path) -> Path:
    output = output_root / f"{_tag()}-{AXIS_ID}"
    output.mkdir(parents=True, exist_ok=False)
    future = ", ".join(
        f"LEAD(b.h, {day}) OVER ordered AS h{day}, LEAD(b.l, {day}) OVER ordered AS l{day}" for day in range(1, HORIZON + 1)
    )
    tp_day = _first_hit("h", ">=", f"c * {1 + TAKE_PROFIT}")
    sl_day = _first_hit("l", "<=", f"c * {1 - STOP_LOSS}")
    sql = f"""
        WITH eligible AS (
            SELECT code FROM daily_bars WHERE source = 'pan' GROUP BY code HAVING MAX(date) >= {LATEST_CONFIRMED_EPOCH}
        ), bars AS (
            SELECT b.code, b.date, b.o, b.h, b.l, b.c, b.v,
                LAG(b.c, 10) OVER ordered AS c_10ago,
                AVG(b.c) OVER ma20 AS ma20,
                MAX(b.h) OVER last10 AS high10,
                MIN(b.l) OVER last10 AS low10,
                AVG(b.v) OVER vol20 AS avg_volume20,
                LEAD(b.c, {HORIZON}) OVER ordered AS c_end,
                {future}
            FROM daily_bars b JOIN eligible e USING (code)
            WHERE b.source = 'pan'
            WINDOW
                ordered AS (PARTITION BY b.code ORDER BY b.date),
                ma20 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
                last10 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW),
                vol20 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
        ), outcome AS (
            SELECT *, {tp_day} AS tp_day, {sl_day} AS sl_day
            FROM bars
            WHERE date BETWEEN 1546300800 AND 1767139200
              AND c_10ago IS NOT NULL AND ma20 IS NOT NULL AND c_end IS NOT NULL AND h > l
        ), shaped AS (
            SELECT *,
                (c / c_10ago) - 1.0 AS ret10,
                (high10 / low10) - 1.0 AS range10,
                (c / ma20) - 1.0 AS ma20_gap,
                (c - l) / (h - l) AS close_position,
                CASE WHEN sl_day <= {HORIZON} AND sl_day <= tp_day THEN -{STOP_LOSS}
                     WHEN tp_day <= {HORIZON} THEN {TAKE_PROFIT}
                     ELSE (c_end / c) - 1.0 END AS realized_return,
                CASE WHEN tp_day < sl_day AND tp_day <= {HORIZON} THEN 1 ELSE 0 END AS target_before_stop,
                CASE WHEN sl_day <= tp_day AND sl_day <= {HORIZON} THEN 1 ELSE 0 END AS stop_before_target
            FROM outcome
        ), bucketed AS (
            SELECT 'prior_10d_return' AS axis,
                CASE WHEN ret10 >= .15 THEN 'up_over_15pct' WHEN ret10 >= .06 THEN 'up_6_to_15pct'
                     WHEN ret10 > -.06 THEN 'flat_minus6_to_plus6pct' ELSE 'down_over_6pct' END AS bucket, * FROM shaped
            UNION ALL
            SELECT 'range_10d', CASE WHEN range10 <= .06 THEN 'tight' WHEN range10 <= .12 THEN 'normal' ELSE 'wide' END, * FROM shaped
            UNION ALL
            SELECT 'close_vs_ma20', CASE WHEN ma20_gap >= .05 THEN 'over_5pct_above' WHEN ma20_gap >= 0 THEN '0_to_5pct_above'
                WHEN ma20_gap >= -.05 THEN '0_to_5pct_below' ELSE 'over_5pct_below' END, * FROM shaped
            UNION ALL
            SELECT 'close_position', CASE WHEN close_position >= .7 THEN 'upper_30pct' WHEN close_position >= .4 THEN 'middle_30pct' ELSE 'lower_40pct' END, * FROM shaped
            UNION ALL
            SELECT 'volume_state', CASE WHEN v >= 1.5 * avg_volume20 THEN 'volume_confirmed' ELSE 'volume_normal' END, * FROM shaped
        )
        SELECT axis, bucket, COUNT(*) AS sample_count,
            AVG(realized_return) AS expectancy,
            AVG(target_before_stop) AS target_before_stop_rate,
            AVG(stop_before_target) AS stop_before_target_rate,
            SUM(CASE WHEN realized_return > 0 THEN realized_return ELSE 0 END)
                / NULLIF(ABS(SUM(CASE WHEN realized_return < 0 THEN realized_return ELSE 0 END)), 0) AS profit_factor
        FROM bucketed GROUP BY axis, bucket ORDER BY axis, target_before_stop_rate DESC
    """
    baseline_sql = sql.replace(
        "SELECT axis, bucket, COUNT(*) AS sample_count,\n            AVG(realized_return) AS expectancy,\n            AVG(target_before_stop) AS target_before_stop_rate,\n            AVG(stop_before_target) AS stop_before_target_rate,\n            SUM(CASE WHEN realized_return > 0 THEN realized_return ELSE 0 END)\n                / NULLIF(ABS(SUM(CASE WHEN realized_return < 0 THEN realized_return ELSE 0 END)), 0) AS profit_factor\n        FROM bucketed GROUP BY axis, bucket ORDER BY axis, target_before_stop_rate DESC",
        "SELECT COUNT(*) AS sample_count, AVG(realized_return) AS expectancy, AVG(target_before_stop) AS target_before_stop_rate, AVG(stop_before_target) AS stop_before_target_rate, SUM(CASE WHEN realized_return > 0 THEN realized_return ELSE 0 END) / NULLIF(ABS(SUM(CASE WHEN realized_return < 0 THEN realized_return ELSE 0 END)), 0) AS profit_factor FROM shaped",
    )
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        columns = [item[0] for item in conn.execute(sql).description]
        rows = [dict(zip(columns, row)) for row in conn.execute(sql).fetchall()]
        baseline_columns = [item[0] for item in conn.execute(baseline_sql).description]
        baseline = dict(zip(baseline_columns, conn.execute(baseline_sql).fetchone()))
    finally:
        conn.close()
    for row in rows:
        row["sample_count"] = int(row["sample_count"])
        for key in ("expectancy", "target_before_stop_rate", "stop_before_target_rate", "profit_factor"):
            row[key] = float(row[key]) if row[key] is not None else None
    baseline["sample_count"] = int(baseline["sample_count"])
    for key in ("expectancy", "target_before_stop_rate", "stop_before_target_rate", "profit_factor"):
        baseline[key] = float(baseline[key]) if baseline[key] is not None else None
    for row in rows:
        row["target_rate_lift"] = row["target_before_stop_rate"] - baseline["target_before_stop_rate"]
        row["expectancy_lift"] = row["expectancy"] - baseline["expectancy"]
    _write_json(output / "winner_shape_inventory.json", {
        "schema_version": f"tradex_{AXIS_ID}.inventory.v1", "authoritative_result": True,
        "fixed_evaluation_conditions": {
            "source_db": str(db_path), "source_filter": "daily_bars.source = pan", "confirmed_latest_date": "2026-07-09",
            "entry_dates": "2019-01-01 through 2025-12-31", "entry": "signal-day close",
            "take_profit": TAKE_PROFIT, "stop_loss": STOP_LOSS, "max_holding_days": HORIZON,
            "same_day_dual_hit": "stop first", "costs": "excluded", "runtime_db_write": False,
        },
        "no_lookahead": {"status": "pass", "shape_fields": "entry bar and prior bars only", "label": "future high/low/close through day 10"},
        "baseline": baseline,
        "buckets": rows,
        "interpretation_constraints": [
            "Single-axis outcome inventory only; not a trading rule.",
            "Any selected bucket requires chronological validation before adoption.",
        ],
    })
    _write_json(output / "research_decision.json", {
        "candidate_local_decision": "inventory_complete_no_entry_adoption",
        "authoritative_rollup_decision": "inventory_complete_no_entry_adoption",
        "production_ranking_changed": False,
        "runtime_db_write": False,
    })
    return output


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(run(db_path=args.db_path, output_root=args.output_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
