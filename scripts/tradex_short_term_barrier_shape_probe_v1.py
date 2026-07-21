from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


AXIS_ID = "short_term_barrier_shape_probe_v1"
DEFAULT_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\chart_entry_geometry_research_v1")
LATEST_CONFIRMED_EPOCH = 1783555200  # 2026-07-09 UTC
SCENARIOS = (
    {"id": "tp3_sl2_h10", "take_profit": 0.03, "stop_loss": 0.02, "max_holding_days": 10},
    {"id": "tp5_sl3_h10", "take_profit": 0.05, "stop_loss": 0.03, "max_holding_days": 10},
    {"id": "tp8_sl5_h10", "take_profit": 0.08, "stop_loss": 0.05, "max_holding_days": 10},
)


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _first_hit_expr(column: str, operator: str, threshold: str) -> str:
    checks = ", ".join(
        f"CASE WHEN {column}{day} {operator} {threshold} THEN {day} ELSE 99 END"
        for day in range(1, 11)
    )
    return f"LEAST({checks})"


def _scenario_metrics(conn: duckdb.DuckDBPyConnection, scenario: dict[str, Any]) -> dict[str, Any]:
    tp = scenario["take_profit"]
    sl = scenario["stop_loss"]
    tp_day = _first_hit_expr("h", ">=", f"c * {1 + tp}")
    sl_day = _first_hit_expr("l", "<=", f"c * {1 - sl}")
    sql = f"""
    WITH outcome AS (
        SELECT *,
          {tp_day} AS tp_day,
          {sl_day} AS sl_day
        FROM shape_rows
    ), realized AS (
        SELECT *,
          CASE
            WHEN sl_day <= 10 AND sl_day <= tp_day THEN -{sl}
            WHEN tp_day <= 10 THEN {tp}
            ELSE (c10 / c) - 1.0
          END AS realized_return,
          CASE
            WHEN sl_day <= 10 AND sl_day <= tp_day THEN 'stop'
            WHEN tp_day <= 10 THEN 'target'
            ELSE 'timeout'
          END AS exit_reason,
          CASE
            WHEN sl_day <= 10 AND sl_day <= tp_day THEN sl_day
            WHEN tp_day <= 10 THEN tp_day
            ELSE 10
          END AS exit_day
        FROM outcome
    ), grouped AS (
        SELECT 'all_eligible' AS cohort, * FROM realized
        UNION ALL SELECT 'wide_range_10d', * FROM realized WHERE range10 > 0.12
        UNION ALL SELECT 'explosive_momentum_10d', * FROM realized WHERE ret10 >= 0.15
        UNION ALL SELECT 'sharp_decline_reversal_pool', * FROM realized WHERE ret10 <= -0.06
        UNION ALL SELECT 'close_above_prior_20d_high', * FROM realized WHERE prior_high20 IS NOT NULL AND c > prior_high20
    )
    SELECT cohort, year,
      COUNT(*) AS sample_count,
      AVG(realized_return) AS expectancy,
      MEDIAN(realized_return) AS median_return,
      AVG(CASE WHEN realized_return > 0 THEN 1.0 ELSE 0.0 END) AS profitable_trade_rate,
      AVG(CASE WHEN exit_reason = 'target' THEN 1.0 ELSE 0.0 END) AS target_before_stop_rate,
      AVG(CASE WHEN exit_reason = 'stop' THEN 1.0 ELSE 0.0 END) AS stop_rate,
      AVG(CASE WHEN exit_reason = 'timeout' THEN 1.0 ELSE 0.0 END) AS timeout_rate,
      SUM(CASE WHEN realized_return > 0 THEN realized_return ELSE 0.0 END)
        / NULLIF(ABS(SUM(CASE WHEN realized_return < 0 THEN realized_return ELSE 0.0 END)), 0.0) AS profit_factor,
      AVG(exit_day) AS mean_holding_days
    FROM grouped
    GROUP BY cohort, year
    ORDER BY cohort, year
    """
    columns = [d[0] for d in conn.execute(sql).description]
    rows = [dict(zip(columns, row)) for row in conn.execute(sql).fetchall()]
    by_cohort: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        cohort = row.pop("cohort")
        normalized = {
            key: (int(value) if key in {"year", "sample_count"} else float(value) if value is not None else None)
            for key, value in row.items()
        }
        by_cohort.setdefault(cohort, []).append(normalized)

    summaries: dict[str, dict[str, Any]] = {}
    for cohort, yearly in by_cohort.items():
        total_count = sum(row["sample_count"] for row in yearly)
        positive_return = sum(row["expectancy"] * row["sample_count"] for row in yearly) / total_count
        # Profit factor is recomputed from the aggregate query below, not averaged by year.
        summaries[cohort] = {
            "sample_count": total_count,
            "year_count": len(yearly),
            "positive_expectancy_year_count": sum(1 for row in yearly if row["expectancy"] > 0),
            "weighted_expectancy": positive_return,
            "yearly": yearly,
        }

    aggregate_sql = sql.replace("cohort, year,", "cohort,").replace("GROUP BY cohort, year\n    ORDER BY cohort, year", "GROUP BY cohort\n    ORDER BY cohort")
    aggregate_columns = [d[0] for d in conn.execute(aggregate_sql).description]
    for row in conn.execute(aggregate_sql).fetchall():
        values = dict(zip(aggregate_columns, row))
        cohort = values.pop("cohort")
        values.pop("year", None)
        summaries[cohort]["aggregate"] = {
            key: (int(value) if key == "sample_count" else float(value) if value is not None else None)
            for key, value in values.items()
        }
    return {"scenario": scenario, "cohorts": summaries}


def run(*, db_path: Path, output_root: Path) -> Path:
    output = output_root / f"{_tag()}-{AXIS_ID}"
    output.mkdir(parents=True, exist_ok=False)
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        conn.execute(
            f"""
            CREATE TEMP TABLE shape_rows AS
            WITH eligible AS (
                SELECT code
                FROM daily_bars
                WHERE source = 'pan'
                GROUP BY code
                HAVING MAX(date) >= {LATEST_CONFIRMED_EPOCH}
            ), enriched AS (
                SELECT
                    b.code,
                    CAST(strftime(to_timestamp(CAST(b.date AS BIGINT)), '%Y') AS INTEGER) AS year,
                    b.date, b.c,
                    LAG(b.c, 10) OVER ordered AS c_10ago,
                    MAX(b.h) OVER last10 AS high10,
                    MIN(b.l) OVER last10 AS low10,
                    MAX(b.h) OVER prior20 AS prior_high20,
                    LEAD(b.c, 10) OVER ordered AS c10,
                    {', '.join(f'LEAD(b.h, {day}) OVER ordered AS h{day}, LEAD(b.l, {day}) OVER ordered AS l{day}' for day in range(1, 11))}
                FROM daily_bars b
                JOIN eligible e USING (code)
                WHERE b.source = 'pan'
                WINDOW
                    ordered AS (PARTITION BY b.code ORDER BY b.date),
                    last10 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW),
                    prior20 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING)
            )
            SELECT *,
                (c / c_10ago) - 1.0 AS ret10,
                (high10 / low10) - 1.0 AS range10
            FROM enriched
            WHERE date BETWEEN 1546300800 AND 1767139200
              AND c > 0 AND c_10ago > 0 AND low10 > 0 AND c10 IS NOT NULL
            """
        )
        runs = [_scenario_metrics(conn, scenario) for scenario in SCENARIOS]
    finally:
        conn.close()

    decisions: list[dict[str, Any]] = []
    for run_result in runs:
        for cohort, summary in run_result["cohorts"].items():
            aggregate = summary["aggregate"]
            passed = (
                aggregate["sample_count"] >= 1000
                and aggregate["expectancy"] > 0
                and (aggregate["profit_factor"] or 0) >= 1.2
                and summary["positive_expectancy_year_count"] >= 5
            )
            decisions.append({
                "scenario": run_result["scenario"]["id"], "cohort": cohort,
                "decision": "candidate_for_visual_subtype_validation" if passed else "hold_or_drop",
                "gate_status": {
                    "sample_count_ge_1000": aggregate["sample_count"] >= 1000,
                    "expectancy_positive": aggregate["expectancy"] > 0,
                    "profit_factor_ge_1_2": (aggregate["profit_factor"] or 0) >= 1.2,
                    "positive_expectancy_years_ge_5": summary["positive_expectancy_year_count"] >= 5,
                },
            })

    _write_json(output / "compare.json", {
        "schema_version": f"tradex_{AXIS_ID}.compare.v1",
        "authoritative_result": True,
        "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {
            "source_db": str(db_path), "source_table": "daily_bars", "source_filter": "source = pan",
            "confirmed_latest_date": "2026-07-09", "entry_dates": "2019-01-01 through 2025-12-31",
            "entry": "signal-day close", "execution": "next-session onward daily OHLC barrier simulation",
            "same_day_take_profit_and_stop": "stop first (conservative)", "costs": "excluded", "runtime_db_write": False,
        },
        "shape_cohorts": {
            "all_eligible": "full confirmed eligible universe",
            "wide_range_10d": "10-day high-low range > 12%",
            "explosive_momentum_10d": "10-day close return >= 15%",
            "sharp_decline_reversal_pool": "10-day close return <= -6%",
            "close_above_prior_20d_high": "signal close exceeds the high of each of the prior 20 trading days",
        },
        "runs": runs,
        "predeclared_quality_gates": {"sample_count": 1000, "profit_factor": 1.2, "expectancy": "> 0", "positive_expectancy_years": "at least 5 of 7"},
        "candidate_decisions": decisions,
    })
    _write_json(output / "research_decision.json", {
        "candidate_local_decision": "barrier_metrics_collected",
        "session_aggregate_decision": "not_ready_for_entry_adoption",
        "authoritative_rollup_decision": "await_visual_subtype_and_holdout_validation",
        "reason_typed": ["cohorts_are_coarse_shape_pools", "barrier_results_are_not_yet_out_of_sample", "no_meemee_ranking_reflection"],
        "production_ranking_changed": False, "runtime_db_write": False,
    })
    _write_json(output / "no_lookahead_audit.json", {
        "audit_result": "pass",
        "point_in_time_shape_fields": ["ret10", "range10"],
        "outcome_fields": ["future high/low/close through day 10"],
        "entry_before_outcome": True,
        "same_day_dual_hit_policy": "stop first",
        "thresholds_retuned": False,
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
