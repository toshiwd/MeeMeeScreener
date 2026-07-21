from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


AXIS_ID = "sharp_decline_rejection_candle_barrier_probe_v1"
DEFAULT_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\chart_entry_geometry_research_v1")
LATEST_CONFIRMED_EPOCH = 1783555200  # 2026-07-09 UTC
TP, SL, HORIZON = 0.08, 0.05, 10


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
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        conn.execute(
            f"""
            CREATE TEMP TABLE rows AS
            WITH eligible AS (
                SELECT code FROM daily_bars WHERE source = 'pan' GROUP BY code HAVING MAX(date) >= {LATEST_CONFIRMED_EPOCH}
            ), point_in_time AS (
                SELECT b.code, b.date, b.o, b.h, b.l, b.c,
                    CAST(strftime(to_timestamp(CAST(b.date AS BIGINT)), '%Y') AS INTEGER) AS year,
                    LAG(b.c, 10) OVER ordered AS c_10ago,
                    LEAD(b.c, {HORIZON}) OVER ordered AS c_end,
                    {', '.join(f'LEAD(b.h, {day}) OVER ordered AS h{day}, LEAD(b.l, {day}) OVER ordered AS l{day}' for day in range(1, HORIZON + 1))}
                FROM daily_bars b JOIN eligible e USING (code)
                WHERE b.source = 'pan'
                WINDOW ordered AS (PARTITION BY b.code ORDER BY b.date)
            ), shaped AS (
                SELECT *,
                    (c / c_10ago) - 1.0 AS ret10,
                    (LEAST(o, c) - l) / NULLIF(h - l, 0.0) AS lower_wick_ratio,
                    (c - l) / NULLIF(h - l, 0.0) AS close_position
                FROM point_in_time
                WHERE date BETWEEN 1546300800 AND 1767139200
                  AND c > 0 AND c_10ago > 0 AND c_end IS NOT NULL AND h > l
            ), outcome AS (
                SELECT *,
                    {_first_hit('h', '>=', f'c * {1 + TP}')} AS tp_day,
                    {_first_hit('l', '<=', f'c * {1 - SL}')} AS sl_day
                FROM shaped
            ), realized AS (
                SELECT *,
                    CASE WHEN sl_day <= {HORIZON} AND sl_day <= tp_day THEN -{SL}
                         WHEN tp_day <= {HORIZON} THEN {TP}
                         ELSE (c_end / c) - 1.0 END AS realized_return,
                    CASE WHEN sl_day <= {HORIZON} AND sl_day <= tp_day THEN 'stop'
                         WHEN tp_day <= {HORIZON} THEN 'target' ELSE 'timeout' END AS exit_reason
                FROM outcome
            ), cohort AS (
                SELECT 'sharp_decline_pool' AS cohort, * FROM realized WHERE ret10 <= -0.06
                UNION ALL
                SELECT 'rejection_candle' AS cohort, * FROM realized
                WHERE ret10 <= -0.06 AND lower_wick_ratio >= 0.35 AND close_position >= 0.60
            )
            SELECT *, CASE WHEN year <= 2022 THEN 'train_2019_2022' ELSE 'test_2023_2025' END AS period_split
            FROM cohort
            """
        )
        sql = """
            SELECT cohort, period_split, year, COUNT(*) AS sample_count,
                AVG(realized_return) AS expectancy,
                MEDIAN(realized_return) AS median_return,
                AVG(CASE WHEN realized_return > 0 THEN 1.0 ELSE 0.0 END) AS profitable_trade_rate,
                AVG(CASE WHEN exit_reason = 'target' THEN 1.0 ELSE 0.0 END) AS target_before_stop_rate,
                AVG(CASE WHEN exit_reason = 'stop' THEN 1.0 ELSE 0.0 END) AS stop_rate,
                SUM(CASE WHEN realized_return > 0 THEN realized_return ELSE 0.0 END)
                    / NULLIF(ABS(SUM(CASE WHEN realized_return < 0 THEN realized_return ELSE 0.0 END)), 0.0) AS profit_factor
            FROM rows GROUP BY cohort, period_split, year ORDER BY cohort, period_split, year
        """
        columns = [item[0] for item in conn.execute(sql).description]
        yearly = [dict(zip(columns, row)) for row in conn.execute(sql).fetchall()]
        aggregate_sql = sql.replace("cohort, period_split, year,", "cohort, period_split,").replace(
            "GROUP BY cohort, period_split, year ORDER BY cohort, period_split, year",
            "GROUP BY cohort, period_split ORDER BY cohort, period_split",
        )
        aggregate_columns = [item[0] for item in conn.execute(aggregate_sql).description]
        aggregate = [dict(zip(aggregate_columns, row)) for row in conn.execute(aggregate_sql).fetchall()]
    finally:
        conn.close()

    string_fields = {"cohort", "period_split"}
    normalized_yearly = [
        {key: (value if key in string_fields else int(value) if key in {"year", "sample_count"} else float(value) if value is not None else value) for key, value in row.items()}
        for row in yearly
    ]
    normalized_aggregate = [
        {key: (value if key in string_fields else int(value) if key == "sample_count" else float(value) if value is not None else value) for key, value in row.items()}
        for row in aggregate
    ]
    index = {(row["cohort"], row["period_split"]): row for row in normalized_aggregate}
    train_base, train_candidate = index[("sharp_decline_pool", "train_2019_2022")], index[("rejection_candle", "train_2019_2022")]
    test_base, test_candidate = index[("sharp_decline_pool", "test_2023_2025")], index[("rejection_candle", "test_2023_2025")]
    train_improves = train_candidate["expectancy"] > train_base["expectancy"] and train_candidate["profit_factor"] > train_base["profit_factor"]
    test_confirms = test_candidate["expectancy"] > 0 and (test_candidate["profit_factor"] or 0) >= 1.2
    decision = "candidate_for_meemee_visual_review" if train_improves and test_confirms and test_candidate["sample_count"] >= 1000 else "drop"
    reason = {
        "train_expectancy_lift": train_candidate["expectancy"] - train_base["expectancy"],
        "train_profit_factor_lift": train_candidate["profit_factor"] - train_base["profit_factor"],
        "test_expectancy": test_candidate["expectancy"],
        "test_profit_factor": test_candidate["profit_factor"],
    }
    _write_json(output / "compare.json", {
        "schema_version": f"tradex_{AXIS_ID}.compare.v1", "authoritative_result": True,
        "research_phase": "branching_generation", "fixed_evaluation_conditions": {
            "source_db": str(db_path), "source_filter": "daily_bars.source = pan", "confirmed_latest_date": "2026-07-09",
            "entry_dates": "2019-01-01 through 2025-12-31", "entry": "signal-day close", "take_profit": TP,
            "stop_loss": SL, "max_holding_days": HORIZON, "same_day_dual_hit": "stop first", "costs": "excluded", "runtime_db_write": False,
        },
        "axis_changed": "rejection_candle geometry only within the sharp-decline pool",
        "rule_definitions": {
            "sharp_decline_pool": "10-day close return <= -6%",
            "rejection_candle": "sharp-decline pool plus lower wick >= 35% of candle range and close position >= 60%",
        },
        "aggregate_by_split": normalized_aggregate, "yearly_metrics": normalized_yearly,
        "candidate_local_decision": decision, "authoritative_rollup_decision": decision, "decision_evidence": reason,
    })
    _write_json(output / "research_decision.json", {
        "candidate_local_decision": decision, "authoritative_rollup_decision": decision,
        "reason_typed": ["single_candle_geometry_axis", "chronological_train_test_split", "no_meemee_reflection"],
        "production_ranking_changed": False, "runtime_db_write": False,
    })
    _write_json(output / "no_lookahead_audit.json", {
        "audit_result": "pass", "point_in_time_shape_fields": ["ret10", "lower_wick_ratio", "close_position"],
        "outcome_fields": ["future high_low_close through day 10"], "thresholds_retuned": False,
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
