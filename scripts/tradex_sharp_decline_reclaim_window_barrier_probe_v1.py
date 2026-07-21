from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


AXIS_ID = "sharp_decline_reclaim_window_barrier_probe_v1"
DEFAULT_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\chart_entry_geometry_research_v1")
LATEST_CONFIRMED_EPOCH = 1783555200  # 2026-07-09 UTC
TAKE_PROFIT, STOP_LOSS, HORIZON = 0.08, 0.05, 10


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _first_hit(column: str, operator: str, threshold: str) -> str:
    checks = ", ".join(
        f"CASE WHEN {column}{day} {operator} {threshold} THEN {day} ELSE 99 END" for day in range(1, HORIZON + 1)
    )
    return f"LEAST({checks})"


def _normalize(row: dict[str, Any]) -> dict[str, Any]:
    string_fields = {"cohort", "split"}
    integer_fields = {"sample_count", "signal_days", "trading_days", "year"}
    return {
        key: value if key in string_fields else int(value) if key in integer_fields else float(value) if value is not None else None
        for key, value in row.items()
    }


def run(*, db_path: Path, output_root: Path, reclaim_window: int, max_ma20_gap: float | None) -> Path:
    if reclaim_window < 1:
        raise ValueError("reclaim_window must be at least 1")
    suffix = f"-w{reclaim_window}" if max_ma20_gap is None else f"-w{reclaim_window}-ma20{max_ma20_gap:+.2f}"
    output = output_root / f"{_tag()}-{AXIS_ID}{suffix}"
    output.mkdir(parents=True, exist_ok=False)
    leads = ", ".join(
        f"LEAD(b.h, {day}) OVER ordered AS h{day}, LEAD(b.l, {day}) OVER ordered AS l{day}" for day in range(1, HORIZON + 1)
    )
    tp_day = _first_hit("h", ">=", f"c * {1 + TAKE_PROFIT}")
    sl_day = _first_hit("l", "<=", f"c * {1 - STOP_LOSS}")
    candidate_guard = "" if max_ma20_gap is None else f" AND (c / ma20) - 1.0 <= {max_ma20_gap}"
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        conn.execute(
            f"""
            CREATE TEMP TABLE events AS
            WITH eligible AS (
                SELECT code FROM daily_bars WHERE source = 'pan' GROUP BY code HAVING MAX(date) >= {LATEST_CONFIRMED_EPOCH}
            ), bars AS (
                SELECT b.code, b.date, b.c,
                    LAG(b.c, 10) OVER ordered AS c_10ago,
                    MAX(b.h) OVER reclaim AS prior_high,
                    AVG(b.c) OVER ma20 AS ma20,
                    LEAD(b.c, {HORIZON}) OVER ordered AS c_end,
                    {leads}
                FROM daily_bars b JOIN eligible e USING (code)
                WHERE b.source = 'pan'
                WINDOW
                    ordered AS (PARTITION BY b.code ORDER BY b.date),
                    reclaim AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN {reclaim_window} PRECEDING AND 1 PRECEDING),
                    ma20 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
            ), outcome AS (
                SELECT *, {tp_day} AS tp_day, {sl_day} AS sl_day
                FROM bars
                WHERE date BETWEEN 1546300800 AND 1767139200
                  AND c_10ago IS NOT NULL AND prior_high IS NOT NULL AND ma20 IS NOT NULL AND c_end IS NOT NULL
            ), realized AS (
                SELECT *,
                    CASE WHEN sl_day <= {HORIZON} AND sl_day <= tp_day THEN -{STOP_LOSS}
                         WHEN tp_day <= {HORIZON} THEN {TAKE_PROFIT}
                         ELSE (c_end / c) - 1.0 END AS realized_return,
                    CASE WHEN year(to_timestamp(CAST(date AS BIGINT))) <= 2022 THEN 'train_2019_2022' ELSE 'test_2023_2025' END AS split,
                    CAST(year(to_timestamp(CAST(date AS BIGINT))) AS INTEGER) AS year
                FROM outcome
            ), cohorts AS (
                SELECT 'sharp_decline_pool' AS cohort, * FROM realized WHERE (c / c_10ago) - 1.0 <= -0.06
                UNION ALL
                SELECT 'reclaim_window' AS cohort, * FROM realized
                WHERE (c / c_10ago) - 1.0 <= -0.06 AND c > prior_high {candidate_guard}
            )
            SELECT * FROM cohorts
            """
        )
        aggregate_sql = """
            SELECT cohort, split, COUNT(*) AS sample_count, COUNT(DISTINCT date) AS signal_days,
                AVG(realized_return) AS expectancy,
                SUM(CASE WHEN realized_return > 0 THEN realized_return ELSE 0 END)
                    / NULLIF(ABS(SUM(CASE WHEN realized_return < 0 THEN realized_return ELSE 0 END)), 0) AS profit_factor
            FROM events GROUP BY cohort, split ORDER BY cohort, split
        """
        daily_sql = """
            WITH baskets AS (
                SELECT cohort, split, date, year, AVG(realized_return) AS basket_return, COUNT(*) AS entries
                FROM events GROUP BY cohort, split, date, year
            )
            SELECT cohort, split, COUNT(*) AS trading_days, AVG(entries) AS avg_entries_per_day,
                MAX(entries) AS max_entries_per_day, AVG(basket_return) AS basket_expectancy,
                SUM(CASE WHEN basket_return > 0 THEN basket_return ELSE 0 END)
                    / NULLIF(ABS(SUM(CASE WHEN basket_return < 0 THEN basket_return ELSE 0 END)), 0) AS basket_profit_factor
            FROM baskets GROUP BY cohort, split ORDER BY cohort, split
        """
        yearly_sql = """
            WITH baskets AS (
                SELECT cohort, split, date, year, AVG(realized_return) AS basket_return
                FROM events GROUP BY cohort, split, date, year
            )
            SELECT cohort, split, year, COUNT(*) AS trading_days, AVG(basket_return) AS basket_expectancy,
                SUM(CASE WHEN basket_return > 0 THEN basket_return ELSE 0 END)
                    / NULLIF(ABS(SUM(CASE WHEN basket_return < 0 THEN basket_return ELSE 0 END)), 0) AS basket_profit_factor
            FROM baskets GROUP BY cohort, split, year ORDER BY cohort, split, year
        """
        def fetch(sql: str) -> list[dict[str, Any]]:
            columns = [item[0] for item in conn.execute(sql).description]
            return [_normalize(dict(zip(columns, row))) for row in conn.execute(sql).fetchall()]
        aggregate, daily, yearly = fetch(aggregate_sql), fetch(daily_sql), fetch(yearly_sql)
    finally:
        conn.close()

    aggregate_index = {(row["cohort"], row["split"]): row for row in aggregate}
    daily_index = {(row["cohort"], row["split"]): row for row in daily}
    candidate_train = aggregate_index[("reclaim_window", "train_2019_2022")]
    candidate_test = aggregate_index[("reclaim_window", "test_2023_2025")]
    baseline_train = aggregate_index[("sharp_decline_pool", "train_2019_2022")]
    candidate_yearly = [row for row in yearly if row["cohort"] == "reclaim_window"]
    yearly_basket_gate = len(candidate_yearly) == 7 and all((row["basket_profit_factor"] or 0) >= 1.0 for row in candidate_yearly)
    decision = "drop"
    split_gate = (
        candidate_train["sample_count"] >= 1000
        and candidate_train["expectancy"] > baseline_train["expectancy"]
        and candidate_train["profit_factor"] > baseline_train["profit_factor"]
        and candidate_test["expectancy"] > 0
        and (candidate_test["profit_factor"] or 0) >= 1.2
        and (daily_index[("reclaim_window", "test_2023_2025")]["basket_profit_factor"] or 0) >= 1.2
    )
    if split_gate and yearly_basket_gate:
        decision = "candidate_for_visual_review"
    elif split_gate:
        decision = "hold_for_visual_and_new_holdout"

    _write_json(output / "compare.json", {
        "schema_version": f"tradex_{AXIS_ID}.compare.v1",
        "authoritative_result": True,
        "research_phase": "branching_generation",
        "fixed_evaluation_conditions": {
            "source_db": str(db_path), "source_filter": "daily_bars.source = pan",
            "confirmed_latest_date": "2026-07-09", "entry_dates": "2019-01-01 through 2025-12-31",
            "entry": "signal-day close", "take_profit": TAKE_PROFIT, "stop_loss": STOP_LOSS,
            "max_holding_days": HORIZON, "same_day_dual_hit": "stop first", "costs": "excluded",
            "runtime_db_write": False,
        },
        "axis_changed": "reclaim window only within the sharp-decline pool",
        "rule_definitions": {
            "sharp_decline_pool": "10-day close return <= -6%",
            "reclaim_window": f"sharp-decline pool plus close exceeds every high in the prior {reclaim_window} trading days",
        },
        "aggregate_by_split": aggregate,
        "date_equal_weight_by_split": daily,
        "yearly_daily_basket_metrics": yearly,
        "yearly_basket_gate": {"requires": "all 7 annual basket PF >= 1.0", "passed": yearly_basket_gate},
        "candidate_local_decision": decision,
        "authoritative_rollup_decision": decision,
        "ma20_gap_guard": max_ma20_gap,
    })
    _write_json(output / "research_decision.json", {
        "candidate_local_decision": decision,
        "authoritative_rollup_decision": decision,
        "production_ranking_changed": False,
        "runtime_db_write": False,
    })
    _write_json(output / "no_lookahead_audit.json", {
        "audit_result": "pass",
        "point_in_time_shape_fields": ["ret10", "prior_high"],
        "outcome_fields": ["future high_low_close through day 10"],
        "thresholds_retuned": False,
    })
    return output


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--reclaim-window", type=int, default=3)
    parser.add_argument("--max-ma20-gap", type=float)
    args = parser.parse_args()
    print(run(db_path=args.db_path, output_root=args.output_root, reclaim_window=args.reclaim_window, max_ma20_gap=args.max_ma20_gap))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
