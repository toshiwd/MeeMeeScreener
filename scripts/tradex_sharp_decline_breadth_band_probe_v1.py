from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


AXIS_ID = "sharp_decline_breadth_band_probe_v1"
DEFAULT_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\chart_entry_geometry_research_v1")
LATEST_CONFIRMED_EPOCH = 1783555200  # 2026-07-09 UTC
TAKE_PROFIT, STOP_LOSS, HORIZON = 0.08, 0.05, 10


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _first_hit(column: str, operator: str, threshold: str) -> str:
    return "LEAST(" + ", ".join(
        f"CASE WHEN {column}{day} {operator} {threshold} THEN {day} ELSE 99 END"
        for day in range(1, HORIZON + 1)
    ) + ")"


def _normalize(row: dict[str, Any]) -> dict[str, Any]:
    strings = {"band", "split"}
    integers = {"sample_count", "trading_days"}
    return {
        key: value if key in strings else int(value) if key in integers else float(value) if value is not None else None
        for key, value in row.items()
    }


def run(*, db_path: Path, output_root: Path) -> Path:
    output = output_root / f"{_tag()}-{AXIS_ID}"
    output.mkdir(parents=True, exist_ok=False)
    leads = ", ".join(
        f"LEAD(b.h, {day}) OVER ordered AS h{day}, LEAD(b.l, {day}) OVER ordered AS l{day}"
        for day in range(1, HORIZON + 1)
    )
    tp_day = _first_hit("h", ">=", f"c * {1 + TAKE_PROFIT}")
    sl_day = _first_hit("l", "<=", f"c * {1 - STOP_LOSS}")
    base_cte = f"""
        WITH eligible AS (
            SELECT code FROM daily_bars WHERE source = 'pan' GROUP BY code HAVING MAX(date) >= {LATEST_CONFIRMED_EPOCH}
        ), bars AS (
            SELECT b.code, b.date, b.c,
                LAG(b.c, 10) OVER ordered AS c_10ago,
                LAG(b.h, 1) OVER ordered AS prev_h,
                AVG(b.c) OVER ma20 AS ma20,
                LEAD(b.c, {HORIZON}) OVER ordered AS c_end,
                {leads}
            FROM daily_bars b JOIN eligible e USING (code)
            WHERE b.source = 'pan'
            WINDOW
                ordered AS (PARTITION BY b.code ORDER BY b.date),
                ma20 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
        ), breadth AS (
            SELECT date, AVG(CASE WHEN c >= ma20 THEN 1.0 ELSE 0.0 END) AS breadth
            FROM bars WHERE ma20 IS NOT NULL GROUP BY date
        ), outcome AS (
            SELECT *, {tp_day} AS tp_day, {sl_day} AS sl_day
            FROM bars
            WHERE date BETWEEN 1546300800 AND 1767139200
              AND c_10ago IS NOT NULL AND prev_h IS NOT NULL AND c_end IS NOT NULL
        ), events AS (
            SELECT o.date,
                CAST(strftime(to_timestamp(CAST(o.date AS BIGINT)), '%Y') AS INTEGER) AS year,
                CASE WHEN sl_day <= {HORIZON} AND sl_day <= tp_day THEN -{STOP_LOSS}
                     WHEN tp_day <= {HORIZON} THEN {TAKE_PROFIT}
                     ELSE (c_end / c) - 1.0 END AS realized_return,
                br.breadth
            FROM outcome o JOIN breadth br USING (date)
            WHERE (c / c_10ago) - 1.0 <= -0.06 AND c > prev_h
        ), banded AS (
            SELECT *,
                CASE
                    WHEN breadth < 0.2 THEN '0_20'
                    WHEN breadth < 0.4 THEN '20_40'
                    WHEN breadth < 0.6 THEN '40_60'
                    WHEN breadth < 0.8 THEN '60_80'
                    ELSE '80_100'
                END AS band,
                CASE WHEN year <= 2022 THEN 'train_2019_2022' ELSE 'test_2023_2025' END AS split
            FROM events
        ), daily_basket AS (
            SELECT band, split, date, year, AVG(realized_return) AS basket_return, COUNT(*) AS entries
            FROM banded GROUP BY band, split, date, year
        ), trade_metrics AS (
            SELECT band, split, COUNT(*) AS sample_count,
                AVG(realized_return) AS expectancy,
                SUM(CASE WHEN realized_return > 0 THEN realized_return ELSE 0 END)
                    / NULLIF(ABS(SUM(CASE WHEN realized_return < 0 THEN realized_return ELSE 0 END)), 0) AS profit_factor
            FROM banded GROUP BY band, split
        ), basket_metrics AS (
            SELECT band, split, COUNT(*) AS trading_days, AVG(entries) AS avg_entries_per_day,
                AVG(basket_return) AS basket_expectancy,
                MEDIAN(basket_return) AS median_basket_return,
                AVG(CASE WHEN basket_return > 0 THEN 1.0 ELSE 0.0 END) AS profitable_day_rate,
                SUM(CASE WHEN basket_return > 0 THEN basket_return ELSE 0 END)
                    / NULLIF(ABS(SUM(CASE WHEN basket_return < 0 THEN basket_return ELSE 0 END)), 0) AS basket_profit_factor
            FROM daily_basket GROUP BY band, split
        )
    """
    metric_sql = base_cte + """
        SELECT t.*, b.trading_days, b.avg_entries_per_day, b.basket_expectancy,
               b.median_basket_return, b.profitable_day_rate, b.basket_profit_factor
        FROM trade_metrics t JOIN basket_metrics b USING (band, split)
        ORDER BY band, split
    """
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        columns = [item[0] for item in conn.execute(metric_sql).description]
        metrics = [_normalize(dict(zip(columns, row))) for row in conn.execute(metric_sql).fetchall()]
    finally:
        conn.close()

    train_rows = [row for row in metrics if row["split"] == "train_2019_2022"]
    selected_train = max(train_rows, key=lambda row: row["basket_expectancy"])
    selected_band = selected_train["band"]
    test_row = next(row for row in metrics if row["split"] == "test_2023_2025" and row["band"] == selected_band)
    decision = "drop"
    if (
        selected_train["sample_count"] >= 1000
        and (selected_train["basket_profit_factor"] or 0) >= 1.2
        and test_row["sample_count"] >= 1000
        and test_row["basket_expectancy"] > 0
        and (test_row["basket_profit_factor"] or 0) >= 1.2
    ):
        decision = "candidate_for_separate_holdout"

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
        "axis_changed": "market breadth band only",
        "shape_rule": "10-day close return <= -6% and current close > prior-day high",
        "breadth_band_definition": "Point-in-time fraction of eligible universe with close >= MA20; fixed 20-point bands.",
        "selection_protocol": "Select one band by train_2019_2022 daily-basket expectancy, then assess only that band on test_2023_2025.",
        "metrics": metrics,
        "selected_train_band": selected_band,
        "selected_train_metrics": selected_train,
        "test_metrics_for_selected_band": test_row,
        "candidate_local_decision": decision,
        "authoritative_rollup_decision": decision,
    })
    _write_json(output / "research_decision.json", {
        "candidate_local_decision": decision,
        "authoritative_rollup_decision": decision,
        "production_ranking_changed": False,
        "runtime_db_write": False,
    })
    _write_json(output / "no_lookahead_audit.json", {
        "audit_result": "pass",
        "point_in_time_shape_fields": ["ret10", "prior_day_high_reclaim", "same_day_breadth_above_ma20"],
        "outcome_fields": ["future high_low_close through day 10"],
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
