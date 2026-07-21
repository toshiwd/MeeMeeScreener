from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


AXIS_ID = "ma20_support_shape_selector_v1"
DEFAULT_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\chart_entry_geometry_research_v1")
LATEST_CONFIRMED_EPOCH = 1783555200
DEFAULT_TAKE_PROFIT, DEFAULT_STOP_LOSS, HORIZON = 0.08, 0.05, 10


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _first_hit(column: str, operator: str, threshold: str) -> str:
    return "LEAST(" + ", ".join(
        f"CASE WHEN {column}{day} {operator} {threshold} THEN {day} ELSE 99 END" for day in range(1, HORIZON + 1)
    ) + ")"


def _normalize(row: dict[str, Any]) -> dict[str, Any]:
    strings = {"selector", "split"}
    integers = {"sample_count", "signal_days", "trading_days", "year"}
    return {key: value if key in strings else int(value) if key in integers else float(value) if value is not None else None for key, value in row.items()}


def _path_metrics(returns: list[float]) -> dict[str, Any]:
    equity = peak = 1.0
    max_drawdown = 0.0
    streak = max_streak = 0
    for value in returns:
        equity *= 1.0 + value
        peak = max(peak, equity)
        max_drawdown = min(max_drawdown, equity / peak - 1.0)
        streak = streak + 1 if value <= 0 else 0
        max_streak = max(max_streak, streak)
    return {"max_drawdown": max_drawdown, "max_nonpositive_streak": max_streak}


def run(*, db_path: Path, output_root: Path, take_profit: float, stop_loss: float) -> Path:
    output = output_root / f"{_tag()}-{AXIS_ID}-tp{take_profit:.2f}-sl{stop_loss:.2f}"
    output.mkdir(parents=True, exist_ok=False)
    future = ", ".join(
        f"LEAD(b.h, {day}) OVER ordered AS h{day}, LEAD(b.l, {day}) OVER ordered AS l{day}" for day in range(1, HORIZON + 1)
    )
    tp_day = _first_hit("h", ">=", f"c * {1 + take_profit}")
    sl_day = _first_hit("l", "<=", f"c * {1 - stop_loss}")
    base = f"""
        WITH eligible AS (
            SELECT code FROM daily_bars WHERE source = 'pan' GROUP BY code HAVING MAX(date) >= {LATEST_CONFIRMED_EPOCH}
        ), bars AS (
            SELECT b.code, b.date, b.h, b.l, b.c, b.v,
                LAG(b.h, 1) OVER ordered AS prior_high1,
                AVG(b.c) OVER ma20 AS ma20,
                AVG(b.c) OVER ma20_lag5 AS ma20_5ago,
                AVG(b.c) OVER ma60 AS ma60,
                MAX(b.h) OVER prior10 AS prior_high10,
                AVG(b.v) OVER vol20 AS avg_volume20,
                LEAD(b.c, {HORIZON}) OVER ordered AS c_end,
                {future}
            FROM daily_bars b JOIN eligible e USING (code)
            WHERE b.source = 'pan'
            WINDOW
                ordered AS (PARTITION BY b.code ORDER BY b.date),
                ma20 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
                ma20_lag5 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 24 PRECEDING AND 5 PRECEDING),
                ma60 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW),
                prior10 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING),
                vol20 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
        ), outcomes AS (
            SELECT *, {tp_day} AS tp_day, {sl_day} AS sl_day
            FROM bars
            WHERE date BETWEEN 1546300800 AND 1767139200
              AND prior_high1 IS NOT NULL AND prior_high10 IS NOT NULL AND ma20 IS NOT NULL AND ma20_5ago IS NOT NULL AND ma60 IS NOT NULL
              AND avg_volume20 IS NOT NULL AND avg_volume20 > 0 AND c_end IS NOT NULL AND h > l
        ), candidates AS (
            SELECT *,
                CASE WHEN sl_day <= {HORIZON} AND sl_day <= tp_day THEN -{stop_loss}
                     WHEN tp_day <= {HORIZON} THEN {take_profit}
                     ELSE (c_end / c) - 1.0 END AS realized_return,
                CASE WHEN year(to_timestamp(CAST(date AS BIGINT))) <= 2021 THEN 'train'
                     WHEN year(to_timestamp(CAST(date AS BIGINT))) <= 2023 THEN 'validation' ELSE 'test' END AS split,
                CAST(year(to_timestamp(CAST(date AS BIGINT))) AS INTEGER) AS year,
                ROW_NUMBER() OVER (PARTITION BY date ORDER BY (v / avg_volume20) DESC, code) AS volume_rank,
                ROW_NUMBER() OVER (PARTITION BY date ORDER BY ((c - l) / (h - l)) DESC, code) AS upper_close_rank,
                ROW_NUMBER() OVER (PARTITION BY date ORDER BY ABS((l / ma20) - 1.0) ASC, code) AS support_precision_rank
            FROM outcomes
            WHERE c > ma60 AND ma20 > ma20_5ago AND l BETWEEN ma20 * 0.99 AND ma20
              AND c > ma20 AND c > prior_high1 AND c <= prior_high10 * 0.98
              AND (c - l) / NULLIF(h - l, 0.0) >= 0.70
        ), selected AS (
            SELECT 'all_pullback_reclaim' AS selector, * FROM candidates
            UNION ALL SELECT 'top1_volume_ratio', * FROM candidates WHERE volume_rank = 1
            UNION ALL SELECT 'top3_volume_ratio', * FROM candidates WHERE volume_rank <= 3
            UNION ALL SELECT 'top1_upper_close', * FROM candidates WHERE upper_close_rank = 1
            UNION ALL SELECT 'top3_upper_close', * FROM candidates WHERE upper_close_rank <= 3
            UNION ALL SELECT 'top1_support_precision', * FROM candidates WHERE support_precision_rank = 1
            UNION ALL SELECT 'top3_support_precision', * FROM candidates WHERE support_precision_rank <= 3
        ), baskets AS (
            SELECT selector, split, date, year, AVG(realized_return) AS basket_return, COUNT(*) AS entries
            FROM selected GROUP BY selector, split, date, year
        ), trade_metrics AS (
            SELECT selector, split, COUNT(*) AS sample_count, COUNT(DISTINCT date) AS signal_days,
                AVG(realized_return) AS expectancy,
                SUM(CASE WHEN realized_return > 0 THEN realized_return ELSE 0 END)
                    / NULLIF(ABS(SUM(CASE WHEN realized_return < 0 THEN realized_return ELSE 0 END)), 0) AS profit_factor
            FROM selected GROUP BY selector, split
        ), basket_metrics AS (
            SELECT selector, split, COUNT(*) AS trading_days, AVG(entries) AS avg_entries_per_day,
                AVG(basket_return) AS basket_expectancy,
                SUM(CASE WHEN basket_return > 0 THEN basket_return ELSE 0 END)
                    / NULLIF(ABS(SUM(CASE WHEN basket_return < 0 THEN basket_return ELSE 0 END)), 0) AS basket_profit_factor
            FROM baskets GROUP BY selector, split
        ), yearly_metrics AS (
            SELECT selector, split, year, COUNT(*) AS trading_days, AVG(basket_return) AS basket_expectancy,
                SUM(CASE WHEN basket_return > 0 THEN basket_return ELSE 0 END)
                    / NULLIF(ABS(SUM(CASE WHEN basket_return < 0 THEN basket_return ELSE 0 END)), 0) AS basket_profit_factor
            FROM baskets GROUP BY selector, split, year
        )
    """
    metrics_sql = base + "SELECT t.*, b.trading_days, b.avg_entries_per_day, b.basket_expectancy, b.basket_profit_factor FROM trade_metrics t JOIN basket_metrics b USING (selector, split) ORDER BY selector, split"
    yearly_sql = base + "SELECT * FROM yearly_metrics ORDER BY selector, year"
    path_sql = base + "SELECT selector, date, basket_return FROM baskets ORDER BY selector, date"
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        def fetch(sql: str) -> list[dict[str, Any]]:
            cols = [item[0] for item in conn.execute(sql).description]
            return [_normalize(dict(zip(cols, row))) for row in conn.execute(sql).fetchall()]
        metrics, yearly = fetch(metrics_sql), fetch(yearly_sql)
        path_rows = conn.execute(path_sql).fetchall()
    finally:
        conn.close()
    decisions = []
    path_by_selector: dict[str, list[float]] = {}
    for selector, _date, basket_return in path_rows:
        path_by_selector.setdefault(str(selector), []).append(float(basket_return))
    for selector in sorted({row["selector"] for row in metrics}):
        rows = {row["split"]: row for row in metrics if row["selector"] == selector}
        annual = [row for row in yearly if row["selector"] == selector]
        split_gate = all(
            split in rows and rows[split]["sample_count"] >= 300 and rows[split]["expectancy"] > 0
            and (rows[split]["profit_factor"] or 0) >= 1.2 and (rows[split]["basket_profit_factor"] or 0) >= 1.15
            for split in ("train", "validation", "test")
        )
        annual_gate = len(annual) == 7 and all((row["basket_profit_factor"] or 0) >= 1.0 for row in annual)
        decisions.append({"selector": selector, "decision": "candidate_for_meemee_visual_review" if split_gate and annual_gate else "drop", "split_gate": split_gate, "annual_gate": annual_gate, "annual_basket_pf_min": min((row["basket_profit_factor"] or 0) for row in annual)})
    payload = {
        "schema_version": f"tradex_{AXIS_ID}.compare.v1", "authoritative_result": True,
        "fixed_evaluation_conditions": {
            "source_db": str(db_path), "source_filter": "daily_bars.source = pan", "confirmed_latest_date": "2026-07-09",
            "shape_rule": "rising MA20, <=1% support undercut, >=2% pullback from prior 10-day high, then prior-day-high reclaim with upper-30% close",
            "selectors": "all candidates, or point-in-time top 1/3 by volume ratio, upper-close position, or MA20-touch precision",
            "entry": "signal-day close", "take_profit": take_profit, "stop_loss": stop_loss,
            "max_holding_days": HORIZON, "same_day_dual_hit": "stop first", "costs": "excluded", "runtime_db_write": False,
        },
        "metrics_by_split": metrics, "yearly_daily_basket_metrics": yearly,
        "daily_basket_risk_proxy": {selector: _path_metrics(returns) for selector, returns in path_by_selector.items()},
        "predeclared_gates": {"each_split": "sample >=300, trade PF >=1.2, basket PF >=1.15, positive expectancy", "annual": "all 7 annual basket PF >=1.0"},
        "candidate_decisions": decisions,
    }
    (output / "compare.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (output / "research_decision.json").write_text(json.dumps({"candidate_local_decision": "see_candidate_decisions", "authoritative_rollup_decision": "see_candidate_decisions", "production_ranking_changed": False, "runtime_db_write": False}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return output


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--take-profit", type=float, default=DEFAULT_TAKE_PROFIT)
    parser.add_argument("--stop-loss", type=float, default=DEFAULT_STOP_LOSS)
    args = parser.parse_args()
    print(run(db_path=args.db_path, output_root=args.output_root, take_profit=args.take_profit, stop_loss=args.stop_loss))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
