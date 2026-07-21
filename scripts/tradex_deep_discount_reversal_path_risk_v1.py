from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


AXIS_ID = "deep_discount_reversal_path_risk_v1"
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


def _path_metrics(returns: list[float]) -> dict[str, Any]:
    equity = 1.0
    peak = 1.0
    max_drawdown = 0.0
    streak = 0
    max_streak = 0
    for value in returns:
        equity *= 1.0 + value
        peak = max(peak, equity)
        max_drawdown = min(max_drawdown, equity / peak - 1.0)
        streak = streak + 1 if value <= 0 else 0
        max_streak = max(max_streak, streak)
    return {"sequence_equity_multiple": equity, "max_drawdown": max_drawdown, "max_nonpositive_streak": max_streak}


def _profit_factor(returns: list[float]) -> float | None:
    positive = sum(value for value in returns if value > 0)
    negative = sum(value for value in returns if value < 0)
    return positive / abs(negative) if negative else None


def run(*, db_path: Path, output_root: Path, scenario: str, take_profit: float, stop_loss: float) -> Path:
    rules = {
        "deep_discount_reversal": {
            "where": "(c / c_10ago) - 1.0 <= -0.06 AND c > prior_high1 AND (c / ma20) - 1.0 <= -0.05",
            "label": "ret10 <= -6%, prior-day-high reclaim, close >=5% below MA20",
        },
        "ma20_support_reclaim_in_long_trend": {
            "where": "c > ma60 AND l <= ma20 AND c > ma20 AND c > prior_high1",
            "label": "above MA60, intraday touches MA20, then closes above MA20 and prior-day high",
        },
        "ma20_support_reclaim_volume_confirmed": {
            "where": "c > ma60 AND l <= ma20 AND c > ma20 AND c > prior_high1 AND v >= avg_volume20",
            "label": "MA20 support reclaim in long trend plus volume >=20-day average",
        },
    }
    if scenario not in rules:
        raise ValueError(f"unsupported scenario: {scenario}")
    rule = rules[scenario]
    output = output_root / f"{_tag()}-{AXIS_ID}-{scenario}-tp{take_profit:.2f}-sl{stop_loss:.2f}"
    output.mkdir(parents=True, exist_ok=False)
    leads = ", ".join(
        f"LEAD(b.h, {day}) OVER ordered AS h{day}, LEAD(b.l, {day}) OVER ordered AS l{day}" for day in range(1, HORIZON + 1)
    )
    tp_day = _first_hit("h", ">=", f"c * {1 + take_profit}")
    sl_day = _first_hit("l", "<=", f"c * {1 - stop_loss}")
    sql = f"""
        WITH eligible AS (
            SELECT code FROM daily_bars WHERE source = 'pan' GROUP BY code HAVING MAX(date) >= {LATEST_CONFIRMED_EPOCH}
        ), bars AS (
            SELECT b.code, b.date, b.l, b.c, b.v,
                LAG(b.c, 10) OVER ordered AS c_10ago,
                LAG(b.h, 1) OVER ordered AS prior_high1,
                AVG(b.c) OVER ma20 AS ma20,
                AVG(b.c) OVER ma60 AS ma60,
                AVG(b.v) OVER vol20 AS avg_volume20,
                LEAD(b.c, {HORIZON}) OVER ordered AS c_end,
                {leads}
            FROM daily_bars b JOIN eligible e USING (code)
            WHERE b.source = 'pan'
            WINDOW
                ordered AS (PARTITION BY b.code ORDER BY b.date),
                ma20 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
                ma60 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
                , vol20 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
        ), outcomes AS (
            SELECT *, {tp_day} AS tp_day, {sl_day} AS sl_day
            FROM bars
            WHERE date BETWEEN 1546300800 AND 1767139200
              AND c_10ago IS NOT NULL AND prior_high1 IS NOT NULL AND ma20 IS NOT NULL AND ma60 IS NOT NULL AND avg_volume20 IS NOT NULL AND c_end IS NOT NULL
        ), events AS (
            SELECT date, CAST(year(to_timestamp(CAST(date AS BIGINT))) AS INTEGER) AS year,
                CASE WHEN sl_day <= {HORIZON} AND sl_day <= tp_day THEN -{stop_loss}
                     WHEN tp_day <= {HORIZON} THEN {take_profit}
                     ELSE (c_end / c) - 1.0 END AS realized_return
            FROM outcomes
            WHERE {rule["where"]}
        )
        SELECT date, year, AVG(realized_return) AS basket_return, COUNT(*) AS entries
        FROM events GROUP BY date, year ORDER BY date
    """
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = conn.execute(sql).fetchall()
    finally:
        conn.close()
    daily = [{"date": int(date), "year": int(year), "basket_return": float(ret), "entries": int(entries)} for date, year, ret, entries in rows]
    all_returns = [row["basket_return"] for row in daily]
    yearly: dict[int, list[float]] = {}
    for row in daily:
        yearly.setdefault(row["year"], []).append(row["basket_return"])
    positive_sum = sum(value for value in all_returns if value > 0)
    negative_sum = sum(value for value in all_returns if value < 0)
    payload = {
        "schema_version": f"tradex_{AXIS_ID}.risk.v1",
        "authoritative_result": True,
        "fixed_evaluation_conditions": {
            "source_db": str(db_path), "source_filter": "daily_bars.source = pan", "confirmed_latest_date": "2026-07-09",
            "scenario": scenario,
            "shape_rule": rule["label"],
            "entry": "signal-day close", "take_profit": take_profit, "stop_loss": stop_loss,
            "max_holding_days": HORIZON, "same_day_dual_hit": "stop first", "costs": "excluded",
        },
        "daily_basket_definition": "All same-date signals are equally weighted. The sequence is a non-overlapping risk proxy, not an executable overlapping-position portfolio simulation.",
        "aggregate": {
            "signal_day_count": len(daily), "mean_entries_per_day": sum(row["entries"] for row in daily) / len(daily),
            "basket_expectancy": sum(all_returns) / len(all_returns),
            "basket_profit_factor": positive_sum / abs(negative_sum),
            **_path_metrics(all_returns),
        },
        "yearly": [
            {"year": year, "signal_day_count": len(values), "basket_expectancy": sum(values) / len(values), **_path_metrics(values)}
            for year, values in sorted(yearly.items())
        ],
        "splits": [
            {
                "split": label,
                "years": years,
                "signal_day_count": len(values),
                "basket_expectancy": sum(values) / len(values),
                "basket_profit_factor": _profit_factor(values),
                **_path_metrics(values),
            }
            for label, years in (("train", (2019, 2020, 2021)), ("validation", (2022, 2023)), ("test", (2024, 2025)))
            for values in [[row["basket_return"] for row in daily if row["year"] in years]]
        ],
    }
    (output / "risk_path.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (output / "research_decision.json").write_text(json.dumps({
        "candidate_local_decision": "risk_path_measured_no_production_adoption",
        "authoritative_rollup_decision": "risk_path_measured_no_production_adoption",
        "production_ranking_changed": False, "runtime_db_write": False,
    }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return output


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--scenario", choices=["deep_discount_reversal", "ma20_support_reclaim_in_long_trend", "ma20_support_reclaim_volume_confirmed"], default="deep_discount_reversal")
    parser.add_argument("--take-profit", type=float, default=DEFAULT_TAKE_PROFIT)
    parser.add_argument("--stop-loss", type=float, default=DEFAULT_STOP_LOSS)
    args = parser.parse_args()
    print(run(db_path=args.db_path, output_root=args.output_root, scenario=args.scenario, take_profit=args.take_profit, stop_loss=args.stop_loss))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
