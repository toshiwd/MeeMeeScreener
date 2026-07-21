from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


AXIS_ID = "train_only_shape_tag_discovery_v1"
DEFAULT_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\chart_entry_geometry_research_v1")
LATEST_CONFIRMED_EPOCH = 1783555200
TAKE_PROFIT, STOP_LOSS, HORIZON = 0.08, 0.05, 10

BASES = {
    "trend_pullback": "c > ma20 AND ma20 > ma60 AND ret10 BETWEEN -0.10 AND -0.03 AND c > prior_high1",
    "ma20_support_reclaim": "c > ma60 AND l <= ma20 AND c > ma20 AND c > prior_high1",
    "trend_20d_breakout": "c > prior_high20 AND c > ma20 AND ma20 > ma60",
    "trend_inside_breakout": "prior_high1 < prior_high2 AND prior_low1 > prior_low2 AND c > prior_high1 AND c > ma20 AND ma20 > ma60",
    "deep_discount_reversal": "ret10 <= -0.06 AND c > prior_high1 AND (c / ma20) - 1.0 <= -0.05",
    "strong_60d_pullback": "ret60 >= 0.20 AND ret10 BETWEEN -0.10 AND -0.03 AND c > ma20 AND c > ma60 AND c > prior_high1",
    "strong_60d_breakout": "ret60 >= 0.20 AND c > prior_high20 AND c > ma20 AND ma20 > ma60",
    "relative_strength_pullback": "ret60_percentile >= 0.80 AND ret10 BETWEEN -0.10 AND -0.03 AND c > ma20 AND c > ma60 AND c > prior_high1",
    "relative_strength_breakout": "ret60_percentile >= 0.80 AND c > prior_high20 AND c > ma20 AND ma20 > ma60",
    "shallow_high_zone_reclaim": "ma20 > ma60 AND c >= prior_high20 * 0.95 AND l BETWEEN ma20 * 0.99 AND ma20 AND c > ma20 AND c > o AND (c - l) / NULLIF(h - l, 0.0) >= 0.70",
}
CONFIRMATIONS = {
    "ma20_rising": "ma20 > ma20_5ago",
    "upper_close": "(c - l) / NULLIF(h - l, 0.0) >= 0.70",
    "volume_confirmed": "v >= avg_volume20",
    "tight_10d_range": "range10 <= 0.10",
    "shallow_ma20_touch": "l BETWEEN ma20 * 0.99 AND ma20",
    "pullback_from_high10": "c <= prior_high10 * 0.98",
    "bullish_body": "c > o AND (c - o) / NULLIF(h - l, 0.0) >= 0.50",
    "volume_dry_up": "v <= avg_volume20 * 0.70",
    "long_lower_wick": "(LEAST(o, c) - l) / NULLIF(h - l, 0.0) >= 0.40",
    "narrow_entry_day": "(h / l) - 1.0 <= 0.08",
    "above_ma60": "c > ma60",
    "ma60_rising": "ma60 > ma60_5ago",
    "market_breadth_middle": "market_breadth BETWEEN 0.20 AND 0.50",
    "market_breadth_high_stable": "market_breadth >= 0.50 AND ABS(market_breadth - market_breadth_5ago) <= 0.10",
    "market_breadth_high_rising": "market_breadth >= 0.50 AND market_breadth >= market_breadth_5ago + 0.05",
    "market_breadth_middle_rising": "market_breadth BETWEEN 0.20 AND 0.50 AND market_breadth >= market_breadth_5ago",
    "market_breadth_low": "market_breadth <= 0.20",
    "market_breadth_high": "market_breadth >= 0.50",
    "market_breadth_middle_rising_range_le3pct": "market_breadth BETWEEN 0.20 AND 0.50 AND market_breadth >= market_breadth_5ago AND market_mean_intraday_range <= 0.03",
    "market_breadth_middle_rising_range_le4pct": "market_breadth BETWEEN 0.20 AND 0.50 AND market_breadth >= market_breadth_5ago AND market_mean_intraday_range <= 0.04",
}
SECONDARY_CONFIRMATIONS = {
    "upper_close": CONFIRMATIONS["upper_close"],
    "volume_confirmed": CONFIRMATIONS["volume_confirmed"],
    "ma20_rising": CONFIRMATIONS["ma20_rising"],
    "shallow_ma20_touch": CONFIRMATIONS["shallow_ma20_touch"],
    "tight_10d_range": CONFIRMATIONS["tight_10d_range"],
}


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _first_hit(column: str, operator: str, threshold: str) -> str:
    return "LEAST(" + ", ".join(
        f"CASE WHEN {column}{day} {operator} {threshold} THEN {day} ELSE 99 END" for day in range(1, HORIZON + 1)
    ) + ")"


def _first_hit_offset(column: str, start: int, operator: str, threshold: str) -> str:
    return "LEAST(" + ", ".join(
        f"CASE WHEN {column}{day} {operator} {threshold} THEN {day - start + 1} ELSE 99 END"
        for day in range(start, start + HORIZON)
    ) + ")"


def _rows(conn: duckdb.DuckDBPyConnection, sql: str) -> list[dict[str, Any]]:
    cursor = conn.execute(sql)
    columns = [item[0] for item in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _metrics(
    conn: duckdb.DuckDBPyConnection, condition: str, return_column: str = "realized_return", table_name: str = "features"
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    sql = f"""
        WITH selected AS (
            SELECT * FROM {table_name} WHERE {condition}
        ), baskets AS (
            SELECT split, year, date, AVG({return_column}) AS basket_return, COUNT(*) AS entries
            FROM selected GROUP BY split, year, date
        ), trade_metrics AS (
            SELECT split, COUNT(*) AS sample_count, COUNT(DISTINCT date) AS signal_days,
                AVG({return_column}) AS expectancy,
                SUM(CASE WHEN {return_column} > 0 THEN {return_column} ELSE 0 END)
                    / NULLIF(ABS(SUM(CASE WHEN {return_column} < 0 THEN {return_column} ELSE 0 END)), 0) AS profit_factor
            FROM selected GROUP BY split
        ), basket_metrics AS (
            SELECT split, COUNT(*) AS trading_days, AVG(entries) AS avg_entries_per_day,
                AVG(basket_return) AS basket_expectancy,
                SUM(CASE WHEN basket_return > 0 THEN basket_return ELSE 0 END)
                    / NULLIF(ABS(SUM(CASE WHEN basket_return < 0 THEN basket_return ELSE 0 END)), 0) AS basket_profit_factor
            FROM baskets GROUP BY split
        )
        SELECT t.*, b.trading_days, b.avg_entries_per_day, b.basket_expectancy, b.basket_profit_factor
        FROM trade_metrics t JOIN basket_metrics b USING (split) ORDER BY split
    """
    yearly_sql = f"""
        WITH selected AS (
            SELECT * FROM {table_name} WHERE {condition}
        ), baskets AS (
            SELECT split, year, date, AVG({return_column}) AS basket_return
            FROM selected GROUP BY split, year, date
        )
        SELECT split, year, COUNT(*) AS trading_days, AVG(basket_return) AS basket_expectancy,
            SUM(CASE WHEN basket_return > 0 THEN basket_return ELSE 0 END)
                / NULLIF(ABS(SUM(CASE WHEN basket_return < 0 THEN basket_return ELSE 0 END)), 0) AS basket_profit_factor
        FROM baskets GROUP BY split, year ORDER BY year
    """
    return _rows(conn, sql), _rows(conn, yearly_sql)


def _passes_train(metrics: list[dict[str, Any]]) -> bool:
    train = next((row for row in metrics if row["split"] == "train"), None)
    return bool(
        train
        and train["sample_count"] >= 300
        and (train["expectancy"] or 0) > 0
        and (train["profit_factor"] or 0) >= 1.20
        and (train["basket_profit_factor"] or 0) >= 1.15
    )


def _passes_full(metrics: list[dict[str, Any]], yearly: list[dict[str, Any]]) -> bool:
    by_split = {row["split"]: row for row in metrics}
    split_gate = all(
        split in by_split
        and by_split[split]["sample_count"] >= 300
        and (by_split[split]["expectancy"] or 0) > 0
        and (by_split[split]["profit_factor"] or 0) >= 1.20
        and (by_split[split]["basket_profit_factor"] or 0) >= 1.15
        for split in ("train", "validation", "test")
    )
    annual_gate = len(yearly) == 7 and all((row["basket_profit_factor"] or 0) >= 1.0 for row in yearly)
    return split_gate and annual_gate


def run(*, db_path: Path, output_root: Path) -> Path:
    output = output_root / f"{_tag()}-{AXIS_ID}"
    output.mkdir(parents=True, exist_ok=False)
    future = ", ".join(
        f"LEAD(b.h, {day}) OVER ordered AS h{day}, LEAD(b.l, {day}) OVER ordered AS l{day}" for day in range(1, HORIZON + 2)
    )
    tp_day = _first_hit("h", ">=", f"c * {1 + TAKE_PROFIT}")
    sl_day = _first_hit("l", "<=", f"c * {1 - STOP_LOSS}")
    next_tp_day = _first_hit_offset("h", 2, ">=", f"c_next * {1 + TAKE_PROFIT}")
    next_sl_day = _first_hit_offset("l", 2, "<=", f"c_next * {1 - STOP_LOSS}")
    features_sql = f"""
        CREATE TEMP TABLE features AS
        WITH eligible AS (
            SELECT code FROM daily_bars WHERE source = 'pan' GROUP BY code HAVING MAX(date) >= {LATEST_CONFIRMED_EPOCH}
        ), bars AS (
            SELECT b.code, b.date, b.o, b.h, b.l, b.c, b.v,
                LAG(b.c, 10) OVER ordered AS c_10ago, LAG(b.c, 60) OVER ordered AS c_60ago,
                LAG(b.h, 1) OVER ordered AS prior_high1, LAG(b.l, 1) OVER ordered AS prior_low1,
                LAG(b.h, 2) OVER ordered AS prior_high2, LAG(b.l, 2) OVER ordered AS prior_low2,
                MAX(b.h) OVER prior10 AS prior_high10, MAX(b.h) OVER prior20 AS prior_high20,
                MAX(b.h) OVER last10 AS high10, MIN(b.l) OVER last10 AS low10,
                AVG(b.c) OVER ma20 AS ma20, AVG(b.c) OVER ma20_lag5 AS ma20_5ago,
                AVG(b.c) OVER ma60 AS ma60, AVG(b.c) OVER ma60_lag5 AS ma60_5ago, AVG(b.v) OVER vol20 AS avg_volume20,
                LEAD(b.c, 1) OVER ordered AS c_next, LEAD(b.c, {HORIZON}) OVER ordered AS c_end,
                LEAD(b.c, {HORIZON + 1}) OVER ordered AS c_end_next, {future}
            FROM daily_bars b JOIN eligible e USING (code)
            WHERE b.source = 'pan'
            WINDOW
                ordered AS (PARTITION BY b.code ORDER BY b.date),
                prior10 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING),
                prior20 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING),
                last10 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW),
                ma20 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
                ma20_lag5 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 24 PRECEDING AND 5 PRECEDING),
                ma60 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW),
                ma60_lag5 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 64 PRECEDING AND 5 PRECEDING),
                vol20 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
        ), market_breadth_raw AS (
            SELECT date, AVG(CASE WHEN c >= ma20 THEN 1.0 ELSE 0.0 END) AS market_breadth
            FROM bars WHERE ma20 IS NOT NULL GROUP BY date
        ), market_range_by_date AS (
            SELECT date, AVG((h / l) - 1.0) AS market_mean_intraday_range
            FROM bars WHERE l > 0 GROUP BY date
        ), market_breadth_by_date AS (
            SELECT date, market_breadth,
                LAG(market_breadth, 5) OVER (ORDER BY date) AS market_breadth_5ago,
                market_mean_intraday_range
            FROM market_breadth_raw JOIN market_range_by_date USING (date)
        ), realized AS (
            SELECT *, (c / c_10ago) - 1.0 AS ret10, (c / c_60ago) - 1.0 AS ret60, (high10 / low10) - 1.0 AS range10,
                {tp_day} AS tp_day, {sl_day} AS sl_day, {next_tp_day} AS next_tp_day, {next_sl_day} AS next_sl_day,
                CAST(year(to_timestamp(CAST(date AS BIGINT))) AS INTEGER) AS year
            FROM bars JOIN market_breadth_by_date USING (date)
            WHERE date BETWEEN 1546300800 AND 1767139200
              AND c_10ago IS NOT NULL AND c_60ago IS NOT NULL AND prior_high1 IS NOT NULL AND prior_low1 IS NOT NULL
              AND prior_high2 IS NOT NULL AND prior_low2 IS NOT NULL AND prior_high10 IS NOT NULL AND prior_high20 IS NOT NULL
              AND ma20 IS NOT NULL AND ma20_5ago IS NOT NULL AND ma60 IS NOT NULL AND ma60_5ago IS NOT NULL AND avg_volume20 IS NOT NULL
              AND c_end IS NOT NULL AND c_next IS NOT NULL AND c_end_next IS NOT NULL
        ), ranked AS (
            SELECT *, CUME_DIST() OVER (PARTITION BY date ORDER BY ret60) AS ret60_percentile
            FROM realized
        )
        SELECT *, CASE WHEN sl_day <= {HORIZON} AND sl_day <= tp_day THEN -{STOP_LOSS}
                       WHEN tp_day <= {HORIZON} THEN {TAKE_PROFIT}
                       ELSE (c_end / c) - 1.0 END AS realized_return,
            CASE WHEN next_sl_day <= {HORIZON} AND next_sl_day <= next_tp_day THEN -{STOP_LOSS}
                 WHEN next_tp_day <= {HORIZON} THEN {TAKE_PROFIT}
                 ELSE (c_end_next / c_next) - 1.0 END AS confirmed_realized_return,
            CASE WHEN year <= 2021 THEN 'train' WHEN year <= 2023 THEN 'validation' ELSE 'test' END AS split
        FROM ranked
    """
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        conn.execute(features_sql)
        screened: list[dict[str, Any]] = []
        for entry_mode, return_column, entry_condition in (
            ("signal_day_close", "realized_return", "TRUE"),
            ("next_day_close_if_higher", "confirmed_realized_return", "c_next > c"),
            ("next_day_close_if_signal_high_broken", "confirmed_realized_return", "c_next > h"),
        ):
            for base_name, base_condition in BASES.items():
                for confirmation_name, confirmation_condition in CONFIRMATIONS.items():
                    scenario = f"{entry_mode}__{base_name}__{confirmation_name}"
                    condition = f"({base_condition}) AND ({confirmation_condition}) AND ({entry_condition})"
                    metrics, yearly = _metrics(conn, condition, return_column)
                    if _passes_train(metrics):
                        screened.append({
                            "scenario": scenario,
                            "entry_mode": entry_mode,
                            "base_tag": base_name,
                            "confirmation_tag": confirmation_name,
                            "selection_condition": condition,
                            "metrics_by_split": metrics,
                            "yearly_daily_basket_metrics": yearly,
                            "train_screen_pass": True,
                            "candidate_local_decision": "candidate_for_meemee_visual_review" if _passes_full(metrics, yearly) else "drop",
                        })
        anchor_condition = (
            f"({BASES['ma20_support_reclaim']}) AND ({CONFIRMATIONS['market_breadth_high_stable']}) AND (c_next > c)"
        )
        for confirmation_name, confirmation_condition in SECONDARY_CONFIRMATIONS.items():
            condition = f"{anchor_condition} AND ({confirmation_condition})"
            metrics, yearly = _metrics(conn, condition, "confirmed_realized_return")
            if _passes_train(metrics):
                screened.append({
                    "scenario": f"next_day_close_if_higher__ma20_support_reclaim__market_breadth_high_stable__{confirmation_name}",
                    "entry_mode": "next_day_close_if_higher",
                    "base_tag": "ma20_support_reclaim",
                    "confirmation_tag": f"market_breadth_high_stable + {confirmation_name}",
                    "selection_condition": condition,
                    "metrics_by_split": metrics,
                    "yearly_daily_basket_metrics": yearly,
                    "train_screen_pass": True,
                    "candidate_local_decision": "candidate_for_meemee_visual_review" if _passes_full(metrics, yearly) else "drop",
                })
        conn.execute(
            f"""
            CREATE TEMP TABLE high_stable_support_ranked AS
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY date ORDER BY (v / avg_volume20) DESC, code) AS volume_rank,
                ROW_NUMBER() OVER (PARTITION BY date ORDER BY ((c - l) / NULLIF(h - l, 0.0)) DESC, code) AS upper_close_rank,
                ROW_NUMBER() OVER (PARTITION BY date ORDER BY ABS((l / ma20) - 1.0) ASC, code) AS support_precision_rank
            FROM features WHERE {anchor_condition}
            """
        )
        for selector_name, rank_condition in (
            ("top1_volume", "volume_rank = 1"), ("top3_volume", "volume_rank <= 3"),
            ("top1_upper_close", "upper_close_rank = 1"), ("top3_upper_close", "upper_close_rank <= 3"),
            ("top1_support_precision", "support_precision_rank = 1"), ("top3_support_precision", "support_precision_rank <= 3"),
        ):
            metrics, yearly = _metrics(conn, rank_condition, "confirmed_realized_return", "high_stable_support_ranked")
            if _passes_train(metrics):
                screened.append({
                    "scenario": f"next_day_close_if_higher__ma20_support_reclaim__market_breadth_high_stable__{selector_name}",
                    "entry_mode": "next_day_close_if_higher",
                    "base_tag": "ma20_support_reclaim",
                    "confirmation_tag": f"market_breadth_high_stable + {selector_name}",
                    "selection_condition": f"{anchor_condition}; {rank_condition}",
                    "metrics_by_split": metrics,
                    "yearly_daily_basket_metrics": yearly,
                    "train_screen_pass": True,
                    "candidate_local_decision": "candidate_for_meemee_visual_review" if _passes_full(metrics, yearly) else "drop",
                })
    finally:
        conn.close()
    payload = {
        "schema_version": f"tradex_{AXIS_ID}.compare.v1",
        "authoritative_result": True,
        "research_phase": "branching_generation",
        "fixed_evaluation_conditions": {
            "source_db": str(db_path), "source_filter": "daily_bars.source = pan", "confirmed_latest_date": "2026-07-09",
            "entry": "signal-day close", "take_profit": TAKE_PROFIT, "stop_loss": STOP_LOSS, "max_holding_days": HORIZON,
            "same_day_dual_hit": "stop first", "costs": "excluded", "runtime_db_write": False,
            "selection_protocol": "only train 2019-2021 determines screened candidates; validation 2022-2023 and test 2024-2025 are held out",
        },
        "base_tags": BASES,
        "confirmation_tags": CONFIRMATIONS,
        "secondary_confirmations_for_high_stable_support_anchor": SECONDARY_CONFIRMATIONS,
        "train_screen_gate": "sample >=300, positive expectancy, trade PF >=1.20, daily basket PF >=1.15",
        "full_adoption_gate": "each split sample >=300, positive expectancy, trade PF >=1.20, daily PF >=1.15; all seven annual daily PF >=1.0",
        "screened_candidates": screened,
        "authoritative_rollup_decision": "candidate_for_meemee_visual_review" if any(item["candidate_local_decision"] == "candidate_for_meemee_visual_review" for item in screened) else "no_candidate",
        "production_ranking_changed": False,
        "runtime_db_write": False,
    }
    (output / "compare.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return output


def main() -> int:
    print(run(db_path=DEFAULT_DB, output_root=DEFAULT_OUTPUT_ROOT))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
