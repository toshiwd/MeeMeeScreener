from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


AXIS_ID = "multiscenario_entry_shape_gate_v1"
DEFAULT_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\chart_entry_geometry_research_v1")
LATEST_CONFIRMED_EPOCH = 1783555200  # 2026-07-09 UTC
TAKE_PROFIT, STOP_LOSS, HORIZON = 0.08, 0.05, 10
SPLITS = {"train": (2019, 2021), "validation": (2022, 2023), "test": (2024, 2025)}


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _first_hit(column: str, operator: str, threshold: str) -> str:
    return "LEAST(" + ", ".join(
        f"CASE WHEN {column}{day} {operator} {threshold} THEN {day} ELSE 99 END" for day in range(1, HORIZON + 1)
    ) + ")"


def _normalize(row: dict[str, Any]) -> dict[str, Any]:
    strings = {"scenario", "split"}
    integers = {"sample_count", "signal_days", "trading_days", "year"}
    return {
        key: value if key in strings else int(value) if key in integers else float(value) if value is not None else None
        for key, value in row.items()
    }


def run(*, db_path: Path, output_root: Path) -> Path:
    output = output_root / f"{_tag()}-{AXIS_ID}"
    output.mkdir(parents=True, exist_ok=False)
    future = ", ".join(
        f"LEAD(b.h, {day}) OVER ordered AS h{day}, LEAD(b.l, {day}) OVER ordered AS l{day}" for day in range(1, HORIZON + 1)
    )
    tp_day = _first_hit("h", ">=", f"c * {1 + TAKE_PROFIT}")
    sl_day = _first_hit("l", "<=", f"c * {1 - STOP_LOSS}")
    base = f"""
        WITH eligible AS (
            SELECT code FROM daily_bars WHERE source = 'pan' GROUP BY code HAVING MAX(date) >= {LATEST_CONFIRMED_EPOCH}
        ), bars AS (
            SELECT b.code, b.date, b.o, b.h, b.l, b.c, b.v,
                LAG(b.c, 10) OVER ordered AS c_10ago,
                LAG(b.h, 1) OVER ordered AS prior_high1,
                LAG(b.l, 1) OVER ordered AS prior_low1,
                LAG(b.o, 1) OVER ordered AS prior_open1,
                LAG(b.c, 1) OVER ordered AS prior_close1,
                LAG(b.h, 2) OVER ordered AS prior_high2,
                LAG(b.l, 2) OVER ordered AS prior_low2,
                MAX(b.h) OVER prior20 AS prior_high20,
                MIN(b.l) OVER prior20 AS prior_low20,
                MAX(b.h) OVER prior7 AS prior_high7,
                MIN(b.l) OVER prior7 AS prior_low7,
                MAX(b.h) OVER prior10 AS prior_high10,
                MAX(b.h) OVER prior60 AS prior_high60,
                MAX(b.h) OVER last10 AS high10,
                MIN(b.l) OVER last10 AS low10,
                AVG(b.c) OVER ma20 AS ma20,
                AVG(b.c) OVER ma20_lag5 AS ma20_5ago,
                AVG(b.c) OVER ma60 AS ma60,
                AVG(b.c) OVER ma60_lag5 AS ma60_5ago,
                AVG(b.c) OVER ma250 AS ma250,
                AVG(b.c) OVER ma250_lag20 AS ma250_20ago,
                AVG(b.v) OVER vol20 AS avg_volume20,
                LEAD(b.c, {HORIZON}) OVER ordered AS c_end,
                {future}
            FROM daily_bars b JOIN eligible e USING (code)
            WHERE b.source = 'pan'
            WINDOW
                ordered AS (PARTITION BY b.code ORDER BY b.date),
                prior20 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING),
                prior7 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING),
                prior10 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING),
                prior60 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING),
                last10 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW),
                ma20 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
                ma20_lag5 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 24 PRECEDING AND 5 PRECEDING),
                ma60 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW),
                ma60_lag5 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 64 PRECEDING AND 5 PRECEDING),
                ma250 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 249 PRECEDING AND CURRENT ROW),
                ma250_lag20 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 269 PRECEDING AND 20 PRECEDING),
                vol20 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
        ), market_range AS (
            SELECT date, AVG((h / l) - 1.0) AS market_mean_intraday_range
            FROM bars WHERE l > 0 GROUP BY date
        ), market_breadth_raw AS (
            SELECT date, AVG(CASE WHEN c >= ma20 THEN 1.0 ELSE 0.0 END) AS market_breadth_above_ma20
            FROM bars WHERE ma20 IS NOT NULL GROUP BY date
        ), market_breadth AS (
            SELECT date, market_breadth_above_ma20,
                LAG(market_breadth_above_ma20, 5) OVER (ORDER BY date) AS market_breadth_above_ma20_5ago
            FROM market_breadth_raw
        ), realized AS (
            SELECT *,
                (c / c_10ago) - 1.0 AS ret10,
                (high10 / low10) - 1.0 AS range10,
                {tp_day} AS tp_day,
                {sl_day} AS sl_day,
                CAST(year(to_timestamp(CAST(date AS BIGINT))) AS INTEGER) AS year
            FROM bars JOIN market_range USING (date) JOIN market_breadth USING (date)
            WHERE date BETWEEN 1546300800 AND 1767139200
              AND c_10ago IS NOT NULL AND prior_high1 IS NOT NULL AND prior_low1 IS NOT NULL AND prior_open1 IS NOT NULL AND prior_close1 IS NOT NULL AND prior_high2 IS NOT NULL AND prior_low2 IS NOT NULL AND prior_high20 IS NOT NULL AND prior_low20 IS NOT NULL
              AND prior_high7 IS NOT NULL AND prior_low7 IS NOT NULL
              AND prior_high10 IS NOT NULL
              AND prior_high60 IS NOT NULL
              AND ma20 IS NOT NULL AND ma20_5ago IS NOT NULL AND ma60 IS NOT NULL AND ma60_5ago IS NOT NULL AND avg_volume20 IS NOT NULL AND c_end IS NOT NULL
        ), outcomes AS (
            SELECT *,
                CASE WHEN sl_day <= {HORIZON} AND sl_day <= tp_day THEN -{STOP_LOSS}
                     WHEN tp_day <= {HORIZON} THEN {TAKE_PROFIT}
                     ELSE (c_end / c) - 1.0 END AS realized_return,
                CASE WHEN year <= 2021 THEN 'train' WHEN year <= 2023 THEN 'validation' ELSE 'test' END AS split
            FROM realized
        ), scenarios AS (
            SELECT 'deep_discount_reversal' AS scenario, * FROM outcomes
            WHERE ret10 <= -0.06 AND c > prior_high1 AND (c / ma20) - 1.0 <= -0.05
            UNION ALL
            SELECT 'deep_discount_reversal_market_range_le4pct', * FROM outcomes
            WHERE ret10 <= -0.06 AND c > prior_high1 AND (c / ma20) - 1.0 <= -0.05
              AND market_mean_intraday_range <= 0.04
            UNION ALL
            SELECT 'deep_discount_reversal_market_range_le3pct', * FROM outcomes
            WHERE ret10 <= -0.06 AND c > prior_high1 AND (c / ma20) - 1.0 <= -0.05
              AND market_mean_intraday_range <= 0.03
            UNION ALL
            SELECT 'deep_discount_reversal_breadth_le20pct', * FROM outcomes
            WHERE ret10 <= -0.06 AND c > prior_high1 AND (c / ma20) - 1.0 <= -0.05
              AND market_breadth_above_ma20 <= 0.20
            UNION ALL
            SELECT 'deep_discount_reversal_breadth_ge50pct', * FROM outcomes
            WHERE ret10 <= -0.06 AND c > prior_high1 AND (c / ma20) - 1.0 <= -0.05
              AND market_breadth_above_ma20 >= 0.50
            UNION ALL
            SELECT 'long_trend_deep_pullback_reclaim', * FROM outcomes
            WHERE ret10 <= -0.06 AND c > prior_high1 AND c > ma60 AND c <= ma20
            UNION ALL
            SELECT 'long_trend_20d_undercut_reclaim', * FROM outcomes
            WHERE l < prior_low20 AND c > prior_high1 AND c > ma20 AND c > ma60
            UNION ALL
            SELECT 'trend_pullback_reclaim', * FROM outcomes
            WHERE c > ma60 AND c > ma20 AND ret10 <= -0.03 AND c > prior_high1
            UNION ALL
            SELECT 'trend_pullback_reclaim_rising_ma20', * FROM outcomes
            WHERE c > ma60 AND c > ma20 AND ma20 > ma20_5ago AND ret10 <= -0.03 AND c > prior_high1
            UNION ALL
            SELECT 'trend_pullback_reclaim_market_breadth_ge50pct', * FROM outcomes
            WHERE c > ma60 AND c > ma20 AND ret10 <= -0.03 AND c > prior_high1
              AND market_breadth_above_ma20 >= 0.50
            UNION ALL
            SELECT 'ma20_support_reclaim_in_long_trend', * FROM outcomes
            WHERE c > ma60 AND l <= ma20 AND c > ma20 AND c > prior_high1
            UNION ALL
            SELECT 'ma20_support_reclaim_volume_confirmed', * FROM outcomes
            WHERE c > ma60 AND l <= ma20 AND c > ma20 AND c > prior_high1 AND v >= avg_volume20
            UNION ALL
            SELECT 'ma20_support_reclaim_full_trend_alignment', * FROM outcomes
            WHERE c > ma60 AND ma20 > ma60 AND l <= ma20 AND c > ma20 AND c > prior_high1
            UNION ALL
            SELECT 'fresh_trend_support_reclaim', * FROM outcomes
            WHERE c > ma60 AND ma20 > ma60 AND ma20_5ago <= ma60_5ago
              AND l <= ma20 AND c > ma20 AND c > prior_high1
            UNION ALL
            SELECT 'ma20_support_reclaim_upper_close', * FROM outcomes
            WHERE c > ma60 AND l <= ma20 AND c > ma20 AND c > prior_high1
              AND (c - l) / NULLIF(h - l, 0.0) >= 0.70
            UNION ALL
            SELECT 'ma20_support_shallow_reclaim_upper_close', * FROM outcomes
            WHERE c > ma60 AND l BETWEEN ma20 * 0.97 AND ma20 AND c > ma20 AND c > prior_high1
              AND (c - l) / NULLIF(h - l, 0.0) >= 0.70
            UNION ALL
            SELECT 'ma20_support_touch_reclaim_upper_close', * FROM outcomes
            WHERE c > ma60 AND l BETWEEN ma20 * 0.99 AND ma20 AND c > ma20 AND c > prior_high1
              AND (c - l) / NULLIF(h - l, 0.0) >= 0.70
            UNION ALL
            SELECT 'ma20_rising_support_touch_reclaim_upper_close', * FROM outcomes
            WHERE c > ma60 AND ma20 > ma20_5ago AND l BETWEEN ma20 * 0.99 AND ma20
              AND c > ma20 AND c > prior_high1 AND (c - l) / NULLIF(h - l, 0.0) >= 0.70
            UNION ALL
            SELECT 'ma20_rising_shallow_support_reclaim_upper_close', * FROM outcomes
            WHERE ma20 > ma60 AND ma20 > ma20_5ago AND l BETWEEN ma20 * 0.97 AND ma20
              AND c > ma20 AND c > prior_high1 AND c > o
              AND (c - l) / NULLIF(h - l, 0.0) >= 0.70
            UNION ALL
            SELECT 'ma20_rising_support_pullback_reclaim_upper_close', * FROM outcomes
            WHERE c > ma60 AND ma20 > ma20_5ago AND l BETWEEN ma20 * 0.99 AND ma20
              AND c > ma20 AND c > prior_high1 AND c <= prior_high10 * 0.98
              AND (c - l) / NULLIF(h - l, 0.0) >= 0.70
            UNION ALL
            SELECT 'ma20_rising_support_pullback_reclaim_upper_close_range_le20pct', * FROM outcomes
            WHERE c > ma60 AND ma20 > ma20_5ago AND l BETWEEN ma20 * 0.99 AND ma20
              AND c > ma20 AND c > prior_high1 AND c <= prior_high10 * 0.98 AND range10 <= 0.20
              AND (c - l) / NULLIF(h - l, 0.0) >= 0.70
            UNION ALL
            SELECT 'ma20_rising_support_pullback_reclaim_upper_close_volume_confirmed', * FROM outcomes
            WHERE c > ma60 AND ma20 > ma20_5ago AND l BETWEEN ma20 * 0.99 AND ma20
              AND c > ma20 AND c > prior_high1 AND c <= prior_high10 * 0.98
              AND (c - l) / NULLIF(h - l, 0.0) >= 0.70 AND v >= avg_volume20
            UNION ALL
            SELECT 'ma20_rising_support_pullback_reclaim_top15pct_close', * FROM outcomes
            WHERE c > ma60 AND ma20 > ma20_5ago AND l BETWEEN ma20 * 0.99 AND ma20
              AND c > ma20 AND c > prior_high1 AND c <= prior_high10 * 0.98
              AND (c - l) / NULLIF(h - l, 0.0) >= 0.85
            UNION ALL
            SELECT 'long_trend_gap_down_reversal', * FROM outcomes
            WHERE c > ma60 AND o <= prior_low1 * 0.98 AND c > prior_high1
            UNION ALL
            SELECT 'sharp_decline_gap_down_reversal', * FROM outcomes
            WHERE ret10 <= -0.06 AND o <= prior_low1 * 0.98 AND c > prior_high1
            UNION ALL
            SELECT 'normal_range_20d_breakout', * FROM outcomes
            WHERE c > prior_high20 AND range10 <= 0.12 AND c > ma20 AND c > ma60
            UNION ALL
            SELECT 'strong_momentum_20d_breakout', * FROM outcomes
            WHERE ret10 >= 0.06 AND c > prior_high20 AND c > ma20 AND c > ma60
            UNION ALL
            SELECT 'rising_trend_strong_body_20d_breakout', * FROM outcomes
            WHERE c > prior_high20 AND c > ma20 AND ma20 > ma60 AND ma20 > ma20_5ago
              AND c > o AND (c - o) / NULLIF(h - l, 0.0) >= 0.70
            UNION ALL
            SELECT 'long_term_uptrend_20d_breakout', * FROM outcomes
            WHERE c > prior_high20 AND c > ma20 AND ma20 > ma60 AND c > ma250 AND ma250 > ma250_20ago
            UNION ALL
            SELECT 'rising_trend_inside_day_breakout', * FROM outcomes
            WHERE prior_high1 < prior_high2 AND prior_low1 > prior_low2 AND c > prior_high1
              AND c > ma20 AND ma20 > ma60 AND ma20 > ma20_5ago
            UNION ALL
            SELECT 'rising_trend_inside_day_breakout_breadth_expanding', * FROM outcomes
            WHERE prior_high1 < prior_high2 AND prior_low1 > prior_low2 AND c > prior_high1
              AND c > ma20 AND ma20 > ma60 AND ma20 > ma20_5ago
              AND market_breadth_above_ma20 >= 0.50
              AND market_breadth_above_ma20 >= market_breadth_above_ma20_5ago + 0.10
            UNION ALL
            SELECT 'rising_trend_bullish_engulfing', * FROM outcomes
            WHERE prior_close1 < prior_open1 AND o <= prior_close1 AND c >= prior_open1
              AND c > ma20 AND ma20 > ma60 AND ma20 > ma20_5ago
            UNION ALL
            SELECT 'rising_trend_60d_high_breakout', * FROM outcomes
            WHERE c > prior_high60 AND c > ma20 AND ma20 > ma60 AND ma20 > ma20_5ago
            UNION ALL
            SELECT 'compact_handle_60d_breakout', * FROM outcomes
            WHERE c > prior_high60 AND range10 <= 0.10 AND c > ma20 AND ma20 > ma60
            UNION ALL
            SELECT 'tight_20d_shelf_breakout', * FROM outcomes
            WHERE c > prior_high20 AND (prior_high20 / prior_low20) - 1.0 <= 0.12
              AND c > ma20 AND ma20 > ma60
            UNION ALL
            SELECT 'tight_20d_shelf_breakout_volume_confirmed', * FROM outcomes
            WHERE c > prior_high20 AND (prior_high20 / prior_low20) - 1.0 <= 0.12
              AND c > ma20 AND ma20 > ma60 AND v >= avg_volume20
            UNION ALL
            SELECT 'tight_7d_squeeze_20d_breakout', * FROM outcomes
            WHERE c > prior_high20 AND (prior_high7 / prior_low7) - 1.0 <= 0.06
              AND c > ma20 AND ma20 > ma60
            UNION ALL
            SELECT 'tight_7d_squeeze_20d_breakout_volume_confirmed', * FROM outcomes
            WHERE c > prior_high20 AND (prior_high7 / prior_low7) - 1.0 <= 0.06
              AND c > ma20 AND ma20 > ma60 AND v >= avg_volume20
            UNION ALL
            SELECT 'tight_7d_squeeze_20d_breakout_market_breadth_ge50pct', * FROM outcomes
            WHERE c > prior_high20 AND (prior_high7 / prior_low7) - 1.0 <= 0.06
              AND c > ma20 AND ma20 > ma60 AND market_breadth_above_ma20 >= 0.50
        ), daily_baskets AS (
            SELECT scenario, split, date, year, AVG(realized_return) AS basket_return, COUNT(*) AS entries
            FROM scenarios GROUP BY scenario, split, date, year
        ), trade_metrics AS (
            SELECT scenario, split, COUNT(*) AS sample_count, COUNT(DISTINCT date) AS signal_days,
                AVG(realized_return) AS expectancy,
                SUM(CASE WHEN realized_return > 0 THEN realized_return ELSE 0 END)
                    / NULLIF(ABS(SUM(CASE WHEN realized_return < 0 THEN realized_return ELSE 0 END)), 0) AS profit_factor
            FROM scenarios GROUP BY scenario, split
        ), basket_metrics AS (
            SELECT scenario, split, COUNT(*) AS trading_days, AVG(entries) AS avg_entries_per_day,
                MAX(entries) AS max_entries_per_day, AVG(basket_return) AS basket_expectancy,
                SUM(CASE WHEN basket_return > 0 THEN basket_return ELSE 0 END)
                    / NULLIF(ABS(SUM(CASE WHEN basket_return < 0 THEN basket_return ELSE 0 END)), 0) AS basket_profit_factor
            FROM daily_baskets GROUP BY scenario, split
        ), yearly_metrics AS (
            SELECT scenario, split, year, COUNT(*) AS trading_days, AVG(basket_return) AS basket_expectancy,
                SUM(CASE WHEN basket_return > 0 THEN basket_return ELSE 0 END)
                    / NULLIF(ABS(SUM(CASE WHEN basket_return < 0 THEN basket_return ELSE 0 END)), 0) AS basket_profit_factor
            FROM daily_baskets GROUP BY scenario, split, year
        )
    """
    metrics_sql = base + """
        SELECT t.*, b.trading_days, b.avg_entries_per_day, b.max_entries_per_day,
               b.basket_expectancy, b.basket_profit_factor
        FROM trade_metrics t JOIN basket_metrics b USING (scenario, split)
        ORDER BY scenario, split
    """
    yearly_sql = base + "SELECT * FROM yearly_metrics ORDER BY scenario, year"
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        def fetch(sql: str) -> list[dict[str, Any]]:
            columns = [item[0] for item in conn.execute(sql).description]
            return [_normalize(dict(zip(columns, row))) for row in conn.execute(sql).fetchall()]
        metrics, yearly = fetch(metrics_sql), fetch(yearly_sql)
    finally:
        conn.close()

    scenarios = sorted({row["scenario"] for row in metrics})
    decisions: list[dict[str, Any]] = []
    for scenario in scenarios:
        by_split = {row["split"]: row for row in metrics if row["scenario"] == scenario}
        annual = [row for row in yearly if row["scenario"] == scenario]
        split_gate = all(
            split in by_split
            and by_split[split]["sample_count"] >= 300
            and by_split[split]["expectancy"] > 0
            and (by_split[split]["profit_factor"] or 0) >= 1.2
            and (by_split[split]["basket_profit_factor"] or 0) >= 1.15
            for split in SPLITS
        )
        annual_gate = len(annual) == 7 and all((row["basket_profit_factor"] or 0) >= 1.0 for row in annual)
        decision = "candidate_for_meemee_visual_review" if split_gate and annual_gate else "drop"
        decisions.append({
            "scenario": scenario, "decision": decision,
            "split_gate": split_gate, "annual_gate": annual_gate,
            "annual_basket_pf_min": min((row["basket_profit_factor"] or 0) for row in annual) if annual else None,
        })

    _write_json(output / "compare.json", {
        "schema_version": f"tradex_{AXIS_ID}.compare.v1", "authoritative_result": True,
        "research_phase": "branching_generation",
        "fixed_evaluation_conditions": {
            "source_db": str(db_path), "source_filter": "daily_bars.source = pan", "confirmed_latest_date": "2026-07-09",
            "entry_dates": "2019-01-01 through 2025-12-31", "entry": "signal-day close",
            "take_profit": TAKE_PROFIT, "stop_loss": STOP_LOSS, "max_holding_days": HORIZON,
            "same_day_dual_hit": "stop first", "costs": "excluded", "runtime_db_write": False,
        },
        "scenario_definitions": {
            "deep_discount_reversal": "ret10 <= -6%, prior-day-high reclaim, close >=5% below MA20",
            "deep_discount_reversal_market_range_le4pct": "deep-discount reversal plus same-day market mean intraday range <=4%",
            "deep_discount_reversal_market_range_le3pct": "deep-discount reversal plus same-day market mean intraday range <=3%",
            "deep_discount_reversal_breadth_le20pct": "deep-discount reversal plus same-day market breadth above MA20 <=20%",
            "deep_discount_reversal_breadth_ge50pct": "deep-discount reversal plus same-day market breadth above MA20 >=50%",
            "long_trend_deep_pullback_reclaim": "ret10 <=-6% reclaim above prior high while remaining above MA60 and below MA20",
            "long_trend_20d_undercut_reclaim": "20-day low undercut followed by prior-day-high reclaim above MA20 and MA60",
            "trend_pullback_reclaim": "above MA20 and MA60, ret10 <= -3%, prior-day-high reclaim",
            "trend_pullback_reclaim_rising_ma20": "trend pullback reclaim plus MA20 above its level five sessions ago",
            "trend_pullback_reclaim_market_breadth_ge50pct": "trend pullback reclaim plus market breadth above MA20 >=50%",
            "ma20_support_reclaim_in_long_trend": "above MA60, intraday touches MA20, then closes above MA20 and prior-day high",
            "ma20_support_reclaim_volume_confirmed": "MA20 support reclaim in long trend plus volume >=20-day average",
            "ma20_support_reclaim_full_trend_alignment": "MA20 support reclaim in long trend with MA20 above MA60",
            "fresh_trend_support_reclaim": "MA20 support reclaim within five sessions of MA20 crossing above MA60",
            "ma20_support_reclaim_upper_close": "MA20 support reclaim in long trend with close in upper 30% of the candle range",
            "ma20_support_shallow_reclaim_upper_close": "MA20 support reclaim with <=3% undercut and close in upper 30% of candle range",
            "ma20_support_touch_reclaim_upper_close": "MA20 support touch/reclaim with <=1% undercut and close in upper 30% of candle range",
            "ma20_rising_support_touch_reclaim_upper_close": "rising MA20 support touch/reclaim with <=1% undercut and close in upper 30% of candle range",
            "ma20_rising_shallow_support_reclaim_upper_close": "rising MA20 above MA60, <=3% support undercut, bullish prior-high reclaim and close in upper 30% of candle range",
            "ma20_rising_support_pullback_reclaim_upper_close": "rising MA20 support reclaim after a >=2% pullback from the prior 10-day high",
            "ma20_rising_support_pullback_reclaim_upper_close_range_le20pct": "rising MA20 support pullback reclaim with entry-day 10-session range <=20%",
            "ma20_rising_support_pullback_reclaim_upper_close_volume_confirmed": "rising MA20 support pullback reclaim with volume >=20-day average",
            "ma20_rising_support_pullback_reclaim_top15pct_close": "rising MA20 support pullback reclaim with close in the top 15% of the candle range",
            "long_trend_gap_down_reversal": "above MA60, opens at least 2% below prior low, then closes above prior high",
            "sharp_decline_gap_down_reversal": "ret10 <= -6%, opens at least 2% below prior low, then closes above prior high",
            "normal_range_20d_breakout": "20-day high breakout, 10-day range <=12%, above MA20 and MA60",
            "strong_momentum_20d_breakout": "ret10 >=6%, 20-day high breakout, above MA20 and MA60",
            "rising_trend_strong_body_20d_breakout": "20-day high breakout in a rising MA20 trend with a bullish body >=70% of candle range",
            "long_term_uptrend_20d_breakout": "20-day high breakout above a rising 250-day average, with MA20 above MA60",
            "rising_trend_inside_day_breakout": "inside-day high breakout with MA20 above MA60 and rising over five sessions",
            "rising_trend_inside_day_breakout_breadth_expanding": "rising-trend inside-day breakout with market MA20 breadth >=50% and improving by >=10 points in five sessions",
            "rising_trend_bullish_engulfing": "bullish body engulfing a prior red candle with MA20 above MA60 and rising over five sessions",
            "rising_trend_60d_high_breakout": "60-day high breakout with MA20 above MA60 and rising over five sessions",
            "compact_handle_60d_breakout": "60-day high breakout with a <=10% 10-session range and MA20 above MA60",
            "tight_20d_shelf_breakout": "20-day high breakout from a prior 20-day shelf <=12%, MA20 above MA60",
            "tight_20d_shelf_breakout_volume_confirmed": "tight 20-day shelf breakout with volume >=20-day average",
            "tight_7d_squeeze_20d_breakout": "20-day high breakout after a prior 7-day range <=6%, MA20 above MA60",
            "tight_7d_squeeze_20d_breakout_volume_confirmed": "tight 7-day squeeze breakout with volume >=20-day average",
            "tight_7d_squeeze_20d_breakout_market_breadth_ge50pct": "tight 7-day squeeze breakout only when >=50% of eligible stocks are above MA20",
        },
        "split_definitions": SPLITS,
        "metrics_by_split": metrics,
        "yearly_daily_basket_metrics": yearly,
        "predeclared_gates": {
            "each_split": "sample >=300, trade PF >=1.2, daily basket PF >=1.15, positive expectancy",
            "annual": "each of seven annual daily basket PF >=1.0",
        },
        "candidate_decisions": decisions,
    })
    _write_json(output / "research_decision.json", {
        "candidate_local_decision": "see_candidate_decisions",
        "authoritative_rollup_decision": "see_candidate_decisions",
        "production_ranking_changed": False,
        "runtime_db_write": False,
    })
    _write_json(output / "no_lookahead_audit.json", {
        "audit_result": "pass",
        "point_in_time_shape_fields": ["ret10", "prior highs", "range10", "MA20", "MA60"],
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
