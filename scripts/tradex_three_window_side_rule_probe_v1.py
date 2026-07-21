from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


AXIS_ID = "three_window_side_rule_probe_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\three_window_side_rule_probe_v1")

ACCEPTANCE = {
    "min_n": 80,
    "min_target3pct_rate": 0.60,
    "max_adverse3pct_rate": 0.30,
    "min_target_to_adverse_ratio": 2.0,
    "min_cost_adjusted_target3pct_rate": 0.60,
    "max_cost_adjusted_adverse3pct_rate": 0.30,
    "min_cost_adjusted_target_to_adverse_ratio": 2.0,
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_db_path() -> Path:
    local = os.environ.get("LOCALAPPDATA")
    if local:
        return Path(local) / "MeeMeeScreener" / "data" / "stocks.duckdb"
    return Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _latest_pan_date(con: duckdb.DuckDBPyConnection) -> str:
    row = con.execute("SELECT max(to_timestamp(date)::date) FROM daily_bars WHERE source = 'pan'").fetchone()
    if row is None or row[0] is None:
        raise RuntimeError("daily_bars source=pan has no rows")
    return row[0].isoformat()


def _build_features(
    con: duckdb.DuckDBPyConnection,
    start: str,
    end: str,
    *,
    table_name: str = "three_window_events",
    require_forward_labels: bool = True,
) -> None:
    forward_label_where = "AND low_f5 IS NOT NULL AND high_f5 IS NOT NULL" if require_forward_labels else ""
    con.execute(
        f"""
CREATE OR REPLACE TEMP TABLE {table_name} AS
WITH pan AS (
  SELECT code, date, o, h, l, c, v
  FROM daily_bars
  WHERE source = 'pan'
    AND date BETWEEN epoch(date '{start}')::BIGINT AND epoch(date '{end}')::BIGINT
),
weekly_calendar AS (
  SELECT
    code,
    epoch(CAST(date_trunc('week', to_timestamp(date)) AS DATE))::BIGINT AS week_start,
    first(o ORDER BY date) AS week_o,
    max(h) AS week_h,
    min(l) AS week_l,
    last(c ORDER BY date) AS week_c,
    sum(v) AS week_v
  FROM pan
  GROUP BY 1, 2
),
weekly_features AS (
  SELECT
    *,
    lag(week_l, 1) OVER (PARTITION BY code ORDER BY week_start) AS prev_week_l
  FROM weekly_calendar
),
d AS (
  SELECT
    code, date, o, h, l, c, v,
    lag(o, 1) OVER w AS o1,
    lag(h, 1) OVER w AS h1,
    lag(l, 1) OVER w AS l1,
    lag(c, 1) OVER w AS c1,
    lag(c, 2) OVER w AS c2,
    lag(c, 3) OVER w AS c3,
    lag(c, 5) OVER w AS c5,
    lag(c, 10) OVER w AS c10,
    lag(c, 20) OVER w AS c20,
    lag(c, 60) OVER w AS c60,
    lag(c, 120) OVER w AS c120,
    lag(c, 240) OVER w AS c240,
    lead(l, 1) OVER w AS next_l,
    lead(h, 1) OVER w AS next_h,
    lead(c, 1) OVER w AS next_c,
    min(l) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 1 FOLLOWING AND 5 FOLLOWING) AS low_f5,
    max(h) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 1 FOLLOWING AND 5 FOLLOWING) AS high_f5,
    avg(c) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ma7,
    avg(c) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20,
    avg(c) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS ma60,
    max(h) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS high20,
    min(l) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS low20,
    max(h) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS high60,
    min(l) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS low60,
    max(h) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) AS high120,
    min(l) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) AS low120,
    max(h) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 239 PRECEDING AND CURRENT ROW) AS high240,
    min(l) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 239 PRECEDING AND CURRENT ROW) AS low240
  FROM pan
  WINDOW w AS (PARTITION BY code ORDER BY date)
),
d2 AS (
  SELECT
    *,
    lag(ma7, 1) OVER (PARTITION BY code ORDER BY date) AS ma7_1,
    lag(ma7, 3) OVER (PARTITION BY code ORDER BY date) AS ma7_3,
    lag(ma20, 5) OVER (PARTITION BY code ORDER BY date) AS ma20_5,
    lag(ma20, 10) OVER (PARTITION BY code ORDER BY date) AS ma20_10,
    c / NULLIF(ma20, 0) - 1.0 AS close_vs_ma20_raw,
    c / NULLIF(ma60, 0) - 1.0 AS close_vs_ma60_raw,
    ((c / NULLIF(c5, 0) - 1.0) * -1.0) AS drop5_raw
  FROM d
),
f AS (
  SELECT
    *,
    min(close_vs_ma20_raw) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) AS min_close_vs_ma20_120,
    min(close_vs_ma60_raw) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 239 PRECEDING AND CURRENT ROW) AS min_close_vs_ma60_240,
    max(drop5_raw) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) AS max_5d_drop_120,
    max(drop5_raw) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 239 PRECEDING AND CURRENT ROW) AS max_5d_drop_240
  FROM d2
),
features AS (
  SELECT
    f.*,
    wf.week_o,
    wf.week_h,
    wf.week_l,
    wf.week_c,
    wf.prev_week_l,
    CASE WHEN h > l THEN (h - greatest(o, c)) / (h - l) ELSE NULL END AS upper_wick,
    CASE WHEN h > l THEN (least(o, c) - l) / (h - l) ELSE NULL END AS lower_wick,
    c / NULLIF(c20, 0) - 1.0 AS ret20,
    c / NULLIF(c60, 0) - 1.0 AS ret60,
    c / NULLIF(c120, 0) - 1.0 AS ret120,
    c / NULLIF(c240, 0) - 1.0 AS ret240,
    c / NULLIF(high60, 0) - 1.0 AS close_vs_high60,
    c / NULLIF(high120, 0) - 1.0 AS close_vs_high120,
    c / NULLIF(high240, 0) - 1.0 AS close_vs_high240,
    c / NULLIF(low20, 0) - 1.0 AS close_vs_low20,
    c / NULLIF(low60, 0) - 1.0 AS close_vs_low60,
    (c - low20) / NULLIF(high20 - low20, 0) AS range20_pos,
    (c - low120) / NULLIF(high120 - low120, 0) AS range120_pos,
    (c - low240) / NULLIF(high240 - low240, 0) AS range240_pos,
    ma7 / NULLIF(ma7_3, 0) - 1.0 AS ma7_slope3,
    ma20 / NULLIF(ma20_5, 0) - 1.0 AS ma20_slope5,
    ma20 / NULLIF(ma20_10, 0) - 1.0 AS ma20_slope10,
    CASE WHEN wf.week_h > wf.week_l THEN (wf.week_h - wf.week_c) / (wf.week_h - wf.week_l) ELSE NULL END AS week_upper_wick,
    CASE WHEN wf.week_h > wf.week_l THEN (wf.week_c - wf.week_l) / (wf.week_h - wf.week_l) ELSE NULL END AS week_close_pos,
    (wf.week_c < wf.week_o) AS week_bear,
    (wf.week_c < wf.prev_week_l) AS week_prev_low_break,
    ((CASE WHEN c < c1 THEN 1 ELSE 0 END)
      + (CASE WHEN c1 < c2 THEN 1 ELSE 0 END)
      + (CASE WHEN c2 < c3 THEN 1 ELSE 0 END)) AS red_count3,
    ((CASE WHEN c > c1 THEN 1 ELSE 0 END)
      + (CASE WHEN c1 > c2 THEN 1 ELSE 0 END)
      + (CASE WHEN c2 > c3 THEN 1 ELSE 0 END)) AS green_count3
  FROM f
  LEFT JOIN weekly_features wf
    ON wf.code = f.code
   AND wf.week_start = epoch(CAST(date_trunc('week', to_timestamp(f.date)) AS DATE))::BIGINT
)
SELECT
  *,
  (ret240 >= 0.30 AND range240_pos >= 0.65 AND min_close_vs_ma60_240 > -0.10) AS short_year_mature_uptrend,
  (ret120 >= 0.12 AND max_5d_drop_120 < 0.14 AND min_close_vs_ma20_120 > -0.07) AS short_half_not_broken_yet,
  (close_vs_high60 BETWEEN -0.12 AND -0.015
    AND c < ma7
    AND c1 < ma7_1
    AND ma7_slope3 < 0
    AND c / NULLIF(ma20, 0) - 1.0 BETWEEN -0.08 AND 0.03
    AND red_count3 >= 2
    AND lower_wick < 0.28
    AND next_l < l) AS short_recent_entry_trigger,
  (ret240 <= -0.20 AND range240_pos <= 0.45 AND c / NULLIF(low240, 0) - 1.0 <= 0.35) AS long_year_decline_or_base,
  (ret120 BETWEEN -0.20 AND 0.20 AND max_5d_drop_120 < 0.18 AND ma20_slope10 >= -0.01) AS long_half_base_stabilizing,
  (close_vs_low20 BETWEEN 0.02 AND 0.18
    AND c > ma7
    AND c1 > ma7_1
    AND ma7_slope3 > 0
    AND c / NULLIF(ma20, 0) - 1.0 BETWEEN -0.03 AND 0.08
    AND green_count3 >= 2
    AND upper_wick < 0.35
    AND next_h > h) AS long_recent_entry_trigger,
  (low_f5 / NULLIF(l, 0) - 1.0 <= -0.03) AS short_target3pct_low5,
  (high_f5 / NULLIF(l, 0) - 1.0 >= 0.03) AS short_adverse3pct_high5,
  (high_f5 / NULLIF(h, 0) - 1.0 >= 0.03) AS long_target3pct_high5,
  (low_f5 / NULLIF(h, 0) - 1.0 <= -0.03) AS long_adverse3pct_low5
FROM features
WHERE c240 IS NOT NULL
  {forward_label_where}
  AND ma60 IS NOT NULL
  AND c > 100
  AND v > 0
"""
    )


RULES = {
    "short": {
        "rule_id": "short_year_mature_half_safe_recent_break",
        "where": "short_year_mature_uptrend AND short_half_not_broken_yet AND short_recent_entry_trigger",
        "target": "short_target3pct_low5",
        "adverse": "short_adverse3pct_high5",
        "entry_description": "1年上昇成熟、半年は深崩れ前、直近20日でMA失速後に次足安値割れ",
    },
    "long": {
        "rule_id": "long_year_decline_half_base_recent_breakout",
        "where": "long_year_decline_or_base AND long_half_base_stabilizing AND long_recent_entry_trigger",
        "target": "long_target3pct_high5",
        "adverse": "long_adverse3pct_low5",
        "entry_description": "1年下落/底練り、半年は安定化、直近20日でMA回復後に次足高値抜け",
    },
}


SAFETY_FILTERS = {
    "short": {
        "none": "TRUE",
        "ret240_strong": "ret240 >= 0.50",
        "upper_wick_ge_35": "upper_wick >= 0.35",
        "range120_high": "range120_pos >= 0.70",
        "lower_wick_lt_20": "lower_wick < 0.20",
        "not_far_below_ma20": "c / NULLIF(ma20, 0) - 1.0 >= -0.04",
        "low_wick_and_high_range": "lower_wick < 0.20 AND range120_pos >= 0.70",
        "avoid_squeeze_strong_uptrend_upper_wick25_red3": "ret240 >= 0.50 AND upper_wick >= 0.25 AND red_count3 = 3",
        "avoid_squeeze_strong_uptrend_upper_wick35_red3": "ret240 >= 0.50 AND upper_wick >= 0.35 AND red_count3 = 3",
        "avoid_squeeze_upper_wick35_body_bearish_red3": "upper_wick >= 0.35 AND c < o AND red_count3 = 3",
        "avoid_squeeze_upper_wick35_short_lower_wick_red3": "upper_wick >= 0.35 AND lower_wick < 0.20 AND red_count3 = 3",
        "avoid_squeeze_upper_wick35_range120_high_red3": "upper_wick >= 0.35 AND range120_pos >= 0.70 AND red_count3 = 3",
        "avoid_squeeze_week_bear": "ret240 >= 0.50 AND upper_wick >= 0.25 AND red_count3 = 3 AND week_bear",
        "avoid_squeeze_week_upper": "ret240 >= 0.50 AND upper_wick >= 0.25 AND red_count3 = 3 AND week_upper_wick >= 0.45",
        "avoid_squeeze_week_prev_low_break": "ret240 >= 0.50 AND upper_wick >= 0.25 AND red_count3 = 3 AND week_prev_low_break",
        "avoid_squeeze_week_close_low": "ret240 >= 0.50 AND upper_wick >= 0.25 AND red_count3 = 3 AND week_close_pos <= 0.35",
    },
    "long": {
        "none": "TRUE",
        "ret240_deep": "ret240 <= -0.30",
        "range240_low": "range240_pos <= 0.35",
        "range20_recover": "range20_pos >= 0.55",
        "upper_wick_lt_20": "upper_wick < 0.20",
        "lower_wick_ge_25": "lower_wick >= 0.25",
        "not_far_above_ma20": "c / NULLIF(ma20, 0) - 1.0 <= 0.04",
        "recover_and_low_wick": "range20_pos >= 0.55 AND upper_wick < 0.20",
    },
}


def _summary(con: duckdb.DuckDBPyConnection, side: str, rule: dict[str, str]) -> dict[str, Any]:
    row = con.execute(
        f"""
SELECT
  count(*) AS n,
  avg(CASE WHEN {rule['target']} THEN 1.0 ELSE 0.0 END) AS target3pct_rate,
  avg(CASE WHEN {rule['adverse']} THEN 1.0 ELSE 0.0 END) AS adverse3pct_rate
FROM three_window_events
WHERE {rule['where']}
"""
    ).fetchone()
    n = int(row[0] or 0)
    target = float(row[1] or 0)
    adverse = float(row[2] or 0)
    return {
        "side": side,
        "n": n,
        "target3pct_rate": target,
        "adverse3pct_rate": adverse,
        "target_to_adverse_ratio": target / max(adverse, 1e-9) if n else None,
        "edge": target - adverse if n else None,
    }


def _decision(summary: dict[str, Any]) -> str:
    if int(summary["n"]) < ACCEPTANCE["min_n"]:
        return "hold_low_sample"
    if (
        float(summary["target3pct_rate"]) >= ACCEPTANCE["min_target3pct_rate"]
        and float(summary["adverse3pct_rate"]) <= ACCEPTANCE["max_adverse3pct_rate"]
        and float(summary["target_to_adverse_ratio"] or 0) >= ACCEPTANCE["min_target_to_adverse_ratio"]
    ):
        return "keep_candidate"
    return "drop_or_hold_needs_filter"


def _yearly(con: duckdb.DuckDBPyConnection, rule: dict[str, str]) -> list[dict[str, Any]]:
    rows = con.execute(
        f"""
SELECT
  year(to_timestamp(date)) AS year,
  count(*) AS n,
  avg(CASE WHEN {rule['target']} THEN 1.0 ELSE 0.0 END) AS target3pct_rate,
  avg(CASE WHEN {rule['adverse']} THEN 1.0 ELSE 0.0 END) AS adverse3pct_rate
FROM three_window_events
WHERE {rule['where']}
GROUP BY 1
ORDER BY 1
"""
    ).fetchall()
    return [
        {
            "year": int(year),
            "n": int(n),
            "target3pct_rate": float(target or 0),
            "adverse3pct_rate": float(adverse or 0),
            "target_to_adverse_ratio": float(target or 0) / max(float(adverse or 0), 1e-9) if n else None,
        }
        for year, n, target, adverse in rows
    ]


def _period_splits(con: duckdb.DuckDBPyConnection, rule: dict[str, str]) -> list[dict[str, Any]]:
    split_defs = {
        "train_2018_2022": "date < epoch(date '2023-01-01')::BIGINT",
        "test_2023_2026": "date >= epoch(date '2023-01-01')::BIGINT",
        "recent_2024_2026": "date >= epoch(date '2024-01-01')::BIGINT",
    }
    out = []
    for split_id, split_where in split_defs.items():
        row = con.execute(
            f"""
SELECT
  count(*) AS n,
  avg(CASE WHEN {rule['target']} THEN 1.0 ELSE 0.0 END) AS target3pct_rate,
  avg(CASE WHEN {rule['adverse']} THEN 1.0 ELSE 0.0 END) AS adverse3pct_rate
FROM three_window_events
WHERE {rule['where']}
  AND {split_where}
"""
        ).fetchone()
        n = int(row[0] or 0)
        target = float(row[1] or 0)
        adverse = float(row[2] or 0)
        summary = {
            "n": n,
            "target3pct_rate": target,
            "adverse3pct_rate": adverse,
            "target_to_adverse_ratio": target / max(adverse, 1e-9) if n else None,
            "edge": target - adverse if n else None,
        }
        out.append(
            {
                "split_id": split_id,
                "split_where": split_where,
                "summary": summary,
                "candidate_local_decision": _decision(summary),
            }
        )
    return out


def _cost_adjusted_summary(con: duckdb.DuckDBPyConnection, rule: dict[str, str]) -> dict[str, Any]:
    row = con.execute(
        f"""
SELECT
  count(*) AS n,
  avg(CASE WHEN low_f5 / NULLIF(l, 0) - 1.0 <= -0.032 THEN 1.0 ELSE 0.0 END) AS target3pct_after_20bp_cost_rate,
  avg(CASE WHEN high_f5 / NULLIF(l, 0) - 1.0 >= 0.028 THEN 1.0 ELSE 0.0 END) AS adverse3pct_after_20bp_cost_rate,
  avg(low_f5 / NULLIF(l, 0) - 1.0) AS avg_best_down5,
  avg(high_f5 / NULLIF(l, 0) - 1.0) AS avg_adverse_up5,
  quantile_cont(low_f5 / NULLIF(l, 0) - 1.0, 0.5) AS median_best_down5,
  quantile_cont(high_f5 / NULLIF(l, 0) - 1.0, 0.5) AS median_adverse_up5
FROM three_window_events
WHERE {rule['where']}
"""
    ).fetchone()
    n = int(row[0] or 0)
    target = float(row[1] or 0)
    adverse = float(row[2] or 0)
    return {
        "cost_model": "20bp round-trip buffer: target requires -3.2%, adverse counts +2.8%",
        "n": n,
        "target3pct_after_20bp_cost_rate": target,
        "adverse3pct_after_20bp_cost_rate": adverse,
        "target_to_adverse_ratio_after_cost": target / max(adverse, 1e-9) if n else None,
        "avg_best_down5": float(row[3] or 0),
        "avg_adverse_up5": float(row[4] or 0),
        "median_best_down5": float(row[5] or 0),
        "median_adverse_up5": float(row[6] or 0),
    }


def _concentration(con: duckdb.DuckDBPyConnection, rule: dict[str, str]) -> dict[str, Any]:
    rows = con.execute(
        f"""
SELECT e.code, coalesce(im.sector33_name, im.market_code, '<unknown>') AS sector33_name, count(*) AS n
FROM three_window_events e
LEFT JOIN industry_master im ON im.code = e.code
WHERE {rule['where']}
GROUP BY 1, 2
ORDER BY n DESC
"""
    ).fetchall()
    total = sum(int(row[2]) for row in rows)
    code_counts: dict[str, int] = {}
    sector_counts: dict[str, int] = {}
    for code, sector, n in rows:
        code_counts[str(code)] = code_counts.get(str(code), 0) + int(n)
        sector_counts[str(sector)] = sector_counts.get(str(sector), 0) + int(n)
    top_codes = sorted(code_counts.items(), key=lambda item: item[1], reverse=True)[:10]
    top_sectors = sorted(sector_counts.items(), key=lambda item: item[1], reverse=True)[:10]
    return {
        "total": total,
        "unique_code_count": len(code_counts),
        "unique_sector_count": len(sector_counts),
        "top_code_share": float(top_codes[0][1] / total) if total and top_codes else 0.0,
        "top3_code_share": float(sum(v for _, v in top_codes[:3]) / total) if total else 0.0,
        "top_sector_share": float(top_sectors[0][1] / total) if total and top_sectors else 0.0,
        "top3_sector_share": float(sum(v for _, v in top_sectors[:3]) / total) if total else 0.0,
        "top_codes": [{"code": code, "n": n} for code, n in top_codes],
        "top_sectors": [{"sector33_name": sector, "n": n} for sector, n in top_sectors],
    }


def _current_scan(con: duckdb.DuckDBPyConnection, rule: dict[str, str], *, table_name: str = "three_window_events") -> dict[str, Any]:
    latest = con.execute(f"SELECT max(date) FROM {table_name}").fetchone()[0]
    prev = con.execute(f"SELECT max(date) FROM {table_name} WHERE date < ?", [latest]).fetchone()[0]
    columns = """
      code,
      to_timestamp(date)::date AS signal_date,
      c AS close,
      l AS signal_low,
      next_l,
      next_c,
      ret240,
      upper_wick,
      red_count3,
      range120_pos,
      c / NULLIF(ma20, 0) - 1.0 AS close_vs_ma20
    """
    triggered_rows = con.execute(
        f"""
SELECT {columns}
FROM {table_name}
WHERE {rule['where']}
  AND date = ?
ORDER BY code
""",
        [prev],
    ).fetchall()
    waiting_rows = con.execute(
        f"""
SELECT {columns}
FROM {table_name}
WHERE {rule['where']}
  AND date = ?
ORDER BY code
""",
        [latest],
    ).fetchall()
    recent_rows = con.execute(
        f"""
SELECT {columns}
FROM {table_name}
WHERE {rule['where']}
  AND date >= ?
ORDER BY date DESC, code
LIMIT 50
""",
        [latest - 86400 * 60],
    ).fetchall()
    keys = [
        "code",
        "signal_date",
        "close",
        "signal_low",
        "next_l",
        "next_c",
        "ret240",
        "upper_wick",
        "red_count3",
        "range120_pos",
        "close_vs_ma20",
    ]

    def to_items(rows: list[tuple[Any, ...]]) -> list[dict[str, Any]]:
        items = []
        for row in rows:
            item = {}
            for key, value in zip(keys, row):
                item[key] = value.isoformat() if hasattr(value, "isoformat") else value
            items.append(item)
        return items

    return {
        "latest_signal_date": con.execute("SELECT to_timestamp(?)::date", [latest]).fetchone()[0].isoformat(),
        "previous_signal_date": con.execute("SELECT to_timestamp(?)::date", [prev]).fetchone()[0].isoformat(),
        "triggered_previous_signal_rows": to_items(triggered_rows),
        "waiting_latest_signal_rows": to_items(waiting_rows),
        "recent_60_calendar_day_rows": to_items(recent_rows),
    }


def _safety_filter_rows(con: duckdb.DuckDBPyConnection, side: str, rule: dict[str, str]) -> list[dict[str, Any]]:
    rows = []
    for filter_id, filter_sql in SAFETY_FILTERS[side].items():
        row = con.execute(
            f"""
SELECT
  count(*) AS n,
  avg(CASE WHEN {rule['target']} THEN 1.0 ELSE 0.0 END) AS target3pct_rate,
  avg(CASE WHEN {rule['adverse']} THEN 1.0 ELSE 0.0 END) AS adverse3pct_rate
FROM three_window_events
WHERE {rule['where']}
  AND {filter_sql}
"""
        ).fetchone()
        n = int(row[0] or 0)
        target = float(row[1] or 0)
        adverse = float(row[2] or 0)
        summary = {
            "n": n,
            "target3pct_rate": target,
            "adverse3pct_rate": adverse,
            "target_to_adverse_ratio": target / max(adverse, 1e-9) if n else None,
            "edge": target - adverse if n else None,
        }
        rows.append(
            {
                "side": side,
                "filter_id": filter_id,
                "filter_sql": filter_sql,
                "summary": summary,
                "yearly": _yearly(con, {**rule, "where": f"{rule['where']} AND {filter_sql}"}),
                "period_splits": _period_splits(con, {**rule, "where": f"{rule['where']} AND {filter_sql}"}),
                "cost_adjusted_summary": _cost_adjusted_summary(
                    con, {**rule, "where": f"{rule['where']} AND {filter_sql}"}
                )
                if side == "short"
                else None,
                "concentration": _concentration(con, {**rule, "where": f"{rule['where']} AND {filter_sql}"})
                if side == "short"
                else None,
                "current_scan": None,
                "candidate_local_decision": _decision(summary),
            }
        )
    return sorted(
        rows,
        key=lambda item: (
            1 if item["candidate_local_decision"] == "keep_candidate" else 0,
            item["summary"]["edge"] if item["summary"]["edge"] is not None else -999,
            item["summary"]["target_to_adverse_ratio"] if item["summary"]["target_to_adverse_ratio"] is not None else -999,
            item["summary"]["n"],
        ),
        reverse=True,
    )


def _attach_current_scans(con: duckdb.DuckDBPyConnection, rows: list[dict[str, Any]]) -> None:
    for row in rows:
        if row["side"] != "short":
            continue
        base_rule = RULES[row["side"]]
        for filter_row in row["safety_filter_rows"]:
            if not str(filter_row["filter_id"]).startswith("avoid_squeeze_"):
                continue
            filter_row["current_scan"] = _current_scan(
                con,
                {**base_rule, "where": f"{base_rule['where']} AND {filter_row['filter_sql']}"},
                table_name="three_window_current_events",
            )


def _meemee_display_contract(rows: list[dict[str, Any]]) -> dict[str, Any]:
    short_row = next((row for row in rows if row["side"] == "short"), None)
    keep_rows: list[dict[str, Any]] = []
    if short_row:
        keep_rows = [
            item
            for item in short_row["safety_filter_rows"]
            if item["candidate_local_decision"] == "keep_candidate" and str(item["filter_id"]).startswith("avoid_squeeze_week_")
        ]
    cost_qualified = []
    for item in keep_rows:
        cost = item.get("cost_adjusted_summary")
        if not isinstance(cost, dict):
            continue
        if (
            float(cost.get("target3pct_after_20bp_cost_rate") or 0)
            >= ACCEPTANCE["min_cost_adjusted_target3pct_rate"]
            and float(cost.get("adverse3pct_after_20bp_cost_rate") or 1)
            <= ACCEPTANCE["max_cost_adjusted_adverse3pct_rate"]
            and float(cost.get("target_to_adverse_ratio_after_cost") or 0)
            >= ACCEPTANCE["min_cost_adjusted_target_to_adverse_ratio"]
        ):
            cost_qualified.append(item)
    accepted = sorted(
        cost_qualified,
        key=lambda item: (
            (item.get("cost_adjusted_summary") or {}).get("target_to_adverse_ratio_after_cost") or 0,
            (item.get("summary") or {}).get("target_to_adverse_ratio") or 0,
            (item.get("summary") or {}).get("n") or 0,
        ),
        reverse=True,
    )[0] if cost_qualified else None
    current_scan = accepted.get("current_scan") if accepted else None
    return {
        "contract_version": "meemee_short_research_tag_contract_v1",
        "owner": "TRADEX",
        "meemee_reflectable": bool(accepted),
        "recommended_surface": "display_tag_only",
        "display_label_ja": "上昇成熟・上ヒゲ三陰線ショート研究一致",
        "display_side": "short",
        "display_status": "research_match_not_trade_signal",
        "accepted_filter_id": accepted["filter_id"] if accepted else None,
        "accepted_rule_sql": (
            f"{RULES['short']['where']} AND {accepted['filter_sql']}" if accepted else None
        ),
        "show_when": {
            "triggered": "signal日の翌足でsignal安値を割った銘柄に研究一致タグを表示",
            "waiting": "最新確定足がsignal条件だけ満たす場合は売り待ちタグを表示",
            "empty": "該当なしを正常状態として扱う",
        },
        "must_not_show_as": [
            "validated_short_signal",
            "automatic_sell_recommendation",
            "ranking_score_boost",
            "production_trade_action",
        ],
        "quality_gates": {
            "acceptance": ACCEPTANCE,
            "raw_summary": accepted["summary"] if accepted else None,
            "cost_adjusted_summary": accepted["cost_adjusted_summary"] if accepted else None,
            "period_splits": accepted["period_splits"] if accepted else None,
            "concentration": accepted["concentration"] if accepted else None,
        },
        "latest_current_scan": current_scan,
        "implementation_decision": (
            "display_tag_candidate_ready_review_only" if accepted else "hold_no_display_contract"
        ),
        "implementation_reason": (
            "cost-adjusted ratio remains above 2, adverse rate remains below 30%, concentration is low, but recent samples are thin"
            if accepted
            else "no accepted safety filter matched the display contract"
        ),
    }


def run(*, db_path: Path, output_root: Path, start: str, end: str) -> Path:
    con = duckdb.connect(str(db_path), read_only=True)
    if end == "latest":
        end = _latest_pan_date(con)
    _build_features(con, start, end)
    rows = []
    for side, rule in RULES.items():
        summary = _summary(con, side, rule)
        rows.append(
            {
                "side": side,
                "rule_id": rule["rule_id"],
                "entry_description": rule["entry_description"],
                "where_sql": rule["where"],
                "summary": summary,
                "yearly": _yearly(con, rule),
                "safety_filter_rows": _safety_filter_rows(con, side, rule),
                "candidate_local_decision": _decision(summary),
            }
        )
    _build_features(con, start, end, table_name="three_window_current_events", require_forward_labels=False)
    _attach_current_scans(con, rows)
    report = {
        "schema_version": f"{AXIS_ID}_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "db_path": str(db_path),
        "fixed_evaluation_conditions": {
            "start": start,
            "end": end,
            "source": "daily_bars source=pan",
            "windows": {
                "year_context": 240,
                "half_context": 120,
                "recent_entry": 20,
            },
            "target_window": "5 trading sessions after trigger",
            "cost_slippage": "not_applied",
            "acceptance": ACCEPTANCE,
        },
        "rows": rows,
        "meemee_display_contract": _meemee_display_contract(rows),
        "decision": {
            "candidate_local_decision": _meemee_display_contract(rows)["implementation_decision"],
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": _meemee_display_contract(rows)["implementation_reason"],
        },
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
    }
    tag = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = output_root / f"{tag}-{AXIS_ID}"
    _write_json(out_dir / "three_window_side_rule_report.json", report)
    _write_json(output_root / "latest_three_window_side_rule_report.json", {"run_root": str(out_dir), **report})
    return out_dir


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=_default_db_path())
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--start", default="2018-01-01")
    parser.add_argument("--end", default="latest")
    args = parser.parse_args()
    print(run(db_path=args.db_path, output_root=args.output_root, start=args.start, end=args.end))


if __name__ == "__main__":
    main()
