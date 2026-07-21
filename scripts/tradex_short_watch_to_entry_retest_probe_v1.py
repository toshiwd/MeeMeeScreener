from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.tradex_three_window_side_rule_probe_v1 import (
    RULES,
    SAFETY_FILTERS,
    _build_features,
    _default_db_path,
    _latest_pan_date,
)


AXIS_ID = "short_watch_to_entry_retest_probe_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_watch_to_entry_retest_probe_v1")
BASE_FILTER_ID = "avoid_squeeze_week_prev_low_break"


ENTRY_RULES = {
    "failed_rebound_close_below_ma7": {
        "description": "発現後2-10本で、いったん1.5%以上戻したが発現足高値を越えられず、陰線でMA7下に戻る",
        "where": """
            rn BETWEEN 2 AND 10
            AND max_high_to_entry <= signal_h * 1.005
            AND max_close_before_entry >= signal_c * 1.015
            AND entry_c < entry_o
            AND entry_c < entry_ma7
        """,
    },
    "failed_rebound_prior_low_break": {
        "description": "発現後2-10本で、いったん1.5%以上戻したが発現足高値を越えられず、前日安値を割る",
        "where": """
            rn BETWEEN 2 AND 10
            AND max_high_to_entry <= signal_h * 1.005
            AND max_close_before_entry >= signal_c * 1.015
            AND entry_l < entry_l1
        """,
    },
    "strict_three_day_rebound_fail": {
        "description": "発現後3-6本で戻しても発現足高値を越えられず、陰線かつMA7下",
        "where": """
            rn BETWEEN 3 AND 6
            AND max_high_to_entry <= signal_h * 1.005
            AND max_close_before_entry >= signal_c * 1.01
            AND entry_c < entry_o
            AND entry_c < entry_ma7
        """,
    },
    "ma7_ma20_double_reject": {
        "description": "発現後2-10本で戻りを入れた後、MA7/MA20の下で陰線失速",
        "where": """
            rn BETWEEN 2 AND 10
            AND max_close_before_entry >= signal_c * 1.015
            AND entry_c < entry_o
            AND entry_c < entry_ma7
            AND entry_c < entry_ma20
        """,
    },
}

BLOWOFF_ENTRY_RULES = {
    "post_blowoff_failed_high_low_break": {
        "description": "売り監視発現後20本以内に8%以上吹き上げ、その後ピーク高値を越えられず、ピーク後安値を割ってMA7下に落ちる",
        "where": """
            rn_after_peak BETWEEN 2 AND 15
            AND peak_gain_pct >= 0.08
            AND max_high_after_peak_to_entry <= peak_h * 1.005
            AND entry_l < low_after_peak_before_entry
            AND entry_c < entry_ma7
        """,
    },
    "post_blowoff_failed_high_ma20_break": {
        "description": "売り監視発現後20本以内に8%以上吹き上げ、その後ピーク高値を越えられず、MA20下に落ちる",
        "where": """
            rn_after_peak BETWEEN 2 AND 20
            AND peak_gain_pct >= 0.08
            AND max_high_after_peak_to_entry <= peak_h * 1.005
            AND entry_c < entry_ma20
        """,
    },
    "post_blowoff_fast_ma7_fail": {
        "description": "売り監視発現後20本以内に8%以上吹き上げ、ピーク後2-7本で高値更新できずMA7下に失速",
        "where": """
            rn_after_peak BETWEEN 2 AND 7
            AND peak_gain_pct >= 0.08
            AND max_high_after_peak_to_entry <= peak_h * 1.005
            AND entry_c < entry_ma7
        """,
    },
    "post_blowoff_fast_low_break_ma7_fail": {
        "description": "売り監視発現後20本以内に8%以上吹き上げ、ピーク後2-7本で高値更新できずピーク後安値割れかつMA7下",
        "where": """
            rn_after_peak BETWEEN 2 AND 7
            AND peak_gain_pct >= 0.08
            AND max_high_after_peak_to_entry <= peak_h * 1.005
            AND entry_l < low_after_peak_before_entry
            AND entry_c < entry_ma7
        """,
    },
    "post_blowoff_fast_low_break_ma7_fail_not_too_far_ma20": {
        "description": "短期吹き上げ失速に、MA20から上に離れすぎていない条件を追加",
        "where": """
            rn_after_peak BETWEEN 2 AND 7
            AND peak_gain_pct >= 0.08
            AND max_high_after_peak_to_entry <= peak_h * 1.005
            AND entry_l < low_after_peak_before_entry
            AND entry_c < entry_ma7
            AND entry_c / NULLIF(entry_ma20, 0) - 1.0 <= 0.08
        """,
    },
    "post_blowoff_fast_ma7_fail_peak_stop_safe": {
        "description": "短期吹き上げ失速後、20本以内にピーク高値を再突破しない",
        "where": """
            rn_after_peak BETWEEN 2 AND 7
            AND peak_gain_pct >= 0.08
            AND max_high_after_peak_to_entry <= peak_h * 1.005
            AND entry_c < entry_ma7
            AND high_f20 <= peak_h * 1.005
        """,
    },
    "post_blowoff_fast_ma7_fail_peak_stop_reward_ge_1": {
        "description": "短期吹き上げ失速後、ピーク高値を損切りに置いたリスクリワードが1以上",
        "where": """
            rn_after_peak BETWEEN 2 AND 7
            AND peak_gain_pct >= 0.08
            AND max_high_after_peak_to_entry <= peak_h * 1.005
            AND entry_c < entry_ma7
            AND high_f20 <= peak_h * 1.005
            AND (entry_c - low_f20) >= (peak_h - entry_c)
        """,
    },
    "post_peak_fast_ma7_fail_any_peak": {
        "description": "吹き上げ率を問わず、ピーク後2-7本で高値更新できずMA7下に失速",
        "where": """
            rn_after_peak BETWEEN 2 AND 7
            AND max_high_after_peak_to_entry <= peak_h * 1.005
            AND entry_c < entry_ma7
        """,
    },
    "post_peak_fast_ma7_fail_signal_low_rr_ge_1": {
        "description": "吹き上げ率を問わず、ピーク高値損切りと発現足安値目標のRRが1以上",
        "where": """
            rn_after_peak BETWEEN 2 AND 7
            AND max_high_after_peak_to_entry <= peak_h * 1.005
            AND entry_c < entry_ma7
            AND (entry_c - signal_l) >= (peak_h - entry_c)
        """,
    },
    "post_peak_fast_ma7_fail_low20_rr_ge_1": {
        "description": "吹き上げ率を問わず、ピーク高値損切りと直近20日安値目標のRRが1以上",
        "where": """
            rn_after_peak BETWEEN 2 AND 7
            AND max_high_after_peak_to_entry <= peak_h * 1.005
            AND entry_c < entry_ma7
            AND (entry_c - low20) / NULLIF(peak_h - entry_c, 0) >= 1.0
        """,
    },
    "post_peak_fast_ma7_fail_low20_rr_ge_1_5": {
        "description": "吹き上げ率を問わず、ピーク高値損切りと直近20日安値目標のRRが1.5以上",
        "where": """
            rn_after_peak BETWEEN 2 AND 7
            AND max_high_after_peak_to_entry <= peak_h * 1.005
            AND entry_c < entry_ma7
            AND (entry_c - low20) / NULLIF(peak_h - entry_c, 0) >= 1.5
        """,
    },
    "post_peak_fast_ma7_fail_low20_rr_ge_1_5_drop_weak_above_ma20": {
        "description": "高値圏ピーク失速。RR1.5以上、かつMA20上で出来高が弱い上昇継続押し目を除外",
        "where": """
            rn_after_peak BETWEEN 2 AND 7
            AND max_high_after_peak_to_entry <= peak_h * 1.005
            AND entry_c < entry_ma7
            AND (entry_c - low20) / NULLIF(peak_h - entry_c, 0) >= 1.5
            AND NOT (
                entry_c / NULLIF(entry_ma20, 0) - 1.0 >= 0.03
                AND entry_v / NULLIF(peak_v, 0) <= 0.60
            )
        """,
    },
    "post_peak_fast_ma7_fail_low20_rr_ge_1_5_drop_weak_signal_volume": {
        "description": "高値圏ピーク失速。RR1.5以上、MA20上の弱出来高押し目と、signal比で出来高が弱い戻りを除外",
        "where": """
            rn_after_peak BETWEEN 2 AND 7
            AND max_high_after_peak_to_entry <= peak_h * 1.005
            AND entry_c < entry_ma7
            AND (entry_c - low20) / NULLIF(peak_h - entry_c, 0) >= 1.5
            AND NOT (
                entry_c / NULLIF(entry_ma20, 0) - 1.0 >= 0.03
                AND entry_v / NULLIF(peak_v, 0) <= 0.60
            )
            AND NOT (
                peak_v / NULLIF(signal_v, 0) <= 0.60
                AND entry_v / NULLIF(signal_v, 0) <= 0.70
            )
        """,
    },
    "post_peak_fast_ma7_fail_low20_rr_ge_1_5_drop_midrange_near_high": {
        "description": "High-zone peak failure. RR>=1.5, weak-volume retests removed, and midrange near-high non-breakdowns removed",
        "where": """
            rn_after_peak BETWEEN 2 AND 7
            AND max_high_after_peak_to_entry <= peak_h * 1.005
            AND entry_c < entry_ma7
            AND (entry_c - low20) / NULLIF(peak_h - entry_c, 0) >= 1.5
            AND NOT (
                entry_c / NULLIF(entry_ma20, 0) - 1.0 >= 0.03
                AND entry_v / NULLIF(peak_v, 0) <= 0.60
            )
            AND NOT (
                peak_v / NULLIF(signal_v, 0) <= 0.60
                AND entry_v / NULLIF(signal_v, 0) <= 0.70
            )
            AND NOT (
                (entry_c - entry_low60) / NULLIF(entry_high60 - entry_low60, 0) BETWEEN 0.55 AND 0.70
                AND entry_c / NULLIF(entry_high60, 0) - 1.0 BETWEEN -0.07 AND -0.03
            )
        """,
    },
    "post_peak_fast_ma7_fail_low20_rr_ge_1_5_drop_midrange_weak_entry_volume": {
        "description": "High-zone peak failure. RR>=1.5, near-high non-breakdowns removed, and midrange weak-entry-volume retests removed",
        "where": """
            rn_after_peak BETWEEN 2 AND 7
            AND max_high_after_peak_to_entry <= peak_h * 1.005
            AND entry_c < entry_ma7
            AND (entry_c - low20) / NULLIF(peak_h - entry_c, 0) >= 1.5
            AND NOT (
                entry_c / NULLIF(entry_ma20, 0) - 1.0 >= 0.03
                AND entry_v / NULLIF(peak_v, 0) <= 0.60
            )
            AND NOT (
                peak_v / NULLIF(signal_v, 0) <= 0.60
                AND entry_v / NULLIF(signal_v, 0) <= 0.70
            )
            AND NOT (
                (entry_c - entry_low60) / NULLIF(entry_high60 - entry_low60, 0) BETWEEN 0.55 AND 0.70
                AND entry_c / NULLIF(entry_high60, 0) - 1.0 BETWEEN -0.07 AND -0.03
            )
            AND NOT (
                (entry_c - entry_low60) / NULLIF(entry_high60 - entry_low60, 0) BETWEEN 0.35 AND 0.50
                AND entry_v / NULLIF(peak_v, 0) <= 0.60
                AND entry_v / NULLIF(signal_v, 0) <= 0.60
                AND entry_c / NULLIF(entry_high60, 0) - 1.0 <= -0.07
            )
        """,
    },
    "post_peak_fast_ma7_fail_low20_rr_ge_1_3_drop_midrange_weak_entry_volume": {
        "description": "High-zone peak failure. RR>=1.3, near-high non-breakdowns removed, and midrange weak-entry-volume retests removed",
        "where": """
            rn_after_peak BETWEEN 2 AND 7
            AND max_high_after_peak_to_entry <= peak_h * 1.005
            AND entry_c < entry_ma7
            AND (entry_c - low20) / NULLIF(peak_h - entry_c, 0) >= 1.3
            AND NOT (
                entry_c / NULLIF(entry_ma20, 0) - 1.0 >= 0.03
                AND entry_v / NULLIF(peak_v, 0) <= 0.60
            )
            AND NOT (
                peak_v / NULLIF(signal_v, 0) <= 0.60
                AND entry_v / NULLIF(signal_v, 0) <= 0.70
            )
            AND NOT (
                (entry_c - entry_low60) / NULLIF(entry_high60 - entry_low60, 0) BETWEEN 0.55 AND 0.70
                AND entry_c / NULLIF(entry_high60, 0) - 1.0 BETWEEN -0.07 AND -0.03
            )
            AND NOT (
                (entry_c - entry_low60) / NULLIF(entry_high60 - entry_low60, 0) BETWEEN 0.35 AND 0.50
                AND entry_v / NULLIF(peak_v, 0) <= 0.60
                AND entry_v / NULLIF(signal_v, 0) <= 0.60
                AND entry_c / NULLIF(entry_high60, 0) - 1.0 <= -0.07
            )
        """,
    },
    "post_peak_fast_ma7_fail_low20_rr_ge_1_1_drop_midrange_weak_entry_volume": {
        "description": "High-zone peak failure. RR>=1.1, near-high non-breakdowns removed, and midrange weak-entry-volume retests removed",
        "where": """
            rn_after_peak BETWEEN 2 AND 7
            AND max_high_after_peak_to_entry <= peak_h * 1.005
            AND entry_c < entry_ma7
            AND (entry_c - low20) / NULLIF(peak_h - entry_c, 0) >= 1.1
            AND NOT (
                entry_c / NULLIF(entry_ma20, 0) - 1.0 >= 0.03
                AND entry_v / NULLIF(peak_v, 0) <= 0.60
            )
            AND NOT (
                peak_v / NULLIF(signal_v, 0) <= 0.60
                AND entry_v / NULLIF(signal_v, 0) <= 0.70
            )
            AND NOT (
                (entry_c - entry_low60) / NULLIF(entry_high60 - entry_low60, 0) BETWEEN 0.55 AND 0.70
                AND entry_c / NULLIF(entry_high60, 0) - 1.0 BETWEEN -0.07 AND -0.03
            )
            AND NOT (
                (entry_c - entry_low60) / NULLIF(entry_high60 - entry_low60, 0) BETWEEN 0.35 AND 0.50
                AND entry_v / NULLIF(peak_v, 0) <= 0.60
                AND entry_v / NULLIF(signal_v, 0) <= 0.60
                AND entry_c / NULLIF(entry_high60, 0) - 1.0 <= -0.07
            )
        """,
    },
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _json_value(value: Any) -> Any:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def _build_entry_candidates(con: duckdb.DuckDBPyConnection) -> None:
    base_rule = RULES["short"]
    base_filter = SAFETY_FILTERS["short"][BASE_FILTER_ID]
    con.execute(
        f"""
CREATE OR REPLACE TEMP TABLE short_watch_signals AS
SELECT
  code,
  date AS signal_date,
  o AS signal_o,
  h AS signal_h,
  l AS signal_l,
  c AS signal_c,
  v AS signal_v,
  ma7 AS signal_ma7,
  ma20 AS signal_ma20,
  ret240,
  upper_wick,
  red_count3,
  week_l,
  prev_week_l
FROM three_window_events
WHERE {base_rule["where"]}
  AND {base_filter}
"""
    )
    con.execute(
        """
CREATE OR REPLACE TEMP TABLE short_watch_blowoff_entry_candidates AS
WITH future20 AS (
  SELECT
    s.*,
    e.date,
    e.o,
    e.h,
    e.l,
    e.c,
    e.v,
    e.ma7,
    e.ma20,
    e.high60,
    e.low60,
    row_number() OVER (PARTITION BY s.code, s.signal_date ORDER BY e.date) AS rn_from_signal
  FROM short_watch_signals s
  JOIN three_window_events e
    ON e.code = s.code
   AND e.date > s.signal_date
   AND e.date <= s.signal_date + 86400 * 40
),
peaks AS (
  SELECT *
  FROM future20
  WHERE rn_from_signal <= 20
  QUALIFY row_number() OVER (PARTITION BY code, signal_date ORDER BY h DESC, date) = 1
),
after_peak AS (
  SELECT
    p.code,
    p.signal_date,
    p.signal_o,
    p.signal_h,
    p.signal_l,
    p.signal_c,
    p.signal_v,
    p.signal_ma7,
    p.signal_ma20,
    p.ret240,
    p.upper_wick,
    p.red_count3,
    p.date AS peak_date,
    p.h AS peak_h,
    p.c AS peak_c,
    p.v AS peak_v,
    p.h / NULLIF(p.signal_c, 0) - 1.0 AS peak_gain_pct,
    e.date AS entry_date,
    e.o AS entry_o,
    e.h AS entry_h,
    e.l AS entry_l,
    e.c AS entry_c,
    e.v AS entry_v,
    e.ma7 AS entry_ma7,
    e.ma20 AS entry_ma20,
    e.high60 AS entry_high60,
    e.low60 AS entry_low60,
    e.ma7_slope3 AS entry_ma7_slope3,
    e.ma20_slope10 AS entry_ma20_slope10,
    e.low20 AS low20,
    row_number() OVER (PARTITION BY p.code, p.signal_date ORDER BY e.date) AS rn_after_peak,
    max(e.h) OVER (
      PARTITION BY p.code, p.signal_date
      ORDER BY e.date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS max_high_after_peak_to_entry,
    min(e.l) OVER (
      PARTITION BY p.code, p.signal_date
      ORDER BY e.date
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS low_after_peak_before_entry
  FROM peaks p
  JOIN three_window_events e
    ON e.code = p.code
   AND e.date > p.date
   AND e.date <= p.date + 86400 * 45
),
future_labeled AS (
  SELECT
    a.*,
    min(d.l) FILTER (WHERE d.date > a.entry_date) AS low_f5,
    max(d.h) FILTER (WHERE d.date > a.entry_date) AS high_f5,
    min(d.l) FILTER (WHERE d.date > a.entry_date AND d.date <= a.entry_date + 86400 * 20) AS low_f20,
    max(d.h) FILTER (WHERE d.date > a.entry_date AND d.date <= a.entry_date + 86400 * 20) AS high_f20
  FROM after_peak a
  JOIN daily_bars d
    ON d.source = 'pan'
   AND d.code = a.code
   AND d.date > a.entry_date
   AND d.date <= a.entry_date + 86400 * 40
  WHERE a.rn_after_peak <= 20
  GROUP BY ALL
)
SELECT *
FROM future_labeled
WHERE low_f5 IS NOT NULL
  AND high_f5 IS NOT NULL
  AND low_f20 IS NOT NULL
  AND high_f20 IS NOT NULL
"""
    )
    con.execute(
        """
CREATE OR REPLACE TEMP TABLE short_watch_entry_candidates AS
WITH future_rows AS (
  SELECT
    s.*,
    e.date AS entry_date,
    e.o AS entry_o,
    e.h AS entry_h,
    e.l AS entry_l,
    e.c AS entry_c,
    e.l1 AS entry_l1,
    e.ma7 AS entry_ma7,
    e.ma20 AS entry_ma20,
    row_number() OVER (PARTITION BY s.code, s.signal_date ORDER BY e.date) AS rn,
    max(e.h) OVER (
      PARTITION BY s.code, s.signal_date
      ORDER BY e.date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS max_high_to_entry,
    max(e.c) OVER (
      PARTITION BY s.code, s.signal_date
      ORDER BY e.date
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS max_close_before_entry
  FROM short_watch_signals s
  JOIN three_window_events e
    ON e.code = s.code
   AND e.date > s.signal_date
   AND e.date <= s.signal_date + 86400 * 20
),
future_labeled AS (
  SELECT
    f.*,
    min(d.l) FILTER (WHERE d.date > f.entry_date) AS low_f5,
    max(d.h) FILTER (WHERE d.date > f.entry_date) AS high_f5,
    min(d.l) FILTER (WHERE d.date > f.entry_date AND d.date <= f.entry_date + 86400 * 20) AS low_f20,
    max(d.h) FILTER (WHERE d.date > f.entry_date AND d.date <= f.entry_date + 86400 * 20) AS high_f20
  FROM future_rows f
  JOIN daily_bars d
    ON d.source = 'pan'
   AND d.code = f.code
   AND d.date > f.entry_date
   AND d.date <= f.entry_date + 86400 * 40
  WHERE f.rn <= 10
  GROUP BY ALL
)
SELECT *
FROM future_labeled
WHERE low_f5 IS NOT NULL
  AND high_f5 IS NOT NULL
  AND low_f20 IS NOT NULL
  AND high_f20 IS NOT NULL
"""
    )


def _summary_for_where(
    con: duckdb.DuckDBPyConnection,
    where_sql: str,
    *,
    table_name: str = "short_watch_entry_candidates",
) -> dict[str, Any]:
    has_peak_columns = table_name == "short_watch_blowoff_entry_candidates"
    peak_metric_sql = (
        """
  avg(CASE WHEN high_f5 >= peak_h * 1.005 THEN 1.0 ELSE 0.0 END) AS peak_stop_f5_rate,
  avg(CASE WHEN high_f20 >= peak_h * 1.005 THEN 1.0 ELSE 0.0 END) AS peak_stop_f20_rate,
  avg((entry_c - low_f20) / NULLIF(peak_h - entry_c, 0)) AS realized_reward_to_peak_risk20,
"""
        if has_peak_columns
        else """
  NULL AS peak_stop_f5_rate,
  NULL AS peak_stop_f20_rate,
  NULL AS realized_reward_to_peak_risk20,
"""
    )
    row = con.execute(
        f"""
WITH first_entries AS (
  SELECT *
  FROM {table_name}
  WHERE {where_sql}
  QUALIFY row_number() OVER (PARTITION BY code, signal_date ORDER BY entry_date) = 1
)
SELECT
  count(*) AS n,
  avg(CASE WHEN low_f5 / NULLIF(entry_c, 0) - 1.0 <= -0.03 THEN 1.0 ELSE 0.0 END) AS target5_down3_rate,
  avg(CASE WHEN high_f5 / NULLIF(entry_c, 0) - 1.0 >= 0.03 THEN 1.0 ELSE 0.0 END) AS adverse5_up3_rate,
  avg(CASE WHEN low_f20 / NULLIF(entry_c, 0) - 1.0 <= -0.03 THEN 1.0 ELSE 0.0 END) AS target20_down3_rate,
  avg(CASE WHEN high_f20 / NULLIF(entry_c, 0) - 1.0 >= 0.03 THEN 1.0 ELSE 0.0 END) AS adverse20_up3_rate,
{peak_metric_sql}
  avg(low_f5 / NULLIF(entry_c, 0) - 1.0) AS avg_best_down5,
  avg(high_f5 / NULLIF(entry_c, 0) - 1.0) AS avg_adverse_up5,
  avg(low_f20 / NULLIF(entry_c, 0) - 1.0) AS avg_best_down20,
  avg(high_f20 / NULLIF(entry_c, 0) - 1.0) AS avg_adverse_up20
FROM first_entries
"""
    ).fetchone()
    n = int(row[0] or 0)
    target5 = float(row[1] or 0)
    adverse5 = float(row[2] or 0)
    target20 = float(row[3] or 0)
    adverse20 = float(row[4] or 0)
    return {
        "n": n,
        "target5_down3_rate": target5,
        "adverse5_up3_rate": adverse5,
        "target5_to_adverse5_ratio": target5 / max(adverse5, 1e-9) if n else None,
        "target20_down3_rate": target20,
        "adverse20_up3_rate": adverse20,
        "target20_to_adverse20_ratio": target20 / max(adverse20, 1e-9) if n else None,
        "peak_stop_f5_rate": float(row[5] or 0),
        "peak_stop_f20_rate": float(row[6] or 0),
        "realized_reward_to_peak_risk20": float(row[7] or 0),
        "avg_best_down5": float(row[8] or 0),
        "avg_adverse_up5": float(row[9] or 0),
        "avg_best_down20": float(row[10] or 0),
        "avg_adverse_up20": float(row[11] or 0),
    }


def _yearly_for_where(
    con: duckdb.DuckDBPyConnection,
    where_sql: str,
    *,
    table_name: str = "short_watch_entry_candidates",
) -> list[dict[str, Any]]:
    has_peak_columns = table_name == "short_watch_blowoff_entry_candidates"
    peak_metric_sql = (
        """
  avg(CASE WHEN high_f20 >= peak_h * 1.005 THEN 1.0 ELSE 0.0 END) AS peak_stop_f20_rate,
  avg((entry_c - low_f20) / NULLIF(peak_h - entry_c, 0)) AS realized_reward_to_peak_risk20
"""
        if has_peak_columns
        else """
  NULL AS peak_stop_f20_rate,
  NULL AS realized_reward_to_peak_risk20
"""
    )
    rows = con.execute(
        f"""
WITH first_entries AS (
  SELECT *
  FROM {table_name}
  WHERE {where_sql}
  QUALIFY row_number() OVER (PARTITION BY code, signal_date ORDER BY entry_date) = 1
)
SELECT
  year(to_timestamp(signal_date)) AS year,
  count(*) AS n,
  avg(CASE WHEN low_f20 / NULLIF(entry_c, 0) - 1.0 <= -0.03 THEN 1.0 ELSE 0.0 END) AS target20_down3_rate,
  avg(CASE WHEN high_f20 / NULLIF(entry_c, 0) - 1.0 >= 0.03 THEN 1.0 ELSE 0.0 END) AS adverse20_up3_rate,
{peak_metric_sql}
FROM first_entries
GROUP BY 1
ORDER BY 1
"""
    ).fetchall()
    return [
        {
            "year": int(year),
            "n": int(n),
            "target20_down3_rate": float(target or 0),
            "adverse20_up3_rate": float(adverse or 0),
            "peak_stop_f20_rate": float(peak_stop or 0),
            "realized_reward_to_peak_risk20": float(peak_rr or 0),
        }
        for year, n, target, adverse, peak_stop, peak_rr in rows
    ]


def _examples_for_where(
    con: duckdb.DuckDBPyConnection,
    where_sql: str,
    *,
    start_year: int = 2026,
    table_name: str = "short_watch_entry_candidates",
) -> list[dict[str, Any]]:
    rows = con.execute(
        f"""
WITH first_entries AS (
  SELECT *
  FROM {table_name}
  WHERE {where_sql}
  QUALIFY row_number() OVER (PARTITION BY code, signal_date ORDER BY entry_date) = 1
)
SELECT
  e.code,
  coalesce(t.name, '') AS name,
  to_timestamp(e.signal_date)::date AS signal_date,
  to_timestamp(e.entry_date)::date AS entry_date,
  e.rn AS entry_days_after_signal,
  NULL::DATE AS peak_date,
  NULL::DOUBLE AS peak_gain_pct,
  e.signal_c,
  e.signal_h,
  e.entry_c,
  e.entry_ma7,
  e.entry_ma20,
  round((e.max_close_before_entry / NULLIF(e.signal_c, 0) - 1.0) * 100, 2) AS rebound_close_pct,
  round((e.max_high_to_entry / NULLIF(e.signal_h, 0) - 1.0) * 100, 2) AS high_vs_signal_high_pct,
  round((e.low_f5 / NULLIF(e.entry_c, 0) - 1.0) * 100, 2) AS best_down5_pct,
  round((e.high_f5 / NULLIF(e.entry_c, 0) - 1.0) * 100, 2) AS adverse_up5_pct,
  round((e.low_f20 / NULLIF(e.entry_c, 0) - 1.0) * 100, 2) AS best_down20_pct,
  round((e.high_f20 / NULLIF(e.entry_c, 0) - 1.0) * 100, 2) AS adverse_up20_pct
FROM first_entries e
LEFT JOIN tickers t ON t.code = e.code
WHERE year(to_timestamp(e.signal_date)) >= ?
ORDER BY e.signal_date, e.code
""",
        [start_year],
    ).fetchall()
    keys = [desc[0] for desc in con.description]
    out = []
    for row in rows:
        item = {}
        for key, value in zip(keys, row):
            item[key] = value.isoformat() if hasattr(value, "isoformat") else value
        out.append(item)
    return out


def _blowoff_examples_for_where(
    con: duckdb.DuckDBPyConnection,
    where_sql: str,
    *,
    start_year: int = 2026,
) -> list[dict[str, Any]]:
    rows = con.execute(
        f"""
WITH first_entries AS (
  SELECT *
  FROM short_watch_blowoff_entry_candidates
  WHERE {where_sql}
  QUALIFY row_number() OVER (PARTITION BY code, signal_date ORDER BY entry_date) = 1
)
SELECT
  e.code,
  coalesce(t.name, '') AS name,
  to_timestamp(e.signal_date)::date AS signal_date,
  to_timestamp(e.peak_date)::date AS peak_date,
  to_timestamp(e.entry_date)::date AS entry_date,
  e.rn_after_peak AS entry_days_after_peak,
  e.signal_c,
  e.peak_h,
  e.entry_c,
  e.entry_ma7,
  e.entry_ma20,
  round(e.peak_gain_pct * 100, 2) AS peak_gain_pct,
  round(e.entry_v / NULLIF(e.peak_v, 0), 2) AS entry_v_vs_peak_v,
  round(e.peak_v / NULLIF(e.signal_v, 0), 2) AS peak_v_vs_signal_v,
  round(e.entry_v / NULLIF(e.signal_v, 0), 2) AS entry_v_vs_signal_v,
{_shape_metrics_select_sql("e.")},
  round((e.max_high_after_peak_to_entry / NULLIF(e.peak_h, 0) - 1.0) * 100, 2) AS high_vs_peak_high_pct,
  round((e.low_f5 / NULLIF(e.entry_c, 0) - 1.0) * 100, 2) AS best_down5_pct,
  round((e.high_f5 / NULLIF(e.entry_c, 0) - 1.0) * 100, 2) AS adverse_up5_pct,
  round((e.low_f20 / NULLIF(e.entry_c, 0) - 1.0) * 100, 2) AS best_down20_pct,
  round((e.high_f20 / NULLIF(e.entry_c, 0) - 1.0) * 100, 2) AS adverse_up20_pct
FROM first_entries e
LEFT JOIN tickers t ON t.code = e.code
WHERE year(to_timestamp(e.signal_date)) >= ?
ORDER BY e.signal_date, e.code
""",
        [start_year],
    ).fetchall()
    keys = [desc[0] for desc in con.description]
    out = []
    for row in rows:
        item = {}
        for key, value in zip(keys, row):
            item[key] = value.isoformat() if hasattr(value, "isoformat") else value
        out.append(item)
    return out


def _blowoff_failure_examples_for_where(
    con: duckdb.DuckDBPyConnection,
    where_sql: str,
    *,
    limit: int = 20,
) -> list[dict[str, Any]]:
    rows = con.execute(
        f"""
WITH first_entries AS (
  SELECT *
  FROM short_watch_blowoff_entry_candidates
  WHERE {where_sql}
  QUALIFY row_number() OVER (PARTITION BY code, signal_date ORDER BY entry_date) = 1
),
failure_examples AS (
  SELECT
    e.code,
    coalesce(t.name, '') AS name,
    to_timestamp(e.signal_date)::date AS signal_date,
    to_timestamp(e.peak_date)::date AS peak_date,
    to_timestamp(e.entry_date)::date AS entry_date,
    e.rn_after_peak AS entry_days_after_peak,
    round(e.peak_gain_pct * 100, 2) AS peak_gain_pct,
    round(e.entry_v / NULLIF(e.peak_v, 0), 2) AS entry_v_vs_peak_v,
    round(e.peak_v / NULLIF(e.signal_v, 0), 2) AS peak_v_vs_signal_v,
    round(e.entry_v / NULLIF(e.signal_v, 0), 2) AS entry_v_vs_signal_v,
{_shape_metrics_select_sql("e.")},
    round((e.entry_c / NULLIF(e.signal_c, 0) - 1.0) * 100, 2) AS entry_vs_signal_pct,
    round((e.entry_c / NULLIF(e.entry_ma20, 0) - 1.0) * 100, 2) AS entry_vs_ma20_pct,
    round((e.entry_ma7 / NULLIF(e.entry_ma20, 0) - 1.0) * 100, 2) AS entry_ma7_vs_ma20_pct,
    round((e.low_f20 / NULLIF(e.entry_c, 0) - 1.0) * 100, 2) AS best_down20_pct,
    round((e.high_f20 / NULLIF(e.entry_c, 0) - 1.0) * 100, 2) AS adverse_up20_pct,
    CASE WHEN e.high_f20 >= e.peak_h * 1.005 THEN 1 ELSE 0 END AS peak_stop20,
    round((e.entry_c - e.low_f20) / NULLIF(e.peak_h - e.entry_c, 0), 2) AS realized_reward_to_peak_risk20
  FROM first_entries e
  LEFT JOIN tickers t ON t.code = e.code
  WHERE e.low_f20 / NULLIF(e.entry_c, 0) - 1.0 > -0.03
     OR e.high_f20 >= e.peak_h * 1.005
)
SELECT *
FROM failure_examples
ORDER BY peak_stop20 DESC, adverse_up20_pct DESC, signal_date, code
LIMIT ?
""",
        [limit],
    ).fetchall()
    keys = [desc[0] for desc in con.description]
    out = []
    for row in rows:
        item = {}
        for key, value in zip(keys, row):
            item[key] = value.isoformat() if hasattr(value, "isoformat") else value
        out.append(item)
    return out


def _shape_case_sql(prefix: str = "e.") -> str:
    entry_c = f"{prefix}entry_c"
    entry_low60 = f"{prefix}entry_low60"
    entry_high60 = f"{prefix}entry_high60"
    low_after_peak = f"{prefix}low_after_peak_before_entry"
    return f"""
    CASE
      WHEN {entry_c} <= {entry_low60} * 1.01 THEN 'daily_60d_box_breakdown'
      WHEN ({entry_c} - {entry_low60}) / NULLIF({entry_high60} - {entry_low60}, 0) <= 0.35 THEN 'daily_60d_lower_range'
      WHEN {entry_c} >= {low_after_peak} AND ({entry_c} - {entry_low60}) / NULLIF({entry_high60} - {entry_low60}, 0) >= 0.55 THEN 'box_not_broken_yet'
      ELSE 'middle_range_or_unclear'
    END
"""


def _shape_metrics_select_sql(prefix: str = "e.") -> str:
    return f"""
  round(({prefix}entry_c - {prefix}entry_low60) / NULLIF({prefix}entry_high60 - {prefix}entry_low60, 0), 2) AS entry_range60_pos,
  round(({prefix}entry_c / NULLIF({prefix}entry_low60, 0) - 1.0) * 100, 2) AS entry_vs_low60_pct,
  round(({prefix}entry_c / NULLIF({prefix}entry_high60, 0) - 1.0) * 100, 2) AS entry_vs_high60_pct,
  round(({prefix}entry_c / NULLIF({prefix}low_after_peak_before_entry, 0) - 1.0) * 100, 2) AS entry_vs_peak_pullback_low_pct,
  round(({prefix}peak_h * 1.005 / NULLIF({prefix}entry_c, 0) - 1.0) * 100, 2) AS peak_stop_gap_pct,
  ({prefix}peak_h * 1.005 / NULLIF({prefix}entry_c, 0) - 1.0 <= 0.03) AS tight_peak_stop,
  ({prefix}entry_l >= {prefix}low_after_peak_before_entry) AS pullback_low_not_broken,
  ({prefix}entry_l < {prefix}low_after_peak_before_entry) AS pullback_low_broken,
  ({prefix}entry_v / NULLIF({prefix}peak_v, 0) >= 0.90) AS volume_reattack,
  ({prefix}entry_v / NULLIF({prefix}peak_v, 0) <= 0.60) AS weak_retest,
  ({prefix}peak_h * 1.005 / NULLIF({prefix}entry_c, 0) - 1.0 <= 0.03
    AND {prefix}entry_v / NULLIF({prefix}peak_v, 0) > 0.60
    AND {prefix}entry_v / NULLIF({prefix}peak_v, 0) < 0.90) AS tight_stop_mid_volume,
  {_shape_case_sql(prefix)} AS shape_tag
"""


def _blowoff_shape_tag_summary_for_where(
    con: duckdb.DuckDBPyConnection,
    where_sql: str,
) -> list[dict[str, Any]]:
    rows = con.execute(
        f"""
WITH first_entries AS (
  SELECT *
  FROM short_watch_blowoff_entry_candidates
  WHERE {where_sql}
  QUALIFY row_number() OVER (PARTITION BY code, signal_date ORDER BY entry_date) = 1
),
tagged AS (
  SELECT
    *,
    {_shape_case_sql("")} AS shape_tag
  FROM first_entries
)
SELECT
  shape_tag,
  count(*) AS n,
  avg(CASE WHEN low_f20 / NULLIF(entry_c, 0) - 1.0 <= -0.05 THEN 1.0 ELSE 0.0 END) AS target5_hit_rate,
  avg(CASE WHEN high_f20 >= peak_h * 1.005 THEN 1.0 ELSE 0.0 END) AS peak_stop_hit_rate,
  avg(low_f20 / NULLIF(entry_c, 0) - 1.0) AS avg_best_down20,
  avg(high_f20 / NULLIF(entry_c, 0) - 1.0) AS avg_worst_up20
FROM tagged
GROUP BY 1
ORDER BY n DESC, shape_tag
"""
    ).fetchall()
    return [
        {
            "shape_tag": str(shape_tag),
            "n": int(n),
            "target5_hit_rate": float(target_hit or 0),
            "peak_stop_hit_rate": float(peak_stop or 0),
            "avg_best_down20": float(avg_down or 0),
            "avg_worst_up20": float(avg_up or 0),
        }
        for shape_tag, n, target_hit, peak_stop, avg_down, avg_up in rows
    ]


def _blowoff_shape_subtag_summary_for_where(
    con: duckdb.DuckDBPyConnection,
    where_sql: str,
) -> list[dict[str, Any]]:
    rows = con.execute(
        f"""
WITH first_entries AS (
  SELECT *
  FROM short_watch_blowoff_entry_candidates
  WHERE {where_sql}
  QUALIFY row_number() OVER (PARTITION BY code, signal_date ORDER BY entry_date) = 1
),
tagged AS (
  SELECT
    *,
    (peak_h * 1.005 / NULLIF(entry_c, 0) - 1.0 <= 0.03) AS tight_peak_stop,
    (entry_l >= low_after_peak_before_entry) AS pullback_low_not_broken,
    (entry_l < low_after_peak_before_entry) AS pullback_low_broken,
    (entry_v / NULLIF(peak_v, 0) >= 0.90) AS volume_reattack,
    (entry_v / NULLIF(peak_v, 0) <= 0.60) AS weak_retest,
    (
      peak_h * 1.005 / NULLIF(entry_c, 0) - 1.0 <= 0.03
      AND entry_v / NULLIF(peak_v, 0) > 0.60
      AND entry_v / NULLIF(peak_v, 0) < 0.90
    ) AS tight_stop_mid_volume
  FROM first_entries
),
expanded AS (
  SELECT 'tight_peak_stop' AS subtag, * FROM tagged WHERE tight_peak_stop
  UNION ALL
  SELECT 'pullback_low_not_broken' AS subtag, * FROM tagged WHERE pullback_low_not_broken
  UNION ALL
  SELECT 'pullback_low_broken' AS subtag, * FROM tagged WHERE pullback_low_broken
  UNION ALL
  SELECT 'volume_reattack' AS subtag, * FROM tagged WHERE volume_reattack
  UNION ALL
  SELECT 'weak_retest' AS subtag, * FROM tagged WHERE weak_retest
  UNION ALL
  SELECT 'tight_stop_mid_volume' AS subtag, * FROM tagged WHERE tight_stop_mid_volume
)
SELECT
  subtag,
  count(*) AS n,
  avg(CASE WHEN low_f20 / NULLIF(entry_c, 0) - 1.0 <= -0.05 THEN 1.0 ELSE 0.0 END) AS target5_hit_rate,
  avg(CASE WHEN high_f20 >= peak_h * 1.005 THEN 1.0 ELSE 0.0 END) AS peak_stop_hit_rate,
  avg(low_f20 / NULLIF(entry_c, 0) - 1.0) AS avg_best_down20,
  avg(high_f20 / NULLIF(entry_c, 0) - 1.0) AS avg_worst_up20
FROM expanded
GROUP BY 1
ORDER BY n DESC, subtag
"""
    ).fetchall()
    return [
        {
            "subtag": str(subtag),
            "n": int(n),
            "target5_hit_rate": float(target_hit or 0),
            "peak_stop_hit_rate": float(peak_stop or 0),
            "avg_best_down20": float(avg_down or 0),
            "avg_worst_up20": float(avg_up or 0),
        }
        for subtag, n, target_hit, peak_stop, avg_down, avg_up in rows
    ]


def _blowoff_exit_timing_for_where(
    con: duckdb.DuckDBPyConnection,
    where_sql: str,
    *,
    targets: tuple[float, ...] = (0.03, 0.05, 0.06, 0.08),
) -> list[dict[str, Any]]:
    out = []
    for target in targets:
        row = con.execute(
            f"""
WITH first_entries AS (
  SELECT *
  FROM short_watch_blowoff_entry_candidates
  WHERE {where_sql}
  QUALIFY row_number() OVER (PARTITION BY code, signal_date ORDER BY entry_date) = 1
),
path AS (
  SELECT
    e.code,
    e.signal_date,
    e.entry_date,
    e.entry_c,
    e.peak_h,
    d.date,
    d.l,
    d.h,
    row_number() OVER (PARTITION BY e.code, e.signal_date ORDER BY d.date) AS rn
  FROM first_entries e
  JOIN daily_bars d
    ON d.source = 'pan'
   AND d.code = e.code
   AND d.date > e.entry_date
   AND d.date <= e.entry_date + 86400 * 40
),
first_touch AS (
  SELECT
    code,
    signal_date,
    entry_date,
    min(CASE WHEN l / NULLIF(entry_c, 0) - 1.0 <= ? THEN rn END) AS target_rn,
    min(CASE WHEN h >= peak_h * 1.005 THEN rn END) AS stop_rn,
    min(l / NULLIF(entry_c, 0) - 1.0) AS best_down,
    max(h / NULLIF(entry_c, 0) - 1.0) AS worst_up
  FROM path
  WHERE rn <= 20
  GROUP BY code, signal_date, entry_date
)
SELECT
  count(*) AS n,
  avg(CASE WHEN target_rn IS NOT NULL THEN 1.0 ELSE 0.0 END) AS target_hit_rate,
  avg(CASE WHEN stop_rn IS NOT NULL THEN 1.0 ELSE 0.0 END) AS peak_stop_hit_rate,
  avg(CASE WHEN target_rn IS NOT NULL AND (stop_rn IS NULL OR target_rn <= stop_rn) THEN 1.0 ELSE 0.0 END) AS target_first_rate,
  avg(CASE WHEN stop_rn IS NOT NULL AND (target_rn IS NULL OR stop_rn < target_rn) THEN 1.0 ELSE 0.0 END) AS stop_first_rate,
  avg(best_down) AS avg_best_down20,
  avg(worst_up) AS avg_worst_up20
FROM first_touch
""",
            [-target],
        ).fetchone()
        out.append(
            {
                "target_down_pct": round(target * 100, 2),
                "n": int(row[0] or 0),
                "target_hit_rate": float(row[1] or 0),
                "peak_stop_hit_rate": float(row[2] or 0),
                "target_first_rate": float(row[3] or 0),
                "stop_first_rate": float(row[4] or 0),
                "avg_best_down20": float(row[5] or 0),
                "avg_worst_up20": float(row[6] or 0),
            }
        )
    return out


def _blowoff_trade_scenario_for_where(
    con: duckdb.DuckDBPyConnection,
    where_sql: str,
    *,
    target_down_pct: float = 0.05,
) -> dict[str, Any]:
    target_threshold = -target_down_pct
    base_cte = f"""
WITH first_entries AS (
  SELECT *
  FROM short_watch_blowoff_entry_candidates
  WHERE {where_sql}
  QUALIFY row_number() OVER (PARTITION BY code, signal_date ORDER BY entry_date) = 1
),
path AS (
  SELECT
    e.code,
    e.signal_date,
    e.entry_date,
    e.entry_c,
    e.peak_h,
    d.date,
    d.l,
    d.h,
    row_number() OVER (PARTITION BY e.code, e.signal_date ORDER BY d.date) AS rn
  FROM first_entries e
  JOIN daily_bars d
    ON d.source = 'pan'
   AND d.code = e.code
   AND d.date > e.entry_date
   AND d.date <= e.entry_date + 86400 * 40
),
first_touch AS (
  SELECT
    e.code,
    coalesce(t.name, '') AS name,
    e.signal_date,
    e.peak_date,
    e.entry_date,
    e.entry_c,
    e.peak_h,
    e.entry_v / NULLIF(e.peak_v, 0) AS entry_v_vs_peak_v,
    e.peak_v / NULLIF(e.signal_v, 0) AS peak_v_vs_signal_v,
    e.entry_v / NULLIF(e.signal_v, 0) AS entry_v_vs_signal_v,
    ({_shape_case_sql("e.")}) AS shape_tag,
    (e.entry_c - e.entry_low60) / NULLIF(e.entry_high60 - e.entry_low60, 0) AS entry_range60_pos,
    e.entry_c / NULLIF(e.entry_low60, 0) - 1.0 AS entry_vs_low60,
    e.entry_c / NULLIF(e.entry_high60, 0) - 1.0 AS entry_vs_high60,
    e.entry_c / NULLIF(e.low_after_peak_before_entry, 0) - 1.0 AS entry_vs_peak_pullback_low,
    e.peak_h * 1.005 / NULLIF(e.entry_c, 0) - 1.0 AS peak_stop_gap,
    e.peak_h * 1.005 / NULLIF(e.entry_c, 0) - 1.0 <= 0.03 AS tight_peak_stop,
    e.entry_l >= e.low_after_peak_before_entry AS pullback_low_not_broken,
    e.entry_l < e.low_after_peak_before_entry AS pullback_low_broken,
    e.entry_v / NULLIF(e.peak_v, 0) >= 0.90 AS volume_reattack,
    e.entry_v / NULLIF(e.peak_v, 0) <= 0.60 AS weak_retest,
    (
      e.peak_h * 1.005 / NULLIF(e.entry_c, 0) - 1.0 <= 0.03
      AND e.entry_v / NULLIF(e.peak_v, 0) > 0.60
      AND e.entry_v / NULLIF(e.peak_v, 0) < 0.90
    ) AS tight_stop_mid_volume,
    min(CASE WHEN p.l / NULLIF(e.entry_c, 0) - 1.0 <= ? THEN p.rn END) AS target_rn,
    min(CASE WHEN p.h >= e.peak_h * 1.005 THEN p.rn END) AS stop_rn,
    min(p.l / NULLIF(e.entry_c, 0) - 1.0) AS best_down20,
    max(p.h / NULLIF(e.entry_c, 0) - 1.0) AS worst_up20,
    e.peak_h * 1.005 / NULLIF(e.entry_c, 0) - 1.0 AS stop_loss_pct
  FROM first_entries e
  JOIN path p
    ON p.code = e.code
   AND p.signal_date = e.signal_date
   AND p.entry_date = e.entry_date
   AND p.rn <= 20
  LEFT JOIN tickers t ON t.code = e.code
  GROUP BY ALL
),
scenario AS (
  SELECT
    *,
    CASE
      WHEN target_rn IS NOT NULL AND (stop_rn IS NULL OR target_rn <= stop_rn) THEN 'target_first'
      WHEN stop_rn IS NOT NULL THEN 'stop_first'
      ELSE 'no_touch'
    END AS outcome,
    CASE
      WHEN target_rn IS NOT NULL AND (stop_rn IS NULL OR target_rn <= stop_rn) THEN ?
      WHEN stop_rn IS NOT NULL THEN -stop_loss_pct
      ELSE 0.0
    END AS model_return
  FROM first_touch
)
"""
    summary_row = con.execute(
        base_cte
        + """
SELECT
  count(*) AS n,
  avg(CASE WHEN outcome = 'target_first' THEN 1.0 ELSE 0.0 END) AS target_first_rate,
  avg(CASE WHEN outcome = 'stop_first' THEN 1.0 ELSE 0.0 END) AS stop_first_rate,
  avg(CASE WHEN outcome = 'no_touch' THEN 1.0 ELSE 0.0 END) AS no_touch_rate,
  avg(model_return) AS avg_model_return,
  median(model_return) AS median_model_return,
  sum(model_return) AS total_model_return,
  avg(best_down20) AS avg_best_down20,
  avg(worst_up20) AS avg_worst_up20
FROM scenario
""",
        [target_threshold, target_down_pct],
    ).fetchone()
    yearly_rows = con.execute(
        base_cte
        + """
SELECT
  year(to_timestamp(signal_date)) AS year,
  count(*) AS n,
  avg(CASE WHEN outcome = 'target_first' THEN 1.0 ELSE 0.0 END) AS target_first_rate,
  avg(CASE WHEN outcome = 'stop_first' THEN 1.0 ELSE 0.0 END) AS stop_first_rate,
  avg(model_return) AS avg_model_return
FROM scenario
GROUP BY 1
ORDER BY 1
""",
        [target_threshold, target_down_pct],
    ).fetchall()
    monthly_rows = con.execute(
        base_cte
        + """
SELECT
  strftime(to_timestamp(signal_date), '%Y-%m') AS month,
  count(*) AS n,
  avg(CASE WHEN outcome = 'target_first' THEN 1.0 ELSE 0.0 END) AS target_first_rate,
  avg(CASE WHEN outcome = 'stop_first' THEN 1.0 ELSE 0.0 END) AS stop_first_rate,
  avg(model_return) AS avg_model_return
FROM scenario
GROUP BY 1
ORDER BY 1
""",
        [target_threshold, target_down_pct],
    ).fetchall()
    examples_rows = con.execute(
        base_cte
        + """
SELECT
  code,
  name,
  to_timestamp(signal_date)::date AS signal_date,
  to_timestamp(peak_date)::date AS peak_date,
  to_timestamp(entry_date)::date AS entry_date,
  outcome,
  target_rn,
  stop_rn,
  round(model_return * 100, 2) AS model_return_pct,
  round(best_down20 * 100, 2) AS best_down20_pct,
  round(worst_up20 * 100, 2) AS worst_up20_pct,
  shape_tag,
  round(entry_range60_pos, 2) AS entry_range60_pos,
  round(entry_vs_low60 * 100, 2) AS entry_vs_low60_pct,
  round(entry_vs_high60 * 100, 2) AS entry_vs_high60_pct,
  round(entry_vs_peak_pullback_low * 100, 2) AS entry_vs_peak_pullback_low_pct,
  round(peak_stop_gap * 100, 2) AS peak_stop_gap_pct,
  tight_peak_stop,
  pullback_low_not_broken,
  pullback_low_broken,
  volume_reattack,
  weak_retest,
  tight_stop_mid_volume,
  round(entry_v_vs_peak_v, 2) AS entry_v_vs_peak_v,
  round(peak_v_vs_signal_v, 2) AS peak_v_vs_signal_v,
  round(entry_v_vs_signal_v, 2) AS entry_v_vs_signal_v
FROM scenario
WHERE year(to_timestamp(signal_date)) >= 2026
ORDER BY signal_date, code
""",
        [target_threshold, target_down_pct],
    ).fetchall()

    return {
        "target_down_pct": round(target_down_pct * 100, 2),
        "stop_policy": "peak_high_plus_0.5pct_first_touch",
        "summary": {
            "n": int(summary_row[0] or 0),
            "target_first_rate": float(summary_row[1] or 0),
            "stop_first_rate": float(summary_row[2] or 0),
            "no_touch_rate": float(summary_row[3] or 0),
            "avg_model_return": float(summary_row[4] or 0),
            "median_model_return": float(summary_row[5] or 0),
            "total_model_return": float(summary_row[6] or 0),
            "avg_best_down20": float(summary_row[7] or 0),
            "avg_worst_up20": float(summary_row[8] or 0),
        },
        "yearly": [
            {
                "year": int(year),
                "n": int(n),
                "target_first_rate": float(target_first or 0),
                "stop_first_rate": float(stop_first or 0),
                "avg_model_return": float(avg_return or 0),
            }
            for year, n, target_first, stop_first, avg_return in yearly_rows
        ],
        "monthly": [
            {
                "month": str(month),
                "n": int(n),
                "target_first_rate": float(target_first or 0),
                "stop_first_rate": float(stop_first or 0),
                "avg_model_return": float(avg_return or 0),
            }
            for month, n, target_first, stop_first, avg_return in monthly_rows
        ],
        "examples_2026": [
            {
                key: value.isoformat() if hasattr(value, "isoformat") else value
                for key, value in zip([desc[0] for desc in con.description], row)
            }
            for row in examples_rows
        ],
    }


def _blowoff_sized_trade_scenario_for_where(
    con: duckdb.DuckDBPyConnection,
    where_sql: str,
    *,
    target_down_pct: float = 0.05,
) -> dict[str, Any]:
    target_threshold = -target_down_pct
    base_cte = f"""
WITH first_entries AS (
  SELECT *
  FROM short_watch_blowoff_entry_candidates
  WHERE {where_sql}
  QUALIFY row_number() OVER (PARTITION BY code, signal_date ORDER BY entry_date) = 1
),
path AS (
  SELECT
    e.code,
    e.signal_date,
    e.entry_date,
    e.entry_c,
    e.peak_h,
    d.date,
    d.l,
    d.h,
    row_number() OVER (PARTITION BY e.code, e.signal_date ORDER BY d.date) AS rn
  FROM first_entries e
  JOIN daily_bars d
    ON d.source = 'pan'
   AND d.code = e.code
   AND d.date > e.entry_date
   AND d.date <= e.entry_date + 86400 * 40
),
first_touch AS (
  SELECT
    e.code,
    coalesce(t.name, '') AS name,
    e.signal_date,
    e.entry_date,
    e.entry_c,
    e.peak_h,
    e.entry_v / NULLIF(e.peak_v, 0) AS entry_v_vs_peak_v,
    e.peak_h * 1.005 / NULLIF(e.entry_c, 0) - 1.0 AS stop_loss_pct,
    (
      e.peak_h * 1.005 / NULLIF(e.entry_c, 0) - 1.0 <= 0.03
      AND e.entry_v / NULLIF(e.peak_v, 0) > 0.60
      AND e.entry_v / NULLIF(e.peak_v, 0) < 0.90
    ) AS tight_stop_mid_volume,
    min(CASE WHEN p.l / NULLIF(e.entry_c, 0) - 1.0 <= ? THEN p.rn END) AS target_rn,
    min(CASE WHEN p.h >= e.peak_h * 1.005 THEN p.rn END) AS stop_rn,
    min(p.l / NULLIF(e.entry_c, 0) - 1.0) AS best_down20,
    max(p.h / NULLIF(e.entry_c, 0) - 1.0) AS worst_up20
  FROM first_entries e
  JOIN path p
    ON p.code = e.code
   AND p.signal_date = e.signal_date
   AND p.entry_date = e.entry_date
   AND p.rn <= 20
  LEFT JOIN tickers t ON t.code = e.code
  GROUP BY ALL
),
scenario AS (
  SELECT
    *,
    CASE WHEN tight_stop_mid_volume THEN 0.5 ELSE 1.0 END AS position_size_weight,
    CASE
      WHEN target_rn IS NOT NULL AND (stop_rn IS NULL OR target_rn <= stop_rn) THEN 'target_first'
      WHEN stop_rn IS NOT NULL THEN 'stop_first'
      ELSE 'no_touch'
    END AS outcome,
    CASE
      WHEN target_rn IS NOT NULL AND (stop_rn IS NULL OR target_rn <= stop_rn) THEN ?
      WHEN stop_rn IS NOT NULL THEN -stop_loss_pct
      ELSE 0.0
    END AS model_return
  FROM first_touch
)
"""
    summary_row = con.execute(
        base_cte
        + """
SELECT
  count(*) AS n,
  avg(position_size_weight) AS avg_position_size_weight,
  avg(CASE WHEN outcome = 'target_first' THEN 1.0 ELSE 0.0 END) AS target_first_rate,
  avg(CASE WHEN outcome = 'stop_first' THEN 1.0 ELSE 0.0 END) AS stop_first_rate,
  avg(model_return) AS avg_unweighted_return,
  avg(model_return * position_size_weight) AS avg_sized_return_per_signal,
  sum(model_return) AS total_unweighted_return,
  sum(model_return * position_size_weight) AS total_sized_return,
  min(model_return) AS worst_unweighted_return,
  min(model_return * position_size_weight) AS worst_sized_return
FROM scenario
""",
        [target_threshold, target_down_pct],
    ).fetchone()
    by_tag_rows = con.execute(
        base_cte
        + """
SELECT
  CASE WHEN tight_stop_mid_volume THEN 'tight_stop_mid_volume' ELSE 'normal_size' END AS size_bucket,
  count(*) AS n,
  avg(CASE WHEN outcome = 'target_first' THEN 1.0 ELSE 0.0 END) AS target_first_rate,
  avg(CASE WHEN outcome = 'stop_first' THEN 1.0 ELSE 0.0 END) AS stop_first_rate,
  avg(position_size_weight) AS avg_position_size_weight,
  avg(model_return) AS avg_unweighted_return,
  avg(model_return * position_size_weight) AS avg_sized_return_per_signal,
  min(model_return * position_size_weight) AS worst_sized_return
FROM scenario
GROUP BY 1
ORDER BY 1
""",
        [target_threshold, target_down_pct],
    ).fetchall()
    examples_rows = con.execute(
        base_cte
        + """
SELECT
  code,
  name,
  to_timestamp(signal_date)::date AS signal_date,
  to_timestamp(entry_date)::date AS entry_date,
  outcome,
  target_rn,
  stop_rn,
  tight_stop_mid_volume,
  position_size_weight,
  round(model_return * 100, 2) AS model_return_pct,
  round((model_return * position_size_weight) * 100, 2) AS sized_return_pct,
  round(entry_v_vs_peak_v, 2) AS entry_v_vs_peak_v,
  round(stop_loss_pct * 100, 2) AS stop_loss_pct
FROM scenario
WHERE tight_stop_mid_volume OR year(to_timestamp(signal_date)) >= 2026
ORDER BY signal_date, code
""",
        [target_threshold, target_down_pct],
    ).fetchall()
    keys = [desc[0] for desc in con.description]
    return {
        "target_down_pct": round(target_down_pct * 100, 2),
        "sizing_policy": "tight_stop_mid_volume_half_size_else_full_size",
        "summary": {
            "n": int(summary_row[0] or 0),
            "avg_position_size_weight": float(summary_row[1] or 0),
            "target_first_rate": float(summary_row[2] or 0),
            "stop_first_rate": float(summary_row[3] or 0),
            "avg_unweighted_return": float(summary_row[4] or 0),
            "avg_sized_return_per_signal": float(summary_row[5] or 0),
            "total_unweighted_return": float(summary_row[6] or 0),
            "total_sized_return": float(summary_row[7] or 0),
            "worst_unweighted_return": float(summary_row[8] or 0),
            "worst_sized_return": float(summary_row[9] or 0),
        },
        "by_size_bucket": [
            {
                "size_bucket": str(bucket),
                "n": int(n),
                "target_first_rate": float(target_first or 0),
                "stop_first_rate": float(stop_first or 0),
                "avg_position_size_weight": float(avg_weight or 0),
                "avg_unweighted_return": float(avg_unweighted or 0),
                "avg_sized_return_per_signal": float(avg_sized or 0),
                "worst_sized_return": float(worst_sized or 0),
            }
            for bucket, n, target_first, stop_first, avg_weight, avg_unweighted, avg_sized, worst_sized in by_tag_rows
        ],
        "examples": [
            {
                key: _json_value(value)
                for key, value in zip(keys, row)
            }
            for row in examples_rows
        ],
    }


def _decision(summary: dict[str, Any]) -> str:
    if summary["n"] < 50:
        return "hold_low_sample"
    if (
        summary["target20_down3_rate"] >= 0.70
        and summary["adverse20_up3_rate"] <= 0.35
        and (summary["target20_to_adverse20_ratio"] or 0) >= 2.0
    ):
        return "keep_watch_to_entry_candidate"
    return "drop_or_hold"


def _practical_blowoff_leaderboard(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in rows:
        if "trade_scenario" not in row:
            continue
        trade = row["trade_scenario"]["summary"]
        sized = row["sized_trade_scenario"]["summary"]
        summary = row["summary"]
        out.append(
            {
                "rule_id": row["rule_id"],
                "candidate_local_decision": row["candidate_local_decision"],
                "n": trade["n"],
                "target_first_rate": trade["target_first_rate"],
                "stop_first_rate": trade["stop_first_rate"],
                "avg_model_return": trade["avg_model_return"],
                "total_model_return": trade["total_model_return"],
                "sized_total_return": sized["total_sized_return"],
                "worst_sized_return": sized["worst_sized_return"],
                "target20_down3_rate": summary["target20_down3_rate"],
                "adverse20_up3_rate": summary["adverse20_up3_rate"],
                "realized_reward_to_peak_risk20": summary["realized_reward_to_peak_risk20"],
            }
        )
    out.sort(
        key=lambda item: (
            item["target_first_rate"],
            -item["stop_first_rate"],
            item["sized_total_return"],
            item["n"],
        ),
        reverse=True,
    )
    return out


def _recent_blowoff_entries_for_where(
    con: duckdb.DuckDBPyConnection,
    where_sql: str,
    *,
    limit: int = 20,
) -> list[dict[str, Any]]:
    rows = con.execute(
        f"""
SELECT
  e.code,
  coalesce(t.name, '') AS name,
  strftime(to_timestamp(e.signal_date), '%Y-%m-%d') AS signal_date,
  strftime(to_timestamp(e.peak_date), '%Y-%m-%d') AS peak_date,
  strftime(to_timestamp(e.entry_date), '%Y-%m-%d') AS entry_date,
  round(e.entry_c, 2) AS entry_c,
  round(e.peak_h, 2) AS peak_h,
  round((e.entry_c - e.low20) / NULLIF(e.peak_h - e.entry_c, 0), 2) AS rr_to_low20,
  round((e.entry_c - e.entry_low60) / NULLIF(e.entry_high60 - e.entry_low60, 0), 2) AS entry_range60_pos,
  round((e.entry_c / NULLIF(e.entry_high60, 0) - 1.0) * 100, 2) AS entry_vs_high60_pct,
  round((e.entry_c / NULLIF(e.entry_ma7, 0) - 1.0) * 100, 2) AS entry_vs_ma7_pct,
  round((e.entry_c / NULLIF(e.entry_ma20, 0) - 1.0) * 100, 2) AS entry_vs_ma20_pct,
  round(e.entry_v / NULLIF(e.peak_v, 0), 2) AS entry_v_vs_peak_v,
  round(e.entry_v / NULLIF(e.signal_v, 0), 2) AS entry_v_vs_signal_v
FROM short_watch_blowoff_entry_candidates e
LEFT JOIN tickers t ON t.code = e.code
WHERE {where_sql}
QUALIFY row_number() OVER (PARTITION BY e.code, e.signal_date ORDER BY e.entry_date) = 1
ORDER BY e.entry_date DESC
LIMIT ?
""",
        [limit],
    ).fetchall()
    keys = [
        "code",
        "name",
        "signal_date",
        "peak_date",
        "entry_date",
        "entry_c",
        "peak_h",
        "rr_to_low20",
        "entry_range60_pos",
        "entry_vs_high60_pct",
        "entry_vs_ma7_pct",
        "entry_vs_ma20_pct",
        "entry_v_vs_peak_v",
        "entry_v_vs_signal_v",
    ]
    return [
        {
            key: _json_value(value)
            for key, value in zip(keys, row)
        }
        for row in rows
    ]


def _blowoff_followup_management_for_where(
    con: duckdb.DuckDBPyConnection,
    where_sql: str,
) -> dict[str, Any]:
    base_cte = f"""
WITH first_entries AS (
  SELECT *
  FROM short_watch_blowoff_entry_candidates
  WHERE {where_sql}
  QUALIFY row_number() OVER (PARTITION BY code, signal_date ORDER BY entry_date) = 1
),
path AS (
  SELECT
    e.code,
    e.signal_date,
    e.entry_date,
    e.entry_c,
    e.entry_l,
    e.entry_h,
    e.entry_ma7,
    e.peak_h,
    d.date,
    d.h,
    d.l,
    d.c,
    row_number() OVER (PARTITION BY e.code, e.signal_date ORDER BY d.date) AS rn
  FROM first_entries e
  JOIN three_window_events d
    ON d.code = e.code
   AND d.date > e.entry_date
   AND d.date <= e.entry_date + 86400 * 40
),
agg AS (
  SELECT
    e.code,
    coalesce(t.name, '') AS name,
    e.signal_date,
    e.entry_date,
    max(CASE WHEN p.rn <= 3 AND p.c > e.entry_ma7 THEN 1 ELSE 0 END) AS close_above_entry_ma7_3,
    max(CASE WHEN p.rn <= 3 AND p.c > e.entry_h THEN 1 ELSE 0 END) AS close_above_entry_high_3,
    max(CASE WHEN p.rn <= 3 AND p.l <= e.entry_l THEN 1 ELSE 0 END) AS break_entry_low_3,
    max(CASE WHEN p.rn <= 3 AND p.c < e.entry_l THEN 1 ELSE 0 END) AS close_below_entry_low_3,
    min(CASE WHEN p.rn <= 3 THEN p.l / NULLIF(e.entry_c, 0) - 1.0 END) AS low3_ret,
    max(CASE WHEN p.rn <= 3 THEN p.h / NULLIF(e.entry_c, 0) - 1.0 END) AS high3_ret,
    min(CASE WHEN p.l / NULLIF(e.entry_c, 0) - 1.0 <= -0.05 THEN p.rn END) AS target_rn,
    min(CASE WHEN p.h >= e.peak_h * 1.005 THEN p.rn END) AS stop_rn,
    min(p.l / NULLIF(e.entry_c, 0) - 1.0) AS best_down20,
    max(p.h / NULLIF(e.entry_c, 0) - 1.0) AS worst_up20
  FROM first_entries e
  JOIN path p
    ON p.code = e.code
   AND p.signal_date = e.signal_date
   AND p.entry_date = e.entry_date
   AND p.rn <= 20
  LEFT JOIN tickers t ON t.code = e.code
  GROUP BY ALL
),
evaluated AS (
  SELECT
    *,
    CASE
      WHEN target_rn IS NOT NULL AND (stop_rn IS NULL OR target_rn <= stop_rn) THEN 1
      ELSE 0
    END AS target_first
  FROM agg
)
"""
    bucket_rows = con.execute(
        base_cte
        + """
SELECT 'all' AS bucket, count(*) AS n, avg(target_first) AS target_first_rate, avg(best_down20) AS avg_best_down20, avg(worst_up20) AS avg_worst_up20
FROM evaluated
UNION ALL
SELECT 'close_above_entry_ma7_3', count(*), avg(target_first), avg(best_down20), avg(worst_up20)
FROM evaluated WHERE close_above_entry_ma7_3 = 1
UNION ALL
SELECT 'not_close_above_entry_ma7_3', count(*), avg(target_first), avg(best_down20), avg(worst_up20)
FROM evaluated WHERE close_above_entry_ma7_3 = 0
UNION ALL
SELECT 'break_entry_low_3', count(*), avg(target_first), avg(best_down20), avg(worst_up20)
FROM evaluated WHERE break_entry_low_3 = 1
UNION ALL
SELECT 'not_break_entry_low_3', count(*), avg(target_first), avg(best_down20), avg(worst_up20)
FROM evaluated WHERE break_entry_low_3 = 0
UNION ALL
SELECT 'close_below_entry_low_3', count(*), avg(target_first), avg(best_down20), avg(worst_up20)
FROM evaluated WHERE close_below_entry_low_3 = 1
UNION ALL
SELECT 'not_close_below_entry_low_3', count(*), avg(target_first), avg(best_down20), avg(worst_up20)
FROM evaluated WHERE close_below_entry_low_3 = 0
"""
    ).fetchall()
    examples_rows = con.execute(
        base_cte
        + """
SELECT
  code,
  name,
  to_timestamp(entry_date)::date AS entry_date,
  target_rn,
  stop_rn,
  round(low3_ret * 100, 2) AS low3_pct,
  round(high3_ret * 100, 2) AS high3_pct,
  close_above_entry_ma7_3,
  close_above_entry_high_3,
  break_entry_low_3,
  close_below_entry_low_3,
  round(best_down20 * 100, 2) AS best_down20_pct,
  round(worst_up20 * 100, 2) AS worst_up20_pct
FROM evaluated
ORDER BY entry_date DESC
LIMIT 20
"""
    ).fetchall()
    return {
        "policy_tested": "first_3_sessions_after_entry",
        "judgment": "do_not_exit_only_because_price_reclaims_entry_ma7_or_entry_high_within_3_sessions",
        "bucket_summary": [
            {
                "bucket": str(bucket),
                "n": int(n or 0),
                "target_first_rate": float(target_first or 0),
                "avg_best_down20": float(avg_best or 0),
                "avg_worst_up20": float(avg_worst or 0),
            }
            for bucket, n, target_first, avg_best, avg_worst in bucket_rows
        ],
        "examples": [
            {
                key: _json_value(value)
                for key, value in zip([desc[0] for desc in con.description], row)
            }
            for row in examples_rows
        ],
    }


def _blowoff_stop_buffer_sensitivity_for_where(
    con: duckdb.DuckDBPyConnection,
    where_sql: str,
    *,
    target_down_pct: float = 0.05,
) -> list[dict[str, Any]]:
    rows_out = []
    for stop_buffer_pct in [0.005, 0.01, 0.015, 0.02]:
        target_threshold = -target_down_pct
        row = con.execute(
            f"""
WITH first_entries AS (
  SELECT *
  FROM short_watch_blowoff_entry_candidates
  WHERE {where_sql}
  QUALIFY row_number() OVER (PARTITION BY code, signal_date ORDER BY entry_date) = 1
),
path AS (
  SELECT
    e.code,
    e.signal_date,
    e.entry_date,
    e.entry_c,
    e.peak_h,
    d.date,
    d.h,
    d.l,
    row_number() OVER (PARTITION BY e.code, e.signal_date ORDER BY d.date) AS rn
  FROM first_entries e
  JOIN daily_bars d
    ON d.source = 'pan'
   AND d.code = e.code
   AND d.date > e.entry_date
   AND d.date <= e.entry_date + 86400 * 40
),
touch AS (
  SELECT
    e.code,
    e.signal_date,
    e.entry_date,
    e.entry_c,
    e.peak_h,
    min(CASE WHEN p.l / NULLIF(e.entry_c, 0) - 1.0 <= ? THEN p.rn END) AS target_rn,
    min(CASE WHEN p.h >= e.peak_h * ? THEN p.rn END) AS stop_rn,
    min(p.l / NULLIF(e.entry_c, 0) - 1.0) AS best_down20,
    max(p.h / NULLIF(e.entry_c, 0) - 1.0) AS worst_up20,
    e.peak_h * ? / NULLIF(e.entry_c, 0) - 1.0 AS stop_loss_pct
  FROM first_entries e
  JOIN path p
    ON p.code = e.code
   AND p.signal_date = e.signal_date
   AND p.entry_date = e.entry_date
   AND p.rn <= 20
  GROUP BY ALL
),
scenario AS (
  SELECT
    *,
    CASE
      WHEN target_rn IS NOT NULL AND (stop_rn IS NULL OR target_rn <= stop_rn) THEN 'target_first'
      WHEN stop_rn IS NOT NULL THEN 'stop_first'
      ELSE 'no_touch'
    END AS outcome,
    CASE
      WHEN target_rn IS NOT NULL AND (stop_rn IS NULL OR target_rn <= stop_rn) THEN ?
      WHEN stop_rn IS NOT NULL THEN -stop_loss_pct
      ELSE 0.0
    END AS model_return
  FROM touch
)
SELECT
  count(*) AS n,
  avg(CASE WHEN outcome = 'target_first' THEN 1.0 ELSE 0.0 END) AS target_first_rate,
  avg(CASE WHEN outcome = 'stop_first' THEN 1.0 ELSE 0.0 END) AS stop_first_rate,
  avg(CASE WHEN outcome = 'no_touch' THEN 1.0 ELSE 0.0 END) AS no_touch_rate,
  avg(model_return) AS avg_model_return,
  sum(model_return) AS total_model_return,
  min(model_return) AS worst_model_return,
  avg(best_down20) AS avg_best_down20,
  avg(worst_up20) AS avg_worst_up20
FROM scenario
""",
            [
                target_threshold,
                1.0 + stop_buffer_pct,
                1.0 + stop_buffer_pct,
                target_down_pct,
            ],
        ).fetchone()
        rows_out.append(
            {
                "stop_buffer_pct": round(stop_buffer_pct * 100, 2),
                "n": int(row[0] or 0),
                "target_first_rate": float(row[1] or 0),
                "stop_first_rate": float(row[2] or 0),
                "no_touch_rate": float(row[3] or 0),
                "avg_model_return": float(row[4] or 0),
                "total_model_return": float(row[5] or 0),
                "worst_model_return": float(row[6] or 0),
                "avg_best_down20": float(row[7] or 0),
                "avg_worst_up20": float(row[8] or 0),
            }
        )
    return rows_out


def _blowoff_target_horizon_sensitivity_for_where(
    con: duckdb.DuckDBPyConnection,
    where_sql: str,
    *,
    stop_buffer_pct: float = 0.01,
) -> list[dict[str, Any]]:
    rows_out = []
    for horizon in [10, 20, 30, 40, 60]:
        for target_down_pct in [0.03, 0.04, 0.05, 0.06, 0.08, 0.10]:
            row = con.execute(
                f"""
WITH first_entries AS (
  SELECT *
  FROM short_watch_blowoff_entry_candidates
  WHERE {where_sql}
  QUALIFY row_number() OVER (PARTITION BY code, signal_date ORDER BY entry_date) = 1
),
path AS (
  SELECT
    e.code,
    e.signal_date,
    e.entry_date,
    e.entry_c,
    e.peak_h,
    d.date,
    d.h,
    d.l,
    row_number() OVER (PARTITION BY e.code, e.signal_date ORDER BY d.date) AS rn
  FROM first_entries e
  JOIN daily_bars d
    ON d.source = 'pan'
   AND d.code = e.code
   AND d.date > e.entry_date
   AND d.date <= e.entry_date + 86400 * 100
),
touch AS (
  SELECT
    e.code,
    e.signal_date,
    e.entry_date,
    e.entry_c,
    e.peak_h,
    min(CASE WHEN p.l / NULLIF(e.entry_c, 0) - 1.0 <= ? THEN p.rn END) AS target_rn,
    min(CASE WHEN p.h >= e.peak_h * ? THEN p.rn END) AS stop_rn,
    e.peak_h * ? / NULLIF(e.entry_c, 0) - 1.0 AS stop_loss_pct
  FROM first_entries e
  JOIN path p
    ON p.code = e.code
   AND p.signal_date = e.signal_date
   AND p.entry_date = e.entry_date
   AND p.rn <= ?
  GROUP BY ALL
),
scenario AS (
  SELECT
    *,
    CASE
      WHEN target_rn IS NOT NULL AND (stop_rn IS NULL OR target_rn <= stop_rn) THEN 'target_first'
      WHEN stop_rn IS NOT NULL THEN 'stop_first'
      ELSE 'no_touch'
    END AS outcome,
    CASE
      WHEN target_rn IS NOT NULL AND (stop_rn IS NULL OR target_rn <= stop_rn) THEN ?
      WHEN stop_rn IS NOT NULL THEN -stop_loss_pct
      ELSE 0.0
    END AS model_return
  FROM touch
)
SELECT
  count(*) AS n,
  avg(CASE WHEN outcome = 'target_first' THEN 1.0 ELSE 0.0 END) AS target_first_rate,
  avg(CASE WHEN outcome = 'stop_first' THEN 1.0 ELSE 0.0 END) AS stop_first_rate,
  avg(CASE WHEN outcome = 'no_touch' THEN 1.0 ELSE 0.0 END) AS no_touch_rate,
  avg(model_return) AS avg_model_return,
  sum(model_return) AS total_model_return,
  min(model_return) AS worst_model_return
FROM scenario
""",
                [
                    -target_down_pct,
                    1.0 + stop_buffer_pct,
                    1.0 + stop_buffer_pct,
                    horizon,
                    target_down_pct,
                ],
            ).fetchone()
            rows_out.append(
                {
                    "horizon_sessions": horizon,
                    "target_down_pct": round(target_down_pct * 100, 2),
                    "stop_buffer_pct": round(stop_buffer_pct * 100, 2),
                    "n": int(row[0] or 0),
                    "target_first_rate": float(row[1] or 0),
                    "stop_first_rate": float(row[2] or 0),
                    "no_touch_rate": float(row[3] or 0),
                    "avg_model_return": float(row[4] or 0),
                    "total_model_return": float(row[5] or 0),
                    "worst_model_return": float(row[6] or 0),
                }
            )
    rows_out.sort(
        key=lambda item: (
            item["total_model_return"],
            item["target_first_rate"],
            -item["stop_first_rate"],
            -item["no_touch_rate"],
        ),
        reverse=True,
    )
    return rows_out


def _blowoff_rr_relaxation_diagnosis(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    core_rule_id = "post_peak_fast_ma7_fail_low20_rr_ge_1_3_drop_midrange_weak_entry_volume"
    relaxed_rule_id = "post_peak_fast_ma7_fail_low20_rr_ge_1_1_drop_midrange_weak_entry_volume"
    if relaxed_rule_id not in BLOWOFF_ENTRY_RULES or core_rule_id not in BLOWOFF_ENTRY_RULES:
        return {"status": "missing_rules", "core_rule_id": core_rule_id, "relaxed_rule_id": relaxed_rule_id}
    core_where = BLOWOFF_ENTRY_RULES[core_rule_id]["where"]
    relaxed_where = BLOWOFF_ENTRY_RULES[relaxed_rule_id]["where"]
    query = f"""
WITH relaxed AS (
  SELECT *
  FROM short_watch_blowoff_entry_candidates
  WHERE {relaxed_where}
  QUALIFY row_number() OVER (PARTITION BY code, signal_date ORDER BY entry_date) = 1
),
core AS (
  SELECT code, signal_date
  FROM short_watch_blowoff_entry_candidates
  WHERE {core_where}
  QUALIFY row_number() OVER (PARTITION BY code, signal_date ORDER BY entry_date) = 1
),
path AS (
  SELECT
    e.code,
    e.signal_date,
    e.entry_date,
    e.entry_c,
    e.peak_h,
    d.date,
    d.h,
    d.l,
    row_number() OVER (PARTITION BY e.code, e.signal_date ORDER BY d.date) AS rn
  FROM relaxed e
  JOIN daily_bars d
    ON d.source = 'pan'
   AND d.code = e.code
   AND d.date > e.entry_date
   AND d.date <= e.entry_date + 86400 * 40
),
touch AS (
  SELECT
    e.code,
    e.signal_date,
    e.entry_date,
    min(CASE WHEN p.l / NULLIF(e.entry_c, 0) - 1.0 <= -0.05 THEN p.rn END) AS target_rn,
    min(CASE WHEN p.h >= e.peak_h * 1.005 THEN p.rn END) AS stop_rn,
    min(p.l / NULLIF(e.entry_c, 0) - 1.0) AS best_down20,
    max(p.h / NULLIF(e.entry_c, 0) - 1.0) AS worst_up20
  FROM relaxed e
  JOIN path p
    ON p.code = e.code
   AND p.signal_date = e.signal_date
   AND p.entry_date = e.entry_date
   AND p.rn <= 20
  GROUP BY ALL
),
labeled AS (
  SELECT
    CASE WHEN c.code IS NULL THEN 'extra_relaxed_only' ELSE 'core' END AS bucket,
    e.code,
    coalesce(t.name, '') AS name,
    strftime(to_timestamp(e.signal_date), '%Y-%m-%d') AS signal_date,
    strftime(to_timestamp(e.peak_date), '%Y-%m-%d') AS peak_date,
    strftime(to_timestamp(e.entry_date), '%Y-%m-%d') AS entry_date,
    (e.entry_c - e.low20) / NULLIF(e.peak_h - e.entry_c, 0) AS rr_to_low20,
    (e.entry_c - e.entry_low60) / NULLIF(e.entry_high60 - e.entry_low60, 0) AS entry_range60_pos,
    e.entry_c / NULLIF(e.entry_high60, 0) - 1.0 AS entry_vs_high60,
    e.entry_c / NULLIF(e.entry_ma7, 0) - 1.0 AS entry_vs_ma7,
    e.entry_c / NULLIF(e.entry_ma20, 0) - 1.0 AS entry_vs_ma20,
    e.entry_v / NULLIF(e.peak_v, 0) AS entry_v_vs_peak_v,
    e.entry_v / NULLIF(e.signal_v, 0) AS entry_v_vs_signal_v,
    CASE
      WHEN x.target_rn IS NOT NULL AND (x.stop_rn IS NULL OR x.target_rn <= x.stop_rn) THEN 'target_first'
      WHEN x.stop_rn IS NOT NULL THEN 'stop_first'
      ELSE 'no_touch'
    END AS outcome,
    x.target_rn,
    x.stop_rn,
    x.best_down20,
    x.worst_up20
  FROM relaxed e
  LEFT JOIN core c
    ON c.code = e.code
   AND c.signal_date = e.signal_date
  LEFT JOIN touch x
    ON x.code = e.code
   AND x.signal_date = e.signal_date
   AND x.entry_date = e.entry_date
  LEFT JOIN tickers t ON t.code = e.code
)
SELECT * FROM labeled
"""
    rows = con.execute(query).fetchdf().to_dict("records")
    summary_rows = con.execute(
        f"""
WITH rows AS ({query})
SELECT
  bucket,
  count(*) AS n,
  avg(CASE WHEN outcome = 'target_first' THEN 1.0 ELSE 0.0 END) AS target_first_rate,
  avg(CASE WHEN outcome = 'stop_first' THEN 1.0 ELSE 0.0 END) AS stop_first_rate,
  avg(rr_to_low20) AS avg_rr_to_low20,
  avg(entry_range60_pos) AS avg_entry_range60_pos,
  avg(entry_vs_ma20) AS avg_entry_vs_ma20,
  avg(entry_v_vs_peak_v) AS avg_entry_v_vs_peak_v,
  avg(entry_v_vs_signal_v) AS avg_entry_v_vs_signal_v,
  min(best_down20) AS best_down20_min,
  max(worst_up20) AS worst_up20_max
FROM rows
GROUP BY bucket
ORDER BY bucket
""",
    ).fetchdf().to_dict("records")
    return {
        "core_rule_id": core_rule_id,
        "relaxed_rule_id": relaxed_rule_id,
        "changed_axis": "rr_to_low20_threshold_1_3_to_1_1",
        "judgment": "drop_relaxation",
        "reason": "relaxing rr_to_low20 increases samples but adds stop_first cases and worsens drawdown",
        "summary_by_bucket": [
            {key: _json_value(value) for key, value in row.items()}
            for row in summary_rows
        ],
        "relaxed_only_examples": [
            {key: _json_value(value) for key, value in row.items()}
            for row in rows
            if row.get("bucket") == "extra_relaxed_only"
        ],
    }


def run(*, db_path: Path, output_root: Path, start: str, end: str) -> Path:
    con = duckdb.connect(str(db_path), read_only=True)
    if end == "latest":
        end = _latest_pan_date(con)
    _build_features(con, start, end, table_name="three_window_events", require_forward_labels=False)
    _build_entry_candidates(con)

    rows = []
    for rule_id, rule in ENTRY_RULES.items():
        summary = _summary_for_where(con, rule["where"])
        rows.append(
            {
                "rule_id": rule_id,
                "description": rule["description"],
                "where_sql": rule["where"],
                "summary": summary,
                "yearly": _yearly_for_where(con, rule["where"]),
                "examples_2026": _examples_for_where(con, rule["where"], start_year=2026),
                "candidate_local_decision": _decision(summary),
            }
        )
    for rule_id, rule in BLOWOFF_ENTRY_RULES.items():
        summary = _summary_for_where(con, rule["where"], table_name="short_watch_blowoff_entry_candidates")
        rows.append(
            {
                "rule_id": rule_id,
                "description": rule["description"],
                "where_sql": rule["where"],
                "summary": summary,
                "yearly": _yearly_for_where(con, rule["where"], table_name="short_watch_blowoff_entry_candidates"),
                "shape_tag_summary": _blowoff_shape_tag_summary_for_where(con, rule["where"]),
                "shape_subtag_summary": _blowoff_shape_subtag_summary_for_where(con, rule["where"]),
                "exit_timing": _blowoff_exit_timing_for_where(con, rule["where"]),
                "trade_scenario": _blowoff_trade_scenario_for_where(con, rule["where"]),
                "sized_trade_scenario": _blowoff_sized_trade_scenario_for_where(con, rule["where"]),
                "examples_2026": _blowoff_examples_for_where(con, rule["where"], start_year=2026),
                "failure_examples": _blowoff_failure_examples_for_where(con, rule["where"]),
                "candidate_local_decision": _decision(summary),
            }
        )
    rows.sort(
        key=lambda item: (
            1 if item["candidate_local_decision"] == "keep_watch_to_entry_candidate" else 0,
            item["summary"]["target20_down3_rate"] - item["summary"]["adverse20_up3_rate"],
            item["summary"]["n"],
        ),
        reverse=True,
    )
    champion = rows[0] if rows else None
    practical_blowoff_leaderboard = _practical_blowoff_leaderboard(rows)
    practical_blowoff_champion = practical_blowoff_leaderboard[0] if practical_blowoff_leaderboard else None
    practical_blowoff_n20_champion = next(
        (item for item in practical_blowoff_leaderboard if item["n"] >= 20),
        None,
    )
    recent_practical_blowoff_entries = (
        _recent_blowoff_entries_for_where(
            con,
            BLOWOFF_ENTRY_RULES[practical_blowoff_champion["rule_id"]]["where"],
        )
        if practical_blowoff_champion and practical_blowoff_champion["rule_id"] in BLOWOFF_ENTRY_RULES
        else []
    )
    practical_blowoff_followup_management = (
        _blowoff_followup_management_for_where(
            con,
            BLOWOFF_ENTRY_RULES[practical_blowoff_champion["rule_id"]]["where"],
        )
        if practical_blowoff_champion and practical_blowoff_champion["rule_id"] in BLOWOFF_ENTRY_RULES
        else None
    )
    practical_blowoff_stop_buffer_sensitivity = (
        _blowoff_stop_buffer_sensitivity_for_where(
            con,
            BLOWOFF_ENTRY_RULES[practical_blowoff_champion["rule_id"]]["where"],
        )
        if practical_blowoff_champion and practical_blowoff_champion["rule_id"] in BLOWOFF_ENTRY_RULES
        else []
    )
    practical_blowoff_n20_stop_buffer_sensitivity = (
        _blowoff_stop_buffer_sensitivity_for_where(
            con,
            BLOWOFF_ENTRY_RULES[practical_blowoff_n20_champion["rule_id"]]["where"],
        )
        if practical_blowoff_n20_champion and practical_blowoff_n20_champion["rule_id"] in BLOWOFF_ENTRY_RULES
        else []
    )
    practical_blowoff_n20_target_horizon_sensitivity = (
        _blowoff_target_horizon_sensitivity_for_where(
            con,
            BLOWOFF_ENTRY_RULES[practical_blowoff_n20_champion["rule_id"]]["where"],
        )
        if practical_blowoff_n20_champion and practical_blowoff_n20_champion["rule_id"] in BLOWOFF_ENTRY_RULES
        else []
    )
    rr_relaxation_diagnosis = _blowoff_rr_relaxation_diagnosis(con)
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
            "base_signal_rule": RULES["short"]["rule_id"],
            "base_filter_id": BASE_FILTER_ID,
            "entry_window": "2-10 trading sessions after base watch signal",
            "outcome_windows": ["5 trading sessions after entry", "20 trading sessions after entry"],
            "cost_slippage": "not_applied",
        },
        "rows": rows,
        "current_champion": {
            "rule_id": champion["rule_id"] if champion else None,
            "candidate_local_decision": champion["candidate_local_decision"] if champion else None,
            "summary": champion["summary"] if champion else None,
        },
        "practical_blowoff_leaderboard": practical_blowoff_leaderboard,
        "practical_blowoff_champion": practical_blowoff_champion,
        "practical_blowoff_n20_champion": practical_blowoff_n20_champion,
        "recent_practical_blowoff_entries": recent_practical_blowoff_entries,
        "practical_blowoff_followup_management": practical_blowoff_followup_management,
        "practical_blowoff_stop_buffer_sensitivity": practical_blowoff_stop_buffer_sensitivity,
        "practical_blowoff_n20_stop_buffer_sensitivity": practical_blowoff_n20_stop_buffer_sensitivity,
        "practical_blowoff_n20_target_horizon_sensitivity": practical_blowoff_n20_target_horizon_sensitivity,
        "rr_relaxation_diagnosis": rr_relaxation_diagnosis,
        "decision": {
            "candidate_local_decision": champion["candidate_local_decision"] if champion else "hold_no_candidate",
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": (
                "watch-to-entry branch improves practical timing after a strong watch signal"
                if champion and champion["candidate_local_decision"] == "keep_watch_to_entry_candidate"
                else "no entry branch met sample and risk gates"
            ),
        },
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
    }
    tag = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = output_root / f"{tag}-{AXIS_ID}"
    _write_json(out_dir / "short_watch_to_entry_retest_report.json", report)
    _write_json(output_root / "latest_short_watch_to_entry_retest_report.json", {"run_root": str(out_dir), **report})
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
