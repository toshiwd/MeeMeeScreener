from __future__ import annotations

import argparse
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import sys
from typing import Any, Iterable

import duckdb

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.core.config import config


@dataclass(frozen=True)
class Gate:
    min_liquidity20d: float = 500_000
    min_up_prob: float = 0.75
    min_ev: float = 0.05
    max_rev_risk: float = 0.40


def _now_iso_jst() -> str:
    # Local time is fine (ledger records are append-only, and the DB path is local).
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _summary(values: list[float]) -> dict[str, Any]:
    n = len(values)
    if n == 0:
        return {
            "mean": None,
            "median": None,
            "win_rate_gt_0": None,
            "hit_rate_ge_2pct": None,
            "min": None,
            "max": None,
        }
    values_sorted = sorted(values)
    mean = sum(values_sorted) / n
    if n % 2 == 1:
        median = values_sorted[n // 2]
    else:
        median = 0.5 * (values_sorted[n // 2 - 1] + values_sorted[n // 2])
    win_rate = sum(1 for v in values_sorted if v > 0) / n
    hit_rate = sum(1 for v in values_sorted if v >= 2.0) / n
    return {
        "mean": float(mean),
        "median": float(median),
        "win_rate_gt_0": float(win_rate),
        "hit_rate_ge_2pct": float(hit_rate),
        "min": float(values_sorted[0]),
        "max": float(values_sorted[-1]),
    }


def _fetch_rows(
    conn: duckdb.DuckDBPyConnection,
    *,
    start_ymd: int,
    end_ymd: int,
    side_dir: str,
    rank: int,
    variant: str,
) -> list[tuple[int, str, int, str | None, float, bool, bool]]:
    """
    Returns tuples:
      (ym, context_state, dt, code, month_end_ret_pct, baseline_pass, variant_pass)
    """
    if variant == "market_regime_ev_rr_v2":
        sql = """
WITH candidates AS (
  SELECT
    r.dt,
    r.code,
    CAST(r.dt/100 AS BIGINT) AS ym,
    r.anchor_price_next_open AS entry_price,
    CAST(json_extract(b.basis_payload_json, '$.liquidity20d') AS DOUBLE) AS liquidity20d,
    CAST(json_extract(b.basis_payload_json, '$.changePct') AS DOUBLE) AS ev,
    GREATEST(
      CAST(json_extract(d.score_snapshot_json, '$.probSideCalib') AS DOUBLE),
      CAST(json_extract(d.score_snapshot_json, '$.probSide') AS DOUBLE),
      CAST(json_extract(b.basis_payload_json, '$.weeklyBreakoutUpProb') AS DOUBLE),
      CAST(json_extract(b.basis_payload_json, '$.monthlyBreakoutUpProb') AS DOUBLE)
    ) AS up_prob,
    GREATEST(
      CAST(json_extract(b.basis_payload_json, '$.weeklyBreakoutDownProb') AS DOUBLE),
      CAST(json_extract(b.basis_payload_json, '$.monthlyBreakoutDownProb') AS DOUBLE)
    ) AS rev_risk,
    json_extract_string(b.basis_payload_json, '$.marketRegime') AS market_regime
  FROM ranking_appearance_daily r
  JOIN signal_basis_daily b
    ON b.dt = r.dt AND b.code = r.code
  JOIN signal_decision_daily d
    ON d.dt = r.dt AND d.code = r.code AND d.side = 'buy'
  WHERE r.dir = ? AND r.rank = ? AND r.dt BETWEEN ? AND ?
),
month_ends AS (
  SELECT
    code,
    CAST(strftime(to_timestamp(date), '%Y%m') AS BIGINT) AS ym,
    MAX(date) AS month_end_dt
  FROM daily_bars
  WHERE CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER) BETWEEN ? AND ?
  GROUP BY code, CAST(strftime(to_timestamp(date), '%Y%m') AS BIGINT)
),
joined AS (
  SELECT
    c.*,
    db.c AS month_end_close
  FROM candidates c
  JOIN month_ends me
    ON me.code = c.code AND me.ym = c.ym
  JOIN daily_bars db
    ON db.code = me.code AND db.date = me.month_end_dt
)
SELECT
  ym,
  market_regime AS context_state,
  dt,
  code,
  ((month_end_close / entry_price) - 1.0) * 100.0 AS month_end_ret_pct,
  (
    liquidity20d >= 500000
    AND up_prob >= 0.75
    AND ev >= 0.05
    AND rev_risk <= 0.40
  ) AS baseline_pass,
  (
    liquidity20d >= 500000
    AND up_prob >= 0.75
    AND (
      CASE
        WHEN market_regime = 'risk_on' THEN (ev >= 0.05 AND rev_risk <= 0.35)
        WHEN market_regime = 'neutral' THEN (ev >= 0.055 AND rev_risk <= 0.33)
        WHEN market_regime = 'risk_off' THEN (ev >= 0.06 AND rev_risk <= 0.30)
        ELSE (ev >= 0.05 AND rev_risk <= 0.40)
      END
    )
  ) AS variant_pass
FROM joined
WHERE entry_price IS NOT NULL AND entry_price > 0 AND month_end_close IS NOT NULL
ORDER BY dt
"""
    elif variant == "monthly_box_ev_rr_v1":
        sql = """
WITH candidates AS (
  SELECT
    r.dt,
    r.code,
    CAST(r.dt/100 AS BIGINT) AS ym,
    r.anchor_price_next_open AS entry_price,
    CAST(json_extract(b.basis_payload_json, '$.liquidity20d') AS DOUBLE) AS liquidity20d,
    CAST(json_extract(b.basis_payload_json, '$.changePct') AS DOUBLE) AS ev,
    GREATEST(
      CAST(json_extract(d.score_snapshot_json, '$.probSideCalib') AS DOUBLE),
      CAST(json_extract(d.score_snapshot_json, '$.probSide') AS DOUBLE),
      CAST(json_extract(b.basis_payload_json, '$.weeklyBreakoutUpProb') AS DOUBLE),
      CAST(json_extract(b.basis_payload_json, '$.monthlyBreakoutUpProb') AS DOUBLE)
    ) AS up_prob,
    GREATEST(
      CAST(json_extract(b.basis_payload_json, '$.weeklyBreakoutDownProb') AS DOUBLE),
      CAST(json_extract(b.basis_payload_json, '$.monthlyBreakoutDownProb') AS DOUBLE)
    ) AS rev_risk,
    json_extract_string(b.basis_payload_json, '$.monthlyBoxState') AS monthly_box_state
  FROM ranking_appearance_daily r
  JOIN signal_basis_daily b
    ON b.dt = r.dt AND b.code = r.code
  JOIN signal_decision_daily d
    ON d.dt = r.dt AND d.code = r.code AND d.side = 'buy'
  WHERE r.dir = ? AND r.rank = ? AND r.dt BETWEEN ? AND ?
),
month_ends AS (
  SELECT
    code,
    CAST(strftime(to_timestamp(date), '%Y%m') AS BIGINT) AS ym,
    MAX(date) AS month_end_dt
  FROM daily_bars
  WHERE CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER) BETWEEN ? AND ?
  GROUP BY code, CAST(strftime(to_timestamp(date), '%Y%m') AS BIGINT)
),
joined AS (
  SELECT
    c.*,
    db.c AS month_end_close
  FROM candidates c
  JOIN month_ends me
    ON me.code = c.code AND me.ym = c.ym
  JOIN daily_bars db
    ON db.code = me.code AND db.date = me.month_end_dt
)
SELECT
  ym,
  monthly_box_state AS context_state,
  dt,
  code,
  ((month_end_close / entry_price) - 1.0) * 100.0 AS month_end_ret_pct,
  (
    liquidity20d >= 500000
    AND up_prob >= 0.75
    AND ev >= 0.05
    AND rev_risk <= 0.40
  ) AS baseline_pass,
  (
    liquidity20d >= 500000
    AND up_prob >= 0.75
    AND (
      CASE
        WHEN monthly_box_state = 'box_upper' THEN (ev >= 0.05 AND rev_risk <= 0.33)
        WHEN monthly_box_state = 'box_mid' THEN (ev >= 0.055 AND rev_risk <= 0.30)
        WHEN monthly_box_state = 'box_lower' THEN (ev >= 0.06 AND rev_risk <= 0.25)
        WHEN monthly_box_state = 'no_box' THEN (ev >= 0.06 AND rev_risk <= 0.28)
        ELSE (ev >= 0.05 AND rev_risk <= 0.40)
      END
    )
  ) AS variant_pass
FROM joined
WHERE entry_price IS NOT NULL AND entry_price > 0 AND month_end_close IS NOT NULL
ORDER BY dt
"""
    elif variant == "monthly_box_ev_rr_v2_boxonly":
        sql = """
WITH candidates AS (
  SELECT
    r.dt,
    r.code,
    CAST(r.dt/100 AS BIGINT) AS ym,
    r.anchor_price_next_open AS entry_price,
    CAST(json_extract(b.basis_payload_json, '$.liquidity20d') AS DOUBLE) AS liquidity20d,
    CAST(json_extract(b.basis_payload_json, '$.changePct') AS DOUBLE) AS ev,
    GREATEST(
      CAST(json_extract(d.score_snapshot_json, '$.probSideCalib') AS DOUBLE),
      CAST(json_extract(d.score_snapshot_json, '$.probSide') AS DOUBLE),
      CAST(json_extract(b.basis_payload_json, '$.weeklyBreakoutUpProb') AS DOUBLE),
      CAST(json_extract(b.basis_payload_json, '$.monthlyBreakoutUpProb') AS DOUBLE)
    ) AS up_prob,
    GREATEST(
      CAST(json_extract(b.basis_payload_json, '$.weeklyBreakoutDownProb') AS DOUBLE),
      CAST(json_extract(b.basis_payload_json, '$.monthlyBreakoutDownProb') AS DOUBLE)
    ) AS rev_risk,
    json_extract_string(b.basis_payload_json, '$.monthlyBoxState') AS monthly_box_state
  FROM ranking_appearance_daily r
  JOIN signal_basis_daily b
    ON b.dt = r.dt AND b.code = r.code
  JOIN signal_decision_daily d
    ON d.dt = r.dt AND d.code = r.code AND d.side = 'buy'
  WHERE r.dir = ? AND r.rank = ? AND r.dt BETWEEN ? AND ?
),
month_ends AS (
  SELECT
    code,
    CAST(strftime(to_timestamp(date), '%Y%m') AS BIGINT) AS ym,
    MAX(date) AS month_end_dt
  FROM daily_bars
  WHERE CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER) BETWEEN ? AND ?
  GROUP BY code, CAST(strftime(to_timestamp(date), '%Y%m') AS BIGINT)
),
joined AS (
  SELECT
    c.*,
    db.c AS month_end_close
  FROM candidates c
  JOIN month_ends me
    ON me.code = c.code AND me.ym = c.ym
  JOIN daily_bars db
    ON db.code = me.code AND db.date = me.month_end_dt
)
SELECT
  ym,
  monthly_box_state AS context_state,
  dt,
  code,
  ((month_end_close / entry_price) - 1.0) * 100.0 AS month_end_ret_pct,
  (
    liquidity20d >= 500000
    AND up_prob >= 0.75
    AND ev >= 0.05
    AND rev_risk <= 0.40
  ) AS baseline_pass,
  (
    liquidity20d >= 500000
    AND up_prob >= 0.75
    AND monthly_box_state IN ('box_upper', 'box_mid', 'box_lower')
    AND (
      CASE
        WHEN monthly_box_state = 'box_upper' THEN (ev >= 0.045 AND rev_risk <= 0.36)
        WHEN monthly_box_state = 'box_mid' THEN (ev >= 0.05 AND rev_risk <= 0.32)
        WHEN monthly_box_state = 'box_lower' THEN (ev >= 0.06 AND rev_risk <= 0.24)
        ELSE FALSE
      END
    )
  ) AS variant_pass
FROM joined
WHERE entry_price IS NOT NULL AND entry_price > 0 AND month_end_close IS NOT NULL
ORDER BY dt
"""
    elif variant == "monthly_box_ev_rr_v3_upper_m4":
        sql = """
WITH candidates AS (
  SELECT
    r.dt,
    r.code,
    CAST(r.dt/100 AS BIGINT) AS ym,
    r.anchor_price_next_open AS entry_price,
    CAST(json_extract(b.basis_payload_json, '$.liquidity20d') AS DOUBLE) AS liquidity20d,
    CAST(json_extract(b.basis_payload_json, '$.changePct') AS DOUBLE) AS ev,
    GREATEST(
      CAST(json_extract(d.score_snapshot_json, '$.probSideCalib') AS DOUBLE),
      CAST(json_extract(d.score_snapshot_json, '$.probSide') AS DOUBLE),
      CAST(json_extract(b.basis_payload_json, '$.weeklyBreakoutUpProb') AS DOUBLE),
      CAST(json_extract(b.basis_payload_json, '$.monthlyBreakoutUpProb') AS DOUBLE)
    ) AS up_prob,
    GREATEST(
      CAST(json_extract(b.basis_payload_json, '$.weeklyBreakoutDownProb') AS DOUBLE),
      CAST(json_extract(b.basis_payload_json, '$.monthlyBreakoutDownProb') AS DOUBLE)
    ) AS rev_risk,
    json_extract_string(b.basis_payload_json, '$.monthlyBoxState') AS monthly_box_state,
    CAST(json_extract(b.basis_payload_json, '$.monthlyBoxMonths') AS DOUBLE) AS monthly_box_months
  FROM ranking_appearance_daily r
  JOIN signal_basis_daily b
    ON b.dt = r.dt AND b.code = r.code
  JOIN signal_decision_daily d
    ON d.dt = r.dt AND d.code = r.code AND d.side = 'buy'
  WHERE r.dir = ? AND r.rank = ? AND r.dt BETWEEN ? AND ?
),
month_ends AS (
  SELECT
    code,
    CAST(strftime(to_timestamp(date), '%Y%m') AS BIGINT) AS ym,
    MAX(date) AS month_end_dt
  FROM daily_bars
  WHERE CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER) BETWEEN ? AND ?
  GROUP BY code, CAST(strftime(to_timestamp(date), '%Y%m') AS BIGINT)
),
joined AS (
  SELECT
    c.*,
    db.c AS month_end_close
  FROM candidates c
  JOIN month_ends me
    ON me.code = c.code AND me.ym = c.ym
  JOIN daily_bars db
    ON db.code = me.code AND db.date = me.month_end_dt
)
SELECT
  ym,
  monthly_box_state AS context_state,
  dt,
  code,
  ((month_end_close / entry_price) - 1.0) * 100.0 AS month_end_ret_pct,
  (
    liquidity20d >= 500000
    AND up_prob >= 0.75
    AND ev >= 0.05
    AND rev_risk <= 0.40
  ) AS baseline_pass,
  (
    liquidity20d >= 500000
    AND up_prob >= 0.75
    AND monthly_box_state = 'box_upper'
    AND monthly_box_months >= 4
    AND (ev >= 0.045 AND rev_risk <= 0.36)
  ) AS variant_pass
FROM joined
WHERE entry_price IS NOT NULL AND entry_price > 0 AND month_end_close IS NOT NULL
ORDER BY dt
"""
    elif variant == "monthly_range_width_ev_rr_v1":
        sql = """
WITH candidates AS (
  SELECT
    r.dt,
    r.code,
    CAST(r.dt/100 AS BIGINT) AS ym,
    r.anchor_price_next_open AS entry_price,
    CAST(json_extract(b.basis_payload_json, '$.liquidity20d') AS DOUBLE) AS liquidity20d,
    CAST(json_extract(b.basis_payload_json, '$.changePct') AS DOUBLE) AS ev,
    GREATEST(
      CAST(json_extract(d.score_snapshot_json, '$.probSideCalib') AS DOUBLE),
      CAST(json_extract(d.score_snapshot_json, '$.probSide') AS DOUBLE),
      CAST(json_extract(b.basis_payload_json, '$.weeklyBreakoutUpProb') AS DOUBLE),
      CAST(json_extract(b.basis_payload_json, '$.monthlyBreakoutUpProb') AS DOUBLE)
    ) AS up_prob,
    GREATEST(
      CAST(json_extract(b.basis_payload_json, '$.weeklyBreakoutDownProb') AS DOUBLE),
      CAST(json_extract(b.basis_payload_json, '$.monthlyBreakoutDownProb') AS DOUBLE)
    ) AS rev_risk,
    CAST(json_extract(b.basis_payload_json, '$.monthlyRangeWidth') AS DOUBLE) AS monthly_range_width
  FROM ranking_appearance_daily r
  JOIN signal_basis_daily b
    ON b.dt = r.dt AND b.code = r.code
  JOIN signal_decision_daily d
    ON d.dt = r.dt AND d.code = r.code AND d.side = 'buy'
  WHERE r.dir = ? AND r.rank = ? AND r.dt BETWEEN ? AND ?
),
month_ends AS (
  SELECT
    code,
    CAST(strftime(to_timestamp(date), '%Y%m') AS BIGINT) AS ym,
    MAX(date) AS month_end_dt
  FROM daily_bars
  WHERE CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER) BETWEEN ? AND ?
  GROUP BY code, CAST(strftime(to_timestamp(date), '%Y%m') AS BIGINT)
),
joined AS (
  SELECT
    c.*,
    db.c AS month_end_close
  FROM candidates c
  JOIN month_ends me
    ON me.code = c.code AND me.ym = c.ym
  JOIN daily_bars db
    ON db.code = me.code AND db.date = me.month_end_dt
)
SELECT
  ym,
  (
    CASE
      WHEN monthly_range_width IS NULL THEN 'UNKNOWN'
      WHEN monthly_range_width <= 0.28 THEN 'tight'
      WHEN monthly_range_width <= 0.54 THEN 'normal'
      WHEN monthly_range_width <= 0.74 THEN 'wide'
      ELSE 'extreme_wide'
    END
  ) AS context_state,
  dt,
  code,
  ((month_end_close / entry_price) - 1.0) * 100.0 AS month_end_ret_pct,
  (
    liquidity20d >= 500000
    AND up_prob >= 0.75
    AND ev >= 0.05
    AND rev_risk <= 0.40
  ) AS baseline_pass,
  (
    liquidity20d >= 500000
    AND up_prob >= 0.75
    AND (
      CASE
        WHEN monthly_range_width IS NULL THEN (ev >= 0.05 AND rev_risk <= 0.40)
        WHEN monthly_range_width <= 0.28 THEN (ev >= 0.05 AND rev_risk <= 0.38)
        WHEN monthly_range_width <= 0.54 THEN (ev >= 0.05 AND rev_risk <= 0.40)
        WHEN monthly_range_width <= 0.74 THEN (ev >= 0.055 AND rev_risk <= 0.35)
        ELSE (ev >= 0.06 AND rev_risk <= 0.30)
      END
    )
  ) AS variant_pass
FROM joined
WHERE entry_price IS NOT NULL AND entry_price > 0 AND month_end_close IS NOT NULL
 ORDER BY dt
 """
    elif variant == "monthly_range_width_ev_rr_v2_soften_wide":
        sql = """
WITH candidates AS (
  SELECT
    r.dt,
    r.code,
    CAST(r.dt/100 AS BIGINT) AS ym,
    r.anchor_price_next_open AS entry_price,
    CAST(json_extract(b.basis_payload_json, '$.liquidity20d') AS DOUBLE) AS liquidity20d,
    CAST(json_extract(b.basis_payload_json, '$.changePct') AS DOUBLE) AS ev,
    GREATEST(
      CAST(json_extract(d.score_snapshot_json, '$.probSideCalib') AS DOUBLE),
      CAST(json_extract(d.score_snapshot_json, '$.probSide') AS DOUBLE),
      CAST(json_extract(b.basis_payload_json, '$.weeklyBreakoutUpProb') AS DOUBLE),
      CAST(json_extract(b.basis_payload_json, '$.monthlyBreakoutUpProb') AS DOUBLE)
    ) AS up_prob,
    GREATEST(
      CAST(json_extract(b.basis_payload_json, '$.weeklyBreakoutDownProb') AS DOUBLE),
      CAST(json_extract(b.basis_payload_json, '$.monthlyBreakoutDownProb') AS DOUBLE)
    ) AS rev_risk,
    CAST(json_extract(b.basis_payload_json, '$.monthlyRangeWidth') AS DOUBLE) AS monthly_range_width
  FROM ranking_appearance_daily r
  JOIN signal_basis_daily b
    ON b.dt = r.dt AND b.code = r.code
  JOIN signal_decision_daily d
    ON d.dt = r.dt AND d.code = r.code AND d.side = 'buy'
  WHERE r.dir = ? AND r.rank = ? AND r.dt BETWEEN ? AND ?
),
month_ends AS (
  SELECT
    code,
    CAST(strftime(to_timestamp(date), '%Y%m') AS BIGINT) AS ym,
    MAX(date) AS month_end_dt
  FROM daily_bars
  WHERE CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER) BETWEEN ? AND ?
  GROUP BY code, CAST(strftime(to_timestamp(date), '%Y%m') AS BIGINT)
),
joined AS (
  SELECT
    c.*,
    db.c AS month_end_close
  FROM candidates c
  JOIN month_ends me
    ON me.code = c.code AND me.ym = c.ym
  JOIN daily_bars db
    ON db.code = me.code AND db.date = me.month_end_dt
)
SELECT
  ym,
  (
    CASE
      WHEN monthly_range_width IS NULL THEN 'UNKNOWN'
      WHEN monthly_range_width <= 0.28 THEN 'tight'
      WHEN monthly_range_width <= 0.54 THEN 'normal'
      WHEN monthly_range_width <= 0.74 THEN 'wide'
      ELSE 'extreme_wide'
    END
  ) AS context_state,
  dt,
  code,
  ((month_end_close / entry_price) - 1.0) * 100.0 AS month_end_ret_pct,
  (
    liquidity20d >= 500000
    AND up_prob >= 0.75
    AND ev >= 0.05
    AND rev_risk <= 0.40
  ) AS baseline_pass,
  (
    liquidity20d >= 500000
    AND up_prob >= 0.75
    AND (
      CASE
        WHEN monthly_range_width IS NULL THEN (ev >= 0.05 AND rev_risk <= 0.40)
        WHEN monthly_range_width <= 0.28 THEN (ev >= 0.05 AND rev_risk <= 0.38)
        WHEN monthly_range_width <= 0.54 THEN (ev >= 0.05 AND rev_risk <= 0.40)
        WHEN monthly_range_width <= 0.74 THEN (ev >= 0.0525 AND rev_risk <= 0.37)
        ELSE (ev >= 0.055 AND rev_risk <= 0.33)
      END
    )
  ) AS variant_pass
FROM joined
WHERE entry_price IS NOT NULL AND entry_price > 0 AND month_end_close IS NOT NULL
ORDER BY dt
"""
    elif variant == "monthly_range_width_ev_rr_v3_exclude_normal":
        sql = """
WITH candidates AS (
  SELECT
    r.dt,
    r.code,
    CAST(r.dt/100 AS BIGINT) AS ym,
    r.anchor_price_next_open AS entry_price,
    CAST(json_extract(b.basis_payload_json, '$.liquidity20d') AS DOUBLE) AS liquidity20d,
    CAST(json_extract(b.basis_payload_json, '$.changePct') AS DOUBLE) AS ev,
    GREATEST(
      CAST(json_extract(d.score_snapshot_json, '$.probSideCalib') AS DOUBLE),
      CAST(json_extract(d.score_snapshot_json, '$.probSide') AS DOUBLE),
      CAST(json_extract(b.basis_payload_json, '$.weeklyBreakoutUpProb') AS DOUBLE),
      CAST(json_extract(b.basis_payload_json, '$.monthlyBreakoutUpProb') AS DOUBLE)
    ) AS up_prob,
    GREATEST(
      CAST(json_extract(b.basis_payload_json, '$.weeklyBreakoutDownProb') AS DOUBLE),
      CAST(json_extract(b.basis_payload_json, '$.monthlyBreakoutDownProb') AS DOUBLE)
    ) AS rev_risk,
    CAST(json_extract(b.basis_payload_json, '$.monthlyRangeWidth') AS DOUBLE) AS monthly_range_width
  FROM ranking_appearance_daily r
  JOIN signal_basis_daily b
    ON b.dt = r.dt AND b.code = r.code
  JOIN signal_decision_daily d
    ON d.dt = r.dt AND d.code = r.code AND d.side = 'buy'
  WHERE r.dir = ? AND r.rank = ? AND r.dt BETWEEN ? AND ?
),
month_ends AS (
  SELECT
    code,
    CAST(strftime(to_timestamp(date), '%Y%m') AS BIGINT) AS ym,
    MAX(date) AS month_end_dt
  FROM daily_bars
  WHERE CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER) BETWEEN ? AND ?
  GROUP BY code, CAST(strftime(to_timestamp(date), '%Y%m') AS BIGINT)
),
joined AS (
  SELECT
    c.*,
    db.c AS month_end_close
  FROM candidates c
  JOIN month_ends me
    ON me.code = c.code AND me.ym = c.ym
  JOIN daily_bars db
    ON db.code = me.code AND db.date = me.month_end_dt
)
SELECT
  ym,
  (
    CASE
      WHEN monthly_range_width IS NULL THEN 'UNKNOWN'
      WHEN monthly_range_width <= 0.28 THEN 'tight'
      WHEN monthly_range_width <= 0.54 THEN 'normal'
      WHEN monthly_range_width <= 0.74 THEN 'wide'
      ELSE 'extreme_wide'
    END
  ) AS context_state,
  dt,
  code,
  ((month_end_close / entry_price) - 1.0) * 100.0 AS month_end_ret_pct,
  (
    liquidity20d >= 500000
    AND up_prob >= 0.75
    AND ev >= 0.05
    AND rev_risk <= 0.40
  ) AS baseline_pass,
  (
    liquidity20d >= 500000
    AND up_prob >= 0.75
    AND monthly_range_width IS NOT NULL
    AND (
      monthly_range_width <= 0.28
      OR (monthly_range_width > 0.54 AND monthly_range_width <= 0.74)
      OR (monthly_range_width > 0.74)
    )
    AND (
      CASE
        WHEN monthly_range_width <= 0.28 THEN (ev >= 0.055 AND rev_risk <= 0.36)
        WHEN monthly_range_width <= 0.54 THEN FALSE
        WHEN monthly_range_width <= 0.74 THEN (ev >= 0.0525 AND rev_risk <= 0.37)
        ELSE (ev >= 0.045 AND rev_risk <= 0.42)
      END
    )
  ) AS variant_pass
FROM joined
WHERE entry_price IS NOT NULL AND entry_price > 0 AND month_end_close IS NOT NULL
ORDER BY dt
"""
    elif variant == "monthly_range_width_ev_rr_v4_tighten_normal_drop_unknown":
        sql = """
WITH candidates AS (
  SELECT
    r.dt,
    r.code,
    CAST(r.dt/100 AS BIGINT) AS ym,
    r.anchor_price_next_open AS entry_price,
    CAST(json_extract(b.basis_payload_json, '$.liquidity20d') AS DOUBLE) AS liquidity20d,
    CAST(json_extract(b.basis_payload_json, '$.changePct') AS DOUBLE) AS ev,
    GREATEST(
      CAST(json_extract(d.score_snapshot_json, '$.probSideCalib') AS DOUBLE),
      CAST(json_extract(d.score_snapshot_json, '$.probSide') AS DOUBLE),
      CAST(json_extract(b.basis_payload_json, '$.weeklyBreakoutUpProb') AS DOUBLE),
      CAST(json_extract(b.basis_payload_json, '$.monthlyBreakoutUpProb') AS DOUBLE)
    ) AS up_prob,
    GREATEST(
      CAST(json_extract(b.basis_payload_json, '$.weeklyBreakoutDownProb') AS DOUBLE),
      CAST(json_extract(b.basis_payload_json, '$.monthlyBreakoutDownProb') AS DOUBLE)
    ) AS rev_risk,
    CAST(json_extract(b.basis_payload_json, '$.monthlyRangeWidth') AS DOUBLE) AS monthly_range_width
  FROM ranking_appearance_daily r
  JOIN signal_basis_daily b
    ON b.dt = r.dt AND b.code = r.code
  JOIN signal_decision_daily d
    ON d.dt = r.dt AND d.code = r.code AND d.side = 'buy'
  WHERE r.dir = ? AND r.rank = ? AND r.dt BETWEEN ? AND ?
),
month_ends AS (
  SELECT
    code,
    CAST(strftime(to_timestamp(date), '%Y%m') AS BIGINT) AS ym,
    MAX(date) AS month_end_dt
  FROM daily_bars
  WHERE CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER) BETWEEN ? AND ?
  GROUP BY code, CAST(strftime(to_timestamp(date), '%Y%m') AS BIGINT)
),
joined AS (
  SELECT
    c.*,
    db.c AS month_end_close
  FROM candidates c
  JOIN month_ends me
    ON me.code = c.code AND me.ym = c.ym
  JOIN daily_bars db
    ON db.code = me.code AND db.date = me.month_end_dt
)
SELECT
  ym,
  (
    CASE
      WHEN monthly_range_width IS NULL THEN 'UNKNOWN'
      WHEN monthly_range_width <= 0.28 THEN 'tight'
      WHEN monthly_range_width <= 0.54 THEN 'normal'
      WHEN monthly_range_width <= 0.74 THEN 'wide'
      ELSE 'extreme_wide'
    END
  ) AS context_state,
  dt,
  code,
  ((month_end_close / entry_price) - 1.0) * 100.0 AS month_end_ret_pct,
  (
    liquidity20d >= 500000
    AND up_prob >= 0.75
    AND ev >= 0.05
    AND rev_risk <= 0.40
  ) AS baseline_pass,
  (
    liquidity20d >= 500000
    AND up_prob >= 0.75
    AND monthly_range_width IS NOT NULL
    AND (
      CASE
        WHEN monthly_range_width <= 0.28 THEN (ev >= 0.05 AND rev_risk <= 0.40)
        WHEN monthly_range_width <= 0.54 THEN (ev >= 0.055 AND rev_risk <= 0.36)
        WHEN monthly_range_width <= 0.74 THEN (ev >= 0.0525 AND rev_risk <= 0.37)
        ELSE (ev >= 0.05 AND rev_risk <= 0.40)
      END
    )
  ) AS variant_pass
FROM joined
WHERE entry_price IS NOT NULL AND entry_price > 0 AND month_end_close IS NOT NULL
ORDER BY dt
"""
    elif variant == "monthly_range_width_ev_rr_v6_strict_normal_drop_unknown":
        sql = """
WITH candidates AS (
  SELECT
    r.dt,
    r.code,
    CAST(r.dt/100 AS BIGINT) AS ym,
    r.anchor_price_next_open AS entry_price,
    CAST(json_extract(b.basis_payload_json, '$.liquidity20d') AS DOUBLE) AS liquidity20d,
    CAST(json_extract(b.basis_payload_json, '$.changePct') AS DOUBLE) AS ev,
    GREATEST(
      CAST(json_extract(d.score_snapshot_json, '$.probSideCalib') AS DOUBLE),
      CAST(json_extract(d.score_snapshot_json, '$.probSide') AS DOUBLE),
      CAST(json_extract(b.basis_payload_json, '$.weeklyBreakoutUpProb') AS DOUBLE),
      CAST(json_extract(b.basis_payload_json, '$.monthlyBreakoutUpProb') AS DOUBLE)
    ) AS up_prob,
    GREATEST(
      CAST(json_extract(b.basis_payload_json, '$.weeklyBreakoutDownProb') AS DOUBLE),
      CAST(json_extract(b.basis_payload_json, '$.monthlyBreakoutDownProb') AS DOUBLE)
    ) AS rev_risk,
    CAST(json_extract(b.basis_payload_json, '$.monthlyRangeWidth') AS DOUBLE) AS monthly_range_width
  FROM ranking_appearance_daily r
  JOIN signal_basis_daily b
    ON b.dt = r.dt AND b.code = r.code
  JOIN signal_decision_daily d
    ON d.dt = r.dt AND d.code = r.code AND d.side = 'buy'
  WHERE r.dir = ? AND r.rank = ? AND r.dt BETWEEN ? AND ?
),
month_ends AS (
  SELECT
    code,
    CAST(strftime(to_timestamp(date), '%Y%m') AS BIGINT) AS ym,
    MAX(date) AS month_end_dt
  FROM daily_bars
  WHERE CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER) BETWEEN ? AND ?
  GROUP BY code, CAST(strftime(to_timestamp(date), '%Y%m') AS BIGINT)
),
joined AS (
  SELECT
    c.*,
    db.c AS month_end_close
  FROM candidates c
  JOIN month_ends me
    ON me.code = c.code AND me.ym = c.ym
  JOIN daily_bars db
    ON db.code = me.code AND db.date = me.month_end_dt
)
SELECT
  ym,
  (
    CASE
      WHEN monthly_range_width IS NULL THEN 'UNKNOWN'
      WHEN monthly_range_width <= 0.28 THEN 'tight'
      WHEN monthly_range_width <= 0.54 THEN 'normal'
      WHEN monthly_range_width <= 0.74 THEN 'wide'
      ELSE 'extreme_wide'
    END
  ) AS context_state,
  dt,
  code,
  ((month_end_close / entry_price) - 1.0) * 100.0 AS month_end_ret_pct,
  (
    liquidity20d >= 500000
    AND up_prob >= 0.75
    AND ev >= 0.05
    AND rev_risk <= 0.40
  ) AS baseline_pass,
  (
    liquidity20d >= 500000
    AND up_prob >= 0.75
    AND monthly_range_width IS NOT NULL
    AND (
      CASE
        WHEN monthly_range_width <= 0.28 THEN (ev >= 0.05 AND rev_risk <= 0.40)
        WHEN monthly_range_width <= 0.54 THEN (ev >= 0.065 AND rev_risk <= 0.33)
        WHEN monthly_range_width <= 0.74 THEN (ev >= 0.0525 AND rev_risk <= 0.37)
        ELSE (ev >= 0.05 AND rev_risk <= 0.40)
      END
    )
  ) AS variant_pass
FROM joined
WHERE entry_price IS NOT NULL AND entry_price > 0 AND month_end_close IS NOT NULL
ORDER BY dt
"""
    elif variant == "monthly_range_width_ev_rr_v7_soft_normal_drop_unknown":
        sql = """
WITH candidates AS (
  SELECT
    r.dt,
    r.code,
    CAST(r.dt/100 AS BIGINT) AS ym,
    r.anchor_price_next_open AS entry_price,
    CAST(json_extract(b.basis_payload_json, '$.liquidity20d') AS DOUBLE) AS liquidity20d,
    CAST(json_extract(b.basis_payload_json, '$.changePct') AS DOUBLE) AS ev,
    GREATEST(
      CAST(json_extract(d.score_snapshot_json, '$.probSideCalib') AS DOUBLE),
      CAST(json_extract(d.score_snapshot_json, '$.probSide') AS DOUBLE),
      CAST(json_extract(b.basis_payload_json, '$.weeklyBreakoutUpProb') AS DOUBLE),
      CAST(json_extract(b.basis_payload_json, '$.monthlyBreakoutUpProb') AS DOUBLE)
    ) AS up_prob,
    GREATEST(
      CAST(json_extract(b.basis_payload_json, '$.weeklyBreakoutDownProb') AS DOUBLE),
      CAST(json_extract(b.basis_payload_json, '$.monthlyBreakoutDownProb') AS DOUBLE)
    ) AS rev_risk,
    CAST(json_extract(b.basis_payload_json, '$.monthlyRangeWidth') AS DOUBLE) AS monthly_range_width
  FROM ranking_appearance_daily r
  JOIN signal_basis_daily b
    ON b.dt = r.dt AND b.code = r.code
  JOIN signal_decision_daily d
    ON d.dt = r.dt AND d.code = r.code AND d.side = 'buy'
  WHERE r.dir = ? AND r.rank = ? AND r.dt BETWEEN ? AND ?
),
month_ends AS (
  SELECT
    code,
    CAST(strftime(to_timestamp(date), '%Y%m') AS BIGINT) AS ym,
    MAX(date) AS month_end_dt
  FROM daily_bars
  WHERE CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER) BETWEEN ? AND ?
  GROUP BY code, CAST(strftime(to_timestamp(date), '%Y%m') AS BIGINT)
),
joined AS (
  SELECT
    c.*,
    db.c AS month_end_close
  FROM candidates c
  JOIN month_ends me
    ON me.code = c.code AND me.ym = c.ym
  JOIN daily_bars db
    ON db.code = me.code AND db.date = me.month_end_dt
)
SELECT
  ym,
  (
    CASE
      WHEN monthly_range_width IS NULL THEN 'UNKNOWN'
      WHEN monthly_range_width <= 0.28 THEN 'tight'
      WHEN monthly_range_width <= 0.54 THEN 'normal'
      WHEN monthly_range_width <= 0.74 THEN 'wide'
      ELSE 'extreme_wide'
    END
  ) AS context_state,
  dt,
  code,
  ((month_end_close / entry_price) - 1.0) * 100.0 AS month_end_ret_pct,
  (
    liquidity20d >= 500000
    AND up_prob >= 0.75
    AND ev >= 0.05
    AND rev_risk <= 0.40
  ) AS baseline_pass,
  (
    liquidity20d >= 500000
    AND up_prob >= 0.75
    AND monthly_range_width IS NOT NULL
    AND (
      CASE
        WHEN monthly_range_width <= 0.28 THEN (ev >= 0.05 AND rev_risk <= 0.40)
        WHEN monthly_range_width <= 0.54 THEN (ev >= 0.06 AND rev_risk <= 0.35)
        WHEN monthly_range_width <= 0.74 THEN (ev >= 0.0525 AND rev_risk <= 0.37)
        ELSE (ev >= 0.05 AND rev_risk <= 0.40)
      END
    )
  ) AS variant_pass
FROM joined
WHERE entry_price IS NOT NULL AND entry_price > 0 AND month_end_close IS NOT NULL
ORDER BY dt
"""
    elif variant == "monthly_range_width_ev_rr_v8_tighten_wide_extreme_drop_unknown":
        sql = """
WITH candidates AS (
  SELECT
    r.dt,
    r.code,
    CAST(r.dt/100 AS BIGINT) AS ym,
    r.anchor_price_next_open AS entry_price,
    CAST(json_extract(b.basis_payload_json, '$.liquidity20d') AS DOUBLE) AS liquidity20d,
    CAST(json_extract(b.basis_payload_json, '$.changePct') AS DOUBLE) AS ev,
    GREATEST(
      CAST(json_extract(d.score_snapshot_json, '$.probSideCalib') AS DOUBLE),
      CAST(json_extract(d.score_snapshot_json, '$.probSide') AS DOUBLE),
      CAST(json_extract(b.basis_payload_json, '$.weeklyBreakoutUpProb') AS DOUBLE),
      CAST(json_extract(b.basis_payload_json, '$.monthlyBreakoutUpProb') AS DOUBLE)
    ) AS up_prob,
    GREATEST(
      CAST(json_extract(b.basis_payload_json, '$.weeklyBreakoutDownProb') AS DOUBLE),
      CAST(json_extract(b.basis_payload_json, '$.monthlyBreakoutDownProb') AS DOUBLE)
    ) AS rev_risk,
    CAST(json_extract(b.basis_payload_json, '$.monthlyRangeWidth') AS DOUBLE) AS monthly_range_width
  FROM ranking_appearance_daily r
  JOIN signal_basis_daily b
    ON b.dt = r.dt AND b.code = r.code
  JOIN signal_decision_daily d
    ON d.dt = r.dt AND d.code = r.code AND d.side = 'buy'
  WHERE r.dir = ? AND r.rank = ? AND r.dt BETWEEN ? AND ?
),
month_ends AS (
  SELECT
    code,
    CAST(strftime(to_timestamp(date), '%Y%m') AS BIGINT) AS ym,
    MAX(date) AS month_end_dt
  FROM daily_bars
  WHERE CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER) BETWEEN ? AND ?
  GROUP BY code, CAST(strftime(to_timestamp(date), '%Y%m') AS BIGINT)
),
joined AS (
  SELECT
    c.*,
    db.c AS month_end_close
  FROM candidates c
  JOIN month_ends me
    ON me.code = c.code AND me.ym = c.ym
  JOIN daily_bars db
    ON db.code = me.code AND db.date = me.month_end_dt
)
SELECT
  ym,
  (
    CASE
      WHEN monthly_range_width IS NULL THEN 'UNKNOWN'
      WHEN monthly_range_width <= 0.28 THEN 'tight'
      WHEN monthly_range_width <= 0.54 THEN 'normal'
      WHEN monthly_range_width <= 0.74 THEN 'wide'
      ELSE 'extreme_wide'
    END
  ) AS context_state,
  dt,
  code,
  ((month_end_close / entry_price) - 1.0) * 100.0 AS month_end_ret_pct,
  (
    liquidity20d >= 500000
    AND up_prob >= 0.75
    AND ev >= 0.05
    AND rev_risk <= 0.40
  ) AS baseline_pass,
  (
    liquidity20d >= 500000
    AND up_prob >= 0.75
    AND monthly_range_width IS NOT NULL
    AND (
      CASE
        WHEN monthly_range_width <= 0.28 THEN (ev >= 0.05 AND rev_risk <= 0.40)
        WHEN monthly_range_width <= 0.54 THEN (ev >= 0.05 AND rev_risk <= 0.40)
        WHEN monthly_range_width <= 0.74 THEN (ev >= 0.06 AND rev_risk <= 0.34)
        ELSE (ev >= 0.065 AND rev_risk <= 0.33)
      END
    )
  ) AS variant_pass
FROM joined
WHERE entry_price IS NOT NULL AND entry_price > 0 AND month_end_close IS NOT NULL
ORDER BY dt
"""
    elif variant == "monthly_range_width_ev_rr_v9_tighten_tight_normal_drop_unknown":
        sql = """
WITH candidates AS (
  SELECT
    r.dt,
    r.code,
    CAST(r.dt/100 AS BIGINT) AS ym,
    r.anchor_price_next_open AS entry_price,
    CAST(json_extract(b.basis_payload_json, '$.liquidity20d') AS DOUBLE) AS liquidity20d,
    CAST(json_extract(b.basis_payload_json, '$.changePct') AS DOUBLE) AS ev,
    GREATEST(
      CAST(json_extract(d.score_snapshot_json, '$.probSideCalib') AS DOUBLE),
      CAST(json_extract(d.score_snapshot_json, '$.probSide') AS DOUBLE),
      CAST(json_extract(b.basis_payload_json, '$.weeklyBreakoutUpProb') AS DOUBLE),
      CAST(json_extract(b.basis_payload_json, '$.monthlyBreakoutUpProb') AS DOUBLE)
    ) AS up_prob,
    GREATEST(
      CAST(json_extract(b.basis_payload_json, '$.weeklyBreakoutDownProb') AS DOUBLE),
      CAST(json_extract(b.basis_payload_json, '$.monthlyBreakoutDownProb') AS DOUBLE)
    ) AS rev_risk,
    CAST(json_extract(b.basis_payload_json, '$.monthlyRangeWidth') AS DOUBLE) AS monthly_range_width
  FROM ranking_appearance_daily r
  JOIN signal_basis_daily b
    ON b.dt = r.dt AND b.code = r.code
  JOIN signal_decision_daily d
    ON d.dt = r.dt AND d.code = r.code AND d.side = 'buy'
  WHERE r.dir = ? AND r.rank = ? AND r.dt BETWEEN ? AND ?
),
month_ends AS (
  SELECT
    code,
    CAST(strftime(to_timestamp(date), '%Y%m') AS BIGINT) AS ym,
    MAX(date) AS month_end_dt
  FROM daily_bars
  WHERE CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER) BETWEEN ? AND ?
  GROUP BY code, CAST(strftime(to_timestamp(date), '%Y%m') AS BIGINT)
),
joined AS (
  SELECT
    c.*,
    db.c AS month_end_close
  FROM candidates c
  JOIN month_ends me
    ON me.code = c.code AND me.ym = c.ym
  JOIN daily_bars db
    ON db.code = me.code AND db.date = me.month_end_dt
)
SELECT
  ym,
  (
    CASE
      WHEN monthly_range_width IS NULL THEN 'UNKNOWN'
      WHEN monthly_range_width <= 0.28 THEN 'tight'
      WHEN monthly_range_width <= 0.54 THEN 'normal'
      WHEN monthly_range_width <= 0.74 THEN 'wide'
      ELSE 'extreme_wide'
    END
  ) AS context_state,
  dt,
  code,
  ((month_end_close / entry_price) - 1.0) * 100.0 AS month_end_ret_pct,
  (
    liquidity20d >= 500000
    AND up_prob >= 0.75
    AND ev >= 0.05
    AND rev_risk <= 0.40
  ) AS baseline_pass,
  (
    liquidity20d >= 500000
    AND up_prob >= 0.75
    AND monthly_range_width IS NOT NULL
    AND (
      CASE
        WHEN monthly_range_width <= 0.28 THEN (ev >= 0.06 AND rev_risk <= 0.35)
        WHEN monthly_range_width <= 0.54 THEN (ev >= 0.065 AND rev_risk <= 0.33)
        WHEN monthly_range_width <= 0.74 THEN (ev >= 0.06 AND rev_risk <= 0.34)
        ELSE (ev >= 0.065 AND rev_risk <= 0.33)
      END
    )
  ) AS variant_pass
FROM joined
WHERE entry_price IS NOT NULL AND entry_price > 0 AND month_end_close IS NOT NULL
ORDER BY dt
"""
    elif variant == "monthly_range_width_ev_rr_v5_exclude_extreme_wide":
        sql = """
WITH candidates AS (
  SELECT
    r.dt,
    r.code,
    CAST(r.dt/100 AS BIGINT) AS ym,
    r.anchor_price_next_open AS entry_price,
    CAST(json_extract(b.basis_payload_json, '$.liquidity20d') AS DOUBLE) AS liquidity20d,
    CAST(json_extract(b.basis_payload_json, '$.changePct') AS DOUBLE) AS ev,
    GREATEST(
      CAST(json_extract(d.score_snapshot_json, '$.probSideCalib') AS DOUBLE),
      CAST(json_extract(d.score_snapshot_json, '$.probSide') AS DOUBLE),
      CAST(json_extract(b.basis_payload_json, '$.weeklyBreakoutUpProb') AS DOUBLE),
      CAST(json_extract(b.basis_payload_json, '$.monthlyBreakoutUpProb') AS DOUBLE)
    ) AS up_prob,
    GREATEST(
      CAST(json_extract(b.basis_payload_json, '$.weeklyBreakoutDownProb') AS DOUBLE),
      CAST(json_extract(b.basis_payload_json, '$.monthlyBreakoutDownProb') AS DOUBLE)
    ) AS rev_risk,
    CAST(json_extract(b.basis_payload_json, '$.monthlyRangeWidth') AS DOUBLE) AS monthly_range_width
  FROM ranking_appearance_daily r
  JOIN signal_basis_daily b
    ON b.dt = r.dt AND b.code = r.code
  JOIN signal_decision_daily d
    ON d.dt = r.dt AND d.code = r.code AND d.side = 'buy'
  WHERE r.dir = ? AND r.rank = ? AND r.dt BETWEEN ? AND ?
),
month_ends AS (
  SELECT
    code,
    CAST(strftime(to_timestamp(date), '%Y%m') AS BIGINT) AS ym,
    MAX(date) AS month_end_dt
  FROM daily_bars
  WHERE CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER) BETWEEN ? AND ?
  GROUP BY code, CAST(strftime(to_timestamp(date), '%Y%m') AS BIGINT)
),
joined AS (
  SELECT
    c.*,
    db.c AS month_end_close
  FROM candidates c
  JOIN month_ends me
    ON me.code = c.code AND me.ym = c.ym
  JOIN daily_bars db
    ON db.code = me.code AND db.date = me.month_end_dt
)
SELECT
  ym,
  (
    CASE
      WHEN monthly_range_width IS NULL THEN 'UNKNOWN'
      WHEN monthly_range_width <= 0.28 THEN 'tight'
      WHEN monthly_range_width <= 0.54 THEN 'normal'
      WHEN monthly_range_width <= 0.74 THEN 'wide'
      ELSE 'extreme_wide'
    END
  ) AS context_state,
  dt,
  code,
  ((month_end_close / entry_price) - 1.0) * 100.0 AS month_end_ret_pct,
  (
    liquidity20d >= 500000
    AND up_prob >= 0.75
    AND ev >= 0.05
    AND rev_risk <= 0.40
  ) AS baseline_pass,
  (
    liquidity20d >= 500000
    AND up_prob >= 0.75
    AND monthly_range_width IS NOT NULL
    AND monthly_range_width <= 0.74
    AND (
      CASE
        WHEN monthly_range_width <= 0.28 THEN (ev >= 0.05 AND rev_risk <= 0.40)
        WHEN monthly_range_width <= 0.54 THEN (ev >= 0.05 AND rev_risk <= 0.40)
        WHEN monthly_range_width <= 0.74 THEN (ev >= 0.0525 AND rev_risk <= 0.37)
        ELSE FALSE
      END
    )
  ) AS variant_pass
FROM joined
WHERE entry_price IS NOT NULL AND entry_price > 0 AND month_end_close IS NOT NULL
ORDER BY dt
"""
    else:
        raise ValueError(f"Unknown variant: {variant}")
    return conn.execute(
        sql,
        [
            side_dir,
            int(rank),
            int(start_ymd),
            int(end_ymd),
            int(start_ymd),
            int(end_ymd),
        ],
    ).fetchall()


def run_proxy(
    *,
    db_path: Path,
    start_ymd: int,
    end_ymd: int,
    side_dir: str,
    rank: int,
    variant: str,
) -> dict[str, Any]:
    with duckdb.connect(str(db_path), read_only=True) as conn:
        rows = _fetch_rows(
            conn,
            start_ymd=start_ymd,
            end_ymd=end_ymd,
            side_dir=side_dir,
            rank=rank,
            variant=variant,
        )

    # month -> counts / ret buckets
    month_days: dict[int, int] = defaultdict(int)
    month_context_counts: dict[int, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    month_base_rets: dict[int, list[float]] = defaultdict(list)
    month_var_rets: dict[int, list[float]] = defaultdict(list)

    for ym, context_state, dt, code, ret_pct, base_pass, var_pass in rows:
        month_days[int(ym)] += 1
        st = str(context_state) if context_state is not None else "UNKNOWN"
        month_context_counts[int(ym)][st] += 1
        if bool(base_pass):
            month_base_rets[int(ym)].append(float(ret_pct))
        if bool(var_pass):
            month_var_rets[int(ym)].append(float(ret_pct))

    months: dict[str, Any] = {}
    for ym in sorted(month_days.keys()):
        months[str(ym)[:4] + "-" + str(ym)[4:6]] = {
            "rank1_days": int(month_days[ym]),
            "monthly_state_counts": dict(
                sorted(month_context_counts[ym].items(), key=lambda kv: (-kv[1], kv[0]))
            ),
            "baseline": {
                "pass_days": int(len(month_base_rets[ym])),
                "month_end_ret_pct": _summary(month_base_rets[ym]),
            },
            "variant": {
                "pass_days": int(len(month_var_rets[ym])),
                "month_end_ret_pct": _summary(month_var_rets[ym]),
            },
        }

    feature_id = ""
    variant_gate: dict[str, Any] = {}
    if variant == "market_regime_ev_rr_v2":
        feature_id = f"blind_month_{start_ymd//100:06d}_market_regime_ev_rr_v2_proxy"
        variant_gate = {
            "id": "market_regime_ev_rr_v2",
            "market_regime_source": "signal_basis_daily.basis_payload_json.marketRegime",
            "thresholds_by_regime": {
                "risk_on": {"entryMinEv": 0.05, "entryMaxRevRisk": 0.35},
                "neutral": {"entryMinEv": 0.055, "entryMaxRevRisk": 0.33},
                "risk_off": {"entryMinEv": 0.06, "entryMaxRevRisk": 0.30},
                "UNKNOWN": {"entryMinEv": 0.05, "entryMaxRevRisk": 0.40},
            },
            "note": "Variant changes only entryMinEv and entryMaxRevRisk; minLiquidity20d and entryMinUpProb stay fixed.",
        }
    elif variant == "monthly_box_ev_rr_v1":
        feature_id = f"blind_month_{start_ymd//100:06d}_monthly_box_ev_rr_v1_proxy"
        variant_gate = {
            "id": "monthly_box_ev_rr_v1",
            "monthly_box_state_source": "signal_basis_daily.basis_payload_json.monthlyBoxState",
            "thresholds_by_state": {
                "box_upper": {"entryMinEv": 0.05, "entryMaxRevRisk": 0.33},
                "box_mid": {"entryMinEv": 0.055, "entryMaxRevRisk": 0.30},
                "box_lower": {"entryMinEv": 0.06, "entryMaxRevRisk": 0.25},
                "no_box": {"entryMinEv": 0.06, "entryMaxRevRisk": 0.28},
                "UNKNOWN": {"entryMinEv": 0.05, "entryMaxRevRisk": 0.40},
            },
            "note": "Variant changes only entryMinEv and entryMaxRevRisk; minLiquidity20d and entryMinUpProb stay fixed.",
        }
    elif variant == "monthly_box_ev_rr_v2_boxonly":
        feature_id = f"blind_month_{start_ymd//100:06d}_monthly_box_ev_rr_v2_boxonly_proxy"
        variant_gate = {
            "id": "monthly_box_ev_rr_v2_boxonly",
            "monthly_box_state_source": "signal_basis_daily.basis_payload_json.monthlyBoxState",
            "monthly_context_filter": {
                "allowed_states": ["box_upper", "box_mid", "box_lower"],
                "note": "Variant requires monthlyBoxState in allowed_states (filters out no_box/UNKNOWN).",
            },
            "thresholds_by_state": {
                "box_upper": {"entryMinEv": 0.045, "entryMaxRevRisk": 0.36},
                "box_mid": {"entryMinEv": 0.05, "entryMaxRevRisk": 0.32},
                "box_lower": {"entryMinEv": 0.06, "entryMaxRevRisk": 0.24},
            },
            "note": "Variant changes only entryMinEv and entryMaxRevRisk, plus a monthly-context filter on monthlyBoxState; minLiquidity20d and entryMinUpProb stay fixed.",
        }
    elif variant == "monthly_box_ev_rr_v3_upper_m4":
        feature_id = f"blind_month_{start_ymd//100:06d}_monthly_box_ev_rr_v3_upper_m4_proxy"
        variant_gate = {
            "id": "monthly_box_ev_rr_v3_upper_m4",
            "monthly_box_state_source": "signal_basis_daily.basis_payload_json.monthlyBoxState",
            "monthly_context_filter": {
                "required_state": "box_upper",
                "monthly_box_months_min": 4,
                "note": "Variant requires monthlyBoxState='box_upper' and monthlyBoxMonths>=4 (filters out no_box/UNKNOWN and short/weak boxes).",
            },
            "thresholds_by_state": {
                "box_upper": {"entryMinEv": 0.045, "entryMaxRevRisk": 0.36},
                "UNKNOWN": {"entryMinEv": 0.05, "entryMaxRevRisk": 0.40},
            },
            "note": "Variant changes only entryMinEv and entryMaxRevRisk, plus a monthly-context filter using monthlyBoxState/monthlyBoxMonths; minLiquidity20d and entryMinUpProb stay fixed.",
        }
    elif variant == "monthly_range_width_ev_rr_v1":
        feature_id = f"blind_month_{start_ymd//100:06d}_monthly_range_width_ev_rr_v1_proxy"
        variant_gate = {
            "id": "monthly_range_width_ev_rr_v1",
            "monthly_range_width_source": "signal_basis_daily.basis_payload_json.monthlyRangeWidth",
            "monthly_context_axis": {
                "bucket_by": "monthlyRangeWidth",
                "buckets": [
                    {"label": "tight", "max_inclusive": 0.28},
                    {"label": "normal", "max_inclusive": 0.54},
                    {"label": "wide", "max_inclusive": 0.74},
                    {"label": "extreme_wide", "min_exclusive": 0.74},
                ],
                "note": "Buckets use fixed cutoffs chosen from rank=1 monthlyRangeWidth quantiles (p25~0.28, p75~0.54, p90~0.74) to ensure the context axis activates.",
            },
            "thresholds_by_bucket": {
                "tight": {"entryMinEv": 0.05, "entryMaxRevRisk": 0.38},
                "normal": {"entryMinEv": 0.05, "entryMaxRevRisk": 0.40},
                "wide": {"entryMinEv": 0.055, "entryMaxRevRisk": 0.35},
                "extreme_wide": {"entryMinEv": 0.06, "entryMaxRevRisk": 0.30},
                "UNKNOWN": {"entryMinEv": 0.05, "entryMaxRevRisk": 0.40},
            },
            "note": "Variant changes only entryMinEv and entryMaxRevRisk; minLiquidity20d and entryMinUpProb stay fixed. Monthly context uses monthlyRangeWidth buckets.",
        }
    elif variant == "monthly_range_width_ev_rr_v2_soften_wide":
        feature_id = f"blind_month_{start_ymd//100:06d}_monthly_range_width_ev_rr_v2_soften_wide_proxy"
        variant_gate = {
            "id": "monthly_range_width_ev_rr_v2_soften_wide",
            "monthly_range_width_source": "signal_basis_daily.basis_payload_json.monthlyRangeWidth",
            "monthly_context_axis": {
                "bucket_by": "monthlyRangeWidth",
                "buckets": [
                    {"label": "tight", "max_inclusive": 0.28},
                    {"label": "normal", "max_inclusive": 0.54},
                    {"label": "wide", "max_inclusive": 0.74},
                    {"label": "extreme_wide", "min_exclusive": 0.74},
                ],
                "note": "Same buckets as v1; v2 relaxes wide/extreme_wide cutoffs to preserve pass-days while still filtering higher revRisk months.",
            },
            "thresholds_by_bucket": {
                "tight": {"entryMinEv": 0.05, "entryMaxRevRisk": 0.38},
                "normal": {"entryMinEv": 0.05, "entryMaxRevRisk": 0.40},
                "wide": {"entryMinEv": 0.0525, "entryMaxRevRisk": 0.37},
                "extreme_wide": {"entryMinEv": 0.055, "entryMaxRevRisk": 0.33},
                "UNKNOWN": {"entryMinEv": 0.05, "entryMaxRevRisk": 0.40},
            },
            "note": "Variant changes only entryMinEv and entryMaxRevRisk; minLiquidity20d and entryMinUpProb stay fixed. Monthly context uses monthlyRangeWidth buckets.",
        }
    elif variant == "monthly_range_width_ev_rr_v3_exclude_normal":
        feature_id = f"blind_month_{start_ymd//100:06d}_monthly_range_width_ev_rr_v3_exclude_normal_proxy"
        variant_gate = {
            "id": "monthly_range_width_ev_rr_v3_exclude_normal",
            "monthly_range_width_source": "signal_basis_daily.basis_payload_json.monthlyRangeWidth",
            "monthly_context_axis": {
                "bucket_by": "monthlyRangeWidth",
                "buckets": [
                    {"label": "tight", "max_inclusive": 0.28},
                    {"label": "normal", "max_inclusive": 0.54},
                    {"label": "wide", "max_inclusive": 0.74},
                    {"label": "extreme_wide", "min_exclusive": 0.74},
                ],
                "note": "Same buckets as v1/v2; v3 tests whether removing the normal bucket improves monthly compounding stability.",
            },
            "monthly_context_filter": {
                "allowed_buckets": ["tight", "wide", "extreme_wide"],
                "excluded_buckets": ["normal", "UNKNOWN"],
                "note": "Hard filter: exclude normal and null monthlyRangeWidth (UNKNOWN).",
            },
            "thresholds_by_bucket": {
                "tight": {"entryMinEv": 0.055, "entryMaxRevRisk": 0.36},
                "normal": {"entryMinEv": 0.05, "entryMaxRevRisk": 0.40},
                "wide": {"entryMinEv": 0.0525, "entryMaxRevRisk": 0.37},
                "extreme_wide": {"entryMinEv": 0.045, "entryMaxRevRisk": 0.42},
                "UNKNOWN": {"entryMinEv": 0.05, "entryMaxRevRisk": 0.40},
            },
            "note": "Variant changes only entryMinEv and entryMaxRevRisk plus a monthlyRangeWidth bucket filter; minLiquidity20d and entryMinUpProb stay fixed.",
        }
    elif variant == "monthly_range_width_ev_rr_v4_tighten_normal_drop_unknown":
        feature_id = f"blind_month_{start_ymd//100:06d}_monthly_range_width_ev_rr_v4_tighten_normal_drop_unknown_proxy"
        variant_gate = {
            "id": "monthly_range_width_ev_rr_v4_tighten_normal_drop_unknown",
            "monthly_range_width_source": "signal_basis_daily.basis_payload_json.monthlyRangeWidth",
            "monthly_context_axis": {
                "bucket_by": "monthlyRangeWidth",
                "buckets": [
                    {"label": "tight", "max_inclusive": 0.28},
                    {"label": "normal", "max_inclusive": 0.54},
                    {"label": "wide", "max_inclusive": 0.74},
                    {"label": "extreme_wide", "min_exclusive": 0.74},
                ],
                "note": "Same buckets as v1/v2; v4 tightens only the normal bucket while dropping null-context (UNKNOWN).",
            },
            "monthly_context_filter": {
                "excluded_buckets": ["UNKNOWN"],
                "note": "Drops null monthlyRangeWidth (UNKNOWN) to avoid mixing missing-context days into the gate.",
            },
            "thresholds_by_bucket": {
                "tight": {"entryMinEv": 0.05, "entryMaxRevRisk": 0.40},
                "normal": {"entryMinEv": 0.055, "entryMaxRevRisk": 0.36},
                "wide": {"entryMinEv": 0.0525, "entryMaxRevRisk": 0.37},
                "extreme_wide": {"entryMinEv": 0.05, "entryMaxRevRisk": 0.40},
                "UNKNOWN": {"entryMinEv": 0.05, "entryMaxRevRisk": 0.40},
            },
            "note": "Variant changes only entryMinEv and entryMaxRevRisk plus an UNKNOWN drop; minLiquidity20d and entryMinUpProb stay fixed. Monthly context uses monthlyRangeWidth buckets.",
        }
    elif variant == "monthly_range_width_ev_rr_v6_strict_normal_drop_unknown":
        feature_id = f"blind_month_{start_ymd//100:06d}_monthly_range_width_ev_rr_v6_strict_normal_drop_unknown_proxy"
        variant_gate = {
            "id": "monthly_range_width_ev_rr_v6_strict_normal_drop_unknown",
            "monthly_range_width_source": "signal_basis_daily.basis_payload_json.monthlyRangeWidth",
            "monthly_context_axis": {
                "bucket_by": "monthlyRangeWidth",
                "buckets": [
                    {"label": "tight", "max_inclusive": 0.28},
                    {"label": "normal", "max_inclusive": 0.54},
                    {"label": "wide", "max_inclusive": 0.74},
                    {"label": "extreme_wide", "min_exclusive": 0.74},
                ],
                "note": "Same buckets as v1/v2/v4; v6 further tightens only the normal bucket (vs v4) while dropping null-context (UNKNOWN).",
            },
            "monthly_context_filter": {
                "excluded_buckets": ["UNKNOWN"],
                "note": "Drops null monthlyRangeWidth (UNKNOWN) to avoid mixing missing-context days into the gate.",
            },
            "thresholds_by_bucket": {
                "tight": {"entryMinEv": 0.05, "entryMaxRevRisk": 0.40},
                "normal": {"entryMinEv": 0.065, "entryMaxRevRisk": 0.33},
                "wide": {"entryMinEv": 0.0525, "entryMaxRevRisk": 0.37},
                "extreme_wide": {"entryMinEv": 0.05, "entryMaxRevRisk": 0.40},
                "UNKNOWN": {"entryMinEv": 0.05, "entryMaxRevRisk": 0.40},
            },
            "note": "Variant changes only entryMinEv and entryMaxRevRisk plus an UNKNOWN drop; minLiquidity20d and entryMinUpProb stay fixed. Monthly context uses monthlyRangeWidth buckets.",
        }
    elif variant == "monthly_range_width_ev_rr_v7_soft_normal_drop_unknown":
        feature_id = f"blind_month_{start_ymd//100:06d}_monthly_range_width_ev_rr_v7_soft_normal_drop_unknown_proxy"
        variant_gate = {
            "id": "monthly_range_width_ev_rr_v7_soft_normal_drop_unknown",
            "monthly_range_width_source": "signal_basis_daily.basis_payload_json.monthlyRangeWidth",
            "monthly_context_axis": {
                "bucket_by": "monthlyRangeWidth",
                "buckets": [
                    {"label": "tight", "max_inclusive": 0.28},
                    {"label": "normal", "max_inclusive": 0.54},
                    {"label": "wide", "max_inclusive": 0.74},
                    {"label": "extreme_wide", "min_exclusive": 0.74},
                ],
                "note": "Same buckets as v1/v2/v4/v6; v7 softens the strict-normal idea to test whether milder normal tightening improves month-to-month stability.",
            },
            "monthly_context_filter": {
                "excluded_buckets": ["UNKNOWN"],
                "note": "Drops null monthlyRangeWidth (UNKNOWN) to avoid mixing missing-context days into the gate.",
            },
            "thresholds_by_bucket": {
                "tight": {"entryMinEv": 0.05, "entryMaxRevRisk": 0.40},
                "normal": {"entryMinEv": 0.06, "entryMaxRevRisk": 0.35},
                "wide": {"entryMinEv": 0.0525, "entryMaxRevRisk": 0.37},
                "extreme_wide": {"entryMinEv": 0.05, "entryMaxRevRisk": 0.40},
                "UNKNOWN": {"entryMinEv": 0.05, "entryMaxRevRisk": 0.40},
            },
            "note": "Variant changes only entryMinEv and entryMaxRevRisk plus an UNKNOWN drop; minLiquidity20d and entryMinUpProb stay fixed. Monthly context uses monthlyRangeWidth buckets.",
        }
    elif variant == "monthly_range_width_ev_rr_v8_tighten_wide_extreme_drop_unknown":
        feature_id = f"blind_month_{start_ymd//100:06d}_monthly_range_width_ev_rr_v8_tighten_wide_extreme_drop_unknown_proxy"
        variant_gate = {
            "id": "monthly_range_width_ev_rr_v8_tighten_wide_extreme_drop_unknown",
            "monthly_range_width_source": "signal_basis_daily.basis_payload_json.monthlyRangeWidth",
            "monthly_context_axis": {
                "bucket_by": "monthlyRangeWidth",
                "buckets": [
                    {"label": "tight", "max_inclusive": 0.28},
                    {"label": "normal", "max_inclusive": 0.54},
                    {"label": "wide", "max_inclusive": 0.74},
                    {"label": "extreme_wide", "min_exclusive": 0.74},
                ],
                "note": "Same buckets as v1/v2/v4/v6/v7; v8 keeps tight/normal at baseline and tightens wide/extreme_wide while dropping null-context (UNKNOWN).",
            },
            "monthly_context_filter": {
                "excluded_buckets": ["UNKNOWN"],
                "note": "Drops null monthlyRangeWidth (UNKNOWN) to avoid mixing missing-context days into the gate.",
            },
            "thresholds_by_bucket": {
                "tight": {"entryMinEv": 0.05, "entryMaxRevRisk": 0.40},
                "normal": {"entryMinEv": 0.05, "entryMaxRevRisk": 0.40},
                "wide": {"entryMinEv": 0.06, "entryMaxRevRisk": 0.34},
                "extreme_wide": {"entryMinEv": 0.065, "entryMaxRevRisk": 0.33},
                "UNKNOWN": {"entryMinEv": 0.05, "entryMaxRevRisk": 0.40},
            },
            "note": "Variant changes only entryMinEv and entryMaxRevRisk plus an UNKNOWN drop; minLiquidity20d and entryMinUpProb stay fixed. Monthly context uses monthlyRangeWidth buckets.",
        }
    elif variant == "monthly_range_width_ev_rr_v9_tighten_tight_normal_drop_unknown":
        feature_id = f"blind_month_{start_ymd//100:06d}_monthly_range_width_ev_rr_v9_tighten_tight_normal_drop_unknown_proxy"
        variant_gate = {
            "id": "monthly_range_width_ev_rr_v9_tighten_tight_normal_drop_unknown",
            "monthly_range_width_source": "signal_basis_daily.basis_payload_json.monthlyRangeWidth",
            "monthly_context_axis": {
                "bucket_by": "monthlyRangeWidth",
                "buckets": [
                    {"label": "tight", "max_inclusive": 0.28},
                    {"label": "normal", "max_inclusive": 0.54},
                    {"label": "wide", "max_inclusive": 0.74},
                    {"label": "extreme_wide", "min_exclusive": 0.74},
                ],
                "note": "Same buckets as v1/v2/v4/v6/v7/v8; v9 extends tightening into the more-common tight bucket while also using strict-normal (vs v7) and keeping v8-style wide/extreme tightening, dropping null-context (UNKNOWN).",
            },
            "monthly_context_filter": {
                "excluded_buckets": ["UNKNOWN"],
                "note": "Drops null monthlyRangeWidth (UNKNOWN) to avoid mixing missing-context days into the gate.",
            },
            "thresholds_by_bucket": {
                "tight": {"entryMinEv": 0.06, "entryMaxRevRisk": 0.35},
                "normal": {"entryMinEv": 0.065, "entryMaxRevRisk": 0.33},
                "wide": {"entryMinEv": 0.06, "entryMaxRevRisk": 0.34},
                "extreme_wide": {"entryMinEv": 0.065, "entryMaxRevRisk": 0.33},
                "UNKNOWN": {"entryMinEv": 0.05, "entryMaxRevRisk": 0.40},
            },
            "note": "Variant changes only entryMinEv and entryMaxRevRisk plus an UNKNOWN drop; minLiquidity20d and entryMinUpProb stay fixed. Monthly context uses monthlyRangeWidth buckets.",
        }
    elif variant == "monthly_range_width_ev_rr_v5_exclude_extreme_wide":
        feature_id = f"blind_month_{start_ymd//100:06d}_monthly_range_width_ev_rr_v5_exclude_extreme_wide_proxy"
        variant_gate = {
            "id": "monthly_range_width_ev_rr_v5_exclude_extreme_wide",
            "monthly_range_width_source": "signal_basis_daily.basis_payload_json.monthlyRangeWidth",
            "monthly_context_axis": {
                "bucket_by": "monthlyRangeWidth",
                "buckets": [
                    {"label": "tight", "max_inclusive": 0.28},
                    {"label": "normal", "max_inclusive": 0.54},
                    {"label": "wide", "max_inclusive": 0.74},
                    {"label": "extreme_wide", "min_exclusive": 0.74},
                ],
                "note": "Same buckets as v1/v2; v5 tests whether filtering out extreme_wide improves monthly compounding stability.",
            },
            "monthly_context_filter": {
                "allowed_buckets": ["tight", "normal", "wide"],
                "excluded_buckets": ["extreme_wide", "UNKNOWN"],
                "note": "Filters out extreme_wide and null monthlyRangeWidth (UNKNOWN).",
            },
            "thresholds_by_bucket": {
                "tight": {"entryMinEv": 0.05, "entryMaxRevRisk": 0.40},
                "normal": {"entryMinEv": 0.05, "entryMaxRevRisk": 0.40},
                "wide": {"entryMinEv": 0.0525, "entryMaxRevRisk": 0.37},
                "extreme_wide": {"entryMinEv": 0.05, "entryMaxRevRisk": 0.40},
                "UNKNOWN": {"entryMinEv": 0.05, "entryMaxRevRisk": 0.40},
            },
            "note": "Variant changes only entryMinEv and entryMaxRevRisk plus a monthlyRangeWidth bucket filter; minLiquidity20d and entryMinUpProb stay fixed.",
        }
    else:
        raise ValueError(f"Unknown variant: {variant}")

    record = {
        "feature_id": feature_id,
        "scope": "toredex_proxy",
        "definition": {
            "generated_at": _now_iso_jst(),
            "source_db": str(db_path),
            "window": f"{start_ymd}..{end_ymd}",
            "candidate_stream": {"table": "ranking_appearance_daily", "filters": {"dir": side_dir, "rank": int(rank)}},
            "monthly_return_proxy": {
                "entry_price": "ranking_appearance_daily.anchor_price_next_open",
                "exit_price": "daily_bars.c at last trading day of month (daily_bars.date epoch -> to_timestamp)",
                "return_pct": "(exit / entry - 1) * 100",
            },
            "baseline_gate": {
                "minLiquidity20d": 500000,
                "entryMinUpProb": 0.75,
                "entryMinEv": 0.05,
                "entryMaxRevRisk": 0.4,
                "upProb_definition": "max(probSideCalib, probSide, weeklyBreakoutUpProb, monthlyBreakoutUpProb)",
                "ev_definition": "changePct",
                "revRisk_definition": "max(weeklyBreakoutDownProb, monthlyBreakoutDownProb)",
            },
            "variant_gate": variant_gate,
        },
        "stats": {"months": months},
        "current_read": "",
        "retest_conditions": [],
    }
    return record


def _render_table(months: dict[str, Any]) -> str:
    header = "| month | rank1 days | base pass | var pass | base median | var median | base hit>=2% | var hit>=2% |"
    sep = "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    lines = [header, sep]
    for month, stats in months.items():
        base = stats["baseline"]["month_end_ret_pct"]
        var = stats["variant"]["month_end_ret_pct"]
        def fmt(x: Any) -> str:
            if x is None:
                return "n/a"
            return f"{float(x):+.2f}%"
        def fmt_rate(x: Any) -> str:
            if x is None:
                return "n/a"
            return f"{100.0*float(x):.2f}%"
        lines.append(
            "| {month} | {days} | {bp} | {vp} | {bm} | {vm} | {bh} | {vh} |".format(
                month=month,
                days=int(stats["rank1_days"]),
                bp=int(stats["baseline"]["pass_days"]),
                vp=int(stats["variant"]["pass_days"]),
                bm=fmt(base["median"]),
                vm=fmt(var["median"]),
                bh=fmt_rate(base["hit_rate_ge_2pct"]),
                vh=fmt_rate(var["hit_rate_ge_2pct"]),
            )
        )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Monthly blind test proxy for entryMinEv/revRisk tweaks")
    parser.add_argument("--db-path", type=Path, default=Path(config.DB_PATH))
    parser.add_argument("--start-ymd", type=int, required=True, help="YYYYMMDD (inclusive)")
    parser.add_argument("--end-ymd", type=int, required=True, help="YYYYMMDD (inclusive; should include month-ends)")
    parser.add_argument("--dir", dest="side_dir", default="up", choices=["up", "down"])
    parser.add_argument("--rank", type=int, default=1)
    parser.add_argument(
        "--variant",
        default="market_regime_ev_rr_v2",
        choices=[
            "market_regime_ev_rr_v2",
            "monthly_box_ev_rr_v1",
            "monthly_box_ev_rr_v2_boxonly",
            "monthly_box_ev_rr_v3_upper_m4",
            "monthly_range_width_ev_rr_v1",
            "monthly_range_width_ev_rr_v2_soften_wide",
            "monthly_range_width_ev_rr_v3_exclude_normal",
            "monthly_range_width_ev_rr_v4_tighten_normal_drop_unknown",
            "monthly_range_width_ev_rr_v6_strict_normal_drop_unknown",
            "monthly_range_width_ev_rr_v7_soft_normal_drop_unknown",
            "monthly_range_width_ev_rr_v8_tighten_wide_extreme_drop_unknown",
            "monthly_range_width_ev_rr_v9_tighten_tight_normal_drop_unknown",
            "monthly_range_width_ev_rr_v5_exclude_extreme_wide",
        ],
    )
    parser.add_argument("--print-json", action="store_true", help="Print record JSON to stdout")
    args = parser.parse_args()

    record = run_proxy(
        db_path=args.db_path,
        start_ymd=int(args.start_ymd),
        end_ymd=int(args.end_ymd),
        side_dir=str(args.side_dir),
        rank=int(args.rank),
        variant=str(args.variant),
    )

    months = record["stats"]["months"]
    print(_render_table(months))
    if args.print_json:
        print("\n---json---")
        print(json.dumps(record, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
