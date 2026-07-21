from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


AXIS_ID = "tradex_bottom_lift_confirmation_timing_eval_v1"
DEFAULT_DB = Path("stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_shape_classification_v1")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


FEATURE_CTE = r"""
WITH base AS (
  SELECT
    code,
    date,
    o, h, l, c, v,
    row_number() OVER wc AS rn,
    avg(c) OVER w7 AS ma7,
    avg(c) OVER w20 AS ma20,
    avg(c) OVER w60 AS ma60,
    avg(c) OVER w100 AS ma100,
    min(l) OVER w60 AS low60,
    max(h) OVER w120 AS high120,
    min(l) OVER w120 AS low120,
    avg(v) OVER w20 AS vol20
  FROM daily_bars
  WINDOW
    wc AS (PARTITION BY code ORDER BY date),
    w7 AS (PARTITION BY code ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
    w20 AS (PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
    w60 AS (PARTITION BY code ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW),
    w100 AS (PARTITION BY code ORDER BY date ROWS BETWEEN 99 PRECEDING AND CURRENT ROW),
    w120 AS (PARTITION BY code ORDER BY date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW)
),
lagged AS (
  SELECT
    *,
    lag(ma20, 5) OVER wc AS ma20_lag5,
    lag(ma60, 10) OVER wc AS ma60_lag10
  FROM base
  WINDOW wc AS (PARTITION BY code ORDER BY date)
),
feat AS (
  SELECT
    *,
    CASE WHEN h > l THEN (h - greatest(o, c)) / (h - l) END AS upper_wick_ratio,
    CASE WHEN h > l THEN (c - l) / (h - l) END AS close_pos,
    CASE WHEN h > l THEN abs(c - o) / (h - l) END AS body_ratio,
    CASE WHEN c > 0 THEN (c - ma7) / c END AS dist_ma7,
    CASE WHEN c > 0 THEN (c - ma20) / c END AS dist_ma20,
    CASE WHEN c > 0 THEN (c - ma60) / c END AS dist_ma60,
    CASE WHEN c > 0 THEN (c - ma100) / c END AS dist_ma100,
    CASE WHEN c > 0 THEN (c - low60) / c END AS room_to_low60,
    CASE WHEN low120 > 0 THEN (c / low120) - 1 END AS pos_from_low120,
    CASE WHEN high120 > 0 THEN (c / high120) - 1 END AS drawdown_from_high120,
    CASE WHEN ma20_lag5 > 0 THEN (ma20 / ma20_lag5) - 1 END AS ma20_slope5,
    CASE WHEN ma60_lag10 > 0 THEN (ma60 / ma60_lag10) - 1 END AS ma60_slope10,
    CASE WHEN vol20 > 0 THEN v / vol20 END AS volume_ratio20
  FROM lagged
),
signals AS (
  SELECT *
  FROM feat
  WHERE
    pos_from_low120 >= 0.6
    AND room_to_low60 >= 0.1
),
future AS (
  SELECT
    s.code,
    s.date AS signal_date,
    s.rn AS signal_rn,
    s.c AS signal_close,
    s.h AS signal_high,
    s.l AS signal_low,
    s.ma20 AS signal_ma20,
    s.ma60 AS signal_ma60,
    s.dist_ma20 AS signal_dist_ma20,
    s.dist_ma60 AS signal_dist_ma60,
    f.date AS confirm_date,
    f.rn - s.rn AS days_after,
    f.o, f.h, f.l, f.c,
    f.ma7, f.ma20, f.ma60,
    f.dist_ma20,
    f.dist_ma60,
    f.upper_wick_ratio,
    f.close_pos,
    f.volume_ratio20
  FROM signals s
  JOIN feat f ON f.code = s.code AND f.rn BETWEEN s.rn + 1 AND s.rn + 5
),
classified AS (
  SELECT
    *,
    CASE
      WHEN c > signal_high AND c > ma20 AND close_pos >= 0.55 THEN 'long_breakout_confirm'
      WHEN l <= ma20 AND c > ma20 AND c > o THEN 'long_support_rebound_ma20'
      WHEN l <= ma60 AND c > ma60 AND c > o THEN 'long_support_rebound_ma60'
      WHEN c < signal_low OR (c < ma20 AND c < ma60) THEN 'short_support_break'
      WHEN upper_wick_ratio >= 0.55 AND close_pos <= 0.35 AND c < signal_high THEN 'short_failed_retest'
      ELSE 'no_confirm'
    END AS confirm_type
  FROM future
),
first_confirm AS (
  SELECT *
  FROM classified
  WHERE confirm_type <> 'no_confirm'
  QUALIFY row_number() OVER (
    PARTITION BY code, signal_date
    ORDER BY
      CASE
        WHEN confirm_type IN ('short_support_break', 'short_failed_retest') THEN 0
        WHEN confirm_type IN ('long_breakout_confirm', 'long_support_rebound_ma20', 'long_support_rebound_ma60') THEN 1
        ELSE 2
      END,
      days_after
  ) = 1
),
entries AS (
  SELECT
    code,
    signal_date,
    confirm_date AS entry_date,
    days_after,
    confirm_type,
    c AS entry_close,
    CASE WHEN confirm_type LIKE 'long_%' THEN 'long'
         WHEN confirm_type LIKE 'short_%' THEN 'short'
         ELSE 'none' END AS side
  FROM first_confirm
),
entry_base AS (
  SELECT e.*, b.rn AS entry_rn
  FROM entries e
  JOIN feat b ON b.code = e.code AND b.date = e.entry_date
),
outcome AS (
  SELECT
    e.*,
    y20.c AS c20,
    (SELECT max(h) FROM feat x WHERE x.code = e.code AND x.rn BETWEEN e.entry_rn + 1 AND e.entry_rn + 20) AS max_h20,
    (SELECT min(l) FROM feat x WHERE x.code = e.code AND x.rn BETWEEN e.entry_rn + 1 AND e.entry_rn + 20) AS min_l20,
    (SELECT max(h) FROM feat x WHERE x.code = e.code AND x.rn BETWEEN e.entry_rn + 1 AND e.entry_rn + 10) AS max_h10
  FROM entry_base e
  LEFT JOIN feat y20 ON y20.code = e.code AND y20.rn = e.entry_rn + 20
)
SELECT
  *,
  CASE WHEN entry_close > 0 THEN c20 / entry_close - 1 END AS ret20,
  CASE WHEN entry_close > 0 THEN max_h20 / entry_close - 1 END AS mfe20,
  CASE WHEN entry_close > 0 THEN min_l20 / entry_close - 1 END AS mae20,
  CASE WHEN entry_close > 0 THEN max_h10 / entry_close - 1 END AS adverse10_for_short
FROM outcome
WHERE c20 IS NOT NULL
"""


def run(*, db_path: Path, output_root: Path) -> Path:
    run_dir = output_root / f"{_tag()}-{AXIS_ID}"
    run_dir.mkdir(parents=True, exist_ok=False)
    with duckdb.connect(str(db_path), read_only=True) as conn:
        # Materialize the outcome rows from the FEATURE_CTE final SELECT.
        conn.execute("CREATE TEMP TABLE confirmation_outcomes AS " + FEATURE_CTE)
        results = conn.execute(
            """
            SELECT
              confirm_type,
              side,
              count(*) AS n,
              count(DISTINCT code) AS unique_codes,
              avg(ret20) AS avg_ret20,
              avg(CASE WHEN ret20 > 0 THEN 1 ELSE 0 END) AS up20_rate,
              avg(CASE WHEN ret20 < 0 THEN 1 ELSE 0 END) AS down20_rate,
              avg(CASE WHEN ret20 >= 0.10 THEN 1 ELSE 0 END) AS close_up_10pct_20d_rate,
              avg(CASE WHEN mfe20 >= 0.10 THEN 1 ELSE 0 END) AS touch_up_10pct_20d_rate,
              avg(CASE WHEN mae20 <= -0.05 THEN 1 ELSE 0 END) AS adverse_down_5pct_20d_rate,
              avg(CASE WHEN ret20 <= -0.10 THEN 1 ELSE 0 END) AS close_down_10pct_20d_rate,
              avg(CASE WHEN mae20 <= -0.10 THEN 1 ELSE 0 END) AS touch_down_10pct_20d_rate,
              avg(CASE WHEN adverse10_for_short >= 0.05 THEN 1 ELSE 0 END) AS adverse_up_5pct_10d_rate,
              quantile_cont(ret20, 0.25) AS ret20_p25,
              quantile_cont(ret20, 0.75) AS ret20_p75
            FROM confirmation_outcomes
            GROUP BY confirm_type, side
            ORDER BY n DESC
            """
        ).fetchdf().to_dict("records")
        summary = conn.execute(
            """
            SELECT
              count(*) AS n,
              count(DISTINCT code || ':' || signal_date::VARCHAR) AS signal_count,
              count(DISTINCT code) AS unique_codes
            FROM confirmation_outcomes
            """
        ).fetchone()
    report = {
        "schema_version": f"{AXIS_ID}_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {
            "source_signal": "bottom_lift pos_from_low120 >= 0.6 and room_to_low60 >= 0.1",
            "confirmation_window_days": "1 to 5 trading days after signal",
            "entry": "first confirmation close",
            "cost_slippage": "none",
        },
        "summary": {"n": summary[0], "signal_count": summary[1], "unique_codes": summary[2]},
        "results": results,
        "decision": {
            "candidate_local_decision": "hold_for_confirmation_rule_cleanup",
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": "confirmation timing rows produced; keep/drop depends on directional separation and adverse move",
        },
        "runtime_db_write": False,
        "meemee_unchanged": True,
        "production_ranking_changed": False,
    }
    _write_json(run_dir / "bottom_lift_confirmation_timing_eval.json", report)
    _write_json(output_root / "latest_bottom_lift_confirmation_timing_eval.json", {"run_root": str(run_dir), **report})
    return run_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(run(db_path=args.db, output_root=args.output_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
