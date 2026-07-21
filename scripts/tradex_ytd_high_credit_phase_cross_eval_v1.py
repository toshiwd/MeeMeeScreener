from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


AXIS_ID = "tradex_ytd_high_credit_phase_cross_eval_v1"
DEFAULT_DB = Path("stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\ytd_high_credit_phase_v1")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def run(*, db_path: Path, output_root: Path) -> Path:
    run_dir = output_root / f"{_tag()}-{AXIS_ID}"
    run_dir.mkdir(parents=True, exist_ok=False)

    feature_sql = r"""
    CREATE TEMP TABLE phase_eval AS
    WITH base AS (
      SELECT
        code,
        date,
        to_timestamp(date) AT TIME ZONE 'Asia/Tokyo' AS date_ts,
        o, h, l, c, v,
        row_number() OVER wc AS rn,
        avg(c) OVER w7 AS ma7,
        avg(c) OVER w20 AS ma20,
        avg(c) OVER w60 AS ma60,
        avg(c) OVER w100 AS ma100,
        min(l) OVER w60 AS low60,
        min(l) OVER w120 AS low120,
        max(h) OVER wytd_prior AS ytd_high_prior
      FROM daily_bars
      WHERE o IS NOT NULL AND h IS NOT NULL AND l IS NOT NULL AND c IS NOT NULL AND c > 0
      WINDOW
        wc AS (PARTITION BY code ORDER BY date),
        w7 AS (PARTITION BY code ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
        w20 AS (PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
        w60 AS (PARTITION BY code ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW),
        w100 AS (PARTITION BY code ORDER BY date ROWS BETWEEN 99 PRECEDING AND CURRENT ROW),
        w120 AS (PARTITION BY code ORDER BY date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW),
        wytd_prior AS (PARTITION BY code, date_part('year', to_timestamp(date) AT TIME ZONE 'Asia/Tokyo') ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
    ),
    events AS (
      SELECT
        *,
        CASE WHEN ytd_high_prior IS NULL OR h > ytd_high_prior THEN 1 ELSE 0 END AS is_new_ytd_high
      FROM base
    ),
    last_high AS (
      SELECT
        *,
        max(CASE WHEN is_new_ytd_high = 1 THEN rn END) OVER wy AS last_ytd_high_rn,
        max(CASE WHEN is_new_ytd_high = 1 THEN h END) OVER wy AS last_ytd_high_price
      FROM events
      WINDOW wy AS (PARTITION BY code, date_part('year', date_ts) ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ),
    feat AS (
      SELECT
        b.*,
        b.rn - b.last_ytd_high_rn AS days_since_ytd_high,
        CASE
          WHEN b.rn - b.last_ytd_high_rn BETWEEN 0 AND 63 THEN 'early_momentum_0_3m'
          WHEN b.rn - b.last_ytd_high_rn BETWEEN 64 AND 105 THEN 'warning_3_5m'
          WHEN b.rn - b.last_ytd_high_rn BETWEEN 106 AND 126 THEN 'credit_pressure_peak_5_6m'
          WHEN b.rn - b.last_ytd_high_rn BETWEEN 127 AND 168 THEN 'post_pressure_rebound_6_8m'
          ELSE 'other'
        END AS ytd_high_credit_phase,
        CASE WHEN c > 0 THEN c / b.last_ytd_high_price - 1 END AS drawdown_from_ytd_high,
        CASE WHEN c > 0 THEN (c - ma20) / c END AS dist_ma20,
        CASE WHEN c > 0 THEN (c - ma60) / c END AS dist_ma60,
        CASE WHEN c > 0 THEN (c - ma100) / c END AS dist_ma100,
        CASE WHEN low60 > 0 THEN c / low60 - 1 END AS room_to_low60,
        CASE WHEN low120 > 0 THEN c / low120 - 1 END AS pos_from_low120,
        CASE WHEN h > l THEN (h - greatest(o, c)) / (h - l) END AS upper_wick_ratio,
        CASE WHEN h > l THEN (c - l) / (h - l) END AS close_pos
      FROM last_high b
      WHERE b.last_ytd_high_rn IS NOT NULL
    ),
    setups AS (
      SELECT
        *,
        CASE
          WHEN feat.pos_from_low120 >= 0.6 AND feat.room_to_low60 >= 0.1 THEN 'bottom_lift'
          WHEN feat.drawdown_from_ytd_high BETWEEN -0.08 AND -0.02 AND feat.c < feat.ma20 THEN 'mild_pullback_below_ma20'
          WHEN feat.drawdown_from_ytd_high <= -0.08 AND feat.c < feat.ma60 THEN 'deep_pullback_below_ma60'
          WHEN feat.drawdown_from_ytd_high >= -0.03 AND feat.c >= feat.ma20 THEN 'near_high_above_ma20'
          WHEN feat.upper_wick_ratio >= 0.55 AND feat.close_pos <= 0.35 AND feat.c < feat.ma20 THEN 'upper_rejection_below_ma20'
          ELSE 'other'
        END AS setup_family,
        y20.c AS c20,
        y60.c AS c60,
        (SELECT min(l) FROM feat x WHERE x.code = feat.code AND x.rn BETWEEN feat.rn + 1 AND feat.rn + 20) AS min_l20,
        (SELECT max(h) FROM feat x WHERE x.code = feat.code AND x.rn BETWEEN feat.rn + 1 AND feat.rn + 20) AS max_h20,
        (SELECT min(l) FROM feat x WHERE x.code = feat.code AND x.rn BETWEEN feat.rn + 1 AND feat.rn + 60) AS min_l60,
        (SELECT max(h) FROM feat x WHERE x.code = feat.code AND x.rn BETWEEN feat.rn + 1 AND feat.rn + 60) AS max_h60
      FROM feat
      LEFT JOIN feat y20 ON y20.code = feat.code AND y20.rn = feat.rn + 20
      LEFT JOIN feat y60 ON y60.code = feat.code AND y60.rn = feat.rn + 60
      WHERE feat.ytd_high_credit_phase <> 'other'
    )
    SELECT
      *,
      CASE WHEN c > 0 THEN c20 / c - 1 END AS ret20,
      CASE WHEN c > 0 THEN c60 / c - 1 END AS ret60,
      CASE WHEN c > 0 THEN min_l20 / c - 1 END AS min_ret20,
      CASE WHEN c > 0 THEN max_h20 / c - 1 END AS max_ret20,
      CASE WHEN c > 0 THEN min_l60 / c - 1 END AS min_ret60,
      CASE WHEN c > 0 THEN max_h60 / c - 1 END AS max_ret60
    FROM setups
    WHERE c20 IS NOT NULL AND c60 IS NOT NULL
    """

    aggregate = """
    SELECT
      ytd_high_credit_phase,
      setup_family,
      count(*) AS n,
      count(DISTINCT code) AS unique_codes,
      avg(ret20) AS avg_ret20,
      avg(ret60) AS avg_ret60,
      avg(CASE WHEN ret20 > 0 THEN 1 ELSE 0 END) AS up20_rate,
      avg(CASE WHEN ret20 < 0 THEN 1 ELSE 0 END) AS down20_rate,
      avg(CASE WHEN min_ret20 <= -0.10 THEN 1 ELSE 0 END) AS touch_down_10pct_20d_rate,
      avg(CASE WHEN max_ret20 >= 0.10 THEN 1 ELSE 0 END) AS touch_up_10pct_20d_rate,
      avg(CASE WHEN min_ret60 <= -0.10 THEN 1 ELSE 0 END) AS touch_down_10pct_60d_rate,
      avg(CASE WHEN max_ret60 >= 0.10 THEN 1 ELSE 0 END) AS touch_up_10pct_60d_rate,
      quantile_cont(ret20, 0.25) AS ret20_p25,
      quantile_cont(ret20, 0.75) AS ret20_p75
    FROM phase_eval
    GROUP BY ytd_high_credit_phase, setup_family
    HAVING count(*) >= 500
    ORDER BY ytd_high_credit_phase, setup_family
    """

    with duckdb.connect(str(db_path), read_only=True) as conn:
        conn.execute(feature_sql)
        rows = conn.execute(aggregate).fetchdf().to_dict("records")
        phase_rows = conn.execute(
            """
            SELECT
              ytd_high_credit_phase,
              count(*) AS n,
              count(DISTINCT code) AS unique_codes,
              avg(ret20) AS avg_ret20,
              avg(ret60) AS avg_ret60,
              avg(CASE WHEN min_ret20 <= -0.10 THEN 1 ELSE 0 END) AS touch_down_10pct_20d_rate,
              avg(CASE WHEN max_ret20 >= 0.10 THEN 1 ELSE 0 END) AS touch_up_10pct_20d_rate
            FROM phase_eval
            GROUP BY ytd_high_credit_phase
            ORDER BY ytd_high_credit_phase
            """
        ).fetchdf().to_dict("records")

    report = {
        "schema_version": f"{AXIS_ID}_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "research_phase": "branching_generation",
        "fixed_evaluation_conditions": {
            "universe": "all daily_bars rows with 20d and 60d forward outcomes",
            "phase_feature": "ytd_high_credit_phase",
            "phase_definitions": {
                "early_momentum_0_3m": "0-63 trading days since latest same-year YTD high",
                "warning_3_5m": "64-105 trading days",
                "credit_pressure_peak_5_6m": "106-126 trading days",
                "post_pressure_rebound_6_8m": "127-168 trading days",
            },
            "cost_slippage": "none",
        },
        "phase_results": phase_rows,
        "phase_setup_results": rows,
        "decision": {
            "candidate_local_decision": "hold_for_rule_selection",
            "authoritative_rollup_decision": "research_only_not_trade_signal",
            "reason": "credit phase is context; keep only if it improves existing setup separation",
        },
        "runtime_db_write": False,
        "meemee_unchanged": True,
        "production_ranking_changed": False,
    }
    _write_json(run_dir / "ytd_high_credit_phase_cross_eval.json", report)
    _write_json(output_root / "latest_ytd_high_credit_phase_cross_eval.json", {"run_root": str(run_dir), **report})
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
