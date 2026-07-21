from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


AXIS_ID = "tradex_ytd_high_months_after_decline_eval_v1"
DEFAULT_DB = Path("stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\ytd_high_months_after_decline_v1")


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
    CREATE TEMP TABLE ytd_high_eval AS
    WITH base AS (
      SELECT
        code,
        date,
        to_timestamp(date) AT TIME ZONE 'Asia/Tokyo' AS date_ts,
        o, h, l, c, v,
        row_number() OVER wc AS rn,
        avg(c) OVER w20 AS ma20,
        avg(c) OVER w60 AS ma60,
        avg(c) OVER w100 AS ma100,
        max(h) OVER wytd AS ytd_high_to_date,
        max(h) OVER wytd_prior AS ytd_high_prior,
        min(l) OVER w60 AS low60,
        max(h) OVER w120 AS high120,
        min(l) OVER w120 AS low120
      FROM daily_bars
      WHERE o IS NOT NULL AND h IS NOT NULL AND l IS NOT NULL AND c IS NOT NULL AND c > 0
      WINDOW
        wc AS (PARTITION BY code ORDER BY date),
        w20 AS (PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
        w60 AS (PARTITION BY code ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW),
        w100 AS (PARTITION BY code ORDER BY date ROWS BETWEEN 99 PRECEDING AND CURRENT ROW),
        w120 AS (PARTITION BY code ORDER BY date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW),
        wytd AS (PARTITION BY code, date_part('year', to_timestamp(date) AT TIME ZONE 'Asia/Tokyo') ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
        wytd_prior AS (PARTITION BY code, date_part('year', to_timestamp(date) AT TIME ZONE 'Asia/Tokyo') ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
    ),
    high_events AS (
      SELECT
        *,
        CASE WHEN ytd_high_prior IS NULL OR h > ytd_high_prior THEN 1 ELSE 0 END AS is_new_ytd_high
      FROM base
    ),
    with_last_high AS (
      SELECT
        *,
        max(CASE WHEN is_new_ytd_high = 1 THEN rn END) OVER wc AS last_ytd_high_rn,
        max(CASE WHEN is_new_ytd_high = 1 THEN date END) OVER wc AS last_ytd_high_date,
        max(CASE WHEN is_new_ytd_high = 1 THEN h END) OVER wc AS last_ytd_high_price
      FROM high_events
      WINDOW wc AS (PARTITION BY code, date_part('year', date_ts) ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ),
    outcomes AS (
      SELECT
        b.*,
        b.rn - b.last_ytd_high_rn AS days_since_ytd_high,
        CASE
          WHEN b.rn - b.last_ytd_high_rn BETWEEN 0 AND 20 THEN '0_1m'
          WHEN b.rn - b.last_ytd_high_rn BETWEEN 21 AND 42 THEN '1_2m'
          WHEN b.rn - b.last_ytd_high_rn BETWEEN 43 AND 63 THEN '2_3m'
          WHEN b.rn - b.last_ytd_high_rn BETWEEN 64 AND 84 THEN '3_4m'
          WHEN b.rn - b.last_ytd_high_rn BETWEEN 85 AND 105 THEN '4_5m'
          WHEN b.rn - b.last_ytd_high_rn BETWEEN 106 AND 126 THEN '5_6m'
          WHEN b.rn - b.last_ytd_high_rn BETWEEN 127 AND 147 THEN '6_7m'
          WHEN b.rn - b.last_ytd_high_rn BETWEEN 148 AND 168 THEN '7_8m'
          ELSE 'other'
        END AS months_since_ytd_high_bucket,
        CASE WHEN b.c > 0 THEN b.c / b.last_ytd_high_price - 1 END AS drawdown_from_last_ytd_high,
        CASE WHEN b.c > 0 THEN (b.c - b.ma20) / b.c END AS dist_ma20,
        CASE WHEN b.c > 0 THEN (b.c - b.ma60) / b.c END AS dist_ma60,
        CASE WHEN b.c > 0 THEN (b.c - b.ma100) / b.c END AS dist_ma100,
        CASE WHEN b.low120 > 0 THEN b.c / b.low120 - 1 END AS pos_from_low120,
        y20.c AS c20,
        y60.c AS c60,
        (SELECT min(l) FROM base x WHERE x.code = b.code AND x.rn BETWEEN b.rn + 1 AND b.rn + 20) AS min_l20,
        (SELECT max(h) FROM base x WHERE x.code = b.code AND x.rn BETWEEN b.rn + 1 AND b.rn + 20) AS max_h20,
        (SELECT min(l) FROM base x WHERE x.code = b.code AND x.rn BETWEEN b.rn + 1 AND b.rn + 60) AS min_l60,
        (SELECT max(h) FROM base x WHERE x.code = b.code AND x.rn BETWEEN b.rn + 1 AND b.rn + 60) AS max_h60
      FROM with_last_high b
      LEFT JOIN base y20 ON y20.code = b.code AND y20.rn = b.rn + 20
      LEFT JOIN base y60 ON y60.code = b.code AND y60.rn = b.rn + 60
      WHERE b.last_ytd_high_rn IS NOT NULL
    )
    SELECT
      *,
      CASE WHEN c > 0 THEN c20 / c - 1 END AS ret20,
      CASE WHEN c > 0 THEN c60 / c - 1 END AS ret60,
      CASE WHEN c > 0 THEN min_l20 / c - 1 END AS min_ret20,
      CASE WHEN c > 0 THEN max_h20 / c - 1 END AS max_ret20,
      CASE WHEN c > 0 THEN min_l60 / c - 1 END AS min_ret60,
      CASE WHEN c > 0 THEN max_h60 / c - 1 END AS max_ret60
    FROM outcomes
    WHERE c20 IS NOT NULL AND c60 IS NOT NULL AND months_since_ytd_high_bucket <> 'other'
    """

    aggregate_sql = """
    SELECT
      months_since_ytd_high_bucket AS bucket,
      count(*) AS n,
      count(DISTINCT code) AS unique_codes,
      avg(ret20) AS avg_ret20,
      avg(ret60) AS avg_ret60,
      avg(CASE WHEN ret20 < 0 THEN 1 ELSE 0 END) AS down20_rate,
      avg(CASE WHEN ret60 < 0 THEN 1 ELSE 0 END) AS down60_rate,
      avg(CASE WHEN min_ret20 <= -0.05 THEN 1 ELSE 0 END) AS touch_down_5pct_20d_rate,
      avg(CASE WHEN min_ret20 <= -0.10 THEN 1 ELSE 0 END) AS touch_down_10pct_20d_rate,
      avg(CASE WHEN min_ret60 <= -0.10 THEN 1 ELSE 0 END) AS touch_down_10pct_60d_rate,
      avg(CASE WHEN max_ret20 >= 0.05 THEN 1 ELSE 0 END) AS adverse_up_5pct_20d_rate,
      avg(CASE WHEN max_ret60 >= 0.10 THEN 1 ELSE 0 END) AS adverse_up_10pct_60d_rate,
      avg(drawdown_from_last_ytd_high) AS avg_drawdown_from_high,
      avg(dist_ma20) AS avg_dist_ma20,
      avg(dist_ma60) AS avg_dist_ma60,
      quantile_cont(ret20, 0.25) AS ret20_p25,
      quantile_cont(ret20, 0.75) AS ret20_p75,
      quantile_cont(ret60, 0.25) AS ret60_p25,
      quantile_cont(ret60, 0.75) AS ret60_p75
    FROM ytd_high_eval
    GROUP BY bucket
    ORDER BY bucket
    """

    conditional_sql = """
    SELECT
      months_since_ytd_high_bucket AS bucket,
      CASE
        WHEN drawdown_from_last_ytd_high BETWEEN -0.08 AND -0.02 AND c < ma20 THEN 'mild_pullback_below_ma20'
        WHEN drawdown_from_last_ytd_high <= -0.08 AND c < ma60 THEN 'deep_pullback_below_ma60'
        WHEN drawdown_from_last_ytd_high >= -0.03 AND c >= ma20 THEN 'near_high_above_ma20'
        WHEN c < ma20 AND ma20 < ma60 THEN 'ma20_below_ma60'
        ELSE 'other'
      END AS condition,
      count(*) AS n,
      count(DISTINCT code) AS unique_codes,
      avg(ret20) AS avg_ret20,
      avg(ret60) AS avg_ret60,
      avg(CASE WHEN ret20 < 0 THEN 1 ELSE 0 END) AS down20_rate,
      avg(CASE WHEN ret60 < 0 THEN 1 ELSE 0 END) AS down60_rate,
      avg(CASE WHEN min_ret20 <= -0.10 THEN 1 ELSE 0 END) AS touch_down_10pct_20d_rate,
      avg(CASE WHEN min_ret60 <= -0.10 THEN 1 ELSE 0 END) AS touch_down_10pct_60d_rate,
      avg(CASE WHEN max_ret20 >= 0.05 THEN 1 ELSE 0 END) AS adverse_up_5pct_20d_rate
    FROM ytd_high_eval
    GROUP BY bucket, condition
    HAVING count(*) >= 300
    ORDER BY bucket, touch_down_10pct_20d_rate DESC, n DESC
    """

    with duckdb.connect(str(db_path), read_only=True) as conn:
        conn.execute(feature_sql)
        summary = conn.execute(
            """
            SELECT
              count(*) AS n,
              count(DISTINCT code) AS unique_codes,
              min(date) AS min_date,
              max(date) AS max_date
            FROM ytd_high_eval
            """
        ).fetchone()
        bucket_results = conn.execute(aggregate_sql).fetchdf().to_dict("records")
        conditional_results = conn.execute(conditional_sql).fetchdf().to_dict("records")

    six_month = [r for r in bucket_results if r["bucket"] == "5_6m"]
    six_to_seven = [r for r in bucket_results if r["bucket"] == "6_7m"]
    decision = "hold"
    reason = "six_month_bucket_requires_comparison"
    if six_month and six_to_seven:
        six = six_month[0]
        sev = six_to_seven[0]
        if six["touch_down_10pct_20d_rate"] > sev["touch_down_10pct_20d_rate"] and six["avg_ret20"] < sev["avg_ret20"]:
            decision = "keep_as_context_filter"
            reason = "5_6m_bucket_has_relatively_higher_20d_downside_than_next_bucket"
        else:
            decision = "drop_as_standalone_rule"
            reason = "5_6m_bucket_is_not_uniquely_weaker_than_adjacent_bucket"

    report = {
        "schema_version": f"{AXIS_ID}_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {
            "universe": "all daily_bars rows with 20d and 60d forward outcomes",
            "ytd_high_definition": "new calendar-year high based on intraday high h",
            "bucket_definition": "trading-day buckets approximating months after last YTD high in the same calendar year",
            "entry": "daily close inside each bucket, not the high event itself",
            "cost_slippage": "none",
        },
        "summary": {
            "n": summary[0],
            "unique_codes": summary[1],
            "min_date_unix": summary[2],
            "max_date_unix": summary[3],
        },
        "bucket_results": bucket_results,
        "conditional_results": conditional_results,
        "decision": {
            "candidate_local_decision": decision,
            "authoritative_rollup_decision": "research_only_not_trade_signal",
            "reason": reason,
        },
        "runtime_db_write": False,
        "meemee_unchanged": True,
        "production_ranking_changed": False,
    }
    _write_json(run_dir / "ytd_high_months_after_decline_eval.json", report)
    _write_json(output_root / "latest_ytd_high_months_after_decline_eval.json", {"run_root": str(run_dir), **report})
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
