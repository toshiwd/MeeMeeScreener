from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


AXIS_ID = "tradex_bottom_lift_long_short_split_eval_v1"
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
    avg(c) OVER w7 AS ma7,
    avg(c) OVER w20 AS ma20,
    avg(c) OVER w60 AS ma60,
    avg(c) OVER w100 AS ma100,
    avg(c) OVER w200 AS ma200,
    min(l) OVER w20 AS low20,
    min(l) OVER w60 AS low60,
    max(h) OVER w20 AS high20,
    max(h) OVER w60 AS high60,
    max(h) OVER w120 AS high120,
    min(l) OVER w120 AS low120,
    avg(v) OVER w20 AS vol20,
    lead(c, 5) OVER wc AS c5,
    lead(c, 10) OVER wc AS c10,
    lead(c, 20) OVER wc AS c20,
    max(h) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 1 FOLLOWING AND 20 FOLLOWING) AS max_h20,
    min(l) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 1 FOLLOWING AND 20 FOLLOWING) AS min_l20,
    max(h) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 1 FOLLOWING AND 10 FOLLOWING) AS max_h10
  FROM daily_bars
  WINDOW
    wc AS (PARTITION BY code ORDER BY date),
    w7 AS (PARTITION BY code ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
    w20 AS (PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
    w60 AS (PARTITION BY code ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW),
    w100 AS (PARTITION BY code ORDER BY date ROWS BETWEEN 99 PRECEDING AND CURRENT ROW),
    w120 AS (PARTITION BY code ORDER BY date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW),
    w200 AS (PARTITION BY code ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW)
),
lagged AS (
  SELECT
    *,
    lag(c, 5) OVER wc AS c_lag5,
    lag(c, 20) OVER wc AS c_lag20,
    lag(c, 60) OVER wc AS c_lag60,
    lag(ma7, 3) OVER wc AS ma7_lag3,
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
    CASE WHEN c > 0 THEN (c - ma200) / c END AS dist_ma200,
    CASE WHEN c > 0 THEN (c - low20) / c END AS room_to_low20,
    CASE WHEN c > 0 THEN (c - low60) / c END AS room_to_low60,
    CASE WHEN c > 0 THEN (high20 / c) - 1 END AS overhead_to_high20,
    CASE WHEN c > 0 THEN (high60 / c) - 1 END AS overhead_to_high60,
    CASE WHEN c > 0 THEN (high120 / c) - 1 END AS overhead_to_high120,
    CASE WHEN low120 > 0 THEN (c / low120) - 1 END AS pos_from_low120,
    CASE WHEN high120 > 0 THEN (c / high120) - 1 END AS drawdown_from_high120,
    CASE WHEN c_lag5 > 0 THEN (c / c_lag5) - 1 END AS ret_5_back,
    CASE WHEN c_lag20 > 0 THEN (c / c_lag20) - 1 END AS ret_20_back,
    CASE WHEN c_lag60 > 0 THEN (c / c_lag60) - 1 END AS ret_60_back,
    CASE WHEN ma7_lag3 > 0 THEN (ma7 / ma7_lag3) - 1 END AS ma7_slope3,
    CASE WHEN ma20_lag5 > 0 THEN (ma20 / ma20_lag5) - 1 END AS ma20_slope5,
    CASE WHEN ma60_lag10 > 0 THEN (ma60 / ma60_lag10) - 1 END AS ma60_slope10,
    CASE WHEN vol20 > 0 THEN v / vol20 END AS volume_ratio20,
    CASE WHEN c > 0 THEN (c5 / c) - 1 END AS ret5,
    CASE WHEN c > 0 THEN (c10 / c) - 1 END AS ret10,
    CASE WHEN c > 0 THEN (c20 / c) - 1 END AS ret20,
    CASE WHEN c > 0 THEN (max_h20 / c) - 1 END AS mfe20,
    CASE WHEN c > 0 THEN (min_l20 / c) - 1 END AS mae20,
    CASE WHEN c > 0 THEN (max_h10 / c) - 1 END AS adverse10_for_short
  FROM lagged
)
"""


RULES = [
    {
        "rule_id": "baseline_all",
        "side": "neutral",
        "description": "all rows with 20d forward outcome",
        "where": "ret20 IS NOT NULL",
    },
    {
        "rule_id": "bottom_lift_all_normalized",
        "side": "neutral",
        "description": "bottom_lift normalized excluding abnormal extreme returns",
        "where": "pos_from_low120 >= 0.6 AND room_to_low60 >= 0.1 AND ret20 BETWEEN -0.8 AND 0.8 AND mfe20 <= 0.8",
    },
    {
        "rule_id": "bottom_lift_intact_uptrend_long",
        "side": "long",
        "description": "price above MA stack with upward short/mid MA slopes",
        "where": "pos_from_low120 >= 0.6 AND room_to_low60 >= 0.1 AND ret20 BETWEEN -0.8 AND 0.8 AND mfe20 <= 0.8 AND dist_ma20 > 0.05 AND dist_ma60 > 0.05 AND dist_ma100 > 0.03 AND ma7_slope3 > 0 AND ma20_slope5 > 0",
    },
    {
        "rule_id": "bottom_lift_pullback_to_support_long",
        "side": "long",
        "description": "bottom lift, above long trend, near MA20/60 support after pullback",
        "where": "pos_from_low120 >= 0.6 AND room_to_low60 >= 0.1 AND ret20 BETWEEN -0.8 AND 0.8 AND mfe20 <= 0.8 AND dist_ma60 > 0 AND dist_ma100 > 0 AND dist_ma20 BETWEEN -0.03 AND 0.05 AND ma60_slope10 > 0",
    },
    {
        "rule_id": "bottom_lift_broken_rebound_short",
        "side": "short",
        "description": "bottom lift but rebound broken: near/below MA20 or MA60 and not strong MA stack",
        "where": "pos_from_low120 >= 0.6 AND room_to_low60 >= 0.1 AND ret20 BETWEEN -0.8 AND 0.8 AND mfe20 <= 0.8 AND (dist_ma20 <= 0.03 OR dist_ma60 <= 0.02) AND NOT (dist_ma20 > 0.08 AND dist_ma60 > 0.08 AND dist_ma100 > 0.08)",
    },
    {
        "rule_id": "bottom_lift_climax_distribution_short",
        "side": "short",
        "description": "bottom lift with high volume/climax and weak close",
        "where": "pos_from_low120 >= 0.6 AND room_to_low60 >= 0.1 AND ret20 BETWEEN -0.8 AND 0.8 AND mfe20 <= 0.8 AND volume_ratio20 >= 2.0 AND close_pos <= 0.35",
    },
]


def _eval_rule(conn: duckdb.DuckDBPyConnection, rule: dict[str, str]) -> dict[str, Any]:
    query = (
        FEATURE_CTE
        + f"""
SELECT
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
FROM feat
WHERE ret20 IS NOT NULL AND {rule["where"]}
"""
    )
    row = conn.execute(query).fetchone()
    keys = [
        "n",
        "unique_codes",
        "avg_ret20",
        "up20_rate",
        "down20_rate",
        "close_up_10pct_20d_rate",
        "touch_up_10pct_20d_rate",
        "adverse_down_5pct_20d_rate",
        "close_down_10pct_20d_rate",
        "touch_down_10pct_20d_rate",
        "adverse_up_5pct_10d_rate",
        "ret20_p25",
        "ret20_p75",
    ]
    return {**rule, **dict(zip(keys, row, strict=False))}


def run(*, db_path: Path, output_root: Path) -> Path:
    run_dir = output_root / f"{_tag()}-{AXIS_ID}"
    run_dir.mkdir(parents=True, exist_ok=False)
    with duckdb.connect(str(db_path), read_only=True) as conn:
        results = [_eval_rule(conn, rule) for rule in RULES]
    baseline = next(row for row in results if row["rule_id"] == "baseline_all")
    for row in results:
        row["lift_touch_up10_vs_baseline"] = row["touch_up_10pct_20d_rate"] - baseline["touch_up_10pct_20d_rate"]
        row["lift_touch_down10_vs_baseline"] = row["touch_down_10pct_20d_rate"] - baseline["touch_down_10pct_20d_rate"]
    report = {
        "schema_version": f"{AXIS_ID}_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {
            "universe": "all daily_bars rows with 20d forward outcome",
            "changed_axis": "bottom_lift long/short split",
            "cost_slippage": "none",
            "abnormal_filter": "ret20 between -80pct and +80pct, mfe20 <= +80pct for bottom_lift variants",
        },
        "baseline": baseline,
        "results": results,
        "decision": {
            "candidate_local_decision": "hold_for_split_refinement",
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": "bottom_lift contains both long continuation and short failure; split must reduce adverse move before use",
        },
        "runtime_db_write": False,
        "meemee_unchanged": True,
        "production_ranking_changed": False,
    }
    _write_json(run_dir / "bottom_lift_long_short_split_eval.json", report)
    _write_json(output_root / "latest_bottom_lift_long_short_split_eval.json", {"run_root": str(run_dir), **report})
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
