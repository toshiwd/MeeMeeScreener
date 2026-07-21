from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


AXIS_ID = "tradex_short_shape_candidate_rules_full_eval_v1"
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
    lead(c, 20) OVER wc AS c20,
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
    CASE WHEN ma20_lag5 > 0 THEN (ma20 / ma20_lag5) - 1 END AS ma20_slope5,
    CASE WHEN ma60_lag10 > 0 THEN (ma60 / ma60_lag10) - 1 END AS ma60_slope10,
    CASE WHEN vol20 > 0 THEN v / vol20 END AS volume_ratio20,
    CASE WHEN c > 0 THEN (c20 / c) - 1 END AS ret20,
    CASE WHEN c > 0 THEN (min_l20 / c) - 1 END AS mae20,
    CASE WHEN c > 0 THEN (max_h10 / c) - 1 END AS adverse10
  FROM lagged
)
"""


RULES = [
    {
        "rule_id": "bottom_lift_failure_core",
        "description": "pos_from_low120 >= 0.6 and room_to_low60 >= 0.1",
        "where": "pos_from_low120 >= 0.6 AND room_to_low60 >= 0.1",
    },
    {
        "rule_id": "bottom_lift_normalized",
        "description": "bottom lift core excluding abnormal forward return / extreme adverse artifacts",
        "where": "pos_from_low120 >= 0.6 AND room_to_low60 >= 0.1 AND ret20 BETWEEN -0.8 AND 0.8 AND adverse10 <= 0.8",
    },
    {
        "rule_id": "bottom_lift_broken_rebound_proxy",
        "description": "bottom lift with broken rebound proxy: below 20MA or close to 60MA, not above all MA stack",
        "where": "pos_from_low120 >= 0.6 AND room_to_low60 >= 0.1 AND ret20 BETWEEN -0.8 AND 0.8 AND adverse10 <= 0.8 AND (dist_ma20 <= 0.03 OR dist_ma60 <= 0.02) AND NOT (dist_ma20 > 0.08 AND dist_ma60 > 0.08 AND dist_ma100 > 0.08)",
    },
    {
        "rule_id": "bottom_lift_intact_uptrend_avoid",
        "description": "bottom lift but intact rising support proxy: price far above MA20/60/100",
        "where": "pos_from_low120 >= 0.6 AND room_to_low60 >= 0.1 AND ret20 BETWEEN -0.8 AND 0.8 AND adverse10 <= 0.8 AND dist_ma20 > 0.08 AND dist_ma60 > 0.08 AND dist_ma100 > 0.08",
    },
    {
        "rule_id": "bottom_lift_above_200ma",
        "description": "pos_from_low120 >= 0.6 and dist_ma200 >= 0",
        "where": "pos_from_low120 >= 0.6 AND dist_ma200 >= 0",
    },
    {
        "rule_id": "small_body_reversal_near_100ma",
        "description": "body_ratio <= 0.2 and dist_ma100 >= -0.1",
        "where": "body_ratio <= 0.2 AND dist_ma100 >= -0.1",
    },
    {
        "rule_id": "bottom_lift_low_volume",
        "description": "pos_from_low120 >= 0.6 and volume_ratio20 <= 1.8",
        "where": "pos_from_low120 >= 0.6 AND volume_ratio20 <= 1.8",
    },
    {
        "rule_id": "volume_event_avoid",
        "description": "volume_ratio20 >= 2.0",
        "where": "volume_ratio20 >= 2.0",
    },
    {
        "rule_id": "baseline_all_forward_available",
        "description": "all rows with 20d forward outcome",
        "where": "ret20 IS NOT NULL",
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
  avg(CASE WHEN ret20 < 0 THEN 1 ELSE 0 END) AS down20_rate,
  avg(CASE WHEN ret20 <= -0.10 THEN 1 ELSE 0 END) AS close_down_10pct_20d_rate,
  avg(CASE WHEN mae20 <= -0.10 THEN 1 ELSE 0 END) AS touch_down_10pct_20d_rate,
  avg(CASE WHEN adverse10 >= 0.05 THEN 1 ELSE 0 END) AS adverse_up_5pct_10d_rate,
  quantile_cont(ret20, 0.25) AS ret20_p25,
  quantile_cont(ret20, 0.10) AS ret20_p10
FROM feat
WHERE ret20 IS NOT NULL AND {rule["where"]}
"""
    )
    row = conn.execute(query).fetchone()
    keys = [
        "n",
        "unique_codes",
        "avg_ret20",
        "down20_rate",
        "close_down_10pct_20d_rate",
        "touch_down_10pct_20d_rate",
        "adverse_up_5pct_10d_rate",
        "ret20_p25",
        "ret20_p10",
    ]
    return {**rule, **dict(zip(keys, row, strict=False))}


def run(*, db_path: Path, output_root: Path) -> Path:
    run_dir = output_root / f"{_tag()}-{AXIS_ID}"
    run_dir.mkdir(parents=True, exist_ok=False)
    with duckdb.connect(str(db_path), read_only=True) as conn:
        results = [_eval_rule(conn, rule) for rule in RULES]
    baseline = next(row for row in results if row["rule_id"] == "baseline_all_forward_available")
    for row in results:
        row["lift_touch10_vs_baseline"] = (
            row["touch_down_10pct_20d_rate"] - baseline["touch_down_10pct_20d_rate"]
            if row.get("touch_down_10pct_20d_rate") is not None
            else None
        )
        row["lift_down20_vs_baseline"] = (
            row["down20_rate"] - baseline["down20_rate"] if row.get("down20_rate") is not None else None
        )
    leaderboard = sorted(
        [row for row in results if row["rule_id"] != "baseline_all_forward_available"],
        key=lambda row: (row["touch_down_10pct_20d_rate"], row["down20_rate"], -row["adverse_up_5pct_10d_rate"]),
        reverse=True,
    )
    report = {
        "schema_version": f"{AXIS_ID}_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {
            "universe": "all daily_bars rows with 20d forward outcome",
            "period": "all available stocks.duckdb daily_bars history",
            "changed_axis": "candidate short-shape classification rules from labeled images",
            "cost_slippage": "none",
        },
        "baseline": baseline,
        "leaderboard": leaderboard,
        "all_results": results,
        "decision": {
            "candidate_local_decision": "hold_for_image_cluster_refinement",
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": "candidate rules must beat baseline materially on full history before promotion",
        },
        "runtime_db_write": False,
        "meemee_unchanged": True,
        "production_ranking_changed": False,
    }
    _write_json(run_dir / "candidate_rules_full_eval.json", report)
    _write_json(output_root / "latest_candidate_rules_full_eval.json", {"run_root": str(run_dir), **report})
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
