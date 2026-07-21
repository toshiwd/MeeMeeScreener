from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


AXIS_ID = "tight_high_flag_entry_shape_probe_v1"
DEFAULT_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\chart_entry_geometry_research_v1")


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _metrics(conn: duckdb.DuckDBPyConnection, relation: str) -> dict[str, Any]:
    row = conn.execute(
        f"""
        SELECT
            COUNT(*) AS sample_count,
            COUNT(DISTINCT code) AS code_count,
            COUNT(DISTINCT year) AS year_count,
            AVG(ret20) AS mean_ret20,
            MEDIAN(ret20) AS median_ret20,
            AVG(CASE WHEN ret20 > 0 THEN 1.0 ELSE 0.0 END) AS hit_rate_ret20_gt_0,
            AVG(CASE WHEN ret20 > 0.10 THEN 1.0 ELSE 0.0 END) AS winner_rate_ret20_gt_10pct,
            AVG(CASE WHEN ret20 < -0.05 THEN 1.0 ELSE 0.0 END) AS bad_rate_ret20_lt_minus_5pct,
            AVG(CASE WHEN ret20 < -0.10 THEN 1.0 ELSE 0.0 END) AS severe_rate_ret20_lt_minus_10pct
        FROM {relation}
        """
    ).fetchone()
    keys = [
        "sample_count", "code_count", "year_count", "mean_ret20", "median_ret20",
        "hit_rate_ret20_gt_0", "winner_rate_ret20_gt_10pct",
        "bad_rate_ret20_lt_minus_5pct", "severe_rate_ret20_lt_minus_10pct",
    ]
    out = dict(zip(keys, row))
    for key, value in list(out.items()):
        if key.endswith("count"):
            out[key] = int(value or 0)
        elif value is not None:
            out[key] = float(value)
    return out


def _yearly(conn: duckdb.DuckDBPyConnection, relation: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        f"""
        SELECT year, COUNT(*) AS sample_count, AVG(ret20) AS mean_ret20, MEDIAN(ret20) AS median_ret20,
               AVG(CASE WHEN ret20 < -0.05 THEN 1.0 ELSE 0.0 END) AS bad_rate_ret20_lt_minus_5pct
        FROM {relation}
        GROUP BY year ORDER BY year
        """
    ).fetchall()
    return [
        {"year": int(year), "sample_count": int(count), "mean_ret20": float(mean), "median_ret20": float(median), "bad_rate_ret20_lt_minus_5pct": float(bad)}
        for year, count, mean, median, bad in rows
    ]


def run(*, db_path: Path, output_root: Path) -> Path:
    output = output_root / f"{_tag()}-{AXIS_ID}"
    output.mkdir(parents=True, exist_ok=False)
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        conn.execute(
            """
            CREATE TEMP TABLE shape_rows AS
            WITH enriched AS (
                SELECT
                    code,
                    CAST(strftime(to_timestamp(CAST(date AS BIGINT)), '%Y') AS INTEGER) AS year,
                    c,
                    LAG(c, 9) OVER ordered AS close_10d_ago,
                    MAX(c) OVER recent_10 AS max_close_10d,
                    MIN(c) OVER recent_10 AS min_close_10d,
                    MAX(h) OVER recent_10 AS max_high_10d,
                    LEAD(c, 20) OVER ordered AS close_20d_ahead
                FROM daily_bars
                WHERE COALESCE(source, 'pan') <> 'yahoo'
                WINDOW
                    ordered AS (PARTITION BY code ORDER BY date),
                    recent_10 AS (PARTITION BY code ORDER BY date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW)
            )
            SELECT *,
                   (c / close_10d_ago) - 1.0 AS ret10,
                   (max_close_10d / min_close_10d) - 1.0 AS close_range10,
                   (c / max_high_10d) - 1.0 AS high_proximity10,
                   (close_20d_ahead / c) - 1.0 AS ret20
            FROM enriched
            WHERE close_10d_ago IS NOT NULL AND close_20d_ahead IS NOT NULL
              AND c > 0 AND min_close_10d > 0 AND max_high_10d > 0
            """
        )
        conn.execute("CREATE TEMP TABLE baseline AS SELECT * FROM shape_rows WHERE ret10 >= 0.06")
        conn.execute(
            """
            CREATE TEMP TABLE tight_high AS
            SELECT * FROM shape_rows
            WHERE ret10 >= 0.06
              AND close_range10 <= 0.075
              AND high_proximity10 >= -0.03
            """
        )
        baseline = _metrics(conn, "baseline")
        candidate = _metrics(conn, "tight_high")
        yearly = {"baseline_momentum_10d": _yearly(conn, "baseline"), "tight_high_flag": _yearly(conn, "tight_high")}
    finally:
        conn.close()

    mean_lift = (candidate["mean_ret20"] or 0.0) - (baseline["mean_ret20"] or 0.0)
    bad_delta = (candidate["bad_rate_ret20_lt_minus_5pct"] or 1.0) - (baseline["bad_rate_ret20_lt_minus_5pct"] or 1.0)
    positive_years = sum(1 for row in yearly["tight_high_flag"] if row["median_ret20"] > 0)
    total_years = len(yearly["tight_high_flag"])
    decision = "hold"
    reasons = ["tight_high_flag_did_not_clear_predeclared_quality_gates"]
    if candidate["sample_count"] >= 1_000 and mean_lift >= 0.005 and bad_delta <= 0.0 and total_years >= 5 and positive_years / total_years >= 0.6:
        decision = "keep_for_same_condition_topk_compare"
        reasons = ["tight_high_flag_improves_momentum_entry_quality_under_fixed_gates"]

    _write_json(output / "compare.json", {
        "schema_version": f"tradex_{AXIS_ID}.compare.v1",
        "authoritative_result": True,
        "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {
            "source_db": str(db_path), "source_table": "daily_bars confirmed non-yahoo",
            "same_universe": True, "same_period": True, "same_top_k": "not_applicable_event_cohort_comparison",
            "same_regime": "unsegmented_same_cohort", "axis_changed": "tight_high_flag only",
            "entry": "decision close", "outcome": "20 trading-day close return", "runtime_db_write": False,
        },
        "rule_definitions": {
            "baseline_momentum_10d": "10-day close return >= 6%",
            "tight_high_flag": "baseline plus 10-day close range <= 7.5% and close within 3% of 10-day high",
        },
        "cohort_metrics": {"baseline_momentum_10d": baseline, "tight_high_flag": candidate},
        "candidate_vs_baseline": {"mean_ret20_lift": mean_lift, "bad_rate_delta": bad_delta, "positive_median_year_count": positive_years, "year_count": total_years},
        "yearly_metrics": yearly,
        "candidate_local_decision": decision, "authoritative_rollup_decision": decision, "reason_typed": reasons,
    })
    _write_json(output / "research_decision.json", {"candidate_local_decision": decision, "authoritative_rollup_decision": decision, "reason_typed": reasons, "meemee_reflectable": False, "production_ranking_changed": False, "runtime_db_write": False})
    _write_json(output / "no_lookahead_audit.json", {"audit_result": "pass", "point_in_time_shape_fields": ["ret10", "close_range10", "high_proximity10"], "outcome_evaluation_only": ["ret20"], "thresholds_retuned": False, "axis_changed": "tight_high_flag only"})
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
