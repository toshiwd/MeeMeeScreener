from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from tradex_bottom_lift_confirmation_timing_eval_v1 import FEATURE_CTE


AXIS_ID = "tradex_bottom_lift_confirmation_image_sample_plan_v1"
DEFAULT_DB = Path("stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_shape_classification_v1")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _yyyymmdd_expr(column: str) -> str:
    return f"strftime(to_timestamp({column}) AT TIME ZONE 'Asia/Tokyo', '%Y-%m-%d')"


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")


def run(*, db_path: Path, output_root: Path, per_bucket: int) -> Path:
    run_dir = output_root / f"{_tag()}-{AXIS_ID}"
    run_dir.mkdir(parents=True, exist_ok=False)

    query = f"""
    CREATE TEMP TABLE confirmation_outcomes AS {FEATURE_CTE}
    """
    sample_query = f"""
    WITH enriched AS (
      SELECT
        *,
        {_yyyymmdd_expr('signal_date')} AS signal_date_ymd,
        {_yyyymmdd_expr('entry_date')} AS entry_date_ymd,
        CASE
          WHEN confirm_type = 'long_support_rebound_ma60'
            AND ret20 >= 0.10
            AND mae20 > -0.05 THEN 'ma60_rebound_clean_success'
          WHEN confirm_type = 'long_support_rebound_ma60'
            AND ret20 >= 0.10 THEN 'ma60_rebound_messy_success'
          WHEN confirm_type = 'long_support_rebound_ma60'
            AND ret20 <= -0.05 THEN 'ma60_rebound_failure'
          WHEN confirm_type = 'long_support_rebound_ma20'
            AND ret20 >= 0.10
            AND mae20 > -0.05 THEN 'ma20_rebound_clean_success'
          WHEN confirm_type = 'long_support_rebound_ma20'
            AND ret20 <= -0.05 THEN 'ma20_rebound_failure'
          ELSE 'other'
        END AS bucket
      FROM confirmation_outcomes
      WHERE confirm_type IN ('long_support_rebound_ma60', 'long_support_rebound_ma20')
    ),
    ranked AS (
      SELECT
        *,
        row_number() OVER (
          PARTITION BY bucket
          ORDER BY entry_date DESC, abs(ret20) DESC, code
        ) AS bucket_rank
      FROM enriched
      WHERE bucket <> 'other'
    )
    SELECT
      code,
      signal_date_ymd AS signal_date,
      entry_date_ymd AS entry_date,
      days_after,
      confirm_type,
      bucket,
      entry_close,
      ret20,
      mfe20,
      mae20,
      adverse10_for_short,
      bucket_rank
    FROM ranked
    WHERE bucket_rank <= ?
    ORDER BY bucket, bucket_rank
    """
    with duckdb.connect(str(db_path), read_only=True) as conn:
        conn.execute(query)
        rows = conn.execute(sample_query, [per_bucket]).fetchdf().to_dict("records")
        bucket_counts = conn.execute(
            """
            SELECT
              CASE
                WHEN confirm_type = 'long_support_rebound_ma60'
                  AND ret20 >= 0.10
                  AND mae20 > -0.05 THEN 'ma60_rebound_clean_success'
                WHEN confirm_type = 'long_support_rebound_ma60'
                  AND ret20 >= 0.10 THEN 'ma60_rebound_messy_success'
                WHEN confirm_type = 'long_support_rebound_ma60'
                  AND ret20 <= -0.05 THEN 'ma60_rebound_failure'
                WHEN confirm_type = 'long_support_rebound_ma20'
                  AND ret20 >= 0.10
                  AND mae20 > -0.05 THEN 'ma20_rebound_clean_success'
                WHEN confirm_type = 'long_support_rebound_ma20'
                  AND ret20 <= -0.05 THEN 'ma20_rebound_failure'
                ELSE 'other'
              END AS bucket,
              count(*) AS n,
              count(DISTINCT code) AS unique_codes
            FROM confirmation_outcomes
            WHERE confirm_type IN ('long_support_rebound_ma60', 'long_support_rebound_ma20')
            GROUP BY bucket
            ORDER BY n DESC
            """
        ).fetchdf().to_dict("records")

    manifest = {
        "schema_version": f"{AXIS_ID}_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "source_axis_id": "tradex_bottom_lift_confirmation_timing_eval_v1",
        "selection_policy": {
            "purpose": "manual visual classification of best bottom_lift confirmation branches",
            "per_bucket": per_bucket,
            "ordering": "recent first, then larger absolute ret20",
            "center_chart_on": "entry_date",
            "review_requires": ["decision-time image", "after-result image"],
        },
        "bucket_counts": bucket_counts,
        "sample_count": len(rows),
        "samples_jsonl": str(run_dir / "image_sample_plan.jsonl"),
        "runtime_db_write": False,
        "meemee_unchanged": True,
    }
    _write_jsonl(run_dir / "image_sample_plan.jsonl", rows)
    _write_json(run_dir / "image_sample_plan_audit.json", manifest)
    _write_json(output_root / "latest_bottom_lift_confirmation_image_sample_plan.json", {"run_root": str(run_dir), **manifest})
    return run_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--per-bucket", type=int, default=8)
    args = parser.parse_args()
    print(run(db_path=args.db, output_root=args.output_root, per_bucket=args.per_bucket))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
