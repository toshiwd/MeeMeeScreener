from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


AXIS_ID = "tradex_2201_like_visual_event_dataset_v1"
DEFAULT_DB = Path("stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\2201_like_visual_event_dataset_v1")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, ensure_ascii=False, default=str) + "\n" for row in rows), encoding="utf-8")


def _date_text(value: int | str) -> str:
    text = str(value)
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    try:
        numeric = int(float(text))
    except ValueError:
        return text
    if numeric > 1_000_000_000:
        return datetime.fromtimestamp(numeric, tz=timezone.utc).strftime("%Y-%m-%d")
    return text


def _clean_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def _clean_row(row: dict[str, Any]) -> dict[str, Any]:
    return {key: _clean_value(value) for key, value in row.items()}


FEATURE_SQL = r"""
WITH base AS (
  SELECT
    code,
    date,
    o, h, l, c, v,
    avg(c) OVER w7 AS ma7,
    avg(c) OVER w20 AS ma20,
    avg(c) OVER w60 AS ma60,
    avg(c) OVER w100 AS ma100,
    min(l) OVER w60 AS low60,
    max(h) OVER w20 AS high20,
    max(h) OVER w60 AS high60,
    min(l) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 45 PRECEDING AND 5 PRECEDING) AS pre_low45,
    max(h) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 90 PRECEDING AND 20 PRECEDING) AS pre_high90,
    lead(c, 5) OVER (PARTITION BY code ORDER BY date) AS c5,
    lead(c, 10) OVER (PARTITION BY code ORDER BY date) AS c10,
    lead(c, 20) OVER (PARTITION BY code ORDER BY date) AS c20,
    min(l) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 1 FOLLOWING AND 20 FOLLOWING) AS min_l20,
    max(h) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 1 FOLLOWING AND 10 FOLLOWING) AS max_h10
  FROM daily_bars
  WINDOW
    w7 AS (PARTITION BY code ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
    w20 AS (PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
    w60 AS (PARTITION BY code ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW),
    w100 AS (PARTITION BY code ORDER BY date ROWS BETWEEN 99 PRECEDING AND CURRENT ROW)
),
feat AS (
  SELECT
    *,
    CASE WHEN h > l THEN (h - greatest(o, c)) / (h - l) ELSE NULL END AS upper_wick_ratio,
    CASE WHEN h > l THEN (c - l) / (h - l) ELSE NULL END AS close_pos,
    CASE WHEN h > l THEN abs(c - o) / (h - l) ELSE NULL END AS body_ratio,
    CASE WHEN c > 0 THEN (c - ma7) / c ELSE NULL END AS dist_ma7,
    CASE WHEN c > 0 THEN (c - ma20) / c ELSE NULL END AS dist_ma20,
    CASE WHEN c > 0 THEN (c - ma60) / c ELSE NULL END AS dist_ma60,
    CASE WHEN c > 0 THEN (c - ma100) / c ELSE NULL END AS dist_ma100,
    CASE WHEN c > 0 THEN (c - low60) / c ELSE NULL END AS room_to_low60,
    CASE WHEN pre_low45 > 0 THEN (c / pre_low45) - 1 ELSE NULL END AS rebound_from_pre_low45,
    CASE WHEN pre_high90 > 0 THEN (c / pre_high90) - 1 ELSE NULL END AS drawdown_from_pre_high90,
    CASE WHEN c > 0 THEN (c5 / c) - 1 ELSE NULL END AS ret5,
    CASE WHEN c > 0 THEN (c10 / c) - 1 ELSE NULL END AS ret10,
    CASE WHEN c > 0 THEN (c20 / c) - 1 ELSE NULL END AS ret20,
    CASE WHEN c > 0 THEN (min_l20 / c) - 1 ELSE NULL END AS mae20,
    CASE WHEN c > 0 THEN (max_h10 / c) - 1 ELSE NULL END AS adverse10
  FROM base
)
SELECT
  code,
  date,
  o, h, l, c, v,
  ma7, ma20, ma60, ma100,
  upper_wick_ratio,
  close_pos,
  body_ratio,
  dist_ma7,
  dist_ma20,
  dist_ma60,
  dist_ma100,
  room_to_low60,
  rebound_from_pre_low45,
  drawdown_from_pre_high90,
  ret5,
  ret10,
  ret20,
  mae20,
  adverse10,
  (
    30 * least(1.0, greatest(0.0, (upper_wick_ratio - 0.75) / 0.20)) +
    20 * least(1.0, greatest(0.0, (0.15 - close_pos) / 0.15)) +
    15 * least(1.0, greatest(0.0, (room_to_low60 - 0.08) / 0.12)) +
    15 * least(1.0, greatest(0.0, (rebound_from_pre_low45 - 0.04) / 0.16)) +
    10 * CASE WHEN dist_ma20 BETWEEN -0.02 AND 0.08 THEN 1 ELSE 0 END +
    10 * CASE WHEN drawdown_from_pre_high90 BETWEEN -0.18 AND 0.03 THEN 1 ELSE 0 END
  ) AS similarity_score
FROM feat
WHERE
  upper_wick_ratio >= 0.75
  AND close_pos <= 0.15
  AND room_to_low60 >= 0.08
  AND dist_ma20 BETWEEN -0.02 AND 0.08
  AND dist_ma60 BETWEEN -0.03 AND 0.08
  AND rebound_from_pre_low45 >= 0.04
  AND drawdown_from_pre_high90 BETWEEN -0.18 AND 0.03
  AND c20 IS NOT NULL
ORDER BY similarity_score DESC, date DESC
"""


def _metric(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"n": 0}

    def nums(key: str) -> list[float]:
        values = []
        for row in rows:
            value = row.get(key)
            if value is None and isinstance(row.get("forward"), dict):
                value = row["forward"].get(key)
            if value is not None:
                values.append(float(value))
        return values

    ret20 = nums("ret20")
    mae20 = nums("mae20")
    adverse10 = nums("adverse10")
    return {
        "n": len(rows),
        "avg_ret20": sum(ret20) / len(ret20) if ret20 else None,
        "down20_rate": sum(1 for value in ret20 if value < 0) / len(ret20) if ret20 else None,
        "close_down_10pct_20d_rate": sum(1 for value in ret20 if value <= -0.10) / len(ret20) if ret20 else None,
        "touch_down_10pct_20d_rate": sum(1 for value in mae20 if value <= -0.10) / len(mae20) if mae20 else None,
        "adverse_up_5pct_10d_rate": sum(1 for value in adverse10 if value >= 0.05) / len(adverse10) if adverse10 else None,
    }


def _row_to_event(row: dict[str, Any]) -> dict[str, Any]:
    row = _clean_row(row)
    ret20 = row.get("ret20")
    mae20 = row.get("mae20")
    adverse10 = row.get("adverse10")
    if mae20 is not None and float(mae20) <= -0.10:
        outcome_bucket = "success_touch_down_10pct_20d"
    elif ret20 is not None and float(ret20) < 0:
        outcome_bucket = "moderate_down_20d"
    elif adverse10 is not None and float(adverse10) >= 0.05:
        outcome_bucket = "failure_squeeze_10d"
    else:
        outcome_bucket = "failure_or_flat"
    return {
        "schema_version": f"{AXIS_ID}_event_v1",
        "pattern_id": "2201_like_rebound_upper_rejection",
        "code": str(row["code"]),
        "as_of": _date_text(row["date"]),
        "date": int(float(row["date"])),
        "similarity_score": float(row["similarity_score"]),
        "features": {
            key: row.get(key)
            for key in [
                "o",
                "h",
                "l",
                "c",
                "v",
                "ma7",
                "ma20",
                "ma60",
                "ma100",
                "upper_wick_ratio",
                "close_pos",
                "body_ratio",
                "dist_ma7",
                "dist_ma20",
                "dist_ma60",
                "dist_ma100",
                "room_to_low60",
                "rebound_from_pre_low45",
                "drawdown_from_pre_high90",
            ]
        },
        "forward": {
            "ret5": row.get("ret5"),
            "ret10": row.get("ret10"),
            "ret20": row.get("ret20"),
            "mae20": row.get("mae20"),
            "adverse10": row.get("adverse10"),
            "outcome_bucket": outcome_bucket,
        },
        "visual_review_status": "unreviewed",
        "non_scope": ["production_ranking", "runtime_db", "MeeMee_display"],
    }


def _sample(events: list[dict[str, Any]], *, per_bucket: int, recent: int) -> list[dict[str, Any]]:
    selected: dict[str, dict[str, Any]] = {}
    for bucket in [
        "success_touch_down_10pct_20d",
        "moderate_down_20d",
        "failure_squeeze_10d",
        "failure_or_flat",
    ]:
        bucket_rows = [row for row in events if row["forward"]["outcome_bucket"] == bucket]
        bucket_rows.sort(key=lambda row: (-row["similarity_score"], -row["date"]))
        for row in bucket_rows[:per_bucket]:
            selected[f"{row['code']}:{row['as_of']}"] = {**row, "sample_reason": f"top_similarity_{bucket}"}
    recent_rows = sorted(events, key=lambda row: (-row["date"], -row["similarity_score"]))
    for row in recent_rows[:recent]:
        selected.setdefault(f"{row['code']}:{row['as_of']}", {**row, "sample_reason": "recent_match"})
    return list(selected.values())


def run(*, db_path: Path, output_root: Path, per_bucket: int, recent: int) -> Path:
    run_dir = output_root / f"{_tag()}-{AXIS_ID}"
    run_dir.mkdir(parents=True, exist_ok=False)
    with duckdb.connect(str(db_path), read_only=True) as conn:
        rows = conn.execute(FEATURE_SQL).fetchdf().to_dict("records")
    events = [_row_to_event(row) for row in rows]
    samples = _sample(events, per_bucket=per_bucket, recent=recent)
    _write_jsonl(run_dir / "events_all.jsonl", events)
    _write_jsonl(run_dir / "screenshot_sample_events.jsonl", samples)
    sample_arg = ",".join(f"{row['code']}:{row['as_of']}" for row in samples)
    screenshot_command = (
        "node scripts\\meemee_detail_clean_screenshot_batch_v1.mjs "
        "--base-url http://127.0.0.1:28888 "
        "--api-base http://127.0.0.1:28888/api "
        f"--output-root {str(run_dir / 'screenshots')} "
        f"--samples {sample_arg} "
        "--centered --center-lookback-months 8 --center-lookahead-months 3 "
        "--viewport 1600x1000 --viewport-fallback"
    )
    report = {
        "schema_version": f"{AXIS_ID}_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "research_phase": "branching_generation",
        "db_path": str(db_path),
        "fixed_evaluation_conditions": {
            "pattern_id": "2201_like_rebound_upper_rejection",
            "universe": "all codes in daily_bars",
            "period": "all daily_bars rows with 20d forward outcome available",
            "rule": {
                "upper_wick_ratio_min": 0.75,
                "close_pos_max": 0.15,
                "room_to_low60_min": 0.08,
                "dist_ma20_range": [-0.02, 0.08],
                "dist_ma60_range": [-0.03, 0.08],
                "rebound_from_pre_low45_min": 0.04,
                "drawdown_from_pre_high90_range": [-0.18, 0.03],
            },
            "sample_policy": {
                "per_outcome_bucket": per_bucket,
                "recent_matches": recent,
            },
        },
        "metrics": {
            "all_events": _metric(events),
            "sample_events": _metric(samples),
        },
        "outcome_counts": {
            bucket: sum(1 for row in events if row["forward"]["outcome_bucket"] == bucket)
            for bucket in [
                "success_touch_down_10pct_20d",
                "moderate_down_20d",
                "failure_squeeze_10d",
                "failure_or_flat",
            ]
        },
        "artifacts": {
            "events_all_jsonl": str(run_dir / "events_all.jsonl"),
            "screenshot_sample_events_jsonl": str(run_dir / "screenshot_sample_events.jsonl"),
            "screenshot_command_txt": str(run_dir / "screenshot_command.txt"),
        },
        "screenshot_batch_command": screenshot_command,
        "decision": {
            "candidate_local_decision": "hold_for_visual_image_generation",
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": "full historical event extraction is ready; image generation should run on the stratified sample first",
        },
        "runtime_db_write": False,
        "meemee_unchanged": True,
        "production_ranking_changed": False,
    }
    (run_dir / "screenshot_command.txt").write_text(screenshot_command + "\n", encoding="utf-8")
    _write_json(run_dir / "event_dataset_audit.json", report)
    _write_json(output_root / "latest_event_dataset_audit.json", {"run_root": str(run_dir), **report})
    return run_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--per-bucket", type=int, default=8)
    parser.add_argument("--recent", type=int, default=16)
    args = parser.parse_args()
    print(run(db_path=args.db, output_root=args.output_root, per_bucket=args.per_bucket, recent=args.recent))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
