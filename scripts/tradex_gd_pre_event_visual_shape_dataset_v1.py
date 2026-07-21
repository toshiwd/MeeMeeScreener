from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


AXIS_ID = "tradex_gd_pre_event_visual_shape_dataset_v1"
DEFAULT_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\gd_pre_event_visual_shape_dataset_v1")


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


def _clean_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def _clean_row(row: dict[str, Any]) -> dict[str, Any]:
    return {key: _clean_value(value) for key, value in row.items()}


FEATURE_SQL = r"""
WITH normalized AS (
  SELECT
    code,
    date,
    CASE
      WHEN date > 30000000 THEN CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)
      ELSE CAST(date AS INTEGER)
    END AS ymd,
    o, h, l, c, v, source
  FROM daily_bars
  WHERE o IS NOT NULL AND h IS NOT NULL AND l IS NOT NULL AND c IS NOT NULL AND c > 0 AND o > 0
),
base AS (
  SELECT
    *,
    lead(ymd) OVER w AS next_ymd,
    lead(o) OVER w AS next_open,
    lead(c) OVER w AS next_close,
    lead(l) OVER w AS next_low,
    lead(h) OVER w AS next_high,
    avg(c) OVER w7 AS ma7,
    avg(c) OVER w20 AS ma20,
    avg(c) OVER w60 AS ma60,
    avg(v) OVER w20 AS vol20,
    min(l) OVER w60 AS low60,
    max(h) OVER w60 AS high60,
    max(h) OVER (PARTITION BY code ORDER BY ymd ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS prior_high20,
    min(l) OVER (PARTITION BY code ORDER BY ymd ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS prior_low20,
    c / lag(c, 5) OVER w - 1 AS ret5,
    c / lag(c, 20) OVER w - 1 AS ret20,
    c / lag(c, 60) OVER w - 1 AS ret60,
    lead(c, 5) OVER w / c - 1 AS post_ret5,
    lead(c, 20) OVER w / c - 1 AS post_ret20
  FROM normalized
  WINDOW
    w AS (PARTITION BY code ORDER BY ymd),
    w7 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
    w20 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
    w60 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
),
feat AS (
  SELECT
    *,
    next_open / c - 1 AS next_gap_pct,
    next_close / c - 1 AS next_close_ret,
    CASE WHEN h > l THEN (h - greatest(o, c)) / (h - l) ELSE NULL END AS latest_upper_wick_ratio,
    CASE WHEN h > l THEN (c - l) / (h - l) ELSE NULL END AS latest_close_pos,
    CASE WHEN h > l THEN abs(c - o) / (h - l) ELSE NULL END AS latest_body_ratio,
    CASE WHEN high60 > low60 THEN (c - low60) / (high60 - low60) ELSE NULL END AS range60_close_pos,
    CASE WHEN ma7 > 0 THEN c / ma7 - 1 ELSE NULL END AS dist_ma7,
    CASE WHEN ma20 > 0 THEN c / ma20 - 1 ELSE NULL END AS dist_ma20,
    CASE WHEN ma60 > 0 THEN c / ma60 - 1 ELSE NULL END AS dist_ma60,
    CASE WHEN vol20 > 0 THEN v / vol20 ELSE NULL END AS volume_vs_20d_avg,
    CASE WHEN prior_high20 > 0 THEN c / prior_high20 - 1 ELSE NULL END AS dist_prior_high20,
    CASE WHEN prior_low20 > 0 THEN c / prior_low20 - 1 ELSE NULL END AS dist_prior_low20
  FROM base
  WHERE
    next_open IS NOT NULL
    AND next_ymd IS NOT NULL
    AND ma20 IS NOT NULL
    AND ma60 IS NOT NULL
    AND next_ymd > ymd
    AND datediff('day', strptime(CAST(ymd AS VARCHAR), '%Y%m%d'), strptime(CAST(next_ymd AS VARCHAR), '%Y%m%d')) BETWEEN 1 AND 10
),
classified AS (
  SELECT
    *,
    CASE
      WHEN next_gap_pct <= -0.03 THEN 'strong_gd'
      WHEN next_gap_pct <= -0.02 THEN 'primary_gd'
      WHEN next_gap_pct > -0.005 AND next_gap_pct < 0.01 THEN 'control_no_gd'
      ELSE 'other'
    END AS event_class,
    (
      20 * CASE WHEN range60_close_pos >= 0.70 THEN 1 ELSE 0 END +
      20 * CASE WHEN latest_upper_wick_ratio >= 0.45 THEN 1 ELSE 0 END +
      15 * CASE WHEN ret20 >= 0.10 THEN 1 ELSE 0 END +
      15 * CASE WHEN dist_ma20 >= 0.05 THEN 1 ELSE 0 END +
      15 * CASE WHEN dist_prior_high20 >= -0.035 THEN 1 ELSE 0 END +
      15 * CASE WHEN latest_close_pos <= 0.35 THEN 1 ELSE 0 END
    ) AS visual_risk_proxy_score
  FROM feat
)
SELECT
  code,
  ymd AS as_of_ymd,
  next_ymd AS event_ymd,
  o, h, l, c, v,
  next_open,
  next_close,
  next_low,
  next_high,
  next_gap_pct,
  next_close_ret,
  post_ret5,
  post_ret20,
  event_class,
  visual_risk_proxy_score,
  latest_upper_wick_ratio,
  latest_close_pos,
  latest_body_ratio,
  range60_close_pos,
  ret5,
  ret20,
  ret60,
  dist_ma7,
  dist_ma20,
  dist_ma60,
  volume_vs_20d_avg,
  dist_prior_high20,
  dist_prior_low20,
  source
FROM classified
WHERE event_class IN ('strong_gd', 'primary_gd', 'control_no_gd')
ORDER BY event_ymd DESC, visual_risk_proxy_score DESC, code
"""


def _ymd_text(value: int | str) -> str:
    text = str(int(value))
    return f"{text[:4]}-{text[4:6]}-{text[6:8]}" if len(text) == 8 else text


def _event_row(row: dict[str, Any]) -> dict[str, Any]:
    row = _clean_row(row)
    event_class = str(row["event_class"])
    return {
        "schema_version": f"{AXIS_ID}_event_v1",
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "pattern_id": "pre_next_day_gap_down_visual_shape",
        "code": str(row["code"]),
        "as_of": _ymd_text(row["as_of_ymd"]),
        "event_date": _ymd_text(row["event_ymd"]),
        "event_class": event_class,
        "label": {
            "next_day_primary_gd": event_class in {"primary_gd", "strong_gd"},
            "next_day_strong_gd": event_class == "strong_gd",
            "next_gap_pct": row.get("next_gap_pct"),
            "next_close_ret": row.get("next_close_ret"),
        },
        "as_of_ohlcv": {key: row.get(key) for key in ["o", "h", "l", "c", "v", "source"]},
        "forward": {key: row.get(key) for key in ["next_open", "next_close", "next_low", "next_high", "post_ret5", "post_ret20"]},
        "visual_shape_proxy": {
            key: row.get(key)
            for key in [
                "visual_risk_proxy_score",
                "latest_upper_wick_ratio",
                "latest_close_pos",
                "latest_body_ratio",
                "range60_close_pos",
                "ret5",
                "ret20",
                "ret60",
                "dist_ma7",
                "dist_ma20",
                "dist_ma60",
                "volume_vs_20d_avg",
                "dist_prior_high20",
                "dist_prior_low20",
            ]
        },
        "visual_review_status": "unreviewed",
        "screenshot_required": True,
        "non_scope": ["production_ranking", "runtime_db_write", "MeeMee_display_change"],
    }


def _sample(events: list[dict[str, Any]], *, per_class: int, recent: int) -> list[dict[str, Any]]:
    selected: dict[str, dict[str, Any]] = {}
    for event_class in ["strong_gd", "primary_gd", "control_no_gd"]:
        rows = [row for row in events if row["event_class"] == event_class]
        rows.sort(
            key=lambda row: (
                float(row["visual_shape_proxy"].get("visual_risk_proxy_score") or 0),
                row["event_date"],
                row["code"],
            ),
            reverse=True,
        )
        for row in rows[:per_class]:
            selected[f"{row['code']}:{row['as_of']}"] = {**row, "sample_reason": f"top_visual_proxy_{event_class}"}
    for row in sorted(events, key=lambda row: (row["event_date"], row["code"]), reverse=True)[:recent]:
        selected.setdefault(f"{row['code']}:{row['as_of']}", {**row, "sample_reason": "recent_event_or_control"})
    return list(selected.values())


def _class_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        key = str(row["event_class"])
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


def _metric(rows: list[dict[str, Any]]) -> dict[str, Any]:
    def values(path: tuple[str, str]) -> list[float]:
        out = []
        for row in rows:
            value = row.get(path[0], {}).get(path[1])
            if value is not None:
                out.append(float(value))
        return out

    gaps = values(("label", "next_gap_pct"))
    score = values(("visual_shape_proxy", "visual_risk_proxy_score"))
    return {
        "n": len(rows),
        "class_counts": _class_counts(rows),
        "avg_next_gap_pct": sum(gaps) / len(gaps) if gaps else None,
        "avg_visual_risk_proxy_score": sum(score) / len(score) if score else None,
    }


def run(*, db_path: Path, output_root: Path, per_class: int, recent: int, limit_rows: int | None) -> Path:
    run_dir = output_root / f"{_tag()}-{AXIS_ID}"
    run_dir.mkdir(parents=True, exist_ok=False)
    with duckdb.connect(str(db_path), read_only=True) as conn:
        rows = conn.execute(FEATURE_SQL).fetchdf().to_dict("records")
    events = [_event_row(row) for row in rows]
    if limit_rows is not None:
        events = events[:limit_rows]
    samples = _sample(events, per_class=per_class, recent=recent)
    sample_arg = ",".join(f"{row['code']}:{row['as_of']}" for row in samples)
    screenshot_command = (
        "node scripts\\meemee_detail_clean_screenshot_batch_v1.mjs "
        "--base-url http://127.0.0.1:28888 "
        "--api-base http://127.0.0.1:28888/api "
        f"--output-root {str(run_dir / 'screenshots')} "
        f"--samples {sample_arg} "
        "--viewport 1600x1000 --viewport-fallback"
    )
    _write_jsonl(run_dir / "events_all.jsonl", events)
    _write_jsonl(run_dir / "screenshot_sample_events.jsonl", samples)
    (run_dir / "screenshot_command.txt").write_text(screenshot_command + "\n", encoding="utf-8")
    audit = {
        "schema_version": f"{AXIS_ID}_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "research_phase": "branching_generation",
        "db_path": str(db_path),
        "fixed_evaluation_conditions": {
            "target": "GD occurrence on the next trading day after the screenshot as_of date",
            "primary_gd": "next_open / as_of_close - 1 <= -0.02",
            "strong_gd": "next_open / as_of_close - 1 <= -0.03",
            "control_no_gd": "-0.005 < next_open / as_of_close - 1 < 0.01",
            "screenshot_route": "MeeMee detail-shot clean screenshot with mainAsOf; no centered lookahead",
            "universe": "daily_bars rows with valid next trading day open and 60-day visual proxy history",
            "sample_policy": {"per_class": per_class, "recent": recent},
        },
        "metrics": {
            "all_events": _metric(events),
            "sample_events": _metric(samples),
        },
        "artifacts": {
            "events_all_jsonl": str(run_dir / "events_all.jsonl"),
            "screenshot_sample_events_jsonl": str(run_dir / "screenshot_sample_events.jsonl"),
            "screenshot_command_txt": str(run_dir / "screenshot_command.txt"),
            "audit_json": str(run_dir / "gd_pre_event_visual_shape_audit.json"),
        },
        "screenshot_batch_command": screenshot_command,
        "decision": {
            "candidate_local_decision": "hold_for_meemee_screenshot_generation_and_visual_labeling",
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": "GD-labeled as_of events are extracted; next step is clean MeeMee screenshot capture and visual labeling",
        },
        "runtime_db_write": False,
        "meemee_unchanged": True,
        "production_ranking_changed": False,
        "silent_fallback_used": False,
        "remaining_risks": [
            "earnings and news-driven GD are not separated yet",
            "visual_shape_proxy is diagnostic only; screenshot labels are still unreviewed",
            "control sampling is not yet matched by sector or market regime",
        ],
    }
    _write_json(run_dir / "gd_pre_event_visual_shape_audit.json", audit)
    _write_json(output_root / "latest_gd_pre_event_visual_shape_audit.json", {"run_root": str(run_dir), **audit})
    return run_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--per-class", type=int, default=12)
    parser.add_argument("--recent", type=int, default=24)
    parser.add_argument("--limit-rows", type=int, default=None)
    args = parser.parse_args()
    print(run(db_path=args.db, output_root=args.output_root, per_class=args.per_class, recent=args.recent, limit_rows=args.limit_rows))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
