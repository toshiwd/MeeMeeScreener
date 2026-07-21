from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


AXIS_ID = "tradex_ytd_high_credit_phase_image_sample_plan_v1"
DEFAULT_EVENT_JSON = Path(
    r"G:\Tradex\ytd_high_credit_phase_v1\latest_ytd_high_credit_phase_event_eval.json"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\ytd_high_credit_phase_v1")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")


def _bucket(row: dict[str, Any]) -> str | None:
    phase = row.get("ytd_high_credit_phase")
    if row.get("setup_family") != "bottom_lift":
        return None
    ret20 = float(row.get("ret20") or 0)
    min_ret20 = float(row.get("min_ret20") or 0)
    max_ret20 = float(row.get("max_ret20") or 0)
    if phase == "credit_pressure_peak_5_6m":
        if min_ret20 <= -0.10 and ret20 <= 0:
            return "credit_peak_short_success"
        if max_ret20 >= 0.10 and ret20 > 0:
            return "credit_peak_short_failure"
    if phase == "post_pressure_rebound_6_8m":
        if max_ret20 >= 0.10 and ret20 > 0:
            return "post_pressure_rebound_long_success"
        if min_ret20 <= -0.10 and ret20 < 0:
            return "post_pressure_rebound_long_failure"
    return None


def run(*, event_json: Path, output_root: Path, per_bucket: int) -> Path:
    run_dir = output_root / f"{_tag()}-{AXIS_ID}"
    run_dir.mkdir(parents=True, exist_ok=False)
    payload = _load_json(event_json)
    focus_rows = payload.get("focus_samples", [])

    buckets: dict[str, list[dict[str, Any]]] = {}
    for row in focus_rows:
        bucket = _bucket(row)
        if not bucket:
            continue
        buckets.setdefault(bucket, []).append(row)

    samples: list[dict[str, Any]] = []
    seen_keys: set[tuple[str, str, str]] = set()
    for bucket, rows in sorted(buckets.items()):
        ranked = sorted(rows, key=lambda r: (str(r.get("event_date", "")), abs(float(r.get("ret20") or 0))), reverse=True)
        for row in ranked:
            key = (str(row["code"]), str(row["event_date"]), bucket)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            samples.append(
                {
                    "code": str(row["code"]),
                    "as_of": str(row["event_date"]),
                    "bucket": bucket,
                    "ytd_high_credit_phase": row["ytd_high_credit_phase"],
                    "setup_family": row["setup_family"],
                    "days_since_ytd_high": row["days_since_ytd_high"],
                    "entry_close": row["entry_close"],
                    "drawdown_from_ytd_high": row["drawdown_from_ytd_high"],
                    "ret20": row["ret20"],
                    "ret60": row["ret60"],
                    "min_ret20": row["min_ret20"],
                    "max_ret20": row["max_ret20"],
                }
            )
            if sum(1 for item in samples if item["bucket"] == bucket) >= per_bucket:
                break

    manifest = {
        "schema_version": f"{AXIS_ID}_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "source_event_json": str(event_json),
        "selection_policy": {
            "per_bucket": per_bucket,
            "center_chart_on": "event_date",
            "review_requires": ["decision-time image", "after-result image"],
        },
        "bucket_counts": {bucket: len(rows) for bucket, rows in sorted(buckets.items())},
        "sample_count": len(samples),
        "samples_jsonl": str(run_dir / "image_sample_plan.jsonl"),
        "runtime_db_write": False,
        "meemee_unchanged": True,
    }
    _write_jsonl(run_dir / "image_sample_plan.jsonl", samples)
    _write_json(run_dir / "image_sample_plan_audit.json", manifest)
    _write_json(output_root / "latest_ytd_high_credit_phase_image_sample_plan.json", {"run_root": str(run_dir), **manifest})
    return run_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--event-json", type=Path, default=DEFAULT_EVENT_JSON)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--per-bucket", type=int, default=4)
    args = parser.parse_args()
    print(run(event_json=args.event_json, output_root=args.output_root, per_bucket=args.per_bucket))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
