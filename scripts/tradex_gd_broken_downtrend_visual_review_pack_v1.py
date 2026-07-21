from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


AXIS_ID = "tradex_gd_broken_downtrend_visual_review_pack_v1"
DEFAULT_SOURCE_RUN = Path(
    r"G:\Tradex\gd_pre_event_visual_shape_dataset_v1"
    r"\20260708T020714Z-tradex_gd_pre_event_visual_shape_dataset_v1"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\gd_broken_downtrend_visual_review_pack_v1")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if text:
                rows.append(json.loads(text))
    return rows


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, ensure_ascii=False, default=str) + "\n" for row in rows), encoding="utf-8")


def _as_float(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _shape_bucket(row: dict[str, Any]) -> str:
    f = row.get("visual_shape_proxy") or {}
    ret20 = _as_float(f.get("ret20")) or 0.0
    dist_ma20 = _as_float(f.get("dist_ma20")) or 0.0
    dist_ma60 = _as_float(f.get("dist_ma60")) or 0.0
    if ret20 <= -0.08 and dist_ma20 < -0.03 and dist_ma60 < -0.05:
        return "already_broken_downtrend"
    return "other"


def _review_row(row: dict[str, Any], *, sample_reason: str) -> dict[str, Any]:
    f = row.get("visual_shape_proxy") or {}
    return {
        "schema_version": f"{AXIS_ID}_row_v1",
        "code": row.get("code"),
        "as_of": row.get("as_of"),
        "event_date": row.get("event_date"),
        "event_class": row.get("event_class"),
        "next_gap_pct": row.get("label", {}).get("next_gap_pct"),
        "next_day_primary_gd": row.get("label", {}).get("next_day_primary_gd"),
        "sample_reason": sample_reason,
        "visual_review_status": "unreviewed",
        "human_visual_labels": [],
        "proxy_features": {
            "ret20": f.get("ret20"),
            "ret60": f.get("ret60"),
            "dist_ma20": f.get("dist_ma20"),
            "dist_ma60": f.get("dist_ma60"),
            "latest_upper_wick_ratio": f.get("latest_upper_wick_ratio"),
            "latest_close_pos": f.get("latest_close_pos"),
            "volume_vs_20d_avg": f.get("volume_vs_20d_avg"),
            "dist_prior_low20": f.get("dist_prior_low20"),
        },
        "non_scope": ["production_ranking", "runtime_db_write", "MeeMee_display_change"],
    }


def _select(rows: list[dict[str, Any]], *, per_group: int, recent: int) -> list[dict[str, Any]]:
    bucket_rows = [row for row in rows if _shape_bucket(row) == "already_broken_downtrend"]
    gd = [
        row
        for row in bucket_rows
        if row.get("label", {}).get("next_day_primary_gd")
        and (_as_float(row.get("label", {}).get("next_gap_pct")) or 0.0) >= -0.12
    ]
    control = [row for row in bucket_rows if not row.get("label", {}).get("next_day_primary_gd")]

    gd.sort(
        key=lambda row: (
            row.get("event_date") or "",
            _as_float(row.get("label", {}).get("next_gap_pct")) or 0.0,
            row.get("code") or "",
        ),
        reverse=True,
    )
    control.sort(
        key=lambda row: (
            row.get("event_date") or "",
            _as_float((row.get("visual_shape_proxy") or {}).get("ret20")) or 0.0,
            row.get("code") or "",
        ),
        reverse=True,
    )

    selected: dict[str, dict[str, Any]] = {}
    for row in gd[:per_group]:
        selected[f"{row['code']}:{row['as_of']}"] = _review_row(row, sample_reason="recent_practical_gd_already_broken_downtrend")
    for row in control[:per_group]:
        selected[f"{row['code']}:{row['as_of']}"] = _review_row(row, sample_reason="recent_control_already_broken_downtrend")
    recent_rows = sorted(bucket_rows, key=lambda row: (row.get("event_date") or "", row.get("code") or ""), reverse=True)
    for row in recent_rows[:recent]:
        selected.setdefault(f"{row['code']}:{row['as_of']}", _review_row(row, sample_reason="recent_already_broken_downtrend"))
    return list(selected.values())


def run(*, source_run: Path, output_root: Path, per_group: int, recent: int) -> Path:
    events_path = source_run / "events_all.jsonl"
    rows = _read_jsonl(events_path)
    selected = _select(rows, per_group=per_group, recent=recent)
    run_dir = output_root / f"{_tag()}-{AXIS_ID}"
    run_dir.mkdir(parents=True, exist_ok=False)
    _write_jsonl(run_dir / "visual_review_samples.jsonl", selected)
    sample_arg = ",".join(f"{row['code']}:{row['as_of']}" for row in selected)
    screenshot_command = (
        "node scripts\\meemee_detail_clean_screenshot_batch_v1.mjs "
        "--base-url http://127.0.0.1:28888 "
        "--api-base http://127.0.0.1:28888/api "
        f"--output-root {str(run_dir / 'screenshots')} "
        f"--samples {sample_arg} "
        "--viewport 1600x1000 --viewport-fallback"
    )
    (run_dir / "screenshot_command.txt").write_text(screenshot_command + "\n", encoding="utf-8")
    gd_count = sum(1 for row in selected if row.get("next_day_primary_gd"))
    audit = {
        "schema_version": f"{AXIS_ID}_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "research_phase": "effectiveness_judgment",
        "source_events_all_jsonl": str(events_path),
        "target_shape_bucket": "already_broken_downtrend",
        "sample_count": len(selected),
        "gd_sample_count": gd_count,
        "control_sample_count": len(selected) - gd_count,
        "artifacts": {
            "visual_review_samples_jsonl": str(run_dir / "visual_review_samples.jsonl"),
            "screenshot_command_txt": str(run_dir / "screenshot_command.txt"),
            "audit_json": str(run_dir / "broken_downtrend_visual_review_pack_audit.json"),
        },
        "screenshot_batch_command": screenshot_command,
        "decision": {
            "candidate_local_decision": "hold_for_screenshot_review",
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": "already_broken_downtrend has elevated GD rate; visual samples are prepared for direct MeeMee screenshot review",
        },
        "runtime_db_write": False,
        "meemee_unchanged": True,
        "production_ranking_changed": False,
    }
    _write_json(run_dir / "broken_downtrend_visual_review_pack_audit.json", audit)
    _write_json(output_root / "latest_broken_downtrend_visual_review_pack_audit.json", {"run_root": str(run_dir), **audit})
    return run_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-run", type=Path, default=DEFAULT_SOURCE_RUN)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--per-group", type=int, default=12)
    parser.add_argument("--recent", type=int, default=12)
    args = parser.parse_args()
    print(run(source_run=args.source_run, output_root=args.output_root, per_group=args.per_group, recent=args.recent))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
