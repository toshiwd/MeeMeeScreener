from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.tradex_short_blowoff_current_scan_v1 import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_CURRENT_ROOT,
    run as run_current_scan,
)
from scripts.tradex_short_blowoff_watch_board_v1 import run as run_watch_board
from scripts.tradex_short_watch_to_entry_retest_probe_v1 import _default_db_path


AXIS_ID = "short_blowoff_pipeline_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_watch_to_entry_retest_probe_v1\pipeline")
DEFAULT_SCREENSHOT_OUTPUT_ROOT = Path(r"G:\Tradex\short_watch_to_entry_retest_probe_v1\current_scan\screenshots")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _stage_summary(path: Path, artifact_name: str) -> dict[str, Any]:
    artifact_path = path / artifact_name
    payload = _read_json(artifact_path)
    return {
        "run_root": str(path),
        "artifact_path": str(artifact_path),
        "confirmed_as_of": payload.get("confirmed_as_of"),
        "decision": payload.get("decision"),
        "current_candidate_count": payload.get("current_candidate_count"),
        "row_count": payload.get("row_count"),
        "blocked_near_miss_count": payload.get("blocked_near_miss_count"),
        "production_ranking_changed": payload.get("production_ranking_changed"),
        "runtime_db_write": payload.get("runtime_db_write"),
        "meemee_unchanged": payload.get("meemee_unchanged"),
    }


def _screenshot_plan(watch_board_path: Path, screenshot_output_root: Path) -> dict[str, Any]:
    board = _read_json(watch_board_path)
    rows = board.get("rows", [])
    samples = [
        f"{row['code']}:{row['entry_date']}"
        for row in rows
        if row.get("code") and row.get("entry_date")
    ]
    command = None
    if samples:
        command = (
            "node scripts/meemee_detail_clean_screenshot_batch_v1.mjs "
            f"--samples {','.join(samples)} "
            f"--output-root {screenshot_output_root} "
            "--centered --center-lookback-months 8 --center-lookahead-months 3 --viewport-fallback"
        )
    return {
        "candidate_count": len(rows),
        "samples": samples,
        "screenshot_command": command,
        "screenshot_output_root": str(screenshot_output_root),
        "requires_meemee_frontend": True,
        "requires_meemee_backend": True,
        "execution_policy": "manual_or_explicit_pipeline_extension_only",
    }


def run(
    *,
    db_path: Path,
    output_root: Path,
    current_root: Path,
    screenshot_output_root: Path,
    start: str,
    end: str,
) -> Path:
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)

    scan_dir = run_current_scan(db_path=db_path, output_root=current_root, start=start, end=end)
    scan_artifact = scan_dir / "current_short_blowoff_candidates.json"
    watch_dir = run_watch_board(current_scan_path=scan_artifact, output_root=current_root)
    watch_artifact = watch_dir / "short_blowoff_watch_board.json"

    stages = {
        "current_scan": _stage_summary(scan_dir, "current_short_blowoff_candidates.json"),
        "watch_board": _stage_summary(watch_dir, "short_blowoff_watch_board.json"),
    }
    watch_decision = stages["watch_board"].get("decision") or {}
    report = {
        "schema_version": f"{AXIS_ID}_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "db_path": str(db_path),
        "current_root": str(current_root),
        "stages": stages,
        "screenshot_plan": _screenshot_plan(watch_artifact, screenshot_output_root),
        "decision": {
            "candidate_local_decision": watch_decision.get("candidate_local_decision", "pipeline_completed"),
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": "pipeline completed high-zone blowoff current scan and watch board in fixed order",
        },
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
    }
    _write_json(output_dir / "short_blowoff_pipeline.json", report)
    _write_json(output_root / "latest_short_blowoff_pipeline.json", {"run_root": str(output_dir), **report})
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=_default_db_path())
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--current-root", type=Path, default=DEFAULT_CURRENT_ROOT)
    parser.add_argument("--screenshot-output-root", type=Path, default=DEFAULT_SCREENSHOT_OUTPUT_ROOT)
    parser.add_argument("--start", default="2018-01-01")
    parser.add_argument("--end", default="latest")
    args = parser.parse_args()
    print(
        run(
            db_path=args.db_path,
            output_root=args.output_root,
            current_root=args.current_root,
            screenshot_output_root=args.screenshot_output_root,
            start=args.start,
            end=args.end,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
