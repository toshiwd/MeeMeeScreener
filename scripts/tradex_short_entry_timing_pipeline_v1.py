from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.append(str(Path(__file__).resolve().parent))

from tradex_short_entry_timing_current_scan_v1 import run as run_confirmed_scan
from tradex_short_entry_timing_provisional_scan_v1 import run as run_provisional_scan
from tradex_short_entry_timing_trigger_board_v1 import run as run_trigger_board
from tradex_short_entry_timing_trigger_recheck_v1 import run as run_trigger_recheck
from tradex_short_entry_timing_watch_board_v1 import run as run_watch_board


AXIS_ID = "short_entry_timing_pipeline_v1"
DEFAULT_DB_PATH = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_entry_timing_rule_probe_v1\pipeline")
DEFAULT_CURRENT_ROOT = Path(r"G:\Tradex\short_entry_timing_rule_probe_v1\current_candidates")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _stage_summary(path: Path, artifact_name: str) -> dict[str, Any]:
    artifact_path = path / artifact_name
    payload = _read_json(artifact_path)
    return {
        "run_root": str(path),
        "artifact_path": str(artifact_path),
        "decision": payload.get("decision"),
        "confirmed_as_of": payload.get("confirmed_as_of"),
        "provisional_as_of": payload.get("provisional_as_of"),
        "setup_event_count": payload.get("setup_event_count"),
        "current_candidate_count": payload.get("current_candidate_count"),
        "status_counts": payload.get("status_counts"),
    }


def run(*, db_path: Path, output_root: Path, current_root: Path, include_confirmed_scan: bool) -> Path:
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    stages: dict[str, Any] = {}
    if include_confirmed_scan:
        confirmed_dir = run_confirmed_scan(db_path=db_path, output_root=current_root)
        stages["confirmed_scan"] = _stage_summary(confirmed_dir, "current_entry_timing_candidates.json")
    provisional_dir = run_provisional_scan(db_path=db_path, output_root=current_root)
    provisional_artifact = provisional_dir / "provisional_entry_timing_candidates.json"
    stages["provisional_scan"] = _stage_summary(provisional_dir, "provisional_entry_timing_candidates.json")
    watch_dir = run_watch_board(provisional_scan_path=provisional_artifact, output_root=current_root)
    watch_artifact = watch_dir / "provisional_watch_board.json"
    stages["watch_board"] = _stage_summary(watch_dir, "provisional_watch_board.json")
    trigger_dir = run_trigger_board(
        db_path=db_path,
        watch_board_path=watch_artifact,
        output_root=current_root,
    )
    trigger_artifact = trigger_dir / "provisional_trigger_board.json"
    stages["trigger_board"] = _stage_summary(trigger_dir, "provisional_trigger_board.json")
    recheck_dir = run_trigger_recheck(
        db_path=db_path,
        trigger_board_path=trigger_artifact,
        output_root=current_root,
    )
    stages["trigger_recheck"] = _stage_summary(recheck_dir, "trigger_recheck.json")
    recheck_decision = stages["trigger_recheck"].get("decision") or {}
    report = {
        "schema_version": AXIS_ID,
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "db_path": str(db_path),
        "current_root": str(current_root),
        "stages": stages,
        "decision": {
            "candidate_local_decision": recheck_decision.get("candidate_local_decision", "pipeline_completed"),
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": "pipeline completed provisional scan, trigger board, and trigger recheck in fixed order",
        },
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
    }
    _write_json(output_dir / "short_entry_timing_pipeline.json", report)
    _write_json(output_root / "latest_short_entry_timing_pipeline.json", {"run_root": str(output_dir), **report})
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--current-root", type=Path, default=DEFAULT_CURRENT_ROOT)
    parser.add_argument("--skip-confirmed-scan", action="store_true")
    args = parser.parse_args()
    print(run(
        db_path=args.db_path,
        output_root=args.output_root,
        current_root=args.current_root,
        include_confirmed_scan=not args.skip_confirmed_scan,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
