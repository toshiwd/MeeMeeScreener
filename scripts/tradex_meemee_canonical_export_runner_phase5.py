from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts.tradex_meemee_canonical_export_batch_phase4 import (
    DEFAULT_DB_PATH,
    DEFAULT_EXPORT_ROOT,
    DEFAULT_PHASE3_DIR,
    audit_batch_progress,
    audit_export,
    materialize_batch,
)


AXIS_ID = "meemee_canonical_export_runner_phase5"
DEFAULT_FRONTEND_DIR = Path(__file__).resolve().parents[1] / "app" / "frontend"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _playwright_render(*, batch_dir: Path, export_root: Path, frontend_dir: Path, timeout_seconds: int) -> dict[str, Any]:
    env = os.environ.copy()
    env["TRADEX_RENDER_MANIFEST"] = str(batch_dir / "render_manifest.jsonl")
    env["TRADEX_BROWSER_RENDER_OUTPUT"] = str(export_root)
    env["TRADEX_PLAYWRIGHT_OUTPUT_DIR"] = str(export_root / "playwright-results")
    started = time.monotonic()
    result = subprocess.run(
        ["npx.cmd", "playwright", "test", "e2e/tradex-meemee-render-reference.spec.ts", "--project=chromium"],
        cwd=frontend_dir,
        env=env,
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        timeout=timeout_seconds,
        check=False,
    )
    return {
        "exit_code": result.returncode,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "stdout_tail": result.stdout[-2000:],
        "stderr_tail": result.stderr[-2000:],
    }


def run_batches(
    *,
    phase3_dir: Path,
    export_root: Path,
    db_path: Path,
    frontend_dir: Path,
    batch_size: int,
    max_batches: int,
    timeout_seconds: int,
    render_fn: Callable[..., dict[str, Any]] = _playwright_render,
) -> Path:
    run_dir = export_root / "runs" / f"{_tag()}-{AXIS_ID}"
    run_dir.mkdir(parents=True, exist_ok=False)
    start_audit = audit_export(phase3_dir=phase3_dir, export_root=export_root)
    batches: list[dict[str, Any]] = []
    stop_reason = "max_batches_reached"
    for index in range(max_batches):
        batch_dir = materialize_batch(
            phase3_dir=phase3_dir,
            export_root=export_root,
            db_path=db_path,
            batch_size=batch_size,
        )
        manifest = _read_jsonl(batch_dir / "render_manifest.jsonl")
        if not manifest:
            stop_reason = "no_pending_images"
            break
        render = render_fn(
            batch_dir=batch_dir,
            export_root=export_root,
            frontend_dir=frontend_dir,
            timeout_seconds=timeout_seconds,
        )
        audit = audit_batch_progress(phase3_dir=phase3_dir, export_root=export_root, batch_dir=batch_dir)
        row = {
            "batch_index": index,
            "batch_dir": str(batch_dir),
            "manifest_image_count": len(manifest),
            "render": render,
            "progress_after_batch": {
                "exported_image_count": audit["exported_image_count"],
                "remaining_image_count": audit["remaining_image_count"],
                "current_batch_missing_image_count": audit["current_batch_missing_image_count"],
                "unique_exported_hash_count": None,
            },
        }
        batches.append(row)
        _write_json(run_dir / "phase5_run_progress.json", {"batches": batches})
        if render["exit_code"] != 0:
            stop_reason = "playwright_failure"
            break
        if audit["current_batch_missing_image_count"] != 0:
            stop_reason = "current_batch_missing_images"
            break
        if not audit["resume_pending"]:
            stop_reason = "all_images_exported"
            break
    final_audit = audit_export(phase3_dir=phase3_dir, export_root=export_root)
    clean_batches = bool(batches) and all(
        row["render"]["exit_code"] == 0
        and row["progress_after_batch"]["current_batch_missing_image_count"] == 0
        for row in batches
    )
    report = {
        "schema_version": "tradex_meemee_canonical_export_runner_phase5_audit_v1",
        "generated_at": _utc_now(),
        "boundary_owner": "TRADEX",
        "phase3_dir": str(phase3_dir),
        "export_root": str(export_root),
        "db_path": str(db_path),
        "batch_size": batch_size,
        "max_batches": max_batches,
        "executed_batch_count": len(batches),
        "stop_reason": stop_reason,
        "start_exported_image_count": start_audit["exported_image_count"],
        "end_exported_image_count": final_audit["exported_image_count"],
        "newly_exported_image_count": final_audit["exported_image_count"] - start_audit["exported_image_count"],
        "remaining_image_count": final_audit["remaining_image_count"],
        "unique_exported_hash_count": final_audit["unique_exported_hash_count"],
        "limited_run_batches_clean": clean_batches,
        "ready_for_unattended_full_export": clean_batches and stop_reason in {"max_batches_reached", "all_images_exported"},
        "ready_for_model_training": final_audit["ready_for_model_training"],
        "judgment": "pass_phase5_unattended_full_export_ready"
        if clean_batches and stop_reason in {"max_batches_reached", "all_images_exported"}
        else "hold_phase5_runner_not_ready",
        "batches": batches,
        "non_scope": ["model training", "probability calibration", "production ranking mutation", "runtime DB write", "MeeMee UI mutation"],
    }
    _write_json(run_dir / "phase5_run_audit.json", report)
    _write_json(export_root / "phase5_latest_run_audit.json", report)
    return run_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase3-dir", type=Path, default=DEFAULT_PHASE3_DIR)
    parser.add_argument("--export-root", type=Path, default=DEFAULT_EXPORT_ROOT)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--frontend-dir", type=Path, default=DEFAULT_FRONTEND_DIR)
    parser.add_argument("--batch-size", type=int, default=128)
    parser.add_argument("--max-batches", type=int, default=4)
    parser.add_argument("--timeout-seconds", type=int, default=180)
    args = parser.parse_args()
    print(run_batches(
        phase3_dir=args.phase3_dir,
        export_root=args.export_root,
        db_path=args.db_path,
        frontend_dir=args.frontend_dir,
        batch_size=args.batch_size,
        max_batches=args.max_batches,
        timeout_seconds=args.timeout_seconds,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
