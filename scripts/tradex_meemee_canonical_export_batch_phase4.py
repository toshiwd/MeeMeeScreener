from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


AXIS_ID = "meemee_canonical_export_batch_phase4"
DEFAULT_PHASE3_DIR = Path(r"G:\Tradex\meemee_multiscale_dataset_scale_phase3\20260601T080600Z-meemee_multiscale_dataset_scale_phase3")
DEFAULT_EXPORT_ROOT = Path(r"G:\Tradex\meemee_canonical_export_phase4")
DEFAULT_DB_PATH = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_BATCH_SIZE = 32


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _fetch_history(conn: duckdb.DuckDBPyConnection, *, code: str, as_of: int) -> list[list[Any]]:
    rows = conn.execute(
        """
        WITH normalized AS (
            SELECT
                CASE
                    WHEN date BETWEEN 19000101 AND 20991231 THEN CAST(date AS INTEGER)
                    WHEN date >= 1000000000000 THEN CAST(strftime(to_timestamp(date / 1000), '%Y%m%d') AS INTEGER)
                    WHEN date >= 1000000000 THEN CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)
                    ELSE NULL
                END AS ymd,
                o, h, l, c, v
            FROM daily_bars
            WHERE code = ?
              AND COALESCE(source, 'pan') <> 'yahoo'
        )
        SELECT ymd, o, h, l, c, v
        FROM normalized
        WHERE ymd IS NOT NULL AND ymd <= ?
          AND o IS NOT NULL AND h IS NOT NULL AND l IS NOT NULL AND c IS NOT NULL
        ORDER BY ymd DESC
        LIMIT 260
        """,
        [code, as_of],
    ).fetchall()[::-1]
    return [list(row) for row in rows]


def materialize_batch(*, phase3_dir: Path, export_root: Path, db_path: Path, batch_size: int) -> Path:
    export_root.mkdir(parents=True, exist_ok=True)
    image_root = export_root / "browser_reference_images"
    image_root.mkdir(exist_ok=True)
    plan = _read_jsonl(phase3_dir / "canonical_image_export_plan.jsonl")
    pending = [row for row in plan if not (export_root / row["image_relpath"]).exists()]
    selected = pending[:batch_size]
    conn = duckdb.connect(str(db_path), read_only=True)
    history_cache: dict[tuple[str, int], list[list[Any]]] = {}
    rows: list[dict[str, Any]] = []
    try:
        for row in selected:
            key = (str(row["code"]), int(row["as_of"]))
            if key not in history_cache:
                history_cache[key] = _fetch_history(conn, code=key[0], as_of=key[1])
            rows.append({**row, "bars_payload": history_cache[key]})
    finally:
        conn.close()
    batch_dir = export_root / "batches" / f"{_tag()}-{AXIS_ID}"
    batch_dir.mkdir(parents=True, exist_ok=False)
    _write_jsonl(batch_dir / "render_manifest.jsonl", rows)
    _write_json(batch_dir / "batch_contract.json", {
        "schema_version": "tradex_meemee_canonical_export_batch_phase4_contract_v1",
        "boundary_owner": "TRADEX",
        "phase3_dir": str(phase3_dir),
        "export_root": str(export_root),
        "db_path": str(db_path),
        "batch_size_requested": batch_size,
        "batch_image_count": len(rows),
        "unique_code_as_of_fetch_count": len(history_cache),
        "resume_policy": "select only plan rows whose canonical image path does not exist",
        "source_policy": "confirmed_non_yahoo_daily_bars_only",
        "non_scope": ["model training", "production ranking mutation", "runtime DB write", "MeeMee UI mutation"],
    })
    return batch_dir


def audit_export(*, phase3_dir: Path, export_root: Path, batch_dir: Path | None = None) -> dict[str, Any]:
    plan = _read_jsonl(phase3_dir / "canonical_image_export_plan.jsonl")
    rows: list[dict[str, Any]] = []
    for row in plan:
        path = export_root / row["image_relpath"]
        if path.exists():
            rows.append({**row, "sha256": _sha256(path)})
    hashes = [row["sha256"] for row in rows]
    exported = len(rows)
    expected = len(plan)
    batch_rows = _read_jsonl(batch_dir / "render_manifest.jsonl") if batch_dir else []
    batch_missing = [
        row["image_relpath"] for row in batch_rows
        if not (export_root / row["image_relpath"]).exists()
    ]
    resumable = exported < expected
    report = {
        "schema_version": "tradex_meemee_canonical_export_phase4_progress_audit_v1",
        "boundary_owner": "TRADEX",
        "phase3_dir": str(phase3_dir),
        "export_root": str(export_root),
        "planned_image_count": expected,
        "exported_image_count": exported,
        "remaining_image_count": expected - exported,
        "export_progress_ratio": round(exported / expected, 8) if expected else 0.0,
        "unique_exported_hash_count": len(set(hashes)),
        "current_batch_image_count": len(batch_rows),
        "current_batch_missing_image_count": len(batch_missing),
        "current_batch_missing_images": batch_missing,
        "resume_pending": resumable,
        "ready_for_full_canonical_export": bool(exported and not batch_missing),
        "ready_for_model_training": exported == expected and expected > 0,
        "judgment": "pass_phase4_batch_export_resume_ready" if exported and not batch_missing else "hold_phase4_batch_export_incomplete",
        "non_scope": ["model training", "probability calibration", "production ranking mutation", "runtime DB write", "MeeMee UI mutation"],
    }
    _write_json(export_root / "canonical_export_progress_audit.json", report)
    _write_json(export_root / "phase4_audit.json", {
        "schema_version": "tradex_meemee_canonical_export_phase4_audit_v1",
        "generated_at": _utc_now(),
        "authoritative_progress": report,
        "judgment": report["judgment"],
    })
    return report


def audit_batch_progress(*, phase3_dir: Path, export_root: Path, batch_dir: Path) -> dict[str, Any]:
    plan = _read_jsonl(phase3_dir / "canonical_image_export_plan.jsonl")
    exported = sum(1 for row in plan if (export_root / row["image_relpath"]).exists())
    batch_rows = _read_jsonl(batch_dir / "render_manifest.jsonl")
    batch_missing = [
        row["image_relpath"] for row in batch_rows
        if not (export_root / row["image_relpath"]).exists()
    ]
    expected = len(plan)
    return {
        "exported_image_count": exported,
        "remaining_image_count": expected - exported,
        "current_batch_missing_image_count": len(batch_missing),
        "current_batch_missing_images": batch_missing,
        "resume_pending": exported < expected,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    prepare = subparsers.add_parser("prepare")
    prepare.add_argument("--phase3-dir", type=Path, default=DEFAULT_PHASE3_DIR)
    prepare.add_argument("--export-root", type=Path, default=DEFAULT_EXPORT_ROOT)
    prepare.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    prepare.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    audit = subparsers.add_parser("audit")
    audit.add_argument("--phase3-dir", type=Path, default=DEFAULT_PHASE3_DIR)
    audit.add_argument("--export-root", type=Path, default=DEFAULT_EXPORT_ROOT)
    audit.add_argument("--batch-dir", type=Path, default=None)
    args = parser.parse_args()
    if args.command == "prepare":
        print(materialize_batch(phase3_dir=args.phase3_dir, export_root=args.export_root, db_path=args.db_path, batch_size=args.batch_size))
    else:
        print(json.dumps(audit_export(phase3_dir=args.phase3_dir, export_root=args.export_root, batch_dir=args.batch_dir), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
