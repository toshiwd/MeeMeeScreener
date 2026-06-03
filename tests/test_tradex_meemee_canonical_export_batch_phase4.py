from __future__ import annotations

import json
from pathlib import Path

import duckdb
from PIL import Image

from scripts.tradex_meemee_canonical_export_batch_phase4 import audit_batch_progress, audit_export, materialize_batch


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def test_phase4_materialization_skips_existing_images_and_audits_resume(tmp_path: Path) -> None:
    phase3 = tmp_path / "phase3"
    phase3.mkdir()
    plan = [
        {
            "image_sample_key": "sample",
            "code": "1001",
            "as_of": 20250110,
            "scale": scale,
            "bars": bars,
            "image_relpath": f"browser_reference_images/sample_{scale}.png",
        }
        for scale, bars in (("micro", 30), ("short", 60), ("structure", 120), ("macro", 240))
    ]
    _write_jsonl(phase3 / "canonical_image_export_plan.jsonl", plan)
    export = tmp_path / "export"
    (export / "browser_reference_images").mkdir(parents=True)
    Image.new("RGB", (4, 4), "white").save(export / plan[0]["image_relpath"])
    db = tmp_path / "stocks.duckdb"
    conn = duckdb.connect(str(db))
    try:
        conn.execute("CREATE TABLE daily_bars(code VARCHAR, date INTEGER, o DOUBLE, h DOUBLE, l DOUBLE, c DOUBLE, v DOUBLE, source VARCHAR)")
        conn.executemany(
            "INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [("1001", 20250000 + index, 100, 102, 99, 101, 1000, "pan") for index in range(1, 11)],
        )
    finally:
        conn.close()

    batch = materialize_batch(phase3_dir=phase3, export_root=export, db_path=db, batch_size=2)
    rows = [json.loads(line) for line in (batch / "render_manifest.jsonl").read_text(encoding="utf-8").splitlines()]
    assert len(rows) == 2
    assert all(row["scale"] != "micro" for row in rows)
    assert all(row["bars_payload"] for row in rows)

    for row in rows:
        Image.new("RGB", (4, 4), row["scale"] == "short" and "red" or "blue").save(export / row["image_relpath"])
    report = audit_export(phase3_dir=phase3, export_root=export, batch_dir=batch)
    assert report["exported_image_count"] == 3
    assert report["remaining_image_count"] == 1
    assert report["current_batch_missing_image_count"] == 0
    assert report["resume_pending"] is True
    assert report["ready_for_full_canonical_export"] is True
    assert report["ready_for_model_training"] is False

    progress = audit_batch_progress(phase3_dir=phase3, export_root=export, batch_dir=batch)
    assert progress["exported_image_count"] == 3
    assert progress["remaining_image_count"] == 1
    assert progress["current_batch_missing_image_count"] == 0
    assert "unique_exported_hash_count" not in progress
