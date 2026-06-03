from __future__ import annotations

import json
from pathlib import Path

import duckdb
from PIL import Image

from scripts.tradex_meemee_canonical_export_runner_phase5 import run_batches
from scripts.tradex_meemee_canonical_export_runner_phase5 import _playwright_render


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def test_phase5_runner_exports_limited_batches_and_stops_at_explicit_cap(tmp_path: Path) -> None:
    phase3 = tmp_path / "phase3"
    phase3.mkdir()
    plan = [
        {
            "image_sample_key": "sample",
            "code": "1001",
            "as_of": 20250110,
            "scale": f"scale{index}",
            "bars": 30,
            "image_relpath": f"browser_reference_images/sample_{index}.png",
        }
        for index in range(5)
    ]
    _write_jsonl(phase3 / "canonical_image_export_plan.jsonl", plan)
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

    def render_fn(**kwargs):
        batch_dir = kwargs["batch_dir"]
        export_root = kwargs["export_root"]
        rows = [json.loads(line) for line in (batch_dir / "render_manifest.jsonl").read_text(encoding="utf-8").splitlines()]
        for row in rows:
            path = export_root / row["image_relpath"]
            path.parent.mkdir(parents=True, exist_ok=True)
            Image.new("RGB", (4, 4), "white").save(path)
        return {"exit_code": 0, "elapsed_seconds": 0.01, "stdout_tail": "", "stderr_tail": ""}

    run_dir = run_batches(
        phase3_dir=phase3,
        export_root=tmp_path / "export",
        db_path=db,
        frontend_dir=tmp_path,
        batch_size=2,
        max_batches=2,
        timeout_seconds=1,
        render_fn=render_fn,
    )
    audit = json.loads((run_dir / "phase5_run_audit.json").read_text(encoding="utf-8"))
    assert audit["executed_batch_count"] == 2
    assert audit["newly_exported_image_count"] == 4
    assert audit["remaining_image_count"] == 1
    assert audit["unique_exported_hash_count"] == 1
    assert audit["stop_reason"] == "max_batches_reached"
    assert audit["limited_run_batches_clean"] is True
    assert audit["ready_for_unattended_full_export"] is True
    assert audit["ready_for_model_training"] is False
    assert audit["judgment"] == "pass_phase5_unattended_full_export_ready"


def test_phase5_playwright_output_is_redirected_outside_repo(monkeypatch, tmp_path: Path) -> None:
    captured = {}

    def fake_run(*args, **kwargs):
        captured["env"] = kwargs["env"]
        return type("Result", (), {"returncode": 0, "stdout": "", "stderr": ""})()

    monkeypatch.setattr("subprocess.run", fake_run)
    batch = tmp_path / "batch"
    batch.mkdir()
    _playwright_render(batch_dir=batch, export_root=tmp_path / "export", frontend_dir=tmp_path, timeout_seconds=1)
    assert captured["env"]["TRADEX_PLAYWRIGHT_OUTPUT_DIR"] == str(tmp_path / "export" / "playwright-results")
