from __future__ import annotations

import json
from pathlib import Path

import duckdb

from scripts.tradex_meemee_equivalent_chart_render_phase0 import SCALES, run


def test_phase0_renderer_writes_multiscale_deterministic_audit(tmp_path: Path) -> None:
    db_path = tmp_path / "stocks.duckdb"
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            "CREATE TABLE daily_bars(code VARCHAR, date INTEGER, o DOUBLE, h DOUBLE, l DOUBLE, c DOUBLE, v DOUBLE, source VARCHAR)"
        )
        conn.executemany(
            "INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("1001", 20260101 + index, 100 + index, 102 + index, 99 + index, 101 + index, 1000 + index, "pan")
                for index in range(10)
            ],
        )
    finally:
        conn.close()

    output_dir = run(output_root=tmp_path / "out", db_path=db_path, as_of=20261231, codes=("1001",))

    audit = json.loads((output_dir / "phase0_audit.json").read_text(encoding="utf-8"))
    determinism = json.loads((output_dir / "renderer_determinism_report.json").read_text(encoding="utf-8"))
    manifest = (output_dir / "image_manifest.jsonl").read_text(encoding="utf-8").splitlines()

    assert audit["boundary_owner"] == "TRADEX"
    assert audit["generated_image_count"] == len(SCALES)
    assert audit["judgment"] == "hold_for_playwright_pixel_comparison"
    assert determinism["renderer_deterministic"] is True
    assert len(manifest) == len(SCALES)
    assert len(list((output_dir / "images").glob("*.png"))) == len(SCALES)
