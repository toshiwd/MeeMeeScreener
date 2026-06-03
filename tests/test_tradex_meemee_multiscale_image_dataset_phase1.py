from __future__ import annotations

import json
from pathlib import Path

import duckdb

from scripts.tradex_meemee_multiscale_image_dataset_phase1 import SCALES, build_dataset


def test_phase1_dataset_keeps_future_labels_out_of_render_manifest(tmp_path: Path) -> None:
    db_path = tmp_path / "stocks.duckdb"
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            "CREATE TABLE daily_bars(code VARCHAR, date INTEGER, o DOUBLE, h DOUBLE, l DOUBLE, c DOUBLE, v DOUBLE, source VARCHAR)"
        )
        conn.executemany(
            "INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("1001", 20250000 + index, 100 + index, 102 + index, 99 + index, 101 + index, 1000 + index, "pan")
                for index in range(1, 281)
            ],
        )
        conn.execute("INSERT INTO daily_bars VALUES ('1001', 20250200, 1, 2, 1, 2, 1, 'yahoo')")
    finally:
        conn.close()

    output_dir = build_dataset(
        output_root=tmp_path / "out",
        db_path=db_path,
        codes=("1001",),
        requested_anchors=(20250260,),
    )
    manifest = [json.loads(line) for line in (output_dir / "image_manifest.jsonl").read_text(encoding="utf-8").splitlines()]
    labels = [json.loads(line) for line in (output_dir / "label_ledger.jsonl").read_text(encoding="utf-8").splitlines()]
    audit = json.loads((output_dir / "phase1_audit.json").read_text(encoding="utf-8"))

    assert len(manifest) == len(SCALES)
    assert len(labels) == 1
    assert all(row["bars_payload"][-1][0] == 20250260 for row in manifest)
    assert all("ret20" not in row and "MFE20" not in row and "MAE20" not in row for row in manifest)
    assert labels[0]["ret20"] is not None
    assert labels[0]["label_start_as_of"] == 20250261
    assert labels[0]["label_end_as_of"] == 20250280
    assert labels[0]["labels_used_in_image_rendering"] is False
    assert audit["point_in_time_payload_passed"] is True
    assert audit["label_isolation_passed"] is True
    assert audit["judgment"] == "hold_for_canonical_browser_export"
