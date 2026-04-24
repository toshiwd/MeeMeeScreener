from __future__ import annotations

import json
from pathlib import Path

from scripts.tradex_sample_2413_short_compare import run_compare


SOURCE_DB_PATH = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb")


def test_tradex_sample_2413_short_compare_smoke(tmp_path: Path) -> None:
    output_dir = tmp_path / "short_compare"
    result = run_compare(
        source_db_path=SOURCE_DB_PATH,
        output_dir=output_dir,
        symbol="2413",
        start_date="2026-01-01",
        end_date="2026-03-31",
        freeze_date="2025-12-31",
    )

    assert result["ok"] is True
    compare = result["compare"]
    decision = result["decision"]
    paths = result["paths"]

    assert compare["same_condition_contract"]["same_symbol"] is True
    assert compare["same_condition_contract"]["same_period"] is True
    assert compare["same_condition_contract"]["same_source_db"] is True
    assert compare["challenger"]["trade_count"] >= 1
    assert compare["challenger"]["first_short_entry_day"] == "2026-01-20"
    assert compare["challenger"]["short_add_count"] >= 1
    assert compare["challenger"]["downside_capture_ratio"] is not None
    assert compare["challenger"]["downside_capture_ratio"] > compare["champion"]["downside_capture_ratio"]
    assert decision["authoritative_rollup_decision"] == "keep"

    compare_path = Path(paths["compare_json"])
    decision_path = Path(paths["decision"])
    summary_path = Path(paths["policy_v2_roundtrip_summary"])
    assert compare_path.exists()
    assert decision_path.exists()
    assert summary_path.exists()

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["aggregate"]["first_short_entry_day"] == "2026-01-20"
    assert summary["roundtrips"][0]["entry_position_transition"] == "0-0 -> 2-0"
    assert summary["roundtrips"][0]["exit_position_transition"] == "2-0 -> 0-0"
