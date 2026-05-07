from __future__ import annotations

import json
from pathlib import Path

from scripts.tradex_feature_surface_batch2_source_discovery_v1 import run_batch2_source_discovery


def test_batch2_source_discovery_writes_required_artifacts(tmp_path: Path) -> None:
    result = run_batch2_source_discovery(output_root=tmp_path / "batch2_source_discovery")
    session_dir = Path(result["output_dir"])

    required = {
        "run_manifest.json",
        "input_resolution.json",
        "event_source_inventory.json",
        "dividend_rights_source_inventory.json",
        "corporate_action_source_inventory.json",
        "volume_participation_source_audit.json",
        "batch2_feature_feasibility_matrix.json",
        "batch2_source_gap_summary.json",
        "batch2_implementation_recommendation.json",
        "feature_surface_batch2_source_discovery_v1_decision.json",
        "_ARTIFACT_COMPLETE.json",
        "source_table_inventory.parquet",
        "volume_missing_rows_sample.parquet",
    }
    assert required.issubset({path.name for path in session_dir.iterdir()})

    decision = json.loads((session_dir / "feature_surface_batch2_source_discovery_v1_decision.json").read_text(encoding="utf-8"))
    volume = json.loads((session_dir / "volume_participation_source_audit.json").read_text(encoding="utf-8"))
    feasibility = json.loads((session_dir / "batch2_feature_feasibility_matrix.json").read_text(encoding="utf-8"))
    recommendation = json.loads((session_dir / "batch2_implementation_recommendation.json").read_text(encoding="utf-8"))
    event_inventory = json.loads((session_dir / "event_source_inventory.json").read_text(encoding="utf-8"))
    corp_inventory = json.loads((session_dir / "corporate_action_source_inventory.json").read_text(encoding="utf-8"))

    assert decision["decision"] == "ready_to_implement_batch2_volume_participation"
    assert volume["repair_diagnosis"]["source_gap_type"] == "surface_join_gap_not_source_absence"
    assert volume["daily_bars_repair"]["matched_rows_with_volume"] == volume["vol_ratio5_20_missing_count"]
    assert volume["ml_feature_daily_repair"]["matched_rows_with_vol_ratio"] == volume["vol_ratio5_20_missing_count"]
    assert len(feasibility) >= 5
    assert recommendation["recommended_path"] == "ready_to_implement_batch2_volume_participation"
    assert any(item["source_table"] == "earnings_planned" for item in event_inventory["sources"])
    assert any(item["source_table"] == "ex_rights" for item in event_inventory["sources"])
    assert any(item["source_table"] == "tdnet_disclosures" for item in corp_inventory["sources"])
    assert any(item["source_table"] == "edinetdb_analysis" for item in corp_inventory["sources"])
