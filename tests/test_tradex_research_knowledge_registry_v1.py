import json
from pathlib import Path

import pandas as pd

from scripts.tradex_research_knowledge_registry_v1 import artifact_row, build_registry, run


def _write(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_unknown_decision_is_not_guessed_and_hash_is_stable(tmp_path: Path) -> None:
    path = _write(tmp_path / "x.json", {"schema_version": "x", "axis_id": "a", "decision": {"authoritative_rollup_decision": "novel_winner"}})
    payload = json.loads(path.read_text())
    first = artifact_row(path, payload)
    second = artifact_row(path, payload)
    assert first["raw_decision"] == "novel_winner"
    assert first["normalized_decision"] == "unknown"
    assert first["artifact_sha256"] == second["artifact_sha256"]
    assert first["semantic_fingerprint"] == "incomplete"


def test_one_artifact_produces_exactly_one_decision_row(tmp_path: Path) -> None:
    path = _write(tmp_path / "compare.json", {"axis_id": "a", "authoritative_rollup_decision": "hold", "decision": {"candidate_local_decision": "drop"}})
    frame, gaps = build_registry([path])
    assert len(frame) == 1
    assert frame.iloc[0]["raw_decision"] == "hold"
    assert gaps == []


def test_nested_top_level_rollup_decision_is_preserved(tmp_path: Path) -> None:
    path = _write(tmp_path / "compare.json", {"authoritative_rollup_decision": {"candidate_local_decision": "drop"}})
    frame, _ = build_registry([path])
    assert len(frame) == 1
    assert frame.iloc[0]["raw_decision"] == "drop"
    assert frame.iloc[0]["decision_source"] == "authoritative_rollup_decision.candidate_local_decision"


def test_run_writes_four_artifacts_without_mutating_source(tmp_path: Path) -> None:
    local = tmp_path / "inventory"
    source = _write(local / "decision.json", {"schema_version": "v1", "axis_id": "f", "artifact_role": "authoritative", "decision": "drop", "runtime_db_write": False})
    before = source.read_bytes()
    root = run(tmp_path / "out", local, [])
    assert source.read_bytes() == before
    assert {p.name for p in root.iterdir()} == {"research_registry.parquet", "registry_manifest.json", "gap_report.json", "RESEARCH_MAP.md"}
    frame = pd.read_parquet(root / "research_registry.parquet")
    assert len(frame) == 1 and frame.iloc[0]["normalized_decision"] == "drop"
    manifest = json.loads((root / "registry_manifest.json").read_text())
    assert manifest["collection_policy"] == "explicit_seed_roots_only_no_g_drive_scan"
