from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


LATEST_ROOT = Path(r"G:\Tradex\exact_candidate_key_repair_v1")


def _latest_bundle() -> Path:
    candidates = [p for p in LATEST_ROOT.iterdir() if p.is_dir()]
    if not candidates:
        raise AssertionError("no exact candidate key repair bundle found")
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _load(bundle: Path, name: str) -> dict:
    return json.loads((bundle / name).read_text(encoding="utf-8"))


def test_latest_bundle_repairs_exact_keys() -> None:
    bundle = _latest_bundle()
    decision = _load(bundle, "exact_candidate_key_repair_v1_decision.json")
    summary = _load(bundle, "candidate_key_repair_summary.json")

    assert decision["decision"] == "exact_candidate_keys_repaired"
    assert decision["status"] == "exact_candidate_keys_repaired"
    assert summary["before"]["exact_key_traceable_count"] == 0
    assert summary["after"]["exact_key_traceable_count"] == 28
    assert summary["before"]["stable_key_traceable_count"] == 17
    assert summary["after"]["stable_key_traceable_count"] == 28
    assert summary["remaining_untraceable_count"] == 0


def test_latest_bundle_emits_repaired_trace_artifacts() -> None:
    bundle = _latest_bundle()
    trace = pd.read_parquet(bundle / "candidate_key_repaired_trace_rows.parquet")
    winner_trace = pd.read_json(bundle / "candidate_key_repaired_top15_loss_trace.json")
    winner_rows = pd.read_parquet(bundle / "candidate_key_repaired_top15_loss_trace_rows.parquet")

    assert "canonical_candidate_key" in trace.columns
    assert "candidate_key_version" in trace.columns
    assert "canonical_key_components" in trace.columns
    assert trace["canonical_candidate_key"].notna().all()
    assert trace["canonical_candidate_key"].nunique() == 2376
    assert winner_rows.shape[0] == 28
    assert winner_rows["canonical_candidate_key"].notna().all()
    assert winner_rows["canonical_candidate_key"].nunique() == 28
    assert winner_rows["exact_traceable_after_repair"].all()
    assert int(winner_trace["exact_key_traceability_after_repair_count"][0]) == 28
