from __future__ import annotations

import json
from pathlib import Path


def _latest_bundle_root() -> Path:
    root = Path(r"G:\Tradex\long_side_reranker_validation_v1")
    candidates = [p for p in root.iterdir() if p.is_dir()]
    if not candidates:
        raise AssertionError("no long-side reranker bundle found")
    return max(candidates, key=lambda p: p.stat().st_mtime)


def test_latest_bundle_is_long_side_only() -> None:
    bundle = _latest_bundle_root()
    decision = json.loads((bundle / "long_side_reranker_validation_v1_decision.json").read_text(encoding="utf-8"))
    validation = json.loads((bundle / "long_side_reranker_input_validation.json").read_text(encoding="utf-8"))

    assert decision["status"] in {
        "ready_to_design_long_side_reranker_challenger",
        "hold_needs_long_side_filter_revision",
        "drop_long_side_high_recall_surface",
        "needs_candidate_generation_refinement",
        "stop_high_recall_line",
    }
    assert validation["only_long_rows_used"] is True
    assert validation["short_rows_in_active_validation"] == 0
    assert validation["frozen_features_present"] is True


def test_bundle_preserves_champion_fields() -> None:
    bundle = _latest_bundle_root()
    validation = json.loads((bundle / "long_side_reranker_input_validation.json").read_text(encoding="utf-8"))
    assert validation["champion_fields_preserved_separately"] is True
    assert validation["champion_score_preserved"] is True
    assert validation["champion_rank_preserved"] is True
