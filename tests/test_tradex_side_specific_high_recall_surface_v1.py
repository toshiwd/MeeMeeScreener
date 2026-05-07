from __future__ import annotations

import json
from pathlib import Path


def _latest_bundle_root() -> Path:
    root = Path(r"G:\Tradex\side_specific_high_recall_surface_v1")
    candidates = [p for p in root.iterdir() if p.is_dir()]
    if not candidates:
        raise AssertionError("no side-specific surface bundle found")
    return max(candidates, key=lambda p: p.stat().st_mtime)


def test_latest_bundle_is_side_specific_surface() -> None:
    bundle = _latest_bundle_root()
    decision = json.loads((bundle / "side_specific_high_recall_surface_v1_decision.json").read_text(encoding="utf-8"))
    surface = json.loads((bundle / "side_specific_surface_summary.json").read_text(encoding="utf-8"))

    assert decision["decision"] == "ready_for_long_side_reranker_validation"
    assert decision["supporting_checks"]["long_feature_complete"] is True
    assert decision["supporting_checks"]["short_active_validation_allowed"] is False
    assert surface["long_decision"] == "active"
    assert surface["short_decision"] == "research_hold"
    assert surface["combined_decision"] == "diagnostic_only"


def test_long_side_surface_really_separates_short_hold() -> None:
    bundle = _latest_bundle_root()
    long_summary = json.loads((bundle / "long_side_active_surface_summary.json").read_text(encoding="utf-8"))
    short_summary = json.loads((bundle / "short_side_research_hold_summary.json").read_text(encoding="utf-8"))

    assert long_summary["active_validation_allowed"] is True
    assert short_summary["active_validation_allowed"] is False
    assert long_summary["role"] == "long_active"
    assert short_summary["role"] == "short_research_hold"
