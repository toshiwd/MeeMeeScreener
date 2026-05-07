from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


def _latest_bundle_root() -> Path:
    root = Path(r"G:\Tradex\long_side_filter_revision_v1")
    candidates = [p for p in root.iterdir() if p.is_dir()]
    if not candidates:
        raise AssertionError("no long-side filter revision bundle found")
    return max(candidates, key=lambda p: p.stat().st_mtime)


def test_latest_bundle_has_restriction_decision() -> None:
    bundle = _latest_bundle_root()
    decision = json.loads((bundle / "long_side_filter_revision_v1_decision.json").read_text(encoding="utf-8"))
    surface = json.loads((bundle / "long_side_filter_revision_surface_comparison.json").read_text(encoding="utf-8"))

    assert decision["decision"] in {
        "ready_to_rebuild_long_surface_with_revised_filter",
        "needs_candidate_generation_refinement",
        "filter_revision_insufficient_stop_high_recall_line",
        "needs_rejected_row_instrumentation",
    }
    assert surface["baseline_long_surface"]["row_count"] == 888
    assert "long_filter_score_040_rank8" in surface["variants"]
    assert "long_filter_score_045_rank5" in surface["variants"]


def test_long_filter_revision_stays_long_only() -> None:
    bundle = _latest_bundle_root()
    reranker = json.loads((bundle / "long_side_filter_revision_reranker_comparison.json").read_text(encoding="utf-8"))
    failure = json.loads((bundle / "long_side_selected_row_failure_audit.json").read_text(encoding="utf-8"))
    rows = pd.read_parquet(bundle / "long_side_filter_revision_rows.parquet")

    assert reranker["baseline_long_surface"]["row_count"] == 888
    assert all(block["variant"].startswith("long_filter_") for block in reranker["variants"].values())
    assert "top5" in failure["selected_by_topk"]
    assert rows.shape[1] > 100
    assert {"tree_hgb_path_value_score", "candidate_pool_tier", "forward_ret_20d"}.issubset(rows.columns)
