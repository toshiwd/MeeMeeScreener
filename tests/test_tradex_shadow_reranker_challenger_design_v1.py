from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.tradex_shadow_reranker_challenger_design_v1 import run_shadow_reranker_challenger_design_v1


def test_shadow_reranker_challenger_design_smoke_writes_required_artifacts(tmp_path: Path) -> None:
    result = run_shadow_reranker_challenger_design_v1(
        output_root=tmp_path / "shadow_reranker_challenger_design_v1",
        limit_anchor_dates=2,
        jobs=2,
    )

    session_dir = Path(result["output_dir"])
    required = {
        "run_manifest.json",
        "input_resolution.json",
        "shadow_challenger_model_spec.json",
        "shadow_challenger_variant_pool_comparison.json",
        "shadow_challenger_topk_membership_diff.parquet",
        "shadow_challenger_robustness_audit.json",
        "shadow_challenger_leakage_audit.json",
        "shadow_challenger_feature_effect_summary.json",
        "shadow_reranker_challenger_design_v1_decision.json",
        "_ARTIFACT_COMPLETE.json",
    }
    names = {path.name for path in session_dir.iterdir()}
    assert required.issubset(names)

    decision = json.loads((session_dir / "shadow_reranker_challenger_design_v1_decision.json").read_text(encoding="utf-8"))
    model_spec = json.loads((session_dir / "shadow_challenger_model_spec.json").read_text(encoding="utf-8"))
    variant_comparison = json.loads((session_dir / "shadow_challenger_variant_pool_comparison.json").read_text(encoding="utf-8"))
    leakage_audit = json.loads((session_dir / "shadow_challenger_leakage_audit.json").read_text(encoding="utf-8"))

    assert decision["decision"] in {
        "keep_for_forward_validation",
        "hold_needs_forward_validation",
        "drop",
        "needs_target_redesign",
        "insufficient_stability",
        "insufficient_time_split_depth",
    }
    assert model_spec["selected_variant"] == "tree_hgb_path_value"
    assert model_spec["target_label"] == "path_value_score_v1"
    assert "no_lookahead_proof" in model_spec
    assert variant_comparison["selected_variant"] == "tree_hgb_path_value"
    assert leakage_audit["schema_version"].startswith("tradex_shadow_reranker_challenger_design_v1")

    diff = pd.read_parquet(session_dir / "shadow_challenger_topk_membership_diff.parquet")
    required_columns = {
        "surface_name",
        "variant_name",
        "topk",
        "anchor_date",
        "month_bucket",
        "side",
        "symbol",
        "candidate_idx",
        "model_score",
        "model_rank",
        "model_selected",
        "champion_selected",
        "membership_changed",
        "selected_overlap",
    }
    assert required_columns.issubset(set(diff.columns))
