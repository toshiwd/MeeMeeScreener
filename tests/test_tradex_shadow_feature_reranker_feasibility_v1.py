from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.tradex_shadow_feature_reranker_feasibility_v1 import run_shadow_feature_reranker_feasibility_v1


def test_shadow_feature_reranker_feasibility_smoke_writes_required_artifacts(tmp_path: Path) -> None:
    result = run_shadow_feature_reranker_feasibility_v1(
        output_root=tmp_path / "shadow_feature_reranker_feasibility_v1",
        limit_anchor_dates=2,
        jobs=2,
    )

    session_dir = Path(result["output_dir"])
    required = {
        "run_manifest.json",
        "input_resolution.json",
        "shadow_reranker_feature_inventory.json",
        "shadow_reranker_label_contract.json",
        "shadow_reranker_split_contract.json",
        "shadow_reranker_model_contract.json",
        "shadow_reranker_variant_pool_comparison.json",
        "shadow_reranker_topk_membership_diff.parquet",
        "shadow_reranker_stability_audit.json",
        "shadow_reranker_feature_effect_summary.json",
        "shadow_feature_reranker_feasibility_v1_decision.json",
        "_ARTIFACT_COMPLETE.json",
    }
    names = {path.name for path in session_dir.iterdir()}
    assert required.issubset(names)

    decision = json.loads((session_dir / "shadow_feature_reranker_feasibility_v1_decision.json").read_text(encoding="utf-8"))
    inventory = json.loads((session_dir / "shadow_reranker_feature_inventory.json").read_text(encoding="utf-8"))
    split_contract = json.loads((session_dir / "shadow_reranker_split_contract.json").read_text(encoding="utf-8"))
    model_contract = json.loads((session_dir / "shadow_reranker_model_contract.json").read_text(encoding="utf-8"))

    assert decision["decision"] in {
        "ready_for_shadow_challenger_design",
        "needs_feature_target_redesign",
        "insufficient_oos_signal",
        "insufficient_time_split_depth",
        "stop_model_reranker_line",
    }
    assert inventory["model_feature_count"] > 0
    assert "leakage_risk" in inventory["feature_classification_counts"]
    assert "forbidden_outcome" in inventory["feature_classification_counts"]
    assert split_contract["status"] in {"ready_for_time_split_evaluation", "insufficient_time_split_depth"}
    assert len(model_contract["model_variants"]) == 5

    diff = pd.read_parquet(session_dir / "shadow_reranker_topk_membership_diff.parquet")
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
