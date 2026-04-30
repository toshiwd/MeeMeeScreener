from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.tradex_score_component_overweight_cap_or_confirmation_v1 import (
    _build_risk_and_confirmation,
    run_score_component_overweight_cap_or_confirmation_v1,
)


def test_risk_and_confirmation_mask_is_proxy_based() -> None:
    frame = pd.DataFrame(
        {
            "anchor_date": ["2024-01-01", "2024-01-01", "2024-01-01"],
            "symbol": ["AAA", "BBB", "CCC"],
            "side": ["long", "long", "short"],
            "champion_selected_top10": [True, True, True],
            "monthly_context": ["monthly_overextended", "monthly_overextended", "monthly_overextended"],
            "weekly_context": ["weekly_overextended", "weekly_overextended", "weekly_overextended"],
            "family_classification": ["regime_dependent_family", "regime_dependent_family", "regime_dependent_family"],
            "shape_classification": ["shape_positive_modifier", "shape_positive_modifier", "shape_positive_modifier"],
            "candle_shape_modifier": ["bull_large", "bull_large", "bull_large"],
            "vol_ratio5_20": [1.2, 0.8, 1.5],
            "candle_body_ratio": [0.6, 0.4, 0.7],
        }
    )
    out = _build_risk_and_confirmation(frame)
    assert bool(out.loc[0, "score_overweight_risk_slice"]) is True
    assert bool(out.loc[1, "score_overweight_risk_slice"]) is True
    assert bool(out.loc[2, "score_overweight_risk_slice"]) is False
    assert bool(out.loc[0, "score_overweight_confirmation_ok"]) is True
    assert bool(out.loc[1, "score_overweight_confirmation_ok"]) is False


def test_smoke_run_emits_authoritative_artifacts(tmp_path: Path) -> None:
    result = run_score_component_overweight_cap_or_confirmation_v1(
        output_root=tmp_path,
        limit_anchor_dates=2,
        jobs=2,
    )
    session_dir = Path(result["session_dir"])
    assert session_dir.exists()
    required = [
        "run_manifest.json",
        "input_resolution.json",
        "score_overweight_policy.json",
        "candidate_score_overweight_rows.parquet",
        "variant_pool_comparison.json",
        "monthly_comparison.json",
        "context_comparison.json",
        "topk_membership_diff.parquet",
        "precision_recall_summary.json",
        "false_positive_cost_summary.json",
        "score_component_overweight_cap_or_confirmation_v1_decision.json",
        "_ARTIFACT_COMPLETE.json",
    ]
    for name in required:
        assert (session_dir / name).exists(), name

    decision = json.loads((session_dir / "score_component_overweight_cap_or_confirmation_v1_decision.json").read_text(encoding="utf-8"))
    assert decision["decision"] in {"keep", "hold", "drop"}

    comparison = json.loads((session_dir / "variant_pool_comparison.json").read_text(encoding="utf-8"))
    assert comparison["delta_vs_original"]["score_overweight_cap"]["5"]["changed_members_count"] >= 0
    assert comparison["delta_vs_original"]["score_overweight_require_confirmation"]["10"]["changed_members_count"] >= 0
