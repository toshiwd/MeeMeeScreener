from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from scripts.tradex_ma_state_family_risk_on_trend_bad_pick_pruner_v1 import (
    RISK_ON_TREND_ONLY_PENALTY,
    _apply_pruner_scores,
    run_ma_state_family_risk_on_trend_bad_pick_pruner_v1,
)


def test_pruner_penalty_caps_and_regime_match() -> None:
    frame = pd.DataFrame(
        [
            {
                "score": 0.80,
                "strict_bad_pick_family": True,
                "relaxed_bad_pick_family": True,
                "bad_pick_watch_family": True,
                "regime_bad_pick_family": True,
                "mae_risk_family": True,
                "endpoint_bad_pick_family": True,
                "family_regime_context": "C:risk_on_trend",
                "family_bad_pick_regime": "C:risk_on_trend",
                "dominant_regime_context": "C:risk_on_trend",
            }
        ]
    )
    adjusted = _apply_pruner_scores(frame)
    assert adjusted.loc[0, "score_adjustment"] == RISK_ON_TREND_ONLY_PENALTY
    assert adjusted.loc[0, "challenger_score"] == pytest.approx(frame.loc[0, "score"] + RISK_ON_TREND_ONLY_PENALTY)
    assert bool(adjusted.loc[0, "regime_match_penalty_applied"]) is True
    assert adjusted.loc[0, "score_adjustment_reason"] == ["regime_bad_pick_family", "risk_on_trend_match", "risk_on_trend_only"]


def test_pruner_smoke_run_writes_required_artifacts(tmp_path: Path) -> None:
    output_root = tmp_path / "bad_pick_pruner"
    result = run_ma_state_family_risk_on_trend_bad_pick_pruner_v1(
        v1_session=Path(r"G:\Tradex\ma_state_family_bad_pick_pruner_v1\20260429T071723Z-2a858f13"),
        v1_1_session=Path(r"G:\Tradex\ma_state_family_bad_pick_pruner_v1_1_narrow_penalty\20260429T072508Z-bf85d2f9"),
        output_root=output_root,
        limit_anchor_dates=2,
    )

    session_dir = Path(result["session_dir"])
    assert session_dir.exists()

    required_files = (
        "run_manifest.json",
        "ma_state_family_risk_on_trend_bad_pick_pruner_v1_compare.json",
        "ma_state_family_risk_on_trend_bad_pick_pruner_v1_decision.json",
        "prior_pruner_delta.json",
        "topk_membership_diff.parquet",
        "penalty_coverage_summary.json",
        "monthly_comparison.json",
        "regime_comparison.json",
        "_ARTIFACT_COMPLETE.json",
    )
    for file_name in required_files:
        assert (session_dir / file_name).exists()

    compare = json.loads((session_dir / "ma_state_family_risk_on_trend_bad_pick_pruner_v1_compare.json").read_text(encoding="utf-8"))
    decision = json.loads((session_dir / "ma_state_family_risk_on_trend_bad_pick_pruner_v1_decision.json").read_text(encoding="utf-8"))
    delta = json.loads((session_dir / "prior_pruner_delta.json").read_text(encoding="utf-8"))
    manifest = json.loads((session_dir / "run_manifest.json").read_text(encoding="utf-8"))
    coverage = json.loads((session_dir / "penalty_coverage_summary.json").read_text(encoding="utf-8"))

    assert compare["schema_version"] == "tradex_ma_state_family_risk_on_trend_bad_pick_pruner_v1_compare_v1"
    assert decision["authoritative_rollup_decision"] in {"keep", "hold", "drop"}
    assert manifest["no_lookahead_inherited"] is True
    assert manifest["v1_session_id"] == "20260429T071723Z-2a858f13"
    assert manifest["v1_1_session_id"] == "20260429T072508Z-bf85d2f9"
    assert delta["schema_version"] == "tradex_ma_state_family_risk_on_trend_bad_pick_pruner_v1_delta_v1"
    assert coverage["candidate_rows"] > 0
    assert coverage["matched_family_rate"] is not None
    assert coverage["family_bad_pick_regime_extraction"] == "dominant_regime_context"

    parquet = pd.read_parquet(session_dir / "topk_membership_diff.parquet")
    assert not parquet.empty
    assert {"score_adjustment", "challenger_score", "state_family_id"}.issubset(set(parquet.columns))
