from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from scripts.tradex_ma_state_family_high_value_boost_v1 import (
    HIGH_VALUE_BOOST,
    _apply_boost_scores,
    run_ma_state_family_high_value_boost_v1,
)


def test_boost_applies_to_high_value_family() -> None:
    frame = pd.DataFrame(
        [
            {
                "score": 0.80,
                "stable_high_value_family": True,
                "family_classification": "stable_high_value_family",
            }
        ]
    )
    adjusted = _apply_boost_scores(frame)
    assert adjusted.loc[0, "score_adjustment"] == HIGH_VALUE_BOOST
    assert adjusted.loc[0, "challenger_score"] == pytest.approx(frame.loc[0, "score"] + HIGH_VALUE_BOOST)
    assert bool(adjusted.loc[0, "high_value_boost_applied"]) is True
    assert adjusted.loc[0, "score_adjustment_reason"] == ["stable_high_value_family", "high_value_boost"]


def test_boost_smoke_run_writes_required_artifacts(tmp_path: Path) -> None:
    output_root = tmp_path / "high_value_boost"
    result = run_ma_state_family_high_value_boost_v1(
        v1_session=Path(r"G:\Tradex\ma_state_family_bad_pick_pruner_v1\20260429T071723Z-2a858f13"),
        v1_1_session=Path(r"G:\Tradex\ma_state_family_bad_pick_pruner_v1_1_narrow_penalty\20260429T072508Z-bf85d2f9"),
        output_root=output_root,
        limit_anchor_dates=2,
    )

    session_dir = Path(result["session_dir"])
    assert session_dir.exists()

    required_files = (
        "run_manifest.json",
        "ma_state_family_high_value_boost_v1_compare.json",
        "ma_state_family_high_value_boost_v1_decision.json",
        "boost_coverage_summary.json",
        "topk_membership_diff.parquet",
        "monthly_comparison.json",
        "regime_comparison.json",
        "_ARTIFACT_COMPLETE.json",
    )
    for file_name in required_files:
        assert (session_dir / file_name).exists()

    compare = json.loads((session_dir / "ma_state_family_high_value_boost_v1_compare.json").read_text(encoding="utf-8"))
    decision = json.loads((session_dir / "ma_state_family_high_value_boost_v1_decision.json").read_text(encoding="utf-8"))
    manifest = json.loads((session_dir / "run_manifest.json").read_text(encoding="utf-8"))
    coverage = json.loads((session_dir / "boost_coverage_summary.json").read_text(encoding="utf-8"))

    assert compare["schema_version"] == "tradex_ma_state_family_high_value_boost_v1_compare_v1"
    assert decision["authoritative_rollup_decision"] in {"keep", "hold", "drop"}
    assert manifest["no_lookahead_inherited"] is True
    assert manifest["v1_session_id"] == "20260429T071723Z-2a858f13"
    assert manifest["v1_1_session_id"] == "20260429T072508Z-bf85d2f9"
    assert coverage["candidate_rows"] > 0
    assert coverage["matched_family_rate"] is not None
    assert coverage["boost_applied_rows"] >= 0
    assert coverage["family_high_value_definition"].startswith("stable_high_value_family")

    parquet = pd.read_parquet(session_dir / "topk_membership_diff.parquet")
    assert not parquet.empty
    assert {"score_adjustment", "challenger_score", "state_family_id"}.issubset(set(parquet.columns))
