from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.tradex_conditional_high_value_candle_shape_modifier_v1 import (
    _derive_candle_shape_modifier,
    run_conditional_high_value_candle_shape_modifier_v1,
)


CONTEXT_SESSION = Path(r"G:\Tradex\multi_timeframe_conditional_state_value_v1\20260429T091138Z-7d26cb7c")
SOURCE_FAMILY_SESSION = Path(r"G:\Tradex\ma_position_path_research_family_filter\20260429T062945Z-87844c56")
REFERENCE_CONTEXT_GATED_BOOST_SESSION = Path(r"G:\Tradex\multi_timeframe_context_gated_high_value_boost_v1\20260429T094730Z-7e1acdee")


def test_candle_shape_bucket_derivation_covers_common_patterns() -> None:
    assert (
        _derive_candle_shape_modifier(
            {
                "o": 10.0,
                "h": 11.0,
                "l": 9.0,
                "c": 10.5,
                "prev_o": 10.2,
                "prev_h": 11.5,
                "prev_l": 8.7,
                "prev_c": 10.0,
                "candle_body_ratio": 0.45,
                "candle_upper_wick_ratio": 0.20,
                "candle_lower_wick_ratio": 0.35,
                "gap_pct": 0.0,
            }
        )
        == "inside_bar"
    )
    assert (
        _derive_candle_shape_modifier(
            {
                "o": 10.0,
                "h": 11.2,
                "l": 9.7,
                "c": 11.0,
                "prev_o": 10.8,
                "prev_h": 11.0,
                "prev_l": 9.5,
                "prev_c": 10.1,
                "candle_body_ratio": 0.55,
                "candle_upper_wick_ratio": 0.10,
                "candle_lower_wick_ratio": 0.20,
                "gap_pct": 0.02,
            }
        )
        == "bull_engulfing"
    )
    assert (
        _derive_candle_shape_modifier(
                {
                    "o": 10.0,
                    "h": 12.0,
                    "l": 9.1,
                    "c": 10.8,
                    "prev_o": 11.5,
                    "prev_h": 11.8,
                    "prev_l": 9.2,
                "prev_c": 11.6,
                "candle_body_ratio": 0.42,
                "candle_upper_wick_ratio": 0.18,
                "candle_lower_wick_ratio": 0.40,
                "gap_pct": 0.03,
            }
        )
        == "gap_up_bull"
    )
    assert (
        _derive_candle_shape_modifier(
            {
                "o": 10.0,
                "h": 10.2,
                "l": 9.4,
                "c": 10.05,
                "candle_body_ratio": 0.04,
                "candle_upper_wick_ratio": 0.48,
                "candle_lower_wick_ratio": 0.48,
                "gap_pct": 0.0,
            }
        )
        == "doji_like"
    )
    assert _derive_candle_shape_modifier({"o": None, "h": 10.0, "l": 9.0, "c": 9.5}) == "no_clear_shape"


def test_conditional_high_value_candle_shape_smoke_run_writes_required_artifacts(tmp_path: Path) -> None:
    output_root = tmp_path / "conditional_high_value_candle_shape_modifier"
    result = run_conditional_high_value_candle_shape_modifier_v1(
        source_context_session=CONTEXT_SESSION,
        source_family_session=SOURCE_FAMILY_SESSION,
        context_gated_boost_session=REFERENCE_CONTEXT_GATED_BOOST_SESSION,
        output_root=output_root,
        limit_symbols=2,
    )

    session_dir = Path(result["session_dir"])
    assert session_dir.exists()

    required_files = (
        "run_manifest.json",
        "candle_shape_definition.json",
        "conditional_shape_value_summary.json",
        "conditional_shape_modifier_classification.json",
        "shape_vs_base_slice_comparison.json",
        "shape_monthly_stability.json",
        "conditional_shape_rows.parquet",
        "conditional_high_value_candle_shape_modifier_v1_decision.json",
        "_ARTIFACT_COMPLETE.json",
    )
    for file_name in required_files:
        assert (session_dir / file_name).exists()

    manifest = json.loads((session_dir / "run_manifest.json").read_text(encoding="utf-8"))
    decision = json.loads((session_dir / "conditional_high_value_candle_shape_modifier_v1_decision.json").read_text(encoding="utf-8"))
    summary = json.loads((session_dir / "conditional_shape_value_summary.json").read_text(encoding="utf-8"))
    definition = json.loads((session_dir / "candle_shape_definition.json").read_text(encoding="utf-8"))
    classification = json.loads((session_dir / "conditional_shape_modifier_classification.json").read_text(encoding="utf-8"))

    assert manifest["schema_version"] == "tradex_conditional_high_value_candle_shape_modifier_v1_manifest_v1"
    assert manifest["source_context_session_id"] == "20260429T091138Z-7d26cb7c"
    assert manifest["source_family_session_id"] == "20260429T062945Z-87844c56"
    assert manifest["reference_context_gated_boost_session_id"] == "20260429T094730Z-7e1acdee"
    assert manifest["no_lookahead_inherited"] is True
    assert manifest["monthly_context_no_lookahead"] is True
    assert manifest["weekly_context_no_lookahead"] is True
    assert decision["recommendation"] in {"keep", "hold", "drop"}
    assert summary["conditional_high_value_row_count"] > 0
    assert summary["shape_bucket_count"] > 0
    assert definition["confirmed_fields"]
    assert "candle_body_ratio" in definition["confirmed_fields"]
    assert "inside_bar" in definition["provisional_fields"]
    assert classification["shape_class_counts"]["shape_sparse_or_unstable"] >= 0

    parquet = pd.read_parquet(session_dir / "conditional_shape_rows.parquet")
    assert not parquet.empty
    assert {"conditional_high_value", "candle_shape_modifier", "monthly_context", "weekly_context"}.issubset(set(parquet.columns))
    assert parquet["conditional_high_value"].all()
    assert parquet["monthly_context_no_lookahead"].all()
    assert parquet["weekly_context_no_lookahead"].all()
    assert parquet["candle_shape_modifier"].notna().all()
