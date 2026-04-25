from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.tradex_selection_layer_weak_regime_bad_pick_removal_v2_calibration import (
    DEFAULT_INPUT_DIR,
    VARIANT_SPECS,
    _make_variant_penalty,
    run_selection_layer_weak_regime_bad_pick_removal_v2_calibration,
)


def test_variant_penalty_distinguishes_calibration_modes() -> None:
    row = pd.Series(
        {
            "side": "long",
            "challenger_rank": 11,
            "cnt60Up": 19.0,
            "monthlyBreakoutUpProb": 0.42,
            "monthlyBreakoutDownProb": 0.61,
            "reclaim60": 0.0,
            "v60Strong": 0.0,
            "marketRegime": "risk_off",
        }
    )

    light_removed, light_reason, light_penalty = _make_variant_penalty(VARIANT_SPECS["v2_light"])(row)
    boundary_removed, boundary_reason, boundary_penalty = _make_variant_penalty(VARIANT_SPECS["v2_boundary_only"])(row)
    strict_removed, strict_reason, strict_penalty = _make_variant_penalty(VARIANT_SPECS["v2_high_confidence_only"])(row)

    assert light_removed is True
    assert light_reason == "weak_regime_bad_pick_removal_v2_light_top11_20"
    assert light_penalty == 1.0

    assert boundary_removed is True
    assert boundary_reason == "weak_regime_bad_pick_removal_v2_boundary_only_top11_20"
    assert boundary_penalty == 1.0

    assert strict_removed is True
    assert strict_reason == "weak_regime_bad_pick_removal_v2_high_confidence_only_top11_20"
    assert strict_penalty == 1.0


def test_selection_layer_weak_regime_bad_pick_removal_v2_calibration_smoke(tmp_path: Path) -> None:
    output_dir = Path(r"C:\t\selection_layer_weak_regime_bad_pick_removal_v2_calibration_smoke")
    result = run_selection_layer_weak_regime_bad_pick_removal_v2_calibration(
        input_dir=DEFAULT_INPUT_DIR,
        output_dir=output_dir,
        anchor_limit=1,
    )

    summary = result["summary"]
    decision = result["decision"]
    compare = result["compare"]

    assert summary["best_variant"] in VARIANT_SPECS
    assert decision["diagnosis_decision"] in {"keep", "hold", "drop"}
    assert len(compare["variants"]) == len(VARIANT_SPECS)
    assert len(compare["variant_rows"]) == len(VARIANT_SPECS)
    assert "full_universe_gate_coverage" in summary
    coverage = summary["full_universe_gate_coverage"]["aggregate"]
    assert coverage["specialized"]["no_trade_rate_mean"] >= coverage["baseline"]["no_trade_rate_mean"]

    for key in (
        "summary",
        "compare",
        "decision",
        "by_variant",
        "by_rank_bucket",
        "by_side",
        "by_action",
    ):
        assert Path(result["paths"][key]).exists(), key

    written_summary = json.loads(Path(result["paths"]["summary"]).read_text(encoding="utf-8"))
    assert written_summary["selection_layer"] == "weak_regime_bad_pick_removal_v2_calibration"
