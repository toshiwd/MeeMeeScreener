from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.tradex_selection_layer_weak_regime_bad_pick_removal import (
    BASELINE_INPUT_DIR,
    _weak_regime_penalty,
    run_selection_layer_weak_regime_bad_pick_removal,
)


def test_weak_regime_penalty_removes_lower_bucket_long() -> None:
    row = pd.Series(
        {
            "side": "long",
            "challenger_rank": 11,
            "cnt60Up": 8.0,
            "monthlyBreakoutUpProb": 0.42,
            "monthlyBreakoutDownProb": 0.61,
            "reclaim60": 0.0,
            "v60Strong": 0.0,
            "marketRegime": "risk_off",
        }
    )

    removed, reason, penalty = _weak_regime_penalty(row)

    assert removed is True
    assert reason == "weak_regime_bad_pick_removal_top11_20"
    assert penalty == 1.0


def test_selection_layer_weak_regime_bad_pick_removal_smoke(tmp_path: Path) -> None:
    output_dir = tmp_path / "selection_layer_weak_regime_bad_pick_removal"
    result = run_selection_layer_weak_regime_bad_pick_removal(
        input_dir=BASELINE_INPUT_DIR,
        output_dir=output_dir,
        anchor_limit=1,
    )

    summary = result["summary"]
    assert summary["authoritative_rollup_decision"] in {"keep", "hold", "drop"}
    assert "branching_metrics" in summary
    assert "selection_divergence_reason" in summary["branching_metrics"]

    for key in (
        "summary",
        "compare",
        "candidate_snapshots",
        "selection_only_ledger",
        "policy_trade_ledger",
        "by_rank_bucket",
        "by_side",
        "by_action",
        "decision",
        "full_universe_gate_coverage",
    ):
        assert Path(result["paths"][key]).exists(), key

    written_summary = json.loads(Path(result["paths"]["summary"]).read_text(encoding="utf-8"))
    assert "weak_regime_only_performance" in written_summary
