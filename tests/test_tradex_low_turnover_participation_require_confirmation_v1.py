from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.tradex_low_turnover_participation_require_confirmation_v1 import (
    run_low_turnover_participation_require_confirmation_v1,
)


def test_low_turnover_participation_require_confirmation_writes_required_artifacts(tmp_path: Path) -> None:
    result = run_low_turnover_participation_require_confirmation_v1(
        output_root=tmp_path / "low_turnover_participation_require_confirmation_v1",
    )
    session_dir = Path(result["output_dir"])

    required = {
        "run_manifest.json",
        "input_resolution.json",
        "low_turnover_participation_false_positive_profile.json",
        "participation_confirmation_policy.json",
        "candidate_participation_confirmation_rows.parquet",
        "variant_pool_comparison.json",
        "monthly_comparison.json",
        "context_comparison.json",
        "topk_membership_diff.parquet",
        "precision_recall_summary.json",
        "false_positive_cost_summary.json",
        "low_turnover_participation_require_confirmation_v1_decision.json",
        "_ARTIFACT_COMPLETE.json",
        "risk_rows_reference_summary.json",
        "participation_field_coverage_summary.json",
        "family_good_pick_overlap_summary.json",
    }
    assert required.issubset({path.name for path in session_dir.iterdir()})

    profile = json.loads((session_dir / "low_turnover_participation_false_positive_profile.json").read_text(encoding="utf-8"))
    policy = json.loads((session_dir / "participation_confirmation_policy.json").read_text(encoding="utf-8"))
    pool = json.loads((session_dir / "variant_pool_comparison.json").read_text(encoding="utf-8"))
    precision = json.loads((session_dir / "precision_recall_summary.json").read_text(encoding="utf-8"))
    false_pos = json.loads((session_dir / "false_positive_cost_summary.json").read_text(encoding="utf-8"))
    decision = json.loads((session_dir / "low_turnover_participation_require_confirmation_v1_decision.json").read_text(encoding="utf-8"))

    assert profile["family_code"] == "low_turnover_participation_false_positive"
    assert profile["family_count"] == 191
    assert profile["top5_count"] > 0
    assert policy["challenger_family"] == "low_turnover_participation_false_positive"
    assert pool["candidate_row_count"] == 2542
    assert pool["target_family_count"] == 191
    assert pool["topk"]["top5"]["changed_members_count"] >= 0
    assert decision["decision"] in {"keep", "hold", "drop", "needs_more_participation_signal"}
    assert decision["status"] == decision["decision"]
    assert "top5" in precision["topk"] and "top10" in precision["topk"]
    assert "top5" in false_pos["topk"] and "top10" in false_pos["topk"]

    rows = pd.read_parquet(session_dir / "candidate_participation_confirmation_rows.parquet")
    assert len(rows) == 2542
    for column in [
        "participation_confirmation_state",
        "participation_confirmation_reason",
        "participation_confirmation_needed",
        "participation_confirmation_ok",
        "effective_rank_score",
        "variant_group_rank",
        "variant_selected_top5",
        "variant_selected_top10",
        "variant_selected_top20",
    ]:
        assert column in rows.columns
