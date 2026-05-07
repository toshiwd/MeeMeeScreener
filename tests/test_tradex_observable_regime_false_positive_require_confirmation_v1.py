from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.tradex_observable_regime_false_positive_require_confirmation_v1 import (
    run_observable_regime_false_positive_require_confirmation_v1,
)


BACKFILL_SESSION = Path(r"G:\Tradex\audit_surface_context_backfill_v1\20260501T051248Z-eba42646")
ENRICHED_CANDIDATE = BACKFILL_SESSION / "candidate_prefilter_rows_context_enriched.parquet"
ENRICHED_UNKNOWN = Path(r"G:\Tradex\bad_pick_unknown_reclassification_enriched_v1\20260501T053110Z-7a584991\enriched_unknown_reclassification_rows.parquet")


def test_observable_regime_false_positive_require_confirmation_smoke_run(tmp_path: Path) -> None:
    output_root = tmp_path / "observable_regime_false_positive_require_confirmation_v1"
    result = run_observable_regime_false_positive_require_confirmation_v1(
        candidate_surface_path=ENRICHED_CANDIDATE,
        unknown_reclassification_path=ENRICHED_UNKNOWN,
        backfill_session=BACKFILL_SESSION,
        output_root=output_root,
        limit_anchor_dates=2,
        jobs=2,
    )

    session_dir = Path(result["session_dir"])
    assert session_dir.exists()

    required_files = (
        "run_manifest.json",
        "input_resolution.json",
        "observable_regime_false_positive_profile.json",
        "confirmation_policy.json",
        "candidate_confirmation_rows.parquet",
        "variant_pool_comparison.json",
        "monthly_comparison.json",
        "context_comparison.json",
        "topk_membership_diff.parquet",
        "precision_recall_summary.json",
        "false_positive_cost_summary.json",
        "observable_regime_false_positive_require_confirmation_v1_decision.json",
        "_ARTIFACT_COMPLETE.json",
    )
    for file_name in required_files:
        assert (session_dir / file_name).exists()

    decision = json.loads((session_dir / "observable_regime_false_positive_require_confirmation_v1_decision.json").read_text(encoding="utf-8"))
    policy = json.loads((session_dir / "confirmation_policy.json").read_text(encoding="utf-8"))
    pool = json.loads((session_dir / "variant_pool_comparison.json").read_text(encoding="utf-8"))
    artifact = json.loads((session_dir / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))

    assert decision["decision"] in {"keep", "drop", "hold", "needs_more_confirmation_field"}
    assert policy["rule_type"] == "require-confirmation"
    assert "monthly_context" in policy["required_fields"]
    assert "path_value_score_v1" in policy["excluded_fields"]
    assert artifact["parse_status"]["run_manifest"] is True
    assert artifact["row_reconciliation"]["baseline_rows"] > 0
    assert pool["comparison"]["top5"]["delta_mean_forward_ret_20d"] is not None

    rows = pd.read_parquet(session_dir / "candidate_confirmation_rows.parquet")
    diff = pd.read_parquet(session_dir / "topk_membership_diff.parquet")
    assert not rows.empty
    assert not diff.empty
    assert {"family_code", "confirmed", "effective_rank_priority", "variant_selected_top5", "variant_selected_top10"}.issubset(rows.columns)
    assert {"changed_top5_member", "changed_top10_member", "changed_top20_member"}.issubset(diff.columns)
