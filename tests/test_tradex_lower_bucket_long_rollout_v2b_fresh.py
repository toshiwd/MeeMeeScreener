from __future__ import annotations

import json
from pathlib import Path

from scripts.tradex_lower_bucket_long_rollout_v2b_fresh import run_lower_bucket_long_rollout_v2b_fresh


def test_lower_bucket_long_rollout_v2b_fresh_smoke(tmp_path: Path) -> None:
    output_dir = tmp_path / "lower_bucket_long_rollout_v2b_fresh"
    result = run_lower_bucket_long_rollout_v2b_fresh(output_dir=output_dir, row_limit=5)
    summary = result["summary"]
    compare = result["compare"]
    exposure = result["exposure_normalization"]

    assert summary["policy_variant"] == "lower_bucket_long_rollout_v2B_fresh"
    assert summary["engine_policy_variant"] == "lower_bucket_long_rollout_v2"
    assert summary["rollout_variant"] == "B"
    assert compare["best_variant"] == "B"
    assert exposure["challenger"]["overall"]["candidate_starvation_flag"] in {True, False}
    assert exposure["challenger"]["by_topk"]["10"]["selected_count"] >= 0

    expected = [
        "lower_bucket_long_rollout_v2B_fresh_summary.json",
        "lower_bucket_long_rollout_v2B_fresh_compare.json",
        "lower_bucket_long_rollout_v2B_fresh_by_rank_bucket.json",
        "lower_bucket_long_rollout_v2B_fresh_by_side.json",
        "lower_bucket_long_rollout_v2B_fresh_by_action.json",
        "lower_bucket_long_rollout_v2B_fresh_exposure_normalization.json",
        "lower_bucket_long_rollout_v2B_fresh_trade_ledger.json",
        "lower_bucket_long_rollout_v2B_fresh_decision.json",
    ]
    for name in expected:
        assert (output_dir / name).exists(), name
        json.loads((output_dir / name).read_text(encoding="utf-8"))


def test_lower_bucket_long_rollout_v2b_fresh_json_is_parseable(tmp_path: Path) -> None:
    output_dir = tmp_path / "lower_bucket_long_rollout_v2b_fresh_json"
    run_lower_bucket_long_rollout_v2b_fresh(output_dir=output_dir, row_limit=3)
    for name in [
        "lower_bucket_long_rollout_v2B_fresh_summary.json",
        "lower_bucket_long_rollout_v2B_fresh_compare.json",
        "lower_bucket_long_rollout_v2B_fresh_decision.json",
        "lower_bucket_long_rollout_v2B_fresh_exposure_normalization.json",
    ]:
        json.loads((output_dir / name).read_text(encoding="utf-8"))
