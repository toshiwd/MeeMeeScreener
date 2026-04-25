from __future__ import annotations

import json
from pathlib import Path

from scripts.tradex_lower_bucket_long_rollout_v2 import run_lower_bucket_long_rollout_v2


def test_lower_bucket_long_rollout_v2_smoke(tmp_path: Path) -> None:
    output_dir = tmp_path / "lower_bucket_long_rollout_v2"
    result = run_lower_bucket_long_rollout_v2(output_dir=output_dir, row_limit=5)
    summary = result["summary"]
    compare = result["compare"]

    assert summary["policy_variant"] == "lower_bucket_long_rollout_v2"
    assert summary["baseline_policy_variant"] == "integrated_specialized_gate_guarded_policy_v1"
    assert summary["variant_aliases"]["C"] == "A"
    assert compare["best_variant"] in {"A", "B", "C"}
    assert "A" in compare["variants"]
    assert "B" in compare["variants"]
    assert "C" in compare["variants"]

    expected = [
        "lower_bucket_long_rollout_v2_summary.json",
        "lower_bucket_long_rollout_v2_compare.json",
        "lower_bucket_long_rollout_v2_by_variant.json",
        "lower_bucket_long_rollout_v2_by_rank_bucket.json",
        "lower_bucket_long_rollout_v2_by_side.json",
        "lower_bucket_long_rollout_v2_by_action.json",
        "lower_bucket_long_rollout_v2_trade_ledger.json",
        "lower_bucket_long_rollout_v2_decision.json",
    ]
    for name in expected:
        assert (output_dir / name).exists(), name
        json.loads((output_dir / name).read_text(encoding="utf-8"))


def test_lower_bucket_long_rollout_v2_json_is_parseable(tmp_path: Path) -> None:
    output_dir = tmp_path / "lower_bucket_long_rollout_v2_json"
    run_lower_bucket_long_rollout_v2(output_dir=output_dir, row_limit=3)
    for name in [
        "lower_bucket_long_rollout_v2_summary.json",
        "lower_bucket_long_rollout_v2_compare.json",
        "lower_bucket_long_rollout_v2_decision.json",
    ]:
        json.loads((output_dir / name).read_text(encoding="utf-8"))
