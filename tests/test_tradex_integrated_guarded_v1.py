from __future__ import annotations

import json
from pathlib import Path

from scripts.tradex_integrated_guarded_v1 import (
    DEFAULT_OUTPUT_DIR,
    run_integrated_guarded_v1,
)


def test_integrated_guarded_v1_keep_and_preserve_selection_edge(tmp_path: Path) -> None:
    output_dir = tmp_path / "integrated_guarded_v1"
    result = run_integrated_guarded_v1(output_dir=output_dir)

    summary = result["summary"]
    compare = result["compare"]

    assert summary["diagnosis_decision"] == "hold"
    assert summary["authoritative_rollup_decision"] == "hold"
    assert summary["selection_only_edge_preserved"] is True
    assert summary["policy_layer_destroyed_edge"] is True
    assert summary["guardrail_rule"] == "long_top5_only_policy"
    assert summary["policy_variant"] == "integrated_specialized_gate_guarded_policy_v1"
    assert summary["selection_variant"] == "specialized_3way_gate"
    assert summary["long_exit_variant"] == "long_late_exit_repair_v1"

    assert summary["topk_observations"]["top5"] > 0
    assert summary["topk_observations"]["top10"] < 0
    assert summary["topk_observations"]["top20"] < 0
    assert summary["top6_10_policy_vs_hold_gap_delta"] > 0
    assert summary["top11_20_policy_vs_hold_gap_delta"] > 0

    assert compare["delta"]["top5_policy_net_realized_pnl"] > 0
    assert compare["delta"]["top10_policy_net_realized_pnl"] < 0
    assert compare["delta"]["top20_policy_net_realized_pnl"] < 0
    assert compare["delta"]["top5_selection_only_avg_ret63"] > 0
    assert compare["delta"]["top10_selection_only_avg_ret63"] > 0
    assert compare["delta"]["top20_selection_only_avg_ret63"] > 0

    for key in (
        "summary_json",
        "compare_json",
        "dates_json",
        "candidate_snapshots_json",
        "selection_only_ledger_json",
        "policy_trade_ledger_json",
        "full_universe_gate_coverage_json",
        "db_provenance_json",
        "exclusion_diagnostics_json",
        "decision_json",
    ):
        assert Path(result["paths"][key]).exists()

    for key in (
        "integrated_guarded_v1_replay_summary.json",
        "integrated_guarded_v1_compare.json",
        "integrated_guarded_v1_dates.json",
        "integrated_guarded_v1_candidate_snapshots.json",
        "integrated_guarded_v1_selection_only_ledger.json",
        "integrated_guarded_v1_policy_trade_ledger.json",
        "integrated_guarded_v1_full_universe_gate_coverage.json",
        "integrated_guarded_v1_db_provenance.json",
        "integrated_guarded_v1_exclusion_diagnostics.json",
        "integrated_guarded_v1_decision.json",
    ):
        assert (output_dir / key).exists()


def test_integrated_guarded_v1_json_is_parseable(tmp_path: Path) -> None:
    output_dir = tmp_path / "integrated_guarded_v1_json"
    run_integrated_guarded_v1(output_dir=output_dir)
    for file_name in (
        "integrated_guarded_v1_replay_summary.json",
        "integrated_guarded_v1_compare.json",
        "integrated_guarded_v1_decision.json",
    ):
        payload = json.loads((output_dir / file_name).read_text(encoding="utf-8"))
        assert isinstance(payload, dict)
