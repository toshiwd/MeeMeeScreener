from __future__ import annotations

import json
from pathlib import Path

from scripts.tradex_long_action_policy_cash_gate_refinement_freeze_summary_v1 import build_freeze_summary


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _make_source_dirs(root: Path) -> tuple[Path, Path, Path]:
    gate_redesign_dir = root / "gate_redesign"
    rank_guard_dir = root / "rank_guard"
    score_guard_dir = root / "score_guard"
    gate_redesign_dir.mkdir(parents=True, exist_ok=True)
    rank_guard_dir.mkdir(parents=True, exist_ok=True)
    score_guard_dir.mkdir(parents=True, exist_ok=True)

    _write_json(
        gate_redesign_dir / "skipped_buy_restoration_summary.json",
        {
            "schema_version": "x",
            "restored_buy_count": 11,
            "restored_good_buy": 8,
            "restored_bad_buy": 3,
            "restored_good_ret_20_mean": 0.06,
            "restored_bad_ret_20_mean": -0.11,
            "false_negative_skip_reduction": 0.38,
            "true_positive_skip_retention": 0.88,
        },
    )
    _write_json(
        gate_redesign_dir / "branch_effect_audit.json",
        {
            "schema_version": "x",
            "branch_effect_present": True,
            "current_vs_redesign": {"diff_counts": {"action": 773}},
            "baseline_vs_current": {"diff_counts": {"action": 6927}},
        },
    )
    _write_json(rank_guard_dir / "rank_guard_tighten_decision.json", {"final_status": "insufficient_rank_separation"})
    _write_json(rank_guard_dir / "rank_guard_diagnostic.json", {"overlap_ranks": [4], "ranks_with_good": [2, 4, 5, 7, 10], "ranks_with_bad": [1, 4, 6], "single_cutoff_justified": False})
    _write_json(score_guard_dir / "same_day_score_guard_diagnostic_decision.json", {"final_status": "insufficient_score_separation"})
    _write_json(score_guard_dir / "score_guard_conflict_summary.json", {"best_separating_field": "score_abs_gap", "best_separating_score": [0.8181818181818182, 0.8, 1.0], "rank_overlap_ranks": [4]})
    return gate_redesign_dir, rank_guard_dir, score_guard_dir


def test_freeze_summary_writes_required_artifacts(tmp_path: Path) -> None:
    gate_redesign_dir, rank_guard_dir, score_guard_dir = _make_source_dirs(tmp_path)
    output_root = tmp_path / "freeze"
    result = build_freeze_summary(
        output_root,
        session_id="session-test",
        gate_redesign_dir=gate_redesign_dir,
        rank_guard_dir=rank_guard_dir,
        score_guard_dir=score_guard_dir,
    )

    session_dir = Path(result["output_dir"])
    expected = {
        "lineage_summary.json",
        "freeze_decision.json",
        "reusable_findings.json",
        "not_for_policy_reasons.json",
        "future_reopen_conditions.json",
        "_ARTIFACT_COMPLETE.json",
    }
    assert expected == {path.name for path in session_dir.iterdir()}

    freeze_decision = json.loads((session_dir / "freeze_decision.json").read_text(encoding="utf-8"))
    assert freeze_decision["decision"] == "freeze_current_cash_gate_refinement_line"
    assert freeze_decision["status"] == "explanation_only"
    assert freeze_decision["promote_ready"] is False
    assert freeze_decision["meemee_reflectable"] is False

    reusable = json.loads((session_dir / "reusable_findings.json").read_text(encoding="utf-8"))
    findings = {row["finding_id"]: row for row in reusable["findings"]}
    assert findings["current_gate_branch_real"]["status"] == "confirmed"
    assert findings["timing_relaxer_recovers_profitable_skipped_buys"]["status"] == "confirmed"

    future = json.loads((session_dir / "future_reopen_conditions.json").read_text(encoding="utf-8"))
    assert "entry_strength" in future["example_fields"]
    assert "rank" in future["do_not_reopen_based_only_on"]

    lineage = json.loads((session_dir / "lineage_summary.json").read_text(encoding="utf-8"))
    assert lineage["decision"] == "freeze_current_cash_gate_refinement_line"
    assert lineage["status"] == "explanation_only"
    assert lineage["same_condition_contract"] is True
