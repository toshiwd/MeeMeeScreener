from __future__ import annotations

import json
from pathlib import Path

from scripts import teppan_shadow_watch_mode_logging_plan_v1 as mod


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _replay_root(tmp_path: Path, *, decision: str = "hold_for_watch_mode", no_mutation_pass: bool = True) -> Path:
    root = tmp_path / "replay"
    _write_json(
        root / "replay_result.json",
        {
            "decision": decision,
            "decision_reason": "teppan_coverage_exists_but_shadow_topk_additions_are_sparse_or_absent",
            "required_metrics": {
                "replay_date_count": 10,
                "dates_with_teppan_pattern_match_top100": ["2026-05-12"],
                "added_by_shadow_top5_count": 0,
                "added_by_shadow_top10_count": 0,
                "added_by_shadow_top20_count": 0,
                "human_review_candidate_count": 0,
            },
        },
    )
    _write_json(
        root / "replay_date_coverage_report.json",
        {
            "metrics": {
                "replay_date_count": 10,
                "dates_with_teppan_pattern_match_top20": ["2026-05-12"],
                "dates_with_teppan_pattern_match_top50": ["2026-05-12"],
                "dates_with_teppan_pattern_match_top100": ["2026-05-12"],
                "dates_with_shadow_top5_additions": [],
                "dates_with_shadow_top10_additions": [],
                "dates_with_shadow_top20_additions": [],
                "total_boosted_candidate_count": 0,
                "total_loss_guard_blocked_count": 1,
                "added_by_shadow_top5_count": 0,
                "added_by_shadow_top10_count": 0,
                "added_by_shadow_top20_count": 0,
                "human_review_candidate_count": 0,
            }
        },
    )
    _write_json(root / "no_mutation_audit.json", {"no_mutation_pass": no_mutation_pass})
    _write_json(root / "next_axis_recommendation.json", {"next": "teppan_shadow_watch_mode_logging_plan_v1"})
    _write_json(root / "_ARTIFACT_COMPLETE.json", {"complete": True})
    return root


def test_watch_mode_plan_ready_and_outputs_required_artifacts(tmp_path: Path) -> None:
    payload = mod.run_teppan_shadow_watch_mode_logging_plan_v1(
        recent_days_replay_root=_replay_root(tmp_path),
        output_parent=tmp_path / "out",
        run_id="watch",
    )

    decision = payload["research_decision"]
    plan = payload["watch_mode_logging_plan"]
    assert decision["decision"] == "watch_mode_ready"
    assert decision["activation_allowed"] is False
    assert plan["runtime_duckdb_write_allowed"] is False
    assert "boost_eligible_count" in plan["watch_metrics"]
    assert payload["artifact_complete"]["complete"] is True
    for name in mod.REQUIRED_OUTPUTS:
        assert (Path(payload["output_root"]) / name).exists(), name

    trigger = json.loads((Path(payload["output_root"]) / "human_review_trigger_contract.json").read_text(encoding="utf-8"))
    assert trigger["next_after_trigger"] == "teppan_shadow_candidate_manual_review_v1"
    assert trigger["activation_allowed_after_trigger"] is False


def test_watch_mode_plan_blocks_when_no_mutation_failed(tmp_path: Path) -> None:
    payload = mod.run_teppan_shadow_watch_mode_logging_plan_v1(
        recent_days_replay_root=_replay_root(tmp_path, no_mutation_pass=False),
        output_parent=tmp_path / "out",
        run_id="watch",
    )

    assert payload["research_decision"]["decision"] == "watch_mode_plan_blocked"
    assert payload["research_decision"]["decision_reason"] == "source_recent_days_replay_no_mutation_failed"


def test_watch_mode_plan_blocks_on_missing_required_inputs(tmp_path: Path) -> None:
    root = tmp_path / "replay"
    root.mkdir()
    _write_json(root / "replay_result.json", {"decision": "hold_for_watch_mode"})

    payload = mod.run_teppan_shadow_watch_mode_logging_plan_v1(
        recent_days_replay_root=root,
        output_parent=tmp_path / "out",
        run_id="watch",
    )

    assert payload["research_decision"]["decision"] == "watch_mode_plan_blocked"
    assert payload["research_decision"]["decision_reason"] == "missing_required_recent_days_replay_inputs"
