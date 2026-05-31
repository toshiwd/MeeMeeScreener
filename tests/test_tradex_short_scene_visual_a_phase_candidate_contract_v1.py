from __future__ import annotations

import json
from pathlib import Path

from scripts.tradex_short_scene_visual_a_phase_candidate_contract_v1 import _contract_decision


def _source(tmp_path: Path) -> dict:
    complete = tmp_path / "_ARTIFACT_COMPLETE.json"
    complete.write_text(json.dumps({"complete": True}), encoding="utf-8")
    return {
        "authoritative_rollup_decision": "keep",
        "candidate_generation_challenger_created": True,
        "meemee_reflectable": False,
        "artifacts": {"artifact_complete": str(complete)},
        "scope": {
            "tradex_only": True,
            "meemee_ranking_changed": False,
            "meemee_ui_changed": False,
            "runtime_db_written": False,
            "silent_fallback_used": False,
            "research_fallback_used": False,
        },
        "coverage": {"oos_positive_active_month_rate": 0.7, "oos_active_month_count": 10},
        "observed_branching": {"changed_top5_members_count": 54},
        "compare": {
            "oos": {
                "top5": {"additive_delta": {"forward_return_20_mean": 0.003, "bad_loser_rate_20": -0.01, "severe_loser_rate_20": -0.01}},
                "top10": {"additive_delta": {"forward_return_20_mean": 0.005, "bad_loser_rate_20": 0.0}},
            }
        },
    }


def test_contract_decision_ready_for_valid_keep(tmp_path: Path) -> None:
    decision = _contract_decision(_source(tmp_path))

    assert decision["decision"] == "candidate_generation_contract_ready"
    assert decision["blockers"] == []


def test_contract_decision_blocks_non_keep(tmp_path: Path) -> None:
    source = _source(tmp_path)
    source["authoritative_rollup_decision"] = "hold"

    decision = _contract_decision(source)

    assert decision["decision"] == "hold"
    assert "source_decision_keep" in decision["blockers"]
