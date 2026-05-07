from __future__ import annotations

import json
from pathlib import Path

from scripts.tradex_daily_action_policy_engine_v1 import REQUIRED_ARTIFACTS, build_artifacts


def test_daily_action_policy_engine_artifact_generator_emits_required_json(tmp_path: Path) -> None:
    result = build_artifacts(output_root=tmp_path, repo_root=Path.cwd(), run_id="smoke", commands_run=["pytest smoke"])

    assert result["ok"] is True
    run_dir = Path(result["run_dir"])
    assert run_dir.exists()
    assert sorted(path.name for path in run_dir.glob("*.json")) == sorted(REQUIRED_ARTIFACTS)

    payloads = {}
    for name in REQUIRED_ARTIFACTS:
        payload = json.loads((run_dir / name).read_text(encoding="utf-8"))
        payloads[name] = payload
        assert payload["research_name"] == "tradex_daily_action_policy_engine_v1"
        assert payload["boundary"] == "TRADEX-only"

    assert payloads["final_decision.json"]["final_status"] == "implementation_done"
    assert payloads["final_decision.json"]["adoption_decision"] == "not_made"
    assert payloads["final_decision.json"]["candidate_policy_implemented"] is False
    assert payloads["research_axis_decision.json"]["candidate_policy_implemented"] is False
    assert payloads["evaluation_contract.json"]["execution_model"]["next_session_open_status"] == "supported"
    assert payloads["_ARTIFACT_COMPLETE.json"]["artifact_list"] == REQUIRED_ARTIFACTS
    assert payloads["_ARTIFACT_COMPLETE.json"]["verification_status"]["required_artifacts_written"] is True


def test_daily_action_policy_engine_artifacts_do_not_use_adoption_labels(tmp_path: Path) -> None:
    result = build_artifacts(output_root=tmp_path, repo_root=Path.cwd(), run_id="labels", commands_run=["pytest smoke"])
    run_dir = Path(result["run_dir"])
    final_decision = json.loads((run_dir / "final_decision.json").read_text(encoding="utf-8"))

    assert final_decision["final_status"] in {"implementation_done", "blocked", "needs_director_review"}
    assert final_decision["final_status"] not in {"keep", "hold", "drop"}
