from __future__ import annotations

import json
from pathlib import Path

from scripts import tradex_image_research_env_setup_for_cnn_phase2b_v1 as mod


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")


def _write_sources(root: Path) -> tuple[Path, Path]:
    phase0 = root / "phase0" / "phase0-run"
    phase2 = root / "phase2" / "phase2-run"
    _write_json(phase0 / "research_decision.json", {"decision": "keep_candidate", "authoritative_research_decision": "image_assisted_phase0_1_ready_for_phase2", "silent_fallback_used": False, "research_fallback_used": False})
    _write_json(phase0 / "phase2_readiness_report.json", {"ready_for_phase2": True})
    _write_json(phase0 / "_ARTIFACT_COMPLETE.json", {"complete": True, "silent_fallback_used": False, "research_fallback_used": False})
    _write_json(phase2 / "research_decision.json", {"decision": "drop", "authoritative_research_decision": "image_only_classifier_phase2_failed", "silent_fallback_used": False, "research_fallback_used": False})
    _write_json(phase2 / "phase3_readiness_report.json", {"ready_for_fusion": False})
    _write_json(phase2 / "_ARTIFACT_COMPLETE.json", {"complete": True, "silent_fallback_used": False, "research_fallback_used": False})
    return phase0, phase2


def test_bool_arg_parses_expected_values() -> None:
    assert mod._bool_arg("true") is True
    assert mod._bool_arg("1") is True
    assert mod._bool_arg("false") is False
    assert mod._bool_arg("0") is False


def test_env_setup_writes_hold_artifacts_without_install(tmp_path: Path) -> None:
    phase0, phase2 = _write_sources(tmp_path)
    result = mod.run_image_research_env_setup_for_cnn_phase2b_v1(
        run_id="env-smoke",
        output_root=tmp_path / "out",
        env_dir=tmp_path / "envs" / "image-cnn-phase2b",
        source_image_phase0_1_run_id=phase0.name,
        source_image_phase2_run_id=phase2.name,
        source_image_phase0_1_root=phase0.parent,
        source_image_phase2_root=phase2.parent,
        phase2b_output_root=tmp_path / "phase2b",
        phase2b_rerun_id="phase2b-rerun",
        create_env=False,
        install_torch=False,
        run_phase2b_if_ready=True,
    )
    output_dir = Path(result["output_dir"])
    for artifact in mod.REQUIRED_ARTIFACTS:
        assert (output_dir / artifact).exists(), artifact

    decision = json.loads((output_dir / "research_decision.json").read_text(encoding="utf-8"))
    complete = json.loads((output_dir / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    acceptance = json.loads((output_dir / "acceptance_criteria_audit.json").read_text(encoding="utf-8"))
    rerun = json.loads((output_dir / "phase2b_rerun_report.json").read_text(encoding="utf-8"))

    assert decision["decision"] == "hold"
    assert decision["env_ready_for_image_cnn_phase2b"] is False
    assert decision["production_dependency_changed"] is False
    assert decision["meemee_runtime_changed"] is False
    assert decision["sklearn_fallback_used"] is False
    assert decision["silent_fallback_used"] is False
    assert decision["research_fallback_used"] is False
    assert complete["complete"] is True
    assert complete["silent_fallback_used"] is False
    assert acceptance["phase2b_rerun_command_artifact_created"] is True
    assert rerun["phase2b_rerun_attempted"] is False


def test_env_setup_rejects_nonfailed_phase2_source(tmp_path: Path) -> None:
    phase0, phase2 = _write_sources(tmp_path)
    _write_json(phase2 / "research_decision.json", {"decision": "hold", "authoritative_research_decision": "image_only_classifier_phase2_hold", "silent_fallback_used": False, "research_fallback_used": False})

    try:
        mod.run_image_research_env_setup_for_cnn_phase2b_v1(
            run_id="bad-source",
            output_root=tmp_path / "out",
            env_dir=tmp_path / "envs" / "image-cnn-phase2b",
            source_image_phase0_1_run_id=phase0.name,
            source_image_phase2_run_id=phase2.name,
            source_image_phase0_1_root=phase0.parent,
            source_image_phase2_root=phase2.parent,
            create_env=False,
            install_torch=False,
        )
    except RuntimeError as exc:
        assert "image_only_classifier_phase2_failed" in str(exc)
    else:
        raise AssertionError("nonfailed Phase2 source should be rejected")
