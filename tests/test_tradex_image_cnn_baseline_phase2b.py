from __future__ import annotations

import json
from pathlib import Path

from scripts import tradex_image_cnn_baseline_phase2b as mod


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")


def _dependency_blocked_payload() -> dict[str, object]:
    return {
        "schema_version": "tradex_image_cnn_baseline_phase2b_dependency_audit_v1",
        "axis_id": mod.AXIS_ID,
        "torch_available": False,
        "torch_version": None,
        "torchvision_available": False,
        "torchvision_version": None,
        "cuda_available": False,
        "selected_device": "unavailable",
        "can_train_simple_cnn_phase2b_v1": False,
        "can_train_resnet18_phase2b_v1": False,
        "sklearn_fallback_allowed": False,
        "sklearn_fallback_used": False,
        "production_runtime_requirements_modified": False,
        "meemee_runtime_dependencies_modified": False,
        "dependency_blocked": True,
        "blocked_reason": "torch_unavailable",
        "isolated_research_environment_notes": ["install torch only in isolated research env"],
    }


def _write_phase0_source(root: Path) -> Path:
    source = root / "phase0" / "phase0-run"
    manifest_rows: list[dict[str, object]] = []
    label_rows: list[dict[str, object]] = []
    split_rows: list[dict[str, object]] = []
    rows = [
        ("train", "top", True, False, False, 0.12),
        ("train", "bottom", False, True, False, -0.08),
        ("validation", "top", True, False, False, 0.10),
        ("validation", "bottom", False, True, False, -0.06),
        ("test", "top", True, False, False, 0.11),
        ("test", "bottom", False, True, False, -0.07),
    ]
    for idx, (split, label, is_top, is_bottom, is_neutral, ret20) in enumerate(rows, start=1):
        key = f"k{idx}"
        event_ymd = 20200100 + idx
        event_date = f"2020-01-{idx:02d}"
        manifest_rows.append(
            {
                "schema_version": "manifest",
                "image_sample_key": key,
                "candidate_event_key": f"candidate-{key}",
                "symbol": f"T{idx:04d}",
                "code": f"T{idx:04d}",
                "event_date": event_date,
                "event_ymd": event_ymd,
                "image_path": str(source / "images" / f"{key}.png"),
                "image_size": 224,
                "image_format": "PNG",
                "source_candidate_set": "wide_strength_pool_events",
                "source_score_family_id": "score_extended_continuation_vs_blowoff_risk_v1",
                "negative_guard_matched": True,
                "safe_full_tag": False,
                "prior_research_score_available": True,
                "prior_risk_score_available": True,
            }
        )
        label_rows.append(
            {
                "schema_version": "labels",
                "image_sample_key": key,
                "primary_label": "future_top15_by_ret20" if is_top else "future_bottom15_by_ret20" if is_bottom else "neutral_middle70",
                "future_top15_by_ret20": is_top,
                "future_bottom15_by_ret20": is_bottom,
                "neutral_middle70": is_neutral,
                "ret20": ret20,
                "MFE20": 0.16 if is_top else 0.02,
                "MAE20": -0.02 if is_top else -0.12,
                "severe_loss20": is_bottom,
                "future_top10_by_ret20": is_top,
                "future_top5_by_ret20": is_top,
                "big_winner_ret20_ge_10pct": is_top,
                "big_winner_MFE20_ge_15pct": is_top,
                "labels_used_in_candidate_key": False,
                "labels_used_in_image_rendering": False,
            }
        )
        split_rows.append(
            {
                "schema_version": "split",
                "image_sample_key": key,
                "symbol": f"T{idx:04d}",
                "event_date": event_date,
                "event_ymd": event_ymd,
                "split": split,
                "embargo_reason": None,
                "negative_guard_matched": True,
                "safe_full_tag": False,
            }
        )
    _write_jsonl(source / "image_manifest.jsonl", manifest_rows)
    _write_jsonl(source / "label_ledger.jsonl", label_rows)
    _write_jsonl(source / "split_assignment_ledger.jsonl", split_rows)
    _write_json(source / "image_renderer_contract.json", {"future_labels_used_in_image_rendering": False})
    _write_json(source / "label_contract.json", {"labels_used_in_candidate_key": False})
    _write_json(source / "split_contract.json", {"split_created": True})
    _write_json(source / "split_leakage_audit.json", {"split_leakage_audit_passed": True, "feature_window_crosses_prior_split_boundary": True, "past_only_feature_window_overlap_allowed": True, "future_label_window_overlap_train_validation": False, "future_label_window_overlap_validation_test": False})
    _write_json(source / "phase2_readiness_report.json", {"ready_for_phase2": True, "image_renderable_event_count": 6, "image_renderable_event_rate": 1.0, "deterministic_hash_pass_rate": 1.0, "split_leakage_audit_passed": True})
    _write_json(source / "research_decision.json", {"decision": "keep_candidate", "authoritative_research_decision": "image_assisted_phase0_1_ready_for_phase2", "silent_fallback_used": False, "research_fallback_used": False})
    _write_json(source / "_ARTIFACT_COMPLETE.json", {"complete": True, "silent_fallback_used": False, "research_fallback_used": False})
    return source


def _write_phase2_source(root: Path, *, decision: str = "image_only_classifier_phase2_failed") -> Path:
    source = root / "phase2" / "phase2-run"
    _write_jsonl(source / "image_score_ledger.jsonl", [{"image_sample_key": "k1", "image_score": 0.5}])
    _write_json(source / "classifier_metrics.json", {"test_roc_auc": 0.5015, "test_mcc": -0.0089})
    _write_json(source / "score_distribution_report.json", {"test_score_separation_top15_vs_bottom15": 0.00159})
    _write_json(source / "topk_proxy_report.json", {"test_image_only_top3_avg_ret20": 0.0157, "test_random_top3_avg_ret20": 0.0141})
    _write_json(source / "negative_guard_image_diagnostics.json", {"negative_guard_classifier_auc": 0.4892, "negative_guard_score_separation": -0.00834})
    _write_json(source / "phase3_readiness_report.json", {"ready_for_fusion": False})
    _write_json(source / "research_decision.json", {"decision": "drop", "authoritative_research_decision": decision, "silent_fallback_used": False, "research_fallback_used": False})
    _write_json(source / "_ARTIFACT_COMPLETE.json", {"complete": True, "silent_fallback_used": False, "research_fallback_used": False})
    return source


def test_validate_phase2_source_rejects_nonfailed_logistic_result(tmp_path: Path) -> None:
    source = _write_phase2_source(tmp_path, decision="image_only_classifier_phase2_hold")

    try:
        mod.validate_phase2_source(source)
    except RuntimeError as exc:
        assert "image_only_classifier_phase2_failed" in str(exc)
    else:
        raise AssertionError("nonfailed logistic source must be rejected")


def test_dependency_audit_disables_sklearn_fallback() -> None:
    audit = mod.audit_dependencies()

    assert audit["sklearn_fallback_allowed"] is False
    assert audit["sklearn_fallback_used"] is False
    assert audit["production_runtime_requirements_modified"] is False
    assert audit["meemee_runtime_dependencies_modified"] is False


def test_phase2b_dependency_blocked_run_writes_hold_artifacts(tmp_path: Path, monkeypatch) -> None:
    phase0 = _write_phase0_source(tmp_path)
    phase2 = _write_phase2_source(tmp_path)
    monkeypatch.setattr(mod, "audit_dependencies", _dependency_blocked_payload)

    result = mod.run_image_cnn_baseline_phase2b(
        source_image_phase0_1_run_id=phase0.name,
        source_image_phase2_run_id=phase2.name,
        source_image_phase0_1_root=phase0.parent,
        source_image_phase2_root=phase2.parent,
        output_root=tmp_path / "out",
        run_id="phase2b-smoke",
    )
    output_dir = Path(result["output_dir"])
    for artifact in mod.REQUIRED_ARTIFACTS:
        assert (output_dir / artifact).exists(), artifact

    complete = json.loads((output_dir / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    decision = json.loads((output_dir / "research_decision.json").read_text(encoding="utf-8"))
    dependency = json.loads((output_dir / "dependency_audit.json").read_text(encoding="utf-8"))
    model = json.loads((output_dir / "phase2b_model_contract.json").read_text(encoding="utf-8"))
    readiness = json.loads((output_dir / "phase3_readiness_report.json").read_text(encoding="utf-8"))
    score_ledger = (output_dir / "cnn_image_score_ledger.jsonl").read_text(encoding="utf-8")

    assert complete["complete"] is True
    assert complete["silent_fallback_used"] is False
    assert complete["research_fallback_used"] is False
    assert decision["decision"] == "hold"
    assert decision["authoritative_research_decision"] == "image_cnn_phase2b_hold"
    assert decision["image_model_trained"] is False
    assert decision["fusion_reranker_created"] is False
    assert decision["production_ranking_changed"] is False
    assert decision["meemee_reflectable"] is False
    assert decision["silent_fallback_used"] is False
    assert decision["research_fallback_used"] is False
    assert dependency["torch_available"] is False
    assert dependency["sklearn_fallback_used"] is False
    assert model["blocked_before_training"] is True
    assert readiness["ready_for_fusion"] is False
    assert score_ledger == ""
    assert result["ready_for_fusion"] is False
