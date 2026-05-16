from __future__ import annotations

import json
from pathlib import Path

from PIL import Image, ImageDraw

from scripts import tradex_image_only_classifier_baseline_phase2 as mod


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")


def _draw_signal_image(path: Path, label: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    image = Image.new("RGB", (224, 224), (255, 255, 255))
    draw = ImageDraw.Draw(image)
    if label == "future_top15_by_ret20":
        draw.line((18, 186, 206, 38), fill=(0, 0, 0), width=8)
        draw.rectangle((150, 30, 200, 70), fill=(20, 20, 20))
    elif label == "future_bottom15_by_ret20":
        draw.line((18, 38, 206, 186), fill=(0, 0, 0), width=8)
        draw.rectangle((20, 150, 70, 190), fill=(20, 20, 20))
    else:
        draw.line((18, 112, 206, 112), fill=(80, 80, 80), width=6)
    image.save(path, format="PNG")


def _write_phase0_source(root: Path) -> Path:
    source = root / "phase0" / "phase0-run"
    image_root = source / "images" / "224"
    manifest_rows: list[dict[str, object]] = []
    label_rows: list[dict[str, object]] = []
    split_rows: list[dict[str, object]] = []
    date_idx = 0
    for split, date_count in [("train", 30), ("validation", 10), ("test", 10)]:
        for _ in range(date_count):
            date_idx += 1
            event_ymd = 20200100 + date_idx
            event_date = f"2020-01-{date_idx:02d}" if date_idx <= 31 else f"2020-02-{date_idx - 31:02d}"
            for label_idx, label in enumerate(["future_top15_by_ret20", "future_bottom15_by_ret20", "neutral_middle70"]):
                key = f"{split}-{date_idx}-{label_idx}"
                symbol = f"T{date_idx:03d}{label_idx}"
                image_path = image_root / f"{event_ymd}_{symbol}_{key}.png"
                _draw_signal_image(image_path, label)
                is_top = label == "future_top15_by_ret20"
                is_bottom = label == "future_bottom15_by_ret20"
                is_neutral = label == "neutral_middle70"
                manifest_rows.append(
                    {
                        "schema_version": "tradex_image_assisted_rerank_phase0_1_image_manifest_row_v1",
                        "image_sample_key": key,
                        "candidate_event_key": f"candidate-{key}",
                        "symbol": symbol,
                        "code": symbol,
                        "event_date": event_date,
                        "event_ymd": event_ymd,
                        "image_path": str(image_path),
                        "image_size": 224,
                        "image_format": "PNG",
                        "source_candidate_set": "wide_strength_pool_events",
                        "source_score_family_id": "score_extended_continuation_vs_blowoff_risk_v1",
                        "negative_guard_matched": label_idx != 2,
                        "safe_full_tag": label_idx == 2,
                        "prior_research_score_available": True,
                        "prior_risk_score_available": True,
                        "window_start_date": "2019-10-01",
                        "window_end_date": event_date,
                        "window_trading_day_count": 80,
                    }
                )
                label_rows.append(
                    {
                        "schema_version": "tradex_image_assisted_rerank_phase0_1_label_ledger_row_v1",
                        "image_sample_key": key,
                        "candidate_event_key": f"candidate-{key}",
                        "symbol": symbol,
                        "event_date": event_date,
                        "event_ymd": event_ymd,
                        "primary_label": label,
                        "future_top15_by_ret20": is_top,
                        "future_bottom15_by_ret20": is_bottom,
                        "neutral_middle70": is_neutral,
                        "ret20": 0.15 if is_top else -0.08 if is_bottom else 0.02,
                        "MFE20": 0.22 if is_top else 0.03 if is_bottom else 0.06,
                        "MAE20": -0.02 if is_top else -0.12 if is_bottom else -0.04,
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
                        "schema_version": "tradex_image_assisted_rerank_phase0_1_split_assignment_row_v1",
                        "image_sample_key": key,
                        "symbol": symbol,
                        "event_date": event_date,
                        "event_ymd": event_ymd,
                        "split": split,
                        "embargo_reason": None,
                        "negative_guard_matched": label_idx != 2,
                        "safe_full_tag": label_idx == 2,
                    }
                )
    _write_jsonl(source / "image_manifest.jsonl", manifest_rows)
    _write_jsonl(source / "label_ledger.jsonl", label_rows)
    _write_jsonl(source / "split_assignment_ledger.jsonl", split_rows)
    _write_json(source / "image_renderer_contract.json", {"window_trading_days": 80, "future_labels_used_in_image_rendering": False})
    _write_json(source / "label_contract.json", {"label_horizon_trading_days": 20, "labels_used_in_candidate_key": False})
    _write_json(source / "split_contract.json", {"split_created": True, "split_policy": "time_block_split_with_embargo"})
    _write_json(
        source / "split_leakage_audit.json",
        {
            "split_leakage_audit_passed": True,
            "feature_window_crosses_prior_split_boundary": True,
            "past_only_feature_window_overlap_allowed": True,
            "future_label_window_overlap_train_validation": False,
            "future_label_window_overlap_validation_test": False,
        },
    )
    _write_json(source / "phase2_readiness_report.json", {"ready_for_phase2": True, "image_renderable_event_count": len(manifest_rows), "image_renderable_event_rate": 1.0, "deterministic_hash_pass_rate": 1.0, "split_leakage_audit_passed": True})
    _write_json(source / "research_decision.json", {"decision": "keep_candidate", "authoritative_research_decision": "image_assisted_phase0_1_ready_for_phase2", "silent_fallback_used": False, "research_fallback_used": False})
    _write_json(source / "_ARTIFACT_COMPLETE.json", {"complete": True, "decision": "keep_candidate", "authoritative_research_decision": "image_assisted_phase0_1_ready_for_phase2", "silent_fallback_used": False, "research_fallback_used": False})
    return source


def test_phase0_source_validation_rejects_nonready_source(tmp_path: Path) -> None:
    source = _write_phase0_source(tmp_path)
    _write_json(source / "research_decision.json", {"decision": "hold", "authoritative_research_decision": "image_assisted_phase0_1_hold", "silent_fallback_used": False, "research_fallback_used": False})

    try:
        mod.validate_phase0_1_source(source)
    except RuntimeError as exc:
        assert "not ready_for_phase2" in str(exc)
    else:
        raise AssertionError("source validation should reject non-ready Phase0/1 artifacts")


def test_phase2_split_leakage_audit_uses_phase0_split_authority(tmp_path: Path) -> None:
    source = _write_phase0_source(tmp_path)
    frame, _ = mod.load_phase0_1_dataset(source)
    audit = mod.build_phase2_split_leakage_audit(frame, source)

    assert audit["split_source"] == "phase0_1_split_assignment_ledger"
    assert audit["split_regenerated"] is False
    assert audit["random_split_used"] is False
    assert audit["phase2_split_leakage_audit_passed"] is True
    assert audit["future_labels_used_as_inference_inputs"] is False


def test_image_only_classifier_phase2_run_writes_required_artifacts(tmp_path: Path) -> None:
    source = _write_phase0_source(tmp_path)
    result = mod.run_image_only_classifier_baseline_phase2(
        source_image_phase0_1_run_id=source.name,
        source_image_phase0_1_root=source.parent,
        output_root=tmp_path / "out",
        run_id="phase2-smoke",
    )
    output_dir = Path(result["output_dir"])
    for artifact in mod.REQUIRED_ARTIFACTS:
        assert (output_dir / artifact).exists(), artifact

    complete = json.loads((output_dir / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    decision = json.loads((output_dir / "research_decision.json").read_text(encoding="utf-8"))
    model_contract = json.loads((output_dir / "phase2_model_contract.json").read_text(encoding="utf-8"))
    label_audit = json.loads((output_dir / "label_usage_audit.json").read_text(encoding="utf-8"))
    classifier = json.loads((output_dir / "classifier_metrics.json").read_text(encoding="utf-8"))
    score_lines = (output_dir / "image_score_ledger.jsonl").read_text(encoding="utf-8").strip().splitlines()

    assert complete["complete"] is True
    assert complete["silent_fallback_used"] is False
    assert decision["image_model_trained"] is True
    assert decision["image_only_classifier_created"] is True
    assert decision["image_score_created"] is True
    assert decision["fusion_reranker_created"] is False
    assert decision["candidate_scoring_created"] is False
    assert decision["threshold_policy_created"] is False
    assert decision["production_ranking_changed"] is False
    assert decision["publish_bundle_created"] is False
    assert decision["meemee_reflectable"] is False
    assert decision["future_labels_used_as_inference_inputs"] is False
    assert label_audit["future_labels_used_as_training_targets_only"] is True
    assert model_contract["image_only"] is True
    assert classifier["test_roc_auc"] is not None
    assert len(score_lines) == 150
    assert result["fusion_reranker_created"] is False
