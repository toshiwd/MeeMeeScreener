from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts import tradex_ranking_loss_or_topk_objective_repair_v1 as mod


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")


def _event_row(year: int, day: int, slot: int) -> dict[str, object]:
    event_date = f"{year}-01-{day:02d}"
    base = {
        "code": f"T{year}{day:02d}{slot}",
        "event_date": event_date,
        "event_month": event_date[:7],
        "pre_candle_energy_state": "pre_candle_energy_mixed",
        "pre_wick_warning_state": "pre_wicks_clean",
        "pre_volume_state": "pre_volume_normal",
        "pre_compression_state": "pre_range_normal",
        "event_daily_candle_state": "daily_strong_bull",
        "weekly_prior_state": "weekly_prior_mixed",
        "monthly_prior_state": "monthly_prior_uptrend",
        "win20": True,
        "severe_loss20": False,
        "event_strength_score": 3.0,
    }
    if slot == 0:
        base.update(
            {
                "pre_ret20_state": "pre20_up",
                "pre_ret5_state": "pre5_up",
                "pre_ma20_path_state": "pre_ma20_reclaim_base",
                "pre_ma60_context_state": "pre_ma60_near_or_above",
                "pre_volume_state": "pre_volume_expansion",
                "event_daily_ret20_state": "daily20_up",
                "ret20_fwd": 0.12,
                "mfe20": 0.18,
                "mae20": -0.025,
                "event_strength_score": 8.0,
            }
        )
    elif slot == 1:
        base.update(
            {
                "pre_ret20_state": "pre20_strong_up",
                "pre_ret5_state": "pre5_strong_up",
                "pre_ma20_path_state": "pre_ma20_already_extended",
                "pre_ma60_context_state": "pre_ma60_extended_above",
                "pre_wick_warning_state": "pre_upper_wick_or_failed_push",
                "pre_volume_state": "pre_volume_expansion",
                "pre_compression_state": "pre_range_wide",
                "weekly_prior_state": "weekly_prior_strong_up",
                "monthly_prior_state": "monthly_prior_strong_up",
                "event_daily_ret20_state": "daily20_strong_up",
                "ret20_fwd": -0.08,
                "mfe20": 0.04,
                "mae20": -0.13,
                "win20": False,
                "severe_loss20": True,
                "event_strength_score": 9.0,
            }
        )
    elif slot == 2:
        base.update(
            {
                "pre_ret20_state": "pre20_flat",
                "pre_ret5_state": "pre5_flat",
                "pre_ma20_path_state": "pre_ma20_reclaim_base",
                "pre_ma60_context_state": "pre_ma60_near_or_above",
                "event_daily_ret20_state": "daily20_flat",
                "ret20_fwd": 0.03,
                "mfe20": 0.06,
                "mae20": -0.04,
                "event_strength_score": 4.0,
            }
        )
    else:
        base.update(
            {
                "pre_ret20_state": "pre20_down",
                "pre_ret5_state": "pre5_down",
                "pre_ma20_path_state": "pre_ma20_below_base",
                "pre_ma60_context_state": "pre_ma60_below",
                "weekly_prior_state": "weekly_prior_downtrend",
                "monthly_prior_state": "monthly_prior_mixed",
                "event_daily_ret20_state": "daily20_down",
                "ret20_fwd": -0.02,
                "mfe20": 0.02,
                "mae20": -0.06,
                "win20": False,
                "event_strength_score": 1.0,
            }
        )
    return base


def _events() -> list[dict[str, object]]:
    rows = []
    for year in range(2018, 2024):
        for day in range(1, 8):
            for slot in range(5):
                rows.append(_event_row(year, day, slot))
    return rows


def _write_source_tree(root: Path) -> dict[str, Path]:
    pattern = root / "pattern" / "pattern-run"
    guard = root / "guard" / "guard-run"
    upside = root / "upside" / "upside-run"
    wide = root / "wide" / "wide-run"
    risk = root / "risk" / "risk-run"
    threshold = root / "threshold" / "threshold-run"
    feature = root / "feature" / "feature-run"
    image2 = root / "image2" / "image2-run"
    imagecnn = root / "imagecnn" / "imagecnn-run"

    _write_json(pattern / "_ARTIFACT_COMPLETE.json", {"complete": True, "silent_fallback_used": False})
    _write_json(pattern / "evaluation_contract.json", {"axis_id": "pre_strength_pattern_mining_v1"})
    _write_json(pattern / "run_manifest.json", {"schema_version": "tradex_research_run_manifest_v1"})
    _write_json(pattern / "feature_availability_audit.json", {"used_future_labels_in_pattern_keys": False})
    _write_json(pattern / "research_decision.json", {"authoritative_research_decision": "promising_pre_strength_patterns_found", "silent_fallback_used": False})
    _write_jsonl(pattern / "pre_strength_event_ledger.jsonl", _events())

    for name, payload in {
        "_ARTIFACT_COMPLETE.json": {"complete": True, "silent_fallback_used": False},
        "evaluation_contract.json": {"axis_id": "pre_strength_guard_validation_v1"},
        "source_artifact_refs.json": {"refs": []},
        "positive_guard_report.json": {"primary_positive_guard_id": "safe_full"},
        "negative_guard_report.json": {"primary_negative_guard_id": "already_extended_strong_up_blowoff_veto"},
        "topk_rotation_proxy_metrics.json": {"topk_rotation_proxy_available": False},
        "research_decision.json": {"authoritative_research_decision": "pre_strength_guard_hold", "silent_fallback_used": False},
    }.items():
        _write_json(guard / name, payload)

    _write_json(upside / "_ARTIFACT_COMPLETE.json", {"complete": True, "silent_fallback_used": False})
    _write_json(upside / "research_decision.json", {"authoritative_research_decision": "upside_capture_failed", "silent_fallback_used": False})
    _write_json(upside / "ranking_coverage_audit.json", {"complete_champion_ranking_available": False})

    _write_json(wide / "_ARTIFACT_COMPLETE.json", {"complete": True, "decision": "hold", "authoritative_research_decision": "wide_strength_pool_upside_rerank_hold", "silent_fallback_used": False})
    _write_json(wide / "research_decision.json", {"decision": "hold", "authoritative_research_decision": "wide_strength_pool_upside_rerank_hold", "best_research_family_id": "momentum_continuation_soft_boost_v1", "silent_fallback_used": False})
    _write_json(wide / "top3_selection_report.json", {"rows": []})
    _write_json(wide / "score_leaderboard.json", {"rows": []})
    _write_json(wide / "ranking_coverage_audit.json", {"complete_champion_ranking_available": False})

    _write_json(risk / "_ARTIFACT_COMPLETE.json", {"complete": True, "decision": "drop", "authoritative_research_decision": "selection_risk_control_drop", "silent_fallback_used": False, "research_fallback_used": False})
    _write_json(risk / "research_decision.json", {"decision": "drop", "authoritative_research_decision": "selection_risk_control_drop", "silent_fallback_used": False, "research_fallback_used": False})
    _write_json(risk / "risk_leaderboard.json", {"rows": []})

    refs = []
    for source, directory in [("pattern", pattern), ("guard", guard), ("upside", upside), ("wide", wide), ("risk", risk)]:
        refs.append({"source": source, "name": "_ARTIFACT_COMPLETE.json", "path": str(directory / "_ARTIFACT_COMPLETE.json"), "exists": True})
    _write_json(threshold / "_ARTIFACT_COMPLETE.json", {"complete": True, "decision": "drop", "authoritative_research_decision": "threshold_no_trade_control_drop", "silent_fallback_used": False, "research_fallback_used": False})
    _write_json(threshold / "research_decision.json", {"decision": "drop", "authoritative_research_decision": "threshold_no_trade_control_drop", "silent_fallback_used": False, "research_fallback_used": False})
    _write_json(threshold / "threshold_leaderboard.json", {"rows": []})
    _write_json(threshold / "source_artifact_refs.json", {"refs": refs})

    _write_json(feature / "_ARTIFACT_COMPLETE.json", {"complete": True, "decision": "hold", "authoritative_research_decision": "winner_nonwinner_feature_diagnosis_hold", "silent_fallback_used": False, "research_fallback_used": False})
    _write_json(feature / "research_decision.json", {"decision": "hold", "authoritative_research_decision": "winner_nonwinner_feature_diagnosis_hold", "silent_fallback_used": False, "research_fallback_used": False})
    _write_json(
        feature / "candidate_feature_shortlist.json",
        {
            "rows": [
                {"feature_id": "same_date_score_rank", "recommended_for_next_scorer": True, "leakage_safe": True},
                {"feature_id": "score_momentum_continuation_soft_boost_v1", "recommended_for_next_scorer": True, "leakage_safe": True},
            ]
        },
    )

    _write_json(image2 / "_ARTIFACT_COMPLETE.json", {"complete": True, "decision": "drop", "authoritative_research_decision": "image_only_classifier_phase2_failed", "silent_fallback_used": False, "research_fallback_used": False})
    _write_json(image2 / "research_decision.json", {"decision": "drop", "authoritative_research_decision": "image_only_classifier_phase2_failed", "silent_fallback_used": False, "research_fallback_used": False})
    _write_json(image2 / "phase3_readiness_report.json", {"ready_for_fusion": False})
    _write_json(image2 / "classifier_metrics.json", {"test_roc_auc": 0.5015, "test_mcc": -0.0089})
    _write_json(image2 / "topk_proxy_report.json", {"test_image_only_top3_avg_ret20": 0.0157})
    _write_json(image2 / "negative_guard_image_diagnostics.json", {"negative_guard_classifier_auc": 0.4892})

    _write_json(imagecnn / "_ARTIFACT_COMPLETE.json", {"complete": True, "decision": "drop", "authoritative_research_decision": "image_cnn_phase2b_failed", "silent_fallback_used": False, "research_fallback_used": False})
    _write_json(imagecnn / "research_decision.json", {"decision": "drop", "authoritative_research_decision": "image_cnn_phase2b_failed", "silent_fallback_used": False, "research_fallback_used": False})
    _write_json(imagecnn / "phase3_readiness_report.json", {"ready_for_fusion": False})
    _write_json(imagecnn / "classifier_metrics.json", {"test_roc_auc": 0.51737, "test_mcc": 0.0})
    _write_json(imagecnn / "topk_proxy_report.json", {"test_cnn_top3_avg_ret20": 0.02119})
    _write_json(imagecnn / "negative_guard_cnn_diagnostics.json", {"negative_guard_classifier_auc": 0.52620})

    return {
        "wide": wide,
        "risk": risk,
        "threshold": threshold,
        "feature": feature,
        "image2": image2,
        "imagecnn": imagecnn,
    }


def test_feature_input_contract_excludes_future_and_image_inputs(tmp_path: Path) -> None:
    events = pd.DataFrame(
        {
            "score_momentum_continuation_soft_boost_v1": [1.0, 2.0],
            "same_date_score_rank": [1.0, 2.0],
            "ret20_fwd": [0.1, -0.1],
            "image_score": [0.9, 0.1],
        }
    )
    contract = mod.build_feature_input_contract(
        events,
        {
            "rows": [
                {"feature_id": "ret20_fwd", "recommended_for_next_scorer": True, "leakage_safe": True},
                {"feature_id": "image_score", "recommended_for_next_scorer": True, "leakage_safe": True},
                {"feature_id": "same_date_score_rank", "recommended_for_next_scorer": True, "leakage_safe": True},
            ]
        },
    )

    assert "same_date_score_rank" in contract["used_features"]
    assert "ret20_fwd" not in contract["used_features"]
    assert "image_score" not in contract["used_features"]
    assert contract["future_labels_used_in_score_inputs"] is False
    assert contract["image_score_used"] is False
    assert contract["cnn_score_used"] is False


def test_ranking_objective_run_writes_required_artifacts(tmp_path: Path) -> None:
    dirs = _write_source_tree(tmp_path)
    result = mod.run_ranking_loss_or_topk_objective_repair_v1(
        source_wide_run_id=dirs["wide"].name,
        source_risk_run_id=dirs["risk"].name,
        source_threshold_run_id=dirs["threshold"].name,
        source_feature_diagnosis_run_id=dirs["feature"].name,
        source_image_phase2_run_id=dirs["image2"].name,
        source_image_cnn_phase2b_run_id=dirs["imagecnn"].name,
        wide_root=dirs["wide"].parent,
        risk_root=dirs["risk"].parent,
        threshold_root=dirs["threshold"].parent,
        feature_diagnosis_root=dirs["feature"].parent,
        image_phase2_root=dirs["image2"].parent,
        image_cnn_phase2b_root=dirs["imagecnn"].parent,
        output_root=tmp_path / "out",
        run_id="ranking-smoke",
    )
    output_dir = Path(result["output_dir"])

    for artifact in mod.REQUIRED_ARTIFACTS:
        assert (output_dir / artifact).exists(), artifact

    complete = json.loads((output_dir / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    decision = json.loads((output_dir / "research_decision.json").read_text(encoding="utf-8"))
    leakage = json.loads((output_dir / "leakage_audit.json").read_text(encoding="utf-8"))
    objective = json.loads((output_dir / "ranking_objective_contract.json").read_text(encoding="utf-8"))
    feature_contract = json.loads((output_dir / "feature_input_contract.json").read_text(encoding="utf-8"))
    score_rows = (output_dir / "ranker_score_ledger.jsonl").read_text(encoding="utf-8").splitlines()

    assert complete["complete"] is True
    assert complete["silent_fallback_used"] is False
    assert complete["research_fallback_used"] is False
    assert decision["ranking_objective_created"] is True
    assert decision["candidate_scoring_scope"] == "research_only"
    assert decision["production_ranking_changed"] is False
    assert decision["publish_bundle_created"] is False
    assert decision["meemee_reflectable"] is False
    assert decision["image_route_paused"] is True
    assert decision["image_score_used"] is False
    assert decision["cnn_score_used"] is False
    assert decision["fusion_reranker_created"] is False
    assert decision["safe_full_used_as_hard_filter"] is False
    assert decision["negative_guard_used_as_hard_veto"] is False
    assert leakage["future_labels_used_in_score_inputs"] is False
    assert leakage["split_leakage_audit_passed"] is True
    assert objective["classification_objective_used"] is False
    assert "ret20_fwd" not in feature_contract["used_features"]
    assert score_rows
    assert result["production_ranking_changed"] is False
