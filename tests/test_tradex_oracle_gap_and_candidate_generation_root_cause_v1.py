from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts import tradex_oracle_gap_and_candidate_generation_root_cause_v1 as mod


def _event_rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    specs = [
        ("A", 0.12, 0.18, -0.02, False, 1),
        ("B", -0.08, 0.03, -0.13, True, 2),
        ("C", 0.02, 0.06, -0.04, False, 3),
        ("D", -0.01, 0.02, -0.05, False, 4),
    ]
    for day in range(1, 5):
        event_date = f"2020-01-{day:02d}"
        for suffix, ret20, mfe20, mae20, severe, rank in specs:
            rows.append(
                {
                    "event_date": event_date,
                    "event_ymd": int(event_date.replace("-", "")),
                    "code": f"T{day}{suffix}",
                    "ret20_fwd": ret20,
                    "mfe20": mfe20,
                    "mae20": mae20,
                    "win20": ret20 > 0,
                    "severe_loss20": severe,
                    "ret20_rank_by_date": rank,
                    "ret20_rank_pct_by_date": rank / 4.0,
                    "is_future_top10_by_ret20": rank == 1,
                    "is_future_top5_by_ret20": rank == 1,
                    "is_big_winner_ret20_ge_10pct": ret20 >= 0.10,
                    "is_big_winner_MFE20_ge_15pct": mfe20 >= 0.15,
                    "pre_ret20_state": "pre20_strong_up" if suffix == "B" else "pre20_up",
                    "pre_ret5_state": "pre5_strong_up" if suffix == "B" else "pre5_up",
                    "pre_ma20_path_state": "pre_ma20_already_extended" if suffix == "B" else "pre_ma20_reclaim_base",
                    "pre_ma60_context_state": "pre_ma60_extended_above" if suffix == "B" else "pre_ma60_near_or_above",
                    "pre_candle_energy_state": "pre_candle_energy_mixed",
                    "pre_wick_warning_state": "pre_upper_wick_or_failed_push" if suffix == "B" else "pre_wicks_clean",
                    "pre_volume_state": "pre_volume_expansion",
                    "pre_compression_state": "pre_range_wide" if suffix == "B" else "pre_range_normal",
                    "weekly_prior_state": "weekly_prior_strong_up" if suffix == "B" else "weekly_prior_mixed",
                    "monthly_prior_state": "monthly_prior_strong_up" if suffix == "B" else "monthly_prior_uptrend",
                    "event_daily_ret20_state": "daily20_strong_up" if suffix == "A" else "daily20_down",
                    "event_daily_candle_state": "daily_strong_bull",
                    "negative_guard_match": suffix == "B",
                    "guard_safe_full": suffix == "A",
                }
            )
    return rows


def _rank_for_family(family_id: str, suffix: str, true_rank: int) -> int:
    if family_id == mod.ORACLE_FAMILY_ID:
        return true_rank
    if family_id == mod.BASELINE_FAMILY_ID:
        return {"B": 1, "C": 2, "D": 3, "A": 4}[suffix]
    if family_id == mod.LISTWISE_FAMILY_ID:
        return {"C": 1, "B": 2, "D": 3, "A": 4}[suffix]
    return {"B": 1, "D": 2, "C": 3, "A": 4}[suffix]


def _ledger_rows(events: list[dict[str, object]]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    families = [
        mod.BASELINE_FAMILY_ID,
        mod.ORACLE_FAMILY_ID,
        mod.PAIRWISE_FAMILY_ID,
        mod.LISTWISE_FAMILY_ID,
        mod.BADPICK_FAMILY_ID,
    ]
    for event in events:
        suffix = str(event["code"])[-1]
        for family_id in families:
            rank = _rank_for_family(family_id, suffix, int(event["ret20_rank_by_date"]))
            rows.append(
                {
                    "ranker_family_id": family_id,
                    "event_date": event["event_date"],
                    "event_ymd": event["event_ymd"],
                    "code": event["code"],
                    "selection_rank": rank,
                    "ret20_fwd": event["ret20_fwd"],
                    "mfe20": event["mfe20"],
                    "mae20": event["mae20"],
                    "win20": event["win20"],
                    "severe_loss20": event["severe_loss20"],
                    "is_future_top10_by_ret20": event["is_future_top10_by_ret20"],
                    "is_future_top5_by_ret20": event["is_future_top5_by_ret20"],
                    "is_big_winner_ret20_ge_10pct": event["is_big_winner_ret20_ge_10pct"],
                    "is_big_winner_MFE20_ge_15pct": event["is_big_winner_MFE20_ge_15pct"],
                    "negative_guard_match": event["negative_guard_match"],
                    "guard_safe_full": event["guard_safe_full"],
                }
            )
    return rows


def _source_status() -> dict[str, object]:
    return {
        "ranking_objective": {
            "research_decision.json": {
                "authoritative_research_decision": "ranking_objective_drop",
                "best_ranker_family_id": mod.LISTWISE_FAMILY_ID,
            }
        }
    }


def test_failure_mode_and_hypothesis_map_are_diagnosis_only() -> None:
    events = pd.DataFrame(_event_rows())
    ledger = pd.DataFrame(_ledger_rows(_event_rows()))

    oracle_gap = mod.build_oracle_gap_decomposition(ledger, {"best_ranker_family_id": mod.LISTWISE_FAMILY_ID})
    pool = mod.build_candidate_pool_recall_report(events)
    ranking = mod.build_within_pool_ranking_failure_report(ledger, mod.LISTWISE_FAMILY_ID)
    timeblock = mod.build_regime_timeblock_decomposition_report(events, ledger, mod.LISTWISE_FAMILY_ID)
    max3 = mod.build_max3_deployment_fit_report(events, ledger)
    label = mod.build_label_objective_mismatch_report(events, ledger)
    failure = mod.build_failure_mode_classification(
        oracle_gap=oracle_gap,
        pool_recall=pool,
        ranking_failure=ranking,
        timeblock=timeblock,
        max3=max3,
        label_mismatch=label,
    )
    hypotheses = mod.build_candidate_generation_hypothesis_map(failure, {}, max3, label)

    assert oracle_gap["full_oracle_available"] is False
    assert ranking["ranking_failure_present"] is True
    assert failure["root_failure_mode_identified"] is True
    assert hypotheses["candidate_generation_hypothesis_map_created"] is True
    assert 1 <= hypotheses["hypothesis_count"] <= 2
    for hypothesis in hypotheses["hypotheses"]:
        assert "safe_full hard filter" in hypothesis["forbidden_shortcuts"]
        assert "negative_guard hard veto" in hypothesis["forbidden_shortcuts"]


def test_oracle_gap_root_cause_run_writes_required_artifacts(tmp_path: Path, monkeypatch) -> None:
    events = pd.DataFrame(_event_rows())
    ledger = pd.DataFrame(_ledger_rows(_event_rows()))

    def fake_validate_sources(**_kwargs):
        return _source_status()

    def fake_load_diagnosis_inputs(**_kwargs):
        return events.copy(), ledger.copy()

    monkeypatch.setattr(mod, "validate_sources", fake_validate_sources)
    monkeypatch.setattr(mod, "load_diagnosis_inputs", fake_load_diagnosis_inputs)

    result = mod.run_oracle_gap_and_candidate_generation_root_cause_v1(
        source_wide_run_id="wide-run",
        source_risk_run_id="risk-run",
        source_threshold_run_id="threshold-run",
        source_feature_diagnosis_run_id="feature-run",
        source_image_phase2_run_id="image2-run",
        source_image_cnn_phase2b_run_id="imagecnn-run",
        source_ranking_objective_run_id="ranking-run",
        wide_root=tmp_path / "wide",
        risk_root=tmp_path / "risk",
        threshold_root=tmp_path / "threshold",
        feature_diagnosis_root=tmp_path / "feature",
        image_phase2_root=tmp_path / "image2",
        image_cnn_phase2b_root=tmp_path / "imagecnn",
        ranking_objective_root=tmp_path / "ranking",
        output_root=tmp_path / "out",
        run_id="root-cause-smoke",
    )
    output_dir = Path(result["output_dir"])
    for artifact in mod.REQUIRED_ARTIFACTS:
        assert (output_dir / artifact).exists(), artifact

    complete = json.loads((output_dir / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    decision = json.loads((output_dir / "research_decision.json").read_text(encoding="utf-8"))
    hypotheses = json.loads((output_dir / "candidate_generation_hypothesis_map.json").read_text(encoding="utf-8"))
    next_axis = json.loads((output_dir / "next_axis_recommendation.json").read_text(encoding="utf-8"))

    assert complete["complete"] is True
    assert complete["silent_fallback_used"] is False
    assert complete["research_fallback_used"] is False
    assert decision["diagnosis_created"] is True
    assert decision["candidate_generation_hypothesis_map_created"] is True
    assert decision["candidate_scoring_created"] is False
    assert decision["ranking_objective_created"] is False
    assert decision["threshold_policy_created"] is False
    assert decision["image_score_used"] is False
    assert decision["fusion_reranker_created"] is False
    assert decision["production_ranking_changed"] is False
    assert decision["meemee_reflectable"] is False
    assert hypotheses["hypothesis_count"] in (1, 2)
    assert next_axis["one_recommended_next_axis_only"] is True
    assert result["candidate_scoring_created"] is False
