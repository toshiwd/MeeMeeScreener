from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts import tradex_missed_winner_event_source_candidate_generation_v1 as mod


def _event_rows(day_count: int = 25) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    specs = [
        ("A", "winner_source", 0.13, 0.19, -0.02, False, 1),
        ("B", "noisy_source", -0.07, 0.03, -0.13, True, 2),
        ("C", "neutral_source", 0.01, 0.04, -0.04, False, 3),
        ("D", "neutral_source", -0.01, 0.02, -0.05, False, 4),
    ]
    for day in range(1, day_count + 1):
        event_date = f"2020-01-{day:02d}"
        for suffix, source, ret20, mfe20, mae20, severe, rank in specs:
            rows.append(
                {
                    "event_date": event_date,
                    "event_ymd": int(event_date.replace("-", "")),
                    "code": f"T{day:03d}{suffix}",
                    "ret20_fwd": ret20,
                    "mfe20": mfe20,
                    "mae20": mae20,
                    "win20": ret20 > 0,
                    "severe_loss20": severe,
                    "ret20_rank_by_date": rank,
                    "ret20_rank_pct_by_date": rank / 4.0,
                    "is_future_top10_by_ret20": suffix == "A",
                    "is_future_top5_by_ret20": suffix == "A",
                    "is_big_winner_ret20_ge_10pct": ret20 >= 0.10,
                    "is_big_winner_MFE20_ge_15pct": mfe20 >= 0.15,
                    "pre_ret20_state": "source_pre20_up" if source == "winner_source" else "source_pre20_extended",
                    "pre_ret5_state": "source_pre5_up" if source == "winner_source" else "source_pre5_exhausted",
                    "pre_ma20_path_state": "source_ma20_reclaim" if source == "winner_source" else source,
                    "pre_ma60_context_state": "source_ma60_near" if source == "winner_source" else "source_ma60_extended",
                    "pre_candle_energy_state": "source_candle_clean",
                    "pre_wick_warning_state": "source_wicks_clean" if source == "winner_source" else "source_upper_wick",
                    "pre_volume_state": "source_volume_expansion",
                    "pre_compression_state": "source_range_normal",
                    "weekly_prior_state": "source_weekly_constructive" if source == "winner_source" else "source_weekly_hot",
                    "monthly_prior_state": "source_monthly_up",
                    "event_daily_ret20_state": "daily20_up",
                    "event_daily_candle_state": "daily_bull",
                    "negative_guard_match": source == "noisy_source",
                    "guard_safe_full": source == "winner_source",
                }
            )
    return rows


def _ledger_rows(events: list[dict[str, object]]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for event in events:
        suffix = str(event["code"])[-1]
        rank = {"B": 1, "C": 2, "D": 3, "A": 4}[suffix]
        rows.append(
            {
                "ranker_family_id": mod.BASELINE_FAMILY_ID,
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


def _source_status(tmp_path: Path | None = None) -> dict[str, object]:
    root = tmp_path or Path(".")
    return {
        "root_cause": {
            "research_decision.json": {
                "authoritative_research_decision": "root_cause_identified_next_axis_ready",
                "recommended_next_axis": mod.AXIS_ID,
            }
        },
        "feature_diagnosis": {
            "candidate_feature_shortlist.json": {"recommended_feature_count": 8},
        },
        "risk_dir": root / "risk",
        "threshold_dir": root / "threshold",
        "ranking_objective_dir": root / "ranking",
    }


def test_event_source_hypothesis_map_is_diagnosis_only() -> None:
    events = pd.DataFrame(_event_rows())
    ledger = pd.DataFrame(_ledger_rows(_event_rows()))
    frame = mod.prepare_event_source_frame(events, ledger)

    missed = mod.build_missed_winner_source_decomposition(frame)
    selected_nonwinner = mod.build_selected_nonwinner_source_decomposition(frame)
    quality = mod.build_event_source_quality_leaderboard(frame)
    same_date = mod.build_same_date_source_miss_report(frame)
    max3 = mod.build_max3_source_structure_report(frame)
    stability = mod.build_time_block_source_stability(frame)
    failure = mod.build_source_failure_mode_classification(
        missed=missed,
        quality=quality,
        same_date=same_date,
        max3=max3,
        stability=stability,
    )
    hypotheses = mod.build_candidate_generation_hypothesis_map(
        missed=missed,
        quality=quality,
        same_date=same_date,
        stability=stability,
        source_failure=failure,
    )

    assert missed["missed_winner_total_count"] == 25
    assert selected_nonwinner["selected_nonwinner_count"] > 0
    assert same_date["winner_source_present_but_under_ranked_rate"] == 1.0
    assert failure["source_under_ranked_count"] >= 1
    assert hypotheses["hypothesis_count"] in (1, 2)
    assert hypotheses["hypotheses"][0]["testable_next_axis"] == "source_specific_candidate_generation_validation_v1"
    assert "safe_full hard filter" in hypotheses["hypotheses"][0]["forbidden_shortcuts"]
    assert "negative_guard hard veto" in hypotheses["hypotheses"][0]["forbidden_shortcuts"]


def test_missed_winner_event_source_run_writes_required_artifacts(tmp_path: Path, monkeypatch) -> None:
    events = pd.DataFrame(_event_rows())
    ledger = pd.DataFrame(_ledger_rows(_event_rows()))

    def fake_validate_sources(**_kwargs):
        return _source_status(tmp_path)

    def fake_load_diagnosis_inputs(**_kwargs):
        return mod.prepare_event_source_frame(events.copy(), ledger.copy()), ledger.copy()

    monkeypatch.setattr(mod, "validate_sources", fake_validate_sources)
    monkeypatch.setattr(mod, "load_diagnosis_inputs", fake_load_diagnosis_inputs)

    result = mod.run_missed_winner_event_source_candidate_generation_v1(
        source_root_cause_run_id="root-cause-run",
        source_wide_run_id="wide-run",
        source_pattern_run_id="pattern-run",
        source_upside_run_id="upside-run",
        source_feature_diagnosis_run_id="feature-run",
        root_cause_root=tmp_path / "root",
        wide_root=tmp_path / "wide",
        pattern_root=tmp_path / "pattern",
        upside_root=tmp_path / "upside",
        feature_diagnosis_root=tmp_path / "feature",
        output_root=tmp_path / "out",
        run_id="missed-winner-source-smoke",
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
    assert decision["event_source_decomposition_created"] is True
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
