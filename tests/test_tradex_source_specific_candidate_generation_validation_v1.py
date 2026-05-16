from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts import tradex_missed_winner_event_source_candidate_generation_v1 as source_mod
from scripts import tradex_source_specific_candidate_generation_validation_v1 as mod


TARGET_SOURCE = (
    "pre_ma20_path_state=source_ma20_reclaim|"
    "pre_ma60_context_state=source_ma60_near|"
    "weekly_prior_state=source_weekly_constructive|"
    "negative_guard_match=False"
)


def _event_rows(day_count: int = 25) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    specs = [
        ("A", "target_source", 0.13, 0.19, -0.02, False, 1),
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
                    "pre_ret20_state": "source_pre20_up" if source == "target_source" else "source_pre20_extended",
                    "pre_ret5_state": "source_pre5_up" if source == "target_source" else "source_pre5_exhausted",
                    "pre_ma20_path_state": "source_ma20_reclaim" if source == "target_source" else source,
                    "pre_ma60_context_state": "source_ma60_near" if source == "target_source" else "source_ma60_extended",
                    "pre_candle_energy_state": "source_candle_clean",
                    "pre_wick_warning_state": "source_wicks_clean",
                    "pre_volume_state": "source_volume_expansion",
                    "pre_compression_state": "source_range_normal",
                    "weekly_prior_state": "source_weekly_constructive" if source == "target_source" else "source_weekly_hot",
                    "monthly_prior_state": "source_monthly_up",
                    "event_daily_ret20_state": "daily20_up",
                    "event_daily_candle_state": "daily_bull",
                    "negative_guard_match": False,
                    "guard_safe_full": source == "target_source",
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
        "missed_winner": {
            "research_decision.json": {
                "authoritative_research_decision": "missed_winner_source_hypothesis_ready",
                "recommended_next_axis": mod.AXIS_ID,
            },
            "candidate_generation_hypothesis_map.json": {
                "hypotheses": [
                    {
                        "hypothesis_id": "source_specific_candidate_generation_v1",
                        "source_family": TARGET_SOURCE,
                        "testable_next_axis": mod.AXIS_ID,
                    }
                ]
            },
        },
        "source_mod_status": {},
        "feature_diagnosis_dir": root / "feature",
    }


def _prepared_frame() -> pd.DataFrame:
    events = pd.DataFrame(_event_rows())
    ledger = pd.DataFrame(_ledger_rows(_event_rows()))
    frame = source_mod.prepare_event_source_frame(events, ledger)
    return mod._source_frame(frame, TARGET_SOURCE)


def test_source_slot_policy_recovers_under_ranked_winner_without_scorer() -> None:
    frame = _prepared_frame()
    primary = mod.build_selection_ledger(frame, max_source_slots=1, family_id=mod.PRIMARY_FAMILY_ID)
    primary_selected = mod._selected_from_ledger(primary)
    diagnostic = mod.build_selection_ledger(frame, max_source_slots=2, family_id=mod.DIAGNOSTIC_FAMILY_ID)
    diagnostic_selected = mod._selected_from_ledger(diagnostic)

    top3 = mod.build_top3_selection_report(frame, primary_selected, diagnostic_selected)
    recovery = mod.build_source_recovery_report(frame, primary_selected)
    noise = mod.build_source_noise_report(recovery)
    timeblock = mod.build_time_block_source_validation(frame, primary_selected)
    comparison = mod.build_baseline_comparison_report(top3, recovery, noise, timeblock)
    outcome = mod.build_validation_outcome_classification(
        mod.build_source_overlap_audit(frame),
        comparison,
        recovery,
        noise,
        timeblock,
    )

    assert top3["changed_top3_members_count_vs_previous_best"] > 0
    assert comparison["selected_top3_avg_ret20_delta_vs_previous_best"] > 0
    assert recovery["recovered_missed_winner_count"] == 25
    assert noise["severe_loser_added_per_recovered_winner"] == 0
    assert outcome["validation_outcome"] == "source_recovers_winners_cleanly"


def test_source_specific_validation_run_writes_required_artifacts(tmp_path: Path, monkeypatch) -> None:
    frame = _prepared_frame()
    ledger = pd.DataFrame(_ledger_rows(_event_rows()))

    def fake_validate_sources(**_kwargs):
        return _source_status(tmp_path)

    def fake_load_validation_inputs(**_kwargs):
        return frame.copy(), ledger.copy()

    monkeypatch.setattr(mod, "validate_sources", fake_validate_sources)
    monkeypatch.setattr(mod, "load_validation_inputs", fake_load_validation_inputs)

    result = mod.run_source_specific_candidate_generation_validation_v1(
        source_missed_winner_run_id="missed-run",
        source_root_cause_run_id="root-run",
        source_wide_run_id="wide-run",
        source_pattern_run_id="pattern-run",
        source_upside_run_id="upside-run",
        missed_winner_root=tmp_path / "missed",
        root_cause_root=tmp_path / "root",
        wide_root=tmp_path / "wide",
        pattern_root=tmp_path / "pattern",
        upside_root=tmp_path / "upside",
        output_root=tmp_path / "out",
        run_id="source-validation-smoke",
    )
    output_dir = Path(result["output_dir"])
    for artifact in mod.REQUIRED_ARTIFACTS:
        assert (output_dir / artifact).exists(), artifact

    complete = json.loads((output_dir / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    decision = json.loads((output_dir / "research_decision.json").read_text(encoding="utf-8"))
    top3 = json.loads((output_dir / "top3_selection_report.json").read_text(encoding="utf-8"))
    recovery = json.loads((output_dir / "source_recovery_report.json").read_text(encoding="utf-8"))

    assert complete["complete"] is True
    assert complete["silent_fallback_used"] is False
    assert decision["candidate_generation_challenger_created"] is True
    assert decision["candidate_scoring_created"] is False
    assert decision["production_ranking_changed"] is False
    assert decision["meemee_reflectable"] is False
    assert top3["changed_top3_members_count_vs_previous_best"] > 0
    assert recovery["recovered_missed_winner_count"] == 25
    assert result["candidate_scoring_created"] is False
