from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts import tradex_source_specific_candidate_generation_v2_noise_decomposition_v1 as mod


SOURCE = (
    "pre_ma20_path_state=pre_ma20_near|"
    "pre_ma60_context_state=pre_ma60_near_or_above|"
    "weekly_prior_state=weekly_prior_strong_up|"
    "negative_guard_match=True"
)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")


def _source_validation_v2_artifacts(root: Path) -> None:
    _write_json(root / "_ARTIFACT_COMPLETE.json", {"complete": True, "silent_fallback_used": False, "research_fallback_used": False})
    _write_json(
        root / "research_decision.json",
        {
            "authoritative_research_decision": "source_specific_candidate_generation_v2_hold",
            "decision": "hold",
            "same_date_support_not_faked": True,
            "silent_fallback_used": False,
            "research_fallback_used": False,
        },
    )
    _write_jsonl(root / "source_candidate_ledger.jsonl", [{"event_date": "2024-01-02", "code": "A0"}])
    _write_json(
        root / "source_recovery_report.json",
        {
            "recovered_missed_winner_count": 1,
            "source_candidate_added_nonwinner_count": 2,
            "source_candidate_added_severe_loser_count": 1,
        },
    )
    _write_json(
        root / "source_noise_report.json",
        {
            "recovered_missed_winner_count": 1,
            "source_candidate_added_nonwinner_count": 2,
            "source_candidate_added_severe_loser_count": 1,
            "nonwinner_added_per_recovered_winner": 2.0,
            "severe_loser_added_per_recovered_winner": 1.0,
        },
    )
    _write_json(
        root / "baseline_comparison_report.json",
        {
            "selected_top3_avg_ret20_delta_vs_previous_best": -0.01,
            "oracle_top3_gap_delta_vs_previous_best": -0.01,
            "changed_top3_members_count_vs_previous_best": 6,
        },
    )
    _write_json(root / "branching_report.json", {"changed_top3_members_count_vs_previous_best": 6, "changed_top5_members_count_vs_previous_best": 6})
    _write_json(
        root / "same_date_support_limitation_report.json",
        {
            "per_source_same_date_support_available": False,
            "same_date_support_not_faked": True,
            "missing_required_context_fields": ["pre_ma60_context_state"],
            "aggregate_same_date_support_only": {"winner_source_present_but_under_ranked_rate": 0.2},
        },
    )
    _write_json(
        root / "time_block_source_validation.json",
        {
            "effect_stable_across_time_blocks": False,
            "positive_top3_delta_time_block_rate": 0.0,
            "rows": [{"time_block": "2024", "top3_avg_ret20_delta": -0.01}],
        },
    )
    _write_json(
        root / "source_artifact_refs.json",
        {
            "source_roots": {
                "mechanism_validation": str(root / "mechanism"),
                "hypothesis_refresh": str(root / "refresh"),
                "second_review": str(root / "second"),
                "missed_winner": str(root / "missed"),
                "root_cause": str(root / "root"),
                "wide": str(root / "wide"),
            }
        },
    )


def _frame_and_ledger() -> tuple[pd.DataFrame, pd.DataFrame]:
    rows = []
    ledger_rows = []
    scenarios = [
        ("2024-01-02", "A0", 0.18, 0.22, -0.03, False, True),
        ("2024-01-03", "A1", -0.16, 0.01, -0.22, False, True),
        ("2024-01-04", "A2", -0.04, 0.03, -0.08, False, True),
    ]
    for idx, (event_date, source_code, ret20, mfe20, mae20, source_baseline_top5, selected_source) in enumerate(scenarios):
        event_ymd = int(event_date.replace("-", ""))
        source_row = {
            "event_date": event_date,
            "event_ymd": event_ymd,
            "code": source_code,
            "selection_rank": 6.0,
            "previous_best_selection_rank": 6.0,
            "source_family": SOURCE,
            "source_specific_candidate": True,
            "baseline_top3": False,
            "baseline_top5": source_baseline_top5,
            "ret20_fwd": ret20,
            "mfe20": mfe20,
            "mae20": mae20,
            "win20": ret20 > 0,
            "severe_loss20": mae20 <= -0.20,
            "future_winner": ret20 >= 0.10 or mfe20 >= 0.15,
            "is_future_top10_by_ret20": ret20 >= 0.10,
            "is_future_top5_by_ret20": ret20 >= 0.10,
            "is_big_winner_ret20_ge_10pct": ret20 >= 0.10,
            "is_big_winner_MFE20_ge_15pct": mfe20 >= 0.15,
            "pre_ma20_path_state": "pre_ma20_near",
            "pre_ma60_context_state": "pre_ma60_near_or_above",
            "weekly_prior_state": "weekly_prior_strong_up",
            "monthly_prior_state": "monthly_prior_flat",
            "pre_ret20_state": "pre_ret20_up",
            "pre_ret5_state": "pre_ret5_up",
            "negative_guard_match": "True",
            "guard_safe_full": "False",
            "time_block": "2024",
        }
        rows.append(source_row)
        ledger_rows.append({**source_row, "ranker_family_id": "source_specific_candidate_generation_validation_v2_max1slot", "selection_rank": 3, "selected_topk": selected_source})
        for rank, code, value in [(1, f"B{idx}", 0.02), (2, f"C{idx}", 0.01), (3, f"D{idx}", 0.05)]:
            base_row = {
                **source_row,
                "code": code,
                "selection_rank": float(rank),
                "previous_best_selection_rank": float(rank),
                "source_family": "other",
                "source_specific_candidate": False,
                "baseline_top3": True,
                "baseline_top5": True,
                "ret20_fwd": value,
                "mfe20": value + 0.02,
                "mae20": -0.03,
                "win20": value > 0,
                "severe_loss20": False,
                "future_winner": False,
                "is_future_top10_by_ret20": False,
                "is_future_top5_by_ret20": False,
                "is_big_winner_ret20_ge_10pct": False,
                "is_big_winner_MFE20_ge_15pct": False,
            }
            rows.append(base_row)
            selected = code in {f"B{idx}", f"C{idx}"}
            ledger_rows.append({**base_row, "ranker_family_id": "source_specific_candidate_generation_validation_v2_max1slot", "selection_rank": rank if selected else None, "selected_topk": selected})
    return pd.DataFrame(rows), pd.DataFrame(ledger_rows)


def test_noise_decomposition_run_writes_required_artifacts(tmp_path: Path, monkeypatch) -> None:
    validation_root = tmp_path / "validation"
    validation_run = validation_root / "v2-run"
    _source_validation_v2_artifacts(validation_run)
    frame, ledger = _frame_and_ledger()
    source_dirs = {
        "mechanism_validation": validation_run / "mechanism",
        "hypothesis_refresh": validation_run / "refresh",
        "second_review": validation_run / "second",
        "missed_winner": validation_run / "missed",
        "root_cause": validation_run / "root",
        "wide": validation_run / "wide",
    }
    monkeypatch.setattr(
        mod,
        "load_reconstructed_inputs",
        lambda _artifacts: (frame, ledger, {"hypothesis_id": "candidate_generation_map_refresh_hypothesis_1", "source_family": SOURCE}, source_dirs),
    )

    result = mod.run_source_specific_candidate_generation_v2_noise_decomposition_v1(
        source_validation_v2_run_id="v2-run",
        validation_v2_root=validation_root,
        output_root=tmp_path / "out",
        run_id="noise-run",
    )
    output_dir = Path(result["output_dir"])

    for artifact in mod.REQUIRED_ARTIFACTS:
        assert (output_dir / artifact).exists(), artifact
    grouped = json.loads((output_dir / "source_v2_grouped_outcome_report.json").read_text(encoding="utf-8"))
    proxy = json.loads((output_dir / "point_in_time_noise_proxy_report.json").read_text(encoding="utf-8"))
    decision = json.loads((output_dir / "research_decision.json").read_text(encoding="utf-8"))
    complete = json.loads((output_dir / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))

    assert grouped["reconciles_with_source_recovery_report"]["recovered_missed_winner_count_matches"] is True
    assert grouped["reconciles_with_source_recovery_report"]["added_nonwinner_count_matches"] is True
    assert grouped["reconciles_with_source_recovery_report"]["added_severe_loser_count_matches"] is True
    assert proxy["calendar_block_used_as_trading_rule"] is False
    assert "ret20_fwd" in proxy["future_label_fields_excluded_from_proxy_inputs"]
    assert decision["candidate_generation_challenger_created"] is False
    assert decision["candidate_scoring_created"] is False
    assert decision["production_ranking_changed"] is False
    assert decision["meemee_reflectable"] is False
    assert decision["same_date_support_not_faked"] is True
    assert decision["authoritative_research_decision"] == "source_complete_ledger_repair_needed"
    assert complete["complete"] is True
