from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts import tradex_source_complete_same_date_ledger_repair_v1 as mod


SOURCE = (
    "pre_ma20_context_state=pre_ma20_near|"
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


def _noise_artifacts(root: Path) -> None:
    _write_json(root / "_ARTIFACT_COMPLETE.json", {"complete": True, "silent_fallback_used": False, "research_fallback_used": False})
    _write_json(
        root / "research_decision.json",
        {
            "decision": "hold",
            "authoritative_research_decision": "source_complete_ledger_repair_needed",
            "silent_fallback_used": False,
            "research_fallback_used": False,
        },
    )
    _write_json(root / "source_v2_archive_or_refine_decision.json", {"authoritative_research_decision": "source_complete_ledger_repair_needed"})
    _write_json(
        root / "same_date_support_limitation_followup_report.json",
        {
            "per_source_same_date_support_available": False,
            "same_date_support_not_faked": True,
            "missing_required_context_fields": ["pre_ma60_context_state"],
        },
    )


def _validation_v2_artifacts(root: Path) -> None:
    _write_json(root / "_ARTIFACT_COMPLETE.json", {"complete": True, "silent_fallback_used": False, "research_fallback_used": False})
    _write_json(
        root / "research_decision.json",
        {
            "authoritative_research_decision": "source_specific_candidate_generation_v2_hold",
            "same_date_support_not_faked": True,
            "silent_fallback_used": False,
            "research_fallback_used": False,
        },
    )
    _write_jsonl(root / "source_candidate_ledger.jsonl", [{"event_date": "2024-01-02", "code": "A0"}])
    _write_json(root / "source_recovery_report.json", {"recovered_missed_winner_count": 1})
    _write_json(root / "source_noise_report.json", {"severe_loser_added_per_recovered_winner": 1.0})
    _write_json(root / "baseline_comparison_report.json", {"selected_top3_avg_ret20_delta_vs_previous_best": -0.01})
    _write_json(root / "branching_report.json", {"changed_top3_members_count_vs_previous_best": 2})
    _write_json(root / "same_date_support_limitation_report.json", {"per_source_same_date_support_available": False, "same_date_support_not_faked": True})
    _write_json(root / "time_block_source_validation.json", {"effect_stable_across_time_blocks": False, "rows": []})
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
    base = [
        ("2024-01-02", "A0", True, False, 0.18, 0.22, -0.03, 6.0),
        ("2024-01-02", "B0", False, True, 0.01, 0.03, -0.02, 1.0),
        ("2024-01-03", "A1", True, True, -0.12, 0.01, -0.21, 5.0),
        ("2024-01-03", "B1", False, True, 0.04, 0.05, -0.03, 1.0),
    ]
    for event_date, code, source_flag, selected_source, ret20, mfe20, mae20, rank in base:
        event_ymd = int(event_date.replace("-", ""))
        row = {
            "event_date": event_date,
            "event_ymd": event_ymd,
            "code": code,
            "selection_rank": rank,
            "research_score": 100.0 - rank,
            "source_family": SOURCE if source_flag else "other",
            "source_specific_candidate": source_flag,
            "baseline_top3": rank <= 3,
            "selected_by_previous_best_top3": rank <= 3,
            "ret20_fwd": ret20,
            "mfe20": mfe20,
            "mae20": mae20,
            "win20": ret20 > 0,
            "severe_loss20": mae20 <= -0.20,
            "future_winner": ret20 >= 0.10 or mfe20 >= 0.15,
            "is_future_top10_by_ret20": ret20 >= 0.10,
            "is_big_winner_ret20_ge_10pct": ret20 >= 0.10,
            "is_big_winner_MFE20_ge_15pct": mfe20 >= 0.15,
            "pre_ma20_path_state": "pre_ma20_near" if source_flag else "pre_ma20_reclaim_base",
            "pre_ma60_context_state": "pre_ma60_near_or_above",
            "weekly_prior_state": "weekly_prior_strong_up" if source_flag else "weekly_prior_uptrend",
            "negative_guard_match": "True" if source_flag else "False",
            "guard_safe_full": "False",
        }
        rows.append(row)
        ledger_rows.append({**row, "ranker_family_id": "source_specific_candidate_generation_validation_v2_max1slot", "selected_topk": selected_source})
    return pd.DataFrame(rows), pd.DataFrame(ledger_rows)


def test_source_complete_same_date_ledger_repair_writes_ready_artifacts(tmp_path: Path, monkeypatch) -> None:
    noise_root = tmp_path / "noise"
    validation_root = tmp_path / "validation"
    _noise_artifacts(noise_root / "noise-run")
    _validation_v2_artifacts(validation_root / "v2-run")
    frame, ledger = _frame_and_ledger()
    source_dirs = {
        "mechanism_validation": validation_root / "v2-run" / "mechanism",
        "hypothesis_refresh": validation_root / "v2-run" / "refresh",
        "second_review": validation_root / "v2-run" / "second",
        "missed_winner": validation_root / "v2-run" / "missed",
        "root_cause": validation_root / "v2-run" / "root",
        "wide": validation_root / "v2-run" / "wide",
    }
    monkeypatch.setattr(
        mod,
        "load_reconstructed_inputs",
        lambda _artifacts: (frame, ledger, {"hypothesis_id": "candidate_generation_map_refresh_hypothesis_1", "source_family": SOURCE}, source_dirs),
    )

    result = mod.run_source_complete_same_date_ledger_repair_v1(
        source_noise_decomposition_run_id="noise-run",
        source_validation_v2_run_id="v2-run",
        noise_decomposition_root=noise_root,
        validation_v2_root=validation_root,
        output_root=tmp_path / "out",
        run_id="repair-run",
    )
    output_dir = Path(result["output_dir"])

    for artifact in mod.REQUIRED_ARTIFACTS:
        assert (output_dir / artifact).exists(), artifact

    decision = json.loads((output_dir / "research_decision.json").read_text(encoding="utf-8"))
    availability = json.loads((output_dir / "field_availability_audit.json").read_text(encoding="utf-8"))
    reconstruction = json.loads((output_dir / "reconstruction_audit.json").read_text(encoding="utf-8"))
    readiness = json.loads((output_dir / "same_date_support_readiness_report.json").read_text(encoding="utf-8"))
    precheck = json.loads((output_dir / "v2_source_support_precheck.json").read_text(encoding="utf-8"))
    leakage = json.loads((output_dir / "leakage_audit.json").read_text(encoding="utf-8"))
    complete = json.loads((output_dir / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    ledger_rows = (output_dir / "source_complete_same_date_ledger.jsonl").read_text(encoding="utf-8").strip().splitlines()

    assert len(ledger_rows) == len(frame)
    assert availability["availability"]["pre_ma60_context_state"]["availability_rate"] == 1.0
    assert reconstruction["reconstruction_failed"] is False
    assert readiness["can_compute_per_source_same_date_support"] is True
    assert precheck["per_source_support_computable"] is True
    assert leakage["future_labels_used_in_source_construction"] is False
    assert decision["authoritative_research_decision"] == "source_complete_ledger_ready"
    assert decision["candidate_generation_challenger_created"] is False
    assert decision["candidate_scoring_created"] is False
    assert decision["production_ranking_changed"] is False
    assert decision["meemee_reflectable"] is False
    assert decision["same_date_support_not_faked"] is True
    assert complete["complete"] is True
