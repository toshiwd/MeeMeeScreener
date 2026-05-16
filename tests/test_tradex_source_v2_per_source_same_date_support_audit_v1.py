from __future__ import annotations

import json
from pathlib import Path

from scripts import tradex_source_v2_per_source_same_date_support_audit_v1 as mod


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")


def _ledger_row(
    event_date: str,
    symbol: str,
    source_family_id: str,
    *,
    v2: bool,
    selected_v2: bool,
    previous_top3: bool,
    ret20: float,
    mfe20: float,
    mae20: float,
) -> dict[str, object]:
    return {
        "event_date": event_date,
        "symbol": symbol,
        "candidate_id": f"{event_date}:{symbol}",
        "stable_event_key": f"{event_date}:{symbol}",
        "source_family_id": source_family_id,
        "source_family_text": source_family_id,
        "source_family_components": {"pre_ma20_context_state": "near", "pre_ma60_context_state": "near_or_above", "weekly_prior_state": "strong", "negative_guard_match": "True"},
        "pre_ma20_context_state": "near",
        "pre_ma60_context_state": "near_or_above",
        "weekly_prior_state": "strong",
        "negative_guard_match": "True",
        "safe_full_tag": False,
        "previous_best_rank": None if not previous_top3 else 2,
        "previous_best_score": None,
        "selected_by_previous_best_top3": previous_top3,
        "selected_by_source_v2": selected_v2,
        "source_v2_candidate_flag": v2,
        "ret20": ret20,
        "MFE20": mfe20,
        "MAE20": mae20,
        "severe_loss20": mae20 <= -0.20,
        "future_top10_by_ret20": ret20 >= 0.10,
        "big_winner_ret20_ge_10pct": ret20 >= 0.10,
        "big_winner_MFE20_ge_15pct": mfe20 >= 0.15,
        "future_winner_evaluation_label": ret20 >= 0.10 or mfe20 >= 0.15,
        "nonwinner_evaluation_label": ret20 <= 0 or mae20 <= -0.20,
    }


def _source_artifacts(root: Path) -> None:
    rows = [
        _ledger_row("2024-01-02", "A", "v2", v2=True, selected_v2=True, previous_top3=False, ret20=-0.12, mfe20=0.01, mae20=-0.22),
        _ledger_row("2024-01-02", "B", "other", v2=False, selected_v2=False, previous_top3=True, ret20=0.16, mfe20=0.20, mae20=-0.02),
        _ledger_row("2024-01-03", "C", "v2", v2=True, selected_v2=True, previous_top3=False, ret20=-0.08, mfe20=0.02, mae20=-0.10),
        _ledger_row("2024-01-03", "D", "other", v2=False, selected_v2=False, previous_top3=True, ret20=0.12, mfe20=0.18, mae20=-0.03),
    ]
    _write_json(root / "_ARTIFACT_COMPLETE.json", {"complete": True, "silent_fallback_used": False, "research_fallback_used": False})
    _write_json(
        root / "research_decision.json",
        {
            "authoritative_research_decision": "source_complete_ledger_ready",
            "per_source_same_date_support_available": True,
            "same_date_support_not_faked": True,
            "future_labels_used_in_source_construction": False,
            "silent_fallback_used": False,
            "research_fallback_used": False,
        },
    )
    _write_json(root / "same_date_support_readiness_report.json", {"can_compute_per_source_same_date_support": True})
    _write_json(root / "v2_source_support_precheck.json", {"v2_source_candidate_count": 2})
    _write_jsonl(root / "source_complete_same_date_ledger.jsonl", rows)


def test_source_v2_same_date_support_audit_writes_archive_artifacts(tmp_path: Path) -> None:
    ledger_root = tmp_path / "ledger"
    noise_root = tmp_path / "noise"
    validation_root = tmp_path / "validation"
    _source_artifacts(ledger_root / "ledger-run")
    _write_json(noise_root / "noise-run" / "_ARTIFACT_COMPLETE.json", {"complete": True, "silent_fallback_used": False, "research_fallback_used": False})
    _write_json(noise_root / "noise-run" / "research_decision.json", {"authoritative_research_decision": "source_complete_ledger_repair_needed"})
    _write_json(validation_root / "v2-run" / "_ARTIFACT_COMPLETE.json", {"complete": True, "silent_fallback_used": False, "research_fallback_used": False})
    _write_json(validation_root / "v2-run" / "research_decision.json", {"authoritative_research_decision": "source_specific_candidate_generation_v2_hold"})

    result = mod.run_source_v2_per_source_same_date_support_audit_v1(
        source_ledger_repair_run_id="ledger-run",
        source_noise_decomposition_run_id="noise-run",
        source_validation_v2_run_id="v2-run",
        ledger_repair_root=ledger_root,
        noise_decomposition_root=noise_root,
        validation_v2_root=validation_root,
        output_root=tmp_path / "out",
        run_id="audit-run",
    )
    output_dir = Path(result["output_dir"])

    for artifact in mod.REQUIRED_ARTIFACTS:
        assert (output_dir / artifact).exists(), artifact

    decision = json.loads((output_dir / "research_decision.json").read_text(encoding="utf-8"))
    archive = json.loads((output_dir / "v2_archive_or_refine_decision.json").read_text(encoding="utf-8"))
    selected = json.loads((output_dir / "v2_source_selected_vs_nonselected_report.json").read_text(encoding="utf-8"))
    relative = json.loads((output_dir / "v2_source_relative_support_report.json").read_text(encoding="utf-8"))
    complete = json.loads((output_dir / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    support_rows = (output_dir / "per_source_same_date_support_ledger.jsonl").read_text(encoding="utf-8").strip().splitlines()

    assert len(support_rows) == 4
    assert selected["recovered_missed_winner_count"] == 0
    assert selected["added_severe_loser_count"] == 1
    assert relative["avg_winner_rate_delta_vs_other_sources"] < 0
    assert archive["authoritative_research_decision"] == "source_v2_archive"
    assert decision["authoritative_research_decision"] == "source_v2_archive"
    assert decision["candidate_generation_challenger_created"] is False
    assert decision["candidate_scoring_created"] is False
    assert decision["production_ranking_changed"] is False
    assert decision["meemee_reflectable"] is False
    assert decision["same_date_support_not_faked"] is True
    assert complete["complete"] is True
