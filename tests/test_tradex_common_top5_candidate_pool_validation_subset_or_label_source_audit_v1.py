from __future__ import annotations

import argparse
import json
from pathlib import Path

from scripts import tradex_common_top5_candidate_pool_validation_subset_or_label_source_audit_v1 as validation


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _row(event_date: str, symbol: str, ret: float | None, *, baseline: bool = False, momentum: bool = False, ma5: bool = False, score: float | None = None, rank: int | None = None, incomplete: bool = False) -> dict:
    severe = None if incomplete else ret is not None and ret <= -0.10
    return {
        "event_date": event_date,
        "symbol": symbol,
        "baseline_candidate_flag": baseline,
        "momentum_candidate_flag": momentum,
        "ma5_h12_candidate_flag": ma5,
        "combined_candidate_flag": momentum or ma5,
        "source_family_flags": {"baseline": baseline, "momentum_continuation_soft_boost_v1": momentum, "ma5_h12_near_bull_ma60_rising": ma5, "combined": momentum or ma5},
        "baseline_score": score if baseline else None,
        "baseline_rank": rank if baseline else None,
        "momentum_score": score if momentum else None,
        "momentum_rank": rank if momentum else None,
        "ma5_h12_context_score": None,
        "ma5_h12_rank": None,
        "combined_score": None,
        "shadow_candidate_rank": None,
        "ret20_fwd": None if incomplete else ret,
        "mfe20": None if incomplete else max(ret or 0.0, 0.02),
        "mae20": None if incomplete else min(ret or 0.0, -0.01),
        "severe_loss20": severe,
        "win20": None if incomplete else (ret or 0.0) > 0.0,
        "is_future_top10_by_ret20": bool(ret is not None and ret >= 0.10) if not incomplete else None,
        "is_big_winner_ret20_ge_10pct": bool(ret is not None and ret >= 0.10) if not incomplete else None,
        "label_unavailable_reason": "incomplete_forward_window" if incomplete else None,
        "ma5_exit_ret": 0.99 if ma5 else None,
    }


def _make_source(tmp_path: Path, rows: list[dict]) -> Path:
    parent = tmp_path / "field_repair"
    root = parent / "repair-run"
    _write_json(root / "research_decision.json", {"authoritative_research_decision": "common_ledger_field_repair_hold"})
    _write_json(root / "_ARTIFACT_COMPLETE.json", {"complete": True})
    _write_jsonl(root / "repaired_common_top5_candidate_ledger.jsonl", rows)
    return parent


def _run(tmp_path: Path, rows: list[dict]) -> Path:
    parent = _make_source(tmp_path, rows)
    args = argparse.Namespace(
        source_field_repair_run_id="repair-run",
        source_field_repair_parent=parent,
        output_parent=tmp_path / "out",
        run_id="validation-run",
    )
    return validation.run(args)


def test_subset_validation_excludes_incomplete_rows_and_records_contract(tmp_path: Path) -> None:
    rows = [
        _row("2020-01-01", "1001", 0.01, baseline=True, score=0.5),
        _row("2020-01-01", "1002", 0.08, momentum=True, score=1.0, rank=1),
        _row("2020-01-01", "1003", 0.06, ma5=True),
        _row("2020-01-02", "1004", None, ma5=True, incomplete=True),
    ]
    output_root = _run(tmp_path, rows)

    decision = _read_json(output_root / "research_decision.json")
    completeness = _read_json(output_root / "label_completeness_audit.json")
    complete = _read_json(output_root / "_ARTIFACT_COMPLETE.json")

    assert decision["subset_only_validation"] is True
    assert decision["full_validation_claimed"] is False
    assert decision["label_complete_rows_used_only"] is True
    assert completeness["label_complete_row_count"] == 3
    assert completeness["label_incomplete_row_count"] == 1
    assert completeness["excluded_due_to_incomplete_forward_window_count"] == 1
    assert complete["complete"] is True


def test_keep_candidate_when_momentum_improves_same_subset_without_unranked_limit(tmp_path: Path) -> None:
    rows: list[dict] = []
    for day in ["2020-01-01", "2020-01-02"]:
        for idx, ret in enumerate([0.01, 0.00, -0.02, -0.04, -0.05], start=1):
            rows.append(_row(day, f"10{idx}", ret, baseline=True, score=10 - idx))
        for idx, ret in enumerate([0.20, 0.15, 0.12], start=1):
            rows.append(_row(day, f"20{idx}", ret, momentum=True, score=20 - idx, rank=idx))
    output_root = _run(tmp_path, rows)

    decision = _read_json(output_root / "research_decision.json")
    comparison = _read_json(output_root / "variant_comparison_report.json")

    assert decision["authoritative_research_decision"] == "subset_top5_validation_keep_candidate"
    momentum = next(row for row in comparison["rows"] if row["variant_id"] == "momentum")
    assert momentum["top5_avg_ret20_delta_vs_baseline"] > 0
    assert momentum["unranked_ma5_cap_policy_used"] is False


def test_ma5_exit_labels_are_not_used_and_unranked_policy_is_explicit(tmp_path: Path) -> None:
    rows = [
        _row("2020-01-01", "1001", 0.01, baseline=True, score=0.5),
        _row("2020-01-01", "2001", 0.12, ma5=True),
        _row("2020-01-01", "2002", 0.11, ma5=True),
        _row("2020-01-01", "2003", 0.10, ma5=True),
    ]
    output_root = _run(tmp_path, rows)

    contract = _read_json(output_root / "subset_validation_contract.json")
    comparison = _read_json(output_root / "variant_comparison_report.json")

    assert contract["ma5_exit_labels_used_as_ret20_labels"] is False
    assert contract["unranked_ma5_cap_policy_explicit"] is True
    ma5 = next(row for row in comparison["rows"] if row["variant_id"] == "ma5_h12")
    assert ma5["unranked_ma5_cap_policy_used"] is True
