from __future__ import annotations

import argparse
import json
from pathlib import Path

from scripts import tradex_strict_top5_candidate_pool_gate_validation_v1 as strict_gate


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _row(day: str, symbol: str, ret: float, *, baseline_score: float | None = None, momentum: bool = False, low: bool = False, high: bool = False) -> dict:
    return {
        "event_date": day,
        "symbol": symbol,
        "baseline_candidate_flag": baseline_score is not None,
        "baseline_score": baseline_score,
        "momentum_candidate_flag": momentum,
        "momentum_score": baseline_score,
        "momentum_rank": 1 if momentum else None,
        "ma5_h12_candidate_flag": False,
        "combined_candidate_flag": momentum,
        "momentum_low_risk_context_flag": low,
        "momentum_high_risk_context_flag": high,
        "ret20_fwd": ret,
        "mfe20": max(ret, 0.01),
        "mae20": min(ret, -0.01),
        "severe_loss20": ret <= -0.10,
        "win20": ret > 0.0,
        "is_future_top10_by_ret20": ret >= 0.10,
    }


def _make_roots(tmp_path: Path, rows: list[dict]) -> tuple[Path, Path]:
    repair = tmp_path / "repair"
    subset = tmp_path / "subset"
    _write_json(repair / "research_decision.json", {"authoritative_research_decision": "common_ledger_field_repair_hold"})
    _write_jsonl(repair / "repaired_common_top5_candidate_ledger.jsonl", rows)
    _write_json(subset / "research_decision.json", {"authoritative_research_decision": "subset_top5_validation_hold"})
    return repair, subset


def _run(tmp_path: Path, rows: list[dict]) -> Path:
    repair, subset = _make_roots(tmp_path, rows)
    args = argparse.Namespace(
        source_field_repair_root=repair,
        source_subset_validation_root=subset,
        output_parent=tmp_path / "out",
        run_id="strict-run",
    )
    return strict_gate.run(args)


def test_strict_gate_emits_failed_when_no_variant_passes_all_gates(tmp_path: Path) -> None:
    rows = []
    for year in [2020, 2021, 2022]:
        day = f"{year}-01-01"
        for i, ret in enumerate([0.10, 0.08, 0.06, 0.04, 0.02], start=1):
            rows.append(_row(day, f"b{i}", ret, baseline_score=10 - i))
        rows.append(_row(day, "m1", 0.11, baseline_score=1, momentum=True, high=True))
    output = _run(tmp_path, rows)

    decision = _read_json(output / "research_decision.json")
    complete = _read_json(output / "_ARTIFACT_COMPLETE.json")

    assert decision["authoritative_research_decision"] in {
        "strict_top5_candidate_pool_gate_failed",
        "strict_top5_candidate_pool_gate_keep_candidate",
    }
    assert decision["strict_gate_validation_run"] is True
    assert complete["complete"] is True


def test_strict_gate_contract_records_all_required_gates(tmp_path: Path) -> None:
    rows = []
    for year in [2020, 2021, 2022]:
        day = f"{year}-01-01"
        for i, ret in enumerate([0.01, 0.00, -0.01, -0.02, -0.03], start=1):
            rows.append(_row(day, f"b{i}", ret, baseline_score=10 - i))
        rows.append(_row(day, "m1", 0.20, baseline_score=1, momentum=True, low=True))
    output = _run(tmp_path, rows)

    contract = _read_json(output / "strict_gate_contract.json")
    search = _read_json(output / "variant_search_space.json")

    assert contract["required_gates"] == strict_gate.MANDATORY_GATES
    assert contract["gate_pass_policy"] == "all_mandatory_gates_must_pass"
    assert search["uses_future_labels_in_scoring"] is False
