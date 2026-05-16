from __future__ import annotations

import argparse
import json
from pathlib import Path

from scripts import tradex_monthly_drawdown_guarded_momentum_top5_gate_v1 as guarded


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _row(
    day: str,
    symbol: str,
    ret: float,
    score: float,
    *,
    momentum: bool = False,
    low: bool = False,
    high: bool = False,
    monthly_state: str = "monthly_prior_uptrend",
) -> dict:
    return {
        "event_date": day,
        "symbol": symbol,
        "baseline_candidate_flag": True,
        "baseline_score": score,
        "combined_candidate_flag": True,
        "momentum_candidate_flag": momentum,
        "momentum_score": score,
        "momentum_rank": 1 if momentum else None,
        "ma5_h12_candidate_flag": False,
        "momentum_low_risk_context_flag": low,
        "momentum_high_risk_context_flag": high,
        "monthly_prior_state": monthly_state,
        "ret20_fwd": ret,
        "mfe20": max(ret, 0.01),
        "mae20": min(ret, -0.01),
        "severe_loss20": ret <= -0.10,
        "win20": ret > 0.0,
        "is_future_top10_by_ret20": ret >= 0.10,
    }


def _make_source(tmp_path: Path, rows: list[dict]) -> Path:
    root = tmp_path / "field_repair"
    _write_json(root / "research_decision.json", {"authoritative_research_decision": "common_ledger_field_repair_hold"})
    _write_jsonl(root / "repaired_common_top5_candidate_ledger.jsonl", rows)
    return root


def _run(tmp_path: Path, rows: list[dict]) -> Path:
    args = argparse.Namespace(
        source_field_repair_root=_make_source(tmp_path, rows),
        output_parent=tmp_path / "out",
        run_id="guarded-run",
    )
    return guarded.run(args)


def test_guarded_momentum_can_pass_all_mandatory_gates(tmp_path: Path) -> None:
    rows: list[dict] = []
    for year in [2020, 2021, 2022]:
        day = f"{year}-01-01"
        rows.extend(
            [
                _row(day, "b1", 0.03, 0.90),
                _row(day, "b2", 0.03, 0.89),
                _row(day, "zbad", -0.12, 0.88, monthly_state="monthly_prior_down_or_drawdown"),
                _row(day, "b4", 0.03, 0.87),
                _row(day, "b5", 0.03, 0.86),
                _row(day, "amom", 0.20, 0.855, momentum=True, low=False, high=False),
            ]
        )
    output = _run(tmp_path, rows)

    decision = _read_json(output / "research_decision.json")
    gate_report = _read_json(output / "gate_pass_fail_report.json")
    complete = _read_json(output / "_ARTIFACT_COMPLETE.json")

    assert decision["authoritative_research_decision"] == "monthly_drawdown_guarded_momentum_top5_gate_keep_candidate"
    assert decision["top5_candidate_pool_clearly_better_than_baseline"] is True
    assert gate_report["pass_count"] > 0
    assert gate_report["best_variant_failed_gates"] == []
    assert complete["complete"] is True


def test_contract_records_eval_only_labels_and_no_mutation(tmp_path: Path) -> None:
    rows: list[dict] = []
    for year in [2020, 2021, 2022]:
        day = f"{year}-01-01"
        for idx, ret in enumerate([0.05, 0.04, 0.03, 0.02, 0.01], start=1):
            rows.append(_row(day, f"b{idx}", ret, 10 - idx))
    output = _run(tmp_path, rows)

    contract = _read_json(output / "guarded_momentum_contract.json")
    mutation = _read_json(output / "no_mutation_audit.json")
    search = _read_json(output / "variant_search_space.json")

    assert contract["risk_guard_field"] == "monthly_prior_state"
    assert contract["risk_guard_point_in_time"] is True
    assert contract["uses_future_labels_in_scoring"] is False
    assert search["uses_future_labels_in_scoring"] is False
    assert mutation["no_mutation_pass"] is True
    assert mutation["production_ranking_changed"] is False
