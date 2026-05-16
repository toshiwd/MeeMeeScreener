from __future__ import annotations

import argparse
import json
from pathlib import Path

from scripts import tradex_monthly_drawdown_guarded_momentum_manual_candidate_review_pack_v1 as pack


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _row(day: str, symbol: str, ret: float, score: float, *, momentum: bool = False, monthly_state: str = "monthly_prior_uptrend") -> dict:
    return {
        "event_date": day,
        "symbol": symbol,
        "baseline_candidate_flag": True,
        "baseline_score": score,
        "combined_candidate_flag": True,
        "momentum_candidate_flag": momentum,
        "ma5_h12_candidate_flag": False,
        "momentum_low_risk_context_flag": False,
        "momentum_high_risk_context_flag": False,
        "monthly_prior_state": monthly_state,
        "ret20_fwd": ret,
        "mfe20": max(ret, 0.01),
        "mae20": min(ret, -0.01),
        "severe_loss20": ret <= -0.10,
        "win20": ret > 0.0,
        "is_future_top10_by_ret20": ret >= 0.10,
    }


def _make_sources(tmp_path: Path, rows: list[dict]) -> Path:
    field_repair = tmp_path / "field_repair"
    top5_gate = tmp_path / "top5_gate"
    pretest = tmp_path / "pretest"
    best_spec = {
        "variant_id": "monthly_drawdown_guarded_momentum_m+0.02_l-0.02_h-0.02_md-0.005",
        "momentum_weight": 0.02,
        "momentum_low_risk_weight": -0.02,
        "momentum_high_risk_penalty": -0.02,
        "monthly_down_or_drawdown_penalty": -0.005,
    }
    _write_jsonl(field_repair / "repaired_common_top5_candidate_ledger.jsonl", rows)
    _write_json(top5_gate / "strict_gate_leaderboard.json", {"best_variant": {"variant_id": best_spec["variant_id"], "spec": best_spec}})
    _write_json(pretest / "research_decision.json", {"authoritative_research_decision": "starter_entry_pretest_keep"})
    _write_json(pretest / "starter_entry_candidate_pool_report.json", {"pretest_gates": {"top5_candidate_pool_clearly_better": True}})
    _write_json(pretest / "_ARTIFACT_COMPLETE.json", {"complete": True})
    _write_json(
        pretest / "run_manifest.json",
        {
            "source_top5_gate_root": str(top5_gate),
            "source_field_repair_root": str(field_repair),
        },
    )
    return pretest


def _run(tmp_path: Path, rows: list[dict]) -> Path:
    args = argparse.Namespace(
        source_pretest_root=_make_sources(tmp_path, rows),
        output_parent=tmp_path / "out",
        run_id="pack-run",
    )
    return pack.run(args)


def test_review_pack_emits_required_artifacts_and_ready_decision(tmp_path: Path) -> None:
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
                _row(day, "amom", 0.20, 0.855, momentum=True),
            ]
        )
    output = _run(tmp_path, rows)

    decision = _read_json(output / "research_decision.json")
    complete = _read_json(output / "_ARTIFACT_COMPLETE.json")
    questions = _read_json(output / "manual_review_questions.json")

    assert decision["authoritative_research_decision"] == "manual_review_pack_ready"
    assert decision["publish_bundle_created"] is False
    assert decision["meemee_reflectable"] is False
    assert decision["auto_select_exactly_3"] is False
    assert complete["complete"] is True
    assert set(pack.REQUIRED_ARTIFACTS) == set(complete["required_artifacts"])
    assert "manual_review_approve" in questions["decision_options"]


def test_review_pack_contains_representative_and_added_removed_examples(tmp_path: Path) -> None:
    rows: list[dict] = []
    for year in [2020, 2021, 2022, 2023]:
        day = f"{year}-01-01"
        rows.extend(
            [
                _row(day, "b1", 0.03, 0.90),
                _row(day, "b2", 0.03, 0.89),
                _row(day, "zbad", -0.12, 0.88, monthly_state="monthly_prior_down_or_drawdown"),
                _row(day, "b4", 0.03, 0.87),
                _row(day, "b5", 0.03, 0.86),
                _row(day, "amom", 0.20, 0.855, momentum=True),
            ]
        )
    output = _run(tmp_path, rows)

    representative = _read_json(output / "representative_top5_candidate_lists.json")
    added_removed = _read_json(output / "added_vs_removed_candidate_examples.json")
    weak = _read_json(output / "weak_year_examples_2023_2025_2026.json")

    assert representative["examples"]["best"]
    assert representative["examples"]["worst"]
    assert added_removed["examples"]
    assert "2023" in weak["weak_years"]
