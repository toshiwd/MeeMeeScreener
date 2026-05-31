from __future__ import annotations

import json

import pandas as pd

from scripts import tradex_starter_entry_objective_reform_v1 as mod


def test_attach_starter_labels_separates_good_bad_and_watch_only() -> None:
    rows = pd.DataFrame(
        [
            {"code": "1000", "decision_date": 20240101, "baseline_rank": 1, "baseline_score": 10, "year": 2024},
            {"code": "2000", "decision_date": 20240101, "baseline_rank": 2, "baseline_score": 9, "year": 2024},
        ]
    )
    labels = pd.DataFrame(
        [
            {"code": "1000", "decision_date": 20240101, "ret5": 0.01, "ret20_path": 0.08, "mae5": -0.01, "mfe5": 0.03, "mae20": -0.02, "mfe20": 0.10, "path20_available": True},
            {"code": "2000", "decision_date": 20240101, "ret5": -0.02, "ret20_path": -0.07, "mae5": -0.04, "mfe5": 0.01, "mae20": -0.10, "mfe20": 0.02, "path20_available": True},
        ]
    )
    out = mod.attach_starter_labels(rows, labels)
    good = out[out["code"] == "1000"].iloc[0]
    bad = out[out["code"] == "2000"].iloc[0]
    assert bool(good["starter_good"])
    assert not bool(good["starter_bad_abs"])
    assert bool(bad["starter_bad_abs"])
    assert bool(bad["immediate_adverse_entry"])
    assert bad["diagnostic_candidate_role"] == "avoid_candidate"


def test_baseline_objective_audit_reports_rates() -> None:
    rows = pd.DataFrame(
        [
            {"year": 2024, "baseline_rank": 1, "path20_available": True, "starter_good": True, "starter_bad": False, "selected_loser": False, "selected_winner": True, "diagnostic_candidate_role": "starter_entry_candidate", "ret20": 0.08, "mae20": -0.02, "mfe20": 0.1, "immediate_adverse_entry": False},
            {"year": 2024, "baseline_rank": 2, "path20_available": True, "starter_good": False, "starter_bad": True, "selected_loser": True, "selected_winner": False, "diagnostic_candidate_role": "watch_only_candidate", "ret20": -0.06, "mae20": -0.09, "mfe20": 0.01, "immediate_adverse_entry": True},
        ]
    )
    audit = mod.baseline_objective_audit(rows)
    row = audit[(audit["period"] == "2024") & (audit["topk"] == 5)].iloc[0]
    assert row["starter_good_rate"] == 0.5
    assert row["starter_bad_rate"] == 0.5
    assert row["watch_only_rate"] == 0.5


def test_decide_prefers_starter_score_model_when_pool_has_ceiling_but_single_axis_weak() -> None:
    gap = {
        "recent_rows_with_path": 5000,
        "starter_good_count": 2000,
        "baseline_top10": {"watch_only_rate": 0.4},
        "oracle_top10": {"oracle_improvement": 0.03},
    }
    feature = pd.DataFrame([{"feature": "x", "good_minus_bad_mean": 0.1}])
    decision = mod.decide(gap, feature)
    assert decision["research_decision"] == "starter_score_model_needed"


def test_label_schema_marks_labels_diagnostic_only() -> None:
    schema = mod.starter_entry_label_schema()
    assert schema["labels_are_diagnostic_only"] is True
    assert "starter_bad_abs" in schema["thresholds"]
    assert json.dumps(schema)
