from __future__ import annotations

import pandas as pd

from scripts import tradex_starter_entry_family_source_split_design_v1 as mod


def test_schema_uses_research_names_not_real_source_fields() -> None:
    schema = mod.schema()

    assert schema["schema_version"] == "research_family_source_schema_v1"
    assert schema["diagnostic_not_real_candidate_source"] is True
    assert schema["source_field_name"] == "research_candidate_source_family"


def test_load_surface_rows_maps_primary_family_to_research_source(tmp_path) -> None:
    root = tmp_path / "family"
    root.mkdir()
    pd.DataFrame(
        [
            {
                "decision_date": 20240101,
                "code": "A",
                "primary_family": "pullback_reclaim_family",
                "baseline_score": 10,
                "baseline_rank": 1,
                "family_feature_availability_json": "{}",
                "family_assignment_reason_json": "{}",
            }
        ]
    ).to_csv(root / "candidate_family_rows.csv", index=False)

    rows = mod.load_surface_rows(root)

    assert rows.loc[0, "research_candidate_source_family"] == "pullback_reclaim_source"
    assert rows.loc[0, "research_family_source_schema_version"] == "research_family_source_schema_v1"
    assert rows.loc[0, "within_family_baseline_rank"] == 1


def test_family_topk_uses_within_family_rank() -> None:
    rows = pd.DataFrame(
        [
            {"year": 2024, "path20_available": True, "research_candidate_source_family": "pullback_reclaim_source", "decision_date": 20240101, "within_family_baseline_rank": 1, "ret20": 0.1, "starter_good": True, "starter_bad": False, "selected_loser": False},
            {"year": 2024, "path20_available": True, "research_candidate_source_family": "pullback_reclaim_source", "decision_date": 20240101, "within_family_baseline_rank": 4, "ret20": -0.1, "starter_good": False, "starter_bad": True, "selected_loser": True},
        ]
    )

    out = mod.family_topk(rows)
    row = out[(out["period"] == "2024") & (out["family_topk"] == 3)].iloc[0]

    assert row["n"] == 1
    assert row["mean_ret20"] == 0.1


def test_decide_allows_pretest_when_candidate_exists() -> None:
    decision = mod.decide(
        {
            "candidates": [
                {
                    "axis_name": "pullback_reclaim_source_surface_axis",
                    "source_family": "pullback_reclaim_source",
                    "recommended_next": "pretest",
                }
            ]
        }
    )

    assert decision["research_decision"] == "family_specific_pretest_allowed"
    assert decision["meemee_reflectable_candidate"] is False
