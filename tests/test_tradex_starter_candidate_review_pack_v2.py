from __future__ import annotations

import json

import pandas as pd

from scripts import tradex_starter_candidate_review_pack_v2 as mod


def _row(**kwargs: object) -> pd.Series:
    base = {
        "research_candidate_source_family": "pullback_reclaim_source",
        "research_risk_tags_json": json.dumps(["no_shadow_risk_tag"]),
        "research_setup_tags_json": json.dumps(["pullback_candidate"]),
        "selected_loser": False,
        "immediate_adverse_entry": False,
        "next_open_available": True,
        "entry_allowed_by_score": True,
        "path20_available": True,
        "baseline_score": 10,
    }
    base.update(kwargs)
    return pd.Series(base)


def test_classify_never_validates_without_keep_gated_artifact() -> None:
    action, reasons, _ = mod.classify(_row(), keep_gated=False)

    assert action == "starter_review"
    assert "no_keep_gated_artifact" in reasons


def test_classify_good_family_with_entry_gap_is_watch_not_empty_review() -> None:
    action, reasons, _ = mod.classify(_row(next_open_available=False), keep_gated=False)

    assert action == "watch"
    assert "entry_liquidity_or_data_coverage_gap" in reasons


def test_overextension_is_limited_to_watch_wait() -> None:
    action, reasons, _ = mod.classify(
        _row(
            research_candidate_source_family="overextension_risk_source",
            research_risk_tags_json=json.dumps(["ma20_overextension_risk", "ma60_overextension_risk"]),
            research_setup_tags_json=json.dumps(["overextension_candidate"]),
        ),
        keep_gated=False,
    )

    assert action == "wait"
    assert "overextension_limited_to_watch_wait" in reasons


def test_select_candidates_uses_family_surfaces_beyond_global_top() -> None:
    rows = pd.DataFrame(
        [
            {"code": "A", "baseline_rank": 1, "baseline_score": 20, "research_candidate_source_family": "uncategorized_source"},
            {"code": "B", "baseline_rank": 2, "baseline_score": 19, "research_candidate_source_family": "uncategorized_source"},
            {"code": "C", "baseline_rank": 3, "baseline_score": 18, "research_candidate_source_family": "uncategorized_source"},
            {"code": "P1", "baseline_rank": 30, "baseline_score": 10, "research_candidate_source_family": "pullback_reclaim_source"},
            {"code": "BR1", "baseline_rank": 40, "baseline_score": 9, "research_candidate_source_family": "breakout_retest_source"},
        ]
    )

    selected = mod.select_candidates(rows, max_rows=10)

    assert set(["P1", "BR1"]).issubset(set(selected["code"]))
    assert "pullback_reclaim_source_top" in set(selected["pick_source"])


def test_resolve_review_date_prefers_current_surface_rows(tmp_path) -> None:
    pd.DataFrame(
        [
            {"decision_date": 20260508, "research_candidate_source_family": "pullback_reclaim_source"},
            {"decision_date": 20260508, "research_candidate_source_family": "breakout_retest_source"},
        ]
    ).to_csv(tmp_path / "current_family_surface_rows.csv", index=False)

    review_date, report = mod.resolve_review_date(tmp_path)

    assert review_date == 20260508
    assert report["latest_global_date_not_used_reason"] is None
    assert report["source_rows_path"].endswith("current_family_surface_rows.csv")


def test_build_pack_marks_stale_review_unavailable(tmp_path) -> None:
    rows = []
    for date, code, family in [
        (20260508, "NEW", "uncategorized_source"),
        (20260306, "OLD", "pullback_reclaim_source"),
    ]:
        rows.append(
            {
                "decision_date": date,
                "code": code,
                "baseline_rank": 1,
                "baseline_score": 10,
                "research_candidate_source_family": family,
                "primary_family": family.replace("_source", "_family"),
                "diagnostic_candidate_role": "unclear_candidate",
                "selected_loser": False,
                "starter_good": False,
                "starter_bad": False,
                "immediate_adverse_entry": False,
                "next_open_available": True,
                "entry_allowed_by_score": True,
                "path20_available": True,
                "research_risk_tags_json": json.dumps(["no_shadow_risk_tag"]),
                "research_setup_tags_json": json.dumps(["pullback_candidate"]),
                "research_regime_tags_json": json.dumps([]),
                "source_artifact_path": "snapshot.csv",
                "source_run_id": "test",
            }
        )
    pd.DataFrame(rows).to_csv(tmp_path / "candidate_family_source_rows.csv", index=False)
    pd.DataFrame([rows[1]]).to_csv(tmp_path / "family_surface_pullback_reclaim.csv", index=False)

    out = mod.build_pack(tmp_path, tmp_path / "out")
    summary = json.loads((out / "review_pack_summary.json").read_text(encoding="utf-8"))
    decision = json.loads((out / "review_pack_decision.json").read_text(encoding="utf-8"))

    assert summary["stale_review_pack"] is True
    assert summary["manual_review_available"] is False
    assert decision["blocker_reason"] == "stale review pack; do not use as current candidate"


def test_build_pack_marks_all_uncategorized_unavailable(tmp_path) -> None:
    pd.DataFrame(
        [
            {
                "decision_date": 20260508,
                "code": "NEW",
                "baseline_rank": 1,
                "baseline_score": 10,
                "research_candidate_source_family": "uncategorized_source",
                "primary_family": "uncategorized_family",
                "diagnostic_candidate_role": "unclear_candidate",
                "selected_loser": False,
                "starter_good": False,
                "starter_bad": False,
                "immediate_adverse_entry": False,
                "next_open_available": True,
                "entry_allowed_by_score": True,
                "path20_available": True,
                "research_risk_tags_json": json.dumps(["no_shadow_risk_tag"]),
                "research_setup_tags_json": json.dumps(["uncategorized_candidate"]),
                "research_regime_tags_json": json.dumps([]),
                "source_artifact_path": "snapshot.csv",
                "source_run_id": "test",
                "feature_freshness_status": "fresh",
                "provisional_used": False,
            }
        ]
    ).to_csv(tmp_path / "current_family_surface_rows.csv", index=False)

    out = mod.build_pack(tmp_path, tmp_path / "out")
    decision = json.loads((out / "review_pack_decision.json").read_text(encoding="utf-8"))

    assert decision["manual_review_available"] is False
    assert decision["blocker_reason"] == "family_assignment_unavailable"
