from __future__ import annotations

import json
from pathlib import Path


LATEST_BUNDLE = Path(r"G:\Tradex\high_recall_filter_revision_v1\20260502T144909Z-320427")


def _load(name: str) -> dict:
    return json.loads((LATEST_BUNDLE / name).read_text(encoding="utf-8"))


def test_latest_bundle_decision_is_side_specific_contract() -> None:
    decision = _load("high_recall_filter_revision_v1_decision.json")
    recommendation = _load("high_recall_filter_revision_recommendation.json")

    assert decision["decision"] == "needs_side_specific_high_recall_contract"
    assert decision["status"] == "needs_side_specific_high_recall_contract"
    assert decision["best_filter_variant"] == "filter_no_exclude_analysis_only"
    assert recommendation["recommended_next_path"] == "needs_side_specific_high_recall_contract"
    assert recommendation["selected_tier_failure_summary"]["exclude_analysis_only_beneficial"] is False


def test_selected_tier_audit_shows_exclude_analysis_only_is_not_beneficial() -> None:
    selected = _load("selected_tier_failure_audit.json")

    assert selected["exclude_analysis_only_beneficial"] is False
    assert selected["risk_flagged_rows_guard_too_loose"] is False
    assert selected["topk"]["top10"]["by_tier"]["exclude_analysis_only"]["mean_forward_ret_20d"] < 0
    assert selected["topk"]["top10"]["by_tier"]["exclude_analysis_only"]["top15_capture_rate"] == 0.0
