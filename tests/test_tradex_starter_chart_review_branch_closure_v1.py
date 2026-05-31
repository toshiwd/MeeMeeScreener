from __future__ import annotations

from scripts import tradex_starter_chart_review_branch_closure_v1 as mod


def test_supported_signature_rows_marks_supported_and_negative() -> None:
    metrics = {
        "a": {"sample_count": 12, "mean_ret20": 0.1, "comparison_vs_untagged_rows": {"mean_ret20_delta_tagged_minus_untagged": 0.02, "sample_allows_comparison": True}},
        "b": {"sample_count": 1, "mean_ret20": -0.01, "comparison_vs_untagged_rows": {"mean_ret20_delta_tagged_minus_untagged": -0.1, "sample_allows_comparison": False}},
    }

    rows = mod.supported_signature_rows(metrics)

    assert rows[0]["signature"] == "a"
    assert rows[0]["supported"] is True
    assert rows[1]["worse_than_untagged"] is True


def test_decide_closes_when_supported_signatures_not_negative_and_negative_is_thin() -> None:
    rows = [
        {"signature": "supported_good", "sample_count": 16, "supported": True, "worse_than_untagged": False},
        {"signature": "thin_bad", "sample_count": 1, "supported": False, "worse_than_untagged": True},
    ]

    assert mod.decide_closure(rows, {"decision": "feature_context_ready_for_signature_pretest"}) == "close_branch_no_reusable_signal"


def test_decide_needs_formal_pretest_when_supported_negative_exists() -> None:
    rows = [{"signature": "supported_bad", "sample_count": 12, "supported": True, "worse_than_untagged": True}]

    assert mod.decide_closure(rows, {"decision": "feature_context_ready_for_signature_pretest"}) == "needs_formal_signature_pretest_before_close"
