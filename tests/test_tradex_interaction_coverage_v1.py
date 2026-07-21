from scripts.tradex_interaction_coverage_v1 import build_coverage, classify_cell


def row(**overrides):
    base = {
        "signal_side": "short",
        "state_combination": "daily=x|weekly=y",
        "sample_count": 50,
        "sample_threshold": 30,
        "coverage_status": "usable",
    }
    base.update(overrides)
    return base


def test_classification_uses_explicit_evidence_not_return_inference():
    assert classify_cell(row(stable_sign_match=True, mfe_20_mean=-99)) == "positive"
    assert classify_cell(row(stable_sign_match=False, mfe_20_mean=99)) == "negative"
    assert classify_cell(row(mfe_20_mean=99)) == "tested"


def test_thin_and_invalid_precede_polarity():
    assert classify_cell(row(sample_count=10, stable_sign_match=True)) == "thin"
    assert classify_cell(row(state_combination="", stable_sign_match=True)) == "invalid"


def test_unstable_is_thin_and_unclassified_is_unknown():
    assert classify_cell(row(coverage_status="unstable")) == "thin"
    assert classify_cell(row(coverage_status="other")) == "unknown"


def test_gap_copy_does_not_claim_single_axis_quality_evidence():
    _, gaps = build_coverage()
    assert "observations exist" in gaps["selection_contract"]
    assert gaps["support_semantics"].endswith("not quality evidence")
    for candidate in gaps["candidates"]:
        assert candidate["reason"] == "single_axis_observations_exist_interaction_untested"
        assert candidate["single_axis_support_semantics"] == "observation_count_not_quality_evidence"
        assert candidate["decision"] == "research_gap_not_trade_adoption"
