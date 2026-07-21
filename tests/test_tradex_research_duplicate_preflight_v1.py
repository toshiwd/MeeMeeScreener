from scripts.tradex_research_duplicate_preflight_v1 import _hash, descriptor_fingerprint, evaluate


def _candidate() -> dict:
    return {"family": "f", "candidate_id": "new", "side": "short", "top_k": 10, "fixed_contract": {"id": "c"},
            "evaluation_units": ["m1"], "ordered_topk": [str(i) for i in range(10)], "code_version": "abc", "data_version": "d1", "features": ["x"]}


def test_blocks_exact_complete_fingerprint() -> None:
    c = _candidate(); fp, _ = descriptor_fingerprint(c)
    rows = [{"candidate_id": "old", "family": "f", "fingerprint_status": "complete", "canonical_configuration_fingerprint": fp, "sources": [{"path": "a"}]}]
    result = evaluate(c, rows)
    assert result["decision"] == "block_exact_duplicate" and result["matched_rows"][0]["candidate_id"] == "old"


def test_blocks_behavioral_only_with_same_complete_scope() -> None:
    c = _candidate(); c["configuration_fingerprint"] = "different"
    rows = [{"candidate_id": "old", "family": "f", "fingerprint_status": "complete", "canonical_configuration_fingerprint": "old",
             "comparison_scope": {"fixed_contract": {"id": "c"}, "side": "short", "top_k": 10, "evaluation_units": ["m1"]},
             "top10_ranked_hash": _hash(c["ordered_topk"]), "sources": []}]
    assert evaluate(c, rows)["decision"] == "block_behavioral_duplicate"


def test_source_claim_is_review_not_block() -> None:
    c = _candidate(); c["configuration_fingerprint"] = "different"; c["ordered_topk"] = []
    rows = [{"candidate_id": "old", "family": "f", "identical_top10_explicit": True, "sources": [{"path": "a"}]}]
    assert evaluate(c, rows)["decision"] == "review_source_duplicate_claim"


def test_missing_inputs_allows_with_unknown_risk() -> None:
    result = evaluate({"family": "f", "candidate_id": "x"}, [])
    assert result["decision"] == "allow_with_unknown_duplicate_risk"
    assert "ordered_topk" in result["missing_inputs"] and "configuration_fingerprint_incomplete" in result["reasons"]
