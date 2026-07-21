from scripts.tradex_research_novel_candidate_queue_v1 import candidate_proposals, classify


def _candidate() -> dict:
    return {"candidate_id":"x","family":"new","side":"short","top_k":10,"fixed_contract":{"id":"c"},"evaluation_units":["2026"],"ordered_topk":[str(i) for i in range(10)],"code_version":"v","data_version":"d","conditions":["one"],"failure_bucket":"regime_mismatch","relevance_2026":[20260301,20260710],"single_axis":"one","difference_from_existing_family":"different","required_instrumentation":["pit"],"revalidation_valid":True,"diagnostic_sample_n":20}


def test_invalid_revalidation_has_highest_priority() -> None:
    c=_candidate();c["revalidation_valid"]=False
    assert classify(c,[],set())[0]=="invalid_revalidation"


def test_duplicate_block_excludes_candidate() -> None:
    c=_candidate();c["configuration_fingerprint"]="unused"
    rows=[{"candidate_id":"old","family":"new","fingerprint_status":"complete","canonical_configuration_fingerprint":"different","comparison_scope":{"fixed_contract":{"id":"c"},"side":"short","top_k":10,"evaluation_units":["2026"]},"top10_ranked_hash":__import__('scripts.tradex_research_duplicate_preflight_v1',fromlist=['_hash'])._hash(c['ordered_topk']),"sources":[]}]
    assert classify(c,rows,set())[0]=="exact_duplicate_block"


def test_low_sample_hold_precedes_novel() -> None:
    c=_candidate();c["diagnostic_sample_n"]=8;c["ordered_topk"]=None
    assert classify(c,[],set())[0]=="low_sample_hold"


def test_complete_novel_candidate_passes() -> None:
    c=_candidate();c["ordered_topk"]=None;c["top_k"]=None
    assert classify(c,[],set())[0]=="novel_candidate"


def test_planned_candidate_is_not_closed_by_baseline_drop(tmp_path) -> None:
    gate=tmp_path/'gate.json';champ=tmp_path/'champ.json'
    gate.write_text(__import__('json').dumps({"comparability_contract":{"same_top_k":True},"next_round_candidates":[{"candidate_name":"outcome_only_cutoff_margin_branch_v1","run_state":"planned","feature_class":"outcome_only_cutoff_branching","target_failure_bucket":"ranking_no_op","expected_to_move":"move cutoff","acceptance_criteria":["instrument branching"],"must_not_change":["contract"],"missing_evidence":["no run"],"plan_artifact":"plan.json"}]}),encoding='utf-8')
    champ.write_text(__import__('json').dumps({"market_failure_buckets":[],"latest_outcome_only_core_track_round":{"decision":"drop","baseline_state":"drop/no_op_closed","next_outcome_only_candidate":"outcome_only_cutoff_margin_branch_v1"}}),encoding='utf-8')
    items=candidate_proposals({"gate":gate,"champion":champ,"interactions":[]})
    assert len(items)==1 and items[0]["baseline_state_reference_only"]=="drop/no_op_closed"
    assert items[0]["execution_preconditions"]["scorer_formula_explicit"] is False
    assert classify(items[0],[],set())[0]=="blocked_missing_baseline_contract"


def test_planned_candidate_with_baseline_but_missing_scorer_is_instrumentation_gap() -> None:
    c=_candidate();c.update({"run_state":"planned","relevance_2026":"unknown","expected_to_move":"x","acceptance_criteria":["a"],"must_not_change":["c"],"plan_artifact":"p","execution_preconditions":{"plan_artifact_exists":True,"baseline_authoritative_artifact_exists":True,"scorer_formula_explicit":False,"scorer_method_implemented":False}})
    assert classify(c,[],set())[0]=="planned_instrumentation_gap"
