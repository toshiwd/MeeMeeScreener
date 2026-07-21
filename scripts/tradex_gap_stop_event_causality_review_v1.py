"""Create the reviewed event-causality rollup for free gap-stop disclosure backfill."""
import argparse, hashlib, json
from pathlib import Path
import pandas as pd


def sha(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()


REVIEWS = {
    "GAP008": {"causality": "possible", "cause_family": "restructuring", "note": "gap-day 08:30 workforce transition result; direction not independently confirmed"},
    "GAP016": {"causality": "likely", "cause_family": "buyback", "note": "buyback decision at 15:00 on prior session; Fact Book is supporting same-time disclosure"},
    "GAP018": {"causality": "likely", "cause_family": "buyback", "note": "buyback decision at 15:00 before holiday and next-session gap stop"},
    "GAP024": {"causality": "possible", "cause_family": "debt_redemption", "note": "early bond redemption disclosed prior day; positive price impact not independently confirmed"},
    "GAP031": {"causality": "unlikely", "cause_family": "website_incident", "note": "website incident at entry-day 09:00 is unlikely to explain later positive gap"},
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--enriched", type=Path, required=True); ap.add_argument("--matches", type=Path, required=True)
    ap.add_argument("--parent-compare", type=Path, required=True); ap.add_argument("--output", type=Path, required=True)
    a = ap.parse_args(); a.output.mkdir(parents=True, exist_ok=False)
    cases = pd.read_csv(a.enriched, encoding="utf-8-sig", dtype={"code": str})
    matches = pd.read_csv(a.matches, encoding="utf-8-sig", dtype={"code": str})
    cases["causality"] = cases.case_id.map(lambda case: REVIEWS.get(case, {}).get("causality", "none_found"))
    cases["cause_family"] = cases.case_id.map(lambda case: REVIEWS.get(case, {}).get("cause_family", "none_found"))
    cases["causality_note"] = cases.case_id.map(lambda case: REVIEWS.get(case, {}).get("note", "no disclosure found in complete secondary TDNET index window"))
    cases.to_csv(a.output / "gap_stop_event_causality_review.csv", index=False, encoding="utf-8-sig")
    likely = int(cases.causality.eq("likely").sum()); possible = int(cases.causality.eq("possible").sum())
    earnings = int(matches.event_class.eq("earnings").sum()) if len(matches) else 0
    buyback_cases = int(cases.cause_family.eq("buyback").sum())
    result = {
        "schema_version": "tradex_gap_stop_event_causality_review_v1.compare.v1",
        "artifact_role": "authoritative_gap_stop_event_causality_review",
        "review_only": True, "research_phase": "effectiveness_judgment",
        "fixed_conditions": {"cases": "fixed 31 gap-stop cases", "source": "free secondary TDNET index plus linked disclosure pages",
                             "causality_levels": ["likely", "possible", "unlikely", "none_found"],
                             "likely_contract": "material disclosure timing directly precedes gap stop and direction is plausibly positive"},
        "authoritative_result": {"cases": int(len(cases)), "any_disclosure_cases": int(cases.free_backfill_status.eq("event_found").sum()),
                                 "likely_event_caused_cases": likely, "possible_event_caused_cases": possible,
                                 "likely_or_possible_rate": float((likely + possible) / len(cases)),
                                 "likely_rate": float(likely / len(cases)), "earnings_cases": earnings,
                                 "buyback_cases": buyback_cases, "no_disclosure_cases": int(cases.free_backfill_status.eq("no_event_found_secondary_index").sum()),
                                 "causality_counts": {str(k): int(v) for k, v in cases.causality.value_counts().items()}},
        "observed_branching": {"changed_top5_members_count": None, "changed_top10_members_count": None, "changed_rank_count": None,
                               "selection_divergence_reason": "gap-stop cases separated by reviewed event causality"},
        "judgment": {"candidate_local_decision": "drop", "session_aggregate_decision": "earnings_crossing_not_main_gap_stop_cause",
                     "authoritative_rollup_decision": "drop_earnings_main_cause_keep_buyback_minority_risk",
                     "reason_type": "zero_earnings_and_only_two_likely_material_event_cases"},
        "not_changed": ["selector", "sizing", "MeeMee", "runtime DB", "production logic"],
    }
    cp = a.output / "compare.json"; cp.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {"sources": {"enriched": {"path": str(a.enriched.resolve()), "sha256": sha(a.enriched)},
                         "matches": {"path": str(a.matches.resolve()), "sha256": sha(a.matches)},
                         "parent_compare": {"path": str(a.parent_compare.resolve()), "sha256": sha(a.parent_compare)}},
             "review_rows": int(len(cases)), "unique_cases": int(cases.case_id.nunique()),
             "review_sha256": sha(a.output / "gap_stop_event_causality_review.csv"), "compare_sha256": sha(cp)}
    (a.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (a.output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(cp)}, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(a.output), "result": result["authoritative_result"], "judgment": result["judgment"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__": main()
