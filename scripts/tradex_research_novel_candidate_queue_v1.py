from __future__ import annotations

import argparse
import glob
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))
from scripts.tradex_research_duplicate_preflight_v1 import evaluate, latest_index, load_rows


AXIS_ID = "tradex_research_novel_candidate_queue_v1"
ROOT = Path(r"G:\Tradex\research_knowledge_registry_v1")
INVENTORY = Path("artifacts/research_inventory")
PRIORITY = ("invalid_revalidation", "exact_duplicate_block", "behavioral_source_claim_review", "drop_freeze_closed", "planned_instrumentation_gap", "blocked_missing_baseline_contract", "low_sample_hold", "unresolved_failure_bucket", "novel_candidate_planned_unknown_2026", "novel_candidate")


def _latest(pattern: str) -> Path | None:
    paths = sorted(glob.glob(pattern)); return Path(paths[-1]) if paths else None


def _read(path: Path | None) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig")) if path and path.is_file() else {}


def _inputs(root: Path, inventory: Path) -> dict[str, Any]:
    registry = _latest(str(root / "*-tradex_research_knowledge_registry_v1" / "research_registry.parquet"))
    cards = _latest(str(root / "*-tradex_short_entry_family_cards_v1" / "short_entry_family_cards.jsonl"))
    interactions = sorted(Path(p) for p in glob.glob(str(root / "*revalidation*" / "revalidation_result.json")))
    duplicate = latest_index(root)
    return {"registry": registry, "cards": cards, "interactions": interactions, "duplicate": duplicate,
            "champion": inventory / "champion_bad_pick_ledger.json", "gate": inventory / "candidate_gate_decision.json", "pruning": inventory / "pruning_decision.json"}


def _closed(inputs: dict[str, Any]) -> set[str]:
    closed: set[str] = set()
    registry = inputs.get("registry")
    if registry:
        df = pd.read_parquet(registry)
        for row in df.itertuples(index=False):
            if str(getattr(row, "normalized_decision", "")) == "drop":
                closed.add(str(getattr(row, "family_id", "")))
    cards = inputs.get("cards")
    if cards:
        for line in cards.read_text(encoding="utf-8-sig").splitlines():
            row = json.loads(line)
            if row.get("normalized_decision") == "drop":
                closed |= {str(row.get("family")), f"{row.get('family')}:{row.get('axis')}"}
    pruning = _read(inputs.get("pruning"))
    for key in ("frozen_peers", "execution_focus"):
        value = pruning.get(key)
        if isinstance(value, list): closed |= {str(x) for x in value}
    return closed


def candidate_proposals(inputs: dict[str, Any]) -> list[dict[str, Any]]:
    champion = _read(inputs.get("champion"))
    gate = _read(inputs.get("gate"))
    unresolved = [str(x) for x in champion.get("market_failure_buckets", [])]
    proposals = []
    latest_interactions: dict[str, dict[str, Any]] = {}
    for path in inputs.get("interactions", []):
        result = _read(path); periods = result.get("periods") if isinstance(result.get("periods"), dict) else {}
        diagnostic = (result.get("comparisons") or {}).get("untouched_diagnostic") or {}
        intersection = diagnostic.get("C_intersection") if isinstance(diagnostic, dict) else {}
        changed_axis = str(result.get("changed_axis") or "")
        proposal = {
            "candidate_id": f"revalidate:{result.get('candidate') or changed_axis}", "family": "multitimeframe_context",
            "side": "short", "top_k": None, "fixed_contract": {"entry": result.get("entry"), "horizon": result.get("horizon"), "periods": periods},
            "evaluation_units": list(periods), "ordered_topk": None, "code_version": result.get("schema_version"), "data_version": result.get("dataset_version"),
            "conditions": [changed_axis], "failure_bucket": "regime_mismatch" if "regime_mismatch" in unresolved else None,
            "relevance_2026": periods.get("untouched_diagnostic"), "single_axis": changed_axis,
            "difference_from_existing_family": "daily/weekly/monthly interaction context rather than current adaptive short shape family",
            "required_instrumentation": ["point_in_time_context", "complete_horizon", "ordered_topk_if_ranking_compare"],
            "revalidation_valid": bool(result.get("no_lookahead_all")) and int((intersection or {}).get("n") or 0) > 0,
            "diagnostic_sample_n": (intersection or {}).get("n"), "source_path": str(path),
        }
        latest_interactions[proposal["candidate_id"]] = proposal
    proposals.extend(latest_interactions.values())
    planned_by_id: dict[str, dict[str, Any]] = {}
    for item in gate.get("next_round_candidates", []):
        if not isinstance(item, dict) or item.get("run_state") != "planned" or not item.get("candidate_name"):
            continue
        cid = str(item["candidate_name"])
        planned_by_id[cid] = {
            "candidate_id": cid, "family": str(item.get("feature_class") or "unknown"), "side": "unknown", "top_k": None,
            "fixed_contract": gate.get("comparability_contract") if isinstance(gate.get("comparability_contract"), dict) else {},
            "evaluation_units": None, "ordered_topk": None, "code_version": None, "data_version": None,
            "conditions": ["cutoff_margin_pressure_only"] if cid == "outcome_only_cutoff_margin_branch_v1" else [],
            "failure_bucket": item.get("target_failure_bucket"), "run_state": "planned", "relevance_2026": "unknown",
            "single_axis": "cutoff-margin pressure only" if cid == "outcome_only_cutoff_margin_branch_v1" else item.get("feature_class"),
            "expected_to_move": item.get("expected_to_move"), "acceptance_criteria": item.get("acceptance_criteria", []),
            "must_not_change": item.get("must_not_change", []), "missing_evidence": item.get("missing_evidence", []),
            "plan_artifact": item.get("plan_artifact"),
            "difference_from_existing_family": "cutoff-margin pressure branch; baseline outcome_only ranking remains closed" if cid == "outcome_only_cutoff_margin_branch_v1" else None,
            "required_instrumentation": list(item.get("acceptance_criteria", [])), "source_path": str(inputs.get("gate")),
        }
    latest_round = champion.get("latest_outcome_only_core_track_round")
    if isinstance(latest_round, dict) and latest_round.get("next_outcome_only_candidate"):
        cid = str(latest_round["next_outcome_only_candidate"])
        if cid in planned_by_id:
            planned_by_id[cid]["champion_next_candidate_source"] = str(inputs.get("champion"))
            planned_by_id[cid]["next_candidate_purpose"] = latest_round.get("next_outcome_only_candidate_purpose")
            planned_by_id[cid]["baseline_state_reference_only"] = latest_round.get("baseline_state")
            planned_by_id[cid]["baseline_decision_reference_only"] = latest_round.get("decision")
            planned_by_id[cid]["plan_artifact"] = planned_by_id[cid].get("plan_artifact") or latest_round.get("branching_redesign_plan_artifact")
            planned_by_id[cid]["baseline_authoritative_artifact"] = latest_round.get("compare_path")
    for item in planned_by_id.values():
        plan = Path(str(item.get("plan_artifact"))) if item.get("plan_artifact") else None
        baseline = Path(str(item.get("baseline_authoritative_artifact"))) if item.get("baseline_authoritative_artifact") else None
        item["execution_preconditions"] = {
            "plan_artifact_exists": bool(plan and plan.is_file()),
            "baseline_authoritative_artifact_exists": bool(baseline and baseline.is_file()),
            "scorer_formula_explicit": False,
            "scorer_method_implemented": False,
        }
        item["required_inputs"] = ["existing_plan_artifact", "existing_baseline_authoritative_artifact", "explicit_scorer_formula", "implemented_scorer_method"]
    proposals.extend(planned_by_id.values())
    return proposals


def classify(candidate: dict[str, Any], duplicate_rows: list[dict[str, Any]], closed: set[str]) -> tuple[str, dict[str, Any]]:
    descriptor = {k: candidate.get(k) for k in ("family", "candidate_id", "side", "top_k", "fixed_contract", "evaluation_units", "ordered_topk", "code_version", "data_version", "conditions")}
    preflight = evaluate(descriptor, duplicate_rows)
    if candidate.get("revalidation_valid") is False:
        return "invalid_revalidation", preflight
    if preflight["decision"] in {"block_exact_duplicate", "block_behavioral_duplicate"}:
        return "exact_duplicate_block", preflight
    if preflight["decision"] == "review_source_duplicate_claim":
        return "behavioral_source_claim_review", preflight
    if candidate.get("family") in closed or f"{candidate.get('family')}:{candidate.get('single_axis')}" in closed:
        return "drop_freeze_closed", preflight
    n = candidate.get("diagnostic_sample_n")
    if isinstance(n, (int, float)) and n < 10:
        return "low_sample_hold", preflight
    if candidate.get("run_state") == "planned" and candidate.get("relevance_2026") == "unknown":
        preconditions = candidate.get("execution_preconditions") if isinstance(candidate.get("execution_preconditions"), dict) else {}
        if not preconditions.get("baseline_authoritative_artifact_exists"):
            return "blocked_missing_baseline_contract", preflight
        if not preconditions or not all(preconditions.values()):
            return "planned_instrumentation_gap", preflight
        planned_required = (candidate.get("failure_bucket"), candidate.get("single_axis"), candidate.get("expected_to_move"),
                            candidate.get("acceptance_criteria"), candidate.get("must_not_change"), candidate.get("plan_artifact"))
        return ("novel_candidate_planned_unknown_2026" if all(planned_required) else "unresolved_failure_bucket"), preflight
    required = (candidate.get("failure_bucket"), candidate.get("relevance_2026"), candidate.get("single_axis"),
                candidate.get("difference_from_existing_family"), candidate.get("required_instrumentation"))
    if not all(required):
        return "unresolved_failure_bucket", preflight
    return "novel_candidate", preflight


def build(inputs: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows = load_rows(inputs["duplicate"]); closed = _closed(inputs)
    accepted, rejected = [], []
    for candidate in candidate_proposals(inputs):
        classification, preflight = classify(candidate, rows, closed)
        item = {**candidate, "classification": classification, "classification_priority": PRIORITY.index(classification), "descriptor": {k: candidate.get(k) for k in ("family", "candidate_id", "side", "top_k", "fixed_contract", "evaluation_units", "ordered_topk", "code_version", "data_version", "conditions")}, "duplicate_preflight": preflight}
        (accepted if classification in {"novel_candidate", "novel_candidate_planned_unknown_2026"} else rejected).append(item)
    accepted.sort(key=lambda x: (x["classification_priority"], x["candidate_id"]))
    rejected.sort(key=lambda x: (x["classification_priority"], x["candidate_id"]))
    return accepted[:3], rejected


def run(root: Path = ROOT, inventory: Path = INVENTORY) -> Path:
    inputs = _inputs(root, inventory); queue, rejected = build(inputs)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"); out = root / f"{stamp}-{AXIS_ID}"; out.mkdir(parents=True, exist_ok=False)
    (out / "novel_candidate_queue.json").write_text(json.dumps({"schema_version": f"{AXIS_ID}.queue.v1", "candidates": queue}, ensure_ascii=False, indent=2)+"\n", encoding="utf-8")
    (out / "rejected_candidates.json").write_text(json.dumps({"schema_version": f"{AXIS_ID}.rejected.v1", "candidates": rejected}, ensure_ascii=False, indent=2)+"\n", encoding="utf-8")
    manifest = {"schema_version": f"{AXIS_ID}.manifest.v1", "generated_at": datetime.now(timezone.utc).isoformat(), "source_paths": {k: [str(x) for x in v] if isinstance(v, list) else str(v) for k,v in inputs.items()}, "queue_count": len(queue), "executable_count": len(queue), "blocked_planned_count": sum(x["classification"] in {"planned_instrumentation_gap", "blocked_missing_baseline_contract"} for x in rejected), "rejected_count": len(rejected), "maximum_queue": 3, "effect_inference_from_numbers": False, "runtime_db_write": False, "production_ranking_changed": False, "meemee_changed": False}
    (out / "manifest.json").write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+"\n",encoding="utf-8"); return out


if __name__ == "__main__":
    p=argparse.ArgumentParser();p.add_argument("--root",type=Path,default=ROOT);p.add_argument("--inventory",type=Path,default=INVENTORY);a=p.parse_args();print(run(a.root,a.inventory))
