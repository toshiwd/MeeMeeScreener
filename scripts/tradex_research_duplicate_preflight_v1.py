from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


INDEX_ROOT = Path(r"G:\Tradex\research_knowledge_registry_v1")
REQUIRED_DESCRIPTOR = ("family", "candidate_id", "side", "top_k", "fixed_contract", "evaluation_units", "ordered_topk", "code_version", "data_version")


def _json(v: Any) -> str:
    return json.dumps(v, ensure_ascii=False, sort_keys=True, default=str)


def _hash(v: Any) -> str:
    return hashlib.sha256(_json(v).encode()).hexdigest()


def latest_index(root: Path = INDEX_ROOT) -> Path:
    candidates = sorted(p / "behavioral_duplicate_index.jsonl" for p in root.glob("*-tradex_behavioral_duplicate_index_v1") if (p / "behavioral_duplicate_index.jsonl").is_file())
    if not candidates:
        raise FileNotFoundError("no behavioral duplicate index")
    return candidates[-1]


def load_rows(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8-sig").splitlines() if line.strip()]


def descriptor_fingerprint(candidate: dict[str, Any]) -> tuple[str, list[str]]:
    missing = [key for key in REQUIRED_DESCRIPTOR if candidate.get(key) in (None, "", [], {})]
    if candidate.get("features") in (None, "", [], {}) and candidate.get("conditions") in (None, "", [], {}):
        missing.append("features_or_conditions")
    if missing:
        return "incomplete", missing
    definition = {key: candidate[key] for key in REQUIRED_DESCRIPTOR if key != "ordered_topk"}
    definition["features_or_conditions"] = candidate.get("features") or candidate.get("conditions")
    return _hash(definition), []


def _same_scope(candidate: dict[str, Any], row: dict[str, Any]) -> bool:
    scope = row.get("comparison_scope") if isinstance(row.get("comparison_scope"), dict) else {}
    return bool(
        candidate.get("fixed_contract") == scope.get("fixed_contract")
        and candidate.get("side") == scope.get("side")
        and candidate.get("top_k") == scope.get("top_k")
        and candidate.get("evaluation_units") == scope.get("evaluation_units")
    )


def evaluate(candidate: dict[str, Any], rows: list[dict[str, Any]], *, index_path: str = "") -> dict[str, Any]:
    fingerprint, missing = descriptor_fingerprint(candidate)
    candidate_explicit_fp = candidate.get("configuration_fingerprint")
    effective_fp = str(candidate_explicit_fp) if candidate_explicit_fp else fingerprint
    exact = [row for row in rows if effective_fp != "incomplete" and row.get("fingerprint_status") == "complete"
             and row.get("canonical_configuration_fingerprint") == effective_fp]
    behavioral = []
    ordered = candidate.get("ordered_topk")
    ordered_complete = isinstance(ordered, list) and len(ordered) == candidate.get("top_k") and len(set(map(str, ordered))) == len(ordered)
    if ordered_complete:
        ranked_hash = _hash([str(x) for x in ordered])
        behavioral = [row for row in rows if _same_scope(candidate, row) and row.get("top10_ranked_hash") == ranked_hash]
    claims = [row for row in rows if (row.get("candidate_id") == candidate.get("candidate_id") or row.get("family") == candidate.get("family"))
              and (row.get("identical_top10_explicit") is True or row.get("ranking_no_op_explicit") is True
                   or row.get("source_peer_duplicate_claims") or row.get("source_overlap_claims"))]
    if exact:
        decision, reasons, matched = "block_exact_duplicate", ["complete_configuration_fingerprint_exact_match"], exact
    elif behavioral:
        decision, reasons, matched = "block_behavioral_duplicate", ["complete_ordered_topk_match_under_same_contract_side_k_and_evaluation_units"], behavioral
    elif claims:
        decision, reasons, matched = "review_source_duplicate_claim", ["source_contains_explicit_duplicate_or_noop_claim_not_recomputed_as_duplicate"], claims
    else:
        decision, reasons, matched = "allow_with_unknown_duplicate_risk", ["no_provable_duplicate_match"] , []
        if effective_fp == "incomplete":
            reasons.append("configuration_fingerprint_incomplete")
        if not ordered_complete:
            reasons.append("ordered_topk_incomplete")
    return {
        "schema_version": "tradex_research_duplicate_preflight_v1", "generated_at": datetime.now(timezone.utc).isoformat(),
        "decision": decision, "candidate_id": candidate.get("candidate_id"), "family": candidate.get("family"),
        "configuration_fingerprint": effective_fp, "reasons": reasons, "missing_inputs": missing,
        "ordered_topk_complete": ordered_complete, "index_path": index_path,
        "matched_rows": [{"candidate_id": r.get("candidate_id"), "family": r.get("family"), "sources": r.get("sources", [])} for r in matched],
        "runtime_db_write": False, "production_ranking_changed": False, "meemee_changed": False,
    }


def run(descriptor_path: Path, output_path: Path, index_path: Path | None = None) -> Path:
    candidate = json.loads(descriptor_path.read_text(encoding="utf-8-sig"))
    if not isinstance(candidate, dict):
        raise ValueError("candidate descriptor must be an object")
    selected = index_path or latest_index()
    result = evaluate(candidate, load_rows(selected), index_path=str(selected))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return output_path


if __name__ == "__main__":
    p = argparse.ArgumentParser(); p.add_argument("descriptor", type=Path); p.add_argument("--index", type=Path); p.add_argument("--output", type=Path, required=True)
    a = p.parse_args(); print(run(a.descriptor, a.output, a.index))
