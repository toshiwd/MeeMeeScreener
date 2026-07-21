from __future__ import annotations

import argparse
import hashlib
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


AXIS_ID = "tradex_behavioral_duplicate_index_v1"
SOURCE_ROOT = Path("artifacts/research_inventory")
OUT = Path(r"G:\Tradex\research_knowledge_registry_v1")
NORMALIZE = {"keep": "keep", "hold": "hold", "drop": "drop"}
MEMBERSHIP_KEYS = ("top10_members", "top10_codes", "top10_symbols", "top_10_members", "top_10_codes")


def _json(v: Any) -> str:
    return json.dumps(v, ensure_ascii=False, sort_keys=True, default=str)


def _hash_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _file_hash(path: Path) -> str:
    return _hash_bytes(path.read_bytes())


def source_paths(root: Path = SOURCE_ROOT) -> list[Path]:
    paths = set(root.glob("bp_*.json")) | set(root.glob("bad_pick_*.json"))
    paths |= {root / "champion_bad_pick_ledger.json", root / "candidate_gate_decision.json"}
    return sorted(p for p in paths if p.is_file())


def _family(candidate: str, item: dict[str, Any]) -> str:
    explicit = item.get("family_name") or item.get("family_id")
    if explicit:
        return str(explicit)
    name = candidate.lower()
    if name.startswith("bp_") or "liquidity" in name or name.startswith("liq_"):
        return "bad_pick_liquidity"
    if "regime" in name:
        return "regime"
    return "unknown"


def _candidate(item: dict[str, Any], fallback: str) -> str:
    return str(item.get("candidate_id") or item.get("candidate_name") or item.get("unit_name") or item.get("symbol") or fallback)


def _membership(value: Any) -> list[str] | None:
    if isinstance(value, dict):
        for key in MEMBERSHIP_KEYS:
            found = value.get(key)
            if isinstance(found, list) and found:
                ordered = [str(x) for x in found]
                return ordered if len(ordered) == len(set(ordered)) else None
        for child in value.values():
            found = _membership(child)
            if found:
                return found
    elif isinstance(value, list):
        for child in value:
            found = _membership(child)
            if found:
                return found
    return None


def _explicit_flag(item: dict[str, Any], token: str) -> bool | str:
    # Only explicit source language is accepted; numeric zero is never translated.
    text = _json(item).lower()
    return True if token in text else "unknown"


def _comparison_scope(item: dict[str, Any]) -> dict[str, Any]:
    contract = item.get("same_condition_contract") or item.get("comparison_contract")
    contract = contract if isinstance(contract, dict) else None
    metrics = item.get("metrics") if isinstance(item.get("metrics"), dict) else {}
    top_k = item.get("top_k") or (contract or {}).get("top_k")
    side = item.get("side") or (contract or {}).get("side")
    units = item.get("evaluation_units") or (contract or {}).get("evaluation_units")
    return {"fixed_contract": contract, "side": side, "top_k": top_k, "evaluation_units": units,
            "changed_top10_source": metrics.get("changed_top10_members_count") if "changed_top10_members_count" in metrics else item.get("changed_top10_members_count")}


def _definition(item: dict[str, Any]) -> tuple[str, str, list[str]]:
    fields = {}
    for key in ("candidate_id", "candidate_name", "unit_name", "family_name", "family_id", "feature_class", "target_failure_bucket", "comparison_contract", "same_condition_contract", "frozen_challenger_definition", "conditions", "features"):
        if item.get(key) not in (None, "", [], {}):
            fields[key] = item[key]
    missing = []
    if not any(k in fields for k in ("candidate_id", "candidate_name", "unit_name")):
        missing.append("candidate_id")
    if not any(k in fields for k in ("family_name", "family_id")):
        missing.append("family")
    if not any(k in fields for k in ("feature_class", "comparison_contract", "same_condition_contract", "frozen_challenger_definition", "conditions", "features")):
        missing.append("definition_or_conditions")
    return (_hash_bytes(_json(fields).encode()) if not missing else "incomplete", "complete" if not missing else "incomplete", missing)


def _expand(path: Path, payload: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    expanded: list[tuple[str, dict[str, Any]]] = []
    for key in ("unit_decisions", "liquidity_decomposition_runs", "candidates", "candidate_decisions", "entries"):
        rows = payload.get(key)
        if isinstance(rows, list):
            expanded.extend((key, row) for row in rows if isinstance(row, dict))
    if not expanded and any(payload.get(k) for k in ("candidate_name", "candidate_id", "unit_name")):
        expanded.append(("top_level_candidate", payload))
    return expanded


def build_index(paths: Iterable[Path]) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    raw_units, warnings = [], []
    for path in paths:
        try:
            payload = json.loads(path.read_text(encoding="utf-8-sig"))
            if not isinstance(payload, dict):
                raise ValueError("top_level_not_object")
            for i, (container, item) in enumerate(_expand(path, payload)):
                candidate = _candidate(item, f"{path.stem}:{container}:{i}")
                family = _family(candidate, item)
                fingerprint, fp_status, fp_missing = _definition(item)
                members = _membership(item)
                scope = _comparison_scope(item)
                raw = item.get("authoritative_candidate_gate") or item.get("candidate_local_decision") or item.get("decision")
                raw = str(raw) if isinstance(raw, str) else ""
                raw_units.append({
                    "candidate_id": candidate, "family": family, "source_container": container,
                    "raw_decision": raw, "normalized_decision": NORMALIZE.get(raw.lower(), "unknown"),
                    "configuration_fingerprint": fingerprint, "fingerprint_status": fp_status,
                    "fingerprint_missing_inputs": fp_missing,
                    "top10_members": members,
                    "top10_membership_hash": _hash_bytes(_json(sorted(members)).encode()) if members else "unknown",
                    "top10_ranked_hash": _hash_bytes(_json(members).encode()) if members else "unknown",
                    "comparison_scope": scope,
                    "ranking_no_op_explicit": _explicit_flag(item, "ranking_no_op"),
                    "identical_top10_explicit": _explicit_flag(item, "identical_top10"),
                    "source_peer_duplicate_claim": item.get("peer_duplicate_state") or "unknown",
                    "source_overlap_claim": item.get("overlap_vs_peer_candidates") if item.get("overlap_vs_peer_candidates") is not None else "unknown",
                    "source": {"path": str(path.resolve()), "sha256": _file_hash(path), "item_index": i},
                    "source_item": item,
                })
        except Exception as exc:
            warnings.append({"path": str(path), "warning": f"parse_failed:{type(exc).__name__}:{exc}"})
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for unit in raw_units:
        grouped[(unit["family"], unit["candidate_id"])].append(unit)
    rows = []
    for (family, candidate), units in sorted(grouped.items()):
        explicit_members = [u["top10_members"] for u in units if u["top10_members"]]
        members = explicit_members[0] if explicit_members else None
        scopes = [u["comparison_scope"] for u in units]
        scope = next((s for s in scopes if s["fixed_contract"] and s["side"] and s["top_k"] and s["evaluation_units"]), None)
        complete_fps = sorted({u["configuration_fingerprint"] for u in units if u["fingerprint_status"] == "complete"})
        exact_duplicate = True if len(complete_fps) == 1 and sum(u["fingerprint_status"] == "complete" for u in units) > 1 else "unknown"
        rows.append({
            "schema_version": f"{AXIS_ID}.row.v1", "candidate_id": candidate, "family": family,
            "raw_decisions": sorted({u["raw_decision"] for u in units if u["raw_decision"]}),
            "normalized_decisions": sorted({u["normalized_decision"] for u in units}),
            "canonical_configuration_fingerprint": complete_fps[0] if len(complete_fps) == 1 else "incomplete",
            "fingerprint_status": "complete" if len(complete_fps) == 1 else "incomplete",
            "exact_duplicate": exact_duplicate, "top10_members": members,
            "top10_membership_hash": _hash_bytes(_json(sorted(members)).encode()) if members else "unknown",
            "top10_ranked_hash": _hash_bytes(_json(members).encode()) if members else "unknown",
            "comparison_scope": scope or {"fixed_contract": None, "side": None, "top_k": None, "evaluation_units": None},
            "top10_jaccard": "unknown", "changed_top10": "unknown", "behavioral_duplicate": "unknown",
            "duplicate_of": "unknown",
            "ranking_no_op_explicit": True if any(u["ranking_no_op_explicit"] is True for u in units) else "unknown",
            "identical_top10_explicit": True if any(u["identical_top10_explicit"] is True for u in units) else "unknown",
            "source_peer_duplicate_claims": sorted({str(u["source_peer_duplicate_claim"]) for u in units if u["source_peer_duplicate_claim"] != "unknown"}),
            "source_overlap_claims": sorted({str(u["source_overlap_claim"]) for u in units if u["source_overlap_claim"] != "unknown"}),
            "parse_warnings": ["conflicting_explicit_top10_memberships"] if len({_json(x) for x in explicit_members}) > 1 else [],
            "sources": [u["source"] for u in units],
        })
    # Behavioral comparisons are legal only when both explicit membership lists exist.
    for i, row in enumerate(rows):
        if not row["top10_members"] or not row["comparison_scope"]["fixed_contract"]:
            continue
        best, best_peer = -1.0, None
        a = set(row["top10_members"])
        for j, peer in enumerate(rows):
            if i == j or not peer["top10_members"]:
                continue
            if peer["comparison_scope"] != row["comparison_scope"] or int(row["comparison_scope"]["top_k"]) != 10:
                continue
            b = set(peer["top10_members"]); union = a | b
            score = len(a & b) / len(union) if union else 1.0
            if score > best:
                best, best_peer = score, peer["candidate_id"]
        if best_peer is not None:
            row["top10_jaccard"] = best
            row["changed_top10"] = len(a.symmetric_difference(set(next(x["top10_members"] for x in rows if x["candidate_id"] == best_peer))))
            row["behavioral_duplicate"] = bool(best >= 0.9)
            row["behavioral_duplicate_peer"] = best_peer
            if row["top10_ranked_hash"] == next(x["top10_ranked_hash"] for x in rows if x["candidate_id"] == best_peer):
                row["duplicate_of"] = best_peer
    return rows, warnings


def run(source_root: Path = SOURCE_ROOT, out: Path = OUT) -> Path:
    paths = source_paths(source_root); rows, warnings = build_index(paths)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    root = out / f"{stamp}-{AXIS_ID}"; root.mkdir(parents=True, exist_ok=False)
    index = root / "behavioral_duplicate_index.jsonl"
    index.write_text("".join(_json(row) + "\n" for row in rows), encoding="utf-8")
    manifest = {"schema_version": f"{AXIS_ID}.manifest.v1", "artifact_role": "authoritative", "generated_at": datetime.now(timezone.utc).isoformat(),
                "source_policy": "explicit_bad_pick_seed_files_only", "source_count": len(paths), "canonical_candidate_count": len(rows),
                "warnings": warnings, "index_sha256": _file_hash(index), "runtime_db_write": False, "production_ranking_changed": False, "meemee_changed": False}
    (root / "manifest.json").write_text(_json(manifest) + "\n", encoding="utf-8")
    summary = {"schema_version": f"{AXIS_ID}.summary.v1", "exact_duplicate_count": sum(r["exact_duplicate"] is True for r in rows),
               "behavioral_duplicate_count": sum(r["behavioral_duplicate"] is True for r in rows),
               "unknown_behavior_count": sum(r["behavioral_duplicate"] == "unknown" for r in rows),
               "ranking_no_op_explicit_count": sum(r["ranking_no_op_explicit"] is True for r in rows),
               "identical_top10_explicit_count": sum(r["identical_top10_explicit"] is True for r in rows)}
    (root / "duplicate_gate_summary.json").write_text(_json(summary) + "\n", encoding="utf-8")
    return root


if __name__ == "__main__":
    p = argparse.ArgumentParser(); p.add_argument("--source-root", type=Path, default=SOURCE_ROOT); p.add_argument("--output-root", type=Path, default=OUT)
    a = p.parse_args(); print(run(a.source_root, a.output_root))
