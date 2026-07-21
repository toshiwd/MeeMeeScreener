from __future__ import annotations

import argparse
import hashlib
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


AXIS_ID = "tradex_short_entry_family_cards_v1"
SOURCE_ROOT = Path("artifacts/research_inventory")
OUT = Path(r"G:\Tradex\research_knowledge_registry_v1")
PREFIX = "entry_precision_short_"
AXES = ("monthlybreak", "alignmentpath", "closepos", "rangeprob", "regime", "wide", "monthly")
NORMALIZE = {"keep": "keep", "hold": "hold", "drop": "drop"}


def _json(v: Any) -> str:
    return json.dumps(v, ensure_ascii=False, sort_keys=True, default=str)


def _hash(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def classify(path: Path, payload: dict[str, Any]) -> tuple[str, str]:
    stem = path.stem.removeprefix(PREFIX)
    ids = " ".join(str(payload.get(k) or "") for k in ("session_id", "baseline_id", "challenger_id")).lower()
    text = f"{stem} {ids}"
    family = "broad_down" if "broad_down" in text else "trend" if "trend" in text else "base"
    axis = "base"
    for candidate in AXES:  # monthlybreak must precede monthly
        if candidate in text:
            axis = candidate
            break
    return family, axis


def _decision(path: Path, payload: dict[str, Any]) -> tuple[str, int, str] | None:
    raw = payload.get("overall_decision") or payload.get("authoritative_rollup_decision")
    if not isinstance(raw, str) or not raw.strip():
        return None
    name = path.name
    # Explicit priority: ordinary *_decision.json, then *_fix_decision.json,
    # then an explicit decision carried by any other artifact.
    priority = 0 if name.endswith("_decision.json") and not name.endswith("_fix_decision.json") else 1 if name.endswith("_fix_decision.json") else 2
    return raw.strip(), priority, "overall_decision" if payload.get("overall_decision") else "authoritative_rollup_decision"


def _contract(payloads: list[dict[str, Any]]) -> dict[str, Any]:
    for payload in payloads:
        for key in ("comparison_contract", "same_condition_contract"):
            if isinstance(payload.get(key), dict):
                return payload[key]
    return {}


def _metrics(payloads: list[dict[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for payload in payloads:
        for key in ("baseline", "challenger", "delta", "variants", "decision_rollup", "wide_stability", "reentry_summary"):
            if key in payload:
                result.setdefault(key, payload[key])
    return result


def _fingerprint(family: str, axis: str, decision_payload: dict[str, Any], contract: dict[str, Any]) -> tuple[str, list[str]]:
    def pick(*keys: str) -> Any:
        for key in keys:
            if contract.get(key) not in (None, "", {}, []):
                return contract[key]
            if decision_payload.get(key) not in (None, "", {}, []):
                return decision_payload[key]
        return None

    values = {
        "family": family, "axis": axis,
        "code_version": pick("code_version", "git_commit", "code_sha"),
        "data_version": pick("data_version", "source_data_version", "db_version"),
        "universe": pick("universe", "symbols", "eligible_universe"),
        "period": pick("period", "window", "evaluation_period"),
        "regime": pick("regime", "focused_regime", "market_regime"),
        "entry": pick("entry", "entry_rule", "entry_condition"),
        "exit": pick("exit", "exit_rule", "exit_condition"),
        "target": pick("target", "objective", "target_definition"),
        "features_or_conditions": pick("features", "conditions", "feature_set", "rule_conditions"),
    }
    missing = [k for k, v in values.items() if v in (None, "", {})]
    return ("incomplete", missing) if missing else (hashlib.sha256(_json(values).encode()).hexdigest(), [])


def build_cards(paths: list[Path]) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    groups: dict[tuple[str, str], list[tuple[Path, dict[str, Any]]]] = defaultdict(list)
    warnings: list[dict[str, str]] = []
    for path in paths:
        try:
            payload = json.loads(path.read_text(encoding="utf-8-sig"))
            if not isinstance(payload, dict):
                raise ValueError("top_level_not_object")
            groups[classify(path, payload)].append((path, payload))
        except Exception as exc:
            warnings.append({"path": str(path), "warning": f"parse_failed:{type(exc).__name__}:{exc}"})
    cards = []
    for (family, axis), items in sorted(groups.items()):
        decisions = [(result, path, payload) for path, payload in items if (result := _decision(path, payload))]
        if not decisions:
            warnings.append({"path": ",".join(str(p) for p, _ in items), "warning": "no_explicit_decision_no_card"})
            continue
        decisions.sort(key=lambda x: (x[0][1], x[1].name))
        (raw, priority, source_field), decision_path, decision_payload = decisions[0]
        contract = _contract([p for _, p in items])
        fingerprint, missing = _fingerprint(family, axis, decision_payload, contract)
        local_warnings = []
        if len(decisions) > 1:
            local_warnings.append("multiple_explicit_decisions_priority_applied")
        if raw.lower() not in NORMALIZE:
            local_warnings.append("decision_not_in_explicit_normalization_map")
        if not contract:
            local_warnings.append("fixed_contract_missing")
        sources = [{"path": str(p.resolve()), "sha256": _hash(p), "schema_version": str(d.get("schema_version") or "unknown")} for p, d in sorted(items)]
        risks = decision_payload.get("remaining_risks")
        cards.append({
            "schema_version": f"{AXIS_ID}.card.v1", "family": family, "axis": axis,
            "session_id": decision_payload.get("session_id"), "baseline_id": decision_payload.get("baseline_id"),
            "challenger_id": decision_payload.get("challenger_id"), "decision_source_path": str(decision_path.resolve()),
            "decision_source_field": source_field, "decision_priority": priority, "raw_decision": raw,
            "normalized_decision": NORMALIZE.get(raw.lower(), "unknown"), "fixed_contract": contract,
            "metrics": _metrics([p for _, p in items]), "risks": risks if isinstance(risks, (list, dict)) else [],
            "sources": sources, "parse_warnings": local_warnings,
            "semantic_fingerprint": fingerprint, "fingerprint_status": "incomplete" if missing else "complete",
            "fingerprint_missing_inputs": missing,
            "runtime_db_write": False, "production_ranking_changed": False, "meemee_changed": False,
        })
    return cards, warnings


def run(source_root: Path = SOURCE_ROOT, out: Path = OUT) -> Path:
    paths = sorted(source_root.glob(f"{PREFIX}*.json"))
    cards, warnings = build_cards(paths)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    root = out / f"{stamp}-{AXIS_ID}"
    root.mkdir(parents=True, exist_ok=False)
    card_path = root / "short_entry_family_cards.jsonl"
    card_path.write_text("".join(_json(card) + "\n" for card in cards), encoding="utf-8")
    manifest = {
        "schema_version": f"{AXIS_ID}.manifest.v1", "artifact_role": "authoritative",
        "generated_at": datetime.now(timezone.utc).isoformat(), "source_policy": "explicit_entry_precision_short_json_only",
        "source_count": len(paths), "card_count": len(cards), "warnings": warnings,
        "cards_sha256": _hash(card_path), "runtime_db_write": False, "production_ranking_changed": False, "meemee_changed": False,
    }
    (root / "manifest.json").write_text(_json(manifest) + "\n", encoding="utf-8")
    return root


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", type=Path, default=SOURCE_ROOT)
    parser.add_argument("--output-root", type=Path, default=OUT)
    args = parser.parse_args()
    print(run(args.source_root, args.output_root))
