from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "artifacts" / "research_inventory"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\research_knowledge_registry_v1")

FILES = {
    "candidate_map": "action_precision_multitimeframe_candidate_map.json",
    "long": "action_precision_multitimeframe_long_decomposition.json",
    "short": "action_precision_multitimeframe_short_decomposition.json",
    "pairwise": "action_precision_multitimeframe_pairwise_effects.json",
    "triple": "action_precision_multitimeframe_triple_effects.json",
    "decision": "authoritative_decision.action_precision.json",
    "mt_decision": "authoritative_decision.action_precision_multitimeframe.json",
}


def _load(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8-sig") as fh:
        value = json.load(fh)
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def classify_cell(row: dict[str, Any]) -> str:
    """Classify evidence quality/polarity, never trade adoption."""
    if not isinstance(row, dict) or not row.get("signal_side") or not row.get("state_combination"):
        return "invalid"
    count = row.get("sample_count")
    threshold = row.get("sample_threshold", 30)
    if not isinstance(count, (int, float)) or count < 0:
        return "invalid"
    if count < (threshold if isinstance(threshold, (int, float)) else 30):
        return "thin"
    coverage = str(row.get("coverage_status", "")).lower()
    if coverage in {"sparse", "thin", "unstable"}:
        return "thin"
    stable = row.get("stable_sign_match")
    if stable is True:
        return "positive"
    if stable is False:
        return "negative"
    if coverage == "usable":
        return "tested"
    return "unknown"


def _iter_level(payload: dict[str, Any], level: str) -> Iterable[tuple[str, str, dict[str, Any]]]:
    for side in ("long", "short"):
        groups = payload.get(side, {})
        if not isinstance(groups, dict):
            continue
        for grouping, rows in groups.items():
            if not isinstance(rows, list):
                continue
            for row in rows:
                if isinstance(row, dict):
                    yield side, str(grouping), {**row, "evidence_level": level}


def _iter_singles(payload: dict[str, Any], side: str) -> Iterable[dict[str, Any]]:
    groups = payload.get("single_timeframe", {})
    if not isinstance(groups, dict):
        return
    for grouping, rows in groups.items():
        if isinstance(rows, list):
            for row in rows:
                if isinstance(row, dict):
                    yield {**row, "signal_side": row.get("signal_side", side), "evidence_level": "single"}


def _tokens(state: str) -> tuple[str, ...]:
    return tuple(sorted(part.strip() for part in state.split("|") if "=" in part))


def build_coverage(source: Path = SOURCE) -> tuple[dict[str, Any], dict[str, Any]]:
    pair = _load(source / FILES["pairwise"])
    triple = _load(source / FILES["triple"])
    long_dec = _load(source / FILES["long"])
    short_dec = _load(source / FILES["short"])
    # Read these contracts so missing/corrupt inputs fail loudly; they do not
    # confer adoption on any evidence cell.
    candidate_map = _load(source / FILES["candidate_map"])
    decision = _load(source / FILES["decision"])
    mt_decision = _load(source / FILES["mt_decision"])

    cells: list[dict[str, Any]] = []
    seen: set[tuple[str, str, tuple[str, ...]]] = set()
    for level, payload in (("pairwise", pair), ("triple", triple)):
        for side, grouping, row in _iter_level(payload, level):
            state = str(row.get("state_combination", ""))
            key = (side, level, _tokens(state))
            duplicate = key in seen
            seen.add(key)
            cells.append({
                "side": side,
                "level": level,
                "grouping": grouping,
                "state_combination": state,
                "classification": classify_cell({**row, "signal_side": side}),
                "sample_count": row.get("sample_count"),
                "sample_threshold": row.get("sample_threshold"),
                "coverage_status": row.get("coverage_status"),
                "stable_sign_match": row.get("stable_sign_match"),
                "duplicate": duplicate,
                "interpretation": "evidence_only_not_trade_adoption",
            })

    counts = {name: 0 for name in ("tested", "positive", "negative", "thin", "invalid", "unknown")}
    for cell in cells:
        counts[cell["classification"]] += 1

    tested_keys = {(c["side"], _tokens(c["state_combination"])) for c in cells if not c["duplicate"]}
    singles: dict[str, dict[str, int]] = {"long": {}, "short": {}}
    for side, payload in (("long", long_dec), ("short", short_dec)):
        for row in _iter_singles(payload, side):
            if classify_cell(row) in {"tested", "positive", "negative"}:
                tok = _tokens(str(row.get("state_combination", "")))
                if len(tok) == 1:
                    singles[side][tok[0]] = max(singles[side].get(tok[0], 0), int(row.get("sample_count", 0)))

    gaps: list[dict[str, Any]] = []
    for side, evidence in singles.items():
        items = sorted(evidence.items())
        for i, (a, na) in enumerate(items):
            for b, nb in items[i + 1:]:
                if a.split("=", 1)[0].split("_main_state_ctx", 1)[0] == b.split("=", 1)[0].split("_main_state_ctx", 1)[0]:
                    continue
                combo = tuple(sorted((a, b)))
                if (side, combo) in tested_keys:
                    continue
                gaps.append({
                    "side": side,
                    "state_combination": "|".join(combo),
                    "reason": "single_axis_observations_exist_interaction_untested",
                    "single_axis_support": {a: na, b: nb},
                    "single_axis_support_semantics": "observation_count_not_quality_evidence",
                    "priority_score": min(na, nb),
                    "decision": "research_gap_not_trade_adoption",
                })
    gaps.sort(key=lambda x: (-x["priority_score"], x["side"], x["state_combination"]))
    gaps = gaps[:3]

    coverage = {
        "schema_version": "tradex_interaction_coverage_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "artifact_role": "research_evidence_coverage",
        "classification_semantics": "positive/negative describe explicit stable_sign_match only; none imply trade adoption",
        "authoritative_decision_context": {
            "action_precision_schema": decision.get("schema_version"),
            "multitimeframe_decision": mt_decision.get("decision"),
        },
        "candidate_map_schema": candidate_map.get("schema_version"),
        "counts": counts,
        "cells": cells,
        "source_files": [str(source / name) for name in FILES.values()],
    }
    gap_output = {
        "schema_version": "tradex_interaction_coverage_v1.gaps",
        "generated_at": coverage["generated_at"],
        "candidate_count": len(gaps),
        "max_candidates": 3,
        "selection_contract": "single-axis observations exist; interaction untested; invalid and duplicate cells excluded",
        "support_semantics": "single_axis_support values are observation counts, not quality evidence",
        "candidates": gaps,
    }
    return coverage, gap_output


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, default=SOURCE)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = args.output_root / f"{stamp}-phase2-interactions"
    out.mkdir(parents=True, exist_ok=False)
    coverage, gaps = build_coverage(args.source)
    (out / "interaction_coverage.json").write_text(json.dumps(coverage, ensure_ascii=False, indent=2), encoding="utf-8")
    (out / "novel_interaction_gaps.json").write_text(json.dumps(gaps, ensure_ascii=False, indent=2), encoding="utf-8")
    print(out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
