from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


AXIS_ID = "tradex_research_knowledge_registry_v1"
DEFAULT_OUT = Path(r"G:\Tradex\research_knowledge_registry_v1")
LOCAL_INVENTORY = Path("artifacts/research_inventory")
G_SEEDS = (
    Path(r"G:\Tradex\adaptive_short_rule_router_v1"),
    Path(r"G:\Tradex\tradex_short_fast10_guard_compare_v1"),
    Path(r"G:\Tradex\tradex_short_dual5of10_guard_compare_v1"),
    Path(r"G:\Tradex\tradex_short_rolling_permission_compare_v1"),
    Path(r"G:\Tradex\tradex_short_rolling_permission_1y_compare_v1"),
    Path(r"G:\Tradex\buy_research_goal_rollup_v1"),
    Path(r"G:\Tradex\short_leaf20_final_rollup_v1"),
    Path(r"G:\Tradex\dual_side_research_rollup_v1"),
    Path(r"G:\Tradex\bad_pick_removal_family_rollup_decision_v1"),
)
DECISION_CONTAINERS = ("decision", "judgment", "research_decision")
EXPLICIT_NORMALIZATION = {
    "keep": "keep", "drop": "drop", "hold": "hold",
    "active": "keep", "watch": "hold", "dormant": "drop",
    "park_family": "hold", "research_only": "hold", "review_only": "hold",
    "drop_all_new_breakout_variants": "drop",
    "retain_existing_leaf_rule_no_additional_rule": "hold",
    "no_additional_buy_rule_exceeded_both_fixed_benchmarks_under_tested_axes": "hold",
    "keep_as_buy_level_equivalent_research_candidate": "keep",
}


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _latest_jsons(root: Path) -> list[Path]:
    if not root.exists():
        return []
    dirs = [p for p in root.iterdir() if p.is_dir()]
    leaf = max(dirs, key=lambda p: p.name) if dirs else root
    preferred = ["compare.json", "final_rollup.json", "session_leaderboard_rollup.json", "final_research_decision.json"]
    found = [leaf / name for name in preferred if (leaf / name).is_file()]
    return found[:1]


def seed_paths(local_root: Path = LOCAL_INVENTORY, g_seeds: Iterable[Path] = G_SEEDS) -> tuple[list[Path], list[dict[str, str]]]:
    paths: list[Path] = []
    gaps: list[dict[str, str]] = []
    if local_root.exists():
        # Direct children only: this is an explicit seed directory, never a repository scan.
        paths.extend(sorted(p for p in local_root.glob("*.json") if p.is_file()))
    else:
        gaps.append({"seed": str(local_root), "reason": "missing_seed_root"})
    for root in g_seeds:
        selected = _latest_jsons(root)
        if selected:
            paths.extend(selected)
        else:
            gaps.append({"seed": str(root), "reason": "missing_authoritative_json"})
    return list(dict.fromkeys(paths)), gaps


def _decision(payload: dict[str, Any]) -> tuple[str, str, str]:
    candidates: list[tuple[str, Any]] = [("authoritative_rollup_decision", payload.get("authoritative_rollup_decision"))]
    top_rollup = payload.get("authoritative_rollup_decision")
    if isinstance(top_rollup, dict):
        candidates.extend([
            ("authoritative_rollup_decision.authoritative_rollup_decision", top_rollup.get("authoritative_rollup_decision")),
            ("authoritative_rollup_decision.candidate_local_decision", top_rollup.get("candidate_local_decision")),
        ])
    for container in DECISION_CONTAINERS:
        value = payload.get(container)
        if isinstance(value, dict):
            candidates.extend([
                (f"{container}.authoritative_rollup_decision", value.get("authoritative_rollup_decision")),
                (f"{container}.session_aggregate_decision", value.get("session_aggregate_decision")),
                (f"{container}.candidate_local_decision", value.get("candidate_local_decision")),
            ])
    candidates.extend([("candidate_local_decision", payload.get("candidate_local_decision")), ("decision", payload.get("decision") if isinstance(payload.get("decision"), str) else None)])
    for source, value in candidates:
        if isinstance(value, str) and value.strip():
            raw = value.strip()
            return raw, EXPLICIT_NORMALIZATION.get(raw.lower(), "unknown"), source
    return "", "unknown", "missing"


def _fixed(payload: dict[str, Any]) -> dict[str, Any]:
    for key in ("fixed_evaluation_conditions", "same_condition_contract", "evaluation_contract", "fixed_benchmarks"):
        if isinstance(payload.get(key), dict):
            return payload[key]
    return {}


def _metrics(payload: dict[str, Any]) -> dict[str, Any]:
    return {k: payload[k] for k in ("metrics", "reports", "comparison", "leaf20_comparison", "family_rollup_result") if k in payload}


def _semantic_fingerprint(payload: dict[str, Any], fixed: dict[str, Any]) -> tuple[str, list[str]]:
    required = {
        "family_id": payload.get("family_id") or payload.get("axis_id"),
        "hypothesis": payload.get("hypothesis") or payload.get("scope_statement"),
        "universe": fixed.get("universe"), "period": fixed.get("period") or fixed.get("development"),
        "top_k": fixed.get("top_k"), "regime": fixed.get("regime"),
        "cost": fixed.get("cost_model") or fixed.get("costs") or fixed.get("cost_borrow_gyakuhibu_ignored"),
        "artifact_detail_level": fixed.get("artifact_detail_level") or payload.get("artifact_detail_level"),
    }
    missing = [k for k, v in required.items() if v in (None, "", [], {})]
    if missing:
        return "incomplete", missing
    return hashlib.sha256(_json(required).encode("utf-8")).hexdigest(), []


def artifact_row(path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    raw, normalized, decision_source = _decision(payload)
    fixed = _fixed(payload)
    fingerprint, missing = _semantic_fingerprint(payload, fixed)
    branching = payload.get("observed_branching") or payload.get("branching")
    return {
        "artifact_path": str(path.resolve()), "artifact_sha256": _sha256(path),
        "schema_version": str(payload.get("schema_version") or "unknown"),
        "artifact_role": str(payload.get("artifact_role") or "unknown"),
        "family_id": str(payload.get("family_id") or payload.get("axis_id") or str(payload.get("schema_version") or "").split(".")[0] or path.parent.parent.name or path.stem),
        "run_id": str(payload.get("run_id") or payload.get("session_id") or path.parent.name),
        "generated_at": str(payload.get("generated_at") or ""),
        "raw_decision": raw, "normalized_decision": normalized, "decision_source": decision_source,
        "fixed_conditions_json": _json(fixed), "metrics_json": _json(_metrics(payload)),
        "lookahead_status": str(payload.get("no_lookahead_status") or (payload.get("no_lookahead_audit") or {}).get("status") or "unknown"),
        "fallback_status": str(payload.get("fallback_status") or ("no_silent_fallback" if payload.get("silent_fallback_used") is False else "unknown")),
        "branching_status": _json(branching) if branching is not None else "unknown",
        "runtime_db_write": payload.get("runtime_db_write"),
        "production_ranking_changed": payload.get("production_ranking_changed"),
        "meemee_changed": payload.get("meemee_changed"),
        "semantic_fingerprint": fingerprint, "fingerprint_missing_inputs_json": _json(missing),
    }


def build_registry(paths: Iterable[Path]) -> tuple[pd.DataFrame, list[dict[str, str]]]:
    rows, gaps = [], []
    for path in paths:
        try:
            payload = json.loads(path.read_text(encoding="utf-8-sig"))
            if not isinstance(payload, dict):
                raise ValueError("top_level_not_object")
            row = artifact_row(path, payload)
            if row["raw_decision"]:
                rows.append(row)  # exactly one decision row per artifact
            else:
                gaps.append({"artifact": str(path), "reason": "no_explicit_decision"})
        except Exception as exc:  # gap is explicit; never silently substitutes another artifact
            gaps.append({"artifact": str(path), "reason": f"parse_failed:{type(exc).__name__}:{exc}"})
    return pd.DataFrame(rows), gaps


def run(output_root: Path = DEFAULT_OUT, local_root: Path = LOCAL_INVENTORY, g_seeds: Iterable[Path] = G_SEEDS) -> Path:
    paths, seed_gaps = seed_paths(local_root, g_seeds)
    frame, artifact_gaps = build_registry(paths)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    root = output_root / f"{stamp}-{AXIS_ID}"
    root.mkdir(parents=True, exist_ok=False)
    frame.to_parquet(root / "research_registry.parquet", index=False)
    manifest = {
        "schema_version": f"{AXIS_ID}.manifest.v1", "artifact_role": "authoritative",
        "generated_at": datetime.now(timezone.utc).isoformat(), "collection_policy": "explicit_seed_roots_only_no_g_drive_scan",
        "seed_roots": [str(local_root), *map(str, g_seeds)], "source_artifact_count": len(paths),
        "decision_row_count": len(frame), "registry_sha256": _sha256(root / "research_registry.parquet"),
        "runtime_db_write": False, "production_ranking_changed": False, "meemee_changed": False,
    }
    (root / "registry_manifest.json").write_text(_json(manifest) + "\n", encoding="utf-8")
    gaps = {"schema_version": f"{AXIS_ID}.gaps.v1", "seed_gaps": seed_gaps, "artifact_gaps": artifact_gaps,
            "unknown_decision_count": int((frame.get("normalized_decision", pd.Series(dtype=str)) == "unknown").sum()),
            "incomplete_fingerprint_count": int((frame.get("semantic_fingerprint", pd.Series(dtype=str)) == "incomplete").sum())}
    (root / "gap_report.json").write_text(_json(gaps) + "\n", encoding="utf-8")
    counts = frame["normalized_decision"].value_counts().to_dict() if not frame.empty else {}
    map_md = "\n".join(["# TRADEX Research Map", "", "Derived from research_registry.parquet; JSON and Parquet remain authoritative.", "",
                         f"- decision rows: {len(frame)}", *[f"- {k}: {v}" for k, v in sorted(counts.items())],
                         f"- incomplete fingerprints: {gaps['incomplete_fingerprint_count']}", f"- artifact gaps: {len(artifact_gaps)}", ""])
    (root / "RESEARCH_MAP.md").write_text(map_md, encoding="utf-8")
    return root


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()
    print(run(args.output_root))
