from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import tradex_iizuka_signal_expectancy_v1 as base

SCRIPT_NAME = "tradex_point_in_time_candidate_pool_contract_audit_v1"
SCHEMA_VERSION = "tradex_point_in_time_candidate_pool_contract_audit_v1"
CONTRACT_PATH = REPO_ROOT / "docs" / "contracts" / "tradex_point_in_time_candidate_pool_contract_v1.json"
DEFAULT_SEARCH_ROOT = Path(r"G:\Tradex")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\point_in_time_candidate_pool_contract_v1")
REQUIRED_ARTIFACTS = [
    "run_manifest.json",
    "input_resolution.json",
    "point_in_time_candidate_pool_contract_snapshot.json",
    "source_inventory.json",
    "reconstruction_feasibility_audit.json",
    "required_field_gap_audit.json",
    "no_lookahead_candidate_pool_audit.json",
    "point_in_time_pool_decision.json",
    "_ARTIFACT_COMPLETE.json",
]
DISCOVERY_KEYWORDS = ("candidate", "ranking", "rank", "pool", "selection", "champion", "prefilter", "min_pool", "reranker")
FUTURE_LABEL_FIELDS = {
    "forward_ret_5d",
    "forward_ret_10d",
    "forward_ret_20d",
    "path_value_score_v1",
    "mfe_20d",
    "mae_20d",
    "top15_label",
    "top20pct_label",
    "bottom15_label",
    "hit_plus_5_before_minus_5",
    "hit_minus_5_before_plus_5",
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _session_id() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{now.microsecond:06d}"


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    base._write_json(path, payload)


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _safe_path(value: str | Path | None, default: Path) -> Path:
    return base._safe_path(value, default)


def _discover(search_root: Path, max_sources: int) -> list[Path]:
    if not search_root.exists():
        return []
    found = []
    for path in search_root.rglob("*.parquet"):
        text = str(path).lower()
        if any(keyword in text for keyword in DISCOVERY_KEYWORDS):
            found.append(path)
    found.sort(key=lambda p: p.stat().st_mtime if p.exists() else 0, reverse=True)
    return found[:max_sources]


def _schema(path: Path) -> tuple[int, list[str]]:
    parquet = pq.ParquetFile(path)
    return int(parquet.metadata.num_rows), list(parquet.schema.names)


def _has_any(columns: set[str], names: list[str]) -> bool:
    return any(name.lower() in columns for name in names)


def _source_kind(path: Path, columns: set[str]) -> str:
    text = str(path).lower()
    if "champion" in text or any(col.startswith("champion_") for col in columns):
        return "champion"
    if "min_pool" in text:
        return "min_pool"
    if "prefilter" in text:
        return "prefilter"
    if "candidate" in text:
        return "candidate_generation"
    if "reranker" in text:
        return "reranker"
    return "derived"


def _inspect_source(path: Path, contract: dict[str, Any]) -> dict[str, Any]:
    try:
        row_count, columns_raw = _schema(path)
    except Exception as exc:
        return {"path": str(path), "status": "blocked", "reason": str(exc)}
    columns = {col.lower() for col in columns_raw}
    future_labels = sorted(col for col in columns_raw if col.lower() in FUTURE_LABEL_FIELDS)
    field_checks = {
        "symbol_or_code": _has_any(columns, ["symbol", "code", "ticker", "sec_code"]),
        "side": "side" in columns,
        "as_of_date": "as_of_date" in columns,
        "candidate_date": "candidate_date" in columns or "anchor_date" in columns or "trade_date" in columns,
        "feature_cutoff_date": "feature_cutoff_date" in columns,
        "universe_membership": _has_any(columns, ["universe_membership", "include_in_broad_pool", "candidate_pool_tier"]),
        "candidate_pool_membership": _has_any(columns, ["candidate_pool_membership", "accepted", "include_in_strict_pool", "candidate_pool_tier"]),
        "prefilter_pass": _has_any(columns, ["prefilter_pass", "accepted", "include_in_broad_pool", "include_in_strict_pool"]),
        "prefilter_reject_reason": _has_any(columns, ["prefilter_reject_reason", "reject_reason", "min_pool_reject_reason"]),
        "champion_score": "champion_score" in columns,
        "champion_rank": "champion_rank" in columns,
        "topk_membership": _has_any(
            columns,
            [
                "champion_selected_top5",
                "champion_selected_top10",
                "champion_selected_top20",
                "champion_selected_top50",
                "rank",
                "champion_rank",
            ],
        ),
        "source_lineage": _has_any(columns, ["source_artifact_path", "source_artifact_session_id", "source_generation_script", "canonical_candidate_key"]),
        "no_future_label_used": "no_future_label_used" in columns,
        "no_lookahead_flags": _has_any(columns, ["feature_cutoff_valid", "candidate_membership_no_lookahead", "champion_score_no_lookahead", "monthly_context_no_lookahead", "weekly_context_no_lookahead"]),
    }
    hard_required = [
        "symbol_or_code",
        "side",
        "candidate_date",
        "candidate_pool_membership",
        "champion_score",
        "champion_rank",
        "topk_membership",
        "no_future_label_used",
        "feature_cutoff_date",
    ]
    missing = [name for name in hard_required if not field_checks.get(name)]
    future_risk = bool(future_labels)
    unsafe_inference = not field_checks["champion_score"] or not field_checks["champion_rank"] or not field_checks["feature_cutoff_date"]
    ready = not missing and not future_risk and not unsafe_inference
    return {
        "path": str(path),
        "status": "confirmed",
        "row_count": row_count,
        "source_kind": _source_kind(path, columns),
        "columns": columns_raw,
        "field_checks": field_checks,
        "missing_required_fields": missing,
        "future_label_fields_present": future_labels,
        "future_label_risk": future_risk,
        "unsafe_inference_risk": unsafe_inference,
        "ready_candidate": ready,
    }


def _summarize_gaps(sources: list[dict[str, Any]]) -> dict[str, Any]:
    confirmed = [s for s in sources if s.get("status") == "confirmed"]
    best = sorted(
        confirmed,
        key=lambda s: (
            len(s.get("missing_required_fields") or []),
            int(s.get("future_label_risk") is True),
            int(s.get("unsafe_inference_risk") is True),
            -int(s.get("row_count") or 0),
        ),
    )[:10]
    missing_counts: dict[str, int] = {}
    for source in confirmed:
        for field in source.get("missing_required_fields") or []:
            missing_counts[field] = missing_counts.get(field, 0) + 1
    return {
        "schema_version": f"{SCHEMA_VERSION}_required_field_gap_audit_v1",
        "generated_at_utc": _utc_now(),
        "confirmed_source_count": len(confirmed),
        "missing_required_field_counts": missing_counts,
        "best_partial_sources": [
            {
                "path": s["path"],
                "row_count": s.get("row_count"),
                "source_kind": s.get("source_kind"),
                "missing_required_fields": s.get("missing_required_fields"),
                "future_label_risk": s.get("future_label_risk"),
                "unsafe_inference_risk": s.get("unsafe_inference_risk"),
            }
            for s in best
        ],
    }


def run_audit(*, search_root: Path, output_root: Path, max_sources: int = 250) -> dict[str, Any]:
    session_root = output_root / _session_id()
    session_root.mkdir(parents=True, exist_ok=True)
    contract = _load_json(CONTRACT_PATH)
    source_paths = _discover(search_root, max_sources)
    sources = [_inspect_source(path, contract) for path in source_paths]
    ready_sources = [s for s in sources if s.get("ready_candidate")]
    future_risk_sources = [s for s in sources if s.get("future_label_risk")]
    unsafe_sources = [s for s in sources if s.get("unsafe_inference_risk")]
    gap_audit = _summarize_gaps(sources)
    can_reconstruct = bool(ready_sources)
    if ready_sources:
        decision = "ready"
        reason = "safe_point_in_time_candidate_pool_source_exists"
    elif future_risk_sources and len(future_risk_sources) >= len([s for s in sources if s.get("status") == "confirmed"]) * 0.5:
        decision = "blocked_future_label_risk"
        reason = "available_candidate_sources_commonly_include_future_or_post_horizon_labels"
    elif unsafe_sources:
        decision = "blocked_unsafe_inference"
        reason = "reconstruction_requires_feature_cutoff_or_champion_score_rank_inference"
    else:
        decision = "blocked_missing_sources"
        reason = "existing_artifacts_do_not_contain_required_point_in_time_fields"
    feasibility = {
        "schema_version": f"{SCHEMA_VERSION}_reconstruction_feasibility_audit_v1",
        "generated_at_utc": _utc_now(),
        "can_reconstruct_without_changing_champion_logic": can_reconstruct,
        "sufficient_source_artifacts": [
            {"path": s["path"], "row_count": s.get("row_count"), "source_kind": s.get("source_kind")} for s in ready_sources[:20]
        ],
        "insufficient_source_summary": {
            "inventoried_source_count": len(sources),
            "ready_source_count": len(ready_sources),
            "future_label_risk_source_count": len(future_risk_sources),
            "unsafe_inference_source_count": len(unsafe_sources),
        },
        "reconstruction_requires_unsafe_inference": not can_reconstruct,
    }
    no_lookahead = {
        "schema_version": f"{SCHEMA_VERSION}_no_lookahead_candidate_pool_audit_v1",
        "generated_at_utc": _utc_now(),
        "pass": can_reconstruct,
        "nearest_next_policy_allowed": False,
        "ready_sources_have_no_future_label_used": all(not s.get("future_label_risk") for s in ready_sources),
        "ready_sources_have_feature_cutoff": all((s.get("field_checks") or {}).get("feature_cutoff_date") for s in ready_sources),
        "ready_sources_have_champion_score_rank": all((s.get("field_checks") or {}).get("champion_score") and (s.get("field_checks") or {}).get("champion_rank") for s in ready_sources),
    }
    decision_payload = {
        "schema_version": f"{SCHEMA_VERSION}_decision_v1",
        "generated_at_utc": _utc_now(),
        "authoritative_decision": decision,
        "decision_reason": reason,
        "ready_source_count": len(ready_sources),
        "selected_ready_source_path": ready_sources[0]["path"] if ready_sources else None,
        "missing_fields_or_artifacts": gap_audit["missing_required_field_counts"],
        "future_label_risk_source_count": len(future_risk_sources),
        "unsafe_inference_source_count": len(unsafe_sources),
        "meemee_changed": False,
        "production_ranking_changed": False,
        "publish_changed": False,
        "ranking_challenger_created": False,
    }
    artifacts = {
        "run_manifest.json": {
            "schema_version": f"{SCHEMA_VERSION}_manifest_v1",
            "generated_at_utc": _utc_now(),
            "script_name": SCRIPT_NAME,
            "session_root": str(session_root),
            "search_root": str(search_root),
            "boundary": "TRADEX-only",
            "contract_path": str(CONTRACT_PATH),
            "meemee_changed": False,
            "production_ranking_changed": False,
            "publish_changed": False,
            "ranking_challenger_created": False,
        },
        "input_resolution.json": {
            "schema_version": f"{SCHEMA_VERSION}_input_resolution_v1",
            "generated_at_utc": _utc_now(),
            "search_root": str(search_root),
            "max_sources": max_sources,
            "inventoried_source_count": len(sources),
            "contract_path": str(CONTRACT_PATH),
        },
        "point_in_time_candidate_pool_contract_snapshot.json": contract,
        "source_inventory.json": {
            "schema_version": f"{SCHEMA_VERSION}_source_inventory_v1",
            "generated_at_utc": _utc_now(),
            "sources": sources,
        },
        "reconstruction_feasibility_audit.json": feasibility,
        "required_field_gap_audit.json": gap_audit,
        "no_lookahead_candidate_pool_audit.json": no_lookahead,
        "point_in_time_pool_decision.json": decision_payload,
    }
    for name, payload in artifacts.items():
        _write_json(session_root / name, payload)
    complete = {
        "schema_version": f"{SCHEMA_VERSION}_artifact_complete_v1",
        "generated_at_utc": _utc_now(),
        "session_root": str(session_root),
        "required_artifacts": REQUIRED_ARTIFACTS,
        "all_present": all((session_root / artifact).exists() for artifact in REQUIRED_ARTIFACTS if artifact != "_ARTIFACT_COMPLETE.json"),
    }
    _write_json(session_root / "_ARTIFACT_COMPLETE.json", complete)
    return {"session_root": str(session_root), "decision": decision}


def main() -> None:
    parser = argparse.ArgumentParser(description="TRADEX point-in-time candidate pool contract audit v1")
    parser.add_argument("--search-root", default=str(DEFAULT_SEARCH_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--max-sources", type=int, default=250)
    args = parser.parse_args()
    result = run_audit(
        search_root=_safe_path(args.search_root, DEFAULT_SEARCH_ROOT),
        output_root=_safe_path(args.output_root, DEFAULT_OUTPUT_ROOT),
        max_sources=args.max_sources,
    )
    print(json.dumps(base._json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
