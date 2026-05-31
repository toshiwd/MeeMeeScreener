from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


AXIS_ID = "buy_candidate_generation_surface_closure_v1"
DEFAULT_V2_EVAL_ROOT = Path(r"G:\Tradex\family_definition_v2_candidate_evaluation\20260525T132927Z-family-definition-v2-candidate-evaluation")
DEFAULT_V2_SOURCE_ROOT = Path(r"G:\Tradex\family_definition_v2_source_rows\20260525T132408Z-family-definition-v2-source-rows")
DEFAULT_SELECTIVITY_ROOT = Path(r"G:\Tradex\pattern_family_selectivity_pretest_v1\20260525T131218Z-pattern-family-selectivity-pretest-v1")
DEFAULT_PREFLIGHT_ROOT = Path(r"G:\Tradex\candidate_generation_rebuild_preflight_v1\20260525T093517Z-candidate-generation-rebuild-preflight-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\buy_candidate_generation_surface_closure_v1")
REQUIRED_ARTIFACTS = (
    "surface_closure_summary.json",
    "lineage.json",
    "failed_surface_evidence.json",
    "missing_contract_decision.json",
    "next_contract_requirements.json",
    "no_lookahead_audit.json",
    "source_coverage.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "item"):
        return _json_ready(value.item())
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def failed_surface_evidence(v2_metrics: dict[str, Any], v2_decisions: dict[str, Any], selectivity_decision: dict[str, Any]) -> dict[str, Any]:
    return {
        "v1_selectivity": {
            "decision": selectivity_decision.get("research_decision"),
            "reason_typed": selectivity_decision.get("reason_typed"),
            "interpretation": "current v1 family/selectivity contract closed as overlap-driven",
        },
        "v2_candidate_evaluation": {
            "family_decisions": v2_decisions,
            "family_metrics": v2_metrics,
            "interpretation": "all v2 source-row families were weak positive without sufficient winner or risk edge",
        },
    }


def missing_contract_decision(preflight_gaps: dict[str, Any], v2_source_contract: dict[str, Any]) -> dict[str, Any]:
    fields = v2_source_contract.get("fields", {})
    missing = {
        "actionable_liquidity_event_fields": fields.get("liquidity_event_fields", {}).get("classification") == "unavailable",
        "earnings_exrights_fields": fields.get("earnings_exrights_fields", {}).get("classification") == "unavailable",
        "asof_high_upside_score_contract": True,
        "event_adjusted_risk_contract": True,
    }
    return {
        "decision": "add_feature_contracts_before_continue",
        "decision_class": "BLOCKED",
        "missing_or_insufficient_contracts": missing,
        "why_current_features_are_insufficient": [
            "v2 all-bars family flags produced breadth but not winner/risk edge",
            "risk control remains limited without actionable liquidity/event/earnings contracts",
            "high-upside concept cannot be faithfully rebuilt without an as-of positive-selection score contract",
            "additional fixed-rule combinations on current flags would be retuning a closed surface",
        ],
        "preflight_feature_gap_reference": preflight_gaps.get("feature_groups", {}),
    }


def next_contract_requirements() -> dict[str, Any]:
    return {
        "required_before_more_family_generation": [
            {
                "contract_id": "asof_positive_selection_score_v1",
                "purpose": "provide point-in-time high-upside probability or score for all eligible symbols/dates without using future outcomes as live features",
                "minimum_requirements": ["chronological training/evaluation separation", "score timestamped by as_of_date", "feature list audited as point-in-time", "OOS calibration and bucket metrics"],
            },
            {
                "contract_id": "actionable_event_liquidity_risk_v1",
                "purpose": "filter or tag avoidable downside from earnings/ex-rights/event/liquidity risk using point-in-time sources",
                "minimum_requirements": ["event date known by as_of_date", "liquidity/turnover proxy not derived from future", "missing data explicitly classified", "no fabricated placeholders"],
            },
        ],
        "allowed_after_contracts_exist": [
            "rebuild family_definition_contract_v3 from all bars",
            "evaluate high-upside contained reserve as independent family, not top10 replacement",
            "risk containment with actionable event/liquidity contracts",
        ],
        "not_allowed": [
            "more threshold retuning on v1/v2 family flags",
            "top10 after-processing rescue",
            "MeeMee reflection",
            "production candidate generator mutation",
        ],
    }


def decide(v2_decision: dict[str, Any], missing: dict[str, Any]) -> tuple[str, str, list[str]]:
    if v2_decision.get("research_decision") == "keep_for_next_stage":
        return "keep_for_next_stage", "KEEP", ["v2_family_keep_found"]
    return "add_feature_contracts_before_continue", "BLOCKED", ["current_v1_v2_family_surfaces_exhausted_missing_feature_contracts_required_before_valid_continuation"]


def run(v2_eval_root: Path, v2_source_root: Path, selectivity_root: Path, preflight_root: Path, output_root: Path) -> Path:
    out = output_root / f"{_now_tag()}-buy-candidate-generation-surface-closure-v1"
    out.mkdir(parents=True, exist_ok=True)
    v2_decision = load_json(v2_eval_root / "research_decision.json")
    v2_metrics = load_json(v2_eval_root / "family_v2_metrics.json")
    v2_family_decisions = load_json(v2_eval_root / "family_v2_candidate_decisions.json")
    selectivity_decision = load_json(selectivity_root / "research_decision.json")
    preflight_gaps = load_json(preflight_root / "feature_contract_gap_audit.json")
    v2_source_contract = load_json(v2_source_root / "feature_contract.json")
    evidence = failed_surface_evidence(v2_metrics, v2_family_decisions, selectivity_decision)
    missing = missing_contract_decision(preflight_gaps, v2_source_contract)
    decision, cls, reasons = decide(v2_decision, missing)
    lineage = {
        "v2_eval_root": v2_eval_root,
        "v2_source_root": v2_source_root,
        "selectivity_root": selectivity_root,
        "preflight_root": preflight_root,
        "artifacts_read": ["research_decision.json", "family_v2_metrics.json", "family_v2_candidate_decisions.json", "feature_contract_gap_audit.json", "feature_contract.json"],
    }
    _write_json(out / "lineage.json", lineage)
    _write_json(out / "failed_surface_evidence.json", evidence)
    _write_json(out / "missing_contract_decision.json", missing)
    _write_json(out / "next_contract_requirements.json", next_contract_requirements())
    _write_json(out / "surface_closure_summary.json", {"axis_id": AXIS_ID, "decision": decision, "decision_class": cls, "reason_typed": reasons, "current_surface_exhausted": cls == "BLOCKED", "recommended_next_action": "add_point_in_time_feature_contracts_before_more_family_testing" if cls == "BLOCKED" else "proceed_to_next_validation_stage", "terminal_condition": "current_research_surface_exhausted_clear_add_feature_contract_decision"})
    _write_json(out / "no_lookahead_audit.json", {"audit_result": "pass", "no_lookahead_pass": True, "closure_artifact_only": True, "existing_artifacts_only": True, "new_features_constructed": False, "outcomes_used_for_lineage_only": True, "runtime_db_write": False, "research_fallback_used": False})
    _write_json(out / "source_coverage.json", {"axis_id": AXIS_ID, "coverage_status": "pass", "lineage_artifacts_read": 5, "research_fallback_used": False})
    _write_json(out / "research_decision.json", {"axis_id": AXIS_ID, "research_decision": decision, "decision_class": cls, "reason_typed": reasons, "meemee_reflectable_candidate": False, "runtime_db_write": False, "production_ranking_changed": False, "production_candidate_generator_changed": False, "publish_allowed": False, "validated_buy_count": 0, "active_gate_created": False, "research_fallback_used": False})
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"complete": True, "artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--v2-eval-root", type=Path, default=DEFAULT_V2_EVAL_ROOT)
    parser.add_argument("--v2-source-root", type=Path, default=DEFAULT_V2_SOURCE_ROOT)
    parser.add_argument("--selectivity-root", type=Path, default=DEFAULT_SELECTIVITY_ROOT)
    parser.add_argument("--preflight-root", type=Path, default=DEFAULT_PREFLIGHT_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    out = run(args.v2_eval_root, args.v2_source_root, args.selectivity_root, args.preflight_root, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
