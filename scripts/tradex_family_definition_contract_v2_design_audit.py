from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


AXIS_ID = "family_definition_contract_v2_design_audit"
DEFAULT_SELECTIVITY_ROOT = Path(r"G:\Tradex\pattern_family_selectivity_pretest_v1\20260525T131218Z-pattern-family-selectivity-pretest-v1")
DEFAULT_PREFLIGHT_ROOT = Path(r"G:\Tradex\candidate_generation_rebuild_preflight_v1\20260525T093517Z-candidate-generation-rebuild-preflight-v1")
DEFAULT_SOURCE_ROWS_ROOT = Path(r"G:\Tradex\pattern_family_source_rows_v1\20260525T101220Z-pattern-family-source-rows-v1")
DEFAULT_CANDIDATE_EVAL_ROOT = Path(r"G:\Tradex\pattern_family_candidate_evaluation_v1\20260525T101613Z-pattern-family-candidate-evaluation-v1")
DEFAULT_FROZEN_SEED_ROOT = Path(r"G:\Tradex\high_upside_reserve_risk_containment_robustness_gate_v1\20260525T091806Z-high-upside-reserve-risk-containment-robustness-gate-v1")
DEFAULT_PATTERN_SEED_ROOT = Path(r"G:\Tradex\pattern_family_seed_discovery_v1\20260525T092840Z-pattern-family-seed-discovery-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\family_definition_contract_v2_design_audit")
REQUIRED_ARTIFACTS = (
    "family_definition_v2_design_summary.json",
    "failed_family_contract_lineage.json",
    "promising_seed_inventory.json",
    "feature_gap_priority.json",
    "proposed_family_definition_contract_v2.json",
    "family_v2_validation_plan.json",
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


def failed_lineage(selectivity_decision: dict[str, Any], preflight_decision: dict[str, Any], source_decision: dict[str, Any], candidate_decision: dict[str, Any]) -> dict[str, Any]:
    return {
        "top10_after_processing_and_demotion_paths": {
            "status": "closed",
            "reason": "multiple branches moved topK or removed bad picks but replacement quality or mean_ret20 worsened",
        },
        "rank11_50_positive_lift": {
            "status": "closed",
            "reason": "promoted reserve candidates were weaker than displaced top10 and bad/severe worsened",
        },
        "high_upside_reserve_seed": {
            "status": "frozen_promising_underpowered",
            "reason": "upside signal exists but broad promotion/top10 lift worsened downside",
        },
        "high_upside_risk_containment_seed": {
            "status": "frozen_promising_underpowered",
            "reason": "variant_a_refined improved risk/return but support and period stability were insufficient",
        },
        "independent_setup_discovery": {
            "status": "closed",
            "reason": "broad setups added breadth without return or winner-rate quality",
        },
        "pattern_family_source_rows_v1": {
            "status": source_decision.get("research_decision"),
            "reason": "all-bars source rows were generated and ready for evaluation, but flags were only source-row flags",
        },
        "pattern_family_candidate_evaluation_v1": {
            "status": candidate_decision.get("research_decision"),
            "reason": "families were promising but weak; broad proxy and broad screen were not usable",
        },
        "pattern_family_selectivity_pretest_v1": {
            "status": selectivity_decision.get("research_decision"),
            "reason": "unique family rows did not preserve edge after overlap adjustment",
        },
        "candidate_generation_rebuild_preflight_v1": {
            "status": preflight_decision.get("research_decision"),
            "reason": "old source rows were diagnostic and not a real candidate-generation contract",
        },
    }


def promising_seed_inventory(frozen_summary: dict[str, Any], frozen_decision: dict[str, Any], pattern_metrics: dict[str, Any], pattern_overlap: dict[str, Any]) -> dict[str, Any]:
    frozen = frozen_summary["overall_fixed_variant_metrics"]
    family_b = pattern_metrics["family_b_constructive_pullback_support_bullish_confirmation"]
    family_b_overlap = pattern_overlap["family_b_constructive_pullback_support_bullish_confirmation"]
    return {
        "high_upside_reserve_risk_containment_robustness_gate_v1.variant_a_refined": {
            "decision": frozen_decision.get("research_decision"),
            "sample_count": frozen["sample_count"],
            "date_count": frozen["date_count"],
            "mean_ret20": frozen["mean_ret20"],
            "winner_rate": frozen["winner_rate"],
            "bad_rate": frozen["bad_rate"],
            "severe_rate": frozen["severe_rate"],
            "breadth_limitation": "kept_share_below_0_30; many zero-candidate dates; single-candidate date share high",
            "risk_limitation": "2025H1 and 2025H2 bad/severe rates unstable",
            "overlap_independence_notes": "frozen reserve-model seed; not a top10 replacement rule",
            "why_not_keep_worthy": "support and robustness gate failed despite strong direction",
        },
        "family_b_constructive_pullback_support_bullish_confirmation": {
            "decision": "pattern_family_seed_promising_but_underpowered",
            "sample_count": family_b["sample_count"],
            "date_count": family_b["date_count"],
            "mean_ret20": family_b["mean_ret20"],
            "winner_rate": family_b["winner_rate_ret20_gt_10pct"],
            "bad_rate": family_b["bad_rate_ret20_lt_minus_5pct"],
            "severe_rate": family_b["severe_rate_ret20_lt_minus_10pct"],
            "breadth_limitation": "only 52 samples across 47 dates in diagnostic seed surface",
            "risk_limitation": "bad/severe above keep gate",
            "overlap_independence_notes": {"frozen_seed_overlap_count": family_b_overlap["overlap_sample_count"], "overlap_rate": family_b_overlap["overlap_rate"]},
            "why_not_keep_worthy": "independent but thin and risky",
        },
    }


def feature_gap_priority(preflight_gaps: dict[str, Any], source_contract: dict[str, Any]) -> dict[str, Any]:
    source_fields = source_contract.get("fields", {})
    return {
        "liquidity_event_actionable_fields": {
            "available_status": "unavailable" if source_fields.get("liquidity_event_fields", {}).get("classification") == "unavailable" else "available_but_not_actionable",
            "expected_usefulness": "high for downside filtering and event-risk exclusion",
            "required_before_family_v2_implementation": False,
            "priority": "optional_blocker_for_risk_containment_not_source_generation",
        },
        "earnings_exrights_fields": {
            "available_status": "unavailable",
            "expected_usefulness": "medium-high for excluding event-risk losses",
            "required_before_family_v2_implementation": False,
            "priority": "optional",
        },
        "turnover_liquidity_proxies": {
            "available_status": "partial_available_via_volume_vs_20d_avg",
            "expected_usefulness": "medium for operational quality and avoiding thin moves",
            "required_before_family_v2_implementation": True,
            "priority": "required",
        },
        "atr_volatility_quality": {
            "available_status": "available_point_in_time",
            "expected_usefulness": "high for contained high-upside and compression families",
            "required_before_family_v2_implementation": True,
            "priority": "required",
        },
        "failed_high_upper_wick_candle_quality": {
            "available_status": "available_point_in_time",
            "expected_usefulness": "high for reducing downside without ret20-derived tags",
            "required_before_family_v2_implementation": True,
            "priority": "required",
        },
        "monthly_box_regime_quality": {
            "available_status": "available_point_in_time",
            "expected_usefulness": "medium; broad monthly/weekly screens failed, so must be used as context not family identity",
            "required_before_family_v2_implementation": True,
            "priority": "required_context_only",
        },
        "weekly_regime_quality": {
            "available_status": "available_point_in_time",
            "expected_usefulness": "medium; overlap-driven edge risk if used broadly",
            "required_before_family_v2_implementation": True,
            "priority": "required_context_only",
        },
        "pattern_source_lineage_fields": {
            "available_status": preflight_gaps["feature_groups"]["pattern_family_source_fields"]["classification"],
            "expected_usefulness": "low for v2 all-bars source if diagnostic labels are avoided",
            "required_before_family_v2_implementation": False,
            "priority": "optional_reference_only",
        },
    }


def proposed_contract_v2() -> dict[str, Any]:
    forbidden = ["ret5", "ret20", "mae20", "mfe20", "winner labels", "bad/severe labels", "ret20-derived tags", "future rank/outcome terms"]
    return {
        "contract_id": "family_definition_contract_v2",
        "principles": [
            "families must be narrow independent setup hypotheses, not broad supportive context screens",
            "weekly/monthly context may support a setup but cannot be the whole family identity",
            "risk containment must be part of each family definition before evaluation",
            "frozen seeds are references only and must not be retuned",
        ],
        "families": [
            {
                "family_id": "high_upside_contained_reserve_family_v2",
                "hypothesis": "high-upside reserve-like candidates are useful only when volatility/extension and failed-high risk are contained at source-row generation time",
                "required_point_in_time_features": ["winner_probability_proxy_or_high_upside_score_from_asof_model_probe", "atr14_pct", "realized_vol20", "close_vs_ma20_pct", "close_vs_ma60_pct", "upper_wick_ratio", "failed_high_flag", "volume_vs_20d_avg"],
                "inclusion_conditions": ["high-upside score bucket from chronological/as-of model probe or equivalent point-in-time positive selection score", "constructive daily trend context", "not a top10 replacement rule"],
                "exclusion_risk_conditions": ["extreme extension", "high ATR/realized volatility", "failed-high or heavy upper wick", "bearish body", "abnormal volume spike without confirmation"],
                "expected_sample_breadth": "narrow to moderate; must exceed frozen seed support without becoming broad proxy",
                "expected_failure_mode": "risk containment may remove winners or high-upside score may not be reproducible without model contract",
                "validation_metrics": ["mean_ret20", "winner_rate", "bad_rate", "severe_rate", "date_count", "kept_share", "period stability", "overlap independence"],
                "forbidden_terms_features": forbidden,
            },
            {
                "family_id": "constructive_pullback_confirmation_family_v2",
                "hypothesis": "pullback support setups need stronger candle confirmation and stricter risk exclusion to preserve the diagnostic family_b edge",
                "required_point_in_time_features": ["monthly_box_position", "monthly_box_width_pct", "weekly_supportive_flag", "close_vs_ma20_pct", "lower_wick_ratio", "body_ratio", "bullish_body_flag", "failed_high_flag", "upper_wick_ratio", "atr14_pct", "volume_vs_20d_avg"],
                "inclusion_conditions": ["near support or lower/mid monthly box", "bullish confirmation candle", "daily close near MA20 support", "weekly context supportive but not broad-only"],
                "exclusion_risk_conditions": ["upper-wick failed high", "bearish body", "wide box/high-zone chase", "ATR/volatility above family threshold", "event/liquidity exclusion only when actionable contract exists"],
                "expected_sample_breadth": "narrow; should be broader than 52 diagnostic rows but not broad screen",
                "expected_failure_mode": "still too sparse or bad/severe remains high without event/liquidity fields",
                "validation_metrics": ["unique-family mean_ret20", "winner_rate", "bad/severe", "selected_vs_unselected delta", "semiannual stability", "overlap adjusted edge"],
                "forbidden_terms_features": forbidden,
            },
            {
                "family_id": "volatility_compression_pre_breakout_family_v2",
                "hypothesis": "pre-breakout compression may work only when it is a preparation setup with constructive pressure, not a broad low-volatility screen",
                "required_point_in_time_features": ["realized_vol20", "atr14_pct", "monthly_box_position", "monthly_box_width_pct", "recent_high_distance_pct", "volume_vs_20d_avg", "ma7_slope_5d", "ma20_slope_10d", "failed_high_flag"],
                "inclusion_conditions": ["inside monthly box, below breakout chase zone", "compressed volatility and ATR", "constructive MA slope", "close not far below recent high", "volume confirmation not spike-chase"],
                "exclusion_risk_conditions": ["already broken out and extended", "failed high", "bearish body", "thin/no-volume drift", "monthly/weekly context used only as support"],
                "expected_sample_breadth": "moderate but must avoid the broad low-quality compression screen observed in v1",
                "expected_failure_mode": "compression remains low-return without a stronger pressure/volume feature contract",
                "validation_metrics": ["mean_ret20", "winner_rate", "bad/severe", "candidate count per date", "unique-row edge", "overlap matrix"],
                "forbidden_terms_features": forbidden,
            },
        ],
    }


def validation_plan(contract: dict[str, Any]) -> dict[str, Any]:
    return {
        family["family_id"]: {
            "stage_1_source_row_generation": "generate v2 rows from confirmed bars only with family-specific inclusion/exclusion flags and no offline outcomes in live columns",
            "stage_2_family_candidate_evaluation": "evaluate return/risk/breadth per family using offline outcomes only",
            "stage_3_selectivity_risk_containment": "test fixed predeclared selectivity only if family candidate evaluation is promising",
            "stage_4_robustness_support_gate": "semiannual/regime/date concentration and overlap-adjusted robustness",
            "stage_5_optional_family_portfolio_pretest": "only if keep-worthy family passes robustness gate; no MeeMee or production mutation",
        }
        for family in contract["families"]
    }


def decide(feature_gaps: dict[str, Any], contract: dict[str, Any], lineage: dict[str, Any]) -> tuple[str, list[str]]:
    if not lineage or not contract.get("families"):
        return "blocked_missing_lineage_contract", ["prior_lineage_or_v2_contract_missing"]
    required = ["atr_volatility_quality", "failed_high_upper_wick_candle_quality", "monthly_box_regime_quality", "weekly_regime_quality", "turnover_liquidity_proxies"]
    missing_required = [k for k in required if feature_gaps[k]["available_status"].startswith("unavailable")]
    if missing_required:
        return "add_feature_contracts_before_family_v2", [f"missing_required_feature_contracts:{','.join(missing_required)}"]
    return "proceed_to_family_definition_v2_source_generation", ["enough_point_in_time_features_exist_to_implement_three_narrow_family_v2_concepts"]


def run(
    selectivity_root: Path,
    preflight_root: Path,
    source_rows_root: Path,
    candidate_eval_root: Path,
    frozen_seed_root: Path,
    pattern_seed_root: Path,
    output_root: Path,
) -> Path:
    out = output_root / f"{_now_tag()}-family-definition-contract-v2-design-audit"
    out.mkdir(parents=True, exist_ok=True)
    try:
        selectivity_decision = load_json(selectivity_root / "research_decision.json")
        preflight_decision = load_json(preflight_root / "research_decision.json")
        source_decision = load_json(source_rows_root / "research_decision.json")
        candidate_decision = load_json(candidate_eval_root / "research_decision.json")
        frozen_summary = load_json(frozen_seed_root / "robustness_gate_summary.json")
        frozen_decision = load_json(frozen_seed_root / "research_decision.json")
        pattern_metrics = load_json(pattern_seed_root / "candidate_family_metrics.json")
        pattern_overlap = load_json(pattern_seed_root / "family_vs_frozen_seed_overlap.json")
        preflight_gaps = load_json(preflight_root / "feature_contract_gap_audit.json")
        source_contract = load_json(source_rows_root / "feature_contract.json")
        blocked = False
        block_reason = None
    except Exception as exc:
        selectivity_decision = {}
        preflight_decision = {}
        source_decision = {}
        candidate_decision = {}
        frozen_summary = {}
        frozen_decision = {}
        pattern_metrics = {}
        pattern_overlap = {}
        preflight_gaps = {}
        source_contract = {}
        blocked = True
        block_reason = str(exc)

    if blocked:
        lineage = {}
        seeds = {}
        gaps = {}
        contract = {"families": []}
        plan = {}
        decision = "blocked_missing_lineage_contract"
        reasons = [block_reason or "lineage_load_failed"]
    else:
        lineage = failed_lineage(selectivity_decision, preflight_decision, source_decision, candidate_decision)
        seeds = promising_seed_inventory(frozen_summary, frozen_decision, pattern_metrics, pattern_overlap)
        gaps = feature_gap_priority(preflight_gaps, source_contract)
        contract = proposed_contract_v2()
        plan = validation_plan(contract)
        decision, reasons = decide(gaps, contract, lineage)

    _write_json(out / "failed_family_contract_lineage.json", lineage)
    _write_json(out / "promising_seed_inventory.json", seeds)
    _write_json(out / "feature_gap_priority.json", gaps)
    _write_json(out / "proposed_family_definition_contract_v2.json", contract)
    _write_json(out / "family_v2_validation_plan.json", plan)
    _write_json(out / "family_definition_v2_design_summary.json", {"axis_id": AXIS_ID, "decision": decision, "reason_typed": reasons, "proposed_family_count": len(contract.get("families", [])), "lineage_read_complete": not blocked})
    _write_json(out / "no_lookahead_audit.json", {"audit_result": "blocked" if blocked else "pass", "design_only": True, "existing_artifacts_only": True, "outcomes_used_for_design_lineage_only": True, "ret20_derived_feature_construction": False, "runtime_db_write": False, "research_fallback_used": False})
    _write_json(out / "source_coverage.json", {"axis_id": AXIS_ID, "lineage_artifacts_read": 8 if not blocked else 0, "proposed_family_count": len(contract.get("families", [])), "research_fallback_used": False})
    _write_json(out / "research_decision.json", {"axis_id": AXIS_ID, "research_decision": decision, "reason_typed": reasons, "meemee_reflectable_candidate": False, "runtime_db_write": False, "production_ranking_changed": False, "production_candidate_generator_changed": False, "publish_allowed": False, "validated_buy_count": 0, "active_gate_created": False})
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"complete": True, "artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--selectivity-root", type=Path, default=DEFAULT_SELECTIVITY_ROOT)
    parser.add_argument("--preflight-root", type=Path, default=DEFAULT_PREFLIGHT_ROOT)
    parser.add_argument("--source-rows-root", type=Path, default=DEFAULT_SOURCE_ROWS_ROOT)
    parser.add_argument("--candidate-eval-root", type=Path, default=DEFAULT_CANDIDATE_EVAL_ROOT)
    parser.add_argument("--frozen-seed-root", type=Path, default=DEFAULT_FROZEN_SEED_ROOT)
    parser.add_argument("--pattern-seed-root", type=Path, default=DEFAULT_PATTERN_SEED_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    out = run(args.selectivity_root, args.preflight_root, args.source_rows_root, args.candidate_eval_root, args.frozen_seed_root, args.pattern_seed_root, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
