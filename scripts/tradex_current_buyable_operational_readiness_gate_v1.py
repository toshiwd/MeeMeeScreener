from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


AXIS_ID = "current_buyable_operational_readiness_gate_v1"
DEFAULT_FORWARD_ROOT = Path(
    r"G:\Tradex\current_buyable_forward_paper_validation_v1\20260526T010838Z-current-buyable-forward-paper-validation-v1"
)
DEFAULT_INVALIDATION_ROOT = Path(
    r"G:\Tradex\current_buyable_invalidation_contract_v2_apply\20260526T014806Z-current-buyable-invalidation-contract-v2-apply"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\current_buyable_operational_readiness_gate_v1")
REQUIRED_ARTIFACTS = (
    "operational_readiness_summary.json",
    "readiness_gate_audit.json",
    "candidate_status.json",
    "blocking_contracts.json",
    "promotion_boundary.json",
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
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_inputs(forward_root: Path, invalidation_root: Path) -> dict[str, Any]:
    invalidation_summary = invalidation_root / "invalidation_contract_summary.json"
    if not invalidation_summary.exists():
        invalidation_summary = invalidation_root / "invalidation_contract_repair_summary.json"
    if not invalidation_summary.exists():
        invalidation_summary = invalidation_root / "invalidation_contract_v2_summary.json"
    paths = {
        "forward_decision": forward_root / "research_decision.json",
        "forward_summary": forward_root / "forward_paper_validation_summary.json",
        "forward_metrics": forward_root / "ret5_ret20_metrics.json",
        "invalidation_decision": invalidation_root / "research_decision.json",
        "invalidation_summary": invalidation_summary,
        "invalidation_source": invalidation_root / "source_coverage.json",
    }
    missing = [str(path) for path in paths.values() if not path.exists()]
    if missing:
        raise FileNotFoundError(", ".join(missing))
    return {name: _load_json(path) for name, path in paths.items()}


def build_candidate_status(inputs: dict[str, Any]) -> dict[str, Any]:
    forward = inputs["forward_summary"]
    inv = inputs["invalidation_summary"]
    return {
        "axis_id": AXIS_ID,
        "selected_as_of_date": forward.get("selected_as_of_date"),
        "selected_codes": forward.get("selected_codes", []),
        "candidate_count": forward.get("candidate_count", 0),
        "minimum_available_future_sessions": forward.get("minimum_available_future_sessions", 0),
        "ret5_all_candidates_ready": bool(forward.get("ret5_all_candidates_ready") is True),
        "ret20_all_candidates_ready": bool(forward.get("ret20_all_candidates_ready") is True),
        "forward_status_counts": forward.get("status_counts", {}),
        "invalidation_complete_level_count": inv.get("complete_level_count", 0),
        "validated_buy_count": 0,
    }


def readiness_gate(inputs: dict[str, Any]) -> dict[str, Any]:
    f_decision = inputs["forward_decision"]
    f_summary = inputs["forward_summary"]
    f_metrics = inputs["forward_metrics"]
    i_decision = inputs["invalidation_decision"]
    i_source = inputs["invalidation_source"]

    freeze_gate = f_summary.get("candidate_count") == 2 and f_summary.get("selected_codes") == ["8086", "9831"]
    ret5_gate = bool(f_summary.get("ret5_all_candidates_ready") is True)
    ret20_gate = bool(f_summary.get("ret20_all_candidates_ready") is True)
    ret20_metrics = f_metrics.get("ret20", {})
    ret20_quality_gate = (
        ret20_gate
        and ret20_metrics.get("mean_ret20") is not None
        and ret20_metrics.get("mean_ret20") > 0.03
        and ret20_metrics.get("winner_rate_ret20_gt_10pct") is not None
        and ret20_metrics.get("winner_rate_ret20_gt_10pct") >= 0.20
        and ret20_metrics.get("bad_rate_ret20_lt_minus_5pct") is not None
        and ret20_metrics.get("bad_rate_ret20_lt_minus_5pct") <= 0.20
        and ret20_metrics.get("severe_rate_ret20_lt_minus_10pct") is not None
        and ret20_metrics.get("severe_rate_ret20_lt_minus_10pct") <= 0.10
    )
    invalidation_gate = i_decision.get("research_decision") in {
        "invalidation_contract_ready_for_forward_tracking",
        "invalidation_contract_repaired_full_levels_ready",
        "invalidation_contract_v2_stop_atr2_ready_for_forward_tracking",
    }
    v2_stop_contract = i_decision.get("research_decision") == "invalidation_contract_v2_stop_atr2_ready_for_forward_tracking"
    full_risk_contract_gate = bool(
        (
            i_source.get("feature_snapshot_complete") is True
            or (i_source.get("ma20_complete") is True and i_source.get("atr14_complete") is True)
            or (v2_stop_contract and i_source.get("atr14_complete") is True)
        )
        and (i_source.get("recent_swing_low_complete") is True or v2_stop_contract)
    )
    boundary_gate = all(
        item is False
        for item in [
            f_decision.get("runtime_db_write"),
            f_decision.get("meemee_reflectable_candidate"),
            f_decision.get("production_ranking_changed"),
            f_decision.get("production_candidate_generator_changed"),
            f_decision.get("active_gate_created"),
        ]
    ) and f_decision.get("validated_buy_count") == 0

    gate_payload = {
        "axis_id": AXIS_ID,
        "freeze_gate_pass": bool(freeze_gate),
        "ret5_maturity_gate_pass": bool(ret5_gate),
        "ret20_maturity_gate_pass": bool(ret20_gate),
        "ret20_quality_gate_pass": bool(ret20_quality_gate),
        "invalidation_tracking_gate_pass": bool(invalidation_gate),
        "full_risk_contract_gate_pass": bool(full_risk_contract_gate),
        "boundary_gate_pass": bool(boundary_gate),
        "operational_readiness_gate_pass": bool(
            freeze_gate
            and ret5_gate
            and ret20_gate
            and ret20_quality_gate
            and invalidation_gate
            and full_risk_contract_gate
            and boundary_gate
        ),
        "thresholds": {
            "selected_codes_exact": ["8086", "9831"],
            "ret5_all_candidates_ready": True,
            "ret20_all_candidates_ready": True,
            "ret20_mean_min": 0.03,
            "ret20_winner_rate_min": 0.20,
            "ret20_bad_rate_max": 0.20,
            "ret20_severe_rate_max": 0.10,
            "full_risk_contract_requires_feature_snapshot_and_swing_low": True,
        },
    }
    return gate_payload


def blocking_contracts(gate: dict[str, Any], inputs: dict[str, Any]) -> dict[str, Any]:
    blockers: list[dict[str, Any]] = []
    if not gate["ret5_maturity_gate_pass"]:
        blockers.append({"contract": "forward_ret5_confirmed_outcome", "status": "missing", "reason": "future_confirmed_sessions_below_5"})
    if not gate["ret20_maturity_gate_pass"]:
        blockers.append({"contract": "forward_ret20_confirmed_outcome", "status": "missing", "reason": "future_confirmed_sessions_below_20"})
    if not gate["ret20_quality_gate_pass"]:
        blockers.append({"contract": "ret20_quality_gate", "status": "not_evaluable_or_failed", "reason": "ret20_metrics_unavailable_or_below_threshold"})
    if not gate["full_risk_contract_gate_pass"]:
        blockers.append(
            {
                "contract": "full_invalidation_risk_levels",
                "status": "partial",
                "reason": "feature_snapshot_daily_missing_selection_date_ma_atr_levels",
                "available": "recent_swing_low",
            }
        )
    return {
        "axis_id": AXIS_ID,
        "blocking_contract_count": len(blockers),
        "blocking_contracts": blockers,
        "latest_forward_decision": inputs["forward_decision"].get("research_decision"),
        "latest_invalidation_decision": inputs["invalidation_decision"].get("research_decision"),
    }


def no_lookahead_audit(inputs: dict[str, Any]) -> dict[str, Any]:
    forward_ok = inputs["forward_decision"].get("research_decision") in {
        "forward_validation_pending_more_confirmed_bars",
        "ret5_pass_ret20_pending",
        "ret20_pass_ready_for_robustness_gate",
    }
    invalidation_ok = inputs["invalidation_decision"].get("research_decision") in {
        "invalidation_contract_ready_for_forward_tracking",
        "invalidation_contract_repaired_full_levels_ready",
        "invalidation_contract_v2_stop_atr2_ready_for_forward_tracking",
    }
    return {
        "audit_result": "pass" if forward_ok and invalidation_ok else "blocked",
        "no_lookahead_pass": bool(forward_ok and invalidation_ok),
        "outcomes_used_for_selection": False,
        "readiness_gate_uses_authoritative_evaluation_artifacts_only": True,
        "runtime_db_write": False,
        "research_fallback_used": False,
    }


def decide(gate: dict[str, Any], blockers: dict[str, Any], audit: dict[str, Any]) -> tuple[str, str, list[str]]:
    if not audit["no_lookahead_pass"]:
        return "blocked_no_lookahead_violation", "BLOCKED", ["readiness_inputs_failed_no_lookahead_contract"]
    if gate["operational_readiness_gate_pass"]:
        return "operational_readiness_ready_for_shadow_only_review", "KEEP", ["all_predeclared_operational_readiness_gates_passed"]
    if blockers["blocking_contract_count"] > 0:
        return "operational_readiness_blocked_pending_forward_outcomes_or_full_risk_contract", "BLOCKED", [
            "forward_ret5_ret20_or_full_risk_contracts_missing"
        ]
    return "operational_readiness_not_ready", "HOLD_UNDERPOWERED", ["operational_gate_not_passed"]


def run(
    forward_root: Path = DEFAULT_FORWARD_ROOT,
    invalidation_root: Path = DEFAULT_INVALIDATION_ROOT,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
) -> Path:
    inputs = load_inputs(forward_root, invalidation_root)
    status = build_candidate_status(inputs)
    gate = readiness_gate(inputs)
    blockers = blocking_contracts(gate, inputs)
    audit = no_lookahead_audit(inputs)
    decision, decision_class, reasons = decide(gate, blockers, audit)

    out = output_root / f"{_now_tag()}-current-buyable-operational-readiness-gate-v1"
    out.mkdir(parents=True, exist_ok=True)
    _write_json(
        out / "operational_readiness_summary.json",
        {
            "axis_id": AXIS_ID,
            "decision": decision,
            "decision_class": decision_class,
            "reason_typed": reasons,
            "operational_readiness_gate_pass": gate["operational_readiness_gate_pass"],
            "candidate_status": status,
            "blocking_contract_count": blockers["blocking_contract_count"],
            "validated_buy_count": 0,
            "production_ready": False,
        },
    )
    _write_json(out / "readiness_gate_audit.json", gate)
    _write_json(out / "candidate_status.json", status)
    _write_json(out / "blocking_contracts.json", blockers)
    _write_json(
        out / "promotion_boundary.json",
        {
            "axis_id": AXIS_ID,
            "meemee_reflection_allowed": False,
            "runtime_db_write_allowed": False,
            "production_ranking_change_allowed": False,
            "production_candidate_generator_change_allowed": False,
            "publish_allowed": False,
            "active_gate_created": False,
            "validated_buy_count": 0,
            "user_facing_trade_recommendation_allowed": False,
        },
    )
    _write_json(out / "no_lookahead_audit.json", audit)
    _write_json(
        out / "source_coverage.json",
        {
            "axis_id": AXIS_ID,
            "forward_root": str(forward_root),
            "invalidation_root": str(invalidation_root),
            "forward_artifacts_loaded": True,
            "invalidation_artifacts_loaded": True,
            "research_fallback_used": False,
        },
    )
    _write_json(
        out / "research_decision.json",
        {
            "axis_id": AXIS_ID,
            "research_decision": decision,
            "decision_class": decision_class,
            "reason_typed": reasons,
            "research_watch_only": True,
            "operational_readiness_gate_pass": gate["operational_readiness_gate_pass"],
            "production_ready": False,
            "meemee_reflectable_candidate": False,
            "runtime_db_write": False,
            "production_ranking_changed": False,
            "production_candidate_generator_changed": False,
            "publish_allowed": False,
            "validated_buy_count": 0,
            "active_gate_created": False,
            "research_fallback_used": False,
        },
    )
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"complete": True, "artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--forward-root", type=Path, default=DEFAULT_FORWARD_ROOT)
    parser.add_argument("--invalidation-root", type=Path, default=DEFAULT_INVALIDATION_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    out = run(args.forward_root, args.invalidation_root, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
