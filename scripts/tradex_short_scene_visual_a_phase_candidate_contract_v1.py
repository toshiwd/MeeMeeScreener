from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_OUTPUT_ROOT = Path("G:/Tradex/short_scene_visual_a_phase_candidate_contract_v1")


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _artifact_complete(compare: dict[str, Any]) -> bool:
    complete_path = compare.get("artifacts", {}).get("artifact_complete")
    if not complete_path:
        return False
    path = Path(str(complete_path))
    if not path.exists():
        return False
    try:
        payload = _load_json(path)
    except json.JSONDecodeError:
        return False
    return payload.get("complete") is True


def _contract_decision(compare: dict[str, Any]) -> dict[str, Any]:
    scope = compare.get("scope", {})
    coverage = compare.get("coverage", {})
    oos = compare.get("compare", {}).get("oos", {})
    top5 = oos.get("top5", {})
    top10 = oos.get("top10", {})
    top5_delta = top5.get("additive_delta", {})
    top10_delta = top10.get("additive_delta", {})
    checks = {
        "source_decision_keep": compare.get("authoritative_rollup_decision") == "keep",
        "artifact_complete": _artifact_complete(compare),
        "tradex_only": scope.get("tradex_only") is True,
        "no_meemee_or_runtime_mutation": all(
            scope.get(field) is False
            for field in ("meemee_ranking_changed", "meemee_ui_changed", "runtime_db_written")
        ),
        "no_fallback": scope.get("silent_fallback_used") is False and scope.get("research_fallback_used") is False,
        "candidate_generation_challenger_created": compare.get("candidate_generation_challenger_created") is True,
        "oos_branching_present": (compare.get("observed_branching", {}).get("changed_top5_members_count") or 0) >= 10,
        "oos_top5_mean_improves": (top5_delta.get("forward_return_20_mean") or 0.0) >= 0.002,
        "oos_top5_bad_loser_not_worse": (top5_delta.get("bad_loser_rate_20") or 0.0) <= 0.0,
        "oos_top5_severe_loser_not_worse": (top5_delta.get("severe_loser_rate_20") or 0.0) <= 0.0,
        "oos_top10_not_damaged": (top10_delta.get("forward_return_20_mean") or 0.0) >= -0.001
        and (top10_delta.get("bad_loser_rate_20") or 0.0) <= 0.0,
        "oos_month_stability_pass": (coverage.get("oos_positive_active_month_rate") or 0.0) >= 0.60
        and (coverage.get("oos_active_month_count") or 0) >= 6,
        "meemee_reflectable_still_false": compare.get("meemee_reflectable") is False,
    }
    blockers = [name for name, passed in checks.items() if not passed]
    if blockers:
        return {
            "decision": "hold",
            "reason_type": "candidate_contract_blocked",
            "checks": checks,
            "blockers": blockers,
        }
    return {
        "decision": "candidate_generation_contract_ready",
        "reason_type": "oos_keep_contract_passed_for_tradex_only_shadow_candidate_generation",
        "checks": checks,
        "blockers": [],
    }


def run_contract(*, source_compare: Path, output_root: Path) -> dict[str, Any]:
    output_dir = output_root / f"{_now_tag()}-short_scene_visual_a_phase_candidate_contract_v1"
    output_dir.mkdir(parents=True, exist_ok=True)
    compare = _load_json(source_compare)
    decision = _contract_decision(compare)
    result = {
        "schema_version": "tradex_short_scene_visual_a_phase_candidate_contract_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "authoritative_result": True,
        "research_phase": "comparison_stabilization",
        "source_compare_json": str(source_compare),
        "candidate_contract": {
            "candidate_id": "short_scene_visual_a_phase_100ma_slope_tight_oos_v1",
            "owner": "TRADEX",
            "side": "sell",
            "rule": {
                "market_scene": "downtrend_a_phase",
                "action_bias": "sell_rebound_rejection_or_lower_low",
                "visual_decision": "pullback_probe_candidate",
                "shape_intent": "a_phase_downtrend_100ma_rejection",
                "ma20_slope_10_floor": -0.005,
                "selection_cardinality": "at most one outside-gap candidate per sell-candidate date",
                "selection_sort": "lower ma20_slope_10 first, then code",
            },
            "production_scoring_created": False,
            "runtime_candidate_generator_created": False,
            "paper_replay_ready": decision["decision"] == "candidate_generation_contract_ready",
            "meemee_reflectable": False,
        },
        "scope": {
            "tradex_only": True,
            "meemee_ranking_changed": False,
            "meemee_ui_changed": False,
            "runtime_db_written": False,
            "silent_fallback_used": False,
            "research_fallback_used": False,
        },
        "authoritative_rollup_decision": decision["decision"],
        "reason_type": decision["reason_type"],
        "checks": decision["checks"],
        "blockers": decision["blockers"],
        "remaining_risks": [
            "contract is for TRADEX shadow candidate generation only",
            "production scoring is not implemented",
            "runtime candidate generator is not implemented",
            "forward live paper replay has not been run",
            "OHLC visual proxy is not pixel screenshot analysis",
        ],
    }
    contract_path = output_dir / "short_scene_visual_a_phase_candidate_contract.json"
    decision_path = output_dir / "short_scene_visual_a_phase_candidate_contract_decision.json"
    result["artifacts"] = {
        "output_dir": str(output_dir),
        "contract_json": str(contract_path),
        "decision_json": str(decision_path),
        "artifact_complete": str(output_dir / "_ARTIFACT_COMPLETE.json"),
    }
    _write_json(contract_path, result)
    _write_json(
        decision_path,
        {
            "schema_version": result["schema_version"],
            "authoritative_rollup_decision": result["authoritative_rollup_decision"],
            "reason_type": result["reason_type"],
            "candidate_contract": result["candidate_contract"],
            "checks": result["checks"],
            "blockers": result["blockers"],
            "remaining_risks": result["remaining_risks"],
            "artifacts": result["artifacts"],
        },
    )
    _write_json(output_dir / "_ARTIFACT_COMPLETE.json", {"complete": True, **result["artifacts"]})
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-compare", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    result = run_contract(source_compare=args.source_compare, output_root=args.output_root)
    print(json.dumps({"decision": result["authoritative_rollup_decision"], "reason_type": result["reason_type"], "artifacts": result["artifacts"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
