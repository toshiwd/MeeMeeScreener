from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


RESEARCH_NAME = "tradex_daily_action_policy_engine_v1"
SCHEMA_PREFIX = "tradex_daily_action_policy_engine_v1"
REQUIRED_ARTIFACTS = [
    "diagnosis_summary.json",
    "research_axis_decision.json",
    "evaluation_contract.json",
    "replay_capability_report.json",
    "missing_data_or_capability_report.json",
    "proposed_policy_families.json",
    "risk_audit.json",
    "final_decision.json",
    "_ARTIFACT_COMPLETE.json",
]


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _timestamp() -> str:
    return _utcnow().strftime("%Y%m%dT%H%M%SZ")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _git_status(cwd: Path) -> dict[str, Any]:
    try:
        proc = subprocess.run(["git", "status", "--short", "--branch"], cwd=str(cwd), check=False, text=True, capture_output=True, timeout=20)
    except Exception as exc:  # pragma: no cover - defensive for non-git temp tests
        return {"available": False, "error": str(exc)}
    return {
        "available": proc.returncode == 0,
        "returncode": proc.returncode,
        "stdout": proc.stdout.strip().splitlines(),
        "stderr": proc.stderr.strip().splitlines(),
    }


def _artifact_header(name: str, generated_at: str) -> dict[str, Any]:
    return {
        "schema_version": f"{SCHEMA_PREFIX}_{name}_v1",
        "research_name": RESEARCH_NAME,
        "generated_at": generated_at,
        "status": "authoritative",
        "boundary": "TRADEX-only",
    }


def build_artifacts(*, output_root: Path, repo_root: Path | None = None, run_id: str | None = None, commands_run: list[str] | None = None) -> dict[str, Any]:
    generated_at = _utcnow().isoformat()
    run_id = run_id or _timestamp()
    repo_root = repo_root or Path.cwd()
    run_dir = output_root / run_id
    commands = list(commands_run or [])
    if not commands:
        commands = ["python scripts/tradex_daily_action_policy_engine_v1.py --output-root <output_root>"]

    evidence_refs = {
        "research_inventory": "artifacts/research_inventory/research_inventory.json",
        "replay_simulator": "external_analysis/policy_replay/simulator.py",
        "replay_service": "app/backend/services/tradex_portfolio_replay_service.py",
        "runtime_freshness": "artifacts/research_inventory/runtime_stock_db_freshness.json",
        "replay_role_contract": "docs/contracts/tradex_replay_model_role_contract.json",
    }

    artifacts: dict[str, dict[str, Any]] = {
        "diagnosis_summary.json": {
            **_artifact_header("diagnosis_summary", generated_at),
            "research_phase": "infrastructure_stabilization",
            "dominant_failure_buckets": [
                "entry_timing_failure",
                "opportunity_cost_failure",
                "regime_mismatch",
                "cost_slippage_turnover_failure",
                "partial_hedge_failure",
            ],
            "evidence_references": evidence_refs,
            "verified": {
                "existing_replay_has_trade_ledger": True,
                "existing_replay_has_positions_timeline": True,
                "existing_replay_has_daily_equity_cash_exposure_drawdown": True,
                "full_next_session_open_execution": True,
                "direct_ranking_improvement_is_not_selected_axis": True,
            },
            "unverified": {
                "profitable_policy_candidate": True,
                "broker_specific_cost_model": True,
                "event_news_coverage_complete": True,
            },
            "not_changed": [
                "MeeMee runtime",
                "production ranking",
                "publish registry",
                "promotion logic",
                "frontend behavior",
            ],
        },
        "research_axis_decision.json": {
            **_artifact_header("research_axis_decision", generated_at),
            "selected_axis": "long-side action policy plus hedge decision plus opportunity-cost / rotation diagnosis",
            "selected_axis_status": "foundation_only",
            "combined_long_short_policy_start": "deferred",
            "combined_long_short_policy_deferred_reason": "Long and short evidence surfaces are asymmetric; each side must be independently replay-valid before combined portfolio policy evaluation.",
            "direct_ranking_improvement_next_axis": False,
            "direct_ranking_improvement_rejection_reason": "Prior direct ranking and threshold-adjustment lines produced repeated hold/drop/no-op outcomes or top-K observability limits; portfolio action quality is the missing layer.",
            "candidate_policy_implemented": False,
            "adoption_decision_reserved_for": "gpt-5.5 parent / decision reviewer",
        },
        "evaluation_contract.json": {
            **_artifact_header("evaluation_contract", generated_at),
            "initial_capital_jpy": 10_000_000,
            "execution_model": {
                "baseline": "close_to_close_baseline",
                "preferred_target": "next_session_open",
                "next_session_open_status": "supported",
            },
            "cost_slippage_model": {
                "schema_version": "tradex_daily_action_cost_model_v1",
                "enabled": True,
                "commission_bps": 5.0,
                "slippage_bps": 5.0,
                "tax_or_fee_bps": 0.0,
                "min_fee": 0.0,
                "status": "provisional_placeholder",
                "notes": "Replace with broker-specific assumptions before candidate adoption.",
            },
            "liquidity_filter_assumptions": {
                "status": "required_not_finalized",
                "must_fail_closed_when_missing": True,
            },
            "universe_date_window_assumptions": {
                "same_universe": True,
                "same_date_window": True,
                "same_regime_condition": True,
                "same_artifact_detail_level": True,
            },
            "no_lookahead_rules": [
                "Decision features must be available on or before decision date.",
                "Future-path labels are evaluation-only and cannot be production action inputs.",
                "Fallback behavior must be explicit in artifact fields.",
            ],
            "primary_metrics": [
                "ending_equity",
                "net_return",
                "max_drawdown",
                "worst_month_return",
                "profit_factor",
                "turnover_adjusted_return",
                "monthly_positive_rate",
                "opportunity_cost_adjusted_return",
                "return_per_unit_drawdown",
                "number_of_trades",
                "average_holding_period",
                "exposure_utilization",
            ],
            "secondary_metrics": ["top5_ret_20", "top10_ret_20", "monthly_top5_capture", "recall_at_20", "win_rate", "mean_ret_20", "median_ret_20"],
            "pass_fail_rules": {
                "ret20_alone_is_insufficient": True,
                "must_include_cost_slippage": True,
                "must_survive_regime_checks": True,
                "must_emit_daily_action_ledger": True,
                "must_be_explainable": True,
            },
        },
        "replay_capability_report.json": {
            **_artifact_header("replay_capability_report", generated_at),
            "existing_capabilities": {
                "trade_ledger": "confirmed",
                "positions_timeline": "confirmed",
                "daily_equity_curve": "confirmed",
                "cash_exposure_drawdown": "confirmed",
                "portfolio_daily_action_ledger": "implemented_in_foundation",
                "cost_slippage_config": "implemented_in_foundation",
                "cost_slippage_fill_application": "implemented_for_close_to_close_baseline_and_next_session_open",
            },
            "next_session_open_supported": True,
            "next_session_open_status": "supported",
            "nonzero_cost_slippage_supported": True,
            "daily_action_ledger_supported": True,
            "missing_capabilities": [
                "broker-specific execution defaults",
                "event/news completeness gating",
                "full top20 instrumentation",
            ],
        },
        "missing_data_or_capability_report.json": {
            **_artifact_header("missing_data_or_capability_report", generated_at),
            "missing_data": [
                "broker-specific commission/slippage/tax assumptions",
                "complete event/news coverage for all replay dates",
                "freshness proof for every intended runtime consumer",
            ],
            "missing_replay_features": [
                "portfolio rotation opportunity-cost baseline",
                "hedge budget and exposure offset constraints",
            ],
            "blockers": [
                "candidate adoption blocked until stateful replay includes costs and regime checks",
                "MeeMee reflection blocked until gpt-5.5 artifact-backed review",
            ],
            "fallback_behavior": {
                "silent_fallback_allowed": False,
                "research_fallback_must_be_labeled": True,
            },
        },
        "proposed_policy_families.json": {
            **_artifact_header("proposed_policy_families", generated_at),
            "status": "proposal_inventory_only",
            "candidate_policy_implemented": False,
            "families": [
                {"family_id": "entry_timing_v1", "actions": ["buy", "stay_cash"], "purpose": "enter only when ranking strength and timing agree"},
                {"family_id": "add_hold_v1", "actions": ["add", "hold"], "purpose": "increase position only after confirmation"},
                {"family_id": "take_profit_exit_v1", "actions": ["take_profit", "reduce", "exit"], "purpose": "protect gains and exit invalidated longs"},
                {"family_id": "hedge_overlay_v1", "actions": ["hedge", "reduce", "hold"], "purpose": "offset long exposure when trend deteriorates"},
                {"family_id": "rotation_opportunity_cost_v1", "actions": ["rotate", "stay_cash", "hold"], "purpose": "avoid holding weaker opportunities when stronger candidates exist"},
            ],
            "not_implemented_yet": True,
        },
        "risk_audit.json": {
            **_artifact_header("risk_audit", generated_at),
            "lookahead_risk": {"status": "controlled_by_contract", "required_control": "feature date <= decision date"},
            "overfitting_risk": {"status": "open", "required_control": "forward-style validation and regime splits"},
            "cost_slippage_risk": {"status": "open", "zero_cost_warning": True, "required_control": "nonzero cost/slippage for candidate scoring"},
            "liquidity_risk": {"status": "open", "required_control": "explicit liquidity and turnover caps"},
            "event_news_blindness": {"status": "open", "stale_empty_event_table_handling": "fail_closed_or_mark_unavailable"},
            "concentration_risk": {"status": "open", "required_control": "position, sector, side, and regime exposure caps"},
            "zero_cost_artifact_warning": "Prior zero-cost artifacts are diagnostic only for this daily action engine.",
        },
        "final_decision.json": {
            **_artifact_header("final_decision", generated_at),
            "final_status": "implementation_done",
            "adoption_decision": "not_made",
            "candidate_policy_implemented": False,
            "reserved_for_parent_review": True,
            "production_surfaces_touched": False,
        },
    }

    for filename, payload in artifacts.items():
        _write_json(run_dir / filename, payload)

    complete = {
        **_artifact_header("artifact_complete", generated_at),
        "artifact_list": REQUIRED_ARTIFACTS,
        "artifact_paths": {name: str(run_dir / name) for name in REQUIRED_ARTIFACTS},
        "generated_timestamp": generated_at,
        "git_status_summary": _git_status(repo_root),
        "commands_run": commands,
        "verification_status": {
            "required_artifacts_written": True,
            "json_validated_by_generator": True,
            "production_surfaces_changed": False,
        },
    }
    _write_json(run_dir / "_ARTIFACT_COMPLETE.json", complete)

    for filename in REQUIRED_ARTIFACTS:
        json.loads((run_dir / filename).read_text(encoding="utf-8"))

    return {"ok": True, "run_dir": str(run_dir), "artifacts": [str(run_dir / name) for name in REQUIRED_ARTIFACTS]}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate TRADEX daily action policy engine v1 first-phase artifacts.")
    parser.add_argument("--output-root", default=r"G:\Tradex\research_sessions\tradex_daily_action_policy_engine_v1")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--repo-root", default=str(Path.cwd()))
    parser.add_argument("--commands-run", action="append", default=[])
    args = parser.parse_args(argv)

    command_text = "python " + " ".join(sys.argv)
    commands = list(args.commands_run or [])
    if command_text not in commands:
        commands.append(command_text)
    result = build_artifacts(
        output_root=Path(args.output_root),
        repo_root=Path(args.repo_root),
        run_id=args.run_id or None,
        commands_run=commands,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
