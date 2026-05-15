from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.services.tradex_readonly_reflection_service import (  # noqa: E402
    CLEAN_CANDIDATE,
    OLD_LOOKAHEAD_CANDIDATE,
    build_readonly_reflection_snapshot,
)


DEFAULT_DROP_ROOT = Path(
    r"G:\Tradex\sell_failed_followthrough_multiyear_portfolio_replay_v1"
    r"\20260515T051628Z-sell-failed-followthrough-multiyear-portfolio-replay-v1"
)
DEFAULT_READONLY_ROOT = Path(
    r"G:\Tradex\sell_failed_followthrough_meemee_readonly_reflection_v1"
    r"\20260515T040337Z-sell-failed-followthrough-meemee-readonly-reflection-v1"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\sell_failed_followthrough_family_freeze_v1")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _load_json(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _write_readme(path: Path, decision: dict[str, Any]) -> None:
    lines = [
        "# sell failed-followthrough family freeze v1",
        "",
        "Authoritative JSON artifacts in this directory are the source of truth.",
        f"candidate_name: {decision['candidate_name']}",
        f"decision: {decision['decision']}",
        f"family_status: {decision['family_status']}",
        f"meemee_readonly_status: {decision['meemee_readonly_status']}",
        "production_ranking_changed: false",
        "active_champion_changed: false",
        "publish_run: false",
        "live_sell_signal_added: false",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _exit_summary(exit_comparison: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in exit_comparison.get("variants") or []:
        challenger = row.get("challenger") or {}
        out.append(
            {
                "exit_variant": row.get("exit_variant"),
                "total_return": challenger.get("total_return"),
                "max_drawdown": challenger.get("max_drawdown"),
                "severe_loser_count": challenger.get("severe_loser_count"),
                "bad_pick_count": challenger.get("bad_pick_count"),
                "number_of_trades": challenger.get("number_of_trades"),
            }
        )
    return out


def build_family_freeze_decision(drop_root: Path, readonly_root: Path) -> dict[str, Any]:
    final = _load_json(drop_root / "final_shadow_trade_decision.json")
    availability = _load_json(drop_root / "data_availability_report.json")
    exit_comparison = _load_json(drop_root / "exit_variant_comparison.json")
    added_bad_pick = _load_json(drop_root / "added_bad_pick_decomposition.json")
    severe = _load_json(drop_root / "severe_loser_audit.json")
    return {
        "schema_version": "sell_failed_followthrough_family_freeze_decision_v1",
        "generated_at": _utc_now(),
        "candidate_name": CLEAN_CANDIDATE,
        "family_status": "dropped",
        "decision": "drop_after_multiyear_replay",
        "shadow_trade_candidate": False,
        "meemee_readonly_status": "dropped_after_multiyear_replay",
        "production_eligible": False,
        "active_ranking_eligible": False,
        "live_signal_eligible": False,
        "source_decision": final.get("decision"),
        "source_shadow_trade_candidate": final.get("shadow_trade_candidate"),
        "authoritative_drop_root": str(drop_root),
        "supersedes_readonly_reflection_root": str(readonly_root),
        "supersession_reason": "multi_year_portfolio_replay_failed",
        "drop_reason": "all_fixed_exit_variants_failed_capital_curve_gates",
        "data_period": availability.get("actual_period"),
        "full_period_result_summary": _exit_summary(exit_comparison),
        "added_bad_pick_impact": added_bad_pick.get("added_bad_pick_impact"),
        "multiyear_added_bad_pick_count": added_bad_pick.get("multiyear_added_bad_pick_count"),
        "severe_loser_variant_counts": severe.get("variant_counts"),
        "blockers": final.get("blockers") or [],
        "production_ranking_changed": False,
        "active_champion_changed": False,
        "active_ranking_changed": False,
        "publish_run": False,
        "live_sell_signal_added": False,
        "silent_fallback_used": False,
        "research_fallback": False,
    }


def build_readonly_supersession(decision: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "sell_failed_followthrough_readonly_reflection_supersession_v1",
        "generated_at": _utc_now(),
        "candidate_name": CLEAN_CANDIDATE,
        "previous_readonly_state_historically_true": True,
        "previous_readonly_positive_state_superseded": True,
        "supersedes_readonly_reflection_root": decision["supersedes_readonly_reflection_root"],
        "authoritative_drop_root": decision["authoritative_drop_root"],
        "supersession_reason": decision["supersession_reason"],
        "new_meemee_readonly_status": decision["meemee_readonly_status"],
        "do_not_delete_prior_artifacts": True,
    }


def build_meemee_drop_status_manifest(decision: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "sell_failed_followthrough_meemee_drop_status_manifest_v1",
        "generated_at": _utc_now(),
        "candidate_name": CLEAN_CANDIDATE,
        "candidate_version": "v1",
        "family_status": "dropped",
        "decision": "drop_after_multiyear_replay",
        "side": "sell",
        "display_level": "read_only_research_candidate",
        "meemee_readonly_status": "dropped_after_multiyear_replay",
        "shadow_trade_candidate": False,
        "no_lookahead_pass": True,
        "production_ranking_changed": False,
        "active_ranking_changed": False,
        "active_champion_changed": False,
        "publish_run": False,
        "live_sell_signal_added": False,
        "old_candidate_status": "lookahead_contaminated_excluded",
        "source_run_root": decision["authoritative_drop_root"],
        "source_decision_artifact": str(Path(decision["authoritative_drop_root"]) / "final_shadow_trade_decision.json"),
        "source_compare_artifact": str(Path(decision["authoritative_drop_root"]) / "exit_variant_comparison.json"),
        "source_contract_artifact": str(Path(decision["authoritative_drop_root"]) / "replay_contract.json"),
        "reflectability_decision_artifact": str(Path(decision["authoritative_drop_root"]) / "final_shadow_trade_decision.json"),
        "supersedes_readonly_reflection_root": decision["supersedes_readonly_reflection_root"],
        "authoritative_drop_root": decision["authoritative_drop_root"],
        "supersession_reason": decision["supersession_reason"],
        "drop_reason": decision["drop_reason"],
        "visible_warning": "Not shadow trade eligible. Not active ranking. Not a live sell signal.",
        "allowed_meemee_usage": [
            "show candidate name",
            "show dropped after multi-year portfolio replay",
            "show final drop decision",
            "show full-period result summary",
            "show max drawdown summary",
            "show added_bad_pick impact",
            "show not active ranking",
        ],
        "forbidden_meemee_usage": [
            "do not use for production ranking",
            "do not generate live sell orders",
            "do not override champion",
            "do not mix with provisional intraday data",
            "do not show as confirmed active signal",
            "do not show as positive reflectable candidate",
            f"do not use old lookahead-contaminated candidate {OLD_LOOKAHEAD_CANDIDATE}",
        ],
        "key_metrics": {
            "shadow_trade_candidate": False,
            "multiyear_added_bad_pick_count": decision.get("multiyear_added_bad_pick_count"),
            "fixed_20d_added_bad_pick_pnl": (decision.get("added_bad_pick_impact") or {}).get("fixed_horizon_20d_added_bad_pick_pnl"),
            "stop5_added_bad_pick_pnl": (decision.get("added_bad_pick_impact") or {}).get("stop5_added_bad_pick_pnl"),
        },
        "full_period_result_summary": decision.get("full_period_result_summary") or [],
        "max_drawdown_summary": [
            {"exit_variant": row.get("exit_variant"), "max_drawdown": row.get("max_drawdown")}
            for row in decision.get("full_period_result_summary") or []
        ],
        "added_bad_pick_impact": decision.get("added_bad_pick_impact") or {},
        "remaining_risks": [
            "Candidate failed multi-year portfolio replay and is not shadow-trade eligible.",
            "Previous read-only positive state is superseded, not deleted.",
            "p_down, ev20_net, and short_score upstream provenance remains unaudited and is not reopened in this freeze task.",
        ],
    }


def build_guardrail_check(manifest: dict[str, Any]) -> dict[str, Any]:
    checks = {
        "family_status_dropped": manifest.get("family_status") == "dropped",
        "decision_drop_after_multiyear_replay": manifest.get("decision") == "drop_after_multiyear_replay",
        "shadow_trade_candidate_false": manifest.get("shadow_trade_candidate") is False,
        "production_ranking_changed_false": manifest.get("production_ranking_changed") is False,
        "active_champion_changed_false": manifest.get("active_champion_changed") is False,
        "publish_run_false": manifest.get("publish_run") is False,
        "live_sell_signal_added_false": manifest.get("live_sell_signal_added") is False,
        "old_candidate_excluded": manifest.get("old_candidate_status") == "lookahead_contaminated_excluded",
        "not_positive_candidate": manifest.get("meemee_readonly_status") == "dropped_after_multiyear_replay",
    }
    return {
        "schema_version": "sell_failed_followthrough_family_freeze_guardrail_check_v1",
        "generated_at": _utc_now(),
        "candidate_name": CLEAN_CANDIDATE,
        "checks": checks,
        "guardrail_pass": all(checks.values()),
        "production_ranking_changed": False,
        "active_champion_changed": False,
        "active_ranking_changed": False,
        "publish_run": False,
        "live_sell_signal_added": False,
        "old_lookahead_contaminated_candidate_excluded": True,
    }


def build_final_decision(guardrail: dict[str, Any]) -> dict[str, Any]:
    blockers: list[str] = []
    if not guardrail.get("guardrail_pass"):
        blockers.append("guardrail_failed")
    decision = "family_frozen_drop_status_reflected" if not blockers else "hold_for_drop_status_integration"
    return {
        "schema_version": "sell_failed_followthrough_family_freeze_final_decision_v1",
        "generated_at": _utc_now(),
        "candidate_name": CLEAN_CANDIDATE,
        "decision": decision,
        "family_status": "dropped",
        "meemee_readonly_status": "dropped_after_multiyear_replay",
        "shadow_trade_candidate": False,
        "production_ranking_changed": False,
        "active_champion_changed": False,
        "active_ranking_changed": False,
        "publish_run": False,
        "live_sell_signal_added": False,
        "blockers": blockers,
        "next_allowed_action": "end_family_and_move_to_next_axis" if not blockers else "repair_drop_status_integration",
    }


def run(
    *,
    drop_root: str | Path = DEFAULT_DROP_ROOT,
    readonly_root: str | Path = DEFAULT_READONLY_ROOT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
) -> dict[str, Any]:
    drop_path = Path(drop_root).expanduser().resolve(strict=False)
    readonly_path = Path(readonly_root).expanduser().resolve(strict=False)
    run_dir = Path(output_root).expanduser().resolve(strict=False) / f"{_utc_stamp()}-sell-failed-followthrough-family-freeze-v1"
    freeze = build_family_freeze_decision(drop_path, readonly_path)
    supersession = build_readonly_supersession(freeze)
    manifest = build_meemee_drop_status_manifest(freeze)
    guardrail = build_guardrail_check(manifest)
    final = build_final_decision(guardrail)
    paths = {
        "family_freeze_decision": run_dir / "family_freeze_decision.json",
        "readonly_reflection_supersession": run_dir / "readonly_reflection_supersession.json",
        "meemee_drop_status_manifest": run_dir / "meemee_drop_status_manifest.json",
        "guardrail_check": run_dir / "guardrail_check.json",
        "README": run_dir / "README.md",
        "complete": run_dir / "_ARTIFACT_COMPLETE.json",
    }
    _write_json(paths["family_freeze_decision"], freeze)
    _write_json(paths["readonly_reflection_supersession"], supersession)
    _write_json(paths["meemee_drop_status_manifest"], manifest)
    _write_json(paths["guardrail_check"], guardrail)
    _write_readme(paths["README"], final)
    _write_json(
        paths["complete"],
        {
            "schema_version": "sell_failed_followthrough_family_freeze_complete_v1",
            "generated_at": _utc_now(),
            "artifact_complete": True,
            "status": "complete",
            "candidate_name": CLEAN_CANDIDATE,
            "decision": final["decision"],
            "family_status": final["family_status"],
            "meemee_readonly_status": final["meemee_readonly_status"],
            "artifact_refs": {key: str(path) for key, path in paths.items() if key != "complete"},
            "authoritative_decision": str(paths["family_freeze_decision"]),
            "readonly_snapshot_after_freeze": build_readonly_reflection_snapshot(run_dir, strict=True),
            "production_ranking_changed": False,
            "active_champion_changed": False,
            "active_ranking_changed": False,
            "publish_run": False,
            "live_sell_signal_added": False,
        },
    )
    return {
        "ok": True,
        "output_dir": str(run_dir),
        "decision": final["decision"],
        "family_status": final["family_status"],
        "meemee_readonly_status": final["meemee_readonly_status"],
        "artifact_refs": {key: str(path) for key, path in paths.items()},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Freeze/drop sell failed-followthrough family after multiyear replay.")
    parser.add_argument("--drop-root", default=str(DEFAULT_DROP_ROOT))
    parser.add_argument("--readonly-root", default=str(DEFAULT_READONLY_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args()
    result = run(drop_root=args.drop_root, readonly_root=args.readonly_root, output_root=args.output_root)
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
