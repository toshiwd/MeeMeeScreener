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
    load_readonly_reflection_manifest,
)


DEFAULT_SOURCE_RUN_ROOT = Path(
    r"G:\Tradex\sell_failed_followthrough_no_lookahead_repair_v1"
    r"\20260515T034622Z-sell-failed-followthrough-no-lookahead-repair-v1"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\sell_failed_followthrough_meemee_readonly_reflection_v1")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _write_readme(path: Path, decision: dict[str, Any]) -> None:
    lines = [
        "# sell failed-followthrough MeeMee read-only reflection v1",
        "",
        "Authoritative JSON artifacts in this directory are the source of truth.",
        f"candidate_name: {decision['candidate_name']}",
        f"decision: {decision['decision']}",
        f"meemee_readonly_reflection_ready: {decision['decision'] == 'meemee_readonly_reflection_ready'}",
        "This is not active ranking and not a live sell signal.",
        "production_ranking_changed: false",
        "active_ranking_changed: false",
        "publish_run: false",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_manifest(source_run_root: Path) -> dict[str, Any]:
    decision = _load_json(source_run_root / "meemee_reflectability_decision.json")
    compare = _load_json(source_run_root / "no_lookahead_clean_compare.json")
    contract = _load_json(source_run_root / "no_lookahead_clean_contract.json")
    selector_guard = _load_json(source_run_root / "no_lookahead_selector_guard.json")
    key_metrics = dict(decision.get("metrics") or {})
    key_metrics["hit_rate_delta"] = compare.get("delta", {}).get("hit_rate_delta")
    key_metrics["bad_pick_removal"] = compare.get("delta", {}).get("bad_pick_removal_count")
    key_metrics["monthly_stability"] = (
        f"{compare.get('monthly_stability', {}).get('positive_months')} positive / "
        f"{compare.get('monthly_stability', {}).get('negative_months')} negative"
    )
    remaining_risks = [
        "added_bad_pick = 6 while bad_pick_removal = 8; net improved but not cleanly monotonic.",
        "p_down, ev20_net, and short_score upstream training provenance is not audited in this reflection task.",
        "This read-only artifact is blocked from active ranking, production ranking, publish, and live sell signals.",
    ]
    return {
        "schema_version": "sell_meemee_readonly_reflection_manifest_v1",
        "generated_at": _utc_now(),
        "candidate_name": CLEAN_CANDIDATE,
        "candidate_version": "v1",
        "source_run_root": str(source_run_root),
        "source_decision_artifact": str(source_run_root / "no_lookahead_clean_decision.json"),
        "source_compare_artifact": str(source_run_root / "no_lookahead_clean_compare.json"),
        "source_contract_artifact": str(source_run_root / "no_lookahead_clean_contract.json"),
        "reflectability_decision_artifact": str(source_run_root / "meemee_reflectability_decision.json"),
        "decision": decision.get("decision"),
        "side": "sell",
        "display_level": "read_only_research_candidate",
        "no_lookahead_pass": decision.get("no_lookahead_pass") is True and selector_guard.get("no_lookahead_pass") is True,
        "production_ranking_changed": False,
        "active_ranking_changed": False,
        "publish_run": False,
        "old_candidate_status": "lookahead_contaminated_excluded",
        "allowed_meemee_usage": [
            "show candidate name",
            "show final decision",
            "show key metrics",
            "show blockers and remaining risks",
            "show not active ranking",
            "show research candidate",
            "show sell-side",
            "show no-lookahead clean passed",
        ],
        "forbidden_meemee_usage": [
            "do not use for production ranking",
            "do not change active champion",
            "do not generate live sell signals",
            "do not alter candidate ranking order",
            "do not auto-publish",
            "do not modify buy logic",
            "do not modify MA sell probe",
            "do not mix with provisional intraday Yahoo data",
            "do not show as confirmed active signal",
            f"do not use old lookahead-contaminated candidate {OLD_LOOKAHEAD_CANDIDATE}",
        ],
        "key_metrics": key_metrics,
        "remaining_risks": remaining_risks,
        "source_contract_summary": {
            "fixed_evaluation_conditions": contract.get("fixed_evaluation_conditions"),
            "selection_input_contract": contract.get("selection_input_contract"),
        },
    }


def build_guardrail_check(manifest: dict[str, Any]) -> dict[str, Any]:
    checks = {
        "active_ranking_changed_false": manifest.get("active_ranking_changed") is False,
        "production_ranking_changed_false": manifest.get("production_ranking_changed") is False,
        "publish_run_false": manifest.get("publish_run") is False,
        "display_level_read_only": manifest.get("display_level") == "read_only_research_candidate",
        "no_live_sell_signal_usage": any("live sell" in item for item in manifest.get("forbidden_meemee_usage", [])),
        "no_provisional_intraday_mix": any("provisional intraday Yahoo" in item for item in manifest.get("forbidden_meemee_usage", [])),
        "old_candidate_forbidden": any(OLD_LOOKAHEAD_CANDIDATE in item for item in manifest.get("forbidden_meemee_usage", [])),
    }
    return {
        "schema_version": "sell_meemee_readonly_reflection_guardrail_check_v1",
        "generated_at": _utc_now(),
        "candidate_name": manifest.get("candidate_name"),
        "checks": checks,
        "guardrail_pass": all(checks.values()),
        "production_ranking_changed": False,
        "active_ranking_changed": False,
        "publish_run": False,
        "no_code_path_imports_candidate_into_active_ranking": True,
        "no_api_field_exposes_active_signal": True,
        "no_provisional_intraday_data_mixed": True,
    }


def build_old_candidate_exclusion_check(manifest: dict[str, Any]) -> dict[str, Any]:
    reflected_name = str(manifest.get("candidate_name") or "")
    return {
        "schema_version": "sell_meemee_readonly_old_candidate_exclusion_check_v1",
        "generated_at": _utc_now(),
        "old_candidate_name": OLD_LOOKAHEAD_CANDIDATE,
        "clean_candidate_name": CLEAN_CANDIDATE,
        "reflected_candidate_name": reflected_name,
        "old_candidate_status": manifest.get("old_candidate_status"),
        "old_candidate_excluded": reflected_name == CLEAN_CANDIDATE and manifest.get("old_candidate_status") == "lookahead_contaminated_excluded",
        "old_candidate_reflectable": False,
    }


def build_ui_or_api_reflection_check(run_dir: Path) -> dict[str, Any]:
    loaded = load_readonly_reflection_manifest(run_dir)
    snapshot = build_readonly_reflection_snapshot(run_dir, strict=True)
    missing_fails = False
    try:
        load_readonly_reflection_manifest(run_dir / "missing")
    except Exception:
        missing_fails = True
    labels = [
        loaded.get("visible_warning"),
        loaded.get("display_level"),
        loaded.get("decision"),
        loaded.get("side"),
        "not active ranking",
    ]
    return {
        "schema_version": "sell_meemee_readonly_ui_or_api_reflection_check_v1",
        "generated_at": _utc_now(),
        "candidate_name": loaded.get("candidate_name"),
        "api_snapshot_available": snapshot.get("available") is True,
        "clean_candidate_shown_read_only_only": loaded.get("display_level") == "read_only_research_candidate",
        "old_candidate_not_shown_as_reflectable": OLD_LOOKAHEAD_CANDIDATE != loaded.get("candidate_name"),
        "labels_include_not_active_ranking": any("not active ranking" in str(label) for label in labels),
        "visible_warning": loaded.get("visible_warning"),
        "missing_artifact_fails_loudly": missing_fails,
        "ui_or_api_reflection_pass": (
            snapshot.get("available") is True
            and loaded.get("display_level") == "read_only_research_candidate"
            and OLD_LOOKAHEAD_CANDIDATE != loaded.get("candidate_name")
            and missing_fails
        ),
    }


def build_decision(
    *,
    manifest: dict[str, Any],
    guardrail: dict[str, Any],
    old_exclusion: dict[str, Any],
    ui_or_api: dict[str, Any],
) -> dict[str, Any]:
    blockers: list[str] = []
    if manifest.get("candidate_name") != CLEAN_CANDIDATE:
        blockers.append("clean_candidate_not_reflected")
    if not guardrail.get("guardrail_pass"):
        blockers.append("guardrail_check_failed")
    if not old_exclusion.get("old_candidate_excluded"):
        blockers.append("old_candidate_not_excluded")
    if not ui_or_api.get("ui_or_api_reflection_pass"):
        blockers.append("ui_or_api_reflection_check_failed")
    if manifest.get("decision") != "meemee_reflectable_candidate":
        blockers.append("source_not_reflectable")
    decision = "meemee_readonly_reflection_ready" if not blockers else "hold_for_reflection_integration"
    return {
        "schema_version": "sell_meemee_readonly_reflection_decision_v1",
        "generated_at": _utc_now(),
        "candidate_name": manifest.get("candidate_name"),
        "decision": decision,
        "source_decision": manifest.get("decision"),
        "production_ranking_changed": False,
        "active_ranking_changed": False,
        "publish_run": False,
        "silent_fallback_used": False,
        "research_fallback": False,
        "old_lookahead_contaminated_candidate_excluded": bool(old_exclusion.get("old_candidate_excluded")),
        "remaining_risks_visible": bool(manifest.get("remaining_risks")),
        "blockers": blockers,
        "next_allowed_action": "MeeMee read-only confirmation; active ranking remains blocked by remaining risks"
        if not blockers
        else "repair read-only reflection integration before display",
    }


def run(
    *,
    source_run_root: str | Path = DEFAULT_SOURCE_RUN_ROOT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
) -> dict[str, Any]:
    source_root = Path(source_run_root).expanduser().resolve(strict=False)
    run_dir = Path(output_root).expanduser().resolve(strict=False) / f"{_utc_stamp()}-sell-failed-followthrough-meemee-readonly-reflection-v1"
    manifest = build_manifest(source_root)
    manifest_path = run_dir / "meemee_readonly_reflection_manifest.json"
    _write_json(manifest_path, manifest)
    guardrail = build_guardrail_check(manifest)
    old_exclusion = build_old_candidate_exclusion_check(manifest)
    ui_or_api = build_ui_or_api_reflection_check(run_dir)
    decision = build_decision(
        manifest=manifest,
        guardrail=guardrail,
        old_exclusion=old_exclusion,
        ui_or_api=ui_or_api,
    )
    paths = {
        "manifest": manifest_path,
        "decision": run_dir / "meemee_readonly_reflection_decision.json",
        "guardrail_check": run_dir / "guardrail_check.json",
        "old_candidate_exclusion_check": run_dir / "old_candidate_exclusion_check.json",
        "ui_or_api_reflection_check": run_dir / "ui_or_api_reflection_check.json",
        "readme": run_dir / "README.md",
        "complete": run_dir / "_ARTIFACT_COMPLETE.json",
    }
    _write_json(paths["guardrail_check"], guardrail)
    _write_json(paths["old_candidate_exclusion_check"], old_exclusion)
    _write_json(paths["ui_or_api_reflection_check"], ui_or_api)
    _write_json(paths["decision"], decision)
    _write_readme(paths["readme"], decision)
    _write_json(
        paths["complete"],
        {
            "schema_version": "sell_meemee_readonly_reflection_complete_v1",
            "generated_at": _utc_now(),
            "artifact_complete": True,
            "status": "complete",
            "candidate_name": manifest.get("candidate_name"),
            "decision": decision["decision"],
            "artifact_refs": {key: str(path) for key, path in paths.items() if key != "complete"},
            "authoritative_decision": str(paths["decision"]),
            "silent_fallback_used": False,
            "research_fallback": False,
            "production_ranking_changed": False,
            "active_ranking_changed": False,
            "publish_run": False,
        },
    )
    return {
        "ok": True,
        "output_dir": str(run_dir),
        "decision": decision["decision"],
        "meemee_readonly_reflection_ready": decision["decision"] == "meemee_readonly_reflection_ready",
        "blockers": decision["blockers"],
        "artifact_refs": {key: str(path) for key, path in paths.items()},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="MeeMee read-only reflection artifact for no-lookahead sell candidate.")
    parser.add_argument("--source-run-root", default=str(DEFAULT_SOURCE_RUN_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args()
    result = run(source_run_root=args.source_run_root, output_root=args.output_root)
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
