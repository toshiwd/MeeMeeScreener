from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_OUTPUT_ROOT = Path("G:/Tradex/scene_family_readiness_inventory_v1")


SCENE_SOURCES = {
    "a_phase_downtrend": [
        Path("G:/Tradex/a_phase_anti_long_high_hold_oos_v1/20260521T185715Z-a_phase_anti_long_high_hold_oos_v1/compare.json"),
        Path("G:/Tradex/short_scene_visual_a_phase_candidate_contract_v1/20260521T183415Z-short_scene_visual_a_phase_candidate_contract_v1/short_scene_visual_a_phase_candidate_contract.json"),
        Path("G:/Tradex/short_scene_visual_a_phase_live_shadow_watch_v1/20260521T190036Z-short_scene_visual_a_phase_live_shadow_watch_v1/live_shadow_candidate_summary.json"),
    ],
    "b_phase_sideways": [
        Path("G:/Tradex/b_phase_signal_probe_v1/20260521T134647Z-b_phase_signal_probe_v1/b_phase_signal_probe_compare.json"),
        Path("G:/Tradex/market_scene_signal_probe_v1/20260521T141909Z-market_scene_signal_probe_v1/market_scene_signal_probe_compare.json"),
    ],
    "c_phase_uptrend": [
        Path("G:/Tradex/market_scene_signal_probe_v1/20260521T141909Z-market_scene_signal_probe_v1/market_scene_signal_probe_compare.json"),
        Path("G:/Tradex/market_scene_visual_proxy_probe_v1/20260521T143319Z-market_scene_visual_proxy_probe_v1/market_scene_visual_proxy_probe_compare.json"),
    ],
    "crash_or_bottoming": [
        Path("G:/Tradex/crash_bottoming_signal_probe_v1/20260521T131806Z-crash_bottoming_signal_probe_v1/crash_bottoming_signal_probe_compare.json"),
        Path("G:/Tradex/market_scene_signal_probe_v1/20260521T141909Z-market_scene_signal_probe_v1/market_scene_signal_probe_compare.json"),
    ],
}


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _load_json_if_exists(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _artifact_complete_for(path: Path) -> bool:
    complete = path.parent / "_ARTIFACT_COMPLETE.json"
    if not complete.exists():
        return False
    try:
        payload = json.loads(complete.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return False
    return payload.get("complete") is True


def _source_summary(path: Path) -> dict[str, Any]:
    payload = _load_json_if_exists(path)
    if payload is None:
        return {
            "path": str(path),
            "exists": False,
            "artifact_complete": False,
            "decision": None,
            "reason_type": "missing_source_artifact",
        }
    return {
        "path": str(path),
        "exists": True,
        "artifact_complete": _artifact_complete_for(path),
        "schema_version": payload.get("schema_version"),
        "decision": payload.get("authoritative_rollup_decision") or payload.get("decision"),
        "reason_type": payload.get("reason_type"),
        "candidate_generation_challenger_created": payload.get("candidate_generation_challenger_created"),
        "paper_replay_ready": payload.get("paper_replay_ready"),
        "meemee_reflectable": payload.get("meemee_reflectable"),
        "coverage": payload.get("coverage"),
        "observed_branching": payload.get("observed_branching"),
    }


def _scene_decision(scene_id: str, sources: list[dict[str, Any]]) -> dict[str, Any]:
    decisions = {source.get("decision") for source in sources if source.get("exists")}
    complete = all(source.get("artifact_complete") for source in sources if source.get("exists"))
    if scene_id == "a_phase_downtrend":
        ready = (
            "keep" in decisions
            and "candidate_generation_contract_ready" in decisions
            and any(source.get("decision") == "hold_no_live_candidate" for source in sources)
            and complete
        )
        return {
            "readiness": "shadow_candidate_generation_ready",
            "judgment": "keep" if ready else "hold",
            "reason_type": "oos_keep_contract_ready_live_shadow_guarded" if ready else "a_phase_sources_incomplete",
            "meemee_reflectable": False,
        }
    if any(decision == "keep" for decision in decisions):
        return {
            "readiness": "research_keep_needs_contract",
            "judgment": "hold",
            "reason_type": "keep_exists_but_no_candidate_contract_or_live_shadow",
            "meemee_reflectable": False,
        }
    if any(decision == "hold" for decision in decisions):
        return {
            "readiness": "probe_only",
            "judgment": "hold",
            "reason_type": "scene_has_hold_probe_but_no_effective_candidate_generation",
            "meemee_reflectable": False,
        }
    return {
        "readiness": "not_ready",
        "judgment": "drop",
        "reason_type": "no_supporting_keep_or_hold_artifact",
        "meemee_reflectable": False,
    }


def _next_axis(scene_rows: dict[str, dict[str, Any]]) -> dict[str, Any]:
    for scene_id in ("b_phase_sideways", "c_phase_uptrend", "crash_or_bottoming"):
        if scene_rows[scene_id]["decision"]["readiness"] == "probe_only":
            return {
                "selected_next_axis": scene_id,
                "reason_type": "has_probe_artifacts_but_no_candidate_contract_or_live_shadow",
                "explicitly_not_selected": [
                    key for key in scene_rows if key != scene_id
                ],
            }
    return {
        "selected_next_axis": None,
        "reason_type": "no_probe_only_scene_available",
        "explicitly_not_selected": list(scene_rows),
    }


def run_inventory(*, output_root: Path) -> dict[str, Any]:
    output_dir = output_root / f"{_now_tag()}-scene_family_readiness_inventory_v1"
    output_dir.mkdir(parents=True, exist_ok=True)
    scene_rows: dict[str, dict[str, Any]] = {}
    for scene_id, paths in SCENE_SOURCES.items():
        sources = [_source_summary(path) for path in paths]
        scene_rows[scene_id] = {
            "scene_id": scene_id,
            "sources": sources,
            "decision": _scene_decision(scene_id, sources),
        }
    result = {
        "schema_version": "tradex_scene_family_readiness_inventory_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "authoritative_result": True,
        "research_phase": "comparison_stabilization",
        "fixed_evaluation_conditions": {
            "purpose": "inventory only; no replay rerun and no scoring change",
            "source_artifacts": "latest known JSON artifacts for A/B/C/crash scene families",
        },
        "scope": {
            "tradex_only": True,
            "meemee_ranking_changed": False,
            "meemee_ui_changed": False,
            "runtime_db_written": False,
            "silent_fallback_used": False,
            "research_fallback_used": False,
        },
        "scene_families": scene_rows,
        "next_axis_decision": _next_axis(scene_rows),
        "authoritative_rollup_decision": "hold",
        "reason_type": "a_phase_ready_but_full_scene_goal_requires_next_family_validation",
        "meemee_reflectable": False,
        "remaining_risks": [
            "B/C/crash families have not reached candidate contract readiness",
            "inventory reads fixed artifact paths and does not rerun probes",
            "A phase is shadow-ready but still not MeeMee-reflectable",
        ],
    }
    inventory_path = output_dir / "scene_family_readiness_inventory.json"
    decision_path = output_dir / "next_axis_decision.json"
    complete_path = output_dir / "_ARTIFACT_COMPLETE.json"
    result["artifacts"] = {
        "output_dir": str(output_dir),
        "inventory_json": str(inventory_path),
        "next_axis_decision_json": str(decision_path),
        "artifact_complete": str(complete_path),
    }
    _write_json(inventory_path, result)
    _write_json(decision_path, result["next_axis_decision"])
    _write_json(complete_path, {"complete": True, **result["artifacts"]})
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    result = run_inventory(output_root=args.output_root)
    print(json.dumps({"decision": result["authoritative_rollup_decision"], "next_axis": result["next_axis_decision"], "artifacts": result["artifacts"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
