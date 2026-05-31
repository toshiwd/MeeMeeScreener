from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.debug.analyze_detail_chart_screenshot import analyze


DEFAULT_OUTPUT_ROOT = Path("G:/Tradex/short_scene_visual_screenshot_gate_v1")
SHORT_ALLOW = {"short_probe_candidate", "watch_short"}
LONG_ALLOW = {"probe_candidate", "watch_or_probe_small"}


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _side_review(analysis: dict[str, Any], side: str) -> dict[str, Any]:
    key = "short_visual_review" if side == "sell" else "long_visual_review"
    review = analysis.get(key)
    return review if isinstance(review, dict) else {}


def _gate_candidate(candidate: dict[str, Any], screenshot_dir: Path) -> dict[str, Any]:
    code = str(candidate.get("code") or "")
    side = str(candidate.get("side") or "")
    screenshot_path = screenshot_dir / f"detail_{code}.png"
    if not code or not screenshot_path.exists():
        return {
            "code": code,
            "side": side,
            "screenshot_path": str(screenshot_path),
            "screenshot_confirmed": False,
            "screenshot_gate": "blocked",
            "reason_type": "screenshot_missing",
        }
    analysis = analyze(screenshot_path)
    review = _side_review(analysis, side)
    decision = str(review.get("decision") or "")
    allowed = decision in (SHORT_ALLOW if side == "sell" else LONG_ALLOW)
    if not analysis.get("confirmed"):
        gate = "blocked"
        reason_type = "screenshot_analysis_unconfirmed"
    elif allowed:
        gate = "pass"
        reason_type = "screenshot_side_review_allows_shadow_candidate"
    else:
        gate = "reject"
        reason_type = "screenshot_side_review_rejects_shadow_candidate"
    return {
        "code": code,
        "name": candidate.get("name"),
        "dt": candidate.get("dt"),
        "side": side,
        "screenshot_path": str(screenshot_path),
        "screenshot_confirmed": bool(analysis.get("confirmed")),
        "candle_pixel_count": analysis.get("candle_pixel_count"),
        "latest_price_position_pct": analysis.get("latest_price_position_pct"),
        "trend_slope_pct": analysis.get("trend_slope_pct"),
        "long_visual_review": analysis.get("long_visual_review"),
        "short_visual_review": analysis.get("short_visual_review"),
        "candidate_side_review": review,
        "screenshot_gate": gate,
        "reason_type": reason_type,
    }


def _rollup(gated: list[dict[str, Any]]) -> dict[str, Any]:
    if not gated:
        return {
            "judgment": "hold_no_shadow_candidate",
            "reason_type": "no_shadow_candidate_to_gate",
            "paper_replay_ready": True,
        }
    passed = [row for row in gated if row.get("screenshot_gate") == "pass"]
    blocked = [row for row in gated if row.get("screenshot_gate") == "blocked"]
    if passed:
        return {
            "judgment": "continue_live_shadow_screenshot_confirmed",
            "reason_type": "at_least_one_shadow_candidate_passed_screenshot_gate",
            "paper_replay_ready": True,
        }
    if blocked:
        return {
            "judgment": "hold",
            "reason_type": "screenshot_gate_blocked_by_missing_or_unconfirmed_image",
            "paper_replay_ready": False,
        }
    return {
        "judgment": "hold_screenshot_rejected",
        "reason_type": "all_shadow_candidates_rejected_by_screenshot_side_review",
        "paper_replay_ready": True,
    }


def run_gate(*, shadow_summary: Path, screenshot_dir: Path, output_root: Path) -> dict[str, Any]:
    output_dir = output_root / f"{_now_tag()}-short_scene_visual_screenshot_gate_v1"
    output_dir.mkdir(parents=True, exist_ok=True)
    summary = _load_json(shadow_summary)
    selected = summary.get("selected_shadow_candidates") or []
    if not isinstance(selected, list):
        selected = []
    gated = [_gate_candidate(candidate, screenshot_dir) for candidate in selected if isinstance(candidate, dict)]
    rollup = _rollup(gated)
    result = {
        "schema_version": "tradex_short_scene_visual_screenshot_gate_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "authoritative_result": True,
        "research_phase": "infrastructure_stabilization",
        "source_shadow_summary_json": str(shadow_summary),
        "screenshot_dir": str(screenshot_dir),
        "fixed_evaluation_conditions": {
            "input_candidates": "selected_shadow_candidates from live shadow watch",
            "screenshot_pattern": "detail_<code>.png",
            "side_review_gate": {
                "sell_allow": sorted(SHORT_ALLOW),
                "buy_allow": sorted(LONG_ALLOW),
            },
            "forward_outcome_available": False,
        },
        "scope": {
            "tradex_only": True,
            "meemee_ranking_changed": False,
            "meemee_ui_changed": False,
            "runtime_db_written": False,
            "silent_fallback_used": False,
            "research_fallback_used": False,
        },
        "coverage": {
            "input_shadow_candidate_count": len(selected),
            "screenshot_gated_candidate_count": len(gated),
            "screenshot_pass_count": sum(1 for row in gated if row.get("screenshot_gate") == "pass"),
            "screenshot_reject_count": sum(1 for row in gated if row.get("screenshot_gate") == "reject"),
            "screenshot_blocked_count": sum(1 for row in gated if row.get("screenshot_gate") == "blocked"),
        },
        "gated_candidates": gated,
        "observed_branching": {
            "changed_top5_members_count": sum(1 for row in gated if row.get("screenshot_gate") == "pass"),
            "changed_top10_members_count": sum(1 for row in gated if row.get("screenshot_gate") == "pass"),
            "changed_rank_count": sum(1 for row in gated if row.get("screenshot_gate") == "pass"),
            "selection_divergence_reason": "screenshot side-review gate filters shadow candidates after OHLC proxy selection",
        },
        "authoritative_rollup_decision": rollup["judgment"],
        "reason_type": rollup["reason_type"],
        "paper_replay_ready": rollup["paper_replay_ready"],
        "meemee_reflectable": False,
        "remaining_risks": [
            "screenshot gate is current-state visual confirmation, not forward return evidence",
            "Pillow pixel heuristic may misread complex overlays or multi-panel layouts",
            "screenshot capture is operationally dependent on local MeeMee browser route availability",
        ],
    }
    gate_path = output_dir / "screenshot_gate_result.json"
    decision_path = output_dir / "screenshot_gate_decision.json"
    complete_path = output_dir / "_ARTIFACT_COMPLETE.json"
    result["artifacts"] = {
        "output_dir": str(output_dir),
        "gate_json": str(gate_path),
        "decision_json": str(decision_path),
        "artifact_complete": str(complete_path),
    }
    _write_json(gate_path, result)
    _write_json(
        decision_path,
        {
            "schema_version": result["schema_version"],
            "authoritative_rollup_decision": result["authoritative_rollup_decision"],
            "reason_type": result["reason_type"],
            "coverage": result["coverage"],
            "gated_candidates": result["gated_candidates"],
            "paper_replay_ready": result["paper_replay_ready"],
            "meemee_reflectable": result["meemee_reflectable"],
            "artifacts": result["artifacts"],
        },
    )
    _write_json(complete_path, {"complete": True, **result["artifacts"]})
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shadow-summary", type=Path, required=True)
    parser.add_argument("--screenshot-dir", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    result = run_gate(
        shadow_summary=args.shadow_summary,
        screenshot_dir=args.screenshot_dir,
        output_root=args.output_root,
    )
    print(
        json.dumps(
            {
                "decision": result["authoritative_rollup_decision"],
                "reason_type": result["reason_type"],
                "coverage": result["coverage"],
                "gated_candidates": result["gated_candidates"],
                "artifacts": result["artifacts"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
