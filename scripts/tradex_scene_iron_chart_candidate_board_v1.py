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

from scripts.tradex_short_scene_visual_candidate_gap_v1 import _write_json


DEFAULT_OUTPUT_ROOT = Path("G:/Tradex/scene_iron_chart_candidate_board_v1")

ARTIFACTS = {
    "a_phase_downtrend_100ma_rejection": Path(
        "G:/Tradex/a_phase_anti_long_high_hold_oos_v1/20260521T185715Z-a_phase_anti_long_high_hold_oos_v1/compare.json"
    ),
    "b_phase_short_breakdown": Path(
        "G:/Tradex/b_phase_short_breakdown_additive_oos_v1/20260521T191156Z-b_phase_short_breakdown_additive_oos_v1/compare.json"
    ),
    "c_phase_20ma_touch_bounce": Path(
        "G:/Tradex/c_phase_long_touch_bounce_guarded_oos_v1/20260521T230154Z-c_phase_long_touch_bounce_guarded_oos_v1/compare.json"
    ),
    "crash_bottom_20ma_reclaim_long": Path(
        "G:/Tradex/crash_bottom_long_reclaim_additive_oos_v1/20260521T225733Z-crash_bottom_long_reclaim_additive_oos_v1/compare.json"
    ),
    "two_ma_simultaneous_breakout_daily": Path(
        "G:/Tradex/two_ma_simultaneous_breakout_long_oos_v1/20260522T003229Z-two_ma_simultaneous_breakout_long_oos_v1/compare.json"
    ),
    "two_ma_simultaneous_breakout_weekly": Path(
        "G:/Tradex/two_ma_simultaneous_breakout_weekly_long_oos_v1/20260522T012540Z-two_ma_simultaneous_breakout_weekly_long_oos_v1/compare.json"
    ),
}


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _metric_summary(data: dict[str, Any]) -> dict[str, Any]:
    compare = data.get("compare") or {}
    oos = compare.get("oos") or compare
    top5 = (oos.get("top5") or {})
    top10 = (oos.get("top10") or {})
    return {
        "authoritative_rollup_decision": data.get("authoritative_rollup_decision"),
        "reason_type": data.get("reason_type"),
        "meemee_reflectable": data.get("meemee_reflectable"),
        "coverage": data.get("coverage"),
        "observed_branching": data.get("observed_branching"),
        "top5_delta": top5.get("additive_delta"),
        "top10_delta": top10.get("additive_delta"),
        "artifact": str(data.get("artifacts", {}).get("compare_json") or ""),
    }


def build_board(*, output_root: Path) -> dict[str, Any]:
    output_dir = output_root / f"{_now_tag()}-scene_iron_chart_candidate_board_v1"
    output_dir.mkdir(parents=True, exist_ok=True)
    loaded = {name: _read(path) for name, path in ARTIFACTS.items() if path.exists()}
    missing = {name: str(path) for name, path in ARTIFACTS.items() if not path.exists()}
    candidates = {
        "a_phase": {
            "current_state": "keep",
            "best_candidate": "a_phase_downtrend_100ma_rejection plus anti_long_high_hold_gate",
            "evidence": _metric_summary(loaded["a_phase_downtrend_100ma_rejection"]) if "a_phase_downtrend_100ma_rejection" in loaded else None,
            "iron_chart_status": "iron_candidate_shadow_ready",
        },
        "b_phase": {
            "current_state": "drop_first_axis",
            "best_candidate": "b_phase_short_breakdown",
            "evidence": _metric_summary(loaded["b_phase_short_breakdown"]) if "b_phase_short_breakdown" in loaded else None,
            "iron_chart_status": "not_found",
        },
        "c_phase": {
            "current_state": "drop_first_axis",
            "best_candidate": "c_phase_20ma_touch_bounce_guarded",
            "evidence": _metric_summary(loaded["c_phase_20ma_touch_bounce"]) if "c_phase_20ma_touch_bounce" in loaded else None,
            "iron_chart_status": "not_found",
        },
        "crash_or_bottoming": {
            "current_state": "drop_first_long_axis",
            "best_candidate": "crash_bottom_20ma_reclaim_long",
            "evidence": _metric_summary(loaded["crash_bottom_20ma_reclaim_long"]) if "crash_bottom_20ma_reclaim_long" in loaded else None,
            "iron_chart_status": "not_found_for_long",
        },
        "two_ma_simultaneous_breakout": {
            "current_state": "daily_drop_weekly_drop_but_weekly_safer",
            "best_candidate": "weekly_two_ma_simultaneous_breakout",
            "daily_evidence": _metric_summary(loaded["two_ma_simultaneous_breakout_daily"]) if "two_ma_simultaneous_breakout_daily" in loaded else None,
            "weekly_evidence": _metric_summary(loaded["two_ma_simultaneous_breakout_weekly"]) if "two_ma_simultaneous_breakout_weekly" in loaded else None,
            "iron_chart_status": "hold_for_diagnostic_only_not_candidate_generation_keep",
        },
    }
    result = {
        "schema_version": "tradex_scene_iron_chart_candidate_board_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "authoritative_result": True,
        "research_phase": "branching_generation",
        "scope": {
            "tradex_only": True,
            "meemee_ranking_changed": False,
            "meemee_ui_changed": False,
            "runtime_db_written": False,
            "silent_fallback_used": False,
            "research_fallback_used": False,
        },
        "candidate_board": candidates,
        "missing_artifacts": missing,
        "next_recommended_axis": {
            "axis": "crash_or_distribution_short_pullback_probe_candidate",
            "reason_type": "remaining_crash_family_short_side_not_yet_topk_validated_and_event_probe_has_positive_short_edge",
            "do_not_change": [
                "do not rescue dropped B/C/two-ma long axes in the same step",
                "do not reflect into MeeMee",
                "do not change champion scoring",
            ],
        },
        "artifacts": {
            "output_dir": str(output_dir),
            "board_json": str(output_dir / "scene_iron_chart_candidate_board.json"),
            "artifact_complete": str(output_dir / "_ARTIFACT_COMPLETE.json"),
        },
        "remaining_risks": [
            "only A phase has a keep result strong enough for shadow candidate generation",
            "weekly two-MA pattern is safer than daily but still not an effective additive top5 challenger",
            "crash family short-side candidate remains unvalidated under topK candidate-generation conditions",
        ],
    }
    _write_json(output_dir / "scene_iron_chart_candidate_board.json", result)
    _write_json(output_dir / "_ARTIFACT_COMPLETE.json", {"complete": True, **result["artifacts"]})
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    result = build_board(output_root=args.output_root)
    print(json.dumps(result["artifacts"], ensure_ascii=False, indent=2))
    print(json.dumps({"next_recommended_axis": result["next_recommended_axis"], "missing_artifacts": result["missing_artifacts"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
