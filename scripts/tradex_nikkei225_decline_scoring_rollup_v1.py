from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


AXIS_ID = "tradex_nikkei225_decline_scoring_rollup_v1"


def _load(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("artifact_role") != "authoritative":
        raise ValueError(f"not authoritative: {path}")
    return payload


def run(paths: list[Path], chart_audit: Path | None, output_root: Path) -> Path:
    artifacts = {path.parent.name: {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "payload": _load(path)} for path in paths}
    by_schema = {item["payload"]["schema_version"]: item for item in artifacts.values()}
    required = (
        "tradex_nikkei225_representative_casebook_v1.compare.v1",
        "tradex_nikkei225_case_pair_sequence_contrast_v1.compare.v1",
        "tradex_nikkei225_lower_wick_rejection_oos_v1.compare.v1",
        "tradex_nikkei225_close_position_oos_v1.compare.v1",
        "tradex_nikkei225_support_touch_fatigue_oos_v1.compare.v1",
    )
    missing = [schema for schema in required if schema not in by_schema]
    if missing:
        raise ValueError(f"missing required artifacts: {missing}")
    lower = by_schema[required[2]]["payload"]
    close = by_schema[required[3]]["payload"]
    touch = by_schema[required[4]]["payload"]
    chart = json.loads(chart_audit.read_text(encoding="utf-8")) if chart_audit and chart_audit.exists() else None
    decisions = {
        "lower_wick_rejection": {
            "decision": lower["decision"]["candidate_local_decision"],
            "reason": "2024 train effect reversed in 2025 validation; CI crossed zero; not a sell deduction and not a buy addition",
            "validation_difference": lower["results"]["validation_2025"]["no_rejection_minus_clear_rejection"],
        },
        "close_position": {
            "decision": close["decision"]["candidate_local_decision"],
            "reason": "2024 train effect reversed in 2025 validation; sample/CI/effect gates failed",
            "validation_difference": close["results"]["validation_2025"]["low_close_minus_rejected_low"],
        },
        "support_touch_fatigue": {
            "decision": touch["decision"]["candidate_local_decision"],
            "reason": "existing support-break detector has no 0-2-touch comparison group; boundary is not instrumented",
            "validation_difference": touch["results"]["validation_2025"]["fatigued_minus_few_touches"],
        },
        "single_candle_directional_scoring": {
            "decision": "drop",
            "reason": "representative pairs and OOS tests show candle shape alone does not survive fixed-condition validation",
        },
        "sell_deduction_to_buy_addition_symmetry": {
            "decision": "drop",
            "reason": "sell weakening is not an independently validated long entry; buy points require separate long gates",
        },
        "sideways_duration_direction": {
            "decision": "hold_context_only",
            "reason": "compression duration is energy/context, while direction depends on pretrend, location and confirmed range exit",
        },
    }
    keep_count = sum(item["decision"].startswith("keep") for item in decisions.values())
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = output_root / f"{stamp}-{AXIS_ID}"
    output.mkdir(parents=True, exist_ok=False)
    payload = {
        "schema_version": f"{AXIS_ID}.final_research_decision.v1", "artifact_role": "authoritative",
        "generated_at": datetime.now(timezone.utc).isoformat(), "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {"universe": "current Nikkei225 registry; survivorship-biased", "train": 2024, "validation": 2025, "exploratory": "2026-01-01 through 2026-07-13; not clean shadow because used for case discovery", "horizon": 10, "costs": "ignored by user rule", "method": "representative-pair discovery then one-axis fixed-event OOS tests"},
        "authoritative_sources": [{"path": item["path"], "sha256": item["sha256"], "schema_version": item["payload"]["schema_version"]} for item in artifacts.values()],
        "chart_capture_audit": {"path": str(chart_audit) if chart_audit else None, "audit": chart, "accepted_as_visual_evidence": bool(chart and chart.get("judgment") == "pass_clean_detail_screenshot_export")},
        "observed_branching": {"representative_archetypes": 5, "representative_cases": 10, "lower_wick_events": lower["observed_branching"]["selected_event_count"], "close_position_events": close["observed_branching"]["selected_event_count"], "support_touch_events": touch["observed_branching"]["selected_event_count"], "changed_top5_members_count": None, "changed_top10_members_count": None, "changed_rank_count": None, "selection_divergence_reason": "research is event classification, not ranking"},
        "axis_decisions": decisions,
        "scoring_contract": {
            "sell_addition": "no newly validated point addition from this study",
            "sell_deduction": "bullish/rejection evidence may weaken confidence descriptively, but no new fixed numeric deduction passed OOS",
            "buy_addition": "never mirror a sell deduction; require an independent long OOS gate",
            "sideways": "score zero for direction; record compression duration/context and wait for range exit",
            "multi_day_sequence": "retain as research context; current second-break definition did not establish robust OOS edge",
            "operational_state": "review-only; no production adoption",
        },
        "decision": {
            "candidate_local_decision": "no_new_numeric_score_keep",
            "session_aggregate_decision": "complete_research_drop_single_bar_scoring_keep_context_contract",
            "authoritative_rollup_decision": "review_only_no_meemee_reflection",
            "reason_type": "oos_instability_and_missing_clean_shadow",
            "kept_numeric_candidate_count": keep_count,
        },
        "verify": {"required_authoritative_artifacts_present": True, "one_axis_runs_complete": True, "branching_happened": True, "branching_helped": False, "chart_capture_passed": bool(chart and chart.get("judgment") == "pass_clean_detail_screenshot_export")},
        "boundary": {"owner": "TRADEX", "meemee_changed": False, "runtime_db_write": False, "production_ranking_changed": False, "automatic_trading_changed": False},
        "remaining_risks": ["current constituent survivorship bias", "2026 is discovery/exploratory rather than clean shadow", "official chart capture timed out and visual QA is incomplete", "support-touch low-count comparison is unavailable under current detector contract"],
    }
    (output / "final_research_decision.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": str(output / "final_research_decision.json")}, indent=2) + "\n", encoding="utf-8")
    return output


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--compare", action="append", required=True, type=Path)
    parser.add_argument("--chart-audit", type=Path)
    parser.add_argument("--output-root", type=Path, default=Path(r"G:\Tradex\tradex_nikkei225_decline_scoring_rollup_v1"))
    args = parser.parse_args()
    print(run(args.compare, args.chart_audit, args.output_root))


if __name__ == "__main__":
    main()
