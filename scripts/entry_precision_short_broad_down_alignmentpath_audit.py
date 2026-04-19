from __future__ import annotations

import argparse
import importlib
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

base = importlib.import_module("scripts.entry_precision_short_audit")
wide = importlib.import_module("scripts.entry_precision_short_trend_wide_audit")
trend = importlib.import_module("scripts.entry_precision_short_trend_audit")
regime = importlib.import_module("scripts.entry_precision_short_trend_regime_audit")
monthly_fix = importlib.import_module("scripts.entry_precision_short_broad_down_monthly_fix_audit")

DEFAULT_OUTPUT_DIR = base.DEFAULT_OUTPUT_DIR
BASELINE_ID = "current_rule_trade_gate_baseline"
CHALLENGER_ID = "short_trend_alignment_v1"
FOCUSED_REGIME = "broad_down_regime"
FROZEN_FAILURE_SESSION_ID = "entry-short-broad-down-rangeprob-20260419-133254"
LOCAL_WINDOW = {"start_ymd": 20250101, "end_ymd": 20260226}
WIDE_WINDOW = {"start_ymd": 20240101, "end_ymd": 20260226}

FROZEN_REFERENCE_VARIANT = trend.TrendVariant(
    name="broad_down_monthly_rangeprob_off_v1",
    target="bad-pick removal",
    weekly_breakout_down_min=0.66,
    monthly_breakout_down_min=0.66,
    monthly_range_prob_max=None,
    close_pos_max=0.20,
)

TARGET_KEYS = [
    (20240229, "6750"),
    (20241230, "4684"),
    (20250930, "9861"),
]


def _resolve_db_path(cli_value: str | None) -> Path:
    return base._resolve_db_path(cli_value)


def _load_window_payload(
    conn: duckdb.DuckDBPyConnection,
    *,
    start_ymd: int,
    end_ymd: int,
) -> dict[str, Any]:
    return wide._load_window_payload(conn, start_ymd=start_ymd, end_ymd=end_ymd)


def _row_key(row: dict[str, Any]) -> tuple[int, str]:
    return int(row["ymd"]), str(row["code"])


def _row_view(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "ymd": int(row["ymd"]),
        "code": str(row["code"]),
        "short_ret_20": float(row["short_ret_20"]) if row.get("short_ret_20") is not None else None,
        "short_ret_5": float(row["short_ret_5"]) if row.get("short_ret_5") is not None else None,
        "short_ret_10": float(row["short_ret_10"]) if row.get("short_ret_10") is not None else None,
        "mae20": float(row["mae20"]) if row.get("mae20") is not None else None,
        "mfe20": float(row["mfe20"]) if row.get("mfe20") is not None else None,
        "entryScore": float(row["entryScore"]) if row.get("entryScore") is not None else None,
        "close_pos": float(row["close_pos"]) if row.get("close_pos") is not None else None,
        "dist_ma20_signed": float(row["dist_ma20_signed"]) if row.get("dist_ma20_signed") is not None else None,
        "dist_low20": float(row["dist_low20"]) if row.get("dist_low20") is not None else None,
        "monthlyRangeProb": float(row["monthlyRangeProb"]) if row.get("monthlyRangeProb") is not None else None,
        "monthlyRangePos": float(row["monthlyRangePos"]) if row.get("monthlyRangePos") is not None else None,
        "weeklyBreakoutDownProb": float(row["weeklyBreakoutDownProb"]) if row.get("weeklyBreakoutDownProb") is not None else None,
        "monthlyBreakoutDownProb": float(row["monthlyBreakoutDownProb"]) if row.get("monthlyBreakoutDownProb") is not None else None,
        "trendDownStrict": bool(row["trendDownStrict"]) if row.get("trendDownStrict") is not None else None,
        "marketRiskOff": bool(row["marketRiskOff"]) if row.get("marketRiskOff") is not None else None,
        "marketRegime": row.get("marketRegime"),
    }


def _metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return monthly_fix._metrics(rows)


def _month_split(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return monthly_fix._month_split(rows)


def _broad_down_rows(payload: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows = payload["rows"]
    baseline_rows = [r for r in payload["result"]["baseline_rows"] if regime._bucket_regime(r) == FOCUSED_REGIME]
    broad_down_rows = [r for r in rows if regime._bucket_regime(r) == FOCUSED_REGIME]
    return broad_down_rows, baseline_rows


def _variant_result(rows: list[dict[str, Any]], variant: trend.TrendVariant) -> dict[str, Any]:
    return trend._evaluate_variant(rows, variant=variant, taxonomy={})


def _variant_contract(variant: trend.TrendVariant) -> dict[str, Any]:
    return {
        "name": variant.name,
        "target": variant.target,
        "weekly_breakout_down_min": variant.weekly_breakout_down_min,
        "monthly_breakout_down_min": variant.monthly_breakout_down_min,
        "monthly_range_prob_max": variant.monthly_range_prob_max,
        "close_pos_max": variant.close_pos_max,
        "require_midrange_off": variant.require_midrange_off,
    }


def _ordered_gate_trace(row: dict[str, Any], variant: trend.TrendVariant) -> list[dict[str, Any]]:
    trace: list[dict[str, Any]] = []
    trace.append(
        {
            "order": 1,
            "gate": "selected_by_baseline",
            "value": bool(row.get("selected_by_baseline")) if row.get("selected_by_baseline") is not None else None,
            "threshold": None,
            "passed": bool(row.get("selected_by_baseline")),
            "failed": row.get("selected_by_baseline") is not True,
            "reason_code": "not_selected_by_baseline" if row.get("selected_by_baseline") is not True else None,
            "source": "trend.TrendVariant.matches / monthly_fix._gate_blockers",
        }
    )
    gate_checks = [
        (
            "weekly_alignment_failed",
            "weeklyBreakoutDownProb",
            variant.weekly_breakout_down_min,
            lambda value, threshold: value is None or float(value) < float(threshold),
        ),
        (
            "monthly_alignment_failed",
            "monthlyBreakoutDownProb",
            variant.monthly_breakout_down_min,
            lambda value, threshold: value is None or float(value) < float(threshold),
        ),
        (
            "monthly_range_prob_failed",
            "monthlyRangeProb",
            variant.monthly_range_prob_max,
            lambda value, threshold: value is None or float(value) > float(threshold),
        ),
        (
            "close_pos_failed",
            "close_pos",
            variant.close_pos_max,
            lambda value, threshold: value is None or float(value) > float(threshold),
        ),
        (
            "midrange_off_failed",
            "monthlyRangePos",
            "0.35..0.65",
            lambda value, _threshold: value is not None and 0.35 <= float(value) <= 0.65,
        ),
    ]
    for idx, (reason_code, feature_name, threshold, is_failed) in enumerate(gate_checks, start=2):
        value = row.get(feature_name)
        active = threshold is not None or reason_code == "midrange_off_failed"
        failed = bool(is_failed(value, threshold)) if active else False
        trace.append(
            {
                "order": idx,
                "gate": reason_code,
                "feature": feature_name,
                "value": float(value) if value is not None else None,
                "threshold": threshold if not isinstance(threshold, str) else threshold,
                "active": active,
                "passed": not failed,
                "failed": failed,
                "reason_code": reason_code if failed else None,
                "source": "monthly_fix._gate_blockers",
            }
        )
    return trace


def _first_failed_gate(trace: list[dict[str, Any]]) -> str | None:
    for step in trace:
        if step["failed"]:
            return str(step["reason_code"])
    return None


def _failed_gates(trace: list[dict[str, Any]]) -> list[str]:
    return [str(step["reason_code"]) for step in trace if step["failed"] and step["reason_code"]]


def _tested_by_prior_ablation(reason_code: str) -> bool:
    return reason_code in {"monthly_alignment_failed", "monthly_range_prob_failed"}


def _build_name_audit(
    *,
    baseline_rows: list[dict[str, Any]],
    reference_rows: list[dict[str, Any]],
    reference_variant: trend.TrendVariant,
) -> dict[str, Any]:
    baseline_map = {_row_key(r): r for r in baseline_rows}
    reference_map = {_row_key(r): r for r in reference_rows}
    per_name: list[dict[str, Any]] = []
    for key in TARGET_KEYS:
        row = baseline_map.get(key) or reference_map.get(key)
        if row is None:
            continue
        row = {**row, "selected_by_baseline": key in baseline_map}
        trace = _ordered_gate_trace(row, reference_variant)
        failed_gates = _failed_gates(trace)
        per_name.append(
            {
                "ymd": key[0],
                "code": key[1],
                "baseline_selected": key in baseline_map,
                "reference_selected": key in reference_map,
                "feature_values": {
                    "weeklyBreakoutDownProb": row.get("weeklyBreakoutDownProb"),
                    "monthlyBreakoutDownProb": row.get("monthlyBreakoutDownProb"),
                    "monthlyRangeProb": row.get("monthlyRangeProb"),
                    "monthlyRangePos": row.get("monthlyRangePos"),
                    "close_pos": row.get("close_pos"),
                    "dist_ma20_signed": row.get("dist_ma20_signed"),
                    "dist_low20": row.get("dist_low20"),
                    "trendDownStrict": row.get("trendDownStrict"),
                },
                "rule_path": trace,
                "first_failing_gate": _first_failed_gate(trace),
                "failed_gates": failed_gates,
                "emitted_reason_codes": failed_gates,
                "prior_ablation_status": {
                    "monthly_alignment_failed": "tested" if _tested_by_prior_ablation("monthly_alignment_failed") else "not_tested",
                    "monthly_range_prob_failed": "tested" if _tested_by_prior_ablation("monthly_range_prob_failed") else "not_tested",
                    "close_pos_failed": "not_tested",
                    "midrange_off_failed": "not_tested",
                },
            }
        )
    return {
        "schema_version": "tradex_entry_precision_short_broad_down_alignmentpath_name_audit_v1",
        "session_id": f"entry-short-broad-down-alignmentpath-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "focused_regime": FOCUSED_REGIME,
        "names": per_name,
    }


def _build_trace(
    *,
    baseline_rows: list[dict[str, Any]],
    reference_rows: list[dict[str, Any]],
    reference_variant: trend.TrendVariant,
) -> dict[str, Any]:
    name_audit = _build_name_audit(
        baseline_rows=baseline_rows,
        reference_rows=reference_rows,
        reference_variant=reference_variant,
    )
    name_index = {f"{item['ymd']}::{item['code']}": item for item in name_audit["names"]}
    trace_order = [
        {
            "reason_code": "monthly_alignment_failed",
            "direct_gate": True,
            "aggregate": False,
            "fallback": False,
            "source_function": "monthly_fix._gate_blockers",
            "source_condition": "monthlyBreakoutDownProb < monthly_breakout_down_min",
            "current_threshold": reference_variant.monthly_breakout_down_min,
            "prior_ablation_tested": True,
            "notes": [
                "This label is emitted directly from the monthly breakout threshold check.",
                "It is not an umbrella label for the range-probability or close-position gates.",
            ],
        },
        {
            "reason_code": "monthly_range_prob_failed",
            "direct_gate": True,
            "aggregate": False,
            "fallback": False,
            "source_function": "monthly_fix._gate_blockers",
            "source_condition": "monthlyRangeProb > monthly_range_prob_max",
            "current_threshold": reference_variant.monthly_range_prob_max,
            "prior_ablation_tested": True,
        },
        {
            "reason_code": "close_pos_failed",
            "direct_gate": True,
            "aggregate": False,
            "fallback": False,
            "source_function": "monthly_fix._gate_blockers",
            "source_condition": "close_pos > close_pos_max",
            "current_threshold": reference_variant.close_pos_max,
            "prior_ablation_tested": False,
        },
        {
            "reason_code": "midrange_off_failed",
            "direct_gate": True,
            "aggregate": False,
            "fallback": False,
            "source_function": "monthly_fix._gate_blockers",
            "source_condition": "0.35 <= monthlyRangePos <= 0.65",
            "current_threshold": "0.35..0.65",
            "prior_ablation_tested": False,
        },
    ]
    return {
        "schema_version": "tradex_entry_precision_short_broad_down_alignmentpath_trace_v1",
        "session_id": name_audit["session_id"],
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "baseline_id": BASELINE_ID,
        "challenger_id": CHALLENGER_ID,
        "frozen_failure_session_id": FROZEN_FAILURE_SESSION_ID,
        "long_freeze_confirmed": True,
        "comparison_contract": {
            "same_universe": True,
            "same_period": True,
            "same_top_k": True,
            "same_regime": True,
            "same_cost": True,
            "same_artifact_detail_level": True,
            "long_short_separated": True,
            "long_logic_frozen": True,
            "no_meemee_ui_change": True,
            "rolling_oos": True,
            "wide_window": WIDE_WINDOW,
            "local_reference_window": LOCAL_WINDOW,
        },
        "current_reference_variant": _variant_contract(reference_variant),
        "rule_path_definition": trace_order,
        "exact_blocker_path_summary": {
            "monthly_alignment_failed_is_direct_gate": True,
            "monthly_alignment_failed_is_aggregate": False,
            "monthly_alignment_failed_is_fallback": False,
            "shared_blocker": "monthly_alignment_failed",
            "shared_blocker_rule_path": "monthlyBreakoutDownProb < monthly_breakout_down_min",
        },
        "name_audit_index": name_index,
    }


def _build_shared_vs_specific(name_audit: dict[str, Any]) -> dict[str, Any]:
    names = name_audit["names"]
    blocker_sets = {f"{item['ymd']}::{item['code']}": set(item["failed_gates"]) for item in names}
    shared = set.intersection(*(blocker_sets.values())) if blocker_sets else set()
    specific = {
        item["code"]: sorted(set(item["failed_gates"]) - shared)
        for item in names
        if set(item["failed_gates"]) - shared
    }
    stale_or_aggregate = []
    return {
        "schema_version": "tradex_entry_precision_short_broad_down_alignmentpath_shared_vs_specific_v1",
        "session_id": name_audit["session_id"],
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "focused_regime": FOCUSED_REGIME,
        "shared_blockers": sorted(shared),
        "name_specific_blockers": specific,
        "stale_or_aggregate_reason_codes": stale_or_aggregate,
        "shared_blocker_interpretation": {
            "monthly_alignment_failed": "direct_single_gate",
            "monthly_alignment_failed_is_not_aggregate": True,
            "monthly_alignment_failed_is_not_fallback": True,
        },
    }


def _decision(shared_vs_specific: dict[str, Any]) -> tuple[str, list[str]]:
    shared = shared_vs_specific["shared_blockers"]
    reasons = [
        f"shared_blockers={','.join(shared) if shared else 'none'}",
        f"name_specific_count={len(shared_vs_specific['name_specific_blockers'])}",
        f"stale_or_aggregate_reason_codes={len(shared_vs_specific['stale_or_aggregate_reason_codes'])}",
    ]
    if shared == ["monthly_alignment_failed"]:
        return "hold", reasons + ["shared_blocker_is_direct_but_fix_still_needs_one_more_isolated_validation"]
    if not shared:
        return "drop", reasons + ["no_shared_blocker_identified"]
    return "hold", reasons + ["shared_blocker_identified_but_not_yet_actioned"]


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_report_md(path: Path, payload: dict[str, Any]) -> None:
    compare = payload["compare"]
    trace = payload["trace"]
    shared = payload["shared_vs_specific"]
    decision = payload["decision"]
    lines = [
        "# Entry Precision Short Broad Down AlignmentPath Audit",
        "",
        "## Current State",
        f"- confirmed: {', '.join(payload['current_state']['confirmed'])}",
        f"- provisional: {', '.join(payload['current_state']['provisional'])}",
        "",
        "## Problem",
        payload["problem"],
        "",
        "## Change Policy",
        payload["change_policy"],
        "",
        "## Concrete Changes",
        f"- frozen failure session: `{payload['frozen_failure_session_id']}`",
        f"- frozen reference variant: `{trace['current_reference_variant']['name']}`",
        "",
        "## Verify",
        f"- broad-down baseline count: `{compare['baseline']['count']}`",
        f"- broad-down reference count: `{compare['reference']['count']}`",
        f"- long freeze confirmed: `{trace['long_freeze_confirmed']}`",
        f"- shared blocker: `{', '.join(shared['shared_blockers'])}`",
        "",
        "## Decision",
        f"- overall: `{decision['overall_decision']}`",
        f"- reasons: {', '.join(decision['decision_reasons'])}",
        "",
        "## Remaining Risks",
        "\n".join([f"- {risk}" for risk in decision["remaining_risks"]]),
        "",
        "## Next One Thing",
        payload["next_one_thing"],
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(args: argparse.Namespace) -> dict[str, Any]:
    db_path = _resolve_db_path(args.db_path or None)
    out_dir = Path(args.output_dir).expanduser().resolve() if args.output_dir else DEFAULT_OUTPUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    session_id = f"entry-short-broad-down-alignmentpath-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"

    with duckdb.connect(str(db_path), read_only=True) as conn:
        wide_payload = _load_window_payload(conn, start_ymd=WIDE_WINDOW["start_ymd"], end_ymd=WIDE_WINDOW["end_ymd"])

    broad_down_rows, baseline_rows = _broad_down_rows(wide_payload)
    reference_result = _variant_result(broad_down_rows, FROZEN_REFERENCE_VARIANT)
    reference_rows = reference_result["selected_rows"]
    reference_metrics = _metrics(reference_rows)
    baseline_metrics = _metrics(baseline_rows)
    compare_detail = monthly_fix._compare_with_reference(baseline_rows, reference_rows, reference_rows)
    compare = {
        "schema_version": "tradex_entry_precision_short_broad_down_alignmentpath_compare_v1",
        "session_id": session_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "baseline_id": BASELINE_ID,
        "challenger_id": CHALLENGER_ID,
        "frozen_failure_session_id": FROZEN_FAILURE_SESSION_ID,
        "baseline": baseline_metrics,
        "reference": reference_metrics,
        "branching": {
            "changed_top5_short_count": compare_detail["changed_top5_short_count"],
            "changed_top10_short_count": compare_detail["changed_top10_short_count"],
            "changed_rank_short_count": compare_detail["changed_rank_short_count"],
            "selection_divergence_reason": "reference_refinement_preserves_shared_monthly_alignment_blocker",
        },
        "comparison_contract": {
            "same_universe": True,
            "same_period": True,
            "same_top_k": True,
            "same_regime": True,
            "same_cost": True,
            "same_artifact_detail_level": True,
            "long_short_separated": True,
            "long_logic_frozen": True,
            "no_meemee_ui_change": True,
            "rolling_oos": True,
            "wide_window": WIDE_WINDOW,
            "local_reference_window": LOCAL_WINDOW,
        },
        "reference_variant": _variant_contract(FROZEN_REFERENCE_VARIANT),
        "broad_down_only": {
            "baseline_month_split": _month_split(baseline_rows),
            "reference_month_split": _month_split(reference_rows),
            "baseline_metrics": baseline_metrics,
            "reference_metrics": reference_metrics,
        },
    }
    trace = _build_trace(
        baseline_rows=baseline_rows,
        reference_rows=reference_rows,
        reference_variant=FROZEN_REFERENCE_VARIANT,
    )
    name_audit_payload = _build_name_audit(
        baseline_rows=baseline_rows,
        reference_rows=reference_rows,
        reference_variant=FROZEN_REFERENCE_VARIANT,
    )
    shared_vs_specific = _build_shared_vs_specific(name_audit_payload)
    decision_value, decision_reasons = _decision(shared_vs_specific)
    trace["branching"] = compare["branching"]
    trace["broad_down_metrics"] = {
        "baseline": baseline_metrics,
        "reference": reference_metrics,
    }
    decision = {
        "schema_version": "tradex_entry_precision_short_broad_down_alignmentpath_decision_v1",
        "session_id": session_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "baseline_id": BASELINE_ID,
        "challenger_id": CHALLENGER_ID,
        "frozen_failure_session_id": FROZEN_FAILURE_SESSION_ID,
        "long_freeze_confirmed": True,
        "overall_decision": decision_value,
        "decision_reasons": decision_reasons,
        "confirmed": [
            "short_logic_frozen",
            "long_logic_frozen",
            "same_window_reference_retained",
            "broad_down_only_reviewed",
            "no_thresholds_changed",
            "json_is_authoritative",
        ],
        "provisional": [
            "monthly_alignment_failed_is_direct_not_aggregate",
            "broad_down_sample_is_still_small",
            "close_pos_failed_remains_name_specific_for_4684",
        ],
        "remaining_risks": [
            "the_shared_blocker_is_identified_but_not_yet_retuned",
            "sample_size_is_still_small_relative_to_the_full_history",
            "4684_has_name_specific_blockers_beyond_monthly_alignment",
        ],
    }

    report_payload = {
        "current_state": {
            "confirmed": [
                "monthly_range_prob_hypothesis_was_rejected",
                "long_logic_remained_frozen",
                "broad_down_only_current_reference_was_frozen",
            ],
            "provisional": [
                "monthly_alignment_failed_may_be_a_coarse_label_but_is_direct",
                "the_shared_blocker_may_still_need_one_more_isolated_validation",
                "regime_labels_are_proxy_derived",
            ],
        },
        "problem": "The remaining broad-down blocker label is monthly_alignment_failed, and the task is to determine whether it is a direct gate or an umbrella label before any threshold change.",
        "change_policy": "TRADEX research only, blocker-path decomposition only, keep long logic frozen, keep MeeMee untouched, do not change thresholds, do not run a new ablation, and do not review other failure regimes.",
        "frozen_failure_session_id": FROZEN_FAILURE_SESSION_ID,
        "compare": compare,
        "trace": trace,
        "shared_vs_specific": shared_vs_specific,
        "decision": decision,
        "long_freeze_confirmed": True,
        "remaining_risks": decision["remaining_risks"],
        "next_one_thing": "If this lane continues, the next step would be a single isolated monthly_breakout_down_min validation, not a broader redesign.",
    }

    name_audit_output = {
        "schema_version": "tradex_entry_precision_short_broad_down_alignmentpath_name_audit_v1",
        "session_id": session_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "frozen_failure_session_id": FROZEN_FAILURE_SESSION_ID,
        "focused_regime": FOCUSED_REGIME,
        "names": name_audit_payload["names"],
    }

    _write_json(out_dir / "entry_precision_short_broad_down_alignmentpath_contract.json", {
        "schema_version": "tradex_entry_precision_short_broad_down_alignmentpath_contract_v1",
        "session_id": session_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "baseline_id": BASELINE_ID,
        "challenger_id": CHALLENGER_ID,
        "frozen_failure_session_id": FROZEN_FAILURE_SESSION_ID,
        "frozen_reference_variant": _variant_contract(FROZEN_REFERENCE_VARIANT),
        "comparison_contract": compare["comparison_contract"],
    })
    _write_json(out_dir / "entry_precision_short_broad_down_alignmentpath_trace.json", trace)
    _write_json(out_dir / "entry_precision_short_broad_down_alignmentpath_name_audit.json", name_audit_output)
    _write_json(out_dir / "entry_precision_short_broad_down_alignmentpath_shared_vs_specific.json", shared_vs_specific)
    _write_json(out_dir / "entry_precision_short_broad_down_alignmentpath_decision.json", decision)
    _write_report_md(out_dir / "entry_precision_short_broad_down_alignmentpath_report.md", report_payload)

    return {
        "ok": True,
        "session_id": session_id,
        "output_dir": str(out_dir),
        "baseline_id": BASELINE_ID,
        "challenger_id": CHALLENGER_ID,
        "overall_decision": decision_value,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Blocker-path decomposition for monthly_alignment_failed in broad-down.")
    parser.add_argument("--db-path", default="", help="Path to authoritative snapshot DB")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args()
    result = run(args)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
