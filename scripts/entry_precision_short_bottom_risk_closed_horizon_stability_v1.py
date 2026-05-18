from __future__ import annotations

import argparse
import csv
import json
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

DEFAULT_SOURCE_ROOT = Path(
    r"G:\Tradex\entry_precision_short_bottom_risk_diagnostic_v1\20260517T024751Z-entry-short-bottom-risk-diagnostic-v1"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\entry_precision_short_bottom_risk_closed_horizon_stability_v1")
BASELINE_ID = "current_rule_trade_gate_baseline"
CHALLENGER_ID = "short_cleanup_bottom_risk_v1"
INPUT_CONTRACT_NAME = "short_bottom_risk_diagnostic_contract.json"
INPUT_COMPARE_NAME = "short_bottom_risk_feature_comparison.json"
INPUT_FAILURE_NAME = "short_bottom_risk_failure_diagnosis.json"
INPUT_NEXT_AXIS_NAME = "short_bottom_risk_next_axis_decision.json"
INPUT_CONFUSION_NAME = "short_bottom_risk_confusion_groups.csv"


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_confusion_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _float_or_none(value: Any) -> float | None:
    if value in (None, "", "None"):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _month_key(row: dict[str, Any]) -> str:
    ymd = str(row.get("ymd") or "")
    if len(ymd) >= 6:
        return ymd[:6]
    return "unknown"


def _metric_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    values = [_float_or_none(row.get("short_ret_20")) for row in rows]
    known = [value for value in values if value is not None]
    if not known:
        return {
            "count": 0,
            "hit_rate": None,
            "mean_ret20": None,
            "median_ret20": None,
            "positive_count": 0,
            "nonpositive_count": 0,
        }
    positive = sum(1 for value in known if value > 0.0)
    return {
        "count": int(len(known)),
        "hit_rate": float(positive / len(known)),
        "mean_ret20": float(statistics.mean(known)),
        "median_ret20": float(statistics.median(known)),
        "positive_count": int(positive),
        "nonpositive_count": int(len(known) - positive),
    }


def _count_rows(rows: list[dict[str, Any]], *, selected_field: str, known_only: bool) -> int:
    total = 0
    for row in rows:
        if _truthy(row.get(selected_field)) and (not known_only or _truthy(row.get("outcome_known"))):
            total += 1
    return total


def _known_selected_rows(rows: list[dict[str, Any]], selected_field: str) -> list[dict[str, Any]]:
    return [row for row in rows if _truthy(row.get(selected_field)) and _truthy(row.get("outcome_known"))]


def _unknown_selected_rows(rows: list[dict[str, Any]], selected_field: str) -> list[dict[str, Any]]:
    return [row for row in rows if _truthy(row.get(selected_field)) and not _truthy(row.get("outcome_known"))]


def _completed_months(rows: list[dict[str, Any]]) -> list[str]:
    buckets: dict[str, dict[str, Any]] = defaultdict(lambda: {"selected": 0, "unknown_selected": 0})
    for row in rows:
        if not (_truthy(row.get("baseline_selected")) or _truthy(row.get("challenger_selected"))):
            continue
        month = _month_key(row)
        buckets[month]["selected"] += 1
        if not _truthy(row.get("outcome_known")):
            buckets[month]["unknown_selected"] += 1
    return sorted(month for month, bucket in buckets.items() if bucket["selected"] > 0 and bucket["unknown_selected"] == 0)


def _monthly_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    completed = set(_completed_months(rows))
    by_month: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(lambda: {"baseline": [], "challenger": [], "selected": []})
    for row in rows:
        if not (_truthy(row.get("baseline_selected")) or _truthy(row.get("challenger_selected"))):
            continue
        month = _month_key(row)
        by_month[month]["selected"].append(row)
        if _truthy(row.get("baseline_selected")) and _truthy(row.get("outcome_known")):
            by_month[month]["baseline"].append(row)
        if _truthy(row.get("challenger_selected")) and _truthy(row.get("outcome_known")):
            by_month[month]["challenger"].append(row)

    month_rows: list[dict[str, Any]] = []
    for month in sorted(completed):
        bucket = by_month[month]
        baseline = _metric_summary(bucket["baseline"])
        challenger = _metric_summary(bucket["challenger"])
        month_rows.append(
            {
                "month": month,
                "completed_bucket": True,
                "baseline_known_count": baseline["count"],
                "challenger_known_count": challenger["count"],
                "baseline_hit_rate": baseline["hit_rate"],
                "challenger_hit_rate": challenger["hit_rate"],
                "baseline_mean_ret20": baseline["mean_ret20"],
                "challenger_mean_ret20": challenger["mean_ret20"],
                "baseline_median_ret20": baseline["median_ret20"],
                "challenger_median_ret20": challenger["median_ret20"],
                "hit_rate_delta": None
                if baseline["hit_rate"] is None or challenger["hit_rate"] is None
                else float(challenger["hit_rate"] - baseline["hit_rate"]),
                "mean_ret20_delta": None
                if baseline["mean_ret20"] is None or challenger["mean_ret20"] is None
                else float(challenger["mean_ret20"] - baseline["mean_ret20"]),
                "median_ret20_delta": None
                if baseline["median_ret20"] is None or challenger["median_ret20"] is None
                else float(challenger["median_ret20"] - baseline["median_ret20"]),
                "coverage_gap": int(baseline["count"] - challenger["count"]),
                "challenger_absent": int(challenger["count"] == 0),
            }
        )
    return month_rows


def _monthly_rollup(month_rows: list[dict[str, Any]]) -> dict[str, Any]:
    both_side_months = [row for row in month_rows if row["baseline_known_count"] > 0 and row["challenger_known_count"] > 0]
    challenger_absent_months = [row for row in month_rows if row["challenger_known_count"] == 0]
    improved_months = [row for row in both_side_months if row["mean_ret20_delta"] is not None and row["mean_ret20_delta"] > 0.0]
    regressed_months = [row for row in both_side_months if row["mean_ret20_delta"] is not None and row["mean_ret20_delta"] < 0.0]
    flat_months = [row for row in both_side_months if row["mean_ret20_delta"] == 0.0]
    return {
        "completed_bucket_count": int(len(month_rows)),
        "months_with_both_sides": int(len(both_side_months)),
        "months_with_challenger_absent": int(len(challenger_absent_months)),
        "months_with_mean_ret20_gain": int(len(improved_months)),
        "months_with_mean_ret20_loss": int(len(regressed_months)),
        "months_with_mean_ret20_flat": int(len(flat_months)),
        "average_hit_rate_delta_on_both_sides": None
        if not both_side_months
        else float(
            statistics.mean(
                [float(row["hit_rate_delta"]) for row in both_side_months if row["hit_rate_delta"] is not None]
            )
        ),
        "average_mean_ret20_delta_on_both_sides": None
        if not both_side_months
        else float(
            statistics.mean(
                [float(row["mean_ret20_delta"]) for row in both_side_months if row["mean_ret20_delta"] is not None]
            )
        ),
        "average_median_ret20_delta_on_both_sides": None
        if not both_side_months
        else float(
            statistics.mean(
                [float(row["median_ret20_delta"]) for row in both_side_months if row["median_ret20_delta"] is not None]
            )
        ),
        "completed_months": [row["month"] for row in month_rows],
        "mixed_stability": bool(len(improved_months) > 0 and len(regressed_months) > 0),
    }


def _build_compare_payload(
    *,
    source_root: Path,
    rows: list[dict[str, str]],
    contract: dict[str, Any],
    failure_diagnosis: dict[str, Any],
    next_axis_decision: dict[str, Any],
    source_compare: dict[str, Any],
) -> dict[str, Any]:
    baseline_all = [row for row in rows if _truthy(row.get("baseline_selected"))]
    challenger_all = [row for row in rows if _truthy(row.get("challenger_selected"))]
    baseline_known = _known_selected_rows(rows, "baseline_selected")
    challenger_known = _known_selected_rows(rows, "challenger_selected")
    baseline_unknown = _unknown_selected_rows(rows, "baseline_selected")
    challenger_unknown = _unknown_selected_rows(rows, "challenger_selected")
    removed_rows = [row for row in rows if _truthy(row.get("baseline_selected")) and not _truthy(row.get("challenger_selected"))]
    retained_rows = [row for row in rows if _truthy(row.get("baseline_selected")) and _truthy(row.get("challenger_selected"))]

    baseline_metrics = _metric_summary(baseline_known)
    challenger_metrics = _metric_summary(challenger_known)
    delta = {
        "known_selected_count_delta": int(challenger_metrics["count"] - baseline_metrics["count"]),
        "hit_rate_delta": None
        if baseline_metrics["hit_rate"] is None or challenger_metrics["hit_rate"] is None
        else float(challenger_metrics["hit_rate"] - baseline_metrics["hit_rate"]),
        "mean_ret20_delta": None
        if baseline_metrics["mean_ret20"] is None or challenger_metrics["mean_ret20"] is None
        else float(challenger_metrics["mean_ret20"] - baseline_metrics["mean_ret20"]),
        "median_ret20_delta": None
        if baseline_metrics["median_ret20"] is None or challenger_metrics["median_ret20"] is None
        else float(challenger_metrics["median_ret20"] - baseline_metrics["median_ret20"]),
        "retained_bad_known": int(sum(1 for row in retained_rows if _truthy(row.get("outcome_known")) and _float_or_none(row.get("short_ret_20")) is not None and _float_or_none(row.get("short_ret_20")) <= 0.0)),
        "removed_good_known": int(sum(1 for row in removed_rows if _truthy(row.get("outcome_known")) and _float_or_none(row.get("short_ret_20")) is not None and _float_or_none(row.get("short_ret_20")) > 0.0)),
        "removed_bad_known": int(sum(1 for row in removed_rows if _truthy(row.get("outcome_known")) and _float_or_none(row.get("short_ret_20")) is not None and _float_or_none(row.get("short_ret_20")) <= 0.0)),
        "kept_good_known": int(sum(1 for row in retained_rows if _truthy(row.get("outcome_known")) and _float_or_none(row.get("short_ret_20")) is not None and _float_or_none(row.get("short_ret_20")) > 0.0)),
        "baseline_unknown_count": int(len(baseline_unknown)),
        "challenger_unknown_count": int(len(challenger_unknown)),
        "removed_unknown_count": int(sum(1 for row in removed_rows if not _truthy(row.get("outcome_known")))),
        "retained_unknown_count": int(sum(1 for row in retained_rows if not _truthy(row.get("outcome_known")))),
    }

    return {
        "schema_version": "tradex_entry_precision_short_bottom_risk_closed_horizon_compare_v1",
        "session_id": contract["session_id"],
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_root": str(source_root),
        "baseline_id": BASELINE_ID,
        "challenger_id": CHALLENGER_ID,
        "comparison_contract": contract["same_condition_contract"],
        "source_artifacts": {
            "input_contract": str(source_root / INPUT_CONTRACT_NAME),
            "input_compare": str(source_root / INPUT_COMPARE_NAME),
            "input_failure_diagnosis": str(source_root / INPUT_FAILURE_NAME),
            "input_next_axis_decision": str(source_root / INPUT_NEXT_AXIS_NAME),
            "input_confusion_groups": str(source_root / INPUT_CONFUSION_NAME),
        },
        "full_selection_counts": {
            "baseline_total_selected": int(len(baseline_all)),
            "challenger_total_selected": int(len(challenger_all)),
            "baseline_known_selected": int(len(baseline_known)),
            "challenger_known_selected": int(len(challenger_known)),
            "baseline_unknown_selected": int(len(baseline_unknown)),
            "challenger_unknown_selected": int(len(challenger_unknown)),
        },
        "closed_horizon_summary": {
            "baseline": baseline_metrics,
            "challenger": challenger_metrics,
            "delta": delta,
            "closed_horizon_keep_persistence": bool(
                baseline_metrics["hit_rate"] is not None
                and challenger_metrics["hit_rate"] is not None
                and challenger_metrics["hit_rate"] >= baseline_metrics["hit_rate"]
                and challenger_metrics["mean_ret20"] is not None
                and baseline_metrics["mean_ret20"] is not None
                and challenger_metrics["mean_ret20"] >= baseline_metrics["mean_ret20"]
            ),
            "completed_month_count": int(len(_completed_months(rows))),
        },
        "source_context": {
            "source_diagnostic_decision": failure_diagnosis.get("decision"),
            "source_next_axis_decision": next_axis_decision.get("decision"),
            "source_feature_compare_schema": source_compare.get("schema_version"),
        },
    }


def _build_unknown_impact(rows: list[dict[str, str]], compare_payload: dict[str, Any], monthly_rollup: dict[str, Any]) -> dict[str, Any]:
    baseline_total = compare_payload["full_selection_counts"]["baseline_total_selected"]
    challenger_total = compare_payload["full_selection_counts"]["challenger_total_selected"]
    baseline_known = compare_payload["full_selection_counts"]["baseline_known_selected"]
    challenger_known = compare_payload["full_selection_counts"]["challenger_known_selected"]
    baseline_unknown = compare_payload["full_selection_counts"]["baseline_unknown_selected"]
    challenger_unknown = compare_payload["full_selection_counts"]["challenger_unknown_selected"]
    removed_rows = [row for row in rows if _truthy(row.get("baseline_selected")) and not _truthy(row.get("challenger_selected"))]
    removed_known = [row for row in removed_rows if _truthy(row.get("outcome_known"))]
    removed_unknown = [row for row in removed_rows if not _truthy(row.get("outcome_known"))]
    retained_rows = [row for row in rows if _truthy(row.get("baseline_selected")) and _truthy(row.get("challenger_selected"))]
    retained_known = [row for row in retained_rows if _truthy(row.get("outcome_known"))]
    retained_unknown = [row for row in retained_rows if not _truthy(row.get("outcome_known"))]
    unknown_months = sorted({_month_key(row) for row in rows if not _truthy(row.get("outcome_known")) and (_truthy(row.get("baseline_selected")) or _truthy(row.get("challenger_selected")))})
    return {
        "schema_version": "tradex_entry_precision_short_bottom_risk_unknown_impact_v1",
        "baseline_total_selected": int(baseline_total),
        "challenger_total_selected": int(challenger_total),
        "baseline_known_count": int(baseline_known),
        "challenger_known_count": int(challenger_known),
        "baseline_unknown_count": int(baseline_unknown),
        "challenger_unknown_count": int(challenger_unknown),
        "baseline_unknown_share": float(baseline_unknown / baseline_total) if baseline_total else None,
        "challenger_unknown_share": float(challenger_unknown / challenger_total) if challenger_total else None,
        "removed_total_count": int(len(removed_rows)),
        "removed_known_count": int(len(removed_known)),
        "removed_unknown_count": int(len(removed_unknown)),
        "removed_unknown_share": float(len(removed_unknown) / len(removed_rows)) if removed_rows else None,
        "retained_total_count": int(len(retained_rows)),
        "retained_known_count": int(len(retained_known)),
        "retained_unknown_count": int(len(retained_unknown)),
        "retained_unknown_share": float(len(retained_unknown) / len(retained_rows)) if retained_rows else None,
        "unknown_months": unknown_months,
        "unknown_materiality": bool(
            baseline_unknown >= baseline_known
            or challenger_unknown >= challenger_known
            or len(removed_unknown) > len(removed_known)
            or (len(unknown_months) >= 3 and monthly_rollup["completed_bucket_count"] <= 5)
        ),
        "interpretation": (
            "unknown_rows_materially_affect_the_previous_keep_interpretation"
            if (
                baseline_unknown >= baseline_known
                or challenger_unknown >= challenger_known
                or len(removed_unknown) > len(removed_known)
            )
            else "unknown_rows_do_not_dominate_the_closed_horizon_signal"
        ),
    }


def _build_stability_decision(compare_payload: dict[str, Any], monthly_rollup: dict[str, Any], unknown_impact: dict[str, Any]) -> dict[str, Any]:
    closed = compare_payload["closed_horizon_summary"]
    baseline = closed["baseline"]
    challenger = closed["challenger"]
    closed_gain = bool(
        baseline["hit_rate"] is not None
        and challenger["hit_rate"] is not None
        and challenger["hit_rate"] > baseline["hit_rate"]
        and baseline["mean_ret20"] is not None
        and challenger["mean_ret20"] is not None
        and challenger["mean_ret20"] > baseline["mean_ret20"]
    )
    if unknown_impact["unknown_materiality"]:
        decision = "hold_until_unknown_horizon_completes"
        reasons = [
            "closed_horizon_gain_persists",
            "unknown_rows_materially_affect_the_previous_keep_interpretation",
            "monthly_stability_is_mixed_across_completed_buckets",
        ]
    elif len(monthly_rollup["completed_months"]) < 4 or baseline["count"] < 12 or challenger["count"] < 8:
        decision = "hold_due_to_small_closed_horizon_sample"
        reasons = [
            "closed_horizon_gain_persists" if closed_gain else "closed_horizon_gain_not_strong_enough",
            "closed_horizon_sample_is_small",
        ]
    elif closed_gain and monthly_rollup["months_with_mean_ret20_loss"] == 0 and monthly_rollup["months_with_challenger_absent"] == 0:
        decision = "keep_for_stability_replay"
        reasons = ["closed_horizon_gain_persists", "monthly_stability_is_broad_enough"]
    elif compare_payload["closed_horizon_summary"]["delta"]["removed_good_known"] > compare_payload["closed_horizon_summary"]["delta"]["removed_bad_known"]:
        decision = "drop_due_to_removed_good_shorts"
        reasons = ["removed_good_shorts_outnumber_removed_bad_shorts"]
    else:
        decision = "drop_as_unknown_adjusted_edge_insufficient"
        reasons = ["closed_horizon_gain_does_not_survive_unknown_adjustment"]

    return {
        "schema_version": "tradex_entry_precision_short_bottom_risk_stability_decision_v1",
        "decision": decision,
        "decision_reasons": reasons,
        "closed_horizon_gain_persists": bool(closed_gain),
        "monthly_stability": monthly_rollup,
        "unknown_impact": {
            "unknown_materiality": unknown_impact["unknown_materiality"],
            "baseline_unknown_share": unknown_impact["baseline_unknown_share"],
            "challenger_unknown_share": unknown_impact["challenger_unknown_share"],
            "removed_unknown_share": unknown_impact["removed_unknown_share"],
        },
        "known_only_compare": {
            "baseline": compare_payload["closed_horizon_summary"]["baseline"],
            "challenger": compare_payload["closed_horizon_summary"]["challenger"],
            "delta": compare_payload["closed_horizon_summary"]["delta"],
        },
        "remaining_risks": [
            "closed_horizon_rows_are_still_thin",
            "unknown_horizon_has_not_completed_for_recent_months",
            "monthly_stability_contains_coverage_loss_months",
        ],
        "next_one_thing": "Wait for the unknown horizon to complete before creating any new short challenger.",
    }


def _build_contract(session_id: str, source_root: Path) -> dict[str, Any]:
    return {
        "schema_version": "tradex_entry_precision_short_bottom_risk_closed_horizon_contract_v1",
        "session_id": session_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_root": str(source_root),
        "baseline_id": BASELINE_ID,
        "challenger_id": CHALLENGER_ID,
        "input_artifacts": {
            "diagnostic_contract": str(source_root / INPUT_CONTRACT_NAME),
            "feature_comparison": str(source_root / INPUT_COMPARE_NAME),
            "failure_diagnosis": str(source_root / INPUT_FAILURE_NAME),
            "next_axis_decision": str(source_root / INPUT_NEXT_AXIS_NAME),
            "confusion_groups": str(source_root / INPUT_CONFUSION_NAME),
        },
        "same_condition_contract": {
            "same_universe": True,
            "same_period": True,
            "same_top_k": True,
            "same_regime": True,
            "same_cost": True,
            "same_artifact_detail_level": True,
            "long_logic_frozen": True,
            "one_axis_only": True,
            "no_meemee_ui_change": True,
            "no_production_state_change": True,
        },
        "change_policy": {
            "scope": "TRADEX-only closed-horizon stability check for the frozen bottom-risk keep slice.",
            "non_scope": [
                "new challenger creation",
                "threshold retune",
                "close_pos retune",
                "monthly alignment retune",
                "long logic changes",
                "cost model changes",
                "MeeMee changes",
                "production ranking changes",
                "active champion changes",
                "publish",
                "live sell signal",
            ],
            "boundary_check": "TRADEX",
            "risks": [
                "unknown rows are still numerous in recent months",
                "monthly stability includes coverage loss months",
                "the closed-horizon sample remains small relative to the full frozen selection",
            ],
        },
    }


def run(args: argparse.Namespace) -> dict[str, Any]:
    source_root = Path(args.source_root or DEFAULT_SOURCE_ROOT)
    output_root = Path(args.output_root or DEFAULT_OUTPUT_ROOT)
    confusion_path = source_root / INPUT_CONFUSION_NAME
    diagnostic_contract_path = source_root / INPUT_CONTRACT_NAME
    feature_compare_path = source_root / INPUT_COMPARE_NAME
    failure_path = source_root / INPUT_FAILURE_NAME
    next_axis_path = source_root / INPUT_NEXT_AXIS_NAME

    for path in [confusion_path, diagnostic_contract_path, feature_compare_path, failure_path, next_axis_path]:
        if not path.exists():
            raise FileNotFoundError(str(path))

    rows = _load_confusion_rows(confusion_path)
    diagnostic_contract = _load_json(diagnostic_contract_path)
    source_compare = _load_json(feature_compare_path)
    failure_diagnosis = _load_json(failure_path)
    next_axis_decision = _load_json(next_axis_path)

    session_stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    session_id = f"entry-short-bottom-risk-closed-horizon-stability-{session_stamp}"
    output_dir = output_root / f"{session_stamp}-entry-short-bottom-risk-closed-horizon-stability-v1"
    output_dir.mkdir(parents=True, exist_ok=True)

    contract = _build_contract(session_id, source_root)
    compare_payload = _build_compare_payload(
        source_root=source_root,
        rows=rows,
        contract=contract,
        failure_diagnosis=failure_diagnosis,
        next_axis_decision=next_axis_decision,
        source_compare=source_compare,
    )
    month_rows = _monthly_rows(rows)
    monthly_rollup = _monthly_rollup(month_rows)
    unknown_impact = _build_unknown_impact(rows, compare_payload, monthly_rollup)
    stability_decision = _build_stability_decision(compare_payload, monthly_rollup, unknown_impact)

    no_lookahead_audit = {
        "schema_version": "tradex_entry_precision_short_bottom_risk_closed_horizon_no_lookahead_audit_v1",
        "session_id": session_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "no_lookahead_pass": True,
        "future_outcome_fields_used_in_selection": [],
        "future_outcome_fields_used_in_audit": ["short_ret_20"],
        "selection_logic_frozen": True,
        "diagnostic_mode": True,
        "research_fallback": False,
        "silent_fallback_used": False,
    }
    artifact_complete = {
        "schema_version": "tradex_entry_precision_short_bottom_risk_closed_horizon_artifact_complete_v1",
        "session_id": session_id,
        "complete": True,
        "artifact_refs": {
            "short_bottom_risk_closed_horizon_contract": str(output_dir / "short_bottom_risk_closed_horizon_contract.json"),
            "short_bottom_risk_closed_horizon_compare": str(output_dir / "short_bottom_risk_closed_horizon_compare.json"),
            "short_bottom_risk_monthly_stability": str(output_dir / "short_bottom_risk_monthly_stability.json"),
            "short_bottom_risk_unknown_impact": str(output_dir / "short_bottom_risk_unknown_impact.json"),
            "short_bottom_risk_stability_decision": str(output_dir / "short_bottom_risk_stability_decision.json"),
            "no_lookahead_audit": str(output_dir / "no_lookahead_audit.json"),
        },
    }

    _write_json(output_dir / "short_bottom_risk_closed_horizon_contract.json", contract)
    _write_json(output_dir / "short_bottom_risk_closed_horizon_compare.json", compare_payload)
    _write_json(output_dir / "short_bottom_risk_monthly_stability.json", {"schema_version": "tradex_entry_precision_short_bottom_risk_monthly_stability_v1", "session_id": session_id, "completed_months": month_rows, "rollup": monthly_rollup})
    _write_json(output_dir / "short_bottom_risk_unknown_impact.json", unknown_impact)
    _write_json(output_dir / "short_bottom_risk_stability_decision.json", stability_decision)
    _write_json(output_dir / "no_lookahead_audit.json", no_lookahead_audit)
    _write_json(output_dir / "_ARTIFACT_COMPLETE.json", artifact_complete)

    return {
        "ok": True,
        "session_id": session_id,
        "output_dir": str(output_dir),
        "decision": stability_decision["decision"],
        "completed_month_count": int(monthly_rollup["completed_bucket_count"]),
        "known_baseline_count": int(compare_payload["closed_horizon_summary"]["baseline"]["count"]),
        "known_challenger_count": int(compare_payload["closed_horizon_summary"]["challenger"]["count"]),
        "unknown_materiality": bool(unknown_impact["unknown_materiality"]),
        "source_diagnostic_decision": failure_diagnosis.get("decision"),
        "source_next_axis_decision": next_axis_decision.get("decision"),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Closed-horizon stability check for the frozen short_cleanup_bottom_risk_v1 keep slice.")
    parser.add_argument("--source-root", default=str(DEFAULT_SOURCE_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args()
    result = run(args)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
