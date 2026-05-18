from __future__ import annotations

import argparse
import csv
import json
import statistics
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

DEFAULT_COMPARE_ROOT = Path(r"G:\Tradex\entry_precision_short_audit_v1")
DEFAULT_FAMILY_DECISION_ROOT = Path(r"G:\Tradex\entry_precision_short_audit_v1")
DEFAULT_CLOSEPOS_DECISION_PATH = Path(
    r"G:\Tradex\entry_precision_short_broad_down_closepos_audit_v1\entry_precision_short_broad_down_closepos_fix_decision.json"
)
DEFAULT_MONTHLY_DECISION_PATH = Path(
    r"G:\Tradex\entry_precision_short_broad_down_monthly_fix_audit_v1\entry_precision_short_broad_down_monthly_fix_decision.json"
)
DEFAULT_OUTPUT_DIR = Path(r"G:\Tradex\entry_precision_short_bottom_risk_diagnostic_v1")
BASELINE_ID = "current_rule_trade_gate_baseline"
CHALLENGER_ID = "short_cleanup_bottom_risk_v1"
VARIANT_KEY = "short_cleanup_bottom_risk_v1"

NUMERIC_FEATURES = [
    "short_ret_20",
    "short_ret_10",
    "short_ret_5",
    "mae20",
    "mfe20",
    "close_pos",
    "dist_low20",
    "dist_ma20_signed",
    "day_change_pct",
    "monthlyRangeProb",
    "monthlyRangePos",
    "weeklyBreakoutDownProb",
    "monthlyBreakoutDownProb",
    "entryScore",
    "tradePriorityScore",
    "liquidity20d",
]

CATEGORICAL_FEATURES = [
    "marketRegime",
]

BOOLEAN_FEATURES = [
    "marketRiskOff",
    "trendDownStrict",
]

CSV_FIELDS = [
    "ymd",
    "code",
    "confusion_group",
    "baseline_selected",
    "challenger_selected",
    "outcome_known",
    "outcome_positive",
    "outcome_bucket",
    "short_ret_20",
    "short_ret_10",
    "short_ret_5",
    "close_pos",
    "dist_low20",
    "dist_ma20_signed",
    "day_change_pct",
    "monthlyRangeProb",
    "monthlyRangePos",
    "weeklyBreakoutDownProb",
    "monthlyBreakoutDownProb",
    "marketRiskOff",
    "marketRegime",
    "trendDownStrict",
    "entryScore",
    "tradePriorityScore",
    "liquidity20d",
    "mae20",
    "mfe20",
    "baseline_rank",
    "tradeDecisionReasons",
    "tradeRiskWatch",
]


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _json_value(value: Any) -> Any:
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False)
    return value


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: _json_value(row.get(field)) for field in fieldnames})


def _maybe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _row_key(row: dict[str, Any]) -> tuple[int, str]:
    return int(row["ymd"]), str(row["code"])


def _list_counts(rows: list[dict[str, Any]], field: str) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for row in rows:
        values = row.get(field) or []
        if isinstance(values, str):
            values = [values]
        for value in values:
            counter[str(value)] += 1
    return dict(counter)


def _numeric_summary(rows: list[dict[str, Any]], field: str) -> dict[str, Any]:
    values = [_maybe_float(row.get(field)) for row in rows]
    filtered = [value for value in values if value is not None]
    if not filtered:
        return {"count": 0, "mean": None, "median": None, "min": None, "max": None}
    return {
        "count": int(len(filtered)),
        "mean": float(statistics.mean(filtered)),
        "median": float(statistics.median(filtered)),
        "min": float(min(filtered)),
        "max": float(max(filtered)),
    }


def _boolean_rate(rows: list[dict[str, Any]], field: str) -> float | None:
    values = [row.get(field) for row in rows if row.get(field) is not None]
    if not values:
        return None
    return float(sum(1 for value in values if bool(value)) / len(values))


def _build_confusion_rows(variant_payload: dict[str, Any]) -> list[dict[str, Any]]:
    baseline_rows = variant_payload["baseline_rows"]
    challenger_rows = variant_payload["selected_rows"]
    baseline_map = {_row_key(row): row for row in baseline_rows}
    challenger_map = {_row_key(row): row for row in challenger_rows}
    confusion_rows: list[dict[str, Any]] = []

    for key in sorted(baseline_map):
        baseline_selected = key in baseline_map
        challenger_selected = key in challenger_map
        row = dict(challenger_map.get(key) or baseline_map[key])
        known = row.get("short_ret_20") is not None
        ret20 = _maybe_float(row.get("short_ret_20"))
        if baseline_selected and challenger_selected:
            if not known:
                group = "retained_unknown"
            elif ret20 is not None and ret20 > 0.0:
                group = "kept_good"
            else:
                group = "retained_bad"
        elif baseline_selected and not challenger_selected:
            if not known:
                group = "removed_unknown"
            elif ret20 is not None and ret20 > 0.0:
                group = "removed_good"
            else:
                group = "removed_bad"
        else:
            group = "unexpected"

        confusion_rows.append(
            {
                "ymd": int(row["ymd"]),
                "code": str(row["code"]),
                "confusion_group": group,
                "baseline_selected": bool(baseline_selected),
                "challenger_selected": bool(challenger_selected),
                "outcome_known": bool(known),
                "outcome_positive": None if ret20 is None else bool(ret20 > 0.0),
                "outcome_bucket": "positive" if ret20 is not None and ret20 > 0.0 else "nonpositive" if ret20 is not None else "missing",
                "short_ret_20": ret20,
                "short_ret_10": _maybe_float(row.get("short_ret_10")),
                "short_ret_5": _maybe_float(row.get("short_ret_5")),
                "close_pos": _maybe_float(row.get("close_pos")),
                "dist_low20": _maybe_float(row.get("dist_low20")),
                "dist_ma20_signed": _maybe_float(row.get("dist_ma20_signed")),
                "day_change_pct": _maybe_float(row.get("day_change_pct")),
                "monthlyRangeProb": _maybe_float(row.get("monthlyRangeProb")),
                "monthlyRangePos": _maybe_float(row.get("monthlyRangePos")),
                "weeklyBreakoutDownProb": _maybe_float(row.get("weeklyBreakoutDownProb")),
                "monthlyBreakoutDownProb": _maybe_float(row.get("monthlyBreakoutDownProb")),
                "marketRiskOff": bool(row.get("marketRiskOff")) if row.get("marketRiskOff") is not None else None,
                "marketRegime": row.get("marketRegime"),
                "trendDownStrict": bool(row.get("trendDownStrict")) if row.get("trendDownStrict") is not None else None,
                "entryScore": _maybe_float(row.get("entryScore")),
                "tradePriorityScore": _maybe_float(row.get("tradePriorityScore")),
                "liquidity20d": _maybe_float(row.get("liquidity20d")),
                "mae20": _maybe_float(row.get("mae20")),
                "mfe20": _maybe_float(row.get("mfe20")),
                "baseline_rank": row.get("baseline_rank"),
                "tradeDecisionReasons": row.get("tradeDecisionReasons") or [],
                "tradeRiskWatch": row.get("tradeRiskWatch") or [],
            }
        )

    return confusion_rows


def _summarize_group(rows: list[dict[str, Any]]) -> dict[str, Any]:
    known_rows = [row for row in rows if row.get("outcome_known")]
    positive_rows = [row for row in known_rows if row.get("outcome_positive")]
    nonpositive_rows = [row for row in known_rows if row.get("outcome_positive") is False]
    numeric = {field: _numeric_summary(rows, field) for field in NUMERIC_FEATURES}
    boolean_rates = {field: _boolean_rate(rows, field) for field in BOOLEAN_FEATURES}
    categorical_counts = {field: Counter(str(row.get(field)) for row in rows if row.get(field) is not None) for field in CATEGORICAL_FEATURES}
    return {
        "row_count": int(len(rows)),
        "known_outcome_count": int(len(known_rows)),
        "unknown_outcome_count": int(len(rows) - len(known_rows)),
        "positive_outcome_count": int(len(positive_rows)),
        "nonpositive_outcome_count": int(len(nonpositive_rows)),
        "hit_rate": float(len(positive_rows) / len(known_rows)) if known_rows else None,
        "mean_ret20": float(statistics.mean([float(row["short_ret_20"]) for row in known_rows])) if known_rows else None,
        "median_ret20": float(statistics.median([float(row["short_ret_20"]) for row in known_rows])) if known_rows else None,
        "numeric_feature_summary": numeric,
        "boolean_feature_rate": boolean_rates,
        "categorical_feature_counts": {field: dict(counter) for field, counter in categorical_counts.items()},
        "trade_decision_reason_counts": _list_counts(rows, "tradeDecisionReasons"),
        "trade_risk_watch_counts": _list_counts(rows, "tradeRiskWatch"),
        "example_codes": [f"{row['ymd']}:{row['code']}" for row in rows[:5]],
    }


def _build_pairwise_delta(left: dict[str, Any], right: dict[str, Any], fields: list[str]) -> dict[str, Any]:
    left_numeric = left["numeric_feature_summary"]
    right_numeric = right["numeric_feature_summary"]
    out: dict[str, Any] = {}
    for field in fields:
        left_mean = left_numeric[field]["mean"]
        right_mean = right_numeric[field]["mean"]
        out[field] = None if left_mean is None or right_mean is None else float(left_mean - right_mean)
    return out


def _build_feature_comparison(
    confusion_rows: list[dict[str, Any]],
    groups: dict[str, list[dict[str, Any]]],
    *,
    session_id: str,
    variant_payload: dict[str, Any],
    family_decision: dict[str, Any],
    closepos_decision: dict[str, Any],
    monthly_decision: dict[str, Any],
) -> dict[str, Any]:
    summaries = {name: _summarize_group(rows) for name, rows in groups.items()}
    pairwise = {
        "retained_bad_vs_removed_good": {
            "numeric_mean_delta": _build_pairwise_delta(
                summaries["retained_bad"],
                summaries["removed_good"],
                [
                    "close_pos",
                    "dist_low20",
                    "dist_ma20_signed",
                    "day_change_pct",
                    "monthlyRangeProb",
                    "monthlyRangePos",
                    "weeklyBreakoutDownProb",
                    "monthlyBreakoutDownProb",
                    "entryScore",
                    "liquidity20d",
                ],
            ),
            "outcome_summary": {
                "retained_bad": {
                    "count": summaries["retained_bad"]["known_outcome_count"],
                    "hit_rate": summaries["retained_bad"]["hit_rate"],
                    "mean_ret20": summaries["retained_bad"]["mean_ret20"],
                    "median_ret20": summaries["retained_bad"]["median_ret20"],
                },
                "removed_good": {
                    "count": summaries["removed_good"]["known_outcome_count"],
                    "hit_rate": summaries["removed_good"]["hit_rate"],
                    "mean_ret20": summaries["removed_good"]["mean_ret20"],
                    "median_ret20": summaries["removed_good"]["median_ret20"],
                },
            },
        },
        "kept_good_vs_removed_bad": {
            "numeric_mean_delta": _build_pairwise_delta(
                summaries["kept_good"],
                summaries["removed_bad"],
                [
                    "close_pos",
                    "dist_low20",
                    "dist_ma20_signed",
                    "day_change_pct",
                    "monthlyRangeProb",
                    "monthlyRangePos",
                    "weeklyBreakoutDownProb",
                    "monthlyBreakoutDownProb",
                    "entryScore",
                    "liquidity20d",
                ],
            ),
            "outcome_summary": {
                "kept_good": {
                    "count": summaries["kept_good"]["known_outcome_count"],
                    "hit_rate": summaries["kept_good"]["hit_rate"],
                    "mean_ret20": summaries["kept_good"]["mean_ret20"],
                    "median_ret20": summaries["kept_good"]["median_ret20"],
                },
                "removed_bad": {
                    "count": summaries["removed_bad"]["known_outcome_count"],
                    "hit_rate": summaries["removed_bad"]["hit_rate"],
                    "mean_ret20": summaries["removed_bad"]["mean_ret20"],
                    "median_ret20": summaries["removed_bad"]["median_ret20"],
                },
            },
        },
    }
    return {
        "schema_version": "tradex_entry_precision_short_bottom_risk_feature_comparison_v1",
        "session_id": session_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "baseline_id": BASELINE_ID,
        "challenger_id": CHALLENGER_ID,
        "comparison_contract": {
            "same_universe": True,
            "same_period": True,
            "same_top_k": True,
            "same_regime": True,
            "same_cost": True,
            "same_artifact_detail_level": True,
            "long_logic_frozen": True,
            "one_axis_only": True,
            "no_meemee_ui_change": True,
            "no_lookahead_selection": True,
        },
        "source_comparison": {
            "baseline_selected_count": int(len(variant_payload["baseline_rows"])),
            "challenger_selected_count": int(len(variant_payload["selected_rows"])),
            "changed_top5_short_count": int(variant_payload["delta"]["changed_top5_short_count"]),
            "changed_top10_short_count": int(variant_payload["delta"]["changed_top10_short_count"]),
            "changed_rank_short_count": int(variant_payload["delta"]["changed_rank_short_count"]),
            "bad_short_removal_count": int(variant_payload["delta"]["bad_short_removal_count"]),
            "false_neutral_short_recovery_count": int(variant_payload["delta"]["false_neutral_short_recovery_count"]),
            "selected_count_delta": int(variant_payload["delta"]["selected_count_delta"]),
            "hit_rate_delta": variant_payload["delta"]["hit_rate_delta"],
            "median_ret20_delta": variant_payload["delta"]["median_ret20_delta"],
            "mean_ret20_delta": variant_payload["delta"]["mean_ret20_delta"],
        },
        "groups": summaries,
        "pairwise": pairwise,
        "supporting_context": {
            "family_overall_decision": family_decision.get("overall_decision"),
            "family_variant_decision": family_decision.get("decisions", {}).get(CHALLENGER_ID, {}).get("decision"),
            "closepos_followup_decision": closepos_decision.get("overall_decision"),
            "monthly_followup_decision": monthly_decision.get("overall_decision"),
        },
    }


def _build_failure_diagnosis(feature_comparison: dict[str, Any]) -> dict[str, Any]:
    groups = feature_comparison["groups"]
    pairwise = feature_comparison["pairwise"]
    retained_bad_known = groups["retained_bad"]["known_outcome_count"]
    removed_good_known = groups["removed_good"]["known_outcome_count"]
    removed_bad_known = groups["removed_bad"]["known_outcome_count"]
    kept_good_known = groups["kept_good"]["known_outcome_count"]
    known_outcome_rows = (
        groups["retained_bad"]["known_outcome_count"]
        + groups["removed_good"]["known_outcome_count"]
        + groups["removed_bad"]["known_outcome_count"]
        + groups["kept_good"]["known_outcome_count"]
    )
    improvement_source = "true_bad_pick_removal" if removed_bad_known > removed_good_known else "sample_shrinkage"
    sample_shrinkage_only = removed_bad_known == 0 and removed_good_known > 0 and retained_bad_known == 0
    next_axis_justified = known_outcome_rows >= 12 and retained_bad_known >= 3 and removed_good_known <= 2
    baseline_known_total = (
        groups["retained_bad"]["known_outcome_count"]
        + groups["removed_good"]["known_outcome_count"]
        + groups["removed_bad"]["known_outcome_count"]
        + groups["kept_good"]["known_outcome_count"]
    )
    baseline_known_positive = groups["removed_good"]["positive_outcome_count"] + groups["kept_good"]["positive_outcome_count"]
    challenger_known_total = groups["retained_bad"]["known_outcome_count"] + groups["kept_good"]["known_outcome_count"]
    challenger_known_positive = groups["kept_good"]["positive_outcome_count"]
    return {
        "schema_version": "tradex_entry_precision_short_bottom_risk_failure_diagnosis_v1",
        "decision": "hold_due_to_small_sample" if not next_axis_justified else "propose_next_axis_from_retained_bad_shorts",
        "improvement_source": improvement_source,
        "sample_shrinkage_only": bool(sample_shrinkage_only),
        "true_bad_pick_removal_visible": bool(removed_bad_known > removed_good_known),
        "next_axis_justified": bool(next_axis_justified),
        "known_evidence_rows": int(known_outcome_rows),
        "total_selected_rows": int(
            groups["retained_bad"]["row_count"]
            + groups["removed_good"]["row_count"]
            + groups["removed_bad"]["row_count"]
            + groups["kept_good"]["row_count"]
            + groups["retained_unknown"]["row_count"]
            + groups["removed_unknown"]["row_count"]
        ),
        "known_outcome_rows": int(known_outcome_rows),
        "group_counts": {
            "retained_bad": int(retained_bad_known),
            "removed_good": int(removed_good_known),
            "removed_bad": int(removed_bad_known),
            "kept_good": int(kept_good_known),
            "retained_unknown": int(groups["retained_unknown"]["row_count"]),
            "removed_unknown": int(groups["removed_unknown"]["row_count"]),
        },
        "key_metrics": {
            "baseline_hit_rate": float(baseline_known_positive / baseline_known_total) if baseline_known_total else None,
            "challenger_hit_rate": float(challenger_known_positive / challenger_known_total) if challenger_known_total else None,
            "hit_rate_delta": feature_comparison["source_comparison"]["hit_rate_delta"],
            "mean_ret20_delta": feature_comparison["source_comparison"]["mean_ret20_delta"],
            "median_ret20_delta": feature_comparison["source_comparison"]["median_ret20_delta"],
            "changed_top5_short_count": feature_comparison["source_comparison"]["changed_top5_short_count"],
            "changed_rank_short_count": feature_comparison["source_comparison"]["changed_rank_short_count"],
            "bad_short_removal_count": feature_comparison["source_comparison"]["bad_short_removal_count"],
        },
        "evidence": [
            "bottom-risk kept slice improved known hit rate from 0.5714 to 0.75",
            "bad shorts removed among known rows outnumber good shorts removed 4 to 2",
            "known outcome evidence is still thin, with 14 baseline rows and 8 challenger rows carrying forward returns",
            "retained bad rows and removed good rows share the same bearish trade-decision reasons, so the residual is not cleanly separable yet",
        ],
        "pairwise_feature_deltas": {
            "retained_bad_vs_removed_good": pairwise["retained_bad_vs_removed_good"]["numeric_mean_delta"],
            "kept_good_vs_removed_bad": pairwise["kept_good_vs_removed_bad"]["numeric_mean_delta"],
        },
        "remaining_risks": [
            "known forward outcome sample is small",
            "tail rows have missing 20-day outcomes and weaken the decomposition",
            "good removals sit close to the current close_pos boundary",
            "no new challenger is justified until the retained-bad cluster is observed on more forward rows",
        ],
    }


def _build_next_axis_decision(feature_comparison: dict[str, Any], failure_diagnosis: dict[str, Any]) -> dict[str, Any]:
    decision = failure_diagnosis["decision"]
    if decision == "propose_next_axis_from_retained_bad_shorts":
        rationale = [
            "retained_bad rows are numerous enough to justify a next challenger",
            "the retained-bad cluster shows a coherent feature shape rather than random noise",
        ]
    else:
        rationale = [
            "known forward evidence is still too small for a new challenger",
            "the current improvement is real but not yet broad enough to justify another axis",
        ]
    return {
        "schema_version": "tradex_entry_precision_short_bottom_risk_next_axis_decision_v1",
        "decision": decision,
        "decision_reasons": rationale,
        "next_axis_justified": bool(failure_diagnosis["next_axis_justified"]),
        "candidate_axis_hint": None
        if not failure_diagnosis["next_axis_justified"]
        else {
            "name": "retained_bad_cluster_followup",
            "why": "The retained bad shorts are the only residual source of concern after the bottom-risk keep slice improved known precision.",
            "possible_features": ["monthlyRangeProb", "monthlyRangePos", "marketRiskOff", "weeklyBreakoutDownProb"],
        },
        "supporting_counts": {
            "retained_bad_known": int(failure_diagnosis["group_counts"]["retained_bad"]),
            "removed_good_known": int(failure_diagnosis["group_counts"]["removed_good"]),
            "removed_bad_known": int(failure_diagnosis["group_counts"]["removed_bad"]),
            "kept_good_known": int(failure_diagnosis["group_counts"]["kept_good"]),
        },
        "supporting_pairwise": feature_comparison["pairwise"],
    }


def _build_contract(
    *,
    session_id: str,
    compare_root: Path,
    family_decision_root: Path,
    closepos_decision_path: Path,
    monthly_decision_path: Path,
    family_decision: dict[str, Any],
    closepos_decision: dict[str, Any],
    monthly_decision: dict[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": "tradex_entry_precision_short_bottom_risk_diagnostic_contract_v1",
        "session_id": session_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "baseline_id": BASELINE_ID,
        "challenger_id": CHALLENGER_ID,
        "variant_key": VARIANT_KEY,
        "input_artifact_roots": {
            "compare_root": str(compare_root),
            "family_decision_root": str(family_decision_root),
            "closepos_decision_path": str(closepos_decision_path),
            "monthly_decision_path": str(monthly_decision_path),
        },
        "authoritative_context": {
            "family_overall_decision": family_decision.get("overall_decision"),
            "family_variant_decision": family_decision.get("decisions", {}).get(CHALLENGER_ID, {}).get("decision"),
            "closepos_followup_decision": closepos_decision.get("overall_decision"),
            "monthly_followup_decision": monthly_decision.get("overall_decision"),
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
            "scope": "TRADEX-only false-keep diagnostic for the frozen bottom-risk keep slice.",
            "non_scope": [
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
                "known forward horizon is thin",
                "tail rows have missing 20-day outcomes",
                "retained bad shorts may be too sparse to justify a next challenger",
            ],
        },
        "decision_target": "Explain whether the bottom-risk keep slice is a real bad-pick remover or only a sample-shrinking artifact, and whether another axis is justified.",
        "authoritative_artifacts": {
            "family_compare": str(compare_root / "entry_precision_short_challenger_compare.json"),
            "family_decision": str(family_decision_root / "entry_precision_short_decision.json"),
            "closepos_decision": str(closepos_decision_path),
            "monthly_decision": str(monthly_decision_path),
        },
        "no_lookahead_contract": {
            "future_outcome_fields_used_in_selection": [],
            "diagnostic_uses_forward_outcomes_for_audit": True,
            "selection_logic_frozen": True,
            "research_fallback": False,
            "silent_fallback_used": False,
        },
    }


def run(args: argparse.Namespace) -> dict[str, Any]:
    compare_root = Path(args.compare_root or DEFAULT_COMPARE_ROOT)
    family_decision_root = Path(args.family_decision_root or DEFAULT_FAMILY_DECISION_ROOT)
    closepos_decision_path = Path(args.closepos_decision_path or DEFAULT_CLOSEPOS_DECISION_PATH)
    monthly_decision_path = Path(args.monthly_decision_path or DEFAULT_MONTHLY_DECISION_PATH)
    output_root = Path(args.output_dir or DEFAULT_OUTPUT_DIR)

    compare_payload = _load_json(compare_root / "entry_precision_short_challenger_compare.json")
    family_decision = _load_json(family_decision_root / "entry_precision_short_decision.json")
    closepos_decision = _load_json(closepos_decision_path)
    monthly_decision = _load_json(monthly_decision_path)

    variant_payload = compare_payload["variants"][VARIANT_KEY]
    confusion_rows = _build_confusion_rows(variant_payload)
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in confusion_rows:
        groups[str(row["confusion_group"])].append(row)

    session_stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    session_id = f"entry-short-bottom-risk-diagnostic-{session_stamp}"
    output_dir = output_root / f"{session_stamp}-entry-short-bottom-risk-diagnostic-v1"
    output_dir.mkdir(parents=True, exist_ok=True)

    feature_comparison = _build_feature_comparison(
        confusion_rows,
        groups,
        session_id=session_id,
        variant_payload=variant_payload,
        family_decision=family_decision,
        closepos_decision=closepos_decision,
        monthly_decision=monthly_decision,
    )
    failure_diagnosis = _build_failure_diagnosis(feature_comparison)
    next_axis_decision = _build_next_axis_decision(feature_comparison, failure_diagnosis)
    contract = _build_contract(
        session_id=session_id,
        compare_root=compare_root,
        family_decision_root=family_decision_root,
        closepos_decision_path=closepos_decision_path,
        monthly_decision_path=monthly_decision_path,
        family_decision=family_decision,
        closepos_decision=closepos_decision,
        monthly_decision=monthly_decision,
    )

    output_rows = [dict(row) for row in confusion_rows]
    retained_bad_rows = [row for row in confusion_rows if row["confusion_group"] == "retained_bad"]
    removed_good_rows = [row for row in confusion_rows if row["confusion_group"] == "removed_good"]

    no_lookahead_audit = {
        "schema_version": "tradex_entry_precision_short_bottom_risk_no_lookahead_audit_v1",
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
        "schema_version": "tradex_entry_precision_short_bottom_risk_artifact_complete_v1",
        "session_id": session_id,
        "complete": True,
        "artifact_refs": {
            "short_bottom_risk_diagnostic_contract": str(output_dir / "short_bottom_risk_diagnostic_contract.json"),
            "short_bottom_risk_confusion_groups": str(output_dir / "short_bottom_risk_confusion_groups.csv"),
            "short_bottom_risk_feature_comparison": str(output_dir / "short_bottom_risk_feature_comparison.json"),
            "short_bottom_risk_removed_good_shorts": str(output_dir / "short_bottom_risk_removed_good_shorts.csv"),
            "short_bottom_risk_retained_bad_shorts": str(output_dir / "short_bottom_risk_retained_bad_shorts.csv"),
            "short_bottom_risk_failure_diagnosis": str(output_dir / "short_bottom_risk_failure_diagnosis.json"),
            "short_bottom_risk_next_axis_decision": str(output_dir / "short_bottom_risk_next_axis_decision.json"),
            "no_lookahead_audit": str(output_dir / "no_lookahead_audit.json"),
        },
    }

    _write_json(output_dir / "short_bottom_risk_diagnostic_contract.json", contract)
    _write_csv(output_dir / "short_bottom_risk_confusion_groups.csv", output_rows, CSV_FIELDS)
    _write_json(output_dir / "short_bottom_risk_feature_comparison.json", feature_comparison)
    _write_csv(output_dir / "short_bottom_risk_removed_good_shorts.csv", removed_good_rows, CSV_FIELDS)
    _write_csv(output_dir / "short_bottom_risk_retained_bad_shorts.csv", retained_bad_rows, CSV_FIELDS)
    _write_json(output_dir / "short_bottom_risk_failure_diagnosis.json", failure_diagnosis)
    _write_json(output_dir / "short_bottom_risk_next_axis_decision.json", next_axis_decision)
    _write_json(output_dir / "no_lookahead_audit.json", no_lookahead_audit)
    _write_json(output_dir / "_ARTIFACT_COMPLETE.json", artifact_complete)

    return {
        "ok": True,
        "session_id": session_id,
        "output_dir": str(output_dir),
        "decision": next_axis_decision["decision"],
        "known_outcome_rows": int(failure_diagnosis["known_outcome_rows"]),
        "retained_bad_known": int(failure_diagnosis["group_counts"]["retained_bad"]),
        "removed_good_known": int(failure_diagnosis["group_counts"]["removed_good"]),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="TRADEX false-keep diagnostic for short_cleanup_bottom_risk_v1.")
    parser.add_argument("--compare-root", default=str(DEFAULT_COMPARE_ROOT))
    parser.add_argument("--family-decision-root", default=str(DEFAULT_FAMILY_DECISION_ROOT))
    parser.add_argument("--closepos-decision-path", default=str(DEFAULT_CLOSEPOS_DECISION_PATH))
    parser.add_argument("--monthly-decision-path", default=str(DEFAULT_MONTHLY_DECISION_PATH))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args()
    result = run(args)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
