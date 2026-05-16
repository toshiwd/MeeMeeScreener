from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd


AXIS_ID = "selected_pattern_family_validation_v1"
DEFAULT_RUN_ID = "20260514T150000Z-selected-pattern-family-validation-v1"
DEFAULT_OUTPUT_PARENT = Path(r"G:\Tradex\selected_pattern_family_validation_v1")
DEFAULT_PORTFOLIO_ROOT = Path(
    r"G:\Tradex\multi_pattern_candidate_generation_portfolio_v1"
    r"\20260514T140000Z-multi-pattern-candidate-generation-portfolio-v1"
)

REQUIRED_OUTPUTS = [
    "validation_contract.json",
    "selected_family_readback.json",
    "family_validation_report.json",
    "family_monthly_stability_report.json",
    "family_overlap_report.json",
    "top5_candidate_pool_readiness_report.json",
    "selected_family_validation_decision.json",
    "next_axis_recommendation.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
]

SIGNAL_COLUMNS = {
    "pre_ret20_state",
    "pre_ret5_state",
    "pre_ma20_path_state",
    "pre_ma60_context_state",
    "pre_candle_energy_state",
    "pre_wick_warning_state",
    "pre_volume_state",
    "pre_compression_state",
    "weekly_prior_state",
    "monthly_prior_state",
    "event_daily_ret20_state",
    "event_daily_candle_state",
}
LABEL_COLUMNS = {"ret20_fwd", "mfe20", "mae20", "win20", "severe_loss20", "entry_next_open"}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--portfolio-root", type=Path, default=DEFAULT_PORTFOLIO_ROOT)
    parser.add_argument("--output-parent", type=Path, default=DEFAULT_OUTPUT_PARENT)
    parser.add_argument("--run-id", default=DEFAULT_RUN_ID)
    args = parser.parse_args()
    run_selected_pattern_family_validation_v1(
        portfolio_root=args.portfolio_root,
        output_parent=args.output_parent,
        run_id=args.run_id,
    )
    return 0


def run_selected_pattern_family_validation_v1(
    *,
    portfolio_root: Path = DEFAULT_PORTFOLIO_ROOT,
    output_parent: Path = DEFAULT_OUTPUT_PARENT,
    run_id: str = DEFAULT_RUN_ID,
) -> dict[str, Any]:
    output_root = output_parent / run_id
    output_root.mkdir(parents=True, exist_ok=True)
    portfolio_root = portfolio_root.resolve()
    selected_payload = _read_json(portfolio_root / "selected_pattern_families_for_validation.json")
    portfolio_decision = _read_json(portfolio_root / "research_decision.json")
    selected = selected_payload.get("selected_pattern_families") or []
    source_artifact = Path(str(selected[0]["source_artifact"])) if selected else None
    pre_strength_root = source_artifact.parent if source_artifact else None
    ledger_path = pre_strength_root / "pre_strength_event_ledger.jsonl" if pre_strength_root else None
    source_contract = _read_json(pre_strength_root / "evaluation_contract.json") if pre_strength_root else {}
    feature_audit = _read_json(pre_strength_root / "feature_availability_audit.json") if pre_strength_root else {}
    events = _read_jsonl_frame(ledger_path) if ledger_path else pd.DataFrame()

    validation_contract = _validation_contract(portfolio_root, pre_strength_root, source_contract, feature_audit, selected)
    selected_readback = _selected_readback(selected_payload, portfolio_decision)
    family_rows = [_validate_family(row, events) for row in selected]
    monthly_report = _monthly_stability_report(family_rows)
    overlap_report = _overlap_report(family_rows)
    top5_readiness = _top5_readiness_report(family_rows)
    decision_payload = _family_validation_decision(family_rows, overlap_report, top5_readiness)
    next_axis = _next_axis(decision_payload)
    research_decision = {
        "schema_version": "tradex_selected_pattern_family_validation_research_decision_v1",
        "axis_id": AXIS_ID,
        "decision": decision_payload["decision"],
        "decision_reason": decision_payload["decision_reason"],
        "keep_family_count": decision_payload["keep_family_count"],
        "hold_family_count": decision_payload["hold_family_count"],
        "drop_family_count": decision_payload["drop_family_count"],
        "selected_next_validation_families": decision_payload["selected_next_validation_families"],
        "top5_direct_branching_test_run": False,
        "candidate_generation_probe_required": True,
        "activation_allowed": False,
        "production_ranking_changed": False,
        "meemee_reflectable": False,
        "publish_created": False,
        "candidate_scoring_created": False,
        "silent_fallback_used": False,
        "generated_at_utc": _utc_now(),
    }
    _write_json(output_root / "validation_contract.json", validation_contract)
    _write_json(output_root / "selected_family_readback.json", selected_readback)
    _write_json(output_root / "family_validation_report.json", {"schema_version": "tradex_family_validation_report_v1", "axis_id": AXIS_ID, "rows": family_rows})
    _write_json(output_root / "family_monthly_stability_report.json", monthly_report)
    _write_json(output_root / "family_overlap_report.json", overlap_report)
    _write_json(output_root / "top5_candidate_pool_readiness_report.json", top5_readiness)
    _write_json(output_root / "selected_family_validation_decision.json", decision_payload)
    _write_json(output_root / "next_axis_recommendation.json", next_axis)
    _write_json(output_root / "research_decision.json", research_decision)
    complete = _artifact_complete(output_root, research_decision)
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    return {
        "output_root": str(output_root),
        "validation_contract": validation_contract,
        "family_validation_report": {"rows": family_rows},
        "family_overlap_report": overlap_report,
        "top5_candidate_pool_readiness_report": top5_readiness,
        "selected_family_validation_decision": decision_payload,
        "next_axis_recommendation": next_axis,
        "research_decision": research_decision,
        "artifact_complete": complete,
    }


def _validation_contract(
    portfolio_root: Path,
    pre_strength_root: Path | None,
    source_contract: Mapping[str, Any],
    feature_audit: Mapping[str, Any],
    selected: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": "tradex_selected_pattern_family_validation_contract_v1",
        "axis_id": AXIS_ID,
        "boundary": "TRADEX-only",
        "source_portfolio_root": str(portfolio_root),
        "source_pre_strength_root": str(pre_strength_root) if pre_strength_root else None,
        "selected_family_count": len(selected),
        "fixed_evaluation_conditions": {
            "same_universe": (source_contract.get("same_condition_controls") or {}).get("same_universe_source"),
            "same_period": (source_contract.get("same_condition_controls") or {}).get("same_period"),
            "same_cost_slippage": (source_contract.get("same_condition_controls") or {}).get("same_cost_slippage"),
            "same_artifact_detail_level": (source_contract.get("same_condition_controls") or {}).get("artifact_detail_level"),
            "entry_convention": source_contract.get("entry_convention_for_evaluation"),
        },
        "future_label_policy": {
            "future_labels_used_for_pattern_keys": False,
            "future_labels_used_for_evaluation": True,
            "source_feature_audit_used_future_labels_in_pattern_keys": feature_audit.get("used_future_labels_in_pattern_keys"),
            "pattern_key_columns": sorted(_pattern_key_columns(selected)),
            "label_columns": sorted(LABEL_COLUMNS),
            "pattern_key_label_overlap": sorted(_pattern_key_columns(selected).intersection(LABEL_COLUMNS)),
        },
        "not_changed": _not_changed(),
        "silent_fallback_used": False,
    }


def _selected_readback(selected_payload: Mapping[str, Any], portfolio_decision: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "tradex_selected_family_readback_v1",
        "axis_id": AXIS_ID,
        "source_portfolio_decision": portfolio_decision.get("decision"),
        "source_selected_count": selected_payload.get("selection_count"),
        "teppan_policy": selected_payload.get("teppan_policy"),
        "selected_pattern_families": selected_payload.get("selected_pattern_families") or [],
    }


def _validate_family(selected: Mapping[str, Any], events: pd.DataFrame) -> dict[str, Any]:
    family_group, conditions = _parse_pre_strength_family_id(str(selected["family_id"]))
    matched = _apply_conditions(events, conditions)
    monthly = _monthly_rows(matched)
    baseline = _metrics(events)
    metrics = _metrics(matched)
    deltas = {
        "avg_ret20_delta_vs_all_events": _none_if_missing(metrics["avg_ret20"], baseline["avg_ret20"], lambda a, b: a - b),
        "win_rate20_delta_vs_all_events": _none_if_missing(metrics["win_rate20"], baseline["win_rate20"], lambda a, b: a - b),
        "severe_loss_rate20_delta_vs_all_events": _none_if_missing(metrics["severe_loss_rate20"], baseline["severe_loss_rate20"], lambda a, b: a - b),
    }
    gates = {
        "event_count_pass": metrics["event_count"] >= 120,
        "month_count_pass": metrics["month_count"] >= 18,
        "positive_month_rate_pass": (metrics["positive_month_rate20"] or 0.0) >= 0.55,
        "avg_ret_pass": (metrics["avg_ret20"] or 0.0) >= 0.01,
        "win_rate_pass": (metrics["win_rate20"] or 0.0) >= 0.55,
        "severe_loss_pass": _value_or(metrics["severe_loss_rate20"], 1.0)
        <= min(0.25, _value_or(baseline["severe_loss_rate20"], 0.25) + 0.02),
        "baseline_avg_ret_delta_pass": (deltas["avg_ret20_delta_vs_all_events"] or -1.0) > 0.0,
        "baseline_win_rate_delta_pass": (deltas["win_rate20_delta_vs_all_events"] or -1.0) > 0.0,
    }
    pass_count = sum(1 for value in gates.values() if value)
    if all(gates.values()):
        decision = "keep_for_candidate_generation_probe"
        reason = "frequency_quality_and_monthly_stability_pass"
    elif pass_count >= 6 and gates["event_count_pass"] and gates["severe_loss_pass"]:
        decision = "hold_for_candidate_generation_probe_with_caveat"
        reason = "mostly_passed_but_requires_probe_or_stability_confirmation"
    else:
        decision = "drop_from_selected_validation"
        reason = "quality_frequency_or_loss_gate_failed"
    return {
        "family_id": selected["family_id"],
        "family_group": family_group,
        "display_name": selected.get("display_name"),
        "mechanism": selected.get("mechanism"),
        "conditions": conditions,
        "source_artifact": selected.get("source_artifact"),
        "source_portfolio_priority_score": selected.get("portfolio_priority_score"),
        "baseline_all_events_metrics": baseline,
        "validated_metrics": metrics,
        "delta_vs_all_events": deltas,
        "gate_results": gates,
        "gate_pass_count": pass_count,
        "monthly_rows": monthly,
        "matched_event_keys": _matched_event_keys(matched),
        "example_events": _example_events(matched),
        "validation_decision": decision,
        "validation_reason": reason,
        "top5_direct_branching_test_run": False,
        "candidate_generation_probe_required": True,
    }


def _family_validation_decision(
    family_rows: Sequence[Mapping[str, Any]],
    overlap_report: Mapping[str, Any],
    top5_readiness: Mapping[str, Any],
) -> dict[str, Any]:
    keep = [row for row in family_rows if row["validation_decision"] == "keep_for_candidate_generation_probe"]
    hold = [row for row in family_rows if row["validation_decision"] == "hold_for_candidate_generation_probe_with_caveat"]
    drop = [row for row in family_rows if row["validation_decision"] == "drop_from_selected_validation"]
    selected_next = keep[:3] if keep else hold[:1]
    if keep and overlap_report["selected_overlap_assessment"] == "acceptable":
        decision = "keep_for_candidate_generation_probe"
        reason = "at_least_one_selected_family_passed_validation_without_high_overlap"
    elif keep or hold:
        decision = "hold_for_candidate_generation_probe_design"
        reason = "candidate_family_evidence_exists_but_probe_scope_or_overlap_requires_care"
    else:
        decision = "drop_selected_pattern_families"
        reason = "no_selected_family_passed_validation_gates"
    return {
        "schema_version": "tradex_selected_family_validation_decision_v1",
        "axis_id": AXIS_ID,
        "decision": decision,
        "decision_reason": reason,
        "keep_family_count": len(keep),
        "hold_family_count": len(hold),
        "drop_family_count": len(drop),
        "selected_next_validation_families": [_decision_view(row) for row in selected_next],
        "top5_candidate_pool_readiness": top5_readiness["readiness_decision"],
        "top5_direct_branching_test_run": False,
        "candidate_generation_probe_required": True,
        "activation_allowed": False,
        "meemee_reflectable": False,
    }


def _next_axis(decision: Mapping[str, Any]) -> dict[str, Any]:
    if decision["decision"] == "keep_for_candidate_generation_probe":
        next_axis = "pre_strength_selected_family_candidate_generation_probe_v1"
    elif decision["decision"] == "hold_for_candidate_generation_probe_design":
        next_axis = "selected_pattern_family_probe_contract_design_v1"
    else:
        next_axis = "multi_pattern_candidate_generation_portfolio_redesign_v1"
    return {
        "schema_version": "tradex_selected_pattern_family_validation_next_axis_recommendation_v1",
        "axis_id": AXIS_ID,
        "decision": decision["decision"],
        "next": next_axis,
        "activation_allowed": False,
        "meemee_reflection_allowed": False,
    }


def _monthly_stability_report(family_rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": "tradex_family_monthly_stability_report_v1",
        "axis_id": AXIS_ID,
        "rows": [
            {
                "family_id": row["family_id"],
                "month_count": row["validated_metrics"]["month_count"],
                "positive_month_rate20": row["validated_metrics"]["positive_month_rate20"],
                "worst_month_mean_ret20": _safe_min([month["mean_ret20"] for month in row["monthly_rows"]]),
                "best_month_mean_ret20": _safe_max([month["mean_ret20"] for month in row["monthly_rows"]]),
                "monthly_stability_pass": row["gate_results"]["positive_month_rate_pass"],
            }
            for row in family_rows
        ],
    }


def _overlap_report(family_rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    pairs = []
    for left, right in combinations(family_rows, 2):
        left_keys = _event_key_set(left)
        right_keys = _event_key_set(right)
        union = left_keys | right_keys
        jaccard = 0.0 if not union else len(left_keys & right_keys) / len(union)
        pairs.append(
            {
                "left_family_id": left["family_id"],
                "right_family_id": right["family_id"],
                "event_overlap_jaccard": jaccard,
                "shared_event_count": len(left_keys & right_keys),
                "overlap_level": "high" if jaccard >= 0.50 else "medium" if jaccard >= 0.25 else "low",
            }
        )
    return {
        "schema_version": "tradex_family_overlap_report_v1",
        "axis_id": AXIS_ID,
        "pair_count": len(pairs),
        "pairs": pairs,
        "selected_overlap_assessment": "acceptable" if all(row["overlap_level"] != "high" for row in pairs) else "needs_deduplication",
    }


def _top5_readiness_report(family_rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    rows = []
    for row in family_rows:
        metrics = row["validated_metrics"]
        ready = (
            row["validation_decision"] == "keep_for_candidate_generation_probe"
            and metrics["event_count"] >= 120
            and _value_or(metrics["severe_loss_rate20"], 1.0) <= 0.25
        )
        rows.append(
            {
                "family_id": row["family_id"],
                "readiness": "probe_ready" if ready else "probe_hold",
                "reason": row["validation_reason"],
                "top5_direct_branching_test_run": False,
                "candidate_generation_probe_required": True,
            }
        )
    return {
        "schema_version": "tradex_top5_candidate_pool_readiness_report_v1",
        "axis_id": AXIS_ID,
        "readiness_decision": "candidate_generation_probe_ready" if any(row["readiness"] == "probe_ready" for row in rows) else "hold",
        "rows": rows,
    }


def _metrics(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return {
            "event_count": 0,
            "symbol_count": 0,
            "month_count": 0,
            "avg_ret20": None,
            "median_ret20": None,
            "win_rate20": None,
            "avg_mfe20": None,
            "avg_mae20": None,
            "profit_factor20": None,
            "severe_loss_rate20": None,
            "positive_month_rate20": None,
        }
    return {
        "event_count": int(len(frame)),
        "symbol_count": int(frame["code"].astype(str).nunique()),
        "month_count": int(frame["event_month"].astype(str).nunique()),
        "avg_ret20": _safe_float(pd.to_numeric(frame["ret20_fwd"], errors="coerce").mean()),
        "median_ret20": _safe_float(pd.to_numeric(frame["ret20_fwd"], errors="coerce").median()),
        "win_rate20": _safe_float(frame["win20"].astype(bool).mean()),
        "avg_mfe20": _safe_float(pd.to_numeric(frame["mfe20"], errors="coerce").mean()),
        "avg_mae20": _safe_float(pd.to_numeric(frame["mae20"], errors="coerce").mean()),
        "profit_factor20": _profit_factor(frame["ret20_fwd"]),
        "severe_loss_rate20": _safe_float(frame["severe_loss20"].astype(bool).mean()),
        "positive_month_rate20": _positive_month_rate(frame),
    }


def _monthly_rows(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    rows = []
    for month, group in frame.groupby("event_month", sort=True):
        rows.append(
            {
                "event_month": str(month),
                "event_count": int(len(group)),
                "mean_ret20": _safe_float(pd.to_numeric(group["ret20_fwd"], errors="coerce").mean()),
                "win_rate20": _safe_float(group["win20"].astype(bool).mean()),
                "severe_loss_rate20": _safe_float(group["severe_loss20"].astype(bool).mean()),
            }
        )
    return rows


def _positive_month_rate(frame: pd.DataFrame) -> float | None:
    monthly = _monthly_rows(frame)
    if not monthly:
        return None
    return _safe_float(sum(1 for row in monthly if (row["mean_ret20"] or 0.0) > 0.0) / len(monthly))


def _profit_factor(values: pd.Series) -> float | None:
    numeric = pd.to_numeric(values, errors="coerce")
    gains = numeric[numeric > 0].sum()
    losses = -numeric[numeric < 0].sum()
    if not math.isfinite(gains) or not math.isfinite(losses) or losses == 0:
        return None
    return _safe_float(gains / losses)


def _parse_pre_strength_family_id(family_id: str) -> tuple[str, dict[str, str]]:
    parts = family_id.split("::", 2)
    if len(parts) != 3 or parts[0] != "pre_strength":
        raise ValueError(f"unsupported selected family id: {family_id}")
    conditions = {}
    for item in parts[2].split("|"):
        key, value = item.split("=", 1)
        conditions[key] = value
    if set(conditions).intersection(LABEL_COLUMNS):
        raise ValueError(f"future label condition found in family id: {family_id}")
    return parts[1], conditions


def _apply_conditions(events: pd.DataFrame, conditions: Mapping[str, str]) -> pd.DataFrame:
    if events.empty:
        return events.copy()
    mask = pd.Series(True, index=events.index)
    for key, value in conditions.items():
        if key not in events.columns:
            mask &= False
        else:
            mask &= events[key].astype(str).eq(str(value))
    return events.loc[mask].copy()


def _pattern_key_columns(selected: Sequence[Mapping[str, Any]]) -> set[str]:
    columns: set[str] = set()
    for row in selected:
        _family, conditions = _parse_pre_strength_family_id(str(row["family_id"]))
        columns.update(conditions)
    return columns


def _event_key_set(row: Mapping[str, Any]) -> set[str]:
    return set(row.get("matched_event_keys") or [])


def _matched_event_keys(frame: pd.DataFrame) -> list[str]:
    if frame.empty:
        return []
    keys = [f"{record['code']}::{record['event_date']}" for record in frame[["code", "event_date"]].to_dict("records")]
    return sorted(set(keys))


def _example_events(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    columns = ["code", "event_date", "event_month", "ret20_fwd", "win20", "severe_loss20"]
    return frame.sort_values(["event_date", "code"])[columns].head(12).to_dict("records")


def _decision_view(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "family_id": row["family_id"],
        "family_group": row["family_group"],
        "validation_decision": row["validation_decision"],
        "event_count": row["validated_metrics"]["event_count"],
        "month_count": row["validated_metrics"]["month_count"],
        "avg_ret20": row["validated_metrics"]["avg_ret20"],
        "win_rate20": row["validated_metrics"]["win_rate20"],
        "severe_loss_rate20": row["validated_metrics"]["severe_loss_rate20"],
    }


def _none_if_missing(left: Any, right: Any, fn: Any) -> float | None:
    if left is None or right is None:
        return None
    return _safe_float(fn(float(left), float(right)))


def _value_or(value: Any, default: float) -> float:
    parsed = _safe_float(value)
    return default if parsed is None else parsed


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _safe_min(values: Sequence[float | None]) -> float | None:
    cleaned = [value for value in values if value is not None]
    return min(cleaned) if cleaned else None


def _safe_max(values: Sequence[float | None]) -> float | None:
    cleaned = [value for value in values if value is not None]
    return max(cleaned) if cleaned else None


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    return data if isinstance(data, dict) else {}


def _read_jsonl_frame(path: Path) -> pd.DataFrame:
    rows = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return pd.DataFrame(rows)


def _artifact_complete(output_root: Path, decision: Mapping[str, Any]) -> dict[str, Any]:
    presence = {name: (output_root / name).exists() for name in REQUIRED_OUTPUTS if name != "_ARTIFACT_COMPLETE.json"}
    presence["_ARTIFACT_COMPLETE.json"] = True
    return {
        "schema_version": "tradex_selected_pattern_family_validation_artifact_complete_v1",
        "axis_id": AXIS_ID,
        "decision": decision.get("decision"),
        "complete": all(presence.values()),
        "required_outputs": REQUIRED_OUTPUTS,
        "present_outputs": presence,
        "output_root": str(output_root),
        "silent_fallback_used": False,
    }


def _not_changed() -> list[str]:
    return [
        "production_ranking",
        "MeeMee_runtime",
        "runtime_DuckDB",
        "display_score",
        "frontend_backend_ui_api",
        "publish_registry",
        "teppan_watch_policy",
        "boost_value",
        "loss_guard",
        "pattern_definitions",
    ]


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(dict(payload)), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
