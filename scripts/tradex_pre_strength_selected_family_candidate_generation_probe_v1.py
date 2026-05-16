from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd


AXIS_ID = "pre_strength_selected_family_candidate_generation_probe_v1"
DEFAULT_RUN_ID = "20260514T160000Z-pre-strength-selected-family-candidate-generation-probe-v1"
DEFAULT_OUTPUT_PARENT = Path(r"G:\Tradex\pre_strength_selected_family_candidate_generation_probe_v1")
DEFAULT_VALIDATION_ROOT = Path(
    r"G:\Tradex\selected_pattern_family_validation_v1"
    r"\20260514T150000Z-selected-pattern-family-validation-v1"
)

TOP5_K = 5
TOP10_K = 10
TOP3_K = 3
BIG_WINNER_RET20 = 0.10

REQUIRED_OUTPUTS = [
    "probe_contract.json",
    "source_family_readback.json",
    "candidate_generation_variants.json",
    "candidate_generation_ledger.jsonl",
    "top5_candidate_pool_report.json",
    "top3_guardrail_report.json",
    "baseline_comparison_report.json",
    "family_contribution_breakdown.json",
    "oracle_reference_report.json",
    "candidate_generation_probe_decision.json",
    "next_axis_recommendation.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
]

LABEL_COLUMNS = {"ret20_fwd", "mfe20", "mae20", "win20", "severe_loss20", "entry_next_open"}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--validation-root", type=Path, default=DEFAULT_VALIDATION_ROOT)
    parser.add_argument("--output-parent", type=Path, default=DEFAULT_OUTPUT_PARENT)
    parser.add_argument("--run-id", default=DEFAULT_RUN_ID)
    args = parser.parse_args()
    run_pre_strength_selected_family_candidate_generation_probe_v1(
        validation_root=args.validation_root,
        output_parent=args.output_parent,
        run_id=args.run_id,
    )
    return 0


def run_pre_strength_selected_family_candidate_generation_probe_v1(
    *,
    validation_root: Path = DEFAULT_VALIDATION_ROOT,
    output_parent: Path = DEFAULT_OUTPUT_PARENT,
    run_id: str = DEFAULT_RUN_ID,
) -> dict[str, Any]:
    output_root = output_parent / run_id
    output_root.mkdir(parents=True, exist_ok=True)
    validation_root = validation_root.resolve()
    validation_decision = _read_json(validation_root / "selected_family_validation_decision.json")
    validation_contract = _read_json(validation_root / "validation_contract.json")
    family_report = _read_json(validation_root / "family_validation_report.json")
    keep_families = validation_decision.get("selected_next_validation_families") or []
    source_pre_strength_root = Path(validation_contract["source_pre_strength_root"])
    events = _read_jsonl_frame(source_pre_strength_root / "pre_strength_event_ledger.jsonl")
    events = _prepare_events(events)

    variants = _build_variants(keep_families, family_report)
    ledger_rows: list[dict[str, Any]] = []
    top5_rows: list[dict[str, Any]] = []
    top3_rows: list[dict[str, Any]] = []
    oracle_rows: list[dict[str, Any]] = []
    contribution_rows: list[dict[str, Any]] = []
    for variant in variants:
        probe = _evaluate_variant(events, variant)
        ledger_rows.extend(probe["ledger_rows"])
        top5_rows.append(probe["top5_metrics"])
        top3_rows.append(probe["top3_metrics"])
        oracle_rows.append(probe["oracle_reference"])
        contribution_rows.append(probe["family_contribution"])

    top5_report = {
        "schema_version": "tradex_pre_strength_selected_family_top5_candidate_pool_report_v1",
        "axis_id": AXIS_ID,
        "evaluation_role": "primary_top5_candidate_pool_quality",
        "final_max3_selection_owner": "human_user",
        "forced_top3_is_primary": False,
        "candidate_pool_size_target": TOP5_K,
        "rows": top5_rows,
    }
    top3_report = {
        "schema_version": "tradex_pre_strength_selected_family_top3_guardrail_report_v1",
        "axis_id": AXIS_ID,
        "evaluation_role": "secondary_top3_guardrail",
        "rows": top3_rows,
    }
    comparison = _comparison_report(top5_rows, top3_rows)
    contribution = {
        "schema_version": "tradex_pre_strength_family_contribution_breakdown_v1",
        "axis_id": AXIS_ID,
        "rows": contribution_rows,
    }
    oracle = {
        "schema_version": "tradex_pre_strength_family_oracle_reference_report_v1",
        "axis_id": AXIS_ID,
        "evaluation_only_reference": True,
        "future_labels_used_for_oracle": True,
        "rows": oracle_rows,
    }
    decision = _decision(comparison)
    next_axis = _next_axis(decision)
    contract = _contract(validation_root, validation_contract, keep_families)
    readback = _readback(validation_decision, validation_contract)
    variant_payload = {"schema_version": "tradex_pre_strength_candidate_generation_variants_v1", "axis_id": AXIS_ID, "variants": variants}
    research_decision = {
        "schema_version": "tradex_pre_strength_selected_family_candidate_generation_probe_research_decision_v1",
        "axis_id": AXIS_ID,
        "decision": decision["decision"],
        "decision_reason": decision["decision_reason"],
        "best_variant_id": decision["best_variant_id"],
        "variant_decisions": decision["variant_decisions"],
        "primary_metric_scope": "top5_candidate_pool_quality",
        "secondary_metric_scope": "top3_operating_guardrail",
        "final_max3_selection_owner": "human_user",
        "forced_top3_is_primary": False,
        "activation_allowed": False,
        "production_ranking_changed": False,
        "meemee_reflectable": False,
        "publish_created": False,
        "candidate_scoring_created": False,
        "ranking_objective_created": False,
        "threshold_policy_created": False,
        "silent_fallback_used": False,
        "generated_at_utc": _utc_now(),
    }

    _write_json(output_root / "probe_contract.json", contract)
    _write_json(output_root / "source_family_readback.json", readback)
    _write_json(output_root / "candidate_generation_variants.json", variant_payload)
    _write_jsonl(output_root / "candidate_generation_ledger.jsonl", ledger_rows)
    _write_json(output_root / "top5_candidate_pool_report.json", top5_report)
    _write_json(output_root / "top3_guardrail_report.json", top3_report)
    _write_json(output_root / "baseline_comparison_report.json", comparison)
    _write_json(output_root / "family_contribution_breakdown.json", contribution)
    _write_json(output_root / "oracle_reference_report.json", oracle)
    _write_json(output_root / "candidate_generation_probe_decision.json", decision)
    _write_json(output_root / "next_axis_recommendation.json", next_axis)
    _write_json(output_root / "research_decision.json", research_decision)
    complete = _artifact_complete(output_root, research_decision)
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    return {
        "output_root": str(output_root),
        "probe_contract": contract,
        "candidate_generation_variants": variant_payload,
        "top5_candidate_pool_report": top5_report,
        "top3_guardrail_report": top3_report,
        "baseline_comparison_report": comparison,
        "candidate_generation_probe_decision": decision,
        "next_axis_recommendation": next_axis,
        "research_decision": research_decision,
        "artifact_complete": complete,
    }


def _build_variants(keep_families: Sequence[Mapping[str, Any]], family_report: Mapping[str, Any]) -> list[dict[str, Any]]:
    report_by_id = {row["family_id"]: row for row in family_report.get("rows") or []}
    variants = []
    for keep in keep_families:
        family_id = str(keep["family_id"])
        variants.append(
            {
                "variant_id": f"{keep['family_group']}_only",
                "variant_type": "single_family",
                "family_ids": [family_id],
                "conditions_by_family": {family_id: report_by_id[family_id]["conditions"]},
            }
        )
    if len(keep_families) >= 2:
        variants.append(
            {
                "variant_id": "combined_keep_families",
                "variant_type": "combined_family_or",
                "family_ids": [str(row["family_id"]) for row in keep_families],
                "conditions_by_family": {str(row["family_id"]): report_by_id[str(row["family_id"])]["conditions"] for row in keep_families},
            }
        )
    return variants


def _evaluate_variant(events: pd.DataFrame, variant: Mapping[str, Any]) -> dict[str, Any]:
    family_masks = {family_id: _condition_mask(events, conditions) for family_id, conditions in variant["conditions_by_family"].items()}
    mask = pd.Series(False, index=events.index)
    for item in family_masks.values():
        mask |= item
    variant_events = events.loc[mask].copy()
    matched_by_index: dict[Any, list[str]] = {}
    for family_id, family_mask in family_masks.items():
        for idx in events.index[family_mask]:
            matched_by_index.setdefault(idx, []).append(family_id)
    variant_events["matched_family_ids"] = [matched_by_index.get(idx, []) for idx in variant_events.index]
    active_dates = sorted(variant_events["event_date"].astype(str).unique().tolist())
    baseline_top5 = _topk_by_date(events[events["event_date"].astype(str).isin(active_dates)], TOP5_K, "baseline_top5")
    baseline_top10 = _topk_by_date(events[events["event_date"].astype(str).isin(active_dates)], TOP10_K, "baseline_top10")
    variant_top5 = _topk_by_date(variant_events, TOP5_K, str(variant["variant_id"]))
    variant_top10 = _topk_by_date(variant_events, TOP10_K, str(variant["variant_id"]))
    baseline_top3 = _topk_by_date(events[events["event_date"].astype(str).isin(active_dates)], TOP3_K, "baseline_top3")
    variant_top3 = _topk_by_date(variant_events, TOP3_K, str(variant["variant_id"]))
    oracle_top3 = _oracle_topk_by_date(events[events["event_date"].astype(str).isin(active_dates)], TOP3_K)
    all_eval_events = events[events["event_date"].astype(str).isin(active_dates)].copy()
    top5_metrics = _top5_metrics(
        variant_top5,
        baseline_top5,
        variant_top10,
        baseline_top10,
        all_eval_events,
        variant_id=str(variant["variant_id"]),
        active_dates=active_dates,
    )
    top3_metrics = _top3_metrics(variant_top3, baseline_top3, oracle_top3, all_eval_events, variant_id=str(variant["variant_id"]))
    contribution = _family_contribution(variant_top5, family_masks, variant, events)
    ledger_rows = [_ledger_row(row, str(variant["variant_id"])) for row in variant_top5.to_dict("records")]
    return {
        "ledger_rows": ledger_rows,
        "top5_metrics": top5_metrics,
        "top3_metrics": top3_metrics,
        "oracle_reference": {
            "variant_id": str(variant["variant_id"]),
            "oracle_top3_avg_ret20": _mean(oracle_top3["ret20_fwd"]),
            "variant_top3_avg_ret20": top3_metrics["top3_avg_ret20"],
            "oracle_top3_gap": top3_metrics["oracle_top3_gap"],
        },
        "family_contribution": contribution,
    }


def _top5_metrics(
    selected: pd.DataFrame,
    baseline: pd.DataFrame,
    selected_top10: pd.DataFrame,
    baseline_top10: pd.DataFrame,
    all_events: pd.DataFrame,
    *,
    variant_id: str,
    active_dates: Sequence[str],
) -> dict[str, Any]:
    selected_keys = _member_keys(selected)
    baseline_keys = _member_keys(baseline)
    selected_top10_keys = _member_keys(selected_top10)
    baseline_top10_keys = _member_keys(baseline_top10)
    total_big = int(all_events["future_big_winner"].sum())
    total_top10 = int(all_events["is_future_top10_by_ret20"].sum())
    day_rows = []
    for date in active_dates:
        day = selected[selected["event_date"].astype(str).eq(str(date))]
        selectable = day["human_selectable"].astype(bool) if len(day) else pd.Series(dtype=bool)
        day_rows.append(
            {
                "event_date": str(date),
                "candidate_count": int(len(day)),
                "human_selectable_count": int(selectable.sum()) if len(day) else 0,
                "has_at_least_3_human_selectable": int(selectable.sum()) >= 3 if len(day) else False,
            }
        )
    return {
        "variant_id": variant_id,
        "evaluation_date_count": len(active_dates),
        "top5_candidate_count": int(len(selected)),
        "top5_avg_ret20": _mean(selected["ret20_fwd"]),
        "top5_win_rate20": _rate(selected["win20"].astype(bool).sum(), len(selected)),
        "top5_big_winner_capture_rate": _rate(selected["future_big_winner"].sum(), total_big),
        "top5_future_top10_capture_rate": _rate(selected["is_future_top10_by_ret20"].sum(), total_top10),
        "top5_severe_loss_rate20": _rate(selected["severe_loss20"].astype(bool).sum(), len(selected)),
        "top5_bad_pick_count": _bad_pick_count(selected),
        "top5_candidate_diversity": _mean(selected.groupby("event_date")["code"].nunique()) if len(selected) else None,
        "human_selectable_day_rate": _rate(sum(1 for row in day_rows if row["has_at_least_3_human_selectable"]), len(day_rows)),
        "top5_changed_members_count_vs_baseline": len(selected_keys.symmetric_difference(baseline_keys)),
        "top10_changed_members_count_vs_baseline": len(selected_top10_keys.symmetric_difference(baseline_top10_keys)),
        "candidate_added_count": len(selected_keys - baseline_keys),
        "candidate_overlap_with_baseline_top5": len(selected_keys & baseline_keys),
        "baseline_same_dates": _summary_metrics(baseline, all_events),
        "day_rows_sample": day_rows[:500],
    }


def _top3_metrics(selected: pd.DataFrame, baseline: pd.DataFrame, oracle: pd.DataFrame, all_events: pd.DataFrame, *, variant_id: str) -> dict[str, Any]:
    selected_nonwinner_when_winner = _nonwinner_when_winner_available(selected, all_events)
    baseline_nonwinner_when_winner = _nonwinner_when_winner_available(baseline, all_events)
    return {
        "variant_id": variant_id,
        "top3_candidate_count": int(len(selected)),
        "top3_avg_ret20": _mean(selected["ret20_fwd"]),
        "top3_severe_loss_rate20": _rate(selected["severe_loss20"].astype(bool).sum(), len(selected)),
        "oracle_top3_avg_ret20": _mean(oracle["ret20_fwd"]),
        "oracle_top3_gap": (_mean(oracle["ret20_fwd"]) or 0.0) - (_mean(selected["ret20_fwd"]) or 0.0),
        "selected_nonwinner_when_winner_available": selected_nonwinner_when_winner,
        "baseline_same_dates": {
            "top3_avg_ret20": _mean(baseline["ret20_fwd"]),
            "top3_severe_loss_rate20": _rate(baseline["severe_loss20"].astype(bool).sum(), len(baseline)),
            "selected_nonwinner_when_winner_available": baseline_nonwinner_when_winner,
        },
    }


def _comparison_report(top5_rows: Sequence[Mapping[str, Any]], top3_rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    top3_by_variant = {row["variant_id"]: row for row in top3_rows}
    rows = []
    for row in top5_rows:
        base = row["baseline_same_dates"]
        top3 = top3_by_variant[row["variant_id"]]
        top3_base = top3["baseline_same_dates"]
        rows.append(
            {
                "variant_id": row["variant_id"],
                "top5_avg_ret20_delta_vs_baseline": _delta(row["top5_avg_ret20"], base["avg_ret20"]),
                "top5_win_rate20_delta_vs_baseline": _delta(row["top5_win_rate20"], base["win_rate20"]),
                "top5_big_winner_capture_rate_delta_vs_baseline": _delta(
                    row["top5_big_winner_capture_rate"], base["big_winner_capture_rate"]
                ),
                "top5_future_top10_capture_rate_delta_vs_baseline": _delta(
                    row["top5_future_top10_capture_rate"], base["future_top10_capture_rate"]
                ),
                "top5_severe_loss_rate_delta_vs_baseline": _delta(row["top5_severe_loss_rate20"], base["severe_loss_rate20"]),
                "top5_bad_pick_count_delta_vs_baseline": int(row["top5_bad_pick_count"] or 0) - int(base["bad_pick_count"] or 0),
                "human_selectable_day_rate_delta_vs_baseline": _delta(row["human_selectable_day_rate"], base["human_selectable_day_rate"]),
                "top5_changed_members_count_vs_baseline": row["top5_changed_members_count_vs_baseline"],
                "top10_changed_members_count_vs_baseline": row["top10_changed_members_count_vs_baseline"],
                "candidate_added_count": row["candidate_added_count"],
                "candidate_overlap_with_baseline_top5": row["candidate_overlap_with_baseline_top5"],
                "top3_avg_ret20_delta_vs_baseline": _delta(top3["top3_avg_ret20"], top3_base["top3_avg_ret20"]),
                "top3_severe_loss_rate_delta_vs_baseline": _delta(top3["top3_severe_loss_rate20"], top3_base["top3_severe_loss_rate20"]),
                "oracle_top3_gap": top3["oracle_top3_gap"],
                "selected_nonwinner_when_winner_available_delta_vs_baseline": _delta(
                    top3["selected_nonwinner_when_winner_available"], top3_base["selected_nonwinner_when_winner_available"]
                ),
            }
        )
    return {"schema_version": "tradex_pre_strength_candidate_generation_baseline_comparison_v1", "axis_id": AXIS_ID, "rows": rows}


def _decision(comparison: Mapping[str, Any]) -> dict[str, Any]:
    variant_decisions = []
    for row in comparison["rows"]:
        top5_return = row["top5_avg_ret20_delta_vs_baseline"] > 0.0
        big_capture = row["top5_big_winner_capture_rate_delta_vs_baseline"] > 0.0
        top10_capture = row["top5_future_top10_capture_rate_delta_vs_baseline"] >= 0.0
        severe_ok = row["top5_severe_loss_rate_delta_vs_baseline"] <= 0.0
        bad_pick_ok = row["top5_bad_pick_count_delta_vs_baseline"] <= 0
        branch = row["top5_changed_members_count_vs_baseline"] > 0 and row["candidate_added_count"] > 0
        top3_not_fatal = row["top3_avg_ret20_delta_vs_baseline"] >= -0.02 and row["top3_severe_loss_rate_delta_vs_baseline"] <= 0.03
        if top5_return and big_capture and top10_capture and severe_ok and bad_pick_ok and branch and top3_not_fatal:
            decision = "keep_candidate"
            reason = "top5_pool_improved_with_risk_not_worse"
        elif top5_return and branch and top3_not_fatal:
            decision = "hold"
            reason = "top5_return_or_branching_improved_but_capture_or_risk_gate_failed"
        else:
            decision = "drop"
            reason = "top5_pool_quality_did_not_improve_enough"
        variant_decisions.append({"variant_id": row["variant_id"], "decision": decision, "decision_reason": reason, "comparison": row})
    keep = [row for row in variant_decisions if row["decision"] == "keep_candidate"]
    hold = [row for row in variant_decisions if row["decision"] == "hold"]
    if keep:
        decision = "keep_candidate"
        reason = "at_least_one_variant_improves_top5_candidate_pool"
        best = keep[0]["variant_id"]
    elif hold:
        decision = "hold"
        reason = "variants_branch_but_require_risk_or_capture_decomposition"
        best = hold[0]["variant_id"]
    else:
        decision = "drop"
        reason = "no_variant_improves_top5_candidate_pool"
        best = variant_decisions[0]["variant_id"] if variant_decisions else None
    return {
        "schema_version": "tradex_pre_strength_candidate_generation_probe_decision_v1",
        "axis_id": AXIS_ID,
        "decision": decision,
        "decision_reason": reason,
        "best_variant_id": best,
        "variant_decisions": variant_decisions,
        "activation_allowed": False,
        "meemee_reflectable": False,
    }


def _next_axis(decision: Mapping[str, Any]) -> dict[str, Any]:
    if decision["decision"] == "keep_candidate":
        next_axis = "starter_entry_candidate_pretest_v1"
    elif decision["decision"] == "hold":
        next_axis = "selected_family_risk_decomposition_v1"
    else:
        next_axis = "pattern_family_portfolio_refresh_v1"
    return {
        "schema_version": "tradex_pre_strength_candidate_generation_next_axis_recommendation_v1",
        "axis_id": AXIS_ID,
        "decision": decision["decision"],
        "next": next_axis,
        "activation_allowed": False,
        "meemee_reflection_allowed": False,
    }


def _prepare_events(events: pd.DataFrame) -> pd.DataFrame:
    out = events.copy()
    out["ret20_fwd"] = pd.to_numeric(out["ret20_fwd"], errors="coerce")
    out["event_strength_score"] = pd.to_numeric(out.get("event_strength_score", 0), errors="coerce").fillna(0.0)
    out["severe_loss20"] = out["severe_loss20"].astype(bool)
    out["win20"] = out["win20"].astype(bool)
    out["future_big_winner"] = out["ret20_fwd"].ge(BIG_WINNER_RET20)
    out["human_selectable"] = out["ret20_fwd"].gt(0.0) & ~out["severe_loss20"]
    out["event_member_key"] = out["code"].astype(str) + "::" + out["event_date"].astype(str)
    out["is_future_top10_by_ret20"] = False
    for _date, idx in out.groupby("event_date")["ret20_fwd"].nlargest(TOP10_K).index:
        out.loc[idx, "is_future_top10_by_ret20"] = True
    return out


def _topk_by_date(frame: pd.DataFrame, k: int, family_id: str) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    rows = []
    for _date, day in frame.groupby("event_date", sort=True):
        selected = day.sort_values(["event_strength_score", "code"], ascending=[False, True]).head(k).copy()
        selected["selection_family_id"] = family_id
        selected["selection_rank"] = range(1, len(selected) + 1)
        rows.append(selected)
    return pd.concat(rows, ignore_index=True) if rows else frame.head(0).copy()


def _oracle_topk_by_date(frame: pd.DataFrame, k: int) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    rows = []
    for _date, day in frame.groupby("event_date", sort=True):
        selected = day.sort_values(["ret20_fwd", "event_strength_score", "code"], ascending=[False, False, True]).head(k).copy()
        selected["selection_family_id"] = "family_only_oracle_evaluation_reference"
        rows.append(selected)
    return pd.concat(rows, ignore_index=True) if rows else frame.head(0).copy()


def _summary_metrics(selected: pd.DataFrame, all_events: pd.DataFrame) -> dict[str, Any]:
    if selected.empty:
        return {
            "avg_ret20": None,
            "win_rate20": 0.0,
            "big_winner_capture_rate": 0.0,
            "future_top10_capture_rate": 0.0,
            "severe_loss_rate20": 0.0,
            "bad_pick_count": 0,
            "human_selectable_day_rate": 0.0,
        }
    day_rows = []
    for _date, day in selected.groupby("event_date", sort=True):
        day_rows.append(int(day["human_selectable"].astype(bool).sum()) >= 3)
    return {
        "avg_ret20": _mean(selected["ret20_fwd"]),
        "win_rate20": _rate(selected["win20"].astype(bool).sum(), len(selected)),
        "big_winner_capture_rate": _rate(selected["future_big_winner"].sum(), all_events["future_big_winner"].sum()),
        "future_top10_capture_rate": _rate(selected["is_future_top10_by_ret20"].sum(), all_events["is_future_top10_by_ret20"].sum()),
        "severe_loss_rate20": _rate(selected["severe_loss20"].astype(bool).sum(), len(selected)),
        "bad_pick_count": _bad_pick_count(selected),
        "human_selectable_day_rate": _rate(sum(day_rows), len(day_rows)),
    }


def _family_contribution(selected: pd.DataFrame, family_masks: Mapping[str, pd.Series], variant: Mapping[str, Any], events: pd.DataFrame) -> dict[str, Any]:
    rows = []
    for family_id in variant["family_ids"]:
        keys = set(events.loc[family_masks[family_id], "event_member_key"].astype(str))
        selected_keys = set(selected["event_member_key"].astype(str))
        rows.append({"family_id": family_id, "candidate_event_count": len(keys), "selected_top5_count": len(keys & selected_keys)})
    return {"variant_id": variant["variant_id"], "rows": rows}


def _ledger_row(row: Mapping[str, Any], variant_id: str) -> dict[str, Any]:
    member_key = f"{row['code']}::{row['event_date']}"
    return {
        "variant_id": variant_id,
        "code": row["code"],
        "event_date": row["event_date"],
        "event_month": row.get("event_month"),
        "event_member_key": member_key,
        "selection_rank": row.get("selection_rank"),
        "event_strength_score": row.get("event_strength_score"),
        "ret20_fwd": row.get("ret20_fwd"),
        "win20": row.get("win20"),
        "severe_loss20": row.get("severe_loss20"),
        "future_big_winner": row.get("future_big_winner"),
        "is_future_top10_by_ret20": row.get("is_future_top10_by_ret20"),
        "matched_family_ids": row.get("matched_family_ids") or [],
    }


def _condition_mask(events: pd.DataFrame, conditions: Mapping[str, Any]) -> pd.Series:
    mask = pd.Series(True, index=events.index)
    for key, value in conditions.items():
        if key in LABEL_COLUMNS:
            raise ValueError(f"future label condition is forbidden: {key}")
        if key not in events.columns:
            mask &= False
        else:
            mask &= events[key].astype(str).eq(str(value))
    return mask


def _member_keys(frame: pd.DataFrame) -> set[str]:
    if frame.empty:
        return set()
    return set(frame["event_member_key"].astype(str))


def _bad_pick_count(frame: pd.DataFrame) -> int:
    if frame.empty:
        return 0
    return int((~frame["win20"].astype(bool) | frame["severe_loss20"].astype(bool)).sum())


def _nonwinner_when_winner_available(selected: pd.DataFrame, all_events: pd.DataFrame) -> float:
    if selected.empty:
        return 0.0
    rates = []
    for date, day_selected in selected.groupby("event_date", sort=True):
        day_all = all_events[all_events["event_date"].astype(str).eq(str(date))]
        if bool(day_all["win20"].astype(bool).any()):
            rates.append(float((~day_selected["win20"].astype(bool)).mean()))
    return _mean(pd.Series(rates)) or 0.0


def _contract(validation_root: Path, validation_contract: Mapping[str, Any], keep_families: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": "tradex_pre_strength_candidate_generation_probe_contract_v1",
        "axis_id": AXIS_ID,
        "boundary": "TRADEX-only",
        "source_validation_root": str(validation_root),
        "source_pre_strength_root": validation_contract.get("source_pre_strength_root"),
        "keep_family_count": len(keep_families),
        "evaluation_date_policy": "same_dates_where_variant_has_candidates",
        "baseline_policy": "same_date_all_pre_strength_events_sorted_by_event_strength_score",
        "variant_policy": "pattern_condition_match_sorted_by_event_strength_score",
        "oracle_policy": "evaluation_only_sort_by_ret20_fwd",
        "future_label_policy": {
            "future_labels_used_for_candidate_generation": False,
            "future_labels_used_for_evaluation": True,
            "future_labels_used_for_oracle_reference": True,
        },
        "not_changed": _not_changed(),
        "silent_fallback_used": False,
    }


def _readback(validation_decision: Mapping[str, Any], validation_contract: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "tradex_pre_strength_source_family_readback_v1",
        "axis_id": AXIS_ID,
        "source_validation_decision": validation_decision.get("decision"),
        "selected_next_validation_families": validation_decision.get("selected_next_validation_families") or [],
        "source_future_label_policy": validation_contract.get("future_label_policy"),
    }


def _artifact_complete(output_root: Path, decision: Mapping[str, Any]) -> dict[str, Any]:
    presence = {name: (output_root / name).exists() for name in REQUIRED_OUTPUTS if name != "_ARTIFACT_COMPLETE.json"}
    presence["_ARTIFACT_COMPLETE.json"] = True
    return {
        "schema_version": "tradex_pre_strength_candidate_generation_probe_artifact_complete_v1",
        "axis_id": AXIS_ID,
        "decision": decision.get("decision"),
        "complete": all(presence.values()),
        "required_outputs": REQUIRED_OUTPUTS,
        "present_outputs": presence,
        "output_root": str(output_root),
        "silent_fallback_used": False,
    }


def _delta(left: Any, right: Any) -> float:
    return (_value_or(left, 0.0) - _value_or(right, 0.0))


def _rate(count: Any, total: Any) -> float:
    total_value = float(total or 0)
    return 0.0 if total_value == 0.0 else float(count or 0) / total_value


def _mean(values: Any) -> float | None:
    if isinstance(values, pd.core.groupby.SeriesGroupBy):
        values = values.nunique()
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    if numeric.empty:
        return None
    return float(numeric.mean())


def _value_or(value: Any, default: float) -> float:
    try:
        if value is None or pd.isna(value):
            return default
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


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


def _not_changed() -> list[str]:
    return [
        "MeeMee_runtime",
        "active_ranking",
        "display_score",
        "runtime_DuckDB",
        "production_publish_registry",
        "frontend_backend_ui_api",
        "teppan_watch_policy",
        "boost_loss_guard_pattern_definitions",
        "threshold_no_trade",
        "image_fusion",
        "sell_side",
        "buy_more_core_logic",
        "exit_optimization",
        "cost_slippage_liquidity",
        "pre_reclaim_accumulation_revival",
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


def _write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(_json_ready(dict(row)), ensure_ascii=False, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
