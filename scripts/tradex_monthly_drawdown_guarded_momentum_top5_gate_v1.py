from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import pandas as pd


AXIS_ID = "monthly_drawdown_guarded_momentum_top5_gate_v1"
SCHEMA_PREFIX = "tradex_monthly_drawdown_guarded_momentum_top5_gate"
DEFAULT_SOURCE_FIELD_REPAIR_ROOT = Path(
    "G:/Tradex/common_ledger_field_repair_v1/20260514T230000Z-common-ledger-field-repair-v1"
)
DEFAULT_OUTPUT_PARENT = Path("G:/Tradex/monthly_drawdown_guarded_momentum_top5_gate_v1")
DEFAULT_RUN_ID = "20260515T000000Z-monthly-drawdown-guarded-momentum-top5-gate-v1"

REQUIRED_ARTIFACTS = [
    "evaluation_contract.json",
    "run_manifest.json",
    "source_artifact_refs.json",
    "guarded_momentum_contract.json",
    "variant_search_space.json",
    "strict_gate_leaderboard.json",
    "gate_pass_fail_report.json",
    "time_block_stability_report.json",
    "family_concentration_report.json",
    "top3_guardrail_report.json",
    "selected_candidate_family_report.json",
    "no_mutation_audit.json",
    "next_axis_recommendation.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
]

MANDATORY_GATES = [
    "top5_avg_ret20_improved",
    "top5_big_winner_capture_improved",
    "top5_future_top10_capture_improved",
    "top5_severe_loss_rate_not_worse",
    "top5_bad_pick_count_not_increased",
    "human_selectable_day_rate_not_worse",
    "time_block_effect_remains",
    "family_concentration_not_excessive",
    "top3_guardrail_not_fatal",
]

MONTHLY_DRAWDOWN_GUARD_VALUES = [
    0.0,
    -0.005,
    -0.01,
    -0.015,
    -0.02,
    -0.03,
    -0.04,
    -0.06,
    -0.08,
    -0.12,
    -0.2,
    -0.5,
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid JSONL at {path}:{line_no}: {exc}") from exc
    return rows


def _float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _rate(numer: int, denom: int) -> float:
    return float(numer / denom) if denom else 0.0


def _complete(row: Mapping[str, Any]) -> bool:
    return all(row.get(field) is not None for field in ("ret20_fwd", "mfe20", "mae20", "severe_loss20"))


def _prepare_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    complete = [row for row in rows if _complete(row)]
    frame = pd.DataFrame(complete)
    if frame.empty:
        raise RuntimeError("label-complete ledger rows are empty")
    bool_cols = [
        "baseline_candidate_flag",
        "momentum_candidate_flag",
        "combined_candidate_flag",
        "ma5_h12_candidate_flag",
        "momentum_low_risk_context_flag",
        "momentum_high_risk_context_flag",
        "severe_loss20",
        "win20",
        "is_future_top10_by_ret20",
    ]
    for col in bool_cols:
        if col not in frame:
            frame[col] = False
        frame[col] = frame[col].fillna(False).astype(bool)
    for col in ("monthly_prior_state", "symbol", "event_date"):
        if col not in frame:
            frame[col] = None
    for col in ("ret20_fwd", "baseline_score"):
        frame[col] = pd.to_numeric(frame.get(col), errors="coerce")
    frame["event_date"] = frame["event_date"].astype(str)
    frame["symbol"] = frame["symbol"].astype(str)
    baseline_dates = set(frame.loc[frame["baseline_candidate_flag"], "event_date"])
    combined_dates = set(frame.loc[frame["combined_candidate_flag"], "event_date"])
    frame = frame[frame["event_date"].isin(baseline_dates & combined_dates)].copy()
    frame["event_year"] = frame["event_date"].str.slice(0, 4)
    frame["is_big_winner_ret20_ge_10pct"] = frame["ret20_fwd"].ge(0.10)
    frame["is_bad_pick"] = frame["ret20_fwd"].le(0.0) | frame["severe_loss20"]
    frame["human_selectable"] = frame["ret20_fwd"].gt(0.0) & ~frame["severe_loss20"]
    return frame


def _variant_specs() -> list[dict[str, Any]]:
    specs = [
        {
            "variant_id": "baseline_reference",
            "momentum_weight": 0.0,
            "momentum_low_risk_weight": 0.0,
            "momentum_high_risk_penalty": 0.0,
            "monthly_down_or_drawdown_penalty": 0.0,
            "description": "source baseline score only",
        }
    ]
    for penalty in MONTHLY_DRAWDOWN_GUARD_VALUES:
        specs.append(
            {
                "variant_id": f"monthly_drawdown_guarded_momentum_m+0.02_l-0.02_h-0.02_md{penalty:+.3f}",
                "momentum_weight": 0.02,
                "momentum_low_risk_weight": -0.02,
                "momentum_high_risk_penalty": -0.02,
                "monthly_down_or_drawdown_penalty": penalty,
                "description": "research-only momentum upside with point-in-time monthly drawdown risk guard",
            }
        )
    return specs


def _score(frame: pd.DataFrame, spec: Mapping[str, Any]) -> pd.Series:
    score = frame["baseline_score"].where(frame["baseline_candidate_flag"])
    monthly_drawdown = frame["monthly_prior_state"].astype(str).eq("monthly_prior_down_or_drawdown")
    score = score + frame["momentum_candidate_flag"].astype(float) * float(spec["momentum_weight"])
    score = score + frame["momentum_low_risk_context_flag"].astype(float) * float(spec["momentum_low_risk_weight"])
    score = score + frame["momentum_high_risk_context_flag"].astype(float) * float(spec["momentum_high_risk_penalty"])
    score = score + monthly_drawdown.astype(float) * float(spec["monthly_down_or_drawdown_penalty"])
    return score


def _select(frame: pd.DataFrame, score: pd.Series, top_k: int = 5) -> pd.DataFrame:
    work = frame.assign(_candidate_score=score)
    work = work[work["_candidate_score"].notna()].sort_values(
        ["event_date", "_candidate_score", "symbol"],
        ascending=[True, False, True],
        kind="stable",
    )
    return work.groupby("event_date", sort=False).head(top_k).copy()


def _metrics(selected: pd.DataFrame, universe: pd.DataFrame, date_count: int) -> dict[str, Any]:
    day_human = selected.groupby("event_date")["human_selectable"].sum()
    return {
        "candidate_count": int(len(selected)),
        "top5_avg_ret20": float(selected["ret20_fwd"].mean()) if not selected.empty else None,
        "top5_big_winner_capture_rate": _rate(
            int(selected["is_big_winner_ret20_ge_10pct"].sum()),
            int(universe["is_big_winner_ret20_ge_10pct"].sum()),
        ),
        "top5_future_top10_capture_rate": _rate(
            int(selected["is_future_top10_by_ret20"].sum()),
            int(universe["is_future_top10_by_ret20"].sum()),
        ),
        "top5_severe_loss_rate20": _rate(int(selected["severe_loss20"].sum()), len(selected)),
        "top5_bad_pick_count": int(selected["is_bad_pick"].sum()),
        "human_selectable_day_rate": _rate(int((day_human >= 3).sum()), date_count),
        "top5_candidate_diversity": float(selected.groupby("event_date")["symbol"].nunique().mean()) if not selected.empty else 0.0,
    }


def _key_set(frame: pd.DataFrame) -> set[tuple[str, str]]:
    return set(zip(frame["event_date"].astype(str), frame["symbol"].astype(str))) if not frame.empty else set()


def _delta(value: Any, baseline: Any) -> float | None:
    left = _float(value)
    right = _float(baseline)
    if left is None or right is None:
        return None
    return float(left - right)


def _family_share(selected: pd.DataFrame) -> dict[str, Any]:
    total = len(selected)
    if not total:
        counts = {"baseline_only": 0, "momentum_overlap": 0, "ma5_h12": 0}
    else:
        ma5 = selected["ma5_h12_candidate_flag"].astype(bool)
        momentum = selected["momentum_candidate_flag"].astype(bool) & ~ma5
        baseline_only = selected["baseline_candidate_flag"].astype(bool) & ~selected["momentum_candidate_flag"].astype(bool) & ~ma5
        counts = {
            "baseline_only": int(baseline_only.sum()),
            "momentum_overlap": int(momentum.sum()),
            "ma5_h12": int(ma5.sum()),
        }
    shares = {key: _rate(value, total) for key, value in counts.items()}
    return {"counts": counts, "shares": shares, "max_family_share": max(shares.values()) if shares else 0.0}


def _time_block_report(selected: pd.DataFrame, baseline: pd.DataFrame) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    years = sorted(set(selected["event_year"].astype(str)) | set(baseline["event_year"].astype(str)))
    for year in years:
        left = selected[selected["event_year"].astype(str) == year]
        right = baseline[baseline["event_year"].astype(str) == year]
        if left.empty or right.empty:
            continue
        rows.append(
            {
                "time_block": year,
                "candidate_count": int(len(left)),
                "baseline_candidate_count": int(len(right)),
                "top5_avg_ret20_delta_vs_baseline": float(left["ret20_fwd"].mean() - right["ret20_fwd"].mean()),
                "top5_severe_loss_rate_delta_vs_baseline": _rate(int(left["severe_loss20"].sum()), len(left))
                - _rate(int(right["severe_loss20"].sum()), len(right)),
                "top5_bad_pick_count_delta_vs_baseline": int(left["is_bad_pick"].sum()) - int(right["is_bad_pick"].sum()),
            }
        )
    positive = sum(1 for row in rows if row["top5_avg_ret20_delta_vs_baseline"] > 0.0)
    return {
        "rows": rows,
        "time_block_count": len(rows),
        "positive_avg_ret20_block_count": positive,
        "positive_avg_ret20_block_rate": _rate(positive, len(rows)),
        "effect_remains": len(rows) >= 3 and _rate(positive, len(rows)) >= 0.60,
    }


def _top3_guardrail(selected: pd.DataFrame, baseline: pd.DataFrame, universe: pd.DataFrame) -> dict[str, Any]:
    sel3 = selected.groupby("event_date", sort=False).head(3)
    base3 = baseline.groupby("event_date", sort=False).head(3)
    oracle3 = (
        universe.sort_values(["event_date", "ret20_fwd", "symbol"], ascending=[True, False, True], kind="stable")
        .groupby("event_date", sort=False)
        .head(3)
    )
    return {
        "top3_avg_ret20": float(sel3["ret20_fwd"].mean()) if not sel3.empty else None,
        "baseline_top3_avg_ret20": float(base3["ret20_fwd"].mean()) if not base3.empty else None,
        "top3_avg_ret20_delta_vs_baseline": float(sel3["ret20_fwd"].mean() - base3["ret20_fwd"].mean())
        if not sel3.empty and not base3.empty
        else None,
        "top3_severe_loss_rate20": _rate(int(sel3["severe_loss20"].sum()), len(sel3)),
        "baseline_top3_severe_loss_rate20": _rate(int(base3["severe_loss20"].sum()), len(base3)),
        "top3_severe_loss_rate_delta_vs_baseline": _rate(int(sel3["severe_loss20"].sum()), len(sel3))
        - _rate(int(base3["severe_loss20"].sum()), len(base3)),
        "oracle_top3_gap": float(oracle3["ret20_fwd"].mean() - sel3["ret20_fwd"].mean()) if not oracle3.empty and not sel3.empty else None,
    }


def _gate_row(
    spec: Mapping[str, Any],
    metrics: Mapping[str, Any],
    baseline_metrics: Mapping[str, Any],
    selected: pd.DataFrame,
    baseline: pd.DataFrame,
    universe: pd.DataFrame,
) -> dict[str, Any]:
    family = _family_share(selected)
    time_block = _time_block_report(selected, baseline)
    guardrail = _top3_guardrail(selected, baseline, universe)
    keys = _key_set(selected)
    base_keys = _key_set(baseline)
    deltas = {
        "top5_avg_ret20_delta_vs_baseline": _delta(metrics["top5_avg_ret20"], baseline_metrics["top5_avg_ret20"]),
        "top5_big_winner_capture_delta_vs_baseline": _delta(
            metrics["top5_big_winner_capture_rate"], baseline_metrics["top5_big_winner_capture_rate"]
        ),
        "top5_future_top10_capture_delta_vs_baseline": _delta(
            metrics["top5_future_top10_capture_rate"], baseline_metrics["top5_future_top10_capture_rate"]
        ),
        "top5_severe_loss_rate_delta_vs_baseline": _delta(metrics["top5_severe_loss_rate20"], baseline_metrics["top5_severe_loss_rate20"]),
        "top5_bad_pick_count_delta_vs_baseline": int(metrics["top5_bad_pick_count"]) - int(baseline_metrics["top5_bad_pick_count"]),
        "human_selectable_day_rate_delta_vs_baseline": _delta(metrics["human_selectable_day_rate"], baseline_metrics["human_selectable_day_rate"]),
        "top5_changed_members_count_vs_baseline": len(keys.symmetric_difference(base_keys)),
        "candidate_added_count": len(keys - base_keys),
    }
    gates = {
        "top5_avg_ret20_improved": (deltas["top5_avg_ret20_delta_vs_baseline"] or 0.0) > 0.0,
        "top5_big_winner_capture_improved": (deltas["top5_big_winner_capture_delta_vs_baseline"] or 0.0) > 0.0,
        "top5_future_top10_capture_improved": (deltas["top5_future_top10_capture_delta_vs_baseline"] or 0.0) > 0.0,
        "top5_severe_loss_rate_not_worse": (deltas["top5_severe_loss_rate_delta_vs_baseline"] or 0.0) <= 0.0,
        "top5_bad_pick_count_not_increased": int(deltas["top5_bad_pick_count_delta_vs_baseline"]) <= 0,
        "human_selectable_day_rate_not_worse": (deltas["human_selectable_day_rate_delta_vs_baseline"] or 0.0) >= 0.0,
        "time_block_effect_remains": bool(time_block["effect_remains"]),
        "family_concentration_not_excessive": family["max_family_share"] <= 0.90,
        "top3_guardrail_not_fatal": (guardrail["top3_avg_ret20_delta_vs_baseline"] or 0.0) >= -0.02
        and (guardrail["top3_severe_loss_rate_delta_vs_baseline"] or 0.0) <= 0.03,
    }
    return {
        "variant_id": spec["variant_id"],
        "spec": spec,
        "metrics": metrics,
        "deltas_vs_baseline": deltas,
        "gate_results": gates,
        "gate_pass_count": sum(bool(value) for value in gates.values()),
        "all_mandatory_gates_pass": all(bool(value) for value in gates.values()),
        "family_concentration": family,
        "time_block_summary": {key: value for key, value in time_block.items() if key != "rows"},
        "top3_guardrail": guardrail,
    }


def run(args: argparse.Namespace) -> Path:
    output_root = args.output_parent / args.run_id
    output_root.mkdir(parents=True, exist_ok=True)
    source_rows = _read_jsonl(args.source_field_repair_root / "repaired_common_top5_candidate_ledger.jsonl")
    source_decision = _read_json(args.source_field_repair_root / "research_decision.json")
    frame = _prepare_frame(source_rows)
    date_count = int(frame["event_date"].nunique())
    baseline_spec = _variant_specs()[0]
    baseline_selected = _select(frame, _score(frame, baseline_spec))
    baseline_metrics = _metrics(baseline_selected, frame, date_count)
    rows: list[dict[str, Any]] = []
    time_blocks_by_variant: dict[str, Any] = {}
    family_by_variant: dict[str, Any] = {}
    guardrail_by_variant: dict[str, Any] = {}
    for spec in _variant_specs():
        selected = _select(frame, _score(frame, spec))
        metrics = _metrics(selected, frame, date_count)
        row = _gate_row(spec, metrics, baseline_metrics, selected, baseline_selected, frame)
        rows.append(row)
        time_blocks_by_variant[spec["variant_id"]] = _time_block_report(selected, baseline_selected)
        family_by_variant[spec["variant_id"]] = row["family_concentration"]
        guardrail_by_variant[spec["variant_id"]] = row["top3_guardrail"]
    non_baseline = [row for row in rows if row["variant_id"] != "baseline_reference"]
    pass_rows = [row for row in non_baseline if row["all_mandatory_gates_pass"]]
    best_rows = sorted(
        non_baseline,
        key=lambda row: (
            row["all_mandatory_gates_pass"],
            row["gate_pass_count"],
            row["deltas_vs_baseline"]["top5_avg_ret20_delta_vs_baseline"] or -999.0,
            row["deltas_vs_baseline"]["top5_big_winner_capture_delta_vs_baseline"] or -999.0,
        ),
        reverse=True,
    )
    best = best_rows[0] if best_rows else None
    if pass_rows:
        decision = "keep_candidate"
        authoritative = "monthly_drawdown_guarded_momentum_top5_gate_keep_candidate"
        next_axis = "starter_entry_candidate_pretest_v1"
        typed_reasons = ["all_mandatory_top5_candidate_pool_gates_passed"]
    else:
        decision = "drop"
        authoritative = "monthly_drawdown_guarded_momentum_top5_gate_failed"
        next_axis = "pattern_family_portfolio_refresh_v2"
        typed_reasons = ["no_monthly_drawdown_guarded_momentum_variant_satisfied_all_gates"]
        if best:
            failed = [gate for gate, passed in best["gate_results"].items() if not passed]
            typed_reasons.append("best_variant_failed_gates:" + ",".join(failed))
    generated_at = _utc_now()
    artifacts: dict[str, dict[str, Any]] = {
        "evaluation_contract.json": {
            "schema_version": f"{SCHEMA_PREFIX}_evaluation_contract_v1",
            "axis_id": AXIS_ID,
            "boundary": "TRADEX-only",
            "source_scope": "label_complete_subset",
            "top5_objective": "human_selects_max3_from_top5_candidate_pool",
            "strict_mandatory_gates": MANDATORY_GATES,
            "candidate_scoring_created": True,
            "candidate_scoring_scope": "research_only_point_in_time_context_adjustment",
            "future_labels_used_for_evaluation_only": True,
            "future_labels_used_in_candidate_construction": False,
            "production_ranking_changed": False,
            "meemee_reflectable": False,
        },
        "run_manifest.json": {
            "schema_version": f"{SCHEMA_PREFIX}_run_manifest_v1",
            "axis_id": AXIS_ID,
            "run_id": args.run_id,
            "generated_at_utc": generated_at,
            "source_field_repair_root": str(args.source_field_repair_root),
            "output_root": str(output_root),
            "label_complete_row_count": int(len(frame)),
            "evaluation_date_count": date_count,
            "variant_count": len(rows),
        },
        "source_artifact_refs.json": {
            "schema_version": f"{SCHEMA_PREFIX}_source_artifact_refs_v1",
            "source_field_repair_root": str(args.source_field_repair_root),
            "source_field_repair_decision": source_decision.get("authoritative_research_decision"),
        },
        "guarded_momentum_contract.json": {
            "schema_version": f"{SCHEMA_PREFIX}_guarded_momentum_contract_v1",
            "candidate_family": "monthly_drawdown_guarded_momentum",
            "base_family": "momentum_continuation_soft_boost_v1",
            "risk_guard_field": "monthly_prior_state",
            "risk_guard_value": "monthly_prior_down_or_drawdown",
            "risk_guard_point_in_time": True,
            "uses_future_labels_in_scoring": False,
            "uses_future_labels_for_evaluation_only": True,
            "monthly_drawdown_guard_penalty_values": MONTHLY_DRAWDOWN_GUARD_VALUES,
        },
        "variant_search_space.json": {
            "schema_version": f"{SCHEMA_PREFIX}_variant_search_space_v1",
            "variant_count": len(rows),
            "search_policy": "bounded_research_only_guard_scan",
            "uses_future_labels_in_scoring": False,
            "dimensions": {
                "momentum_weight": [0.02],
                "momentum_low_risk_weight": [-0.02],
                "momentum_high_risk_penalty": [-0.02],
                "monthly_down_or_drawdown_penalty": MONTHLY_DRAWDOWN_GUARD_VALUES,
            },
        },
        "strict_gate_leaderboard.json": {
            "schema_version": f"{SCHEMA_PREFIX}_leaderboard_v1",
            "axis_id": AXIS_ID,
            "baseline_reference": next(row for row in rows if row["variant_id"] == "baseline_reference"),
            "pass_count": len(pass_rows),
            "best_variant": best,
            "pass_rows": pass_rows,
            "top_rows": best_rows[:25],
        },
        "gate_pass_fail_report.json": {
            "schema_version": f"{SCHEMA_PREFIX}_gate_pass_fail_report_v1",
            "axis_id": AXIS_ID,
            "pass_count": len(pass_rows),
            "failed_count": len(non_baseline) - len(pass_rows),
            "mandatory_gates": MANDATORY_GATES,
            "best_variant_gate_results": best["gate_results"] if best else {},
            "best_variant_failed_gates": [gate for gate, passed in best["gate_results"].items() if not passed] if best else [],
        },
        "time_block_stability_report.json": {
            "schema_version": f"{SCHEMA_PREFIX}_time_block_stability_report_v1",
            "axis_id": AXIS_ID,
            "best_variant_id": best["variant_id"] if best else None,
            "best_variant": time_blocks_by_variant.get(best["variant_id"]) if best else None,
        },
        "family_concentration_report.json": {
            "schema_version": f"{SCHEMA_PREFIX}_family_concentration_report_v1",
            "axis_id": AXIS_ID,
            "best_variant_id": best["variant_id"] if best else None,
            "best_variant": family_by_variant.get(best["variant_id"]) if best else None,
        },
        "top3_guardrail_report.json": {
            "schema_version": f"{SCHEMA_PREFIX}_top3_guardrail_report_v1",
            "axis_id": AXIS_ID,
            "best_variant_id": best["variant_id"] if best else None,
            "best_variant": guardrail_by_variant.get(best["variant_id"]) if best else None,
            "baseline_reference": guardrail_by_variant.get("baseline_reference"),
        },
        "selected_candidate_family_report.json": {
            "schema_version": f"{SCHEMA_PREFIX}_selected_candidate_family_report_v1",
            "axis_id": AXIS_ID,
            "selected_family": "monthly_drawdown_guarded_momentum",
            "best_variant_id": best["variant_id"] if best else None,
            "best_variant_all_mandatory_gates_pass": bool(best and best["all_mandatory_gates_pass"]),
            "best_variant_deltas_vs_baseline": best["deltas_vs_baseline"] if best else {},
            "selected_family_is_single_source_dominated": bool(best and best["family_concentration"]["max_family_share"] > 0.90),
            "candidate_pool_objective": "top5_pool_for_human_max3_selection",
        },
        "no_mutation_audit.json": {
            "schema_version": f"{SCHEMA_PREFIX}_no_mutation_audit_v1",
            "axis_id": AXIS_ID,
            "production_ranking_changed": False,
            "runtime_duckdb_written": False,
            "display_score_changed": False,
            "publish_bundle_created": False,
            "production_publish_registered": False,
            "meemee_runtime_changed": False,
            "frontend_backend_changed": False,
            "no_mutation_pass": True,
        },
        "next_axis_recommendation.json": {
            "schema_version": f"{SCHEMA_PREFIX}_next_axis_recommendation_v1",
            "axis_id": AXIS_ID,
            "decision": decision,
            "next": next_axis,
            "reason": authoritative,
        },
        "research_decision.json": {
            "schema_version": f"{SCHEMA_PREFIX}_research_decision_v1",
            "generated_at_utc": generated_at,
            "research_phase": "monthly_drawdown_guarded_momentum_top5_gate",
            "boundary": "TRADEX-only",
            "axis_moved": "monthly_drawdown_guarded_momentum_top5_gate",
            "source_field_repair_decision": source_decision.get("authoritative_research_decision"),
            "strict_gate_validation_run": True,
            "all_mandatory_gates_required": True,
            "strict_pass_variant_count": len(pass_rows),
            "best_variant_id": best["variant_id"] if best else None,
            "top5_candidate_pool_clearly_better_than_baseline": bool(pass_rows),
            "candidate_scoring_created": True,
            "candidate_scoring_scope": "TRADEX_research_only",
            "production_ranking_changed": False,
            "publish_bundle_created": False,
            "meemee_reflectable": False,
            "future_labels_used_for_evaluation_only": True,
            "future_labels_used_in_candidate_construction": False,
            "silent_fallback_used": False,
            "research_fallback_used": False,
            "decision": decision,
            "authoritative_research_decision": authoritative,
            "typed_reasons": typed_reasons,
        },
    }
    for name, payload in artifacts.items():
        _write_json(output_root / name, payload)
    complete = {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "axis_id": AXIS_ID,
        "run_id": args.run_id,
        "artifact_root": str(output_root),
        "required_artifacts": REQUIRED_ARTIFACTS,
        "artifacts": {},
        "complete": False,
    }
    for name in REQUIRED_ARTIFACTS:
        path = output_root / name
        complete["artifacts"][name] = {"exists": path.exists(), "bytes": path.stat().st_size if path.exists() else 0}
    complete["complete"] = all(item["exists"] and item["bytes"] > 0 for name, item in complete["artifacts"].items() if name != "_ARTIFACT_COMPLETE.json")
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    complete["artifacts"]["_ARTIFACT_COMPLETE.json"] = {
        "exists": (output_root / "_ARTIFACT_COMPLETE.json").exists(),
        "bytes": (output_root / "_ARTIFACT_COMPLETE.json").stat().st_size,
    }
    complete["complete"] = all(item["exists"] and item["bytes"] > 0 for item in complete["artifacts"].values())
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    return output_root


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-field-repair-root", type=Path, default=DEFAULT_SOURCE_FIELD_REPAIR_ROOT)
    parser.add_argument("--output-parent", type=Path, default=DEFAULT_OUTPUT_PARENT)
    parser.add_argument("--run-id", default=DEFAULT_RUN_ID)
    return parser


def main() -> None:
    output_root = run(_parser().parse_args())
    print(json.dumps({"axis_id": AXIS_ID, "artifact_root": str(output_root)}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
