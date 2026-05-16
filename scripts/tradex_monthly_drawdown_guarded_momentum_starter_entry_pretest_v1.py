from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import tradex_monthly_drawdown_guarded_momentum_top5_gate_v1 as top5_gate


AXIS_ID = "monthly_drawdown_guarded_momentum_starter_entry_pretest_v1"
SCHEMA_PREFIX = "tradex_monthly_drawdown_guarded_momentum_starter_entry_pretest"
DEFAULT_SOURCE_TOP5_GATE_ROOT = Path(
    "G:/Tradex/monthly_drawdown_guarded_momentum_top5_gate_v1/"
    "20260515T000000Z-monthly-drawdown-guarded-momentum-top5-gate-v1"
)
DEFAULT_SOURCE_FIELD_REPAIR_ROOT = Path(
    "G:/Tradex/common_ledger_field_repair_v1/20260514T230000Z-common-ledger-field-repair-v1"
)
DEFAULT_OUTPUT_PARENT = Path("G:/Tradex/monthly_drawdown_guarded_momentum_starter_entry_pretest_v1")
DEFAULT_RUN_ID = "20260515T003000Z-monthly-drawdown-guarded-momentum-starter-entry-pretest-v1"

REQUIRED_ARTIFACTS = [
    "evaluation_contract.json",
    "run_manifest.json",
    "source_artifact_refs.json",
    "starter_entry_pretest_contract.json",
    "starter_entry_leaderboard.json",
    "starter_entry_candidate_pool_report.json",
    "operational_frequency_report.json",
    "monthly_candidate_frequency_report.json",
    "candidate_source_mix_report.json",
    "time_block_stability_report.json",
    "guardrail_report.json",
    "human_selectable_day_report.json",
    "starter_entry_candidate_snapshot.jsonl",
    "no_mutation_audit.json",
    "next_axis_recommendation.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
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


def _write_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            json.dump(row, handle, ensure_ascii=False, sort_keys=True)
            handle.write("\n")


def _candidate_snapshot(selected: pd.DataFrame, baseline: pd.DataFrame) -> list[dict[str, Any]]:
    baseline_keys = top5_gate._key_set(baseline)
    rows: list[dict[str, Any]] = []
    ranked = selected.sort_values(["event_date", "_candidate_score", "symbol"], ascending=[True, False, True], kind="stable").copy()
    ranked["starter_rank"] = ranked.groupby("event_date", sort=False).cumcount() + 1
    for _, row in ranked.iterrows():
        key = (str(row["event_date"]), str(row["symbol"]))
        rows.append(
            {
                "event_date": str(row["event_date"]),
                "symbol": str(row["symbol"]),
                "starter_rank": int(row["starter_rank"]),
                "starter_score": float(row["_candidate_score"]),
                "added_vs_baseline": key not in baseline_keys,
                "baseline_candidate_flag": bool(row["baseline_candidate_flag"]),
                "momentum_candidate_flag": bool(row["momentum_candidate_flag"]),
                "ma5_h12_candidate_flag": bool(row["ma5_h12_candidate_flag"]),
                "monthly_prior_state": row.get("monthly_prior_state"),
                "ret20_fwd": float(row["ret20_fwd"]),
                "win20": bool(row["win20"]),
                "severe_loss20": bool(row["severe_loss20"]),
                "is_bad_pick": bool(row["is_bad_pick"]),
                "human_selectable": bool(row["human_selectable"]),
                "is_big_winner_ret20_ge_10pct": bool(row["is_big_winner_ret20_ge_10pct"]),
                "is_future_top10_by_ret20": bool(row["is_future_top10_by_ret20"]),
            }
        )
    return rows


def _select_with_score(frame: pd.DataFrame, spec: Mapping[str, Any]) -> pd.DataFrame:
    score = top5_gate._score(frame, spec)
    selected = top5_gate._select(frame, score)
    return selected.assign(_candidate_score=score.loc[selected.index])


def _day_rate_counts(selected: pd.DataFrame, date_count: int) -> dict[str, Any]:
    usable = selected.groupby("event_date")["human_selectable"].sum()
    return {
        "starter_candidate_day_count": int(selected["event_date"].nunique()),
        "days_with_1_plus_usable_candidate": int((usable >= 1).sum()),
        "days_with_2_plus_usable_candidates": int((usable >= 2).sum()),
        "days_with_3_plus_usable_candidates": int((usable >= 3).sum()),
        "days_with_1_plus_usable_candidate_rate": top5_gate._rate(int((usable >= 1).sum()), date_count),
        "days_with_2_plus_usable_candidates_rate": top5_gate._rate(int((usable >= 2).sum()), date_count),
        "days_with_3_plus_usable_candidates_rate": top5_gate._rate(int((usable >= 3).sum()), date_count),
    }


def _candidate_source_mix(selected: pd.DataFrame) -> dict[str, Any]:
    family = top5_gate._family_share(selected)
    total = len(selected)
    monthly_guarded = selected["monthly_prior_state"].astype(str).eq("monthly_prior_down_or_drawdown")
    return {
        "candidate_source_mix": family,
        "monthly_down_or_drawdown_selected_count": int(monthly_guarded.sum()),
        "monthly_down_or_drawdown_selected_share": top5_gate._rate(int(monthly_guarded.sum()), total),
        "momentum_selected_count": int(selected["momentum_candidate_flag"].sum()),
        "baseline_selected_count": int(selected["baseline_candidate_flag"].sum()),
        "ma5_h12_selected_count": int(selected["ma5_h12_candidate_flag"].sum()),
    }


def _monthly_frequency(selected: pd.DataFrame) -> dict[str, Any]:
    work = selected.copy()
    work["event_month"] = work["event_date"].astype(str).str.slice(0, 7)
    by_month = []
    for month, group in work.groupby("event_month", sort=True):
        by_month.append(
            {
                "event_month": str(month),
                "candidate_count": int(len(group)),
                "day_count": int(group["event_date"].nunique()),
                "human_selectable_count": int(group["human_selectable"].sum()),
                "severe_loss_count": int(group["severe_loss20"].sum()),
            }
        )
    return {
        "month_count": len(by_month),
        "months_with_candidates": sum(1 for row in by_month if row["candidate_count"] > 0),
        "avg_candidate_count_per_month": float(pd.Series([row["candidate_count"] for row in by_month]).mean()) if by_month else 0.0,
        "rows": by_month,
    }


def _guardrail(selected: pd.DataFrame, baseline: pd.DataFrame, universe: pd.DataFrame) -> dict[str, Any]:
    top3 = top5_gate._top3_guardrail(selected, baseline, universe)
    sel3 = selected.groupby("event_date", sort=False).head(3)
    available_winner_days = 0
    selected_nonwinner_when_winner_available = 0
    for event_date, group in universe.groupby("event_date", sort=False):
        if not bool(group["win20"].any()):
            continue
        available_winner_days += 1
        picked = sel3[sel3["event_date"] == event_date]
        if not picked.empty and not bool(picked["win20"].all()):
            selected_nonwinner_when_winner_available += 1
    top3["selected_nonwinner_when_winner_available"] = int(selected_nonwinner_when_winner_available)
    top3["winner_available_day_count"] = int(available_winner_days)
    top3["selected_nonwinner_when_winner_available_rate"] = top5_gate._rate(
        selected_nonwinner_when_winner_available, available_winner_days
    )
    return top3


def _comparison_row(
    variant_id: str,
    selected: pd.DataFrame,
    baseline: pd.DataFrame,
    universe: pd.DataFrame,
    date_count: int,
) -> dict[str, Any]:
    metrics = top5_gate._metrics(selected, universe, date_count)
    baseline_metrics = top5_gate._metrics(baseline, universe, date_count)
    keys = top5_gate._key_set(selected)
    baseline_keys = top5_gate._key_set(baseline)
    deltas = {
        "top5_avg_ret20_delta_vs_baseline": top5_gate._delta(metrics["top5_avg_ret20"], baseline_metrics["top5_avg_ret20"]),
        "top5_win_rate20_delta_vs_baseline": float(selected["win20"].mean() - baseline["win20"].mean()),
        "top5_big_winner_capture_delta_vs_baseline": top5_gate._delta(
            metrics["top5_big_winner_capture_rate"], baseline_metrics["top5_big_winner_capture_rate"]
        ),
        "top5_future_top10_capture_delta_vs_baseline": top5_gate._delta(
            metrics["top5_future_top10_capture_rate"], baseline_metrics["top5_future_top10_capture_rate"]
        ),
        "top5_severe_loss_rate_delta_vs_baseline": top5_gate._delta(
            metrics["top5_severe_loss_rate20"], baseline_metrics["top5_severe_loss_rate20"]
        ),
        "top5_bad_pick_count_delta_vs_baseline": int(metrics["top5_bad_pick_count"]) - int(baseline_metrics["top5_bad_pick_count"]),
        "human_selectable_day_rate_delta_vs_baseline": top5_gate._delta(
            metrics["human_selectable_day_rate"], baseline_metrics["human_selectable_day_rate"]
        ),
        "top5_candidate_diversity_delta_vs_baseline": top5_gate._delta(
            metrics["top5_candidate_diversity"], baseline_metrics["top5_candidate_diversity"]
        ),
        "candidate_added_count": len(keys - baseline_keys),
        "candidate_removed_count": len(baseline_keys - keys),
        "top5_changed_members_count_vs_baseline": len(keys.symmetric_difference(baseline_keys)),
    }
    return {
        "variant_id": variant_id,
        "metrics": {
            **metrics,
            "top5_win_rate20": float(selected["win20"].mean()) if not selected.empty else None,
        },
        "deltas_vs_baseline": deltas,
        "operational_frequency": _day_rate_counts(selected, date_count),
        "candidate_source_mix": _candidate_source_mix(selected),
        "time_block": top5_gate._time_block_report(selected, baseline),
        "guardrail": _guardrail(selected, baseline, universe),
    }


def _pretest_gates(row: Mapping[str, Any]) -> dict[str, bool]:
    deltas = row["deltas_vs_baseline"]
    operational = row["operational_frequency"]
    family = row["candidate_source_mix"]["candidate_source_mix"]
    time_block = row["time_block"]
    guardrail = row["guardrail"]
    return {
        "top5_candidate_pool_clearly_better": bool(
            (deltas["top5_avg_ret20_delta_vs_baseline"] or 0.0) > 0
            and (deltas["top5_big_winner_capture_delta_vs_baseline"] or 0.0) > 0
            and (deltas["top5_future_top10_capture_delta_vs_baseline"] or 0.0) > 0
        ),
        "human_selectable_day_rate_not_worse": (deltas["human_selectable_day_rate_delta_vs_baseline"] or 0.0) >= 0,
        "days_with_3_plus_usable_candidates_sufficient": operational["days_with_3_plus_usable_candidates_rate"] >= 0.40,
        "severe_loss_not_worse": (deltas["top5_severe_loss_rate_delta_vs_baseline"] or 0.0) <= 0,
        "bad_pick_not_increased": int(deltas["top5_bad_pick_count_delta_vs_baseline"]) <= 0,
        "winner_capture_not_worse": (deltas["top5_big_winner_capture_delta_vs_baseline"] or 0.0) >= 0,
        "future_top10_capture_not_worse": (deltas["top5_future_top10_capture_delta_vs_baseline"] or 0.0) >= 0,
        "time_block_majority_positive": bool(time_block["effect_remains"]),
        "family_concentration_not_excessive": family["max_family_share"] <= 0.90,
        "top3_guardrail_not_fatal": (guardrail["top3_avg_ret20_delta_vs_baseline"] or 0.0) >= -0.02
        and (guardrail["top3_severe_loss_rate_delta_vs_baseline"] or 0.0) <= 0.03,
    }


def run(args: argparse.Namespace) -> Path:
    output_root = args.output_parent / args.run_id
    output_root.mkdir(parents=True, exist_ok=True)
    source_decision = _read_json(args.source_top5_gate_root / "research_decision.json")
    source_leaderboard = _read_json(args.source_top5_gate_root / "strict_gate_leaderboard.json")
    source_complete = _read_json(args.source_top5_gate_root / "_ARTIFACT_COMPLETE.json")
    source_rows = top5_gate._read_jsonl(args.source_field_repair_root / "repaired_common_top5_candidate_ledger.jsonl")
    frame = top5_gate._prepare_frame(source_rows)
    date_count = int(frame["event_date"].nunique())
    baseline_spec = top5_gate._variant_specs()[0]
    best_spec = source_leaderboard["best_variant"]["spec"]
    baseline_selected = _select_with_score(frame, baseline_spec)
    starter_selected = _select_with_score(frame, best_spec)
    baseline_row = _comparison_row("baseline_reference", baseline_selected, baseline_selected, frame, date_count)
    starter_row = _comparison_row("monthly_drawdown_guarded_momentum_starter_entry", starter_selected, baseline_selected, frame, date_count)
    pretest_gates = _pretest_gates(starter_row)
    all_gates_pass = all(pretest_gates.values())
    if all_gates_pass:
        decision = "keep_candidate"
        authoritative = "starter_entry_pretest_keep"
        next_axis = "manual_candidate_review_pack_v1"
        typed_reasons = ["starter_entry_pretest_all_gates_passed"]
    else:
        decision = "hold"
        authoritative = "starter_entry_pretest_hold"
        next_axis = "starter_entry_frequency_or_timeblock_audit_v1"
        failed = [gate for gate, passed in pretest_gates.items() if not passed]
        typed_reasons = ["starter_entry_pretest_failed_gates:" + ",".join(failed)]
    generated_at = _utc_now()
    candidate_rows = _candidate_snapshot(starter_selected, baseline_selected)
    payloads: dict[str, dict[str, Any]] = {
        "evaluation_contract.json": {
            "schema_version": f"{SCHEMA_PREFIX}_evaluation_contract_v1",
            "axis_id": AXIS_ID,
            "boundary": "TRADEX-only",
            "source_top5_gate_decision": source_decision.get("authoritative_research_decision"),
            "top5_objective": "human_selects_max3_from_top5_candidate_pool",
            "fixed_conditions": {
                "same_source_ledger": True,
                "same_label_complete_subset": True,
                "same_top_k": 5,
                "buy_side_only": True,
            },
            "future_labels_used_for_evaluation_only": True,
            "future_labels_used_in_candidate_construction": False,
        },
        "run_manifest.json": {
            "schema_version": f"{SCHEMA_PREFIX}_run_manifest_v1",
            "axis_id": AXIS_ID,
            "run_id": args.run_id,
            "generated_at_utc": generated_at,
            "source_top5_gate_root": str(args.source_top5_gate_root),
            "source_field_repair_root": str(args.source_field_repair_root),
            "output_root": str(output_root),
            "label_complete_row_count": int(len(frame)),
            "evaluation_date_count": date_count,
            "starter_candidate_count": int(len(starter_selected)),
        },
        "source_artifact_refs.json": {
            "schema_version": f"{SCHEMA_PREFIX}_source_artifact_refs_v1",
            "source_top5_gate_root": str(args.source_top5_gate_root),
            "source_top5_gate_decision": source_decision.get("authoritative_research_decision"),
            "source_top5_gate_complete": bool(source_complete.get("complete")),
            "source_field_repair_root": str(args.source_field_repair_root),
        },
        "starter_entry_pretest_contract.json": {
            "schema_version": f"{SCHEMA_PREFIX}_contract_v1",
            "pretest_variant_id": source_leaderboard["best_variant"]["variant_id"],
            "best_spec": best_spec,
            "user_selection_owner": "human_selects_up_to_3_from_top5",
            "auto_select_exactly_3": False,
            "days_with_3_plus_usable_candidates_rate_min": 0.40,
            "gate_policy": "all_pretest_gates_must_pass_for_keep",
            "uses_future_labels_in_scoring": False,
        },
        "starter_entry_leaderboard.json": {
            "schema_version": f"{SCHEMA_PREFIX}_leaderboard_v1",
            "baseline_reference": baseline_row,
            "starter_entry_variant": starter_row,
            "pretest_gates": pretest_gates,
            "all_pretest_gates_pass": all_gates_pass,
        },
        "starter_entry_candidate_pool_report.json": {
            "schema_version": f"{SCHEMA_PREFIX}_candidate_pool_report_v1",
            "baseline_metrics": baseline_row["metrics"],
            "starter_metrics": starter_row["metrics"],
            "deltas_vs_baseline": starter_row["deltas_vs_baseline"],
            "pretest_gates": pretest_gates,
        },
        "operational_frequency_report.json": {
            "schema_version": f"{SCHEMA_PREFIX}_operational_frequency_report_v1",
            "baseline": baseline_row["operational_frequency"],
            "starter_entry": starter_row["operational_frequency"],
        },
        "monthly_candidate_frequency_report.json": {
            "schema_version": f"{SCHEMA_PREFIX}_monthly_candidate_frequency_report_v1",
            "starter_entry": _monthly_frequency(starter_selected),
        },
        "candidate_source_mix_report.json": {
            "schema_version": f"{SCHEMA_PREFIX}_candidate_source_mix_report_v1",
            "baseline": baseline_row["candidate_source_mix"],
            "starter_entry": starter_row["candidate_source_mix"],
        },
        "time_block_stability_report.json": {
            "schema_version": f"{SCHEMA_PREFIX}_time_block_stability_report_v1",
            "starter_entry": starter_row["time_block"],
        },
        "guardrail_report.json": {
            "schema_version": f"{SCHEMA_PREFIX}_guardrail_report_v1",
            "starter_entry": starter_row["guardrail"],
        },
        "human_selectable_day_report.json": {
            "schema_version": f"{SCHEMA_PREFIX}_human_selectable_day_report_v1",
            "baseline": baseline_row["operational_frequency"],
            "starter_entry": starter_row["operational_frequency"],
            "human_selectable_day_rate_delta_vs_baseline": starter_row["deltas_vs_baseline"][
                "human_selectable_day_rate_delta_vs_baseline"
            ],
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
            "research_phase": "monthly_drawdown_guarded_momentum_starter_entry_pretest",
            "boundary": "TRADEX-only",
            "axis_moved": "starter_entry_candidate_pretest",
            "source_top5_gate_decision": source_decision.get("authoritative_research_decision"),
            "starter_entry_pretest_run": True,
            "starter_entry_candidate_set_created": True,
            "top5_candidate_pool_clearly_better_than_baseline": bool(pretest_gates["top5_candidate_pool_clearly_better"]),
            "human_max3_selection_owner": True,
            "auto_select_exactly_3": False,
            "candidate_scoring_created": True,
            "candidate_scoring_scope": "TRADEX_research_only_pretest",
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
    for name, payload in payloads.items():
        _write_json(output_root / name, payload)
    _write_jsonl(output_root / "starter_entry_candidate_snapshot.jsonl", candidate_rows)
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
    parser.add_argument("--source-top5-gate-root", type=Path, default=DEFAULT_SOURCE_TOP5_GATE_ROOT)
    parser.add_argument("--source-field-repair-root", type=Path, default=DEFAULT_SOURCE_FIELD_REPAIR_ROOT)
    parser.add_argument("--output-parent", type=Path, default=DEFAULT_OUTPUT_PARENT)
    parser.add_argument("--run-id", default=DEFAULT_RUN_ID)
    return parser


def main() -> None:
    output_root = run(_parser().parse_args())
    print(json.dumps({"axis_id": AXIS_ID, "artifact_root": str(output_root)}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
