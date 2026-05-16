from __future__ import annotations

import argparse
import json
import math
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping


AXIS_ID = "common_top5_candidate_pool_validation_subset_or_label_source_audit_v1"
SCHEMA_PREFIX = "tradex_common_top5_candidate_pool_validation_subset"
DEFAULT_SOURCE_FIELD_REPAIR_PARENT = Path("G:/Tradex/common_ledger_field_repair_v1")
DEFAULT_SOURCE_FIELD_REPAIR_RUN_ID = "20260514T230000Z-common-ledger-field-repair-v1"
DEFAULT_OUTPUT_PARENT = Path("G:/Tradex/common_top5_candidate_pool_validation_subset_or_label_source_audit_v1")
DEFAULT_RUN_ID = "20260514T233000Z-common-top5-candidate-pool-validation-subset-or-label-source-audit-v1"
TOP5_K = 5
TOP3_K = 3
BIG_WINNER_RET20_THRESHOLD = 0.10
BAD_PICK_RET20_THRESHOLD = 0.0

REQUIRED_ARTIFACTS = [
    "evaluation_contract.json",
    "run_manifest.json",
    "source_artifact_refs.json",
    "subset_validation_contract.json",
    "label_completeness_audit.json",
    "excluded_row_report.json",
    "family_distribution_after_exclusion_report.json",
    "common_top5_validation_leaderboard.json",
    "top5_candidate_pool_report.json",
    "variant_comparison_report.json",
    "guardrail_report.json",
    "human_selectable_day_report.json",
    "next_axis_recommendation.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
]

VARIANTS = {
    "baseline": "baseline_candidate_flag",
    "momentum": "momentum_candidate_flag",
    "ma5_h12": "ma5_h12_candidate_flag",
    "combined": "combined_candidate_flag",
}

FAMILY_FLAGS = [
    "baseline_candidate_flag",
    "momentum_candidate_flag",
    "ma5_h12_candidate_flag",
    "combined_candidate_flag",
]

SCORE_RANK_FIELDS = [
    "baseline_score",
    "baseline_rank",
    "momentum_score",
    "momentum_rank",
    "ma5_h12_context_score",
    "ma5_h12_rank",
    "combined_score",
    "shadow_candidate_rank",
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
            text = line.strip()
            if not text:
                continue
            try:
                rows.append(json.loads(text))
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid JSONL at {path}:{line_no}: {exc}") from exc
    return rows


def _is_available(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, float) and math.isnan(value):
        return False
    return True


def _float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes"}
    return bool(value)


def _mean(values: Iterable[Any]) -> float | None:
    nums = [_float(value) for value in values]
    nums = [value for value in nums if value is not None]
    if not nums:
        return None
    return float(sum(nums) / len(nums))


def _rate(numer: int | float, denom: int | float) -> float:
    return float(numer / denom) if denom else 0.0


def _key(row: Mapping[str, Any]) -> str:
    return f"{row.get('event_date')}::{row.get('symbol')}"


def _label_complete(row: Mapping[str, Any]) -> bool:
    return all(_is_available(row.get(field)) for field in ("ret20_fwd", "mfe20", "mae20", "severe_loss20"))


def _is_bad_pick(row: Mapping[str, Any]) -> bool:
    ret = _float(row.get("ret20_fwd"))
    return ret is None or ret <= BAD_PICK_RET20_THRESHOLD or _bool(row.get("severe_loss20"))


def _is_human_selectable(row: Mapping[str, Any]) -> bool:
    ret = _float(row.get("ret20_fwd"))
    return ret is not None and ret > 0.0 and not _bool(row.get("severe_loss20"))


def _is_big_winner(row: Mapping[str, Any]) -> bool:
    ret = _float(row.get("ret20_fwd"))
    return ret is not None and ret >= BIG_WINNER_RET20_THRESHOLD


def _sort_for_variant(variant_id: str) -> Callable[[Mapping[str, Any]], tuple[Any, ...]]:
    def baseline(row: Mapping[str, Any]) -> tuple[Any, ...]:
        score = _float(row.get("baseline_score"))
        rank = _float(row.get("baseline_rank"))
        return (0 if score is not None else 1, -(score or -999999.0), rank if rank is not None else 999999.0, str(row.get("symbol")))

    def momentum(row: Mapping[str, Any]) -> tuple[Any, ...]:
        rank = _float(row.get("momentum_rank"))
        score = _float(row.get("momentum_score"))
        return (rank if rank is not None else 999999.0, 0 if score is not None else 1, -(score or -999999.0), str(row.get("symbol")))

    def ma5(row: Mapping[str, Any]) -> tuple[Any, ...]:
        return (str(row.get("symbol")),)

    def combined(row: Mapping[str, Any]) -> tuple[Any, ...]:
        if _bool(row.get("momentum_candidate_flag")):
            rank = _float(row.get("momentum_rank"))
            score = _float(row.get("momentum_score"))
            return (0, rank if rank is not None else 999999.0, 0 if score is not None else 1, -(score or -999999.0), str(row.get("symbol")))
        return (1, str(row.get("symbol")))

    return {"baseline": baseline, "momentum": momentum, "ma5_h12": ma5, "combined": combined}[variant_id]


def _select_topk(rows: list[dict[str, Any]], variant_id: str, k: int, evaluation_dates: set[str]) -> list[dict[str, Any]]:
    flag = VARIANTS[variant_id]
    by_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        event_date = str(row.get("event_date"))
        if event_date in evaluation_dates and _bool(row.get(flag)):
            by_date[event_date].append(row)
    sorter = _sort_for_variant(variant_id)
    selected: list[dict[str, Any]] = []
    for event_date in sorted(evaluation_dates):
        day_rows = sorted(by_date.get(event_date, []), key=sorter)[:k]
        for rank, row in enumerate(day_rows, start=1):
            out = dict(row)
            out["selection_variant_id"] = variant_id
            out["selection_rank_evaluation_only"] = rank
            selected.append(out)
    return selected


def _oracle_topk(rows: list[dict[str, Any]], k: int, evaluation_dates: set[str]) -> list[dict[str, Any]]:
    by_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        event_date = str(row.get("event_date"))
        if event_date in evaluation_dates:
            by_date[event_date].append(row)
    selected: list[dict[str, Any]] = []
    for event_date in sorted(evaluation_dates):
        day_rows = sorted(
            by_date.get(event_date, []),
            key=lambda row: (-(_float(row.get("ret20_fwd")) or -999999.0), str(row.get("symbol"))),
        )[:k]
        selected.extend(day_rows)
    return selected


def _metrics(selected: list[dict[str, Any]], universe: list[dict[str, Any]], evaluation_dates: set[str], variant_id: str) -> dict[str, Any]:
    total_big = sum(1 for row in universe if _is_big_winner(row))
    total_top10 = sum(1 for row in universe if _bool(row.get("is_future_top10_by_ret20")))
    selected_big = sum(1 for row in selected if _is_big_winner(row))
    selected_top10 = sum(1 for row in selected if _bool(row.get("is_future_top10_by_ret20")))
    selected_dates = {str(row.get("event_date")) for row in selected}
    day_selectable = []
    diversity_counts = []
    for event_date in sorted(evaluation_dates):
        day_rows = [row for row in selected if str(row.get("event_date")) == event_date]
        day_selectable.append(sum(1 for row in day_rows if _is_human_selectable(row)) >= 3)
        if day_rows:
            diversity_counts.append(len({str(row.get("symbol")) for row in day_rows}))
    return {
        "variant_id": variant_id,
        "evaluation_date_count": len(evaluation_dates),
        "date_with_candidate_count": len(selected_dates),
        "top5_candidate_count": len(selected),
        "top5_avg_ret20": _mean(row.get("ret20_fwd") for row in selected),
        "top5_win_rate20": _rate(sum(1 for row in selected if _bool(row.get("win20"))), len(selected)),
        "top5_big_winner_capture_rate": _rate(selected_big, total_big),
        "top5_future_top10_capture_rate": _rate(selected_top10, total_top10),
        "top5_severe_loss_rate20": _rate(sum(1 for row in selected if _bool(row.get("severe_loss20"))), len(selected)),
        "top5_bad_pick_count": sum(1 for row in selected if _is_bad_pick(row)),
        "top5_candidate_diversity": _mean(diversity_counts),
        "human_selectable_day_rate": _rate(sum(1 for ok in day_selectable if ok), len(day_selectable)),
        "unranked_ma5_cap_policy_used": variant_id in {"ma5_h12", "combined"},
        "score_rank_fabricated": False,
    }


def _guardrail_metrics(selected: list[dict[str, Any]], baseline: list[dict[str, Any]], oracle: list[dict[str, Any]], universe: list[dict[str, Any]], variant_id: str) -> dict[str, Any]:
    avg = _mean(row.get("ret20_fwd") for row in selected)
    oracle_avg = _mean(row.get("ret20_fwd") for row in oracle)
    baseline_avg = _mean(row.get("ret20_fwd") for row in baseline)
    return {
        "variant_id": variant_id,
        "top3_candidate_count": len(selected),
        "top3_avg_ret20": avg,
        "top3_severe_loss_rate20": _rate(sum(1 for row in selected if _bool(row.get("severe_loss20"))), len(selected)),
        "baseline_top3_avg_ret20": baseline_avg,
        "baseline_top3_severe_loss_rate20": _rate(sum(1 for row in baseline if _bool(row.get("severe_loss20"))), len(baseline)),
        "oracle_top3_avg_ret20": oracle_avg,
        "oracle_top3_gap": (oracle_avg or 0.0) - (avg or 0.0),
        "selected_nonwinner_when_winner_available": _nonwinner_when_winner_available(selected, universe),
        "baseline_selected_nonwinner_when_winner_available": _nonwinner_when_winner_available(baseline, universe),
    }


def _nonwinner_when_winner_available(selected: list[dict[str, Any]], universe: list[dict[str, Any]]) -> float:
    by_date_selected: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_date_universe: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in selected:
        by_date_selected[str(row.get("event_date"))].append(row)
    for row in universe:
        by_date_universe[str(row.get("event_date"))].append(row)
    rates: list[float] = []
    for event_date, day_selected in by_date_selected.items():
        if any(_bool(row.get("win20")) for row in by_date_universe[event_date]):
            rates.append(_rate(sum(1 for row in day_selected if not _bool(row.get("win20"))), len(day_selected)))
    return _mean(rates) or 0.0


def _delta(value: Any, baseline: Any) -> float | None:
    left = _float(value)
    right = _float(baseline)
    if left is None or right is None:
        return None
    return float(left - right)


def _label_completeness(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    complete = [row for row in rows if _label_complete(row)]
    incomplete = [row for row in rows if not _label_complete(row)]
    excluded_by_date = Counter(str(row.get("event_date")) for row in incomplete)
    excluded_by_family = {
        field: sum(1 for row in incomplete if _bool(row.get(field)))
        for field in FAMILY_FLAGS
    }
    unique_dates = sorted({str(row.get("event_date")) for row in rows})
    latest_dates = set(unique_dates[-20:])
    excluded_dates = set(excluded_by_date)
    audit = {
        "schema_version": f"{SCHEMA_PREFIX}_label_completeness_audit_v1",
        "axis_id": AXIS_ID,
        "source_row_count": len(rows),
        "label_complete_row_count": len(complete),
        "label_incomplete_row_count": len(incomplete),
        "coverage_rate": _rate(len(complete), len(rows)),
        "excluded_due_to_incomplete_forward_window_count": len(incomplete),
        "excluded_rows_by_date": dict(sorted(excluded_by_date.items())),
        "excluded_rows_by_family": excluded_by_family,
        "excluded_rows_concentrated_in_latest_dates": bool(excluded_dates) and excluded_dates.issubset(latest_dates),
        "latest_date_count_window_for_concentration": 20,
        "label_complete_rows_used_only": True,
        "label_incomplete_rows_excluded": True,
    }
    return complete, incomplete, audit


def _family_distribution(rows: list[dict[str, Any]], complete: list[dict[str, Any]], incomplete: list[dict[str, Any]]) -> dict[str, Any]:
    before_counts = {field: sum(1 for row in rows if _bool(row.get(field))) for field in FAMILY_FLAGS}
    after_counts = {field: sum(1 for row in complete if _bool(row.get(field))) for field in FAMILY_FLAGS}
    excluded_counts = {field: sum(1 for row in incomplete if _bool(row.get(field))) for field in FAMILY_FLAGS}
    rows_out = []
    material = False
    for field in FAMILY_FLAGS:
        before_rate = _rate(before_counts[field], len(rows))
        after_rate = _rate(after_counts[field], len(complete))
        delta = after_rate - before_rate
        if abs(delta) > 0.02:
            material = True
        rows_out.append(
            {
                "family_flag": field,
                "before_count": before_counts[field],
                "after_count": after_counts[field],
                "excluded_count": excluded_counts[field],
                "before_rate": before_rate,
                "after_rate": after_rate,
                "after_minus_before_rate_delta": delta,
            }
        )
    return {
        "schema_version": f"{SCHEMA_PREFIX}_family_distribution_after_exclusion_report_v1",
        "axis_id": AXIS_ID,
        "rows": rows_out,
        "family_distribution_materially_changed": material,
        "materiality_threshold_abs_rate_delta": 0.02,
    }


def _run_validation(rows: list[dict[str, Any]]) -> dict[str, Any]:
    complete, incomplete, completeness = _label_completeness(rows)
    baseline_dates = {str(row.get("event_date")) for row in complete if _bool(row.get("baseline_candidate_flag"))}
    combined_dates = {str(row.get("event_date")) for row in complete if _bool(row.get("combined_candidate_flag"))}
    evaluation_dates = baseline_dates & combined_dates
    universe = [row for row in complete if str(row.get("event_date")) in evaluation_dates]
    selected_top5 = {variant: _select_topk(complete, variant, TOP5_K, evaluation_dates) for variant in VARIANTS}
    selected_top3 = {variant: _select_topk(complete, variant, TOP3_K, evaluation_dates) for variant in VARIANTS}
    oracle_top3 = _oracle_topk(universe, TOP3_K, evaluation_dates)
    top5_rows = [_metrics(selected_top5[variant], universe, evaluation_dates, variant) for variant in VARIANTS]
    guard_rows = [
        _guardrail_metrics(selected_top3[variant], selected_top3["baseline"], oracle_top3, universe, variant)
        for variant in VARIANTS
    ]
    by_variant = {row["variant_id"]: row for row in top5_rows}
    guard_by_variant = {row["variant_id"]: row for row in guard_rows}
    baseline = by_variant["baseline"]
    baseline_keys = {_key(row) for row in selected_top5["baseline"]}
    comparison_rows = []
    for variant in ["momentum", "ma5_h12", "combined"]:
        row = by_variant[variant]
        keys = {_key(item) for item in selected_top5[variant]}
        guard = guard_by_variant[variant]
        base_guard = guard_by_variant["baseline"]
        comparison_rows.append(
            {
                "variant_id": variant,
                "top5_avg_ret20_delta_vs_baseline": _delta(row["top5_avg_ret20"], baseline["top5_avg_ret20"]),
                "top5_win_rate20_delta_vs_baseline": _delta(row["top5_win_rate20"], baseline["top5_win_rate20"]),
                "top5_big_winner_capture_rate_delta_vs_baseline": _delta(row["top5_big_winner_capture_rate"], baseline["top5_big_winner_capture_rate"]),
                "top5_future_top10_capture_rate_delta_vs_baseline": _delta(row["top5_future_top10_capture_rate"], baseline["top5_future_top10_capture_rate"]),
                "top5_severe_loss_rate_delta_vs_baseline": _delta(row["top5_severe_loss_rate20"], baseline["top5_severe_loss_rate20"]),
                "top5_bad_pick_count_delta_vs_baseline": int(row["top5_bad_pick_count"]) - int(baseline["top5_bad_pick_count"]),
                "human_selectable_day_rate_delta_vs_baseline": _delta(row["human_selectable_day_rate"], baseline["human_selectable_day_rate"]),
                "top5_changed_members_count_vs_baseline": len(keys.symmetric_difference(baseline_keys)),
                "candidate_added_count": len(keys - baseline_keys),
                "candidate_overlap_with_baseline_top5": len(keys & baseline_keys),
                "top3_avg_ret20_delta_vs_baseline": _delta(guard["top3_avg_ret20"], base_guard["top3_avg_ret20"]),
                "top3_severe_loss_rate_delta_vs_baseline": _delta(guard["top3_severe_loss_rate20"], base_guard["top3_severe_loss_rate20"]),
                "oracle_top3_gap": guard["oracle_top3_gap"],
                "selected_nonwinner_when_winner_available_delta_vs_baseline": _delta(
                    guard["selected_nonwinner_when_winner_available"],
                    base_guard["selected_nonwinner_when_winner_available"],
                ),
                "unranked_ma5_cap_policy_used": row["unranked_ma5_cap_policy_used"],
            }
        )
    return {
        "complete": complete,
        "incomplete": incomplete,
        "completeness": completeness,
        "family_distribution": _family_distribution(rows, complete, incomplete),
        "evaluation_dates": evaluation_dates,
        "universe": universe,
        "selected_top5": selected_top5,
        "selected_top3": selected_top3,
        "oracle_top3": oracle_top3,
        "top5_rows": top5_rows,
        "guard_rows": guard_rows,
        "comparison_rows": comparison_rows,
    }


def _decision(validation: Mapping[str, Any]) -> dict[str, Any]:
    completeness = validation["completeness"]
    distribution = validation["family_distribution"]
    coverage_ok = completeness["coverage_rate"] >= 0.95
    distribution_ok = not distribution["family_distribution_materially_changed"]
    variant_decisions = []
    for row in validation["comparison_rows"]:
        avg_ok = (row["top5_avg_ret20_delta_vs_baseline"] or 0.0) > 0.0
        big_ok = (row["top5_big_winner_capture_rate_delta_vs_baseline"] or 0.0) > 0.0
        top10_ok = (row["top5_future_top10_capture_rate_delta_vs_baseline"] or 0.0) > 0.0
        severe_ok = (row["top5_severe_loss_rate_delta_vs_baseline"] or 0.0) <= 0.0
        bad_ok = int(row["top5_bad_pick_count_delta_vs_baseline"]) <= 0
        human_ok = (row["human_selectable_day_rate_delta_vs_baseline"] or 0.0) >= 0.0
        top3_ok = (row["top3_avg_ret20_delta_vs_baseline"] or 0.0) >= -0.02 and (row["top3_severe_loss_rate_delta_vs_baseline"] or 0.0) <= 0.03
        branch = int(row["top5_changed_members_count_vs_baseline"]) > 0 and int(row["candidate_added_count"]) > 0
        unranked_limit = bool(row["unranked_ma5_cap_policy_used"])
        if coverage_ok and distribution_ok and avg_ok and big_ok and top10_ok and severe_ok and bad_ok and human_ok and top3_ok and branch and not unranked_limit:
            decision = "keep_candidate"
            reason = "subset_top5_pool_improved_without_unranked_additive_limitation"
        elif coverage_ok and distribution_ok and avg_ok and branch and top3_ok:
            decision = "hold"
            reason = "subset_top5_pool_branching_or_return_positive_but_risk_capture_or_unranked_limit_remains"
        else:
            decision = "drop"
            reason = "subset_top5_pool_quality_not_improved_enough"
        variant_decisions.append({"variant_id": row["variant_id"], "decision": decision, "decision_reason": reason, "comparison": row})
    keep = [row for row in variant_decisions if row["decision"] == "keep_candidate"]
    hold = [row for row in variant_decisions if row["decision"] == "hold"]
    if keep:
        decision = "keep_candidate"
        authoritative = "subset_top5_validation_keep_candidate"
        best = keep[0]["variant_id"]
        typed = ["label_complete_subset_coverage_sufficient", "top5_pool_quality_improved"]
    elif hold:
        decision = "hold"
        authoritative = "subset_top5_validation_hold"
        best = hold[0]["variant_id"]
        typed = ["subset_validation_has_positive_branching_but_requires_decomposition_or_full_labels"]
    else:
        decision = "drop"
        authoritative = "subset_top5_validation_drop"
        best = variant_decisions[0]["variant_id"] if variant_decisions else None
        typed = ["no_variant_improved_subset_top5_candidate_pool_enough"]
    if not coverage_ok:
        decision = "hold"
        authoritative = "subset_top5_validation_hold"
        typed.append("label_complete_subset_coverage_insufficient")
    if not distribution_ok:
        decision = "hold"
        authoritative = "subset_top5_validation_hold"
        typed.append("excluded_rows_change_family_distribution_materially")
    return {
        "decision": decision,
        "authoritative_research_decision": authoritative,
        "best_variant_id": best,
        "variant_decisions": variant_decisions,
        "typed_reasons": typed,
    }


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run subset-only common top5 candidate pool validation.")
    parser.add_argument("--source-field-repair-run-id", default=DEFAULT_SOURCE_FIELD_REPAIR_RUN_ID)
    parser.add_argument("--source-field-repair-parent", type=Path, default=DEFAULT_SOURCE_FIELD_REPAIR_PARENT)
    parser.add_argument("--output-parent", type=Path, default=DEFAULT_OUTPUT_PARENT)
    parser.add_argument("--run-id", default=DEFAULT_RUN_ID)
    return parser


def run(args: argparse.Namespace) -> Path:
    source_root = args.source_field_repair_parent / args.source_field_repair_run_id
    output_root = args.output_parent / args.run_id
    output_root.mkdir(parents=True, exist_ok=True)
    source_decision = _read_json(source_root / "research_decision.json")
    source_complete = _read_json(source_root / "_ARTIFACT_COMPLETE.json")
    rows = _read_jsonl(source_root / "repaired_common_top5_candidate_ledger.jsonl")
    validation = _run_validation(rows)
    decision = _decision(validation)
    generated_at = _utc_now()
    excluded_by_date = validation["completeness"]["excluded_rows_by_date"]
    excluded_by_family = validation["completeness"]["excluded_rows_by_family"]
    top5_by_variant = {row["variant_id"]: row for row in validation["top5_rows"]}
    guard_by_variant = {row["variant_id"]: row for row in validation["guard_rows"]}

    payloads: dict[str, dict[str, Any]] = {
        "evaluation_contract.json": {
            "schema_version": f"{SCHEMA_PREFIX}_evaluation_contract_v1",
            "axis_id": AXIS_ID,
            "boundary": "TRADEX-only",
            "validation_scope": "subset_only",
            "full_validation_claimed": False,
            "label_complete_rows_used_only": True,
            "label_incomplete_rows_excluded": True,
            "top5_policy": {
                "baseline": "baseline_score_desc_no_score_fabrication",
                "momentum": "momentum_rank_then_score",
                "ma5_h12": "unranked_membership_symbol_order_cap_at_5",
                "combined": "momentum_ranked_first_then_unranked_ma5_symbol_order",
            },
            "candidate_construction_changed": False,
            "membership_flags_changed": False,
            "future_labels_used_for_evaluation_only": True,
            "future_labels_used_in_candidate_construction": False,
        },
        "run_manifest.json": {
            "schema_version": f"{SCHEMA_PREFIX}_run_manifest_v1",
            "axis_id": AXIS_ID,
            "run_id": args.run_id,
            "generated_at_utc": generated_at,
            "source_field_repair_root": str(source_root),
            "output_root": str(output_root),
            "source_row_count": len(rows),
            "evaluation_date_count": len(validation["evaluation_dates"]),
            "label_complete_row_count": validation["completeness"]["label_complete_row_count"],
            "label_incomplete_row_count": validation["completeness"]["label_incomplete_row_count"],
        },
        "source_artifact_refs.json": {
            "schema_version": f"{SCHEMA_PREFIX}_source_artifact_refs_v1",
            "source_field_repair_root": str(source_root),
            "source_field_repair_decision": source_decision.get("authoritative_research_decision"),
            "source_artifact_complete": source_complete.get("complete"),
            "source_repaired_ledger": str(source_root / "repaired_common_top5_candidate_ledger.jsonl"),
        },
        "subset_validation_contract.json": {
            "schema_version": f"{SCHEMA_PREFIX}_subset_validation_contract_v1",
            "subset_only_validation": True,
            "full_validation_claimed": False,
            "excluded_rows_are_not_silent": True,
            "label_complete_definition": ["ret20_fwd", "mfe20", "mae20", "severe_loss20"],
            "ma5_exit_labels_used_as_ret20_labels": False,
            "score_rank_fabricated": False,
            "unranked_ma5_cap_policy_explicit": True,
            "starter_entry_pretest_run": False,
        },
        "label_completeness_audit.json": validation["completeness"],
        "excluded_row_report.json": {
            "schema_version": f"{SCHEMA_PREFIX}_excluded_row_report_v1",
            "axis_id": AXIS_ID,
            "excluded_due_to_incomplete_forward_window_count": validation["completeness"]["excluded_due_to_incomplete_forward_window_count"],
            "excluded_rows_by_date": excluded_by_date,
            "excluded_rows_by_family": excluded_by_family,
            "excluded_rows_concentrated_in_latest_dates": validation["completeness"]["excluded_rows_concentrated_in_latest_dates"],
            "sample_excluded_rows": [
                {
                    "event_date": row.get("event_date"),
                    "symbol": row.get("symbol"),
                    "label_unavailable_reason": row.get("label_unavailable_reason"),
                    "source_family_flags": row.get("source_family_flags"),
                }
                for row in validation["incomplete"][:100]
            ],
        },
        "family_distribution_after_exclusion_report.json": validation["family_distribution"],
        "common_top5_validation_leaderboard.json": {
            "schema_version": f"{SCHEMA_PREFIX}_leaderboard_v1",
            "axis_id": AXIS_ID,
            "rows": sorted(validation["top5_rows"], key=lambda row: (-(row["top5_avg_ret20"] or -999.0), row["variant_id"])),
            "comparison_vs_baseline": validation["comparison_rows"],
        },
        "top5_candidate_pool_report.json": {
            "schema_version": f"{SCHEMA_PREFIX}_top5_candidate_pool_report_v1",
            "axis_id": AXIS_ID,
            "evaluation_date_count": len(validation["evaluation_dates"]),
            "variant_metrics": validation["top5_rows"],
            "baseline_metrics": top5_by_variant["baseline"],
        },
        "variant_comparison_report.json": {
            "schema_version": f"{SCHEMA_PREFIX}_variant_comparison_report_v1",
            "axis_id": AXIS_ID,
            "rows": validation["comparison_rows"],
        },
        "guardrail_report.json": {
            "schema_version": f"{SCHEMA_PREFIX}_guardrail_report_v1",
            "axis_id": AXIS_ID,
            "rows": validation["guard_rows"],
            "baseline_guardrail": guard_by_variant["baseline"],
        },
        "human_selectable_day_report.json": {
            "schema_version": f"{SCHEMA_PREFIX}_human_selectable_day_report_v1",
            "axis_id": AXIS_ID,
            "rows": [
                {
                    "variant_id": variant,
                    "human_selectable_day_rate": top5_by_variant[variant]["human_selectable_day_rate"],
                    "delta_vs_baseline": _delta(top5_by_variant[variant]["human_selectable_day_rate"], top5_by_variant["baseline"]["human_selectable_day_rate"]),
                }
                for variant in VARIANTS
            ],
        },
    }
    next_axis = "starter_entry_candidate_pretest_v1" if decision["decision"] == "keep_candidate" else (
        "full_label_completion_wait_or_incremental_repair_v1" if decision["decision"] == "hold" else "pattern_family_portfolio_refresh_v2"
    )
    payloads["next_axis_recommendation.json"] = {
        "schema_version": f"{SCHEMA_PREFIX}_next_axis_recommendation_v1",
        "axis_id": AXIS_ID,
        "decision": decision["decision"],
        "next": next_axis,
        "reason": decision["authoritative_research_decision"],
    }
    payloads["research_decision.json"] = {
        "schema_version": f"{SCHEMA_PREFIX}_research_decision_v1",
        "generated_at_utc": generated_at,
        "research_phase": "common_top5_candidate_pool_validation_subset_or_label_source_audit",
        "boundary": "TRADEX-only",
        "axis_moved": "common_top5_candidate_pool_validation_subset",
        "source_field_repair_decision": "common_ledger_field_repair_hold",
        "subset_only_validation": True,
        "full_validation_claimed": False,
        "label_complete_rows_used_only": True,
        "label_incomplete_rows_excluded": True,
        "label_complete_row_count": validation["completeness"]["label_complete_row_count"],
        "label_incomplete_row_count": validation["completeness"]["label_incomplete_row_count"],
        "coverage_rate": validation["completeness"]["coverage_rate"],
        "candidate_construction_changed": False,
        "membership_flags_changed": False,
        "ma5_exit_labels_used_as_ret20_labels": False,
        "score_rank_fabricated": False,
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
        "future_labels_used_for_evaluation_only": True,
        "future_labels_used_in_candidate_construction": False,
        "silent_fallback_used": False,
        "research_fallback_used": False,
        "decision": decision["decision"],
        "authoritative_research_decision": decision["authoritative_research_decision"],
        "best_variant_id": decision["best_variant_id"],
        "typed_reasons": decision["typed_reasons"],
        "variant_decisions": decision["variant_decisions"],
    }
    for name, payload in payloads.items():
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


def main() -> None:
    args = _build_arg_parser().parse_args()
    output_root = run(args)
    print(json.dumps({"axis_id": AXIS_ID, "artifact_root": str(output_root)}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
