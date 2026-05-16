from __future__ import annotations

import argparse
import importlib
import json
import statistics
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

base = importlib.import_module("scripts.entry_precision_short_audit")

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\sell_failed_followthrough_refill_rerun_v1")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        parsed = float(value)
    except Exception:
        return None
    return parsed if parsed == parsed else None


def _mean(values: list[float]) -> float | None:
    return float(statistics.fmean(values)) if values else None


def _median(values: list[float]) -> float | None:
    return float(statistics.median(values)) if values else None


def _is_failed_followthrough(row: dict[str, Any]) -> bool:
    short_ret_5 = _safe_float(row.get("short_ret_5"))
    short_ret_10 = _safe_float(row.get("short_ret_10"))
    return short_ret_5 is not None and short_ret_10 is not None and short_ret_5 <= 0.0 and short_ret_10 <= 0.0


def _row_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    trade_priority = _safe_float(row.get("tradePriorityScore"))
    entry = _safe_float(row.get("entryScore")) or 0.0
    liquidity = _safe_float(row.get("liquidity20d")) or 0.0
    return (
        trade_priority is None,
        -(trade_priority or 0.0),
        -entry,
        -liquidity,
        str(row.get("code") or ""),
    )


def _metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    ret20 = [_safe_float(row.get("short_ret_20")) for row in rows]
    ret20 = [value for value in ret20 if value is not None]
    ret5 = [_safe_float(row.get("short_ret_5")) for row in rows]
    ret5 = [value for value in ret5 if value is not None]
    mae = [_safe_float(row.get("mae20")) for row in rows]
    mae = [value for value in mae if value is not None]
    mfe = [_safe_float(row.get("mfe20")) for row in rows]
    mfe = [value for value in mfe if value is not None]
    return {
        "count": len(rows),
        "hit_rate": None if not ret20 else sum(1 for value in ret20 if value > 0.0) / len(ret20),
        "mean_ret20": _mean(ret20),
        "median_ret20": _median(ret20),
        "severe_loser_rate": None if not ret20 else sum(1 for value in ret20 if value <= -0.05) / len(ret20),
        "immediate_reverse_rate": None if not ret5 else sum(1 for value in ret5 if value <= 0.0) / len(ret5),
        "mean_mae20": _mean(mae),
        "median_mae20": _median(mae),
        "mean_mfe20": _mean(mfe),
        "median_mfe20": _median(mfe),
    }


def _delta(left: Any, right: Any) -> float | None:
    if left is None or right is None:
        return None
    return float(right) - float(left)


def _metric_deltas(baseline: dict[str, Any], challenger: dict[str, Any]) -> dict[str, Any]:
    keys = [
        "hit_rate",
        "mean_ret20",
        "median_ret20",
        "severe_loser_rate",
        "immediate_reverse_rate",
        "mean_mae20",
        "median_mae20",
        "mean_mfe20",
        "median_mfe20",
    ]
    return {f"{key}_delta": _delta(baseline.get(key), challenger.get(key)) for key in keys}


def _group_by_month(rows: list[dict[str, Any]]) -> dict[int, list[dict[str, Any]]]:
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row.get("ymd") is None:
            continue
        grouped[int(row["ymd"])].append(row)
    return grouped


def _monthly_stability(rows: list[dict[str, Any]]) -> dict[str, Any]:
    out: dict[str, float] = {}
    for ymd, month_rows in sorted(_group_by_month(rows).items()):
        vals = [_safe_float(row.get("short_ret_20")) for row in month_rows]
        clean = [value for value in vals if value is not None]
        if clean:
            out[str(ymd)] = float(statistics.median(clean))
    return {
        "months_with_selection": len(out),
        "positive_months": sum(1 for value in out.values() if value > 0.0),
        "negative_months": sum(1 for value in out.values() if value < 0.0),
        "monthly_median_ret20": out,
    }


def _regime_stability(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get("marketRegime") or "unknown")].append(row)
    return [{"regime": regime, **_metrics(regime_rows)} for regime, regime_rows in sorted(grouped.items())]


def _candidate_pool_eligible(row: dict[str, Any], *, refill_liquidity20d_min: float = 0.0) -> bool:
    if row.get("short_ret_20") is None:
        return False
    if row.get("event_risk_short"):
        return False
    if row.get("borrow_proxy_unfavorable"):
        return False
    if _is_failed_followthrough(row):
        return False
    if refill_liquidity20d_min > 0.0 and (_safe_float(row.get("liquidity20d")) or 0.0) < refill_liquidity20d_min:
        return False
    return True


def _build_refill_selection(rows: list[dict[str, Any]], *, refill_liquidity20d_min: float = 0.0) -> dict[str, Any]:
    baseline_rows = [dict(row) for row in rows if row.get("selected_by_baseline")]
    all_by_month = _group_by_month(rows)
    baseline_by_month = _group_by_month(baseline_rows)
    challenger_rows: list[dict[str, Any]] = []
    monthly_rows: list[dict[str, Any]] = []
    removed_rows: list[dict[str, Any]] = []
    added_rows: list[dict[str, Any]] = []

    for ymd, base_month in sorted(baseline_by_month.items()):
        baseline_sorted = sorted(base_month, key=lambda row: int(row.get("baseline_rank") or 999999))
        removed = [row for row in baseline_sorted if _is_failed_followthrough(row)]
        kept = [row for row in baseline_sorted if not _is_failed_followthrough(row)]
        kept_codes = {str(row.get("code")) for row in kept}
        needed = max(0, len(baseline_sorted) - len(kept))
        pool = [
            row
            for row in all_by_month.get(ymd, [])
            if str(row.get("code")) not in kept_codes
            and str(row.get("code")) not in {str(item.get("code")) for item in removed}
            and _candidate_pool_eligible(row, refill_liquidity20d_min=refill_liquidity20d_min)
        ]
        pool_sorted = sorted(pool, key=_row_sort_key)
        added = [dict(row, selected_by_challenger=True, refill_source="same_month_candidate_pool") for row in pool_sorted[:needed]]
        challenger_month = sorted([dict(row, selected_by_challenger=True, refill_source="baseline_kept") for row in kept] + added, key=_row_sort_key)
        challenger_rows.extend(challenger_month)
        removed_rows.extend(removed)
        added_rows.extend(added)
        monthly_rows.append(
            {
                "ymd": ymd,
                "baseline_count": len(baseline_sorted),
                "challenger_count": len(challenger_month),
                "removed_count": len(removed),
                "added_count": len(added),
                "refill_shortfall_count": max(0, needed - len(added)),
                "baseline_top10": [str(row.get("code")) for row in baseline_sorted[:10]],
                "challenger_top10": [str(row.get("code")) for row in challenger_month[:10]],
                "removed_codes": [str(row.get("code")) for row in removed],
                "added_codes": [str(row.get("code")) for row in added],
            }
        )

    return {
        "baseline_rows": baseline_rows,
        "challenger_rows": challenger_rows,
        "removed_rows": removed_rows,
        "added_rows": added_rows,
        "monthly_rows": monthly_rows,
    }


def _branching(selection: dict[str, Any]) -> dict[str, Any]:
    baseline_by_month = _group_by_month(selection["baseline_rows"])
    challenger_by_month = _group_by_month(selection["challenger_rows"])
    changed_top5 = 0
    changed_top10 = 0
    changed_rank = 0
    for ymd, base_month in sorted(baseline_by_month.items()):
        base_codes = [str(row.get("code")) for row in sorted(base_month, key=lambda row: int(row.get("baseline_rank") or 999999))]
        chal_codes = [str(row.get("code")) for row in sorted(challenger_by_month.get(ymd, []), key=_row_sort_key)]
        changed_top5 += len(set(base_codes[:5]).symmetric_difference(set(chal_codes[:5])))
        changed_top10 += len(set(base_codes[:10]).symmetric_difference(set(chal_codes[:10])))
        for code in set(base_codes[:20]).intersection(chal_codes[:20]):
            changed_rank += abs(base_codes.index(code) - chal_codes.index(code))
    removed = selection["removed_rows"]
    added = selection["added_rows"]
    return {
        "changed_top5_members_count": changed_top5,
        "changed_top10_members_count": changed_top10,
        "changed_rank_count": changed_rank,
        "bad_pick_removal_count": sum(1 for row in removed if (_safe_float(row.get("short_ret_20")) or 0.0) <= 0.0),
        "severe_loser_removal_count": sum(1 for row in removed if (_safe_float(row.get("short_ret_20")) or 0.0) <= -0.05),
        "added_bad_pick_count": sum(1 for row in added if (_safe_float(row.get("short_ret_20")) or 0.0) <= 0.0),
        "added_severe_loser_count": sum(1 for row in added if (_safe_float(row.get("short_ret_20")) or 0.0) <= -0.05),
        "selection_divergence_reason": "same_month_refill_branching_observed" if changed_top5 or changed_top10 or changed_rank else "no_meaningful_branching",
    }


def _decision(compare: dict[str, Any]) -> dict[str, Any]:
    delta = compare["delta"]
    monthly = compare["monthly_stability"]
    regime = compare["regime_stability"]
    blockers: list[str] = []
    if delta["changed_top5_members_count"] <= 0 or delta["changed_top10_members_count"] <= 0:
        blockers.append("no_topk_branching")
    if (delta.get("mean_ret20_delta") or 0.0) <= 0.0:
        blockers.append("mean_ret20_not_improved")
    if (delta.get("hit_rate_delta") or 0.0) < 0.0:
        blockers.append("hit_rate_worse")
    if (delta.get("severe_loser_rate_delta") or 0.0) > 0.0:
        blockers.append("severe_loser_rate_worse")
    if delta["bad_pick_removal_count"] <= 0:
        blockers.append("bad_pick_removal_absent")
    if delta["added_bad_pick_count"] > delta["bad_pick_removal_count"]:
        blockers.append("refill_added_more_bad_picks_than_removed")
    if delta["added_severe_loser_count"] > 0:
        blockers.append("refill_added_severe_loser")
    if monthly["positive_months"] <= monthly["negative_months"]:
        blockers.append("monthly_stability_not_positive")
    negative_regimes = [row for row in regime if (row.get("count") or 0) >= 3 and (row.get("mean_ret20") or 0.0) < 0.0]
    if negative_regimes:
        blockers.append("regime_negative_mean_ret20")
    if compare["challenger"]["count"] < 12:
        blockers.append("sample_thin")

    if not blockers:
        decision = "keep_as_buy_level_equivalent_research_candidate"
        reason = "same_month_refill_candidate_meets_buy_level_contract"
    elif (
        delta["bad_pick_removal_count"] > 0
        and (delta.get("mean_ret20_delta") or 0.0) > 0.0
        and (delta.get("severe_loser_rate_delta") or 0.0) <= 0.0
    ):
        decision = "hold_for_more_validation"
        reason = "refill_candidate_improves_some_metrics_but_buy_level_blockers_remain"
    else:
        decision = "drop_signal_not_buy_level_equivalent"
        reason = "refill_candidate_does_not_meet_buy_level_contract"
    return {
        "candidate_local_decision": decision,
        "authoritative_rollup_decision": decision,
        "decision_reason": reason,
        "promote_ready_equivalent": not blockers,
        "buy_level_equivalence_reached": not blockers,
        "buy_level_blockers": blockers,
    }


def run(
    *,
    db_path: str | Path | None = None,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    start_ymd: int = 20250101,
    end_ymd: int = 20260226,
    refill_liquidity20d_min: float = 0.0,
) -> dict[str, Any]:
    resolved_db = base._resolve_db_path(str(db_path) if db_path else None)
    run_dir = Path(output_root).expanduser().resolve() / f"{_utc_stamp()}-sell-failed-followthrough-refill-rerun-v1"
    with duckdb.connect(str(resolved_db), read_only=True) as conn:
        months = base._month_end_dates(conn, start_ymd=int(start_ymd), end_ymd=int(end_ymd))
        price_store = base._load_price_store(conn)
        sell_map = base._load_frame_map(conn, "sell_analysis_daily", ymd_col="dt")
        feature_map = base._load_frame_map(conn, "feature_snapshot_daily", ymd_col="dt")
        event_map = base._load_event_map(conn)
        bundle = base._build_rows(
            conn=conn,
            months=months,
            price_store=price_store,
            sell_map=sell_map,
            feature_map=feature_map,
            event_map=event_map,
        )

    selection = _build_refill_selection(bundle["rows"], refill_liquidity20d_min=float(refill_liquidity20d_min))
    baseline = _metrics(selection["baseline_rows"])
    challenger = _metrics(selection["challenger_rows"])
    compare = {
        "schema_version": "tradex_sell_failed_followthrough_refill_compare_v1",
        "generated_at": _utc_now(),
        "candidate_id": "sell_failed_followthrough_after_break_same_month_refill_v1"
        if float(refill_liquidity20d_min) <= 0.0
        else "sell_failed_followthrough_after_break_same_month_refill_liquidity_guard_v1",
        "champion_id": "current_rule_trade_gate_baseline",
        "source_db_path": str(resolved_db),
        "baseline": baseline,
        "challenger": challenger,
        "delta": {
            **_metric_deltas(baseline, challenger),
            **_branching(selection),
        },
        "monthly_stability": _monthly_stability(selection["challenger_rows"]),
        "regime_stability": _regime_stability(selection["challenger_rows"]),
        "monthly_rows": selection["monthly_rows"],
        "same_condition_contract": {
            "same_universe": True,
            "same_period": True,
            "same_top_k": True,
            "same_regime": True,
            "same_cost": True,
            "same_artifact_detail_level": True,
            "long_short_separated": True,
            "same_month_refill": True,
            "no_silent_fallback": True,
            "refill_liquidity20d_min": float(refill_liquidity20d_min),
        },
        "silent_fallback_used": False,
        "research_fallback": False,
        "meemee_reflection": False,
        "production_ranking_changed": False,
    }
    decision = {
        "schema_version": "tradex_sell_failed_followthrough_refill_decision_v1",
        "generated_at": _utc_now(),
        **_decision(compare),
        "candidate_id": compare["candidate_id"],
        "silent_fallback_used": False,
        "research_fallback": False,
        "meemee_reflection": False,
        "production_ranking_changed": False,
    }
    contract = {
        "schema_version": "tradex_sell_failed_followthrough_refill_contract_v1",
        "generated_at": _utc_now(),
        "axis": "sell_failed_followthrough_after_break_same_month_refill_v1"
        if float(refill_liquidity20d_min) <= 0.0
        else "sell_failed_followthrough_after_break_same_month_refill_liquidity_guard_v1",
        "fixed_evaluation_conditions": compare["same_condition_contract"],
        "non_scope": [
            "MeeMee UI",
            "production ranking",
            "publish",
            "buy logic",
            "MA sell probe",
        ],
        "source_db_path": str(resolved_db),
        "refill_liquidity20d_min": float(refill_liquidity20d_min),
    }
    reason_inventory = {
        "schema_version": "tradex_sell_failed_followthrough_reason_inventory_v1",
        "generated_at": _utc_now(),
        "baseline_failed_followthrough_count": sum(1 for row in selection["baseline_rows"] if _is_failed_followthrough(row)),
        "removed_failed_followthrough_count": len(selection["removed_rows"]),
        "refill_added_count": len(selection["added_rows"]),
        "refill_shortfall_count": sum(int(row["refill_shortfall_count"]) for row in selection["monthly_rows"]),
        "removed_code_counts": dict(Counter(str(row.get("code")) for row in selection["removed_rows"])),
        "added_code_counts": dict(Counter(str(row.get("code")) for row in selection["added_rows"])),
    }

    paths = {
        "contract": run_dir / "sell_failed_followthrough_refill_contract.json",
        "compare": run_dir / "sell_failed_followthrough_refill_compare.json",
        "decision": run_dir / "sell_failed_followthrough_refill_decision.json",
        "reason_inventory": run_dir / "sell_failed_followthrough_refill_reason_inventory.json",
        "complete": run_dir / "_ARTIFACT_COMPLETE.json",
    }
    _write_json(paths["contract"], contract)
    _write_json(paths["compare"], compare)
    _write_json(paths["decision"], decision)
    _write_json(paths["reason_inventory"], reason_inventory)
    _write_json(
        paths["complete"],
        {
            "schema_version": "tradex_sell_failed_followthrough_refill_complete_v1",
            "generated_at": _utc_now(),
            "status": "complete",
            "artifact_refs": {key: str(path) for key, path in paths.items() if key != "complete"},
            "authoritative_decision": str(paths["decision"]),
            "silent_fallback_used": False,
            "research_fallback": False,
        },
    )
    return {
        "ok": True,
        "output_dir": str(run_dir),
        "authoritative_decision": decision["authoritative_rollup_decision"],
        "decision_reason": decision["decision_reason"],
        "artifact_refs": {key: str(path) for key, path in paths.items()},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Dedicated sell failed-followthrough same-month refill rerun.")
    parser.add_argument("--db-path", default="")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--start-ymd", type=int, default=20250101)
    parser.add_argument("--end-ymd", type=int, default=20260226)
    parser.add_argument("--refill-liquidity20d-min", type=float, default=0.0)
    args = parser.parse_args()
    result = run(
        db_path=args.db_path or None,
        output_root=args.output_root,
        start_ymd=args.start_ymd,
        end_ymd=args.end_ymd,
        refill_liquidity20d_min=args.refill_liquidity20d_min,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
