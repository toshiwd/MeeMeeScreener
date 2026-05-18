from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SCHEMA_PREFIX = "sell_monthly_breakout_hard_filter_compare_v1"
DEFAULT_SOURCE_ROOT = Path(
    r"G:\Tradex\sell_side_multiyear_top5_candidate_outcome_contract_v1"
    r"\20260516T043853Z-sell-side-multiyear-top5-candidate-outcome-contract-v1"
)
DEFAULT_COMPARE_ROOT = Path(
    r"G:\Tradex\negative_selection_monthly_breakout_regime_challenger_compare_v1"
    r"\20260516T061723Z-negative-selection-monthly-breakout-regime-challenger-compare-v1"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\sell_monthly_breakout_hard_filter_compare_v1")
TOP_K = 5


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        parsed = float(value)
    except Exception:
        return None
    return parsed if math.isfinite(parsed) else None


def _load_rows(source_root: Path) -> list[dict[str, Any]]:
    path = source_root / "candidate_outcome_table_top50.jsonl"
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                rows.append(json.loads(line))
    return rows


def _load_threshold(compare_root: Path) -> float:
    definition = _read_json(compare_root / "challenger_definition.json")
    threshold = definition.get("thresholds", {}).get("monthly_breakout_up_prob_low_q25")
    parsed = _safe_float(threshold)
    if parsed is None:
        raise RuntimeError("monthly_breakout_up_prob_low_q25 threshold not found")
    return parsed


def _top_rows(rows: list[dict[str, Any]], *, top_k: int) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda row: int(row.get("rank") or 10**9))[:top_k]


def _is_low_monthly_up_bucket(row: dict[str, Any], threshold: float) -> bool:
    value = _safe_float(row.get("monthly_breakout_up_prob"))
    return value is not None and value <= threshold


def _select_hard_filter(rows: list[dict[str, Any]], *, threshold: float, top_k: int) -> list[dict[str, Any]]:
    ranked = sorted(rows, key=lambda row: int(row.get("rank") or 10**9))
    survivors = [row for row in ranked if not _is_low_monthly_up_bucket(row, threshold)]
    return survivors[:top_k]


def _summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    returns = [_safe_float(row.get("short_ret20_next_open_to_20d_close")) for row in rows]
    returns = [float(value) for value in returns if value is not None]
    bad = [bool(row.get("bad_pick")) for row in rows]
    severe = [bool(row.get("severe_loser")) for row in rows]
    return {
        "count": len(rows),
        "hit_rate": None if not returns else sum(1 for value in returns if value > 0.0) / len(returns),
        "mean_short_ret20": None if not returns else sum(returns) / len(returns),
        "bad_pick_count": sum(1 for value in bad if value),
        "bad_pick_rate": None if not bad else sum(1 for value in bad if value) / len(bad),
        "severe_loser_count": sum(1 for value in severe if value),
        "severe_loser_rate": None if not severe else sum(1 for value in severe if value) / len(severe),
    }


def _delta(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    keys = ["hit_rate", "mean_short_ret20", "bad_pick_rate", "severe_loser_rate"]
    out: dict[str, Any] = {}
    for key in keys:
        if left.get(key) is None or right.get(key) is None:
            out[f"{key}_delta"] = None
        else:
            out[f"{key}_delta"] = float(right[key]) - float(left[key])
    out["bad_pick_delta"] = int(right["bad_pick_count"]) - int(left["bad_pick_count"])
    out["severe_loser_delta"] = int(right["severe_loser_count"]) - int(left["severe_loser_count"])
    return out


def _yearly(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[str(row.get("year"))].append(row)
    return {year: _summary(items) for year, items in sorted(groups.items())}


def _evaluate(rows: list[dict[str, Any]], *, threshold: float) -> dict[str, Any]:
    by_date: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row.get("side") == "sell" and row.get("execution_available") is True:
            by_date[int(row["as_of_date"])].append(row)

    baseline: list[dict[str, Any]] = []
    challenger: list[dict[str, Any]] = []
    changed_top5_members_count = 0
    changed_rank_count = 0
    insufficient_refill_dates = 0
    monthly_change: dict[str, int] = defaultdict(int)
    filtered_candidate_count = 0

    for as_of_date, date_rows in sorted(by_date.items()):
        base = _top_rows(date_rows, top_k=TOP_K)
        chal = _select_hard_filter(date_rows, threshold=threshold, top_k=TOP_K)
        if len(chal) < TOP_K:
            insufficient_refill_dates += 1
        baseline.extend(base)
        challenger.extend(chal)
        base_codes = [str(row["code"]) for row in base]
        chal_codes = [str(row["code"]) for row in chal]
        changed = len(set(base_codes).symmetric_difference(chal_codes))
        changed_top5_members_count += changed
        monthly_change[str(date_rows[0].get("month"))] += changed
        for code in set(base_codes).intersection(chal_codes):
            changed_rank_count += abs(base_codes.index(code) - chal_codes.index(code))
        filtered_candidate_count += sum(1 for row in base if _is_low_monthly_up_bucket(row, threshold))

    base_summary = _summary(baseline)
    chal_summary = _summary(challenger)
    deltas = _delta(base_summary, chal_summary)
    yearly_baseline = _yearly(baseline)
    yearly_challenger = _yearly(challenger)
    improvement_years = 0
    worse_years = 0
    for year, chal in yearly_challenger.items():
        base = yearly_baseline.get(year, {})
        if base.get("mean_short_ret20") is None or chal.get("mean_short_ret20") is None:
            continue
        diff = float(chal["mean_short_ret20"]) - float(base["mean_short_ret20"])
        if diff > 0:
            improvement_years += 1
        elif diff < 0:
            worse_years += 1

    blockers: list[str] = []
    if changed_top5_members_count <= 0:
        blockers.append("no_top5_branching")
    if (deltas.get("mean_short_ret20_delta") or 0.0) <= 0.0:
        blockers.append("mean_short_ret20_not_improved")
    if (deltas.get("hit_rate_delta") or 0.0) < 0.0:
        blockers.append("hit_rate_worse")
    if int(deltas["bad_pick_delta"]) >= 0:
        blockers.append("bad_pick_not_reduced")
    if int(deltas["severe_loser_delta"]) > 0:
        blockers.append("severe_loser_added")
    if worse_years > 0:
        blockers.append("yearly_stability_worse_year_present")
    if insufficient_refill_dates > 0:
        blockers.append("hard_filter_created_underfilled_top5_dates")

    if not blockers:
        decision = "keep_for_portfolio_replay"
    elif changed_top5_members_count > 0 and int(deltas["bad_pick_delta"]) < 0 and (deltas.get("mean_short_ret20_delta") or 0.0) > 0.0:
        decision = "hold_for_risk_repair_or_refill_rule"
    else:
        decision = "drop_hard_filter"

    return {
        "baseline": base_summary,
        "challenger": chal_summary,
        "delta": deltas
        | {
            "changed_top5_members_count": changed_top5_members_count,
            "changed_rank_count": changed_rank_count,
            "filtered_baseline_top5_candidate_count": filtered_candidate_count,
            "insufficient_refill_dates": insufficient_refill_dates,
        },
        "yearly_baseline": yearly_baseline,
        "yearly_challenger": yearly_challenger,
        "monthly_changed_top5_members_count": dict(sorted(monthly_change.items())),
        "decision": decision,
        "blockers": blockers,
        "improvement_year_count": improvement_years,
        "worse_year_count": worse_years,
    }


def run(
    *,
    source_root: str | Path = DEFAULT_SOURCE_ROOT,
    compare_root: str | Path = DEFAULT_COMPARE_ROOT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
) -> dict[str, Any]:
    source_root = Path(source_root)
    compare_root = Path(compare_root)
    output_root = Path(output_root)
    threshold = _load_threshold(compare_root)
    rows = _load_rows(source_root)
    result = _evaluate(rows, threshold=threshold)
    run_dir = output_root / f"{_utc_stamp()}-sell-monthly-breakout-hard-filter-compare-v1"

    contract = {
        "schema_version": f"{SCHEMA_PREFIX}_contract_v1",
        "axis": "monthly_breakout_up_prob_low_q25_hard_filter",
        "source_root": str(source_root),
        "source_compare_root": str(compare_root),
        "fixed_evaluation_conditions": {
            "same_universe": True,
            "same_period": True,
            "same_top_k": True,
            "same_regime_condition": True,
            "same_cost_slippage": True,
            "same_artifact_detail_level": True,
        },
        "threshold": threshold,
        "threshold_source": "source challenger_definition.json monthly_breakout_up_prob_low_q25",
        "non_scope": ["MeeMee", "production ranking", "active champion", "publish", "live sell signal", "threshold tuning"],
        "silent_fallback_used": False,
        "research_fallback": False,
    }
    audit = {
        "schema_version": f"{SCHEMA_PREFIX}_no_lookahead_audit_v1",
        "no_lookahead_pass": True,
        "selection_fields": ["as_of_date", "rank", "side", "execution_available", "monthly_breakout_up_prob"],
        "future_outcome_fields_used_in_selection": [],
        "future_outcome_fields_evaluation_only": [
            "short_ret20_next_open_to_20d_close",
            "bad_pick",
            "severe_loser",
            "good_candidate",
            "exit_close",
            "exit_date",
        ],
        "silent_fallback_used": False,
        "research_fallback": False,
    }
    decision = {
        "schema_version": f"{SCHEMA_PREFIX}_decision_v1",
        "generated_at": _utc_now(),
        "authoritative_rollup_decision": result["decision"],
        "decision": result["decision"],
        "blockers": result["blockers"],
        "candidate_name": "monthly_breakout_up_prob_low_q25_hard_filter_v1",
        "source_candidate_name": "monthly_breakout_up_prob_regime_short_risk_guard_soft_demotion_v1",
        "portfolio_replay_allowed_next": result["decision"] == "keep_for_portfolio_replay",
        "active_champion_changed": False,
        "production_ranking_changed": False,
        "publish_run": False,
        "live_sell_signal_added": False,
        "silent_fallback_used": False,
        "research_fallback": False,
    }
    compare = {
        "schema_version": f"{SCHEMA_PREFIX}_compare_v1",
        "generated_at": _utc_now(),
        "contract": contract,
        "no_lookahead": audit,
        **result,
    }
    paths = {
        "contract": run_dir / "hard_filter_contract.json",
        "compare": run_dir / "hard_filter_compare.json",
        "decision": run_dir / "hard_filter_decision.json",
        "no_lookahead_audit": run_dir / "hard_filter_no_lookahead_audit.json",
    }
    _write_json(paths["contract"], contract)
    _write_json(paths["compare"], compare)
    _write_json(paths["decision"], decision)
    _write_json(paths["no_lookahead_audit"], audit)
    complete = {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "generated_at": _utc_now(),
        "status": "complete",
        "complete": True,
        "artifact_refs": {key: str(path) for key, path in paths.items()},
        "authoritative_decision": str(paths["decision"]),
        "authoritative_compare": str(paths["compare"]),
        "decision": result["decision"],
        "silent_fallback_used": False,
        "research_fallback": False,
        "production_ranking_changed": False,
        "active_champion_changed": False,
        "publish_run": False,
        "live_sell_signal_added": False,
    }
    _write_json(run_dir / "_ARTIFACT_COMPLETE.json", complete)
    return {
        "ok": True,
        "output_dir": str(run_dir),
        "decision": result["decision"],
        "artifact_refs": complete["artifact_refs"] | {"_ARTIFACT_COMPLETE": str(run_dir / "_ARTIFACT_COMPLETE.json")},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="TRADEX-only sell monthly breakout hard-filter fixed-condition compare.")
    parser.add_argument("--source-root", default=str(DEFAULT_SOURCE_ROOT))
    parser.add_argument("--compare-root", default=str(DEFAULT_COMPARE_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args()
    result = run(source_root=args.source_root, compare_root=args.compare_root, output_root=args.output_root)
    print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
