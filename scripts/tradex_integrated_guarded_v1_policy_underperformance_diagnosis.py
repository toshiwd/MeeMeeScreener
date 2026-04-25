from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any


REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


INPUT_DIR = Path(r"G:\Tradex\sample_replays\tradex_integrated_guarded_v1_stress200")
DEFAULT_OUTPUT_DIR = Path(r"G:\Tradex\sample_replays\tradex_integrated_guarded_v1_policy_underperformance")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _first_reason(row: dict[str, Any]) -> str:
    for key in (
        "exit_reason_primary",
        "entry_reason_primary",
        "add_reason_primary",
        "hedge_reason_primary",
        "cover_reason_primary",
        "flat_reason_primary",
        "trim_reason_primary",
    ):
        value = row.get(key)
        if value not in (None, ""):
            return str(value)
    return "None"


def _group_final_policy_rows(rows: list[dict[str, Any]]) -> dict[tuple[str, str, str], dict[str, Any]]:
    grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in rows:
        key = (str(row.get("anchor_date")), str(row.get("symbol")), str(row.get("side")))
        existing = grouped.get(key)
        if existing is None or str(row.get("date")) > str(existing.get("date")):
            grouped[key] = row
    return grouped


def _scale_from_records(records: list[dict[str, Any]]) -> float:
    ratios: list[float] = []
    for rec in records:
        ret = abs(float(rec.get("ret63") or 0.0))
        pnl = abs(float(rec.get("policy_net_realized_pnl") or 0.0))
        if ret > 1e-9 and pnl > 0.0:
            ratios.append(pnl / ret)
    if not ratios:
        return 1.0
    return float(median(ratios))


def _position_records(
    selection_rows: list[dict[str, Any]],
    policy_rows: list[dict[str, Any]],
    coverage_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    selection_map = {
        (str(row.get("anchor_date")), str(row.get("symbol")), str(row.get("side"))): row
        for row in selection_rows
    }
    policy_final = _group_final_policy_rows(policy_rows)
    coverage_map = {str(row.get("anchor_date")): row for row in coverage_rows}
    grouped: list[dict[str, Any]] = []
    for key, sel in selection_map.items():
        pol = policy_final.get(key)
        if pol is None:
            continue
        anchor_date, symbol, side = key
        rank_bucket = str(pol.get("rank_bucket") or sel.get("rank_bucket") or "unknown")
        grouped.append(
            {
                "anchor_date": anchor_date,
                "symbol": symbol,
                "side": side,
                "rank_bucket": rank_bucket,
                "month_bucket": str(sel.get("month_bucket") or pol.get("month_bucket") or anchor_date[:7]),
                "ret63": float(sel.get("ret63") or 0.0),
                "ret10": float(sel.get("ret10") or 0.0),
                "ret20": float(sel.get("ret20") or 0.0),
                "ret5": float(sel.get("ret5") or 0.0),
                "mfe63": float(sel.get("mfe63") or 0.0),
                "mae63": float(sel.get("mae63") or 0.0),
                "max_adverse_excursion": float(sel.get("max_adverse_excursion") or 0.0),
                "result_bucket": str(sel.get("result_bucket") or "unknown"),
                "champion_rank": sel.get("champion_rank"),
                "challenger_rank": sel.get("challenger_rank"),
                "changed_top5_member": bool(sel.get("changed_top5_member", False)),
                "changed_top10_member": bool(sel.get("changed_top10_member", False)),
                "changed_top20_member": bool(sel.get("changed_top20_member", False)),
                "policy_net_realized_pnl": float(pol.get("policy_net_realized_pnl") or 0.0),
                "policy_unrealized_pnl": float(pol.get("unrealized_pnl") or 0.0),
                "policy_realized_pnl": float(pol.get("realized_pnl") or 0.0),
                "policy_max_drawdown_during_holding": float(pol.get("policy_max_drawdown_during_holding") or 0.0),
                "selected_action": str(pol.get("selected_action") or "stay"),
                "reason": _first_reason(pol),
                "entry_reason_primary": pol.get("entry_reason_primary"),
                "exit_reason_primary": pol.get("exit_reason_primary"),
                "add_reason_primary": pol.get("add_reason_primary"),
                "hedge_reason_primary": pol.get("hedge_reason_primary"),
                "cover_reason_primary": pol.get("cover_reason_primary"),
                "flat_reason_primary": pol.get("flat_reason_primary"),
                "trim_reason_primary": pol.get("trim_reason_primary"),
                "buy_delta_units": float(pol.get("buy_delta_units") or 0.0),
                "sell_delta_units": float(pol.get("sell_delta_units") or 0.0),
                "turnover_units": abs(float(pol.get("buy_delta_units") or 0.0)) + abs(float(pol.get("sell_delta_units") or 0.0)),
                "trade_flag": 0 if str(pol.get("selected_action") or "stay") == "stay" else 1,
                "no_trade_rate": float((coverage_map.get(anchor_date) or {}).get("specialized", {}).get("no_trade_rate", 0.0)),
                "long_tradable_rate": float((coverage_map.get(anchor_date) or {}).get("specialized", {}).get("long_tradable_rate", 0.0)),
                "short_tradable_rate": float((coverage_map.get(anchor_date) or {}).get("specialized", {}).get("short_tradable_rate", 0.0)),
                "long_no_selected_symbols": int((coverage_map.get(anchor_date) or {}).get("specialized", {}).get("long_no_selected_symbols", 0)),
                "short_no_selected_symbols": int((coverage_map.get(anchor_date) or {}).get("specialized", {}).get("short_no_selected_symbols", 0)),
            }
        )

    scales: dict[tuple[str, str], float] = {}
    for bucket in ("top5", "top6_10", "top11_20"):
        for side in ("long", "short"):
            subset = [rec for rec in grouped if rec["rank_bucket"] == bucket and rec["side"] == side]
            scale = _scale_from_records(subset)
            if scale <= 0:
                scale = 1.0
            scales[(bucket, side)] = scale

    for rec in grouped:
        scale = scales.get((rec["rank_bucket"], rec["side"]), 1.0)
        rec["hold_pnl_proxy"] = rec["ret63"] * scale
        rec["policy_vs_hold_gap"] = rec["policy_net_realized_pnl"] - rec["hold_pnl_proxy"]
    return grouped


def _aggregate_positions(records: list[dict[str, Any]], group_key: str, top20_delta: float) -> list[dict[str, Any]]:
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for rec in records:
        buckets[str(rec[group_key])].append(rec)

    all_gap = sum(max(0.0, -rec["policy_vs_hold_gap"]) for rec in records) or 1.0
    out: list[dict[str, Any]] = []
    for key, rows in buckets.items():
        row_gap = sum(rec["policy_vs_hold_gap"] for rec in rows)
        negative_share = sum(max(0.0, -rec["policy_vs_hold_gap"]) for rec in rows) / all_gap
        out.append(
            {
                group_key: key,
                "count": len(rows),
                "selection_only_avg_ret63": float(mean(rec["ret63"] for rec in rows)),
                "selection_only_median_ret63": float(median(rec["ret63"] for rec in rows)),
                "bad_pick_rate": float(sum(1 for rec in rows if rec["result_bucket"] != "win") / len(rows)),
                "policy_net_realized_pnl_sum": float(sum(rec["policy_net_realized_pnl"] for rec in rows)),
                "policy_unrealized_pnl_sum": float(sum(rec["policy_unrealized_pnl"] for rec in rows)),
                "policy_realized_pnl_sum": float(sum(rec["policy_realized_pnl"] for rec in rows)),
                "policy_vs_hold_gap": float(row_gap),
                "trade_count": int(sum(rec["trade_flag"] for rec in rows)),
                "turnover": float(sum(rec["turnover_units"] for rec in rows)),
                "max_drawdown": float(min(rec["policy_max_drawdown_during_holding"] for rec in rows)),
                "mfe63_mean": float(mean(rec["mfe63"] for rec in rows)),
                "mae63_mean": float(mean(rec["mae63"] for rec in rows)),
                "worst_mae63": float(min(rec["mae63"] for rec in rows)),
                "contribution_to_top20_gap": float(top20_delta * negative_share),
                "top_contributing_symbols": sorted(
                    (
                        {
                            "symbol": rec["symbol"],
                            "side": rec["side"],
                            "rank_bucket": rec["rank_bucket"],
                            "policy_vs_hold_gap": rec["policy_vs_hold_gap"],
                            "anchor_date": rec["anchor_date"],
                            "reason": rec["reason"],
                        }
                        for rec in rows
                    ),
                    key=lambda item: item["policy_vs_hold_gap"],
                )[:10],
            }
        )
    return sorted(out, key=lambda item: item["policy_vs_hold_gap"])


def _aggregate_daily_actions(rows: list[dict[str, Any]], top20_delta: float) -> list[dict[str, Any]]:
    buckets: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        action = str(row.get("selected_action") or "stay")
        reason = _first_reason(row)
        buckets[(action, reason)].append(row)
    total_loss = sum(max(0.0, -float(row.get("realized_pnl") or 0.0)) for row in rows) or 1.0
    out: list[dict[str, Any]] = []
    for (action, reason), group in buckets.items():
        realized = [float(row.get("realized_pnl") or 0.0) for row in group]
        loss_share = sum(max(0.0, -v) for v in realized) / total_loss
        out.append(
            {
                "action": action,
                "reason": reason,
                "count": len(group),
                "realized_pnl_sum": float(sum(realized)),
                "realized_pnl_mean": float(mean(realized)),
                "win_rate": float(sum(1 for v in realized if v > 0) / len(realized)),
                "contribution_to_top20_gap": float(top20_delta * loss_share),
            }
        )
    return sorted(out, key=lambda item: item["realized_pnl_sum"])


def _aggregate_by_side(records: list[dict[str, Any]], top20_delta: float) -> list[dict[str, Any]]:
    return _aggregate_positions(records, "side", top20_delta)


def _aggregate_by_anchor(records: list[dict[str, Any]], top20_delta: float) -> list[dict[str, Any]]:
    aggregated = _aggregate_positions(records, "anchor_date", top20_delta)
    return aggregated


def _aggregate_by_symbol(records: list[dict[str, Any]], top20_delta: float) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for rec in records:
        grouped[(rec["symbol"], rec["side"], rec["rank_bucket"])].append(rec)
    total_loss = sum(max(0.0, -rec["policy_vs_hold_gap"]) for rec in records) or 1.0
    out: list[dict[str, Any]] = []
    for (symbol, side, rank_bucket), rows in grouped.items():
        row_gap = sum(rec["policy_vs_hold_gap"] for rec in rows)
        negative_share = sum(max(0.0, -rec["policy_vs_hold_gap"]) for rec in rows) / total_loss
        worst = min(rows, key=lambda rec: rec["policy_vs_hold_gap"])
        out.append(
            {
                "symbol": symbol,
                "side": side,
                "rank_bucket": rank_bucket,
                "count": len(rows),
                "policy_vs_hold_gap": float(row_gap),
                "contribution_to_top20_gap": float(top20_delta * negative_share),
                "worst_anchor": worst["anchor_date"],
                "reason": worst["reason"],
                "trade_count": int(sum(rec["trade_flag"] for rec in rows)),
                "turnover": float(sum(rec["turnover_units"] for rec in rows)),
                "bad_pick_rate": float(sum(1 for rec in rows if rec["result_bucket"] != "win") / len(rows)),
            }
        )
    return sorted(out, key=lambda item: item["policy_vs_hold_gap"])


def run_diagnosis(
    *,
    input_dir: Path = INPUT_DIR,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
) -> dict[str, Any]:
    replay_summary = _load_json(input_dir / "integrated_guarded_v1_replay_summary.json")
    compare = _load_json(input_dir / "integrated_guarded_v1_compare.json")
    decision = _load_json(input_dir / "integrated_guarded_v1_decision.json")
    selection_ledger = _load_json(input_dir / "integrated_guarded_v1_selection_only_ledger.json")
    policy_ledger = _load_json(input_dir / "integrated_guarded_v1_policy_trade_ledger.json")
    candidate_snapshots = _load_json(input_dir / "integrated_guarded_v1_candidate_snapshots.json")
    coverage = _load_json(input_dir / "integrated_guarded_v1_full_universe_gate_coverage.json")
    exclusion = _load_json(input_dir / "integrated_guarded_v1_exclusion_diagnostics.json")

    selection_rows = list(selection_ledger["rows"])
    policy_rows = list(policy_ledger["rows"])
    coverage_rows = list(coverage["rows"])
    records = _position_records(selection_rows, policy_rows, coverage_rows)
    top20_delta = float(replay_summary["top20_policy_net_realized_pnl_delta"])

    bucket_rows = _aggregate_positions(records, "rank_bucket", top20_delta)
    side_rows = _aggregate_by_side(records, top20_delta)
    action_rows = _aggregate_daily_actions(policy_rows, top20_delta)
    anchor_rows = _aggregate_by_anchor(records, top20_delta)
    symbol_rows = _aggregate_by_symbol(records, top20_delta)

    bucket_lookup = {row["rank_bucket"]: row for row in bucket_rows}
    side_lookup = {row["side"]: row for row in side_rows}
    top10_delta = float(replay_summary["top10_policy_net_realized_pnl_delta"])
    top5_delta = float(replay_summary["top5_policy_net_realized_pnl_delta"])
    selection_top10_delta = float(replay_summary["comparison_topk"]["10"]["selection_only_avg_ret63"])
    selection_top20_delta = float(replay_summary["comparison_topk"]["20"]["selection_only_avg_ret63"])
    selection_top5_delta = float(replay_summary["comparison_topk"]["5"]["selection_only_avg_ret63"])

    daily_side_realized = {
        side: float(sum(float(row.get("realized_pnl") or 0.0) for row in policy_rows if str(row.get("side")) == side))
        for side in ("long", "short")
    }
    problem_records = [rec for rec in records if rec["rank_bucket"] in {"top6_10", "top11_20"}] or list(records)
    top_rank_bucket = "top11_20" if top20_delta < 0.0 else ("top6_10" if top10_delta < 0.0 else "top5")
    top_action = action_rows[0] if action_rows else {}
    top_symbol = symbol_rows[0] if symbol_rows else {}
    top_anchor = anchor_rows[0] if anchor_rows else {}

    late_exit_suppressed = bool(replay_summary.get("late_exit_loss_count_lower_buckets_challenger", 0) == 0)
    lower_bucket_drag = bool(top10_delta < 0.0 or top20_delta < 0.0)
    short_supply_next_axis = bool(
        float(replay_summary["summary_references"]["full_universe_short_tradable_rate_mean_specialized"]) < 0.10
        and abs(sum(float(row.get("realized_pnl") or 0.0) for row in policy_rows if str(row.get("side")) == "short" and str(row.get("rank_bucket")) != "top5"))
        > abs(sum(float(row.get("realized_pnl") or 0.0) for row in policy_rows if str(row.get("side")) == "long" and str(row.get("rank_bucket")) != "top5")) * 0.25
    )

    if selection_top5_delta > 0 and selection_top10_delta > 0 and selection_top20_delta > 0:
        if top10_delta < 0.0 or top20_delta < 0.0:
            diagnosis_decision = "selection_keep_policy_repair_needed"
        else:
            diagnosis_decision = "selection_keep_policy_hold"
    elif top10_delta < 0.0 or top20_delta < 0.0:
        diagnosis_decision = "selection_hold_policy_hold"
    else:
        diagnosis_decision = "inconclusive"

    primary_failure_reason = "lower_rank_long_hold_only_drag" if lower_bucket_drag else "policy_level_underperformance"
    secondary_failure_reasons = []
    if short_supply_next_axis:
        secondary_failure_reasons.append("short_side_supply_thin")
    if float(replay_summary["summary_references"]["full_universe_no_trade_rate_mean_specialized"]) > 0.80:
        secondary_failure_reasons.append("no_trade_rate_high")
    exclusion_aggregate = exclusion.get("rows", {}).get("aggregate", {})
    if float(exclusion_aggregate.get("skipped_symbols_without_basis_row_count") or 0) > 0:
        secondary_failure_reasons.append("basis_row_skip_present")

    summary = {
        "schema_version": "tradex_integrated_guarded_v1_policy_underperformance_diagnosis_v1",
        "generated_at": _utc_now(),
        "diagnosis_decision": diagnosis_decision,
        "primary_failure_reason": primary_failure_reason,
        "secondary_failure_reasons": secondary_failure_reasons,
        "top_contributing_rank_bucket": top_rank_bucket,
        "top_contributing_side": (
            "long"
            if sum(float(row.get("realized_pnl") or 0.0) for row in policy_rows if str(row.get("side")) == "long" and str(row.get("rank_bucket")) != "top5")
            <= sum(float(row.get("realized_pnl") or 0.0) for row in policy_rows if str(row.get("side")) == "short" and str(row.get("rank_bucket")) != "top5")
            else "short"
        ),
        "top_contributing_actions": [row["action"] for row in action_rows[:5]],
        "top_contributing_symbols": [
            {"symbol": row["symbol"], "side": row["side"], "rank_bucket": row["rank_bucket"], "policy_vs_hold_gap": row["policy_vs_hold_gap"]}
            for row in [row for row in symbol_rows if row["rank_bucket"] != "top5"][:10]
        ],
        "top_contributing_anchors": [
            {"anchor_date": row["anchor_date"], "policy_vs_hold_gap": row["policy_vs_hold_gap"]}
            for row in [row for row in anchor_rows if row["policy_vs_hold_gap"] <= 0][:10]
        ],
        "whether_late_exit_remains_suppressed": late_exit_suppressed,
        "whether_lower_bucket_drag_still_exists": lower_bucket_drag,
        "whether_short_side_supply_is_the_next_axis": short_supply_next_axis,
        "recommended_next_axis": "policy_rollout_reenable_lower_bucket_long_policy" if lower_bucket_drag else "short_side_supply_review",
        "selection_only_edge_preserved": bool(replay_summary.get("selection_only_edge_preserved", False)),
        "policy_layer_destroyed_edge": bool(replay_summary.get("policy_layer_destroyed_edge", False)),
        "baseline_policy_reference": replay_summary.get("baseline_policy_reference", {}),
        "integrated_policy_reference": replay_summary.get("selection_topk_repair_policy", {}),
        "topk_deltas": replay_summary.get("comparison_topk", {}),
        "top20_policy_delta": top20_delta,
        "top10_policy_delta": top10_delta,
        "top5_policy_delta": top5_delta,
        "input_artifacts": {
            "integrated_guarded_v1_replay_summary": str(input_dir / "integrated_guarded_v1_replay_summary.json"),
            "integrated_guarded_v1_compare": str(input_dir / "integrated_guarded_v1_compare.json"),
            "integrated_guarded_v1_decision": str(input_dir / "integrated_guarded_v1_decision.json"),
            "integrated_guarded_v1_selection_only_ledger": str(input_dir / "integrated_guarded_v1_selection_only_ledger.json"),
            "integrated_guarded_v1_policy_trade_ledger": str(input_dir / "integrated_guarded_v1_policy_trade_ledger.json"),
            "integrated_guarded_v1_candidate_snapshots": str(input_dir / "integrated_guarded_v1_candidate_snapshots.json"),
            "integrated_guarded_v1_full_universe_gate_coverage": str(input_dir / "integrated_guarded_v1_full_universe_gate_coverage.json"),
            "integrated_guarded_v1_exclusion_diagnostics": str(input_dir / "integrated_guarded_v1_exclusion_diagnostics.json"),
        },
    }

    outputs = {
        "integrated_guarded_v1_policy_underperformance_diagnosis.json": summary,
        "integrated_guarded_v1_policy_gap_by_rank_bucket.json": bucket_rows,
        "integrated_guarded_v1_policy_gap_by_side.json": side_rows,
        "integrated_guarded_v1_policy_gap_by_action.json": action_rows,
        "integrated_guarded_v1_policy_gap_by_anchor.json": anchor_rows,
        "integrated_guarded_v1_policy_gap_by_symbol.json": symbol_rows,
    }
    output_paths = {
        name: _write_json(output_dir / name, {"schema_version": name.replace(".json", "_v1"), "generated_at": _utc_now(), "rows": rows} if isinstance(rows, list) else rows)
        for name, rows in outputs.items()
    }
    # overwrite the summary file with the richer object structure expected by the report
    _write_json(output_dir / "integrated_guarded_v1_policy_underperformance_diagnosis.json", summary)
    return {
        "summary": summary,
        "output_dir": str(output_dir),
        "paths": {key: str(value) for key, value in output_paths.items()},
        "replay_summary": replay_summary,
        "compare": compare,
        "decision": decision,
        "coverage_rows_count": len(coverage_rows),
        "selection_rows_count": len(selection_rows),
        "policy_rows_count": len(policy_rows),
        "candidate_snapshot_rows_count": len(candidate_snapshots.get("rows") or []),
        "exclusion": exclusion,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Diagnose integrated guarded policy underperformance from authoritative JSON artifacts.")
    parser.add_argument("--input-dir", type=Path, default=INPUT_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args(argv)
    payload = run_diagnosis(input_dir=args.input_dir, output_dir=args.output_dir)
    print(json.dumps(payload["summary"], ensure_ascii=False, sort_keys=True, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
