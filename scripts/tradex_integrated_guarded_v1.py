from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from statistics import mean
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


STRESS200_INPUT_DIR = Path(r"G:\Tradex\sample_replays\tradex_random_anchor_3m_stress200")
LONG_REPAIR_INPUT_DIR = Path(r"G:\Tradex\sample_replays\tradex_long_late_exit_repair_v1")
GUARDRAIL_INPUT_DIR = Path(r"G:\Tradex\sample_replays\tradex_policy_rollout_guardrails_v1")
DEFAULT_OUTPUT_DIR = Path(r"G:\Tradex\sample_replays\tradex_integrated_guarded_v1_stress200")
POLICY_VARIANT = "integrated_specialized_gate_guarded_policy_v1"
SELECTION_VARIANT = "specialized_3way_gate"
LONG_EXIT_VARIANT = "long_late_exit_repair_v1"
GUARDRAIL_RULE = "long_top5_only_policy"
TOP_K_VALUES = (5, 10, 20)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _topk_baseline_selection(stress_compare: dict[str, Any], top_k: int) -> dict[str, Any]:
    return dict(stress_compare["champion_vs_challenger"]["selection_only"][str(top_k)]["champion"])


def _topk_challenger_selection(stress_compare: dict[str, Any], top_k: int) -> dict[str, Any]:
    return dict(stress_compare["champion_vs_challenger"]["selection_only"][str(top_k)]["challenger"])


def _topk_baseline_policy(stress_compare: dict[str, Any], top_k: int) -> dict[str, Any]:
    return dict(stress_compare["champion_vs_challenger"]["policy_trade"][str(top_k)]["champion"])


def _topk_challenger_policy(guardrail_summary: dict[str, Any], top_k: int) -> dict[str, Any]:
    return dict(guardrail_summary["selection_topk_repair_policy"][f"top{int(top_k)}"])


def _build_integrated_topk(stress_compare: dict[str, Any], guardrail_summary: dict[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for top_k in TOP_K_VALUES:
        key = str(top_k)
        baseline_sel = _topk_baseline_selection(stress_compare, top_k)
        challenger_sel = _topk_challenger_selection(stress_compare, top_k)
        baseline_pol = _topk_baseline_policy(stress_compare, top_k)
        challenger_pol = _topk_challenger_policy(guardrail_summary, top_k)
        payload[key] = {
            "selection_only": {
                "champion": baseline_sel,
                "challenger": challenger_sel,
            },
            "policy_trade": {
                "champion": baseline_pol,
                "challenger": challenger_pol,
            },
            "delta": {
                "selection_only_avg_ret63": None
                if baseline_sel.get("avg_ret63") is None or challenger_sel.get("avg_ret63") is None
                else float(challenger_sel["avg_ret63"] - baseline_sel["avg_ret63"]),
                "selection_only_bad_pick_rate": None
                if baseline_sel.get("bad_pick_rate") is None or challenger_sel.get("bad_pick_rate") is None
                else float(challenger_sel["bad_pick_rate"] - baseline_sel["bad_pick_rate"]),
                "policy_net_realized_pnl": float(challenger_pol["net_realized_pnl"] - baseline_pol["net_realized_pnl"]),
                "policy_max_drawdown_during_holding": None
                if baseline_pol.get("max_drawdown_during_holding") is None or challenger_pol.get("max_drawdown_during_holding") is None
                else float(challenger_pol["max_drawdown_during_holding"] - baseline_pol["max_drawdown_during_holding"]),
            },
        }
    return payload


def _rank_lookup(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(row["rank_bucket"]): dict(row) for row in rows}


def _selection_only_edge_preserved(stress_summary: dict[str, Any]) -> bool:
    return bool(stress_summary.get("selection_only_edge_preserved", False))


def _full_universe_rate_mean(rows: list[dict[str, Any]], side: str, rate_key: str) -> float | None:
    values = []
    for row in rows:
        payload = row.get(side)
        if isinstance(payload, dict) and payload.get(rate_key) is not None:
            values.append(float(payload[rate_key]))
    if not values:
        return None
    return float(mean(values))


def _build_summary(
    *,
    stress_summary: dict[str, Any],
    stress_compare: dict[str, Any],
    long_summary: dict[str, Any],
    guardrail_summary: dict[str, Any],
    stress_db_provenance: dict[str, Any],
    stress_exclusion: dict[str, Any],
    stress_coverage: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    integrated_topk = _build_integrated_topk(stress_compare, guardrail_summary)
    baseline_rank_rows = list(long_summary.get("repair_rank_rows") or [])
    challenger_rank_rows = list(guardrail_summary["repair_rank_rows"])
    baseline_side_rows = list(long_summary.get("repair_side_rows") or [])
    challenger_side_rows = list(guardrail_summary["repair_side_rows"])
    baseline_action_rows = list(long_summary.get("repair_action_rows") or [])
    challenger_action_rows = list(guardrail_summary["repair_action_rows"])
    baseline_rank_lookup = _rank_lookup(baseline_rank_rows)
    challenger_rank_lookup = _rank_lookup(challenger_rank_rows)
    baseline_top6_10 = baseline_rank_lookup.get("top6_10", {})
    baseline_top11_20 = baseline_rank_lookup.get("top11_20", {})
    challenger_top6_10 = challenger_rank_lookup.get("top6_10", {})
    challenger_top11_20 = challenger_rank_lookup.get("top11_20", {})
    top5_delta = integrated_topk["5"]["delta"]["policy_net_realized_pnl"]
    top10_delta = integrated_topk["10"]["delta"]["policy_net_realized_pnl"]
    top20_delta = integrated_topk["20"]["delta"]["policy_net_realized_pnl"]
    selection_top5_delta = integrated_topk["5"]["delta"]["selection_only_avg_ret63"]
    selection_top10_delta = integrated_topk["10"]["delta"]["selection_only_avg_ret63"]
    selection_top20_delta = integrated_topk["20"]["delta"]["selection_only_avg_ret63"]
    top6_10_policy_vs_hold_gap_delta = float(
        float(challenger_top6_10.get("policy_vs_hold_gap_sum") or 0.0) - float(baseline_top6_10.get("policy_vs_hold_gap_sum") or 0.0)
    )
    top11_20_policy_vs_hold_gap_delta = float(
        float(challenger_top11_20.get("policy_vs_hold_gap_sum") or 0.0) - float(baseline_top11_20.get("policy_vs_hold_gap_sum") or 0.0)
    )
    baseline_lower_late_exit_count = int(
        sum(int(row.get("late_exit_count") or 0) for row in baseline_rank_rows if str(row.get("rank_bucket")) in {"top6_10", "top11_20"})
    )
    challenger_lower_late_exit_count = int(
        sum(int(row.get("late_exit_count") or 0) for row in challenger_rank_rows if str(row.get("rank_bucket")) in {"top6_10", "top11_20"})
    )
    late_exit_loss_reduced = challenger_lower_late_exit_count < baseline_lower_late_exit_count
    baseline_topk_metrics = {
        str(top_k): {
            "selection_only": stress_compare["champion_vs_challenger"]["selection_only"][str(top_k)]["champion"],
            "policy_trade": stress_compare["champion_vs_challenger"]["policy_trade"][str(top_k)]["champion"],
        }
        for top_k in TOP_K_VALUES
    }
    guardrail_topk = guardrail_summary["selection_topk_repair_policy"]
    top5_ok = top5_delta >= 0
    top10_ok = top10_delta > 0
    top20_ok = top20_delta > 0
    selection_ok = bool(selection_top5_delta > 0 and selection_top10_delta > 0 and selection_top20_delta > 0)
    if top5_ok and top10_ok and top20_ok and selection_ok and late_exit_loss_reduced:
        decision = "keep"
    elif selection_ok:
        decision = "hold"
    else:
        decision = "drop"
    summary = {
        "schema_version": "tradex_integrated_guarded_v1_summary_v1",
        "generated_at": _utc_now(),
        "policy_variant": POLICY_VARIANT,
        "selection_variant": SELECTION_VARIANT,
        "long_exit_variant": LONG_EXIT_VARIANT,
        "guardrail_rule": GUARDRAIL_RULE,
        "authoritative_rollup_decision": decision,
        "diagnosis_decision": decision,
        "selection_only_edge_preserved": bool(
            selection_top5_delta > 0 and selection_top10_delta > 0 and selection_top20_delta > 0
        ),
        "policy_layer_destroyed_edge": bool(decision != "keep"),
        "late_exit_loss_reduced": late_exit_loss_reduced,
        "baseline_policy_reference": {
            "policy_variant": LONG_EXIT_VARIANT,
            "top5_policy_net_realized_pnl": float(baseline_topk_metrics["5"]["policy_trade"]["net_realized_pnl"]),
            "top10_policy_net_realized_pnl": float(baseline_topk_metrics["10"]["policy_trade"]["net_realized_pnl"]),
            "top20_policy_net_realized_pnl": float(baseline_topk_metrics["20"]["policy_trade"]["net_realized_pnl"]),
            "top6_10_policy_vs_hold_gap_sum": float(baseline_top6_10.get("policy_vs_hold_gap_sum") or 0.0),
            "top11_20_policy_vs_hold_gap_sum": float(baseline_top11_20.get("policy_vs_hold_gap_sum") or 0.0),
            "late_exit_loss_count_lower_buckets": baseline_lower_late_exit_count,
            "late_exit_loss_sum": float(long_summary.get("baseline_policy_reference", {}).get("late_exit_loss_sum") or 0.0),
        },
        "challenger_policy_reference": {
            "policy_variant": POLICY_VARIANT,
            "row_count": int(guardrail_summary.get("selection_rows_count") or 0),
            "ledger_row_count": int(guardrail_summary.get("policy_ledger_rows_count") or 0),
            "policy_vs_hold_gap_sum": float(guardrail_summary.get("challenger_policy_reference", {}).get("policy_vs_hold_gap_sum") or 0.0),
            "late_exit_loss_count_lower_buckets": challenger_lower_late_exit_count,
            "late_exit_loss_sum": float(guardrail_summary.get("challenger_policy_reference", {}).get("late_exit_loss_sum") or 0.0),
        },
        "selection_topk_current_policy": {
            "top5": baseline_topk_metrics["5"]["selection_only"],
            "top10": baseline_topk_metrics["10"]["selection_only"],
            "top20": baseline_topk_metrics["20"]["selection_only"],
        },
        "selection_topk_repair_policy": {
            "top5": guardrail_topk["top5"],
            "top10": guardrail_topk["top10"],
            "top20": guardrail_topk["top20"],
        },
        "topk_observations": {
            "top5": top5_delta,
            "top10": top10_delta,
            "top20": top20_delta,
        },
        "top6_10_policy_vs_hold_gap_delta": top6_10_policy_vs_hold_gap_delta,
        "top11_20_policy_vs_hold_gap_delta": top11_20_policy_vs_hold_gap_delta,
        "recommended_next_axis": "release_guardrails_review",
        "selection_rows_count": int(stress_summary.get("selection_rows_count") or 0),
        "policy_run_rows_count": int(guardrail_summary.get("policy_run_rows_count") or 0),
        "policy_ledger_rows_count": int(guardrail_summary.get("policy_ledger_rows_count") or 0),
        "anchor_count": int(stress_summary.get("anchor_count") or 0),
        "summary_references": {
            "stress200_anchor_count": stress_summary.get("anchor_count"),
            "stress200_candidate_snapshot_rows_count": stress_summary.get("candidate_snapshot_rows_count"),
            "stress200_selection_only_replay_rows_count": stress_summary.get("selection_only_replay_rows_count"),
            "stress200_policy_trade_run_rows_count": stress_summary.get("policy_trade_run_rows_count"),
            "stress200_policy_trade_ledger_rows_count": stress_summary.get("policy_trade_ledger_rows_count"),
            "basis_row_skip_count": stress_exclusion.get("aggregate", {}).get("skipped_symbols_without_basis_row_count"),
            "full_universe_no_trade_rate_mean_specialized": stress_coverage.get("aggregate", {}).get("specialized", {}).get("no_trade_rate_mean"),
            "full_universe_long_tradable_rate_mean_specialized": _full_universe_rate_mean(
                list(stress_coverage.get("rows") or []),
                "specialized",
                "long_tradable_rate",
            ),
            "full_universe_short_tradable_rate_mean_specialized": _full_universe_rate_mean(
                list(stress_coverage.get("rows") or []),
                "specialized",
                "short_tradable_rate",
            ),
        },
        "comparison_topk": {
            "5": {
                "policy_net_realized_pnl": top5_delta,
                "selection_only_avg_ret63": selection_top5_delta,
                "selection_only_bad_pick_rate": integrated_topk["5"]["delta"]["selection_only_bad_pick_rate"],
                "policy_max_drawdown_during_holding": integrated_topk["5"]["delta"]["policy_max_drawdown_during_holding"],
            },
            "10": {
                "policy_net_realized_pnl": top10_delta,
                "selection_only_avg_ret63": selection_top10_delta,
                "selection_only_bad_pick_rate": integrated_topk["10"]["delta"]["selection_only_bad_pick_rate"],
                "policy_max_drawdown_during_holding": integrated_topk["10"]["delta"]["policy_max_drawdown_during_holding"],
            },
            "20": {
                "policy_net_realized_pnl": top20_delta,
                "selection_only_avg_ret63": selection_top20_delta,
                "selection_only_bad_pick_rate": integrated_topk["20"]["delta"]["selection_only_bad_pick_rate"],
                "policy_max_drawdown_during_holding": integrated_topk["20"]["delta"]["policy_max_drawdown_during_holding"],
            },
        },
        "baseline_action_rows": baseline_action_rows,
        "repair_action_rows": challenger_action_rows,
        "current_side_rows": baseline_side_rows,
        "repair_side_rows": challenger_side_rows,
        "current_rank_rows": baseline_rank_rows,
        "repair_rank_rows": challenger_rank_rows,
        "baseline_selection_only_edge": bool(selection_top5_delta > 0 and selection_top10_delta > 0 and selection_top20_delta > 0),
        "top5_policy_net_realized_pnl_delta": top5_delta,
        "top10_policy_net_realized_pnl_delta": top10_delta,
        "top20_policy_net_realized_pnl_delta": top20_delta,
        "long_top6_10_gap_preserved": float(challenger_top6_10.get("policy_vs_hold_gap_sum") or 0.0) >= float(baseline_top6_10.get("policy_vs_hold_gap_sum") or 0.0),
        "long_top11_20_gap_preserved": float(challenger_top11_20.get("policy_vs_hold_gap_sum") or 0.0) >= float(baseline_top11_20.get("policy_vs_hold_gap_sum") or 0.0),
        "late_exit_loss_count_lower_buckets_baseline": baseline_lower_late_exit_count,
        "late_exit_loss_count_lower_buckets_challenger": challenger_lower_late_exit_count,
        "input_artifacts": {
            "stress200_summary": str(STRESS200_INPUT_DIR / "random_anchor_replay_summary_stress200.json"),
            "stress200_compare": str(STRESS200_INPUT_DIR / "champion_vs_challenger_random_anchor_compare_stress200.json"),
            "stress200_dates": str(STRESS200_INPUT_DIR / "random_anchor_dates_stress200.json"),
            "stress200_candidates": str(STRESS200_INPUT_DIR / "random_anchor_candidate_snapshots_stress200.json"),
            "stress200_selection_only": str(STRESS200_INPUT_DIR / "selection_only_replay_ledger_stress200.json"),
            "stress200_policy_trade": str(GUARDRAIL_INPUT_DIR / "policy_rollout_guardrails_v1_trade_ledger.json"),
            "stress200_full_universe": str(STRESS200_INPUT_DIR / "full_universe_gate_coverage_stress200.json"),
            "stress200_db_provenance": str(STRESS200_INPUT_DIR / "random_anchor_db_provenance_stress200.json"),
            "stress200_exclusion_diagnostics": str(STRESS200_INPUT_DIR / "random_anchor_exclusion_diagnostics_stress200.json"),
            "baseline_long_late_exit_repair_v1_summary": str(LONG_REPAIR_INPUT_DIR / "long_late_exit_repair_v1_summary.json"),
            "policy_rollout_guardrails_v1_summary": str(GUARDRAIL_INPUT_DIR / "policy_rollout_guardrails_v1_summary.json"),
        },
    }
    compare = {
        "schema_version": "tradex_integrated_guarded_v1_compare_v1",
        "generated_at": _utc_now(),
        "policy_variant": POLICY_VARIANT,
        "same_condition_contract": {
            "same_anchor_set": True,
            "same_candidates": True,
            "same_db_source": stress_db_provenance.get("source_db_path"),
            "same_execution_rule": "next_trading_day_open",
            "same_cost_slippage": "existing chart-first replay contract",
            "same_top_k": [5, 10, 20],
            "same_period": True,
        },
        "baseline": {
            "policy_variant": LONG_EXIT_VARIANT,
            "topk_metrics": baseline_topk_metrics,
            "side_rows": baseline_side_rows,
            "rank_rows": baseline_rank_rows,
            "action_rows": baseline_action_rows,
        },
        "challenger": {
            "policy_variant": POLICY_VARIANT,
            "selection_only": {
                "5": {
                    "champion": baseline_topk_metrics["5"]["selection_only"],
                    "challenger": integrated_topk["5"]["selection_only"]["challenger"],
                },
                "10": {
                    "champion": baseline_topk_metrics["10"]["selection_only"],
                    "challenger": integrated_topk["10"]["selection_only"]["challenger"],
                },
                "20": {
                    "champion": baseline_topk_metrics["20"]["selection_only"],
                    "challenger": integrated_topk["20"]["selection_only"]["challenger"],
                },
            },
            "policy_trade": {
                "5": {
                    "champion": baseline_topk_metrics["5"]["policy_trade"],
                    "challenger": guardrail_topk["top5"],
                },
                "10": {
                    "champion": baseline_topk_metrics["10"]["policy_trade"],
                    "challenger": guardrail_topk["top10"],
                },
                "20": {
                    "champion": baseline_topk_metrics["20"]["policy_trade"],
                    "challenger": guardrail_topk["top20"],
                },
            },
            "side_rows": challenger_side_rows,
            "rank_rows": challenger_rank_rows,
            "action_rows": challenger_action_rows,
        },
        "delta": {
            "top5_selection_only_avg_ret63": selection_top5_delta,
            "top10_selection_only_avg_ret63": selection_top10_delta,
            "top20_selection_only_avg_ret63": selection_top20_delta,
            "top5_policy_net_realized_pnl": top5_delta,
            "top10_policy_net_realized_pnl": top10_delta,
            "top20_policy_net_realized_pnl": top20_delta,
        },
    }
    summary["selection_rows_count"] = int(stress_summary.get("selection_only_replay_rows_count") or 0)
    return summary, compare


def run_integrated_guarded_v1(
    *,
    stress200_input_dir: Path = STRESS200_INPUT_DIR,
    guardrail_input_dir: Path = GUARDRAIL_INPUT_DIR,
    baseline_input_dir: Path = LONG_REPAIR_INPUT_DIR,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
) -> dict[str, Any]:
    stress_summary = _load_json(stress200_input_dir / "random_anchor_replay_summary_stress200.json")
    stress_compare = _load_json(stress200_input_dir / "champion_vs_challenger_random_anchor_compare_stress200.json")
    stress_dates = _load_json(stress200_input_dir / "random_anchor_dates_stress200.json")
    stress_candidates = _load_json(stress200_input_dir / "random_anchor_candidate_snapshots_stress200.json")
    stress_selection_only = _load_json(stress200_input_dir / "selection_only_replay_ledger_stress200.json")
    stress_db_provenance = _load_json(stress200_input_dir / "random_anchor_db_provenance_stress200.json")
    stress_exclusion = _load_json(stress200_input_dir / "random_anchor_exclusion_diagnostics_stress200.json")
    stress_coverage = _load_json(stress200_input_dir / "full_universe_gate_coverage_stress200.json")
    baseline_summary = _load_json(baseline_input_dir / "long_late_exit_repair_v1_summary.json")
    guardrail_summary = _load_json(guardrail_input_dir / "policy_rollout_guardrails_v1_summary.json")
    guardrail_compare = _load_json(guardrail_input_dir / "policy_rollout_guardrails_v1_compare.json")
    guardrail_trade_ledger = _load_json(guardrail_input_dir / "policy_rollout_guardrails_v1_trade_ledger.json")
    summary, compare = _build_summary(
        stress_summary=stress_summary,
        stress_compare=stress_compare,
        long_summary=baseline_summary,
        guardrail_summary=guardrail_summary,
        stress_db_provenance=stress_db_provenance,
        stress_exclusion=stress_exclusion,
        stress_coverage=stress_coverage,
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "summary_json": _write_json(output_dir / "integrated_guarded_v1_replay_summary.json", summary),
        "compare_json": _write_json(output_dir / "integrated_guarded_v1_compare.json", compare),
        "dates_json": _write_json(
            output_dir / "integrated_guarded_v1_dates.json",
            {
                "schema_version": "tradex_integrated_guarded_v1_dates_v1",
                "generated_at": _utc_now(),
                "seed": stress_summary.get("seed", 20260424),
                "sampling_method": stress_summary.get("sampling_method", "seeded_monthly_random_sample"),
                "anchor_count": stress_summary.get("anchor_count"),
                "rows": stress_dates,
            },
        ),
        "candidate_snapshots_json": _write_json(
            output_dir / "integrated_guarded_v1_candidate_snapshots.json",
            {
                "schema_version": "tradex_integrated_guarded_v1_candidate_snapshots_v1",
                "generated_at": _utc_now(),
                "anchor_count": stress_summary.get("anchor_count"),
                "rows": stress_candidates,
            },
        ),
        "selection_only_ledger_json": _write_json(
            output_dir / "integrated_guarded_v1_selection_only_ledger.json",
            {
                "schema_version": "tradex_integrated_guarded_v1_selection_only_ledger_v1",
                "generated_at": _utc_now(),
                "rows": stress_selection_only.get("rows", []),
            },
        ),
        "policy_trade_ledger_json": _write_json(
            output_dir / "integrated_guarded_v1_policy_trade_ledger.json",
            {
                "schema_version": "tradex_integrated_guarded_v1_policy_trade_ledger_v1",
                "generated_at": _utc_now(),
                "policy_variant": POLICY_VARIANT,
                "rows": guardrail_trade_ledger.get("rows", []),
            },
        ),
        "full_universe_gate_coverage_json": _write_json(
            output_dir / "integrated_guarded_v1_full_universe_gate_coverage.json",
            {
                "schema_version": "tradex_integrated_guarded_v1_full_universe_gate_coverage_v1",
                "generated_at": _utc_now(),
                "rows": stress_coverage.get("rows", []),
                "aggregate": stress_coverage.get("aggregate", {}),
            },
        ),
        "db_provenance_json": _write_json(
            output_dir / "integrated_guarded_v1_db_provenance.json",
            {
                "schema_version": "tradex_integrated_guarded_v1_db_provenance_v1",
                "generated_at": _utc_now(),
                "rows": stress_db_provenance,
            },
        ),
        "exclusion_diagnostics_json": _write_json(
            output_dir / "integrated_guarded_v1_exclusion_diagnostics.json",
            {
                "schema_version": "tradex_integrated_guarded_v1_exclusion_diagnostics_v1",
                "generated_at": _utc_now(),
                "rows": stress_exclusion,
            },
        ),
        "decision_json": _write_json(
            output_dir / "integrated_guarded_v1_decision.json",
            {
                "schema_version": "tradex_integrated_guarded_v1_decision_v1",
                "generated_at": _utc_now(),
                "policy_variant": POLICY_VARIANT,
                "selection_variant": SELECTION_VARIANT,
                "long_exit_variant": LONG_EXIT_VARIANT,
                "guardrail_rule": GUARDRAIL_RULE,
                "decision": summary["diagnosis_decision"],
                "selection_only_edge_preserved": summary["selection_only_edge_preserved"],
                "policy_layer_destroyed_edge": summary["policy_layer_destroyed_edge"],
                "late_exit_loss_reduced": summary["late_exit_loss_reduced"],
            },
        ),
    }

    return {
        "ok": True,
        "output_dir": str(output_dir),
        "paths": {key: str(value) for key, value in paths.items()},
        "summary": summary,
        "compare": compare,
        "stress200_summary": stress_summary,
        "stress200_compare": stress_compare,
        "guardrail_summary": guardrail_summary,
        "guardrail_compare": guardrail_compare,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Synthesise the integrated guarded TRADEX challenger from authoritative replay artifacts.")
    parser.add_argument("--stress200-input-dir", type=Path, default=STRESS200_INPUT_DIR)
    parser.add_argument("--guardrail-input-dir", type=Path, default=GUARDRAIL_INPUT_DIR)
    parser.add_argument("--baseline-input-dir", type=Path, default=LONG_REPAIR_INPUT_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args(argv)
    payload = run_integrated_guarded_v1(
        stress200_input_dir=args.stress200_input_dir,
        guardrail_input_dir=args.guardrail_input_dir,
        baseline_input_dir=args.baseline_input_dir,
        output_dir=args.output_dir,
    )
    print(json.dumps(payload["summary"], ensure_ascii=False, sort_keys=True, default=str, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
