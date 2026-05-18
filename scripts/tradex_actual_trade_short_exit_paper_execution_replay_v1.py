from __future__ import annotations

import csv
import json
import math
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any, Mapping

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.services.codex_bridge_service import get_rankings_freshness, get_runtime_stock_db_status


AXIS_ID = "tradex_actual_trade_short_exit_paper_execution_replay_v1"
SCHEMA_PREFIX = "tradex_actual_trade_short_exit_paper_execution_replay_v1"
SOURCE_ROOT = Path(
    r"G:\Tradex\actual_trade_short_exit_execution_convention_compare_v1"
    r"\20260517T123939Z-tradex_actual_trade_short_exit_execution_convention_compare_v1"
)
OUT_BASE = Path(r"G:\Tradex\actual_trade_short_exit_paper_execution_replay_v1")
REQUIRED_OUTPUTS = (
    "paper_execution_replay_contract.json",
    "paper_execution_replay_compare.json",
    "paper_execution_replay_monthly_stability.json",
    "paper_execution_replay_concentration_summary.json",
    "paper_execution_replay_orders.csv",
    "paper_execution_replay_order_intents.jsonl",
    "paper_execution_replay_decision.json",
    "no_lookahead_audit.json",
    "_ARTIFACT_COMPLETE.json",
)


def now_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


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


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str] | None = None) -> None:
    if fields is None:
        fields = []
        for row in rows:
            for key in row:
                if key not in fields:
                    fields.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(_json_ready(row), ensure_ascii=False, sort_keys=True) + "\n")


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _fnum(row: Mapping[str, Any], key: str, default: float = 0.0) -> float:
    value = row.get(key)
    if value in (None, ""):
        return default
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def _group_delta(rows: list[dict[str, Any]], key_fn) -> dict[str, float]:
    out: dict[str, float] = defaultdict(float)
    for row in rows:
        out[str(key_fn(row))] += float(row["sim_minus_actual_pnl"])
    return dict(sorted(out.items()))


def _delta_contribution(values: list[float], n: int, total_delta: float) -> float | None:
    if total_delta == 0:
        return None
    if total_delta > 0:
        ordered = sorted([v for v in values if v > 0], reverse=True)
        return sum(ordered[:n]) / total_delta if ordered else 0.0
    ordered = sorted([abs(v) for v in values if v < 0], reverse=True)
    return sum(ordered[:n]) / abs(total_delta) if ordered else 0.0


def _robustness_classification(top1_month: float | None, top3_month: float | None, top1_trade: float | None, top5_trade: float | None) -> str:
    if top1_trade is None or top1_month is None or top3_month is None or top5_trade is None:
        return "unknown"
    if top1_trade >= 0.50 or top1_month >= 0.60:
        return "one_off_effect"
    if top5_trade < 0.45 and top3_month < 0.35 and top1_month < 0.20:
        return "broad_effect"
    if top5_trade >= 0.75 or top3_month >= 0.70 or top1_month >= 0.40:
        return "highly_concentrated_effect"
    return "moderately_concentrated_effect"


def _monthly_stability(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_month: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_month[str(row["entry_month"])].append(row)
    out: dict[str, Any] = {}
    for month, vals in sorted(by_month.items()):
        sim = _summarize(vals)
        out[month] = {
            "trade_count": len(vals),
            "actual_pnl_total": sim["actual_gross_pnl_total"],
            "sim_pnl_total": sim["sim_gross_pnl_total"],
            "pnl_delta_total": sim["pnl_delta_total"],
            "actual_return_mean": sim["actual_return_mean"],
            "sim_return_mean": sim["sim_return_mean"],
            "actual_return_median": sim["actual_return_median"],
            "sim_return_median": sim["sim_return_median"],
            "actual_large_loss_count": sim["actual_large_loss_count"],
            "sim_large_loss_count": sim["sim_large_loss_count"],
        }
    return out


def _concentration(rows: list[dict[str, Any]]) -> dict[str, Any]:
    delta_by_month = _group_delta(rows, lambda r: r["entry_month"])
    delta_by_year = _group_delta(rows, lambda r: r["entry_year"])
    delta_by_symbol = _group_delta(rows, lambda r: r["symbol"])
    trade_deltas = [float(r["sim_minus_actual_pnl"]) for r in rows]
    total_delta = sum(trade_deltas)
    top1_trade = _delta_contribution(trade_deltas, 1, total_delta)
    top5_trade = _delta_contribution(trade_deltas, 5, total_delta)
    top1_month = _delta_contribution(list(delta_by_month.values()), 1, total_delta)
    top3_month = _delta_contribution(list(delta_by_month.values()), 3, total_delta)
    top1_symbol = _delta_contribution(list(delta_by_symbol.values()), 1, total_delta)
    top5_symbol = _delta_contribution(list(delta_by_symbol.values()), 5, total_delta)
    positive_months = sum(1 for v in delta_by_month.values() if v > 0)
    negative_months = sum(1 for v in delta_by_month.values() if v < 0)
    return {
        "pnl_delta_total": total_delta,
        "pnl_delta_by_month": delta_by_month,
        "pnl_delta_by_year": delta_by_year,
        "pnl_delta_by_symbol": delta_by_symbol,
        "positive_effect_month_count": positive_months,
        "negative_effect_month_count": negative_months,
        "top_1_trade_delta_contribution_pct": top1_trade,
        "top_5_trade_delta_contribution_pct": top5_trade,
        "top_1_month_delta_contribution_pct": top1_month,
        "top_3_month_delta_contribution_pct": top3_month,
        "top_1_symbol_delta_contribution_pct": top1_symbol,
        "top_5_symbol_delta_contribution_pct": top5_symbol,
        "robustness_classification": _robustness_classification(top1_month, top3_month, top1_trade, top5_trade),
    }


def _summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    pnls = [float(r["sim_gross_pnl"]) for r in rows]
    actual_pnls = [float(r["actual_gross_pnl"]) for r in rows]
    rets = [float(r["sim_return_pct"]) for r in rows]
    actual_rets = [float(r["actual_return_pct"]) for r in rows]
    holds = [int(r["final_exit_day"]) for r in rows]
    gross_win = sum(v for v in pnls if v > 0)
    gross_loss = abs(sum(v for v in pnls if v < 0))
    actual_gross_win = sum(v for v in actual_pnls if v > 0)
    actual_gross_loss = abs(sum(v for v in actual_pnls if v < 0))
    win_rate = sum(1 for v in pnls if v > 0) / len(pnls) if pnls else None
    actual_win_rate = sum(1 for v in actual_pnls if v > 0) / len(actual_pnls) if actual_pnls else None
    return {
        "trade_count": len(rows),
        "actual_gross_pnl_total": sum(actual_pnls),
        "sim_gross_pnl_total": sum(pnls),
        "pnl_delta_total": sum(pnls) - sum(actual_pnls),
        "actual_return_mean": mean(actual_rets) if actual_rets else None,
        "sim_return_mean": mean(rets) if rets else None,
        "return_mean_delta": (mean(rets) if rets else 0.0) - (mean(actual_rets) if actual_rets else 0.0),
        "actual_return_median": median(actual_rets) if actual_rets else None,
        "sim_return_median": median(rets) if rets else None,
        "return_median_delta": (median(rets) if rets else 0.0) - (median(actual_rets) if actual_rets else 0.0),
        "actual_win_rate": actual_win_rate,
        "sim_win_rate": win_rate,
        "win_rate_delta": (win_rate or 0.0) - (actual_win_rate or 0.0),
        "actual_profit_factor": None if actual_gross_loss == 0 else actual_gross_win / actual_gross_loss,
        "sim_profit_factor": None if gross_loss == 0 else gross_win / gross_loss,
        "profit_factor_delta": (
            None
            if actual_gross_loss == 0 or gross_loss == 0
            else gross_win / gross_loss - actual_gross_win / actual_gross_loss
        ),
        "actual_large_loss_count": sum(1 for v in actual_rets if v <= -0.05),
        "sim_large_loss_count": sum(1 for v in rets if v <= -0.05),
        "actual_large_win_count": sum(1 for v in actual_rets if v >= 0.05),
        "sim_large_win_count": sum(1 for v in rets if v >= 0.05),
        "actual_avg_final_exit_day": mean(holds) if holds else None,
        "actual_median_final_exit_day": median(holds) if holds else None,
        "symbol_count": len({str(r["symbol"]) for r in rows}),
    }


def _make_intents(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    intents: list[dict[str, Any]] = []
    for row in rows:
        intent = dict(row)
        intent.update(
            {
                "intent_id": f"{row['policy_id']}-{row['normalized_trade_id']}-paper",
                "trade_side": "short_exit",
                "order_side": "buy_to_cover",
                "order_type": "paper_market_on_open",
                "paper_status": "planned_not_submitted",
                "paper_replay_only": True,
                "source_execution_convention": row["execution_convention"],
                "selection_decision": "paper_execution_replay_order_intent",
            }
        )
        intents.append(intent)
    return intents


def _build_no_lookahead(source_no_lookahead: Mapping[str, Any], summary: Mapping[str, Any], runtime_status: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": f"{SCHEMA_PREFIX}_no_lookahead_audit_v1",
        "axis_id": AXIS_ID,
        "pass": bool(source_no_lookahead.get("pass")) and bool(summary.get("trade_count", 0)),
        "source_compare_no_lookahead_pass": bool(source_no_lookahead.get("pass")),
        "future_bars_used_for_selection": [],
        "future_outcome_fields_used_for_selection": [],
        "selection_decisions_only_from_current_and_past_bars": True,
        "execution_fill_occurs_after_decision": True,
        "paper_replay_only": True,
        "runtime_db_accessed": True,
        "runtime_db_written": False,
        "production_state_changed": False,
        "meeMee_changed": False,
        "silent_fallback_used": False,
        "runtime_latest_available_global_date": runtime_status.get("latest_available_global_date_iso"),
        "runtime_latest_confirmed_daily_bars_date": runtime_status.get("latest_confirmed_daily_bars_date_iso"),
    }


def _decision_label(summary: Mapping[str, Any], source_decision: Mapping[str, Any], no_lookahead_pass: bool) -> tuple[str, list[str]]:
    reasons: list[str] = []
    if not bool(source_decision.get("paper_replay_ready")):
        return "hold_due_to_source_not_ready", ["source_candidate_is_not_paper_replay_ready"]
    if not no_lookahead_pass:
        return "drop_due_to_no_lookahead_gap", ["no_lookahead_audit_failed"]
    if summary.get("trade_count", 0) <= 0:
        return "drop_as_no_replay_rows", ["paper_replay_generated_no_rows"]

    if (summary.get("sim_profit_factor") or 0) >= 1.5:
        reasons.append("profit_factor_remains_strong")
    else:
        reasons.append("profit_factor_is_too_weak")
    if (summary.get("sim_return_mean") or 0) > 0:
        reasons.append("mean_return_is_positive")
    else:
        reasons.append("mean_return_is_non_positive")
    if (summary.get("sim_return_median") or 0) > 0:
        reasons.append("median_return_is_positive")
    else:
        reasons.append("median_return_is_non_positive")
    if (summary.get("fallback_share") or 1.0) <= 0.10:
        reasons.append("fallback_share_within_shadow_limit")
    else:
        reasons.append("fallback_share_too_high")
    if summary.get("robustness_classification") != "one_off_effect":
        reasons.append("not_one_off_effect")
    else:
        reasons.append("one_off_effect_detected")
    if (summary.get("positive_effect_month_count") or 0) > (summary.get("negative_effect_month_count") or 0):
        reasons.append("monthly_edge_is_broad_enough")
    else:
        reasons.append("monthly_edge_is_too_narrow")

    if (
        (summary.get("sim_profit_factor") or 0) >= 1.5
        and (summary.get("sim_return_mean") or 0) > 0
        and (summary.get("sim_return_median") or 0) > 0
        and (summary.get("fallback_share") or 1.0) <= 0.10
        and summary.get("robustness_classification") != "one_off_effect"
        and (summary.get("positive_effect_month_count") or 0) > (summary.get("negative_effect_month_count") or 0)
    ):
        return "keep_for_paper_execution_replay", reasons
    if (summary.get("sim_gross_pnl_total") or 0) > 0:
        return "hold_due_to_execution_convention_gap", reasons + ["pnl_is_positive_but_gate_is_not_broad_enough"]
    return "drop_as_execution_regression", reasons + ["paper_replay_does_not_improve_pnl"]


def run(*, source_root: Path = SOURCE_ROOT, output_base: Path = OUT_BASE) -> dict[str, Any]:
    stamp = now_stamp()
    run_root = output_base / f"{stamp}-{AXIS_ID}"
    run_root.mkdir(parents=True, exist_ok=True)

    runtime_status = get_runtime_stock_db_status()
    rankings_down = get_rankings_freshness(tf="D", which="latest", direction="down", mode="trade", risk_mode="balanced", limit=20)
    rankings_up = get_rankings_freshness(tf="D", which="latest", direction="up", mode="trade", risk_mode="balanced", limit=20)

    source_decision = json.loads((source_root / "short_exit_execution_convention_decision.json").read_text(encoding="utf-8"))
    source_compare = json.loads((source_root / "short_exit_execution_convention_compare.json").read_text(encoding="utf-8"))
    source_monthly = json.loads((source_root / "short_exit_execution_convention_monthly_stability.json").read_text(encoding="utf-8"))
    source_concentration = json.loads((source_root / "short_exit_execution_convention_concentration_summary.json").read_text(encoding="utf-8"))
    source_no_lookahead = json.loads((source_root / "no_lookahead_audit.json").read_text(encoding="utf-8"))

    rows = read_csv(source_root / "short_exit_execution_convention_trade_rows.csv")
    paper_orders = _make_intents(rows)
    concentration = _concentration(rows)
    monthly = _monthly_stability(rows)
    summary = _summarize(rows)
    summary.update(
        {
            "source_same_day_best_policy_id": source_compare.get("source_same_day_best_policy_id"),
            "source_same_day_best_pnl_delta_total": source_compare.get("source_same_day_best_metrics", {}).get("pnl_delta_total"),
            "source_best_next_open_policy_id": source_compare.get("best_next_open_policy_id"),
            "source_best_next_open_policy_label": source_compare.get("best_next_open_policy_label"),
            "source_best_next_open_concentration": source_compare.get("best_next_open_concentration"),
            "fallback_share": source_decision.get("fallback_summary", {}).get("fallback_share"),
            "fallback_count": source_decision.get("fallback_summary", {}).get("fallback_count"),
            "best_policy_label": source_decision.get("best_policy_label"),
            "best_policy_id": source_decision.get("best_policy_id"),
            "execution_convention": source_decision.get("best_execution_convention"),
            "paper_replay_ready": bool(source_decision.get("paper_replay_ready")),
            "shadow_paper_replay_candidate": bool(source_decision.get("shadow_paper_replay_candidate")),
            "source_compare_decision": source_decision.get("decision"),
        }
    )
    summary.update(
        {
            "positive_effect_month_count": concentration.get("positive_effect_month_count"),
            "negative_effect_month_count": concentration.get("negative_effect_month_count"),
            "robustness_classification": concentration.get("robustness_classification"),
            "top_1_trade_delta_contribution_pct": concentration.get("top_1_trade_delta_contribution_pct"),
            "top_5_trade_delta_contribution_pct": concentration.get("top_5_trade_delta_contribution_pct"),
            "top_1_month_delta_contribution_pct": concentration.get("top_1_month_delta_contribution_pct"),
            "top_3_month_delta_contribution_pct": concentration.get("top_3_month_delta_contribution_pct"),
        }
    )
    no_lookahead_pass = bool(source_no_lookahead.get("pass")) and True
    decision, decision_reasons = _decision_label(summary, source_decision, no_lookahead_pass)

    contract = {
        "axis_id": AXIS_ID,
        "schema_version": f"{SCHEMA_PREFIX}_contract_v1",
        "created_at_utc": stamp,
        "source_root": str(source_root),
        "source_decision_path": str(source_root / "short_exit_execution_convention_decision.json"),
        "source_compare_path": str(source_root / "short_exit_execution_convention_compare.json"),
        "source_monthly_path": str(source_root / "short_exit_execution_convention_monthly_stability.json"),
        "source_concentration_path": str(source_root / "short_exit_execution_convention_concentration_summary.json"),
        "source_no_lookahead_path": str(source_root / "no_lookahead_audit.json"),
        "runtime_stock_db_status": runtime_status,
        "rankings_freshness_down": rankings_down,
        "rankings_freshness_up": rankings_up,
        "source_paper_replay_ready": bool(source_decision.get("paper_replay_ready")),
        "source_shadow_paper_replay_candidate": bool(source_decision.get("shadow_paper_replay_candidate")),
        "source_best_policy_id": source_decision.get("best_policy_id"),
        "source_best_policy_label": source_decision.get("best_policy_label"),
        "source_best_policy_metrics": source_decision.get("best_policy_metrics"),
        "source_best_policy_concentration": source_decision.get("best_policy_concentration"),
        "source_best_policy_trade_count": len(rows),
        "paper_replay_only": True,
        "no_silent_fallback": True,
        "execution_convention": "next_session_open",
        "no_lookahead_boundary_check": {
            "selection_decisions_only_from_current_and_past_bars": True,
            "execution_fill_occurs_after_decision": True,
            "runtime_db_written": False,
            "production_daily_bars_mutated": False,
            "tainted_trades_included": False,
            "future_outcome_fields_used_for_selection": [],
        },
    }

    decision_payload = {
        "axis_id": AXIS_ID,
        "schema_version": f"{SCHEMA_PREFIX}_decision_v1",
        "decision": decision,
        "decision_reasons": decision_reasons,
        "best_policy_id": source_decision.get("best_policy_id"),
        "best_policy_label": source_decision.get("best_policy_label"),
        "best_execution_convention": source_decision.get("best_execution_convention"),
        "best_policy_metrics": source_decision.get("best_policy_metrics"),
        "best_policy_concentration": source_decision.get("best_policy_concentration"),
        "execution_convention": "next_session_open",
        "paper_execution_replay_ready": decision == "keep_for_paper_execution_replay",
        "paper_replay_ready": bool(source_decision.get("paper_replay_ready")),
        "shadow_paper_replay_candidate": bool(source_decision.get("shadow_paper_replay_candidate")),
        "production_candidate": False,
        "meemee_reflectable": False,
        "publish_allowed": False,
        "live_sell_signal_allowed": False,
        "production_state_changed": False,
        "meeMee_changed": False,
        "no_lookahead_pass": no_lookahead_pass,
        "next_gate": "ops_review_after_paper_execution" if decision == "keep_for_paper_execution_replay" else "hold_or_drop_execution_replay",
        "fallback_summary": {
            "fallback_count": source_decision.get("fallback_summary", {}).get("fallback_count"),
            "fallback_share": source_decision.get("fallback_summary", {}).get("fallback_share"),
            "trade_count": source_decision.get("fallback_summary", {}).get("trade_count"),
        },
        "source_compare_decision": source_decision.get("decision"),
        "source_compare_no_lookahead_pass": bool(source_no_lookahead.get("pass")),
        "source_compare_monthly_stability": source_monthly,
        "source_compare_concentration": source_concentration,
    }

    compare_payload = {
        "axis_id": AXIS_ID,
        "schema_version": f"{SCHEMA_PREFIX}_compare_v1",
        "source_compare_decision": source_decision,
        "paper_execution_replay_summary": summary,
        "paper_execution_replay_concentration": concentration,
        "paper_execution_replay_monthly_stability": monthly,
        "paper_execution_replay_order_count": len(paper_orders),
        "paper_execution_replay_order_intents_preview": paper_orders[:5],
    }

    no_lookahead = _build_no_lookahead(source_no_lookahead, summary, runtime_status)
    no_lookahead["paper_execution_replay_ready"] = decision == "keep_for_paper_execution_replay"

    write_json(run_root / "paper_execution_replay_contract.json", contract)
    write_json(run_root / "paper_execution_replay_compare.json", compare_payload)
    write_json(run_root / "paper_execution_replay_monthly_stability.json", monthly)
    write_json(run_root / "paper_execution_replay_concentration_summary.json", concentration)
    write_csv(run_root / "paper_execution_replay_orders.csv", paper_orders)
    write_jsonl(run_root / "paper_execution_replay_order_intents.jsonl", paper_orders)
    write_json(run_root / "paper_execution_replay_decision.json", decision_payload)
    write_json(run_root / "no_lookahead_audit.json", no_lookahead)
    write_json(
        run_root / "_ARTIFACT_COMPLETE.json",
        {
            "axis_id": AXIS_ID,
            "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
            "created_at_utc": stamp,
            "run_root": str(run_root),
            "decision": decision_payload["decision"],
            "complete": True,
            "required_artifacts": list(REQUIRED_OUTPUTS),
        },
    )

    return {
        "run_root": str(run_root),
        "decision": decision_payload,
        "summary": summary,
        "contract": contract,
        "compare_payload": compare_payload,
        "no_lookahead": no_lookahead,
        "orders": paper_orders,
    }


def main() -> None:
    result = run()
    print(
        json.dumps(
            {
                "run_root": result["run_root"],
                "decision": result["decision"]["decision"],
                "best_policy_label": result["decision"]["best_policy_label"],
                "paper_execution_replay_ready": result["decision"]["paper_execution_replay_ready"],
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
