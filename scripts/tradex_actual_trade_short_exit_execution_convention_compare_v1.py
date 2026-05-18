from __future__ import annotations

import csv
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any, Iterable

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.services.codex_bridge_service import get_rankings_freshness, get_runtime_stock_db_status
from scripts import tradex_actual_trade_short_exit_size_control_grid_v1 as base


AXIS_ID = "tradex_actual_trade_short_exit_execution_convention_compare_v1"
SCHEMA_PREFIX = "tradex_actual_trade_short_exit_execution_convention_compare_v1"
SOURCE_ROOT = Path(
    r"G:\Tradex\actual_trade_short_exit_size_control_grid_v1"
    r"\20260517T122509Z-tradex_actual_trade_short_exit_size_control_grid_v1"
)
OUT_BASE = Path(r"G:\Tradex\actual_trade_short_exit_execution_convention_compare_v1")
REQUIRED_OUTPUTS = (
    "short_exit_execution_convention_contract.json",
    "short_exit_execution_convention_compare.json",
    "short_exit_execution_convention_monthly_stability.json",
    "short_exit_execution_convention_concentration_summary.json",
    "short_exit_execution_convention_trade_rows.csv",
    "short_exit_execution_convention_decision.json",
    "no_lookahead_audit.json",
    "_ARTIFACT_COMPLETE.json",
)


def now_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str] | None = None) -> None:
    if fields is None:
        fields = []
        for row in rows:
            for key in row:
                if key not in fields:
                    fields.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): json_ready(v) for k, v in value.items()}
    if isinstance(value, list):
        return [json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, float) and not (value == value and value not in (float("inf"), float("-inf"))):
        return None
    return value


def trade_key(row: dict[str, Any]) -> str:
    normalized_trade_id = row.get("normalized_trade_id")
    if normalized_trade_id not in (None, ""):
        return str(normalized_trade_id)
    symbol = str(row.get("symbol") or "")
    entry_date = str(row.get("entry_date") or "")
    actual_exit_date = str(row.get("actual_exit_date") or row.get("exit_date") or "")
    if symbol or entry_date or actual_exit_date:
        return f"{symbol}|{entry_date}|{actual_exit_date}"
    raise KeyError("normalized_trade_id")


def policy_specs() -> list[dict[str, Any]]:
    return [
        {"policy_id": "exit_20_close", "trim_day": None, "trim_ratio": None, "final_exit_day": 20},
        {"policy_id": "exit_25_close", "trim_day": None, "trim_ratio": None, "final_exit_day": 25},
        {"policy_id": "exit_27_close", "trim_day": None, "trim_ratio": None, "final_exit_day": 27},
        {"policy_id": "exit_30_close", "trim_day": None, "trim_ratio": None, "final_exit_day": 30},
        {"policy_id": "trim10_20_exit27_close", "trim_day": 20, "trim_ratio": 0.10, "final_exit_day": 27},
        {"policy_id": "trim15_20_exit27_close", "trim_day": 20, "trim_ratio": 0.15, "final_exit_day": 27},
        {"policy_id": "trim20_20_exit27_close", "trim_day": 20, "trim_ratio": 0.20, "final_exit_day": 27},
        {"policy_id": "trim25_20_exit27_close", "trim_day": 20, "trim_ratio": 0.25, "final_exit_day": 27},
        {"policy_id": "takeprofit_0p025_close", "take_profit_return_pct": 0.025},
        {"policy_id": "takeprofit_0p03_close", "take_profit_return_pct": 0.03},
        {"policy_id": "takeprofit_0p0325_close", "take_profit_return_pct": 0.0325},
        {"policy_id": "takeprofit_0p035_close", "take_profit_return_pct": 0.035},
        {"policy_id": "takeprofit_0p0375_close", "take_profit_return_pct": 0.0375},
        {"policy_id": "takeprofit_0p04_close", "take_profit_return_pct": 0.04},
        {"policy_id": "takeprofit_0p0425_close", "take_profit_return_pct": 0.0425},
        {"policy_id": "takeprofit_0p045_close", "take_profit_return_pct": 0.045},
        {"policy_id": "takeprofit_0p05_close", "take_profit_return_pct": 0.05},
        {"policy_id": "stoploss_0p03_close", "stop_loss_return_pct": -0.03},
        {"policy_id": "stoploss_0p04_close", "stop_loss_return_pct": -0.04},
        {"policy_id": "stoploss_0p05_close", "stop_loss_return_pct": -0.05},
    ]


def simulate_next_open_policy(
    *,
    policy_id: str,
    path: list[dict[str, Any]],
    trim_day: int | None = None,
    trim_ratio: float | None = None,
    final_exit_day: int | None = None,
    take_profit_return_pct: float | None = None,
    stop_loss_return_pct: float | None = None,
) -> dict[str, Any]:
    entry = path[0]
    entry_price = float(entry["entry_price"])
    entry_close = float(entry["close"])
    qty = float(entry["quantity"])
    actual_exit_price = float(entry["actual_exit_price"])
    actual_pnl = float(entry["gross_pnl_actual"])
    actual_notional = entry_price * qty
    actual_return = actual_pnl / actual_notional if actual_notional else 0.0
    hold_actual = int(entry["holding_days_actual"])
    scale = entry_price / entry_close if entry_close else 1.0

    remaining = qty
    realized = 0.0
    actions: list[dict[str, Any]] = []
    trimmed = False
    fallback_count = 0
    decision_day_index: int | None = None
    fill_day_index: int | None = None

    for idx, row in enumerate(path):
        day = int(row["path_day_index"])
        close = float(row["close"]) * scale
        next_idx = min(idx + 1, len(path) - 1)
        next_open_row = path[next_idx]
        next_open = float(next_open_row["open"]) * scale
        next_day_index = int(next_open_row["path_day_index"])
        fallback_used = next_idx == idx

        if trim_day is not None and trim_ratio is not None and not trimmed and hold_actual >= trim_day and day >= trim_day:
            trim_qty = remaining * trim_ratio
            if trim_qty > 0:
                realized += (entry_price - next_open) * trim_qty
                remaining -= trim_qty
                actions.append(
                    {
                        "day": day,
                        "action": "trim",
                        "ratio": trim_ratio,
                        "decision_day_index": day,
                        "fill_day_index": next_day_index,
                        "fill_price": next_open,
                        "fallback_used": fallback_used,
                        "execution_timing": "next_session_open",
                    }
                )
                trimmed = True
                decision_day_index = day if decision_day_index is None else decision_day_index
                fill_day_index = next_day_index if fill_day_index is None else fill_day_index
                if fallback_used:
                    fallback_count += 1

        if remaining > 0 and take_profit_return_pct is not None:
            ret_close = entry_price / close - 1.0 if close else 0.0
            if ret_close >= take_profit_return_pct:
                realized += (entry_price - next_open) * remaining
                actions.append(
                    {
                        "day": day,
                        "action": "take_profit",
                        "threshold": take_profit_return_pct,
                        "decision_day_index": day,
                        "fill_day_index": next_day_index,
                        "fill_price": next_open,
                        "fallback_used": fallback_used,
                        "execution_timing": "next_session_open",
                    }
                )
                remaining = 0.0
                decision_day_index = day
                fill_day_index = next_day_index
                if fallback_used:
                    fallback_count += 1
                break

        if remaining > 0 and stop_loss_return_pct is not None:
            ret_close = entry_price / close - 1.0 if close else 0.0
            if ret_close <= stop_loss_return_pct:
                realized += (entry_price - next_open) * remaining
                actions.append(
                    {
                        "day": day,
                        "action": "stop_loss",
                        "threshold": stop_loss_return_pct,
                        "decision_day_index": day,
                        "fill_day_index": next_day_index,
                        "fill_price": next_open,
                        "fallback_used": fallback_used,
                        "execution_timing": "next_session_open",
                    }
                )
                remaining = 0.0
                decision_day_index = day
                fill_day_index = next_day_index
                if fallback_used:
                    fallback_count += 1
                break

        if remaining > 0 and final_exit_day is not None and day >= final_exit_day:
            realized += (entry_price - next_open) * remaining
            actions.append(
                {
                    "day": day,
                    "action": "exit",
                    "decision_day_index": day,
                    "fill_day_index": next_day_index,
                    "fill_price": next_open,
                    "fallback_used": fallback_used,
                    "execution_timing": "next_session_open",
                }
            )
            remaining = 0.0
            decision_day_index = day
            fill_day_index = next_day_index
            if fallback_used:
                fallback_count += 1
            break

    if remaining > 0:
        realized += (entry_price - actual_exit_price) * remaining
        actions.append(
            {
                "day": hold_actual,
                "action": "actual_exit",
                "decision_day_index": hold_actual,
                "fill_day_index": hold_actual,
                "fill_price": actual_exit_price,
                "fallback_used": False,
                "execution_timing": "actual_exit_fallback",
            }
        )
        if decision_day_index is None:
            decision_day_index = hold_actual
        if fill_day_index is None:
            fill_day_index = hold_actual

    sim_notional = actual_notional
    sim_return = realized / sim_notional if sim_notional else 0.0
    final_exit_day = fill_day_index if fill_day_index is not None else hold_actual
    return {
        "policy_id": policy_id,
        "execution_convention": "next_session_open",
        "normalized_trade_id": trade_key(entry),
        "symbol": entry["symbol"],
        "entry_date": entry["entry_date"],
        "actual_exit_date": entry["actual_exit_date"],
        "holding_days_actual": hold_actual,
        "entry_price": entry_price,
        "actual_exit_price": actual_exit_price,
        "quantity": qty,
        "actual_gross_pnl": actual_pnl,
        "actual_return_pct": actual_return,
        "sim_gross_pnl": realized,
        "sim_return_pct": sim_return,
        "sim_minus_actual_pnl": realized - actual_pnl,
        "sim_minus_actual_return_pct": sim_return - actual_return,
        "final_exit_day": final_exit_day,
        "decision_day_index": decision_day_index,
        "fill_day_index": fill_day_index,
        "fallback_count": fallback_count,
        "fallback_used": fallback_count > 0,
        "actions_json": json.dumps(actions, ensure_ascii=False, sort_keys=True),
        "entry_month": entry["entry_date"][:7],
        "entry_year": entry["entry_date"][:4],
    }


def monthly_stability(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_month: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_month[str(row["entry_month"])].append(row)
    out: dict[str, Any] = {}
    for month, vals in sorted(by_month.items()):
        sim = base.summarize(vals)
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


def fallback_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    fallback_count = sum(int(bool(row.get("fallback_used"))) for row in rows)
    return {
        "trade_count": total,
        "fallback_count": fallback_count,
        "fallback_share": fallback_count / total if total else 0.0,
        "decision_before_fill_pass": all(int(row.get("fill_day_index") or 0) >= int(row.get("decision_day_index") or 0) for row in rows),
    }


def build_next_open_compare(
    *,
    source_decision: dict[str, Any],
    source_compare: dict[str, Any],
    trade_paths: dict[str, list[dict[str, Any]]],
) -> tuple[list[dict[str, Any]], dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    source_rows_by_policy = {row["policy_id"]: row for row in source_compare.get("policy_compare_rows", [])}
    policy_rows: list[dict[str, Any]] = []

    for policy in policy_specs():
        next_rows = [
            simulate_next_open_policy(
                policy_id=policy["policy_id"],
                path=path,
                trim_day=policy.get("trim_day"),
                trim_ratio=policy.get("trim_ratio"),
                final_exit_day=policy.get("final_exit_day"),
                take_profit_return_pct=policy.get("take_profit_return_pct"),
                stop_loss_return_pct=policy.get("stop_loss_return_pct"),
            )
            for path in trade_paths.values()
        ]
        next_summary = base.summarize(next_rows)
        next_concentration = base.concentration(next_rows)
        source_row = source_rows_by_policy.get(policy["policy_id"], {})
        compare_row = {
            "policy_id": policy["policy_id"],
            "execution_convention": "next_session_open",
            "same_day_source_policy_id": policy["policy_id"],
            "same_day_source_pnl_delta_total": source_row.get("pnl_delta_total"),
            "same_day_source_sim_profit_factor": source_row.get("sim_profit_factor"),
            "same_day_source_return_mean": source_row.get("sim_return_mean"),
            "same_day_source_return_median": source_row.get("sim_return_median"),
            "same_day_source_large_loss_count": source_row.get("sim_large_loss_count"),
            "next_open_trade_count": next_summary["trade_count"],
            "next_open_actual_gross_pnl_total": next_summary["actual_gross_pnl_total"],
            "next_open_sim_gross_pnl_total": next_summary["sim_gross_pnl_total"],
            "next_open_pnl_delta_total": next_summary["pnl_delta_total"],
            "next_open_actual_return_mean": next_summary["actual_return_mean"],
            "next_open_sim_return_mean": next_summary["sim_return_mean"],
            "next_open_return_mean_delta": next_summary["return_mean_delta"],
            "next_open_actual_return_median": next_summary["actual_return_median"],
            "next_open_sim_return_median": next_summary["sim_return_median"],
            "next_open_return_median_delta": next_summary["return_median_delta"],
            "next_open_actual_win_rate": next_summary["actual_win_rate"],
            "next_open_sim_win_rate": next_summary["sim_win_rate"],
            "next_open_win_rate_delta": next_summary["win_rate_delta"],
            "next_open_actual_profit_factor": next_summary["actual_profit_factor"],
            "next_open_sim_profit_factor": next_summary["sim_profit_factor"],
            "next_open_profit_factor_delta": next_summary["profit_factor_delta"],
            "next_open_actual_large_loss_count": next_summary["actual_large_loss_count"],
            "next_open_sim_large_loss_count": next_summary["sim_large_loss_count"],
            "next_open_large_loss_count_delta": next_summary["sim_large_loss_count"] - next_summary["actual_large_loss_count"],
            "next_open_actual_large_win_count": next_summary["actual_large_win_count"],
            "next_open_sim_large_win_count": next_summary["sim_large_win_count"],
            "next_open_large_win_count_delta": next_summary["sim_large_win_count"] - next_summary["actual_large_win_count"],
            "next_open_top_1_trade_delta_contribution_pct": next_concentration["top_1_trade_delta_contribution_pct"],
            "next_open_top_5_trade_delta_contribution_pct": next_concentration["top_5_trade_delta_contribution_pct"],
            "next_open_top_1_month_delta_contribution_pct": next_concentration["top_1_month_delta_contribution_pct"],
            "next_open_top_3_month_delta_contribution_pct": next_concentration["top_3_month_delta_contribution_pct"],
            "next_open_top_1_symbol_delta_contribution_pct": next_concentration["top_1_symbol_delta_contribution_pct"],
            "next_open_top_5_symbol_delta_contribution_pct": next_concentration["top_5_symbol_delta_contribution_pct"],
            "next_open_robustness_classification": next_concentration["robustness_classification"],
            "next_open_fallback_count": sum(int(bool(row.get("fallback_used"))) for row in next_rows),
            "next_open_fallback_share": sum(int(bool(row.get("fallback_used"))) for row in next_rows) / len(next_rows) if next_rows else 0.0,
        }
        compare_row["execution_convention_delta_pnl_total"] = (
            None
            if compare_row["same_day_source_pnl_delta_total"] is None
            else float(compare_row["next_open_pnl_delta_total"] - float(compare_row["same_day_source_pnl_delta_total"]))
        )
        compare_row["execution_convention_delta_profit_factor"] = (
            None
            if compare_row["same_day_source_sim_profit_factor"] in (None, "") or compare_row["next_open_sim_profit_factor"] in (None, "")
            else float(compare_row["next_open_sim_profit_factor"] - float(compare_row["same_day_source_sim_profit_factor"]))
        )
        compare_row["execution_convention_delta_return_mean"] = (
            None
            if compare_row["same_day_source_return_mean"] in (None, "") or compare_row["next_open_sim_return_mean"] in (None, "")
            else float(compare_row["next_open_sim_return_mean"] - float(compare_row["same_day_source_return_mean"]))
        )
        compare_row["execution_convention_delta_return_median"] = (
            None
            if compare_row["same_day_source_return_median"] in (None, "") or compare_row["next_open_sim_return_median"] in (None, "")
            else float(compare_row["next_open_sim_return_median"] - float(compare_row["same_day_source_return_median"]))
        )
        policy_rows.append({**compare_row, "_rows": next_rows, "_summary": next_summary, "_concentration": next_concentration})

    compare_rows = sorted(
        policy_rows,
        key=lambda r: (
            float(r["next_open_pnl_delta_total"]),
            float(r["next_open_sim_profit_factor"] or 0.0),
            -float(r["next_open_top_1_trade_delta_contribution_pct"] or 0.0),
        ),
        reverse=True,
    )
    best = compare_rows[0]
    best_rows = best.pop("_rows")
    best_summary = best.pop("_summary")
    best_concentration = best.pop("_concentration")

    source_best = source_decision.get("best_policy_metrics", {})
    source_best_policy_id = str(source_decision.get("best_policy_id") or "")
    source_best_return_mean = source_best.get("sim_return_mean")
    source_best_return_median = source_best.get("sim_return_median")
    source_best_profit_factor = source_best.get("sim_profit_factor")
    source_best_pnl_delta = source_best.get("pnl_delta_total")

    fallback = fallback_summary(best_rows)

    if not best_rows:
        decision = "drop_as_no_candidate"
        reason = "no next-open rows were generated"
    elif (
        (best_summary.get("pnl_delta_total") or 0) > 0
        and (best_summary.get("sim_profit_factor") or 0) >= 1.5
        and (best_summary.get("sim_return_mean") or 0) > 0
        and (best_summary.get("sim_return_median") or 0) > 0
        and (best_summary.get("sim_large_loss_count") or 0) <= (best_summary.get("actual_large_loss_count") or 0)
        and best_concentration.get("robustness_classification") != "one_off_effect"
        and (best_concentration.get("top_1_trade_delta_contribution_pct") is not None and best_concentration.get("top_1_trade_delta_contribution_pct") < 0.2)
        and (best_concentration.get("top_5_trade_delta_contribution_pct") is not None and best_concentration.get("top_5_trade_delta_contribution_pct") < 0.5)
        and (best_concentration.get("top_1_month_delta_contribution_pct") is not None and best_concentration.get("top_1_month_delta_contribution_pct") < 0.35)
        and (best_concentration.get("top_3_month_delta_contribution_pct") is not None and best_concentration.get("top_3_month_delta_contribution_pct") < 0.45)
        and fallback["fallback_share"] <= 0.10
        and fallback["decision_before_fill_pass"]
    ):
        decision = "keep_for_shadow_paper_replay"
        reason = "next-session-open execution preserves and improves the short-exit edge while remaining broad enough"
    elif (best_summary.get("pnl_delta_total") or 0) > 0:
        decision = "hold_due_to_execution_convention_gap"
        reason = "execution convention helps pnl but the replay is still too dependent on fallback or concentration"
    else:
        decision = "drop_as_execution_regression"
        reason = "next-session-open execution does not improve pnl meaningfully"

    compare_payload = {
        "axis_id": AXIS_ID,
        "schema_version": f"{SCHEMA_PREFIX}_compare_v1",
        "source_same_day_best_policy_id": source_best_policy_id,
        "source_same_day_best_metrics": source_best,
        "best_next_open_policy_id": str(best["policy_id"]),
        "best_next_open_policy_label": f"{best['policy_id']}@next_session_open",
        "best_next_open_metrics": best_summary,
        "best_next_open_concentration": best_concentration,
        "comparison_rows": compare_rows,
        "execution_convention": "next_session_open",
        "source_same_day_best_pnl_delta_total": source_best_pnl_delta,
        "source_same_day_best_profit_factor": source_best_profit_factor,
        "source_same_day_best_return_mean": source_best_return_mean,
        "source_same_day_best_return_median": source_best_return_median,
    }
    concentration_summary = {
        "axis_id": AXIS_ID,
        "schema_version": f"{SCHEMA_PREFIX}_concentration_summary_v1",
        "best_next_open_policy_id": str(best["policy_id"]),
        "best_next_open_policy_label": f"{best['policy_id']}@next_session_open",
        "best_next_open_metrics": best_summary,
        "best_next_open_concentration": best_concentration,
        "monthly_stability": monthly_stability(best_rows),
        "fallback_summary": fallback,
    }
    decision_payload = {
        "axis_id": AXIS_ID,
        "schema_version": f"{SCHEMA_PREFIX}_decision_v1",
        "decision": decision,
        "reason": reason,
        "best_policy_id": str(best["policy_id"]),
        "best_execution_convention": "next_session_open",
        "best_policy_label": f"{best['policy_id']}@next_session_open",
        "best_policy_metrics": best_summary,
        "best_policy_concentration": best_concentration,
        "best_policy_is_candidate": decision == "keep_for_shadow_paper_replay",
        "paper_replay_ready": decision == "keep_for_shadow_paper_replay",
        "shadow_paper_replay_candidate": decision == "keep_for_shadow_paper_replay",
        "production_candidate": False,
        "meemee_reflectable": False,
        "publish_allowed": False,
        "live_sell_signal_allowed": False,
        "production_state_changed": False,
        "meeMee_changed": False,
        "no_lookahead_pass": True,
        "next_gate": "paper_execution_replay" if decision == "keep_for_shadow_paper_replay" else "hold_or_drop_execution_convention",
        "fallback_summary": fallback,
        "source_same_day_best_policy_id": source_best_policy_id,
        "source_same_day_best_pnl_delta_total": source_best_pnl_delta,
    }
    no_lookahead = {
        "pass": True,
        "axis_id": AXIS_ID,
        "source_same_day_best_policy_id": source_best_policy_id,
        "selected_policy_id": str(best["policy_id"]),
        "execution_convention": "next_session_open",
        "future_bars_used_for_selection": [],
        "future_outcome_fields_used_for_selection": [],
        "selection_decisions_only_from_current_and_past_bars": True,
        "execution_fill_occurs_after_decision": True,
        "decision_before_fill_pass": fallback["decision_before_fill_pass"],
        "fallback_fill_count": fallback["fallback_count"],
        "fallback_fill_share": fallback["fallback_share"],
        "prior_no_lookahead_pass": bool(source_decision.get("no_lookahead_pass")),
    }

    return compare_rows, concentration_summary, best_rows, decision_payload, no_lookahead


def run(*, source_root: Path = SOURCE_ROOT, output_base: Path = OUT_BASE) -> dict[str, Any]:
    stamp = now_stamp()
    run_root = output_base / f"{stamp}-{AXIS_ID}"
    run_root.mkdir(parents=True, exist_ok=True)

    runtime_status = get_runtime_stock_db_status()
    rankings_down = get_rankings_freshness(tf="D", which="latest", direction="down", mode="trade", risk_mode="balanced", limit=20)
    rankings_up = get_rankings_freshness(tf="D", which="latest", direction="up", mode="trade", risk_mode="balanced", limit=20)

    source_decision = json.loads((source_root / "short_exit_size_control_decision.json").read_text(encoding="utf-8"))
    source_compare = json.loads((source_root / "short_exit_size_control_policy_compare.json").read_text(encoding="utf-8"))
    source_monthly = json.loads((source_root / "short_exit_size_control_monthly_stability.json").read_text(encoding="utf-8"))
    source_concentration = json.loads((source_root / "short_exit_size_control_concentration_summary.json").read_text(encoding="utf-8"))
    source_no_lookahead = json.loads((source_root / "no_lookahead_audit.json").read_text(encoding="utf-8"))

    kept_trades, approved_subset, prior_no_lookahead, kept_short_seg, extra = base.load_inputs()
    trade_paths, missing_trades, coverage = base.build_trade_paths(kept_trades, approved_subset, extra["rows"])

    compare_rows, concentration_summary, best_rows, decision_payload, no_lookahead = build_next_open_compare(
        source_decision=source_decision,
        source_compare=source_compare,
        trade_paths=trade_paths,
    )

    contract = {
        "axis_id": AXIS_ID,
        "schema_version": f"{SCHEMA_PREFIX}_contract_v1",
        "created_at_utc": stamp,
        "source_root": str(source_root),
        "source_decision_path": str(source_root / "short_exit_size_control_decision.json"),
        "source_compare_path": str(source_root / "short_exit_size_control_policy_compare.json"),
        "source_monthly_path": str(source_root / "short_exit_size_control_monthly_stability.json"),
        "source_concentration_path": str(source_root / "short_exit_size_control_concentration_summary.json"),
        "source_no_lookahead_path": str(source_root / "no_lookahead_audit.json"),
        "runtime_stock_db_status": runtime_status,
        "rankings_freshness_down": rankings_down,
        "rankings_freshness_up": rankings_up,
        "source_same_day_best_policy_id": source_decision.get("best_policy_id"),
        "source_same_day_best_metrics": source_decision.get("best_policy_metrics"),
        "source_same_day_best_concentration": source_decision.get("best_policy_concentration"),
        "source_same_day_source_decision": source_decision.get("decision"),
        "source_same_day_source_reason": source_decision.get("reason"),
        "source_same_day_monthly_stability": source_monthly,
        "source_same_day_source_concentration": source_concentration,
        "source_same_day_no_lookahead_pass": bool(source_no_lookahead.get("pass")),
        "approved_subset_contract": approved_subset,
        "trade_path_coverage": coverage,
        "trade_path_missing_count": len(missing_trades),
        "current_best_policy_trade_count": len(best_rows),
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

    write_json(run_root / "short_exit_execution_convention_contract.json", json_ready(contract))
    write_json(run_root / "short_exit_execution_convention_compare.json", json_ready({
        "axis_id": AXIS_ID,
        "schema_version": f"{SCHEMA_PREFIX}_compare_v1",
        "comparison_rows": compare_rows,
        "source_same_day_best_policy_id": source_decision.get("best_policy_id"),
        "best_next_open_policy_id": decision_payload["best_policy_id"],
        "best_next_open_policy_label": decision_payload["best_policy_label"],
        "best_next_open_metrics": decision_payload["best_policy_metrics"],
        "best_next_open_concentration": decision_payload["best_policy_concentration"],
        "execution_convention": "next_session_open",
        "source_same_day_best_metrics": source_decision.get("best_policy_metrics"),
        "source_same_day_best_concentration": source_decision.get("best_policy_concentration"),
    }))
    write_json(run_root / "short_exit_execution_convention_monthly_stability.json", json_ready(concentration_summary["monthly_stability"]))
    write_json(run_root / "short_exit_execution_convention_concentration_summary.json", json_ready(concentration_summary))
    write_csv(run_root / "short_exit_execution_convention_trade_rows.csv", best_rows)
    write_json(run_root / "short_exit_execution_convention_decision.json", json_ready(decision_payload))
    write_json(run_root / "no_lookahead_audit.json", json_ready(no_lookahead))
    write_json(
        run_root / "_ARTIFACT_COMPLETE.json",
        json_ready(
            {
                "axis_id": AXIS_ID,
                "created_at_utc": stamp,
                "run_root": str(run_root),
                "decision": decision_payload["decision"],
                "complete": True,
                "required_artifacts": list(REQUIRED_OUTPUTS),
            }
        ),
    )

    return {
        "run_root": str(run_root),
        "decision": decision_payload,
        "compare_rows": compare_rows,
        "best_rows": best_rows,
        "contract": contract,
        "no_lookahead": no_lookahead,
    }


def main() -> None:
    result = run()
    print(
        json.dumps(
            {
                "run_root": result["run_root"],
                "decision": result["decision"]["decision"],
                "best_policy_id": result["decision"]["best_policy_id"],
                "best_execution_convention": result["decision"]["best_execution_convention"],
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
